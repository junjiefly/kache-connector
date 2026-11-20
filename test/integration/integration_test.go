package integration

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/errs"
	"github.com/junjiefly/kache-connector/internal/metrics"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Spec test 4, 5, 27: PutBatch/GetBatch happy paths and missing-key behavior.
func TestPutGetHappyPath(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	d1 := []byte("hello kv cache")
	d2 := bytes.Repeat([]byte{0xab, 0xcd}, 100)

	rep := h.put(t,
		&kachev1.PutChunk{Key: "k1", Data: d1, SizeBytes: uint64(len(d1)), Attributes: map[string]string{"model": "m1"}},
		&kachev1.PutChunk{Key: "k2", Data: d2, SizeBytes: uint64(len(d2)), ContentHash: hashOf(d2)},
	)
	if !rep.GetCommitted() || rep.GetPutId() == "" {
		t.Fatalf("put not committed: %+v", rep)
	}
	for _, r := range rep.GetResults() {
		if !r.GetOk() {
			t.Fatalf("put result not ok: %+v", r)
		}
	}
	if got := len(h.plane.Writes()); got != 2 {
		t.Fatalf("want 2 data-plane writes, got %d", got)
	}

	get := h.get(t, "k1", "k2", "missing")
	r1, r2, r3 := get.GetResults()[0], get.GetResults()[1], get.GetResults()[2]
	if !r1.GetHit() || !bytes.Equal(r1.GetData(), d1) {
		t.Fatalf("k1 read mismatch: %+v", r1)
	}
	if r1.GetSizeBytes() != uint64(len(d1)) || !bytes.Equal(r1.GetContentHash(), hashOf(d1)) {
		t.Fatalf("k1 metadata mismatch: %+v", r1)
	}
	if r1.GetAttributes()["model"] != "m1" || r1.GetLastModifiedUnixSec() == 0 {
		t.Fatalf("k1 attributes/mtime mismatch: %+v", r1)
	}
	if !r2.GetHit() || !bytes.Equal(r2.GetData(), d2) {
		t.Fatalf("k2 read mismatch")
	}
	if r3.GetHit() || r3.GetErrorCode() != "KEY_NOT_FOUND" {
		t.Fatalf("missing key must be a KEY_NOT_FOUND miss: %+v", r3)
	}

	match, err := h.conn.Match(h.ctx, &kachev1.MatchBatchRequest{Keys: []string{"k1", "missing"}})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !match.GetResults()[0].GetHit() || match.GetResults()[1].GetHit() {
		t.Fatalf("match mismatch: %+v", match.GetResults())
	}

	del, err := h.conn.Delete(h.ctx, &kachev1.DeleteBatchRequest{Keys: []string{"k1", "missing"}})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !del.GetResults()[0].GetDeleted() {
		t.Fatalf("delete k1 failed: %+v", del.GetResults()[0])
	}
	if !del.GetResults()[1].GetDeleted() || del.GetResults()[1].GetErrorCode() != "" {
		t.Fatalf("delete missing mismatch: %+v", del.GetResults()[1])
	}
	if after := h.get(t, "k1"); after.GetResults()[0].GetHit() {
		t.Fatal("deleted key still visible")
	}
}

// Spec test 6: namespace mismatch rejection on every batch API.
func TestNamespaceMismatch(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	other := "other-ns"
	if _, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Namespace: other, Chunks: []*kachev1.PutChunk{chunk("k", []byte("x"))}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Put: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Namespace: other, Keys: []string{"k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Get: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Match(h.ctx, &kachev1.MatchBatchRequest{Namespace: other, Keys: []string{"k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Match: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Delete(h.ctx, &kachev1.DeleteBatchRequest{Namespace: other, Keys: []string{"k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Delete: want INVALID_ARGUMENT, got %v", err)
	}
	if h.srv.PrepareCalls != 0 || h.srv.ResolveCalls != 0 {
		t.Fatal("mismatched namespace must not reach the server")
	}
}

// Spec test 7: chunk-size limit, local and server-caps effective minimum.
func TestChunkSizeLimit(t *testing.T) {
	t.Run("local limit", func(t *testing.T) {
		h := newHarness(t, harnessConfig{mutate: func(c *kvcache.Config) {
			c.Limits.MaxChunkBytes = 8
		}})
		_, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("k", make([]byte, 16))}})
		if errs.From(err) != errs.ChunkTooLarge {
			t.Fatalf("want CHUNK_TOO_LARGE, got %v", err)
		}
		if h.srv.PrepareCalls != 0 {
			t.Fatal("oversize chunk must fail before Prepare")
		}
	})
	t.Run("server caps limit", func(t *testing.T) {
		caps := testserver.DefaultCaps()
		caps.MaxChunkBytes = 8
		h := newHarness(t, harnessConfig{server: testserver.Options{Caps: caps}})
		_, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("k", make([]byte, 16))}})
		if errs.From(err) != errs.ChunkTooLarge {
			t.Fatalf("want CHUNK_TOO_LARGE from server caps, got %v", err)
		}
	})
}

// Spec test 8: batch-size limits (chunk count and total bytes).
func TestBatchSizeLimit(t *testing.T) {
	t.Run("chunk count", func(t *testing.T) {
		h := newHarness(t, harnessConfig{mutate: func(c *kvcache.Config) {
			c.Limits.MaxBatchChunks = 1
		}})
		_, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("a", []byte("x")), chunk("b", []byte("y"))}})
		if errs.From(err) != errs.BatchTooLarge {
			t.Fatalf("Put: want BATCH_TOO_LARGE, got %v", err)
		}
		if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: []string{"a", "b"}}); errs.From(err) != errs.BatchTooLarge {
			t.Fatalf("Get: want BATCH_TOO_LARGE, got %v", err)
		}
	})
	t.Run("total bytes", func(t *testing.T) {
		caps := testserver.DefaultCaps()
		caps.MaxBatchBytes = 24
		h := newHarness(t, harnessConfig{server: testserver.Options{Caps: caps}})
		_, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("a", make([]byte, 16)), chunk("b", make([]byte, 16))}})
		if errs.From(err) != errs.BatchTooLarge {
			t.Fatalf("want BATCH_TOO_LARGE for 32 > 24 bytes, got %v", err)
		}
	})
}

// Spec test 9: declared size must equal payload length.
func TestPayloadLengthMismatch(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	_, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{
		{Key: "k", Data: []byte("abc"), SizeBytes: 4},
	}})
	if errs.From(err) != errs.PayloadLengthMismatch {
		t.Fatalf("want PAYLOAD_LENGTH_MISMATCH, got %v", err)
	}
	if _, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("z", nil)}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("zero-size chunk: want INVALID_ARGUMENT, got %v", err)
	}
}

// Spec test 10: duplicate keys rejected on every batch API.
func TestDuplicateKeys(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	if _, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("k", []byte("x")), chunk("k", []byte("y"))}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Put: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: []string{"k", "k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Get: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Match(h.ctx, &kachev1.MatchBatchRequest{Keys: []string{"k", "k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Match: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Delete(h.ctx, &kachev1.DeleteBatchRequest{Keys: []string{"k", "k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("Delete: want INVALID_ARGUMENT, got %v", err)
	}
}

func TestEmptyBatchAndEmptyKey(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("empty batch: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: []string{""}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("empty key: want INVALID_ARGUMENT, got %v", err)
	}
	if _, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{ClientRequestId: "not-a-uuid", Chunks: []*kachev1.PutChunk{chunk("k", []byte("x"))}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("bad client_request_id: want INVALID_ARGUMENT, got %v", err)
	}
}

// Spec test 11: putting an existing key overwrites the stored chunk, which is
// overwrite, and a rejected Prepare without put_id must not trigger Abort.
func TestExistingKey(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.mustPut(t, "k", []byte("original"))
	rep := h.put(t, chunk("k", []byte("replacement")))
	if rep.GetCommitted() {
		t.Fatal("overwrite must not commit")
	}
	if rep.GetResults()[0].GetErrorCode() != "KEY_EXISTS" {
		t.Fatalf("want KEY_EXISTS, got %+v", rep.GetResults()[0])
	}
	if h.srv.AbortCalls != 0 {
		t.Fatalf("rejected Prepare without put_id must not Abort, got %d calls", h.srv.AbortCalls)
	}
	if got := h.get(t, "k"); !bytes.Equal(got.GetResults()[0].GetData(), []byte("original")) {
		t.Fatal("original value was modified")
	}
}

// Spec test 12: multi-block chunk assembly on write and read.
func TestMultiBlockAssembly(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{Splitter: testserver.MultiBlockSplitter(7)}})
	d1 := bytes.Repeat([]byte("abcdefghij"), 3)[:25] // 4 blocks: 7+7+7+4
	d2 := []byte("0123456789")                       // 2 blocks: 7+3
	rep := h.put(t, chunk("k1", d1), chunk("k2", d2))
	if !rep.GetCommitted() {
		t.Fatalf("put not committed: %+v", rep.GetResults())
	}
	if got := len(h.plane.Writes()); got != 6 {
		t.Fatalf("want 6 block writes, got %d", got)
	}
	get := h.get(t, "k1", "k2")
	if !bytes.Equal(get.GetResults()[0].GetData(), d1) || !bytes.Equal(get.GetResults()[1].GetData(), d2) {
		t.Fatal("multi-block assembly mismatch")
	}
}

// Wire padding: wire_len > logical_len must zero-pad writes and trim reads.
func TestWirePadding(t *testing.T) {
	split := func(chunkIndex int, size uint64) []testserver.BlockSpec {
		return []testserver.BlockSpec{{ChunkIndex: -1, LogicalLen: uint32(size), WireLen: uint32(size) + 8}}
	}
	h := newHarness(t, harnessConfig{server: testserver.Options{Splitter: split}})
	data := []byte("padding-test")
	h.mustPut(t, "k", data)
	writes := h.plane.Writes()
	if len(writes) != 1 || writes[0].Length != uint32(len(data))+8 {
		t.Fatalf("want one padded write of %d bytes, got %+v", len(data)+8, writes)
	}
	if !bytes.Equal(writes[0].Payload[:len(data)], data) {
		t.Fatal("payload prefix mismatch")
	}
	for _, b := range writes[0].Payload[len(data):] {
		if b != 0 {
			t.Fatal("wire padding must be zero-filled")
		}
	}
	if got := h.get(t, "k"); !bytes.Equal(got.GetResults()[0].GetData(), data) {
		t.Fatal("padded read must trim to logical_len")
	}
}

// Spec test 13: write-plan coverage validation rejects gaps before any write.
func TestWritePlanCoverageRejected(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Splitter: testserver.MultiBlockSplitter(7),
		MutatePrepare: func(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
			return blocks[:1] // drop coverage of the 25-byte chunk
		},
	}})
	rep := h.put(t, chunk("k", make([]byte, 25)))
	if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "INVALID_WRITE_PLAN" {
		t.Fatalf("want INVALID_WRITE_PLAN, got %+v", rep)
	}
	if msg := rep.GetResults()[0].GetMessage(); msg != "write plan rejected by validation" {
		t.Fatalf("per-key message must not carry plan details, got %q", msg)
	}
	if len(h.plane.Writes()) != 0 {
		t.Fatal("invalid plan must be rejected before data-plane writes")
	}
	if h.srv.AbortCalls != 1 {
		t.Fatalf("invalid plan must Abort, got %d calls", h.srv.AbortCalls)
	}
}

// Spec test 14: read-plan coverage validation.
func TestReadPlanCoverageRejected(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{Splitter: testserver.MultiBlockSplitter(7)}})
	h.mustPut(t, "k", make([]byte, 25))
	h.srv.MutateBlocks(testNamespace, "k", func(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
		return blocks[:3] // 21 of 25 bytes covered
	})
	res := h.get(t, "k").GetResults()[0]
	if res.GetHit() || res.GetErrorCode() != "INVALID_READ_PLAN" {
		t.Fatalf("want INVALID_READ_PLAN, got %+v", res)
	}
	if msg := res.GetMessage(); msg != "read plan rejected by validation" {
		t.Fatalf("per-key message must not carry plan details, got %q", msg)
	}
}

// Spec test 15: unsupported ref_kind on write and read plans.
func TestUnsupportedRefKind(t *testing.T) {
	t.Run("write plan", func(t *testing.T) {
		h := newHarness(t, harnessConfig{server: testserver.Options{
			MutatePrepare: func(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
				for _, b := range blocks {
					b.RefKind = "mystery_v9"
				}
				return blocks
			},
		}})
		rep := h.put(t, chunk("k", []byte("data")))
		if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "UNSUPPORTED_REF_KIND" {
			t.Fatalf("want UNSUPPORTED_REF_KIND, got %+v", rep)
		}
		if h.srv.AbortCalls != 1 || len(h.plane.Writes()) != 0 {
			t.Fatal("unsupported ref_kind must Abort before writing")
		}
	})
	t.Run("read plan", func(t *testing.T) {
		h := newHarness(t, harnessConfig{})
		h.mustPut(t, "k", []byte("data"))
		h.srv.MutateBlocks(testNamespace, "k", func(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
			for _, b := range blocks {
				b.RefKind = "mystery_v9"
			}
			return blocks
		})
		res := h.get(t, "k").GetResults()[0]
		if res.GetHit() || res.GetErrorCode() != "INVALID_READ_PLAN" {
			t.Fatalf("want INVALID_READ_PLAN, got %+v", res)
		}
	})
}

// Spec test 16: duplicate target_id within a block rejects the write plan.
func TestDuplicateTargetID(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		MutatePrepare: func(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
			for _, b := range blocks {
				b.Targets = append(b.Targets, b.Targets[0])
			}
			return blocks
		},
	}})
	rep := h.put(t, chunk("k", []byte("data")))
	if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "INVALID_WRITE_PLAN" {
		t.Fatalf("want INVALID_WRITE_PLAN, got %+v", rep)
	}
	if len(h.plane.Writes()) != 0 || h.srv.AbortCalls != 1 {
		t.Fatal("duplicate target_id must Abort before writing")
	}
}

// Spec test 17: the server rejects Commit requests with missing, unknown or
// duplicate target results (exercised through the direct control client).
func TestCommitValidation(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Targets: []testserver.TargetSpec{
			{Endpoint: "127.0.0.1:7201", Transport: "tcp"},
			{Endpoint: "127.0.0.1:7202", Transport: "tcp"},
		},
	}})
	cc := h.controlClient(t)
	prep := h.prepareDirect(t, cc, "11111111-2222-3333-4444-555555555555", "k", 16)

	missing := commitResultsFor(prep)
	missing[0].Results = missing[0].Results[:1] // drop one replica result
	rep, err := cc.Commit(h.ctx, &kachev1.CommitPutBatchRequest{Namespace: testNamespace, PutId: prep.GetPutId(), Results: missing})
	if err != nil || rep.GetCommitted() || rep.GetErrors()[0].GetCode() != "INVALID_ARGUMENT" {
		t.Fatalf("missing target result must be rejected: rep=%+v err=%v", rep, err)
	}

	unknown := commitResultsFor(prep)
	unknown[0].PlanBlockId = "no-such-block"
	if _, err := cc.Commit(h.ctx, &kachev1.CommitPutBatchRequest{Namespace: testNamespace, PutId: prep.GetPutId(), Results: unknown}); errs.From(err) != errs.Internal {
		t.Fatalf("unknown plan_block_id: want INTERNAL, got %v", err)
	}

	dup := commitResultsFor(prep)
	dup[0].Results = append(dup[0].Results, dup[0].Results[0])
	rep, err = cc.Commit(h.ctx, &kachev1.CommitPutBatchRequest{Namespace: testNamespace, PutId: prep.GetPutId(), Results: dup})
	if err != nil || rep.GetCommitted() {
		t.Fatalf("duplicate target result must be rejected: rep=%+v err=%v", rep, err)
	}

	ok, err := cc.Commit(h.ctx, &kachev1.CommitPutBatchRequest{Namespace: testNamespace, PutId: prep.GetPutId(), Results: commitResultsFor(prep)})
	if err != nil || !ok.GetCommitted() {
		t.Fatalf("valid commit must succeed: rep=%+v err=%v", ok, err)
	}
}

// Spec tests 18, 19: partial replica failure is all-or-nothing and triggers Abort.
func TestReplicaWriteAllOrNothing(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Targets: []testserver.TargetSpec{
			{Endpoint: "127.0.0.1:7201", Transport: "tcp"},
			{Endpoint: "127.0.0.1:7202", Transport: "tcp"},
		},
	}})
	h.plane.FailWriteEndpoint("127.0.0.1:7202")
	rep := h.put(t, chunk("k", []byte("data")))
	if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "WRITE_TARGET_FAILED" {
		t.Fatalf("partial replica failure must fail the batch: %+v", rep)
	}
	if h.srv.AbortCalls != 1 {
		t.Fatalf("want 1 Abort, got %d", h.srv.AbortCalls)
	}
	if got := len(h.plane.Writes()); got != 2 {
		t.Fatalf("both replicas must be attempted, got %d writes", got)
	}
	if h.get(t, "k").GetResults()[0].GetHit() {
		t.Fatal("failed put must not be visible")
	}
	if m := h.metrics(t).Snapshot().Counters[metrics.PutAbortTotal]; m != 1 {
		t.Fatalf("put_abort_total = %d", m)
	}
}

func TestTargetFailureTriggersAbort(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.plane.FailWriteEndpoint("127.0.0.1:7101")
	rep := h.put(t, chunk("k", []byte("data")))
	if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "WRITE_TARGET_FAILED" {
		t.Fatalf("want WRITE_TARGET_FAILED, got %+v", rep)
	}
	if h.srv.AbortCalls != 1 {
		t.Fatalf("want 1 Abort, got %d", h.srv.AbortCalls)
	}
	if m := h.metrics(t).Snapshot().Counters[metrics.TargetWriteFailures]; m == 0 {
		t.Fatal("target_write_failures not counted")
	}
}

// Control intentionally does not implement Prepare idempotency. A lost
// Prepare response may leave an allocated plan, but retrying would allocate
// duplicate blocks; connector therefore fails the Put instead of retrying.
func TestPrepareResponseLossDoesNotRetry(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.srv.DropNextPrepareResponse()
	rep, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{
		ClientRequestId: "11111111-2222-3333-4444-555555555555",
		Chunks:          []*kachev1.PutChunk{chunk("k", []byte("data"))},
	})
	if rep.GetCommitted() || errs.From(err) != errs.TransportUnavailable {
		t.Fatalf("put after lost Prepare response must fail without retry: rep=%+v err=%v", rep, err)
	}
	if h.srv.PrepareCalls != 1 {
		t.Fatalf("want exactly 1 Prepare call, got %d", h.srv.PrepareCalls)
	}

	// The same client_request_id allocates a new plan, matching the Control's
	// no-idempotency contract.
	cc := h.controlClient(t)
	p1 := h.prepareDirect(t, cc, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "k2", 16)
	p2 := h.prepareDirect(t, cc, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "k2", 16)
	if p1.GetPutId() == p2.GetPutId() {
		t.Fatalf("same request id must allocate a new put_id: %q vs %q", p1.GetPutId(), p2.GetPutId())
	}
}

// Spec test 21: Commit retry after a simulated response loss is idempotent.
func TestCommitRetryAfterLoss(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.srv.DropNextCommitResponse()
	h.mustPut(t, "k", []byte("data"))
	if h.srv.CommitCalls != 2 {
		t.Fatalf("want 2 Commit calls (retry), got %d", h.srv.CommitCalls)
	}
	if got := h.get(t, "k"); !bytes.Equal(got.GetResults()[0].GetData(), []byte("data")) {
		t.Fatal("data must be readable after commit retry")
	}
}

// Spec test 22: pending-put expiry (PLAN_EXPIRED at validation, PUT_EXPIRED at commit).
func TestPendingPutExpiry(t *testing.T) {
	t.Run("expired plan", func(t *testing.T) {
		h := newHarness(t, harnessConfig{})
		h.srv.ExpireNextPlan()
		rep := h.put(t, chunk("k", []byte("data")))
		if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "PLAN_EXPIRED" {
			t.Fatalf("want PLAN_EXPIRED, got %+v", rep)
		}
		if h.srv.AbortCalls != 1 || len(h.plane.Writes()) != 0 {
			t.Fatal("expired plan must Abort before writing")
		}
	})
	t.Run("expired before commit", func(t *testing.T) {
		h := newHarness(t, harnessConfig{})
		cc := h.controlClient(t)
		prep := h.prepareDirect(t, cc, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "k", 16)
		h.srv.ExpirePending(prep.GetPutId())
		rep, err := cc.Commit(h.ctx, &kachev1.CommitPutBatchRequest{Namespace: testNamespace, PutId: prep.GetPutId(), Results: commitResultsFor(prep)})
		if err != nil || rep.GetCommitted() || rep.GetErrors()[0].GetCode() != "PUT_EXPIRED" {
			t.Fatalf("want PUT_EXPIRED, got rep=%+v err=%v", rep, err)
		}
	})
}

// Spec test 23: read failover across replica targets; one successful read per block.
func TestReadFailover(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Targets: []testserver.TargetSpec{
			{Endpoint: "127.0.0.1:7201", Transport: "tcp"},
			{Endpoint: "127.0.0.1:7202", Transport: "tcp"},
		},
	}})
	data := []byte("replicated payload")
	h.mustPut(t, "k", data)
	h.plane.FailReadEndpoint("127.0.0.1:7201")
	res := h.get(t, "k").GetResults()[0]
	if !res.GetHit() || !bytes.Equal(res.GetData(), data) {
		t.Fatalf("read must fail over to healthy replica: %+v", res)
	}
	var primary, secondary int
	for _, r := range h.plane.Reads() {
		switch r.Endpoint {
		case "127.0.0.1:7201":
			primary++
		case "127.0.0.1:7202":
			secondary++
		}
	}
	if primary != 1 || secondary != 1 {
		t.Fatalf("want one failed primary read then one successful replica read, got %d/%d", primary, secondary)
	}
}

// Spec test 24: transport fallback RDMA -> TCP via fallback_endpoint.
func TestTransportFallbackRdmaToTCP(t *testing.T) {
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Targets: []testserver.TargetSpec{{
			Endpoint: "rdma-primary:9000", Transport: "rdma", FallbackEndpoint: "tcp-fallback:9001",
		}},
	}})
	h.plane.FailWriteEndpoint("rdma-primary:9000")
	h.plane.FailReadEndpoint("rdma-primary:9000")
	data := []byte("fallback payload")
	h.mustPut(t, "k", data)
	var fellBack bool
	for _, w := range h.plane.Writes() {
		if w.Endpoint == "tcp-fallback:9001" && w.Transport == "tcp" {
			fellBack = true
		}
	}
	if !fellBack {
		t.Fatal("write must fall back to the TCP endpoint")
	}
	res := h.get(t, "k").GetResults()[0]
	if !res.GetHit() || !bytes.Equal(res.GetData(), data) {
		t.Fatalf("read must fall back to TCP: %+v", res)
	}
	if m := h.metrics(t).Snapshot().Counters[metrics.TransportFallbackTotal]; m < 2 {
		t.Fatalf("transport_fallback_total = %d, want >= 2 (write + read)", m)
	}
}

// Spec test 25: server caps can disable transport fallback even when local
// config enables it.
func TestFallbackDisabledByServerCaps(t *testing.T) {
	caps := testserver.DefaultCaps()
	caps.TransportFallbackEnabled = false
	h := newHarness(t, harnessConfig{server: testserver.Options{
		Caps: caps,
		Targets: []testserver.TargetSpec{{
			Endpoint: "rdma-primary:9000", Transport: "rdma", FallbackEndpoint: "tcp-fallback:9001",
		}},
	}})
	h.mustPut(t, "k", []byte("data")) // rdma healthy at write time
	h.plane.FailReadEndpoint("rdma-primary:9000")
	res := h.get(t, "k").GetResults()[0]
	if res.GetHit() || res.GetErrorCode() != "READ_TARGET_FAILED" {
		t.Fatalf("read must fail without fallback: %+v", res)
	}
	h.plane.FailWriteEndpoint("rdma-primary:9000")
	rep := h.put(t, chunk("k2", []byte("data")))
	if rep.GetCommitted() || rep.GetResults()[0].GetErrorCode() != "WRITE_TARGET_FAILED" {
		t.Fatalf("write must fail without fallback: %+v", rep)
	}
	if m := h.metrics(t).Snapshot().Counters[metrics.TransportFallbackTotal]; m != 0 {
		t.Fatalf("transport_fallback_total = %d, want 0", m)
	}
}

// Spec test 26: SHA-256 mismatch detection (stored hash and corrupted payload).
func TestHashMismatch(t *testing.T) {
	t.Run("stored hash corrupted", func(t *testing.T) {
		h := newHarness(t, harnessConfig{})
		h.mustPut(t, "k", []byte("data"))
		h.srv.CorruptHash(testNamespace, "k")
		res := h.get(t, "k").GetResults()[0]
		if res.GetHit() || res.GetErrorCode() != "HASH_MISMATCH" {
			t.Fatalf("want HASH_MISMATCH, got %+v", res)
		}
	})
	t.Run("payload corrupted", func(t *testing.T) {
		h := newHarness(t, harnessConfig{})
		h.mustPut(t, "k", []byte("data"))
		cc := h.controlClient(t)
		resolve, err := cc.Resolve(h.ctx, &kachev1.ResolveBatchRequest{Namespace: testNamespace, Keys: []string{"k"}})
		if err != nil || len(resolve.GetPlans()) != 1 {
			t.Fatalf("Resolve: %v", err)
		}
		h.plane.CorruptReadRef(resolve.GetPlans()[0].GetBlocks()[0].GetOpaqueRef())
		res := h.get(t, "k").GetResults()[0]
		if res.GetHit() || res.GetErrorCode() != "HASH_MISMATCH" {
			t.Fatalf("want HASH_MISMATCH for corrupted payload, got %+v", res)
		}
	})
}

// Spec test 28: concurrent PutBatch/GetBatch on one connector.
func TestConcurrentPutGet(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	shared := bytes.Repeat([]byte("shared"), 32)
	h.mustPut(t, "shared", shared)

	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("conc/k%d", w)
			data := bytes.Repeat([]byte{byte(w)}, 128)
			rep, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk(key, data)}})
			if err != nil || !rep.GetCommitted() {
				t.Errorf("worker %d put: rep=%+v err=%v", w, rep, err)
				return
			}
			got, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: []string{key, "shared"}})
			if err != nil {
				t.Errorf("worker %d get: %v", w, err)
				return
			}
			if !bytes.Equal(got.GetResults()[0].GetData(), data) || !bytes.Equal(got.GetResults()[1].GetData(), shared) {
				t.Errorf("worker %d data mismatch", w)
			}
		}(w)
	}
	wg.Wait()
}

// Spec acceptance 19: stress test proving bounded in-flight bytes.
func TestBoundedInflightStress(t *testing.T) {
	const inflightCap = 4096
	h := newHarness(t, harnessConfig{
		server: testserver.Options{Splitter: testserver.MultiBlockSplitter(256)},
		mutate: func(c *kvcache.Config) {
			c.Limits.MaxChunkBytes = 1024
			c.Limits.MaxInflightBytes = inflightCap
			c.Limits.MaxConcurrentBlocks = 8
		},
	})
	h.plane.SetWriteDelay(time.Millisecond)

	const workers, chunksPer = 8, 6
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var chunks []*kachev1.PutChunk
			var keys []string
			for c := 0; c < chunksPer; c++ {
				key := fmt.Sprintf("stress/w%d/c%d", w, c)
				data := bytes.Repeat([]byte{byte(w*31 + c)}, 1024)
				chunks = append(chunks, chunk(key, data))
				keys = append(keys, key)
			}
			rep, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: chunks})
			if err != nil || !rep.GetCommitted() {
				t.Errorf("worker %d put: rep=%+v err=%v", w, rep, err)
				return
			}
			got, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: keys})
			if err != nil {
				t.Errorf("worker %d get: %v", w, err)
				return
			}
			for i, r := range got.GetResults() {
				if !r.GetHit() || !bytes.Equal(r.GetData(), chunks[i].GetData()) {
					t.Errorf("worker %d chunk %d mismatch", w, i)
				}
			}
		}(w)
	}
	wg.Wait()

	inflight, maxSeen, capacity := h.poolStats(t)
	if capacity != inflightCap {
		t.Fatalf("pool capacity = %d, want %d", capacity, inflightCap)
	}
	if maxSeen > inflightCap {
		t.Fatalf("max in-flight %d exceeds bound %d", maxSeen, inflightCap)
	}
	if inflight != 0 {
		t.Fatalf("in-flight leak: %d bytes still reserved", inflight)
	}
	if got := len(h.plane.Writes()); got != workers*chunksPer*4 {
		t.Fatalf("want %d block writes, got %d", workers*chunksPer*4, got)
	}
}

// Spec acceptance 20: all required metrics exist and count.
func TestMetricsRegistered(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.mustPut(t, "k", []byte("data"))
	h.get(t, "k", "missing")
	snap := h.metrics(t).Snapshot()
	required := []string{
		metrics.PutBatchTotal, metrics.PutChunkTotal, metrics.PutBytesTotal,
		metrics.PutCommitTotal, metrics.PutAbortTotal,
		metrics.GetBatchTotal, metrics.GetChunkTotal, metrics.GetBytesTotal,
		metrics.GetHitTotal, metrics.GetMissTotal,
		metrics.TargetWriteFailures, metrics.TargetReadFailures, metrics.TransportFallbackTotal,
	}
	for _, name := range required {
		if _, ok := snap.Counters[name]; !ok {
			t.Fatalf("counter %s missing", name)
		}
	}
	for _, name := range []string{metrics.BufferPoolWaitSeconds, metrics.RequestDurationSeconds} {
		if _, ok := snap.Histograms[name]; !ok {
			t.Fatalf("histogram %s missing", name)
		}
	}
	if snap.Counters[metrics.PutBatchTotal] != 1 || snap.Counters[metrics.PutCommitTotal] != 1 {
		t.Fatalf("put counters wrong: %+v", snap.Counters)
	}
	if snap.Counters[metrics.GetHitTotal] != 1 || snap.Counters[metrics.GetMissTotal] != 1 {
		t.Fatalf("get counters wrong: %+v", snap.Counters)
	}
	if snap.Histograms[metrics.RequestDurationSeconds].Count < 2 {
		t.Fatal("request_duration_seconds not observed")
	}
}

func TestCloseIdempotentAndClosedCalls(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	if err := h.conn.Close(h.ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := h.conn.Close(h.ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: []string{"k"}}); errs.From(err) != errs.InvalidArgument {
		t.Fatalf("call after close: want INVALID_ARGUMENT, got %v", err)
	}
}
