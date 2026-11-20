package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

type stubFactory struct{ refKinds []string }

func (f stubFactory) Transport() string     { return "tcp" }
func (f stubFactory) MaxBlockBytes() uint32 { return 64 << 20 }
func (f stubFactory) SupportsRefKind(k string) bool {
	for _, r := range f.refKinds {
		if r == k {
			return true
		}
	}
	return false
}
func (f stubFactory) Open(ctx context.Context, target DataTarget) (DataTransport, error) {
	panic("not used in plan validation tests")
}

func lookupTCP(transport string) (Factory, bool) {
	if transport == "tcp" || transport == "rdma" {
		return stubFactory{refKinds: []string{"test_ref_v1"}}, true
	}
	return nil, false
}

func testLimits() Limits {
	return Limits{MaxChunkBytes: 1 << 20, MaxBatchChunks: 8, MaxConcurrentBlocks: 4}
}

func validBlock(id string, chunkIndex uint32, offset uint64, length uint32) *kachev1.PhysicalBlock {
	return &kachev1.PhysicalBlock{
		PlanBlockId: id,
		Kind:        kachev1.BlockKind_BLOCK_KIND_REGULAR,
		ChunkIndex:  chunkIndex,
		ChunkOffset: offset,
		LogicalLen:  length,
		WireLen:     length,
		OpaqueRef:   []byte{0x0a, 0x01, 0x78},
		RefKind:     "test_ref_v1",
		Targets: []*kachev1.DataTarget{{
			TargetId: "t0", Endpoint: "127.0.0.1:7101", Capability: []byte("cap"), Transport: "tcp",
		}},
	}
}

func validPrepare(blocks ...*kachev1.PhysicalBlock) *kachev1.PreparePutBatchReply {
	return &kachev1.PreparePutBatchReply{
		Accepted:         true,
		PutId:            "put-1",
		Blocks:           blocks,
		ExpiresAtUnixSec: time.Now().Add(time.Minute).Unix(),
	}
}

func chunks(sizes ...uint64) []*kachev1.PutChunk {
	out := make([]*kachev1.PutChunk, len(sizes))
	for i, s := range sizes {
		out[i] = &kachev1.PutChunk{Key: string(rune('a' + i)), SizeBytes: s, Data: make([]byte, s)}
	}
	return out
}

func TestWritePlanValid(t *testing.T) {
	prep := validPrepare(validBlock("b0", 0, 0, 10), validBlock("b1", 0, 10, 6))
	if err := validateWritePlan(prep, chunks(16), testLimits(), "auto", lookupTCP, time.Now()); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
}

func TestWritePlanCoverage(t *testing.T) {
	cases := []struct {
		name   string
		blocks []*kachev1.PhysicalBlock
	}{
		{"gap", []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10), validBlock("b1", 0, 12, 6)}},
		{"overlap", []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10), validBlock("b1", 0, 8, 8)}},
		{"unsorted", []*kachev1.PhysicalBlock{validBlock("b1", 0, 10, 6), validBlock("b0", 0, 0, 10)}},
		{"short coverage", []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10)}},
		{"long coverage", []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10), validBlock("b1", 0, 10, 8)}},
		{"no blocks", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWritePlan(validPrepare(tc.blocks...), chunks(16), testLimits(), "auto", lookupTCP, time.Now())
			if errs.From(err) != errs.InvalidWritePlan {
				t.Fatalf("want INVALID_WRITE_PLAN, got %v", err)
			}
		})
	}
}

func TestWritePlanStructuralRules(t *testing.T) {
	dupID := validBlock("b0", 0, 0, 16)
	dupTarget := validBlock("b0", 0, 0, 16)
	dupTarget.Targets = append(dupTarget.Targets, dupTarget.Targets[0])
	noTarget := validBlock("b0", 0, 0, 16)
	noTarget.Targets = nil
	badRef := validBlock("b0", 0, 0, 16)
	badRef.RefKind = "mystery_v9"
	emptyRef := validBlock("b0", 0, 0, 16)
	emptyRef.OpaqueRef = nil
	smallWire := validBlock("b0", 0, 0, 16)
	smallWire.WireLen = 8
	badIndex := validBlock("b0", 5, 0, 16)
	badKind := validBlock("b0", 0, 0, 16)
	badKind.Kind = kachev1.BlockKind_BLOCK_KIND_UNSPECIFIED
	noCap := validBlock("b0", 0, 0, 16)
	noCap.Targets[0].Capability = nil
	unknownTransport := validBlock("b0", 0, 0, 16)
	unknownTransport.Targets[0].Transport = "carrier-pigeon"

	cases := []struct {
		name string
		prep *kachev1.PreparePutBatchReply
		code errs.Code
	}{
		{"not accepted", &kachev1.PreparePutBatchReply{Accepted: false}, errs.InvalidWritePlan},
		{"empty put_id", &kachev1.PreparePutBatchReply{Accepted: true}, errs.InvalidWritePlan},
		{"expired", &kachev1.PreparePutBatchReply{Accepted: true, PutId: "p", ExpiresAtUnixSec: time.Now().Add(-time.Minute).Unix(), Blocks: []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 16)}}, errs.PlanExpired},
		{"duplicate plan_block_id", validPrepare(dupID, dupID), errs.InvalidWritePlan},
		{"duplicate target_id", validPrepare(dupTarget), errs.InvalidWritePlan},
		{"no targets", validPrepare(noTarget), errs.InvalidWritePlan},
		{"unsupported ref_kind", validPrepare(badRef), errs.UnsupportedRefKind},
		{"empty opaque_ref", validPrepare(emptyRef), errs.InvalidWritePlan},
		{"wire shorter than logical", validPrepare(smallWire), errs.InvalidWritePlan},
		{"chunk_index out of range", validPrepare(badIndex), errs.InvalidWritePlan},
		{"unspecified kind", validPrepare(badKind), errs.InvalidWritePlan},
		{"missing capability", validPrepare(noCap), errs.InvalidWritePlan},
		{"unknown transport", validPrepare(unknownTransport), errs.TransportUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWritePlan(tc.prep, chunks(16), testLimits(), "auto", lookupTCP, time.Now())
			if errs.From(err) != tc.code {
				t.Fatalf("want %s, got %v", tc.code, err)
			}
		})
	}
}

func TestWritePlanModeFiltering(t *testing.T) {
	rdmaOnly := validBlock("b0", 0, 0, 16)
	rdmaOnly.Targets[0].Transport = "rdma"
	err := validateWritePlan(validPrepare(rdmaOnly), chunks(16), testLimits(), "tcp", lookupTCP, time.Now())
	if errs.From(err) != errs.InvalidWritePlan {
		t.Fatalf("tcp mode must reject rdma-only write plan, got %v", err)
	}
	if err := validateWritePlan(validPrepare(rdmaOnly), chunks(16), testLimits(), "rdma", lookupTCP, time.Now()); err != nil {
		t.Fatalf("rdma mode must accept rdma plan: %v", err)
	}
}

func descBlock(id string, chunkIndex uint32, chunkSize uint64, payload []byte) *kachev1.PhysicalBlock {
	b := validBlock(id, chunkIndex, chunkSize, uint32(len(payload)))
	b.Kind = kachev1.BlockKind_BLOCK_KIND_DESC
	b.Payload = payload
	return b
}

func TestWritePlanDescBlock(t *testing.T) {
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	b0 := validBlock("b0", 0, 0, 10)
	b1 := validBlock("b1", 0, 10, 6)
	desc := descBlock("desc", 0, 16, payload)
	prep := validPrepare(b0, b1, desc)
	if err := validateWritePlan(prep, chunks(16), testLimits(), "auto", lookupTCP, time.Now()); err != nil {
		t.Fatalf("valid desc plan rejected: %v", err)
	}
}

func TestWritePlanDescEmptyPayload(t *testing.T) {
	b0 := validBlock("b0", 0, 0, 16)
	desc := descBlock("desc", 0, 16, nil)
	err := validateWritePlan(validPrepare(b0, desc), chunks(16), testLimits(), "auto", lookupTCP, time.Now())
	if errs.From(err) != errs.InvalidWritePlan {
		t.Fatalf("want INVALID_WRITE_PLAN for empty desc payload, got %v", err)
	}
}

func TestWritePlanDescMultiple(t *testing.T) {
	payload := []byte{0x01, 0x02}
	b0 := validBlock("b0", 0, 0, 16)
	d0 := descBlock("d0", 0, 16, payload)
	d1 := descBlock("d1", 0, 16, payload)
	err := validateWritePlan(validPrepare(b0, d0, d1), chunks(16), testLimits(), "auto", lookupTCP, time.Now())
	if errs.From(err) != errs.InvalidWritePlan {
		t.Fatalf("want INVALID_WRITE_PLAN for multiple desc blocks, got %v", err)
	}
}

func TestWritePlanWireLimit(t *testing.T) {
	lim := testLimits()
	lim.MaxWireBlockBytes = 8
	err := validateWritePlan(validPrepare(validBlock("b0", 0, 0, 16)), chunks(16), lim, "auto", lookupTCP, time.Now())
	if errs.From(err) != errs.InvalidWritePlan {
		t.Fatalf("want INVALID_WRITE_PLAN for oversized wire block, got %v", err)
	}
}

func TestReadPlanValidation(t *testing.T) {
	valid := &kachev1.ReadPlan{
		Key:       "k",
		SizeBytes: 16,
		Blocks:    []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10), validBlock("b1", 0, 10, 6)},
	}
	if err := validateReadPlan(valid, testLimits(), lookupTCP); err != nil {
		t.Fatalf("valid read plan rejected: %v", err)
	}

	gap := &kachev1.ReadPlan{Key: "k", SizeBytes: 16, Blocks: []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 10), validBlock("b1", 0, 12, 4)}}
	if err := validateReadPlan(gap, testLimits(), lookupTCP); errs.From(err) != errs.InvalidReadPlan {
		t.Fatalf("want INVALID_READ_PLAN for gap, got %v", err)
	}
	zero := &kachev1.ReadPlan{Key: "k"}
	if err := validateReadPlan(zero, testLimits(), lookupTCP); errs.From(err) != errs.InvalidReadPlan {
		t.Fatalf("want INVALID_READ_PLAN for zero size, got %v", err)
	}
	badHash := &kachev1.ReadPlan{Key: "k", SizeBytes: 16, Blocks: []*kachev1.PhysicalBlock{validBlock("b0", 0, 0, 16)}, ContentHash: []byte("short")}
	if err := validateReadPlan(badHash, testLimits(), lookupTCP); errs.From(err) != errs.InvalidReadPlan {
		t.Fatalf("want INVALID_READ_PLAN for short hash, got %v", err)
	}
}
