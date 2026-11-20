package testserver

import (
	"context"
	"testing"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

func prepareOne(t *testing.T, srv *Server, rid, key string) *kachev1.PreparePutBatchReply {
	t.Helper()
	rep, err := srv.PreparePutBatch(context.Background(), &kachev1.PreparePutBatchRequest{
		Namespace:       "ns",
		ClientRequestId: rid,
		Chunks:          []*kachev1.PutChunkMeta{{Key: key, SizeBytes: 4, ContentHash: make([]byte, 32)}},
	})
	if err != nil {
		t.Fatalf("prepare %s: %v", rid, err)
	}
	if !rep.GetAccepted() {
		t.Fatalf("prepare %s rejected: %v", rid, rep.GetErrors())
	}
	return rep
}

func commitReply(t *testing.T, srv *Server, rep *kachev1.PreparePutBatchReply) *kachev1.CommitPutBatchReply {
	t.Helper()
	var results []*kachev1.BlockWriteResult
	for _, blk := range rep.GetBlocks() {
		var trs []*kachev1.TargetWriteResult
		for _, tg := range blk.GetTargets() {
			trs = append(trs, &kachev1.TargetWriteResult{TargetId: tg.GetTargetId(), Ok: true})
		}
		results = append(results, &kachev1.BlockWriteResult{PlanBlockId: blk.GetPlanBlockId(), Results: trs})
	}
	cr, err := srv.CommitPutBatch(context.Background(), &kachev1.CommitPutBatchRequest{
		Namespace: "ns", PutId: rep.GetPutId(), Results: results,
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	return cr
}

func TestCommitRejectsOverwrite(t *testing.T) {
	srv := New(Options{})
	a := prepareOne(t, srv, "req-a", "k")
	b := prepareOne(t, srv, "req-b", "k")

	if cr := commitReply(t, srv, a); !cr.GetCommitted() {
		t.Fatalf("first commit rejected: %v", cr.GetErrors())
	}
	cr := commitReply(t, srv, b)
	if cr.GetCommitted() {
		t.Fatal("second commit overwrote a committed key")
	}
	if len(cr.GetErrors()) != 1 || cr.GetErrors()[0].GetCode() != "KEY_EXISTS" {
		t.Fatalf("want KEY_EXISTS, got %v", cr.GetErrors())
	}
	if cr := commitReply(t, srv, a); !cr.GetCommitted() {
		t.Fatalf("idempotent re-commit of first put failed: %v", cr.GetErrors())
	}
}
