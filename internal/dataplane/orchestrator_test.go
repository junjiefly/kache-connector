package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/internal/memory"
)

func budgetOrchestrator(capacity int64) *Orchestrator {
	return NewOrchestrator(nil, nil, memory.New(capacity, nil), Settings{})
}

// TestBatchBudgetBlocksSecondAcquire pins spec §13: one batch may hold at
// most half the pool, so its own concurrent acquisitions queue up.
func TestBatchBudgetBlocksSecondAcquire(t *testing.T) {
	o := budgetOrchestrator(200)
	budget, total := o.newBatchBudget()
	if total != 100 {
		t.Fatalf("budget total = %d, want 100", total)
	}
	ctx := context.Background()
	buf1, rel1, err := o.acquireBuf(ctx, budget, total, 80)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	_ = buf1
	done := make(chan error, 1)
	go func() {
		_, rel2, err := o.acquireBuf(ctx, budget, total, 80)
		if err == nil {
			rel2()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("second acquire should block on the batch budget, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	rel1()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second acquire after release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second acquire never completed after release")
	}
	if got := o.pool.InflightBytes(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
}

// TestBatchBudgetBypassForOversize: a single block larger than the budget
// must still be servable (it only needs to fit the pool itself).
func TestBatchBudgetBypassForOversize(t *testing.T) {
	o := budgetOrchestrator(200)
	budget, total := o.newBatchBudget()
	ctx := context.Background()
	_, rel1, err := o.acquireBuf(ctx, budget, total, 80)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	_, rel2, err := o.acquireBuf(ctx, budget, total, 120)
	if err != nil {
		t.Fatalf("oversize acquire should bypass the budget: %v", err)
	}
	rel2()
	rel1()
	if got := o.pool.InflightBytes(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
}
