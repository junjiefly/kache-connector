package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Spec 搂8.5: if the caller's context is cancelled after Prepare, the connector
// must call Abort before returning cancellation. Spec 搂12: cancellation is
// terminal for the operation (no fallback or replica failover afterwards).
func TestCancelDuringWriteReturnsCancellation(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.plane.SetWriteDelay(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	rep, err := h.conn.Put(ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("cancel-w", []byte("data"))}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got err=%v rep=%+v", err, rep)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Put did not return promptly after cancellation")
	}
	if h.srv.AbortCalls == 0 || h.srv.CommitCalls != 0 {
		t.Fatalf("want Abort without Commit, got abort=%d commit=%d", h.srv.AbortCalls, h.srv.CommitCalls)
	}
}

func TestCancelDuringReadReturnsCancellation(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.mustPut(t, "cancel-r", []byte("data"))
	h.plane.SetReadDelay(5 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	start := time.Now()
	rep, err := h.conn.Get(ctx, &kachev1.GetBatchRequest{Keys: []string{"cancel-r"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got err=%v rep=%+v", err, rep)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Get did not return promptly after cancellation")
	}
}
