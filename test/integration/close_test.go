package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Close with a deadline shorter than an in-flight operation must return the
// deadline error, let the operation finish safely, and stay idempotent.
func TestCloseTimeoutWithInflightPut(t *testing.T) {
	h := newHarness(t, harnessConfig{})
	h.plane.SetWriteDelay(300 * time.Millisecond)
	putDone := make(chan error, 1)
	go func() {
		_, err := h.conn.Put(context.Background(), &kachev1.PutBatchRequest{
			Chunks: []*kachev1.PutChunk{chunk("close-key", []byte("data"))},
		})
		putDone <- err
	}()
	// Wait until the write is recorded (happens before the delay) instead of
	// sleeping a fixed amount, which flakes under race-detector load.
	deadline := time.Now().Add(3 * time.Second)
	for len(h.plane.Writes()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Put never reached the data plane")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.conn.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close: want DeadlineExceeded, got %v", err)
	}
	select {
	case err := <-putDone:
		if err != nil {
			t.Fatalf("in-flight Put failed after Close timeout: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight Put never completed")
	}
	if err := h.conn.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
