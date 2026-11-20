package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
)

func TestPoolAcquireRelease(t *testing.T) {
	p := New(1024, nil)
	b, err := p.Acquire(context.Background(), 512)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if len(b) != 512 {
		t.Fatalf("len = %d", len(b))
	}
	for i, v := range b {
		if v != 0 {
			t.Fatalf("buffer not zeroed at %d", i)
		}
	}
	if p.InflightBytes() != 512 {
		t.Fatalf("inflight = %d", p.InflightBytes())
	}
	p.Release(b)
	if p.InflightBytes() != 0 {
		t.Fatalf("inflight after release = %d", p.InflightBytes())
	}
}

func TestPoolRejectsOversize(t *testing.T) {
	p := New(1024, nil)
	_, err := p.Acquire(context.Background(), 2048)
	if errs.From(err) != errs.ChunkTooLarge {
		t.Fatalf("want CHUNK_TOO_LARGE, got %v", err)
	}
}

func TestPoolBackpressure(t *testing.T) {
	p := New(1024, nil)
	b, err := p.Acquire(context.Background(), 1024)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx, 1); err == nil {
		t.Fatal("Acquire should block until capacity frees or ctx done")
	}
	p.Release(b)
	if _, err := p.Acquire(context.Background(), 1024); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
}

func TestPoolBoundedUnderConcurrency(t *testing.T) {
	const capacity = 4096
	p := New(capacity, nil)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				b, err := p.Acquire(context.Background(), 512)
				if err != nil {
					t.Errorf("Acquire: %v", err)
					return
				}
				time.Sleep(time.Millisecond)
				p.Release(b)
			}
		}()
	}
	wg.Wait()
	if got := p.MaxSeenBytes(); got > capacity {
		t.Fatalf("max in-flight %d exceeds capacity %d", got, capacity)
	}
	if p.InflightBytes() != 0 {
		t.Fatalf("in-flight leak: %d", p.InflightBytes())
	}
}
