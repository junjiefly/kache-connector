package dataplane

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
)

type dialTransport struct{}

func (dialTransport) WriteBlocks(context.Context, []DataWriteRequest) ([]DataWriteResult, error) {
	return nil, nil
}
func (dialTransport) ReadBlocks(context.Context, []DataReadRequest) ([]DataReadResult, error) {
	return nil, nil
}
func (dialTransport) Close(context.Context) error { return nil }

type dialFactory struct {
	name  string
	opens atomic.Int32
	open  func(ctx context.Context, t DataTarget) (DataTransport, error)
}

func (f *dialFactory) Transport() string           { return f.name }
func (f *dialFactory) SupportsRefKind(string) bool { return true }
func (f *dialFactory) MaxBlockBytes() uint32       { return 1 << 20 }
func (f *dialFactory) Open(ctx context.Context, t DataTarget) (DataTransport, error) {
	f.opens.Add(1)
	return f.open(ctx, t)
}

func targetFor(transport, endpoint string) DataTarget {
	return DataTarget{TargetID: "t1", Endpoint: endpoint, Capability: []byte("cap"), Transport: transport}
}

// A slow dial for one endpoint must not block session acquisition for another.
func TestSlowDialDoesNotBlockOtherEndpoints(t *testing.T) {
	release := make(chan struct{})
	slow := &dialFactory{name: "tcp", open: func(ctx context.Context, _ DataTarget) (DataTransport, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return dialTransport{}, nil
	}}
	fast := &dialFactory{name: "rdma", open: func(context.Context, DataTarget) (DataTransport, error) {
		return dialTransport{}, nil
	}}
	m := NewSessionManager([]Factory{slow, fast}, time.Minute, nil)
	ctx := context.Background()

	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		_, rel, err := m.Acquire(ctx, targetFor("tcp", "10.0.0.1:1"))
		if err == nil {
			rel(nil)
		}
	}()
	time.Sleep(20 * time.Millisecond) // let the slow dial start

	done := make(chan error, 1)
	go func() {
		_, rel, err := m.Acquire(ctx, targetFor("rdma", "10.0.0.2:2"))
		if err == nil {
			rel(nil)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fast endpoint acquire: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("fast endpoint blocked behind slow dial")
	}
	close(release)
	<-slowDone
}

// Concurrent acquires for the same key dial exactly once and reuse the session.
func TestConcurrentAcquireDialsOnce(t *testing.T) {
	f := &dialFactory{name: "tcp", open: func(context.Context, DataTarget) (DataTransport, error) {
		time.Sleep(20 * time.Millisecond)
		return dialTransport{}, nil
	}}
	m := NewSessionManager([]Factory{f}, time.Minute, nil)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, rel, err := m.Acquire(context.Background(), targetFor("tcp", "10.0.0.3:3"))
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			rel(nil)
		}()
	}
	wg.Wait()
	if n := f.opens.Load(); n != 1 {
		t.Fatalf("dialed %d times, want 1", n)
	}
}

// Per-key dial locks must be reclaimed once their session is gone.
func TestKeyLocksReclaimed(t *testing.T) {
	f := &dialFactory{name: "tcp", open: func(context.Context, DataTarget) (DataTransport, error) {
		return dialTransport{}, nil
	}}
	m := NewSessionManager([]Factory{f}, time.Minute, nil)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		tgt := targetFor("tcp", "10.0.0.4:4")
		tgt.Capability = []byte{byte(i)}
		_, rel, err := m.Acquire(ctx, tgt)
		if err != nil {
			t.Fatal(err)
		}
		rel(nil)
	}
	m.CloseIdle(time.Now().Add(time.Hour))
	m.mu.Lock()
	sessions, locks := len(m.sessions), len(m.keyMu)
	m.mu.Unlock()
	if sessions != 0 || locks != 0 {
		t.Fatalf("after CloseIdle: sessions=%d keyMu=%d, want 0/0", sessions, locks)
	}

	// Error-discard path must also reclaim the lock.
	_, rel, err := m.Acquire(ctx, targetFor("tcp", "10.0.0.5:5"))
	if err != nil {
		t.Fatal(err)
	}
	rel(errs.New(errs.TransportUnavailable, "simulated failure"))
	m.mu.Lock()
	sessions, locks = len(m.sessions), len(m.keyMu)
	m.mu.Unlock()
	if sessions != 0 || locks != 0 {
		t.Fatalf("after error release: sessions=%d keyMu=%d, want 0/0", sessions, locks)
	}

	// Close with a live session must reclaim the lock too.
	_, rel, err = m.Acquire(ctx, targetFor("tcp", "10.0.0.6:6"))
	if err != nil {
		t.Fatal(err)
	}
	rel(nil)
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	locks = len(m.keyMu)
	m.mu.Unlock()
	if locks != 0 {
		t.Fatalf("after Close: keyMu=%d, want 0", locks)
	}
}
