// Package memory provides the bounded buffer pool used by the data plane.
package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/junjiefly/kache-connector/internal/errs"
	"github.com/junjiefly/kache-connector/internal/metrics"
)

// Pool bounds in-flight payload bytes and recycles buffers. Acquire blocks
// until capacity is available or the context is done (backpressure).
type Pool struct {
	maxBytes int64
	sem      *semaphore.Weighted
	buffers  sync.Pool
	metrics  *metrics.Registry

	inflight atomic.Int64
	maxSeen  atomic.Int64
}

func New(maxBytes int64, m *metrics.Registry) *Pool {
	if m == nil {
		m = metrics.New()
	}
	return &Pool{
		maxBytes: maxBytes,
		sem:      semaphore.NewWeighted(maxBytes),
		metrics:  m,
	}
}

// Acquire reserves n bytes and returns a zeroed buffer of length n.
func (p *Pool) Acquire(ctx context.Context, n int) ([]byte, error) {
	if n <= 0 {
		return nil, errs.New(errs.InvalidArgument, "buffer size must be positive")
	}
	if int64(n) > p.maxBytes {
		return nil, errs.New(errs.ChunkTooLarge, "buffer of %d bytes exceeds pool capacity %d", n, p.maxBytes)
	}
	start := time.Now()
	if err := p.sem.Acquire(ctx, int64(n)); err != nil {
		return nil, err
	}
	p.metrics.Observe(metrics.BufferPoolWaitSeconds, time.Since(start).Seconds())
	cur := p.inflight.Add(int64(n))
	for {
		old := p.maxSeen.Load()
		if cur <= old || p.maxSeen.CompareAndSwap(old, cur) {
			break
		}
	}
	if b, ok := p.buffers.Get().([]byte); ok && cap(b) >= n {
		b = b[:n]
		clear(b)
		return b, nil
	}
	return make([]byte, n), nil
}

// Release returns a buffer acquired from Acquire.
func (p *Pool) Release(b []byte) {
	if b == nil {
		return
	}
	p.inflight.Add(-int64(len(b)))
	p.sem.Release(int64(len(b)))
	if cap(b) <= 1<<20 {
		p.buffers.Put(b[:cap(b)])
	}
}

func (p *Pool) InflightBytes() int64 { return p.inflight.Load() }

// MaxSeenBytes reports the high-water mark of reserved bytes (for tests).
func (p *Pool) MaxSeenBytes() int64 { return p.maxSeen.Load() }

func (p *Pool) CapacityBytes() int64 { return p.maxBytes }
