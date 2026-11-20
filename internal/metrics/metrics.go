// Package metrics provides process-local counters and histograms.
package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// Required metric names from the connector specification.
const (
	PutBatchTotal          = "put_batch_total"
	PutChunkTotal          = "put_chunk_total"
	PutBytesTotal          = "put_bytes_total"
	PutCommitTotal         = "put_commit_total"
	PutAbortTotal          = "put_abort_total"
	GetBatchTotal          = "get_batch_total"
	GetChunkTotal          = "get_chunk_total"
	GetBytesTotal          = "get_bytes_total"
	GetHitTotal            = "get_hit_total"
	GetMissTotal           = "get_miss_total"
	TargetWriteFailures    = "target_write_failures"
	TargetReadFailures     = "target_read_failures"
	TransportFallbackTotal = "transport_fallback_total"
	BufferPoolWaitSeconds  = "buffer_pool_wait_seconds"
	RequestDurationSeconds = "request_duration_seconds"
)

var histogramBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type counter struct{ v atomic.Uint64 }

type histogram struct {
	mu      sync.Mutex
	count   uint64
	sum     float64
	buckets []uint64
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range histogramBuckets {
		if v <= b {
			h.buckets[i]++
		}
	}
}

// Registry holds all connector metrics. It is safe for concurrent use.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*counter
	histograms map[string]*histogram
}

func New() *Registry {
	r := &Registry{
		counters:   map[string]*counter{},
		histograms: map[string]*histogram{},
	}
	for _, name := range []string{
		PutBatchTotal, PutChunkTotal, PutBytesTotal, PutCommitTotal, PutAbortTotal,
		GetBatchTotal, GetChunkTotal, GetBytesTotal, GetHitTotal, GetMissTotal,
		TargetWriteFailures, TargetReadFailures, TransportFallbackTotal,
	} {
		r.counters[name] = &counter{}
	}
	for _, name := range []string{BufferPoolWaitSeconds, RequestDurationSeconds} {
		r.histograms[name] = &histogram{buckets: make([]uint64, len(histogramBuckets))}
	}
	return r
}

func (r *Registry) Inc(name string, delta uint64) {
	r.mu.RLock()
	c := r.counters[name]
	r.mu.RUnlock()
	if c != nil {
		c.v.Add(delta)
	}
}

func (r *Registry) Observe(name string, seconds float64) {
	r.mu.RLock()
	h := r.histograms[name]
	r.mu.RUnlock()
	if h != nil {
		h.observe(seconds)
	}
}

type Snapshot struct {
	Counters   map[string]uint64
	Histograms map[string]HistogramSnapshot
}

type HistogramSnapshot struct {
	Count   uint64
	Sum     float64
	Buckets map[float64]uint64
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := Snapshot{Counters: map[string]uint64{}, Histograms: map[string]HistogramSnapshot{}}
	for name, c := range r.counters {
		s.Counters[name] = c.v.Load()
	}
	for name, h := range r.histograms {
		h.mu.Lock()
		hs := HistogramSnapshot{Count: h.count, Sum: h.sum, Buckets: map[float64]uint64{}}
		for i, b := range histogramBuckets {
			hs.Buckets[b] = h.buckets[i]
		}
		hs.Buckets[math.Inf(1)] = h.count
		h.mu.Unlock()
		s.Histograms[name] = hs
	}
	return s
}

// Names returns all registered metric names in sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.counters)+len(r.histograms))
	for n := range r.counters {
		names = append(names, n)
	}
	for n := range r.histograms {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
