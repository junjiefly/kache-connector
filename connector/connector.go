// Package kvcache implements the in-process KV Cache connector.
package kvcache

import (
	"context"
	"crypto/tls"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/junjiefly/kache-connector/internal/controlclient"
	"github.com/junjiefly/kache-connector/internal/dataplane"
	"github.com/junjiefly/kache-connector/internal/errs"
	"github.com/junjiefly/kache-connector/internal/ids"
	"github.com/junjiefly/kache-connector/internal/memory"
	"github.com/junjiefly/kache-connector/internal/metrics"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Connector is the stable public API. Implementations are safe for concurrent
// use from multiple goroutines.
type Connector interface {
	Caps(ctx context.Context) (*kachev1.GetCapsReply, error)
	Match(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error)
	Get(ctx context.Context, req *kachev1.GetBatchRequest) (*kachev1.GetBatchReply, error)
	Put(ctx context.Context, req *kachev1.PutBatchRequest) (*kachev1.PutBatchReply, error)
	Delete(ctx context.Context, req *kachev1.DeleteBatchRequest) (*kachev1.DeleteBatchReply, error)
	ListControlServers(ctx context.Context, req *kachev1.ListControlServersRequest) (*kachev1.ListControlServersReply, error)
	Close(ctx context.Context) error
}

type connector struct {
	cfg      Config
	opts     options
	ctrl     controlclient.Client
	sessions *dataplane.SessionManager
	pool     *memory.Pool
	metrics  *metrics.Registry
	orch     *dataplane.Orchestrator

	capsMu   sync.Mutex
	caps     *kachev1.GetCapsReply
	capsAt   time.Time
	capsCall *capsCall

	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
	stop   chan struct{}
}

// New constructs a connector. It validates configuration locally and does not
// contact the server; capabilities are fetched on first use.
func New(ctx context.Context, cfg Config, opts ...Option) (Connector, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := buildOptions(opts)
	if len(o.factories) == 0 {
		return nil, errs.New(errs.TransportUnavailable, "no transport factories injected; use WithFactories with a private transport adapter")
	}

	var token string
	if cfg.ControlPlane.AuthTokenEnv != "" {
		token = os.Getenv(cfg.ControlPlane.AuthTokenEnv)
	}
	var tlsCfg *tls.Config
	if cfg.ControlPlane.TLS.Enabled {
		t := cfg.ControlPlane.TLS
		loaded, err := controlclient.LoadTLS(t.CAFile, t.CertFile, t.KeyFile, t.ServerName)
		if err != nil {
			return nil, errs.Wrap(errs.InvalidArgument, err, "load TLS configuration")
		}
		tlsCfg = loaded
	}
	ctrl, err := controlclient.New(ctx, controlclient.Options{
		Endpoints: cfg.ControlPlane.Endpoints,
		Timeout:   cfg.controlTimeout(),
		Token:     token,
		TLS:       tlsCfg,
		Dialer:    o.dialer,
		Logger:    o.logger,
	})
	if err != nil {
		return nil, err
	}

	m := metrics.New()
	sessions := dataplane.NewSessionManager(o.factories, cfg.sessionIdleTimeout(), o.logger)
	pool := memory.New(cfg.Limits.MaxInflightBytes, m)
	orch := dataplane.NewOrchestrator(ctrl, sessions, pool, dataplane.Settings{
		Mode:                cfg.Transport.Mode,
		MaxConcurrentBlocks: cfg.Limits.MaxConcurrentBlocks,
		Logger:              o.logger,
		Clock:               o.clock,
		Metrics:             m,
	})
	c := &connector{
		cfg: cfg, opts: o, ctrl: ctrl, sessions: sessions,
		pool: pool, metrics: m, orch: orch, stop: make(chan struct{}),
	}
	go c.reapSessions()
	return c, nil
}

// Metrics exposes the connector metric registry (read-only snapshots).
func (c *connector) Metrics() *metrics.Registry { return c.metrics }

// PoolStats exposes buffer-pool high-water marks for tests and diagnostics.
func (c *connector) PoolStats() (inflight, maxSeen, capacity int64) {
	return c.pool.InflightBytes(), c.pool.MaxSeenBytes(), c.pool.CapacityBytes()
}

func (c *connector) reapSessions() {
	t := time.NewTicker(c.cfg.sessionIdleTimeout() / 2)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case now := <-t.C:
			c.sessions.CloseIdle(now)
		}
	}
}

func (c *connector) enter() error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errs.New(errs.InvalidArgument, "connector is closed")
	}
	c.wg.Add(1)
	return nil
}

func (c *connector) leave() {
	c.wg.Done()
	c.mu.RUnlock()
}

func (c *connector) observe(start time.Time) {
	c.metrics.Observe(metrics.RequestDurationSeconds, c.opts.clock().Sub(start).Seconds())
}

// capsCall is one in-flight capabilities refresh shared by all waiters.
type capsCall struct {
	done chan struct{}
	rep  *kachev1.GetCapsReply
	err  error
}

// serverCaps returns cached capabilities, refreshing when the window elapsed.
// The refresh RPC runs outside capsMu so concurrent batches share one call
// instead of serializing behind it.
func (c *connector) serverCaps(ctx context.Context) (*kachev1.GetCapsReply, error) {
	c.capsMu.Lock()
	refresh := c.cfg.capsRefresh()
	if c.caps != nil && (refresh == 0 || c.opts.clock().Sub(c.capsAt) < refresh) {
		rep := c.caps
		c.capsMu.Unlock()
		return rep, nil
	}
	if call := c.capsCall; call != nil {
		c.capsMu.Unlock()
		select {
		case <-call.done:
			return call.rep, call.err
		case <-ctx.Done():
			return nil, errs.Wrap(errs.TransportUnavailable, ctx.Err(), "fetch capabilities")
		}
	}
	call := &capsCall{done: make(chan struct{})}
	c.capsCall = call
	c.capsMu.Unlock()

	call.rep, call.err = c.ctrl.Caps(ctx, c.cfg.Namespace)
	if call.err != nil {
		call.err = errs.Wrap(errs.TransportUnavailable, call.err, "fetch capabilities")
	} else if call.rep.GetProtocolVersion() != 1 {
		call.err = errs.New(errs.TransportUnavailable, "unsupported control protocol version %d", call.rep.GetProtocolVersion())
	}
	c.capsMu.Lock()
	if call.err == nil {
		c.caps = call.rep
		c.capsAt = c.opts.clock()
	}
	c.capsCall = nil
	c.capsMu.Unlock()
	close(call.done)
	return call.rep, call.err
}

func (c *connector) effectiveLimits(ctx context.Context) (dataplane.Limits, error) {
	caps, err := c.serverCaps(ctx)
	if err != nil {
		return dataplane.Limits{}, err
	}
	lim := dataplane.Limits{
		MaxChunkBytes:       c.cfg.Limits.MaxChunkBytes,
		MaxBatchChunks:      c.cfg.Limits.MaxBatchChunks,
		MaxConcurrentBlocks: c.cfg.Limits.MaxConcurrentBlocks,
		FallbackEnabled:     c.cfg.Transport.FallbackEnabled && caps.GetTransportFallbackEnabled(),
	}
	if v := int64(caps.GetMaxChunkBytes()); v > 0 && v < lim.MaxChunkBytes {
		lim.MaxChunkBytes = v
	}
	if v := int(caps.GetMaxBatchChunks()); v > 0 && v < lim.MaxBatchChunks {
		lim.MaxBatchChunks = v
	}
	if v := int64(caps.GetMaxBatchBytes()); v > 0 && (lim.MaxBatchBytes == 0 || v < lim.MaxBatchBytes) {
		lim.MaxBatchBytes = v
	}
	if v := int(caps.GetMaxConcurrentBlocks()); v > 0 && v < lim.MaxConcurrentBlocks {
		lim.MaxConcurrentBlocks = v
	}
	if v := caps.GetMaxWireBlockBytes(); v > 0 {
		lim.MaxWireBlockBytes = v
	}
	if local := c.cfg.Limits.MaxWireBlockBytes; local > 0 && (lim.MaxWireBlockBytes == 0 || local < int64(lim.MaxWireBlockBytes)) {
		lim.MaxWireBlockBytes = uint32(local)
	}
	return lim, nil
}

func (c *connector) checkNamespace(ns string) error {
	if ns != "" && ns != c.cfg.Namespace {
		return errs.New(errs.InvalidArgument, "namespace %q does not match configured namespace %q", ns, c.cfg.Namespace)
	}
	return nil
}

func validateKeys(keys []string, maxChunks int) error {
	if len(keys) == 0 {
		return errs.New(errs.InvalidArgument, "empty batch")
	}
	if len(keys) > maxChunks {
		return errs.New(errs.BatchTooLarge, "batch of %d keys exceeds limit %d", len(keys), maxChunks)
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k == "" {
			return errs.New(errs.InvalidArgument, "empty key")
		}
		if seen[k] {
			return errs.New(errs.InvalidArgument, "duplicate key %q", k)
		}
		seen[k] = true
	}
	return nil
}

func (c *connector) Caps(ctx context.Context) (*kachev1.GetCapsReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	defer c.observe(c.opts.clock())
	rep, err := c.serverCaps(ctx)
	if err != nil {
		return nil, err
	}
	return proto.Clone(rep).(*kachev1.GetCapsReply), nil
}

func (c *connector) Match(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	defer c.observe(c.opts.clock())
	if err := c.checkNamespace(req.GetNamespace()); err != nil {
		return nil, err
	}
	lim, err := c.effectiveLimits(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateKeys(req.GetKeys(), lim.MaxBatchChunks); err != nil {
		return nil, err
	}
	clone := proto.Clone(req).(*kachev1.MatchBatchRequest)
	clone.Namespace = c.cfg.Namespace
	rep, err := c.ctrl.Match(ctx, clone)
	if err != nil {
		return nil, err
	}
	if err := checkResultKeys(req.GetKeys(), len(rep.GetResults()), func(i int) string { return rep.GetResults()[i].GetKey() }); err != nil {
		return nil, err
	}
	return rep, nil
}

func (c *connector) Get(ctx context.Context, req *kachev1.GetBatchRequest) (*kachev1.GetBatchReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	start := c.opts.clock()
	defer c.observe(start)
	if err := c.checkNamespace(req.GetNamespace()); err != nil {
		return nil, err
	}
	lim, err := c.effectiveLimits(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateKeys(req.GetKeys(), lim.MaxBatchChunks); err != nil {
		return nil, err
	}
	clone := proto.Clone(req).(*kachev1.GetBatchRequest)
	clone.Namespace = c.cfg.Namespace
	rep, err := c.orch.Get(ctx, clone, lim)
	if err != nil {
		return nil, err
	}
	c.metrics.Inc(metrics.GetBatchTotal, 1)
	c.metrics.Inc(metrics.GetChunkTotal, uint64(len(req.GetKeys())))
	for _, r := range rep.GetResults() {
		if r.GetHit() {
			c.metrics.Inc(metrics.GetHitTotal, 1)
			c.metrics.Inc(metrics.GetBytesTotal, r.GetSizeBytes())
		} else if code := r.GetErrorCode(); code == "" || code == string(errs.KeyNotFound) {
			c.metrics.Inc(metrics.GetMissTotal, 1)
		}
	}
	return rep, nil
}

func (c *connector) Put(ctx context.Context, req *kachev1.PutBatchRequest) (*kachev1.PutBatchReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	start := c.opts.clock()
	defer c.observe(start)
	if err := c.checkNamespace(req.GetNamespace()); err != nil {
		return nil, err
	}
	lim, err := c.effectiveLimits(ctx)
	if err != nil {
		return nil, err
	}
	if err := validatePut(req, lim); err != nil {
		return nil, err
	}
	clone := proto.Clone(req).(*kachev1.PutBatchRequest)
	clone.Namespace = c.cfg.Namespace
	if clone.GetClientRequestId() == "" {
		clone.ClientRequestId = ids.NewUUID()
	}
	var totalBytes uint64
	for _, ch := range clone.GetChunks() {
		totalBytes += uint64(len(ch.GetData()))
	}
	rep, err := c.orch.Put(ctx, clone, lim)
	if err != nil {
		return nil, err
	}
	c.metrics.Inc(metrics.PutBatchTotal, 1)
	c.metrics.Inc(metrics.PutChunkTotal, uint64(len(clone.GetChunks())))
	c.metrics.Inc(metrics.PutBytesTotal, totalBytes)
	return rep, nil
}

func validatePut(req *kachev1.PutBatchRequest, lim dataplane.Limits) error {
	chunks := req.GetChunks()
	if len(chunks) == 0 {
		return errs.New(errs.InvalidArgument, "empty batch")
	}
	if len(chunks) > lim.MaxBatchChunks {
		return errs.New(errs.BatchTooLarge, "batch of %d chunks exceeds limit %d", len(chunks), lim.MaxBatchChunks)
	}
	if id := req.GetClientRequestId(); id != "" && !ids.IsUUID(id) {
		return errs.New(errs.InvalidArgument, "client_request_id must be a UUID")
	}
	seen := make(map[string]bool, len(chunks))
	var total int64
	for _, ch := range chunks {
		if ch.GetKey() == "" {
			return errs.New(errs.InvalidArgument, "empty key")
		}
		if seen[ch.GetKey()] {
			return errs.New(errs.InvalidArgument, "duplicate key %q", ch.GetKey())
		}
		seen[ch.GetKey()] = true
		size := int64(len(ch.GetData()))
		if size == 0 {
			return errs.New(errs.InvalidArgument, "zero-size chunk for key %q", ch.GetKey())
		}
		if ch.GetSizeBytes() != uint64(size) {
			return errs.New(errs.PayloadLengthMismatch, "key %q: size_bytes %d != payload length %d", ch.GetKey(), ch.GetSizeBytes(), size)
		}
		if size > lim.MaxChunkBytes {
			return errs.New(errs.ChunkTooLarge, "key %q: chunk of %d bytes exceeds limit %d", ch.GetKey(), size, lim.MaxChunkBytes)
		}
		if h := ch.GetContentHash(); len(h) != 0 && len(h) != 32 {
			return errs.New(errs.InvalidArgument, "key %q: content_hash must be 32 bytes", ch.GetKey())
		}
		total += size
	}
	if lim.MaxBatchBytes > 0 && total > lim.MaxBatchBytes {
		return errs.New(errs.BatchTooLarge, "batch of %d bytes exceeds limit %d", total, lim.MaxBatchBytes)
	}
	return nil
}

func (c *connector) Delete(ctx context.Context, req *kachev1.DeleteBatchRequest) (*kachev1.DeleteBatchReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	defer c.observe(c.opts.clock())
	if err := c.checkNamespace(req.GetNamespace()); err != nil {
		return nil, err
	}
	lim, err := c.effectiveLimits(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateKeys(req.GetKeys(), lim.MaxBatchChunks); err != nil {
		return nil, err
	}
	clone := proto.Clone(req).(*kachev1.DeleteBatchRequest)
	clone.Namespace = c.cfg.Namespace
	rep, err := c.ctrl.Delete(ctx, clone)
	if err != nil {
		return nil, err
	}
	if err := checkResultKeys(req.GetKeys(), len(rep.GetResults()), func(i int) string { return rep.GetResults()[i].GetKey() }); err != nil {
		return nil, err
	}
	return rep, nil
}

// ListControlServers exposes the Control node list so applications can
// rebalance endpoint config or verify cluster membership.
func (c *connector) ListControlServers(ctx context.Context, req *kachev1.ListControlServersRequest) (*kachev1.ListControlServersReply, error) {
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.leave()
	defer c.observe(c.opts.clock())
	if req == nil {
		req = &kachev1.ListControlServersRequest{}
	}
	if err := c.checkNamespace(req.GetNamespace()); err != nil {
		return nil, err
	}
	clone := proto.Clone(req).(*kachev1.ListControlServersRequest)
	clone.Namespace = c.cfg.Namespace
	return c.ctrl.ListServers(ctx, clone)
}

func checkResultKeys(keys []string, n int, keyAt func(int) string) error {
	if n != len(keys) {
		return errs.New(errs.Internal, "server returned %d results for %d keys", n, len(keys))
	}
	for i, k := range keys {
		if keyAt(i) != k {
			return errs.New(errs.Internal, "result %d key mismatch", i)
		}
	}
	return nil
}

func (c *connector) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	close(c.stop)

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		_ = c.sessions.Close(ctx)
		return c.ctrl.Close()
	case <-ctx.Done():
		// In-flight calls outlived the close deadline. Clean up once they
		// finish so sessions and control connections are not leaked; closing
		// them now would pull transports out from under active operations.
		go func() {
			<-done
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = c.sessions.Close(cctx)
			_ = c.ctrl.Close()
		}()
		return ctx.Err()
	}
}
