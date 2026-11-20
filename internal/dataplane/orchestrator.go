package dataplane

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/junjiefly/kache-connector/internal/controlclient"
	"github.com/junjiefly/kache-connector/internal/errs"
	"github.com/junjiefly/kache-connector/internal/ids"
	"github.com/junjiefly/kache-connector/internal/memory"
	"github.com/junjiefly/kache-connector/internal/metrics"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Settings control orchestration behavior shared by every transport backend.
// Transport fallback is gated per operation by Limits.FallbackEnabled, which
// the connector computes as local config AND server capabilities.
type Settings struct {
	Mode                string
	MaxConcurrentBlocks int
	Logger              *slog.Logger
	Clock               func() time.Time
	Metrics             *metrics.Registry
}

// Orchestrator executes server-issued write/read plans. It never interprets
// opaque refs; those are passed unchanged to the selected transport backend.
type Orchestrator struct {
	ctrl     controlclient.Client
	sessions *SessionManager
	pool     *memory.Pool
	s        Settings
}

func NewOrchestrator(ctrl controlclient.Client, sessions *SessionManager, pool *memory.Pool, s Settings) *Orchestrator {
	if s.Logger == nil {
		s.Logger = slog.New(slog.DiscardHandler)
	}
	if s.Clock == nil {
		s.Clock = time.Now
	}
	if s.Metrics == nil {
		s.Metrics = metrics.New()
	}
	if s.MaxConcurrentBlocks <= 0 {
		s.MaxConcurrentBlocks = 16
	}
	return &Orchestrator{ctrl: ctrl, sessions: sessions, pool: pool, s: s}
}

func (o *Orchestrator) lookup(transport string) (Factory, bool) {
	return o.sessions.Factory(transport)
}

// Put runs Prepare -> plan validation -> bounded replica writes -> Commit/Abort.
func (o *Orchestrator) Put(ctx context.Context, req *kachev1.PutBatchRequest, lim Limits) (*kachev1.PutBatchReply, error) {
	chunks := req.GetChunks()
	metas := make([]*kachev1.PutChunkMeta, len(chunks))
	for i, ch := range chunks {
		hash := ch.GetContentHash()
		if len(hash) == 0 {
			sum := sha256.Sum256(ch.GetData())
			hash = sum[:]
		}
		metas[i] = &kachev1.PutChunkMeta{
			Key:         ch.GetKey(),
			SizeBytes:   uint64(len(ch.GetData())),
			ContentHash: hash,
			Attributes:  ch.GetAttributes(),
			TtlSeconds:  ch.GetTtlSeconds(),
		}
	}
	prep, err := o.ctrl.Prepare(ctx, &kachev1.PreparePutBatchRequest{
		Namespace:       req.GetNamespace(),
		ClientRequestId: req.GetClientRequestId(),
		Chunks:          metas,
		Hints:           req.GetHints(),
	})
	if err != nil {
		return nil, err
	}
	if !prep.GetAccepted() {
		if prep.GetPutId() != "" {
			o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), "prepare rejected")
		}
		return rejectedReply(chunks, prep), nil
	}
	if err := validateWritePlan(prep, chunks, lim, o.s.Mode, o.lookup, o.s.Clock()); err != nil {
		o.s.Logger.Warn("invalid write plan", "put_id", prep.GetPutId(), "client_request_id", req.GetClientRequestId(), "code", errs.From(err), "detail", err.Error())
		o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), "invalid write plan")
		return failedReply(chunks, prep.GetPutId(), errs.From(err), "write plan rejected by validation"), nil
	}

	jobs := buildWriteJobs(prep, chunks)
	results, expired, err := o.executeWrites(ctx, jobs, prep.GetExpiresAtUnixSec(), lim)
	if err != nil {
		o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), "write execution cancelled")
		return nil, err
	}
	if expired || anyWriteFailed(results) {
		code := errs.WriteTargetFailed
		if expired {
			code = errs.PutExpired
		}
		o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), string(code))
		return failedReply(chunks, prep.GetPutId(), code, "data-plane write failed"), nil
	}

	commitReq := &kachev1.CommitPutBatchRequest{
		Namespace: req.GetNamespace(),
		PutId:     prep.GetPutId(),
		Results:   commitResults(prep, jobs, results),
	}
	commit, err := o.ctrl.Commit(ctx, commitReq)
	if err != nil {
		o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), "commit failed")
		// Raw transport errors can embed endpoints; keep them out of replies.
		return failedReply(chunks, prep.GetPutId(), errs.From(err), "commit request failed"), nil
	}
	if !commit.GetCommitted() {
		code, msg := firstPutError(commit.GetErrors(), errs.Internal)
		o.abort(ctx, req.GetNamespace(), prep.GetPutId(), req.GetClientRequestId(), "commit rejected")
		return failedReply(chunks, prep.GetPutId(), code, msg), nil
	}
	o.s.Metrics.Inc(metrics.PutCommitTotal, 1)
	return okReply(chunks, prep.GetPutId()), nil
}

type writeJob struct {
	block  *kachev1.PhysicalBlock
	target DataTarget
	chunk  *kachev1.PutChunk
}

func buildWriteJobs(prep *kachev1.PreparePutBatchReply, chunks []*kachev1.PutChunk) []writeJob {
	var jobs []writeJob
	for _, b := range prep.GetBlocks() {
		for _, t := range b.GetTargets() {
			jobs = append(jobs, writeJob{block: b, target: TargetFromProto(t), chunk: chunks[b.GetChunkIndex()]})
		}
	}
	return jobs
}

// newBatchBudget caps how many pool bytes one batch may hold at once, so a
// single large batch cannot occupy the whole pool (spec 搂13). Allocations
// larger than the budget bypass it: one oversized block/key must still fit.
func (o *Orchestrator) newBatchBudget() (*semaphore.Weighted, int64) {
	total := o.pool.CapacityBytes() / 2
	if total < 1 {
		total = 1
	}
	return semaphore.NewWeighted(total), total
}

func (o *Orchestrator) acquireBuf(ctx context.Context, budget *semaphore.Weighted, budgetTotal int64, n int) ([]byte, func(), error) {
	if int64(n) > budgetTotal {
		buf, err := o.pool.Acquire(ctx, n)
		if err != nil {
			return nil, nil, err
		}
		return buf, func() { o.pool.Release(buf) }, nil
	}
	if err := budget.Acquire(ctx, int64(n)); err != nil {
		return nil, nil, err
	}
	buf, err := o.pool.Acquire(ctx, n)
	if err != nil {
		budget.Release(int64(n))
		return nil, nil, err
	}
	return buf, func() {
		o.pool.Release(buf)
		budget.Release(int64(n))
	}, nil
}

func (o *Orchestrator) executeWrites(ctx context.Context, jobs []writeJob, expiresAt int64, lim Limits) ([]DataWriteResult, bool, error) {
	results := make([]DataWriteResult, len(jobs))
	var expired atomic.Bool
	budget, budgetTotal := o.newBatchBudget()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(lim.MaxConcurrentBlocks)
	for i := range jobs {
		i := i
		job := jobs[i]
		g.Go(func() error {
			if expired.Load() || (expiresAt != 0 && o.s.Clock().Unix() >= expiresAt) {
				expired.Store(true)
				results[i] = DataWriteResult{PlanBlockID: job.block.GetPlanBlockId(), TargetID: job.target.TargetID, ErrorCode: string(errs.PutExpired)}
				return nil
			}
			wire := int(job.block.GetWireLen())
			buf, releaseBuf, err := o.acquireBuf(gctx, budget, budgetTotal, wire)
			if err != nil {
				if gctx.Err() != nil {
					return gctx.Err()
				}
				results[i] = DataWriteResult{PlanBlockID: job.block.GetPlanBlockId(), TargetID: job.target.TargetID, ErrorCode: string(errs.WriteTargetFailed)}
				return nil
			}
			defer releaseBuf()
			if job.block.GetKind() == kachev1.BlockKind_BLOCK_KIND_DESC {
				copy(buf, job.block.GetPayload())
			} else {
				off := job.block.GetChunkOffset()
				logical := int(job.block.GetLogicalLen())
				copy(buf, job.chunk.GetData()[off:off+uint64(logical)])
			}
			// buf tail beyond logical is already zero-filled (wire padding).
			res, err := o.writeOne(gctx, job, buf, lim)
			if err != nil {
				return err
			}
			results[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, expired.Load(), err
	}
	return results, expired.Load(), nil
}

// writeOne reports caller cancellation as an error (terminal, spec 搂12);
// other target failures become a failed result so the batch can abort.
func (o *Orchestrator) writeOne(ctx context.Context, job writeJob, payload []byte, lim Limits) (DataWriteResult, error) {
	req := DataWriteRequest{
		PlanBlockID: job.block.GetPlanBlockId(),
		Target:      job.target,
		RefKind:     job.block.GetRefKind(),
		OpaqueRef:   job.block.GetOpaqueRef(),
		Offset:      0,
		Length:      uint32(len(payload)),
		Payload:     payload,
		RequestID:   ids.NewUUID(),
	}
	res, err := o.writeVia(ctx, job.target, req)
	if err == nil && res.OK {
		return res, nil
	}
	if ctx.Err() != nil {
		return DataWriteResult{}, ctx.Err()
	}
	if lim.FallbackEnabled && job.target.Transport == "rdma" && job.target.FallbackEndpoint != "" {
		fb := job.target
		fb.Endpoint = job.target.FallbackEndpoint
		fb.Transport = "tcp"
		fb.FallbackEndpoint = ""
		req.Target = fb
		o.s.Metrics.Inc(metrics.TransportFallbackTotal, 1)
		res, err = o.writeVia(ctx, fb, req)
		if err == nil && res.OK {
			return res, nil
		}
		if ctx.Err() != nil {
			return DataWriteResult{}, ctx.Err()
		}
	}
	o.s.Metrics.Inc(metrics.TargetWriteFailures, 1)
	return DataWriteResult{PlanBlockID: job.block.GetPlanBlockId(), TargetID: job.target.TargetID, ErrorCode: string(errs.WriteTargetFailed)}, nil
}

func (o *Orchestrator) writeVia(ctx context.Context, target DataTarget, req DataWriteRequest) (DataWriteResult, error) {
	tr, release, err := o.sessions.Acquire(ctx, target)
	if err != nil {
		return DataWriteResult{}, err
	}
	results, err := tr.WriteBlocks(ctx, []DataWriteRequest{req})
	release(err)
	if err != nil {
		return DataWriteResult{}, err
	}
	if len(results) != 1 {
		return DataWriteResult{}, errs.New(errs.Internal, "transport returned %d results for 1 request", len(results))
	}
	r := results[0]
	if r.OK && r.BytesWritten == 0 {
		r.BytesWritten = uint32(len(req.Payload))
	}
	return r, nil
}

func anyWriteFailed(results []DataWriteResult) bool {
	for _, r := range results {
		if !r.OK {
			return true
		}
	}
	return false
}

func commitResults(prep *kachev1.PreparePutBatchReply, jobs []writeJob, results []DataWriteResult) []*kachev1.BlockWriteResult {
	byBlock := map[string][]*kachev1.TargetWriteResult{}
	for i, job := range jobs {
		r := results[i]
		byBlock[job.block.GetPlanBlockId()] = append(byBlock[job.block.GetPlanBlockId()], &kachev1.TargetWriteResult{
			TargetId:     job.target.TargetID,
			Endpoint:     job.target.Endpoint,
			Ok:           r.OK,
			ErrorCode:    r.ErrorCode,
			BytesWritten: r.BytesWritten,
		})
	}
	out := make([]*kachev1.BlockWriteResult, 0, len(prep.GetBlocks()))
	for _, b := range prep.GetBlocks() {
		out = append(out, &kachev1.BlockWriteResult{PlanBlockId: b.GetPlanBlockId(), Results: byBlock[b.GetPlanBlockId()]})
	}
	return out
}

// abort is best-effort and idempotent; it runs even when ctx is cancelled.
func (o *Orchestrator) abort(ctx context.Context, namespace, putID, requestID, reason string) {
	if putID == "" {
		return
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	_, err := o.ctrl.Abort(actx, &kachev1.AbortPutBatchRequest{Namespace: namespace, PutId: putID, Reason: reason})
	o.s.Metrics.Inc(metrics.PutAbortTotal, 1)
	if err != nil {
		o.s.Logger.Warn("abort failed; pending lease left to server expiry", "put_id", putID, "client_request_id", requestID, "code", errs.From(err))
	}
}

func rejectedReply(chunks []*kachev1.PutChunk, prep *kachev1.PreparePutBatchReply) *kachev1.PutBatchReply {
	byIndex := map[uint32]*kachev1.PutError{}
	for _, e := range prep.GetErrors() {
		byIndex[e.GetChunkIndex()] = e
	}
	results := make([]*kachev1.PutResult, len(chunks))
	for i, ch := range chunks {
		code, msg := string(errs.InvalidArgument), "prepare rejected"
		if e, ok := byIndex[uint32(i)]; ok {
			code, msg = e.GetCode(), e.GetMessage()
		}
		results[i] = &kachev1.PutResult{Key: ch.GetKey(), Ok: false, ErrorCode: code, Message: msg}
	}
	return &kachev1.PutBatchReply{Committed: false, PutId: prep.GetPutId(), Results: results}
}

func failedReply(chunks []*kachev1.PutChunk, putID string, code errs.Code, msg string) *kachev1.PutBatchReply {
	results := make([]*kachev1.PutResult, len(chunks))
	for i, ch := range chunks {
		results[i] = &kachev1.PutResult{Key: ch.GetKey(), Ok: false, ErrorCode: string(code), Message: msg}
	}
	return &kachev1.PutBatchReply{Committed: false, PutId: putID, Results: results}
}

func okReply(chunks []*kachev1.PutChunk, putID string) *kachev1.PutBatchReply {
	results := make([]*kachev1.PutResult, len(chunks))
	for i, ch := range chunks {
		results[i] = &kachev1.PutResult{Key: ch.GetKey(), Ok: true}
	}
	return &kachev1.PutBatchReply{Committed: true, PutId: putID, Results: results}
}

func firstPutError(errors []*kachev1.PutError, fallback errs.Code) (errs.Code, string) {
	for _, e := range errors {
		if e.GetCode() != "" {
			return errs.Code(e.GetCode()), e.GetMessage()
		}
	}
	return fallback, "commit rejected"
}

// Get runs Resolve -> read-plan validation -> replica reads -> assembly/hash.
func (o *Orchestrator) Get(ctx context.Context, req *kachev1.GetBatchRequest, lim Limits) (*kachev1.GetBatchReply, error) {
	rep, err := o.ctrl.Resolve(ctx, &kachev1.ResolveBatchRequest{
		Namespace: req.GetNamespace(),
		Keys:      req.GetKeys(),
		Hints:     req.GetHints(),
	})
	if err != nil {
		return nil, err
	}
	plans := map[string]*kachev1.ReadPlan{}
	for _, p := range rep.GetPlans() {
		if _, dup := plans[p.GetKey()]; dup {
			return nil, errs.New(errs.Internal, "duplicate plan for key %q", p.GetKey())
		}
		plans[p.GetKey()] = p
	}
	misses := map[string]*kachev1.MatchResult{}
	for _, m := range rep.GetMisses() {
		if _, dup := misses[m.GetKey()]; dup {
			return nil, errs.New(errs.Internal, "duplicate miss for key %q", m.GetKey())
		}
		misses[m.GetKey()] = m
	}
	keys := req.GetKeys()
	for _, k := range keys {
		_, hasPlan := plans[k]
		_, hasMiss := misses[k]
		if hasPlan == hasMiss {
			return nil, errs.New(errs.Internal, "resolve reply must contain exactly one outcome for key %q", k)
		}
	}
	results := make([]*kachev1.GetResult, len(keys))
	budget, budgetTotal := o.newBatchBudget()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(lim.MaxConcurrentBlocks)
	for i := range keys {
		i := i
		key := keys[i]
		g.Go(func() error {
			if m, ok := misses[key]; ok {
				code := m.GetErrorCode()
				if code == "" {
					code = string(errs.KeyNotFound)
				}
				results[i] = &kachev1.GetResult{Key: key, Hit: false, ErrorCode: code}
				return nil
			}
			res, err := o.readKey(gctx, plans[key], lim, budget, budgetTotal)
			if err != nil {
				return err
			}
			results[i] = res
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return &kachev1.GetBatchReply{Results: results}, nil
}

func (o *Orchestrator) readKey(ctx context.Context, plan *kachev1.ReadPlan, lim Limits, budget *semaphore.Weighted, budgetTotal int64) (*kachev1.GetResult, error) {
	key := plan.GetKey()
	if err := validateReadPlan(plan, lim, o.lookup); err != nil {
		o.s.Logger.Warn("invalid read plan", "key", key, "code", errs.InvalidReadPlan, "detail", err.Error())
		return &kachev1.GetResult{Key: key, Hit: false, ErrorCode: string(errs.InvalidReadPlan), Message: "read plan rejected by validation"}, nil
	}
	size := int(plan.GetSizeBytes())
	buf, releaseBuf, err := o.acquireBuf(ctx, budget, budgetTotal, size)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return &kachev1.GetResult{Key: key, Hit: false, ErrorCode: string(errs.ReadTargetFailed), Message: "buffer pool exhausted"}, nil
	}
	defer releaseBuf()
	for _, b := range plan.GetBlocks() {
		data, err := o.readBlock(ctx, b, lim)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &kachev1.GetResult{Key: key, Hit: false, ErrorCode: string(errs.ReadTargetFailed), Message: "all targets failed for block"}, nil
		}
		copy(buf[b.GetChunkOffset():], data)
	}
	if h := plan.GetContentHash(); len(h) == 32 {
		sum := sha256.Sum256(buf)
		if string(sum[:]) != string(h) {
			return &kachev1.GetResult{Key: key, Hit: false, ErrorCode: string(errs.HashMismatch), Message: "content hash mismatch"}, nil
		}
	}
	return &kachev1.GetResult{
		Key:                 key,
		Hit:                 true,
		SizeBytes:           plan.GetSizeBytes(),
		ContentHash:         plan.GetContentHash(),
		Attributes:          plan.GetAttributes(),
		Data:                append([]byte(nil), buf...),
		LastModifiedUnixSec: plan.GetLastModifiedUnixSec(),
	}, nil
}

// readBlock reads one physical block from the first healthy target, trying
// transport fallback before moving to the next replica target. Caller
// cancellation stops failover immediately and is returned as an error.
func (o *Orchestrator) readBlock(ctx context.Context, b *kachev1.PhysicalBlock, lim Limits) ([]byte, error) {
	for _, tp := range b.GetTargets() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		target := TargetFromProto(tp)
		if !modeAllows(o.s.Mode, target.Transport) {
			continue
		}
		data, ok := o.readViaTarget(ctx, b, target, lim)
		if ok {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if lim.FallbackEnabled && target.Transport == "rdma" && target.FallbackEndpoint != "" {
			fb := target
			fb.Endpoint = target.FallbackEndpoint
			fb.Transport = "tcp"
			fb.FallbackEndpoint = ""
			o.s.Metrics.Inc(metrics.TransportFallbackTotal, 1)
			if data, ok := o.readViaTarget(ctx, b, fb, lim); ok {
				return data, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, errs.New(errs.ReadTargetFailed, "no healthy target for block %s", b.GetPlanBlockId())
}

func (o *Orchestrator) readViaTarget(ctx context.Context, b *kachev1.PhysicalBlock, target DataTarget, lim Limits) ([]byte, bool) {
	req := DataReadRequest{
		PlanBlockID: b.GetPlanBlockId(),
		Target:      target,
		RefKind:     b.GetRefKind(),
		OpaqueRef:   b.GetOpaqueRef(),
		Offset:      0,
		Length:      b.GetWireLen(),
		RequestID:   ids.NewUUID(),
	}
	tr, release, err := o.sessions.Acquire(ctx, target)
	if err != nil {
		o.s.Metrics.Inc(metrics.TargetReadFailures, 1)
		return nil, false
	}
	results, err := tr.ReadBlocks(ctx, []DataReadRequest{req})
	release(err)
	if err != nil || len(results) != 1 || !results[0].OK || uint32(len(results[0].Data)) < b.GetWireLen() {
		o.s.Metrics.Inc(metrics.TargetReadFailures, 1)
		return nil, false
	}
	return results[0].Data[:b.GetLogicalLen()], true
}
