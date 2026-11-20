// Package testserver provides an in-memory KV Cache control-plane server and
// data-plane test doubles for local verification. It is not production logic.
package testserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// TargetSpec describes one replica target returned in plans.
type TargetSpec struct {
	Endpoint         string
	Transport        string
	Capability       []byte
	FallbackEndpoint string
}

// BlockSpec lets tests control the chunk-to-block split, including invalid plans.
type BlockSpec struct {
	PlanBlockID string
	ChunkIndex  int32 // -1 means use the real chunk index
	ChunkOffset uint64
	LogicalLen  uint32
	WireLen     uint32
	Kind        kachev1.BlockKind
	RefKind     string
	OpaqueRef   []byte // nil means the server generates an opaque test reference
	Targets     []TargetSpec
}

// Splitter maps a chunk (by index and size) to physical blocks.
type Splitter func(chunkIndex int, size uint64) []BlockSpec

type Options struct {
	Caps         *kachev1.GetCapsReply
	Targets      []TargetSpec
	Splitter     Splitter
	Clock        func() time.Time
	LeaseSeconds int64
	// RefKind is the generic ref discriminator used by the public test server.
	// Private adapters inject their own value when testing production encoding.
	RefKind string
	// MutatePrepare, when set, rewrites the physical blocks of every accepted
	// Prepare reply before it is stored and returned. Tests use it to inject
	// invalid write plans (gaps, duplicate target IDs, unsupported ref kinds).
	MutatePrepare func([]*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock
	// MakeRef creates an opaque backend reference. Production adapters inject
	// their own test encoder; the public test server never imports one.
	MakeRef func(blockID string) []byte
}

type pendingPut struct {
	namespace string
	requestID string
	metas     []*kachev1.PutChunkMeta
	blocks    []*kachev1.PhysicalBlock
	expiresAt int64
	reply     *kachev1.PreparePutBatchReply
}

// Server implements kachev1.KacheControlServer with injectable behavior.
type Server struct {
	kachev1.UnimplementedKacheControlServer

	mu        sync.Mutex
	store     *memoryStore
	pending   map[string]*pendingPut
	committed map[string]*kachev1.CommitPutBatchReply
	aborted   map[string]bool
	caps      *kachev1.GetCapsReply
	targets   []TargetSpec
	splitter  Splitter
	clock     func() time.Time
	lease     int64
	mutate    func([]*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock
	makeRef   func(blockID string) []byte
	refKind   string

	dropPrepareOnce int
	dropCommitOnce  int
	expireNextPlan  bool

	PrepareCalls int
	CommitCalls  int
	AbortCalls   int
	MatchCalls   int
	ResolveCalls int
	DeleteCalls  int
}

func DefaultCaps() *kachev1.GetCapsReply {
	return &kachev1.GetCapsReply{
		ProtocolVersion:          1,
		Namespace:                "default",
		PoolId:                   1,
		ReplicaCount:             2,
		RegularBlockSize:         4 << 20,
		MaxChunkBytes:            64 << 20,
		MaxBatchChunks:           64,
		MaxBatchBytes:            256 << 20,
		MaxBlocksPerBatch:        512,
		MaxConcurrentBlocks:      16,
		MaxWireBlockBytes:        64 << 20,
		SupportedTransports:      []string{"rdma", "tcp"},
		TransportFallbackEnabled: true,
		DedupEnabled:             false,
		DefaultTtlSeconds:        3600,
		MaxTtlSeconds:            7 * 24 * 3600,
		RequestIndexTtlSeconds:   3600,
	}
}

func DefaultTargets() []TargetSpec {
	return []TargetSpec{{Endpoint: "127.0.0.1:7101", Transport: "tcp", Capability: []byte("cap-1")}}
}

func New(opts Options) *Server {
	if opts.Caps == nil {
		opts.Caps = DefaultCaps()
	}
	if len(opts.Targets) == 0 {
		opts.Targets = DefaultTargets()
	}
	if opts.Splitter == nil {
		opts.Splitter = singleBlockSplitter
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.LeaseSeconds == 0 {
		opts.LeaseSeconds = 60
	}
	if opts.RefKind == "" {
		opts.RefKind = "test_ref_v1"
	}
	if opts.MakeRef == nil {
		opts.MakeRef = opaqueRef
	}
	return &Server{
		store:     newMemoryStore(),
		pending:   map[string]*pendingPut{},
		committed: map[string]*kachev1.CommitPutBatchReply{},
		aborted:   map[string]bool{},
		caps:      opts.Caps,
		targets:   opts.Targets,
		splitter:  opts.Splitter,
		clock:     opts.Clock,
		lease:     opts.LeaseSeconds,
		mutate:    opts.MutatePrepare,
		makeRef:   opts.MakeRef,
		refKind:   opts.RefKind,
	}
}

func singleBlockSplitter(chunkIndex int, size uint64) []BlockSpec {
	return []BlockSpec{{ChunkIndex: -1, ChunkOffset: 0, LogicalLen: uint32(size), WireLen: uint32(size)}}
}

// MultiBlockSplitter splits each chunk into fixed-size blocks.
func MultiBlockSplitter(blockSize uint32) Splitter {
	return func(chunkIndex int, size uint64) []BlockSpec {
		var out []BlockSpec
		var off uint64
		for off < size {
			n := uint64(blockSize)
			if size-off < n {
				n = size - off
			}
			out = append(out, BlockSpec{ChunkIndex: -1, ChunkOffset: off, LogicalLen: uint32(n), WireLen: uint32(n)})
			off += n
		}
		return out
	}
}

// DropNextPrepareResponse simulates a lost Prepare response after processing.
func (s *Server) DropNextPrepareResponse() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropPrepareOnce++
}

// DropNextCommitResponse simulates a lost Commit response after processing.
func (s *Server) DropNextCommitResponse() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropCommitOnce++
}

// ExpireNextPlan makes the next accepted Prepare reply already expired.
func (s *Server) ExpireNextPlan() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireNextPlan = true
}

// ExpirePending forces a pending put to expire immediately.
func (s *Server) ExpirePending(putID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.pending[putID]; p != nil {
		p.expiresAt = s.clock().Unix() - 1
	}
}

// CorruptHash changes the stored content hash of a committed key.
func (s *Server) CorruptHash(namespace, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.store.get(namespace, key); e != nil {
		e.hash = make([]byte, 32)
	}
}

// MutateBlocks lets tests corrupt a committed read plan.
func (s *Server) MutateBlocks(namespace, key string, fn func([]*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.store.get(namespace, key); e != nil {
		e.blocks = fn(e.blocks)
	}
}

// PendingPutID returns the put_id of the most recent pending put (test helper).
func (s *Server) PendingPutID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.pending {
		return id
	}
	return ""
}

func (s *Server) GetCaps(ctx context.Context, req *kachev1.GetCapsRequest) (*kachev1.GetCapsReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.caps).(*kachev1.GetCapsReply), nil
}

func (s *Server) MatchBatch(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MatchCalls++
	if req.GetNamespace() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty namespace")
	}
	results := make([]*kachev1.MatchResult, len(req.GetKeys()))
	for i, k := range req.GetKeys() {
		if s.store.visible(req.GetNamespace(), k) {
			e := s.store.get(req.GetNamespace(), k)
			results[i] = &kachev1.MatchResult{Key: k, Hit: true, Attributes: e.attributes}
		} else {
			results[i] = &kachev1.MatchResult{Key: k, Hit: false, ErrorCode: "KEY_NOT_FOUND"}
		}
	}
	return &kachev1.MatchBatchReply{Results: results}, nil
}

func (s *Server) ResolveBatch(ctx context.Context, req *kachev1.ResolveBatchRequest) (*kachev1.ResolveBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ResolveCalls++
	if req.GetNamespace() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty namespace")
	}
	rep := &kachev1.ResolveBatchReply{}
	for _, k := range req.GetKeys() {
		e := s.store.get(req.GetNamespace(), k)
		if e == nil || e.state != stateCommitted {
			rep.Misses = append(rep.Misses, &kachev1.MatchResult{Key: k, Hit: false, ErrorCode: "KEY_NOT_FOUND"})
			continue
		}
		rep.Plans = append(rep.Plans, &kachev1.ReadPlan{
			Key:                 k,
			SizeBytes:           e.size,
			Blocks:              cloneBlocks(e.blocks),
			Attributes:          e.attributes,
			ContentHash:         e.hash,
			LastModifiedUnixSec: e.lastModified,
		})
	}
	return rep, nil
}

func (s *Server) DeleteBatch(ctx context.Context, req *kachev1.DeleteBatchRequest) (*kachev1.DeleteBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DeleteCalls++
	if req.GetNamespace() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty namespace")
	}
	results := make([]*kachev1.DeleteResult, len(req.GetKeys()))
	for i, k := range req.GetKeys() {
		e := s.store.get(req.GetNamespace(), k)
		if e != nil && e.state == stateCommitted {
			e.state = stateDeleted
			results[i] = &kachev1.DeleteResult{Key: k, Deleted: true}
		} else {
			// Control treats a missing chunk as already deleted.
			results[i] = &kachev1.DeleteResult{Key: k, Deleted: true}
		}
	}
	return &kachev1.DeleteBatchReply{Results: results}, nil
}

func (s *Server) ListControlServers(ctx context.Context, req *kachev1.ListControlServersRequest) (*kachev1.ListControlServersReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &kachev1.ListControlServersReply{
		Servers: []*kachev1.ControlServer{{
			AdvertiseAddr:        "test-only",
			State:                "ACTIVE",
			LastHeartbeatUnixSec: s.clock().Unix(),
		}},
	}, nil
}

func (s *Server) PreparePutBatch(ctx context.Context, req *kachev1.PreparePutBatchRequest) (*kachev1.PreparePutBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.PrepareCalls++
	if req.GetNamespace() == "" || req.GetClientRequestId() == "" || len(req.GetChunks()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "namespace, client_request_id and chunks are required")
	}
	var putErrors []*kachev1.PutError
	seen := map[string]bool{}
	for i, ch := range req.GetChunks() {
		code := ""
		switch {
		case ch.GetKey() == "":
			code = "INVALID_ARGUMENT"
		case ch.GetSizeBytes() == 0:
			code = "INVALID_ARGUMENT"
		case seen[ch.GetKey()]:
			code = "INVALID_ARGUMENT"
		case len(ch.GetContentHash()) != 32:
			code = "INVALID_ARGUMENT"
		case s.store.visible(req.GetNamespace(), ch.GetKey()):
			code = "KEY_EXISTS"
		}
		seen[ch.GetKey()] = true
		if code != "" {
			putErrors = append(putErrors, &kachev1.PutError{ChunkIndex: uint32(i), Code: code, Message: "chunk rejected"})
		}
	}
	if len(putErrors) > 0 {
		return &kachev1.PreparePutBatchReply{Accepted: false, Errors: putErrors}, nil
	}

	putID := "put-" + randomHex(8)
	var blocks []*kachev1.PhysicalBlock
	for i, ch := range req.GetChunks() {
		for j, spec := range s.splitter(i, ch.GetSizeBytes()) {
			blocks = append(blocks, s.buildBlock(putID, i, j, spec))
		}
	}
	if s.mutate != nil {
		blocks = s.mutate(blocks)
	}
	expires := s.clock().Unix() + s.lease
	if s.expireNextPlan {
		s.expireNextPlan = false
		expires = s.clock().Unix() - 1
	}
	reply := &kachev1.PreparePutBatchReply{
		Accepted:         true,
		PutId:            putID,
		Blocks:           blocks,
		ExpiresAtUnixSec: expires,
	}
	p := &pendingPut{
		namespace: req.GetNamespace(),
		requestID: req.GetClientRequestId(),
		metas:     req.GetChunks(),
		blocks:    blocks,
		expiresAt: expires,
		reply:     reply,
	}
	s.pending[putID] = p
	return s.maybeDropPrepare(proto.Clone(reply).(*kachev1.PreparePutBatchReply))
}

func (s *Server) maybeDropPrepare(rep *kachev1.PreparePutBatchReply) (*kachev1.PreparePutBatchReply, error) {
	if s.dropPrepareOnce > 0 {
		s.dropPrepareOnce--
		return nil, status.Error(codes.Unavailable, "simulated lost prepare response")
	}
	return rep, nil
}

func (s *Server) buildBlock(putID string, chunkIndex, blockIndex int, spec BlockSpec) *kachev1.PhysicalBlock {
	idx := chunkIndex
	if spec.ChunkIndex >= 0 {
		idx = int(spec.ChunkIndex)
	}
	kind := spec.Kind
	if kind == kachev1.BlockKind_BLOCK_KIND_UNSPECIFIED {
		kind = kachev1.BlockKind_BLOCK_KIND_REGULAR
	}
	refKind := spec.RefKind
	if refKind == "" {
		refKind = s.refKind
	}
	ref := spec.OpaqueRef
	if ref == nil {
		ref = s.makeRef(fmt.Sprintf("%s-b%d-%d", putID, chunkIndex, blockIndex))
	}
	targets := spec.Targets
	if targets == nil {
		targets = s.targets
	}
	var protoTargets []*kachev1.DataTarget
	for ti, t := range targets {
		capability := t.Capability
		if len(capability) == 0 {
			capability = []byte(fmt.Sprintf("cap-%s-%d", t.Endpoint, ti))
		}
		protoTargets = append(protoTargets, &kachev1.DataTarget{
			TargetId:         fmt.Sprintf("t-%d-%s", ti, t.Endpoint),
			Endpoint:         t.Endpoint,
			Capability:       capability,
			Transport:        t.Transport,
			FallbackEndpoint: t.FallbackEndpoint,
		})
	}
	id := spec.PlanBlockID
	if id == "" {
		id = fmt.Sprintf("%s-b%d-%d", putID, chunkIndex, blockIndex)
	}
	wire := spec.WireLen
	if wire == 0 {
		wire = spec.LogicalLen
	}
	return &kachev1.PhysicalBlock{
		PlanBlockId: id,
		Kind:        kind,
		ChunkIndex:  uint32(idx),
		ChunkOffset: spec.ChunkOffset,
		LogicalLen:  spec.LogicalLen,
		WireLen:     wire,
		OpaqueRef:   ref,
		Targets:     protoTargets,
		RefKind:     refKind,
	}
}

func (s *Server) CommitPutBatch(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CommitCalls++
	if rep, ok := s.committed[req.GetPutId()]; ok {
		return s.maybeDropCommit(proto.Clone(rep).(*kachev1.CommitPutBatchReply))
	}
	p, ok := s.pending[req.GetPutId()]
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "PUT_NOT_PREPARED")
	}
	if s.clock().Unix() >= p.expiresAt {
		delete(s.pending, req.GetPutId())
		return &kachev1.CommitPutBatchReply{Committed: false, Errors: []*kachev1.PutError{{Code: "PUT_EXPIRED", Message: "pending put expired"}}}, nil
	}
	if err := validateCommit(p, req); err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.Internal {
			return nil, err
		}
		return &kachev1.CommitPutBatchReply{Committed: false, Errors: []*kachev1.PutError{{Code: "INVALID_ARGUMENT", Message: err.Error()}}}, nil
	}
	// Another put may have committed the same key while this one was pending;
	// overwrites are rejected (spec 15.1).
	var exists []*kachev1.PutError
	for i, meta := range p.metas {
		if s.store.visible(p.namespace, meta.GetKey()) {
			exists = append(exists, &kachev1.PutError{ChunkIndex: uint32(i), Code: "KEY_EXISTS", Message: "key committed by another put"})
		}
	}
	if len(exists) > 0 {
		return &kachev1.CommitPutBatchReply{Committed: false, Errors: exists}, nil
	}
	now := s.clock().Unix()
	var keys []string
	for i, meta := range p.metas {
		var blocks []*kachev1.PhysicalBlock
		for _, b := range p.blocks {
			if int(b.GetChunkIndex()) == i {
				blocks = append(blocks, b)
			}
		}
		s.store.put(p.namespace, &storedEntry{
			key:          meta.GetKey(),
			size:         meta.GetSizeBytes(),
			hash:         meta.GetContentHash(),
			attributes:   meta.GetAttributes(),
			blocks:       blocks,
			lastModified: now,
			state:        stateCommitted,
		})
		keys = append(keys, meta.GetKey())
	}
	delete(s.pending, req.GetPutId())
	rep := &kachev1.CommitPutBatchReply{Committed: true, Keys: keys}
	s.committed[req.GetPutId()] = rep
	return s.maybeDropCommit(proto.Clone(rep).(*kachev1.CommitPutBatchReply))
}

func (s *Server) maybeDropCommit(rep *kachev1.CommitPutBatchReply) (*kachev1.CommitPutBatchReply, error) {
	if s.dropCommitOnce > 0 {
		s.dropCommitOnce--
		return nil, status.Error(codes.Unavailable, "simulated lost commit response")
	}
	return rep, nil
}

func validateCommit(p *pendingPut, req *kachev1.CommitPutBatchRequest) error {
	if len(req.GetResults()) != len(p.blocks) {
		return fmt.Errorf("expected %d block results, got %d", len(p.blocks), len(req.GetResults()))
	}
	byID := map[string]*kachev1.PhysicalBlock{}
	for _, b := range p.blocks {
		byID[b.GetPlanBlockId()] = b
	}
	seenBlocks := map[string]bool{}
	for _, br := range req.GetResults() {
		b, ok := byID[br.GetPlanBlockId()]
		if !ok {
			return status.Error(codes.Internal, "unknown plan_block_id in commit")
		}
		if seenBlocks[br.GetPlanBlockId()] {
			return fmt.Errorf("duplicate block result %s", br.GetPlanBlockId())
		}
		seenBlocks[br.GetPlanBlockId()] = true
		want := map[string]bool{}
		for _, t := range b.GetTargets() {
			want[t.GetTargetId()] = true
		}
		got := map[string]bool{}
		for _, tr := range br.GetResults() {
			if !want[tr.GetTargetId()] {
				return fmt.Errorf("unknown target_id %s", tr.GetTargetId())
			}
			if got[tr.GetTargetId()] {
				return fmt.Errorf("duplicate target_id %s", tr.GetTargetId())
			}
			got[tr.GetTargetId()] = true
			if !tr.GetOk() {
				return fmt.Errorf("target %s reported failure", tr.GetTargetId())
			}
		}
		if len(got) != len(want) {
			return fmt.Errorf("missing target results for block %s", br.GetPlanBlockId())
		}
	}
	return nil
}

func (s *Server) AbortPutBatch(ctx context.Context, req *kachev1.AbortPutBatchRequest) (*kachev1.AbortPutBatchReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.AbortCalls++
	if _, ok := s.pending[req.GetPutId()]; ok {
		delete(s.pending, req.GetPutId())
	}
	s.aborted[req.GetPutId()] = true
	return &kachev1.AbortPutBatchReply{Aborted: true}, nil
}

func sameMetas(a, b []*kachev1.PutChunkMeta) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetKey() != b[i].GetKey() || a[i].GetSizeBytes() != b[i].GetSizeBytes() || string(a[i].GetContentHash()) != string(b[i].GetContentHash()) {
			return false
		}
		if len(a[i].GetAttributes()) != len(b[i].GetAttributes()) {
			return false
		}
		for k, v := range a[i].GetAttributes() {
			if b[i].GetAttributes()[k] != v {
				return false
			}
		}
	}
	return true
}

func cloneBlocks(blocks []*kachev1.PhysicalBlock) []*kachev1.PhysicalBlock {
	out := make([]*kachev1.PhysicalBlock, len(blocks))
	for i, b := range blocks {
		out[i] = proto.Clone(b).(*kachev1.PhysicalBlock)
	}
	return out
}

func opaqueRef(blockID string) []byte {
	var uuid [16]byte
	_, _ = rand.Read(uuid[:])
	buf := make([]byte, 0, len(blockID)+len(uuid))
	buf = append(buf, blockID...)
	buf = append(buf, uuid[:]...)
	return buf
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
