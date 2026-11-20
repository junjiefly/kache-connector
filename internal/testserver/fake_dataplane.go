package testserver

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	"github.com/junjiefly/kache-connector/internal/dataplane"
)

// WriteRecord captures one data-plane write for test assertions.
type WriteRecord struct {
	PlanBlockID string
	TargetID    string
	Endpoint    string
	Transport   string
	Length      uint32
	Payload     []byte
}

// ReadRecord captures one data-plane read for test assertions.
type ReadRecord struct {
	PlanBlockID string
	TargetID    string
	Endpoint    string
	Transport   string
	Length      uint32
}

// FakePlane is an in-memory data plane keyed by (opaque_ref, endpoint).
// The connector core treats refs as opaque; only this double maps them.
type FakePlane struct {
	mu     sync.Mutex
	store  map[string][]byte
	writes []WriteRecord
	reads  []ReadRecord

	failWriteEndpoints map[string]bool
	failReadEndpoints  map[string]bool
	failWriteTargets   map[string]bool
	failReadTargets    map[string]bool
	corruptReadRefs    map[string]bool
	writeDelay         time.Duration
	readDelay          time.Duration
}

func NewFakePlane() *FakePlane {
	return &FakePlane{
		store:              map[string][]byte{},
		failWriteEndpoints: map[string]bool{},
		failReadEndpoints:  map[string]bool{},
		failWriteTargets:   map[string]bool{},
		failReadTargets:    map[string]bool{},
		corruptReadRefs:    map[string]bool{},
	}
}

func (f *FakePlane) FailWriteEndpoint(ep string) { f.set(f.failWriteEndpoints, ep) }
func (f *FakePlane) FailReadEndpoint(ep string)  { f.set(f.failReadEndpoints, ep) }
func (f *FakePlane) FailWriteTarget(id string)   { f.set(f.failWriteTargets, id) }
func (f *FakePlane) FailReadTarget(id string)    { f.set(f.failReadTargets, id) }

func (f *FakePlane) set(m map[string]bool, k string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m[k] = true
}

// CorruptReadRef flips one byte of every read for the given opaque ref.
func (f *FakePlane) CorruptReadRef(opaqueRef []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.corruptReadRefs[hex.EncodeToString(opaqueRef)] = true
}

func (f *FakePlane) SetWriteDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeDelay = d
}

func (f *FakePlane) SetReadDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readDelay = d
}

func (f *FakePlane) Writes() []WriteRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]WriteRecord(nil), f.writes...)
}

func (f *FakePlane) Reads() []ReadRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ReadRecord(nil), f.reads...)
}

// Factory returns a dataplane.Factory for the given transport name.
func (f *FakePlane) Factory(transport string) dataplane.Factory {
	return &fakeFactory{plane: f, transport: transport}
}

type fakeFactory struct {
	plane     *FakePlane
	transport string
}

func (f *fakeFactory) Transport() string             { return f.transport }
func (f *fakeFactory) SupportsRefKind(k string) bool { return k == "test_ref_v1" }
func (f *fakeFactory) MaxBlockBytes() uint32         { return 64 << 20 }

func (f *fakeFactory) Open(ctx context.Context, target dataplane.DataTarget) (dataplane.DataTransport, error) {
	return &fakeSession{plane: f.plane, target: target}, nil
}

type fakeSession struct {
	plane  *FakePlane
	target dataplane.DataTarget
}

func refKey(ref []byte, endpoint string) string {
	return hex.EncodeToString(ref) + "|" + endpoint
}

func (s *fakeSession) WriteBlocks(ctx context.Context, reqs []dataplane.DataWriteRequest) ([]dataplane.DataWriteResult, error) {
	out := make([]dataplane.DataWriteResult, len(reqs))
	for i, r := range reqs {
		s.plane.mu.Lock()
		delay := s.plane.writeDelay
		fail := s.plane.failWriteEndpoints[r.Target.Endpoint] || s.plane.failWriteTargets[r.Target.TargetID]
		s.plane.writes = append(s.plane.writes, WriteRecord{
			PlanBlockID: r.PlanBlockID, TargetID: r.Target.TargetID, Endpoint: r.Target.Endpoint,
			Transport: r.Target.Transport, Length: r.Length, Payload: append([]byte(nil), r.Payload...),
		})
		s.plane.mu.Unlock()
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
		if fail {
			out[i] = dataplane.DataWriteResult{PlanBlockID: r.PlanBlockID, TargetID: r.Target.TargetID, OK: false, ErrorCode: "WRITE_TARGET_FAILED"}
			continue
		}
		if uint32(len(r.Payload)) != r.Length {
			out[i] = dataplane.DataWriteResult{PlanBlockID: r.PlanBlockID, TargetID: r.Target.TargetID, OK: false, ErrorCode: "PAYLOAD_LENGTH_MISMATCH"}
			continue
		}
		s.plane.mu.Lock()
		s.plane.store[refKey(r.OpaqueRef, r.Target.Endpoint)] = append([]byte(nil), r.Payload...)
		s.plane.mu.Unlock()
		out[i] = dataplane.DataWriteResult{PlanBlockID: r.PlanBlockID, TargetID: r.Target.TargetID, OK: true, BytesWritten: uint32(len(r.Payload))}
	}
	return out, nil
}

func (s *fakeSession) ReadBlocks(ctx context.Context, reqs []dataplane.DataReadRequest) ([]dataplane.DataReadResult, error) {
	out := make([]dataplane.DataReadResult, len(reqs))
	for i, r := range reqs {
		s.plane.mu.Lock()
		delay := s.plane.readDelay
		fail := s.plane.failReadEndpoints[r.Target.Endpoint] || s.plane.failReadTargets[r.Target.TargetID]
		corrupt := s.plane.corruptReadRefs[hex.EncodeToString(r.OpaqueRef)]
		data := s.plane.store[refKey(r.OpaqueRef, r.Target.Endpoint)]
		s.plane.reads = append(s.plane.reads, ReadRecord{
			PlanBlockID: r.PlanBlockID, TargetID: r.Target.TargetID, Endpoint: r.Target.Endpoint,
			Transport: r.Target.Transport, Length: r.Length,
		})
		s.plane.mu.Unlock()
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
		if fail || data == nil {
			out[i] = dataplane.DataReadResult{TargetID: r.Target.TargetID, OK: false, ErrorCode: "READ_TARGET_FAILED"}
			continue
		}
		payload := append([]byte(nil), data...)
		if corrupt && len(payload) > 0 {
			payload[0] ^= 0xff
		}
		out[i] = dataplane.DataReadResult{TargetID: r.Target.TargetID, OK: true, Data: payload}
	}
	return out, nil
}

func (s *fakeSession) Close(ctx context.Context) error { return nil }

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
