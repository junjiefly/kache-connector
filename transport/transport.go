// Package transport defines the public data-plane SPI used by the connector
// core. Production RDMA/TCP implementations live outside the open-source
// module and are injected with kvcache.WithFactories.
package transport

import (
	"context"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Target is the connector's view of a protobuf DataTarget.
// Capability is an opaque per-target token; it must never be logged.
type Target struct {
	TargetID         string
	Endpoint         string
	Capability       []byte
	Hints            map[string]string
	Transport        string
	FallbackEndpoint string
}

// TargetFromProto converts a protobuf DataTarget without copying the
// capability or hints maps; callers must treat their contents as immutable.
func TargetFromProto(t *kachev1.DataTarget) Target {
	return Target{
		TargetID:         t.GetTargetId(),
		Endpoint:         t.GetEndpoint(),
		Capability:       t.GetCapability(),
		Hints:            t.GetHints(),
		Transport:        t.GetTransport(),
		FallbackEndpoint: t.GetFallbackEndpoint(),
	}
}

// ToProto converts a target back to its wire representation.
func (t Target) ToProto() *kachev1.DataTarget {
	return &kachev1.DataTarget{
		TargetId:         t.TargetID,
		Endpoint:         t.Endpoint,
		Capability:       t.Capability,
		Hints:            t.Hints,
		Transport:        t.Transport,
		FallbackEndpoint: t.FallbackEndpoint,
	}
}

type WriteRequest struct {
	PlanBlockID string
	Target      Target
	RefKind     string
	OpaqueRef   []byte
	Offset      uint64
	Length      uint32
	Payload     []byte
	RequestID   string
}

type ReadRequest struct {
	PlanBlockID string
	Target      Target
	RefKind     string
	OpaqueRef   []byte
	Offset      uint64
	Length      uint32
	RequestID   string
}

type WriteResult struct {
	PlanBlockID  string
	TargetID     string
	OK           bool
	ErrorCode    string
	BytesWritten uint32
}

type ReadResult struct {
	TargetID  string
	OK        bool
	ErrorCode string
	Data      []byte
}

// Session is a session bound to one (endpoint, transport) pair.
// Results must be returned in request order.
type Session interface {
	WriteBlocks(ctx context.Context, reqs []WriteRequest) ([]WriteResult, error)
	ReadBlocks(ctx context.Context, reqs []ReadRequest) ([]ReadResult, error)
	Close(ctx context.Context) error
}

// Factory creates sessions for one transport kind.
type Factory interface {
	Transport() string
	SupportsRefKind(refKind string) bool
	MaxBlockBytes() uint32
	Open(ctx context.Context, target Target) (Session, error)
}
