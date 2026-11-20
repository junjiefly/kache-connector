// Package dataplane defines the transport-neutral data-plane contract and
// the Put/Get orchestration shared by all backends.
package dataplane

import (
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
	"github.com/junjiefly/kache-connector/transport"
)

type (
	DataTarget       = transport.Target
	DataWriteRequest = transport.WriteRequest
	DataReadRequest  = transport.ReadRequest
	DataWriteResult  = transport.WriteResult
	DataReadResult   = transport.ReadResult
	DataTransport    = transport.Session
	Factory          = transport.Factory
)

func TargetFromProto(t *kachev1.DataTarget) transport.Target {
	return transport.TargetFromProto(t)
}

// Limits are the effective (min of local and server) limits for one operation.
type Limits struct {
	MaxChunkBytes       int64
	MaxBatchChunks      int
	MaxBatchBytes       int64
	MaxConcurrentBlocks int
	MaxWireBlockBytes   uint32
	FallbackEnabled     bool
}
