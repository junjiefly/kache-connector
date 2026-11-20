// Package integration contains end-to-end tests against the local test
// doubles: a real loopback gRPC control-plane server plus a fake data plane.
// No GPU or RDMA hardware is required.
package integration

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/controlclient"
	"github.com/junjiefly/kache-connector/internal/metrics"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

const testNamespace = "test-ns"

type harness struct {
	srv   *testserver.Server
	plane *testserver.FakePlane
	conn  kvcache.Connector
	addr  string
	ctx   context.Context
}

type harnessConfig struct {
	server  testserver.Options
	mutate  func(*kvcache.Config)
	connOpt []kvcache.Option
}

func newHarness(t testing.TB, hc harnessConfig) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	plane := testserver.NewFakePlane()
	srv := testserver.New(hc.server)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	kachev1.RegisterKacheControlServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	cfg := kvcache.Config{
		Namespace: testNamespace,
		ControlPlane: kvcache.ControlPlaneConfig{
			Endpoints:        []string{lis.Addr().String()},
			TimeoutMS:        5000,
			InsecureTestMode: true,
		},
		Transport: kvcache.TransportConfig{
			Mode:            "auto",
			FallbackEnabled: true,
			RDMA:            kvcache.RDMATransportConfig{Enabled: true},
			TCP:             kvcache.TCPTransportConfig{Enabled: true},
		},
	}
	if hc.mutate != nil {
		hc.mutate(&cfg)
	}
	opts := append([]kvcache.Option{kvcache.WithFactories(plane.Factory("tcp"), plane.Factory("rdma"))}, hc.connOpt...)
	conn, err := kvcache.New(ctx, cfg, opts...)
	if err != nil {
		gs.Stop()
		t.Fatalf("connector.New: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close(context.Background())
		gs.Stop()
	})
	return &harness{srv: srv, plane: plane, conn: conn, addr: lis.Addr().String(), ctx: ctx}
}

// controlClient dials the test server directly, bypassing the connector, for
// server-side contract tests (commit validation, idempotency, expiry).
func (h *harness) controlClient(t testing.TB) controlclient.Client {
	t.Helper()
	c, err := controlclient.New(h.ctx, controlclient.Options{
		Endpoints: []string{h.addr},
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("controlclient.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (h *harness) metrics(t testing.TB) *metrics.Registry {
	t.Helper()
	mp, ok := h.conn.(interface{ Metrics() *metrics.Registry })
	if !ok {
		t.Fatal("connector does not expose Metrics()")
	}
	return mp.Metrics()
}

func (h *harness) poolStats(t testing.TB) (inflight, maxSeen, capacity int64) {
	t.Helper()
	pp, ok := h.conn.(interface {
		PoolStats() (int64, int64, int64)
	})
	if !ok {
		t.Fatal("connector does not expose PoolStats()")
	}
	return pp.PoolStats()
}

func chunk(key string, data []byte) *kachev1.PutChunk {
	return &kachev1.PutChunk{Key: key, Data: data, SizeBytes: uint64(len(data))}
}

func (h *harness) put(t testing.TB, chunks ...*kachev1.PutChunk) *kachev1.PutBatchReply {
	t.Helper()
	rep, err := h.conn.Put(h.ctx, &kachev1.PutBatchRequest{Chunks: chunks})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return rep
}

func (h *harness) mustPut(t testing.TB, key string, data []byte) {
	t.Helper()
	rep := h.put(t, chunk(key, data))
	if !rep.GetCommitted() {
		t.Fatalf("put %q not committed: %+v", key, rep.GetResults())
	}
}

func (h *harness) get(t testing.TB, keys ...string) *kachev1.GetBatchReply {
	t.Helper()
	rep, err := h.conn.Get(h.ctx, &kachev1.GetBatchRequest{Keys: keys})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return rep
}

func hashOf(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// commitResultsFor builds a fully successful Commit request for a Prepare reply.
func commitResultsFor(prep *kachev1.PreparePutBatchReply) []*kachev1.BlockWriteResult {
	var out []*kachev1.BlockWriteResult
	for _, b := range prep.GetBlocks() {
		br := &kachev1.BlockWriteResult{PlanBlockId: b.GetPlanBlockId()}
		for _, tgt := range b.GetTargets() {
			br.Results = append(br.Results, &kachev1.TargetWriteResult{
				TargetId: tgt.GetTargetId(), Endpoint: tgt.GetEndpoint(), Ok: true, BytesWritten: b.GetWireLen(),
			})
		}
		out = append(out, br)
	}
	return out
}

func (h *harness) prepareDirect(t testing.TB, cc controlclient.Client, requestID, key string, size uint64) *kachev1.PreparePutBatchReply {
	t.Helper()
	prep, err := cc.Prepare(h.ctx, &kachev1.PreparePutBatchRequest{
		Namespace:       testNamespace,
		ClientRequestId: requestID,
		Chunks:          []*kachev1.PutChunkMeta{{Key: key, SizeBytes: size, ContentHash: hashOf(make([]byte, size))}},
	})
	if err != nil {
		t.Fatalf("direct Prepare: %v", err)
	}
	if !prep.GetAccepted() {
		t.Fatalf("direct Prepare rejected: %+v", prep.GetErrors())
	}
	return prep
}
