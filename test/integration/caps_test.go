package integration

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

type capsGateServer struct {
	kachev1.KacheControlServer
	calls    atomic.Int32
	blocking atomic.Bool
	gate     chan struct{}
}

func (g *capsGateServer) GetCaps(ctx context.Context, req *kachev1.GetCapsRequest) (*kachev1.GetCapsReply, error) {
	g.calls.Add(1)
	if g.blocking.Load() {
		select {
		case <-g.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.KacheControlServer.GetCaps(ctx, req)
}

// When a caps refresh hangs, concurrent callers must share one refresh attempt
// instead of each serializing behind the mutex and retrying the RPC in turn.
func TestCapsRefreshSingleflight(t *testing.T) {
	gate := &capsGateServer{
		KacheControlServer: testserver.New(testserver.Options{}),
		gate:               make(chan struct{}),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	kachev1.RegisterKacheControlServer(gs, gate)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() { close(gate.gate); gs.Stop() })

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	plane := testserver.NewFakePlane()
	cfg := kvcache.Config{
		Namespace: testNamespace,
		ControlPlane: kvcache.ControlPlaneConfig{
			Endpoints:          []string{lis.Addr().String()},
			TimeoutMS:          5000,
			InsecureTestMode:   true,
			CapsRefreshSeconds: 1,
		},
		Transport: kvcache.TransportConfig{Mode: "tcp", TCP: kvcache.TCPTransportConfig{Enabled: true}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kvcache.New(ctx, cfg,
		kvcache.WithClock(func() time.Time { return time.Unix(0, now.Load()) }),
		kvcache.WithFactories(plane.Factory("tcp")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	if _, err := conn.Caps(ctx); err != nil {
		t.Fatalf("seed caps: %v", err)
	}
	now.Add(int64(2 * time.Second))
	gate.blocking.Store(true)

	const n = 4
	var wg sync.WaitGroup
	errsCh := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, ccancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer ccancel()
			_, err := conn.Caps(cctx)
			errsCh <- err
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	for i := 0; i < n; i++ {
		if err := <-errsCh; err == nil {
			t.Fatal("want error while refresh is blocked")
		}
	}
	if got := gate.calls.Load(); got != 2 {
		t.Fatalf("GetCaps called %d times, want 2 (seed + one shared refresh)", got)
	}
	if elapsed > time.Second {
		t.Fatalf("callers serialized behind the refresh: %v", elapsed)
	}
}
