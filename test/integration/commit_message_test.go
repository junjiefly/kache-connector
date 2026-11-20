package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

type commitFailServer struct {
	*testserver.Server
}

func (s *commitFailServer) CommitPutBatch(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error) {
	return nil, status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:41945: connect: connection refused"`)
}

// Acceptance criterion 6: Python-visible replies must not carry endpoints.
// A Commit transport failure must surface a sanitized per-key message.
func TestCommitFailureMessageSanitized(t *testing.T) {
	srv := &commitFailServer{Server: testserver.New(testserver.Options{})}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	kachev1.RegisterKacheControlServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	plane := testserver.NewFakePlane()
	cfg := kvcache.Config{
		Namespace: testNamespace,
		ControlPlane: kvcache.ControlPlaneConfig{
			Endpoints:        []string{lis.Addr().String()},
			TimeoutMS:        2000,
			InsecureTestMode: true,
		},
		Transport: kvcache.TransportConfig{Mode: "tcp", TCP: kvcache.TCPTransportConfig{Enabled: true}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := kvcache.New(ctx, cfg, kvcache.WithFactories(plane.Factory("tcp")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	rep, err := conn.Put(ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{chunk("k", []byte("data"))}})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rep.GetCommitted() {
		t.Fatal("want failed put")
	}
	r := rep.GetResults()[0]
	if r.GetErrorCode() != "TRANSPORT_UNAVAILABLE" {
		t.Fatalf("error code = %q, want TRANSPORT_UNAVAILABLE", r.GetErrorCode())
	}
	if strings.Contains(r.GetMessage(), "127.0.0.1") || strings.Contains(r.GetMessage(), "dial") {
		t.Fatalf("message leaks transport details: %q", r.GetMessage())
	}
	if srv.AbortCalls == 0 {
		t.Fatal("want Abort after failed Commit")
	}
}
