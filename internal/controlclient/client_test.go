package controlclient

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// TestOneOverallDeadlineAcrossRetries pins spec 搂14: retries and endpoint
// failover must share one deadline, never restart the full timeout.
func TestOneOverallDeadlineAcrossRetries(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			// Hold the connection open without answering, so every RPC
			// blocks until its deadline.
			defer conn.Close()
		}
	}()

	c, err := New(context.Background(), Options{
		Endpoints: []string{lis.Addr().String()},
		Timeout:   150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	start := time.Now()
	_, err = c.Match(context.Background(), &kachev1.MatchBatchRequest{Keys: []string{"k"}})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("want deadline error from unresponsive endpoint")
	}
	// With a per-retry timeout restart this would take >= 3*150ms + backoff.
	if elapsed > 350*time.Millisecond {
		t.Fatalf("deadline restarted across retries: took %v", elapsed)
	}
}

type fakeControl struct {
	kachev1.UnimplementedKacheControlServer
	match  func(context.Context, *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error)
	commit func(context.Context, *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error)
}

func (f *fakeControl) MatchBatch(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
	return f.match(ctx, req)
}

func (f *fakeControl) CommitPutBatch(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error) {
	return f.commit(ctx, req)
}

func startFakeControl(t *testing.T, svc kachev1.KacheControlServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	kachev1.RegisterKacheControlServer(gs, svc)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func TestEndpointFailoverOrder(t *testing.T) {
	var calls []string
	first := startFakeControl(t, &fakeControl{match: func(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
		calls = append(calls, "first")
		return nil, status.Error(codes.Unavailable, "down")
	}})
	second := startFakeControl(t, &fakeControl{match: func(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
		calls = append(calls, "second")
		return &kachev1.MatchBatchReply{Results: []*kachev1.MatchResult{{Key: "k", Hit: true}}}, nil
	}})

	c, err := New(context.Background(), Options{
		Endpoints: []string{first, second},
		Timeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	rep, err := c.Match(context.Background(), &kachev1.MatchBatchRequest{Keys: []string{"k"}})
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if !rep.GetResults()[0].GetHit() {
		t.Fatal("want hit from second endpoint")
	}
	if len(calls) == 0 || calls[0] != "first" || calls[len(calls)-1] != "second" {
		t.Fatalf("endpoints not tried in configured order: %v", calls)
	}
}

// Spec 搂16: failure logs must carry the operation, request ID and error code.
func TestFailureLogIncludesRequestID(t *testing.T) {
	addr := startFakeControl(t, &fakeControl{commit: func(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}})
	var buf bytes.Buffer
	c, err := New(context.Background(), Options{
		Endpoints: []string{addr},
		Timeout:   2 * time.Second,
		Logger:    slog.New(slog.NewTextHandler(&buf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	_, _ = c.Commit(context.Background(), &kachev1.CommitPutBatchRequest{PutId: "put-log-test"})
	out := buf.String()
	for _, want := range []string{"op=CommitPutBatch", "put_id=put-log-test", "code=TRANSPORT_UNAVAILABLE"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log missing %q: %s", want, out)
		}
	}
}
