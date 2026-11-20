// Command kvc-smoke runs an end-to-end smoke path against the local test
// doubles, or serves them for the Python ctypes smoke test (-serve).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	kvcache "github.com/junjiefly/kache-connector/connector"
	"github.com/junjiefly/kache-connector/internal/testserver"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

func main() {
	serve := flag.Bool("serve", false, "serve test doubles for the Python smoke test")
	endpointFile := flag.String("endpoint-file", "", "write connector config JSON to this file (-serve)")
	failReads := flag.Bool("fail-reads", false, "make the data node fail all reads (-serve)")
	flag.Parse()
	if *serve {
		if err := runServe(*endpointFile, *failReads); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := runSmoke(); err != nil {
		log.Fatal(err)
	}
}

func startControl(srv *testserver.Server) (string, func(), error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	gs := grpc.NewServer()
	kachev1.RegisterKacheControlServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), gs.Stop, nil
}

func runSmoke() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	plane := testserver.NewFakePlane()
	srv := testserver.New(testserver.Options{})
	addr, stop, err := startControl(srv)
	if err != nil {
		return err
	}
	defer stop()
	cfg := kvcache.Config{
		Namespace: "smoke",
		ControlPlane: kvcache.ControlPlaneConfig{
			Endpoints:        []string{addr},
			InsecureTestMode: true,
		},
		Transport: kvcache.TransportConfig{Mode: "tcp", TCP: kvcache.TCPTransportConfig{Enabled: true}},
	}
	conn, err := kvcache.New(ctx, cfg, kvcache.WithFactories(plane.Factory("tcp"), plane.Factory("rdma")))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	data := []byte("hello kv cache")
	put, err := conn.Put(ctx, &kachev1.PutBatchRequest{Chunks: []*kachev1.PutChunk{
		{Key: "smoke/k1", Data: data, SizeBytes: uint64(len(data))},
	}})
	if err != nil {
		return err
	}
	if !put.GetCommitted() {
		return fmt.Errorf("put not committed: %+v", put.GetResults())
	}
	get, err := conn.Get(ctx, &kachev1.GetBatchRequest{Keys: []string{"smoke/k1", "smoke/missing"}})
	if err != nil {
		return err
	}
	if !get.GetResults()[0].GetHit() || string(get.GetResults()[0].GetData()) != string(data) {
		return fmt.Errorf("get mismatch: %+v", get.GetResults()[0])
	}
	if get.GetResults()[1].GetHit() {
		return fmt.Errorf("expected miss")
	}
	del, err := conn.Delete(ctx, &kachev1.DeleteBatchRequest{Keys: []string{"smoke/k1"}})
	if err != nil {
		return err
	}
	if !del.GetResults()[0].GetDeleted() {
		return fmt.Errorf("delete failed")
	}
	fmt.Println("smoke OK: put/get/miss/delete passed")
	return nil
}

func runServe(endpointFile string, failReads bool) error {
	node, err := testserver.StartTCPNode()
	if err != nil {
		return err
	}
	defer node.Close()
	if failReads {
		node.FailAllReads()
	}
	srv := testserver.New(testserver.Options{
		Targets: []testserver.TargetSpec{{Endpoint: node.Endpoint(), Transport: "tcp"}},
	})
	addr, stop, err := startControl(srv)
	if err != nil {
		return err
	}
	defer stop()
	cfg := map[string]any{
		"namespace": "llm-cache",
		"control_plane": map[string]any{
			"endpoints":          []string{addr},
			"timeout_ms":         5000,
			"insecure_test_mode": true,
		},
		"transport": map[string]any{
			"mode":             "tcp",
			"fallback_enabled": false,
			"rdma":             map[string]any{"enabled": false},
			"tcp":              map[string]any{"enabled": true, "dial_timeout_ms": 2000},
		},
	}
	if endpointFile != "" {
		b, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(endpointFile, b, 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("serving control=%s data=%s\n", addr, node.Endpoint())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	return nil
}
