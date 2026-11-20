package testserver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/transport"
)

func TestTCPTransportFactoryRoundTrip(t *testing.T) {
	node, err := StartTCPNode()
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	factory := NewTCPTransportFactory(1000)
	ctx := context.Background()
	session, err := factory.Open(ctx, transport.Target{Endpoint: node.Endpoint(), Transport: "tcp"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close(ctx) }()

	ref := []byte("test-block")
	write, err := session.WriteBlocks(ctx, []transport.WriteRequest{{
		PlanBlockID: "b1", Target: transport.Target{TargetID: "t1"},
		RefKind: "test_ref_v1", OpaqueRef: ref, Length: 5, Payload: []byte("hello"), RequestID: "rid",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(write) != 1 || !write[0].OK || write[0].BytesWritten != 5 {
		t.Fatalf("unexpected write result: %+v", write)
	}
	read, err := session.ReadBlocks(ctx, []transport.ReadRequest{{
		PlanBlockID: "b1", Target: transport.Target{TargetID: "t1"},
		RefKind: "test_ref_v1", OpaqueRef: ref, Length: 5, RequestID: "rid",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(read) != 1 || !read[0].OK || string(read[0].Data) != "hello" {
		t.Fatalf("unexpected read result: %+v", read)
	}
}

// A malicious length prefix must not cause a large allocation; the node
// closes the connection instead.
func TestTCPNodeOversizedFrameRejected(t *testing.T) {
	n, err := StartTCPNode()
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	cases := []struct {
		name  string
		frame func() []byte
	}{
		{"header", func() []byte {
			var lb [4]byte
			binary.BigEndian.PutUint32(lb[:], 1<<28)
			return lb[:]
		}},
		{"write payload", func() []byte {
			hb, _ := json.Marshal(nodeHeader{Op: "write", Key: "aa", Len: 1 << 30})
			frame := make([]byte, 4+len(hb))
			binary.BigEndian.PutUint32(frame[:4], uint32(len(hb)))
			copy(frame[4:], hb)
			return frame
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.Dial("tcp", n.Endpoint())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			if _, err := conn.Write(tc.frame()); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			buf := make([]byte, 1)
			if _, err := conn.Read(buf); err != io.EOF {
				t.Fatalf("want EOF from closed connection, got %v", err)
			}
			runtime.ReadMemStats(&after)
			if delta := after.TotalAlloc - before.TotalAlloc; delta > 1<<20 {
				t.Fatalf("oversized frame allocated %d bytes", delta)
			}
		})
	}
}
