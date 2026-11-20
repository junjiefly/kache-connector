package testserver

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync"
)

// TCPNode is a real TCP data node speaking the private data-plane test wire format.
// It is used by integration tests and the Python smoke test.
type TCPNode struct {
	ln        net.Listener
	mu        sync.Mutex
	data      map[string][]byte
	fail      map[string]bool
	failReads bool
	done      chan struct{}
}

func StartTCPNode() (*TCPNode, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	n := &TCPNode{ln: ln, data: map[string][]byte{}, fail: map[string]bool{}, done: make(chan struct{})}
	go n.serve()
	return n, nil
}

func (n *TCPNode) Endpoint() string { return n.ln.Addr().String() }

// FailKey makes reads/writes for a hex-encoded ref key fail.
func (n *TCPNode) FailKey(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fail[key] = true
}

// FailAllReads makes every read fail while writes keep succeeding.
func (n *TCPNode) FailAllReads() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failReads = true
}

func (n *TCPNode) Close() error {
	close(n.done)
	return n.ln.Close()
}

func (n *TCPNode) serve() {
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			select {
			case <-n.done:
				return
			default:
				return
			}
		}
		go n.handle(conn)
	}
}

const (
	nodeMaxHeaderBytes  = 64 << 10
	nodeMaxPayloadBytes = 64 << 20
)

type nodeHeader struct {
	Op  string `json:"op"`
	Key string `json:"key"`
	RID string `json:"rid"`
	Len uint32 `json:"len"`
}

type nodeReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	N     uint32 `json:"n"`
}

func (n *TCPNode) handle(conn net.Conn) {
	defer conn.Close()
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}
	for {
		var lb [4]byte
		readN, err := io.ReadFull(conn, lb[:])
		_ = readN
		if err != nil {
			return
		}
		hlen := binary.BigEndian.Uint32(lb[:])
		if hlen > nodeMaxHeaderBytes {
			return
		}
		hb := make([]byte, hlen)
		_, headerErr := io.ReadFull(conn, hb)
		err = headerErr
		if err != nil {
			return
		}
		var h nodeHeader
		if err := json.Unmarshal(hb, &h); err != nil {
			return
		}
		var payload []byte
		if h.Op == "write" {
			if h.Len > nodeMaxPayloadBytes {
				return
			}
			payload = make([]byte, h.Len)
			_, payloadErr := io.ReadFull(conn, payload)
			err = payloadErr
			if err != nil {
				return
			}
		}
		rep, body := n.apply(h, payload)
		rb, _ := json.Marshal(rep)
		reply := make([]byte, 4+len(rb)+len(body))
		binary.BigEndian.PutUint32(reply[:4], uint32(len(rb)))
		copy(reply[4:], rb)
		copy(reply[4+len(rb):], body)
		_, err = conn.Write(reply)
		if err != nil {
			return
		}
	}
}

func (n *TCPNode) apply(h nodeHeader, payload []byte) (nodeReply, []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail[h.Key] {
		return nodeReply{OK: false, Error: "WRITE_TARGET_FAILED"}, nil
	}
	if h.Op == "read" && n.failReads {
		return nodeReply{OK: false, Error: "READ_TARGET_FAILED"}, nil
	}
	switch h.Op {
	case "write":
		n.data[h.Key] = payload
		return nodeReply{OK: true, N: 0}, nil
	case "read":
		data, ok := n.data[h.Key]
		if !ok {
			return nodeReply{OK: false, Error: "READ_TARGET_FAILED"}, nil
		}
		if uint32(len(data)) < h.Len {
			return nodeReply{OK: false, Error: "READ_TARGET_FAILED"}, nil
		}
		return nodeReply{OK: true, N: h.Len}, data[:h.Len]
	default:
		return nodeReply{OK: false, Error: "INVALID_ARGUMENT"}, nil
	}
}
