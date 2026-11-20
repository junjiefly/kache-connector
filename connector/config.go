package kvcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
)

const (
	defaultMaxChunkBytes       = 64 << 20
	defaultMaxBatchChunks      = 64
	defaultMaxInflightBytes    = 256 << 20
	defaultMaxConcurrentBlocks = 16
	defaultControlTimeoutMS    = 5000
	defaultCapsRefreshSeconds  = 60
	defaultSessionIdleMS       = 60000
	defaultRDMADialTimeoutMS   = 2000
	defaultTCPDialTimeoutMS    = 2000
	defaultRDMAPoolBytes       = 256 << 20
	defaultRDMAMaxOutstanding  = 32
)

// Config is the JSON configuration accepted by the C ABI and the Go API.
type Config struct {
	Namespace    string             `json:"namespace"`
	ControlPlane ControlPlaneConfig `json:"control_plane"`
	Transport    TransportConfig    `json:"transport"`
	Limits       LimitsConfig       `json:"limits"`
}

type ControlPlaneConfig struct {
	Endpoints          []string  `json:"endpoints"`
	TimeoutMS          int       `json:"timeout_ms"`
	AuthTokenEnv       string    `json:"auth_token_env"`
	TLS                TLSConfig `json:"tls"`
	InsecureTestMode   bool      `json:"insecure_test_mode"`
	CapsRefreshSeconds int       `json:"caps_refresh_seconds"`
}

type TLSConfig struct {
	Enabled    bool   `json:"enabled"`
	CAFile     string `json:"ca_file"`
	CertFile   string `json:"cert_file"`
	KeyFile    string `json:"key_file"`
	ServerName string `json:"server_name"`
}

type TransportConfig struct {
	Mode                 string              `json:"mode"`
	FallbackEnabled      bool                `json:"fallback_enabled"`
	SessionIdleTimeoutMS int                 `json:"session_idle_timeout_ms"`
	RDMA                 RDMATransportConfig `json:"rdma"`
	TCP                  TCPTransportConfig  `json:"tcp"`
}

type RDMATransportConfig struct {
	Enabled        bool  `json:"enabled"`
	DialTimeoutMS  int   `json:"dial_timeout_ms"`
	PoolSizeBytes  int64 `json:"pool_size_bytes"`
	MaxOutstanding int   `json:"max_outstanding"`
}

type TCPTransportConfig struct {
	Enabled       bool `json:"enabled"`
	DialTimeoutMS int  `json:"dial_timeout_ms"`
}

type LimitsConfig struct {
	MaxChunkBytes       int64 `json:"max_chunk_bytes"`
	MaxBatchChunks      int   `json:"max_batch_chunks"`
	MaxInflightBytes    int64 `json:"max_inflight_bytes"`
	MaxConcurrentBlocks int   `json:"max_concurrent_blocks"`
	MaxWireBlockBytes   int64 `json:"max_wire_block_bytes"`
}

// ParseConfig decodes, defaults and validates a JSON configuration.
func ParseConfig(data []byte) (*Config, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, errs.New(errs.InvalidArgument, "invalid config JSON: %v", err)
	}
	if dec.More() {
		return nil, errs.New(errs.InvalidArgument, "trailing data after config JSON")
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	cp := &c.ControlPlane
	if cp.TimeoutMS == 0 {
		cp.TimeoutMS = defaultControlTimeoutMS
	}
	if cp.CapsRefreshSeconds == 0 {
		cp.CapsRefreshSeconds = defaultCapsRefreshSeconds
	}
	tr := &c.Transport
	if tr.Mode == "" {
		tr.Mode = "auto"
	}
	if tr.SessionIdleTimeoutMS == 0 {
		tr.SessionIdleTimeoutMS = defaultSessionIdleMS
	}
	if tr.RDMA.PoolSizeBytes == 0 {
		tr.RDMA.PoolSizeBytes = defaultRDMAPoolBytes
	}
	if tr.RDMA.DialTimeoutMS == 0 {
		tr.RDMA.DialTimeoutMS = defaultRDMADialTimeoutMS
	}
	if tr.RDMA.MaxOutstanding == 0 {
		tr.RDMA.MaxOutstanding = defaultRDMAMaxOutstanding
	}
	if tr.TCP.DialTimeoutMS == 0 {
		tr.TCP.DialTimeoutMS = defaultTCPDialTimeoutMS
	}
	lim := &c.Limits
	if lim.MaxChunkBytes == 0 {
		lim.MaxChunkBytes = defaultMaxChunkBytes
	}
	if lim.MaxBatchChunks == 0 {
		lim.MaxBatchChunks = defaultMaxBatchChunks
	}
	if lim.MaxInflightBytes == 0 {
		lim.MaxInflightBytes = defaultMaxInflightBytes
	}
	if lim.MaxConcurrentBlocks == 0 {
		lim.MaxConcurrentBlocks = defaultMaxConcurrentBlocks
	}
}

// Validate checks the configuration without contacting the server.
func (c *Config) Validate() error {
	if c.Namespace == "" {
		return errs.New(errs.InvalidArgument, "namespace is required")
	}
	cp := c.ControlPlane
	if len(cp.Endpoints) == 0 {
		return errs.New(errs.InvalidArgument, "control_plane.endpoints is required")
	}
	for _, ep := range cp.Endpoints {
		host, port, err := net.SplitHostPort(ep)
		if err != nil || host == "" || port == "" {
			return errs.New(errs.InvalidArgument, "invalid control-plane endpoint %q (want host:port)", ep)
		}
	}
	if cp.TimeoutMS <= 0 {
		return errs.New(errs.InvalidArgument, "control_plane.timeout_ms must be positive")
	}
	if cp.CapsRefreshSeconds < 0 {
		return errs.New(errs.InvalidArgument, "control_plane.caps_refresh_seconds must be >= 0")
	}
	if cp.AuthTokenEnv != "" && os.Getenv(cp.AuthTokenEnv) == "" {
		return errs.New(errs.InvalidArgument, "auth token env %q is not set", cp.AuthTokenEnv)
	}
	if cp.TLS.Enabled {
		if (cp.TLS.CertFile == "") != (cp.TLS.KeyFile == "") {
			return errs.New(errs.InvalidArgument, "tls cert_file and key_file must be set together")
		}
		for _, f := range []string{cp.TLS.CAFile, cp.TLS.CertFile, cp.TLS.KeyFile} {
			if f != "" {
				if _, err := os.Stat(f); err != nil {
					return errs.New(errs.InvalidArgument, "tls file %q: %v", f, err)
				}
			}
		}
	}
	if !cp.TLS.Enabled && cp.AuthTokenEnv == "" && !cp.InsecureTestMode {
		return errs.New(errs.InvalidArgument, "tls or auth token required unless insecure_test_mode=true")
	}
	if cp.InsecureTestMode {
		if cp.TLS.Enabled {
			return errs.New(errs.InvalidArgument, "insecure_test_mode cannot be combined with tls")
		}
		for _, ep := range cp.Endpoints {
			if !isLoopbackEndpoint(ep) {
				return errs.New(errs.InvalidArgument, "insecure_test_mode requires loopback control-plane endpoints, got %q", ep)
			}
		}
	}
	switch c.Transport.Mode {
	case "rdma", "tcp", "auto":
	default:
		return errs.New(errs.InvalidArgument, "transport.mode must be rdma, tcp or auto")
	}
	if c.Transport.Mode != "tcp" && !c.Transport.RDMA.Enabled {
		return errs.New(errs.InvalidArgument, "transport.rdma.enabled must be true for mode %q", c.Transport.Mode)
	}
	if c.Transport.Mode != "rdma" && !c.Transport.TCP.Enabled {
		return errs.New(errs.InvalidArgument, "transport.tcp.enabled must be true for mode %q", c.Transport.Mode)
	}
	if c.Transport.FallbackEnabled && !c.Transport.TCP.Enabled {
		return errs.New(errs.InvalidArgument, "transport fallback requires tcp.enabled")
	}
	if c.Transport.SessionIdleTimeoutMS <= 0 {
		return errs.New(errs.InvalidArgument, "transport.session_idle_timeout_ms must be positive")
	}
	if c.Transport.RDMA.PoolSizeBytes <= 0 || c.Transport.RDMA.MaxOutstanding <= 0 {
		return errs.New(errs.InvalidArgument, "rdma pool_size_bytes and max_outstanding must be positive")
	}
	if c.Transport.RDMA.DialTimeoutMS <= 0 {
		return errs.New(errs.InvalidArgument, "rdma dial_timeout_ms must be positive")
	}
	if c.Transport.TCP.DialTimeoutMS <= 0 {
		return errs.New(errs.InvalidArgument, "tcp.dial_timeout_ms must be positive")
	}
	lim := c.Limits
	if lim.MaxChunkBytes <= 0 || lim.MaxBatchChunks <= 0 || lim.MaxInflightBytes <= 0 || lim.MaxConcurrentBlocks <= 0 {
		return errs.New(errs.InvalidArgument, "limits must be positive")
	}
	if lim.MaxWireBlockBytes < 0 {
		return errs.New(errs.InvalidArgument, "limits.max_wire_block_bytes must be >= 0")
	}
	if lim.MaxWireBlockBytes > int64(^uint32(0)) {
		return errs.New(errs.InvalidArgument, "limits.max_wire_block_bytes exceeds uint32")
	}
	if lim.MaxInflightBytes < lim.MaxChunkBytes {
		return errs.New(errs.InvalidArgument, "max_inflight_bytes must be >= max_chunk_bytes")
	}
	return nil
}

func isLoopbackEndpoint(ep string) bool {
	host, _, err := net.SplitHostPort(ep)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c *Config) controlTimeout() time.Duration {
	return time.Duration(c.ControlPlane.TimeoutMS) * time.Millisecond
}

func (c *Config) capsRefresh() time.Duration {
	return time.Duration(c.ControlPlane.CapsRefreshSeconds) * time.Second
}

func (c *Config) sessionIdleTimeout() time.Duration {
	return time.Duration(c.Transport.SessionIdleTimeoutMS) * time.Millisecond
}

func (c *Config) String() string {
	// Never include tokens or credentials; endpoints and limits are safe.
	return fmt.Sprintf("Config{namespace:%s endpoints:%v mode:%s}", c.Namespace, c.ControlPlane.Endpoints, c.Transport.Mode)
}
