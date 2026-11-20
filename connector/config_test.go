package kvcache

import (
	"strings"
	"testing"
	"time"

	"github.com/junjiefly/kache-connector/internal/errs"
)

const validConfigJSON = `{
  "namespace": "llm-cache",
  "control_plane": {"endpoints": ["127.0.0.1:9100"], "insecure_test_mode": true},
  "transport": {"mode": "tcp", "tcp": {"enabled": true}}
}`

func baseConfig() Config {
	return Config{
		Namespace: "llm-cache",
		ControlPlane: ControlPlaneConfig{
			Endpoints:        []string{"127.0.0.1:9100"},
			InsecureTestMode: true,
		},
		Transport: TransportConfig{Mode: "tcp", TCP: TCPTransportConfig{Enabled: true}},
	}
}

func TestParseConfigValid(t *testing.T) {
	cfg, err := ParseConfig([]byte(validConfigJSON))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Namespace != "llm-cache" {
		t.Fatalf("namespace = %q", cfg.Namespace)
	}
	if cfg.ControlPlane.TimeoutMS != defaultControlTimeoutMS {
		t.Fatalf("timeout default = %d", cfg.ControlPlane.TimeoutMS)
	}
	if cfg.Limits.MaxChunkBytes != defaultMaxChunkBytes || cfg.Limits.MaxBatchChunks != defaultMaxBatchChunks {
		t.Fatalf("limit defaults not applied: %+v", cfg.Limits)
	}
	if cfg.Transport.RDMA.PoolSizeBytes != defaultRDMAPoolBytes {
		t.Fatalf("rdma pool default = %d", cfg.Transport.RDMA.PoolSizeBytes)
	}
	if cfg.Transport.SessionIdleTimeoutMS != defaultSessionIdleMS {
		t.Fatalf("session idle default = %d", cfg.Transport.SessionIdleTimeoutMS)
	}
}

func TestParseConfigRejectsUnknownField(t *testing.T) {
	_, err := ParseConfig([]byte(`{"namespace":"x","bogus":1}`))
	if errs.From(err) != errs.InvalidArgument {
		t.Fatalf("want INVALID_ARGUMENT, got %v", err)
	}
}

func TestParseConfigRejectsTrailingData(t *testing.T) {
	_, err := ParseConfig([]byte(validConfigJSON + `{"namespace":"y"}`))
	if errs.From(err) != errs.InvalidArgument {
		t.Fatalf("want INVALID_ARGUMENT, got %v", err)
	}
}

func TestParseConfigRejectsBadJSON(t *testing.T) {
	if _, err := ParseConfig([]byte(`{`)); err == nil {
		t.Fatal("want error for truncated JSON")
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		code   errs.Code
	}{
		{"missing namespace", func(c *Config) { c.Namespace = "" }, errs.InvalidArgument},
		{"missing endpoints", func(c *Config) { c.ControlPlane.Endpoints = nil }, errs.InvalidArgument},
		{"endpoint not host:port", func(c *Config) { c.ControlPlane.Endpoints = []string{"http://x"} }, errs.InvalidArgument},
		{"endpoint missing port", func(c *Config) { c.ControlPlane.Endpoints = []string{"127.0.0.1"} }, errs.InvalidArgument},
		{"negative timeout", func(c *Config) { c.ControlPlane.TimeoutMS = -1 }, errs.InvalidArgument},
		{"negative caps refresh", func(c *Config) { c.ControlPlane.CapsRefreshSeconds = -1 }, errs.InvalidArgument},
		{"auth env unset", func(c *Config) {
			c.ControlPlane.InsecureTestMode = false
			c.ControlPlane.AuthTokenEnv = "KVC_TEST_TOKEN_DEFINITELY_UNSET"
		}, errs.InvalidArgument},
		{"no tls no token no insecure", func(c *Config) { c.ControlPlane.InsecureTestMode = false }, errs.InvalidArgument},
		{"insecure non-loopback", func(c *Config) { c.ControlPlane.Endpoints = []string{"10.0.0.8:9100"} }, errs.InvalidArgument},
		{"insecure with tls", func(c *Config) { c.ControlPlane.TLS.Enabled = true }, errs.InvalidArgument},
		{"tls cert without key", func(c *Config) {
			c.ControlPlane.InsecureTestMode = false
			c.ControlPlane.TLS = TLSConfig{Enabled: true, CertFile: "cert.pem"}
		}, errs.InvalidArgument},
		{"tls missing file", func(c *Config) {
			c.ControlPlane.InsecureTestMode = false
			c.ControlPlane.TLS = TLSConfig{Enabled: true, CAFile: "no-such-ca.pem"}
		}, errs.InvalidArgument},
		{"bad mode", func(c *Config) { c.Transport.Mode = "udp" }, errs.InvalidArgument},
		{"auto without rdma", func(c *Config) { c.Transport.Mode = "auto" }, errs.InvalidArgument},
		{"tcp mode without tcp", func(c *Config) { c.Transport.TCP.Enabled = false }, errs.InvalidArgument},
		{"fallback without tcp", func(c *Config) {
			c.Transport.Mode = "rdma"
			c.Transport.RDMA.Enabled = true
			c.Transport.TCP.Enabled = false
			c.Transport.FallbackEnabled = true
		}, errs.InvalidArgument},
		{"negative rdma pool", func(c *Config) { c.Transport.RDMA.PoolSizeBytes = -1 }, errs.InvalidArgument},
		{"negative session idle", func(c *Config) { c.Transport.SessionIdleTimeoutMS = -1 }, errs.InvalidArgument},
		{"zero tcp dial timeout", func(c *Config) { c.Transport.TCP.DialTimeoutMS = -1 }, errs.InvalidArgument},
		{"negative chunk limit", func(c *Config) { c.Limits.MaxChunkBytes = -1 }, errs.InvalidArgument},
		{"negative wire limit", func(c *Config) { c.Limits.MaxWireBlockBytes = -1 }, errs.InvalidArgument},
		{"wire limit exceeds uint32", func(c *Config) {
			c.Limits.MaxWireBlockBytes = int64(^uint32(0)) + 1
		}, errs.InvalidArgument},
		{"inflight below chunk", func(c *Config) {
			c.Limits.MaxChunkBytes = 1024
			c.Limits.MaxInflightBytes = 512
		}, errs.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.applyDefaults()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if errs.From(err) != tc.code {
				t.Fatalf("want %s, got %v", tc.code, err)
			}
		})
	}
}

func TestConfigValidVariants(t *testing.T) {
	t.Run("auto with both transports", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Transport.Mode = "auto"
		cfg.Transport.RDMA.Enabled = true
		cfg.Transport.FallbackEnabled = true
		cfg.applyDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
	t.Run("auth token from env", func(t *testing.T) {
		t.Setenv("KVC_TEST_TOKEN", "secret-token")
		cfg := baseConfig()
		cfg.ControlPlane.InsecureTestMode = false
		cfg.ControlPlane.AuthTokenEnv = "KVC_TEST_TOKEN"
		cfg.applyDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
	t.Run("localhost endpoint allowed insecure", func(t *testing.T) {
		cfg := baseConfig()
		cfg.ControlPlane.Endpoints = []string{"localhost:9100"}
		cfg.applyDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	})
	t.Run("custom session idle timeout", func(t *testing.T) {
		cfg, err := ParseConfig([]byte(`{
		  "namespace": "llm-cache",
		  "control_plane": {"endpoints": ["127.0.0.1:9100"], "insecure_test_mode": true},
		  "transport": {"mode": "tcp", "tcp": {"enabled": true}, "session_idle_timeout_ms": 1500}
		}`))
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		if cfg.sessionIdleTimeout() != 1500*time.Millisecond {
			t.Fatalf("session idle = %v", cfg.sessionIdleTimeout())
		}
	})
}

func TestConfigStringHasNoSecrets(t *testing.T) {
	cfg := baseConfig()
	cfg.ControlPlane.AuthTokenEnv = "KVC_TEST_TOKEN"
	s := cfg.String()
	if strings.Contains(s, "secret") || strings.Contains(s, "token=") {
		t.Fatalf("config string leaks secrets: %s", s)
	}
}
