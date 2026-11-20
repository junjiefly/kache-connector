package kvcache

import (
	"context"
	"testing"

	"github.com/junjiefly/kache-connector/internal/errs"
)

func TestNewWithoutInjectedTransportFails(t *testing.T) {
	cfg := Config{
		Namespace: "test-ns",
		ControlPlane: ControlPlaneConfig{
			Endpoints:        []string{"127.0.0.1:9100"},
			InsecureTestMode: true,
		},
		Transport: TransportConfig{
			Mode: "tcp",
			TCP:  TCPTransportConfig{Enabled: true},
		},
	}
	_, err := New(context.Background(), cfg)
	if errs.From(err) != errs.TransportUnavailable {
		t.Fatalf("want TRANSPORT_UNAVAILABLE, got %v", err)
	}
}
