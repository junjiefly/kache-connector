package kvcache

import (
	"log/slog"
	"time"

	"github.com/junjiefly/kache-connector/internal/controlclient"
	"github.com/junjiefly/kache-connector/transport"
)

// Option customizes connector construction. Options are primarily used by
// tests to inject transports and control-plane connections.
type Option func(*options)

type options struct {
	logger    *slog.Logger
	clock     func() time.Time
	dialer    controlclient.DialFunc
	factories []transport.Factory
}

// WithLogger sets the structured logger. Tokens, capabilities, opaque refs
// and payloads are never logged.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithClock overrides the time source (used by expiry tests).
func WithClock(f func() time.Time) Option {
	return func(o *options) { o.clock = f }
}

// WithControlDialer overrides how control-plane gRPC connections are created.
func WithControlDialer(d controlclient.DialFunc) Option {
	return func(o *options) { o.dialer = d }
}

// WithFactories injects the transport factories that execute server data-plane
// plans. The open-source core does not provide RDMA/TCP factories; production
// builds must inject them from a private transport adapter.
func WithFactories(f ...transport.Factory) Option {
	return func(o *options) { o.factories = f }
}

func buildOptions(opts []Option) options {
	o := options{
		logger: slog.New(slog.DiscardHandler),
		clock:  time.Now,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}
