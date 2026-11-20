// Package controlclient implements the gRPC control-plane client with auth,
// endpoint failover, one overall deadline per call and idempotent retries.
package controlclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/junjiefly/kache-connector/internal/errs"
	kachev1 "github.com/junjiefly/kache-connector/proto/kache/v1"
)

// Client is the control-plane surface used by the connector.
type Client interface {
	Caps(ctx context.Context, namespace string) (*kachev1.GetCapsReply, error)
	Match(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error)
	Resolve(ctx context.Context, req *kachev1.ResolveBatchRequest) (*kachev1.ResolveBatchReply, error)
	Prepare(ctx context.Context, req *kachev1.PreparePutBatchRequest) (*kachev1.PreparePutBatchReply, error)
	Commit(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error)
	Abort(ctx context.Context, req *kachev1.AbortPutBatchRequest) (*kachev1.AbortPutBatchReply, error)
	Delete(ctx context.Context, req *kachev1.DeleteBatchRequest) (*kachev1.DeleteBatchReply, error)
	ListServers(ctx context.Context, req *kachev1.ListControlServersRequest) (*kachev1.ListControlServersReply, error)
	Close() error
}

// DialFunc creates a gRPC connection for one endpoint.
type DialFunc func(ctx context.Context, endpoint string, tlsCfg *tls.Config, timeout time.Duration) (*grpc.ClientConn, error)

type Options struct {
	Endpoints []string
	Timeout   time.Duration
	Token     string
	TLS       *tls.Config
	Dialer    DialFunc
	Logger    *slog.Logger
}

type client struct {
	conns   []*grpc.ClientConn
	stubs   []kachev1.KacheControlClient
	timeout time.Duration
	token   string
	log     *slog.Logger
	mu      sync.Mutex
	sticky  map[string]int
}

func New(ctx context.Context, opts Options) (Client, error) {
	if len(opts.Endpoints) == 0 {
		return nil, errs.New(errs.InvalidArgument, "no control-plane endpoints")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	dial := opts.Dialer
	if dial == nil {
		dial = defaultDial
	}
	c := &client{timeout: opts.Timeout, token: opts.Token, log: opts.Logger, sticky: map[string]int{}}
	for _, ep := range opts.Endpoints {
		conn, err := dial(ctx, ep, opts.TLS, opts.Timeout)
		if err != nil {
			_ = c.Close()
			return nil, errs.Wrap(errs.TransportUnavailable, err, "dial control plane %s", ep)
		}
		c.conns = append(c.conns, conn)
		c.stubs = append(c.stubs, kachev1.NewKacheControlClient(conn))
	}
	return c, nil
}

func defaultDial(ctx context.Context, endpoint string, tlsCfg *tls.Config, timeout time.Duration) (*grpc.ClientConn, error) {
	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}
	return grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))
}

// LoadTLS builds a TLS config from file paths.
func LoadTLS(caFile, certFile, keyFile, serverName string) (*tls.Config, error) {
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in CA file")
		}
		cfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// call applies one overall deadline for the whole operation (including all
// retries and endpoint failover), then retries idempotent calls within it.
func (c *client) call(ctx context.Context, op string, fn func(context.Context, kachev1.KacheControlClient) error, attrs ...any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	err := Do(ctx, retryAttempts, retryBase, func(ctx context.Context) error {
		return c.invoke(ctx, fn)
	})
	if err != nil {
		c.log.Warn("control-plane call failed", append([]any{"op", op, "code", errs.From(err)}, attrs...)...)
	}
	return err
}

// invoke tries endpoints in configured order under the shared deadline.
func (c *client) invoke(ctx context.Context, fn func(context.Context, kachev1.KacheControlClient) error) error {
	if c.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
	}
	var last error
	for _, stub := range c.stubs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := fn(ctx, stub)
		if err == nil {
			return nil
		}
		last = err
		if !retryable(err) {
			return err
		}
	}
	return last
}

func wrapErr(err error) error {
	if err == nil {
		return nil
	}
	return errs.AsError(err)
}

// invokeSticky targets exactly one endpoint. Put plans live in Control-local
// memory, so Commit/Abort must stay on the node that ran Prepare; failing
// over would only produce misleading PUT_NOT_PREPARED replies.
func (c *client) invokeSticky(ctx context.Context, index int, fn func(context.Context, kachev1.KacheControlClient) error) error {
	if c.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
	}
	if index < 0 || index >= len(c.stubs) {
		return errs.New(errs.TransportUnavailable, "no sticky control endpoint")
	}
	return fn(ctx, c.stubs[index])
}

func (c *client) stickyIndex(putID string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	index, ok := c.sticky[putID]
	return index, ok
}

func (c *client) setSticky(putID string, index int) {
	if putID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sticky[putID] = index
}

func (c *client) clearSticky(putID string) {
	if putID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sticky, putID)
}

const retryAttempts = 3
const retryBase = 20 * time.Millisecond

func (c *client) Caps(ctx context.Context, namespace string) (*kachev1.GetCapsReply, error) {
	var rep *kachev1.GetCapsReply
	err := c.call(ctx, "GetCaps", func(ctx context.Context, s kachev1.KacheControlClient) error {
		r, err := s.GetCaps(ctx, &kachev1.GetCapsRequest{Namespace: namespace})
		rep = r
		return err
	})
	return rep, wrapErr(err)
}

func (c *client) Match(ctx context.Context, req *kachev1.MatchBatchRequest) (*kachev1.MatchBatchReply, error) {
	var rep *kachev1.MatchBatchReply
	err := c.call(ctx, "MatchBatch", func(ctx context.Context, s kachev1.KacheControlClient) error {
		r, err := s.MatchBatch(ctx, req)
		rep = r
		return err
	})
	return rep, wrapErr(err)
}

func (c *client) Resolve(ctx context.Context, req *kachev1.ResolveBatchRequest) (*kachev1.ResolveBatchReply, error) {
	var rep *kachev1.ResolveBatchReply
	err := c.call(ctx, "ResolveBatch", func(ctx context.Context, s kachev1.KacheControlClient) error {
		r, err := s.ResolveBatch(ctx, req)
		rep = r
		return err
	})
	return rep, wrapErr(err)
}

func (c *client) Prepare(ctx context.Context, req *kachev1.PreparePutBatchRequest) (*kachev1.PreparePutBatchReply, error) {
	var rep *kachev1.PreparePutBatchReply
	// Prepare is not retried: a lost response may already have created a
	// plan, and repeating the request would allocate duplicate blocks until
	// the plan TTL expires. Endpoints are visited once each under one
	// shared deadline so a down first endpoint can still be skipped.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var err error
	for i, s := range c.stubs {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		r, callErr := s.PreparePutBatch(ctx, req)
		err = callErr
		rep = r
		if callErr == nil {
			c.setSticky(r.GetPutId(), i)
			break
		}
		if !retryable(err) {
			break
		}
	}
	if err != nil {
		c.log.Warn("control-plane call failed", "op", "PreparePutBatch", "code", errs.From(err), "client_request_id", req.GetClientRequestId())
	}
	return rep, wrapErr(err)
}

func (c *client) Commit(ctx context.Context, req *kachev1.CommitPutBatchRequest) (*kachev1.CommitPutBatchReply, error) {
	var rep *kachev1.CommitPutBatchReply
	index, ok := c.stickyIndex(req.GetPutId())
	var err error
	if ok {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		err = Do(ctx, retryAttempts, retryBase, func(ctx context.Context) error {
			return c.invokeSticky(ctx, index, func(ctx context.Context, s kachev1.KacheControlClient) error {
				r, err := s.CommitPutBatch(ctx, req)
				rep = r
				return err
			})
		})
	} else {
		err = c.call(ctx, "CommitPutBatch", func(ctx context.Context, s kachev1.KacheControlClient) error {
			r, err := s.CommitPutBatch(ctx, req)
			rep = r
			return err
		}, "put_id", req.GetPutId())
	}
	if ok && err != nil {
		c.log.Warn("control-plane call failed", "op", "CommitPutBatch", "code", errs.From(err), "put_id", req.GetPutId())
	}
	c.clearSticky(req.GetPutId())
	return rep, wrapErr(err)
}

func (c *client) Abort(ctx context.Context, req *kachev1.AbortPutBatchRequest) (*kachev1.AbortPutBatchReply, error) {
	var rep *kachev1.AbortPutBatchReply
	index, ok := c.stickyIndex(req.GetPutId())
	var err error
	if ok {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		err = Do(ctx, retryAttempts, retryBase, func(ctx context.Context) error {
			return c.invokeSticky(ctx, index, func(ctx context.Context, s kachev1.KacheControlClient) error {
				r, err := s.AbortPutBatch(ctx, req)
				rep = r
				return err
			})
		})
	} else {
		err = c.call(ctx, "AbortPutBatch", func(ctx context.Context, s kachev1.KacheControlClient) error {
			r, err := s.AbortPutBatch(ctx, req)
			rep = r
			return err
		}, "put_id", req.GetPutId())
	}
	if ok && err != nil {
		c.log.Warn("control-plane call failed", "op", "AbortPutBatch", "code", errs.From(err), "put_id", req.GetPutId())
	}
	c.clearSticky(req.GetPutId())
	return rep, wrapErr(err)
}

func (c *client) Delete(ctx context.Context, req *kachev1.DeleteBatchRequest) (*kachev1.DeleteBatchReply, error) {
	var rep *kachev1.DeleteBatchReply
	err := c.call(ctx, "DeleteBatch", func(ctx context.Context, s kachev1.KacheControlClient) error {
		r, err := s.DeleteBatch(ctx, req)
		rep = r
		return err
	})
	return rep, wrapErr(err)
}

func (c *client) ListServers(ctx context.Context, req *kachev1.ListControlServersRequest) (*kachev1.ListControlServersReply, error) {
	var rep *kachev1.ListControlServersReply
	err := c.call(ctx, "ListControlServers", func(ctx context.Context, s kachev1.KacheControlClient) error {
		r, err := s.ListControlServers(ctx, req)
		rep = r
		return err
	})
	return rep, wrapErr(err)
}

func (c *client) Close() error {
	var first error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.conns = nil
	c.stubs = nil
	return first
}
