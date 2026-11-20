package controlclient

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryable reports whether a control-plane call may be retried. Only
// idempotent calls are wrapped with Do, so this is safe for all of them.
func retryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.Unavailable || s.Code() == codes.ResourceExhausted
}

// Do retries fn with exponential backoff while the context permits.
func Do(ctx context.Context, attempts int, base time.Duration, fn func(context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn(ctx)
		if err == nil || !retryable(err) {
			return err
		}
		if i == attempts-1 {
			break
		}
		delay := base << i
		if delay > 200*time.Millisecond {
			delay = 200 * time.Millisecond
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return err
}
