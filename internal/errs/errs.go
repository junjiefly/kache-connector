// Package errs defines the stable connector error codes.
package errs

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Code string

const (
	InvalidArgument       Code = "INVALID_ARGUMENT"
	ChunkTooLarge         Code = "CHUNK_TOO_LARGE"
	BatchTooLarge         Code = "BATCH_TOO_LARGE"
	PayloadLengthMismatch Code = "PAYLOAD_LENGTH_MISMATCH"
	KeyNotFound           Code = "KEY_NOT_FOUND"
	KeyExists             Code = "KEY_EXISTS"
	PutNotPrepared        Code = "PUT_NOT_PREPARED"
	PutExpired            Code = "PUT_EXPIRED"
	PlanExpired           Code = "PLAN_EXPIRED"
	InvalidWritePlan      Code = "INVALID_WRITE_PLAN"
	InvalidReadPlan       Code = "INVALID_READ_PLAN"
	UnsupportedRefKind    Code = "UNSUPPORTED_REF_KIND"
	WriteTargetFailed     Code = "WRITE_TARGET_FAILED"
	ReadTargetFailed      Code = "READ_TARGET_FAILED"
	HashMismatch          Code = "HASH_MISMATCH"
	TransportUnavailable  Code = "TRANSPORT_UNAVAILABLE"
	PermissionDenied      Code = "PERMISSION_DENIED"
	ServerBusy            Code = "SERVER_BUSY"
	Internal              Code = "INTERNAL"
)

type Error struct {
	Code    Code
	Message string
	Err     error
}

func New(code Code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func Wrap(code Code, err error, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), Err: err}
}

func (e *Error) Error() string {
	if e.Err != nil {
		return string(e.Code) + ": " + e.Message + ": " + e.Err.Error()
	}
	return string(e.Code) + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Err }

// From maps an arbitrary error to a stable code.
func From(err error) Code {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	if errors.Is(err, context.Canceled) {
		return InvalidArgument
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return TransportUnavailable
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.InvalidArgument, codes.OutOfRange:
			return InvalidArgument
		case codes.NotFound:
			return KeyNotFound
		case codes.AlreadyExists:
			return KeyExists
		case codes.PermissionDenied, codes.Unauthenticated:
			return PermissionDenied
		case codes.ResourceExhausted:
			return ServerBusy
		case codes.Unavailable, codes.DeadlineExceeded:
			return TransportUnavailable
		case codes.FailedPrecondition:
			return PutNotPrepared
		case codes.Internal, codes.Unknown, codes.DataLoss:
			return Internal
		}
	}
	return Internal
}

// AsError converts err into an *Error, preserving an existing code.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return Wrap(From(err), err, "%s", err.Error())
}
