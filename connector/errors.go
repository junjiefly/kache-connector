package kvcache

import "github.com/junjiefly/kache-connector/internal/errs"

// Error is a connector-level failure with a stable error code.
type Error = errs.Error

// Stable error codes shared by the Go API, the C ABI and Python results.
const (
	CodeInvalidArgument       = errs.InvalidArgument
	CodeChunkTooLarge         = errs.ChunkTooLarge
	CodeBatchTooLarge         = errs.BatchTooLarge
	CodePayloadLengthMismatch = errs.PayloadLengthMismatch
	CodeKeyNotFound           = errs.KeyNotFound
	CodeKeyExists             = errs.KeyExists
	CodePutNotPrepared        = errs.PutNotPrepared
	CodePutExpired            = errs.PutExpired
	CodePlanExpired           = errs.PlanExpired
	CodeInvalidWritePlan      = errs.InvalidWritePlan
	CodeInvalidReadPlan       = errs.InvalidReadPlan
	CodeUnsupportedRefKind    = errs.UnsupportedRefKind
	CodeWriteTargetFailed     = errs.WriteTargetFailed
	CodeReadTargetFailed      = errs.ReadTargetFailed
	CodeHashMismatch          = errs.HashMismatch
	CodeTransportUnavailable  = errs.TransportUnavailable
	CodePermissionDenied      = errs.PermissionDenied
	CodeServerBusy            = errs.ServerBusy
	CodeInternal              = errs.Internal
)

// ErrorCode extracts the stable code from an error (INTERNAL for unknown errors).
func ErrorCode(err error) errs.Code { return errs.From(err) }
