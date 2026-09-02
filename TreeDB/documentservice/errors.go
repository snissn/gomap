package documentservice

import (
	"errors"
	"fmt"
	"net/http"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// ErrorCode is the stable, pre-alpha service error vocabulary returned by the
// HTTP API and exposed by Go service methods.
type ErrorCode string

const (
	CodeInvalidRequest   ErrorCode = "invalid_request"
	CodeMalformedJSON    ErrorCode = "malformed_json"
	CodeIndexNotFound    ErrorCode = "index_not_found"
	CodeIndexUnavailable ErrorCode = "index_unavailable"
	CodeIndexStale       ErrorCode = "index_stale"
	CodeSnapshotMismatch ErrorCode = "snapshot_mismatch"
	CodeConflict         ErrorCode = "conflict"
	CodeUnsupported      ErrorCode = "unsupported"
	CodeInternal         ErrorCode = "internal"
)

// Error is a structured service error. Callers should branch on Code rather
// than matching message text.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Err     error     `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return string(e.Code) + ": " + e.Message
	}
	return string(e.Code)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func serviceError(code ErrorCode, message string) error {
	return &Error{Code: code, Message: message}
}

func serviceErrorf(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func wrapServiceError(code ErrorCode, message string, err error) error {
	if err == nil {
		return serviceError(code, message)
	}
	return &Error{Code: code, Message: message, Err: err}
}

// ErrorCodeOf returns the service code for err. Non-service errors are internal.
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var serviceErr *Error
	if errors.As(err, &serviceErr) && serviceErr != nil {
		return serviceErr.Code
	}
	return CodeInternal
}

func httpStatusForError(err error) int {
	switch ErrorCodeOf(err) {
	case CodeInvalidRequest, CodeMalformedJSON:
		return http.StatusBadRequest
	case CodeIndexNotFound:
		return http.StatusNotFound
	case CodeConflict, CodeIndexStale, CodeSnapshotMismatch:
		return http.StatusConflict
	case CodeIndexUnavailable:
		return http.StatusServiceUnavailable
	case CodeUnsupported:
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

func serviceErrorFromCollectionOpen(err error, index string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, backenddb.ErrClosed) {
		return wrapServiceError(CodeIndexUnavailable, "TreeDB backend is closed", err)
	}
	return wrapServiceError(CodeIndexNotFound, fmt.Sprintf("index %q was not found", index), err)
}
