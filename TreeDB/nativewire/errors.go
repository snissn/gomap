package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

var ErrServerClosed = errors.New("nativewire: server is closed")

type WireError struct {
	Code      iwire.ErrorCode
	Retryable bool
	Message   string
}

func (e *WireError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("nativewire: remote error code %d", e.Code)
	}
	return fmt.Sprintf("nativewire: remote error code %d: %s", e.Code, e.Message)
}

func errorCodeFor(err error) iwire.ErrorCode {
	if err == nil {
		return 0
	}
	if code, ok := iwire.ErrorCodeOf(err); ok {
		return code
	}
	switch {
	case errors.Is(err, context.Canceled):
		return iwire.ErrCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return iwire.ErrTimeout
	case errors.Is(err, collections.ErrCollectionNotFound):
		return iwire.ErrCollectionNotFound
	case errors.Is(err, collections.ErrIndexNotFound):
		return iwire.ErrIndexNotFound
	case errors.Is(err, collections.ErrDuplicateDocumentID):
		return iwire.ErrDuplicateDocumentID
	case errors.Is(err, collections.ErrDocumentExists):
		return iwire.ErrDocumentExists
	case errors.Is(err, collections.ErrUniqueIndexConflict):
		return iwire.ErrUniqueIndexConflict
	case errors.Is(err, backenddb.ErrClosed), errors.Is(err, ErrServerClosed), errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		return iwire.ErrCanceled
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "duplicate document id"):
		return iwire.ErrDuplicateDocumentID
	case strings.Contains(msg, "document already exists"):
		return iwire.ErrDocumentExists
	case strings.Contains(msg, "unique index conflict"):
		return iwire.ErrUniqueIndexConflict
	case strings.Contains(msg, "collection not found"):
		return iwire.ErrCollectionNotFound
	case strings.Contains(msg, "index not found"):
		return iwire.ErrIndexNotFound
	}
	return iwire.ErrInternal
}

func retryableError(code iwire.ErrorCode) bool {
	switch code {
	case iwire.ErrTimeout, iwire.ErrCanceled, iwire.ErrResourceExhausted, iwire.ErrDurabilityUnavailable, iwire.ErrConsistencyUnavailable:
		return true
	default:
		return false
	}
}
