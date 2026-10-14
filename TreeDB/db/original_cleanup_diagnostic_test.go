package db

import (
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This read-only diagnostic preserves the original Close outcome and every
// assertion. Fixed original receipt fields are copied under their own lock.
func logOriginalCleanupDiagnostic(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Logf("cleanup outcome type=%T ownership=%v conflict=%v closed=%v recovery=%v",
		err, errors.Is(err, rootpublication.ErrResourceOwnership),
		errors.Is(err, rootpublication.ErrResourceConflict), errors.Is(err, os.ErrClosed),
		errors.Is(err, ErrRecoveryRequired))
	var visit func(error, int)
	visit = func(e error, depth int) {
		if e == nil || depth > 12 {
			return
		}
		t.Logf("cleanup depth=%d type=%T cause=%v", depth, e, e)
		if f, ok := e.(*originalDBCleanupFailure); ok {
			f.mu.Lock()
			causes, payload := f.causes, f.payload
			f.mu.Unlock()
			t.Logf("cleanup original payload=%T", payload)
			for _, cause := range causes {
				visit(cause, depth+1)
			}
		} else if joined, ok := e.(interface{ Unwrap() []error }); ok {
			for _, cause := range joined.Unwrap() {
				visit(cause, depth+1)
			}
		} else if wrapped, ok := e.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
}
