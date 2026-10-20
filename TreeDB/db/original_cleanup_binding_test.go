package db

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type callbackPriorBindingError struct {
	db            *DB
	mu            sync.Mutex
	prior         error
	selections    int
	legacyInvoked bool
}

func (*callbackPriorBindingError) Error() string { return "original binding test" }
func (failure *callbackPriorBindingError) OriginalCleanupPriorBinding() rootpublication.OriginalCleanupPriorBinding {
	// This is a deliberately reentrant selection callback. The DB error latch
	// must already be released before any structural interface method runs.
	_ = failure.db.backgroundError()
	failure.selections++
	return rootpublication.BindOriginalCleanupPrior(&failure.mu, &failure.prior)
}
func (failure *callbackPriorBindingError) LinkPriorCleanupError(error) {
	failure.legacyInvoked = true
}
func (failure *callbackPriorBindingError) OriginalCleanupPriorAvailable() bool {
	failure.legacyInvoked = true
	return true
}
func (failure *callbackPriorBindingError) TryLinkPriorCleanupError(error) bool {
	failure.legacyInvoked = true
	return false
}

func TestOriginalCleanupBindingKeepsCallbacksOutsideErrorLatch(t *testing.T) {
	prior := errors.New("original previous failure")
	database := &DB{bgErr: prior}
	incoming := &callbackPriorBindingError{db: database}
	done := make(chan struct{})
	go func() {
		database.retainOriginalCleanupError(incoming)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("selection callback blocked on the DB error latch")
	}
	if database.backgroundError() != incoming || incoming.prior != prior || incoming.selections != 1 || incoming.legacyInvoked {
		t.Fatalf("original binding changed custody or invoked legacy callback: root=%v prior=%v selections=%d legacy=%v", database.backgroundError(), incoming.prior, incoming.selections, incoming.legacyInvoked)
	}
}
