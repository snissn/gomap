package mvccadmission

import (
	"fmt"
	"sync"
)

// CompletionLease transfers an already-held Store floor read lock to storage.
// Release is independent of future observation, and is safe on every failure
// path. It introduces no publication authority or user callback.
type CompletionLease struct {
	once         sync.Once
	lock         *sync.RWMutex
	failureClass error
	operation    string
}

func NewCompletionLease(lock *sync.RWMutex, failureClass error, operation string) *CompletionLease {
	return &CompletionLease{lock: lock, failureClass: failureClass, operation: operation}
}

func (l *CompletionLease) Release() {
	if l != nil {
		l.once.Do(func() { l.lock.RUnlock() })
	}
}

// Result preserves the submitting Store's storage-error classification without
// transferring an arbitrary callback into the database completion owner.
func (l *CompletionLease) Result(err error) error {
	if err == nil || l == nil || l.failureClass == nil {
		return err
	}
	return fmt.Errorf("%w: %s: %w", l.failureClass, l.operation, err)
}
