package rootpublication

import "sync"

// OriginalCleanupPriorBinding borrows the mutex and empty prior edge embedded
// in an original admitted cleanup receipt. It is a call-local value, not an
// owner, allocation, or error keeper. The caller retains the original error
// throughout selection and binding and discards this value before releasing it.
//
// Selecting an error may invoke arbitrary error methods outside the DB latch.
// Binding the selected edge invokes only concrete sync operations and a header
// assignment, so it can safely revalidate the DB error revision under its latch.
type OriginalCleanupPriorBinding struct {
	mutex   *sync.Mutex
	rwMutex *sync.RWMutex
	prior   *error
}

func BindOriginalCleanupPrior(mutex *sync.Mutex, prior *error) OriginalCleanupPriorBinding {
	if mutex == nil || prior == nil {
		return OriginalCleanupPriorBinding{}
	}
	return OriginalCleanupPriorBinding{mutex: mutex, prior: prior}
}

func BindOriginalCleanupPriorRW(mutex *sync.RWMutex, prior *error) OriginalCleanupPriorBinding {
	if mutex == nil || prior == nil {
		return OriginalCleanupPriorBinding{}
	}
	return OriginalCleanupPriorBinding{rwMutex: mutex, prior: prior}
}

func (binding OriginalCleanupPriorBinding) Valid() bool {
	return binding.prior != nil && (binding.mutex != nil || binding.rwMutex != nil)
}

func (binding OriginalCleanupPriorBinding) lock() {
	if binding.mutex != nil {
		binding.mutex.Lock()
	} else {
		binding.rwMutex.Lock()
	}
}

func (binding OriginalCleanupPriorBinding) unlock() {
	if binding.mutex != nil {
		binding.mutex.Unlock()
	} else {
		binding.rwMutex.Unlock()
	}
}

func (binding OriginalCleanupPriorBinding) Available() bool {
	if !binding.Valid() {
		return false
	}
	binding.lock()
	available := *binding.prior == nil
	binding.unlock()
	return available
}

// TryLink writes only the already-selected original edge. Cycle checks and all
// error/interface callbacks must have completed before entering this method.
func (binding OriginalCleanupPriorBinding) TryLink(prior error) bool {
	if !binding.Valid() || prior == nil {
		return false
	}
	binding.lock()
	linked := *binding.prior == nil
	if linked {
		*binding.prior = prior
	}
	binding.unlock()
	return linked
}
