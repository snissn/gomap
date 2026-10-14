package db

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// The error receipt is part of the original DB, not another hook registry. Its
// cause backing remains admitted when a callback reports retained cleanup debt.
type captureTeardownFailure struct{ causes []error }

func (e *captureTeardownFailure) Error() string   { return "capture terminal cleanup failed" }
func (e *captureTeardownFailure) Unwrap() []error { return e.causes }

// The producer already holds its original teardown read lease. Callback
// controls are admitted by that producer; this helper admits only the SAME DB
// registry's actual array growth before allocation and copying.
func (db *DB) tryRegisterCaptureTeardownHookWithMetadata(hook func() error, metadata *retainedalloc.Owner) (bool, error) {
	if db == nil || hook == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	db.captureTeardownHooksMu.Lock()
	defer db.captureTeardownHooksMu.Unlock()
	if db.captureTeardownHooksClosed {
		return false, nil
	}
	if err := db.appendCaptureTeardownHookLocked(hook, metadata); err != nil {
		return false, err
	}
	return true, nil
}
func (db *DB) appendCaptureTeardownHookLocked(hook func() error, metadata *retainedalloc.Owner) error {
	var selected *retainedalloc.Owner
	if db.valueLogIdentityPins != nil {
		selected = db.valueLogIdentityPins.MetadataOwner()
	}
	if metadata != selected {
		return rootpublication.ErrResourceOwnership
	}
	if db.captureTeardownHooksOwner != nil && db.captureTeardownHooksOwner != selected {
		return rootpublication.ErrResourceOwnership
	}
	if selected != nil && cap(db.captureTeardownHooks) != 0 && db.captureTeardownHooksOwner == nil {
		return rootpublication.ErrResourceOwnership
	}
	if len(db.captureTeardownHooks) == cap(db.captureTeardownHooks) {
		capacity := cap(db.captureTeardownHooks) * 2
		if capacity == 0 {
			capacity = 1
		}
		if capacity < cap(db.captureTeardownHooks) {
			return retainedalloc.ErrCapacity
		}
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(hook)))
		if selected != nil {
			if err := selected.AddPending(charge); err != nil {
				return err
			}
		}
		old, oldCharge := db.captureTeardownHooks, db.captureTeardownHooksCharge
		next := make([]func() error, len(old), capacity)
		copy(next, old)
		clear(old)
		db.captureTeardownHooks = next
		db.captureTeardownHooksOwner = selected
		db.captureTeardownHooksCharge = charge
		old = nil
		if selected != nil && oldCharge != 0 {
			selected.RemovePending(oldCharge)
		}
	}
	db.captureTeardownHooks = append(db.captureTeardownHooks, hook)
	return nil
}
