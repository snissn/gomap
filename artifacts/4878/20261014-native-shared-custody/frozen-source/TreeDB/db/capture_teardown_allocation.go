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

// Each record belongs to the SAME admitted hook array. Completion data is
// callback-free: it does not retain the disposing authority header.
type captureTeardownHook struct {
	hook             func() error
	completionOwner  *retainedalloc.Owner
	completionCharge uint64
}

// Producers already hold the original teardown read lease and admit their
// callback controls before registration. The DB admits actual array growth.
func (db *DB) tryRegisterCaptureTeardownHookWithMetadata(hook func() error, metadata *retainedalloc.Owner) (bool, error) {
	return db.tryRegisterCaptureTeardownRecord(captureTeardownHook{hook: hook}, metadata)
}

// A successful callback has removed every authority payload/physical alias but
// leaves this exact containing/control charge admitted until its caller clears
// the method value. A failed callback leaves the original record and debt.
func (db *DB) tryRegisterCaptureTeardownOwnedHook(hook func() error, registryMetadata, completionOwner *retainedalloc.Owner, completionCharge uint64) (bool, error) {
	if completionOwner == nil || completionCharge == 0 {
		return false, rootpublication.ErrResourceOwnership
	}
	return db.tryRegisterCaptureTeardownRecord(captureTeardownHook{hook: hook, completionOwner: completionOwner, completionCharge: completionCharge}, registryMetadata)
}
func (db *DB) tryRegisterCaptureTeardownRecord(record captureTeardownHook, metadata *retainedalloc.Owner) (bool, error) {
	if db == nil || record.hook == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	db.captureTeardownHooksMu.Lock()
	defer db.captureTeardownHooksMu.Unlock()
	if db.captureTeardownHooksClosed {
		return false, nil
	}
	if err := db.appendCaptureTeardownHookLocked(record, metadata); err != nil {
		return false, err
	}
	return true, nil
}
func (db *DB) appendCaptureTeardownHookLocked(record captureTeardownHook, metadata *retainedalloc.Owner) error {
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
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(record)))
		if selected != nil {
			if err := selected.AddPending(charge); err != nil {
				return err
			}
		}
		old, oldCharge := db.captureTeardownHooks, db.captureTeardownHooksCharge
		next := make([]captureTeardownHook, len(old), capacity)
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
	db.captureTeardownHooks = append(db.captureTeardownHooks, record)
	return nil
}
