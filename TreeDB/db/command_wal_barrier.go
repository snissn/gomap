package db

import (
	"errors"
	"sync"
)

// RegisterCommandWALRawPublishBarrier registers a callback that raw command-WAL
// writers must run before appending a new raw KV command frame. Higher-level
// command executors use this to drain already-appended staged command frames so
// raw KV publishes cannot create AppliedCommandLSN gaps.
func (db *DB) RegisterCommandWALRawPublishBarrier(hook func() error) func() {
	if db == nil || hook == nil {
		return func() {}
	}
	db.commandWALRawBarrierMu.Lock()
	idx := len(db.commandWALRawBarriers)
	db.commandWALRawBarriers = append(db.commandWALRawBarriers, hook)
	db.commandWALRawBarrierMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.commandWALRawBarrierMu.Lock()
			if idx >= 0 && idx < len(db.commandWALRawBarriers) && db.commandWALRawBarriers[idx] != nil {
				db.commandWALRawBarriers[idx] = nil
			}
			db.commandWALRawBarrierMu.Unlock()
		})
	}
}

func (db *DB) runCommandWALRawPublishBarriers() error {
	if db == nil || !db.commandWAL {
		return nil
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	db.commandWALRawBarrierMu.Lock()
	hooks := append([]func() error(nil), db.commandWALRawBarriers...)
	db.commandWALRawBarrierMu.Unlock()

	var errs []error
	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		if err := hook(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (db *DB) lockCommandWALRawPublish() func() {
	if db == nil || !db.commandWAL {
		return func() {}
	}
	db.commandWALRawPublishMu.Lock()
	return db.commandWALRawPublishMu.Unlock
}

// LockCommandWALStaging prevents a raw command-WAL publish from starting while
// a higher-level command has appended a frame but not yet made it publishable.
func (db *DB) LockCommandWALStaging() func() {
	if db == nil || !db.commandWAL {
		return func() {}
	}
	db.commandWALRawPublishMu.RLock()
	return db.commandWALRawPublishMu.RUnlock
}
