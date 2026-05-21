package db

import (
	"errors"
	"sync"
)

type commandWALRawBarrier struct {
	id   uint64
	hook func() error
}

// RegisterCommandWALRawPublishBarrier registers a callback that raw command-WAL
// writers must run before appending a new raw KV command frame. Higher-level
// command executors use this to drain already-appended staged command frames so
// raw KV publishes cannot create AppliedCommandLSN gaps.
func (db *DB) RegisterCommandWALRawPublishBarrier(hook func() error) func() {
	if db == nil || hook == nil {
		return func() {}
	}
	db.commandWALRawBarrierMu.Lock()
	db.commandWALRawBarrierNextID++
	id := db.commandWALRawBarrierNextID
	db.commandWALRawBarriers = append(db.commandWALRawBarriers, commandWALRawBarrier{id: id, hook: hook})
	db.commandWALRawBarrierMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.commandWALRawBarrierMu.Lock()
			for i := range db.commandWALRawBarriers {
				if db.commandWALRawBarriers[i].id == id {
					copy(db.commandWALRawBarriers[i:], db.commandWALRawBarriers[i+1:])
					last := len(db.commandWALRawBarriers) - 1
					db.commandWALRawBarriers[last] = commandWALRawBarrier{}
					db.commandWALRawBarriers = db.commandWALRawBarriers[:last]
					break
				}
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
	hooks := make([]func() error, 0, len(db.commandWALRawBarriers))
	for _, barrier := range db.commandWALRawBarriers {
		if barrier.hook != nil {
			hooks = append(hooks, barrier.hook)
		}
	}
	db.commandWALRawBarrierMu.Unlock()

	var errs []error
	for _, hook := range hooks {
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
