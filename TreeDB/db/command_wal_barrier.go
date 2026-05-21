package db

import (
	"errors"
	"sync"
)

type commandWALRawBarrier struct {
	id   uint64
	hook func() error
	wg   sync.WaitGroup
}

// RegisterCommandWALRawPublishBarrier registers a callback that raw command-WAL
// writers must run before appending a new raw KV command frame. Higher-level
// command executors use this to drain already-appended staged command frames so
// raw KV publishes cannot create AppliedCommandLSN gaps.
func (db *DB) RegisterCommandWALRawPublishBarrier(hook func() error) func() {
	if db == nil || hook == nil || !db.commandWAL {
		return func() {}
	}
	db.closeHooksMu.Lock()
	if !db.acceptingCloseHooksLocked() {
		db.closeHooksMu.Unlock()
		return func() {}
	}
	db.commandWALRawBarrierMu.Lock()
	db.commandWALRawBarrierNextID++
	id := db.commandWALRawBarrierNextID
	barrier := &commandWALRawBarrier{id: id, hook: hook}
	db.commandWALRawBarriers = append(db.commandWALRawBarriers, barrier)
	db.commandWALRawBarrierMu.Unlock()
	db.closeHooksMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			var removed *commandWALRawBarrier
			db.commandWALRawBarrierMu.Lock()
			for i := range db.commandWALRawBarriers {
				if db.commandWALRawBarriers[i] != nil && db.commandWALRawBarriers[i].id == id {
					removed = db.commandWALRawBarriers[i]
					copy(db.commandWALRawBarriers[i:], db.commandWALRawBarriers[i+1:])
					last := len(db.commandWALRawBarriers) - 1
					db.commandWALRawBarriers[last] = nil
					db.commandWALRawBarriers = db.commandWALRawBarriers[:last]
					break
				}
			}
			db.commandWALRawBarrierMu.Unlock()
			if removed != nil {
				removed.wg.Wait()
			}
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
	barriers := make([]*commandWALRawBarrier, 0, len(db.commandWALRawBarriers))
	for _, barrier := range db.commandWALRawBarriers {
		if barrier != nil && barrier.hook != nil {
			barrier.wg.Add(1)
			barriers = append(barriers, barrier)
		}
	}
	db.commandWALRawBarrierMu.Unlock()

	var errs []error
	for _, barrier := range barriers {
		func() {
			defer barrier.wg.Done()
			if err := barrier.hook(); err != nil {
				errs = append(errs, err)
			}
		}()
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
