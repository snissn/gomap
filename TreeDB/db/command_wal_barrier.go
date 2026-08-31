package db

import (
	"errors"
	"sync"
)

type commandWALRawBarrier struct {
	id     uint64
	hook   func() error
	active bool
	mu     sync.Mutex
	wg     sync.WaitGroup
}

// RegisterCommandWALRawPublishBarrier registers a callback that raw command-WAL
// writers must run before appending a new raw KV command frame. Higher-level
// command executors use this to drain already-appended staged command frames so
// raw KV publishes cannot create AppliedCommandLSN gaps. A caller takes
// exclusive pre-raw admission before it acquires the command-WAL publish mutex;
// hooks still run with that mutex held so existing staged publishers retain
// their atomic raw-publish contract. The returned unregister
// function waits for in-flight hooks and must not be called from the hook itself.
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
	barrier := &commandWALRawBarrier{id: id, hook: hook, active: true}
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
				removed.mu.Lock()
				removed.active = false
				removed.mu.Unlock()
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
			barriers = append(barriers, barrier)
		}
	}
	db.commandWALRawBarrierMu.Unlock()

	var errs []error
	for _, barrier := range barriers {
		barrier.mu.Lock()
		if !barrier.active || barrier.hook == nil {
			barrier.mu.Unlock()
			continue
		}
		hook := barrier.hook
		barrier.wg.Add(1)
		barrier.mu.Unlock()
		func() {
			defer barrier.wg.Done()
			if err := hook(); err != nil {
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

// TryLockCommandWALPreparedPublish claims shared pre-raw admission for a
// prepared higher-level publisher. It never waits: a quiescent boundary that
// is pending or active makes it return false so the caller can relinquish its
// prepared work and let that boundary drain it synchronously. It claims teardown
// opportunistically first, preserving teardown -> admission -> raw order.
func (db *DB) TryLockCommandWALPreparedPublish() (func(), bool) {
	if db == nil || !db.commandWAL {
		return func() {}, true
	}
	if db.closing.Load() || !db.teardownMu.TryRLock() {
		return nil, false
	}
	if db.closing.Load() {
		db.teardownMu.RUnlock()
		return nil, false
	}
	if !db.commandWALRawAdmissionMu.TryRLock() {
		db.teardownMu.RUnlock()
		return nil, false
	}
	return func() {
		db.commandWALRawAdmissionMu.RUnlock()
		db.teardownMu.RUnlock()
	}, true
}

// lockCommandWALQuiescentAdmission prevents newly prepared publishers from
// entering their final raw publish and waits for a publisher that already won
// shared admission. Callers already hold teardown and acquire raw afterwards.
func (db *DB) lockCommandWALQuiescentAdmission() func() {
	if db == nil || !db.commandWAL {
		return func() {}
	}
	db.commandWALRawAdmissionMu.Lock()
	return db.commandWALRawAdmissionMu.Unlock
}

// lockCommandWALRawPublishWithTeardown preserves the global shutdown order:
// root publishers and ordinary batches acquire teardown before raw publish, so
// higher-level command-WAL guards must do the same. Release raw first so Close
// can never observe a teardown reader that is waiting on a lock owned by a
// later reader.
func (db *DB) lockCommandWALRawPublishWithTeardown() func() {
	if db == nil || !db.commandWAL {
		return func() {}
	}
	db.teardownMu.RLock()
	db.commandWALRawPublishMu.Lock()
	return db.unlockCommandWALRawPublishWithTeardown
}

func (db *DB) unlockCommandWALRawPublishWithTeardown() {
	db.commandWALRawPublishMu.Unlock()
	db.teardownMu.RUnlock()
}

// LockCommandWALPublish pins DB teardown and serializes a higher-level
// command-WAL publish without running raw-publish barriers. Callers must arrange
// any higher-level draining required before appending or publishing under the
// returned guard.
func (db *DB) LockCommandWALPublish() func() {
	return db.lockCommandWALRawPublishWithTeardown()
}

// LockCommandWALPublishWithBarriers serializes a public command-WAL append and
// drains registered staged-command barriers before the caller appends a frame.
// The returned guard also pins teardown and must be released after raw publish.
func (db *DB) LockCommandWALPublishWithBarriers() (func(), error) {
	if db == nil || !db.commandWAL {
		return func() {}, nil
	}
	db.teardownMu.RLock()
	unlockAdmission := db.lockCommandWALQuiescentAdmission()
	unlockRaw := db.lockCommandWALRawPublish()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		unlockRaw()
		unlockAdmission()
		db.teardownMu.RUnlock()
		return nil, err
	}
	return func() {
		unlockRaw()
		unlockAdmission()
		db.teardownMu.RUnlock()
	}, nil
}

// lockCommandWALPublishWithBarriersTeardownPinned is the inner form for root
// publishers that already own a teardown read lease. Reacquiring an RWMutex
// read lease while Close is queued would deadlock under writer preference.
func (db *DB) lockCommandWALPublishWithBarriersTeardownPinned() (func(), error) {
	unlockAdmission := db.lockCommandWALQuiescentAdmission()
	unlockRaw := db.lockCommandWALRawPublish()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		unlockRaw()
		unlockAdmission()
		return nil, err
	}
	return func() {
		unlockRaw()
		unlockAdmission()
	}, nil
}

// LockCommandWALStaging pins DB teardown and prevents any command-WAL
// append/publish path from starting while a higher-level command has appended a
// frame but not yet made it publishable. Staged publishers inherit both leases.
func (db *DB) LockCommandWALStaging() func() {
	return db.lockCommandWALRawPublishWithTeardown()
}
