package db

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/commandwalbarrier"
)

type commandWALRawBarrier struct {
	id     uint64
	hook   func() error
	active bool
	mu     sync.Mutex
	wg     sync.WaitGroup
}

// RegisterCommandWALRawPublishBarrier registers a callback that raw command-WAL
// writers must run before appending a new raw KV command frame. Checkpoint also
// runs these callbacks in WAL-free mode, allowing registered higher-level
// executors to hand off pending acknowledged writes before capturing its root.
// WAL-free ordinary root publications do not run these callbacks. Higher-level
// command executors use this to drain already-appended staged command frames so
// raw KV publishes cannot create AppliedCommandLSN gaps. A caller takes
// exclusive pre-raw admission before it acquires the command-WAL publish mutex;
// hooks still run with that mutex held so existing staged publishers retain
// their atomic raw-publish contract. A higher-level drain that would wait for
// foreground publication returns an internal PendingDrain handoff instead;
// the boundary owner drops all its leases, drains, then reruns every hook. A hook must not append command-WAL frames,
// acquire LockCommandWALStaging, or call a path that does either. The returned unregister
// function waits for in-flight hooks and must not be called from the hook itself.
func (db *DB) RegisterCommandWALRawPublishBarrier(hook func() error) func() {
	if db == nil || hook == nil {
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
	if db == nil {
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
		err := func() error {
			defer barrier.wg.Done()
			return hook()
		}()
		if err != nil {
			// Restart at the owning boundary before visiting later barriers.
			// All barriers are rerun after the unlocked drain succeeds.
			if commandWALPendingDrain(err) != nil {
				if len(errs) != 0 {
					return errors.Join(errs...)
				}
				return err
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// drainNoWALSyncPublishBarriers covers acknowledged higher-level buffers before
// a public raw KV sync publication. Internal physical/ordered-root publishers
// bypass it so draining a collection cannot recursively drain itself.
func (db *DB) drainNoWALSyncPublishBarriers() error {
	for {
		// No-WAL hooks inspect collection-owned locks. Do not hold teardown:
		// their owner may need a backend lease while Close is queued.
		if db.closing.Load() {
			return ErrClosed
		}
		if db.readOnly {
			return ErrReadOnly
		}
		if err := db.publicationPoisonedError(); err != nil {
			return err
		}
		err := db.runCommandWALRawPublishBarriers()
		drain := commandWALPendingDrain(err)
		if drain == nil {
			return err
		}
		if err := drain.Drain(); err != nil {
			return err
		}
	}
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

// LockCommandWALPreparedPublish serializes the final raw publish for a
// prepared publisher that already owns the teardown and shared-admission
// leases returned by TryLockCommandWALPreparedPublish.
func (db *DB) LockCommandWALPreparedPublish() func() {
	return db.lockCommandWALRawPublish()
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
	for {
		db.teardownMu.RLock()
		if db.closing.Load() {
			db.teardownMu.RUnlock()
			return nil, ErrClosed
		}
		db.commandWALRawAdmissionMu.Lock()
		db.commandWALRawPublishMu.Lock()
		if db.closing.Load() {
			db.unlockCommandWALRawPublishWithAdmissionAndTeardown()
			return nil, ErrClosed
		}
		err := db.runCommandWALRawPublishBarriers()
		if err == nil {
			return db.unlockCommandWALRawPublishWithAdmissionAndTeardown, nil
		}
		db.unlockCommandWALRawPublishWithAdmissionAndTeardown()
		drain := commandWALPendingDrain(err)
		if drain == nil {
			return nil, err
		}
		if err := drain.Drain(); err != nil {
			return nil, err
		}
	}
}

func commandWALPendingDrain(err error) *commandwalbarrier.PendingDrain {
	var drain *commandwalbarrier.PendingDrain
	if errors.As(err, &drain) {
		return drain
	}
	return nil
}

func (db *DB) unlockCommandWALRawPublishWithAdmissionAndTeardown() {
	db.commandWALRawPublishMu.Unlock()
	db.commandWALRawAdmissionMu.Unlock()
	db.teardownMu.RUnlock()
}

// lockCommandWALPublishWithBarriersTeardownPinned is the inner form for root
// publishers that already own a teardown read lease. Reacquiring an RWMutex
// read lease while Close is queued would deadlock under writer preference.
func (db *DB) lockCommandWALPublishWithBarriersTeardownPinned() (func(), error) {
	db.commandWALRawAdmissionMu.Lock()
	db.commandWALRawPublishMu.Lock()
	if err := db.runCommandWALRawPublishBarriers(); err != nil {
		db.unlockCommandWALRawPublishWithAdmission()
		return nil, err
	}
	return db.unlockCommandWALRawPublishWithAdmission, nil
}

func (db *DB) unlockCommandWALRawPublishWithAdmission() {
	db.commandWALRawPublishMu.Unlock()
	db.commandWALRawAdmissionMu.Unlock()
}

// LockCommandWALStaging pins DB teardown and prevents any command-WAL
// append/publish path from starting while a higher-level command has appended a
// frame but not yet made it publishable. Staged publishers inherit both leases.
func (db *DB) LockCommandWALStaging() func() {
	return db.lockCommandWALRawPublishWithTeardown()
}

// CommandWALStagingGuardV1 owns the actual teardown/raw leases acquired by a
// command executor. Capture borrowers share this guard; they acquire no new
// teardown read lease and cannot release its original ownership.
type CommandWALStagingGuardV1 struct {
	ownership *commandWALStagingOwnershipV1
}

// Copies of the exported opaque handle share actual DB-minted ownership.
type commandWALStagingOwnershipV1 struct {
	unlock      func()
	db          *DB
	mu          sync.Mutex
	active      bool
	transferred bool
	intent      *CommandWALIntent
}

func (db *DB) LockCommandWALStagingGuardV1() (*CommandWALStagingGuardV1, error) {
	if db == nil || !db.commandWAL {
		return nil, ErrCommandWALUnsupported
	}
	db.teardownMu.RLock()
	if db.closing.Load() {
		db.teardownMu.RUnlock()
		return nil, ErrClosed
	}
	db.commandWALRawPublishMu.Lock()
	if db.closing.Load() {
		db.unlockCommandWALRawPublishWithTeardown()
		return nil, ErrClosed
	}
	return &CommandWALStagingGuardV1{ownership: &commandWALStagingOwnershipV1{db: db, active: true, unlock: db.unlockCommandWALRawPublishWithTeardown}}, nil
}

// LockCommandWALStagingGuardWithBarriersV1 mints the same typed staging
// ownership after the existing public barriers have drained. Its actual lease
// additionally owns raw admission, so Release must use that acquisition's unlock.
func (db *DB) LockCommandWALStagingGuardWithBarriersV1() (*CommandWALStagingGuardV1, error) {
	if db == nil || !db.commandWAL {
		return nil, ErrCommandWALUnsupported
	}
	unlock, err := db.LockCommandWALPublishWithBarriers()
	if err != nil {
		return nil, err
	}
	return &CommandWALStagingGuardV1{ownership: &commandWALStagingOwnershipV1{db: db, active: true, unlock: unlock}}, nil
}

// ValidateDBV1 verifies the actual live DB-minted guard before ownership transfer.
// Rejection does not release the original owner's lease.
func (guard *CommandWALStagingGuardV1) ValidateDBV1(db *DB) error {
	if guard == nil || guard.ownership == nil {
		return ErrCommandWALRejected
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if !held.active || db == nil || held.db != db {
		return ErrCommandWALRejected
	}
	return nil
}

// ClaimAppendOwnershipV1 transfers this exact DB-minted guard to one apply
// handle owner. Copies cannot create another independent release authority.
func (guard *CommandWALStagingGuardV1) ClaimAppendOwnershipV1(db *DB) error {
	if guard == nil || guard.ownership == nil {
		return ErrCommandWALRejected
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if !held.active || db == nil || held.db != db || held.intent != nil || held.transferred {
		return ErrCommandWALRejected
	}
	held.transferred = true
	return nil
}

// Append assigns this guard to the actual staged intent before any borrowing.
func (guard *CommandWALStagingGuardV1) Append(intent *CommandWALIntent, sync bool) (uint64, error) {
	if guard == nil || guard.ownership == nil {
		return 0, ErrClosed
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if !held.active || held.db == nil || held.intent != nil || intent == nil || intent.AssignedLSN() != 0 {
		return 0, ErrCommandWALRejected
	}
	lsn, err := held.db.AppendStagedCommandWALIntent(intent, sync)
	if err == nil && lsn != 0 {
		held.intent = intent
	}
	return lsn, err
}

// Release expires all capture borrowers at Finalize/Abort, then relinquishes
// raw before teardown. Borrowers are callback-scoped and must finish first.
func (guard *CommandWALStagingGuardV1) Release() {
	if guard == nil || guard.ownership == nil {
		return
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if !held.active {
		return
	}
	held.active = false
	held.unlock()
}

func (guard *CommandWALStagingGuardV1) validateCaptureBorrowLocked(db *DB, intent *CommandWALIntent) error {
	if guard == nil || guard.ownership == nil {
		return ErrCommandWALRejected
	}
	held := guard.ownership
	if !held.active || db == nil || held.db != db || intent == nil || held.intent != intent || !intent.StagedForPublish() || intent.AssignedLSN() == 0 {
		return ErrCommandWALRejected
	}
	return nil
}

// BorrowStableResourceCaptureLeaseV1 validates the live guard's exact DB and
// intent. It stays usable if Close starts after Append, until guard release.
func (guard *CommandWALStagingGuardV1) BorrowStableResourceCaptureLeaseV1(db *DB, intent *CommandWALIntent) (*StableResourceCaptureLease, error) {
	if guard == nil || guard.ownership == nil {
		return nil, ErrClosed
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if err := guard.validateCaptureBorrowLocked(db, intent); err != nil {
		return nil, err
	}
	return &StableResourceCaptureLease{db: db, borrowedGuard: guard, borrowedIntent: intent}, nil
}
