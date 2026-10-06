package caching

import (
	"context"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// The COW frontier is the bounded cut vector itself. The existing admission
// flags, command cutover hook, physical publication authority and durability
// hooks keep their ordering; legacy mutable rotation/queue copies are absent.
func (db *DB) checkpointCOWContext(ctx context.Context, automatic bool) error {
	if db.closing.Load() {
		return errDBClosing
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lockCheckpointMutexContext(ctx, &db.flushMu); err != nil {
		return err
	}
	defer db.flushMu.Unlock()
	db.checkpointMu.Lock()
	db.checkpointPostFrontierAdmission.Store(false)
	db.checkpointWriteCutoverActive.Store(true)
	db.checkpointing.Store(true)
	db.checkpointMu.Unlock()
	defer func() {
		db.checkpointMu.Lock()
		db.checkpointPostFrontierAdmission.Store(false)
		db.checkpointWriteCutoverActive.Store(false)
		db.checkpointing.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}()
	// A prior accepted physical publication must complete its handoff first.
	// Refusal keeps the covered prefix and never replays backend work.
	if err := db.completeCOWHandoff(); err != nil {
		return err
	}
	if db.cow.refreshRequired.Load() {
		if err := db.refreshCOWBackendBasis(); err != nil {
			return err
		}
	}
	if err := lockCheckpointWriteMutexContext(ctx, &db.writeMu); err != nil {
		return err
	}
	cutover := time.Now()
	if hook := db.testBeforeCheckpointFrontierCapture; hook != nil {
		hook()
	}
	c := db.cow
	c.writerMu.Lock()
	rollover, err := c.prepareRollover()
	if err == nil && rollover != nil {
		rollover.install()
		db.mutableBytes.Store(0)
	}
	c.writerMu.Unlock()
	if err == nil && db.snapshotCommandWALCheckpointCutover() && db.externalCommandWAL {
		db.checkpointPostFrontierAdmission.Store(true)
	}
	db.writeMu.Unlock()
	db.recordCheckpointCutover(time.Since(cutover))
	db.checkpointMu.Lock()
	db.checkpointWriteCutoverActive.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()
	rollover.drain()
	if err != nil {
		return err
	}
	if hook := db.testAfterCheckpointFrontierCapture; hook != nil {
		hook()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// COW admits only the canonical command-WAL and NoWAL cached profiles. Their
	// value log is independent persistent storage, synced before root coverage.
	if err = db.checkpointFlushValueLogLanes(); err != nil {
		return err
	}
	var publish checkpointCommandWALPublish
	if hook := db.loadCommandWALCheckpointPublishHook(); hook != nil {
		publish.appliedLSN, publish.ranges, err = hook(true)
		if err != nil {
			return err
		}
	}
	if hook := db.testBeforeCheckpointFrontierDrain; hook != nil {
		hook()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = db.flushCOWFrozen(true, &publish); err != nil {
		return err
	}
	if publish.appliedLSN != 0 && !publish.consumed {
		if _, err = db.publishCommandWALCheckpointApplied(publish.appliedLSN, publish.ranges); err != nil {
			return err
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = db.backendCheckpointBoundary(automatic); err != nil {
		return err
	}
	if publish.appliedLSN != 0 || publish.consumed {
		if err = db.cleanupCommandWALCheckpointBoundary(automatic, true); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) rolloverCOWForFlush() error {
	if err := db.beginDirectWrite(); err != nil {
		return err
	}
	c := db.cow
	c.writerMu.Lock()
	p, err := c.prepareRollover()
	if err == nil && p != nil {
		p.install()
		db.mutableBytes.Store(0)
	}
	c.writerMu.Unlock()
	db.writeMu.RUnlock()
	p.drain()
	return err
}

func (db *DB) flushCOWBackground(syncFlush bool) bool {
	if db.cow.readClosed.Load() {
		return false
	}
	if err := db.flushCOWFrozen(syncFlush, nil); err != nil {
		if err != backenddb.ErrSnapshotCapacity {
			if db.notifyError != nil {
				db.notifyError(err)
			}
		}
		return false
	}
	return true
}

// closeCOWFrontier consumes Close's writeMu fence and returns with it unlocked.
// closing already denies new admissions. All retirement, storage IO and read
// teardown exclusion occur after that fence is released.
func (db *DB) closeCOWFrontier() error {
	c := db.cow
	c.writerMu.Lock()
	p, err := c.prepareRollover()
	if err == nil && p != nil {
		p.install()
		db.mutableBytes.Store(0)
	}
	c.writerMu.Unlock()
	if err == nil {
		db.snapshotCommandWALCheckpointCutover()
	}
	db.writeMu.Unlock()
	p.drain()
	defer c.close()
	if err != nil {
		return err
	}
	if err = db.checkpointFlushValueLogLanes(); err != nil {
		return err
	}
	var publish checkpointCommandWALPublish
	if hook := db.loadCommandWALCheckpointPublishHook(); hook != nil {
		publish.appliedLSN, publish.ranges, err = hook(true)
		if err != nil {
			return err
		}
	}
	if err = db.flushCOWFrozen(true, &publish); err != nil {
		return err
	}
	if publish.appliedLSN != 0 && !publish.consumed {
		if _, err = db.publishCommandWALCheckpointApplied(publish.appliedLSN, publish.ranges); err != nil {
			return err
		}
	}
	if err = db.backendCheckpointBoundary(false); err != nil {
		return err
	}
	if publish.appliedLSN != 0 || publish.consumed {
		return db.cleanupCommandWALCheckpointBoundary(false, true)
	}
	return nil
}
