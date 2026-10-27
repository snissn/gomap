package caching

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// The COW frontier is the bounded cut vector itself. The existing admission
// flags, command cutover hook, physical publication authority and durability
// hooks keep their ordering; legacy mutable rotation/queue copies are absent.
func (db *DB) checkpointCOWContext(ctx context.Context, automatic bool) error {
	return db.checkpointCOWColumnCompaction(ctx, automatic, nil)
}

func (db *DB) CompactNativeColumnStore(ctx context.Context, operation backenddb.NativeColumnStoreCompaction) error {
	if operation == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return db.checkpointCOWColumnCompaction(ctx, false, operation)
}

func (db *DB) checkpointCOWColumnCompaction(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction) (retErr error) {
	start := time.Now()
	defer func() {
		dur := uint64(time.Since(start))
		db.checkpointRuns.Add(1)
		db.checkpointTotalNs.Add(dur)
		updateAtomicMaxUint64(&db.checkpointMaxNs, dur)
	}()
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
	var frontier *cowReadCut
	err := db.rotateCOWCheckpointFrontier(ctx, true, &frontier)
	defer func() { retErr = joinCOWCleanupError(retErr, db.cow.releaseCut(frontier)) }()
	db.recordCheckpointCutover(time.Since(cutover))
	db.checkpointMu.Lock()
	db.checkpointWriteCutoverActive.Store(false)
	db.checkpointCond.Broadcast()
	db.checkpointMu.Unlock()
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
	// Preserve the original captured endpoint before the flush consumes frontier.
	through := rootpublication.SealedPrefix{LogicalSequence: frontier.frozenLogicalSeq, CanonicalLSN: frontier.frozenCommandLSN}
	var publish checkpointCommandWALPublish
	if hook := db.testBeforeCheckpointFrontierDrain; hook != nil {
		hook()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	err = db.flushCOWFrozenCutWithColumnCompaction(true, &publish, frontier, operation != nil, operation)
	frontier = nil
	if err != nil {
		return err
	}
	if operation != nil {
		// An adopted compaction has completed its original build and handoff.
		// Finish this same checkpoint's durability and retirement boundary even
		// if its caller canceled during construction. Ordinary checkpoints keep
		// their existing cancellation semantics.
		ctx = context.WithoutCancel(ctx)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = db.backendCheckpointBoundary(automatic); err != nil {
		return err
	}
	if automatic && db.externalCommandWAL {
		if err = db.refreshCOWAutomaticCheckpointFallback(ctx, through); err != nil {
			return err
		}
	}
	if !automatic && db.cow.prefix != nil {
		// Physical WaitThrough wakes before the dispatcher finishes accepted
		// custody retirement. Join that SAME captured endpoint, including an
		// original release already running on the dispatcher. Keep flushMu so
		// direct Close cannot detach/refund prefix during this wait; accepted
		// custody only queues cleanup and needs none of the cache writer gates.
		if err = db.cow.prefix.WaitAccepted(ctx, through); err != nil {
			return err
		}
	}
	if publish.appliedLSN != 0 || publish.consumed {
		if err = db.cleanupCommandWALCheckpointBoundary(automatic, true); err != nil {
			return err
		}
	}
	return db.vacuumCOWCheckpointBasis(automatic)
}

// rotateCOWCheckpointFrontier consumes the caller's exclusive writeMu fence
// and returns with it unlocked. The existing checkpoint/Close admission fence
// excludes writers through the finite retry, while flushMu remains outer D.
func (db *DB) rotateCOWCheckpointFrontier(ctx context.Context, postFrontier bool, captured **cowReadCut) error {
	c := db.cow
	for attempt := 0; ; attempt++ {
		c.writerMu.Lock()
		rollover, err := c.prepareRollover()
		if err == nil && rollover != nil {
			rollover.install()
			db.mutableBytes.Store(0)
		}
		c.writerMu.Unlock()
		// writeMu still excludes every same-owner append. Rotate the journal at
		// this complete raw+semantic cut, independently of public raw notices.
		// Rotation is a file fence; only the canonical WAL owner can advance W.
		if err == nil && db.externalCommandWAL {
			wal, ok := db.backend.(interface {
				CommandWALActiveBytes() int64
				RotateCommandWALActiveSegment(bool) error
			})
			if !ok {
				err = ErrCOWUnsupported
			} else if wal.CommandWALActiveBytes() > 0 {
				err = wal.RotateCommandWALActiveSegment(true)
			}
		}
		if err == nil && db.snapshotCommandWALCheckpointCutover() && db.externalCommandWAL && postFrontier {
			db.checkpointPostFrontierAdmission.Store(true)
		}
		if err == nil && captured != nil {
			*captured = c.retainCut()
		}
		db.writeMu.Unlock()
		if cleanupErr := rollover.drain(); cleanupErr != nil {
			db.reportError(cleanupErr)
			return joinCOWCleanupError(err, cleanupErr)
		}
		// A complete captured prefix can contain native-only sources. Raw
		// table count alone does not describe its reclaimable coverage.
		if !errors.Is(err, memtable.ErrCOWCapacity) || attempt != 0 ||
			(len(c.cut.frozen) == 0 && c.cut.frozenLogicalSeq <= c.cut.basis.logicalSeq) {
			return err
		}
		// A full source frontier can be reclaimed before rotating the mutable
		// suffix. This physical root keeps its old AppliedLSN: the command cut also
		// covers mutable effects and is captured only after successful rotation.
		// IO and retirement run outside both writeMu and the COW writer owner.
		if err = db.checkpointFlushValueLogLanes(); err != nil {
			return err
		}
		if err = db.flushCOWFrozen(true, nil); err != nil {
			return err
		}
		if err = lockCheckpointWriteMutexContext(ctx, &db.writeMu); err != nil {
			return err
		}
	}
}

// Checkpoint already owns flushMu. Close post-frontier admission and drain any
// admitted writer before semantic-preserving index maintenance. Storage IO and
// retirement run outside writeMu; the existing cutover flags exclude new writes.
func (db *DB) vacuumCOWCheckpointBasis(automatic bool) error {
	db.checkpointMu.Lock()
	db.checkpointPostFrontierAdmission.Store(false)
	db.checkpointWriteCutoverActive.Store(true)
	db.checkpointMu.Unlock()
	db.writeMu.Lock()
	db.writeMu.Unlock()
	before := db.checkpointAutoVacuumRuns.Load()
	db.cow.refreshRequired.Store(true)
	err := db.maybeVacuumSparseIndexOnCheckpoint(automatic)
	if err == nil && db.checkpointAutoVacuumRuns.Load() == before {
		db.cow.refreshRequired.Store(false)
		return nil
	}
	// An error may follow accepted maintenance. Keep old roots and pins until
	// this explicit fenced refresh succeeds; pressure can be retried by the next
	// checkpoint and never silently rebases an accepted command.
	return errors.Join(err, db.refreshCOWBackendBasis())
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
	cleanupErr := p.drain()
	if cleanupErr != nil {
		db.reportError(cleanupErr)
	}
	return joinCOWCleanupError(err, cleanupErr)
}

func (db *DB) flushCOWBackground(syncFlush bool) bool {
	if db.cow.readClosed.Load() {
		return false
	}
	if err := db.flushCOWFrozen(syncFlush, nil); err != nil {
		if !errors.Is(err, backenddb.ErrSnapshotCapacity) && !errors.Is(err, memtable.ErrCOWCapacity) {
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
func (db *DB) closeCOWFrontier() (closeErr error) {
	c := db.cow
	var frontier *cowReadCut
	defer func() {
		closeErr = joinCOWCleanupError(closeErr, c.releaseCut(frontier))
		closeErr = joinCOWCleanupError(closeErr, c.closeWithBudget(false))
	}()
	// Like checkpoint, finish the original accepted physical handoff before
	// capturing another cut. Completion may retain its original failure; it
	// must run outside writeMu while closing and flushMu exclude new writers.
	db.writeMu.Unlock()
	if err := db.completeCOWHandoff(); err != nil {
		return err
	}
	db.writeMu.Lock()
	err := db.rotateCOWCheckpointFrontier(context.Background(), false, &frontier)
	if err != nil {
		return err
	}
	if err = db.checkpointFlushValueLogLanes(); err != nil {
		return err
	}
	var publish checkpointCommandWALPublish
	err = db.flushCOWFrozenCut(true, &publish, frontier)
	frontier = nil
	if err != nil {
		return err
	}
	if err = db.backendCheckpointBoundary(false); err != nil {
		return err
	}
	if publish.appliedLSN != 0 || publish.consumed {
		return db.cleanupCommandWALCheckpointBoundary(false, true)
	}
	return nil
}
