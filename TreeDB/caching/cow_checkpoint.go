package caching

import (
	"context"
	"errors"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

func (db *DB) checkpointCOWColumnCompaction(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction) error {
	return db.checkpointCOWColumnCompactionProjected(ctx, automatic, operation, nil)
}

func (db *DB) checkpointCOWColumnCompactionProjected(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, projected *backenddb.NativeProjectedGenerationPressure) error {
	defer db.observeCOWCheckpointRun(time.Now())
	return db.materializeCOWPrefixProjected(ctx, automatic, operation, cowCheckpointMaterialization, projected)
}

func (db *DB) observeCOWCheckpointRun(start time.Time) {
	dur := uint64(time.Since(start))
	db.checkpointRuns.Add(1)
	db.checkpointTotalNs.Add(dur)
	updateAtomicMaxUint64(&db.checkpointMaxNs, dur)
}

// reserveRawTxnPairAfterPressure is only for ordinary durable paired denial.
// It never duplicates flush ownership, holds half a pair, or waits with a
// foreground publication lock. The original retry remains outside flushMu.
func (db *DB) reserveRawTxnPairAfterPressure(denied rootpublication.AcceptedOrdinaryPairRefusal) (rootpublication.AcceptedTicket, rootpublication.AcceptedTicket, error) {
	ticket, spare, err := db.joinRawTxnPressureCheckpoint(denied)
	if err != nil || ticket.Valid() {
		return ticket, spare, err
	}
	ticket, spare, err = db.cow.prefix.ReserveAcceptedOrdinaryPair()
	return ticket, spare, acceptedAdmissionError(err)
}

func (db *DB) joinRawTxnPressureCheckpoint(denied rootpublication.AcceptedOrdinaryPairRefusal) (rootpublication.AcceptedTicket, rootpublication.AcceptedTicket, error) {
	if db.closing.Load() {
		return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, errDBClosing
	}
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeCOWAcceptedPairPressureWait, durabilitycut.ResourceAuxiliary, db.dir); err != nil {
		return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, err
	}
	start := time.Now()
	checkpoint := false
	defer func() {
		if checkpoint {
			db.observeCOWCheckpointRun(start)
		}
	}()
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	if db.closing.Load() {
		return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, errDBClosing
	}
	if !db.maintenanceActive.Load() &&
		db.cow.handoff == nil && !db.cow.refreshRequired.Load() && db.nativePressureDrainEnabled() {
		if err := backendPublicationReady(db.backend); err != nil {
			return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, err
		}
		ticket, spare, err := db.cow.prefix.ReserveAcceptedOrdinaryPairAfterRetirement(denied)
		if err != nil || ticket.Valid() {
			return ticket, spare, err
		}
	}
	checkpoint = true
	err := db.materializeCOWPrefixProjectedLocked(context.Background(), false, nil, cowCheckpointMaterialization, nil)
	return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, err
}

type cowPrefixMaterialization uint8

const (
	cowCheckpointMaterialization cowPrefixMaterialization = iota
	cowSynchronousPressureMaterialization
	cowGenerationPressureMaterialization
	cowReservationPressureMaterialization
)

// cowGenerationRetryableRefusal is added only where the original attempt can
// still proceed on the same frozen prefix. A later cleanup failure must
// survive alongside the refusal and must not be treated as a retry.
type cowGenerationRetryableRefusal struct{ error }

func (e cowGenerationRetryableRefusal) Unwrap() error { return e.error }

type cowGenerationSnapshotAllocationRefusal struct {
	error
	through                              rootpublication.SealedPrefix
	basisRefresh                         bool
	maintenanceBytes, maintenanceRefunds uint64
}

func (e *cowGenerationSnapshotAllocationRefusal) Unwrap() error { return e.error }

func (c *cowCache) generationFrozenThrough() rootpublication.SealedPrefix {
	c.cutMu.Lock()
	defer c.cutMu.Unlock()
	if cut := c.cut; cut != nil {
		return rootpublication.SealedPrefix{LogicalSequence: cut.frozenLogicalSeq, CanonicalLSN: cut.frozenCommandLSN}
	}
	return rootpublication.SealedPrefix{}
}

func cowGenerationRetryableOnly(err error) bool {
	for err != nil {
		switch e := err.(type) {
		case cowGenerationRetryableRefusal:
			return true
		case interface{ Unwrap() []error }:
			children := e.Unwrap()
			if len(children) != 1 {
				return false
			}
			err = children[0]
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		default:
			return false
		}
	}
	return false
}

func cowGenerationAdmissionError(generationPressure bool, err error) error {
	if !generationPressure {
		return err
	}
	for cause := err; cause != nil; {
		if cause == memtable.ErrCOWCapacity || cause == backenddb.ErrSnapshotCapacity {
			return cowGenerationRetryableRefusal{err}
		}
		// Backend build-group admission joins its primary result with cleanup.
		// Accept only the admission sentinel on its own.
		joined, ok := cause.(interface{ Unwrap() []error })
		if !ok {
			break
		}
		children := joined.Unwrap()
		if len(children) != 1 {
			break
		}
		cause = children[0]
	}
	return err
}

// materializeCOWPrefix keeps the original captured raw+semantic cut, build group
// and handoff. Pressure-only completion joins its exact durable seal and accepted
// retirement, leaving journal rotation and covered-file maintenance to the already
// running size-maintenance worker. Public checkpoint/Close/Drain are unchanged.
func (db *DB) materializeCOWPrefix(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, mode cowPrefixMaterialization) error {
	return db.materializeCOWPrefixProjected(ctx, automatic, operation, mode, nil)
}

func (db *DB) materializeCOWPrefixProjected(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, mode cowPrefixMaterialization, projected *backenddb.NativeProjectedGenerationPressure) error {
	if db.closing.Load() {
		return errDBClosing
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	waitedForFlushMu := false
	if !db.flushMu.TryLock() {
		waitedForFlushMu = true
		if err := lockCheckpointMutexContext(ctx, &db.flushMu); err != nil {
			return err
		}
	}
	defer db.flushMu.Unlock()
	if mode == cowReservationPressureMaterialization {
		if db.closing.Load() {
			return errDBClosing
		}
		if db.cow.prefix != nil {
			f := db.cow.prefix.LogicalFrontiers()
			if f.V <= f.S.LogicalSequence && f.C <= f.S.CanonicalLSN {
				return nil
			}
		}
		observed, obsErr := db.cow.nativeProducerPressureState()
		if obsErr != nil {
			return obsErr
		}
		limits := db.cow.budget.Limits()
		if !observed.reservationSoftPressure(limits) {
			if db.cow.reservationDrainFloor.Load() != 0 {
				db.cow.reservationDrainFloor.Store(0)
			}
			return nil
		}
		if floor := db.cow.reservationDrainFloor.Load(); floor != 0 && observed.stats.ReservedBytes < floor {
			return nil
		}
	} else if waitedForFlushMu && mode == cowSynchronousPressureMaterialization && projected == nil {
		if db.closing.Load() {
			return errDBClosing
		}
		observed, obsErr := db.cow.nativeProducerPressureState()
		if obsErr == nil && !observed.underPressure(db.cow.budget.Limits()) {
			return nil
		}
	}
	return db.materializeCOWPrefixProjectedLocked(ctx, automatic, operation, mode, projected)
}

// materializeCOWPrefixProjectedLocked is the original durable materialization;
// its caller owns flushMu through the final unwind and cleanup.
func (db *DB) materializeCOWPrefixProjectedLocked(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, mode cowPrefixMaterialization, projected *backenddb.NativeProjectedGenerationPressure) (retErr error) {
	pressureOnly := mode != cowCheckpointMaterialization
	generationPressure := mode == cowGenerationPressureMaterialization
	if generationPressure {
		if db.closing.Load() {
			return errDBClosing
		}
		defer func() {
			var refusal *cowGenerationSnapshotAllocationRefusal
			if cowGenerationRetryableOnly(retErr) && errors.As(retErr, &refusal) {
				if refusal.basisRefresh {
					refusal.through = db.cow.generationFrozenThrough()
				}
				// Seal the observation after the attempt's full unwind but
				// before another physical basis/owner can use the flush fence.
				refusal.maintenanceBytes, refusal.maintenanceRefunds = db.cow.maintenance.MaintenanceUsage()
			}
		}()
	}
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
	if err := db.completeCOWHandoffForGeneration(generationPressure); err != nil {
		return err
	}
	if db.cow.refreshRequired.Load() {
		// A writer may have entered before the reconciliation debt or this
		// checkpoint's admission fence. Drain it before sizing the successor
		// to the exact frozen sources. Keep writeMu free during storage work.
		if err := lockCheckpointWriteMutexContext(ctx, &db.writeMu); err != nil {
			return err
		}
		db.writeMu.Unlock()
		if err := db.refreshCOWBackendBasisForGeneration(generationPressure); err != nil {
			return err
		}
	}
	if err := lockCheckpointWriteMutexContext(ctx, &db.writeMu); err != nil {
		return err
	}
	if projected != nil {
		// A physical worker can advance the active generation while this
		// producer waits on flush ownership. The exclusive write fence now
		// ties the scope check to the ensuing frontier rotation.
		db.cow.writerMu.Lock()
		needed := nativeProjectedGenerationNeedsRollover(db.cow.cut, *projected)
		db.cow.writerMu.Unlock()
		if !needed {
			db.writeMu.Unlock()
			return nil
		}
	}
	cutover := time.Now()
	if hook := db.testBeforeCheckpointFrontierCapture; hook != nil {
		hook()
	}
	var frontier *cowReadCut
	var err error
	if generationPressure {
		// Producer admission already froze and marked this prefix. Retain
		// that root set and use its frozen endpoint; concurrent writes in
		// the mutable successor belong to a later publication.
		frontier = db.cow.retainCut()
		if frontier == nil {
			err = backenddb.ErrClosed
		} else if db.snapshotCommandWALCheckpointCutover() {
			db.checkpointPostFrontierAdmission.Store(true)
		}
		db.writeMu.Unlock()
	} else {
		// Explicit Checkpoint on relaxed command-WAL defers active WAL segment
		// rotation until after index.db + leaf_vlog durably cover the frontier,
		// avoiding a synchronous fsync of a segment about to be unlinked.
		rotateJournal := !pressureOnly && (!db.relaxedSync || automatic || durabilitycut.Enabled() || db.testBeforeCheckpointFrontierDrain != nil || db.testAfterCheckpointFrontierCapture != nil)
		err = db.rotateCOWFrontier(ctx, true, &frontier, rotateJournal)
	}
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
	// Preserve the original captured endpoint before the flush consumes frontier.
	through := rootpublication.SealedPrefix{LogicalSequence: frontier.frozenLogicalSeq, CanonicalLSN: frontier.frozenCommandLSN}
	// COW admits only the canonical command-WAL and NoWAL cached profiles. Their
	// value log is independent persistent storage, synced before root coverage.
	if err = db.checkpointFlushValueLogLanes(); err != nil {
		return err
	}
	var publish checkpointCommandWALPublish
	if hook := db.testBeforeCheckpointFrontierDrain; hook != nil {
		hook()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	err = db.flushCOWFrozenCutWithColumnCompaction(true, &publish, frontier, operation != nil, operation, generationPressure)
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
	if pressureOnly {
		// The receipt was copied before handoff teardown. A no-op cut has no new
		// receipt; its accepted endpoint is still joined below. Never substitute a
		// sampled backend root or maximum LSN for this publication authority.
		if publish.seal.Valid() {
			if err = publish.seal.Wait(ctx); err != nil {
				return err
			}
		}
		return db.cow.prefix.WaitAccepted(ctx, through)
	}
	cleanupNeeded := publish.appliedLSN != 0 || publish.consumed
	cleanupHook := db.loadCommandWALCheckpointCleanupHook()
	checkpoint, canDeferCleanup := db.backend.(interface {
		backendDeferredCoveredPrefixMaintainer
		CheckpointWithoutCommandWALCleanup() error
	})
	cleanupAfterAccept := canDeferCleanup && !automatic && operation == nil && db.externalCommandWAL && cleanupNeeded && cleanupHook != nil
	if cleanupAfterAccept {
		err = checkpoint.CheckpointWithoutCommandWALCleanup()
	} else {
		err = db.backendCheckpointBoundary(automatic)
	}
	if err != nil {
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
			if cleanupAfterAccept {
				db.requestCommandWALCleanup()
			}
			return err
		}
	}
	if cleanupAfterAccept {
		start := time.Now()
		err = runCommandWALCheckpointCleanupHook(cleanupHook, true)
		recordCheckpointStageSince(&db.checkpointStageCommandWALCleanup, start)
		if err != nil {
			return err
		}
	} else if cleanupNeeded {
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
	return db.rotateCOWFrontier(ctx, postFrontier, captured, true)
}

// rotateJournal is false only for producer pressure with an existing size worker.
// Rollover, finite source-capacity retry and original cutover capture stay shared.
func (db *DB) rotateCOWFrontier(ctx context.Context, postFrontier bool, captured **cowReadCut, rotateJournal bool) error {
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
		if err == nil && db.externalCommandWAL && rotateJournal {
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
	_, err := db.rolloverCOWForFlushProjected(nil)
	return err
}

func (db *DB) rolloverCOWForFlushProjected(projected *backenddb.NativeProjectedGenerationPressure) (bool, error) {
	if err := db.beginDirectWrite(); err != nil {
		return false, err
	}
	c := db.cow
	c.writerMu.Lock()
	if projected != nil && !nativeProjectedGenerationNeedsRollover(c.cut, *projected) {
		c.writerMu.Unlock()
		db.writeMu.RUnlock()
		return false, nil
	}
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
	return p != nil && p.installed, joinCOWCleanupError(err, cleanupErr)
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
