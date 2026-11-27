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

func (db *DB) checkpointCOWContextDeferringCommandWALCleanup(ctx context.Context) error {
	defer db.observeCOWCheckpointRun(time.Now())
	return db.materializeCOWPrefixProjected(ctx, false, nil, cowCheckpointDeferCommandWALCleanup, nil, true)
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
	return db.materializeCOWPrefixProjected(ctx, automatic, operation, cowCheckpointMaterialization, projected, !automatic && operation == nil)
}

// checkpointCOWPressure runs a full durable COW checkpoint boundary for
// internal foreground pressure relief or exact-seal publication without
// triggering synchronous leaf-generation pack+GC maintenance.
func (db *DB) checkpointCOWPressure(ctx context.Context, projected *backenddb.NativeProjectedGenerationPressure) error {
	defer db.observeCOWCheckpointRun(time.Now())
	return db.materializeCOWPrefixProjected(ctx, false, nil, cowCheckpointMaterialization, projected, false)
}

func (db *DB) observeCOWCheckpointRun(start time.Time) {
	dur := uint64(time.Since(start))
	db.checkpointRuns.Add(1)
	db.checkpointTotalNs.Add(dur)
	updateAtomicMaxUint64(&db.checkpointMaxNs, dur)
}

// cowAcceptedPressureShouldRetry decides whether a capacity refusal after a
// pressure checkpoint pass represents forward progress worth retrying. It
// requires that reclaimable pending entries existed atomically at refusal time
// (hadPending) and that either S strictly advanced or an already-sealed
// frontier (before.(V,C) <= before.S) accumulated newly installed entries
// (after.(V,C) > after.S) while the checkpoint ran.
func cowAcceptedPressureShouldRetry(before, after rootpublication.LogicalFrontiers, hadPending bool) bool {
	if !hadPending {
		return false
	}
	return after.S.LogicalSequence > before.S.LogicalSequence ||
		after.S.CanonicalLSN > before.S.CanonicalLSN ||
		((before.V <= before.S.LogicalSequence && before.C <= before.S.CanonicalLSN) &&
			(after.V > after.S.LogicalSequence || after.C > after.S.CanonicalLSN))
}

// reserveRawTxnPairAfterPressure is only for ordinary durable paired denial.
// It never duplicates flush ownership, holds half a pair, or waits with a
// foreground publication lock. The original retry remains outside flushMu.
// Under high concurrency, other writers can refill the accepted-prefix ring
// between retirement inside the checkpoint and the caller's retry; keep
// retrying while forward progress occurs (S advances, or an empty frontier
// accumulates new unsealed entries) and pending entries existed at denial time.
func (db *DB) reserveRawTxnPairAfterPressure(denied rootpublication.AcceptedOrdinaryPairRefusal) (rootpublication.AcceptedTicket, rootpublication.AcceptedTicket, error) {
	for {
		before := db.cow.prefix.LogicalFrontiers()
		ticket, spare, err := db.joinRawTxnPressureCheckpoint(denied)
		if err != nil || ticket.Valid() {
			return ticket, spare, err
		}
		var nextDenied rootpublication.AcceptedOrdinaryPairRefusal
		ticket, spare, nextDenied, err = db.cow.prefix.ReserveAcceptedOrdinaryPairWithRefusal()
		if err == nil {
			return ticket, spare, nil
		}
		if !nextDenied.Valid() {
			return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, acceptedAdmissionError(err)
		}
		after := db.cow.prefix.LogicalFrontiers()
		if !cowAcceptedPressureShouldRetry(before, after, nextDenied.HasPending()) {
			return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, acceptedAdmissionError(err)
		}
		denied = nextDenied
	}
}

// reserveCOWAcceptedOrdinaryAfterPressure seals a full accepted-prefix ring
// using the reserved maintenance lane and retries ordinary single-slot
// admission. Queued waiters entering flushMu after another writer already
// retired the refused prefix reuse that retirement instead of running
// back-to-back checkpoints. If concurrent writers refill the ring after forward
// progress and pending entries existed at denial time, keep retrying.
func (db *DB) reserveCOWAcceptedOrdinaryAfterPressure(c *cowCache, denied rootpublication.AcceptedOrdinaryRefusal, cause error) (rootpublication.AcceptedTicket, error) {
	if !denied.Valid() {
		return rootpublication.AcceptedTicket{}, acceptedAdmissionError(cause)
	}
	for {
		before := c.prefix.LogicalFrontiers()
		ticket, err := db.joinCOWAcceptedOrdinaryPressureCheckpoint(denied)
		if err != nil || ticket.Valid() {
			return ticket, err
		}
		var nextDenied rootpublication.AcceptedOrdinaryRefusal
		ticket, nextDenied, err = c.prefix.ReserveAcceptedOrdinaryWithRefusal()
		if err == nil {
			return ticket, nil
		}
		if !nextDenied.Valid() {
			return rootpublication.AcceptedTicket{}, acceptedAdmissionError(err)
		}
		after := c.prefix.LogicalFrontiers()
		if !cowAcceptedPressureShouldRetry(before, after, nextDenied.HasPending()) {
			return rootpublication.AcceptedTicket{}, acceptedAdmissionError(err)
		}
		denied = nextDenied
	}
}

func (db *DB) joinCOWAcceptedOrdinaryPressureCheckpoint(denied rootpublication.AcceptedOrdinaryRefusal) (rootpublication.AcceptedTicket, error) {
	if db.closing.Load() {
		return rootpublication.AcceptedTicket{}, errDBClosing
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
		return rootpublication.AcceptedTicket{}, errDBClosing
	}
	if !db.maintenanceActive.Load() &&
		db.cow.handoff == nil && !db.cow.refreshRequired.Load() && db.nativePressureDrainEnabled() {
		if err := backendPublicationReady(db.backend); err != nil {
			return rootpublication.AcceptedTicket{}, err
		}
		requireStableWAL := db.externalCommandWAL && !db.relaxedSync
		ticket, err := db.cow.prefix.ReserveAcceptedOrdinaryAfterRetirement(denied, requireStableWAL)
		if err != nil || ticket.Valid() {
			return ticket, err
		}
	}
	checkpoint = true
	err := db.materializeCOWPrefixProjectedLocked(context.Background(), false, nil, cowCheckpointMaterialization, nil, false)
	return rootpublication.AcceptedTicket{}, err
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
	err := db.materializeCOWPrefixProjectedLocked(context.Background(), false, nil, cowCheckpointMaterialization, nil, false)
	return rootpublication.AcceptedTicket{}, rootpublication.AcceptedTicket{}, err
}

type cowPrefixMaterialization uint8

const (
	cowCheckpointMaterialization cowPrefixMaterialization = iota
	cowCheckpointDeferCommandWALCleanup
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
	return db.materializeCOWPrefixProjected(ctx, automatic, operation, mode, nil, false)
}

func (db *DB) materializeCOWPrefixProjected(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, mode cowPrefixMaterialization, projected *backenddb.NativeProjectedGenerationPressure, reclaimLeafDebt bool) error {
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
	return db.materializeCOWPrefixProjectedLocked(ctx, automatic, operation, mode, projected, reclaimLeafDebt)
}

// materializeCOWPrefixProjectedLocked is the original durable materialization;
// its caller owns flushMu through the final unwind and cleanup.
func (db *DB) materializeCOWPrefixProjectedLocked(ctx context.Context, automatic bool, operation backenddb.NativeColumnStoreCompaction, mode cowPrefixMaterialization, projected *backenddb.NativeProjectedGenerationPressure, reclaimLeafDebt bool) (retErr error) {
	deferCommandWALCleanup := mode == cowCheckpointDeferCommandWALCleanup
	pressureOnly := mode != cowCheckpointMaterialization && !deferCommandWALCleanup
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
		db.maybeKickDeferredValueLogDictPublish()
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
	deferRelaxedWALSync := false
	if generationPressure {
		// Producer admission already froze and marked this prefix. Retain
		// that root set and use its frozen endpoint; concurrent writes in
		// the mutable successor belong to a later publication.
		frontier = db.cow.retainCut()
		if frontier == nil {
			err = backenddb.ErrClosed
		} else if db.snapshotCommandWALCheckpointCutover() || (!db.externalCommandWAL && db.nativePressureDrainEnabled()) {
			db.checkpointPostFrontierAdmission.Store(true)
		}
		db.writeMu.Unlock()
	} else {
		// Explicit Checkpoint on relaxed command-WAL defers active WAL segment
		// rotation and WAL fsync until after index.db + leaf_vlog durably cover
		// the frontier, avoiding a synchronous fsync of a segment about to be
		// unlinked.
		rotateJournal := !pressureOnly && (!db.relaxedSync || automatic || durabilitycut.Enabled() || db.testBeforeCheckpointFrontierDrain != nil || db.testAfterCheckpointFrontierCapture != nil)
		deferRelaxedWALSync = !pressureOnly && !rotateJournal
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
	flushedDirty := frontier != nil && len(frontier.frozen) > 0
	err = db.flushCOWFrozenCutWithColumnCompaction(true, &publish, frontier, operation != nil, operation, generationPressure, !deferRelaxedWALSync)
	frontier = nil
	if err != nil {
		return err
	}
	flushedDirty = flushedDirty || publish.consumed || publish.appliedLSN != 0
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
	deferCleanupToCaller := canDeferCleanup && deferCommandWALCleanup && !automatic && operation == nil && db.externalCommandWAL
	cleanupAfterAccept := !deferCleanupToCaller && canDeferCleanup && !automatic && operation == nil && db.externalCommandWAL && cleanupNeeded && cleanupHook != nil
	if deferCleanupToCaller || cleanupAfterAccept {
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
			if cleanupAfterAccept || deferCleanupToCaller {
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
	} else if cleanupNeeded && !deferCleanupToCaller {
		if err = db.cleanupCommandWALCheckpointBoundary(automatic, true); err != nil {
			return err
		}
	}
	if reclaimLeafDebt && !automatic && operation == nil && !durabilitycut.Enabled() &&
		db.testBeforeCheckpointFrontierDrain == nil && db.testAfterCheckpointFrontierCapture == nil {
		if err = db.maybeRunPrivateCOWLeafPackAtCheckpointLocked(ctx, flushedDirty); err != nil {
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
		if err == nil && postFrontier && ((db.snapshotCommandWALCheckpointCutover() && db.externalCommandWAL) || (!db.externalCommandWAL && db.nativePressureDrainEnabled())) {
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
