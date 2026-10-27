package caching

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// A slice does not own cache write admission. flushMu prevents a checkpoint,
// backend handoff, or Close from replacing/discarding its exact physical basis.
// Normal private COW writers continue; writers requiring a flush and explicit
// checkpoints can wait for this cooperative timeout, including native I/O.
const (
	privateCOWLeafPackCopyBytes   = 32 << 20
	privateCOWLeafPackGenerations = 2
	privateCOWLeafPackInterval    = 30 * time.Second
	privateCOWLeafPackTimeout     = 5 * time.Second
)

type backendPrivateCOWLeafPackRunner interface {
	LeafGenerationPackRunOnceForPrivateCOW(*transactionowner.Owner, context.Context, backenddb.LeafGenerationPackFromPlanOptions) (backenddb.LeafGenerationPackRunOnceStats, error)
}

// Initial WAL-on scheduler state is not an opt-out. Explicit disables, unlike
// that initial state, remain authoritative. ExternalCommandWAL disables only
// the redundant cache journal, not durability. True WAL-off/non-COW is unchanged.
func (db *DB) privateCOWLeafPackEnabled() bool {
	if db == nil || db.cow == nil || db.cow.txnOwner == nil || (db.disableJournal && !db.externalCommandWAL) || !db.indexOuterLeavesInValueLog ||
		db.valueLogGenerationPolicy != uint8(backenddb.ValueLogGenerationHotWarmCold) || envBool(envDisableVlogGenerationLoop) {
		return false
	}
	if _, set := os.LookupEnv(envEnableLeafGenerationPackMaintenance); set && !envBool(envEnableLeafGenerationPackMaintenance) {
		return false
	}
	_, ok := db.backend.(backendPrivateCOWLeafPackRunner)
	return ok
}

// Creating a writable leaf file can advance only the physical manifest view
// before its references enter a backend root. Compare every logical operand;
// the captured snapshot itself retains its exact older file/view capabilities.
func samePrivateCOWLeafLogicalBasis(a, b backenddb.StateToken) bool {
	return a.CommitSeq == b.CommitSeq && a.RootPageID == b.RootPageID &&
		a.SystemRootPageID == b.SystemRootPageID && a.AppliedCommandLSN == b.AppliedCommandLSN && a.MaxEntryRevision == b.MaxEntryRevision
}

// runPrivateCOWLeafPackSlice performs one planner/selection/pack pass and at most
// one ensuing GC. It never drains a plan to completion, waits for quiet traffic,
// or retries until useful work appears. Both no-progress and failures back off.
func (db *DB) runPrivateCOWLeafPackSlice() (attempted bool, retErr error) {
	if !db.privateCOWLeafPackEnabled() || db.closing.Load() || db.suppressBackgroundVlogGenerationForMaintenancePhase() ||
		db.coalesceDeferredVectorBuildVlogMaintenance() || db.maintenanceActive.Load() {
		return false, nil
	}
	if paused, _ := vlogGenerationMaintenancePausedByFile(); paused {
		db.vlogGenerationMaintenanceSkipPauseFile.Add(1)
		return false, nil
	}
	if last := db.vlogGenerationPrivateCOWPackLastAttemptNS.Load(); last != 0 && time.Since(time.Unix(0, last)) < privateCOWLeafPackInterval {
		return false, nil
	}
	if !db.vlogGenerationMaintenanceActive.CompareAndSwap(false, true) {
		return false, nil
	}
	defer db.vlogGenerationMaintenanceActive.Store(false)
	// Do not queue this background slice behind a pending foreground owner.
	if !db.flushMu.TryLock() {
		return false, nil
	}
	defer db.flushMu.Unlock()
	if db.closing.Load() || db.maintenanceActive.Load() || db.suppressBackgroundVlogGenerationForMaintenancePhase() {
		return false, nil
	}
	// A failed accepted handoff must not be retried at every scheduler tick.
	db.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().UnixNano())
	if err := db.completeCOWHandoff(); err != nil {
		return true, err
	}
	if db.cow.refreshRequired.Load() {
		return false, backenddb.ErrRecoveryRequired
	}
	reader, ok := db.backend.(backendStateTokenReader)
	if !ok {
		return false, ErrCOWUnsupported
	}
	cut := db.cow.retainCut()
	if cut == nil {
		return false, backenddb.ErrClosed
	}
	original, live := cut.basis.snapshot.StateToken()
	through := rootpublication.SealedPrefix{LogicalSequence: cut.basis.logicalSeq, CanonicalLSN: original.AppliedCommandLSN}
	releaseErr := db.cow.releaseCut(cut)
	if releaseErr != nil {
		return false, releaseErr
	}
	actual, available := reader.StateToken()
	if !live || !available || !samePrivateCOWLeafLogicalBasis(original, actual) {
		err := backenddb.ErrRootPublicationBasisMismatch
		db.cow.refreshRequired.Store(true)
		db.poisonNativeHandoff(err)
		return false, err
	}
	db.vlogGenerationPrivateCOWPackLastAttemptNS.Store(time.Now().UnixNano())
	db.vlogGenerationLeafPackAttempts.Add(1)
	db.vlogGenerationLeafPackAdmitted.Add(1)
	db.vlogGenerationLeafPackGCLastEligibleGenerations.Store(0)
	db.vlogGenerationLeafPackGCLastDeletedGenerations.Store(0)
	db.vlogGenerationLeafPackGCLastDeletedFiles.Store(0)
	db.vlogGenerationLeafPackGCLastDeletedBytes.Store(0)
	db.vlogGenerationLeafPackLastReclaimedBytes.Store(0) // no inferred physical reclaim credit
	db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerRunning)
	db.vlogGenerationLastReason.Store(vlogGenerationReasonLeafPack)
	started := time.Now()
	defer func() {
		db.vlogGenerationLeafPackLastWallNanos.Store(uint64(time.Since(started)))
		db.vlogGenerationSchedulerState.Store(vlogGenerationSchedulerIdle)
		if retErr != nil {
			db.vlogGenerationLeafPackErrors.Add(1)
			db.storeVlogGenerationLeafPackLastSkipReason(retErr.Error())
		}
	}()
	ctx, cancel := db.leafGenerationPackMaintenanceContext(privateCOWLeafPackTimeout)
	defer cancel()
	roots, systemRoots := db.publishedLeafGenerationProtectionIDs()
	stats, packErr := db.backend.(backendPrivateCOWLeafPackRunner).LeafGenerationPackRunOnceForPrivateCOW(db.cow.txnOwner, ctx, backenddb.LeafGenerationPackFromPlanOptions{
		Sync: true, MinPublishedAgeCommits: 1, MinCandidateGenerations: 1,
		MinReclaimPerByteCopiedPPM: leafGenerationPackMaintenanceDefaultMinReclaimPerByteCopiedPPM,
		MaxGenerations:             privateCOWLeafPackGenerations, MaxBytesToCopy: privateCOWLeafPackCopyBytes,
		ReserveRIDs: db.ReserveValueLogRIDs, ProtectedRootIDs: roots, ProtectedSystemRootIDs: systemRoots,
	})
	db.vlogGenerationLeafPackLastSelectionGenerations.Store(int64(len(stats.Selection.GenerationIDs)))
	db.vlogGenerationLeafPackLastSelectionBytesToCopy.Store(stats.Selection.BytesToCopy)
	db.vlogGenerationLeafPackLastSelectionBytesDead.Store(stats.Selection.BytesDead)
	db.vlogGenerationLeafPackLastExpectedReclaimPerByteCopiedPPM.Store(int64(stats.Selection.ExpectedReclaimPerByteCopiedPPM))
	if stats.Selection.BytesToCopy > 0 {
		db.vlogGenerationLeafPackSelectionBytesToCopy.Add(uint64(stats.Selection.BytesToCopy))
	}
	if stats.Selection.BytesDead > 0 {
		db.vlogGenerationLeafPackSelectionBytesDead.Add(uint64(stats.Selection.BytesDead))
	}
	if len(stats.Selection.GenerationIDs) > 0 {
		db.vlogGenerationLeafPackSelectionGenerations.Add(uint64(len(stats.Selection.GenerationIDs)))
	}
	if stats.Selection.ExpectedReclaimPerByteCopiedPPM > 0 {
		db.vlogGenerationLeafPackExpectedReclaimPerByteCopiedPPM.Add(uint64(stats.Selection.ExpectedReclaimPerByteCopiedPPM))
	}
	db.vlogGenerationLeafPackLastExpectedReclaimBytes.Store(stats.Selection.ExpectedReclaimBytes)
	if stats.Selection.ExpectedReclaimBytes > 0 {
		db.vlogGenerationLeafPackExpectedReclaimBytes.Add(uint64(stats.Selection.ExpectedReclaimBytes))
	}
	if stats.Pack.BytesCopied > 0 {
		db.vlogGenerationLeafPackBytesCopied.Add(uint64(stats.Pack.BytesCopied))
	}
	db.vlogGenerationLeafPackLastBytesCopied.Store(stats.Pack.BytesCopied)
	if stats.Ran {
		db.vlogGenerationLeafPackRuns.Add(1)
		db.vlogGenerationLeafPackLastUnixNano.Store(time.Now().UnixNano())
	}
	// A failed finalizer can already have accepted a replacement. Reconcile on
	// every error as well; a declined plan with no error made no publication.
	if stats.Ran || packErr != nil {
		if err := db.adoptPrivateCOWLeafBasis(original); err != nil {
			return true, errors.Join(packErr, err)
		}
	}
	if packErr != nil {
		packErr = db.privateCOWLeafMaintenanceError(packErr)
		if db.cow.refreshRequired.Load() {
			return true, packErr
		}
		if errors.Is(packErr, context.Canceled) || errors.Is(packErr, context.DeadlineExceeded) || errors.Is(packErr, backenddb.ErrLeafGenerationMaintenanceLimit) {
			db.vlogGenerationLeafPackSkips.Add(1)
			db.storeVlogGenerationLeafPackLastSkipReason(packErr.Error())
			return true, nil
		}
		return true, packErr
	}
	if !stats.Ran {
		db.vlogGenerationLeafPackSkips.Add(1)
		db.storeVlogGenerationLeafPackLastSkipReason(stats.SkipReason)
	} else {
		db.storeVlogGenerationLeafPackLastSkipReason("")
	}
	// A previous slice may have adopted an accepted pack and then timed out or
	// returned a finalization error before converging its recovery slot. Retry
	// that lawful work even if this plan is a no-op, using the existing backend
	// predicate and this slice's captured applied endpoint. No extra timeout,
	// pending owner or logical coverage is created. Mutable suffixes survive;
	// newer frozen suffixes defer to their own checkpoint as before.
	if err := db.refreshCOWAutomaticCheckpointFallback(ctx, through); err != nil {
		return true, db.privateCOWLeafMaintenanceError(err)
	}
	// Even a no-op pack plan reaches GC: a later checkpoint may have released
	// the fallback/pin that prevented an earlier retirement.
	// No forced reclaim: both recoverable roots and historical reader pins still
	// govern deletion. BytesDeleted is logical retirement, not proved unlink.
	if gc, ok := db.backend.(backendLeafGenerationGCRunner); ok && !envBool(envDisableVlogGenerationGC) && ctx.Err() == nil && !db.closing.Load() {
		opts := db.leafGenerationGCOptions()
		gcStats, gcErr := gc.LeafGenerationGC(ctx, opts)
		db.vlogGenerationLeafPackGCRuns.Add(1)
		if gcStats.GenerationsEligible > 0 {
			db.vlogGenerationLeafPackGCEligibleGenerations.Add(uint64(gcStats.GenerationsEligible))
		}
		db.vlogGenerationLeafPackGCLastEligibleGenerations.Store(int64(gcStats.GenerationsEligible))
		if gcStats.GenerationsDeleted > 0 {
			db.vlogGenerationLeafPackGCDeletedGenerations.Add(uint64(gcStats.GenerationsDeleted))
		}
		if gcStats.FilesDeleted > 0 {
			db.vlogGenerationLeafPackGCDeletedFiles.Add(uint64(gcStats.FilesDeleted))
		}
		if gcStats.BytesDeleted > 0 {
			db.vlogGenerationLeafPackGCDeletedBytes.Add(uint64(gcStats.BytesDeleted))
		}
		db.vlogGenerationLeafPackGCLastDeletedGenerations.Store(int64(gcStats.GenerationsDeleted))
		db.vlogGenerationLeafPackGCLastDeletedFiles.Store(int64(gcStats.FilesDeleted))
		db.vlogGenerationLeafPackGCLastDeletedBytes.Store(gcStats.BytesDeleted)
		// GC can publish a manifest view even when it returns a later error.
		if err := db.adoptPrivateCOWLeafBasis(original); err != nil {
			return true, errors.Join(gcErr, err)
		}
		gcErr = db.privateCOWLeafMaintenanceError(gcErr)
		if db.cow.refreshRequired.Load() {
			return true, gcErr
		}
		if errors.Is(gcErr, context.Canceled) || errors.Is(gcErr, context.DeadlineExceeded) || errors.Is(gcErr, backenddb.ErrLeafGenerationMaintenanceLimit) {
			return true, nil
		}
		return true, gcErr
	}
	return true, nil
}

// Caller holds flushMu, not maintenanceActive or writerMu. The actual snapshot
// and its admitted resources come from the original backend. Nil coverage is
// essential: this physical maintenance cannot consume any logical COW source.
func (db *DB) adoptPrivateCOWLeafBasis(original backenddb.StateToken) error {
	// A declined/failed pre-publication operation needs no new cut or credit.
	current, live := db.backend.(backendStateTokenReader).StateToken()
	cut := db.cow.retainCut()
	if cut == nil {
		return backenddb.ErrClosed
	}
	prior, priorLive := cut.basis.snapshot.StateToken()
	releaseErr := db.cow.releaseCut(cut)
	if releaseErr != nil {
		db.cow.refreshRequired.Store(true)
		db.poisonNativeHandoff(releaseErr)
		return releaseErr
	}
	if live && priorLive && current == prior {
		return nil
	}
	basis, err := db.cow.captureBackendBasis(db.backend.(backendSnapshotProvider))
	if err == nil {
		next, ok := basis.snapshot.StateToken()
		if !ok || next.AppliedCommandLSN != original.AppliedCommandLSN || next.MaxEntryRevision != original.MaxEntryRevision {
			err = fmt.Errorf("private COW leaf maintenance changed logical frontier: %w", backenddb.ErrRootPublicationBasisMismatch)
		}
		if err == nil {
			var old *cowReadCut
			old, err = db.installCOWBasis(basis, nil)
			if old != nil {
				err = joinCOWCleanupError(err, old.drain())
			}
		}
		err = joinCOWCleanupError(err, basis.release())
	}
	if err == nil && db.valueLogReader != nil {
		err = db.valueLogReader.Refresh()
	}
	if err != nil {
		db.cow.refreshRequired.Store(true)
		db.poisonNativeHandoff(err)
		return backendMaintenanceError("private COW leaf basis adoption", err)
	}
	db.cow.refreshRequired.Store(false)
	return nil
}

// A late backend publication error may already have poisoned its original
// owner. Do not clear that uncertainty merely because a snapshot could be read.
func (db *DB) privateCOWLeafMaintenanceError(err error) error {
	if err == nil {
		return nil
	}
	if ready, ok := db.backend.(interface{ CheckStorageMaintenanceReady() error }); ok {
		if cause := ready.CheckStorageMaintenanceReady(); cause != nil {
			db.cow.refreshRequired.Store(true)
			db.poisonNativeHandoff(cause)
			return errors.Join(err, cause)
		}
	}
	return err
}
