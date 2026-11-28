package caching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"runtime"
	"strconv"
)

// backendMaintenanceError adds no allocations on successful calls while
// preserving errors.Is/As through a failure's concrete ownership phase.
func backendMaintenanceError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backend maintenance %s: %w", stage, err)
}

// RunBackendMaintenance fences cached writers around an existing backend
// operation. Its caller performs the required checkpoint first. COW then
// refreshes the equivalent backend basis before source writes resume.
func (db *DB) RunBackendMaintenance(fn func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true}, fn)
}

// RunBackendMaintenanceWithPostRefresh completes caller-owned retirement after
// the old COW basis drains, under the SAME maintenance writer fence. A failed
// operation or basis refresh never grants permission to run the settle callback.
func (db *DB) RunBackendMaintenanceWithPostRefresh(fn, settle func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true, afterCOWRefresh: settle}, fn)
}

// BeginSystemRootMutation acquires the COW maintenance fence around a backend
// SystemRootPageID-only mutation (such as dictionary publication) so the COW
// backend basis stays synchronized. When invoked re-entrantly from the active
// maintenance goroutine (for example during LeafGenerationPack or
// ValueLogRewriteOnline), it returns (nil, nil) because the caller already
// holds the maintenance fence and will refresh the COW basis on completion.
func (db *DB) BeginSystemRootMutation() (func(error) error, error) {
	if db == nil || db.cow == nil {
		return nil, nil
	}
	gid := currentMaintenanceGoroutineID()
	if gid != 0 && db.maintenanceOwnerGID.Load() == gid {
		return nil, nil
	}
	db.checkpointMu.Lock()
	for db.checkpointing.Load() || db.maintenanceActive.Load() {
		db.checkpointCond.Wait()
	}
	db.maintenanceActive.Store(true)
	db.maintenanceOwnerGID.Store(gid)
	db.checkpointMu.Unlock()
	releaseMaintenance := func() {
		db.checkpointMu.Lock()
		db.maintenanceOwnerGID.Store(0)
		db.maintenanceActive.Store(false)
		db.checkpointCond.Broadcast()
		db.checkpointMu.Unlock()
	}

	db.flushMu.Lock()
	db.writeMu.Lock()
	db.writeMu.Unlock()
	if err := db.completeCOWHandoff(); err != nil {
		db.flushMu.Unlock()
		releaseMaintenance()
		return nil, backendMaintenanceError("complete COW handoff", err)
	}
	db.cow.refreshRequired.Store(true)
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			db.flushMu.Unlock()
			releaseMaintenance()
			return nil, backendMaintenanceError("refresh backend value-log set before callback", err)
		}
	}
	return func(fnErr error) error {
		defer func() {
			db.flushMu.Unlock()
			releaseMaintenance()
		}()
		if db.closing.Load() {
			return fnErr
		}
		reconcileErr := backendMaintenanceError("reconcile cached value-log readers and writers", db.reconcileSplitValueLogWritersAfterBackendMaintenanceAdmitted(false))
		return db.finishCOWBackendMaintenance(errors.Join(fnErr, reconcileErr))
	}, nil
}

func currentMaintenanceGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, err := strconv.ParseUint(string(fields[1]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func (db *DB) refreshCOWBackendBasis() error {
	return db.refreshCOWBackendBasisForGeneration(false)
}

func (db *DB) refreshCOWBackendBasisForGeneration(generationPressure bool) error {
	basis, err := db.cow.captureBackendBasis(db.backend.(backendSnapshotProvider))
	if err != nil {
		return backendMaintenanceError("capture COW basis", cowGenerationAdmissionError(generationPressure, err))
	}
	defer db.releaseCOWBasisOnUnwind(&basis)
	old, err := db.installCOWBasis(basis, generationPressure)
	cleanupErr := basis.release()
	basis = nil
	if refusal, ok := err.(*cowGenerationSnapshotAllocationRefusal); ok && cleanupErr == nil {
		err = cowGenerationRetryableRefusal{refusal}
	}
	err = joinCOWCleanupError(err, cleanupErr)
	if err != nil {
		return backendMaintenanceError("install COW basis", err)
	}
	if old != nil {
		err = old.drain()
	}
	db.cow.refreshRequired.Store(false)
	return backendMaintenanceError("drain previous COW basis", err)
}

// Original capture ownership also survives a provider panic/Goexit. The
// containing basis admission covers these fixed deferred-call cells. Normal
// paths consume and clear the slot explicitly, preserving their error order.
func (db *DB) releaseCOWBasisOnUnwind(slot **cowBackendBasis) {
	basis := *slot
	*slot = nil
	if basis != nil {
		// A live slot means the install did not return normally. Physical
		// maintenance may already be accepted even if capture release succeeds.
		// Publish reconciliation debt before its fallible destructor runs.
		db.cow.refreshRequired.Store(true)
		if err := basis.release(); err != nil {
			db.poisonNativeHandoff(err)
			db.retainOriginalCleanupError(err)
		}
	}
}

func (db *DB) finishCOWBackendMaintenance(fnErr error) error {
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return errors.Join(fnErr, backendMaintenanceError("refresh backend value-log set after callback", err))
		}
	}
	return errors.Join(fnErr, db.refreshCOWBackendBasis())
}

// ValueLogRewriteOnline fences a named equivalent physical rewrite with the
// existing cache owner. The caller supplies ordinary rewrite options only; it
// cannot grant this role to a callback or another physical publication.
func (db *DB) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (stats backenddb.ValueLogRewriteStats, err error) {
	if db == nil || db.cow == nil {
		return stats, backenddb.ErrNativeSemanticUnsupported
	}
	err = db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true, completeAdmittedCOW: true}, func() error {
		// Read the original role only after admission: pending Close cannot dispose
		// its containing cache while this operation owns the original flush fence.
		if db.cow.txnOwner == nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		rewriter, ok := db.backend.(interface {
			ValueLogRewriteOnlineForPrivateCOW(*transactionowner.Owner, context.Context, backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
		})
		if !ok {
			return backenddb.ErrNativeSemanticUnsupported
		}
		var rewriteErr error
		stats, rewriteErr = rewriter.ValueLogRewriteOnlineForPrivateCOW(db.cow.txnOwner, ctx, opts)
		return rewriteErr
	})
	return stats, err
}

func (db *DB) hasActiveTypedColumnGraph() (bool, error) {
	if db == nil || db.cow == nil {
		return false, nil
	}
	cut := db.cow.retainCut()
	if cut == nil {
		return false, backenddb.ErrClosed
	}
	active := false
	if obs, ok := cut.semanticRootSet.(backenddb.NativeSemanticTypedColumnGraphObserver); ok && obs.HasActiveTypedColumnGraph() {
		active = true
	}
	if !active {
		if obs, ok := cut.frozenSemanticRootSet.(backenddb.NativeSemanticTypedColumnGraphObserver); ok && obs.HasActiveTypedColumnGraph() {
			active = true
		}
	}
	return active, db.cow.releaseCut(cut)
}

// CompactStorage fences owner-authenticated storage compaction under the
// existing private-COW maintenance fence. It inspects semantic state under
// flushMu before mutating storage and fails closed when active typed
// column-graph collections exist, refreshes the COW backend basis after
// compaction, converges the command-WAL fallback slot when applicable, and
// invokes settle under the SAME writer fence.
func (db *DB) CompactStorage(ctx context.Context, opts backenddb.CompactStorageOptions, settle func(*backenddb.CompactStorageStats) error) (stats backenddb.CompactStorageStats, err error) {
	if db == nil || db.cow == nil {
		return stats, backenddb.ErrNativeSemanticUnsupported
	}
	postRefresh := func() error {
		if db.externalCommandWAL {
			if fallbackErr := db.refreshCOWCheckpointFallback(); fallbackErr != nil {
				return fallbackErr
			}
		}
		if settle == nil {
			return nil
		}
		if settleErr := settle(&stats); settleErr != nil {
			return settleErr
		}
		// If the first post-refresh GC pass newly transitioned packed or
		// retiring leaf generations to deleted/zombie, the live COW read basis
		// captured before that GC pass still retains the pre-GC ValueLogSet.
		// Refresh and drain that basis so zombie files unlink immediately, then
		// run one final settle pass to prune deleted manifest records and
		// refresh stats.After / FilesDeleted.
		needsSecondSettle := stats.LeafGenerationGC.GenerationsRetiring > 0 ||
			(stats.LeafGenerationGC.BytesDeleted > 0 && stats.LeafGenerationGC.FilesDeleted == 0) ||
			stats.LeafGenerationGC.GenerationsEligible > stats.LeafGenerationGC.GenerationsDeleted
		if needsSecondSettle {
			if refreshErr := db.finishCOWBackendMaintenance(nil); refreshErr != nil {
				return refreshErr
			}
			return settle(&stats)
		}
		return nil
	}
	err = db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{
		skipCheckpoint:      true,
		completeAdmittedCOW: true,
		afterCOWRefresh:     postRefresh,
	}, func() error {
		if db.cow.txnOwner == nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		activeTypedGraph, checkErr := db.hasActiveTypedColumnGraph()
		if checkErr != nil {
			return checkErr
		}
		if activeTypedGraph {
			return backenddb.ErrNativeSemanticUnsupported
		}
		compactor, ok := db.backend.(interface {
			CompactStorageForPrivateCOW(*transactionowner.Owner, context.Context, backenddb.CompactStorageOptions) (backenddb.CompactStorageStats, error)
		})
		if !ok {
			return backenddb.ErrNativeSemanticUnsupported
		}
		var compactErr error
		stats, compactErr = compactor.CompactStorageForPrivateCOW(db.cow.txnOwner, ctx, opts)
		return compactErr
	})
	return stats, err
}
