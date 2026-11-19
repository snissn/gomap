package db

import (
	"context"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// CommandWALCheckpointFallbackNeedsRefresh samples the real selectable-root
// convergence predicate against the caller's exact complete-cut basis. This
// grants no publication authority and captures no replacement Snapshot.
func (db *DB) CommandWALCheckpointFallbackNeedsRefresh(basis StateToken) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	if !db.commandWAL {
		return false, nil
	}
	// Keep Close from stopping the coordinator while this check is admitted.
	// The physical seal must run outside both foreground publication gates.
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return false, ErrClosed
	}
	db.mu.RLock()
	runtime := db.rootPublication
	visible, live := stateTokenFromState(db.state.Load())
	var coordinator *rootpublication.Coordinator
	if runtime != nil {
		coordinator = runtime.coordinator
	}
	db.mu.RUnlock()
	if !live {
		return false, ErrClosed
	}
	return db.commandWALCheckpointFallbackNeedsRefreshAfterCapture(basis, visible, runtime, coordinator)
}

// The caller pins teardown from capture through the final check. Vacuum can
// still replace this runtime and stop its coordinator before the wait starts.
func (db *DB) commandWALCheckpointFallbackNeedsRefreshAfterCapture(basis, visible StateToken, runtime *rootPublicationRuntimeV1, coordinator *rootpublication.Coordinator) (bool, error) {
	if coordinator != nil {
		if err := coordinator.WaitThrough(context.Background(), visible.CommitSeq); err != nil {
			if err == rootpublication.ErrClosed || err == rootpublication.ErrPublicationStopped {
				db.mu.RLock()
				replaced := db.rootPublication != runtime
				db.mu.RUnlock()
				if replaced {
					return false, ErrRootPublicationBasisMismatch
				}
			}
			return false, publicRootPublicationErrorV1(err)
		}
	}

	// A writer or online vacuum may have advanced the backend while the seal
	// was running. The old wait certifies only the captured runtime and basis.
	db.commandWALRawPublishMu.Lock()
	defer db.commandWALRawPublishMu.Unlock()
	db.writeMu.RLock()
	defer db.writeMu.RUnlock()
	if db.vacuumCutoverInProgress.Load() {
		// Vacuum can hold durablePublishMu while reacquiring writeMu.
		return false, ErrRootPublicationBasisMismatch
	}
	db.mu.RLock()
	currentRuntime := db.rootPublication
	db.mu.RUnlock()
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	current, live := stateTokenFromState(db.state.Load())
	if !live || currentRuntime != runtime || current != visible || current != basis {
		return false, fmt.Errorf("%w: fallback basis=%+v current=%+v", ErrRootPublicationBasisMismatch, basis, current)
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return false, err
	}
	if db.durableRoot.slot > 1 {
		return false, ErrRootPublicationBasisMismatch
	}
	return db.commandWALCheckpointFallbackNeedsRefreshLocked(current.AppliedCommandLSN, db.durableRoot.slotRecord[db.durableRoot.slot], db.durableRoot.slotRecord[db.durableRoot.slot^1]), nil
}
