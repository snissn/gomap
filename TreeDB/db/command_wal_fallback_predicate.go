package db

import "context"

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
	unlock := db.lockCommandWALRawPublishWithTeardown()
	defer unlock()
	db.writeMu.RLock()
	defer db.writeMu.RUnlock()
	db.mu.RLock()
	runtime, visible := db.rootPublication, db.state.Load()
	db.mu.RUnlock()
	if visible == nil {
		return false, ErrClosed
	}
	if runtime != nil && runtime.coordinator != nil {
		if err := runtime.coordinator.WaitThrough(context.Background(), visible.CommitSeq); err != nil {
			return false, publicRootPublicationErrorV1(err)
		}
	}
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	state := db.state.Load()
	current, live := stateTokenFromState(state)
	if !live || current != basis {
		return false, ErrRootPublicationBasisMismatch
	}
	if db.durableRoot.slot > 1 {
		return false, ErrRootPublicationBasisMismatch
	}
	return db.commandWALCheckpointFallbackNeedsRefreshLocked(state.AppliedCommandLSN, db.durableRoot.slotRecord[db.durableRoot.slot], db.durableRoot.slotRecord[db.durableRoot.slot^1]), nil
}
