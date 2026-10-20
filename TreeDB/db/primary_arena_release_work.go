package db

import "sync"

// Original physical cleanup is attempted once. A failure retains this same
// owner, its remaining aliases and its containing charge.
type primaryArenaCloseWork struct {
	mu                 sync.Mutex
	started, done      bool
	arenaErr, proofErr error
}

// releaseOriginalWithOutcome consumes exactly one original edge. Its by-value
// completion carries accounting only; the containing caller must clear its
// owner alias and return from cleanup before consuming that completion.
func (owner *primaryArenaOwnerV5) releaseOriginalWithOutcome() (consumed, physicalDebt bool, completion primaryRuntimeDisposalCompletionV6, err error) {
	if owner == nil {
		return false, false, completion, ErrSnapshotCapacity
	}
	owner.mu.Lock()
	if owner.refs == 0 {
		owner.mu.Unlock()
		owner.closeWork.mu.Lock()
		defer owner.closeWork.mu.Unlock()
		if owner.closeWork.done && owner.arena != nil {
			return false, true, completion, &owner.closeFailure
		}
		return false, false, completion, ErrSnapshotCapacity
	}
	owner.refs--
	prior := owner.cleanupErr
	if owner.refs != 0 {
		owner.mu.Unlock()
		return true, false, completion, prior
	}
	owner.closeWork.started = true
	owner.mu.Unlock()
	state := &owner.closeWork
	state.mu.Lock()
	defer state.mu.Unlock()
	state.arenaErr = owner.arena.Close()
	state.proofErr = owner.clearNamespaceV5()
	if state.arenaErr != nil || state.proofErr != nil || prior != nil {
		owner.closeFailure.causes = [3]error{state.arenaErr, state.proofErr, prior}
		owner.arena.MetadataOwner().CleanupFailed()
		state.done = true
		return true, true, completion, &owner.closeFailure
	}
	completion = primaryRuntimeDisposalCompletionV6{owner.arena.MetadataOwner(), owner.metadataCharge}
	owner.arena, owner.metadataCharge = nil, 0
	state.done = true
	return true, false, completion, nil
}
