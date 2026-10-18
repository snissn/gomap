package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// RefreshCOWCommandWALCheckpointFallback converges recovery slots through the
// same complete-cut physical build and exact Snapshot handoff as a checkpoint.
// It never grants the ordinary backend publisher cache-owner authority.
func (db *DB) RefreshCOWCommandWALCheckpointFallback() error {
	if db == nil || db.cow == nil {
		return ErrCOWUnsupported
	}
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{
		skipCheckpoint: true, cowCheckpointFallback: true,
	}, nil)
}

// Called only under maintenance admission and flushMu, after admitted writers
// and the previous accepted handoff drain. No generic post-maintenance capture
// follows: the build itself preadmits and binds its exact successor Snapshot.
func (db *DB) refreshCOWCheckpointFallback() (retErr error) {
	c := db.cow
	backend, ok := db.backend.(interface {
		CommandWALCheckpointFallbackNeedsRefresh(backenddb.StateToken) (bool, error)
	})
	if !ok {
		return ErrCOWUnsupported
	}
	cut := c.retainCut()
	defer func() { retErr = joinCOWCleanupError(retErr, c.releaseCut(cut)) }()
	if cut == nil || c.prefix == nil {
		return backenddb.ErrClosed
	}
	frontiers := c.prefix.LogicalFrontiers()
	if cut.logicalSeq != frontiers.S.LogicalSequence || cut.visibleCommandLSN != frontiers.S.CanonicalLSN || len(cut.frozen) != 0 || cut.basis.logicalSeq != cut.logicalSeq {
		return backenddb.ErrRootPublicationBasisMismatch
	}
	basis, live := cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	needed, err := backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil || !needed {
		return err
	}
	err = db.flushCOWFrozenCutForFallback(true, nil, cut, true)
	cut = nil // the same flush owner consumed the original retained cut
	if err != nil {
		return err
	}
	basis, live = c.cut.basis.snapshot.StateToken()
	if !live {
		return backenddb.ErrClosed
	}
	needed, err = backend.CommandWALCheckpointFallbackNeedsRefresh(basis)
	if err != nil {
		return err
	}
	if needed {
		return backenddb.ErrRootPublicationBasisMismatch
	}
	return nil
}
