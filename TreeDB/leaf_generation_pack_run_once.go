package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationPackRunOnceStats describes one bounded admission/evaluation pass
// for leaf-generation packing.
type LeafGenerationPackRunOnceStats = treedbdb.LeafGenerationPackRunOnceStats

// LeafGenerationPackRunOnce computes the current plan, applies bounded
// selection, and either runs one pack pass or reports why it skipped.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationPackRunOnce(ctx context.Context, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackRunOnceStats, error) {
	var out LeafGenerationPackRunOnceStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-pack-run-once")
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
		protectedRootIDs, protectedSystemRootIDs := db.cached.ProtectedLeafGenerationRootIDPair()
		opts.ProtectedRootIDs = mergeCompactStorageProtectedRootIDs(opts.ProtectedRootIDs, protectedRootIDs)
		opts.ProtectedSystemRootIDs = mergeCompactStorageProtectedRootIDs(opts.ProtectedSystemRootIDs, protectedSystemRootIDs)
		if opts.ReserveRIDs == nil {
			opts.ReserveRIDs = db.cached.ReserveValueLogRIDs
		}
	}
	stats, err := db.backend.LeafGenerationPackRunOnce(ctx, treedbdb.LeafGenerationPackFromPlanOptions(opts))
	if err = db.reconcileCachedBackendMaintenance(err); err != nil {
		return out, err
	}
	success = true
	return LeafGenerationPackRunOnceStats(stats), nil
}
