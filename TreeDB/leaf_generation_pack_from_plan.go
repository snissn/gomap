package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationPackFromPlanOptions combines planner thresholds, bounded
// selection limits, and pack execution settings for the manual from-plan path.
type LeafGenerationPackFromPlanOptions = treedbdb.LeafGenerationPackFromPlanOptions

// LeafGenerationPackFromPlan computes the current plan, selects a bounded
// candidate prefix, then packs those sealed generations.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationPackFromPlan(ctx context.Context, opts LeafGenerationPackFromPlanOptions) (LeafGenerationPackStats, error) {
	var out LeafGenerationPackStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-pack-from-plan")
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
	stats, err := db.backend.LeafGenerationPackFromPlan(ctx, treedbdb.LeafGenerationPackFromPlanOptions(opts))
	if err = db.reconcileCachedBackendMaintenance(err); err != nil {
		return out, err
	}
	success = true
	return LeafGenerationPackStats(stats), nil
}
