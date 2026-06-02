package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationPlanOptions controls leaf-generation sparse-pack planning.
type LeafGenerationPlanOptions = treedbdb.LeafGenerationPlanOptions

// LeafGenerationPlanGeneration describes one manifest generation's current live
// and reclaim geometry.
type LeafGenerationPlanGeneration = treedbdb.LeafGenerationPlanGeneration

// LeafGenerationPlan summarizes explicit leaf-pack candidate generations.
type LeafGenerationPlan = treedbdb.LeafGenerationPlan

// LeafGenerationPlan scans the current live tree and reports per-generation
// live/dead bytes for explicit leaf-pack planning.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationPlan(ctx context.Context, opts LeafGenerationPlanOptions) (LeafGenerationPlan, error) {
	var out LeafGenerationPlan
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-plan")
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
		protectedRootIDs, protectedSystemRootIDs := db.cached.ProtectedLeafGenerationRootIDPair()
		opts.ProtectedRootIDs = mergeCompactStorageProtectedRootIDs(opts.ProtectedRootIDs, protectedRootIDs)
		opts.ProtectedSystemRootIDs = mergeCompactStorageProtectedRootIDs(opts.ProtectedSystemRootIDs, protectedSystemRootIDs)
	}
	plan, err := db.backend.LeafGenerationPlan(ctx, treedbdb.LeafGenerationPlanOptions(opts))
	if err != nil {
		return out, err
	}
	success = true
	return LeafGenerationPlan(plan), nil
}
