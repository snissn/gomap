package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationGCOptions controls whole-generation leaf-log garbage
// collection.
type LeafGenerationGCOptions struct {
	DryRun                 bool
	ProtectedRootIDs       []uint64
	ProtectedSystemRootIDs []uint64
}

// LeafGenerationGCStats summarizes whole-generation leaf-log GC work.
type LeafGenerationGCStats struct {
	GenerationsTotal    int
	GenerationsWritable int
	GenerationsLive     int
	GenerationsRetiring int
	GenerationsEligible int
	GenerationsDeleted  int
	FilesDeleted        int
}

// LeafGenerationGC deletes fully unreachable, unpinned sealed leaf generations.
//
// In cached mode, this first checkpoints so the backend root used for
// reachability matches the current public DB state.
func (db *DB) LeafGenerationGC(ctx context.Context, opts LeafGenerationGCOptions) (LeafGenerationGCStats, error) {
	var out LeafGenerationGCStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-gc")
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

	stats, err := db.backend.LeafGenerationGC(ctx, treedbdb.LeafGenerationGCOptions{
		DryRun:                 opts.DryRun,
		ProtectedRootIDs:       opts.ProtectedRootIDs,
		ProtectedSystemRootIDs: opts.ProtectedSystemRootIDs,
	})
	if err != nil {
		return out, err
	}
	out.GenerationsTotal = stats.GenerationsTotal
	out.GenerationsWritable = stats.GenerationsWritable
	out.GenerationsLive = stats.GenerationsLive
	out.GenerationsRetiring = stats.GenerationsRetiring
	out.GenerationsEligible = stats.GenerationsEligible
	out.GenerationsDeleted = stats.GenerationsDeleted
	out.FilesDeleted = stats.FilesDeleted
	success = true
	return out, nil
}
