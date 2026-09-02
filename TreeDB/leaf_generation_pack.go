package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationPackOptions selects sealed source generations for explicit
// leaf-page compaction.
type LeafGenerationPackOptions = treedbdb.LeafGenerationPackOptions

// LeafGenerationPackStats summarizes explicit leaf-generation pack work.
type LeafGenerationPackStats = treedbdb.LeafGenerationPackStats

// LeafGenerationPack copies live pages from sealed source generations into a
// fresh leaf-log output so whole-generation GC can later reclaim the old files.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationPack(ctx context.Context, opts LeafGenerationPackOptions) (LeafGenerationPackStats, error) {
	var out LeafGenerationPackStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-pack")
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
	stats, err := db.backend.LeafGenerationPack(ctx, treedbdb.LeafGenerationPackOptions(opts))
	if err = db.reconcileCachedBackendMaintenance(err); err != nil {
		return out, err
	}
	success = true
	return LeafGenerationPackStats(stats), nil
}
