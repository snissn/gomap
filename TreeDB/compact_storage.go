package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// CompactStorageMode selects how aggressively CompactStorage should reclaim
// storage. Empty/default currently maps to full storage compaction.
type CompactStorageMode = treedbdb.CompactStorageMode

const (
	CompactStorageDefault = treedbdb.CompactStorageDefault
	CompactStorageFull    = treedbdb.CompactStorageFull
	CompactStorageQuick   = treedbdb.CompactStorageQuick
)

// CompactStorageOptions controls full storage compaction across TreeDB storage
// domains. Prefer this high-level API over manually sequencing value-log
// rewrite, value-log GC, leaf-generation pack/GC, and index vacuum.
type CompactStorageOptions = treedbdb.CompactStorageOptions

// CompactStorageUsage summarizes file usage for a storage domain.
type CompactStorageUsage = treedbdb.CompactStorageUsage

// CompactStorageDebt summarizes remaining work after planning or compaction.
type CompactStorageDebt = treedbdb.CompactStorageDebt

// CompactStoragePhaseStats records one phase in a full compaction run.
type CompactStoragePhaseStats = treedbdb.CompactStoragePhaseStats

// CompactStorageStats is the single high-level report for TreeDB storage
// compaction and planning.
type CompactStorageStats = treedbdb.CompactStorageStats

// CompactStoragePlan reports full storage compaction debt without mutating the
// database. It is safe for read-only opens.
func (db *DB) CompactStoragePlan(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	var out CompactStorageStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	if err := db.applyCachedCompactStorageOptions(&opts, false); err != nil {
		return out, err
	}
	return db.backend.CompactStoragePlan(ctx, treedbdb.CompactStorageOptions(opts))
}

// CompactStorage runs the recommended full storage compaction sequence:
// value-log rewrite, value-log GC, leaf-generation pack, leaf-generation GC,
// index vacuum, final GC settle passes, empty value-log file cleanup, and a
// final audit.
func (db *DB) CompactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	var out CompactStorageStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("compact-storage")
	success := false
	defer func() { finishMaintenance(success) }()

	if err := db.applyCachedCompactStorageOptions(&opts, true); err != nil {
		return out, err
	}
	finishValueLogFence := func() {}
	if db.cached != nil && opts.UnsafeValueLogReclaimFencedUnreferenced {
		var err error
		finishValueLogFence, err = db.cached.BeginValueLogMaintenanceFence(ctx)
		if err != nil {
			return out, err
		}
		db.cached.PruneRetainedValueLogsForMaintenance()
		if err := db.backend.RefreshValueLogSet(); err != nil {
			finishValueLogFence()
			return out, err
		}
	}
	fenceActive := true
	defer func() {
		if fenceActive {
			finishValueLogFence()
		}
	}()
	stats, err := db.backend.CompactStorage(ctx, treedbdb.CompactStorageOptions(opts))
	if err = db.reconcileCachedBackendMaintenance(err); err != nil {
		return out, err
	}
	finishValueLogFence()
	fenceActive = false
	if db.cached != nil && len(stats.ValueLogRewrite.SourceFileIDsUnreferenced) > 0 {
		if err := db.cached.ReclaimObservedValueLogSources(ctx, stats.ValueLogRewrite.SourceFileIDsUnreferenced); err != nil {
			return out, err
		}
	}
	success = true
	return CompactStorageStats(stats), nil
}

func (db *DB) applyCachedCompactStorageOptions(opts *CompactStorageOptions, checkpoint bool) error {
	if db == nil || db.cached == nil || opts == nil {
		return nil
	}
	if checkpoint {
		if err := db.Checkpoint(); err != nil {
			return err
		}
	}
	explicitProtectedPaths := append([]string(nil), opts.ValueLogProtectedPaths...)
	userProtectedPathsFunc := opts.ValueLogProtectedPathsFunc
	explicitProtectedRootIDs := append([]uint64(nil), opts.LeafGenerationProtectedRootIDs...)
	userProtectedRootIDsFunc := opts.LeafGenerationProtectedRootIDsFunc
	opts.ValueLogProtectedPathsFunc = func() []string {
		var out []string
		if userProtectedPathsFunc != nil {
			out = appendCompactStorageProtectedPaths(out, userProtectedPathsFunc())
		}
		out = appendCompactStorageProtectedPaths(out, db.cached.ValueLogProtectedPaths())
		return out
	}
	opts.LeafGenerationProtectedRootIDsFunc = func() []uint64 {
		var out []uint64
		if userProtectedRootIDsFunc != nil {
			out = appendCompactStorageProtectedRootIDs(out, userProtectedRootIDsFunc())
		}
		out = appendCompactStorageProtectedRootIDs(out, db.cached.ProtectedLeafGenerationRootIDs())
		return out
	}
	opts.ValueLogProtectedPaths = explicitProtectedPaths
	opts.LeafGenerationProtectedRootIDs = explicitProtectedRootIDs
	if opts.ReserveRIDs == nil {
		opts.ReserveRIDs = db.cached.ReserveValueLogRIDs
	}
	if checkpoint && len(explicitProtectedPaths) == 0 && userProtectedPathsFunc == nil {
		opts.UnsafeValueLogReclaimFencedUnreferenced = true
		if opts.ValueLogFencedProtectedPathsFunc == nil {
			opts.ValueLogFencedProtectedPathsFunc = db.cached.ValueLogInUsePaths
		}
	}
	return nil
}

func appendCompactStorageProtectedRootIDs(dst []uint64, src []uint64) []uint64 {
	for _, rootID := range src {
		if rootID == 0 {
			continue
		}
		seen := false
		for _, existing := range dst {
			if existing == rootID {
				seen = true
				break
			}
		}
		if !seen {
			dst = append(dst, rootID)
		}
	}
	return dst
}

func appendCompactStorageProtectedPaths(dst []string, src []string) []string {
	for _, path := range src {
		seen := false
		for _, existing := range dst {
			if existing == path {
				seen = true
				break
			}
		}
		if !seen {
			dst = append(dst, path)
		}
	}
	return dst
}
