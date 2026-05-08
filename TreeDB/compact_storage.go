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

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
		if opts.ReserveRIDs == nil {
			opts.ReserveRIDs = db.cached.ReserveValueLogRIDs
		}
		opts.DisableZeroByteValueLogCleanup = true
	}
	stats, err := db.backend.CompactStorage(ctx, treedbdb.CompactStorageOptions(opts))
	if err != nil {
		return out, err
	}
	success = true
	return CompactStorageStats(stats), nil
}
