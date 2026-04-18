package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// LeafGenerationLogicalRebuildStats summarizes a frozen leaf-only logical
// rebuild that rewrites outer-leaf pages and swaps a fresh index.
type LeafGenerationLogicalRebuildStats = treedbdb.LeafGenerationLogicalRebuildStats

// ErrLeafGenerationLogicalRebuildNoCandidate reports that no eligible sealed
// single-file leaf run was available for an online rebuild pass.
var ErrLeafGenerationLogicalRebuildNoCandidate = treedbdb.ErrLeafGenerationLogicalRebuildNoCandidate

// LeafGenerationLogicalRebuildRunOnceOptions controls one incremental online
// logical rebuild pass over a single eligible sealed leaf file.
type LeafGenerationLogicalRebuildRunOnceOptions = treedbdb.LeafGenerationLogicalRebuildRunOnceOptions

// LeafGenerationLogicalRebuildRunOnceStats summarizes one incremental online
// logical rebuild pass.
type LeafGenerationLogicalRebuildRunOnceStats = treedbdb.LeafGenerationLogicalRebuildRunOnceStats

// LeafGenerationLogicalRebuildOffline rebuilds outer-leaf pages logically into
// a fresh leaf_vlog directory and swaps it with a fresh index.db under the
// offline DB lock. Value-log pointers remain unchanged.
func LeafGenerationLogicalRebuildOffline(opts Options) (LeafGenerationLogicalRebuildStats, error) {
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := treedbdb.LoadFormatConfig(layout.mainDir); err != nil {
			return LeafGenerationLogicalRebuildStats{}, err
		} else if ok {
			cfg.ApplyToOptions(&opts)
		}
	}

	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	defer func() { _ = sideCleanup() }()

	stats, err := treedbdb.LeafGenerationLogicalRebuildOffline(opts)
	if err != nil {
		return LeafGenerationLogicalRebuildStats{}, err
	}
	return LeafGenerationLogicalRebuildStats(stats), nil
}

// LeafGenerationLogicalRebuildRunOnce performs one incremental online logical
// rebuild pass against an eligible sealed single-file leaf run.
//
// In cached mode, this first checkpoints so the backend matches the durable
// tree before the online maintenance pass begins.
func (db *DB) LeafGenerationLogicalRebuildRunOnce(ctx context.Context, opts LeafGenerationLogicalRebuildRunOnceOptions) (LeafGenerationLogicalRebuildRunOnceStats, error) {
	var out LeafGenerationLogicalRebuildRunOnceStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-logical-rebuild")
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
	}
	backendOpts := treedbdb.LeafGenerationLogicalRebuildRunOnceOptions(opts)
	if db.cached != nil {
		backendOpts.ReserveRIDs = db.cached.ReserveValueLogRIDs
	}
	stats, err := db.backend.LeafGenerationLogicalRebuildRunOnce(ctx, backendOpts)
	if err != nil {
		return out, err
	}
	success = true
	return LeafGenerationLogicalRebuildRunOnceStats(stats), nil
}
