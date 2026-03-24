package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// ValueLogRewriteStats summarizes value-log rewrite compaction results.
type ValueLogRewriteStats struct {
	SegmentsBefore int
	SegmentsAfter  int
	BytesBefore    int64
	BytesAfter     int64
	RecordsCopied  int
}

// ValueLogRewritePlan summarizes which segments a sparse online rewrite would
// select under the provided options.
type ValueLogRewritePlan = treedbdb.ValueLogRewritePlan

// ValueLogRewriteOnlineOptions controls online rewrite batching behavior.
type ValueLogRewriteOnlineOptions = treedbdb.ValueLogRewriteOnlineOptions

// ValueLogRewriteLocalityPolicy controls pointer rewrite ordering.
type ValueLogRewriteLocalityPolicy = treedbdb.ValueLogRewriteLocalityPolicy

const (
	ValueLogRewriteLocalityDefault = treedbdb.ValueLogRewriteLocalityDefault
	ValueLogRewriteLocalityGrouped = treedbdb.ValueLogRewriteLocalityGrouped
)

// ValueLogRewriteOffline rewrites value-log pointers into new segments and swaps
// index.db to reference the new log. This is an offline operation that requires
// an exclusive lock and a clean commitlog.
func ValueLogRewriteOffline(opts Options) (ValueLogRewriteStats, error) {
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	// Preserve the persisted on-disk format knobs by default so offline rewrite
	// doesn't accidentally rebuild the index/value-log into a different layout.
	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := treedbdb.LoadFormatConfig(layout.mainDir); err != nil {
			return ValueLogRewriteStats{}, err
		} else if ok {
			cfg.ApplyToOptions(&opts)
		}
	}

	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	defer func() { _ = sideCleanup() }()

	stats, err := treedbdb.ValueLogRewriteOffline(opts)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	return ValueLogRewriteStats(stats), nil
}

// ValueLogRewriteOnline rewrites pointer-backed values in bounded commit
// batches and atomically swaps keys to rewritten pointers.
//
// In cached mode this method checkpoints first to establish a stable backend
// baseline before running online rewrite against the backend DB.
func (db *DB) ValueLogRewriteOnline(ctx context.Context, opts ValueLogRewriteOnlineOptions) (ValueLogRewriteStats, error) {
	if err := db.ensureOpen(); err != nil {
		return ValueLogRewriteStats{}, err
	}
	if db.backend == nil {
		return ValueLogRewriteStats{}, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("rewrite")
	success := false
	defer func() { finishMaintenance(success) }()

	backendOpts := treedbdb.ValueLogRewriteOnlineOptions(opts)
	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return ValueLogRewriteStats{}, err
		}
		if len(backendOpts.ProtectedPaths) == 0 {
			backendOpts.ProtectedPaths = db.cached.ValueLogRetainedPaths()
		}
		if len(backendOpts.ProtectedPaths) == 0 {
			// Cached-mode callers may have concurrent writers even when there are
			// no retained paths yet; pass a non-empty slice to activate the
			// backend rewrite's active-segment protection.
			backendOpts.ProtectedPaths = []string{""}
		}
	}
	stats, err := db.backend.ValueLogRewriteOnline(ctx, backendOpts)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	success = true
	return ValueLogRewriteStats(stats), nil
}

// ValueLogRewritePlan returns the segments that would be selected for sparse
// online rewrite under the provided options.
func (db *DB) ValueLogRewritePlan(ctx context.Context, opts ValueLogRewriteOnlineOptions) (ValueLogRewritePlan, error) {
	if err := db.ensureOpen(); err != nil {
		return ValueLogRewritePlan{}, err
	}
	if db.backend == nil {
		return ValueLogRewritePlan{}, ErrClosed
	}
	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return ValueLogRewritePlan{}, err
		}
	}
	plan, err := db.backend.ValueLogRewritePlan(ctx, treedbdb.ValueLogRewriteOnlineOptions(opts))
	if err != nil {
		return ValueLogRewritePlan{}, err
	}
	return ValueLogRewritePlan(plan), nil
}
