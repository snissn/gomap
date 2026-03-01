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
	maindbDir, err := resolveMainDBDir(opts.Dir)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	opts.Dir = maindbDir

	stats, err := treedbdb.ValueLogRewriteOffline(opts)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	return ValueLogRewriteStats(stats), nil
}

// ValueLogRewriteOnline rewrites pointer-backed values in bounded commit
// batches and atomically swaps keys to rewritten pointers.
//
// In cached mode this method flushes first so the backend reflects all buffered
// writes before running online rewrite against the backend DB.
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
	if db.cached != nil {
		// Avoid Checkpoint() here: it may rotate/prune log segments, which is not
		// required for rewrite correctness and can surprise callers that use
		// rewrite as a pure compaction step.
		if err := db.cached.Flush(); err != nil {
			return ValueLogRewriteStats{}, err
		}
	}
	stats, err := db.backend.ValueLogRewriteOnline(ctx, treedbdb.ValueLogRewriteOnlineOptions(opts))
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	success = true
	return ValueLogRewriteStats(stats), nil
}
