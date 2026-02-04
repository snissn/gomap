package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// ValueLogGCOptions controls value-log garbage collection.
type ValueLogGCOptions struct {
	DryRun bool
}

// ValueLogGCStats summarizes value-log GC work.
type ValueLogGCStats struct {
	SegmentsTotal      int
	SegmentsReferenced int
	SegmentsActive     int
	SegmentsEligible   int
	SegmentsDeleted    int
	BytesTotal         int64
	BytesReferenced    int64
	BytesActive        int64
	BytesEligible      int64
	BytesDeleted       int64
}

// ValueLogGC deletes fully-unreferenced value-log segments.
//
// In cached mode, this first checkpoints to ensure memtable/WAL state is fully
// reflected in the backend before scanning pointers.
func (db *DB) ValueLogGC(ctx context.Context, opts ValueLogGCOptions) (ValueLogGCStats, error) {
	var out ValueLogGCStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	if db.cached != nil {
		if err := db.cached.Checkpoint(); err != nil {
			return out, err
		}
	}

	stats, err := db.backend.ValueLogGC(ctx, treedbdb.ValueLogGCOptions{DryRun: opts.DryRun})
	if err != nil {
		return out, err
	}
	out = ValueLogGCStats(stats)
	return out, nil
}
