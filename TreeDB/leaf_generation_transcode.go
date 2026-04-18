package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

type LeafGenerationTranscodeOptions = treedbdb.LeafGenerationTranscodeOptions
type LeafGenerationTranscodePlanGeneration = treedbdb.LeafGenerationTranscodePlanGeneration
type LeafGenerationTranscodePlan = treedbdb.LeafGenerationTranscodePlan
type LeafGenerationTranscodeSelection = treedbdb.LeafGenerationTranscodeSelection
type LeafGenerationTranscodeRunOnceStats = treedbdb.LeafGenerationTranscodeRunOnceStats

// LeafGenerationTranscodePlan estimates the size win from rewriting sealed live
// leaf generations with the outer-leaf dict pipeline.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationTranscodePlan(ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodePlan, error) {
	var out LeafGenerationTranscodePlan
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-transcode-plan")
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
	}
	plan, err := db.backend.LeafGenerationTranscodePlan(ctx, treedbdb.LeafGenerationTranscodeOptions(opts))
	if err != nil {
		return out, err
	}
	success = true
	return LeafGenerationTranscodePlan(plan), nil
}

// LeafGenerationTranscodeRunOnce computes the current transcode plan, selects a
// bounded subset, and rewrites those sealed generations once.
//
// In cached mode, this first checkpoints so the backend roots match the current
// public DB state.
func (db *DB) LeafGenerationTranscodeRunOnce(ctx context.Context, opts LeafGenerationTranscodeOptions) (LeafGenerationTranscodeRunOnceStats, error) {
	var out LeafGenerationTranscodeRunOnceStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance := db.beginFullScanMaintenance("leaf-transcode-run-once")
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
		if opts.ReserveRIDs == nil {
			opts.ReserveRIDs = db.cached.ReserveValueLogRIDs
		}
	}
	stats, err := db.backend.LeafGenerationTranscodeRunOnce(ctx, treedbdb.LeafGenerationTranscodeOptions(opts))
	if err != nil {
		return out, err
	}
	success = true
	return LeafGenerationTranscodeRunOnceStats(stats), nil
}
