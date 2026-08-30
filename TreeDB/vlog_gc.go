package treedb

import (
	"context"
	"fmt"
	"strings"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// ValueLogGCMode controls cached-mode coordination for value-log GC.
//
// Pre-alpha note: "online" currently uses retained-path protection and may
// fail closed to dry-run when protection data is unavailable.
type ValueLogGCMode string

const (
	ValueLogGCModeStrict ValueLogGCMode = "strict"
	ValueLogGCModeOnline ValueLogGCMode = "online"
)

func normalizeValueLogGCMode(mode ValueLogGCMode) (ValueLogGCMode, error) {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "", string(ValueLogGCModeStrict):
		return ValueLogGCModeStrict, nil
	case string(ValueLogGCModeOnline):
		return ValueLogGCModeOnline, nil
	default:
		return ValueLogGCModeStrict, fmt.Errorf("unsupported ValueLogGC mode %q (expected strict|online)", mode)
	}
}

// ValueLogGCOptions controls value-log garbage collection.
type ValueLogGCOptions struct {
	DryRun bool
	// Mode controls cached-mode behavior. Empty defaults to "strict".
	Mode ValueLogGCMode
}

// ValueLogGCStats summarizes value-log GC work.
type ValueLogGCStats struct {
	SegmentsTotal      int
	SegmentsReferenced int
	SegmentsActive     int
	SegmentsProtected  int
	SegmentsEligible   int
	SegmentsDeleted    int
	BytesTotal         int64
	BytesReferenced    int64
	BytesActive        int64
	BytesProtected     int64
	BytesEligible      int64
	BytesDeleted       int64
	// FailClosedToDryRun reports that online mode intentionally converted the
	// operation to a dry-run for safety.
	FailClosedToDryRun bool
}

// ValueLogGC deletes fully-unreferenced value-log segments.
//
// In cached mode, strict mode checkpoints first. Online mode avoids a global
// checkpoint and protects retained value-log segments; if no protection data is
// available, it fails closed to dry-run.
func (db *DB) ValueLogGC(ctx context.Context, opts ValueLogGCOptions) (ValueLogGCStats, error) {
	var out ValueLogGCStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lockFullScanMaintenanceContext(ctx, &db.bgVac.runMu); err != nil {
		return out, err
	}
	defer db.bgVac.runMu.Unlock()
	if db.bgVac.deferredVectorBuildClosed.Load() || db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance, err := db.beginFullScanMaintenanceContext(ctx, "gc")
	if err != nil {
		return out, err
	}
	db.endDeferredVectorBuild()
	success := false
	defer func() { finishMaintenance(success) }()

	mode, err := normalizeValueLogGCMode(opts.Mode)
	if err != nil {
		return out, err
	}

	backendOpts := treedbdb.ValueLogGCOptions{DryRun: opts.DryRun}
	if db.cached != nil {
		switch mode {
		case ValueLogGCModeStrict:
			// Route through public checkpoint entry point to preserve lock ordering.
			if err := db.Checkpoint(); err != nil {
				return out, err
			}
		case ValueLogGCModeOnline:
			backendOpts.ProtectedPaths = db.cached.ValueLogProtectedPaths()
			// Fail closed when online mode has no protected-path data.
			if !backendOpts.DryRun && len(backendOpts.ProtectedPaths) == 0 {
				backendOpts.DryRun = true
				out.FailClosedToDryRun = true
			}
		}
	}

	stats, err := db.backend.ValueLogGC(ctx, backendOpts)
	if err != nil {
		return out, err
	}
	out.SegmentsTotal = stats.SegmentsTotal
	out.SegmentsReferenced = stats.SegmentsReferenced
	out.SegmentsActive = stats.SegmentsActive
	out.SegmentsProtected = stats.SegmentsProtected
	out.SegmentsEligible = stats.SegmentsEligible
	out.SegmentsDeleted = stats.SegmentsDeleted
	out.BytesTotal = stats.BytesTotal
	out.BytesReferenced = stats.BytesReferenced
	out.BytesActive = stats.BytesActive
	out.BytesProtected = stats.BytesProtected
	out.BytesEligible = stats.BytesEligible
	out.BytesDeleted = stats.BytesDeleted
	success = true
	return out, nil
}
