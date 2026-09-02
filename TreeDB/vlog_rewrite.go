package treedb

import (
	"context"
	"fmt"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/template"
)

// ValueLogRewriteStats summarizes value-log rewrite compaction results.
type ValueLogRewriteStats = treedbdb.ValueLogRewriteStats

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
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return ValueLogRewriteStats{}, err
	}
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	// Preserve the persisted on-disk format knobs by default so offline rewrite
	// doesn't accidentally rebuild the index/value-log into a different layout.
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := treedbdb.CommandWALRequiredFeatureEnabled(layout.mainDir)
		if err != nil {
			return ValueLogRewriteStats{}, err
		}
		if requiresCommandWAL {
			return ValueLogRewriteStats{}, treedbdb.ErrCommandWALUnsupported
		}
	} else {
		if cfg, ok, err := treedbdb.LoadFormatConfig(layout.mainDir); err != nil {
			return ValueLogRewriteStats{}, err
		} else if ok {
			if cfg.RequiresCommandWALV1() {
				return ValueLogRewriteStats{}, treedbdb.ErrCommandWALUnsupported
			}
			cfg.ApplyToOptions(&opts)
		}
	}
	if opts.ValueLog.TemplateMode != template.TemplateOff {
		return ValueLogRewriteStats{}, fmt.Errorf("%w: offline template rewrite requires dependency-closed rewritten-root publication (#3679)", rootpublication.ErrUnresolvedResource)
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lockFullScanMaintenanceContext(ctx, &db.bgVac.runMu); err != nil {
		return ValueLogRewriteStats{}, err
	}
	defer db.bgVac.runMu.Unlock()
	if db.bgVac.deferredVectorBuildClosed.Load() || db.backend == nil {
		return ValueLogRewriteStats{}, ErrClosed
	}
	_, finishMaintenance, err := db.beginFullScanMaintenanceContext(ctx, "rewrite")
	if err != nil {
		return ValueLogRewriteStats{}, err
	}
	db.endDeferredVectorBuild()
	success := false
	defer func() { finishMaintenance(success) }()

	backendOpts := treedbdb.ValueLogRewriteOnlineOptions(opts)
	if db.cached != nil {
		if err := db.Checkpoint(); err != nil {
			return ValueLogRewriteStats{}, err
		}
		if len(backendOpts.ProtectedPaths) == 0 {
			backendOpts.ProtectedPaths = db.cached.ValueLogProtectedPaths()
			if len(backendOpts.ProtectedPaths) == 0 {
				// Sentinel keeps backend active-segment protection enabled even
				// before cached mode has concrete protected paths to forward.
				backendOpts.ProtectedPaths = []string{""}
			}
		}
		protectedRootIDs, protectedSystemRootIDs := db.cached.ProtectedLeafGenerationRootIDPair()
		backendOpts.LeafGenerationProtectedRootIDs = mergeCompactStorageProtectedRootIDs(backendOpts.LeafGenerationProtectedRootIDs, protectedRootIDs)
		backendOpts.LeafGenerationProtectedSystemRootIDs = mergeCompactStorageProtectedRootIDs(backendOpts.LeafGenerationProtectedSystemRootIDs, protectedSystemRootIDs)
		if backendOpts.ReserveRIDs == nil {
			backendOpts.ReserveRIDs = db.cached.ReserveValueLogRIDs
		}
	}
	stats, err := db.backend.ValueLogRewriteOnline(ctx, backendOpts)
	if err = db.reconcileCachedBackendMaintenance(err); err != nil {
		return ValueLogRewriteStats{}, err
	}
	if db.cached != nil && len(stats.SourceFileIDsUnreferenced) > 0 {
		reclaimStats, err := db.cached.ReclaimObservedValueLogSources(ctx, stats.SourceFileIDsUnreferenced)
		if err != nil {
			return ValueLogRewriteStats{}, err
		}
		stats.SourceSegmentsReclaimed += reclaimStats.ObservedSourceSegmentsDeleted
		stats.SourceBytesReclaimed += reclaimStats.ObservedSourceBytesDeleted
	}
	success = true
	return ValueLogRewriteStats(stats), nil
}
