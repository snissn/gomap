package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// CompactStorageMode selects how aggressively CompactStorage should reclaim
// storage. Empty/default currently maps to full storage compaction.
type CompactStorageMode = treedbdb.CompactStorageMode

const (
	CompactStorageDefault    = treedbdb.CompactStorageDefault
	CompactStorageFull       = treedbdb.CompactStorageFull
	CompactStorageQuick      = treedbdb.CompactStorageQuick
	CompactStorageExhaustive = treedbdb.CompactStorageExhaustive
)

type CompactStorageOwnerStatus = treedbdb.CompactStorageOwnerStatus

const (
	CompactStorageOwnerStatusSupportedTarget      = treedbdb.CompactStorageOwnerStatusSupportedTarget
	CompactStorageOwnerStatusLiveWriterFailClosed = treedbdb.CompactStorageOwnerStatusLiveWriterFailClosed
	CompactStorageOwnerStatusExternalUnsupported  = treedbdb.CompactStorageOwnerStatusExternalUnsupported
	CompactStorageOwnerStatusBlockingBug          = treedbdb.CompactStorageOwnerStatusBlockingBug
)

type CompactStorageLeafPageLogOwnerClass = treedbdb.CompactStorageLeafPageLogOwnerClass

const (
	CompactStorageLeafPageLogOwnerNone                     = treedbdb.CompactStorageLeafPageLogOwnerNone
	CompactStorageLeafPageLogOwnerCommandWALReplayInline   = treedbdb.CompactStorageLeafPageLogOwnerCommandWALReplayInline
	CompactStorageLeafPageLogOwnerInternalHiddenByWrapper  = treedbdb.CompactStorageLeafPageLogOwnerInternalHiddenByWrapper
	CompactStorageLeafPageLogOwnerCachedWrapper            = treedbdb.CompactStorageLeafPageLogOwnerCachedWrapper
	CompactStorageLeafPageLogOwnerStandaloneCallerExternal = treedbdb.CompactStorageLeafPageLogOwnerStandaloneCallerExternal
)

type CompactStorageLifecycleState = treedbdb.CompactStorageLifecycleState

const (
	CompactStorageLifecycleExclusiveMaintenance = treedbdb.CompactStorageLifecycleExclusiveMaintenance
	CompactStorageLifecycleQuiescedMaintenance  = treedbdb.CompactStorageLifecycleQuiescedMaintenance
	CompactStorageLifecycleActiveWriter         = treedbdb.CompactStorageLifecycleActiveWriter
)

var ErrCompactStorageLeafPageLogOwnerUnsupported = treedbdb.ErrCompactStorageLeafPageLogOwnerUnsupported

var ErrCompactStorageLeafPageLogHandoffCleanup = treedbdb.ErrCompactStorageLeafPageLogHandoffCleanup

var ErrCompactStorageAuditStale = treedbdb.ErrCompactStorageAuditStale

type CompactStorageLeafPageLogOwnerClassification = treedbdb.CompactStorageLeafPageLogOwnerClassification

type CompactStorageLeafPageLogOwnerError = treedbdb.CompactStorageLeafPageLogOwnerError

type CompactStorageLeafPageLogHandoffError = treedbdb.CompactStorageLeafPageLogHandoffError

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

type CompactStoragePhaseStatus = treedbdb.CompactStoragePhaseStatus

// VacuumOnlineStats records the production index replacement's phase and pause
// timings when CompactStorage runs index vacuum.
type VacuumOnlineStats = treedbdb.VacuumOnlineStats

const (
	CompactStoragePhaseStatusPlanned     = treedbdb.CompactStoragePhaseStatusPlanned
	CompactStoragePhaseStatusNotRequired = treedbdb.CompactStoragePhaseStatusNotRequired
	CompactStoragePhaseStatusSucceeded   = treedbdb.CompactStoragePhaseStatusSucceeded
	CompactStoragePhaseStatusDeferred    = treedbdb.CompactStoragePhaseStatusDeferred
	CompactStoragePhaseStatusUnsupported = treedbdb.CompactStoragePhaseStatusUnsupported
	CompactStoragePhaseStatusFailed      = treedbdb.CompactStoragePhaseStatusFailed
)

// CompactStorageAuditStats records shared reachability work and reuse decisions.
type CompactStorageAuditStats = treedbdb.CompactStorageAuditStats

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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		db.bgVac.endDeferredVectorBuild()
		return out, err
	}
	if err := lockFullScanMaintenanceContext(ctx, &db.bgVac.runMu); err != nil {
		return out, err
	}
	defer db.bgVac.runMu.Unlock()
	db.bgVac.endDeferredVectorBuild()
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
		reclaimStats, err := db.cached.ReclaimObservedValueLogSources(ctx, stats.ValueLogRewrite.SourceFileIDsUnreferenced)
		if err != nil {
			return out, err
		}
		stats.ValueLogRewrite.SourceSegmentsReclaimed += reclaimStats.ObservedSourceSegmentsDeleted
		stats.ValueLogRewrite.SourceBytesReclaimed += reclaimStats.ObservedSourceBytesDeleted
	}
	success = true
	db.bgVac.deferredVectorBuildDebt.Store(false)
	return CompactStorageStats(stats), nil
}

// CompactStorageLeafPageLogOwnerClassification reports the backend leaf-log
// owner classification currently visible to CompactStorage. The lifecycle
// argument is a modeled contract category, not runtime proof of quiescence.
func (db *DB) CompactStorageLeafPageLogOwnerClassification(lifecycle CompactStorageLifecycleState) CompactStorageLeafPageLogOwnerClassification {
	if db == nil || db.backend == nil {
		return treedbdb.CompactStorageLeafPageLogOwnerClassification{}
	}
	return db.backend.CompactStorageLeafPageLogOwnerClassification(lifecycle)
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
	explicitProtectedSystemRootIDs := append([]uint64(nil), opts.LeafGenerationProtectedSystemRootIDs...)
	userProtectedSystemRootIDsFunc := opts.LeafGenerationProtectedSystemRootIDsFunc
	userProtectedRootIDPairFunc := opts.LeafGenerationProtectedRootIDPairFunc
	opts.ValueLogProtectedPathsFunc = func() []string {
		var out []string
		if userProtectedPathsFunc != nil {
			out = appendCompactStorageProtectedPaths(out, userProtectedPathsFunc())
		}
		out = appendCompactStorageProtectedPaths(out, db.cached.ValueLogProtectedPaths())
		return out
	}
	opts.LeafGenerationProtectedRootIDPairFunc = func() ([]uint64, []uint64) {
		var rootIDs []uint64
		var systemRootIDs []uint64
		if userProtectedRootIDPairFunc != nil {
			userRootIDs, userSystemRootIDs := userProtectedRootIDPairFunc()
			rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, userRootIDs)
			systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, userSystemRootIDs)
		}
		cachedRootIDs, cachedSystemRootIDs := db.cached.ProtectedLeafGenerationRootIDPair()
		rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, cachedRootIDs)
		systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, cachedSystemRootIDs)
		return rootIDs, systemRootIDs
	}
	opts.LeafGenerationProtectedRootIDsFunc = userProtectedRootIDsFunc
	opts.LeafGenerationProtectedSystemRootIDsFunc = userProtectedSystemRootIDsFunc
	opts.ValueLogProtectedPaths = explicitProtectedPaths
	opts.LeafGenerationProtectedRootIDs = explicitProtectedRootIDs
	opts.LeafGenerationProtectedSystemRootIDs = explicitProtectedSystemRootIDs
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

func mergeCompactStorageProtectedRootIDs(dst []uint64, src []uint64) []uint64 {
	out := append([]uint64(nil), dst...)
	return appendCompactStorageProtectedRootIDs(out, src)
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
