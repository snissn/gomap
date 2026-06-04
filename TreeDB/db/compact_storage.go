package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// CompactStorageMode selects how aggressively CompactStorage should reclaim
// storage. Empty/default currently maps to full storage compaction.
type CompactStorageMode string

const (
	CompactStorageDefault    CompactStorageMode = ""
	CompactStorageFull       CompactStorageMode = "full"
	CompactStorageQuick      CompactStorageMode = "quick"
	CompactStorageExhaustive CompactStorageMode = "exhaustive"
)

// CompactStorageOptions controls full storage compaction across TreeDB storage
// domains. Prefer this high-level API over manually sequencing value-log
// rewrite, value-log GC, leaf-generation pack/GC, and index vacuum.
type CompactStorageOptions struct {
	Mode CompactStorageMode

	// DryRun reports the current compaction plan without mutating storage.
	DryRun bool

	// SyncEachPhase asks rewrite/pack phases to fsync each durable batch.
	SyncEachPhase bool

	// Value-log rewrite knobs.
	ValueLogRewriteBatchSize       int
	ValueLogRewriteMaxSegmentBytes int64
	ValueLogProtectedPaths         []string
	// ValueLogProtectedPathsFunc refreshes online protected paths before each
	// value-log rewrite/GC audit or applied phase. Live cached-mode callers use
	// this so foreground writer rotations that happen during a multi-phase
	// compaction cannot leave newly queued value-log segments unprotected.
	ValueLogProtectedPathsFunc func() []string
	// ValueLogFencedProtectedPathsFunc refreshes paths that remain unsafe for
	// fenced observed-only reclaim after the caller has performed its write
	// fence. Cached callers use this to protect in-use writer paths without
	// preserving retained paths that are independently proven unreachable.
	ValueLogFencedProtectedPathsFunc func() []string

	// Leaf-generation pack knobs. Defaults are intentionally bounded to keep a
	// single compaction request finite while still draining ordinary debt.
	LeafGenerationProtectedRootIDs []uint64
	// LeafGenerationProtectedSystemRootIDs are system roots whose collection
	// descriptors should be expanded into additional protected leaf-generation
	// roots during compaction.
	LeafGenerationProtectedSystemRootIDs []uint64
	// LeafGenerationProtectedRootIDsFunc refreshes additional leaf-generation
	// roots before each leaf plan/pack/GC phase. Cached/native interop callers
	// use this so roots published outside backend meta roots keep their leaf-log
	// children live during compaction.
	LeafGenerationProtectedRootIDsFunc func() []uint64
	// LeafGenerationProtectedSystemRootIDsFunc refreshes additional system roots
	// whose collection root descriptors should be expanded during compaction.
	LeafGenerationProtectedSystemRootIDsFunc func() []uint64
	// LeafGenerationProtectedRootIDPairFunc refreshes ordinary and system roots
	// from one caller snapshot when both lists come from the same source.
	LeafGenerationProtectedRootIDPairFunc func() (rootIDs []uint64, systemRootIDs []uint64)
	LeafPackMaxPasses                     int
	LeafPackMaxGenerationsPerPass         int
	LeafPackMaxBytesToCopyPerPass         int64
	LeafPackMinExpectedReclaimBytes       int64
	LeafPackMinExpectedReclaimRatioPPM    int
	LeafPackMinReclaimPerCopyPPM          int
	// LeafPackForce bypasses leaf-pack admission thresholds. It is enabled by
	// CompactStorageExhaustive so benchmark/VACUUM-equivalent compaction can
	// drain low-yield physical debt instead of stopping at production policy
	// thresholds.
	LeafPackForce      bool
	LeafPackLeafFrameK int

	// ReserveRIDs lets cached-mode callers share the live RID allocator with
	// foreground writers.
	ReserveRIDs func(count int) (start uint64, err error)

	// UnsafeValueLogReclaimFencedUnreferenced permits CompactStorage to reclaim
	// unreferenced value-log segments that cached retained/current-writer
	// protection would otherwise hide. Callers must first checkpoint, fence
	// writers, rotate away from any candidate writer segment, and refresh the
	// value-log set. Backend-only callers should leave it disabled.
	UnsafeValueLogReclaimFencedUnreferenced bool

	// DisableZeroByteValueLogCleanup leaves zero-byte value_vlog segment files in
	// place. This is intended for specialty diagnostics that need to inspect
	// empty segment files after compaction.
	DisableZeroByteValueLogCleanup bool
}

// CompactStorageUsage summarizes file usage for a storage domain.
type CompactStorageUsage struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	Bytes         int64  `json:"bytes"`
	Files         int    `json:"files"`
	ZeroByteFiles int    `json:"zero_byte_files"`
}

// CompactStorageDebt summarizes remaining work after planning or compaction.
type CompactStorageDebt struct {
	ValueLogRewriteSegments int   `json:"value_log_rewrite_segments"`
	ValueLogRewriteBytes    int64 `json:"value_log_rewrite_bytes"`
	ValueLogGCSegments      int   `json:"value_log_gc_segments"`
	ValueLogGCBytes         int64 `json:"value_log_gc_bytes"`
	LeafPackGenerations     int   `json:"leaf_pack_generations"`
	LeafPackBytes           int64 `json:"leaf_pack_bytes"`
	LeafGCGenerations       int   `json:"leaf_gc_generations"`
	LeafGCBytes             int64 `json:"leaf_gc_bytes"`
	ZeroByteValueLogFiles   int   `json:"zero_byte_value_log_files"`
}

// Empty reports whether the storage audit found no meaningful compaction debt.
func (d CompactStorageDebt) Empty() bool {
	return d.ValueLogRewriteSegments == 0 &&
		d.ValueLogRewriteBytes == 0 &&
		d.ValueLogGCSegments == 0 &&
		d.ValueLogGCBytes == 0 &&
		d.LeafPackGenerations == 0 &&
		d.LeafPackBytes == 0 &&
		d.LeafGCGenerations == 0 &&
		d.LeafGCBytes == 0 &&
		d.ZeroByteValueLogFiles == 0
}

// CompactStoragePhaseStats records one phase in a full compaction run.
type CompactStoragePhaseStats struct {
	Name          string `json:"name"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkipReason    string `json:"skip_reason,omitempty"`
	WallTimeNanos int64  `json:"wall_time_nanos"`
}

// CompactStorageStats is the single high-level report for TreeDB storage
// compaction and planning.
type CompactStorageStats struct {
	Mode   CompactStorageMode `json:"mode"`
	DryRun bool               `json:"dry_run"`

	Before []CompactStorageUsage `json:"before"`
	After  []CompactStorageUsage `json:"after"`

	Phases []CompactStoragePhaseStats `json:"phases,omitempty"`

	ValueLogRewritePlan ValueLogRewritePlan              `json:"value_log_rewrite_plan"`
	ValueLogRewrite     ValueLogRewriteStats             `json:"value_log_rewrite,omitempty"`
	ValueLogGC          ValueLogGCStats                  `json:"value_log_gc"`
	LeafGenerationPlan  LeafGenerationPlan               `json:"leaf_generation_plan"`
	LeafGenerationPacks []LeafGenerationPackRunOnceStats `json:"leaf_generation_packs,omitempty"`
	LeafGenerationGC    LeafGenerationGCStats            `json:"leaf_generation_gc"`

	ZeroByteValueLogFilesDeleted int `json:"zero_byte_value_log_files_deleted,omitempty"`

	RemainingDebt CompactStorageDebt `json:"remaining_debt"`

	// FullyCompacted is the legacy policy-oriented compacted flag. For
	// exhaustive mode, ByteMinimized reports the stricter benchmark contract.
	FullyCompacted       bool `json:"fully_compacted"`
	PolicyFullyCompacted bool `json:"policy_fully_compacted"`
	ByteMinimized        bool `json:"byte_minimized"`
}

// CompactStoragePlan reports full storage compaction debt without mutating the
// database. It is safe for read-only opens.
func (db *DB) CompactStoragePlan(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	opts.DryRun = true
	return db.compactStorage(ctx, opts)
}

// CompactStorage runs the recommended full storage compaction sequence:
// value-log rewrite, value-log GC, leaf-generation pack, leaf-generation GC,
// index vacuum, final GC settle passes, empty value-log file cleanup, and a
// final audit.
func (db *DB) CompactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	opts.DryRun = false
	return db.compactStorage(ctx, opts)
}

func (db *DB) compactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	var stats CompactStorageStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeCompactStorageOptions(opts)
	stats.Mode = opts.Mode
	stats.DryRun = opts.DryRun
	if db.readOnly && !opts.DryRun {
		return stats, ErrReadOnly
	}
	if !opts.DryRun {
		if err := db.commandWALPoisonedError(); err != nil {
			return stats, err
		}
	}
	maintenanceLocked := false
	if !opts.DryRun {
		db.maintenanceMu.Lock()
		maintenanceLocked = true
		defer db.maintenanceMu.Unlock()
	}

	before, err := compactStorageUsage(db.dir)
	if err != nil {
		return stats, err
	}
	stats.Before = before

	initialDebt, err := db.populateCompactStorageAudit(ctx, opts, &stats, !maintenanceLocked, nil)
	if err != nil {
		return stats, err
	}
	stats.RemainingDebt = initialDebt
	if opts.DryRun {
		stats.After = before
		stats.FullyCompacted = initialDebt.Empty()
		stats.PolicyFullyCompacted = stats.FullyCompacted
		stats.ByteMinimized = false
		return stats, nil
	}

	if err := db.runCompactStoragePhase(&stats, "checkpoint", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}
	if err := db.prepareCompactStorageRIDAllocator(&opts); err != nil {
		return stats, err
	}

	compactLeafLog, cleanupLeafLog, err := db.installCompactStorageLeafPageLog(opts)
	if err != nil {
		return stats, err
	}
	defer cleanupLeafLog()
	if opts.Mode == CompactStorageExhaustive && db.indexOuterLeavesInValueLog && compactLeafLog == nil && db.leafPageLog != nil {
		return stats, fmt.Errorf("treedb: exhaustive compact requires an internally-owned leaf page log; close or clear the installed leaf page log before compacting")
	}
	if err := db.runCompactStoragePhase(&stats, "value-log-rewrite", func() error {
		protectedPaths := compactStorageOnlineRewriteProtectedPaths(opts)
		protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
		rewriteOpts := compactStorageRewritePlanOptions(protectedPaths)
		rewriteOpts.LeafGenerationProtectedRootIDs = protectedRootIDs
		rewriteOpts.LeafGenerationProtectedSystemRootIDs = protectedSystemRootIDs
		rewriteOpts.BatchSize = opts.ValueLogRewriteBatchSize
		rewriteOpts.SyncEachBatch = opts.SyncEachPhase
		rewriteOpts.MaxSegmentBytes = opts.ValueLogRewriteMaxSegmentBytes
		rewriteOpts.LocalityPolicy = ValueLogRewriteLocalityGrouped
		rewriteOpts.ReserveRIDs = opts.ReserveRIDs
		rewrite, err := db.valueLogRewriteOnline(ctx, rewriteOpts, !maintenanceLocked)
		stats.ValueLogRewrite = rewrite
		return err
	}); err != nil {
		return stats, err
	}
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-value-log-rewrite", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}

	if err := db.runCompactStoragePhase(&stats, "value-log-gc", func() error {
		gc, err := db.valueLogGC(ctx, ValueLogGCOptions{ProtectedPaths: compactStorageValueLogProtectedPaths(opts)}, !maintenanceLocked)
		stats.ValueLogGC = gc
		return err
	}); err != nil {
		return stats, err
	}
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-value-log-gc", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}

	if opts.Mode == CompactStorageExhaustive {
		sealedCurrent := false
		if err := db.runCompactStoragePhase(&stats, "seal-current-leaf-generation", func() error {
			var err error
			sealedCurrent, err = db.sealCompactStorageCurrentLeafGeneration(compactLeafLog)
			return err
		}); err != nil {
			return stats, err
		}
		if !sealedCurrent && len(stats.Phases) > 0 {
			stats.Phases[len(stats.Phases)-1].Skipped = true
			stats.Phases[len(stats.Phases)-1].SkipReason = "no current leaf generation files"
		}
	}

	for pass := 0; pass < opts.LeafPackMaxPasses; pass++ {
		var pack LeafGenerationPackRunOnceStats
		phaseName := fmt.Sprintf("leaf-generation-pack-%d", pass+1)
		if err := db.runCompactStoragePhase(&stats, phaseName, func() error {
			var err error
			protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
			pack, err = db.leafGenerationPackRunOnce(ctx, compactStorageLeafPackFromPlanOptions(opts, protectedRootIDs, protectedSystemRootIDs), !maintenanceLocked)
			return err
		}); err != nil {
			return stats, err
		}
		stats.LeafGenerationPacks = append(stats.LeafGenerationPacks, pack)
		if !pack.Ran {
			if len(stats.Phases) > 0 {
				stats.Phases[len(stats.Phases)-1].Skipped = true
				stats.Phases[len(stats.Phases)-1].SkipReason = pack.SkipReason
			}
			break
		}
		if err := db.refreshCompactStorageLeafPageLog(compactLeafLog); err != nil {
			return stats, err
		}
		if err := db.runCompactStoragePhase(&stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
			return db.Checkpoint()
		}); err != nil {
			return stats, err
		}
	}

	if err := db.runCompactStoragePhase(&stats, "leaf-generation-gc", func() error {
		protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
		gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
			ProtectedRootIDs:       protectedRootIDs,
			ProtectedSystemRootIDs: protectedSystemRootIDs,
		}, !maintenanceLocked)
		stats.LeafGenerationGC = gc
		return err
	}); err != nil {
		return stats, err
	}
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-leaf-generation-gc", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}

	indexVacuumSkipped := false
	if err := db.runCompactStoragePhase(&stats, "index-vacuum", func() error {
		indexVacuumSkipped = false
		err := db.vacuumIndexOnline(ctx, !maintenanceLocked)
		if errors.Is(err, ErrVacuumUnsupported) {
			indexVacuumSkipped = true
			return nil
		}
		return err
	}); err != nil {
		return stats, err
	}
	if indexVacuumSkipped {
		if len(stats.Phases) > 0 {
			stats.Phases[len(stats.Phases)-1].Skipped = true
			stats.Phases[len(stats.Phases)-1].SkipReason = ErrVacuumUnsupported.Error()
		}
	} else if err := db.runCompactStoragePhase(&stats, "checkpoint-after-index-vacuum", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}

	if err := db.settleCompactStorageGC(ctx, opts, &stats, !maintenanceLocked); err != nil {
		return stats, err
	}

	if opts.DisableZeroByteValueLogCleanup {
		stats.Phases = append(stats.Phases, CompactStoragePhaseStats{
			Name:       "prune-empty-value-log-files",
			Skipped:    true,
			SkipReason: "disabled",
		})
	} else {
		if err := db.runCompactStoragePhase(&stats, "prune-empty-value-log-files", func() error {
			deleted, err := db.pruneZeroByteValueLogFiles(compactStorageCleanupValueLogProtectedPaths(opts))
			stats.ZeroByteValueLogFilesDeleted = deleted
			return err
		}); err != nil {
			return stats, err
		}
	}

	after, err := compactStorageUsage(db.dir)
	if err != nil {
		return stats, err
	}
	stats.After = after

	var finalAudit CompactStorageStats
	finalDebt, err := db.populateCompactStorageAudit(ctx, opts, &finalAudit, !maintenanceLocked, nil)
	if err != nil {
		return stats, err
	}
	stats.ValueLogRewritePlan = finalAudit.ValueLogRewritePlan
	stats.LeafGenerationPlan = finalAudit.LeafGenerationPlan
	stats.RemainingDebt = finalDebt
	stats.FullyCompacted = finalDebt.Empty()
	stats.PolicyFullyCompacted = stats.FullyCompacted
	stats.ByteMinimized = opts.Mode == CompactStorageExhaustive && finalDebt.Empty()
	return stats, nil
}

func (db *DB) sealCompactStorageCurrentLeafGeneration(compactLeafLog *rewriteWriter) (bool, error) {
	if db == nil || db.leafGenerationManifest == nil {
		return false, nil
	}
	commitSeq := uint64(1)
	if state := db.State(); state != nil && state.CommitSeq != 0 {
		commitSeq = state.CommitSeq
	}
	db.mu.Lock()
	if db.leafGenerationManifest == nil {
		db.mu.Unlock()
		return false, nil
	}
	nextManifest := db.leafGenerationManifest.clone()
	db.mu.Unlock()

	rawFileIDs, changed, err := nextManifest.sealCurrentGeneration(commitSeq)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}

	if compactLeafLog != nil {
		if _, currentFileID, ok := compactLeafLog.CurrentValueLogSegment(); ok {
			if currentRawFileID, ok := rawLeafGenerationFileID(currentFileID); ok {
				for _, sealedRawFileID := range rawFileIDs {
					if sealedRawFileID == currentRawFileID {
						if err := compactLeafLog.rotateLeaf(); err != nil {
							return false, err
						}
						if db.valueLogManager != nil {
							if rotatedPath, rotatedFileID, ok := compactLeafLog.CurrentValueLogSegment(); ok {
								if err := db.valueLogManager.RegisterSegment(rotatedPath, rotatedFileID); err != nil {
									return false, err
								}
							}
						}
						break
					}
				}
			}
		}
	}

	if err := db.persistLeafGenerationManifestAndRecordLengthIndexes(nextManifest, rawFileIDs); err != nil {
		return false, err
	}
	db.mu.Lock()
	db.leafGenerationManifest = nextManifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) settleCompactStorageGC(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats, lockMaintenance bool) error {
	const maxSettlePasses = 4
	for pass := 0; pass < maxSettlePasses; pass++ {
		var audit CompactStorageStats
		var fencedIDs []uint32
		debt, err := db.populateCompactStorageAudit(ctx, opts, &audit, lockMaintenance, &fencedIDs)
		if err != nil {
			return err
		}
		if debt.ValueLogGCSegments == 0 && debt.LeafGCGenerations == 0 && len(fencedIDs) == 0 {
			return nil
		}
		if debt.ValueLogGCSegments > 0 {
			phaseName := fmt.Sprintf("settle-value-log-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				gc, err := db.valueLogGC(ctx, ValueLogGCOptions{ProtectedPaths: compactStorageValueLogProtectedPaths(opts)}, lockMaintenance)
				stats.ValueLogGC = gc
				return err
			}); err != nil {
				return err
			}
			if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
				return db.Checkpoint()
			}); err != nil {
				return err
			}
		}
		if len(fencedIDs) > 0 {
			phaseName := fmt.Sprintf("settle-fenced-value-log-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				// Fenced IDs are independently proven unreachable by a fresh
				// root scan below. Cached callers rotate and block writers
				// before enabling this path, so ReclaimActive can collect the
				// formerly-active files that would otherwise reappear as debt
				// after close/reopen.
				gc, err := db.valueLogGC(ctx, ValueLogGCOptions{
					ObservedSourceFileIDs:            fencedIDs,
					ObservedSourceAssumeUnreferenced: true,
					ObservedSourceReclaimActive:      true,
					ProtectedPaths:                   compactStorageFencedValueLogProtectedPaths(opts),
				}, lockMaintenance)
				stats.ValueLogGC = gc
				return err
			}); err != nil {
				return err
			}
			if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
				return db.Checkpoint()
			}); err != nil {
				return err
			}
		}
		if debt.LeafGCGenerations > 0 {
			phaseName := fmt.Sprintf("settle-leaf-generation-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
				gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
					ProtectedRootIDs:       protectedRootIDs,
					ProtectedSystemRootIDs: protectedSystemRootIDs,
				}, lockMaintenance)
				stats.LeafGenerationGC = gc
				return err
			}); err != nil {
				return err
			}
			if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
				return db.Checkpoint()
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) prepareCompactStorageRIDAllocator(opts *CompactStorageOptions) error {
	if db == nil || opts == nil || opts.ReserveRIDs != nil || !db.indexOuterLeavesInValueLog {
		return nil
	}
	segments, err := rewriteWALSegmentsLister(db.dir)
	if err != nil {
		return fmt.Errorf("list value-log segments for compact storage rid selection in %s: %w", db.dir, err)
	}
	nextRID, err := rewriteRIDStartScanner(segments)
	if err != nil {
		return fmt.Errorf("scan compact storage rid start in %s: %w", db.dir, err)
	}
	allocator := newRewriteRIDAllocator(nextRID, nil)
	opts.ReserveRIDs = allocator.Reserve
	return nil
}

func normalizeCompactStorageOptions(opts CompactStorageOptions) CompactStorageOptions {
	switch opts.Mode {
	case CompactStorageDefault:
		opts.Mode = CompactStorageFull
	case CompactStorageFull, CompactStorageQuick, CompactStorageExhaustive:
	default:
		opts.Mode = CompactStorageFull
	}
	if opts.ValueLogRewriteBatchSize < 0 {
		opts.ValueLogRewriteBatchSize = 0
	}
	if opts.Mode == CompactStorageExhaustive {
		opts.LeafPackForce = true
	}
	if opts.LeafPackMaxPasses <= 0 {
		switch opts.Mode {
		case CompactStorageQuick:
			opts.LeafPackMaxPasses = 1
		case CompactStorageExhaustive:
			opts.LeafPackMaxPasses = 256
		default:
			opts.LeafPackMaxPasses = 32
		}
	}
	if opts.LeafPackMaxGenerationsPerPass < 0 {
		opts.LeafPackMaxGenerationsPerPass = 0
	}
	if opts.LeafPackMaxGenerationsPerPass == 0 {
		switch opts.Mode {
		case CompactStorageQuick:
			opts.LeafPackMaxGenerationsPerPass = 8
		case CompactStorageExhaustive:
			opts.LeafPackMaxGenerationsPerPass = 0
		default:
			opts.LeafPackMaxGenerationsPerPass = 64
		}
	}
	if opts.LeafPackMaxBytesToCopyPerPass < 0 {
		opts.LeafPackMaxBytesToCopyPerPass = 0
	}
	if opts.LeafPackMaxBytesToCopyPerPass == 0 {
		switch opts.Mode {
		case CompactStorageQuick:
			opts.LeafPackMaxBytesToCopyPerPass = 256 << 20
		case CompactStorageExhaustive:
			opts.LeafPackMaxBytesToCopyPerPass = 0
		default:
			opts.LeafPackMaxBytesToCopyPerPass = 1 << 30
		}
	}
	return opts
}

func compactStorageRewritePlanOptions(protectedPaths []string) ValueLogRewriteOnlineOptions {
	return ValueLogRewriteOnlineOptions{
		MinSegmentStaleBytes: 1,
		ProtectedPaths:       protectedPaths,
	}
}

func compactStorageLeafPackFromPlanOptions(opts CompactStorageOptions, protectedRootIDs, protectedSystemRootIDs []uint64) LeafGenerationPackFromPlanOptions {
	out := LeafGenerationPackFromPlanOptions{
		Sync:                       opts.SyncEachPhase,
		Force:                      opts.LeafPackForce,
		MinExpectedReclaimBytes:    opts.LeafPackMinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.LeafPackMinExpectedReclaimRatioPPM,
		MinReclaimPerByteCopiedPPM: opts.LeafPackMinReclaimPerCopyPPM,
		MaxGenerations:             opts.LeafPackMaxGenerationsPerPass,
		MaxBytesToCopy:             opts.LeafPackMaxBytesToCopyPerPass,
		ReserveRIDs:                opts.ReserveRIDs,
		LeafFrameK:                 normalizeLeafGenerationPackLeafFrameK(opts.LeafPackLeafFrameK),
		ProtectedRootIDs:           protectedRootIDs,
		ProtectedSystemRootIDs:     protectedSystemRootIDs,
	}
	if !out.Force && out.MinExpectedReclaimBytes == 0 && out.MinExpectedReclaimRatioPPM == 0 && out.MinReclaimPerByteCopiedPPM == 0 {
		out.MinReclaimPerByteCopiedPPM = leafGenerationPackDefaultMinReclaimPerByteCopiedPPM
	}
	return out
}

func compactStorageLeafPackDebtFromPlan(plan LeafGenerationPlan, opts LeafGenerationPackFromPlanOptions) (int, int64, error) {
	if plan.Admission != leafGenerationPlanAdmissionEligible || len(plan.Candidates) == 0 {
		return 0, 0, nil
	}
	selection, err := SelectLeafGenerationPackCandidates(plan, leafGenerationPackFromPlanSelectOptions(opts))
	if err != nil {
		if compactStorageLeafPackSelectionErrorMeansNoDebt(err) {
			return 0, 0, nil
		}
		return len(plan.Candidates), plan.ExpectedReclaimBytes, nil
	}
	return len(selection.GenerationIDs), selection.ExpectedReclaimBytes, nil
}

func compactStorageLeafPackSelectionErrorMeansNoDebt(err error) bool {
	return errors.Is(err, errLeafGenerationPackSelectionThreshold)
}

func (db *DB) populateCompactStorageAudit(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats, lockMaintenance bool, fencedIDsOut *[]uint32) (CompactStorageDebt, error) {
	var debt CompactStorageDebt
	protectedPaths := compactStorageFencedValueLogProtectedPaths(opts)
	rewritePlan, err := db.ValueLogRewritePlan(ctx, compactStorageRewritePlanOptions(protectedPaths))
	if err != nil {
		return debt, err
	}
	stats.ValueLogRewritePlan = rewritePlan
	debt.ValueLogRewriteSegments = rewritePlan.SegmentsSelected
	debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesStale
	if debt.ValueLogRewriteBytes == 0 {
		debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesTotal
	}

	valueGC, err := db.valueLogGC(ctx, ValueLogGCOptions{DryRun: true, ProtectedPaths: protectedPaths}, lockMaintenance)
	if err != nil {
		return debt, err
	}
	stats.ValueLogGC = valueGC
	debt.ValueLogGCSegments = valueGC.SegmentsEligible
	debt.ValueLogGCBytes = valueGC.BytesEligible
	fencedValueLogIDs, fencedValueLogBytes, err := db.compactStorageFencedUnreferencedValueLogIDs(ctx, opts)
	if err != nil {
		return debt, err
	}
	if fencedIDsOut != nil {
		*fencedIDsOut = append((*fencedIDsOut)[:0], fencedValueLogIDs...)
	}
	debt.ValueLogGCSegments += len(fencedValueLogIDs)
	debt.ValueLogGCBytes += fencedValueLogBytes

	protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
	leafPackOpts := compactStorageLeafPackFromPlanOptions(opts, protectedRootIDs, protectedSystemRootIDs)
	leafPlan, err := db.LeafGenerationPlan(ctx, leafGenerationPackFromPlanPlanOptions(leafPackOpts))
	if err != nil {
		return debt, err
	}
	stats.LeafGenerationPlan = leafPlan
	leafPackGenerations, leafPackBytes, err := compactStorageLeafPackDebtFromPlan(leafPlan, leafPackOpts)
	if err != nil {
		return debt, err
	}
	debt.LeafPackGenerations = leafPackGenerations
	debt.LeafPackBytes = leafPackBytes

	protectedRootIDs, protectedSystemRootIDs = db.compactStorageLeafGenerationProtectedRootIDPair(opts)
	leafGC, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
		DryRun:                 true,
		ProtectedRootIDs:       protectedRootIDs,
		ProtectedSystemRootIDs: protectedSystemRootIDs,
	}, lockMaintenance)
	if err != nil {
		return debt, err
	}
	stats.LeafGenerationGC = leafGC
	debt.LeafGCGenerations = leafGC.GenerationsEligible
	debt.LeafGCBytes = leafGC.BytesEligible

	usage, err := compactStorageUsage(db.dir)
	if err != nil {
		return debt, err
	}
	if !opts.DisableZeroByteValueLogCleanup {
		currentPaths, currentIDs := db.currentValueLogProtectedRefs()
		allProtectedPaths := mergeUniqueNonEmptyPaths(protectedPaths, currentPaths)
		zeroBytes, err := zeroByteValueLogFilesFromUsage(usage, allProtectedPaths, currentIDs)
		if err != nil {
			return debt, err
		}
		debt.ZeroByteValueLogFiles = zeroBytes
	}
	return debt, nil
}

func (db *DB) compactStorageFencedUnreferencedValueLogIDs(ctx context.Context, opts CompactStorageOptions) ([]uint32, int64, error) {
	if db == nil || !opts.UnsafeValueLogReclaimFencedUnreferenced || db.valueLogManager == nil {
		return nil, 0, nil
	}
	referenced, err := db.compactStorageScannedValueLogRefs(ctx)
	if err != nil {
		return nil, 0, err
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return nil, 0, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		return nil, 0, nil
	}
	defer func() { _ = db.valueLogManager.Release(set) }()

	files := db.valueOnlyValueLogFiles(set.Files)
	protectedPaths := compactStorageFencedValueLogProtectedPaths(opts)
	protected := compactStorageProtectedPathSet(protectedPaths)
	protectedFileIDs := compactStorageProtectedFileIDSet(protectedPaths, nil)
	ids := make([]uint32, 0, len(files))
	var bytes int64
	for id, f := range files {
		if _, ok := referenced[id]; ok {
			continue
		}
		if f == nil {
			continue
		}
		if _, ok := protected[filepath.Clean(f.Path)]; ok {
			continue
		}
		if _, ok := protectedFileIDs[id]; ok {
			continue
		}
		size := fileSize(f)
		if size <= 0 {
			continue
		}
		ids = append(ids, id)
		bytes += size
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids, bytes, nil
}

func (db *DB) compactStorageScannedValueLogRefs(ctx context.Context) (map[uint32]struct{}, error) {
	counts, _, err := db.scanValueLogRefCounts(ctx)
	if err != nil && errors.Is(err, valuelog.ErrFileNotFound) {
		if refreshErr := db.RefreshValueLogSet(); refreshErr != nil {
			return nil, refreshErr
		}
		counts, _, err = db.scanValueLogRefCounts(ctx)
	}
	if err != nil {
		return nil, err
	}
	refs := make(map[uint32]struct{}, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		refs[fileID] = struct{}{}
	}
	return refs, nil
}

func (db *DB) runCompactStoragePhase(stats *CompactStorageStats, name string, fn func() error) error {
	started := time.Now()
	err := fn()
	stats.Phases = append(stats.Phases, CompactStoragePhaseStats{
		Name:          name,
		WallTimeNanos: time.Since(started).Nanoseconds(),
	})
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if db != nil && db.compactStorageAfterPhase != nil {
		db.compactStorageAfterPhase(name)
	}
	return nil
}

func compactStorageUsage(dir string) ([]CompactStorageUsage, error) {
	layout := resolveStorageLayout(dir)
	domains := []struct {
		name string
		path string
	}{
		{name: "index", path: filepath.Join(dir, indexFileName)},
		{name: "wal", path: layout.walDir},
		{name: "value_vlog", path: layout.valueVLogDir},
		{name: "leaf_vlog", path: layout.leafVLogDir},
	}
	out := make([]CompactStorageUsage, 0, len(domains)+1)
	var total CompactStorageUsage
	total.Name = "total"
	total.Path = dir
	for _, domain := range domains {
		usage, err := compactStoragePathUsage(domain.name, domain.path)
		if err != nil {
			return nil, err
		}
		out = append(out, usage)
		total.Bytes += usage.Bytes
		total.Files += usage.Files
		total.ZeroByteFiles += usage.ZeroByteFiles
	}
	out = append(out, total)
	return out, nil
}

func compactStoragePathUsage(name, path string) (CompactStorageUsage, error) {
	usage := CompactStorageUsage{Name: name, Path: path}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return usage, nil
		}
		return usage, err
	}
	if !info.IsDir() {
		usage.Files = 1
		usage.Bytes = info.Size()
		if info.Size() == 0 {
			usage.ZeroByteFiles = 1
		}
		return usage, nil
	}
	err = filepath.WalkDir(path, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		usage.Files++
		usage.Bytes += info.Size()
		if info.Size() == 0 {
			usage.ZeroByteFiles++
		}
		return nil
	})
	return usage, err
}

func zeroByteValueLogFilesFromUsage(usage []CompactStorageUsage, protectedPaths []string, protectedFileIDs []uint32) (int, error) {
	for _, domain := range usage {
		if domain.Name == "value_vlog" {
			return zeroByteValueLogSegmentFiles(domain.Path, protectedPaths, protectedFileIDs)
		}
	}
	return 0, nil
}

func compactStorageValueLogProtectedPaths(opts CompactStorageOptions) []string {
	if opts.ValueLogProtectedPathsFunc == nil {
		if len(opts.ValueLogProtectedPaths) == 0 {
			return nil
		}
		return opts.ValueLogProtectedPaths
	}
	dynamic := opts.ValueLogProtectedPathsFunc()
	if len(opts.ValueLogProtectedPaths) == 0 {
		if len(dynamic) == 0 {
			return nil
		}
		return dynamic
	}
	if len(dynamic) == 0 {
		return opts.ValueLogProtectedPaths
	}
	seen := make(map[string]struct{}, len(opts.ValueLogProtectedPaths)+len(dynamic))
	out := make([]string, 0, len(opts.ValueLogProtectedPaths)+len(dynamic))
	for _, path := range opts.ValueLogProtectedPaths {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	for _, path := range dynamic {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

func (db *DB) compactStorageLeafGenerationProtectedRootIDPair(opts CompactStorageOptions) ([]uint64, []uint64) {
	var rootIDs []uint64
	var systemRootIDs []uint64
	rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, opts.LeafGenerationProtectedRootIDs)
	systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, opts.LeafGenerationProtectedSystemRootIDs)
	if opts.LeafGenerationProtectedRootIDsFunc != nil {
		rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, opts.LeafGenerationProtectedRootIDsFunc())
	}
	if opts.LeafGenerationProtectedSystemRootIDsFunc != nil {
		systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, opts.LeafGenerationProtectedSystemRootIDsFunc())
	}
	if opts.LeafGenerationProtectedRootIDPairFunc != nil {
		dynamicRootIDs, dynamicSystemRootIDs := opts.LeafGenerationProtectedRootIDPairFunc()
		rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, dynamicRootIDs)
		systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, dynamicSystemRootIDs)
	}
	dynamicRootIDs, dynamicSystemRootIDs := db.protectedLeafGenerationRootIDPairFromLeafPageLog()
	rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, dynamicRootIDs)
	systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, dynamicSystemRootIDs)
	return rootIDs, systemRootIDs
}

func appendCompactStorageProtectedRootIDs(dst []uint64, src []uint64) []uint64 {
	if len(src) == 0 {
		return dst
	}
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

func compactStorageOnlineRewriteProtectedPaths(opts CompactStorageOptions) []string {
	protectedPaths := compactStorageValueLogProtectedPaths(opts)
	if len(protectedPaths) == 0 {
		// Sentinel keeps active-segment protection enabled for live online
		// rewrite even when no concrete protected paths are currently known.
		return []string{""}
	}
	return protectedPaths
}

func compactStorageFencedValueLogProtectedPaths(opts CompactStorageOptions) []string {
	if opts.ValueLogFencedProtectedPathsFunc == nil {
		return compactStorageValueLogProtectedPaths(opts)
	}
	dynamic := opts.ValueLogFencedProtectedPathsFunc()
	if len(opts.ValueLogProtectedPaths) > 0 {
		if len(dynamic) == 0 {
			return opts.ValueLogProtectedPaths
		}
		return compactStorageMergeProtectedPaths(opts.ValueLogProtectedPaths, dynamic)
	}
	if len(dynamic) == 0 {
		return nil
	}
	return dynamic
}

func compactStorageMergeProtectedPaths(static, dynamic []string) []string {
	seen := make(map[string]struct{}, len(static)+len(dynamic))
	out := make([]string, 0, len(static)+len(dynamic))
	for _, path := range static {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	for _, path := range dynamic {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

func compactStorageCleanupValueLogProtectedPaths(opts CompactStorageOptions) []string {
	if opts.UnsafeValueLogReclaimFencedUnreferenced {
		return compactStorageFencedValueLogProtectedPaths(opts)
	}
	return compactStorageValueLogProtectedPaths(opts)
}

func (db *DB) pruneZeroByteValueLogFiles(protectedPaths []string) (int, error) {
	layout := resolveStorageLayout(db.dir)
	entries, err := os.ReadDir(layout.valueVLogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	currentPaths, currentIDs := db.currentValueLogProtectedRefs()
	protectedPaths = mergeUniqueNonEmptyPaths(protectedPaths, currentPaths)
	protected := compactStorageProtectedPathSet(protectedPaths)
	protectedFileIDs := compactStorageProtectedFileIDSet(protectedPaths, currentIDs)
	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !compactStorageIsValueLogSegmentName(name) {
			continue
		}
		path := filepath.Join(layout.valueVLogDir, name)
		if _, ok := protected[filepath.Clean(path)]; ok {
			continue
		}
		if fileID, ok := compactStorageValueLogFileID(name); ok {
			if _, ok := protectedFileIDs[fileID]; ok {
				continue
			}
		}
		info, err := entry.Info()
		if err != nil {
			return deleted, err
		}
		if info.Size() != 0 {
			continue
		}
		if db.valueLogManager != nil {
			if fileID, ok := compactStorageValueLogFileID(name); ok {
				tracked, _, err := db.valueLogManager.MarkZombieIfTracked(fileID)
				if err != nil {
					return deleted, err
				}
				if tracked {
					if err := db.publishValueLogSetNoRefresh(); err != nil {
						return deleted, err
					}
					if removed, err := db.valueLogManager.RemoveSegmentIfUnpinned(fileID); err != nil {
						return deleted, err
					} else if removed {
						deleted++
						continue
					}
					if _, err := os.Stat(path); err != nil {
						if os.IsNotExist(err) {
							deleted++
							continue
						}
						return deleted, err
					}
					continue
				}
			}
		}
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			if compactStorageIsBusyRemoveError(err) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	if deleted > 0 && db.valueLogManager != nil {
		if err := db.valueLogManager.Refresh(); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func (db *DB) currentValueLogProtectedRefs() ([]string, []uint32) {
	if db == nil {
		return nil, nil
	}
	var fileIDs []uint32
	if db.valueLogManager != nil {
		fileIDs = append(fileIDs, db.valueLogManager.CurrentWritableFileIDs()...)
	}
	appender := db.currentValueLogAppender()
	if appender == nil {
		return nil, fileIDs
	}
	path, fileID, ok := appender.CurrentValueLogSegment()
	var paths []string
	if ok && path != "" {
		paths = []string{path}
	}
	if ok && fileID != 0 {
		fileIDs = append(fileIDs, fileID)
	}
	return paths, fileIDs
}

func zeroByteValueLogSegmentFiles(dir string, protectedPaths []string, protectedFileIDs []uint32) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	protected := compactStorageProtectedPathSet(protectedPaths)
	protectedIDs := compactStorageProtectedFileIDSet(protectedPaths, protectedFileIDs)
	count := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !compactStorageIsValueLogSegmentName(name) {
			continue
		}
		if fileID, ok := compactStorageValueLogFileID(name); ok {
			if _, ok := protectedIDs[fileID]; ok {
				continue
			}
		}
		path := filepath.Join(dir, name)
		if _, ok := protected[filepath.Clean(path)]; ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		if info.Size() == 0 {
			count++
		}
	}
	return count, nil
}

func compactStorageProtectedPathSet(paths []string) map[string]struct{} {
	protected := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		protected[filepath.Clean(path)] = struct{}{}
	}
	return protected
}

func compactStorageProtectedFileIDSet(paths []string, fileIDs []uint32) map[uint32]struct{} {
	protected := make(map[uint32]struct{}, len(paths)+len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID == 0 {
			continue
		}
		protected[fileID] = struct{}{}
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		if fileID, ok := compactStorageValueLogFileID(filepath.Base(path)); ok {
			protected[fileID] = struct{}{}
		}
	}
	return protected
}

func compactStorageIsValueLogSegmentName(name string) bool {
	if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
		return false
	}
	_, ok := compactStorageValueLogFileID(name)
	return ok
}

func compactStorageValueLogFileID(name string) (uint32, bool) {
	lane, seq, valueLog, ok := parseLogSeq(name)
	if !ok || !valueLog || lane < 0 {
		return 0, false
	}
	if uint64(lane) > uint64(^uint32(0)) || seq > uint64(^uint32(0)) {
		return 0, false
	}
	fileID, err := valuelog.EncodeFileID(uint32(lane), uint32(seq))
	if err != nil {
		return 0, false
	}
	return fileID, true
}

func (db *DB) installCompactStorageLeafPageLog(opts CompactStorageOptions) (*rewriteWriter, func(), error) {
	if db == nil || !db.indexOuterLeavesInValueLog || db.leafPageLog != nil {
		return nil, func() {}, nil
	}
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		return nil, nil, err
	}
	leafStartSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	writer := newRewriteWriter(ValueLogDirPath(db.dir), 0, 0, 0)
	writer.ConfigureLeafLog(LeafLogDirPath(db.dir), rewriteLeafLogLaneID, leafStartSeq)
	nextRID := uint64(0)
	if opts.ReserveRIDs == nil {
		nextRID, err = rewriteRIDStartScanner(segments)
		if err != nil {
			_ = writer.Close()
			return nil, nil, fmt.Errorf("scan rewrite rid start in %s: %w", db.dir, err)
		}
	}
	writer.ridAlloc = newRewriteRIDAllocator(nextRID, opts.ReserveRIDs)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	if writer.blockCompression {
		if state := db.State(); state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, err := prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if err != nil {
				_ = writer.Close()
				return nil, nil, err
			}
			if leafDictID != 0 && len(leafDictBytes) > 0 {
				writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			}
		}
	}
	db.SetLeafPageLog(writer)
	return writer, func() {
		db.SetLeafPageLog(nil)
		_ = writer.Close()
	}, nil
}

func (db *DB) refreshCompactStorageLeafPageLog(writer *rewriteWriter) error {
	if db == nil || writer == nil || writer.leafDir == "" {
		return nil
	}
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		return err
	}
	leafSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	return writer.resetLeafLogSeqAtLeast(leafSeq)
}
