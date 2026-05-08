package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// CompactStorageMode selects how aggressively CompactStorage should reclaim
// storage. Empty/default currently maps to full storage compaction.
type CompactStorageMode string

const (
	CompactStorageDefault CompactStorageMode = ""
	CompactStorageFull    CompactStorageMode = "full"
	CompactStorageQuick   CompactStorageMode = "quick"
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

	// Leaf-generation pack knobs. Defaults are intentionally bounded to keep a
	// single compaction request finite while still draining ordinary debt.
	LeafPackMaxPasses                  int
	LeafPackMaxGenerationsPerPass      int
	LeafPackMaxBytesToCopyPerPass      int64
	LeafPackMinExpectedReclaimBytes    int64
	LeafPackMinExpectedReclaimRatioPPM int
	LeafPackMinReclaimPerCopyPPM       int
	LeafPackLeafFrameK                 int

	// ReserveRIDs lets cached-mode callers share the live RID allocator with
	// foreground writers.
	ReserveRIDs func(count int) (start uint64, err error)

	// DisableZeroByteValueLogCleanup leaves zero-byte value_vlog segment files in
	// place. Live cached-mode wrappers use this because their current writer
	// files may be open even when empty.
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
	LeafGCSegments          int   `json:"leaf_gc_generations"`
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
		d.LeafGCSegments == 0 &&
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

	RemainingDebt  CompactStorageDebt `json:"remaining_debt"`
	FullyCompacted bool               `json:"fully_compacted"`
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

	before, err := compactStorageUsage(db.dir)
	if err != nil {
		return stats, err
	}
	stats.Before = before

	initialDebt, err := db.populateCompactStorageAudit(ctx, opts, &stats)
	if err != nil {
		return stats, err
	}
	stats.RemainingDebt = initialDebt
	if opts.DryRun {
		stats.After = before
		stats.FullyCompacted = initialDebt.Empty()
		return stats, nil
	}

	if err := db.runCompactStoragePhase(&stats, "checkpoint", func() error {
		return db.Checkpoint()
	}); err != nil {
		return stats, err
	}

	cleanupLeafLog, err := db.installCompactStorageLeafPageLog()
	if err != nil {
		return stats, err
	}
	defer cleanupLeafLog()
	if err := db.runCompactStoragePhase(&stats, "value-log-rewrite", func() error {
		rewrite, err := db.ValueLogRewriteOnline(ctx, ValueLogRewriteOnlineOptions{
			BatchSize:       opts.ValueLogRewriteBatchSize,
			SyncEachBatch:   opts.SyncEachPhase,
			MaxSegmentBytes: opts.ValueLogRewriteMaxSegmentBytes,
			LocalityPolicy:  ValueLogRewriteLocalityGrouped,
			ReserveRIDs:     opts.ReserveRIDs,
		})
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
		gc, err := db.ValueLogGC(ctx, ValueLogGCOptions{})
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

	for pass := 0; pass < opts.LeafPackMaxPasses; pass++ {
		var pack LeafGenerationPackRunOnceStats
		phaseName := fmt.Sprintf("leaf-generation-pack-%d", pass+1)
		if err := db.runCompactStoragePhase(&stats, phaseName, func() error {
			var err error
			pack, err = db.LeafGenerationPackRunOnce(ctx, LeafGenerationPackFromPlanOptions{
				Sync:                       opts.SyncEachPhase,
				MinExpectedReclaimBytes:    opts.LeafPackMinExpectedReclaimBytes,
				MinExpectedReclaimRatioPPM: opts.LeafPackMinExpectedReclaimRatioPPM,
				MinReclaimPerByteCopiedPPM: opts.LeafPackMinReclaimPerCopyPPM,
				MaxGenerations:             opts.LeafPackMaxGenerationsPerPass,
				MaxBytesToCopy:             opts.LeafPackMaxBytesToCopyPerPass,
				ReserveRIDs:                opts.ReserveRIDs,
				LeafFrameK:                 opts.LeafPackLeafFrameK,
			})
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
		if err := db.runCompactStoragePhase(&stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
			return db.Checkpoint()
		}); err != nil {
			return stats, err
		}
	}

	if err := db.runCompactStoragePhase(&stats, "leaf-generation-gc", func() error {
		gc, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{})
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
		err := db.VacuumIndexOnline(ctx)
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

	if err := db.settleCompactStorageGC(ctx, opts, &stats); err != nil {
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
			deleted, err := db.pruneZeroByteValueLogFiles()
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
	finalDebt, err := db.populateCompactStorageAudit(ctx, opts, &finalAudit)
	if err != nil {
		return stats, err
	}
	stats.ValueLogRewritePlan = finalAudit.ValueLogRewritePlan
	stats.ValueLogGC = finalAudit.ValueLogGC
	stats.LeafGenerationPlan = finalAudit.LeafGenerationPlan
	stats.LeafGenerationGC = finalAudit.LeafGenerationGC
	stats.RemainingDebt = finalDebt
	stats.FullyCompacted = finalDebt.Empty()
	return stats, nil
}

func (db *DB) settleCompactStorageGC(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats) error {
	const maxSettlePasses = 4
	for pass := 0; pass < maxSettlePasses; pass++ {
		var audit CompactStorageStats
		debt, err := db.populateCompactStorageAudit(ctx, opts, &audit)
		if err != nil {
			return err
		}
		if debt.ValueLogGCSegments == 0 && debt.LeafGCSegments == 0 {
			return nil
		}
		if debt.ValueLogGCSegments > 0 {
			phaseName := fmt.Sprintf("settle-value-log-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				gc, err := db.ValueLogGC(ctx, ValueLogGCOptions{})
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
		if debt.LeafGCSegments > 0 {
			phaseName := fmt.Sprintf("settle-leaf-generation-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				gc, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{})
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

func normalizeCompactStorageOptions(opts CompactStorageOptions) CompactStorageOptions {
	switch opts.Mode {
	case CompactStorageDefault:
		opts.Mode = CompactStorageFull
	case CompactStorageFull, CompactStorageQuick:
	default:
		opts.Mode = CompactStorageFull
	}
	if opts.ValueLogRewriteBatchSize < 0 {
		opts.ValueLogRewriteBatchSize = 0
	}
	if opts.LeafPackMaxPasses <= 0 {
		if opts.Mode == CompactStorageQuick {
			opts.LeafPackMaxPasses = 1
		} else {
			opts.LeafPackMaxPasses = 32
		}
	}
	if opts.LeafPackMaxGenerationsPerPass < 0 {
		opts.LeafPackMaxGenerationsPerPass = 0
	}
	return opts
}

func compactStorageRewritePlanOptions() ValueLogRewriteOnlineOptions {
	return ValueLogRewriteOnlineOptions{
		MinSegmentStaleBytes: 1,
	}
}

func (db *DB) populateCompactStorageAudit(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats) (CompactStorageDebt, error) {
	var debt CompactStorageDebt
	rewritePlan, err := db.ValueLogRewritePlan(ctx, compactStorageRewritePlanOptions())
	if err != nil {
		return debt, err
	}
	stats.ValueLogRewritePlan = rewritePlan
	debt.ValueLogRewriteSegments = rewritePlan.SegmentsSelected
	debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesStale
	if debt.ValueLogRewriteBytes == 0 {
		debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesTotal
	}

	valueGC, err := db.ValueLogGC(ctx, ValueLogGCOptions{DryRun: true})
	if err != nil {
		return debt, err
	}
	stats.ValueLogGC = valueGC
	debt.ValueLogGCSegments = valueGC.SegmentsEligible
	debt.ValueLogGCBytes = valueGC.BytesEligible

	leafPlan, err := db.LeafGenerationPlan(ctx, LeafGenerationPlanOptions{
		MinExpectedReclaimBytes:    opts.LeafPackMinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.LeafPackMinExpectedReclaimRatioPPM,
		MinReclaimPerByteCopiedPPM: opts.LeafPackMinReclaimPerCopyPPM,
	})
	if err != nil {
		return debt, err
	}
	stats.LeafGenerationPlan = leafPlan
	if leafPlan.Admission == leafGenerationPlanAdmissionEligible {
		debt.LeafPackGenerations = len(leafPlan.Candidates)
		debt.LeafPackBytes = leafPlan.ExpectedReclaimBytes
	}

	leafGC, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{DryRun: true})
	if err != nil {
		return debt, err
	}
	stats.LeafGenerationGC = leafGC
	debt.LeafGCSegments = leafGC.GenerationsEligible
	debt.LeafGCBytes = leafGC.BytesEligible

	usage, err := compactStorageUsage(db.dir)
	if err != nil {
		return debt, err
	}
	debt.ZeroByteValueLogFiles = zeroByteValueLogFilesFromUsage(usage)
	return debt, nil
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
		{name: "index", path: filepath.Join(dir, "index.db")},
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

func zeroByteValueLogFilesFromUsage(usage []CompactStorageUsage) int {
	for _, domain := range usage {
		if domain.Name == "value_vlog" {
			return domain.ZeroByteFiles
		}
	}
	return 0
}

func (db *DB) pruneZeroByteValueLogFiles() (int, error) {
	layout := resolveStorageLayout(db.dir)
	entries, err := os.ReadDir(layout.valueVLogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
			continue
		}
		path := filepath.Join(layout.valueVLogDir, name)
		info, err := entry.Info()
		if err != nil {
			return deleted, err
		}
		if info.Size() != 0 {
			continue
		}
		if db.valueLogManager != nil {
			if fileID, ok := compactStorageValueLogFileID(name); ok && db.valueLogManager.HasSegment(fileID) {
				if err := db.valueLogManager.RemoveSegment(fileID); err != nil {
					return deleted, err
				}
				deleted++
				continue
			}
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
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

func (db *DB) installCompactStorageLeafPageLog() (func(), error) {
	if db == nil || !db.indexOuterLeavesInValueLog || db.leafPageLog != nil {
		return func() {}, nil
	}
	segments, err := listValueLogSegments(db.dir)
	if err != nil {
		return nil, err
	}
	leafStartSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	writer := newRewriteWriter(ValueLogDirPath(db.dir), 0, 0, 0)
	writer.ConfigureLeafLog(LeafLogDirPath(db.dir), rewriteLeafLogLaneID, leafStartSeq)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	if writer.blockCompression {
		if state := db.State(); state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, err := prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if err != nil {
				_ = writer.Close()
				return nil, err
			}
			if leafDictID != 0 && len(leafDictBytes) > 0 {
				writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			}
		}
	}
	db.SetLeafPageLog(writer)
	return func() {
		db.SetLeafPageLog(nil)
		_ = writer.Close()
	}, nil
}
