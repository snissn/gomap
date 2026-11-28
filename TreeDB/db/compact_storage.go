package db

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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

// CompactStorageOwnerStatus is the production support category for an
// exhaustive compact leaf-page-log owner.
type CompactStorageOwnerStatus string

const (
	CompactStorageOwnerStatusSupportedTarget      CompactStorageOwnerStatus = "supported-target"
	CompactStorageOwnerStatusLiveWriterFailClosed CompactStorageOwnerStatus = "live-writer fail-closed"
	CompactStorageOwnerStatusExternalUnsupported  CompactStorageOwnerStatus = "external-owner unsupported"
	CompactStorageOwnerStatusBlockingBug          CompactStorageOwnerStatus = "blocking-bug"
)

// CompactStorageLeafPageLogOwnerClass identifies who owns the currently
// installed leaf-page-log writer from CompactStorage's point of view.
type CompactStorageLeafPageLogOwnerClass string

const (
	CompactStorageLeafPageLogOwnerNone                     CompactStorageLeafPageLogOwnerClass = "no installed leaf log"
	CompactStorageLeafPageLogOwnerCommandWALReplayInline   CompactStorageLeafPageLogOwnerClass = "command-WAL/replay-inline internal owner"
	CompactStorageLeafPageLogOwnerInternalHiddenByWrapper  CompactStorageLeafPageLogOwnerClass = "internal owner hidden by wrapper"
	CompactStorageLeafPageLogOwnerCachedWrapper            CompactStorageLeafPageLogOwnerClass = "cached/wrapper owner"
	CompactStorageLeafPageLogOwnerStandaloneCallerExternal CompactStorageLeafPageLogOwnerClass = "standalone/caller external owner"
)

// CompactStorageLifecycleState records the writer lifecycle assumption used by
// the owner classifier.
type CompactStorageLifecycleState string

const (
	CompactStorageLifecycleExclusiveMaintenance CompactStorageLifecycleState = "exclusive maintenance"
	CompactStorageLifecycleQuiescedMaintenance  CompactStorageLifecycleState = "quiesced maintenance"
	CompactStorageLifecycleActiveWriter         CompactStorageLifecycleState = "active writer"
)

// ErrCompactStorageLeafPageLogOwnerUnsupported is returned when exhaustive
// compact reaches an installed leaf-page-log owner it cannot safely replace.
var ErrCompactStorageLeafPageLogOwnerUnsupported = errors.New("treedb: compact storage leaf page log owner unsupported")

// ErrCompactStorageLeafPageLogHandoffCleanup is returned when CompactStorage
// cannot safely restore the previous leaf-page-log owner after installing its
// temporary compact writer.
var ErrCompactStorageLeafPageLogHandoffCleanup = errors.New("treedb: compact storage leaf page log handoff cleanup failed")

// ErrCompactStorageAuditStale is a retryable error reporting that both attempts
// to build a coherent CompactStorage audit were invalidated before publication.
var ErrCompactStorageAuditStale = errors.New("treedb: compact storage audit snapshot became stale")

// LeafPageLogCompactStorageHandoff is implemented by internally owned
// leaf-page-log writers that can be safely restored after CompactStorage
// temporarily replaces them with its compact writer.
type LeafPageLogCompactStorageHandoff interface {
	AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error
}

// LeafPageLogSealedSegmentDetacher is implemented by internally owned
// multi-lane leaf-page-log writers that can detach worker lanes still parked
// on already-sealed generations so LeafGenerationGC can reclaim them without
// eagerly allocating empty replacement segments.
type LeafPageLogSealedSegmentDetacher interface {
	DetachSealedLeafPageLogSegments(sealedRawFileIDs map[uint32]struct{}) error
}

// CompactStorageLeafPageLogOwnerClassification is the compact-owner production
// support contract attached to fail-closed exhaustive compact errors.
type CompactStorageLeafPageLogOwnerClassification struct {
	OwnerClass CompactStorageLeafPageLogOwnerClass
	Status     CompactStorageOwnerStatus
	// Lifecycle is a caller-supplied modeled state, not runtime proof. The
	// current CompactStorage refusal path classifies after taking the exclusive
	// maintenance lock and therefore passes CompactStorageLifecycleExclusiveMaintenance.
	Lifecycle          CompactStorageLifecycleState
	Replaceable        bool
	RequiresQuiescence bool
	Detail             string
}

// CompactStorageLeafPageLogOwnerError carries the owner classification that
// caused exhaustive compact to fail closed.
type CompactStorageLeafPageLogOwnerError struct {
	Classification CompactStorageLeafPageLogOwnerClassification
}

func (e *CompactStorageLeafPageLogOwnerError) Error() string {
	classification := CompactStorageLeafPageLogOwnerClassification{}
	if e != nil {
		classification = e.Classification
	}
	status := classification.Status
	if status == "" {
		status = CompactStorageOwnerStatusExternalUnsupported
	}
	return fmt.Sprintf(
		"treedb: exhaustive compact requires an internally-owned leaf page log; close or clear the installed leaf page log before compacting (owner=%s status=%s lifecycle=%s)",
		classification.OwnerClass,
		status,
		classification.Lifecycle,
	)
}

func (e *CompactStorageLeafPageLogOwnerError) Unwrap() error {
	return ErrCompactStorageLeafPageLogOwnerUnsupported
}

// CompactStorageLeafPageLogHandoffError reports the restore stage that failed
// after CompactStorage installed its temporary leaf-page-log writer. When
// restoration cannot be proven safe, CompactStorage fails closed by clearing the
// active leaf-page-log writer; close and reopen the database before resuming
// writes.
type CompactStorageLeafPageLogHandoffError struct {
	Stage    string
	Recovery string
	Err      error
}

func (e *CompactStorageLeafPageLogHandoffError) Error() string {
	stage := ""
	recovery := "close and reopen the database before resuming writes"
	var err error
	if e != nil {
		stage = e.Stage
		if e.Recovery != "" {
			recovery = e.Recovery
		}
		err = e.Err
	}
	if stage == "" {
		stage = "unknown"
	}
	if err == nil {
		return fmt.Sprintf("treedb: compact storage leaf page log handoff cleanup failed at %s; recovery: %s", stage, recovery)
	}
	return fmt.Sprintf("treedb: compact storage leaf page log handoff cleanup failed at %s: %v; recovery: %s", stage, err, recovery)
}

func (e *CompactStorageLeafPageLogHandoffError) Unwrap() []error {
	if e == nil || e.Err == nil {
		return []error{ErrCompactStorageLeafPageLogHandoffCleanup}
	}
	return []error{ErrCompactStorageLeafPageLogHandoffCleanup, e.Err}
}

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
	// ValueLogRewriteMinSegmentStaleRatio and
	// ValueLogRewriteMinSegmentStaleBytes control non-exhaustive sparse
	// value-log rewrite admission. Both thresholds are conjunctive when set:
	// full/quick mode selects a segment only when stale ratio and stale bytes
	// both meet the mode policy. The default byte floor is capped at the
	// configured stale ratio for small segments, so a small segment that is
	// sufficiently stale is not permanently exempt; an explicit byte override
	// remains an exact conjunctive floor. Zero uses mode defaults. Exhaustive
	// mode ignores these knobs and rewrites any partially-live segment with
	// stale bytes so it remains the byte-minimizing mode.
	ValueLogRewriteMinSegmentStaleRatio float64
	ValueLogRewriteMinSegmentStaleBytes int64
	ValueLogProtectedPaths              []string
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

	// Leaf-generation pack knobs. Non-exhaustive defaults keep copy bytes per
	// pass finite while still draining ordinary debt.
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

	IndexVacuumRequired                    bool   `json:"index_vacuum_required"`
	IndexVacuumReason                      string `json:"index_vacuum_reason"`
	IndexVacuumTotalPages                  uint64 `json:"index_vacuum_total_pages"`
	IndexVacuumUserPages                   uint64 `json:"index_vacuum_user_pages"`
	IndexVacuumUserSpan                    uint64 `json:"index_vacuum_user_span"`
	IndexVacuumUserSpanRatioPPM            uint64 `json:"index_vacuum_user_span_ratio_ppm"`
	IndexVacuumFreelistReclaimablePages    uint64 `json:"index_vacuum_freelist_reclaimable_pages"`
	IndexVacuumFreelistReclaimableRatioPPM uint64 `json:"index_vacuum_freelist_reclaimable_ratio_ppm"`
	IndexVacuumCollectionRootPages         uint64 `json:"index_vacuum_collection_root_pages"`
	IndexVacuumCollectionRootSpan          uint64 `json:"index_vacuum_collection_root_span"`
	IndexVacuumCollectionRootSpanRatioPPM  uint64 `json:"index_vacuum_collection_root_span_ratio_ppm"`
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
		d.ZeroByteValueLogFiles == 0 &&
		!d.IndexVacuumRequired
}

// CompactStoragePhaseStatus identifies the terminal decision for a compaction
// phase. Empty is retained for older phase records that do not need a policy
// disposition.
type CompactStoragePhaseStatus string

const (
	CompactStoragePhaseStatusPlanned     CompactStoragePhaseStatus = "planned"
	CompactStoragePhaseStatusNotRequired CompactStoragePhaseStatus = "not_required"
	CompactStoragePhaseStatusSucceeded   CompactStoragePhaseStatus = "succeeded"
	CompactStoragePhaseStatusDeferred    CompactStoragePhaseStatus = "deferred"
	CompactStoragePhaseStatusUnsupported CompactStoragePhaseStatus = "unsupported"
	CompactStoragePhaseStatusFailed      CompactStoragePhaseStatus = "failed"
)

// CompactStoragePhaseStats records one phase in a full compaction run.
type CompactStoragePhaseStats struct {
	Name          string                    `json:"name"`
	Status        CompactStoragePhaseStatus `json:"status,omitempty"`
	Required      bool                      `json:"required,omitempty"`
	Reason        string                    `json:"reason,omitempty"`
	Skipped       bool                      `json:"skipped,omitempty"`
	SkipReason    string                    `json:"skip_reason,omitempty"`
	WallTimeNanos int64                     `json:"wall_time_nanos"`
}

// CompactStorageAuditStats records shared reachability work and structural
// reuse decisions made while producing a CompactStorage report.
type CompactStorageAuditStats struct {
	SharedScans                   uint64 `json:"shared_scans"`
	StructuralReuseHits           uint64 `json:"structural_reuse_hits"`
	StructuralReuseMisses         uint64 `json:"structural_reuse_misses"`
	RevalidationRetries           uint64 `json:"revalidation_retries"`
	RootSets                      uint64 `json:"root_sets"`
	PagesVisited                  uint64 `json:"pages_visited"`
	MemoHits                      uint64 `json:"memo_hits"`
	PointerProjections            uint64 `json:"pointer_projections"`
	GroupedRecordDedupeHits       uint64 `json:"grouped_record_dedupe_hits"`
	PhysicalBytesRead             uint64 `json:"physical_bytes_read"`
	LastStructuralReuseMissReason string `json:"last_structural_reuse_miss_reason,omitempty"`
}

// CompactStorageStats is the single high-level report for TreeDB storage
// compaction and planning.
type CompactStorageStats struct {
	Mode   CompactStorageMode `json:"mode"`
	DryRun bool               `json:"dry_run"`

	Before []CompactStorageUsage `json:"before"`
	After  []CompactStorageUsage `json:"after"`

	Phases []CompactStoragePhaseStats `json:"phases,omitempty"`
	Audit  CompactStorageAuditStats   `json:"audit"`

	ValueLogRewritePlan ValueLogRewritePlan              `json:"value_log_rewrite_plan"`
	ValueLogRewrite     ValueLogRewriteStats             `json:"value_log_rewrite,omitempty"`
	ValueLogGC          ValueLogGCStats                  `json:"value_log_gc"`
	LeafGenerationPlan  LeafGenerationPlan               `json:"leaf_generation_plan"`
	LeafGenerationPacks []LeafGenerationPackRunOnceStats `json:"leaf_generation_packs,omitempty"`
	LeafGenerationGC    LeafGenerationGCStats            `json:"leaf_generation_gc"`
	IndexVacuum         VacuumOnlineStats                `json:"index_vacuum"`

	ZeroByteValueLogFilesDeleted int `json:"zero_byte_value_log_files_deleted,omitempty"`

	RemainingDebt CompactStorageDebt `json:"remaining_debt"`

	// FullyCompacted is the legacy policy-oriented compacted flag and matches
	// PolicyFullyCompacted. Non-exhaustive modes compute value-log rewrite debt
	// against their stale-ratio/stale-byte policy, so they can be policy compacted
	// while ByteMinimized remains false. Exhaustive mode keeps the stricter
	// byte-minimizing rewrite policy and is the only mode that asserts
	// ByteMinimized.
	FullyCompacted       bool `json:"fully_compacted"`
	PolicyFullyCompacted bool `json:"policy_fully_compacted"`
	ByteMinimized        bool `json:"byte_minimized"`
}

type compactStorageFencedValueLogRefEvent struct {
	Source             valueLogRefResolutionSource
	ReferencedSegments int
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
	stats, err := db.compactStorageWithOwner(ctx, opts, nil)
	if err != nil {
		return stats, err
	}
	if err := db.settleCompactStorageCommandWAL(ctx, opts, &stats); err != nil {
		return stats, err
	}
	return stats, nil
}

// settleCompactStorageCommandWAL converges the command-WAL checkpoint fallback
// and prunes covered command-WAL segments after exhaustive compaction. It must
// run strictly outside db.maintenanceMu so RefreshCommandWALCheckpointFallback,
// Checkpoint, and CleanupCommandWALCoveredSegmentsAtCheckpoint preserve the
// canonical lock order without self-deadlock.
func (db *DB) settleCompactStorageCommandWAL(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats) error {
	if db == nil || db.readOnly || !db.CommandWALEnabled() {
		return nil
	}
	opts = normalizeCompactStorageOptions(opts)
	if opts.DryRun || opts.Mode != CompactStorageExhaustive {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.runCompactStoragePhase(stats, "command-wal-settle", func() error {
		if err := db.RefreshCommandWALCheckpointFallback(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.Checkpoint(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return db.CleanupCommandWALCoveredSegmentsAtCheckpoint(false)
	}); err != nil {
		return err
	}
	leafGCReclaimed := false
	if db.indexOuterLeavesInValueLog && stats != nil && (stats.LeafGenerationGC.GenerationsRetiring > 0 || stats.RemainingDebt.LeafGCGenerations > 0) {
		if err := db.runCompactStoragePhase(stats, "leaf-generation-gc-after-command-wal-settle", func() error {
			protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
			prior := stats.LeafGenerationGC
			gc, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{
				ProtectedRootIDs:       protectedRootIDs,
				ProtectedSystemRootIDs: protectedSystemRootIDs,
			})
			leafGCReclaimed = gc.GenerationsDeleted > 0 || gc.FilesDeleted > 0
			gc.BytesDeleted += prior.BytesDeleted
			gc.BytesEligible += prior.BytesEligible
			gc.FilesDeleted += prior.FilesDeleted
			gc.GenerationsDeleted += prior.GenerationsDeleted
			gc.GenerationsEligible += prior.GenerationsEligible
			stats.LeafGenerationGC = gc
			return err
		}); err != nil {
			return err
		}
	}
	if stats != nil {
		if leafGCReclaimed {
			final, err := db.CompactStoragePlan(ctx, opts)
			if err != nil {
				return err
			}
			addCompactStorageAuditStats(&stats.Audit, final.Audit)
			stats.After = final.After
			stats.RemainingDebt = final.RemainingDebt
			if stats.ValueLogGC.SegmentsPending > stats.RemainingDebt.ValueLogGCSegments {
				stats.RemainingDebt.ValueLogGCSegments = stats.ValueLogGC.SegmentsPending
			}
			if stats.ValueLogGC.BytesPending > stats.RemainingDebt.ValueLogGCBytes {
				stats.RemainingDebt.ValueLogGCBytes = stats.ValueLogGC.BytesPending
			}
			stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized = compactStorageCompactionFlags(opts, stats.RemainingDebt)
			return nil
		}
		after, err := compactStorageUsage(db.dir)
		if err != nil {
			return err
		}
		stats.After = after
	}
	return nil
}

// RefreshCompactStorageUsage updates stats.After from the current on-disk
// storage usage of backend directory dir.
func RefreshCompactStorageUsage(dir string, stats *CompactStorageStats) error {
	if stats == nil {
		return nil
	}
	after, err := compactStorageUsage(dir)
	if err != nil {
		return err
	}
	stats.After = after
	return nil
}

// PruneClosedEmptyCommandWALSegments removes preallocated empty (zero-logical-
// byte) command-WAL segment containers from a cleanly closed backend directory
// while holding the exclusive DB lock and journal owner lock. If any command-
// WAL segment contains frames or fails container validation, no files are
// removed.
func PruneClosedEmptyCommandWALSegments(dir string) (int, error) {
	if dir == "" {
		return 0, errors.New("db dir required")
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "LOCK"))
	if err != nil {
		return 0, err
	}
	defer func() { _ = lock.Close() }()
	return pruneClosedEmptyCommandWALSegmentsLocked(dir)
}

// pruneClosedEmptyCommandWALSegmentsLocked removes preallocated empty
// (zero-logical-byte) command-WAL segment containers while the caller already
// holds the exclusive DB LOCK for dir.
func pruneClosedEmptyCommandWALSegmentsLocked(dir string) (int, error) {
	if dir == "" {
		return 0, errors.New("db dir required")
	}
	walDir := WALDirPath(dir)
	info, err := os.Stat(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if !info.IsDir() {
		return 0, nil
	}

	// Avoid creating command-wal-journal-owner.lock in directories that have no
	// command-WAL segment files (e.g. non-command-WAL profiles).
	preEntries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	hasCommandSegments := false
	for _, entry := range preEntries {
		if !entry.IsDir() && entry.Type().IsRegular() && commitlog.IsCommandSegmentName(entry.Name()) {
			hasCommandSegments = true
			break
		}
	}
	if !hasCommandSegments {
		return 0, nil
	}

	owner, err := commitlog.AcquireJournalOwner(walDir)
	if err != nil {
		return 0, err
	}
	defer func() { _ = owner.Close() }()

	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var emptyPaths []string
	for _, entry := range entries {
		if entry.IsDir() || !commitlog.IsCommandSegmentName(entry.Name()) {
			continue
		}
		if !entry.Type().IsRegular() {
			continue
		}
		segPath := filepath.Join(walDir, entry.Name())
		f, err := os.Open(segPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		logicalSize, sizeErr := commitlog.LogicalFileSize(f)
		closeErr := f.Close()
		if sizeErr != nil || closeErr != nil || logicalSize != 0 {
			// Retain all segments conservatively whenever any segment has
			// frames or does not validate as an empty WAL container.
			return 0, nil
		}
		emptyPaths = append(emptyPaths, segPath)
	}
	if len(emptyPaths) == 0 {
		return 0, nil
	}

	removed := 0
	for _, segPath := range emptyPaths {
		if err := os.Remove(segPath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return removed, err
		}
		removed++
	}
	if removed > 0 {
		if err := syncDirFn(walDir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// CompactStorageForPrivateCOW runs storage compaction for the cache's
// originally bound private-COW owner. The caller keeps its maintenance fence
// through basis refresh and post-refresh settlement; this method grants no
// callback a publisher role and fails closed on online index vacuum while
// private-COW or native semantic ownership is active.
func (db *DB) CompactStorageForPrivateCOW(owner *transactionowner.Owner, ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	if db == nil || owner == nil || owner != db.privateCOWOwner {
		return CompactStorageStats{}, ErrNativeSemanticUnsupported
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return CompactStorageStats{}, err
	}
	tracked := false
	if !db.nativeSemanticActive.Load() {
		var err error
		tracked, err = db.beginSemanticBackendOperation(nil, false)
		if err != nil {
			return CompactStorageStats{}, err
		}
	}
	defer db.endSemanticBackendOperation(tracked)
	opts.DryRun = false
	return db.compactStorageWithOwner(ctx, opts, owner)
}

// LeafGenerationSealedDebt reports the current count and best-effort byte
// footprint of sealed leaf-log generations without scanning the index tree.
func (db *DB) LeafGenerationSealedDebt() (generations int, bytes int64) {
	if db == nil || !db.indexOuterLeavesInValueLog {
		return 0, 0
	}
	var set *valuelog.Set
	if db.valueLogManager != nil {
		set = db.valueLogManager.CurrentSetNoRefresh()
		if set != nil {
			defer func() { _ = db.valueLogManager.Release(set) }()
		}
	}
	m := db.collectLeafGenerationMetrics(set, nil)
	return m.generationsSealed, m.bytesSealed
}

func compactStorageRanLeafGenerationPack(stats *CompactStorageStats) bool {
	if stats == nil {
		return false
	}
	for _, p := range stats.LeafGenerationPacks {
		if p.Ran && (p.Pack.BytesCopied > 0 || p.Pack.LeafPagesCopied > 0 || len(p.Selection.GenerationIDs) > 0 || len(p.Pack.SourceGenerationIDs) > 0) {
			return true
		}
	}
	return false
}

// SettleCompactStorageAfterReadBasisRefresh retries leaf retirement held by a
// cached caller's previous read basis. The caller keeps its writer fence across
// the original compaction, basis refresh, and this settle operation. Ordinary
// root, recoverable-slot/parent, and independent reader protections still apply.
// No checkpoint or root publication is needed merely to retire those read pins.
func (db *DB) SettleCompactStorageAfterReadBasisRefresh(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats) error {
	if stats == nil {
		return nil
	}
	hasRetiring := stats.LeafGenerationGC.GenerationsRetiring > 0
	hasUnprunedDeleted := (stats.LeafGenerationGC.BytesDeleted > 0 && stats.LeafGenerationGC.FilesDeleted == 0) ||
		stats.LeafGenerationGC.GenerationsEligible > stats.LeafGenerationGC.GenerationsDeleted
	hasPackedLeafGenerations := compactStorageRanLeafGenerationPack(stats)
	if !hasRetiring && !hasUnprunedDeleted && !hasPackedLeafGenerations {
		return nil
	}
	opts = normalizeCompactStorageOptions(opts)
	priorBytesDeleted := stats.LeafGenerationGC.BytesDeleted
	priorBytesEligible := stats.LeafGenerationGC.BytesEligible
	priorFilesDeleted := stats.LeafGenerationGC.FilesDeleted
	priorGenerationsDeleted := stats.LeafGenerationGC.GenerationsDeleted
	priorGenerationsEligible := stats.LeafGenerationGC.GenerationsEligible
	if err := db.runCompactStoragePhase(stats, "leaf-generation-gc-after-read-basis-refresh", func() error {
		if _, err := db.refreshDurableRootSlotsForDeletedOuterLeafFiles(nil); err != nil {
			return err
		}
		roots, systems := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
		gc, err := db.LeafGenerationGC(ctx, LeafGenerationGCOptions{ProtectedRootIDs: roots, ProtectedSystemRootIDs: systems})
		gc.BytesDeleted += priorBytesDeleted
		gc.BytesEligible += priorBytesEligible
		gc.FilesDeleted += priorFilesDeleted
		gc.GenerationsDeleted += priorGenerationsDeleted
		gc.GenerationsEligible += priorGenerationsEligible
		stats.LeafGenerationGC = gc
		return err
	}); err != nil {
		return err
	}
	final, err := db.CompactStoragePlan(ctx, opts)
	if err != nil {
		return err
	}
	addCompactStorageAuditStats(&stats.Audit, final.Audit)
	stats.After, stats.RemainingDebt = final.After, final.RemainingDebt
	stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized = final.FullyCompacted, final.PolicyFullyCompacted, final.ByteMinimized
	return nil
}

func (db *DB) compactStorage(ctx context.Context, opts CompactStorageOptions) (CompactStorageStats, error) {
	return db.compactStorageWithOwner(ctx, opts, nil)
}

func (db *DB) compactStorageWithOwner(ctx context.Context, opts CompactStorageOptions, owner *transactionowner.Owner) (stats CompactStorageStats, err error) {
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if owner != nil && owner != db.privateCOWOwner {
		return stats, ErrNativeSemanticUnsupported
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opts = normalizeCompactStorageOptions(opts)
	stats.Mode = opts.Mode
	stats.DryRun = opts.DryRun
	auditSession := &compactStorageAuditSession{}
	defer auditSession.close()
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
		if hook := db.testStorageMaintenanceBeforeLockHook; hook != nil {
			hook("compact-storage")
		}
		db.maintenanceMu.Lock()
		maintenanceLocked = true
		defer db.maintenanceMu.Unlock()
		if owner != nil && owner != db.privateCOWOwner {
			return stats, ErrNativeSemanticUnsupported
		}
		if err := db.CheckStorageMaintenanceReady(); err != nil {
			return stats, err
		}
		if hook := db.testStorageMaintenanceAfterLockHook; hook != nil {
			if err := hook("compact-storage"); err != nil {
				return stats, err
			}
		}
		restoreLeafPageReadCache := db.disableLeafPageReadCacheForMaintenance()
		defer restoreLeafPageReadCache()
	}

	before, err := compactStorageUsage(db.dir)
	if err != nil {
		return stats, err
	}
	stats.Before = before

	if !opts.DryRun && db.indexOuterLeavesInValueLog {
		commitSeq := uint64(1)
		if state, ok := db.StateToken(); ok && state.CommitSeq != 0 {
			commitSeq = state.CommitSeq
		}
		if _, err := db.reconcileLeafGenerationManifestWithDirInPlace(commitSeq); err != nil {
			return stats, err
		}
	}

	initialDebt, err := db.populateCompactStorageAudit(ctx, opts, &stats, !maintenanceLocked, nil, nil, auditSession)
	if err != nil {
		return stats, err
	}
	initialIndexDebt, err := db.compactStorageIndexVacuumDebt(ctx, opts)
	if err != nil {
		return stats, err
	}
	mergeCompactStorageIndexDebt(&initialDebt, initialIndexDebt)
	compactStorageApplyLeafGenerationIndexDebt(&initialDebt, stats.LeafGenerationGC)
	stats.RemainingDebt = initialDebt
	if opts.DryRun {
		stats.Phases = append(stats.Phases, compactStorageIndexVacuumPlanPhase(initialDebt))
		stats.After = before
		stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized = compactStorageCompactionFlags(opts, initialDebt)
		return stats, nil
	}

	if err := db.runCompactStoragePhase(&stats, "checkpoint", func() error {
		return db.checkpoint(true)
	}); err != nil {
		return stats, err
	}
	if err := db.prepareCompactStorageRIDAllocator(&opts); err != nil {
		return stats, err
	}

	compactLeafLog, leafLogHandoff, err := db.installCompactStorageLeafPageLog(opts)
	if err != nil {
		return stats, err
	}
	cleanupLeafLogDone := false
	defer func() {
		if !cleanupLeafLogDone && leafLogHandoff != nil {
			if cleanupErr := leafLogHandoff.cleanup(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	if opts.Mode == CompactStorageExhaustive && db.indexOuterLeavesInValueLog && compactLeafLog == nil && db.leafPageLog != nil {
		return stats, &CompactStorageLeafPageLogOwnerError{
			Classification: compactStorageClassifyLeafPageLogOwner(db.leafPageLog, CompactStorageLifecycleExclusiveMaintenance),
		}
	}

	// Leaf-pack/GC can make pre-compact leaf generations unreachable before
	// index-vacuum has cloned the current index. Keep a public snapshot pinned
	// through vacuum so those source leaf-log files remain readable.
	var indexVacuumLeafGuard *Snapshot
	if db.indexOuterLeavesInValueLog {
		indexVacuumLeafGuard = db.AcquireSnapshot()
		if indexVacuumLeafGuard == nil {
			return stats, ErrClosed
		}
		defer func() {
			if indexVacuumLeafGuard != nil {
				_ = indexVacuumLeafGuard.Close()
			}
		}()
	}
	releaseIndexVacuumLeafGuard := func() error {
		if indexVacuumLeafGuard == nil {
			return nil
		}
		err := indexVacuumLeafGuard.Close()
		indexVacuumLeafGuard = nil
		return err
	}
	releaseIndexVacuumLeafGuardValueLogPin := func() error {
		if indexVacuumLeafGuard == nil || !indexVacuumLeafGuard.vlogPinned {
			return nil
		}
		if indexVacuumLeafGuard.state == nil || indexVacuumLeafGuard.vlogManager == nil {
			return nil
		}
		set := indexVacuumLeafGuard.state.ValueLogSet
		if set == nil {
			return nil
		}
		err := indexVacuumLeafGuard.vlogManager.Release(set)
		indexVacuumLeafGuard.vlogPinned = false
		return err
	}

	if err := db.runCompactStoragePhase(&stats, "value-log-rewrite", func() error {
		protectedPaths := compactStorageOnlineRewriteProtectedPaths(opts)
		protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
		rewriteOpts := compactStorageRewritePlanOptions(opts, protectedPaths)
		rewriteOpts.LeafGenerationProtectedRootIDs = protectedRootIDs
		rewriteOpts.LeafGenerationProtectedSystemRootIDs = protectedSystemRootIDs
		rewriteOpts.BatchSize = opts.ValueLogRewriteBatchSize
		rewriteOpts.SyncEachBatch = opts.SyncEachPhase
		rewriteOpts.MaxSegmentBytes = opts.ValueLogRewriteMaxSegmentBytes
		rewriteOpts.LocalityPolicy = ValueLogRewriteLocalityGrouped
		rewriteOpts.ReserveRIDs = opts.ReserveRIDs
		rewrite, err := db.valueLogRewriteOnlineWithOwner(ctx, rewriteOpts, !maintenanceLocked, owner)
		stats.ValueLogRewrite = rewrite
		return err
	}); err != nil {
		return stats, err
	}
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-value-log-rewrite", func() error {
		return db.checkpoint(maintenanceLocked)
	}); err != nil {
		return stats, err
	}

	if err := db.runCompactStorageReclamationPhase(&stats, "value-log-gc", func() error {
		gc, err := db.valueLogGC(ctx, ValueLogGCOptions{ProtectedPaths: compactStorageValueLogProtectedPaths(opts)}, !maintenanceLocked)
		stats.ValueLogGC = gc
		return err
	}); err != nil {
		return stats, err
	}
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-value-log-gc", func() error {
		return db.checkpoint(maintenanceLocked)
	}); err != nil {
		return stats, err
	}

	compactLeafPackCreatedFileIDs := make(map[uint32]struct{})
	var compactLeafPackPlanState compactStorageLeafPackPlanState
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

	leafPackPassesRemaining := opts.LeafPackMaxPasses
	for pass := 0; pass < opts.LeafPackMaxPasses; pass++ {
		leafPackPassesRemaining--
		var pack LeafGenerationPackRunOnceStats
		phaseName := fmt.Sprintf("leaf-generation-pack-%d", pass+1)
		packErr := db.runCompactStoragePhase(&stats, phaseName, func() error {
			var err error
			protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
			pack, err = db.compactStorageLeafGenerationPackRunOnceWithOwner(ctx, compactStorageLeafPackFromPlanOptions(opts, protectedRootIDs, protectedSystemRootIDs), !maintenanceLocked, compactLeafPackCreatedFileIDs, &compactLeafPackPlanState, owner)
			return err
		})
		if packErr != nil {
			if opts.Mode != CompactStorageExhaustive && errors.Is(packErr, rootpublication.ErrNamespacePersistenceUnsupported) && !errors.Is(packErr, ErrRecoveryRequired) {
				if len(stats.Phases) > 0 {
					phase := &stats.Phases[len(stats.Phases)-1]
					phase.Status = CompactStoragePhaseStatusUnsupported
					phase.Reason = packErr.Error()
					phase.Skipped = true
					phase.SkipReason = packErr.Error()
				}
				leafPackPassesRemaining = 0
				break
			}
			return stats, packErr
		}
		stats.LeafGenerationPacks = append(stats.LeafGenerationPacks, pack)
		if !pack.Ran {
			if len(stats.Phases) > 0 {
				stats.Phases[len(stats.Phases)-1].Skipped = true
				stats.Phases[len(stats.Phases)-1].SkipReason = pack.SkipReason
			}
			break
		}
		compactStorageRememberLeafPackCreatedFileIDs(compactLeafPackCreatedFileIDs, pack)
		if err := db.refreshCompactStorageLeafPageLog(compactLeafLog); err != nil {
			return stats, err
		}
		if err := db.runCompactStoragePhase(&stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
			return db.checkpoint(maintenanceLocked)
		}); err != nil {
			return stats, err
		}
	}

	if err := db.runCompactStoragePhase(&stats, "leaf-generation-gc", func() error {
		protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
		gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
			ProtectedRootIDs:           protectedRootIDs,
			ProtectedSystemRootIDs:     protectedSystemRootIDs,
			DetachSealedCurrentWriters: true,
		}, !maintenanceLocked)
		stats.LeafGenerationGC = gc
		return err
	}); err != nil {
		return stats, err
	}
	compactLeafPackPlanState.invalidate()
	if err := db.runCompactStoragePhase(&stats, "checkpoint-after-leaf-generation-gc", func() error {
		if err := db.checkpoint(maintenanceLocked); err != nil {
			return err
		}
		refreshed, err := db.refreshDurableRootSlotsForDeletedOuterLeafFiles(owner)
		if err != nil {
			return err
		}
		if refreshed {
			protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
			prior := stats.LeafGenerationGC
			gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
				ProtectedRootIDs:           protectedRootIDs,
				ProtectedSystemRootIDs:     protectedSystemRootIDs,
				DetachSealedCurrentWriters: true,
			}, !maintenanceLocked)
			gc.BytesDeleted += prior.BytesDeleted
			gc.BytesEligible += prior.BytesEligible
			gc.FilesDeleted += prior.FilesDeleted
			gc.GenerationsDeleted += prior.GenerationsDeleted
			gc.GenerationsEligible += prior.GenerationsEligible
			stats.LeafGenerationGC = gc
			return err
		}
		return nil
	}); err != nil {
		return stats, err
	}

	indexDebt, err := db.compactStorageIndexVacuumDebtForExecution(ctx, opts, owner)
	if err != nil {
		stats.Phases = append(stats.Phases, CompactStoragePhaseStats{
			Name:       "index-vacuum",
			Status:     CompactStoragePhaseStatusFailed,
			Reason:     err.Error(),
			Skipped:    true,
			SkipReason: err.Error(),
		})
		return stats, fmt.Errorf("index-vacuum-plan: %w", err)
	}
	compactStorageApplyLeafGenerationIndexDebt(&indexDebt, stats.LeafGenerationGC)
	indexVacuumCompleted := false
	indexVacuumRetainedGuard := false
	if !indexDebt.IndexVacuumRequired {
		indexVacuumRetainedGuard = true
		stats.Phases = append(stats.Phases, CompactStoragePhaseStats{
			Name:       "index-vacuum",
			Status:     CompactStoragePhaseStatusNotRequired,
			Reason:     indexDebt.IndexVacuumReason,
			Skipped:    true,
			SkipReason: "no index vacuum policy debt",
		})
	} else {
		vacuumErr := db.runCompactStoragePhase(&stats, "index-vacuum", func() error {
			return db.compactStorageVacuumIndexOnlineWithOwner(ctx, !maintenanceLocked, owner)
		})
		phase := &stats.Phases[len(stats.Phases)-1]
		phase.Required = true
		phase.Reason = indexDebt.IndexVacuumReason
		switch {
		case vacuumErr == nil:
			phase.Status = CompactStoragePhaseStatusSucceeded
			stats.IndexVacuum = db.vacuumOnlineStatsSnapshot()
			indexVacuumCompleted = true
		case errors.Is(vacuumErr, ErrVacuumUnsupported):
			phase.Status = CompactStoragePhaseStatusUnsupported
			phase.Reason = vacuumErr.Error()
			phase.Skipped = true
			phase.SkipReason = vacuumErr.Error()
			indexVacuumRetainedGuard = true
		case compactStorageIndexVacuumTransient(vacuumErr):
			phase.Status = CompactStoragePhaseStatusDeferred
			phase.Reason = vacuumErr.Error()
			phase.Skipped = true
			phase.SkipReason = vacuumErr.Error()
			indexVacuumRetainedGuard = true
		default:
			phase.Status = CompactStoragePhaseStatusFailed
			phase.Reason = vacuumErr.Error()
			return stats, vacuumErr
		}
	}
	if indexVacuumCompleted {
		if err := db.runCompactStoragePhase(&stats, "checkpoint-after-index-vacuum", func() error {
			if err := db.checkpoint(maintenanceLocked); err != nil {
				return err
			}
			if err := db.refreshDurableRootFallback(); err != nil && (db.commandWAL || !compactStorageFallbackRefreshNonConvergence(err)) {
				return err
			}
			return nil
		}); err != nil {
			return stats, err
		}
	}
	if !indexVacuumRetainedGuard {
		if err := releaseIndexVacuumLeafGuard(); err != nil {
			return stats, err
		}
	}

	compactLeafPackPlanState.invalidate()
	if err := db.settleCompactStorageGCWithOwner(ctx, opts, &stats, !maintenanceLocked, compactLeafLog, compactLeafPackCreatedFileIDs, &leafPackPassesRemaining, auditSession, &compactLeafPackPlanState, owner); err != nil {
		return stats, err
	}
	if indexVacuumRetainedGuard && !indexDebt.IndexVacuumRequired {
		if err := releaseIndexVacuumLeafGuard(); err != nil {
			return stats, err
		}
		indexVacuumRetainedGuard = false
	}
	// If index vacuum was unsupported or deferred, keep leaf-generation pins
	// through final audit, but drop the unrelated value-log set pin before
	// value-log cleanup so zero-byte value_vlog segments are not kept alive.
	if indexVacuumRetainedGuard {
		if err := releaseIndexVacuumLeafGuardValueLogPin(); err != nil {
			return stats, err
		}
	}

	if opts.DisableZeroByteValueLogCleanup {
		stats.Phases = append(stats.Phases, CompactStoragePhaseStats{
			Name:       "prune-empty-value-log-files",
			Skipped:    true,
			SkipReason: "disabled",
		})
	} else {
		if err := db.runCompactStorageReclamationPhase(&stats, "prune-empty-value-log-files", func() error {
			deleted, err := db.pruneZeroByteValueLogFiles(compactStorageCleanupValueLogProtectedPaths(opts))
			stats.ZeroByteValueLogFilesDeleted = deleted
			return err
		}); err != nil {
			return stats, err
		}
	}

	var finalAudit CompactStorageStats
	var finalDebt CompactStorageDebt
	refreshFinalAudit := func() error {
		finalAudit = CompactStorageStats{}
		auditSession.valid = false
		var auditErr error
		finalDebt, auditErr = db.populateCompactStorageAudit(ctx, opts, &finalAudit, !maintenanceLocked, nil, compactLeafPackCreatedFileIDs, auditSession)
		if auditErr != nil {
			return auditErr
		}
		finalIndexDebt, auditErr := db.compactStorageIndexVacuumDebt(ctx, opts)
		if auditErr != nil {
			return auditErr
		}
		mergeCompactStorageIndexDebt(&finalDebt, finalIndexDebt)
		compactStorageApplyLeafGenerationIndexDebt(&finalDebt, finalAudit.LeafGenerationGC)
		return nil
	}
	if err := refreshFinalAudit(); err != nil {
		return stats, err
	}
	if !indexVacuumRetainedGuard && finalDebt.IndexVacuumRequired {
		vacuumErr := db.runCompactStoragePhase(&stats, "index-vacuum-settle", func() error {
			return db.compactStorageVacuumIndexOnlineWithOwner(ctx, !maintenanceLocked, owner)
		})
		phase := &stats.Phases[len(stats.Phases)-1]
		phase.Required = true
		phase.Reason = finalDebt.IndexVacuumReason
		switch {
		case vacuumErr == nil:
			phase.Status = CompactStoragePhaseStatusSucceeded
			stats.IndexVacuum = db.vacuumOnlineStatsSnapshot()
			if err := db.runCompactStoragePhase(&stats, "checkpoint-after-index-vacuum-settle", func() error {
				if err := db.checkpoint(maintenanceLocked); err != nil {
					return err
				}
				if err := db.refreshDurableRootFallback(); err != nil && (db.commandWAL || !compactStorageFallbackRefreshNonConvergence(err)) {
					return err
				}
				return nil
			}); err != nil {
				return stats, err
			}
			// Vacuum can append system leaves and leave their source generation
			// packable. Settle that debt within the same run-wide pass budget.
			compactLeafPackPlanState.invalidate()
			if err := db.settleCompactStorageGCWithOwner(ctx, opts, &stats, !maintenanceLocked, compactLeafLog, compactLeafPackCreatedFileIDs, &leafPackPassesRemaining, auditSession, &compactLeafPackPlanState, owner); err != nil {
				return stats, err
			}
			if err := refreshFinalAudit(); err != nil {
				return stats, err
			}
		case errors.Is(vacuumErr, ErrVacuumUnsupported):
			phase.Status = CompactStoragePhaseStatusUnsupported
			phase.Reason = vacuumErr.Error()
			phase.Skipped = true
			phase.SkipReason = vacuumErr.Error()
		case compactStorageIndexVacuumTransient(vacuumErr):
			phase.Status = CompactStoragePhaseStatusDeferred
			phase.Reason = vacuumErr.Error()
			phase.Skipped = true
			phase.SkipReason = vacuumErr.Error()
		default:
			phase.Status = CompactStoragePhaseStatusFailed
			phase.Reason = vacuumErr.Error()
			return stats, vacuumErr
		}
	}
	after, err := compactStorageUsage(db.dir)
	if err != nil {
		return stats, err
	}
	stats.After = after
	stats.ValueLogRewritePlan = finalAudit.ValueLogRewritePlan
	stats.LeafGenerationPlan = finalAudit.LeafGenerationPlan
	addCompactStorageAuditStats(&stats.Audit, finalAudit.Audit)
	stats.RemainingDebt = finalDebt
	// A zombie is removed from the manager's current topology before its exact
	// durable-slot/snapshot handles are released. The final filesystem audit
	// therefore cannot rediscover that pending deletion through the live set.
	// Preserve the applied GC result so CompactStorage never reports fully
	// compacted while a safely retained zombie still exists.
	if stats.ValueLogGC.SegmentsPending > stats.RemainingDebt.ValueLogGCSegments {
		stats.RemainingDebt.ValueLogGCSegments = stats.ValueLogGC.SegmentsPending
	}
	if stats.ValueLogGC.BytesPending > stats.RemainingDebt.ValueLogGCBytes {
		stats.RemainingDebt.ValueLogGCBytes = stats.ValueLogGC.BytesPending
	}
	if leafLogHandoff != nil {
		if err := leafLogHandoff.cleanup(); err != nil {
			return stats, err
		}
	}
	cleanupLeafLogDone = true
	// Reconcile any retired value-log handles from final compaction phases
	// (such as zero-byte segment pruning or deferred snapshot release) strictly
	// outside publishPrepareMu/valueLogPublicationMu, returning hook errors
	// separately without wrapping them as primary ErrRecoveryRequired.
	if err := db.runPostValueLogGCHook(); err != nil {
		return stats, err
	}
	stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized = compactStorageCompactionFlags(opts, stats.RemainingDebt)
	return stats, nil
}

func compactStorageIndexVacuumPlanPhase(debt CompactStorageDebt) CompactStoragePhaseStats {
	phase := CompactStoragePhaseStats{Name: "index-vacuum", Reason: debt.IndexVacuumReason}
	if !debt.IndexVacuumRequired {
		phase.Status = CompactStoragePhaseStatusNotRequired
		phase.Skipped = true
		phase.SkipReason = "no index vacuum policy debt"
		return phase
	}
	phase.Required = true
	if runtime.GOOS == "windows" {
		phase.Status = CompactStoragePhaseStatusUnsupported
		phase.Reason = ErrVacuumUnsupported.Error()
		phase.Skipped = true
		phase.SkipReason = ErrVacuumUnsupported.Error()
		return phase
	}
	phase.Status = CompactStoragePhaseStatusPlanned
	return phase
}

func (db *DB) compactStorageIndexVacuumDebtFromReport(opts CompactStorageOptions, report IndexVacuumTriggerReport, slotCopies uint64) CompactStorageDebt {
	var debt CompactStorageDebt
	populateCompactStorageIndexDebtWithSlotCopies(opts, report, slotCopies, &debt)
	if !debt.IndexVacuumRequired && opts.Mode == CompactStorageExhaustive && db.compactStorageIndexChunkSlackReclaimable(report) {
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "chunk_slack"
	}
	return debt
}

func (db *DB) compactStorageIndexVacuumDebt(ctx context.Context, opts CompactStorageOptions) (CompactStorageDebt, error) {
	report, err := db.IndexVacuumTriggerReportContext(ctx)
	if err != nil {
		return CompactStorageDebt{}, err
	}
	return db.compactStorageIndexVacuumDebtFromReport(opts, report, db.compactStorageExpectedVacuumSlotCopies()), nil
}

func (db *DB) compactStorageIndexVacuumDebtForExecution(ctx context.Context, opts CompactStorageOptions, owner *transactionowner.Owner) (CompactStorageDebt, error) {
	report, err := db.IndexVacuumTriggerReportContext(ctx)
	if err != nil {
		return CompactStorageDebt{}, err
	}
	slotCopies := db.compactStorageExpectedVacuumSlotCopies()
	debt := db.compactStorageIndexVacuumDebtFromReport(opts, report, slotCopies)
	// On non-command-WAL DATA indexes whose durable slots currently diverge,
	// check whether converging the fallback slot onto the current durable root
	// would let a 1-copy online vacuum reclaim whole-file slack. Attempt a
	// best-effort fallback refresh now: on a quiet DB the slots converge and
	// vacuum shrinks the file to 1x; under active writers non-convergence leaves
	// slotCopies == 2 so CompactStorage avoids launching a futile 2-slot rebuild.
	// Never issue an unowned fallback refresh while private-COW or native
	// semantic ownership is active (matching compactStorageVacuumIndexOnlineWithOwner).
	if !debt.IndexVacuumRequired && slotCopies == 2 && db != nil && !db.commandWAL &&
		owner == nil && !db.nativeSemanticActive.Load() &&
		runtime.GOOS != "windows" && !db.readOnly {
		if idx := db.idx.Load(); idx != nil && idx.primary == nil {
			singleCopyDebt := db.compactStorageIndexVacuumDebtFromReport(opts, report, 1)
			if singleCopyDebt.IndexVacuumRequired {
				if ctx != nil {
					if err := ctx.Err(); err != nil {
						return CompactStorageDebt{}, err
					}
				}
				if err := db.refreshDurableRootFallback(); err != nil && !compactStorageFallbackRefreshNonConvergence(err) {
					return CompactStorageDebt{}, err
				}
				slotCopies = db.compactStorageExpectedVacuumSlotCopies()
				debt = db.compactStorageIndexVacuumDebtFromReport(opts, report, slotCopies)
			}
		}
	}
	return debt, nil
}

func compactStorageFallbackRefreshNonConvergence(err error) bool {
	return errors.Is(err, errDurableRootFallbackDidNotConverge)
}

func (db *DB) compactStorageDurableSlotsDiverged() bool {
	if db == nil {
		return false
	}
	db.durablePublishMu.Lock()
	s0 := db.durableRoot.slotRecord[0]
	s1 := db.durableRoot.slotRecord[1]
	db.durablePublishMu.Unlock()
	return s0.CommitSeq != 0 && s1.CommitSeq != 0 &&
		(s0.UserRootPageID != s1.UserRootPageID ||
			s0.SystemRootPageID != s1.SystemRootPageID ||
			s0.AppliedCommandLSN != s1.AppliedCommandLSN ||
			s0.MaxEntryRevision != s1.MaxEntryRevision)
}

func (db *DB) compactStorageExpectedVacuumSlotCopies() uint64 {
	if db == nil {
		return 1
	}
	idx := db.idx.Load()
	if idx != nil && idx.primary != nil {
		return 2
	}
	if db.commandWAL {
		state := db.state.Load()
		if state != nil && state.AppliedCommandLSN != 0 {
			return 1
		}
	}
	if db.compactStorageDurableSlotsDiverged() {
		return 2
	}
	return 1
}

func (db *DB) compactStorageIndexChunkSlackReclaimable(report IndexVacuumTriggerReport) bool {
	if db == nil || db.dir == "" {
		return false
	}
	gen := db.idx.Load()
	if gen == nil || gen.pager == nil {
		return false
	}
	replacementChunk := vacuumReplacementChunkSize(db.chunkSize, gen)
	if replacementChunk <= 0 || db.chunkSize <= replacementChunk {
		return false
	}
	info, err := os.Stat(filepath.Join(db.dir, indexFileName))
	if err != nil || info.Size() <= replacementChunk {
		return false
	}
	estPages := report.TotalPages
	if report.FreelistReclaimablePages < estPages {
		estPages -= report.FreelistReclaimablePages
	}
	if estPages < 2 {
		estPages = 2
	}
	replacementChunkPages := uint64(replacementChunk / page.PageSize)
	if replacementChunkPages == 0 {
		return false
	}
	targetChunks := (estPages + replacementChunkPages - 1) / replacementChunkPages
	currentChunks := uint64(info.Size()) / uint64(replacementChunk)
	return currentChunks > targetChunks+1
}

func compactStorageIndexVacuumTransient(err error) bool {
	return errors.Is(err, ErrVacuumConcurrentMutation) ||
		errors.Is(err, ErrVacuumInProgress) ||
		errors.Is(err, ErrRecoverableRootSetStale) ||
		errors.Is(err, ErrDurableWALCleanupProofStale) ||
		errors.Is(err, rootpublication.ErrResourcePinned)
}

func (db *DB) compactStorageVacuumIndexOnline(ctx context.Context, lockMaintenance bool) error {
	return db.compactStorageVacuumIndexOnlineWithOwner(ctx, lockMaintenance, nil)
}

func (db *DB) compactStorageVacuumIndexOnlineWithOwner(ctx context.Context, lockMaintenance bool, owner *transactionowner.Owner) error {
	// Online index vacuum replaces rootPublication and stops the old runtime,
	// which would destroy db.acceptedPrefix while private-COW or native semantic
	// ownership is active. Fail closed with ErrVacuumUnsupported so CompactStorage
	// records the unsupported disposition and preserves leaf/value reclamation.
	if owner != nil || (db != nil && db.nativeSemanticActive.Load()) {
		return ErrVacuumUnsupported
	}
	if db != nil && db.compactStorageVacuumIndexOnlineHook != nil {
		return db.compactStorageVacuumIndexOnlineHook(ctx, lockMaintenance)
	}
	// Best-effort convergence of both durable meta slots onto the already-durable
	// current root before online vacuum on DATA indexes so quiet DBs emit a single
	// compact copy (1x live pages) instead of duplicating the B-tree for slot^1.
	// On non-command-WAL profiles with concurrent writers, non-convergence is
	// non-fatal and falls through safely to standard 2-slot online vacuum.
	if db != nil && runtime.GOOS != "windows" && !db.readOnly {
		if idx := db.idx.Load(); idx != nil && idx.primary == nil {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := db.refreshDurableRootFallback(); err != nil && !compactStorageFallbackRefreshNonConvergence(err) {
				return err
			}
		}
	}
	return db.vacuumIndexOnline(ctx, lockMaintenance)
}

func compactStorageCompactionFlags(opts CompactStorageOptions, debt CompactStorageDebt) (fullyCompacted bool, policyFullyCompacted bool, byteMinimized bool) {
	policyFullyCompacted = debt.Empty()
	fullyCompacted = policyFullyCompacted
	byteMinimized = opts.Mode == CompactStorageExhaustive && debt.Empty()
	return fullyCompacted, policyFullyCompacted, byteMinimized
}

func (db *DB) sealCompactStorageCurrentLeafGeneration(compactLeafLog *rewriteWriter) (bool, error) {
	if db == nil {
		return false, nil
	}
	var sealedFileIDs []uint32
	// Sidecar errors may call back into the DB. Report them after releasing
	// the gate that serializes the manifest and operational view.
	defer func() { db.persistLeafGenerationRecordLengthIndexes(sealedFileIDs) }()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	commitSeq := uint64(1)
	if state, ok := db.StateToken(); ok && state.CommitSeq != 0 {
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
	} else if db.leafPageLog != nil && len(rawFileIDs) > 0 {
		sealedSet := make(map[uint32]struct{}, len(rawFileIDs))
		for _, rawID := range rawFileIDs {
			if rawID != 0 {
				sealedSet[rawID] = struct{}{}
			}
		}
		if len(sealedSet) > 0 {
			if err := detachSealedLeafPageLogSegments(db.leafPageLog, sealedSet); err != nil {
				return false, err
			}
			stillCurrent, err := db.currentLeafPageLogRawFileIDSet()
			if err != nil {
				return false, err
			}
			db.demoteSealedLeafLogCurrentWritable(sealedSet, stillCurrent)
		}
	}

	if err := db.replaceLeafGenerationManifest(nextManifest); err != nil {
		return false, err
	}
	sealedFileIDs = rawFileIDs
	db.mu.Lock()
	db.leafGenerationManifest = nextManifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) settleCompactStorageGC(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats, lockMaintenance bool, compactLeafLog *rewriteWriter, ignoredLeafPackRawFileIDs map[uint32]struct{}, leafPackPassesRemaining *int, auditSession *compactStorageAuditSession, leafPackPlanState *compactStorageLeafPackPlanState) error {
	return db.settleCompactStorageGCWithOwner(ctx, opts, stats, lockMaintenance, compactLeafLog, ignoredLeafPackRawFileIDs, leafPackPassesRemaining, auditSession, leafPackPlanState, nil)
}

func (db *DB) settleCompactStorageGCWithOwner(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats, lockMaintenance bool, compactLeafLog *rewriteWriter, ignoredLeafPackRawFileIDs map[uint32]struct{}, leafPackPassesRemaining *int, auditSession *compactStorageAuditSession, leafPackPlanState *compactStorageLeafPackPlanState, owner *transactionowner.Owner) error {
	const maxSettlePasses = 4
	for pass := 0; pass < maxSettlePasses; pass++ {
		var audit CompactStorageStats
		var fencedIDs []uint32
		debt, err := db.populateCompactStorageAudit(ctx, opts, &audit, lockMaintenance, &fencedIDs, ignoredLeafPackRawFileIDs, auditSession)
		if err != nil {
			return err
		}
		addCompactStorageAuditStats(&stats.Audit, audit.Audit)
		leafPackDebtActionable := debt.LeafPackGenerations > 0 && *leafPackPassesRemaining > 0
		if debt.ValueLogGCSegments == 0 && !leafPackDebtActionable && debt.LeafGCGenerations == 0 && len(fencedIDs) == 0 {
			return nil
		}
		if debt.ValueLogGCSegments > 0 {
			phaseName := fmt.Sprintf("settle-value-log-gc-%d", pass+1)
			if err := db.runCompactStorageReclamationPhase(stats, phaseName, func() error {
				gc, err := db.valueLogGC(ctx, ValueLogGCOptions{ProtectedPaths: compactStorageValueLogProtectedPaths(opts)}, lockMaintenance)
				stats.ValueLogGC = gc
				return err
			}); err != nil {
				return err
			}
			if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
				return db.checkpoint(!lockMaintenance)
			}); err != nil {
				return err
			}
		}
		if len(fencedIDs) > 0 {
			phaseName := fmt.Sprintf("settle-fenced-value-log-gc-%d", pass+1)
			if err := db.runCompactStorageReclamationPhase(stats, phaseName, func() error {
				// Fenced IDs are independently proven unreachable by the
				// tracker-aware referenced set (strict commit-sequence match) or
				// its full-scan fallback. Cached callers rotate and block writers
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
				return db.checkpoint(!lockMaintenance)
			}); err != nil {
				return err
			}
		}
		if leafPackDebtActionable {
			(*leafPackPassesRemaining)--
			var pack LeafGenerationPackRunOnceStats
			phaseName := fmt.Sprintf("settle-leaf-generation-pack-%d", pass+1)
			packErr := db.runCompactStoragePhase(stats, phaseName, func() error {
				var err error
				protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
				pack, err = db.compactStorageLeafGenerationPackRunOnceWithOwner(ctx, compactStorageLeafPackFromPlanOptions(opts, protectedRootIDs, protectedSystemRootIDs), lockMaintenance, ignoredLeafPackRawFileIDs, leafPackPlanState, owner)
				return err
			})
			if packErr != nil {
				if opts.Mode != CompactStorageExhaustive && errors.Is(packErr, rootpublication.ErrNamespacePersistenceUnsupported) && !errors.Is(packErr, ErrRecoveryRequired) {
					if len(stats.Phases) > 0 {
						phase := &stats.Phases[len(stats.Phases)-1]
						phase.Status = CompactStoragePhaseStatusUnsupported
						phase.Reason = packErr.Error()
						phase.Skipped = true
						phase.SkipReason = packErr.Error()
					}
					*leafPackPassesRemaining = 0
				} else {
					return packErr
				}
			} else {
				stats.LeafGenerationPacks = append(stats.LeafGenerationPacks, pack)
				if !pack.Ran {
					if len(stats.Phases) > 0 {
						stats.Phases[len(stats.Phases)-1].Skipped = true
						stats.Phases[len(stats.Phases)-1].SkipReason = pack.SkipReason
					}
				} else {
					compactStorageRememberLeafPackCreatedFileIDs(ignoredLeafPackRawFileIDs, pack)
					if err := db.refreshCompactStorageLeafPageLog(compactLeafLog); err != nil {
						return err
					}
					if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
						return db.checkpoint(!lockMaintenance)
					}); err != nil {
						return err
					}
				}
			}
		}
		if debt.LeafGCGenerations > 0 {
			leafPackPlanState.invalidate()
			phaseName := fmt.Sprintf("settle-leaf-generation-gc-%d", pass+1)
			if err := db.runCompactStoragePhase(stats, phaseName, func() error {
				protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
				gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
					ProtectedRootIDs:           protectedRootIDs,
					ProtectedSystemRootIDs:     protectedSystemRootIDs,
					DetachSealedCurrentWriters: true,
				}, lockMaintenance)
				stats.LeafGenerationGC = gc
				return err
			}); err != nil {
				return err
			}
			if err := db.runCompactStoragePhase(stats, fmt.Sprintf("checkpoint-after-%s", phaseName), func() error {
				if err := db.checkpoint(!lockMaintenance); err != nil {
					return err
				}
				refreshed, err := db.refreshDurableRootSlotsForDeletedOuterLeafFiles(owner)
				if err != nil {
					return err
				}
				if refreshed {
					protectedRootIDs, protectedSystemRootIDs := db.compactStorageLeafGenerationProtectedRootIDPair(opts)
					prior := stats.LeafGenerationGC
					gc, err := db.leafGenerationGC(ctx, LeafGenerationGCOptions{
						ProtectedRootIDs:           protectedRootIDs,
						ProtectedSystemRootIDs:     protectedSystemRootIDs,
						DetachSealedCurrentWriters: true,
					}, lockMaintenance)
					gc.BytesDeleted += prior.BytesDeleted
					gc.BytesEligible += prior.BytesEligible
					gc.FilesDeleted += prior.FilesDeleted
					gc.GenerationsDeleted += prior.GenerationsDeleted
					gc.GenerationsEligible += prior.GenerationsEligible
					stats.LeafGenerationGC = gc
					return err
				}
				return nil
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
	if reserver := db.currentValueLogRIDReserver(); reserver != nil {
		opts.ReserveRIDs = reserver.ReserveRIDs
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
	if opts.ValueLogRewriteMinSegmentStaleRatio < 0 {
		opts.ValueLogRewriteMinSegmentStaleRatio = 0
	}
	if opts.ValueLogRewriteMinSegmentStaleBytes < 0 {
		opts.ValueLogRewriteMinSegmentStaleBytes = 0
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
			// Full compaction is already bounded by LeafPackMaxBytesToCopyPerPass.
			// Do not add an arbitrary generation-count cap: large copied-state audits
			// can have many small dead leaf generations where the old 64-generation
			// cap stopped a pass before the byte budget was necessarily exhausted.
			opts.LeafPackMaxGenerationsPerPass = 0
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

const (
	compactStoragePolicyValueLogRewriteMinStaleRatio = 0.30
	compactStoragePolicyValueLogRewriteMinStaleBytes = 8 << 20
)

func compactStorageRewritePlanOptions(opts CompactStorageOptions, protectedPaths []string) ValueLogRewriteOnlineOptions {
	ratio := float64(0)
	staleBytes := int64(1)
	staleBytesCapRatio := float64(0)
	if opts.Mode != CompactStorageExhaustive {
		ratio = compactStoragePolicyValueLogRewriteMinStaleRatio
		staleBytes = compactStoragePolicyValueLogRewriteMinStaleBytes
		if opts.ValueLogRewriteMinSegmentStaleRatio > 0 {
			ratio = opts.ValueLogRewriteMinSegmentStaleRatio
		}
		if opts.ValueLogRewriteMinSegmentStaleBytes > 0 {
			staleBytes = opts.ValueLogRewriteMinSegmentStaleBytes
		} else {
			// Keep the 8 MiB default meaningful for normal segments without
			// permanently excluding a small segment that meets the ratio policy.
			staleBytesCapRatio = ratio
		}
	}
	return ValueLogRewriteOnlineOptions{
		MinSegmentStaleRatio:         ratio,
		MinSegmentStaleBytes:         staleBytes,
		MinSegmentStaleBytesCapRatio: staleBytesCapRatio,
		ProtectedPaths:               protectedPaths,
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

const compactStorageLeafPackSkipRunCreated = "compact_storage_run_created"

type compactStorageLeafPackPlanState struct {
	key   treeReachabilityCacheKey
	live  leafGenerationLiveScanStats
	valid bool
}

func (state *compactStorageLeafPackPlanState) invalidate() {
	if state == nil {
		return
	}
	*state = compactStorageLeafPackPlanState{}
}

func (state *compactStorageLeafPackPlanState) resetFromPlan(plan LeafGenerationPlan) {
	if state == nil || plan.liveStats.Generations == nil {
		return
	}
	state.key = plan.stateKey
	state.live = cloneLeafGenerationLiveScanStats(plan.liveStats)
	state.valid = true
}

func (db *DB) compactStorageLeafGenerationPlan(ctx context.Context, opts LeafGenerationPlanOptions, carry *compactStorageLeafPackPlanState) (LeafGenerationPlan, error) {
	if carry != nil && carry.valid {
		snap := db.AcquireSnapshot()
		if snap == nil {
			return LeafGenerationPlan{}, ErrClosed
		}
		published := snap.state
		db.valueLogStateCaptured("compact-carry-plan", published)
		key, ok := leafGenerationPlanStateKey(published, opts)
		if ok && key == carry.key && published.LeafGenerations != nil && published.LeafGenerations.sourceManifest != nil {
			plan := db.leafGenerationPlanFromLiveStats(opts, published, published.LeafGenerations.sourceManifest, carry.live)
			plan.stateKey = key
			plan.liveStats = cloneLeafGenerationLiveScanStats(carry.live)
			return plan, snap.Close()
		}
		carry.invalidate()
		if err := snap.Close(); err != nil {
			return LeafGenerationPlan{}, err
		}
	}
	plan, err := db.LeafGenerationPlan(ctx, opts)
	if err != nil {
		return plan, err
	}
	carry.resetFromPlan(plan)
	return plan, nil
}

func (state *compactStorageLeafPackPlanState) advance(db *DB, pack LeafGenerationPackStats, result leafGenerationPackCarryResult) (retErr error) {
	if state == nil || !state.valid || db == nil || pack.CopyAborts != 0 || result.publishedState == nil || result.protectedRootsOverlapSourceMaintenance {
		state.invalidate()
		return nil
	}
	sourceKey := result.sourceStateKey
	sourceKey.protectedRoots = state.key.protectedRoots
	if sourceKey != state.key || result.publishedState.LeafGenerations == nil {
		state.invalidate()
		return nil
	}

	// The carry result is borrowed scalar/immutable topology only. File sizing
	// uses the SAME live captured publication, never its retired Set alias.
	snap := db.AcquireSnapshot()
	if snap == nil {
		state.invalidate()
		return ErrClosed
	}
	defer func() { retErr = errors.Join(retErr, snap.Close()) }()
	publishedKey, publishedOK := leafGenerationLiveStatsKeyForState(result.publishedState)
	capturedKey, capturedOK := leafGenerationLiveStatsKeyForState(snap.state)
	if !publishedOK || !capturedOK || publishedKey != capturedKey {
		state.invalidate()
		return nil
	}
	db.valueLogStateCaptured("compact-carry-advance", snap.state)

	live := cloneLeafGenerationLiveScanStats(state.live)
	if result.trackSourceLiveMoved {
		for generationID, moved := range result.sourceLiveMovedByGeneration {
			remaining := live.Generations[generationID]
			remaining.LivePages -= moved.LivePages
			remaining.LiveBytes -= moved.LiveBytes
			if remaining.LivePages < 0 {
				remaining.LivePages = 0
			}
			if remaining.LiveBytes < 0 {
				remaining.LiveBytes = 0
			}
			live.Generations[generationID] = remaining
		}
	} else {
		for _, generationID := range pack.SourceGenerationIDs {
			live.Generations[generationID] = leafGenerationLiveTotals{}
		}
	}

	createdGenerations := make(map[uint64]struct{})
	view := snap.state.LeafGenerations
	for _, rawFileID := range pack.CreatedFileIDs {
		if generationID := view.FileToGeneration[rawFileID]; generationID != 0 {
			createdGenerations[generationID] = struct{}{}
		}
	}
	for generationID := range createdGenerations {
		generation := view.Generations[generationID]
		var bytesTotal int64
		for _, rawFileID := range generation.FileIDs {
			bytesTotal += leafGenerationRawFilePhysicalSize(db.dir, snap.state.ValueLogSet, rawFileID)
		}
		live.Generations[generationID] = leafGenerationLiveTotals{
			LivePages: max(1, len(generation.FileIDs)),
			LiveBytes: bytesTotal,
		}
	}

	key, ok := leafGenerationLiveStatsKeyForState(result.publishedState)
	if !ok {
		state.invalidate()
		return nil
	}
	key.protectedRoots = state.key.protectedRoots
	state.key = key
	state.live = live
	state.valid = true
	return nil
}

func (db *DB) compactStorageLeafGenerationPackRunOnce(ctx context.Context, opts LeafGenerationPackFromPlanOptions, lockMaintenance bool, ignoredRawFileIDs map[uint32]struct{}, carry *compactStorageLeafPackPlanState) (LeafGenerationPackRunOnceStats, error) {
	return db.compactStorageLeafGenerationPackRunOnceWithOwner(ctx, opts, lockMaintenance, ignoredRawFileIDs, carry, nil)
}

func (db *DB) compactStorageLeafGenerationPackRunOnceWithOwner(ctx context.Context, opts LeafGenerationPackFromPlanOptions, lockMaintenance bool, ignoredRawFileIDs map[uint32]struct{}, carry *compactStorageLeafPackPlanState, owner *transactionowner.Owner) (LeafGenerationPackRunOnceStats, error) {
	var stats LeafGenerationPackRunOnceStats
	plan, err := db.compactStorageLeafGenerationPlan(ctx, leafGenerationPackFromPlanPlanOptions(opts), carry)
	if err != nil {
		return stats, err
	}
	plan = compactStorageFilterIgnoredLeafPackPlan(plan, opts, ignoredRawFileIDs)
	stats.Plan = plan
	if plan.Admission != leafGenerationPlanAdmissionEligible {
		stats.SkipReason = fmt.Sprintf("plan_admission:%s", plan.Admission)
		return stats, nil
	}
	selection, err := SelectLeafGenerationPackCandidates(plan, leafGenerationPackFromPlanSelectOptions(opts))
	if err != nil {
		stats.SkipReason = fmt.Sprintf("selection:%v", err)
		return stats, nil
	}
	stats.Selection = selection
	var carryResult leafGenerationPackCarryResult
	packOpts := leafGenerationPackFromPlanPackOptions(opts, selection.GenerationIDs)
	packOpts.privateCOWOwner = owner
	packOpts.skipLeafPageLogAdvance = true
	packStats, err := db.leafGenerationPackSelectedWithCarry(ctx, packOpts, selectedLeafGenerationPackPlan(selection), lockMaintenance, &carryResult)
	if err != nil {
		carry.invalidate()
		return stats, err
	}
	stats.Pack = packStats
	stats.Ran = true
	return stats, carry.advance(db, packStats, carryResult)
}

func compactStorageFilterIgnoredLeafPackPlan(plan LeafGenerationPlan, opts LeafGenerationPackFromPlanOptions, ignoredRawFileIDs map[uint32]struct{}) LeafGenerationPlan {
	if len(ignoredRawFileIDs) == 0 || len(plan.Candidates) == 0 {
		return plan
	}
	ignoredGenerationIDs := make(map[uint64]struct{})
	out := plan
	out.Generations = append([]LeafGenerationPlanGeneration(nil), plan.Generations...)
	for i := range out.Generations {
		if !out.Generations[i].Eligible || !compactStorageLeafGenerationFilesIgnored(out.Generations[i], ignoredRawFileIDs) {
			continue
		}
		ignoredGenerationIDs[out.Generations[i].GenerationID] = struct{}{}
		out.Generations[i].Eligible = false
		out.Generations[i].SkipReason = compactStorageLeafPackSkipRunCreated
	}
	out.Candidates = make([]LeafGenerationPlanGeneration, 0, len(plan.Candidates))
	out.CandidateGenerationIDs = make([]uint64, 0, len(plan.Candidates))
	out.CandidateBytesTotal = 0
	out.CandidateBytesLive = 0
	out.CandidateBytesDead = 0
	out.CandidateBytesToCopy = 0
	out.CandidateLivePages = 0
	for _, gen := range plan.Candidates {
		if _, ignored := ignoredGenerationIDs[gen.GenerationID]; ignored {
			continue
		}
		out.Candidates = append(out.Candidates, gen)
		out.CandidateGenerationIDs = append(out.CandidateGenerationIDs, gen.GenerationID)
		out.CandidateBytesTotal += gen.BytesTotal
		out.CandidateBytesLive += gen.BytesLive
		out.CandidateBytesDead += gen.BytesDead
		out.CandidateBytesToCopy += gen.BytesToCopy
		out.CandidateLivePages += gen.LivePages
	}
	out.ExpectedReclaimBytes = out.CandidateBytesDead
	out.ExpectedReclaimRatioPPM = ratioPPM(out.CandidateBytesDead, out.CandidateBytesTotal)
	out.ExpectedReclaimPerByteCopiedPPM = ratioPPM(out.CandidateBytesDead, out.CandidateBytesToCopy)
	out.Admission = leafGenerationPlanAdmission(leafGenerationPackFromPlanPlanOptions(opts), out)
	return out
}

func compactStorageLeafGenerationFilesIgnored(gen LeafGenerationPlanGeneration, ignoredRawFileIDs map[uint32]struct{}) bool {
	if len(gen.FileIDs) == 0 {
		return false
	}
	for _, fileID := range gen.FileIDs {
		if _, ok := ignoredRawFileIDs[fileID]; !ok {
			return false
		}
	}
	return true
}

func compactStorageRememberLeafPackCreatedFileIDs(ignoredRawFileIDs map[uint32]struct{}, pack LeafGenerationPackRunOnceStats) {
	if len(pack.Pack.CreatedFileIDs) == 0 {
		return
	}
	for _, fileID := range pack.Pack.CreatedFileIDs {
		if fileID != 0 {
			ignoredRawFileIDs[fileID] = struct{}{}
		}
	}
}

func (db *DB) populateCompactStorageAudit(ctx context.Context, opts CompactStorageOptions, stats *CompactStorageStats, lockMaintenance bool, fencedIDsOut *[]uint32, ignoredLeafPackRawFileIDs map[uint32]struct{}, session *compactStorageAuditSession) (CompactStorageDebt, error) {
	var debt CompactStorageDebt
	_ = lockMaintenance
	in, raw, auditStats, err := db.compactStorageSharedAudit(ctx, opts, session)
	if err != nil {
		return debt, err
	}
	defer in.close()
	stats.Audit = auditStats
	protectedPaths := in.protectedPaths
	set := in.snap.state.ValueLogSet
	rewritePlan := db.compactStorageRewritePlanFromAudit(
		compactStorageRewritePlanOptions(opts, protectedPaths),
		set,
		raw.valueLogLiveBytesBySegment,
	)
	stats.ValueLogRewritePlan = rewritePlan
	debt.ValueLogRewriteSegments = rewritePlan.SegmentsSelected
	debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesStale
	if debt.ValueLogRewriteBytes == 0 {
		debt.ValueLogRewriteBytes = rewritePlan.SelectedBytesTotal
	}

	valueGC, err := db.compactStorageValueLogGCFromAudit(ctx, set, raw.valueLogReferencedSegments, protectedPaths)
	if err != nil {
		return debt, err
	}
	stats.ValueLogGC = valueGC
	debt.ValueLogGCSegments = valueGC.SegmentsEligible
	debt.ValueLogGCBytes = valueGC.BytesEligible
	fencedValueLogIDs, fencedValueLogBytes := db.compactStorageFencedUnreferencedFromAudit(opts, set, raw.valueLogReferencedSegments, protectedPaths)
	if fencedIDsOut != nil {
		*fencedIDsOut = append((*fencedIDsOut)[:0], fencedValueLogIDs...)
	}
	debt.ValueLogGCSegments += len(fencedValueLogIDs)
	debt.ValueLogGCBytes += fencedValueLogBytes

	protectedRootIDs, protectedSystemRootIDs := in.protectedRootIDs, in.protectedSystemRootIDs
	leafPackOpts := compactStorageLeafPackFromPlanOptions(opts, protectedRootIDs, protectedSystemRootIDs)
	leafPlan := db.compactStorageLeafGenerationPlanFromAudit(leafGenerationPackFromPlanPlanOptions(leafPackOpts), in, raw.leafGenerationLive)
	leafPlan = compactStorageFilterIgnoredLeafPackPlan(leafPlan, leafPackOpts, ignoredLeafPackRawFileIDs)
	stats.LeafGenerationPlan = leafPlan
	leafPackGenerations, leafPackBytes, err := compactStorageLeafPackDebtFromPlan(leafPlan, leafPackOpts)
	if err != nil {
		return debt, err
	}
	debt.LeafPackGenerations = leafPackGenerations
	debt.LeafPackBytes = leafPackBytes

	leafGC, err := db.compactStorageLeafGenerationGCFromAudit(ctx, in, raw.leafGenerationLive)
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
		currentPaths, currentIDs, err := db.currentValueLogProtectedRefs()
		if err != nil {
			return debt, err
		}
		allProtectedPaths := mergeUniqueNonEmptyPaths(protectedPaths, currentPaths)
		zeroBytes, err := zeroByteValueLogFilesFromUsage(usage, allProtectedPaths, currentIDs)
		if err != nil {
			return debt, err
		}
		debt.ZeroByteValueLogFiles = zeroBytes
	}
	return debt, nil
}

const (
	compactStorageIndexUserSpanRatioPPM               uint64 = 1_200_000
	compactStorageIndexFreelistReclaimablePages       uint64 = 64
	compactStorageIndexFreelistReclaimablePPM         uint64 = 250_000
	compactStorageIndexCollectionRootPages            uint64 = 16
	compactStorageIndexCollectionRootSpanRatioPPM     uint64 = 1_200_000
	compactStorageIndexWholeFileMinTotalPages         uint64 = 256
	compactStorageIndexWholeFileMetaOverheadPages     uint64 = 64
	compactStorageIndexWholeFileMinExcessPages        uint64 = 64
	compactStorageIndexWholeFileSlackRatioPPM         uint64 = 1_250_000
	compactStorageIndexCollectionLeafFillMinLeaves    uint64 = 256
	compactStorageIndexCollectionEmptyLeafMinPages    uint64 = 64
	compactStorageIndexCollectionUnderfullLeafMinPage uint64 = 256
)

func compactStorageExpectedPostVacuumPages(report IndexVacuumTriggerReport, slotCopies uint64) uint64 {
	livePages := report.UserPages + report.CollectionRootPages + report.SystemPages
	if livePages == 0 {
		return 0
	}
	if slotCopies == 0 {
		slotCopies = 1
	}
	chunkPages := report.ReplacementChunkPages
	if chunkPages == 0 {
		chunkPages = uint64(defaultChunkSize / page.PageSize)
	}
	raw := slotCopies*livePages + compactStorageIndexWholeFileMetaOverheadPages
	return ((raw + chunkPages - 1) / chunkPages) * chunkPages
}

func compactStorageRatioAtLeastPPM(num, denom, ratioPPM uint64) bool {
	hiL, loL := bits.Mul64(num, 1_000_000)
	hiR, loR := bits.Mul64(denom, ratioPPM)
	return hiL > hiR || (hiL == hiR && loL >= loR)
}

func compactStorageCollectionLeafFillUnderfullDebt(report IndexVacuumTriggerReport) bool {
	if report.CollectionRootLeafPages < compactStorageIndexCollectionLeafFillMinLeaves {
		return false
	}
	if report.CollectionRootEmptyLeafPages >= compactStorageIndexCollectionEmptyLeafMinPages &&
		report.CollectionRootEmptyLeafPages*10 >= report.CollectionRootLeafPages {
		return true
	}
	return report.CollectionRootUnderfullLeafPages >= compactStorageIndexCollectionUnderfullLeafMinPage &&
		report.CollectionRootUnderfullLeafPages*10 >= report.CollectionRootLeafPages*4
}

func populateCompactStorageIndexDebt(opts CompactStorageOptions, report IndexVacuumTriggerReport, debt *CompactStorageDebt) {
	populateCompactStorageIndexDebtWithSlotCopies(opts, report, 1, debt)
}

func populateCompactStorageIndexDebtWithSlotCopies(opts CompactStorageOptions, report IndexVacuumTriggerReport, slotCopies uint64, debt *CompactStorageDebt) {
	if debt == nil {
		return
	}
	debt.IndexVacuumReason = "none"
	debt.IndexVacuumTotalPages = report.TotalPages
	debt.IndexVacuumUserPages = report.UserPages
	debt.IndexVacuumUserSpan = report.UserSpan
	debt.IndexVacuumUserSpanRatioPPM = report.UserSpanRatioPPM
	debt.IndexVacuumFreelistReclaimablePages = report.FreelistReclaimablePages
	debt.IndexVacuumFreelistReclaimableRatioPPM = report.FreelistReclaimableRatioPPM
	debt.IndexVacuumCollectionRootPages = report.CollectionRootPages
	debt.IndexVacuumCollectionRootSpan = report.CollectionRootSpan
	debt.IndexVacuumCollectionRootSpanRatioPPM = report.CollectionRootSpanRatioPPM

	expectedPostVacuumPages := compactStorageExpectedPostVacuumPages(report, slotCopies)

	if opts.Mode == CompactStorageExhaustive {
		switch {
		case report.FreelistReclaimablePages > 0:
			debt.IndexVacuumRequired = true
			debt.IndexVacuumReason = "freelist"
		case report.UserPages > 0 && report.UserSpan > report.UserPages:
			debt.IndexVacuumRequired = true
			debt.IndexVacuumReason = "user"
		case report.CollectionRootPages > 0 && report.CollectionRootSpan > report.CollectionRootPages:
			debt.IndexVacuumRequired = true
			debt.IndexVacuumReason = "collection_roots"
		case report.CollectionRootEmptyLeafPages > 0 || compactStorageCollectionLeafFillUnderfullDebt(report):
			debt.IndexVacuumRequired = true
			debt.IndexVacuumReason = "collection_leaf_fill"
		case expectedPostVacuumPages > 0 &&
			report.TotalPages >= compactStorageIndexWholeFileMinTotalPages &&
			report.TotalPages > expectedPostVacuumPages+compactStorageIndexWholeFileMinExcessPages:
			debt.IndexVacuumRequired = true
			debt.IndexVacuumReason = "whole_file_slack"
		}
		return
	}

	switch {
	case report.UserPages > 0 && report.UserSpanRatioPPM >= compactStorageIndexUserSpanRatioPPM:
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "user"
	case report.FreelistReclaimableValid &&
		report.FreelistReclaimablePages >= compactStorageIndexFreelistReclaimablePages &&
		report.FreelistReclaimableRatioPPM >= compactStorageIndexFreelistReclaimablePPM:
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "freelist"
	case report.CollectionRootSpanRatioValid &&
		report.CollectionRootPages >= compactStorageIndexCollectionRootPages &&
		report.CollectionRootSpanRatioPPM >= compactStorageIndexCollectionRootSpanRatioPPM:
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "collection_roots"
	case expectedPostVacuumPages > 0 &&
		report.TotalPages >= compactStorageIndexWholeFileMinTotalPages &&
		report.TotalPages >= expectedPostVacuumPages+compactStorageIndexWholeFileMinExcessPages &&
		compactStorageRatioAtLeastPPM(report.TotalPages, expectedPostVacuumPages, compactStorageIndexWholeFileSlackRatioPPM):
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "whole_file_slack"
	case compactStorageCollectionLeafFillUnderfullDebt(report):
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "collection_leaf_fill"
	}
}

func mergeCompactStorageIndexDebt(dst *CompactStorageDebt, src CompactStorageDebt) {
	if dst == nil {
		return
	}
	dst.IndexVacuumRequired = src.IndexVacuumRequired
	dst.IndexVacuumReason = src.IndexVacuumReason
	dst.IndexVacuumTotalPages = src.IndexVacuumTotalPages
	dst.IndexVacuumUserPages = src.IndexVacuumUserPages
	dst.IndexVacuumUserSpan = src.IndexVacuumUserSpan
	dst.IndexVacuumUserSpanRatioPPM = src.IndexVacuumUserSpanRatioPPM
	dst.IndexVacuumFreelistReclaimablePages = src.IndexVacuumFreelistReclaimablePages
	dst.IndexVacuumFreelistReclaimableRatioPPM = src.IndexVacuumFreelistReclaimableRatioPPM
	dst.IndexVacuumCollectionRootPages = src.IndexVacuumCollectionRootPages
	dst.IndexVacuumCollectionRootSpan = src.IndexVacuumCollectionRootSpan
	dst.IndexVacuumCollectionRootSpanRatioPPM = src.IndexVacuumCollectionRootSpanRatioPPM
}

func compactStorageApplyLeafGenerationIndexDebt(debt *CompactStorageDebt, leafGC LeafGenerationGCStats) {
	if !debt.IndexVacuumRequired && leafGC.GenerationsRetiring > 0 {
		debt.IndexVacuumRequired = true
		debt.IndexVacuumReason = "leaf_generation"
	}
}

func (db *DB) compactStorageFencedUnreferencedValueLogIDs(ctx context.Context, opts CompactStorageOptions) ([]uint32, int64, error) {
	if db == nil || !opts.UnsafeValueLogReclaimFencedUnreferenced || db.valueLogManager == nil {
		return nil, 0, nil
	}
	referenced, err := db.compactStorageReferencedValueLogRefs(ctx)
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

func (db *DB) compactStorageReferencedValueLogRefs(ctx context.Context) (map[uint32]struct{}, error) {
	refs, source, err := db.referencedValueLogSegmentsWithSource(ctx)
	if err != nil {
		return nil, err
	}
	if db != nil && db.compactStorageFencedValueLogRefHook != nil {
		db.compactStorageFencedValueLogRefHook(compactStorageFencedValueLogRefEvent{
			Source:             source,
			ReferencedSegments: len(refs),
		})
	}
	return refs, nil
}

// Reclamation can conservatively refuse a stale root capability before any
// destructive mutation. Preserve those candidates and report deferred work;
// publisher contention is not a failure of the remaining compaction phases.
func (db *DB) runCompactStorageReclamationPhase(stats *CompactStorageStats, name string, fn func() error) error {
	err := db.runCompactStoragePhase(stats, name, fn)
	if compactStorageOnlyStaleRootError(err) {
		phase := &stats.Phases[len(stats.Phases)-1]
		phase.Status = CompactStoragePhaseStatusDeferred
		phase.Reason = err.Error()
		phase.Skipped = true
		phase.SkipReason = err.Error()
		return nil
	}
	return err
}

// A stale capability can be joined with a real release or publication failure.
// Defer only when every underlying cause is the contention sentinel.
func compactStorageOnlyStaleRootError(err error) bool {
	if err == ErrRecoverableRootSetStale {
		return true
	}
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		causes := cause.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, child := range causes {
			if !compactStorageOnlyStaleRootError(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return compactStorageOnlyStaleRootError(cause.Unwrap())
	default:
		return false
	}
}

func (db *DB) runCompactStoragePhase(stats *CompactStorageStats, name string, fn func() error) error {
	if db != nil && db.compactStorageBeforePhase != nil {
		db.compactStorageBeforePhase(name)
	}
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
			// Identity-based deletion quarantines a segment before removing it.
			// A released quarantine directory can therefore disappear after
			// WalkDir enumerates it but before WalkDir reads its children. That
			// completed deletion should simply be absent from this usage snapshot.
			if compactStorageConcurrentChildDeletion(usage.Path, path, walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if compactStorageConcurrentChildDeletion(usage.Path, path, err) {
				return nil
			}
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

func compactStorageConcurrentChildDeletion(root, path string, err error) bool {
	return path != root && os.IsNotExist(err)
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
	rootIDs, systemRootIDs := compactStorageOptionProtectedRootIDPair(opts)
	dynamicRootIDs, dynamicSystemRootIDs := db.protectedLeafGenerationRootIDPairFromLeafPageLog()
	rootIDs = appendCompactStorageProtectedRootIDs(rootIDs, dynamicRootIDs)
	systemRootIDs = appendCompactStorageProtectedRootIDs(systemRootIDs, dynamicSystemRootIDs)
	return rootIDs, systemRootIDs
}

func compactStorageOptionProtectedRootIDPair(opts CompactStorageOptions) ([]uint64, []uint64) {
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
	recoverableRoots, err := db.captureRecoverableRootSetWithMaintenanceLockHeld(context.Background())
	if err != nil {
		return 0, err
	}
	defer recoverableRoots.Release()
	db.publishPrepareMu.Lock()
	defer db.publishPrepareMu.Unlock()
	if err := recoverableRoots.revalidateWithPublishPrepareLockHeld(); err != nil {
		return 0, err
	}
	// No root publication can advance while publishPrepareMu is held. Consume
	// the capability so exact topology pins do not themselves prevent removal
	// of a zero-byte file proven outside every recoverable root.
	recoverableRoots.Release()

	layout := resolveStorageLayout(db.dir)
	entries, err := os.ReadDir(layout.valueVLogDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	currentPaths, currentIDs, err := db.currentValueLogProtectedRefs()
	if err != nil {
		return 0, err
	}
	protectedPaths = mergeUniqueNonEmptyPaths(protectedPaths, currentPaths)
	protected := compactStorageProtectedPathSet(protectedPaths)
	protectedFileIDs := compactStorageProtectedFileIDSet(protectedPaths, currentIDs)
	deleted := 0
	deletionNamespaceDirty := false
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
			if compactStorageConcurrentChildDeletion(layout.valueVLogDir, path, err) {
				continue
			}
			return deleted, err
		}
		if info.Size() != 0 {
			continue
		}
		if db.valueLogManager != nil {
			if fileID, ok := compactStorageValueLogFileID(name); ok {
				if err := db.valueLogManager.RegisterSegment(path, fileID); err != nil {
					if os.IsNotExist(err) {
						continue
					}
					return deleted, err
				}
				tracked, _, err := db.valueLogManager.MarkZombieIfTracked(fileID)
				if err != nil {
					return deleted, err
				}
				if tracked {
					if err := db.publishValueLogSetNoRefresh(); err != nil {
						return deleted, err
					}
					var removed bool
					var removeErr error
					if hook := db.testCompactStorageRemoveValueLogSegmentHook; hook != nil {
						removed, removeErr = hook(fileID)
					} else {
						removed, removeErr = db.valueLogManager.RemoveSegmentIfUnpinned(fileID)
					}
					if removed {
						deleted++
						deletionNamespaceDirty = true
					}
					if removeErr != nil {
						var syncErr error
						if deletionNamespaceDirty {
							syncErr = db.syncDeletionNamespaceDirectoryOrPoison(
								layout.valueVLogDir,
								durabilitycut.ResourceValueLog,
								"compact storage: sync value-log deletion namespace",
							)
						}
						return deleted, errors.Join(removeErr, ErrRecoveryRequired, syncErr)
					}
					if removed {
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
		removed, err := removePersistentFile(layout.valueVLogDir, path, durabilitycut.ResourceValueLog)
		if err != nil {
			if compactStorageIsBusyRemoveError(err) {
				continue
			}
			return deleted, err
		}
		if !removed {
			continue
		}
		deleted++
		deletionNamespaceDirty = true
	}
	if deletionNamespaceDirty {
		if err := db.syncDeletionNamespaceDirectoryOrPoison(
			layout.valueVLogDir,
			durabilitycut.ResourceValueLog,
			"compact storage: sync value-log deletion namespace",
		); err != nil {
			return deleted, err
		}
	}
	if deleted > 0 {
		if db.valueLogManager != nil {
			if err := db.valueLogManager.Refresh(); err != nil {
				return deleted, err
			}
		}
	}
	return deleted, nil
}

func (db *DB) currentValueLogProtectedRefs() ([]string, []uint32, error) {
	if db == nil {
		return nil, nil, nil
	}
	var fileIDs []uint32
	if db.valueLogManager != nil {
		fileIDs = append(fileIDs, db.valueLogManager.CurrentWritableFileIDs()...)
	}
	appender := db.currentValueLogAppender()
	if appender == nil {
		return nil, fileIDs, nil
	}
	segments, err := valueLogAppenderCurrentSegments(appender)
	if err != nil {
		return nil, nil, fmt.Errorf("list current value-log appender segments: %w", err)
	}
	paths := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment.Path != "" {
			paths = append(paths, segment.Path)
		}
		if segment.FileID != 0 {
			fileIDs = append(fileIDs, segment.FileID)
		}
	}
	return paths, fileIDs, nil
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
			if compactStorageConcurrentChildDeletion(dir, path, err) {
				continue
			}
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

type compactStorageLeafPageLogHandoff struct {
	db                  *DB
	writer              *rewriteWriter
	previousLeafPageLog LeafPageLog
	done                bool
}

var compactStorageLeafPageLogHandoffCloseWriter = func(w *rewriteWriter) error {
	return w.Close()
}

var compactStorageLeafPageLogHandoffListSegments = listValueLogSegments

func (h *compactStorageLeafPageLogHandoff) cleanup() error {
	if h == nil || h.db == nil || h.writer == nil {
		return nil
	}
	if h.done {
		return nil
	}
	h.done = true
	var cleanupErr error
	if err := compactStorageLeafPageLogHandoffCloseWriter(h.writer); err != nil {
		cleanupErr = errors.Join(cleanupErr, compactStorageLeafPageLogHandoffError("close compact writer", err))
		h.db.setLeafPageLogRaw(nil)
		return cleanupErr
	}
	if h.previousLeafPageLog == nil {
		h.db.setLeafPageLogRaw(nil)
		return cleanupErr
	}
	segments, err := compactStorageLeafPageLogHandoffListSegments(h.db.dir)
	if err != nil {
		h.db.setLeafPageLogRaw(nil)
		return errors.Join(cleanupErr, compactStorageLeafPageLogHandoffError("scan compact leaf segments", err))
	}
	leafSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	if err := compactStorageAdvanceLeafPageLogSeqAtLeast(h.previousLeafPageLog, leafSeq); err != nil {
		h.db.setLeafPageLogRaw(nil)
		return errors.Join(cleanupErr, compactStorageLeafPageLogHandoffError("restore previous owner", err))
	}
	h.db.setLeafPageLogRaw(h.previousLeafPageLog)
	return cleanupErr
}

func compactStorageLeafPageLogHandoffError(stage string, err error) error {
	return &CompactStorageLeafPageLogHandoffError{
		Stage:    stage,
		Recovery: "close and reopen the database before resuming writes",
		Err:      err,
	}
}

func (db *DB) installCompactStorageLeafPageLog(opts CompactStorageOptions) (*rewriteWriter, *compactStorageLeafPageLogHandoff, error) {
	if db == nil || !db.indexOuterLeavesInValueLog {
		return nil, nil, nil
	}
	previousLeafPageLog := db.leafPageLog
	if previousLeafPageLog != nil {
		if opts.Mode != CompactStorageExhaustive || !compactStorageReplaceableLeafPageLog(previousLeafPageLog) {
			return nil, nil, nil
		}
	}
	segments, err := rewriteWALSegmentsLister(db.dir)
	if err != nil {
		return nil, nil, err
	}
	leafStartSeq := maxRewriteLaneSeq(segments, rewriteLeafLogLaneID)
	writer := newRewriteWriter(ValueLogDirPath(db.dir), 0, 0, 0, db.valueLogIdentityPins)
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
	db.SetLeafPageLog(writer)
	if writer.blockCompression {
		leafDictID, leafDictBytes, leafDictUseRawPages, err := db.prepareCurrentRewriteLeafDict()
		if err != nil {
			db.SetLeafPageLog(previousLeafPageLog)
			return nil, nil, errors.Join(err, writer.Close())
		}
		if leafDictID != 0 && len(leafDictBytes) > 0 {
			writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
		}
	}
	return writer, &compactStorageLeafPageLogHandoff{
		db:                  db,
		writer:              writer,
		previousLeafPageLog: previousLeafPageLog,
	}, nil
}

func compactStorageReplaceableLeafPageLog(log LeafPageLog) bool {
	classification := compactStorageClassifyLeafPageLogOwner(log, CompactStorageLifecycleExclusiveMaintenance)
	return classification.Replaceable && classification.Status == CompactStorageOwnerStatusSupportedTarget
}

// CompactStorageLeafPageLogOwnerClassification reports the compact-owner
// support category for the currently installed leaf-page log. The lifecycle
// argument is a modeled assumption used by the contract matrix; it does not
// prove the database is actually in that lifecycle state.
func (db *DB) CompactStorageLeafPageLogOwnerClassification(lifecycle CompactStorageLifecycleState) CompactStorageLeafPageLogOwnerClassification {
	if db == nil {
		return compactStorageClassifyLeafPageLogOwner(nil, lifecycle)
	}
	db.writeMu.Lock()
	log := db.leafPageLog
	db.writeMu.Unlock()
	return compactStorageClassifyLeafPageLogOwner(log, lifecycle)
}

func compactStorageClassifyLeafPageLogOwner(log LeafPageLog, lifecycle CompactStorageLifecycleState) CompactStorageLeafPageLogOwnerClassification {
	if lifecycle == "" {
		lifecycle = CompactStorageLifecycleExclusiveMaintenance
	}
	ownerClass := compactStorageLeafPageLogOwnerClass(log)
	classification := CompactStorageLeafPageLogOwnerClassification{
		OwnerClass: ownerClass,
		Lifecycle:  lifecycle,
	}
	switch ownerClass {
	case CompactStorageLeafPageLogOwnerNone:
		classification.Status = CompactStorageOwnerStatusSupportedTarget
		classification.Replaceable = true
	case CompactStorageLeafPageLogOwnerCommandWALReplayInline:
		if lifecycle == CompactStorageLifecycleActiveWriter {
			classification.Status = CompactStorageOwnerStatusLiveWriterFailClosed
			classification.RequiresQuiescence = true
			classification.Detail = "modeled active-writer status: command-WAL cleanup and checkpoint/close drains must quiesce before exhaustive compact can replace the replay-inline owner"
		} else if !compactStorageLeafPageLogHandoffCapable(log) {
			classification.Status = CompactStorageOwnerStatusBlockingBug
			classification.Detail = "command-WAL replay-inline owner does not expose the compact handoff restore capability"
		} else {
			classification.Status = CompactStorageOwnerStatusSupportedTarget
			classification.Replaceable = true
		}
	case CompactStorageLeafPageLogOwnerInternalHiddenByWrapper:
		if lifecycle == CompactStorageLifecycleActiveWriter {
			classification.Status = CompactStorageOwnerStatusLiveWriterFailClosed
			classification.RequiresQuiescence = true
			classification.Detail = "modeled active-writer status: lane/wrapper replay-inline owner must be quiesced before exhaustive compact can reason about replacement"
		} else if !compactStorageLeafPageLogHandoffCapable(log) {
			classification.Status = CompactStorageOwnerStatusBlockingBug
			classification.Detail = "replay-inline owner is internally owned but hidden behind wrappers without a compact handoff restore capability"
		} else {
			classification.Status = CompactStorageOwnerStatusSupportedTarget
			classification.Replaceable = true
			classification.Detail = "internally owned replay-inline wrapper exposes a compact handoff restore capability"
		}
	case CompactStorageLeafPageLogOwnerCachedWrapper:
		classification.RequiresQuiescence = true
		if lifecycle == CompactStorageLifecycleActiveWriter {
			classification.Status = CompactStorageOwnerStatusLiveWriterFailClosed
			classification.Detail = "modeled active-writer status: background flush/apply workers, checkpoint/close drains, and cached backlog must be fenced before exhaustive compact can take over the owner"
		} else if !compactStorageLeafPageLogHandoffCapable(log) {
			classification.Status = CompactStorageOwnerStatusBlockingBug
			classification.Detail = "cached/wrapper owner does not expose the compact handoff restore capability"
		} else {
			classification.Status = CompactStorageOwnerStatusLiveWriterFailClosed
			classification.Detail = "cached/wrapper owner has compact handoff support, but CompactStorage does not fence cached writes, background flush/apply workers, checkpoint/close drains, and cached backlog for the full exhaustive run"
		}
	default:
		classification.Status = CompactStorageOwnerStatusExternalUnsupported
		classification.Detail = "standalone caller-owned leaf logs remain outside exhaustive compact ownership"
	}
	return classification
}

func compactStorageLeafPageLogHandoffCapable(log LeafPageLog) bool {
	if log == nil {
		return true
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return compactStorageLeafPageLogHandoffCapable(wrapped.inner)
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		lanes, _ := group.snapshotLanesAndLocks()
		if len(lanes) == 0 {
			return false
		}
		seenLane := false
		for _, lane := range lanes {
			if lane == nil {
				continue
			}
			seenLane = true
			if !compactStorageLeafPageLogHandoffCapable(lane) {
				return false
			}
		}
		return seenLane
	}
	_, ok := log.(LeafPageLogCompactStorageHandoff)
	return ok
}

func compactStorageAdvanceLeafPageLogSeqAtLeast(log LeafPageLog, seq uint32) error {
	if log == nil || seq == 0 {
		return nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return compactStorageAdvanceLeafPageLogSeqAtLeast(wrapped.inner, seq)
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		return group.advanceCompactStorageLeafPageLogSeqAtLeast(seq)
	}
	handoff, ok := log.(LeafPageLogCompactStorageHandoff)
	if !ok {
		return fmt.Errorf("treedb: compact storage leaf-page-log owner %T cannot advance after handoff", log)
	}
	return handoff.AdvanceCompactStorageLeafPageLogSeqAtLeast(seq)
}

func detachSealedLeafPageLogSegments(log LeafPageLog, sealedRawFileIDs map[uint32]struct{}) error {
	if log == nil || len(sealedRawFileIDs) == 0 {
		return nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return detachSealedLeafPageLogSegments(wrapped.inner, sealedRawFileIDs)
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		return group.detachSealedLeafPageLogSegments(sealedRawFileIDs)
	}
	detacher, ok := log.(LeafPageLogSealedSegmentDetacher)
	if !ok {
		return nil
	}
	return detacher.DetachSealedLeafPageLogSegments(sealedRawFileIDs)
}

func (db *DB) demoteSealedLeafLogCurrentWritable(sealedRawFileIDs, stillCurrentRawFileIDs map[uint32]struct{}) {
	if db == nil || db.valueLogManager == nil || len(sealedRawFileIDs) == 0 {
		return
	}
	for _, id := range db.valueLogManager.CurrentWritableFileIDs() {
		lane, _ := valuelog.DecodeFileID(id)
		if lane != valuelog.ReservedLeafLogLaneID {
			continue
		}
		rawID := page.ValueLogSegmentID(id)
		if _, current := stillCurrentRawFileIDs[rawID]; current {
			continue
		}
		if _, sealed := sealedRawFileIDs[rawID]; sealed {
			db.valueLogManager.DemoteCurrentWritable(id)
		}
	}
}

func compactStorageLeafPageLogOwnerClass(log LeafPageLog) CompactStorageLeafPageLogOwnerClass {
	if log == nil {
		return CompactStorageLeafPageLogOwnerNone
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		return compactStorageLeafPageLogOwnerClass(wrapped.inner)
	}
	if _, ok := log.(replayInlineLeafPageLog); ok {
		return CompactStorageLeafPageLogOwnerCommandWALReplayInline
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		return compactStorageLeafPageLogLaneGroupOwnerClass(group)
	}
	if compactStorageLooksLikeCachedLeafPageLog(log) {
		return CompactStorageLeafPageLogOwnerCachedWrapper
	}
	return CompactStorageLeafPageLogOwnerStandaloneCallerExternal
}

func compactStorageLeafPageLogLaneGroupOwnerClass(group *leafPageLogLaneGroup) CompactStorageLeafPageLogOwnerClass {
	if group == nil {
		return CompactStorageLeafPageLogOwnerNone
	}
	lanes, _ := group.snapshotLanesAndLocks()
	if len(lanes) == 0 {
		return CompactStorageLeafPageLogOwnerStandaloneCallerExternal
	}
	allCommandWALReplayInline := true
	allExternal := true
	for _, lane := range lanes {
		switch compactStorageLeafPageLogOwnerClass(lane) {
		case CompactStorageLeafPageLogOwnerCommandWALReplayInline:
			allExternal = false
		case CompactStorageLeafPageLogOwnerCachedWrapper:
			return CompactStorageLeafPageLogOwnerCachedWrapper
		case CompactStorageLeafPageLogOwnerInternalHiddenByWrapper:
			allExternal = false
		case CompactStorageLeafPageLogOwnerStandaloneCallerExternal:
			allCommandWALReplayInline = false
		default:
			allCommandWALReplayInline = false
			allExternal = false
		}
	}
	if allCommandWALReplayInline {
		return CompactStorageLeafPageLogOwnerInternalHiddenByWrapper
	}
	if allExternal {
		return CompactStorageLeafPageLogOwnerStandaloneCallerExternal
	}
	return CompactStorageLeafPageLogOwnerInternalHiddenByWrapper
}

func compactStorageLooksLikeCachedLeafPageLog(log LeafPageLog) bool {
	if log == nil {
		return false
	}
	marker, ok := log.(LeafPageLogCachedWrapperOwner)
	return ok && marker.CompactStorageCachedWrapperOwner()
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
