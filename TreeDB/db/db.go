package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/keyupdate"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/template"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	MetaPage0ID = 0
	MetaPage1ID = 1
	KeepRecent  = 1

	closeSnapshotDrainTimeout = 10 * time.Second
	closeSnapshotDrainSleep   = 500 * time.Microsecond

	snapshotAcquireShardCount = 256
	snapshotAcquireShardMask  = snapshotAcquireShardCount - 1
)

type DBState struct {
	CommitSeq                  uint64
	RootPageID                 uint64
	SystemRootPageID           uint64
	ValueLogSet                *valuelog.Set
	LeafGenerations            *leafGenerationView
	LeafGenerationStateVersion uint64
}

// snapshotView is the coherent publication unit for snapshot acquisition.
// AcquireSnapshot reads this single pointer so idx/state/vlog manager always
// come from the same publish event.
type snapshotView struct {
	idx         *indexGen
	state       *DBState
	vlogManager *valuelog.Manager
}

type DB struct {
	valueLogManager                *valuelog.Manager
	snapshotViewRO                 atomic.Pointer[snapshotView]
	snapshotAcquireRO              [snapshotAcquireShardCount]atomic.Int32
	valueLogRefTracker             *valueLogRefTracker
	leafPageLog                    LeafPageLog
	leafPageReadCache              *leafPageReadCache
	leafGenerationManifest         *leafGenerationManifest
	leafGenerationPendingMu        sync.Mutex
	leafGenerationPendingFileIDs   []uint32
	leafGenerationPendingSet       map[uint32]struct{}
	leafGenerationPendingCommitSeq map[uint32]uint64
	lock                           *lockfile.Lock
	adaptive                       *adaptive.Controller
	pruner                         pruneWorker
	leafGenerationPins             leafGenerationPinTracker

	// idx is the current index generation (pager + MVCC lifecycle state).
	idx atomic.Pointer[indexGen]

	idxMu   sync.Mutex
	idxAll  map[uint64]*indexGen
	idxNext uint64

	snapPool     *SnapshotPool
	ghostManager *indexGhostManager

	dir                  string
	chunkSize            int64
	preferAppendAlloc    bool
	freelistRegionPages  uint64
	freelistRegionRadius int

	readOnly bool

	keepRecent                     uint64
	policy                         WritePolicy
	valueLogCompression            ValueLogCompressionMode
	valueLogAutoPolicy             ValueLogAutoPolicy
	valueLogBlockCodec             ValueLogBlockCodec
	valueLogDictLookup             valuelog.DictLookup
	valueLogDictCurrentForClass    func(context.Context, string) (uint64, error)
	valueLogDictLeafPayloadMode    func(context.Context, uint64) (bool, bool, error)
	valueLogDictPut                func(context.Context, []byte) (uint64, error)
	valueLogDictSetCurrentForClass func(context.Context, string, uint64) error
	valueLogDictSetLeafPayloadMode func(context.Context, uint64, bool) error
	valueLogDomainThresholds       []ValueLogDomainThreshold
	leafFillTargetPPM              uint32
	internalFillTargetPPM          uint32
	leafPrefixCompression          bool
	indexColumnarLeaves            bool
	indexPackedValuePtr            bool
	indexInternalBaseDelta         bool
	indexOuterLeavesInValueLog     bool
	indexAdaptiveLeafEncoding      bool
	piggybackCompaction            bool
	maintenanceOpsPerCoalesce      int
	zipperParallelMergeSource      zipper.ParallelMergePressureSource

	mu               sync.RWMutex
	writeMu          sync.RWMutex
	commitMu         sync.Mutex
	updateLocks      keyupdate.Locks
	maintenanceMu    sync.Mutex
	combineMu        sync.RWMutex
	combineReqCh     chan *commitCombineReq
	combineStopCh    chan struct{}
	combineDoneCh    chan struct{}
	vacuumInProgress atomic.Bool
	vacuum           vacuumRecorder
	meta             page.MetaPageBody
	metaPageID       uint64

	state atomic.Pointer[DBState]

	// readRetryRefresh* deduplicates read-triggered value-log refreshes
	// (ErrFileNotFound retry path) so concurrent readers share one refresh scan.
	readRetryRefreshMu            sync.Mutex
	readRetryRefreshInFlight      bool
	readRetryRefreshDone          chan struct{}
	readRetryRefreshErr           error
	readRetryRefreshEpoch         atomic.Uint64
	readRetryRefreshLeaderCount   atomic.Uint64
	readRetryRefreshFollowerCount atomic.Uint64
	readRetryRefreshSkippedEpoch  atomic.Uint64

	notifyError  func(error)
	bgErrMu      sync.Mutex
	bgErr        error
	closeHooksMu sync.Mutex
	closeHooks   []func() error
	// closeHooksClosed is set when close hook draining begins. Close hooks run
	// while writes are still available, so registrations after that point would
	// otherwise be silently lost.
	closeHooksClosed bool

	leafGenerationLiveStatsMu        sync.RWMutex
	leafGenerationLiveStatsCache     leafGenerationLiveStatsCache
	leafGenerationSubtreeStatsMu     sync.RWMutex
	leafGenerationSubtreeStatsByPage map[uint64]leafGenerationSubtreeStats
	leafGenerationRecordLengthMu     sync.RWMutex
	leafGenerationRecordLengthByFile map[uint32]*leafGenerationRecordLengthIndex
	leafGenerationStateVersion       uint64

	rewritePlanLiveBytesMu    sync.RWMutex
	rewritePlanLiveBytesCache valueLogRewriteLiveBytesCache

	// Stage-5 publish watermark metrics (backend commit publish path).
	publishWatermarkWaitTotalNs    atomic.Uint64
	publishWatermarkHoldTotalNs    atomic.Uint64
	publishWatermarkLatencySamples atomic.Uint64
	publishWatermarkLatencyMaxNs   atomic.Uint64
	publishWatermarkLatencyBuckets [publishWatermarkLatencyBucketCount]atomic.Uint64

	// Ordered-root delta groups are the collection multi-root publish hot path.
	orderedRootDeltaGroupCalls          atomic.Uint64
	orderedRootDeltaGroupErrors         atomic.Uint64
	orderedRootDeltaGroupRoots          atomic.Uint64
	orderedRootDeltaGroupWaitTotalNs    atomic.Uint64
	orderedRootDeltaGroupHoldTotalNs    atomic.Uint64
	orderedRootDeltaGroupLatencyMaxNs   atomic.Uint64
	orderedRootDeltaGroupLatencyBuckets [publishWatermarkLatencyBucketCount]atomic.Uint64

	// R4 warm-publish counters. Warm native apply is used for bounded deltas;
	// larger or ineligible deltas record an explicit rebuild fallback selection.
	systemRootWarmPublishAttempts         atomic.Uint64
	systemRootWarmNativeApplyAttempts     atomic.Uint64
	systemRootWarmPublishRebuildFallbacks atomic.Uint64
	systemRootWarmPreservedPages          atomic.Uint64
	systemRootWarmRewrittenPages          atomic.Uint64

	rootProbeKeyFallbackCalls    atomic.Uint64
	rootProbeKeyFallbackItems    atomic.Uint64
	rootProbePrefixFallbackCalls atomic.Uint64
	rootProbePrefixFallbackItems atomic.Uint64

	// testFailFinalizeCommit forces finalizeCommitLocked to fail before writing
	// the next meta page. Used by crash-safety tests.
	testFailFinalizeCommit        atomic.Bool
	testBatchCreateHook           func()
	testOrderedRootPublishHook    func(baseRoot uint64)
	testSystemRootWarmMaxDeltaOps int
	// testFailWriteMeta forces writeMeta to fail before mutating the target meta
	// page so tests can exercise pre-publish cleanup paths.
	testFailWriteMeta atomic.Bool
	closing           atomic.Bool
}

type valueLogRewriteLiveBytesKey struct {
	commitSeq  uint64
	rootID     uint64
	systemRoot uint64
}

type valueLogRewriteLiveBytesCache struct {
	key valueLogRewriteLiveBytesKey
	// liveByID is published by clone-and-replace and treated as immutable after
	// publication, so readers may snapshot the map header under RLock and clone
	// after unlocking without racing a writer mutating the same map.
	liveByID map[uint32]int64
}

type treeReachabilityCacheKey struct {
	commitSeq           uint64
	rootID              uint64
	systemRoot          uint64
	leafGenerationStamp uint64
}

type leafGenerationLiveStatsCache struct {
	key   treeReachabilityCacheKey
	stats leafGenerationLiveScanStats
	ok    bool
}

const (
	defaultChunkSize                 = 256 * 1024
	defaultMaintenanceOpsPerCoalesce = 400_000
)

const snapshotShardHintUnset = -1

var errTestFinalizeCommitFailpoint = errors.New("treedb: finalize commit failpoint")
var errTestWriteMetaFailpoint = errors.New("treedb: write meta failpoint")

type finalizeCommitError struct {
	err                        error
	cleanupCreatedSegmentsSafe bool
}

func (e *finalizeCommitError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *finalizeCommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func wrapFinalizeCommitError(err error, cleanupCreatedSegmentsSafe bool) error {
	if err == nil {
		return nil
	}
	return &finalizeCommitError{
		err:                        err,
		cleanupCreatedSegmentsSafe: cleanupCreatedSegmentsSafe,
	}
}

func finalizeCommitErrorAllowsCreatedSegmentCleanup(err error) bool {
	var commitErr *finalizeCommitError
	return errors.As(err, &commitErr) && commitErr.cleanupCreatedSegmentsSafe
}

// DurabilityMode configures cached-mode durability semantics.
//
// These modes are explicit and intentionally replace the previous boolean
// combination of DisableWAL + RelaxedSync + AllowUnsafe.
type DurabilityMode uint8

const (
	// DurabilityDurable enables WAL (journal) and uses fsync for sync operations.
	DurabilityDurable DurabilityMode = iota
	// DurabilityWALOnRelaxed keeps WAL enabled but disables fsync (crash-consistent).
	DurabilityWALOnRelaxed
	// DurabilityWALOffRelaxed disables WAL and fsync (unsafe; recent writes
	// may be lost and sync calls may defer backend publication until a later
	// checkpoint/flush boundary).
	DurabilityWALOffRelaxed
)

// IntegrityMode configures value-log read integrity checks.
//
// It intentionally replaces the previous DisableReadChecksum boolean.
type IntegrityMode uint8

const (
	// IntegrityVerify enables checksum verification on value-log reads.
	IntegrityVerify IntegrityMode = iota
	// IntegritySkipChecksums disables checksum verification on value-log reads (unsafe).
	IntegritySkipChecksums
)

// ValueLogCompressionMode selects value-log compression behavior in cached mode.
type ValueLogCompressionMode uint8

const (
	// ValueLogCompressionOff stores value-log grouped frames uncompressed.
	//
	// Zero is intentionally reserved as "unset/default".
	// db.Open normalizes zero to ValueLogCompressionAuto.
	ValueLogCompressionOff ValueLogCompressionMode = iota + 1
	// ValueLogCompressionBlock uses block compression without dictionaries.
	ValueLogCompressionBlock
	// ValueLogCompressionDict uses dictionary compression when available.
	ValueLogCompressionDict
	// ValueLogCompressionAuto adaptively chooses off/block/dict.
	ValueLogCompressionAuto
)

// ValueLogBlockCodec selects the block codec used for block compression modes.
type ValueLogBlockCodec uint8

const (
	ValueLogBlockSnappy ValueLogBlockCodec = iota
	ValueLogBlockLZ4
)

// ValueLogAutoPolicy controls auto-mode dict vs block selection bias.
type ValueLogAutoPolicy uint8

const (
	ValueLogAutoBalanced ValueLogAutoPolicy = iota
	ValueLogAutoThroughput
	ValueLogAutoSize
)

// ValueLogDictClassMode controls whether dictionary state is shared across all
// value-log payloads or split by payload class.
type ValueLogDictClassMode uint8

const (
	// ValueLogDictClassSingle keeps one shared dictionary stream for all
	// value-log payloads.
	ValueLogDictClassSingle ValueLogDictClassMode = iota
	// ValueLogDictClassSplitOuterLeaf keeps separate dictionary streams for
	// outer-leaf payloads and single-value payloads.
	ValueLogDictClassSplitOuterLeaf
)

// ValueLogGenerationPolicy controls generation-aware value-log placement.
// PR1 scaffolding: behavior remains legacy append-only until allocator/rewrite
// phases land; this policy is currently configuration + observability only.
type ValueLogGenerationPolicy uint8

const (
	// ValueLogGenerationDefault selects the library default (currently
	// hot/warm/cold in cached mode).
	//
	// This is intentionally the zero value so callers can opt into the default
	// behavior without explicitly setting a policy.
	ValueLogGenerationDefault ValueLogGenerationPolicy = iota
	// ValueLogGenerationOff keeps legacy single-generation behavior (no
	// background generation maintenance).
	ValueLogGenerationOff
	// ValueLogGenerationHotWarmCold enables hot/warm/cold generation policy.
	ValueLogGenerationHotWarmCold
)

// ValueLogGenerationConfig configures generational value-log behavior.
type ValueLogGenerationConfig struct {
	// Policy selects generation behavior. Off preserves current behavior.
	Policy ValueLogGenerationPolicy
	// LeafSegmentTargetBytes configures target segment size for leaf_vlog
	// generations when outer leaves are stored out-of-line.
	//
	// 0 uses the implementation default leaf-generation target.
	LeafSegmentTargetBytes int64
	// HotSegmentTargetBytes configures target segment size for hot generation.
	// 0 uses implementation default.
	HotSegmentTargetBytes int64
	// WarmSegmentTargetBytes configures target segment size for warm generation.
	// 0 uses implementation default.
	WarmSegmentTargetBytes int64
	// ColdSegmentTargetBytes configures target segment size for cold generation.
	// 0 uses implementation default.
	ColdSegmentTargetBytes int64
	// RewriteBudgetBytesPerSec bounds background incremental rewrite bandwidth.
	// 0 disables byte-budget trigger.
	RewriteBudgetBytesPerSec int64
	// RewriteBudgetRecordsPerSec bounds background incremental rewrite records/s.
	// 0 disables record-budget trigger.
	RewriteBudgetRecordsPerSec int
	// RewriteTriggerStaleRatioPPM triggers rewrite when stale/live ratio exceeds
	// threshold (parts-per-million, 0 disables).
	RewriteTriggerStaleRatioPPM uint32
	// RewriteTriggerTotalBytes triggers rewrite when total retained bytes exceeds
	// threshold (0 disables).
	RewriteTriggerTotalBytes int64
	// RewriteTriggerChurnPerSec triggers rewrite when churn rate exceeds
	// threshold (0 disables).
	RewriteTriggerChurnPerSec int64
	// RewriteMinSegmentAge gates online rewrite to source segments that are at
	// least this old.
	//
	// 0 uses the implementation default.
	RewriteMinSegmentAge time.Duration
}

// ValueLogDomainThreshold overrides inline-vs-pointer placement policy for keys
// under a domain prefix.
//
// A key belongs to the first matching prefix after normalization
// (longest-prefix wins).
type ValueLogDomainThreshold struct {
	// Prefix selects the key domain this override applies to.
	Prefix []byte
	// InlineThreshold is the maximum inline value size for keys in Prefix.
	// Values larger than this threshold are eligible for value-log pointers.
	// Zero forces all non-empty values in this domain to pointer placement.
	InlineThreshold int
}

// ValueLogOptions configures value-log pointer behavior and optional compression/dict tuning.
type ValueLogOptions struct {
	// Compression selects value-log compression behavior.
	Compression ValueLogCompressionMode
	// BlockCodec selects the block codec for block compression.
	BlockCodec ValueLogBlockCodec
	// BlockTargetCompressedBytes guides grouped block size adaptation.
	//
	// 0 uses a default.
	BlockTargetCompressedBytes int
	// IncompressibleHoldBytes configures auto-mode suppression duration after
	// repeated incompressible probes.
	//
	// 0 uses a default.
	IncompressibleHoldBytes int
	// IncompressibleProbeIntervalBytes controls probe cadence while
	// incompressible hold is active.
	//
	// 0 uses a default.
	IncompressibleProbeIntervalBytes int
	// AutoPolicy controls auto-mode bias (throughput, balanced, size).
	AutoPolicy ValueLogAutoPolicy
	// DictClassMode controls dictionary-state partitioning:
	// 0=single (default shared dict stream), 1=split_outer_leaf.
	DictClassMode ValueLogDictClassMode

	// PointerThreshold controls when value-log pointers are used.
	// Values <= 0 use a default threshold. In cached mode, relaxed durability
	// settings may choose a smaller default to avoid large-scale update cliffs by
	// pushing moderate values into the value log.
	PointerThreshold int
	// Generational configures generation-aware value-log placement and rewrite
	// scheduling. PR1 wires config and stats only; behavior remains legacy until
	// follow-on phases land.
	Generational ValueLogGenerationConfig
	// ForcePointers stores all values out-of-line in the value log (no inline values).
	ForcePointers bool
	// DomainInlineThresholds provides optional per-domain overrides for
	// inline-vs-pointer placement. These overrides are evaluated by
	// longest-prefix match and fall back to PointerThreshold/default behavior
	// when no domain matches.
	DomainInlineThresholds []ValueLogDomainThreshold
	// RawWritevMinAvgBytes controls raw grouped-frame writev usage.
	//
	// 0 enables adaptive mode (no average-bytes floor).
	RawWritevMinAvgBytes int
	// RawWritevMinBatchRecords controls minimum grouped records before raw writev
	// is considered.
	//
	// <=0 uses the default.
	RawWritevMinBatchRecords int

	// ReadIntegrity configures checksum verification on value-log reads.
	ReadIntegrity IntegrityMode

	// MaxRetainedBytes emits a warning when retained value-log bytes exceed this
	// threshold (0 disables warnings). Cached mode only.
	MaxRetainedBytes int64
	// MaxRetainedBytesHard disables value-log pointers for new large values once
	// retained bytes exceed this threshold (0 disables the cap).
	MaxRetainedBytesHard int64

	// DictLookup provides dictionary bytes for value-log decoding.
	DictLookup valuelog.DictLookup
	// DictCurrentForClass resolves the current dictionary ID for a payload class.
	// Offline/maintenance rewrite uses this to seed class-specific rewrite codecs.
	DictCurrentForClass func(context.Context, string) (uint64, error)
	// DictLeafPayloadMode reports whether a published leaf dictionary expects raw
	// 4KiB leaf pages (useRawPages=true) or compact split-leaf payloads
	// (useRawPages=false). The returned ok flag is false when no explicit mode is
	// recorded and callers should fall back to legacy defaults.
	DictLeafPayloadMode func(context.Context, uint64) (useRawPages bool, ok bool, err error)
	// DictPut persists dictionary bytes and returns the stable dictionary ID.
	// Offline/maintenance rewrite may use this to bootstrap a class-specific dict
	// before rewriting into dict-compressed frames.
	DictPut func(context.Context, []byte) (uint64, error)
	// DictSetCurrentForClass marks a dictionary ID as the current dict for the
	// provided payload class. Rewrite bootstrap uses this after publishing a new
	// class-specific dict.
	DictSetCurrentForClass func(context.Context, string, uint64) error
	// DictSetLeafPayloadMode records whether a published leaf dictionary expects
	// raw 4KiB leaf pages or compact split-leaf payloads.
	DictSetLeafPayloadMode func(context.Context, uint64, bool) error

	// DictTrain configures background dictionary training for value-log frame
	// compression in cached mode.
	DictTrain compression.TrainConfig
	// DictAdaptiveRatio enables best-effort adaptive disable/pause of value-log
	// dictionary compression when payload compression ratios degrade (0 disables).
	DictAdaptiveRatio float64
	// DictMetricsWindowBytes controls the rolling window size for ratio tracking (0=default).
	DictMetricsWindowBytes int
	// DictMetricsMinRecords controls how many records must be observed in a window
	// before adaptive pause triggers (0=default).
	DictMetricsMinRecords int
	// DictMetricsPauseBytes controls how long to pause dict compression after a degraded
	// window is detected (0=default).
	DictMetricsPauseBytes int
	// DictIncompressibleHoldBytes enables classifier-driven hold mode for
	// high-entropy streams. While hold mode is active, dict attempts and trainer
	// collection are bypassed until hold bytes are consumed.
	//
	// 0 uses profile/default hold configuration; <0 explicitly disables hold
	// mode and opts out of profile defaults.
	DictIncompressibleHoldBytes int
	// DictProbeIntervalBytes controls periodic probe attempts while
	// incompressible hold mode is active.
	//
	// <=0 uses a default derived from hold bytes.
	DictProbeIntervalBytes int
	// DictMinPayloadSavingsRatio rejects newly trained dictionaries whose payload
	// ratio does not improve by at least this fraction (0 uses a cached-mode
	// throughput-oriented default: 0.02 normally, 0.05 with ForcePointers or
	// WAL disabled).
	DictMinPayloadSavingsRatio float64
	// DictMaxK clamps the maximum group size (K) used for value-log dict-compressed
	// frames.
	//
	// Larger K can improve compression ratio (more cross-record matches) and can
	// reduce framing overhead, but may increase CPU and tail latency due to larger
	// encode/decode units.
	//
	// Values <= 0 use the default (32). Values above the engine maximum are clamped.
	DictMaxK int
	// DictFrameEncodeLevel controls the zstd encoder level used for dict-compressed
	// value-log frames.
	//
	// Values <= 0 use the default (SpeedFastest).
	DictFrameEncodeLevel zstd.EncoderLevel
	// DictFrameEnableEntropy enables entropy coding for dict-compressed value-log
	// frames (higher ratio, lower throughput).
	//
	// Default is false (throughput-focused: no-entropy compression).
	DictFrameEnableEntropy bool

	// CompressionAutotune configures the wall-time value-log compression autotuner.
	CompressionAutotune valuelog.AutotuneOptions

	// TemplateMode controls template-based compression for value-log values.
	TemplateMode template.Mode
	// TemplateConfig controls template creation and encoding behavior.
	TemplateConfig template.Config
	// TemplateReadStrict controls strict template decode behavior.
	TemplateReadStrict bool
	// TemplateStore provides template routing/definition lookups for template
	// encoding (for example in offline rewrite prepass experiments).
	TemplateStore template.Store
	// TemplateLookup provides template definition bytes for value-log decoding.
	TemplateLookup valuelog.TemplateLookup
	// TemplateDecodeOptions controls decode caps for template payloads.
	TemplateDecodeOptions template.DecodeOptions
}

type Options struct {
	Dir string
	// IgnoreFormatConfig disables best-effort persisted format.json loading in
	// TreeDB open paths that auto-apply index-format knobs from disk (e.g.
	// treedb.Open, treedb.OpenBackend) and in offline maintenance helpers
	// (VacuumIndexOffline, ValueLogRewriteOffline, treemap vacuum/rewrite/vlog-gc).
	IgnoreFormatConfig bool
	// ReadOnly opens the database without acquiring an exclusive lock and without
	// modifying on-disk state (no recovery truncation, no WAL replay, no background
	// maintenance). Only read operations are supported.
	ReadOnly  bool
	ChunkSize int64 // Default 256KiB
	// DictDBChunkSize controls the mmap chunk size used for the `dictdb/` side
	// store when TreeDB is opened via the public `treedb.Open` wrapper.
	//
	// It is intentionally independent of ChunkSize so benchmarks and callers can
	// tune the main index pager without inflating dictdb disk usage.
	//
	// Values <= 0 use a default of 64KiB.
	DictDBChunkSize int64
	// TemplateDBChunkSize controls the mmap chunk size used for the `templatedb/`
	// side store when template compression is enabled.
	//
	// Values <= 0 use a default of 64KiB.
	TemplateDBChunkSize int64
	KeepRecent          uint64 // Default 1
	// PagerSyncConcurrency controls how many goroutines may msync dirty chunks
	// in parallel during Sync. Values <= 0 use the default (1).
	PagerSyncConcurrency int
	// PagerMmapPopulate enables MAP_POPULATE on Linux when mmapping index.db
	// chunks. This can reduce minor-fault overhead under random access patterns
	// at the cost of increased work at map/grow time.
	PagerMmapPopulate bool
	// PagerPrefetchOnRead enables best-effort prefetch hints (madvise WILLNEED)
	// for mmapped index chunks (Linux only). When enabled, TreeDB may issue
	// prefetch requests opportunistically (e.g. before rewriting child pages
	// during checkpoint/merge). It is a no-op on unsupported platforms.
	PagerPrefetchOnRead bool

	// Durability configures cached-mode durability semantics.
	//
	// The default (zero) is DurabilityDurable.
	Durability DurabilityMode
	// DisableBackgroundPrune keeps pruning on the commit critical path (legacy
	// behavior). When false (default), a bounded background pruner frees pages
	// asynchronously to reduce commit latency under churn.
	DisableBackgroundPrune bool
	// PruneInterval controls how often the background pruner wakes up (0 uses a
	// default).
	PruneInterval time.Duration
	// PruneMaxPages bounds how many pages are freed per pruner tick (0 uses a
	// default; <0 means unlimited).
	PruneMaxPages int
	// PruneMaxDuration bounds how long a pruner tick may run (0 uses a default;
	// <0 means unlimited).
	PruneMaxDuration time.Duration

	FlushThreshold int64
	// MemtableMode selects the cached-mode memtable implementation.
	// Supported values: "skiplist", "hash_sorted", "btree", "append_only", "adaptive".
	MemtableMode string
	// MemtableShards controls the number of mutable memtable shards in cached
	// mode. Values <= 0 use a runtime-dependent default.
	MemtableShards int
	// DomainIngressWorkers enables experimental domain-local ingress workers in
	// cached mode. Values <= 0 keep the legacy direct write path.
	DomainIngressWorkers int
	// DomainIngressQueueSize configures the per-worker ingress queue length when
	// DomainIngressWorkers is enabled. Values <= 0 use a default.
	DomainIngressQueueSize int
	// PreferAppendAlloc makes the page allocator ignore the freelist and append
	// new pages instead. This can improve scan locality under churn at the cost
	// of file growth (space is reclaimed later via vacuum).
	PreferAppendAlloc bool
	// FreelistRegionPages and FreelistRegionRadius bias freelist reuse toward
	// nearby page regions to improve locality. Leave both at 0 to disable the
	// bias (default). If either is set, missing values will use defaults.
	// Set FreelistRegionRadius < 0 to force-disable the bias.
	FreelistRegionPages  uint64
	FreelistRegionRadius int

	// LeafFillTargetPPM and InternalFillTargetPPM control how full newly-written
	// B+Tree pages are allowed to become before forcing a split (soft-full).
	// Lower values reduce split churn and slow re-fragmentation under updates, at
	// the cost of higher page count (more index bytes).
	//
	// Values are in parts-per-million where 1_000_000 means "allow full pages"
	// (current behavior). Zero uses the default (1_000_000).
	LeafFillTargetPPM     uint32
	InternalFillTargetPPM uint32
	// MaintenanceOpsPerCoalesce controls the maintenance budget during zipper
	// merge. It bounds coalesce work to roughly len(ops)/K operations per batch.
	// 0 uses the default; negative disables the budget (full maintenance).
	MaintenanceOpsPerCoalesce int
	// LeafPrefixCompression enables prefix-compressed leaf nodes for new pages.
	LeafPrefixCompression bool
	// IndexColumnarLeaves enables the experimental columnar leaf encoding for new pages.
	IndexColumnarLeaves bool
	// IndexPackedValuePtr enables the experimental packed 12-byte ValuePtr encoding
	// for pointer entries in new leaf pages.
	//
	// Packed pointers store ValuePtr.Offset as u32 on disk. Callers must ensure
	// value-log segments are rotated such that offsets remain representable.
	IndexPackedValuePtr bool
	// IndexInternalBaseDelta enables the experimental internal-node base-delta encoding.
	IndexInternalBaseDelta bool
	// IndexOuterLeavesInValueLog stores B+Tree leaf pages (the pages containing
	// key/value entries) in the persistent value log instead of index.db.
	//
	// When enabled, internal nodes store encoded value-log pointers for leaf
	// children. This is pre-alpha and changes on-disk format/assumptions.
	IndexOuterLeavesInValueLog bool
	// IndexAdaptiveLeafEncoding enables per-page adaptive selection of leaf
	// encoding flags using deterministic heuristics from key/value shape.
	//
	// This option only affects newly-written leaf pages.
	IndexAdaptiveLeafEncoding bool
	// MaxQueuedMemtables controls how much immutable-memtable backlog the cached
	// layer will allow before applying backpressure (i.e. forcing flush work on
	// writers). A negative value disables backpressure entirely (higher short-term
	// ingest, but potentially unbounded flush debt). Zero uses the default.
	MaxQueuedMemtables int

	// SlowdownBacklogSeconds begins applying writer backpressure when queued flush
	// backlog exceeds this many seconds of estimated flush work (0 disables).
	SlowdownBacklogSeconds float64
	// StopBacklogSeconds blocks writers when queued flush backlog exceeds this many
	// seconds of estimated flush work (0 disables).
	StopBacklogSeconds float64
	// MaxBacklogBytes is an absolute cap on queued flush backlog bytes (0 disables).
	MaxBacklogBytes int64

	// WriterFlushMaxMemtables bounds how much queued work a writer will help flush
	// per write when backpressure is active (0 uses a default).
	WriterFlushMaxMemtables int
	// WriterFlushMaxDuration bounds how long a writer will spend helping flush per
	// write when backpressure is active (0 disables the time bound).
	WriterFlushMaxDuration time.Duration
	// FlushBuildConcurrency controls how many goroutines may be used to build a
	// combined flush batch from multiple immutable memtables in cached mode.
	// Values <= 1 disable parallelism.
	FlushBuildConcurrency int
	// FlushBuildMinEntries gates the parallel build path by total entries.
	// Values <= 0 use a default of 16k.
	FlushBuildMinEntries int
	// FlushBuildMinUnits gates the parallel build path by number of queued units.
	// Values <= 0 use a default of 2.
	FlushBuildMinUnits int
	// FlushBuildChunkCap controls the maximum entries per build chunk.
	// A value of 0 enables adaptive chunk sizing, values < 0 use the fixed default of 8192,
	// and values > 0 set an explicit cap.
	FlushBuildChunkCap int
	// FlushBuildChunkTargetBytes controls adaptive chunk sizing (bytes per chunk).
	// Values <= 0 use a default of 2MiB.
	FlushBuildChunkTargetBytes int
	// FlushBuildChunkMinBytes clamps adaptive chunk sizes (minimum bytes).
	// Values <= 0 use a default of 1MiB.
	FlushBuildChunkMinBytes int
	// FlushBuildChunkMaxBytes clamps adaptive chunk sizes (maximum bytes).
	// Values <= 0 use a default of 4MiB.
	FlushBuildChunkMaxBytes int
	// FlushBuildPrefetchUnits controls how many memtables to start building ahead
	// of the consumer. Values <= 0 use FlushBuildConcurrency.
	FlushBuildPrefetchUnits int

	// FlushBackendMaxEntries caps how many operations are buffered into a single
	// backend batch before committing it and continuing with a fresh batch.
	//
	// This increases backend commit cadence during very large flushes, which can
	// reduce index.db high-watermark growth under small KeepRecent windows by
	// making retired pages eligible for reuse sooner.
	//
	// 0 uses the internal default. Negative disables chunking (single backend
	// commit per flush).
	FlushBackendMaxEntries int
	// FlushBackendMaxBatches caps how many intermediate backend commits a single
	// flush may emit (0=default, <0=disable cap).
	FlushBackendMaxBatches int

	// JournalLanes controls the number of active commit/value log lanes (0=default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// JournalCompression enables best-effort zstd compression for cached-mode
	// journal/commitlog segments (metadata only).
	//
	// The redo log will only keep compressed bytes when they are smaller than the
	// raw payload, so compression never causes size amplification.
	JournalCompression bool

	// ValueLog configures value-log pointer behavior and read integrity.
	ValueLog ValueLogOptions

	// NotifyError is an optional hook for background maintenance failures.
	NotifyError func(error)

	// VerifyOnRead forces checksum verification on every index page read,
	// bypassing the verified-page cache.
	VerifyOnRead bool
	// DisableSideStores skips opening dictdb/templatedb side stores.
	// This is intended for internal side-store usage (e.g. templatedb itself).
	DisableSideStores bool

	// DisablePiggybackCompaction disables opportunistic defragmentation during writes.
	// When false (default), nodes are rewritten if their siblings are physically
	// distant, keeping the tree clustered. Set to true to maximize write speed.
	DisablePiggybackCompaction bool

	// BackgroundCheckpointInterval enables periodic durable checkpoints in cached
	// mode. A checkpoint creates a backend sync boundary and trims
	// cached-mode WAL segments to keep `wal/` growth bounded.
	//
	// Semantics:
	// - `0` uses a default.
	// - `<0` disables the periodic interval trigger.
	BackgroundCheckpointInterval time.Duration
	// BackgroundCheckpointIdleDuration triggers an opportunistic checkpoint after
	// a period of write-idleness in cached mode.
	//
	// Semantics:
	// - `0` uses a default.
	// - `<0` disables the idle trigger.
	BackgroundCheckpointIdleDuration time.Duration
	// BackgroundIndexVacuumInterval enables periodic online index vacuum passes.
	// `0` uses a default; `<0` disables.
	BackgroundIndexVacuumInterval time.Duration
	// BackgroundIndexVacuumSpanRatioPPM sets the span ratio threshold that
	// triggers a vacuum pass (0 uses a default).
	BackgroundIndexVacuumSpanRatioPPM uint32
	// MaxWALBytes triggers an immediate checkpoint in cached mode when the sum of
	// WAL segment sizes exceeds this many bytes (0 uses a default; <0 disables the
	// size trigger). This is an operational safety cap; it does not make each
	// individual write durable (use *Sync APIs for that).
	MaxWALBytes int64
}

type Snapshot struct {
	db                *DB
	idx               *indexGen
	state             *DBState
	vlogManager       *valuelog.Manager
	vlogPinned        bool
	leafGenerationIDs []uint64
	// leafGenerationPinnedIDs mirrors the generation IDs retained by this
	// snapshot for stats/debugging. Release follows leafGenerationPinSet or
	// leafGenerationRefs when those optimized paths are present.
	leafGenerationPinnedIDs []uint64
	leafGenerationRefs      []*leafGenerationPinRef
	leafGenerationPinSet    *leafGenerationPinSet
	registryID              int64
	reader                  valueReader
	tree                    tree.Tree
	closed                  atomic.Bool
	treePager               *pager.Pager
	treeRoot                uint64
	// registryShardHint is used to route reader registrations to a stable fast
	// registry shard for this snapshot object across operations.
	registryShardHint int
}

func registryHintFromSnapshot(s *Snapshot) int {
	if s == nil {
		return snapshotShardHintUnset
	}
	if s.registryShardHint != snapshotShardHintUnset {
		return s.registryShardHint
	}
	h := uint64(uintptr(unsafe.Pointer(s)))
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return int(h & uint64(lifecycle.FastReaderShardMask))
}

func (s *Snapshot) Pager() *pager.Pager {
	if s == nil || s.idx == nil {
		return nil
	}
	return s.idx.pager
}

func (s *Snapshot) State() *DBState {
	if s == nil {
		return nil
	}
	return s.state
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	if db.closing.Load() {
		return nil
	}
	snap := db.snapPool.Get()
	if snap.registryShardHint == snapshotShardHintUnset {
		snap.registryShardHint = registryHintFromSnapshot(snap)
	}
	acqShard := snapshotAcquireShard()
	db.snapshotAcquireRO[acqShard].Add(1)
	defer db.snapshotAcquireRO[acqShard].Add(-1)
	if db.closing.Load() {
		db.snapPool.Put(snap)
		return nil
	}

	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.state == nil {
		db.snapPool.Put(snap)
		return nil
	}
	idx := view.idx
	state := view.state
	vm := view.vlogManager
	vlogSet := state.ValueLogSet
	vlogNeedsPin := vlogSet != nil && len(vlogSet.Files) > 0
	if vlogNeedsPin {
		if vm == nil {
			db.snapPool.Put(snap)
			return nil
		}
		vm.Acquire(vlogSet)
	}

	var registryID int64
	if idx != nil {
		if idx.registry == nil {
			if vlogNeedsPin && vm != nil {
				_ = vm.Release(vlogSet)
			}
			db.snapPool.Put(snap)
			return nil
		}
		registryID, snap.registryShardHint = idx.registry.RegisterWithHint(state.CommitSeq, snap.registryShardHint)
	}
	if state.LeafGenerations != nil {
		snap.leafGenerationIDs = state.LeafGenerations.GenerationOrder
		if state.LeafGenerations.PinSet != nil {
			snap.leafGenerationPinSet = state.LeafGenerations.PinSet
			if db.retainLeafGenerationPinSet(snap.leafGenerationPinSet) {
				snap.leafGenerationPinnedIDs = snap.leafGenerationIDs
			} else {
				snap.leafGenerationPinnedIDs = nil
			}
			snap.leafGenerationRefs = snap.leafGenerationRefs[:0]
		} else if len(state.LeafGenerations.PinRefs) == len(state.LeafGenerations.GenerationOrder) && len(state.LeafGenerations.PinRefs) > 0 {
			snap.leafGenerationRefs = append(snap.leafGenerationRefs[:0], state.LeafGenerations.PinRefs...)
			db.pinLeafGenerationRefs(snap.leafGenerationRefs)
			snap.leafGenerationPinnedIDs = snap.leafGenerationIDs
		} else {
			snap.leafGenerationRefs = snap.leafGenerationRefs[:0]
			db.pinLeafGenerationIDs(snap.leafGenerationIDs)
			snap.leafGenerationPinnedIDs = snap.leafGenerationIDs
		}
	} else {
		snap.leafGenerationIDs = nil
		snap.leafGenerationPinnedIDs = nil
		snap.leafGenerationRefs = snap.leafGenerationRefs[:0]
		snap.leafGenerationPinSet = nil
	}

	snap.db = db
	snap.idx = idx
	snap.state = state
	snap.vlogManager = vm
	snap.vlogPinned = vlogNeedsPin
	snap.reader.reconfigure(vlogSet)
	snap.registryID = registryID
	if idx != nil {
		sameTree := snap.treePager == idx.pager &&
			snap.treeRoot == state.RootPageID
		if !sameTree {
			snap.tree.Reset(idx.pager, &snap.reader, state.RootPageID)
			snap.treePager = idx.pager
			snap.treeRoot = state.RootPageID
		}
	} else {
		if snap.treePager != nil || snap.treeRoot != 0 {
			snap.tree.Reset(nil, nil, 0)
			snap.treePager = nil
			snap.treeRoot = 0
		}
	}
	snap.closed.Store(false)
	return snap
}

func (s *Snapshot) releaseLeafGenerationPins() {
	if s == nil {
		return
	}
	if s.db != nil {
		switch {
		case s.leafGenerationPinSet != nil:
			s.db.releaseLeafGenerationPinSet(s.leafGenerationPinSet)
		case len(s.leafGenerationRefs) > 0:
			s.db.unpinLeafGenerationRefs(s.leafGenerationRefs)
		case len(s.leafGenerationIDs) > 0:
			s.db.unpinLeafGenerationIDs(s.leafGenerationIDs)
		}
	}
	s.leafGenerationIDs = nil
	s.leafGenerationPinnedIDs = nil
	s.leafGenerationPinSet = nil
	clear(s.leafGenerationRefs)
	s.leafGenerationRefs = s.leafGenerationRefs[:0]
}

// Close releases the snapshot.
func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	var err error
	if s.vlogPinned && s.state != nil && s.state.ValueLogSet != nil && s.vlogManager != nil {
		if relErr := s.vlogManager.Release(s.state.ValueLogSet); relErr != nil {
			err = relErr
		}
	}
	if s.idx != nil {
		if s.registryID != 0 {
			s.idx.registry.Unregister(s.registryID)
		}
		if s.db != nil {
			if s.db.idx.Load() != s.idx {
				s.db.maybeReleaseRetiredIndex(s.idx)
			}
		}
	}
	s.releaseLeafGenerationPins()
	if s.db != nil {
		s.db.snapPool.Put(s)
	}
	return err
}

// Open opens the database.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("db dir required")
	}
	if !opts.IgnoreFormatConfig {
		if cfg, ok, err := LoadFormatConfig(opts.Dir); err != nil {
			return nil, err
		} else if ok {
			cfg.ApplyIndexFormatToOptions(&opts)
		}
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	if opts.KeepRecent == 0 {
		opts.KeepRecent = KeepRecent
	}
	if opts.LeafFillTargetPPM == 0 {
		opts.LeafFillTargetPPM = 1_000_000
	}
	if opts.InternalFillTargetPPM == 0 {
		opts.InternalFillTargetPPM = 1_000_000
	}
	if opts.MaintenanceOpsPerCoalesce == 0 {
		opts.MaintenanceOpsPerCoalesce = defaultMaintenanceOpsPerCoalesce
	} else if opts.MaintenanceOpsPerCoalesce < 0 {
		opts.MaintenanceOpsPerCoalesce = 0
	}
	if opts.PruneInterval == 0 {
		opts.PruneInterval = 250 * time.Millisecond
	}
	if opts.PruneMaxPages == 0 {
		opts.PruneMaxPages = 4096
	}
	if opts.PruneMaxDuration == 0 {
		opts.PruneMaxDuration = 25 * time.Millisecond
	}
	if opts.FreelistRegionRadius < 0 {
		opts.FreelistRegionPages = 0
		opts.FreelistRegionRadius = 0
	} else if opts.FreelistRegionPages > 0 || opts.FreelistRegionRadius > 0 {
		if opts.FreelistRegionPages == 0 {
			opts.FreelistRegionPages = 8192
		}
		if opts.FreelistRegionRadius == 0 {
			opts.FreelistRegionRadius = 1
		}
	} else if !opts.PreferAppendAlloc {
		opts.FreelistRegionPages = 8192
		opts.FreelistRegionRadius = 1
	}
	if opts.ValueLog.Compression == 0 {
		opts.ValueLog.Compression = ValueLogCompressionAuto
	}
	if opts.IndexOuterLeavesInValueLog && opts.IndexInternalBaseDelta {
		// Leaf refs encode value-log pointers in internal child IDs, which are
		// incompatible with internal base-delta child ID encodings. Enforce the
		// effective behavior early so Options and persisted format config remain
		// consistent.
		opts.IndexInternalBaseDelta = false
	}

	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	warnInsecureDir(opts.Dir, opts.NotifyError)
	if err := ensureNoLegacyMixedWALValueSegments(opts.Dir); err != nil {
		return nil, err
	}

	if opts.ReadOnly {
		return openReadOnly(opts)
	}

	if err := ensureStorageLayoutDirs(opts.Dir); err != nil {
		return nil, err
	}

	lock, err := lockfile.Acquire(filepath.Join(opts.Dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	db, err := openWithLock(opts, lock)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return db, nil
}

func validateOptions(opts Options) error {
	if opts.ReadOnly {
		// Read-only opens never mutate on-disk state, so "unsafe" write options do
		// not apply.
		return nil
	}
	switch opts.Durability {
	case DurabilityDurable, DurabilityWALOnRelaxed, DurabilityWALOffRelaxed:
	default:
		return fmt.Errorf("treedb: invalid durability mode %d", opts.Durability)
	}
	switch opts.ValueLog.ReadIntegrity {
	case IntegrityVerify, IntegritySkipChecksums:
	default:
		return fmt.Errorf("treedb: invalid value-log integrity mode %d", opts.ValueLog.ReadIntegrity)
	}
	switch opts.ValueLog.Compression {
	case 0:
		// Unset/default mode is allowed for backward-compatible behavior.
	case ValueLogCompressionOff, ValueLogCompressionBlock, ValueLogCompressionDict, ValueLogCompressionAuto:
	default:
		return fmt.Errorf("treedb: invalid value-log compression mode %d", opts.ValueLog.Compression)
	}
	switch opts.ValueLog.BlockCodec {
	case ValueLogBlockSnappy, ValueLogBlockLZ4:
	default:
		return fmt.Errorf("treedb: invalid value-log block codec %d", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.BlockTargetCompressedBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log block target compressed bytes %d", opts.ValueLog.BlockTargetCompressedBytes)
	}
	if opts.ValueLog.BlockTargetCompressedBytes > 0 {
		const (
			minBlockTargetCompressedBytes = 256
			maxBlockTargetCompressedBytes = 1 << 20
		)
		if opts.ValueLog.BlockTargetCompressedBytes < minBlockTargetCompressedBytes || opts.ValueLog.BlockTargetCompressedBytes > maxBlockTargetCompressedBytes {
			return fmt.Errorf("treedb: value-log block target compressed bytes out of range [%d,%d]: %d", minBlockTargetCompressedBytes, maxBlockTargetCompressedBytes, opts.ValueLog.BlockTargetCompressedBytes)
		}
	}
	if opts.ValueLog.IncompressibleHoldBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log incompressible hold bytes %d", opts.ValueLog.IncompressibleHoldBytes)
	}
	if opts.ValueLog.IncompressibleProbeIntervalBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log incompressible probe interval bytes %d", opts.ValueLog.IncompressibleProbeIntervalBytes)
	}
	switch opts.ValueLog.AutoPolicy {
	case ValueLogAutoThroughput, ValueLogAutoBalanced, ValueLogAutoSize:
	default:
		return fmt.Errorf("treedb: invalid value-log auto policy %d", opts.ValueLog.AutoPolicy)
	}
	switch opts.ValueLog.Generational.Policy {
	case ValueLogGenerationDefault, ValueLogGenerationOff, ValueLogGenerationHotWarmCold:
	default:
		return fmt.Errorf("treedb: invalid value-log generation policy %d", opts.ValueLog.Generational.Policy)
	}
	if opts.ValueLog.Generational.HotSegmentTargetBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log generational hot segment target bytes %d", opts.ValueLog.Generational.HotSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.LeafSegmentTargetBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log generational leaf segment target bytes %d", opts.ValueLog.Generational.LeafSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.WarmSegmentTargetBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log generational warm segment target bytes %d", opts.ValueLog.Generational.WarmSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.ColdSegmentTargetBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log generational cold segment target bytes %d", opts.ValueLog.Generational.ColdSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.RewriteBudgetBytesPerSec < 0 {
		return fmt.Errorf("treedb: invalid value-log generational rewrite budget bytes/sec %d", opts.ValueLog.Generational.RewriteBudgetBytesPerSec)
	}
	if opts.ValueLog.Generational.RewriteBudgetRecordsPerSec < 0 {
		return fmt.Errorf("treedb: invalid value-log generational rewrite budget records/sec %d", opts.ValueLog.Generational.RewriteBudgetRecordsPerSec)
	}
	if opts.ValueLog.Generational.RewriteTriggerTotalBytes < 0 {
		return fmt.Errorf("treedb: invalid value-log generational rewrite trigger total bytes %d", opts.ValueLog.Generational.RewriteTriggerTotalBytes)
	}
	if opts.ValueLog.Generational.RewriteTriggerChurnPerSec < 0 {
		return fmt.Errorf("treedb: invalid value-log generational rewrite trigger churn/sec %d", opts.ValueLog.Generational.RewriteTriggerChurnPerSec)
	}
	if opts.ValueLog.Generational.RewriteMinSegmentAge < 0 {
		return fmt.Errorf("treedb: invalid value-log generational rewrite min segment age %s", opts.ValueLog.Generational.RewriteMinSegmentAge)
	}
	seenDomains := make(map[string]struct{}, len(opts.ValueLog.DomainInlineThresholds))
	for i := range opts.ValueLog.DomainInlineThresholds {
		d := opts.ValueLog.DomainInlineThresholds[i]
		if len(d.Prefix) == 0 {
			return fmt.Errorf("treedb: value-log domain threshold[%d] has empty prefix", i)
		}
		if d.InlineThreshold < 0 {
			return fmt.Errorf("treedb: value-log domain threshold[%d] has negative inline threshold %d", i, d.InlineThreshold)
		}
		key := string(d.Prefix)
		if _, dup := seenDomains[key]; dup {
			return fmt.Errorf("treedb: duplicate value-log domain threshold prefix %q", d.Prefix)
		}
		seenDomains[key] = struct{}{}
	}
	return nil
}

func resolveInlineThresholdAndAdaptive(opts Options) (*adaptive.Controller, int) {
	inlineThreshold := page.DefaultInlineThreshold
	adaptiveCtrl := adaptive.New()
	if opts.ValueLog.PointerThreshold > 0 {
		inlineThreshold = opts.ValueLog.PointerThreshold
		adaptiveCtrl = nil
	}
	if opts.ValueLog.ForcePointers {
		inlineThreshold = 0
		adaptiveCtrl = nil
	}
	return adaptiveCtrl, inlineThreshold
}

func openWithLock(opts Options, lock *lockfile.Lock) (*DB, error) {
	if err := recoverIndexSwap(opts.Dir); err != nil {
		return nil, err
	}

	idxPath := filepath.Join(opts.Dir, "index.db")
	p, err := pager.OpenWithOptions(idxPath, opts.ChunkSize, pager.OpenOptions{
		MmapPopulate:   opts.PagerMmapPopulate,
		PrefetchOnRead: opts.PagerPrefetchOnRead,
	})
	if err != nil {
		return nil, err
	}
	if opts.PagerSyncConcurrency > 0 {
		p.SetSyncConcurrency(opts.PagerSyncConcurrency)
	}
	p.SetVerifyOnRead(opts.VerifyOnRead)

	layout := resolveStorageLayout(opts.Dir)
	vm, err := valuelog.NewManager(layout.valueVLogDir)
	if err != nil {
		p.Close()
		return nil, err
	}
	if err := vm.AddScanDir(layout.leafVLogDir); err != nil {
		_ = vm.Close()
		p.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)

	gen := newIndexGen(1, p, alloc, z)

	adaptiveCtrl, inlineThreshold := resolveInlineThresholdAndAdaptive(opts)

	db := &DB{
		valueLogManager:                vm,
		valueLogRefTracker:             newValueLogRefTracker(),
		lock:                           lock,
		adaptive:                       adaptiveCtrl,
		keepRecent:                     opts.KeepRecent,
		valueLogCompression:            opts.ValueLog.Compression,
		valueLogAutoPolicy:             opts.ValueLog.AutoPolicy,
		valueLogBlockCodec:             opts.ValueLog.BlockCodec,
		valueLogDictLookup:             opts.ValueLog.DictLookup,
		valueLogDictCurrentForClass:    opts.ValueLog.DictCurrentForClass,
		valueLogDictLeafPayloadMode:    opts.ValueLog.DictLeafPayloadMode,
		valueLogDictPut:                opts.ValueLog.DictPut,
		valueLogDictSetCurrentForClass: opts.ValueLog.DictSetCurrentForClass,
		valueLogDictSetLeafPayloadMode: opts.ValueLog.DictSetLeafPayloadMode,
		valueLogDomainThresholds:       NormalizeValueLogDomainThresholds(opts.ValueLog.DomainInlineThresholds),
		leafFillTargetPPM:              opts.LeafFillTargetPPM,
		internalFillTargetPPM:          opts.InternalFillTargetPPM,
		leafPrefixCompression:          opts.LeafPrefixCompression,
		indexColumnarLeaves:            opts.IndexColumnarLeaves,
		indexPackedValuePtr:            opts.IndexPackedValuePtr,
		indexInternalBaseDelta:         opts.IndexInternalBaseDelta,
		indexOuterLeavesInValueLog:     opts.IndexOuterLeavesInValueLog,
		indexAdaptiveLeafEncoding:      opts.IndexAdaptiveLeafEncoding,
		piggybackCompaction:            !opts.DisablePiggybackCompaction,
		maintenanceOpsPerCoalesce:      opts.MaintenanceOpsPerCoalesce,
		dir:                            opts.Dir,
		chunkSize:                      opts.ChunkSize,
		preferAppendAlloc:              opts.PreferAppendAlloc,
		freelistRegionPages:            opts.FreelistRegionPages,
		freelistRegionRadius:           opts.FreelistRegionRadius,
		policy: WritePolicy{
			InlineThreshold: inlineThreshold,
			FlushThreshold:  opts.FlushThreshold,
		},

		idxAll:  map[uint64]*indexGen{gen.id: gen},
		idxNext: gen.id + 1,

		snapPool:     NewSnapshotPool(),
		ghostManager: &indexGhostManager{},
		notifyError:  opts.NotifyError,
	}
	db.ghostManager.start()
	db.idx.Store(gen)

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetLeafPrefixCompression(opts.LeafPrefixCompression)
	gen.zipper.SetIndexColumnarLeaves(opts.IndexColumnarLeaves)
	gen.zipper.SetIndexPackedValuePtr(opts.IndexPackedValuePtr)
	gen.zipper.SetIndexInternalBaseDelta(opts.IndexInternalBaseDelta)
	gen.zipper.SetOuterLeavesInValueLog(opts.IndexOuterLeavesInValueLog)
	db.leafPageReadCache = newLeafPageReadCache(LeafPageReadCacheEntries)
	if db.leafPageReadCache != nil {
		gen.zipper.SetLeafPageReader(newCachedLeafPageReader(db.leafPageReadCache, vm))
	} else {
		gen.zipper.SetLeafPageReader(vm)
	}
	gen.zipper.SetAdaptiveLeafEncoding(opts.IndexAdaptiveLeafEncoding)
	gen.zipper.SetMaintenanceOpsPerCoalesce(opts.MaintenanceOpsPerCoalesce)

	if err := db.recover(); err != nil {
		db.Close()
		return nil, err
	}

	if opts.Durability != DurabilityWALOffRelaxed {
		segments, err := listRecoverySegments(opts.Dir)
		if err != nil {
			db.Close()
			return nil, err
		}
		if err := replayWALIntoBackend(db, segments, opts.WALMaxSegmentBytes, opts.ValueLog.DictLookup); err != nil {
			db.Close()
			return nil, err
		}
	}

	// Recovery and WAL/value-log replay may touch the manager's file set. Reapply
	// decode hooks and refresh before publishing the initial snapshot so
	// read-write opens decode dict/template-backed values the same way as
	// read-only opens and offline maintenance helpers.
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)
	if err := vm.Refresh(); err != nil {
		db.Close()
		return nil, err
	}
	if opts.IndexOuterLeavesInValueLog {
		manifest, err := loadOrCreateLeafGenerationManifest(layout.leafVLogDir, db.meta.CommitSeq, false)
		if err != nil {
			db.Close()
			return nil, err
		}
		db.leafGenerationManifest = manifest
	}

	// Initialize State after recovery so log cleanup can proceed without pinning.
	initialState := &DBState{
		CommitSeq:                  db.meta.CommitSeq,
		RootPageID:                 db.meta.UserRootPageID,
		SystemRootPageID:           db.meta.SystemRootPageID,
		ValueLogSet:                vm.CurrentSet(),
		LeafGenerations:            db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	db.state.Store(initialState)
	db.publishSnapshotView(gen, initialState, vm)
	if err := db.initValueLogRefTracker(); err != nil {
		db.Close()
		return nil, err
	}

	db.pruner.Start(db, pruneWorkerOptions{
		enabled:     !opts.DisableBackgroundPrune,
		interval:    opts.PruneInterval,
		maxPages:    opts.PruneMaxPages,
		maxDuration: opts.PruneMaxDuration,
	})
	db.startCommitCombiner()

	// Best-effort: persist the format knobs used for this DB directory so
	// offline maintenance tooling (treemap, offline vacuum/rewrite) can preserve
	// the intended on-disk layout without requiring callers to re-specify flags.
	if err := SaveFormatConfig(opts.Dir, formatConfigFromOptions(opts)); err != nil && opts.NotifyError != nil {
		opts.NotifyError(err)
	}

	return db, nil
}

// RegisterCloseHook registers a callback that runs before Close marks the DB as
// closing, while normal write/publish APIs are still available.
func (db *DB) RegisterCloseHook(hook func() error) func() {
	if db == nil || hook == nil {
		return func() {}
	}
	db.closeHooksMu.Lock()
	if db.closeHooksClosed || db.closing.Load() {
		db.closeHooksMu.Unlock()
		return func() {}
	}
	idx := len(db.closeHooks)
	db.closeHooks = append(db.closeHooks, hook)
	db.closeHooksMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			db.closeHooksMu.Lock()
			if idx >= 0 && idx < len(db.closeHooks) && db.closeHooks[idx] != nil {
				db.closeHooks[idx] = nil
			}
			db.closeHooksMu.Unlock()
		})
	}
}

// RunCloseHooks runs and clears registered close hooks. Wrappers that own a
// backend DB should call this before they start closing resources required by
// backend publish APIs.
func (db *DB) RunCloseHooks() error {
	if db == nil {
		return nil
	}
	db.closeHooksMu.Lock()
	if db.closeHooksClosed {
		db.closeHooksMu.Unlock()
		return nil
	}
	db.closeHooksClosed = true
	hooks := append([]func() error(nil), db.closeHooks...)
	clear(db.closeHooks)
	db.closeHooks = nil
	db.closeHooksMu.Unlock()

	var errs []error
	for _, hook := range hooks {
		if hook == nil {
			continue
		}
		if err := hook(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (db *DB) Close() error {
	var errs []error
	if err := db.RunCloseHooks(); err != nil {
		errs = append(errs, err)
	}
	db.closing.Store(true)
	db.stopCommitCombiner()
	db.pruner.Stop()
	if db.ghostManager != nil {
		db.ghostManager.stop()
	}

	db.mu.Lock()
	db.clearSnapshotView()
	vm := db.valueLogManager
	db.valueLogManager = nil
	lock := db.lock
	db.lock = nil
	db.mu.Unlock()

	drainDeadline := time.Now().Add(closeSnapshotDrainTimeout)
	for db.snapshotAcquireInFlight() > 0 {
		if time.Now().After(drainDeadline) {
			break
		}
		time.Sleep(closeSnapshotDrainSleep)
	}
	if remaining := db.snapshotAcquireInFlight(); remaining > 0 {
		errs = append(errs, fmt.Errorf("db: Close timed out waiting for %d in-flight read-only snapshot acquisitions to complete", remaining))
	}
	if err := db.closeAllIndexes(); err != nil {
		errs = append(errs, err)
	}
	if vm != nil {
		if err := vm.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if lock != nil {
		if err := lock.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if bgErr := db.backgroundError(); bgErr != nil {
		errs = append(errs, bgErr)
	}
	return errors.Join(errs...)
}

func (db *DB) reportError(err error) {
	if err == nil {
		return
	}
	if db.notifyError != nil {
		db.notifyError(err)
	}
	db.bgErrMu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.bgErrMu.Unlock()
}

func (db *DB) backgroundError() error {
	db.bgErrMu.Lock()
	defer db.bgErrMu.Unlock()
	return db.bgErr
}

func (db *DB) snapshotAcquireInFlight() int32 {
	var total int32
	for i := range db.snapshotAcquireRO {
		total += db.snapshotAcquireRO[i].Load()
	}
	return total
}

func snapshotAcquireShard() int {
	var marker int
	p := uintptr(unsafe.Pointer(&marker))
	return int((p ^ (p >> 7) ^ (p >> 13)) & uintptr(snapshotAcquireShardMask))
}

// recover reads meta pages and restores state.
func (db *DB) recover() error {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing pager")
	}
	p := idx.pager

	if p.PageCount() < 2 {
		if db.readOnly {
			return errors.New("read-only open requires an existing index with meta pages")
		}
		if _, err := p.Alloc(2); err != nil {
			return err
		}
		db.meta = page.MetaPageBody{}
		db.metaPageID = MetaPage1ID

		rootID, err := p.Alloc(1)
		if err != nil {
			return err
		}
		data, err := p.GetForWrite(rootID)
		if err != nil {
			return err
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		b.SetPageID(rootID)
		b.Finish()

		db.meta.UserRootPageID = rootID

		// Init System Root
		sysRootID, err := p.Alloc(1)
		if err != nil {
			return err
		}
		dataSys, err := p.GetForWrite(sysRootID)
		if err != nil {
			return err
		}
		bSys := node.NewBuilderWithOptions(dataSys, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		})
		bSys.SetPageID(sysRootID)
		bSys.Finish()

		db.meta.SystemRootPageID = sysRootID
		db.meta.CommitSeq = 0

		if err := db.writeMeta(MetaPage0ID, db.meta); err != nil {
			return err
		}
		if err := db.writeMeta(MetaPage1ID, db.meta); err != nil {
			return err
		}
		db.metaPageID = MetaPage0ID
		return nil
	}

	m0, valid0 := db.readMeta(MetaPage0ID)
	m1, valid1 := db.readMeta(MetaPage1ID)

	type metaCandidate struct {
		id   uint64
		meta page.MetaPageBody
	}
	var candidates []metaCandidate
	if valid0 {
		candidates = append(candidates, metaCandidate{id: MetaPage0ID, meta: m0})
	}
	if valid1 {
		candidates = append(candidates, metaCandidate{id: MetaPage1ID, meta: m1})
	}
	if len(candidates) == 0 {
		return errors.New("both meta pages corrupted")
	}
	if len(candidates) == 2 && candidates[0].meta.CommitSeq < candidates[1].meta.CommitSeq {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}

	var chosen *metaCandidate
	for i := range candidates {
		c := &candidates[i]

		if !db.rootPageValid(p, c.meta.UserRootPageID) || !db.rootPageValid(p, c.meta.SystemRootPageID) {
			continue
		}
		if !db.freelistHeadValid(p, c.meta.FreelistHeadID) {
			continue
		}

		chosen = c
		break
	}
	if chosen == nil {
		return errors.New("no valid meta page")
	}

	db.meta = chosen.meta
	db.metaPageID = chosen.id

	if chosen.meta.TotalPages > 0 {
		p.SetPageCount(chosen.meta.TotalPages)
	}

	// Update Allocator Head
	idx.allocator.SetHead(chosen.meta.FreelistHeadID)

	return nil
}

func (db *DB) rootPageValid(p *pager.Pager, pageID uint64) bool {
	if pageID == 0 || p == nil {
		return false
	}
	data, err := p.Get(pageID)
	if err != nil {
		return false
	}
	n := node.NewNode(data)
	verifyAlways := p.VerifyOnRead()
	if verifyAlways || !p.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return false
		}
		if !verifyAlways {
			p.MarkVerified(pageID)
		}
	}
	switch n.Type() {
	case page.PageTypeLeaf, page.PageTypeInternal:
		return true
	default:
		return false
	}
}

func (db *DB) freelistHeadValid(p *pager.Pager, head uint64) bool {
	if head == 0 || p == nil {
		return true
	}
	data, err := p.Get(head)
	if err != nil {
		return false
	}
	n := node.NewNode(data)
	verifyAlways := p.VerifyOnRead()
	if verifyAlways || !p.IsVerified(head) {
		if !n.VerifyChecksum() {
			return false
		}
		if !verifyAlways {
			p.MarkVerified(head)
		}
	}
	return n.Type() == page.PageTypeFreelist
}

func (db *DB) readMeta(pageID uint64) (page.MetaPageBody, bool) {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return page.MetaPageBody{}, false
	}

	data, err := idx.pager.Get(pageID)
	if err != nil {
		return page.MetaPageBody{}, false
	}
	n := node.NewNode(data)

	verifyAlways := idx.pager.VerifyOnRead()
	if verifyAlways || !idx.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return page.MetaPageBody{}, false
		}
		if !verifyAlways {
			idx.pager.MarkVerified(pageID)
		}
	}

	if n.Type() != page.PageTypeMeta {
		return page.MetaPageBody{}, false
	}
	return page.DecodeMetaBody(data[page.PageHeaderSize:]), true
}

func (db *DB) writeMeta(pageID uint64, meta page.MetaPageBody) error {
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing pager")
	}
	if db.testFailWriteMeta.Load() {
		return errTestWriteMetaFailpoint
	}

	data, err := idx.pager.GetForWrite(pageID)
	if err != nil {
		return err
	}
	meta.Encode(data[page.PageHeaderSize:])
	n := node.NewNode(data)
	n.SetPageID(pageID)
	n.SetType(page.PageTypeMeta)
	n.SetCount(0)
	n.UpdateChecksum()
	return nil
}

type finalizeCommitPost struct {
	oldState                          *DBState
	metrics                           adaptive.Metrics
	vlogRefDelta                      *valueLogRefDelta
	commitSeq                         uint64
	kickPrune                         bool
	doPrune                           bool
	debug                             bool
	sync                              bool
	start                             time.Time
	durSync1                          time.Duration
	durMeta                           time.Duration
	durSync2                          time.Duration
	persistLeafGenerationManifest     bool
	persistLeafGenerationManifestView *leafGenerationManifest
	persistLeafGenerationRawFileIDs   []uint32
	drainLeafGenerationPending        bool
}

// finalizeCommitLocked performs the durability-critical publish path.
// Callers that already hold commit serialization may run post work after
// releasing the serialization lock.
func (db *DB) finalizeCommitLocked(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics, touchedValueLogSegments []uint32, forceValueLogRefresh bool, vlogRefDelta *valueLogRefDelta, leafManifest *leafGenerationManifest, leafManifestRawFileIDs []uint32) (finalizeCommitPost, error) {
	post := finalizeCommitPost{
		metrics: metrics,
	}
	prePublishErr := func(err error) error {
		return wrapFinalizeCommitError(err, true)
	}
	if db.readOnly {
		return post, ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return post, errors.New("missing index")
	}

	// Ensure value-log-backed leaf pages are flushed before we publish an index
	// commit that references them. Per-root storage policies can use the leaf
	// page log even when the DB-level default stores outer leaves in index pages.
	if db.leafPageLog != nil {
		if sync {
			if err := db.leafPageLog.Sync(); err != nil {
				return post, prePublishErr(err)
			}
		} else {
			if err := db.leafPageLog.Flush(); err != nil {
				return post, prePublishErr(err)
			}
		}
	}
	debugTiming := commitTimingEnabled()
	var (
		start    time.Time
		durSync1 time.Duration
		durMeta  time.Duration
		durSync2 time.Duration
	)
	if debugTiming {
		start = time.Now()
	}
	var watermarkWait, watermarkHold time.Duration

	// 1. Sync Data (Index Pages) - No DB Lock
	if sync {
		t0 := time.Now()
		if err := idx.pager.Sync(); err != nil {
			return post, prePublishErr(err)
		}
		if debugTiming {
			durSync1 = time.Since(t0)
		}
	}

	// 2. Prepare Meta - Short Lock
	lockStart := time.Now()
	db.mu.Lock()
	watermarkWait += time.Since(lockStart)
	holdStart := time.Now()
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.SystemRootPageID = sysRootID
	nextMeta.FreelistHeadID = idx.allocator.Head()
	nextMeta.TotalPages = idx.pager.PageCount()

	targetPageID := uint64(0)
	if db.metaPageID == 0 {
		targetPageID = 1
	}
	db.mu.Unlock()
	watermarkHold += time.Since(holdStart)

	leafPageSegmentRegistered := false
	leafPageSegmentFileID := uint32(0)
	if db.testFailFinalizeCommit.Load() {
		return post, prePublishErr(errTestFinalizeCommitFailpoint)
	}
	if forceValueLogRefresh && db.valueLogManager != nil {
		path, fileID, ok := db.currentLeafPageLogSegment()
		if ok {
			leafPageSegmentFileID = fileID
		}
		registered, err := db.ensureLeafPageLogSegmentRegisteredAt(path, fileID, 0)
		if err != nil {
			return post, prePublishErr(err)
		}
		leafPageSegmentRegistered = registered
	}

	// 3. Write Meta - No DB Lock
	t0 := time.Now()
	if err := db.writeMeta(targetPageID, nextMeta); err != nil {
		return post, prePublishErr(err)
	}
	if debugTiming {
		durMeta = time.Since(t0)
	}

	// 4. Sync Meta - No DB Lock
	if sync {
		t1 := time.Now()
		if err := idx.pager.Sync(); err != nil {
			return post, err
		}
		if debugTiming {
			durSync2 = time.Since(t1)
		}
	}

	// 5. Update visible state and retire pages.
	lockStart = time.Now()
	db.mu.Lock()
	watermarkWait += time.Since(lockStart)
	holdStart = time.Now()
	db.meta = nextMeta
	db.metaPageID = targetPageID
	idx.graveyard.Add(nextMeta.CommitSeq-1, retired)
	post.oldState = db.state.Load()
	var valueLogSet *valuelog.Set
	if db.valueLogManager != nil {
		needRefresh := false
		if len(touchedValueLogSegments) > 0 {
			for _, id := range touchedValueLogSegments {
				if !db.valueLogManager.HasSegment(id) {
					needRefresh = true
					break
				}
			}
		}
		if forceValueLogRefresh && !leafPageSegmentRegistered {
			// If no registration path is available, force one refresh as a
			// safety fallback before publishing the new state.
			needRefresh = true
		}
		if needRefresh {
			if err := db.valueLogManager.Refresh(); err != nil {
				db.mu.Unlock()
				return post, err
			}
			valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
		} else {
			valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
		}
	}
	leafGenerationView := db.currentLeafGenerationView()
	if leafManifest != nil {
		db.leafGenerationManifest = leafManifest
		post.persistLeafGenerationManifest = true
		post.persistLeafGenerationManifestView = leafManifest
		post.persistLeafGenerationRawFileIDs = append(post.persistLeafGenerationRawFileIDs[:0], leafManifestRawFileIDs...)
		leafGenerationView = newLeafGenerationView(leafManifest)
	}
	if leafPageSegmentRegistered && leafPageSegmentFileID != 0 {
		db.queueLeafGenerationWritableFileIDAtCommit(leafPageSegmentFileID, nextMeta.CommitSeq)
	}
	if db.leafPageLog != nil {
		stagedLeafManifest, changed, err := db.stagedLeafGenerationManifestWithPending(db.leafGenerationManifest, 0, nextMeta.CommitSeq)
		if err != nil {
			db.mu.Unlock()
			return post, err
		}
		if changed {
			leafGenerationView = newLeafGenerationView(stagedLeafManifest)
		}
	}
	newState := &DBState{
		CommitSeq:        nextMeta.CommitSeq,
		RootPageID:       nextMeta.UserRootPageID,
		SystemRootPageID: nextMeta.SystemRootPageID,
		ValueLogSet:      valueLogSet,
		LeafGenerations:  leafGenerationView,
	}
	if leafGenerationView != nil {
		db.leafGenerationStateVersion++
		newState.LeafGenerationStateVersion = db.leafGenerationStateVersion
	}
	db.state.Store(newState)
	db.publishSnapshotView(idx, newState, db.valueLogManager)
	post.commitSeq = nextMeta.CommitSeq
	post.vlogRefDelta = vlogRefDelta
	if db.leafPageLog != nil {
		post.drainLeafGenerationPending = true
	}
	db.mu.Unlock()
	watermarkHold += time.Since(holdStart)
	db.observePublishWatermark(watermarkWait, watermarkHold, watermarkWait+watermarkHold)

	post.kickPrune = db.pruner.Enabled()
	post.doPrune = !post.kickPrune
	if debugTiming {
		post.debug = true
		post.sync = sync
		post.start = start
		post.durSync1 = durSync1
		post.durMeta = durMeta
		post.durSync2 = durSync2
	}

	return post, nil
}

func (db *DB) finalizeCommitPostWork(post finalizeCommitPost) {
	var durPrune time.Duration

	if post.vlogRefDelta != nil {
		defer releaseValueLogRefDelta(post.vlogRefDelta)
	}
	if db.valueLogRefTracker != nil {
		if post.vlogRefDelta != nil {
			if err := db.valueLogRefTracker.applyDelta(post.commitSeq, post.vlogRefDelta); err != nil {
				db.valueLogRefTracker.invalidate()
				db.reportError(err)
			}
		} else {
			db.valueLogRefTracker.invalidate()
		}
	}

	// Keep pruning out of the commit serialization critical section.
	if post.kickPrune {
		db.pruner.Kick()
	} else if post.doPrune {
		t0 := time.Now()
		db.Prune()
		if post.debug {
			durPrune = time.Since(t0)
		}
	}

	if post.oldState != nil {
		_ = db.valueLogManager.Release(post.oldState.ValueLogSet)
	}
	if post.persistLeafGenerationManifest || post.drainLeafGenerationPending {
		var (
			persistErr error
			pendingErr error
		)
		db.commitMu.Lock()
		currentCommitSeq := db.meta.CommitSeq
		currentManifest := db.leafGenerationManifest
		if post.persistLeafGenerationManifest && currentManifest == post.persistLeafGenerationManifestView {
			persistErr = db.persistLeafGenerationManifestAndRecordLengthIndexes(post.persistLeafGenerationManifestView, post.persistLeafGenerationRawFileIDs)
		}
		if post.drainLeafGenerationPending {
			pendingErr = db.noteLeafGenerationPendingFileIDs(0, currentCommitSeq)
		}
		db.commitMu.Unlock()
		if persistErr != nil {
			// The commit is already durable and published at this point. Returning
			// this error to the caller would make retry behavior unsafe.
			db.reportError(persistErr)
		}
		if pendingErr != nil {
			db.reportError(pendingErr)
		}
	}

	if db.adaptive != nil {
		db.adaptive.RecordCommit(post.metrics)
	}

	if post.debug {
		commitTimingPrintf(
			"treedb: commit_timing sync=%t sync1=%s meta=%s sync2=%s prune=%s total=%s\n",
			post.sync,
			post.durSync1,
			post.durMeta,
			post.durSync2,
			durPrune,
			time.Since(post.start),
		)
	}
}

// finalizeCommit handles durability and state updates.
func (db *DB) finalizeCommit(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics, touchedValueLogSegments []uint32, forceValueLogRefresh bool, vlogRefDelta *valueLogRefDelta, leafManifest *leafGenerationManifest, leafManifestRawFileIDs []uint32) error {
	post, err := db.finalizeCommitLocked(newRootID, sysRootID, retired, sync, metrics, touchedValueLogSegments, forceValueLogRefresh, vlogRefDelta, leafManifest, leafManifestRawFileIDs)
	if err != nil {
		return err
	}
	db.finalizeCommitPostWork(post)
	return nil
}

// Commit persists the new root (Sync=true by default).
// Note: This is usually called internally by Batch.Write or externally if manual root management.
// If manual, retired pages are unknown? `Commit` signature assumes manual root.
// If external user calls Commit, they might not know retired pages.
// We'll accept nil for retired if manual.
func (db *DB) Commit(newRootID uint64) error {
	if db.readOnly {
		return ErrReadOnly
	}
	// Public Commit assumes the caller has built a new tree.
	// We need to serialize with other writers.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Since we are committing a root provided by caller, we assume they based it on current state?
	// If caller is external, they might have read old state.
	// But Commit(newRoot) implies "Force Set Root".
	// We just commit it.

	// Need sysRootID.
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	return db.finalizeCommit(newRootID, sysRoot, nil, true, adaptive.Metrics{}, nil, true, nil, nil, nil)
}

// Checkpoint forces a durable boundary for previously-published backend state.
//
// Unlike Commit, this does not publish a new root or advance CommitSeq. It is
// intended for callers that already made writes visible with relaxed durability
// and now need those writes durable on disk.
func (db *DB) Checkpoint() error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return errors.New("missing index")
	}
	debugTiming := commitTimingEnabled()
	var (
		start      time.Time
		durLeafLog time.Duration
		durPager   time.Duration
	)
	if debugTiming {
		start = time.Now()
	}
	if db.leafPageLog != nil {
		if debugTiming {
			t0 := time.Now()
			if err := db.leafPageLog.Sync(); err != nil {
				return err
			}
			durLeafLog = time.Since(t0)
		} else if err := db.leafPageLog.Sync(); err != nil {
			return err
		}
	}
	if debugTiming {
		t1 := time.Now()
		if err := idx.pager.Sync(); err != nil {
			return err
		}
		durPager = time.Since(t1)
		commitTimingPrintf(
			"treedb: checkpoint_timing leaf_log=%s pager=%s total=%s\n",
			durLeafLog,
			durPager,
			time.Since(start),
		)
	} else if err := idx.pager.Sync(); err != nil {
		return err
	}
	return nil
}

// Prune reclaims pages from the graveyard.
func (db *DB) Prune() {
	if db.readOnly {
		return
	}
	idx := db.idx.Load()
	if idx == nil {
		return
	}
	idx.acquire()
	defer db.releaseIndex(idx)

	min := idx.registry.MinPinnedSeq()
	db.mu.RLock()
	current := db.meta.CommitSeq
	db.mu.RUnlock()

	freed := idx.graveyard.Extract(min, current, db.keepRecent)

	if len(freed) > 0 {
		for _, id := range freed {
			_ = idx.allocator.Free(id) // Ignore error?
		}
	}
}

// Get returns value from snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return s.tree.Get(key)
}

// GetAppend appends the value for key to dst and returns the grown slice.
// If key is not found, it returns dst and tree.ErrKeyNotFound.
func (s *Snapshot) GetAppend(key, dst []byte) ([]byte, error) {
	return s.tree.GetAppend(key, dst)
}

// GetUnsafe returns a zero-copy view of the value from the snapshot.
// The slice is valid until the snapshot is closed.
func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	return s.tree.GetUnsafe(key)
}

func (s *Snapshot) Has(key []byte) (bool, error) {
	return s.tree.Has(key)
}

func (s *Snapshot) HasMany(keys [][]byte) ([]bool, error) {
	return s.tree.HasMany(keys)
}

func (s *Snapshot) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	return s.tree.HasPrefixes(prefixes)
}

// GetEntry returns the persisted leaf entry for key.
func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	return s.tree.GetEntry(key)
}

// GetEntryExact is an alias for GetEntry.
func (s *Snapshot) GetEntryExact(key []byte) (node.LeafEntry, error) {
	return s.GetEntry(key)
}

// Getters
func (db *DB) Pager() *pager.Pager {
	idx := db.idx.Load()
	if idx == nil {
		return nil
	}
	return idx.pager
}
func (db *DB) Zipper() *zipper.Zipper {
	idx := db.idx.Load()
	if idx == nil {
		return nil
	}
	return idx.zipper
}

// SetZipperParallelMergePressureSource installs an optional pressure signal for
// future zipper generations and the current live zipper.
func (db *DB) SetZipperParallelMergePressureSource(src zipper.ParallelMergePressureSource) {
	if db == nil {
		return
	}
	db.idxMu.Lock()
	db.zipperParallelMergeSource = src
	if idx := db.idx.Load(); idx != nil && idx.zipper != nil {
		idx.zipper.SetParallelMergePressureSource(src)
	}
	db.idxMu.Unlock()
}

func (db *DB) InlineThreshold() int {
	if db == nil {
		return page.DefaultInlineThreshold
	}
	if db.adaptive != nil {
		return db.adaptive.GetThreshold()
	}
	if db.policy.InlineThreshold >= 0 {
		return db.policy.InlineThreshold
	}
	return page.DefaultInlineThreshold
}

func (db *DB) InlineThresholdForKey(key []byte) int {
	if db == nil {
		return page.DefaultInlineThreshold
	}
	return ResolveInlineThresholdForKey(db.InlineThreshold(), key, db.valueLogDomainThresholds)
}

func (db *DB) State() *DBState {
	return db.state.Load()
}

// IsClosing reports whether the database is closing. It returns true if db is nil.
func (db *DB) IsClosing() bool {
	return db == nil || db.closing.Load()
}

func (db *DB) publishSnapshotView(idx *indexGen, state *DBState, vm *valuelog.Manager) {
	if db == nil {
		return
	}
	if idx == nil || state == nil {
		db.snapshotViewRO.Store(nil)
		return
	}
	old := db.snapshotViewRO.Load()
	if old != nil && old.state != nil && old.state.LeafGenerations != nil && old.state.LeafGenerations != state.LeafGenerations {
		db.markLeafGenerationPinSetStale(old.state.LeafGenerations.PinSet)
	}
	db.snapshotViewRO.Store(&snapshotView{
		idx:         idx,
		state:       state,
		vlogManager: vm,
	})
	if state.LeafGenerations != nil && db.snapshotAcquireInFlight() == 0 {
		db.leafGenerationPins.pruneInactiveGenerationIDs(state.LeafGenerations.GenerationOrder)
	}
}

func (db *DB) clearSnapshotView() {
	if db == nil {
		return
	}
	db.snapshotViewRO.Store(nil)
}

// RefreshValueLogSet publishes a new DBState with the current value-log set
// (excluding zombies) without creating a new commit.
func (db *DB) RefreshValueLogSet() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.valueLogManager == nil {
		return nil
	}
	oldState := db.state.Load()
	if oldState == nil {
		return nil
	}
	if err := db.valueLogManager.Refresh(); err != nil {
		return err
	}

	newState := &DBState{
		CommitSeq:                  oldState.CommitSeq,
		RootPageID:                 oldState.RootPageID,
		SystemRootPageID:           oldState.SystemRootPageID,
		ValueLogSet:                db.valueLogManager.CurrentSetNoRefresh(),
		LeafGenerations:            oldState.LeafGenerations,
		LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)

	if oldState.ValueLogSet != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}

// publishValueLogSetNoRefresh publishes a new DBState using the currently
// registered value-log set, avoiding a filesystem scan when possible.
func (db *DB) publishValueLogSetNoRefresh() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.valueLogManager == nil {
		return nil
	}
	oldState := db.state.Load()
	if oldState == nil {
		return nil
	}

	valueLogSet := db.valueLogManager.CurrentSetNoRefresh()
	if valueLogSet == nil || len(valueLogSet.Files) == 0 {
		// Safety fallback: discover segments created out-of-process.
		if valueLogSet != nil {
			_ = db.valueLogManager.Release(valueLogSet)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return err
		}
		valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
	}

	newState := &DBState{
		CommitSeq:                  oldState.CommitSeq,
		RootPageID:                 oldState.RootPageID,
		SystemRootPageID:           oldState.SystemRootPageID,
		ValueLogSet:                valueLogSet,
		LeafGenerations:            oldState.LeafGenerations,
		LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)

	if oldState.ValueLogSet != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}

// MarkValueLogZombie marks a value-log segment as zombie so it can be removed
// once all snapshots release it.
func (db *DB) MarkValueLogZombie(id uint32) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("value log manager unavailable")
	}
	return db.valueLogManager.MarkZombie(id)
}

// CompactIndex rewrites the entire B-Tree sequentially to the end of the file.
// This improves Full Scan performance by restoring physical locality.
// Note: This operation causes file growth as old pages are not immediately reclaimed
// (they are leaked to the freelist but not reused during this append-only build).
func (db *DB) CompactIndex() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	idx := db.idx.Load()
	if idx == nil {
		return errors.New("missing index")
	}

	// Acquire Snapshot
	db.mu.RLock()
	state := db.state.Load()
	reader := newValueReader(state.ValueLogSet)
	tr := tree.New(idx.pager, reader, state.RootPageID)
	rootID := state.RootPageID
	db.mu.RUnlock()

	// Collect pages in the old tree so they can be retired after the swap.
	retired, err := tr.CollectPageIDs()
	if err != nil {
		return err
	}

	// Scan in pointer-projection mode so pointer-backed layouts preserve raw
	// pointer metadata during rebuild.
	iter := tr.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
	defer iter.Close()

	// Build new tree sequentially
	alloc := &pagerAllocator{p: idx.pager}
	newRoot, err := bulk.BuildWithOptions(iter, alloc, idx.pager, bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
		LeafColumnar:          db.indexColumnarLeaves,
		PackedValuePtr:        db.indexPackedValuePtr,
		InternalBaseDelta:     db.indexInternalBaseDelta,
	})
	if err != nil {
		return err
	}

	db.mu.Lock()
	if db.meta.UserRootPageID != rootID {
		db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during compaction")
	}
	sysRoot := db.meta.SystemRootPageID
	db.mu.Unlock()

	// Commit new root and retire the old tree pages.
	return db.finalizeCommit(newRoot, sysRoot, retired, true, adaptive.Metrics{}, nil, true, nil, nil, nil)
}

type pagerAllocator struct {
	p *pager.Pager
}

func (a *pagerAllocator) Alloc(hint uint64) (uint64, error) {
	return a.p.Alloc(1)
}
