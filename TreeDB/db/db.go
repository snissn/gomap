package db

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/keyupdate"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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

	defaultFlushApplyMinEntries = 4096
	defaultFlushApplyMinSpans   = 2
	defaultFlushApplyMinBytes   = 64 * 1024

	closeSnapshotDrainTimeout = 10 * time.Second
	closeSnapshotDrainSleep   = 500 * time.Microsecond

	snapshotAcquireShardCount = 256
	snapshotAcquireShardMask  = snapshotAcquireShardCount - 1
	snapshotRootTreeRetainMax = 16
)

func normalizeFlushApplyConcurrency(workers int) int {
	return normalizeFlushApplyConcurrencyForGOMAXPROCS(workers, runtimeGOMAXPROCS())
}

type DBState struct {
	CommitSeq                  uint64
	RootPageID                 uint64
	SystemRootPageID           uint64
	AppliedCommandLSN          uint64
	MaxEntryRevision           page.EntryRevision
	ValueLogSet                *valuelog.Set
	LeafGenerations            *leafGenerationView
	LeafGenerationStateVersion uint64
}

// StateToken is an allocation-free, immutable scalar view of one coherently
// published DB state. It intentionally excludes pointer-bearing state.
type StateToken struct {
	CommitSeq                  uint64
	RootPageID                 uint64
	SystemRootPageID           uint64
	AppliedCommandLSN          uint64
	MaxEntryRevision           page.EntryRevision
	LeafGenerationStateVersion uint64
}

// snapshotView is the coherent publication unit for snapshot acquisition.
// AcquireSnapshot reads this single pointer so index, state, value-log manager,
// and publication epoch always come from the same publish event.
type snapshotView struct {
	idx                    *indexGen
	state                  *DBState
	vlogManager            *valuelog.Manager
	systemRootPublishEpoch uint64
}

type DB struct {
	valueLogManager                *valuelog.Manager
	valueLogIdentityPins           *rootpublication.IdentityPinRegistry
	snapshotViewRO                 atomic.Pointer[snapshotView]
	snapshotAcquireRO              [snapshotAcquireShardCount]atomic.Int32
	snapshotAcquireEpoch           atomic.Uint64
	valueLogRefTracker             *valueLogRefTracker
	valueLogAppender               atomic.Pointer[valueLogAppenderHolder]
	leafPageLog                    LeafPageLog
	leafPageLogVersion             uint64
	leafPageReadCache              *leafPageReadCache
	leafGenerationManifest         *leafGenerationManifest
	leafGenerationManifestStore    *leafGenerationManifestStore
	leafGenerationPendingMu        sync.Mutex
	leafGenerationPendingFileIDs   []uint32
	leafGenerationPendingSet       map[uint32]struct{}
	leafGenerationPendingCommitSeq map[uint32]uint64
	lock                           *lockfile.Lock
	adaptive                       *adaptive.Controller
	pruner                         pruneWorker
	leafGenerationPins             leafGenerationPinTracker
	// Test hooks used for exact compaction phase-boundary coordination.
	compactStorageBeforePhase           func(string)
	compactStorageAfterPhase            func(string)
	compactStorageVacuumIndexOnlineHook func(context.Context, bool) error
	// Test hook used to observe whether fenced value-log reclaim resolved
	// referenced segments through the tracker or the full-scan fallback.
	compactStorageFencedValueLogRefHook func(compactStorageFencedValueLogRefEvent)
	// Test hook used to invalidate a shared CompactStorage audit immediately
	// before its exact publication-basis revalidation.
	compactStorageAuditBeforeRevalidate func(attempt int)
	// Test hook used to advance protected-root providers at deterministic
	// capture boundaries.
	compactStorageAuditProtectedBasisHook func(stage string, attempt int)
	// Test hook used to invalidate a value-log GC recoverable-root capability
	// immediately before its destructive revalidation.
	testValueLogGCBeforeRevalidateHook func()

	// idx is the current index generation (pager + MVCC lifecycle state).
	idx atomic.Pointer[indexGen]

	idxMu   sync.Mutex
	idxAll  map[uint64]*indexGen
	idxNext uint64

	snapPool     *SnapshotPool
	ghostManager *indexGhostManager

	dir                  string
	commandWALDir        string
	columnAssetRootDir   string
	chunkSize            int64
	preferAppendAlloc    bool
	freelistRegionPages  uint64
	freelistRegionRadius int

	readOnly                       bool
	resolvedProfile                DurabilityProfile
	deprecatedProfileAlias         DurabilityProfile
	durability                     DurabilityMode
	commandWAL                     bool
	commandWALStatsScan            bool
	walMaxSegmentBytes             int64
	commandWALSegmentTargetBytes   int64
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
	stableDictionaryResourcesMu    sync.RWMutex
	stableDictionaryResources      StableDictionaryResourceProvider
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

	mu                        sync.RWMutex
	writeMu                   sync.RWMutex
	teardownMu                sync.RWMutex // Pins Close-sensitive resources outside writeMu.
	commitMu                  sync.Mutex
	durablePublishMu          sync.Mutex
	rootPublication           *rootPublicationRuntimeV1
	rootPublicationFixedDelay time.Duration
	// rootReuseMu seals acquisition of a visible root generation while durable
	// publication converts retired COW pages into reusable pages. Existing
	// readers remain concurrent; only new generation captures wait for the
	// matching durable and visible root install.
	rootReuseMu      sync.RWMutex
	publishPrepareMu sync.RWMutex
	// valueLogPublicationMu prevents snapshots and RefreshValueLogSet from
	// observing segments while maintenance is promoting/registering them before
	// the root/meta publication that makes them authoritative.
	valueLogPublicationMu           sync.RWMutex
	pendingValueLogAppendMu         sync.Mutex
	pendingValueLogAppendFileIDRefs map[uint32]int
	pendingValueLogAppendPtrRefs    map[page.ValuePtr]int
	updateLocks                     keyupdate.Locks
	maintenanceMu                   sync.Mutex
	stableIndexCaptures             atomic.Int64
	durableCandidateIndexCaptures   atomic.Int64
	combineMu                       sync.RWMutex
	combineReqCh                    chan *commitCombineReq
	combineStopCh                   chan struct{}
	combineDoneCh                   chan struct{}
	vacuumInProgress                atomic.Bool
	// vacuumCutoverInProgress is narrower than vacuumInProgress: online vacuum
	// keeps accepting ordinary durable publications while it builds the
	// replacement, then raises this gate only while it seals and installs the
	// replacement namespace.
	vacuumCutoverInProgress atomic.Bool
	// vacuumCutoverGateMu protects the completion channel for the narrow online
	// vacuum cutover. A writer that acquires writeMu while the replacement pager
	// is being synced drops writeMu, waits for this channel, then retries
	// admission. That lets the cutover perform dependency/index/meta sync with no
	// DB write lock held without exposing an old-generation mutation window.
	vacuumCutoverGateMu    sync.Mutex
	vacuumCutoverDone      chan struct{}
	vacuum                 vacuumRecorder
	systemRootPublishEpoch atomic.Uint64
	vacuumOnlineLast       atomic.Pointer[VacuumOnlineStats]
	// Package-private deterministic hooks for online-vacuum concurrency tests.
	vacuumCollectionClonePageHook func(vacuumCollectionClonePhase, uint64)
	vacuumBeforeCutoverHook       func(int)
	vacuumBeforeRecorderFenceHook func()
	vacuumPagerSyncHook           func(vacuumPagerSyncPhase)
	vacuumPreflushHook            func() error
	vacuumReplacementRuntimeHook  func(*rootPublicationRuntimeV1) error
	meta                          page.MetaPageBody
	metaPageID                    uint64
	durableRoot                   durableRootRuntimeV1
	entryRevisionFloor            atomic.Uint64
	commandJournal                *commitlog.CommandJournal
	conditionalActiveTxnCount     atomic.Int64
	conditionalOracle             conditionalConflictOracle

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
	// closeHooksBefore run before ordinary closeHooks. Use this for hooks that
	// must drain buffered writes before lower-level resource cleanup hooks run.
	closeHooksBefore []func() error
	closeHooks       []func() error
	// closeHooksClosed is set when close hook draining begins. Close hooks run
	// while writes are still available, so registrations after that point would
	// otherwise be silently lost.
	closeHooksClosed bool
	// closeHooksRunning is protected by closeHooksMu. A reentrant Close from a
	// user callback is folded into the active outer close operation.
	closeHooksRunning bool
	// closeHooksOwner identifies the goroutine executing user callbacks. It is
	// non-zero only while closeHooksRunning is true.
	closeHooksOwner          uint64
	closeHooksCloseRequested bool
	// closeHooksWaitHook is a deterministic test seam for the concurrent waiter.
	closeHooksWaitHook func()
	closeHooksWG       sync.WaitGroup
	closeTeardownOnce  sync.Once
	closeTeardownErr   error

	internalTeardownHooksMu     sync.Mutex
	internalTeardownHooks       []func() error
	internalTeardownHooksClosed bool
	// captureTeardownHooks are registered by producers while they hold a
	// teardownMu read lease. Close drains them only after acquiring teardownMu
	// exclusively, so a recovery callback cannot race its admitted producer.
	captureTeardownHooksMu     sync.Mutex
	captureTeardownHooks       []func() error
	captureTeardownHooksClosed bool

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
	orderedRootDeltaGroupCalls                                  atomic.Uint64
	orderedRootDeltaGroupErrors                                 atomic.Uint64
	orderedRootDeltaGroupRoots                                  atomic.Uint64
	orderedRootDeltaGroupWaitTotalNs                            atomic.Uint64
	orderedRootDeltaGroupHoldTotalNs                            atomic.Uint64
	orderedRootDeltaGroupLatencyMaxNs                           atomic.Uint64
	orderedRootDeltaGroupLatencyBuckets                         [publishWatermarkLatencyBucketCount]atomic.Uint64
	orderedRootDeltaGroupPreflightNs                            atomic.Uint64
	orderedRootDeltaGroupRootApplyNs                            atomic.Uint64
	orderedRootDeltaGroupRootApplyCalls                         atomic.Uint64
	orderedRootDeltaGroupRootApplyParallelGroups                atomic.Uint64
	orderedRootDeltaGroupRootApplyParallelRoots                 atomic.Uint64
	orderedRootDeltaGroupRootApplyOps                           atomic.Uint64
	orderedRootDeltaGroupRootApplyNodeLoads                     atomic.Uint64
	orderedRootDeltaGroupRootApplyPagerNodeLoads                atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogNodeLoads              atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogCacheHits              atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogReaderCalls            atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogViewReads              atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogScratchReads           atomic.Uint64
	orderedRootDeltaGroupRootApplyPagerNodeBytesRead            atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogNodeBytesRead          atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesRead    atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafMerges                    atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalMerges                atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalParallelMerges        atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalParallelChildren      atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalParallelWorkers       atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalParallelOps           atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafPagesWritten              atomic.Uint64
	orderedRootDeltaGroupRootApplyPagerLeafPagesWritten         atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogPagesWritten           atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafPageBytesWritten          atomic.Uint64
	orderedRootDeltaGroupRootApplyPagerLeafPageBytesWritten     atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogPageBytesWritten       atomic.Uint64
	orderedRootDeltaGroupRootApplyLeafLogRecordHintBytesWritten atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalPagesWritten          atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalPageBytesWritten      atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalChildRefs             atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalPageChildRefs         atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalLeafLogRefs           atomic.Uint64
	orderedRootDeltaGroupRootApplyInternalLeafLogRefCopies      atomic.Uint64
	orderedRootDeltaGroupRootApplyRootSplitLevels               atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareNs             atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareCalls          atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareErrors         atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareValidationFail atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareRequested      atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareRequestedMax   atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareSpans          atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanOps        atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareSpanBytes      atomic.Uint64
	orderedRootDeltaGroupRootApplyReadOnlyPrepareWorkerRanges   atomic.Uint64
	orderedRootDeltaGroupSystemBuildNs                          atomic.Uint64
	orderedRootDeltaGroupSystemApplyNs                          atomic.Uint64
	orderedRootDeltaGroupSystemApplyCalls                       atomic.Uint64
	orderedRootDeltaGroupSystemApplyOps                         atomic.Uint64
	orderedRootDeltaGroupSystemApplyNodeLoads                   atomic.Uint64
	orderedRootDeltaGroupPublishPrepareNs                       atomic.Uint64
	orderedRootDeltaGroupPublishPrepareCalls                    atomic.Uint64
	orderedRootDeltaGroupPublishPrepareErrors                   atomic.Uint64
	orderedRootDeltaGroupFinalizeNs                             atomic.Uint64
	orderedRootDeltaGroupFinalizeCalls                          atomic.Uint64
	logicalOrderedRootObserverMu                                sync.Mutex
	logicalOrderedRootObserverID                                uint64
	logicalOrderedRootObserver                                  func()
	orderedRootSpanNativeCandidateOps                           atomic.Uint64
	orderedRootSpanNativeCandidateSpans                         atomic.Uint64
	orderedRootSpanNativeEligibleOps                            atomic.Uint64
	orderedRootSpanNativeEligibleSpans                          atomic.Uint64
	orderedRootSpanNativeUsedOps                                atomic.Uint64
	orderedRootSpanNativeUsedSpans                              atomic.Uint64
	orderedRootSpanNativeIneligibleOps                          atomic.Uint64
	orderedRootSpanNativeIneligibleSpans                        atomic.Uint64
	orderedRootSpanNativeFallbacks                              atomic.Uint64
	orderedRootSpanNativeFallbackReasonCounts                   [FlushSpanRunFallbackReasonCount]atomic.Uint64
	orderedRootSpanNativeFallbackOps                            [FlushSpanRunFallbackReasonCount]atomic.Uint64
	orderedRootSpanNativeFallbackSpans                          [FlushSpanRunFallbackReasonCount]atomic.Uint64
	orderedRootSpanNativeRouteCounters                          [orderedRootSpanNativeRouteCount]orderedRootSpanNativeRouteCounters
	rawSpanNativeCandidateOps                                   atomic.Uint64
	rawSpanNativeCandidateSpans                                 atomic.Uint64
	rawSpanNativeEligibleOps                                    atomic.Uint64
	rawSpanNativeEligibleSpans                                  atomic.Uint64
	rawSpanNativeUsedOps                                        atomic.Uint64
	rawSpanNativeUsedSpans                                      atomic.Uint64
	rawSpanNativeIneligibleOps                                  atomic.Uint64
	rawSpanNativeIneligibleSpans                                atomic.Uint64
	rawSpanNativeFallbacks                                      atomic.Uint64
	rawSpanNativeFallbackReasonCounts                           [FlushSpanRunFallbackReasonCount]atomic.Uint64
	rawSpanNativeFallbackOps                                    [FlushSpanRunFallbackReasonCount]atomic.Uint64
	rawSpanNativeFallbackSpans                                  [FlushSpanRunFallbackReasonCount]atomic.Uint64
	rawSpanNativeRouteCounters                                  [rawSpanNativeRouteCount]rawSpanNativeRouteCounters

	// Cached-flush/root-apply M0 counters. These are coarse per-apply counters
	// used by benchmark artifacts; avoid per-node timing in zipper recursion.
	flushApplyCalls                               atomic.Uint64
	flushApplyErrors                              atomic.Uint64
	flushApplyOps                                 atomic.Uint64
	flushApplyNs                                  atomic.Uint64
	flushApplyOldNodeLoads                        atomic.Uint64
	flushApplyOldPagerNodeLoads                   atomic.Uint64
	flushApplyOldLeafLogNodeLoads                 atomic.Uint64
	flushApplyOldLeafLogCacheHits                 atomic.Uint64
	flushApplyOldLeafLogReaderCalls               atomic.Uint64
	flushApplyOldLeafLogViewReads                 atomic.Uint64
	flushApplyOldLeafLogScratchReads              atomic.Uint64
	flushApplyOldPagerNodeBytesRead               atomic.Uint64
	flushApplyOldLeafLogNodeBytesRead             atomic.Uint64
	flushApplyOldLeafLogRecordHintBytesRead       atomic.Uint64
	flushApplyLeafMerges                          atomic.Uint64
	flushApplyInternalMerges                      atomic.Uint64
	flushApplyInternalParallelMerges              atomic.Uint64
	flushApplyInternalParallelChildren            atomic.Uint64
	flushApplyInternalParallelWorkers             atomic.Uint64
	flushApplyInternalParallelOps                 atomic.Uint64
	flushApplyLeafPagesWritten                    atomic.Uint64
	flushApplyPagerLeafPagesWritten               atomic.Uint64
	flushApplyLeafLogPagesWritten                 atomic.Uint64
	flushApplyLeafPageBytesWritten                atomic.Uint64
	flushApplyPagerLeafPageBytesWritten           atomic.Uint64
	flushApplyLeafLogPageBytesWritten             atomic.Uint64
	flushApplyLeafLogRecordHintBytesWritten       atomic.Uint64
	flushApplyPreparedOutputLeafLogPagesPrepared  atomic.Uint64
	flushApplyPreparedOutputLeafLogBytesPrepared  atomic.Uint64
	flushApplyPreparedOutputLeafLogPagesInstalled atomic.Uint64
	flushApplyPreparedOutputLeafLogBytesInstalled atomic.Uint64
	flushApplyPreparedOutputLeafLogPagesAbandoned atomic.Uint64
	flushApplyPreparedOutputLeafLogBytesAbandoned atomic.Uint64
	flushApplyPreparedOutputRetiredPagesPrepared  atomic.Uint64
	flushApplyPreparedOutputRetiredPagesInstalled atomic.Uint64
	flushApplyPreparedOutputRetiredPagesAbandoned atomic.Uint64
	flushApplyInternalPagesWritten                atomic.Uint64
	flushApplyInternalPageBytesWritten            atomic.Uint64
	flushApplyInternalChildRefs                   atomic.Uint64
	flushApplyRootSplitLevels                     atomic.Uint64
	flushApplyRootReduceNs                        atomic.Uint64
	flushApplyReadOnlyPrepareCalls                atomic.Uint64
	flushApplyReadOnlyPrepareErrors               atomic.Uint64
	flushApplyReadOnlyPrepareValidationFail       atomic.Uint64
	flushApplyReadOnlyPrepareNs                   atomic.Uint64
	flushApplyReadOnlyPrepareRequested            atomic.Uint64
	flushApplyReadOnlyPrepareRequestedMax         atomic.Uint64
	flushApplyReadOnlyPrepareSpans                atomic.Uint64
	flushApplyReadOnlyPrepareSpansMax             atomic.Uint64
	flushApplyReadOnlyPrepareSpanOps              atomic.Uint64
	flushApplyReadOnlyPrepareSpanOpsMax           atomic.Uint64
	flushApplyReadOnlyPrepareSpanBytes            atomic.Uint64
	flushApplyReadOnlyPrepareSpanBytesMax         atomic.Uint64
	flushApplyReadOnlyPrepareSingleOpSpans        atomic.Uint64
	flushApplyReadOnlyPrepareSingleOpSpansMax     atomic.Uint64
	flushApplyReadOnlyPrepareWorkerRanges         atomic.Uint64
	flushApplyReadOnlyPrepareWorkerRangesMax      atomic.Uint64
	flushApplySpanNativeCandidateOps              atomic.Uint64
	flushApplySpanNativeCandidateSpans            atomic.Uint64
	flushApplySpanNativeEligibleOps               atomic.Uint64
	flushApplySpanNativeEligibleSpans             atomic.Uint64
	flushApplySpanNativeUsedOps                   atomic.Uint64
	flushApplySpanNativeUsedSpans                 atomic.Uint64
	flushApplySpanNativeIneligibleOps             atomic.Uint64
	flushApplySpanNativeIneligibleSpans           atomic.Uint64
	flushApplySpanNativeFallbacks                 atomic.Uint64
	flushApplySpanNativeFallbackOps               [FlushSpanRunFallbackReasonCount]atomic.Uint64
	flushApplySpanNativeFallbackSpans             [FlushSpanRunFallbackReasonCount]atomic.Uint64
	flushApplyCommitWaitNs                        atomic.Uint64
	flushApplyPublishPrepareCalls                 atomic.Uint64
	flushApplyPublishPrepareErrors                atomic.Uint64
	flushApplyPublishPrepareNs                    atomic.Uint64
	flushApplyPublishFinalInstallCalls            atomic.Uint64
	flushApplyPublishFinalInstallNs               atomic.Uint64
	flushApplyGuardedPublishCalls                 atomic.Uint64
	flushApplyGuardedPublishNs                    atomic.Uint64
	flushApplyLeafLogOutputReservationWaitNs      atomic.Uint64
	flushApplyLeafLogOutputAppendWaitNs           atomic.Uint64
	flushApplyLeafLogOutputAppendCalls            atomic.Uint64
	flushApplyLeafLogOutputAppendPages            atomic.Uint64
	flushApplyLeafLogOutputLaneTasks              [adaptive.ZipperLeafLogOutputLaneStatsMax + 1]atomic.Uint64
	flushApplyLeafLogOutputLaneTaskOverflow       atomic.Uint64
	flushApplySpanNativeWorkerBusyNs              atomic.Uint64
	flushApplySpanNativeWorkerIdleNs              atomic.Uint64
	flushApplySpanNativeWorkerWaitNs              atomic.Uint64
	flushApplySpanNativeReadyTasks                atomic.Uint64
	flushApplySpanNativeDispatchedTasks           atomic.Uint64
	flushApplySpanNativeCompletedTasks            atomic.Uint64
	flushApplySpanNativeQueueDepthMax             atomic.Uint64
	flushApplySpanNativeScheduledWorkers          atomic.Uint64
	flushApplySpanNativeScheduledWorkersMax       atomic.Uint64
	flushApplySpanNativeTaskSpansTotal            atomic.Uint64
	flushApplySpanNativeTaskSpansMin              atomic.Uint64
	flushApplySpanNativeTaskSpansMax              atomic.Uint64
	flushApplySpanNativeTaskOpsTotal              atomic.Uint64
	flushApplySpanNativeTaskOpsMin                atomic.Uint64
	flushApplySpanNativeTaskOpsMax                atomic.Uint64
	flushApplySpanNativeTaskBytesTotal            atomic.Uint64
	flushApplySpanNativeTaskBytesMin              atomic.Uint64
	flushApplySpanNativeTaskBytesMax              atomic.Uint64
	flushApplySpanNativeSingleSpanTasks           atomic.Uint64
	flushApplyRetries                             atomic.Uint64
	flushApplyMismatches                          atomic.Uint64

	flushApplySpanNativeReducerValidationGuard      atomic.Bool
	flushApplySpanNativeReducerValidationGuardTrips atomic.Uint64

	flushAdmission FlushAdmissionDecision

	flushApplyConcurrency int
	flushApplyMinEntries  int
	flushApplyMinSpans    int
	flushApplyMinBytes    int
	flushApplySpanNative  bool
	flushApplyWorkerPool  *zipper.ApplyWorkerPool

	flushApplyReadOnlyPrepareMu   sync.Mutex
	flushApplyReadOnlyPrepareFree []*flushApplyReadOnlyPrepareBuffer

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
	// testFailSyncMeta fails after the alternate meta page has been written but
	// before its durability outcome is known.
	testFailSyncMeta atomic.Bool
	// testFailDurableRootVisibleInstall fails after the alternate meta page is
	// durably selected but before the matching in-memory state is installed.
	// The writable handle must poison because recovery now owns reconciliation.
	testFailDurableRootVisibleInstall atomic.Bool
	// testFailDurableRootAfterCOWPrepare fails after allocator preparation but
	// before the DB candidate owns that prepared state. Preparation must abort
	// atomically so a later publication cannot inherit stale retirements.
	testFailDurableRootAfterCOWPrepare             atomic.Bool
	testFailCommandWALFlush                        atomic.Bool
	testAfterOptimisticBaseCaptureHook             func()
	testAfterOptimisticApplyHook                   func()
	testAfterOptimisticPublishPrepareHook          func()
	testCommandWALAfterBuilderAcquireHook          func()
	testCommandWALBeforeDurablePublishLockHook     func()
	testCommandWALCleanupAfterScanHook             func()
	testBeforeFinalizeCommitHook                   func()
	testAfterFinalizeRootSerializationReleaseHook  func()
	testDurableRootCandidatePreparedHook           func()
	testRootPublicationDependencyBytes             atomic.Uint64
	testScanCandidateExternalReferencesHook        func()
	testCheckpointAfterPoisonPreflightHook         func()
	testConditionalReadOnlyAfterClosePreflight     func()
	testOrderedRootBatchAfterClosePreflightHook    func()
	testStorageMaintenanceBeforeLockHook           func(string)
	testStorageMaintenanceAfterLockHook            func(string) error
	testCompactStorageRemoveValueLogSegmentHook    func(uint32) (bool, error)
	testCommandWALRecoveryFailAfterLSN             atomic.Uint64
	testCommandWALRecoveryFailBeforeDependencySync atomic.Bool
	commandWALReplayLSN                            atomic.Uint64
	commandWALReplayToken                          atomic.Uint64
	commandWALReplayTokenSeq                       atomic.Uint64
	// commandWALFlushPoisoned is intentionally cleared only by closing and
	// reopening the DB. After an append reached the journal but flush/sync or
	// root publication failed, continuing on the same handle could create an
	// unrecoverable LSN gap.
	commandWALFlushPoisoned atomic.Bool
	commandWALDurableLSN    atomic.Uint64
	// commandWALSessionAppliedLSN is the durable-root applied frontier used to
	// seed this journal owner's LSN sequence. When recovery classified a lower
	// physical durable-WAL frontier, checkpoint must not bridge that pre-session
	// gap merely by syncing an empty or newly-created successor segment.
	commandWALSessionAppliedLSN uint64
	commandWALDebt              CommandWALDependencyDebt
	// publicationPoisoned is set after an outcome-ambiguous root/meta publish or
	// after bounded pre-meta retry exhaustion leaves a prepared COW candidate.
	// It is intentionally cleared only by close/reopen.
	publicationPoisoned atomic.Bool

	durableRootManifestBuildCount     atomic.Uint64
	durableRootManifestBuildNs        atomic.Uint64
	durableRootManifestEntriesSeen    atomic.Uint64
	durableRootManifestEntriesEncoded atomic.Uint64
	durableRootManifestBytesEncoded   atomic.Uint64

	commandWALStatsMu                sync.Mutex
	commandWALRequiredFeature        bool
	commandWALRequiredErr            string
	commandWALStatsAppliedLSN        uint64
	commandWALStatsSummary           commandWALStatsSummary
	commandWALStatsOK                bool
	commandWALLiveAccepted           atomic.Uint64
	commandWALLiveAcceptedMax        atomic.Uint64
	commandWALLiveCovered            atomic.Uint64
	commandWALLiveCoveredMax         atomic.Uint64
	commandWALAppendCount            atomic.Uint64
	commandWALAppendNs               atomic.Uint64
	commandWALAppendPointCount       atomic.Uint64
	commandWALAppendPointNs          atomic.Uint64
	commandWALAppendPayloadCount     atomic.Uint64
	commandWALAppendPayloadNs        atomic.Uint64
	commandWALAppendEntryScanCount   atomic.Uint64
	commandWALAppendEntryScanNs      atomic.Uint64
	commandWALAppendIntentCount      atomic.Uint64
	commandWALAppendIntentNs         atomic.Uint64
	commandWALFlushCount             atomic.Uint64
	commandWALFlushNs                atomic.Uint64
	commandWALSyncCount              atomic.Uint64
	commandWALSyncNs                 atomic.Uint64
	commandWALFlushPathCount         [commandWALStatsPathCount]atomic.Uint64
	commandWALFlushPathNs            [commandWALStatsPathCount]atomic.Uint64
	commandWALSyncPathCount          [commandWALStatsPathCount]atomic.Uint64
	commandWALSyncPathNs             [commandWALStatsPathCount]atomic.Uint64
	commandWALCleanupScans           atomic.Uint64
	commandWALCleanupScanNs          atomic.Uint64
	commandWALCleanupScanBytes       atomic.Uint64
	commandWALCleanupScanFrames      atomic.Uint64
	commandWALCleanupProofs          atomic.Uint64
	commandWALCleanupProofNs         atomic.Uint64
	commandWALCleanupNs              atomic.Uint64
	commandWALCleanupCovered         atomic.Uint64
	commandWALCleanupCoveredBytes    atomic.Uint64
	commandWALCleanupRetained        atomic.Uint64
	commandWALCleanupRetainedBytes   atomic.Uint64
	commandWALCleanupRetainedActive  atomic.Uint64
	commandWALCleanupRetainedUncover atomic.Uint64
	commandWALCleanupRetainedPinned  atomic.Uint64
	commandWALCleanupRetainedError   atomic.Uint64
	commandWALCleanupRetries         atomic.Uint64
	commandWALCleanupNamespaceSyncs  atomic.Uint64
	commandWALCleanupNamespaceErrors atomic.Uint64
	commandWALCleanupProofFrontier   atomic.Uint64
	commandWALCleanupProofDurableLSN atomic.Uint64
	commandWALCleanupSelectedRoot    atomic.Uint64
	commandWALCleanupOlderRoot       atomic.Uint64
	commandWALCleanupCaptureEpoch    atomic.Uint64
	commandWALCleanupNamespaceGen    atomic.Uint64
	commandWALCleanupOldestPinnedLSN atomic.Uint64
	commandWALCleanupNamespaceDirty  atomic.Bool
	commandWALCleanupUnlinkedPending atomic.Uint64
	commandWALCleanupBytesPending    atomic.Uint64
	commandWALCleanupMu              sync.Mutex
	commandWALCleanupRemoved         atomic.Uint64
	commandWALCleanupBytes           atomic.Uint64
	commandWALClosedBytes            atomic.Int64
	conditionalTxnStarted            atomic.Uint64
	conditionalTxnClosed             atomic.Uint64
	conditionalTxnCommitAttempts     atomic.Uint64
	conditionalTxnCommits            atomic.Uint64
	conditionalTxnConflicts          atomic.Uint64
	conditionalTxnReadSetSamples     atomic.Uint64
	conditionalTxnReadSetEntries     atomic.Uint64
	conditionalTxnReadSetMax         atomic.Uint64
	conditionalTxnCommandWALPayloads atomic.Uint64
	conditionalOracleRecordedPoints  atomic.Uint64
	conditionalOracleRecordedRanges  atomic.Uint64
	conditionalOracleRootMarkers     atomic.Uint64
	conditionalOraclePrunes          atomic.Uint64
	conditionalOraclePrunedPoints    atomic.Uint64
	conditionalOraclePrunedRanges    atomic.Uint64
	commandWALRawPublishMu           sync.RWMutex
	commandWALRawBarrierMu           sync.Mutex
	commandWALRawBarrierNextID       uint64
	commandWALRawBarriers            []*commandWALRawBarrier
	closing                          atomic.Bool
}

// These hooks let package tests attach producer-side fixtures to the exact DB
// handle that owns their value-log manager. Production leaves them nil.
var (
	testDBOpenHook  func(*DB) error
	testDBCloseHook func(*DB)
)

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
	protectedRoots      [32]byte
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
var errTestCommandWALFlushFailpoint = errors.New("treedb: command wal flush failpoint")
var errTestCommandWALRecoveryDependencySyncFailpoint = errors.New("treedb: command wal recovery dependency sync failpoint")
var errTestWriteMetaFailpoint = errors.New("treedb: write meta failpoint")
var errTestSyncMetaFailpoint = errors.New("treedb: sync meta failpoint")
var errTestDurableRootVisibleInstallFailpoint = errors.New("treedb: durable root visible install failpoint")
var errTestDurableRootAfterCOWPrepareFailpoint = errors.New("treedb: durable root post-COW-prepare failpoint")

const (
	commandWALWriterBufferSize              = 16 << 20
	commandWALDeferredPointBufferSize       = 64 << 20
	commandWALDeferredPointBufferRetainSize = 4 << 20
)

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

type acceptedFinalizeCommitError struct {
	err error
}

func (e *acceptedFinalizeCommitError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *acceptedFinalizeCommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *acceptedFinalizeCommitError) CommitPublicationAccepted() bool {
	return e != nil
}

func wrapAcceptedFinalizeCommitError(err error) error {
	if err == nil {
		return nil
	}
	return &acceptedFinalizeCommitError{err: err}
}

// CommitPublicationAccepted reports whether err was returned after root
// publication ownership transferred and the candidate became visible. Such an
// error may still describe an unsatisfied admission or durability obligation;
// callers must not replay the logical mutation as though publication never
// happened.
func CommitPublicationAccepted(err error) bool {
	var accepted interface {
		CommitPublicationAccepted() bool
	}
	return errors.As(err, &accepted) && accepted.CommitPublicationAccepted()
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
	ValueLogBlockZSTD
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
	// CurrentWritableMmap enables mmap-backed reads for current writable
	// value-log segments. This reduces random-read ReadAt syscall pressure at
	// the cost of a larger mapped virtual-address window.
	CurrentWritableMmap bool

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
	// ResolvedProfile is the canonical public durability contract selected by
	// the TreeDB profile resolver. Public constructors populate it before any
	// backend or cached-layer routing decision is made.
	ResolvedProfile DurabilityProfile
	// DeprecatedProfileAlias records a legacy Go alias that resolved to
	// ResolvedProfile. It is diagnostic-only and never controls behavior.
	DeprecatedProfileAlias DurabilityProfile
	// UnsafeBenchmarkProfile proves that bench_unsafe entered through an explicit
	// benchmark/test constructor boundary. Ordinary production opens reject the
	// profile when this marker is false.
	UnsafeBenchmarkProfile bool
	// IgnoreFormatConfig disables best-effort persisted format.json loading in
	// TreeDB open paths that auto-apply index-format knobs from disk (e.g.
	// treedb.Open, treedb.OpenBackend) and in offline maintenance helpers
	// (VacuumIndexOffline, ValueLogRewriteOffline, treemap vacuum/rewrite/vlog-gc).
	IgnoreFormatConfig bool
	// CommandWAL enables the compatibility-breaking command-WAL mode for direct
	// backend raw KV writes. It is also enabled automatically when format.json
	// advertises the command_wal_v2 required feature.
	//
	// Public treedb.Open write handles route raw KV writes through the direct
	// backend command-WAL path while command_wal_v2 is active, avoiding the
	// legacy cached redo journal until cached writes are converted to typed
	// command frames.
	CommandWAL bool
	// AllowLegacyCachedRedoJournalReplay permits explicit forensic recovery of
	// pre-command-WAL cached redo-journal segments. Normal command-WAL and public
	// profile opens should leave this false so dirty legacy journals fail closed
	// instead of being silently replayed by the old compatibility path.
	//
	// Deprecated: this compatibility escape hatch exists only for pre-alpha
	// legacy directories and focused recovery tests.
	AllowLegacyCachedRedoJournalReplay bool
	// CommandWALStatsScan enables expensive diagnostic Stats() counters that scan
	// command-WAL segment files for frame counts and max LSN. Keep this disabled
	// for normal telemetry; benchmark proof paths can opt in explicitly.
	CommandWALStatsScan bool
	// PublicBatchWriteSyncPhaseStats enables request-scoped phase timing for
	// command-WAL public Batch.WriteSync calls. It is intended for focused
	// profiling only and remains disabled by default so normal writes do not pay
	// the additional clock and atomic-counter overhead.
	PublicBatchWriteSyncPhaseStats bool
	// ReadOnly opens the database without acquiring an exclusive lock and without
	// modifying on-disk state (no recovery truncation, no WAL replay, no background
	// maintenance). Only read operations are supported. Under the collection WAL
	// target contract, read-only open must fail with a recovery-required error if
	// committed unapplied collection WAL needs mutating recovery.
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
	// LeafPageReadCacheEntries controls the bounded process-local cache used for
	// B-tree leaf pages stored in the value log. The cache stores decoded 4KiB
	// leaf pages and is most useful for sparse update/publish/read workloads that
	// revisit recently-written outer leaves.
	//
	// Semantics:
	//   - 0 uses the process default/env override.
	//   - <0 disables the cache for this DB.
	//   - >0 sets the exact number of set-associative cache entries.
	LeafPageReadCacheEntries int
	// LeafPageReadCacheWriteAdmission controls write-side cache population for
	// outer-leaf pages stored in the value log. The field's zero value is the
	// historical immediate admission behavior for explicit/off policies; the
	// default auto flush-admission policy upgrades it to the measured adaptive
	// write-admission candidate. Adaptive admission only changes in-memory cache
	// population, not persistent leaf-log/value-log writes or pointer validity.
	LeafPageReadCacheWriteAdmission LeafPageReadCacheWriteAdmissionPolicy

	// Durability configures cached-mode durability semantics.
	//
	// The default (zero) is DurabilityDurable.
	Durability DurabilityMode
	// rootPublicationFixedDelay is a package-local benchmark control. Zero uses
	// the adaptive production policy; non-zero values run the same coordinator
	// and publisher path with a fixed timer delay.
	rootPublicationFixedDelay time.Duration
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

	// FlushAdmissionPolicy selects how TreeDB admits the span-native/backlog
	// flush/apply candidate path. The zero value (auto) admits the measured
	// hardware-aware adaptive candidate, selecting up to the detected physical
	// core count capped by GOMAXPROCS and a conservative upper bound on
	// sufficiently parallel hosts. If physical-core detection is unavailable, auto
	// falls back to the existing GOMAXPROCS-capped bound. Explicit preserves
	// caller-supplied knobs; Off force-disables span-native/backlog/concurrency as
	// a rollback policy.
	FlushAdmissionPolicy FlushAdmissionPolicy

	// FlushApplyConcurrency enables M2 parallel COW apply for backend flush/write
	// batches using a bounded reusable worker pool. It is separate from
	// FlushBuildConcurrency. Values <=1 keep the worker-pool path off for
	// explicit policy; the default auto admission policy chooses a hardware-aware
	// candidate capped by GOMAXPROCS when the low-concurrency guardrail passes.
	FlushApplyConcurrency int
	// FlushApplyMinEntries gates opt-in parallel apply by planned span-local ops.
	// Values <=0 use the internal default.
	FlushApplyMinEntries int
	// FlushApplyMinSpans gates opt-in parallel apply by planned leaf span count.
	// Values <=0 use the internal default.
	FlushApplyMinSpans int
	// FlushApplyMinBytes gates opt-in parallel apply by planned span bytes.
	// Values <=0 use the internal default.
	FlushApplyMinBytes int
	// FlushApplySpanNative enables the M10 span-native apply candidate path. The
	// default auto admission policy enables it only for the measured capped
	// adaptive candidate; explicit policy preserves caller-provided values.
	// Unsupported runs fall back to recursive apply.
	FlushApplySpanNative bool

	// FlushBackendMaxEntries caps how many operations are buffered into one
	// backend apply chunk before continuing with a fresh batch.
	//
	// TreeDB stages all chunks from one logical flush in a private root-build
	// transaction. Only the final complete root is publication-eligible; no
	// intermediate chunk is independently visible or recoverable.
	//
	// 0 uses the internal default. Negative disables chunking (single backend
	// apply per flush).
	FlushBackendMaxEntries int
	// FlushBackendMaxBatches caps how many backend apply chunks a single flush may
	// emit (0=default, <0=disable cap).
	FlushBackendMaxBatches int

	// FlushSpanRunTargetPlanning enables diagnostic read-only target-leaf planning
	// for canonical cached flush runs. It is default-off; M9's default write path
	// builds canonical multi-memtable runs but does not pay the extra target-span
	// traversal unless this diagnostic knob is explicitly enabled.
	FlushSpanRunTargetPlanning bool

	// FlushBacklogCoalescing enables the M11 bounded adaptive cached-flush
	// coalescing controller. The default auto admission policy enables it for the
	// measured capped adaptive candidate; when enabled the cache layer may
	// include additional already-sealed eligible memtables in one canonical flush
	// run under observed cumulative single-op-span pressure and explicit budgets.
	FlushBacklogCoalescing bool
	// FlushBacklogCoalescingMaxMemtables bounds memtables per coalesced point run
	// after preserving the pre-existing base collector minimum (0=default, capped
	// internally).
	FlushBacklogCoalescingMaxMemtables int
	// FlushBacklogCoalescingMaxBytes is a soft queued-byte budget per coalesced
	// point run. It is checked before adding the next memtable after at least one
	// unit, so a selected run can exceed this value by one whole memtable and the
	// budget never tightens the pre-existing base collector (0=default).
	FlushBacklogCoalescingMaxBytes int64
	// FlushBacklogCoalescingMaxOps is a soft queued point-op budget per coalesced
	// point run. It is checked before adding the next memtable after at least one
	// unit, so a selected run can exceed this value by one whole memtable and the
	// budget never tightens the pre-existing base collector (0=default).
	FlushBacklogCoalescingMaxOps int
	// FlushBacklogCoalescingMinAge requires the oldest queued memtable to be at
	// least this old before adaptive coalescing admits extra work (0=no age floor).
	FlushBacklogCoalescingMinAge time.Duration
	// FlushBacklogCoalescingSingleOpSpanRatio is the observed single-op span ratio
	// that triggers coalescing (0=default). Pressure gates use cumulative apply/span
	// counters; after workload-shape changes, eligibility may decay only as
	// cumulative ratios change, while each admitted run remains bounded by the
	// explicit budgets above.
	FlushBacklogCoalescingSingleOpSpanRatio float64
	// FlushBacklogCoalescingMaxOpsPerSpan is the observed ops/span ceiling that
	// still counts as single-op pressure (0=default).
	FlushBacklogCoalescingMaxOpsPerSpan float64
	// FlushBacklogCoalescingMinOldLeafBytesPerOp optionally requires observed
	// old-leaf decode bytes/op before coalescing (0=disabled).
	FlushBacklogCoalescingMinOldLeafBytesPerOp float64

	flushAdmissionDecision   FlushAdmissionDecision
	flushAdmissionNormalized bool

	// JournalLanes controls the number of active commit/value log lanes (0=default).
	// Max supported lanes is 255; value-log segment sequence per lane is capped at 8,388,607.
	JournalLanes int
	// WALMaxSegmentBytes caps the size of a single WAL segment payload.
	// 0 uses the default limit.
	WALMaxSegmentBytes int64
	// CommandWALSegmentTargetBytes bounds command-WAL active segment growth by
	// rotating to a fresh command-WAL segment before an append would make a
	// non-empty active segment exceed the target. It is separate from
	// WALMaxSegmentBytes, which remains a per-frame payload cap. 0 disables
	// runtime command-WAL rotation.
	CommandWALSegmentTargetBytes int64
	// JournalCompression enables best-effort zstd compression for generic cached-mode
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

	// testCommandWALRecoveryFailAfterLSN injects a one-shot recovery failure
	// after the given LSN is published. It is package-private test plumbing so
	// crash-recovery tests can avoid process-global failpoints.
	testCommandWALRecoveryFailAfterLSN uint64
	// testCommandWALRecoveryFailBeforeDependencySync injects a one-shot
	// recovery failure after exact relaxed-tail dependencies are captured but
	// before they are synced or any replayed root can be published.
	testCommandWALRecoveryFailBeforeDependencySync bool

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
	// BackgroundIndexVacuumSpanRatioPPM sets the user-tree span ratio threshold
	// that triggers a vacuum pass (0 uses a default).
	BackgroundIndexVacuumSpanRatioPPM uint32
	// BackgroundIndexVacuumMaxBacklogSkips bounds consecutive cached-backlog skips
	// before a trigger probe is forced (0 uses a conservative default).
	BackgroundIndexVacuumMaxBacklogSkips uint32
	// BackgroundIndexVacuumFreelistReclaimableRatioPPM and
	// BackgroundIndexVacuumFreelistReclaimablePages trigger vacuum when both
	// freelist reclaimable debt thresholds are met (0 uses conservative defaults).
	BackgroundIndexVacuumFreelistReclaimableRatioPPM uint32
	BackgroundIndexVacuumFreelistReclaimablePages    uint64
	// BackgroundIndexVacuumCollectionRootSpanRatioPPM and
	// BackgroundIndexVacuumCollectionRootPages trigger vacuum when both
	// collection-root span debt thresholds are met (0 uses conservative defaults).
	BackgroundIndexVacuumCollectionRootSpanRatioPPM uint32
	BackgroundIndexVacuumCollectionRootPages        uint64
	// MaxWALBytes triggers an immediate checkpoint in cached mode when the sum of
	// WAL segment sizes exceeds this many bytes (0 uses a default; <0 disables the
	// size trigger). This is an operational safety cap; it does not make each
	// individual write durable (use *Sync APIs for that).
	MaxWALBytes int64
}

// Snapshot is a consistent point-in-time database view.
//
// Snapshot pointers are single-use: after Close returns, callers must discard
// the pointer and any later method call remains invalid even after subsequent
// snapshots are acquired.
type Snapshot struct {
	db                     *DB
	idx                    *indexGen
	state                  *DBState
	vlogManager            *valuelog.Manager
	vlogPinned             bool
	systemRootPublishEpoch uint64
	leafGenerationIDs      []uint64
	// leafGenerationPinnedIDs mirrors the generation IDs retained by this
	// snapshot for stats/debugging. Release follows leafGenerationPinSet or
	// leafGenerationRefs when those optimized paths are present.
	leafGenerationPinnedIDs []uint64
	leafGenerationRefs      []*leafGenerationPinRef
	leafGenerationPinSet    *leafGenerationPinSet
	registryID              int64
	reader                  valueReader
	tree                    tree.Tree
	rootTreesMu             sync.Mutex
	rootTrees               []snapshotRootTree
	closed                  atomic.Bool
	generation              atomic.Uint64
	finalized               atomic.Bool
	readState               atomic.Uint64
	iteratorMu              sync.Mutex
	iterators               map[*snapshotBoundIterator]struct{}
	treePager               *pager.Pager
	treeRoot                uint64
	// registryShardHint is used to route reader registrations to a stable fast
	// registry shard for this snapshot object across operations.
	registryShardHint             int
	stableIndexCapture            bool
	stableIndexCaptureTransferred bool
	stableIndexCaptureCounter     *atomic.Int64
}

type snapshotRootTree struct {
	root uint64
	tree tree.Tree
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
	if err := s.beginRead(); err != nil {
		return nil
	}
	defer s.endRead()
	if s.idx == nil {
		return nil
	}
	return s.idx.pager
}

func (s *Snapshot) State() *DBState {
	if err := s.beginRead(); err != nil {
		return nil
	}
	defer s.endRead()
	return cloneDBState(s.state)
}

// StateToken returns the immutable scalar state pinned by the snapshot.
func (s *Snapshot) StateToken() (StateToken, bool) {
	if err := s.beginRead(); err != nil {
		return StateToken{}, false
	}
	defer s.endRead()
	return stateTokenFromState(s.state)
}

// AcquireSnapshot returns a new snapshot.
func (db *DB) AcquireSnapshot() *Snapshot {
	if db == nil {
		return nil
	}
	db.valueLogPublicationMu.RLock()
	defer db.valueLogPublicationMu.RUnlock()
	return db.acquireSnapshotWithValueLogPublicationLockHeld()
}

// acquireSnapshotWithValueLogPublicationLockHeld captures a normal snapshot
// when the caller already owns either side of valueLogPublicationMu. It exists
// for publication paths that hold the exclusive side while sealing a candidate;
// recursively taking RLock on sync.RWMutex would deadlock. All other snapshot
// lifetime, root-reuse, value-log, and generation pins remain identical to
// AcquireSnapshot.
func (db *DB) acquireSnapshotWithValueLogPublicationLockHeld() *Snapshot {
	if db == nil {
		return nil
	}
	db.rootReuseMu.RLock()
	defer db.rootReuseMu.RUnlock()
	if db.closing.Load() || db.publicationPoisoned.Load() {
		return nil
	}
	snap := db.snapPool.Get()
	if snap.registryShardHint == snapshotShardHintUnset {
		snap.registryShardHint = registryHintFromSnapshot(snap)
	}
	acqShard := snapshotAcquireShard()
	db.snapshotAcquireRO[acqShard].Add(1)
	db.snapshotAcquireEpoch.Add(1)
	defer func() {
		// Publish the completion epoch before dropping the in-flight count so
		// MinPinnedSnapshotCommitSeq cannot miss a just-registered snapshot.
		db.snapshotAcquireEpoch.Add(1)
		db.snapshotAcquireRO[acqShard].Add(-1)
	}()
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
	snap.systemRootPublishEpoch = view.systemRootPublishEpoch
	snap.reader.reconfigure(vlogSet, db.leafPageReadCache)
	for i := range snap.rootTrees {
		snap.rootTrees[i].root = 0
		snap.rootTrees[i].tree.Reset(nil, nil, 0)
	}
	if cap(snap.rootTrees) > snapshotRootTreeRetainMax {
		snap.rootTrees = nil
	} else {
		snap.rootTrees = snap.rootTrees[:0]
	}
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
	snap.iteratorMu.Lock()
	snap.closed.Store(false)
	snap.readState.Store(0)
	snap.iteratorMu.Unlock()
	return snap
}

// AcquireStableSnapshot pins the current index generation against online
// vacuum namespace replacement. Close releases the maintenance pin only after
// every snapshot-bound reader has drained.
func (db *DB) AcquireStableSnapshot() *Snapshot {
	if db == nil {
		return nil
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	db.stableIndexCaptures.Add(1)
	snapshot.stableIndexCapture = true
	snapshot.stableIndexCaptureCounter = &db.stableIndexCaptures
	return snapshot
}

// acquireStableSnapshotWithMaintenanceLockHeld captures the same stable-index
// lease as AcquireStableSnapshot when the caller already owns maintenanceMu.
// Maintenance operations must use this path to avoid recursively locking the
// non-reentrant maintenance gate during durable-root candidate preparation.
func (db *DB) acquireStableSnapshotWithMaintenanceLockHeld() *Snapshot {
	if db == nil {
		return nil
	}
	snapshot := db.AcquireSnapshot()
	if snapshot == nil {
		return nil
	}
	db.stableIndexCaptures.Add(1)
	snapshot.stableIndexCapture = true
	snapshot.stableIndexCaptureCounter = &db.stableIndexCaptures
	return snapshot
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
	s.iteratorMu.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.iteratorMu.Unlock()
		return nil
	}
	s.readState.Or(snapshotReadClosedBit)
	s.invalidateBoundIteratorsLocked()
	s.iteratorMu.Unlock()
	return s.finalizeCloseIfUnreferenced()
}

func (s *Snapshot) finalizeCloseIfUnreferenced() error {
	s.iteratorMu.Lock()
	if len(s.iterators) != 0 || s.readState.Load() != snapshotReadClosedBit || !s.finalized.CompareAndSwap(false, true) {
		s.iteratorMu.Unlock()
		return nil
	}
	s.iteratorMu.Unlock()
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
	if s.stableIndexCapture && s.stableIndexCaptureCounter != nil && !s.stableIndexCaptureTransferred {
		s.stableIndexCaptureCounter.Add(-1)
	}
	if s.stableIndexCapture {
		s.stableIndexCapture = false
	}
	s.stableIndexCaptureCounter = nil
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
	if err := ValidateDurabilityProfileGate(opts.Dir, opts.ResolvedProfile); err != nil {
		return nil, err
	}
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := CommandWALRequiredFeatureEnabled(opts.Dir)
		if err != nil {
			return nil, err
		}
		opts.CommandWAL = opts.CommandWAL || requiresCommandWAL
	} else {
		if cfg, ok, err := LoadFormatConfig(opts.Dir); err != nil {
			return nil, err
		} else if ok {
			opts.CommandWAL = opts.CommandWAL || cfg.RequiresCommandWALV1()
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
	NormalizeFlushAdmissionOptions(&opts)
	if opts.FlushApplyMinEntries <= 0 {
		opts.FlushApplyMinEntries = defaultFlushApplyMinEntries
	}
	if opts.FlushApplyMinSpans <= 0 {
		opts.FlushApplyMinSpans = defaultFlushApplyMinSpans
	}
	if opts.FlushApplyMinBytes <= 0 {
		opts.FlushApplyMinBytes = defaultFlushApplyMinBytes
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
	if testDBOpenHook != nil {
		if err := testDBOpenHook(db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return db, nil
}

func validateOptions(opts Options) error {
	if _, err := resolveLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries); err != nil {
		return err
	}
	switch opts.LeafPageReadCacheWriteAdmission {
	case LeafPageReadCacheWriteAdmissionImmediate, LeafPageReadCacheWriteAdmissionAdaptive:
	default:
		return fmt.Errorf("treedb: invalid leaf page read cache write admission policy %d", opts.LeafPageReadCacheWriteAdmission)
	}
	if !opts.FlushAdmissionPolicy.Valid() {
		return fmt.Errorf("treedb: invalid flush admission policy %d", opts.FlushAdmissionPolicy)
	}
	if err := validateResolvedDurabilityProfile(opts); err != nil {
		return err
	}
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
	if opts.CommandWAL && opts.Durability == DurabilityWALOffRelaxed {
		return fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
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
	case ValueLogBlockSnappy, ValueLogBlockLZ4, ValueLogBlockZSTD:
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
	if opts.ReadOnly {
		return nil, errors.New("BUG: treedb: openWithLock called with read-only options")
	}
	if err := recoverIndexSwap(opts.Dir); err != nil {
		return nil, err
	}
	if err := cleanupLeafGenerationPackStagingDirs(resolveStorageLayout(opts.Dir).leafVLogDir); err != nil {
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
	valueLogIdentityPins := rootpublication.NewIdentityPinRegistry()
	vm, err := valuelog.NewManagerForBoundedRecoveryWithStableResourcePinRegistry(layout.valueVLogDir, valueLogIdentityPins)
	if err != nil {
		p.Close()
		return nil, err
	}
	if err := vm.AddScanDirForBoundedRecovery(layout.leafVLogDir); err != nil {
		_ = vm.Close()
		p.Close()
		return nil, err
	}
	vm.SetDisableReadChecksum(opts.ValueLog.ReadIntegrity == IntegritySkipChecksums)
	vm.SetCurrentWritableMmapEnabled(opts.ValueLog.CurrentWritableMmap)
	vm.SetMultiCurrentWritableLane(valuelog.ReservedLeafLogLaneID, opts.IndexOuterLeavesInValueLog)
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)

	alloc := freelist.New(p, 0)
	alloc.SetPreferAppend(opts.PreferAppendAlloc)
	alloc.SetFreelistRegion(opts.FreelistRegionPages, opts.FreelistRegionRadius)

	z := zipper.New(p, alloc)

	gen := newIndexGen(1, p, alloc, z)

	adaptiveCtrl, inlineThreshold := resolveInlineThresholdAndAdaptive(opts)
	flushApplyConcurrency := normalizeFlushApplyConcurrency(opts.FlushApplyConcurrency)
	var flushApplyWorkerPool *zipper.ApplyWorkerPool
	if flushApplyConcurrency > 1 {
		flushApplyWorkerPool = zipper.NewApplyWorkerPool(flushApplyConcurrency)
	}

	db := &DB{
		valueLogManager:                vm,
		valueLogIdentityPins:           valueLogIdentityPins,
		valueLogRefTracker:             newValueLogRefTrackerForOptions(opts),
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
		flushAdmission:                 FlushAdmissionDecisionForOptions(opts),
		flushApplyConcurrency:          flushApplyConcurrency,
		flushApplyMinEntries:           opts.FlushApplyMinEntries,
		flushApplyMinSpans:             opts.FlushApplyMinSpans,
		flushApplyMinBytes:             opts.FlushApplyMinBytes,
		flushApplySpanNative:           opts.FlushApplySpanNative,
		flushApplyWorkerPool:           flushApplyWorkerPool,
		dir:                            opts.Dir,
		commandWALDir:                  layout.walDir,
		columnAssetRootDir:             layout.columnAssetDir,
		chunkSize:                      opts.ChunkSize,
		preferAppendAlloc:              opts.PreferAppendAlloc,
		freelistRegionPages:            opts.FreelistRegionPages,
		freelistRegionRadius:           opts.FreelistRegionRadius,
		durability:                     opts.Durability,
		resolvedProfile:                opts.ResolvedProfile,
		deprecatedProfileAlias:         opts.DeprecatedProfileAlias,
		rootPublicationFixedDelay:      opts.rootPublicationFixedDelay,
		commandWAL:                     opts.CommandWAL,
		commandWALStatsScan:            opts.CommandWALStatsScan,
		walMaxSegmentBytes:             opts.WALMaxSegmentBytes,
		commandWALSegmentTargetBytes:   opts.CommandWALSegmentTargetBytes,
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
	vm.SetDeferredDeletionSync(func(dir string, resource durabilitycut.Resource) error {
		return db.syncDeletionNamespaceDirectoryOrPoison(
			dir,
			resource,
			"treedb: sync deferred value-log deletion namespace",
		)
	})
	db.initializeLeafGenerationManifestStore(layout.leafVLogDir, valueLogIdentityPins)
	db.ghostManager.start()
	db.idx.Store(gen)
	if opts.testCommandWALRecoveryFailAfterLSN != 0 {
		db.testCommandWALRecoveryFailAfterLSN.Store(opts.testCommandWALRecoveryFailAfterLSN)
	}
	if opts.testCommandWALRecoveryFailBeforeDependencySync {
		db.testCommandWALRecoveryFailBeforeDependencySync.Store(true)
	}

	gen.zipper.SetFillTargets(opts.LeafFillTargetPPM, opts.InternalFillTargetPPM)
	gen.zipper.SetPiggybackCompaction(!opts.DisablePiggybackCompaction)
	gen.zipper.SetLeafPrefixCompression(opts.LeafPrefixCompression)
	gen.zipper.SetIndexColumnarLeaves(opts.IndexColumnarLeaves)
	gen.zipper.SetIndexPackedValuePtr(opts.IndexPackedValuePtr)
	gen.zipper.SetIndexInternalBaseDelta(opts.IndexInternalBaseDelta)
	gen.zipper.SetOuterLeavesInValueLog(opts.IndexOuterLeavesInValueLog)
	db.leafPageReadCache = newLeafPageReadCacheWithWriteAdmission(configuredLeafPageReadCacheEntries(opts.LeafPageReadCacheEntries), opts.LeafPageReadCacheWriteAdmission)
	gen.zipper.SetLeafPageReader(db.leafPageReader(vm))
	gen.zipper.SetAdaptiveLeafEncoding(opts.IndexAdaptiveLeafEncoding)
	gen.zipper.SetMaintenanceOpsPerCoalesce(opts.MaintenanceOpsPerCoalesce)

	if err := db.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.seedEntryRevisionFloor()

	segments, err := listRecoverySegments(opts.Dir)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	needsCommandWALFormat, err := commandWALFormatNeedsActivation(opts)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if needsCommandWALFormat {
		if err := ValidateCommandWALActivationClean(opts.Dir); err != nil {
			_ = db.Close()
			return nil, err
		}
		// Persist the required feature gate before running typed recovery so that
		// if recovery mutates WAL segments (cleanup pass) and then the open fails,
		// the next open uses typed recovery rather than the legacy path.
		cfg, err := formatConfigFromOptionsPreservingRequiredFeatures(opts)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		// Use the raw writer here because ValidateCommandWALActivationClean was
		// already called above. Re-checking after recovery would make first
		// activation depend on the transient post-recovery WAL directory shape
		// instead of the explicit activation boundary.
		if err := writeFormatConfig(opts.Dir, cfg); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if opts.CommandWAL {
		if err := replayCommandWALIntoBackend(db, segments, opts.WALMaxSegmentBytes, opts.ValueLog.DictLookup); err != nil {
			_ = db.Close()
			return nil, err
		}
		// Re-list segments after replay because V2 recovery may repair or remove
		// an incomplete suffix. Open the default writer on lane 0 before ordinary
		// cleanup so every destructive proof includes the owned journal namespace
		// generation and active identity. When lane 0's
		// active segment has a terminal tail, reopen that segment so
		// OpenCommandJournal can truncate the tail. Otherwise, append to a fresh
		// lane-0 segment to avoid extending a covered segment.
		commandSegmentSeq := uint64(0)
		journalSegments, err := listRecoverySegments(opts.Dir)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		activeSeqByLane := commandWALActiveSeqByLane(journalSegments)
		commandSegmentSeq = activeSeqByLane[0]
		if commandSegmentSeq == ^uint64(0) {
			_ = db.Close()
			return nil, fmt.Errorf("%w: dir=%s lane=0 active_seq=%d", ErrCommandWALSegmentSeqExhausted, WALDirPath(opts.Dir), commandSegmentSeq)
		}
		hasTerminalTail, err := commandWALLaneActiveHasTerminalTail(journalSegments, 0, opts.WALMaxSegmentBytes)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if commandSegmentSeq != 0 && !hasTerminalTail {
			commandSegmentSeq++
		}
		journal, err := commitlog.OpenCommandJournal(WALDirPath(opts.Dir), commitlog.CommandJournalOptions{
			MaxSegmentSize:                  opts.WALMaxSegmentBytes,
			SegmentTargetBytes:              opts.CommandWALSegmentTargetBytes,
			BufferSize:                      commandWALWriterBufferSize,
			DeferredCommandBufferSize:       commandWALDeferredPointBufferSize,
			DeferredCommandBufferRetainSize: commandWALDeferredPointBufferRetainSize,
			Compress:                        opts.JournalCompression,
			OnSegmentRotated:                db.observeCommandWALSegmentRotated,
			InitialLSN:                      db.meta.AppliedCommandLSN,
			SegmentSeq:                      commandSegmentSeq,
			CaptureStableResources:          true,
		})
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.commandJournal = journal
		db.commandWALSessionAppliedLSN = db.meta.AppliedCommandLSN
		// replayCommandWALIntoBackend records the physically classified durable
		// frontier. A recovered durable root may advance AppliedCommandLSN beyond
		// that WAL frontier, but opening a writer does not sync the recovered WAL
		// lineage and therefore must not mint cleanup authority from root state.
		db.refreshCommandWALClosedBytes()
		if !db.readOnly {
			if err := db.CleanupCommandWALCoveredSegments(true); err != nil &&
				!errors.Is(err, errDurableWALCleanupProofUnavailable) {
				_ = db.Close()
				return nil, err
			}
		}
		db.cacheCommandWALRequiredFeatureStats()
	} else {
		// If a directory requires command replay but this open is not command-WAL
		// enabled, fail closed before legacy replay can misinterpret typed frames.
		// Frames already covered by AppliedCommandLSN are filtered below.
		if err := requireNoUnappliedCommandWALFrames(opts.Dir, db.meta.AppliedCommandLSN, opts.WALMaxSegmentBytes); err != nil {
			_ = db.Close()
			return nil, err
		}
		legacySegments, hasLegacyRedoJournal, err := legacyCachedRedoJournalReplaySegments(db, segments, opts.WALMaxSegmentBytes)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if hasLegacyRedoJournal && !opts.AllowLegacyCachedRedoJournalReplay {
			_ = db.Close()
			return nil, legacyCachedRedoJournalReplayDisabledError(legacySegments, opts.WALMaxSegmentBytes)
		}
		// The explicit compatibility escape hatch also classifies incomplete-only
		// legacy segments. They contain no replayable batch, but the authorized
		// forensic recovery pass must still remove the torn suffix so it is not
		// rediscovered on every reopen.
		if opts.Durability != DurabilityWALOffRelaxed || hasLegacyRedoJournal || opts.AllowLegacyCachedRedoJournalReplay {
			if err := replayWALIntoBackend(db, legacySegments, opts.WALMaxSegmentBytes, opts.ValueLog.DictLookup); err != nil {
				_ = db.Close()
				return nil, err
			}
		}
	}

	// Recovery registers the exact resource paths named by each viable durable
	// manifest, and replay producers register newly created segments directly.
	// Reapply decode hooks without replacing that bounded inventory with a
	// directory-wide discovery pass.
	vm.SetDictLookup(opts.ValueLog.DictLookup)
	vm.SetTemplateLookup(opts.ValueLog.TemplateLookup, opts.ValueLog.TemplateDecodeOptions)
	recoverySet := vm.CurrentSetNoRefresh()
	reader := newValueReader(recoverySet)
	releaseRecoverySet := func() error {
		if recoverySet == nil {
			return nil
		}
		err := vm.Release(recoverySet)
		recoverySet = nil
		return err
	}
	_, err = recoverLeafGenerationResetAfterOfflineVacuum(opts.Dir, p, reader, db.meta.UserRootPageID, db.meta.SystemRootPageID, db.meta.CommitSeq, releaseRecoverySet, vm.EvictSegment)
	if releaseErr := releaseRecoverySet(); err == nil && releaseErr != nil {
		err = releaseErr
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	if opts.IndexOuterLeavesInValueLog {
		manifest, selectedExact, err := db.loadSelectedDurableLeafGenerationManifest()
		if err == nil && !selectedExact {
			manifest, err = loadOrCreateLeafGenerationManifestWithStore(layout.leafVLogDir, db.meta.CommitSeq, false, db.leafGenerationManifestStore)
		}
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
		AppliedCommandLSN:          db.meta.AppliedCommandLSN,
		MaxEntryRevision:           page.EntryRevision(db.meta.MaxEntryRevision),
		ValueLogSet:                vm.CurrentSetNoRefresh(),
		LeafGenerations:            db.currentLeafGenerationView(),
		LeafGenerationStateVersion: db.leafGenerationStateVersion,
	}
	oldInitialState := db.state.Swap(initialState)
	db.publishSnapshotView(gen, initialState, vm)
	if oldInitialState != nil && oldInitialState.ValueLogSet != nil {
		_ = vm.Release(oldInitialState.ValueLogSet)
	}
	if err := db.initValueLogRefTracker(); err != nil {
		db.Close()
		return nil, err
	}
	if opts.CommandWAL {
		if err := db.installCommandWALValueLogAppender(); err != nil {
			db.Close()
			return nil, err
		}
	}
	// Recovery, replay, the visible snapshot, reachability tracking, and any
	// command-WAL appender must all name the same root before asynchronous root
	// publication can admit its first builder.
	if err := db.initializeRootPublicationRuntimeV1(gen); err != nil {
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
	if err := saveOpenFormatConfig(opts); err != nil {
		if opts.ResolvedProfile != "" {
			_ = db.Close()
			return nil, fmt.Errorf("treedb: persist durability profile manifest: %w", err)
		}
		if opts.NotifyError != nil {
			opts.NotifyError(err)
		}
	}

	return db, nil
}

func (db *DB) acceptingCloseHooksLocked() bool {
	return db != nil && !db.closeHooksClosed && !db.closing.Load()
}

// RegisterCloseHook registers a callback that runs before Close marks the DB as
// closing, while normal write/publish APIs are still available.
func (db *DB) RegisterCloseHook(hook func() error) func() {
	unregister, _ := db.RegisterCloseHookIfOpen(hook)
	return unregister
}

// RegisterCloseHookBefore registers a callback that runs before ordinary close
// hooks. It is intended for high-level owners that must flush buffered state
// while lower-level resources registered during DB open are still available.
func (db *DB) RegisterCloseHookBefore(hook func() error) func() {
	if db == nil || hook == nil {
		return func() {}
	}
	db.closeHooksMu.Lock()
	defer db.closeHooksMu.Unlock()
	if !db.acceptingCloseHooksLocked() {
		return func() {}
	}
	idx := len(db.closeHooksBefore)
	db.closeHooksBefore = append(db.closeHooksBefore, hook)

	var once sync.Once
	return func() {
		once.Do(func() {
			db.closeHooksMu.Lock()
			if idx >= 0 && idx < len(db.closeHooksBefore) && db.closeHooksBefore[idx] != nil {
				db.closeHooksBefore[idx] = nil
			}
			db.closeHooksMu.Unlock()
		})
	}
}

// RegisterCloseHookIfOpen is like RegisterCloseHook, but also reports whether
// the hook was retained. Callers that attach external state to the hook can use
// this to avoid leaks when registration races DB close.
func (db *DB) RegisterCloseHookIfOpen(hook func() error) (func(), bool) {
	return db.RegisterCloseHookIfOpenAfter(nil, hook)
}

// RegisterCloseHookIfOpenAfter runs setup while close-hook registration is
// serialized with RunCloseHooks, then registers hook if setup returns true. The
// returned bool reports that the DB was still accepting close-hook registration
// and setup, if any, ran inside that accepted registration window. If setup
// returns false, no hook is retained even though the registration window was
// accepted.
func (db *DB) RegisterCloseHookIfOpenAfter(setup func() bool, hook func() error) (func(), bool) {
	if db == nil || hook == nil {
		return func() {}, false
	}
	db.closeHooksMu.Lock()
	defer db.closeHooksMu.Unlock()
	if !db.acceptingCloseHooksLocked() {
		return func() {}, false
	}
	if setup != nil && !setup() {
		return func() {}, true
	}
	idx := len(db.closeHooks)
	db.closeHooks = append(db.closeHooks, hook)

	var once sync.Once
	return func() {
		once.Do(func() {
			db.closeHooksMu.Lock()
			if idx >= 0 && idx < len(db.closeHooks) && db.closeHooks[idx] != nil {
				db.closeHooks[idx] = nil
			}
			db.closeHooksMu.Unlock()
		})
	}, true
}

// RunCloseHooks runs and clears registered user close hooks. Callbacks run
// outside DB maintenance and before Close marks the DB as closing, so they may
// call normal DB APIs, including RunCloseHooks and maintenance operations.
func (db *DB) RunCloseHooks() error {
	if db == nil {
		return nil
	}
	hookErr := db.runCloseHooks()
	db.closeHooksMu.Lock()
	closeHooksRunning := db.closeHooksRunning
	closeHooksOwner := db.closeHooksOwner
	closeHooksWaitHook := db.closeHooksWaitHook
	db.closeHooksMu.Unlock()
	if closeHooksRunning {
		if caller := currentGoroutineID(); caller != 0 && caller == closeHooksOwner {
			return hookErr
		}
		if closeHooksWaitHook != nil {
			closeHooksWaitHook()
		}
		db.closeHooksWG.Wait()
	}

	db.closeHooksMu.Lock()
	closeRequested := db.closeHooksCloseRequested
	db.closeHooksCloseRequested = false
	db.closeHooksMu.Unlock()
	if closeRequested {
		return errors.Join(hookErr, db.closeAfterHooksOnce())
	}
	return hookErr
}

func (db *DB) runCloseHooks() (retErr error) {
	db.closeHooksMu.Lock()
	if db.closeHooksClosed {
		db.closeHooksMu.Unlock()
		return nil
	}
	db.closeHooksClosed = true
	hooksBefore := append([]func() error(nil), db.closeHooksBefore...)
	clear(db.closeHooksBefore)
	db.closeHooksBefore = nil
	hooks := append([]func() error(nil), db.closeHooks...)
	clear(db.closeHooks)
	db.closeHooks = nil
	db.closeHooksRunning = true
	db.closeHooksOwner = currentGoroutineID()
	db.closeHooksWG.Add(1)
	db.closeHooksMu.Unlock()
	defer func() {
		db.closeHooksMu.Lock()
		db.closeHooksOwner = 0
		db.closeHooksRunning = false
		db.closeHooksMu.Unlock()
		db.closeHooksWG.Done()
	}()

	var errs []error
	for _, hook := range hooksBefore {
		if hook == nil {
			continue
		}
		if err := hook(); err != nil {
			errs = append(errs, err)
		}
	}
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

func (db *DB) registerInternalTeardownHook(hook func() error) func() {
	if db == nil || hook == nil {
		return func() {}
	}
	db.internalTeardownHooksMu.Lock()
	defer db.internalTeardownHooksMu.Unlock()
	if db.internalTeardownHooksClosed || db.closing.Load() {
		return func() {}
	}
	idx := len(db.internalTeardownHooks)
	db.internalTeardownHooks = append(db.internalTeardownHooks, hook)

	var once sync.Once
	return func() {
		once.Do(func() {
			db.internalTeardownHooksMu.Lock()
			if idx >= 0 && idx < len(db.internalTeardownHooks) && db.internalTeardownHooks[idx] != nil {
				db.internalTeardownHooks[idx] = nil
			}
			db.internalTeardownHooksMu.Unlock()
		})
	}
}

// runInternalTeardownHooksMaintenanceLocked runs DB-owned resource teardown
// only after active maintenance has drained and closing rejects new work.
func (db *DB) runInternalTeardownHooksMaintenanceLocked() error {
	db.internalTeardownHooksMu.Lock()
	if db.internalTeardownHooksClosed {
		db.internalTeardownHooksMu.Unlock()
		return nil
	}
	db.internalTeardownHooksClosed = true
	hooks := append([]func() error(nil), db.internalTeardownHooks...)
	clear(db.internalTeardownHooks)
	db.internalTeardownHooks = nil
	db.internalTeardownHooksMu.Unlock()

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

// registerCaptureTeardownHook retains recovery work discovered by a producer
// that already owns a teardownMu read lease. Unlike ordinary internal teardown
// hooks, these callbacks must not run until every admitted producer has
// released its lease.
func (db *DB) registerCaptureTeardownHook(hook func() error) func() {
	cancel, _ := db.tryRegisterCaptureTeardownHook(hook)
	return cancel
}

// tryRegisterCaptureTeardownHook reports whether the hook was accepted. A
// producer holding teardownMu for reading must always observe accepted=true;
// the explicit result lets lease-backed callers fail closed if that invariant
// is ever violated.
func (db *DB) tryRegisterCaptureTeardownHook(hook func() error) (func(), bool) {
	if db == nil || hook == nil {
		return func() {}, false
	}
	db.captureTeardownHooksMu.Lock()
	defer db.captureTeardownHooksMu.Unlock()
	if db.captureTeardownHooksClosed {
		return func() {}, false
	}
	idx := len(db.captureTeardownHooks)
	db.captureTeardownHooks = append(db.captureTeardownHooks, hook)

	var once sync.Once
	return func() {
		once.Do(func() {
			db.captureTeardownHooksMu.Lock()
			if idx >= 0 && idx < len(db.captureTeardownHooks) && db.captureTeardownHooks[idx] != nil {
				db.captureTeardownHooks[idx] = nil
			}
			db.captureTeardownHooksMu.Unlock()
		})
	}, true
}

// runCaptureTeardownHooksLocked runs after Close owns teardownMu exclusively.
// Producers holding capture leases have therefore finished registering and
// can no longer race their retained recovery authority.
func (db *DB) runCaptureTeardownHooksLocked() error {
	db.captureTeardownHooksMu.Lock()
	if db.captureTeardownHooksClosed {
		db.captureTeardownHooksMu.Unlock()
		return nil
	}
	db.captureTeardownHooksClosed = true
	hooks := append([]func() error(nil), db.captureTeardownHooks...)
	clear(db.captureTeardownHooks)
	db.captureTeardownHooks = nil
	db.captureTeardownHooksMu.Unlock()

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
	if db == nil {
		return nil
	}
	db.closeHooksMu.Lock()
	closeHooksRunning := db.closeHooksRunning
	closeHooksOwner := db.closeHooksOwner
	closeHooksWaitHook := db.closeHooksWaitHook
	db.closeHooksMu.Unlock()
	if closeHooksRunning {
		if caller := currentGoroutineID(); caller != 0 && caller == closeHooksOwner {
			// A user hook cannot synchronously wait for the hook drain that is
			// invoking it. Record teardown for the active outer Close or standalone
			// RunCloseHooks owner to complete after callbacks return.
			db.closeHooksMu.Lock()
			db.closeHooksCloseRequested = true
			db.closeHooksMu.Unlock()
			return nil
		}
		if closeHooksWaitHook != nil {
			closeHooksWaitHook()
		}
		db.closeHooksWG.Wait()
	}

	var errs []error
	if err := db.runCloseHooks(); err != nil {
		errs = append(errs, err)
	}
	// Another caller may have started the once-only hook drain. Do not begin
	// production teardown until those callbacks have returned.
	db.closeHooksWG.Wait()
	if err := db.closeAfterHooksOnce(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (db *DB) closeAfterHooksOnce() error {
	db.closeTeardownOnce.Do(func() {
		if testDBCloseHook != nil {
			testDBCloseHook(db)
		}
		db.closeTeardownErr = db.closeAfterHooks()
	})
	return db.closeTeardownErr
}

func (db *DB) closeAfterHooks() error {
	var errs []error

	// User callbacks have flushed their higher-level buffers. First pass through
	// maintenanceMu so an already-admitted maintenance operation can finish
	// before closing becomes visible. Release it immediately after closing the
	// admission gate: stable root publication must never run while it is held.
	db.maintenanceMu.Lock()
	db.closing.Store(true)
	db.maintenanceMu.Unlock()
	// Wait for every already-admitted writer to finish constructing and handing
	// off its immutable root candidate.
	db.writeMu.Lock()
	if db.flushApplyWorkerPool != nil {
		db.flushApplyWorkerPool.Close()
		db.flushApplyWorkerPool = nil
	}
	db.clearFlushApplyReadOnlyPrepareBuffers()
	db.writeMu.Unlock()

	// A root producer releases writeMu after transferring its immutable build to
	// the durable publisher, but it keeps teardownMu for reading until enqueue,
	// admission, and ordered post-work have completed. Cross the teardown gate
	// before stopping the coordinator so Close cannot strand such an admitted
	// producer between build handoff and enqueue. Release the gate before taking
	// maintenanceMu below: a closing dry-run maintenance caller may already hold
	// maintenanceMu while waiting to observe the teardown barrier.
	db.teardownMu.Lock()
	var (
		rootRuntime *rootPublicationRuntimeV1
		rootHandoff *rootpublication.RecoveryResourceHandoff
	)
	if rootRuntime = db.rootPublication; rootRuntime != nil && rootRuntime.coordinator != nil {
		if err := rootRuntime.coordinator.Drain(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("drain root publication: %w", publicRootPublicationErrorV1(err)))
		}
		if err := rootRuntime.coordinator.Stop(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("stop root publication: %w", publicRootPublicationErrorV1(err)))
		}
		var err error
		rootHandoff, err = rootRuntime.coordinator.TakeRecoveryHandoff()
		if err != nil {
			errs = append(errs, fmt.Errorf("take root publication recovery handoff: %w", publicRootPublicationErrorV1(err)))
		}
		db.rootPublication = nil
	}
	db.teardownMu.Unlock()
	// Writers and durable-root publication are now drained, while the command
	// journal and its exact active namespace identity are still owned by this
	// handle. Unavailable proof is the expected conservative result when relaxed
	// roots run ahead of the dependency-closed WAL frontier; retain those files
	// and let a later checkpoint/reopen converge. A pre-existing post-append or
	// publication poison is likewise not cleanup authority: retain the complete
	// WAL unchanged for the next owner. Healthy handles surface actual cleanup
	// failures, but teardown continues so cleanup errors never strand resources.
	if db.commandWAL && !db.readOnly && db.commandJournal != nil && db.commandWALPoisonedError() == nil {
		if err := db.cleanupCommandWALCoveredSegmentsV1(); err != nil &&
			!errors.Is(err, errDurableWALCleanupProofUnavailable) &&
			!errors.Is(err, errDurableWALCleanupProofStale) &&
			!errors.Is(err, commitlog.ErrCommandWALCleanupSnapshotStale) {
			errs = append(errs, fmt.Errorf("cleanup command WAL during close: %w", err))
		}
	}

	// No stable root I/O can begin beyond this point. Maintenance and teardown
	// may now close producer resources and the index without racing Publisher.
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if err := db.runInternalTeardownHooksMaintenanceLocked(); err != nil {
		errs = append(errs, err)
	}
	leafPageLog := db.leafPageLog
	leafGenerationManifestStore := db.leafGenerationManifestStore
	if leafGenerationManifestStore != nil {
		leafGenerationManifestStore.Close()
	}
	// Operations using teardownMu may briefly take writeMu while preparing or
	// revalidating work. Never wait for them while holding writeMu.
	db.teardownMu.Lock()
	defer db.teardownMu.Unlock()
	if err := db.runCaptureTeardownHooksLocked(); err != nil {
		errs = append(errs, err)
	}
	db.stopCommitCombiner()
	db.pruner.Stop()
	if db.valueLogRefTracker != nil {
		if err := db.persistValueLogRefTracker(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.ghostManager != nil {
		db.ghostManager.stop()
	}

	db.mu.Lock()
	db.clearSnapshotView()
	vm := db.valueLogManager
	db.valueLogManager = nil
	commandJournal := db.commandJournal
	db.commandJournal = nil
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
	db.releaseDurableRootResourcesV1()
	if err := db.closeAllIndexes(); err != nil {
		errs = append(errs, err)
	}
	if group, ok := leafPageLog.(*leafPageLogLaneGroup); ok {
		if err := group.CloseSelectedLanes(); err != nil {
			errs = append(errs, err)
		}
	}
	db.commandWALDebt.releaseAll()
	if vm != nil {
		if err := vm.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if commandJournal != nil {
		if err := commandJournal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Pending candidate handles and allocator transactions stay pinned through
	// resource and index shutdown. Only now may the live-runtime clones and the
	// coordinator's exact recovery handoff be released.
	if rootRuntime != nil {
		rootRuntime.release()
	}
	if rootHandoff != nil {
		rootHandoff.Release()
	}
	// Namespace sync proofs are valid only for this DB lifetime. Stable
	// publication closures retain exact handles independently and remain usable
	// after shutdown, while a later DB instance establishes fresh evidence.
	db.valueLogIdentityPins.ClearStableNamespaceLinks()
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

func currentGoroutineID() uint64 {
	// The runtime stack header begins with "goroutine <id> ". Recording that ID
	// once around callback execution gives Close an explicit, depth-independent
	// reentrancy marker without treating unrelated concurrent callers as hooks.
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	if n <= len(prefix) || string(buf[:len(prefix)]) != prefix {
		return 0
	}
	end := len(prefix)
	for end < n && buf[end] >= '0' && buf[end] <= '9' {
		end++
	}
	if end == len(prefix) {
		return 0
	}
	id, err := strconv.ParseUint(string(buf[len(prefix):end]), 10, 64)
	if err != nil {
		return 0
	}
	return id
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
		return db.initializeDurableRootV1(idx)
	}

	selected, err := selectDurableRootV1(p, p.PageCount(), db.validateDurableDependencyManifestV1)
	if err != nil {
		return err
	}
	selectionInstalled := false
	defer func() {
		if selectionInstalled {
			return
		}
		for i := range selected.SlotResources {
			selected.SlotResources[i].Release()
			selected.SlotResources[i] = nil
		}
	}()
	p.SetPageCount(selected.Record.TotalPages)
	if err := idx.allocator.EnableCOWV1(selected.Freelist, freelist.NewReservationLedger()); err != nil {
		return fmt.Errorf("enable recovered COW freelist: %w", err)
	}
	db.installDurableRootSelectionV1(selected)
	selectionInstalled = true
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
	body := data[page.PageHeaderSize:]
	// Meta page selection is still commit-sequence based. The command-WAL V1
	// decoder only changes how the selected page interprets the reserved
	// AppliedCommandLSN extension bytes: unmarked legacy pages decode as zero.
	return page.DecodeMetaBodyCommandWALV1(body), true
}

type finalizeCommitPost struct {
	accepted                          bool
	oldState                          *DBState
	metrics                           adaptive.Metrics
	vlogRefDelta                      *valueLogRefDelta
	vlogRefTrackerAdvanced            bool
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
	persistLeafGenerationIndexesOnly  bool
	persistLeafGenerationManifestView *leafGenerationManifest
	persistLeafGenerationStateView    *leafGenerationView
	persistLeafGenerationRawFileIDs   []uint32
	clearLeafGenerationPendingFileIDs []uint32
	drainLeafGenerationPending        bool
}

// finalizeCommitLockedWithOptions performs the durability-critical publish
// path. Every caller must bind its constructed roots to the exact visible
// commit sequence from which they were derived.
func (db *DB) finalizeCommitLockedWithOptions(newRootID uint64, sysRootID uint64, retired []uint64, sync bool, metrics adaptive.Metrics, touchedValueLogSegments []uint32, forceValueLogRefresh bool, vlogRefDelta *valueLogRefDelta, leafManifest *leafGenerationManifest, leafManifestRawFileIDs []uint32, opts finalizeCommitOptions) (finalizeCommitPost, error) {
	post := finalizeCommitPost{
		metrics: metrics,
	}
	// Producer closures are move-only inputs. Merge transfers their handles to
	// the durable candidate; every earlier exit releases them here.
	defer opts.durableResources.Release()
	prePublishErr := func(err error) error {
		return wrapFinalizeCommitError(err, true)
	}
	if db.readOnly {
		return post, ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return post, err
	}
	if !opts.hasExpectedBaseCommitSeq {
		return post, errors.New("missing expected base commit sequence")
	}
	rootRuntime := db.rootPublication
	builder := opts.rootPublicationBuilder
	if rootRuntime != nil && builder == nil {
		var err error
		builder, err = rootRuntime.coordinator.AcquireBuilder(context.Background())
		if err != nil {
			return post, prePublishErr(fmt.Errorf("acquire root-publication builder: %w", publicRootPublicationErrorV1(err)))
		}
	}
	if builder != nil {
		defer builder.Release()
	}
	// Serialize before reading the visible meta projection. A preceding
	// publication may already have synced its durable meta while it is still
	// installing the matching in-memory state; preparing nextMeta before this
	// gate would then reuse the just-published commit sequence.
	durablePublishLocked := true
	releaseDurablePublishAction := opts.durablePublishRelease
	if opts.durablePublishLocked {
		if releaseDurablePublishAction == nil {
			// Preserve legacy test/caller safety while making the transferred-lock
			// contract explicit for every production prelocked path.
			releaseDurablePublishAction = func() { db.durablePublishMu.Unlock() }
		}
	} else {
		db.durablePublishMu.Lock()
		releaseDurablePublishAction = func() {
			db.durablePublishMu.Unlock()
		}
	}
	releaseDurablePublish := func() {
		if !durablePublishLocked {
			return
		}
		durablePublishLocked = false
		releaseDurablePublishAction()
	}
	defer releaseDurablePublish()
	// A publication that owned the durable gate while this caller waited may
	// have crossed an outcome-ambiguous cut and poisoned the open handle. The
	// admission check above cannot authorize work after that predecessor, and
	// dependency capture is allowed to use projections that take no snapshot.
	// Recheck while serialized before deriving or materializing another meta.
	if err := db.commandWALPoisonedError(); err != nil {
		return post, err
	}
	if opts.hasExpectedBaseCommitSeq {
		db.mu.RLock()
		currentCommitSeq := db.meta.CommitSeq
		db.mu.RUnlock()
		if currentCommitSeq != opts.expectedBaseCommitSeq {
			return post, wrapFinalizeCommitError(fmt.Errorf("%w: base=%d current=%d", errDurableRootCandidateStale, opts.expectedBaseCommitSeq, currentCommitSeq), true)
		}
	}
	idx := opts.durableIndex
	if idx == nil {
		idx = db.idx.Load()
	}
	if idx == nil {
		return post, errors.New("missing index")
	}
	valueLogAppender := db.currentValueLogAppender()

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
	var inlinePublishPrepareGuard *finalizeCommitPrepareGuard
	if !opts.skipPrePublishFlush {
		db.publishPrepareMu.RLock()
		inlinePublishPrepareGuard = &finalizeCommitPrepareGuard{db: db}
		defer inlinePublishPrepareGuard.Release()
		t0 := time.Now()
		if err := db.flushFinalizeCommitDurability(idx, valueLogAppender, sync); err != nil {
			return post, prePublishErr(err)
		}
		if debugTiming {
			durSync1 = time.Since(t0)
		}
	}
	var watermarkWait, watermarkHold time.Duration

	// 1. Data/leaf-log/value-log flush is complete here. Prepared publish callers
	// may have done it outside commit serialization; ordinary callers did it
	// above through flushFinalizeCommitDurability.

	// 2. Prepare Meta - Short Lock
	lockStart := time.Now()
	db.mu.Lock()
	watermarkWait += time.Since(lockStart)
	holdStart := time.Now()
	oldUserRootID := db.meta.UserRootPageID
	nextMeta := db.meta
	nextMeta.CommitSeq++
	nextMeta.UserRootPageID = newRootID
	nextMeta.SystemRootPageID = sysRootID
	nextMeta.FreelistHeadID = idx.allocator.Head()
	nextMeta.TotalPages = idx.pager.PageCount()
	if opts.commandWALPublish {
		if err := durabilitycut.EmitLSN(durabilitycut.BeforeAppliedLSNAdvance, durabilitycut.ResourceMeta, db.dir, opts.appliedCommandLSN); err != nil {
			db.mu.Unlock()
			watermarkHold += time.Since(holdStart)
			return post, prePublishErr(err)
		}
		if err := validateCommandWALPublishLocked(db.meta, newRootID, sysRootID, opts); err != nil {
			db.mu.Unlock()
			watermarkHold += time.Since(holdStart)
			return post, prePublishErr(err)
		}
		nextMeta.AppliedCommandLSN = opts.appliedCommandLSN
		if err := durabilitycut.EmitLSN(durabilitycut.AfterAppliedLSNAdvance, durabilitycut.ResourceMeta, db.dir, opts.appliedCommandLSN); err != nil {
			db.mu.Unlock()
			watermarkHold += time.Since(holdStart)
			return post, prePublishErr(err)
		}
	}
	if opts.maxEntryRevision != page.LegacyEntryRevision && uint64(opts.maxEntryRevision) > nextMeta.MaxEntryRevision {
		nextMeta.MaxEntryRevision = uint64(opts.maxEntryRevision)
	}

	db.mu.Unlock()
	watermarkHold += time.Since(holdStart)

	if db.testFailFinalizeCommit.Load() {
		return post, prePublishErr(errTestFinalizeCommitFailpoint)
	}
	// Every newly reachable external segment must be registered before the
	// immutable manifest captures its exact identity. Visible-state installation
	// consumes that bounded registered inventory without a directory scan.
	if db.valueLogManager != nil {
		if _, err := db.registerLeafPageLogSegmentsForPublish(); err != nil {
			return post, prePublishErr(err)
		}
		if valueLogAppender != nil {
			path, fileID, ok := valueLogAppender.CurrentValueLogSegment()
			if ok && path != "" && fileID != 0 {
				if _, err := db.ensureValueLogSegmentRegisteredAt(path, fileID); err != nil {
					return post, prePublishErr(err)
				}
			}
		}
	}
	if rootRuntime != nil {
		return db.finalizeQueuedRootPublicationV1(
			rootRuntime, builder, idx, nextMeta, oldUserRootID, newRootID,
			retired, sync, post, vlogRefDelta, leafManifest,
			leafManifestRawFileIDs, opts, inlinePublishPrepareGuard,
			releaseDurablePublish,
		)
	}

	// 3. Materialize the COW inventory, sync the exact index identity, write the
	// alternate durable-meta slot once, and sync that slot. The durable-root
	// publisher owns the only V1 meta mutation path.
	t0 := time.Now()
	var err error
	// Capture the candidate's external closure before taking rootReuseMu. The
	// full-scan fallback uses an ordinary snapshot, which must enter through the
	// reader side of that gate. Root serialization and durablePublishMu keep the
	// logical candidate fixed while the closure is captured; the exclusive gate
	// below then seals new old-generation readers before reuse is sampled.
	var durableResources *rootpublication.StableResourceSet
	if db.durableRoot.pending == nil {
		durableResources, err = db.captureDurableRootResourcesV1(idx, nextMeta, vlogRefDelta, opts.durableResources, opts.durableResourceRequirements, opts.durableResourceMutation, opts.valueLogPublicationLocked, opts.publishTiming)
		if err != nil {
			return post, wrapFinalizeCommitError(fmt.Errorf("capture durable-root dependencies: %w", err), true)
		}
	} else if opts.durableResources != nil {
		opts.durableResources.Release()
		return post, wrapFinalizeCommitError(errors.New("durable-root retry received replacement producer resources"), true)
	}
	// Seal new base-generation captures before the reuse capability is sampled.
	// The gate remains held until publishSnapshotView exposes the same root that
	// the durable meta and allocator generation make authoritative.
	db.rootReuseMu.Lock()
	rootReuseLocked := true
	defer func() {
		if rootReuseLocked {
			db.rootReuseMu.Unlock()
		}
	}()
	candidate, prepareErr := db.prepareOrResumeDurableRootCandidateV1(idx, nextMeta, retired, durableResources, opts.closeTeardownPinned)
	if prepareErr != nil {
		return post, wrapFinalizeCommitError(prepareErr, !errors.Is(prepareErr, ErrRecoveryRequired))
	}
	if releaseRootSerialization := opts.releaseRootSerialization; releaseRootSerialization != nil {
		// The immutable candidate now owns its allocator reservations and exact
		// resource handles. No root-serialization lock is needed by the remaining
		// flush/sync/meta transaction or the visible-state install below.
		releaseRootSerialization()
	}
	if hook := db.testDurableRootCandidatePreparedHook; hook != nil {
		hook()
	}
	nextMeta, err = db.executeDurableRootCandidateWithRetryV1(candidate)
	if err != nil {
		return post, err
	}
	poisonVisibleInstall := func(err error) (finalizeCommitPost, error) {
		db.publicationPoisoned.Store(true)
		return post, wrapFinalizeCommitError(errors.Join(err, ErrRecoveryRequired), false)
	}
	if db.testFailDurableRootVisibleInstall.Load() {
		return poisonVisibleInstall(errTestDurableRootVisibleInstallFailpoint)
	}
	if debugTiming {
		durMeta = time.Since(t0)
	}

	// 4. Update visible state. Page retirement is part of the COW generation
	// sealed by publishDurableRootV1, never an in-memory graveyard side effect.
	lockStart = time.Now()
	db.mu.Lock()
	watermarkWait += time.Since(lockStart)
	holdStart = time.Now()
	db.meta = nextMeta
	db.advanceEntryRevisionFloor(page.EntryRevision(nextMeta.MaxEntryRevision))
	db.metaPageID = db.durableRoot.slot
	post.oldState = db.state.Load()
	var valueLogSet *valuelog.Set
	if db.valueLogManager != nil {
		// The exact durable-root closure has already resolved and retained every
		// reachable external segment, and unresolved producer registrations fail
		// before the alternate meta is mutated. A broad directory scan here would
		// add unrelated files rather than strengthen the candidate closure.
		valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
	}
	var leafGenerationView *leafGenerationView
	if leafManifest != nil {
		db.leafGenerationManifest = leafManifest
		post.persistLeafGenerationManifest = !opts.leafManifestAlreadyPersistent
		post.persistLeafGenerationIndexesOnly = opts.leafManifestAlreadyPersistent
		post.persistLeafGenerationManifestView = leafManifest
		post.persistLeafGenerationRawFileIDs = append(post.persistLeafGenerationRawFileIDs[:0], leafManifestRawFileIDs...)
		leafGenerationView = db.leafGenerationViewForManifest(leafManifest)
	}
	if db.leafPageLog != nil {
		stagedLeafManifest, err := db.stagedLeafGenerationManifestWithPendingResult(db.leafGenerationManifest, 0, nextMeta.CommitSeq)
		if err != nil {
			db.mu.Unlock()
			return poisonVisibleInstall(err)
		}
		if stagedLeafManifest.changed {
			leafGenerationView = db.leafGenerationViewForManifest(stagedLeafManifest.manifest)
			post.persistLeafGenerationManifest = true
			post.persistLeafGenerationManifestView = stagedLeafManifest.manifest
			post.persistLeafGenerationStateView = leafGenerationView
			post.persistLeafGenerationRawFileIDs = append(post.persistLeafGenerationRawFileIDs, stagedLeafManifest.rawFileIDs...)
			post.clearLeafGenerationPendingFileIDs = append(post.clearLeafGenerationPendingFileIDs[:0], stagedLeafManifest.pendingFileIDs...)
		}
	}
	if leafGenerationView == nil {
		leafGenerationView = db.currentLeafGenerationView()
	}
	newState := &DBState{
		CommitSeq:         nextMeta.CommitSeq,
		RootPageID:        nextMeta.UserRootPageID,
		SystemRootPageID:  nextMeta.SystemRootPageID,
		AppliedCommandLSN: nextMeta.AppliedCommandLSN,
		MaxEntryRevision:  page.EntryRevision(nextMeta.MaxEntryRevision),
		ValueLogSet:       valueLogSet,
		LeafGenerations:   leafGenerationView,
	}
	if leafGenerationView != nil {
		db.leafGenerationStateVersion++
		newState.LeafGenerationStateVersion = db.leafGenerationStateVersion
	}
	db.state.Store(newState)
	opts.conditionalMutation.record(db, nextMeta.CommitSeq)
	if opts.commandWALPublish {
		previousApplied := uint64(0)
		if post.oldState != nil {
			previousApplied = post.oldState.AppliedCommandLSN
		}
		db.observeCommandWALCovered(previousApplied, nextMeta.AppliedCommandLSN)
	}
	db.publishSnapshotView(idx, newState, db.valueLogManager)
	post.commitSeq = nextMeta.CommitSeq
	post.vlogRefDelta = vlogRefDelta
	if db.leafPageLog != nil && len(post.clearLeafGenerationPendingFileIDs) == 0 {
		post.drainLeafGenerationPending = true
	}
	db.mu.Unlock()
	db.rootReuseMu.Unlock()
	rootReuseLocked = false
	watermarkHold += time.Since(holdStart)
	db.observePublishWatermark(watermarkWait, watermarkHold, watermarkWait+watermarkHold)

	// The reachability tracker is publication input for the next durable
	// candidate, so advance it before durablePublishMu can pass to another
	// publisher. Deferring this to caller post-work permits out-of-order deltas
	// and can make a sequence-current tracker carry counts from an older root.
	if db.valueLogRefTracker != nil {
		if post.vlogRefDelta != nil {
			if err := db.valueLogRefTracker.applyDelta(post.commitSeq, post.vlogRefDelta); err != nil {
				db.valueLogRefTracker.invalidate()
				db.reportError(err)
			}
		} else {
			db.valueLogRefTracker.invalidate()
		}
		post.vlogRefTrackerAdvanced = true
	}
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
	if !opts.skipConditionalRootConflict {
		db.conditionalRecordRootCommit(oldUserRootID, newRootID, post.commitSeq)
	}
	if opts.recordVacuumMutation != nil {
		opts.recordVacuumMutation()
	}

	return post, nil
}

func (db *DB) finalizeCommitPostWork(post finalizeCommitPost) {
	var durPrune time.Duration

	if post.vlogRefDelta != nil {
		defer releaseValueLogRefDelta(post.vlogRefDelta)
	}
	if db.valueLogRefTracker != nil && !post.vlogRefTrackerAdvanced {
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
	if post.persistLeafGenerationManifest || post.persistLeafGenerationIndexesOnly || post.drainLeafGenerationPending || len(post.clearLeafGenerationPendingFileIDs) > 0 {
		var (
			persistErr        error
			pendingErr        error
			clearPending      bool
			assignPersisted   bool
			manifestPersisted bool
		)
		db.commitMu.Lock()
		currentCommitSeq := db.meta.CommitSeq
		currentManifest := db.leafGenerationManifest
		currentState := db.state.Load()
		if post.persistLeafGenerationManifest {
			switch {
			case currentManifest == post.persistLeafGenerationManifestView:
				manifestPersisted = true
			case post.persistLeafGenerationStateView != nil && currentState != nil && currentState.LeafGenerations == post.persistLeafGenerationStateView:
				manifestPersisted = true
				assignPersisted = true
			}
			if manifestPersisted {
				persistErr = db.persistLeafGenerationManifestAndRecordLengthIndexes(post.persistLeafGenerationManifestView, post.persistLeafGenerationRawFileIDs)
				if persistErr == nil {
					clearPending = len(post.clearLeafGenerationPendingFileIDs) > 0
					if assignPersisted {
						db.mu.Lock()
						if state := db.state.Load(); state != nil && state.LeafGenerations == post.persistLeafGenerationStateView {
							db.leafGenerationManifest = post.persistLeafGenerationManifestView
						}
						db.mu.Unlock()
					}
				}
			}
		} else if post.persistLeafGenerationIndexesOnly {
			db.persistLeafGenerationRecordLengthIndexes(post.persistLeafGenerationRawFileIDs)
		} else if len(post.clearLeafGenerationPendingFileIDs) > 0 {
			clearPending = true
		}
		if clearPending {
			db.clearLeafGenerationPendingFileIDs(post.clearLeafGenerationPendingFileIDs)
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

// finalizeAcceptedCommitPostWorkOnError completes the non-serialized work for
// a candidate whose ownership and visible-state activation already crossed the
// point of no return, even though a later admission or durability wait failed.
//
// The value-log ref delta remains producer-owned on every error return. Clear
// it from this post-work copy so the producer's existing error cleanup releases
// it exactly once; all other accepted-publication work belongs to the DB and
// must not be deferred to a later durability retry.
func (db *DB) finalizeAcceptedCommitPostWorkOnError(post finalizeCommitPost) finalizeCommitPost {
	if !post.accepted {
		return post
	}
	post.vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	commitSeq := post.commitSeq
	return finalizeCommitPost{accepted: true, commitSeq: commitSeq}
}

// finalizeCommitReleasingRootSerialization transfers a completed root build to
// the durable publisher. The caller must hold every lock named by release. The
// expected base sequence must be the visible commit observed before root
// construction began; sampling it in this helper would let a predecessor
// activate after construction but before finalization and bless a stale root.
//
// The helper acquires publication serialization in root-lock order, releases those
// locks before legacy dependency preparation, and keeps the durable publisher
// locked through candidate preparation, exact I/O, and visible-state install.
func (db *DB) finalizeCommitReleasingRootSerialization(
	newRootID uint64,
	sysRootID uint64,
	retired []uint64,
	sync bool,
	metrics adaptive.Metrics,
	touchedValueLogSegments []uint32,
	forceValueLogRefresh bool,
	vlogRefDelta *valueLogRefDelta,
	leafManifest *leafGenerationManifest,
	leafManifestRawFileIDs []uint32,
	opts finalizeCommitOptions,
	expectedBaseCommitSeq uint64,
	release func(),
	onError func(error),
) (finalizeCommitPost, error) {
	if release == nil {
		return finalizeCommitPost{}, errors.New("missing root-serialization release")
	}
	if hook := db.testBeforeFinalizeCommitHook; hook != nil {
		hook()
	}
	if opts.durableIndex == nil {
		opts.durableIndex = db.idx.Load()
	}
	opts.expectedBaseCommitSeq = expectedBaseCommitSeq
	opts.hasExpectedBaseCommitSeq = true
	rootRuntime := db.rootPublication
	var builder *rootpublication.BuilderToken
	if rootRuntime != nil {
		var err error
		builder, err = rootRuntime.coordinator.AcquireBuilder(context.Background())
		if err != nil {
			err = publicRootPublicationErrorV1(err)
			if onError != nil {
				onError(err)
			}
			return finalizeCommitPost{}, fmt.Errorf("acquire root-publication builder: %w", err)
		}
		defer builder.Release()
	}
	durablePublishLocked := false
	releaseDurablePublish := func() {
		if durablePublishLocked {
			db.durablePublishMu.Unlock()
			durablePublishLocked = false
		}
	}
	db.durablePublishMu.Lock()
	durablePublishLocked = true
	defer releaseDurablePublish()
	release()
	if hook := db.testAfterFinalizeRootSerializationReleaseHook; hook != nil {
		hook()
	}

	prepareStart := time.Now()
	guard, err := db.prepareFinalizeCommitDurability(sync)
	if opts.publishTiming != nil {
		opts.publishTiming.FinalizePrepareDurability += time.Since(prepareStart)
	}
	if err != nil {
		if onError != nil {
			onError(err)
		}
		return finalizeCommitPost{}, err
	}
	defer guard.Release()

	opts.skipPrePublishFlush = true
	opts.durablePublishLocked = true
	opts.durablePublishRelease = releaseDurablePublish
	opts.rootPublicationBuilder = builder
	opts.releaseRootSerialization = func() {}
	post, err := db.finalizeCommitLockedWithOptions(
		newRootID, sysRootID, retired, sync, metrics, touchedValueLogSegments,
		forceValueLogRefresh, vlogRefDelta, leafManifest, leafManifestRawFileIDs, opts,
	)
	if err != nil && !post.accepted && onError != nil {
		onError(err)
	}
	return post, err
}

// CommitAtState synchronously persists a manually constructed user root only
// when the visible commit sequence and roots still match the caller's basis.
// Callers must capture basis before constructing newRootID.
func (db *DB) CommitAtState(newRootID uint64, basis StateToken) error {
	return db.commitManualRoot(newRootID, basis, true)
}

// ForceCommit synchronously force-sets a manually constructed user root. It
// intentionally does not detect publications that occurred while newRootID was
// constructed. Prefer CommitAtState unless last-writer-wins replacement is the
// explicit operation.
func (db *DB) ForceCommit(newRootID uint64) error {
	return db.commitManualRoot(newRootID, StateToken{}, false)
}

func (db *DB) commitManualRoot(newRootID uint64, basis StateToken, conditional bool) error {
	if db.readOnly {
		return ErrReadOnly
	}
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if err := db.rejectUnloggedCommandWALRootPublish(); err != nil {
		return err
	}
	for {
		post, err := db.commitManualRootAttempt(newRootID, basis, conditional)
		if err == nil {
			db.finalizeCommitPostWork(post)
			return nil
		}
		if !errors.Is(err, errDurableRootCandidateStale) {
			return err
		}
		if conditional {
			return fmt.Errorf(
				"%w: manual root basis advanced during publication from commit=%d",
				ErrConcurrentModification,
				basis.CommitSeq,
			)
		}
	}
}

func (db *DB) commitManualRootAttempt(newRootID uint64, basis StateToken, conditional bool) (finalizeCommitPost, error) {
	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return finalizeCommitPost{}, err
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	sysRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if conditional && (baseSeq != basis.CommitSeq || userRoot != basis.RootPageID || sysRoot != basis.SystemRootPageID) {
		return finalizeCommitPost{}, fmt.Errorf(
			"%w: manual root basis commit=%d/%d user_root=%d/%d system_root=%d/%d",
			ErrConcurrentModification,
			basis.CommitSeq, baseSeq,
			basis.RootPageID, userRoot,
			basis.SystemRootPageID, sysRoot,
		)
	}

	return db.finalizeCommitReleasingRootSerialization(
		newRootID, sysRoot, nil, true, adaptive.Metrics{}, nil, true, nil, nil, nil,
		finalizeCommitOptions{closeTeardownPinned: true},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
}

// Checkpoint forces a durable boundary for previously-published backend state.
//
// Unlike CommitAtState or ForceCommit, this does not publish a new root or
// advance CommitSeq. It is intended for callers that already made writes
// visible with relaxed durability and now need those writes durable on disk.
func (db *DB) Checkpoint() error {
	return db.checkpoint(false)
}

// checkpoint runs with maintenanceAlreadyHeld only for an enclosing backend
// maintenance operation such as CompactStorage. Command-WAL cleanup must not
// recursively acquire maintenanceMu in that case.
func (db *DB) checkpoint(maintenanceAlreadyHeld bool) error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if db.testCheckpointAfterPoisonPreflightHook != nil {
		db.testCheckpointAfterPoisonPreflightHook()
	}

	// Command publishers take raw-publish before writeMu. Checkpoint must use
	// the same order so its command-WAL frontier cannot deadlock a writer that
	// has appended a frame and is waiting to publish the matching root.
	unlockCommandWALPublish := db.lockCommandWALRawPublish()
	defer func() { unlockCommandWALPublish() }()

	db.writeMu.Lock()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		db.writeMu.Unlock()
		return err
	}
	if err := db.commandWALPoisonedError(); err != nil {
		db.writeMu.Unlock()
		return err
	}

	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		db.writeMu.Unlock()
		return errors.New("missing index")
	}
	if runtime := db.rootPublication; runtime != nil && runtime.coordinator != nil {
		db.mu.RLock()
		visibleCommitSeq := db.meta.CommitSeq
		db.mu.RUnlock()
		db.writeMu.Unlock()
		// Let post-cut writers enter while the coordinator makes the captured
		// root prefix durable. Reacquire raw-publish afterwards and close the
		// exact then-current WAL prefix; a durable WAL frontier may safely run
		// ahead of the cleanup frontier selected from recovery-safe roots.
		unlockCommandWALPublish()
		unlockCommandWALPublish = func() {}
		// Checkpoint is a root durability boundary in every profile. Waiting is
		// deliberately outside write/commit/root-build locks; Publisher owns the
		// dependency -> index -> alternate-meta ordering for this exact prefix.
		if err := runtime.coordinator.WaitThrough(context.Background(), visibleCommitSeq); err != nil {
			return publicRootPublicationErrorV1(err)
		}
		unlockCommandWALPublish = db.lockCommandWALRawPublish()
		rotatedCommandWAL, commandWALPrefixAdvanced, err := db.closeCommandWALCheckpointPrefix()
		if err != nil {
			return err
		}
		if rotatedCommandWAL || commandWALPrefixAdvanced {
			// Cleanup captures and revalidates its own exact journal namespace
			// snapshot. Release raw-publish before maintenance admission so a
			// concurrent maintenance checkpoint cannot invert those locks.
			unlockCommandWALPublish()
			unlockCommandWALPublish = func() {}
			if err := db.cleanupCommandWALCoveredSegmentsAtCheckpointV1(maintenanceAlreadyHeld); err != nil {
				return err
			}
		}
		return nil
	}
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
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
	rotatedCommandWAL, commandWALPrefixAdvanced, err := db.closeCommandWALCheckpointPrefix()
	if err != nil {
		return err
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
	if rotatedCommandWAL || commandWALPrefixAdvanced {
		// The cleanup proof independently rejects new appends and namespace
		// changes. Drop write/raw-publish before maintenance admission to keep
		// the global maintenance -> raw-publish -> write order acyclic.
		db.writeMu.Unlock()
		writeLocked = false
		unlockCommandWALPublish()
		unlockCommandWALPublish = func() {}
		if err := db.cleanupCommandWALCoveredSegmentsAtCheckpointV1(maintenanceAlreadyHeld); err != nil {
			return err
		}
	}
	return nil
}

// closeCommandWALCheckpointPrefix establishes the dependency-closed physical
// WAL frontier used by cleanup proofs. The caller must hold raw-publish so the
// latest assigned LSN, dependency debt, active segment sync, and frontier
// publication describe one append-serialization cut.
func (db *DB) closeCommandWALCheckpointPrefix() (rotated, advanced bool, err error) {
	if db == nil || !db.commandWAL || db.commandJournal == nil {
		return false, false, nil
	}
	nextLSN := db.CommandWALNextLSN()
	if nextLSN <= 1 {
		return false, false, nil
	}
	frontier := nextLSN - 1
	durableWALLSN := db.commandWALDurableLSN.Load()
	// A relaxed frame can be recovered into a durable root while remaining
	// above the physically classified durable-WAL frontier. The writer then
	// starts at AppliedCommandLSN+1, often with an empty successor. Syncing that
	// successor proves nothing about the older recovered segment, so only a
	// session whose classified frontier already covers its initial applied LSN
	// may promote the prefix through this checkpoint path. A later reopen can
	// establish that recovery baseline; an explicit durable append/barrier may
	// independently advance commandWALDurableLSN through its own contract.
	advanced = durableWALLSN >= db.commandWALSessionAppliedLSN && durableWALLSN < frontier
	if advanced {
		if err := db.syncCommandWALDependenciesThrough(frontier, nil); err != nil {
			return false, false, err
		}
	}
	if db.CommandWALActiveBytes() > 0 {
		if err := db.RotateCommandWALActiveSegment(true); err != nil {
			return false, false, err
		}
		rotated = true
	} else if advanced {
		// Automatic rotations retain exact stable handles in dependency debt.
		// Sync the current (possibly empty) active target as the final journal
		// boundary after those older exact handles have crossed their barriers.
		if err := db.FlushCommandWAL(true); err != nil {
			return false, false, err
		}
	}
	if advanced {
		db.closeCommandWALDurablePrefixThrough(frontier)
	}
	return rotated, advanced, nil
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

	min := db.MinPinnedSnapshotCommitSeq()
	state := db.state.Load()
	if state == nil {
		return
	}
	current := state.CommitSeq

	freed := idx.graveyard.Extract(min, current, db.keepRecent)

	if len(freed) > 0 {
		for _, id := range freed {
			_ = idx.allocator.Free(id) // Ignore error?
		}
	}
}

// Get returns value from snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	return s.tree.Get(normalizeRawKVPointKey(key))
}

// GetAppend appends the value for key to dst and returns the grown slice.
// If key is not found, it returns dst and tree.ErrKeyNotFound.
func (s *Snapshot) GetAppend(key, dst []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return dst, err
	}
	defer s.endRead()
	return s.tree.GetAppend(normalizeRawKVPointKey(key), dst)
}

// GetVersionedAppend appends the value for key to dst and returns the native
// entry revision stored with the visible leaf entry.
func (s *Snapshot) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	if err := s.beginRead(); err != nil {
		return dst, page.LegacyEntryRevision, err
	}
	defer s.endRead()
	return s.tree.GetVersionedAppend(normalizeRawKVPointKey(key), dst)
}

// GetVersioned returns a safe value copy plus the native entry revision stored
// with the visible leaf entry.
func (s *Snapshot) GetVersioned(key []byte) ([]byte, page.EntryRevision, error) {
	out, revision, err := s.GetVersionedAppend(key, nil)
	if err != nil {
		return nil, revision, err
	}
	if len(out) == 0 {
		return []byte{}, revision, nil
	}
	if cap(out) == len(out) {
		return out, revision, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, revision, nil
}

// GetManyView calls fn once for each key with a read-only value view.
// Values are valid until fn returns and must be copied before retaining.
func (s *Snapshot) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	keys = normalizeRawKVPointKeys(keys)
	return s.tree.GetManyView(keys, fn)
}

// GetUnsafe returns a zero-copy view of the value from the snapshot.
// The slice is valid until the snapshot is closed.
func (s *Snapshot) GetUnsafe(key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	return s.tree.GetUnsafe(normalizeRawKVPointKey(key))
}

func (s *Snapshot) Has(key []byte) (bool, error) {
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	return s.tree.Has(normalizeRawKVPointKey(key))
}

func (s *Snapshot) HasMany(keys [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	return s.tree.HasMany(normalizeRawKVPointKeys(keys))
}

func (s *Snapshot) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	return s.tree.HasPrefixes(prefixes)
}

// GetEntry returns the persisted leaf entry for key.
func (s *Snapshot) GetEntry(key []byte) (node.LeafEntry, error) {
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	return s.tree.GetEntry(normalizeRawKVPointKey(key))
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
	if db == nil {
		return nil
	}
	return cloneDBState(db.state.Load())
}

// StateToken returns an immutable scalar state from one coherent publication.
func (db *DB) StateToken() (StateToken, bool) {
	if db == nil {
		return StateToken{}, false
	}
	view := db.snapshotViewRO.Load()
	if view == nil {
		return StateToken{}, false
	}
	return stateTokenFromState(view.state)
}

func stateTokenFromState(state *DBState) (StateToken, bool) {
	if state == nil {
		return StateToken{}, false
	}
	return StateToken{
		CommitSeq:                  state.CommitSeq,
		RootPageID:                 state.RootPageID,
		SystemRootPageID:           state.SystemRootPageID,
		AppliedCommandLSN:          state.AppliedCommandLSN,
		MaxEntryRevision:           state.MaxEntryRevision,
		LeafGenerationStateVersion: state.LeafGenerationStateVersion,
	}, true
}

func cloneDBState(state *DBState) *DBState {
	if state == nil {
		return nil
	}
	copyState := *state
	return &copyState
}

// IsClosing reports whether the database is closing. It returns true if db is nil.
func (db *DB) IsClosing() bool {
	return db == nil || db.closing.Load()
}

// beginVacuumCutoverGateLocked starts the narrow interval in which online
// vacuum has stopped recording old-generation mutations but deliberately does
// not hold writeMu while syncing the replacement. The caller holds writeMu.
func (db *DB) beginVacuumCutoverGateLocked() {
	db.vacuumCutoverGateMu.Lock()
	defer db.vacuumCutoverGateMu.Unlock()
	if db.vacuumCutoverDone != nil || db.vacuumCutoverInProgress.Load() {
		panic("treedb: vacuum cutover gate already active")
	}
	db.vacuumCutoverDone = make(chan struct{})
	db.vacuumCutoverInProgress.Store(true)
}

// endVacuumCutoverGateLocked releases writers after the replacement has either
// been installed or abandoned. The caller holds writeMu, so awakened writers
// cannot pass admission until the final cutover state is visible.
func (db *DB) endVacuumCutoverGateLocked() {
	db.vacuumCutoverGateMu.Lock()
	done := db.vacuumCutoverDone
	db.vacuumCutoverDone = nil
	db.vacuumCutoverInProgress.Store(false)
	if done != nil {
		close(done)
	}
	db.vacuumCutoverGateMu.Unlock()
}

// waitForVacuumCutoverWriteLocked keeps ordinary writers blocked while online
// vacuum syncs a replacement pager without writeMu. The caller must hold
// writeMu exclusively; it returns with writeMu held exclusively.
func (db *DB) waitForVacuumCutoverWriteLocked() {
	for db != nil && db.vacuumCutoverInProgress.Load() {
		db.vacuumCutoverGateMu.Lock()
		done := db.vacuumCutoverDone
		active := db.vacuumCutoverInProgress.Load() && done != nil
		db.vacuumCutoverGateMu.Unlock()
		if !active {
			return
		}
		db.writeMu.Unlock()
		<-done
		db.writeMu.Lock()
	}
}

// checkWriteAdmissionLocked rejects writers that lost the race with Close and
// waits out the sync-without-writeMu interval of an online-vacuum cutover.
// Callers must hold writeMu exclusively so a successful admission remains
// valid until their write-critical section completes.
func (db *DB) checkWriteAdmissionLocked() error {
	if db == nil {
		return ErrClosed
	}
	db.waitForVacuumCutoverWriteLocked()
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return err
	}
	return nil
}

// checkReadAdmissionLocked is the read-lock counterpart used by validation and
// optimistic build phases that do not mutate the selected generation.
func (db *DB) checkReadAdmissionLocked() error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return err
	}
	return nil
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
	coherentRootChanged := old == nil || old.idx == nil || old.state == nil || old.idx.id != idx.id || old.state.SystemRootPageID != state.SystemRootPageID
	publishEpoch := db.systemRootPublishEpoch.Load()
	if coherentRootChanged {
		publishEpoch = db.systemRootPublishEpoch.Add(1)
	}
	if old != nil && old.state != nil && old.state.LeafGenerations != nil && old.state.LeafGenerations != state.LeafGenerations {
		db.markLeafGenerationPinSetStale(old.state.LeafGenerations.PinSet)
	}
	db.snapshotViewRO.Store(&snapshotView{
		idx:                    idx,
		state:                  state,
		vlogManager:            vm,
		systemRootPublishEpoch: publishEpoch,
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
	if db == nil {
		return ErrClosed
	}
	db.valueLogPublicationMu.RLock()
	defer db.valueLogPublicationMu.RUnlock()
	if err := db.publicationPoisonedError(); err != nil {
		return err
	}
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
		AppliedCommandLSN:          oldState.AppliedCommandLSN,
		MaxEntryRevision:           oldState.MaxEntryRevision,
		ValueLogSet:                db.valueLogManager.CurrentSetNoRefresh(),
		LeafGenerations:            oldState.LeafGenerations,
		LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)

	if oldState.ValueLogSet != nil {
		if err := db.valueLogManager.Release(oldState.ValueLogSet); err != nil {
			return errors.Join(err, ErrRecoveryRequired)
		}
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
		AppliedCommandLSN:          oldState.AppliedCommandLSN,
		MaxEntryRevision:           oldState.MaxEntryRevision,
		ValueLogSet:                valueLogSet,
		LeafGenerations:            oldState.LeafGenerations,
		LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)

	if oldState.ValueLogSet != nil {
		if err := db.valueLogManager.Release(oldState.ValueLogSet); err != nil {
			return errors.Join(err, ErrRecoveryRequired)
		}
	}
	return nil
}

// MarkValueLogZombie marks a value-log segment as zombie so it can be removed
// once all snapshots release it.
func (db *DB) MarkValueLogZombie(id uint32) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("value log manager unavailable")
	}
	stats, err := db.valueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{id},
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
		observedSourceMissingIsError:     true,
	}, true)
	if err != nil {
		if errors.Is(err, ErrRecoverableRootSetStale) {
			return errors.Join(
				fmt.Errorf("%w: file_id=%d", ErrValueLogZombieDeferred, id),
				err,
			)
		}
		return err
	}
	if stats.ObservedSourceSegmentsEligible != 1 {
		return fmt.Errorf(
			"%w: file_id=%d referenced=%d active=%d protected=%d",
			ErrValueLogZombieDeferred,
			id,
			stats.ObservedSourceSegmentsReferenced,
			stats.ObservedSourceSegmentsActive,
			stats.ObservedSourceSegmentsProtected,
		)
	}
	return nil
}

// CompactIndex rewrites the entire B-Tree sequentially to the end of the file.
// This improves Full Scan performance by restoring physical locality.
// Note: This operation causes file growth as old pages are not immediately reclaimed
// (they are leaked to the freelist but not reused during this append-only build).
func (db *DB) CompactIndex() error {
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return err
	}
	if err := db.rejectUnloggedCommandWALRootPublish(); err != nil {
		return err
	}

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
	baseSeq := db.meta.CommitSeq
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
	alloc := appendOnlyPageAllocator{alloc: idx.allocator}
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
	post, err := db.finalizeCommitReleasingRootSerialization(
		newRoot, sysRoot, retired, true, adaptive.Metrics{}, nil, true, nil, nil, nil,
		finalizeCommitOptions{closeTeardownPinned: true},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return err
	}
	db.finalizeCommitPostWork(post)
	return nil
}

type pagerAllocator struct {
	p *pager.Pager
}

func (a *pagerAllocator) Alloc(hint uint64) (uint64, error) {
	return a.p.Alloc(1)
}
