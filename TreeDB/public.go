package treedb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/limits"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

// Options configures TreeDB. It is re-exported from TreeDB/db for convenience.
type Options = db.Options

var errVacuumUnsupported = db.ErrVacuumUnsupported

// EntryRevision is TreeDB's native per-entry revision metadata. Revision zero is
// reserved for legacy entries that do not carry revision metadata.
type EntryRevision = page.EntryRevision

const LegacyEntryRevision = page.LegacyEntryRevision

// ConditionalTxn is TreeDB's native conditional transaction type.
type ConditionalTxn struct {
	backend         *db.ConditionalTxn
	cached          caching.ConditionalTxn
	cachedActive    bool
	snapshotExposed bool
}

type MaintenancePhase = caching.MaintenancePhase

type LeafPageReadCacheWriteAdmissionPolicy = db.LeafPageReadCacheWriteAdmissionPolicy

type FlushAdmissionPolicy = db.FlushAdmissionPolicy

type FlushAdmissionDecision = db.FlushAdmissionDecision

const (
	MaintenancePhaseSteady  = caching.MaintenancePhaseSteady
	MaintenancePhaseRestore = caching.MaintenancePhaseRestore
	MaintenancePhaseCatchUp = caching.MaintenancePhaseCatchUp

	LeafPageReadCacheWriteAdmissionImmediate = db.LeafPageReadCacheWriteAdmissionImmediate
	LeafPageReadCacheWriteAdmissionAdaptive  = db.LeafPageReadCacheWriteAdmissionAdaptive

	FlushAdmissionPolicyExplicit = db.FlushAdmissionPolicyExplicit
	FlushAdmissionPolicyOff      = db.FlushAdmissionPolicyOff
	FlushAdmissionPolicyAuto     = db.FlushAdmissionPolicyAuto
)

// ErrNamespacePersistenceUnsupported reports that the active platform or
// filesystem cannot persist a directory creation required by a successful
// writable Open.
var ErrNamespacePersistenceUnsupported = db.ErrNamespacePersistenceUnsupported

var ensureOpenStorageLayoutDirs = db.EnsureStorageLayoutDirsForOpen

const (
	defaultChunkSize     = 256 * 1024
	defaultDictChunkSize = 64 * 1024
	// Template side-store pages intentionally use the same default size as dictdb
	// pages so both auxiliary stores behave consistently under the unified layout.
	defaultTemplateChunkSize = defaultDictChunkSize

	defaultSlowdownBacklogSeconds        = 1.0
	defaultStopBacklogSeconds            = 2.0
	defaultAdaptiveMaxBacklogBytes int64 = 2 << 30
)

// Iterator is the public storage-neutral iterator contract returned by TreeDB.
// Seek remains inside the original [start,end) domain: forward iterators choose
// the first key >= target, while reverse iterators choose the first key <=
// target (nil seeks to the greatest key). See package TreeDB/iterator for view
// ownership rules.
type Iterator = publiciterator.Iterator

// Batch is the public batch contract returned by TreeDB.
// Both cached and backend implementations satisfy it.
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	DeleteRange(start, end []byte) error
	Write() error
	WriteSync() error
	Close() error
	Replay(func(batch.Entry) error) error
	GetByteSize() (int, error)
}

type getManyPlanner interface {
	GetManyParallelPlan(keyCount int) (workers int, parallel bool)
}

// GetManyViewFunc receives one GetManyView result. DB-level callbacks may be
// invoked in any order and may be invoked concurrently for large batches;
// callers that mutate shared state must synchronize it. The value slice is a
// read-only view that is valid only until the callback returns (or until a
// snapshot/view boundary documented by the caller closes, whichever comes
// first). Copy value before retaining it. Missing/tombstoned keys are reported
// with found=false and value=nil.
type GetManyViewFunc = db.GetManyViewFunc

// UpdateOp describes the write produced by an Update callback.
type UpdateOp = db.UpdateOp

const (
	// UpdateNoop leaves the key unchanged.
	UpdateNoop = db.UpdateNoop
	// UpdateSet replaces the key with Value.
	UpdateSet = db.UpdateSet
	// UpdateDelete removes the key.
	UpdateDelete = db.UpdateDelete
)

// UpdateResult is returned by an Update callback.
type UpdateResult = db.UpdateResult

// SetUpdate returns an Update result that replaces the key with value.
func SetUpdate(value []byte) UpdateResult {
	return db.SetUpdate(value)
}

// DeleteUpdate returns an Update result that removes the key.
func DeleteUpdate() UpdateResult {
	return db.DeleteUpdate()
}

// NoopUpdate returns an Update result that leaves the key unchanged.
func NoopUpdate() UpdateResult {
	return db.NoopUpdate()
}

// UpdateFunc transforms the current value for a key into a mutation. The old
// value is nil when the key is absent and is a safe copy when present. The
// callback may be retried if the key changes before the mutation commits.
type UpdateFunc = db.UpdateFunc

// DB is the public TreeDB handle (cached mode by default; read-only opens skip caching).
type DB struct {
	cached                               *caching.DB
	backend                              *db.DB
	dictdb                               *db.DB
	templateDB                           *DB
	writePath                            writePathInfo
	lifecycleMu                          sync.RWMutex
	commandWALCached                     bool
	commandWALPendingMu                  sync.Mutex
	commandWALPublicPublishMu            sync.Mutex
	commandWALPublicOperationGate        sync.RWMutex
	commandWALPublicPayloadPool          sync.Pool
	commandWALFirst                      atomic.Uint64
	commandWALLast                       atomic.Uint64
	commandWALCheckpointCutoverLast      atomic.Uint64
	commandWALCheckpointCutoverErr       error
	commandWALLiveFrames                 atomic.Uint64
	commandWALGroupCommit                publicCommandWALGroupCommit
	commandWALPublicBatchSetCalls        atomic.Uint64
	commandWALPublicBatchSetBytes        atomic.Uint64
	commandWALPublicBatchSetViewCalls    atomic.Uint64
	commandWALPublicBatchSetViewBytes    atomic.Uint64
	commandWALPublicBatchDeleteCalls     atomic.Uint64
	commandWALPublicBatchDeleteBytes     atomic.Uint64
	commandWALPublicBatchDeleteViewCalls atomic.Uint64
	commandWALPublicBatchDeleteViewBytes atomic.Uint64
	rawSpanNativePublicUpdateReject      atomic.Uint64
	rawSpanNativePublicUpdateSyncReject  atomic.Uint64
	publicBatchWrite                     publicOperationStats
	publicBatchWriteSync                 publicOperationStats
	publicBatchWriteSyncPhaseEnabled     bool
	publicBatchWriteSyncPhase            publicBatchWriteSyncPhaseStats
	publicCheckpoint                     publicOperationStats
	bgVac                                bgIndexVacuumWorker
	notifyError                          func(error)
	bgErrMu                              sync.Mutex
	bgErr                                error
	resolvedProfile                      Profile
	deprecatedProfileAlias               Profile
	durabilityMode                       string
	valueLogReadIntegrity                string
	dir                                  string
	maintenance                          maintenanceCoordinator
}

type publicOperationStats struct {
	calls   atomic.Uint64
	errors  atomic.Uint64
	nsTotal atomic.Uint64
	lastNs  atomic.Int64
	maxNs   atomic.Int64
}

type publicBatchWriteSyncPhaseSample struct {
	checkpointGate                                 time.Duration
	preflightMaterialization                       time.Duration
	commandCallback                                time.Duration
	commandPublicPayloadEntryScanPreparation       time.Duration
	commandPublicPreparationObserved               bool
	commandPublishLockBarrierWait                  time.Duration
	commandPublishLockBarrierWaitObserved          bool
	commandBackendIntentPlanningSerialization      time.Duration
	commandBackendIntentPlanningObserved           bool
	commandExternalRefOrdering                     time.Duration
	commandExternalRefOrderingObserved             bool
	commandAppend                                  time.Duration
	commandAppendObserved                          bool
	commandFlush                                   time.Duration
	commandFlushObserved                           bool
	commandGroupCommitWait                         time.Duration
	commandGroupCommitWaitObserved                 bool
	commandSync                                    time.Duration
	commandSyncObserved                            bool
	commandPostAppendPendingLSNBookkeeping         time.Duration
	commandPostAppendPendingLSNBookkeepingObserved bool
	commandEmptyBarrier                            time.Duration
	commandEmptyBarrierObserved                    bool
	memtablePublicationReset                       time.Duration
}

type publicBatchWriteSyncPhaseStats struct {
	calls                                       atomic.Uint64
	errors                                      atomic.Uint64
	wallNs                                      atomic.Uint64
	checkpointGateNs                            atomic.Uint64
	preflightMaterializationNs                  atomic.Uint64
	commandCallbackNs                           atomic.Uint64
	commandPublicPreparationCalls               atomic.Uint64
	commandPublicPayloadEntryScanPreparationNs  atomic.Uint64
	commandPublishLockBarrierWaitCalls          atomic.Uint64
	commandPublishLockBarrierWaitNs             atomic.Uint64
	commandBackendIntentPlanningCalls           atomic.Uint64
	commandBackendIntentPlanningSerializationNs atomic.Uint64
	commandExternalRefOrderingCalls             atomic.Uint64
	commandExternalRefOrderingNs                atomic.Uint64
	commandAppendCalls                          atomic.Uint64
	commandAppendNs                             atomic.Uint64
	commandFlushCalls                           atomic.Uint64
	commandFlushNs                              atomic.Uint64
	commandGroupCommitWaitCalls                 atomic.Uint64
	commandGroupCommitWaitNs                    atomic.Uint64
	commandSyncCalls                            atomic.Uint64
	commandSyncNs                               atomic.Uint64
	commandPostAppendPendingLSNBookkeepingCalls atomic.Uint64
	commandPostAppendPendingLSNBookkeepingNs    atomic.Uint64
	commandEmptyBarrierCalls                    atomic.Uint64
	commandEmptyBarrierNs                       atomic.Uint64
	commandOtherNs                              atomic.Uint64
	memtablePublicationResetNs                  atomic.Uint64
	residualNs                                  atomic.Uint64
	topLevelPartitionOverruns                   atomic.Uint64
	commandPartitionOverruns                    atomic.Uint64
}

var (
	testAfterPublicCommandWALPointAppend func(commitlog.RawKVOperation)
	testAfterCachedCheckpoint            func()
	testDuringPublicCloseAfterCheckpoint func()
)

type writePathInfo struct {
	mode       string
	valueStore string
	redoLog    string
	warn       bool
}

type maintenanceCoordinator struct {
	mu sync.Mutex

	active atomic.Int32

	deferrals atomic.Uint64
	waitTotal atomic.Int64
	waitMax   atomic.Int64

	gcRuns     atomic.Uint64
	leafGCRuns atomic.Uint64
	vacuumRuns atomic.Uint64

	lastGCAt     atomic.Int64
	lastLeafGCAt atomic.Int64
	lastVacuumAt atomic.Int64
}

const (
	maintenanceOpNone int32 = iota
	maintenanceOpGC
	maintenanceOpLeafGC
	maintenanceOpVacuum
	maintenanceOpOther
)

func maintenanceOpCode(op string) int32 {
	switch op {
	case "gc":
		return maintenanceOpGC
	case "leaf-gc":
		return maintenanceOpLeafGC
	case "vacuum":
		return maintenanceOpVacuum
	case "":
		return maintenanceOpNone
	default:
		return maintenanceOpOther
	}
}

func maintenanceActiveLabel(code int32) string {
	switch code {
	case maintenanceOpGC:
		return "gc"
	case maintenanceOpLeafGC:
		return "leaf-gc"
	case maintenanceOpVacuum:
		return "vacuum"
	case maintenanceOpOther:
		return "other"
	default:
		return ""
	}
}

func atomicStoreMaxInt64(dst *atomic.Int64, value int64) {
	for {
		cur := dst.Load()
		if value <= cur {
			return
		}
		if dst.CompareAndSwap(cur, value) {
			return
		}
	}
}

func (s *publicOperationStats) observe(start time.Time, err error) {
	if s == nil {
		return
	}
	elapsed := time.Since(start).Nanoseconds()
	if elapsed < 0 {
		elapsed = 0
	}
	s.calls.Add(1)
	if err != nil {
		s.errors.Add(1)
	}
	s.nsTotal.Add(uint64(elapsed))
	s.lastNs.Store(elapsed)
	atomicStoreMaxInt64(&s.maxNs, elapsed)
}

func publicOperationStatsInto(stats map[string]string, prefix string, s *publicOperationStats) {
	if stats == nil || s == nil || prefix == "" {
		return
	}
	stats[prefix+".calls_total"] = fmt.Sprintf("%d", s.calls.Load())
	stats[prefix+".errors_total"] = fmt.Sprintf("%d", s.errors.Load())
	stats[prefix+".ns_total"] = fmt.Sprintf("%d", s.nsTotal.Load())
	stats[prefix+".last_ns"] = fmt.Sprintf("%d", s.lastNs.Load())
	stats[prefix+".max_ns"] = fmt.Sprintf("%d", s.maxNs.Load())
}

func nonNegativeDurationNs(value time.Duration) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value.Nanoseconds())
}

func (s *publicBatchWriteSyncPhaseStats) observe(start time.Time, err error, sample publicBatchWriteSyncPhaseSample) {
	if s == nil {
		return
	}
	wallNs := nonNegativeDurationNs(time.Since(start))
	checkpointGateNs := nonNegativeDurationNs(sample.checkpointGate)
	preflightMaterializationNs := nonNegativeDurationNs(sample.preflightMaterialization)
	commandCallbackNs := nonNegativeDurationNs(sample.commandCallback)
	commandPublicPayloadEntryScanPreparationNs := nonNegativeDurationNs(sample.commandPublicPayloadEntryScanPreparation)
	commandPublishLockBarrierWaitNs := nonNegativeDurationNs(sample.commandPublishLockBarrierWait)
	commandBackendIntentPlanningSerializationNs := nonNegativeDurationNs(sample.commandBackendIntentPlanningSerialization)
	// Public preparation runs inside the backend planning interval: the backend
	// holds the command-WAL publish serialization while it invokes the public
	// preparation callback. Publish those two labels as an exclusive partition,
	// rather than counting that nested preparation twice.
	if commandPublicPayloadEntryScanPreparationNs <= commandBackendIntentPlanningSerializationNs {
		commandBackendIntentPlanningSerializationNs -= commandPublicPayloadEntryScanPreparationNs
	}
	commandExternalRefOrderingNs := nonNegativeDurationNs(sample.commandExternalRefOrdering)
	commandAppendNs := nonNegativeDurationNs(sample.commandAppend)
	commandFlushNs := nonNegativeDurationNs(sample.commandFlush)
	commandGroupCommitWaitNs := nonNegativeDurationNs(sample.commandGroupCommitWait)
	commandSyncNs := nonNegativeDurationNs(sample.commandSync)
	commandPostAppendPendingLSNBookkeepingNs := nonNegativeDurationNs(sample.commandPostAppendPendingLSNBookkeeping)
	commandEmptyBarrierNs := nonNegativeDurationNs(sample.commandEmptyBarrier)
	memtablePublicationResetNs := nonNegativeDurationNs(sample.memtablePublicationReset)

	topLevelKnownNs := checkpointGateNs + preflightMaterializationNs + commandCallbackNs + memtablePublicationResetNs
	residualNs := uint64(0)
	if topLevelKnownNs <= wallNs {
		residualNs = wallNs - topLevelKnownNs
	} else {
		s.topLevelPartitionOverruns.Add(1)
	}
	commandKnownNs := commandPublicPayloadEntryScanPreparationNs +
		commandPublishLockBarrierWaitNs +
		commandBackendIntentPlanningSerializationNs +
		commandExternalRefOrderingNs +
		commandAppendNs +
		commandFlushNs +
		commandGroupCommitWaitNs +
		commandSyncNs +
		commandPostAppendPendingLSNBookkeepingNs +
		commandEmptyBarrierNs
	commandOtherNs := uint64(0)
	if commandKnownNs <= commandCallbackNs {
		commandOtherNs = commandCallbackNs - commandKnownNs
	} else {
		s.commandPartitionOverruns.Add(1)
	}

	s.calls.Add(1)
	if err != nil {
		s.errors.Add(1)
	}
	s.wallNs.Add(wallNs)
	s.checkpointGateNs.Add(checkpointGateNs)
	s.preflightMaterializationNs.Add(preflightMaterializationNs)
	s.commandCallbackNs.Add(commandCallbackNs)
	if sample.commandPublicPreparationObserved {
		s.commandPublicPreparationCalls.Add(1)
	}
	s.commandPublicPayloadEntryScanPreparationNs.Add(commandPublicPayloadEntryScanPreparationNs)
	if sample.commandPublishLockBarrierWaitObserved {
		s.commandPublishLockBarrierWaitCalls.Add(1)
	}
	s.commandPublishLockBarrierWaitNs.Add(commandPublishLockBarrierWaitNs)
	if sample.commandBackendIntentPlanningObserved {
		s.commandBackendIntentPlanningCalls.Add(1)
	}
	s.commandBackendIntentPlanningSerializationNs.Add(commandBackendIntentPlanningSerializationNs)
	if sample.commandExternalRefOrderingObserved {
		s.commandExternalRefOrderingCalls.Add(1)
	}
	s.commandExternalRefOrderingNs.Add(commandExternalRefOrderingNs)
	if sample.commandAppendObserved {
		s.commandAppendCalls.Add(1)
	}
	s.commandAppendNs.Add(commandAppendNs)
	if sample.commandFlushObserved {
		s.commandFlushCalls.Add(1)
	}
	s.commandFlushNs.Add(commandFlushNs)
	if sample.commandGroupCommitWaitObserved {
		s.commandGroupCommitWaitCalls.Add(1)
	}
	s.commandGroupCommitWaitNs.Add(commandGroupCommitWaitNs)
	if sample.commandSyncObserved {
		s.commandSyncCalls.Add(1)
	}
	s.commandSyncNs.Add(commandSyncNs)
	if sample.commandPostAppendPendingLSNBookkeepingObserved {
		s.commandPostAppendPendingLSNBookkeepingCalls.Add(1)
	}
	s.commandPostAppendPendingLSNBookkeepingNs.Add(commandPostAppendPendingLSNBookkeepingNs)
	if sample.commandEmptyBarrierObserved {
		s.commandEmptyBarrierCalls.Add(1)
	}
	s.commandEmptyBarrierNs.Add(commandEmptyBarrierNs)
	s.commandOtherNs.Add(commandOtherNs)
	s.memtablePublicationResetNs.Add(memtablePublicationResetNs)
	s.residualNs.Add(residualNs)
}

func publicBatchWriteSyncPhaseStatsInto(stats map[string]string, enabled bool, s *publicBatchWriteSyncPhaseStats) {
	if stats == nil || s == nil {
		return
	}
	prefix := "treedb.public.batch.write_sync.phase."
	stats[prefix+"enabled"] = strconv.FormatBool(enabled)
	stats[prefix+"scope"] = "command_wal_public_batch_write_sync"
	stats[prefix+"top_level_partition"] = "checkpoint_gate+preflight_materialization+command_callback+memtable_publication_reset+residual"
	stats[prefix+"command_callback_partition"] = "command_public_payload_entry_scan_preparation+command_publish_lock_barrier_wait+command_backend_intent_planning_serialization+command_external_ref_ordering+command_append+command_flush+command_group_commit_wait+command_sync+command_post_append_pending_lsn_bookkeeping+command_empty_barrier+command_other"
	stats[prefix+"calls_total"] = fmt.Sprintf("%d", s.calls.Load())
	stats[prefix+"errors_total"] = fmt.Sprintf("%d", s.errors.Load())
	stats[prefix+"wall.ns_total"] = fmt.Sprintf("%d", s.wallNs.Load())
	stats[prefix+"checkpoint_gate.ns_total"] = fmt.Sprintf("%d", s.checkpointGateNs.Load())
	stats[prefix+"preflight_materialization.ns_total"] = fmt.Sprintf("%d", s.preflightMaterializationNs.Load())
	stats[prefix+"command_callback.ns_total"] = fmt.Sprintf("%d", s.commandCallbackNs.Load())
	stats[prefix+"command_public_payload_entry_scan_preparation.calls_total"] = fmt.Sprintf("%d", s.commandPublicPreparationCalls.Load())
	stats[prefix+"command_public_payload_entry_scan_preparation.ns_total"] = fmt.Sprintf("%d", s.commandPublicPayloadEntryScanPreparationNs.Load())
	stats[prefix+"command_publish_lock_barrier_wait.calls_total"] = fmt.Sprintf("%d", s.commandPublishLockBarrierWaitCalls.Load())
	stats[prefix+"command_publish_lock_barrier_wait.ns_total"] = fmt.Sprintf("%d", s.commandPublishLockBarrierWaitNs.Load())
	stats[prefix+"command_backend_intent_planning_serialization.calls_total"] = fmt.Sprintf("%d", s.commandBackendIntentPlanningCalls.Load())
	stats[prefix+"command_backend_intent_planning_serialization.ns_total"] = fmt.Sprintf("%d", s.commandBackendIntentPlanningSerializationNs.Load())
	stats[prefix+"command_external_ref_ordering.calls_total"] = fmt.Sprintf("%d", s.commandExternalRefOrderingCalls.Load())
	stats[prefix+"command_external_ref_ordering.ns_total"] = fmt.Sprintf("%d", s.commandExternalRefOrderingNs.Load())
	stats[prefix+"command_append.calls_total"] = fmt.Sprintf("%d", s.commandAppendCalls.Load())
	stats[prefix+"command_append.ns_total"] = fmt.Sprintf("%d", s.commandAppendNs.Load())
	stats[prefix+"command_flush.calls_total"] = fmt.Sprintf("%d", s.commandFlushCalls.Load())
	stats[prefix+"command_flush.ns_total"] = fmt.Sprintf("%d", s.commandFlushNs.Load())
	stats[prefix+"command_group_commit_wait.calls_total"] = fmt.Sprintf("%d", s.commandGroupCommitWaitCalls.Load())
	stats[prefix+"command_group_commit_wait.ns_total"] = fmt.Sprintf("%d", s.commandGroupCommitWaitNs.Load())
	stats[prefix+"command_sync.calls_total"] = fmt.Sprintf("%d", s.commandSyncCalls.Load())
	stats[prefix+"command_sync.ns_total"] = fmt.Sprintf("%d", s.commandSyncNs.Load())
	stats[prefix+"command_post_append_pending_lsn_bookkeeping.calls_total"] = fmt.Sprintf("%d", s.commandPostAppendPendingLSNBookkeepingCalls.Load())
	stats[prefix+"command_post_append_pending_lsn_bookkeeping.ns_total"] = fmt.Sprintf("%d", s.commandPostAppendPendingLSNBookkeepingNs.Load())
	stats[prefix+"command_empty_barrier.calls_total"] = fmt.Sprintf("%d", s.commandEmptyBarrierCalls.Load())
	stats[prefix+"command_empty_barrier.ns_total"] = fmt.Sprintf("%d", s.commandEmptyBarrierNs.Load())
	stats[prefix+"command_other.ns_total"] = fmt.Sprintf("%d", s.commandOtherNs.Load())
	stats[prefix+"memtable_publication_reset.ns_total"] = fmt.Sprintf("%d", s.memtablePublicationResetNs.Load())
	stats[prefix+"residual.ns_total"] = fmt.Sprintf("%d", s.residualNs.Load())
	stats[prefix+"top_level_partition_overruns_total"] = fmt.Sprintf("%d", s.topLevelPartitionOverruns.Load())
	stats[prefix+"command_partition_overruns_total"] = fmt.Sprintf("%d", s.commandPartitionOverruns.Load())
}

func writePathFromOptions(opts Options) writePathInfo {
	info := writePathInfo{
		mode:       "cached",
		valueStore: "value_log",
		redoLog:    "on",
	}
	if opts.ReadOnly {
		info.mode = "readonly"
	}
	if opts.CommandWAL && !opts.ReadOnly {
		info.mode = "command_wal_cached"
		info.redoLog = "command_wal"
	}
	if opts.Durability == db.DurabilityWALOffRelaxed {
		info.redoLog = "off"
	}
	return info
}

func writePathStatsInto(stats map[string]string, info writePathInfo) {
	if stats == nil {
		return
	}
	stats["treedb.write_path.mode"] = info.mode
	stats["treedb.write_path.value_store"] = info.valueStore
	stats["treedb.write_path.redo_log"] = info.redoLog
}

func forceTemplateCompressionOff(opts *Options) {
	if opts == nil {
		return
	}
	// Template compression remains experimental; keep runtime paths disabled so
	// normal TreeDB opens only exercise dict/block/off value-log compression.
	opts.ValueLog.TemplateMode = template.TemplateOff
	opts.ValueLog.TemplateReadStrict = false
	opts.ValueLog.TemplateLookup = nil
	opts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
}

func (db *DB) ensureOpen() error {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return ErrClosed
	}
	return nil
}

func (db *DB) beginPublicOperation() error {
	if db == nil {
		return ErrClosed
	}
	db.lifecycleMu.RLock()
	if db.cached == nil && db.backend == nil {
		db.lifecycleMu.RUnlock()
		return ErrClosed
	}
	// Cached mutations can otherwise be admitted without entering the backend,
	// even after an outcome-ambiguous root publication has poisoned it. Consult
	// the backend poison gate while the lifecycle read lock keeps both layers
	// stable; every caller of beginPublicOperation is a write, transaction,
	// batch, or durability boundary.
	if db.backend != nil {
		if err := db.backend.CheckCommandWALPublishReady(); err != nil {
			db.lifecycleMu.RUnlock()
			return err
		}
	}
	return nil
}

func (db *DB) beginFullScanMaintenance(op string) (time.Duration, func(success bool)) {
	wait, finish, _ := db.beginFullScanMaintenanceContext(context.Background(), op)
	return wait, finish
}

func lockFullScanMaintenanceContext(ctx context.Context, mu *sync.Mutex) error {
	if ctx.Done() == nil {
		mu.Lock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if mu.TryLock() {
			if err := ctx.Err(); err != nil {
				mu.Unlock()
				return err
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (db *DB) beginFullScanMaintenanceContext(ctx context.Context, op string) (time.Duration, func(success bool), error) {
	if db == nil {
		return 0, func(bool) {}, nil
	}
	wait := time.Duration(0)
	if err := ctx.Err(); err != nil {
		return wait, func(bool) {}, err
	}
	if db.maintenance.mu.TryLock() {
		if err := ctx.Err(); err != nil {
			db.maintenance.mu.Unlock()
			return wait, func(bool) {}, err
		}
	} else {
		waitStart := time.Now()
		err := lockFullScanMaintenanceContext(ctx, &db.maintenance.mu)
		wait = time.Since(waitStart)
		db.maintenance.deferrals.Add(1)
		db.maintenance.waitTotal.Add(wait.Nanoseconds())
		atomicStoreMaxInt64(&db.maintenance.waitMax, wait.Nanoseconds())
		if err != nil {
			return wait, func(bool) {}, err
		}
	}
	db.maintenance.active.Store(maintenanceOpCode(op))

	return wait, func(success bool) {
		if success {
			now := time.Now().UnixNano()
			switch op {
			case "gc":
				db.maintenance.gcRuns.Add(1)
				db.maintenance.lastGCAt.Store(now)
			case "leaf-gc":
				db.maintenance.leafGCRuns.Add(1)
				db.maintenance.lastLeafGCAt.Store(now)
			case "vacuum":
				db.maintenance.vacuumRuns.Add(1)
				db.maintenance.lastVacuumAt.Store(now)
			}
		}
		db.maintenance.active.Store(maintenanceOpNone)
		db.maintenance.mu.Unlock()
	}, nil
}

func maintenanceStatsInto(stats map[string]string, m *maintenanceCoordinator) {
	if stats == nil || m == nil {
		return
	}
	active := maintenanceActiveLabel(m.active.Load())
	deferrals := m.deferrals.Load()
	waitTotal := time.Duration(m.waitTotal.Load())
	waitMax := time.Duration(m.waitMax.Load())
	gcRuns := m.gcRuns.Load()
	leafGCRuns := m.leafGCRuns.Load()
	vacuumRuns := m.vacuumRuns.Load()
	lastGCAt := m.lastGCAt.Load()
	lastLeafGCAt := m.lastLeafGCAt.Load()
	lastVacuumAt := m.lastVacuumAt.Load()

	stats["treedb.maintenance.full_scan.active"] = active
	stats["treedb.maintenance.full_scan.deferrals"] = fmt.Sprintf("%d", deferrals)
	stats["treedb.maintenance.full_scan.wait_total_ms"] = fmt.Sprintf("%.3f", float64(waitTotal)/float64(time.Millisecond))
	stats["treedb.maintenance.full_scan.wait_max_ms"] = fmt.Sprintf("%.3f", float64(waitMax)/float64(time.Millisecond))
	stats["treedb.maintenance.full_scan.gc_runs"] = fmt.Sprintf("%d", gcRuns)
	stats["treedb.maintenance.full_scan.leaf_gc_runs"] = fmt.Sprintf("%d", leafGCRuns)
	stats["treedb.maintenance.full_scan.vacuum_runs"] = fmt.Sprintf("%d", vacuumRuns)
	if lastGCAt > 0 {
		stats["treedb.maintenance.full_scan.last_gc_unix_nano"] = fmt.Sprintf("%d", lastGCAt)
	} else {
		stats["treedb.maintenance.full_scan.last_gc_unix_nano"] = "0"
	}
	if lastLeafGCAt > 0 {
		stats["treedb.maintenance.full_scan.last_leaf_gc_unix_nano"] = fmt.Sprintf("%d", lastLeafGCAt)
	} else {
		stats["treedb.maintenance.full_scan.last_leaf_gc_unix_nano"] = "0"
	}
	if lastVacuumAt > 0 {
		stats["treedb.maintenance.full_scan.last_vacuum_unix_nano"] = fmt.Sprintf("%d", lastVacuumAt)
	} else {
		stats["treedb.maintenance.full_scan.last_vacuum_unix_nano"] = "0"
	}
}

func normalizeBackpressureDefaults(opts *Options) {
	if opts == nil {
		return
	}
	if opts.SlowdownBacklogSeconds < 0 {
		opts.SlowdownBacklogSeconds = 0
	}
	if opts.StopBacklogSeconds < 0 {
		opts.StopBacklogSeconds = 0
	}
	if opts.MaxBacklogBytes < 0 {
		opts.MaxBacklogBytes = 0
	}
	if opts.SlowdownBacklogSeconds != 0 || opts.StopBacklogSeconds != 0 || opts.MaxBacklogBytes != 0 {
		return
	}

	slowdown := defaultSlowdownBacklogSeconds
	stop := defaultStopBacklogSeconds

	opts.SlowdownBacklogSeconds = slowdown
	opts.StopBacklogSeconds = stop
	opts.MaxBacklogBytes = defaultAdaptiveMaxBacklogBytes
}

// Open opens TreeDB. By default it enables caching (write-back layer).
func Open(opts Options) (*DB, error) {
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return nil, err
	}
	return openResolved(opts)
}

func openResolved(opts Options) (*DB, error) {
	// Cached mode writes to the backend in large flush batches, so commit sequence
	// advances much more slowly than "number of writes". A large KeepRecent value
	// can therefore delay page reuse for a very long time (and cause index.db to
	// balloon under update-heavy workloads). Default to aggressive reuse in cached
	// mode unless the caller specifies otherwise.
	chunkSizeDefaulted := opts.ChunkSize == 0
	if chunkSizeDefaulted {
		opts.ChunkSize = defaultChunkSize
	}
	dictChunkSize := opts.DictDBChunkSize
	if dictChunkSize <= 0 {
		dictChunkSize = defaultDictChunkSize
	}
	templateChunkSize := opts.TemplateDBChunkSize
	if templateChunkSize <= 0 {
		templateChunkSize = defaultTemplateChunkSize
	}
	if opts.KeepRecent == 0 && !opts.ReadOnly {
		opts.KeepRecent = 1
	}
	if opts.ValueLog.Compression == 0 {
		opts.ValueLog.Compression = db.ValueLogCompressionAuto
	}

	applyEnvMaintenanceOverrides(&opts)
	forceTemplateCompressionOff(&opts)

	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return nil, err
	}
	rootDir := layout.rootDir
	maindbDir := layout.mainDir
	dictdbDir := layout.dictdbDir
	templatedbDir := layout.templatedbDir

	// Apply persisted index encoding knobs so tools and apps can open existing
	// dirs without needing to re-specify format-affecting flags.
	//
	// This is intentionally limited to index encoding flags; runtime policies
	// (like value-log compression) remain controlled by opts/env unless the
	// caller opts out via IgnoreFormatConfig.
	var persistedFormat *db.FormatConfig
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := db.CommandWALRequiredFeatureEnabled(maindbDir)
		if err != nil {
			return nil, err
		}
		opts.CommandWAL = opts.CommandWAL || requiresCommandWAL
	} else {
		if cfg, ok, err := db.LoadFormatConfig(maindbDir); err != nil {
			return nil, err
		} else if ok {
			opts.CommandWAL = opts.CommandWAL || cfg.RequiresCommandWALV1()
			cfg.ApplyIndexFormatToOptions(&opts)
			persistedFormat = &cfg
		}
	}
	// Apply runtime-only index/cache overrides after loading persisted format.json
	// so downstream apps can toggle safe behavior without plumbing new CLI flags.
	//
	// Index format knobs (leaf encoding, outer leaf refs, etc) are persisted per-DB
	// in format.json; allowing env overrides to conflict with on-disk format can
	// corrupt format.json and/or make existing pages unreadable. For safety, reject
	// conflicting format overrides when format.json is present.
	applyEnvIndexRuntimeOverrides(&opts)
	if persistedFormat != nil {
		if err := validateEnvIndexFormatOverrides(*persistedFormat, maindbDir); err != nil {
			return nil, err
		}
	} else {
		applyEnvIndexFormatOverrides(&opts)
	}

	// Keep opts.DisableSideStores consistent with the resolved layout.
	opts.DisableSideStores = layout.disableSideStores
	db.NormalizeFlushAdmissionOptions(&opts)

	writePath := writePathFromOptions(opts)
	if envBool(envWritePathLog) {
		effectivePolicy := opts.ValueLog.Generational.Policy
		if effectivePolicy == ValueLogGenerationDefault {
			effectivePolicy = ValueLogGenerationHotWarmCold
		}
		fmt.Fprintf(
			os.Stderr,
			"treedb write_path mode=%s value_store=%s redo_log=%s vlog_generation_policy_raw=%d vlog_generation_policy_effective=%d\n",
			writePath.mode,
			writePath.valueStore,
			writePath.redoLog,
			opts.ValueLog.Generational.Policy,
			effectivePolicy,
		)
	}

	// Dict compression requires a persistent dict store so dictionaries can be
	// published and older dict-compressed frames remain decodable.
	//
	// DisableSideStores removes dictdb/templatedb plumbing. When it is enabled,
	// auto compression automatically runs in a no-dict configuration by
	// disabling dictionary training so block/off decisions can continue to work.
	if opts.DisableSideStores {
		switch opts.ValueLog.Compression {
		case ValueLogCompressionDict:
			return nil, fmt.Errorf("treedb: dict compression requires side stores (dictdb); set DisableSideStores=false")
		case ValueLogCompressionAuto:
			// Side stores disabled means we cannot publish dictionaries. In this
			// configuration auto mode behaves as "no-dict auto" (block/off only).
			if opts.ValueLog.DictTrain.TrainBytes >= 0 {
				train := opts.ValueLog.DictTrain
				train.TrainBytes = -1
				opts.ValueLog.DictTrain = train
			}
		}
	}

	if opts.ReadOnly {
		if _, err := os.Stat(maindbDir); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("treedb: maindb directory missing for read-only open: %s", maindbDir)
			}
			return nil, err
		}
		if !opts.DisableSideStores {
			if _, err := os.Stat(dictdbDir); err != nil {
				if os.IsNotExist(err) {
					return nil, fmt.Errorf("treedb: dictdb directory missing for read-only open: %s", dictdbDir)
				}
				return nil, err
			}
			if opts.ValueLog.TemplateMode != template.TemplateOff {
				if _, err := os.Stat(templatedbDir); err != nil {
					if os.IsNotExist(err) {
						return nil, fmt.Errorf("treedb: templatedb directory missing for read-only open: %s", templatedbDir)
					}
					return nil, err
				}
			}
		}
	} else {
		dirs := []string{rootDir}
		if maindbDir != rootDir {
			dirs = append(dirs, maindbDir)
		}
		if !opts.DisableSideStores {
			dirs = append(dirs, dictdbDir)
			if opts.ValueLog.TemplateMode != template.TemplateOff {
				dirs = append(dirs, templatedbDir)
			}
		}
		if err := ensureOpenStorageLayoutDirs(0o755, filepath.Join(maindbDir, "index.db"), dirs...); err != nil {
			return nil, fmt.Errorf("treedb: ensure public storage layout: %w", err)
		}
	}

	var dictBackend *db.DB
	var dictStore *dictdb.Store
	var templateDB *DB
	var templateStore *templatedb.Store
	if !opts.DisableSideStores {
		dictOpts := opts
		dictOpts.Dir = dictdbDir
		dictOpts.ResolvedProfile = ""
		dictOpts.DeprecatedProfileAlias = ""
		dictOpts.UnsafeBenchmarkProfile = false
		dictOpts.CommandWAL = false
		// Side stores are opened via the backend (no caching layer). They must
		// not inherit outer-leaf-in-value-log from the main DB, since that mode
		// requires a leaf-page log wired by the cached layer.
		dictOpts.IndexOuterLeavesInValueLog = false
		dictOpts.DisableBackgroundPrune = true
		dictOpts.ValueLog.DictLookup = nil
		dictOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// dictdb stores small metadata values (e.g. current dict id, hash->id map)
		// inline. ForcePointers would set InlineThreshold=0 and break these writes.
		dictOpts.ValueLog.ForcePointers = false
		// Avoid inheriting pointer/domain placement rules from the main DB. Side
		// stores keep small values inline by default.
		dictOpts.ValueLog.PointerThreshold = 0
		dictOpts.ValueLog.DomainInlineThresholds = nil
		dictOpts.ValueLog.Compression = db.ValueLogCompressionOff
		dictOpts.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
		dictOpts.ChunkSize = dictChunkSize
		var err error
		dictBackend, err = db.Open(dictOpts)
		if err != nil {
			return nil, err
		}
		dictStore = dictdb.New(dictBackend)

		opts.ValueLog.DictLookup = func(dictID uint64) ([]byte, error) {
			return dictStore.GetDictBytes(context.Background(), dictID)
		}
		opts.ValueLog.DictCurrentForClass = func(ctx context.Context, class string) (uint64, error) {
			return dictStore.GetCurrentForClass(ctx, class)
		}
		opts.ValueLog.DictLeafPayloadMode = func(ctx context.Context, dictID uint64) (bool, bool, error) {
			return dictStore.GetLeafPayloadMode(ctx, dictID)
		}
		if !opts.ReadOnly {
			opts.ValueLog.DictPut = func(ctx context.Context, dictBytes []byte) (uint64, error) {
				return dictStore.PutDictBytes(ctx, dictBytes)
			}
			opts.ValueLog.DictSetCurrentForClass = func(ctx context.Context, class string, dictID uint64) error {
				return dictStore.SetCurrentForClass(ctx, class, dictID)
			}
			opts.ValueLog.DictSetLeafPayloadMode = func(ctx context.Context, dictID uint64, useRawPages bool) error {
				return dictStore.SetLeafPayloadMode(ctx, dictID, useRawPages)
			}
		}
	}

	if !opts.DisableSideStores && opts.ValueLog.TemplateMode != template.TemplateOff {
		templateOpts := opts
		templateOpts.Dir = templatedbDir
		templateOpts.ResolvedProfile = ""
		templateOpts.DeprecatedProfileAlias = ""
		templateOpts.UnsafeBenchmarkProfile = false
		templateOpts.CommandWAL = false
		templateOpts.DisableSideStores = true
		templateOpts.DisableBackgroundPrune = true
		// Like dictdb, templatedb is an internal side store; avoid inheriting the
		// main DB's outer-leaf value-log layout (not needed here, and it adds
		// unnecessary value-log churn).
		templateOpts.IndexOuterLeavesInValueLog = false
		templateOpts.ValueLog.DictLookup = nil
		templateOpts.ValueLog.DictTrain = TrainConfig{TrainBytes: -1}
		// templatedb uses batch.Set for small routing/index entries. Do not
		// propagate ForcePointers from the main DB into this internal store.
		templateOpts.ValueLog.ForcePointers = false
		templateOpts.ValueLog.PointerThreshold = 0
		templateOpts.ValueLog.DomainInlineThresholds = nil
		templateOpts.ValueLog.Compression = db.ValueLogCompressionOff
		templateOpts.ValueLog.CompressionAutotune = AutotuneOptions{Mode: AutotuneOff}
		templateOpts.ValueLog.TemplateMode = template.TemplateOff
		templateOpts.ValueLog.TemplateLookup = nil
		templateOpts.ValueLog.TemplateDecodeOptions = template.DecodeOptions{}
		templateOpts.ChunkSize = templateChunkSize

		var err error
		templateDB, err = openResolved(templateOpts)
		if err != nil {
			if dictBackend != nil {
				_ = dictBackend.Close()
			}
			return nil, err
		}

		tcfg := template.NormalizeConfig(opts.ValueLog.TemplateConfig)
		templateStore = templatedb.New(templateKV{db: templateDB}, templatedb.Config{
			MaxCandidatesPerFP:    tcfg.MaxCandidatesPerFP,
			MaxCandidateListBytes: tcfg.MaxCandidateListBytes,
		})
		decodeOpts := template.DecodeOptions{MaxGaps: tcfg.MaxGaps, MaxDecodedBytes: tcfg.MaxDecodedBytes, DefCacheSize: tcfg.DefCacheSize}
		if decodeOpts.MaxDecodedBytes <= 0 && limits.MaxRecordSize > 0 {
			decodeOpts.MaxDecodedBytes = int(limits.MaxRecordSize)
		}
		opts.ValueLog.TemplateLookup = func(templateID uint64) ([]byte, error) {
			return templateStore.GetTemplateDef(context.Background(), templateID)
		}
		opts.ValueLog.TemplateDecodeOptions = decodeOpts
	}
	opts.Dir = maindbDir
	backend, err := db.Open(opts)
	if err != nil {
		if dictBackend != nil {
			_ = dictBackend.Close()
		}
		if templateDB != nil {
			_ = templateDB.Close()
		}
		return nil, err
	}

	if opts.ReadOnly {
		return &DB{backend: backend, dictdb: dictBackend, templateDB: templateDB, writePath: writePath, notifyError: opts.NotifyError, resolvedProfile: Profile(opts.ResolvedProfile), deprecatedProfileAlias: Profile(opts.DeprecatedProfileAlias), durabilityMode: computeDurabilityMode(opts), valueLogReadIntegrity: valueLogReadIntegrityLabel(opts), dir: rootDir}, nil
	}

	normalizeBackpressureDefaults(&opts)
	if opts.MemtableMode == "" {
		opts.MemtableMode = "adaptive"
	}

	disableWAL := opts.Durability == db.DurabilityWALOffRelaxed
	relaxedSync := opts.Durability != db.DurabilityDurable
	disableReadChecksum := opts.ValueLog.ReadIntegrity == db.IntegritySkipChecksums
	allowUnsafe := disableWAL || relaxedSync || disableReadChecksum
	valueLogMaxSegmentBytes := int64(0)
	if opts.IndexPackedValuePtr || opts.IndexOuterLeavesInValueLog {
		valueLogMaxSegmentBytes = int64(^uint32(0)) - 4
	}

	cached, err := caching.Open(opts.Dir, backend, caching.Options{
		FlushThreshold:                             opts.FlushThreshold,
		MemtableMode:                               opts.MemtableMode,
		MemtableShards:                             opts.MemtableShards,
		DomainIngressWorkers:                       opts.DomainIngressWorkers,
		DomainIngressQueueSize:                     opts.DomainIngressQueueSize,
		MaxQueuedMemtables:                         opts.MaxQueuedMemtables,
		SlowdownBacklogSeconds:                     opts.SlowdownBacklogSeconds,
		StopBacklogSeconds:                         opts.StopBacklogSeconds,
		MaxBacklogBytes:                            opts.MaxBacklogBytes,
		WriterFlushMaxMemtables:                    opts.WriterFlushMaxMemtables,
		WriterFlushMaxDuration:                     opts.WriterFlushMaxDuration,
		FlushBuildConcurrency:                      opts.FlushBuildConcurrency,
		FlushBuildMinEntries:                       opts.FlushBuildMinEntries,
		FlushBuildMinUnits:                         opts.FlushBuildMinUnits,
		FlushBuildChunkCap:                         opts.FlushBuildChunkCap,
		FlushBuildChunkTargetBytes:                 opts.FlushBuildChunkTargetBytes,
		FlushBuildChunkMinBytes:                    opts.FlushBuildChunkMinBytes,
		FlushBuildChunkMaxBytes:                    opts.FlushBuildChunkMaxBytes,
		FlushBuildPrefetchUnits:                    opts.FlushBuildPrefetchUnits,
		FlushApplyConcurrency:                      opts.FlushApplyConcurrency,
		FlushApplyMinEntries:                       opts.FlushApplyMinEntries,
		FlushApplyMinSpans:                         opts.FlushApplyMinSpans,
		FlushApplyMinBytes:                         opts.FlushApplyMinBytes,
		FlushApplySpanNative:                       opts.FlushApplySpanNative,
		FlushBackendMaxEntries:                     opts.FlushBackendMaxEntries,
		FlushBackendMaxBatches:                     opts.FlushBackendMaxBatches,
		FlushSpanRunTargetPlanning:                 opts.FlushSpanRunTargetPlanning,
		FlushBacklogCoalescing:                     opts.FlushBacklogCoalescing,
		FlushBacklogCoalescingMaxMemtables:         opts.FlushBacklogCoalescingMaxMemtables,
		FlushBacklogCoalescingMaxBytes:             opts.FlushBacklogCoalescingMaxBytes,
		FlushBacklogCoalescingMaxOps:               opts.FlushBacklogCoalescingMaxOps,
		FlushBacklogCoalescingMinAge:               opts.FlushBacklogCoalescingMinAge,
		FlushBacklogCoalescingSingleOpSpanRatio:    opts.FlushBacklogCoalescingSingleOpSpanRatio,
		FlushBacklogCoalescingMaxOpsPerSpan:        opts.FlushBacklogCoalescingMaxOpsPerSpan,
		FlushBacklogCoalescingMinOldLeafBytesPerOp: opts.FlushBacklogCoalescingMinOldLeafBytesPerOp,
		DisableWAL:                                 disableWAL,
		ExternalCommandWAL:                         opts.CommandWAL,
		JournalLanes:                               opts.JournalLanes,
		WALMaxSegmentBytes:                         opts.WALMaxSegmentBytes,
		JournalCompression:                         opts.JournalCompression,
		RelaxedSync:                                relaxedSync,
		DisableReadChecksum:                        disableReadChecksum,
		ValueLogPointerThreshold:                   opts.ValueLog.PointerThreshold,
		IndexOuterLeavesInValueLog:                 opts.IndexOuterLeavesInValueLog,
		ValueLogDomainInlineThresholds:             opts.ValueLog.DomainInlineThresholds,
		ValueLogRawWritevMinAvgBytes:               opts.ValueLog.RawWritevMinAvgBytes,
		ValueLogRawWritevMinBatchRecords:           opts.ValueLog.RawWritevMinBatchRecords,
		ValueLogMaxSegmentBytes:                    valueLogMaxSegmentBytes,
		ValueLogCompression:                        uint8(opts.ValueLog.Compression),
		ValueLogBlockCodec:                         uint8(opts.ValueLog.BlockCodec),
		ValueLogBlockTargetCompressedBytes:         opts.ValueLog.BlockTargetCompressedBytes,
		ValueLogIncompressibleHoldBytes:            opts.ValueLog.IncompressibleHoldBytes,
		ValueLogIncompressibleProbeBytes:           opts.ValueLog.IncompressibleProbeIntervalBytes,
		ValueLogAutoPolicy:                         uint8(opts.ValueLog.AutoPolicy),
		ValueLogGenerationPolicy:                   uint8(opts.ValueLog.Generational.Policy),
		ValueLogGenerationLeafSegmentTargetBytes:   opts.ValueLog.Generational.LeafSegmentTargetBytes,
		ValueLogGenerationHotSegmentTargetBytes:    opts.ValueLog.Generational.HotSegmentTargetBytes,
		ValueLogGenerationWarmSegmentTargetBytes:   opts.ValueLog.Generational.WarmSegmentTargetBytes,
		ValueLogGenerationColdSegmentTargetBytes:   opts.ValueLog.Generational.ColdSegmentTargetBytes,
		ValueLogRewriteBudgetBytesPerSec:           opts.ValueLog.Generational.RewriteBudgetBytesPerSec,
		ValueLogRewriteBudgetRecordsPerSec:         opts.ValueLog.Generational.RewriteBudgetRecordsPerSec,
		ValueLogRewriteTriggerStaleRatioPPM:        opts.ValueLog.Generational.RewriteTriggerStaleRatioPPM,
		ValueLogRewriteTriggerTotalBytes:           opts.ValueLog.Generational.RewriteTriggerTotalBytes,
		ValueLogRewriteTriggerChurnPerSec:          opts.ValueLog.Generational.RewriteTriggerChurnPerSec,
		ValueLogRewriteMinSegmentAge:               opts.ValueLog.Generational.RewriteMinSegmentAge,
		ForceValueLogPointers:                      opts.ValueLog.ForcePointers,
		ValueLogCurrentWritableMmap:                opts.ValueLog.CurrentWritableMmap,
		ValueLogDictTrain:                          opts.ValueLog.DictTrain,
		ValueLogDictMaxK:                           opts.ValueLog.DictMaxK,
		ValueLogDictClassMode:                      uint8(opts.ValueLog.DictClassMode),
		ValueLogDictFrameEncodeLevel:               opts.ValueLog.DictFrameEncodeLevel,
		ValueLogDictFrameEnableEntropy:             opts.ValueLog.DictFrameEnableEntropy,
		ValueLogDictAdaptiveRatio:                  opts.ValueLog.DictAdaptiveRatio,
		ValueLogDictMetricsWindowBytes:             opts.ValueLog.DictMetricsWindowBytes,
		ValueLogDictMetricsMinRecords:              opts.ValueLog.DictMetricsMinRecords,
		ValueLogDictMetricsPauseBytes:              opts.ValueLog.DictMetricsPauseBytes,
		ValueLogDictIncompressibleHoldBytes:        opts.ValueLog.DictIncompressibleHoldBytes,
		ValueLogDictProbeIntervalBytes:             opts.ValueLog.DictProbeIntervalBytes,
		ValueLogDictMinPayloadSavingsRatio:         opts.ValueLog.DictMinPayloadSavingsRatio,
		ValueLogCompressionAutotune:                opts.ValueLog.CompressionAutotune,
		ValueLogTemplateMode:                       opts.ValueLog.TemplateMode,
		ValueLogTemplateConfig:                     opts.ValueLog.TemplateConfig,
		ValueLogTemplateReadStrict:                 opts.ValueLog.TemplateReadStrict,
		AllowUnsafe:                                allowUnsafe,
		MaxValueLogRetainedBytes:                   opts.ValueLog.MaxRetainedBytes,
		MaxValueLogRetainedBytesHard:               opts.ValueLog.MaxRetainedBytesHard,
		NotifyError:                                opts.NotifyError,
	})
	if err != nil {
		_ = backend.Close()
		if dictBackend != nil {
			_ = dictBackend.Close()
		}
		if templateDB != nil {
			_ = templateDB.Close()
		}
		return nil, err
	}

	cached.SetDictStore(dictStore)
	cached.SetTemplateStore(templateStore)
	out := &DB{cached: cached, backend: backend, dictdb: dictBackend, templateDB: templateDB, writePath: writePath, commandWALCached: opts.CommandWAL, publicBatchWriteSyncPhaseEnabled: opts.CommandWAL && opts.PublicBatchWriteSyncPhaseStats, notifyError: opts.NotifyError, resolvedProfile: Profile(opts.ResolvedProfile), deprecatedProfileAlias: Profile(opts.DeprecatedProfileAlias), durabilityMode: computeDurabilityMode(opts), valueLogReadIntegrity: valueLogReadIntegrityLabel(opts), dir: rootDir}
	if out.commandWALCached {
		cached.SetCommandWALCheckpointCutoverHook(out.snapshotPublicCommandWALCheckpointCutover)
		cached.SetCommandWALCheckpointPublishHook(out.preparePublicCommandWALPendingPublish)
		cached.SetCommandWALCheckpointCleanupHook(out.cleanupPublicCommandWALCheckpoint)
		cached.SetAutoCheckpointWALBytesHook(out.publicCommandWALAutoCheckpointBytes)
	}

	// Cached-mode auto checkpointing is enabled by default to keep `wal/` growth
	// bounded for long-running workloads, aligning operational expectations with
	// typical LSM engines (log segments do not grow without bound).
	autoInterval := opts.BackgroundCheckpointInterval
	if autoInterval == 0 {
		autoInterval = 30 * time.Second
	}
	if autoInterval < 0 {
		autoInterval = 0
	}
	maxWALBytes := opts.MaxWALBytes
	if maxWALBytes == 0 {
		maxWALBytes = 2 << 30 // 2GiB
	}
	if maxWALBytes < 0 {
		maxWALBytes = 0
	}
	idleInterval := opts.BackgroundCheckpointIdleDuration
	if idleInterval == 0 {
		idleInterval = 2 * time.Second
	}
	if idleInterval < 0 {
		idleInterval = 0
	}
	// Auto checkpointing manages cached redo WAL bytes, and in command-WAL mode
	// uses the command-WAL active segment byte hook installed above.
	if (!disableWAL || out.commandWALCached) && (autoInterval > 0 || maxWALBytes > 0 || idleInterval > 0) {
		cached.StartAutoCheckpoint(autoInterval, maxWALBytes, idleInterval)
	}

	vacuumInterval := opts.BackgroundIndexVacuumInterval
	if vacuumInterval == 0 {
		vacuumInterval = 30 * time.Second
	}
	if vacuumInterval < 0 {
		vacuumInterval = 0
	}
	if vacuumInterval > 0 && !opts.ReadOnly && runtime.GOOS != "windows" {
		out.bgVac.Start(out, bgIndexVacuumConfig{
			Interval:                    vacuumInterval,
			SpanRatioPPM:                opts.BackgroundIndexVacuumSpanRatioPPM,
			MaxBacklogSkips:             opts.BackgroundIndexVacuumMaxBacklogSkips,
			FreelistReclaimableRatioPPM: opts.BackgroundIndexVacuumFreelistReclaimableRatioPPM,
			FreelistReclaimablePages:    opts.BackgroundIndexVacuumFreelistReclaimablePages,
			CollectionRootSpanRatioPPM:  opts.BackgroundIndexVacuumCollectionRootSpanRatioPPM,
			CollectionRootPages:         opts.BackgroundIndexVacuumCollectionRootPages,
		})
	}
	cached.SetStatsHook(out.publicCachedExpvarStatsInto)

	return out, nil
}

func (db *DB) publicCachedExpvarStatsInto(stats map[string]string) {
	if db == nil {
		return
	}
	db.publicCommandWALBatchStatsInto(stats)
	db.publicCommandWALGroupStatsInto(stats)
	db.publicOperationStatsInto(stats)
	bgIndexVacuumStatsInto(stats, &db.bgVac)
	maintenanceStatsInto(stats, &db.maintenance)
}

const (
	envDisableBackgroundPrune         = "TREEDB_DISABLE_BACKGROUND_PRUNE"
	envDisableBackgroundIndexVacuum   = "TREEDB_DISABLE_BACKGROUND_INDEX_VACUUM"
	envDisableVlogGeneration          = "TREEDB_DISABLE_VLOG_GENERATION"
	envPublicBatchWriteSyncPhaseStats = "TREEDB_PUBLIC_BATCH_WRITE_SYNC_PHASE_STATS"

	// Value-log compression knobs (cached mode).
	//
	// These are env-driven so downstream apps can toggle behavior without
	// plumbing new CLI flags through multiple repos.
	envVlogCompression      = "TREEDB_VLOG_COMPRESSION"        // auto|dict|block|off
	envVlogAutoPolicy       = "TREEDB_VLOG_AUTO_POLICY"        // balanced|throughput|size
	envVlogBlockCodec       = "TREEDB_VLOG_BLOCK_CODEC"        // snappy|lz4|zstd
	envVlogBlockTargetBytes = "TREEDB_VLOG_BLOCK_TARGET_BYTES" // int (compressed target bytes)

	// Value-log dictionary compression knobs (cached mode).
	//
	// Enabling dict compression requires:
	//   - ValueLog compression mode that allows dicts (auto/dict), and
	//   - Dict training enabled (TrainBytes > 0), and
	//   - Side stores enabled (dictdb), and
	//   - Split value log enabled (value pointers used).
	envVlogDictEnable                  = "TREEDB_VLOG_DICT_ENABLE"                     // bool
	envVlogDictTrainBytes              = "TREEDB_VLOG_DICT_TRAIN_BYTES"                // int
	envVlogDictBytes                   = "TREEDB_VLOG_DICT_BYTES"                      // int
	envVlogDictMinRecords              = "TREEDB_VLOG_DICT_MIN_RECORDS"                // int
	envVlogDictMaxRecordBytes          = "TREEDB_VLOG_DICT_MAX_RECORD_BYTES"           // int
	envVlogDictSampleStride            = "TREEDB_VLOG_DICT_SAMPLE_STRIDE"              // int
	envVlogDictDedupWindow             = "TREEDB_VLOG_DICT_DEDUP_WINDOW"               // int
	envVlogDictTrainLevel              = "TREEDB_VLOG_DICT_TRAIN_LEVEL"                // int
	envVlogDictMaxK                    = "TREEDB_VLOG_DICT_MAX_K"                      // int
	envVlogDictClassMode               = "TREEDB_VLOG_DICT_CLASS_MODE"                 // single|split_outer_leaf
	envVlogDictZstdLevel               = "TREEDB_VLOG_DICT_ZSTD_LEVEL"                 // fastest|default|better|best|int
	envVlogDictEntropy                 = "TREEDB_VLOG_DICT_ENTROPY"                    // bool
	envVlogDictAdaptiveRatio           = "TREEDB_VLOG_DICT_ADAPTIVE_RATIO"             // float64
	envVlogDictMinPayloadSavings       = "TREEDB_VLOG_DICT_MIN_PAYLOAD_SAVINGS_RATIO"  // float64
	envVlogMaxRetainedBytes            = "TREEDB_VLOG_MAX_RETAINED_BYTES"              // int64
	envVlogMaxRetainedBytesHard        = "TREEDB_VLOG_MAX_RETAINED_BYTES_HARD"         // int64
	envVlogRewriteBudgetBytesPerSec    = "TREEDB_VLOG_REWRITE_BUDGET_BYTES_PER_SEC"    // int64
	envVlogRewriteBudgetRecordsPerSec  = "TREEDB_VLOG_REWRITE_BUDGET_RECORDS_PER_SEC"  // int
	envVlogRewriteTriggerTotalBytes    = "TREEDB_VLOG_REWRITE_TRIGGER_TOTAL_BYTES"     // int64
	envVlogRewriteTriggerStaleRatioPPM = "TREEDB_VLOG_REWRITE_TRIGGER_STALE_RATIO_PPM" // uint32
	envVlogRewriteTriggerChurnPerSec   = "TREEDB_VLOG_REWRITE_TRIGGER_CHURN_PER_SEC"   // int64
)

// Index/cache override knobs (applied on top of Options + profile defaults).
//
// These are intentionally env-driven to allow isolating correctness/perf issues
// in real workloads without plumbing new CLI flags through multiple repos.
const (
	envVerifyOnRead               = "TREEDB_VERIFY_ON_READ"                  // bool
	envPreferAppendAlloc          = "TREEDB_PREFER_APPEND_ALLOC"             // bool
	envDisablePiggybackCompaction = "TREEDB_DISABLE_PIGGYBACK_COMPACTION"    // bool
	envLeafPrefixCompression      = "TREEDB_LEAF_PREFIX_COMPRESSION"         // bool
	envIndexColumnarLeaves        = "TREEDB_INDEX_COLUMNAR_LEAVES"           // bool
	envIndexPackedValuePtr        = "TREEDB_INDEX_PACKED_VALUE_PTR"          // bool
	envIndexInternalBaseDelta     = "TREEDB_INDEX_INTERNAL_BASE_DELTA"       // bool
	envIndexOuterLeavesInValueLog = "TREEDB_INDEX_OUTER_LEAVES_IN_VALUE_LOG" // bool
	envIndexAdaptiveLeafEncoding  = "TREEDB_INDEX_ADAPTIVE_LEAF_ENCODING"    // bool
)

func applyEnvMaintenanceOverrides(opts *Options) {
	if opts == nil {
		return
	}
	if envBool(envDisableBackgroundPrune) {
		opts.DisableBackgroundPrune = true
	}
	if envBool(envDisableBackgroundIndexVacuum) {
		opts.BackgroundIndexVacuumInterval = -1
	}
	if envBool(envDisableVlogGeneration) {
		opts.ValueLog.Generational.Policy = ValueLogGenerationOff
	}
	if enabled, ok := envBoolSet(envPublicBatchWriteSyncPhaseStats); ok {
		opts.PublicBatchWriteSyncPhaseStats = enabled
	}

	if val, ok := envString(envVlogCompression); ok {
		switch strings.ToLower(val) {
		case "auto":
			opts.ValueLog.Compression = ValueLogCompressionAuto
		case "dict":
			opts.ValueLog.Compression = ValueLogCompressionDict
		case "block":
			opts.ValueLog.Compression = ValueLogCompressionBlock
		case "off", "none", "0":
			opts.ValueLog.Compression = ValueLogCompressionOff
		}
	}
	if val, ok := envString(envVlogAutoPolicy); ok {
		switch strings.ToLower(val) {
		case "balanced":
			opts.ValueLog.AutoPolicy = ValueLogAutoBalanced
		case "throughput":
			opts.ValueLog.AutoPolicy = ValueLogAutoThroughput
		case "size":
			opts.ValueLog.AutoPolicy = ValueLogAutoSize
		}
	}
	if val, ok := envString(envVlogBlockCodec); ok {
		switch strings.ToLower(val) {
		case "snappy":
			opts.ValueLog.BlockCodec = ValueLogBlockSnappy
		case "lz4":
			opts.ValueLog.BlockCodec = ValueLogBlockLZ4
		case "zstd":
			opts.ValueLog.BlockCodec = ValueLogBlockZSTD
		}
	}
	if v, ok := envInt(envVlogBlockTargetBytes); ok {
		opts.ValueLog.BlockTargetCompressedBytes = v
	}

	if enabled, ok := envBoolSet(envVlogDictEnable); ok {
		if enabled {
			EnableValueLogDictCompression(opts)
		} else {
			DisableValueLogDictCompression(opts)
		}
	}
	train := opts.ValueLog.DictTrain
	trainTouched := false
	if v, ok := envInt(envVlogDictTrainBytes); ok {
		train.TrainBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictBytes); ok {
		train.DictBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictMinRecords); ok {
		train.MinRecords = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictMaxRecordBytes); ok {
		train.MaxRecordBytes = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictSampleStride); ok {
		train.SampleStride = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictDedupWindow); ok {
		train.DedupWindow = v
		trainTouched = true
	}
	if v, ok := envInt(envVlogDictTrainLevel); ok {
		train.Level = v
		trainTouched = true
	}
	if trainTouched {
		opts.ValueLog.DictTrain = train
	}
	if v, ok := envInt(envVlogDictMaxK); ok {
		opts.ValueLog.DictMaxK = v
	}
	if v, ok := envString(envVlogDictClassMode); ok {
		mode := strings.ToLower(strings.TrimSpace(v))
		switch mode {
		case "single", "default":
			opts.ValueLog.DictClassMode = ValueLogDictClassSingle
		case "split_outer_leaf", "split-outer-leaf", "split", "outer_leaf_split":
			opts.ValueLog.DictClassMode = ValueLogDictClassSplitOuterLeaf
		default:
			log.Printf("treedb: unsupported %s=%q; keeping existing ValueLog.DictClassMode", envVlogDictClassMode, v)
		}
	}
	if v, ok := envString(envVlogDictZstdLevel); ok {
		if level, ok := parseZstdEncoderLevel(v); ok {
			opts.ValueLog.DictFrameEncodeLevel = level
		}
	}
	if enabled, ok := envBoolSet(envVlogDictEntropy); ok {
		opts.ValueLog.DictFrameEnableEntropy = enabled
	}
	if v, ok := envFloat64(envVlogDictAdaptiveRatio); ok {
		opts.ValueLog.DictAdaptiveRatio = v
	}
	if v, ok := envFloat64(envVlogDictMinPayloadSavings); ok {
		opts.ValueLog.DictMinPayloadSavingsRatio = v
	}
	if v, ok := envInt64(envVlogMaxRetainedBytes); ok {
		opts.ValueLog.MaxRetainedBytes = v
	}
	if v, ok := envInt64(envVlogMaxRetainedBytesHard); ok {
		opts.ValueLog.MaxRetainedBytesHard = v
	}
	if v, ok := envInt64(envVlogRewriteBudgetBytesPerSec); ok {
		opts.ValueLog.Generational.RewriteBudgetBytesPerSec = v
	}
	if v, ok := envInt(envVlogRewriteBudgetRecordsPerSec); ok {
		opts.ValueLog.Generational.RewriteBudgetRecordsPerSec = v
	}
	if v, ok := envInt64(envVlogRewriteTriggerTotalBytes); ok {
		opts.ValueLog.Generational.RewriteTriggerTotalBytes = v
	}
	if v, ok := envInt64(envVlogRewriteTriggerStaleRatioPPM); ok {
		if v < 0 {
			v = 0
		}
		maxUint32Int64 := int64(^uint32(0))
		if v > maxUint32Int64 {
			v = maxUint32Int64
		}
		opts.ValueLog.Generational.RewriteTriggerStaleRatioPPM = uint32(v)
	}
	if v, ok := envInt64(envVlogRewriteTriggerChurnPerSec); ok {
		opts.ValueLog.Generational.RewriteTriggerChurnPerSec = v
	}
}

func applyEnvIndexRuntimeOverrides(opts *Options) {
	if opts == nil {
		return
	}

	if v, ok := envBoolSet(envVerifyOnRead); ok {
		opts.VerifyOnRead = v
	}
	if v, ok := envBoolSet(envPreferAppendAlloc); ok {
		opts.PreferAppendAlloc = v
	}
	if v, ok := envBoolSet(envDisablePiggybackCompaction); ok {
		opts.DisablePiggybackCompaction = v
	}
}

func validateEnvIndexFormatOverrides(cfg db.FormatConfig, mainDir string) error {
	formatPath := filepath.Join(mainDir, "format.json")
	if v, ok := envBoolSet(envLeafPrefixCompression); ok && v != cfg.LeafPrefixCompression {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (leaf_prefix_compression=%t); rebuild the DB directory to change index format", envLeafPrefixCompression, v, formatPath, cfg.LeafPrefixCompression)
	}
	if v, ok := envBoolSet(envIndexColumnarLeaves); ok && v != cfg.IndexColumnarLeaves {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (index_columnar_leaves=%t); rebuild the DB directory to change index format", envIndexColumnarLeaves, v, formatPath, cfg.IndexColumnarLeaves)
	}
	if v, ok := envBoolSet(envIndexPackedValuePtr); ok && v != cfg.IndexPackedValuePtr {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (index_packed_valueptr=%t); rebuild the DB directory to change index format", envIndexPackedValuePtr, v, formatPath, cfg.IndexPackedValuePtr)
	}
	if v, ok := envBoolSet(envIndexInternalBaseDelta); ok && v != cfg.IndexInternalBaseDelta {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (index_internal_base_delta=%t); rebuild the DB directory to change index format", envIndexInternalBaseDelta, v, formatPath, cfg.IndexInternalBaseDelta)
	}
	if v, ok := envBoolSet(envIndexOuterLeavesInValueLog); ok && v != cfg.IndexOuterLeavesInValueLog {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (index_outer_leaves_in_vlog=%t); rebuild the DB directory to change index format", envIndexOuterLeavesInValueLog, v, formatPath, cfg.IndexOuterLeavesInValueLog)
	}
	if v, ok := envBoolSet(envIndexAdaptiveLeafEncoding); ok && v != cfg.IndexAdaptiveLeafEncoding {
		return fmt.Errorf("treedb: %s=%t conflicts with %s (index_adaptive_leaf_encoding=%t); rebuild the DB directory to change index format", envIndexAdaptiveLeafEncoding, v, formatPath, cfg.IndexAdaptiveLeafEncoding)
	}
	return nil
}

func applyEnvIndexFormatOverrides(opts *Options) {
	if opts == nil {
		return
	}

	if v, ok := envBoolSet(envLeafPrefixCompression); ok {
		opts.LeafPrefixCompression = v
	}
	if v, ok := envBoolSet(envIndexColumnarLeaves); ok {
		opts.IndexColumnarLeaves = v
	}
	if v, ok := envBoolSet(envIndexPackedValuePtr); ok {
		opts.IndexPackedValuePtr = v
	}
	if v, ok := envBoolSet(envIndexInternalBaseDelta); ok {
		opts.IndexInternalBaseDelta = v
	}
	if v, ok := envBoolSet(envIndexOuterLeavesInValueLog); ok {
		opts.IndexOuterLeavesInValueLog = v
	}
	if v, ok := envBoolSet(envIndexAdaptiveLeafEncoding); ok {
		opts.IndexAdaptiveLeafEncoding = v
	}
}

func valueLogReadIntegrityLabel(opts Options) string {
	switch opts.ValueLog.ReadIntegrity {
	case db.IntegritySkipChecksums:
		return "unsafe-skip-checksums"
	default:
		return "verify"
	}
}

func computeDurabilityMode(opts Options) string {
	if opts.ReadOnly {
		return "read_only"
	}
	mode := "wal_on_sync"
	switch opts.Durability {
	case db.DurabilityDurable:
		mode = "wal_on_sync"
	case db.DurabilityWALOnRelaxed:
		mode = "wal_on_relaxed_sync"
	case db.DurabilityWALOffRelaxed:
		mode = "wal_off_relaxed_sync"
	default:
		mode = fmt.Sprintf("durability_%d", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity == db.IntegritySkipChecksums {
		mode += "+no_read_checksum"
	}
	if opts.VerifyOnRead {
		mode += "+verify_on_read"
	}
	return mode
}

const (
	envCloseCheckpoint        = "TREEDB_CLOSE_CHECKPOINT"
	envCloseCompactIndex      = "TREEDB_CLOSE_COMPACT_INDEX"
	envCloseVacuumIndexOnline = "TREEDB_CLOSE_VACUUM_INDEX_ONLINE"
	envCloseVacuumTimeout     = "TREEDB_CLOSE_VACUUM_TIMEOUT"
	envCloseLog               = "TREEDB_CLOSE_LOG"
	envCloseScopeContains     = "TREEDB_CLOSE_SCOPE_CONTAINS"
	envWritePathLog           = "TREEDB_WRITE_PATH_LOG"
)

func envBool(name string) bool {
	val, ok := os.LookupEnv(name)
	if !ok {
		return false
	}
	if val == "" {
		return true
	}
	parsed, err := strconv.ParseBool(val)
	return err == nil && parsed
}

func envBoolSet(name string) (bool, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return false, false
	}
	if val == "" {
		return true, true
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func envString(name string) (string, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return "", false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return "", false
	}
	return val, true
}

func envInt(name string) (int, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func envInt64(name string) (int64, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func envFloat64(name string) (float64, bool) {
	val, ok := os.LookupEnv(name)
	if !ok {
		return 0, false
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func parseZstdEncoderLevel(val string) (zstd.EncoderLevel, bool) {
	v := strings.TrimSpace(strings.ToLower(val))
	switch v {
	case "fastest":
		return zstd.SpeedFastest, true
	case "default":
		return zstd.SpeedDefault, true
	case "better":
		return zstd.SpeedBetterCompression, true
	case "best":
		return zstd.SpeedBestCompression, true
	default:
		// Accept integer levels for convenience.
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, false
		}
		level := zstd.EncoderLevel(i)
		switch level {
		case zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression, zstd.SpeedBestCompression:
			return level, true
		default:
			return 0, false
		}
	}
}

func envDuration(name string, def time.Duration) time.Duration {
	val, ok := os.LookupEnv(name)
	if !ok || val == "" {
		return def
	}
	if d, err := time.ParseDuration(val); err == nil {
		return d
	}
	if secs, err := strconv.Atoi(val); err == nil {
		return time.Duration(secs) * time.Second
	}
	return def
}

func (db *DB) closeMaintenanceEnabled() bool {
	scope := os.Getenv(envCloseScopeContains)
	if scope == "" {
		return true
	}
	if db == nil {
		return false
	}
	if db.dir == "" {
		return true
	}
	return strings.Contains(db.dir, scope)
}

func (db *DB) closeMaintenance() error {
	logEnabled := envBool(envCloseLog)
	if !db.closeMaintenanceEnabled() {
		if logEnabled {
			log.Printf("treedb: close maintenance skipped dir=%q", db.dir)
		}
		return nil
	}
	var err error
	if envBool(envCloseCheckpoint) {
		if logEnabled {
			log.Printf("treedb: close checkpoint start")
		}
		if e := db.Checkpoint(); e != nil {
			err = errors.Join(err, e)
		}
		if logEnabled {
			log.Printf("treedb: close checkpoint done")
		}
	}
	if envBool(envCloseCompactIndex) {
		if logEnabled {
			log.Printf("treedb: close compact index start")
		}
		if e := db.CompactIndex(); e != nil {
			err = errors.Join(err, e)
		}
		if logEnabled {
			log.Printf("treedb: close compact index done")
		}
	}
	if envBool(envCloseVacuumIndexOnline) {
		timeout := envDuration(envCloseVacuumTimeout, 30*time.Minute)
		if logEnabled {
			log.Printf("treedb: close vacuum index online start timeout=%s", timeout)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if e := db.VacuumIndexOnline(ctx); e != nil {
			if errors.Is(e, errVacuumUnsupported) {
				if logEnabled {
					log.Printf("treedb: close vacuum index online skipped: %v", e)
				}
			} else {
				err = errors.Join(err, e)
			}
		}
		cancel()
		if logEnabled {
			log.Printf("treedb: close vacuum index online done")
		}
	}
	return err
}

// Close closes the DB.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.bgVac.Stop()
	var err error
	if db.cached != nil || db.backend != nil {
		if e := db.closeMaintenance(); e != nil {
			err = errors.Join(err, e)
		}
	}

	db.lifecycleMu.Lock()
	defer db.lifecycleMu.Unlock()

	// Close cached layer first if present
	if db.cached != nil {
		if db.commandWALCached {
			if e := db.checkpointCachedForPublicCommandWAL(); e != nil {
				wrapped := fmt.Errorf("treedb: final command WAL checkpoint during close: %w", e)
				db.reportError(wrapped)
				err = errors.Join(err, wrapped)
			}
			if testDuringPublicCloseAfterCheckpoint != nil {
				testDuringPublicCloseAfterCheckpoint()
			}
		}
		err = errors.Join(err, db.cached.Close())
		db.cached = nil
	}
	db.closePublicCommandWALGroupCommit()

	// Always close backend if present
	if db.backend != nil {
		err = errors.Join(err, db.backend.Close())
		db.backend = nil
	}
	if db.dictdb != nil {
		err = errors.Join(err, db.dictdb.Close())
		db.dictdb = nil
	}
	if db.templateDB != nil {
		err = errors.Join(err, db.templateDB.Close())
		db.templateDB = nil
	}

	return errors.Join(err, db.backgroundError())
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

// Get returns the value for a key.
//
// Semantics: Returns a safe copy of the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	key = normalizeRawKVPointKey(key)
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Get(key)
	}
	return db.backend.Get(key)
}

// GetVersioned returns the value for a key plus TreeDB's native per-entry
// revision. Missing keys return a nil value and nil error, matching Get.
func (db *DB) GetVersioned(key []byte) ([]byte, EntryRevision, error) {
	key = normalizeRawKVPointKey(key)
	if err := db.ensureOpen(); err != nil {
		return nil, LegacyEntryRevision, err
	}
	if db.cached != nil {
		return db.cached.GetVersioned(key)
	}
	return db.backend.GetVersioned(key)
}

// GetMany returns values for keys.
//
// Semantics: Returns safe copies of values. Missing keys are returned as nil
// entries with no error.
func (db *DB) GetMany(keys [][]byte) ([][]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.GetMany(keys)
	}
	return db.backend.GetMany(keys)
}

// GetManyView calls fn once for each key with a read-only value view. The
// callback may be invoked in any order and may be invoked concurrently for
// large batches; the index argument identifies the input key. Values are valid
// only until fn returns and must be copied before retaining. Missing keys are
// reported with found=false and value=nil. Existing safe-copy GetMany semantics
// are unchanged.
func (db *DB) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.GetManyView(keys, fn)
	}
	return db.backend.GetManyView(keys, fn)
}

// GetManyParallelPlan reports how TreeDB would schedule GetMany for the given
// key count. It can be used by adapters to enforce external worker budgets
// without duplicating TreeDB scheduler constants.
func (db *DB) GetManyParallelPlan(keyCount int) (workers int, parallel bool) {
	if db == nil {
		return 1, false
	}
	if keyCount <= 0 {
		return 1, false
	}
	if db.cached != nil {
		if planner, ok := any(db.cached).(getManyPlanner); ok {
			return planner.GetManyParallelPlan(keyCount)
		}
	}
	if db.backend != nil {
		if planner, ok := any(db.backend).(getManyPlanner); ok {
			return planner.GetManyParallelPlan(keyCount)
		}
	}
	workers = runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > keyCount {
		workers = keyCount
	}
	return workers, workers > 1
}

// GetUnsafe returns the value for a key.
//
// Semantics: Returns a safe copy of the value. For zero-copy views tied to a
// snapshot lifetime, use AcquireSnapshot().GetUnsafe.
func (db *DB) GetUnsafe(key []byte) ([]byte, error) {
	return db.Get(key)
}

// GetAppend appends the value for the key to dst and returns the new slice.
// It avoids internal allocations by using the provided buffer.
// If the key is not found, it returns dst and ErrKeyNotFound.
func (db *DB) GetAppend(key, dst []byte) ([]byte, error) {
	if err := db.ensureOpen(); err != nil {
		return dst, err
	}
	if db.cached != nil {
		return db.cached.GetAppend(key, dst)
	}
	return db.backend.GetAppend(key, dst)
}

// GetVersionedAppend appends the value for key to dst and returns TreeDB's
// native per-entry revision. Missing/tombstoned keys return dst and
// ErrKeyNotFound; tombstones preserve their stored revision.
func (db *DB) GetVersionedAppend(key, dst []byte) ([]byte, EntryRevision, error) {
	if err := db.ensureOpen(); err != nil {
		return dst, LegacyEntryRevision, err
	}
	if db.cached != nil {
		return db.cached.GetVersionedAppend(key, dst)
	}
	return db.backend.GetVersionedAppend(key, dst)
}

// Has reports whether a key exists in the database.
func (db *DB) Has(key []byte) (bool, error) {
	key = normalizeRawKVPointKey(key)
	if err := db.ensureOpen(); err != nil {
		return false, err
	}
	if db.cached != nil {
		return db.cached.Has(key)
	}
	return db.backend.Has(key)
}

// HasMany reports whether each key exists.
func (db *DB) HasMany(keys [][]byte) ([]bool, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.HasMany(keys)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return nil, ErrClosed
	}
	defer snap.Close()
	return snap.HasMany(keys)
}

// HasPrefixes reports whether each prefix has at least one visible key.
func (db *DB) HasPrefixes(prefixes [][]byte) ([]bool, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.HasPrefixes(prefixes)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return nil, ErrClosed
	}
	defer snap.Close()
	return snap.HasPrefixes(prefixes)
}

// Set writes a key/value pair using the selected profile's ordinary ACK class.
// command_wal_durable makes the command-WAL prefix durable before returning;
// relaxed profiles do not force a durability boundary here.
func (db *DB) Set(key, value []byte) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		if db.commandWALCached {
			var publication publicCommandWALPublication
			err := db.cached.SetAfterCommandWALAppendWithPreparedRevision(key, value, func(assignRevision func() page.EntryRevision) error {
				return db.appendPublicRawKVPointCommand(commitlog.RawKVOpSet, key, value, assignRevision, db.commandWALOrdinaryWriteRequiresSync(), &publication)
			})
			db.finishPublicCommandWALGroupPublication(publication, err)
			return err
		}
		return db.cached.Set(key, value)
	}
	return db.backend.Set(key, value)
}

// SetSync writes a key/value pair and forces a durability boundary.
// When the command WAL is enabled, explicit sync operations opt up to durable
// V2 publication even when the configured default durability is relaxed.
func (db *DB) SetSync(key, value []byte) error {
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		if db.commandWALCached {
			var publication publicCommandWALPublication
			err := db.cached.SetAfterCommandWALAppendWithPreparedRevision(key, value, func(assignRevision func() page.EntryRevision) error {
				return db.appendPublicRawKVPointCommand(commitlog.RawKVOpSet, key, value, assignRevision, true, &publication)
			})
			db.finishPublicCommandWALGroupPublication(publication, err)
			return err
		}
		return db.cached.SetSync(key, value)
	}
	return db.backend.SetSync(key, value)
}

// Update applies fn to the current value for key and writes the returned
// mutation without forcing an fsync boundary.
//
// Concurrent Update calls for the same key on the same DB handle do not lose
// each other's changes: if the key changes while fn is running, Update retries
// with the newer value. Point Set/Delete calls on the same handle participate in
// the same single-key commit serialization but remain unconditional writes.
// Because fn may be retried, it should avoid externally visible side effects.
func (db *DB) Update(key []byte, fn UpdateFunc) error {
	key = normalizeRawKVPointKey(key)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.commandWALCached {
		db.rawSpanNativePublicUpdateReject.Add(1)
		return ErrCommandWALRejected
	}
	if db.cached != nil {
		return db.cached.Update(key, fn)
	}
	return db.backend.Update(key, fn)
}

// UpdateSync applies fn to the current value for key and writes the returned
// mutation with a sync durability boundary. Command-WAL cached mode rejects
// callback-based updates until they have deterministic replay support.
//
// Concurrent UpdateSync/Update calls for the same key on the same DB handle do
// not lose each other's changes. Because fn may be retried, it should avoid
// externally visible side effects.
func (db *DB) UpdateSync(key []byte, fn UpdateFunc) error {
	key = normalizeRawKVPointKey(key)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.commandWALCached {
		db.rawSpanNativePublicUpdateSyncReject.Add(1)
		return ErrCommandWALRejected
	}
	if db.cached != nil {
		return db.cached.UpdateSync(key, fn)
	}
	return db.backend.UpdateSync(key, fn)
}

// InitConditionalTxn initializes tx as a native conditional transaction.
// Callers must pass zero-value or closed transaction storage.
func (db *DB) InitConditionalTxn(tx *ConditionalTxn) error {
	return db.initConditionalTxn(tx, false)
}

// InitConditionalTxnWithSnapshot initializes tx as a native conditional
// transaction and exposes its pinned opening snapshot through tx.Snapshot.
// The transaction-owned snapshot supports point reads; range iteration fails
// closed until native conditional range guards are part of the public contract.
// Callers must pass zero-value or closed transaction storage.
func (db *DB) InitConditionalTxnWithSnapshot(tx *ConditionalTxn) error {
	return db.initConditionalTxn(tx, true)
}

func (db *DB) initConditionalTxn(tx *ConditionalTxn, withSnapshot bool) error {
	if tx == nil || tx.cachedActive || tx.backend != nil {
		return ErrConditionalTxnClosed
	}
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		if db.commandWALCached {
			return ErrConditionalTxnUnsupported
		}
		var err error
		if withSnapshot {
			err = db.cached.InitConditionalTxnWithSnapshot(&tx.cached)
		} else {
			err = db.cached.InitConditionalTxn(&tx.cached)
		}
		if err != nil {
			return err
		}
		tx.backend = nil
		tx.cachedActive = true
		tx.snapshotExposed = withSnapshot
		return nil
	}
	if db.backend == nil {
		return ErrClosed
	}
	backendTx, err := db.backend.NewConditionalTxn()
	if err != nil {
		return err
	}
	*tx = ConditionalTxn{backend: backendTx, snapshotExposed: withSnapshot}
	return nil
}

// NewConditionalTxn opens a native conditional transaction.
func (db *DB) NewConditionalTxn() (*ConditionalTxn, error) {
	tx := &ConditionalTxn{}
	if err := db.InitConditionalTxn(tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// NewConditionalTxnWithSnapshot opens a native conditional transaction and
// exposes its pinned opening snapshot through tx.Snapshot. The transaction owns
// that snapshot and closes it on Commit, CommitSync, or Close. The snapshot
// supports point reads; range iteration fails closed until native conditional
// range guards are part of the public contract.
func (db *DB) NewConditionalTxnWithSnapshot() (*ConditionalTxn, error) {
	tx := &ConditionalTxn{}
	if err := db.InitConditionalTxnWithSnapshot(tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// Delete removes a key using the selected profile's ordinary ACK class.
func (db *DB) Delete(key []byte) error {
	key = normalizeRawKVPointKey(key)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		if db.commandWALCached {
			var publication publicCommandWALPublication
			err := db.cached.DeleteAfterCommandWALAppendWithPreparedRevision(key, func(assignRevision func() page.EntryRevision) error {
				return db.appendPublicRawKVPointCommand(commitlog.RawKVOpDelete, key, nil, assignRevision, db.commandWALOrdinaryWriteRequiresSync(), &publication)
			})
			db.finishPublicCommandWALGroupPublication(publication, err)
			return err
		}
		return db.cached.Delete(key)
	}
	return db.backend.Delete(key)
}

// DeleteRange removes all keys in the range [start, end).
//
// This is primarily used by benchmark suites and maintenance tooling. In cached
// mode, it may use fast paths that avoid per-key tombstones when safe.
func (db *DB) DeleteRange(start, end []byte) error {
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.commandWALCached {
		if batch.IsDeleteRangeNoop(start, end) {
			return nil
		}
		appended := false
		var publication publicCommandWALPublication
		err := db.cached.DeleteRangeAfterCommandWALAppend(start, end, func() error {
			if err := db.appendPublicRawKVDeleteRangeCommand(start, end, db.commandWALOrdinaryWriteRequiresSync(), &publication); err != nil {
				return err
			}
			appended = true
			return nil
		})
		db.finishPublicCommandWALGroupPublication(publication, err)
		if err != nil && appended && db.backend != nil {
			db.backend.MarkCommandWALRecoveryRequired()
		}
		return err
	}
	if db.cached != nil {
		return db.cached.DeleteRange(start, end)
	}
	// Backend-only mode: fall back to iterating keys and issuing deletes.
	it, err := db.backend.Iterator(start, end)
	if err != nil {
		return err
	}
	defer it.Close()

	b := db.backend.NewBatch()
	defer b.Close()
	for it.Valid() {
		if !it.IsDeleted() {
			if err := b.Delete(it.UnsafeKey()); err != nil {
				return err
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return b.Write()
}

// DeleteSync removes a key and forces a durability boundary.
func (db *DB) DeleteSync(key []byte) error {
	key = normalizeRawKVPointKey(key)
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		if db.commandWALCached {
			var publication publicCommandWALPublication
			err := db.cached.DeleteAfterCommandWALAppendWithPreparedRevision(key, func(assignRevision func() page.EntryRevision) error {
				return db.appendPublicRawKVPointCommand(commitlog.RawKVOpDelete, key, nil, assignRevision, true, &publication)
			})
			db.finishPublicCommandWALGroupPublication(publication, err)
			return err
		}
		return db.cached.DeleteSync(key)
	}
	return db.backend.DeleteSync(key)
}

// Iterator returns a forward iterator over the range [start, end).
func (db *DB) Iterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.Iterator(start, end)
	}
	return db.backend.Iterator(start, end)
}

// SeekGE returns owned copies of the first visible physical key and value in
// [start,end). A miss returns nil, nil, false, nil.
func (db *DB) SeekGE(start, end []byte) ([]byte, []byte, bool, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, nil, false, err
	}
	if db.cached != nil {
		return db.cached.SeekGE(start, end)
	}
	it, err := db.backend.Iterator(start, end)
	if err != nil {
		return nil, nil, false, err
	}
	defer it.Close()
	if !it.Valid() {
		return nil, nil, false, it.Error()
	}
	return it.KeyCopy(nil), it.ValueCopy(nil), true, it.Error()
}

// ReverseIterator returns a reverse iterator over the range [start, end).
func (db *DB) ReverseIterator(start, end []byte) (Iterator, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.ReverseIterator(start, end)
	}
	return db.backend.ReverseIterator(start, end)
}

// NewBatch creates a new batch for buffered writes.
func (db *DB) NewBatch() Batch {
	if err := db.beginPublicOperation(); err != nil {
		return nil
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		inner := db.cached.NewBatch()
		if db.commandWALCached {
			return newCommandWALPublicBatch(db, inner, 0)
		}
		return inner
	}
	return db.backend.NewBatch()
}

// NewBatchWithSize creates a new batch with a best-effort capacity hint.
//
// The size parameter is a best-effort hint, not a strict limit. Small hints are
// treated like approximate entry reserves. Larger hints may be interpreted as a
// byte budget and normalized into an internal entry estimate instead.
// Extremely large hints may also be capped internally.
//
// Callers must not rely on an exact 1:1 mapping between size and the number of
// entries or bytes that can be written to the batch.
func (db *DB) NewBatchWithSize(size int) Batch {
	if err := db.beginPublicOperation(); err != nil {
		return nil
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		inner := db.cached.NewBatchWithSize(size)
		if db.commandWALCached {
			return newCommandWALPublicBatch(db, inner, size)
		}
		return inner
	}
	return db.backend.NewBatchWithSize(size)
}

// AcquireSnapshot returns a new snapshot.
//
// In cached mode, snapshots include writes that are buffered in memtables.
// In read-only mode (no caching), snapshots are backend-only.
//
// Callers may use GetUnsafe to obtain zero-copy views tied to the snapshot lifetime.
func (db *DB) AcquireSnapshot() Snapshot {
	if db == nil {
		return nil
	}
	if db.cached != nil {
		if snap := db.cached.AcquireBackendSnapshotFastPath(); snap != nil {
			return snap
		}
		return db.cached.AcquireSnapshot()
	}
	if db.backend == nil {
		return nil
	}
	return db.backend.AcquireSnapshot()
}

// Stats returns diagnostic stats for the active backend and cached layer.
func (db *DB) Stats() map[string]string {
	if db == nil || (db.cached == nil && db.backend == nil) {
		return nil
	}
	if db.cached != nil {
		stats := db.cached.Stats()
		if stats == nil {
			stats = make(map[string]string)
		}
		writePathStatsInto(stats, db.writePath)
		db.profileStatsInto(stats)
		db.publicCommandWALLiveStatsInto(stats)
		db.publicCommandWALBatchStatsInto(stats)
		db.publicCommandWALGroupStatsInto(stats)
		db.publicRawSpanNativeStatsInto(stats)
		db.publicOperationStatsInto(stats)
		stats["treedb.durability_mode"] = db.durabilityMode
		stats["treedb.vlog.read_integrity"] = db.valueLogReadIntegrity
		bgIndexVacuumStatsInto(stats, &db.bgVac)
		maintenanceStatsInto(stats, &db.maintenance)
		return stats
	}
	stats := db.backend.Stats()
	if stats == nil {
		stats = make(map[string]string)
	}
	writePathStatsInto(stats, db.writePath)
	db.profileStatsInto(stats)
	db.publicRawSpanNativeStatsInto(stats)
	db.publicOperationStatsInto(stats)
	stats["treedb.durability_mode"] = db.durabilityMode
	stats["treedb.vlog.read_integrity"] = db.valueLogReadIntegrity
	bgIndexVacuumStatsInto(stats, &db.bgVac)
	maintenanceStatsInto(stats, &db.maintenance)
	return stats
}

func (db *DB) profileStatsInto(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	stats["treedb.profile.resolved"] = string(db.resolvedProfile)
	stats["treedb.profile.ordinary_ack_class"] = db.resolvedProfile.OrdinaryAckClass()
	stats["treedb.profile.production"] = fmt.Sprintf("%t", db.resolvedProfile.Production())
	stats["treedb.profile.bench_unsafe"] = fmt.Sprintf("%t", db.resolvedProfile == ProfileBenchUnsafe)
	stats["treedb.profile.deprecated_alias"] = string(db.deprecatedProfileAlias)
}

func (db *DB) publicRawSpanNativeStatsInto(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	update := db.rawSpanNativePublicUpdateReject.Load()
	updateSync := db.rawSpanNativePublicUpdateSyncReject.Load()
	prefix := "treedb.raw.span_native.public."
	stats[prefix+"command_wal_rejections_total"] = fmt.Sprintf("%d", update+updateSync)
	stats[prefix+"route.update.fallback.reason.command_wal_barrier.count_total"] = fmt.Sprintf("%d", update)
	stats[prefix+"route.update.fallback.reason.command_wal_barrier.ops_total"] = fmt.Sprintf("%d", update)
	stats[prefix+"route.update_sync.fallback.reason.command_wal_barrier.count_total"] = fmt.Sprintf("%d", updateSync)
	stats[prefix+"route.update_sync.fallback.reason.command_wal_barrier.ops_total"] = fmt.Sprintf("%d", updateSync)
}

func (db *DB) publicOperationStatsInto(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	publicOperationStatsInto(stats, "treedb.public.batch.write", &db.publicBatchWrite)
	publicOperationStatsInto(stats, "treedb.public.batch.write_sync", &db.publicBatchWriteSync)
	publicBatchWriteSyncPhaseStatsInto(stats, db.publicBatchWriteSyncPhaseEnabled, &db.publicBatchWriteSyncPhase)
	publicOperationStatsInto(stats, "treedb.public.checkpoint", &db.publicCheckpoint)
}

func (db *DB) observePublicBatchWrite(sync bool, start time.Time, err error) {
	if db == nil {
		return
	}
	if sync {
		db.publicBatchWriteSync.observe(start, err)
		return
	}
	db.publicBatchWrite.observe(start, err)
}

func (db *DB) observePublicCheckpoint(start time.Time, err error) {
	if db == nil {
		return
	}
	db.publicCheckpoint.observe(start, err)
}

// DurabilityMode reports the effective durability/integrity policy string.
func (db *DB) DurabilityMode() string {
	if db == nil {
		return ""
	}
	return db.durabilityMode
}

// ResolvedProfile reports the immutable canonical profile selected at open.
func (db *DB) ResolvedProfile() Profile {
	if db == nil {
		return ""
	}
	return db.resolvedProfile
}

func (db *DB) MaintenancePhase() MaintenancePhase {
	if db == nil || db.cached == nil {
		return MaintenancePhaseSteady
	}
	return db.cached.MaintenancePhase()
}

func (db *DB) SetMaintenancePhase(phase MaintenancePhase) {
	if db == nil || db.cached == nil {
		return
	}
	db.cached.SetMaintenancePhase(phase)
}

// Print dumps best-effort debug output for the underlying backend.
func (db *DB) Print() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.Print()
	}
	return db.backend.Print()
}

// Checkpoint forces a durable backend boundary and trims cached-mode WAL
// segments, so long-running cached-mode workloads do not accumulate unbounded
// `wal/` growth.
//
// In cached mode this flushes queued memtables with backend sync and resets the
// WAL to a fresh segment. In backend mode it forces a sync boundary.
//
// Current collection-local pending writes may have their own flush-boundary
// behavior; see docs/spec/contracts.md for the current collection contract and
// the PR1 collection WAL target contract. After collection WAL lands,
// Checkpoint returning nil must also cover pre-cut collection WAL transactions
// or return/report explicit collection WAL debt.
func (db *DB) Checkpoint() (err error) {
	start := time.Now()
	defer func() {
		db.observePublicCheckpoint(start, err)
	}()
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	defer db.lifecycleMu.RUnlock()
	if db.cached != nil {
		return db.checkpointCachedForPublicCommandWAL()
	}
	return db.backend.Checkpoint()
}

func (db *DB) checkpointCachedForPublicCommandWAL() error {
	if db == nil || db.cached == nil {
		return ErrClosed
	}
	if err := db.cached.Checkpoint(); err != nil {
		return err
	}
	if db.commandWALCached && db.backend != nil {
		if err := db.refreshPublicCommandWALCheckpointFallback(); err != nil {
			return err
		}
		if err := db.backend.CleanupCommandWALCoveredSegmentsAtCheckpoint(true); err != nil {
			return err
		}
	}
	if testAfterCachedCheckpoint != nil {
		testAfterCachedCheckpoint()
	}
	db.clearPublishedPublicCommandWALPending()
	return nil
}

// CompactIndex performs an in-place index vacuum (bulk rebuild) on the backend.
// In cached mode it first drains the caching layer so the backend reflects all
// buffered writes before rebuilding.
func (db *DB) CompactIndex() error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.backend == nil {
		return ErrClosed
	}

	if db.cached != nil {
		if err := db.cached.Drain(); err != nil {
			return err
		}
	}
	err := db.backend.CompactIndex()
	return db.reconcileCachedBackendMaintenance(err)
}

// VacuumIndexOnline rebuilds the user index into a new file and swaps it in with
// a short writer pause. Disk space from the old index is reclaimed once any old
// snapshots/iterators drain.
func (db *DB) VacuumIndexOnline(ctx context.Context) error {
	_, err := db.vacuumIndexOnlineStats(ctx)
	return err
}

func (db *DB) vacuumIndexOnlineStats(ctx context.Context) (VacuumOnlineStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return VacuumOnlineStats{}, err
	}
	// Reject unsupported platforms before maintenance acquisition or cached
	// checkpointing so this API remains non-mutating when it cannot run.
	if runtime.GOOS == "windows" {
		return VacuumOnlineStats{}, errVacuumUnsupported
	}
	_, finishMaintenance, err := db.beginFullScanMaintenanceContext(ctx, "vacuum")
	if err != nil {
		return VacuumOnlineStats{}, err
	}
	success := false
	defer func() { finishMaintenance(success) }()
	if err := db.beginPublicOperation(); err != nil {
		return VacuumOnlineStats{}, err
	}
	defer db.lifecycleMu.RUnlock()
	if db.backend == nil {
		return VacuumOnlineStats{}, ErrClosed
	}

	// In cached mode, ensure the backend reflects all buffered writes before
	// rebuilding/switching the index file. This avoids exposing a backend state
	// that temporarily "forgets" keys that only existed in memtables/WAL, which
	// can break higher layers that assume a stable durable boundary.
	if db.cached != nil {
		if err := db.cached.CheckpointContext(ctx); err != nil {
			return VacuumOnlineStats{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return VacuumOnlineStats{}, err
	}

	// Collection command-WAL writers buffer root deltas above the cached layer.
	// Drain those buffers and prevent new ones from publishing against the old
	// index generation until the replacement generation is installed.
	// ponytail: this blocks those writers for one vacuum; add generation-aware
	// buffer rebasing only if the measured maintenance pause requires it.
	var unlockCommandWALPublish func()
	if db.commandWALCached {
		unlockCommandWALPublish, err = db.backend.LockCommandWALPublishWithBarriers()
		if err != nil {
			return VacuumOnlineStats{}, err
		}
		defer unlockCommandWALPublish()
	}

	stats := db.backend.VacuumIndexOnline(ctx)
	onlineStats := db.backend.VacuumOnlineStats()
	if err := db.reconcileCachedBackendMaintenance(stats); err != nil {
		return onlineStats, err
	}
	success = true
	return onlineStats, nil
}

// VacuumOnlineStats returns an owned snapshot of the latest backend online
// vacuum attempt. The snapshot is diagnostic only and never retains payloads.
func (db *DB) VacuumOnlineStats() VacuumOnlineStats {
	if db == nil {
		return VacuumOnlineStats{}
	}
	db.lifecycleMu.RLock()
	defer db.lifecycleMu.RUnlock()
	if db.backend == nil {
		return VacuumOnlineStats{}
	}
	return db.backend.VacuumOnlineStats()
}

// VacuumIndexOffline rewrites `index.db` into a fresh file and swaps it in.
// This is intended to reclaim space and restore locality after long churn.
//
// It is an offline operation: it acquires the exclusive open lock for opts.Dir.
func VacuumIndexOffline(opts Options) error {
	if err := resolveOpenProfileOptions(&opts); err != nil {
		return err
	}
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		return err
	}
	opts.Dir = layout.mainDir
	opts.DisableSideStores = layout.disableSideStores

	// Preserve the persisted on-disk format knobs by default so offline index
	// maintenance doesn't accidentally rewrite the DB into a different layout.
	if opts.IgnoreFormatConfig {
		requiresCommandWAL, err := db.CommandWALRequiredFeatureEnabled(layout.mainDir)
		if err != nil {
			return err
		}
		opts.CommandWAL = opts.CommandWAL || requiresCommandWAL
	} else {
		if cfg, ok, err := db.LoadFormatConfig(layout.mainDir); err != nil {
			return err
		} else if ok {
			opts.CommandWAL = opts.CommandWAL || cfg.RequiresCommandWALV1()
			cfg.ApplyToOptions(&opts)
		}
	}

	sideCleanup, err := wireSideStoreLookups(layout.rootDir, &opts)
	if err != nil {
		return err
	}
	defer func() { _ = sideCleanup() }()

	return db.VacuumIndexOffline(opts)
}

// FragmentationReport returns best-effort structural stats about the on-disk user
// index that help diagnose scan regressions after churn.
//
// Note: In cached mode this reflects the backend state only; queued memtables are
// not included unless the caller has explicitly drained the cache (e.g. via
// close+reopen or a maintenance operation that drains).
func (db *DB) FragmentationReport() (map[string]string, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.backend == nil {
		return nil, ErrClosed
	}
	return db.backend.FragmentationReport()
}
