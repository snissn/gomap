package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	collectionMetaVersion        = 2
	maxCollectionMutationRetries = 64
	maxCollectionInt             = int(^uint(0) >> 1)

	// DefaultIndexedWriteMemtableMaxDocuments bounds the native indexed
	// collection write-domain before it auto-flushes to persistent roots.
	DefaultIndexedWriteMemtableMaxDocuments = 96000
	// DefaultIndexedWriteMemtableAsyncFlushMaxDocuments uses a larger publish
	// cadence for async indexed flushes. Background publishing decouples writer
	// staging from root apply enough that larger immutable flush units reduce
	// root-apply amplification without penalizing synchronous/checkpoint-heavy
	// insert shapes that still use DefaultIndexedWriteMemtableMaxDocuments.
	DefaultIndexedWriteMemtableAsyncFlushMaxDocuments = 256000
	// DefaultIndexedWriteMemtableMaxRootRuns bounds accumulated root-local
	// mutation runs. When the document threshold is also enabled, hitting this
	// limit compacts mutable root runs in memory before publishing so many small
	// indexed update batches do not create an expensive pending-run chain before
	// the document threshold is reached.
	DefaultIndexedWriteMemtableMaxRootRuns = DefaultIndexedWriteMemtableMaxDocuments
	// DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns pairs with
	// DefaultIndexedWriteMemtableAsyncFlushMaxDocuments.
	DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns = DefaultIndexedWriteMemtableAsyncFlushMaxDocuments
	// DefaultIndexedWriteMemtableDirectBatchDocuments keeps large, already
	// well-amortized InsertBatch calls on the immediate publish path. Smaller
	// batches use the indexed write-domain memtable path by default.
	DefaultIndexedWriteMemtableDirectBatchDocuments = 16000
	// DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits bounds opt-in
	// background indexed flush work. When the queue reaches this many immutable
	// flush units, the triggering writer publishes synchronously to cap memory
	// and visibility lag.
	DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits = 4
	defaultCollectionUpdateCombineMaxBatch              = 256
	defaultCollectionUpdateCombineDrainYields           = 1
	collectionUpdateCombineIdleTTL                      = 30 * time.Second
	collectionPprofComponentKey                         = "gomap_component"
	collectionPprofOperationKey                         = "gomap_operation"
	collectionPprofIndexedAsyncFlush                    = "collections_indexed_async_flush"
	collectionPprofIndexedAsyncFlushRun                 = "publish_buffered_indexed_flush"
)

var (
	ErrCollectionNotFound  = errors.New("collections: collection not found")
	ErrDocumentExists      = errors.New("collections: document already exists")
	ErrDuplicateDocumentID = errors.New("collections: duplicate document id in batch")
	ErrIndexNotFound       = errors.New("collections: index not found")
	ErrUniqueIndexConflict = errors.New("collections: unique index conflict")
	ErrConcurrentMutation  = errors.New("collections: concurrent mutation")

	errBSONIDMutation                          = errors.New("collections: update replacement cannot modify _id")
	errCollectionManagerNil                    = errors.New("collections: collection manager is nil")
	errCollectionNil                           = errors.New("collections: collection is nil")
	errCollectionDBNil                         = errors.New("collections: db is nil")
	errCollectionNotFound                      = ErrCollectionNotFound
	errCollectionRootOverlaysRequireCompaction = errors.New("collections: collection root overlays require compaction before writes")
	errUpdateBatchHasSecondaryUniqueIndex      = errors.New("collections: update batch has secondary unique index")
	errUpdateBatchChangesSecondaryUniqueIndex  = errors.New("collections: update batch changes secondary unique index")
	errIndexedFlushLostOwnership               = errors.New("collections: async indexed publish lost ownership of in-flight flush units")
	errUpdateCombinerStopped                   = errors.New("collections: update combiner stopped before DB update completed; callback may have been invoked")
	indexedAsyncFlushPprofLabels               = pprof.Labels(
		collectionPprofComponentKey, collectionPprofIndexedAsyncFlush,
		collectionPprofOperationKey, collectionPprofIndexedAsyncFlushRun,
	)
)

// UpdateBatchItem describes one document update in a batch. DocumentID must be
// non-empty and unique within the batch. Update receives the current stored
// document bytes and returns the replacement document bytes in the same format
// expected by Update. If Update returns changed=true, replacement must be a
// complete valid stored document for the collection format; returning
// replacement=nil, changed=false is the supported no-op form.
type UpdateBatchItem struct {
	DocumentID []byte
	Update     func(current []byte) (replacement []byte, changed bool, err error)
}

type updateBatchMode uint8

const (
	updateBatchModeAny updateBatchMode = iota
	updateBatchModeNoSecondaryUniqueIndexes
	updateBatchModeNoSecondaryUniqueIndexChanges
)

const bsonIDSnapshotInlineValueLen = 64

// UpdateBatchResult reports the outcome for one UpdateBatch item.
type UpdateBatchResult struct {
	Matched  bool
	Modified bool
}

// UpdateBatchItemError wraps an error produced while preparing or applying one
// item in an UpdateBatch call.
type UpdateBatchItemError struct {
	Index int
	Err   error
}

func (e *UpdateBatchItemError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("collections: update batch index %d: %v", e.Index, e.Err)
}

func (e *UpdateBatchItemError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsDuplicateKeyError(err error) bool {
	return errors.Is(err, ErrDocumentExists) ||
		errors.Is(err, ErrDuplicateDocumentID) ||
		errors.Is(err, ErrUniqueIndexConflict)
}

const (
	systemCollectionMetaPrefix        = "collections/meta/"
	systemCollectionRootPrefix        = "collections/root/"
	systemCollectionRootOverlayPrefix = "collections/root-overlay/"
)

func backendRootStoragePolicy(policy RootStoragePolicy) (backenddb.OrderedRootStoragePolicy, error) {
	switch policy {
	case RootStorageDefault:
		return backenddb.OrderedRootStorageDefault, nil
	case RootStorageFast:
		return backenddb.OrderedRootStoragePagerLeaves, nil
	case RootStorageCompressed:
		return backenddb.OrderedRootStorageValueLogLeaves, nil
	default:
		return backenddb.OrderedRootStorageDefault, fmt.Errorf("collections: unsupported root storage policy %q", policy)
	}
}

func collectionPlannerOptions(meta CollectionMeta) (collectionOptions, error) {
	documentFormat, err := normalizeDocumentFormat(meta.Options.DocumentFormat)
	if err != nil {
		return collectionOptions{}, err
	}
	dataPolicy, err := backendRootStoragePolicy(meta.Options.DataRootStoragePolicy)
	if err != nil {
		return collectionOptions{}, err
	}
	indexStatePolicy, err := backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
	if err != nil {
		return collectionOptions{}, err
	}
	return collectionOptions{
		allowArrayValuesInIndex: meta.Options.AllowArrayValuesInIndex,
		documentFormat:          documentFormat,
		dataStoragePolicy:       dataPolicy,
		indexStateStoragePolicy: indexStatePolicy,
	}, nil
}

func persistIndexStateForOptions(opts collectionOptions) bool {
	return persistIndexStateForDocumentFormat(opts.documentFormat)
}

func persistIndexStateForDocumentFormat(format DocumentFormat) bool {
	switch normalizedDocumentFormat(format) {
	case DocumentFormatTemplateV1, DocumentFormatBSON:
		return false
	default:
		return true
	}
}

type CollectionManager struct {
	db                       *backenddb.DB
	closeUnregister          func()
	closing                  atomic.Bool
	updateBatchDetailedStats atomic.Bool
	domainMu                 sync.RWMutex
	domains                  map[string]*collectionWriteDomain
}

type Collection struct {
	db                *backenddb.DB
	writeDomain       *collectionWriteDomain
	meta              CollectionMeta
	catalogMu         sync.RWMutex
	catalogCommitSeq  uint64
	catalogSystemRoot uint64
	catalog           *collectionCatalog
	insertStatsMu     sync.RWMutex
	lastInsertStats   CollectionInsertStats
	updateStatsMu     sync.RWMutex
	lastUpdateStats   CollectionUpdateStats
}

type CollectionRootOverlayCompactionStats struct {
	Roots        int
	OverlayRoots int
}

// StoredDocumentJSONMaterializer reuses any resources needed to materialize
// stored collection documents as JSON.
type StoredDocumentJSONMaterializer struct {
	documentFormat   DocumentFormat
	templateResolver templateV1Resolver
	closeFn          func() error
}

// Close releases resources held by the materializer.
func (m *StoredDocumentJSONMaterializer) Close() error {
	if m == nil || m.closeFn == nil {
		return nil
	}
	closeFn := m.closeFn
	m.closeFn = nil
	return closeFn()
}

// DocumentFormat returns the collection storage format this materializer was
// created for.
func (m *StoredDocumentJSONMaterializer) DocumentFormat() DocumentFormat {
	if m == nil {
		return DocumentFormatDefault
	}
	return m.documentFormat
}

// StoredDocumentJSON materializes one stored collection document as JSON bytes.
func (m *StoredDocumentJSONMaterializer) StoredDocumentJSON(document []byte) ([]byte, error) {
	if m == nil {
		return nil, errCollectionNil
	}
	switch m.documentFormat {
	case DocumentFormatJSON:
		return bytes.Clone(document), nil
	case DocumentFormatBSON:
		raw := bson.Raw(document)
		if err := raw.Validate(); err != nil {
			return nil, fmt.Errorf("collections: BSON stored document: %w", err)
		}
		return bson.MarshalExtJSON(raw, true, false)
	case DocumentFormatTemplateV1:
		return templateV1StoredDocumentJSON(document, m.templateResolver)
	default:
		return nil, fmt.Errorf("collections: unsupported document format %q", m.documentFormat)
	}
}

// CollectionInsertStats captures phase timings and counters from the most
// recent successful InsertBatch call on a Collection handle.
type CollectionInsertStats struct {
	Documents                    int
	Indexes                      int
	Runs                         int
	BufferedIndexedBatches       int
	BufferedIndexedBypassBatches int
	PrepareDocuments             time.Duration
	IndexStateExtraction         time.Duration
	// DuplicateDocumentPreflight includes duplicate-ID detection and
	// existing-document conflict checks.
	DuplicateDocumentPreflight time.Duration
	UniqueIndexPreflight       time.Duration
	TemplateRunBuild           time.Duration
	PrimaryRunBuild            time.Duration
	IndexStateRunBuild         time.Duration
	SecondaryRunBuild          time.Duration
	Publish                    time.Duration
	SecondaryEntries           int
	SecondaryKeyBytes          int
	SecondarySortedRuns        int
	SecondaryUnsortedRuns      int
	SecondaryRuns              []CollectionSecondaryRunStats
}

// CollectionSecondaryRunStats captures per-secondary-index run construction
// counters from an InsertBatch call.
type CollectionSecondaryRunStats struct {
	IndexName     string
	Entries       int
	KeyBytes      int
	AlreadySorted bool
	Build         time.Duration
}

// CollectionUpdateStats captures phase timings and counters from the most
// recent successful Update/UpdateBatch-style call on a Collection handle.
type CollectionUpdateStats struct {
	Items                int
	Matched              int
	Modified             int
	Indexes              int
	Runs                 int
	BufferedBatches      int
	CurrentRead          time.Duration
	Callback             time.Duration
	PrepareDocuments     time.Duration
	IndexStateExtraction time.Duration
	UniqueIndexPreflight time.Duration
	TemplateRunBuild     time.Duration
	PrimaryRunBuild      time.Duration
	IndexStateRunBuild   time.Duration
	SecondaryRunBuild    time.Duration
	BufferStage          time.Duration
	// Buffer-stage subphase timings are populated only when
	// CollectionManager.SetUpdateBatchDetailedStatsEnabled(true) is enabled.
	// BufferStageLockHold is an enclosing domain mutex hold-time metric and
	// overlaps the validation/root/index/root-append subphases; it is not
	// additive with those child counters.
	BufferStagePrecheck time.Duration
	// BufferStageLockWait measures domain mutex acquisition time, including
	// relock contention after an async flush wait. It does not include async
	// flush completion waits performed after releasing the mutex for
	// backpressure.
	BufferStageLockWait      time.Duration
	BufferStageLockHold      time.Duration
	BufferStageValidation    time.Duration
	BufferStageRootScan      time.Duration
	BufferStageDomainPrepare time.Duration
	BufferStagePrimaryIdx    time.Duration
	BufferStageUniqueIdx     time.Duration
	BufferStageRootAppend    time.Duration
	// BufferStageFlush measures local threshold-flush schedule/publish work
	// performed while staging an indexed buffered update batch. It excludes
	// waits for an already-running async flush that leave no local schedule or
	// publish work for the current batch.
	BufferStageFlush       time.Duration
	Publish                time.Duration
	SecondaryDeleteEntries int
	SecondarySetEntries    int
	SecondaryKeyBytes      int
	SecondaryRuns          []CollectionUpdateSecondaryRunStats
	IndexValueChanges      int
	IndexValueUnchanged    int
	MaskFallbacks          int
	UniqueIndexChecks      int
	UniqueIndexCheckSkips  int
	// IndexStats is populated for the first inline index runtimes when detailed
	// update-batch stats are enabled. The first IndexStatsCount entries are
	// valid; remaining indexes are represented only by aggregate counters.
	IndexStatsCount int
	IndexStats      [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats
}

const maxCollectionUpdateInlineIndexStats = 8

// CollectionUpdateSecondaryRunStats captures per-secondary-index delta counters
// from an UpdateBatch-style call.
type CollectionUpdateSecondaryRunStats struct {
	IndexName string
	Deletes   int
	Sets      int
	KeyBytes  int
}

// CollectionUpdateIndexStats captures per-index update planning decisions from
// an UpdateBatch-style call. Changed/unchanged are counted per modified
// document for each cached index runtime.
type CollectionUpdateIndexStats struct {
	CollectionName    string
	IndexName         string
	IndexOrdinal      int
	Unique            bool
	Changed           int
	Unchanged         int
	UniqueChecks      int
	UniqueCheckSkips  int
	SecondaryRuns     int
	SecondaryDeletes  int
	SecondarySets     int
	SecondaryKeyBytes int
}

// CollectionManagerStats captures aggregate write-domain counters for a
// CollectionManager. The counters are process-local observability; they are
// not persisted with collection metadata.
type CollectionManagerStats struct {
	Domains                         int
	PendingDocuments                int
	PendingBytes                    int64
	PendingRootRuns                 int
	PendingIndexedFlushUnits        int
	PendingIndexedSemanticRecords   int
	OverlayMutableDocuments         int
	OverlayQueuedIndexedFlushUnits  int
	OverlayActiveIndexedFlushUnits  int
	OverlayVisibleDepth             int
	IndexedAsyncFlushRunning        int
	MutationLockCalls               uint64
	MutationLockWait                time.Duration
	MutationLockHold                time.Duration
	IndexedStageBatches             uint64
	IndexedStageDocs                uint64
	IndexedStageBytes               uint64
	IndexedStageRootRuns            uint64
	IndexedSemanticRawRecords       uint64
	IndexedSemanticRawIndexDeltas   uint64
	IndexedSemanticFallbackRecords  uint64
	IndexedSemanticEffectiveRecords uint64
	IndexedAutoFlushes              uint64
	IndexedAsyncFlushScheduled      uint64
	IndexedAsyncFlushBackpressure   uint64
	IndexedAsyncFlushWait           time.Duration
	IndexedAsyncFlushErrors         uint64
	IndexedFlushCalls               uint64
	IndexedFlushErrors              uint64
	IndexedFlushForcedDrains        uint64
	IndexedFlushUnits               uint64
	IndexedFlushDocs                uint64
	IndexedFlushBytes               uint64
	IndexedFlushRootRuns            uint64
	IndexedFlushRoots               uint64
	IndexedFlushDuration            time.Duration
	IndexedFlushMaterialize         time.Duration
	IndexedFlushPublish             time.Duration
	RootDeltaPlanPrimaryRoots       uint64
	RootDeltaPlanTemplateRoots      uint64
	RootDeltaPlanIndexStateRoots    uint64
	RootDeltaPlanSecondaryRoots     uint64
	RootDeltaPlanEntries            uint64
	RootDeltaPlanKeyBytes           uint64
	RootDeltaPlanValueBytes         uint64
	RootDeltaPlanTombstones         uint64
	PrimaryOnlyUpdateCalls          uint64
	PrimaryOnlyMatched              uint64
	PrimaryOnlyModified             uint64
	PrimaryOnlyBufferedCalls        uint64
	PrimaryOnlyRootPublishes        uint64
	PrimaryOnlyRootDeltaEntries     uint64
	PrimaryOnlyRootDeltaKeyBytes    uint64
	PrimaryOnlyRootDeltaValueBytes  uint64
	PrimaryOnlyCoalescedDocs        uint64
	UpdateCombineRequests           uint64
	UpdateCombineBatches            uint64
	UpdateCombineBatchedRequests    uint64
	UpdateCombineFallbackRequests   uint64
	UpdateCombineQueueDepthMax      uint64
	UpdateBatchCalls                uint64
	UpdateBatchItems                uint64
	UpdateBatchMatched              uint64
	UpdateBatchModified             uint64
	UpdateBatchRuns                 uint64
	UpdateBatchBufferedBatches      uint64
	UpdateBatchCurrentRead          time.Duration
	UpdateBatchCallback             time.Duration
	UpdateBatchPrepareDocuments     time.Duration
	UpdateBatchIndexStateExtract    time.Duration
	UpdateBatchUniquePreflight      time.Duration
	UpdateBatchTemplateRunBuild     time.Duration
	UpdateBatchPrimaryRunBuild      time.Duration
	UpdateBatchIndexStateRunBuild   time.Duration
	UpdateBatchSecondaryRunBuild    time.Duration
	UpdateBatchBufferStage          time.Duration
	// Detailed buffer-stage aggregate timings are populated only when
	// CollectionManager.SetUpdateBatchDetailedStatsEnabled(true) is enabled.
	// UpdateBatchBufferLockHold is an enclosing domain mutex hold-time metric
	// and overlaps the validation/root/index/root-append subphases; it is not
	// additive with those child counters.
	UpdateBatchBufferPrecheck      time.Duration
	UpdateBatchBufferLockWait      time.Duration
	UpdateBatchBufferLockHold      time.Duration
	UpdateBatchBufferValidation    time.Duration
	UpdateBatchBufferRootScan      time.Duration
	UpdateBatchBufferDomainPrepare time.Duration
	UpdateBatchBufferPrimaryIdx    time.Duration
	UpdateBatchBufferUniqueIdx     time.Duration
	UpdateBatchBufferRootAppend    time.Duration
	// UpdateBatchBufferFlush measures only threshold-flush work that was
	// actually scheduled/executed while staging indexed buffered update batches.
	UpdateBatchBufferFlush         time.Duration
	UpdateBatchPublish             time.Duration
	UpdateBatchSecondaryDeletes    uint64
	UpdateBatchSecondarySets       uint64
	UpdateBatchSecondaryKeyBytes   uint64
	UpdateBatchIndexValueChanges   uint64
	UpdateBatchIndexValueUnchanged uint64
	UpdateBatchMaskFallbacks       uint64
	UpdateBatchUniqueChecks        uint64
	UpdateBatchUniqueCheckSkips    uint64
	UpdateBatchIndexStatsCount     int
	UpdateBatchIndexStats          [maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats
	IndexedFlushRequeues           uint64
	IndexedFlushRequeuedUnits      uint64
	IndexedFlushLostOwnership      uint64
	IndexedFlushRootBaseMismatches uint64
}

// DocumentRecord is one primary collection record returned by ScanDocuments.
// ID and Document are cloned byte slices owned by the caller.
type DocumentRecord struct {
	ID       []byte
	Document []byte
}

type RootStoragePolicy string

const (
	RootStorageDefault    RootStoragePolicy = ""
	RootStorageFast       RootStoragePolicy = "fast"
	RootStorageCompressed RootStoragePolicy = "compressed"
)

type DocumentFormat string

const (
	DocumentFormatDefault    DocumentFormat = ""
	DocumentFormatJSON       DocumentFormat = "json"
	DocumentFormatBSON       DocumentFormat = "bson"
	DocumentFormatTemplateV1 DocumentFormat = "template-v1"
)

type IndexValueType string

const (
	IndexValueString IndexValueType = "string"
	IndexValueBool   IndexValueType = "bool"
	IndexValueInt64  IndexValueType = "int64"
	IndexValueDouble IndexValueType = "double"
)

type CollectionOptions struct {
	AllowArrayValuesInIndex bool              `json:"allow_array_values_in_index,omitempty"`
	DocumentFormat          DocumentFormat    `json:"document_format,omitempty"`
	DataRootStoragePolicy   RootStoragePolicy `json:"data_root_storage_policy,omitempty"`
	IndexStateStoragePolicy RootStoragePolicy `json:"index_state_storage_policy,omitempty"`
	// DisableIndexedWriteMemtables opts an indexed collection out of the native
	// write-domain memtable path. It is intended for debugging and baseline
	// comparisons; indexed collections use memtables by default.
	DisableIndexedWriteMemtables bool `json:"disable_indexed_write_memtables,omitempty"`
	// BufferedIndexedWrites is normalized metadata describing whether indexed
	// inserts and safe update root deltas are staged in the collection write
	// domain before Flush/Close or auto-flush. Staged writes are visible to
	// primary and secondary reads on the same manager, but durability remains at
	// the flush boundary, matching the existing no-index buffered path.
	BufferedIndexedWrites bool `json:"buffered_indexed_writes,omitempty"`
	// BufferedIndexedWriteMaxDocuments flushes indexed write buffers once this
	// many staged documents are pending. Zero uses the native default for indexed
	// memtables unless DisableIndexedWriteMemtables is set or the schema has no
	// indexes.
	BufferedIndexedWriteMaxDocuments int `json:"buffered_indexed_write_max_documents,omitempty"`
	// BufferedIndexedWriteMaxBytes flushes indexed write buffers once the staged
	// root-run payload estimate reaches this many bytes. Zero leaves flushing to
	// explicit Flush/Close calls.
	BufferedIndexedWriteMaxBytes int64 `json:"buffered_indexed_write_max_bytes,omitempty"`
	// BufferedIndexedWriteMaxRootRuns flushes indexed write buffers once this
	// many root-local mutation runs are pending. Zero disables the run-count
	// trigger when another flush limit is explicitly configured; when all
	// indexed buffer limits are zero, metadata normalization installs native
	// defaults.
	BufferedIndexedWriteMaxRootRuns int `json:"buffered_indexed_write_max_root_runs,omitempty"`
	// BufferedIndexedAsyncFlush lets threshold-triggered indexed memtable flushes
	// publish from a background goroutine. This is an opt-in performance mode:
	// Flush, FlushAll, and backend Close still drain pending indexed writes before
	// returning, but auto-flush thresholds no longer imply immediate durability.
	BufferedIndexedAsyncFlush bool `json:"buffered_indexed_async_flush,omitempty"`
	// BufferedIndexedOverlayRoots publishes indexed memtable flush units as
	// durable overlay roots instead of immediately applying them into base roots.
	// Maintenance can later compact overlay roots into base roots; reads merge
	// overlay roots over base roots while they are pending.
	BufferedIndexedOverlayRoots bool `json:"buffered_indexed_overlay_roots,omitempty"`
	// BufferedIndexedAsyncFlushMaxQueuedUnits bounds immutable indexed flush
	// units queued for the background publisher. Zero uses the native default
	// when async flush is enabled on an indexed schema.
	BufferedIndexedAsyncFlushMaxQueuedUnits int `json:"buffered_indexed_async_flush_max_queued_units,omitempty"`
}

type IndexDefinition struct {
	Name          string            `json:"name"`
	Field         string            `json:"field"`
	ValueType     IndexValueType    `json:"value_type"`
	Unique        bool              `json:"unique,omitempty"`
	MultiKey      bool              `json:"multi_key,omitempty"`
	StoragePolicy RootStoragePolicy `json:"storage_policy,omitempty"`
}

type CollectionMeta struct {
	Name    string            `json:"name"`
	Options CollectionOptions `json:"options,omitempty"`
	Indexes []IndexDefinition `json:"indexes,omitempty"`
}

type collectionMetaDisk struct {
	Version int               `json:"version"`
	Name    string            `json:"name"`
	Options CollectionOptions `json:"options,omitempty"`
	Indexes []IndexDefinition `json:"indexes,omitempty"`
}

type collectionCatalog struct {
	// collectionCatalog is immutable once cached or published. Root updates must
	// create a replacement catalog via cloneCatalogWithRootUpdates.
	meta               CollectionMeta
	roots              map[string]uint64
	rootOverlays       map[string][]uint64
	rootOverlayFilters map[string]map[uint64]collectionRootOverlayFilter
	primaryRootName    string
	templateRootName   string
	indexStateRootName string
	indexRuntimes      []indexRuntime
	indexRuntimesErr   error
}

type collectionRootOverlayFilter struct {
	words []uint64
	count uint32
}

const (
	// Overlay filters are handle-local metadata. Persisted overlay descriptors
	// keep only root IDs, so reopened handles fall back to safe maybe-present
	// probing instead of growing system-root descriptors.
	collectionRootOverlayFilterBits    = 2 << 20
	collectionRootOverlayFilterWords   = collectionRootOverlayFilterBits / 64
	maxCollectionRootOverlayFilterKeys = 512 << 10
)

type createIndexBackfillPlan struct {
	rootNames   []string
	baseRootIDs map[string]uint64
	tables      []memtable.Table
	policies    []backenddb.OrderedRootStoragePolicy
}

type noIndexBatchEntry struct {
	id       []byte
	document []byte
}

type indexedSemanticRecordKind uint8

const (
	indexedSemanticRecordUnknown indexedSemanticRecordKind = iota
	indexedSemanticRecordUpdate
)

type indexedSemanticFallbackReason uint8

const (
	indexedSemanticFallbackNone indexedSemanticFallbackReason = iota
	indexedSemanticFallbackRawOnly
)

type indexedSemanticRecord struct {
	kind        indexedSemanticRecordKind
	documentID  []byte
	indexDeltas []indexedSemanticIndexDelta
	fallback    indexedSemanticFallbackReason
}

type indexedSemanticIndexDelta struct {
	indexName  string
	rootName   string
	runtimeIdx int
	unique     bool
	oldValues  [][]byte
	newValues  [][]byte
}

type indexedFlushUnit struct {
	rootRuns        map[string][]memtable.Table
	rootPolicies    map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs     map[string]uint64
	uniqueValueRuns map[string][]memtable.Table
	semanticRecords []indexedSemanticRecord
	arenaRefs       [][]byte
	docCount        int
	byteCount       int64
	rootRunCount    int
}

type coalescedFlushBatchState uint8

const (
	coalescedFlushBatchQueued coalescedFlushBatchState = iota
	coalescedFlushBatchActive
	coalescedFlushBatchMaterializing
	coalescedFlushBatchPublishing
	coalescedFlushBatchPublished
	coalescedFlushBatchRequeued
	coalescedFlushBatchLostOwnership
)

type coalescedFlushBatch struct {
	state coalescedFlushBatchState

	// Original immutable units, in FIFO order. The merged unit is only the
	// mechanical publish view for the current DB ordered-root API.
	units           []indexedFlushUnit
	mergedUnit      indexedFlushUnit
	semanticRecords []indexedSemanticRecord

	rootNames          []string
	rootBaseIDs        map[string]uint64
	rootOverlays       map[string][]uint64
	rootOverlayFilters map[string]collectionRootOverlayFilter

	docCount       int
	byteCount      int64
	rootRunCount   int
	rootCount      int
	rootDeltaStats collectionRootDeltaPlanStats
}

type indexedFlushPublishWork struct {
	pin            *backenddb.Snapshot
	meta           CollectionMeta
	catalog        *collectionCatalog
	baseSystemRoot uint64
	baseCommitSeq  uint64
	batch          coalescedFlushBatch
}

type bufferedIndexedCheckpoint struct {
	loaded                 bool
	meta                   CollectionMeta
	catalog                *collectionCatalog
	baseCommitSeq          uint64
	baseSystemRoot         uint64
	primaryRoot            uint64
	count                  int
	bufferedBytes          int64
	mutableCount           int
	mutableBytes           int64
	writeGeneration        uint64
	rootRuns               map[string][]memtable.Table
	rootMutableRuns        map[string]memtable.Table
	rootPolicies           map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs            map[string]uint64
	rootValueArenas        [][]byte
	indexedSemanticRecords []indexedSemanticRecord
	indexedPublishingUnits []indexedFlushUnit
	indexedFlushUnits      []indexedFlushUnit
	primaryRunIndexActive  bool
	uniqueValueRuns        map[string][]memtable.Table
	rootRunCount           int
}

type bufferedUniqueValueIndex struct {
	values     map[uint64][]byte
	collisions map[uint64][][]byte
	arenas     [][]byte
}

type bufferedPrimaryRunIndex struct {
	values     map[uint64]bufferedPrimaryRunRef
	collisions map[uint64][]bufferedPrimaryRunRef
	arenas     [][]byte
}

type bufferedPrimaryRunRef struct {
	key   []byte
	table memtable.Table
}

type collectionWriteDomain struct {
	// mutationMu serializes root descriptor publishes for handles opened
	// through the same manager so optimistic retries do not starve under
	// sustained collection write contention.
	mutationMu             sync.Mutex
	mu                     sync.RWMutex
	indexedAsyncMu         sync.Mutex
	indexedAsyncCond       *sync.Cond
	indexedAsyncRun        bool
	indexedAsyncErr        error
	updateCombineMu        sync.Mutex
	updateCombiner         *collectionUpdateCombiner
	updateDraining         *collectionUpdateCombiner
	updateCombineDone      bool
	updateCombineTTL       time.Duration
	closingWrites          atomic.Bool
	loaded                 bool
	meta                   CollectionMeta
	catalog                *collectionCatalog
	baseCommitSeq          uint64
	baseSystemRoot         uint64
	primaryRoot            uint64
	storagePolicy          backenddb.OrderedRootStoragePolicy
	table                  memtable.Table
	indexedPublishingUnits []indexedFlushUnit
	indexedFlushUnits      []indexedFlushUnit
	rootRuns               map[string][]memtable.Table
	rootMutableRuns        map[string]memtable.Table
	rootPolicies           map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs            map[string]uint64
	rootValueArenas        [][]byte
	indexedSemanticRecords []indexedSemanticRecord
	primaryIDIndex         *bufferedUniqueValueIndex
	// Built lazily by readers so write-only indexed buffering does not pay for
	// an auxiliary lookup structure it never uses.
	primaryRunIndex  *bufferedPrimaryRunIndex
	uniqueValueRuns  map[string][]memtable.Table
	uniqueValueIndex map[string]*bufferedUniqueValueIndex
	count            int
	bufferedBytes    int64
	mutableCount     int
	mutableBytes     int64
	rootRunCount     int
	writeGeneration  uint64

	mutationLockCalls                atomic.Uint64
	mutationLockWaitTotalNs          atomic.Uint64
	mutationLockHoldTotalNs          atomic.Uint64
	indexedStageBatches              atomic.Uint64
	indexedStageDocs                 atomic.Uint64
	indexedStageBytes                atomic.Uint64
	indexedStageRootRuns             atomic.Uint64
	indexedSemanticRawRecords        atomic.Uint64
	indexedSemanticRawIndexDeltas    atomic.Uint64
	indexedSemanticFallbackRecords   atomic.Uint64
	indexedSemanticEffectiveRecords  atomic.Uint64
	indexedAutoFlushes               atomic.Uint64
	indexedAsyncFlushScheduled       atomic.Uint64
	indexedAsyncFlushBackpressure    atomic.Uint64
	indexedAsyncFlushWaitTotalNs     atomic.Uint64
	indexedAsyncFlushErrors          atomic.Uint64
	indexedFlushCalls                atomic.Uint64
	indexedFlushErrors               atomic.Uint64
	indexedFlushForcedDrains         atomic.Uint64
	indexedFlushUnitsTotal           atomic.Uint64
	indexedFlushRequeues             atomic.Uint64
	indexedFlushRequeuedUnits        atomic.Uint64
	indexedFlushLostOwnership        atomic.Uint64
	indexedFlushRootBaseMismatches   atomic.Uint64
	indexedFlushDocs                 atomic.Uint64
	indexedFlushBytes                atomic.Uint64
	indexedFlushRootRuns             atomic.Uint64
	indexedFlushRoots                atomic.Uint64
	indexedFlushDurationTotalNs      atomic.Uint64
	indexedFlushMaterializeTotalNs   atomic.Uint64
	indexedFlushPublishTotalNs       atomic.Uint64
	rootDeltaPlanPrimaryRoots        atomic.Uint64
	rootDeltaPlanTemplateRoots       atomic.Uint64
	rootDeltaPlanIndexStateRoots     atomic.Uint64
	rootDeltaPlanSecondaryRoots      atomic.Uint64
	rootDeltaPlanEntries             atomic.Uint64
	rootDeltaPlanKeyBytes            atomic.Uint64
	rootDeltaPlanValueBytes          atomic.Uint64
	rootDeltaPlanTombstones          atomic.Uint64
	primaryOnlyUpdateCalls           atomic.Uint64
	primaryOnlyMatched               atomic.Uint64
	primaryOnlyModified              atomic.Uint64
	primaryOnlyBufferedCalls         atomic.Uint64
	primaryOnlyRootPublishes         atomic.Uint64
	primaryOnlyRootDeltaEntries      atomic.Uint64
	primaryOnlyRootDeltaKeyBytes     atomic.Uint64
	primaryOnlyRootDeltaValueBytes   atomic.Uint64
	primaryOnlyCoalescedDocs         atomic.Uint64
	updateCombineRequests            atomic.Uint64
	updateCombineBatches             atomic.Uint64
	updateCombineBatchedRequests     atomic.Uint64
	updateCombineFallbackRequests    atomic.Uint64
	updateCombineQueueDepthMax       atomic.Uint64
	updateBatchCalls                 atomic.Uint64
	updateBatchItems                 atomic.Uint64
	updateBatchMatched               atomic.Uint64
	updateBatchModified              atomic.Uint64
	updateBatchRuns                  atomic.Uint64
	updateBatchBufferedBatches       atomic.Uint64
	updateBatchCurrentReadNs         atomic.Uint64
	updateBatchCallbackNs            atomic.Uint64
	updateBatchPrepareNs             atomic.Uint64
	updateBatchIndexStateNs          atomic.Uint64
	updateBatchUniquePreflightNs     atomic.Uint64
	updateBatchTemplateRunNs         atomic.Uint64
	updateBatchPrimaryRunNs          atomic.Uint64
	updateBatchIndexStateRunNs       atomic.Uint64
	updateBatchSecondaryRunNs        atomic.Uint64
	updateBatchBufferStageNs         atomic.Uint64
	updateBatchBufferPrecheckNs      atomic.Uint64
	updateBatchBufferLockWaitNs      atomic.Uint64
	updateBatchBufferLockHoldNs      atomic.Uint64
	updateBatchBufferValidationNs    atomic.Uint64
	updateBatchBufferRootScanNs      atomic.Uint64
	updateBatchBufferDomainPrepareNs atomic.Uint64
	updateBatchBufferPrimaryIdxNs    atomic.Uint64
	updateBatchBufferUniqueIdxNs     atomic.Uint64
	updateBatchBufferRootAppendNs    atomic.Uint64
	updateBatchBufferFlushNs         atomic.Uint64
	updateBatchPublishNs             atomic.Uint64
	updateBatchSecondaryDeletes      atomic.Uint64
	updateBatchSecondarySets         atomic.Uint64
	updateBatchSecondaryKeyBytes     atomic.Uint64
	updateBatchIndexValueChanges     atomic.Uint64
	updateBatchIndexValueUnchanged   atomic.Uint64
	updateBatchMaskFallbacks         atomic.Uint64
	updateBatchUniqueChecks          atomic.Uint64
	updateBatchUniqueCheckSkips      atomic.Uint64
	updateBatchDetailedStats         atomic.Bool
	updateBatchIndexChanged          [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUnchanged        [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUniqueChecks     [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUniqueSkips      [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryRuns    [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryDeletes [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondarySets    [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryBytes   [maxCollectionUpdateInlineIndexStats]atomic.Uint64
}

func NewCollectionManager(database *backenddb.DB) *CollectionManager {
	manager := &CollectionManager{db: database}
	if database != nil {
		manager.closeUnregister = database.RegisterCloseHook(manager.closeForBackend)
	}
	return manager
}

// SetUpdateBatchDetailedStatsEnabled toggles high-resolution update-batch phase
// timings for this manager's collection write domains. Lightweight counters
// remain enabled either way; timings are opt-in so normal write paths do not pay
// for repeated clock reads unless a benchmark or profiler explicitly asks.
func (m *CollectionManager) SetUpdateBatchDetailedStatsEnabled(enabled bool) {
	if m == nil {
		return
	}
	m.updateBatchDetailedStats.Store(enabled)
	m.domainMu.RLock()
	for _, domain := range m.domains {
		if domain != nil {
			domain.updateBatchDetailedStats.Store(enabled)
		}
	}
	m.domainMu.RUnlock()
}

func (m *CollectionManager) closeForBackend() error {
	m.closing.Store(true)
	m.stopUpdateCombiners()
	return m.FlushAll()
}

func (m *CollectionManager) isClosing() bool {
	return m.closing.Load() || m.db == nil || m.db.IsClosing()
}

func (m *CollectionManager) stopUpdateCombiners() {
	if m == nil {
		return
	}
	m.domainMu.RLock()
	domains := make([]*collectionWriteDomain, 0, len(m.domains))
	for _, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domain)
		}
	}
	m.domainMu.RUnlock()
	for _, domain := range domains {
		domain.stopUpdateCombiner()
	}
}

// LastInsertStats returns phase timings and counters from the most recent
// successful InsertBatch call on this Collection handle.
func (c *Collection) LastInsertStats() CollectionInsertStats {
	if c == nil {
		return CollectionInsertStats{}
	}
	c.insertStatsMu.RLock()
	defer c.insertStatsMu.RUnlock()
	return cloneCollectionInsertStats(c.lastInsertStats)
}

func (c *Collection) setLastInsertStats(stats CollectionInsertStats) {
	if c == nil {
		return
	}
	c.insertStatsMu.Lock()
	c.lastInsertStats = cloneCollectionInsertStats(stats)
	c.insertStatsMu.Unlock()
}

func cloneCollectionInsertStats(stats CollectionInsertStats) CollectionInsertStats {
	if len(stats.SecondaryRuns) > 0 {
		stats.SecondaryRuns = append([]CollectionSecondaryRunStats(nil), stats.SecondaryRuns...)
	}
	return stats
}

// LastUpdateStats returns phase timings and counters from the most recent
// successful UpdateBatch-style call on this Collection handle.
func (c *Collection) LastUpdateStats() CollectionUpdateStats {
	if c == nil {
		return CollectionUpdateStats{}
	}
	c.updateStatsMu.RLock()
	defer c.updateStatsMu.RUnlock()
	return cloneCollectionUpdateStats(c.lastUpdateStats)
}

func (c *Collection) setLastUpdateStats(stats CollectionUpdateStats) {
	if c == nil {
		return
	}
	c.updateStatsMu.Lock()
	c.lastUpdateStats = cloneCollectionUpdateStats(stats)
	c.updateStatsMu.Unlock()
	if c.writeDomain != nil {
		c.writeDomain.observeUpdateBatchStats(stats)
	}
}

func cloneCollectionUpdateStats(stats CollectionUpdateStats) CollectionUpdateStats {
	if len(stats.SecondaryRuns) > 0 {
		stats.SecondaryRuns = append([]CollectionUpdateSecondaryRunStats(nil), stats.SecondaryRuns...)
	}
	return stats
}

func initCollectionUpdateIndexStats(stats *CollectionUpdateStats, collectionName string, runtimes []indexRuntime, enabled bool) {
	if stats == nil || !enabled || len(runtimes) == 0 {
		return
	}
	count := len(runtimes)
	if count > len(stats.IndexStats) {
		count = len(stats.IndexStats)
	}
	stats.IndexStatsCount = count
	for i := 0; i < count; i++ {
		runtime := runtimes[i]
		stats.IndexStats[i] = CollectionUpdateIndexStats{
			CollectionName: collectionName,
			IndexName:      runtime.def.name,
			IndexOrdinal:   i,
			Unique:         runtime.def.unique,
		}
	}
}

func (c *Collection) updateBatchDetailedStatsEnabled() bool {
	return c != nil && c.writeDomain != nil && c.writeDomain.updateBatchDetailedStats.Load()
}

func updateBatchStatsNow(enabled bool) time.Time {
	if !enabled {
		return time.Time{}
	}
	return time.Now()
}

func updateBatchStatsSince(enabled bool, start time.Time) time.Duration {
	if !enabled {
		return 0
	}
	return time.Since(start)
}

func updateBatchStatsDuration(enabled bool, duration time.Duration) time.Duration {
	if !enabled {
		return 0
	}
	return duration
}

// Stats returns aggregate process-local collection write-domain metrics with
// stable TreeDB benchmark key names.
func (m *CollectionManager) Stats() map[string]string {
	stats := m.StatsSnapshot()
	if stats == (CollectionManagerStats{}) {
		return nil
	}
	out := make(map[string]string, 32)
	out["treedb.collections.write_domain.domains"] = fmt.Sprintf("%d", stats.Domains)
	out["treedb.collections.write_domain.pending_docs"] = fmt.Sprintf("%d", stats.PendingDocuments)
	out["treedb.collections.write_domain.pending_bytes"] = fmt.Sprintf("%d", stats.PendingBytes)
	out["treedb.collections.write_domain.pending_root_runs"] = fmt.Sprintf("%d", stats.PendingRootRuns)
	out["treedb.collections.write_domain.pending_indexed_flush_units"] = fmt.Sprintf("%d", stats.PendingIndexedFlushUnits)
	out["treedb.collections.write_domain.pending_indexed_semantic_raw_records"] = fmt.Sprintf("%d", stats.PendingIndexedSemanticRecords)
	out["treedb.collections.write_domain.overlay.mutable_docs"] = fmt.Sprintf("%d", stats.OverlayMutableDocuments)
	out["treedb.collections.write_domain.overlay.queued_indexed_flush_units"] = fmt.Sprintf("%d", stats.OverlayQueuedIndexedFlushUnits)
	out["treedb.collections.write_domain.overlay.active_indexed_flush_units"] = fmt.Sprintf("%d", stats.OverlayActiveIndexedFlushUnits)
	out["treedb.collections.write_domain.overlay.visible_depth"] = fmt.Sprintf("%d", stats.OverlayVisibleDepth)
	out["treedb.collections.write_domain.indexed_async_flush.running_domains"] = fmt.Sprintf("%d", stats.IndexedAsyncFlushRunning)
	out["treedb.collections.write_domain.mutation_lock.calls_total"] = fmt.Sprintf("%d", stats.MutationLockCalls)
	out["treedb.collections.write_domain.mutation_lock.wait_ns_total"] = fmt.Sprintf("%d", stats.MutationLockWait.Nanoseconds())
	out["treedb.collections.write_domain.mutation_lock.hold_ns_total"] = fmt.Sprintf("%d", stats.MutationLockHold.Nanoseconds())
	if denom := stats.MutationLockWait + stats.MutationLockHold; denom > 0 {
		out["treedb.collections.write_domain.mutation_lock.wait_share_pct"] = fmt.Sprintf("%.3f", 100*float64(stats.MutationLockWait)/float64(denom))
	}
	out["treedb.collections.write_domain.indexed_stage.batches_total"] = fmt.Sprintf("%d", stats.IndexedStageBatches)
	out["treedb.collections.write_domain.indexed_stage.docs_total"] = fmt.Sprintf("%d", stats.IndexedStageDocs)
	out["treedb.collections.write_domain.indexed_stage.bytes_total"] = fmt.Sprintf("%d", stats.IndexedStageBytes)
	out["treedb.collections.write_domain.indexed_stage.root_runs_total"] = fmt.Sprintf("%d", stats.IndexedStageRootRuns)
	out["treedb.collections.write_domain.indexed_semantic.raw_records_total"] = fmt.Sprintf("%d", stats.IndexedSemanticRawRecords)
	out["treedb.collections.write_domain.indexed_semantic.raw_index_deltas_total"] = fmt.Sprintf("%d", stats.IndexedSemanticRawIndexDeltas)
	out["treedb.collections.write_domain.indexed_semantic.fallback_records_total"] = fmt.Sprintf("%d", stats.IndexedSemanticFallbackRecords)
	out["treedb.collections.write_domain.indexed_semantic.effective_records_total"] = fmt.Sprintf("%d", stats.IndexedSemanticEffectiveRecords)
	out["treedb.collections.write_domain.indexed_stage.auto_flushes_total"] = fmt.Sprintf("%d", stats.IndexedAutoFlushes)
	out["treedb.collections.write_domain.indexed_async_flush.scheduled_total"] = fmt.Sprintf("%d", stats.IndexedAsyncFlushScheduled)
	out["treedb.collections.write_domain.indexed_async_flush.backpressure_sync_total"] = fmt.Sprintf("%d", stats.IndexedAsyncFlushBackpressure)
	out["treedb.collections.write_domain.indexed_async_flush.wait_ns_total"] = fmt.Sprintf("%d", stats.IndexedAsyncFlushWait.Nanoseconds())
	out["treedb.collections.write_domain.indexed_async_flush.errors_total"] = fmt.Sprintf("%d", stats.IndexedAsyncFlushErrors)
	out["treedb.collections.write_domain.indexed_flush.calls_total"] = fmt.Sprintf("%d", stats.IndexedFlushCalls)
	out["treedb.collections.write_domain.indexed_flush.errors_total"] = fmt.Sprintf("%d", stats.IndexedFlushErrors)
	out["treedb.collections.write_domain.indexed_flush.forced_drains_total"] = fmt.Sprintf("%d", stats.IndexedFlushForcedDrains)
	out["treedb.collections.write_domain.indexed_flush.units_total"] = fmt.Sprintf("%d", stats.IndexedFlushUnits)
	out["treedb.collections.write_domain.indexed_flush.requeues_total"] = fmt.Sprintf("%d", stats.IndexedFlushRequeues)
	out["treedb.collections.write_domain.indexed_flush.requeued_units_total"] = fmt.Sprintf("%d", stats.IndexedFlushRequeuedUnits)
	out["treedb.collections.write_domain.indexed_flush.lost_ownership_total"] = fmt.Sprintf("%d", stats.IndexedFlushLostOwnership)
	out["treedb.collections.write_domain.indexed_flush.root_base_mismatch_total"] = fmt.Sprintf("%d", stats.IndexedFlushRootBaseMismatches)
	out["treedb.collections.write_domain.indexed_flush.docs_total"] = fmt.Sprintf("%d", stats.IndexedFlushDocs)
	out["treedb.collections.write_domain.indexed_flush.bytes_total"] = fmt.Sprintf("%d", stats.IndexedFlushBytes)
	out["treedb.collections.write_domain.indexed_flush.root_runs_total"] = fmt.Sprintf("%d", stats.IndexedFlushRootRuns)
	out["treedb.collections.write_domain.indexed_flush.roots_total"] = fmt.Sprintf("%d", stats.IndexedFlushRoots)
	out["treedb.collections.write_domain.indexed_flush.duration_ns_total"] = fmt.Sprintf("%d", stats.IndexedFlushDuration.Nanoseconds())
	out["treedb.collections.write_domain.indexed_flush.materialize_ns_total"] = fmt.Sprintf("%d", stats.IndexedFlushMaterialize.Nanoseconds())
	out["treedb.collections.write_domain.indexed_flush.publish_ns_total"] = fmt.Sprintf("%d", stats.IndexedFlushPublish.Nanoseconds())
	out["treedb.collections.write_domain.root_delta_plan.roots.primary_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanPrimaryRoots)
	out["treedb.collections.write_domain.root_delta_plan.roots.template_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanTemplateRoots)
	out["treedb.collections.write_domain.root_delta_plan.roots.index_state_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanIndexStateRoots)
	out["treedb.collections.write_domain.root_delta_plan.roots.secondary_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanSecondaryRoots)
	out["treedb.collections.write_domain.root_delta_plan.entries_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanEntries)
	out["treedb.collections.write_domain.root_delta_plan.key_bytes_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanKeyBytes)
	out["treedb.collections.write_domain.root_delta_plan.value_bytes_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanValueBytes)
	out["treedb.collections.write_domain.root_delta_plan.tombstones_total"] = fmt.Sprintf("%d", stats.RootDeltaPlanTombstones)
	out["treedb.collections.write_domain.primary_only.update_calls_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyUpdateCalls)
	out["treedb.collections.write_domain.primary_only.matched_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyMatched)
	out["treedb.collections.write_domain.primary_only.modified_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyModified)
	out["treedb.collections.write_domain.primary_only.buffered_calls_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyBufferedCalls)
	out["treedb.collections.write_domain.primary_only.root_publishes_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyRootPublishes)
	out["treedb.collections.write_domain.primary_only.root_delta_entries_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyRootDeltaEntries)
	out["treedb.collections.write_domain.primary_only.root_delta_key_bytes_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyRootDeltaKeyBytes)
	out["treedb.collections.write_domain.primary_only.root_delta_value_bytes_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyRootDeltaValueBytes)
	out["treedb.collections.write_domain.primary_only.coalesced_docs_total"] = fmt.Sprintf("%d", stats.PrimaryOnlyCoalescedDocs)
	out["treedb.collections.write_domain.update_combine.requests_total"] = fmt.Sprintf("%d", stats.UpdateCombineRequests)
	out["treedb.collections.write_domain.update_combine.batches_total"] = fmt.Sprintf("%d", stats.UpdateCombineBatches)
	out["treedb.collections.write_domain.update_combine.batched_requests_total"] = fmt.Sprintf("%d", stats.UpdateCombineBatchedRequests)
	out["treedb.collections.write_domain.update_combine.fallback_requests_total"] = fmt.Sprintf("%d", stats.UpdateCombineFallbackRequests)
	out["treedb.collections.write_domain.update_combine.queue_depth_max"] = fmt.Sprintf("%d", stats.UpdateCombineQueueDepthMax)
	out["treedb.collections.write_domain.update_batch.calls_total"] = fmt.Sprintf("%d", stats.UpdateBatchCalls)
	out["treedb.collections.write_domain.update_batch.items_total"] = fmt.Sprintf("%d", stats.UpdateBatchItems)
	out["treedb.collections.write_domain.update_batch.matched_total"] = fmt.Sprintf("%d", stats.UpdateBatchMatched)
	out["treedb.collections.write_domain.update_batch.modified_total"] = fmt.Sprintf("%d", stats.UpdateBatchModified)
	out["treedb.collections.write_domain.update_batch.root_runs_total"] = fmt.Sprintf("%d", stats.UpdateBatchRuns)
	out["treedb.collections.write_domain.update_batch.buffered_batches_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferedBatches)
	out["treedb.collections.write_domain.update_batch.current_read_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchCurrentRead.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.callback_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchCallback.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.prepare_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchPrepareDocuments.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.index_state_extract_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchIndexStateExtract.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.unique_preflight_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchUniquePreflight.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.template_run_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchTemplateRunBuild.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.primary_run_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchPrimaryRunBuild.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.index_state_run_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchIndexStateRunBuild.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.secondary_runs_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchSecondaryRunBuild.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferStage.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_precheck_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferPrecheck.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_lock_wait_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferLockWait.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_lock_hold_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferLockHold.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_validation_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferValidation.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_root_scan_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferRootScan.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_domain_prepare_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferDomainPrepare.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferPrimaryIdx.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferUniqueIdx.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_root_append_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferRootAppend.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_flush_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferFlush.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.publish_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchPublish.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.secondary_deletes_total"] = fmt.Sprintf("%d", stats.UpdateBatchSecondaryDeletes)
	out["treedb.collections.write_domain.update_batch.secondary_sets_total"] = fmt.Sprintf("%d", stats.UpdateBatchSecondarySets)
	out["treedb.collections.write_domain.update_batch.secondary_key_bytes_total"] = fmt.Sprintf("%d", stats.UpdateBatchSecondaryKeyBytes)
	out["treedb.collections.write_domain.update_batch.index_value_changes_total"] = fmt.Sprintf("%d", stats.UpdateBatchIndexValueChanges)
	out["treedb.collections.write_domain.update_batch.index_value_unchanged_total"] = fmt.Sprintf("%d", stats.UpdateBatchIndexValueUnchanged)
	out["treedb.collections.write_domain.update_batch.changed_index_fast_mask_fallbacks_total"] = fmt.Sprintf("%d", stats.UpdateBatchMaskFallbacks)
	out["treedb.collections.write_domain.update_batch.unique_checks_total"] = fmt.Sprintf("%d", stats.UpdateBatchUniqueChecks)
	out["treedb.collections.write_domain.update_batch.unique_check_skips_total"] = fmt.Sprintf("%d", stats.UpdateBatchUniqueCheckSkips)
	for i := 0; i < stats.UpdateBatchIndexStatsCount && i < len(stats.UpdateBatchIndexStats); i++ {
		indexStats := stats.UpdateBatchIndexStats[i]
		if indexStats.IndexName == "" {
			continue
		}
		prefix := fmt.Sprintf(
			"treedb.collections.write_domain.update_batch.collection.%s.index.%d.%s.",
			collectionStatsMetricToken(indexStats.CollectionName),
			indexStats.IndexOrdinal,
			collectionStatsMetricToken(indexStats.IndexName),
		)
		if indexStats.Unique {
			out[prefix+"unique"] = "1"
		} else {
			out[prefix+"unique"] = "0"
		}
		out[prefix+"changed_total"] = fmt.Sprintf("%d", indexStats.Changed)
		out[prefix+"unchanged_total"] = fmt.Sprintf("%d", indexStats.Unchanged)
		out[prefix+"unique_checks_total"] = fmt.Sprintf("%d", indexStats.UniqueChecks)
		out[prefix+"unique_check_skips_total"] = fmt.Sprintf("%d", indexStats.UniqueCheckSkips)
		out[prefix+"secondary_runs_total"] = fmt.Sprintf("%d", indexStats.SecondaryRuns)
		out[prefix+"secondary_deletes_total"] = fmt.Sprintf("%d", indexStats.SecondaryDeletes)
		out[prefix+"secondary_sets_total"] = fmt.Sprintf("%d", indexStats.SecondarySets)
		out[prefix+"secondary_key_bytes_total"] = fmt.Sprintf("%d", indexStats.SecondaryKeyBytes)
	}
	return out
}

func collectionStatsMetricToken(name string) string {
	original := strings.TrimSpace(name)
	name = strings.ToLower(original)
	var builder strings.Builder
	builder.Grow(len(name))
	lastUnderscore := false
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(builder.String(), "_")
	if out == "" {
		return "unnamed"
	}
	return fmt.Sprintf("%s_%016x", out, xxhash.Sum64String(original))
}

// StatsSnapshot returns aggregate process-local collection write-domain
// counters in typed form for tests and in-process diagnostics.
func (m *CollectionManager) StatsSnapshot() CollectionManagerStats {
	if m == nil {
		return CollectionManagerStats{}
	}
	type domainTarget struct {
		name   string
		domain *collectionWriteDomain
	}
	m.domainMu.RLock()
	domains := make([]domainTarget, 0, len(m.domains))
	for name, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domainTarget{name: name, domain: domain})
		}
	}
	m.domainMu.RUnlock()
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].name < domains[j].name
	})

	var stats CollectionManagerStats
	stats.Domains = len(domains)
	for _, target := range domains {
		stats.add(target.domain.statsSnapshot())
	}
	return stats
}

// ResetUpdateCombineQueueDepthMax clears the process-local update-combiner
// queue-depth maximum. It is intended for benchmark/profiling windows; it does
// not affect collection contents or pending writes.
func (m *CollectionManager) ResetUpdateCombineQueueDepthMax() {
	if m == nil {
		return
	}
	m.domainMu.RLock()
	domains := make([]*collectionWriteDomain, 0, len(m.domains))
	for _, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domain)
		}
	}
	m.domainMu.RUnlock()
	for _, domain := range domains {
		domain.updateCombineQueueDepthMax.Store(0)
	}
}

func (s *CollectionManagerStats) add(other CollectionManagerStats) {
	if s == nil {
		return
	}
	s.PendingDocuments = saturatingAddNonNegativeInt(s.PendingDocuments, other.PendingDocuments)
	s.PendingBytes = saturatingAddNonNegativeInt64(s.PendingBytes, other.PendingBytes)
	s.PendingRootRuns = saturatingAddNonNegativeInt(s.PendingRootRuns, other.PendingRootRuns)
	s.PendingIndexedFlushUnits = saturatingAddNonNegativeInt(s.PendingIndexedFlushUnits, other.PendingIndexedFlushUnits)
	s.PendingIndexedSemanticRecords = saturatingAddNonNegativeInt(s.PendingIndexedSemanticRecords, other.PendingIndexedSemanticRecords)
	s.OverlayMutableDocuments = saturatingAddNonNegativeInt(s.OverlayMutableDocuments, other.OverlayMutableDocuments)
	s.OverlayQueuedIndexedFlushUnits = saturatingAddNonNegativeInt(s.OverlayQueuedIndexedFlushUnits, other.OverlayQueuedIndexedFlushUnits)
	s.OverlayActiveIndexedFlushUnits = saturatingAddNonNegativeInt(s.OverlayActiveIndexedFlushUnits, other.OverlayActiveIndexedFlushUnits)
	s.OverlayVisibleDepth = saturatingAddNonNegativeInt(s.OverlayVisibleDepth, other.OverlayVisibleDepth)
	s.IndexedAsyncFlushRunning = saturatingAddNonNegativeInt(s.IndexedAsyncFlushRunning, other.IndexedAsyncFlushRunning)
	s.MutationLockCalls += other.MutationLockCalls
	s.MutationLockWait += other.MutationLockWait
	s.MutationLockHold += other.MutationLockHold
	s.IndexedStageBatches += other.IndexedStageBatches
	s.IndexedStageDocs += other.IndexedStageDocs
	s.IndexedStageBytes += other.IndexedStageBytes
	s.IndexedStageRootRuns += other.IndexedStageRootRuns
	s.IndexedSemanticRawRecords += other.IndexedSemanticRawRecords
	s.IndexedSemanticRawIndexDeltas += other.IndexedSemanticRawIndexDeltas
	s.IndexedSemanticFallbackRecords += other.IndexedSemanticFallbackRecords
	s.IndexedSemanticEffectiveRecords += other.IndexedSemanticEffectiveRecords
	s.IndexedAutoFlushes += other.IndexedAutoFlushes
	s.IndexedAsyncFlushScheduled += other.IndexedAsyncFlushScheduled
	s.IndexedAsyncFlushBackpressure += other.IndexedAsyncFlushBackpressure
	s.IndexedAsyncFlushWait += other.IndexedAsyncFlushWait
	s.IndexedAsyncFlushErrors += other.IndexedAsyncFlushErrors
	s.IndexedFlushCalls += other.IndexedFlushCalls
	s.IndexedFlushErrors += other.IndexedFlushErrors
	s.IndexedFlushForcedDrains += other.IndexedFlushForcedDrains
	s.IndexedFlushUnits += other.IndexedFlushUnits
	s.IndexedFlushRequeues += other.IndexedFlushRequeues
	s.IndexedFlushRequeuedUnits += other.IndexedFlushRequeuedUnits
	s.IndexedFlushLostOwnership += other.IndexedFlushLostOwnership
	s.IndexedFlushRootBaseMismatches += other.IndexedFlushRootBaseMismatches
	s.IndexedFlushDocs += other.IndexedFlushDocs
	s.IndexedFlushBytes += other.IndexedFlushBytes
	s.IndexedFlushRootRuns += other.IndexedFlushRootRuns
	s.IndexedFlushRoots += other.IndexedFlushRoots
	s.IndexedFlushDuration += other.IndexedFlushDuration
	s.IndexedFlushMaterialize += other.IndexedFlushMaterialize
	s.IndexedFlushPublish += other.IndexedFlushPublish
	s.RootDeltaPlanPrimaryRoots += other.RootDeltaPlanPrimaryRoots
	s.RootDeltaPlanTemplateRoots += other.RootDeltaPlanTemplateRoots
	s.RootDeltaPlanIndexStateRoots += other.RootDeltaPlanIndexStateRoots
	s.RootDeltaPlanSecondaryRoots += other.RootDeltaPlanSecondaryRoots
	s.RootDeltaPlanEntries += other.RootDeltaPlanEntries
	s.RootDeltaPlanKeyBytes += other.RootDeltaPlanKeyBytes
	s.RootDeltaPlanValueBytes += other.RootDeltaPlanValueBytes
	s.RootDeltaPlanTombstones += other.RootDeltaPlanTombstones
	s.PrimaryOnlyUpdateCalls += other.PrimaryOnlyUpdateCalls
	s.PrimaryOnlyMatched += other.PrimaryOnlyMatched
	s.PrimaryOnlyModified += other.PrimaryOnlyModified
	s.PrimaryOnlyBufferedCalls += other.PrimaryOnlyBufferedCalls
	s.PrimaryOnlyRootPublishes += other.PrimaryOnlyRootPublishes
	s.PrimaryOnlyRootDeltaEntries += other.PrimaryOnlyRootDeltaEntries
	s.PrimaryOnlyRootDeltaKeyBytes += other.PrimaryOnlyRootDeltaKeyBytes
	s.PrimaryOnlyRootDeltaValueBytes += other.PrimaryOnlyRootDeltaValueBytes
	s.PrimaryOnlyCoalescedDocs += other.PrimaryOnlyCoalescedDocs
	s.UpdateCombineRequests += other.UpdateCombineRequests
	s.UpdateCombineBatches += other.UpdateCombineBatches
	s.UpdateCombineBatchedRequests += other.UpdateCombineBatchedRequests
	s.UpdateCombineFallbackRequests += other.UpdateCombineFallbackRequests
	if other.UpdateCombineQueueDepthMax > s.UpdateCombineQueueDepthMax {
		s.UpdateCombineQueueDepthMax = other.UpdateCombineQueueDepthMax
	}
	s.UpdateBatchCalls += other.UpdateBatchCalls
	s.UpdateBatchItems += other.UpdateBatchItems
	s.UpdateBatchMatched += other.UpdateBatchMatched
	s.UpdateBatchModified += other.UpdateBatchModified
	s.UpdateBatchRuns += other.UpdateBatchRuns
	s.UpdateBatchBufferedBatches += other.UpdateBatchBufferedBatches
	s.UpdateBatchCurrentRead += other.UpdateBatchCurrentRead
	s.UpdateBatchCallback += other.UpdateBatchCallback
	s.UpdateBatchPrepareDocuments += other.UpdateBatchPrepareDocuments
	s.UpdateBatchIndexStateExtract += other.UpdateBatchIndexStateExtract
	s.UpdateBatchUniquePreflight += other.UpdateBatchUniquePreflight
	s.UpdateBatchTemplateRunBuild += other.UpdateBatchTemplateRunBuild
	s.UpdateBatchPrimaryRunBuild += other.UpdateBatchPrimaryRunBuild
	s.UpdateBatchIndexStateRunBuild += other.UpdateBatchIndexStateRunBuild
	s.UpdateBatchSecondaryRunBuild += other.UpdateBatchSecondaryRunBuild
	s.UpdateBatchBufferStage += other.UpdateBatchBufferStage
	s.UpdateBatchBufferPrecheck += other.UpdateBatchBufferPrecheck
	s.UpdateBatchBufferLockWait += other.UpdateBatchBufferLockWait
	s.UpdateBatchBufferLockHold += other.UpdateBatchBufferLockHold
	s.UpdateBatchBufferValidation += other.UpdateBatchBufferValidation
	s.UpdateBatchBufferRootScan += other.UpdateBatchBufferRootScan
	s.UpdateBatchBufferDomainPrepare += other.UpdateBatchBufferDomainPrepare
	s.UpdateBatchBufferPrimaryIdx += other.UpdateBatchBufferPrimaryIdx
	s.UpdateBatchBufferUniqueIdx += other.UpdateBatchBufferUniqueIdx
	s.UpdateBatchBufferRootAppend += other.UpdateBatchBufferRootAppend
	s.UpdateBatchBufferFlush += other.UpdateBatchBufferFlush
	s.UpdateBatchPublish += other.UpdateBatchPublish
	s.UpdateBatchSecondaryDeletes += other.UpdateBatchSecondaryDeletes
	s.UpdateBatchSecondarySets += other.UpdateBatchSecondarySets
	s.UpdateBatchSecondaryKeyBytes += other.UpdateBatchSecondaryKeyBytes
	s.UpdateBatchIndexValueChanges += other.UpdateBatchIndexValueChanges
	s.UpdateBatchIndexValueUnchanged += other.UpdateBatchIndexValueUnchanged
	s.UpdateBatchMaskFallbacks += other.UpdateBatchMaskFallbacks
	s.UpdateBatchUniqueChecks += other.UpdateBatchUniqueChecks
	s.UpdateBatchUniqueCheckSkips += other.UpdateBatchUniqueCheckSkips
	for i := 0; i < other.UpdateBatchIndexStatsCount && i < len(other.UpdateBatchIndexStats); i++ {
		mergeCollectionUpdateIndexStat(&s.UpdateBatchIndexStats, &s.UpdateBatchIndexStatsCount, other.UpdateBatchIndexStats[i])
	}
}

func (domain *collectionWriteDomain) statsSnapshot() CollectionManagerStats {
	if domain == nil {
		return CollectionManagerStats{}
	}
	var stats CollectionManagerStats
	domain.mu.RLock()
	stats.PendingDocuments = domain.count
	stats.PendingBytes = domain.bufferedBytes
	pendingRootRuns := bufferedIndexedRootRunCount(domain)
	stats.PendingRootRuns = pendingRootRuns
	stats.PendingIndexedFlushUnits = len(domain.indexedPublishingUnits) + len(domain.indexedFlushUnits)
	stats.PendingIndexedSemanticRecords = pendingIndexedSemanticRecordCountLocked(domain)
	stats.OverlayMutableDocuments = domain.mutableCount
	stats.OverlayQueuedIndexedFlushUnits = len(domain.indexedFlushUnits)
	stats.OverlayActiveIndexedFlushUnits = len(domain.indexedPublishingUnits)
	stats.OverlayVisibleDepth = collectionWriteDomainVisibleDepthLocked(domain)
	stats.UpdateBatchIndexStatsCount = len(domain.meta.Indexes)
	if stats.UpdateBatchIndexStatsCount > len(stats.UpdateBatchIndexStats) {
		stats.UpdateBatchIndexStatsCount = len(stats.UpdateBatchIndexStats)
	}
	for i := 0; i < stats.UpdateBatchIndexStatsCount; i++ {
		stats.UpdateBatchIndexStats[i].CollectionName = domain.meta.Name
		stats.UpdateBatchIndexStats[i].IndexName = domain.meta.Indexes[i].Name
		stats.UpdateBatchIndexStats[i].IndexOrdinal = i
		stats.UpdateBatchIndexStats[i].Unique = domain.meta.Indexes[i].Unique
	}
	domain.mu.RUnlock()
	if domain.indexedAsyncFlushRunning() {
		stats.IndexedAsyncFlushRunning = 1
	}

	stats.MutationLockCalls = domain.mutationLockCalls.Load()
	stats.MutationLockWait = durationFromAtomicNs(domain.mutationLockWaitTotalNs.Load())
	stats.MutationLockHold = durationFromAtomicNs(domain.mutationLockHoldTotalNs.Load())
	stats.IndexedStageBatches = domain.indexedStageBatches.Load()
	stats.IndexedStageDocs = domain.indexedStageDocs.Load()
	stats.IndexedStageBytes = domain.indexedStageBytes.Load()
	stats.IndexedStageRootRuns = domain.indexedStageRootRuns.Load()
	stats.IndexedSemanticRawRecords = domain.indexedSemanticRawRecords.Load()
	stats.IndexedSemanticRawIndexDeltas = domain.indexedSemanticRawIndexDeltas.Load()
	stats.IndexedSemanticFallbackRecords = domain.indexedSemanticFallbackRecords.Load()
	stats.IndexedSemanticEffectiveRecords = domain.indexedSemanticEffectiveRecords.Load()
	stats.IndexedAutoFlushes = domain.indexedAutoFlushes.Load()
	stats.IndexedAsyncFlushScheduled = domain.indexedAsyncFlushScheduled.Load()
	stats.IndexedAsyncFlushBackpressure = domain.indexedAsyncFlushBackpressure.Load()
	stats.IndexedAsyncFlushWait = durationFromAtomicNs(domain.indexedAsyncFlushWaitTotalNs.Load())
	stats.IndexedAsyncFlushErrors = domain.indexedAsyncFlushErrors.Load()
	stats.IndexedFlushCalls = domain.indexedFlushCalls.Load()
	stats.IndexedFlushErrors = domain.indexedFlushErrors.Load()
	stats.IndexedFlushForcedDrains = domain.indexedFlushForcedDrains.Load()
	stats.IndexedFlushUnits = domain.indexedFlushUnitsTotal.Load()
	stats.IndexedFlushRequeues = domain.indexedFlushRequeues.Load()
	stats.IndexedFlushRequeuedUnits = domain.indexedFlushRequeuedUnits.Load()
	stats.IndexedFlushLostOwnership = domain.indexedFlushLostOwnership.Load()
	stats.IndexedFlushRootBaseMismatches = domain.indexedFlushRootBaseMismatches.Load()
	stats.IndexedFlushDocs = domain.indexedFlushDocs.Load()
	stats.IndexedFlushBytes = domain.indexedFlushBytes.Load()
	stats.IndexedFlushRootRuns = domain.indexedFlushRootRuns.Load()
	stats.IndexedFlushRoots = domain.indexedFlushRoots.Load()
	stats.IndexedFlushDuration = durationFromAtomicNs(domain.indexedFlushDurationTotalNs.Load())
	stats.IndexedFlushMaterialize = durationFromAtomicNs(domain.indexedFlushMaterializeTotalNs.Load())
	stats.IndexedFlushPublish = durationFromAtomicNs(domain.indexedFlushPublishTotalNs.Load())
	stats.RootDeltaPlanPrimaryRoots = domain.rootDeltaPlanPrimaryRoots.Load()
	stats.RootDeltaPlanTemplateRoots = domain.rootDeltaPlanTemplateRoots.Load()
	stats.RootDeltaPlanIndexStateRoots = domain.rootDeltaPlanIndexStateRoots.Load()
	stats.RootDeltaPlanSecondaryRoots = domain.rootDeltaPlanSecondaryRoots.Load()
	stats.RootDeltaPlanEntries = domain.rootDeltaPlanEntries.Load()
	stats.RootDeltaPlanKeyBytes = domain.rootDeltaPlanKeyBytes.Load()
	stats.RootDeltaPlanValueBytes = domain.rootDeltaPlanValueBytes.Load()
	stats.RootDeltaPlanTombstones = domain.rootDeltaPlanTombstones.Load()
	stats.PrimaryOnlyUpdateCalls = domain.primaryOnlyUpdateCalls.Load()
	stats.PrimaryOnlyMatched = domain.primaryOnlyMatched.Load()
	stats.PrimaryOnlyModified = domain.primaryOnlyModified.Load()
	stats.PrimaryOnlyBufferedCalls = domain.primaryOnlyBufferedCalls.Load()
	stats.PrimaryOnlyRootPublishes = domain.primaryOnlyRootPublishes.Load()
	stats.PrimaryOnlyRootDeltaEntries = domain.primaryOnlyRootDeltaEntries.Load()
	stats.PrimaryOnlyRootDeltaKeyBytes = domain.primaryOnlyRootDeltaKeyBytes.Load()
	stats.PrimaryOnlyRootDeltaValueBytes = domain.primaryOnlyRootDeltaValueBytes.Load()
	stats.PrimaryOnlyCoalescedDocs = domain.primaryOnlyCoalescedDocs.Load()
	stats.UpdateCombineRequests = domain.updateCombineRequests.Load()
	stats.UpdateCombineBatches = domain.updateCombineBatches.Load()
	stats.UpdateCombineBatchedRequests = domain.updateCombineBatchedRequests.Load()
	stats.UpdateCombineFallbackRequests = domain.updateCombineFallbackRequests.Load()
	stats.UpdateCombineQueueDepthMax = domain.updateCombineQueueDepthMax.Load()
	stats.UpdateBatchCalls = domain.updateBatchCalls.Load()
	stats.UpdateBatchItems = domain.updateBatchItems.Load()
	stats.UpdateBatchMatched = domain.updateBatchMatched.Load()
	stats.UpdateBatchModified = domain.updateBatchModified.Load()
	stats.UpdateBatchRuns = domain.updateBatchRuns.Load()
	stats.UpdateBatchBufferedBatches = domain.updateBatchBufferedBatches.Load()
	stats.UpdateBatchCurrentRead = durationFromAtomicNs(domain.updateBatchCurrentReadNs.Load())
	stats.UpdateBatchCallback = durationFromAtomicNs(domain.updateBatchCallbackNs.Load())
	stats.UpdateBatchPrepareDocuments = durationFromAtomicNs(domain.updateBatchPrepareNs.Load())
	stats.UpdateBatchIndexStateExtract = durationFromAtomicNs(domain.updateBatchIndexStateNs.Load())
	stats.UpdateBatchUniquePreflight = durationFromAtomicNs(domain.updateBatchUniquePreflightNs.Load())
	stats.UpdateBatchTemplateRunBuild = durationFromAtomicNs(domain.updateBatchTemplateRunNs.Load())
	stats.UpdateBatchPrimaryRunBuild = durationFromAtomicNs(domain.updateBatchPrimaryRunNs.Load())
	stats.UpdateBatchIndexStateRunBuild = durationFromAtomicNs(domain.updateBatchIndexStateRunNs.Load())
	stats.UpdateBatchSecondaryRunBuild = durationFromAtomicNs(domain.updateBatchSecondaryRunNs.Load())
	stats.UpdateBatchBufferStage = durationFromAtomicNs(domain.updateBatchBufferStageNs.Load())
	stats.UpdateBatchBufferPrecheck = durationFromAtomicNs(domain.updateBatchBufferPrecheckNs.Load())
	stats.UpdateBatchBufferLockWait = durationFromAtomicNs(domain.updateBatchBufferLockWaitNs.Load())
	stats.UpdateBatchBufferLockHold = durationFromAtomicNs(domain.updateBatchBufferLockHoldNs.Load())
	stats.UpdateBatchBufferValidation = durationFromAtomicNs(domain.updateBatchBufferValidationNs.Load())
	stats.UpdateBatchBufferRootScan = durationFromAtomicNs(domain.updateBatchBufferRootScanNs.Load())
	stats.UpdateBatchBufferDomainPrepare = durationFromAtomicNs(domain.updateBatchBufferDomainPrepareNs.Load())
	stats.UpdateBatchBufferPrimaryIdx = durationFromAtomicNs(domain.updateBatchBufferPrimaryIdxNs.Load())
	stats.UpdateBatchBufferUniqueIdx = durationFromAtomicNs(domain.updateBatchBufferUniqueIdxNs.Load())
	stats.UpdateBatchBufferRootAppend = durationFromAtomicNs(domain.updateBatchBufferRootAppendNs.Load())
	stats.UpdateBatchBufferFlush = durationFromAtomicNs(domain.updateBatchBufferFlushNs.Load())
	stats.UpdateBatchPublish = durationFromAtomicNs(domain.updateBatchPublishNs.Load())
	stats.UpdateBatchSecondaryDeletes = domain.updateBatchSecondaryDeletes.Load()
	stats.UpdateBatchSecondarySets = domain.updateBatchSecondarySets.Load()
	stats.UpdateBatchSecondaryKeyBytes = domain.updateBatchSecondaryKeyBytes.Load()
	stats.UpdateBatchIndexValueChanges = domain.updateBatchIndexValueChanges.Load()
	stats.UpdateBatchIndexValueUnchanged = domain.updateBatchIndexValueUnchanged.Load()
	stats.UpdateBatchMaskFallbacks = domain.updateBatchMaskFallbacks.Load()
	stats.UpdateBatchUniqueChecks = domain.updateBatchUniqueChecks.Load()
	stats.UpdateBatchUniqueCheckSkips = domain.updateBatchUniqueCheckSkips.Load()
	for i := 0; i < stats.UpdateBatchIndexStatsCount; i++ {
		stats.UpdateBatchIndexStats[i].Changed = collectionStatsUint64ToInt(domain.updateBatchIndexChanged[i].Load())
		stats.UpdateBatchIndexStats[i].Unchanged = collectionStatsUint64ToInt(domain.updateBatchIndexUnchanged[i].Load())
		stats.UpdateBatchIndexStats[i].UniqueChecks = collectionStatsUint64ToInt(domain.updateBatchIndexUniqueChecks[i].Load())
		stats.UpdateBatchIndexStats[i].UniqueCheckSkips = collectionStatsUint64ToInt(domain.updateBatchIndexUniqueSkips[i].Load())
		stats.UpdateBatchIndexStats[i].SecondaryRuns = collectionStatsUint64ToInt(domain.updateBatchIndexSecondaryRuns[i].Load())
		stats.UpdateBatchIndexStats[i].SecondaryDeletes = collectionStatsUint64ToInt(domain.updateBatchIndexSecondaryDeletes[i].Load())
		stats.UpdateBatchIndexStats[i].SecondarySets = collectionStatsUint64ToInt(domain.updateBatchIndexSecondarySets[i].Load())
		stats.UpdateBatchIndexStats[i].SecondaryKeyBytes = collectionStatsUint64ToInt(domain.updateBatchIndexSecondaryBytes[i].Load())
	}
	return stats
}

func collectionWriteDomainVisibleDepthLocked(domain *collectionWriteDomain) int {
	if domain == nil {
		return 0
	}
	depth := len(domain.indexedPublishingUnits) + len(domain.indexedFlushUnits)
	if len(domain.rootRuns) > 0 || domain.mutableCount > 0 || domain.rootRunCount > 0 {
		depth++
	}
	return depth
}

func pendingIndexedSemanticRecordCountLocked(domain *collectionWriteDomain) int {
	if domain == nil {
		return 0
	}
	total := len(domain.indexedSemanticRecords)
	for _, unit := range domain.indexedPublishingUnits {
		total = saturatingAddNonNegativeInt(total, len(unit.semanticRecords))
	}
	for _, unit := range domain.indexedFlushUnits {
		total = saturatingAddNonNegativeInt(total, len(unit.semanticRecords))
	}
	return total
}

func indexedSemanticRecordIndexDeltaCount(records []indexedSemanticRecord) int {
	total := 0
	for _, record := range records {
		total = saturatingAddNonNegativeInt(total, len(record.indexDeltas))
	}
	return total
}

func indexedSemanticRecordFallbackCount(records []indexedSemanticRecord) int {
	total := 0
	for _, record := range records {
		if record.fallback != indexedSemanticFallbackNone {
			total = saturatingAddNonNegativeInt(total, 1)
		}
	}
	return total
}

func collectionStatsUint64ToInt(v uint64) int {
	if v > uint64(maxCollectionInt) {
		return maxCollectionInt
	}
	return int(v)
}

func durationFromAtomicNs(ns uint64) time.Duration {
	const maxDurationNs = uint64(1<<63 - 1)
	if ns > maxDurationNs {
		return time.Duration(maxDurationNs)
	}
	return time.Duration(ns)
}

func durationToAtomicNs(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return uint64(d.Nanoseconds())
}

func atomicMaxUint64(value *atomic.Uint64, candidate uint64) {
	if value == nil {
		return
	}
	for {
		current := value.Load()
		if candidate <= current || value.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func (domain *collectionWriteDomain) observeMutationLock(wait, hold time.Duration) {
	if domain == nil {
		return
	}
	domain.mutationLockCalls.Add(1)
	domain.mutationLockWaitTotalNs.Add(durationToAtomicNs(wait))
	domain.mutationLockHoldTotalNs.Add(durationToAtomicNs(hold))
}

func (domain *collectionWriteDomain) observeUpdateBatchStats(stats CollectionUpdateStats) {
	if domain == nil {
		return
	}
	domain.updateBatchCalls.Add(1)
	if stats.Items > 0 {
		domain.updateBatchItems.Add(uint64(stats.Items))
	}
	if stats.Matched > 0 {
		domain.updateBatchMatched.Add(uint64(stats.Matched))
	}
	if stats.Modified > 0 {
		domain.updateBatchModified.Add(uint64(stats.Modified))
	}
	if stats.Runs > 0 {
		domain.updateBatchRuns.Add(uint64(stats.Runs))
	}
	if stats.BufferedBatches > 0 {
		domain.updateBatchBufferedBatches.Add(uint64(stats.BufferedBatches))
	}
	domain.updateBatchCurrentReadNs.Add(durationToAtomicNs(stats.CurrentRead))
	domain.updateBatchCallbackNs.Add(durationToAtomicNs(stats.Callback))
	domain.updateBatchPrepareNs.Add(durationToAtomicNs(stats.PrepareDocuments))
	domain.updateBatchIndexStateNs.Add(durationToAtomicNs(stats.IndexStateExtraction))
	domain.updateBatchUniquePreflightNs.Add(durationToAtomicNs(stats.UniqueIndexPreflight))
	domain.updateBatchTemplateRunNs.Add(durationToAtomicNs(stats.TemplateRunBuild))
	domain.updateBatchPrimaryRunNs.Add(durationToAtomicNs(stats.PrimaryRunBuild))
	domain.updateBatchIndexStateRunNs.Add(durationToAtomicNs(stats.IndexStateRunBuild))
	domain.updateBatchSecondaryRunNs.Add(durationToAtomicNs(stats.SecondaryRunBuild))
	domain.updateBatchBufferStageNs.Add(durationToAtomicNs(stats.BufferStage))
	if collectionUpdateStatsHasBufferStageBreakdown(stats) {
		domain.updateBatchBufferPrecheckNs.Add(durationToAtomicNs(stats.BufferStagePrecheck))
		domain.updateBatchBufferLockWaitNs.Add(durationToAtomicNs(stats.BufferStageLockWait))
		domain.updateBatchBufferLockHoldNs.Add(durationToAtomicNs(stats.BufferStageLockHold))
		domain.updateBatchBufferValidationNs.Add(durationToAtomicNs(stats.BufferStageValidation))
		domain.updateBatchBufferRootScanNs.Add(durationToAtomicNs(stats.BufferStageRootScan))
		domain.updateBatchBufferDomainPrepareNs.Add(durationToAtomicNs(stats.BufferStageDomainPrepare))
		domain.updateBatchBufferPrimaryIdxNs.Add(durationToAtomicNs(stats.BufferStagePrimaryIdx))
		domain.updateBatchBufferUniqueIdxNs.Add(durationToAtomicNs(stats.BufferStageUniqueIdx))
		domain.updateBatchBufferRootAppendNs.Add(durationToAtomicNs(stats.BufferStageRootAppend))
		domain.updateBatchBufferFlushNs.Add(durationToAtomicNs(stats.BufferStageFlush))
	}
	domain.updateBatchPublishNs.Add(durationToAtomicNs(stats.Publish))
	if stats.SecondaryDeleteEntries > 0 {
		domain.updateBatchSecondaryDeletes.Add(uint64(stats.SecondaryDeleteEntries))
	}
	if stats.SecondarySetEntries > 0 {
		domain.updateBatchSecondarySets.Add(uint64(stats.SecondarySetEntries))
	}
	if stats.SecondaryKeyBytes > 0 {
		domain.updateBatchSecondaryKeyBytes.Add(uint64(stats.SecondaryKeyBytes))
	}
	if stats.IndexValueChanges > 0 {
		domain.updateBatchIndexValueChanges.Add(uint64(stats.IndexValueChanges))
	}
	if stats.IndexValueUnchanged > 0 {
		domain.updateBatchIndexValueUnchanged.Add(uint64(stats.IndexValueUnchanged))
	}
	if stats.MaskFallbacks > 0 {
		domain.updateBatchMaskFallbacks.Add(uint64(stats.MaskFallbacks))
	}
	if stats.UniqueIndexChecks > 0 {
		domain.updateBatchUniqueChecks.Add(uint64(stats.UniqueIndexChecks))
	}
	if stats.UniqueIndexCheckSkips > 0 {
		domain.updateBatchUniqueCheckSkips.Add(uint64(stats.UniqueIndexCheckSkips))
	}
	if stats.IndexStatsCount > 0 {
		for i := 0; i < stats.IndexStatsCount && i < len(stats.IndexStats); i++ {
			indexStats := stats.IndexStats[i]
			atomicAddNonNegativeInt(&domain.updateBatchIndexChanged[i], indexStats.Changed)
			atomicAddNonNegativeInt(&domain.updateBatchIndexUnchanged[i], indexStats.Unchanged)
			atomicAddNonNegativeInt(&domain.updateBatchIndexUniqueChecks[i], indexStats.UniqueChecks)
			atomicAddNonNegativeInt(&domain.updateBatchIndexUniqueSkips[i], indexStats.UniqueCheckSkips)
			atomicAddNonNegativeInt(&domain.updateBatchIndexSecondaryRuns[i], indexStats.SecondaryRuns)
			atomicAddNonNegativeInt(&domain.updateBatchIndexSecondaryDeletes[i], indexStats.SecondaryDeletes)
			atomicAddNonNegativeInt(&domain.updateBatchIndexSecondarySets[i], indexStats.SecondarySets)
			atomicAddNonNegativeInt(&domain.updateBatchIndexSecondaryBytes[i], indexStats.SecondaryKeyBytes)
		}
	}
}

func atomicAddNonNegativeInt(counter *atomic.Uint64, n int) {
	if counter == nil || n <= 0 {
		return
	}
	counter.Add(uint64(n))
}

func mergeCollectionUpdateIndexStat(dst *[maxCollectionUpdateInlineIndexStats]CollectionUpdateIndexStats, dstCount *int, src CollectionUpdateIndexStats) {
	if dst == nil || dstCount == nil || src.IndexName == "" {
		return
	}
	for i := 0; i < *dstCount && i < len(dst); i++ {
		if sameCollectionUpdateIndexStat(dst[i], src) {
			dst[i].Unique = src.Unique
			dst[i].Changed = saturatingAddNonNegativeInt(dst[i].Changed, src.Changed)
			dst[i].Unchanged = saturatingAddNonNegativeInt(dst[i].Unchanged, src.Unchanged)
			dst[i].UniqueChecks = saturatingAddNonNegativeInt(dst[i].UniqueChecks, src.UniqueChecks)
			dst[i].UniqueCheckSkips = saturatingAddNonNegativeInt(dst[i].UniqueCheckSkips, src.UniqueCheckSkips)
			dst[i].SecondaryRuns = saturatingAddNonNegativeInt(dst[i].SecondaryRuns, src.SecondaryRuns)
			dst[i].SecondaryDeletes = saturatingAddNonNegativeInt(dst[i].SecondaryDeletes, src.SecondaryDeletes)
			dst[i].SecondarySets = saturatingAddNonNegativeInt(dst[i].SecondarySets, src.SecondarySets)
			dst[i].SecondaryKeyBytes = saturatingAddNonNegativeInt(dst[i].SecondaryKeyBytes, src.SecondaryKeyBytes)
			return
		}
	}
	if *dstCount >= len(dst) {
		return
	}
	dst[*dstCount] = src
	*dstCount = *dstCount + 1
}

func sameCollectionUpdateIndexStat(a, b CollectionUpdateIndexStats) bool {
	return a.CollectionName == b.CollectionName &&
		a.IndexOrdinal == b.IndexOrdinal &&
		a.IndexName == b.IndexName
}

func collectionUpdateStatsHasBufferStageBreakdown(stats CollectionUpdateStats) bool {
	return stats.BufferStagePrecheck != 0 ||
		stats.BufferStageLockWait != 0 ||
		stats.BufferStageLockHold != 0 ||
		stats.BufferStageValidation != 0 ||
		stats.BufferStageRootScan != 0 ||
		stats.BufferStageDomainPrepare != 0 ||
		stats.BufferStagePrimaryIdx != 0 ||
		stats.BufferStageUniqueIdx != 0 ||
		stats.BufferStageRootAppend != 0 ||
		stats.BufferStageFlush != 0
}

func (domain *collectionWriteDomain) observeIndexedStage(docs int, bytes int64, rootRuns int) {
	if domain == nil {
		return
	}
	domain.indexedStageBatches.Add(1)
	if docs > 0 {
		domain.indexedStageDocs.Add(uint64(docs))
	}
	if bytes > 0 {
		domain.indexedStageBytes.Add(uint64(bytes))
	}
	if rootRuns > 0 {
		domain.indexedStageRootRuns.Add(uint64(rootRuns))
	}
}

func (domain *collectionWriteDomain) observeIndexedSemanticRawRecords(records []indexedSemanticRecord) {
	if domain == nil || len(records) == 0 {
		return
	}
	domain.indexedSemanticRawRecords.Add(uint64(len(records)))
	if deltas := indexedSemanticRecordIndexDeltaCount(records); deltas > 0 {
		domain.indexedSemanticRawIndexDeltas.Add(uint64(deltas))
	}
	if fallbacks := indexedSemanticRecordFallbackCount(records); fallbacks > 0 {
		domain.indexedSemanticFallbackRecords.Add(uint64(fallbacks))
	}
}

func (domain *collectionWriteDomain) beginIndexedAsyncFlush() bool {
	if domain == nil {
		return false
	}
	domain.indexedAsyncMu.Lock()
	defer domain.indexedAsyncMu.Unlock()
	if domain.indexedAsyncCond == nil {
		domain.indexedAsyncCond = sync.NewCond(&domain.indexedAsyncMu)
	}
	if domain.indexedAsyncRun {
		return false
	}
	domain.indexedAsyncRun = true
	return true
}

func (domain *collectionWriteDomain) finishIndexedAsyncFlush(err error) {
	if domain == nil {
		return
	}
	if err != nil {
		domain.indexedAsyncFlushErrors.Add(1)
	}
	domain.indexedAsyncMu.Lock()
	if err != nil {
		domain.indexedAsyncErr = err
	}
	domain.indexedAsyncRun = false
	if domain.indexedAsyncCond != nil {
		domain.indexedAsyncCond.Broadcast()
	}
	domain.indexedAsyncMu.Unlock()
}

func (domain *collectionWriteDomain) waitIndexedAsyncFlush() {
	if domain == nil {
		return
	}
	var waitStart time.Time
	domain.indexedAsyncMu.Lock()
	if domain.indexedAsyncCond == nil {
		domain.indexedAsyncCond = sync.NewCond(&domain.indexedAsyncMu)
	}
	for domain.indexedAsyncRun {
		if waitStart.IsZero() {
			waitStart = time.Now()
		}
		runCollectionWaitIndexedAsyncFlushHook()
		domain.indexedAsyncCond.Wait()
	}
	domain.indexedAsyncMu.Unlock()
	if !waitStart.IsZero() {
		domain.indexedAsyncFlushWaitTotalNs.Add(durationToAtomicNs(time.Since(waitStart)))
	}
}

func (domain *collectionWriteDomain) indexedAsyncFlushRunning() bool {
	if domain == nil {
		return false
	}
	domain.indexedAsyncMu.Lock()
	running := domain.indexedAsyncRun
	domain.indexedAsyncMu.Unlock()
	return running
}

func (domain *collectionWriteDomain) clearIndexedAsyncFlushError() {
	if domain == nil {
		return
	}
	domain.indexedAsyncMu.Lock()
	domain.indexedAsyncErr = nil
	domain.indexedAsyncMu.Unlock()
}

func (domain *collectionWriteDomain) consumeIndexedAsyncFlushError() error {
	if domain == nil {
		return nil
	}
	domain.indexedAsyncMu.Lock()
	err := domain.indexedAsyncErr
	domain.indexedAsyncErr = nil
	domain.indexedAsyncMu.Unlock()
	return err
}

var collectionWaitIndexedAsyncFlushHook struct {
	mu sync.Mutex
	fn func()
}

func setCollectionWaitIndexedAsyncFlushHookForTest(fn func()) func() {
	collectionWaitIndexedAsyncFlushHook.mu.Lock()
	prev := collectionWaitIndexedAsyncFlushHook.fn
	collectionWaitIndexedAsyncFlushHook.fn = fn
	collectionWaitIndexedAsyncFlushHook.mu.Unlock()
	return func() {
		collectionWaitIndexedAsyncFlushHook.mu.Lock()
		collectionWaitIndexedAsyncFlushHook.fn = prev
		collectionWaitIndexedAsyncFlushHook.mu.Unlock()
	}
}

func runCollectionWaitIndexedAsyncFlushHook() {
	collectionWaitIndexedAsyncFlushHook.mu.Lock()
	fn := collectionWaitIndexedAsyncFlushHook.fn
	collectionWaitIndexedAsyncFlushHook.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (domain *collectionWriteDomain) observeIndexedFlush(units, docs int, bytes int64, rootRuns, roots int, duration, materialize, publish time.Duration, err error) {
	if domain == nil {
		return
	}
	domain.indexedFlushCalls.Add(1)
	if err != nil {
		domain.indexedFlushErrors.Add(1)
	}
	if units > 0 {
		domain.indexedFlushUnitsTotal.Add(uint64(units))
	}
	if docs > 0 {
		domain.indexedFlushDocs.Add(uint64(docs))
	}
	if bytes > 0 {
		domain.indexedFlushBytes.Add(uint64(bytes))
	}
	if rootRuns > 0 {
		domain.indexedFlushRootRuns.Add(uint64(rootRuns))
	}
	if roots > 0 {
		domain.indexedFlushRoots.Add(uint64(roots))
	}
	domain.indexedFlushDurationTotalNs.Add(durationToAtomicNs(duration))
	domain.indexedFlushMaterializeTotalNs.Add(durationToAtomicNs(materialize))
	domain.indexedFlushPublishTotalNs.Add(durationToAtomicNs(publish))
}

func (domain *collectionWriteDomain) observeIndexedFlushForcedDrain() {
	if domain == nil {
		return
	}
	domain.indexedFlushForcedDrains.Add(1)
}

type collectionRootDeltaPlanStats struct {
	primaryRoots      uint64
	templateRoots     uint64
	indexStateRoots   uint64
	secondaryRoots    uint64
	entries           uint64
	keyBytes          uint64
	valueBytes        uint64
	tombstones        uint64
	primaryEntries    uint64
	primaryKeyBytes   uint64
	primaryValueBytes uint64
	primaryTombstones uint64
}

func (domain *collectionWriteDomain) observeRootDeltaPlan(stats collectionRootDeltaPlanStats) {
	if domain == nil || stats == (collectionRootDeltaPlanStats{}) {
		return
	}
	domain.rootDeltaPlanPrimaryRoots.Add(stats.primaryRoots)
	domain.rootDeltaPlanTemplateRoots.Add(stats.templateRoots)
	domain.rootDeltaPlanIndexStateRoots.Add(stats.indexStateRoots)
	domain.rootDeltaPlanSecondaryRoots.Add(stats.secondaryRoots)
	domain.rootDeltaPlanEntries.Add(stats.entries)
	domain.rootDeltaPlanKeyBytes.Add(stats.keyBytes)
	domain.rootDeltaPlanValueBytes.Add(stats.valueBytes)
	domain.rootDeltaPlanTombstones.Add(stats.tombstones)
}

func (domain *collectionWriteDomain) observePrimaryOnlyUpdate(matched, modified, published bool, deltaStats collectionRootDeltaPlanStats) {
	items := 1
	matchedCount := 0
	if matched {
		matchedCount = 1
	}
	modifiedCount := 0
	if modified {
		modifiedCount = 1
	}
	domain.observePrimaryOnlyUpdateBatch(items, matchedCount, modifiedCount, published, deltaStats)
}

func (domain *collectionWriteDomain) observePrimaryOnlyUpdateBatch(items, matched, modified int, published bool, deltaStats collectionRootDeltaPlanStats) {
	if domain == nil {
		return
	}
	if items > 0 {
		domain.primaryOnlyUpdateCalls.Add(uint64(items))
	}
	if matched > 0 {
		domain.primaryOnlyMatched.Add(uint64(matched))
	}
	if modified > 0 {
		domain.primaryOnlyModified.Add(uint64(modified))
	}
	if !published {
		return
	}
	domain.primaryOnlyRootPublishes.Add(1)
	domain.primaryOnlyRootDeltaEntries.Add(deltaStats.primaryEntries)
	domain.primaryOnlyRootDeltaKeyBytes.Add(deltaStats.primaryKeyBytes)
	domain.primaryOnlyRootDeltaValueBytes.Add(deltaStats.primaryValueBytes)
	if modified > 0 {
		domain.primaryOnlyCoalescedDocs.Add(uint64(modified))
	}
}

func (domain *collectionWriteDomain) observeUpdateCombineRequest(queueDepth int) {
	if domain == nil {
		return
	}
	domain.updateCombineRequests.Add(1)
	if queueDepth > 0 {
		atomicMaxUint64(&domain.updateCombineQueueDepthMax, uint64(queueDepth))
	}
}

func (domain *collectionWriteDomain) observeUpdateCombineBatch(requests int, fallback bool) {
	if domain == nil || requests <= 0 {
		return
	}
	domain.updateCombineBatches.Add(1)
	domain.updateCombineBatchedRequests.Add(uint64(requests))
	if fallback {
		domain.updateCombineFallbackRequests.Add(uint64(requests))
	}
}

func (m *CollectionManager) writeDomainForCollection(name string) *collectionWriteDomain {
	if m == nil {
		return nil
	}
	if m.closing.Load() {
		return nil
	}
	m.domainMu.Lock()
	defer m.domainMu.Unlock()
	if m.closing.Load() {
		return nil
	}
	if m.domains == nil {
		m.domains = make(map[string]*collectionWriteDomain)
	}
	if domain := m.domains[name]; domain != nil {
		return domain
	}
	domain := &collectionWriteDomain{}
	domain.updateBatchDetailedStats.Store(m.updateBatchDetailedStats.Load())
	m.domains[name] = domain
	return domain
}

func (m *CollectionManager) existingWriteDomainForCollection(name string) *collectionWriteDomain {
	if m == nil {
		return nil
	}
	if m.closing.Load() {
		return nil
	}
	m.domainMu.RLock()
	defer m.domainMu.RUnlock()
	if m.domains == nil {
		return nil
	}
	return m.domains[name]
}

// FlushAll publishes buffered writes for every collection opened through this
// manager. The backend DB also calls this as a close hook while write APIs are
// still available.
func (m *CollectionManager) FlushAll() error {
	if m == nil || m.db == nil {
		return nil
	}
	m.domainMu.RLock()
	domains := make([]*collectionWriteDomain, 0, len(m.domains))
	for _, domain := range m.domains {
		if domain != nil {
			domains = append(domains, domain)
		}
	}
	m.domainMu.RUnlock()

	var errs []error
	for _, domain := range domains {
		domain.waitIndexedAsyncFlush()
		if err := flushCollectionWriteDomain(m.db, domain); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func flushCollectionWriteDomain(db *backenddb.DB, domain *collectionWriteDomain) error {
	if db == nil || domain == nil {
		return nil
	}
	collection := &Collection{db: db, writeDomain: domain}
	unlockMutation := lockCollectionDomainMutation(domain)
	defer unlockMutation.Unlock()
	domain.waitIndexedAsyncFlush()
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if hasBufferedIndexedRootRuns(domain) {
		domain.observeIndexedFlushForcedDrain()
	}
	return collection.flushBufferedWritesLocked(domain)
}

func flushCollectionWriteDomainAsync(db *backenddb.DB, domain *collectionWriteDomain) error {
	if db == nil || domain == nil {
		return nil
	}
	collection := &Collection{db: db, writeDomain: domain}
	for {
		work, err := collection.prepareIndexedAsyncPublish()
		if err != nil || work == nil {
			return err
		}
		if err := collection.publishPreparedIndexedFlush(work); err != nil {
			return err
		}
		domain.mu.RLock()
		more := len(domain.indexedFlushUnits) > 0
		domain.mu.RUnlock()
		if !more {
			return nil
		}
	}
}

func (c *Collection) scheduleIndexedAsyncFlush(domain *collectionWriteDomain) bool {
	if c == nil || c.db == nil || domain == nil {
		return false
	}
	if !domain.beginIndexedAsyncFlush() {
		return false
	}
	domain.indexedAsyncFlushScheduled.Add(1)
	db := c.db
	go func() {
		var err error
		pprof.Do(context.Background(), indexedAsyncFlushPprofLabels, func(context.Context) {
			err = flushCollectionWriteDomainAsync(db, domain)
		})
		domain.finishIndexedAsyncFlush(err)
	}()
	return true
}

func (c *Collection) lockMutation() collectionMutationUnlock {
	if c == nil || c.writeDomain == nil {
		return collectionMutationUnlock{}
	}
	return lockCollectionDomainMutation(c.writeDomain)
}

type collectionMutationUnlock struct {
	domain    *collectionWriteDomain
	holdStart time.Time
	wait      time.Duration
}

func lockCollectionDomainMutation(domain *collectionWriteDomain) collectionMutationUnlock {
	if domain == nil {
		return collectionMutationUnlock{}
	}
	lockStart := time.Now()
	domain.mutationMu.Lock()
	holdStart := time.Now()
	wait := holdStart.Sub(lockStart)
	return collectionMutationUnlock{domain: domain, holdStart: holdStart, wait: wait}
}

func (unlock collectionMutationUnlock) Unlock() {
	if unlock.domain == nil {
		return
	}
	hold := time.Since(unlock.holdStart)
	unlock.domain.mutationMu.Unlock()
	unlock.domain.observeMutationLock(unlock.wait, hold)
}

func (m *CollectionManager) CreateCollection(meta *CollectionMeta) (*CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	if m.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if meta == nil {
		return nil, errors.New("collections: nil collection metadata")
	}
	normalized, err := normalizeCollectionMeta(*meta)
	if err != nil {
		return nil, err
	}

	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	existing, err := loadCollectionCatalog(snap, normalized.Name)
	_ = snap.Close()
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !sameCollectionMeta(existing.meta, normalized) {
			return nil, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		return existing.meta.copy(), nil
	}

	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		return nil, err
	}
	_, _, err = m.db.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		current := m.db.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		existing, err := loadCollectionCatalog(current, normalized.Name)
		if err != nil {
			return nil, err
		}
		if existing != nil && !sameCollectionMeta(existing.meta, normalized) {
			return nil, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		iter, err := buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
		})
		if err != nil {
			return nil, err
		}
		return iter, nil
	})
	if err != nil {
		return nil, err
	}
	return normalized.copy(), nil
}

func (m *CollectionManager) OpenCollection(name string) (*Collection, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	if m.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if err := ValidateCollectionName(name); err != nil {
		return nil, err
	}
	if collection, ok := m.openCollectionFromWriteDomainCache(name); ok {
		if m.db.IsClosing() {
			return nil, backenddb.ErrClosed
		}
		return collection, nil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	collection := &Collection{
		db:          m.db,
		writeDomain: m.writeDomainForCollection(catalog.meta.Name),
		meta:        copyCollectionMeta(catalog.meta),
	}
	if collection.writeDomain == nil {
		return nil, backenddb.ErrClosed
	}
	collection.rememberCatalog(snap, catalog)
	collection.noteWriteDomainCatalog(snapshotSystemRoot(snap), catalog)
	return collection, nil
}

func (m *CollectionManager) openCollectionFromWriteDomainCache(name string) (*Collection, bool) {
	if m == nil || m.isClosing() {
		return nil, false
	}
	state := m.db.State()
	if state == nil || state.SystemRootPageID == 0 {
		return nil, false
	}
	domain := m.existingWriteDomainForCollection(name)
	if domain == nil {
		return nil, false
	}
	catalog := cachedWriteDomainCatalogForState(domain, state.SystemRootPageID, state.CommitSeq)
	if catalog == nil {
		return nil, false
	}
	currentState := m.db.State()
	if currentState == nil ||
		currentState.SystemRootPageID != state.SystemRootPageID ||
		currentState.CommitSeq != state.CommitSeq {
		return nil, false
	}
	collection := &Collection{
		db:          m.db,
		writeDomain: domain,
		meta:        copyCollectionMeta(catalog.meta),
	}
	collection.rememberCatalogAtSystemRoot(state.SystemRootPageID, catalog)
	return collection, true
}

func (m *CollectionManager) ListCollections() ([]CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	if snap.State() == nil || snap.State().SystemRootPageID == 0 {
		return nil, nil
	}
	prefix := []byte(systemCollectionMetaPrefix)
	it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	var out []CollectionMeta
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if !it.IsDeleted() {
			meta, err := decodeCollectionMeta(it.ValueCopy(nil))
			if err != nil {
				return nil, err
			}
			out = append(out, meta)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (c *Collection) Name() string {
	if c == nil {
		return ""
	}
	return c.meta.Name
}

func (c *Collection) Meta() CollectionMeta {
	if c == nil {
		return CollectionMeta{}
	}
	return *c.meta.copy()
}

// SameCachedCatalog reports whether both handles were opened against the same
// collection catalog state.
func (c *Collection) SameCachedCatalog(other *Collection) bool {
	if c == nil || other == nil {
		return false
	}
	cName, cSystemRoot, cCommitSeq := c.cachedCatalogIdentity()
	otherName, otherSystemRoot, otherCommitSeq := other.cachedCatalogIdentity()
	return cName != "" &&
		cName == otherName &&
		cSystemRoot != 0 &&
		cSystemRoot == otherSystemRoot &&
		cCommitSeq == otherCommitSeq
}

func (c *Collection) cachedCatalogIdentity() (name string, systemRoot, commitSeq uint64) {
	if c == nil {
		return "", 0, 0
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	return c.meta.Name, c.catalogSystemRoot, c.catalogCommitSeq
}

func (c *Collection) CreateIndex(def IndexDefinition) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, err
	}

	baseMeta := catalog.meta
	c.meta = baseMeta
	baseOptions, err := collectionPlannerOptions(baseMeta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	baseOptions = collectionOptionsWithTemplateV1Resolver(baseOptions, snap, catalog)
	newMeta, normalizedDef, err := addIndexToCollectionMeta(baseMeta, def)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	newRuntime, err := singleIndexRuntime(normalizedDef)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	existingRuntimes, err := (insertBatchPlanner{
		collection: baseMeta.Name,
		indexes:    plannerIndexes(baseMeta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plan, err := buildCreateIndexBackfillPlan(snap, catalog, newRuntime, existingRuntimes, baseOptions)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(plan.tables)
	}()
	for i, rootName := range plan.rootNames {
		iter := plan.tables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      plan.baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: plan.policies[i],
		})
	}

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaAndRootDescriptorSystemIterator(baseMeta, newMeta, plan.rootNames, plan.baseRootIDs, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.rootNames) {
		return nil, unexpectedOrderedRootCountError(newMeta.Name, len(plan.rootNames), len(rootIDs))
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, plan.rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) DropIndex(name string) (*CollectionMeta, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	return c.dropIndexes(map[string]struct{}{name: {}}, false)
}

func (c *Collection) DropIndexes(names []string) (*CollectionMeta, error) {
	if len(names) == 0 {
		return nil, ErrIndexNotFound
	}
	nameSet := make(map[string]struct{}, len(names))
	for _, name := range names {
		if err := ValidateIndexName(name); err != nil {
			return nil, err
		}
		nameSet[name] = struct{}{}
	}
	return c.dropIndexes(nameSet, false)
}

func (c *Collection) DropAllIndexes() (*CollectionMeta, error) {
	return c.dropIndexes(nil, true)
}

func (c *Collection) dropIndexes(names map[string]struct{}, all bool) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, err
	}
	baseMeta := catalog.meta
	c.meta = baseMeta
	baseSystemRoot := snapshotSystemRoot(snap)
	_ = snap.Close()

	nextIndexes := make([]IndexDefinition, 0, len(baseMeta.Indexes))
	clearedRootNames := make([]string, 0, len(baseMeta.Indexes)+1)
	dropped := 0
	for _, idx := range baseMeta.Indexes {
		if all {
			dropped++
			clearedRootNames = append(clearedRootNames, collectionSecondaryRootName(baseMeta.Name, idx.Name))
			continue
		}
		if _, ok := names[idx.Name]; ok {
			dropped++
			clearedRootNames = append(clearedRootNames, collectionSecondaryRootName(baseMeta.Name, idx.Name))
			continue
		}
		nextIndexes = append(nextIndexes, idx)
	}
	if !all && dropped != len(names) {
		return nil, ErrIndexNotFound
	}
	if dropped == 0 {
		c.meta = baseMeta
		c.rememberCatalogAtSystemRoot(baseSystemRoot, catalog)
		return baseMeta.copy(), nil
	}

	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name:    baseMeta.Name,
		Options: baseMeta.Options,
		Indexes: nextIndexes,
	})
	if err != nil {
		return nil, err
	}
	if len(newMeta.Indexes) == 0 && persistIndexStateForDocumentFormat(baseMeta.Options.DocumentFormat) {
		clearedRootNames = append(clearedRootNames, collectionIndexStateRootName(baseMeta.Name))
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, clearedRootNames)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	clearedRootIDs := make([]uint64, len(clearedRootNames))
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, clearedRootNames, clearedRootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return newMeta.copy(), nil
}

func (c *Collection) Insert(id, document []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if len(c.meta.Indexes) == 0 {
		return c.insertOneNoIndexBuffered(id, document)
	}
	ids, err := c.InsertBatch([][]byte{id}, [][]byte{document})
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

// Flush publishes buffered collection-local writes to the backend roots. Single
// no-index inserts use this boundary to match TreeDB's cached write path while
// still giving callers an explicit durability/visibility point.
func (c *Collection) Flush() error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if c.writeDomain != nil {
		unlockMutation := c.lockMutation()
		defer unlockMutation.Unlock()
		c.writeDomain.waitIndexedAsyncFlush()
		return c.flushBufferedWrites()
	}
	return c.flushBufferedWrites()
}

// CompactRootOverlays folds durable collection root overlays into their base
// collection roots and clears the overlay descriptors in the same backend
// commit. It is an explicit maintenance boundary for the overlay-root write-back
// architecture: hot writes may publish durable overlays quickly, then maintenance
// can restore the simple one-root read shape.
func (c *Collection) CompactRootOverlays(ctx context.Context) (CollectionRootOverlayCompactionStats, error) {
	if c == nil {
		return CollectionRootOverlayCompactionStats{}, errCollectionNil
	}
	if c.db == nil {
		return CollectionRootOverlayCompactionStats{}, errCollectionDBNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if c.writeDomain != nil {
		c.writeDomain.waitIndexedAsyncFlush()
	}
	return c.compactRootOverlaysLocked(ctx)
}

func (c *Collection) compactRootOverlaysLocked(ctx context.Context) (CollectionRootOverlayCompactionStats, error) {
	var stats CollectionRootOverlayCompactionStats
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return stats, err
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return stats, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return stats, err
	}
	if catalog == nil {
		return stats, errCollectionNotFound
	}
	rootNames := catalog.overlayRootNames()
	if len(rootNames) == 0 {
		return stats, nil
	}
	baseRootIDs := make(map[string]uint64, len(rootNames))
	rootOverlays := make(map[string][]uint64, len(rootNames))
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(rootNames))
	cleanupIters := func() {
		for i := range ordered {
			if ordered[i].Iter != nil {
				_ = ordered[i].Iter.Close()
			}
		}
	}
	for _, rootName := range rootNames {
		if err := ctx.Err(); err != nil {
			cleanupIters()
			return stats, err
		}
		overlays := append([]uint64(nil), catalog.overlayRootIDs(rootName)...)
		if len(overlays) == 0 {
			continue
		}
		policy, err := collectionRootStoragePolicy(catalog.meta, rootName)
		if err != nil {
			cleanupIters()
			return stats, err
		}
		it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
		if err != nil {
			cleanupIters()
			return stats, err
		}
		if it == nil {
			it = &systemTargetIterator{}
		}
		baseRootIDs[rootName] = catalog.rootID(rootName)
		rootOverlays[rootName] = overlays
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      0,
			Iter:          it,
			StoragePolicy: policy,
		})
		stats.Roots++
		stats.OverlayRoots += len(overlays)
	}
	if len(ordered) == 0 {
		return stats, nil
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootOverlayCompactionSystemDeltaIteratorForMeta(catalog.meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootOverlays, rootIDs)
	})
	cleanupIters()
	if err != nil {
		return stats, err
	}
	if len(rootIDs) != len(rootNames) {
		return stats, unexpectedOrderedRootCountError(catalog.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, rootNames, rootIDs)
	c.meta = catalog.meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	return stats, nil
}

func (c *Collection) insertOneNoIndexBuffered(id, document []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	domain := c.writeDomain
	if domain == nil {
		return c.insertOneNoIndex(id, document)
	}

	domain.mu.Lock()
	catalog, plannerOptions, indexed, err := c.ensureWriteDomainLocked(domain)
	if err != nil {
		domain.mu.Unlock()
		return nil, err
	}
	if indexed || plannerOptions.documentFormat != DocumentFormatJSON {
		domain.mu.Unlock()
		return c.insertOneViaBatch(id, document)
	}
	if catalog == nil {
		domain.mu.Unlock()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	if domain.table == nil {
		domain.table = newCollectionRunTable(0)
	}
	if _, _, flags, found := domain.table.GetEntry(id); found && flags&node.FlagTombstone == 0 {
		domain.mu.Unlock()
		return nil, ErrDocumentExists
	}
	if domain.primaryRoot != 0 {
		exists, err := c.persistedDocumentExists(domain.primaryRoot, id)
		if err != nil {
			domain.mu.Unlock()
			return nil, err
		}
		if exists {
			domain.mu.Unlock()
			return nil, ErrDocumentExists
		}
	}
	domain.storagePolicy = plannerOptions.dataStoragePolicy
	domain.table.SetEntry(id, document, page.ValuePtr{}, node.FlagInline)
	domain.count++
	resultID := bytes.Clone(id)
	domain.mu.Unlock()
	return resultID, nil
}

func (c *Collection) ensureWriteDomainLocked(domain *collectionWriteDomain) (*collectionCatalog, collectionOptions, bool, error) {
	if domain == nil {
		return nil, collectionOptions{}, false, errors.New("collections: missing write domain")
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if domain.loaded && domain.count > 0 {
		catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
			return nil, collectionOptions{}, false, err
		}
		options, err := collectionPlannerOptions(catalog.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return catalog, options, len(catalog.meta.Indexes) > 0, nil
	}
	if domain.loaded && domain.count == 0 && domain.baseSystemRoot == currentSystemRoot && domain.baseCommitSeq == currentCommitSeq {
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(domain.catalog); err != nil {
			return nil, collectionOptions{}, false, err
		}
		options, err := collectionPlannerOptions(domain.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return domain.catalog, options, len(domain.meta.Indexes) > 0, nil
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, collectionOptions{}, false, backenddb.ErrClosed
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	if catalog := cachedWriteDomainCatalogForStateLocked(domain, baseSystemRoot, baseCommitSeq); catalog != nil {
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
			_ = snap.Close()
			return nil, collectionOptions{}, false, err
		}
		c.rememberCatalog(snap, catalog)
		_ = snap.Close()
		options, err := collectionPlannerOptions(catalog.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return catalog, options, len(catalog.meta.Indexes) > 0, nil
	}
	name := c.meta.Name
	if domain.meta.Name != "" {
		name = domain.meta.Name
	}
	catalog, err := loadCollectionCatalog(snap, name)
	if err != nil {
		_ = snap.Close()
		return nil, collectionOptions{}, false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, collectionOptions{}, false, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, collectionOptions{}, false, err
	}
	c.rememberCatalog(snap, catalog)
	_ = snap.Close()

	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, collectionOptions{}, false, err
	}
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(rootName)
	domain.storagePolicy = options.dataStoragePolicy
	return catalog, options, len(catalog.meta.Indexes) > 0, nil
}

func (c *Collection) revalidateBufferedWriteDomainLocked(domain *collectionWriteDomain, currentCommitSeq, currentSystemRoot uint64) (*collectionCatalog, error) {
	if c == nil || c.db == nil {
		return nil, errCollectionDBNil
	}
	if domain == nil {
		return nil, errors.New("collections: missing write domain")
	}
	if domain.catalog == nil {
		return nil, errCollectionNotFound
	}
	if domain.baseSystemRoot == currentSystemRoot && domain.baseCommitSeq == currentCommitSeq {
		return domain.catalog, nil
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(snap, domain.meta.Name)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	_ = snap.Close()
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		return nil, err
	}
	if !sameCollectionMeta(catalog.meta, domain.meta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", domain.meta.Name)
	}

	primaryRootName := collectionPrimaryRootName(domain.meta.Name)
	if hasBufferedIndexedRootRuns(domain) {
		if err := forEachPendingIndexedRootBaseIDLocked(domain, func(rootName string, baseRootID uint64) error {
			if rootID := catalog.rootID(rootName); rootID != baseRootID {
				return errConcurrentRootModification(domain.meta.Name, rootName)
			}
			if !uint64SlicesEqual(catalog.overlayRootIDs(rootName), domain.catalog.overlayRootIDs(rootName)) {
				return errConcurrentRootModification(domain.meta.Name, rootName)
			}
			return nil
		}); err != nil {
			if errors.Is(err, ErrConcurrentMutation) {
				domain.indexedFlushRootBaseMismatches.Add(1)
			}
			return nil, err
		}
	} else {
		if rootID := catalog.rootID(primaryRootName); rootID != domain.primaryRoot {
			return nil, errConcurrentRootModification(domain.meta.Name, primaryRootName)
		}
		if !uint64SlicesEqual(catalog.overlayRootIDs(primaryRootName), domain.catalog.overlayRootIDs(primaryRootName)) {
			return nil, errConcurrentRootModification(domain.meta.Name, primaryRootName)
		}
	}
	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, err
	}
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(primaryRootName)
	domain.storagePolicy = options.dataStoragePolicy
	c.meta = catalog.meta
	c.rememberCatalogAtSystemRoot(baseSystemRoot, catalog)
	return catalog, nil
}

func (c *Collection) persistedDocumentExists(rootID uint64, id []byte) (bool, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	if _, err := snap.GetEntryAtRoot(rootID, id); err == nil {
		return true, nil
	} else if !errors.Is(err, tree.ErrKeyNotFound) {
		return false, err
	}
	return false, nil
}

func (c *Collection) flushBufferedNoIndex() error {
	domain := c.writeDomain
	if domain == nil {
		return nil
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if hasBufferedIndexedRootRuns(domain) {
		return nil
	}
	return c.flushBufferedNoIndexLocked(domain)
}

func (c *Collection) flushBufferedWrites() error {
	domain := c.writeDomain
	if domain == nil {
		return nil
	}
	for {
		domain.waitIndexedAsyncFlush()
		domain.mu.Lock()
		if domain.indexedAsyncFlushRunning() {
			domain.mu.Unlock()
			continue
		}
		if hasBufferedIndexedRootRuns(domain) {
			domain.observeIndexedFlushForcedDrain()
		}
		err := c.flushBufferedWritesLocked(domain)
		domain.mu.Unlock()
		return err
	}
}

func (c *Collection) flushBufferedWritesLocked(domain *collectionWriteDomain) error {
	if domain == nil {
		return nil
	}
	if domain.count == 0 {
		return domain.consumeIndexedAsyncFlushError()
	}
	var err error
	if hasBufferedIndexedRootRuns(domain) {
		err = c.flushBufferedIndexedLocked(domain)
	} else {
		err = c.flushBufferedNoIndexLocked(domain)
	}
	if err == nil {
		domain.clearIndexedAsyncFlushError()
	}
	return err
}

func (c *Collection) flushBufferedNoIndexLocked(domain *collectionWriteDomain) error {
	if domain == nil || domain.count == 0 || domain.table == nil {
		return nil
	}
	if domain.catalog == nil {
		return errCollectionNotFound
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return err
	}
	meta := catalog.meta
	if len(meta.Indexes) > 0 {
		return errors.New("collections: buffered no-index writes cannot be flushed into indexed schema")
	}
	c.meta = meta
	rootName := collectionPrimaryRootName(meta.Name)
	baseRoot := domain.primaryRoot
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return backenddb.ErrClosed
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()
	pinnedCatalog, err := loadCollectionCatalog(pin, meta.Name)
	if err != nil {
		return err
	}
	if pinnedCatalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(pinnedCatalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	if got := pinnedCatalog.rootID(rootName); got != baseRoot {
		return errConcurrentRootModification(meta.Name, rootName)
	}
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	table := domain.table
	iter := table.NewIterator(nil, nil)

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: domain.storagePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	_ = iter.Close()
	if err != nil {
		return err
	}
	if len(rootIDs) != 1 {
		return unexpectedOrderedRootCountError(meta.Name, 1, len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(domain.catalog, meta, []string{rootName}, rootIDs)
	domain.loaded = true
	domain.meta = meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = rootIDs[0]
	domain.table = newCollectionRunTable(0)
	domain.count = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetCollectionRunTable(table)
	return nil
}

func (c *Collection) shouldBufferIndexedInserts(meta CollectionMeta) bool {
	return c != nil && c.writeDomain != nil && meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0
}

func (c *Collection) shouldBufferIndexedInsertBatch(meta CollectionMeta, documentCount int) bool {
	if !c.shouldBufferIndexedInserts(meta) {
		return false
	}
	if documentCount >= DefaultIndexedWriteMemtableDirectBatchDocuments &&
		isDefaultIndexedWriteMemtableMaxDocuments(meta.Options) {
		return false
	}
	return true
}

func isDefaultIndexedWriteMemtableMaxDocuments(opts CollectionOptions) bool {
	if opts.BufferedIndexedWriteMaxDocuments == DefaultIndexedWriteMemtableMaxDocuments {
		return true
	}
	return opts.BufferedIndexedAsyncFlush &&
		opts.BufferedIndexedWriteMaxDocuments == DefaultIndexedWriteMemtableAsyncFlushMaxDocuments
}

func (c *Collection) bufferIndexedInsertPlanLocked(catalog *collectionCatalog, baseCommitSeq, baseSystemRoot uint64, plan *insertBatchPlan) (time.Duration, error) {
	domain := c.writeDomain
	if domain == nil {
		return 0, errors.New("collections: missing write domain")
	}
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if catalog == nil {
		return 0, errCollectionNotFound
	}
	if len(catalog.meta.Indexes) == 0 {
		return 0, errors.New("collections: indexed write buffer requires an indexed schema")
	}
	if domain.count > 0 {
		currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
		currentCatalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
		if err != nil {
			return 0, err
		}
		if !sameCollectionMeta(currentCatalog.meta, catalog.meta) {
			return 0, fmt.Errorf("collections: concurrent schema modification detected for %q", catalog.meta.Name)
		}
		if err := forEachPendingIndexedRootBaseIDLocked(domain, func(rootName string, baseRoot uint64) error {
			if got := currentCatalog.rootID(rootName); got != baseRoot {
				return errConcurrentRootModification(catalog.meta.Name, rootName)
			}
			return nil
		}); err != nil {
			return 0, err
		}
		catalog = currentCatalog
	} else {
		c.initializeWriteDomainFromCatalogLocked(domain, catalog, baseCommitSeq, baseSystemRoot)
	}

	if err := c.rejectBufferedIndexedInsertConflictsLocked(domain, catalog.meta, plan); err != nil {
		return 0, err
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(plan.runs))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(plan.runs))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(plan.runs))
	}
	if domain.uniqueValueRuns == nil {
		domain.uniqueValueRuns = make(map[string][]memtable.Table)
	}
	if domain.uniqueValueIndex == nil {
		domain.uniqueValueIndex = make(map[string]*bufferedUniqueValueIndex)
	}
	autoFlushEnabled := bufferedIndexedAutoFlushEnabled(catalog.meta.Options)
	freezeMutableIndexedRunMapsLocked(domain)
	var checkpoint bufferedIndexedCheckpoint
	if autoFlushEnabled {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	uniqueIndexes := uniqueCollectionIndexNames(catalog.meta)
	var stagedBytes int64
	stagedRootRuns := 0
	for _, run := range plan.runs {
		var uniqueValueTable memtable.Table
		var uniquePrefixes [][]byte
		if _, ok := uniqueIndexes[run.indexName]; ok && run.kind == collectionRootSecondary {
			var err error
			uniqueValueTable, uniquePrefixes, err = bufferedUniqueIndexValueRun(run.table, run.indexValueType)
			if err != nil {
				if autoFlushEnabled {
					rollbackBufferedIndexedDomain(domain, checkpoint)
				}
				return 0, err
			}
		}
		baseRoot := catalog.rootID(run.name)
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, run.name); ok && pendingBaseRoot != baseRoot {
			if autoFlushEnabled {
				rollbackBufferedIndexedDomain(domain, checkpoint)
			}
			return 0, errConcurrentRootModification(catalog.meta.Name, run.name)
		}
		if _, ok := domain.rootBaseIDs[run.name]; !ok {
			domain.rootBaseIDs[run.name] = baseRoot
		}
		domain.rootPolicies[run.name] = run.storagePolicy
		domain.rootRuns[run.name] = append(domain.rootRuns[run.name], run.table)
		domain.rootRunCount = saturatingAddNonNegativeInt(domain.rootRunCount, 1)
		stagedRootRuns++
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, run.table.Size())
		if run.kind == collectionRootPrimary {
			if domain.primaryIDIndex == nil {
				domain.primaryIDIndex = newBufferedUniqueValueIndex(max(1, run.table.Len()))
			}
			if err := addBufferedPrimaryIDs(domain.primaryIDIndex, run.table); err != nil {
				if autoFlushEnabled {
					rollbackBufferedIndexedDomain(domain, checkpoint)
				}
				return 0, err
			}
			if domain.primaryRunIndex != nil {
				if err := addBufferedPrimaryRunIndexEntries(domain.primaryRunIndex, run.table); err != nil {
					domain.primaryRunIndex = nil
					if autoFlushEnabled {
						rollbackBufferedIndexedDomain(domain, checkpoint)
					}
					return 0, err
				}
			}
		}
		if uniqueValueTable != nil {
			domain.uniqueValueRuns[run.indexName] = append(domain.uniqueValueRuns[run.indexName], uniqueValueTable)
			index := domain.uniqueValueIndex[run.indexName]
			if index == nil {
				index = newBufferedUniqueValueIndex(max(1, len(uniquePrefixes)))
				domain.uniqueValueIndex[run.indexName] = index
			}
			index.addAll(uniquePrefixes)
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, uniqueValueTable.Size())
		}
	}
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.count += len(plan.resultIDs)
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, len(plan.resultIDs))
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, stagedBytes)
	domain.writeGeneration++
	domain.observeIndexedStage(len(plan.resultIDs), stagedBytes, stagedRootRuns)
	c.meta = catalog.meta
	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, catalog.meta.Options)
	if err != nil {
		if autoFlushEnabled {
			rollbackBufferedIndexedDomain(domain, checkpoint)
		}
		return 0, err
	}
	if shouldFlushBufferedIndexedWrites(domain, catalog.meta.Options) {
		flushElapsed, _, _, err := c.flushBufferedIndexedAfterThresholdLocked(domain, catalog.meta.Options)
		if err != nil {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			return 0, err
		}
		resetCollectionTables(compactedObsolete)
		return flushElapsed, nil
	}
	resetCollectionTables(compactedObsolete)
	return 0, nil
}

func (c *Collection) initializeWriteDomainFromCatalogLocked(domain *collectionWriteDomain, catalog *collectionCatalog, baseCommitSeq, baseSystemRoot uint64) {
	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.indexedPublishingUnits = nil
	domain.indexedFlushUnits = nil
	domain.rootRuns = nil
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.rootValueArenas = nil
	domain.indexedSemanticRecords = nil
	domain.rootRunCount = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	domain.bufferedBytes = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
}

func shouldFlushBufferedIndexedWrites(domain *collectionWriteDomain, opts CollectionOptions) bool {
	if domain == nil || domain.count == 0 {
		return false
	}
	count := domain.count
	bytes := domain.bufferedBytes
	rootRuns := bufferedIndexedRootRunCount(domain)
	if opts.BufferedIndexedAsyncFlush {
		if len(domain.rootRuns) == 0 {
			return false
		}
		count = domain.mutableCount
		bytes = domain.mutableBytes
		rootRuns = domain.rootRunCount
	}
	if opts.BufferedIndexedWriteMaxDocuments > 0 && count >= opts.BufferedIndexedWriteMaxDocuments {
		return true
	}
	if opts.BufferedIndexedWriteMaxBytes > 0 && bytes >= opts.BufferedIndexedWriteMaxBytes {
		return true
	}
	if opts.BufferedIndexedWriteMaxRootRuns > 0 && rootRuns >= opts.BufferedIndexedWriteMaxRootRuns {
		return true
	}
	return false
}

func shouldFlushBufferedIndexedWritesAfterAdding(domain *collectionWriteDomain, opts CollectionOptions, addedCount int, addedBytes int64, addedRootRuns int) bool {
	if domain == nil || addedCount <= 0 {
		return false
	}
	baseCount := domain.count
	baseBytes := domain.bufferedBytes
	baseRootRuns := bufferedIndexedRootRunCount(domain)
	if opts.BufferedIndexedAsyncFlush {
		baseCount = domain.mutableCount
		baseBytes = domain.mutableBytes
		baseRootRuns = domain.rootRunCount
	}
	nextCount := saturatingAddNonNegativeInt(baseCount, addedCount)
	if opts.BufferedIndexedWriteMaxDocuments > 0 && nextCount >= opts.BufferedIndexedWriteMaxDocuments {
		return true
	}
	nextBytes := saturatingAddNonNegativeInt64(baseBytes, addedBytes)
	if opts.BufferedIndexedWriteMaxBytes > 0 && nextBytes >= opts.BufferedIndexedWriteMaxBytes {
		return true
	}
	nextRootRuns := saturatingAddNonNegativeInt(baseRootRuns, addedRootRuns)
	if opts.BufferedIndexedWriteMaxRootRuns > 0 && nextRootRuns >= opts.BufferedIndexedWriteMaxRootRuns {
		return true
	}
	return false
}

func shouldCompactBufferedIndexedMutableRuns(domain *collectionWriteDomain, opts CollectionOptions) bool {
	if domain == nil || domain.rootRunCount <= 0 {
		return false
	}
	if opts.BufferedIndexedWriteMaxRootRuns <= 0 || opts.BufferedIndexedWriteMaxDocuments <= 0 {
		return false
	}
	if domain.rootRunCount < opts.BufferedIndexedWriteMaxRootRuns {
		return false
	}
	return domain.mutableCount < opts.BufferedIndexedWriteMaxDocuments
}

func maybeCompactBufferedIndexedMutableRunsLocked(domain *collectionWriteDomain, opts CollectionOptions) ([]memtable.Table, error) {
	if !shouldCompactBufferedIndexedMutableRuns(domain, opts) {
		return nil, nil
	}
	beforeBytes := tableRunMapSize(domain.rootRuns) + tableRunMapSize(domain.uniqueValueRuns)
	compactedRootRuns, obsoleteRootRuns, rootRunsChanged, err := compactTableRunMap(domain.rootRuns)
	if err != nil {
		return nil, err
	}
	compactedUniqueRuns, obsoleteUniqueRuns, uniqueRunsChanged, err := compactTableRunMap(domain.uniqueValueRuns)
	if err != nil {
		resetTableRunMap(compactedRootRuns, domain.rootRuns)
		return nil, err
	}
	if !rootRunsChanged && !uniqueRunsChanged {
		return nil, nil
	}
	afterBytes := tableRunMapSize(compactedRootRuns) + tableRunMapSize(compactedUniqueRuns)
	domain.rootRuns = compactedRootRuns
	domain.rootMutableRuns = nil
	domain.uniqueValueRuns = compactedUniqueRuns
	domain.rootRunCount = tableRunMapRunCount(domain.rootRuns)
	domain.bufferedBytes = saturatingAddNonNegativeInt64(subtractNonNegativeInt64(domain.bufferedBytes, beforeBytes), afterBytes)
	domain.mutableBytes = saturatingAddNonNegativeInt64(subtractNonNegativeInt64(domain.mutableBytes, beforeBytes), afterBytes)
	domain.writeGeneration++
	rebuildBufferedPendingIndexesLocked(domain, domain.meta.Name, domain.primaryRunIndex != nil)
	obsolete := make([]memtable.Table, 0, len(obsoleteRootRuns)+len(obsoleteUniqueRuns))
	obsolete = append(obsolete, obsoleteRootRuns...)
	obsolete = append(obsolete, obsoleteUniqueRuns...)
	return obsolete, nil
}

func compactTableRunMap(runs map[string][]memtable.Table) (map[string][]memtable.Table, []memtable.Table, bool, error) {
	if len(runs) == 0 {
		return runs, nil, false, nil
	}
	out := make(map[string][]memtable.Table, len(runs))
	var obsolete []memtable.Table
	changed := false
	for name, tables := range runs {
		if len(tables) <= 1 {
			out[name] = tables
			continue
		}
		table, err := compactTableRuns(tables)
		if err != nil {
			resetTableRunMap(out, runs)
			return nil, nil, false, err
		}
		out[name] = []memtable.Table{table}
		obsolete = append(obsolete, tables...)
		changed = true
	}
	return out, obsolete, changed, nil
}

func compactTableRuns(runs []memtable.Table) (memtable.Table, error) {
	entryCapacity := 0
	for _, table := range runs {
		if table != nil {
			entryCapacity = saturatingAddNonNegativeInt(entryCapacity, table.Len())
		}
	}
	table := newCollectionRunTable(entryCapacity)
	iter := newBufferedRootRunsIteratorWithDeleted(runs, nil, nil, true)
	defer func() { _ = iter.Close() }()
	for ; iter.Valid(); iter.Next() {
		key := bytes.Clone(iter.UnsafeKey())
		value, ptr, flags := iter.UnsafeEntry()
		var valueCopy []byte
		if value != nil {
			valueCopy = bytes.Clone(value)
		}
		table.SetEntrySteal(key, valueCopy, ptr, flags)
	}
	if err := iter.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	table.Freeze()
	return table, nil
}

func mutableRootRunLocked(domain *collectionWriteDomain, rootName string) (memtable.Table, bool) {
	if domain == nil || rootName == "" {
		return nil, false
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table)
	}
	if domain.rootMutableRuns == nil {
		domain.rootMutableRuns = make(map[string]memtable.Table)
	}
	if table := domain.rootMutableRuns[rootName]; table != nil {
		return table, false
	}
	table := newCollectionRootAccumulatorRunTable()
	domain.rootMutableRuns[rootName] = table
	domain.rootRuns[rootName] = append(domain.rootRuns[rootName], table)
	domain.rootRunCount = saturatingAddNonNegativeInt(domain.rootRunCount, 1)
	return table, true
}

func newCollectionRootAccumulatorRunTable() memtable.Table {
	return newFreezeSortRunTable()
}

func freezeMutableIndexedRunMapsLocked(domain *collectionWriteDomain) {
	if domain == nil || len(domain.rootMutableRuns) == 0 {
		return
	}
	for _, table := range domain.rootMutableRuns {
		if table != nil {
			table.Freeze()
		}
	}
	domain.rootMutableRuns = nil
}

func estimateAccumulatedRootRunsForNamesLocked(domain *collectionWriteDomain, rootNames []string) int {
	added := 0
	for i, rootName := range rootNames {
		if rootName == "" {
			continue
		}
		seen := false
		for j := 0; j < i; j++ {
			if rootNames[j] == rootName {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		if domain != nil && domain.rootMutableRuns != nil && domain.rootMutableRuns[rootName] != nil {
			continue
		}
		added++
	}
	return added
}

func resetTableRunMap(runs, preserved map[string][]memtable.Table) {
	if len(runs) == 0 {
		return
	}
	preservedTables := make(map[memtable.Table]struct{})
	for _, tables := range preserved {
		for _, table := range tables {
			if table != nil {
				preservedTables[table] = struct{}{}
			}
		}
	}
	for _, tables := range runs {
		for _, table := range tables {
			if table == nil {
				continue
			}
			if _, ok := preservedTables[table]; ok {
				continue
			}
			resetCollectionRunTable(table)
		}
	}
}

func tableRunMapSize(runs map[string][]memtable.Table) int64 {
	var total int64
	for _, tables := range runs {
		for _, table := range tables {
			if table != nil {
				total = saturatingAddNonNegativeInt64(total, table.Size())
			}
		}
	}
	return total
}

func tableRunMapRunCount(runs map[string][]memtable.Table) int {
	total := 0
	for _, tables := range runs {
		total = saturatingAddNonNegativeInt(total, len(tables))
	}
	return total
}

func bufferedIndexedRootRunCount(domain *collectionWriteDomain) int {
	if domain == nil {
		return 0
	}
	total := 0
	for _, unit := range domain.indexedPublishingUnits {
		if unit.rootRunCount > 0 || len(unit.rootRuns) == 0 {
			total = saturatingAddNonNegativeInt(total, unit.rootRunCount)
			continue
		}
		for _, runs := range unit.rootRuns {
			total = saturatingAddNonNegativeInt(total, len(runs))
		}
	}
	for _, unit := range domain.indexedFlushUnits {
		if unit.rootRunCount > 0 || len(unit.rootRuns) == 0 {
			total = saturatingAddNonNegativeInt(total, unit.rootRunCount)
			continue
		}
		for _, runs := range unit.rootRuns {
			total = saturatingAddNonNegativeInt(total, len(runs))
		}
	}
	if domain.rootRunCount > 0 || len(domain.rootRuns) == 0 {
		return saturatingAddNonNegativeInt(total, domain.rootRunCount)
	}
	for _, runs := range domain.rootRuns {
		total = saturatingAddNonNegativeInt(total, len(runs))
	}
	return total
}

func saturatingAddNonNegativeInt(total, n int) int {
	if n <= 0 {
		return total
	}
	if total > maxCollectionInt-n {
		return maxCollectionInt
	}
	return total + n
}

func bufferedIndexedAutoFlushEnabled(opts CollectionOptions) bool {
	return opts.BufferedIndexedWriteMaxDocuments > 0 || opts.BufferedIndexedWriteMaxBytes > 0 || opts.BufferedIndexedWriteMaxRootRuns > 0
}

func collectionObservedElapsedSince(start time.Time) time.Duration {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
}

// flushBufferedIndexedAfterThresholdLocked returns local schedule/publish
// duration, the full interval where domain.mu was deliberately released while
// waiting for an already-running async flush, and any mutex reacquire wait after
// that async wait. The released interval is not counted as local flush work.
func (c *Collection) flushBufferedIndexedAfterThresholdLocked(domain *collectionWriteDomain, opts CollectionOptions) (time.Duration, time.Duration, time.Duration, error) {
	if domain == nil {
		return 0, 0, 0, nil
	}
	domain.indexedAutoFlushes.Add(1)
	if opts.BufferedIndexedAsyncFlush {
		rotateIndexedMutableToFlushUnitLocked(domain)
		if opts.BufferedIndexedAsyncFlushMaxQueuedUnits > 0 && len(domain.indexedFlushUnits) >= opts.BufferedIndexedAsyncFlushMaxQueuedUnits {
			if len(domain.indexedPublishingUnits) == 0 {
				domain.indexedAsyncFlushBackpressure.Add(1)
				domain.observeIndexedFlushForcedDrain()
				flushStart := time.Now()
				err := c.flushBufferedIndexedLocked(domain)
				return collectionObservedElapsedSince(flushStart), 0, 0, err
			}
			domain.indexedAsyncFlushBackpressure.Add(1)
			flushStart := time.Now()
			var lockReleased time.Duration
			var relockWait time.Duration
			if domain.indexedAsyncFlushRunning() {
				unlockStart := time.Now()
				domain.mu.Unlock()
				domain.waitIndexedAsyncFlush()
				relockStart := time.Now()
				domain.mu.Lock()
				relockWait += time.Since(relockStart)
				lockReleased += time.Since(unlockStart)
				if err := domain.consumeIndexedAsyncFlushError(); err != nil {
					return 0, lockReleased, relockWait, err
				}
				if domain.count == 0 || len(domain.indexedFlushUnits) == 0 || !hasBufferedIndexedRootRuns(domain) {
					return 0, lockReleased, relockWait, nil
				}
				if len(domain.indexedPublishingUnits) == 0 {
					domain.observeIndexedFlushForcedDrain()
					flushStart = time.Now()
					err := c.flushBufferedIndexedLocked(domain)
					return collectionObservedElapsedSince(flushStart), lockReleased, relockWait, err
				}
			}
			flushStart = time.Now()
			if !c.scheduleIndexedAsyncFlush(domain) && len(domain.indexedPublishingUnits) == 0 {
				domain.observeIndexedFlushForcedDrain()
				err := c.flushBufferedIndexedLocked(domain)
				return collectionObservedElapsedSince(flushStart), lockReleased, relockWait, err
			}
			return collectionObservedElapsedSince(flushStart), lockReleased, relockWait, nil
		}
		flushStart := time.Now()
		if !c.scheduleIndexedAsyncFlush(domain) && len(domain.indexedPublishingUnits) == 0 {
			domain.observeIndexedFlushForcedDrain()
			err := c.flushBufferedIndexedLocked(domain)
			return collectionObservedElapsedSince(flushStart), 0, 0, err
		}
		return collectionObservedElapsedSince(flushStart), 0, 0, nil
	}
	flushStart := time.Now()
	err := c.flushBufferedIndexedLocked(domain)
	return collectionObservedElapsedSince(flushStart), 0, 0, err
}

func hasBufferedIndexedRootRuns(domain *collectionWriteDomain) bool {
	return bufferedIndexedRootRunCount(domain) > 0
}

func pendingIndexedRootRunsLocked(domain *collectionWriteDomain, rootName string) []memtable.Table {
	if domain == nil || rootName == "" {
		return nil
	}
	if len(domain.indexedPublishingUnits) == 0 && len(domain.indexedFlushUnits) == 0 {
		return domain.rootRuns[rootName]
	}
	total := len(domain.rootRuns[rootName])
	for _, unit := range domain.indexedPublishingUnits {
		total = saturatingAddNonNegativeInt(total, len(unit.rootRuns[rootName]))
	}
	for _, unit := range domain.indexedFlushUnits {
		total = saturatingAddNonNegativeInt(total, len(unit.rootRuns[rootName]))
	}
	if total == 0 {
		return nil
	}
	out := make([]memtable.Table, 0, total)
	for _, unit := range domain.indexedPublishingUnits {
		out = append(out, unit.rootRuns[rootName]...)
	}
	for _, unit := range domain.indexedFlushUnits {
		out = append(out, unit.rootRuns[rootName]...)
	}
	out = append(out, domain.rootRuns[rootName]...)
	return out
}

func hasPendingIndexedRootRunsForRootLocked(domain *collectionWriteDomain, rootName string) bool {
	if domain == nil || rootName == "" {
		return false
	}
	if len(domain.rootRuns[rootName]) > 0 {
		return true
	}
	for _, unit := range domain.indexedPublishingUnits {
		if len(unit.rootRuns[rootName]) > 0 {
			return true
		}
	}
	for _, unit := range domain.indexedFlushUnits {
		if len(unit.rootRuns[rootName]) > 0 {
			return true
		}
	}
	return false
}

func pendingIndexedRootRunMapLocked(domain *collectionWriteDomain) map[string][]memtable.Table {
	if domain == nil {
		return nil
	}
	if len(domain.indexedPublishingUnits) == 0 && len(domain.indexedFlushUnits) == 0 {
		return domain.rootRuns
	}
	out := make(map[string][]memtable.Table, len(domain.rootRuns)+len(domain.indexedPublishingUnits)+len(domain.indexedFlushUnits))
	for _, unit := range domain.indexedPublishingUnits {
		appendTableRunMap(out, unit.rootRuns)
	}
	for _, unit := range domain.indexedFlushUnits {
		appendTableRunMap(out, unit.rootRuns)
	}
	appendTableRunMap(out, domain.rootRuns)
	if len(out) == 0 {
		return nil
	}
	return out
}

func appendTableRunMap(dst, src map[string][]memtable.Table) {
	if len(src) == 0 {
		return
	}
	for name, runs := range src {
		if len(runs) == 0 {
			continue
		}
		dst[name] = append(dst[name], runs...)
	}
}

func pendingIndexedRootBaseIDLocked(domain *collectionWriteDomain, rootName string) (uint64, bool) {
	if domain == nil || rootName == "" {
		return 0, false
	}
	for _, unit := range domain.indexedPublishingUnits {
		if baseRoot, ok := unit.rootBaseIDs[rootName]; ok {
			return baseRoot, true
		}
	}
	for _, unit := range domain.indexedFlushUnits {
		if baseRoot, ok := unit.rootBaseIDs[rootName]; ok {
			return baseRoot, true
		}
	}
	baseRoot, ok := domain.rootBaseIDs[rootName]
	return baseRoot, ok
}

func forEachPendingIndexedRootBaseIDLocked(domain *collectionWriteDomain, fn func(rootName string, baseRoot uint64) error) error {
	if domain == nil || fn == nil {
		return nil
	}
	if len(domain.indexedPublishingUnits) == 0 && len(domain.indexedFlushUnits) == 0 {
		for rootName, baseRoot := range domain.rootBaseIDs {
			if err := fn(rootName, baseRoot); err != nil {
				return err
			}
		}
		return nil
	}
	seen := make(map[string]uint64, len(domain.rootBaseIDs))
	for _, unit := range domain.indexedPublishingUnits {
		for rootName, baseRoot := range unit.rootBaseIDs {
			if prior, ok := seen[rootName]; ok {
				if prior != baseRoot {
					return fmt.Errorf("collections: buffered indexed root %q has conflicting base roots %d and %d", rootName, prior, baseRoot)
				}
				continue
			}
			seen[rootName] = baseRoot
			if err := fn(rootName, baseRoot); err != nil {
				return err
			}
		}
	}
	for _, unit := range domain.indexedFlushUnits {
		for rootName, baseRoot := range unit.rootBaseIDs {
			if prior, ok := seen[rootName]; ok {
				if prior != baseRoot {
					return fmt.Errorf("collections: buffered indexed root %q has conflicting base roots %d and %d", rootName, prior, baseRoot)
				}
				continue
			}
			seen[rootName] = baseRoot
			if err := fn(rootName, baseRoot); err != nil {
				return err
			}
		}
	}
	for rootName, baseRoot := range domain.rootBaseIDs {
		if prior, ok := seen[rootName]; ok {
			if prior != baseRoot {
				return fmt.Errorf("collections: buffered indexed root %q has conflicting base roots %d and %d", rootName, prior, baseRoot)
			}
			continue
		}
		seen[rootName] = baseRoot
		if err := fn(rootName, baseRoot); err != nil {
			return err
		}
	}
	return nil
}

func saturatingAddNonNegativeInt64(total, n int64) int64 {
	if n <= 0 {
		return total
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if total > maxInt64-n {
		return maxInt64
	}
	return total + n
}

func subtractNonNegativeInt(total, n int) int {
	if n <= 0 {
		return total
	}
	if n >= total {
		return 0
	}
	return total - n
}

func subtractNonNegativeInt64(total, n int64) int64 {
	if n <= 0 {
		return total
	}
	if n >= total {
		return 0
	}
	return total - n
}

func checkpointBufferedIndexedDomain(domain *collectionWriteDomain) bufferedIndexedCheckpoint {
	if domain == nil {
		return bufferedIndexedCheckpoint{}
	}
	return bufferedIndexedCheckpoint{
		loaded:                 domain.loaded,
		meta:                   domain.meta,
		catalog:                domain.catalog,
		baseCommitSeq:          domain.baseCommitSeq,
		baseSystemRoot:         domain.baseSystemRoot,
		primaryRoot:            domain.primaryRoot,
		count:                  domain.count,
		bufferedBytes:          domain.bufferedBytes,
		mutableCount:           domain.mutableCount,
		mutableBytes:           domain.mutableBytes,
		writeGeneration:        domain.writeGeneration,
		rootRuns:               cloneTableRunMap(domain.rootRuns),
		rootMutableRuns:        cloneMutableRunMap(domain.rootMutableRuns),
		rootPolicies:           cloneRootPolicyMap(domain.rootPolicies),
		rootBaseIDs:            cloneUint64Map(domain.rootBaseIDs),
		rootValueArenas:        cloneArenaRefs(domain.rootValueArenas),
		indexedSemanticRecords: cloneIndexedSemanticRecords(domain.indexedSemanticRecords),
		indexedPublishingUnits: cloneIndexedFlushUnits(domain.indexedPublishingUnits),
		indexedFlushUnits:      cloneIndexedFlushUnits(domain.indexedFlushUnits),
		primaryRunIndexActive:  domain.primaryRunIndex != nil,
		uniqueValueRuns:        cloneTableRunMap(domain.uniqueValueRuns),
		rootRunCount:           domain.rootRunCount,
	}
}

func rollbackBufferedIndexedDomain(domain *collectionWriteDomain, checkpoint bufferedIndexedCheckpoint) {
	if domain == nil {
		return
	}
	keep := bufferedIndexedCheckpointTableSet(checkpoint)
	resetIndexedFlushUnitsAddedAfterCheckpoint(domain.indexedFlushUnits, checkpoint)
	resetTablesNotInSet(domain.rootRuns, keep)
	resetTablesNotInSet(domain.uniqueValueRuns, keep)
	domain.loaded = checkpoint.loaded
	domain.meta = checkpoint.meta
	domain.catalog = checkpoint.catalog
	domain.baseCommitSeq = checkpoint.baseCommitSeq
	domain.baseSystemRoot = checkpoint.baseSystemRoot
	domain.primaryRoot = checkpoint.primaryRoot
	domain.count = checkpoint.count
	domain.bufferedBytes = checkpoint.bufferedBytes
	domain.mutableCount = checkpoint.mutableCount
	domain.mutableBytes = checkpoint.mutableBytes
	domain.writeGeneration = checkpoint.writeGeneration
	domain.indexedPublishingUnits = checkpoint.indexedPublishingUnits
	domain.indexedFlushUnits = checkpoint.indexedFlushUnits
	domain.rootRuns = checkpoint.rootRuns
	domain.rootMutableRuns = checkpoint.rootMutableRuns
	domain.rootPolicies = checkpoint.rootPolicies
	domain.rootBaseIDs = checkpoint.rootBaseIDs
	domain.rootValueArenas = checkpoint.rootValueArenas
	domain.indexedSemanticRecords = checkpoint.indexedSemanticRecords
	domain.rootRunCount = checkpoint.rootRunCount
	pendingRuns := indexedFlushUnitPendingRootRunMap(indexedFlushUnitsWithPublishing(checkpoint.indexedPublishingUnits, checkpoint.indexedFlushUnits), checkpoint.rootRuns)
	domain.primaryIDIndex = rebuildBufferedPrimaryIDIndex(checkpoint.meta.Name, pendingRuns)
	if checkpoint.primaryRunIndexActive {
		index, err := rebuildBufferedPrimaryRunIndex(checkpoint.meta.Name, pendingRuns)
		if err != nil {
			index = nil
		} else if index == nil {
			index = newBufferedPrimaryRunIndex(0)
		}
		domain.primaryRunIndex = index
	} else {
		domain.primaryRunIndex = nil
	}
	domain.uniqueValueRuns = checkpoint.uniqueValueRuns
	domain.uniqueValueIndex = rebuildBufferedUniqueValueIndexes(indexedFlushUnitPendingUniqueValueRunMap(indexedFlushUnitsWithPublishing(checkpoint.indexedPublishingUnits, checkpoint.indexedFlushUnits), checkpoint.uniqueValueRuns))
}

func cloneIndexedFlushUnits(in []indexedFlushUnit) []indexedFlushUnit {
	if len(in) == 0 {
		return nil
	}
	out := make([]indexedFlushUnit, len(in))
	for i, unit := range in {
		out[i] = indexedFlushUnit{
			rootRuns:        cloneTableRunMap(unit.rootRuns),
			rootPolicies:    cloneRootPolicyMap(unit.rootPolicies),
			rootBaseIDs:     cloneUint64Map(unit.rootBaseIDs),
			uniqueValueRuns: cloneTableRunMap(unit.uniqueValueRuns),
			semanticRecords: cloneIndexedSemanticRecords(unit.semanticRecords),
			arenaRefs:       cloneArenaRefs(unit.arenaRefs),
			docCount:        unit.docCount,
			byteCount:       unit.byteCount,
			rootRunCount:    unit.rootRunCount,
		}
	}
	return out
}

func cloneIndexedSemanticRecords(in []indexedSemanticRecord) []indexedSemanticRecord {
	if len(in) == 0 {
		return nil
	}
	out := make([]indexedSemanticRecord, len(in))
	for i, record := range in {
		out[i] = indexedSemanticRecord{
			kind:        record.kind,
			documentID:  bytes.Clone(record.documentID),
			fallback:    record.fallback,
			indexDeltas: cloneIndexedSemanticIndexDeltas(record.indexDeltas),
		}
	}
	return out
}

func cloneIndexedSemanticIndexDeltas(in []indexedSemanticIndexDelta) []indexedSemanticIndexDelta {
	if len(in) == 0 {
		return nil
	}
	out := make([]indexedSemanticIndexDelta, len(in))
	for i, delta := range in {
		out[i] = indexedSemanticIndexDelta{
			indexName:  delta.indexName,
			rootName:   delta.rootName,
			runtimeIdx: delta.runtimeIdx,
			unique:     delta.unique,
			oldValues:  cloneIndexedSemanticValueSet(delta.oldValues),
			newValues:  cloneIndexedSemanticValueSet(delta.newValues),
		}
	}
	return out
}

func cloneIndexedSemanticValueSet(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make([][]byte, len(in))
	for i, value := range in {
		out[i] = bytes.Clone(value)
	}
	return out
}

func cloneArenaRefs(in [][]byte) [][]byte {
	if len(in) == 0 {
		return nil
	}
	return append([][]byte(nil), in...)
}

func indexedFlushUnitsWithPublishing(publishing, queued []indexedFlushUnit) []indexedFlushUnit {
	if len(publishing) == 0 {
		return queued
	}
	if len(queued) == 0 {
		return publishing
	}
	out := make([]indexedFlushUnit, 0, len(publishing)+len(queued))
	out = append(out, publishing...)
	out = append(out, queued...)
	return out
}

func indexedFlushUnitPendingRootRunMap(units []indexedFlushUnit, mutable map[string][]memtable.Table) map[string][]memtable.Table {
	if len(units) == 0 {
		return mutable
	}
	out := make(map[string][]memtable.Table, len(mutable)+len(units))
	for _, unit := range units {
		appendTableRunMap(out, unit.rootRuns)
	}
	appendTableRunMap(out, mutable)
	if len(out) == 0 {
		return nil
	}
	return out
}

func indexedFlushUnitPendingUniqueValueRunMap(units []indexedFlushUnit, mutable map[string][]memtable.Table) map[string][]memtable.Table {
	if len(units) == 0 {
		return mutable
	}
	out := make(map[string][]memtable.Table, len(mutable)+len(units))
	for _, unit := range units {
		appendTableRunMap(out, unit.uniqueValueRuns)
	}
	appendTableRunMap(out, mutable)
	if len(out) == 0 {
		return nil
	}
	return out
}

func pendingIndexedUniqueValueRunMapLocked(domain *collectionWriteDomain) map[string][]memtable.Table {
	if domain == nil {
		return nil
	}
	return indexedFlushUnitPendingUniqueValueRunMap(indexedFlushUnitsWithPublishing(domain.indexedPublishingUnits, domain.indexedFlushUnits), domain.uniqueValueRuns)
}

// pendingUniqueReservationIndexLocked requires domain.mu to be held. When
// cache is true it may write the rebuilt pending unique index back to the
// domain, so callers using only an RLock must pass cache=false.
func pendingUniqueReservationIndexLocked(domain *collectionWriteDomain, indexName string, cache bool) *bufferedUniqueValueIndex {
	if domain == nil || indexName == "" {
		return nil
	}
	if index := domain.uniqueValueIndex[indexName]; index != nil {
		return index
	}
	runs := pendingIndexedUniqueValueRunMapLocked(domain)[indexName]
	if len(runs) == 0 {
		return nil
	}
	index := rebuildBufferedUniqueValueIndexes(map[string][]memtable.Table{indexName: runs})[indexName]
	if cache && index != nil {
		if domain.uniqueValueIndex == nil {
			domain.uniqueValueIndex = make(map[string]*bufferedUniqueValueIndex, 1)
		}
		domain.uniqueValueIndex[indexName] = index
	}
	return index
}

// pendingUniqueReservationProbeLocked requires domain.mu to be held.
func pendingUniqueReservationProbeLocked(domain *collectionWriteDomain, indexName string, valuePrefix []byte) bool {
	index := pendingUniqueReservationIndexLocked(domain, indexName, false)
	return index != nil && index.contains(valuePrefix)
}

func rebuildBufferedPendingIndexesLocked(domain *collectionWriteDomain, collectionName string, preservePrimaryRunIndex bool) {
	if domain == nil {
		return
	}
	pendingRuns := pendingIndexedRootRunMapLocked(domain)
	domain.primaryIDIndex = rebuildBufferedPrimaryIDIndex(collectionName, pendingRuns)
	if preservePrimaryRunIndex {
		index, err := rebuildBufferedPrimaryRunIndex(collectionName, pendingRuns)
		if err != nil {
			index = nil
		} else if index == nil {
			index = newBufferedPrimaryRunIndex(0)
		}
		domain.primaryRunIndex = index
	} else {
		domain.primaryRunIndex = nil
	}
	domain.uniqueValueIndex = rebuildBufferedUniqueValueIndexes(pendingIndexedUniqueValueRunMapLocked(domain))
}

func resetIndexedFlushUnitsAddedAfterCheckpoint(current []indexedFlushUnit, checkpoint bufferedIndexedCheckpoint) {
	if len(current) == 0 {
		return
	}
	keep := bufferedIndexedCheckpointTableSet(checkpoint)
	for _, unit := range current {
		resetTablesNotInSet(unit.rootRuns, keep)
		resetTablesNotInSet(unit.uniqueValueRuns, keep)
	}
}

func bufferedIndexedCheckpointTableSet(checkpoint bufferedIndexedCheckpoint) map[memtable.Table]struct{} {
	out := make(map[memtable.Table]struct{})
	addTablesToSet := func(runs map[string][]memtable.Table) {
		for _, tables := range runs {
			for _, table := range tables {
				if table != nil {
					out[table] = struct{}{}
				}
			}
		}
	}
	addTablesToSet(checkpoint.rootRuns)
	addTablesToSet(checkpoint.uniqueValueRuns)
	for _, unit := range checkpoint.indexedFlushUnits {
		addTablesToSet(unit.rootRuns)
		addTablesToSet(unit.uniqueValueRuns)
	}
	for _, unit := range checkpoint.indexedPublishingUnits {
		addTablesToSet(unit.rootRuns)
		addTablesToSet(unit.uniqueValueRuns)
	}
	return out
}

func resetTablesNotInSet(runs map[string][]memtable.Table, keep map[memtable.Table]struct{}) {
	for _, tables := range runs {
		for _, table := range tables {
			if table == nil {
				continue
			}
			if _, ok := keep[table]; ok {
				continue
			}
			resetCollectionRunTable(table)
		}
	}
}

func cloneTableRunMap(in map[string][]memtable.Table) map[string][]memtable.Table {
	if in == nil {
		return nil
	}
	out := make(map[string][]memtable.Table, len(in))
	for name, runs := range in {
		out[name] = runs
	}
	return out
}

func cloneMutableRunMap(in map[string]memtable.Table) map[string]memtable.Table {
	if in == nil {
		return nil
	}
	out := make(map[string]memtable.Table, len(in))
	for name, table := range in {
		out[name] = table
	}
	return out
}

func cloneRootPolicyMap(in map[string]backenddb.OrderedRootStoragePolicy) map[string]backenddb.OrderedRootStoragePolicy {
	if in == nil {
		return nil
	}
	out := make(map[string]backenddb.OrderedRootStoragePolicy, len(in))
	for name, policy := range in {
		out[name] = policy
	}
	return out
}

func cloneUint64Map(in map[string]uint64) map[string]uint64 {
	if in == nil {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for name, value := range in {
		out[name] = value
	}
	return out
}

func newBufferedUniqueValueIndex(capacity int) *bufferedUniqueValueIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedUniqueValueIndex{values: make(map[uint64][]byte, capacity)}
}

func (idx *bufferedUniqueValueIndex) len() int {
	if idx == nil {
		return 0
	}
	total := len(idx.values)
	for _, collisions := range idx.collisions {
		total += len(collisions)
	}
	return total
}

func (idx *bufferedUniqueValueIndex) addAll(prefixes [][]byte) {
	if idx == nil {
		return
	}
	if idx.values == nil {
		idx.values = make(map[uint64][]byte, len(prefixes))
	}
	for _, prefix := range prefixes {
		idx.add(prefix)
	}
}

func (idx *bufferedUniqueValueIndex) add(prefix []byte) {
	if idx == nil {
		return
	}
	hash := xxhash.Sum64(prefix)
	if existing, ok := idx.values[hash]; ok {
		if bytes.Equal(existing, prefix) {
			return
		}
		if idx.collisions == nil {
			idx.collisions = make(map[uint64][][]byte)
		}
		idx.collisions[hash] = append(idx.collisions[hash], prefix)
		return
	}
	idx.values[hash] = prefix
}

func (idx *bufferedUniqueValueIndex) contains(prefix []byte) bool {
	if idx == nil || len(idx.values) == 0 {
		return false
	}
	hash := xxhash.Sum64(prefix)
	if bytes.Equal(idx.values[hash], prefix) {
		return true
	}
	for _, candidate := range idx.collisions[hash] {
		if bytes.Equal(candidate, prefix) {
			return true
		}
	}
	return false
}

func rebuildBufferedUniqueValueIndexes(runs map[string][]memtable.Table) map[string]*bufferedUniqueValueIndex {
	if len(runs) == 0 {
		return nil
	}
	out := make(map[string]*bufferedUniqueValueIndex, len(runs))
	for indexName, tables := range runs {
		index := newBufferedUniqueValueIndex(bufferedRunLenHint(tables))
		for _, table := range tables {
			it := table.NewIterator(nil, nil)
			for it.Valid() {
				index.add(bytes.Clone(it.UnsafeKey()))
				it.Next()
			}
			_ = it.Close()
		}
		if index.len() > 0 {
			out[indexName] = index
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *Collection) rejectBufferedIndexedInsertConflictsLocked(domain *collectionWriteDomain, meta CollectionMeta, plan *insertBatchPlan) error {
	if domain == nil || domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return nil
	}
	primaryName := collectionPrimaryRootName(meta.Name)
	if pendingPrimary := pendingIndexedRootRunsLocked(domain, primaryName); len(pendingPrimary) > 0 {
		for _, run := range plan.runs {
			if run.name != primaryName {
				continue
			}
			if err := rejectBufferedPrimaryConflicts(domain.primaryIDIndex, pendingPrimary, run.table); err != nil {
				return err
			}
			break
		}
	}
	uniqueIndexes := uniqueCollectionIndexNames(meta)
	for _, run := range plan.runs {
		if run.kind != collectionRootSecondary {
			continue
		}
		if _, ok := uniqueIndexes[run.indexName]; !ok {
			continue
		}
		pending := pendingUniqueReservationIndexLocked(domain, run.indexName, true)
		if pending == nil || pending.len() == 0 {
			continue
		}
		if err := rejectBufferedUniqueIndexConflicts(run.indexName, run.indexValueType, pending, run.table); err != nil {
			return err
		}
	}
	return nil
}

func rejectBufferedPrimaryConflicts(pendingIndex *bufferedUniqueValueIndex, pendingPrimary []memtable.Table, batchPrimary memtable.Table) error {
	it := batchPrimary.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if pendingIndex != nil && pendingIndex.contains(it.UnsafeKey()) {
			return ErrDocumentExists
		}
		if pendingIndex == nil {
			if _, _, flags, found := getBufferedRunEntry(pendingPrimary, it.UnsafeKey()); found && flags&node.FlagTombstone == 0 {
				return ErrDocumentExists
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func addBufferedPrimaryIDs(index *bufferedUniqueValueIndex, batchPrimary memtable.Table) error {
	if index == nil || batchPrimary == nil {
		return nil
	}
	arena := make([]byte, 0, bufferedPrimaryIDArenaCap(batchPrimary.Len()))
	it := batchPrimary.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		start := len(arena)
		arena = append(arena, key...)
		index.add(arena[start:len(arena)])
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	if len(arena) > 0 {
		index.arenas = append(index.arenas, arena)
	}
	return nil
}

func newBufferedPrimaryRunIndex(capacity int) *bufferedPrimaryRunIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedPrimaryRunIndex{values: make(map[uint64]bufferedPrimaryRunRef, capacity)}
}

func (index *bufferedPrimaryRunIndex) addRef(hash uint64, ref bufferedPrimaryRunRef) {
	if index == nil || ref.table == nil {
		return
	}
	if existing, ok := index.values[hash]; !ok {
		index.values[hash] = ref
		return
	} else if bytes.Equal(existing.key, ref.key) {
		index.values[hash] = ref
		return
	}
	collisions := index.collisions
	if collisions == nil {
		collisions = make(map[uint64][]bufferedPrimaryRunRef)
		index.collisions = collisions
	}
	bucket := collisions[hash]
	for i := len(bucket) - 1; i >= 0; i-- {
		if bytes.Equal(bucket[i].key, ref.key) {
			bucket[i] = ref
			collisions[hash] = bucket
			return
		}
	}
	collisions[hash] = append(bucket, ref)
}

func (index *bufferedPrimaryRunIndex) lookup(key []byte) (memtable.Table, bool) {
	if index == nil {
		return nil, false
	}
	hash := xxhash.Sum64(key)
	if ref, ok := index.values[hash]; ok && bytes.Equal(ref.key, key) {
		return ref.table, true
	}
	for _, ref := range index.collisions[hash] {
		if bytes.Equal(ref.key, key) {
			return ref.table, true
		}
	}
	return nil, false
}

func addBufferedPrimaryRunIndexEntries(index *bufferedPrimaryRunIndex, batchPrimary memtable.Table) error {
	if index == nil || batchPrimary == nil {
		return nil
	}
	stableKeys := false
	if stable, ok := batchPrimary.(memtable.StableUnsafeIteratorTable); ok {
		stableKeys = stable.StableUnsafeIteratorSlices()
	}
	it := batchPrimary.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	if stableKeys {
		for it.Valid() {
			key := it.UnsafeKey()
			index.addRef(xxhash.Sum64(key), bufferedPrimaryRunRef{key: key, table: batchPrimary})
			it.Next()
		}
		return it.Error()
	}
	arena := make([]byte, 0, bufferedPrimaryIDArenaCap(batchPrimary.Len()))
	refs := make([]bufferedPrimaryRunRef, 0, batchPrimary.Len())
	hashes := make([]uint64, 0, batchPrimary.Len())
	for it.Valid() {
		key := it.UnsafeKey()
		refKey := key
		start := len(arena)
		arena = append(arena, key...)
		refKey = arena[start:len(arena)]
		hashes = append(hashes, xxhash.Sum64(key))
		refs = append(refs, bufferedPrimaryRunRef{key: refKey, table: batchPrimary})
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	for i, ref := range refs {
		index.addRef(hashes[i], ref)
	}
	if len(arena) > 0 {
		index.arenas = append(index.arenas, arena)
	}
	return nil
}

func addBufferedPrimaryRunIndexKeys(index *bufferedPrimaryRunIndex, keys [][]byte, table memtable.Table) {
	if index == nil || table == nil || len(keys) == 0 {
		return
	}
	arena := make([]byte, 0, bufferedPrimaryIDArenaCap(len(keys)))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		start := len(arena)
		arena = append(arena, key...)
		refKey := arena[start:len(arena)]
		index.addRef(xxhash.Sum64(key), bufferedPrimaryRunRef{key: refKey, table: table})
	}
	if len(arena) > 0 {
		index.arenas = append(index.arenas, arena)
	}
}

func bufferedPrimaryIDArenaCap(entries int) int {
	if entries <= 0 {
		return 0
	}
	const bytesPerKeyEstimate = 16
	if entries > maxCollectionInt/bytesPerKeyEstimate {
		return 0
	}
	return entries * bytesPerKeyEstimate
}

func rebuildBufferedPrimaryRunIndex(collectionName string, runs map[string][]memtable.Table) (*bufferedPrimaryRunIndex, error) {
	if collectionName == "" || len(runs) == 0 {
		return nil, nil
	}
	tables := runs[collectionPrimaryRootName(collectionName)]
	if len(tables) == 0 {
		return nil, nil
	}
	index := newBufferedPrimaryRunIndex(bufferedRunLenHint(tables))
	for _, table := range tables {
		if err := addBufferedPrimaryRunIndexEntries(index, table); err != nil {
			return nil, err
		}
	}
	if len(index.values) == 0 {
		return nil, nil
	}
	return index, nil
}

func rebuildBufferedPrimaryIDIndex(collectionName string, runs map[string][]memtable.Table) *bufferedUniqueValueIndex {
	if collectionName == "" || len(runs) == 0 {
		return nil
	}
	tables := runs[collectionPrimaryRootName(collectionName)]
	if len(tables) == 0 {
		return nil
	}
	index := newBufferedUniqueValueIndex(bufferedRunLenHint(tables))
	for _, table := range tables {
		if err := addBufferedPrimaryIDs(index, table); err != nil {
			return nil
		}
	}
	if index.len() == 0 {
		return nil
	}
	return index
}

func bufferedRunLenHint(tables []memtable.Table) int {
	total := 0
	for _, table := range tables {
		if table == nil {
			continue
		}
		total = saturatingAddNonNegativeInt(total, table.Len())
	}
	return boundedBufferedRunLenHint(total)
}

// bufferedRunLenHintMaxCapacity keeps duplicate-heavy pending-run rebuilds from
// sizing maps to the full historical mutation count when the final cardinality
// may be much smaller.
const bufferedRunLenHintMaxCapacity = 1 << 16

func boundedBufferedRunLenHint(total int) int {
	if total <= 0 {
		return 0
	}
	if total > bufferedRunLenHintMaxCapacity {
		return bufferedRunLenHintMaxCapacity
	}
	return total
}

func uniqueCollectionIndexNames(meta CollectionMeta) map[string]struct{} {
	uniqueIndexes := make(map[string]struct{}, len(meta.Indexes))
	for _, idx := range meta.Indexes {
		if idx.Unique {
			uniqueIndexes[idx.Name] = struct{}{}
		}
	}
	return uniqueIndexes
}

func rejectBufferedUniqueIndexConflicts(indexName string, valueType IndexValueType, pendingIndex *bufferedUniqueValueIndex, batchIndex memtable.Table) error {
	it := batchIndex.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		prefix, err := indexEntryValuePrefix(it.UnsafeKey(), valueType)
		if err != nil {
			return err
		}
		if pendingIndex.contains(prefix) {
			return fmt.Errorf("%w %q", ErrUniqueIndexConflict, indexName)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func bufferedUniqueIndexValueRun(batchIndex memtable.Table, valueType IndexValueType) (memtable.Table, [][]byte, error) {
	table := newCollectionRunTable(max(0, batchIndex.Len()))
	arenaCap := batchIndex.Size()
	if arenaCap < 0 || arenaCap > int64(maxCollectionInt) {
		arenaCap = 0
	}
	arena := make([]byte, 0, int(arenaCap))
	prefixes := make([][]byte, 0, max(0, batchIndex.Len()))
	it := batchIndex.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		prefix, err := indexEntryValuePrefix(it.UnsafeKey(), valueType)
		if err != nil {
			resetCollectionRunTable(table)
			return nil, nil, err
		}
		start := len(arena)
		arena = append(arena, prefix...)
		owned := arena[start:len(arena)]
		setCollectionRunValue(table, owned, nil)
		prefixes = append(prefixes, owned)
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, nil, err
	}
	table.Freeze()
	return table, prefixes, nil
}

func indexEntryValuePrefix(key []byte, valueType IndexValueType) ([]byte, error) {
	n, err := indexComponentLength(valueType, key)
	if err != nil {
		return nil, err
	}
	return key[:n], nil
}

func getBufferedRunEntry(runs []memtable.Table, key []byte) ([]byte, page.ValuePtr, byte, bool) {
	for i := len(runs) - 1; i >= 0; i-- {
		if runs[i] == nil {
			continue
		}
		if value, ptr, flags, found := runs[i].GetEntry(key); found {
			return value, ptr, flags, true
		}
	}
	return nil, page.ValuePtr{}, 0, false
}

func cloneCollectionRunTables(runs []memtable.Table) ([]memtable.Table, error) {
	if len(runs) == 0 {
		return nil, nil
	}
	out := make([]memtable.Table, 0, len(runs))
	for _, run := range runs {
		if run == nil {
			out = append(out, nil)
			continue
		}
		cloned, err := cloneCollectionRunTable(run)
		if err != nil {
			resetCollectionTables(out)
			return nil, err
		}
		out = append(out, cloned)
	}
	return out, nil
}

func cloneCollectionRunTable(run memtable.Table) (memtable.Table, error) {
	if run == nil {
		return nil, nil
	}
	table := newCollectionRunTable(max(0, run.Len()))
	it := run.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		value, ptr, flags := it.UnsafeEntry()
		table.SetEntrySteal(bytes.Clone(it.UnsafeKey()), bytes.Clone(value), ptr, flags)
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	table.Freeze()
	return table, nil
}

type bufferedRootRunHeapItem struct {
	idx      int
	priority int
	key      []byte
}

type bufferedRootRunHeap []bufferedRootRunHeapItem

func (h bufferedRootRunHeap) Len() int { return len(h) }

func (h bufferedRootRunHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h[i].key, h[j].key); cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}

func (h bufferedRootRunHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *bufferedRootRunHeap) push(item bufferedRootRunHeapItem) {
	*h = append(*h, item)
	h.up(len(*h) - 1)
}

func (h *bufferedRootRunHeap) init() {
	n := len(*h)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *bufferedRootRunHeap) pop() bufferedRootRunHeapItem {
	old := *h
	n := len(old)
	if n == 0 {
		return bufferedRootRunHeapItem{}
	}
	old.Swap(0, n-1)
	h.down(0, n-1)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h bufferedRootRunHeap) peek() *bufferedRootRunHeapItem {
	if len(h) == 0 {
		return nil
	}
	return &h[0]
}

func (h *bufferedRootRunHeap) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *bufferedRootRunHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

type bufferedRootRunsIterator struct {
	iters              []iterator.UnsafeIterator
	priorities         []int
	priorityInline     [8]int
	heap               bufferedRootRunHeap
	cur                bufferedRootRunHeapItem
	hasCur             bool
	valid              bool
	includeDeleted     bool
	stableUnsafeSlices bool
	lenHint            int
	start              []byte
	end                []byte
	closed             bool
	firstErr           error
}

type bufferedRootRunIteratorSource struct {
	iter     iterator.UnsafeIterator
	priority int
	lenHint  int
}

func newBufferedRootRunsIterator(runs []memtable.Table, start, end []byte) iterator.UnsafeIterator {
	return newBufferedRootRunsIteratorWithDeleted(runs, start, end, false)
}

func newBufferedRootRunsIteratorWithDeleted(runs []memtable.Table, start, end []byte, includeDeleted bool) iterator.UnsafeIterator {
	if len(runs) == 1 && includeDeleted && runs[0] != nil {
		return runs[0].NewIterator(start, end)
	}
	sources := make([]bufferedRootRunIteratorSource, 0, len(runs))
	stableUnsafeSlices := true
	for i, run := range runs {
		if run == nil {
			continue
		}
		if stable, ok := run.(memtable.StableUnsafeIteratorTable); !ok || !stable.StableUnsafeIteratorSlices() {
			stableUnsafeSlices = false
		}
		lenHint := 0
		if start == nil && end == nil {
			lenHint = run.Len()
		}
		sources = append(sources, bufferedRootRunIteratorSource{
			iter:     run.NewIterator(start, end),
			priority: len(runs) - 1 - i,
			lenHint:  lenHint,
		})
	}
	return newBufferedRootRunIteratorSourcesIteratorWithDeleted(sources, start, end, includeDeleted, stableUnsafeSlices)
}

func newBufferedRootRunIteratorSourcesIteratorWithDeleted(sources []bufferedRootRunIteratorSource, start, end []byte, includeDeleted, stableUnsafeSlices bool) iterator.UnsafeIterator {
	it := &bufferedRootRunsIterator{
		iters:              make([]iterator.UnsafeIterator, 0, len(sources)),
		heap:               make(bufferedRootRunHeap, 0, len(sources)),
		includeDeleted:     includeDeleted,
		stableUnsafeSlices: stableUnsafeSlices,
		start:              start,
		end:                end,
	}
	if len(sources) <= len(it.priorityInline) {
		it.priorities = it.priorityInline[:0]
	} else {
		it.priorities = make([]int, 0, len(sources))
	}
	for _, source := range sources {
		if source.iter == nil {
			continue
		}
		if start == nil && end == nil {
			it.lenHint = saturatingAddNonNegativeInt(it.lenHint, source.lenHint)
		}
		idx := len(it.iters)
		it.iters = append(it.iters, source.iter)
		it.priorities = append(it.priorities, source.priority)
		if source.iter.Valid() {
			it.heap = append(it.heap, bufferedRootRunHeapItem{
				idx:      idx,
				priority: source.priority,
				key:      source.iter.UnsafeKey(),
			})
		}
	}
	it.heap.init()
	it.advance()
	return it
}

func (it *bufferedRootRunsIterator) Valid() bool {
	return it != nil && it.valid
}

func (it *bufferedRootRunsIterator) Next() {
	if it == nil || !it.valid {
		return
	}
	if it.hasCur {
		if it.advanceCurrentItemDirect(it.cur) {
			return
		}
		it.hasCur = false
	}
	it.advance()
}

func (it *bufferedRootRunsIterator) Seek(key []byte) {
	if it == nil || it.closed {
		return
	}
	if it.start != nil && bytes.Compare(key, it.start) < 0 {
		key = it.start
	}
	it.heap = it.heap[:0]
	it.valid = false
	it.hasCur = false
	for idx, source := range it.iters {
		source.Seek(key)
		if source.Valid() {
			it.heap.push(bufferedRootRunHeapItem{
				idx:      idx,
				priority: it.priorities[idx],
				key:      source.UnsafeKey(),
			})
		}
	}
	it.advance()
}

func (it *bufferedRootRunsIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.iters[it.cur.idx].UnsafeKey()
}

func (it *bufferedRootRunsIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.iters[it.cur.idx].UnsafeValue()
}

func (it *bufferedRootRunsIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.iters[it.cur.idx].UnsafeEntry()
}

func (it *bufferedRootRunsIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *bufferedRootRunsIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *bufferedRootRunsIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst[:0], it.UnsafeKey()...)
}

func (it *bufferedRootRunsIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst[:0], it.UnsafeValue()...)
}

func (it *bufferedRootRunsIterator) IsDeleted() bool {
	return it.Valid() && it.iters[it.cur.idx].IsDeleted()
}

func (it *bufferedRootRunsIterator) Error() error {
	if it == nil {
		return nil
	}
	if it.firstErr != nil {
		return it.firstErr
	}
	for _, source := range it.iters {
		if err := source.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (it *bufferedRootRunsIterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	var firstErr error
	for _, source := range it.iters {
		if err := source.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	it.valid = false
	it.hasCur = false
	it.heap = nil
	if firstErr != nil {
		it.firstErr = firstErr
	}
	return firstErr
}

func (it *bufferedRootRunsIterator) Domain() (start, end []byte) {
	if it == nil {
		return nil, nil
	}
	return it.start, it.end
}

func (it *bufferedRootRunsIterator) StableUnsafeIteratorSlices() bool {
	return it != nil && it.stableUnsafeSlices
}

func (it *bufferedRootRunsIterator) OrderedUniqueUnsafeIterator() bool {
	return true
}

func (it *bufferedRootRunsIterator) Len() int {
	if it == nil {
		return 0
	}
	return it.lenHint
}

func (it *bufferedRootRunsIterator) advance() {
	it.valid = false
	it.hasCur = false
	for it.heap.Len() > 0 {
		top := it.heap.pop()
		key := top.key
		if it.end != nil && bytes.Compare(key, it.end) >= 0 {
			return
		}
		for it.heap.Len() > 0 {
			next := it.heap.peek()
			if next == nil || !bytes.Equal(next.key, key) {
				break
			}
			shadowed := it.heap.pop()
			it.advanceItem(shadowed)
		}
		if !it.includeDeleted && it.iters[top.idx].IsDeleted() {
			it.advanceItem(top)
			continue
		}
		it.cur = top
		it.hasCur = true
		it.valid = true
		return
	}
}

func (it *bufferedRootRunsIterator) advanceItem(item bufferedRootRunHeapItem) {
	source := it.iters[item.idx]
	source.Next()
	if source.Valid() {
		item.key = source.UnsafeKey()
		it.heap.push(item)
	} else if err := source.Error(); err != nil && it.firstErr == nil {
		it.firstErr = err
	}
}

func (it *bufferedRootRunsIterator) advanceCurrentItemDirect(item bufferedRootRunHeapItem) bool {
	source := it.iters[item.idx]
	source.Next()
	if !source.Valid() {
		if err := source.Error(); err != nil && it.firstErr == nil {
			it.firstErr = err
		}
		return false
	}
	item.key = source.UnsafeKey()
	if it.end != nil && bytes.Compare(item.key, it.end) >= 0 {
		return false
	}
	if !it.includeDeleted && source.IsDeleted() {
		it.heap.push(item)
		return false
	}
	next := it.heap.peek()
	if next == nil || bytes.Compare(item.key, next.key) < 0 {
		it.cur = item
		it.hasCur = true
		it.valid = true
		return true
	}
	it.heap.push(item)
	return false
}

func (c *Collection) prepareIndexedAsyncPublish() (*indexedFlushPublishWork, error) {
	if c == nil || c.writeDomain == nil {
		return nil, nil
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	return c.prepareIndexedAsyncPublishLocked(domain)
}

func (c *Collection) prepareIndexedAsyncPublishLocked(domain *collectionWriteDomain) (*indexedFlushPublishWork, error) {
	if c == nil || c.db == nil || domain == nil || domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return nil, nil
	}
	if len(domain.indexedPublishingUnits) != 0 {
		return nil, errors.New("collections: indexed async publish still in flight")
	}
	if len(domain.indexedFlushUnits) == 0 && len(domain.rootRuns) == 0 {
		return nil, nil
	}
	if domain.catalog == nil {
		return nil, errCollectionNotFound
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return nil, err
	}
	meta := catalog.meta
	c.meta = meta
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return nil, backenddb.ErrClosed
	}
	work := &indexedFlushPublishWork{pin: pin, meta: meta, catalog: catalog}
	defer func() {
		if err != nil && work.pin != nil {
			_ = work.pin.Close()
		}
	}()
	pinnedCatalog, err := loadCollectionCatalog(pin, meta.Name)
	if err != nil {
		return nil, err
	}
	if pinnedCatalog == nil {
		err = errCollectionNotFound
		return nil, err
	}
	if !sameCollectionMeta(pinnedCatalog.meta, meta) {
		err = fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
		return nil, err
	}
	if err = forEachPendingIndexedRootBaseIDLocked(domain, func(rootName string, baseRoot uint64) error {
		if got := pinnedCatalog.rootID(rootName); got != baseRoot {
			return errConcurrentRootModification(meta.Name, rootName)
		}
		return nil
	}); err != nil {
		if errors.Is(err, ErrConcurrentMutation) {
			// This is distinct from revalidateBufferedWriteDomainLocked above:
			// revalidation checks the current root before acquiring the publish
			// pin, while this catches races observed after the pin is held.
			domain.indexedFlushRootBaseMismatches.Add(1)
		}
		return nil, err
	}

	rotateIndexedMutableToFlushUnitLocked(domain)
	units := domain.indexedFlushUnits
	batch, err := buildCoalescedFlushBatchFromUnits(meta, catalog, units)
	if err != nil {
		return nil, err
	}
	if len(batch.rootNames) == 0 {
		_ = pin.Close()
		work.pin = nil
		domain.indexedFlushUnits = nil
		domain.rootMutableRuns = nil
		domain.rootValueArenas = nil
		domain.indexedSemanticRecords = nil
		domain.count = 0
		domain.bufferedBytes = 0
		domain.mutableCount = 0
		domain.mutableBytes = 0
		return nil, nil
	}
	batch.state = coalescedFlushBatchActive
	work.baseSystemRoot = snapshotSystemRoot(pin)
	work.baseCommitSeq = snapshotCommitSeq(pin)
	work.batch = batch

	domain.indexedPublishingUnits = append([]indexedFlushUnit(nil), units...)
	domain.indexedFlushUnits = nil
	domain.writeGeneration++
	return work, nil
}

func (c *Collection) publishPreparedIndexedFlush(work *indexedFlushPublishWork) error {
	if c == nil || c.db == nil || work == nil {
		return nil
	}
	defer func() {
		if work.pin != nil {
			_ = work.pin.Close()
			work.pin = nil
		}
	}()
	if collectionMetaUsesIndexedOverlayRoots(work.meta) {
		materializeStart := time.Now()
		work.batch.state = coalescedFlushBatchMaterializing
		rootOverlayFilters, err := buildCollectionRootOverlayFilters(work.batch.rootNames, work.batch.mergedUnit.rootRuns, work.batch.rootOverlays, work.catalog.rootOverlayFilters)
		if err != nil {
			materializeElapsed := collectionObservedElapsedSince(materializeStart)
			return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
		}
		work.batch.rootOverlayFilters = rootOverlayFilters
		ordered, cleanupDeltas, err := buildBufferedRootOverlayDeltaBatchPublishInputs(work.batch.rootNames, work.batch.mergedUnit.rootRuns, work.batch.mergedUnit.rootPolicies, work.batch.rootOverlays)
		if err != nil {
			materializeElapsed := collectionObservedElapsedSince(materializeStart)
			return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
		}
		work.batch.rootDeltaStats = collectionRootDeltaPlanStatsFromOrdered(work.meta.Name, work.batch.rootNames, ordered)
		materializeElapsed := collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		work.batch.state = coalescedFlushBatchPublishing
		newSystemRoot, rootIDs, publishErr := c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootOverlayDescriptorSystemDeltaIteratorForMeta(work.meta, work.baseCommitSeq, work.baseSystemRoot, work.batch.rootNames, work.batch.rootBaseIDs, work.batch.rootOverlays, rootIDs)
		})
		publishElapsed := collectionObservedElapsedSince(publishStart)
		cleanupDeltas()
		if publishErr == nil && len(rootIDs) != len(work.batch.rootNames) {
			publishErr = unexpectedOrderedRootCountError(work.meta.Name, len(work.batch.rootNames), len(rootIDs))
		}
		completeErr := c.completePreparedIndexedFlush(work, newSystemRoot, rootIDs, publishErr, materializeElapsed+publishElapsed, materializeElapsed, publishElapsed)
		if completeErr != nil {
			return completeErr
		}
		return publishErr
	}
	materializeStart := time.Now()
	work.batch.state = coalescedFlushBatchMaterializing
	ordered, cleanupDeltas, err := buildBufferedRootDeltaBatchPublishInputs(work.batch.rootNames, work.batch.mergedUnit.rootRuns, work.batch.rootBaseIDs, work.batch.mergedUnit.rootPolicies)
	if err != nil {
		materializeElapsed := collectionObservedElapsedSince(materializeStart)
		return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
	}
	work.batch.rootDeltaStats = collectionRootDeltaPlanStatsFromOrdered(work.meta.Name, work.batch.rootNames, ordered)
	materializeElapsed := collectionObservedElapsedSince(materializeStart)
	publishStart := time.Now()
	work.batch.state = coalescedFlushBatchPublishing
	newSystemRoot, rootIDs, publishErr := c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(work.meta, work.baseCommitSeq, work.baseSystemRoot, work.batch.rootNames, work.batch.rootBaseIDs, rootIDs)
	})
	publishElapsed := collectionObservedElapsedSince(publishStart)
	cleanupDeltas()
	if publishErr == nil && len(rootIDs) != len(work.batch.rootNames) {
		publishErr = unexpectedOrderedRootCountError(work.meta.Name, len(work.batch.rootNames), len(rootIDs))
	}
	completeErr := c.completePreparedIndexedFlush(work, newSystemRoot, rootIDs, publishErr, materializeElapsed+publishElapsed, materializeElapsed, publishElapsed)
	if completeErr != nil {
		return completeErr
	}
	return publishErr
}

func buildBufferedRootOverlayDeltaBatchPublishInputs(rootNames []string, rootRuns map[string][]memtable.Table, rootPolicies map[string]backenddb.OrderedRootStoragePolicy, rootOverlays map[string][]uint64) ([]backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	specs := make([]bufferedRootDeltaBatchSpec, len(rootNames))
	for i, rootName := range rootNames {
		specs[i] = bufferedRootDeltaBatchSpec{
			rootName:                  rootName,
			baseRoot:                  overlayDeltaBaseRoot(rootOverlays[rootName]),
			storagePolicy:             rootPolicies[rootName],
			includeDeletedOnColdBuild: true,
			parallelApply:             true,
		}
	}
	return buildBufferedRootDeltaBatchPublishInputsFromSpecs(specs, rootRuns)
}

func overlayDeltaBaseRoot(overlays []uint64) uint64 {
	if len(overlays) == 1 {
		return overlays[0]
	}
	return 0
}

func buildBufferedRootDeltaBatchPublishInputs(rootNames []string, rootRuns map[string][]memtable.Table, rootBaseIDs map[string]uint64, rootPolicies map[string]backenddb.OrderedRootStoragePolicy) ([]backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	specs := make([]bufferedRootDeltaBatchSpec, 0, len(rootNames))
	for _, rootName := range rootNames {
		baseRoot, ok := rootBaseIDs[rootName]
		if !ok {
			return nil, func() {}, fmt.Errorf("collections: buffered indexed flush missing base root for %q", rootName)
		}
		specs = append(specs, bufferedRootDeltaBatchSpec{
			rootName:      rootName,
			baseRoot:      baseRoot,
			storagePolicy: rootPolicies[rootName],
			parallelApply: true,
		})
	}
	return buildBufferedRootDeltaBatchPublishInputsFromSpecs(specs, rootRuns)
}

type bufferedRootDeltaBatchSpec struct {
	rootName                  string
	baseRoot                  uint64
	storagePolicy             backenddb.OrderedRootStoragePolicy
	includeDeletedOnColdBuild bool
	parallelApply             bool
}

func buildBufferedRootDeltaBatchPublishInputsFromSpecs(specs []bufferedRootDeltaBatchSpec, rootRuns map[string][]memtable.Table) ([]backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	ordered := make([]backenddb.OrderedRootDeltaBatchPublishInput, len(specs))
	iterators := make([]iterator.UnsafeIterator, len(specs))
	cleanup := func() {
		for idx := range ordered {
			if ordered[idx].Delta != nil {
				_ = ordered[idx].Delta.Close()
				ordered[idx].Delta = nil
			}
		}
		for _, it := range iterators {
			if it != nil {
				_ = it.Close()
			}
		}
	}
	if len(specs) <= 1 {
		for i := range specs {
			if err := buildBufferedRootDeltaBatchPublishInput(specs[i], rootRuns, &ordered[i], &iterators[i]); err != nil {
				cleanup()
				return nil, func() {}, err
			}
		}
		return ordered, cleanup, nil
	}

	parallelism := runtime.GOMAXPROCS(0)
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > len(specs) {
		parallelism = len(specs)
	}
	if parallelism <= 1 {
		for i := range specs {
			if err := buildBufferedRootDeltaBatchPublishInput(specs[i], rootRuns, &ordered[i], &iterators[i]); err != nil {
				cleanup()
				return nil, func() {}, err
			}
		}
		return ordered, cleanup, nil
	}
	errs := make([]error, len(specs))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				errs[i] = buildBufferedRootDeltaBatchPublishInput(specs[i], rootRuns, &ordered[i], &iterators[i])
			}
		}()
	}
	for i := range specs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
	}
	return ordered, cleanup, nil
}

func buildBufferedRootDeltaBatchPublishInput(spec bufferedRootDeltaBatchSpec, rootRuns map[string][]memtable.Table, ordered *backenddb.OrderedRootDeltaBatchPublishInput, iterOut *iterator.UnsafeIterator) error {
	iter := newBufferedRootRunsIteratorWithDeleted(rootRuns[spec.rootName], nil, nil, true)
	if iterOut != nil {
		*iterOut = iter
	}
	delta, err := backenddb.OrderedRootDeltaBatchFromIterator(iter)
	if err != nil {
		return err
	}
	*ordered = backenddb.OrderedRootDeltaBatchPublishInput{
		BaseRoot:                  spec.baseRoot,
		Delta:                     delta,
		StoragePolicy:             spec.storagePolicy,
		IncludeDeletedOnColdBuild: spec.includeDeletedOnColdBuild,
		ParallelApply:             spec.parallelApply,
	}
	return nil
}

func buildRootDeltaBatchPublishInputsFromTables(collectionName string, rootNames []string, tables []memtable.Table, rootBaseIDs map[string]uint64, policies []backenddb.OrderedRootStoragePolicy) ([]backenddb.OrderedRootDeltaBatchPublishInput, func(), error) {
	if len(rootNames) != len(tables) || len(rootNames) != len(policies) {
		return nil, func() {}, fmt.Errorf("collections: collection %q invalid delta lengths roots=%d tables=%d policies=%d", collectionName, len(rootNames), len(tables), len(policies))
	}
	ordered := make([]backenddb.OrderedRootDeltaBatchPublishInput, 0, len(rootNames))
	iterators := make([]iterator.UnsafeIterator, 0, len(rootNames))
	cleanup := func() {
		for idx := range ordered {
			if ordered[idx].Delta != nil {
				_ = ordered[idx].Delta.Close()
				ordered[idx].Delta = nil
			}
		}
		for _, it := range iterators {
			_ = it.Close()
		}
	}
	for i, rootName := range rootNames {
		baseRoot, ok := rootBaseIDs[rootName]
		if !ok {
			cleanup()
			return nil, func() {}, fmt.Errorf("collections: collection %q delta publish missing base root for %q", collectionName, rootName)
		}
		iter := tables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		delta, err := backenddb.OrderedRootDeltaBatchFromIterator(iter)
		if err != nil {
			cleanup()
			return nil, func() {}, err
		}
		ordered = append(ordered, backenddb.OrderedRootDeltaBatchPublishInput{
			BaseRoot:      baseRoot,
			Delta:         delta,
			StoragePolicy: policies[i],
		})
	}
	return ordered, cleanup, nil
}

func collectionRootDeltaPlanStatsFromOrdered(collectionName string, rootNames []string, ordered []backenddb.OrderedRootDeltaBatchPublishInput) collectionRootDeltaPlanStats {
	var stats collectionRootDeltaPlanStats
	for i, rootName := range rootNames {
		kind := stats.addRoot(collectionName, rootName)
		if i < len(ordered) {
			stats.addBatch(kind, ordered[i].Delta)
		}
	}
	return stats
}

type collectionRootDeltaPlanKind uint8

const (
	collectionRootDeltaPlanUnknown collectionRootDeltaPlanKind = iota
	collectionRootDeltaPlanPrimary
	collectionRootDeltaPlanTemplate
	collectionRootDeltaPlanIndexState
	collectionRootDeltaPlanSecondary
)

func (stats *collectionRootDeltaPlanStats) addRoot(collectionName, rootName string) collectionRootDeltaPlanKind {
	if stats == nil || rootName == "" {
		return collectionRootDeltaPlanUnknown
	}
	switch {
	case rootName == collectionPrimaryRootName(collectionName):
		stats.primaryRoots++
		return collectionRootDeltaPlanPrimary
	case rootName == collectionTemplateRootName(collectionName):
		stats.templateRoots++
		return collectionRootDeltaPlanTemplate
	case rootName == collectionIndexStateRootName(collectionName):
		stats.indexStateRoots++
		return collectionRootDeltaPlanIndexState
	case strings.HasPrefix(rootName, collectionName+"/index/"):
		stats.secondaryRoots++
		return collectionRootDeltaPlanSecondary
	}
	return collectionRootDeltaPlanUnknown
}

func (stats *collectionRootDeltaPlanStats) addBatch(kind collectionRootDeltaPlanKind, delta *batch.Batch) {
	if stats == nil || delta == nil {
		return
	}
	for _, entry := range delta.SortedEntries() {
		stats.entries++
		stats.keyBytes += uint64(len(entry.Key))
		if kind == collectionRootDeltaPlanPrimary {
			stats.primaryEntries++
			stats.primaryKeyBytes += uint64(len(entry.Key))
		}
		if entry.Type == batch.OpDelete {
			stats.tombstones++
			if kind == collectionRootDeltaPlanPrimary {
				stats.primaryTombstones++
			}
			continue
		}
		valueBytes := uint64(len(entry.Value))
		if entry.IsPtr {
			valueBytes += page.ValuePtrSize
		}
		stats.valueBytes += valueBytes
		if kind == collectionRootDeltaPlanPrimary {
			stats.primaryValueBytes += valueBytes
		}
	}
}

func (c *Collection) completePreparedIndexedFlush(work *indexedFlushPublishWork, newSystemRoot uint64, rootIDs []uint64, publishErr error, elapsed, materializeElapsed, publishElapsed time.Duration) error {
	if c == nil || c.writeDomain == nil || work == nil {
		return publishErr
	}
	completeStart := time.Now()
	observedElapsed := func() time.Duration {
		return elapsed + collectionObservedElapsedSince(completeStart)
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	preservePrimaryRunIndex := domain.primaryRunIndex != nil
	if publishErr != nil {
		if errors.Is(publishErr, ErrConcurrentMutation) {
			domain.indexedFlushRootBaseMismatches.Add(1)
		}
		removed, owned := removeIndexedPublishingWorkUnitsLocked(domain, work.batch.units)
		if !owned {
			err := errors.Join(errIndexedFlushLostOwnership, publishErr)
			work.batch.state = coalescedFlushBatchLostOwnership
			domain.indexedFlushLostOwnership.Add(1)
			domain.observeIndexedFlush(len(work.batch.units), work.batch.docCount, work.batch.byteCount, work.batch.rootRunCount, work.batch.rootCount, observedElapsed(), materializeElapsed, publishElapsed, err)
			return err
		}
		if len(removed) > 0 {
			domain.indexedFlushRequeues.Add(1)
			domain.indexedFlushRequeuedUnits.Add(uint64(len(removed)))
		}
		work.batch.state = coalescedFlushBatchRequeued
		domain.indexedFlushUnits = append(removed, domain.indexedFlushUnits...)
		rebuildBufferedPendingIndexesLocked(domain, work.meta.Name, preservePrimaryRunIndex)
		domain.observeIndexedFlush(len(work.batch.units), work.batch.docCount, work.batch.byteCount, work.batch.rootRunCount, work.batch.rootCount, observedElapsed(), materializeElapsed, publishElapsed, publishErr)
		return publishErr
	}
	baseCatalog := domain.catalog
	if baseCatalog == nil {
		baseCatalog = work.catalog
	}
	overlayPublish := collectionMetaUsesIndexedOverlayRoots(work.meta)
	nextCatalog := cloneCatalogWithRootUpdates(baseCatalog, work.meta, work.batch.rootNames, rootIDs)
	if overlayPublish {
		nextCatalog = cloneCatalogWithRootOverlays(baseCatalog, work.meta, work.batch.rootNames, rootIDs)
		nextCatalog = cloneCatalogWithRootOverlayFilters(nextCatalog, work.batch.rootNames, rootIDs, work.batch.rootOverlayFilters)
	}
	oldPublishing, owned := removeIndexedPublishingWorkUnitsLocked(domain, work.batch.units)
	if !owned {
		work.batch.state = coalescedFlushBatchLostOwnership
		domain.indexedFlushLostOwnership.Add(1)
		domain.observeIndexedFlush(len(work.batch.units), work.batch.docCount, work.batch.byteCount, work.batch.rootRunCount, work.batch.rootCount, observedElapsed(), materializeElapsed, publishElapsed, errIndexedFlushLostOwnership)
		return errIndexedFlushLostOwnership
	}
	work.batch.state = coalescedFlushBatchPublished
	domain.loaded = true
	domain.meta = work.meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = nextCatalog.rootID(collectionPrimaryRootName(work.meta.Name))
	domain.count = subtractNonNegativeInt(domain.count, work.batch.docCount)
	domain.bufferedBytes = subtractNonNegativeInt64(domain.bufferedBytes, work.batch.byteCount)
	domain.clearIndexedAsyncFlushError()
	if !overlayPublish {
		retargetPendingIndexedRootBaseIDsLocked(domain, work.batch.rootNames, work.batch.rootBaseIDs, rootIDs)
	}
	rebuildBufferedPendingIndexesLocked(domain, work.meta.Name, preservePrimaryRunIndex)
	c.meta = work.meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetIndexedFlushUnits(oldPublishing)
	domain.observeIndexedFlush(len(work.batch.units), work.batch.docCount, work.batch.byteCount, work.batch.rootRunCount, work.batch.rootCount, observedElapsed(), materializeElapsed, publishElapsed, nil)
	domain.observeRootDeltaPlan(work.batch.rootDeltaStats)
	return nil
}

func (c *Collection) flushBufferedIndexedLocked(domain *collectionWriteDomain) (err error) {
	if domain == nil || domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return nil
	}
	if len(domain.indexedPublishingUnits) > 0 {
		return errors.New("collections: indexed async publish still in flight")
	}
	if domain.catalog == nil {
		return errCollectionNotFound
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return err
	}
	meta := catalog.meta
	c.meta = meta
	pin := c.db.AcquireSnapshot()
	if pin == nil {
		return backenddb.ErrClosed
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()
	pinnedCatalog, err := loadCollectionCatalog(pin, meta.Name)
	if err != nil {
		return err
	}
	if pinnedCatalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(pinnedCatalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	if err := forEachPendingIndexedRootBaseIDLocked(domain, func(rootName string, baseRoot uint64) error {
		if got := pinnedCatalog.rootID(rootName); got != baseRoot {
			return errConcurrentRootModification(meta.Name, rootName)
		}
		return nil
	}); err != nil {
		return err
	}

	rotateIndexedMutableToFlushUnitLocked(domain)
	flushUnit := mergedIndexedFlushUnitLocked(domain)
	rootNames := orderedBufferedRootNames(meta, flushUnit.rootRuns)
	if len(rootNames) == 0 {
		domain.indexedFlushUnits = nil
		domain.rootMutableRuns = nil
		domain.rootValueArenas = nil
		domain.indexedSemanticRecords = nil
		domain.count = 0
		domain.bufferedBytes = 0
		domain.mutableCount = 0
		domain.mutableBytes = 0
		return nil
	}
	flushDocs := domain.count
	flushBytes := domain.bufferedBytes
	flushUnits := len(domain.indexedFlushUnits)
	flushRootRuns := bufferedIndexedRootRunCount(domain)
	flushRoots := len(rootNames)
	flushStart := time.Now()
	var materializeElapsed time.Duration
	var publishElapsed time.Duration
	defer func() {
		domain.observeIndexedFlush(flushUnits, flushDocs, flushBytes, flushRootRuns, flushRoots, collectionObservedElapsedSince(flushStart), materializeElapsed, publishElapsed, err)
	}()
	baseSystemRoot := snapshotSystemRoot(pin)
	baseCommitSeq := snapshotCommitSeq(pin)
	baseRootIDs := make(map[string]uint64, len(rootNames))
	rootOverlays := make(map[string][]uint64, len(rootNames))
	for _, rootName := range rootNames {
		baseRoot, ok := flushUnit.rootBaseIDs[rootName]
		if !ok {
			return fmt.Errorf("collections: buffered indexed flush missing base root for %q", rootName)
		}
		baseRootIDs[rootName] = baseRoot
		rootOverlays[rootName] = append([]uint64(nil), catalog.overlayRootIDs(rootName)...)
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	var rootOverlayFilters map[string]collectionRootOverlayFilter
	if collectionMetaUsesIndexedOverlayRoots(meta) {
		materializeStart := time.Now()
		rootOverlayFilters, err = buildCollectionRootOverlayFilters(rootNames, flushUnit.rootRuns, rootOverlays, catalog.rootOverlayFilters)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		ordered, cleanupDeltas, err := buildBufferedRootOverlayDeltaBatchPublishInputs(rootNames, flushUnit.rootRuns, flushUnit.rootPolicies, rootOverlays)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		rootDeltaStats := collectionRootDeltaPlanStatsFromOrdered(meta.Name, rootNames, ordered)
		materializeElapsed = collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootOverlayDescriptorSystemDeltaIteratorForMeta(meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootOverlays, rootIDs)
		})
		publishElapsed = collectionObservedElapsedSince(publishStart)
		cleanupDeltas()
		if err == nil {
			domain.observeRootDeltaPlan(rootDeltaStats)
		}
	} else {
		materializeStart := time.Now()
		ordered, cleanupDeltas, err := buildBufferedRootDeltaBatchPublishInputs(rootNames, flushUnit.rootRuns, flushUnit.rootBaseIDs, flushUnit.rootPolicies)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		rootDeltaStats := collectionRootDeltaPlanStatsFromOrdered(meta.Name, rootNames, ordered)
		materializeElapsed = collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		})
		publishElapsed = collectionObservedElapsedSince(publishStart)
		cleanupDeltas()
		if err == nil {
			domain.observeRootDeltaPlan(rootDeltaStats)
		}
	}
	if err != nil {
		return err
	}
	if len(rootIDs) != len(rootNames) {
		return unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(domain.catalog, meta, rootNames, rootIDs)
	if collectionMetaUsesIndexedOverlayRoots(meta) {
		nextCatalog = cloneCatalogWithRootOverlays(domain.catalog, meta, rootNames, rootIDs)
		nextCatalog = cloneCatalogWithRootOverlayFilters(nextCatalog, rootNames, rootIDs, rootOverlayFilters)
	}
	oldUnits := domain.indexedFlushUnits
	oldRuns := domain.rootRuns
	domain.loaded = true
	domain.meta = meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = nextCatalog.rootID(collectionPrimaryRootName(meta.Name))
	domain.indexedFlushUnits = nil
	domain.rootRuns = nil
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.rootValueArenas = nil
	domain.indexedSemanticRecords = nil
	domain.rootRunCount = 0
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	oldUniqueValueRuns := domain.uniqueValueRuns
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	domain.count = 0
	domain.bufferedBytes = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetIndexedFlushUnits(oldUnits)
	for _, runs := range oldRuns {
		resetCollectionTables(runs)
	}
	for _, runs := range oldUniqueValueRuns {
		resetCollectionTables(runs)
	}
	return nil
}

func rotateIndexedMutableToFlushUnitLocked(domain *collectionWriteDomain) bool {
	if domain == nil || len(domain.rootRuns) == 0 {
		return false
	}
	freezeMutableIndexedRunMapsLocked(domain)
	unit := indexedFlushUnit{
		rootRuns:        domain.rootRuns,
		rootPolicies:    domain.rootPolicies,
		rootBaseIDs:     domain.rootBaseIDs,
		uniqueValueRuns: domain.uniqueValueRuns,
		semanticRecords: domain.indexedSemanticRecords,
		arenaRefs:       domain.rootValueArenas,
		docCount:        domain.mutableCount,
		byteCount:       domain.mutableBytes,
		rootRunCount:    domain.rootRunCount,
	}
	domain.indexedFlushUnits = append(domain.indexedFlushUnits, unit)
	domain.rootRuns = nil
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.uniqueValueRuns = nil
	domain.rootValueArenas = nil
	domain.indexedSemanticRecords = nil
	domain.rootRunCount = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	return true
}

func removeIndexedPublishingUnitsLocked(domain *collectionWriteDomain, n int) []indexedFlushUnit {
	if domain == nil || n <= 0 || len(domain.indexedPublishingUnits) == 0 {
		return nil
	}
	if n > len(domain.indexedPublishingUnits) {
		n = len(domain.indexedPublishingUnits)
	}
	removed := append([]indexedFlushUnit(nil), domain.indexedPublishingUnits[:n]...)
	remaining := domain.indexedPublishingUnits[n:]
	if len(remaining) == 0 {
		domain.indexedPublishingUnits = nil
	} else {
		domain.indexedPublishingUnits = remaining
	}
	return removed
}

func removeIndexedPublishingWorkUnitsLocked(domain *collectionWriteDomain, units []indexedFlushUnit) ([]indexedFlushUnit, bool) {
	if len(units) == 0 {
		return nil, true
	}
	if domain == nil || len(domain.indexedPublishingUnits) != len(units) {
		return nil, false
	}
	for i := range units {
		if !sameIndexedFlushUnitTables(domain.indexedPublishingUnits[i], units[i]) {
			return nil, false
		}
	}
	return removeIndexedPublishingUnitsLocked(domain, len(units)), true
}

func sameIndexedFlushUnitTables(a, b indexedFlushUnit) bool {
	return sameTableRunMap(a.rootRuns, b.rootRuns) && sameTableRunMap(a.uniqueValueRuns, b.uniqueValueRuns)
}

func sameTableRunMap(a, b map[string][]memtable.Table) bool {
	if len(a) != len(b) {
		return false
	}
	for name, aRuns := range a {
		bRuns, ok := b[name]
		if !ok || len(aRuns) != len(bRuns) {
			return false
		}
		for i := range aRuns {
			if aRuns[i] != bRuns[i] {
				return false
			}
		}
	}
	return true
}

func retargetPendingIndexedRootBaseIDsLocked(domain *collectionWriteDomain, rootNames []string, oldBaseIDs map[string]uint64, newRootIDs []uint64) {
	if domain == nil || len(rootNames) == 0 || len(newRootIDs) == 0 {
		return
	}
	retarget := func(rootBaseIDs map[string]uint64) {
		for i, rootName := range rootNames {
			if i >= len(newRootIDs) {
				return
			}
			oldBase, ok := oldBaseIDs[rootName]
			if !ok {
				continue
			}
			if current, ok := rootBaseIDs[rootName]; ok && current == oldBase {
				rootBaseIDs[rootName] = newRootIDs[i]
			}
		}
	}
	for i := range domain.indexedFlushUnits {
		retarget(domain.indexedFlushUnits[i].rootBaseIDs)
	}
	retarget(domain.rootBaseIDs)
}

func buildCoalescedFlushBatchFromUnits(meta CollectionMeta, catalog *collectionCatalog, units []indexedFlushUnit) (coalescedFlushBatch, error) {
	merged := mergedIndexedFlushUnits(units)
	rootNames := orderedBufferedRootNames(meta, merged.rootRuns)
	batch := coalescedFlushBatch{
		state:           coalescedFlushBatchQueued,
		units:           append([]indexedFlushUnit(nil), units...),
		mergedUnit:      merged,
		semanticRecords: cloneIndexedSemanticRecords(merged.semanticRecords),
		rootNames:       rootNames,
		docCount:        merged.docCount,
		byteCount:       merged.byteCount,
		rootRunCount:    indexedFlushUnitRootRunCount(merged),
		rootCount:       len(rootNames),
	}
	if len(rootNames) == 0 {
		return batch, nil
	}
	if catalog == nil {
		return coalescedFlushBatch{}, errCollectionNotFound
	}
	batch.rootBaseIDs = make(map[string]uint64, len(rootNames))
	batch.rootOverlays = make(map[string][]uint64, len(rootNames))
	for _, rootName := range rootNames {
		baseRoot, ok := merged.rootBaseIDs[rootName]
		if !ok {
			return coalescedFlushBatch{}, fmt.Errorf("collections: buffered indexed flush missing base root for %q", rootName)
		}
		batch.rootBaseIDs[rootName] = baseRoot
		batch.rootOverlays[rootName] = append([]uint64(nil), catalog.overlayRootIDs(rootName)...)
	}
	return batch, nil
}

func mergedIndexedFlushUnits(units []indexedFlushUnit) indexedFlushUnit {
	if len(units) == 0 {
		return indexedFlushUnit{}
	}
	if len(units) == 1 {
		return units[0]
	}
	unit := indexedFlushUnit{
		rootRuns:        make(map[string][]memtable.Table, len(units)),
		rootPolicies:    make(map[string]backenddb.OrderedRootStoragePolicy, len(units)),
		rootBaseIDs:     make(map[string]uint64, len(units)),
		uniqueValueRuns: make(map[string][]memtable.Table, len(units)),
	}
	for _, pending := range units {
		mergeIndexedFlushUnit(&unit, pending)
	}
	if len(unit.rootRuns) == 0 {
		unit.rootRuns = nil
	}
	if len(unit.rootPolicies) == 0 {
		unit.rootPolicies = nil
	}
	if len(unit.rootBaseIDs) == 0 {
		unit.rootBaseIDs = nil
	}
	if len(unit.uniqueValueRuns) == 0 {
		unit.uniqueValueRuns = nil
	}
	if len(unit.semanticRecords) == 0 {
		unit.semanticRecords = nil
	}
	return unit
}

func mergedIndexedFlushUnitLocked(domain *collectionWriteDomain) indexedFlushUnit {
	if domain == nil {
		return indexedFlushUnit{}
	}
	if len(domain.indexedFlushUnits) == 1 && len(domain.rootRuns) == 0 {
		return domain.indexedFlushUnits[0]
	}
	unit := indexedFlushUnit{
		rootRuns:        make(map[string][]memtable.Table, len(domain.rootRuns)+len(domain.indexedFlushUnits)),
		rootPolicies:    make(map[string]backenddb.OrderedRootStoragePolicy, len(domain.rootPolicies)+len(domain.indexedFlushUnits)),
		rootBaseIDs:     make(map[string]uint64, len(domain.rootBaseIDs)+len(domain.indexedFlushUnits)),
		uniqueValueRuns: make(map[string][]memtable.Table, len(domain.uniqueValueRuns)+len(domain.indexedFlushUnits)),
	}
	for _, pending := range domain.indexedFlushUnits {
		mergeIndexedFlushUnit(&unit, pending)
	}
	mergeIndexedFlushUnit(&unit, indexedFlushUnit{
		rootRuns:        domain.rootRuns,
		rootPolicies:    domain.rootPolicies,
		rootBaseIDs:     domain.rootBaseIDs,
		uniqueValueRuns: domain.uniqueValueRuns,
		semanticRecords: domain.indexedSemanticRecords,
		arenaRefs:       domain.rootValueArenas,
		rootRunCount:    domain.rootRunCount,
	})
	if len(unit.rootRuns) == 0 {
		unit.rootRuns = nil
	}
	if len(unit.rootPolicies) == 0 {
		unit.rootPolicies = nil
	}
	if len(unit.rootBaseIDs) == 0 {
		unit.rootBaseIDs = nil
	}
	if len(unit.uniqueValueRuns) == 0 {
		unit.uniqueValueRuns = nil
	}
	if len(unit.semanticRecords) == 0 {
		unit.semanticRecords = nil
	}
	return unit
}

func mergeIndexedFlushUnit(dst *indexedFlushUnit, src indexedFlushUnit) {
	if dst == nil {
		return
	}
	appendTableRunMap(dst.rootRuns, src.rootRuns)
	appendTableRunMap(dst.uniqueValueRuns, src.uniqueValueRuns)
	dst.semanticRecords = append(dst.semanticRecords, src.semanticRecords...)
	dst.arenaRefs = append(dst.arenaRefs, src.arenaRefs...)
	for rootName, policy := range src.rootPolicies {
		dst.rootPolicies[rootName] = policy
	}
	for rootName, baseRoot := range src.rootBaseIDs {
		if _, ok := dst.rootBaseIDs[rootName]; !ok {
			dst.rootBaseIDs[rootName] = baseRoot
		}
	}
	dst.docCount = saturatingAddNonNegativeInt(dst.docCount, src.docCount)
	dst.byteCount = saturatingAddNonNegativeInt64(dst.byteCount, src.byteCount)
	dst.rootRunCount = saturatingAddNonNegativeInt(dst.rootRunCount, indexedFlushUnitRootRunCount(src))
}

func indexedFlushUnitRootRunCount(unit indexedFlushUnit) int {
	if unit.rootRunCount > 0 || len(unit.rootRuns) == 0 {
		return unit.rootRunCount
	}
	total := 0
	for _, runs := range unit.rootRuns {
		total = saturatingAddNonNegativeInt(total, len(runs))
	}
	return total
}

func resetIndexedFlushUnits(units []indexedFlushUnit) {
	for _, unit := range units {
		for _, runs := range unit.rootRuns {
			resetCollectionTables(runs)
		}
		for _, runs := range unit.uniqueValueRuns {
			resetCollectionTables(runs)
		}
	}
}

func orderedBufferedRootNames(meta CollectionMeta, runs map[string][]memtable.Table) []string {
	if len(runs) == 0 {
		return nil
	}
	out := make([]string, 0, len(runs))
	seen := make(map[string]struct{}, len(runs))
	for _, rootName := range collectionRootNames(meta) {
		if len(runs[rootName]) > 0 {
			out = append(out, rootName)
			seen[rootName] = struct{}{}
		}
	}
	extra := make([]string, 0)
	for rootName, rootRuns := range runs {
		if _, ok := seen[rootName]; !ok {
			if len(rootRuns) > 0 {
				extra = append(extra, rootName)
			}
		}
	}
	sort.Strings(extra)
	out = append(out, extra...)
	return out
}

func (c *Collection) insertOneNoIndex(id, document []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, err
	}
	c.meta = catalog.meta
	if len(c.meta.Indexes) > 0 {
		_ = snap.Close()
		return c.insertOneViaBatch(id, document)
	}
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if plannerOptions.documentFormat != DocumentFormatJSON {
		_ = snap.Close()
		return c.insertOneViaBatch(id, document)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	rootName := collectionPrimaryRootName(c.meta.Name)
	if entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, rootName, id); err == nil {
		if entry.Flags&node.FlagTombstone == 0 {
			_ = snap.Close()
			return nil, ErrDocumentExists
		}
	} else if !errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		return nil, err
	}
	baseRoot := catalog.rootID(rootName)
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	resultID := bytes.Clone(id)
	iter := &systemTargetIterator{entries: []systemTargetEntry{{
		key:   resultID,
		value: bytes.Clone(document),
	}}}
	defer func() { _ = iter.Close() }()

	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: plannerOptions.dataStoragePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, map[string]uint64{rootName: baseRoot}, rootIDs)
	})
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != 1 {
		return nil, unexpectedOrderedRootCountError(c.meta.Name, 1, len(rootIDs))
	}
	c.rememberCatalogAtSystemRoot(newSystemRoot, cloneCatalogWithRootUpdates(catalog, c.meta, []string{rootName}, rootIDs))
	return resultID, nil
}

func (c *Collection) insertOneViaBatch(id, document []byte) ([]byte, error) {
	ids, err := c.InsertBatch([][]byte{id}, [][]byte{document})
	if err != nil {
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

func (c *Collection) InsertBatch(ids, documents [][]byte) ([][]byte, error) {
	return c.insertBatch(ids, documents, false)
}

// InsertBatchValidatedBSON inserts native BSON documents that the caller has
// already validated. It is intended for wire-protocol gateways that validate
// BSON while parsing the request and need to avoid a duplicate full-document
// validation pass on the insert hot path.
func (c *Collection) InsertBatchValidatedBSON(ids, documents [][]byte) ([][]byte, error) {
	return c.insertBatch(ids, documents, true)
}

func (c *Collection) insertBatch(ids, documents [][]byte, trustedValidBSON bool) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}

	return retryInsertBatchMutation(func() ([][]byte, error) {
		return c.insertBatchOnce(ids, documents, trustedValidBSON)
	})
}

func retryInsertBatchMutation(run func() ([][]byte, error)) ([][]byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		resultIDs, err := run()
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return resultIDs, err
	}
	return nil, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) insertBatchOnce(ids, documents [][]byte, trustedValidBSON bool) ([][]byte, error) {
	unlockMutation := c.lockMutation()
	mutationLocked := true
	unlockIfLocked := func() {
		if mutationLocked {
			unlockMutation.Unlock()
			mutationLocked = false
		}
	}
	defer unlockIfLocked()

	if len(documents) == 0 {
		c.setLastInsertStats(CollectionInsertStats{
			Documents: 0,
			Indexes:   len(c.meta.Indexes),
		})
		return nil, nil
	}
	if err := c.flushBufferedNoIndex(); err != nil {
		return nil, err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	closePlanningSnapshot := func() {
		if snap != nil {
			_ = snap.Close()
			snap = nil
		}
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	if catalog == nil {
		closePlanningSnapshot()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	meta := catalog.meta
	c.meta = meta
	plannerOptions, err := collectionPlannerOptions(meta)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	indexedMemtablesEnabled := c.shouldBufferIndexedInserts(meta)
	bufferIndexedInserts := c.shouldBufferIndexedInsertBatch(meta, len(documents))
	if indexedMemtablesEnabled && !bufferIndexedInserts {
		closePlanningSnapshot()
		if err := c.flushBufferedWrites(); err != nil {
			return nil, err
		}
		snap = c.db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		catalog, err = c.catalogForSnapshot(snap)
		if err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		if catalog == nil {
			closePlanningSnapshot()
			return nil, errCollectionNotFound
		}
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		meta = catalog.meta
		c.meta = meta
		plannerOptions, err = collectionPlannerOptions(meta)
		if err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
		if err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		indexedMemtablesEnabled = c.shouldBufferIndexedInserts(meta)
		bufferIndexedInserts = c.shouldBufferIndexedInsertBatch(meta, len(documents))
	}
	if bufferIndexedInserts {
		plannerOptions = collectionOptionsWithBufferedTemplateV1Resolver(plannerOptions, c.writeDomain, meta.Name)
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	if len(meta.Indexes) == 0 && plannerOptions.documentFormat == DocumentFormatJSON {
		return c.insertBatchNoIndex(catalog, snap, baseCommitSeq, baseSystemRoot, plannerOptions, ids, documents)
	}

	unlockForPlanning := shouldUnlockInsertPlanning(plannerOptions, indexedMemtablesEnabled, bufferIndexedInserts)
	if unlockForPlanning {
		closePlanningSnapshot()
		unlockIfLocked()
	}

	planner := insertBatchPlanner{
		collection:     meta.Name,
		primaryRoot:    collectionPrimaryRootName(meta.Name),
		templateRoot:   collectionTemplateRootName(meta.Name),
		indexStateRoot: collectionIndexStateRootName(meta.Name),
		indexes:        plannerIndexes(meta.Indexes),
		options:        plannerOptions,
	}
	if bufferIndexedInserts {
		planner.buildPrimaryVal = clonePrimaryDocument
		planner.cloneTemplateRunValues = true
	}
	plan, err := planner.planInsertBatch(ids, documents)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	if bufferIndexedInserts {
		plan.stats.BufferedIndexedBatches = 1
	} else if indexedMemtablesEnabled {
		plan.stats.BufferedIndexedBypassBatches = 1
	}
	if len(plan.runs) == 0 {
		closePlanningSnapshot()
		c.setLastInsertStats(plan.stats.CollectionInsertStats)
		return plan.resultIDs, nil
	}

	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)

	if bufferIndexedInserts {
		resultIDs, err := cloneBatchDocumentIDs(plan.resultIDs)
		if err != nil {
			closePlanningSnapshot()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		pin, currentCatalog, pinCommitSeq, pinSystemRoot, err := c.lockAndValidateInsertBatchPlan(&mutationLocked, &unlockMutation, snap, catalog, meta, rootNames, baseRootIDs, plan)
		if err != nil {
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		bufferFlushElapsed, err := c.bufferIndexedInsertPlanLocked(currentCatalog, pinCommitSeq, pinSystemRoot, plan)
		_ = pin.Close()
		if err != nil {
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		plan.stats.Publish += bufferFlushElapsed
		c.setLastInsertStats(plan.stats.CollectionInsertStats)
		return resultIDs, nil
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		closePlanningSnapshot()
		resetCollectionRunTables(plan.runs)
		return nil, err
	}

	pin, currentCatalog, pinCommitSeq, pinSystemRoot, err := c.lockAndValidateInsertBatchPlan(&mutationLocked, &unlockMutation, snap, catalog, meta, rootNames, baseRootIDs, plan)
	if err != nil {
		resetCollectionRunTables(plan.runs)
		return nil, err
	}
	baseCommitSeq = pinCommitSeq
	baseSystemRoot = pinSystemRoot
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.runs))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.runs))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionRunTables(plan.runs)
	}()
	for _, run := range plan.runs {
		iter := run.table.NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[run.name],
			Iter:          iter,
			StoragePolicy: run.storagePolicy,
		})
	}

	publishStart := time.Now()
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	plan.stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.runs) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(plan.runs), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(currentCatalog, meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.setLastInsertStats(plan.stats.CollectionInsertStats)
	return plan.resultIDs, nil
}

func shouldUnlockInsertPlanning(opts collectionOptions, indexedMemtablesEnabled, bufferIndexedInserts bool) bool {
	if opts.documentFormat == DocumentFormatTemplateV1 {
		return false
	}
	if indexedMemtablesEnabled && !bufferIndexedInserts {
		return false
	}
	return true
}

func insertBatchPlanRootNamesAndBaseIDs(plan *insertBatchPlan, catalog *collectionCatalog) ([]string, map[string]uint64) {
	if plan == nil {
		return nil, nil
	}
	rootNames := make([]string, len(plan.runs))
	baseRootIDs := make(map[string]uint64, len(plan.runs))
	for i, run := range plan.runs {
		rootNames[i] = run.name
		if catalog != nil {
			baseRootIDs[run.name] = catalog.rootID(run.name)
		}
	}
	return rootNames, baseRootIDs
}

type insertBatchValidationContext struct {
	snap           *backenddb.Snapshot
	catalog        *collectionCatalog
	meta           CollectionMeta
	rootNames      []string
	baseRootIDs    map[string]uint64
	plan           *insertBatchPlan
	allowRootDrift bool
}

func (c *Collection) lockAndValidateInsertBatchPlan(
	mutationLocked *bool,
	unlockMutation *collectionMutationUnlock,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	meta CollectionMeta,
	rootNames []string,
	baseRootIDs map[string]uint64,
	plan *insertBatchPlan,
) (*backenddb.Snapshot, *collectionCatalog, uint64, uint64, error) {
	plannedWithMutationLocked := *mutationLocked
	if !*mutationLocked {
		*unlockMutation = c.lockMutation()
		*mutationLocked = true
	}
	c.meta = meta
	validation := insertBatchValidationContext{
		snap:           snap,
		catalog:        catalog,
		meta:           meta,
		rootNames:      rootNames,
		baseRootIDs:    baseRootIDs,
		plan:           plan,
		allowRootDrift: !plannedWithMutationLocked,
	}
	pin, currentCatalog, err := c.validateInsertBatchPlanAfterPlanningLocked(plannedWithMutationLocked, validation)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if !plannedWithMutationLocked {
		updateInsertBatchBaseRootIDs(rootNames, baseRootIDs, currentCatalog)
	}
	return pin, currentCatalog, snapshotCommitSeq(pin), snapshotSystemRoot(pin), nil
}

func updateInsertBatchBaseRootIDs(rootNames []string, baseRootIDs map[string]uint64, catalog *collectionCatalog) {
	if catalog == nil || baseRootIDs == nil {
		return
	}
	for _, rootName := range rootNames {
		baseRootIDs[rootName] = catalog.rootID(rootName)
	}
}

func (c *Collection) validateInsertBatchPlanAfterPlanningLocked(plannedWithMutationLocked bool, validation insertBatchValidationContext) (*backenddb.Snapshot, *collectionCatalog, error) {
	if plannedWithMutationLocked {
		if err := c.validateInsertBatchPlanWithSnapshotLocked(validation); err != nil {
			if validation.snap != nil {
				_ = validation.snap.Close()
			}
			return nil, nil, err
		}
		return validation.snap, validation.catalog, nil
	}
	current, currentCatalog, err := c.validateInsertBatchPlanLocked(validation)
	if validation.snap != nil {
		_ = validation.snap.Close()
	}
	if err != nil {
		return nil, nil, err
	}
	return current, currentCatalog, nil
}

func (c *Collection) validateInsertBatchPlanLocked(validation insertBatchValidationContext) (*backenddb.Snapshot, *collectionCatalog, error) {
	if c == nil || c.db == nil {
		return nil, nil, backenddb.ErrClosed
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, nil, backenddb.ErrClosed
	}
	catalog, err := loadCollectionCatalog(current, validation.meta.Name)
	if err != nil {
		_ = current.Close()
		return nil, nil, err
	}
	if catalog == nil {
		_ = current.Close()
		return nil, nil, errCollectionNotFound
	}
	validation.snap = current
	validation.catalog = catalog
	if err := c.validateInsertBatchPlanWithSnapshotLocked(validation); err != nil {
		_ = current.Close()
		return nil, nil, err
	}
	return current, catalog, nil
}

func (c *Collection) validateInsertBatchPlanWithSnapshotLocked(validation insertBatchValidationContext) error {
	if c == nil || c.db == nil || validation.snap == nil {
		return backenddb.ErrClosed
	}
	if validation.catalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(validation.catalog.meta, validation.meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", validation.meta.Name)
	}
	for _, rootName := range validation.rootNames {
		want, ok := validation.baseRootIDs[rootName]
		if !ok {
			return fmt.Errorf("collections: insert plan missing base root id collection=%q root=%q", validation.meta.Name, rootName)
		}
		if got := validation.catalog.rootID(rootName); got != want && !validation.allowRootDrift {
			return errConcurrentRootModification(validation.meta.Name, rootName)
		}
	}
	return validation.plan.checkPersistedConflicts(validation.snap, validation.catalog)
}

func collectionOptionsWithTrustedBSONDocuments(opts collectionOptions, trusted bool) (collectionOptions, error) {
	if !trusted {
		return opts, nil
	}
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return collectionOptions{}, errors.New("collections: trusted BSON insert requires BSON document format")
	}
	opts.trustedBSONDocuments = true
	return opts, nil
}

func (c *Collection) insertBatchNoIndex(
	catalog *collectionCatalog,
	snap *backenddb.Snapshot,
	baseCommitSeq uint64,
	baseSystemRoot uint64,
	plannerOptions collectionOptions,
	ids, documents [][]byte,
) ([][]byte, error) {
	if len(ids) != len(documents) {
		_ = snap.Close()
		return nil, fmt.Errorf("collections: caller-provided batch ids length mismatch")
	}

	stats := CollectionInsertStats{
		Documents: len(documents),
		Indexes:   len(c.meta.Indexes),
	}
	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	entries := make([]noIndexBatchEntry, len(documents))
	for i := range documents {
		id := resultIDs[i]
		entries[i] = noIndexBatchEntry{
			id:       id,
			document: documents[i],
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].id, entries[j].id) < 0
	})
	phaseStart := time.Now()
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].id, entries[i].id) {
			_ = snap.Close()
			return nil, ErrDuplicateDocumentID
		}
	}

	rootName := collectionPrimaryRootName(c.meta.Name)
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		baseRoot := catalog.rootID(rootName)
		if baseRoot != 0 {
			keys := make([][]byte, len(entries))
			for i := range entries {
				keys[i] = entries[i].id
			}
			exists, err := snap.HasAnySortedAtRoot(baseRoot, keys)
			if err != nil {
				_ = snap.Close()
				return nil, err
			}
			if exists {
				_ = snap.Close()
				return nil, ErrDocumentExists
			}
		}
	} else {
		for i := range entries {
			entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, rootName, entries[i].id)
			if err == nil {
				if entry.Flags&node.FlagTombstone == 0 {
					_ = snap.Close()
					return nil, ErrDocumentExists
				}
				continue
			}
			if !errors.Is(err, tree.ErrKeyNotFound) {
				_ = snap.Close()
				return nil, err
			}
		}
	}
	stats.DuplicateDocumentPreflight = time.Since(phaseStart)
	baseRoot := catalog.rootID(rootName)
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	phaseStart = time.Now()
	table := newCollectionRunTable(len(entries))
	for i := range entries {
		setCollectionRunValue(table, entries[i].id, entries[i].document)
	}
	table.Freeze()
	stats.PrimaryRunBuild = time.Since(phaseStart)
	iter := table.NewIterator(nil, nil)
	defer func() {
		_ = iter.Close()
		resetCollectionRunTable(table)
	}()

	baseRootIDs := map[string]uint64{rootName: baseRoot}
	publishStart := time.Now()
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: plannerOptions.dataStoragePolicy,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != 1 {
		return nil, unexpectedOrderedRootCountError(c.meta.Name, 1, len(rootIDs))
	}
	stats.Runs = 1
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, []string{rootName}, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.setLastInsertStats(stats)
	return resultIDs, nil
}

func (c *Collection) Delete(documentID []byte) error {
	_, err := c.DeleteDocument(documentID)
	return err
}

// DeleteDocument removes a document and reports whether this call deleted an
// existing primary document.
func (c *Collection) DeleteDocument(documentID []byte) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return false, err
	}
	if len(documentID) == 0 {
		return false, errors.New("collections: document id cannot be empty")
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return false, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		deleted, err := c.deleteDocumentOnce(documentID)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return deleted, err
	}
	return false, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) deleteDocumentOnce(documentID []byte) (bool, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return false, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return false, err
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	entry, primaryRoot, err := collectionGetEntryAtCatalogRoot(snap, catalog, primaryRootName, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		return false, nil
	}
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		_ = snap.Close()
		return false, nil
	}

	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	var state documentIndexState
	if len(runtimes) > 0 {
		state, err = loadDeleteIndexState(snap, catalog, documentID, entry.Value, runtimes, plannerOptions)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
	}

	rootNames := []string{collectionPrimaryRootName(c.meta.Name)}
	baseRootIDs := map[string]uint64{
		rootNames[0]: primaryRoot,
	}
	policies := []backenddb.OrderedRootStoragePolicy{plannerOptions.dataStoragePolicy}
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			stateRootID := catalog.rootID(stateRootName)
			if stateRootID != 0 {
				rootNames = append(rootNames, stateRootName)
				baseRootIDs[stateRootName] = stateRootID
				policies = append(policies, plannerOptions.indexStateStoragePolicy)
				deltaTables = append(deltaTables, buildDeleteRootDeltaTable([][]byte{documentID}))
			}
		}
		for _, runtime := range runtimes {
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			if rootID == 0 {
				continue
			}
			deleteValues := state[runtime.def.name]
			if len(deleteValues) == 0 {
				continue
			}
			table := newCollectionRunTable(len(deleteValues))
			if err := deleteSecondaryEntriesForDocument(table, runtime, state, documentID); err != nil {
				_ = snap.Close()
				resetCollectionRunTable(table)
				return false, err
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	defer func() {
		resetCollectionTables(deltaTables)
	}()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(c.meta.Name, rootNames, deltaTables, baseRootIDs, policies)
	if err != nil {
		return false, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(c.meta.Name, rootNames, ordered)
	}
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	cleanupDeltas()
	if err != nil {
		return false, err
	}
	if len(rootIDs) != len(rootNames) {
		return false, unexpectedOrderedRootCountError(c.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if c.writeDomain != nil {
		c.writeDomain.observeRootDeltaPlan(deltaStats)
	}
	return true, nil
}

func (c *Collection) Replace(documentID, document []byte) (bool, error) {
	matched, _, err := c.Update(documentID, func(current []byte) ([]byte, bool, error) {
		if bytes.Equal(current, document) {
			return current, false, nil
		}
		return document, true, nil
	})
	return matched, err
}

// Update applies update to the latest document value and retries if another
// collection write changes the root before this update publishes. For indexed
// collections with BufferedIndexedWrites enabled, updates that do not change
// secondary unique index values may be staged in the write domain and become
// durable at Flush/Close or auto-flush.
//
// For no-secondary-index collections, single-document Update deliberately
// preserves the current synchronous publish contract: pending collection-local
// writes are flushed first, then a modified document is published to the
// primary root before Update returns. No-index updates may coalesce only
// opportunistically when callers already reach the existing combiner as a
// multi-request UpdateBatch-backed batch; they are not staged in the no-index
// insert buffer or indexed flush queues.
//
// Callback panics are recovered and returned as errors in both direct and
// combined execution. When the collection write domain combines concurrent
// updates, update may run on an internal combiner goroutine. The callback must
// not rely on caller goroutine behavior such as recover, runtime.Goexit, or
// testing.T.Fatal.
func (c *Collection) Update(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	if err := validateCollectionUpdateInput(c, documentID, update); err != nil {
		return false, false, err
	}
	if combiner := c.updateCombiner(); combiner != nil {
		return combiner.update(c, documentID, update)
	}
	return c.updateDirect(documentID, update)
}

func validateCollectionUpdateInput(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return err
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if update == nil {
		return errors.New("collections: update function is nil")
	}
	return nil
}

func (c *Collection) updateDirect(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	// PR2b pins no-index Update as synchronous: pending no-index inserts or
	// indexed buffered writes are drained before reading/planning the update,
	// and a modified no-index replacement publishes before this call returns.
	if err := c.flushBufferedWrites(); err != nil {
		return false, false, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		matched, modified, err := c.updateDocumentOnce(documentID, update)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return matched, modified, err
	}
	return false, false, collectionMutationRetryExhausted(lastErr)
}

// UpdateBatch applies a unique set of document updates under one collection
// mutation. Missing documents report Matched=false. Duplicate document IDs are
// rejected so callers that require same-document ordering can fall back to
// sequential Update calls.
func (c *Collection) UpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	results, _, err := c.updateBatch(items, updateBatchModeAny)
	return results, err
}

// UpdateBatchIfNoSecondaryUniqueIndexes applies UpdateBatch only when the
// collection has no secondary unique indexes in the planning snapshot. It
// reports batched=false without applying updates if a unique secondary index is
// present so callers can preserve ordered per-document update semantics. When
// batched=false and err=nil, the returned results are zero-valued with len(items).
func (c *Collection) UpdateBatchIfNoSecondaryUniqueIndexes(items []UpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	return c.updateBatch(items, updateBatchModeNoSecondaryUniqueIndexes)
}

// UpdateBatchIfNoSecondaryUniqueIndexChanges applies UpdateBatch only when no
// secondary unique index value changes in the planning snapshot. This lets the
// write combiner batch updates that touch non-unique fields on schemas that
// also have unique indexes, while preserving per-document fallback semantics
// for unique value mutations. When batched=false and err=nil, the returned
// results are zero-valued with len(items).
func (c *Collection) UpdateBatchIfNoSecondaryUniqueIndexChanges(items []UpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	return c.updateBatch(items, updateBatchModeNoSecondaryUniqueIndexChanges)
}

func (c *Collection) updateBatch(items []UpdateBatchItem, mode updateBatchMode) ([]UpdateBatchResult, bool, error) {
	if c == nil {
		return nil, false, errCollectionNil
	}
	if c.db == nil {
		return nil, false, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, false, err
	}
	if len(items) == 0 {
		c.setLastUpdateStats(CollectionUpdateStats{})
		return nil, true, nil
	}
	if err := validateUpdateBatchItems(items); err != nil {
		return nil, false, err
	}
	items = cloneUpdateBatchItems(items)
	return c.updateBatchOwnedItems(items, mode)
}

func (c *Collection) updateBatchOwnedItems(items []UpdateBatchItem, mode updateBatchMode) ([]UpdateBatchResult, bool, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		results, err := c.updateBatchOnce(items, mode)
		if errors.Is(err, errUpdateBatchHasSecondaryUniqueIndex) ||
			errors.Is(err, errUpdateBatchChangesSecondaryUniqueIndex) {
			return make([]UpdateBatchResult, len(items)), false, nil
		}
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return results, true, err
	}
	return nil, false, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) ensureWriteDomainOpen() error {
	if c == nil || c.db == nil {
		return nil
	}
	if c.db.IsClosing() {
		return backenddb.ErrClosed
	}
	if c.writeDomain != nil && c.writeDomain.closingWrites.Load() {
		return backenddb.ErrClosed
	}
	return nil
}

func validateUpdateBatchItems(items []UpdateBatchItem) error {
	seen := make(map[string]struct{}, len(items))
	for i, item := range items {
		if len(item.DocumentID) == 0 {
			return fmt.Errorf("collections: document id cannot be empty at index %d", i)
		}
		if item.Update == nil {
			return fmt.Errorf("collections: update function is nil at index %d", i)
		}
		key := string(item.DocumentID)
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func updateBatchItemError(index int, err error) error {
	if err == nil {
		return nil
	}
	return &UpdateBatchItemError{Index: index, Err: err}
}

type collectionUpdateCombiner struct {
	maxBatch int
	idleTTL  time.Duration
	requests chan collectionUpdateCombineRequest
	done     chan struct{}
	domain   *collectionWriteDomain
	running  atomic.Bool
	mu       sync.RWMutex
	stopped  bool

	batchScratch []collectionUpdateCombineRequest
	itemsScratch []UpdateBatchItem
	waiters      sync.Pool
	drainYield   func()
}

const collectionUpdateCombineInlineDocumentIDMax = 64

type collectionUpdateCombineRequest struct {
	collection          *Collection
	documentID          []byte
	documentIDInline    [collectionUpdateCombineInlineDocumentIDMax]byte
	documentIDInlineLen int
	documentHash        uint64
	update              func(current []byte) (replacement []byte, changed bool, err error)
	done                chan collectionUpdateCombineResult
}

type collectionUpdateCombineResult struct {
	matched  bool
	modified bool
	err      error
}

type collectionUpdateCombineWaiter struct {
	ch chan collectionUpdateCombineResult
}

func (c *Collection) updateCombiner() *collectionUpdateCombiner {
	if c == nil || c.db == nil || c.db.IsClosing() || c.writeDomain == nil {
		return nil
	}
	domain := c.writeDomain
	if domain.closingWrites.Load() {
		return nil
	}
	for {
		domain.updateCombineMu.Lock()
		if domain.updateCombineDone {
			domain.updateCombineMu.Unlock()
			return nil
		}
		if draining := domain.updateDraining; draining != nil {
			domain.updateCombineMu.Unlock()
			draining.waitDone()
			continue
		}
		if domain.updateCombiner != nil {
			combiner := domain.updateCombiner
			if !combiner.isStopped() {
				domain.updateCombineMu.Unlock()
				return combiner
			}
			if domain.updateCombiner == combiner {
				domain.updateCombiner = nil
			}
			domain.updateCombineMu.Unlock()
			continue
		}
		combiner := &collectionUpdateCombiner{
			maxBatch: defaultCollectionUpdateCombineMaxBatch,
			idleTTL:  domain.updateCombineIdleTTL(),
			requests: make(chan collectionUpdateCombineRequest, defaultCollectionUpdateCombineMaxBatch*4),
			done:     make(chan struct{}),
			domain:   domain,
		}
		domain.updateCombiner = combiner
		domain.updateCombineMu.Unlock()
		go combiner.run()
		return combiner
	}
}

func (domain *collectionWriteDomain) stopUpdateCombiner() {
	if domain == nil {
		return
	}
	domain.closingWrites.Store(true)
	domain.updateCombineMu.Lock()
	combiner := domain.updateCombiner
	draining := domain.updateDraining
	domain.updateCombiner = nil
	domain.updateDraining = nil
	domain.updateCombineDone = true
	domain.updateCombineMu.Unlock()
	if combiner != nil {
		combiner.stop()
	}
	if draining != nil && draining != combiner {
		draining.waitDone()
	}
}

func (combiner *collectionUpdateCombiner) update(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	if combiner == nil || combiner.maxBatch <= 1 {
		if err := c.ensureWriteDomainOpen(); err != nil {
			return false, false, err
		}
		return c.updateDirect(documentID, update)
	}
	waiter := combiner.getWaiter()
	req := newCollectionUpdateCombineRequest(c, documentID, update, waiter.ch)
	if !combiner.enqueue(req) {
		combiner.putWaiter(waiter)
		// Combining is a best-effort throughput optimization. Saturated or stopped
		// combiners fall back to the direct path so updates still make progress.
		if combiner.isStopped() {
			combiner.waitDone()
		}
		if err := c.ensureWriteDomainOpen(); err != nil {
			return false, false, err
		}
		return c.updateDirect(documentID, update)
	}
	result, reusableWaiter := combiner.waitForUpdateResult(waiter.ch)
	if reusableWaiter {
		combiner.putWaiter(waiter)
	}
	return result.matched, result.modified, result.err
}

func newCollectionUpdateCombineRequest(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), done chan collectionUpdateCombineResult) collectionUpdateCombineRequest {
	req := collectionUpdateCombineRequest{
		collection: c,
		update:     update,
		done:       done,
	}
	if len(documentID) <= collectionUpdateCombineInlineDocumentIDMax {
		req.documentIDInlineLen = len(documentID)
		copy(req.documentIDInline[:], documentID)
		req.documentHash = xxhash.Sum64(req.documentIDInline[:req.documentIDInlineLen])
		return req
	}
	req.documentID = bytes.Clone(documentID)
	req.documentHash = xxhash.Sum64(req.documentID)
	return req
}

func (req *collectionUpdateCombineRequest) documentIDBytes() []byte {
	if req == nil {
		return nil
	}
	if req.documentIDInlineLen > 0 {
		return req.documentIDInline[:req.documentIDInlineLen]
	}
	return req.documentID
}

func (combiner *collectionUpdateCombiner) getWaiter() *collectionUpdateCombineWaiter {
	if combiner == nil {
		return &collectionUpdateCombineWaiter{ch: make(chan collectionUpdateCombineResult, 1)}
	}
	if v := combiner.waiters.Get(); v != nil {
		waiter, _ := v.(*collectionUpdateCombineWaiter)
		if waiter != nil && waiter.ch != nil {
			return waiter
		}
	}
	return &collectionUpdateCombineWaiter{ch: make(chan collectionUpdateCombineResult, 1)}
}

func (combiner *collectionUpdateCombiner) putWaiter(waiter *collectionUpdateCombineWaiter) {
	if combiner == nil || waiter == nil || waiter.ch == nil {
		return
	}
	select {
	case <-waiter.ch:
	default:
	}
	combiner.waiters.Put(waiter)
}

func (combiner *collectionUpdateCombiner) waitForUpdateResult(done chan collectionUpdateCombineResult) (collectionUpdateCombineResult, bool) {
	select {
	case result := <-done:
		return result, true
	default:
	}
	if combiner == nil || combiner.done == nil {
		return <-done, true
	}
	select {
	case result := <-done:
		return result, true
	case <-combiner.done:
		select {
		case result := <-done:
			return result, true
		default:
			return collectionUpdateCombineResult{err: errUpdateCombinerStopped}, false
		}
	}
}

func (combiner *collectionUpdateCombiner) enqueue(req collectionUpdateCombineRequest) bool {
	if combiner == nil {
		return false
	}
	if validateCollectionUpdateCombineRequest(req) != nil {
		return false
	}
	if req.done == nil || cap(req.done) == 0 || len(req.done) > 0 {
		return false
	}
	combiner.mu.RLock()
	defer combiner.mu.RUnlock()
	if combiner.stopped {
		return false
	}
	select {
	case combiner.requests <- req:
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineRequest(len(combiner.requests))
		}
		return true
	default:
		return false
	}
}

func (combiner *collectionUpdateCombiner) stop() {
	if combiner == nil {
		return
	}
	_ = combiner.closeRequests()
	combiner.waitDone()
}

func (combiner *collectionUpdateCombiner) waitDone() {
	if combiner == nil || combiner.done == nil {
		return
	}
	<-combiner.done
}

func (combiner *collectionUpdateCombiner) closeRequests() bool {
	combiner.mu.Lock()
	defer combiner.mu.Unlock()
	if combiner.stopped {
		return false
	}
	combiner.stopped = true
	if combiner.requests != nil {
		close(combiner.requests)
	}
	return true
}

func (combiner *collectionUpdateCombiner) isStopped() bool {
	combiner.mu.RLock()
	defer combiner.mu.RUnlock()
	return combiner.stopped
}

func (combiner *collectionUpdateCombiner) run() {
	defer func() {
		_ = combiner.closeRequests()
		if combiner.done != nil {
			close(combiner.done)
		}
	}()
	if combiner.idleTTL <= 0 {
		for first := range combiner.requests {
			combiner.runBatchStartingWith(first)
		}
		return
	}
	idle := time.NewTimer(combiner.idleTTL)
	defer idle.Stop()
	for {
		select {
		case first, ok := <-combiner.requests:
			if !ok {
				return
			}
			stopAndDrainTimer(idle)
			combiner.runBatchStartingWith(first)
			idle.Reset(combiner.idleTTL)
		case <-idle.C:
			if combiner.retireIdle() {
				return
			}
			idle.Reset(combiner.idleTTL)
		}
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (combiner *collectionUpdateCombiner) retireIdle() bool {
	if combiner == nil {
		return false
	}
	stopped := false
	if combiner.domain != nil {
		combiner.domain.updateCombineMu.Lock()
		if combiner.domain.updateCombiner == combiner {
			stopped = combiner.closeRequests()
			if stopped {
				combiner.domain.updateCombiner = nil
				combiner.domain.updateDraining = combiner
			}
		}
		combiner.domain.updateCombineMu.Unlock()
	} else {
		stopped = combiner.closeRequests()
	}
	if !stopped {
		return false
	}
	for req := range combiner.requests {
		completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
	}
	if combiner.domain != nil {
		combiner.domain.updateCombineMu.Lock()
		if combiner.domain.updateDraining == combiner {
			combiner.domain.updateDraining = nil
		}
		combiner.domain.updateCombineMu.Unlock()
	}
	return true
}

func (combiner *collectionUpdateCombiner) runBatchStartingWith(first collectionUpdateCombineRequest) {
	batchCap := combiner.maxBatch
	if batchCap < 1 {
		batchCap = 1
	}
	if cap(combiner.batchScratch) < batchCap {
		combiner.batchScratch = make([]collectionUpdateCombineRequest, 0, batchCap)
	}
	clear(combiner.batchScratch)
	batch := combiner.batchScratch[:0]
	batch = append(batch, first)
	drainYields := 0
	for len(batch) < batchCap {
		select {
		case req, ok := <-combiner.requests:
			if !ok {
				combiner.runBatch(batch)
				clear(batch)
				combiner.batchScratch = batch[:0]
				return
			}
			batch = append(batch, req)
		default:
			if drainYields < defaultCollectionUpdateCombineDrainYields {
				drainYields++
				if combiner.drainYield != nil {
					combiner.drainYield()
				} else {
					runtime.Gosched()
				}
				continue
			}
			combiner.runBatch(batch)
			clear(batch)
			combiner.batchScratch = batch[:0]
			return
		}
	}
	combiner.runBatch(batch)
	clear(batch)
	combiner.batchScratch = batch[:0]
}

func (combiner *collectionUpdateCombiner) runBatch(batch []collectionUpdateCombineRequest) {
	combiner.running.Store(true)
	defer combiner.running.Store(false)
	defer func() {
		if recovered := recover(); recovered != nil {
			if combiner.domain != nil {
				combiner.domain.observeUpdateCombineBatch(len(batch), false)
			}
			completeUpdateCombineBatchWithError(batch, collectionUpdatePanicError("combiner", recovered))
		}
	}()
	if combiner.domain == nil ||
		combiner.domain.closingWrites.Load() ||
		collectionUpdateCombineHasDuplicateIDs(batch) ||
		!collectionUpdateCombineSameCollection(batch) {
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(batch), true)
		}
		for _, req := range batch {
			completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
		}
		return
	}
	if cap(combiner.itemsScratch) < len(batch) {
		combiner.itemsScratch = make([]UpdateBatchItem, len(batch))
	}
	clear(combiner.itemsScratch)
	items := combiner.itemsScratch[:len(batch)]
	for i := range batch {
		req := &batch[i]
		items[i] = UpdateBatchItem{
			DocumentID: req.documentIDBytes(),
			Update:     req.update,
		}
	}
	results, batched, err := batch[0].collection.updateBatchOwnedItems(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	clear(items)
	combiner.itemsScratch = items[:0]
	if !batched && err == nil {
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(batch), true)
		}
		for _, req := range batch {
			completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
		}
		return
	}
	if err != nil {
		if completeUpdateCombineBatchWithItemFallback(batch, err) {
			if combiner.domain != nil {
				combiner.domain.observeUpdateCombineBatch(len(batch), true)
			}
			return
		}
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(batch), false)
		}
		completeUpdateCombineBatchWithError(batch, err)
		return
	}
	if len(results) != len(batch) {
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(batch), false)
		}
		completeUpdateCombineBatchWithError(batch, fmt.Errorf("collections: update combiner result count %d for batch size %d", len(results), len(batch)))
		return
	}
	if combiner.domain != nil {
		combiner.domain.observeUpdateCombineBatch(len(batch), false)
	}
	for i, req := range batch {
		result := results[i]
		completeUpdateCombineRequest(req, collectionUpdateCombineResult{matched: result.Matched, modified: result.Modified})
	}
}

func collectionUpdateCombineSameCollection(batch []collectionUpdateCombineRequest) bool {
	if len(batch) == 0 {
		return true
	}
	first := batch[0].collection
	var firstDomain *collectionWriteDomain
	if first != nil {
		firstDomain = first.writeDomain
	}
	for _, req := range batch[1:] {
		if req.collection == first {
			continue
		}
		if req.collection == nil || firstDomain == nil || req.collection.writeDomain != firstDomain {
			return false
		}
	}
	return true
}

func runUpdateCombineDirect(req collectionUpdateCombineRequest) collectionUpdateCombineResult {
	if err := validateCollectionUpdateCombineRequest(req); err != nil {
		return collectionUpdateCombineResult{err: err}
	}
	matched, modified, err := req.collection.updateDirect((&req).documentIDBytes(), req.update)
	return collectionUpdateCombineResult{matched: matched, modified: modified, err: err}
}

func validateCollectionUpdateCombineRequest(req collectionUpdateCombineRequest) error {
	if req.collection == nil {
		return errCollectionNil
	}
	if req.collection.db == nil {
		return errCollectionDBNil
	}
	if len((&req).documentIDBytes()) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if req.update == nil {
		return errors.New("collections: update function is nil")
	}
	return nil
}

func collectionUpdatePanicError(where string, recovered any) error {
	return fmt.Errorf("collections: update %s panic (%T): %v", where, recovered, recovered)
}

func recoverCollectionUpdateCallback(update func(current []byte) (replacement []byte, changed bool, err error)) func(current []byte) (replacement []byte, changed bool, err error) {
	return func(current []byte) (replacement []byte, changed bool, err error) {
		return callCollectionUpdateCallback(update, current)
	}
}

func callCollectionUpdateCallback(update func(current []byte) (replacement []byte, changed bool, err error), current []byte) (replacement []byte, changed bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			replacement = nil
			changed = false
			err = collectionUpdatePanicError("callback", recovered)
		}
	}()
	return update(current)
}

func (domain *collectionWriteDomain) updateCombineIdleTTL() time.Duration {
	if domain != nil && domain.updateCombineTTL > 0 {
		return domain.updateCombineTTL
	}
	return collectionUpdateCombineIdleTTL
}

func completeUpdateCombineBatchWithError(batch []collectionUpdateCombineRequest, err error) {
	for _, req := range batch {
		completeUpdateCombineRequest(req, collectionUpdateCombineResult{err: err})
	}
}

func completeUpdateCombineBatchWithItemFallback(batch []collectionUpdateCombineRequest, err error) bool {
	var itemErr *UpdateBatchItemError
	if !errors.As(err, &itemErr) {
		return false
	}
	if itemErr.Index < 0 || itemErr.Index >= len(batch) {
		return false
	}
	for i, req := range batch {
		if i == itemErr.Index {
			reqErr := itemErr.Err
			if reqErr == nil {
				reqErr = err
			}
			completeUpdateCombineRequest(req, collectionUpdateCombineResult{err: reqErr})
			continue
		}
		completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
	}
	return true
}

func completeUpdateCombineRequest(req collectionUpdateCombineRequest, result collectionUpdateCombineResult) {
	if req.done == nil {
		return
	}
	req.done <- result
}

func collectionUpdateCombineHasDuplicateIDs(batch []collectionUpdateCombineRequest) bool {
	if len(batch) < 2 {
		return false
	}
	for i, req := range batch {
		reqDocumentID := (&req).documentIDBytes()
		hash := req.documentHash
		if hash == 0 && len(reqDocumentID) > 0 {
			hash = xxhash.Sum64(reqDocumentID)
		}
		for j := 0; j < i; j++ {
			prev := batch[j]
			prevDocumentID := (&prev).documentIDBytes()
			prevHash := prev.documentHash
			if prevHash == 0 && len(prevDocumentID) > 0 {
				prevHash = xxhash.Sum64(prevDocumentID)
			}
			if prevHash == hash && bytes.Equal(prevDocumentID, reqDocumentID) {
				return true
			}
		}
	}
	return false
}

func cloneUpdateBatchItems(items []UpdateBatchItem) []UpdateBatchItem {
	out := make([]UpdateBatchItem, len(items))
	for i, item := range items {
		out[i] = item
		out[i].DocumentID = bytes.Clone(item.DocumentID)
	}
	return out
}

func validateBSONReplacementPreservesID(current, replacement []byte, opts collectionOptions) error {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return nil
	}
	currentRaw := bson.Raw(current)
	if err := currentRaw.Validate(); err != nil {
		return fmt.Errorf("collections: current BSON document: %w", err)
	}
	replacementRaw := bson.Raw(replacement)
	if err := replacementRaw.Validate(); err != nil {
		return fmt.Errorf("collections: replacement BSON document: %w", err)
	}
	currentID := currentRaw.Lookup("_id")
	replacementID := replacementRaw.Lookup("_id")
	if currentID.IsZero() && replacementID.IsZero() {
		return nil
	}
	if currentID.IsZero() || replacementID.IsZero() || !currentID.Equal(replacementID) {
		return errBSONIDMutation
	}
	return nil
}

type bsonIDSnapshot struct {
	typ       bson.Type
	value     []byte
	inline    [bsonIDSnapshotInlineValueLen]byte
	inlineLen int
	present   bool
}

func captureBSONIDSnapshot(document []byte, opts collectionOptions) (bsonIDSnapshot, error) {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return bsonIDSnapshot{}, nil
	}
	raw := bson.Raw(document)
	if err := raw.Validate(); err != nil {
		return bsonIDSnapshot{}, fmt.Errorf("collections: current BSON document: %w", err)
	}
	id := raw.Lookup("_id")
	if id.IsZero() {
		return bsonIDSnapshot{}, nil
	}
	snapshot := bsonIDSnapshot{typ: id.Type, present: true}
	if len(id.Value) <= len(snapshot.inline) {
		copy(snapshot.inline[:], id.Value)
		snapshot.inlineLen = len(id.Value)
	} else {
		snapshot.value = bytes.Clone(id.Value)
	}
	return snapshot, nil
}

func (s bsonIDSnapshot) isZero() bool {
	return !s.present
}

func (s bsonIDSnapshot) equalRawValue(value bson.RawValue) bool {
	if !s.present || value.IsZero() || s.typ != value.Type {
		return false
	}
	if s.value != nil {
		return bytes.Equal(s.value, value.Value)
	}
	return bytes.Equal(s.inline[:s.inlineLen], value.Value)
}

func validateBSONReplacementPreservesIDSnapshot(currentID bsonIDSnapshot, replacement []byte, opts collectionOptions) error {
	if normalizedDocumentFormat(opts.documentFormat) != DocumentFormatBSON {
		return nil
	}
	replacementRaw := bson.Raw(replacement)
	if err := replacementRaw.Validate(); err != nil {
		return fmt.Errorf("collections: replacement BSON document: %w", err)
	}
	replacementID := replacementRaw.Lookup("_id")
	currentMissing := currentID.isZero()
	replacementMissing := replacementID.IsZero()
	if currentMissing && replacementMissing {
		return nil
	}
	if currentMissing || replacementMissing || !currentID.equalRawValue(replacementID) {
		return errBSONIDMutation
	}
	return nil
}

func waitBeforeCollectionMutationRetry(attempt int) {
	if attempt+1 >= maxCollectionMutationRetries {
		return
	}
	if attempt < 4 {
		runtime.Gosched()
		return
	}
	shift := attempt - 4
	if shift > 6 {
		shift = 6
	}
	time.Sleep(time.Duration(1<<shift) * time.Microsecond)
}

func collectionMutationRetryExhausted(err error) error {
	if err == nil {
		err = ErrConcurrentMutation
	}
	return fmt.Errorf("collections: retry budget exceeded after %d attempts: %w", maxCollectionMutationRetries, err)
}

func errConcurrentRootModification(collectionName, rootName string) error {
	return fmt.Errorf("%w: concurrent root modification detected for collection=%q root=%q", ErrConcurrentMutation, collectionName, rootName)
}

func (c *Collection) updateDocumentOnce(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	detailedStats := c.updateBatchDetailedStatsEnabled()
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, false, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if catalog == nil {
		_ = snap.Close()
		return false, false, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return false, false, err
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptions(c.meta)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	stats := CollectionUpdateStats{
		Items:   1,
		Indexes: len(c.meta.Indexes),
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	primaryOnlyUpdate := len(c.meta.Indexes) == 0
	baseUserRoot := snapshotUserRoot(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	phaseStart := updateBatchStatsNow(detailedStats)
	currentValue, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, documentID, nil)
	stats.CurrentRead += updateBatchStatsSince(detailedStats, phaseStart)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if !found {
		_ = snap.Close()
		if primaryOnlyUpdate && c.writeDomain != nil {
			c.writeDomain.observePrimaryOnlyUpdate(false, false, false, collectionRootDeltaPlanStats{})
		}
		c.setLastUpdateStats(stats)
		return false, false, nil
	}
	stats.Matched = 1
	primaryRoot := catalog.rootID(primaryRootName)

	runtimes, err := (insertBatchPlanner{
		collection: c.meta.Name,
		indexes:    plannerIndexes(c.meta.Indexes),
	}).indexRuntimes()
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	initCollectionUpdateIndexStats(&stats, c.meta.Name, runtimes, detailedStats)
	currentID, err := captureBSONIDSnapshot(currentValue, plannerOptions)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	var oldState documentIndexState
	var newState documentIndexState
	indexStateChanged := false
	if len(runtimes) > 0 {
		phaseStart = updateBatchStatsNow(detailedStats)
		oldState, err = indexStateForDocument(currentValue, runtimes, plannerOptions)
		stats.IndexStateExtraction += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
	}

	phaseStart = updateBatchStatsNow(detailedStats)
	document, changed, err := callCollectionUpdateCallback(update, currentValue)
	stats.Callback += updateBatchStatsSince(detailedStats, phaseStart)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if !changed {
		_ = snap.Close()
		if primaryOnlyUpdate && c.writeDomain != nil {
			c.writeDomain.observePrimaryOnlyUpdate(true, false, false, collectionRootDeltaPlanStats{})
		}
		c.setLastUpdateStats(stats)
		return true, false, nil
	}
	if err := validateBSONReplacementPreservesIDSnapshot(currentID, document, plannerOptions); err != nil {
		_ = snap.Close()
		return false, false, err
	}
	phaseStart = updateBatchStatsNow(detailedStats)
	preparedDocuments, templateRecords, templateResolver, err := prepareInsertDocuments([][]byte{document}, plannerOptions)
	stats.PrepareDocuments += updateBatchStatsSince(detailedStats, phaseStart)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if len(preparedDocuments) != 1 {
		_ = snap.Close()
		return false, false, errors.New("collections: update prepared unexpected document count")
	}
	document = preparedDocuments[0]
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}

	if len(runtimes) > 0 {
		phaseStart = updateBatchStatsNow(detailedStats)
		newState, err = indexStateForDocument(document, runtimes, plannerOptions)
		stats.IndexStateExtraction += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
		indexStateChanged = !documentIndexStatesEqual(oldState, newState)
		for runtimeIdx, runtime := range runtimes {
			if _, ok := updateIndexChangedMaskBit(runtimeIdx); !ok {
				stats.MaskFallbacks++
			}
			if documentIndexRuntimeChanged(oldState, newState, runtime) {
				stats.IndexValueChanges++
				if runtime.def.unique {
					stats.UniqueIndexChecks++
				}
				if runtimeIdx < stats.IndexStatsCount {
					stats.IndexStats[runtimeIdx].Changed++
					if runtime.def.unique {
						stats.IndexStats[runtimeIdx].UniqueChecks++
					}
				}
				continue
			}
			stats.IndexValueUnchanged++
			if runtime.def.unique {
				stats.UniqueIndexCheckSkips++
			}
			if runtimeIdx < stats.IndexStatsCount {
				stats.IndexStats[runtimeIdx].Unchanged++
				if runtime.def.unique {
					stats.IndexStats[runtimeIdx].UniqueCheckSkips++
				}
			}
		}
		if indexStateChanged {
			phaseStart = updateBatchStatsNow(detailedStats)
			if err := rejectReplaceUniqueConflicts(snap, catalog, runtimes, oldState, newState, documentID, nil); err != nil {
				_ = snap.Close()
				return false, false, err
			}
			stats.UniqueIndexPreflight += updateBatchStatsSince(detailedStats, phaseStart)
		}
	}

	rootNames := make([]string, 0, 2+len(runtimes))
	baseRootIDs := make(map[string]uint64, 2+len(runtimes))
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, 2+len(runtimes))
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))

	if len(templateRecords) > 0 {
		phaseStart = updateBatchStatsNow(detailedStats)
		templatePlan := &insertBatchPlan{}
		if err := (insertBatchPlanner{
			collection:             c.meta.Name,
			templateRoot:           collectionTemplateRootName(c.meta.Name),
			options:                plannerOptions,
			cloneTemplateRunValues: true,
		}).emitTemplateRun(templatePlan, templateRecords); err != nil {
			_ = snap.Close()
			return false, false, err
		}
		for _, run := range templatePlan.runs {
			rootNames = append(rootNames, run.name)
			baseRootIDs[run.name] = catalog.rootID(run.name)
			policies = append(policies, run.storagePolicy)
			deltaTables = append(deltaTables, run.table)
		}
		stats.TemplateRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
	}

	rootNames = append(rootNames, primaryRootName)
	baseRootIDs[primaryRootName] = primaryRoot
	policies = append(policies, plannerOptions.dataStoragePolicy)
	phaseStart = updateBatchStatsNow(detailedStats)
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, bytes.Clone(documentID), document)
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)
	stats.PrimaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)

	if indexStateChanged {
		if persistIndexStateForOptions(plannerOptions) {
			phaseStart = updateBatchStatsNow(detailedStats)
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			stateRootID := catalog.rootID(stateRootName)
			rootNames = append(rootNames, stateRootName)
			baseRootIDs[stateRootName] = stateRootID
			policies = append(policies, plannerOptions.indexStateStoragePolicy)
			stateTable := newCollectionRunTable(1)
			rawState, err := encodeDocumentIndexState(newState)
			if err != nil {
				_ = snap.Close()
				resetCollectionTables(append(deltaTables, stateTable))
				return false, false, err
			}
			if len(newState) == 0 {
				stateTable.DeleteSteal(bytes.Clone(documentID))
			} else {
				stateTable.SetSteal(bytes.Clone(documentID), rawState)
			}
			stateTable.Freeze()
			deltaTables = append(deltaTables, stateTable)
			stats.IndexStateRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
		}

		phaseStart = updateBatchStatsNow(detailedStats)
		for runtimeIdx, runtime := range runtimes {
			if !documentIndexRuntimeChanged(oldState, newState, runtime) {
				continue
			}
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			table := newCollectionRunTable(0)
			runStats := CollectionUpdateSecondaryRunStats{IndexName: runtime.def.name}
			for _, encoded := range oldState[runtime.def.name] {
				keyLen, err := deleteCollectionSecondaryIndexEntry(table, encoded, documentID)
				if err != nil {
					_ = snap.Close()
					resetCollectionTables(append(deltaTables, table))
					return false, false, err
				}
				stats.SecondaryDeleteEntries++
				stats.SecondaryKeyBytes += keyLen
				runStats.Deletes++
				runStats.KeyBytes += keyLen
				if runtimeIdx < stats.IndexStatsCount {
					stats.IndexStats[runtimeIdx].SecondaryDeletes++
					stats.IndexStats[runtimeIdx].SecondaryKeyBytes += keyLen
				}
			}
			for _, encoded := range newState[runtime.def.name] {
				keyLen, err := setCollectionSecondaryIndexEntry(table, encoded, documentID)
				if err != nil {
					_ = snap.Close()
					resetCollectionTables(append(deltaTables, table))
					return false, false, err
				}
				stats.SecondarySetEntries++
				stats.SecondaryKeyBytes += keyLen
				runStats.Sets++
				runStats.KeyBytes += keyLen
				if runtimeIdx < stats.IndexStatsCount {
					stats.IndexStats[runtimeIdx].SecondarySets++
					stats.IndexStats[runtimeIdx].SecondaryKeyBytes += keyLen
				}
			}
			if table.Len() == 0 {
				resetCollectionRunTable(table)
				continue
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
			if runStats.Deletes != 0 || runStats.Sets != 0 || runStats.KeyBytes != 0 {
				stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			}
			if runtimeIdx < stats.IndexStatsCount {
				stats.IndexStats[runtimeIdx].SecondaryRuns++
			}
		}
		stats.SecondaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	defer func() {
		resetCollectionTables(deltaTables)
	}()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(c.meta.Name, rootNames, deltaTables, baseRootIDs, policies)
	if err != nil {
		return false, false, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(c.meta.Name, rootNames, ordered)
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(baseUserRoot, baseSystemRoot, baseCommitSeq)
	}
	phaseStart = updateBatchStatsNow(detailedStats)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
	})
	stats.Publish += updateBatchStatsSince(detailedStats, phaseStart)
	cleanupDeltas()
	if err != nil {
		return false, false, err
	}
	if len(rootIDs) != len(rootNames) {
		return false, false, unexpectedOrderedRootCountError(c.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if c.writeDomain != nil {
		c.writeDomain.observeRootDeltaPlan(deltaStats)
		if primaryOnlyUpdate {
			c.writeDomain.observePrimaryOnlyUpdate(true, true, true, deltaStats)
		}
	}
	stats.Modified = 1
	stats.Runs = len(rootNames)
	c.setLastUpdateStats(stats)
	return true, true, nil
}

type preparedBatchUpdate struct {
	itemIndex         int
	documentID        []byte
	document          []byte
	oldState          orderedDocumentIndexState
	newState          orderedDocumentIndexState
	changedIndexes    uint64
	indexStateChanged bool
}

type updateBatchPlan struct {
	results                     []UpdateBatchResult
	stats                       CollectionUpdateStats
	meta                        CollectionMeta
	catalog                     *collectionCatalog
	snap                        *backenddb.Snapshot
	baseUserRoot                uint64
	baseSystemRoot              uint64
	baseCommitSeq               uint64
	rootNames                   []string
	baseRootIDs                 map[string]uint64
	policies                    []backenddb.OrderedRootStoragePolicy
	deltaTables                 []memtable.Table
	directBufferedUpdate        *directBufferedUpdatePlan
	semanticRecords             []indexedSemanticRecord
	uniqueSecondaryIndexByRoot  []int
	canBufferIndexedUpdateBatch bool
	bufferedBase                bool
	bufferedReadGeneration      uint64
	bufferedReadBlocked         bool
	scratch                     *updateBatchPlanScratch
}

type directBufferedUpdatePlan struct {
	templateEntries    []directBufferedRootEntry
	primaryEntries     []directBufferedRootEntry
	secondaryRootPlans []directBufferedSecondaryRootPlan
	templateRootName   string
	primaryRootName    string
	stagedBytes        int64
}

type directBufferedRootEntry struct {
	key   []byte
	value []byte
	flags byte
}

type directBufferedSecondaryRootPlan struct {
	rootName   string
	entries    []directBufferedSecondaryRootEntry
	arena      []byte
	deletes    int
	sets       int
	keyBytes   int
	runtimeIdx int
}

type directBufferedSecondaryRootEntry struct {
	key       []byte
	tombstone bool
}

func buildDirectBufferedTemplateRootEntries(records []templateV1Record) []directBufferedRootEntry {
	if len(records) == 0 {
		return nil
	}
	entries := make([]directBufferedRootEntry, len(records))
	for i := range records {
		entries[i] = directBufferedRootEntry{
			key:   bytes.Clone(records[i].id[:]),
			value: bytes.Clone(records[i].raw),
			flags: node.FlagInline,
		}
	}
	return entries
}

func buildDirectBufferedPrimaryRootEntries(changed []preparedBatchUpdate) []directBufferedRootEntry {
	if len(changed) == 0 {
		return nil
	}
	entries := make([]directBufferedRootEntry, len(changed))
	for i := range changed {
		entries[i] = directBufferedRootEntry{
			key:   bytes.Clone(changed[i].documentID),
			value: changed[i].document,
			flags: node.FlagInline,
		}
	}
	return entries
}

func detachUpdateBatchPlanDocumentArena(scratch *updateBatchPlanScratch) []byte {
	if scratch == nil || len(scratch.documentArena) == 0 {
		return nil
	}
	arena := scratch.documentArena
	scratch.documentArena = nil
	return arena
}

func retainDirectBufferedDocumentArenaLocked(domain *collectionWriteDomain, plan *updateBatchPlan) {
	if domain == nil || plan == nil || plan.directBufferedUpdate == nil {
		return
	}
	arena := detachUpdateBatchPlanDocumentArena(plan.scratch)
	if len(arena) == 0 {
		return
	}
	domain.rootValueArenas = append(domain.rootValueArenas, arena)
}

func applyDirectBufferedRootEntries(table memtable.Table, entries []directBufferedRootEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return applyCollectionRunEntriesWithFlags(table, len(entries), func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
		entry := entries[i]
		return entry.key, entry.value, page.ValuePtr{}, entry.flags, nil
	})
}

func buildIndexedSemanticUpdateRecords(collectionName string, runtimes []indexRuntime, updates []preparedBatchUpdate) []indexedSemanticRecord {
	if len(updates) == 0 {
		return nil
	}
	records := make([]indexedSemanticRecord, 0, len(updates))
	for _, update := range updates {
		record := indexedSemanticRecord{
			kind:       indexedSemanticRecordUpdate,
			documentID: bytes.Clone(update.documentID),
			fallback:   indexedSemanticFallbackRawOnly,
		}
		if update.indexStateChanged && len(runtimes) > 0 {
			for runtimeIdx, runtime := range runtimes {
				if !preparedBatchUpdateIndexChanged(update, runtimeIdx) {
					continue
				}
				record.indexDeltas = append(record.indexDeltas, indexedSemanticIndexDelta{
					indexName:  runtime.def.name,
					rootName:   runtimeSecondaryRootName(collectionName, runtime),
					runtimeIdx: runtimeIdx,
					unique:     runtime.def.unique,
					oldValues:  cloneIndexedSemanticValueSet(update.oldState.valuesAt(runtimeIdx)),
					newValues:  cloneIndexedSemanticValueSet(update.newState.valuesAt(runtimeIdx)),
				})
			}
		}
		records = append(records, record)
	}
	return records
}

func appendIndexedSemanticRecordsLocked(domain *collectionWriteDomain, records []indexedSemanticRecord) {
	if domain == nil || len(records) == 0 {
		return
	}
	domain.indexedSemanticRecords = append(domain.indexedSemanticRecords, cloneIndexedSemanticRecords(records)...)
}

func buildDirectBufferedSecondaryRootPlans(collectionName string, runtimes []indexRuntime, changed []preparedBatchUpdate, stats *CollectionUpdateStats) ([]directBufferedSecondaryRootPlan, int64, error) {
	if len(runtimes) == 0 || len(changed) == 0 {
		return nil, 0, nil
	}
	plans := make([]directBufferedSecondaryRootPlan, 0, len(runtimes))
	var stagedBytes int64
	for runtimeIdx, runtime := range runtimes {
		runStats := CollectionUpdateSecondaryRunStats{IndexName: runtime.def.name}
		for _, item := range changed {
			if !item.indexStateChanged || !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, encoded := range item.oldState.valuesAt(runtimeIdx) {
				keyBytes := len(encoded) + len(item.documentID)
				runStats.Deletes++
				runStats.KeyBytes += keyBytes
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				keyBytes := len(encoded) + len(item.documentID)
				runStats.Sets++
				runStats.KeyBytes += keyBytes
			}
		}
		entryCount := runStats.Deletes + runStats.Sets
		if entryCount == 0 {
			continue
		}
		plan := directBufferedSecondaryRootPlan{
			rootName:   runtimeSecondaryRootName(collectionName, runtime),
			entries:    make([]directBufferedSecondaryRootEntry, 0, entryCount),
			arena:      make([]byte, 0, runStats.KeyBytes),
			deletes:    runStats.Deletes,
			sets:       runStats.Sets,
			keyBytes:   runStats.KeyBytes,
			runtimeIdx: runtimeIdx,
		}
		for _, item := range changed {
			if !item.indexStateChanged || !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
				continue
			}
			for _, encoded := range item.oldState.valuesAt(runtimeIdx) {
				var key []byte
				var err error
				plan.arena, key, err = appendIndexEntryKey(plan.arena, encoded, item.documentID)
				if err != nil {
					return nil, 0, err
				}
				plan.entries = append(plan.entries, directBufferedSecondaryRootEntry{key: key, tombstone: true})
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				var key []byte
				var err error
				plan.arena, key, err = appendIndexEntryKey(plan.arena, encoded, item.documentID)
				if err != nil {
					return nil, 0, err
				}
				plan.entries = append(plan.entries, directBufferedSecondaryRootEntry{key: key})
			}
		}
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(runStats.KeyBytes))
		if stats != nil {
			stats.SecondaryDeleteEntries += runStats.Deletes
			stats.SecondarySetEntries += runStats.Sets
			stats.SecondaryKeyBytes += runStats.KeyBytes
			stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			if runtimeIdx < stats.IndexStatsCount {
				stats.IndexStats[runtimeIdx].SecondaryDeletes += runStats.Deletes
				stats.IndexStats[runtimeIdx].SecondarySets += runStats.Sets
				stats.IndexStats[runtimeIdx].SecondaryKeyBytes += runStats.KeyBytes
				stats.IndexStats[runtimeIdx].SecondaryRuns++
			}
		}
		plans = append(plans, plan)
	}
	return plans, stagedBytes, nil
}

var updateBatchPlanPool sync.Pool

func newUpdateBatchPlan() *updateBatchPlan {
	if v := updateBatchPlanPool.Get(); v != nil {
		if plan, ok := v.(*updateBatchPlan); ok && plan != nil {
			return plan
		}
	}
	return &updateBatchPlan{}
}

type updateBatchPlanScratch struct {
	changed          []preparedBatchUpdate
	changedDocuments [][]byte
	documentArena    []byte
	stateArena       indexEncodeArena
	rootNames        []string
	baseRootIDs      map[string]uint64
	policies         []backenddb.OrderedRootStoragePolicy
	deltaTables      []memtable.Table
	uniqueSecondary  []int
}

var updateBatchPlanScratchPool sync.Pool

const (
	updateBatchPlanScratchMaxChangedCap           = 1 << 15
	updateBatchPlanScratchDocumentBytes           = 256
	updateBatchPlanScratchMaxInitialDocumentArena = 4 << 20
	updateBatchPlanScratchMaxDocumentArena        = 8 << 20
	updateBatchPlanScratchMaxRootNameCap          = 64
	updateBatchPlanScratchMaxStateArenaCap        = 4 << 20
	updateBatchPlanScratchMaxStateSliceCap        = 1 << 16
	updateBatchPlanScratchMaxValueRefCap          = 1 << 16
)

func estimateUpdateBatchPlanDocumentArenaBytes(itemCount int) int {
	if itemCount <= 0 {
		return 0
	}
	if itemCount > updateBatchPlanScratchMaxInitialDocumentArena/updateBatchPlanScratchDocumentBytes {
		return updateBatchPlanScratchMaxInitialDocumentArena
	}
	return itemCount * updateBatchPlanScratchDocumentBytes
}

func getUpdateBatchPlanScratch(itemCount, runtimeCount int) *updateBatchPlanScratch {
	var scratch *updateBatchPlanScratch
	if v := updateBatchPlanScratchPool.Get(); v != nil {
		scratch, _ = v.(*updateBatchPlanScratch)
	}
	if scratch == nil {
		scratch = &updateBatchPlanScratch{}
	}
	if cap(scratch.changed) < itemCount {
		scratch.changed = make([]preparedBatchUpdate, 0, itemCount)
	} else {
		scratch.changed = scratch.changed[:0]
	}
	if cap(scratch.changedDocuments) < itemCount {
		scratch.changedDocuments = make([][]byte, 0, itemCount)
	} else {
		scratch.changedDocuments = scratch.changedDocuments[:0]
	}
	documentArenaBytes := estimateUpdateBatchPlanDocumentArenaBytes(itemCount)
	if cap(scratch.documentArena) < documentArenaBytes {
		scratch.documentArena = make([]byte, 0, documentArenaBytes)
	} else {
		scratch.documentArena = scratch.documentArena[:0]
	}
	arenaBytes := estimateIndexEncodeArenaBytesForCount(itemCount*2, runtimeCount)
	if cap(scratch.stateArena.buf) < arenaBytes {
		scratch.stateArena.buf = make([]byte, 0, arenaBytes)
	} else {
		scratch.stateArena.buf = scratch.stateArena.buf[:0]
	}
	valueRefs := estimateIndexValueRefCountForCount(itemCount*2, runtimeCount)
	if cap(scratch.stateArena.valueRefs) < valueRefs {
		scratch.stateArena.valueRefs = make([][]byte, 0, valueRefs)
	} else {
		scratch.stateArena.valueRefs = scratch.stateArena.valueRefs[:0]
	}
	stateSlots := estimateIndexStateSlotCountForCount(itemCount*2, runtimeCount)
	if cap(scratch.stateArena.states) < stateSlots {
		scratch.stateArena.states = make([][][]byte, 0, stateSlots)
	} else {
		scratch.stateArena.states = scratch.stateArena.states[:0]
	}
	rootCap := 2 + runtimeCount
	if cap(scratch.rootNames) < rootCap || cap(scratch.rootNames) > updateBatchPlanScratchMaxRootNameCap {
		scratch.rootNames = make([]string, 0, rootCap)
	} else {
		scratch.rootNames = scratch.rootNames[:0]
	}
	if scratch.baseRootIDs == nil || len(scratch.baseRootIDs) > updateBatchPlanScratchMaxRootNameCap {
		scratch.baseRootIDs = make(map[string]uint64, rootCap)
	} else {
		clear(scratch.baseRootIDs)
	}
	if cap(scratch.policies) < rootCap || cap(scratch.policies) > updateBatchPlanScratchMaxRootNameCap {
		scratch.policies = make([]backenddb.OrderedRootStoragePolicy, 0, rootCap)
	} else {
		scratch.policies = scratch.policies[:0]
	}
	if cap(scratch.deltaTables) < rootCap || cap(scratch.deltaTables) > updateBatchPlanScratchMaxRootNameCap {
		scratch.deltaTables = make([]memtable.Table, 0, rootCap)
	} else {
		scratch.deltaTables = scratch.deltaTables[:0]
	}
	if cap(scratch.uniqueSecondary) < rootCap || cap(scratch.uniqueSecondary) > updateBatchPlanScratchMaxRootNameCap {
		scratch.uniqueSecondary = make([]int, 0, rootCap)
	} else {
		scratch.uniqueSecondary = scratch.uniqueSecondary[:0]
	}
	return scratch
}

func putUpdateBatchPlanScratch(scratch *updateBatchPlanScratch) {
	if scratch == nil {
		return
	}
	clear(scratch.changed)
	if cap(scratch.changed) > updateBatchPlanScratchMaxChangedCap {
		scratch.changed = nil
	} else {
		scratch.changed = scratch.changed[:0]
	}
	clear(scratch.changedDocuments)
	if cap(scratch.changedDocuments) > updateBatchPlanScratchMaxChangedCap {
		scratch.changedDocuments = nil
	} else {
		scratch.changedDocuments = scratch.changedDocuments[:0]
	}
	if cap(scratch.documentArena) > updateBatchPlanScratchMaxDocumentArena {
		scratch.documentArena = nil
	} else {
		scratch.documentArena = scratch.documentArena[:0]
	}
	if cap(scratch.stateArena.buf) > updateBatchPlanScratchMaxStateArenaCap {
		scratch.stateArena.buf = nil
	} else {
		scratch.stateArena.buf = scratch.stateArena.buf[:0]
	}
	clear(scratch.stateArena.valueRefs)
	if cap(scratch.stateArena.valueRefs) > updateBatchPlanScratchMaxValueRefCap {
		scratch.stateArena.valueRefs = nil
	} else {
		scratch.stateArena.valueRefs = scratch.stateArena.valueRefs[:0]
	}
	clear(scratch.stateArena.states)
	if cap(scratch.stateArena.states) > updateBatchPlanScratchMaxStateSliceCap {
		scratch.stateArena.states = nil
	} else {
		scratch.stateArena.states = scratch.stateArena.states[:0]
	}
	clear(scratch.rootNames)
	scratch.rootNames = scratch.rootNames[:0]
	if len(scratch.baseRootIDs) > updateBatchPlanScratchMaxRootNameCap {
		scratch.baseRootIDs = nil
	} else {
		clear(scratch.baseRootIDs)
	}
	clear(scratch.policies)
	scratch.policies = scratch.policies[:0]
	clear(scratch.deltaTables)
	scratch.deltaTables = scratch.deltaTables[:0]
	clear(scratch.uniqueSecondary)
	scratch.uniqueSecondary = scratch.uniqueSecondary[:0]
	updateBatchPlanScratchPool.Put(scratch)
}

func appendUpdateBatchPlanScratchDocument(scratch *updateBatchPlanScratch, document []byte) []byte {
	if scratch == nil || len(document) == 0 {
		return nil
	}
	start := len(scratch.documentArena)
	scratch.documentArena = append(scratch.documentArena, document...)
	return scratch.documentArena[start:len(scratch.documentArena):len(scratch.documentArena)]
}

type updateBatchBufferedRead struct {
	enabled         bool
	primaryEntries  []updateBatchBufferedEntry
	primaryBuffer   *updateBatchBufferedEntryBuffer
	writeGeneration uint64
}

type updateBatchBufferedEntry struct {
	value []byte
	flags byte
	found bool
}

var updateBatchBufferedEntryPool sync.Pool

type updateBatchBufferedEntryBuffer struct {
	entries []updateBatchBufferedEntry
	arena   []byte
}

const updateBatchBufferedEntryPoolMaxCap = 1 << 15
const updateBatchBufferedValueArenaPoolMaxCap = updateBatchPlanScratchMaxDocumentArena

func getUpdateBatchBufferedEntries(count int) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer) {
	if count <= 0 {
		return nil, nil
	}
	if count > updateBatchBufferedEntryPoolMaxCap {
		return make([]updateBatchBufferedEntry, count), nil
	}
	if v := updateBatchBufferedEntryPool.Get(); v != nil {
		if buffer, ok := v.(*updateBatchBufferedEntryBuffer); ok && buffer != nil {
			if cap(buffer.entries) >= count {
				buffer.entries = buffer.entries[:count]
				buffer.ensureValueArenaCapacity(estimateUpdateBatchPlanDocumentArenaBytes(count))
				return buffer.entries, buffer
			}
			putUpdateBatchBufferedEntries(buffer.entries, buffer)
		}
	}
	buffer := &updateBatchBufferedEntryBuffer{
		entries: make([]updateBatchBufferedEntry, count),
	}
	buffer.ensureValueArenaCapacity(estimateUpdateBatchPlanDocumentArenaBytes(count))
	return buffer.entries, buffer
}

func putUpdateBatchBufferedEntries(entries []updateBatchBufferedEntry, buffer *updateBatchBufferedEntryBuffer) {
	if entries == nil || buffer == nil || cap(entries) == 0 || cap(entries) > updateBatchBufferedEntryPoolMaxCap {
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	buffer.entries = full[:0]
	if cap(buffer.arena) > updateBatchBufferedValueArenaPoolMaxCap {
		buffer.arena = nil
	} else if buffer.arena != nil {
		buffer.arena = buffer.arena[:0]
	}
	updateBatchBufferedEntryPool.Put(buffer)
}

func (buffer *updateBatchBufferedEntryBuffer) copyValue(value []byte) []byte {
	if value == nil {
		return nil
	}
	if buffer == nil {
		return bytes.Clone(value)
	}
	if len(value) == 0 {
		if buffer.arena == nil {
			buffer.ensureValueArenaCapacity(1)
		}
		return buffer.arena[len(buffer.arena):len(buffer.arena):len(buffer.arena)]
	}
	start := len(buffer.arena)
	buffer.arena = append(buffer.arena, value...)
	return buffer.arena[start:len(buffer.arena):len(buffer.arena)]
}

func (buffer *updateBatchBufferedEntryBuffer) ensureValueArenaCapacity(capacity int) {
	if buffer == nil {
		return
	}
	if capacity <= 0 {
		buffer.arena = buffer.arena[:0]
		return
	}
	if cap(buffer.arena) < capacity || cap(buffer.arena) > updateBatchBufferedValueArenaPoolMaxCap {
		buffer.arena = make([]byte, 0, capacity)
		return
	}
	buffer.arena = buffer.arena[:0]
}

type updateBatchCurrentDocument struct {
	value    []byte
	buffered bool
	found    bool
}

func (plan *updateBatchPlan) close() {
	if plan == nil {
		return
	}
	if plan.snap != nil {
		_ = plan.snap.Close()
	}
	resetCollectionTables(plan.deltaTables)
	if plan.scratch != nil {
		putUpdateBatchPlanScratch(plan.scratch)
	}
	*plan = updateBatchPlan{}
	updateBatchPlanPool.Put(plan)
}

func (c *Collection) validateUpdateBatchPlanRootDescriptors(plan *updateBatchPlan) error {
	if plan == nil {
		return nil
	}
	if plan.catalog != nil && len(plan.catalog.rootOverlays) != 0 && collectionMetaUsesIndexedOverlayRoots(plan.catalog.meta) {
		return c.validateRootOverlayDescriptorSystemDeltaForMeta(plan.meta, plan.baseCommitSeq, plan.baseSystemRoot, plan.rootNames, plan.baseRootIDs, plan.catalog.rootOverlays)
	}
	return c.validateRootDescriptorSystemDeltaForMeta(plan.meta, plan.baseCommitSeq, plan.baseSystemRoot, plan.rootNames, plan.baseRootIDs)
}

func (c *Collection) shouldUseDirectBufferedUpdatePlan(meta CollectionMeta, opts collectionOptions, canBuffer bool, mode updateBatchMode) bool {
	if c == nil || c.writeDomain == nil || !canBuffer || mode == updateBatchModeAny {
		return false
	}
	if !meta.Options.BufferedIndexedWrites || len(meta.Indexes) == 0 {
		return false
	}
	return !persistIndexStateForOptions(opts)
}

func (c *Collection) updateBatchOnce(items []UpdateBatchItem, mode updateBatchMode) ([]UpdateBatchResult, error) {
	if c.shouldPlanUpdateBatchWithBufferedWrites(mode) {
		useBufferedRead := true
		for {
			plan, err := c.buildUpdateBatchPlan(items, mode, useBufferedRead)
			if err != nil {
				return nil, err
			}
			if plan == nil {
				return nil, nil
			}
			var results []UpdateBatchResult
			replan := false
			err = c.withMutationLock(func() error {
				if len(plan.deltaTables) == 0 && plan.directBufferedUpdate == nil {
					if plan.bufferedReadBlocked && useBufferedRead {
						if err := c.flushBufferedWrites(); err != nil {
							return err
						}
						useBufferedRead = false
						replan = true
						return nil
					}
					if plan.bufferedBase && !c.bufferedUpdateBatchPlanStillCurrent(plan) {
						if useBufferedRead {
							if err := c.flushBufferedWrites(); err != nil {
								return err
							}
							useBufferedRead = false
							replan = true
							return nil
						}
						return ErrConcurrentMutation
					}
					if err := c.validateUpdateBatchPlanRootDescriptors(plan); err != nil {
						return err
					}
					c.meta = plan.meta
					results = plan.results
					return nil
				}
				buffered, bufferErr := c.bufferUpdateBatchPlanLocked(plan)
				if bufferErr != nil {
					if errors.Is(bufferErr, ErrConcurrentMutation) && useBufferedRead {
						if err := c.flushBufferedWrites(); err != nil {
							return err
						}
						useBufferedRead = false
						replan = true
						return nil
					}
					return bufferErr
				}
				if buffered {
					c.meta = plan.meta
					results = plan.results
					return nil
				}
				if plan.directBufferedUpdate != nil {
					if err := c.flushBufferedWrites(); err != nil {
						return err
					}
					useBufferedRead = false
					replan = true
					return nil
				}
				if err := c.flushBufferedWrites(); err != nil {
					return err
				}
				if useBufferedRead {
					useBufferedRead = false
					replan = true
					return nil
				}
				var publishErr error
				results, publishErr = c.publishUpdateBatchPlanLocked(plan)
				return publishErr
			})
			primaryOnlyNoPublish := len(plan.meta.Indexes) == 0 && len(plan.deltaTables) == 0 && plan.directBufferedUpdate == nil
			stats := plan.stats
			plan.close()
			if err != nil {
				return nil, err
			}
			if replan {
				continue
			}
			if primaryOnlyNoPublish && c.writeDomain != nil {
				c.writeDomain.observePrimaryOnlyUpdateBatch(stats.Items, stats.Matched, stats.Modified, false, collectionRootDeltaPlanStats{})
			}
			c.setLastUpdateStats(stats)
			return results, nil
		}
	}

	if err := c.withMutationLock(func() error {
		return c.flushBufferedWrites()
	}); err != nil {
		return nil, err
	}

	plan, err := c.buildUpdateBatchPlan(items, mode, false)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, nil
	}
	defer plan.close()
	if len(plan.deltaTables) == 0 && plan.directBufferedUpdate == nil {
		if err := c.withMutationLock(func() error {
			if err := c.flushBufferedWrites(); err != nil {
				return err
			}
			if err := c.validateUpdateBatchPlanRootDescriptors(plan); err != nil {
				return err
			}
			c.meta = plan.meta
			return nil
		}); err != nil {
			return nil, err
		}
		if len(plan.meta.Indexes) == 0 && c.writeDomain != nil {
			c.writeDomain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, false, collectionRootDeltaPlanStats{})
		}
		c.setLastUpdateStats(plan.stats)
		return plan.results, nil
	}

	var results []UpdateBatchResult
	err = c.withMutationLock(func() error {
		buffered, bufferErr := c.bufferUpdateBatchPlanLocked(plan)
		if bufferErr != nil {
			return bufferErr
		}
		if buffered {
			results = plan.results
			return nil
		}
		if plan.directBufferedUpdate != nil {
			if err := c.flushBufferedWrites(); err != nil {
				return err
			}
			return ErrConcurrentMutation
		}
		var publishErr error
		if err := c.flushBufferedWrites(); err != nil {
			return err
		}
		results, publishErr = c.publishUpdateBatchPlanLocked(plan)
		return publishErr
	})
	if err == nil {
		c.setLastUpdateStats(plan.stats)
	}
	return results, err
}

func (c *Collection) bufferedUpdateBatchPlanStillCurrent(plan *updateBatchPlan) bool {
	if plan == nil || !plan.bufferedBase {
		return true
	}
	domain := c.writeDomain
	if domain == nil {
		return false
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	if domain.count == 0 {
		return false
	}
	return updateBatchCanReadBufferedDomainLocked(domain, plan.meta, plan.baseSystemRoot) &&
		plan.bufferedReadGeneration == domain.writeGeneration
}

func (c *Collection) withMutationLock(fn func() error) error {
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	return fn()
}

func (c *Collection) shouldPlanUpdateBatchWithBufferedWrites(mode updateBatchMode) bool {
	if c == nil || c.writeDomain == nil || mode == updateBatchModeAny {
		return false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return domain.count > 0 && hasBufferedIndexedRootRuns(domain)
}

func updateBatchCanReadBufferedDomainLocked(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64) bool {
	if domain == nil || domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return false
	}
	if !domain.loaded || domain.catalog == nil {
		return false
	}
	if domain.baseSystemRoot != baseSystemRoot {
		return false
	}
	if !sameCollectionMeta(domain.meta, meta) {
		return false
	}
	collectionName := bufferedDomainCollectionName(domain, meta.Name)
	if collectionName == "" || !hasPendingIndexedRootRunsForRootLocked(domain, collectionPrimaryRootName(collectionName)) {
		return false
	}
	return meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0
}

func readUpdateBatchCurrentDocument(primaryReader *backenddb.SnapshotRootReader, primaryReaderOK bool, itemIndex int, documentID []byte, buffered updateBatchBufferedRead, dst []byte) (updateBatchCurrentDocument, error) {
	if buffered.enabled {
		if itemIndex >= 0 && itemIndex < len(buffered.primaryEntries) {
			entry := buffered.primaryEntries[itemIndex]
			if entry.found {
				if entry.flags&node.FlagTombstone != 0 {
					return updateBatchCurrentDocument{buffered: true}, nil
				}
				return updateBatchCurrentDocument{value: entry.value, buffered: true, found: true}, nil
			}
		}
	}
	if !primaryReaderOK {
		return updateBatchCurrentDocument{}, nil
	}
	value, err := primaryReader.GetAppend(documentID, dst)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return updateBatchCurrentDocument{}, nil
	}
	if err != nil {
		return updateBatchCurrentDocument{}, err
	}
	return updateBatchCurrentDocument{value: value, found: true}, nil
}

func readUpdateBatchCurrentDocumentAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, primaryRootName string, primaryReader *backenddb.SnapshotRootReader, primaryReaderOK bool, itemIndex int, documentID []byte, buffered updateBatchBufferedRead, dst []byte) (updateBatchCurrentDocument, error) {
	if buffered.enabled {
		if itemIndex >= 0 && itemIndex < len(buffered.primaryEntries) {
			entry := buffered.primaryEntries[itemIndex]
			if entry.found {
				if entry.flags&node.FlagTombstone != 0 {
					return updateBatchCurrentDocument{buffered: true}, nil
				}
				return updateBatchCurrentDocument{value: entry.value, buffered: true, found: true}, nil
			}
		}
	}
	if catalog == nil || len(catalog.overlayRootIDs(primaryRootName)) == 0 {
		return readUpdateBatchCurrentDocument(primaryReader, primaryReaderOK, -1, documentID, updateBatchBufferedRead{}, dst)
	}
	if catalog.rootID(primaryRootName) == 0 || catalog.anyOverlayRootMayContainKey(primaryRootName, documentID) {
		value, overlayFound, documentFound, err := collectionGetAppendAtCatalogOverlayRoot(snap, catalog, primaryRootName, documentID, dst)
		if err != nil {
			return updateBatchCurrentDocument{}, err
		}
		if overlayFound {
			return updateBatchCurrentDocument{value: value, found: documentFound}, nil
		}
	}
	return readUpdateBatchCurrentDocument(primaryReader, primaryReaderOK, -1, documentID, updateBatchBufferedRead{}, dst)
}

const updateBatchBufferedPrimaryDirectProbeLimit = 1024

func snapshotUpdateBatchBufferedPrimaryEntries(runs []memtable.Table, items []UpdateBatchItem) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer, error) {
	entries, buffer := getUpdateBatchBufferedEntries(len(items))
	if len(runs) == 0 || len(items) == 0 {
		return entries, buffer, nil
	}
	if len(runs) <= 1 || len(runs)*len(items) <= updateBatchBufferedPrimaryDirectProbeLimit {
		for i, item := range items {
			if value, _, flags, found := getBufferedRunEntry(runs, item.DocumentID); found {
				entries[i] = updateBatchBufferedEntry{
					value: buffer.copyValue(value),
					flags: flags,
					found: true,
				}
			}
		}
		return entries, buffer, nil
	}
	targets := make(map[string]int, len(items))
	for i, item := range items {
		targets[string(item.DocumentID)] = i
	}
	for i := len(runs) - 1; i >= 0 && len(targets) > 0; i-- {
		run := runs[i]
		if run == nil {
			continue
		}
		it := run.NewIterator(nil, nil)
		for it.Valid() && len(targets) > 0 {
			key := string(it.UnsafeKey())
			if itemIndex, ok := targets[key]; ok && !entries[itemIndex].found {
				value, _, flags := it.UnsafeEntry()
				entries[itemIndex] = updateBatchBufferedEntry{
					value: buffer.copyValue(value),
					flags: flags,
					found: true,
				}
				delete(targets, key)
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			_ = it.Close()
			putUpdateBatchBufferedEntries(entries, buffer)
			return nil, nil, err
		}
		_ = it.Close()
	}
	return entries, buffer, nil
}

func snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(index *bufferedPrimaryRunIndex, items []UpdateBatchItem) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer, error) {
	entries, buffer := getUpdateBatchBufferedEntries(len(items))
	if index == nil || len(items) == 0 {
		return entries, buffer, nil
	}
	for i, item := range items {
		table, ok := index.lookup(item.DocumentID)
		if !ok || table == nil {
			continue
		}
		value, _, flags, found := table.GetEntry(item.DocumentID)
		if !found {
			continue
		}
		entries[i] = updateBatchBufferedEntry{
			value: buffer.copyValue(value),
			flags: flags,
			found: true,
		}
	}
	return entries, buffer, nil
}

func snapshotUpdateBatchBufferedRead(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64, items []UpdateBatchItem, documentFormat DocumentFormat) (updateBatchBufferedRead, []memtable.Table, bool, error) {
	if domain == nil {
		return updateBatchBufferedRead{}, nil, false, nil
	}
	domain.mu.RLock()
	read, templateRuns, blocked, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(domain, meta, baseSystemRoot, items, documentFormat, true)
	domain.mu.RUnlock()
	if err != nil || !needPrimaryRunIndex {
		return read, templateRuns, blocked, err
	}

	domain.mu.Lock()
	defer domain.mu.Unlock()
	bufferedCollectionName := bufferedDomainCollectionName(domain, meta.Name)
	if updateBatchCanReadBufferedDomainLocked(domain, meta, baseSystemRoot) && domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, bufferedCollectionName) {
		index, err := rebuildBufferedPrimaryRunIndex(bufferedCollectionName, pendingIndexedRootRunMapLocked(domain))
		if err != nil {
			return updateBatchBufferedRead{}, nil, false, err
		}
		if index == nil {
			index = newBufferedPrimaryRunIndex(0)
		}
		domain.primaryRunIndex = index
	}
	read, templateRuns, blocked, _, err = snapshotUpdateBatchBufferedReadLocked(domain, meta, baseSystemRoot, items, documentFormat, false)
	return read, templateRuns, blocked, err
}

func snapshotUpdateBatchBufferedReadLocked(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64, items []UpdateBatchItem, documentFormat DocumentFormat, allowPrimaryRunIndexBuild bool) (updateBatchBufferedRead, []memtable.Table, bool, bool, error) {
	if updateBatchCanReadBufferedDomainLocked(domain, meta, baseSystemRoot) {
		bufferedCollectionName := bufferedDomainCollectionName(domain, meta.Name)
		if allowPrimaryRunIndexBuild && domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, bufferedCollectionName) {
			return updateBatchBufferedRead{}, nil, false, true, nil
		}
		var primaryEntries []updateBatchBufferedEntry
		var primaryBuffer *updateBatchBufferedEntryBuffer
		var err error
		if domain.primaryRunIndex != nil {
			primaryEntries, primaryBuffer, err = snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(domain.primaryRunIndex, items)
		} else {
			primaryRuns := pendingIndexedRootRunsLocked(domain, collectionPrimaryRootName(bufferedCollectionName))
			primaryEntries, primaryBuffer, err = snapshotUpdateBatchBufferedPrimaryEntries(primaryRuns, items)
		}
		if err != nil {
			return updateBatchBufferedRead{}, nil, false, false, err
		}
		var templateRuns []memtable.Table
		if normalizedDocumentFormat(documentFormat) == DocumentFormatTemplateV1 {
			templateRuns, err = cloneCollectionRunTables(pendingIndexedRootRunsLocked(domain, collectionTemplateRootName(bufferedCollectionName)))
			if err != nil {
				return updateBatchBufferedRead{}, nil, false, false, err
			}
		}
		return updateBatchBufferedRead{
			enabled:         true,
			primaryEntries:  primaryEntries,
			primaryBuffer:   primaryBuffer,
			writeGeneration: domain.writeGeneration,
		}, templateRuns, false, false, nil
	}
	if domain.count > 0 && hasBufferedIndexedRootRuns(domain) {
		return updateBatchBufferedRead{}, nil, true, false, nil
	}
	return updateBatchBufferedRead{}, nil, false, false, nil
}

func (c *Collection) buildUpdateBatchPlan(items []UpdateBatchItem, mode updateBatchMode, useBufferedRead bool) (*updateBatchPlan, error) {
	results := make([]UpdateBatchResult, len(items))
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if catalog == nil {
		_ = snap.Close()
		return nil, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		_ = snap.Close()
		return nil, err
	}
	meta := catalog.meta
	stats := CollectionUpdateStats{
		Items:   len(items),
		Indexes: len(meta.Indexes),
	}
	detailedStats := c.updateBatchDetailedStatsEnabled()
	if mode == updateBatchModeNoSecondaryUniqueIndexes && collectionMetaHasSecondaryUniqueIndex(meta) {
		_ = snap.Close()
		return nil, errUpdateBatchHasSecondaryUniqueIndex
	}
	canBufferIndexedUpdateBatch := len(meta.Indexes) > 0 &&
		(!collectionMetaHasSecondaryUniqueIndex(meta) ||
			mode == updateBatchModeNoSecondaryUniqueIndexes ||
			mode == updateBatchModeNoSecondaryUniqueIndexChanges)
	plannerOptions, err := collectionPlannerOptions(meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseUserRoot := snapshotUserRoot(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	var bufferedRead updateBatchBufferedRead
	var bufferedTemplateRuns []memtable.Table
	defer func() { resetCollectionTables(bufferedTemplateRuns) }()
	bufferedReadBlocked := false
	if domain := c.writeDomain; useBufferedRead && domain != nil && mode != updateBatchModeAny {
		bufferedRead, bufferedTemplateRuns, bufferedReadBlocked, err = snapshotUpdateBatchBufferedRead(domain, meta, baseSystemRoot, items, plannerOptions.documentFormat)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		defer putUpdateBatchBufferedEntries(bufferedRead.primaryEntries, bufferedRead.primaryBuffer)
		if len(bufferedTemplateRuns) > 0 {
			plannerOptions = collectionOptionsWithBufferedTemplateV1RunsResolver(plannerOptions, bufferedTemplateRuns)
		}
	}

	primaryRootName := catalog.primaryRootName
	if primaryRootName == "" {
		primaryRootName = collectionPrimaryRootName(meta.Name)
	}
	templateRootName := catalog.templateRootName
	if templateRootName == "" {
		templateRootName = collectionTemplateRootName(meta.Name)
	}
	indexStateRootName := catalog.indexStateRootName
	if indexStateRootName == "" {
		indexStateRootName = collectionIndexStateRootName(meta.Name)
	}
	primaryRoot := catalog.rootID(primaryRootName)
	if primaryRoot == 0 && !bufferedRead.enabled && len(catalog.overlayRootIDs(primaryRootName)) == 0 {
		plan := newUpdateBatchPlan()
		*plan = updateBatchPlan{
			results:                results,
			stats:                  stats,
			meta:                   meta,
			catalog:                catalog,
			snap:                   snap,
			baseUserRoot:           baseUserRoot,
			baseSystemRoot:         baseSystemRoot,
			baseCommitSeq:          baseCommitSeq,
			rootNames:              []string{primaryRootName},
			baseRootIDs:            map[string]uint64{primaryRootName: primaryRoot},
			bufferedReadGeneration: bufferedRead.writeGeneration,
			bufferedReadBlocked:    bufferedReadBlocked,
		}
		return plan, nil
	}
	runtimes, err := catalog.cachedIndexRuntimes()
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	var primaryReader backenddb.SnapshotRootReader
	primaryReaderOK := false
	if primaryRoot != 0 {
		primaryReader, err = snap.ReaderAtRoot(primaryRoot)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		primaryReaderOK = true
	}

	scratch := getUpdateBatchPlanScratch(len(items), len(runtimes))
	scratchOwnedByPlan := false
	changed := scratch.changed
	changedDocuments := scratch.changedDocuments
	stateArena := &scratch.stateArena
	rootNames := scratch.rootNames
	baseRootIDs := scratch.baseRootIDs
	policies := scratch.policies
	deltaTables := scratch.deltaTables
	uniqueSecondary := scratch.uniqueSecondary
	defer func() {
		scratch.changed = changed
		scratch.changedDocuments = changedDocuments
		scratch.rootNames = rootNames
		scratch.policies = policies
		scratch.deltaTables = deltaTables
		scratch.uniqueSecondary = uniqueSecondary
		if !scratchOwnedByPlan {
			putUpdateBatchPlanScratch(scratch)
		}
	}()
	initCollectionUpdateIndexStats(&stats, meta.Name, runtimes, detailedStats)
	var currentScratch []byte
	for i, item := range items {
		phaseStart := updateBatchStatsNow(detailedStats)
		current, err := readUpdateBatchCurrentDocumentAtCatalogRoot(snap, catalog, primaryRootName, &primaryReader, primaryReaderOK, i, item.DocumentID, bufferedRead, currentScratch[:0])
		stats.CurrentRead += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			_ = snap.Close()
			return nil, updateBatchItemError(i, err)
		}
		if !current.found {
			continue
		}
		results[i].Matched = true
		currentID, err := captureBSONIDSnapshot(current.value, plannerOptions)
		if err != nil {
			_ = snap.Close()
			return nil, updateBatchItemError(i, err)
		}
		prepared := preparedBatchUpdate{
			itemIndex:  i,
			documentID: item.DocumentID,
		}
		if len(runtimes) > 0 {
			phaseStart = updateBatchStatsNow(detailedStats)
			prepared.oldState, err = orderedIndexStateForDocumentWithArena(current.value, runtimes, plannerOptions, stateArena)
			stats.IndexStateExtraction += updateBatchStatsSince(detailedStats, phaseStart)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			}
		}
		phaseStart = updateBatchStatsNow(detailedStats)
		document, changedOne, err := callCollectionUpdateCallback(item.Update, current.value)
		stats.Callback += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			_ = snap.Close()
			return nil, updateBatchItemError(i, err)
		}
		if !changedOne {
			if !current.buffered {
				currentScratch = current.value[:0]
			}
			continue
		}
		if len(document) == 0 {
			_ = snap.Close()
			return nil, updateBatchItemError(i, errors.New("changed replacement document cannot be empty"))
		}
		if err := validateBSONReplacementPreservesIDSnapshot(currentID, document, plannerOptions); err != nil {
			_ = snap.Close()
			return nil, updateBatchItemError(i, err)
		}
		changed = append(changed, prepared)
		changedDocuments = append(changedDocuments, appendUpdateBatchPlanScratchDocument(scratch, document))
		if !current.buffered {
			currentScratch = current.value[:0]
		}
	}
	if len(changed) == 0 {
		primaryRootName := collectionPrimaryRootName(meta.Name)
		baseRootIDs[primaryRootName] = primaryRoot
		rootNames = append(rootNames, primaryRootName)
		uniqueSecondary = append(uniqueSecondary, -1)
		plan := newUpdateBatchPlan()
		*plan = updateBatchPlan{
			results:                    results,
			stats:                      updateCollectionUpdateStatsCounts(stats, results, 0),
			meta:                       meta,
			catalog:                    catalog,
			snap:                       snap,
			baseUserRoot:               baseUserRoot,
			baseSystemRoot:             baseSystemRoot,
			baseCommitSeq:              baseCommitSeq,
			rootNames:                  rootNames,
			baseRootIDs:                baseRootIDs,
			uniqueSecondaryIndexByRoot: uniqueSecondary,
			bufferedBase:               bufferedRead.enabled,
			bufferedReadGeneration:     bufferedRead.writeGeneration,
			bufferedReadBlocked:        bufferedReadBlocked,
			scratch:                    scratch,
		}
		scratchOwnedByPlan = true
		return plan, nil
	}

	phaseStart := updateBatchStatsNow(detailedStats)
	preparedDocuments, templateRecords, templateResolver, err := prepareInsertDocuments(changedDocuments, plannerOptions)
	stats.PrepareDocuments += updateBatchStatsSince(detailedStats, phaseStart)
	if err != nil {
		for i, document := range changedDocuments {
			if _, _, _, itemErr := prepareInsertDocuments([][]byte{document}, plannerOptions); itemErr != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(changed[i].itemIndex, itemErr)
			}
		}
		_ = snap.Close()
		return nil, fmt.Errorf("collections: update batch replacement prepare: %w", err)
	}
	if len(preparedDocuments) != len(changed) {
		_ = snap.Close()
		return nil, errors.New("collections: update batch prepared unexpected document count")
	}
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}
	for i := range changed {
		changed[i].document = preparedDocuments[i]
		if len(runtimes) > 0 {
			phaseStart = updateBatchStatsNow(detailedStats)
			changed[i].newState, err = orderedIndexStateForDocumentWithArena(changed[i].document, runtimes, plannerOptions, stateArena)
			stats.IndexStateExtraction += updateBatchStatsSince(detailedStats, phaseStart)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(changed[i].itemIndex, err)
			}
			var changedIndexes uint64
			indexStateChanged := false
			for runtimeIdx, runtime := range runtimes {
				runtimeChanged := orderedDocumentIndexRuntimeChanged(changed[i].oldState, changed[i].newState, runtimeIdx)
				maskBit, maskBitOK := updateIndexChangedMaskBit(runtimeIdx)
				if !maskBitOK {
					stats.MaskFallbacks++
				}
				if runtimeChanged {
					indexStateChanged = true
					if maskBitOK {
						changedIndexes |= maskBit
					}
					stats.IndexValueChanges++
					if runtime.def.unique {
						stats.UniqueIndexChecks++
					}
					if runtimeIdx < stats.IndexStatsCount {
						stats.IndexStats[runtimeIdx].Changed++
						if runtime.def.unique {
							stats.IndexStats[runtimeIdx].UniqueChecks++
						}
					}
					continue
				}
				stats.IndexValueUnchanged++
				if runtime.def.unique {
					stats.UniqueIndexCheckSkips++
				}
				if runtimeIdx < stats.IndexStatsCount {
					stats.IndexStats[runtimeIdx].Unchanged++
					if runtime.def.unique {
						stats.IndexStats[runtimeIdx].UniqueCheckSkips++
					}
				}
			}
			changed[i].changedIndexes = changedIndexes
			changed[i].indexStateChanged = indexStateChanged
		}
	}
	if mode == updateBatchModeNoSecondaryUniqueIndexChanges && updateBatchChangesSecondaryUniqueIndex(runtimes, changed) {
		_ = snap.Close()
		return nil, errUpdateBatchChangesSecondaryUniqueIndex
	}
	phaseStart = updateBatchStatsNow(detailedStats)
	batchReplacements := batchUniqueReplacementOwners(runtimes, changed)
	for i := range changed {
		if changed[i].indexStateChanged {
			if err := rejectReplaceUniqueConflictsOrdered(snap, catalog, runtimes, changed[i], batchReplacements); err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(changed[i].itemIndex, err)
			}
		}
	}
	if err := rejectBatchUniqueConflicts(runtimes, changed); err != nil {
		_ = snap.Close()
		return nil, err
	}
	stats.UniqueIndexPreflight += updateBatchStatsSince(detailedStats, phaseStart)

	success := false
	if c.shouldUseDirectBufferedUpdatePlan(meta, plannerOptions, canBufferIndexedUpdateBatch, mode) {
		phaseStart = updateBatchStatsNow(detailedStats)
		var templateEntries []directBufferedRootEntry
		if len(templateRecords) > 0 {
			sortTemplateV1Records(templateRecords)
			var err error
			templateRecords, err = dedupeTemplateV1Records(templateRecords)
			if err != nil {
				_ = snap.Close()
				return nil, err
			}
			templateEntries = buildDirectBufferedTemplateRootEntries(templateRecords)
			rootNames = append(rootNames, templateRootName)
			uniqueSecondary = append(uniqueSecondary, -1)
			baseRootIDs[templateRootName] = catalog.rootID(templateRootName)
			policies = append(policies, plannerOptions.dataStoragePolicy)
		}
		stats.TemplateRunBuild += updateBatchStatsSince(detailedStats, phaseStart)

		phaseStart = updateBatchStatsNow(detailedStats)
		primaryEntries := buildDirectBufferedPrimaryRootEntries(changed)
		rootNames = append(rootNames, primaryRootName)
		uniqueSecondary = append(uniqueSecondary, -1)
		baseRootIDs[primaryRootName] = primaryRoot
		policies = append(policies, plannerOptions.dataStoragePolicy)
		var stagedBytes int64
		for i := range templateRecords {
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(len(templateRecords[i].id)+len(templateRecords[i].raw)))
		}
		for i := range changed {
			results[changed[i].itemIndex].Modified = true
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(len(changed[i].documentID)+len(changed[i].document)))
		}
		stats.PrimaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)

		phaseStart = updateBatchStatsNow(detailedStats)
		secondaryRootPlans, secondaryStagedBytes, err := buildDirectBufferedSecondaryRootPlans(meta.Name, runtimes, changed, &stats)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, secondaryStagedBytes)
		for _, secondaryPlan := range secondaryRootPlans {
			runtime := runtimes[secondaryPlan.runtimeIdx]
			rootName := secondaryPlan.rootName
			rootNames = append(rootNames, rootName)
			if runtime.def.unique {
				uniqueSecondary = append(uniqueSecondary, secondaryPlan.runtimeIdx)
			} else {
				uniqueSecondary = append(uniqueSecondary, -1)
			}
			baseRootIDs[rootName] = catalog.rootID(rootName)
			policies = append(policies, runtime.def.storagePolicy)
		}
		stats.SecondaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
		success = true
		plan := newUpdateBatchPlan()
		stats = updateCollectionUpdateStatsCounts(stats, results, len(rootNames))
		semanticRecords := buildIndexedSemanticUpdateRecords(meta.Name, runtimes, changed)
		*plan = updateBatchPlan{
			results:                     results,
			stats:                       stats,
			meta:                        meta,
			catalog:                     catalog,
			snap:                        snap,
			baseUserRoot:                baseUserRoot,
			baseSystemRoot:              baseSystemRoot,
			baseCommitSeq:               baseCommitSeq,
			rootNames:                   rootNames,
			baseRootIDs:                 baseRootIDs,
			uniqueSecondaryIndexByRoot:  uniqueSecondary,
			canBufferIndexedUpdateBatch: canBufferIndexedUpdateBatch,
			bufferedBase:                bufferedRead.enabled,
			bufferedReadGeneration:      bufferedRead.writeGeneration,
			bufferedReadBlocked:         bufferedReadBlocked,
			policies:                    policies,
			semanticRecords:             semanticRecords,
			directBufferedUpdate: &directBufferedUpdatePlan{
				templateEntries:    templateEntries,
				primaryEntries:     primaryEntries,
				secondaryRootPlans: secondaryRootPlans,
				templateRootName:   templateRootName,
				primaryRootName:    primaryRootName,
				stagedBytes:        stagedBytes,
			},
			scratch: scratch,
		}
		scratchOwnedByPlan = true
		return plan, nil
	}

	var stateTable memtable.Table
	secondaryTables := make(map[string]memtable.Table, len(runtimes))
	secondaryRunStats := make([]CollectionUpdateSecondaryRunStats, len(runtimes))
	defer func() {
		if success {
			return
		}
		_ = snap.Close()
		resetCollectionTables(deltaTables)
		resetCollectionRunTable(stateTable)
		for _, table := range secondaryTables {
			resetCollectionRunTable(table)
		}
	}()

	if len(templateRecords) > 0 {
		phaseStart = updateBatchStatsNow(detailedStats)
		templatePlan := &insertBatchPlan{}
		if err := (insertBatchPlanner{
			collection:             meta.Name,
			templateRoot:           templateRootName,
			options:                plannerOptions,
			cloneTemplateRunValues: true,
		}).emitTemplateRun(templatePlan, templateRecords); err != nil {
			return nil, err
		}
		for _, run := range templatePlan.runs {
			rootNames = append(rootNames, run.name)
			uniqueSecondary = append(uniqueSecondary, -1)
			baseRootIDs[run.name] = catalog.rootID(run.name)
			policies = append(policies, run.storagePolicy)
			deltaTables = append(deltaTables, run.table)
		}
		stats.TemplateRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
	}

	rootNames = append(rootNames, primaryRootName)
	uniqueSecondary = append(uniqueSecondary, -1)
	baseRootIDs[primaryRootName] = primaryRoot
	policies = append(policies, plannerOptions.dataStoragePolicy)
	phaseStart = updateBatchStatsNow(detailedStats)
	primaryTable := newCollectionRunTable(len(changed))
	for _, item := range changed {
		setCollectionRunCopiedValue(primaryTable, item.documentID, item.document)
		results[item.itemIndex].Modified = true
	}
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)
	stats.PrimaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateTable = newCollectionRunTable(len(changed))
		}
		phaseStart = updateBatchStatsNow(detailedStats)
		for _, item := range changed {
			if !item.indexStateChanged || stateTable == nil {
				continue
			}
			if orderedDocumentIndexStateEmpty(item.newState) {
				stateTable.DeleteSteal(bytes.Clone(item.documentID))
			} else {
				rawState, err := encodeRuntimeOrderedDocumentIndexState(item.newState, runtimes)
				if err != nil {
					_ = snap.Close()
					return nil, err
				}
				stateTable.SetSteal(bytes.Clone(item.documentID), rawState)
			}
		}
		if stateTable != nil && stateTable.Len() > 0 {
			stateTable.Freeze()
			stateRootName := indexStateRootName
			rootNames = append(rootNames, stateRootName)
			uniqueSecondary = append(uniqueSecondary, -1)
			baseRootIDs[stateRootName] = catalog.rootID(stateRootName)
			policies = append(policies, plannerOptions.indexStateStoragePolicy)
			deltaTables = append(deltaTables, stateTable)
			stateTable = nil
		}
		stats.IndexStateRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
		phaseStart = updateBatchStatsNow(detailedStats)
		for _, item := range changed {
			if !item.indexStateChanged {
				continue
			}
			for runtimeIdx, runtime := range runtimes {
				if !preparedBatchUpdateIndexChanged(item, runtimeIdx) {
					continue
				}
				rootName := runtimeSecondaryRootName(meta.Name, runtime)
				table := secondaryTables[rootName]
				if table == nil {
					table = newCollectionRunTable(0)
					secondaryTables[rootName] = table
				}
				runStats := &secondaryRunStats[runtimeIdx]
				if runStats.IndexName == "" {
					runStats.IndexName = runtime.def.name
				}
				for _, encoded := range item.oldState.valuesAt(runtimeIdx) {
					keyLen, err := deleteCollectionSecondaryIndexEntry(table, encoded, item.documentID)
					if err != nil {
						_ = snap.Close()
						return nil, err
					}
					stats.SecondaryDeleteEntries++
					stats.SecondaryKeyBytes += keyLen
					runStats.Deletes++
					runStats.KeyBytes += keyLen
					if runtimeIdx < stats.IndexStatsCount {
						stats.IndexStats[runtimeIdx].SecondaryDeletes++
						stats.IndexStats[runtimeIdx].SecondaryKeyBytes += keyLen
					}
				}
				for _, encoded := range item.newState.valuesAt(runtimeIdx) {
					keyLen, err := setCollectionSecondaryIndexEntry(table, encoded, item.documentID)
					if err != nil {
						_ = snap.Close()
						return nil, err
					}
					stats.SecondarySetEntries++
					stats.SecondaryKeyBytes += keyLen
					runStats.Sets++
					runStats.KeyBytes += keyLen
					if runtimeIdx < stats.IndexStatsCount {
						stats.IndexStats[runtimeIdx].SecondarySets++
						stats.IndexStats[runtimeIdx].SecondaryKeyBytes += keyLen
					}
				}
			}
		}
		for runtimeIdx, runtime := range runtimes {
			rootName := runtimeSecondaryRootName(meta.Name, runtime)
			table := secondaryTables[rootName]
			if table == nil || table.Len() == 0 {
				continue
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			if runtime.def.unique {
				uniqueSecondary = append(uniqueSecondary, runtimeIdx)
			} else {
				uniqueSecondary = append(uniqueSecondary, -1)
			}
			baseRootIDs[rootName] = catalog.rootID(rootName)
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
			if runStats := secondaryRunStats[runtimeIdx]; runStats.Deletes != 0 || runStats.Sets != 0 || runStats.KeyBytes != 0 {
				stats.SecondaryRuns = append(stats.SecondaryRuns, runStats)
			}
			if runtimeIdx < stats.IndexStatsCount {
				stats.IndexStats[runtimeIdx].SecondaryRuns++
			}
			delete(secondaryTables, rootName)
		}
		stats.SecondaryRunBuild += updateBatchStatsSince(detailedStats, phaseStart)
	}

	// Tables moved into deltaTables are nil/deleted above; only unused scratch
	// tables remain here for immediate reset.
	resetCollectionRunTable(stateTable)
	for _, table := range secondaryTables {
		resetCollectionRunTable(table)
	}
	success = true
	plan := newUpdateBatchPlan()
	stats = updateCollectionUpdateStatsCounts(stats, results, len(deltaTables))
	semanticRecords := buildIndexedSemanticUpdateRecords(meta.Name, runtimes, changed)
	*plan = updateBatchPlan{
		results:                     results,
		stats:                       stats,
		meta:                        meta,
		catalog:                     catalog,
		snap:                        snap,
		baseUserRoot:                baseUserRoot,
		baseSystemRoot:              baseSystemRoot,
		baseCommitSeq:               baseCommitSeq,
		rootNames:                   rootNames,
		baseRootIDs:                 baseRootIDs,
		uniqueSecondaryIndexByRoot:  uniqueSecondary,
		canBufferIndexedUpdateBatch: canBufferIndexedUpdateBatch,
		bufferedBase:                bufferedRead.enabled,
		bufferedReadGeneration:      bufferedRead.writeGeneration,
		bufferedReadBlocked:         bufferedReadBlocked,
		policies:                    policies,
		deltaTables:                 deltaTables,
		semanticRecords:             semanticRecords,
		scratch:                     scratch,
	}
	scratchOwnedByPlan = true
	return plan, nil
}

func (c *Collection) publishUpdateBatchPlanLocked(plan *updateBatchPlan) ([]UpdateBatchResult, error) {
	if plan == nil {
		return nil, nil
	}
	if len(plan.deltaTables) == 0 {
		return plan.results, nil
	}
	if err := rejectCatalogRootOverlaysForWrite(plan.catalog); err != nil {
		return nil, err
	}
	if len(plan.rootNames) != len(plan.deltaTables) || len(plan.rootNames) != len(plan.policies) {
		return nil, fmt.Errorf("collections: UpdateBatch collection %q invalid plan lengths roots=%d deltas=%d policies=%d", plan.meta.Name, len(plan.rootNames), len(plan.deltaTables), len(plan.policies))
	}

	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(plan.meta.Name, plan.rootNames, plan.deltaTables, plan.baseRootIDs, plan.policies)
	if err != nil {
		return nil, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(plan.meta.Name, plan.rootNames, ordered)
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(plan.baseUserRoot, plan.baseSystemRoot, plan.baseCommitSeq)
	}
	detailedStats := c.updateBatchDetailedStatsEnabled()
	publishStart := updateBatchStatsNow(detailedStats)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(plan.meta, plan.baseCommitSeq, plan.baseSystemRoot, plan.rootNames, plan.baseRootIDs, rootIDs)
	})
	cleanupDeltas()
	plan.stats.Publish += updateBatchStatsSince(detailedStats, publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(plan.rootNames) {
		return nil, unexpectedOrderedRootCountError(plan.meta.Name, len(plan.rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(plan.catalog, plan.meta, plan.rootNames, rootIDs)
	c.meta = plan.meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if c.writeDomain != nil {
		c.writeDomain.observeRootDeltaPlan(deltaStats)
		if len(plan.meta.Indexes) == 0 {
			c.writeDomain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, true, deltaStats)
		}
	}
	return plan.results, nil
}

func updateBatchPlanUniqueSecondaryIndex(plan *updateBatchPlan, rootOffset int) (IndexDefinition, bool) {
	if plan == nil || rootOffset < 0 || rootOffset >= len(plan.uniqueSecondaryIndexByRoot) {
		return IndexDefinition{}, false
	}
	indexOffset := plan.uniqueSecondaryIndexByRoot[rootOffset]
	if indexOffset < 0 || indexOffset >= len(plan.meta.Indexes) {
		return IndexDefinition{}, false
	}
	indexDef := plan.meta.Indexes[indexOffset]
	if !indexDef.Unique {
		return IndexDefinition{}, false
	}
	return indexDef, true
}

func (c *Collection) bufferDirectUpdateBatchPlanLocked(plan *updateBatchPlan) (bool, error) {
	if c == nil || plan == nil || plan.directBufferedUpdate == nil || len(plan.directBufferedUpdate.primaryEntries) == 0 {
		return false, nil
	}
	direct := plan.directBufferedUpdate
	detailedStats := c.updateBatchDetailedStatsEnabled()
	bufferStart := updateBatchStatsNow(detailedStats)
	precheckStart := updateBatchStatsNow(detailedStats)
	defer func() {
		plan.stats.BufferStage += updateBatchStatsSince(detailedStats, bufferStart)
	}()
	if c.writeDomain == nil || !plan.canBufferIndexedUpdateBatch || !plan.meta.Options.BufferedIndexedWrites || len(plan.meta.Indexes) == 0 {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, nil
	}
	if len(plan.rootNames) == 0 || len(plan.rootNames) != len(plan.policies) || len(plan.rootNames) != len(plan.uniqueSecondaryIndexByRoot) {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, fmt.Errorf("collections: UpdateBatch collection %q invalid direct plan lengths roots=%d policies=%d unique=%d", plan.meta.Name, len(plan.rootNames), len(plan.policies), len(plan.uniqueSecondaryIndexByRoot))
	}
	modifiedCount := updateBatchModifiedCount(plan.results)
	if modifiedCount == 0 {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, nil
	}
	plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)

	domain := c.writeDomain
	lockStart := updateBatchStatsNow(detailedStats)
	domain.mu.Lock()
	plan.stats.BufferStageLockWait += updateBatchStatsSince(detailedStats, lockStart)
	lockHoldStart := updateBatchStatsNow(detailedStats)
	var lockReleasedDuringHold time.Duration
	defer func() {
		lockHold := updateBatchStatsSince(detailedStats, lockHoldStart)
		if lockReleasedDuringHold > 0 {
			if lockHold > lockReleasedDuringHold {
				lockHold -= lockReleasedDuringHold
			} else {
				lockHold = 0
			}
		}
		plan.stats.BufferStageLockHold += lockHold
		domain.mu.Unlock()
	}()

	phaseStart := updateBatchStatsNow(detailedStats)
	if domain.count != 0 {
		if !plan.bufferedBase || !updateBatchCanReadBufferedDomainLocked(domain, plan.meta, plan.baseSystemRoot) {
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
		if plan.bufferedReadGeneration != domain.writeGeneration {
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
	}
	if err := c.validateUpdateBatchPlanRootDescriptors(plan); err != nil {
		plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
		return false, err
	}
	plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)

	phaseStart = updateBatchStatsNow(detailedStats)
	for _, rootName := range plan.rootNames {
		baseRoot, ok := plan.baseRootIDs[rootName]
		if !ok {
			plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
			return false, fmt.Errorf("collections: UpdateBatch collection %q direct plan missing base root for %q", plan.meta.Name, rootName)
		}
		if hasPendingIndexedRootRunsForRootLocked(domain, rootName) {
			if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
				plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
				return false, errConcurrentRootModification(plan.meta.Name, rootName)
			}
		}
	}
	plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)

	phaseStart = updateBatchStatsNow(detailedStats)
	if domain.count == 0 && plan.catalog != nil {
		c.initializeWriteDomainFromCatalogLocked(domain, plan.catalog, plan.baseCommitSeq, plan.baseSystemRoot)
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(plan.rootNames))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(plan.rootNames))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(plan.rootNames))
	}
	addedRootRuns := estimateAccumulatedRootRunsForNamesLocked(domain, plan.rootNames)
	shouldAutoFlushAfterAdding := shouldFlushBufferedIndexedWritesAfterAdding(domain, plan.meta.Options, modifiedCount, direct.stagedBytes, addedRootRuns)
	if !shouldAutoFlushAfterAdding && collectionMetaHasSecondaryUniqueIndex(plan.meta) && bufferedIndexedAutoFlushEnabled(plan.meta.Options) {
		for i := range plan.rootNames {
			if _, ok := updateBatchPlanUniqueSecondaryIndex(plan, i); ok {
				shouldAutoFlushAfterAdding = true
				break
			}
		}
	}
	if shouldAutoFlushAfterAdding {
		freezeMutableIndexedRunMapsLocked(domain)
	}
	var checkpoint bufferedIndexedCheckpoint
	collectionMetaCheckpoint := c.meta
	if shouldAutoFlushAfterAdding {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	plan.stats.BufferStageDomainPrepare += updateBatchStatsSince(detailedStats, phaseStart)

	phaseStart = updateBatchStatsNow(detailedStats)
	rootTables := make(map[string]memtable.Table, len(plan.rootNames))
	actualRootRuns := 0
	for i, rootName := range plan.rootNames {
		baseRoot := plan.baseRootIDs[rootName]
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			return false, errConcurrentRootModification(plan.meta.Name, rootName)
		}
		if _, ok := domain.rootBaseIDs[rootName]; !ok {
			domain.rootBaseIDs[rootName] = baseRoot
		}
		domain.rootPolicies[rootName] = plan.policies[i]
		table, created := mutableRootRunLocked(domain, rootName)
		if table == nil {
			return false, fmt.Errorf("collections: UpdateBatch collection %q failed to allocate direct root accumulator for %q", plan.meta.Name, rootName)
		}
		rootTables[rootName] = table
		if created {
			actualRootRuns = saturatingAddNonNegativeInt(actualRootRuns, 1)
		}
	}
	if len(direct.templateEntries) > 0 {
		templateTable := rootTables[direct.templateRootName]
		if templateTable == nil {
			return false, fmt.Errorf("collections: UpdateBatch collection %q missing direct template root accumulator for %q", plan.meta.Name, direct.templateRootName)
		}
		if err := applyDirectBufferedRootEntries(templateTable, direct.templateEntries); err != nil {
			return false, err
		}
	}
	primaryTable := rootTables[direct.primaryRootName]
	var primaryIndexKeys [][]byte
	if domain.primaryRunIndex != nil {
		primaryIndexKeys = make([][]byte, 0, len(direct.primaryEntries))
	}
	if err := applyDirectBufferedRootEntries(primaryTable, direct.primaryEntries); err != nil {
		return false, err
	}
	for _, entry := range direct.primaryEntries {
		if primaryIndexKeys != nil {
			primaryIndexKeys = append(primaryIndexKeys, entry.key)
		}
	}
	if primaryIndexKeys != nil {
		addBufferedPrimaryRunIndexKeys(domain.primaryRunIndex, primaryIndexKeys, primaryTable)
	}
	for _, secondaryPlan := range direct.secondaryRootPlans {
		table := rootTables[secondaryPlan.rootName]
		if table == nil {
			continue
		}
		if err := applyCollectionRunEntriesWithFlags(table, len(secondaryPlan.entries), func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error) {
			entry := secondaryPlan.entries[i]
			if entry.tombstone {
				return entry.key, nil, page.ValuePtr{}, node.FlagTombstone, nil
			}
			return entry.key, nil, page.ValuePtr{}, node.FlagInline, nil
		}); err != nil {
			if shouldAutoFlushAfterAdding {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	plan.stats.BufferStageRootAppend += updateBatchStatsSince(detailedStats, phaseStart)
	retainDirectBufferedDocumentArenaLocked(domain, plan)

	domain.loaded = true
	domain.meta = plan.meta
	domain.catalog = plan.catalog
	domain.baseCommitSeq = plan.baseCommitSeq
	domain.baseSystemRoot = plan.baseSystemRoot
	if plan.catalog != nil {
		domain.primaryRoot = plan.catalog.rootID(collectionPrimaryRootName(plan.meta.Name))
	}
	domain.count += modifiedCount
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, direct.stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, modifiedCount)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, direct.stagedBytes)
	domain.writeGeneration++
	domain.observeIndexedStage(modifiedCount, direct.stagedBytes, actualRootRuns)
	c.meta = plan.meta
	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, plan.meta.Options)
	if err != nil {
		if shouldAutoFlushAfterAdding {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return false, err
	}
	semanticRecords := plan.semanticRecords
	if len(semanticRecords) > 0 {
		appendIndexedSemanticRecordsLocked(domain, semanticRecords)
	}
	if shouldFlushBufferedIndexedWrites(domain, plan.meta.Options) {
		flushDuration, lockReleased, relockWait, err := c.flushBufferedIndexedAfterThresholdLocked(domain, plan.meta.Options)
		if lockReleased > 0 {
			lockReleasedDuringHold += lockReleased
		}
		plan.stats.BufferStageLockWait += updateBatchStatsDuration(detailedStats, relockWait)
		plan.stats.BufferStageFlush += updateBatchStatsDuration(detailedStats, flushDuration)
		if err != nil {
			if shouldAutoFlushAfterAdding {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	resetCollectionTables(compactedObsolete)
	if len(semanticRecords) > 0 {
		domain.observeIndexedSemanticRawRecords(semanticRecords)
	}
	plan.stats.BufferedBatches = 1
	return true, nil
}

func (c *Collection) bufferUpdateBatchPlanLocked(plan *updateBatchPlan) (bool, error) {
	if c == nil || plan == nil {
		return false, nil
	}
	if plan.directBufferedUpdate != nil {
		return c.bufferDirectUpdateBatchPlanLocked(plan)
	}
	if len(plan.deltaTables) == 0 {
		return false, nil
	}
	detailedStats := c.updateBatchDetailedStatsEnabled()
	bufferStart := updateBatchStatsNow(detailedStats)
	precheckStart := updateBatchStatsNow(detailedStats)
	defer func() {
		plan.stats.BufferStage += updateBatchStatsSince(detailedStats, bufferStart)
	}()
	if c.writeDomain == nil || !plan.canBufferIndexedUpdateBatch || !plan.meta.Options.BufferedIndexedWrites || len(plan.meta.Indexes) == 0 {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, nil
	}
	if len(plan.rootNames) != len(plan.deltaTables) || len(plan.rootNames) != len(plan.policies) {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, fmt.Errorf("collections: UpdateBatch collection %q invalid plan lengths roots=%d deltas=%d policies=%d", plan.meta.Name, len(plan.rootNames), len(plan.deltaTables), len(plan.policies))
	}
	if len(plan.uniqueSecondaryIndexByRoot) != len(plan.rootNames) {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, fmt.Errorf("collections: UpdateBatch collection %q invalid unique index plan length roots=%d unique=%d", plan.meta.Name, len(plan.rootNames), len(plan.uniqueSecondaryIndexByRoot))
	}
	modifiedCount := updateBatchModifiedCount(plan.results)
	if modifiedCount == 0 {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, nil
	}
	hasDeltaTable := false
	for _, table := range plan.deltaTables {
		if table != nil && table.Len() > 0 {
			hasDeltaTable = true
			break
		}
	}
	if !hasDeltaTable {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, fmt.Errorf("collections: UpdateBatch collection %q modified rows without delta tables modified=%d roots=%d deltas=%d policies=%d", plan.meta.Name, modifiedCount, len(plan.rootNames), len(plan.deltaTables), len(plan.policies))
	}
	plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
	domain := c.writeDomain
	lockStart := updateBatchStatsNow(detailedStats)
	domain.mu.Lock()
	plan.stats.BufferStageLockWait += updateBatchStatsSince(detailedStats, lockStart)
	lockHoldStart := updateBatchStatsNow(detailedStats)
	var lockReleasedDuringHold time.Duration
	defer func() {
		lockHold := updateBatchStatsSince(detailedStats, lockHoldStart)
		if lockReleasedDuringHold > 0 {
			if lockHold > lockReleasedDuringHold {
				lockHold -= lockReleasedDuringHold
			} else {
				lockHold = 0
			}
		}
		plan.stats.BufferStageLockHold += lockHold
		domain.mu.Unlock()
	}()
	phaseStart := updateBatchStatsNow(detailedStats)
	if domain.count != 0 {
		if !plan.bufferedBase || !updateBatchCanReadBufferedDomainLocked(domain, plan.meta, plan.baseSystemRoot) {
			// The caller retries from the normal pre-plan flush path so a plan built
			// before a concurrent buffered write is not published against stale roots.
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
		if plan.bufferedReadGeneration != domain.writeGeneration {
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
	}
	if err := c.validateUpdateBatchPlanRootDescriptors(plan); err != nil {
		plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
		return false, err
	}
	plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
	var stagedBytes int64
	stagedRootRuns := 0
	phaseStart = updateBatchStatsNow(detailedStats)
	for i, rootName := range plan.rootNames {
		baseRoot, ok := plan.baseRootIDs[rootName]
		if !ok {
			plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
			return false, fmt.Errorf("collections: UpdateBatch collection %q plan missing base root for %q", plan.meta.Name, rootName)
		}
		table := plan.deltaTables[i]
		if table == nil || table.Len() == 0 {
			continue
		}
		if hasPendingIndexedRootRunsForRootLocked(domain, rootName) {
			if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
				plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
				return false, errConcurrentRootModification(plan.meta.Name, rootName)
			}
		}
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, table.Size())
		stagedRootRuns++
	}
	plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
	phaseStart = updateBatchStatsNow(detailedStats)
	if domain.count == 0 && plan.catalog != nil {
		c.initializeWriteDomainFromCatalogLocked(domain, plan.catalog, plan.baseCommitSeq, plan.baseSystemRoot)
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(plan.rootNames))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(plan.rootNames))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(plan.rootNames))
	}
	freezeMutableIndexedRunMapsLocked(domain)
	hasUniqueSecondaryRoots := collectionMetaHasSecondaryUniqueIndex(plan.meta)
	projectedStagedBytes := stagedBytes
	shouldAutoFlushAfterAdding := shouldFlushBufferedIndexedWritesAfterAdding(domain, plan.meta.Options, modifiedCount, projectedStagedBytes, stagedRootRuns)
	if !shouldAutoFlushAfterAdding && hasUniqueSecondaryRoots && bufferedIndexedAutoFlushEnabled(plan.meta.Options) {
		for i := range plan.rootNames {
			if _, ok := updateBatchPlanUniqueSecondaryIndex(plan, i); !ok {
				continue
			}
			table := plan.deltaTables[i]
			if table != nil && table.Len() > 0 {
				shouldAutoFlushAfterAdding = true
				break
			}
		}
	}
	var checkpoint bufferedIndexedCheckpoint
	collectionMetaCheckpoint := c.meta
	if shouldAutoFlushAfterAdding {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	plan.stats.BufferStageDomainPrepare += updateBatchStatsSince(detailedStats, phaseStart)
	for i, rootName := range plan.rootNames {
		baseRoot := plan.baseRootIDs[rootName]
		table := plan.deltaTables[i]
		if table == nil || table.Len() == 0 {
			continue
		}
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			return false, errConcurrentRootModification(plan.meta.Name, rootName)
		}
		if _, ok := domain.rootBaseIDs[rootName]; !ok {
			domain.rootBaseIDs[rootName] = baseRoot
		}
		if rootName == collectionPrimaryRootName(plan.meta.Name) && domain.primaryRunIndex != nil {
			phaseStart = updateBatchStatsNow(detailedStats)
			if err := addBufferedPrimaryRunIndexEntries(domain.primaryRunIndex, table); err != nil {
				plan.stats.BufferStagePrimaryIdx += updateBatchStatsSince(detailedStats, phaseStart)
				domain.primaryRunIndex = nil
				if shouldAutoFlushAfterAdding {
					rollbackBufferedIndexedDomain(domain, checkpoint)
					c.meta = collectionMetaCheckpoint
				}
				return false, err
			}
			plan.stats.BufferStagePrimaryIdx += updateBatchStatsSince(detailedStats, phaseStart)
		}
		if indexDef, ok := updateBatchPlanUniqueSecondaryIndex(plan, i); ok {
			phaseStart = updateBatchStatsNow(detailedStats)
			uniqueValueTable, uniquePrefixes, err := bufferedUniqueIndexValueRun(table, indexDef.ValueType)
			if err != nil {
				plan.stats.BufferStageUniqueIdx += updateBatchStatsSince(detailedStats, phaseStart)
				if shouldAutoFlushAfterAdding {
					rollbackBufferedIndexedDomain(domain, checkpoint)
					c.meta = collectionMetaCheckpoint
				}
				return false, err
			}
			if domain.uniqueValueRuns == nil {
				domain.uniqueValueRuns = make(map[string][]memtable.Table)
			}
			if domain.uniqueValueIndex == nil {
				domain.uniqueValueIndex = make(map[string]*bufferedUniqueValueIndex)
			}
			domain.uniqueValueRuns[indexDef.Name] = append(domain.uniqueValueRuns[indexDef.Name], uniqueValueTable)
			index := domain.uniqueValueIndex[indexDef.Name]
			if index == nil {
				index = newBufferedUniqueValueIndex(max(1, len(uniquePrefixes)))
				domain.uniqueValueIndex[indexDef.Name] = index
			}
			index.addAll(uniquePrefixes)
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, uniqueValueTable.Size())
			plan.stats.BufferStageUniqueIdx += updateBatchStatsSince(detailedStats, phaseStart)
		}
		phaseStart = updateBatchStatsNow(detailedStats)
		domain.rootPolicies[rootName] = plan.policies[i]
		domain.rootRuns[rootName] = append(domain.rootRuns[rootName], table)
		domain.rootRunCount = saturatingAddNonNegativeInt(domain.rootRunCount, 1)
		plan.deltaTables[i] = nil
		plan.stats.BufferStageRootAppend += updateBatchStatsSince(detailedStats, phaseStart)
	}
	plan.deltaTables = nil
	domain.loaded = true
	domain.meta = plan.meta
	domain.catalog = plan.catalog
	domain.baseCommitSeq = plan.baseCommitSeq
	domain.baseSystemRoot = plan.baseSystemRoot
	if plan.catalog != nil {
		domain.primaryRoot = plan.catalog.rootID(collectionPrimaryRootName(plan.meta.Name))
	}
	domain.count += modifiedCount
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, modifiedCount)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, stagedBytes)
	domain.writeGeneration++
	domain.observeIndexedStage(modifiedCount, stagedBytes, stagedRootRuns)
	c.meta = plan.meta
	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, plan.meta.Options)
	if err != nil {
		if shouldAutoFlushAfterAdding {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return false, err
	}
	semanticRecords := plan.semanticRecords
	if len(semanticRecords) > 0 {
		appendIndexedSemanticRecordsLocked(domain, semanticRecords)
	}
	if shouldFlushBufferedIndexedWrites(domain, plan.meta.Options) {
		flushDuration, lockReleased, relockWait, err := c.flushBufferedIndexedAfterThresholdLocked(domain, plan.meta.Options)
		if lockReleased > 0 {
			lockReleasedDuringHold += lockReleased
		}
		plan.stats.BufferStageLockWait += updateBatchStatsDuration(detailedStats, relockWait)
		plan.stats.BufferStageFlush += updateBatchStatsDuration(detailedStats, flushDuration)
		if err != nil {
			if shouldAutoFlushAfterAdding {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	resetCollectionTables(compactedObsolete)
	if len(semanticRecords) > 0 {
		domain.observeIndexedSemanticRawRecords(semanticRecords)
	}
	plan.stats.BufferedBatches = 1
	return true, nil
}

func updateBatchModifiedCount(results []UpdateBatchResult) int {
	count := 0
	for _, result := range results {
		if result.Modified {
			count++
		}
	}
	return count
}

func updateBatchMatchedCount(results []UpdateBatchResult) int {
	count := 0
	for _, result := range results {
		if result.Matched {
			count++
		}
	}
	return count
}

func updateCollectionUpdateStatsCounts(stats CollectionUpdateStats, results []UpdateBatchResult, runs int) CollectionUpdateStats {
	stats.Matched = updateBatchMatchedCount(results)
	stats.Modified = updateBatchModifiedCount(results)
	stats.Runs = runs
	return stats
}

func rejectBatchUniqueConflicts(runtimes []indexRuntime, updates []preparedBatchUpdate) error {
	type seenUniqueValue struct {
		documentID []byte
		itemIndex  int
	}

	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		hasChangedUniqueValue := false
		for _, update := range updates {
			if !preparedBatchUpdateIndexChanged(update, runtimeIdx) {
				continue
			}
			hasChangedUniqueValue = true
			break
		}
		if !hasChangedUniqueValue {
			continue
		}
		seen := make(map[string]seenUniqueValue)
		for _, update := range updates {
			for _, encoded := range update.newState.valuesAt(runtimeIdx) {
				key := string(encoded)
				if previous, ok := seen[key]; ok && !bytes.Equal(previous.documentID, update.documentID) {
					return fmt.Errorf("%w %q: batch indexes %d and %d document ids %q and %q", ErrUniqueIndexConflict, runtime.def.name, previous.itemIndex, update.itemIndex, previous.documentID, update.documentID)
				}
				seen[key] = seenUniqueValue{documentID: update.documentID, itemIndex: update.itemIndex}
			}
		}
	}
	return nil
}

func updateBatchChangesSecondaryUniqueIndex(runtimes []indexRuntime, updates []preparedBatchUpdate) bool {
	if len(runtimes) == 0 || len(updates) == 0 {
		return false
	}
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		for _, update := range updates {
			if preparedBatchUpdateIndexChanged(update, runtimeIdx) {
				return true
			}
		}
	}
	return false
}

func documentIndexRuntimeChanged(oldState, newState documentIndexState, runtime indexRuntime) bool {
	return !normalizedEncodedIndexValuesEqual(oldState[runtime.def.name], newState[runtime.def.name])
}

func orderedDocumentIndexRuntimeChanged(oldState, newState orderedDocumentIndexState, runtimeIdx int) bool {
	return !normalizedEncodedIndexValuesEqual(oldState.valuesAt(runtimeIdx), newState.valuesAt(runtimeIdx))
}

func updateIndexChangedMaskBit(runtimeIdx int) (uint64, bool) {
	if runtimeIdx < 0 || runtimeIdx >= 64 {
		return 0, false
	}
	return uint64(1) << uint(runtimeIdx), true
}

func preparedBatchUpdateIndexChanged(update preparedBatchUpdate, runtimeIdx int) bool {
	if bit, ok := updateIndexChangedMaskBit(runtimeIdx); ok {
		return update.changedIndexes&bit != 0
	}
	return orderedDocumentIndexRuntimeChanged(update.oldState, update.newState, runtimeIdx)
}

func normalizedEncodedIndexValuesEqual(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

type batchUniqueReplacementSet map[string]map[string]map[string]struct{}

func batchUniqueReplacementOwners(runtimes []indexRuntime, updates []preparedBatchUpdate) batchUniqueReplacementSet {
	if len(runtimes) == 0 || len(updates) == 0 {
		return nil
	}
	var out batchUniqueReplacementSet
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		indexName := runtime.def.name
		for _, update := range updates {
			if !preparedBatchUpdateIndexChanged(update, runtimeIdx) {
				continue
			}
			oldValues := update.oldState.valuesAt(runtimeIdx)
			if len(oldValues) == 0 {
				continue
			}
			newValues := update.newState.valuesAt(runtimeIdx)
			for _, oldValue := range oldValues {
				if documentIndexStateContainsValue(newValues, oldValue) {
					continue
				}
				if out == nil {
					out = make(batchUniqueReplacementSet)
				}
				byValue := out[indexName]
				if byValue == nil {
					byValue = make(map[string]map[string]struct{})
					out[indexName] = byValue
				}
				owners := byValue[string(oldValue)]
				if owners == nil {
					owners = make(map[string]struct{})
					byValue[string(oldValue)] = owners
				}
				owners[string(update.documentID)] = struct{}{}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func documentIndexStateContainsValue(values [][]byte, target []byte) bool {
	i := sort.Search(len(values), func(i int) bool {
		return bytes.Compare(values[i], target) >= 0
	})
	return i < len(values) && bytes.Equal(values[i], target)
}

func (s batchUniqueReplacementSet) allows(indexName string, encoded, documentID []byte) bool {
	if len(s) == 0 {
		return false
	}
	byValue := s[indexName]
	if len(byValue) == 0 {
		return false
	}
	owners := byValue[string(encoded)]
	if len(owners) == 0 {
		return false
	}
	_, ok := owners[string(documentID)]
	return ok
}

func (c *Collection) catalogForSnapshot(snap *backenddb.Snapshot) (*collectionCatalog, error) {
	return c.catalogForSnapshotWithWriteDomainLockState(snap, false)
}

func (c *Collection) catalogForSnapshotWithWriteDomainLocked(snap *backenddb.Snapshot) (*collectionCatalog, error) {
	return c.catalogForSnapshotWithWriteDomainLockState(snap, true)
}

func (c *Collection) catalogForSnapshotWithWriteDomainLockState(snap *backenddb.Snapshot, writeDomainLocked bool) (*collectionCatalog, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	systemRoot := snapshotSystemRoot(snap)
	commitSeq := snapshotCommitSeq(snap)

	c.catalogMu.RLock()
	if cached := c.catalog; cached != nil && c.catalogSystemRoot == systemRoot && c.catalogCommitSeq == commitSeq {
		c.catalogMu.RUnlock()
		return cached, nil
	}
	c.catalogMu.RUnlock()

	var cached *collectionCatalog
	if writeDomainLocked {
		cached = cachedWriteDomainCatalogForStateLocked(c.writeDomain, systemRoot, commitSeq)
	} else {
		cached = cachedWriteDomainCatalogForState(c.writeDomain, systemRoot, commitSeq)
	}
	if cached != nil {
		c.rememberCatalog(snap, cached)
		return cached, nil
	}

	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil {
		return nil, err
	}
	c.rememberCatalog(snap, catalog)
	return catalog, nil
}

func cachedWriteDomainCatalogForState(domain *collectionWriteDomain, systemRoot, commitSeq uint64) *collectionCatalog {
	if domain == nil || systemRoot == 0 {
		return nil
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return cachedWriteDomainCatalogForStateLocked(domain, systemRoot, commitSeq)
}

func cachedWriteDomainCatalogForStateLocked(domain *collectionWriteDomain, systemRoot, commitSeq uint64) *collectionCatalog {
	if domain == nil || systemRoot == 0 {
		return nil
	}
	if !domain.loaded || domain.catalog == nil || domain.baseSystemRoot != systemRoot || domain.baseCommitSeq != commitSeq {
		return nil
	}
	// Collection catalogs are immutable after publication, so callers may retain
	// and share this pointer across handle-local caches.
	return domain.catalog
}

func snapshotSystemRoot(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.SystemRootPageID
	}
	return 0
}

func snapshotUserRoot(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.RootPageID
	}
	return 0
}

func snapshotCommitSeq(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state := snap.State(); state != nil {
		return state.CommitSeq
	}
	return 0
}

func dbCommitSeqAndSystemRoot(db *backenddb.DB) (uint64, uint64) {
	if db == nil {
		return 0, 0
	}
	if state := db.State(); state != nil {
		return state.CommitSeq, state.SystemRootPageID
	}
	return 0, 0
}

func (c *Collection) commitSeqForSystemRoot(systemRoot uint64) uint64 {
	if c == nil {
		return 0
	}
	commitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot != systemRoot {
		return 0
	}
	return commitSeq
}

func (c *Collection) rememberCatalog(snap *backenddb.Snapshot, catalog *collectionCatalog) {
	if c == nil || snap == nil || catalog == nil {
		return
	}
	commitSeq := snapshotCommitSeq(snap)
	systemRoot := snapshotSystemRoot(snap)
	c.catalogMu.Lock()
	c.catalogCommitSeq = commitSeq
	c.catalogSystemRoot = systemRoot
	c.catalog = catalog
	c.catalogMu.Unlock()
}

func (c *Collection) rememberCatalogAtSystemRoot(systemRoot uint64, catalog *collectionCatalog) {
	if c == nil || catalog == nil {
		return
	}
	commitSeq := c.commitSeqForSystemRoot(systemRoot)
	c.catalogMu.Lock()
	c.catalogCommitSeq = commitSeq
	c.catalogSystemRoot = systemRoot
	c.catalog = catalog
	c.catalogMu.Unlock()
}

func (c *Collection) noteWriteDomainCatalog(systemRoot uint64, catalog *collectionCatalog) {
	if c == nil || catalog == nil || c.writeDomain == nil {
		return
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if domain.count != 0 {
		return
	}
	options, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		domain.loaded = false
		return
	}
	if !collectionMetaIndexSchemasEqual(domain.meta, catalog.meta) {
		resetCollectionUpdateIndexAggregateStatsLocked(domain)
	}
	domain.loaded = true
	domain.meta = copyCollectionMeta(catalog.meta)
	domain.catalog = catalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(systemRoot)
	domain.baseSystemRoot = systemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.storagePolicy = options.dataStoragePolicy
	domain.indexedFlushUnits = nil
	domain.rootRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.rootValueArenas = nil
	domain.rootRunCount = 0
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueIndex = nil
	if domain.table == nil {
		domain.table = newCollectionRunTable(0)
	}
}

func collectionMetaIndexSchemasEqual(left, right CollectionMeta) bool {
	if left.Name != right.Name || len(left.Indexes) != len(right.Indexes) {
		return false
	}
	for i := range left.Indexes {
		if left.Indexes[i] != right.Indexes[i] {
			return false
		}
	}
	return true
}

func resetCollectionUpdateIndexAggregateStatsLocked(domain *collectionWriteDomain) {
	if domain == nil {
		return
	}
	for i := 0; i < maxCollectionUpdateInlineIndexStats; i++ {
		domain.updateBatchIndexChanged[i].Store(0)
		domain.updateBatchIndexUnchanged[i].Store(0)
		domain.updateBatchIndexUniqueChecks[i].Store(0)
		domain.updateBatchIndexUniqueSkips[i].Store(0)
		domain.updateBatchIndexSecondaryRuns[i].Store(0)
		domain.updateBatchIndexSecondaryDeletes[i].Store(0)
		domain.updateBatchIndexSecondarySets[i].Store(0)
		domain.updateBatchIndexSecondaryBytes[i].Store(0)
	}
}

func cloneCatalogWithRootUpdates(base *collectionCatalog, meta CollectionMeta, rootNames []string, rootIDs []uint64) *collectionCatalog {
	roots := make(map[string]uint64)
	var rootOverlays map[string][]uint64
	var rootOverlayFilters map[string]map[uint64]collectionRootOverlayFilter
	if base != nil {
		for name, rootID := range base.roots {
			roots[name] = rootID
		}
		rootOverlays = cloneRootOverlayMap(base.rootOverlays)
		rootOverlayFilters = cloneRootOverlayFilterMap(base.rootOverlayFilters)
	}
	for i, name := range rootNames {
		if i < len(rootIDs) {
			roots[name] = rootIDs[i]
			if len(rootOverlays) != 0 {
				delete(rootOverlays, name)
			}
			if len(rootOverlayFilters) != 0 {
				delete(rootOverlayFilters, name)
			}
		}
	}
	return newCollectionCatalogWithOverlayMetadataOwned(copyCollectionMeta(meta), roots, rootOverlays, rootOverlayFilters)
}

func cloneCatalogWithRootOverlays(base *collectionCatalog, meta CollectionMeta, rootNames []string, rootIDs []uint64) *collectionCatalog {
	roots := make(map[string]uint64)
	var rootOverlays map[string][]uint64
	var rootOverlayFilters map[string]map[uint64]collectionRootOverlayFilter
	if base != nil {
		for name, rootID := range base.roots {
			roots[name] = rootID
		}
		rootOverlays = cloneRootOverlayMap(base.rootOverlays)
		rootOverlayFilters = cloneRootOverlayFilterMap(base.rootOverlayFilters)
	}
	if rootOverlays == nil {
		rootOverlays = make(map[string][]uint64)
	}
	for i, name := range rootNames {
		if i >= len(rootIDs) || rootIDs[i] == 0 {
			continue
		}
		existing := rootOverlays[name]
		overlays := overlayDescriptorRootsAfterDelta(existing, rootIDs[i])
		rootOverlays[name] = overlays
		if len(rootOverlayFilters) != 0 {
			rootOverlayFilters[name] = pruneRootOverlayFilters(rootOverlayFilters[name], overlays)
			if len(rootOverlayFilters[name]) == 0 {
				delete(rootOverlayFilters, name)
			}
		}
	}
	return newCollectionCatalogWithOverlayMetadataOwned(copyCollectionMeta(meta), roots, rootOverlays, rootOverlayFilters)
}

func cloneCatalogWithRootOverlayFilters(base *collectionCatalog, rootNames []string, rootIDs []uint64, filters map[string]collectionRootOverlayFilter) *collectionCatalog {
	if base == nil || len(filters) == 0 {
		return base
	}
	roots := make(map[string]uint64, len(base.roots))
	for name, rootID := range base.roots {
		roots[name] = rootID
	}
	rootOverlays := cloneRootOverlayMap(base.rootOverlays)
	rootOverlayFilters := cloneRootOverlayFilterMap(base.rootOverlayFilters)
	if rootOverlayFilters == nil {
		rootOverlayFilters = make(map[string]map[uint64]collectionRootOverlayFilter)
	}
	for i, rootName := range rootNames {
		if i >= len(rootIDs) || rootIDs[i] == 0 {
			continue
		}
		filter, ok := filters[rootName]
		if !ok {
			continue
		}
		byRoot := rootOverlayFilters[rootName]
		if byRoot == nil {
			byRoot = make(map[uint64]collectionRootOverlayFilter)
			rootOverlayFilters[rootName] = byRoot
		}
		byRoot[rootIDs[i]] = filter.clone()
	}
	return newCollectionCatalogWithOverlayMetadataOwned(copyCollectionMeta(base.meta), roots, rootOverlays, rootOverlayFilters)
}

func newCollectionCatalog(meta CollectionMeta, roots map[string]uint64) *collectionCatalog {
	return newCollectionCatalogWithOverlays(meta, roots, nil)
}

func newCollectionCatalogWithOverlays(meta CollectionMeta, roots map[string]uint64, rootOverlays map[string][]uint64) *collectionCatalog {
	return newCollectionCatalogWithOverlayMetadata(meta, roots, rootOverlays, nil)
}

func newCollectionCatalogWithOverlayMetadata(meta CollectionMeta, roots map[string]uint64, rootOverlays map[string][]uint64, rootOverlayFilters map[string]map[uint64]collectionRootOverlayFilter) *collectionCatalog {
	return newCollectionCatalogWithOverlayMetadataOwned(meta, roots, cloneRootOverlayMap(rootOverlays), cloneRootOverlayFilterMap(rootOverlayFilters))
}

func newCollectionCatalogWithOverlayMetadataOwned(meta CollectionMeta, roots map[string]uint64, rootOverlays map[string][]uint64, rootOverlayFilters map[string]map[uint64]collectionRootOverlayFilter) *collectionCatalog {
	catalog := &collectionCatalog{
		meta:               meta,
		roots:              roots,
		rootOverlays:       rootOverlays,
		rootOverlayFilters: rootOverlayFilters,
		primaryRootName:    collectionPrimaryRootName(meta.Name),
		templateRootName:   collectionTemplateRootName(meta.Name),
		indexStateRootName: collectionIndexStateRootName(meta.Name),
	}
	if len(meta.Indexes) > 0 {
		catalog.indexRuntimes, catalog.indexRuntimesErr = (insertBatchPlanner{
			collection: meta.Name,
			indexes:    plannerIndexes(meta.Indexes),
		}).indexRuntimes()
	}
	return catalog
}

func cloneRootOverlayMap(in map[string][]uint64) map[string][]uint64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]uint64, len(in))
	for name, roots := range in {
		if len(roots) == 0 {
			continue
		}
		out[name] = append([]uint64(nil), roots...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneRootOverlayFilterMap(in map[string]map[uint64]collectionRootOverlayFilter) map[string]map[uint64]collectionRootOverlayFilter {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]map[uint64]collectionRootOverlayFilter, len(in))
	for rootName, byRoot := range in {
		if len(byRoot) == 0 {
			continue
		}
		outByRoot := make(map[uint64]collectionRootOverlayFilter, len(byRoot))
		for rootID, filter := range byRoot {
			outByRoot[rootID] = filter.clone()
		}
		if len(outByRoot) != 0 {
			out[rootName] = outByRoot
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func pruneRootOverlayFilters(filters map[uint64]collectionRootOverlayFilter, rootIDs []uint64) map[uint64]collectionRootOverlayFilter {
	if len(filters) == 0 || len(rootIDs) == 0 {
		return nil
	}
	out := make(map[uint64]collectionRootOverlayFilter, len(rootIDs))
	for _, rootID := range rootIDs {
		filter, ok := filters[rootID]
		if !ok {
			continue
		}
		out[rootID] = filter.clone()
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func rejectCatalogRootOverlaysForWrite(catalog *collectionCatalog) error {
	if catalog == nil || len(catalog.rootOverlays) == 0 {
		return nil
	}
	return errCollectionRootOverlaysRequireCompaction
}

func rejectCatalogRootOverlaysForIndexedBufferWrite(catalog *collectionCatalog) error {
	if catalog == nil || len(catalog.rootOverlays) == 0 || collectionMetaUsesIndexedOverlayRoots(catalog.meta) {
		return nil
	}
	return errCollectionRootOverlaysRequireCompaction
}

func collectionMetaUsesIndexedOverlayRoots(meta CollectionMeta) bool {
	return meta.Options.BufferedIndexedOverlayRoots && meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0
}

func (c *collectionCatalog) cachedIndexRuntimes() ([]indexRuntime, error) {
	if c == nil {
		return nil, nil
	}
	return c.indexRuntimes, c.indexRuntimesErr
}

func runtimeSecondaryRootName(collection string, runtime indexRuntime) string {
	if runtime.secondaryRootName != "" {
		return runtime.secondaryRootName
	}
	return collectionSecondaryRootName(collection, runtime.def.name)
}

func buildDeleteRootDeltaTable(deleteKeys [][]byte) memtable.Table {
	table := newCollectionRunTable(len(deleteKeys))
	for _, key := range deleteKeys {
		table.DeleteSteal(bytes.Clone(key))
	}
	table.Freeze()
	return table
}

func buildCreateIndexBackfillPlan(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	newRuntime indexRuntime,
	existingRuntimes []indexRuntime,
	opts collectionOptions,
) (*createIndexBackfillPlan, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	primaryRootID := catalog.rootID(primaryRootName)
	plan := &createIndexBackfillPlan{
		baseRootIDs: make(map[string]uint64, 2),
	}
	plan.baseRootIDs[primaryRootName] = primaryRootID
	if primaryRootID == 0 {
		return plan, nil
	}

	it, err := collectionIteratorAtCatalogRoot(snap, catalog, primaryRootName, nil, nil, false)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return plan, nil
	}
	defer func() { _ = it.Close() }()

	persistIndexState := persistIndexStateForOptions(opts)
	var indexStateTable memtable.Table
	if persistIndexState {
		indexStateTable = newCollectionRunTable(0)
	}
	secondaryTable := newCollectionRunTable(0)
	uniqueProbes := make([]uniqueProbeCandidate, 0)
	stateRootName := collectionIndexStateRootName(catalog.meta.Name)
	stateRootID := catalog.rootID(stateRootName)
	secondaryRootName := collectionSecondaryRootName(catalog.meta.Name, newRuntime.def.name)
	documentCount := 0
	secondaryCount := 0
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		documentID := bytes.Clone(it.UnsafeKey())
		document := it.ValueCopy(nil)
		if err := it.Error(); err != nil {
			return nil, err
		}

		newState, err := indexStateForDocument(document, []indexRuntime{newRuntime}, opts)
		if err != nil {
			return nil, err
		}
		values := newState[newRuntime.def.name]
		if persistIndexState {
			existingState, err := loadBackfillIndexState(snap, stateRootID, documentID, document, existingRuntimes, opts)
			if err != nil {
				return nil, err
			}
			merged := cloneDocumentIndexState(existingState)
			if len(values) > 0 {
				merged[newRuntime.def.name] = values
			} else {
				delete(merged, newRuntime.def.name)
			}
			rawState, err := encodeNormalizedDocumentIndexState(merged)
			if err != nil {
				return nil, err
			}
			indexStateTable.SetSteal(bytes.Clone(documentID), rawState)
			documentCount++
		}

		for _, encoded := range values {
			if _, err := setCollectionSecondaryIndexEntry(secondaryTable, encoded, documentID); err != nil {
				return nil, err
			}
			secondaryCount++
			if !newRuntime.def.unique {
				continue
			}
			uniqueProbes = append(uniqueProbes, uniqueProbeCandidate{
				indexName:    newRuntime.def.name,
				encodedValue: encoded,
				documentID:   bytes.Clone(documentID),
			})
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if _, err := buildUniqueProbeRuns(uniqueProbes); err != nil {
		return nil, err
	}
	if documentCount > 0 {
		indexStateTable.Freeze()
		plan.rootNames = append(plan.rootNames, stateRootName)
		plan.baseRootIDs[stateRootName] = stateRootID
		plan.tables = append(plan.tables, indexStateTable)
		plan.policies = append(plan.policies, opts.indexStateStoragePolicy)
	}
	if secondaryCount > 0 {
		secondaryTable.Freeze()
		plan.rootNames = append(plan.rootNames, secondaryRootName)
		plan.baseRootIDs[secondaryRootName] = catalog.rootID(secondaryRootName)
		plan.tables = append(plan.tables, secondaryTable)
		plan.policies = append(plan.policies, newRuntime.def.storagePolicy)
	}
	return plan, nil
}

func loadBackfillIndexState(snap *backenddb.Snapshot, stateRootID uint64, documentID, document []byte, existingRuntimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	if persistIndexStateForOptions(opts) && stateRootID != 0 {
		entry, err := snap.GetEntryAtRoot(stateRootID, documentID)
		if err == nil {
			return decodeDocumentIndexState(entry.Value)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
	}
	return indexStateForDocument(document, existingRuntimes, opts)
}

func cloneDocumentIndexState(state documentIndexState) documentIndexState {
	out := make(documentIndexState, len(state))
	for name, values := range state {
		out[name] = normalizeEncodedIndexValues(values)
	}
	return out
}

func documentIndexStatesEqual(left, right documentIndexState) bool {
	if len(left) != len(right) {
		return false
	}
	for name, leftValues := range left {
		rightValues, ok := right[name]
		if !ok || len(leftValues) != len(rightValues) {
			return false
		}
		for i := range leftValues {
			if !bytes.Equal(leftValues[i], rightValues[i]) {
				return false
			}
		}
	}
	return true
}

func orderedDocumentIndexStatesEqual(left, right orderedDocumentIndexState) bool {
	maxLen := len(left)
	if len(right) > maxLen {
		maxLen = len(right)
	}
	for i := 0; i < maxLen; i++ {
		if !normalizedEncodedIndexValuesEqual(left.valuesAt(i), right.valuesAt(i)) {
			return false
		}
	}
	return true
}

func orderedDocumentIndexStateEmpty(state orderedDocumentIndexState) bool {
	for i := range state {
		if len(state[i]) > 0 {
			return false
		}
	}
	return true
}

func (c *Collection) buildRootDescriptorSystemIterator(rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(c.meta.Name, len(rootNames), len(rootIDs))
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, c.meta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	for _, rootName := range rootNames {
		if got, want := catalog.rootID(rootName), baseRootIDs[rootName]; got != want {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", rootName)
		}
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemTargetIterator(current, updates)
}

func unexpectedOrderedRootCountError(collectionName string, expected, actual int) error {
	if collectionName != "" {
		return fmt.Errorf("collections: ordered root publish returned unexpected root count collection=%q expected=%d actual=%d", collectionName, expected, actual)
	}
	return fmt.Errorf("collections: ordered root publish returned unexpected root count expected=%d actual=%d", expected, actual)
}

func (c *Collection) buildRootDescriptorSystemDeltaIterator(expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if c == nil || c.db == nil {
		return nil, backenddb.ErrClosed
	}
	return c.buildRootDescriptorSystemDeltaIteratorForMeta(c.meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, rootIDs)
}

func (c *Collection) buildRootDescriptorSystemDeltaIteratorForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}

func (c *Collection) buildRootOverlayDescriptorSystemDeltaIteratorForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, expectedOverlays map[string][]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if c == nil || c.db == nil {
		return nil, backenddb.ErrClosed
	}
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	if err := c.validateRootOverlayDescriptorSystemDeltaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, expectedOverlays); err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames))
	for i, rootName := range rootNames {
		existing := expectedOverlays[rootName]
		overlays := overlayDescriptorRootsAfterDelta(existing, rootIDs[i])
		updates[systemCollectionRootOverlayKey(rootName)] = encodeRootIDList(overlays)
	}
	return buildSystemDeltaIterator(updates)
}

func (c *Collection) buildRootOverlayCompactionSystemDeltaIteratorForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, expectedOverlays map[string][]uint64, rootIDs []uint64) (iterator.UnsafeIterator, error) {
	if c == nil || c.db == nil {
		return nil, backenddb.ErrClosed
	}
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	if err := c.validateRootOverlayDescriptorSnapshotForMeta(current, meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, expectedOverlays); err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames)*2)
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
		updates[systemCollectionRootOverlayKey(rootName)] = encodeRootIDList(nil)
	}
	return buildSystemDeltaIterator(updates)
}

func overlayDescriptorRootsAfterDelta(existing []uint64, newRoot uint64) []uint64 {
	if newRoot == 0 {
		return append([]uint64(nil), existing...)
	}
	if len(existing) <= 1 {
		return []uint64{newRoot}
	}
	overlays := make([]uint64, 0, len(existing)+1)
	overlays = append(overlays, newRoot)
	overlays = append(overlays, existing...)
	return overlays
}

func (c *Collection) validateRootDescriptorSystemDelta(expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	return c.validateRootDescriptorSystemDeltaForMeta(c.meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs)
}

func (c *Collection) validateRootDescriptorSystemDeltaForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	for _, rootName := range rootNames {
		if _, ok := baseRootIDs[rootName]; !ok {
			return fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, rootName)
		}
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot != expectedSystemRoot || currentCommitSeq != expectedCommitSeq {
		current := c.db.AcquireSnapshot()
		if current == nil {
			return backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		catalog, err := loadCollectionCatalog(current, meta.Name)
		if err != nil {
			return err
		}
		if catalog == nil {
			return errCollectionNotFound
		}
		if !sameCollectionMeta(catalog.meta, meta) {
			return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
		}
		for _, rootName := range rootNames {
			want := baseRootIDs[rootName]
			if got := catalog.rootID(rootName); got != want {
				return errConcurrentRootModification(meta.Name, rootName)
			}
		}
	}
	return nil
}

func (c *Collection) validateRootOverlayDescriptorSystemDeltaForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, expectedOverlays map[string][]uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	for _, rootName := range rootNames {
		if _, ok := baseRootIDs[rootName]; !ok {
			return fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, rootName)
		}
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot == expectedSystemRoot && currentCommitSeq == expectedCommitSeq {
		return nil
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	return c.validateRootOverlayDescriptorSnapshotForMeta(current, meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, expectedOverlays)
}

func (c *Collection) validateRootOverlayDescriptorSnapshotForMeta(snap *backenddb.Snapshot, meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, expectedOverlays map[string][]uint64) error {
	if snap == nil {
		return backenddb.ErrClosed
	}
	if snap.State() == nil {
		return backenddb.ErrClosed
	}
	// A raw TreeDB user-root commit does not change collection descriptors, and
	// unrelated collection commits should not block this collection. Validate the
	// descriptor contents directly instead of relying only on commit sequence.
	_ = expectedCommitSeq
	_ = expectedSystemRoot
	catalog, err := loadCollectionCatalog(snap, meta.Name)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	for _, rootName := range rootNames {
		wantRoot, ok := baseRootIDs[rootName]
		if !ok {
			return fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, rootName)
		}
		if got := catalog.rootID(rootName); got != wantRoot {
			return errConcurrentRootModification(meta.Name, rootName)
		}
		if !uint64SlicesEqual(catalog.overlayRootIDs(rootName), expectedOverlays[rootName]) {
			return errConcurrentRootModification(meta.Name, rootName)
		}
	}
	return nil
}

func uint64SlicesEqual(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (c *Collection) validateMutationRootDescriptors(expectedUserRoot, expectedSystemRoot, expectedCommitSeq uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	state := c.db.State()
	if state == nil {
		return backenddb.ErrClosed
	}
	if state.CommitSeq == expectedCommitSeq {
		return nil
	}
	// Raw TreeDB writes advance CommitSeq and move only the user root. Those do
	// not invalidate collection roots, so Update can still publish safely.
	if state.SystemRootPageID == expectedSystemRoot {
		if state.RootPageID != expectedUserRoot {
			return nil
		}
		return fmt.Errorf("%w: ambiguous same-root commit detected", ErrConcurrentMutation)
	}
	return fmt.Errorf("%w: concurrent collection root modification detected", ErrConcurrentMutation)
}

func (c *Collection) buildSchemaAndRootDescriptorSystemIterator(
	baseMeta CollectionMeta,
	newMeta CollectionMeta,
	rootNames []string,
	baseRootIDs map[string]uint64,
	rootIDs []uint64,
) (iterator.UnsafeIterator, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(newMeta.Name, len(rootNames), len(rootIDs))
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, baseMeta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, baseMeta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", baseMeta.Name)
	}
	primaryRootName := collectionPrimaryRootName(baseMeta.Name)
	if baseRootID, ok := baseRootIDs[primaryRootName]; ok {
		if got := catalog.rootID(primaryRootName); got != baseRootID {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", primaryRootName)
		}
	}
	for _, rootName := range rootNames {
		if got, want := catalog.rootID(rootName), baseRootIDs[rootName]; got != want {
			return nil, fmt.Errorf("collections: concurrent root modification detected for %q", rootName)
		}
	}

	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, 1+len(rootNames))
	updates[systemCollectionMetaKey(baseMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemTargetIterator(current, updates)
}

func (c *Collection) buildSchemaOnlySystemDeltaIterator(baseMeta CollectionMeta, encodedMeta []byte, clearedRootNames []string) (iterator.UnsafeIterator, error) {
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, baseMeta.Name)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	if !sameCollectionMeta(catalog.meta, baseMeta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", baseMeta.Name)
	}
	updates := map[string][]byte{
		systemCollectionMetaKey(baseMeta.Name): encodedMeta,
	}
	for _, rootName := range clearedRootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(0)
	}
	return buildSystemDeltaIterator(updates)
}

func loadDeleteIndexState(snap *backenddb.Snapshot, catalog *collectionCatalog, documentID, document []byte, runtimes []indexRuntime, opts collectionOptions) (documentIndexState, error) {
	if catalog == nil || len(runtimes) == 0 {
		return nil, nil
	}
	stateRootName := collectionIndexStateRootName(catalog.meta.Name)
	stateRoot := catalog.rootID(stateRootName)
	if persistIndexStateForOptions(opts) && (stateRoot != 0 || len(catalog.overlayRootIDs(stateRootName)) != 0) {
		entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, stateRootName, documentID)
		if err == nil {
			return decodeDocumentIndexState(entry.Value)
		}
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
	}
	return indexStateForDocument(document, runtimes, opts)
}

func deleteSecondaryEntriesForDocument(table memtable.Table, runtime indexRuntime, state documentIndexState, documentID []byte) error {
	if table == nil {
		return nil
	}
	values := state[runtime.def.name]
	if len(values) == 0 {
		return nil
	}
	for _, encoded := range values {
		if _, err := deleteCollectionSecondaryIndexEntry(table, encoded, documentID); err != nil {
			return err
		}
	}
	return nil
}

func rejectReplaceUniqueConflicts(snap *backenddb.Snapshot, catalog *collectionCatalog, runtimes []indexRuntime, oldState, newState documentIndexState, documentID []byte, batchReplacements batchUniqueReplacementSet) error {
	if snap == nil || catalog == nil {
		return nil
	}
	for _, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		if !documentIndexRuntimeChanged(oldState, newState, runtime) {
			continue
		}
		rootName := collectionSecondaryRootName(catalog.meta.Name, runtime.def.name)
		for _, encoded := range newState[runtime.def.name] {
			_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encoded)), encoded)
			if err != nil {
				return err
			}
			it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, prefixEnd(prefix), true)
			if err != nil {
				return err
			}
			if it == nil {
				continue
			}
			conflict := false
			for it.Valid() {
				key := it.UnsafeKey()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				ownerID := key[len(prefix):]
				if !it.IsDeleted() && !bytes.Equal(ownerID, documentID) && !batchReplacements.allows(runtime.def.name, encoded, ownerID) {
					conflict = true
					break
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			if iterErr != nil {
				return iterErr
			}
			if conflict {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
			}
		}
	}
	return nil
}

func rejectReplaceUniqueConflictsOrdered(snap *backenddb.Snapshot, catalog *collectionCatalog, runtimes []indexRuntime, update preparedBatchUpdate, batchReplacements batchUniqueReplacementSet) error {
	if snap == nil || catalog == nil {
		return nil
	}
	for runtimeIdx, runtime := range runtimes {
		if !runtime.def.unique {
			continue
		}
		if !preparedBatchUpdateIndexChanged(update, runtimeIdx) {
			continue
		}
		rootName := collectionSecondaryRootName(catalog.meta.Name, runtime.def.name)
		for _, encoded := range update.newState.valuesAt(runtimeIdx) {
			_, prefix, err := appendIndexValuePrefixSlice(make([]byte, 0, 2+len(encoded)), encoded)
			if err != nil {
				return err
			}
			it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, prefixEnd(prefix), true)
			if err != nil {
				return err
			}
			if it == nil {
				continue
			}
			conflict := false
			for it.Valid() {
				key := it.UnsafeKey()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				ownerID := key[len(prefix):]
				if !it.IsDeleted() && !bytes.Equal(ownerID, update.documentID) && !batchReplacements.allows(runtime.def.name, encoded, ownerID) {
					conflict = true
					break
				}
				it.Next()
			}
			iterErr := it.Error()
			_ = it.Close()
			if iterErr != nil {
				return iterErr
			}
			if conflict {
				return fmt.Errorf("%w %q", ErrUniqueIndexConflict, runtime.def.name)
			}
		}
	}
	return nil
}

// Get returns an owned copy of the document for documentID.
//
// Missing documents return (nil, nil), matching the existing collection API.
// Present-but-empty documents return a non-nil empty slice.
func (c *Collection) Get(documentID []byte) ([]byte, error) {
	out, found, err := c.GetInto(documentID, nil)
	if err != nil || !found {
		return nil, err
	}
	if len(out) == 0 {
		return []byte{}, nil
	}
	if cap(out) == len(out) {
		return out, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, nil
}

// NewStoredDocumentJSONMaterializer prepares a reusable materializer for stored
// collection documents. Callers that materialize multiple template-v1 documents
// should reuse one materializer so the backend snapshot and template resolver
// are shared across the request.
func (c *Collection) NewStoredDocumentJSONMaterializer() (*StoredDocumentJSONMaterializer, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	documentFormat, err := normalizeDocumentFormat(c.meta.Options.DocumentFormat)
	if err != nil {
		return nil, err
	}
	switch documentFormat {
	case DocumentFormatJSON, DocumentFormatBSON:
		return &StoredDocumentJSONMaterializer{documentFormat: documentFormat}, nil
	case DocumentFormatTemplateV1:
		snap := c.db.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		closeOnErr := true
		defer func() {
			if closeOnErr {
				_ = snap.Close()
			}
		}()
		catalog, err := c.catalogForSnapshot(snap)
		if err != nil {
			return nil, err
		}
		if catalog == nil {
			return nil, errCollectionNotFound
		}
		plannerOptions, err := collectionPlannerOptions(c.meta)
		if err != nil {
			return nil, err
		}
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		plannerOptions = collectionOptionsWithBufferedTemplateV1Resolver(plannerOptions, c.writeDomain, c.meta.Name)
		closeOnErr = false
		return &StoredDocumentJSONMaterializer{
			documentFormat:   documentFormat,
			templateResolver: plannerOptions.templateResolver,
			closeFn:          snap.Close,
		}, nil
	default:
		return nil, fmt.Errorf("collections: unsupported document format %q", c.meta.Options.DocumentFormat)
	}
}

// StoredDocumentJSON materializes one stored collection document as JSON bytes.
// JSON-format collections return an owned copy of document. BSON-format
// collections return canonical Extended JSON. Template-v1 collections resolve
// the document's template from the collection template root and any buffered
// template runs.
func (c *Collection) StoredDocumentJSON(document []byte) ([]byte, error) {
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()
	return materializer.StoredDocumentJSON(document)
}

// GetInto appends the document for documentID into dst[:0].
//
// The returned slice is owned by the caller. Missing documents return
// (dst[:0], false, nil).
func (c *Collection) GetInto(documentID []byte, dst []byte) ([]byte, bool, error) {
	if c == nil {
		return dst[:0], false, errCollectionNil
	}
	if c.db == nil {
		return dst[:0], false, errCollectionDBNil
	}
	if len(documentID) == 0 {
		return dst[:0], false, errors.New("collections: document id cannot be empty")
	}
	if value, buffered, found := c.getBufferedDocumentInto(documentID, dst); buffered {
		return value, found, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return dst[:0], false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return dst[:0], false, err
	}
	if catalog == nil {
		return dst[:0], false, errCollectionNotFound
	}
	return collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(c.meta.Name), documentID, dst)
}

func (c *Collection) getBufferedDocumentInto(documentID []byte, dst []byte) ([]byte, bool, bool) {
	if c == nil || c.writeDomain == nil {
		return nil, false, false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	if domain.count == 0 {
		domain.mu.RUnlock()
		return nil, false, false
	}
	if domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, c.meta.Name) {
		domain.mu.RUnlock()
		return c.getBufferedDocumentIntoWithPrimaryRunIndex(documentID, dst)
	}
	value, buffered, found := c.getBufferedDocumentIntoLocked(domain, documentID, dst)
	domain.mu.RUnlock()
	return value, buffered, found
}

func (c *Collection) getBufferedDocumentIntoWithPrimaryRunIndex(documentID []byte, dst []byte) ([]byte, bool, bool) {
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if domain.count == 0 {
		return nil, false, false
	}
	if domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, c.meta.Name) {
		collectionName := bufferedDomainCollectionName(domain, c.meta.Name)
		index, err := rebuildBufferedPrimaryRunIndex(collectionName, pendingIndexedRootRunMapLocked(domain))
		if err == nil {
			if index == nil {
				index = newBufferedPrimaryRunIndex(0)
			}
			domain.primaryRunIndex = index
		}
	}
	return c.getBufferedDocumentIntoLocked(domain, documentID, dst)
}

func bufferedDomainCollectionName(domain *collectionWriteDomain, fallback string) string {
	if domain != nil && domain.meta.Name != "" {
		return domain.meta.Name
	}
	return fallback
}

func hasBufferedPrimaryRootRuns(domain *collectionWriteDomain, fallbackCollectionName string) bool {
	if domain == nil || !hasBufferedIndexedRootRuns(domain) {
		return false
	}
	collectionName := bufferedDomainCollectionName(domain, fallbackCollectionName)
	if collectionName == "" {
		return false
	}
	return hasPendingIndexedRootRunsForRootLocked(domain, collectionPrimaryRootName(collectionName))
}

func (c *Collection) getBufferedDocumentIntoLocked(domain *collectionWriteDomain, documentID []byte, dst []byte) ([]byte, bool, bool) {
	table := domain.table
	if hasBufferedIndexedRootRuns(domain) {
		name := collectionPrimaryRootName(c.meta.Name)
		if domain.meta.Name != "" {
			name = collectionPrimaryRootName(domain.meta.Name)
		}
		var value []byte
		var flags byte
		found := false
		if table, ok := domain.primaryRunIndex.lookup(documentID); ok {
			value, _, flags, found = table.GetEntry(documentID)
		} else if domain.primaryRunIndex == nil {
			value, _, flags, found = getBufferedRunEntry(pendingIndexedRootRunsLocked(domain, name), documentID)
		}
		if found {
			if flags&node.FlagTombstone != 0 {
				return dst[:0], true, false
			}
			return append(dst[:0], value...), true, true
		}
	}
	if table == nil {
		return nil, false, false
	}
	value, _, flags, found := table.GetEntry(documentID)
	if !found {
		return nil, false, false
	}
	if flags&node.FlagTombstone != 0 {
		return dst[:0], true, false
	}
	return append(dst[:0], value...), true, true
}

func (c *Collection) FindByIndex(indexName, value string) ([][]byte, error) {
	return c.FindByIndexValue(indexName, value)
}

type IndexRangeBound struct {
	Value     any
	Inclusive bool
	Unbounded bool
}

type IndexRangeOptions struct {
	Lower IndexRangeBound
	Upper IndexRangeBound
	Limit int
	Desc  bool
}

// FindByIndexValue returns document IDs whose named secondary index equals
// value. Query values must match the index value type. If indexName does not
// exist, it returns nil, nil.
func (c *Collection) FindByIndexValue(indexName string, value any) ([][]byte, error) {
	out, _, err := c.findByIndexValue(indexName, value, 0)
	return out, err
}

// FindByIndexValueLimit is like FindByIndexValue but stops after maxResults
// document IDs and reports whether additional matches were present. If
// indexName does not exist, it returns nil, false, nil.
func (c *Collection) FindByIndexValueLimit(indexName string, value any, maxResults int) ([][]byte, bool, error) {
	if maxResults <= 0 {
		return nil, false, errors.New("collections: max index results must be positive")
	}
	return c.findByIndexValue(indexName, value, maxResults)
}

func (c *Collection) FindByIndexRange(indexName string, opts IndexRangeOptions) ([][]byte, bool, error) {
	if opts.Limit < 0 {
		return nil, false, errors.New("collections: index range limit cannot be negative")
	}
	capHint := 16
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	var out [][]byte
	truncated, found, err := c.scanIndexRange(indexName, opts, func(id []byte) (bool, error) {
		if out == nil {
			out = make([][]byte, 0, capHint)
		}
		out = append(out, id)
		return true, nil
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if out == nil {
		out = make([][]byte, 0)
	}
	return out, truncated, nil
}

func (c *Collection) ScanIndexRange(indexName string, opts IndexRangeOptions, fn func(id []byte) (bool, error)) (bool, error) {
	if fn == nil {
		return false, errors.New("collections: nil index range callback")
	}
	truncated, _, err := c.scanIndexRange(indexName, opts, fn)
	return truncated, err
}

func (c *Collection) findByIndexValue(indexName string, value any, maxResults int) ([][]byte, bool, error) {
	if maxResults < 0 {
		return nil, false, errors.New("collections: max index results cannot be negative")
	}
	return c.FindByIndexRange(indexName, IndexRangeOptions{
		Lower: IndexRangeBound{Value: value, Inclusive: true},
		Upper: IndexRangeBound{Value: value, Inclusive: true},
		Limit: maxResults,
	})
}

func (c *Collection) scanIndexRange(indexName string, opts IndexRangeOptions, fn func(id []byte) (bool, error)) (bool, bool, error) {
	if c == nil {
		return false, false, errCollectionNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return false, false, err
	}
	if opts.Limit < 0 {
		return false, false, errors.New("collections: index range limit cannot be negative")
	}
	if opts.Desc {
		return false, false, errors.New("collections: descending index range scans are not supported")
	}
	if err := c.flushBufferedNoIndex(); err != nil {
		return false, false, err
	}
	domain := c.writeDomain
	domainLocked := false
	if domain != nil {
		domain.mu.RLock()
		domainLocked = true
		defer func() {
			if domainLocked {
				domain.mu.RUnlock()
			}
		}()
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshotWithWriteDomainLocked(snap)
	if err != nil {
		return false, false, err
	}
	if catalog == nil {
		return false, false, errCollectionNotFound
	}
	idx, ok := findIndex(catalog.meta.Indexes, indexName)
	if !ok {
		return false, false, nil
	}
	start, end, empty, err := indexRangeScanBounds(idx.ValueType, opts)
	if err != nil {
		return false, true, err
	}
	if empty {
		return false, true, nil
	}
	exactPrefix, exactPrefixScan, err := exactIndexRangePrefix(idx.ValueType, opts)
	if err != nil {
		return false, true, err
	}
	var bufferedTable memtable.Table
	if exactPrefixScan {
		bufferedTable, err = bufferedIndexPrefixTableLocked(domain, catalog.meta.Name, indexName, idx.Unique, exactPrefix, opts.Limit)
	} else {
		bufferedTable, err = bufferedIndexRangeTableLocked(domain, catalog.meta.Name, indexName, start, end)
	}
	if err != nil {
		return false, true, err
	}
	if domainLocked {
		domain.mu.RUnlock()
		domainLocked = false
	}
	if bufferedTable != nil {
		defer resetCollectionRunTable(bufferedTable)
	}
	var bufferedIt iterator.UnsafeIterator
	if bufferedTable != nil {
		bufferedIt = bufferedTable.NewIterator(start, end)
		defer func() { _ = bufferedIt.Close() }()
	}
	var persistedIt iterator.UnsafeIterator
	persistedIt, err = collectionIteratorAtCatalogRoot(snap, catalog, collectionSecondaryRootName(catalog.meta.Name, idx.Name), start, end, true)
	if err != nil {
		return false, true, err
	}
	if persistedIt != nil {
		defer func() { _ = persistedIt.Close() }()
	}
	truncated, err := scanMergedCollectionIndexIDs(bufferedIt, persistedIt, idx.ValueType, opts.Limit, fn)
	return truncated, true, err
}

func exactIndexRangePrefix(valueType IndexValueType, opts IndexRangeOptions) ([]byte, bool, error) {
	if opts.Lower.Unbounded || opts.Upper.Unbounded || !opts.Lower.Inclusive || !opts.Upper.Inclusive {
		return nil, false, nil
	}
	lower, err := encodeIndexScalar(valueType, opts.Lower.Value)
	if err != nil {
		return nil, false, err
	}
	upper, err := encodeIndexScalar(valueType, opts.Upper.Value)
	if err != nil {
		return nil, false, err
	}
	if !bytes.Equal(lower, upper) {
		return nil, false, nil
	}
	return lower, true, nil
}

// bufferedIndexTableLocked materializes buffered entries for one secondary
// index prefix while domain.mu is held. The returned pooled table is owned by
// the caller and must be released with resetCollectionRunTable.
func bufferedIndexTableLocked(domain *collectionWriteDomain, collectionName, indexName string, unique bool, prefix []byte, maxResults int) (memtable.Table, error) {
	return bufferedIndexPrefixTableLocked(domain, collectionName, indexName, unique, prefix, maxResults)
}

func bufferedIndexPrefixTableLocked(domain *collectionWriteDomain, collectionName, indexName string, unique bool, prefix []byte, maxResults int) (memtable.Table, error) {
	if domain == nil {
		return nil, nil
	}
	if domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return nil, nil
	}
	if domain.meta.Name != "" {
		collectionName = domain.meta.Name
	}
	runs := pendingIndexedRootRunsLocked(domain, collectionSecondaryRootName(collectionName, indexName))
	if len(runs) == 0 {
		return nil, nil
	}
	if unique {
		pending := pendingUniqueReservationIndexLocked(domain, indexName, false)
		if pending != nil && !pending.contains(prefix) {
			return nil, nil
		}
	}
	table := newCollectionRunTable(0)
	liveLimit := collectionLimitedResultSentinel(maxResults)
	liveCount := 0
	it := newBufferedRootRunsIteratorWithDeleted(runs, prefix, prefixEnd(prefix), true)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if it.IsDeleted() {
			table.DeleteSteal(bytes.Clone(key))
		} else {
			setCollectionRunValue(table, bytes.Clone(key), nil)
			liveCount++
			if liveLimit > 0 && liveCount >= liveLimit {
				break
			}
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	if table.Len() == 0 {
		resetCollectionRunTable(table)
		return nil, nil
	}
	table.Freeze()
	return table, nil
}

// bufferedIndexRangeTableLocked materializes all buffered entries in a secondary
// index range while domain.mu is held. It intentionally does not apply a result
// limit because buffered tombstones outside the first live matches can suppress
// persisted entries later in the same range. The source run tables are pooled
// and may be reset by a concurrent flush as soon as domain.mu is released, so
// callers must materialize an owned table before merging with the persisted
// snapshot iterator.
func bufferedIndexRangeTableLocked(domain *collectionWriteDomain, collectionName, indexName string, start, end []byte) (memtable.Table, error) {
	if domain == nil {
		return nil, nil
	}
	if domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
		return nil, nil
	}
	if domain.meta.Name != "" {
		collectionName = domain.meta.Name
	}
	runs := pendingIndexedRootRunsLocked(domain, collectionSecondaryRootName(collectionName, indexName))
	if len(runs) == 0 {
		return nil, nil
	}
	table := newCollectionRunTable(0)
	it := newBufferedRootRunsIteratorWithDeleted(runs, start, end, true)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := bytes.Clone(it.UnsafeKey())
		if it.IsDeleted() {
			table.DeleteSteal(key)
		} else {
			setCollectionRunValue(table, key, nil)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	if table.Len() == 0 {
		resetCollectionRunTable(table)
		return nil, nil
	}
	table.Freeze()
	return table, nil
}

func indexRangeScanBounds(valueType IndexValueType, opts IndexRangeOptions) ([]byte, []byte, bool, error) {
	var start []byte
	var end []byte
	var err error
	var lowerEncoded []byte
	var upperEncoded []byte
	lowerBounded := !opts.Lower.Unbounded
	upperBounded := !opts.Upper.Unbounded
	if lowerBounded {
		lowerEncoded, err = encodeIndexScalar(valueType, opts.Lower.Value)
		if err != nil {
			return nil, nil, false, err
		}
	}
	if upperBounded {
		upperEncoded, err = encodeIndexScalar(valueType, opts.Upper.Value)
		if err != nil {
			return nil, nil, false, err
		}
	}

	if valueType == IndexValueDouble {
		upperNaN := upperBounded && encodedDoubleComponentIsNaN(upperEncoded)
		switch {
		case upperNaN && !opts.Upper.Inclusive:
			return nil, nil, true, nil
		}
	}

	if lowerBounded {
		if opts.Lower.Inclusive {
			start = bytes.Clone(lowerEncoded)
		} else {
			start = prefixEnd(lowerEncoded)
			if start == nil {
				return nil, nil, true, nil
			}
		}
	}
	if upperBounded {
		if opts.Upper.Inclusive {
			end = prefixEnd(upperEncoded)
		} else {
			end = bytes.Clone(upperEncoded)
		}
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil, true, nil
	}
	return start, end, false, nil
}

func encodedDoubleComponentIsNaN(encoded []byte) bool {
	return len(encoded) == 1 && encoded[0] == 0x00
}

func scanMergedCollectionIndexIDs(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, fn func([]byte) (bool, error)) (bool, error) {
	if maxResults < 0 {
		return false, errors.New("collections: max index results cannot be negative")
	}
	seen := make(map[string]struct{})
	emitted := 0
	emit := func(key []byte) (bool, bool, error) {
		id, err := indexKeyDocumentID(valueType, key)
		if err != nil {
			return false, false, err
		}
		idKey := string(id)
		if _, ok := seen[idKey]; ok {
			return true, false, nil
		}
		seen[idKey] = struct{}{}
		if maxResults > 0 && emitted >= maxResults {
			return false, true, nil
		}
		cont, err := fn(bytes.Clone(id))
		if err != nil {
			return false, false, err
		}
		emitted++
		return cont, false, nil
	}
	for {
		bufferedKey, bufferedOK := collectionIndexIteratorKey(bufferedIt)
		persistedKey, persistedOK := collectionIndexIteratorKey(persistedIt)
		if !bufferedOK && !persistedOK {
			break
		}
		switch {
		case !bufferedOK:
			if !persistedIt.IsDeleted() {
				cont, truncated, err := emit(persistedKey)
				if err != nil || truncated || !cont {
					return truncated, err
				}
			}
			persistedIt.Next()
		case !persistedOK:
			if !bufferedIt.IsDeleted() {
				cont, truncated, err := emit(bufferedKey)
				if err != nil || truncated || !cont {
					return truncated, err
				}
			}
			bufferedIt.Next()
		default:
			cmp := bytes.Compare(bufferedKey, persistedKey)
			if cmp < 0 {
				if !bufferedIt.IsDeleted() {
					cont, truncated, err := emit(bufferedKey)
					if err != nil || truncated || !cont {
						return truncated, err
					}
				}
				bufferedIt.Next()
			} else if cmp > 0 {
				if !persistedIt.IsDeleted() {
					cont, truncated, err := emit(persistedKey)
					if err != nil || truncated || !cont {
						return truncated, err
					}
				}
				persistedIt.Next()
			} else {
				if !bufferedIt.IsDeleted() {
					cont, truncated, err := emit(bufferedKey)
					if err != nil || truncated || !cont {
						return truncated, err
					}
				}
				bufferedIt.Next()
				persistedIt.Next()
			}
		}
	}
	if err := collectionIndexIteratorError(bufferedIt); err != nil {
		return false, err
	}
	if err := collectionIndexIteratorError(persistedIt); err != nil {
		return false, err
	}
	return false, nil
}

func collectMergedCollectionIndexIDs(bufferedIt, persistedIt iterator.UnsafeIterator, prefix []byte, maxResults int) ([][]byte, bool, error) {
	limit := collectionLimitedResultSentinel(maxResults)
	capHint := 16
	if limit > 0 {
		capHint = limit
		const maxInitialCap = 1024
		if capHint > maxInitialCap {
			capHint = maxInitialCap
		}
	}
	out := make([][]byte, 0, capHint)
	appendID := func(id []byte) {
		if len(out) > 0 && bytes.Equal(out[len(out)-1], id) {
			return
		}
		out = append(out, bytes.Clone(id))
	}
	for limit == 0 || len(out) < limit {
		bufferedID, bufferedOK := collectionIndexIteratorID(bufferedIt, prefix)
		persistedID, persistedOK := collectionIndexIteratorID(persistedIt, prefix)
		if !bufferedOK && !persistedOK {
			break
		}
		switch {
		case !bufferedOK:
			if !persistedIt.IsDeleted() {
				appendID(persistedID)
			}
			persistedIt.Next()
		case !persistedOK:
			if !bufferedIt.IsDeleted() {
				appendID(bufferedID)
			}
			bufferedIt.Next()
		default:
			cmp := bytes.Compare(bufferedID, persistedID)
			if cmp < 0 {
				if !bufferedIt.IsDeleted() {
					appendID(bufferedID)
				}
				bufferedIt.Next()
			} else if cmp > 0 {
				if !persistedIt.IsDeleted() {
					appendID(persistedID)
				}
				persistedIt.Next()
			} else {
				if !bufferedIt.IsDeleted() {
					appendID(bufferedID)
				}
				bufferedIt.Next()
				persistedIt.Next()
			}
		}
	}
	if err := collectionIndexIteratorError(bufferedIt); err != nil {
		return nil, false, err
	}
	if err := collectionIndexIteratorError(persistedIt); err != nil {
		return nil, false, err
	}
	truncated := false
	if maxResults > 0 && len(out) > maxResults {
		out = out[:maxResults]
		truncated = true
	}
	return out, truncated, nil
}

func collectionLimitedResultSentinel(maxResults int) int {
	if maxResults <= 0 || maxResults >= maxCollectionInt {
		return 0
	}
	return maxResults + 1
}

func collectionIndexIteratorID(it iterator.UnsafeIterator, prefix []byte) ([]byte, bool) {
	if it == nil || !it.Valid() {
		return nil, false
	}
	key := it.UnsafeKey()
	if !bytes.HasPrefix(key, prefix) {
		return nil, false
	}
	return key[len(prefix):], true
}

func collectionIndexIteratorKey(it iterator.UnsafeIterator) ([]byte, bool) {
	if it == nil || !it.Valid() {
		return nil, false
	}
	return it.UnsafeKey(), true
}

func collectionIndexIteratorError(it iterator.UnsafeIterator) error {
	if it == nil {
		return nil
	}
	return it.Error()
}

// ScanDocuments flushes buffered writes before acquiring a snapshot, then scans
// the collection primary root up to maxDocuments. The returned boolean is true
// when additional documents were present beyond the limit.
func (c *Collection) ScanDocuments(maxDocuments int) ([]DocumentRecord, bool, error) {
	out := make([]DocumentRecord, 0)
	truncated, err := c.ScanDocumentsFunc(maxDocuments, func(record DocumentRecord) (bool, error) {
		out = append(out, record)
		return true, nil
	})
	if err != nil {
		return nil, false, err
	}
	return out, truncated, nil
}

// ScanDocumentsFunc flushes buffered writes before acquiring a snapshot, then
// calls fn for primary collection records until maxDocuments is reached, the
// collection is exhausted, or fn returns false. The returned boolean is true
// only when additional documents were present beyond the maxDocuments limit.
func (c *Collection) ScanDocumentsFunc(maxDocuments int, fn func(DocumentRecord) (bool, error)) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if maxDocuments <= 0 {
		return false, errors.New("collections: max documents must be positive")
	}
	if fn == nil {
		return false, errors.New("collections: scan callback is nil")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return false, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return false, err
	}
	if catalog == nil {
		return false, errCollectionNotFound
	}
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return false, err
	}
	if it == nil {
		return false, nil
	}
	defer func() { _ = it.Close() }()
	truncated := false
	scanned := 0
	for it.Valid() {
		if it.IsDeleted() {
			it.Next()
			continue
		}
		if scanned >= maxDocuments {
			truncated = true
			break
		}
		record := DocumentRecord{
			ID:       bytes.Clone(it.UnsafeKey()),
			Document: it.ValueCopy(nil),
		}
		scanned++
		next, err := fn(record)
		if err != nil {
			return false, err
		}
		if !next {
			return false, nil
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	return truncated, nil
}

func loadCollectionCatalog(snap *backenddb.Snapshot, name string) (*collectionCatalog, error) {
	raw, ok, err := getSystemValue(snap, systemCollectionMetaKey(name))
	if err != nil || !ok {
		return nil, err
	}
	meta, err := decodeCollectionMeta(raw)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]uint64)
	rootNames := collectionRootNames(meta)
	for _, rootName := range rootNames {
		rawRoot, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		rootID, err := decodeRootID(rawRoot)
		if err != nil {
			return nil, fmt.Errorf("collections: root %q: %w", rootName, err)
		}
		roots[rootName] = rootID
	}
	rootOverlays, err := loadCollectionCatalogRootOverlays(snap, rootNames)
	if err != nil {
		return nil, err
	}
	return newCollectionCatalogWithOverlays(meta, roots, rootOverlays), nil
}

func loadCollectionCatalogRootOverlays(snap *backenddb.Snapshot, rootNames []string) (map[string][]uint64, error) {
	if snap == nil || snap.State() == nil || snap.State().SystemRootPageID == 0 || len(rootNames) == 0 {
		return nil, nil
	}
	prefix := []byte(systemCollectionRootOverlayPrefix)
	it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, prefix, prefixEnd(prefix))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	if !it.Valid() || !bytes.HasPrefix(it.UnsafeKey(), prefix) {
		return nil, it.Error()
	}
	rootNameSet := make(map[string]struct{}, len(rootNames))
	for _, rootName := range rootNames {
		rootNameSet[rootName] = struct{}{}
	}
	rootOverlays := make(map[string][]uint64)
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		rootName := strings.TrimPrefix(string(key), systemCollectionRootOverlayPrefix)
		if _, ok := rootNameSet[rootName]; !ok {
			it.Next()
			continue
		}
		overlayIDs, err := decodeRootIDList(it.ValueCopy(nil))
		if err != nil {
			return nil, fmt.Errorf("collections: root %q overlays: %w", rootName, err)
		}
		if len(overlayIDs) != 0 {
			rootOverlays[rootName] = overlayIDs
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(rootOverlays) == 0 {
		return nil, nil
	}
	return rootOverlays, nil
}

func (c *collectionCatalog) rootID(rootName string) uint64 {
	if c == nil || c.roots == nil {
		return 0
	}
	return c.roots[rootName]
}

func (c *collectionCatalog) overlayRootNames() []string {
	if c == nil || len(c.rootOverlays) == 0 {
		return nil
	}
	names := make([]string, 0, len(c.rootOverlays))
	for rootName, overlays := range c.rootOverlays {
		if len(overlays) != 0 {
			names = append(names, rootName)
		}
	}
	sort.Strings(names)
	return names
}

func (c *collectionCatalog) overlayRootIDs(rootName string) []uint64 {
	if c == nil || len(c.rootOverlays) == 0 {
		return nil
	}
	return c.rootOverlays[rootName]
}

func (c *collectionCatalog) overlayRootMayContainKey(rootName string, rootID uint64, key []byte) bool {
	if c == nil || rootID == 0 || len(c.rootOverlayFilters) == 0 {
		return true
	}
	byRoot := c.rootOverlayFilters[rootName]
	if byRoot == nil {
		return true
	}
	filter, ok := byRoot[rootID]
	if !ok {
		return true
	}
	return filter.mayContainKey(key)
}

func (c *collectionCatalog) anyOverlayRootMayContainKey(rootName string, key []byte) bool {
	if c == nil {
		return false
	}
	for _, rootID := range c.overlayRootIDs(rootName) {
		if c.overlayRootMayContainKey(rootName, rootID, key) {
			return true
		}
	}
	return false
}

func collectionRootStoragePolicy(meta CollectionMeta, rootName string) (backenddb.OrderedRootStoragePolicy, error) {
	switch rootName {
	case collectionPrimaryRootName(meta.Name), collectionTemplateRootName(meta.Name):
		return backendRootStoragePolicy(meta.Options.DataRootStoragePolicy)
	case collectionIndexStateRootName(meta.Name):
		return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
	}
	for _, idx := range meta.Indexes {
		if rootName == collectionSecondaryRootName(meta.Name, idx.Name) {
			return backendRootStoragePolicy(idx.StoragePolicy)
		}
	}
	return backenddb.OrderedRootStorageDefault, fmt.Errorf("collections: unknown collection root %q for %q", rootName, meta.Name)
}

func (c *collectionCatalog) rootStack(rootName string) []uint64 {
	if c == nil || rootName == "" {
		return nil
	}
	overlays := c.overlayRootIDs(rootName)
	baseRoot := c.rootID(rootName)
	if len(overlays) == 0 {
		if baseRoot == 0 {
			return nil
		}
		return []uint64{baseRoot}
	}
	// Overlay descriptors are ordered newest-to-oldest so the merge iterator can
	// let newer overlay entries shadow older overlays and the base root.
	out := make([]uint64, 0, len(overlays)+1)
	out = append(out, overlays...)
	if baseRoot != 0 {
		out = append(out, baseRoot)
	}
	return out
}

func collectionGetEntryAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key []byte) (node.LeafEntry, uint64, error) {
	if snap == nil {
		return node.LeafEntry{}, 0, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return node.LeafEntry{}, 0, tree.ErrKeyNotFound
		}
		entry, err := snap.GetEntryAtRoot(rootID, key)
		return entry, rootID, err
	}
	useOverlayFilters := catalog.rootID(rootName) != 0
	for _, rootID := range catalog.rootStack(rootName) {
		if useOverlayFilters && !catalog.overlayRootMayContainKey(rootName, rootID, key) {
			continue
		}
		entry, err := snap.GetEntryAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return node.LeafEntry{}, 0, err
		}
		return entry, rootID, nil
	}
	return node.LeafEntry{}, 0, tree.ErrKeyNotFound
}

func collectionGetAppendAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key, dst []byte) ([]byte, bool, error) {
	if snap == nil {
		return dst[:0], false, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return dst[:0], false, nil
		}
		out, err := snap.GetAppendAtRoot(rootID, key, dst[:0])
		if errors.Is(err, tree.ErrKeyNotFound) {
			return dst[:0], false, nil
		}
		if err != nil {
			return dst[:0], false, err
		}
		return out, true, nil
	}
	entry, rootID, err := collectionGetEntryAtCatalogRoot(snap, catalog, rootName, key)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return dst[:0], false, nil
	}
	out, err := snap.GetAppendAtRoot(rootID, key, dst[:0])
	if errors.Is(err, tree.ErrKeyNotFound) {
		return dst[:0], false, nil
	}
	if err != nil {
		return dst[:0], false, err
	}
	return out, true, nil
}

func collectionGetAppendAtCatalogOverlayRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key, dst []byte) ([]byte, bool, bool, error) {
	if snap == nil {
		return dst[:0], false, false, backenddb.ErrClosed
	}
	useOverlayFilters := catalog.rootID(rootName) != 0
	for _, rootID := range catalog.overlayRootIDs(rootName) {
		if rootID == 0 {
			continue
		}
		if useOverlayFilters && !catalog.overlayRootMayContainKey(rootName, rootID, key) {
			continue
		}
		entry, err := snap.GetEntryAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return dst[:0], false, false, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return dst[:0], true, false, nil
		}
		if entry.Flags&node.FlagPointer == 0 {
			return append(dst[:0], entry.Value...), true, true, nil
		}
		out, err := snap.GetAppendAtRoot(rootID, key, dst[:0])
		if errors.Is(err, tree.ErrKeyNotFound) {
			return dst[:0], false, false, nil
		}
		if err != nil {
			return dst[:0], false, false, err
		}
		return out, true, true, nil
	}
	return dst[:0], false, false, nil
}

func collectionIteratorAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool) (iterator.UnsafeIterator, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return nil, nil
		}
		it, err := snap.IteratorAtRootWithOptions(rootID, start, end, backenddb.IteratorOptions{IncludeTombstones: includeDeleted})
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, nil
		}
		return it, err
	}
	rootIDs := catalog.rootStack(rootName)
	if len(rootIDs) == 0 {
		return nil, nil
	}
	sources := make([]bufferedRootRunIteratorSource, 0, len(rootIDs))
	for i, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		it, err := snap.IteratorAtRootWithOptions(rootID, start, end, backenddb.IteratorOptions{IncludeTombstones: true})
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			for _, source := range sources {
				if source.iter != nil {
					_ = source.iter.Close()
				}
			}
			return nil, err
		}
		sources = append(sources, bufferedRootRunIteratorSource{
			iter:     it,
			priority: i,
		})
	}
	if len(sources) == 0 {
		return nil, nil
	}
	return newBufferedRootRunIteratorSourcesIteratorWithDeleted(sources, start, end, includeDeleted, false), nil
}

func (c *collectionCatalog) copy() *collectionCatalog {
	if c == nil {
		return nil
	}
	roots := make(map[string]uint64, len(c.roots))
	for name, rootID := range c.roots {
		roots[name] = rootID
	}
	rootOverlays := cloneRootOverlayMap(c.rootOverlays)
	rootOverlayFilters := cloneRootOverlayFilterMap(c.rootOverlayFilters)
	return newCollectionCatalogWithOverlayMetadataOwned(copyCollectionMeta(c.meta), roots, rootOverlays, rootOverlayFilters)
}

func getSystemValue(snap *backenddb.Snapshot, key string) ([]byte, bool, error) {
	if snap == nil || snap.State() == nil || snap.State().SystemRootPageID == 0 {
		return nil, false, nil
	}
	entry, err := snap.GetEntryAtRoot(snap.State().SystemRootPageID, []byte(key))
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return bytes.Clone(entry.Value), true, nil
}

type systemTargetEntry struct {
	key   []byte
	value []byte
}

type systemTargetIterator struct {
	entries []systemTargetEntry
	idx     int
}

func (it *systemTargetIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.entries)
}

func (it *systemTargetIterator) Next() {
	if it != nil && it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *systemTargetIterator) Seek(key []byte) {
	if it == nil {
		return
	}
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) >= 0
	})
}

func (it *systemTargetIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].key
}

func (it *systemTargetIterator) UnsafeValue() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *systemTargetIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline
	}
	return it.entries[it.idx].value, page.ValuePtr{}, node.FlagInline
}

func (it *systemTargetIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *systemTargetIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *systemTargetIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].key...)
}

func (it *systemTargetIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *systemTargetIterator) IsDeleted() bool {
	return false
}

func (it *systemTargetIterator) Error() error {
	return nil
}

func (it *systemTargetIterator) Close() error {
	return nil
}

func (it *systemTargetIterator) Domain() (start, end []byte) {
	return nil, nil
}

func (it *systemTargetIterator) StableUnsafeIteratorSlices() bool {
	return true
}

func (it *systemTargetIterator) Len() int {
	if it == nil {
		return 0
	}
	return len(it.entries)
}

func buildSystemTargetIterator(snap *backenddb.Snapshot, updates map[string][]byte) (iterator.UnsafeIterator, error) {
	updateEntries := make([]systemTargetEntry, 0, len(updates))
	for key, value := range updates {
		updateEntries = append(updateEntries, systemTargetEntry{
			key:   []byte(key),
			value: bytes.Clone(value),
		})
	}
	sort.Slice(updateEntries, func(i, j int) bool {
		return bytes.Compare(updateEntries[i].key, updateEntries[j].key) < 0
	})

	entries := make([]systemTargetEntry, 0, len(updateEntries))
	updateIdx := 0
	if snap != nil && snap.State() != nil && snap.State().SystemRootPageID != 0 {
		it, err := snap.IteratorAtRoot(snap.State().SystemRootPageID, nil, nil)
		if err != nil && !errors.Is(err, tree.ErrKeyNotFound) {
			return nil, err
		}
		if err == nil {
			defer func() { _ = it.Close() }()
			for it.Valid() {
				if it.IsDeleted() {
					it.Next()
					continue
				}
				currKey := it.UnsafeKey()
				for updateIdx < len(updateEntries) && bytes.Compare(updateEntries[updateIdx].key, currKey) < 0 {
					entries = append(entries, updateEntries[updateIdx])
					updateIdx++
				}
				if updateIdx < len(updateEntries) && bytes.Equal(updateEntries[updateIdx].key, currKey) {
					entries = append(entries, updateEntries[updateIdx])
					updateIdx++
				} else {
					entries = append(entries, systemTargetEntry{
						key:   bytes.Clone(currKey),
						value: it.ValueCopy(nil),
					})
				}
				it.Next()
			}
			iterErr := it.Error()
			if iterErr != nil {
				return nil, iterErr
			}
		}
	}
	for updateIdx < len(updateEntries) {
		entries = append(entries, updateEntries[updateIdx])
		updateIdx++
	}
	return &systemTargetIterator{entries: entries}, nil
}

func buildSystemDeltaIterator(updates map[string][]byte) (iterator.UnsafeIterator, error) {
	entries := make([]systemTargetEntry, 0, len(updates))
	for key, value := range updates {
		entries = append(entries, systemTargetEntry{
			key:   []byte(key),
			value: bytes.Clone(value),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &systemTargetIterator{entries: entries}, nil
}

func encodeCollectionMeta(meta CollectionMeta) ([]byte, error) {
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	return json.Marshal(collectionMetaDisk{
		Version: collectionMetaVersion,
		Name:    normalized.Name,
		Options: normalized.Options,
		Indexes: normalized.Indexes,
	})
}

func decodeCollectionMeta(raw []byte) (CollectionMeta, error) {
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		return CollectionMeta{}, err
	}
	if disk.Version != collectionMetaVersion {
		return CollectionMeta{}, fmt.Errorf("collections: unsupported collection metadata version %d", disk.Version)
	}
	return normalizeCollectionMeta(CollectionMeta{
		Name:    disk.Name,
		Options: disk.Options,
		Indexes: disk.Indexes,
	})
}

func normalizeCollectionMeta(meta CollectionMeta) (CollectionMeta, error) {
	if err := ValidateCollectionName(meta.Name); err != nil {
		return CollectionMeta{}, err
	}
	if _, err := backendRootStoragePolicy(meta.Options.DataRootStoragePolicy); err != nil {
		return CollectionMeta{}, err
	}
	if _, err := backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy); err != nil {
		return CollectionMeta{}, err
	}
	if meta.Options.BufferedIndexedWriteMaxDocuments < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed write max documents cannot be negative")
	}
	if meta.Options.BufferedIndexedWriteMaxBytes < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed write max bytes cannot be negative")
	}
	if meta.Options.BufferedIndexedWriteMaxRootRuns < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed write max root runs cannot be negative")
	}
	if meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits < 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed async flush max queued units cannot be negative")
	}
	documentFormat, err := normalizeDocumentFormat(meta.Options.DocumentFormat)
	if err != nil {
		return CollectionMeta{}, err
	}
	if documentFormat == DocumentFormatJSON {
		meta.Options.DocumentFormat = DocumentFormatDefault
	} else {
		meta.Options.DocumentFormat = documentFormat
	}
	indexes := append([]IndexDefinition(nil), meta.Indexes...)
	sort.SliceStable(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})
	seen := make(map[string]struct{}, len(indexes))
	for i := range indexes {
		if err := ValidateIndexName(indexes[i].Name); err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index name %q: %w", indexes[i].Name, err)
		}
		if err := ValidateIndexPath(indexes[i].Field); err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index %q field: %w", indexes[i].Name, err)
		}
		valueType, err := normalizeIndexValueType(indexes[i].ValueType)
		if err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index %q value_type: %w", indexes[i].Name, err)
		}
		indexes[i].ValueType = valueType
		if _, err := backendRootStoragePolicy(indexes[i].StoragePolicy); err != nil {
			return CollectionMeta{}, err
		}
		if _, ok := seen[indexes[i].Name]; ok {
			return CollectionMeta{}, fmt.Errorf("collections: duplicate index %q", indexes[i].Name)
		}
		seen[indexes[i].Name] = struct{}{}
	}
	meta.Indexes = indexes
	if meta.Options.DisableIndexedWriteMemtables {
		meta.Options.BufferedIndexedWrites = false
		meta.Options.BufferedIndexedWriteMaxDocuments = 0
		meta.Options.BufferedIndexedWriteMaxBytes = 0
		meta.Options.BufferedIndexedWriteMaxRootRuns = 0
		meta.Options.BufferedIndexedAsyncFlush = false
		meta.Options.BufferedIndexedOverlayRoots = false
		meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits = 0
	} else if len(meta.Indexes) == 0 {
		meta.Options.BufferedIndexedWrites = false
	} else {
		meta.Options.BufferedIndexedWrites = true
		defaultMaxDocuments := DefaultIndexedWriteMemtableMaxDocuments
		defaultMaxRootRuns := DefaultIndexedWriteMemtableMaxRootRuns
		if meta.Options.BufferedIndexedAsyncFlush {
			defaultMaxDocuments = DefaultIndexedWriteMemtableAsyncFlushMaxDocuments
			defaultMaxRootRuns = DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns
		}
		useNativeDocumentDefault := meta.Options.BufferedIndexedWriteMaxDocuments == 0
		if useNativeDocumentDefault {
			meta.Options.BufferedIndexedWriteMaxDocuments = defaultMaxDocuments
		}
		if useNativeDocumentDefault && meta.Options.BufferedIndexedWriteMaxBytes == 0 && meta.Options.BufferedIndexedWriteMaxRootRuns == 0 {
			meta.Options.BufferedIndexedWriteMaxRootRuns = defaultMaxRootRuns
		}
		if meta.Options.BufferedIndexedAsyncFlush && meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits == 0 {
			meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits = DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits
		}
	}
	return meta, nil
}

func normalizeIndexValueType(valueType IndexValueType) (IndexValueType, error) {
	switch valueType {
	case IndexValueString, IndexValueBool, IndexValueInt64, IndexValueDouble:
		return valueType, nil
	case "":
		return "", errors.New("value_type is required")
	default:
		return "", fmt.Errorf("unsupported value_type %q", valueType)
	}
}

func (m CollectionMeta) copy() *CollectionMeta {
	return &CollectionMeta{
		Name:    m.Name,
		Options: m.Options,
		Indexes: append([]IndexDefinition(nil), m.Indexes...),
	}
}

func copyCollectionMeta(meta CollectionMeta) CollectionMeta {
	if copied := meta.copy(); copied != nil {
		return *copied
	}
	return meta
}

func sameCollectionMeta(a, b CollectionMeta) bool {
	if collectionMetaValuesEqual(a, b) {
		return true
	}
	na, err := normalizeCollectionMeta(a)
	if err != nil {
		return false
	}
	nb, err := normalizeCollectionMeta(b)
	if err != nil {
		return false
	}
	return collectionMetaValuesEqual(na, nb)
}

func collectionMetaValuesEqual(a, b CollectionMeta) bool {
	if a.Name != b.Name || a.Options != b.Options || len(a.Indexes) != len(b.Indexes) {
		return false
	}
	for i := range a.Indexes {
		if a.Indexes[i] != b.Indexes[i] {
			return false
		}
	}
	return true
}

func collectionMetaHasSecondaryUniqueIndex(meta CollectionMeta) bool {
	for _, idx := range meta.Indexes {
		if idx.Unique {
			return true
		}
	}
	return false
}

func addIndexToCollectionMeta(meta CollectionMeta, def IndexDefinition) (CollectionMeta, IndexDefinition, error) {
	if _, ok := findIndex(meta.Indexes, def.Name); ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	candidate := CollectionMeta{
		Name:    meta.Name,
		Options: meta.Options,
		Indexes: append(append([]IndexDefinition(nil), meta.Indexes...), def),
	}
	normalized, err := normalizeCollectionMeta(candidate)
	if err != nil {
		return CollectionMeta{}, IndexDefinition{}, err
	}
	normalizedDef, ok := findIndex(normalized.Indexes, def.Name)
	if !ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: normalized index %q not found", def.Name)
	}
	return normalized, normalizedDef, nil
}

func singleIndexRuntime(def IndexDefinition) (indexRuntime, error) {
	runtimes, err := (insertBatchPlanner{
		indexes: plannerIndexes([]IndexDefinition{def}),
	}).indexRuntimes()
	if err != nil {
		return indexRuntime{}, err
	}
	if len(runtimes) != 1 {
		return indexRuntime{}, errors.New("collections: expected one index runtime")
	}
	return runtimes[0], nil
}

func plannerIndexes(indexes []IndexDefinition) []indexDefinition {
	out := make([]indexDefinition, len(indexes))
	for i, idx := range indexes {
		policy, _ := backendRootStoragePolicy(idx.StoragePolicy)
		out[i] = indexDefinition{
			name:          idx.Name,
			field:         idx.Field,
			valueType:     idx.ValueType,
			unique:        idx.Unique,
			multiKey:      idx.MultiKey,
			storagePolicy: policy,
		}
	}
	return out
}

func uniqueIndexRootIDs(catalog *collectionCatalog) map[string]uint64 {
	if catalog == nil {
		return nil
	}
	out := make(map[string]uint64)
	for _, idx := range catalog.meta.Indexes {
		if !idx.Unique {
			continue
		}
		rootID := catalog.rootID(collectionSecondaryRootName(catalog.meta.Name, idx.Name))
		if rootID != 0 {
			out[idx.Name] = rootID
		}
	}
	return out
}

func uniqueIndexNamesWithDataOrOverlays(catalog *collectionCatalog) map[string]struct{} {
	if catalog == nil {
		return nil
	}
	out := make(map[string]struct{})
	for _, idx := range catalog.meta.Indexes {
		if !idx.Unique {
			continue
		}
		rootName := collectionSecondaryRootName(catalog.meta.Name, idx.Name)
		rootID := catalog.rootID(rootName)
		if rootID != 0 || len(catalog.overlayRootIDs(rootName)) != 0 {
			out[idx.Name] = struct{}{}
		}
	}
	return out
}

func findIndex(indexes []IndexDefinition, name string) (IndexDefinition, bool) {
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return IndexDefinition{}, false
}

func collectionRootNames(meta CollectionMeta) []string {
	out := []string{collectionPrimaryRootName(meta.Name)}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatTemplateV1 {
		out = append(out, collectionTemplateRootName(meta.Name))
	}
	if len(meta.Indexes) > 0 && persistIndexStateForDocumentFormat(meta.Options.DocumentFormat) {
		out = append(out, collectionIndexStateRootName(meta.Name))
	}
	for _, idx := range meta.Indexes {
		out = append(out, collectionSecondaryRootName(meta.Name, idx.Name))
	}
	return out
}

func collectionPrimaryRootName(collection string) string {
	return collection + "/primary"
}

func collectionTemplateRootName(collection string) string {
	return collection + "/templates"
}

func collectionIndexStateRootName(collection string) string {
	return collection + "/index-state"
}

func collectionSecondaryRootName(collection, indexName string) string {
	return collection + "/index/" + indexName
}

func systemCollectionMetaKey(collection string) string {
	return systemCollectionMetaPrefix + collection
}

func systemCollectionRootKey(rootName string) string {
	return systemCollectionRootPrefix + rootName
}

func systemCollectionRootOverlayKey(rootName string) string {
	return systemCollectionRootOverlayPrefix + rootName
}

func encodeRootID(rootID uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, rootID)
	return out
}

func decodeRootID(raw []byte) (uint64, error) {
	if len(raw) != 8 {
		return 0, errors.New("malformed root id")
	}
	return binary.BigEndian.Uint64(raw), nil
}

func encodeRootIDList(rootIDs []uint64) []byte {
	if len(rootIDs) == 0 {
		return nil
	}
	out := make([]byte, len(rootIDs)*8)
	for i, rootID := range rootIDs {
		binary.BigEndian.PutUint64(out[i*8:(i+1)*8], rootID)
	}
	return out
}

func decodeRootIDList(raw []byte) ([]uint64, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw)%8 != 0 {
		return nil, errors.New("malformed root id list")
	}
	out := make([]uint64, len(raw)/8)
	for i := range out {
		out[i] = binary.BigEndian.Uint64(raw[i*8 : (i+1)*8])
	}
	return out, nil
}

func buildCollectionRootOverlayFilters(rootNames []string, rootRuns map[string][]memtable.Table, rootOverlays map[string][]uint64, existingFilters map[string]map[uint64]collectionRootOverlayFilter) (map[string]collectionRootOverlayFilter, error) {
	if len(rootNames) == 0 || len(rootRuns) == 0 {
		return nil, nil
	}
	filters := make(map[string]collectionRootOverlayFilter, len(rootNames))
	for _, rootName := range rootNames {
		// The current read bottleneck is the primary document root; secondary
		// overlay probes are left on the existing safe path until their cost is
		// proven separately.
		if !strings.HasSuffix(rootName, "/primary") {
			continue
		}
		baseRoot := overlayDeltaBaseRoot(rootOverlays[rootName])
		var baseFilter collectionRootOverlayFilter
		if baseRoot != 0 {
			var ok bool
			baseFilter, ok = existingFilters[rootName][baseRoot]
			if !ok {
				continue
			}
		}
		filter, err := buildCollectionRootOverlayFilter(rootRuns[rootName])
		if err != nil {
			return nil, err
		}
		if filter.empty() {
			continue
		}
		if baseRoot != 0 {
			filter = unionCollectionRootOverlayFilters(baseFilter, filter)
		}
		if !filter.empty() && filter.count <= maxCollectionRootOverlayFilterKeys {
			filters[rootName] = filter
		}
	}
	return filters, nil
}

func buildCollectionRootOverlayFilter(runs []memtable.Table) (collectionRootOverlayFilter, error) {
	if len(runs) == 0 {
		return collectionRootOverlayFilter{}, nil
	}
	it := newBufferedRootRunsIteratorWithDeleted(runs, nil, nil, true)
	defer func() { _ = it.Close() }()
	var filter collectionRootOverlayFilter
	for it.Valid() {
		filter.addKey(it.UnsafeKey())
		if filter.count > maxCollectionRootOverlayFilterKeys {
			return collectionRootOverlayFilter{}, it.Error()
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return collectionRootOverlayFilter{}, err
	}
	return filter, nil
}

func unionCollectionRootOverlayFilters(left, right collectionRootOverlayFilter) collectionRootOverlayFilter {
	if left.empty() {
		return right.clone()
	}
	if right.empty() {
		return left.clone()
	}
	out := left.clone()
	for i := range out.words {
		out.words[i] |= right.words[i]
	}
	out.count = saturatingAddUint32(out.count, right.count)
	return out
}

func (f collectionRootOverlayFilter) mayContainKey(key []byte) bool {
	if f.empty() {
		return false
	}
	hash := xxhash.Sum64(key)
	for i := uint64(0); i < 4; i++ {
		bit := collectionRootOverlayFilterBit(hash, i)
		if f.words[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func (f *collectionRootOverlayFilter) addKey(key []byte) {
	if f.words == nil {
		f.words = make([]uint64, collectionRootOverlayFilterWords)
	}
	hash := xxhash.Sum64(key)
	for i := uint64(0); i < 4; i++ {
		bit := collectionRootOverlayFilterBit(hash, i)
		f.words[bit>>6] |= uint64(1) << (bit & 63)
	}
	if f.count < ^uint32(0) {
		f.count++
	}
}

func (f collectionRootOverlayFilter) clone() collectionRootOverlayFilter {
	if len(f.words) == 0 {
		return collectionRootOverlayFilter{}
	}
	return collectionRootOverlayFilter{words: append([]uint64(nil), f.words...), count: f.count}
}

func (f collectionRootOverlayFilter) empty() bool {
	return len(f.words) == 0
}

func collectionRootOverlayFilterBit(hash, ordinal uint64) uint64 {
	mixed := hash + ordinal*0x9e3779b97f4a7c15
	mixed ^= mixed >> 33
	mixed *= 0xff51afd7ed558ccd
	mixed ^= mixed >> 33
	return mixed & (collectionRootOverlayFilterBits - 1)
}

func saturatingAddUint32(left, right uint32) uint32 {
	sum := uint64(left) + uint64(right)
	if sum > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(sum)
}

func ValidateCollectionName(name string) error {
	if len(name) == 0 {
		return errors.New("collection name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("collection name too long")
	}
	if strings.ContainsAny(name, "\x00/:") {
		return errors.New("collection name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("collection name has leading or trailing spaces")
	}
	if !utf8.ValidString(name) {
		return errors.New("collection name invalid utf-8")
	}
	return nil
}

func ValidateIndexName(name string) error {
	if len(name) == 0 {
		return errors.New("index name cannot be empty")
	}
	if len(name) > 128 {
		return errors.New("index name too long")
	}
	if strings.ContainsAny(name, "\x00/:") {
		return errors.New("index name contains reserved punctuation")
	}
	if strings.TrimSpace(name) != name {
		return errors.New("index name has leading or trailing spaces")
	}
	if !utf8.ValidString(name) {
		return errors.New("index name invalid utf-8")
	}
	return nil
}

func ValidateIndexPath(path string) error {
	if len(path) == 0 {
		return errors.New("path cannot be empty")
	}
	if strings.Contains(path, "\x00") {
		return errors.New("path cannot contain NUL")
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return errors.New("path cannot contain empty segments")
	}
	return nil
}
