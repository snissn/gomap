package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/brq"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	collectionMetaVersion        = 6
	collectionMetaVersionV5      = 5
	maxCollectionMutationRetries = 64
	// Bound stale buffered-read replans so a writer under constant buffered
	// pressure eventually falls back to a publish boundary or outer retry.
	maxUpdateBatchBufferedReadReplans = 8
	maxCollectionInt                  = int(^uint(0) >> 1)

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
	// DefaultIndexedWriteMemtableAccumulatorBatchDocuments keeps small indexed
	// InsertBatch calls from creating one frozen root run per call. Larger
	// batches already amortize sorted frozen runs well and keep that path.
	DefaultIndexedWriteMemtableAccumulatorBatchDocuments = 1024
	// DefaultIndexedWriteMemtableAccumulatorLockedPlanningDocuments lets the
	// single-document accumulator path keep planning locked while uncontended,
	// avoiding a relock/catalog-validation round trip once per insert. Larger
	// calls can release the mutation lock while planning.
	DefaultIndexedWriteMemtableAccumulatorLockedPlanningDocuments = 1
	indexedInsertPlanningUnlockMinWait                            = 10 * time.Microsecond
	// DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits bounds default
	// background indexed flush work. When the queue reaches this many immutable
	// flush units, the triggering writer publishes synchronously to cap memory
	// and visibility lag.
	DefaultIndexedWriteMemtableAsyncFlushMaxQueuedUnits = 4
	defaultCollectionUpdateCombineMaxBatch              = 256
	defaultCollectionUpdateCombineShards                = 1
	defaultCollectionUpdateCombineDrainYields           = 1
	defaultCollectionUpdateCombineLaneDrainYields       = 4
	defaultCollectionUpdateCombinePreparedDrainYields   = 128
	collectionUpdateCombineIdleTTL                      = 30 * time.Second
	collectionUpdateCombineInlineQuietPeriod            = time.Millisecond
	collectionPprofComponentKey                         = "gomap_component"
	collectionPprofOperationKey                         = "gomap_operation"
	collectionPprofIndexedAsyncFlush                    = "collections_indexed_async_flush"
	collectionPprofIndexedAsyncFlushRun                 = "publish_buffered_indexed_flush"
)

var (
	ErrCollectionNotFound                = errors.New("collections: collection not found")
	ErrDocumentExists                    = errors.New("collections: document already exists")
	ErrDuplicateDocumentID               = errors.New("collections: duplicate document id in batch")
	ErrIndexNotFound                     = errors.New("collections: index not found")
	ErrUniqueIndexConflict               = errors.New("collections: unique index conflict")
	ErrConcurrentMutation                = errors.New("collections: concurrent mutation")
	ErrDurabilityUnavailable             = errors.New("collections: durability unavailable")
	ErrCommitAmbiguous                   = errors.New("collections: commit ambiguous")
	ErrRecoveryRequired                  = errors.New("collections: recovery required")
	ErrColumnAssetReachabilityIncomplete = errors.New("collections: column asset reachability incomplete")

	errBSONIDMutation                          = errors.New("collections: update replacement cannot modify _id")
	errCollectionManagerNil                    = errors.New("collections: collection manager is nil")
	errCollectionNil                           = errors.New("collections: collection is nil")
	errCollectionDBNil                         = errors.New("collections: db is nil")
	errCollectionNotFound                      = ErrCollectionNotFound
	errConcurrentSchemaModification            = errors.New("collections: concurrent schema modification")
	errCollectionRootOverlaysRequireCompaction = errors.New("collections: collection root overlays require compaction before writes")
	errCollectionIndexScanWorkCap              = errors.New("collections: compound index scan inspected-entry cap reached")
	errCollectionIndexScanRetainedBytesCap     = errors.New("collections: compound index scan retained-ID byte cap reached")
	errUpdateBatchHasSecondaryUniqueIndex      = errors.New("collections: update batch has secondary unique index")
	errUpdateBatchChangesSecondaryUniqueIndex  = errors.New("collections: update batch changes secondary unique index")
	errIndexedFlushLostOwnership               = errors.New("collections: async indexed publish lost ownership of in-flight flush units")
	errCreateCollectionNoopExistingSchema      = errors.New("collections: create collection no-op existing schema")
	errUpdateCombinerStopped                   = errors.New("collections: update combiner stopped before DB update completed; callback may have been invoked")
	indexedAsyncFlushPprofLabels               = pprof.Labels(
		collectionPprofComponentKey, collectionPprofIndexedAsyncFlush,
		collectionPprofOperationKey, collectionPprofIndexedAsyncFlushRun,
	)
)

var testBeforeCommandWALBufferedUpdateStageLockHook struct {
	installMu sync.Mutex
	ptr       atomic.Pointer[testCommandWALBufferedUpdateStageLockHook]
}

type testCommandWALBufferedUpdateStageLockHook struct {
	fn func()
}

var testBeforeInsertBatchPlanningHook struct {
	installMu sync.Mutex
	ptr       atomic.Pointer[testInsertBatchPlanningHook]
}

type testInsertBatchPlanningHook struct {
	fn func()
}

var testBeforeCreateCollectionPublishHook struct {
	installMu sync.Mutex
	ptr       atomic.Pointer[testCreateCollectionPublishHook]
}

type testCreateCollectionPublishHook struct {
	fn func(CollectionMeta)
}

type collectionCatalogLoadFaultStage string

const (
	collectionCatalogLoadFaultMeta collectionCatalogLoadFaultStage = "meta"
	collectionCatalogLoadFaultRoot collectionCatalogLoadFaultStage = "root"
)

type collectionCatalogLoadFaultContext struct {
	Collection string
	Stage      collectionCatalogLoadFaultStage
	RootName   string
	CommitSeq  uint64
	SystemRoot uint64
}

var testCollectionCatalogLoadHook struct {
	installMu sync.Mutex
	ptr       atomic.Pointer[collectionCatalogLoadHook]
}

type collectionCatalogLoadHook struct {
	fn func(collectionCatalogLoadFaultContext) error
}

// CommitAmbiguousError reports that a collection mutation reached its logical
// commit point before a later visibility, flush, checkpoint, response, or
// bookkeeping step failed. The operation may already be visible or recoverable;
// callers should not blindly retry non-idempotent mutations.
type CommitAmbiguousError struct {
	Operation string
	Err       error
}

func (e *CommitAmbiguousError) Error() string {
	if e == nil {
		return ErrCommitAmbiguous.Error()
	}
	if e.Operation == "" {
		if e.Err == nil {
			return ErrCommitAmbiguous.Error()
		}
		return ErrCommitAmbiguous.Error() + ": " + e.Err.Error()
	}
	if e.Err == nil {
		return ErrCommitAmbiguous.Error() + ": " + e.Operation
	}
	return ErrCommitAmbiguous.Error() + ": " + e.Operation + ": " + e.Err.Error()
}

func (e *CommitAmbiguousError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *CommitAmbiguousError) Is(target error) bool {
	return target == ErrCommitAmbiguous
}

func commitAmbiguousError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrCommitAmbiguous) {
		return err
	}
	return &CommitAmbiguousError{Operation: operation, Err: err}
}

func commandWALBufferedUpdateCommitAmbiguous(err error) error {
	if err == nil {
		return nil
	}
	var ambiguous *CommitAmbiguousError
	if errors.As(err, &ambiguous) {
		return err
	}
	return &CommitAmbiguousError{Operation: "command WAL buffered update", Err: err}
}

func commandWALBufferedInsertCommitAmbiguous(err error) error {
	if err == nil {
		return nil
	}
	var ambiguous *CommitAmbiguousError
	if errors.As(err, &ambiguous) {
		return err
	}
	return &CommitAmbiguousError{Operation: "command WAL buffered insert", Err: err}
}

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

type updateBatchItem struct {
	UpdateBatchItem
	bsonSet                       bsonSetUpdate
	hasBSONSet                    bool
	allowTemplateV1StoredDocument bool
}

func newBSONSetUpdateBatchItem(documentID []byte, spec bsonSetUpdate) updateBatchItem {
	return updateBatchItem{
		UpdateBatchItem: UpdateBatchItem{
			DocumentID: bytes.Clone(documentID),
		},
		bsonSet:    spec,
		hasBSONSet: true,
	}
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
	systemCollectionMetaPrefix               = "collections/meta/"
	systemCollectionRootPrefix               = "collections/root/"
	systemCollectionRootOverlayPrefix        = "collections/root-overlay/"
	systemCollectionDocumentGenerationPrefix = "collections/document-generation/"
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

func backendCollectionDataRootStoragePolicy(db *backenddb.DB, policy RootStoragePolicy) (backenddb.OrderedRootStoragePolicy, error) {
	base, err := backendRootStoragePolicy(policy)
	if err != nil {
		return base, err
	}
	if (policy == RootStorageDefault || policy == RootStorageFast) && db != nil && db.HasValueLogAppender() {
		return backenddb.OrderedRootStorageValueLogLeaves, nil
	}
	return base, nil
}

func collectionPlannerOptions(meta CollectionMeta) (collectionOptions, error) {
	return collectionPlannerOptionsForDB(nil, meta)
}

func collectionPlannerOptionsForDB(db *backenddb.DB, meta CollectionMeta) (collectionOptions, error) {
	documentFormat, err := normalizeDocumentFormat(meta.Options.DocumentFormat)
	if err != nil {
		return collectionOptions{}, err
	}
	dataPolicy, err := backendCollectionDataRootStoragePolicy(db, meta.Options.DataRootStoragePolicy)
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
	commandWALCoordinator    *collectionCommandWALCoordinator
	commandWALRawUnregister  func()
	domainMu                 sync.RWMutex
	domains                  map[string]*collectionWriteDomain
	collectionsMu            sync.RWMutex
	collections              map[*Collection]struct{}
}

type collectionManagerOptions struct {
	registerBackendHooks bool
}

type Collection struct {
	db                         *backenddb.DB
	manager                    *CollectionManager
	writeDomain                *collectionWriteDomain
	commandWALRawPublishLocked bool
	name                       string
	meta                       CollectionMeta
	catalogMu                  sync.RWMutex
	catalogCommitSeq           uint64
	catalogSystemRoot          uint64
	catalog                    *collectionCatalog
	insertStatsMu              sync.RWMutex
	lastInsertStats            CollectionInsertStats
	updateStatsMu              sync.RWMutex
	lastUpdateStats            CollectionUpdateStats
	documentScanStatsMu        sync.RWMutex
	lastDocumentScanStats      CollectionDocumentScanStats
	vectorIndexLoadMu          sync.Mutex
	vectorIndexMutationMu      sync.Mutex
	vectorIndexesMu            sync.RWMutex
	vectorIndexes              map[string]*VectorIndex
	vectorPreparedSearchMu     sync.Mutex
	vectorPreparedSearch       map[string]*columnVectorGraphSharedPreparedSearchCacheEntry
	vectorPreparedSearchHits   uint64
	vectorPreparedSearchMisses uint64
	vectorPreparedSearchWaits  uint64
	vectorPreparedSearchBuilds uint64

	vectorBufferedSearchMu            sync.Mutex
	vectorBufferedSearch              map[collectionVectorIndexPreparedSearchCacheSlot]*collectionVectorIndexPreparedSearchCacheEntry
	vectorBufferedSearchHits          uint64
	vectorBufferedSearchMisses        uint64
	vectorBufferedSearchWaits         uint64
	vectorBufferedSearchBuilds        uint64
	vectorBufferedSearchInvalidations uint64
	vectorBufferedSearchCloses        uint64
	vectorBufferedSearchErrors        uint64

	typedColumnOneShotMu            sync.Mutex
	typedColumnOneShot              map[collectionTypedColumnOneShotCacheSlot]*collectionTypedColumnOneShotCacheEntry
	typedColumnOneShotClock         uint64
	typedColumnOneShotHits          uint64
	typedColumnOneShotMisses        uint64
	typedColumnOneShotBuilds        uint64
	typedColumnOneShotInvalidations uint64

	queryReadyGenerationMu            sync.Mutex
	queryReadyGenerationEntry         *collectionQueryReadyGenerationCacheEntry
	queryReadyGenerationHits          uint64
	queryReadyGenerationBuilds        uint64
	queryReadyGenerationInvalidations uint64
}

// CollectionDocumentScanStats describes the most recent ScanDocumentsFunc
// execution on this Collection handle. It is a per-scan snapshot, not a
// cumulative process metric.
type CollectionDocumentScanStats struct {
	CertifiedMonotonicPath bool
	GenericFallback        bool
	PhysicalPasses         uint64
	PhysicalRows           uint64
	PhysicalBytes          uint64
	PhysicalDecodedBlocks  uint64
	// LocatorLookupBatches and LocatorLookups are control-root point reads used
	// by generic reconstruction. They are separate from whole-asset scan stats.
	LocatorLookupBatches uint64
	LocatorLookups       uint64
	PointRowFetches      uint64
	ReconstructedRows    uint64
	MaxRecordWindow      uint64
	MaxVisibleRowWindow  uint64
	// MaxTypedGenerations is the peak number of resident typed generations.
	MaxTypedGenerations uint64
	// MaxTypedDecodedBytes is the peak owned decoded typed value state for one
	// bounded reconstruction window. It excludes source part bytes.
	MaxTypedDecodedBytes uint64
	// MaxTypedSourcePartBytes is the largest encoded typed part read while
	// reconstructing a bounded window. It is reported separately from owned
	// decoded state because the read cache reuses its source buffer.
	MaxTypedSourcePartBytes uint64
	// MaxRetainedBlocks is the peak number of decoded semantic-stream retained
	// blocks kept by the scan-wide fixed-capacity cache.
	MaxRetainedBlocks         uint64
	PreflightProjectedColumns uint64
}

type CollectionRootOverlayCompactionStats struct {
	Roots        int
	OverlayRoots int
}

type collectionRootOverlayCompactionResult struct {
	stats        CollectionRootOverlayCompactionStats
	rootIDs      []uint64
	systemRootID uint64
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

// ColumnPublishCandidateResourceWork distinguishes physical resource-entry
// work from logical-obligation and persistent-index work while constructing
// ordered-root publication candidates.
type ColumnPublishCandidateResourceWork struct {
	CloneOperations                 uint64
	FreezeOperations                uint64
	RequirementFieldsInspected      uint64
	RequirementObligationsInspected uint64
	SourceEntriesInspected          uint64
	SourceObligationsInspected      uint64
	RetainedEntries                 uint64
	RetainedObligations             uint64
	DroppedEntries                  uint64
	DroppedObligations              uint64
	CopiedEntries                   uint64
	CopiedObligations               uint64
	PhysicalHandleCopies            uint64
	LogicalObligationNormalizations uint64
	RetainedIndexNodeVisits         uint64
	RetainedIndexNodeCopies         uint64
	LogicalIndexNodesAdmitted       uint64
	NewlyAdmittedEntries            uint64
	NewlyAdmittedObligations        uint64
	RemovedObligations              uint64
	AppendOnlyFastPath              uint64
	AppendOnlyFallbacks             uint64
	DestructiveFallbacks            uint64
	FullClosureValidations          uint64
}

func (work *ColumnPublishCandidateResourceWork) Add(other ColumnPublishCandidateResourceWork) {
	if work == nil {
		return
	}
	work.CloneOperations += other.CloneOperations
	work.FreezeOperations += other.FreezeOperations
	work.RequirementFieldsInspected += other.RequirementFieldsInspected
	work.RequirementObligationsInspected += other.RequirementObligationsInspected
	work.SourceEntriesInspected += other.SourceEntriesInspected
	work.SourceObligationsInspected += other.SourceObligationsInspected
	work.RetainedEntries += other.RetainedEntries
	work.RetainedObligations += other.RetainedObligations
	work.DroppedEntries += other.DroppedEntries
	work.DroppedObligations += other.DroppedObligations
	work.CopiedEntries += other.CopiedEntries
	work.CopiedObligations += other.CopiedObligations
	work.PhysicalHandleCopies += other.PhysicalHandleCopies
	work.LogicalObligationNormalizations += other.LogicalObligationNormalizations
	work.RetainedIndexNodeVisits += other.RetainedIndexNodeVisits
	work.RetainedIndexNodeCopies += other.RetainedIndexNodeCopies
	work.LogicalIndexNodesAdmitted += other.LogicalIndexNodesAdmitted
	work.NewlyAdmittedEntries += other.NewlyAdmittedEntries
	work.NewlyAdmittedObligations += other.NewlyAdmittedObligations
	work.RemovedObligations += other.RemovedObligations
	work.AppendOnlyFastPath += other.AppendOnlyFastPath
	work.AppendOnlyFallbacks += other.AppendOnlyFallbacks
	work.DestructiveFallbacks += other.DestructiveFallbacks
	work.FullClosureValidations += other.FullClosureValidations
}

// CollectionInsertStats captures phase timings and counters from the most
// recent successful InsertBatch call on a Collection handle.
type CollectionInsertStats struct {
	Documents                    int
	Indexes                      int
	Runs                         int
	BufferedIndexedBatches       int
	BufferedIndexedBypassBatches int
	ValidationPreflightReused    int
	ValidationPreflightRechecked int
	PrepareDocuments             time.Duration
	IndexStateExtraction         time.Duration
	// DuplicateDocumentPreflight includes duplicate-ID detection and
	// existing-document conflict checks.
	DuplicateDocumentPreflight time.Duration
	// RetainedPayloadPrepare includes retained-payload transforms for column
	// stores before the primary run is built.
	RetainedPayloadPrepare                          time.Duration
	RetainedPayloadRows                             int
	RetainedPayloadDeclaredRows                     int
	RetainedPayloadSemanticStreamBlocks             int
	RetainedPayloadSemanticStreamWorkerCount        int
	RetainedPayloadSemanticStreamDeclaredRowPrepare time.Duration
	RetainedPayloadSemanticStreamBlockPrepareWall   time.Duration
	RetainedPayloadSemanticStreamBlockCollect       time.Duration
	RetainedPayloadSemanticStreamBlockEncoderSetup  time.Duration
	RetainedPayloadSemanticStreamBlockRawEncode     time.Duration
	RetainedPayloadSemanticStreamBlockStoredEncode  time.Duration
	RetainedPayloadSemanticStreamBlockFinalize      time.Duration
	RetainedPayloadSemanticStreamTableBuild         time.Duration
	RetainedPayloadValueLogPointerize               time.Duration
	RetainedPayloadValueLogValues                   int
	RetainedPayloadValueLogBytes                    int64
	RetainedStreamValueLogPointerize                time.Duration
	RetainedStreamValueLogValues                    int
	RetainedStreamValueLogBytes                     int64
	// ColumnPublish* fields are populated for typed-column InsertBatch paths
	// that route through the command-WAL column manifest publish path.
	ColumnPublishBuildColumnDelta time.Duration
	ColumnPublishBuildSystemDelta time.Duration
	ColumnPublishCommit           time.Duration
	// ColumnPublish* below decompose the DB portion of ColumnPublishCommit into
	// non-overlapping ordered-root publication phases.
	ColumnPublishWriteLockWait    time.Duration
	ColumnPublishPreflight        time.Duration
	ColumnPublishCommandWALAppend time.Duration
	ColumnPublishOrderedRootApply time.Duration
	ColumnPublishSystemRootApply  time.Duration
	ColumnPublishFinalize         time.Duration
	// ColumnPublishFinalize* fields subdivide ColumnPublishFinalize. They are
	// diagnostic children and are not added to CommitExclusiveTotal.
	ColumnPublishFinalizePrepareDurability         time.Duration
	ColumnPublishFinalizeCandidateBuild            time.Duration
	ColumnPublishFinalizeCandidateVisibleBaseClone time.Duration
	ColumnPublishFinalizeCandidateInheritedFilter  time.Duration
	ColumnPublishFinalizeCandidateFreshCapture     time.Duration
	ColumnPublishFinalizeCandidateClosureAssemble  time.Duration
	ColumnPublishFinalizeCandidateVisibleClone     time.Duration
	ColumnPublishFinalizeCandidateCOWPrepare       time.Duration
	ColumnPublishFinalizeCandidateOther            time.Duration
	ColumnPublishFinalizeCandidateResourceWork     ColumnPublishCandidateResourceWork
	ColumnPublishFinalizeEnqueueActivation         time.Duration
	ColumnPublishFinalizeAdmissionWait             time.Duration
	ColumnPublishFinalizeDurabilityWait            time.Duration
	ColumnPublishPostFinalize                      time.Duration
	ColumnPublishDocumentExtraction                time.Duration
	ColumnPublishDeclaredColumnEncoding            time.Duration
	ColumnPublishAssetPreparation                  time.Duration
	ColumnPublishRowAssetPreparation               time.Duration
	ColumnPublishTypedColumnPreparation            time.Duration

	ColumnPublishTypedColumnDictionaryBuild    time.Duration
	ColumnPublishTypedColumnRowMaterialization time.Duration
	ColumnPublishTypedColumnPartBuild          time.Duration
	ColumnPublishTypedColumnImageBuild         time.Duration

	ColumnPublishDictionaryPreparation                 time.Duration
	ColumnPublishInt64Preparation                      time.Duration
	ColumnPublishAggregateMetadataPrepare              time.Duration
	ColumnPublishRowSidecarSharedBuild                 time.Duration
	ColumnPublishAssetAppend                           time.Duration
	ColumnPublishAssetAppendOpen                       time.Duration
	ColumnPublishAssetAppendWrite                      time.Duration
	ColumnPublishAssetAppendClose                      time.Duration
	ColumnPublishAssetAppendFileSync                   time.Duration
	ColumnPublishAssetAppendFileClose                  time.Duration
	ColumnPublishAssetAppendDirSync                    time.Duration
	ColumnPublishAssetAppendCleanup                    time.Duration
	ColumnPublishAssetAppenderCloseCount               int
	ColumnPublishAssetAppendFileSyncCount              int
	ColumnPublishAssetSyncEpochCount                   int
	ColumnPublishSharedSegmentAppenderCloseCount       int
	ColumnPublishSharedSegmentAppendFileSyncCount      int
	ColumnPublishSharedSegmentAppendSyncEpochCount     int
	ColumnPublishDirectViewSegmentAppenderCloseCount   int
	ColumnPublishDirectViewSegmentAppendFileSyncCount  int
	ColumnPublishDirectViewSegmentAppendSyncEpochCount int
	ColumnPublishManifestEncode                        time.Duration
	ColumnPublishAssetClosureValidation                time.Duration
	ColumnPublishRootDeltaConstruction                 time.Duration
	ColumnPublishSystemDeltaConstruction               time.Duration
	ColumnPublishRootDeltaMaterialization              time.Duration
	ColumnPublishRows                                  int
	ColumnPublishPreparedAssets                        int
	ColumnPublishRowAssetBytes                         int64
	ColumnPublishRowAssetCount                         int
	ColumnPublishTypedColumnBytes                      int64
	ColumnPublishTypedColumnCount                      int
	ColumnPublishDictionaryBytes                       int64
	ColumnPublishDictionaryCount                       int
	ColumnPublishInt64Bytes                            int64
	ColumnPublishInt64Count                            int
	ColumnPublishAggregateMetadataBytes                int64
	ColumnPublishAggregateMetadataCount                int
	ColumnPublishSharedAppendBytes                     int64
	ColumnPublishSharedAppendCount                     int
	ColumnPublishSharedSegmentAppendBytes              int64
	ColumnPublishSharedSegmentAppendCount              int
	ColumnPublishDirectViewSegmentAppendBytes          int64
	ColumnPublishDirectViewSegmentAppendCount          int
	ColumnPublishRequiredAssetBytes                    int64
	ColumnPublishManifestBytes                         int64
	ColumnPublishManifestMutationRecords               int
	ColumnPublishManifestMutationBytes                 int64
	UniqueIndexPreflight                               time.Duration
	TemplateRunBuild                                   time.Duration
	PrimaryRunBuild                                    time.Duration
	IndexStateRunBuild                                 time.Duration
	SecondaryRunBuild                                  time.Duration
	Publish                                            time.Duration
	SecondaryEntries                                   int
	SecondaryKeyBytes                                  int
	SecondarySortedRuns                                int
	SecondaryUnsortedRuns                              int
	SecondaryRuns                                      []CollectionSecondaryRunStats
}

// ColumnPublishCommitExclusiveTotal returns the non-overlapping DB publication
// phases recorded for the most recent typed-column insert.
func (s CollectionInsertStats) ColumnPublishCommitExclusiveTotal() time.Duration {
	return s.ColumnPublishWriteLockWait + s.ColumnPublishPreflight +
		s.ColumnPublishBuildColumnDelta + s.ColumnPublishBuildSystemDelta +
		s.ColumnPublishCommandWALAppend + s.ColumnPublishOrderedRootApply +
		s.ColumnPublishSystemRootApply + s.ColumnPublishFinalize + s.ColumnPublishPostFinalize
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
//
// Some timings are nested rather than additive siblings. IndexStateExtraction
// is the total time spent extracting old and new index state; OldIndexStateExtract
// and NewIndexStateExtract are components of that total. BufferStageRootAppend
// overlaps with BufferStagePrimaryAppend and BufferStageSecondaryAppend, and
// BufferStageLockHold encloses other buffer-stage subphases while the write
// domain mutex is held.
type CollectionUpdateStats struct {
	Items           int
	Matched         int
	Modified        int
	Indexes         int
	Runs            int
	BufferedBatches int
	CurrentRead     time.Duration
	Callback        time.Duration
	// StructuredUpdateApply measures built-in structured update application,
	// such as BSON $set, separately from user callback time.
	StructuredUpdateApply        time.Duration
	StructuredUpdateApplications int
	PrepareDocuments             time.Duration
	// IndexStateExtraction includes both OldIndexStateExtract and
	// NewIndexStateExtract; do not add all three together.
	IndexStateExtraction time.Duration
	OldIndexStateExtract time.Duration
	NewIndexStateExtract time.Duration
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
	BufferStageLockWait        time.Duration
	BufferStageLockHold        time.Duration
	BufferStageValidation      time.Duration
	BufferStageRootScan        time.Duration
	BufferStageDomainPrepare   time.Duration
	BufferStageFreeze          time.Duration
	BufferStageRootTable       time.Duration
	BufferStagePrimaryIdx      time.Duration
	BufferStageUniqueIdx       time.Duration
	BufferStagePrimaryAppend   time.Duration
	BufferStageSecondaryAppend time.Duration
	// BufferStageRootAppend is the total root-append time and overlaps with
	// BufferStagePrimaryAppend and BufferStageSecondaryAppend.
	BufferStageRootAppend time.Duration
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

// CollectionUpdateCombineBucketCount is the number of power-of-two buckets used
// for update-combiner queue-depth and batch-size observations.
const CollectionUpdateCombineBucketCount = 10

var collectionUpdateCombineBucketUpperBounds = [CollectionUpdateCombineBucketCount]int{
	1, 2, 4, 8, 16, 32, 64, 128, 256, 0,
}

// CollectionUpdateCombineBucketLabel returns the stable metric suffix for an
// update-combiner bucket. The final bucket is an overflow bucket.
func CollectionUpdateCombineBucketLabel(index int) string {
	if index < 0 || index >= CollectionUpdateCombineBucketCount {
		return ""
	}
	upper := collectionUpdateCombineBucketUpperBounds[index]
	if upper > 0 {
		return fmt.Sprintf("le_%d", upper)
	}
	return fmt.Sprintf("gt_%d", collectionUpdateCombineBucketUpperBounds[index-1])
}

func collectionUpdateCombineBucketIndex(value int) int {
	for i, upper := range collectionUpdateCombineBucketUpperBounds {
		if upper == 0 || value <= upper {
			return i
		}
	}
	return CollectionUpdateCombineBucketCount - 1
}

// CollectionManagerStats captures aggregate write-domain counters for a
// CollectionManager. The counters are process-local observability; they are
// not persisted with collection metadata.
//
// The update-batch timing aggregates preserve the same nesting semantics as
// CollectionUpdateStats: UpdateBatchIndexStateExtract includes old/new index
// extraction, UpdateBatchBufferRootAppend overlaps with primary/secondary
// append timings, and UpdateBatchBufferLockHold encloses other buffer-stage
// work done while holding the write-domain mutex.
type CollectionManagerStats struct {
	Domains                            int
	PendingDocuments                   int
	PendingBytes                       int64
	PendingRootRuns                    int
	PendingIndexedFlushUnits           int
	OverlayMutableDocuments            int
	OverlayQueuedIndexedFlushUnits     int
	OverlayActiveIndexedFlushUnits     int
	OverlayVisibleDepth                int
	IndexedAsyncFlushRunning           int
	MutationLockCalls                  uint64
	MutationLockWait                   time.Duration
	MutationLockHold                   time.Duration
	IndexedStageBatches                uint64
	IndexedStageDocs                   uint64
	IndexedStageBytes                  uint64
	IndexedStageRootRuns               uint64
	InsertValidationPreflightReused    uint64
	InsertValidationPreflightRechecked uint64
	IndexedAutoFlushes                 uint64
	IndexedAsyncFlushScheduled         uint64
	IndexedAsyncFlushBackpressure      uint64
	IndexedAsyncFlushWait              time.Duration
	IndexedAsyncFlushErrors            uint64
	IndexedFlushCalls                  uint64
	IndexedFlushErrors                 uint64
	IndexedFlushForcedDrains           uint64
	IndexedFlushUnits                  uint64
	IndexedFlushDocs                   uint64
	IndexedFlushBytes                  uint64
	IndexedFlushRootRuns               uint64
	IndexedFlushRoots                  uint64
	IndexedFlushDuration               time.Duration
	IndexedFlushMaterialize            time.Duration
	IndexedFlushPublish                time.Duration
	RootDeltaPlanPrimaryRoots          uint64
	RootDeltaPlanTemplateRoots         uint64
	RootDeltaPlanIndexStateRoots       uint64
	RootDeltaPlanSecondaryRoots        uint64
	RootDeltaPlanEntries               uint64
	RootDeltaPlanKeyBytes              uint64
	RootDeltaPlanValueBytes            uint64
	RootDeltaPlanTombstones            uint64
	PrimaryOnlyUpdateCalls             uint64
	PrimaryOnlyMatched                 uint64
	PrimaryOnlyModified                uint64
	PrimaryOnlyBufferedCalls           uint64
	PrimaryOnlyRootPublishes           uint64
	PrimaryOnlyRootDeltaEntries        uint64
	PrimaryOnlyRootDeltaKeyBytes       uint64
	PrimaryOnlyRootDeltaValueBytes     uint64
	PrimaryOnlyCoalescedDocs           uint64
	UpdateCombineRequests              uint64
	UpdateCombineBatches               uint64
	UpdateCombineBatchedRequests       uint64
	UpdateCombineFallbackRequests      uint64
	UpdateCombineQueueDepthMax         uint64
	UpdateCombineInlineRequests        uint64
	UpdateCombineEnqueue               time.Duration
	UpdateCombineWait                  time.Duration
	UpdateCombineQueueWait             time.Duration
	UpdateCombineDrain                 time.Duration
	UpdateCombineRun                   time.Duration
	UpdateCombineResultDelivery        time.Duration
	UpdateCombineQueueDepthBuckets     [CollectionUpdateCombineBucketCount]uint64
	UpdateCombineBatchSizeBuckets      [CollectionUpdateCombineBucketCount]uint64
	UpdateBatchCalls                   uint64
	UpdateBatchItems                   uint64
	UpdateBatchMatched                 uint64
	UpdateBatchModified                uint64
	UpdateBatchRuns                    uint64
	UpdateBatchBufferedBatches         uint64
	UpdateBatchCurrentRead             time.Duration
	UpdateBatchCallback                time.Duration
	UpdateBatchStructuredApply         time.Duration
	UpdateBatchPrepareDocuments        time.Duration
	// UpdateBatchIndexStateExtract includes UpdateBatchOldIndexStateExtract
	// and UpdateBatchNewIndexStateExtract; do not add all three together.
	UpdateBatchIndexStateExtract    time.Duration
	UpdateBatchOldIndexStateExtract time.Duration
	UpdateBatchNewIndexStateExtract time.Duration
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
	UpdateBatchBufferPrecheck        time.Duration
	UpdateBatchBufferLockWait        time.Duration
	UpdateBatchBufferLockHold        time.Duration
	UpdateBatchBufferValidation      time.Duration
	UpdateBatchBufferRootScan        time.Duration
	UpdateBatchBufferDomainPrepare   time.Duration
	UpdateBatchBufferFreeze          time.Duration
	UpdateBatchBufferRootTable       time.Duration
	UpdateBatchBufferPrimaryIdx      time.Duration
	UpdateBatchBufferUniqueIdx       time.Duration
	UpdateBatchBufferPrimaryAppend   time.Duration
	UpdateBatchBufferSecondaryAppend time.Duration
	// UpdateBatchBufferRootAppend is the total root-append time and overlaps
	// with UpdateBatchBufferPrimaryAppend and UpdateBatchBufferSecondaryAppend.
	UpdateBatchBufferRootAppend time.Duration
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

// BorrowedDocumentRecord is one primary collection record borrowed during a
// callback scan. This is an unsafe performance type: ID and Document are valid
// only until the callback returns and must not be retained or modified.
type BorrowedDocumentRecord struct {
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
	// IndexValueBSONOrderedV2 identifies the versioned BSON scalar key codec.
	// It is valid only for BSON collections and intentionally has no legacy
	// treedbValueType equivalent.
	IndexValueBSONOrderedV2 IndexValueType = "bson-ordered-v2"
)

// IndexDirection is the bytewise order of one BSON scalar index component.
type IndexDirection int8

const (
	IndexDirectionAscending  IndexDirection = 1
	IndexDirectionDescending IndexDirection = -1
)

// IndexComponent is one ordered path in a scalar compound index.
type IndexComponent struct {
	Field     string         `json:"field"`
	Direction IndexDirection `json:"direction"`
}

type CollectionOptions struct {
	AllowArrayValuesInIndex bool           `json:"allow_array_values_in_index,omitempty"`
	DocumentFormat          DocumentFormat `json:"document_format,omitempty"`
	// ColumnStore enables the production-facing column-lane control-plane
	// metadata for this collection. The actual column assets, mutation adapter,
	// and command-WAL publication path are staged by later milestones.
	ColumnStore *ColumnStoreConfig `json:"column_store,omitempty"`
	// DataRootStoragePolicy selects the requested collection data-root layout.
	// Cached TreeDB backends with a persistent value-log appender promote
	// default/fast data roots to value-log leaf roots at runtime so large
	// documents can be stored through stable value pointers.
	DataRootStoragePolicy   RootStoragePolicy `json:"data_root_storage_policy,omitempty"`
	IndexStateStoragePolicy RootStoragePolicy `json:"index_state_storage_policy,omitempty"`
	// DisableIndexedWriteMemtables opts an indexed collection out of the native
	// write-domain memtable path. It is intended for debugging and baseline
	// comparisons; indexed collections use memtables by default.
	DisableIndexedWriteMemtables bool `json:"disable_indexed_write_memtables,omitempty"`
	// DisableBufferedIndexedAsyncFlush opts an indexed collection out of the
	// default background threshold-publish path. It is intended for debugging,
	// baseline comparisons, and workloads that explicitly prefer foreground
	// threshold publish. This is a publish policy, not a per-update durability
	// boundary.
	DisableBufferedIndexedAsyncFlush bool `json:"disable_buffered_indexed_async_flush,omitempty"`
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
	// publish from a background goroutine. Indexed schemas enable this by
	// default unless DisableBufferedIndexedAsyncFlush is set. Flush, FlushAll,
	// and backend Close still drain pending indexed writes before returning, but
	// auto-flush thresholds do not imply per-update durability.
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
	// Components persists the ordered BSON v2 definition. Field remains the
	// compatibility spelling for legacy callers and is normalized to component 0.
	Components []IndexComponent `json:"components,omitempty"`
}

// VectorIndexDefinition declares a document vector field as an ANN-capable
// collection index. PRs after the metadata/API step persist and maintain the
// HNSW graph through collection index roots.
type VectorIndexDefinition struct {
	Name             string                           `json:"name"`
	Field            string                           `json:"field"`
	Metric           VectorMetric                     `json:"metric"`
	Dimensions       int                              `json:"dimensions"`
	M                int                              `json:"m,omitempty"`
	EfConstruction   int                              `json:"ef_construction,omitempty"`
	EfSearch         int                              `json:"ef_search,omitempty"`
	Encoding         VectorIndexEncoding              `json:"encoding,omitempty"`
	Strategy         VectorIndexStrategy              `json:"strategy,omitempty"`
	SchemaGeneration uint64                           `json:"schema_generation,omitempty"`
	QuantizedIndexes []QuantizedVectorIndexDefinition `json:"quantized_indexes,omitempty"`
}

// QuantizedVectorIndexDefinition declares a named derived score plane attached
// to a column_graph vector index. Query modes must still select these indexes
// explicitly, and search must fail closed until matching prepared assets are
// loaded and scored. For scalar_u8, an omitted ScalarU8Calibration preserves the
// legacy v1 default; calibrated per-granule alpha remains explicit opt-in.
type QuantizedVectorIndexDefinition struct {
	Name                string                     `json:"name"`
	Codec               string                     `json:"codec"`
	Version             uint32                     `json:"version,omitempty"`
	ScalarU8Calibration *ScalarU8CalibrationConfig `json:"scalar_u8_calibration,omitempty"`
}

type CollectionMeta struct {
	Name          string                  `json:"name"`
	Options       CollectionOptions       `json:"options,omitempty"`
	Indexes       []IndexDefinition       `json:"indexes,omitempty"`
	VectorIndexes []VectorIndexDefinition `json:"vector_indexes,omitempty"`
	TextIndexes   []TextIndexDefinition   `json:"text_indexes,omitempty"`
}

type collectionMetaDisk struct {
	Version       int                     `json:"version"`
	Name          string                  `json:"name"`
	Options       CollectionOptions       `json:"options,omitempty"`
	Indexes       []IndexDefinition       `json:"indexes,omitempty"`
	VectorIndexes []VectorIndexDefinition `json:"vector_indexes,omitempty"`
	TextIndexes   []TextIndexDefinition   `json:"text_indexes,omitempty"`
}

type collectionCatalog struct {
	// collectionCatalog is immutable once cached or published. Root updates must
	// create a replacement catalog via cloneCatalogWithRootUpdates.
	meta                   CollectionMeta
	roots                  map[string]uint64
	rootOverlays           map[string][]uint64
	rootOverlayFilters     map[string]map[uint64]collectionRootOverlayFilter
	primaryRootName        string
	templateRootName       string
	indexStateRootName     string
	columnManifestRootName string
	indexRuntimes          []indexRuntime
	indexRuntimesErr       error
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

type indexedFlushUnit struct {
	rootRuns        map[string][]memtable.Table
	rootPolicies    map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs     map[string]uint64
	uniqueValueRuns map[string][]memtable.Table
	arenaRefs       [][]byte
	primaryOverlay  *bufferedPrimaryOverlay
	docCount        int
	byteCount       int64
	rootRunCount    int
}

type indexedFlushPublishWork struct {
	pin                *backenddb.Snapshot
	meta               CollectionMeta
	catalog            *collectionCatalog
	baseSystemRoot     uint64
	baseCommitSeq      uint64
	units              []indexedFlushUnit
	flushUnit          indexedFlushUnit
	rootNames          []string
	rootBaseIDs        map[string]uint64
	rootOverlays       map[string][]uint64
	rootOverlayFilters map[string]collectionRootOverlayFilter
	docCount           int
	byteCount          int64
	rootRunCount       int
	rootCount          int
	rootDeltaStats     collectionRootDeltaPlanStats
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
	primaryWriteIndex      *bufferedPrimaryWriteIndex
	rootRuns               map[string][]memtable.Table
	rootMutableRuns        map[string]memtable.Table
	rootPolicies           map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs            map[string]uint64
	rootValueArenas        [][]byte
	primaryOverlay         *bufferedPrimaryOverlay
	primaryCache           *bufferedPrimaryOverlay
	primaryCacheSystemRoot uint64
	primaryCacheCollection string
	primaryCacheDirty      bool
	indexedPublishingUnits []indexedFlushUnit
	indexedFlushUnits      []indexedFlushUnit
	primaryRunIndexActive  bool
	uniqueValueRuns        map[string][]memtable.Table
	uniqueValueMutableRuns map[string]memtable.Table
	rootRunCount           int
	indexedDeletesOnly     bool
	pendingCommandWALFirst uint64
	pendingCommandWALLast  uint64
}

type bufferedUniqueValueIndex struct {
	values     map[uint64][]byte
	collisions map[uint64][][]byte
	arenas     [][]byte
}

type bufferedPrimaryRunIndex struct {
	values        map[uint64]bufferedPrimaryRunRef
	collisions    map[uint64][]bufferedPrimaryRunRef
	arenas        [][]byte
	directEntries bool
}

type bufferedPrimaryWriteIndex struct {
	generations map[uint64]uint64
}

type bufferedPrimaryRunRef struct {
	key        []byte
	table      memtable.Table
	value      []byte
	flags      byte
	entryValid bool
}

type bufferedPrimaryOverlay struct {
	values     map[uint64]bufferedPrimaryOverlayRef
	collisions map[uint64][]bufferedPrimaryOverlayRef
	count      int
}

type bufferedPrimaryOverlayRef struct {
	key   []byte
	value []byte
	flags byte
}

type collectionWriteDomain struct {
	// mutationMu serializes root descriptor publishes for handles opened
	// through the same manager so optimistic retries do not starve under
	// sustained collection write contention.
	mutationMu               sync.Mutex
	mu                       sync.RWMutex
	indexedAsyncMu           sync.Mutex
	indexedAsyncCond         *sync.Cond
	indexedAsyncRun          bool
	indexedAsyncErr          error
	indexedPrepareCond       *sync.Cond
	indexedPrepareFreezes    int
	updateCombineMu          sync.Mutex
	updateCombiner           *collectionUpdateCombiner
	updateDraining           *collectionUpdateCombiner
	updateCombineDone        bool
	updateCombineTTL         time.Duration
	updateCombineShards      int
	updateCombineLaneWorkers bool
	closingWrites            atomic.Bool
	loaded                   bool
	meta                     CollectionMeta
	catalog                  *collectionCatalog
	baseCommitSeq            uint64
	baseSystemRoot           uint64
	primaryRoot              uint64
	storagePolicy            backenddb.OrderedRootStoragePolicy
	commandWALCoordinator    atomic.Pointer[collectionCommandWALCoordinator]
	schemaCoordinator        *collectionSchemaCoordinator
	table                    memtable.Table
	indexedPublishingUnits   []indexedFlushUnit
	indexedFlushUnits        []indexedFlushUnit
	rootRuns                 map[string][]memtable.Table
	rootMutableRuns          map[string]memtable.Table
	rootPolicies             map[string]backenddb.OrderedRootStoragePolicy
	rootBaseIDs              map[string]uint64
	rootValueArenas          [][]byte
	primaryOverlay           *bufferedPrimaryOverlay
	primaryCache             *bufferedPrimaryOverlay
	primaryCacheSystemRoot   uint64
	primaryCacheCollection   string
	primaryCacheDirty        bool
	primaryIDIndex           *bufferedUniqueValueIndex
	nativeVectorIndexLoadMu  sync.Mutex
	nativeVectorMutationMu   sync.Mutex
	// ponytail: share non-vector admission but serialize vector writes so every
	// acknowledgment is published; use cohorts only if vector writes prove limiting.
	nativeVectorAdmissionMu  sync.RWMutex
	nativeVectorCoverageMu   sync.RWMutex
	nativeVectorActiveMu     sync.Mutex
	nativeVectorActive       int
	nativeVectorReconciled   bool
	nativeVectorSearchActive atomic.Bool
	nativeVectorIndexesMu    sync.RWMutex
	nativeVectorIndexes      map[string]*VectorIndex
	nativeVectorPublishMu    sync.RWMutex
	// Built lazily by readers so write-only indexed buffering does not pay for
	// an auxiliary lookup structure it never uses.
	primaryRunIndex             *bufferedPrimaryRunIndex
	uniqueValueRuns             map[string][]memtable.Table
	uniqueValueMutableRuns      map[string]memtable.Table
	uniqueValueIndex            map[string]*bufferedUniqueValueIndex
	count                       int
	bufferedBytes               int64
	mutableCount                int
	mutableBytes                int64
	rootRunCount                int
	writeGeneration             uint64
	primaryWriteIndex           *bufferedPrimaryWriteIndex
	indexedDeletesOnly          bool
	pendingCommandWALFirst      uint64
	pendingCommandWALLast       uint64
	commandWALStageReservations atomic.Int32

	mutationLockCalls                  atomic.Uint64
	mutationLockWaitTotalNs            atomic.Uint64
	mutationLockHoldTotalNs            atomic.Uint64
	indexedStageBatches                atomic.Uint64
	indexedStageDocs                   atomic.Uint64
	indexedStageBytes                  atomic.Uint64
	indexedStageRootRuns               atomic.Uint64
	insertValidationPreflightReused    atomic.Uint64
	insertValidationPreflightRechecked atomic.Uint64
	indexedAutoFlushes                 atomic.Uint64
	indexedAsyncFlushScheduled         atomic.Uint64
	indexedAsyncFlushBackpressure      atomic.Uint64
	indexedAsyncFlushWaitTotalNs       atomic.Uint64
	indexedAsyncFlushErrors            atomic.Uint64
	indexedFlushCalls                  atomic.Uint64
	indexedFlushErrors                 atomic.Uint64
	indexedFlushForcedDrains           atomic.Uint64
	indexedFlushUnitsTotal             atomic.Uint64
	indexedFlushRequeues               atomic.Uint64
	indexedFlushRequeuedUnits          atomic.Uint64
	indexedFlushLostOwnership          atomic.Uint64
	indexedFlushRootBaseMismatches     atomic.Uint64
	indexedFlushDocs                   atomic.Uint64
	indexedFlushBytes                  atomic.Uint64
	indexedFlushRootRuns               atomic.Uint64
	indexedFlushRoots                  atomic.Uint64
	indexedFlushDurationTotalNs        atomic.Uint64
	indexedFlushMaterializeTotalNs     atomic.Uint64
	indexedFlushPublishTotalNs         atomic.Uint64
	rootDeltaPlanPrimaryRoots          atomic.Uint64
	rootDeltaPlanTemplateRoots         atomic.Uint64
	rootDeltaPlanIndexStateRoots       atomic.Uint64
	rootDeltaPlanSecondaryRoots        atomic.Uint64
	rootDeltaPlanEntries               atomic.Uint64
	rootDeltaPlanKeyBytes              atomic.Uint64
	rootDeltaPlanValueBytes            atomic.Uint64
	rootDeltaPlanTombstones            atomic.Uint64
	primaryOnlyUpdateCalls             atomic.Uint64
	primaryOnlyMatched                 atomic.Uint64
	primaryOnlyModified                atomic.Uint64
	primaryOnlyBufferedCalls           atomic.Uint64
	primaryOnlyRootPublishes           atomic.Uint64
	primaryOnlyRootDeltaEntries        atomic.Uint64
	primaryOnlyRootDeltaKeyBytes       atomic.Uint64
	primaryOnlyRootDeltaValueBytes     atomic.Uint64
	primaryOnlyCoalescedDocs           atomic.Uint64
	updateCombineRequests              atomic.Uint64
	updateCombineBatches               atomic.Uint64
	updateCombineBatchedRequests       atomic.Uint64
	updateCombineFallbackRequests      atomic.Uint64
	updateCombineQueueDepthMax         atomic.Uint64
	updateCombineInlineRequests        atomic.Uint64
	updateInlineInFlight               atomic.Uint64
	updateCombineLastRequestUnixNano   atomic.Int64
	updateInlineDocumentID             [collectionUpdateCombineInlineDocumentIDMax]byte
	updateInlineDocumentIDHeap         []byte
	updateInlineItems                  [1]updateBatchItem
	updateCombineEnqueueNs             atomic.Uint64
	updateCombineWaitNs                atomic.Uint64
	updateCombineQueueWaitNs           atomic.Uint64
	updateCombineDrainNs               atomic.Uint64
	updateCombineRunNs                 atomic.Uint64
	updateCombineResultDeliveryNs      atomic.Uint64
	updateCombineQueueDepthBuckets     [CollectionUpdateCombineBucketCount]atomic.Uint64
	updateCombineBatchSizeBuckets      [CollectionUpdateCombineBucketCount]atomic.Uint64
	updateBatchCalls                   atomic.Uint64
	updateBatchItems                   atomic.Uint64
	updateBatchMatched                 atomic.Uint64
	updateBatchModified                atomic.Uint64
	updateBatchRuns                    atomic.Uint64
	updateBatchBufferedBatches         atomic.Uint64
	updateBatchCurrentReadNs           atomic.Uint64
	updateBatchCallbackNs              atomic.Uint64
	updateBatchStructuredApplyNs       atomic.Uint64
	updateBatchPrepareNs               atomic.Uint64
	updateBatchIndexStateNs            atomic.Uint64
	updateBatchOldIndexStateNs         atomic.Uint64
	updateBatchNewIndexStateNs         atomic.Uint64
	updateBatchUniquePreflightNs       atomic.Uint64
	updateBatchTemplateRunNs           atomic.Uint64
	updateBatchPrimaryRunNs            atomic.Uint64
	updateBatchIndexStateRunNs         atomic.Uint64
	updateBatchSecondaryRunNs          atomic.Uint64
	updateBatchBufferStageNs           atomic.Uint64
	updateBatchBufferPrecheckNs        atomic.Uint64
	updateBatchBufferLockWaitNs        atomic.Uint64
	updateBatchBufferLockHoldNs        atomic.Uint64
	updateBatchBufferValidationNs      atomic.Uint64
	updateBatchBufferRootScanNs        atomic.Uint64
	updateBatchBufferDomainPrepareNs   atomic.Uint64
	updateBatchBufferFreezeNs          atomic.Uint64
	updateBatchBufferRootTableNs       atomic.Uint64
	updateBatchBufferPrimaryIdxNs      atomic.Uint64
	updateBatchBufferUniqueIdxNs       atomic.Uint64
	updateBatchBufferPrimaryAppendNs   atomic.Uint64
	updateBatchBufferSecondaryAppendNs atomic.Uint64
	updateBatchBufferRootAppendNs      atomic.Uint64
	updateBatchBufferFlushNs           atomic.Uint64
	updateBatchPublishNs               atomic.Uint64
	updateBatchSecondaryDeletes        atomic.Uint64
	updateBatchSecondarySets           atomic.Uint64
	updateBatchSecondaryKeyBytes       atomic.Uint64
	updateBatchIndexValueChanges       atomic.Uint64
	updateBatchIndexValueUnchanged     atomic.Uint64
	updateBatchMaskFallbacks           atomic.Uint64
	updateBatchUniqueChecks            atomic.Uint64
	updateBatchUniqueCheckSkips        atomic.Uint64
	updateBatchDetailedStats           atomic.Bool
	updateBatchIndexChanged            [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUnchanged          [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUniqueChecks       [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexUniqueSkips        [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryRuns      [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryDeletes   [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondarySets      [maxCollectionUpdateInlineIndexStats]atomic.Uint64
	updateBatchIndexSecondaryBytes     [maxCollectionUpdateInlineIndexStats]atomic.Uint64
}

func NewCollectionManager(database *backenddb.DB) *CollectionManager {
	return newCollectionManager(database, collectionManagerOptions{registerBackendHooks: true})
}

func newCollectionManager(database *backenddb.DB, opts collectionManagerOptions) *CollectionManager {
	manager := &CollectionManager{db: database}
	if database != nil {
		manager.commandWALCoordinator = collectionCommandWALCoordinatorForDB(database)
		if opts.registerBackendHooks {
			manager.commandWALRawUnregister = database.RegisterCommandWALRawPublishBarrier(manager.flushPendingCommandWALBeforeRawPublish)
			manager.closeUnregister = database.RegisterCloseHookBefore(manager.closeForBackend)
		}
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
	defer func() {
		if m.commandWALRawUnregister != nil {
			m.commandWALRawUnregister()
			m.commandWALRawUnregister = nil
		}
	}()
	m.stopUpdateCombiners()
	m.closeCollectionTypedColumnOneShotCaches()
	queryReadyErr := m.closeCollectionQueryReadyGenerationCaches()
	cacheErr := m.closeCollectionVectorIndexPreparedSearchCaches()
	flushErr := m.FlushAll()
	return errors.Join(queryReadyErr, cacheErr, flushErr)
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
	c.setLastInsertStatsOwned(cloneCollectionInsertStats(stats))
}

func (c *Collection) setLastInsertStatsOwned(stats CollectionInsertStats) {
	if c == nil {
		return
	}
	c.insertStatsMu.Lock()
	c.lastInsertStats = stats
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

// LastDocumentScanStats returns an owned snapshot of the most recent
// ScanDocumentsFunc execution on this Collection handle.
func (c *Collection) LastDocumentScanStats() CollectionDocumentScanStats {
	if c == nil {
		return CollectionDocumentScanStats{}
	}
	c.documentScanStatsMu.RLock()
	defer c.documentScanStatsMu.RUnlock()
	return c.lastDocumentScanStats
}

func (c *Collection) setLastDocumentScanStats(stats CollectionDocumentScanStats) {
	if c == nil {
		return
	}
	c.documentScanStatsMu.Lock()
	c.lastDocumentScanStats = stats
	c.documentScanStatsMu.Unlock()
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
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
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
	out["treedb.collections.write_domain.insert.validation_preflight_reused_total"] = fmt.Sprintf("%d", stats.InsertValidationPreflightReused)
	out["treedb.collections.write_domain.insert.validation_preflight_rechecked_total"] = fmt.Sprintf("%d", stats.InsertValidationPreflightRechecked)
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
	out["treedb.collections.write_domain.update_combine.inline_requests_total"] = fmt.Sprintf("%d", stats.UpdateCombineInlineRequests)
	out["treedb.collections.write_domain.update_combine.enqueue_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineEnqueue.Nanoseconds())
	out["treedb.collections.write_domain.update_combine.wait_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineWait.Nanoseconds())
	out["treedb.collections.write_domain.update_combine.queue_wait_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineQueueWait.Nanoseconds())
	out["treedb.collections.write_domain.update_combine.drain_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineDrain.Nanoseconds())
	out["treedb.collections.write_domain.update_combine.run_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineRun.Nanoseconds())
	out["treedb.collections.write_domain.update_combine.result_delivery_ns_total"] = fmt.Sprintf("%d", stats.UpdateCombineResultDelivery.Nanoseconds())
	for i, count := range stats.UpdateCombineQueueDepthBuckets {
		label := CollectionUpdateCombineBucketLabel(i)
		if label == "" {
			continue
		}
		out["treedb.collections.write_domain.update_combine.queue_depth_bucket_"+label+"_total"] = fmt.Sprintf("%d", count)
	}
	for i, count := range stats.UpdateCombineBatchSizeBuckets {
		label := CollectionUpdateCombineBucketLabel(i)
		if label == "" {
			continue
		}
		out["treedb.collections.write_domain.update_combine.batch_size_bucket_"+label+"_total"] = fmt.Sprintf("%d", count)
	}
	out["treedb.collections.write_domain.update_batch.calls_total"] = fmt.Sprintf("%d", stats.UpdateBatchCalls)
	out["treedb.collections.write_domain.update_batch.items_total"] = fmt.Sprintf("%d", stats.UpdateBatchItems)
	out["treedb.collections.write_domain.update_batch.matched_total"] = fmt.Sprintf("%d", stats.UpdateBatchMatched)
	out["treedb.collections.write_domain.update_batch.modified_total"] = fmt.Sprintf("%d", stats.UpdateBatchModified)
	out["treedb.collections.write_domain.update_batch.root_runs_total"] = fmt.Sprintf("%d", stats.UpdateBatchRuns)
	out["treedb.collections.write_domain.update_batch.buffered_batches_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferedBatches)
	out["treedb.collections.write_domain.update_batch.current_read_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchCurrentRead.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.callback_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchCallback.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.structured_apply_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchStructuredApply.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.prepare_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchPrepareDocuments.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.index_state_extract_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchIndexStateExtract.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.old_index_state_extract_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchOldIndexStateExtract.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.new_index_state_extract_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchNewIndexStateExtract.Nanoseconds())
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
	out["treedb.collections.write_domain.update_batch.buffer_stage_freeze_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferFreeze.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_root_table_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferRootTable.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_primary_index_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferPrimaryIdx.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_unique_index_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferUniqueIdx.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_primary_append_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferPrimaryAppend.Nanoseconds())
	out["treedb.collections.write_domain.update_batch.buffer_stage_secondary_append_ns_total"] = fmt.Sprintf("%d", stats.UpdateBatchBufferSecondaryAppend.Nanoseconds())
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

// ResetUpdateCombinersForProfiling stops current update combiners so subsequent
// profiling operations recreate them inside the measured context. It is
// intended for benchmark/profiling harnesses; it may block while a combiner
// drains in-flight updates. Call it after benchmark warmup and before the
// measured phase; it does not flush collection contents or change update
// semantics.
func (m *CollectionManager) ResetUpdateCombinersForProfiling() {
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
		domain.resetUpdateCombinerForProfiling()
	}
}

// SetUpdateCombineShardsForProfiling sets the update-combiner ingress shard
// count for already-opened collection write domains managed by m. It is intended
// for benchmark/profiling experiments and resets active combiners so subsequent
// updates use the new lane count.
func (m *CollectionManager) SetUpdateCombineShardsForProfiling(shards int) {
	if m == nil {
		return
	}
	if shards < 1 {
		shards = defaultCollectionUpdateCombineShards
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
		domain.setUpdateCombineShardsForProfiling(shards)
	}
}

// SetUpdateCombineLaneWorkersForProfiling switches sharded update-combiner
// ingress between the global-combiner path and a lane-worker path. Lane workers
// prepare direct buffered update plans concurrently by document-id shard, then
// merge plans through the same buffered staging layer used by ordinary updates.
func (m *CollectionManager) SetUpdateCombineLaneWorkersForProfiling(enabled bool) {
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
		domain.setUpdateCombineLaneWorkersForProfiling(enabled)
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
	s.InsertValidationPreflightReused += other.InsertValidationPreflightReused
	s.InsertValidationPreflightRechecked += other.InsertValidationPreflightRechecked
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
	s.UpdateCombineInlineRequests += other.UpdateCombineInlineRequests
	s.UpdateCombineEnqueue += other.UpdateCombineEnqueue
	s.UpdateCombineWait += other.UpdateCombineWait
	s.UpdateCombineQueueWait += other.UpdateCombineQueueWait
	s.UpdateCombineDrain += other.UpdateCombineDrain
	s.UpdateCombineRun += other.UpdateCombineRun
	s.UpdateCombineResultDelivery += other.UpdateCombineResultDelivery
	for i := range s.UpdateCombineQueueDepthBuckets {
		s.UpdateCombineQueueDepthBuckets[i] += other.UpdateCombineQueueDepthBuckets[i]
		s.UpdateCombineBatchSizeBuckets[i] += other.UpdateCombineBatchSizeBuckets[i]
	}
	s.UpdateBatchCalls += other.UpdateBatchCalls
	s.UpdateBatchItems += other.UpdateBatchItems
	s.UpdateBatchMatched += other.UpdateBatchMatched
	s.UpdateBatchModified += other.UpdateBatchModified
	s.UpdateBatchRuns += other.UpdateBatchRuns
	s.UpdateBatchBufferedBatches += other.UpdateBatchBufferedBatches
	s.UpdateBatchCurrentRead += other.UpdateBatchCurrentRead
	s.UpdateBatchCallback += other.UpdateBatchCallback
	s.UpdateBatchStructuredApply += other.UpdateBatchStructuredApply
	s.UpdateBatchPrepareDocuments += other.UpdateBatchPrepareDocuments
	s.UpdateBatchIndexStateExtract += other.UpdateBatchIndexStateExtract
	s.UpdateBatchOldIndexStateExtract += other.UpdateBatchOldIndexStateExtract
	s.UpdateBatchNewIndexStateExtract += other.UpdateBatchNewIndexStateExtract
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
	s.UpdateBatchBufferFreeze += other.UpdateBatchBufferFreeze
	s.UpdateBatchBufferRootTable += other.UpdateBatchBufferRootTable
	s.UpdateBatchBufferPrimaryIdx += other.UpdateBatchBufferPrimaryIdx
	s.UpdateBatchBufferUniqueIdx += other.UpdateBatchBufferUniqueIdx
	s.UpdateBatchBufferPrimaryAppend += other.UpdateBatchBufferPrimaryAppend
	s.UpdateBatchBufferSecondaryAppend += other.UpdateBatchBufferSecondaryAppend
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
	stats.InsertValidationPreflightReused = domain.insertValidationPreflightReused.Load()
	stats.InsertValidationPreflightRechecked = domain.insertValidationPreflightRechecked.Load()
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
	stats.UpdateCombineInlineRequests = domain.updateCombineInlineRequests.Load()
	stats.UpdateCombineEnqueue = durationFromAtomicNs(domain.updateCombineEnqueueNs.Load())
	stats.UpdateCombineWait = durationFromAtomicNs(domain.updateCombineWaitNs.Load())
	stats.UpdateCombineQueueWait = durationFromAtomicNs(domain.updateCombineQueueWaitNs.Load())
	stats.UpdateCombineDrain = durationFromAtomicNs(domain.updateCombineDrainNs.Load())
	stats.UpdateCombineRun = durationFromAtomicNs(domain.updateCombineRunNs.Load())
	stats.UpdateCombineResultDelivery = durationFromAtomicNs(domain.updateCombineResultDeliveryNs.Load())
	for i := range stats.UpdateCombineQueueDepthBuckets {
		stats.UpdateCombineQueueDepthBuckets[i] = domain.updateCombineQueueDepthBuckets[i].Load()
		stats.UpdateCombineBatchSizeBuckets[i] = domain.updateCombineBatchSizeBuckets[i].Load()
	}
	stats.UpdateBatchCalls = domain.updateBatchCalls.Load()
	stats.UpdateBatchItems = domain.updateBatchItems.Load()
	stats.UpdateBatchMatched = domain.updateBatchMatched.Load()
	stats.UpdateBatchModified = domain.updateBatchModified.Load()
	stats.UpdateBatchRuns = domain.updateBatchRuns.Load()
	stats.UpdateBatchBufferedBatches = domain.updateBatchBufferedBatches.Load()
	stats.UpdateBatchCurrentRead = durationFromAtomicNs(domain.updateBatchCurrentReadNs.Load())
	stats.UpdateBatchCallback = durationFromAtomicNs(domain.updateBatchCallbackNs.Load())
	stats.UpdateBatchStructuredApply = durationFromAtomicNs(domain.updateBatchStructuredApplyNs.Load())
	stats.UpdateBatchPrepareDocuments = durationFromAtomicNs(domain.updateBatchPrepareNs.Load())
	stats.UpdateBatchIndexStateExtract = durationFromAtomicNs(domain.updateBatchIndexStateNs.Load())
	stats.UpdateBatchOldIndexStateExtract = durationFromAtomicNs(domain.updateBatchOldIndexStateNs.Load())
	stats.UpdateBatchNewIndexStateExtract = durationFromAtomicNs(domain.updateBatchNewIndexStateNs.Load())
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
	stats.UpdateBatchBufferFreeze = durationFromAtomicNs(domain.updateBatchBufferFreezeNs.Load())
	stats.UpdateBatchBufferRootTable = durationFromAtomicNs(domain.updateBatchBufferRootTableNs.Load())
	stats.UpdateBatchBufferPrimaryIdx = durationFromAtomicNs(domain.updateBatchBufferPrimaryIdxNs.Load())
	stats.UpdateBatchBufferUniqueIdx = durationFromAtomicNs(domain.updateBatchBufferUniqueIdxNs.Load())
	stats.UpdateBatchBufferPrimaryAppend = durationFromAtomicNs(domain.updateBatchBufferPrimaryAppendNs.Load())
	stats.UpdateBatchBufferSecondaryAppend = durationFromAtomicNs(domain.updateBatchBufferSecondaryAppendNs.Load())
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

func (domain *collectionWriteDomain) observeInsertValidationPreflight(rechecked bool) {
	if domain == nil {
		return
	}
	if rechecked {
		domain.insertValidationPreflightRechecked.Add(1)
		return
	}
	domain.insertValidationPreflightReused.Add(1)
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
	domain.updateBatchStructuredApplyNs.Add(durationToAtomicNs(stats.StructuredUpdateApply))
	domain.updateBatchPrepareNs.Add(durationToAtomicNs(stats.PrepareDocuments))
	domain.updateBatchIndexStateNs.Add(durationToAtomicNs(stats.IndexStateExtraction))
	domain.updateBatchOldIndexStateNs.Add(durationToAtomicNs(stats.OldIndexStateExtract))
	domain.updateBatchNewIndexStateNs.Add(durationToAtomicNs(stats.NewIndexStateExtract))
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
		domain.updateBatchBufferFreezeNs.Add(durationToAtomicNs(stats.BufferStageFreeze))
		domain.updateBatchBufferRootTableNs.Add(durationToAtomicNs(stats.BufferStageRootTable))
		domain.updateBatchBufferPrimaryIdxNs.Add(durationToAtomicNs(stats.BufferStagePrimaryIdx))
		domain.updateBatchBufferUniqueIdxNs.Add(durationToAtomicNs(stats.BufferStageUniqueIdx))
		domain.updateBatchBufferPrimaryAppendNs.Add(durationToAtomicNs(stats.BufferStagePrimaryAppend))
		domain.updateBatchBufferSecondaryAppendNs.Add(durationToAtomicNs(stats.BufferStageSecondaryAppend))
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
		stats.BufferStageFreeze != 0 ||
		stats.BufferStageRootTable != 0 ||
		stats.BufferStagePrimaryIdx != 0 ||
		stats.BufferStageUniqueIdx != 0 ||
		stats.BufferStagePrimaryAppend != 0 ||
		stats.BufferStageSecondaryAppend != 0 ||
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
		domain.indexedAsyncFlushWaitTotalNs.Add(durationToAtomicNs(collectionObservedElapsedSince(waitStart)))
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

func (domain *collectionWriteDomain) beginIndexedPrepareFreezeLocked() {
	if domain == nil {
		return
	}
	if domain.indexedPrepareCond == nil {
		domain.indexedPrepareCond = sync.NewCond(&domain.mu)
	}
	domain.indexedPrepareFreezes++
}

func (domain *collectionWriteDomain) finishIndexedPrepareFreezeLocked() {
	if domain == nil {
		return
	}
	if domain.indexedPrepareFreezes <= 0 {
		panic("collections: indexed prepare freeze finish without matching begin")
	}
	domain.indexedPrepareFreezes--
	if domain.indexedPrepareFreezes == 0 && domain.indexedPrepareCond != nil {
		domain.indexedPrepareCond.Broadcast()
	}
}

func (domain *collectionWriteDomain) waitIndexedPrepareFreezeLocked() time.Duration {
	if domain == nil || domain.indexedPrepareFreezes <= 0 {
		return 0
	}
	if domain.indexedPrepareCond == nil {
		domain.indexedPrepareCond = sync.NewCond(&domain.mu)
	}
	start := time.Now()
	for domain.indexedPrepareFreezes > 0 {
		domain.indexedPrepareCond.Wait()
	}
	return time.Since(start)
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

var collectionPrimaryRunIndexRebuildHook struct {
	installMu sync.Mutex
	mu        sync.Mutex
	fn        func(collection string, tables, entries int)
}

func setCollectionPrimaryRunIndexRebuildHookForTest(fn func(collection string, tables, entries int)) func() {
	collectionPrimaryRunIndexRebuildHook.installMu.Lock()
	collectionPrimaryRunIndexRebuildHook.mu.Lock()
	prev := collectionPrimaryRunIndexRebuildHook.fn
	collectionPrimaryRunIndexRebuildHook.fn = fn
	collectionPrimaryRunIndexRebuildHook.mu.Unlock()
	return func() {
		collectionPrimaryRunIndexRebuildHook.mu.Lock()
		collectionPrimaryRunIndexRebuildHook.fn = prev
		collectionPrimaryRunIndexRebuildHook.mu.Unlock()
		collectionPrimaryRunIndexRebuildHook.installMu.Unlock()
	}
}

func runCollectionPrimaryRunIndexRebuildHook(collection string, tables, entries int) {
	collectionPrimaryRunIndexRebuildHook.mu.Lock()
	fn := collectionPrimaryRunIndexRebuildHook.fn
	collectionPrimaryRunIndexRebuildHook.mu.Unlock()
	if fn != nil {
		fn(collection, tables, entries)
	}
}

func setTestBeforeCommandWALBufferedUpdateStageLockForTest(fn func()) func() {
	testBeforeCommandWALBufferedUpdateStageLockHook.installMu.Lock()
	prev := testBeforeCommandWALBufferedUpdateStageLockHook.ptr.Load()
	var next *testCommandWALBufferedUpdateStageLockHook
	if fn != nil {
		next = &testCommandWALBufferedUpdateStageLockHook{fn: fn}
	}
	testBeforeCommandWALBufferedUpdateStageLockHook.ptr.Store(next)
	var once sync.Once
	return func() {
		once.Do(func() {
			testBeforeCommandWALBufferedUpdateStageLockHook.ptr.CompareAndSwap(next, prev)
			testBeforeCommandWALBufferedUpdateStageLockHook.installMu.Unlock()
		})
	}
}

func runTestBeforeCommandWALBufferedUpdateStageLockHook() {
	hook := testBeforeCommandWALBufferedUpdateStageLockHook.ptr.Load()
	if hook != nil && hook.fn != nil {
		hook.fn()
	}
}

func setTestCollectionCatalogLoadHookForTest(fn func(collectionCatalogLoadFaultContext) error) func() {
	testCollectionCatalogLoadHook.installMu.Lock()
	prev := testCollectionCatalogLoadHook.ptr.Load()
	var next *collectionCatalogLoadHook
	if fn != nil {
		next = &collectionCatalogLoadHook{fn: fn}
	}
	testCollectionCatalogLoadHook.ptr.Store(next)
	var once sync.Once
	return func() {
		once.Do(func() {
			testCollectionCatalogLoadHook.ptr.Store(prev)
			testCollectionCatalogLoadHook.installMu.Unlock()
		})
	}
}

func runTestCollectionCatalogLoadHook(ctx collectionCatalogLoadFaultContext) error {
	hook := testCollectionCatalogLoadHook.ptr.Load()
	if hook == nil || hook.fn == nil {
		return nil
	}
	return hook.fn(ctx)
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
	domain.updateCombineLastRequestUnixNano.Store(time.Now().UnixNano())
	if queueDepth > 0 {
		atomicMaxUint64(&domain.updateCombineQueueDepthMax, uint64(queueDepth))
		if domain.updateBatchDetailedStats.Load() {
			domain.updateCombineQueueDepthBuckets[collectionUpdateCombineBucketIndex(queueDepth)].Add(1)
		}
	}
}

func (domain *collectionWriteDomain) observeUpdateCombineInline() {
	if domain == nil {
		return
	}
	domain.updateCombineInlineRequests.Add(1)
}

func (domain *collectionWriteDomain) observeUpdateCombineEnqueue(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineEnqueueNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineWait(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineWaitNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineQueueWait(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineQueueWaitNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineDrain(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineDrainNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineRun(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineRunNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineResultDelivery(d time.Duration) {
	if domain == nil || d <= 0 {
		return
	}
	domain.updateCombineResultDeliveryNs.Add(durationToAtomicNs(d))
}

func (domain *collectionWriteDomain) observeUpdateCombineBatch(requests int, fallback bool) {
	if domain == nil || requests <= 0 {
		return
	}
	domain.updateCombineBatches.Add(1)
	domain.updateCombineBatchedRequests.Add(uint64(requests))
	if domain.updateBatchDetailedStats.Load() {
		domain.updateCombineBatchSizeBuckets[collectionUpdateCombineBucketIndex(requests)].Add(1)
	}
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
	schemaCoord := collectionSchemaCoordinatorForDBCollection(m.db, name)
	if domain := m.domains[name]; domain != nil {
		if domain.commandWALCoordinator.Load() == nil {
			domain.commandWALCoordinator.Store(m.commandWALCoordinator)
		}
		if domain.schemaCoordinator == nil {
			domain.schemaCoordinator = schemaCoord
			if schemaCoord != nil {
				schemaCoord.registerDomain(domain)
			}
		}
		return domain
	}
	domain := &collectionWriteDomain{
		updateCombineShards: defaultCollectionUpdateCombineShards,
		schemaCoordinator:   schemaCoord,
	}
	domain.commandWALCoordinator.Store(m.commandWALCoordinator)
	domain.updateBatchDetailedStats.Store(m.updateBatchDetailedStats.Load())
	if schemaCoord != nil {
		schemaCoord.registerDomain(domain)
	}
	m.domains[name] = domain
	return domain
}

func (domain *collectionWriteDomain) setUpdateCombineShardsForProfiling(shards int) {
	if domain == nil {
		return
	}
	if shards < 1 {
		shards = defaultCollectionUpdateCombineShards
	}
	domain.updateCombineMu.Lock()
	if domain.updateCombineShards == shards {
		domain.updateCombineMu.Unlock()
		return
	}
	domain.updateCombineShards = shards
	domain.updateCombineMu.Unlock()
	domain.resetUpdateCombinerForProfiling()
}

func (domain *collectionWriteDomain) setUpdateCombineLaneWorkersForProfiling(enabled bool) {
	if domain == nil {
		return
	}
	domain.updateCombineMu.Lock()
	if domain.updateCombineLaneWorkers == enabled {
		domain.updateCombineMu.Unlock()
		return
	}
	domain.updateCombineLaneWorkers = enabled
	domain.updateCombineMu.Unlock()
	domain.resetUpdateCombinerForProfiling()
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

// FlushAll publishes buffered writes for every collection write domain known to
// this manager, then persists dirty native vector indexes registered through
// collection handles. The backend DB also calls this as a close hook while
// write APIs are still available.
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
	m.collectionsMu.RLock()
	collections := make([]*Collection, 0, len(m.collections))
	for collection := range m.collections {
		if collection != nil {
			collections = append(collections, collection)
		}
	}
	m.collectionsMu.RUnlock()

	var errs []error
	for _, domain := range domains {
		domain.waitIndexedAsyncFlush()
		if err := flushCollectionWriteDomain(m.db, domain); err != nil {
			errs = append(errs, err)
		}
	}
	for _, collection := range collections {
		if err := collection.persistDirtyNativeVectorIndexes(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SyncForStandaloneWriteConcern closes the durable storage boundary used by a
// standalone Mongo journal acknowledgement. Collection-local buffers are
// published first. Command-WAL databases then persist a dependency-complete
// command prefix; WAL-free databases seal a backend root with Checkpoint.
//
// This is intentionally stronger than FlushAll, which is only a visibility
// and draining boundary. physicalSync is false only when a command-WAL prefix
// was already durable (or no command prefix exists); the WAL-free checkpoint
// path always performs a physical sync boundary.
func (m *CollectionManager) SyncForStandaloneWriteConcern() (physicalSync bool, err error) {
	if m == nil || m.db == nil {
		return false, backenddb.ErrClosed
	}
	if err := m.FlushAll(); err != nil {
		return false, err
	}
	if m.db.CommandWALEnabled() {
		return m.db.SyncCommandWALAppliedPrefix()
	}
	if err := m.db.Checkpoint(); err != nil {
		return false, err
	}
	return true, nil
}

func flushCollectionWriteDomain(db *backenddb.DB, domain *collectionWriteDomain) error {
	return flushCollectionWriteDomainWithRawPublishState(db, domain, false)
}

func flushCollectionWriteDomainWithHeldCommandWALRawPublishLock(db *backenddb.DB, domain *collectionWriteDomain) error {
	return flushCollectionWriteDomainWithRawPublishState(db, domain, true)
}

func flushCollectionWriteDomainWithRawPublishState(db *backenddb.DB, domain *collectionWriteDomain, rawPublishLocked bool) error {
	if db == nil || domain == nil {
		return nil
	}
	collection := &Collection{db: db, writeDomain: domain, commandWALRawPublishLocked: rawPublishLocked}
	unlockMutation := lockCollectionDomainMutation(domain)
	defer unlockMutation.Unlock()
	domain.waitIndexedAsyncFlush()
	domain.mu.Lock()
	if hasBufferedIndexedRootRuns(domain) {
		domain.observeIndexedFlushForcedDrain()
	}
	publishedPrimary := hasBufferedPrimaryWritesLocked(domain, domain.meta.Name)
	err := collection.flushBufferedWritesLocked(domain)
	domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
	if err == nil && publishedPrimary {
		err = collection.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked()
	}
	domain.mu.Unlock()
	return err
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

func (c *Collection) tryLockMutation() (collectionMutationUnlock, bool) {
	if c == nil || c.writeDomain == nil {
		return collectionMutationUnlock{}, false
	}
	return tryLockCollectionDomainMutation(c.writeDomain)
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

func tryLockCollectionDomainMutation(domain *collectionWriteDomain) (collectionMutationUnlock, bool) {
	if domain == nil {
		return collectionMutationUnlock{}, false
	}
	if !domain.mutationMu.TryLock() {
		return collectionMutationUnlock{}, false
	}
	return collectionMutationUnlock{domain: domain, holdStart: time.Now()}, true
}

func (unlock collectionMutationUnlock) Unlock() {
	if unlock.domain == nil {
		return
	}
	hold := time.Since(unlock.holdStart)
	unlock.domain.mutationMu.Unlock()
	unlock.domain.observeMutationLock(unlock.wait, hold)
}

// CreateCollection publishes a collection catalog entry.
//
// Current implementations publish metadata through backend roots before
// success. Under the collection WAL target contract, success remains
// process-crash recoverable under WAL-on modes and is not an fsync guarantee
// unless composed with a sync-capable barrier.
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
	return m.createCollectionWithCommandWALIntent(normalized, nil)
}

// CreateCollectionWithCommandWALIntent applies a catalog create through the
// normal collection catalog executor while covering a command-WAL frame that
// was already appended by a deterministic apply layer.
func (m *CollectionManager) CreateCollectionWithCommandWALIntent(meta CollectionMeta, commandWALIntent *backenddb.CommandWALIntent) (*CollectionMeta, error) {
	if m == nil {
		return nil, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, errCollectionDBNil
	}
	if m.isClosing() {
		return nil, backenddb.ErrClosed
	}
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	return m.createCollectionWithCommandWALIntent(normalized, commandWALIntent)
}

// CreateCollectionWithPreparedCommandWALIntent applies a catalog create through
// the normal collection catalog executor while prepareCommandWALIntent appends
// and returns the already-covered command-WAL intent. The callback runs after
// the collection schema lock is held, preserving the public create lock order.
func (m *CollectionManager) CreateCollectionWithPreparedCommandWALIntent(meta CollectionMeta, prepareCommandWALIntent func() (*backenddb.CommandWALIntent, error)) (*CollectionMeta, error) {
	created, _, err := m.CreateCollectionWithPreparedCommandWALIntentStatus(meta, prepareCommandWALIntent)
	return created, err
}

// CreateCollectionWithPreparedCommandWALIntentStatus is the status-returning
// form of CreateCollectionWithPreparedCommandWALIntent. alreadyExisted is
// derived after the collection schema lock is held.
func (m *CollectionManager) CreateCollectionWithPreparedCommandWALIntentStatus(meta CollectionMeta, prepareCommandWALIntent func() (*backenddb.CommandWALIntent, error)) (*CollectionMeta, bool, error) {
	return m.CreateCollectionWithPreparedCommandWALIntentStatusAndPreflight(meta, prepareCommandWALIntent, nil)
}

// CreateCollectionWithPreparedCommandWALIntentStatusAndPreflight is the
// status-returning form of CreateCollectionWithPreparedCommandWALIntent with an
// extra publish preflight composed after the existing-schema no-op check.
func (m *CollectionManager) CreateCollectionWithPreparedCommandWALIntentStatusAndPreflight(meta CollectionMeta, prepareCommandWALIntent func() (*backenddb.CommandWALIntent, error), extraPreflight backenddb.OrderedRootGroupPreflight) (*CollectionMeta, bool, error) {
	if m == nil {
		return nil, false, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, false, errCollectionDBNil
	}
	if m.isClosing() {
		return nil, false, backenddb.ErrClosed
	}
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		return nil, false, err
	}
	return m.createCollectionWithPreparedCommandWALIntent(normalized, nil, prepareCommandWALIntent, extraPreflight)
}

func (m *CollectionManager) createCollectionWithCommandWALIntent(normalized CollectionMeta, commandWALIntent *backenddb.CommandWALIntent) (*CollectionMeta, error) {
	created, _, err := m.createCollectionWithPreparedCommandWALIntent(normalized, commandWALIntent, nil, nil)
	return created, err
}

func (m *CollectionManager) createCollectionWithPreparedCommandWALIntent(normalized CollectionMeta, commandWALIntent *backenddb.CommandWALIntent, prepareCommandWALIntent func() (*backenddb.CommandWALIntent, error), extraPreflight backenddb.OrderedRootGroupPreflight) (*CollectionMeta, bool, error) {
	coveredCommandWALIntent := commandWALIntent != nil && commandWALIntent.AssignedLSN() != 0
	coveredCommandWALIntent = coveredCommandWALIntent || prepareCommandWALIntent != nil
	if !coveredCommandWALIntent {
		if err := validateColumnStoreProfileSupportForDB(m.db, normalized.Options.ColumnStore, "create"); err != nil {
			return nil, false, err
		}
	}
	unlockSchema := func() {}
	if coord := collectionSchemaCoordinatorForDBCollection(m.db, normalized.Name); coord != nil {
		coord.schemaMu.Lock()
		unlockSchema = coord.schemaMu.Unlock
	}
	defer unlockSchema()
	commandWALIntentPrepared := commandWALIntent != nil
	prepareIntent := func() (*backenddb.CommandWALIntent, error) {
		if commandWALIntentPrepared {
			return commandWALIntent, nil
		}
		commandWALIntentPrepared = true
		if prepareCommandWALIntent == nil {
			return nil, nil
		}
		intent, err := prepareCommandWALIntent()
		if err != nil {
			return nil, err
		}
		if intent == nil {
			return nil, errors.New("collections: prepared command-WAL intent is nil")
		}
		if intent.AssignedLSN() == 0 {
			return nil, errors.New("collections: prepared command-WAL intent has no assigned LSN")
		}
		commandWALIntent = intent
		return commandWALIntent, nil
	}
	snap := m.db.AcquireStableSnapshot()
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	existing, err := loadCollectionCatalog(snap, normalized.Name)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if !sameCollectionMeta(existing.meta, normalized) {
			return nil, false, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		commandWALIntent, err := prepareIntent()
		if err != nil {
			return nil, false, err
		}
		if commandWALIntent != nil {
			if err := m.publishCommandWALNoop(commandWALIntent, false); err != nil {
				return nil, false, err
			}
		}
		return existing.meta.copy(), true, nil
	}
	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		return nil, false, err
	}
	plan, err := m.buildCreateCollectionInitialTextV2Plan(normalized)
	if err != nil {
		return nil, false, err
	}
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.rootNames))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionTables(plan.tables)
	}()
	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.rootNames))
	for i, rootName := range plan.rootNames {
		iter := plan.tables[i].NewIterator(nil, nil)
		iterators = append(iterators, iter)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      plan.baseRootIDs[rootName],
			Iter:          iter,
			StoragePolicy: plan.policies[i],
		})
	}
	if hook := testBeforeCreateCollectionPublishHook.ptr.Load(); hook != nil && hook.fn != nil {
		hook.fn(normalized)
	}
	buildSystemDelta := func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != len(plan.rootNames) {
			return nil, unexpectedOrderedRootCountError(normalized.Name, len(plan.rootNames), len(rootIDs))
		}
		return buildSystemDeltaIterator(createCollectionSystemUpdates(normalized.Name, encoded, plan.rootNames, rootIDs))
	}
	preflight := m.createCollectionExistingSchemaPreflight(normalized, extraPreflight)
	if m.db.CommandWALEnabled() || commandWALIntent != nil || prepareCommandWALIntent != nil {
		commandWALIntent, err := prepareIntent()
		if err != nil {
			return nil, false, err
		}
		intent, err := m.newCatalogCreateCollectionCommandWALIntent(normalized, commandWALIntent)
		if err != nil {
			return nil, false, err
		}
		err = m.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			_, _, publishErr := m.db.PublishStagedOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(ordered, preflight, intent, func(_ backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return buildSystemDelta(rootIDs)
			})
			if errors.Is(publishErr, errCreateCollectionNoopExistingSchema) {
				return m.db.PublishStagedCommandWALNoop(intent, false)
			}
			return publishErr
		})
		if err != nil {
			if errors.Is(err, errCreateCollectionNoopExistingSchema) {
				return normalized.copy(), true, nil
			}
			return nil, false, err
		}
		return normalized.copy(), false, nil
	}
	if len(ordered) == 0 {
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
			if existing != nil {
				if !sameCollectionMeta(existing.meta, normalized) {
					return nil, fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
				}
				return nil, errCreateCollectionNoopExistingSchema
			}
			return buildSystemTargetIterator(current, createCollectionSystemUpdates(normalized.Name, encoded, nil, nil))
		})
	} else {
		_, _, err = m.db.PublishOrderedRootDeltaGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if len(rootIDs) != len(plan.rootNames) {
				return nil, unexpectedOrderedRootCountError(normalized.Name, len(plan.rootNames), len(rootIDs))
			}
			current := m.db.AcquireSnapshot()
			if current == nil {
				return nil, backenddb.ErrClosed
			}
			defer func() { _ = current.Close() }()
			return buildSystemTargetIterator(current, createCollectionSystemUpdates(normalized.Name, encoded, plan.rootNames, rootIDs))
		})
	}
	if errors.Is(err, errCreateCollectionNoopExistingSchema) {
		return normalized.copy(), true, nil
	}
	if err != nil {
		return nil, false, err
	}
	return normalized.copy(), false, nil
}

func (m *CollectionManager) createCollectionExistingSchemaPreflight(normalized CollectionMeta, extra backenddb.OrderedRootGroupPreflight) backenddb.OrderedRootGroupPreflight {
	return func() error {
		if m == nil || m.db == nil {
			return errCollectionDBNil
		}
		current := m.db.AcquireSnapshot()
		if current == nil {
			return backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		existing, err := loadCollectionCatalog(current, normalized.Name)
		if err != nil {
			return err
		}
		if existing == nil {
			if extra != nil {
				return extra()
			}
			return nil
		}
		if !sameCollectionMeta(existing.meta, normalized) {
			return fmt.Errorf("collections: existing schema for %q is incompatible", normalized.Name)
		}
		return errCreateCollectionNoopExistingSchema
	}
}

func (m *CollectionManager) buildCreateCollectionInitialTextV2Plan(meta CollectionMeta) (*createTextIndexBackfillPlan, error) {
	plan := &createTextIndexBackfillPlan{baseRootIDs: make(map[string]uint64)}
	if len(meta.TextIndexes) == 0 {
		return plan, nil
	}
	hasV2 := false
	for _, idx := range meta.TextIndexes {
		if idx.Version == TextIndexVersionV2 {
			hasV2 = true
			break
		}
	}
	if !hasV2 {
		return plan, nil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	opts, err := collectionPlannerOptionsForDB(m.db, meta)
	if err != nil {
		return nil, err
	}
	catalog := &collectionCatalog{meta: meta, roots: map[string]uint64{}}
	for _, idx := range meta.TextIndexes {
		if idx.Version != TextIndexVersionV2 {
			continue
		}
		idxPlan, err := buildCreateTextV2IndexBackfillPlan(snap, catalog, idx, opts)
		if err != nil {
			resetCollectionTables(plan.tables)
			return nil, err
		}
		for rootName, baseRootID := range idxPlan.baseRootIDs {
			if _, ok := plan.baseRootIDs[rootName]; !ok {
				plan.baseRootIDs[rootName] = baseRootID
			}
		}
		plan.rootNames = append(plan.rootNames, idxPlan.rootNames...)
		plan.tables = append(plan.tables, idxPlan.tables...)
		plan.policies = append(plan.policies, idxPlan.policies...)
	}
	return plan, nil
}

func createCollectionSystemUpdates(collection string, encodedMeta []byte, rootNames []string, rootIDs []uint64) map[string][]byte {
	updates := make(map[string][]byte, 1+len(rootNames))
	updates[systemCollectionMetaKey(collection)] = encodedMeta
	for i, rootName := range rootNames {
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return updates
}

func (m *CollectionManager) OpenCollection(name string) (*Collection, error) {
	return m.openCollectionWithCommandWALIntent(name, nil)
}

func (m *CollectionManager) openCollectionWithCommandWALIntent(name string, commandWALIntent *backenddb.CommandWALIntent) (*Collection, error) {
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
	coveredCommandWALIntent := commandWALIntent != nil && commandWALIntent.AssignedLSN() != 0
	if collection, ok := m.openCollectionFromWriteDomainCache(name); ok {
		if m.db.IsClosing() {
			return nil, backenddb.ErrClosed
		}
		if !coveredCommandWALIntent {
			if err := validateColumnStoreProfileSupportForDB(m.db, collection.meta.Options.ColumnStore, "open"); err != nil {
				return nil, err
			}
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
	if !coveredCommandWALIntent {
		if err := validateColumnStoreProfileSupportForDB(m.db, catalog.meta.Options.ColumnStore, "open"); err != nil {
			return nil, err
		}
	}
	collection := &Collection{
		db:          m.db,
		manager:     m,
		writeDomain: m.writeDomainForCollection(catalog.meta.Name),
		name:        catalog.meta.Name,
		// Collection catalogs are immutable once loaded; public Meta returns a
		// defensive copy, so handles can keep the catalog meta value directly.
		meta: catalog.meta,
	}
	if collection.writeDomain == nil {
		return nil, backenddb.ErrClosed
	}
	collection.rememberCatalog(snap, catalog)
	collection.noteWriteDomainCatalog(snapshotSystemRoot(snap), catalog)
	return collection, nil
}

func (m *CollectionManager) registerCollectionHandle(collection *Collection) {
	if m == nil || collection == nil {
		return
	}
	m.collectionsMu.Lock()
	defer m.collectionsMu.Unlock()
	m.registerCollectionHandleLocked(collection)
}

func (m *CollectionManager) registerCollectionHandleIfOpen(collection *Collection) bool {
	if m == nil || collection == nil {
		return false
	}
	m.collectionsMu.Lock()
	defer m.collectionsMu.Unlock()
	if m.isClosing() {
		return false
	}
	m.registerCollectionHandleLocked(collection)
	return true
}

func (m *CollectionManager) registerCollectionHandleLocked(collection *Collection) {
	if m.collections == nil {
		m.collections = make(map[*Collection]struct{})
	}
	m.collections[collection] = struct{}{}
}

func (m *CollectionManager) unregisterCollectionHandle(collection *Collection) {
	if m == nil || collection == nil {
		return
	}
	m.collectionsMu.Lock()
	defer m.collectionsMu.Unlock()
	delete(m.collections, collection)
}

func (m *CollectionManager) openCollectionFromWriteDomainCache(name string) (*Collection, bool) {
	if m == nil || m.isClosing() {
		return nil, false
	}
	state, ok := m.db.StateToken()
	if !ok || state.SystemRootPageID == 0 {
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
	currentState, ok := m.db.StateToken()
	if !ok ||
		currentState.SystemRootPageID != state.SystemRootPageID ||
		currentState.CommitSeq != state.CommitSeq {
		return nil, false
	}
	collection := &Collection{
		db:          m.db,
		manager:     m,
		writeDomain: domain,
		name:        catalog.meta.Name,
		// Collection catalogs are immutable once loaded; public Meta returns a
		// defensive copy, so handles can keep the catalog meta value directly.
		meta: catalog.meta,
	}
	collection.rememberCatalogAtSystemRoot(state.SystemRootPageID, catalog)
	return collection, true
}

func (m *CollectionManager) ListCollections() ([]CollectionMeta, error) {
	out, _, err := m.ListCollectionsBounded(0)
	return out, err
}

// ListCollectionsBounded returns catalog metadata in deterministic order and
// stops after inspecting at most maxCollections catalog entries when that limit
// is positive. Deleted entries consume the same work budget, so callers can
// fail closed without walking unbounded catalog history.
func (m *CollectionManager) ListCollectionsBounded(maxCollections int) ([]CollectionMeta, bool, error) {
	if m == nil {
		return nil, false, errCollectionManagerNil
	}
	if m.db == nil {
		return nil, false, errCollectionDBNil
	}
	snap := m.db.AcquireSnapshot()
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	state, ok := snap.StateToken()
	if !ok || state.SystemRootPageID == 0 {
		return nil, false, nil
	}
	prefix := []byte(systemCollectionMetaPrefix)
	it, err := snap.IteratorAtRootWithOptions(state.SystemRootPageID, prefix, prefixEnd(prefix), backenddb.IteratorOptions{IncludeTombstones: true})
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = it.Close() }()

	return listCollectionMetasBounded(it, prefix, maxCollections)
}

type collectionMetaIterator interface {
	Valid() bool
	Next()
	UnsafeKey() []byte
	ValueCopy([]byte) []byte
	IsDeleted() bool
	Error() error
}

func listCollectionMetasBounded(it collectionMetaIterator, prefix []byte, maxCollections int) ([]CollectionMeta, bool, error) {
	var out []CollectionMeta
	inspected := 0
	for it.Valid() {
		key := it.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if maxCollections > 0 && inspected >= maxCollections {
			if err := it.Error(); err != nil {
				return nil, false, err
			}
			sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
			return out, true, nil
		}
		inspected++
		if !it.IsDeleted() {
			meta, err := decodeCollectionMeta(it.ValueCopy(nil))
			if err != nil {
				return nil, false, err
			}
			out = append(out, meta)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, false, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, false, nil
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

// MetaView returns the collection metadata without deep-copying slice fields.
// The returned value is for read-only internal fast paths; callers must not
// mutate Indexes, VectorIndexes, TextIndexes, or other referenced fields.
func (c *Collection) MetaView() CollectionMeta {
	if c == nil {
		return CollectionMeta{}
	}
	return c.meta
}

// SameCachedCatalog reports whether both handles are cached against the same
// collection catalog state. For commit-agnostic catalog shapes, the system root
// is the catalog identity; data-only commits can advance CommitSeq without
// changing collection metadata or root descriptors.
func (c *Collection) SameCachedCatalog(other *Collection) bool {
	if c == nil || other == nil {
		return false
	}
	cName, cSystemRoot, cCommitSeq, cCommitAgnostic := c.cachedCatalogIdentity()
	otherName, otherSystemRoot, otherCommitSeq, otherCommitAgnostic := other.cachedCatalogIdentity()
	return cName != "" &&
		cName == otherName &&
		cSystemRoot != 0 &&
		cSystemRoot == otherSystemRoot &&
		(cCommitSeq == otherCommitSeq || (cCommitAgnostic && otherCommitAgnostic))
}

func (c *Collection) cachedCatalogIdentity() (name string, systemRoot, commitSeq uint64, commitAgnostic bool) {
	if c == nil {
		return "", 0, 0, false
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	return c.meta.Name, c.catalogSystemRoot, c.catalogCommitSeq, c.canReuseCachedCatalogAcrossDataOnlyCommits(c.catalog)
}

// CreateIndex creates and backfills one secondary index as a schema barrier.
//
// Current implementations drain pending writes before planning the backfill.
// Under the collection WAL target contract, success means the index descriptor
// and backfilled roots are atomically recoverable; unique conflicts remain
// pre-commit errors that expose no partial index state.
func (c *Collection) CreateIndex(def IndexDefinition) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if c.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog index mutation is rejected under command_wal_v2 until catalog index commands are supported", backenddb.ErrCommandWALRejected)
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
	baseOptions, err := collectionPlannerOptionsForDB(c.db, baseMeta)
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
	if err := rejectCreateIndexOnRetainedColumnField(baseMeta, normalizedDef); err != nil {
		_ = snap.Close()
		return nil, err
	}
	newRuntime, err := singleIndexRuntime(normalizedDef)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	existingRuntimes, err := catalog.cachedIndexRuntimes()
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

func rejectCreateIndexOnRetainedColumnField(meta CollectionMeta, def IndexDefinition) error {
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload == ColumnRetainedPayloadFull {
		return nil
	}
	field := strings.TrimSpace(def.Field)
	if field == "" {
		return nil
	}
	if cfg.RetainedPayload == ColumnRetainedPayloadNone {
		return fmt.Errorf("collections: CreateIndex on retained-payload-none collection field %q is unsupported because primary rows retain no JSON payload for index maintenance", field)
	}
	for _, col := range cfg.Columns {
		columnPath := strings.TrimSpace(col.Path)
		if columnRetainedPayloadPathOverlaps(field, columnPath) {
			return fmt.Errorf("collections: CreateIndex on retained-payload column field %q is unsupported because primary rows omit declared column payloads", field)
		}
	}
	return nil
}

func columnRetainedPayloadPathOverlaps(indexPath, columnPath string) bool {
	if indexPath == "" || columnPath == "" {
		return false
	}
	return indexPath == columnPath ||
		strings.HasPrefix(indexPath, columnPath+".") ||
		strings.HasPrefix(columnPath, indexPath+".")
}

// CreateVectorIndex adds vector index metadata to the collection schema.
//
// This metadata-only API makes vector fields declarable as document-store
// indexes. Follow-on PRs persist and maintain the native HNSW graph under the
// declared index.
func (c *Collection) CreateVectorIndex(def VectorIndexDefinition) (*CollectionMeta, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if admissionMu := c.nativeVectorAdmissionMutex(); admissionMu != nil {
		admissionMu.Lock()
		defer admissionMu.Unlock()
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
	primaryRootName := collectionPrimaryRootName(baseMeta.Name)
	registerEmptyRuntime := catalog.rootID(primaryRootName) == 0 && len(catalog.overlayRootIDs(primaryRootName)) == 0
	var sourceDocumentGeneration uint64
	if registerEmptyRuntime {
		sourceDocumentGeneration, err = vectorIndexDocumentGeneration(snap, catalog)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
	}
	schemaGeneration := snapshotCommitSeq(snap) + 1
	if schemaGeneration == 0 {
		schemaGeneration = 1
	}
	_ = snap.Close()

	def.SchemaGeneration = schemaGeneration
	newMeta, normalizedDef, err := addVectorIndexToCollectionMeta(baseMeta, def)
	if err != nil {
		return nil, err
	}
	var runtime *VectorIndex
	if registerEmptyRuntime {
		runtime, err = newVectorIndex(c, vectorIndexOptionsFromDefinition(normalizedDef))
		if err != nil {
			return nil, err
		}
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, nil)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, nil, nil)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if registerEmptyRuntime {
		if vectorIndexDefinitionUsesNativeRuntime(normalizedDef) {
			runtime.recordSourceDocumentGeneration(sourceDocumentGeneration)
			if _, err := c.installNativeVectorIndexCandidate(runtime, 0, nil, 0); err != nil {
				return nil, err
			}
		} else {
			c.RegisterVectorIndex(runtime)
		}
	}
	return newMeta.copy(), nil
}

func (c *Collection) DropVectorIndex(name string) (*CollectionMeta, error) {
	if err := ValidateIndexName(name); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if admissionMu := c.nativeVectorAdmissionMutex(); admissionMu != nil {
		admissionMu.Lock()
		defer admissionMu.Unlock()
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
	_ = snap.Close()

	nextIndexes := make([]VectorIndexDefinition, 0, len(baseMeta.VectorIndexes))
	dropped := false
	for _, idx := range baseMeta.VectorIndexes {
		if idx.Name == name {
			dropped = true
			continue
		}
		nextIndexes = append(nextIndexes, idx)
	}
	if !dropped {
		return nil, ErrIndexNotFound
	}
	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name:          baseMeta.Name,
		Options:       baseMeta.Options,
		Indexes:       baseMeta.Indexes,
		VectorIndexes: nextIndexes,
		TextIndexes:   baseMeta.TextIndexes,
	})
	if err != nil {
		return nil, err
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		return nil, err
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorPublishMu.Lock()
		defer c.writeDomain.nativeVectorPublishMu.Unlock()
	}
	clearedRootNames := []string{collectionVectorIndexRootName(baseMeta.Name, name)}
	newSystemRoot, _, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return c.buildSchemaOnlySystemDeltaIterator(baseMeta, encodedMeta, clearedRootNames)
	})
	if err != nil {
		return nil, err
	}
	c.meta = newMeta
	nextCatalog := cloneCatalogWithRootUpdates(catalog, newMeta, clearedRootNames, []uint64{0})
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.UnregisterVectorIndex(name)
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
	if c.db.CommandWALEnabled() {
		return nil, fmt.Errorf("%w: collection catalog index mutation is rejected under command_wal_v2 until catalog index commands are supported", backenddb.ErrCommandWALRejected)
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
		Name:          baseMeta.Name,
		Options:       baseMeta.Options,
		Indexes:       nextIndexes,
		VectorIndexes: baseMeta.VectorIndexes,
		TextIndexes:   baseMeta.TextIndexes,
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

// Insert adds one document and returns the stored document ID.
//
// Current durability is mode/path dependent; see docs/spec/contracts.md. Under
// the collection WAL target contract, WAL-on success is process-crash
// recoverable but not necessarily published, checkpointed, or fsynced. WAL-off
// relaxed success remains process-local until Flush, FlushAll, Checkpoint, or
// Close covers the write.
func (c *Collection) Insert(id, document []byte) ([]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationInsert); err != nil {
		return nil, err
	}
	if len(c.meta.Indexes) == 0 && len(c.meta.VectorIndexes) == 0 && len(c.meta.TextIndexes) == 0 && !c.db.CommandWALEnabled() {
		unlockSchema := c.lockCollectionSchemaRead()
		defer unlockSchema()
		if c.hasBufferedNoIndexBSONPrimaryOverlayOrRootRuns() {
			if err := c.withMutationLock(func() error {
				return c.flushBufferedWrites()
			}); err != nil {
				return nil, err
			}
		}
		return c.insertOneNoIndexBuffered(id, document)
	}
	ids, err := c.InsertBatch([][]byte{id}, [][]byte{document})
	if err != nil {
		if errors.Is(err, ErrCommitAmbiguous) && len(ids) == 1 {
			return ids[0], err
		}
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

// Flush publishes buffered collection-local writes for visibility and drains
// collection-local work. It does not promise a durable WAL or root boundary;
// callers that need durability must use the selected profile's explicit sync,
// Checkpoint, or clean Close contract.
func (c *Collection) Flush() error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if c.writeDomain != nil {
		unlockMutation := c.lockMutation()
		c.writeDomain.waitIndexedAsyncFlush()
		err := c.flushBufferedWrites()
		unlockMutation.Unlock()
		if err != nil {
			return err
		}
		return c.persistDirtyNativeVectorIndexes()
	}
	if err := c.flushBufferedWrites(); err != nil {
		return err
	}
	return c.persistDirtyNativeVectorIndexes()
}

// CompactRootOverlays folds durable collection root overlays into their base
// collection roots and clears the overlay descriptors in the same backend
// commit. It is an explicit maintenance boundary for the overlay-root write-back
// architecture: hot writes may publish durable overlays quickly, then maintenance
// can restore the simple one-root read shape.
func (c *Collection) CompactRootOverlays(ctx context.Context) (CollectionRootOverlayCompactionStats, error) {
	result, err := c.compactRootOverlays(ctx)
	return result.stats, err
}

func (c *Collection) compactRootOverlays(ctx context.Context) (collectionRootOverlayCompactionResult, error) {
	var result collectionRootOverlayCompactionResult
	if c == nil {
		return result, errCollectionNil
	}
	if c.db == nil {
		return result, errCollectionDBNil
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

func (c *Collection) compactRootOverlaysLocked(ctx context.Context) (collectionRootOverlayCompactionResult, error) {
	var result collectionRootOverlayCompactionResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := c.flushBufferedWrites(); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return result, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return result, err
	}
	if catalog == nil {
		return result, errCollectionNotFound
	}
	rootNames := catalog.overlayRootNames()
	if len(rootNames) == 0 {
		return result, nil
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
			return result, err
		}
		overlays := append([]uint64(nil), catalog.overlayRootIDs(rootName)...)
		if len(overlays) == 0 {
			continue
		}
		policy, err := collectionRootStoragePolicyForDB(c.db, catalog.meta, rootName)
		if err != nil {
			cleanupIters()
			return result, err
		}
		it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, nil, nil, false)
		if err != nil {
			cleanupIters()
			return result, err
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
		result.stats.Roots++
		result.stats.OverlayRoots += len(overlays)
	}
	if len(ordered) == 0 {
		return result, nil
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootOverlayCompactionSystemDeltaIteratorForMeta(catalog.meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootOverlays, rootIDs)
	})
	cleanupIters()
	if err != nil {
		return result, err
	}
	if len(rootIDs) != len(rootNames) {
		return result, unexpectedOrderedRootCountError(catalog.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, catalog.meta, rootNames, rootIDs)
	c.meta = catalog.meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	result.rootIDs = appendCollectionCompactStorageProtectedRootIDs(result.rootIDs, rootIDs)
	result.systemRootID = newSystemRoot
	return result, nil
}

func (c *Collection) insertOneNoIndexBuffered(id, document []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, errors.New("collections: document id cannot be empty")
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationInsert); err != nil {
		return nil, err
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
	if catalog == nil {
		domain.mu.Unlock()
		return nil, errCollectionNotFound
	}
	c.meta = catalog.meta
	if err := c.requireColumnStoreCommandWAL(catalog.meta, nil); err != nil {
		domain.mu.Unlock()
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(catalog.meta, ColumnPublishOperationInsert); err != nil {
		domain.mu.Unlock()
		return nil, err
	}
	if indexed || len(catalog.meta.VectorIndexes) > 0 || len(catalog.meta.TextIndexes) > 0 || plannerOptions.documentFormat != DocumentFormatJSON {
		domain.mu.Unlock()
		return c.insertOneViaBatch(id, document)
	}
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
	resultID := bytes.Clone(id)
	domain.table.SetEntry(resultID, document, page.ValuePtr{}, node.FlagInline)
	domain.count++
	domain.writeGeneration++
	domain.notePrimaryWriteKeysLocked([][]byte{resultID}, domain.writeGeneration)
	domain.mu.Unlock()
	return resultID, nil
}

func (c *Collection) canBufferNoIndexInsertBatchAck() bool {
	return c != nil &&
		c.db != nil &&
		c.writeDomain != nil &&
		c.db.DurabilityMode() == backenddb.DurabilityWALOffRelaxed
}

func (c *Collection) canBufferCommandWALNoIndexInsertBatch(meta CollectionMeta, format DocumentFormat, commandWALIntent *backenddb.CommandWALIntent, documentCount int) bool {
	if c == nil || c.db == nil || c.writeDomain == nil || commandWALIntent != nil || documentCount <= 0 {
		return false
	}
	if !c.db.CommandWALEnabled() || c.db.DurabilityMode() == backenddb.DurabilityWALOffRelaxed {
		return false
	}
	if len(meta.Indexes) != 0 || len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 || columnStoreWriteEnabled(meta) {
		return false
	}
	switch normalizedDocumentFormat(format) {
	case DocumentFormatJSON, DocumentFormatBSON:
		return true
	default:
		return false
	}
}

func (c *Collection) canUseCommandWALIndexedInsertBuffer(meta CollectionMeta, format DocumentFormat, commandWALIntent *backenddb.CommandWALIntent) bool {
	if c == nil || c.db == nil || c.writeDomain == nil || commandWALIntent != nil {
		return false
	}
	if !c.db.CommandWALEnabled() || c.db.DurabilityMode() == backenddb.DurabilityWALOffRelaxed {
		return false
	}
	if len(meta.VectorIndexes) != 0 || len(meta.TextIndexes) != 0 || columnStoreWriteEnabled(meta) {
		return false
	}
	if !c.shouldBufferIndexedInserts(meta) {
		return false
	}
	switch normalizedDocumentFormat(format) {
	case DocumentFormatJSON, DocumentFormatBSON:
		return true
	default:
		return false
	}
}

func (c *Collection) canBufferCommandWALIndexedInsertBatch(meta CollectionMeta, format DocumentFormat, commandWALIntent *backenddb.CommandWALIntent, documentCount int) bool {
	return documentCount > 0 &&
		c.canUseCommandWALIndexedInsertBuffer(meta, format, commandWALIntent) &&
		c.shouldBufferIndexedInsertBatch(meta, documentCount)
}

func (c *Collection) canBufferDirectUpdateAck() bool {
	if c == nil || c.db == nil || c.writeDomain == nil {
		return false
	}
	if c.db.CommandWALEnabled() {
		return c.db.DurabilityMode() != backenddb.DurabilityWALOffRelaxed
	}
	switch c.db.DurabilityMode() {
	case backenddb.DurabilityWALOffRelaxed, backenddb.DurabilityWALOnRelaxed:
		return true
	default:
		return false
	}
}

func isBSONDocumentFormat(format DocumentFormat) bool {
	normalized, err := normalizeDocumentFormat(format)
	return err == nil && normalized == DocumentFormatBSON
}

func (c *Collection) hasBufferedNoIndexBSONPrimaryOverlayOrRootRuns() bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	if domain.count == 0 || len(domain.meta.Indexes) != 0 {
		return false
	}
	collectionName := bufferedDomainCollectionName(domain, c.meta.Name)
	if collectionName == "" {
		return false
	}
	hasPrimaryWrites := hasBufferedPrimaryOverlay(domain) ||
		hasPendingIndexedPrimaryOverlay(domain) ||
		hasPendingRootRunsForRootLocked(domain, collectionPrimaryRootName(collectionName))
	if !hasPrimaryWrites {
		return false
	}
	return isBSONDocumentFormat(domain.meta.Options.DocumentFormat)
}

func (c *Collection) bufferNoIndexInsertBatch(
	domain *collectionWriteDomain,
	catalog *collectionCatalog,
	snap *backenddb.Snapshot,
	plannerOptions collectionOptions,
	ids, documents [][]byte,
	execOpts insertBatchExecutionOptions,
) ([][]byte, bool, error) {
	if domain == nil || catalog == nil || snap == nil || len(catalog.meta.Indexes) > 0 {
		return nil, false, nil
	}
	if err := c.requireColumnStoreCommandWAL(catalog.meta, nil); err != nil {
		return nil, true, err
	}
	if err := requireColumnStoreWriteOperationSupported(catalog.meta, ColumnPublishOperationInsert); err != nil {
		return nil, true, err
	}
	switch normalizedDocumentFormat(plannerOptions.documentFormat) {
	case DocumentFormatJSON, DocumentFormatBSON:
	default:
		return nil, false, nil
	}
	if len(ids) != len(documents) {
		return nil, true, fmt.Errorf("collections: caller-provided batch ids length mismatch ids=%d documents=%d", len(ids), len(documents))
	}
	if len(documents) == 0 {
		c.setLastInsertStats(CollectionInsertStats{
			Documents: 0,
			Indexes:   len(catalog.meta.Indexes),
		})
		return nil, true, nil
	}
	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, true, err
	}
	preparedDocuments, _, _, _, err := prepareInsertDocuments(documents, plannerOptions)
	if err != nil {
		return nil, true, err
	}
	entries := make([]noIndexBatchEntry, len(preparedDocuments))
	for i := range preparedDocuments {
		entries[i] = noIndexBatchEntry{
			id:       resultIDs[i],
			document: bytes.Clone(preparedDocuments[i]),
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].id, entries[j].id) < 0
	})
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].id, entries[i].id) {
			return nil, true, ErrDuplicateDocumentID
		}
	}

	domain.mu.Lock()
	defer domain.mu.Unlock()
	currentCatalog, currentOptions, indexed, err := c.ensureWriteDomainLocked(domain)
	if err != nil {
		return nil, true, err
	}
	if indexed || currentCatalog == nil || !sameCollectionMeta(currentCatalog.meta, catalog.meta) {
		return nil, false, nil
	}
	switch normalizedDocumentFormat(currentOptions.documentFormat) {
	case DocumentFormatJSON, DocumentFormatBSON:
	default:
		return nil, false, nil
	}
	c.meta = currentCatalog.meta
	if domain.table == nil {
		domain.table = newCollectionRunTable(0)
	}
	for _, entry := range entries {
		if _, _, flags, found := domain.table.GetEntry(entry.id); found && flags&node.FlagTombstone == 0 {
			return nil, true, ErrDocumentExists
		}
	}
	if domain.primaryRoot != 0 {
		keys := make([][]byte, len(entries))
		for i := range entries {
			keys[i] = entries[i].id
		}
		exists, err := snap.HasAnySortedAtRoot(domain.primaryRoot, keys)
		if err != nil {
			return nil, true, err
		}
		if exists {
			return nil, true, ErrDocumentExists
		}
	}
	domain.storagePolicy = currentOptions.dataStoragePolicy
	for _, entry := range entries {
		domain.table.SetEntry(entry.id, entry.document, page.ValuePtr{}, node.FlagInline)
	}
	domain.count += len(entries)
	domain.writeGeneration++
	domain.notePrimaryWriteKeysLocked(resultIDs, domain.writeGeneration)
	c.setLastInsertStats(CollectionInsertStats{
		Documents: len(entries),
		Indexes:   0,
		Runs:      1,
	})
	return maybeInsertBatchResultIDs(resultIDs, execOpts), true, nil
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
		options, err := collectionPlannerOptionsForDB(c.db, catalog.meta)
		if err != nil {
			return nil, collectionOptions{}, false, err
		}
		return catalog, options, len(catalog.meta.Indexes) > 0, nil
	}
	if domain.loaded && domain.count == 0 && domain.baseSystemRoot == currentSystemRoot && domain.baseCommitSeq == currentCommitSeq {
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(domain.catalog); err != nil {
			return nil, collectionOptions{}, false, err
		}
		options, err := collectionPlannerOptionsForDB(c.db, domain.meta)
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
		options, err := collectionPlannerOptionsForDB(c.db, catalog.meta)
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

	options, err := collectionPlannerOptionsForDB(c.db, catalog.meta)
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
	if err := c.requireColumnStoreCommandWAL(catalog.meta, nil); err != nil {
		return nil, err
	}
	if !sameCollectionMeta(catalog.meta, domain.meta) {
		return nil, fmt.Errorf("collections: concurrent schema modification detected for %q", domain.meta.Name)
	}

	primaryRootName := collectionPrimaryRootName(domain.meta.Name)
	if hasBufferedIndexedRootRuns(domain) {
		if err := forEachPendingIndexedRootBaseIDLocked(domain, func(rootName string, baseRootID uint64) error {
			if rootID := catalog.rootID(rootName); rootID != baseRootID {
				return errBufferedRootBaseMismatch(domain.meta.Name, rootName)
			}
			if !uint64SlicesEqual(catalog.overlayRootIDs(rootName), domain.catalog.overlayRootIDs(rootName)) {
				return errBufferedRootBaseMismatch(domain.meta.Name, rootName)
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
	options, err := collectionPlannerOptionsForDB(c.db, catalog.meta)
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
	if hasBufferedIndexedPendingWrites(domain) {
		domain.mu.Unlock()
		return nil
	}
	publishedPrimary := hasBufferedPrimaryWritesLocked(domain, domain.meta.Name)
	err := c.flushBufferedNoIndexLocked(domain)
	domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
	if err == nil && publishedPrimary {
		err = c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked()
	}
	domain.mu.Unlock()
	return err
}

func (c *Collection) flushBufferedWrites() error {
	return c.flushBufferedWritesWithRawPublishStateAndCoverage(false, false)
}

func (c *Collection) flushBufferedWritesWithRawPublishState(rawPublishLocked bool) error {
	return c.flushBufferedWritesWithRawPublishStateAndCoverage(rawPublishLocked, false)
}

func (c *Collection) flushBufferedWritesWithCoverageLocked() error {
	return c.flushBufferedWritesWithRawPublishStateAndCoverage(false, true)
}

func (c *Collection) flushBufferedWritesWithRawPublishStateAndCoverage(rawPublishLocked, coverageLocked bool) error {
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
		publishedPrimary := hasBufferedPrimaryWritesLocked(domain, domain.meta.Name)
		err := c.flushBufferedWritesLockedWithRawPublishState(domain, rawPublishLocked)
		domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
		if err == nil && coverageLocked {
			err = c.recordReconciledVectorIndexCoverageWithWriteDomainLocked(c.registeredVectorIndexes())
		} else if err == nil && publishedPrimary {
			err = c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked()
		}
		domain.mu.Unlock()
		return err
	}
}

func (c *Collection) flushBufferedWritesLocked(domain *collectionWriteDomain) error {
	return c.flushBufferedWritesLockedWithRawPublishState(domain, false)
}

func (c *Collection) flushBufferedWritesLockedWithRawPublishState(domain *collectionWriteDomain, rawPublishLocked bool) error {
	if domain == nil {
		return nil
	}
	materializePrimaryOverlayLocked(domain)
	if domain.count == 0 {
		return domain.consumeIndexedAsyncFlushError()
	}
	var err error
	if hasBufferedIndexedRootRuns(domain) {
		err = c.flushBufferedIndexedLockedWithRawPublishState(domain, rawPublishLocked)
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
	if err := c.requireColumnStoreCommandWAL(meta, nil); err != nil {
		return err
	}
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
	publishTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(c.db, meta, rootName, table)
	if err != nil {
		return err
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	iter := publishTable.NewIterator(nil, nil)

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
	domain.bufferedBytes = 0
	domain.indexedDeletesOnly = false
	domain.mutableCount = 0
	domain.mutableBytes = 0
	domain.primaryWriteIndex = nil
	c.meta = meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetCollectionRunTable(table)
	return nil
}

func (c *Collection) shouldBufferIndexedInserts(meta CollectionMeta) bool {
	return c != nil && c.writeDomain != nil && meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0 && len(meta.TextIndexes) == 0
}

func (c *Collection) shouldBufferIndexedDeletes(meta CollectionMeta) bool {
	return c != nil &&
		c.writeDomain != nil &&
		meta.Options.BufferedIndexedWrites &&
		len(meta.Indexes) > 0 &&
		len(meta.TextIndexes) == 0 &&
		!collectionMetaHasSecondaryUniqueIndex(meta)
}

func (c *Collection) shouldFlushBeforeIndexedDelete(meta CollectionMeta) bool {
	if !c.shouldBufferIndexedDeletes(meta) {
		return true
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if !domain.indexedDeletesOnly {
		return true
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	catalog, err := c.revalidateBufferedWriteDomainLocked(domain, currentCommitSeq, currentSystemRoot)
	if err != nil {
		return true
	}
	return !c.shouldBufferIndexedDeletes(catalog.meta)
}

func (c *Collection) hasBufferedIndexedDeletesOnly() bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return domain.count > 0 && domain.indexedDeletesOnly
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

func (c *Collection) bufferIndexedInsertPlanLocked(catalog *collectionCatalog, baseCommitSeq, baseSystemRoot uint64, plan *insertBatchPlan, commandWALStageIntent *backenddb.CommandWALIntent, rawStageLocked bool, releaseCommandWALRawStage func()) (elapsed time.Duration, err error) {
	domain := c.writeDomain
	if domain == nil {
		return 0, errors.New("collections: missing write domain")
	}
	var unlockCommandWALRawStage func()
	if releaseCommandWALRawStage == nil {
		releaseCommandWALRawStage = func() {
			if unlockCommandWALRawStage != nil {
				unlockCommandWALRawStage()
				unlockCommandWALRawStage = nil
			}
		}
	}
	releaseLocalCommandWALRawStage := func() {
		if unlockCommandWALRawStage != nil {
			unlockCommandWALRawStage()
			unlockCommandWALRawStage = nil
		}
	}
	defer releaseLocalCommandWALRawStage()
	defer releaseCommandWALRawStage()
	commandWALStageAppended := false
	appendCommandWALBeforeStage := func() (uint64, error) {
		if commandWALStageIntent == nil {
			return 0, nil
		}
		if c.db == nil {
			return 0, errCollectionDBNil
		}
		if !rawStageLocked && unlockCommandWALRawStage == nil {
			unlockCommandWALRawStage = c.db.LockCommandWALStaging()
		}
		lsn, appendErr := c.db.AppendStagedCommandWALIntent(commandWALStageIntent, false)
		if appendErr != nil {
			return 0, appendErr
		}
		commandWALStageAppended = lsn != 0
		return lsn, nil
	}
	defer func() {
		if err != nil && commandWALStageAppended && c.db != nil {
			c.db.MarkCommandWALIntentRecoveryRequired(commandWALStageIntent)
			err = commandWALBufferedInsertCommitAmbiguous(err)
		}
	}()
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if catalog == nil {
		return 0, errCollectionNotFound
	}
	if len(catalog.meta.Indexes) == 0 && commandWALStageIntent == nil {
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
				return errBufferedRootBaseMismatch(catalog.meta.Name, rootName)
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
	ensureBufferedPrimaryRunIndexLocked(domain, len(plan.primaryKeys))
	commandWALLSN, err := appendCommandWALBeforeStage()
	if err != nil {
		return 0, err
	}
	if plan.directBufferedInsert != nil {
		return c.bufferDirectIndexedInsertPlanLocked(domain, catalog, plan, commandWALLSN, releaseCommandWALRawStage)
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
			return 0, errBufferedRootBaseMismatch(catalog.meta.Name, run.name)
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
	domain.indexedDeletesOnly = false
	domain.writeGeneration++
	domain.notePrimaryWriteKeysLocked(plan.resultIDs, domain.writeGeneration)
	domain.observeIndexedStage(len(plan.resultIDs), stagedBytes, stagedRootRuns)
	c.meta = catalog.meta
	if commandWALLSN != 0 {
		if err := domain.recordPendingCommandWALLSNLocked(c.db, commandWALLSN); err != nil {
			return 0, commandWALBufferedInsertCommitAmbiguous(err)
		}
		releaseCommandWALRawStage()
	}
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

func (c *Collection) bufferDirectIndexedInsertPlanLocked(domain *collectionWriteDomain, catalog *collectionCatalog, plan *insertBatchPlan, commandWALLSN uint64, releaseCommandWALRawStage func()) (time.Duration, error) {
	direct := plan.directBufferedInsert
	if direct == nil {
		return 0, nil
	}
	if len(direct.rootNames) == 0 || len(direct.rootNames) != len(direct.policies) {
		return 0, fmt.Errorf("collections: InsertBatch collection %q invalid direct plan lengths roots=%d policies=%d", catalog.meta.Name, len(direct.rootNames), len(direct.policies))
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(direct.rootNames))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(direct.rootNames))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(direct.rootNames))
	}
	if domain.uniqueValueRuns == nil {
		domain.uniqueValueRuns = make(map[string][]memtable.Table)
	}
	if domain.uniqueValueIndex == nil {
		domain.uniqueValueIndex = make(map[string]*bufferedUniqueValueIndex)
	}
	ensureBufferedPrimaryRunIndexLocked(domain, len(plan.primaryKeys))

	addedRootRuns := estimateAccumulatedRootRunsForNamesLocked(domain, direct.rootNames)
	shouldAutoFlushAfterAdding := shouldFlushBufferedIndexedWritesAfterAdding(domain, catalog.meta.Options, len(plan.resultIDs), direct.stagedBytes, addedRootRuns)
	var preAppendFreezeTables []memtable.Table
	if shouldAutoFlushAfterAdding {
		preAppendFreezeTables = detachMutableIndexedRunTablesLocked(domain)
	}
	var checkpoint bufferedIndexedCheckpoint
	collectionMetaCheckpoint := c.meta
	rollbackOnError := shouldAutoFlushAfterAdding
	if shouldAutoFlushAfterAdding {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	rollbackGeneration := checkpoint.writeGeneration
	freezePreAppendTables := func() time.Duration {
		if len(preAppendFreezeTables) == 0 {
			return 0
		}
		freezeDuration, lockReleased, _ := freezeIndexedRunTablesOutsideLock(domain, preAppendFreezeTables)
		if lockReleased > 0 && rollbackOnError && domain.writeGeneration != rollbackGeneration {
			rollbackOnError = false
		}
		preAppendFreezeTables = nil
		return freezeDuration
	}
	defer freezePreAppendTables()

	var rootTablesScratch [8]memtable.Table
	rootTables := rootTablesScratch[:0]
	if len(direct.rootNames) > len(rootTablesScratch) {
		rootTables = make([]memtable.Table, len(direct.rootNames))
	} else {
		rootTables = rootTablesScratch[:len(direct.rootNames)]
	}
	rootTable := func(rootName string) memtable.Table {
		for i, existing := range direct.rootNames {
			if existing == rootName {
				return rootTables[i]
			}
		}
		return nil
	}
	actualRootRuns := 0
	for i, rootName := range direct.rootNames {
		baseRoot := catalog.rootID(rootName)
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			return 0, errBufferedRootBaseMismatch(catalog.meta.Name, rootName)
		}
		if _, ok := domain.rootBaseIDs[rootName]; !ok {
			domain.rootBaseIDs[rootName] = baseRoot
		}
		domain.rootPolicies[rootName] = direct.policies[i]
		table, created := mutableRootRunLocked(domain, rootName)
		if table == nil {
			return 0, fmt.Errorf("collections: InsertBatch collection %q failed to allocate direct root accumulator for %q", catalog.meta.Name, rootName)
		}
		rootTables[i] = table
		if created {
			actualRootRuns = saturatingAddNonNegativeInt(actualRootRuns, 1)
		}
	}
	if len(direct.templateEntries) > 0 {
		templateTable := rootTable(direct.templateRootName)
		if templateTable == nil {
			return 0, fmt.Errorf("collections: InsertBatch collection %q missing direct template root accumulator for %q", catalog.meta.Name, direct.templateRootName)
		}
		if err := applyDirectBufferedRootEntries(templateTable, direct.templateEntries); err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return 0, err
		}
	}
	if len(direct.indexStateEntries) > 0 {
		indexStateTable := rootTable(direct.indexStateRootName)
		if indexStateTable == nil {
			return 0, fmt.Errorf("collections: InsertBatch collection %q missing direct index-state root accumulator for %q", catalog.meta.Name, direct.indexStateRootName)
		}
		if err := applyDirectBufferedRootEntries(indexStateTable, direct.indexStateEntries); err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return 0, err
		}
	}
	primaryTable := rootTable(direct.primaryRootName)
	if primaryTable == nil {
		return 0, fmt.Errorf("collections: InsertBatch collection %q missing direct primary root accumulator for %q", catalog.meta.Name, direct.primaryRootName)
	}
	if err := applyDirectBufferedRootEntries(primaryTable, direct.primaryEntries); err != nil {
		if rollbackOnError {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return 0, err
	}
	if domain.primaryIDIndex == nil {
		domain.primaryIDIndex = newBufferedUniqueValueIndex(max(1, len(plan.primaryKeys)))
	}
	addBufferedPrimaryIDKeys(domain.primaryIDIndex, plan.primaryKeys)
	if domain.primaryRunIndex != nil {
		addBufferedPrimaryRunIndexKeys(domain.primaryRunIndex, plan.primaryKeys, primaryTable)
	}
	for _, secondaryPlan := range direct.secondaryRootPlans {
		table := rootTable(secondaryPlan.rootName)
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
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return 0, err
		}
	}
	for _, uniquePlan := range direct.uniqueValueRootPlans {
		table, _ := mutableUniqueValueRunLocked(domain, uniquePlan.indexName)
		if table == nil {
			return 0, fmt.Errorf("collections: InsertBatch collection %q failed to allocate direct unique accumulator for %q", catalog.meta.Name, uniquePlan.indexName)
		}
		if err := applyDirectBufferedUniqueValuePrefixes(table, uniquePlan.prefixes); err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return 0, err
		}
		index := domain.uniqueValueIndex[uniquePlan.indexName]
		if index == nil {
			index = newBufferedUniqueValueIndex(max(1, len(uniquePlan.prefixes)))
			domain.uniqueValueIndex[uniquePlan.indexName] = index
		}
		index.addAll(uniquePlan.prefixes)
	}

	domain.loaded = true
	domain.meta = catalog.meta
	domain.catalog = catalog
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(catalog.meta.Name))
	domain.count += len(plan.resultIDs)
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, direct.stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, len(plan.resultIDs))
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, direct.stagedBytes)
	domain.writeGeneration++
	domain.notePrimaryWriteKeysLocked(plan.primaryKeys, domain.writeGeneration)
	if rollbackOnError {
		rollbackGeneration = domain.writeGeneration
	}
	domain.observeIndexedStage(len(plan.resultIDs), direct.stagedBytes, actualRootRuns)
	c.meta = catalog.meta
	if commandWALLSN != 0 {
		if err := domain.recordPendingCommandWALLSNLocked(c.db, commandWALLSN); err != nil {
			return 0, commandWALBufferedInsertCommitAmbiguous(err)
		}
		if releaseCommandWALRawStage != nil {
			releaseCommandWALRawStage()
		}
	}

	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, catalog.meta.Options)
	if err != nil {
		if rollbackOnError {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return 0, err
	}
	if shouldFlushBufferedIndexedWrites(domain, catalog.meta.Options) {
		_ = freezePreAppendTables()
		flushElapsed, _, _, err := c.flushBufferedIndexedAfterThresholdLocked(domain, catalog.meta.Options)
		if err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return flushElapsed, err
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
	domain.primaryOverlay = nil
	domain.rootRunCount = 0
	domain.indexedDeletesOnly = false
	domain.mutableCount = 0
	domain.mutableBytes = 0
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueMutableRuns = nil
	domain.uniqueValueIndex = nil
	domain.bufferedBytes = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	retainPrimaryDocumentCacheForCatalogLocked(domain, catalog.meta, baseSystemRoot)
}

func shouldFlushBufferedIndexedWrites(domain *collectionWriteDomain, opts CollectionOptions) bool {
	if domain == nil || domain.count == 0 {
		return false
	}
	count := domain.count
	bytes := domain.bufferedBytes
	rootRuns := bufferedIndexedRootRunCount(domain)
	if opts.BufferedIndexedAsyncFlush {
		if len(domain.rootRuns) == 0 && !hasBufferedPrimaryOverlay(domain) {
			return false
		}
		count = domain.mutableCount
		bytes = domain.mutableBytes
		rootRuns = domain.rootRunCount
		if hasBufferedPrimaryOverlay(domain) {
			rootRuns = saturatingAddNonNegativeInt(rootRuns, 1)
		}
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
		if hasBufferedPrimaryOverlay(domain) {
			baseRootRuns = saturatingAddNonNegativeInt(baseRootRuns, 1)
		}
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
	if domain.indexedPrepareFreezes > 0 {
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
	domain.uniqueValueMutableRuns = nil
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

func setCollectionRunEntryStealWithRevision(table memtable.Table, key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	if revision != page.LegacyEntryRevision {
		if writer, ok := table.(memtable.RevisionStealTable); ok {
			writer.SetEntryStealWithRevision(key, value, ptr, flags, revision)
			return
		}
		if writer, ok := table.(memtable.RevisionTable); ok {
			writer.SetEntryWithRevision(key, value, ptr, flags, revision)
			return
		}
	}
	table.SetEntrySteal(key, value, ptr, flags)
}

func deleteCollectionRunEntryStealWithRevision(table memtable.Table, key []byte, revision page.EntryRevision) {
	if revision != page.LegacyEntryRevision {
		if writer, ok := table.(memtable.RevisionStealTable); ok {
			writer.SetEntryStealWithRevision(key, nil, page.ValuePtr{}, node.FlagTombstone, revision)
			return
		}
		if writer, ok := table.(memtable.RevisionTable); ok {
			writer.SetEntryWithRevision(key, nil, page.ValuePtr{}, node.FlagTombstone, revision)
			return
		}
	}
	table.DeleteSteal(key)
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
		value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)
		var valueCopy []byte
		if value != nil {
			valueCopy = bytes.Clone(value)
		}
		setCollectionRunEntryStealWithRevision(table, key, valueCopy, ptr, flags, revision)
	}
	if err := iter.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	table.Freeze()
	return table, nil
}

type collectionPointerizedRunEntry struct {
	key      []byte
	value    []byte
	ptr      page.ValuePtr
	flags    byte
	revision page.EntryRevision
}

const (
	collectionPointerizeBatchMaxValues           = 1024
	collectionPointerizeBatchMaxBytes            = 4 << 20
	collectionRetainedTemplatePackMinBatchValues = 16
)

type collectionPointerizeOptions struct {
	inlineThresholdForKey        func([]byte) int
	packRetainedTemplateV1Values bool
}

type collectionPointerizeStats struct {
	Values int
	Bytes  int64
}

func collectionRunTableHasStableUnsafeSlices(table memtable.Table) bool {
	stable, ok := table.(memtable.StableUnsafeIteratorTable)
	return ok && stable.StableUnsafeIteratorSlices()
}

func pointerizeCollectionRunTableValues(db *backenddb.DB, table memtable.Table) (memtable.Table, bool, error) {
	return pointerizeCollectionRunTableValuesWithOptions(db, table, collectionPointerizeOptions{})
}

func pointerizeCollectionRunTableValuesForRoot(db *backenddb.DB, meta CollectionMeta, rootName string, table memtable.Table) (memtable.Table, bool, error) {
	out, pointerized, _, err := pointerizeCollectionRunTableValuesForRootWithStats(db, meta, rootName, table)
	return out, pointerized, err
}

func pointerizeCollectionRunTableValuesForRootWithStats(db *backenddb.DB, meta CollectionMeta, rootName string, table memtable.Table) (memtable.Table, bool, collectionPointerizeStats, error) {
	return pointerizeCollectionRunTableValuesWithOptionsAndStats(db, table, collectionPointerizeOptions{
		inlineThresholdForKey:        collectionValueLogInlineThresholdResolverForRoot(db, meta, rootName),
		packRetainedTemplateV1Values: collectionRootStoresRetainedPayloadBodies(meta, rootName) && columnStoreRetainedPayloadUsesTemplateV1(meta.Options.ColumnStore),
	})
}

func collectionValueLogInlineThresholdResolverForRoot(db *backenddb.DB, meta CollectionMeta, rootName string) func([]byte) int {
	if db == nil || (!collectionRootStoresRetainedPayloadBodies(meta, rootName) && !collectionRootStoresRetainedSemanticStreamBlocks(meta, rootName)) {
		return nil
	}
	return func([]byte) int {
		// Retained payload bodies and semantic stream blocks are persistent value-log
		// payloads. Keep them out of leaf pages; empty values still remain inline.
		return 0
	}
}

func collectionRootStoresRetainedPayloadBodies(meta CollectionMeta, rootName string) bool {
	if rootName == "" || rootName != collectionPrimaryRootName(meta.Name) {
		return false
	}
	cfg := meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled {
		return false
	}
	if columnStoreRetainedPayloadUsesSemanticStreamV1(cfg) {
		return false
	}
	switch columnRetainedPayloadAuditPolicy(cfg) {
	case ColumnRetainedPayloadNone:
		return false
	case ColumnRetainedPayloadNonColumn, ColumnRetainedPayloadFull:
		return true
	default:
		// Invalid retained-payload policies are rejected during collection metadata
		// normalization. If one reaches placement, fail closed by keeping the body
		// out of leaf pages rather than silently inlining it.
		return true
	}
}

func collectionRootStoresRetainedSemanticStreamBlocks(meta CollectionMeta, rootName string) bool {
	return rootName != "" &&
		rootName == collectionRetainedSemanticStreamRootName(meta.Name) &&
		columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore)
}

func pointerizeCollectionRunTableValuesWithOptions(db *backenddb.DB, table memtable.Table, opts collectionPointerizeOptions) (memtable.Table, bool, error) {
	out, pointerized, _, err := pointerizeCollectionRunTableValuesWithOptionsAndStats(db, table, opts)
	return out, pointerized, err
}

func pointerizeCollectionRunTableValuesWithOptionsAndStats(db *backenddb.DB, table memtable.Table, opts collectionPointerizeOptions) (memtable.Table, bool, collectionPointerizeStats, error) {
	var stats collectionPointerizeStats
	if db == nil || table == nil || !db.HasValueLogAppender() {
		return table, false, stats, nil
	}
	inlineThresholdForKey := opts.inlineThresholdForKey
	if inlineThresholdForKey == nil {
		inlineThresholdForKey = db.InlineThresholdForKey
	}
	needsPointer := false
	probe := table.NewIterator(nil, nil)
	for probe.Valid() {
		value, _, flags := probe.UnsafeEntry()
		if flags&node.FlagTombstone == 0 &&
			flags&node.FlagPointer == 0 &&
			len(value) > inlineThresholdForKey(probe.UnsafeKey()) {
			needsPointer = true
			break
		}
		probe.Next()
	}
	probeErr := probe.Error()
	_ = probe.Close()
	if probeErr != nil {
		return table, false, stats, probeErr
	}
	if !needsPointer {
		return table, false, stats, nil
	}

	entries := make([]collectionPointerizedRunEntry, 0, table.Len())
	// AppendValueLogValues completes the append before returning. For run tables
	// that promise immutable iterator values across Next/Close, borrow those
	// slices for the synchronous append and avoid a per-value clone; unstable
	// iterator tables keep the defensive copy below.
	borrowPointerValues := collectionRunTableHasStableUnsafeSlices(table)
	batchValues := make([][]byte, 0, collectionPointerizeBatchMaxValues)
	batchEntryIndexes := make([]int, 0, collectionPointerizeBatchMaxValues)
	batchBytes := 0
	pointerized := false
	var appendedPtrs []page.ValuePtr
	success := false
	defer func() {
		if !success {
			db.ReleaseValueLogValues(appendedPtrs)
		}
	}()
	flushBatch := func() error {
		if len(batchValues) == 0 {
			return nil
		}
		ptrs, err := appendCollectionPointerizedBatchValues(db, batchValues, opts.packRetainedTemplateV1Values)
		if err != nil {
			return err
		}
		if len(ptrs) != len(batchValues) {
			db.ReleaseValueLogValues(ptrs)
			return fmt.Errorf("collections: value-log appender returned %d ptrs for %d values", len(ptrs), len(batchValues))
		}
		appendedPtrs = append(appendedPtrs, ptrs...)
		for i, ptr := range ptrs {
			entryIndex := batchEntryIndexes[i]
			entries[entryIndex].value = nil
			entries[entryIndex].ptr = ptr
			entries[entryIndex].flags = (entries[entryIndex].flags &^ node.FlagTombstone) | node.FlagPointer
		}
		for i := range batchValues {
			batchValues[i] = nil
		}
		batchValues = batchValues[:0]
		batchEntryIndexes = batchEntryIndexes[:0]
		batchBytes = 0
		pointerized = true
		return nil
	}
	it := table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
		entry := collectionPointerizedRunEntry{
			key:      bytes.Clone(it.UnsafeKey()),
			ptr:      ptr,
			flags:    flags,
			revision: revision,
		}
		if flags&node.FlagTombstone == 0 &&
			flags&node.FlagPointer == 0 &&
			len(value) > inlineThresholdForKey(entry.key) {
			entries = append(entries, entry)
			appendValue := value
			if !borrowPointerValues {
				appendValue = bytes.Clone(value)
			}
			batchValues = append(batchValues, appendValue)
			batchEntryIndexes = append(batchEntryIndexes, len(entries)-1)
			batchBytes += len(appendValue)
			stats.Values++
			stats.Bytes = saturatingAddNonNegativeInt64(stats.Bytes, int64(len(appendValue)))
			if len(batchValues) >= collectionPointerizeBatchMaxValues || batchBytes >= collectionPointerizeBatchMaxBytes {
				if err := flushBatch(); err != nil {
					return table, false, stats, err
				}
			}
		} else if value != nil {
			entry.value = bytes.Clone(value)
			entries = append(entries, entry)
		} else {
			entries = append(entries, entry)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return table, false, stats, err
	}
	if err := flushBatch(); err != nil {
		return table, false, stats, err
	}
	if !pointerized {
		return table, false, stats, nil
	}
	out := newCollectionRunTable(len(entries))
	for i := range entries {
		entry := entries[i]
		setCollectionRunEntryStealWithRevision(out, entry.key, entry.value, entry.ptr, entry.flags, entry.revision)
	}
	out.Freeze()
	success = true
	return &collectionPointerizedRunTable{Table: out, db: db, ptrs: appendedPtrs}, true, stats, nil
}

type collectionPointerizedRunTable struct {
	memtable.Table
	db   *backenddb.DB
	ptrs []page.ValuePtr
}

func (t *collectionPointerizedRunTable) Release() {
	if t == nil {
		return
	}
	if t.db != nil && len(t.ptrs) > 0 {
		t.db.ReleaseValueLogValues(t.ptrs)
		t.db = nil
		t.ptrs = nil
	}
	resetCollectionRunTable(t.Table)
}

func (t *collectionPointerizedRunTable) Reset() {
	t.Release()
}

func appendCollectionPointerizedBatchValues(db *backenddb.DB, values [][]byte, packRetainedTemplateV1 bool) ([]page.ValuePtr, error) {
	if !packRetainedTemplateV1 || len(values) < collectionRetainedTemplatePackMinBatchValues {
		return db.AppendValueLogValues(values)
	}
	order, ok := collectionRetainedTemplateV1PackOrder(values)
	if !ok {
		return db.AppendValueLogValues(values)
	}
	packed := make([][]byte, len(values))
	for i, sourceIdx := range order {
		packed[i] = values[sourceIdx]
	}
	packedPtrs, err := db.AppendValueLogValues(packed)
	if err != nil {
		return nil, err
	}
	if len(packedPtrs) != len(values) {
		db.ReleaseValueLogValues(packedPtrs)
		return nil, fmt.Errorf("collections: retained template-v1 packed append returned %d ptrs for %d values", len(packedPtrs), len(values))
	}
	ptrs := make([]page.ValuePtr, len(values))
	for packedIdx, sourceIdx := range order {
		ptrs[sourceIdx] = packedPtrs[packedIdx]
	}
	return ptrs, nil
}

func collectionRetainedTemplateV1PackOrder(values [][]byte) ([]int, bool) {
	if len(values) < 2 {
		return nil, false
	}
	type valueTemplate struct {
		index      int
		templateID uint64
	}
	order := make([]valueTemplate, len(values))
	distinct := false
	var firstID uint64
	for i, value := range values {
		root, err := parseTemplateV1StoredDocument(value)
		if err != nil {
			return nil, false
		}
		if i == 0 {
			firstID = root.templateID
		} else if root.templateID != firstID {
			distinct = true
		}
		order[i] = valueTemplate{
			index:      i,
			templateID: root.templateID,
		}
	}
	if !distinct {
		return nil, false
	}
	sort.SliceStable(order, func(i, j int) bool {
		return order[i].templateID < order[j].templateID
	})
	out := make([]int, len(order))
	changed := false
	for i := range order {
		out[i] = order[i].index
		if out[i] != i {
			changed = true
		}
	}
	if !changed {
		return nil, false
	}
	return out, true
}

func pointerizeInsertBatchPlanDataRuns(db *backenddb.DB, meta CollectionMeta, plan *insertBatchPlan) ([]memtable.Table, error) {
	if plan == nil || db == nil || !db.HasValueLogAppender() {
		return nil, nil
	}
	var obsolete []memtable.Table
	for i := range plan.runs {
		switch plan.runs[i].kind {
		case collectionRootPrimary, collectionRootTemplate:
		default:
			continue
		}
		pointerizedTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(db, meta, plan.runs[i].name, plan.runs[i].table)
		if err != nil {
			resetCollectionTables(obsolete)
			return nil, err
		}
		if !pointerized {
			continue
		}
		obsolete = append(obsolete, plan.runs[i].table)
		plan.runs[i].table = pointerizedTable
	}
	return obsolete, nil
}

func collectionDataRootNameSet(meta CollectionMeta) map[string]struct{} {
	out := map[string]struct{}{
		collectionPrimaryRootName(meta.Name):  {},
		collectionTemplateRootName(meta.Name): {},
	}
	if columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
		out[collectionRetainedSemanticStreamRootName(meta.Name)] = struct{}{}
	}
	return out
}

func pointerizeCollectionDataRootDeltaTables(db *backenddb.DB, meta CollectionMeta, rootNames []string, tables []memtable.Table) ([]memtable.Table, func(), error) {
	if db == nil || !db.HasValueLogAppender() || len(rootNames) == 0 || len(tables) == 0 {
		return tables, func() {}, nil
	}
	if len(rootNames) != len(tables) {
		return nil, nil, fmt.Errorf("collections: invalid delta table lengths roots=%d tables=%d", len(rootNames), len(tables))
	}
	dataRoots := collectionDataRootNameSet(meta)
	var out []memtable.Table
	var pointerizedTables []memtable.Table
	cleanup := func() {
		resetCollectionTables(pointerizedTables)
	}
	for i, rootName := range rootNames {
		if _, ok := dataRoots[rootName]; !ok {
			continue
		}
		pointerizedTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(db, meta, rootName, tables[i])
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		if !pointerized {
			continue
		}
		if out == nil {
			out = append([]memtable.Table(nil), tables...)
		}
		out[i] = pointerizedTable
		pointerizedTables = append(pointerizedTables, pointerizedTable)
	}
	if out == nil {
		return tables, func() {}, nil
	}
	return out, cleanup, nil
}

func pointerizeCollectionDataRootRunMapValues(db *backenddb.DB, meta CollectionMeta, rootRuns map[string][]memtable.Table) (map[string][]memtable.Table, func(), error) {
	if db == nil || !db.HasValueLogAppender() || len(rootRuns) == 0 {
		return rootRuns, func() {}, nil
	}
	dataRoots := collectionDataRootNameSet(meta)
	var out map[string][]memtable.Table
	var clonedSlices map[string]bool
	var pointerizedTables []memtable.Table
	cleanup := func() {
		resetCollectionTables(pointerizedTables)
	}
	ensureOut := func() {
		if out != nil {
			return
		}
		out = make(map[string][]memtable.Table, len(rootRuns))
		for name, runs := range rootRuns {
			out[name] = runs
		}
		clonedSlices = make(map[string]bool, 2)
	}
	for rootName, runs := range rootRuns {
		if _, ok := dataRoots[rootName]; !ok {
			continue
		}
		for i, run := range runs {
			pointerizedTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(db, meta, rootName, run)
			if err != nil {
				cleanup()
				return nil, nil, err
			}
			if !pointerized {
				continue
			}
			ensureOut()
			if !clonedSlices[rootName] {
				out[rootName] = append([]memtable.Table(nil), runs...)
				clonedSlices[rootName] = true
			}
			out[rootName][i] = pointerizedTable
			pointerizedTables = append(pointerizedTables, pointerizedTable)
		}
	}
	if out == nil {
		return rootRuns, func() {}, nil
	}
	return out, cleanup, nil
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

func materializePrimaryOverlayLocked(domain *collectionWriteDomain) bool {
	if !hasBufferedPrimaryOverlay(domain) {
		return false
	}
	collectionName := bufferedDomainCollectionName(domain, "")
	if collectionName == "" {
		return false
	}
	rootName := collectionPrimaryRootName(collectionName)
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, 1)
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, 1)
	}
	if _, ok := domain.rootPolicies[rootName]; !ok {
		domain.rootPolicies[rootName] = domain.storagePolicy
	}
	if _, ok := domain.rootBaseIDs[rootName]; !ok {
		domain.rootBaseIDs[rootName] = domain.primaryRoot
	}
	table, created := mutableRootRunLocked(domain, rootName)
	if table == nil {
		return false
	}
	entries := domain.primaryOverlay.appendEntries(make([]directBufferedRootEntry, 0, domain.primaryOverlay.len()))
	stagePrimaryDocumentCacheEntriesLocked(domain, domain.meta, domain.baseSystemRoot, entries)
	keys := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if entry.flags&node.FlagTombstone != 0 {
			table.DeleteSteal(entry.key)
		} else {
			table.SetEntrySteal(entry.key, entry.value, page.ValuePtr{}, entry.flags)
		}
		keys = append(keys, entry.key)
	}
	if domain.primaryRunIndex != nil {
		addBufferedPrimaryRunIndexKeys(domain.primaryRunIndex, keys, table)
	}
	domain.primaryOverlay = nil
	return created
}

func materializeIndexedFlushUnitPrimaryOverlays(meta CollectionMeta, flushUnit *indexedFlushUnit, units []indexedFlushUnit) ([]memtable.Table, error) {
	if flushUnit == nil || !hasIndexedFlushUnitPrimaryOverlay(units) {
		return nil, nil
	}
	rootName := collectionPrimaryRootName(meta.Name)
	if rootName == "" {
		return nil, nil
	}
	var table memtable.Table
	var materialized []memtable.Table
	orderedRootRuns := make(map[string][]memtable.Table, len(flushUnit.rootRuns)+1)
	scratch := make([]directBufferedRootEntry, 0)
	for i := range units {
		appendTableRunMap(orderedRootRuns, units[i].rootRuns)
		overlay := units[i].primaryOverlay
		if overlay == nil || overlay.len() == 0 {
			continue
		}
		table = newCollectionRootAccumulatorRunTable()
		scratch = overlay.appendEntries(scratch[:0])
		if err := applyDirectBufferedRootEntries(table, scratch); err != nil {
			resetCollectionRunTable(table)
			resetCollectionTables(materialized)
			return nil, err
		}
		if table.Len() == 0 {
			resetCollectionRunTable(table)
			table = nil
			continue
		}
		table.Freeze()
		orderedRootRuns[rootName] = append(orderedRootRuns[rootName], table)
		materialized = append(materialized, table)
		table = nil
	}
	if len(materialized) == 0 {
		return nil, nil
	}
	flushUnit.rootRuns = orderedRootRuns
	return materialized, nil
}

func mutableUniqueValueRunLocked(domain *collectionWriteDomain, indexName string) (memtable.Table, bool) {
	if domain == nil || indexName == "" {
		return nil, false
	}
	if domain.uniqueValueRuns == nil {
		domain.uniqueValueRuns = make(map[string][]memtable.Table)
	}
	if domain.uniqueValueMutableRuns == nil {
		domain.uniqueValueMutableRuns = make(map[string]memtable.Table)
	}
	if table := domain.uniqueValueMutableRuns[indexName]; table != nil {
		return table, false
	}
	table := newCollectionRootAccumulatorRunTable()
	domain.uniqueValueMutableRuns[indexName] = table
	domain.uniqueValueRuns[indexName] = append(domain.uniqueValueRuns[indexName], table)
	return table, true
}

func newCollectionRootAccumulatorRunTable() memtable.Table {
	return newFreezeSortRunTable()
}

func freezeMutableIndexedRunMapsLocked(domain *collectionWriteDomain) {
	if domain == nil || (len(domain.rootMutableRuns) == 0 && len(domain.uniqueValueMutableRuns) == 0) {
		return
	}
	for _, table := range domain.rootMutableRuns {
		if table != nil {
			table.Freeze()
		}
	}
	for _, table := range domain.uniqueValueMutableRuns {
		if table != nil {
			table.Freeze()
		}
	}
	domain.rootMutableRuns = nil
	domain.uniqueValueMutableRuns = nil
}

func detachMutableIndexedRunTablesLocked(domain *collectionWriteDomain) []memtable.Table {
	if domain == nil || (len(domain.rootMutableRuns) == 0 && len(domain.uniqueValueMutableRuns) == 0) {
		return nil
	}
	tables := make([]memtable.Table, 0, len(domain.rootMutableRuns)+len(domain.uniqueValueMutableRuns))
	for _, table := range domain.rootMutableRuns {
		if table != nil {
			tables = append(tables, table)
		}
	}
	for _, table := range domain.uniqueValueMutableRuns {
		if table != nil {
			tables = append(tables, table)
		}
	}
	domain.rootMutableRuns = nil
	domain.uniqueValueMutableRuns = nil
	return tables
}

func freezeIndexedRunTables(tables []memtable.Table) {
	for _, table := range tables {
		if table != nil {
			table.Freeze()
		}
	}
}

func freezeIndexedRunTablesObserved(tables []memtable.Table) time.Duration {
	if len(tables) == 0 {
		return 0
	}
	freezeStart := time.Now()
	freezeIndexedRunTables(tables)
	return collectionObservedElapsedSince(freezeStart)
}

// freezeIndexedRunTablesOutsideLock requires domain.mu to be held by the caller.
// It releases domain.mu for expensive table-local sort/coalesce work, then
// reacquires domain.mu before returning.
func freezeIndexedRunTablesOutsideLock(domain *collectionWriteDomain, tables []memtable.Table) (freezeDuration, lockReleased, relockWait time.Duration) {
	if len(tables) == 0 {
		return 0, 0, 0
	}
	if domain == nil {
		return freezeIndexedRunTablesObserved(tables), 0, 0
	}
	domain.beginIndexedPrepareFreezeLocked()
	prepareFinished := false
	lockHeld := true
	defer func() {
		if prepareFinished {
			return
		}
		if !lockHeld {
			domain.mu.Lock()
			lockHeld = true
		}
		domain.finishIndexedPrepareFreezeLocked()
	}()
	unlockStart := time.Now()
	domain.mu.Unlock()
	lockHeld = false
	freezeStart := time.Now()
	freezeIndexedRunTables(tables)
	freezeDuration = collectionObservedElapsedSince(freezeStart)
	relockStart := time.Now()
	domain.mu.Lock()
	lockReleased = time.Since(unlockStart)
	lockHeld = true
	relockWait = time.Since(relockStart)
	domain.finishIndexedPrepareFreezeLocked()
	prepareFinished = true
	return freezeDuration, lockReleased, relockWait
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
	var prepareWait time.Duration
	if domain.indexedPrepareFreezes > 0 {
		prepareWait = domain.waitIndexedPrepareFreezeLocked()
		if domain.count == 0 || !hasBufferedIndexedPendingWrites(domain) {
			return 0, prepareWait, 0, nil
		}
	}
	domain.indexedAutoFlushes.Add(1)
	if opts.BufferedIndexedAsyncFlush {
		rotateIndexedMutableToFlushUnitForAsyncLocked(domain)
		if opts.BufferedIndexedAsyncFlushMaxQueuedUnits > 0 && len(domain.indexedFlushUnits) >= opts.BufferedIndexedAsyncFlushMaxQueuedUnits {
			if len(domain.indexedPublishingUnits) == 0 {
				domain.indexedAsyncFlushBackpressure.Add(1)
				domain.observeIndexedFlushForcedDrain()
				flushStart := time.Now()
				err := c.flushBufferedIndexedLocked(domain)
				return collectionObservedElapsedSince(flushStart), prepareWait, 0, err
			}
			domain.indexedAsyncFlushBackpressure.Add(1)
			flushStart := time.Now()
			lockReleased := prepareWait
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
				if domain.count == 0 || len(domain.indexedFlushUnits) == 0 || !hasBufferedIndexedPendingWrites(domain) {
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
			return collectionObservedElapsedSince(flushStart), prepareWait, 0, err
		}
		return collectionObservedElapsedSince(flushStart), prepareWait, 0, nil
	}
	flushStart := time.Now()
	err := c.flushBufferedIndexedLocked(domain)
	return collectionObservedElapsedSince(flushStart), prepareWait, 0, err
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

func hasPendingRootRunsForRootLocked(domain *collectionWriteDomain, rootName string) bool {
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
	if collectionName := bufferedDomainCollectionName(domain, ""); collectionName != "" {
		if rootName == collectionPrimaryRootName(collectionName) &&
			(hasBufferedPrimaryOverlay(domain) || hasPendingIndexedPrimaryOverlay(domain)) {
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
		primaryOverlay:         cloneBufferedPrimaryOverlay(domain.primaryOverlay),
		primaryCache:           cloneBufferedPrimaryOverlay(domain.primaryCache),
		primaryCacheSystemRoot: domain.primaryCacheSystemRoot,
		primaryCacheCollection: domain.primaryCacheCollection,
		primaryCacheDirty:      domain.primaryCacheDirty,
		indexedPublishingUnits: cloneIndexedFlushUnits(domain.indexedPublishingUnits),
		indexedFlushUnits:      cloneIndexedFlushUnits(domain.indexedFlushUnits),
		primaryRunIndexActive:  domain.primaryRunIndex != nil,
		primaryWriteIndex:      cloneBufferedPrimaryWriteIndex(domain.primaryWriteIndex),
		uniqueValueRuns:        cloneTableRunMap(domain.uniqueValueRuns),
		uniqueValueMutableRuns: cloneMutableRunMap(domain.uniqueValueMutableRuns),
		rootRunCount:           domain.rootRunCount,
		indexedDeletesOnly:     domain.indexedDeletesOnly,
		pendingCommandWALFirst: domain.pendingCommandWALFirst,
		pendingCommandWALLast:  domain.pendingCommandWALLast,
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
	domain.primaryOverlay = checkpoint.primaryOverlay
	domain.primaryCache = checkpoint.primaryCache
	domain.primaryCacheSystemRoot = checkpoint.primaryCacheSystemRoot
	domain.primaryCacheCollection = checkpoint.primaryCacheCollection
	domain.primaryCacheDirty = checkpoint.primaryCacheDirty
	domain.rootRunCount = checkpoint.rootRunCount
	domain.indexedDeletesOnly = checkpoint.indexedDeletesOnly
	domain.pendingCommandWALFirst = checkpoint.pendingCommandWALFirst
	domain.pendingCommandWALLast = checkpoint.pendingCommandWALLast
	if collectionCommandWALDomainPendingLocked(domain) {
		domain.reserveCommandWALCoordinatorOwnerLocked()
	} else {
		domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
	}
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
	domain.primaryWriteIndex = checkpoint.primaryWriteIndex
	domain.uniqueValueRuns = checkpoint.uniqueValueRuns
	domain.uniqueValueMutableRuns = checkpoint.uniqueValueMutableRuns
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
			arenaRefs:       cloneArenaRefs(unit.arenaRefs),
			primaryOverlay:  cloneBufferedPrimaryOverlay(unit.primaryOverlay),
			docCount:        unit.docCount,
			byteCount:       unit.byteCount,
			rootRunCount:    unit.rootRunCount,
		}
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
	if domain.primaryWriteIndex != nil {
		domain.primaryWriteIndex = rebuildBufferedPrimaryWriteIndex(collectionName, pendingRuns, domain.writeGeneration)
		if domain.primaryWriteIndex == nil && hasBufferedPrimaryOverlay(domain) {
			domain.primaryWriteIndex = newBufferedPrimaryWriteIndex(domain.primaryOverlay.len())
		}
		if domain.primaryWriteIndex == nil && hasPendingIndexedPrimaryOverlay(domain) {
			domain.primaryWriteIndex = newBufferedPrimaryWriteIndex(0)
		}
		for i := range domain.indexedPublishingUnits {
			domain.primaryWriteIndex.addOverlay(domain.indexedPublishingUnits[i].primaryOverlay, domain.writeGeneration)
		}
		for i := range domain.indexedFlushUnits {
			domain.primaryWriteIndex.addOverlay(domain.indexedFlushUnits[i].primaryOverlay, domain.writeGeneration)
		}
		domain.primaryWriteIndex.addOverlay(domain.primaryOverlay, domain.writeGeneration)
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
		if direct := plan.directBufferedInsert; direct != nil {
			if err := rejectBufferedPrimaryConflictKeys(domain.primaryIDIndex, pendingPrimary, plan.primaryKeys); err != nil {
				return err
			}
		} else {
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
	}
	if direct := plan.directBufferedInsert; direct != nil {
		for _, uniquePlan := range direct.uniqueValueRootPlans {
			pending := pendingUniqueReservationIndexLocked(domain, uniquePlan.indexName, true)
			if pending == nil || pending.len() == 0 {
				continue
			}
			if err := rejectBufferedUniqueValuePrefixConflicts(uniquePlan.indexName, pending, uniquePlan.prefixes); err != nil {
				return err
			}
		}
		return nil
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

func rejectBufferedPrimaryConflictKeys(pendingIndex *bufferedUniqueValueIndex, pendingPrimary []memtable.Table, keys [][]byte) error {
	for _, key := range keys {
		if pendingIndex != nil && pendingIndex.contains(key) {
			return ErrDocumentExists
		}
		if pendingIndex == nil {
			if _, _, flags, found := getBufferedRunEntry(pendingPrimary, key); found && flags&node.FlagTombstone == 0 {
				return ErrDocumentExists
			}
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

func rejectBufferedUniqueValuePrefixConflicts(indexName string, pending *bufferedUniqueValueIndex, prefixes [][]byte) error {
	for _, prefix := range prefixes {
		if pending != nil && pending.contains(prefix) {
			return fmt.Errorf("%w %q", ErrUniqueIndexConflict, indexName)
		}
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

func addBufferedPrimaryIDKeys(index *bufferedUniqueValueIndex, keys [][]byte) {
	if index == nil || len(keys) == 0 {
		return
	}
	arena := make([]byte, 0, bufferedPrimaryIDArenaCap(len(keys)))
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		start := len(arena)
		arena = append(arena, key...)
		index.add(arena[start:len(arena)])
	}
	if len(arena) > 0 {
		index.arenas = append(index.arenas, arena)
	}
}

func newBufferedPrimaryWriteIndex(capacity int) *bufferedPrimaryWriteIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedPrimaryWriteIndex{generations: make(map[uint64]uint64, capacity)}
}

func (index *bufferedPrimaryWriteIndex) len() int {
	if index == nil {
		return 0
	}
	return len(index.generations)
}

func (index *bufferedPrimaryWriteIndex) addHash(hash uint64, generation uint64) {
	if index == nil {
		return
	}
	if index.generations == nil {
		index.generations = make(map[uint64]uint64)
	}
	if existing := index.generations[hash]; generation > existing {
		index.generations[hash] = generation
	}
}

func (index *bufferedPrimaryWriteIndex) addKeys(keys [][]byte, generation uint64) {
	if index == nil || len(keys) == 0 {
		return
	}
	if index.generations == nil {
		index.generations = make(map[uint64]uint64, len(keys))
	}
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		index.addHash(xxhash.Sum64(key), generation)
	}
}

func (index *bufferedPrimaryWriteIndex) addEntries(entries []directBufferedRootEntry, generation uint64) {
	if index == nil || len(entries) == 0 {
		return
	}
	if index.generations == nil {
		index.generations = make(map[uint64]uint64, len(entries))
	}
	for _, entry := range entries {
		if len(entry.key) == 0 {
			continue
		}
		index.addHash(xxhash.Sum64(entry.key), generation)
	}
}

func (index *bufferedPrimaryWriteIndex) addTable(table memtable.Table, generation uint64) error {
	if index == nil || table == nil {
		return nil
	}
	if index.generations == nil {
		index.generations = make(map[uint64]uint64, table.Len())
	}
	it := table.NewIterator(nil, nil)
	defer func() { _ = it.Close() }()
	for it.Valid() {
		key := it.UnsafeKey()
		if len(key) != 0 {
			index.addHash(xxhash.Sum64(key), generation)
		}
		it.Next()
	}
	if err := it.Error(); err != nil {
		return err
	}
	return nil
}

func (index *bufferedPrimaryWriteIndex) generation(key []byte) (uint64, bool) {
	if index == nil || len(index.generations) == 0 {
		return 0, false
	}
	generation, ok := index.generations[xxhash.Sum64(key)]
	return generation, ok
}

func cloneBufferedPrimaryWriteIndex(in *bufferedPrimaryWriteIndex) *bufferedPrimaryWriteIndex {
	if in == nil {
		return nil
	}
	out := newBufferedPrimaryWriteIndex(in.len())
	for hash, generation := range in.generations {
		out.generations[hash] = generation
	}
	if out.len() == 0 {
		return nil
	}
	return out
}

func newBufferedPrimaryOverlay(capacity int) *bufferedPrimaryOverlay {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedPrimaryOverlay{values: make(map[uint64]bufferedPrimaryOverlayRef, capacity)}
}

func (overlay *bufferedPrimaryOverlay) len() int {
	if overlay == nil {
		return 0
	}
	return overlay.count
}

func (overlay *bufferedPrimaryOverlay) addEntry(entry directBufferedRootEntry) {
	if overlay == nil || len(entry.key) == 0 {
		return
	}
	if overlay.values == nil {
		overlay.values = make(map[uint64]bufferedPrimaryOverlayRef)
	}
	ref := bufferedPrimaryOverlayRef{key: entry.key, value: entry.value, flags: entry.flags}
	hash := xxhash.Sum64(entry.key)
	if existing, ok := overlay.values[hash]; !ok {
		overlay.values[hash] = ref
		overlay.count++
		return
	} else if bytes.Equal(existing.key, entry.key) {
		overlay.values[hash] = ref
		return
	}
	if overlay.collisions == nil {
		overlay.collisions = make(map[uint64][]bufferedPrimaryOverlayRef)
	}
	bucket := overlay.collisions[hash]
	for i := len(bucket) - 1; i >= 0; i-- {
		if bytes.Equal(bucket[i].key, entry.key) {
			bucket[i] = ref
			overlay.collisions[hash] = bucket
			return
		}
	}
	overlay.collisions[hash] = append(bucket, ref)
	overlay.count++
}

func (overlay *bufferedPrimaryOverlay) addEntries(entries []directBufferedRootEntry) {
	if overlay == nil || len(entries) == 0 {
		return
	}
	for _, entry := range entries {
		overlay.addEntry(entry)
	}
}

func (overlay *bufferedPrimaryOverlay) lookupRef(key []byte) (bufferedPrimaryOverlayRef, bool) {
	if overlay == nil || len(overlay.values) == 0 {
		return bufferedPrimaryOverlayRef{}, false
	}
	hash := xxhash.Sum64(key)
	if ref, ok := overlay.values[hash]; ok && bytes.Equal(ref.key, key) {
		return ref, true
	}
	for _, ref := range overlay.collisions[hash] {
		if bytes.Equal(ref.key, key) {
			return ref, true
		}
	}
	return bufferedPrimaryOverlayRef{}, false
}

func (overlay *bufferedPrimaryOverlay) appendEntries(dst []directBufferedRootEntry) []directBufferedRootEntry {
	if overlay == nil || overlay.count == 0 {
		return dst
	}
	for _, ref := range overlay.values {
		dst = append(dst, directBufferedRootEntry{key: ref.key, value: ref.value, flags: ref.flags})
	}
	for _, bucket := range overlay.collisions {
		for _, ref := range bucket {
			dst = append(dst, directBufferedRootEntry{key: ref.key, value: ref.value, flags: ref.flags})
		}
	}
	return dst
}

func lookupIndexedFlushUnitPrimaryOverlay(units []indexedFlushUnit, key []byte) (bufferedPrimaryOverlayRef, bool) {
	for i := len(units) - 1; i >= 0; i-- {
		overlay := units[i].primaryOverlay
		if overlay == nil || overlay.len() == 0 {
			continue
		}
		if ref, ok := overlay.lookupRef(key); ok {
			return ref, true
		}
	}
	return bufferedPrimaryOverlayRef{}, false
}

func lookupPendingPrimaryOverlayLocked(domain *collectionWriteDomain, key []byte) (bufferedPrimaryOverlayRef, bool) {
	if domain == nil {
		return bufferedPrimaryOverlayRef{}, false
	}
	if ref, ok := lookupIndexedFlushUnitPrimaryOverlay(domain.indexedFlushUnits, key); ok {
		return ref, true
	}
	if ref, ok := lookupIndexedFlushUnitPrimaryOverlay(domain.indexedPublishingUnits, key); ok {
		return ref, true
	}
	return bufferedPrimaryOverlayRef{}, false
}

func cloneBufferedPrimaryOverlay(in *bufferedPrimaryOverlay) *bufferedPrimaryOverlay {
	if in == nil || in.count == 0 {
		return nil
	}
	out := newBufferedPrimaryOverlay(len(in.values))
	out.count = in.count
	for hash, ref := range in.values {
		out.values[hash] = ref
	}
	if len(in.collisions) > 0 {
		out.collisions = make(map[uint64][]bufferedPrimaryOverlayRef, len(in.collisions))
		for hash, bucket := range in.collisions {
			out.collisions[hash] = append([]bufferedPrimaryOverlayRef(nil), bucket...)
		}
	}
	return out
}

func clearPrimaryDocumentCacheLocked(domain *collectionWriteDomain) {
	if domain == nil {
		return
	}
	domain.primaryCache = nil
	domain.primaryCacheSystemRoot = 0
	domain.primaryCacheCollection = ""
	domain.primaryCacheDirty = false
}

func (c *Collection) clearWriteDomainPrimaryDocumentCache() {
	if c == nil || c.writeDomain == nil {
		return
	}
	domain := c.writeDomain
	domain.mu.Lock()
	clearPrimaryDocumentCacheLocked(domain)
	domain.mu.Unlock()
}

func retainPrimaryDocumentCacheForCatalogLocked(domain *collectionWriteDomain, meta CollectionMeta, systemRoot uint64) {
	if domain == nil || domain.primaryCache == nil || domain.primaryCache.len() == 0 {
		clearPrimaryDocumentCacheLocked(domain)
		return
	}
	if systemRoot == 0 ||
		domain.primaryCacheCollection != meta.Name ||
		!collectionMetaIndexSchemasEqual(domain.meta, meta) {
		clearPrimaryDocumentCacheLocked(domain)
		return
	}
	if domain.primaryCacheDirty || domain.primaryCacheSystemRoot == systemRoot {
		domain.primaryCacheSystemRoot = systemRoot
		domain.primaryCacheCollection = meta.Name
		if domain.count == 0 {
			domain.primaryCacheDirty = false
		}
		return
	}
	clearPrimaryDocumentCacheLocked(domain)
}

func stagePrimaryDocumentCacheEntriesLocked(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64, entries []directBufferedRootEntry) {
	if domain == nil || len(entries) == 0 || baseSystemRoot == 0 || meta.Name == "" {
		return
	}
	if domain.primaryCache != nil &&
		(domain.primaryCacheSystemRoot != baseSystemRoot || domain.primaryCacheCollection != meta.Name) {
		clearPrimaryDocumentCacheLocked(domain)
	}
	if domain.primaryCache == nil {
		domain.primaryCache = newBufferedPrimaryOverlay(len(entries))
		domain.primaryCacheSystemRoot = baseSystemRoot
		domain.primaryCacheCollection = meta.Name
	}
	domain.primaryCache.addEntries(entries)
	domain.primaryCacheDirty = true
}

// replacePrimaryDocumentCacheAfterInsert keeps only the just-committed
// no-index insert batch in the existing root-bound primary cache. The cache
// owns its bytes because InsertBatch callers may reuse their input as soon as
// the call returns.
func (c *Collection) replacePrimaryDocumentCacheAfterInsert(systemRoot uint64, meta CollectionMeta, entries []noIndexBatchEntry) {
	if c == nil || c.writeDomain == nil || systemRoot == 0 || len(entries) == 0 ||
		normalizedDocumentFormat(meta.Options.DocumentFormat) != DocumentFormatJSON ||
		columnStoreNeedsRetainedPayloadTransform(meta) {
		return
	}
	hasNativeVectorIndex := false
	for _, def := range meta.VectorIndexes {
		if vectorIndexDefinitionUsesNativeRuntime(def) {
			hasNativeVectorIndex = true
			break
		}
	}
	if !hasNativeVectorIndex {
		return
	}
	maxInt := int(^uint(0) >> 1)
	totalBytes := 0
	for _, entry := range entries {
		if len(entry.id) > maxInt-totalBytes || len(entry.document) > maxInt-totalBytes-len(entry.id) {
			return
		}
		totalBytes += len(entry.id) + len(entry.document)
	}
	arena := make([]byte, 0, totalBytes)
	cacheEntries := make([]directBufferedRootEntry, 0, len(entries))
	for _, entry := range entries {
		keyStart := len(arena)
		arena = append(arena, entry.id...)
		key := arena[keyStart:len(arena):len(arena)]
		valueStart := len(arena)
		arena = append(arena, entry.document...)
		value := arena[valueStart:len(arena):len(arena)]
		cacheEntries = append(cacheEntries, directBufferedRootEntry{
			key:   key,
			value: value,
			flags: node.FlagInline,
		})
	}

	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	clearPrimaryDocumentCacheLocked(domain)
	stagePrimaryDocumentCacheEntriesLocked(domain, meta, systemRoot, cacheEntries)
	retainPrimaryDocumentCacheForCatalogLocked(domain, meta, systemRoot)
}

func snapshotUpdateBatchPrimaryCache(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64, items []updateBatchItem) updateBatchBufferedRead {
	if domain == nil || baseSystemRoot == 0 || len(items) == 0 {
		return updateBatchBufferedRead{}
	}
	if columnStoreNeedsRetainedPayloadTransform(meta) {
		return updateBatchBufferedRead{}
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	cache := domain.primaryCache
	if cache == nil || cache.len() == 0 ||
		domain.primaryCacheSystemRoot != baseSystemRoot ||
		domain.primaryCacheCollection != meta.Name {
		return updateBatchBufferedRead{}
	}
	if domain.loaded && domain.catalog != nil && !sameCollectionMeta(domain.meta, meta) {
		return updateBatchBufferedRead{}
	}
	entries, buffer := getUpdateBatchBufferedEntrySlots(len(items))
	hits := 0
	for i, item := range items {
		ref, ok := cache.lookupRef(item.DocumentID)
		if !ok {
			continue
		}
		entries[i] = updateBatchBufferedEntry{
			value: ref.value,
			flags: ref.flags,
			found: true,
		}
		hits++
	}
	if hits == 0 {
		putUpdateBatchBufferedEntries(entries, buffer)
		return updateBatchBufferedRead{}
	}
	return updateBatchBufferedRead{
		enabled:        true,
		primaryEntries: entries,
		primaryBuffer:  buffer,
	}
}

func hasBufferedPrimaryOverlay(domain *collectionWriteDomain) bool {
	return domain != nil && domain.primaryOverlay != nil && domain.primaryOverlay.len() > 0
}

func hasIndexedFlushUnitPrimaryOverlay(units []indexedFlushUnit) bool {
	for i := range units {
		if units[i].primaryOverlay != nil && units[i].primaryOverlay.len() > 0 {
			return true
		}
	}
	return false
}

func hasPendingIndexedPrimaryOverlay(domain *collectionWriteDomain) bool {
	return domain != nil &&
		(hasIndexedFlushUnitPrimaryOverlay(domain.indexedFlushUnits) ||
			hasIndexedFlushUnitPrimaryOverlay(domain.indexedPublishingUnits))
}

func hasBufferedNoIndexTableWritesLocked(domain *collectionWriteDomain) bool {
	return domain != nil && domain.table != nil && domain.table.Len() > 0
}

func hasBufferedIndexedPendingWrites(domain *collectionWriteDomain) bool {
	return hasBufferedIndexedRootRuns(domain) || hasBufferedPrimaryOverlay(domain) || hasPendingIndexedPrimaryOverlay(domain)
}

func (index *bufferedPrimaryWriteIndex) addOverlay(overlay *bufferedPrimaryOverlay, generation uint64) {
	if index == nil || overlay == nil || overlay.len() == 0 {
		return
	}
	if index.generations == nil {
		index.generations = make(map[uint64]uint64, overlay.len())
	}
	for hash := range overlay.values {
		index.addHash(hash, generation)
	}
	for hash := range overlay.collisions {
		index.addHash(hash, generation)
	}
}

func rebuildBufferedPrimaryWriteIndex(collectionName string, runs map[string][]memtable.Table, generation uint64) *bufferedPrimaryWriteIndex {
	if collectionName == "" || len(runs) == 0 {
		return nil
	}
	tables := runs[collectionPrimaryRootName(collectionName)]
	if len(tables) == 0 {
		return nil
	}
	index := newBufferedPrimaryWriteIndex(bufferedRunLenHint(tables))
	for _, table := range tables {
		if err := index.addTable(table, generation); err != nil {
			return nil
		}
	}
	if index.len() == 0 {
		return nil
	}
	return index
}

func (domain *collectionWriteDomain) notePrimaryWriteKeysLocked(keys [][]byte, generation uint64) {
	if domain == nil || len(keys) == 0 {
		return
	}
	if domain.primaryWriteIndex == nil {
		domain.primaryWriteIndex = newBufferedPrimaryWriteIndex(len(keys))
	}
	domain.primaryWriteIndex.addKeys(keys, generation)
}

func (domain *collectionWriteDomain) notePrimaryWriteEntriesLocked(entries []directBufferedRootEntry, generation uint64) {
	if domain == nil || len(entries) == 0 {
		return
	}
	if domain.primaryWriteIndex == nil {
		domain.primaryWriteIndex = newBufferedPrimaryWriteIndex(len(entries))
	}
	domain.primaryWriteIndex.addEntries(entries, generation)
}

func (domain *collectionWriteDomain) notePrimaryWriteTableLocked(table memtable.Table, generation uint64) error {
	if domain == nil || table == nil || table.Len() == 0 {
		return nil
	}
	if domain.primaryWriteIndex == nil {
		domain.primaryWriteIndex = newBufferedPrimaryWriteIndex(table.Len())
	}
	return domain.primaryWriteIndex.addTable(table, generation)
}

func applyDirectBufferedUniqueValuePrefixes(table memtable.Table, prefixes [][]byte) error {
	if len(prefixes) == 0 {
		return nil
	}
	return applyCollectionRunEntries(table, len(prefixes), func(i int) (key, value []byte, err error) {
		return prefixes[i], nil, nil
	})
}

func newBufferedPrimaryRunIndex(capacity int) *bufferedPrimaryRunIndex {
	return newBufferedPrimaryRunIndexWithDirectEntries(capacity, false)
}

func ensureBufferedPrimaryRunIndexLocked(domain *collectionWriteDomain, capacity int) *bufferedPrimaryRunIndex {
	if domain == nil {
		return nil
	}
	if domain.primaryRunIndex != nil {
		return domain.primaryRunIndex
	}
	collectionName := bufferedDomainCollectionName(domain, "")
	if hasBufferedPrimaryRootRuns(domain, collectionName) {
		index, err := rebuildBufferedPrimaryRunIndex(collectionName, pendingIndexedRootRunMapLocked(domain))
		if err != nil {
			return nil
		}
		if index == nil {
			index = newBufferedPrimaryRunIndex(0)
		}
		domain.primaryRunIndex = index
		return domain.primaryRunIndex
	}
	domain.primaryRunIndex = newBufferedPrimaryRunIndex(max(1, capacity))
	return domain.primaryRunIndex
}

func newBufferedPrimaryRunIndexWithDirectEntries(capacity int, directEntries bool) *bufferedPrimaryRunIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &bufferedPrimaryRunIndex{
		values:        make(map[uint64]bufferedPrimaryRunRef, capacity),
		directEntries: directEntries,
	}
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

func (index *bufferedPrimaryRunIndex) lookupRef(key []byte) (bufferedPrimaryRunRef, bool) {
	if index == nil {
		return bufferedPrimaryRunRef{}, false
	}
	hash := xxhash.Sum64(key)
	if ref, ok := index.values[hash]; ok && bytes.Equal(ref.key, key) {
		return ref, true
	}
	for _, ref := range index.collisions[hash] {
		if bytes.Equal(ref.key, key) {
			return ref, true
		}
	}
	return bufferedPrimaryRunRef{}, false
}

func (index *bufferedPrimaryRunIndex) lookup(key []byte) (memtable.Table, bool) {
	ref, ok := index.lookupRef(key)
	if !ok {
		return nil, false
	}
	return ref.table, true
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
			ref := bufferedPrimaryRunRef{key: key, table: batchPrimary}
			if index.directEntries {
				value, _, flags := it.UnsafeEntry()
				ref.value = value
				ref.flags = flags
				ref.entryValid = true
			}
			index.addRef(xxhash.Sum64(key), ref)
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
	entries := bufferedRunLenHint(tables)
	runCollectionPrimaryRunIndexRebuildHook(collectionName, len(tables), entries)
	index := newBufferedPrimaryRunIndexWithDirectEntries(entries, true)
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
	if valueType == IndexValueBSONOrderedV2 {
		return bsonIndexKeyValuePrefixV2(key)
	}
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
	return cloneCollectionRunTableFromIterator(table, it)
}

func cloneCollectionRunTableFromIterator(table memtable.Table, it iterator.UnsafeIterator) (memtable.Table, error) {
	for it.Valid() {
		value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
		setCollectionRunEntryStealWithRevision(table, bytes.Clone(it.UnsafeKey()), bytes.Clone(value), ptr, flags, revision)
		it.Next()
	}
	if err := it.Error(); err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	table.Freeze()
	return table, nil
}

type collectionRunTableSnapshot struct {
	iter iterator.UnsafeIterator
	len  int
}

func closeCollectionRunTableSnapshots(snapshots []collectionRunTableSnapshot) {
	for _, snapshot := range snapshots {
		if snapshot.iter != nil {
			_ = snapshot.iter.Close()
		}
	}
}

func cloneCollectionRunTableSnapshots(snapshots []collectionRunTableSnapshot) ([]memtable.Table, error) {
	if len(snapshots) == 0 {
		return nil, nil
	}
	defer closeCollectionRunTableSnapshots(snapshots)
	out := make([]memtable.Table, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.iter == nil {
			out = append(out, nil)
			continue
		}
		table := newCollectionRunTable(max(0, snapshot.len))
		cloned, err := cloneCollectionRunTableFromIterator(table, snapshot.iter)
		if err != nil {
			resetCollectionTables(out)
			return nil, err
		}
		out = append(out, cloned)
	}
	return out, nil
}

func bufferedTemplateRunSnapshotsLocked(domain *collectionWriteDomain, collectionName string) []collectionRunTableSnapshot {
	rootName := collectionTemplateRootName(collectionName)
	pendingRuns := pendingIndexedRootRunsLocked(domain, rootName)
	if len(pendingRuns) == 0 {
		return nil
	}
	snapshots := make([]collectionRunTableSnapshot, 0, len(pendingRuns))
	for _, run := range pendingRuns {
		if run == nil {
			continue
		}
		// Create the iterator while domain.mu is held so async publish completion
		// cannot remove and reset the run before NewIterator pins or snapshots it.
		// Frozen freeze-sort iterators hold the run table read lock until Close;
		// mutable iterators copy entries. The actual entry clone happens after
		// domain.mu is released.
		length := run.Len()
		snapshots = append(snapshots, collectionRunTableSnapshot{
			iter: run.NewIterator(nil, nil),
			len:  length,
		})
	}
	return snapshots
}

func cloneBufferedTemplateV1Runs(domain *collectionWriteDomain, collectionName string) ([]memtable.Table, error) {
	if domain == nil || collectionName == "" {
		return nil, nil
	}
	domain.mu.RLock()
	snapshots := bufferedTemplateRunSnapshotsLocked(domain, collectionName)
	domain.mu.RUnlock()
	return cloneCollectionRunTableSnapshots(snapshots)
}

type bufferedRootRunHeapItem struct {
	idx      int
	priority int
	key      []byte
}

type bufferedRootRunHeap struct {
	items   []bufferedRootRunHeapItem
	reverse bool
}

func (h bufferedRootRunHeap) Len() int { return len(h.items) }

func (h bufferedRootRunHeap) Less(i, j int) bool {
	if cmp := bytes.Compare(h.items[i].key, h.items[j].key); cmp != 0 {
		if h.reverse {
			return cmp > 0
		}
		return cmp < 0
	}
	return h.items[i].priority < h.items[j].priority
}

func (h bufferedRootRunHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *bufferedRootRunHeap) push(item bufferedRootRunHeapItem) {
	h.items = append(h.items, item)
	h.up(len(h.items) - 1)
}

func (h *bufferedRootRunHeap) init() {
	n := len(h.items)
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

func (h *bufferedRootRunHeap) pop() bufferedRootRunHeapItem {
	old := h.items
	n := len(old)
	if n == 0 {
		return bufferedRootRunHeapItem{}
	}
	h.Swap(0, n-1)
	h.down(0, n-1)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

func (h bufferedRootRunHeap) peek() *bufferedRootRunHeapItem {
	if len(h.items) == 0 {
		return nil
	}
	return &h.items[0]
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
	reverse            bool
	stableUnsafeSlices bool
	lenHint            int
	start              []byte
	end                []byte
	closed             bool
	firstErr           error
	maxInspected       int
	inspected          int
	workCapped         bool
	onInspected        func(int)
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
	return newBufferedRootRunsIteratorWithDeletedDirection(runs, start, end, includeDeleted, false)
}

func newBufferedRootRunsReverseIteratorWithDeleted(runs []memtable.Table, start, end []byte, includeDeleted bool) iterator.UnsafeIterator {
	return newBufferedRootRunsIteratorWithDeletedDirection(runs, start, end, includeDeleted, true)
}

func newBufferedRootRunsIteratorWithDeletedDirection(runs []memtable.Table, start, end []byte, includeDeleted, reverse bool) iterator.UnsafeIterator {
	return newBufferedRootRunsIteratorWithDeletedDirectionWorkCap(runs, start, end, includeDeleted, reverse, 0)
}

func newBufferedRootRunsIteratorWithDeletedDirectionWorkCap(runs []memtable.Table, start, end []byte, includeDeleted, reverse bool, maxInspected int) iterator.UnsafeIterator {
	return newBufferedRootRunsIteratorWithDeletedDirectionWorkCapAndInspect(runs, start, end, includeDeleted, reverse, maxInspected, nil)
}

func newBufferedRootRunsIteratorWithDeletedDirectionWorkCapAndInspect(runs []memtable.Table, start, end []byte, includeDeleted, reverse bool, maxInspected int, onInspected func(int)) iterator.UnsafeIterator {
	if maxInspected > 0 && len(runs) > maxInspected {
		return &bufferedRootRunsIterator{firstErr: errCollectionIndexScanWorkCap, workCapped: true}
	}
	if maxInspected == 0 && len(runs) == 1 && includeDeleted && runs[0] != nil {
		if reverse {
			return runs[0].NewReverseIterator(start, end)
		}
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
			iter: func() iterator.UnsafeIterator {
				if reverse {
					return run.NewReverseIterator(start, end)
				}
				return run.NewIterator(start, end)
			}(),
			priority: len(runs) - 1 - i,
			lenHint:  lenHint,
		})
	}
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect(sources, start, end, includeDeleted, stableUnsafeSlices, reverse, maxInspected, onInspected)
}

func newBufferedRootRunIteratorSourcesIteratorWithDeleted(sources []bufferedRootRunIteratorSource, start, end []byte, includeDeleted, stableUnsafeSlices bool) iterator.UnsafeIterator {
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirection(sources, start, end, includeDeleted, stableUnsafeSlices, false)
}

func newBufferedRootRunIteratorSourcesIteratorWithDeletedDirection(sources []bufferedRootRunIteratorSource, start, end []byte, includeDeleted, stableUnsafeSlices, reverse bool) iterator.UnsafeIterator {
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCap(sources, start, end, includeDeleted, stableUnsafeSlices, reverse, 0)
}

func newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCap(sources []bufferedRootRunIteratorSource, start, end []byte, includeDeleted, stableUnsafeSlices, reverse bool, maxInspected int) iterator.UnsafeIterator {
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect(sources, start, end, includeDeleted, stableUnsafeSlices, reverse, maxInspected, nil)
}

func newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect(sources []bufferedRootRunIteratorSource, start, end []byte, includeDeleted, stableUnsafeSlices, reverse bool, maxInspected int, onInspected func(int)) iterator.UnsafeIterator {
	// Source construction is physical work too: a direct bounded scan must not
	// open an unbounded stack of overlay roots before the capped merge gets a
	// chance to reject it. A source has an initially-positioned physical entry,
	// so the same budget is a safe upper bound for both source initialization
	// and subsequently inspected entries.
	if maxInspected > 0 && len(sources) > maxInspected {
		for _, source := range sources {
			if source.iter != nil {
				_ = source.iter.Close()
			}
		}
		return &bufferedRootRunsIterator{firstErr: errCollectionIndexScanWorkCap, workCapped: true}
	}
	it := &bufferedRootRunsIterator{
		iters:              make([]iterator.UnsafeIterator, 0, len(sources)),
		heap:               bufferedRootRunHeap{items: make([]bufferedRootRunHeapItem, 0, len(sources)), reverse: reverse},
		includeDeleted:     includeDeleted,
		reverse:            reverse,
		stableUnsafeSlices: stableUnsafeSlices,
		start:              start,
		end:                end,
		maxInspected:       maxInspected,
		onInspected:        onInspected,
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
		if source.iter.Valid() && it.inspect(1) {
			it.heap.items = append(it.heap.items, bufferedRootRunHeapItem{
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
	if !it.reverse && it.start != nil && bytes.Compare(key, it.start) < 0 {
		key = it.start
	}
	it.heap.items = it.heap.items[:0]
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
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *bufferedRootRunsIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision
	}
	return iterator.UnsafeEntryWithRevision(it.iters[it.cur.idx])
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
	it.heap.items = nil
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
	for !it.workCapped && it.heap.Len() > 0 {
		top := it.heap.pop()
		key := top.key
		if !it.reverse && it.end != nil && bytes.Compare(key, it.end) >= 0 {
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
	if !it.inspect(1) {
		return
	}
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
	if !it.inspect(1) {
		return false
	}
	source.Next()
	if !source.Valid() {
		if err := source.Error(); err != nil && it.firstErr == nil {
			it.firstErr = err
		}
		return false
	}
	item.key = source.UnsafeKey()
	if !it.reverse && it.end != nil && bytes.Compare(item.key, it.end) >= 0 {
		return false
	}
	if !it.includeDeleted && source.IsDeleted() {
		it.heap.push(item)
		return false
	}
	next := it.heap.peek()
	if next == nil || (!it.reverse && bytes.Compare(item.key, next.key) < 0) || (it.reverse && bytes.Compare(item.key, next.key) > 0) {
		it.cur = item
		it.hasCur = true
		it.valid = true
		return true
	}
	it.heap.push(item)
	return false
}

func (it *bufferedRootRunsIterator) inspect(count int) bool {
	if it.maxInspected <= 0 || count <= it.maxInspected-it.inspected {
		it.inspected += count
		if it.onInspected != nil {
			it.onInspected(count)
		}
		return true
	}
	it.workCapped = true
	it.firstErr = errCollectionIndexScanWorkCap
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
	if c == nil || c.db == nil || domain == nil {
		return nil, nil
	}
	domain.waitIndexedPrepareFreezeLocked()
	if domain.count == 0 || !hasBufferedIndexedPendingWrites(domain) {
		return nil, nil
	}
	if len(domain.indexedFlushUnits) == 0 && len(domain.rootRuns) == 0 && !hasBufferedPrimaryOverlay(domain) {
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
			return errBufferedRootBaseMismatch(meta.Name, rootName)
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

	rotateIndexedMutableToFlushUnitForAsyncLocked(domain)
	units := domain.indexedFlushUnits
	flushUnit := mergedIndexedFlushUnits(units)
	rootNames := orderedBufferedRootNames(meta, flushUnit.rootRuns)
	if hasIndexedFlushUnitPrimaryOverlay(units) {
		rootNames = appendOrderedRootName(rootNames, collectionPrimaryRootName(meta.Name))
	}
	if len(rootNames) == 0 {
		_ = pin.Close()
		work.pin = nil
		domain.indexedFlushUnits = nil
		domain.rootMutableRuns = nil
		domain.rootValueArenas = nil
		domain.primaryOverlay = nil
		domain.count = 0
		domain.indexedDeletesOnly = false
		domain.bufferedBytes = 0
		domain.mutableCount = 0
		domain.mutableBytes = 0
		domain.primaryWriteIndex = nil
		return nil, nil
	}
	rootBaseIDs := make(map[string]uint64, len(rootNames))
	rootOverlays := make(map[string][]uint64, len(rootNames))
	for _, rootName := range rootNames {
		baseRoot, ok := flushUnit.rootBaseIDs[rootName]
		if !ok {
			err = fmt.Errorf("collections: buffered indexed flush missing base root for %q", rootName)
			return nil, err
		}
		rootBaseIDs[rootName] = baseRoot
		rootOverlays[rootName] = append([]uint64(nil), catalog.overlayRootIDs(rootName)...)
	}
	work.baseSystemRoot = snapshotSystemRoot(pin)
	work.baseCommitSeq = snapshotCommitSeq(pin)
	work.units = units
	work.flushUnit = flushUnit
	work.rootNames = rootNames
	work.rootBaseIDs = rootBaseIDs
	work.rootOverlays = rootOverlays
	work.docCount = flushUnit.docCount
	work.byteCount = flushUnit.byteCount
	work.rootRunCount = indexedFlushUnitRootRunCount(flushUnit)
	work.rootCount = len(rootNames)

	domain.indexedPublishingUnits = append(domain.indexedPublishingUnits, units...)
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
	overlayMaterializeStart := time.Now()
	materializedPrimaryRuns, err := materializeIndexedFlushUnitPrimaryOverlays(work.meta, &work.flushUnit, work.units)
	overlayMaterializeElapsed := time.Duration(0)
	if len(materializedPrimaryRuns) > 0 || err != nil {
		overlayMaterializeElapsed = collectionObservedElapsedSince(overlayMaterializeStart)
	}
	if len(materializedPrimaryRuns) > 0 {
		defer resetCollectionTables(materializedPrimaryRuns)
	}
	if err != nil {
		return c.completePreparedIndexedFlush(work, 0, nil, err, overlayMaterializeElapsed, overlayMaterializeElapsed, 0)
	}
	publishRootRuns, cleanupPointerizedRuns, err := pointerizeCollectionDataRootRunMapValues(c.db, work.meta, work.flushUnit.rootRuns)
	if err != nil {
		return c.completePreparedIndexedFlush(work, 0, nil, err, overlayMaterializeElapsed, overlayMaterializeElapsed, 0)
	}
	defer cleanupPointerizedRuns()
	if collectionMetaUsesIndexedOverlayRoots(work.meta) {
		materializeStart := time.Now()
		rootOverlayFilters, err := buildCollectionRootOverlayFilters(work.rootNames, work.flushUnit.rootRuns, work.rootOverlays, work.catalog.rootOverlayFilters)
		if err != nil {
			materializeElapsed := overlayMaterializeElapsed + collectionObservedElapsedSince(materializeStart)
			return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
		}
		work.rootOverlayFilters = rootOverlayFilters
		ordered, cleanupDeltas, err := buildBufferedRootOverlayDeltaBatchPublishInputs(work.rootNames, publishRootRuns, work.flushUnit.rootPolicies, work.rootOverlays)
		if err != nil {
			materializeElapsed := overlayMaterializeElapsed + collectionObservedElapsedSince(materializeStart)
			return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
		}
		work.rootDeltaStats = collectionRootDeltaPlanStatsFromOrdered(work.meta.Name, work.rootNames, ordered)
		materializeElapsed := overlayMaterializeElapsed + collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		newSystemRoot, rootIDs, publishErr := c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootOverlayDescriptorSystemDeltaIteratorForMeta(work.meta, work.baseCommitSeq, work.baseSystemRoot, work.rootNames, work.rootBaseIDs, work.rootOverlays, rootIDs)
		})
		publishElapsed := collectionObservedElapsedSince(publishStart)
		cleanupDeltas()
		if publishErr == nil && len(rootIDs) != len(work.rootNames) {
			publishErr = unexpectedOrderedRootCountError(work.meta.Name, len(work.rootNames), len(rootIDs))
		}
		completeErr := c.completePreparedIndexedFlush(work, newSystemRoot, rootIDs, publishErr, materializeElapsed+publishElapsed, materializeElapsed, publishElapsed)
		if completeErr != nil {
			return completeErr
		}
		if publishErr != nil {
			return publishErr
		}
		return nil
	}
	materializeStart := time.Now()
	ordered, cleanupDeltas, err := buildBufferedRootDeltaBatchPublishInputs(work.rootNames, publishRootRuns, work.rootBaseIDs, work.flushUnit.rootPolicies)
	if err != nil {
		materializeElapsed := overlayMaterializeElapsed + collectionObservedElapsedSince(materializeStart)
		return c.completePreparedIndexedFlush(work, 0, nil, err, materializeElapsed, materializeElapsed, 0)
	}
	work.rootDeltaStats = collectionRootDeltaPlanStatsFromOrdered(work.meta.Name, work.rootNames, ordered)
	materializeElapsed := overlayMaterializeElapsed + collectionObservedElapsedSince(materializeStart)
	publishStart := time.Now()
	newSystemRoot, rootIDs, publishErr := c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(work.meta, work.baseCommitSeq, work.baseSystemRoot, work.rootNames, work.rootBaseIDs, rootIDs)
	})
	publishElapsed := collectionObservedElapsedSince(publishStart)
	cleanupDeltas()
	if publishErr == nil && len(rootIDs) != len(work.rootNames) {
		publishErr = unexpectedOrderedRootCountError(work.meta.Name, len(work.rootNames), len(rootIDs))
	}
	completeErr := c.completePreparedIndexedFlush(work, newSystemRoot, rootIDs, publishErr, materializeElapsed+publishElapsed, materializeElapsed, publishElapsed)
	if completeErr != nil {
		return completeErr
	}
	if publishErr != nil {
		return publishErr
	}
	return nil
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
		SpanNativeRoute:           backenddb.OrderedRootSpanNativeRouteCollectionBufferedRoots,
		SpanNativeContext:         "collection buffered root delta publish",
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
			BaseRoot:          baseRoot,
			Delta:             delta,
			StoragePolicy:     policies[i],
			SpanNativeRoute:   backenddb.OrderedRootSpanNativeRouteCollectionBufferedRoots,
			SpanNativeContext: "collection root delta batch publish",
		})
	}
	return ordered, cleanup, nil
}

func coalesceCollectionRootDeltaTables(collectionName string, rootNames []string, tables []memtable.Table, policies []backenddb.OrderedRootStoragePolicy) ([]string, []memtable.Table, []backenddb.OrderedRootStoragePolicy, func(), error) {
	if len(rootNames) != len(tables) || len(rootNames) != len(policies) {
		return nil, nil, nil, func() {}, fmt.Errorf("collections: collection %q invalid delta lengths roots=%d tables=%d policies=%d", collectionName, len(rootNames), len(tables), len(policies))
	}
	if len(rootNames) < 2 {
		return rootNames, tables, policies, func() {}, nil
	}
	firstByRoot := make(map[string]int, len(rootNames))
	duplicate := false
	for i, rootName := range rootNames {
		if first, ok := firstByRoot[rootName]; ok {
			if policies[i] != policies[first] {
				return nil, nil, nil, func() {}, fmt.Errorf("collections: collection %q root %q has mismatched delta policies %d and %d", collectionName, rootName, policies[first], policies[i])
			}
			duplicate = true
			continue
		}
		firstByRoot[rootName] = i
	}
	if !duplicate {
		return rootNames, tables, policies, func() {}, nil
	}

	type rootDeltaGroup struct {
		name    string
		policy  backenddb.OrderedRootStoragePolicy
		indexes []int
	}
	groups := make([]rootDeltaGroup, 0, len(firstByRoot))
	groupByRoot := make(map[string]int, len(firstByRoot))
	for i, rootName := range rootNames {
		if groupIdx, ok := groupByRoot[rootName]; ok {
			groups[groupIdx].indexes = append(groups[groupIdx].indexes, i)
			continue
		}
		groupByRoot[rootName] = len(groups)
		groups = append(groups, rootDeltaGroup{
			name:    rootName,
			policy:  policies[i],
			indexes: []int{i},
		})
	}

	outNames := make([]string, 0, len(groups))
	outTables := make([]memtable.Table, 0, len(groups))
	outPolicies := make([]backenddb.OrderedRootStoragePolicy, 0, len(groups))
	var coalesced []memtable.Table
	cleanup := func() {
		resetCollectionTables(coalesced)
	}
	for _, group := range groups {
		outNames = append(outNames, group.name)
		outPolicies = append(outPolicies, group.policy)
		if len(group.indexes) == 1 {
			outTables = append(outTables, tables[group.indexes[0]])
			continue
		}
		groupTables := make([]memtable.Table, 0, len(group.indexes))
		for _, idx := range group.indexes {
			groupTables = append(groupTables, tables[idx])
		}
		merged, err := mergeCollectionRootDeltaTables(groupTables)
		if err != nil {
			cleanup()
			return nil, nil, nil, func() {}, err
		}
		coalesced = append(coalesced, merged)
		outTables = append(outTables, merged)
	}
	return outNames, outTables, outPolicies, cleanup, nil
}

func mergeCollectionRootDeltaTables(tables []memtable.Table) (memtable.Table, error) {
	total := 0
	for _, table := range tables {
		if table != nil {
			total += table.Len()
		}
	}
	merged := newCollectionRunTable(total)
	for _, table := range tables {
		if table == nil || table.Len() == 0 {
			continue
		}
		iter := table.NewIterator(nil, nil)
		for iter.Valid() {
			key := bytes.Clone(iter.UnsafeKey())
			value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)
			if flags&node.FlagTombstone != 0 {
				deleteCollectionRunEntryStealWithRevision(merged, key, revision)
			} else {
				setCollectionRunEntryStealWithRevision(merged, key, bytes.Clone(value), ptr, flags, revision)
			}
			iter.Next()
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			resetCollectionRunTable(merged)
			return nil, err
		}
		if err := iter.Close(); err != nil {
			resetCollectionRunTable(merged)
			return nil, err
		}
	}
	merged.Freeze()
	return merged, nil
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
		removed, owned := removeIndexedPublishingWorkUnitsLocked(domain, work.units)
		if !owned {
			err := errors.Join(errIndexedFlushLostOwnership, publishErr)
			domain.indexedFlushLostOwnership.Add(1)
			domain.observeIndexedFlush(len(work.units), work.docCount, work.byteCount, work.rootRunCount, work.rootCount, observedElapsed(), materializeElapsed, publishElapsed, err)
			return err
		}
		if len(removed) > 0 {
			domain.indexedFlushRequeues.Add(1)
			domain.indexedFlushRequeuedUnits.Add(uint64(len(removed)))
		}
		domain.indexedFlushUnits = append(removed, domain.indexedFlushUnits...)
		rebuildBufferedPendingIndexesLocked(domain, work.meta.Name, preservePrimaryRunIndex)
		domain.observeIndexedFlush(len(work.units), work.docCount, work.byteCount, work.rootRunCount, work.rootCount, observedElapsed(), materializeElapsed, publishElapsed, publishErr)
		return publishErr
	}
	baseCatalog := domain.catalog
	if baseCatalog == nil {
		baseCatalog = work.catalog
	}
	overlayPublish := collectionMetaUsesIndexedOverlayRoots(work.meta)
	nextCatalog := cloneCatalogWithRootUpdates(baseCatalog, work.meta, work.rootNames, rootIDs)
	if overlayPublish {
		nextCatalog = cloneCatalogWithRootOverlays(baseCatalog, work.meta, work.rootNames, rootIDs)
		nextCatalog = cloneCatalogWithRootOverlayFilters(nextCatalog, work.rootNames, rootIDs, work.rootOverlayFilters)
	}
	oldPublishing, owned := removeIndexedPublishingWorkUnitsLocked(domain, work.units)
	if !owned {
		domain.indexedFlushLostOwnership.Add(1)
		domain.observeIndexedFlush(len(work.units), work.docCount, work.byteCount, work.rootRunCount, work.rootCount, observedElapsed(), materializeElapsed, publishElapsed, errIndexedFlushLostOwnership)
		return errIndexedFlushLostOwnership
	}
	domain.loaded = true
	domain.meta = work.meta
	domain.catalog = nextCatalog
	domain.baseCommitSeq = c.commitSeqForSystemRoot(newSystemRoot)
	domain.baseSystemRoot = newSystemRoot
	domain.primaryRoot = nextCatalog.rootID(collectionPrimaryRootName(work.meta.Name))
	domain.count = subtractNonNegativeInt(domain.count, work.docCount)
	domain.bufferedBytes = subtractNonNegativeInt64(domain.bufferedBytes, work.byteCount)
	retainPrimaryDocumentCacheForCatalogLocked(domain, work.meta, newSystemRoot)
	domain.clearIndexedAsyncFlushError()
	if !overlayPublish {
		retargetPendingIndexedRootBaseIDsLocked(domain, work.rootNames, work.rootBaseIDs, rootIDs)
	}
	rebuildBufferedPendingIndexesLocked(domain, work.meta.Name, preservePrimaryRunIndex)
	c.meta = work.meta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	resetIndexedFlushUnits(oldPublishing)
	domain.observeIndexedFlush(len(work.units), work.docCount, work.byteCount, work.rootRunCount, work.rootCount, observedElapsed(), materializeElapsed, publishElapsed, nil)
	domain.observeRootDeltaPlan(work.rootDeltaStats)
	if slices.Contains(work.rootNames, collectionPrimaryRootName(work.meta.Name)) {
		return c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked()
	}
	return nil
}

func (c *Collection) flushBufferedIndexedLocked(domain *collectionWriteDomain) (err error) {
	return c.flushBufferedIndexedLockedWithRawPublishState(domain, false)
}

func (c *Collection) flushBufferedIndexedLockedWithRawPublishState(domain *collectionWriteDomain, rawPublishLocked bool) (err error) {
	if domain == nil {
		return nil
	}
	domain.waitIndexedPrepareFreezeLocked()
	materializePrimaryOverlayLocked(domain)
	if domain.count == 0 || !hasBufferedIndexedRootRuns(domain) {
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
			return errBufferedRootBaseMismatch(meta.Name, rootName)
		}
		return nil
	}); err != nil {
		return err
	}

	rotateIndexedMutableToFlushUnitLocked(domain)
	flushUnit, materializedPrimaryRuns, err := mergedIndexedFlushUnitForSyncLocked(meta, domain)
	if err != nil {
		return err
	}
	if len(materializedPrimaryRuns) > 0 {
		defer resetCollectionTables(materializedPrimaryRuns)
	}
	rootNames := orderedBufferedRootNames(meta, flushUnit.rootRuns)
	if len(rootNames) == 0 {
		domain.indexedFlushUnits = nil
		domain.rootMutableRuns = nil
		domain.rootValueArenas = nil
		domain.primaryOverlay = nil
		domain.count = 0
		domain.indexedDeletesOnly = false
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
	publishRootRuns, cleanupPointerizedRuns, err := pointerizeCollectionDataRootRunMapValues(c.db, meta, flushUnit.rootRuns)
	if err != nil {
		return err
	}
	defer cleanupPointerizedRuns()
	var newSystemRoot uint64
	var rootIDs []uint64
	var rootOverlayFilters map[string]collectionRootOverlayFilter
	var commandWALAppliedLSN uint64
	publishWithCommandWALCoordinator := func(fn func(*backenddb.CommandWALIntent) error) error {
		unlock, err := c.lockCommandWALFlushPublishCoordinator(domain)
		if err != nil {
			return err
		}
		defer unlock()
		commandWALIntent, appliedLSN, err := domain.pendingCommandWALCoverageIntentLocked(c.db)
		if err != nil {
			return err
		}
		commandWALAppliedLSN = appliedLSN
		return fn(commandWALIntent)
	}
	if collectionMetaUsesIndexedOverlayRoots(meta) {
		materializeStart := time.Now()
		rootOverlayFilters, err = buildCollectionRootOverlayFilters(rootNames, flushUnit.rootRuns, rootOverlays, catalog.rootOverlayFilters)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		var ordered []backenddb.OrderedRootDeltaBatchPublishInput
		var cleanupDeltas func()
		ordered, cleanupDeltas, err = buildBufferedRootOverlayDeltaBatchPublishInputs(rootNames, publishRootRuns, flushUnit.rootPolicies, rootOverlays)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		rootDeltaStats := collectionRootDeltaPlanStatsFromOrdered(meta.Name, rootNames, ordered)
		materializeElapsed = collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		err = publishWithCommandWALCoordinator(func(commandWALIntent *backenddb.CommandWALIntent) error {
			if commandWALIntent != nil {
				newSystemRoot, rootIDs, err = c.publishBufferedOrderedRootDeltaBatchGroupWithCommandWAL(ordered, commandWALIntent, rawPublishLocked, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
					return c.buildRootOverlayDescriptorSystemDeltaIteratorForMeta(meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootOverlays, rootIDs)
				})
			} else {
				newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
					return c.buildRootOverlayDescriptorSystemDeltaIteratorForMeta(meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootOverlays, rootIDs)
				})
			}
			return err
		})
		publishElapsed = collectionObservedElapsedSince(publishStart)
		cleanupDeltas()
		if err == nil {
			domain.observeRootDeltaPlan(rootDeltaStats)
		}
	} else {
		materializeStart := time.Now()
		var ordered []backenddb.OrderedRootDeltaBatchPublishInput
		var cleanupDeltas func()
		ordered, cleanupDeltas, err = buildBufferedRootDeltaBatchPublishInputs(rootNames, publishRootRuns, flushUnit.rootBaseIDs, flushUnit.rootPolicies)
		if err != nil {
			materializeElapsed = collectionObservedElapsedSince(materializeStart)
			return err
		}
		rootDeltaStats := collectionRootDeltaPlanStatsFromOrdered(meta.Name, rootNames, ordered)
		materializeElapsed = collectionObservedElapsedSince(materializeStart)
		publishStart := time.Now()
		err = publishWithCommandWALCoordinator(func(commandWALIntent *backenddb.CommandWALIntent) error {
			if commandWALIntent != nil {
				newSystemRoot, rootIDs, err = c.publishBufferedOrderedRootDeltaBatchGroupWithCommandWAL(ordered, commandWALIntent, rawPublishLocked, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
					return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
				})
			} else {
				newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
					return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
				})
			}
			return err
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
	retainPrimaryDocumentCacheForCatalogLocked(domain, meta, newSystemRoot)
	domain.indexedFlushUnits = nil
	domain.rootRuns = nil
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.rootValueArenas = nil
	domain.primaryOverlay = nil
	domain.rootRunCount = 0
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	domain.primaryWriteIndex = nil
	oldUniqueValueRuns := domain.uniqueValueRuns
	domain.uniqueValueRuns = nil
	domain.uniqueValueMutableRuns = nil
	domain.uniqueValueIndex = nil
	domain.count = 0
	domain.indexedDeletesOnly = false
	domain.bufferedBytes = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	if commandWALAppliedLSN != 0 {
		domain.clearPendingCommandWALThroughLocked(commandWALAppliedLSN)
		domain.clearCommandWALCoordinatorOwnerIfNoPendingLocked()
	}
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

func (c *Collection) publishBufferedOrderedRootDeltaBatchGroupWithCommandWAL(ordered []backenddb.OrderedRootDeltaBatchPublishInput, commandWALIntent *backenddb.CommandWALIntent, rawPublishLocked bool, buildSystemDeltaIter backenddb.OrderedRootGroupSystemBuilder) (uint64, []uint64, error) {
	if c == nil || c.db == nil {
		return 0, nil, errCollectionDBNil
	}
	if rawPublishLocked || c.commandWALRawPublishLocked {
		return c.db.PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, buildSystemDeltaIter)
	}
	return c.db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, buildSystemDeltaIter)
}

func rotateIndexedMutableToFlushUnitLocked(domain *collectionWriteDomain) bool {
	materializePrimaryOverlayLocked(domain)
	if domain == nil || len(domain.rootRuns) == 0 {
		return false
	}
	freezeMutableIndexedRunMapsLocked(domain)
	unit := indexedFlushUnit{
		rootRuns:        domain.rootRuns,
		rootPolicies:    domain.rootPolicies,
		rootBaseIDs:     domain.rootBaseIDs,
		uniqueValueRuns: domain.uniqueValueRuns,
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
	domain.uniqueValueMutableRuns = nil
	domain.rootValueArenas = nil
	domain.rootRunCount = 0
	domain.mutableCount = 0
	domain.mutableBytes = 0
	return true
}

func rotateIndexedMutableToFlushUnitForAsyncLocked(domain *collectionWriteDomain) bool {
	if domain == nil || (len(domain.rootRuns) == 0 && !hasBufferedPrimaryOverlay(domain)) {
		return false
	}
	freezeMutableIndexedRunMapsLocked(domain)
	rootRunCount := domain.rootRunCount
	if hasBufferedPrimaryOverlay(domain) {
		rootRunCount = saturatingAddNonNegativeInt(rootRunCount, 1)
	}
	unit := indexedFlushUnit{
		rootRuns:        domain.rootRuns,
		rootPolicies:    domain.rootPolicies,
		rootBaseIDs:     domain.rootBaseIDs,
		uniqueValueRuns: domain.uniqueValueRuns,
		arenaRefs:       domain.rootValueArenas,
		primaryOverlay:  domain.primaryOverlay,
		docCount:        domain.mutableCount,
		byteCount:       domain.mutableBytes,
		rootRunCount:    rootRunCount,
	}
	domain.indexedFlushUnits = append(domain.indexedFlushUnits, unit)
	domain.rootRuns = nil
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueMutableRuns = nil
	domain.rootValueArenas = nil
	domain.primaryOverlay = nil
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
	removed := domain.indexedPublishingUnits[:n]
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
	if domain == nil || len(domain.indexedPublishingUnits) < len(units) {
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
	return sameTableRunMap(a.rootRuns, b.rootRuns) &&
		sameTableRunMap(a.uniqueValueRuns, b.uniqueValueRuns) &&
		a.primaryOverlay == b.primaryOverlay
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
	return unit
}

func mergedIndexedFlushUnitForSyncLocked(meta CollectionMeta, domain *collectionWriteDomain) (indexedFlushUnit, []memtable.Table, error) {
	if domain == nil {
		return indexedFlushUnit{}, nil, nil
	}
	if !hasIndexedFlushUnitPrimaryOverlay(domain.indexedFlushUnits) {
		return mergedIndexedFlushUnitLocked(domain), nil, nil
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
	materializedPrimaryRuns, err := materializeIndexedFlushUnitPrimaryOverlays(meta, &unit, domain.indexedFlushUnits)
	if err != nil {
		return indexedFlushUnit{}, nil, err
	}
	mergeIndexedFlushUnit(&unit, indexedFlushUnit{
		rootRuns:        domain.rootRuns,
		rootPolicies:    domain.rootPolicies,
		rootBaseIDs:     domain.rootBaseIDs,
		uniqueValueRuns: domain.uniqueValueRuns,
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
	return unit, materializedPrimaryRuns, nil
}

func mergeIndexedFlushUnit(dst *indexedFlushUnit, src indexedFlushUnit) {
	if dst == nil {
		return
	}
	appendTableRunMap(dst.rootRuns, src.rootRuns)
	appendTableRunMap(dst.uniqueValueRuns, src.uniqueValueRuns)
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

func appendOrderedRootName(rootNames []string, rootName string) []string {
	if rootName == "" {
		return rootNames
	}
	for _, existing := range rootNames {
		if existing == rootName {
			return rootNames
		}
	}
	return append(rootNames, rootName)
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
	if len(c.meta.Indexes) > 0 || len(c.meta.VectorIndexes) > 0 || len(c.meta.TextIndexes) > 0 {
		_ = snap.Close()
		return c.insertOneViaBatch(id, document)
	}
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, c.meta)
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
	table := newCollectionRunTable(1)
	setCollectionRunValue(table, resultID, bytes.Clone(document))
	table.Freeze()
	publishTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(c.db, c.meta, rootName, table)
	if err != nil {
		resetCollectionRunTable(table)
		return nil, err
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	iter := publishTable.NewIterator(nil, nil)
	defer func() {
		_ = iter.Close()
		resetCollectionRunTable(table)
	}()

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
		if errors.Is(err, ErrCommitAmbiguous) && len(ids) == 1 {
			return ids[0], err
		}
		return nil, err
	}
	if len(ids) != 1 {
		return nil, errors.New("collections: insert returned no document id")
	}
	return ids[0], nil
}

// InsertBatch adds a batch of documents and returns the stored document IDs.
//
// Under the collection WAL target contract, WAL-on success makes the whole
// batch recoverable as one mutation boundary. Ordinary pre-commit errors expose
// no partial batch. Post-commit failures must be reported as commit-ambiguous.
func (c *Collection) InsertBatch(ids, documents [][]byte) ([][]byte, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	resultIDs, err := c.insertBatch(ids, documents, false, nil)
	if err == nil {
		err = commitAmbiguousError("InsertBatch vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
	}
	return resultIDs, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// InsertBatchWithTemplateV1Encoder inserts template-v1 documents and teaches
// encoder any numeric template IDs resolved by the successful insert. Later
// EncodeDocument calls on the same encoder can then emit compact TD1D stored
// documents directly instead of hash-addressed TD1H insert documents.
func (c *Collection) InsertBatchWithTemplateV1Encoder(ids, documents [][]byte, encoder *TemplateV1Encoder) ([][]byte, error) {
	if encoder == nil {
		return nil, errors.New("collections: template-v1 encoder cannot be nil")
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	resultIDs, err := c.insertBatch(ids, documents, false, encoder)
	if err == nil {
		err = commitAmbiguousError("InsertBatchWithTemplateV1Encoder vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
	}
	return resultIDs, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// InsertBatchValidatedBSON inserts native BSON documents that the caller has
// already validated. It is intended for wire-protocol gateways that validate
// BSON while parsing the request and need to avoid a duplicate full-document
// validation pass on the insert hot path.
func (c *Collection) InsertBatchValidatedBSON(ids, documents [][]byte) ([][]byte, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	resultIDs, err := c.insertBatch(ids, documents, true, nil)
	if err == nil {
		err = commitAmbiguousError("InsertBatchValidatedBSON vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
	}
	return resultIDs, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// PreflightCommandWALMutation checks collection-local command-WAL support for
// an already-classified deterministic apply mutation before R3a appends its
// local command-WAL frame.
func (c *Collection) PreflightCommandWALMutation(operation ColumnPublishOperation) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return err
	}
	return requireColumnStoreWriteOperationSupported(c.meta, operation)
}

// PreflightInsertBatchConflicts checks deterministic insert-batch conflicts
// before an external command-WAL owner stages its local frame. It publishes any
// buffered writes first so the persisted primary and unique roots match the
// collection's visible state, then reuses normal insert planning conflict
// probes without publishing the planned mutation.
func (c *Collection) PreflightInsertBatchConflicts(ids, documents [][]byte, trustedValidBSON bool) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return err
	}

	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	meta := catalog.meta
	c.meta = meta
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		return err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		return err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	indexRuntimes, indexRuntimesErr := catalog.cachedIndexRuntimes()
	planner := insertBatchPlanner{
		collection:             meta.Name,
		primaryRoot:            catalog.primaryRootName,
		templateRoot:           catalog.templateRootName,
		indexStateRoot:         catalog.indexStateRootName,
		cachedIndexRuntimes:    indexRuntimes,
		cachedIndexRuntimesErr: indexRuntimesErr,
		options:                plannerOptions,
	}
	plan, err := planner.planInsertBatch(ids, documents)
	if err != nil {
		return err
	}
	defer resetCollectionRunTables(plan.runs)
	return plan.checkPersistedConflicts(snap, catalog)
}

// PreflightReplaceBatchConflicts checks deterministic replacement conflicts
// before an external command-WAL owner stages its local frame. It publishes any
// buffered writes first so persisted roots match visible state, then reuses
// normal update planning without publishing the planned mutation.
func (c *Collection) PreflightReplaceBatchConflicts(ids, documents [][]byte) error {
	if c == nil {
		return errCollectionNil
	}
	if c.db == nil {
		return errCollectionDBNil
	}
	items, err := replaceBatchUpdateItems(ids, documents)
	if err != nil {
		return err
	}
	ownedItems, err := prepareUpdateBatchItems(items)
	if err != nil {
		return err
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return err
	}
	plan, err := c.buildUpdateBatchPlan(ownedItems, updateBatchModeAny, false, nil)
	if err != nil {
		return err
	}
	if plan != nil {
		plan.close()
	}
	return nil
}

// InsertBatchWithCommandWALIntent applies an already-appended collection insert
// command-WAL frame through the normal insert executor. It is reserved for R3a
// deterministic apply; ordinary callers should use InsertBatch or
// InsertBatchValidatedBSON so the collection owns command-WAL creation.
func (c *Collection) InsertBatchWithCommandWALIntent(ids, documents [][]byte, trustedValidBSON bool, commandWALIntent *backenddb.CommandWALIntent) ([][]byte, error) {
	if commandWALIntent == nil {
		return nil, errors.New("collections: InsertBatchWithCommandWALIntent requires command WAL intent")
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	resultIDs, err := c.insertBatchWithCommandWALIntent(ids, documents, trustedValidBSON, nil, commandWALIntent, insertBatchExecutionOptions{returnResultIDs: true})
	if err == nil {
		if trustedValidBSON {
			err = commitAmbiguousError("InsertBatchValidatedBSON vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
		} else {
			err = commitAmbiguousError("InsertBatch vector index maintenance", c.notifyVectorIndexesUpsert(resultIDs))
		}
	}
	return resultIDs, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// NativewireInsertBatchNoResultIDs executes an insert batch without cloning
// response-owned result IDs. It exists for the nativewire gateway omit-result
// fast path; public callers that need returned IDs should keep using
// InsertBatch or InsertBatchValidatedBSON.
func (c *Collection) NativewireInsertBatchNoResultIDs(ids, documents [][]byte, trustedValidBSON bool) error {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	_, err := c.insertBatchWithCommandWALIntent(ids, documents, trustedValidBSON, nil, nil, insertBatchExecutionOptions{returnResultIDs: false})
	if err == nil {
		if trustedValidBSON {
			err = commitAmbiguousError("InsertBatchValidatedBSON vector index maintenance", c.notifyVectorIndexesUpsert(ids))
		} else {
			err = commitAmbiguousError("InsertBatch vector index maintenance", c.notifyVectorIndexesUpsert(ids))
		}
	}
	return c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

func (c *Collection) insertBatch(ids, documents [][]byte, trustedValidBSON bool, templateEncoder *TemplateV1Encoder) ([][]byte, error) {
	return c.insertBatchWithCommandWALIntent(ids, documents, trustedValidBSON, templateEncoder, nil, insertBatchExecutionOptions{returnResultIDs: true})
}

type insertBatchExecutionOptions struct {
	returnResultIDs bool
}

func (c *Collection) insertBatchWithCommandWALIntent(ids, documents [][]byte, trustedValidBSON bool, templateEncoder *TemplateV1Encoder, commandWALIntent *backenddb.CommandWALIntent, execOpts insertBatchExecutionOptions) ([][]byte, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return nil, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()

	return retryInsertBatchMutation(func() ([][]byte, error) {
		return c.insertBatchOnce(ids, documents, trustedValidBSON, templateEncoder, commandWALIntent, execOpts)
	})
}

func retryInsertBatchMutation(run func() ([][]byte, error)) ([][]byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		resultIDs, err := run()
		if isRetriableCollectionMutationError(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return resultIDs, err
	}
	return nil, collectionMutationRetryExhausted(lastErr)
}

func isRetriableCollectionMutationError(err error) bool {
	// Once publication ownership transferred, running a logical mutation again
	// can apply it twice. Callers with operation-specific reconciliation must
	// handle this status before consulting ordinary pre-publication retry classes.
	if backenddb.CommitPublicationAccepted(err) {
		return false
	}
	return errors.Is(err, ErrConcurrentMutation) ||
		isRetriableOrderedRootPublishConflict(err) ||
		isRetriableCollectionCatalogReadEOF(err)
}

func isRetriableOrderedRootPublishConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "concurrent modification detected during ordered root publish") ||
		strings.Contains(err.Error(), "concurrent modification detected during ordered root group publish") ||
		strings.Contains(err.Error(), "durable-root candidate base changed:")
}

func isRetriableCollectionCatalogReadEOF(err error) bool {
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false
	}
	return strings.Contains(err.Error(), "collections: load catalog")
}

func (c *Collection) insertBatchOnce(ids, documents [][]byte, trustedValidBSON bool, templateEncoder *TemplateV1Encoder, commandWALIntent *backenddb.CommandWALIntent, execOpts insertBatchExecutionOptions) ([][]byte, error) {
	if shouldAttemptOptimisticInsertBatchPlanning(documents, templateEncoder, commandWALIntent, c.commandWALActive(commandWALIntent)) {
		if unlockMutation, locked := c.tryLockMutation(); locked {
			mutationLocked := true
			return c.insertBatchOnceWithLockState(ids, documents, trustedValidBSON, templateEncoder, commandWALIntent, execOpts, &unlockMutation, &mutationLocked)
		}
		if resultIDs, err, attempted := c.insertBatchOnceWithOptimisticPlanning(ids, documents, trustedValidBSON, execOpts); attempted {
			return resultIDs, err
		}
	}
	unlockMutation := c.lockMutation()
	mutationLocked := true
	return c.insertBatchOnceWithLockState(ids, documents, trustedValidBSON, templateEncoder, commandWALIntent, execOpts, &unlockMutation, &mutationLocked)
}

func shouldAttemptOptimisticInsertBatchPlanning(documents [][]byte, templateEncoder *TemplateV1Encoder, commandWALIntent *backenddb.CommandWALIntent, commandWALActive bool) bool {
	return len(documents) > 0 &&
		len(documents) <= DefaultIndexedWriteMemtableAccumulatorLockedPlanningDocuments &&
		templateEncoder == nil &&
		commandWALIntent == nil &&
		!commandWALActive
}

func (c *Collection) insertBatchOnceWithOptimisticPlanning(ids, documents [][]byte, trustedValidBSON bool, execOpts insertBatchExecutionOptions) ([][]byte, error, bool) {
	if c == nil || c.db == nil {
		return nil, nil, false
	}
	if c.hasBufferedNoIndexBSONPrimaryOverlayOrRootRuns() || c.hasBufferedIndexedDeletesOnly() {
		return nil, nil, false
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed, true
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
		return nil, err, true
	}
	if catalog == nil {
		closePlanningSnapshot()
		return nil, errCollectionNotFound, true
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		closePlanningSnapshot()
		return nil, err, true
	}
	meta := catalog.meta
	if len(meta.Indexes) == 0 {
		closePlanningSnapshot()
		return nil, nil, false
	}
	if columnStoreWriteEnabled(meta) {
		closePlanningSnapshot()
		return nil, nil, false
	}
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		closePlanningSnapshot()
		return nil, err, true
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		closePlanningSnapshot()
		return nil, err, true
	}
	if normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatTemplateV1 {
		closePlanningSnapshot()
		return nil, nil, false
	}
	indexedMemtablesEnabled := c.shouldBufferIndexedInserts(meta)
	bufferIndexedInserts := c.shouldBufferIndexedInsertBatch(meta, len(documents))
	directBufferedInsertAccumulators := bufferIndexedInserts && shouldUseDirectBufferedInsertAccumulators(len(documents))
	if !indexedMemtablesEnabled || !bufferIndexedInserts || !directBufferedInsertAccumulators {
		closePlanningSnapshot()
		return nil, nil, false
	}

	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	indexRuntimes, indexRuntimesErr := catalog.cachedIndexRuntimes()
	planner := insertBatchPlanner{
		collection:             meta.Name,
		primaryRoot:            catalog.primaryRootName,
		templateRoot:           catalog.templateRootName,
		indexStateRoot:         catalog.indexStateRootName,
		cachedIndexRuntimes:    indexRuntimes,
		cachedIndexRuntimesErr: indexRuntimesErr,
		options:                plannerOptions,
		buildPrimaryVal:        clonePrimaryDocument,
		cloneTemplateRunValues: true,
		directBufferedRuns:     true,
	}
	runTestBeforeInsertBatchPlanningHook()
	plan, err := planner.planInsertBatchWithPreflight(ids, documents, persistedConflictPreflightForInsertBatchSnapshot(snap, catalog))
	if err != nil {
		closePlanningSnapshot()
		return nil, err, true
	}
	plan.stats.BufferedIndexedBatches = 1
	if !insertBatchPlanHasRootWork(plan) {
		closePlanningSnapshot()
		c.setLastInsertStatsOwned(plan.stats.CollectionInsertStats)
		return maybeInsertBatchResultIDs(plan.resultIDs, execOpts), nil, true
	}
	resultIDs, err := cloneInsertBatchResultIDs(plan.resultIDs, execOpts)
	if err != nil {
		closePlanningSnapshot()
		resetCollectionRunTables(plan.runs)
		return nil, err, true
	}
	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)

	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if c.hasBufferedNoIndexBSONPrimaryOverlayOrRootRuns() || c.hasBufferedIndexedDeletesOnly() {
		closePlanningSnapshot()
		resetCollectionRunTables(plan.runs)
		return nil, ErrConcurrentMutation, true
	}
	c.meta = meta
	validation := insertBatchValidationContext{
		snap:                      snap,
		catalog:                   catalog,
		meta:                      meta,
		rootNames:                 rootNames,
		baseRootIDs:               baseRootIDs,
		plan:                      plan,
		allowRootDrift:            true,
		persistedConflictsChecked: true,
		preflightBaseCommitSeq:    baseCommitSeq,
		preflightBaseSystemRoot:   baseSystemRoot,
	}
	pin, currentCatalog, err := c.validateInsertBatchPlanAfterPlanningLocked(false, validation)
	snap = nil
	if err != nil {
		resetCollectionRunTables(plan.runs)
		return nil, err, true
	}
	updateInsertBatchBaseRootIDs(rootNames, baseRootIDs, currentCatalog)
	pinCommitSeq := snapshotCommitSeq(pin)
	pinSystemRoot := snapshotSystemRoot(pin)
	bufferFlushElapsed, err := c.bufferIndexedInsertPlanLocked(currentCatalog, pinCommitSeq, pinSystemRoot, plan, nil, false, nil)
	_ = pin.Close()
	if err != nil {
		resetCollectionRunTables(plan.runs)
		return nil, err, true
	}
	plan.stats.Publish += bufferFlushElapsed
	c.setLastInsertStatsOwned(plan.stats.CollectionInsertStats)
	return resultIDs, nil, true
}

func (c *Collection) insertBatchOnceWithLockState(
	ids, documents [][]byte,
	trustedValidBSON bool,
	templateEncoder *TemplateV1Encoder,
	commandWALIntent *backenddb.CommandWALIntent,
	execOpts insertBatchExecutionOptions,
	unlockMutation *collectionMutationUnlock,
	mutationLocked *bool,
) ([][]byte, error) {
	unlockIfLocked := func() {
		if mutationLocked != nil && *mutationLocked && unlockMutation != nil {
			unlockMutation.Unlock()
			*mutationLocked = false
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
	skipInitialNoIndexFlush := false
	commandWALActive := c.commandWALActive(commandWALIntent)
	commandWALNoIndexBufferCandidate := c.canBufferCommandWALNoIndexInsertBatch(c.meta, c.meta.Options.DocumentFormat, commandWALIntent, len(documents))
	if !commandWALActive &&
		c.canBufferNoIndexInsertBatchAck() &&
		len(c.meta.Indexes) == 0 &&
		len(c.meta.TextIndexes) == 0 &&
		canBufferNoIndexInsertBatchFormat(c.meta.Options.DocumentFormat, trustedValidBSON) {
		skipInitialNoIndexFlush = true
	}
	if !skipInitialNoIndexFlush {
		if err := c.flushBufferedNoIndex(); err != nil {
			return nil, err
		}
	}
	if c.hasBufferedNoIndexBSONPrimaryOverlayOrRootRuns() && !commandWALNoIndexBufferCandidate {
		if err := c.flushBufferedWrites(); err != nil {
			return nil, err
		}
	}
	if c.hasBufferedIndexedDeletesOnly() {
		if err := c.flushBufferedWrites(); err != nil {
			return nil, err
		}
	}

	var bufferedTemplateRuns []memtable.Table
	defer func() { resetCollectionTables(bufferedTemplateRuns) }()
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
	if err := c.requireColumnStoreCommandWAL(meta, commandWALIntent); err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationInsert); err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	if templateEncoder != nil && normalizedDocumentFormat(plannerOptions.documentFormat) != DocumentFormatTemplateV1 {
		closePlanningSnapshot()
		return nil, errors.New("collections: InsertBatchWithTemplateV1Encoder requires a template-v1 collection")
	}
	if templateEncoder.learnedTemplateV1ScopeMismatch(c) {
		closePlanningSnapshot()
		return nil, errors.New("collections: template-v1 encoder is bound to a different collection")
	}
	plannerOptions.learnTemplateIDs = templateEncoder != nil
	plannerOptions.allowTemplateV1Stored = templateEncoder.allowsTemplateV1StoredDocuments(c)
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	if skipInitialNoIndexFlush &&
		(len(meta.Indexes) != 0 || len(meta.TextIndexes) != 0 || !canBufferNoIndexInsertBatchFormat(plannerOptions.documentFormat, trustedValidBSON)) {
		catalog, meta, plannerOptions, err = c.reloadInsertBatchPlanningSnapshot(&snap, trustedValidBSON, c.flushBufferedNoIndex)
		if err != nil {
			return nil, err
		}
	}
	commandWALNoIndexBufferedMode := c.canBufferCommandWALNoIndexInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
	commandWALIndexedBufferEnabled := c.canUseCommandWALIndexedInsertBuffer(meta, plannerOptions.documentFormat, commandWALIntent)
	commandWALIndexedBufferedMode := c.canBufferCommandWALIndexedInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
	commandWALBufferedMode := commandWALNoIndexBufferedMode || commandWALIndexedBufferedMode
	indexedMemtablesEnabled := (!commandWALActive && c.shouldBufferIndexedInserts(meta)) || commandWALNoIndexBufferedMode || commandWALIndexedBufferEnabled
	bufferIndexedInserts := (!commandWALActive && c.shouldBufferIndexedInsertBatch(meta, len(documents))) || commandWALBufferedMode
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
		plannerOptions, err = collectionPlannerOptionsForDB(c.db, meta)
		if err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
		if err != nil {
			closePlanningSnapshot()
			return nil, err
		}
		if templateEncoder != nil && normalizedDocumentFormat(plannerOptions.documentFormat) != DocumentFormatTemplateV1 {
			closePlanningSnapshot()
			return nil, errors.New("collections: InsertBatchWithTemplateV1Encoder requires a template-v1 collection")
		}
		if templateEncoder.learnedTemplateV1ScopeMismatch(c) {
			closePlanningSnapshot()
			return nil, errors.New("collections: template-v1 encoder is bound to a different collection")
		}
		plannerOptions.learnTemplateIDs = templateEncoder != nil
		plannerOptions.allowTemplateV1Stored = templateEncoder.allowsTemplateV1StoredDocuments(c)
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		commandWALNoIndexBufferedMode = c.canBufferCommandWALNoIndexInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
		commandWALIndexedBufferEnabled = c.canUseCommandWALIndexedInsertBuffer(meta, plannerOptions.documentFormat, commandWALIntent)
		commandWALIndexedBufferedMode = c.canBufferCommandWALIndexedInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
		commandWALBufferedMode = commandWALNoIndexBufferedMode || commandWALIndexedBufferedMode
		indexedMemtablesEnabled = (!commandWALActive && c.shouldBufferIndexedInserts(meta)) || commandWALNoIndexBufferedMode || commandWALIndexedBufferEnabled
		bufferIndexedInserts = (!commandWALActive && c.shouldBufferIndexedInsertBatch(meta, len(documents))) || commandWALBufferedMode
	}
	if bufferIndexedInserts {
		if normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatTemplateV1 {
			bufferedTemplateRuns, err = cloneBufferedTemplateV1Runs(c.writeDomain, meta.Name)
			if err != nil {
				closePlanningSnapshot()
				return nil, err
			}
			if templateV1PlanningSnapshotNeedsRefresh(c.writeDomain, snap, bufferedTemplateRuns) {
				// Clone buffered template runs first, then refresh the fallback snapshot.
				// This closes the race where async publish removes a template run from the
				// buffered overlay after the original snapshot but before the clone.
				catalog, meta, plannerOptions, err = c.refreshTemplateV1PlanningSnapshot(&snap, trustedValidBSON, bufferedTemplateRuns, true)
				if err != nil {
					closePlanningSnapshot()
					return nil, err
				}
				c.meta = meta
				plannerOptions.learnTemplateIDs = templateEncoder != nil
				plannerOptions.allowTemplateV1Stored = templateEncoder.allowsTemplateV1StoredDocuments(c)
				commandWALNoIndexBufferedMode = c.canBufferCommandWALNoIndexInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
				commandWALIndexedBufferEnabled = c.canUseCommandWALIndexedInsertBuffer(meta, plannerOptions.documentFormat, commandWALIntent)
				commandWALIndexedBufferedMode = c.canBufferCommandWALIndexedInsertBatch(meta, plannerOptions.documentFormat, commandWALIntent, len(documents))
				commandWALBufferedMode = commandWALNoIndexBufferedMode || commandWALIndexedBufferedMode
				indexedMemtablesEnabled = (!commandWALActive && c.shouldBufferIndexedInserts(meta)) || commandWALNoIndexBufferedMode || commandWALIndexedBufferEnabled
				bufferIndexedInserts = (!commandWALActive && c.shouldBufferIndexedInsertBatch(meta, len(documents))) || commandWALBufferedMode
			}
		}
	}
	if !commandWALActive &&
		len(meta.Indexes) == 0 &&
		len(meta.TextIndexes) == 0 &&
		c.canBufferNoIndexInsertBatchAck() &&
		canBufferNoIndexInsertBatchFormat(plannerOptions.documentFormat, trustedValidBSON) {
		if resultIDs, buffered, err := c.bufferNoIndexInsertBatch(c.writeDomain, catalog, snap, plannerOptions, ids, documents, execOpts); buffered {
			closePlanningSnapshot()
			return resultIDs, err
		} else if skipInitialNoIndexFlush {
			catalog, meta, plannerOptions, err = c.reloadInsertBatchPlanningSnapshot(&snap, trustedValidBSON, c.flushBufferedNoIndex)
			if err != nil {
				return nil, err
			}
		}
	}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	if len(meta.Indexes) == 0 {
		if plannerOptions.documentFormat == DocumentFormatJSON && !commandWALNoIndexBufferedMode {
			return c.insertBatchNoIndex(catalog, snap, baseCommitSeq, baseSystemRoot, plannerOptions, ids, documents, commandWALIntent, execOpts)
		}
	}

	directBufferedInsertAccumulators := bufferIndexedInserts && shouldUseDirectBufferedInsertAccumulators(len(documents))
	mutationLockWait := time.Duration(0)
	if unlockMutation != nil {
		mutationLockWait = unlockMutation.wait
	}
	keepDirectBufferedInsertPlanningLocked := directBufferedInsertAccumulators && shouldKeepDirectBufferedInsertPlanningLocked(plannerOptions, len(documents), mutationLockWait)
	unlockForPlanning := shouldUnlockInsertPlanning(plannerOptions, indexedMemtablesEnabled, bufferIndexedInserts, keepDirectBufferedInsertPlanningLocked)
	if len(meta.TextIndexes) > 0 {
		unlockForPlanning = false
	}
	preflightPersistedConflicts := false
	preflight := insertBatchPreflight{}
	if unlockForPlanning {
		if directBufferedInsertAccumulators && canPreflightInsertBatchPersistedConflicts(snap, catalog) {
			preflight = persistedConflictPreflightForInsertBatchSnapshot(snap, catalog)
			preflightPersistedConflicts = true
		} else {
			closePlanningSnapshot()
		}
		unlockIfLocked()
		runTestBeforeInsertBatchPlanningHook()
	}

	indexRuntimes, indexRuntimesErr := catalog.cachedIndexRuntimes()
	planner := insertBatchPlanner{
		collection:             meta.Name,
		primaryRoot:            catalog.primaryRootName,
		templateRoot:           catalog.templateRootName,
		indexStateRoot:         catalog.indexStateRootName,
		cachedIndexRuntimes:    indexRuntimes,
		cachedIndexRuntimesErr: indexRuntimesErr,
		options:                plannerOptions,
	}
	if bufferIndexedInserts {
		planner.buildPrimaryVal = clonePrimaryDocument
		planner.cloneTemplateRunValues = true
		planner.directBufferedRuns = directBufferedInsertAccumulators
	}
	var plan *insertBatchPlan
	if preflightPersistedConflicts {
		plan, err = planner.planInsertBatchWithPreflight(ids, documents, preflight)
	} else {
		plan, err = planner.planInsertBatch(ids, documents)
	}
	if err != nil {
		closePlanningSnapshot()
		return nil, err
	}
	if bufferIndexedInserts {
		plan.stats.BufferedIndexedBatches = 1
	} else if indexedMemtablesEnabled {
		plan.stats.BufferedIndexedBypassBatches = 1
	}
	if len(meta.TextIndexes) > 0 {
		if err := appendTextIndexInsertPlanDeltas(snap, catalog, plannerOptions, plan); err != nil {
			closePlanningSnapshot()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
	}
	if !insertBatchPlanHasRootWork(plan) {
		closePlanningSnapshot()
		templateEncoder.learnTemplateV1Templates(c, plan.templateLearned)
		c.setLastInsertStatsOwned(plan.stats.CollectionInsertStats)
		return maybeInsertBatchResultIDs(plan.resultIDs, execOpts), nil
	}

	rootNames, baseRootIDs := insertBatchPlanRootNamesAndBaseIDs(plan, catalog)

	if bufferIndexedInserts {
		resultIDs, err := cloneInsertBatchResultIDs(plan.resultIDs, execOpts)
		if err != nil {
			closePlanningSnapshot()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		var bufferedCommandWALIntent *backenddb.CommandWALIntent
		if commandWALBufferedMode {
			docs, err := collectionDocumentsFromInsertPlan(plan, collectionPrimaryRootName(meta.Name))
			if err != nil {
				closePlanningSnapshot()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
			bufferedCommandWALIntent, err = c.newCollectionInsertCommandWALIntent(docs, nil)
			if err != nil {
				closePlanningSnapshot()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
		}
		var unlockCommandWALRawStage func()
		releaseCommandWALRawStage := func() {
			if unlockCommandWALRawStage != nil {
				unlockCommandWALRawStage()
				unlockCommandWALRawStage = nil
			}
		}
		defer releaseCommandWALRawStage()
		if bufferedCommandWALIntent != nil && c.db != nil {
			unlockCommandWALRawStage = c.db.LockCommandWALStaging()
			if err := c.drainCommandWALStageCoordinatorBeforeMutationWithHeldRawPublishLock(); err != nil {
				closePlanningSnapshot()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
		}
		pin, currentCatalog, pinCommitSeq, pinSystemRoot, err := c.lockAndValidateInsertBatchPlan(mutationLocked, unlockMutation, snap, catalog, meta, rootNames, baseRootIDs, preflightPersistedConflicts, baseCommitSeq, baseSystemRoot, plan)
		if err != nil {
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		var unlockCommandWALStage func()
		if bufferedCommandWALIntent != nil {
			unlockCommandWALStage, err = c.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
			if err != nil {
				_ = pin.Close()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
			defer unlockCommandWALStage()
		}
		bufferFlushElapsed, err := c.bufferIndexedInsertPlanLocked(currentCatalog, pinCommitSeq, pinSystemRoot, plan, bufferedCommandWALIntent, bufferedCommandWALIntent != nil && c.db != nil, releaseCommandWALRawStage)
		_ = pin.Close()
		if err != nil {
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
		plan.stats.Publish += bufferFlushElapsed
		templateEncoder.learnTemplateV1Templates(c, plan.templateLearned)
		c.setLastInsertStatsOwned(plan.stats.CollectionInsertStats)
		return resultIDs, nil
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		closePlanningSnapshot()
		resetCollectionRunTables(plan.runs)
		return nil, err
	}
	var commandWALDocuments []commitlog.CollectionDocument
	if commandWALIntent != nil && commandWALActive {
		commandWALDocuments, err = collectionDocumentsFromBatchInput(ids, documents)
		if err != nil {
			closePlanningSnapshot()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
	}
	if commandWALIntent == nil && commandWALActive {
		var docs []commitlog.CollectionDocument
		if normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatTemplateV1 {
			docs, err = collectionDocumentsFromBatchInput(ids, documents)
			if err != nil {
				closePlanningSnapshot()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
		} else {
			docs, err = collectionDocumentsFromInsertPlan(plan, collectionPrimaryRootName(meta.Name))
			if err != nil {
				closePlanningSnapshot()
				resetCollectionRunTables(plan.runs)
				return nil, err
			}
		}
		commandWALDocuments = docs
		commandWALIntent, err = c.newCollectionInsertCommandWALIntent(docs, nil)
		if err != nil {
			closePlanningSnapshot()
			resetCollectionRunTables(plan.runs)
			return nil, err
		}
	}

	pin, currentCatalog, pinCommitSeq, pinSystemRoot, err := c.lockAndValidateInsertBatchPlan(mutationLocked, unlockMutation, snap, catalog, meta, rootNames, baseRootIDs, preflightPersistedConflicts, baseCommitSeq, baseSystemRoot, plan)
	if err != nil {
		resetCollectionRunTables(plan.runs)
		return nil, err
	}
	baseCommitSeq = pinCommitSeq
	baseSystemRoot = pinSystemRoot
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = pin.Close() }()
	obsoletePointerizedTables, err := pointerizeInsertBatchPlanDataRuns(c.db, meta, plan)
	if err != nil {
		return nil, err
	}
	defer resetCollectionTables(obsoletePointerizedTables)

	ordered := make([]backenddb.OrderedRootDeltaPublishInput, 0, len(plan.runs))
	iterators := make([]iterator.UnsafeIterator, 0, len(plan.runs))
	defer func() {
		for _, it := range iterators {
			_ = it.Close()
		}
		resetCollectionRunTables(plan.runs)
	}()
	for i, run := range plan.runs {
		iter := run.table.NewIterator(nil, nil)
		iterators = append(iterators, iter)
		if i >= len(baseRootIDs) {
			return nil, fmt.Errorf("collections: insert plan missing base root id collection=%q root=%q", meta.Name, run.name)
		}
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      baseRootIDs[i],
			Iter:          iter,
			StoragePolicy: run.storagePolicy,
		})
	}
	baseRootIDMap := insertBatchBaseRootIDMap(rootNames, baseRootIDs)

	publishStart := time.Now()
	var newSystemRoot uint64
	var rootIDs []uint64
	var publishMeta CollectionMeta
	var publishRootNames []string
	if columnStoreWriteEnabled(meta) {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaGroupMaybeColumn(ordered, columnWritePublishInput{
				meta:             meta,
				catalog:          currentCatalog,
				baseCommitSeq:    baseCommitSeq,
				baseSystemRoot:   baseSystemRoot,
				rootNames:        cloneColumnPublishRootNames(rootNames),
				baseRootIDs:      cloneColumnPublishBaseRootIDs(baseRootIDMap),
				commandWALIntent: commandWALIntent,
				rawPublishLocked: true,
				operation:        ColumnPublishOperationInsert,
				documents:        columnWriteDocumentsFromCommitLog(commandWALDocuments),
				rows:             len(plan.resultIDs),
				insertStats:      &plan.stats.CollectionInsertStats,
			})
			return err
		})
	} else if commandWALIntent != nil {
		publishMeta = meta
		publishRootNames = rootNames
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDMap, rootIDs)
			})
			return err
		})
	} else {
		publishMeta = meta
		publishRootNames = rootNames
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDMap, rootIDs)
		})
	}
	plan.stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(publishRootNames) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(publishRootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(currentCatalog, publishMeta, publishRootNames, rootIDs)
	c.meta = publishMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	templateEncoder.learnTemplateV1Templates(c, plan.templateLearned)
	c.setLastInsertStatsOwned(plan.stats.CollectionInsertStats)
	return maybeInsertBatchResultIDs(plan.resultIDs, execOpts), nil
}

func (c *Collection) reloadInsertBatchPlanningSnapshot(
	snap **backenddb.Snapshot,
	trustedValidBSON bool,
	flush func() error,
) (*collectionCatalog, CollectionMeta, collectionOptions, error) {
	if snap == nil {
		return nil, CollectionMeta{}, collectionOptions{}, errors.New("collections: missing planning snapshot")
	}
	if *snap != nil {
		_ = (*snap).Close()
		*snap = nil
	}
	if flush != nil {
		if err := flush(); err != nil {
			return nil, CollectionMeta{}, collectionOptions{}, err
		}
	}
	next := c.db.AcquireSnapshot()
	if next == nil {
		return nil, CollectionMeta{}, collectionOptions{}, backenddb.ErrClosed
	}
	closeNext := true
	defer func() {
		if closeNext {
			_ = next.Close()
		}
	}()
	catalog, err := c.catalogForSnapshot(next)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	if catalog == nil {
		return nil, CollectionMeta{}, collectionOptions{}, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	meta := catalog.meta
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, next, catalog)
	c.meta = meta
	*snap = next
	closeNext = false
	return catalog, meta, plannerOptions, nil
}

func shouldUseDirectBufferedInsertAccumulators(documentCount int) bool {
	return documentCount > 0 && documentCount <= DefaultIndexedWriteMemtableAccumulatorBatchDocuments
}

func canBufferNoIndexInsertBatchFormat(format DocumentFormat, trustedValidBSON bool) bool {
	switch normalizedDocumentFormat(format) {
	case DocumentFormatJSON:
		return true
	case DocumentFormatBSON:
		return trustedValidBSON
	default:
		return false
	}
}

func shouldKeepDirectBufferedInsertPlanningLocked(opts collectionOptions, documentCount int, mutationLockWait time.Duration) bool {
	if documentCount <= 0 || documentCount > DefaultIndexedWriteMemtableAccumulatorLockedPlanningDocuments {
		return false
	}
	if normalizedDocumentFormat(opts.documentFormat) == DocumentFormatTemplateV1 {
		return true
	}
	// JSON/BSON direct-buffered inserts only pay the unlock/relock validation
	// path after observing real mutation-lock contention.
	if mutationLockWait >= indexedInsertPlanningUnlockMinWait {
		return false
	}
	return true
}

func shouldUnlockInsertPlanning(opts collectionOptions, indexedMemtablesEnabled, bufferIndexedInserts, keepDirectBufferedInsertPlanningLocked bool) bool {
	if normalizedDocumentFormat(opts.documentFormat) == DocumentFormatTemplateV1 {
		return false
	}
	if keepDirectBufferedInsertPlanningLocked {
		return false
	}
	if indexedMemtablesEnabled && !bufferIndexedInserts {
		return false
	}
	return true
}

func (c *Collection) refreshTemplateV1PlanningSnapshot(snap **backenddb.Snapshot, trustedValidBSON bool, bufferedTemplateRuns []memtable.Table, rejectBufferedOverlays bool) (*collectionCatalog, CollectionMeta, collectionOptions, error) {
	if snap == nil {
		return nil, CollectionMeta{}, collectionOptions{}, errors.New("collections: missing planning snapshot")
	}
	if *snap != nil {
		_ = (*snap).Close()
		*snap = nil
	}
	refreshed := c.db.AcquireSnapshot()
	if refreshed == nil {
		return nil, CollectionMeta{}, collectionOptions{}, backenddb.ErrClosed
	}
	closeRefreshed := true
	defer func() {
		if closeRefreshed {
			_ = refreshed.Close()
		}
	}()
	catalog, err := c.catalogForSnapshot(refreshed)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	if catalog == nil {
		return nil, CollectionMeta{}, collectionOptions{}, errCollectionNotFound
	}
	if rejectBufferedOverlays {
		if err := rejectCatalogRootOverlaysForIndexedBufferWrite(catalog); err != nil {
			return nil, CollectionMeta{}, collectionOptions{}, err
		}
	}
	meta := catalog.meta
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	plannerOptions, err = collectionOptionsWithTrustedBSONDocuments(plannerOptions, trustedValidBSON)
	if err != nil {
		return nil, CollectionMeta{}, collectionOptions{}, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, refreshed, catalog)
	if len(bufferedTemplateRuns) > 0 {
		plannerOptions = collectionOptionsWithBufferedTemplateV1RunsResolver(plannerOptions, bufferedTemplateRuns)
	}
	*snap = refreshed
	closeRefreshed = false
	return catalog, meta, plannerOptions, nil
}

func templateV1PlanningSnapshotNeedsRefresh(domain *collectionWriteDomain, snap *backenddb.Snapshot, bufferedTemplateRuns []memtable.Table) bool {
	if len(bufferedTemplateRuns) > 0 {
		return true
	}
	return collectionWriteDomainSnapshotStale(domain, snapshotCommitSeq(snap), snapshotSystemRoot(snap))
}

func insertBatchPlanRootNamesAndBaseIDs(plan *insertBatchPlan, catalog *collectionCatalog) ([]string, []uint64) {
	if plan == nil {
		return nil, nil
	}
	if direct := plan.directBufferedInsert; direct != nil && len(direct.rootNames) > 0 {
		rootNames := direct.rootNames
		baseRootIDs := make([]uint64, len(rootNames))
		for i, rootName := range rootNames {
			if catalog != nil {
				baseRootIDs[i] = catalog.rootID(rootName)
			}
		}
		return rootNames, baseRootIDs
	}
	rootNames := make([]string, len(plan.runs))
	baseRootIDs := make([]uint64, len(plan.runs))
	for i, run := range plan.runs {
		rootNames[i] = run.name
		if catalog != nil {
			baseRootIDs[i] = catalog.rootID(run.name)
		}
	}
	return rootNames, baseRootIDs
}

func insertBatchPlanHasRootWork(plan *insertBatchPlan) bool {
	if plan == nil {
		return false
	}
	if len(plan.runs) > 0 {
		return true
	}
	return plan.directBufferedInsert != nil && len(plan.directBufferedInsert.rootNames) > 0
}

func maybeInsertBatchResultIDs(resultIDs [][]byte, execOpts insertBatchExecutionOptions) [][]byte {
	if !execOpts.returnResultIDs {
		return nil
	}
	return resultIDs
}

func cloneInsertBatchResultIDs(resultIDs [][]byte, execOpts insertBatchExecutionOptions) ([][]byte, error) {
	if !execOpts.returnResultIDs {
		return nil, nil
	}
	return cloneBatchDocumentIDs(resultIDs)
}

type insertBatchValidationContext struct {
	snap                      *backenddb.Snapshot
	catalog                   *collectionCatalog
	meta                      CollectionMeta
	rootNames                 []string
	baseRootIDs               []uint64
	plan                      *insertBatchPlan
	allowRootDrift            bool
	persistedConflictsChecked bool
	preflightBaseCommitSeq    uint64
	preflightBaseSystemRoot   uint64
}

func (c *Collection) lockAndValidateInsertBatchPlan(
	mutationLocked *bool,
	unlockMutation *collectionMutationUnlock,
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	meta CollectionMeta,
	rootNames []string,
	baseRootIDs []uint64,
	persistedConflictsChecked bool,
	preflightBaseCommitSeq uint64,
	preflightBaseSystemRoot uint64,
	plan *insertBatchPlan,
) (*backenddb.Snapshot, *collectionCatalog, uint64, uint64, error) {
	plannedWithMutationLocked := *mutationLocked
	if !*mutationLocked {
		*unlockMutation = c.lockMutation()
		*mutationLocked = true
	}
	c.meta = meta
	validation := insertBatchValidationContext{
		snap:                      snap,
		catalog:                   catalog,
		meta:                      meta,
		rootNames:                 rootNames,
		baseRootIDs:               baseRootIDs,
		plan:                      plan,
		allowRootDrift:            !plannedWithMutationLocked,
		persistedConflictsChecked: persistedConflictsChecked,
		preflightBaseCommitSeq:    preflightBaseCommitSeq,
		preflightBaseSystemRoot:   preflightBaseSystemRoot,
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

func updateInsertBatchBaseRootIDs(rootNames []string, baseRootIDs []uint64, catalog *collectionCatalog) {
	if catalog == nil || baseRootIDs == nil {
		return
	}
	for i, rootName := range rootNames {
		if i >= len(baseRootIDs) {
			return
		}
		baseRootIDs[i] = catalog.rootID(rootName)
	}
}

func insertBatchBaseRootID(rootNames []string, baseRootIDs []uint64, rootName string) (uint64, bool) {
	for i, name := range rootNames {
		if name != rootName {
			continue
		}
		if i >= len(baseRootIDs) {
			return 0, false
		}
		return baseRootIDs[i], true
	}
	return 0, false
}

func insertBatchBaseRootIDMap(rootNames []string, baseRootIDs []uint64) map[string]uint64 {
	if len(rootNames) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(rootNames))
	for i, rootName := range rootNames {
		if i >= len(baseRootIDs) {
			break
		}
		out[rootName] = baseRootIDs[i]
	}
	return out
}

func runTestBeforeInsertBatchPlanningHook() {
	hook := testBeforeInsertBatchPlanningHook.ptr.Load()
	if hook != nil && hook.fn != nil {
		hook.fn()
	}
}

func canPreflightInsertBatchPersistedConflicts(snap *backenddb.Snapshot, catalog *collectionCatalog) bool {
	return snap != nil && catalog != nil && len(catalog.rootOverlays) == 0
}

func persistedConflictPreflightForInsertBatchSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog) insertBatchPreflight {
	if !canPreflightInsertBatchPersistedConflicts(snap, catalog) {
		return insertBatchPreflight{}
	}
	return insertBatchPreflight{
		snapshot:           snap,
		primaryRootID:      catalog.rootID(collectionPrimaryRootName(catalog.meta.Name)),
		uniqueIndexRootIDs: uniqueIndexRootIDs(catalog),
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
	if validation.snap != nil && current != validation.snap {
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
	if validation.persistedConflictsChecked && validation.snap != nil && validation.catalog != nil {
		currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
		if currentCommitSeq == validation.preflightBaseCommitSeq && currentSystemRoot == validation.preflightBaseSystemRoot {
			c.writeDomain.observeInsertValidationPreflight(false)
			if validation.plan != nil {
				validation.plan.stats.ValidationPreflightReused++
			}
			if err := c.validateInsertBatchPlanWithSnapshotLocked(validation); err != nil {
				return nil, nil, err
			}
			return validation.snap, validation.catalog, nil
		}
		c.writeDomain.observeInsertValidationPreflight(true)
		if validation.plan != nil {
			validation.plan.stats.ValidationPreflightRechecked++
		}
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return nil, nil, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(current)
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
	validation.persistedConflictsChecked = false
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
	for i, rootName := range validation.rootNames {
		if i >= len(validation.baseRootIDs) {
			return fmt.Errorf("collections: insert plan missing base root id collection=%q root=%q", validation.meta.Name, rootName)
		}
		want := validation.baseRootIDs[i]
		if got := validation.catalog.rootID(rootName); got != want && !validation.allowRootDrift {
			return errConcurrentRootModification(validation.meta.Name, rootName)
		}
	}
	if validation.persistedConflictsChecked {
		return nil
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
	commandWALIntent *backenddb.CommandWALIntent,
	execOpts insertBatchExecutionOptions,
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
	if commandWALIntent == nil && c.commandWALActive(nil) {
		commandWALIntent, err = c.newCollectionInsertCommandWALIntent(collectionDocumentsFromNoIndexEntries(entries), nil)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	var retainedDocuments [][]byte
	var retainedTemplateRecords []templateV1Record
	var retainedSemanticStreamBlocks memtable.Table
	var retainedDeclaredRows []columnDeclaredRow
	var retainedDeclaredRowsReady bool
	if columnStoreNeedsRetainedPayloadTransform(c.meta) {
		phaseStart = time.Now()
		fullDocumentIDs := make([][]byte, len(entries))
		fullDocuments := make([][]byte, len(entries))
		for i := range entries {
			fullDocumentIDs[i] = entries[i].id
			fullDocuments[i] = entries[i].document
		}
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(*c.meta.Options.ColumnStore, fullDocumentIDs, fullDocuments, columnRetainedPayloadTemplateResolver(snap, catalog))
		if err != nil {
			return nil, err
		}
		retainedDocuments = prepared.documents
		retainedTemplateRecords = prepared.templateRecords
		retainedSemanticStreamBlocks = prepared.semanticStreamBlocks
		retainedDeclaredRows = prepared.declaredRows
		retainedDeclaredRowsReady = prepared.declaredRowsReady
		stats.RetainedPayloadPrepare = time.Since(phaseStart)
		stats.RetainedPayloadRows = len(entries)
		if retainedDeclaredRowsReady {
			stats.RetainedPayloadDeclaredRows = len(retainedDeclaredRows)
		}
		if retainedSemanticStreamBlocks != nil {
			stats.RetainedPayloadSemanticStreamBlocks = retainedSemanticStreamBlocks.Len()
		}
		stats.RetainedPayloadSemanticStreamWorkerCount = prepared.semanticStreamPrepareMetrics.WorkerCount
		stats.RetainedPayloadSemanticStreamDeclaredRowPrepare = prepared.semanticStreamPrepareMetrics.DeclaredRowPrepare
		stats.RetainedPayloadSemanticStreamBlockPrepareWall = prepared.semanticStreamPrepareMetrics.BlockPrepareWall
		stats.RetainedPayloadSemanticStreamBlockCollect = prepared.semanticStreamPrepareMetrics.BlockCollect
		stats.RetainedPayloadSemanticStreamBlockEncoderSetup = prepared.semanticStreamPrepareMetrics.BlockEncoderSetup
		stats.RetainedPayloadSemanticStreamBlockRawEncode = prepared.semanticStreamPrepareMetrics.BlockRawEncode
		stats.RetainedPayloadSemanticStreamBlockStoredEncode = prepared.semanticStreamPrepareMetrics.BlockStoredEncode
		stats.RetainedPayloadSemanticStreamBlockFinalize = prepared.semanticStreamPrepareMetrics.BlockFinalize
		stats.RetainedPayloadSemanticStreamTableBuild = prepared.semanticStreamPrepareMetrics.TableBuild
	}

	phaseStart = time.Now()
	table := newCollectionRunTable(len(entries))
	var rowRemainderBytes int64
	for i := range entries {
		storedDocument := entries[i].document
		if retainedDocuments != nil {
			storedDocument = retainedDocuments[i]
		}
		rowRemainderBytes = saturatingAddNonNegativeInt64(rowRemainderBytes, int64(len(entries[i].id)+len(storedDocument)))
		setCollectionRunValue(table, entries[i].id, storedDocument)
	}
	table.Freeze()
	stats.PrimaryRunBuild = time.Since(phaseStart)
	phaseStart = time.Now()
	publishTable, pointerized, pointerizeStats, err := pointerizeCollectionRunTableValuesForRootWithStats(c.db, c.meta, rootName, table)
	if err != nil {
		return nil, err
	}
	if columnStoreNeedsRetainedPayloadTransform(c.meta) {
		stats.RetainedPayloadValueLogPointerize = time.Since(phaseStart)
		stats.RetainedPayloadValueLogValues = pointerizeStats.Values
		stats.RetainedPayloadValueLogBytes = pointerizeStats.Bytes
	}
	if pointerized {
		defer resetCollectionRunTable(publishTable)
	}
	iter := publishTable.NewIterator(nil, nil)
	defer func() {
		_ = iter.Close()
		resetCollectionRunTable(table)
	}()

	publishStart := time.Now()
	rootNames := []string{rootName}
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	ordered := []backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: plannerOptions.dataStoragePolicy,
	}}
	var templateTables []memtable.Table
	var templateIters []iterator.UnsafeIterator
	defer func() {
		for _, it := range templateIters {
			_ = it.Close()
		}
		resetCollectionTables(templateTables)
	}()
	if len(retainedTemplateRecords) > 0 {
		templatePlan := &insertBatchPlan{}
		if err := (insertBatchPlanner{
			collection:             c.meta.Name,
			templateRoot:           collectionTemplateRootName(c.meta.Name),
			options:                plannerOptions,
			cloneTemplateRunValues: true,
		}).emitTemplateRun(templatePlan, retainedTemplateRecords); err != nil {
			return nil, err
		}
		for _, run := range templatePlan.runs {
			templateTables = append(templateTables, run.table)
			templateIter := run.table.NewIterator(nil, nil)
			templateIters = append(templateIters, templateIter)
			rootNames = append(rootNames, run.name)
			baseRootIDs[run.name] = catalog.rootID(run.name)
			ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
				BaseRoot:      catalog.rootID(run.name),
				Iter:          templateIter,
				StoragePolicy: run.storagePolicy,
			})
		}
	}
	var retainedSemanticStreamTables []memtable.Table
	var retainedSemanticStreamIters []iterator.UnsafeIterator
	defer func() {
		for _, it := range retainedSemanticStreamIters {
			_ = it.Close()
		}
		resetCollectionTables(retainedSemanticStreamTables)
	}()
	if retainedSemanticStreamBlocks != nil && retainedSemanticStreamBlocks.Len() > 0 {
		streamRootName := collectionRetainedSemanticStreamRootName(c.meta.Name)
		streamPolicy, err := collectionRootStoragePolicyForDB(c.db, c.meta, streamRootName)
		if err != nil {
			return nil, err
		}
		streamPublishTable := retainedSemanticStreamBlocks
		retainedSemanticStreamTables = append(retainedSemanticStreamTables, retainedSemanticStreamBlocks)
		phaseStart := time.Now()
		if pointerizedStreamTable, pointerized, pointerizeStats, err := pointerizeCollectionRunTableValuesForRootWithStats(c.db, c.meta, streamRootName, retainedSemanticStreamBlocks); err != nil {
			return nil, err
		} else if pointerized {
			streamPublishTable = pointerizedStreamTable
			retainedSemanticStreamTables = append(retainedSemanticStreamTables, pointerizedStreamTable)
			stats.RetainedStreamValueLogValues = pointerizeStats.Values
			stats.RetainedStreamValueLogBytes = pointerizeStats.Bytes
		}
		stats.RetainedStreamValueLogPointerize = time.Since(phaseStart)
		streamIter := streamPublishTable.NewIterator(nil, nil)
		retainedSemanticStreamIters = append(retainedSemanticStreamIters, streamIter)
		rootNames = append(rootNames, streamRootName)
		baseRootIDs[streamRootName] = catalog.rootID(streamRootName)
		ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
			BaseRoot:      catalog.rootID(streamRootName),
			Iter:          streamIter,
			StoragePolicy: streamPolicy,
		})
	}
	var textTables []memtable.Table
	var textIters []iterator.UnsafeIterator
	defer func() {
		for _, it := range textIters {
			_ = it.Close()
		}
		resetCollectionTables(textTables)
	}()
	if len(c.meta.TextIndexes) > 0 {
		textRootNames := make([]string, 0, len(c.meta.TextIndexes)*3)
		textBaseRootIDs := make(map[string]uint64, len(c.meta.TextIndexes)*3)
		textPolicies := make([]backenddb.OrderedRootStoragePolicy, 0, len(c.meta.TextIndexes)*3)
		if err := appendTextIndexNoIndexInsertDeltas(snap, catalog, plannerOptions, entries, &textRootNames, textBaseRootIDs, &textPolicies, &textTables); err != nil {
			return nil, err
		}
		for i, textRootName := range textRootNames {
			textIter := textTables[i].NewIterator(nil, nil)
			textIters = append(textIters, textIter)
			rootNames = append(rootNames, textRootName)
			baseRootIDs[textRootName] = textBaseRootIDs[textRootName]
			ordered = append(ordered, backenddb.OrderedRootDeltaPublishInput{
				BaseRoot:      textBaseRootIDs[textRootName],
				Iter:          textIter,
				StoragePolicy: textPolicies[i],
			})
		}
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	if columnStoreWriteEnabled(c.meta) {
		var publishMeta CollectionMeta
		var publishRootNames []string
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaGroupMaybeColumn(ordered, columnWritePublishInput{
				meta:              c.meta,
				catalog:           catalog,
				baseCommitSeq:     baseCommitSeq,
				baseSystemRoot:    baseSystemRoot,
				rootNames:         cloneColumnPublishRootNames(rootNames),
				baseRootIDs:       cloneColumnPublishBaseRootIDs(baseRootIDs),
				commandWALIntent:  commandWALIntent,
				rawPublishLocked:  true,
				operation:         ColumnPublishOperationInsert,
				documents:         columnWriteDocumentsFromNoIndexEntries(entries),
				rows:              len(entries),
				declaredRows:      retainedDeclaredRows,
				declaredRowsReady: retainedDeclaredRowsReady,
				rowRemainderBytes: rowRemainderBytes,
				insertStats:       &stats,
			})
			return err
		})
		stats.Publish = time.Since(publishStart)
		if err != nil {
			return nil, err
		}
		if len(rootIDs) != len(publishRootNames) {
			return nil, unexpectedOrderedRootCountError(c.meta.Name, len(publishRootNames), len(rootIDs))
		}
		stats.Runs = len(rootNames)
		nextCatalog := cloneCatalogWithRootUpdates(catalog, publishMeta, publishRootNames, rootIDs)
		c.meta = publishMeta
		c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
		c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
		c.replacePrimaryDocumentCacheAfterInsert(newSystemRoot, publishMeta, entries)
		c.setLastInsertStats(stats)
		return maybeInsertBatchResultIDs(resultIDs, execOpts), nil
	}

	if commandWALIntent != nil {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
			})
			return err
		})
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		})
	}
	stats.Publish = time.Since(publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(c.meta.Name, len(rootNames), len(rootIDs))
	}
	stats.Runs = len(rootNames)
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.replacePrimaryDocumentCacheAfterInsert(newSystemRoot, c.meta, entries)
	c.setLastInsertStats(stats)
	return maybeInsertBatchResultIDs(resultIDs, execOpts), nil
}

// Delete removes one document. See DeleteDocument for the matched/deleted
// result.
//
// Under the collection WAL target contract, WAL-on success is process-crash
// recoverable; WAL-off relaxed success is not durable-at-ack until an explicit
// persistence boundary covers the write.
func (c *Collection) Delete(documentID []byte) error {
	_, err := c.DeleteDocument(documentID)
	return err
}

// DeleteDocument removes a document and reports whether this call deleted an
// existing primary document.
func (c *Collection) DeleteDocument(documentID []byte) (bool, error) {
	return c.deleteDocumentIf(documentID, nil)
}

// DeleteDocumentIf removes a document only when predicate accepts its current
// stored value. The predicate runs inside the collection mutation boundary;
// false leaves the document unchanged and an error is returned before commit.
// It may be retried after a retriable publish failure, so it must be side-effect
// free and safe to invoke more than once.
func (c *Collection) DeleteDocumentIf(documentID []byte, predicate func(current []byte) (bool, error)) (bool, error) {
	if predicate == nil {
		return false, errors.New("collections: delete predicate is nil")
	}
	return c.deleteDocumentIf(documentID, predicate)
}

func (c *Collection) deleteDocumentIf(documentID []byte, predicate func(current []byte) (bool, error)) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return false, err
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if len(documentID) == 0 {
		return false, errors.New("collections: document id cannot be empty")
	}
	unlockMutation := c.lockMutation()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation.Unlock()
		}
	}()
	if c.commandWALActive(nil) || c.shouldFlushBeforeIndexedDelete(c.meta) {
		if err := c.flushBufferedWrites(); err != nil {
			return false, err
		}
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		deleted, err := c.deleteDocumentOnce(documentID, predicate, nil)
		if isRetriableCollectionMutationError(err) {
			lastErr = err
			if flushErr := c.flushBufferedWrites(); flushErr != nil {
				return false, flushErr
			}
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		if err == nil && deleted {
			unlockMutation.Unlock()
			mutationLocked = false
			err = commitAmbiguousError("DeleteDocument vector index maintenance", c.notifyVectorIndexesDelete([][]byte{documentID}))
		}
		return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
	}
	return false, collectionMutationRetryExhausted(lastErr)
}

// DeleteBatch removes a caller-provided batch of document IDs as one collection
// root publish and returns the number of existing documents removed. Missing
// documents are ignored. Duplicate IDs in the request are rejected so callers do
// not depend on ordered same-ID semantics.
//
// Under the collection WAL target contract, WAL-on success makes the whole
// delete batch recoverable as one mutation boundary. Pre-commit errors expose no
// partial batch; post-commit failures must be commit-ambiguous.
func (c *Collection) DeleteBatch(documentIDs [][]byte) (int, error) {
	if c == nil {
		return 0, errCollectionNil
	}
	if c.db == nil {
		return 0, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, err
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	for i, id := range documentIDs {
		if len(id) == 0 {
			return 0, fmt.Errorf("collections: document id cannot be empty at index %d", i)
		}
	}
	ids, err := cloneBatchDocumentIDs(documentIDs)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		key := string(id)
		if _, ok := seen[key]; ok {
			return 0, fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
		}
		seen[key] = struct{}{}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	unlockMutation := c.lockMutation()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation.Unlock()
		}
	}()
	if c.commandWALActive(nil) || c.shouldFlushBeforeIndexedDelete(c.meta) {
		if err := c.flushBufferedWrites(); err != nil {
			return 0, err
		}
	}
	deleted, err := c.deleteBatchWithCommandWALIntent(ids, nil)
	if err == nil && deleted > 0 {
		unlockMutation.Unlock()
		mutationLocked = false
		err = commitAmbiguousError("DeleteBatch vector index maintenance", c.notifyVectorIndexesDelete(ids))
	}
	return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// DeleteBatchWithCommandWALIntent applies an already-appended collection delete
// command-WAL frame through the normal delete executor. It is reserved for R3a
// deterministic apply; ordinary callers should use DeleteBatch.
func (c *Collection) DeleteBatchWithCommandWALIntent(documentIDs [][]byte, commandWALIntent *backenddb.CommandWALIntent) (int, error) {
	if commandWALIntent == nil {
		return 0, errors.New("collections: DeleteBatchWithCommandWALIntent requires command WAL intent")
	}
	if c == nil {
		return 0, errCollectionNil
	}
	if c.db == nil {
		return 0, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, err
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	for i, id := range documentIDs {
		if len(id) == 0 {
			return 0, fmt.Errorf("collections: document id cannot be empty at index %d", i)
		}
	}
	ids, err := cloneBatchDocumentIDs(documentIDs)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(ids))
	for i, id := range ids {
		key := string(id)
		if _, ok := seen[key]; ok {
			return 0, fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, i)
		}
		seen[key] = struct{}{}
	}
	unlockMutation := c.lockMutation()
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockMutation.Unlock()
		}
	}()
	if c.commandWALActive(commandWALIntent) || c.shouldFlushBeforeIndexedDelete(c.meta) {
		if err := c.flushBufferedWrites(); err != nil {
			return 0, err
		}
	}
	deleted, err := c.deleteBatchWithCommandWALIntent(ids, commandWALIntent)
	if err == nil && deleted > 0 {
		unlockMutation.Unlock()
		mutationLocked = false
		err = commitAmbiguousError("DeleteBatch vector index maintenance", c.notifyVectorIndexesDelete(ids))
	}
	return deleted, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

func (c *Collection) deleteBatchWithCommandWALIntent(ids [][]byte, commandWALIntent *backenddb.CommandWALIntent) (int, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		deleted, err := c.deleteBatchOnce(ids, commandWALIntent)
		if errors.Is(err, ErrConcurrentMutation) {
			lastErr = err
			if flushErr := c.flushBufferedWrites(); flushErr != nil {
				return 0, flushErr
			}
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return deleted, err
	}
	return 0, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) deleteBatchOnce(documentIDs [][]byte, commandWALIntent *backenddb.CommandWALIntent) (int, error) {
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.ErrClosed
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		return 0, err
	}
	if catalog == nil {
		_ = snap.Close()
		return 0, errCollectionNotFound
	}
	if err := rejectCatalogRootOverlaysForWrite(catalog); err != nil {
		_ = snap.Close()
		return 0, err
	}
	c.meta = catalog.meta
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, c.meta)
	if err != nil {
		_ = snap.Close()
		return 0, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	if err := c.requireColumnStoreCommandWAL(c.meta, commandWALIntent); err != nil {
		_ = snap.Close()
		return 0, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationDelete); err != nil {
		_ = snap.Close()
		return 0, err
	}

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	commandWALActive := c.commandWALActive(commandWALIntent)
	if commandWALIntent == nil && commandWALActive {
		commandWALIntent, err = c.newCollectionDeleteCommandWALIntent(documentIDs, nil)
		if err != nil {
			_ = snap.Close()
			return 0, err
		}
	}
	primaryRoot := catalog.rootID(primaryRootName)
	if primaryRoot == 0 {
		_ = snap.Close()
		if commandWALIntent != nil {
			if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	runtimes, err := catalog.cachedIndexRuntimes()
	if err != nil {
		_ = snap.Close()
		return 0, err
	}

	type existingDelete struct {
		id           []byte
		state        documentIndexState
		document     []byte
		primaryValue []byte
	}
	existing := make([]existingDelete, 0, len(documentIDs))
	for _, documentID := range documentIDs {
		if c.writeDomain != nil {
			c.writeDomain.mu.RLock()
			alreadyDeleted := c.bufferedIndexedDeleteTombstoneLocked(c.writeDomain, documentID)
			c.writeDomain.mu.RUnlock()
			if alreadyDeleted {
				continue
			}
		}
		entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, primaryRootName, documentID)
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			_ = snap.Close()
			return 0, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			continue
		}
		item := existingDelete{id: documentID}
		if columnStoreRetainedPayloadUsesSemanticStreamV1(c.meta.Options.ColumnStore) {
			primaryValue, ok, err := columnRetainedSemanticStreamV1PrimaryValueForReclaim(snap, catalog, primaryRootName, documentID, entry)
			if err != nil {
				_ = snap.Close()
				return 0, err
			}
			if ok {
				item.primaryValue = primaryValue
			}
		}
		if len(c.meta.TextIndexes) > 0 {
			item.document = bytes.Clone(entry.Value)
		}
		if len(runtimes) > 0 {
			item.state, err = loadDeleteIndexState(snap, catalog, documentID, entry.Value, runtimes, plannerOptions)
			if err != nil {
				_ = snap.Close()
				return 0, err
			}
		}
		existing = append(existing, item)
	}
	if len(existing) == 0 {
		_ = snap.Close()
		if commandWALIntent != nil {
			if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	deleteIDs := make([][]byte, len(existing))
	for i := range existing {
		deleteIDs[i] = existing[i].id
	}
	rootNames := []string{primaryRootName}
	baseRootIDs := map[string]uint64{primaryRootName: primaryRoot}
	policies := []backenddb.OrderedRootStoragePolicy{plannerOptions.dataStoragePolicy}
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes))
	deltaTables = append(deltaTables, buildDeleteRootDeltaTable(deleteIDs))

	if len(runtimes) > 0 {
		if persistIndexStateForOptions(plannerOptions) {
			stateRootName := collectionIndexStateRootName(c.meta.Name)
			stateRootID := catalog.rootID(stateRootName)
			if stateRootID != 0 {
				rootNames = append(rootNames, stateRootName)
				baseRootIDs[stateRootName] = stateRootID
				policies = append(policies, plannerOptions.indexStateStoragePolicy)
				deltaTables = append(deltaTables, buildDeleteRootDeltaTable(deleteIDs))
			}
		}
		for _, runtime := range runtimes {
			rootName := collectionSecondaryRootName(c.meta.Name, runtime.def.name)
			rootID := catalog.rootID(rootName)
			if rootID == 0 {
				continue
			}
			var table memtable.Table
			for _, item := range existing {
				if len(item.state[runtime.def.name]) == 0 {
					continue
				}
				if table == nil {
					table = newCollectionRunTable(len(existing))
				}
				if err := deleteSecondaryEntriesForDocument(table, runtime, item.state, item.id); err != nil {
					_ = snap.Close()
					if table != nil {
						resetCollectionRunTable(table)
					}
					resetCollectionTables(deltaTables)
					return 0, err
				}
			}
			if table == nil {
				continue
			}
			table.Freeze()
			rootNames = append(rootNames, rootName)
			baseRootIDs[rootName] = rootID
			policies = append(policies, runtime.def.storagePolicy)
			deltaTables = append(deltaTables, table)
		}
	}

	fallbackDocuments := make([][]byte, 0, len(existing))
	if len(c.meta.TextIndexes) > 0 {
		for _, item := range existing {
			fallbackDocuments = append(fallbackDocuments, item.document)
		}
		if err := appendTextIndexDeleteDeltas(snap, catalog, plannerOptions, deleteIDs, fallbackDocuments, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
			_ = snap.Close()
			resetCollectionTables(deltaTables)
			return 0, err
		}
	}
	var semanticReclaimValues [][]byte
	for _, item := range existing {
		if len(item.primaryValue) != 0 {
			semanticReclaimValues = append(semanticReclaimValues, item.primaryValue)
		}
	}
	if err := appendColumnRetainedSemanticStreamV1ReclaimDeltas(c.db, snap, catalog, c.meta, deleteIDs, semanticReclaimValues, nil, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		_ = snap.Close()
		resetCollectionTables(deltaTables)
		return 0, err
	}

	if !commandWALActive && len(c.meta.TextIndexes) == 0 {
		if buffered, err := c.bufferIndexedDeleteTablesLocked(catalog, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, policies, deltaTables, len(existing)); buffered || err != nil {
			_ = snap.Close()
			if err != nil {
				resetCollectionTables(deltaTables)
				return 0, err
			}
			return len(existing), err
		}
	}
	defer func() { _ = snap.Close() }()
	defer func() { resetCollectionTables(deltaTables) }()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(c.meta.Name, rootNames, deltaTables, baseRootIDs, policies)
	if err != nil {
		return 0, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(c.meta.Name, rootNames, ordered)
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	if columnStoreWriteEnabled(c.meta) {
		var publishMeta CollectionMeta
		var publishRootNames []string
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaBatchGroupMaybeColumn(ordered, nil, columnWritePublishInput{
				meta:             c.meta,
				catalog:          catalog,
				baseCommitSeq:    baseCommitSeq,
				baseSystemRoot:   baseSystemRoot,
				rootNames:        cloneColumnPublishRootNames(rootNames),
				baseRootIDs:      cloneColumnPublishBaseRootIDs(baseRootIDs),
				commandWALIntent: commandWALIntent,
				rawPublishLocked: true,
				operation:        ColumnPublishOperationDelete,
				documents:        columnWriteDocumentsFromIDs(deleteIDs),
				rows:             len(existing),
			})
			return err
		})
		cleanupDeltas()
		if err != nil {
			return 0, err
		}
		if len(rootIDs) != len(publishRootNames) {
			return 0, unexpectedOrderedRootCountError(c.meta.Name, len(publishRootNames), len(rootIDs))
		}
		nextCatalog := cloneCatalogWithRootUpdates(catalog, publishMeta, publishRootNames, rootIDs)
		c.meta = publishMeta
		c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
		c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
		c.clearWriteDomainPrimaryDocumentCache()
		if c.writeDomain != nil {
			c.writeDomain.observeRootDeltaPlan(deltaStats)
		}
		return len(existing), nil
	} else if commandWALIntent != nil {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
			})
			return err
		})
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		})
	}
	cleanupDeltas()
	if err != nil {
		return 0, err
	}
	if len(rootIDs) != len(rootNames) {
		return 0, unexpectedOrderedRootCountError(c.meta.Name, len(rootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, c.meta, rootNames, rootIDs)
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.clearWriteDomainPrimaryDocumentCache()
	if c.writeDomain != nil {
		c.writeDomain.observeRootDeltaPlan(deltaStats)
	}
	return len(existing), nil
}

func (c *Collection) deleteDocumentOnce(documentID []byte, predicate func(current []byte) (bool, error), commandWALIntent *backenddb.CommandWALIntent) (bool, error) {
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
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, c.meta)
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)

	if err := c.requireColumnStoreCommandWAL(c.meta, commandWALIntent); err != nil {
		_ = snap.Close()
		return false, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationDelete); err != nil {
		_ = snap.Close()
		return false, err
	}

	primaryRootName := collectionPrimaryRootName(c.meta.Name)
	commandWALActive := c.commandWALActive(commandWALIntent)
	// A conditional miss must not append a durable delete command frame. Ordinary
	// deletes retain their historical missing-key command-WAL no-op behavior.
	if predicate == nil && commandWALIntent == nil && commandWALActive {
		commandWALIntent, err = c.newCollectionDeleteCommandWALIntent([][]byte{documentID}, nil)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
	}
	if c.writeDomain != nil {
		c.writeDomain.mu.RLock()
		alreadyDeleted := c.bufferedIndexedDeleteTombstoneLocked(c.writeDomain, documentID)
		c.writeDomain.mu.RUnlock()
		if alreadyDeleted {
			_ = snap.Close()
			if commandWALIntent != nil {
				if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
					return false, err
				}
			}
			return false, nil
		}
	}
	entry, primaryRoot, err := collectionGetEntryAtCatalogRoot(snap, catalog, primaryRootName, documentID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		_ = snap.Close()
		if commandWALIntent != nil {
			if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if err != nil {
		_ = snap.Close()
		return false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		_ = snap.Close()
		if commandWALIntent != nil {
			if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if predicate != nil {
		current, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, documentID, nil)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
		if !found {
			_ = snap.Close()
			return false, nil
		}
		if columnStoreCanReconstructDocument(c.meta) {
			current, err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, documentID, current)
			if err != nil {
				_ = snap.Close()
				return false, err
			}
		}
		deleteOK, err := predicate(current)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
		if !deleteOK {
			_ = snap.Close()
			return false, nil
		}
		if commandWALIntent == nil && commandWALActive {
			commandWALIntent, err = c.newCollectionDeleteCommandWALIntent([][]byte{documentID}, nil)
			if err != nil {
				_ = snap.Close()
				return false, err
			}
		}
	}
	var semanticReclaimValue []byte
	if columnStoreRetainedPayloadUsesSemanticStreamV1(c.meta.Options.ColumnStore) {
		primaryValue, ok, err := columnRetainedSemanticStreamV1PrimaryValueForReclaim(snap, catalog, primaryRootName, documentID, entry)
		if err != nil {
			_ = snap.Close()
			return false, err
		}
		if ok {
			semanticReclaimValue = primaryValue
		}
	}
	runtimes, err := catalog.cachedIndexRuntimes()
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
	if len(c.meta.TextIndexes) > 0 {
		if err := appendTextIndexDeleteDeltas(snap, catalog, plannerOptions, [][]byte{documentID}, [][]byte{entry.Value}, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
			_ = snap.Close()
			resetCollectionTables(deltaTables)
			return false, err
		}
	}
	if err := appendColumnRetainedSemanticStreamV1ReclaimDeltas(c.db, snap, catalog, c.meta, [][]byte{documentID}, [][]byte{semanticReclaimValue}, nil, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		_ = snap.Close()
		resetCollectionTables(deltaTables)
		return false, err
	}
	if !commandWALActive && len(c.meta.TextIndexes) == 0 {
		if buffered, err := c.bufferIndexedDeleteTablesLocked(catalog, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, policies, deltaTables, 1); buffered || err != nil {
			_ = snap.Close()
			if err != nil {
				resetCollectionTables(deltaTables)
			}
			return buffered, err
		}
	}
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
	var newSystemRoot uint64
	var rootIDs []uint64
	var publishMeta CollectionMeta
	var publishRootNames []string
	if columnStoreWriteEnabled(c.meta) {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaBatchGroupMaybeColumn(ordered, nil, columnWritePublishInput{
				meta:             c.meta,
				catalog:          catalog,
				baseCommitSeq:    baseCommitSeq,
				baseSystemRoot:   baseSystemRoot,
				rootNames:        cloneColumnPublishRootNames(rootNames),
				baseRootIDs:      cloneColumnPublishBaseRootIDs(baseRootIDs),
				commandWALIntent: commandWALIntent,
				rawPublishLocked: true,
				operation:        ColumnPublishOperationDelete,
				documents:        columnWriteDocumentsFromIDs([][]byte{documentID}),
				rows:             1,
			})
			return err
		})
	} else if commandWALIntent != nil {
		publishMeta = c.meta
		publishRootNames = rootNames
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
			})
			return err
		})
	} else {
		publishMeta = c.meta
		publishRootNames = rootNames
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs, rootIDs)
		})
	}
	cleanupDeltas()
	if err != nil {
		return false, err
	}
	if len(rootIDs) != len(publishRootNames) {
		return false, unexpectedOrderedRootCountError(c.meta.Name, len(publishRootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, publishMeta, publishRootNames, rootIDs)
	c.meta = publishMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	c.clearWriteDomainPrimaryDocumentCache()
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
// For no-secondary-index collections, generic callback-based Update operations
// preserve synchronous publish semantics in every durability mode: pending
// writes are flushed before planning, and modified documents publish to the
// primary root before Update returns. Single-document BSON $set updates use a
// separate direct buffered path and may stage when durability mode allows
// deferred flush/publish behavior (WAL-off relaxed and WAL-on relaxed).
//
// Callback panics are recovered and returned as errors in both direct and
// combined execution. When the collection write domain combines concurrent
// updates, update may run on an internal combiner goroutine. The callback must
// not rely on caller goroutine behavior such as recover, runtime.Goexit, or
// testing.T.Fatal.
//
// Under the collection WAL target contract, WAL-on success is process-crash
// recoverable for update operations that publish before returning. Direct-
// buffered BSON $set updates in WAL-on relaxed mode defer that recoverability
// boundary until Flush/Close publishes the staged root runs. Retry after timeout
// or commit-ambiguous failure is safe only when the update function is
// idempotent or protected by an application-level guard or durable idempotency
// key.
func (c *Collection) Update(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	if err := validateCollectionUpdateInput(c, documentID, update); err != nil {
		return false, false, err
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return false, false, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		return false, false, err
	}
	if c.commandWALActive(nil) {
		results, _, err := c.updateBatchOwnedItemsWithCommandWALIntent([]updateBatchItem{{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: bytes.Clone(documentID),
				Update:     update,
			},
		}}, updateBatchModeAny, nil)
		if err != nil {
			return false, false, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
		}
		if len(results) != 1 {
			return false, false, fmt.Errorf("collections: update result count %d for single command WAL update", len(results))
		}
		if results[0].Modified {
			err = commitAmbiguousError("Update vector index maintenance", c.notifyVectorIndexesUpsert([][]byte{documentID}))
		}
		return results[0].Matched, results[0].Modified, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
	}
	var matched, modified bool
	var err error
	if combiner, domain := c.updateFastPathWithoutCreatingCombiner(); combiner != nil {
		matched, modified, err = combiner.update(c, documentID, update, bsonSetUpdate{}, false)
	} else if domain != nil {
		defer domain.finishInlineUpdateWithoutCombiner()
		domain.observeUpdateCombineInline()
		matched, modified, err = c.updateSingleInlineWithoutCombiner(domain, documentID, update, bsonSetUpdate{}, false)
	} else if combiner := c.updateCombiner(); combiner != nil {
		matched, modified, err = combiner.update(c, documentID, update, bsonSetUpdate{}, false)
	} else {
		matched, modified, err = c.updateDirect(documentID, update)
	}
	if err == nil && modified {
		err = commitAmbiguousError("Update vector index maintenance", c.notifyVectorIndexesUpsert([][]byte{documentID}))
	}
	return matched, modified, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

func validateCollectionUpdateInput(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) error {
	if err := validateCollectionUpdateDocumentInput(c, documentID); err != nil {
		return err
	}
	if update == nil {
		return errors.New("collections: update function is nil")
	}
	return nil
}

func validateCollectionUpdateDocumentInput(c *Collection, documentID []byte) error {
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
		if isRetriableCollectionMutationError(err) {
			lastErr = err
			waitBeforeCollectionMutationRetry(attempt)
			continue
		}
		return matched, modified, err
	}
	return false, false, collectionMutationRetryExhausted(lastErr)
}

func (c *Collection) updateDirectBSONSet(documentID []byte, spec bsonSetUpdate) (bool, bool, error) {
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if err := c.flushBufferedWrites(); err != nil {
		return false, false, err
	}

	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		matched, modified, err := c.updateDocumentBSONSetOnce(documentID, spec)
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
//
// Under the collection WAL target contract, WAL-on success makes the whole
// batch recoverable as one mutation boundary. Item/callback errors are
// pre-commit unless explicitly documented otherwise and must expose no partial
// batch. Post-commit failures must be commit-ambiguous for the whole batch.
func (c *Collection) UpdateBatch(items []UpdateBatchItem) ([]UpdateBatchResult, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	results, _, err := c.updateBatch(items, updateBatchModeAny)
	if err == nil {
		err = commitAmbiguousError("UpdateBatch vector index maintenance", c.notifyVectorIndexesUpdateBatch(items, results))
	}
	return results, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// UpdateBatchIfNoSecondaryUniqueIndexes applies UpdateBatch only when the
// collection has no secondary unique indexes in the planning snapshot. It
// reports batched=false without applying updates if a unique secondary index is
// present so callers can preserve ordered per-document update semantics. When
// batched=false and err=nil, the returned results are zero-valued with len(items).
func (c *Collection) UpdateBatchIfNoSecondaryUniqueIndexes(items []UpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	results, batched, err := c.updateBatch(items, updateBatchModeNoSecondaryUniqueIndexes)
	if err == nil && batched {
		err = commitAmbiguousError("UpdateBatchIfNoSecondaryUniqueIndexes vector index maintenance", c.notifyVectorIndexesUpdateBatch(items, results))
	}
	return results, batched, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// UpdateBatchIfNoSecondaryUniqueIndexChanges applies UpdateBatch only when no
// secondary unique index value changes in the planning snapshot. This lets the
// write combiner batch updates that touch non-unique fields on schemas that
// also have unique indexes, while preserving per-document fallback semantics
// for unique value mutations. When batched=false and err=nil, the returned
// results are zero-valued with len(items).
func (c *Collection) UpdateBatchIfNoSecondaryUniqueIndexChanges(items []UpdateBatchItem) ([]UpdateBatchResult, bool, error) {
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	results, batched, err := c.updateBatch(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	if err == nil && batched {
		err = commitAmbiguousError("UpdateBatchIfNoSecondaryUniqueIndexChanges vector index maintenance", c.notifyVectorIndexesUpdateBatch(items, results))
	}
	return results, batched, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
}

// ReplaceBatchWithCommandWALIntent applies existing-only full-document
// replacements with an already-appended collection update command-WAL frame. It
// is reserved for R3a deterministic apply; ordinary callers should use Update
// or UpdateBatch.
func (c *Collection) ReplaceBatchWithCommandWALIntent(ids, documents [][]byte, commandWALIntent *backenddb.CommandWALIntent) (int, int, error) {
	if commandWALIntent == nil {
		return 0, 0, errors.New("collections: ReplaceBatchWithCommandWALIntent requires command WAL intent")
	}
	unlockCoverage := c.lockVectorIndexCoverageMutation()
	defer unlockCoverage()
	items, err := replaceBatchUpdateItems(ids, documents)
	if err != nil {
		return 0, 0, err
	}
	ownedItems, err := prepareUpdateBatchItems(items)
	if err != nil {
		return 0, 0, err
	}
	if c == nil {
		return 0, 0, errCollectionNil
	}
	if c.db == nil {
		return 0, 0, errCollectionDBNil
	}
	if err := c.ensureWriteDomainOpen(); err != nil {
		return 0, 0, err
	}
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if err := c.requireColumnStoreCommandWAL(c.meta, commandWALIntent); err != nil {
		return 0, 0, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		return 0, 0, err
	}
	results, _, err := c.updateBatchOwnedItemsWithCommandWALIntent(ownedItems, updateBatchModeAny, commandWALIntent)
	if err == nil {
		err = commitAmbiguousError("ReplaceBatchWithCommandWALIntent vector index maintenance", c.notifyVectorIndexesUpdateBatch(items, results))
	}
	if err != nil {
		return 0, 0, c.invalidateVectorIndexCoverageOnAcceptedMutation(err)
	}
	matched, modified := 0, 0
	for _, result := range results {
		if result.Matched {
			matched++
		}
		if result.Modified {
			modified++
		}
	}
	return matched, modified, nil
}

func replaceBatchUpdateItems(ids, documents [][]byte) ([]UpdateBatchItem, error) {
	if len(ids) != len(documents) {
		return nil, fmt.Errorf("collections: replace ids length %d does not match documents length %d", len(ids), len(documents))
	}
	items := make([]UpdateBatchItem, len(ids))
	for i := range ids {
		id := bytes.Clone(ids[i])
		replacement := bytes.Clone(documents[i])
		items[i] = UpdateBatchItem{
			DocumentID: id,
			Update: func(doc []byte) func([]byte) ([]byte, bool, error) {
				return func(current []byte) ([]byte, bool, error) {
					if current == nil {
						return nil, false, nil
					}
					if bytes.Equal(current, doc) {
						return current, false, nil
					}
					return doc, true, nil
				}
			}(replacement),
		}
	}
	return items, nil
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
	unlockSchema := c.lockCollectionSchemaRead()
	defer unlockSchema()
	if len(items) == 0 {
		c.setLastUpdateStats(CollectionUpdateStats{})
		return nil, true, nil
	}
	ownedItems, err := prepareUpdateBatchItems(items)
	if err != nil {
		return nil, false, err
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		return nil, false, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		return nil, false, err
	}
	return c.updateBatchOwnedItems(ownedItems, mode)
}

func (c *Collection) updateBatchOwnedItems(items []updateBatchItem, mode updateBatchMode) ([]UpdateBatchResult, bool, error) {
	return c.updateBatchOwnedItemsWithCommandWALIntent(items, mode, nil)
}

func (c *Collection) updateBatchOwnedItemsWithCommandWALIntent(items []updateBatchItem, mode updateBatchMode, commandWALIntent *backenddb.CommandWALIntent) ([]UpdateBatchResult, bool, error) {
	var lastErr error
	for attempt := 0; attempt < maxCollectionMutationRetries; attempt++ {
		results, err := c.updateBatchOnce(items, mode, commandWALIntent)
		if errors.Is(err, errUpdateBatchHasSecondaryUniqueIndex) ||
			errors.Is(err, errUpdateBatchChangesSecondaryUniqueIndex) {
			return make([]UpdateBatchResult, len(items)), false, nil
		}
		// A buffered root-base mismatch means this handle still has staged writes
		// against an old root. Retrying the same items just rebuilds the same
		// stale plan; the caller must drain or surface the conflict instead.
		if isBufferedRootBaseMismatch(err) {
			return make([]UpdateBatchResult, len(items)), false, err
		}
		if isRetriableCollectionMutationError(err) {
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
		if err := validateUpdateBatchItem(item, i, seen); err != nil {
			return err
		}
	}
	return nil
}

func prepareUpdateBatchItems(items []UpdateBatchItem) ([]updateBatchItem, error) {
	out := make([]updateBatchItem, len(items))
	seen := make(map[string]struct{}, len(items))
	for i, item := range items {
		if err := validateUpdateBatchItem(item, i, seen); err != nil {
			return nil, err
		}
		out[i] = updateBatchItem{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: bytes.Clone(item.DocumentID),
				Update:     item.Update,
			},
		}
	}
	return out, nil
}

func validateUpdateBatchItem(item UpdateBatchItem, index int, seen map[string]struct{}) error {
	if len(item.DocumentID) == 0 {
		return fmt.Errorf("collections: document id cannot be empty at index %d", index)
	}
	if item.Update == nil {
		return fmt.Errorf("collections: update function is nil at index %d", index)
	}
	key := string(item.DocumentID)
	if _, ok := seen[key]; ok {
		return fmt.Errorf("%w at index %d", ErrDuplicateDocumentID, index)
	}
	seen[key] = struct{}{}
	return nil
}

func updateBatchBSONSetItemIndex(items []updateBatchItem) int {
	for i, item := range items {
		if item.hasBSONSet {
			return i
		}
	}
	return -1
}

func updateBatchItemsAllHaveBSONSet(items []updateBatchItem) bool {
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if !item.hasBSONSet {
			return false
		}
	}
	return true
}

func updateBatchItemsAllowTemplateV1StoredDocuments(items []updateBatchItem) bool {
	for _, item := range items {
		if item.allowTemplateV1StoredDocument {
			return true
		}
	}
	return false
}

func updateBatchItemError(index int, err error) error {
	if err == nil {
		return nil
	}
	return &UpdateBatchItemError{Index: index, Err: err}
}

type collectionUpdateCombiner struct {
	maxBatch     int
	idleTTL      time.Duration
	requests     chan collectionUpdateCombineRequest
	done         chan struct{}
	domain       *collectionWriteDomain
	running      atomic.Bool
	runningCount atomic.Int64
	mu           sync.RWMutex
	stopped      bool

	shardedRequests []chan collectionUpdateCombineRequest
	readyShards     chan int
	preparedBatches chan collectionUpdateCombinePreparedBatch
	laneWorkers     bool
	nextShard       int
	deferred        []collectionUpdateCombineRequest
	deferredCount   atomic.Int64

	batchScratch []collectionUpdateCombineRequest
	itemsScratch []updateBatchItem
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
	bsonSet             bsonSetUpdate
	hasBSONSet          bool
	queuedAt            time.Time
	done                chan collectionUpdateCombineResult
}

type collectionUpdateCombineResult struct {
	matched  bool
	modified bool
	err      error
}

type collectionUpdateCombinePreparedBatch struct {
	batch          []collectionUpdateCombineRequest
	plan           *updateBatchPlan
	err            error
	fallbackDirect bool
	staged         chan struct{}
}

type collectionUpdateCombineWaiter struct {
	ch chan collectionUpdateCombineResult
}

func (c *Collection) updateFastPathWithoutCreatingCombiner() (*collectionUpdateCombiner, *collectionWriteDomain) {
	if c == nil || c.db == nil || c.db.IsClosing() || c.writeDomain == nil {
		return nil, nil
	}
	domain := c.writeDomain
	if domain.closingWrites.Load() {
		return nil, nil
	}
	domain.updateCombineMu.Lock()
	defer domain.updateCombineMu.Unlock()
	if domain.updateCombineDone || domain.updateDraining != nil {
		return nil, nil
	}
	combiner := domain.updateCombiner
	if combiner != nil {
		if combiner.isStopped() {
			if domain.updateCombiner == combiner {
				domain.updateCombiner = nil
			}
		} else if combiner.hasQueuedOrRunning() {
			return combiner, nil
		}
	}
	if !domain.updateInlineQuietAfterCombinerActivity() {
		return nil, nil
	}
	if domain.updateInlineInFlight.CompareAndSwap(0, 1) {
		return nil, domain
	}
	return nil, nil
}

func (domain *collectionWriteDomain) updateInlineQuietAfterCombinerActivity() bool {
	if domain == nil {
		return false
	}
	last := domain.updateCombineLastRequestUnixNano.Load()
	if last == 0 {
		return true
	}
	return time.Now().UnixNano()-int64(last) >= int64(collectionUpdateCombineInlineQuietPeriod)
}

func (domain *collectionWriteDomain) finishInlineUpdateWithoutCombiner() {
	if domain == nil {
		return
	}
	domain.updateInlineInFlight.Store(0)
}

func (domain *collectionWriteDomain) waitInlineUpdateWithoutCombiner() {
	if domain == nil {
		return
	}
	for domain.updateInlineInFlight.Load() != 0 {
		runtime.Gosched()
		time.Sleep(100 * time.Microsecond)
	}
}

func (c *Collection) updateSingleInlineWithoutCombiner(domain *collectionWriteDomain, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), bsonSet bsonSetUpdate, hasBSONSet bool) (matched bool, modified bool, err error) {
	items := domain.prepareInlineUpdateItems(documentID, update, bsonSet, hasBSONSet)
	defer domain.clearInlineUpdateItems()
	defer func() {
		if recovered := recover(); recovered != nil {
			matched = false
			modified = false
			err = collectionUpdatePanicError("inline", recovered)
		}
	}()
	results, batched, err := c.updateBatchOwnedItems(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	if !batched && err == nil {
		if items[0].hasBSONSet {
			if c.commandWALActive(nil) {
				return c.updateBSONSetDirect(items[0].DocumentID, items[0].bsonSet)
			}
			return c.updateDirectBSONSet(items[0].DocumentID, items[0].bsonSet)
		}
		return c.updateDirect(items[0].DocumentID, items[0].Update)
	}
	if err != nil {
		var itemErr *UpdateBatchItemError
		if errors.As(err, &itemErr) && itemErr.Index == 0 && itemErr.Err != nil {
			return false, false, itemErr.Err
		}
		return false, false, err
	}
	if len(results) != 1 {
		return false, false, fmt.Errorf("collections: inline update result count %d for single update", len(results))
	}
	return results[0].Matched, results[0].Modified, nil
}

func (domain *collectionWriteDomain) prepareInlineUpdateItems(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), bsonSet bsonSetUpdate, hasBSONSet bool) []updateBatchItem {
	if domain == nil {
		return []updateBatchItem{{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: documentID,
				Update:     update,
			},
			bsonSet:    bsonSet,
			hasBSONSet: hasBSONSet,
		}}
	}
	var ownedDocumentID []byte
	if len(documentID) <= len(domain.updateInlineDocumentID) {
		copy(domain.updateInlineDocumentID[:], documentID)
		ownedDocumentID = domain.updateInlineDocumentID[:len(documentID):len(documentID)]
	} else {
		if cap(domain.updateInlineDocumentIDHeap) < len(documentID) {
			domain.updateInlineDocumentIDHeap = make([]byte, len(documentID))
		} else {
			domain.updateInlineDocumentIDHeap = domain.updateInlineDocumentIDHeap[:len(documentID)]
		}
		copy(domain.updateInlineDocumentIDHeap, documentID)
		ownedDocumentID = domain.updateInlineDocumentIDHeap[:len(documentID):len(documentID)]
	}
	domain.updateInlineItems[0] = updateBatchItem{
		UpdateBatchItem: UpdateBatchItem{
			DocumentID: ownedDocumentID,
			Update:     update,
		},
		bsonSet:    bsonSet,
		hasBSONSet: hasBSONSet,
	}
	return domain.updateInlineItems[:]
}

func (domain *collectionWriteDomain) clearInlineUpdateItems() {
	if domain == nil {
		return
	}
	domain.updateInlineItems[0] = updateBatchItem{}
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
		combiner := domain.newUpdateCombinerLocked()
		domain.updateCombiner = combiner
		domain.updateCombineMu.Unlock()
		combiner.start()
		return combiner
	}
}

func (domain *collectionWriteDomain) newUpdateCombinerLocked() *collectionUpdateCombiner {
	shards := domain.updateCombineShardCountLocked()
	combiner := &collectionUpdateCombiner{
		maxBatch: defaultCollectionUpdateCombineMaxBatch,
		idleTTL:  domain.updateCombineIdleTTL(),
		done:     make(chan struct{}),
		domain:   domain,
	}
	if shards <= 1 {
		combiner.requests = make(chan collectionUpdateCombineRequest, defaultCollectionUpdateCombineMaxBatch*4)
		return combiner
	}
	combiner.shardedRequests = make([]chan collectionUpdateCombineRequest, shards)
	for i := range combiner.shardedRequests {
		combiner.shardedRequests[i] = make(chan collectionUpdateCombineRequest, defaultCollectionUpdateCombineMaxBatch*4)
	}
	combiner.readyShards = make(chan int, shards*4)
	combiner.laneWorkers = domain.updateCombineLaneWorkers
	if combiner.laneWorkers {
		combiner.preparedBatches = make(chan collectionUpdateCombinePreparedBatch, shards*4)
	}
	return combiner
}

func (domain *collectionWriteDomain) updateCombineShardCountLocked() int {
	if domain == nil || domain.updateCombineShards < 1 {
		return defaultCollectionUpdateCombineShards
	}
	return domain.updateCombineShards
}

func (domain *collectionWriteDomain) stopUpdateCombiner() {
	domain.drainUpdateCombiner(updateCombinerDrainClose)
}

func (domain *collectionWriteDomain) resetUpdateCombinerForProfiling() {
	domain.drainUpdateCombiner(updateCombinerDrainResetForProfiling)
}

type updateCombinerDrainMode uint8

const (
	updateCombinerDrainClose updateCombinerDrainMode = iota
	updateCombinerDrainResetForProfiling
)

func (domain *collectionWriteDomain) drainUpdateCombiner(mode updateCombinerDrainMode) {
	if domain == nil {
		return
	}
	if mode == updateCombinerDrainClose {
		domain.closingWrites.Store(true)
	}
	domain.updateCombineMu.Lock()
	combiner := domain.updateCombiner
	draining := domain.updateDraining
	domain.updateCombiner = nil
	if mode == updateCombinerDrainClose {
		domain.updateDraining = nil
		domain.updateCombineDone = true
	} else if combiner != nil {
		domain.updateDraining = combiner
	}
	domain.updateCombineMu.Unlock()
	if combiner != nil {
		combiner.stop()
	}
	if draining != nil && draining != combiner {
		draining.waitDone()
	}
	if mode == updateCombinerDrainClose {
		domain.waitInlineUpdateWithoutCombiner()
	}
	if mode != updateCombinerDrainResetForProfiling {
		return
	}
	domain.updateCombineMu.Lock()
	if combiner != nil && domain.updateDraining == combiner {
		domain.updateDraining = nil
	}
	if draining != nil && domain.updateDraining == draining {
		domain.updateDraining = nil
	}
	domain.updateCombineMu.Unlock()
}

func (combiner *collectionUpdateCombiner) update(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), bsonSet bsonSetUpdate, hasBSONSet bool) (bool, bool, error) {
	if combiner == nil || combiner.maxBatch <= 1 {
		if err := c.ensureWriteDomainOpen(); err != nil {
			return false, false, err
		}
		if hasBSONSet {
			return c.updateBSONSetDirect(documentID, bsonSet)
		}
		return c.updateDirect(documentID, update)
	}
	waiter := combiner.getWaiter()
	req := newCollectionUpdateCombineRequest(c, documentID, update, bsonSet, hasBSONSet, waiter.ch)
	detailedStats := combiner.domain != nil && combiner.domain.updateBatchDetailedStats.Load()
	var enqueueStart time.Time
	if detailedStats {
		enqueueStart = time.Now()
		req.queuedAt = enqueueStart
	}
	if !combiner.enqueue(req) {
		if detailedStats {
			combiner.domain.observeUpdateCombineEnqueue(time.Since(enqueueStart))
		}
		combiner.putWaiter(waiter)
		// Combining is a best-effort throughput optimization. Saturated or stopped
		// combiners fall back to the direct path so updates still make progress.
		if combiner.isStopped() {
			combiner.waitDone()
		}
		if err := c.ensureWriteDomainOpen(); err != nil {
			return false, false, err
		}
		if hasBSONSet {
			return c.updateBSONSetDirect(documentID, bsonSet)
		}
		return c.updateDirect(documentID, update)
	}
	if detailedStats {
		combiner.domain.observeUpdateCombineEnqueue(time.Since(enqueueStart))
	}
	var waitStart time.Time
	if detailedStats {
		waitStart = time.Now()
	}
	result, reusableWaiter := combiner.waitForUpdateResult(waiter.ch)
	if detailedStats {
		combiner.domain.observeUpdateCombineWait(time.Since(waitStart))
	}
	if reusableWaiter {
		combiner.putWaiter(waiter)
	}
	return result.matched, result.modified, result.err
}

func hashCollectionUpdateDocumentID(documentID []byte) uint64 {
	return xxhash.Sum64(documentID)
}

func (combiner *collectionUpdateCombiner) hasShardedIngress() bool {
	return combiner != nil && len(combiner.shardedRequests) > 0
}

func (combiner *collectionUpdateCombiner) hasShardWorkers() bool {
	return combiner != nil && combiner.laneWorkers && len(combiner.shardedRequests) > 0
}

func (combiner *collectionUpdateCombiner) hasQueuedOrRunning() bool {
	if combiner == nil {
		return false
	}
	if combiner.hasShardWorkers() && !combiner.isStopped() {
		return true
	}
	if combiner.running.Load() || combiner.deferredCount.Load() > 0 {
		return true
	}
	if combiner.hasShardedIngress() {
		for _, requests := range combiner.shardedRequests {
			if len(requests) > 0 {
				return true
			}
		}
		return false
	}
	return len(combiner.requests) > 0
}

func (combiner *collectionUpdateCombiner) start() {
	if combiner != nil {
		if combiner.hasShardWorkers() {
			go combiner.runShardWorkers()
			return
		}
		go combiner.run()
	}
}

func newCollectionUpdateCombineRequest(c *Collection, documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), bsonSet bsonSetUpdate, hasBSONSet bool, done chan collectionUpdateCombineResult) collectionUpdateCombineRequest {
	req := collectionUpdateCombineRequest{
		collection: c,
		update:     update,
		bsonSet:    bsonSet,
		hasBSONSet: hasBSONSet,
		done:       done,
	}
	if len(documentID) <= collectionUpdateCombineInlineDocumentIDMax {
		req.documentIDInlineLen = len(documentID)
		copy(req.documentIDInline[:], documentID)
		req.documentHash = hashCollectionUpdateDocumentID(req.documentIDInline[:req.documentIDInlineLen])
		return req
	}
	req.documentID = bytes.Clone(documentID)
	req.documentHash = hashCollectionUpdateDocumentID(req.documentID)
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
	if combiner.hasShardedIngress() {
		shard := int(req.documentHash % uint64(len(combiner.shardedRequests)))
		requests := combiner.shardedRequests[shard]
		select {
		case requests <- req:
			if !combiner.hasShardWorkers() {
				select {
				case combiner.readyShards <- shard:
				default:
				}
			}
			if combiner.domain != nil {
				combiner.domain.observeUpdateCombineRequest(len(requests))
			}
			return true
		default:
			return false
		}
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
	if combiner.hasShardedIngress() {
		for _, requests := range combiner.shardedRequests {
			close(requests)
		}
		if combiner.readyShards != nil {
			close(combiner.readyShards)
		}
		return true
	}
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
	if combiner.hasShardedIngress() {
		combiner.runSharded()
		return
	}
	if combiner.idleTTL <= 0 {
		for {
			first, ok := combiner.waitForNextRequest()
			if !ok {
				return
			}
			combiner.runBatchStartingWith(first)
		}
	}
	idle := time.NewTimer(combiner.idleTTL)
	defer idle.Stop()
	for {
		if first, ok := combiner.popDeferredRequest(); ok {
			stopAndDrainTimer(idle)
			combiner.runBatchStartingWith(first)
			idle.Reset(combiner.idleTTL)
			continue
		}
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

func (combiner *collectionUpdateCombiner) runSharded() {
	if combiner.idleTTL <= 0 {
		for {
			first, ok := combiner.waitForNextRequest()
			if !ok {
				return
			}
			combiner.runBatchStartingWith(first)
		}
	}
	idle := time.NewTimer(combiner.idleTTL)
	defer idle.Stop()
	for {
		if first, ok := combiner.popDeferredRequest(); ok {
			stopAndDrainTimer(idle)
			combiner.runBatchStartingWith(first)
			idle.Reset(combiner.idleTTL)
			continue
		}
		select {
		case _, ok := <-combiner.readyShards:
			if !ok {
				return
			}
			first, got, closed := combiner.tryDequeueShardedRequest()
			if !got {
				if closed {
					return
				}
				continue
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

func (combiner *collectionUpdateCombiner) runShardWorkers() {
	defer func() {
		_ = combiner.closeRequests()
		if combiner.done != nil {
			close(combiner.done)
		}
	}()
	var wg sync.WaitGroup
	for shard := range combiner.shardedRequests {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			combiner.runShardWorker(shard)
		}(shard)
	}
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		if combiner.preparedBatches != nil {
			close(combiner.preparedBatches)
		}
		close(workersDone)
	}()
	combiner.runPreparedBatchMerger()
	<-workersDone
}

func (combiner *collectionUpdateCombiner) runShardWorker(shard int) {
	if combiner == nil || shard < 0 || shard >= len(combiner.shardedRequests) {
		return
	}
	requests := combiner.shardedRequests[shard]
	batchCap := combiner.maxBatch
	if batchCap < 1 {
		batchCap = 1
	}
	batch := make([]collectionUpdateCombineRequest, 0, batchCap)
	deferred := make([]collectionUpdateCombineRequest, 0)
	deferredHead := 0
	itemsScratch := make([]updateBatchItem, 0, batchCap)
	detailedStats := combiner.domain != nil && combiner.domain.updateBatchDetailedStats.Load()
	requestsClosed := false
	hasDeferred := func() bool {
		return deferredHead < len(deferred)
	}
	popDeferred := func() (collectionUpdateCombineRequest, bool) {
		if !hasDeferred() {
			if len(deferred) > 0 {
				clear(deferred)
				deferred = deferred[:0]
				deferredHead = 0
			}
			return collectionUpdateCombineRequest{}, false
		}
		req := deferred[deferredHead]
		deferred[deferredHead] = collectionUpdateCombineRequest{}
		deferredHead++
		if deferredHead == len(deferred) {
			deferred = deferred[:0]
			deferredHead = 0
		} else if deferredHead >= 32 && deferredHead*2 >= len(deferred) {
			remaining := copy(deferred, deferred[deferredHead:])
			clear(deferred[remaining:])
			deferred = deferred[:remaining]
			deferredHead = 0
		}
		return req, true
	}
	for {
		first, fromDeferred := popDeferred()
		if !fromDeferred {
			if requestsClosed {
				return
			}
			var ok bool
			first, ok = <-requests
			if !ok {
				return
			}
			combiner.observeUpdateCombineDequeued(first, detailedStats)
			first.queuedAt = time.Time{}
		}
		batch = append(batch[:0], first)
		drainYields := 0
		var drainStart time.Time
		if detailedStats {
			drainStart = time.Now()
		}
		for len(batch) < batchCap {
			if requestsClosed {
				goto flush
			}
			select {
			case req, ok := <-requests:
				if !ok {
					requestsClosed = true
					goto flush
				}
				combiner.observeUpdateCombineDequeued(req, detailedStats)
				req.queuedAt = time.Time{}
				if collectionUpdateCombineBatchHasID(batch, req) {
					deferred = append(deferred, req)
					continue
				}
				batch = append(batch, req)
			default:
				if drainYields < defaultCollectionUpdateCombineLaneDrainYields {
					drainYields++
					runtime.Gosched()
					continue
				}
				goto flush
			}
		}
	flush:
		if detailedStats {
			combiner.domain.observeUpdateCombineDrain(time.Since(drainStart))
		}
		prepared := combiner.prepareBatchWithScratch(batch, &itemsScratch)
		if combiner.preparedBatches == nil {
			combiner.completePreparedBatch(prepared)
		} else {
			prepared.staged = make(chan struct{})
			combiner.preparedBatches <- prepared
			<-prepared.staged
		}
		clear(batch)
		batch = batch[:0]
		if requestsClosed && !hasDeferred() {
			return
		}
	}
}

func (combiner *collectionUpdateCombiner) prepareBatchWithScratch(batch []collectionUpdateCombineRequest, itemsScratch *[]updateBatchItem) collectionUpdateCombinePreparedBatch {
	ownedBatch := batch
	prepared := collectionUpdateCombinePreparedBatch{batch: ownedBatch}
	combiner.beginUpdateCombineRun()
	defer combiner.endUpdateCombineRun()
	detailedStats := combiner.domain != nil && combiner.domain.updateBatchDetailedStats.Load()
	var runStart time.Time
	if detailedStats {
		runStart = time.Now()
	}
	defer func() {
		if detailedStats {
			combiner.domain.observeUpdateCombineRun(time.Since(runStart))
		}
	}()
	defer func() {
		if recovered := recover(); recovered != nil {
			prepared.err = collectionUpdatePanicError("combiner_prepare", recovered)
		}
	}()
	if combiner.domain == nil ||
		combiner.domain.closingWrites.Load() ||
		collectionUpdateCombineHasDuplicateIDs(ownedBatch) ||
		!collectionUpdateCombineSameCollection(ownedBatch) {
		prepared.fallbackDirect = true
		return prepared
	}
	if itemsScratch == nil {
		localScratch := make([]updateBatchItem, 0, len(ownedBatch))
		itemsScratch = &localScratch
	}
	if cap(*itemsScratch) < len(ownedBatch) {
		*itemsScratch = make([]updateBatchItem, len(ownedBatch))
	}
	clear(*itemsScratch)
	items := (*itemsScratch)[:len(ownedBatch)]
	for i := range ownedBatch {
		req := &ownedBatch[i]
		items[i] = updateBatchItem{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: req.documentIDBytes(),
				Update:     req.update,
			},
			bsonSet:    req.bsonSet,
			hasBSONSet: req.hasBSONSet,
		}
	}
	plan, err := ownedBatch[0].collection.buildUpdateBatchPlan(items, updateBatchModeNoSecondaryUniqueIndexChanges, combiner.hasShardWorkers(), nil)
	clear(items)
	*itemsScratch = items[:0]
	if errors.Is(err, errUpdateBatchHasSecondaryUniqueIndex) ||
		errors.Is(err, errUpdateBatchChangesSecondaryUniqueIndex) {
		prepared.fallbackDirect = true
		return prepared
	}
	if err != nil {
		prepared.err = err
		return prepared
	}
	if plan != nil && plan.directBufferedUpdate == nil && len(plan.deltaTables) > 0 {
		plan.close()
		prepared.fallbackDirect = true
		return prepared
	}
	if plan != nil && plan.directBufferedUpdate == nil && len(plan.deltaTables) == 0 &&
		len(ownedBatch) > 0 && ownedBatch[0].collection != nil &&
		ownedBatch[0].collection.commandWALActive(nil) {
		plan.close()
		prepared.fallbackDirect = true
		return prepared
	}
	if plan == nil || plan.directBufferedUpdate == nil {
		prepared.plan = plan
		return prepared
	}
	prepared.plan = plan
	return prepared
}

func (combiner *collectionUpdateCombiner) runPreparedBatchMerger() {
	if combiner == nil || combiner.preparedBatches == nil {
		return
	}
	batchCap := combiner.maxBatch
	if batchCap < 1 {
		batchCap = 1
	}
	pending := make([]collectionUpdateCombinePreparedBatch, 0, batchCap)
	for first := range combiner.preparedBatches {
		pending = append(pending[:0], first)
		pendingRequests := len(first.batch)
		drainYields := 0
		for pendingRequests < batchCap {
			select {
			case prepared, ok := <-combiner.preparedBatches:
				if !ok {
					combiner.stagePreparedBatches(pending)
					return
				}
				pending = append(pending, prepared)
				pendingRequests += len(prepared.batch)
			default:
				if drainYields < defaultCollectionUpdateCombinePreparedDrainYields {
					drainYields++
					runtime.Gosched()
					continue
				}
				goto stage
			}
		}
	stage:
		combiner.stagePreparedBatches(pending)
		for i := range pending {
			pending[i] = collectionUpdateCombinePreparedBatch{}
		}
	}
}

func (combiner *collectionUpdateCombiner) stagePreparedBatches(prepared []collectionUpdateCombinePreparedBatch) {
	if len(prepared) == 0 {
		return
	}
	direct := prepared[:0]
	for _, p := range prepared {
		if p.err != nil || p.fallbackDirect || p.plan == nil || p.plan.directBufferedUpdate == nil {
			combiner.completePreparedBatch(p)
			continue
		}
		direct = append(direct, p)
	}
	if len(direct) == 0 {
		return
	}
	if len(direct) == 1 {
		combiner.stageSingleDirectPreparedBatch(direct[0])
		return
	}
	merged, collection, err := mergeDirectBufferedPreparedBatches(direct)
	if err == nil {
		var unlockCommandWALRawStage func()
		unlockCommandWALRawStage, err = collection.prepareDirectUpdateCommandWALStage(merged)
		releaseCommandWALRawStage := func() {
			if unlockCommandWALRawStage != nil {
				unlockCommandWALRawStage()
				unlockCommandWALRawStage = nil
			}
		}
		defer releaseCommandWALRawStage()
		merged.releaseCommandWALRawStage = releaseCommandWALRawStage
	}
	if err == nil {
		err = collection.withMutationLock(func() error {
			var unlockCommandWALStage func()
			if merged.bufferedCommandWALIntent != nil {
				var lockErr error
				unlockCommandWALStage, lockErr = collection.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
				if lockErr != nil {
					return lockErr
				}
				defer unlockCommandWALStage()
			}
			buffered, bufferErr := collection.bufferDirectUpdateBatchPlanLocked(merged)
			if bufferErr != nil {
				return bufferErr
			}
			if !buffered {
				return ErrConcurrentMutation
			}
			if collection.writeDomain != nil {
				collection.writeDomain.mu.Lock()
				for _, p := range direct {
					retainDirectBufferedDocumentArenaLocked(collection.writeDomain, p.plan)
				}
				collection.writeDomain.mu.Unlock()
			}
			collection.setLastUpdateStats(merged.stats)
			return nil
		})
	}
	if err != nil {
		for _, p := range direct {
			if errors.Is(err, ErrConcurrentMutation) {
				combiner.completePreparedBatchWithDirectFallback(p)
			} else {
				combiner.completePreparedBatchWithError(p, err)
			}
		}
		return
	}
	for _, p := range direct {
		combiner.completePreparedBatch(p)
	}
}

func (combiner *collectionUpdateCombiner) stageSingleDirectPreparedBatch(prepared collectionUpdateCombinePreparedBatch) {
	if prepared.plan == nil || prepared.plan.directBufferedUpdate == nil || len(prepared.batch) == 0 || prepared.batch[0].collection == nil {
		combiner.completePreparedBatchWithError(prepared, errors.New("collections: invalid prepared direct update batch"))
		return
	}
	collection := prepared.batch[0].collection
	unlockCommandWALRawStage, err := collection.prepareDirectUpdateCommandWALStage(prepared.plan)
	releaseCommandWALRawStage := func() {
		if unlockCommandWALRawStage != nil {
			unlockCommandWALRawStage()
			unlockCommandWALRawStage = nil
		}
	}
	defer releaseCommandWALRawStage()
	prepared.plan.releaseCommandWALRawStage = releaseCommandWALRawStage
	if err != nil {
		combiner.completePreparedBatchWithError(prepared, err)
		return
	}
	err = collection.withMutationLock(func() error {
		var unlockCommandWALStage func()
		if prepared.plan.bufferedCommandWALIntent != nil {
			var lockErr error
			unlockCommandWALStage, lockErr = collection.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
			if lockErr != nil {
				return lockErr
			}
			defer unlockCommandWALStage()
		}
		buffered, bufferErr := collection.bufferDirectUpdateBatchPlanLocked(prepared.plan)
		if bufferErr != nil {
			return bufferErr
		}
		if !buffered {
			return ErrConcurrentMutation
		}
		collection.setLastUpdateStats(prepared.plan.stats)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrConcurrentMutation) {
			combiner.completePreparedBatchWithDirectFallback(prepared)
			return
		}
		combiner.completePreparedBatchWithError(prepared, err)
		return
	}
	combiner.completePreparedBatch(prepared)
}

func (combiner *collectionUpdateCombiner) completePreparedBatch(prepared collectionUpdateCombinePreparedBatch) {
	defer signalPreparedBatchStaged(prepared)
	defer func() {
		if prepared.plan != nil {
			prepared.plan.close()
		}
	}()
	if prepared.err != nil {
		if completeUpdateCombineBatchWithItemFallback(prepared.batch, prepared.err) {
			if combiner.domain != nil {
				combiner.domain.observeUpdateCombineBatch(len(prepared.batch), true)
			}
			return
		}
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(prepared.batch), false)
		}
		completeUpdateCombineBatchWithError(prepared.batch, prepared.err)
		return
	}
	if prepared.fallbackDirect {
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(prepared.batch), true)
		}
		for _, req := range prepared.batch {
			completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
		}
		return
	}
	if prepared.plan == nil {
		completeUpdateCombineBatchWithError(prepared.batch, errors.New("collections: update combiner prepared nil plan"))
		return
	}
	results := prepared.plan.results
	if len(results) != len(prepared.batch) {
		completeUpdateCombineBatchWithError(prepared.batch, fmt.Errorf("collections: update combiner result count %d for prepared batch size %d", len(results), len(prepared.batch)))
		return
	}
	if combiner.domain != nil {
		combiner.domain.observeUpdateCombineBatch(len(prepared.batch), false)
	}
	for i, req := range prepared.batch {
		result := results[i]
		completeUpdateCombineRequest(req, collectionUpdateCombineResult{matched: result.Matched, modified: result.Modified})
	}
}

func (combiner *collectionUpdateCombiner) completePreparedBatchWithError(prepared collectionUpdateCombinePreparedBatch, err error) {
	defer signalPreparedBatchStaged(prepared)
	defer func() {
		if prepared.plan != nil {
			prepared.plan.close()
		}
	}()
	if completeUpdateCombineBatchWithItemFallback(prepared.batch, err) {
		if combiner.domain != nil {
			combiner.domain.observeUpdateCombineBatch(len(prepared.batch), true)
		}
		return
	}
	if combiner.domain != nil {
		combiner.domain.observeUpdateCombineBatch(len(prepared.batch), false)
	}
	completeUpdateCombineBatchWithError(prepared.batch, err)
}

func (combiner *collectionUpdateCombiner) completePreparedBatchWithDirectFallback(prepared collectionUpdateCombinePreparedBatch) {
	defer signalPreparedBatchStaged(prepared)
	defer func() {
		if prepared.plan != nil {
			prepared.plan.close()
		}
	}()
	if combiner.domain != nil {
		combiner.domain.observeUpdateCombineBatch(len(prepared.batch), true)
	}
	for _, req := range prepared.batch {
		completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
	}
}

func signalPreparedBatchStaged(prepared collectionUpdateCombinePreparedBatch) {
	if prepared.staged != nil {
		close(prepared.staged)
	}
}

func mergeDirectBufferedPreparedBatches(prepared []collectionUpdateCombinePreparedBatch) (*updateBatchPlan, *Collection, error) {
	if len(prepared) == 0 {
		return nil, nil, errors.New("collections: no prepared update batches to merge")
	}
	firstPlan := prepared[0].plan
	if firstPlan == nil || firstPlan.directBufferedUpdate == nil || len(prepared[0].batch) == 0 || prepared[0].batch[0].collection == nil {
		return nil, nil, errors.New("collections: invalid prepared direct update batch")
	}
	collection := prepared[0].batch[0].collection
	commonBufferedBase := firstPlan.bufferedBase
	commonBufferedReadGeneration := firstPlan.bufferedReadGeneration
	sameBufferedRead := true
	totalResults := 0
	totalTemplateEntries := 0
	totalPrimaryEntries := 0
	totalSecondaryRootPlans := 0
	totalCommandWALDocuments := 0
	for _, p := range prepared {
		plan := p.plan
		if plan == nil || plan.directBufferedUpdate == nil {
			return nil, nil, errors.New("collections: cannot merge non-direct update plan")
		}
		if len(p.batch) == 0 || p.batch[0].collection != collection {
			return nil, nil, errors.New("collections: cannot merge update plans across collections")
		}
		if !sameCollectionMeta(plan.meta, firstPlan.meta) {
			return nil, nil, fmt.Errorf("%w: cannot merge update plans across collection schemas", ErrConcurrentMutation)
		}
		if (plan.catalog == nil) != (firstPlan.catalog == nil) {
			return nil, nil, fmt.Errorf("%w: cannot merge update plans across collection catalogs", ErrConcurrentMutation)
		}
		if plan.catalog != nil && !sameCollectionMeta(plan.catalog.meta, firstPlan.catalog.meta) {
			return nil, nil, fmt.Errorf("%w: cannot merge update plans across collection catalog schemas", ErrConcurrentMutation)
		}
		if plan.bufferedBase != commonBufferedBase || plan.bufferedReadGeneration != commonBufferedReadGeneration {
			sameBufferedRead = false
		}
		totalResults += len(plan.results)
		totalTemplateEntries += len(plan.directBufferedUpdate.templateEntries)
		totalPrimaryEntries += len(plan.directBufferedUpdate.primaryEntries)
		totalSecondaryRootPlans += len(plan.directBufferedUpdate.secondaryRootPlans)
		totalCommandWALDocuments += len(plan.commandWALDocuments)
	}
	merged := &updateBatchPlan{
		meta:                        firstPlan.meta,
		catalog:                     firstPlan.catalog,
		baseUserRoot:                firstPlan.baseUserRoot,
		baseSystemRoot:              firstPlan.baseSystemRoot,
		baseCommitSeq:               firstPlan.baseCommitSeq,
		baseRootIDs:                 make(map[string]uint64, len(firstPlan.baseRootIDs)),
		canBufferIndexedUpdateBatch: firstPlan.canBufferIndexedUpdateBatch,
		canBufferDirectUpdateBatch:  true,
		directBufferedUpdate: &directBufferedUpdatePlan{
			templateRootName: firstPlan.directBufferedUpdate.templateRootName,
			primaryRootName:  firstPlan.directBufferedUpdate.primaryRootName,
		},
	}
	if totalResults > 0 {
		merged.results = make([]UpdateBatchResult, 0, totalResults)
	}
	if totalTemplateEntries > 0 {
		merged.directBufferedUpdate.templateEntries = make([]directBufferedRootEntry, 0, totalTemplateEntries)
	}
	if totalPrimaryEntries > 0 {
		merged.directBufferedUpdate.primaryEntries = make([]directBufferedRootEntry, 0, totalPrimaryEntries)
		if !sameBufferedRead {
			merged.directBufferedUpdate.primaryEntryReadGenerations = make([]uint64, 0, totalPrimaryEntries)
		}
	}
	if totalSecondaryRootPlans > 0 {
		merged.directBufferedUpdate.secondaryRootPlans = make([]directBufferedSecondaryRootPlan, 0, totalSecondaryRootPlans)
	}
	if totalCommandWALDocuments > 0 {
		merged.commandWALDocuments = make([]commitlog.CollectionDocument, 0, totalCommandWALDocuments)
	}
	rootIndex := make(map[string]int, len(firstPlan.rootNames))
	addRoot := func(plan *updateBatchPlan, idx int) error {
		if idx < 0 || idx >= len(plan.rootNames) || idx >= len(plan.policies) || idx >= len(plan.uniqueSecondaryIndexByRoot) {
			return errors.New("collections: invalid direct update root metadata")
		}
		rootName := plan.rootNames[idx]
		baseRoot, ok := plan.baseRootIDs[rootName]
		if !ok {
			return fmt.Errorf("collections: direct update plan missing base root for %q", rootName)
		}
		if existingIdx, ok := rootIndex[rootName]; ok {
			if merged.baseRootIDs[rootName] != baseRoot ||
				merged.uniqueSecondaryIndexByRoot[existingIdx] != plan.uniqueSecondaryIndexByRoot[idx] {
				return fmt.Errorf("%w: incompatible direct update root %q", ErrConcurrentMutation, rootName)
			}
			return nil
		}
		rootIndex[rootName] = len(merged.rootNames)
		merged.rootNames = append(merged.rootNames, rootName)
		merged.baseRootIDs[rootName] = baseRoot
		merged.policies = append(merged.policies, plan.policies[idx])
		merged.uniqueSecondaryIndexByRoot = append(merged.uniqueSecondaryIndexByRoot, plan.uniqueSecondaryIndexByRoot[idx])
		return nil
	}
	for _, p := range prepared {
		plan := p.plan
		for i := range plan.rootNames {
			if err := addRoot(plan, i); err != nil {
				return nil, nil, err
			}
		}
		merged.results = append(merged.results, plan.results...)
		addCollectionUpdateStatsForMerge(&merged.stats, plan.stats)
		merged.directBufferedUpdate.templateEntries = append(merged.directBufferedUpdate.templateEntries, plan.directBufferedUpdate.templateEntries...)
		merged.directBufferedUpdate.primaryEntries = append(merged.directBufferedUpdate.primaryEntries, plan.directBufferedUpdate.primaryEntries...)
		readGeneration := uint64(0)
		if plan.bufferedBase {
			readGeneration = plan.bufferedReadGeneration
		}
		if !sameBufferedRead {
			for range plan.directBufferedUpdate.primaryEntries {
				merged.directBufferedUpdate.primaryEntryReadGenerations = append(merged.directBufferedUpdate.primaryEntryReadGenerations, readGeneration)
			}
		}
		merged.directBufferedUpdate.secondaryRootPlans = append(merged.directBufferedUpdate.secondaryRootPlans, plan.directBufferedUpdate.secondaryRootPlans...)
		merged.directBufferedUpdate.stagedBytes += plan.directBufferedUpdate.stagedBytes
		merged.commandWALDocuments = append(merged.commandWALDocuments, plan.commandWALDocuments...)
		merged.rowRemainderBytes = saturatingAddNonNegativeInt64(merged.rowRemainderBytes, plan.rowRemainderBytes)
	}
	if sameBufferedRead {
		merged.bufferedBase = commonBufferedBase
		merged.bufferedReadGeneration = commonBufferedReadGeneration
	}
	return merged, collection, nil
}

func (c *Collection) prepareDirectUpdateCommandWALStage(plan *updateBatchPlan) (func(), error) {
	if c == nil || plan == nil || !c.commandWALActive(nil) ||
		plan.bufferedCommandWALIntent != nil ||
		!plan.canStageDirectBufferedUpdateAfterCommandWALAppend() ||
		len(plan.commandWALDocuments) == 0 {
		return nil, nil
	}
	intent, err := c.newCollectionUpdateCommandWALIntent(plan.commandWALDocuments, nil)
	if err != nil {
		return nil, err
	}
	plan.bufferedCommandWALIntent = intent
	if c.db == nil {
		return nil, nil
	}
	runTestBeforeCommandWALBufferedUpdateStageLockHook()
	unlockRawStage := c.db.LockCommandWALStaging()
	if err := c.drainCommandWALStageCoordinatorBeforeMutationWithHeldRawPublishLock(); err != nil {
		unlockRawStage()
		return nil, err
	}
	return unlockRawStage, nil
}

func addCollectionUpdateStatsForMerge(dst *CollectionUpdateStats, src CollectionUpdateStats) {
	if dst == nil {
		return
	}
	if dst.Indexes == 0 {
		dst.Indexes = src.Indexes
	}
	dst.Items += src.Items
	dst.Matched += src.Matched
	dst.Modified += src.Modified
	dst.Runs += src.Runs
	dst.BufferedBatches += src.BufferedBatches
	dst.CurrentRead += src.CurrentRead
	dst.Callback += src.Callback
	dst.StructuredUpdateApply += src.StructuredUpdateApply
	dst.StructuredUpdateApplications += src.StructuredUpdateApplications
	dst.PrepareDocuments += src.PrepareDocuments
	dst.IndexStateExtraction += src.IndexStateExtraction
	dst.OldIndexStateExtract += src.OldIndexStateExtract
	dst.NewIndexStateExtract += src.NewIndexStateExtract
	dst.UniqueIndexPreflight += src.UniqueIndexPreflight
	dst.TemplateRunBuild += src.TemplateRunBuild
	dst.PrimaryRunBuild += src.PrimaryRunBuild
	dst.IndexStateRunBuild += src.IndexStateRunBuild
	dst.SecondaryRunBuild += src.SecondaryRunBuild
	dst.SecondaryDeleteEntries += src.SecondaryDeleteEntries
	dst.SecondarySetEntries += src.SecondarySetEntries
	dst.SecondaryKeyBytes += src.SecondaryKeyBytes
	dst.IndexValueChanges += src.IndexValueChanges
	dst.IndexValueUnchanged += src.IndexValueUnchanged
	dst.MaskFallbacks += src.MaskFallbacks
	dst.UniqueIndexChecks += src.UniqueIndexChecks
	dst.UniqueIndexCheckSkips += src.UniqueIndexCheckSkips
	for _, secondaryRun := range src.SecondaryRuns {
		mergeCollectionUpdateSecondaryRunStatsForMerge(&dst.SecondaryRuns, secondaryRun)
	}
	for i := 0; i < src.IndexStatsCount && i < len(src.IndexStats); i++ {
		mergeCollectionUpdateIndexStat(&dst.IndexStats, &dst.IndexStatsCount, src.IndexStats[i])
	}
}

func mergeCollectionUpdateSecondaryRunStatsForMerge(dst *[]CollectionUpdateSecondaryRunStats, src CollectionUpdateSecondaryRunStats) {
	if dst == nil || src.IndexName == "" {
		return
	}
	for i := range *dst {
		if (*dst)[i].IndexName == src.IndexName {
			(*dst)[i].Deletes = saturatingAddNonNegativeInt((*dst)[i].Deletes, src.Deletes)
			(*dst)[i].Sets = saturatingAddNonNegativeInt((*dst)[i].Sets, src.Sets)
			(*dst)[i].KeyBytes = saturatingAddNonNegativeInt((*dst)[i].KeyBytes, src.KeyBytes)
			return
		}
	}
	*dst = append(*dst, src)
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (combiner *collectionUpdateCombiner) waitForNextRequest() (collectionUpdateCombineRequest, bool) {
	if combiner == nil {
		return collectionUpdateCombineRequest{}, false
	}
	if req, ok := combiner.popDeferredRequest(); ok {
		return req, true
	}
	if combiner.hasShardedIngress() {
		return combiner.waitForShardedRequest()
	}
	req, ok := <-combiner.requests
	return req, ok
}

func (combiner *collectionUpdateCombiner) popDeferredRequest() (collectionUpdateCombineRequest, bool) {
	if combiner == nil || len(combiner.deferred) == 0 {
		return collectionUpdateCombineRequest{}, false
	}
	req := combiner.deferred[0]
	copy(combiner.deferred, combiner.deferred[1:])
	combiner.deferred[len(combiner.deferred)-1] = collectionUpdateCombineRequest{}
	combiner.deferred = combiner.deferred[:len(combiner.deferred)-1]
	combiner.deferredCount.Add(-1)
	return req, true
}

func (combiner *collectionUpdateCombiner) deferUpdateCombineRequest(req collectionUpdateCombineRequest) {
	if combiner == nil {
		return
	}
	combiner.deferred = append(combiner.deferred, req)
	combiner.deferredCount.Add(1)
}

func (combiner *collectionUpdateCombiner) waitForShardedRequest() (collectionUpdateCombineRequest, bool) {
	for {
		if req, ok, closed := combiner.tryDequeueShardedRequest(); ok || closed {
			return req, ok
		}
		_, ok := <-combiner.readyShards
		if !ok {
			if req, got, _ := combiner.tryDequeueShardedRequest(); got {
				return req, true
			}
			return collectionUpdateCombineRequest{}, false
		}
	}
}

func (combiner *collectionUpdateCombiner) tryDequeueRequest() (collectionUpdateCombineRequest, bool, bool) {
	if combiner.hasShardedIngress() {
		return combiner.tryDequeueShardedRequest()
	}
	select {
	case req, ok := <-combiner.requests:
		return req, ok, !ok
	default:
		return collectionUpdateCombineRequest{}, false, false
	}
}

func (combiner *collectionUpdateCombiner) tryDequeueShardedRequest() (collectionUpdateCombineRequest, bool, bool) {
	if combiner == nil || len(combiner.shardedRequests) == 0 {
		return collectionUpdateCombineRequest{}, false, true
	}
	allClosed := true
	for offset := 0; offset < len(combiner.shardedRequests); offset++ {
		idx := (combiner.nextShard + offset) % len(combiner.shardedRequests)
		select {
		case req, ok := <-combiner.shardedRequests[idx]:
			if ok {
				combiner.nextShard = (idx + 1) % len(combiner.shardedRequests)
				return req, true, false
			}
		default:
			allClosed = false
		}
	}
	return collectionUpdateCombineRequest{}, false, allClosed
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
	combiner.drainClosedRequestsDirect()
	if combiner.domain != nil {
		combiner.domain.updateCombineMu.Lock()
		if combiner.domain.updateDraining == combiner {
			combiner.domain.updateDraining = nil
		}
		combiner.domain.updateCombineMu.Unlock()
	}
	return true
}

func (combiner *collectionUpdateCombiner) drainClosedRequestsDirect() {
	if combiner == nil {
		return
	}
	for {
		req, ok := combiner.popDeferredRequest()
		if !ok {
			break
		}
		completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
	}
	if combiner.hasShardedIngress() {
		for {
			req, ok, closed := combiner.tryDequeueShardedRequest()
			if ok {
				completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
				continue
			}
			if closed {
				return
			}
			return
		}
	}
	for req := range combiner.requests {
		completeUpdateCombineRequest(req, runUpdateCombineDirect(req))
	}
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
	detailedStats := combiner.domain != nil && combiner.domain.updateBatchDetailedStats.Load()
	combiner.observeUpdateCombineDequeued(first, detailedStats)
	deferredCount := len(combiner.deferred)
	for i := 0; i < deferredCount && len(batch) < batchCap; i++ {
		req, ok := combiner.popDeferredRequest()
		if !ok {
			break
		}
		batch = combiner.appendUpdateCombineRequestToBatch(batch, req)
	}
	var drainStart time.Time
	if detailedStats {
		drainStart = time.Now()
	}
	finishDrain := func() {
		if detailedStats {
			combiner.domain.observeUpdateCombineDrain(time.Since(drainStart))
		}
	}
	for len(batch) < batchCap {
		req, ok, closed := combiner.tryDequeueRequest()
		if ok {
			combiner.observeUpdateCombineDequeued(req, detailedStats)
			req.queuedAt = time.Time{}
			batch = combiner.appendUpdateCombineRequestToBatch(batch, req)
			continue
		}
		if closed {
			finishDrain()
			combiner.runBatch(batch)
			clear(batch)
			combiner.batchScratch = batch[:0]
			return
		}
		{
			if drainYields < defaultCollectionUpdateCombineDrainYields {
				drainYields++
				if combiner.drainYield != nil {
					combiner.drainYield()
				} else {
					runtime.Gosched()
				}
				continue
			}
			finishDrain()
			combiner.runBatch(batch)
			clear(batch)
			combiner.batchScratch = batch[:0]
			return
		}
	}
	finishDrain()
	combiner.runBatch(batch)
	clear(batch)
	combiner.batchScratch = batch[:0]
}

func (combiner *collectionUpdateCombiner) appendUpdateCombineRequestToBatch(batch []collectionUpdateCombineRequest, req collectionUpdateCombineRequest) []collectionUpdateCombineRequest {
	if collectionUpdateCombineBatchHasID(batch, req) {
		combiner.deferUpdateCombineRequest(req)
		return batch
	}
	return append(batch, req)
}

func collectionUpdateCombineBatchHasID(batch []collectionUpdateCombineRequest, req collectionUpdateCombineRequest) bool {
	reqDocumentID := (&req).documentIDBytes()
	hash := req.documentHash
	if hash == 0 && len(reqDocumentID) > 0 {
		hash = hashCollectionUpdateDocumentID(reqDocumentID)
	}
	for i := range batch {
		prev := &batch[i]
		prevDocumentID := prev.documentIDBytes()
		prevHash := prev.documentHash
		if prevHash == 0 && len(prevDocumentID) > 0 {
			prevHash = hashCollectionUpdateDocumentID(prevDocumentID)
		}
		if prevHash == hash && bytes.Equal(prevDocumentID, reqDocumentID) {
			return true
		}
	}
	return false
}

func (combiner *collectionUpdateCombiner) observeUpdateCombineDequeued(req collectionUpdateCombineRequest, detailedStats bool) {
	if !detailedStats || combiner == nil || combiner.domain == nil || req.queuedAt.IsZero() {
		return
	}
	combiner.domain.observeUpdateCombineQueueWait(time.Since(req.queuedAt))
}

func (combiner *collectionUpdateCombiner) beginUpdateCombineRun() {
	if combiner == nil {
		return
	}
	if combiner.runningCount.Add(1) == 1 {
		combiner.running.Store(true)
	}
}

func (combiner *collectionUpdateCombiner) endUpdateCombineRun() {
	if combiner == nil {
		return
	}
	if combiner.runningCount.Add(-1) == 0 {
		combiner.running.Store(false)
	}
}

func (combiner *collectionUpdateCombiner) runBatch(batch []collectionUpdateCombineRequest) {
	combiner.runBatchWithScratch(batch, &combiner.itemsScratch)
}

func (combiner *collectionUpdateCombiner) runBatchWithScratch(batch []collectionUpdateCombineRequest, itemsScratch *[]updateBatchItem) {
	combiner.beginUpdateCombineRun()
	defer combiner.endUpdateCombineRun()
	detailedStats := combiner.domain != nil && combiner.domain.updateBatchDetailedStats.Load()
	var runStart time.Time
	if detailedStats {
		runStart = time.Now()
	}
	defer func() {
		if detailedStats {
			combiner.domain.observeUpdateCombineRun(time.Since(runStart))
		}
	}()
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
	if itemsScratch == nil {
		localScratch := make([]updateBatchItem, 0, len(batch))
		itemsScratch = &localScratch
	}
	if cap(*itemsScratch) < len(batch) {
		*itemsScratch = make([]updateBatchItem, len(batch))
	}
	clear(*itemsScratch)
	items := (*itemsScratch)[:len(batch)]
	for i := range batch {
		req := &batch[i]
		items[i] = updateBatchItem{
			UpdateBatchItem: UpdateBatchItem{
				DocumentID: req.documentIDBytes(),
				Update:     req.update,
			},
			bsonSet:    req.bsonSet,
			hasBSONSet: req.hasBSONSet,
		}
	}
	results, batched, err := batch[0].collection.updateBatchOwnedItems(items, updateBatchModeNoSecondaryUniqueIndexChanges)
	clear(items)
	*itemsScratch = items[:0]
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
	if req.hasBSONSet {
		matched, modified, err := req.collection.updateBSONSetDirect((&req).documentIDBytes(), req.bsonSet)
		return collectionUpdateCombineResult{matched: matched, modified: modified, err: err}
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
	if req.update != nil && req.hasBSONSet {
		return errors.New("collections: update request cannot set both update function and BSON $set")
	}
	if req.update == nil && !req.hasBSONSet {
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
	domain := (*collectionWriteDomain)(nil)
	if req.collection != nil {
		domain = req.collection.writeDomain
	}
	if domain == nil || !domain.updateBatchDetailedStats.Load() {
		req.done <- result
		return
	}
	start := time.Now()
	req.done <- result
	domain.observeUpdateCombineResultDelivery(time.Since(start))
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

type concurrentRootModificationError struct {
	collectionName string
	rootName       string
}

func (err concurrentRootModificationError) Error() string {
	return fmt.Sprintf("%v: concurrent root modification detected for collection=%q root=%q", ErrConcurrentMutation, err.collectionName, err.rootName)
}

func (err concurrentRootModificationError) Unwrap() error {
	return ErrConcurrentMutation
}

func errConcurrentRootModification(collectionName, rootName string) error {
	return concurrentRootModificationError{collectionName: collectionName, rootName: rootName}
}

type bufferedRootBaseMismatchError struct {
	cause concurrentRootModificationError
}

func (err bufferedRootBaseMismatchError) Error() string {
	return fmt.Sprintf("%v: buffered root base mismatch for collection=%q root=%q", ErrConcurrentMutation, err.cause.collectionName, err.cause.rootName)
}

func (err bufferedRootBaseMismatchError) Unwrap() error {
	return err.cause
}

func errBufferedRootBaseMismatch(collectionName, rootName string) error {
	return bufferedRootBaseMismatchError{cause: concurrentRootModificationError{collectionName: collectionName, rootName: rootName}}
}

// isConcurrentRootModification includes buffered root base mismatches because
// bufferedRootBaseMismatchError unwraps to concurrentRootModificationError.
// Use isBufferedRootBaseMismatch when callers need to distinguish that case.
func isConcurrentRootModification(err error) bool {
	var rootErr concurrentRootModificationError
	return errors.As(err, &rootErr)
}

func isBufferedRootBaseMismatch(err error) bool {
	var rootErr bufferedRootBaseMismatchError
	return errors.As(err, &rootErr)
}

func (c *Collection) updateDocumentOnce(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error)) (bool, bool, error) {
	return c.updateDocumentOnceApply(documentID, update, bsonSetUpdate{}, false)
}

func (c *Collection) updateDocumentBSONSetOnce(documentID []byte, spec bsonSetUpdate) (bool, bool, error) {
	return c.updateDocumentOnceApply(documentID, nil, spec, true)
}

func (c *Collection) updateDocumentOnceApply(documentID []byte, update func(current []byte) (replacement []byte, changed bool, err error), bsonSet bsonSetUpdate, hasBSONSet bool) (bool, bool, error) {
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
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, c.meta)
	if err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if err := c.requireColumnStoreCommandWAL(c.meta, nil); err != nil {
		_ = snap.Close()
		return false, false, err
	}
	if err := requireColumnStoreWriteOperationSupported(c.meta, ColumnPublishOperationUpdate); err != nil {
		_ = snap.Close()
		return false, false, err
	}
	stats := CollectionUpdateStats{
		Items:   1,
		Indexes: len(c.meta.Indexes),
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	primaryOnlyUpdate := len(c.meta.Indexes) == 0 && len(c.meta.TextIndexes) == 0
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
	var semanticReclaimValue []byte
	if columnStoreRetainedPayloadUsesSemanticStreamV1(c.meta.Options.ColumnStore) {
		if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(currentValue); err != nil {
			_ = snap.Close()
			return false, false, err
		} else if ok {
			semanticReclaimValue = bytes.Clone(currentValue)
		}
	}
	if columnStoreCanReconstructDocument(c.meta) {
		currentValue, err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, documentID, currentValue)
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
	}
	primaryRoot := catalog.rootID(primaryRootName)

	runtimes, err := catalog.cachedIndexRuntimes()
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
	var document []byte
	var changed bool
	if hasBSONSet {
		document, changed, err = callBSONSetUpdateApply(bsonSet, currentValue)
		stats.StructuredUpdateApplications++
		stats.StructuredUpdateApply += updateBatchStatsSince(detailedStats, phaseStart)
	} else {
		document, changed, err = callCollectionUpdateCallback(update, currentValue)
		stats.Callback += updateBatchStatsSince(detailedStats, phaseStart)
	}
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
	var commandWALDocument []byte
	if normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatTemplateV1 {
		commandWALDocument = bytes.Clone(document)
	}
	phaseStart = updateBatchStatsNow(detailedStats)
	preparedDocuments, templateRecords, _, templateResolver, err := prepareInsertDocuments([][]byte{document}, plannerOptions)
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
	if commandWALDocument == nil {
		commandWALDocument = document
	}
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}
	var retainedPrimaryDocument []byte
	var retainedSemanticStreamBlocks memtable.Table
	if columnStoreNeedsRetainedPayloadTransform(c.meta) {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocuments(*c.meta.Options.ColumnStore, [][]byte{document}, columnRetainedPayloadTemplateResolver(snap, catalog))
		if err != nil {
			_ = snap.Close()
			return false, false, err
		}
		if len(prepared.documents) != 1 {
			_ = snap.Close()
			return false, false, errors.New("collections: update retained payload prepared unexpected document count")
		}
		retainedPrimaryDocument = prepared.documents[0]
		retainedSemanticStreamBlocks = prepared.semanticStreamBlocks
		templateRecords = append(templateRecords, prepared.templateRecords...)
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

	rootNames := make([]string, 0, 2+len(runtimes)+len(c.meta.TextIndexes)*3)
	baseRootIDs := make(map[string]uint64, 2+len(runtimes)+len(c.meta.TextIndexes)*3)
	policies := make([]backenddb.OrderedRootStoragePolicy, 0, 2+len(runtimes)+len(c.meta.TextIndexes)*3)
	deltaTables := make([]memtable.Table, 0, 2+len(runtimes)+len(c.meta.TextIndexes)*3)

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
	primaryDocument := document
	var rowRemainderBytes int64
	if retainedPrimaryDocument != nil {
		primaryDocument = retainedPrimaryDocument
	}
	rowRemainderBytes = int64(len(documentID) + len(primaryDocument))
	primaryTable := newCollectionRunTable(1)
	setCollectionRunValue(primaryTable, bytes.Clone(documentID), primaryDocument)
	primaryTable.Freeze()
	if pointerizedPrimaryTable, pointerized, err := pointerizeCollectionRunTableValuesForRoot(c.db, c.meta, primaryRootName, primaryTable); err != nil {
		_ = snap.Close()
		resetCollectionTables(append(deltaTables, primaryTable))
		return false, false, err
	} else if pointerized {
		deltaTables = append(deltaTables, pointerizedPrimaryTable)
		resetCollectionRunTable(primaryTable)
		primaryTable = nil
	} else {
		deltaTables = append(deltaTables, primaryTable)
	}
	if err := appendColumnRetainedSemanticStreamV1BlockDeltas(c.db, catalog, c.meta, retainedSemanticStreamBlocks, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		_ = snap.Close()
		resetCollectionRunTable(retainedSemanticStreamBlocks)
		resetCollectionTables(deltaTables)
		return false, false, err
	}
	retainedSemanticStreamBlocks = nil
	if err := appendColumnRetainedSemanticStreamV1ReclaimDeltas(c.db, snap, catalog, c.meta, [][]byte{documentID}, [][]byte{semanticReclaimValue}, [][]byte{primaryDocument}, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		_ = snap.Close()
		resetCollectionTables(deltaTables)
		return false, false, err
	}
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
				keyLen, err := deleteCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, encoded, documentID)
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
				keyLen, err := setCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, encoded, documentID)
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
	if len(c.meta.TextIndexes) > 0 {
		textChanged := []preparedBatchUpdate{{documentID: documentID, document: document}}
		if err := appendTextIndexUpdateDeltas(snap, catalog, plannerOptions, textChanged, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
			_ = snap.Close()
			resetCollectionTables(deltaTables)
			return false, false, err
		}
	}
	// Keep the base snapshot pinned through publish so page reuse cannot invalidate
	// base roots before stale-root validation rejects concurrent modifications.
	defer func() { _ = snap.Close() }()

	defer func() {
		resetCollectionTables(deltaTables)
	}()
	coalescedRootNames, publishDeltaTables, publishPolicies, cleanupCoalesced, err := coalesceCollectionRootDeltaTables(c.meta.Name, rootNames, deltaTables, policies)
	if err != nil {
		return false, false, err
	}
	defer cleanupCoalesced()
	publishDeltaTables, cleanupPointerized, err := pointerizeCollectionDataRootDeltaTables(c.db, c.meta, coalescedRootNames, publishDeltaTables)
	if err != nil {
		return false, false, err
	}
	defer cleanupPointerized()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(c.meta.Name, coalescedRootNames, publishDeltaTables, baseRootIDs, publishPolicies)
	if err != nil {
		return false, false, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(c.meta.Name, coalescedRootNames, ordered)
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(baseUserRoot, baseSystemRoot, baseCommitSeq)
	}
	var commandWALIntent *backenddb.CommandWALIntent
	var columnDocuments []columnWriteDocument
	if columnStoreWriteEnabled(c.meta) && c.commandWALActive(nil) {
		docs := []commitlog.CollectionDocument{{
			ID:       bytes.Clone(documentID),
			Document: bytes.Clone(commandWALDocument),
		}}
		columnDocuments = columnWriteDocumentsFromCommitLog(docs)
		commandWALIntent, err = c.newCollectionUpdateCommandWALIntent(docs, nil)
		if err != nil {
			return false, false, err
		}
	}
	phaseStart = updateBatchStatsNow(detailedStats)
	var newSystemRoot uint64
	var rootIDs []uint64
	var publishMeta CollectionMeta
	var publishRootNames []string
	if columnStoreWriteEnabled(c.meta) {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaBatchGroupMaybeColumn(ordered, preflight, columnWritePublishInput{
				meta:              c.meta,
				catalog:           catalog,
				baseCommitSeq:     baseCommitSeq,
				baseSystemRoot:    baseSystemRoot,
				rootNames:         cloneColumnPublishRootNames(coalescedRootNames),
				baseRootIDs:       cloneColumnPublishBaseRootIDs(baseRootIDs),
				commandWALIntent:  commandWALIntent,
				rawPublishLocked:  true,
				operation:         ColumnPublishOperationUpdate,
				documents:         columnDocuments,
				rows:              1,
				rowRemainderBytes: rowRemainderBytes,
			})
			return err
		})
	} else {
		publishMeta = c.meta
		publishRootNames = coalescedRootNames
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIterator(baseCommitSeq, baseSystemRoot, coalescedRootNames, baseRootIDs, rootIDs)
		})
	}
	stats.Publish += updateBatchStatsSince(detailedStats, phaseStart)
	cleanupDeltas()
	if err != nil {
		return false, false, err
	}
	if len(rootIDs) != len(publishRootNames) {
		return false, false, unexpectedOrderedRootCountError(c.meta.Name, len(publishRootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(catalog, publishMeta, publishRootNames, rootIDs)
	c.meta = publishMeta
	c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
	c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
	if c.writeDomain != nil {
		c.writeDomain.observeRootDeltaPlan(deltaStats)
		if primaryOnlyUpdate {
			c.writeDomain.observePrimaryOnlyUpdate(true, true, true, deltaStats)
		}
	}
	stats.Modified = 1
	stats.Runs = len(publishRootNames)
	c.setLastUpdateStats(stats)
	return true, true, nil
}

type preparedBatchUpdate struct {
	itemIndex                int
	documentID               []byte
	document                 []byte
	primaryDocument          []byte
	oldPrimaryValue          []byte
	hasPrimaryDocument       bool
	oldState                 orderedDocumentIndexState
	newState                 orderedDocumentIndexState
	changedIndexes           uint64
	indexStateChanged        bool
	knownAffectedIndexes     bool
	affectedIndexRuntimeMask uint64
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
	commandWALDocuments         []commitlog.CollectionDocument
	rowRemainderBytes           int64
	bufferedCommandWALIntent    *backenddb.CommandWALIntent
	bufferedCommandWALLSN       uint64
	releaseCommandWALRawStage   func()
	directBufferedUpdate        *directBufferedUpdatePlan
	uniqueSecondaryIndexByRoot  []int
	canBufferIndexedUpdateBatch bool
	canBufferDirectUpdateBatch  bool
	bufferedBase                bool
	bufferedReadGeneration      uint64
	bufferedReadBlocked         bool
	scratch                     *updateBatchPlanScratch
}

type directBufferedUpdatePlan struct {
	templateEntries             []directBufferedRootEntry
	primaryEntries              []directBufferedRootEntry
	primaryEntryReadGenerations []uint64
	secondaryRootPlans          []directBufferedSecondaryRootPlan
	templateRootName            string
	primaryRootName             string
	stagedBytes                 int64
}

func (plan *updateBatchPlan) canStageDirectBufferedUpdateAfterCommandWALAppend() bool {
	return plan != nil &&
		plan.directBufferedUpdate != nil &&
		plan.directBufferedUpdate.stagesOnlyPrimaryRoot()
}

func (direct *directBufferedUpdatePlan) stagesOnlyPrimaryRoot() bool {
	return direct != nil &&
		len(direct.templateEntries) == 0 &&
		len(direct.secondaryRootPlans) == 0
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
	indexName  string
	valueType  IndexValueType
	unique     bool
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
	entryCount := len(records)*2 + 1
	entries := make([]directBufferedRootEntry, entryCount)
	var maxID uint64
	for _, record := range records {
		if record.id > maxID {
			maxID = record.id
		}
	}
	entries[0] = directBufferedRootEntry{
		key:   templateV1NextIDKey(),
		value: encodeTemplateV1ID(maxID + 1),
		flags: node.FlagInline,
	}
	for i, record := range records {
		entryOffset := i*2 + 1
		entries[entryOffset] = directBufferedRootEntry{
			key:   templateV1HashKey(record.hash),
			value: encodeTemplateV1ID(record.id),
			flags: node.FlagInline,
		}
		entries[entryOffset+1] = directBufferedRootEntry{
			key:   templateV1RecordKey(record.id),
			value: bytes.Clone(record.raw),
			flags: node.FlagInline,
		}
	}
	return entries
}

func buildDirectBufferedPrimaryRootEntries(changed []preparedBatchUpdate, scratch *updateBatchPlanScratch) []directBufferedRootEntry {
	if len(changed) == 0 {
		return nil
	}
	entries := make([]directBufferedRootEntry, len(changed))
	for i := range changed {
		entries[i] = directBufferedRootEntry{
			key:   appendUpdateBatchPlanScratchKey(scratch, changed[i].documentID),
			value: preparedBatchUpdatePrimaryDocument(changed[i]),
			flags: node.FlagInline,
		}
	}
	return entries
}

func preparedBatchUpdatePrimaryDocument(item preparedBatchUpdate) []byte {
	if item.hasPrimaryDocument {
		return item.primaryDocument
	}
	return item.document
}

func preparedBatchUpdatesPrimaryDocumentBytes(changed []preparedBatchUpdate) int64 {
	var total int64
	for _, item := range changed {
		total = saturatingAddNonNegativeInt64(total, int64(len(item.documentID)+len(preparedBatchUpdatePrimaryDocument(item))))
	}
	return total
}

func detachUpdateBatchPlanDocumentArena(scratch *updateBatchPlanScratch) []byte {
	if scratch == nil || len(scratch.documentArena) == 0 {
		return nil
	}
	arena := scratch.documentArena
	scratch.documentArena = nil
	return arena
}

func detachUpdateBatchPlanKeyArena(scratch *updateBatchPlanScratch) []byte {
	if scratch == nil || len(scratch.keyArena) == 0 {
		return nil
	}
	arena := scratch.keyArena
	scratch.keyArena = nil
	return arena
}

func retainDirectBufferedDocumentArenaLocked(domain *collectionWriteDomain, plan *updateBatchPlan) {
	if domain == nil || plan == nil || plan.directBufferedUpdate == nil {
		return
	}
	arena := detachUpdateBatchPlanDocumentArena(plan.scratch)
	if len(arena) > 0 {
		domain.rootValueArenas = append(domain.rootValueArenas, arena)
	}
	keyArena := detachUpdateBatchPlanKeyArena(plan.scratch)
	if len(keyArena) > 0 {
		domain.rootValueArenas = append(domain.rootValueArenas, keyArena)
	}
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
			indexName:  runtime.def.name,
			valueType:  runtime.def.valueType,
			unique:     runtime.def.unique,
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
				plan.arena, key, err = appendIndexEntryKeyForValueType(plan.arena, runtime.def.valueType, encoded, item.documentID)
				if err != nil {
					return nil, 0, err
				}
				plan.entries = append(plan.entries, directBufferedSecondaryRootEntry{key: key, tombstone: true})
			}
			for _, encoded := range item.newState.valuesAt(runtimeIdx) {
				var key []byte
				var err error
				plan.arena, key, err = appendIndexEntryKeyForValueType(plan.arena, runtime.def.valueType, encoded, item.documentID)
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
	changed                []preparedBatchUpdate
	changedDocuments       [][]byte
	documentArena          []byte
	keyArena               []byte
	bsonSetDocumentScratch []byte
	stateArena             indexEncodeArena
	rootNames              []string
	baseRootIDs            map[string]uint64
	policies               []backenddb.OrderedRootStoragePolicy
	deltaTables            []memtable.Table
	uniqueSecondary        []int
}

var updateBatchPlanScratchPool sync.Pool

const (
	updateBatchPlanScratchMaxChangedCap           = 1 << 15
	updateBatchPlanScratchDocumentBytes           = 256
	updateBatchPlanScratchMaxInitialDocumentArena = 4 << 20
	updateBatchPlanScratchMaxDocumentArena        = 8 << 20
	// Keep BSON replacement scratch below the primary document arena so pooled
	// steady-state memory cannot double while retaining typical multi-MiB docs.
	updateBatchPlanScratchMaxBSONSetDocumentBytes = updateBatchPlanScratchMaxDocumentArena / 2
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
	if cap(scratch.keyArena) < itemCount*collectionUpdateCombineInlineDocumentIDMax {
		scratch.keyArena = make([]byte, 0, itemCount*collectionUpdateCombineInlineDocumentIDMax)
	} else {
		scratch.keyArena = scratch.keyArena[:0]
	}
	scratch.bsonSetDocumentScratch = scratch.bsonSetDocumentScratch[:0]
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
	if cap(scratch.keyArena) > updateBatchPlanScratchMaxDocumentArena {
		scratch.keyArena = nil
	} else {
		scratch.keyArena = scratch.keyArena[:0]
	}
	if cap(scratch.bsonSetDocumentScratch) > updateBatchPlanScratchMaxBSONSetDocumentBytes {
		scratch.bsonSetDocumentScratch = nil
	} else {
		scratch.bsonSetDocumentScratch = scratch.bsonSetDocumentScratch[:0]
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
	if scratch == nil {
		return nil
	}
	if len(document) == 0 {
		start := len(scratch.documentArena)
		scratch.documentArena = append(scratch.documentArena, 0)
		return scratch.documentArena[start:start:start]
	}
	start := len(scratch.documentArena)
	scratch.documentArena = append(scratch.documentArena, document...)
	return scratch.documentArena[start:len(scratch.documentArena):len(scratch.documentArena)]
}

func appendUpdateBatchPlanScratchKey(scratch *updateBatchPlanScratch, key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if scratch == nil {
		return bytes.Clone(key)
	}
	start := len(scratch.keyArena)
	scratch.keyArena = append(scratch.keyArena, key...)
	return scratch.keyArena[start:len(scratch.keyArena):len(scratch.keyArena)]
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

func getUpdateBatchBufferedEntrySlots(count int) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer) {
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
				clear(buffer.entries)
				buffer.arena = buffer.arena[:0]
				return buffer.entries, buffer
			}
			putUpdateBatchBufferedEntries(buffer.entries, buffer)
		}
	}
	buffer := &updateBatchBufferedEntryBuffer{
		entries: make([]updateBatchBufferedEntry, count),
	}
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

func (c *Collection) shouldUseDirectBufferedUpdatePlan(meta CollectionMeta, opts collectionOptions, canBuffer bool, mode updateBatchMode, secondaryIndexChanges updateBatchSecondaryIndexChangeSummary, changed []preparedBatchUpdate, structuredBSONSetBatch bool) bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	if len(changed) == 0 {
		return false
	}
	if columnStoreNeedsRetainedPayloadTransform(meta) {
		return false
	}
	if len(meta.TextIndexes) != 0 {
		return false
	}
	if len(meta.Indexes) == 0 {
		return c.canBufferDirectUpdateAck() &&
			mode == updateBatchModeNoSecondaryUniqueIndexChanges &&
			isBSONDocumentFormat(opts.documentFormat) &&
			structuredBSONSetBatch
	}
	if !meta.Options.BufferedIndexedWrites {
		return false
	}
	if c.commandWALActive(nil) {
		// The mode excludes secondary-unique changes at the planner boundary;
		// this runtime guard is the authoritative check for all secondary indexes.
		return c.canBufferDirectUpdateAck() &&
			mode == updateBatchModeNoSecondaryUniqueIndexChanges &&
			isBSONDocumentFormat(opts.documentFormat) &&
			structuredBSONSetBatch &&
			canBuffer &&
			!secondaryIndexChanges.any &&
			!persistIndexStateForOptions(opts)
	}
	if mode == updateBatchModeAny && collectionMetaHasSecondaryUniqueIndex(meta) {
		if secondaryIndexChanges.unique {
			return false
		}
		canBuffer = true
	}
	if !canBuffer {
		return false
	}
	return !persistIndexStateForOptions(opts)
}

func (c *Collection) updateBatchOnce(items []updateBatchItem, mode updateBatchMode, commandWALIntent *backenddb.CommandWALIntent) ([]UpdateBatchResult, error) {
	commandWALBufferedMode := c.commandWALActive(commandWALIntent) && commandWALIntent == nil && mode != updateBatchModeAny
	if (!c.commandWALActive(commandWALIntent) || commandWALBufferedMode) && (c.shouldPlanUpdateBatchWithBufferedWrites(mode, items) || commandWALBufferedMode) {
		useBufferedRead := true
		bufferedReadReplans := 0
		for {
			plan, err := c.buildUpdateBatchPlan(items, mode, useBufferedRead, commandWALIntent)
			if err != nil {
				return nil, err
			}
			if plan == nil {
				return nil, nil
			}
			var results []UpdateBatchResult
			replan := false
			replanWaitAttempt := -1
			replanBufferedRead := func() error {
				if bufferedReadReplans < maxUpdateBatchBufferedReadReplans {
					replanWaitAttempt = bufferedReadReplans
					bufferedReadReplans++
					replan = true
					return nil
				}
				if err := c.flushBufferedWrites(); err != nil {
					return err
				}
				replanWaitAttempt = maxUpdateBatchBufferedReadReplans
				useBufferedRead = false
				replan = true
				return nil
			}
			err = func() error {
				if commandWALBufferedMode &&
					plan.canStageDirectBufferedUpdateAfterCommandWALAppend() &&
					len(plan.commandWALDocuments) > 0 {
					intent, err := c.newCollectionUpdateCommandWALIntent(plan.commandWALDocuments, nil)
					if err != nil {
						return err
					}
					plan.bufferedCommandWALIntent = intent
				}
				pendingCommandWALBeforeMutation := false
				if plan.bufferedCommandWALIntent != nil && c.writeDomain != nil {
					c.writeDomain.mu.RLock()
					pendingCommandWALBeforeMutation = collectionCommandWALDomainPendingLocked(c.writeDomain)
					c.writeDomain.mu.RUnlock()
				}
				var unlockCommandWALRawStageBeforeMutation func()
				var unlockCommandWALRawStage func()
				releaseCommandWALRawStage := func() {
					if unlockCommandWALRawStage != nil {
						unlockCommandWALRawStage()
						unlockCommandWALRawStage = nil
					}
					if unlockCommandWALRawStageBeforeMutation != nil {
						unlockCommandWALRawStageBeforeMutation()
						unlockCommandWALRawStageBeforeMutation = nil
					}
				}
				if pendingCommandWALBeforeMutation && c.db != nil {
					runTestBeforeCommandWALBufferedUpdateStageLockHook()
					unlockCommandWALRawStageBeforeMutation = c.db.LockCommandWALStaging()
					defer releaseCommandWALRawStage()
				}
				return c.withMutationLock(func() error {
					if plan.bufferedCommandWALIntent != nil && c.db != nil {
						if !pendingCommandWALBeforeMutation {
							runTestBeforeCommandWALBufferedUpdateStageLockHook()
							unlockCommandWALRawStage = c.db.LockCommandWALStaging()
							defer releaseCommandWALRawStage()
							if err := c.drainCommandWALStageCoordinatorBeforeMutationWithHeldRawPublishLock(); err != nil {
								return err
							}
						}
						plan.releaseCommandWALRawStage = releaseCommandWALRawStage
					}
					var unlockCommandWALStage func()
					if plan.bufferedCommandWALIntent != nil {
						var err error
						unlockCommandWALStage, err = c.lockCommandWALStageCoordinatorWithHeldRawPublishLock()
						if err != nil {
							return err
						}
						defer unlockCommandWALStage()
					}
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
								// A buffered snapshot can go stale while update callbacks run
								// before the mutation lock. Replan against the newer buffered
								// domain instead of turning that race into a publish boundary.
								return replanBufferedRead()
							}
							return ErrConcurrentMutation
						}
						if err := c.validateUpdateBatchPlanRootDescriptors(plan); err != nil {
							return err
						}
						c.meta = plan.meta
						if commandWALBufferedMode {
							intent, err := c.newCollectionUpdateCommandWALIntent(nil, nil)
							if err != nil {
								return err
							}
							if err := c.publishCommandWALNoop(intent, false); err != nil {
								return err
							}
						}
						results = plan.results
						return nil
					}
					buffered, bufferErr := c.bufferUpdateBatchPlanLocked(plan)
					if bufferErr != nil {
						if errors.Is(bufferErr, ErrConcurrentMutation) && useBufferedRead {
							if commandWALBufferedMode && plan.bufferedCommandWALLSN != 0 {
								return &CommitAmbiguousError{Operation: "command WAL buffered update", Err: bufferErr}
							}
							if !isConcurrentRootModification(bufferErr) && plan.bufferedBase && c.bufferedUpdateBatchPlanCanStillRead(plan) {
								// The buffered domain moved between planning and staging. Keep
								// the update in the buffered layer by replanning with a fresh
								// buffered snapshot.
								return replanBufferedRead()
							}
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
						if !c.canBufferDirectUpdateAck() {
							if err := c.flushBufferedWrites(); err != nil {
								return err
							}
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
					publishIntent := commandWALIntent
					if publishIntent == nil && c.commandWALActive(nil) && len(plan.commandWALDocuments) > 0 {
						publishIntent, err = c.newCollectionUpdateCommandWALIntent(plan.commandWALDocuments, nil)
						if err != nil {
							return err
						}
					}
					results, publishErr = c.publishUpdateBatchPlanLocked(plan, publishIntent)
					return publishErr
				})
			}()
			primaryOnlyNoPublish := len(plan.meta.Indexes) == 0 && len(plan.deltaTables) == 0 && plan.directBufferedUpdate == nil
			stats := plan.stats
			plan.close()
			if err != nil {
				return nil, err
			}
			if replan {
				if replanWaitAttempt >= 0 {
					waitBeforeCollectionMutationRetry(replanWaitAttempt)
				}
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

	plan, err := c.buildUpdateBatchPlan(items, mode, false, commandWALIntent)
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
			if err := c.requireColumnStoreCommandWAL(plan.meta, commandWALIntent); err != nil {
				return err
			}
			c.meta = plan.meta
			if c.commandWALActive(commandWALIntent) {
				if commandWALIntent == nil {
					commandWALIntent, err = c.newCollectionUpdateCommandWALIntent(nil, nil)
					if err != nil {
						return err
					}
				}
				if err := c.publishCommandWALNoop(commandWALIntent, false); err != nil {
					return err
				}
			}
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
		if !c.commandWALActive(commandWALIntent) {
			if err := c.requireColumnStoreCommandWAL(plan.meta, commandWALIntent); err != nil {
				return err
			}
			buffered, bufferErr := c.bufferUpdateBatchPlanLocked(plan)
			if bufferErr != nil {
				return bufferErr
			}
			if buffered {
				results = plan.results
				return nil
			}
			if plan.directBufferedUpdate != nil {
				if !c.canBufferDirectUpdateAck() {
					if err := c.flushBufferedWrites(); err != nil {
						return err
					}
				}
				return ErrConcurrentMutation
			}
		}
		var publishErr error
		if err := c.flushBufferedWrites(); err != nil {
			return err
		}
		publishIntent := commandWALIntent
		if publishIntent == nil && c.commandWALActive(nil) && len(plan.commandWALDocuments) > 0 {
			publishIntent, err = c.newCollectionUpdateCommandWALIntent(plan.commandWALDocuments, nil)
			if err != nil {
				return err
			}
		}
		results, publishErr = c.publishUpdateBatchPlanLocked(plan, publishIntent)
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

func (c *Collection) bufferedUpdateBatchPlanCanStillRead(plan *updateBatchPlan) bool {
	if plan == nil || !plan.bufferedBase {
		return false
	}
	domain := c.writeDomain
	if domain == nil {
		return false
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return updateBatchCanReadBufferedDomainLocked(domain, plan.meta, plan.baseSystemRoot)
}

func (c *Collection) withMutationLock(fn func() error) error {
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	return fn()
}

func updateBatchCanStageDirectNoIndexTableUpdate(c *Collection, domain *collectionWriteDomain, mode updateBatchMode, items []updateBatchItem) bool {
	if c == nil || domain == nil || mode != updateBatchModeNoSecondaryUniqueIndexChanges {
		return false
	}
	if !c.canBufferDirectUpdateAck() || !updateBatchItemsAllHaveBSONSet(items) {
		return false
	}
	meta := c.meta
	if domain.loaded {
		meta = domain.meta
	}
	if len(meta.Indexes) != 0 || columnStoreNeedsRetainedPayloadTransform(meta) {
		return false
	}
	return isBSONDocumentFormat(normalizedDocumentFormat(meta.Options.DocumentFormat))
}

func (c *Collection) shouldPlanUpdateBatchWithBufferedWrites(mode updateBatchMode, items []updateBatchItem) bool {
	if c == nil || c.writeDomain == nil {
		return false
	}
	if mode == updateBatchModeAny {
		return false
	}
	domain := c.writeDomain
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	if domain.count == 0 {
		return false
	}
	if hasBufferedIndexedPendingWrites(domain) {
		return true
	}
	if hasBufferedNoIndexTableWritesLocked(domain) {
		return updateBatchCanStageDirectNoIndexTableUpdate(c, domain, mode, items)
	}
	return hasBufferedPrimaryWritesLocked(domain, c.meta.Name)
}

func updateBatchCanReadBufferedDomainLocked(domain *collectionWriteDomain, meta CollectionMeta, baseSystemRoot uint64) bool {
	if domain == nil || domain.count == 0 || !hasBufferedPrimaryWritesLocked(domain, meta.Name) {
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
	if collectionName == "" || !hasBufferedPrimaryWritesLocked(domain, collectionName) {
		return false
	}
	if len(meta.Indexes) == 0 {
		return true
	}
	return meta.Options.BufferedIndexedWrites && len(meta.Indexes) > 0
}

func (domain *collectionWriteDomain) directUpdatePlanHasPrimaryWriteConflictLocked(plan *updateBatchPlan) bool {
	if domain == nil || plan == nil || plan.directBufferedUpdate == nil {
		return true
	}
	baseGeneration := uint64(0)
	if plan.bufferedBase {
		baseGeneration = plan.bufferedReadGeneration
	}
	index := domain.primaryWriteIndex
	if index == nil && domain.count > 0 {
		collectionName := bufferedDomainCollectionName(domain, plan.meta.Name)
		index = rebuildBufferedPrimaryWriteIndex(collectionName, pendingIndexedRootRunMapLocked(domain), domain.writeGeneration)
		if index == nil && hasBufferedPrimaryOverlay(domain) {
			index = newBufferedPrimaryWriteIndex(domain.primaryOverlay.len())
		}
		if index == nil && hasPendingIndexedPrimaryOverlay(domain) {
			index = newBufferedPrimaryWriteIndex(0)
		}
		index.addOverlay(domain.primaryOverlay, domain.writeGeneration)
		for i := range domain.indexedPublishingUnits {
			index.addOverlay(domain.indexedPublishingUnits[i].primaryOverlay, domain.writeGeneration)
		}
		for i := range domain.indexedFlushUnits {
			index.addOverlay(domain.indexedFlushUnits[i].primaryOverlay, domain.writeGeneration)
		}
		domain.primaryWriteIndex = index
	}
	if index == nil || index.len() == 0 {
		return false
	}
	for i, entry := range plan.directBufferedUpdate.primaryEntries {
		entryBaseGeneration := baseGeneration
		if i < len(plan.directBufferedUpdate.primaryEntryReadGenerations) {
			entryBaseGeneration = plan.directBufferedUpdate.primaryEntryReadGenerations[i]
		}
		if generation, ok := index.generation(entry.key); ok && generation > entryBaseGeneration {
			return true
		}
	}
	return false
}

func readUpdateBatchCurrentDocumentFromBuffered(itemIndex int, buffered updateBatchBufferedRead) (updateBatchCurrentDocument, bool) {
	if buffered.enabled {
		if itemIndex >= 0 && itemIndex < len(buffered.primaryEntries) {
			entry := buffered.primaryEntries[itemIndex]
			if entry.found {
				if entry.flags&node.FlagTombstone != 0 {
					return updateBatchCurrentDocument{buffered: true}, true
				}
				return updateBatchCurrentDocument{value: entry.value, buffered: true, found: true}, true
			}
		}
	}
	return updateBatchCurrentDocument{}, false
}

func readUpdateBatchCurrentDocument(primaryReader *backenddb.SnapshotRootReader, primaryReaderOK bool, itemIndex int, documentID []byte, buffered updateBatchBufferedRead, cached updateBatchBufferedRead, dst []byte) (updateBatchCurrentDocument, error) {
	if current, ok := readUpdateBatchCurrentDocumentFromBuffered(itemIndex, buffered); ok {
		return current, nil
	}
	if current, ok := readUpdateBatchCurrentDocumentFromBuffered(itemIndex, cached); ok {
		return current, nil
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

func readUpdateBatchCurrentDocumentAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, primaryRootName string, primaryReader *backenddb.SnapshotRootReader, primaryReaderOK bool, itemIndex int, documentID []byte, buffered updateBatchBufferedRead, cached updateBatchBufferedRead, dst []byte) (updateBatchCurrentDocument, error) {
	if current, ok := readUpdateBatchCurrentDocumentFromBuffered(itemIndex, buffered); ok {
		return current, nil
	}
	if current, ok := readUpdateBatchCurrentDocumentFromBuffered(itemIndex, cached); ok {
		return current, nil
	}
	if catalog == nil || len(catalog.overlayRootIDs(primaryRootName)) == 0 {
		return readUpdateBatchCurrentDocument(primaryReader, primaryReaderOK, -1, documentID, updateBatchBufferedRead{}, updateBatchBufferedRead{}, dst)
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
	return readUpdateBatchCurrentDocument(primaryReader, primaryReaderOK, -1, documentID, updateBatchBufferedRead{}, updateBatchBufferedRead{}, dst)
}

const updateBatchBufferedPrimaryDirectProbeLimit = 1024

func snapshotUpdateBatchBufferedPrimaryEntries(runs []memtable.Table, items []updateBatchItem) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer, error) {
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

func snapshotUpdateBatchBufferedPrimaryEntriesFromIndex(index *bufferedPrimaryRunIndex, items []updateBatchItem) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer, error) {
	entries, buffer := getUpdateBatchBufferedEntries(len(items))
	if index == nil || len(items) == 0 {
		return entries, buffer, nil
	}
	for i, item := range items {
		ref, ok := index.lookupRef(item.DocumentID)
		if !ok {
			continue
		}
		value, flags, found := ref.value, ref.flags, ref.entryValid
		if !found {
			if ref.table == nil {
				continue
			}
			value, _, flags, found = ref.table.GetEntry(item.DocumentID)
		}
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

func fillUpdateBatchBufferedPrimaryEntriesFromTable(entries []updateBatchBufferedEntry, buffer *updateBatchBufferedEntryBuffer, table memtable.Table, items []updateBatchItem, missing int) int {
	if table == nil || missing <= 0 {
		return missing
	}
	for i, item := range items {
		if entries[i].found {
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
		missing--
		if missing <= 0 {
			break
		}
	}
	return missing
}

func snapshotUpdateBatchBufferedPrimaryEntriesLocked(domain *collectionWriteDomain, collectionName string, items []updateBatchItem) ([]updateBatchBufferedEntry, *updateBatchBufferedEntryBuffer, error) {
	entries, buffer := getUpdateBatchBufferedEntries(len(items))
	if domain == nil || len(items) == 0 {
		return entries, buffer, nil
	}
	missing := len(items)
	if overlay := domain.primaryOverlay; overlay != nil && overlay.len() > 0 {
		for i, item := range items {
			ref, ok := overlay.lookupRef(item.DocumentID)
			if !ok {
				continue
			}
			entries[i] = updateBatchBufferedEntry{
				value: buffer.copyValue(ref.value),
				flags: ref.flags,
				found: true,
			}
			missing--
		}
	}
	if missing <= 0 {
		return entries, buffer, nil
	}
	primaryRuns := domain.rootRuns[collectionPrimaryRootName(collectionName)]
	for i, item := range items {
		if entries[i].found {
			continue
		}
		ref, ok := lookupPendingPrimaryOverlayLocked(domain, item.DocumentID)
		if !ok {
			continue
		}
		if value, _, flags, found := getBufferedRunEntry(primaryRuns, item.DocumentID); found {
			entries[i] = updateBatchBufferedEntry{
				value: buffer.copyValue(value),
				flags: flags,
				found: true,
			}
			missing--
			continue
		}
		entries[i] = updateBatchBufferedEntry{
			value: buffer.copyValue(ref.value),
			flags: ref.flags,
			found: true,
		}
		missing--
	}
	if missing <= 0 {
		return entries, buffer, nil
	}
	if domain.primaryRunIndex != nil {
		for i, item := range items {
			if entries[i].found {
				continue
			}
			ref, ok := domain.primaryRunIndex.lookupRef(item.DocumentID)
			if !ok {
				continue
			}
			value, flags, found := ref.value, ref.flags, ref.entryValid
			if !found {
				if ref.table == nil {
					continue
				}
				value, _, flags, found = ref.table.GetEntry(item.DocumentID)
			}
			if !found {
				continue
			}
			entries[i] = updateBatchBufferedEntry{
				value: buffer.copyValue(value),
				flags: flags,
				found: true,
			}
			missing--
		}
		fillUpdateBatchBufferedPrimaryEntriesFromTable(entries, buffer, domain.table, items, missing)
		return entries, buffer, nil
	}
	runs := pendingIndexedRootRunsLocked(domain, collectionPrimaryRootName(collectionName))
	if len(runs) == 0 {
		fillUpdateBatchBufferedPrimaryEntriesFromTable(entries, buffer, domain.table, items, missing)
		return entries, buffer, nil
	}
	if len(runs) <= 1 || len(runs)*len(items) <= updateBatchBufferedPrimaryDirectProbeLimit {
		for i, item := range items {
			if entries[i].found {
				continue
			}
			if value, _, flags, found := getBufferedRunEntry(runs, item.DocumentID); found {
				entries[i] = updateBatchBufferedEntry{
					value: buffer.copyValue(value),
					flags: flags,
					found: true,
				}
			}
		}
		fillUpdateBatchBufferedPrimaryEntriesFromTable(entries, buffer, domain.table, items, missing)
		return entries, buffer, nil
	}
	targets := make(map[string]int, missing)
	for i, item := range items {
		if !entries[i].found {
			targets[string(item.DocumentID)] = i
		}
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
	fillUpdateBatchBufferedPrimaryEntriesFromTable(entries, buffer, domain.table, items, len(targets))
	return entries, buffer, nil
}

func snapshotUpdateBatchBufferedRead(domain *collectionWriteDomain, meta CollectionMeta, baseCommitSeq uint64, baseSystemRoot uint64, items []updateBatchItem, documentFormat DocumentFormat) (updateBatchBufferedRead, []memtable.Table, bool, bool, error) {
	if domain == nil {
		return updateBatchBufferedRead{}, nil, false, false, nil
	}
	domain.mu.RLock()
	read, templateRuns, blocked, staleSnapshot, needPrimaryRunIndex, err := snapshotUpdateBatchBufferedReadLocked(domain, meta, baseCommitSeq, baseSystemRoot, items, documentFormat, true)
	domain.mu.RUnlock()
	if err != nil || !needPrimaryRunIndex {
		return read, templateRuns, blocked, staleSnapshot, err
	}

	domain.mu.Lock()
	defer domain.mu.Unlock()
	bufferedCollectionName := bufferedDomainCollectionName(domain, meta.Name)
	if updateBatchCanReadBufferedDomainLocked(domain, meta, baseSystemRoot) && domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, bufferedCollectionName) {
		index, err := rebuildBufferedPrimaryRunIndex(bufferedCollectionName, pendingIndexedRootRunMapLocked(domain))
		if err != nil {
			return updateBatchBufferedRead{}, nil, false, false, err
		}
		if index == nil {
			index = newBufferedPrimaryRunIndex(0)
		}
		domain.primaryRunIndex = index
	}
	read, templateRuns, blocked, staleSnapshot, _, err = snapshotUpdateBatchBufferedReadLocked(domain, meta, baseCommitSeq, baseSystemRoot, items, documentFormat, false)
	return read, templateRuns, blocked, staleSnapshot, err
}

func snapshotUpdateBatchBufferedReadLocked(domain *collectionWriteDomain, meta CollectionMeta, baseCommitSeq uint64, baseSystemRoot uint64, items []updateBatchItem, documentFormat DocumentFormat, allowPrimaryRunIndexBuild bool) (updateBatchBufferedRead, []memtable.Table, bool, bool, bool, error) {
	if columnStoreNeedsRetainedPayloadTransform(meta) {
		if domain != nil && domain.count > 0 {
			return updateBatchBufferedRead{}, nil, true, false, false, nil
		}
		return updateBatchBufferedRead{}, nil, false, false, false, nil
	}
	if updateBatchCanReadBufferedDomainLocked(domain, meta, baseSystemRoot) {
		bufferedCollectionName := bufferedDomainCollectionName(domain, meta.Name)
		if allowPrimaryRunIndexBuild && domain.primaryRunIndex == nil && hasBufferedPrimaryRootRuns(domain, bufferedCollectionName) {
			return updateBatchBufferedRead{}, nil, false, false, true, nil
		}
		var primaryEntries []updateBatchBufferedEntry
		var primaryBuffer *updateBatchBufferedEntryBuffer
		var err error
		primaryEntries, primaryBuffer, err = snapshotUpdateBatchBufferedPrimaryEntriesLocked(domain, bufferedCollectionName, items)
		if err != nil {
			return updateBatchBufferedRead{}, nil, false, false, false, err
		}
		var templateRuns []memtable.Table
		if normalizedDocumentFormat(documentFormat) == DocumentFormatTemplateV1 {
			templateRuns, err = cloneCollectionRunTables(pendingIndexedRootRunsLocked(domain, collectionTemplateRootName(bufferedCollectionName)))
			if err != nil {
				return updateBatchBufferedRead{}, nil, false, false, false, err
			}
		}
		return updateBatchBufferedRead{
			enabled:         true,
			primaryEntries:  primaryEntries,
			primaryBuffer:   primaryBuffer,
			writeGeneration: domain.writeGeneration,
		}, templateRuns, false, false, false, nil
	}
	if domain.count > 0 && hasBufferedPrimaryWritesLocked(domain, meta.Name) {
		return updateBatchBufferedRead{}, nil, true, false, false, nil
	}
	if updateBatchBufferedSnapshotStaleLocked(domain, baseCommitSeq, baseSystemRoot) {
		return updateBatchBufferedRead{}, nil, false, true, false, nil
	}
	return updateBatchBufferedRead{}, nil, false, false, false, nil
}

func updateBatchBufferedSnapshotStaleLocked(domain *collectionWriteDomain, baseCommitSeq uint64, baseSystemRoot uint64) bool {
	if domain == nil || !domain.loaded || domain.catalog == nil {
		return false
	}
	if domain.baseSystemRoot == 0 || domain.baseSystemRoot == baseSystemRoot {
		return false
	}
	if domain.baseCommitSeq != 0 && baseCommitSeq != 0 && domain.baseCommitSeq <= baseCommitSeq {
		return false
	}
	return true
}

func collectionWriteDomainSnapshotStale(domain *collectionWriteDomain, baseCommitSeq uint64, baseSystemRoot uint64) bool {
	if domain == nil {
		return false
	}
	domain.mu.RLock()
	defer domain.mu.RUnlock()
	return updateBatchBufferedSnapshotStaleLocked(domain, baseCommitSeq, baseSystemRoot)
}

func (c *Collection) buildUpdateBatchPlan(items []updateBatchItem, mode updateBatchMode, useBufferedRead bool, commandWALIntent *backenddb.CommandWALIntent) (*updateBatchPlan, error) {
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
	if err := c.requireColumnStoreCommandWAL(meta, commandWALIntent); err != nil {
		_ = snap.Close()
		return nil, err
	}
	if err := requireColumnStoreWriteOperationSupported(meta, ColumnPublishOperationUpdate); err != nil {
		_ = snap.Close()
		return nil, err
	}
	stats := CollectionUpdateStats{
		Items:   len(items),
		Indexes: len(meta.Indexes),
	}
	detailedStats := c.updateBatchDetailedStatsEnabled()
	if mode == updateBatchModeNoSecondaryUniqueIndexes && collectionMetaHasSecondaryUniqueIndex(meta) {
		_ = snap.Close()
		return nil, errUpdateBatchHasSecondaryUniqueIndex
	}
	retainedPayloadTransform := columnStoreNeedsRetainedPayloadTransform(meta)
	canBufferIndexedUpdateBatch := !retainedPayloadTransform &&
		len(meta.Indexes) > 0 &&
		len(meta.TextIndexes) == 0 &&
		(!collectionMetaHasSecondaryUniqueIndex(meta) ||
			mode == updateBatchModeNoSecondaryUniqueIndexes ||
			mode == updateBatchModeNoSecondaryUniqueIndexChanges)
	plannerOptions, err := collectionPlannerOptionsForDB(c.db, meta)
	if err != nil {
		_ = snap.Close()
		return nil, err
	}
	if itemIndex := updateBatchBSONSetItemIndex(items); itemIndex >= 0 && normalizedDocumentFormat(plannerOptions.documentFormat) != DocumentFormatBSON {
		_ = snap.Close()
		return nil, updateBatchItemError(itemIndex, errBSONSetRequiresBSONFormat)
	}
	plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
	if updateBatchItemsAllowTemplateV1StoredDocuments(items) {
		plannerOptions.allowTemplateV1Stored = true
	}
	baseUserRoot := snapshotUserRoot(snap)
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	var cachedPrimaryRead updateBatchBufferedRead
	if !retainedPayloadTransform {
		cachedPrimaryRead = snapshotUpdateBatchPrimaryCache(c.writeDomain, meta, baseSystemRoot, items)
	}
	defer putUpdateBatchBufferedEntries(cachedPrimaryRead.primaryEntries, cachedPrimaryRead.primaryBuffer)
	var bufferedRead updateBatchBufferedRead
	var bufferedTemplateRuns []memtable.Table
	defer func() { resetCollectionTables(bufferedTemplateRuns) }()
	bufferedReadBlocked := false
	if domain := c.writeDomain; useBufferedRead && domain != nil {
		var staleBufferedSnapshot bool
		bufferedRead, bufferedTemplateRuns, bufferedReadBlocked, staleBufferedSnapshot, err = snapshotUpdateBatchBufferedRead(domain, meta, baseCommitSeq, baseSystemRoot, items, plannerOptions.documentFormat)
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		if staleBufferedSnapshot {
			_ = snap.Close()
			return nil, ErrConcurrentMutation
		}
		defer putUpdateBatchBufferedEntries(bufferedRead.primaryEntries, bufferedRead.primaryBuffer)
		if len(bufferedTemplateRuns) > 0 {
			plannerOptions = collectionOptionsWithBufferedTemplateV1RunsResolver(plannerOptions, bufferedTemplateRuns)
		}
		if bufferedReadBlocked && retainedPayloadTransform {
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
				bufferedReadGeneration: bufferedRead.writeGeneration,
				bufferedReadBlocked:    true,
			}
			return plan, nil
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
		current, err := readUpdateBatchCurrentDocumentAtCatalogRoot(snap, catalog, primaryRootName, &primaryReader, primaryReaderOK, i, item.DocumentID, bufferedRead, cachedPrimaryRead, currentScratch[:0])
		stats.CurrentRead += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			_ = snap.Close()
			return nil, updateBatchItemError(i, err)
		}
		if !current.found {
			continue
		}
		prepared := preparedBatchUpdate{
			itemIndex:  i,
			documentID: item.DocumentID,
		}
		if columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
			if _, _, ok, err := parseColumnRetainedSemanticStreamV1Locator(current.value); err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			} else if ok {
				prepared.oldPrimaryValue = appendUpdateBatchPlanScratchDocument(scratch, current.value)
			}
		}
		if columnStoreCanReconstructDocument(meta) {
			current.value, err = c.reconstructColumnDocumentAtSnapshot(snap, catalog, item.DocumentID, current.value)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			}
			current.buffered = false
		}
		results[i].Matched = true
		var currentID bsonIDSnapshot
		var document []byte
		var changedOne bool
		if item.hasBSONSet {
			phaseStart = updateBatchStatsNow(detailedStats)
			scratch.bsonSetDocumentScratch, document, changedOne, err = callBSONSetUpdateAppendReplacement(item.bsonSet, scratch.bsonSetDocumentScratch[:0], current.value)
			stats.StructuredUpdateApplications++
			stats.StructuredUpdateApply += updateBatchStatsSince(detailedStats, phaseStart)
			// When changedOne is true, document aliases bsonSetDocumentScratch
			// until it is copied into the plan document arena below. No-op
			// updates may return current.value and are discarded before staging.
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, fmt.Errorf("collections: current BSON document: %w", err))
			}
		} else {
			currentID, err = captureBSONIDSnapshot(current.value, plannerOptions)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			}
		}
		if len(runtimes) > 0 {
			if item.hasBSONSet {
				prepared.affectedIndexRuntimeMask, prepared.knownAffectedIndexes = item.bsonSet.affectedIndexMask(runtimes, plannerOptions)
			}
			if !prepared.knownAffectedIndexes || prepared.affectedIndexRuntimeMask != 0 {
				phaseStart = updateBatchStatsNow(detailedStats)
				if prepared.knownAffectedIndexes {
					prepared.oldState, err = orderedIndexStateForKnownValidDocumentRuntimeMask(current.value, runtimes, prepared.affectedIndexRuntimeMask, plannerOptions, stateArena)
				} else {
					prepared.oldState, err = orderedIndexStateForDocumentWithArena(current.value, runtimes, plannerOptions, stateArena)
				}
				phaseDuration := updateBatchStatsSince(detailedStats, phaseStart)
				stats.IndexStateExtraction += phaseDuration
				stats.OldIndexStateExtract += phaseDuration
				if err != nil {
					_ = snap.Close()
					return nil, updateBatchItemError(i, err)
				}
			}
		}
		if !item.hasBSONSet {
			phaseStart = updateBatchStatsNow(detailedStats)
			document, changedOne, err = callCollectionUpdateCallback(item.Update, current.value)
			stats.Callback += updateBatchStatsSince(detailedStats, phaseStart)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			}
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
		if !item.hasBSONSet {
			err = validateBSONReplacementPreservesIDSnapshot(currentID, document, plannerOptions)
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(i, err)
			}
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
	var preparedDocuments [][]byte
	var templateRecords []templateV1Record
	var templateResolver templateV1Resolver
	if updateBatchItemsAllHaveBSONSet(items) && normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatBSON {
		preparedDocuments = changedDocuments
	} else {
		var err error
		preparedDocuments, templateRecords, _, templateResolver, err = prepareInsertDocuments(changedDocuments, plannerOptions)
		stats.PrepareDocuments += updateBatchStatsSince(detailedStats, phaseStart)
		if err != nil {
			for i, document := range changedDocuments {
				if _, _, _, _, itemErr := prepareInsertDocuments([][]byte{document}, plannerOptions); itemErr != nil {
					_ = snap.Close()
					return nil, updateBatchItemError(changed[i].itemIndex, itemErr)
				}
			}
			_ = snap.Close()
			return nil, fmt.Errorf("collections: update batch replacement prepare: %w", err)
		}
	}
	if len(preparedDocuments) != len(changed) {
		_ = snap.Close()
		return nil, errors.New("collections: update batch prepared unexpected document count")
	}
	if templateResolver != nil {
		plannerOptions.templateResolver = templateResolver
	}
	var templateV1CommandWALDocuments []commitlog.CollectionDocument
	if normalizedDocumentFormat(plannerOptions.documentFormat) == DocumentFormatTemplateV1 {
		templateV1CommandWALDocuments = collectionDocumentsFromBatchUpdateDocuments(changed, changedDocuments)
	}
	var retainedPrimaryDocuments [][]byte
	var retainedSemanticStreamBlocks memtable.Table
	if columnStoreNeedsRetainedPayloadTransform(meta) {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocuments(*meta.Options.ColumnStore, changedDocuments, columnRetainedPayloadTemplateResolver(snap, catalog))
		if err != nil {
			_ = snap.Close()
			return nil, err
		}
		if len(prepared.documents) != len(changed) {
			_ = snap.Close()
			return nil, errors.New("collections: update batch retained payload prepared unexpected document count")
		}
		retainedPrimaryDocuments = prepared.documents
		retainedSemanticStreamBlocks = prepared.semanticStreamBlocks
		templateRecords = append(templateRecords, prepared.templateRecords...)
	}
	for i := range changed {
		changed[i].document = preparedDocuments[i]
		if retainedPrimaryDocuments != nil {
			changed[i].primaryDocument = appendUpdateBatchPlanScratchDocument(scratch, retainedPrimaryDocuments[i])
			changed[i].hasPrimaryDocument = true
		}
		if len(runtimes) > 0 {
			if changed[i].knownAffectedIndexes && changed[i].affectedIndexRuntimeMask == 0 {
				recordKnownUnaffectedIndexStats(runtimes, &stats)
				continue
			}
			phaseStart = updateBatchStatsNow(detailedStats)
			if changed[i].knownAffectedIndexes {
				changed[i].newState, err = orderedIndexStateForKnownValidDocumentRuntimeMask(changed[i].document, runtimes, changed[i].affectedIndexRuntimeMask, plannerOptions, stateArena)
			} else {
				changed[i].newState, err = orderedIndexStateForDocumentWithArena(changed[i].document, runtimes, plannerOptions, stateArena)
			}
			phaseDuration := updateBatchStatsSince(detailedStats, phaseStart)
			stats.IndexStateExtraction += phaseDuration
			stats.NewIndexStateExtract += phaseDuration
			if err != nil {
				_ = snap.Close()
				return nil, updateBatchItemError(changed[i].itemIndex, err)
			}
			var changedIndexes uint64
			indexStateChanged := false
			for runtimeIdx, runtime := range runtimes {
				runtimeAffected := true
				if changed[i].knownAffectedIndexes {
					runtimeAffected = changed[i].affectedIndexRuntimeMask&(uint64(1)<<uint(runtimeIdx)) != 0
				}
				runtimeChanged := false
				if runtimeAffected {
					runtimeChanged = orderedDocumentIndexRuntimeChanged(changed[i].oldState, changed[i].newState, runtimeIdx)
				}
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
	commandWALDocuments := collectionDocumentsFromPreparedBatchUpdates(changed)
	if templateV1CommandWALDocuments != nil {
		commandWALDocuments = templateV1CommandWALDocuments
	}
	rowRemainderBytes := preparedBatchUpdatesPrimaryDocumentBytes(changed)
	var secondaryIndexChanges updateBatchSecondaryIndexChangeSummary
	if c.commandWALActive(nil) {
		secondaryIndexChanges = summarizeUpdateBatchSecondaryIndexChanges(runtimes, changed)
	} else {
		secondaryIndexChanges.unique = updateBatchChangesSecondaryUniqueIndex(runtimes, changed)
	}
	if mode == updateBatchModeNoSecondaryUniqueIndexChanges && secondaryIndexChanges.unique {
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
	hasRetainedSemanticStreamBlocks := retainedSemanticStreamBlocks != nil && retainedSemanticStreamBlocks.Len() > 0
	canBufferDirectUpdateBatch := !hasRetainedSemanticStreamBlocks && c.shouldUseDirectBufferedUpdatePlan(meta, plannerOptions, canBufferIndexedUpdateBatch, mode, secondaryIndexChanges, changed, updateBatchItemsAllHaveBSONSet(items))
	if canBufferDirectUpdateBatch {
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
		primaryEntries := buildDirectBufferedPrimaryRootEntries(changed, scratch)
		rootNames = append(rootNames, primaryRootName)
		uniqueSecondary = append(uniqueSecondary, -1)
		baseRootIDs[primaryRootName] = primaryRoot
		policies = append(policies, plannerOptions.dataStoragePolicy)
		var stagedBytes int64
		for i := range templateEntries {
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(len(templateEntries[i].key)+len(templateEntries[i].value)))
		}
		for i := range changed {
			results[changed[i].itemIndex].Modified = true
			stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, int64(len(changed[i].documentID)+len(preparedBatchUpdatePrimaryDocument(changed[i]))))
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
			canBufferDirectUpdateBatch:  canBufferDirectUpdateBatch,
			bufferedBase:                bufferedRead.enabled,
			bufferedReadGeneration:      bufferedRead.writeGeneration,
			bufferedReadBlocked:         bufferedReadBlocked,
			policies:                    policies,
			commandWALDocuments:         commandWALDocuments,
			rowRemainderBytes:           rowRemainderBytes,
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

	if err := appendColumnRetainedSemanticStreamV1BlockDeltas(c.db, catalog, meta, retainedSemanticStreamBlocks, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		return nil, err
	}
	retainedSemanticStreamBlocks = nil

	rootNames = append(rootNames, primaryRootName)
	uniqueSecondary = append(uniqueSecondary, -1)
	baseRootIDs[primaryRootName] = primaryRoot
	policies = append(policies, plannerOptions.dataStoragePolicy)
	phaseStart = updateBatchStatsNow(detailedStats)
	primaryTable := newCollectionRunTable(len(changed))
	for _, item := range changed {
		primaryDocument := preparedBatchUpdatePrimaryDocument(item)
		setCollectionRunCopiedValue(primaryTable, item.documentID, primaryDocument)
		results[item.itemIndex].Modified = true
	}
	primaryTable.Freeze()
	deltaTables = append(deltaTables, primaryTable)
	var semanticReclaimIDs [][]byte
	var semanticReclaimValues [][]byte
	var semanticReplacementValues [][]byte
	for _, item := range changed {
		if len(item.oldPrimaryValue) == 0 {
			continue
		}
		semanticReclaimIDs = append(semanticReclaimIDs, item.documentID)
		semanticReclaimValues = append(semanticReclaimValues, item.oldPrimaryValue)
		semanticReplacementValues = append(semanticReplacementValues, preparedBatchUpdatePrimaryDocument(item))
	}
	if err := appendColumnRetainedSemanticStreamV1ReclaimDeltas(c.db, snap, catalog, meta, semanticReclaimIDs, semanticReclaimValues, semanticReplacementValues, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
		_ = snap.Close()
		return nil, err
	}
	for len(uniqueSecondary) < len(rootNames) {
		uniqueSecondary = append(uniqueSecondary, -1)
	}
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
					keyLen, err := deleteCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, encoded, item.documentID)
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
					keyLen, err := setCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, encoded, item.documentID)
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
	if len(meta.TextIndexes) > 0 {
		if err := appendTextIndexUpdateDeltas(snap, catalog, plannerOptions, changed, &rootNames, baseRootIDs, &policies, &deltaTables); err != nil {
			_ = snap.Close()
			return nil, err
		}
		for len(uniqueSecondary) < len(rootNames) {
			uniqueSecondary = append(uniqueSecondary, -1)
		}
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
		commandWALDocuments:         commandWALDocuments,
		rowRemainderBytes:           rowRemainderBytes,
		scratch:                     scratch,
	}
	scratchOwnedByPlan = true
	return plan, nil
}

func (c *Collection) publishUpdateBatchPlanLocked(plan *updateBatchPlan, commandWALIntent *backenddb.CommandWALIntent) ([]UpdateBatchResult, error) {
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

	coalescedRootNames, publishTables, publishPolicies, cleanupCoalesced, err := coalesceCollectionRootDeltaTables(plan.meta.Name, plan.rootNames, plan.deltaTables, plan.policies)
	if err != nil {
		return nil, err
	}
	defer cleanupCoalesced()
	publishTables, cleanupPointerized, err := pointerizeCollectionDataRootDeltaTables(c.db, plan.meta, coalescedRootNames, publishTables)
	if err != nil {
		return nil, err
	}
	defer cleanupPointerized()
	ordered, cleanupDeltas, err := buildRootDeltaBatchPublishInputsFromTables(plan.meta.Name, coalescedRootNames, publishTables, plan.baseRootIDs, publishPolicies)
	if err != nil {
		return nil, err
	}
	var deltaStats collectionRootDeltaPlanStats
	if c.writeDomain != nil {
		deltaStats = collectionRootDeltaPlanStatsFromOrdered(plan.meta.Name, coalescedRootNames, ordered)
	}
	preflight := func() error {
		return c.validateMutationRootDescriptors(plan.baseUserRoot, plan.baseSystemRoot, plan.baseCommitSeq)
	}
	detailedStats := c.updateBatchDetailedStatsEnabled()
	publishStart := updateBatchStatsNow(detailedStats)
	var newSystemRoot uint64
	var rootIDs []uint64
	if columnStoreWriteEnabled(plan.meta) {
		var publishMeta CollectionMeta
		var publishRootNames []string
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, publishMeta, publishRootNames, err = c.publishRootDeltaBatchGroupMaybeColumn(ordered, preflight, columnWritePublishInput{
				meta:              plan.meta,
				catalog:           plan.catalog,
				baseCommitSeq:     plan.baseCommitSeq,
				baseSystemRoot:    plan.baseSystemRoot,
				rootNames:         cloneColumnPublishRootNames(coalescedRootNames),
				baseRootIDs:       cloneColumnPublishBaseRootIDs(plan.baseRootIDs),
				commandWALIntent:  commandWALIntent,
				rawPublishLocked:  true,
				operation:         ColumnPublishOperationUpdate,
				documents:         columnWriteDocumentsFromCommitLog(plan.commandWALDocuments),
				rows:              plan.stats.Modified,
				rowRemainderBytes: plan.rowRemainderBytes,
			})
			return err
		})
		cleanupDeltas()
		plan.stats.Publish += updateBatchStatsSince(detailedStats, publishStart)
		if err != nil {
			return nil, err
		}
		if len(rootIDs) != len(publishRootNames) {
			return nil, unexpectedOrderedRootCountError(plan.meta.Name, len(publishRootNames), len(rootIDs))
		}
		nextCatalog := cloneCatalogWithRootUpdates(plan.catalog, publishMeta, publishRootNames, rootIDs)
		c.meta = publishMeta
		c.rememberCatalogAtSystemRoot(newSystemRoot, nextCatalog)
		c.noteWriteDomainCatalog(newSystemRoot, nextCatalog)
		if c.writeDomain != nil {
			c.writeDomain.observeRootDeltaPlan(deltaStats)
			if len(plan.meta.Indexes) == 0 {
				c.writeDomain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, true, deltaStats)
			}
		}
		return plan.results, nil
	} else if commandWALIntent != nil {
		err = c.withCommandWALPublishCoordinatorForIntent(commandWALIntent, func() error {
			newSystemRoot, rootIDs, err = c.db.PublishStagedOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered, preflight, commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIteratorForMeta(plan.meta, plan.baseCommitSeq, plan.baseSystemRoot, coalescedRootNames, plan.baseRootIDs, rootIDs)
			})
			return err
		})
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(plan.meta, plan.baseCommitSeq, plan.baseSystemRoot, coalescedRootNames, plan.baseRootIDs, rootIDs)
		})
	}
	cleanupDeltas()
	plan.stats.Publish += updateBatchStatsSince(detailedStats, publishStart)
	if err != nil {
		return nil, err
	}
	if len(rootIDs) != len(coalescedRootNames) {
		return nil, unexpectedOrderedRootCountError(plan.meta.Name, len(coalescedRootNames), len(rootIDs))
	}
	nextCatalog := cloneCatalogWithRootUpdates(plan.catalog, plan.meta, coalescedRootNames, rootIDs)
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

func directUpdatePrimaryRootPolicy(plan *updateBatchPlan, primaryRootName string) (backenddb.OrderedRootStoragePolicy, bool) {
	if plan == nil || primaryRootName == "" {
		return 0, false
	}
	for i, rootName := range plan.rootNames {
		if rootName == primaryRootName && i < len(plan.policies) {
			return plan.policies[i], true
		}
	}
	return 0, false
}

func canStageDirectPrimaryOverlay(plan *updateBatchPlan, direct *directBufferedUpdatePlan, primaryOnlyDirectUpdate bool, wouldFlush bool) bool {
	return plan != nil &&
		direct != nil &&
		!wouldFlush &&
		(primaryOnlyDirectUpdate || (len(plan.meta.Indexes) > 0 && plan.meta.Options.BufferedIndexedWrites)) &&
		len(direct.templateEntries) == 0 &&
		len(direct.secondaryRootPlans) == 0 &&
		direct.primaryRootName != "" &&
		len(direct.primaryEntries) > 0
}

func (c *Collection) stageDirectPrimaryOverlayLocked(domain *collectionWriteDomain, plan *updateBatchPlan, modifiedCount int) (bool, error) {
	if c == nil || domain == nil || plan == nil || plan.directBufferedUpdate == nil {
		return false, nil
	}
	direct := plan.directBufferedUpdate
	baseRoot, ok := plan.baseRootIDs[direct.primaryRootName]
	if !ok {
		return false, fmt.Errorf("collections: UpdateBatch collection %q direct primary overlay missing base root for %q", plan.meta.Name, direct.primaryRootName)
	}
	if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, direct.primaryRootName); ok && pendingBaseRoot != baseRoot {
		return false, errBufferedRootBaseMismatch(plan.meta.Name, direct.primaryRootName)
	}
	policy, ok := directUpdatePrimaryRootPolicy(plan, direct.primaryRootName)
	if !ok {
		return false, fmt.Errorf("collections: UpdateBatch collection %q direct primary overlay missing policy for %q", plan.meta.Name, direct.primaryRootName)
	}
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, 1)
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, 1)
	}
	if _, ok := domain.rootBaseIDs[direct.primaryRootName]; !ok {
		domain.rootBaseIDs[direct.primaryRootName] = baseRoot
	}
	domain.rootPolicies[direct.primaryRootName] = policy
	if domain.primaryOverlay == nil {
		domain.primaryOverlay = newBufferedPrimaryOverlay(len(direct.primaryEntries))
	}
	domain.primaryOverlay.addEntries(direct.primaryEntries)
	retainDirectBufferedDocumentArenaLocked(domain, plan)

	domain.loaded = true
	domain.meta = plan.meta
	domain.catalog = plan.catalog
	domain.baseCommitSeq = plan.baseCommitSeq
	domain.baseSystemRoot = plan.baseSystemRoot
	if plan.catalog != nil {
		domain.primaryRoot = plan.catalog.rootID(direct.primaryRootName)
	}
	domain.count += modifiedCount
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, direct.stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, modifiedCount)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, direct.stagedBytes)
	domain.indexedDeletesOnly = false
	domain.writeGeneration++
	domain.notePrimaryWriteEntriesLocked(direct.primaryEntries, domain.writeGeneration)
	domain.observeIndexedStage(modifiedCount, direct.stagedBytes, 0)
	if len(plan.meta.Indexes) == 0 {
		domain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, false, collectionRootDeltaPlanStats{})
	}
	c.meta = plan.meta
	plan.stats.BufferedBatches = 1
	return true, nil
}

func (c *Collection) stageDirectNoIndexTableUpdateLocked(domain *collectionWriteDomain, plan *updateBatchPlan, modifiedCount int) (bool, error) {
	if c == nil || domain == nil || plan == nil || plan.directBufferedUpdate == nil {
		return false, nil
	}
	direct := plan.directBufferedUpdate
	if len(plan.meta.Indexes) != 0 ||
		len(direct.templateEntries) != 0 ||
		len(direct.secondaryRootPlans) != 0 ||
		!hasBufferedNoIndexTableWritesLocked(domain) ||
		hasBufferedIndexedPendingWrites(domain) {
		return false, nil
	}
	if direct.primaryRootName == "" || plan.catalog == nil || direct.primaryRootName != plan.catalog.primaryRootName {
		return false, nil
	}
	baseRoot, ok := plan.baseRootIDs[direct.primaryRootName]
	if !ok {
		return false, fmt.Errorf("collections: UpdateBatch collection %q direct no-index table update missing base root for %q", plan.meta.Name, direct.primaryRootName)
	}
	if domain.primaryRoot != baseRoot {
		return false, errConcurrentRootModification(plan.meta.Name, direct.primaryRootName)
	}
	policy, ok := directUpdatePrimaryRootPolicy(plan, direct.primaryRootName)
	if ok {
		domain.storagePolicy = policy
	}
	newEntries := 0
	var stagedByteDelta int64
	for _, entry := range direct.primaryEntries {
		entryBytes := int64(len(entry.key) + len(entry.value))
		oldValue, _, _, found := domain.table.GetEntry(entry.key)
		if !found {
			newEntries++
			stagedByteDelta = saturatingAddNonNegativeInt64(stagedByteDelta, entryBytes)
		} else {
			oldBytes := int64(len(entry.key) + len(oldValue))
			if entryBytes > oldBytes {
				stagedByteDelta = saturatingAddNonNegativeInt64(stagedByteDelta, entryBytes-oldBytes)
			}
		}
		domain.table.SetEntry(entry.key, entry.value, page.ValuePtr{}, entry.flags)
	}
	domain.loaded = true
	domain.meta = plan.meta
	domain.catalog = plan.catalog
	domain.baseCommitSeq = plan.baseCommitSeq
	domain.baseSystemRoot = plan.baseSystemRoot
	if plan.catalog != nil {
		domain.primaryRoot = plan.catalog.rootID(collectionPrimaryRootName(plan.meta.Name))
	}
	domain.count = saturatingAddNonNegativeInt(domain.count, newEntries)
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedByteDelta)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, modifiedCount)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, stagedByteDelta)
	domain.indexedDeletesOnly = false
	domain.writeGeneration++
	domain.notePrimaryWriteEntriesLocked(direct.primaryEntries, domain.writeGeneration)
	if modifiedCount > 0 {
		domain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, false, collectionRootDeltaPlanStats{})
	}
	c.meta = plan.meta
	plan.stats.BufferedBatches = 1
	return true, nil
}

func (c *Collection) bufferDirectUpdateBatchPlanLocked(plan *updateBatchPlan) (buffered bool, err error) {
	if c == nil || plan == nil || plan.directBufferedUpdate == nil || len(plan.directBufferedUpdate.primaryEntries) == 0 {
		return false, nil
	}
	commandWALStageIntent := plan.bufferedCommandWALIntent
	commandWALStageAppended := false
	defer func() {
		if err != nil && commandWALStageAppended && c.db != nil {
			c.db.MarkCommandWALIntentRecoveryRequired(commandWALStageIntent)
			err = commandWALBufferedUpdateCommitAmbiguous(err)
		}
	}()
	appendCommandWALBeforeStage := func() error {
		if commandWALStageIntent == nil || plan.bufferedCommandWALLSN != 0 {
			return nil
		}
		if c.db == nil {
			return errCollectionDBNil
		}
		lsn, appendErr := c.db.AppendStagedCommandWALIntent(commandWALStageIntent, false)
		if appendErr != nil {
			return appendErr
		}
		plan.bufferedCommandWALLSN = lsn
		commandWALStageAppended = lsn != 0
		return nil
	}
	direct := plan.directBufferedUpdate
	detailedStats := c.updateBatchDetailedStatsEnabled()
	bufferStart := updateBatchStatsNow(detailedStats)
	precheckStart := updateBatchStatsNow(detailedStats)
	defer func() {
		plan.stats.BufferStage += updateBatchStatsSince(detailedStats, bufferStart)
	}()
	primaryOnlyDirectUpdate := len(plan.meta.Indexes) == 0
	if c.writeDomain == nil || !plan.canBufferDirectUpdateBatch || (!primaryOnlyDirectUpdate && !plan.meta.Options.BufferedIndexedWrites) {
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
	if err := c.requireColumnStoreCommandWAL(plan.meta, nil); err != nil {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, err
	}
	if c.commandWALActive(nil) && commandWALStageIntent == nil {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, nil
	}
	plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)

	domain := c.writeDomain
	commandWALPendingRecorded := false
	recordPendingCommandWAL := func() error {
		if plan.bufferedCommandWALLSN == 0 || commandWALPendingRecorded {
			return nil
		}
		if err := domain.recordPendingCommandWALLSNLocked(c.db, plan.bufferedCommandWALLSN); err != nil {
			return commandWALBufferedUpdateCommitAmbiguous(err)
		}
		commandWALPendingRecorded = true
		if plan.releaseCommandWALRawStage != nil {
			plan.releaseCommandWALRawStage()
		}
		return nil
	}
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
		canReadBuffered := updateBatchCanReadBufferedDomainLocked(domain, plan.meta, plan.baseSystemRoot)
		if !canReadBuffered {
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
		currentBufferedPlan := plan.bufferedBase && plan.bufferedReadGeneration == domain.writeGeneration
		if len(direct.templateEntries) > 0 && !currentBufferedPlan {
			// Template-v1 IDs are collection-global; stale direct plans can reuse
			// an ID even when their primary document writes do not conflict.
			plan.stats.BufferStageValidation += updateBatchStatsSince(detailedStats, phaseStart)
			return false, ErrConcurrentMutation
		}
		if !currentBufferedPlan && domain.directUpdatePlanHasPrimaryWriteConflictLocked(plan) {
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
		if hasPendingRootRunsForRootLocked(domain, rootName) {
			if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
				plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
				return false, errBufferedRootBaseMismatch(plan.meta.Name, rootName)
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
	shouldAutoFlushAfterAdding := false
	if !primaryOnlyDirectUpdate {
		shouldAutoFlushAfterAdding = shouldFlushBufferedIndexedWritesAfterAdding(domain, plan.meta.Options, modifiedCount, direct.stagedBytes, addedRootRuns)
	}
	requiresPreAppendFreeze := false
	if !primaryOnlyDirectUpdate && collectionMetaHasSecondaryUniqueIndex(plan.meta) && bufferedIndexedAutoFlushEnabled(plan.meta.Options) {
		for i := range plan.rootNames {
			if _, ok := updateBatchPlanUniqueSecondaryIndex(plan, i); ok {
				requiresPreAppendFreeze = true
				shouldAutoFlushAfterAdding = true
				break
			}
		}
	}
	if shouldAutoFlushAfterAdding && requiresPreAppendFreeze {
		freezeStart := updateBatchStatsNow(detailedStats)
		freezeMutableIndexedRunMapsLocked(domain)
		plan.stats.BufferStageFreeze += updateBatchStatsSince(detailedStats, freezeStart)
	}
	var preAppendFreezeTables []memtable.Table
	if shouldAutoFlushAfterAdding && !requiresPreAppendFreeze {
		preAppendFreezeTables = detachMutableIndexedRunTablesLocked(domain)
	}
	var checkpoint bufferedIndexedCheckpoint
	collectionMetaCheckpoint := c.meta
	rollbackOnError := shouldAutoFlushAfterAdding
	if shouldAutoFlushAfterAdding {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	rollbackGeneration := checkpoint.writeGeneration
	freezePreAppendTables := func() {
		if len(preAppendFreezeTables) == 0 {
			return
		}
		freezeDuration, lockReleased, relockWait := freezeIndexedRunTablesOutsideLock(domain, preAppendFreezeTables)
		if lockReleased > 0 {
			lockReleasedDuringHold += lockReleased
			if rollbackOnError && domain.writeGeneration != rollbackGeneration {
				rollbackOnError = false
			}
		}
		plan.stats.BufferStageLockWait += updateBatchStatsDuration(detailedStats, relockWait)
		plan.stats.BufferStageFreeze += updateBatchStatsDuration(detailedStats, freezeDuration)
		preAppendFreezeTables = nil
	}
	defer freezePreAppendTables()
	plan.stats.BufferStageDomainPrepare += updateBatchStatsSince(detailedStats, phaseStart)

	overlayWouldFlush := false
	if !primaryOnlyDirectUpdate {
		overlayWouldFlush = shouldFlushBufferedIndexedWritesAfterAdding(domain, plan.meta.Options, modifiedCount, direct.stagedBytes, 0)
	}
	if err := appendCommandWALBeforeStage(); err != nil {
		return false, err
	}
	if primaryOnlyDirectUpdate && commandWALStageIntent == nil && hasBufferedNoIndexTableWritesLocked(domain) && !hasBufferedIndexedPendingWrites(domain) {
		tableAppendStart := updateBatchStatsNow(detailedStats)
		buffered, err := c.stageDirectNoIndexTableUpdateLocked(domain, plan, modifiedCount)
		tableAppendDuration := updateBatchStatsSince(detailedStats, tableAppendStart)
		plan.stats.BufferStagePrimaryAppend += tableAppendDuration
		plan.stats.BufferStageRootAppend += tableAppendDuration
		return buffered, err
	}
	if canStageDirectPrimaryOverlay(plan, direct, primaryOnlyDirectUpdate, shouldAutoFlushAfterAdding || overlayWouldFlush) {
		overlayAppendStart := updateBatchStatsNow(detailedStats)
		buffered, err := c.stageDirectPrimaryOverlayLocked(domain, plan, modifiedCount)
		overlayAppendDuration := updateBatchStatsSince(detailedStats, overlayAppendStart)
		plan.stats.BufferStagePrimaryAppend += overlayAppendDuration
		plan.stats.BufferStageRootAppend += overlayAppendDuration
		if err == nil && buffered {
			if err := recordPendingCommandWAL(); err != nil {
				return false, err
			}
		}
		return buffered, err
	}
	ensureBufferedPrimaryRunIndexLocked(domain, len(direct.primaryEntries))
	materializePrimaryOverlayLocked(domain)

	phaseStart = updateBatchStatsNow(detailedStats)
	rootTableStart := phaseStart
	rootTables := make(map[string]memtable.Table, len(plan.rootNames))
	actualRootRuns := 0
	for i, rootName := range plan.rootNames {
		baseRoot := plan.baseRootIDs[rootName]
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			return false, errBufferedRootBaseMismatch(plan.meta.Name, rootName)
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
	plan.stats.BufferStageRootTable += updateBatchStatsSince(detailedStats, rootTableStart)
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
	primaryAppendStart := updateBatchStatsNow(detailedStats)
	if err := applyDirectBufferedRootEntries(primaryTable, direct.primaryEntries); err != nil {
		return false, err
	}
	for _, entry := range direct.primaryEntries {
		if primaryIndexKeys != nil {
			primaryIndexKeys = append(primaryIndexKeys, entry.key)
		}
	}
	plan.stats.BufferStagePrimaryAppend += updateBatchStatsSince(detailedStats, primaryAppendStart)
	if primaryIndexKeys != nil {
		addBufferedPrimaryRunIndexKeys(domain.primaryRunIndex, primaryIndexKeys, primaryTable)
	}
	if len(direct.secondaryRootPlans) > 0 {
		secondaryAppendStart := updateBatchStatsNow(detailedStats)
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
				if rollbackOnError {
					rollbackBufferedIndexedDomain(domain, checkpoint)
					c.meta = collectionMetaCheckpoint
				}
				return false, err
			}
		}
		plan.stats.BufferStageSecondaryAppend += updateBatchStatsSince(detailedStats, secondaryAppendStart)
	}
	plan.stats.BufferStageRootAppend += updateBatchStatsSince(detailedStats, phaseStart)
	retainDirectBufferedDocumentArenaLocked(domain, plan)
	stagePrimaryDocumentCacheEntriesLocked(domain, plan.meta, plan.baseSystemRoot, direct.primaryEntries)

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
	domain.indexedDeletesOnly = false
	domain.writeGeneration++
	domain.notePrimaryWriteEntriesLocked(direct.primaryEntries, domain.writeGeneration)
	if rollbackOnError {
		rollbackGeneration = domain.writeGeneration
	}
	domain.observeIndexedStage(modifiedCount, direct.stagedBytes, actualRootRuns)
	if primaryOnlyDirectUpdate {
		domain.observePrimaryOnlyUpdateBatch(plan.stats.Items, plan.stats.Matched, plan.stats.Modified, false, collectionRootDeltaPlanStats{})
	}
	c.meta = plan.meta
	var compactedObsolete []memtable.Table
	if !primaryOnlyDirectUpdate {
		var err error
		compactedObsolete, err = maybeCompactBufferedIndexedMutableRunsLocked(domain, plan.meta.Options)
		if err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	// Record after staging before any synchronous threshold flush; the flush
	// clears the pending range after it advances AppliedCommandLSN.
	if err := recordPendingCommandWAL(); err != nil {
		return false, err
	}
	if !primaryOnlyDirectUpdate && shouldFlushBufferedIndexedWrites(domain, plan.meta.Options) {
		freezePreAppendTables()
		flushDuration, lockReleased, relockWait, err := c.flushBufferedIndexedAfterThresholdLocked(domain, plan.meta.Options)
		if lockReleased > 0 {
			lockReleasedDuringHold += lockReleased
			if rollbackOnError && domain.writeGeneration != rollbackGeneration {
				rollbackOnError = false
			}
		}
		plan.stats.BufferStageLockWait += updateBatchStatsDuration(detailedStats, relockWait)
		plan.stats.BufferStageFlush += updateBatchStatsDuration(detailedStats, flushDuration)
		if err != nil {
			// The command frame is appended and staged writes are buffered, so a
			// flush failure is commit-ambiguous and the deferred poison forces recovery.
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	resetCollectionTables(compactedObsolete)
	plan.stats.BufferedBatches = 1
	// If no threshold flush published the range, leave it pending for the next
	// explicit/background flush. The helper is idempotent if already recorded.
	if err := recordPendingCommandWAL(); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Collection) bufferUpdateBatchPlanLocked(plan *updateBatchPlan) (bool, error) {
	if c == nil || plan == nil {
		return false, nil
	}
	if c.commandWALActive(nil) && plan.bufferedCommandWALIntent == nil {
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
	if err := c.requireColumnStoreCommandWAL(plan.meta, nil); err != nil {
		plan.stats.BufferStagePrecheck += updateBatchStatsSince(detailedStats, precheckStart)
		return false, err
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
	primaryRootName := collectionPrimaryRootName(plan.meta.Name)
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
		if hasPendingRootRunsForRootLocked(domain, rootName) {
			if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
				plan.stats.BufferStageRootScan += updateBatchStatsSince(detailedStats, phaseStart)
				return false, errBufferedRootBaseMismatch(plan.meta.Name, rootName)
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
	freezeStart := updateBatchStatsNow(detailedStats)
	freezeMutableIndexedRunMapsLocked(domain)
	plan.stats.BufferStageFreeze += updateBatchStatsSince(detailedStats, freezeStart)
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
	rollbackOnError := shouldAutoFlushAfterAdding
	if shouldAutoFlushAfterAdding {
		checkpoint = checkpointBufferedIndexedDomain(domain)
	}
	rollbackGeneration := checkpoint.writeGeneration
	plan.stats.BufferStageDomainPrepare += updateBatchStatsSince(detailedStats, phaseStart)
	ensureBufferedPrimaryRunIndexLocked(domain, modifiedCount)
	materializePrimaryOverlayLocked(domain)
	clearPrimaryDocumentCacheLocked(domain)
	var primaryWriteTable memtable.Table
	for i, rootName := range plan.rootNames {
		baseRoot := plan.baseRootIDs[rootName]
		table := plan.deltaTables[i]
		if table == nil || table.Len() == 0 {
			continue
		}
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			return false, errBufferedRootBaseMismatch(plan.meta.Name, rootName)
		}
		if rootName == primaryRootName {
			primaryWriteTable = table
		}
		if _, ok := domain.rootBaseIDs[rootName]; !ok {
			domain.rootBaseIDs[rootName] = baseRoot
		}
		if rootName == primaryRootName && domain.primaryRunIndex != nil {
			phaseStart = updateBatchStatsNow(detailedStats)
			if err := addBufferedPrimaryRunIndexEntries(domain.primaryRunIndex, table); err != nil {
				plan.stats.BufferStagePrimaryIdx += updateBatchStatsSince(detailedStats, phaseStart)
				domain.primaryRunIndex = nil
				if rollbackOnError {
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
				if rollbackOnError {
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
		appendDuration := updateBatchStatsSince(detailedStats, phaseStart)
		if appendDuration > 0 {
			if rootName == primaryRootName {
				plan.stats.BufferStagePrimaryAppend += appendDuration
			} else {
				plan.stats.BufferStageSecondaryAppend += appendDuration
			}
		}
		plan.stats.BufferStageRootAppend += appendDuration
	}
	plan.deltaTables = nil
	domain.loaded = true
	domain.meta = plan.meta
	domain.catalog = plan.catalog
	domain.baseCommitSeq = plan.baseCommitSeq
	domain.baseSystemRoot = plan.baseSystemRoot
	if plan.catalog != nil {
		domain.primaryRoot = plan.catalog.rootID(primaryRootName)
	}
	domain.count += modifiedCount
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, modifiedCount)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, stagedBytes)
	domain.indexedDeletesOnly = false
	domain.writeGeneration++
	if err := domain.notePrimaryWriteTableLocked(primaryWriteTable, domain.writeGeneration); err != nil {
		if rollbackOnError {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return false, err
	}
	if rollbackOnError {
		rollbackGeneration = domain.writeGeneration
	}
	domain.observeIndexedStage(modifiedCount, stagedBytes, stagedRootRuns)
	c.meta = plan.meta
	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, plan.meta.Options)
	if err != nil {
		if rollbackOnError {
			rollbackBufferedIndexedDomain(domain, checkpoint)
			c.meta = collectionMetaCheckpoint
		}
		return false, err
	}
	if shouldFlushBufferedIndexedWrites(domain, plan.meta.Options) {
		flushDuration, lockReleased, relockWait, err := c.flushBufferedIndexedAfterThresholdLocked(domain, plan.meta.Options)
		if lockReleased > 0 {
			lockReleasedDuringHold += lockReleased
			if rollbackOnError && domain.writeGeneration != rollbackGeneration {
				rollbackOnError = false
			}
		}
		plan.stats.BufferStageLockWait += updateBatchStatsDuration(detailedStats, relockWait)
		plan.stats.BufferStageFlush += updateBatchStatsDuration(detailedStats, flushDuration)
		if err != nil {
			if rollbackOnError {
				rollbackBufferedIndexedDomain(domain, checkpoint)
				c.meta = collectionMetaCheckpoint
			}
			return false, err
		}
	}
	resetCollectionTables(compactedObsolete)
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

type updateBatchSecondaryIndexChangeSummary struct {
	any    bool
	unique bool
}

func summarizeUpdateBatchSecondaryIndexChanges(runtimes []indexRuntime, updates []preparedBatchUpdate) updateBatchSecondaryIndexChangeSummary {
	var summary updateBatchSecondaryIndexChangeSummary
	for runtimeIdx := range runtimes {
		for _, update := range updates {
			if preparedBatchUpdateIndexChanged(update, runtimeIdx) {
				summary.any = true
				if runtimes[runtimeIdx].def.unique {
					summary.unique = true
				}
				if summary.any && summary.unique {
					return summary
				}
			}
		}
	}
	return summary
}

func updateBatchChangesSecondaryUniqueIndex(runtimes []indexRuntime, updates []preparedBatchUpdate) bool {
	for runtimeIdx := range runtimes {
		if !runtimes[runtimeIdx].def.unique {
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

func updateBatchChangesSecondaryIndex(runtimes []indexRuntime, updates []preparedBatchUpdate) bool {
	return summarizeUpdateBatchSecondaryIndexChanges(runtimes, updates).any
}

func documentIndexRuntimeChanged(oldState, newState documentIndexState, runtime indexRuntime) bool {
	return !normalizedEncodedIndexValuesEqual(oldState[runtime.def.name], newState[runtime.def.name])
}

func orderedDocumentIndexRuntimeChanged(oldState, newState orderedDocumentIndexState, runtimeIdx int) bool {
	return !normalizedEncodedIndexValuesEqual(oldState.valuesAt(runtimeIdx), newState.valuesAt(runtimeIdx))
}

func recordKnownUnaffectedIndexStats(runtimes []indexRuntime, stats *CollectionUpdateStats) {
	if stats == nil {
		return
	}
	for runtimeIdx, runtime := range runtimes {
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
	if cached := c.catalog; cached != nil &&
		systemRoot != 0 &&
		c.catalogSystemRoot == systemRoot &&
		(c.catalogCommitSeq == commitSeq || c.canReuseCachedCatalogAcrossDataOnlyCommits(cached)) {
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

	catalog, err := loadCollectionCatalog(snap, c.collectionName())
	if err != nil {
		return nil, err
	}
	c.rememberCatalog(snap, catalog)
	return catalog, nil
}

func (c *Collection) collectionName() string {
	if c == nil {
		return ""
	}
	if c.name != "" {
		return c.name
	}
	return c.meta.Name
}

func (c *Collection) canReuseCachedCatalogAcrossDataOnlyCommits(catalog *collectionCatalog) bool {
	return c != nil &&
		c.db != nil &&
		c.db.CommandWALEnabled() &&
		catalog != nil &&
		len(catalog.meta.VectorIndexes) == 0
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
	if state, ok := snap.StateToken(); ok {
		return state.SystemRootPageID
	}
	return 0
}

func snapshotUserRoot(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state, ok := snap.StateToken(); ok {
		return state.RootPageID
	}
	return 0
}

func snapshotCommitSeq(snap *backenddb.Snapshot) uint64 {
	if snap == nil {
		return 0
	}
	if state, ok := snap.StateToken(); ok {
		return state.CommitSeq
	}
	return 0
}

func dbCommitSeqAndSystemRoot(db *backenddb.DB) (uint64, uint64) {
	if db == nil {
		return 0, 0
	}
	if state, ok := db.StateToken(); ok {
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
	options, err := collectionPlannerOptionsForDB(c.db, catalog.meta)
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
	domain.rootMutableRuns = nil
	domain.rootPolicies = nil
	domain.rootBaseIDs = nil
	domain.rootValueArenas = nil
	domain.primaryOverlay = nil
	retainPrimaryDocumentCacheForCatalogLocked(domain, catalog.meta, systemRoot)
	domain.rootRunCount = 0
	domain.indexedDeletesOnly = false
	domain.primaryIDIndex = nil
	domain.primaryRunIndex = nil
	domain.primaryWriteIndex = nil
	domain.uniqueValueRuns = nil
	domain.uniqueValueMutableRuns = nil
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
		if !indexDefinitionValuesEqual(left.Indexes[i], right.Indexes[i]) {
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
		meta:                   meta,
		roots:                  roots,
		rootOverlays:           rootOverlays,
		rootOverlayFilters:     rootOverlayFilters,
		primaryRootName:        collectionPrimaryRootName(meta.Name),
		templateRootName:       collectionTemplateRootName(meta.Name),
		indexStateRootName:     collectionIndexStateRootName(meta.Name),
		columnManifestRootName: collectionColumnManifestRootName(meta.Name),
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

func (c *Collection) bufferedIndexedDeleteTombstoneLocked(domain *collectionWriteDomain, documentID []byte) bool {
	if c == nil || domain == nil || domain.count == 0 || !domain.indexedDeletesOnly {
		return false
	}
	_, buffered, found := c.getBufferedDocumentIntoLocked(domain, documentID, nil)
	return buffered && !found
}

func (c *Collection) bufferIndexedDeleteTablesLocked(
	catalog *collectionCatalog,
	baseCommitSeq uint64,
	baseSystemRoot uint64,
	rootNames []string,
	baseRootIDs map[string]uint64,
	policies []backenddb.OrderedRootStoragePolicy,
	deltaTables []memtable.Table,
	deleted int,
) (bool, error) {
	if c == nil || c.writeDomain == nil || catalog == nil || deleted == 0 || len(deltaTables) == 0 {
		return false, nil
	}
	meta := catalog.meta
	if !c.shouldBufferIndexedDeletes(meta) {
		return false, nil
	}
	if err := c.requireColumnStoreCommandWAL(meta, nil); err != nil {
		return false, err
	}
	if len(rootNames) != len(deltaTables) || len(rootNames) != len(policies) {
		return false, fmt.Errorf("collections: Delete collection %q invalid plan lengths roots=%d deltas=%d policies=%d", meta.Name, len(rootNames), len(deltaTables), len(policies))
	}
	domain := c.writeDomain
	domain.mu.Lock()
	defer domain.mu.Unlock()
	if domain.count != 0 && !domain.indexedDeletesOnly {
		return false, nil
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(meta, baseCommitSeq, baseSystemRoot, rootNames, baseRootIDs); err != nil {
		return false, err
	}
	checkpoint := checkpointBufferedIndexedDomain(domain)
	collectionMetaCheckpoint := c.meta
	rollback := func() {
		rollbackBufferedIndexedDomain(domain, checkpoint)
		c.meta = collectionMetaCheckpoint
	}
	if domain.count == 0 {
		c.initializeWriteDomainFromCatalogLocked(domain, catalog, baseCommitSeq, baseSystemRoot)
	}
	clearPrimaryDocumentCacheLocked(domain)
	if domain.rootPolicies == nil {
		domain.rootPolicies = make(map[string]backenddb.OrderedRootStoragePolicy, len(rootNames))
	}
	if domain.rootBaseIDs == nil {
		domain.rootBaseIDs = make(map[string]uint64, len(rootNames))
	}
	if domain.rootRuns == nil {
		domain.rootRuns = make(map[string][]memtable.Table, len(rootNames))
	}
	freezeMutableIndexedRunMapsLocked(domain)
	domain.primaryRunIndex = nil
	var stagedBytes int64
	stagedRootRuns := 0
	primaryRootName := collectionPrimaryRootName(meta.Name)
	var primaryWriteTable memtable.Table
	for i, rootName := range rootNames {
		table := deltaTables[i]
		if table == nil || table.Len() == 0 {
			continue
		}
		baseRoot := baseRootIDs[rootName]
		if pendingBaseRoot, ok := pendingIndexedRootBaseIDLocked(domain, rootName); ok && pendingBaseRoot != baseRoot {
			rollback()
			return false, errBufferedRootBaseMismatch(meta.Name, rootName)
		}
	}
	for i, rootName := range rootNames {
		table := deltaTables[i]
		if table == nil || table.Len() == 0 {
			continue
		}
		baseRoot := baseRootIDs[rootName]
		if _, ok := domain.rootBaseIDs[rootName]; !ok {
			domain.rootBaseIDs[rootName] = baseRoot
		}
		if rootName == primaryRootName {
			primaryWriteTable = table
		}
		domain.rootPolicies[rootName] = policies[i]
		domain.rootRuns[rootName] = append(domain.rootRuns[rootName], table)
		domain.rootRunCount = saturatingAddNonNegativeInt(domain.rootRunCount, 1)
		stagedRootRuns++
		stagedBytes = saturatingAddNonNegativeInt64(stagedBytes, table.Size())
		deltaTables[i] = nil
	}
	if stagedRootRuns == 0 {
		rollback()
		return false, nil
	}
	domain.loaded = true
	domain.meta = meta
	domain.catalog = catalog
	domain.baseCommitSeq = baseCommitSeq
	domain.baseSystemRoot = baseSystemRoot
	domain.primaryRoot = catalog.rootID(collectionPrimaryRootName(meta.Name))
	domain.count += deleted
	domain.bufferedBytes = saturatingAddNonNegativeInt64(domain.bufferedBytes, stagedBytes)
	domain.mutableCount = saturatingAddNonNegativeInt(domain.mutableCount, deleted)
	domain.mutableBytes = saturatingAddNonNegativeInt64(domain.mutableBytes, stagedBytes)
	domain.indexedDeletesOnly = true
	domain.writeGeneration++
	if err := domain.notePrimaryWriteTableLocked(primaryWriteTable, domain.writeGeneration); err != nil {
		rollback()
		return false, err
	}
	domain.observeIndexedStage(deleted, stagedBytes, stagedRootRuns)
	c.meta = meta
	compactedObsolete, err := maybeCompactBufferedIndexedMutableRunsLocked(domain, meta.Options)
	if err != nil {
		rollback()
		return false, err
	}
	if shouldFlushBufferedIndexedWrites(domain, meta.Options) {
		flushElapsed, _, _, err := c.flushBufferedIndexedAfterThresholdLocked(domain, meta.Options)
		_ = flushElapsed
		if err != nil {
			rollback()
			return false, err
		}
	}
	resetCollectionTables(compactedObsolete)
	return true, nil
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
			if _, err := setCollectionSecondaryIndexEntryForValueType(secondaryTable, newRuntime.def.valueType, encoded, documentID); err != nil {
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
	if err := c.addDocumentMutationGenerationUpdate(updates, c.meta, rootNames); err != nil {
		return nil, err
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
	if err := c.addDocumentMutationGenerationUpdate(updates, meta, rootNames); err != nil {
		return nil, err
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
	if err := c.addDocumentMutationGenerationUpdate(updates, meta, rootNames); err != nil {
		return nil, err
	}
	return buildSystemDeltaIterator(updates)
}

func (c *Collection) addDocumentMutationGenerationUpdate(updates map[string][]byte, meta CollectionMeta, rootNames []string) error {
	primaryRootName := collectionPrimaryRootName(meta.Name)
	for _, rootName := range rootNames {
		if rootName != primaryRootName {
			continue
		}
		state, ok := c.db.StateToken()
		if !ok {
			return backenddb.ErrClosed
		}
		if state.CommitSeq == ^uint64(0) {
			return errors.New("collections: document generation exhausted")
		}
		updates[systemCollectionDocumentGenerationKey(meta.Name)] = encodeRootID(state.CommitSeq + 1)
		break
	}
	return nil
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
	if _, ok := snap.StateToken(); !ok {
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
	state, ok := c.db.StateToken()
	if !ok {
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
		return nil, fmt.Errorf("%w detected for %q", errConcurrentSchemaModification, baseMeta.Name)
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
		return nil, fmt.Errorf("%w detected for %q", errConcurrentSchemaModification, baseMeta.Name)
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
		if _, err := deleteCollectionSecondaryIndexEntryForValueType(table, runtime.def.valueType, encoded, documentID); err != nil {
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
				ownerID, err := indexKeyDocumentID(runtime.def.valueType, key)
				if err != nil {
					_ = it.Close()
					return err
				}
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
				ownerID, err := indexKeyDocumentID(runtime.def.valueType, key)
				if err != nil {
					_ = it.Close()
					return err
				}
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
		var bufferedTemplateRuns []memtable.Table
		defer func() {
			if closeOnErr {
				resetCollectionTables(bufferedTemplateRuns)
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
		plannerOptions, err := collectionPlannerOptionsForDB(c.db, c.meta)
		if err != nil {
			return nil, err
		}
		plannerOptions = collectionOptionsWithTemplateV1Resolver(plannerOptions, snap, catalog)
		bufferedTemplateRuns, err = cloneBufferedTemplateV1Runs(c.writeDomain, c.meta.Name)
		if err != nil {
			return nil, err
		}
		if templateV1PlanningSnapshotNeedsRefresh(c.writeDomain, snap, bufferedTemplateRuns) {
			_, _, plannerOptions, err = c.refreshTemplateV1PlanningSnapshot(&snap, false, bufferedTemplateRuns, false)
			if err != nil {
				return nil, err
			}
		}
		closeOnErr = false
		return &StoredDocumentJSONMaterializer{
			documentFormat:   documentFormat,
			templateResolver: plannerOptions.templateResolver,
			closeFn: func() error {
				resetCollectionTables(bufferedTemplateRuns)
				return snap.Close()
			},
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
	value, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(c.meta.Name), documentID, dst)
	if err != nil || !found || !columnStoreCanReconstructDocument(catalog.meta) {
		return value, found, err
	}
	reconstructed, err := c.reconstructColumnDocumentAtSnapshot(snap, catalog, documentID, value)
	if err != nil {
		return dst[:0], false, err
	}
	return append(dst[:0], reconstructed...), true, nil
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
	return hasPendingRootRunsForRootLocked(domain, collectionPrimaryRootName(collectionName))
}

func hasBufferedPrimaryWritesLocked(domain *collectionWriteDomain, fallbackCollectionName string) bool {
	if hasBufferedNoIndexTableWritesLocked(domain) {
		return true
	}
	if hasBufferedPrimaryOverlay(domain) {
		return true
	}
	if hasPendingIndexedPrimaryOverlay(domain) {
		return true
	}
	return hasBufferedPrimaryRootRuns(domain, fallbackCollectionName)
}

func (c *Collection) getBufferedDocumentIntoLocked(domain *collectionWriteDomain, documentID []byte, dst []byte) ([]byte, bool, bool) {
	table := domain.table
	if ref, ok := domain.primaryOverlay.lookupRef(documentID); ok {
		if ref.flags&node.FlagTombstone != 0 {
			return dst[:0], true, false
		}
		return append(dst[:0], ref.value...), true, true
	}
	if ref, ok := lookupPendingPrimaryOverlayLocked(domain, documentID); ok {
		if value, flags, found := c.getCurrentPrimaryRootRunEntryLocked(domain, documentID); found {
			if flags&node.FlagTombstone != 0 {
				return dst[:0], true, false
			}
			return append(dst[:0], value...), true, true
		}
		if ref.flags&node.FlagTombstone != 0 {
			return dst[:0], true, false
		}
		return append(dst[:0], ref.value...), true, true
	}
	if hasBufferedIndexedRootRuns(domain) {
		name := collectionPrimaryRootName(c.meta.Name)
		if domain.meta.Name != "" {
			name = collectionPrimaryRootName(domain.meta.Name)
		}
		var value []byte
		var flags byte
		found := false
		if domain.primaryRunIndex == nil {
			value, _, flags, found = getBufferedRunEntry(pendingIndexedRootRunsLocked(domain, name), documentID)
		} else if ref, ok := domain.primaryRunIndex.lookupRef(documentID); ok {
			value, flags, found = ref.value, ref.flags, ref.entryValid
			if !found && ref.table != nil {
				value, _, flags, found = ref.table.GetEntry(documentID)
			}
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

func (c *Collection) getCurrentPrimaryRootRunEntryLocked(domain *collectionWriteDomain, documentID []byte) ([]byte, byte, bool) {
	if c == nil || domain == nil {
		return nil, 0, false
	}
	name := collectionPrimaryRootName(c.meta.Name)
	if domain.meta.Name != "" {
		name = collectionPrimaryRootName(domain.meta.Name)
	}
	value, _, flags, found := getBufferedRunEntry(domain.rootRuns[name], documentID)
	return value, flags, found
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

// CompoundIndexRangeOptions describes a direct BSON v2 index scan. Prefix
// supplies equality values for the leading components; Lower and Upper bound
// at most the immediately following component. Bounds are expressed in the
// component's logical BSON order, while results follow the index definition
// unless Desc is set. This is an explicit collection primitive: it does not
// participate in query planning.
type CompoundIndexRangeOptions struct {
	Prefix []bson.RawValue
	Lower  IndexRangeBound
	Upper  IndexRangeBound
	Limit  int
	Desc   bool
	// StableDocumentIDTies makes a BSON v2 scan emit equal logical index keys in
	// ascending document-ID order (or DocumentIDLess order when supplied). The
	// default retains the historical physical order for direct callers. A result limit may
	// return the ascending-ID prefix of a fully buffered group; a work-cap stop
	// never publishes an incomplete group and reports truncation instead.
	StableDocumentIDTies bool
	// DocumentIDLess optionally supplies the ordering for IDs within a stable
	// logical-index-key tie group. Nil uses the historical encoded-byte order.
	// Callers whose document IDs have a higher-level collation may provide their
	// own deterministic comparator without materializing the whole result set.
	DocumentIDLess func(left, right []byte) bool
	// MaxInspected optionally overrides the normal Limit*64 physical-entry
	// budget. It is for planners that need enough work to complete a stable tie
	// group even when their result page is smaller than that group.
	MaxInspected int
	// MaxRetainedIDBytes bounds IDs accumulated for the returned slice. Zero
	// retains the historical direct-API behavior; a positive value returns a
	// truncated prefix before retaining more owned ID payload.
	MaxRetainedIDBytes int
	// Inspected, when non-nil, receives the physical source work consumed by
	// this scan, including source positioning and bounded source advances. It
	// is reset before scanning. Planners can carry a single inspection budget
	// across several canonical probes without treating each probe as entitled
	// to a fresh work cap.
	Inspected *int
}

const defaultIndexRangeResultCap = 16

// compoundIndexRangeInspectedMultiplier bounds work spent resolving deleted
// and shadowed entries in buffered/persisted overlay runs. A direct scan
// reports truncation when this cap is reached even when it has emitted fewer
// than Limit IDs, so callers never mistake an unbounded overlay walk for a
// bounded range operation.
const compoundIndexRangeInspectedMultiplier = 64

func compoundIndexRangeInspectedCap(limit int) int {
	if limit > int(^uint(0)>>1)/compoundIndexRangeInspectedMultiplier {
		return int(^uint(0) >> 1)
	}
	return limit * compoundIndexRangeInspectedMultiplier
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
	capHint := defaultIndexRangeResultCap
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

// FindByCompoundIndexRange returns document IDs in compound-index order. It
// supports BSON v2 indexes with an equality prefix and one optional range
// suffix; field extraction and planner selection remain outside this API.
// Limit must be positive. The scan examines at most Limit*64 physical index
// entries (including overlay tombstones and shadowed duplicates); reaching
// either the result or inspected-entry bound returns truncated=true.
func (c *Collection) FindByCompoundIndexRange(indexName string, opts CompoundIndexRangeOptions) ([][]byte, bool, error) {
	if c == nil {
		return nil, false, errCollectionNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return nil, false, err
	}
	if opts.Limit <= 0 {
		return nil, false, errors.New("collections: compound index range limit must be positive")
	}
	if opts.MaxInspected < 0 {
		return nil, false, errors.New("collections: compound index max inspected entries cannot be negative")
	}
	if opts.MaxRetainedIDBytes < 0 {
		return nil, false, errors.New("collections: compound index retained-ID byte cap cannot be negative")
	}
	if opts.Inspected != nil {
		*opts.Inspected = 0
	}
	if err := c.flushBufferedNoIndex(); err != nil {
		return nil, false, err
	}
	domain := c.writeDomain
	if domain != nil {
		domain.mu.RLock()
		defer domain.mu.RUnlock()
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshotWithWriteDomainLocked(snap)
	if err != nil {
		return nil, false, err
	}
	if catalog == nil {
		return nil, false, errCollectionNotFound
	}
	idx, ok := findIndex(catalog.meta.Indexes, indexName)
	if !ok {
		return nil, false, nil
	}
	if opts.StableDocumentIDTies {
		components, err := normalizeIndexComponents(idx)
		if err != nil {
			return nil, false, err
		}
		// BSON-v2 stores missing and null in distinct physical runs. A bounded
		// stable tie buffer can normalize them only when the equality prefix
		// fixes every component except one; otherwise later component values can
		// separate equal logical keys non-adjacently.
		if len(components)-len(opts.Prefix) > 1 {
			return nil, false, errors.New("collections: stable document-ID ties require at most one unfixed compound component")
		}
	}
	start, end, empty, err := compoundIndexRangeScanBounds(idx, opts)
	if err != nil {
		return nil, false, err
	}
	if empty {
		return nil, false, nil
	}
	workCap := compoundIndexRangeInspectedCap(opts.Limit)
	if opts.MaxInspected > 0 {
		workCap = opts.MaxInspected
	}
	// Charge physical overlay sources before their dedupe/merge. When both
	// live-buffer and persisted sources exist, split the cap conservatively so
	// their combined physical work cannot exceed the documented bound.
	bufferedBudget := workCap / 2
	if workCap > 0 && bufferedBudget == 0 {
		// Zero is the legacy unlimited iterator sentinel. A caller-requested
		// positive cap of one cannot safely divide between buffered and
		// persisted overlays, so reject before opening either source.
		return nil, true, nil
	}
	sourceInspected := 0
	recordSourceInspection := func(count int) { sourceInspected += count }
	if opts.Inspected != nil {
		// Preserve source work on every fail-closed path too. In particular, a
		// capped buffered or persisted overlay must still debit the planner's
		// shared budget before it rejects the probe.
		defer func() { *opts.Inspected = sourceInspected }()
	}
	bufferedTable, err := bufferedIndexRangeTableLocked(domain, catalog.meta.Name, indexName, start, end, bufferedBudget, recordSourceInspection)
	if err != nil {
		if errors.Is(err, errCollectionIndexScanWorkCap) {
			// Do not materialize an unbounded live-buffer interval merely to
			// discover tombstones. Returning no partial ordering is fail-closed.
			return nil, true, nil
		}
		return nil, false, err
	}
	if bufferedTable != nil {
		defer resetCollectionRunTable(bufferedTable)
	} else {
		bufferedBudget = 0
	}
	var bufferedIt iterator.UnsafeIterator
	if bufferedTable != nil {
		if opts.Desc {
			bufferedIt = bufferedTable.NewReverseIterator(start, end)
		} else {
			bufferedIt = bufferedTable.NewIterator(start, end)
		}
		defer func() { _ = bufferedIt.Close() }()
	}
	var persistedIt iterator.UnsafeIterator
	persistedBudget := workCap - bufferedBudget
	if opts.Desc {
		persistedIt, err = collectionReverseIteratorAtCatalogRootWithWorkCapAndInspect(snap, catalog, collectionSecondaryRootName(catalog.meta.Name, idx.Name), start, end, true, persistedBudget, recordSourceInspection)
	} else {
		persistedIt, err = collectionIteratorAtCatalogRootWithWorkCapAndInspect(snap, catalog, collectionSecondaryRootName(catalog.meta.Name, idx.Name), start, end, true, persistedBudget, recordSourceInspection)
	}
	if err != nil {
		if errors.Is(err, errCollectionIndexScanWorkCap) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if persistedIt != nil {
		defer func() { _ = persistedIt.Close() }()
	}
	ids := make([][]byte, 0)
	retainedIDBytes := 0
	truncated, err := scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap(bufferedIt, persistedIt, idx.ValueType, opts.Limit, opts.Desc, workCap, scanMergedCollectionIndexIDOptions{
		CloneDocumentID:      true,
		DedupeDocumentID:     shouldDedupeIndexDocumentIDs(idx, catalog.meta.Options),
		StableDocumentIDTies: opts.StableDocumentIDTies,
		DocumentIDLess:       opts.DocumentIDLess,
		LogicalIndexKey:      BSONIndexKeyStableSortPrefixV2,
		// A caller-supplied retained-ID cap must bound the stable scanner's
		// transient group and dedupe allocations as well as the result slice.
		// Otherwise a cursor plan could allocate the default 64 MiB tie buffer
		// before its own smaller admission cap observes the IDs.
		MaxStableTieBytes: opts.MaxRetainedIDBytes,
	}, func(id []byte) (bool, error) {
		charge := len(id) + stableDocumentIDTieEntryOverhead
		if charge < len(id) || (opts.MaxRetainedIDBytes > 0 && charge > opts.MaxRetainedIDBytes-retainedIDBytes) {
			return false, errCollectionIndexScanRetainedBytesCap
		}
		ids = append(ids, id)
		retainedIDBytes += charge
		return true, nil
	})
	if err != nil {
		if errors.Is(err, errCollectionIndexScanWorkCap) || errors.Is(err, errCollectionIndexScanRetainedBytesCap) {
			return ids, true, nil
		}
		return nil, false, err
	}
	if len(ids) == 0 {
		// A bounded stable group can hit its work or retained-byte cap before
		// publishing its first ID. Preserve that signal: callers such as cursor
		// admission must reject rather than mistake an incomplete scan for an
		// empty result set.
		return nil, truncated, nil
	}
	return ids, truncated, nil
}

// FindDocumentsByIndexRange returns primary documents whose named secondary
// index falls inside opts, preserving index order. Persisted index and primary
// reads share one snapshot/catalog; same-manager buffered documents are still
// consulted before the persisted primary root so pending indexed writes remain
// visible. Descending scans are not supported. Because this API holds a
// write-domain read lock while pairing secondary IDs with buffered primary
// documents, callers must provide a positive Limit.
func (c *Collection) FindDocumentsByIndexRange(indexName string, opts IndexRangeOptions) ([]DocumentRecord, bool, error) {
	capHint := defaultIndexRangeResultCap
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	var out []DocumentRecord
	truncated, indexFound, err := c.scanDocumentsByIndexRange(indexName, opts, func(record BorrowedDocumentRecord) (bool, error) {
		if out == nil {
			out = make([]DocumentRecord, 0, capHint)
		}
		out = append(out, DocumentRecord{
			ID:       bytes.Clone(record.ID),
			Document: bytes.Clone(record.Document),
		})
		return true, nil
	})
	if err != nil {
		return nil, false, err
	}
	if !indexFound {
		return nil, false, nil
	}
	if out == nil {
		out = make([]DocumentRecord, 0)
	}
	return out, truncated, nil
}

// ScanBorrowedDocumentsByIndexRange calls fn with primary documents whose named
// secondary index falls inside opts, preserving index order. This is a borrowed
// performance API for gateway integrations: record slices are valid only during
// the callback, and fn may run while the collection write-domain read lock is
// held. The callback must not retain or modify slices, call back into Collection,
// or perform blocking work. Missing indexes are treated as empty scans.
// Descending scans are not supported, and opts.Limit must be positive. General
// callers should use FindDocumentsByIndexRange.
func (c *Collection) ScanBorrowedDocumentsByIndexRange(indexName string, opts IndexRangeOptions, fn func(BorrowedDocumentRecord) (bool, error)) (bool, error) {
	if fn == nil {
		return false, errors.New("collections: nil borrowed index document range callback")
	}
	truncated, _, err := c.scanDocumentsByIndexRange(indexName, opts, fn)
	return truncated, err
}

func (c *Collection) scanDocumentsByIndexRange(indexName string, opts IndexRangeOptions, fn func(BorrowedDocumentRecord) (bool, error)) (truncated bool, indexFound bool, err error) {
	if c == nil {
		return false, false, errCollectionNil
	}
	if c.db == nil {
		return false, false, errCollectionDBNil
	}
	if err := ValidateIndexName(indexName); err != nil {
		return false, false, err
	}
	if opts.Limit < 0 {
		return false, false, errors.New("collections: index range limit cannot be negative")
	}
	if opts.Limit == 0 {
		return false, false, errors.New("collections: document index range reads require a positive limit")
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
		// Keep the read lock through the scan so buffered secondary IDs and
		// buffered primary documents come from one write-domain view. Releasing
		// here would require copying the pending primary view up front, which is
		// too much work for the common small-limit range probe.
	}
	var snap *backenddb.Snapshot
	var bufferedTable memtable.Table
	var bufferedIt iterator.UnsafeIterator
	var persistedIt iterator.UnsafeIterator
	defer func() {
		if domainLocked {
			domain.mu.RUnlock()
		}
		if persistedIt != nil {
			_ = persistedIt.Close()
		}
		if bufferedIt != nil {
			_ = bufferedIt.Close()
		}
		if bufferedTable != nil {
			resetCollectionRunTable(bufferedTable)
		}
		if snap != nil {
			_ = snap.Close()
		}
	}()
	snap = c.db.AcquireSnapshot()
	if snap == nil {
		return false, false, backenddb.ErrClosed
	}
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
	if orderedBSONIndexRequiresCompoundRangeAPI(idx) {
		return false, false, errors.New("collections: compound or descending BSON v2 indexes require FindByCompoundIndexRange")
	}
	start, end, empty, err := indexRangeScanBounds(idx.ValueType, opts)
	if err != nil {
		return false, false, err
	}
	if empty {
		return false, true, nil
	}
	exactPrefix, exactPrefixScan, err := exactIndexRangePrefix(idx.ValueType, opts)
	if err != nil {
		return false, false, err
	}
	if exactPrefixScan {
		// Exact document scans still need buffered tombstones for unique indexes.
		// The unique reservation fast path only proves pending live values, so use
		// the uncapped run-table path; tombstones can sort after the live entries
		// that satisfy opts.Limit and still need to hide persisted index rows.
		bufferedTable, err = bufferedIndexPrefixTableLocked(domain, catalog.meta.Name, indexName, false, exactPrefix, 0)
	} else {
		bufferedTable, err = bufferedIndexRangeTableLocked(domain, catalog.meta.Name, indexName, start, end, 0, nil)
	}
	if err != nil {
		return false, false, err
	}
	if bufferedTable != nil {
		bufferedIt = bufferedTable.NewIterator(start, end)
	}
	persistedIt, err = collectionIteratorAtCatalogRoot(snap, catalog, collectionSecondaryRootName(catalog.meta.Name, idx.Name), start, end, true)
	if err != nil {
		return false, false, err
	}
	primaryRootName := collectionPrimaryRootName(catalog.meta.Name)
	var primaryReader backenddb.SnapshotRootReader
	primaryReaderOK := false
	primaryRootMissing := false
	if len(catalog.overlayRootIDs(primaryRootName)) == 0 {
		primaryRootID := catalog.rootID(primaryRootName)
		if primaryRootID == 0 {
			primaryRootMissing = true
		} else {
			primaryReader, err = snap.ReaderAtRoot(primaryRootID)
			if err != nil {
				return false, true, err
			} else {
				primaryReaderOK = true
			}
		}
	}
	var scratch []byte
	dedupeDocumentIDs := shouldDedupeIndexDocumentIDs(idx, catalog.meta.Options)
	documentCount := 0
	documentTruncated := false
	truncated, err = scanMergedCollectionIndexIDsBorrowed(bufferedIt, persistedIt, idx.ValueType, 0, dedupeDocumentIDs, func(id []byte) (bool, error) {
		var value []byte
		var buffered, found bool
		if domainLocked {
			value, buffered, found = c.getBufferedDocumentIntoLocked(domain, id, scratch[:0])
		}
		if !buffered {
			if primaryReaderOK {
				var err error
				value, err = primaryReader.GetAppend(id, scratch[:0])
				if errors.Is(err, tree.ErrKeyNotFound) {
					found = false
				} else if err != nil {
					return false, err
				} else {
					found = true
				}
			} else if primaryRootMissing {
				found = false
			} else {
				var err error
				value, found, err = collectionGetAppendAtCatalogRoot(snap, catalog, primaryRootName, id, scratch[:0])
				if err != nil {
					return false, err
				}
			}
		}
		if !found {
			return true, nil
		}
		if opts.Limit > 0 && documentCount >= opts.Limit {
			documentTruncated = true
			return false, nil
		}
		scratch = value
		cont, err := fn(BorrowedDocumentRecord{
			ID:       id,
			Document: value,
		})
		if err != nil {
			return false, err
		}
		if !cont {
			return false, nil
		}
		documentCount++
		return true, nil
	})
	if err != nil {
		return false, false, err
	}
	if documentTruncated {
		return true, true, nil
	}
	return truncated, true, nil
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

// orderedBSONIndexRequiresCompoundRangeAPI reports whether the ordered BSON v2
// definition needs direction-aware component bounds. The legacy scalar range
// APIs intentionally do not provide those bounds; callers must use
// FindByCompoundIndexRange instead.
func orderedBSONIndexRequiresCompoundRangeAPI(idx IndexDefinition) bool {
	return idx.ValueType == IndexValueBSONOrderedV2 &&
		(len(idx.Components) > 1 || (len(idx.Components) == 1 && idx.Components[0].Direction == IndexDirectionDescending))
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
	if orderedBSONIndexRequiresCompoundRangeAPI(idx) {
		return false, false, errors.New("collections: compound or descending BSON v2 indexes require FindByCompoundIndexRange")
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
		// Exact scans still need buffered tombstones for unique indexes. The unique
		// reservation fast path only proves pending live values, so use the
		// uncapped run-table path; tombstones can sort after the live entries that
		// satisfy opts.Limit and still need to hide persisted index rows.
		bufferedTable, err = bufferedIndexPrefixTableLocked(domain, catalog.meta.Name, indexName, false, exactPrefix, 0)
	} else {
		bufferedTable, err = bufferedIndexRangeTableLocked(domain, catalog.meta.Name, indexName, start, end, 0, nil)
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
	dedupeDocumentIDs := shouldDedupeIndexDocumentIDs(idx, catalog.meta.Options)
	truncated, err := scanMergedCollectionIndexIDs(bufferedIt, persistedIt, idx.ValueType, opts.Limit, dedupeDocumentIDs, fn)
	return truncated, true, err
}

func shouldDedupeIndexDocumentIDs(idx IndexDefinition, opts CollectionOptions) bool {
	return idx.MultiKey || opts.AllowArrayValuesInIndex
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
func bufferedIndexRangeTableLocked(domain *collectionWriteDomain, collectionName, indexName string, start, end []byte, maxEntries int, onInspected func(int)) (memtable.Table, error) {
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
	it := newBufferedRootRunsIteratorWithDeletedDirectionWorkCapAndInspect(runs, start, end, true, false, maxEntries, onInspected)
	defer func() { _ = it.Close() }()
	inspected := 0
	for it.Valid() {
		if maxEntries > 0 && inspected >= maxEntries {
			resetCollectionRunTable(table)
			return nil, errCollectionIndexScanWorkCap
		}
		inspected++
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

func compoundIndexRangeScanBounds(idx IndexDefinition, opts CompoundIndexRangeOptions) ([]byte, []byte, bool, error) {
	if idx.ValueType != IndexValueBSONOrderedV2 {
		return nil, nil, false, errors.New("collections: compound index range requires BSON v2 key format")
	}
	components := idx.Components
	if len(components) == 0 {
		components = []IndexComponent{{Field: idx.Field, Direction: IndexDirectionAscending}}
	}
	if len(opts.Prefix) > len(components) {
		return nil, nil, false, errors.New("collections: compound index equality prefix exceeds component count")
	}
	prefix := make([]byte, 0)
	for i, value := range opts.Prefix {
		encoded, err := encodeBSONIndexComponentInDirection(value, components[i].Direction)
		if err != nil {
			return nil, nil, false, err
		}
		prefix = append(prefix, encoded...)
	}
	lowerBounded := !opts.Lower.Unbounded && opts.Lower.Value != nil
	upperBounded := !opts.Upper.Unbounded && opts.Upper.Value != nil
	if len(opts.Prefix) == len(components) && (lowerBounded || upperBounded) {
		return nil, nil, false, errors.New("collections: compound index range has no suffix component after a full equality prefix")
	}
	prefixEndValue := prefixEnd(prefix)
	if prefixEndValue == nil && len(prefix) != 0 {
		return nil, nil, true, nil
	}
	if len(opts.Prefix) == len(components) {
		return prefix, prefixEndValue, false, nil
	}
	var lowerEncoded, upperEncoded []byte
	var err error
	if lowerBounded {
		value, ok := opts.Lower.Value.(bson.RawValue)
		if !ok {
			return nil, nil, false, fmt.Errorf("collections: compound lower bound must be bson.RawValue, got %T", opts.Lower.Value)
		}
		lowerEncoded, err = encodeBSONIndexComponentInDirection(value, components[len(opts.Prefix)].Direction)
		if err != nil {
			return nil, nil, false, err
		}
	}
	if upperBounded {
		value, ok := opts.Upper.Value.(bson.RawValue)
		if !ok {
			return nil, nil, false, fmt.Errorf("collections: compound upper bound must be bson.RawValue, got %T", opts.Upper.Value)
		}
		upperEncoded, err = encodeBSONIndexComponentInDirection(value, components[len(opts.Prefix)].Direction)
		if err != nil {
			return nil, nil, false, err
		}
	}
	start, end := bytes.Clone(prefix), prefixEndValue
	// The encoded descending component reverses physical ordering, so exchange
	// logical lower/upper bounds before applying ordinary lexicographic bounds.
	if components[len(opts.Prefix)].Direction == IndexDirectionDescending {
		lowerEncoded, upperEncoded = upperEncoded, lowerEncoded
		lowerBounded, upperBounded = upperBounded, lowerBounded
		lowerInclusive, upperInclusive := opts.Upper.Inclusive, opts.Lower.Inclusive
		opts.Lower.Inclusive, opts.Upper.Inclusive = lowerInclusive, upperInclusive
	}
	if lowerBounded {
		candidate := append(append([]byte(nil), prefix...), lowerEncoded...)
		if opts.Lower.Inclusive {
			start = candidate
		} else if start = prefixEnd(candidate); start == nil {
			return nil, nil, true, nil
		}
	}
	if upperBounded {
		candidate := append(append([]byte(nil), prefix...), upperEncoded...)
		if opts.Upper.Inclusive {
			end = prefixEnd(candidate)
		} else {
			end = candidate
		}
	}
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil, true, nil
	}
	return start, end, false, nil
}

func encodeBSONIndexComponentInDirection(value bson.RawValue, direction IndexDirection) ([]byte, error) {
	encoded, err := encodeBSONIndexKeyComponentV2(value)
	if err != nil {
		return nil, err
	}
	if direction == IndexDirectionDescending {
		return descendingBSONIndexKeyComponentV2(encoded)
	}
	return encoded, nil
}

func encodedDoubleComponentIsNaN(encoded []byte) bool {
	return len(encoded) == 1 && encoded[0] == 0x00
}

func scanMergedCollectionIndexIDs(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, dedupeDocumentIDs bool, fn func([]byte) (bool, error)) (bool, error) {
	return scanMergedCollectionIndexIDsWithOptionsAndDirection(bufferedIt, persistedIt, valueType, maxResults, false, scanMergedCollectionIndexIDOptions{
		CloneDocumentID:  true,
		DedupeDocumentID: dedupeDocumentIDs,
	}, fn)
}

func scanMergedCollectionIndexIDsReverse(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, dedupeDocumentIDs bool, fn func([]byte) (bool, error)) (bool, error) {
	return scanMergedCollectionIndexIDsWithOptionsAndDirection(bufferedIt, persistedIt, valueType, maxResults, true, scanMergedCollectionIndexIDOptions{
		CloneDocumentID:  true,
		DedupeDocumentID: dedupeDocumentIDs,
	}, fn)
}

// scanMergedCollectionIndexIDsBorrowed calls fn with document IDs that may alias
// iterator key memory. fn must not retain or mutate id after returning; clone it
// first if the ID needs to outlive the callback.
func scanMergedCollectionIndexIDsBorrowed(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, dedupeDocumentIDs bool, fn func([]byte) (bool, error)) (bool, error) {
	return scanMergedCollectionIndexIDsWithOptions(bufferedIt, persistedIt, valueType, maxResults, scanMergedCollectionIndexIDOptions{
		CloneDocumentID:  false,
		DedupeDocumentID: dedupeDocumentIDs,
	}, fn)
}

type scanMergedCollectionIndexIDOptions struct {
	CloneDocumentID      bool
	DedupeDocumentID     bool
	StableDocumentIDTies bool
	DocumentIDLess       func(left, right []byte) bool
	// MaxStableTieBytes bounds all document-ID memory retained by stable mode:
	// both the current logical-key group's cloned IDs and dedupe keys retained
	// across completed groups. Zero selects the conservative production default.
	// It is an internal seam so the public range API retains a fixed, bounded
	// memory contract while tests can exercise the fail-closed path cheaply.
	MaxStableTieBytes int
	// LogicalIndexKey extracts the encoded logical index value from an entry.
	// It is required only for StableDocumentIDTies.
	LogicalIndexKey func([]byte) ([]byte, error)
}

const (
	// maxStableDocumentIDTieBytes is deliberately independent of the result
	// count/work cap: document IDs are variable-sized, so an entry-only cap
	// would still permit an arbitrarily large temporary tie buffer.
	maxStableDocumentIDTieBytes = 64 << 20
	// Account for cloned byte slices and dedupe-map string keys as well as their
	// payloads. The conservative fixed charge keeps retained stable-scan memory
	// beneath the advertised byte ceiling without relying on runtime internals.
	stableDocumentIDTieEntryOverhead = 32
)

func scanMergedCollectionIndexIDsWithOptions(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, opts scanMergedCollectionIndexIDOptions, fn func([]byte) (bool, error)) (bool, error) {
	return scanMergedCollectionIndexIDsWithOptionsAndDirection(bufferedIt, persistedIt, valueType, maxResults, false, opts, fn)
}

func scanMergedCollectionIndexIDsWithOptionsAndDirection(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, reverse bool, opts scanMergedCollectionIndexIDOptions, fn func([]byte) (bool, error)) (bool, error) {
	return scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap(bufferedIt, persistedIt, valueType, maxResults, reverse, 0, opts, fn)
}

// scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap is the bounded
// variant used by public direct scans. maxInspected counts physical index
// entries examined, including tombstones and duplicate/shadowed overlay keys;
// zero leaves the historical internal callers unbounded.
func scanMergedCollectionIndexIDsWithOptionsAndDirectionWorkCap(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, reverse bool, maxInspected int, opts scanMergedCollectionIndexIDOptions, fn func([]byte) (bool, error)) (bool, error) {
	if maxResults < 0 {
		return false, errors.New("collections: max index results cannot be negative")
	}
	if maxInspected < 0 {
		return false, errors.New("collections: max inspected index entries cannot be negative")
	}
	inspected := 0
	inspect := func(count int) bool {
		if maxInspected > 0 && count > maxInspected-inspected {
			return false
		}
		inspected += count
		return true
	}
	if opts.StableDocumentIDTies {
		if opts.LogicalIndexKey == nil {
			return false, errors.New("collections: stable document-ID ties require a logical index key parser")
		}
		// Stable mode buffers one complete logical tie group before publishing it.
		// Require an explicit cap so that buffer cannot become unbounded through a
		// future internal caller (public compound scans always supply one).
		if maxInspected == 0 {
			return false, errors.New("collections: stable document-ID ties require a positive inspected-entry cap")
		}
		if opts.MaxStableTieBytes < 0 {
			return false, errors.New("collections: stable document-ID ties cannot use a negative byte cap")
		}
		return scanMergedCollectionIndexIDsStable(bufferedIt, persistedIt, valueType, maxResults, reverse, inspect, opts, fn)
	}
	if bufferedIt == nil && !opts.DedupeDocumentID {
		emitted := 0
		for {
			persistedKey, persistedOK := collectionIndexIteratorKey(persistedIt)
			if !persistedOK {
				break
			}
			if !inspect(1) {
				return true, collectionIndexIteratorError(persistedIt)
			}
			if persistedIt.IsDeleted() {
				persistedIt.Next()
				continue
			}
			if maxResults > 0 && emitted >= maxResults {
				return true, collectionIndexIteratorError(persistedIt)
			}
			id, err := indexKeyDocumentID(valueType, persistedKey)
			if err != nil {
				return false, err
			}
			if opts.CloneDocumentID {
				id = bytes.Clone(id)
			}
			cont, err := fn(id)
			if err != nil || !cont {
				return false, err
			}
			emitted++
			persistedIt.Next()
		}
		return false, collectionIndexIteratorError(persistedIt)
	}
	var seen map[string]struct{}
	if opts.DedupeDocumentID {
		if maxResults > 0 {
			seen = make(map[string]struct{}, maxResults)
		} else {
			seen = make(map[string]struct{})
		}
	}
	emitted := 0
	emit := func(key []byte) (bool, bool, error) {
		id, err := indexKeyDocumentID(valueType, key)
		if err != nil {
			return false, false, err
		}
		if opts.DedupeDocumentID {
			idKey := string(id)
			if _, ok := seen[idKey]; ok {
				return true, false, nil
			}
			seen[idKey] = struct{}{}
		}
		if maxResults > 0 && emitted >= maxResults {
			return false, true, nil
		}
		if opts.CloneDocumentID {
			id = bytes.Clone(id)
		}
		cont, err := fn(id)
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
			if !inspect(1) {
				return true, collectionIndexIteratorError(persistedIt)
			}
			if !persistedIt.IsDeleted() {
				cont, truncated, err := emit(persistedKey)
				if err != nil || truncated || !cont {
					return truncated, err
				}
			}
			persistedIt.Next()
		case !persistedOK:
			if !inspect(1) {
				return true, collectionIndexIteratorError(bufferedIt)
			}
			if !bufferedIt.IsDeleted() {
				cont, truncated, err := emit(bufferedKey)
				if err != nil || truncated || !cont {
					return truncated, err
				}
			}
			bufferedIt.Next()
		default:
			cmp := bytes.Compare(bufferedKey, persistedKey)
			if (!reverse && cmp < 0) || (reverse && cmp > 0) {
				if !inspect(1) {
					return true, collectionIndexIteratorError(bufferedIt)
				}
				if !bufferedIt.IsDeleted() {
					cont, truncated, err := emit(bufferedKey)
					if err != nil || truncated || !cont {
						return truncated, err
					}
				}
				bufferedIt.Next()
			} else if (!reverse && cmp > 0) || (reverse && cmp < 0) {
				if !inspect(1) {
					return true, collectionIndexIteratorError(persistedIt)
				}
				if !persistedIt.IsDeleted() {
					cont, truncated, err := emit(persistedKey)
					if err != nil || truncated || !cont {
						return truncated, err
					}
				}
				persistedIt.Next()
			} else {
				if !inspect(2) {
					return true, nil
				}
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

// scanMergedCollectionIndexIDsStable preserves logical-index order in either
// direction while normalizing each equal-key run to ascending document ID
// order. It deliberately buffers only one run; callers never pay a
// full-result sort.
func scanMergedCollectionIndexIDsStable(bufferedIt, persistedIt iterator.UnsafeIterator, valueType IndexValueType, maxResults int, reverse bool, inspect func(int) bool, opts scanMergedCollectionIndexIDOptions, fn func([]byte) (bool, error)) (bool, error) {
	nextLiveKey := func() ([]byte, bool, bool, error) {
		for {
			bufferedKey, bufferedOK := collectionIndexIteratorKey(bufferedIt)
			persistedKey, persistedOK := collectionIndexIteratorKey(persistedIt)
			if !bufferedOK && !persistedOK {
				if err := collectionIndexIteratorError(bufferedIt); err != nil {
					return nil, false, false, err
				}
				if err := collectionIndexIteratorError(persistedIt); err != nil {
					return nil, false, false, err
				}
				return nil, false, false, nil
			}
			var key []byte
			var deleted bool
			switch {
			case !bufferedOK:
				if !inspect(1) {
					return nil, false, true, collectionIndexIteratorError(persistedIt)
				}
				key, deleted = bytes.Clone(persistedKey), persistedIt.IsDeleted()
				persistedIt.Next()
			case !persistedOK:
				if !inspect(1) {
					return nil, false, true, collectionIndexIteratorError(bufferedIt)
				}
				key, deleted = bytes.Clone(bufferedKey), bufferedIt.IsDeleted()
				bufferedIt.Next()
			default:
				cmp := bytes.Compare(bufferedKey, persistedKey)
				if (!reverse && cmp < 0) || (reverse && cmp > 0) {
					if !inspect(1) {
						return nil, false, true, collectionIndexIteratorError(bufferedIt)
					}
					key, deleted = bytes.Clone(bufferedKey), bufferedIt.IsDeleted()
					bufferedIt.Next()
				} else if (!reverse && cmp > 0) || (reverse && cmp < 0) {
					if !inspect(1) {
						return nil, false, true, collectionIndexIteratorError(persistedIt)
					}
					key, deleted = bytes.Clone(persistedKey), persistedIt.IsDeleted()
					persistedIt.Next()
				} else {
					if !inspect(2) {
						return nil, false, true, nil
					}
					key, deleted = bytes.Clone(bufferedKey), bufferedIt.IsDeleted()
					bufferedIt.Next()
					persistedIt.Next()
				}
			}
			if !deleted {
				return key, true, false, nil
			}
		}
	}

	var seen map[string]struct{}
	if opts.DedupeDocumentID {
		seen = make(map[string]struct{})
	}
	emitted := 0
	var groupKey []byte
	groupIDs := make([][]byte, 0)
	maxStableBytes := opts.MaxStableTieBytes
	if maxStableBytes == 0 {
		maxStableBytes = maxStableDocumentIDTieBytes
	}
	// seenBytes persists for the entire scan while groupBytes covers the
	// currently buffered logical key (including its cloned logical-key prefix).
	// They deliberately share one budget: a multikey scan can otherwise retain
	// a map's worth of document IDs in addition to each bounded tie group.
	seenBytes := 0
	groupBytes := 0
	canRetain := func(bytes int) bool {
		return bytes >= 0 && bytes <= maxStableBytes-seenBytes-groupBytes
	}
	emitGroup := func() (bool, bool, error) {
		if len(groupIDs) == 0 {
			// A dedupe-only group still retained its logical-key prefix. Release
			// that per-group charge before starting the next group; otherwise a
			// sequence of duplicate-only keys incorrectly consumes the entire
			// stable-tie budget.
			groupBytes = 0
			return true, false, nil
		}
		less := opts.DocumentIDLess
		if less == nil {
			less = func(left, right []byte) bool { return bytes.Compare(left, right) < 0 }
		}
		sort.Slice(groupIDs, func(i, j int) bool { return less(groupIDs[i], groupIDs[j]) })
		remaining := len(groupIDs)
		truncated := false
		if maxResults > 0 && remaining > maxResults-emitted {
			// The sorted prefix of a complete tie group is still deterministic.
			// Work caps never enter this path: they return before a group can be
			// completed, so they cannot expose an ambiguously incomplete group.
			remaining = maxResults - emitted
			truncated = true
		}
		for _, id := range groupIDs[:remaining] {
			cont, err := fn(id)
			if err != nil {
				return false, false, err
			}
			emitted++
			if !cont {
				return false, false, nil
			}
		}
		groupIDs = groupIDs[:0]
		groupBytes = 0
		return true, truncated, nil
	}
	for {
		key, ok, capped, err := nextLiveKey()
		if err != nil {
			return false, err
		}
		if capped {
			return true, nil
		}
		if !ok {
			cont, truncated, err := emitGroup()
			if err != nil || truncated || !cont {
				return truncated, err
			}
			return false, nil
		}
		logicalKey, err := opts.LogicalIndexKey(key)
		if err != nil {
			return false, err
		}
		if groupKey != nil && !bytes.Equal(groupKey, logicalKey) {
			cont, truncated, err := emitGroup()
			if err != nil || truncated || !cont {
				return truncated, err
			}
			groupKey = nil
		}
		if groupKey == nil {
			keyBytes := len(logicalKey) + stableDocumentIDTieEntryOverhead
			if keyBytes < len(logicalKey) || !canRetain(keyBytes) {
				return true, nil
			}
			groupKey = bytes.Clone(logicalKey)
			groupBytes += keyBytes
		}
		id, err := indexKeyDocumentID(valueType, key)
		if err != nil {
			return false, err
		}
		if opts.DedupeDocumentID {
			// Check before string conversion: map keys own their string payload for
			// the scan lifetime, and this also prevents an oversized ID from being
			// copied solely to discover that it must be rejected.
			entryBytes := len(id) + stableDocumentIDTieEntryOverhead
			if entryBytes < len(id) || !canRetain(entryBytes) {
				return true, nil
			}
			idKey := string(id)
			if _, duplicate := seen[idKey]; duplicate {
				continue
			} else {
				seen[idKey] = struct{}{}
				seenBytes += entryBytes
			}
		}
		// Never retain a partial stable group: callers would otherwise see an
		// order that could change once the remaining equal-key IDs were sorted.
		// This is checked before cloning so an oversized ID cannot transiently
		// escape the group-memory bound.
		entryBytes := len(id) + stableDocumentIDTieEntryOverhead
		if entryBytes < len(id) || !canRetain(entryBytes) {
			return true, nil
		}
		groupIDs = append(groupIDs, bytes.Clone(id))
		groupBytes += entryBytes
	}
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

// ScanDocumentIDsFunc flushes buffered writes before acquiring a snapshot, then
// calls fn for primary collection document IDs until maxDocuments is reached,
// the collection is exhausted, or fn returns false. Unlike ScanDocumentsFunc,
// it does not materialize or reconstruct document payloads.
func (c *Collection) ScanDocumentIDsFunc(maxDocuments int, fn func([]byte) (bool, error)) (bool, error) {
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
		id := bytes.Clone(it.UnsafeKey())
		scanned++
		next, err := fn(id)
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

// ScanDocumentIDsPhysicalFunc is the diagnostics-only counterpart of
// ScanDocumentIDsFunc. Its limit and returned inspected count charge every
// physical primary-source entry (including tombstones and merged-run work),
// while fn receives only live IDs. It therefore proves a complete live count
// only when the physical primary walk is exhausted.
func (c *Collection) ScanDocumentIDsPhysicalFunc(maxEntries int, fn func([]byte) (bool, error)) (inspected int, truncated bool, err error) {
	if c == nil {
		return 0, false, errCollectionNil
	}
	if c.db == nil {
		return 0, false, errCollectionDBNil
	}
	if maxEntries < 0 {
		return 0, false, errors.New("collections: max physical entries must not be negative")
	}
	if fn == nil {
		return 0, false, errors.New("collections: scan callback is nil")
	}
	if err := c.flushBufferedWrites(); err != nil {
		return 0, false, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return 0, false, err
	}
	if catalog == nil {
		return 0, false, errCollectionNotFound
	}
	rootName := collectionPrimaryRootName(catalog.meta.Name)
	if maxEntries == 0 {
		// A missing primary root and no overlay roots are a metadata proof that
		// this collection has no physical primary entries. Anything else must
		// fail closed rather than inspect a first entry outside the caller's
		// shared budget.
		if catalog.rootID(rootName) == 0 && len(catalog.overlayRootIDs(rootName)) == 0 {
			return 0, false, nil
		}
		return 0, true, nil
	}
	it, err := collectionIteratorAtCatalogRootWithWorkCapAndInspect(snap, catalog, rootName, nil, nil, true, maxEntries, func(count int) { inspected += count })
	if err != nil {
		return inspected, false, err
	}
	if it == nil {
		return inspected, false, nil
	}
	defer func() { _ = it.Close() }()
	for it.Valid() {
		if !it.IsDeleted() {
			next, err := fn(bytes.Clone(it.UnsafeKey()))
			if err != nil {
				return inspected, false, err
			}
			if !next {
				return inspected, false, nil
			}
		}
		it.Next()
	}
	if errors.Is(it.Error(), errCollectionIndexScanWorkCap) {
		return inspected, true, nil
	}
	if err := it.Error(); err != nil {
		return inspected, false, err
	}
	return inspected, false, nil
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
	var scanStats CollectionDocumentScanStats
	defer func() { c.setLastDocumentScanStats(scanStats) }()
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
	reconstructDocuments := columnStoreCanReconstructDocument(catalog.meta)
	if reconstructDocuments {
		certified, preflightDiag, preflightRan, err := c.preflightMonotonicColumnReconstruction(snap, catalog, maxDocuments)
		if err != nil {
			return false, err
		}
		if preflightRan {
			scanStats.PreflightProjectedColumns = uint64(preflightDiag.ProjectedColumns)
			scanStats.PhysicalPasses++
			scanStats.PhysicalRows += uint64(preflightDiag.RowsScanned)
			scanStats.PhysicalBytes += uint64(preflightDiag.PhysicalBytesScanned)
			scanStats.PhysicalDecodedBlocks += uint64(preflightDiag.DecodedBlocks)
		}
		if certified {
			scanStats.CertifiedMonotonicPath = true
			return c.scanDocumentsFuncWithMonotonicColumnReconstruction(snap, catalog, maxDocuments, fn, &scanStats)
		}
		scanStats.GenericFallback = true
		return c.scanDocumentsFuncWithColumnReconstruction(snap, catalog, it, maxDocuments, fn, &scanStats)
	}
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

func (c *Collection) scanDocumentsFuncWithColumnReconstruction(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	it iterator.UnsafeIterator,
	maxDocuments int,
	fn func(DocumentRecord) (bool, error),
	stats *CollectionDocumentScanStats,
) (bool, error) {
	capacity := maxDocuments
	if capacity > 256 {
		capacity = 256
	}
	seen := 0
	for it.Valid() {
		ids := make([][]byte, 0, capacity)
		for it.Valid() && len(ids) < capacity && seen+len(ids) < maxDocuments {
			if !it.IsDeleted() {
				ids = append(ids, bytes.Clone(it.UnsafeKey()))
			}
			it.Next()
		}
		if err := it.Error(); err != nil {
			return false, err
		}
		if len(ids) == 0 {
			break
		}
		if stats != nil {
			stats.LocatorLookupBatches++
			if uint64(len(ids)) > stats.MaxRecordWindow {
				stats.MaxRecordWindow = uint64(len(ids))
			}
		}
		// Point-row and typed reconstruction caches are owned by the read view.
		// Scope the view to this bounded ID window so scans spanning many
		// immutable assets cannot retain one cache entry per asset.
		view := newCollectionReadViewAtSnapshot(c, snap, catalog, false, "")
		refs, err := view.LookupDocumentRowRefsByID(ids, DocumentFetchOptions{})
		if err != nil {
			_ = view.Close()
			return false, err
		}
		if stats != nil {
			stats.LocatorLookups += refs.Stats.RowLocatorLookups
		}
		rowRefs := make([]DocumentRowRef, len(refs.Results))
		for i, result := range refs.Results {
			if !result.Found {
				_ = view.Close()
				return false, fmt.Errorf("collections: column reconstruction missing primary row locator for id %q", string(ids[i]))
			}
			rowRefs[i] = result.RowRef
		}
		fetched, err := view.fetchDocumentsByResolvedRowRef(rowRefs, DocumentFetchOptions{})
		if err != nil {
			_ = view.Close()
			return false, err
		}
		if err := view.Close(); err != nil {
			return false, err
		}
		if stats != nil {
			stats.PointRowFetches += fetched.Stats.PointRowFetches
		}
		for _, result := range fetched.Results {
			if !result.Found {
				return false, fmt.Errorf("collections: column reconstruction missing point row")
			}
			seen++
			next, err := fn(DocumentRecord{ID: bytes.Clone(result.ID), Document: result.Document})
			if err != nil {
				return false, err
			}
			if !next {
				return false, nil
			}
		}
	}
	if err := it.Error(); err != nil {
		return false, err
	}
	for it.Valid() {
		if !it.IsDeleted() {
			return true, nil
		}
		it.Next()
	}
	return false, it.Error()
}

func loadCollectionCatalog(snap *backenddb.Snapshot, name string) (*collectionCatalog, error) {
	if err := runTestCollectionCatalogLoadHook(collectionCatalogLoadFaultContext{
		Collection: name,
		Stage:      collectionCatalogLoadFaultMeta,
		CommitSeq:  snapshotCommitSeq(snap),
		SystemRoot: snapshotSystemRoot(snap),
	}); err != nil {
		return nil, fmt.Errorf("collections: load catalog %q meta: %w", name, err)
	}
	raw, ok, err := getSystemValue(snap, systemCollectionMetaKey(name))
	if err != nil || !ok {
		if err != nil {
			return nil, fmt.Errorf("collections: load catalog %q meta: %w", name, err)
		}
		return nil, err
	}
	meta, err := decodeCollectionMeta(raw)
	if err != nil {
		return nil, err
	}
	roots := make(map[string]uint64)
	rootNames := collectionRootNames(meta)
	for _, rootName := range rootNames {
		if err := runTestCollectionCatalogLoadHook(collectionCatalogLoadFaultContext{
			Collection: name,
			Stage:      collectionCatalogLoadFaultRoot,
			RootName:   rootName,
			CommitSeq:  snapshotCommitSeq(snap),
			SystemRoot: snapshotSystemRoot(snap),
		}); err != nil {
			return nil, fmt.Errorf("collections: load catalog %q root %q: %w", name, rootName, err)
		}
		rawRoot, ok, err := getSystemValue(snap, systemCollectionRootKey(rootName))
		if err != nil {
			return nil, fmt.Errorf("collections: load catalog %q root %q: %w", name, rootName, err)
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
		return nil, fmt.Errorf("collections: load catalog %q root overlays: %w", name, err)
	}
	catalog := newCollectionCatalogWithOverlays(meta, roots, rootOverlays)
	if err := validateColumnStoreCatalogRoot(snap, catalog); err != nil {
		return nil, err
	}
	return catalog, nil
}

func loadCollectionCatalogRootOverlays(snap *backenddb.Snapshot, rootNames []string) (map[string][]uint64, error) {
	if snap == nil || len(rootNames) == 0 {
		return nil, nil
	}
	state, ok := snap.StateToken()
	if !ok || state.SystemRootPageID == 0 {
		return nil, nil
	}
	prefix := []byte(systemCollectionRootOverlayPrefix)
	it, err := snap.IteratorAtRoot(state.SystemRootPageID, prefix, prefixEnd(prefix))
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
	return collectionRootStoragePolicyForDB(nil, meta, rootName)
}

func collectionRootStoragePolicyForDB(db *backenddb.DB, meta CollectionMeta, rootName string) (backenddb.OrderedRootStoragePolicy, error) {
	switch rootName {
	case collectionPrimaryRootName(meta.Name), collectionTemplateRootName(meta.Name):
		return backendCollectionDataRootStoragePolicy(db, meta.Options.DataRootStoragePolicy)
	case collectionIndexStateRootName(meta.Name):
		return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
	case collectionColumnManifestRootName(meta.Name):
		if meta.Options.ColumnStore != nil {
			return backendRootStoragePolicy(meta.Options.ColumnStore.ControlRootStoragePolicy)
		}
	case collectionColumnRowLocatorRootName(meta.Name):
		if meta.Options.ColumnStore != nil {
			return backendRootStoragePolicy(meta.Options.ColumnStore.ControlRootStoragePolicy)
		}
	case collectionRetainedSemanticStreamRootName(meta.Name):
		if columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
			return backendCollectionDataRootStoragePolicy(db, meta.Options.DataRootStoragePolicy)
		}
	}
	for _, idx := range meta.Indexes {
		if rootName == collectionSecondaryRootName(meta.Name, idx.Name) {
			return backendRootStoragePolicy(idx.StoragePolicy)
		}
	}
	for _, idx := range meta.VectorIndexes {
		if rootName == collectionVectorIndexRootName(meta.Name, idx.Name) {
			return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
		}
	}
	for _, idx := range meta.TextIndexes {
		if idx.Version == TextIndexVersionV2 {
			switch rootName {
			case collectionTextV2DocMapRootName(meta.Name, idx.Name), collectionTextV2PostingBlocksRootName(meta.Name, idx.Name), collectionTextV2NormBlocksRootName(meta.Name, idx.Name), collectionTextV2PositionsRootName(meta.Name, idx.Name):
				return backendRootStoragePolicy(idx.StoragePolicy)
			case collectionTextV2DocIDRootName(meta.Name, idx.Name), collectionTextV2TermsRootName(meta.Name, idx.Name), collectionTextV2GenerationsRootName(meta.Name, idx.Name):
				return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
			}
			continue
		}
		switch rootName {
		case collectionTextIndexRootName(meta.Name, idx.Name):
			return backendRootStoragePolicy(idx.StoragePolicy)
		case collectionTextStateRootName(meta.Name, idx.Name), collectionTextStatsRootName(meta.Name, idx.Name):
			return backendRootStoragePolicy(meta.Options.IndexStateStoragePolicy)
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

func collectionGetManyViewAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, keys [][]byte, fn backenddb.GetManyViewFunc) error {
	if fn == nil {
		return errors.New("collections: GetManyView nil callback")
	}
	if snap == nil {
		return backenddb.ErrClosed
	}
	if len(keys) == 0 {
		return nil
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			for i, key := range keys {
				if err := fn(i, key, nil, false); err != nil {
					return err
				}
			}
			return nil
		}
		err := snap.GetManyViewAtRoot(rootID, keys, fn)
		if errors.Is(err, tree.ErrKeyNotFound) {
			for i, key := range keys {
				if err := fn(i, key, nil, false); err != nil {
					return err
				}
			}
			return nil
		}
		return err
	}

	var scratch []byte
	for i, key := range keys {
		value, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, key, scratch[:0])
		if err != nil {
			return err
		}
		if !found {
			if err := fn(i, key, nil, false); err != nil {
				return err
			}
			continue
		}
		if err := fn(i, key, value, true); err != nil {
			return err
		}
		if cap(value) <= 64<<10 {
			scratch = value[:0]
		} else {
			scratch = nil
		}
	}
	return nil
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
	return collectionIteratorAtCatalogRootWithWorkCap(snap, catalog, rootName, start, end, includeDeleted, 0)
}

func collectionIteratorAtCatalogRootWithWorkCap(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool, maxInspected int) (iterator.UnsafeIterator, error) {
	return collectionIteratorAtCatalogRootWithWorkCapAndInspect(snap, catalog, rootName, start, end, includeDeleted, maxInspected, nil)
}

func collectionIteratorAtCatalogRootWithWorkCapAndInspect(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool, maxInspected int, onInspected func(int)) (iterator.UnsafeIterator, error) {
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
		if err == nil && it != nil && maxInspected > 0 {
			return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect([]bufferedRootRunIteratorSource{{iter: it}}, start, end, includeDeleted, false, false, maxInspected, onInspected), nil
		}
		return it, err
	}
	rootIDs := catalog.rootStack(rootName)
	if len(rootIDs) == 0 {
		return nil, nil
	}
	if maxInspected > 0 && len(rootIDs) > maxInspected {
		return nil, errCollectionIndexScanWorkCap
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
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect(sources, start, end, includeDeleted, false, false, maxInspected, onInspected), nil
}

func collectionReverseIteratorAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool) (iterator.UnsafeIterator, error) {
	return collectionReverseIteratorAtCatalogRootWithWorkCap(snap, catalog, rootName, start, end, includeDeleted, 0)
}

func collectionReverseIteratorAtCatalogRootWithWorkCap(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool, maxInspected int) (iterator.UnsafeIterator, error) {
	return collectionReverseIteratorAtCatalogRootWithWorkCapAndInspect(snap, catalog, rootName, start, end, includeDeleted, maxInspected, nil)
}

func collectionReverseIteratorAtCatalogRootWithWorkCapAndInspect(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, start, end []byte, includeDeleted bool, maxInspected int, onInspected func(int)) (iterator.UnsafeIterator, error) {
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return nil, nil
		}
		it, err := snap.ReverseIteratorAtRootWithOptions(rootID, start, end, backenddb.IteratorOptions{IncludeTombstones: includeDeleted})
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, nil
		}
		if err == nil && it != nil && maxInspected > 0 {
			return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect([]bufferedRootRunIteratorSource{{iter: it}}, start, end, includeDeleted, false, true, maxInspected, onInspected), nil
		}
		return it, err
	}
	rootIDs := catalog.rootStack(rootName)
	if maxInspected > 0 && len(rootIDs) > maxInspected {
		return nil, errCollectionIndexScanWorkCap
	}
	sources := make([]bufferedRootRunIteratorSource, 0, len(rootIDs))
	for i, rootID := range rootIDs {
		if rootID == 0 {
			continue
		}
		it, err := snap.ReverseIteratorAtRootWithOptions(rootID, start, end, backenddb.IteratorOptions{IncludeTombstones: true})
		if errors.Is(err, tree.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			for _, source := range sources {
				_ = source.iter.Close()
			}
			return nil, err
		}
		sources = append(sources, bufferedRootRunIteratorSource{iter: it, priority: i})
	}
	if len(sources) == 0 {
		return nil, nil
	}
	return newBufferedRootRunIteratorSourcesIteratorWithDeletedDirectionWorkCapAndInspect(sources, start, end, includeDeleted, false, true, maxInspected, onInspected), nil
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
	if snap == nil {
		return nil, false, nil
	}
	state, ok := snap.StateToken()
	if !ok || state.SystemRootPageID == 0 {
		return nil, false, nil
	}
	entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte(key))
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
	flags byte
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
	if !it.Valid() || it.IsDeleted() {
		return nil
	}
	return it.entries[it.idx].value
}

func (it *systemTargetIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *systemTargetIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision
	}
	entry := it.entries[it.idx]
	if entry.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision
	}
	flags := entry.flags
	if flags == 0 {
		flags = node.FlagInline
	}
	return entry.value, page.ValuePtr{}, flags, page.LegacyEntryRevision
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
	if !it.Valid() || it.IsDeleted() {
		return dst[:0]
	}
	return append(dst, it.entries[it.idx].value...)
}

func (it *systemTargetIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.idx].flags&node.FlagTombstone != 0
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
	state, stateOK := snap.StateToken()
	if snap != nil && stateOK && state.SystemRootPageID != 0 {
		it, err := snap.IteratorAtRoot(state.SystemRootPageID, nil, nil)
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
	return encodeNormalizedCollectionMeta(normalized)
}

// encodeNormalizedCollectionMeta requires meta to already be normalized by
// normalizeCollectionMeta so disk metadata preserves canonical defaults/order.
func encodeNormalizedCollectionMeta(meta CollectionMeta) ([]byte, error) {
	return json.Marshal(collectionMetaDisk{
		Version:       collectionMetaVersion,
		Name:          meta.Name,
		Options:       meta.Options,
		Indexes:       meta.Indexes,
		VectorIndexes: meta.VectorIndexes,
		TextIndexes:   meta.TextIndexes,
	})
}

func decodeCollectionMeta(raw []byte) (CollectionMeta, error) {
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		return CollectionMeta{}, err
	}
	if disk.Version != collectionMetaVersionV5 && disk.Version != collectionMetaVersion {
		return CollectionMeta{}, fmt.Errorf("collections: unsupported collection metadata version %d", disk.Version)
	}
	if disk.Version == collectionMetaVersionV5 {
		for _, index := range disk.Indexes {
			if len(index.Components) != 0 {
				return CollectionMeta{}, errors.New("collections: version 5 metadata cannot define compound index components")
			}
		}
	}
	meta := CollectionMeta{
		Name:          disk.Name,
		Options:       disk.Options,
		Indexes:       disk.Indexes,
		VectorIndexes: disk.VectorIndexes,
		TextIndexes:   disk.TextIndexes,
	}
	// Metadata written before the text-index version field may omit `version`
	// while still owning v1 roots. Preserve those existing on-disk indexes as v1;
	// only new API declarations use the current default (v2).
	for i := range meta.TextIndexes {
		if meta.TextIndexes[i].Version == TextIndexVersionDefault {
			meta.TextIndexes[i].Version = TextIndexVersionV1
		}
	}
	return normalizeCollectionMeta(meta)
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
	columnStore, err := normalizeColumnStoreConfig(meta.Name, meta.Options.ColumnStore)
	if err != nil {
		return CollectionMeta{}, err
	}
	meta.Options.ColumnStore = columnStore
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
	if meta.Options.DisableBufferedIndexedAsyncFlush && meta.Options.BufferedIndexedAsyncFlush {
		return CollectionMeta{}, errors.New("collections: buffered indexed async flush cannot be both enabled and disabled")
	}
	if meta.Options.DisableBufferedIndexedAsyncFlush && meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits != 0 {
		return CollectionMeta{}, errors.New("collections: buffered indexed async flush max queued units cannot be set when async flush is disabled")
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
	indexes := copyIndexDefinitions(meta.Indexes)
	sort.SliceStable(indexes, func(i, j int) bool {
		return indexes[i].Name < indexes[j].Name
	})
	seen := make(map[string]struct{}, len(indexes))
	for i := range indexes {
		hadExplicitComponents := len(indexes[i].Components) != 0
		components, err := normalizeIndexComponents(indexes[i])
		if err != nil {
			return CollectionMeta{}, fmt.Errorf("collections: invalid index %q components: %w", indexes[i].Name, err)
		}
		// Preserve the compact legacy single-field ascending spelling on disk.
		// Only explicit ordered definitions need the versioned Components slice.
		if hadExplicitComponents {
			indexes[i].Components = components
		} else {
			indexes[i].Components = nil
		}
		indexes[i].Field = components[0].Field
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
		if valueType == IndexValueBSONOrderedV2 && documentFormat != DocumentFormatBSON {
			return CollectionMeta{}, fmt.Errorf("collections: index %q BSON v2 key format requires BSON document_format", indexes[i].Name)
		}
		if valueType != IndexValueBSONOrderedV2 && (len(components) != 1 || components[0].Direction != IndexDirectionAscending) {
			return CollectionMeta{}, fmt.Errorf("collections: index %q compound or descending components require BSON v2 key format", indexes[i].Name)
		}
		if indexes[i].MultiKey && (len(components) > 1 || components[0].Direction == IndexDirectionDescending) {
			// Ordered BSON v2 compound and descending extraction deliberately fails
			// closed on arrays; do not publish a multikey contract they cannot honor.
			return CollectionMeta{}, fmt.Errorf("collections: index %q ordered BSON v2 components do not support multikey", indexes[i].Name)
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
	vectorIndexes := append([]VectorIndexDefinition(nil), meta.VectorIndexes...)
	for i := range vectorIndexes {
		normalized, err := normalizeVectorIndexDefinition(vectorIndexes[i])
		if err != nil {
			name := vectorIndexes[i].Name
			if name == "" {
				name = vectorIndexDefaultName(vectorIndexes[i].Field)
			}
			return CollectionMeta{}, fmt.Errorf("collections: invalid vector index %q: %w", name, err)
		}
		vectorIndexes[i] = normalized
	}
	sort.SliceStable(vectorIndexes, func(i, j int) bool {
		return vectorIndexes[i].Name < vectorIndexes[j].Name
	})
	for i := range vectorIndexes {
		if _, ok := seen[vectorIndexes[i].Name]; ok {
			return CollectionMeta{}, fmt.Errorf("collections: duplicate index %q", vectorIndexes[i].Name)
		}
		seen[vectorIndexes[i].Name] = struct{}{}
	}
	meta.VectorIndexes = vectorIndexes
	textIndexes := copyTextIndexDefinitions(meta.TextIndexes)
	for i := range textIndexes {
		normalized, err := normalizeTextIndexDefinition(textIndexes[i])
		if err != nil {
			name := textIndexes[i].Name
			if name == "" {
				name = "<unnamed>"
			}
			return CollectionMeta{}, fmt.Errorf("collections: invalid text index %q: %w", name, err)
		}
		textIndexes[i] = normalized
	}
	sort.SliceStable(textIndexes, func(i, j int) bool {
		return textIndexes[i].Name < textIndexes[j].Name
	})
	for i := range textIndexes {
		if _, ok := seen[textIndexes[i].Name]; ok {
			return CollectionMeta{}, fmt.Errorf("collections: duplicate index %q", textIndexes[i].Name)
		}
		seen[textIndexes[i].Name] = struct{}{}
	}
	meta.TextIndexes = textIndexes
	if meta.Options.DisableIndexedWriteMemtables {
		meta.Options.BufferedIndexedWrites = false
		meta.Options.BufferedIndexedWriteMaxDocuments = 0
		meta.Options.BufferedIndexedWriteMaxBytes = 0
		meta.Options.BufferedIndexedWriteMaxRootRuns = 0
		meta.Options.BufferedIndexedOverlayRoots = false
	} else if len(meta.Indexes) == 0 && len(meta.VectorIndexes) == 0 && len(meta.TextIndexes) == 0 {
		meta.Options.BufferedIndexedWrites = false
	} else {
		meta.Options.BufferedIndexedWrites = true
		// Indexed schemas publish threshold flushes asynchronously by default.
		// Foreground threshold publish is an explicit opt-out so old metadata with
		// BufferedIndexedAsyncFlush=false still normalizes to the current default.
		if meta.Options.DisableBufferedIndexedAsyncFlush {
			meta.Options.BufferedIndexedAsyncFlush = false
			meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits = 0
		} else {
			meta.Options.BufferedIndexedAsyncFlush = true
		}
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
	if !meta.Options.BufferedIndexedWrites {
		meta.Options.BufferedIndexedAsyncFlush = false
		meta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits = 0
	}
	return meta, nil
}

const maxCompoundIndexComponents = 4

func normalizeIndexComponents(def IndexDefinition) ([]IndexComponent, error) {
	components := append([]IndexComponent(nil), def.Components...)
	if len(components) == 0 {
		components = []IndexComponent{{Field: def.Field, Direction: IndexDirectionAscending}}
	}
	if len(components) == 0 || len(components) > maxCompoundIndexComponents {
		return nil, fmt.Errorf("component count must be in [1,%d]", maxCompoundIndexComponents)
	}
	seen := make(map[string]struct{}, len(components))
	for i := range components {
		if err := ValidateIndexPath(components[i].Field); err != nil {
			return nil, fmt.Errorf("component[%d] field: %w", i, err)
		}
		if components[i].Direction != IndexDirectionAscending && components[i].Direction != IndexDirectionDescending {
			return nil, fmt.Errorf("component[%d] direction must be 1 or -1", i)
		}
		if _, ok := seen[components[i].Field]; ok {
			return nil, fmt.Errorf("duplicate component field %q", components[i].Field)
		}
		seen[components[i].Field] = struct{}{}
	}
	if def.Field != "" && def.Field != components[0].Field {
		return nil, fmt.Errorf("field %q disagrees with first component %q", def.Field, components[0].Field)
	}
	return components, nil
}

func normalizeIndexValueType(valueType IndexValueType) (IndexValueType, error) {
	switch valueType {
	case IndexValueString, IndexValueBool, IndexValueInt64, IndexValueDouble, IndexValueBSONOrderedV2:
		return valueType, nil
	case "":
		return "", errors.New("value_type is required")
	default:
		return "", fmt.Errorf("unsupported value_type %q", valueType)
	}
}

func normalizeVectorIndexDefinition(def VectorIndexDefinition) (VectorIndexDefinition, error) {
	if def.Name == "" {
		def.Name = vectorIndexDefaultName(def.Field)
	}
	if err := ValidateIndexName(def.Name); err != nil {
		return VectorIndexDefinition{}, err
	}
	if err := ValidateIndexPath(def.Field); err != nil {
		return VectorIndexDefinition{}, fmt.Errorf("field: %w", err)
	}
	if _, err := parseVectorFieldPath(def.Field); err != nil {
		return VectorIndexDefinition{}, err
	}
	if def.Dimensions <= 0 {
		return VectorIndexDefinition{}, errors.New("dimensions must be positive")
	}
	metric, err := normalizeVectorMetric(def.Metric)
	if err != nil {
		return VectorIndexDefinition{}, err
	}
	encoding, err := normalizeVectorIndexEncoding(def.Encoding)
	if err != nil {
		return VectorIndexDefinition{}, err
	}
	strategy, err := normalizeVectorIndexStrategy(def.Strategy)
	if err != nil {
		return VectorIndexDefinition{}, err
	}
	def.Metric = metric
	def.Encoding = encoding
	def.Strategy = strategy
	quantized, err := normalizeQuantizedVectorIndexDefinitions(def)
	if err != nil {
		return VectorIndexDefinition{}, err
	}
	def.QuantizedIndexes = quantized
	if len(def.QuantizedIndexes) > 0 && def.Strategy != VectorIndexStrategyColumnGraph {
		return VectorIndexDefinition{}, fmt.Errorf("collections: quantized vector indexes require strategy %q", VectorIndexStrategyColumnGraph)
	}
	if def.Strategy == VectorIndexStrategyColumnGraph {
		if def.Metric != VectorMetricCosine {
			return VectorIndexDefinition{}, fmt.Errorf("collections: column_graph vector index %q supports only metric %q", def.Name, VectorMetricCosine)
		}
		if def.Encoding != VectorIndexEncodingFloat32 {
			return VectorIndexDefinition{}, fmt.Errorf("collections: column_graph vector index %q supports only encoding %q", def.Name, VectorIndexEncodingFloat32)
		}
	}
	if def.M < 0 {
		return VectorIndexDefinition{}, errors.New("m cannot be negative")
	}
	if def.EfConstruction < 0 {
		return VectorIndexDefinition{}, errors.New("ef_construction cannot be negative")
	}
	if def.EfSearch < 0 {
		return VectorIndexDefinition{}, errors.New("ef_search cannot be negative")
	}
	if def.M <= 0 {
		def.M = defaultVectorIndexM
	}
	if def.EfConstruction <= 0 {
		def.EfConstruction = defaultVectorIndexEfConstruction
	}
	if def.EfConstruction < def.M {
		def.EfConstruction = def.M
	}
	if def.EfSearch <= 0 {
		def.EfSearch = defaultVectorIndexEfSearch
	}
	return def, nil
}

func normalizeVectorIndexStrategy(strategy VectorIndexStrategy) (VectorIndexStrategy, error) {
	switch strategy {
	case "":
		return VectorIndexStrategyNativeRuntime, nil
	case VectorIndexStrategyNativeRuntime, VectorIndexStrategyColumnGraph:
		return strategy, nil
	default:
		return "", fmt.Errorf("unsupported strategy %q", strategy)
	}
}

const QuantizedVectorCodecScalarU8 = "scalar_u8"

func normalizeQuantizedVectorIndexDefinitions(def VectorIndexDefinition) ([]QuantizedVectorIndexDefinition, error) {
	if len(def.QuantizedIndexes) == 0 {
		return nil, nil
	}
	out := make([]QuantizedVectorIndexDefinition, len(def.QuantizedIndexes))
	seen := make(map[string]struct{}, len(def.QuantizedIndexes))
	for i, q := range def.QuantizedIndexes {
		if q.Name == "" {
			return nil, fmt.Errorf("collections: vector index %q quantized index[%d] name is required", def.Name, i)
		}
		if err := ValidateIndexName(q.Name); err != nil {
			return nil, fmt.Errorf("collections: vector index %q quantized index[%d] name: %w", def.Name, i, err)
		}
		if _, ok := seen[q.Name]; ok {
			return nil, fmt.Errorf("collections: vector index %q duplicate quantized index %q", def.Name, q.Name)
		}
		seen[q.Name] = struct{}{}
		switch q.Codec {
		case "":
			q.Codec = QuantizedVectorCodecScalarU8
		case QuantizedVectorCodecScalarU8, rabitq.CodecName, brq.CodecName:
		default:
			return nil, fmt.Errorf("collections: vector index %q quantized index %q codec %q is unsupported", def.Name, q.Name, q.Codec)
		}
		scalarU8Calibration, err := normalizeScalarU8CalibrationConfig(def.Name, i, q)
		if err != nil {
			return nil, err
		}
		q.ScalarU8Calibration = scalarU8Calibration
		if q.Version == 0 {
			q.Version = 1
		}
		switch q.Codec {
		case QuantizedVectorCodecScalarU8:
			if q.Version != 1 {
				return nil, fmt.Errorf("collections: vector index %q quantized index %q scalar_u8 version=%d is unsupported", def.Name, q.Name, q.Version)
			}
		case rabitq.CodecName:
			if q.Version != rabitq.CodecVersion {
				return nil, fmt.Errorf("collections: vector index %q quantized index %q rabitq_1bit version=%d is unsupported", def.Name, q.Name, q.Version)
			}
		case brq.CodecName:
			if q.Version != brq.CodecVersion {
				return nil, fmt.Errorf("collections: vector index %q quantized index %q brq_1bit version=%d is unsupported", def.Name, q.Name, q.Version)
			}
		default:
			return nil, fmt.Errorf("collections: vector index %q quantized index %q codec %q missing version validation", def.Name, q.Name, q.Codec)
		}
		out[i] = q
	}
	return out, nil
}

func findQuantizedVectorIndex(def VectorIndexDefinition, name string) (QuantizedVectorIndexDefinition, bool) {
	for _, q := range def.QuantizedIndexes {
		if q.Name == name {
			return q, true
		}
	}
	return QuantizedVectorIndexDefinition{}, false
}

func (m CollectionMeta) copy() *CollectionMeta {
	return &CollectionMeta{
		Name:          m.Name,
		Options:       copyCollectionOptions(m.Options),
		Indexes:       copyIndexDefinitions(m.Indexes),
		VectorIndexes: copyVectorIndexDefinitions(m.VectorIndexes),
		TextIndexes:   copyTextIndexDefinitions(m.TextIndexes),
	}
}

func copyIndexDefinitions(in []IndexDefinition) []IndexDefinition {
	out := append([]IndexDefinition(nil), in...)
	for i := range out {
		out[i].Components = append([]IndexComponent(nil), out[i].Components...)
	}
	return out
}

func copyVectorIndexDefinitions(in []VectorIndexDefinition) []VectorIndexDefinition {
	out := append([]VectorIndexDefinition(nil), in...)
	for i := range out {
		out[i].QuantizedIndexes = copyQuantizedVectorIndexDefinitions(out[i].QuantizedIndexes)
	}
	return out
}

func copyQuantizedVectorIndexDefinitions(in []QuantizedVectorIndexDefinition) []QuantizedVectorIndexDefinition {
	out := append([]QuantizedVectorIndexDefinition(nil), in...)
	for i := range out {
		out[i].ScalarU8Calibration = scalarU8CalibrationConfigClone(out[i].ScalarU8Calibration)
	}
	return out
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
	if a.Name != b.Name ||
		!collectionOptionsEqual(a.Options, b.Options) ||
		len(a.Indexes) != len(b.Indexes) ||
		len(a.VectorIndexes) != len(b.VectorIndexes) ||
		len(a.TextIndexes) != len(b.TextIndexes) {
		return false
	}
	for i := range a.Indexes {
		if !indexDefinitionValuesEqual(a.Indexes[i], b.Indexes[i]) {
			return false
		}
	}
	for i := range a.VectorIndexes {
		if !vectorIndexDefinitionValuesEqual(a.VectorIndexes[i], b.VectorIndexes[i]) {
			return false
		}
	}
	for i := range a.TextIndexes {
		if !textIndexDefinitionValuesEqual(a.TextIndexes[i], b.TextIndexes[i]) {
			return false
		}
	}
	return true
}

func indexDefinitionValuesEqual(a, b IndexDefinition) bool {
	if a.Name != b.Name || a.Field != b.Field || a.ValueType != b.ValueType || a.Unique != b.Unique || a.MultiKey != b.MultiKey || a.StoragePolicy != b.StoragePolicy || len(a.Components) != len(b.Components) {
		return false
	}
	for i := range a.Components {
		if a.Components[i] != b.Components[i] {
			return false
		}
	}
	return true
}

func vectorIndexDefinitionValuesEqual(a, b VectorIndexDefinition) bool {
	if a.Name != b.Name ||
		a.Field != b.Field ||
		a.Metric != b.Metric ||
		a.Dimensions != b.Dimensions ||
		a.M != b.M ||
		a.EfConstruction != b.EfConstruction ||
		a.EfSearch != b.EfSearch ||
		a.Encoding != b.Encoding ||
		a.Strategy != b.Strategy ||
		a.SchemaGeneration != b.SchemaGeneration ||
		len(a.QuantizedIndexes) != len(b.QuantizedIndexes) {
		return false
	}
	for i := range a.QuantizedIndexes {
		if !quantizedVectorIndexDefinitionValuesEqual(a.QuantizedIndexes[i], b.QuantizedIndexes[i]) {
			return false
		}
	}
	return true
}

func quantizedVectorIndexDefinitionValuesEqual(a, b QuantizedVectorIndexDefinition) bool {
	if a.Name != b.Name || a.Codec != b.Codec || a.Version != b.Version {
		return false
	}
	if a.Codec == QuantizedVectorCodecScalarU8 {
		return scalarU8CalibrationConfigEqual(a.ScalarU8Calibration, b.ScalarU8Calibration)
	}
	return scalarU8CalibrationConfigStrictEqual(a.ScalarU8Calibration, b.ScalarU8Calibration)
}

func collectionMetaHasSecondaryUniqueIndex(meta CollectionMeta) bool {
	for _, idx := range meta.Indexes {
		if idx.Unique {
			return true
		}
	}
	return false
}

func addVectorIndexToCollectionMeta(meta CollectionMeta, def VectorIndexDefinition) (CollectionMeta, VectorIndexDefinition, error) {
	if _, ok := findIndex(meta.Indexes, def.Name); ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, def.Name); ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findTextIndex(meta.TextIndexes, def.Name); ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	candidate := CollectionMeta{
		Name:          meta.Name,
		Options:       meta.Options,
		Indexes:       copyIndexDefinitions(meta.Indexes),
		VectorIndexes: append(append([]VectorIndexDefinition(nil), meta.VectorIndexes...), def),
		TextIndexes:   copyTextIndexDefinitions(meta.TextIndexes),
	}
	normalized, err := normalizeCollectionMeta(candidate)
	if err != nil {
		return CollectionMeta{}, VectorIndexDefinition{}, err
	}
	normalizedDef, ok := findVectorIndex(normalized.VectorIndexes, def.Name)
	if !ok && def.Name == "" {
		normalizedDef, ok = findVectorIndex(normalized.VectorIndexes, vectorIndexDefaultName(def.Field))
	}
	if !ok {
		return CollectionMeta{}, VectorIndexDefinition{}, fmt.Errorf("collections: normalized vector index %q not found", def.Name)
	}
	return normalized, normalizedDef, nil
}

func addIndexToCollectionMeta(meta CollectionMeta, def IndexDefinition) (CollectionMeta, IndexDefinition, error) {
	if _, ok := findIndex(meta.Indexes, def.Name); ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findVectorIndex(meta.VectorIndexes, def.Name); ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	if _, ok := findTextIndex(meta.TextIndexes, def.Name); ok {
		return CollectionMeta{}, IndexDefinition{}, fmt.Errorf("collections: duplicate index %q", def.Name)
	}
	candidate := CollectionMeta{
		Name:          meta.Name,
		Options:       meta.Options,
		Indexes:       append(copyIndexDefinitions(meta.Indexes), def),
		VectorIndexes: append([]VectorIndexDefinition(nil), meta.VectorIndexes...),
		TextIndexes:   copyTextIndexDefinitions(meta.TextIndexes),
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
			components:    append([]IndexComponent(nil), idx.Components...),
		}
	}
	return out
}

func uniqueIndexRootIDs(catalog *collectionCatalog) map[string]uint64 {
	if catalog == nil {
		return nil
	}
	out := make(map[string]uint64)
	if runtimes, err := catalog.cachedIndexRuntimes(); err == nil && len(runtimes) > 0 {
		for _, runtime := range runtimes {
			if !runtime.def.unique {
				continue
			}
			rootID := catalog.rootID(runtimeSecondaryRootName(catalog.meta.Name, runtime))
			if rootID != 0 {
				out[runtime.def.name] = rootID
			}
		}
		return out
	}
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

func findVectorIndex(indexes []VectorIndexDefinition, name string) (VectorIndexDefinition, bool) {
	for _, idx := range indexes {
		if idx.Name == name {
			return idx, true
		}
	}
	return VectorIndexDefinition{}, false
}

func collectionRootNames(meta CollectionMeta) []string {
	out := []string{collectionPrimaryRootName(meta.Name)}
	if normalizedDocumentFormat(meta.Options.DocumentFormat) == DocumentFormatTemplateV1 || columnStoreRetainedPayloadUsesTemplateV1(meta.Options.ColumnStore) {
		out = append(out, collectionTemplateRootName(meta.Name))
	}
	if len(meta.Indexes) > 0 && persistIndexStateForDocumentFormat(meta.Options.DocumentFormat) {
		out = append(out, collectionIndexStateRootName(meta.Name))
	}
	if meta.Options.ColumnStore != nil && meta.Options.ColumnStore.Enabled {
		out = append(out, collectionColumnManifestRootName(meta.Name))
		// The locator maps each live primary ID to the physical row created by
		// the same command-WAL publication. It is deliberately a separate root:
		// primary values remain their existing raw-document/value-log payloads.
		out = append(out, collectionColumnRowLocatorRootName(meta.Name))
	}
	if columnStoreRetainedPayloadUsesSemanticStreamV1(meta.Options.ColumnStore) {
		out = append(out, collectionRetainedSemanticStreamRootName(meta.Name))
	}
	for _, idx := range meta.Indexes {
		out = append(out, collectionSecondaryRootName(meta.Name, idx.Name))
	}
	for _, idx := range meta.VectorIndexes {
		out = append(out, collectionVectorIndexRootName(meta.Name, idx.Name))
	}
	for _, idx := range meta.TextIndexes {
		out = append(out, collectionTextRootNamesForDefinition(meta.Name, idx)...)
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

func collectionColumnManifestRootName(collection string) string {
	return collection + "/column/manifest"
}

func collectionColumnRowLocatorRootName(collection string) string {
	return collection + "/column/row-locator"
}

func collectionRetainedSemanticStreamRootName(collection string) string {
	return collection + "/retained/semantic-stream-v1"
}

func collectionSecondaryRootName(collection, indexName string) string {
	return collection + "/index/" + indexName
}

func collectionVectorIndexRootName(collection, indexName string) string {
	return collection + "/vector-index/" + indexName
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

func systemCollectionDocumentGenerationKey(collection string) string {
	return systemCollectionDocumentGenerationPrefix + collection
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
