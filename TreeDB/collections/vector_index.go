package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	defaultVectorIndexM                       = 16
	defaultVectorIndexEfConstruction          = 128
	defaultVectorIndexEfSearch                = 64
	defaultVectorIndexFetchMultiple           = 4
	defaultVectorIndexRebuildPPM              = 250_000
	defaultVectorIndexExactFilterMax          = 1024
	defaultVectorRecallBatchCells             = 1 << 20
	maxVectorIndexEagerNeighborCap            = 64
	minVectorIndexParallelReciprocalNeighbors = 4
)

const (
	vectorIndexFallbackMissingVectorIndexMetadata = "missing_vector_index_metadata"
	vectorIndexFallbackMissingGraphRoot           = "missing_graph_root"
	vectorIndexFallbackMissingGraphRootEntry      = "missing_graph_root_entry"
	vectorIndexFallbackInvalidGraphRootKey        = "invalid_graph_root_key"
	vectorIndexFallbackInvalidGraphRootEntry      = "invalid_graph_root_entry"
	vectorIndexFallbackStaleRuntimeIndex          = "stale_runtime_index_ignored"
	vectorIndexFallbackStaleDocumentRoot          = "stale_document_root"
	vectorIndexFallbackMetaMismatch               = "meta_mismatch"
	vectorIndexFallbackInvalidEncoding            = "invalid_encoding"
	vectorIndexFallbackMetaEncodingMismatch       = "meta_encoding_mismatch"
	vectorIndexFallbackMetaDimensionMismatch      = "meta_dimension_mismatch"
	vectorIndexFallbackInvalidDimensions          = "invalid_dimensions"
	vectorIndexFallbackInvalidEdgeNode            = "invalid_edge_node"
	vectorIndexFallbackInvalidTombstone           = "invalid_tombstone"
	vectorIndexFallbackInvalidDocMapNode          = "invalid_docmap_node"
	vectorIndexFallbackInvalidEntry               = "invalid_entry"
	vectorIndexFallbackMissingManifest            = "missing_manifest"
	vectorIndexFallbackInvalidManifest            = "invalid_manifest"
)

var errVectorIndexStaleRuntime = errors.New("collections: vector index runtime handle is stale")

var nativeVectorIndexBeforeInstallHookForTest struct {
	mu sync.Mutex
	fn func(string)
}

var nativeVectorIndexBeforeAutoPersistSaveHookForTest struct {
	mu sync.Mutex
	fn func(string)
}

func setNativeVectorIndexBeforeInstallHookForTest(fn func(string)) func() {
	nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
	previous := nativeVectorIndexBeforeInstallHookForTest.fn
	nativeVectorIndexBeforeInstallHookForTest.fn = fn
	nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	return func() {
		nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
		nativeVectorIndexBeforeInstallHookForTest.fn = previous
		nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	}
}

func runNativeVectorIndexBeforeInstallHookForTest(name string) {
	nativeVectorIndexBeforeInstallHookForTest.mu.Lock()
	fn := nativeVectorIndexBeforeInstallHookForTest.fn
	nativeVectorIndexBeforeInstallHookForTest.mu.Unlock()
	if fn != nil {
		fn(name)
	}
}

func setNativeVectorIndexBeforeAutoPersistSaveHookForTest(fn func(string)) func() {
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
	previous := nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn = fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	return func() {
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn = previous
		nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	}
}

func runNativeVectorIndexBeforeAutoPersistSaveHookForTest(name string) {
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Lock()
	fn := nativeVectorIndexBeforeAutoPersistSaveHookForTest.fn
	nativeVectorIndexBeforeAutoPersistSaveHookForTest.mu.Unlock()
	if fn != nil {
		fn(name)
	}
}

// VectorIndexEncoding selects the process-local ANN vector copy format. The
// collection row remains canonical; float32 indexes can rerank directly from the
// indexed vector copy, while compressed indexes rerank from canonical rows.
type VectorIndexEncoding uint8

const (
	VectorIndexEncodingFloat32 VectorIndexEncoding = iota
	VectorIndexEncodingInt8
)

type VectorIndexStrategy string

const (
	VectorIndexStrategyNativeRuntime VectorIndexStrategy = "native_runtime"
	// VectorIndexStrategyColumnGraph selects the physical column-store graph path.
	// Until graph assets are built and published, status must report unavailable
	// or rebuild-needed rather than falling back to a decoded in-memory graph.
	VectorIndexStrategyColumnGraph VectorIndexStrategy = "column_graph"
)

type VectorIndexState string

const (
	VectorIndexStateNativeRuntime            VectorIndexState = "native_runtime"
	VectorIndexStateColumnGraphLoaded        VectorIndexState = "column_graph_loaded"
	VectorIndexStateColumnGraphUnavailable   VectorIndexState = "column_graph_unavailable"
	VectorIndexStateColumnGraphRebuildNeeded VectorIndexState = "column_graph_rebuild_needed"
)

type VectorIndexReason string

const (
	VectorIndexReasonNativeRuntime                     VectorIndexReason = "native_runtime_index"
	VectorIndexReasonColumnGraphRebuildNeeded          VectorIndexReason = "column_graph_rebuild_needed"
	VectorIndexReasonPhysicalColumnAssetSupportMissing VectorIndexReason = "physical_column_asset_support_missing"
	VectorIndexReasonColumnGraphAssetMismatch          VectorIndexReason = "column_graph_asset_mismatch"
	VectorIndexReasonColumnGraphCorrupt                VectorIndexReason = "column_graph_corrupt"
	VectorIndexReasonColumnGraphUnsupportedVisibility  VectorIndexReason = "column_graph_unsupported_visibility"
	VectorIndexReasonColumnGraphUnsupportedMetric      VectorIndexReason = "column_graph_unsupported_metric"
	VectorIndexReasonUnsupportedStrategy               VectorIndexReason = "unsupported_vector_index_strategy"
)

func (e VectorIndexEncoding) String() string {
	switch e {
	case VectorIndexEncodingFloat32:
		return "float32"
	case VectorIndexEncodingInt8:
		return "int8"
	default:
		return fmt.Sprintf("unknown(%d)", e)
	}
}

func (e VectorIndexEncoding) MarshalJSON() ([]byte, error) {
	encoding, err := normalizeVectorIndexEncoding(e)
	if err != nil {
		return nil, err
	}
	return json.Marshal(encoding.String())
}

func (e *VectorIndexEncoding) UnmarshalJSON(raw []byte) error {
	if e == nil {
		return errors.New("collections: nil vector index encoding")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		encoding, err := parseVectorIndexEncoding(s)
		if err != nil {
			return err
		}
		*e = encoding
		return nil
	}
	var n uint8
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	encoding, err := normalizeVectorIndexEncoding(VectorIndexEncoding(n))
	if err != nil {
		return err
	}
	*e = encoding
	return nil
}

// VectorIndexOptions configures an in-memory vector secondary index built from
// collection rows. The index stores stable collection document IDs and vector
// copies for graph search; TreeDB collection rows remain canonical for returned
// document payloads and filtered reranking.
type VectorIndexOptions struct {
	Name                string
	Field               string
	Metric              VectorMetric
	Dimensions          int
	M                   int
	EfConstruction      int
	EfSearch            int
	RebuildDeletedRatio float64
	Encoding            VectorIndexEncoding
	schemaGeneration    uint64
}

// VectorIndexQueryMode selects the score plane used by column_graph search.
// The zero value is exact to preserve existing search behavior. Quantized modes
// must be selected explicitly with a named quantized index and fail closed until
// matching assets and scorers are available.
type VectorIndexQueryMode string

const (
	VectorIndexQueryModeExact           VectorIndexQueryMode = "exact"
	VectorIndexQueryModeQuantizedOnly   VectorIndexQueryMode = "quantized_only"
	VectorIndexQueryModeQuantizedRerank VectorIndexQueryMode = "quantized_rerank"
)

// VectorIndexSearchOptions configures one vector index search.
//
// Collection-level high-QPS no-document searches are intentionally narrow: use
// an explicit column_graph index, exact/zero QueryMode, IncludeDocuments=false,
// no document projection, and no legacy filter fields. The current
// Collection.SearchVectorIndex method is a response-owned convenience boundary
// that uses the cached hnsw_search_pack_v1 route for healthy exact no-document
// calls and falls back to a one-shot searcher for unsupported shapes.
// Collection.SearchVectorIndexWithBuffer exposes caller-owned result storage for
// the exact no-document hnsw_search_pack_v1 seam and reuses collection-owned
// prepared pack state on warmed healthy current
// state; callers that need explicit snapshot lifetime control can still use a
// reusable VectorIndexSearcher with VectorIndexSearcher.SearchWithBuffer. To
// materialize documents after a no-document search, open a CollectionReadView
// and call FetchDocumentsForVectorIndexSearchResults as an explicit, separately
// measured fetch phase.
type VectorIndexSearchOptions struct {
	// IndexName is used by collection-level physical column_graph search.
	IndexName string
	// Query is used by collection-level physical column_graph search.
	Query []float32
	// QueryMode selects exact, quantized-only, or quantized-rerank search for
	// collection column_graph APIs. The zero value is exact. Only exact/zero mode
	// is in the collection-level no-document high-QPS contract; quantized modes are
	// explicit score-plane paths with their own fail-closed semantics.
	QueryMode VectorIndexQueryMode
	// QuantizedIndexName selects the named derived score plane for quantized modes.
	QuantizedIndexName string
	// QuantizedRerankCandidates bounds the quantized candidate set reranked by
	// exact float32 vectors in quantized_rerank mode. Zero uses the normalized
	// ef_search candidate set.
	QuantizedRerankCandidates int
	TopK                      int
	EfSearch                  int
	FetchMultiplier           int
	Filter                    func(DocumentRecord) (bool, error)
	IndexRangeFilter          *VectorIndexRangeFilter
	ExactFilterMaxDocs        int
	DisableExactFallback      bool
	// IncludeDocuments materializes documents after column_graph top-k selection.
	IncludeDocuments bool
	// DocumentFetchOptions controls optional projected final-fetch materialization.
	// It is used only when IncludeDocuments is true; the zero value returns full documents.
	DocumentFetchOptions DocumentFetchOptions
	// MaxDecodedBlocks bounds the physical column row reader cache for column_graph search.
	MaxDecodedBlocks int
	// StatsMode selects column_graph search telemetry detail. The zero value
	// preserves full diagnostics; production/minimal mode keeps source-health,
	// fallback, admission, and result counters with lower hot-loop overhead on
	// the healthy combined prepared path.
	StatsMode VectorIndexSearchStatsMode
	// scoreBatchMode is an internal exact-order indexed-scoring test/benchmark hook.
	scoreBatchMode columnVectorGraphScoreBatchMode
}

// VectorIndexTrace reports how one vector-index search was executed.
type VectorIndexTrace struct {
	Strategy                 string
	EfSearch                 int
	FetchMultiplier          int
	CandidatesExamined       int
	CandidatesAfterTombstone int
	CandidatesAfterFilter    int
	RerankCount              int
	ReturnedCount            int
	ExactFallbackReason      string
}

// VectorIndexStats reports process-local in-memory vector index state.
type VectorIndexStats struct {
	Name                string
	Field               string
	Metric              VectorMetric
	Encoding            VectorIndexEncoding
	Dimensions          int
	M                   int
	EfConstruction      int
	EfSearch            int
	Nodes               int
	LiveDocs            int
	DeletedDocs         int
	DeletedRatio        float64
	BytesMemory         int64
	BytesDisk           int64
	AvgDegree           float64
	MaxLevel            int
	Epoch               uint64
	SnapshotDirty       bool
	LastRebuildDuration time.Duration
	RebuildNeeded       bool
	LiveANNFullRebuilds uint64
}

// VectorIndexRecall reports ANN overlap with exact search for sampled queries.
type VectorIndexRecall struct {
	Queries      int
	TopK         int
	ExactTotal   int
	ANNTotal     int
	Overlap      int
	Recall       float64
	SearchTraces []VectorIndexTrace
}

// VectorIndex is the process-local runtime graph for collection vector fields.
// Declared collection vector indexes can persist this runtime graph into a
// TreeDB-managed collection root; ad hoc indexes can still be rebuilt from
// primary collection rows.
type VectorIndex struct {
	collection *Collection

	name                string
	nativeRootName      string
	field               string
	fieldPath           []string
	metric              VectorMetric
	encoding            VectorIndexEncoding
	dimensions          int
	m                   int
	efConstruction      int
	efSearch            int
	rebuildDeletedRatio float64
	schemaGeneration    uint64

	mu                  sync.RWMutex
	nativePublicationMu sync.RWMutex
	nodes               []vectorIndexNode
	currentNode         map[string]int
	entry               int
	maxLevel            int
	insertScratch       vectorIndexSearchScratch
	searchScratch       sync.Pool
	searchView          atomic.Pointer[vectorIndexSearchView]
	searchViewSpare     atomic.Pointer[vectorIndexSearchView]
	searchViewDirty     map[int]struct{}
	// parallelReciprocalLinks is enabled for native runtime indexes and the
	// untraced offline column graph builder. Traced and partition builds stay
	// serial.
	parallelReciprocalLinks bool

	mutationSeq              uint64
	sourceDocumentRootsValid bool
	sourceDocumentGeneration uint64
	sourceDocumentState      backenddb.StateToken
	sourceDocumentStateValid bool
	persistedEpoch           uint64
	fullSnapshotBaseEpoch    uint64
	persistedBytesDisk       int64
	persistedSnapshotDirty   bool
	nativePersistent         bool
	dirtyMeta                bool
	dirtyNodes               map[int]struct{}
	dirtyDocs                map[string]struct{}
	lastRebuildDuration      time.Duration
	liveANNFullRebuilds      uint64
	insertQuantScratch       []int8
	// constructionTrace is an offline-only nullable sink installed by the
	// partition-pack diagnostic builder. Normal collection indexes never set it.
	constructionTrace        *vectorIndexConstructionTraceV1
	layer0ConstructionPolicy *vectorIndexLayer0ConstructionPolicyV1
	// qualityPostfillCandidates belongs to the temporary builder, never to the
	// evidence sink: graph construction must be identical with tracing off.
	qualityPostfillCandidates map[int]map[int]struct{}
}

// vectorIndexLayer0ConstructionPolicyV1 is an offline-only experiment seam.
// It changes the new node's layer-0 selection but leaves reciprocal capacity
// and pruning at the canonical 2M limit.
type vectorIndexLayer0ConstructionPolicyV1 struct {
	initialSelectionFactor int
	backfill               bool
	qualityPostfill        bool
	robustPruneRefinement  bool
}

// vectorIndexConstructionTraceV1 is deliberately private: construction
// provenance is benchmark evidence, not a runtime index contract.
type vectorIndexConstructionTraceV1 struct {
	// detailed is enabled only for predeclared offline diagnostic partitions.
	// Compact traces retain live origins/final survivors but never allocate a
	// historical event per reciprocal maintenance operation.
	detailed                bool
	selections              []vectorIndexConstructionSelectionV1
	events                  []vectorIndexConstructionEdgeEventV1
	pending                 map[vectorIndexConstructionEdgeKeyV1]string
	origins                 map[vectorIndexConstructionEdgeKeyV1]string
	sampleIDs               map[string]struct{}
	nativeInsertionOrdinals []int
	pruneKeeps              uint64
	compactLifecycle        VectorPartitionConstructionCompactLifecycleV1
	postfillEdges           uint64
}
type vectorIndexConstructionSelectionV1 struct {
	Node, Layer, Candidates, Selected, DiversitySelected, BackfillSelected int
	CandidateNodes                                                         []int
	Sampled                                                                bool
}
type vectorIndexConstructionEdgeKeyV1 struct{ From, To, Layer int }
type vectorIndexConstructionEdgeEventV1 struct {
	From, To, Layer, InsertionOrdinal int
	Origin, Action                    string
}

func vectorIndexConstructionOriginIndexV1(origin string) (int, bool) {
	switch origin {
	case "diversity_selected":
		return 0, true
	case "nearest_backfill":
		return 1, true
	case "reciprocal_add":
		return 2, true
	case "reciprocity_repair":
		return 3, true
	case "overlay_rewrite":
		return 4, true
	case "quality_postfill":
		return 5, true
	case "robust_prune_refinement":
		return 6, true
	case "robust_prune_residual_fill":
		return 7, true
	}
	return 0, false
}

func (t *vectorIndexConstructionTraceV1) countLifecycle(origin, action string) {
	originIndex, ok := vectorIndexConstructionOriginIndexV1(origin)
	if !ok {
		return
	}
	switch action {
	case "initial_add":
		t.compactLifecycle.InitialAdd[originIndex]++
	case "reciprocal_add":
		t.compactLifecycle.ReciprocalAdd[originIndex]++
	case "reciprocal_prune_keep":
		t.pruneKeeps++
		t.compactLifecycle.PruneKeep[originIndex]++
	case "reciprocal_prune_drop":
		t.compactLifecycle.PruneDrop[originIndex]++
	case "reciprocity_repair_add", "overlay_rewrite_add":
		t.compactLifecycle.VariantAdd[originIndex]++
	case "reciprocity_repair_drop", "overlay_rewrite_drop":
		t.compactLifecycle.VariantDrop[originIndex]++
	case "robust_prune_add", "robust_prune_residual_fill_add":
		t.compactLifecycle.VariantAdd[originIndex]++
	case "robust_prune_drop":
		t.compactLifecycle.VariantDrop[originIndex]++
	case "quality_postfill_add":
		t.compactLifecycle.QualityPostfillAdd[originIndex]++
	}
}

func (t *vectorIndexConstructionTraceV1) selectEdge(from, to, layer int, origin string) {
	if t == nil {
		return
	}
	t.init()
	t.pending[vectorIndexConstructionEdgeKeyV1{from, to, layer}] = origin
}

func (t *vectorIndexConstructionTraceV1) init() {
	if t.pending == nil {
		t.pending = make(map[vectorIndexConstructionEdgeKeyV1]string)
	}
	if t.origins == nil {
		t.origins = make(map[vectorIndexConstructionEdgeKeyV1]string)
	}
}

func (t *vectorIndexConstructionTraceV1) record(from, to, layer int, origin, action string) {
	if t == nil {
		return
	}
	if action != "final_survivor" {
		t.countLifecycle(origin, action)
	}
	if action == "reciprocal_prune_keep" {
		return
	}
	insertionOrdinal := from
	if to > insertionOrdinal {
		insertionOrdinal = to
	}
	// Variant rewrites are recorded after BFS locality remapping. Translate
	// those persisted ordinals back to causal insertion order; pre-remap build
	// events intentionally retain their native node ordinals directly.
	if len(t.nativeInsertionOrdinals) != 0 && from >= 0 && to >= 0 && from < len(t.nativeInsertionOrdinals) && to < len(t.nativeInsertionOrdinals) {
		insertionOrdinal = t.nativeInsertionOrdinals[from]
		if t.nativeInsertionOrdinals[to] > insertionOrdinal {
			insertionOrdinal = t.nativeInsertionOrdinals[to]
		}
	}
	if !t.detailed && action != "final_survivor" {
		return
	}
	if !t.detailed { // compact final survivors live in origins, not events.
		return
	}
	t.events = append(t.events, vectorIndexConstructionEdgeEventV1{From: from, To: to, Layer: layer, InsertionOrdinal: insertionOrdinal, Origin: origin, Action: action})
}

type vectorIndexNode struct {
	documentID    []byte
	vector        []float32
	quantized     []int8
	quantScale    float32
	normSquared   float64
	cachedInvNorm float32
	level         int
	neighbors     [][]vectorIndexNeighbor
	deleted       bool
}

type vectorIndexNeighbor struct {
	nodeID   int
	distance float32
}

// BuildVectorIndex builds an in-memory vector secondary index from the current
// live collection rows.
func (c *Collection) BuildVectorIndex(opts VectorIndexOptions) (*VectorIndex, error) {
	return c.buildVectorIndex(opts, true)
}

func (c *Collection) buildVectorIndex(opts VectorIndexOptions, register bool) (*VectorIndex, error) {
	return c.buildVectorIndexPrepared(opts, register, true, false)
}

func (c *Collection) buildVectorIndexPrepared(opts VectorIndexOptions, register, flushBuffered, liveANNFullRebuild bool) (*VectorIndex, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	index, err := newVectorIndex(c, opts)
	if err != nil {
		return nil, err
	}
	if flushBuffered {
		if err := c.flushBufferedWrites(); err != nil {
			return nil, err
		}
	}
	sourceDocumentGeneration, err := c.currentVectorIndexDocumentGeneration()
	if err != nil {
		return nil, err
	}
	nativeDef, declared := findVectorIndex(c.meta.VectorIndexes, index.name)
	nativePersistent := declared && vectorIndexDefinitionUsesNativeRuntime(nativeDef)
	var replaceCurrent *VectorIndex
	var replaceMutationSeq uint64
	index.setNativePersistent(nativePersistent)
	if nativePersistent {
		index.recordNativeDefinition(nativeDef)
		baseEpoch, err := c.currentNativeVectorIndexRootID(index.name)
		if err != nil {
			return nil, err
		}
		index.recordFullSnapshotBaseEpoch(baseEpoch)
		if register {
			replaceCurrent = c.registeredVectorIndex(index.name)
			replaceMutationSeq = replaceCurrent.nativeMutationSequence()
		}
	}
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()

	_, err = c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		vector, ok, err := vectorFromStoredDocument(materializer, record.Document, index.fieldPath)
		if err != nil {
			return false, fmt.Errorf("collections: vector field %q in document %q: %w", index.field, record.ID, err)
		}
		if !ok {
			return true, nil
		}
		if err := index.insertVectorLocked(record.ID, vector); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	currentDocumentGeneration, currentDocumentState, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(false)
	if err != nil {
		return nil, err
	}
	if currentDocumentGeneration != sourceDocumentGeneration {
		return nil, ErrConcurrentMutation
	}
	index.recordSourceDocumentStateUnpublished(sourceDocumentGeneration, currentDocumentState)
	if liveANNFullRebuild {
		index.markLiveANNFullRebuild()
	}
	if register {
		if nativePersistent {
			index, err = c.installNativeVectorIndexCandidate(index, index.nativeSnapshotBaseEpochForFullSave(), replaceCurrent, replaceMutationSeq)
			if err != nil {
				return nil, err
			}
		} else {
			c.RegisterVectorIndex(index)
		}
		if c.manager != nil && index.needsNativeAutoPersist() {
			c.manager.registerCollectionHandle(c)
		}
	}
	return index, nil
}

func (c *Collection) currentVectorIndexDocumentGeneration() (uint64, error) {
	return c.currentVectorIndexDocumentGenerationWithWriteDomainLockState(false)
}

func (c *Collection) currentVectorIndexDocumentGenerationWithWriteDomainLocked() (uint64, error) {
	return c.currentVectorIndexDocumentGenerationWithWriteDomainLockState(true)
}

func (c *Collection) currentVectorIndexDocumentGenerationWithWriteDomainLockState(writeDomainLocked bool) (uint64, error) {
	generation, _, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked)
	return generation, err
}

func (c *Collection) currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked bool) (uint64, backenddb.StateToken, error) {
	if c == nil {
		return 0, backenddb.StateToken{}, errCollectionNil
	}
	if c.db == nil {
		return 0, backenddb.StateToken{}, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshotWithWriteDomainLockState(snap, writeDomainLocked)
	if err != nil {
		return 0, backenddb.StateToken{}, err
	}
	if catalog == nil {
		return 0, backenddb.StateToken{}, errCollectionNotFound
	}
	generation, err := vectorIndexDocumentGeneration(snap, catalog)
	if err != nil {
		return 0, backenddb.StateToken{}, err
	}
	state, ok := snap.StateToken()
	if !ok {
		return 0, backenddb.StateToken{}, backenddb.ErrClosed
	}
	return generation, state, nil
}

func vectorIndexDocumentGeneration(snap *backenddb.Snapshot, catalog *collectionCatalog) (uint64, error) {
	if snap == nil || catalog == nil {
		return 0, errCollectionNotFound
	}
	raw, ok, err := getSystemValue(snap, systemCollectionDocumentGenerationKey(catalog.meta.Name))
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return decodeRootID(raw)
}

func newVectorIndex(c *Collection, opts VectorIndexOptions) (*VectorIndex, error) {
	if opts.Name == "" {
		opts.Name = vectorIndexDefaultName(opts.Field)
	}
	fieldPath, err := parseVectorFieldPath(opts.Field)
	if err != nil {
		return nil, err
	}
	metric, err := normalizeVectorMetric(opts.Metric)
	if err != nil {
		return nil, err
	}
	encoding, err := normalizeVectorIndexEncoding(opts.Encoding)
	if err != nil {
		return nil, err
	}
	if opts.Dimensions < 0 {
		return nil, errors.New("collections: vector index dimensions cannot be negative")
	}
	m := opts.M
	if m <= 0 {
		m = defaultVectorIndexM
	}
	efConstruction := opts.EfConstruction
	if efConstruction <= 0 {
		efConstruction = defaultVectorIndexEfConstruction
	}
	if efConstruction < m {
		efConstruction = m
	}
	efSearch := opts.EfSearch
	if efSearch <= 0 {
		efSearch = defaultVectorIndexEfSearch
	}
	rebuildRatio := opts.RebuildDeletedRatio
	if rebuildRatio <= 0 {
		rebuildRatio = float64(defaultVectorIndexRebuildPPM) / 1_000_000
	}
	if rebuildRatio > 1 {
		return nil, errors.New("collections: vector index rebuild deleted ratio cannot exceed 1")
	}
	var nativeRootName string
	if c != nil {
		nativeRootName = collectionVectorIndexRootName(c.collectionName(), opts.Name)
	}
	return &VectorIndex{
		collection:          c,
		name:                opts.Name,
		nativeRootName:      nativeRootName,
		field:               opts.Field,
		fieldPath:           fieldPath,
		metric:              metric,
		encoding:            encoding,
		dimensions:          opts.Dimensions,
		m:                   m,
		efConstruction:      efConstruction,
		efSearch:            efSearch,
		rebuildDeletedRatio: rebuildRatio,
		schemaGeneration:    opts.schemaGeneration,
		currentNode:         make(map[string]int),
		entry:               -1,
		maxLevel:            -1,
	}, nil
}

func normalizeVectorIndexEncoding(encoding VectorIndexEncoding) (VectorIndexEncoding, error) {
	switch encoding {
	case VectorIndexEncodingFloat32, VectorIndexEncodingInt8:
		return encoding, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %d", encoding)
	}
}

func parseVectorIndexEncoding(value string) (VectorIndexEncoding, error) {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "float32":
		return VectorIndexEncodingFloat32, nil
	case "int8":
		return VectorIndexEncodingInt8, nil
	default:
		return VectorIndexEncodingFloat32, fmt.Errorf("collections: unsupported vector index encoding %q", value)
	}
}

// RegisterVectorIndex attaches an in-memory vector index to this collection so
// successful collection inserts, updates, and deletes keep the index in sync.
func (c *Collection) RegisterVectorIndex(index *VectorIndex) {
	if c == nil || index == nil {
		return
	}
	if index.searchView.Load() == nil && index.hasValidSourceDocumentRoots() {
		index.publishSearchView()
	}
	index.collection = c
	if def, ok := findVectorIndex(c.meta.VectorIndexes, index.name); ok && vectorIndexDefinitionUsesNativeRuntime(def) {
		index.recordNativeDefinition(def)
		if c.writeDomain != nil {
			c.writeDomain.nativeVectorIndexesMu.Lock()
			if c.writeDomain.nativeVectorIndexes == nil {
				c.writeDomain.nativeVectorIndexes = make(map[string]*VectorIndex)
			}
			c.writeDomain.nativeVectorIndexes[index.name] = index
			c.writeDomain.nativeVectorIndexesMu.Unlock()
			return
		}
	}
	index.setNativePersistent(false)
	c.vectorIndexesMu.Lock()
	defer c.vectorIndexesMu.Unlock()
	if c.vectorIndexes == nil {
		c.vectorIndexes = make(map[string]*VectorIndex)
	}
	c.vectorIndexes[index.name] = index
}

func (c *Collection) isRegisteredVectorIndex(index *VectorIndex) bool {
	if c == nil || index == nil {
		return false
	}
	return c.registeredVectorIndex(index.name) == index
}

func (c *Collection) vectorIndexRuntimeIsStale(index *VectorIndex) bool {
	if c == nil || index == nil {
		return false
	}
	registered := c.registeredVectorIndex(index.name)
	if registered == index {
		stale, err := c.registeredVectorIndexNativeRuntimeIsStale(index)
		return err == nil && stale
	}
	if registered != nil {
		return true
	}
	if index.isNativePersistent() || collectionMetaDeclaresNativeVectorIndex(c.meta, index.name) {
		return true
	}
	declared, err := c.refreshNativeVectorIndexDeclaration(index.name)
	return err == nil && declared
}

func (c *Collection) registeredVectorIndexNativeRuntimeIsStale(index *VectorIndex) (bool, error) {
	if c == nil || index == nil || c.db == nil {
		return false, nil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil || catalog == nil {
		return false, err
	}
	c.rememberCatalog(snap, catalog)
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, index.name)
	if !ok || !vectorIndexDefinitionUsesNativeRuntime(def) {
		return index.isNativePersistent(), nil
	}
	wasNativePersistent := index.isNativePersistent()
	if reason := index.validateNativeSnapshotDefinition(def); reason != "" {
		if reason == "schema_generation_mismatch" && !wasNativePersistent {
			index.recordNativeDefinition(def)
			rootName := collectionVectorIndexRootName(catalog.meta.Name, index.name)
			return catalog.rootID(rootName) != index.nativeSnapshotBaseEpochForFullSave(), nil
		}
		return true, nil
	}
	index.recordNativeDefinition(def)
	rootName := collectionVectorIndexRootName(catalog.meta.Name, index.name)
	return catalog.rootID(rootName) != index.nativeSnapshotBaseEpochForFullSave(), nil
}

// UnregisterVectorIndex detaches a registered in-memory vector index.
func (c *Collection) UnregisterVectorIndex(name string) {
	if c == nil {
		return
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexesMu.Lock()
		delete(c.writeDomain.nativeVectorIndexes, name)
		c.writeDomain.nativeVectorIndexesMu.Unlock()
	}
	c.vectorIndexesMu.Lock()
	delete(c.vectorIndexes, name)
	c.vectorIndexesMu.Unlock()
	if !c.hasNativePersistentVectorIndex() && c.manager != nil && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
}

func (c *Collection) hasNativePersistentVectorIndex() bool {
	for _, index := range c.registeredVectorIndexes() {
		if index != nil && index.isNativePersistent() {
			return true
		}
	}
	return false
}

func (c *Collection) registeredVectorIndexes() []*VectorIndex {
	if c == nil {
		return nil
	}
	var out []*VectorIndex
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexesMu.RLock()
		out = make([]*VectorIndex, 0, len(c.writeDomain.nativeVectorIndexes))
		for _, index := range c.writeDomain.nativeVectorIndexes {
			out = append(out, index)
		}
		c.writeDomain.nativeVectorIndexesMu.RUnlock()
	}
	c.vectorIndexesMu.RLock()
	if len(c.vectorIndexes) == 0 {
		c.vectorIndexesMu.RUnlock()
		return out
	}
	sharedNames := make(map[string]struct{}, len(out))
	for _, index := range out {
		sharedNames[index.name] = struct{}{}
	}
	for name, index := range c.vectorIndexes {
		if _, shared := sharedNames[name]; !shared {
			out = append(out, index)
		}
	}
	c.vectorIndexesMu.RUnlock()
	return out
}

func (c *Collection) hasRegisteredVectorIndex(name string) bool {
	return c != nil && name != "" && c.registeredVectorIndex(name) != nil
}

func (c *Collection) lockNativeVectorIndexLoad() func() {
	if c != nil && c.writeDomain != nil {
		c.writeDomain.nativeVectorIndexLoadMu.Lock()
		return c.writeDomain.nativeVectorIndexLoadMu.Unlock
	}
	c.vectorIndexLoadMu.Lock()
	return c.vectorIndexLoadMu.Unlock
}

func (idx *VectorIndex) nativePublicationLock() *sync.RWMutex {
	if idx != nil && idx.collection != nil && idx.collection.writeDomain != nil {
		return &idx.collection.writeDomain.nativeVectorPublishMu
	}
	return &idx.nativePublicationMu
}

func (c *Collection) ensureDeclaredNativeVectorIndexesLoaded() (map[string]struct{}, error) {
	if c == nil {
		return nil, nil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if len(c.registeredVectorIndexes()) == 0 {
		commitSeq, systemRoot := dbCommitSeqAndSystemRoot(c.db)
		c.catalogMu.RLock()
		catalogHasNoVectorIndexes := c.catalog != nil &&
			c.catalogCommitSeq == commitSeq &&
			c.catalogSystemRoot == systemRoot &&
			len(c.catalog.meta.VectorIndexes) == 0
		c.catalogMu.RUnlock()
		if catalogHasNoVectorIndexes {
			return nil, nil
		}
	}
	if c.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		return nil, nil
	}
	unlockLoad := c.lockNativeVectorIndexLoad()
	defer unlockLoad()
	if c.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		return nil, nil
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
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	_ = snap.Close()

	declared := make(map[string]VectorIndexDefinition, len(c.meta.VectorIndexes))
	for _, def := range c.meta.VectorIndexes {
		if vectorIndexDefinitionUsesNativeRuntime(def) {
			declared[def.Name] = def
		}
	}
	for _, index := range c.registeredVectorIndexes() {
		def, ok := declared[index.name]
		if !ok {
			if index.isNativePersistent() {
				c.UnregisterVectorIndex(index.name)
			}
			continue
		}
		if index.validateNativeSnapshotDefinition(def) != "" {
			c.UnregisterVectorIndex(index.name)
		}
	}
	var rebuilt map[string]struct{}
	for _, def := range c.meta.VectorIndexes {
		if !vectorIndexDefinitionUsesNativeRuntime(def) {
			continue
		}
		if index := c.registeredVectorIndex(def.Name); index != nil {
			if index.validateNativeSnapshotDefinition(def) == "" && index.hasValidSourceDocumentRoots() {
				index.setNativePersistent(true)
				continue
			}
			if !index.hasValidSourceDocumentRoots() {
				_, err := c.buildVectorIndexPrepared(vectorIndexOptionsFromDefinition(def), true, true, true)
				if err != nil {
					return nil, err
				}
				if rebuilt == nil {
					rebuilt = make(map[string]struct{}, 1)
				}
				rebuilt[def.Name] = struct{}{}
				continue
			}
		}
		index, status, err := c.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		if err != nil {
			return nil, err
		}
		if index != nil {
			continue
		}
		if status.ExactFallbackReason != "" {
			_, err := c.buildVectorIndexPrepared(vectorIndexOptionsFromDefinition(def), true, true, true)
			if err != nil {
				return nil, err
			}
			if rebuilt == nil {
				rebuilt = make(map[string]struct{}, 1)
			}
			rebuilt[def.Name] = struct{}{}
		}
	}
	return rebuilt, nil
}

func (c *Collection) declaredNativeVectorIndexesLoadedForCurrentCatalog() bool {
	if c == nil || c.db == nil {
		return false
	}
	commitSeq, systemRoot := dbCommitSeqAndSystemRoot(c.db)
	c.catalogMu.RLock()
	catalogCurrent := c.catalog != nil && c.catalogCommitSeq == commitSeq && c.catalogSystemRoot == systemRoot
	var defs []VectorIndexDefinition
	if catalogCurrent {
		defs = append([]VectorIndexDefinition(nil), c.catalog.meta.VectorIndexes...)
	}
	c.catalogMu.RUnlock()
	if !catalogCurrent {
		return false
	}
	nativeDefs := make([]VectorIndexDefinition, 0, len(defs))
	for _, def := range defs {
		if vectorIndexDefinitionUsesNativeRuntime(def) {
			nativeDefs = append(nativeDefs, def)
		}
	}
	if len(nativeDefs) == 0 {
		for _, index := range c.registeredVectorIndexes() {
			if index.isNativePersistent() {
				return false
			}
		}
		return true
	}
	declared := make(map[string]struct{}, len(nativeDefs))
	for _, def := range nativeDefs {
		declared[def.Name] = struct{}{}
		index := c.registeredVectorIndex(def.Name)
		if index == nil || !index.isNativePersistent() || !index.hasValidSourceDocumentRoots() || index.validateNativeSnapshotDefinition(def) != "" {
			return false
		}
	}
	for _, index := range c.registeredVectorIndexes() {
		if !index.isNativePersistent() {
			continue
		}
		if _, ok := declared[index.name]; !ok {
			return false
		}
	}
	return true
}

func vectorIndexDefinitionUsesNativeRuntime(def VectorIndexDefinition) bool {
	return def.Strategy == "" || def.Strategy == VectorIndexStrategyNativeRuntime
}

func (c *Collection) notifyVectorIndexesUpsert(documentIDs [][]byte) error {
	return c.reconcileVectorIndexes(documentIDs)
}

func (c *Collection) notifyVectorIndexesDelete(documentIDs [][]byte) error {
	return c.reconcileVectorIndexes(documentIDs)
}

func (c *Collection) reconcileVectorIndexes(documentIDs [][]byte) error {
	if len(documentIDs) == 0 {
		return nil
	}
	unlockMutation := c.lockVectorIndexMutation()
	defer unlockMutation()
	rebuilt, err := c.ensureDeclaredNativeVectorIndexesLoaded()
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverage()
		return err
	}
	unlockPublication := c.lockNativeVectorIndexPublicationRead()
	defer unlockPublication()
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil
	}
	if c.manager != nil && vectorIndexListHasNativePersistent(indexes) {
		c.manager.registerCollectionHandle(c)
	}
	needsInsert := false
	for _, index := range indexes {
		if _, ok := rebuilt[index.name]; !ok {
			needsInsert = true
			break
		}
	}
	if !needsInsert {
		return c.recordReconciledVectorIndexCoverage(indexes)
	}
	cachedPrimaryRead := c.snapshotVectorIndexPrimaryCache(documentIDs)
	defer putUpdateBatchBufferedEntries(cachedPrimaryRead.primaryEntries, cachedPrimaryRead.primaryBuffer)
	materializer, err := c.NewStoredDocumentJSONMaterializer()
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return err
	}
	defer func() { _ = materializer.Close() }()
	for _, index := range indexes {
		if _, ok := rebuilt[index.name]; ok {
			continue
		}
		for documentIndex, documentID := range documentIDs {
			if len(documentID) == 0 {
				continue
			}
			var document []byte
			if cachedPrimaryRead.enabled && cachedPrimaryRead.primaryEntries[documentIndex].found {
				entry := cachedPrimaryRead.primaryEntries[documentIndex]
				if entry.flags&node.FlagTombstone == 0 {
					document = entry.value
				}
			} else {
				document, err = c.Get(documentID)
				if err != nil {
					c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
					return err
				}
			}
			if err := index.insertStoredDocumentUnpublished(materializer, documentID, document); err != nil {
				c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
				return err
			}
		}
	}
	return c.recordReconciledVectorIndexCoverage(indexes)
}

func (c *Collection) snapshotVectorIndexPrimaryCache(documentIDs [][]byte) updateBatchBufferedRead {
	if c == nil || c.db == nil || len(documentIDs) == 0 {
		return updateBatchBufferedRead{}
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return updateBatchBufferedRead{}
	}
	defer func() { _ = snap.Close() }()
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil || catalog == nil {
		return updateBatchBufferedRead{}
	}
	items := make([]updateBatchItem, len(documentIDs))
	for i, documentID := range documentIDs {
		items[i].DocumentID = documentID
	}
	return snapshotUpdateBatchPrimaryCache(c.writeDomain, catalog.meta, snapshotSystemRoot(snap), items)
}

func (c *Collection) invalidateRegisteredVectorIndexDocumentCoverage() {
	unlockPublication := c.lockNativeVectorIndexPublicationRead()
	defer unlockPublication()
	c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
}

func (c *Collection) invalidateRegisteredVectorIndexDocumentCoverageLocked() {
	for _, index := range c.registeredVectorIndexes() {
		index.invalidateSourceDocumentRoots()
	}
}

func (c *Collection) invalidateVectorIndexCoverageOnAcceptedMutation(err error) error {
	if backenddb.CommitPublicationAccepted(err) {
		c.invalidateRegisteredVectorIndexDocumentCoverage()
	}
	return err
}

func (c *Collection) lockVectorIndexMutation() func() {
	if c == nil {
		return func() {}
	}
	if c.writeDomain != nil {
		c.writeDomain.nativeVectorMutationMu.Lock()
		return c.writeDomain.nativeVectorMutationMu.Unlock
	}
	c.vectorIndexMutationMu.Lock()
	return c.vectorIndexMutationMu.Unlock
}

func (c *Collection) lockVectorIndexCoverageMutation() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	domain := c.writeDomain
	domain.nativeVectorAdmissionMu.RLock()
	domain.mu.RLock()
	hasMaintainedVectorIndexes := false
	for _, def := range domain.meta.VectorIndexes {
		if vectorIndexDefinitionUsesNativeRuntime(def) {
			hasMaintainedVectorIndexes = true
			break
		}
	}
	domain.mu.RUnlock()
	domain.nativeVectorIndexesMu.RLock()
	hasMaintainedVectorIndexes = hasMaintainedVectorIndexes || len(domain.nativeVectorIndexes) != 0
	domain.nativeVectorIndexesMu.RUnlock()
	exclusiveAdmission := hasMaintainedVectorIndexes
	if exclusiveAdmission {
		domain.nativeVectorAdmissionMu.RUnlock()
		domain.nativeVectorAdmissionMu.Lock()
	}
	domain.nativeVectorCoverageMu.RLock()
	domain.nativeVectorActiveMu.Lock()
	domain.nativeVectorActive++
	domain.nativeVectorSearchActive.Store(true)
	domain.nativeVectorActiveMu.Unlock()
	return func() {
		domain.mu.RLock()
		domain.nativeVectorActiveMu.Lock()
		domain.nativeVectorActive--
		if domain.nativeVectorActive == 0 {
			indexes := c.registeredVectorIndexes()
			if len(indexes) != 0 {
				if generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(true); err == nil {
					for _, index := range indexes {
						if index.hasValidSourceDocumentRoots() {
							index.recordSourceDocumentStateAndPublishIfChanged(generation, state)
						}
					}
				}
			}
			domain.nativeVectorSearchActive.Store(false)
		}
		domain.nativeVectorCoverageMu.RUnlock()
		domain.nativeVectorActiveMu.Unlock()
		domain.mu.RUnlock()
		if exclusiveAdmission {
			domain.nativeVectorAdmissionMu.Unlock()
		} else {
			domain.nativeVectorAdmissionMu.RUnlock()
		}
	}
}

func (c *Collection) lockVectorIndexCoveragePersistence() func() {
	if c == nil || c.writeDomain == nil {
		return func() {}
	}
	c.writeDomain.nativeVectorCoverageMu.Lock()
	return c.writeDomain.nativeVectorCoverageMu.Unlock
}

func (c *Collection) recordReconciledVectorIndexCoverage(indexes []*VectorIndex) error {
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, false)
}

func (c *Collection) recordReconciledVectorIndexCoverageWithWriteDomainLocked(indexes []*VectorIndex) error {
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, true)
}

func (c *Collection) recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes []*VectorIndex, writeDomainLocked bool) error {
	if c == nil {
		return errCollectionNil
	}
	if c.writeDomain == nil {
		generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(writeDomainLocked)
		if err != nil {
			c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
			return err
		}
		for _, index := range indexes {
			if c.isRegisteredVectorIndex(index) {
				index.recordSourceDocumentStateAndPublish(generation, state)
			}
		}
		return nil
	}

	domain := c.writeDomain
	if !writeDomainLocked {
		domain.mu.RLock()
		defer domain.mu.RUnlock()
	}
	domain.nativeVectorActiveMu.Lock()
	defer domain.nativeVectorActiveMu.Unlock()
	if domain.nativeVectorActive != 0 {
		return nil
	}
	domain.nativeVectorSearchActive.Store(true)
	defer domain.nativeVectorSearchActive.Store(false)
	generation, state, err := c.currentVectorIndexDocumentStateWithWriteDomainLockState(true)
	if err != nil {
		c.invalidateRegisteredVectorIndexDocumentCoverageLocked()
		return err
	}
	for _, index := range indexes {
		if c.isRegisteredVectorIndex(index) && index.hasValidSourceDocumentRoots() {
			index.recordSourceDocumentStateAndPublish(generation, state)
		}
	}
	return nil
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublish() error {
	return c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(false)
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLocked() error {
	return c.recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(true)
}

func (c *Collection) recordVectorIndexCoverageAfterBufferedDocumentPublishWithWriteDomainLockState(writeDomainLocked bool) error {
	if c == nil || c.writeDomain == nil {
		return nil
	}
	indexes := c.registeredVectorIndexes()
	if len(indexes) == 0 {
		return nil
	}
	domain := c.writeDomain
	domain.nativeVectorActiveMu.Lock()
	active := domain.nativeVectorActive != 0
	domain.nativeVectorActiveMu.Unlock()
	if active {
		return nil
	}

	if !domain.nativeVectorCoverageMu.TryLock() {
		// The holder is either a public mutation, whose final unlock certifies
		// coverage, or persistence, whose locked flush does the same.
		return nil
	}
	defer domain.nativeVectorCoverageMu.Unlock()
	domain.nativeVectorActiveMu.Lock()
	active = domain.nativeVectorActive != 0
	domain.nativeVectorActiveMu.Unlock()
	if active {
		return nil
	}
	return c.recordReconciledVectorIndexCoverageWithWriteDomainLockState(indexes, writeDomainLocked)
}

func (c *Collection) lockNativeVectorIndexPublicationRead() func() {
	if c != nil && c.writeDomain != nil {
		c.writeDomain.nativeVectorPublishMu.RLock()
		return c.writeDomain.nativeVectorPublishMu.RUnlock
	}
	return func() {}
}

func vectorIndexListHasNativePersistent(indexes []*VectorIndex) bool {
	for _, index := range indexes {
		if index != nil && index.isNativePersistent() {
			return true
		}
	}
	return false
}

func (c *Collection) notifyVectorIndexesUpdateBatch(items []UpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) notifyVectorIndexesBSONSetUpdateBatch(items []BSONSetUpdateBatchItem, results []UpdateBatchResult) error {
	if len(items) == 0 || len(results) == 0 {
		return nil
	}
	var updated [][]byte
	for i := range items {
		if i >= len(results) {
			break
		}
		if results[i].Modified {
			updated = append(updated, items[i].DocumentID)
		}
	}
	return c.notifyVectorIndexesUpsert(updated)
}

func (c *Collection) persistNativeVectorIndexIfDeclared(index *VectorIndex) error {
	if c == nil || index == nil {
		return nil
	}
	if index.isNativePersistent() {
		runNativeVectorIndexBeforeAutoPersistSaveHookForTest(index.name)
		_, err := index.SaveNativeDeltaSnapshot()
		if errors.Is(err, errVectorIndexNotDeclared) {
			return nil
		}
		return err
	}
	if !index.needsNativeAutoPersist() {
		return nil
	}
	if !collectionMetaDeclaresNativeVectorIndex(c.meta, index.name) {
		declared, err := c.refreshNativeVectorIndexDeclaration(index.name)
		if err != nil || !declared {
			return err
		}
	}
	_, err := index.SaveNativeSnapshot()
	if errors.Is(err, errVectorIndexNotDeclared) {
		return nil
	}
	return err
}

func (c *Collection) refreshNativeVectorIndexDeclaration(name string) (bool, error) {
	if c == nil {
		return false, errCollectionNil
	}
	if c.db == nil {
		return false, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return false, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, c.meta.Name)
	if err != nil || catalog == nil {
		return false, err
	}
	c.meta = catalog.meta
	c.rememberCatalog(snap, catalog)
	return collectionMetaDeclaresNativeVectorIndex(catalog.meta, name), nil
}

func (c *Collection) persistDirtyNativeVectorIndexes() error {
	indexes := c.registeredVectorIndexes()
	for _, index := range indexes {
		if err := c.persistNativeVectorIndexIfDeclared(index); err != nil {
			return err
		}
	}
	if c.manager != nil && !c.hasDirtyNativeVectorIndex() && !c.hasCollectionVectorIndexPreparedSearchCacheEntries() && !c.hasCollectionTypedColumnOneShotCacheEntries() && !c.hasCollectionQueryReadyGenerationCache() {
		c.manager.unregisterCollectionHandle(c)
	}
	return nil
}

func (c *Collection) hasDirtyNativeVectorIndex() bool {
	for _, index := range c.registeredVectorIndexes() {
		if index == nil || (!index.isNativePersistent() && !collectionMetaDeclaresNativeVectorIndex(c.meta, index.name)) {
			continue
		}
		if index.needsNativeAutoPersist() {
			return true
		}
	}
	return false
}

func collectionMetaDeclaresNativeVectorIndex(meta CollectionMeta, name string) bool {
	if name == "" {
		return false
	}
	def, ok := findVectorIndex(meta.VectorIndexes, name)
	return ok && vectorIndexDefinitionUsesNativeRuntime(def)
}

// InsertDocument adds or replaces one committed collection document in the
// in-memory index. Missing or null vector fields leave the document unindexed
// and tombstone any previous indexed version.
func (idx *VectorIndex) InsertDocument(documentID []byte) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	document, err := idx.collection.Get(documentID)
	if err != nil {
		return err
	}
	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return err
	}
	defer func() { _ = materializer.Close() }()
	return idx.insertStoredDocument(materializer, documentID, document)
}

func (idx *VectorIndex) insertStoredDocument(materializer *StoredDocumentJSONMaterializer, documentID, document []byte) error {
	return idx.insertStoredDocumentWithPublication(materializer, documentID, document, true)
}

func (idx *VectorIndex) insertStoredDocumentUnpublished(materializer *StoredDocumentJSONMaterializer, documentID, document []byte) error {
	return idx.insertStoredDocumentWithPublication(materializer, documentID, document, false)
}

func (idx *VectorIndex) insertStoredDocumentWithPublication(materializer *StoredDocumentJSONMaterializer, documentID, document []byte, publish bool) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if document == nil {
		idx.mu.Lock()
		idx.tombstoneDocumentIDLocked(documentID)
		if publish {
			idx.publishSearchViewLocked(false)
		}
		idx.mu.Unlock()
		return nil
	}
	vector, ok, err := vectorFromStoredDocument(materializer, document, idx.fieldPath)
	if err != nil {
		return fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, documentID, err)
	}
	if !ok {
		idx.mu.Lock()
		idx.tombstoneDocumentIDLocked(documentID)
		if publish {
			idx.publishSearchViewLocked(false)
		}
		idx.mu.Unlock()
		return nil
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.insertVectorLocked(documentID, vector); err != nil {
		return err
	}
	if publish {
		idx.publishSearchViewLocked(false)
	}
	return nil
}

// TombstoneDocumentID marks the current indexed version of documentID deleted.
// Tombstoned nodes remain in the graph until the caller rebuilds the index.
func (idx *VectorIndex) TombstoneDocumentID(documentID []byte) {
	if idx == nil || len(documentID) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tombstoneDocumentIDLocked(documentID)
	idx.publishSearchViewLocked(false)
}

func (idx *VectorIndex) insertVectorLocked(documentID []byte, vector []float32) error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	if len(documentID) == 0 {
		return errors.New("collections: document id cannot be empty")
	}
	if len(vector) == 0 {
		return errors.New("collections: vector cannot be empty")
	}
	if err := validateFloat32Vector(vector); err != nil {
		return err
	}
	vectorNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		vectorNorm = vectorNormSquared(vector)
		if vectorNorm == 0 {
			return errors.New("collections: cosine vector cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(vector, vectorNorm)
		if err != nil {
			return err
		}
		prepared = &preparedQuery
	}
	if idx.dimensions == 0 {
		idx.dimensions = len(vector)
		idx.markVectorMetaDirtyLocked()
	} else if len(vector) != idx.dimensions {
		return fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", idx.field, documentID, len(vector), idx.dimensions)
	}
	var quantized []int8
	var quantScale float32
	if idx.encoding == VectorIndexEncodingInt8 {
		idx.insertQuantScratch, quantScale = quantizeVectorIndexInt8Into(idx.insertQuantScratch[:0], vector)
		quantized = idx.insertQuantScratch
	}
	if nodeID, ok := idx.currentNode[string(documentID)]; ok && nodeID >= 0 && nodeID < len(idx.nodes) {
		node := &idx.nodes[nodeID]
		if !node.deleted {
			switch idx.encoding {
			case VectorIndexEncodingInt8:
				if node.matchesQuantizedVector(quantized, quantScale) {
					return nil
				}
			default:
				if node.matchesVector(vector) {
					return nil
				}
			}
		}
	}
	idx.tombstoneDocumentIDLocked(documentID)

	nodeID := len(idx.nodes)
	level := idx.levelForDocumentID(documentID)
	if idx.encoding == VectorIndexEncodingInt8 {
		quantized = append([]int8(nil), quantized...)
	}
	node := idx.newVectorIndexNodePrepared(documentID, vector, level, quantized, quantScale)
	idx.nodes = append(idx.nodes, node)
	idx.markGraphChangedLocked()
	idx.markVectorNodeDirtyLocked(nodeID)
	idx.markVectorDocDirtyLocked(documentID)
	idx.currentNode[string(documentID)] = nodeID
	if idx.entry < 0 {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
		return nil
	}
	entryPoint := idx.entry
	for layer := idx.maxLevel; layer > level; layer-- {
		entryPoint = idx.greedyNearestAtLayerLocked(vector, vectorNorm, prepared, entryPoint, layer)
	}
	for layer := minInt(level, idx.maxLevel); layer >= 0; layer-- {
		candidates := idx.searchLayerWithScratchLocked(vector, vectorNorm, prepared, entryPoint, idx.efConstruction, layer, &idx.insertScratch)
		selectionLimit := idx.maxNeighborsForLayer(layer)
		if layer == 0 && idx.layer0ConstructionPolicy != nil {
			selectionLimit = idx.m * idx.layer0ConstructionPolicy.initialSelectionFactor
		}
		neighbors := idx.selectLayerNeighborsLocked(vector, vectorNorm, prepared, candidates, layer, selectionLimit, nodeID)
		idx.linkSelectedNeighborsLocked(nodeID, neighbors, layer)
		if len(neighbors) > 0 {
			entryPoint = neighbors[0]
		}
	}
	if level > idx.maxLevel {
		idx.entry = nodeID
		idx.maxLevel = level
		idx.markVectorMetaDirtyLocked()
	}
	return nil
}

func (idx *VectorIndex) linkSelectedNeighborsLocked(nodeID int, neighbors []int, layer int) {
	workers := 1
	if idx.parallelReciprocalLinks &&
		idx.constructionTrace == nil &&
		idx.layer0ConstructionPolicy == nil &&
		idx.qualityPostfillCandidates == nil &&
		vectorIndexNeighborIDsDistinct(neighbors) {
		workers = vectorIndexReciprocalLinkWorkerCount(len(neighbors))
	}
	if workers == 1 {
		for _, neighborID := range neighbors {
			// selectLayerNeighborsLocked records the exact selection origin before
			// this directed initial link is made.
			idx.linkLayerLocked(nodeID, neighborID, layer, true)
			idx.linkLayerLocked(neighborID, nodeID, layer, true)
		}
		return
	}

	// Keep the new node's ordered outgoing edges serial. Each worker below owns
	// a distinct old node's adjacency slice and only reads immutable node data.
	for _, neighborID := range neighbors {
		idx.linkLayerLocked(nodeID, neighborID, layer, true)
	}
	var wg sync.WaitGroup
	for worker := 1; worker < workers; worker++ {
		worker := worker
		wg.Go(func() {
			for neighbor := worker; neighbor < len(neighbors); neighbor += workers {
				idx.linkLayerLocked(neighbors[neighbor], nodeID, layer, false)
			}
		})
	}
	for neighbor := 0; neighbor < len(neighbors); neighbor += workers {
		idx.linkLayerLocked(neighbors[neighbor], nodeID, layer, false)
	}
	wg.Wait()
	for _, neighborID := range neighbors {
		idx.markVectorNodeDirtyLocked(neighborID)
	}
}

func vectorIndexReciprocalLinkWorkerCount(neighbors int) int {
	if neighbors < minVectorIndexParallelReciprocalNeighbors {
		return 1
	}
	return minInt(runtime.GOMAXPROCS(0), neighbors)
}

func vectorIndexNeighborIDsDistinct(neighbors []int) bool {
	for i := range neighbors {
		for j := 0; j < i; j++ {
			if neighbors[i] == neighbors[j] {
				return false
			}
		}
	}
	return true
}

func (node *vectorIndexNode) matchesVector(vector []float32) bool {
	if node == nil {
		return false
	}
	return slices.Equal(node.vector, vector)
}

func (node *vectorIndexNode) matchesQuantizedVector(quantized []int8, quantScale float32) bool {
	if node == nil {
		return false
	}
	return node.quantScale == quantScale && slices.Equal(node.quantized, quantized)
}

func (node *vectorIndexNode) matchesInt8SourceVector(vector []float32) bool {
	if node == nil || len(node.quantized) != len(vector) {
		return false
	}
	scale := vectorIndexInt8Scale(vector)
	if node.quantScale != scale {
		return false
	}
	for i, value := range vector {
		if node.quantized[i] != quantizeVectorIndexInt8Value(value, scale) {
			return false
		}
	}
	return true
}

func (idx *VectorIndex) newVectorIndexNode(documentID []byte, vector []float32, level int) vectorIndexNode {
	var quantized []int8
	var quantScale float32
	if idx.encoding == VectorIndexEncodingInt8 {
		quantized, quantScale = quantizeVectorIndexInt8(vector)
	}
	return idx.newVectorIndexNodePrepared(documentID, vector, level, quantized, quantScale)
}

func (idx *VectorIndex) newVectorIndexNodePrepared(documentID []byte, vector []float32, level int, quantized []int8, quantScale float32) vectorIndexNode {
	node := vectorIndexNode{
		documentID: bytes.Clone(documentID),
		level:      level,
		neighbors:  make([][]vectorIndexNeighbor, level+1),
	}
	for layer := range node.neighbors {
		node.neighbors[layer] = make([]vectorIndexNeighbor, 0, idx.initialNeighborCapacityForLayer(layer))
	}
	switch idx.encoding {
	case VectorIndexEncodingInt8:
		node.quantized = quantized
		node.quantScale = quantScale
	default:
		node.vector = append([]float32(nil), vector...)
	}
	node.cacheVectorNorms()
	return node
}

func quantizeVectorIndexInt8(vector []float32) ([]int8, float32) {
	out := make([]int8, 0, len(vector))
	return quantizeVectorIndexInt8Into(out, vector)
}

func quantizeVectorIndexInt8Into(dst []int8, vector []float32) ([]int8, float32) {
	scale := vectorIndexInt8Scale(vector)
	if cap(dst) < len(vector) {
		dst = make([]int8, len(vector))
	} else {
		dst = dst[:len(vector)]
	}
	for i, value := range vector {
		dst[i] = quantizeVectorIndexInt8Value(value, scale)
	}
	return dst, scale
}

func vectorIndexInt8Scale(vector []float32) float32 {
	var maxAbs float32
	for _, value := range vector {
		abs := value
		if abs < 0 {
			abs = -abs
		}
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	scale := maxAbs / 127
	if scale == 0 {
		scale = 1
	}
	return scale
}

func quantizeVectorIndexInt8Value(value, scale float32) int8 {
	q := int(math.Round(float64(value / scale)))
	if q > 127 {
		q = 127
	} else if q < -127 {
		q = -127
	}
	return int8(q)
}

func (idx *VectorIndex) tombstoneDocumentIDLocked(documentID []byte) {
	nodeID, ok := idx.currentNode[string(documentID)]
	if !ok {
		return
	}
	if nodeID >= 0 && nodeID < len(idx.nodes) {
		idx.nodes[nodeID].deleted = true
		idx.markVectorNodeDirtyLocked(nodeID)
	}
	delete(idx.currentNode, string(documentID))
	idx.markVectorDocDirtyLocked(documentID)
	idx.markGraphChangedLocked()
	if idx.entry == nodeID {
		idx.entry = idx.firstLiveNodeLocked()
		idx.maxLevel = idx.maxLiveLevelLocked()
		if idx.entry >= 0 {
			idx.entry = idx.firstLiveNodeAtLevelLocked(idx.maxLevel)
		}
		idx.markVectorMetaDirtyLocked()
	}
}

func (idx *VectorIndex) firstLiveNodeLocked() int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted {
			return i
		}
	}
	return -1
}

func (idx *VectorIndex) maxLiveLevelLocked() int {
	maxLevel := -1
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level > maxLevel {
			maxLevel = idx.nodes[i].level
		}
	}
	return maxLevel
}

func (idx *VectorIndex) firstLiveNodeAtLevelLocked(level int) int {
	for i := range idx.nodes {
		if !idx.nodes[i].deleted && idx.nodes[i].level >= level {
			return i
		}
	}
	return -1
}

func (idx *VectorIndex) levelForDocumentID(documentID []byte) int {
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], xxhash.Sum64(documentID)^xxhash.Sum64String(idx.name))
	hash := xxhash.Sum64(seed[:])
	promotionBase := idx.m
	if promotionBase < 2 {
		promotionBase = 2
	}
	if hash == 0 {
		return 32
	}
	u := (float64(hash>>11) + 1) / (float64(uint64(1)<<53) + 1)
	level := int(-math.Log(u) / math.Log(float64(promotionBase)))
	if level > 32 {
		return 32
	}
	return level
}

func (idx *VectorIndex) markGraphChangedLocked() {
	idx.mutationSeq++
	if idx.persistedEpoch != 0 {
		idx.persistedSnapshotDirty = true
	}
}

func (idx *VectorIndex) requireFullNativeSnapshotLocked() {
	if !idx.nativePersistent {
		return
	}
	if idx.persistedEpoch != 0 {
		idx.fullSnapshotBaseEpoch = idx.persistedEpoch
	}
	idx.persistedEpoch = 0
	idx.persistedBytesDisk = 0
	idx.persistedSnapshotDirty = true
	idx.dirtyMeta = false
	clear(idx.dirtyNodes)
	clear(idx.dirtyDocs)
}

func (idx *VectorIndex) setNativePersistent(enabled bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.nativePersistent = enabled
	idx.parallelReciprocalLinks = enabled
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordNativeDefinition(def VectorIndexDefinition) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.nativePersistent = true
	idx.parallelReciprocalLinks = true
	idx.schemaGeneration = def.SchemaGeneration
	idx.mu.Unlock()
}

func (idx *VectorIndex) isNativePersistent() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nativePersistent
}

func (idx *VectorIndex) nativeSnapshotBaseEpochForFullSave() uint64 {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.persistedEpoch != 0 {
		return idx.persistedEpoch
	}
	return idx.fullSnapshotBaseEpoch
}

func (idx *VectorIndex) recordFullSnapshotBaseEpoch(epoch uint64) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.fullSnapshotBaseEpoch = epoch
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordSourceDocumentGeneration(generation uint64) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	wasValid := idx.sourceDocumentRootsValid
	changed := !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != generation
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = generation
	idx.sourceDocumentStateValid = false
	if changed && idx.nativePersistent {
		idx.dirtyMeta = true
	}
	if !wasValid {
		idx.publishSearchViewLocked(false)
	}
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordSourceDocumentState(generation uint64, state backenddb.StateToken) {
	idx.recordSourceDocumentStateWithPublication(generation, state, true)
}

func (idx *VectorIndex) recordSourceDocumentStateUnpublished(generation uint64, state backenddb.StateToken) {
	idx.recordSourceDocumentStateWithPublication(generation, state, false)
}

func (idx *VectorIndex) recordSourceDocumentStateWithPublication(generation uint64, state backenddb.StateToken, publish bool) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	wasValid := idx.recordSourceDocumentStateLocked(generation, state)
	if publish && !wasValid {
		idx.publishSearchViewLocked(false)
	}
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordSourceDocumentStateAndPublish(generation uint64, state backenddb.StateToken) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.recordSourceDocumentStateLocked(generation, state)
	idx.publishSearchViewLocked(false)
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordSourceDocumentStateAndPublishIfChanged(generation uint64, state backenddb.StateToken) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	view := idx.searchView.Load()
	publish := !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != generation ||
		view == nil || len(view.nodes) != len(idx.nodes) || len(idx.searchViewDirty) != 0
	idx.recordSourceDocumentStateLocked(generation, state)
	if publish {
		idx.publishSearchViewLocked(false)
	}
	idx.mu.Unlock()
}

func (idx *VectorIndex) recordSourceDocumentStateLocked(generation uint64, state backenddb.StateToken) bool {
	wasValid := idx.sourceDocumentRootsValid
	changed := !idx.sourceDocumentRootsValid || idx.sourceDocumentGeneration != generation
	idx.sourceDocumentRootsValid = true
	idx.sourceDocumentGeneration = generation
	idx.sourceDocumentState = state
	idx.sourceDocumentStateValid = true
	if changed && idx.nativePersistent {
		idx.dirtyMeta = true
	}
	return wasValid
}

func (idx *VectorIndex) invalidateSourceDocumentRoots() {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.sourceDocumentRootsValid = false
	idx.sourceDocumentStateValid = false
	idx.publishSearchViewLocked(false)
	idx.mu.Unlock()
}

func (idx *VectorIndex) hasValidSourceDocumentRoots() bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	valid := idx.sourceDocumentRootsValid
	idx.mu.RUnlock()
	return valid
}

func (idx *VectorIndex) coversSourceDocumentGeneration(generation uint64) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && idx.sourceDocumentGeneration == generation
	idx.mu.RUnlock()
	return covers
}

func (idx *VectorIndex) coversSourceDocumentState(state backenddb.StateToken) bool {
	if idx == nil {
		return false
	}
	idx.mu.RLock()
	covers := idx.sourceDocumentRootsValid && idx.sourceDocumentStateValid && idx.sourceDocumentState == state
	idx.mu.RUnlock()
	return covers
}

func (idx *VectorIndex) sourceDocumentCoverage() (uint64, bool) {
	if idx == nil {
		return 0, false
	}
	idx.mu.RLock()
	generation, valid := idx.sourceDocumentGeneration, idx.sourceDocumentRootsValid
	idx.mu.RUnlock()
	return generation, valid
}

func (idx *VectorIndex) markVectorMetaDirtyLocked() {
	if !idx.nativePersistent {
		return
	}
	idx.dirtyMeta = true
}

func (idx *VectorIndex) markVectorNodeDirtyLocked(nodeID int) {
	if nodeID < 0 {
		return
	}
	if view := idx.searchView.Load(); view != nil && nodeID < len(view.nodes) {
		if idx.searchViewDirty == nil {
			idx.searchViewDirty = make(map[int]struct{})
		}
		idx.searchViewDirty[nodeID] = struct{}{}
	}
	if !idx.nativePersistent {
		return
	}
	if idx.dirtyNodes == nil {
		idx.dirtyNodes = make(map[int]struct{})
	}
	idx.dirtyNodes[nodeID] = struct{}{}
}

func (idx *VectorIndex) markVectorDocDirtyLocked(documentID []byte) {
	if !idx.nativePersistent || len(documentID) == 0 {
		return
	}
	if idx.dirtyDocs == nil {
		idx.dirtyDocs = make(map[string]struct{})
	}
	idx.dirtyDocs[string(documentID)] = struct{}{}
}

func (idx *VectorIndex) maxNeighborsForLayer(layer int) int {
	if layer == 0 {
		return maxInt(idx.m*2, idx.m)
	}
	return idx.m
}

func (idx *VectorIndex) initialNeighborCapacityForLayer(layer int) int {
	return minInt(idx.maxNeighborsForLayer(layer), maxVectorIndexEagerNeighborCap)
}

func normalizeVectorIndexEdgeDistance(distance float32) (float32, bool) {
	if math.IsNaN(float64(distance)) || math.IsInf(float64(distance), 1) {
		return 0, false
	}
	if math.IsInf(float64(distance), -1) {
		return -math.MaxFloat32, true
	}
	return distance, true
}

func (idx *VectorIndex) linkLayerLocked(fromNodeID, toNodeID, layer int, markDirty bool) {
	if fromNodeID < 0 || fromNodeID >= len(idx.nodes) {
		return
	}
	if layer < 0 || layer > idx.nodes[fromNodeID].level {
		return
	}
	neighbors := idx.nodes[fromNodeID].neighbors[layer]
	// Preserve the pre-link slice for trace-only reciprocal-prune accounting.
	// append may reuse the node slice's backing array, so reading the node's
	// old slice after append can incorrectly treat the newly linked edge as an
	// existing edge and produce a contradictory lifecycle.
	var preexisting []vectorIndexNeighbor
	if idx.constructionTrace != nil {
		preexisting = append(preexisting, neighbors...)
	}
	for _, existing := range neighbors {
		if existing.nodeID == toNodeID {
			return
		}
	}
	distance := idx.distanceBetweenNodesLocked(fromNodeID, toNodeID)
	var ok bool
	distance, ok = normalizeVectorIndexEdgeDistance(distance)
	if !ok {
		return
	}
	neighbors = append(neighbors, vectorIndexNeighbor{nodeID: toNodeID, distance: distance})
	trace := idx.constructionTrace
	var origin string
	if trace != nil {
		trace.init()
		key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: toNodeID, Layer: layer}
		origin = trace.pending[key]
		if origin != "" {
			delete(trace.pending, key)
			trace.record(fromNodeID, toNodeID, layer, origin, "initial_add")
		} else {
			origin = "reciprocal_add"
			trace.record(fromNodeID, toNodeID, layer, origin, "reciprocal_add")
		}
		trace.origins[key] = origin
	}
	limit := idx.maxNeighborsForLayer(layer)
	if len(neighbors) > limit {
		neighbors = idx.pruneLayerNeighborsLocked(fromNodeID, neighbors, limit)
		if trace != nil {
			kept := make(map[int]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				kept[neighbor.nodeID] = struct{}{}
			}
			// The just-pruned list is the authoritative reciprocal maintenance
			// outcome for every edge that was present before the prune.
			for _, neighbor := range preexisting {
				key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: neighbor.nodeID, Layer: layer}
				edgeOrigin := trace.origins[key]
				if _, ok := kept[neighbor.nodeID]; ok {
					trace.record(fromNodeID, neighbor.nodeID, layer, edgeOrigin, "reciprocal_prune_keep")
					continue
				}
				trace.record(fromNodeID, neighbor.nodeID, layer, edgeOrigin, "reciprocal_prune_drop")
				delete(trace.origins, key)
			}
			// The newly added edge is not yet in the node slice above.
			key := vectorIndexConstructionEdgeKeyV1{From: fromNodeID, To: toNodeID, Layer: layer}
			if _, ok := kept[toNodeID]; ok {
				trace.record(fromNodeID, toNodeID, layer, trace.origins[key], "reciprocal_prune_keep")
			} else {
				trace.record(fromNodeID, toNodeID, layer, trace.origins[key], "reciprocal_prune_drop")
				delete(trace.origins, key)
			}
		}
	}
	idx.nodes[fromNodeID].neighbors[layer] = neighbors
	if markDirty {
		idx.markVectorNodeDirtyLocked(fromNodeID)
	}
}

func (idx *VectorIndex) pruneLayerNeighborsLocked(_ int, neighbors []vectorIndexNeighbor, limit int) []vectorIndexNeighbor {
	if limit <= 0 || len(neighbors) == 0 {
		return nil
	}
	if len(neighbors) <= limit {
		return neighbors
	}
	var stack [128]vectorIndexCandidate
	scored := stack[:0]
	if len(neighbors) > len(stack) {
		scored = make([]vectorIndexCandidate, 0, len(neighbors))
	}
	for _, neighbor := range neighbors {
		neighborID := neighbor.nodeID
		if neighborID < 0 || neighborID >= len(idx.nodes) {
			continue
		}
		distance, ok := normalizeVectorIndexEdgeDistance(neighbor.distance)
		if !ok {
			continue
		}
		scored = append(scored, vectorIndexCandidate{nodeID: neighborID, distance: distance})
	}
	scored, _ = idx.selectDiverseCandidatesWithAccountingLocked(scored, limit)
	out := neighbors[:0]
	for _, candidate := range scored {
		out = append(out, vectorIndexNeighbor{nodeID: candidate.nodeID, distance: candidate.distance})
	}
	return out
}

// Search returns ANN candidates from the in-memory graph and reranks the final
// result set. Unfiltered float32 cosine indexes rerank from resident vectors;
// filtered or compressed searches rerank from canonical collection rows. If
// graph search underfills and DisableExactFallback is false, it falls back to
// the exact scan API.
func (idx *VectorIndex) Search(query []float32, opts VectorIndexSearchOptions) ([]VectorSearchResult, VectorIndexTrace, error) {
	trace := VectorIndexTrace{Strategy: "ann_graph"}
	if idx == nil {
		return nil, trace, errors.New("collections: vector index is nil")
	}
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		return nil, trace, err
	}
	if queryMode != columnVectorGraphNativeSearchQueryModeExact {
		return nil, trace, fmt.Errorf("%w: native_runtime vector index %q does not support quantized query mode %q", ErrVectorIndexSearchUnavailable, idx.name, opts.QueryMode)
	}
	if opts.TopK <= 0 {
		return nil, trace, errors.New("collections: vector search TopK must be positive")
	}
	if len(query) == 0 {
		return nil, trace, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return nil, trace, fmt.Errorf("collections: vector query: %w", err)
	}
	queryNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return nil, trace, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return nil, trace, err
		}
		prepared = &preparedQuery
	}
	idx.mu.RLock()
	if idx.nativePersistent && !idx.sourceDocumentRootsValid {
		idx.mu.RUnlock()
		return nil, trace, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, idx.name)
	}
	if idx.dimensions != 0 && len(query) != idx.dimensions {
		dims := idx.dimensions
		idx.mu.RUnlock()
		return nil, trace, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), dims)
	}
	ef := opts.EfSearch
	if ef <= 0 {
		ef = idx.efSearch
	}
	fetchMultiplier := opts.FetchMultiplier
	if fetchMultiplier <= 0 {
		fetchMultiplier = defaultVectorIndexFetchMultiple
	}
	candidateLimit := opts.TopK * fetchMultiplier
	if candidateLimit < ef {
		candidateLimit = ef
	}
	if candidateLimit < opts.TopK {
		candidateLimit = opts.TopK
	}
	trace.EfSearch = ef
	trace.FetchMultiplier = fetchMultiplier
	idx.mu.RUnlock()

	var rangeIDs [][]byte
	var rangeFilter func(DocumentRecord) (bool, error)
	if opts.IndexRangeFilter != nil {
		exactFilterMax := opts.ExactFilterMaxDocs
		if exactFilterMax <= 0 {
			exactFilterMax = defaultVectorIndexExactFilterMax
		}
		probeIDs, truncated, err := idx.collection.vectorSearchIndexRangeDocumentIDs(opts.IndexRangeFilter, exactFilterMax+1)
		if err != nil {
			return nil, trace, err
		}
		if !truncated && len(probeIDs) <= exactFilterMax {
			trace.Strategy = "exact_filtered"
			trace.CandidatesExamined = len(probeIDs)
			trace.CandidatesAfterTombstone = len(probeIDs)
			results, err := idx.rerankCandidates(query, probeIDs, opts.Filter, opts.TopK, &trace)
			if err != nil {
				return nil, trace, err
			}
			sortVectorSearchResults(results)
			if len(results) > opts.TopK {
				results = results[:opts.TopK]
			}
			trace.ReturnedCount = len(results)
			return results, trace, nil
		}
		rangeIDs, _, err = idx.collection.vectorSearchIndexRangeDocumentIDs(opts.IndexRangeFilter, 0)
		if err != nil {
			return nil, trace, err
		}
		allowed := vectorDocumentIDSet(rangeIDs)
		rangeFilter = func(record DocumentRecord) (bool, error) {
			if _, ok := allowed[string(record.ID)]; !ok {
				return false, nil
			}
			if opts.Filter == nil {
				return true, nil
			}
			return opts.Filter(record)
		}
		trace.Strategy = "ann_postfilter"
	}
	idx.mu.RLock()
	if idx.nativePersistent && !idx.sourceDocumentRootsValid {
		idx.mu.RUnlock()
		return nil, trace, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, idx.name)
	}
	scratch := idx.getSearchScratch()
	candidates := idx.searchCandidatesLocked(query, queryNorm, prepared, candidateLimit, scratch)
	trace.CandidatesExamined = len(candidates)
	filter := opts.Filter
	if rangeFilter != nil {
		filter = rangeFilter
	}
	fastRerank := filter == nil && idx.metric == VectorMetricCosine && idx.encoding == VectorIndexEncodingFloat32
	var results []VectorSearchResult
	var resultNodeIDs []int
	var resultNodeIDStack [64]int
	var candidateIDs [][]byte
	if fastRerank {
		rerankLimit := len(candidates)
		resultNodeIDs = resultNodeIDStack[:0]
		if rerankLimit > len(resultNodeIDStack) {
			resultNodeIDs = make([]int, 0, rerankLimit)
		}
		results, resultNodeIDs, err = idx.rerankFloat32CosineCandidatesFromNodesLocked(query, queryNorm, prepared.invNorm, candidates, rerankLimit, resultNodeIDs, &trace)
	} else {
		candidateIDs = idx.currentCandidateDocumentIDsLocked(candidates)
		trace.CandidatesAfterTombstone = len(candidateIDs)
	}
	idx.putSearchScratch(scratch)
	idx.mu.RUnlock()

	if fastRerank && err == nil {
		results, err = idx.attachVectorSearchResultDocuments(results, resultNodeIDs, opts.TopK)
	} else if !fastRerank {
		results, err = idx.rerankCandidates(query, candidateIDs, filter, opts.TopK, &trace)
	}
	if err != nil {
		return nil, trace, err
	}
	sortVectorSearchResults(results)
	if len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	trace.ReturnedCount = len(results)
	if len(results) < opts.TopK && !opts.DisableExactFallback {
		if opts.IndexRangeFilter != nil {
			trace.Strategy = "ann_postfilter_exact_fallback"
		} else {
			trace.Strategy = "ann_graph_exact_fallback"
		}
		trace.ExactFallbackReason = "underfilled_results"
		exact, err := idx.collection.SearchVectorsExact(query, VectorSearchOptions{
			Field:            idx.field,
			Metric:           idx.metric,
			TopK:             opts.TopK,
			Filter:           opts.Filter,
			IndexRangeFilter: opts.IndexRangeFilter,
		})
		if err != nil {
			return nil, trace, err
		}
		trace.ReturnedCount = len(exact)
		return exact, trace, nil
	}
	return results, trace, nil
}

func (idx *VectorIndex) currentCandidateDocumentIDsLocked(candidates []vectorIndexCandidate) [][]byte {
	candidateIDs := make([][]byte, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		candidateIDs = append(candidateIDs, bytes.Clone(node.documentID))
	}
	return candidateIDs
}

func (idx *VectorIndex) searchGraphOnly(query []float32, topK, efSearch int) ([]VectorSearchResult, error) {
	if idx == nil {
		return nil, errors.New("collections: vector index is nil")
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	scratch := idx.getSearchScratch()
	defer idx.putSearchScratch(scratch)
	candidates, err := idx.searchGraphOnlyCandidatesLocked(query, topK, efSearch, scratch)
	if err != nil {
		return nil, err
	}
	results := make([]VectorSearchResult, 0, minInt(topK, len(candidates)))
	for _, candidate := range candidates {
		if len(results) >= topK {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		results = append(results, VectorSearchResult{
			DocumentID: node.documentID,
			Distance:   candidate.distance,
		})
	}
	cloneVectorSearchResultDocumentIDs(results)
	return results, nil
}

func (idx *VectorIndex) searchGraphOnlyWithBuffer(query []float32, topK, efSearch int, buffer *VectorIndexSearchBuffer) ([]VectorIndexSearchResult, vectorIndexNativeSearchState, error) {
	if idx == nil {
		return nil, vectorIndexNativeSearchState{}, errors.New("collections: vector index is nil")
	}
	if buffer == nil {
		return nil, vectorIndexNativeSearchState{}, errors.New("collections: nil vector index search buffer")
	}
	buffer.Reset()
	if view := idx.acquireSearchView(); view != nil {
		results, err := view.searchGraphOnlyWithBuffer(query, topK, efSearch, buffer)
		state := view.nativeSearchState()
		idx.releaseSearchView(view)
		return results, state, err
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	candidates, err := idx.searchGraphOnlyCandidatesLocked(query, topK, efSearch, &buffer.nativeSearchScratch)
	if err != nil {
		return nil, vectorIndexNativeSearchState{}, err
	}
	resultCount := 0
	idByteCount := 0
	for _, candidate := range candidates {
		if resultCount >= topK {
			break
		}
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		idByteCount, err = addVectorIndexSearchByteTotal(idByteCount, len(node.documentID), math.MaxInt, "result id")
		if err != nil {
			return nil, vectorIndexNativeSearchState{}, err
		}
		resultCount++
	}
	buffer.results = resizeVectorIndexSearchResultBuffer(buffer.results, resultCount)
	buffer.idBytes = resizeVectorIndexSearchByteBuffer(buffer.idBytes, idByteCount)
	resultIndex := 0
	idOffset := 0
	for _, candidate := range candidates {
		if resultIndex >= resultCount {
			break
		}
		node := idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		nextIDOffset := idOffset + len(node.documentID)
		id := buffer.idBytes[idOffset:nextIDOffset:nextIDOffset]
		copy(id, node.documentID)
		buffer.results[resultIndex] = VectorIndexSearchResult{ID: id, Score: 1 - float64(candidate.distance)}
		resultIndex++
		idOffset = nextIDOffset
	}
	deletedDocs := len(idx.nodes) - len(idx.currentNode)
	return buffer.results, vectorIndexNativeSearchState{
		liveDocs:      len(idx.currentNode),
		rebuildNeeded: deletedDocs > 0 && float64(deletedDocs)/float64(len(idx.nodes)) >= idx.rebuildDeletedRatio,
		fullRebuilds:  idx.liveANNFullRebuilds,
	}, nil
}

func (idx *VectorIndex) searchGraphOnlyCandidatesLocked(query []float32, topK, efSearch int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, error) {
	if idx.nativePersistent && !idx.sourceDocumentRootsValid {
		return nil, fmt.Errorf("%w: native_runtime vector index %q does not cover current documents", ErrVectorIndexSearchUnavailable, idx.name)
	}
	return idx.searchGraphOnlyCandidatesWithLiveDocsLocked(query, topK, efSearch, len(idx.currentNode), scratch)
}

func (idx *VectorIndex) searchGraphOnlyCandidatesWithLiveDocsLocked(query []float32, topK, efSearch, liveDocs int, scratch *vectorIndexSearchScratch) ([]vectorIndexCandidate, error) {
	if topK < 0 {
		return nil, errors.New("collections: vector search TopK must be positive")
	}
	if len(query) == 0 {
		return nil, errors.New("collections: vector query cannot be empty")
	}
	if err := validateFloat32Vector(query); err != nil {
		return nil, fmt.Errorf("collections: vector query: %w", err)
	}
	queryNorm := float64(-1)
	if idx.metric == VectorMetricCosine {
		queryNorm = vectorNormSquared(query)
		if queryNorm == 0 {
			return nil, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	var prepared *preparedFloat32CosineQuery
	if idx.metric == VectorMetricCosine {
		preparedQuery, err := prepareFloat32CosineQuery(query, queryNorm)
		if err != nil {
			return nil, err
		}
		prepared = &preparedQuery
	}
	if idx.dimensions != 0 && len(query) != idx.dimensions {
		return nil, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), idx.dimensions)
	}
	if topK == 0 {
		return nil, nil
	}
	if liveDocs == 0 {
		return nil, nil
	}
	limit := efSearch
	if limit <= 0 {
		limit = idx.efSearch
	}
	if limit < topK {
		limit = topK
	}
	if limit > liveDocs {
		limit = liveDocs
	}
	candidates := idx.searchCurrentCandidatesWithLiveDocsLocked(query, queryNorm, prepared, limit, liveDocs, scratch)
	if target := minInt(topK, liveDocs); len(candidates) < target {
		return nil, fmt.Errorf("%w: native graph search returned %d of %d live candidates within bounded traversal; rebuild the vector index", ErrVectorIndexSearchUnavailable, len(candidates), target)
	}
	return candidates, nil
}

func vectorDocumentIDSet(ids [][]byte) map[string]struct{} {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[string(id)] = struct{}{}
	}
	return out
}

func (idx *VectorIndex) rerankFloat32CosineCandidatesFromNodesLocked(query []float32, queryNormSquared float64, queryInvNorm float32, candidates []vectorIndexCandidate, topK int, rankedNodeIDs []int, trace *VectorIndexTrace) ([]VectorSearchResult, []int, error) {
	if len(candidates) == 0 {
		return []VectorSearchResult{}, nil, nil
	}
	dims := len(query)
	if dims == 0 {
		return nil, nil, errors.New("collections: invalid vector rerank query")
	}

	ranked := make([]VectorSearchResult, 0, minInt(topK, len(candidates)))
	rankedNodeIDs = rankedNodeIDs[:0]
	liveCandidates := 0
	for _, candidate := range candidates {
		if candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		node := &idx.nodes[candidate.nodeID]
		if node.deleted {
			continue
		}
		currentNodeID, ok := idx.currentNode[string(node.documentID)]
		if !ok || currentNodeID != candidate.nodeID {
			continue
		}
		liveCandidates++
		if len(node.vector) != dims {
			return nil, nil, fmt.Errorf("collections: vector dimensions differ: %d vs %d", dims, len(node.vector))
		}
		if node.cachedInvNorm == 0 {
			return nil, nil, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		dot := dotProductFloat32ForCosine(query, node.vector, queryNormSquared, node.normSquared)
		distance := float32(1 - dot*float64(queryInvNorm)*float64(node.cachedInvNorm))
		ranked, rankedNodeIDs = appendBoundedVectorIndexNodeResult(ranked, rankedNodeIDs, VectorSearchResult{
			DocumentID: node.documentID,
			Distance:   distance,
		}, candidate.nodeID, topK)
	}
	if trace != nil {
		trace.CandidatesAfterTombstone = liveCandidates
		trace.CandidatesAfterFilter += liveCandidates
		trace.RerankCount += liveCandidates
	}

	if ranked == nil {
		return []VectorSearchResult{}, nil, nil
	}
	return ranked, rankedNodeIDs, nil
}

func (idx *VectorIndex) rerankCandidates(query []float32, candidateIDs [][]byte, filter func(DocumentRecord) (bool, error), topK int, trace *VectorIndexTrace) ([]VectorSearchResult, error) {
	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, err
	}
	defer func() { _ = materializer.Close() }()

	results := make([]VectorSearchResult, 0, minInt(topK, len(candidateIDs)))
	for _, documentID := range candidateIDs {
		document, found, err := idx.collection.GetInto(documentID, nil)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		resultID := bytes.Clone(documentID)
		record := DocumentRecord{ID: resultID, Document: document}
		if filter != nil {
			include, err := filter(record)
			if err != nil {
				return nil, err
			}
			if !include {
				continue
			}
		}
		if trace != nil {
			trace.CandidatesAfterFilter++
		}
		vector, ok, err := vectorFromStoredDocument(materializer, document, idx.fieldPath)
		if err != nil {
			return nil, fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, documentID, err)
		}
		if !ok || len(vector) != len(query) {
			continue
		}
		distance, err := exactVectorDistance(query, vector, idx.metric)
		if err != nil {
			return nil, err
		}
		if trace != nil {
			trace.RerankCount++
		}
		results = appendBoundedVectorSearchResult(results, VectorSearchResult{
			DocumentID: resultID,
			Distance:   distance,
			Document:   document,
		}, topK)
	}
	return results, nil
}

func appendBoundedVectorIndexNodeResult(matches []VectorSearchResult, nodeIDs []int, result VectorSearchResult, nodeID, limit int) ([]VectorSearchResult, []int) {
	if limit <= 0 {
		return matches, nodeIDs
	}
	if len(matches) == limit && compareVectorSearchResults(result, matches[len(matches)-1]) >= 0 {
		return matches, nodeIDs
	}
	matches = append(matches, result)
	nodeIDs = append(nodeIDs, nodeID)
	for i := len(matches) - 1; i > 0 && compareVectorSearchResults(matches[i], matches[i-1]) < 0; i-- {
		matches[i], matches[i-1] = matches[i-1], matches[i]
		nodeIDs[i], nodeIDs[i-1] = nodeIDs[i-1], nodeIDs[i]
	}
	if len(matches) > limit {
		matches = matches[:limit]
		nodeIDs = nodeIDs[:limit]
	}
	return matches, nodeIDs
}

func (idx *VectorIndex) filterAttachedCurrentNodeResults(results []VectorSearchResult, nodeIDs []int) []VectorSearchResult {
	if len(results) == 0 || len(nodeIDs) != len(results) {
		return results
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	filtered := results[:0]
	for i, result := range results {
		currentNodeID, ok := idx.currentNode[string(result.DocumentID)]
		if !ok || currentNodeID != nodeIDs[i] {
			continue
		}
		if currentNodeID < 0 || currentNodeID >= len(idx.nodes) || idx.nodes[currentNodeID].deleted {
			continue
		}
		filtered = append(filtered, result)
	}
	if filtered == nil {
		return []VectorSearchResult{}
	}
	return filtered
}

func (idx *VectorIndex) isCurrentVectorNodeResult(documentID []byte, nodeID int) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	currentNodeID, ok := idx.currentNode[string(documentID)]
	if !ok || currentNodeID != nodeID {
		return false
	}
	return currentNodeID >= 0 && currentNodeID < len(idx.nodes) && !idx.nodes[currentNodeID].deleted
}

func (idx *VectorIndex) attachVectorSearchResultDocuments(ranked []VectorSearchResult, rankedNodeIDs []int, topK int) ([]VectorSearchResult, error) {
	if idx == nil {
		return nil, errors.New("collections: vector index is nil")
	}
	if idx.collection == nil {
		return nil, errCollectionNil
	}
	if idx.collection.db == nil {
		return nil, errCollectionDBNil
	}
	results := ranked[:0]
	resultNodeIDs := rankedNodeIDs[:0]
	limit := minInt(topK, len(ranked))
	var modeStack [64]vectorDocumentAttachMode
	modes := modeStack[:0]
	if limit > len(modeStack) {
		modes = make([]vectorDocumentAttachMode, 0, limit)
	}
	resultIDBytes := 0
	snapshotCopyBytes := 0

	snap := idx.collection.db.AcquireSnapshot()
	if snap == nil {
		return nil, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	catalog, err := idx.collection.catalogForSnapshot(snap)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	rootName := collectionPrimaryRootName(idx.collection.meta.Name)

	for i := range ranked {
		if len(results) >= limit {
			break
		}
		result := ranked[i]
		document, buffered, found := idx.collection.getBufferedDocumentInto(result.DocumentID, nil)
		if buffered {
			if !found {
				continue
			}
			if !idx.isCurrentVectorNodeResult(result.DocumentID, rankedNodeIDs[i]) {
				continue
			}
			result.Document = document
			resultIDBytes += len(result.DocumentID)
			results = append(results, result)
			resultNodeIDs = append(resultNodeIDs, rankedNodeIDs[i])
			modes = append(modes, vectorDocumentAttachBuffered)
			continue
		}
		document, found, err = collectionGetUnsafeAtCatalogRoot(snap, catalog, rootName, result.DocumentID)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if !idx.isCurrentVectorNodeResult(result.DocumentID, rankedNodeIDs[i]) {
			continue
		}
		document = bytes.Clone(document)
		result.Document = document
		resultIDBytes += len(result.DocumentID)
		snapshotCopyBytes += len(document)
		results = append(results, result)
		resultNodeIDs = append(resultNodeIDs, rankedNodeIDs[i])
		modes = append(modes, vectorDocumentAttachSnapshotView)
	}
	if len(results) == 0 {
		return []VectorSearchResult{}, nil
	}

	var arena []byte
	if arenaBytes := resultIDBytes + snapshotCopyBytes; arenaBytes > 0 {
		arena = make([]byte, 0, arenaBytes)
	}
	out := results[:0]
	for i := range results {
		result := results[i]
		if len(result.DocumentID) > 0 {
			start := len(arena)
			arena = append(arena, result.DocumentID...)
			result.DocumentID = arena[start:len(arena):len(arena)]
		}
		switch modes[i] {
		case vectorDocumentAttachSnapshotView:
			start := len(arena)
			arena = append(arena, result.Document...)
			result.Document = arena[start:len(arena):len(arena)]
		}
		out = append(out, result)
	}
	if len(out) == 0 {
		return []VectorSearchResult{}, nil
	}
	return idx.filterAttachedCurrentNodeResults(out, resultNodeIDs), nil
}

type vectorDocumentAttachMode uint8

const (
	vectorDocumentAttachBuffered vectorDocumentAttachMode = iota
	vectorDocumentAttachSnapshotView
)

func collectionGetUnsafeAtCatalogRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, key []byte) ([]byte, bool, error) {
	if snap == nil {
		return nil, false, backenddb.ErrClosed
	}
	if len(catalog.overlayRootIDs(rootName)) == 0 {
		rootID := catalog.rootID(rootName)
		if rootID == 0 {
			return nil, false, nil
		}
		out, err := snap.GetUnsafeAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, false, nil
		}
		return out, err == nil, err
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
			return nil, false, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, false, nil
		}
		out, err := snap.GetUnsafeAtRoot(rootID, key)
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, false, nil
		}
		return out, err == nil, err
	}
	return nil, false, nil
}

func cloneVectorSearchResultDocumentIDs(results []VectorSearchResult) {
	total := 0
	for _, result := range results {
		total += len(result.DocumentID)
	}
	if total == 0 {
		return
	}
	arena := make([]byte, 0, total)
	for i := range results {
		start := len(arena)
		arena = append(arena, results[i].DocumentID...)
		results[i].DocumentID = arena[start:len(arena):len(arena)]
	}
}

func (idx *VectorIndex) searchCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	entryPoint := idx.entry
	for layer := idx.maxLevel; layer > 0; layer-- {
		entryPoint = idx.greedyNearestAtLayerLocked(query, queryNormSquared, prepared, entryPoint, layer)
	}
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, 0, scratch)
}

func (idx *VectorIndex) searchCurrentCandidatesLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchCurrentCandidatesWithLiveDocsLocked(query, queryNormSquared, prepared, limit, len(idx.currentNode), scratch)
}

func (idx *VectorIndex) searchCurrentCandidatesWithLiveDocsLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, limit, liveDocs int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	if idx.entry < 0 || len(idx.nodes) == 0 || limit <= 0 {
		return nil
	}
	stale := len(idx.nodes) - liveDocs
	if stale == 0 {
		entryPoint := idx.entry
		for layer := idx.maxLevel; layer > 0; layer-- {
			entryPoint = idx.greedyNearestAtLayerLocked(query, queryNormSquared, prepared, entryPoint, layer)
		}
		return idx.searchLayerWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, 0, scratch)
	}
	explorationLimit := limit
	if stale > 0 && explorationLimit < len(idx.nodes) {
		// Tombstones remain waypoints, but historical churn must not turn a
		// bounded ANN query into an O(nodes) traversal. Permit at most one
		// layer-0 neighbor fanout of stale waypoints per live candidate.
		degree := maxInt(1, idx.maxNeighborsForLayer(0))
		maxExtra := math.MaxInt
		if degree > 1 && limit <= math.MaxInt/(degree-1) {
			maxExtra = limit * (degree - 1)
		}
		explorationLimit += minInt(stale, minInt(maxExtra, len(idx.nodes)-explorationLimit))
	}
	entryPoint := idx.entry
	upperExplored := 0
	// Keep the requested live-candidate budget plus one possible stale entry
	// point for layer 0. Upper layers share the remaining stale allowance.
	upperLimit := maxInt(0, explorationLimit-limit-1)
	for layer := idx.maxLevel; layer > 0 && upperExplored < upperLimit; layer-- {
		entryPoint = idx.greedyNearestAtLayerBoundedLocked(query, queryNormSquared, prepared, entryPoint, layer, upperLimit, &upperExplored)
	}
	result := idx.searchLayerCurrentWithScratchLocked(query, queryNormSquared, prepared, entryPoint, limit, explorationLimit-upperExplored, 0, scratch)
	scratch.explored += upperExplored
	scratch.explorationLimit = explorationLimit
	return result
}

func (idx *VectorIndex) greedyNearestAtLayerBoundedLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint, layer, scoreLimit int, scored *int) int {
	if entryPoint < 0 || scored == nil || *scored >= scoreLimit {
		return entryPoint
	}
	best := entryPoint
	bestDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best)
	(*scored)++
	for changed := true; changed; {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			if *scored >= scoreLimit {
				return best
			}
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighbor.nodeID)
			(*scored)++
			if distance < bestDistance {
				best = neighbor.nodeID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *VectorIndex) greedyNearestAtLayerLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, layer int) int {
	if entryPoint < 0 {
		return entryPoint
	}
	best := entryPoint
	bestDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, best)
	changed := true
	for changed {
		changed = false
		for _, neighbor := range idx.layerNeighborsLocked(best, layer) {
			neighborID := neighbor.nodeID
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
			if distance < bestDistance {
				best = neighborID
				bestDistance = distance
				changed = true
			}
		}
	}
	return best
}

func (idx *VectorIndex) searchLayerLocked(query []float32, queryNormSquared float64, entryPoint int, limit int, layer int) []vectorIndexCandidate {
	return idx.searchLayerWithScratchLocked(query, queryNormSquared, nil, entryPoint, limit, layer, nil)
}

func (idx *VectorIndex) getSearchScratch() *vectorIndexSearchScratch {
	if scratch, ok := idx.searchScratch.Get().(*vectorIndexSearchScratch); ok {
		return scratch
	}
	return &vectorIndexSearchScratch{}
}

func (idx *VectorIndex) putSearchScratch(scratch *vectorIndexSearchScratch) {
	if scratch == nil {
		return
	}
	idx.searchScratch.Put(scratch)
}

func (idx *VectorIndex) searchLayerWithScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeLocked(query, queryNormSquared, prepared, entryPoint, limit, limit, layer, scratch, false)
}

func (idx *VectorIndex) searchLayerCurrentWithScratchLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch) []vectorIndexCandidate {
	return idx.searchLayerWithScratchModeLocked(query, queryNormSquared, prepared, entryPoint, limit, explorationLimit, layer, scratch, true)
}

func (idx *VectorIndex) searchLayerWithScratchModeLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, entryPoint int, limit, explorationLimit int, layer int, scratch *vectorIndexSearchScratch, currentOnly bool) []vectorIndexCandidate {
	if entryPoint < 0 || entryPoint >= len(idx.nodes) || limit <= 0 {
		return nil
	}
	entryDistance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, entryPoint)
	if math.IsInf(float64(entryDistance), 1) {
		return nil
	}
	if scratch == nil {
		scratch = &vectorIndexSearchScratch{}
	}
	scratch.explorationLimit = explorationLimit
	visited, mark := scratch.nextVisitedEpoch(len(idx.nodes))
	visited[entryPoint] = mark
	scratch.explored = 1
	entry := vectorIndexCandidate{nodeID: entryPoint, distance: entryDistance}
	queue := scratch.queue[:0]
	queue.push(entry)
	best := scratch.best[:0]
	best.pushBounded(entry, explorationLimit)
	liveBest := scratch.liveBest[:0]
	if currentOnly && !idx.nodes[entryPoint].deleted {
		liveBest.pushBounded(entry, limit)
	}
search:
	for len(queue) > 0 {
		current := queue.pop()
		if len(best) >= explorationLimit && vectorIndexCandidateWorse(current, best[0]) {
			break
		}
		if current.nodeID < 0 || current.nodeID >= len(idx.nodes) {
			continue
		}
		for _, neighbor := range idx.layerNeighborsLocked(current.nodeID, layer) {
			neighborID := neighbor.nodeID
			if neighborID < 0 || neighborID >= len(idx.nodes) || visited[neighborID] == mark {
				continue
			}
			if currentOnly && scratch.explored >= explorationLimit {
				break search
			}
			visited[neighborID] = mark
			distance := idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, prepared, neighborID)
			scratch.explored++
			if math.IsInf(float64(distance), 1) {
				continue
			}
			candidate := vectorIndexCandidate{nodeID: neighborID, distance: distance}
			if len(best) >= explorationLimit && !vectorIndexCandidateLess(candidate, best[0]) {
				continue
			}
			queue.push(candidate)
			best.pushBounded(candidate, explorationLimit)
			if currentOnly && !idx.nodes[neighborID].deleted {
				liveBest.pushBounded(candidate, limit)
			}
		}
	}
	scratch.queue = queue[:0]
	scratch.best = best[:0]
	scratch.liveBest = liveBest[:0]
	out := scratch.out[:0]
	if currentOnly {
		out = append(out, liveBest...)
	} else {
		out = append(out, best...)
	}
	scratch.out = out
	sortVectorIndexCandidates(out)
	return out
}

func (idx *VectorIndex) layerNeighborsLocked(nodeID int, layer int) []vectorIndexNeighbor {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return nil
	}
	node := idx.nodes[nodeID]
	if layer < 0 || layer >= len(node.neighbors) {
		return nil
	}
	return node.neighbors[layer]
}

func (idx *VectorIndex) selectLayerNeighborsLocked(vector []float32, vectorNormSquared float64, prepared *preparedFloat32CosineQuery, candidates []vectorIndexCandidate, layer, limit, excludeNodeID int) []int {
	if limit <= 0 {
		return nil
	}
	trace := idx.constructionTrace
	candidateCount := len(candidates)
	var sampledCandidates []int
	sampled := false
	if trace != nil {
		// Evidence counts candidate ordinals, not raw heap entries. The search
		// selection itself deliberately retains duplicate entries for historical
		// behavior, but sampled and unsampled accounting must share set semantics.
		distinct := make(map[int]struct{}, len(candidates))
		sampled = layer == 0 && excludeNodeID >= 0 && excludeNodeID < len(idx.nodes)
		if sampled {
			_, sampled = trace.sampleIDs[string(idx.nodes[excludeNodeID].documentID)]
			if sampled {
				sampledCandidates = make([]int, 0, len(candidates))
			}
		}
		for _, candidate := range candidates {
			if candidate.nodeID == excludeNodeID || candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) || idx.nodes[candidate.nodeID].level < layer || math.IsInf(float64(candidate.distance), 1) {
				continue
			}
			if _, seen := distinct[candidate.nodeID]; seen {
				continue
			}
			distinct[candidate.nodeID] = struct{}{}
			if sampled {
				sampledCandidates = append(sampledCandidates, candidate.nodeID)
			}
		}
		candidateCount = len(distinct)
	}
	scored := candidates[:0]
	for _, candidate := range candidates {
		if candidate.nodeID == excludeNodeID || candidate.nodeID < 0 || candidate.nodeID >= len(idx.nodes) {
			continue
		}
		if idx.nodes[candidate.nodeID].level < layer {
			continue
		}
		if math.IsInf(float64(candidate.distance), 1) {
			continue
		}
		scored = append(scored, candidate)
	}
	var diversitySelected int
	var diversity map[int]bool
	backfill := true
	qualityPostfill := false
	robustPruneRefinement := false
	if layer == 0 && idx.layer0ConstructionPolicy != nil {
		backfill = idx.layer0ConstructionPolicy.backfill
		qualityPostfill = idx.layer0ConstructionPolicy.qualityPostfill
		robustPruneRefinement = idx.layer0ConstructionPolicy.robustPruneRefinement
	}
	// Keep every finite construction comparison, not only diversity rejects.
	// An initial directed selection is an equally valid observed pair for the
	// reverse endpoint; retaining the symmetric relation lets the explicit
	// post-construction pass fill early nodes without inventing an unobserved
	// nearest-neighbor fallback.
	capturePostfill := qualityPostfill || robustPruneRefinement
	var constructionCandidates []vectorIndexCandidate
	if capturePostfill {
		// This candidate-pool snapshot is needed only by the two offline final
		// refinements. Keep the ordinary and non-refinement policy paths free of
		// this allocation.
		constructionCandidates = append([]vectorIndexCandidate(nil), scored...)
	}
	scored, diversitySelected, _, diversity, _ = idx.selectDiverseCandidatesWithDetailsLocked(scored, limit, trace != nil, backfill, false)
	if capturePostfill {
		idx.captureQualityPostfillCandidatesLocked(excludeNodeID, constructionCandidates)
	}
	if trace != nil {
		selection := vectorIndexConstructionSelectionV1{Node: excludeNodeID, Layer: layer, Candidates: candidateCount, Selected: len(scored), DiversitySelected: diversitySelected, BackfillSelected: len(scored) - diversitySelected}
		if sampled {
			selection.Sampled = true
			selection.CandidateNodes = sampledCandidates
		}
		trace.selections = append(trace.selections, selection)
		for _, candidate := range scored {
			origin := "nearest_backfill"
			if diversity[candidate.nodeID] {
				origin = "diversity_selected"
			}
			trace.selectEdge(excludeNodeID, candidate.nodeID, layer, origin)
		}
	}
	out := make([]int, len(scored))
	for i := range scored {
		out[i] = scored[i].nodeID
	}
	return out
}

func (idx *VectorIndex) selectDiverseCandidatesLocked(candidates []vectorIndexCandidate, limit int) []vectorIndexCandidate {
	selected, _ := idx.selectDiverseCandidatesWithAccountingLocked(candidates, limit)
	return selected
}

func (idx *VectorIndex) selectDiverseCandidatesWithAccountingLocked(candidates []vectorIndexCandidate, limit int) ([]vectorIndexCandidate, int) {
	selected, diversitySelected, _, _, _ := idx.selectDiverseCandidatesWithDetailsLocked(candidates, limit, false, true, false)
	return selected, diversitySelected
}

// selectDiverseCandidatesWithOriginsLocked preserves the existing candidate
// order while retaining the causal classification of each selected edge.
func (idx *VectorIndex) selectDiverseCandidatesWithOriginsLocked(candidates []vectorIndexCandidate, limit int) ([]vectorIndexCandidate, int, map[int]bool) {
	selected, diversitySelected, _, origins, _ := idx.selectDiverseCandidatesWithDetailsLocked(candidates, limit, true, true, false)
	return selected, diversitySelected, origins
}

func (idx *VectorIndex) selectDiverseCandidatesWithDetailsLocked(candidates []vectorIndexCandidate, limit int, includeOrigins, backfillEnabled, captureQualityPostfill bool) ([]vectorIndexCandidate, int, int, map[int]bool, []vectorIndexCandidate) {
	if limit <= 0 || len(candidates) == 0 {
		return nil, 0, 0, nil, nil
	}
	idx.sortVectorIndexCandidatesByDistanceLocked(candidates)
	// Backfill-on preserves the historical degree-filling fast path.  The
	// backfill-off construction policies instead need to run the diversity
	// predicate even below their selection cap: a small candidate set can still
	// contain mutually redundant neighbors, and those rejected candidates are
	// the causal pool for the later offline refinements.
	if len(candidates) <= limit && backfillEnabled {
		if !includeOrigins {
			return candidates, len(candidates), 0, nil, nil
		}
		diversity := make(map[int]bool, len(candidates))
		for _, candidate := range candidates {
			diversity[candidate.nodeID] = true
		}
		return candidates, len(candidates), 0, diversity, nil
	}
	if idx.metric == VectorMetricInnerProduct {
		if !includeOrigins {
			return candidates[:limit], limit, 0, nil, nil
		}
		diversity := make(map[int]bool, limit)
		for _, candidate := range candidates[:limit] {
			diversity[candidate.nodeID] = true
		}
		return candidates[:limit], limit, 0, diversity, nil
	}
	var selectedStack [128]vectorIndexCandidate
	selected := selectedStack[:0]
	if limit > len(selectedStack) {
		selected = make([]vectorIndexCandidate, 0, limit)
	}
	var rejectedStack [128]vectorIndexCandidate
	rejected := rejectedStack[:0]
	if len(candidates) > len(rejectedStack) {
		rejected = make([]vectorIndexCandidate, 0, len(candidates))
	}
	for _, candidate := range candidates {
		if len(selected) >= limit {
			rejected = append(rejected, candidate)
			continue
		}
		if idx.vectorIndexCandidateIsDiverseLocked(candidate, selected) {
			selected = append(selected, candidate)
		} else {
			rejected = append(rejected, candidate)
		}
	}
	out := candidates[:0]
	var diversity map[int]bool
	if includeOrigins {
		diversity = make(map[int]bool, len(selected))
		for _, candidate := range selected {
			diversity[candidate.nodeID] = true
		}
	}
	if !backfillEnabled {
		out = append(out, selected...)
		if captureQualityPostfill {
			return out, len(selected), 0, diversity, append([]vectorIndexCandidate(nil), rejected...)
		}
		return out, len(selected), 0, diversity, nil
	}
	backfill := minInt(limit-len(selected), len(rejected))
	if backfill == 0 {
		out = append(out, selected...)
		return out, len(selected), 0, diversity, nil
	}
	selectedPos := 0
	rejectedPos := 0
	for selectedPos < len(selected) && rejectedPos < backfill {
		if idx.compareVectorIndexCandidatesByDistanceLocked(selected[selectedPos], rejected[rejectedPos]) <= 0 {
			out = append(out, selected[selectedPos])
			selectedPos++
			continue
		}
		out = append(out, rejected[rejectedPos])
		rejectedPos++
	}
	out = append(out, selected[selectedPos:]...)
	out = append(out, rejected[rejectedPos:backfill]...)
	return out, len(selected), 0, diversity, nil
}

func (idx *VectorIndex) captureQualityPostfillCandidatesLocked(from int, candidates []vectorIndexCandidate) {
	if len(candidates) == 0 || from < 0 {
		return
	}
	if idx.qualityPostfillCandidates == nil {
		idx.qualityPostfillCandidates = make(map[int]map[int]struct{})
	}
	seen := idx.qualityPostfillCandidates[from]
	if seen == nil {
		seen = make(map[int]struct{}, len(candidates))
		idx.qualityPostfillCandidates[from] = seen
	}
	for _, candidate := range candidates {
		to := candidate.nodeID
		if to < 0 || to >= len(idx.nodes) || to == from {
			continue
		}
		seen[to] = struct{}{}
		// Distance is symmetric for the cosine-only offline policies.  Retain
		// the reverse endpoint too: this exact pair was scored during HNSW
		// construction, even if only `from` selected it at that time.
		reverse := idx.qualityPostfillCandidates[to]
		if reverse == nil {
			reverse = make(map[int]struct{})
			idx.qualityPostfillCandidates[to] = reverse
		}
		reverse[from] = struct{}{}
	}
}

// applyQualityPostfillLocked is the offline experiment's explicit final L0
// stage. It runs only after all insertion-side reciprocal pruning and before
// BFS locality remapping. Candidates are the union of finite construction
// comparisons observed in either direction; each row fills only its unused 2M
// outgoing capacity, ordered by maximum nearest-neighbour separation, then
// source distance and native ordinal.
func (idx *VectorIndex) applyQualityPostfillLocked(trace *vectorIndexConstructionTraceV1, degreeLimit int) error {
	if degreeLimit <= 0 {
		return nil
	}
	for from := range idx.nodes {
		if len(idx.nodes[from].neighbors) == 0 {
			continue
		}
		neighbors := idx.nodes[from].neighbors[0]
		target := minInt(degreeLimit, len(idx.nodes)-1)
		if len(neighbors) > target {
			return fmt.Errorf("collections: least-redundant separation postfill degree from=%d degree=%d limit=%d", from, len(neighbors), degreeLimit)
		}
		pool := idx.qualityPostfillCandidates[from]
		for len(neighbors) < target {
			present := make(map[int]struct{}, len(neighbors))
			for _, neighbor := range neighbors {
				present[neighbor.nodeID] = struct{}{}
			}
			best, bestMargin, bestDistance := -1, float32(math.Inf(-1)), float32(math.Inf(1))
			for to := range pool {
				if to < 0 || to >= len(idx.nodes) || to == from {
					continue
				}
				if _, exists := present[to]; exists {
					continue
				}
				distance, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[from], &idx.nodes[to])
				if err != nil {
					return err
				}
				margin := float32(math.Inf(1))
				for _, neighbor := range neighbors {
					separation, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[to], &idx.nodes[neighbor.nodeID])
					if err != nil {
						return err
					}
					if separation < margin {
						margin = separation
					}
				}
				if margin > bestMargin || (margin == bestMargin && (distance < bestDistance || (distance == bestDistance && to < best))) {
					best, bestMargin, bestDistance = to, margin, distance
				}
			}
			if best < 0 {
				break
			}
			neighbors = append(neighbors, vectorIndexNeighbor{nodeID: best, distance: bestDistance})
			if trace != nil {
				trace.init()
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: best, Layer: 0}
				if _, exists := trace.origins[key]; exists {
					return fmt.Errorf("collections: least-redundant separation postfill duplicate edge from=%d to=%d", from, best)
				}
				trace.origins[key] = "quality_postfill"
				trace.record(from, best, 0, "quality_postfill", "quality_postfill_add")
				if trace.postfillEdges == ^uint64(0) {
					return fmt.Errorf("collections: least-redundant separation postfill overflow")
				}
				trace.postfillEdges++
			}
		}
		idx.nodes[from].neighbors[0] = neighbors
		if len(neighbors) != target {
			return fmt.Errorf("collections: least-redundant separation postfill insufficient observed candidates from=%d degree=%d target=%d pool=%d", from, len(neighbors), target, len(pool))
		}
	}
	return nil
}

// applyRobustPruneRefinementLocked is the single #4172 causal follow-up. It
// applies DiskANN RobustPrune with the predeclared Euclidean alpha=1.2 to each completed
// L0 neighbourhood, using only its current edges and the bounded observed
// construction-pair pool. Classic RobustPrune may underfill R; a separately
// attributed nearest residual fill restores the fixed 2M encoded degree budget.
func (idx *VectorIndex) applyRobustPruneRefinementLocked(trace *vectorIndexConstructionTraceV1, degreeLimit int) error {
	// For normalized vectors this builder stores cosine distance, which is
	// proportional to squared Euclidean distance. DiskANN's alpha*d(a,b)
	// threshold therefore becomes alpha^2*cosineDistance(a,b), not alpha.
	const alphaSquared = float32(1.44)
	if degreeLimit <= 0 {
		return nil
	}
	type candidate struct {
		nodeID   int
		distance float32
	}
	for from := range idx.nodes {
		if len(idx.nodes[from].neighbors) == 0 {
			continue
		}
		target := minInt(degreeLimit, len(idx.nodes)-1)
		old := append([]vectorIndexNeighbor(nil), idx.nodes[from].neighbors[0]...)
		pool := make(map[int]struct{}, len(old)+len(idx.qualityPostfillCandidates[from]))
		for _, neighbor := range old {
			pool[neighbor.nodeID] = struct{}{}
		}
		for to := range idx.qualityPostfillCandidates[from] {
			pool[to] = struct{}{}
		}
		candidates := make([]candidate, 0, len(pool))
		for to := range pool {
			if to < 0 || to >= len(idx.nodes) || to == from {
				continue
			}
			distance, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[from], &idx.nodes[to])
			if err != nil {
				return err
			}
			candidates = append(candidates, candidate{nodeID: to, distance: distance})
		}
		slices.SortFunc(candidates, func(left, right candidate) int {
			if left.distance < right.distance {
				return -1
			}
			if left.distance > right.distance {
				return 1
			}
			return left.nodeID - right.nodeID
		})
		selected := make([]candidate, 0, target)
		remaining := append([]candidate(nil), candidates...)
		for len(remaining) != 0 && len(selected) < target {
			chosen := remaining[0]
			selected = append(selected, chosen)
			next := remaining[:0]
			for _, other := range remaining[1:] {
				separation, err := vectorDistanceBetweenFloat32NodesCosine(&idx.nodes[chosen.nodeID], &idx.nodes[other.nodeID])
				if err != nil {
					return err
				}
				if vectorIndexRobustPruneOccludesV1(alphaSquared, separation, other.distance) {
					continue
				}
				next = append(next, other)
			}
			remaining = next
		}
		// robust remains the pre-residual RobustPrune result; present grows with
		// residual fill so the provenance of those additional edges stays distinct.
		robust := make(map[int]struct{}, len(selected))
		for _, item := range selected {
			robust[item.nodeID] = struct{}{}
		}
		present := make(map[int]struct{}, len(selected))
		for _, item := range selected {
			present[item.nodeID] = struct{}{}
		}
		for _, item := range candidates {
			if len(selected) == target {
				break
			}
			if _, exists := present[item.nodeID]; exists {
				continue
			}
			selected = append(selected, item)
			present[item.nodeID] = struct{}{}
		}
		if len(selected) != target {
			return fmt.Errorf("collections: robust prune refinement insufficient candidates from=%d selected=%d target=%d", from, len(selected), target)
		}
		oldSet := make(map[int]struct{}, len(old))
		for _, neighbor := range old {
			oldSet[neighbor.nodeID] = struct{}{}
		}
		if trace != nil {
			trace.init()
			for _, neighbor := range old {
				if _, keep := present[neighbor.nodeID]; keep {
					continue
				}
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: neighbor.nodeID, Layer: 0}
				origin, ok := trace.origins[key]
				if !ok {
					return fmt.Errorf("collections: robust prune refinement missing origin from=%d to=%d", from, neighbor.nodeID)
				}
				delete(trace.origins, key)
				trace.record(from, neighbor.nodeID, 0, origin, "robust_prune_drop")
			}
			for _, item := range selected {
				if _, existed := oldSet[item.nodeID]; existed {
					continue
				}
				origin, action := "robust_prune_refinement", "robust_prune_add"
				if _, selectedByRobust := robust[item.nodeID]; !selectedByRobust {
					origin, action = "robust_prune_residual_fill", "robust_prune_residual_fill_add"
				}
				key := vectorIndexConstructionEdgeKeyV1{From: from, To: item.nodeID, Layer: 0}
				trace.origins[key] = origin
				trace.record(from, item.nodeID, 0, origin, action)
			}
		}
		neighbors := make([]vectorIndexNeighbor, len(selected))
		for i, item := range selected {
			neighbors[i] = vectorIndexNeighbor{nodeID: item.nodeID, distance: item.distance}
		}
		idx.nodes[from].neighbors[0] = neighbors
	}
	return nil
}

func vectorIndexRobustPruneOccludesV1(alphaSquared, chosenToCandidate, sourceToCandidate float32) bool {
	return alphaSquared*chosenToCandidate <= sourceToCandidate
}

func (idx *VectorIndex) vectorIndexCandidateIsDiverseLocked(candidate vectorIndexCandidate, selected []vectorIndexCandidate) bool {
	if idx.metric == VectorMetricCosine && candidate.nodeID >= 0 && candidate.nodeID < len(idx.nodes) {
		candidateNode := &idx.nodes[candidate.nodeID]
		if len(candidateNode.vector) > 0 && candidateNode.cachedInvNorm != 0 {
			for _, existing := range selected {
				distance := idx.distanceBetweenFloat32CosineCandidateAndNodeLocked(candidateNode, existing.nodeID)
				if math.IsInf(float64(distance), 1) {
					continue
				}
				if distance < candidate.distance {
					return false
				}
			}
			return true
		}
	}
	for _, existing := range selected {
		distance, ok := idx.distanceBetweenNodesFastLocked(candidate.nodeID, existing.nodeID)
		if !ok {
			distance = idx.distanceBetweenNodesLocked(candidate.nodeID, existing.nodeID)
		}
		if distance < candidate.distance {
			return false
		}
	}
	return true
}

func (idx *VectorIndex) distanceBetweenFloat32CosineCandidateAndNodeLocked(candidate *vectorIndexNode, existingNodeID int) float32 {
	if existingNodeID < 0 || existingNodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	existing := &idx.nodes[existingNodeID]
	if len(existing.vector) == len(candidate.vector) && existing.cachedInvNorm != 0 {
		return vectorDistanceBetweenFloat32NodesCosineUnchecked(candidate, existing)
	}
	distance, err := vectorDistanceBetweenStoredNodes(candidate, existing, idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

// sortVectorIndexCandidatesByDistanceLocked keeps the common already-sorted
// case allocation-free and handles the exactly-one-candidate-appended-at-tail
// case with insertion sort. Broader near-sorted shapes intentionally fall back
// to the full sort instead of carrying more repair logic in the hot path.
func (idx *VectorIndex) sortVectorIndexCandidatesByDistanceLocked(candidates []vectorIndexCandidate) {
	inversion := idx.firstVectorIndexCandidateInversionLocked(candidates)
	switch {
	case inversion == -1:
		return
	case inversion == len(candidates)-1:
		idx.insertTailVectorIndexCandidateByDistanceLocked(candidates)
	default:
		slices.SortFunc(candidates, idx.compareVectorIndexCandidatesByDistanceLocked)
	}
}

func (idx *VectorIndex) firstVectorIndexCandidateInversionLocked(candidates []vectorIndexCandidate) int {
	for i := 1; i < len(candidates); i++ {
		if idx.compareVectorIndexCandidatesByDistanceLocked(candidates[i-1], candidates[i]) > 0 {
			return i
		}
	}
	return -1
}

func (idx *VectorIndex) insertTailVectorIndexCandidateByDistanceLocked(candidates []vectorIndexCandidate) {
	tail := len(candidates) - 1
	if tail <= 0 {
		return
	}
	candidate := candidates[tail]
	insert := tail - 1
	for insert >= 0 && idx.compareVectorIndexCandidatesByDistanceLocked(candidates[insert], candidate) > 0 {
		candidates[insert+1] = candidates[insert]
		insert--
	}
	candidates[insert+1] = candidate
}

func (idx *VectorIndex) compareVectorIndexCandidatesByDistanceLocked(left, right vectorIndexCandidate) int {
	if left.distance < right.distance {
		return -1
	}
	if left.distance > right.distance {
		return 1
	}
	if left.nodeID >= 0 && left.nodeID < len(idx.nodes) && right.nodeID >= 0 && right.nodeID < len(idx.nodes) {
		if cmp := bytes.Compare(idx.nodes[left.nodeID].documentID, idx.nodes[right.nodeID].documentID); cmp != 0 {
			return cmp
		}
	}
	if left.nodeID < right.nodeID {
		return -1
	}
	if left.nodeID > right.nodeID {
		return 1
	}
	return 0
}

func (idx *VectorIndex) distanceToNodeLocked(query []float32, nodeID int) float32 {
	return idx.distanceToNodeWithQueryNormLocked(query, -1, nodeID)
}

func (idx *VectorIndex) distanceToNodeWithQueryNormLocked(query []float32, queryNormSquared float64, nodeID int) float32 {
	return idx.distanceToNodeWithPreparedQueryLocked(query, queryNormSquared, nil, nodeID)
}

func (idx *VectorIndex) distanceToNodeWithPreparedQueryLocked(query []float32, queryNormSquared float64, prepared *preparedFloat32CosineQuery, nodeID int) float32 {
	if nodeID < 0 || nodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	node := &idx.nodes[nodeID]
	if prepared != nil && idx.metric == VectorMetricCosine && len(node.vector) > 0 {
		return vectorDistanceToFloat32NodeCosineUnchecked(*prepared, node)
	}
	distance, err := vectorDistanceToStoredNodeWithQueryNorm(query, queryNormSquared, node, idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *VectorIndex) distanceBetweenNodesLocked(leftNodeID, rightNodeID int) float32 {
	if leftNodeID < 0 || leftNodeID >= len(idx.nodes) || rightNodeID < 0 || rightNodeID >= len(idx.nodes) {
		return float32(math.Inf(1))
	}
	distance, err := vectorDistanceBetweenStoredNodes(&idx.nodes[leftNodeID], &idx.nodes[rightNodeID], idx.metric)
	if err != nil {
		return float32(math.Inf(1))
	}
	return distance
}

func (idx *VectorIndex) distanceBetweenNodesFastLocked(leftNodeID, rightNodeID int) (float32, bool) {
	if leftNodeID < 0 || leftNodeID >= len(idx.nodes) || rightNodeID < 0 || rightNodeID >= len(idx.nodes) {
		return 0, false
	}
	left := &idx.nodes[leftNodeID]
	right := &idx.nodes[rightNodeID]
	if idx.metric == VectorMetricCosine && canUseUncheckedFloat32NodeCosine(left, right, idx.dimensions) {
		return vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right), true
	}
	distance, err := vectorDistanceBetweenStoredNodes(left, right, idx.metric)
	if err != nil {
		return 0, false
	}
	return distance, true
}

func vectorDistanceToStoredNode(query []float32, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	return vectorDistanceToStoredNodeWithQueryNorm(query, -1, node, metric)
}

func vectorDistanceToStoredNodeWithQueryNorm(query []float32, queryNormSquared float64, node *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := node.vectorDimensions()
	if len(query) != dims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query), dims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(node.vector) > 0 {
			return vectorDistanceToFloat32NodeCosine(query, queryNormSquared, node)
		}
		var dot float64
		for i, left := range query {
			right := node.vectorValueAt(i)
			dot += float64(left * right)
		}
		leftNorm := queryNormSquared
		if leftNorm < 0 {
			leftNorm = vectorNormSquared(query)
		}
		rightNorm := node.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i, left := range query {
			diff := float64(left - node.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i, left := range query {
			dot += float64(left * node.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

func vectorDistanceBetweenStoredNodes(left, right *vectorIndexNode, metric VectorMetric) (float32, error) {
	dims := left.vectorDimensions()
	rightDims := right.vectorDimensions()
	if dims != rightDims {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", dims, rightDims)
	}
	switch metric {
	case VectorMetricCosine:
		if len(left.vector) > 0 && len(right.vector) > 0 {
			return vectorDistanceBetweenFloat32NodesCosine(left, right)
		}
		var dot float64
		for i := 0; i < dims; i++ {
			leftValue := left.vectorValueAt(i)
			rightValue := right.vectorValueAt(i)
			dot += float64(leftValue * rightValue)
		}
		leftNorm := left.cachedNormSquared()
		rightNorm := right.cachedNormSquared()
		if leftNorm == 0 || rightNorm == 0 {
			return 0, errors.New("collections: cosine vector cannot have zero magnitude")
		}
		return float32(1 - dot/(math.Sqrt(leftNorm)*math.Sqrt(rightNorm))), nil
	case VectorMetricL2:
		var sum float64
		for i := 0; i < dims; i++ {
			diff := float64(left.vectorValueAt(i) - right.vectorValueAt(i))
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case VectorMetricInnerProduct:
		var dot float64
		for i := 0; i < dims; i++ {
			dot += float64(left.vectorValueAt(i) * right.vectorValueAt(i))
		}
		return float32(-dot), nil
	default:
		return 0, fmt.Errorf("collections: unsupported vector metric %d", metric)
	}
}

type preparedFloat32CosineQuery struct {
	vector      []float32
	normSquared float64
	invNorm     float32
}

func prepareFloat32CosineQuery(query []float32, queryNormSquared float64) (preparedFloat32CosineQuery, error) {
	leftNorm := queryNormSquared
	if leftNorm < 0 {
		leftNorm = vectorNormSquared(query)
	}
	if leftNorm == 0 {
		return preparedFloat32CosineQuery{}, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return preparedFloat32CosineQuery{
		vector:      query,
		normSquared: leftNorm,
		invNorm:     float32(1 / math.Sqrt(leftNorm)),
	}, nil
}

func vectorDistanceToFloat32NodeCosine(query []float32, queryNormSquared float64, node *vectorIndexNode) (float32, error) {
	prepared, err := prepareFloat32CosineQuery(query, queryNormSquared)
	if err != nil {
		return 0, err
	}
	return vectorDistanceToFloat32NodeCosinePrepared(prepared, node)
}

func vectorDistanceToFloat32NodeCosinePrepared(query preparedFloat32CosineQuery, node *vectorIndexNode) (float32, error) {
	if len(query.vector) != len(node.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(query.vector), len(node.vector))
	}
	if node.cachedInvNorm == 0 {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return vectorDistanceToFloat32NodeCosineUnchecked(query, node), nil
}

func vectorDistanceToFloat32NodeCosineUnchecked(query preparedFloat32CosineQuery, node *vectorIndexNode) float32 {
	n := len(query.vector)
	if n != len(node.vector) {
		panic(fmt.Sprintf("collections: vector dimensions differ: %d vs %d", n, len(node.vector)))
	}
	dot := dotProductFloat32ForCosine(query.vector, node.vector, query.normSquared, node.normSquared)
	return float32(1 - dot*float64(query.invNorm)*float64(node.cachedInvNorm))
}

func vectorDistanceBetweenFloat32NodesCosine(left, right *vectorIndexNode) (float32, error) {
	if len(left.vector) != len(right.vector) {
		return 0, fmt.Errorf("collections: vector dimensions differ: %d vs %d", len(left.vector), len(right.vector))
	}
	if !canUseUncheckedFloat32NodeCosine(left, right, 0) {
		return 0, errors.New("collections: cosine vector cannot have zero magnitude")
	}
	return vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right), nil
}

func canUseUncheckedFloat32NodeCosine(left, right *vectorIndexNode, dimensions int) bool {
	return len(left.vector) > 0 &&
		len(left.vector) == len(right.vector) &&
		(dimensions == 0 || len(left.vector) == dimensions) &&
		left.cachedInvNorm != 0 &&
		right.cachedInvNorm != 0
}

func vectorDistanceBetweenFloat32NodesCosineUnchecked(left, right *vectorIndexNode) float32 {
	dot := dotProductFloat32ForCosine(left.vector, right.vector, left.normSquared, right.normSquared)
	return float32(1 - dot*float64(left.cachedInvNorm)*float64(right.cachedInvNorm))
}

func dotProductFloat32ForCosine(left, right []float32, leftNormSquared, rightNormSquared float64) float64 {
	if safeFloat32DotProductForCosine(leftNormSquared, rightNormSquared) {
		return float64(vectorDotProductFloat32(left, right))
	}
	return dotProductFloat32Wide(left, right)
}

func safeFloat32DotProductForCosine(leftNormSquared, rightNormSquared float64) bool {
	if leftNormSquared <= 0 || rightNormSquared <= 0 {
		return false
	}
	const maxDot = float64(math.MaxFloat32)
	return leftNormSquared <= maxDot*maxDot/rightNormSquared
}

func dotProductFloat32Wide(left, right []float32) float64 {
	var dot float64
	for i := range left {
		dot += float64(left[i]) * float64(right[i])
	}
	return dot
}

func (node *vectorIndexNode) vectorDimensions() int {
	if len(node.vector) > 0 {
		return len(node.vector)
	}
	return len(node.quantized)
}

func (node *vectorIndexNode) vectorValueAt(i int) float32 {
	if len(node.vector) > 0 {
		return node.vector[i]
	}
	return float32(node.quantized[i]) * node.quantScale
}

func (node *vectorIndexNode) cachedNormSquared() float64 {
	if node.normSquared > 0 {
		return node.normSquared
	}
	return node.storedNormSquared()
}

func (node *vectorIndexNode) cacheVectorNorms() {
	node.normSquared = node.storedNormSquared()
	if node.normSquared > 0 {
		node.cachedInvNorm = float32(1 / math.Sqrt(node.normSquared))
	} else {
		node.cachedInvNorm = 0
	}
}

func (node *vectorIndexNode) storedNormSquared() float64 {
	var norm float64
	dims := node.vectorDimensions()
	for i := 0; i < dims; i++ {
		value := float64(node.vectorValueAt(i))
		norm += value * value
	}
	return norm
}

func (node *vectorIndexNode) vectorBytes() int {
	if len(node.quantized) > 0 {
		return len(node.quantized) + 4
	}
	return len(node.vector) * 4
}

type vectorIndexCandidate struct {
	nodeID   int
	distance float32
}

type vectorIndexSearchScratch struct {
	visitedEpochs    []uint32
	visitedEpoch     uint32
	explorationLimit int
	explored         int
	queue            vectorIndexMinCandidateHeap
	best             vectorIndexMaxCandidateHeap
	liveBest         vectorIndexMaxCandidateHeap
	out              []vectorIndexCandidate
}

func (scratch *vectorIndexSearchScratch) nextVisitedEpoch(nodes int) ([]uint32, uint32) {
	if cap(scratch.visitedEpochs) < nodes {
		scratch.visitedEpochs = make([]uint32, nodes, growVectorIndexScratchCapacity(cap(scratch.visitedEpochs), nodes))
	} else {
		scratch.visitedEpochs = scratch.visitedEpochs[:nodes]
	}
	scratch.visitedEpoch++
	if scratch.visitedEpoch == 0 {
		clear(scratch.visitedEpochs)
		scratch.visitedEpoch = 1
	}
	return scratch.visitedEpochs, scratch.visitedEpoch
}

func growVectorIndexScratchCapacity(current int, required int) int {
	next := current
	if next < 64 {
		next = 64
	}
	for next < required {
		next *= 2
	}
	return next
}

func sortVectorIndexCandidates(candidates []vectorIndexCandidate) {
	slices.SortFunc(candidates, func(left, right vectorIndexCandidate) int {
		if left.distance < right.distance {
			return -1
		}
		if left.distance > right.distance {
			return 1
		}
		if left.nodeID < right.nodeID {
			return -1
		}
		if left.nodeID > right.nodeID {
			return 1
		}
		return 0
	})
}

func vectorIndexCandidateLess(left, right vectorIndexCandidate) bool {
	if left.distance != right.distance {
		return left.distance < right.distance
	}
	return left.nodeID < right.nodeID
}

func vectorIndexCandidateWorse(left, right vectorIndexCandidate) bool {
	return vectorIndexCandidateLess(right, left)
}

type vectorIndexMinCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMinCandidateHeap) push(candidate vectorIndexCandidate) {
	*h = append(*h, candidate)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !vectorIndexCandidateLess((*h)[child], (*h)[parent]) {
			break
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h *vectorIndexMinCandidateHeap) pop() vectorIndexCandidate {
	out := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	h.down(0)
	return out
}

func (h vectorIndexMinCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateLess(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateLess(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

type vectorIndexMaxCandidateHeap []vectorIndexCandidate

func (h *vectorIndexMaxCandidateHeap) pushBounded(candidate vectorIndexCandidate, limit int) {
	if limit <= 0 {
		return
	}
	if len(*h) < limit {
		*h = append(*h, candidate)
		h.up(len(*h) - 1)
		return
	}
	if !vectorIndexCandidateLess(candidate, (*h)[0]) {
		return
	}
	(*h)[0] = candidate
	h.down(0)
}

func (h *vectorIndexMaxCandidateHeap) up(child int) {
	for child > 0 {
		parent := (child - 1) / 2
		if !vectorIndexCandidateWorse((*h)[child], (*h)[parent]) {
			return
		}
		(*h)[child], (*h)[parent] = (*h)[parent], (*h)[child]
		child = parent
	}
}

func (h vectorIndexMaxCandidateHeap) down(parent int) {
	for {
		left := parent*2 + 1
		if left >= len(h) {
			return
		}
		child := left
		right := left + 1
		if right < len(h) && vectorIndexCandidateWorse(h[right], h[left]) {
			child = right
		}
		if !vectorIndexCandidateWorse(h[child], h[parent]) {
			return
		}
		h[parent], h[child] = h[child], h[parent]
		parent = child
	}
}

// Stats returns a snapshot of in-memory vector index state.
func (idx *VectorIndex) Stats() VectorIndexStats {
	if idx == nil {
		return VectorIndexStats{}
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	stats := VectorIndexStats{
		Name:                idx.name,
		Field:               idx.field,
		Metric:              idx.metric,
		Encoding:            idx.encoding,
		Dimensions:          idx.dimensions,
		M:                   idx.m,
		EfConstruction:      idx.efConstruction,
		EfSearch:            idx.efSearch,
		Nodes:               len(idx.nodes),
		LiveDocs:            len(idx.currentNode),
		MaxLevel:            idx.maxLevel,
		Epoch:               idx.persistedEpoch,
		BytesDisk:           idx.persistedBytesDisk,
		SnapshotDirty:       idx.persistedSnapshotDirty || (idx.nativePersistent && idx.persistedEpoch == 0 && idx.mutationSeq != 0),
		LastRebuildDuration: idx.lastRebuildDuration,
		LiveANNFullRebuilds: idx.liveANNFullRebuilds,
	}
	var edges int
	var vectorBytes int64
	for i := range idx.nodes {
		node := idx.nodes[i]
		if node.deleted {
			stats.DeletedDocs++
		}
		for _, layerNeighbors := range node.neighbors {
			edges += len(layerNeighbors)
		}
		vectorBytes += int64(node.vectorBytes())
		vectorBytes += int64(len(node.documentID))
	}
	if stats.Nodes > 0 {
		stats.DeletedRatio = float64(stats.DeletedDocs) / float64(stats.Nodes)
		stats.AvgDegree = float64(edges) / float64(stats.Nodes)
	}
	// Approximate heap footprint; edge accounting tracks the neighbor struct
	// size but excludes slice headers and spare capacity.
	stats.BytesMemory = vectorBytes + int64(edges)*int64(unsafe.Sizeof(vectorIndexNeighbor{})) + int64(stats.Nodes*32)
	stats.RebuildNeeded = stats.DeletedRatio >= idx.rebuildDeletedRatio && stats.DeletedDocs > 0
	return stats
}

func (idx *VectorIndex) nativeSearchState() (epoch uint64, bytesDisk int64, liveDocs int, rebuildNeeded bool, fullRebuilds uint64) {
	if idx == nil {
		return 0, 0, 0, false, 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	liveDocs = len(idx.currentNode)
	deletedDocs := len(idx.nodes) - liveDocs
	rebuildNeeded = deletedDocs > 0 && float64(deletedDocs)/float64(len(idx.nodes)) >= idx.rebuildDeletedRatio
	return idx.persistedEpoch, idx.persistedBytesDisk, liveDocs, rebuildNeeded, idx.liveANNFullRebuilds
}

func (idx *VectorIndex) nativeMutationSequence() uint64 {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.mutationSeq
}

func (idx *VectorIndex) markLiveANNFullRebuild() {
	idx.mu.Lock()
	idx.liveANNFullRebuilds++
	idx.mu.Unlock()
}

// CheckRecall compares indexed search with exact search for the supplied query
// vectors and reports recall@TopK.
func (idx *VectorIndex) CheckRecall(queries [][]float32, opts VectorIndexSearchOptions) (VectorIndexRecall, error) {
	recall := VectorIndexRecall{Queries: len(queries), TopK: opts.TopK}
	if idx == nil {
		return recall, errors.New("collections: vector index is nil")
	}
	if opts.TopK <= 0 {
		return recall, errors.New("collections: vector search TopK must be positive")
	}
	exactBatch, usedBatch, err := idx.checkRecallExactBatch(queries, opts)
	if err != nil {
		return recall, err
	}
	recall.SearchTraces = make([]VectorIndexTrace, 0, len(queries))
	for i, query := range queries {
		var exact []VectorSearchResult
		if usedBatch {
			exact = exactBatch[i]
		} else {
			var err error
			exact, err = idx.collection.SearchVectorsExact(query, VectorSearchOptions{
				Field:            idx.field,
				Metric:           idx.metric,
				TopK:             opts.TopK,
				Filter:           opts.Filter,
				IndexRangeFilter: opts.IndexRangeFilter,
			})
			if err != nil {
				return recall, err
			}
		}
		searchOpts := opts
		searchOpts.DisableExactFallback = true
		ann, trace, err := idx.Search(query, searchOpts)
		if err != nil {
			return recall, err
		}
		recall.SearchTraces = append(recall.SearchTraces, trace)
		recall.ExactTotal += len(exact)
		recall.ANNTotal += len(ann)
		exactSet := make(map[string]struct{}, len(exact))
		for _, result := range exact {
			exactSet[string(result.DocumentID)] = struct{}{}
		}
		for _, result := range ann {
			if _, ok := exactSet[string(result.DocumentID)]; ok {
				recall.Overlap++
			}
		}
	}
	if recall.ExactTotal > 0 {
		recall.Recall = float64(recall.Overlap) / float64(recall.ExactTotal)
	}
	return recall, nil
}

func (idx *VectorIndex) checkRecallExactBatch(queries [][]float32, opts VectorIndexSearchOptions) ([][]VectorSearchResult, bool, error) {
	if len(queries) < 2 || opts.Filter != nil || opts.IndexRangeFilter != nil || idx.metric != VectorMetricCosine {
		return nil, false, nil
	}
	queryDims := len(queries[0])
	if queryDims == 0 {
		return nil, true, errors.New("collections: vector query cannot be empty")
	}
	if idx.dimensions != 0 && queryDims != idx.dimensions {
		return nil, true, fmt.Errorf("collections: vector query has dimension %d, want %d", queryDims, idx.dimensions)
	}
	for _, query := range queries {
		if len(query) != queryDims {
			return nil, true, fmt.Errorf("collections: vector query has dimension %d, want %d", len(query), queryDims)
		}
		if err := validateFloat32Vector(query); err != nil {
			return nil, true, fmt.Errorf("collections: vector query: %w", err)
		}
		queryNormSquared := vectorNormSquared(query)
		if queryNormSquared == 0 {
			return nil, true, errors.New("collections: cosine vector query cannot have zero magnitude")
		}
	}
	if err := idx.collection.flushBufferedWrites(); err != nil {
		return nil, true, err
	}

	materializer, err := idx.collection.NewStoredDocumentJSONMaterializer()
	if err != nil {
		return nil, true, err
	}
	defer func() { _ = materializer.Close() }()

	estimatedDocs := 0
	idx.mu.RLock()
	if liveDocs := len(idx.currentNode); liveDocs > 0 {
		estimatedDocs = liveDocs
	}
	idx.mu.RUnlock()
	matrixCap := 0
	if estimatedDocs <= maxCollectionInt/queryDims {
		matrixCap = estimatedDocs * queryDims
	}
	documentIDs := make([][]byte, 0, estimatedDocs)
	vectorMatrix := make([]float32, 0, matrixCap)
	documentNorms := make([]float64, 0, estimatedDocs)
	_, err = idx.collection.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		vector, ok, err := vectorFromStoredDocument(materializer, record.Document, idx.fieldPath)
		if err != nil {
			return false, fmt.Errorf("collections: vector field %q in document %q: %w", idx.field, record.ID, err)
		}
		if !ok {
			return true, nil
		}
		if len(vector) != queryDims {
			return false, fmt.Errorf("collections: vector field %q in document %q has dimension %d, want %d", idx.field, record.ID, len(vector), queryDims)
		}
		vectorNorm := vectorNormSquared(vector)
		if vectorNorm == 0 {
			return false, fmt.Errorf("collections: vector field %q in document %q: cosine vectors cannot have zero magnitude", idx.field, record.ID)
		}
		documentIDs = append(documentIDs, bytes.Clone(record.ID))
		vectorMatrix = append(vectorMatrix, vector...)
		documentNorms = append(documentNorms, vectorNorm)
		return true, nil
	})
	if err != nil {
		return nil, true, err
	}

	exact := make([][]VectorSearchResult, len(queries))
	if len(documentIDs) == 0 {
		for i := range exact {
			exact[i] = []VectorSearchResult{}
		}
		return exact, true, nil
	}
	if !cosineRecallBatchSafe(queries, documentNorms) {
		return nil, false, nil
	}

	maxBatchQueries := defaultVectorRecallBatchCells / len(documentIDs)
	if maxBatchQueries < 1 {
		maxBatchQueries = 1
	}
	for queryStart := 0; queryStart < len(queries); queryStart += maxBatchQueries {
		queryEnd := minInt(queryStart+maxBatchQueries, len(queries))
		queryBatch := queries[queryStart:queryEnd]
		queryMatrix := make([]float32, 0, len(queryBatch)*queryDims)
		for _, query := range queryBatch {
			queryMatrix = append(queryMatrix, query...)
		}
		distances := make([]float64, len(queryBatch)*len(documentIDs))
		angularDistancesFloat32Batch(queryMatrix, vectorMatrix, documentNorms, len(queryBatch), len(documentIDs), queryDims, distances)
		for batchIndex := range queryBatch {
			row := distances[batchIndex*len(documentIDs) : (batchIndex+1)*len(documentIDs)]
			matches := make([]VectorSearchResult, 0, opts.TopK)
			for docIndex, distance := range row {
				matches = appendBoundedVectorSearchResult(matches, VectorSearchResult{
					DocumentID: documentIDs[docIndex],
					Distance:   float32(distance),
				}, opts.TopK)
			}
			if matches == nil {
				matches = []VectorSearchResult{}
			}
			exact[queryStart+batchIndex] = matches
		}
	}
	return exact, true, nil
}

func cosineRecallBatchSafe(queries [][]float32, documentNorms []float64) bool {
	for _, query := range queries {
		queryNorm := vectorNormSquared(query)
		for _, documentNorm := range documentNorms {
			if !safeFloat32DotProductForCosine(queryNorm, documentNorm) {
				return false
			}
		}
	}
	return true
}

func vectorFromStoredDocument(materializer *StoredDocumentJSONMaterializer, document []byte, fieldPath []string) ([]float32, bool, error) {
	if materializer != nil && materializer.DocumentFormat() == DocumentFormatBSON {
		return vectorFromBSONField(document, fieldPath)
	}
	if materializer == nil {
		return nil, false, errors.New("collections: nil stored document materializer")
	}
	jsonDoc, err := materializer.StoredDocumentJSON(document)
	if err != nil {
		return nil, false, err
	}
	return vectorFromJSONField(jsonDoc, fieldPath)
}

// Rebuild removes tombstoned/superseded graph nodes by rebuilding the index from
// live canonical collection rows. It preserves vector index options and swaps
// the rebuilt graph into the receiver.
func (idx *VectorIndex) Rebuild() error {
	if idx == nil {
		return errors.New("collections: vector index is nil")
	}
	c := idx.collection
	if c == nil {
		return errCollectionNil
	}
	unlockMutation := c.lockMutation()
	defer unlockMutation.Unlock()
	if c.vectorIndexRuntimeIsStale(idx) {
		return fmt.Errorf("%w: %q", errVectorIndexStaleRuntime, idx.name)
	}
	start := time.Now()
	rebuilt, err := c.buildVectorIndex(VectorIndexOptions{
		Name:                idx.name,
		Field:               idx.field,
		Metric:              idx.metric,
		Encoding:            idx.encoding,
		Dimensions:          idx.dimensions,
		M:                   idx.m,
		EfConstruction:      idx.efConstruction,
		EfSearch:            idx.efSearch,
		RebuildDeletedRatio: idx.rebuildDeletedRatio,
	}, false)
	if err != nil {
		return err
	}
	rebuilt.mu.RLock()
	nodes := cloneVectorIndexNodes(rebuilt.nodes)
	currentNode := cloneVectorIndexCurrentNode(rebuilt.currentNode)
	entry := rebuilt.entry
	maxLevel := rebuilt.maxLevel
	dimensions := rebuilt.dimensions
	sourceDocumentGeneration := rebuilt.sourceDocumentGeneration
	sourceDocumentRootsValid := rebuilt.sourceDocumentRootsValid
	sourceDocumentState := rebuilt.sourceDocumentState
	sourceDocumentStateValid := rebuilt.sourceDocumentStateValid
	rebuilt.mu.RUnlock()

	idx.mu.Lock()
	idx.nodes = nodes
	idx.currentNode = currentNode
	idx.entry = entry
	idx.maxLevel = maxLevel
	idx.dimensions = dimensions
	idx.sourceDocumentGeneration = sourceDocumentGeneration
	idx.sourceDocumentRootsValid = sourceDocumentRootsValid
	idx.sourceDocumentState = sourceDocumentState
	idx.sourceDocumentStateValid = sourceDocumentStateValid
	idx.lastRebuildDuration = collectionObservedElapsedSince(start)
	idx.markGraphChangedLocked()
	idx.requireFullNativeSnapshotLocked()
	idx.publishSearchViewLocked(true)
	idx.mu.Unlock()
	c.RegisterVectorIndex(idx)
	if c.manager != nil && idx.needsNativeAutoPersist() {
		c.manager.registerCollectionHandle(c)
	}
	return nil
}

func cloneVectorIndexNodes(in []vectorIndexNode) []vectorIndexNode {
	out := make([]vectorIndexNode, len(in))
	for i := range in {
		out[i] = vectorIndexNode{
			documentID:    bytes.Clone(in[i].documentID),
			vector:        append([]float32(nil), in[i].vector...),
			quantized:     append([]int8(nil), in[i].quantized...),
			quantScale:    in[i].quantScale,
			normSquared:   in[i].normSquared,
			cachedInvNorm: in[i].cachedInvNorm,
			level:         in[i].level,
			deleted:       in[i].deleted,
			neighbors:     make([][]vectorIndexNeighbor, len(in[i].neighbors)),
		}
		for layer := range in[i].neighbors {
			out[i].neighbors[layer] = append([]vectorIndexNeighbor(nil), in[i].neighbors[layer]...)
		}
	}
	return out
}

func cloneVectorIndexCurrentNode(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
