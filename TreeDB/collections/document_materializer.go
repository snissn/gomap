package collections

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync/atomic"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// ErrVectorIndexSnapshotMismatch reports that a buffered native vector search
// cannot be materialized from a read view with the same document visibility.
var ErrVectorIndexSnapshotMismatch = errors.New("collections: vector index search snapshot mismatch")

// DocumentFetchOptions configures snapshot-bound document materialization. The
// zero value preserves Collection.Get-style full-document output and verified
// column-asset reads. Projection paths are explicit JSON top-level fields:
// IncludePaths is an allowlist when non-empty, ExcludePaths wins over includes,
// missing fields are ignored, and present JSON null values are preserved. The
// same projection contract is applied to retained payload fields, typed-row
// asset fields, and typed-column-part fields.
type DocumentFetchOptions struct {
	// Context optionally cancels batch document materialization.
	Context context.Context
	// IncludePaths is an optional allowlist of top-level JSON fields to return.
	// When non-empty, fields not listed here are skipped. Nested projection paths
	// are intentionally unsupported for this pre-alpha API and fail closed.
	IncludePaths []string
	// ExcludePaths is an optional denylist of top-level JSON fields to skip.
	// Excludes take precedence over IncludePaths.
	ExcludePaths []string
	// Format selects the materialized document format. The zero value preserves
	// collection-default output; projected fetches currently require JSON output.
	Format DocumentFormat
	// ColumnAssetReadIntegrity controls typed-row/physical row asset reads used
	// to locate or point-fetch the visible row for a document. Typed-column part
	// reconstruction uses the prepared read-view cache with verified reads.
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

// DocumentRowRef identifies a document row in a snapshot-bound typed-storage
// materialization request. Refs are produced by typed-storage locator lookups or
// by FetchDocumentsByID scan reconstruction and are validated against the
// decoded physical row before materialization.
type DocumentRowRef struct {
	DocumentID        []byte
	Generation        uint64
	PartID            uint64
	RowIndex          int
	AppliedCommandLSN uint64
}

// DocumentFetchResult is one materialized document result. ID and Document are
// response-owned slices; non-empty Document slices are cap-limited so appending
// to one result cannot mutate another result in the same response. Missing
// documents have Found=false and Document=nil. RowRef is populated only when
// typed-storage reconstruction found a visible physical row; it is zero for
// retained-payload-only results.
type DocumentFetchResult struct {
	ID       []byte
	Document []byte
	Found    bool
	RowRef   DocumentRowRef
}

// DocumentMaterializationStats attributes the work performed by a
// CollectionReadView fetch. Counters describe the fetch call; AssetActiveHandles
// is the read view's current mappedresource handle count after the fetch.
type DocumentMaterializationStats struct {
	DocumentsRequested  uint64
	DocumentsFetched    uint64
	DocumentsMissing    uint64
	DocumentBytes       uint64
	OutputBytes         uint64
	FieldsReconstructed uint64
	FieldsSkipped       uint64
	// EmbeddingVectorReads/Bytes count canonical FP32 vectors read solely for
	// final document output. EmbeddingOutputBytes counts their encoded JSON field.
	EmbeddingVectorReads uint64
	EmbeddingVectorBytes uint64
	EmbeddingOutputBytes uint64
	FetchNanos           int64

	RetainedPayloadFetches uint64
	RetainedPayloadBytes   uint64

	VisibilityScans         uint64
	VisibilityRowsScanned   uint64
	VisibilityRows          uint64
	VisibilityPhysicalBytes int64
	VisibilityNanos         int64

	TypedColumnRows        uint64
	TypedColumnCacheHits   uint64
	TypedColumnCacheMisses uint64
	TypedColumnPartLoads   uint64
	TypedColumnPartDecodes uint64
	TypedColumnNanos       int64

	JSONReconstructionRows  uint64
	JSONReconstructionNanos int64

	RowLocatorBuilds        uint64
	RowLocatorLookups       uint64
	RowLocatorMisses        uint64
	RowLocatorRowsScanned   uint64
	RowLocatorPhysicalBytes int64
	RowLocatorNanos         int64

	PointRowFetches uint64
	PointRowDecodes uint64

	RowRefFallbackScans      uint64
	RowRefUnsupported        uint64
	RowRefValidationFailures uint64

	AssetMmapHits        uint64
	AssetReadAtFallbacks uint64
	AssetFileOpens       uint64
	AssetFileCloses      uint64
	AssetActiveHandles   int64
}

// DocumentFetchResponse contains ordered materialization results and per-call
// diagnostics.
type DocumentFetchResponse struct {
	Results []DocumentFetchResult
	Stats   DocumentMaterializationStats
}

// DocumentRowRefLookupResult is one ordered document-row locator lookup result.
// Missing or deleted documents have Found=false and a zero RowRef.
type DocumentRowRefLookupResult struct {
	ID     []byte
	RowRef DocumentRowRef
	Found  bool
}

// DocumentRowRefLookupResponse contains ordered row-ref lookup results and
// diagnostics for the snapshot-derived locator work.
type DocumentRowRefLookupResponse struct {
	Results []DocumentRowRefLookupResult
	Stats   DocumentMaterializationStats
}

var collectionReadViewScopeSeq atomic.Uint64

type documentRowPartKey struct {
	Generation uint64
	PartID     uint64
}

// CollectionReadView is a closeable snapshot-bound document materializer for a
// collection. It preserves the catalog/root visibility that existed when the
// view was opened. Returned documents are owned by the fetch response. The view
// is not concurrency-safe; callers that fetch concurrently should open one view
// per worker or synchronize externally.
const documentPointRowMaxBorrowedBlocks = 32

type CollectionReadView struct {
	collection      *Collection
	snapshot        *backenddb.Snapshot
	catalog         *collectionCatalog
	ownsSnap        bool
	closed          bool
	typedGraphOwner *typedGraphReadOwner

	assetScopeKind                  mappedresource.ScopeKind
	assetScopeID                    string
	assetManager                    *mappedresource.Manager
	assetClosedCounters             documentMaterializerAssetCounters
	rowAssetReadCache               *columnPhysicalAssetReadCache
	rowAssetReadIntegrity           ColumnAssetReadIntegrity
	typedColumnAssetReadCache       *columnPhysicalAssetReadCache
	typedColumnReconstructionCache  *typedColumnPartReconstructionCache
	columnSnapshotView              *columnPhysicalScanSnapshotView
	preparedMaterializer            *columnPhysicalScanSnapshotView // immutable, fully validated publication metadata
	pointRowRefs                    map[documentRowPartKey]columnManifestAssetRefForScan
	pointRowBlocks                  map[documentRowPartKey]*columnPhysicalRowReaderBlock
	pointRowCreditLimit             int64
	pointRowCreditUsed              int64
	pointRowWorkspaceCredit         int64
	pointRowDescriptorCredit        int64
	pointRowMaxBlockBytes           int64
	pointRowCacheEvictions          uint64
	rowEmissionScratch              *documentRowEmissionScratch
	pointRowProjection              *columnPhysicalScanProjection
	orderedPointRowRefs             bool // ephemeral GetInto and bounded range views avoid a full part lookup map
	validatedPointRowRefs           *columnPhysicalScanSnapshotView
	forceAssetReadAtFallbackForTest bool
}

// Meta returns an owned copy of metadata from this view's captured catalog,
// not the collection handle's latest catalog. It remains bound to this view
// across subsequent data and schema publications.
func (v *CollectionReadView) Meta() (CollectionMeta, error) {
	if err := v.validateOpen(); err != nil {
		return CollectionMeta{}, err
	}
	return *v.catalog.meta.copy(), nil
}

// VisitIndexValueIDs visits every ID equal to value in the captured scalar
// index. IDs are borrowed until the callback returns. Callback errors stop the
// scan and are returned; a missing index is an error, not an empty result.
func (v *CollectionReadView) VisitIndexValueIDs(indexName string, value any, visit func([]byte) error) error {
	if err := v.validateOpen(); err != nil {
		return err
	}
	if visit == nil {
		return errors.New("collections: nil index ID visitor")
	}
	endForegroundRead := v.beginForegroundRead()
	defer endForegroundRead()
	lookup := hybridScalarLookupView{snapshot: v.snapshot, catalog: v.catalog}
	_, truncated, err := lookup.visitLeafIDs(HybridScalarFilter{IndexName: indexName, Value: value}, 0, 0, nil, visit)
	if err != nil {
		return err
	}
	if truncated {
		return errors.New("collections: index ID visit was truncated")
	}
	return nil
}

// OpenCollectionReadView opens a snapshot-bound document materializer. Buffered
// writes are flushed before the snapshot is acquired so the view matches normal
// Collection.Get visibility at open time; later writes are not visible through
// the view.
func (c *Collection) OpenCollectionReadView() (*CollectionReadView, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if err := c.flushBufferedWrites(); err != nil {
		return nil, err
	}
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
	view := newCollectionReadViewAtSnapshot(c, snap, catalog, true, mappedresource.ScopeCollectionReadView)
	snap.DetachForegroundRead()
	closeOnErr = false
	return view, nil
}

// OpenCollectionReadViewForVectorIndexSearch opens a document read view and
// validates it against the opaque combined native publication identity carried
// by response. The identity is intentionally not part of the public wire shape.
func (c *Collection) OpenCollectionReadViewForVectorIndexSearch(response VectorIndexSearchResponse) (*CollectionReadView, error) {
	view, err := c.OpenCollectionReadView()
	if err != nil {
		return nil, err
	}
	endForegroundRead := view.beginForegroundRead()
	defer endForegroundRead()
	closeWithError := func(err error) (*CollectionReadView, error) {
		return nil, errors.Join(err, view.Close())
	}
	visibility := response.visibility
	if visibility.runtime == nil ||
		visibility.collectionName != view.catalog.meta.Name ||
		visibility.indexName != response.IndexName ||
		visibility.strategy != response.Strategy {
		return closeWithError(fmt.Errorf("%w: response has no matching native visibility identity", ErrVectorIndexSnapshotMismatch))
	}
	def, ok := findVectorIndex(view.catalog.meta.VectorIndexes, visibility.indexName)
	if !ok ||
		def.Strategy != visibility.strategy ||
		def.SchemaGeneration != visibility.schemaGeneration ||
		c.registeredVectorIndex(visibility.indexName) != visibility.runtime {
		return closeWithError(fmt.Errorf("%w: vector index %q identity changed", ErrVectorIndexSnapshotMismatch, visibility.indexName))
	}
	if !visibility.runtime.nativeSearchStateCoversCurrentDocuments(vectorIndexNativeSearchState{
		mutationSeq:              visibility.mutationSeq,
		sourceDocumentGeneration: visibility.sourceDocumentGeneration,
	}) {
		return closeWithError(fmt.Errorf("%w: vector index %q publication identity changed", ErrVectorIndexSnapshotMismatch, visibility.indexName))
	}
	generation, err := vectorIndexDocumentGeneration(view.snapshot, view.catalog)
	if err != nil {
		return closeWithError(err)
	}
	if generation != visibility.sourceDocumentGeneration {
		return closeWithError(fmt.Errorf(
			"%w: vector index %q searched document generation %d but read view has generation %d",
			ErrVectorIndexSnapshotMismatch,
			visibility.indexName,
			visibility.sourceDocumentGeneration,
			generation,
		))
	}
	return view, nil
}

// SearchVectorIndexWithBufferReadView searches and opens the matching document
// snapshot while excluding coverage mutations across the combined operation.
func (c *Collection) SearchVectorIndexWithBufferReadView(opts VectorIndexSearchOptions, buffer *VectorIndexSearchBuffer) (VectorIndexSearchResponse, *CollectionReadView, error) {
	if c.typedGraphServingPolicy() != nil {
		return c.searchTypedGraphServing(opts, buffer, true)
	}
	unlock := c.lockVectorIndexCoveragePersistence()
	defer unlock()
	response, err := c.searchVectorIndexWithBuffer(opts, buffer, true)
	if err != nil {
		return response, nil, err
	}
	view, err := c.OpenCollectionReadViewForVectorIndexSearch(response)
	if err != nil {
		return response, nil, err
	}
	return response, view, nil
}

func newCollectionReadViewAtSnapshot(c *Collection, snap *backenddb.Snapshot, catalog *collectionCatalog, ownsSnap bool, scopeKind mappedresource.ScopeKind) *CollectionReadView {
	if scopeKind == "" {
		scopeKind = mappedresource.ScopeCollectionReadView
	}
	return &CollectionReadView{
		collection:     c,
		snapshot:       snap,
		catalog:        catalog,
		ownsSnap:       ownsSnap,
		assetScopeKind: scopeKind,
		assetScopeID:   newCollectionReadViewScopeID(scopeKind),
	}
}

func newCollectionReadViewScopeID(kind mappedresource.ScopeKind) string {
	seq := collectionReadViewScopeSeq.Add(1)
	return fmt.Sprintf("%s-%d", kind, seq)
}

func (v *CollectionReadView) beginForegroundRead() func() {
	if v == nil || v.collection == nil || v.collection.db == nil {
		return noCollectionForegroundReadEnd
	}
	return v.collection.db.BeginForegroundRead()
}

// Close releases the snapshot owned by the read view. Views that are bound to a
// caller-owned snapshot still become closed, but leave snapshot release to the
// owner.
func (v *CollectionReadView) Close() error {
	if v == nil || v.closed {
		return nil
	}
	v.closed = true
	cacheErr := v.closeAssetReadCaches()
	var snapErr error
	if v.ownsSnap && v.snapshot != nil {
		snapErr = v.snapshot.Close()
		v.snapshot = nil
	}
	var ownerErr error
	if owner := v.typedGraphOwner; owner != nil {
		v.typedGraphOwner = nil
		ownerErr = owner.Close()
	}
	v.snapshot = nil
	v.catalog = nil
	return errors.Join(cacheErr, snapErr, ownerErr)
}

// FetchDocumentsByID materializes full documents for ids in input order. Missing
// documents produce Found=false results without failing the whole fetch.
func (v *CollectionReadView) FetchDocumentsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	endForegroundRead := v.beginForegroundRead()
	defer endForegroundRead()
	start := time.Now()
	response, err := v.fetchDocumentsByID(ids, nil, opts)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
}

func (v *CollectionReadView) validateOpen() error {
	if v == nil || v.collection == nil {
		return errors.New("collections: nil collection read view")
	}
	if v.closed {
		return errors.New("collections: collection read view is closed")
	}
	if v.collection.db == nil || v.collection.db.IsClosing() {
		return backenddb.ErrClosed
	}
	if v.snapshot == nil || v.catalog == nil {
		return errors.New("collections: nil collection read view")
	}
	return nil
}

type documentMaterializerAssetCounters struct {
	mmapHits        uint64
	readAtFallbacks uint64
	fileOpens       uint64
	fileCloses      uint64
	servingBorrows  uint64
	activeHandles   int64
}

// materializerServingSourceAccess returns the one non-owning pool access held
// by a public typed-serving read view. The view's typedGraphOwner owns the
// complete serving capability and closes both materializer caches before that
// capability, so no per-read pool ref is needed. Zero-row serving has no
// physical holder and deliberately keeps its established local materializer.
func (v *CollectionReadView) materializerServingSourceAccess() (*columnVectorGraphSourceAccess, error) {
	if v == nil || v.typedGraphOwner == nil {
		return nil, nil
	}
	owner := v.typedGraphOwner
	if owner.closed || owner.overlay == nil || owner.overlay.current != v || owner.overlay.base == nil || owner.overlay.base.reader == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	reader := owner.overlay.base.reader
	if reader.RowCount() == 0 && reader.sharedServingHolder == nil && reader.sharedPreparedSearch == nil {
		return nil, nil
	}
	capability := reader.sharedServingHolder
	if capability == nil || capability.pin == nil || capability.ref == nil || reader.sharedPreparedSearch != capability.ref {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	ref := capability.ref
	holder := ref.holder
	if ref.key.family != columnVectorGraphSharedPreparedSearchKeyServing || holder == nil || holder.key != ref.key || holder.servingSegments == nil || holder.servingSourceAccess == nil || holder.servingSourceAccess.pool != holder.servingSegments {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	return holder.servingSourceAccess, nil
}

func (v *CollectionReadView) ensureAssetReadCaches(cfg ColumnStoreConfig, rowIntegrity ColumnAssetReadIntegrity) error {
	if v == nil || v.collection == nil || v.collection.db == nil {
		return errors.New("collections: nil collection read view")
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: document materializer requires column asset manager metadata")
	}
	rowIntegrity = normalizeDocumentMaterializerReadIntegrity(rowIntegrity)
	if v.assetManager == nil {
		v.assetManager = mappedresource.NewManager()
	}
	rootDir := v.collection.db.ColumnAssetRootDir()
	namespace := cfg.AssetManager.Namespace
	servingAccess, err := v.materializerServingSourceAccess()
	if err != nil {
		return err
	}
	if v.rowAssetReadCache != nil && v.rowAssetReadIntegrity == rowIntegrity && v.rowAssetReadCache.namespace == namespace {
		if err := v.rowAssetReadCache.useServingSourceAccess(servingAccess); err != nil {
			return err
		}
	} else {
		if v.rowAssetReadCache != nil {
			v.clearDerivedRowFetchCaches()
			if err := v.rowAssetReadCache.close(); err != nil {
				return err
			}
			v.assetClosedCounters.addReadCache(v.rowAssetReadCache)
			v.rowAssetReadCache = nil
		}
		readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(rootDir, namespace, rowIntegrity)
		if err != nil {
			return err
		}
		readCache.returnViews = true
		readCache.boundedRangeViews = true
		readCache.admitFile = v.admitPointRowFile
		readCache.forceReadAtFallback = v.forceAssetReadAtFallbackForTest
		readCache.trustCachedVerifyFileIdentity = true
		if err := readCache.useMappedResourceManager(v.assetManager, v.assetScope(cfg, "typed-row document materializer"), "document materializer typed-row asset read"); err != nil {
			_ = readCache.close()
			return err
		}
		if err := readCache.useServingSourceAccess(servingAccess); err != nil {
			_ = readCache.close()
			return err
		}
		v.rowAssetReadCache = &readCache
		v.rowAssetReadIntegrity = rowIntegrity
	}
	if v.typedColumnAssetReadCache != nil && v.typedColumnAssetReadCache.namespace == namespace {
		if err := v.typedColumnAssetReadCache.useServingSourceAccess(servingAccess); err != nil {
			return err
		}
	} else {
		if v.typedColumnAssetReadCache != nil {
			v.typedColumnReconstructionCache = nil
			if err := v.typedColumnAssetReadCache.close(); err != nil {
				return err
			}
			v.assetClosedCounters.addReadCache(v.typedColumnAssetReadCache)
			v.typedColumnAssetReadCache = nil
		}
		readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(rootDir, namespace, ColumnAssetReadIntegrityVerify)
		if err != nil {
			return err
		}
		readCache.returnViews = true
		readCache.boundedRangeViews = true
		readCache.admitFile = v.admitPointRowFile
		readCache.forceReadAtFallback = v.forceAssetReadAtFallbackForTest
		readCache.trustCachedVerifyFileIdentity = true
		if err := readCache.useMappedResourceManager(v.assetManager, v.assetScope(cfg, "typed-column document materializer"), "document materializer typed-column asset read"); err != nil {
			_ = readCache.close()
			return err
		}
		if err := readCache.useServingSourceAccess(servingAccess); err != nil {
			_ = readCache.close()
			return err
		}
		v.typedColumnAssetReadCache = &readCache
	}
	return nil
}

func normalizeDocumentMaterializerReadIntegrity(in ColumnAssetReadIntegrity) ColumnAssetReadIntegrity {
	if in == "" {
		return ColumnAssetReadIntegrityVerify
	}
	return in
}

func (v *CollectionReadView) assetScope(cfg ColumnStoreConfig, reason string) mappedresource.Scope {
	kind := mappedresource.ScopeCollectionReadView
	id := "collection_read_view"
	if v != nil {
		if v.assetScopeKind != "" {
			kind = v.assetScopeKind
		}
		if v.assetScopeID != "" {
			id = v.assetScopeID
		}
	}
	generation := uint64(0)
	if cfg.ActiveManifest != nil {
		generation = cfg.ActiveManifest.Generation
	}
	namespace := ""
	if cfg.AssetManager != nil {
		namespace = cfg.AssetManager.Namespace
	}
	collectionName := ""
	if v != nil && v.catalog != nil {
		collectionName = v.catalog.meta.Name
	}
	return mappedresource.Scope{
		Kind:       kind,
		ID:         id,
		Collection: collectionName,
		Namespace:  namespace,
		Generation: generation,
		Reason:     reason,
	}
}

func (v *CollectionReadView) assetCounters() documentMaterializerAssetCounters {
	var out documentMaterializerAssetCounters
	if v == nil {
		return out
	}
	out = v.assetClosedCounters
	if v.rowAssetReadCache != nil {
		stats := v.rowAssetReadCache.lifecycleStats()
		out.mmapHits += stats.MmapHits
		out.readAtFallbacks += stats.ReadAtFallbacks
		out.fileOpens += stats.FileOpens
		out.fileCloses += stats.FileCloses
		out.servingBorrows += stats.ServingBorrows
	}
	if v.typedColumnAssetReadCache != nil {
		stats := v.typedColumnAssetReadCache.lifecycleStats()
		out.mmapHits += stats.MmapHits
		out.readAtFallbacks += stats.ReadAtFallbacks
		out.fileOpens += stats.FileOpens
		out.fileCloses += stats.FileCloses
		out.servingBorrows += stats.ServingBorrows
	}
	if v.assetManager != nil {
		out.activeHandles = v.assetManager.ActiveHandles()
	}
	return out
}

func (c *documentMaterializerAssetCounters) addReadCache(readCache *columnPhysicalAssetReadCache) {
	if c == nil || readCache == nil {
		return
	}
	stats := readCache.lifecycleStats()
	c.mmapHits += stats.MmapHits
	c.readAtFallbacks += stats.ReadAtFallbacks
	c.fileOpens += stats.FileOpens
	c.fileCloses += stats.FileCloses
	c.servingBorrows += stats.ServingBorrows
}

func addDocumentMaterializerAssetCounterDeltas(stats *DocumentMaterializationStats, before, after documentMaterializerAssetCounters) {
	if stats == nil {
		return
	}
	stats.AssetMmapHits += deltaUint64(before.mmapHits, after.mmapHits)
	stats.AssetReadAtFallbacks += deltaUint64(before.readAtFallbacks, after.readAtFallbacks)
	stats.AssetFileOpens += deltaUint64(before.fileOpens, after.fileOpens)
	stats.AssetFileCloses += deltaUint64(before.fileCloses, after.fileCloses)
	stats.AssetActiveHandles = after.activeHandles
}

func (v *CollectionReadView) closeAssetReadCaches() error {
	if v == nil {
		return nil
	}
	v.clearDerivedRowFetchCaches()
	var closeErr error
	if v.rowAssetReadCache != nil {
		closeErr = errors.Join(closeErr, v.rowAssetReadCache.close())
		v.assetClosedCounters.addReadCache(v.rowAssetReadCache)
		v.rowAssetReadCache = nil
	}
	if v.typedColumnAssetReadCache != nil {
		closeErr = errors.Join(closeErr, v.typedColumnAssetReadCache.close())
		v.assetClosedCounters.addReadCache(v.typedColumnAssetReadCache)
		v.typedColumnAssetReadCache = nil
	}
	v.typedColumnReconstructionCache = nil
	return closeErr
}

func (v *CollectionReadView) clearDerivedRowFetchCaches() {
	if v == nil {
		return
	}
	v.pointRowBlocks = nil
	v.rowEmissionScratch = nil
	v.pointRowCreditUsed = 0
	v.pointRowWorkspaceCredit = 0
	v.pointRowDescriptorCredit = 0
	v.pointRowCreditLimit = 0
	v.pointRowMaxBlockBytes = 0
	v.columnSnapshotView = nil
	v.preparedMaterializer = nil
	if v.typedColumnReconstructionCache != nil {
		v.typedColumnReconstructionCache.Prepared = nil
	}
	v.pointRowRefs = nil
	v.validatedPointRowRefs = nil
	v.pointRowProjection = nil
}

// LookupDocumentRowRefsByID resolves document IDs to snapshot-visible typed-row
// refs in input order using the read view's persisted point locator. Missing
// or deleted documents return Found=false.
func (v *CollectionReadView) LookupDocumentRowRefsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentRowRefLookupResponse, error) {
	endForegroundRead := v.beginForegroundRead()
	defer endForegroundRead()
	start := time.Now()
	response, err := v.lookupDocumentRowRefsByID(ids, opts)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
}

// visitDocumentRowRefsByID borrows IDs for synchronous consumption. Its caller
// owns the read-view pin and foreground-read envelope for the whole operation.
func (v *CollectionReadView) visitDocumentRowRefsByID(ids [][]byte, visit func([]byte, DocumentRowRef, bool) error) (DocumentMaterializationStats, error) {
	return v.visitDocumentRowRefsByIDMode(ids, visit, false)
}

func (v *CollectionReadView) visitDocumentScoringRowRefsByID(ids [][]byte, visit func([]byte, DocumentRowRef, bool) error) (DocumentMaterializationStats, error) {
	return v.visitDocumentRowRefsByIDMode(ids, visit, true)
}

func (v *CollectionReadView) visitDocumentRowRefsByIDMode(ids [][]byte, visit func([]byte, DocumentRowRef, bool) error, scoring bool) (DocumentMaterializationStats, error) {
	decode := decodeColumnPrimaryRowLocatorBorrowedID
	if scoring {
		decode = decodeColumnScoringRowLocatorBorrowedID
	}
	if err := v.validateOpen(); err != nil {
		return DocumentMaterializationStats{}, err
	}
	if len(ids) == 0 {
		return DocumentMaterializationStats{}, nil
	}
	stats := DocumentMaterializationStats{DocumentsRequested: uint64(len(ids))}
	if !columnStoreCanReconstructDocument(v.catalog.meta) {
		stats.RowRefUnsupported++
		return stats, errors.New("collections: document row ref lookup requires typed-storage reconstruction support")
	}
	for i, id := range ids {
		if len(id) == 0 {
			return stats, fmt.Errorf("collections: document id at position %d cannot be empty", i)
		}
	}
	if visit == nil {
		return stats, errors.New("collections: nil row ref visitor")
	}
	locatorRootName := collectionColumnRowLocatorRootName(v.catalog.meta.Name)
	if v.catalog.rootID(locatorRootName) == 0 {
		primaryRootName := collectionPrimaryRootName(v.catalog.meta.Name)
		if v.catalog.rootID(primaryRootName) != 0 || len(v.catalog.overlayRootIDs(primaryRootName)) != 0 {
			return stats, fmt.Errorf("collections: primary row locator root is absent for collection %q", v.catalog.meta.Name)
		}
		for _, id := range ids {
			stats.RowLocatorLookups++
			stats.RowLocatorMisses++
			if err := visit(id, DocumentRowRef{}, false); err != nil {
				return stats, err
			}
		}
		return stats, nil
	}
	// Match the existing tree batch reader's minimum grouping size. Small final-K
	// lookups and overlay roots retain their direct point-read path.
	if len(ids) >= 64 && len(v.catalog.overlayRootIDs(locatorRootName)) == 0 {
		return v.visitGroupedDocumentRowRefsByID(locatorRootName, ids, visit, stats, scoring)
	}
	var scratch []byte
	for _, id := range ids {
		stats.RowLocatorLookups++
		value, found, err := collectionGetAppendAtCatalogRoot(v.snapshot, v.catalog, locatorRootName, id, scratch[:0])
		if err != nil {
			return stats, fmt.Errorf("collections: primary row locator lookup for id %q: %w", string(id), err)
		}
		var ref DocumentRowRef
		if found {
			scratch = value
			ref, err = decode(id, value)
			if err != nil {
				return stats, err
			}
		} else {
			stats.RowLocatorMisses++
		}
		// Neither the borrowed ID nor the reusable locator value escapes through
		// the response. Internal visitors must consume the ref synchronously.
		if err := visit(id, ref, found); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

// visitGroupedDocumentRowRefsByID bounds read-ahead to one 512-ID chunk. Storage
// callbacks may arrive out of order; only copied scalar coordinates survive them.
// Lookup/miss counters include resolved read-ahead, not unfinished tree traversal.
// Caller mapping work advances only during ordered delivery and stops at its
// first error. A storage error aborts delivery of the current chunk.
func (v *CollectionReadView) visitGroupedDocumentRowRefsByID(root string, ids [][]byte, visit func([]byte, DocumentRowRef, bool) error, stats DocumentMaterializationStats, scoring bool) (DocumentMaterializationStats, error) {
	decode := decodeColumnPrimaryRowLocatorBorrowedID
	if scoring {
		decode = decodeColumnScoringRowLocatorBorrowedID
	}
	type coordinates struct {
		generation, partID uint64
		rowIndex           int
		appliedCommandLSN  uint64
	}
	refs := make([]coordinates, min(512, len(ids)))
	for start := 0; start < len(ids); start += len(refs) {
		chunk := ids[start:min(start+len(refs), len(ids))]
		clear(refs)
		var decodeErr error
		decodeErrIndex := len(chunk)
		err := collectionGetManyViewAtCatalogRoot(v.snapshot, v.catalog, root, chunk, func(i int, id, value []byte, found bool) error {
			stats.RowLocatorLookups++
			if !found {
				stats.RowLocatorMisses++
				return nil
			}
			// Retain the earliest invalid input, not the first storage callback.
			// Nothing after it can be delivered; earlier callbacks may still fail.
			if i < decodeErrIndex {
				ref, err := decode(id, value)
				if err != nil {
					decodeErr, decodeErrIndex = err, i
				} else {
					refs[i] = coordinates{ref.Generation, ref.PartID, ref.RowIndex, ref.AppliedCommandLSN}
				}
			}
			// Delivery errors never enter the helper's ErrKeyNotFound handling.
			return nil
		})
		if err != nil {
			return stats, fmt.Errorf("collections: primary row locator batch lookup: %w", err)
		}
		for i, id := range chunk {
			if i == decodeErrIndex {
				return stats, decodeErr
			}
			c := refs[i]
			found := c.generation != 0 // Valid locators always have a generation.
			var ref DocumentRowRef
			if found {
				ref = DocumentRowRef{DocumentID: id, Generation: c.generation, PartID: c.partID, RowIndex: c.rowIndex, AppliedCommandLSN: c.appliedCommandLSN}
			}
			if err := visit(id, ref, found); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

func (v *CollectionReadView) lookupDocumentRowRefsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentRowRefLookupResponse, error) {
	if err := v.validateOpen(); err != nil {
		return DocumentRowRefLookupResponse{}, err
	}
	if len(ids) == 0 {
		return DocumentRowRefLookupResponse{}, nil
	}
	response := DocumentRowRefLookupResponse{
		Results: make([]DocumentRowRefLookupResult, len(ids)),
		Stats: DocumentMaterializationStats{
			DocumentsRequested: uint64(len(ids)),
		},
	}
	if !columnStoreCanReconstructDocument(v.catalog.meta) {
		response.Stats.RowRefUnsupported++
		return response, errors.New("collections: document row ref lookup requires typed-storage reconstruction support")
	}
	var idArena []byte
	for i, id := range ids {
		if len(id) == 0 {
			return response, fmt.Errorf("collections: document id at position %d cannot be empty", i)
		}
		idStart := len(idArena)
		idArena = append(idArena, id...)
		response.Results[i].ID = idArena[idStart:len(idArena):len(idArena)]
	}
	i := 0
	stats, err := v.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
		if found {
			// Preserve the public API's two independently owned ID buffers.
			ref.DocumentID = append([]byte(nil), response.Results[i].ID...)
			response.Results[i].RowRef = ref
			response.Results[i].Found = true
		}
		i++
		return nil
	})
	response.Stats = stats
	return response, err
}

// materializerColumnSnapshotView certifies the entire classic manifest once on
// the exact immutable catalog. The shared value never owns a snapshot, context,
// reader, asset handle or decoded row. Each view binds the captured cut locally.
func (v *CollectionReadView) materializerColumnSnapshotView(cfg ColumnStoreConfig) (columnPhysicalScanSnapshotView, error) {
	if err := v.validateOpen(); err != nil {
		return columnPhysicalScanSnapshotView{}, err
	}
	if v.columnSnapshotView != nil {
		if v.typedGraphOwner == nil && v.columnSnapshotView != v.preparedMaterializer {
			// An uncertified/test-rebound view must validate unrelated entries
			// before even deriving dimensions for admission.
			if err := validateOrderedDocumentPartRefs(v.columnSnapshotView.AssetRefs); err != nil {
				return columnPhysicalScanSnapshotView{}, err
			}
		}
		return v.bindMaterializerMetadata(*v.columnSnapshotView)
	}
	catalog := v.catalog
	catalog.materializerMu.Lock()
	defer catalog.materializerMu.Unlock()
	if catalog.materializerMetadata == nil {
		view, err := v.collection.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(v.snapshot, catalog, catalog.meta.Name, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)), cfg, true, columnManifestScanNoSidecars())
		if err != nil {
			return view, err
		}
		if err := validateOrderedDocumentPartRefs(view.AssetRefs); err != nil {
			return view, err
		}
		prepareDocumentPointRowDimensions(&view)
		view.Catalog, view.snapshot = nil, nil
		view.CommitSeq, view.SystemRoot = 0, 0
		catalog.materializerMetadata = &view
	}
	v.columnSnapshotView = catalog.materializerMetadata
	v.preparedMaterializer = catalog.materializerMetadata
	v.validatedPointRowRefs = catalog.materializerMetadata
	return v.bindMaterializerMetadata(*catalog.materializerMetadata)
}

func (v *CollectionReadView) bindMaterializerMetadata(view columnPhysicalScanSnapshotView) (columnPhysicalScanSnapshotView, error) {
	token, ok := v.snapshot.StateToken()
	if !ok {
		return view, backenddb.ErrClosed
	}
	view.Catalog, view.snapshot = v.catalog, v.snapshot
	view.CommitSeq, view.SystemRoot = token.CommitSeq, token.SystemRootPageID
	return view, nil
}

func noColumnPhysicalScanProjection(cfg ColumnStoreConfig) columnPhysicalScanProjection {
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(cfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	return projection
}

// Scoring coordinates may name the immutable full row preserved by CRL2.
// Validate that link against this snapshot, then return document authority.
// Explicit public row-ref requests remain strict and do not follow that link.
func (v *CollectionReadView) resolveDocumentRowRefLatest(ref DocumentRowRef, stats *DocumentMaterializationStats, scoringRef bool) (DocumentRowRef, error) {
	if v == nil || v.catalog == nil || v.snapshot == nil {
		return DocumentRowRef{}, errors.New("collections: nil collection read view")
	}
	if stats != nil {
		stats.RowLocatorLookups++
	}
	locatorRootName := collectionColumnRowLocatorRootName(v.catalog.meta.Name)
	if v.catalog.rootID(locatorRootName) == 0 {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator root is absent for collection %q", v.catalog.meta.Name)
	}
	value, found, err := collectionGetAppendAtCatalogRoot(v.snapshot, v.catalog, locatorRootName, ref.DocumentID, nil)
	if err != nil {
		return DocumentRowRef{}, fmt.Errorf("collections: primary row locator validation for id %q: %w", string(ref.DocumentID), err)
	}
	if !found || len(value) == 0 {
		if stats != nil {
			stats.RowLocatorMisses++
			stats.RowRefValidationFailures++
		}
		return DocumentRowRef{}, fmt.Errorf("collections: document row ref for id %q is not visible in primary root", string(ref.DocumentID))
	}
	latest, err := decodeColumnPrimaryRowLocatorBorrowedID(ref.DocumentID, value)
	if err != nil {
		return DocumentRowRef{}, err
	}
	expected := latest
	if scoringRef && len(value) != columnPrimaryRowLocatorValueSize {
		// The complete CRL2 value was validated above.
		expected, err = decodeColumnRowCoordinates(ref.DocumentID, value[36:])
		if err != nil {
			return DocumentRowRef{}, err
		}
	}
	if err := validateDocumentRowRefMatchesRowRef(ref, expected); err != nil {
		if stats != nil {
			stats.RowRefValidationFailures++
		}
		return DocumentRowRef{}, err
	}
	return latest, nil
}

func (v *CollectionReadView) fetchDocumentPointRow(view columnPhysicalScanSnapshotView, ref DocumentRowRef, projection columnPhysicalScanProjection, scratch *columnPhysicalRowReaderScratch, stats *DocumentMaterializationStats) (columnPhysicalVisibleRow, error) {
	row, err := v.fetchDocumentPointRowUnresolved(view, ref, projection, scratch, stats)
	if err != nil || row.Preserved == nil {
		return row, err
	}
	var preservedScratch columnPhysicalRowReaderScratch
	full, err := v.fetchDocumentPointRowUnresolved(view, row.Preserved.ref(row.ID), projection, &preservedScratch, stats)
	if err != nil {
		return columnPhysicalVisibleRow{}, err
	}
	if full.Preserved != nil || full.Deleted {
		return columnPhysicalVisibleRow{}, errors.New("collections: metadata row must reference a live ordinary full row")
	}
	for colIdx, col := range view.Config.Columns {
		output := projection.outputByColumn[colIdx]
		if output >= 0 && !columnMetadataStoredColumn(col) {
			// Copy the value, not the payload. Both blocks remain pinned by this view.
			row.Values[output] = full.Values[output]
		}
	}
	return row, nil
}

func (v *CollectionReadView) fetchDocumentPointRowUnresolved(view columnPhysicalScanSnapshotView, ref DocumentRowRef, projection columnPhysicalScanProjection, scratch *columnPhysicalRowReaderScratch, stats *DocumentMaterializationStats) (columnPhysicalVisibleRow, error) {
	if stats != nil {
		stats.PointRowFetches++
	}
	assetRef, err := v.pointRowAssetRef(view, ref)
	if err != nil {
		return columnPhysicalVisibleRow{}, err
	}
	block, err := v.loadPointRowBlock(view, assetRef)
	if err != nil {
		return columnPhysicalVisibleRow{}, err
	}
	if ref.RowIndex < 0 || ref.RowIndex >= len(block.rowOffsets) {
		return columnPhysicalVisibleRow{}, fmt.Errorf("collections: document row ref for id %q row_index=%d outside physical rows=%d", string(ref.DocumentID), ref.RowIndex, len(block.rowOffsets))
	}
	reader := &columnPhysicalRowReader{view: view, projection: projection}
	row, err := reader.decodeRowFromBlock(block, ref.RowIndex, ref.RowIndex, scratch)
	if err != nil {
		return columnPhysicalVisibleRow{}, err
	}
	if stats != nil {
		stats.PointRowDecodes++
	}
	if err := validateDocumentRowRefMatchesPointRow(ref, row); err != nil {
		return columnPhysicalVisibleRow{}, err
	}
	return columnPhysicalVisibleRowFromReaderRow(row), nil
}

func (v *CollectionReadView) pointRowAssetRef(view columnPhysicalScanSnapshotView, ref DocumentRowRef) (columnManifestAssetRefForScan, error) {
	key := documentRowPartKey{Generation: ref.Generation, PartID: ref.PartID}
	var assetRef columnManifestAssetRefForScan
	var ok bool
	if v.preparedMaterializer != nil && (v.typedGraphOwner != nil || v.columnSnapshotView == v.preparedMaterializer) {
		assetRef, ok = materializerPartRef(v.preparedMaterializer.AssetRefs, ref.Generation, ref.PartID)
	} else if v.orderedPointRowRefs {
		// Manifest keys are ordered by generation/part. Validate the entire
		// immutable snapshot once, including unrelated entries, before using
		// binary search for any row. Derived-cache resets invalidate this mark.
		if v.columnSnapshotView == nil || v.validatedPointRowRefs != v.columnSnapshotView {
			if err := validateOrderedDocumentPartRefs(view.AssetRefs); err != nil {
				return columnManifestAssetRefForScan{}, err
			}
			v.validatedPointRowRefs = v.columnSnapshotView
		}
		assetRef, ok = materializerPartRef(view.AssetRefs, ref.Generation, ref.PartID)
	} else {
		if v.pointRowRefs == nil {
			v.pointRowRefs = make(map[documentRowPartKey]columnManifestAssetRefForScan, len(view.AssetRefs))
			for _, assetRef := range view.AssetRefs {
				if assetRef.Ref.Kind != ColumnAssetKindTCS1PartImage {
					return columnManifestAssetRefForScan{}, fmt.Errorf("collections: document row ref unsupported asset kind %q", assetRef.Ref.Kind)
				}
				key := documentRowPartKey{Generation: assetRef.Ref.Generation, PartID: assetRef.Ref.PartID}
				if _, exists := v.pointRowRefs[key]; exists {
					return columnManifestAssetRefForScan{}, fmt.Errorf("collections: duplicate document row ref asset generation=%d part_id=%d", key.Generation, key.PartID)
				}
				v.pointRowRefs[key] = assetRef
			}
		}
		assetRef, ok = v.pointRowRefs[key]
	}
	if !ok {
		return columnManifestAssetRefForScan{}, fmt.Errorf("collections: document row ref for id %q generation=%d part_id=%d is not present in snapshot", string(ref.DocumentID), ref.Generation, ref.PartID)
	}
	return assetRef, nil
}

func validateOrderedDocumentPartRefs(refs []columnManifestAssetRefForScan) error {
	for i, candidate := range refs {
		if candidate.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return fmt.Errorf("collections: document row ref unsupported asset kind %q", candidate.Ref.Kind)
		}
		if i > 0 {
			order := compareMaterializerPartRefs(refs[i-1], candidate)
			if order == 0 {
				return fmt.Errorf("collections: duplicate document row ref asset generation=%d part_id=%d", candidate.Ref.Generation, candidate.Ref.PartID)
			}
			if order > 0 {
				return errors.New("collections: document row ref assets are not ordered by generation/part")
			}
		}
	}
	return nil
}

func compareMaterializerPartRefs(a, b columnManifestAssetRefForScan) int {
	if order := cmp.Compare(a.Ref.Generation, b.Ref.Generation); order != 0 {
		return order
	}
	return cmp.Compare(a.Ref.PartID, b.Ref.PartID)
}

// Publication validates strict generation/part ordering once. Readers borrow
// the owned sorted refs and keep only decoded payloads in their request cache.
func materializerPartRef(refs []columnManifestAssetRefForScan, generation, partID uint64) (columnManifestAssetRefForScan, bool) {
	i, found := slices.BinarySearchFunc(refs, columnManifestAssetRefForScan{Ref: ColumnAssetRef{Generation: generation, PartID: partID}}, compareMaterializerPartRefs)
	if !found {
		return columnManifestAssetRefForScan{}, false
	}
	return refs[i], true
}

// A credit is an upper envelope, not retained length. It includes page-rounded
// encoded backing, actual offset capacity, schema-derived decoded-value/vector
// backing, cloned headers, descriptor ownership and manager bookkeeping.
func documentPointRowBlockCredit(ref columnManifestAssetRefForScan, cfg ColumnStoreConfig) (int64, error) {
	if ref.Ref.Length <= 0 || ref.Rows < 0 {
		return 0, errors.New("collections: invalid document row backing dimensions")
	}
	credit := uint64(ref.Ref.Length)
	add := func(count, width uint64) bool {
		if width != 0 && count > (math.MaxInt64-credit)/width {
			return false
		}
		credit += count * width
		return true
	}
	metadata := uint64(2*os.Getpagesize()) + uint64(unsafe.Sizeof(columnPhysicalRowReaderBlock{})) + mappedresource.ConservativeHandleMetadataBytes() + uint64(unsafe.Sizeof(columnPhysicalAssetSegmentReader{})) + uint64(len(cfg.AssetManager.Namespace))
	if !add(1, metadata) {
		return 0, errDocumentPointRowOversize
	}
	if ref.Ref.Kind == ColumnAssetKindTCS1PartImage {
		// Offsets are exact-sized today; allow two backing-capacity slots per row.
		if !add(uint64(ref.Rows), 2*uint64(unsafe.Sizeof(int(0)))) {
			return 0, errDocumentPointRowOversize
		}
	} else {
		// Existing typed reconstruction holds column-major decoded values plus
		// primary-ID/row lookup and intermediate decoded vector backing.
		if !add(uint64(ref.Rows), 32) {
			return 0, errDocumentPointRowOversize
		}
		for _, col := range cfg.Columns {
			if columnStoreColumnOwnerOrRowAsset(col) != TypedStorageOwnerColumnPart {
				continue
			}
			width := 2*uint64(unsafe.Sizeof(columnDeclaredValue{})) + 64
			elements := max(col.VectorDims, col.ElementsPerRow, 0)
			if uint64(elements) > (math.MaxInt64-width)/16 {
				return 0, errDocumentPointRowOversize
			}
			width += uint64(elements) * 16
			if !add(uint64(ref.Rows), width) {
				return 0, errDocumentPointRowOversize
			}
		}
		// Variable payload decoding can transiently own two copies in addition
		// to the pinned encoded extent; neither escapes into returned JSON.
		if !add(uint64(ref.Ref.Length), 2) {
			return 0, errDocumentPointRowOversize
		}
	}
	return int64(credit), nil
}

// A representable schema/extent budget is required for the borrowed route.
// Oversize arithmetic is an eligibility failure, not corrupt metadata; the
// existing fully validated owned reconstruction remains the portable fallback.
var errDocumentPointRowOversize = errors.New("collections: document row backing exceeds representable admission")

func documentPointRowWorkspaceCredit(cfg ColumnStoreConfig, maxEncodedBytes int64) (int64, error) {
	credit := uint64(unsafe.Sizeof(documentRowEmissionScratch{}))
	add := func(n, width uint64) bool {
		if width != 0 && n > (math.MaxInt64-credit)/width {
			return false
		}
		credit += n * width
		return true
	}
	// Three declared-value arrays, JSON scalar slots, and fixed cursor/map
	// descriptors. Raw variable payload scratch has at most two growth spans.
	width := 3*uint64(unsafe.Sizeof(columnDeclaredValue{})) + uint64(unsafe.Sizeof(columnReconstructedDeclaredValue{}))
	if !add(uint64(len(cfg.Columns)), width) || !add(64, 128) || !add(uint64(maxEncodedBytes), 2) {
		return 0, errDocumentPointRowOversize
	}
	for _, col := range cfg.Columns {
		if !add(uint64(max(col.VectorDims, col.ElementsPerRow, 0)), 16) {
			return 0, errDocumentPointRowOversize
		}
	}
	return int64(credit), nil
}

// prepareDocumentPointRowDimensions derives only schema/manifest maxima. The
// classic catalog calls it after complete validation, so ephemeral readers do
// not repeat a scan of every historical part. Other prepared owners derive the
// same dimensions on their local metadata copy; unsupported sizes keep the
// existing owned fallback rather than weakening admission.
func prepareDocumentPointRowDimensions(view *columnPhysicalScanSnapshotView) {
	if view.pointRowDimensionsPrepared {
		return
	}
	view.pointRowDimensionsPrepared = true
	for _, refs := range [][]columnManifestAssetRefForScan{view.AssetRefs, view.TypedColumnPartRefs} {
		for _, ref := range refs {
			credit, err := documentPointRowBlockCredit(ref, view.FullConfig)
			if err != nil {
				view.pointRowDimensionsErr = err
				return
			}
			view.pointRowMaximumEncoded = max(view.pointRowMaximumEncoded, ref.Ref.Length)
			view.pointRowMaximumCredit = max(view.pointRowMaximumCredit, credit)
		}
	}
	view.pointRowWorkspaceCredit, view.pointRowDimensionsErr = documentPointRowWorkspaceCredit(view.FullConfig, view.pointRowMaximumEncoded)
}

func (v *CollectionReadView) preparePointRowCredit(view columnPhysicalScanSnapshotView) error {
	if v.pointRowCreditLimit != 0 {
		return nil
	}
	// FD/cache objects and complete owned path/header text are independent
	// of the encoded range. Charge the actual directory/name lengths too.
	descriptorCredit := int64(len(v.rowAssetReadCache.segmentDir)) + int64(len(columnAssetSegmentFileName(^uint32(0)))) + 1 + int64(len(view.CollectionName)) + int64(2*unsafe.Sizeof(columnPhysicalAssetSegmentReader{})+2*unsafe.Sizeof(os.File{}))
	if descriptorCredit < 0 {
		return errDocumentPointRowOversize
	}
	prepareDocumentPointRowDimensions(&view)
	if view.pointRowDimensionsErr != nil {
		return view.pointRowDimensionsErr
	}
	maximum, maxCredit, workspace := view.pointRowMaximumEncoded, view.pointRowMaximumCredit, view.pointRowWorkspaceCredit
	if maxCredit <= 0 {
		return errors.New("collections: document row admission has no captured asset")
	}
	if maxCredit > math.MaxInt64-descriptorCredit {
		return errDocumentPointRowOversize
	}
	maxCredit += descriptorCredit
	if maxCredit > (math.MaxInt64-workspace)/documentPointRowMaxBorrowedBlocks {
		return errDocumentPointRowOversize
	}
	v.pointRowDescriptorCredit = descriptorCredit
	v.pointRowMaxBlockBytes = maximum
	v.pointRowWorkspaceCredit = workspace
	v.pointRowCreditUsed = workspace
	v.pointRowCreditLimit = maxCredit*documentPointRowMaxBorrowedBlocks + workspace
	return nil
}

func (v *CollectionReadView) pointRowOpenFiles() int {
	n := 0
	for _, c := range []*columnPhysicalAssetReadCache{v.rowAssetReadCache, v.typedColumnAssetReadCache} {
		if c != nil {
			n += len(c.files)
			if c.file != nil {
				n++
			}
		}
	}
	return n
}

func (v *CollectionReadView) admitPointRowFile() error {
	if v.pointRowOpenFiles() >= documentPointRowMaxBorrowedBlocks {
		return errors.New("collections: document file admission exhausted before open")
	}
	return nil
}

func (v *CollectionReadView) admitPointRowAsset(ref ColumnAssetRef) error {
	prepared := v.preparedMaterializer
	if prepared == nil {
		return errors.New("collections: document row admission requires captured metadata")
	}
	refs := prepared.AssetRefs
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		refs = prepared.TypedColumnPartRefs
	}
	entry, ok := materializerPartRef(refs, ref.Generation, ref.PartID)
	if !ok || entry.Ref != ref {
		return errors.New("collections: document row admission asset is outside captured manifest")
	}
	// Installed serving metadata is deliberately compact and omits FullConfig.
	// Admission binds its exact refs to this captured catalog's validated schema.
	credit, err := documentPointRowBlockCredit(entry, *v.catalog.meta.Options.ColumnStore)
	if err != nil {
		return err
	}
	if credit > math.MaxInt64-v.pointRowDescriptorCredit {
		return errDocumentPointRowOversize
	}
	credit += v.pointRowDescriptorCredit
	if credit > v.pointRowCreditLimit-v.pointRowCreditUsed {
		return errors.New("collections: document row admission exhausted before load")
	}
	v.pointRowCreditUsed += credit
	return nil
}

// Called only between synchronous row emissions. A CRL2 row can borrow two
// blocks and a typed-column part on the next emission; reserve all three slots.
// No borrowed row/string/vector escapes into the owned result documents.
func (v *CollectionReadView) preparePointRowEmission(cfg ColumnStoreConfig, integrity ColumnAssetReadIntegrity) error {
	handles := int64(0)
	if v.assetManager != nil {
		handles = v.assetManager.ActiveHandles()
	}
	if len(v.pointRowBlocks) <= documentPointRowMaxBorrowedBlocks-3 && handles <= documentPointRowMaxBorrowedBlocks-3 && v.pointRowOpenFiles() <= documentPointRowMaxBorrowedBlocks-3 {
		return nil
	}
	v.pointRowBlocks = nil
	v.pointRowCreditUsed = v.pointRowWorkspaceCredit
	v.pointRowCacheEvictions++
	var err error
	if v.rowAssetReadCache != nil {
		err = errors.Join(err, v.rowAssetReadCache.close())
		v.assetClosedCounters.addReadCache(v.rowAssetReadCache)
		v.rowAssetReadCache = nil
	}
	if v.typedColumnAssetReadCache != nil {
		err = errors.Join(err, v.typedColumnAssetReadCache.close())
		v.assetClosedCounters.addReadCache(v.typedColumnAssetReadCache)
		v.typedColumnAssetReadCache = nil
	}
	v.typedColumnReconstructionCache = nil
	if err != nil {
		return err
	}
	return v.ensureAssetReadCaches(cfg, integrity)
}

func (v *CollectionReadView) loadPointRowBlock(view columnPhysicalScanSnapshotView, assetRef columnManifestAssetRefForScan) (*columnPhysicalRowReaderBlock, error) {
	key := documentRowPartKey{Generation: assetRef.Ref.Generation, PartID: assetRef.Ref.PartID}
	if block := v.pointRowBlocks[key]; block != nil {
		return block, nil
	}
	if v.rowAssetReadCache == nil {
		return nil, errors.New("collections: document row point fetch requires row asset read cache")
	}
	if err := v.preparePointRowCredit(view); err != nil {
		return nil, err
	}
	if len(v.pointRowBlocks) >= documentPointRowMaxBorrowedBlocks {
		return nil, errors.New("collections: document row admission exhausted before load")
	}
	raw, err := v.rowAssetReadCache.read(assetRef.Ref, nil)
	if err != nil {
		return nil, fmt.Errorf("collections: document row point fetch read generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
	}
	if !v.rowAssetReadCache.lastView {
		raw = bytes.Clone(raw)
	}
	header, version, rowsOffset, err := parseColumnPhysicalAssetScanHeader(raw, assetRef.Ref, view.CollectionName, &view.Config, assetRef.Reason)
	if err != nil {
		return nil, fmt.Errorf("collections: document row point fetch header generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
	}
	if header.RowCount != assetRef.Rows {
		return nil, errors.New("collections: physical row count disagrees with captured manifest")
	}
	header = cloneColumnPhysicalAssetScanHeader(header)
	rowIndex, err := v.rowAssetReadCache.indexRows(raw, assetRef.Ref, version, rowsOffset, header, &view.Config)
	if err != nil {
		return nil, fmt.Errorf("collections: document row point fetch index generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
	}
	block := &columnPhysicalRowReaderBlock{
		assetOrdinal:  -1,
		raw:           raw,
		version:       version,
		header:        header,
		rowOffsets:    rowIndex.offsets,
		rowEncoding:   rowIndex.rowEncoding,
		fixedIDWidth:  rowIndex.fixedIDWidth,
		denseIDBase:   rowIndex.denseIDBase,
		residentBytes: int64(cap(raw)) + int64(cap(rowIndex.offsets))*int64(unsafe.Sizeof(int(0))) + int64(cap(header.Collection)+cap(header.Namespace)),
	}
	if v.pointRowBlocks == nil {
		v.pointRowBlocks = make(map[documentRowPartKey]*columnPhysicalRowReaderBlock)
	}
	// A mapped raw slice may belong to the serving pool. This cache is nested
	// under the read view: clearDerivedRowFetchCaches and the request's logical
	// handles close before typedGraphOwner.Close releases the holder/pool.
	// Retain no borrowed reader object here.
	v.pointRowBlocks[key] = block
	return block, nil
}

func (v *CollectionReadView) pointRowScanProjection(view columnPhysicalScanSnapshotView, projected []string) (columnPhysicalScanProjection, error) {
	if projected != nil {
		return newColumnPhysicalScanProjection(view.Config, projected)
	}
	if v.pointRowProjection != nil {
		return *v.pointRowProjection, nil
	}
	projection, err := newColumnPhysicalScanProjection(view.Config, nil)
	if err != nil {
		return columnPhysicalScanProjection{}, err
	}
	v.pointRowProjection = &projection
	return projection, nil
}

// FetchDocumentsByRowRef materializes full documents for row refs in input
// order. Supported typed-row refs are point-fetched by generation/part/row_index
// and validated against the decoded physical row before reconstruction. Row-ref
// fetches are strict: a missing primary-root document, stale ref, or physical
// mismatch fails the request instead of silently returning a partial batch.
func (v *CollectionReadView) FetchDocumentsByRowRef(refs []DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	endForegroundRead := v.beginForegroundRead()
	defer endForegroundRead()
	start := time.Now()
	response, err := v.fetchDocumentsByRowRef(refs, opts, documentRowRefStrict)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
}

// fetchDocumentsByResolvedRowRef is reserved for refs obtained from this read
// view's persistent locator root. The lookup and fetch share one immutable
// snapshot, so repeating latest-locator validation would be redundant. Primary
// visibility and physical-row coordinate validation still fail closed below.
func (v *CollectionReadView) fetchDocumentsByResolvedRowRef(refs []DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	return v.fetchDocumentsByRowRef(refs, opts, documentRowRefResolved)
}

// Only internal graph results carry scoring refs. The shared fetch loop checks
// each ref against the captured locator once, just as strict row-ref fetch does.
func (v *CollectionReadView) fetchDocumentsByScoringRowRef(refs []DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	start := time.Now()
	response, err := v.fetchDocumentsByRowRef(refs, opts, documentRowRefScoring)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
}

type documentRowRefAuthority uint8

const (
	documentRowRefStrict documentRowRefAuthority = iota
	documentRowRefResolved
	documentRowRefScoring
)

func (v *CollectionReadView) fetchDocumentsByRowRef(refs []DocumentRowRef, opts DocumentFetchOptions, authority documentRowRefAuthority) (out DocumentFetchResponse, err error) {
	workstats.Output.Materialization.Attempts.Add(1)
	defer func() {
		w := documentMaterializationWork(out.Stats)
		w.Requested = uint64(len(refs))
		workstats.Output.Materialization.Add(w)
		workstats.Output.Materialization.Finish(err == nil)
	}()
	if err := documentFetchContextErr(opts.Context); err != nil {
		return DocumentFetchResponse{}, err
	}
	if err := v.validateOpen(); err != nil {
		return DocumentFetchResponse{}, err
	}
	if len(refs) == 0 {
		return DocumentFetchResponse{}, nil
	}
	projection, err := normalizeDocumentFetchProjection(v.catalog.meta, opts)
	if err != nil {
		return DocumentFetchResponse{}, err
	}
	if !columnStoreCanReconstructDocument(v.catalog.meta) {
		response := DocumentFetchResponse{Stats: DocumentMaterializationStats{DocumentsRequested: uint64(len(refs)), RowRefUnsupported: 1}}
		return response, errors.New("collections: document row ref materialization requires typed-storage reconstruction support")
	}
	ids := make([][]byte, len(refs))
	for i := range refs {
		if err := validateDocumentRowRefForPointFetch(i, refs[i]); err != nil {
			return DocumentFetchResponse{}, err
		}
		ids[i] = refs[i].DocumentID
	}
	response, retained, foundCount, err := v.fetchRetainedPayloadsByID(ids, opts.Context)
	if err != nil {
		return response, err
	}
	if foundCount != len(refs) {
		for i := range response.Results {
			if !response.Results[i].Found {
				response.Stats.RowRefValidationFailures++
				return response, fmt.Errorf("collections: document row ref for id %q is not visible in primary root", string(response.Results[i].ID))
			}
		}
	}
	return v.fetchColumnStoreDocumentsByRowRef(response, refs, retained, opts, projection, authority)
}

func (v *CollectionReadView) fetchDocumentsByID(ids [][]byte, expected []*DocumentRowRef, opts DocumentFetchOptions) (out DocumentFetchResponse, err error) {
	workstats.Output.Materialization.Attempts.Add(1)
	defer func() {
		w := documentMaterializationWork(out.Stats)
		w.Requested = uint64(len(ids))
		workstats.Output.Materialization.Add(w)
		workstats.Output.Materialization.Finish(err == nil)
	}()
	if err := documentFetchContextErr(opts.Context); err != nil {
		return DocumentFetchResponse{}, err
	}
	if err := v.validateOpen(); err != nil {
		return DocumentFetchResponse{}, err
	}
	if expected != nil && len(expected) != len(ids) {
		return DocumentFetchResponse{}, errors.New("collections: document row ref count does not match ids")
	}
	if len(ids) == 0 {
		return DocumentFetchResponse{}, nil
	}
	projection, err := normalizeDocumentFetchProjection(v.catalog.meta, opts)
	if err != nil {
		return DocumentFetchResponse{}, err
	}
	response, retained, foundCount, err := v.fetchRetainedPayloadsByID(ids, opts.Context)
	if err != nil {
		return response, err
	}
	if foundCount == 0 {
		return response, nil
	}
	if expected != nil && !columnStoreCanReconstructDocument(v.catalog.meta) {
		return response, errors.New("collections: document row ref materialization requires typed-storage reconstruction support")
	}
	if !columnStoreCanReconstructDocument(v.catalog.meta) {
		var documentArena []byte
		for i := range response.Results {
			if err := documentFetchContextErr(opts.Context); err != nil {
				return response, err
			}
			if !response.Results[i].Found {
				continue
			}
			document := retained[i]
			if projection.active() {
				var err error
				document, err = projectJSONDocument(retained[i], projection, &response.Stats)
				if err != nil {
					return response, err
				}
				documentArena = appendDocumentFetchOwnedBytes(documentArena, document, &response.Results[i])
			} else if len(document) == 0 {
				response.Results[i].Document = []byte{}
			} else {
				response.Results[i].Document = document[:len(document):len(document)]
			}
			response.Stats.DocumentsFetched++
			response.Stats.DocumentBytes += uint64(len(response.Results[i].Document))
			response.Stats.OutputBytes += uint64(len(response.Results[i].Document))
		}
		return response, nil
	}
	return v.fetchColumnStoreDocumentsByID(response, ids, retained, expected, opts, projection)
}

func (v *CollectionReadView) fetchRetainedPayloadsByID(ids [][]byte, ctx context.Context) (DocumentFetchResponse, [][]byte, int, error) {
	if err := documentFetchContextErr(ctx); err != nil {
		return DocumentFetchResponse{}, nil, 0, err
	}
	response := DocumentFetchResponse{
		Results: make([]DocumentFetchResult, len(ids)),
		Stats: DocumentMaterializationStats{
			DocumentsRequested: uint64(len(ids)),
		},
	}
	var idArena []byte
	retained := make([][]byte, len(ids))
	for i, id := range ids {
		if len(id) == 0 {
			return response, retained, 0, fmt.Errorf("collections: document id at position %d cannot be empty", i)
		}
		idStart := len(idArena)
		idArena = append(idArena, id...)
		response.Results[i].ID = idArena[idStart:len(idArena):len(idArena)]
	}

	foundCount := 0
	var retainedArena []byte
	var cfg ColumnStoreConfig
	resolveRetained := false
	if v.catalog != nil && v.catalog.meta.Options.ColumnStore != nil {
		cfg = v.catalog.meta.Options.ColumnStore.copy()
		resolveRetained = columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg)
	}
	err := collectionGetManyViewAtCatalogRoot(v.snapshot, v.catalog, collectionPrimaryRootName(v.catalog.meta.Name), ids, func(i int, _ []byte, value []byte, found bool) error {
		if err := documentFetchContextErr(ctx); err != nil {
			return err
		}
		if i < 0 || i >= len(ids) {
			return fmt.Errorf("collections: GetManyView callback index %d outside %d ids", i, len(ids))
		}
		if !found {
			response.Stats.DocumentsMissing++
			return nil
		}
		response.Results[i].Found = true
		if resolveRetained {
			resolved, err := resolveColumnRetainedPayloadAtSnapshot(v.snapshot, v.catalog, cfg, value)
			if err != nil {
				return err
			}
			value = resolved
		}
		if len(value) == 0 {
			retained[i] = []byte{}
		} else {
			start := len(retainedArena)
			retainedArena = append(retainedArena, value...)
			retained[i] = retainedArena[start:len(retainedArena):len(retainedArena)]
		}
		foundCount++
		response.Stats.RetainedPayloadFetches++
		response.Stats.RetainedPayloadBytes += uint64(len(value))
		return nil
	})
	if err != nil {
		return response, retained, foundCount, err
	}
	return response, retained, foundCount, nil
}

func documentFetchContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (v *CollectionReadView) fetchColumnStoreDocumentsByRowRef(response DocumentFetchResponse, refs []DocumentRowRef, retained [][]byte, opts DocumentFetchOptions, projection *documentProjection, authority documentRowRefAuthority) (out DocumentFetchResponse, err error) {
	return v.fetchColumnStoreDocumentsByRowRefInto(response, refs, retained, opts, projection, authority, nil, false)
}

// A single worker owns this scratch. Borrowed values are cleared after each
// synchronous emission and before cache eviction; capacity is schema-derived.
type documentRowEmissionScratch struct {
	row            columnPhysicalRowReaderScratch
	typed          []columnDeclaredValue
	merge          []columnDeclaredValue
	reconstruction columnDocumentReconstructionScratch
}

func (s *documentRowEmissionScratch) clearBorrowed() {
	if s == nil {
		return
	}
	clear(s.row.Values)
	clear(s.typed)
	clear(s.merge)
	s.reconstruction.clearBorrowed()
}

func (v *CollectionReadView) fetchColumnStoreDocumentsByRowRefInto(response DocumentFetchResponse, refs []DocumentRowRef, retained [][]byte, opts DocumentFetchOptions, projection *documentProjection, authority documentRowRefAuthority, documentArena []byte, callerOutput bool) (out DocumentFetchResponse, err error) {
	out = response
	cfg := *v.catalog.meta.Options.ColumnStore
	readIntegrity := opts.ColumnAssetReadIntegrity
	if readIntegrity == "" {
		readIntegrity = ColumnAssetReadIntegrityVerify
	}
	if err := v.ensureAssetReadCaches(cfg, readIntegrity); err != nil {
		return out, err
	}
	assetCountersBefore := v.assetCounters()
	defer func() {
		addDocumentMaterializerAssetCounterDeltas(&out.Stats, assetCountersBefore, v.assetCounters())
	}()
	// Failed admission/decode must release its reservation and any opened
	// descriptors. A retry never accumulates failed pins or stale borrowed data.
	defer func() {
		if err != nil {
			err = errors.Join(err, v.closeAssetReadCaches())
		}
	}()
	view, err := v.materializerColumnSnapshotView(cfg)
	if err != nil {
		return out, err
	}
	if err := v.preparePointRowCredit(view); err != nil {
		if errors.Is(err, errDocumentPointRowOversize) {
			return v.fetchColumnStoreDocumentsOwnedFallback(response, refs, retained, opts, projection, authority, documentArena, callerOutput)
		}
		return out, err
	}
	v.rowAssetReadCache.admitResource = v.admitPointRowAsset
	if v.typedColumnAssetReadCache != nil {
		v.typedColumnAssetReadCache.admitResource = v.admitPointRowAsset
	}
	selectedColumns := documentProjectionSelectedColumns(cfg, projection)
	rowProjection := documentProjectionRowAssetColumns(cfg, selectedColumns)
	typedProjection := documentProjectionTypedColumnPartSelection(cfg, selectedColumns)
	pointProjection, err := v.pointRowScanProjection(view, rowProjection)
	if err != nil {
		return out, err
	}
	typedColumnCache := v.typedColumnReconstructionCacheForConfig(cfg)
	manifestRootID := v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name))
	if v.rowEmissionScratch == nil {
		v.rowEmissionScratch = &documentRowEmissionScratch{merge: make([]columnDeclaredValue, 0, len(cfg.Columns))}
	}
	worker := v.rowEmissionScratch
	defer worker.clearBorrowed()
	retainedTemplateResolver := columnRetainedPayloadTemplateResolver(v.snapshot, v.catalog)
	for i := range out.Results {
		if err := documentFetchContextErr(opts.Context); err != nil {
			return out, err
		}
		if !out.Results[i].Found {
			// ID fetch preserves missing entries. Explicit row-ref callers already
			// require every primary row to exist before entering this shared loop.
			continue
		}
		if err := v.preparePointRowEmission(cfg, readIntegrity); err != nil {
			return out, err
		}
		v.rowAssetReadCache.admitResource = v.admitPointRowAsset
		if v.typedColumnAssetReadCache != nil {
			v.typedColumnAssetReadCache.admitResource = v.admitPointRowAsset
		}
		typedColumnCache = v.typedColumnReconstructionCacheForConfig(cfg)
		ref := refs[i]
		if authority != documentRowRefResolved {
			ref, err = v.resolveDocumentRowRefLatest(ref, &out.Stats, authority == documentRowRefScoring)
			if err != nil {
				return out, err
			}
		}
		row, err := v.fetchDocumentPointRow(view, ref, pointProjection, &worker.row, &out.Stats)
		if err != nil {
			out.Stats.RowRefValidationFailures++
			return out, err
		}
		rowRef := documentRowRefCoordinatesFromVisibleRow(row)
		rowRef.DocumentID = out.Results[i].ID
		out.Results[i].RowRef = rowRef

		beforeCacheHits, beforeCacheMisses, beforePartLoads, beforePartDecodes := typedColumnCacheCounters(typedColumnCache)
		typedStart := time.Now()
		typedValues, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCacheProjected(v.snapshot, manifestRootID, cfg, row, typedColumnCache, worker.typed, typedProjection)
		typedElapsed := time.Since(typedStart)
		if err != nil {
			return out, err
		}
		if documentProjectionHasSelectedTypedColumn(typedProjection) && (len(typedValues.Values) > 0 || columnStoreHasTypedColumnPartOwners(cfg)) {
			out.Stats.TypedColumnRows++
		}
		afterCacheHits, afterCacheMisses, afterPartLoads, afterPartDecodes := typedColumnCacheCounters(typedColumnCache)
		out.Stats.TypedColumnCacheHits += deltaUint64(beforeCacheHits, afterCacheHits)
		out.Stats.TypedColumnCacheMisses += deltaUint64(beforeCacheMisses, afterCacheMisses)
		out.Stats.TypedColumnPartLoads += deltaUint64(beforePartLoads, afterPartLoads)
		out.Stats.TypedColumnPartDecodes += deltaUint64(beforePartDecodes, afterPartDecodes)
		out.Stats.TypedColumnNanos += typedElapsed.Nanoseconds()

		reconstructStart := time.Now()
		fullValues, err := mergeColumnReconstructionValuesProjectedInto(cfg, row.Values, typedValues.Values, selectedColumns, worker.merge)
		if err != nil {
			return out, err
		}
		var document []byte
		worker.typed = typedValues.Values
		worker.merge = fullValues
		if row.Deleted {
			return out, errors.New("collections: column reconstruction latest physical row is deleted")
		}
		documentArena, document, err = reconstructColumnJSONDocumentProjectedIntoWithScratch(documentArena, cfg, retained[i], fullValues, projection, &out.Stats, retainedTemplateResolver, &worker.reconstruction)
		if err != nil {
			return out, err
		}
		out.Stats.JSONReconstructionNanos += time.Since(reconstructStart).Nanoseconds()
		out.Stats.JSONReconstructionRows++
		out.Results[i].Document = document
		if callerOutput {
			out.Results[i].Document = documentArena
		}
		out.Stats.DocumentsFetched++
		out.Stats.DocumentBytes += uint64(len(out.Results[i].Document))
		out.Stats.OutputBytes += uint64(len(out.Results[i].Document))
		worker.clearBorrowed()
	}
	return out, nil
}

// Only representability failures select this owned, uncached legacy route.
// It preserves the same snapshot, strict/scoring locator validation, full
// physical-row validation, projection writer and caller destination contract.
// Its global visibility work is charged and has no bounded-route speed claim.
func (v *CollectionReadView) fetchColumnStoreDocumentsOwnedFallback(response DocumentFetchResponse, refs []DocumentRowRef, retained [][]byte, opts DocumentFetchOptions, projection *documentProjection, authority documentRowRefAuthority, arena []byte, callerOutput bool) (DocumentFetchResponse, error) {
	cfg := *v.catalog.meta.Options.ColumnStore
	for i := range response.Results {
		if err := documentFetchContextErr(opts.Context); err != nil {
			return response, err
		}
		if !response.Results[i].Found {
			continue
		}
		ref := refs[i]
		var err error
		if authority != documentRowRefResolved {
			ref, err = v.resolveDocumentRowRefLatest(ref, &response.Stats, authority == documentRowRefScoring)
			if err != nil {
				return response, err
			}
		}
		start := time.Now()
		row, diag, found, err := v.collection.latestColumnPhysicalVisibleRowAtSnapshot(v.snapshot, v.catalog, response.Results[i].ID, nil)
		response.Stats.RowRefFallbackScans++
		response.Stats.VisibilityScans++
		response.Stats.VisibilityPhysicalBytes += diag.PhysicalBytesScanned
		response.Stats.VisibilityRows++
		response.Stats.VisibilityRowsScanned += uint64(diag.RowsScanned)
		response.Stats.VisibilityNanos += time.Since(start).Nanoseconds()
		if err != nil {
			return response, err
		}
		if !found {
			return response, errors.New("collections: owned fallback has no visible physical row")
		}
		if err := validateDocumentRowRefMatchesVisibleRow(ref, row); err != nil {
			response.Stats.RowRefValidationFailures++
			return response, err
		}
		values, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshot(v.snapshot, v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name)), cfg, row)
		if err != nil {
			return response, err
		}
		full, err := mergeColumnReconstructionValues(cfg, row.Values, values.Values)
		if err != nil {
			return response, err
		}
		start = time.Now()
		var doc []byte
		arena, doc, err = reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(arena, cfg, retained[i], row, full, projection, &response.Stats, columnRetainedPayloadTemplateResolver(v.snapshot, v.catalog))
		if err != nil {
			return response, err
		}
		response.Stats.JSONReconstructionNanos += time.Since(start).Nanoseconds()
		response.Stats.JSONReconstructionRows++
		response.Results[i].RowRef = documentRowRefCoordinatesFromVisibleRow(row)
		response.Results[i].RowRef.DocumentID = response.Results[i].ID
		response.Results[i].Document = doc
		if callerOutput {
			response.Results[i].Document = arena
		}
		response.Stats.DocumentsFetched++
		response.Stats.DocumentBytes += uint64(len(doc))
		response.Stats.OutputBytes += uint64(len(doc))
	}
	return response, nil
}

func (v *CollectionReadView) fetchColumnStoreDocumentsByID(response DocumentFetchResponse, ids [][]byte, retained [][]byte, expected []*DocumentRowRef, opts DocumentFetchOptions, projection *documentProjection) (DocumentFetchResponse, error) {
	// The primary payloads are already fetched and owned by response. Resolve
	// only final requested IDs on this same pin; do not rebuild visibility arenas.
	refs := make([]DocumentRowRef, len(ids))
	position := 0
	stats, err := v.visitDocumentRowRefsByID(ids, func(_ []byte, ref DocumentRowRef, found bool) error {
		i := position
		position++
		if err := documentFetchContextErr(opts.Context); err != nil {
			return err
		}
		if found != response.Results[i].Found {
			response.Stats.RowRefValidationFailures++
			return fmt.Errorf("collections: primary row locator visibility disagrees for id %q", ids[i])
		}
		if !found {
			return nil
		}
		ref.DocumentID = response.Results[i].ID
		if expected != nil && expected[i] != nil {
			if err := validateDocumentRowRefMatchesRowRef(*expected[i], ref); err != nil {
				response.Stats.RowRefValidationFailures++
				return err
			}
		}
		refs[i] = ref
		return nil
	})
	response.Stats.RowLocatorLookups += stats.RowLocatorLookups
	response.Stats.RowLocatorMisses += stats.RowLocatorMisses
	if err != nil {
		return response, err
	}
	return v.fetchColumnStoreDocumentsByRowRef(response, refs, retained, opts, projection, documentRowRefResolved)
}

// materializeRetainedTypedDocument reuses this view's locator and point decoder
// when the caller has already read the primary payload on the same snapshot.
// The result owns its bytes; the input ID and payload are borrowed for this call.
func (v *CollectionReadView) materializeRetainedTypedDocument(id, retained []byte) ([]byte, error) {
	return v.materializeRetainedTypedDocumentInto(id, retained, nil)
}

func (v *CollectionReadView) materializeRetainedTypedDocumentInto(id, retained, dst []byte) (document []byte, err error) {
	workstats.Output.Materialization.Attempts.Add(1)
	var response DocumentFetchResponse
	defer func() {
		w := documentMaterializationWork(response.Stats)
		w.Requested = 1
		workstats.Output.Materialization.Add(w)
		workstats.Output.Materialization.Finish(err == nil)
	}()
	if err := v.validateOpen(); err != nil {
		return nil, err
	}
	if v.catalog.rootID(collectionColumnRowLocatorRootName(v.catalog.meta.Name)) == 0 || v.catalog.meta.Options.ColumnStore.ActiveManifest.Format == columnSourceDirectoryFormatV2 {
		var diag columnDocumentReconstructionDiagnostics
		document, diag, err = v.collection.reconstructColumnDocumentAtSnapshotWithDiagnostics(v.snapshot, v.catalog, id, retained)
		response.Stats.DocumentsRequested = 1
		response.Stats.VisibilityRows = uint64(diag.VisibilityRows)
		response.Stats.VisibilityPhysicalBytes = diag.PhysicalBytesScanned
		response.Stats.JSONReconstructionRows = uint64(diag.ReconstructionRows)
		if v.catalog.meta.Options.ColumnStore.ActiveManifest.Format != columnSourceDirectoryFormatV2 {
			response.Stats.RowRefFallbackScans = 1
			response.Stats.VisibilityScans = 1
		}
		if err == nil {
			response.Stats.DocumentsFetched = 1
			response.Stats.DocumentBytes = uint64(len(document))
			response.Stats.OutputBytes = uint64(len(document))
		}
		return document, err
	}
	if columnStoreRetainedPayloadUsesSemanticStreamV1(v.catalog.meta.Options.ColumnStore) {
		retained, err = resolveColumnRetainedPayloadAtSnapshot(v.snapshot, v.catalog, *v.catalog.meta.Options.ColumnStore, retained)
		if err != nil {
			return nil, err
		}
	}
	results := [1]DocumentFetchResult{{ID: id, Found: true}}
	ids := [1][]byte{id}
	payloads := [1][]byte{retained}
	refs := [1]DocumentRowRef{}
	response.Results = results[:]
	response.Stats.DocumentsRequested = 1
	stats, lookupErr := v.visitDocumentRowRefsByID(ids[:], func(_ []byte, ref DocumentRowRef, found bool) error {
		if !found {
			return fmt.Errorf("collections: primary row locator visibility disagrees for id %q", id)
		}
		refs[0] = ref
		return nil
	})
	response.Stats.RowLocatorLookups += stats.RowLocatorLookups
	response.Stats.RowLocatorMisses += stats.RowLocatorMisses
	if lookupErr != nil {
		return dst[:0], lookupErr
	}
	response, err = v.fetchColumnStoreDocumentsByRowRefInto(response, refs[:], payloads[:], DocumentFetchOptions{}, nil, documentRowRefResolved, dst, true)

	if err != nil {
		return nil, err
	}
	return response.Results[0].Document, nil
}

func appendDocumentFetchOwnedBytes(arena []byte, src []byte, result *DocumentFetchResult) []byte {
	if result == nil {
		return arena
	}
	if len(src) == 0 {
		result.Document = []byte{}
		return arena
	}
	start := len(arena)
	arena = append(arena, src...)
	result.Document = arena[start:len(arena):len(arena)]
	return arena
}

func documentRowRefFromVisibleRow(row columnPhysicalVisibleRow) DocumentRowRef {
	ref := documentRowRefCoordinatesFromVisibleRow(row)
	ref.DocumentID = bytes.Clone(row.ID)
	return ref
}

func documentRowRefCoordinatesFromVisibleRow(row columnPhysicalVisibleRow) DocumentRowRef {
	return DocumentRowRef{
		Generation:        row.Generation,
		PartID:            row.PartID,
		RowIndex:          row.RowIndex,
		AppliedCommandLSN: row.AppliedCommandLSN,
	}
}

func columnPhysicalVisibleRowFromReaderRow(row columnPhysicalRowReaderRow) columnPhysicalVisibleRow {
	return columnPhysicalVisibleRow{
		Preserved:         row.Preserved,
		Generation:        row.Generation,
		PartID:            row.PartID,
		AppliedCommandLSN: row.AppliedCommandLSN,
		Operation:         row.Operation,
		RowIndex:          row.RowIndex,
		ID:                row.ID,
		Values:            row.Values,
		Deleted:           row.Deleted,
	}
}

func validateDocumentRowRefForPointFetch(pos int, ref DocumentRowRef) error {
	if len(ref.DocumentID) == 0 {
		return fmt.Errorf("collections: document row ref %d missing document id", pos)
	}
	if ref.Generation == 0 {
		return fmt.Errorf("collections: document row ref %d missing generation", pos)
	}
	if ref.PartID == 0 {
		return fmt.Errorf("collections: document row ref %d missing part_id", pos)
	}
	if ref.RowIndex < 0 {
		return fmt.Errorf("collections: document row ref %d has negative row_index", pos)
	}
	if ref.AppliedCommandLSN == 0 {
		return fmt.Errorf("collections: document row ref %d missing applied_command_lsn", pos)
	}
	return nil
}

func validateDocumentRowRefMatchesPointRow(ref DocumentRowRef, row columnPhysicalRowReaderRow) error {
	if len(ref.DocumentID) == 0 {
		return errors.New("collections: document row ref missing document id")
	}
	if !bytes.Equal(ref.DocumentID, row.ID) {
		return fmt.Errorf("collections: document row ref id %q does not match physical row id %q", string(ref.DocumentID), string(row.ID))
	}
	if ref.Generation != row.Generation {
		return fmt.Errorf("collections: document row ref for id %q generation=%d does not match physical generation=%d", string(ref.DocumentID), ref.Generation, row.Generation)
	}
	if ref.PartID != row.PartID {
		return fmt.Errorf("collections: document row ref for id %q part_id=%d does not match physical part_id=%d", string(ref.DocumentID), ref.PartID, row.PartID)
	}
	if ref.RowIndex != row.RowIndex {
		return fmt.Errorf("collections: document row ref for id %q row_index=%d does not match physical row_index=%d", string(ref.DocumentID), ref.RowIndex, row.RowIndex)
	}
	if ref.AppliedCommandLSN != row.AppliedCommandLSN {
		return fmt.Errorf("collections: document row ref for id %q applied_command_lsn=%d does not match physical applied_command_lsn=%d", string(ref.DocumentID), ref.AppliedCommandLSN, row.AppliedCommandLSN)
	}
	if row.Deleted {
		return fmt.Errorf("collections: document row ref for id %q points at deleted physical row", string(ref.DocumentID))
	}
	return nil
}

func validateDocumentRowRefMatchesRowRef(ref DocumentRowRef, latest DocumentRowRef) error {
	if len(ref.DocumentID) == 0 {
		return errors.New("collections: document row ref missing document id")
	}
	if len(latest.DocumentID) != 0 && !bytes.Equal(ref.DocumentID, latest.DocumentID) {
		return fmt.Errorf("collections: document row ref id %q does not match latest-visible id %q", string(ref.DocumentID), string(latest.DocumentID))
	}
	if ref.Generation != latest.Generation {
		return fmt.Errorf("collections: document row ref for id %q generation=%d does not match latest-visible generation=%d", string(ref.DocumentID), ref.Generation, latest.Generation)
	}
	if ref.PartID != latest.PartID {
		return fmt.Errorf("collections: document row ref for id %q part_id=%d does not match latest-visible part_id=%d", string(ref.DocumentID), ref.PartID, latest.PartID)
	}
	if ref.RowIndex != latest.RowIndex {
		return fmt.Errorf("collections: document row ref for id %q row_index=%d does not match latest-visible row_index=%d", string(ref.DocumentID), ref.RowIndex, latest.RowIndex)
	}
	if ref.AppliedCommandLSN != latest.AppliedCommandLSN {
		return fmt.Errorf("collections: document row ref for id %q applied_command_lsn=%d does not match latest-visible applied_command_lsn=%d", string(ref.DocumentID), ref.AppliedCommandLSN, latest.AppliedCommandLSN)
	}
	return nil
}

func validateDocumentRowRefMatchesVisibleRow(ref DocumentRowRef, row columnPhysicalVisibleRow) error {
	if len(ref.DocumentID) == 0 {
		return errors.New("collections: document row ref missing document id")
	}
	if !bytes.Equal(ref.DocumentID, row.ID) {
		return fmt.Errorf("collections: document row ref id %q does not match visible row id %q", string(ref.DocumentID), string(row.ID))
	}
	if ref.RowIndex < 0 {
		return fmt.Errorf("collections: document row ref for id %q has negative row index", string(ref.DocumentID))
	}
	if ref.RowIndex != row.RowIndex {
		return fmt.Errorf("collections: document row ref for id %q row_index=%d does not match visible row_index=%d", string(ref.DocumentID), ref.RowIndex, row.RowIndex)
	}
	if ref.Generation != 0 && ref.Generation != row.Generation {
		return fmt.Errorf("collections: document row ref for id %q generation=%d does not match visible generation=%d", string(ref.DocumentID), ref.Generation, row.Generation)
	}
	if ref.PartID != 0 && ref.PartID != row.PartID {
		return fmt.Errorf("collections: document row ref for id %q part_id=%d does not match visible part_id=%d", string(ref.DocumentID), ref.PartID, row.PartID)
	}
	if ref.AppliedCommandLSN != 0 && ref.AppliedCommandLSN != row.AppliedCommandLSN {
		return fmt.Errorf("collections: document row ref for id %q applied_command_lsn=%d does not match visible applied_command_lsn=%d", string(ref.DocumentID), ref.AppliedCommandLSN, row.AppliedCommandLSN)
	}
	return nil
}

func (v *CollectionReadView) typedColumnReconstructionCacheForConfig(cfg ColumnStoreConfig) *typedColumnPartReconstructionCache {
	if v == nil || !columnStoreHasTypedColumnPartOwners(cfg) {
		return nil
	}
	if v.typedColumnReconstructionCache == nil || v.typedColumnReconstructionCache.ReadCache != v.typedColumnAssetReadCache {
		v.typedColumnReconstructionCache = &typedColumnPartReconstructionCache{
			Parts:     make(map[uint64]typedColumnPartDecodedValues),
			ReadCache: v.typedColumnAssetReadCache,
		}
	}
	v.typedColumnReconstructionCache.Prepared = v.preparedMaterializer
	return v.typedColumnReconstructionCache
}

func typedColumnCacheCounters(cache *typedColumnPartReconstructionCache) (hits, misses, loads, decodes uint64) {
	if cache == nil {
		return 0, 0, 0, 0
	}
	return cache.CacheHits, cache.CacheMisses, cache.PartLoads, cache.TypedPartDecodes
}

func addDocumentMaterializationStatsToVectorStats(dst *VectorIndexSearchStats, src DocumentMaterializationStats) error {
	if dst == nil {
		return nil
	}
	dst.DocumentsFetched += src.DocumentsFetched
	dst.DocumentsMissing += src.DocumentsMissing
	dst.DocumentBytes += src.DocumentBytes
	dst.DocumentOutputBytes += src.OutputBytes
	dst.DocumentFieldsReconstructed += src.FieldsReconstructed
	dst.DocumentFieldsSkipped += src.FieldsSkipped
	dst.DocumentEmbeddingVectorReads += src.EmbeddingVectorReads
	dst.DocumentEmbeddingVectorBytes += src.EmbeddingVectorBytes
	dst.DocumentEmbeddingOutputBytes += src.EmbeddingOutputBytes
	dst.DocumentFetchNanos += uint64(maxInt64ForMetric(src.FetchNanos, 0))
	dst.DocumentRetainedFetches += src.RetainedPayloadFetches
	dst.DocumentRetainedBytes += src.RetainedPayloadBytes
	dst.DocumentVisibilityScans += src.VisibilityScans
	dst.DocumentVisibilityRowsScanned += src.VisibilityRowsScanned
	dst.DocumentVisibilityRows += src.VisibilityRows
	visibilityBytes, err := addInt64ToUint64Metric(dst.DocumentVisibilityPhysicalBytes, src.VisibilityPhysicalBytes, "document visibility physical bytes")
	if err != nil {
		return err
	}
	dst.DocumentVisibilityPhysicalBytes = visibilityBytes
	dst.DocumentVisibilityNanos += uint64(maxInt64ForMetric(src.VisibilityNanos, 0))
	dst.DocumentTypedColumnRows += src.TypedColumnRows
	dst.DocumentTypedColumnCacheHits += src.TypedColumnCacheHits
	dst.DocumentTypedColumnCacheMisses += src.TypedColumnCacheMisses
	dst.DocumentTypedColumnPartLoads += src.TypedColumnPartLoads
	dst.DocumentTypedColumnPartDecodes += src.TypedColumnPartDecodes
	dst.DocumentTypedColumnNanos += uint64(maxInt64ForMetric(src.TypedColumnNanos, 0))
	dst.DocumentJSONReconstructionRows += src.JSONReconstructionRows
	dst.DocumentJSONReconstructionNanos += uint64(maxInt64ForMetric(src.JSONReconstructionNanos, 0))
	dst.DocumentRowLocatorBuilds += src.RowLocatorBuilds
	dst.DocumentRowLocatorLookups += src.RowLocatorLookups
	dst.DocumentRowLocatorMisses += src.RowLocatorMisses
	dst.DocumentRowLocatorRowsScanned += src.RowLocatorRowsScanned
	locatorBytes, err := addInt64ToUint64Metric(dst.DocumentRowLocatorPhysicalBytes, src.RowLocatorPhysicalBytes, "document row locator physical bytes")
	if err != nil {
		return err
	}
	dst.DocumentRowLocatorPhysicalBytes = locatorBytes
	dst.DocumentRowLocatorNanos += uint64(maxInt64ForMetric(src.RowLocatorNanos, 0))
	dst.DocumentPointRowFetches += src.PointRowFetches
	dst.DocumentPointRowDecodes += src.PointRowDecodes
	dst.DocumentRowRefFallbackScans += src.RowRefFallbackScans
	dst.DocumentRowRefUnsupported += src.RowRefUnsupported
	dst.DocumentRowRefValidationFailures += src.RowRefValidationFailures
	dst.DocumentAssetMmapHits += src.AssetMmapHits
	dst.DocumentAssetReadAtFallbacks += src.AssetReadAtFallbacks
	dst.DocumentAssetFileOpens += src.AssetFileOpens
	dst.DocumentAssetFileCloses += src.AssetFileCloses
	dst.DocumentAssetActiveHandles = src.AssetActiveHandles
	return nil
}

func addInt64ToUint64Metric(total uint64, n int64, label string) (uint64, error) {
	if n < 0 || uint64(n) > ^uint64(0)-total {
		return 0, fmt.Errorf("collections: %s overflow", label)
	}
	return total + uint64(n), nil
}

func maxInt64ForMetric(n, floor int64) int64 {
	if n < floor {
		return floor
	}
	return n
}

func documentMaterializationWork(s DocumentMaterializationStats) workstats.OutputStats {
	return workstats.OutputStats{Requested: s.DocumentsRequested, Fetched: s.DocumentsFetched, Missing: s.DocumentsMissing,
		OutputBytes: s.OutputBytes, RetainedPayloadFetches: s.RetainedPayloadFetches, JSONReconstructionRows: s.JSONReconstructionRows, TypedColumnRows: s.TypedColumnRows}
}
