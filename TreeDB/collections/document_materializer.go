package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
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
	FetchNanos          int64

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

type documentRowLocator struct {
	byID map[string]DocumentRowRef
}

type documentRowLocatorCandidate struct {
	ref     DocumentRowRef
	ordinal int
	deleted bool
}

// CollectionReadView is a closeable snapshot-bound document materializer for a
// collection. It preserves the catalog/root visibility that existed when the
// view was opened. Returned documents are owned by the fetch response. The view
// is not concurrency-safe; callers that fetch concurrently should open one view
// per worker or synchronize externally.
type CollectionReadView struct {
	collection *Collection
	snapshot   *backenddb.Snapshot
	catalog    *collectionCatalog
	ownsSnap   bool
	closed     bool

	assetScopeKind                  mappedresource.ScopeKind
	assetScopeID                    string
	assetManager                    *mappedresource.Manager
	assetClosedCounters             documentMaterializerAssetCounters
	rowAssetReadCache               *columnPhysicalAssetReadCache
	rowAssetReadIntegrity           ColumnAssetReadIntegrity
	typedColumnAssetReadCache       *columnPhysicalAssetReadCache
	typedColumnReconstructionCache  *typedColumnPartReconstructionCache
	columnSnapshotView              *columnPhysicalScanSnapshotView
	rowLocator                      *documentRowLocator
	pointRowRefs                    map[documentRowPartKey]columnManifestAssetRefForScan
	pointRowBlocks                  map[documentRowPartKey]*columnPhysicalRowReaderBlock
	pointRowProjection              *columnPhysicalScanProjection
	forceAssetReadAtFallbackForTest bool
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
	return errors.Join(cacheErr, snapErr)
}

// FetchDocumentsByID materializes full documents for ids in input order. Missing
// documents produce Found=false results without failing the whole fetch.
func (v *CollectionReadView) FetchDocumentsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
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
	activeHandles   int64
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
	if v.rowAssetReadCache == nil || v.rowAssetReadIntegrity != rowIntegrity || v.rowAssetReadCache.namespace != namespace {
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
		readCache.forceReadAtFallback = v.forceAssetReadAtFallbackForTest
		readCache.trustCachedVerifyFileIdentity = true
		if err := readCache.useMappedResourceManager(v.assetManager, v.assetScope(cfg, "typed-row document materializer"), "document materializer typed-row asset read"); err != nil {
			_ = readCache.close()
			return err
		}
		v.rowAssetReadCache = &readCache
		v.rowAssetReadIntegrity = rowIntegrity
	}
	if v.typedColumnAssetReadCache == nil || v.typedColumnAssetReadCache.namespace != namespace {
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
		readCache.forceReadAtFallback = v.forceAssetReadAtFallbackForTest
		readCache.trustCachedVerifyFileIdentity = true
		if err := readCache.useMappedResourceManager(v.assetManager, v.assetScope(cfg, "typed-column document materializer"), "document materializer typed-column asset read"); err != nil {
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
	}
	if v.typedColumnAssetReadCache != nil {
		stats := v.typedColumnAssetReadCache.lifecycleStats()
		out.mmapHits += stats.MmapHits
		out.readAtFallbacks += stats.ReadAtFallbacks
		out.fileOpens += stats.FileOpens
		out.fileCloses += stats.FileCloses
	}
	if v.assetManager != nil {
		out.activeHandles = v.assetManager.Stats().ActiveHandles
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
	v.rowLocator = nil
	v.columnSnapshotView = nil
	v.pointRowRefs = nil
	v.pointRowProjection = nil
}

// LookupDocumentRowRefsByID resolves document IDs to snapshot-visible typed-row
// refs in input order. It builds or reuses a generic materializer row-locator
// map for the read view; missing or deleted documents return Found=false.
func (v *CollectionReadView) LookupDocumentRowRefsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentRowRefLookupResponse, error) {
	start := time.Now()
	response, err := v.lookupDocumentRowRefsByID(ids, opts)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
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
	locatorRootName := collectionColumnRowLocatorRootName(v.catalog.meta.Name)
	if v.catalog.rootID(locatorRootName) == 0 {
		primaryRootName := collectionPrimaryRootName(v.catalog.meta.Name)
		if v.catalog.rootID(primaryRootName) == 0 && len(v.catalog.overlayRootIDs(primaryRootName)) == 0 {
			response.Stats.RowLocatorLookups = uint64(len(ids))
			response.Stats.RowLocatorMisses = uint64(len(ids))
			return response, nil
		}
		return response, fmt.Errorf("collections: primary row locator root is absent for collection %q", v.catalog.meta.Name)
	}
	for i := range response.Results {
		ownedID := response.Results[i].ID
		response.Stats.RowLocatorLookups++
		value, found, err := collectionGetAppendAtCatalogRoot(v.snapshot, v.catalog, locatorRootName, ownedID, nil)
		if err != nil {
			return response, fmt.Errorf("collections: primary row locator lookup for id %q: %w", string(ownedID), err)
		}
		if !found {
			response.Stats.RowLocatorMisses++
			continue
		}
		rowRef, err := decodeColumnPrimaryRowLocator(ownedID, value)
		if err != nil {
			return response, err
		}
		response.Results[i].RowRef = rowRef
		response.Results[i].Found = true
	}
	return response, nil
}

func (v *CollectionReadView) materializerColumnSnapshotView(cfg ColumnStoreConfig) (columnPhysicalScanSnapshotView, error) {
	if v == nil || v.collection == nil || v.catalog == nil || v.snapshot == nil {
		return columnPhysicalScanSnapshotView{}, errors.New("collections: nil collection read view")
	}
	if v.columnSnapshotView != nil {
		return *v.columnSnapshotView, nil
	}
	collectionName := v.catalog.meta.Name
	rootID := v.catalog.rootID(collectionColumnManifestRootName(collectionName))
	view, err := v.collection.prepareColumnPhysicalScanSnapshotViewAtSnapshotWithSidecars(v.snapshot, v.catalog, collectionName, rootID, cfg, true, columnManifestScanNoSidecars())
	if err != nil {
		return columnPhysicalScanSnapshotView{}, err
	}
	v.columnSnapshotView = &view
	return view, nil
}

func (v *CollectionReadView) ensureDocumentRowLocator(cfg ColumnStoreConfig, readIntegrity ColumnAssetReadIntegrity, stats *DocumentMaterializationStats) error {
	if v == nil {
		return errors.New("collections: nil collection read view")
	}
	if v.rowLocator != nil {
		return nil
	}
	if err := v.ensureAssetReadCaches(cfg, readIntegrity); err != nil {
		return err
	}
	view, err := v.materializerColumnSnapshotView(cfg)
	if err != nil {
		return err
	}
	projection := noColumnPhysicalScanProjection(view.Config)
	latest := make(map[string]documentRowLocatorCandidate, max(len(view.AssetRefs), 1))
	var rawScratch []byte
	buildStart := time.Now()
	if stats != nil {
		stats.RowLocatorBuilds++
	}
	for ordinal, assetRef := range view.AssetRefs {
		if assetRef.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return fmt.Errorf("collections: document row locator unsupported asset kind %q", assetRef.Ref.Kind)
		}
		raw, err := v.rowAssetReadCache.read(assetRef.Ref, rawScratch)
		if err != nil {
			return fmt.Errorf("collections: document row locator read generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
		}
		rawScratch = raw
		if stats != nil {
			stats.RowLocatorPhysicalBytes += int64(len(raw))
		}
		summary, err := scanColumnPhysicalAssetRowsWithManifestOperation(raw, assetRef.Ref, view.CollectionName, &view.Config, projection, assetRef.Reason, func(row columnPhysicalScanRowView) error {
			key := string(row.ID)
			candidate := documentRowLocatorCandidate{ref: documentRowRefFromScanRowView(row), ordinal: ordinal, deleted: row.Deleted}
			if existing, ok := latest[key]; !ok || documentRowLocatorCandidateNewer(candidate, existing) {
				latest[key] = candidate
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("collections: document row locator decode generation=%d part_id=%d: %w", assetRef.Ref.Generation, assetRef.Ref.PartID, err)
		}
		if stats != nil {
			stats.RowLocatorRowsScanned += uint64(summary.rows)
		}
	}
	locator := &documentRowLocator{byID: make(map[string]DocumentRowRef, len(latest))}
	for key, candidate := range latest {
		if candidate.deleted {
			continue
		}
		locator.byID[key] = candidate.ref
	}
	v.rowLocator = locator
	if stats != nil {
		stats.RowLocatorNanos += time.Since(buildStart).Nanoseconds()
	}
	return nil
}

func noColumnPhysicalScanProjection(cfg ColumnStoreConfig) columnPhysicalScanProjection {
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(cfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	return projection
}

func (v *CollectionReadView) validateDocumentRowRefLatest(ref DocumentRowRef, stats *DocumentMaterializationStats) error {
	if v == nil || v.catalog == nil || v.snapshot == nil {
		return errors.New("collections: nil collection read view")
	}
	if stats != nil {
		stats.RowLocatorLookups++
	}
	locatorRootName := collectionColumnRowLocatorRootName(v.catalog.meta.Name)
	if v.catalog.rootID(locatorRootName) == 0 {
		return fmt.Errorf("collections: primary row locator root is absent for collection %q", v.catalog.meta.Name)
	}
	value, found, err := collectionGetAppendAtCatalogRoot(v.snapshot, v.catalog, locatorRootName, ref.DocumentID, nil)
	if err != nil {
		return fmt.Errorf("collections: primary row locator validation for id %q: %w", string(ref.DocumentID), err)
	}
	if !found || len(value) == 0 {
		if stats != nil {
			stats.RowLocatorMisses++
			stats.RowRefValidationFailures++
		}
		return fmt.Errorf("collections: document row ref for id %q is not visible in primary root", string(ref.DocumentID))
	}
	latest, err := decodeColumnPrimaryRowLocator(ref.DocumentID, value)
	if err != nil {
		return err
	}
	if err := validateDocumentRowRefMatchesRowRef(ref, latest); err != nil {
		if stats != nil {
			stats.RowRefValidationFailures++
		}
		return err
	}
	return nil
}

func (v *CollectionReadView) fetchDocumentPointRow(view columnPhysicalScanSnapshotView, ref DocumentRowRef, projection columnPhysicalScanProjection, scratch *columnPhysicalRowReaderScratch, stats *DocumentMaterializationStats) (columnPhysicalVisibleRow, error) {
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
	key := documentRowPartKey{Generation: ref.Generation, PartID: ref.PartID}
	assetRef, ok := v.pointRowRefs[key]
	if !ok {
		return columnManifestAssetRefForScan{}, fmt.Errorf("collections: document row ref for id %q generation=%d part_id=%d is not present in snapshot", string(ref.DocumentID), ref.Generation, ref.PartID)
	}
	return assetRef, nil
}

func (v *CollectionReadView) loadPointRowBlock(view columnPhysicalScanSnapshotView, assetRef columnManifestAssetRefForScan) (*columnPhysicalRowReaderBlock, error) {
	key := documentRowPartKey{Generation: assetRef.Ref.Generation, PartID: assetRef.Ref.PartID}
	if block := v.pointRowBlocks[key]; block != nil {
		return block, nil
	}
	if v.rowAssetReadCache == nil {
		return nil, errors.New("collections: document row point fetch requires row asset read cache")
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
	header = cloneColumnPhysicalAssetScanHeader(header)
	rowIndex, err := indexColumnPhysicalAssetReaderRows(raw, version, rowsOffset, header, &view.Config)
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
		residentBytes: int64(len(raw)),
	}
	if v.pointRowBlocks == nil {
		v.pointRowBlocks = make(map[documentRowPartKey]*columnPhysicalRowReaderBlock)
	}
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
	start := time.Now()
	response, err := v.fetchDocumentsByRowRef(refs, opts, false)
	response.Stats.FetchNanos = time.Since(start).Nanoseconds()
	return response, err
}

// fetchDocumentsByResolvedRowRef is reserved for refs obtained from this read
// view's persistent locator root. The lookup and fetch share one immutable
// snapshot, so repeating latest-locator validation would be redundant. Primary
// visibility and physical-row coordinate validation still fail closed below.
func (v *CollectionReadView) fetchDocumentsByResolvedRowRef(refs []DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	return v.fetchDocumentsByRowRef(refs, opts, true)
}

func (v *CollectionReadView) fetchDocumentsByRowRef(refs []DocumentRowRef, opts DocumentFetchOptions, locatorResolvedAtView bool) (DocumentFetchResponse, error) {
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
	response, retained, foundCount, err := v.fetchRetainedPayloadsByID(ids)
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
	return v.fetchColumnStoreDocumentsByRowRef(response, refs, retained, opts, projection, locatorResolvedAtView)
}

func (v *CollectionReadView) fetchDocumentsByID(ids [][]byte, expected []*DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
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
	response, retained, foundCount, err := v.fetchRetainedPayloadsByID(ids)
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

func (v *CollectionReadView) fetchRetainedPayloadsByID(ids [][]byte) (DocumentFetchResponse, [][]byte, int, error) {
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

func (v *CollectionReadView) fetchColumnStoreDocumentsByRowRef(response DocumentFetchResponse, refs []DocumentRowRef, retained [][]byte, opts DocumentFetchOptions, projection *documentProjection, locatorResolvedAtView bool) (out DocumentFetchResponse, err error) {
	out = response
	cfg := v.catalog.meta.Options.ColumnStore.copy()
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
	view, err := v.materializerColumnSnapshotView(cfg)
	if err != nil {
		return out, err
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
	typedScratch := make([]columnDeclaredValue, 0, len(columnStoreTypedColumnPartFields(cfg)))
	mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
	retainedTemplateResolver := columnRetainedPayloadTemplateResolver(v.snapshot, v.catalog)
	var rowScratch columnPhysicalRowReaderScratch
	var documentArena []byte
	for i := range out.Results {
		if !out.Results[i].Found {
			out.Stats.RowRefValidationFailures++
			return out, fmt.Errorf("collections: document row ref for id %q is not visible in primary root", string(out.Results[i].ID))
		}
		if !locatorResolvedAtView {
			if err := v.validateDocumentRowRefLatest(refs[i], &out.Stats); err != nil {
				return out, err
			}
		}
		row, err := v.fetchDocumentPointRow(view, refs[i], pointProjection, &rowScratch, &out.Stats)
		if err != nil {
			out.Stats.RowRefValidationFailures++
			return out, err
		}
		rowRef := documentRowRefCoordinatesFromVisibleRow(row)
		rowRef.DocumentID = out.Results[i].ID
		out.Results[i].RowRef = rowRef

		beforeCacheHits, beforeCacheMisses, beforePartLoads, beforePartDecodes := typedColumnCacheCounters(typedColumnCache)
		typedStart := time.Now()
		typedValues, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCacheProjected(v.snapshot, manifestRootID, cfg, row, typedColumnCache, typedScratch, typedProjection)
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
		fullValues, err := mergeColumnReconstructionValuesProjectedInto(cfg, row.Values, typedValues.Values, selectedColumns, mergeScratch)
		if err != nil {
			return out, err
		}
		var document []byte
		documentArena, document, err = reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(documentArena, cfg, retained[i], row, fullValues, projection, &out.Stats, retainedTemplateResolver)
		if err != nil {
			return out, err
		}
		out.Stats.JSONReconstructionNanos += time.Since(reconstructStart).Nanoseconds()
		out.Stats.JSONReconstructionRows++
		out.Results[i].Document = document
		out.Stats.DocumentsFetched++
		out.Stats.DocumentBytes += uint64(len(out.Results[i].Document))
		out.Stats.OutputBytes += uint64(len(out.Results[i].Document))
	}
	return out, nil
}

func (v *CollectionReadView) fetchColumnStoreDocumentsByID(response DocumentFetchResponse, ids [][]byte, retained [][]byte, expected []*DocumentRowRef, opts DocumentFetchOptions, projection *documentProjection) (out DocumentFetchResponse, err error) {
	out = response
	cfg := v.catalog.meta.Options.ColumnStore.copy()
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
	selectedColumns := documentProjectionSelectedColumns(cfg, projection)
	rowProjection := documentProjectionRowAssetColumns(cfg, selectedColumns)
	typedProjection := documentProjectionTypedColumnPartSelection(cfg, selectedColumns)
	visibleStart := time.Now()
	visible, err := v.collection.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadCache(
		v.snapshot,
		v.catalog,
		v.catalog.meta.Name,
		v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name)),
		cfg,
		true,
		newColumnPhysicalVisibilityTargetIDs(ids),
		rowProjection,
		readIntegrity,
		v.rowAssetReadCache,
	)
	out.Stats.VisibilityNanos = time.Since(visibleStart).Nanoseconds()
	out.Stats.VisibilityScans++
	out.Stats.VisibilityRowsScanned = uint64(visible.Diagnostics.RowsScanned)
	out.Stats.VisibilityRows = uint64(len(visible.Rows))
	out.Stats.VisibilityPhysicalBytes = visible.Diagnostics.PhysicalBytesScanned
	if err != nil {
		return out, err
	}
	visibleByID := make(map[string]columnPhysicalVisibleRow, len(visible.Rows))
	for _, row := range visible.Rows {
		visibleByID[string(row.ID)] = row
	}
	typedColumnCache := v.typedColumnReconstructionCacheForConfig(cfg)
	manifestRootID := v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name))
	typedScratch := make([]columnDeclaredValue, 0, len(columnStoreTypedColumnPartFields(cfg)))
	mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
	retainedTemplateResolver := columnRetainedPayloadTemplateResolver(v.snapshot, v.catalog)
	var documentArena []byte
	for i := range out.Results {
		if !out.Results[i].Found {
			continue
		}
		row, ok := visibleByID[string(out.Results[i].ID)]
		if !ok {
			return out, fmt.Errorf("collections: column reconstruction missing visible physical row for id %q", string(out.Results[i].ID))
		}
		if row.Deleted {
			return out, fmt.Errorf("collections: column reconstruction latest physical row is deleted for id %q", string(out.Results[i].ID))
		}
		rowRef := documentRowRefFromVisibleRow(row)
		if expected != nil && expected[i] != nil {
			if err := validateDocumentRowRefMatchesVisibleRow(*expected[i], row); err != nil {
				return out, err
			}
			rowRef.DocumentID = append(rowRef.DocumentID[:0], expected[i].DocumentID...)
		}
		out.Results[i].RowRef = rowRef

		beforeCacheHits, beforeCacheMisses, beforePartLoads, beforePartDecodes := typedColumnCacheCounters(typedColumnCache)
		typedStart := time.Now()
		typedValues, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCacheProjected(v.snapshot, manifestRootID, cfg, row, typedColumnCache, typedScratch, typedProjection)
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
		fullValues, err := mergeColumnReconstructionValuesProjectedInto(cfg, row.Values, typedValues.Values, selectedColumns, mergeScratch)
		if err != nil {
			return out, err
		}
		var document []byte
		documentArena, document, err = reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(documentArena, cfg, retained[i], row, fullValues, projection, &out.Stats, retainedTemplateResolver)
		if err != nil {
			return out, err
		}
		out.Stats.JSONReconstructionNanos += time.Since(reconstructStart).Nanoseconds()
		out.Stats.JSONReconstructionRows++
		out.Results[i].Document = document
		out.Stats.DocumentsFetched++
		out.Stats.DocumentBytes += uint64(len(out.Results[i].Document))
		out.Stats.OutputBytes += uint64(len(out.Results[i].Document))
	}
	return out, nil
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

func documentRowRefFromScanRowView(row columnPhysicalScanRowView) DocumentRowRef {
	// Row locators are keyed by document ID, so the stored ref only needs the
	// physical row coordinates. Lookup callers attach an owned DocumentID to the
	// response row ref; avoiding a per-row ID clone keeps locator builds cheap.
	return DocumentRowRef{
		Generation:        row.Generation,
		PartID:            row.PartID,
		RowIndex:          row.RowIndex,
		AppliedCommandLSN: row.AppliedCommandLSN,
	}
}

func documentRowLocatorCandidateNewer(a, b documentRowLocatorCandidate) bool {
	if a.ref.AppliedCommandLSN != b.ref.AppliedCommandLSN {
		return a.ref.AppliedCommandLSN > b.ref.AppliedCommandLSN
	}
	if a.ref.Generation != b.ref.Generation {
		return a.ref.Generation > b.ref.Generation
	}
	if a.ref.PartID != b.ref.PartID {
		return a.ref.PartID > b.ref.PartID
	}
	if a.ref.RowIndex != b.ref.RowIndex {
		return a.ref.RowIndex > b.ref.RowIndex
	}
	return a.ordinal > b.ordinal
}

func columnPhysicalVisibleRowFromReaderRow(row columnPhysicalRowReaderRow) columnPhysicalVisibleRow {
	return columnPhysicalVisibleRow{
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
