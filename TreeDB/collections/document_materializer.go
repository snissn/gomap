package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// DocumentFetchOptions configures snapshot-bound document materialization.
// Projection and row-locator point-fetch options are intentionally deferred to
// later materializer milestones; the zero value preserves Collection.Get-style
// full-document output and verified column-asset reads.
type DocumentFetchOptions struct {
	// ColumnAssetReadIntegrity controls typed-row/physical row asset reads used
	// to find the visible row for a document. Typed-column part reconstruction
	// continues to use the default verified read path until the prepared asset
	// lifecycle lands.
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

// DocumentRowRef identifies a document row in a snapshot-bound typed-storage
// materialization request. #1872 keeps the row-ref API seam but still resolves
// documents through the batched document-ID scan; direct O(topK) row-locator
// point fetch is deferred to #1874.
type DocumentRowRef struct {
	DocumentID        []byte
	Generation        uint64
	PartID            uint64
	RowIndex          int
	AppliedCommandLSN uint64
}

// DocumentFetchResult is one materialized document result. ID and Document are
// response-owned slices. Missing documents have Found=false and Document=nil.
// RowRef is populated only when typed-storage reconstruction found a visible
// physical row; it is zero for retained-payload-only results.
type DocumentFetchResult struct {
	ID       []byte
	Document []byte
	Found    bool
	RowRef   DocumentRowRef
}

// DocumentMaterializationStats attributes the work performed by a
// CollectionReadView fetch. Counters describe the fetch call, not the lifetime
// of the read view.
type DocumentMaterializationStats struct {
	DocumentsRequested uint64
	DocumentsFetched   uint64
	DocumentsMissing   uint64
	DocumentBytes      uint64

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

	RowRefFallbackScans uint64
}

// DocumentFetchResponse contains ordered materialization results and per-call
// diagnostics.
type DocumentFetchResponse struct {
	Results []DocumentFetchResult
	Stats   DocumentMaterializationStats
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
	view := newCollectionReadViewAtSnapshot(c, snap, catalog, true)
	closeOnErr = false
	return view, nil
}

func newCollectionReadViewAtSnapshot(c *Collection, snap *backenddb.Snapshot, catalog *collectionCatalog, ownsSnap bool) *CollectionReadView {
	return &CollectionReadView{collection: c, snapshot: snap, catalog: catalog, ownsSnap: ownsSnap}
}

// Close releases the snapshot owned by the read view. Views that are bound to a
// caller-owned snapshot still become closed, but leave snapshot release to the
// owner.
func (v *CollectionReadView) Close() error {
	if v == nil || v.closed {
		return nil
	}
	v.closed = true
	if v.ownsSnap && v.snapshot != nil {
		err := v.snapshot.Close()
		v.snapshot = nil
		return err
	}
	return nil
}

// FetchDocumentsByID materializes full documents for ids in input order. Missing
// documents produce Found=false results without failing the whole fetch.
func (v *CollectionReadView) FetchDocumentsByID(ids [][]byte, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	return v.fetchDocumentsByID(ids, nil, opts)
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

// FetchDocumentsByRowRef materializes full documents for row refs in input
// order. The current implementation validates the row ref against the visible
// row discovered by a batched document-ID scan; direct row-locator fetch is
// intentionally deferred to #1874.
func (v *CollectionReadView) FetchDocumentsByRowRef(refs []DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	if err := v.validateOpen(); err != nil {
		return DocumentFetchResponse{}, err
	}
	if len(refs) == 0 {
		return DocumentFetchResponse{}, nil
	}
	ids := make([][]byte, len(refs))
	expected := make([]*DocumentRowRef, len(refs))
	for i := range refs {
		if len(refs[i].DocumentID) == 0 {
			return DocumentFetchResponse{}, fmt.Errorf("collections: document row ref %d missing document id", i)
		}
		if refs[i].RowIndex < 0 {
			return DocumentFetchResponse{}, fmt.Errorf("collections: document row ref %d has negative row index", i)
		}
		ids[i] = refs[i].DocumentID
		expected[i] = &refs[i]
	}
	response, err := v.fetchDocumentsByID(ids, expected, opts)
	response.Stats.RowRefFallbackScans++
	return response, err
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
	response := DocumentFetchResponse{
		Results: make([]DocumentFetchResult, len(ids)),
		Stats: DocumentMaterializationStats{
			DocumentsRequested: uint64(len(ids)),
		},
	}
	var idArena []byte
	retained := make([][]byte, len(ids))
	foundCount := 0
	for i, id := range ids {
		if len(id) == 0 {
			return response, fmt.Errorf("collections: document id at position %d cannot be empty", i)
		}
		idStart := len(idArena)
		idArena = append(idArena, id...)
		ownedID := idArena[idStart:len(idArena):len(idArena)]
		response.Results[i].ID = ownedID
		value, found, err := collectionGetAppendAtCatalogRoot(v.snapshot, v.catalog, collectionPrimaryRootName(v.catalog.meta.Name), id, nil)
		if err != nil {
			return response, err
		}
		if !found {
			response.Stats.DocumentsMissing++
			continue
		}
		response.Results[i].Found = true
		retained[i] = value
		foundCount++
		response.Stats.RetainedPayloadFetches++
		response.Stats.RetainedPayloadBytes += uint64(len(value))
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
			documentArena = appendDocumentFetchOwnedBytes(documentArena, retained[i], &response.Results[i])
			response.Stats.DocumentsFetched++
			response.Stats.DocumentBytes += uint64(len(response.Results[i].Document))
		}
		return response, nil
	}
	return v.fetchColumnStoreDocumentsByID(response, ids, retained, expected, opts)
}

func (v *CollectionReadView) fetchColumnStoreDocumentsByID(response DocumentFetchResponse, ids [][]byte, retained [][]byte, expected []*DocumentRowRef, opts DocumentFetchOptions) (DocumentFetchResponse, error) {
	cfg := v.catalog.meta.Options.ColumnStore.copy()
	readIntegrity := opts.ColumnAssetReadIntegrity
	if readIntegrity == "" {
		readIntegrity = ColumnAssetReadIntegrityVerify
	}
	visibleStart := time.Now()
	visible, err := v.collection.scanColumnPhysicalVisibleRowsAtSnapshotForTargetsWithReadIntegrity(
		v.snapshot,
		v.catalog,
		v.catalog.meta.Name,
		v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name)),
		cfg,
		true,
		newColumnPhysicalVisibilityTargetIDs(ids),
		nil,
		readIntegrity,
	)
	response.Stats.VisibilityNanos = time.Since(visibleStart).Nanoseconds()
	response.Stats.VisibilityScans++
	response.Stats.VisibilityRowsScanned = uint64(visible.Diagnostics.RowsScanned)
	response.Stats.VisibilityRows = uint64(len(visible.Rows))
	response.Stats.VisibilityPhysicalBytes = visible.Diagnostics.PhysicalBytesScanned
	if err != nil {
		return response, err
	}
	visibleByID := make(map[string]columnPhysicalVisibleRow, len(visible.Rows))
	for _, row := range visible.Rows {
		visibleByID[string(row.ID)] = row
	}
	var typedColumnCache *typedColumnPartReconstructionCache
	if columnStoreHasTypedColumnPartOwners(cfg) {
		typedColumnCache = &typedColumnPartReconstructionCache{Parts: make(map[uint64]typedColumnPartDecodedValues)}
	}
	manifestRootID := v.catalog.rootID(collectionColumnManifestRootName(v.catalog.meta.Name))
	typedScratch := make([]columnDeclaredValue, 0, len(columnStoreTypedColumnPartFields(cfg)))
	mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
	var documentArena []byte
	for i := range response.Results {
		if !response.Results[i].Found {
			continue
		}
		row, ok := visibleByID[string(response.Results[i].ID)]
		if !ok {
			return response, fmt.Errorf("collections: column reconstruction missing visible physical row for id %q", string(response.Results[i].ID))
		}
		if row.Deleted {
			return response, fmt.Errorf("collections: column reconstruction latest physical row is deleted for id %q", string(response.Results[i].ID))
		}
		rowRef := documentRowRefFromVisibleRow(row)
		if expected != nil && expected[i] != nil {
			if err := validateDocumentRowRefMatchesVisibleRow(*expected[i], row); err != nil {
				return response, err
			}
			rowRef.DocumentID = append(rowRef.DocumentID[:0], expected[i].DocumentID...)
		}
		response.Results[i].RowRef = rowRef

		beforeCacheHits, beforeCacheMisses, beforePartLoads, beforePartDecodes := typedColumnCacheCounters(typedColumnCache)
		typedStart := time.Now()
		typedValues, err := v.collection.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(v.snapshot, manifestRootID, cfg, row, typedColumnCache, typedScratch)
		typedElapsed := time.Since(typedStart)
		if err != nil {
			return response, err
		}
		if len(typedValues.Values) > 0 || columnStoreHasTypedColumnPartOwners(cfg) {
			response.Stats.TypedColumnRows++
		}
		afterCacheHits, afterCacheMisses, afterPartLoads, afterPartDecodes := typedColumnCacheCounters(typedColumnCache)
		response.Stats.TypedColumnCacheHits += deltaUint64(beforeCacheHits, afterCacheHits)
		response.Stats.TypedColumnCacheMisses += deltaUint64(beforeCacheMisses, afterCacheMisses)
		response.Stats.TypedColumnPartLoads += deltaUint64(beforePartLoads, afterPartLoads)
		response.Stats.TypedColumnPartDecodes += deltaUint64(beforePartDecodes, afterPartDecodes)
		response.Stats.TypedColumnNanos += typedElapsed.Nanoseconds()

		reconstructStart := time.Now()
		fullValues, err := mergeColumnReconstructionValuesInto(cfg, row.Values, typedValues.Values, mergeScratch)
		if err != nil {
			return response, err
		}
		reconstructed, err := reconstructColumnDocumentFromVisibleRowValues(cfg, retained[i], row, fullValues)
		if err != nil {
			return response, err
		}
		response.Stats.JSONReconstructionNanos += time.Since(reconstructStart).Nanoseconds()
		response.Stats.JSONReconstructionRows++
		documentArena = appendDocumentFetchOwnedBytes(documentArena, reconstructed, &response.Results[i])
		response.Stats.DocumentsFetched++
		response.Stats.DocumentBytes += uint64(len(response.Results[i].Document))
	}
	return response, nil
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
	return DocumentRowRef{
		DocumentID:        bytes.Clone(row.ID),
		Generation:        row.Generation,
		PartID:            row.PartID,
		RowIndex:          row.RowIndex,
		AppliedCommandLSN: row.AppliedCommandLSN,
	}
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
	dst.DocumentRowRefFallbackScans += src.RowRefFallbackScans
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
