package collections

import (
	"bytes"
	"errors"
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// columnReconstructionMonotonicBatchSize bounds all reconstruction-owned
// records and visible rows on the certified insert-only fast path.
const columnReconstructionMonotonicBatchSize = 256

var errColumnReconstructionMonotonicStop = errors.New("collections: stop monotonic reconstruction scan")
var errColumnReconstructionMonotonicPreflightLimit = errors.New("collections: stop monotonic reconstruction preflight")

// columnStoreSupportsMonotonicSelectedTypedReconstruction is a metadata-only
// gate for the certified path. That path asks each typed-column part to decode
// selected rows; keep it opt-in to exactly the shapes supported by
// scanColumnValuesRows so incompatible layouts retain the generic, full-part
// reconstruction behavior.
func columnStoreSupportsMonotonicSelectedTypedReconstruction(cfg ColumnStoreConfig) bool {
	for _, key := range cfg.SortKey {
		for _, column := range cfg.Columns {
			if column.Name == key.Column && columnStoreColumnIsTypedColumnPart(column) {
				// Typed-column parts are stored in their sort-key order, while
				// visible RowIndex remains in primary/physical order. The certified
				// path has no locator remapping, so retain generic reconstruction.
				return false
			}
		}
	}
	for _, field := range columnStoreTypedColumnPartFields(cfg) {
		if field.Nullable {
			return false
		}
		if columnStoreValueTypeIsPrimitiveScalar(field.ValueType) ||
			columnStoreValueTypeIsDenseNumericVector(field.ValueType) ||
			field.ValueType == ColumnStoreValueByteVector ||
			columnStoreValueTypeIsPackedUintVector(field.ValueType) {
			continue
		}
		switch field.ValueType {
		case ColumnStoreValueString, ColumnStoreValueInt64, ColumnStoreValueBool:
			continue
		default:
			return false
		}
	}
	return true
}

// preflightMonotonicColumnReconstruction certifies that the physical row assets
// and primary root describe the same ordered, insert-only snapshot. Any normal
// mutation or ordering mismatch declines the fast path; structural mismatch
// fails closed.
func (c *Collection) preflightMonotonicColumnReconstruction(snap *backenddb.Snapshot, catalog *collectionCatalog, maxDocuments int) (bool, columnPhysicalScanDiagnostics, bool, error) {
	cfg := catalog.meta.Options.ColumnStore.copy()
	if !columnStoreSupportsMonotonicSelectedTypedReconstruction(cfg) {
		return false, columnPhysicalScanDiagnostics{}, false, nil
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(snap, catalog, catalog.meta.Name, rootID, cfg, true)
	if err != nil {
		return false, view.Diagnostics, false, err
	}
	if view.MutationParts != 0 {
		return false, view.Diagnostics, false, nil
	}
	primary, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil {
		return false, view.Diagnostics, false, err
	}
	if primary == nil {
		return false, view.Diagnostics, false, nil
	}
	defer func() { _ = primary.Close() }()

	classifier := monotonicColumnReconstructionPreflight{eligible: true}
	physicalRows := 0
	diag, err := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{ProjectedColumns: []string{}, Visitor: func(row columnPhysicalScanRowView) error {
		physicalRows++
		// A preflight must inspect one additional row to distinguish a complete
		// maxDocuments-row stream from a longer stream. Do not certify a prefix:
		// the bounded generic path reconstructs that prefix without a whole
		// physical-stream pass.
		if physicalRows > maxDocuments {
			return errColumnReconstructionMonotonicPreflightLimit
		}
		if primary.Valid() {
			classifier.observe(row, primary.UnsafeKey(), true)
			primary.Next()
			if err := primary.Error(); err != nil {
				return err
			}
			return nil
		}
		classifier.observe(row, nil, false)
		return nil
	}})
	if errors.Is(err, errColumnReconstructionMonotonicPreflightLimit) {
		// The physical scanner reports completed assets only. Preserve the exact
		// partial row count for callers enforcing a bounded scan budget.
		diag.RowsScanned = physicalRows
		return false, diag, true, nil
	}
	if err != nil {
		return false, diag, true, err
	}
	if err := primary.Error(); err != nil {
		return false, diag, true, err
	}
	certified, err := classifier.finish(primary.Valid())
	if err != nil {
		return false, diag, true, err
	}
	return certified, diag, true, nil
}

// monotonicColumnReconstructionPreflight is deliberately independent of the
// storage scanner so malformed ordered-ID streams can be tested directly.
type monotonicColumnReconstructionPreflight struct {
	previous                           []byte
	previousGeneration, previousPartID uint64
	havePreviousPart                   bool
	eligible, primaryMismatch, orphan  bool
	duplicate                          bool
}

func (s *monotonicColumnReconstructionPreflight) observe(row columnPhysicalScanRowView, primaryID []byte, primaryValid bool) {
	if row.Deleted || row.Operation != ColumnPublishOperationInsert {
		s.eligible = false
		return
	}
	if s.previous != nil {
		order := bytes.Compare(s.previous, row.ID)
		if order == 0 {
			s.duplicate = true
		}
		if order > 0 {
			s.eligible = false
		}
	}
	if s.havePreviousPart && (row.Generation < s.previousGeneration || (row.Generation == s.previousGeneration && row.PartID < s.previousPartID)) {
		s.eligible = false
	}
	s.previousGeneration, s.previousPartID, s.havePreviousPart = row.Generation, row.PartID, true
	if !primaryValid {
		s.orphan = true
		return
	}
	if !bytes.Equal(primaryID, row.ID) {
		s.primaryMismatch = true
	}
	s.previous = append(s.previous[:0], row.ID...)
}

func (s *monotonicColumnReconstructionPreflight) finish(primaryValid bool) (bool, error) {
	if s.duplicate {
		return false, errors.New("collections: monotonic reconstruction duplicate physical id")
	}
	if !s.eligible {
		return false, nil
	}
	if s.orphan || s.primaryMismatch {
		return false, errors.New("collections: monotonic reconstruction primary/physical id mismatch")
	}
	if primaryValid {
		return false, errors.New("collections: monotonic reconstruction primary row missing physical match")
	}
	return true, nil
}

func (c *Collection) scanDocumentsFuncWithMonotonicColumnReconstruction(snap *backenddb.Snapshot, catalog *collectionCatalog, maxDocuments int, fn func(DocumentRecord) (bool, error), stats *CollectionDocumentScanStats) (bool, error) {
	cfg := catalog.meta.Options.ColumnStore.copy()
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	view, err := c.prepareColumnPhysicalScanSnapshotViewAtSnapshot(snap, catalog, catalog.meta.Name, rootID, cfg, true)
	if err != nil {
		return false, err
	}
	primary, err := collectionIteratorAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), nil, nil, false)
	if err != nil || primary == nil {
		return false, err
	}
	defer func() { _ = primary.Close() }()

	var typedCache *typedColumnPartReconstructionCache
	if columnStoreHasTypedColumnPartOwners(cfg) {
		readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
		if err != nil {
			return false, err
		}
		typedCache = &typedColumnPartReconstructionCache{ReadCache: &readCache}
		defer func() { _ = typedCache.ReadCache.close() }()
	}
	// Retained semantic blocks may span reconstruction windows. Keep the cache
	// for the whole certified scan so a block is decoded once, while its fixed
	// capacity keeps the retained state bounded independently of row count.
	var retainedCache *columnRetainedSemanticStreamV1DecodeCache
	if columnStoreRetainedPayloadUsesSemanticStreamV1(&cfg) {
		retainedCache = newColumnRetainedSemanticStreamV1DecodeCache()
	}

	records := make([]DocumentRecord, 0, min(maxDocuments, columnReconstructionMonotonicBatchSize))
	visibleRows := make([]columnPhysicalVisibleRow, 0, cap(records))
	scanned := 0
	visitedPhysicalRows := 0
	stopped := false
	var callbackErr error
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		if stats != nil {
			stats.MaxRecordWindow = max(stats.MaxRecordWindow, uint64(len(records)))
			stats.MaxVisibleRowWindow = max(stats.MaxVisibleRowWindow, uint64(len(visibleRows)))
		}
		manifestRootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
		mergeScratch := make([]columnDeclaredValue, 0, len(cfg.Columns))
		resolver := columnRetainedPayloadTemplateResolver(snap, catalog)
		typedWindow, err := c.typedColumnPartValuesForMonotonicWindow(snap, manifestRootID, cfg, visibleRows, typedCache)
		if err != nil {
			return err
		}
		if stats != nil && typedCache != nil {
			stats.MaxTypedGenerations = max(stats.MaxTypedGenerations, typedCache.WindowGenerations)
			stats.MaxTypedDecodedBytes = max(stats.MaxTypedDecodedBytes, typedCache.WindowDecodedBytes)
			stats.MaxTypedSourcePartBytes = max(stats.MaxTypedSourcePartBytes, typedCache.WindowSourcePartBytes)
		}
		for i := range records {
			fullValues, err := mergeColumnReconstructionValuesInto(cfg, visibleRows[i].Values, typedWindow[i].Values, mergeScratch)
			if err != nil {
				return err
			}
			retained, err := resolveColumnRetainedPayloadAtSnapshotWithCache(snap, catalog, cfg, records[i].Document, retainedCache)
			if err != nil {
				return err
			}
			_, reconstructed, err := reconstructColumnDocumentFromVisibleRowValuesProjectedIntoWithResolver(nil, cfg, retained, visibleRows[i], fullValues, nil, nil, resolver)
			if err != nil {
				return err
			}
			records[i].Document = reconstructed
			if stats != nil {
				stats.ReconstructedRows++
				if retainedCache != nil {
					stats.MaxRetainedBlocks = max(stats.MaxRetainedBlocks, uint64(len(retainedCache.blocks)))
				}
			}
			next, err := fn(records[i])
			if err != nil {
				return err
			}
			if !next {
				stopped = true
				return nil
			}
		}
		records = records[:0]
		visibleRows = visibleRows[:0]
		return nil
	}
	diag, scanErr := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{Visitor: func(row columnPhysicalScanRowView) error {
		visitedPhysicalRows++
		if scanned >= maxDocuments {
			return errColumnReconstructionMonotonicStop
		}
		if !primary.Valid() || !bytes.Equal(primary.UnsafeKey(), row.ID) {
			return fmt.Errorf("collections: certified monotonic reconstruction primary mismatch for id %q", row.ID)
		}
		records = append(records, DocumentRecord{ID: bytes.Clone(primary.UnsafeKey()), Document: primary.ValueCopy(nil)})
		var visible columnPhysicalVisibleRow
		assignColumnPhysicalVisibleRow(&visible, row)
		visibleRows = append(visibleRows, visible)
		primary.Next()
		if err := primary.Error(); err != nil {
			return err
		}
		scanned++
		if len(records) == columnReconstructionMonotonicBatchSize {
			if err := flush(); err != nil {
				callbackErr = err
				return errColumnReconstructionMonotonicStop
			}
			if stopped {
				return errColumnReconstructionMonotonicStop
			}
		}
		return nil
	}})
	if stats != nil {
		stats.PhysicalPasses++
		// The generic scanner does not return a partial summary when the visitor
		// stops mid-asset, so use the exact visitor count for this pass.
		stats.PhysicalRows += uint64(visitedPhysicalRows)
		stats.PhysicalBytes += uint64(diag.PhysicalBytesScanned)
		stats.PhysicalDecodedBlocks += uint64(diag.DecodedBlocks)
	}
	if callbackErr != nil {
		return false, callbackErr
	}
	if scanErr != nil && !errors.Is(scanErr, errColumnReconstructionMonotonicStop) {
		return false, scanErr
	}
	if stopped {
		return false, nil
	}
	if err := flush(); err != nil {
		return false, err
	}
	if stopped {
		return false, nil
	}
	if err := primary.Error(); err != nil {
		return false, err
	}
	if scanned < maxDocuments {
		return false, nil
	}
	// The preflight has already proved exact one-to-one equality, so a remaining
	// primary record is the only possible truncation signal at this snapshot.
	return primary.Valid(), nil
}

// typedColumnPartValuesForMonotonicWindow decodes only row indexes referenced
// by this bounded reconstruction window. The caller's certified ordering makes
// generations monotonic; output positions preserve primary callback order.
func (c *Collection) typedColumnPartValuesForMonotonicWindow(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, rows []columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache) ([]typedColumnPartVisibleValues, error) {
	out := make([]typedColumnPartVisibleValues, len(rows))
	if !columnStoreHasTypedColumnPartOwners(cfg) {
		return out, nil
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if len(fields) == 0 {
		return out, nil
	}
	if cache == nil {
		cache = &typedColumnPartReconstructionCache{}
	}
	cache.WindowDecodedBytes = 0
	cache.WindowGenerations = 0
	cache.WindowSourcePartBytes = 0
	cache.Fields = fields
	if cache.ReadCache == nil {
		rc, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
		if err != nil {
			return nil, err
		}
		cache.ReadCache = &rc
		defer func() { _ = cache.ReadCache.close() }()
	}
	for start := 0; start < len(rows); {
		generation := rows[start].Generation
		end := start + 1
		for end < len(rows) && rows[end].Generation == generation {
			end++
		}
		indexes := make([]int, end-start)
		for i := start; i < end; i++ {
			indexes[i-start] = rows[i].RowIndex
		}
		part := cache.SelectivePart
		if part == nil || cache.SelectivePartGeneration != generation {
			ref, found, err := c.typedColumnPartRefForGenerationWithCache(snap, rootID, cfg, generation, cache)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("collections: typed-column reconstruction missing typed_column_part asset for generation=%d", generation)
			}
			raw, err := cache.ReadCache.read(ref.Ref, nil)
			if err != nil {
				return nil, err
			}
			cache.WindowSourcePartBytes = max(cache.WindowSourcePartBytes, uint64(len(raw)))
			cache.PartLoads++
			part, err = typedColumnAdapterPartFromBytesForReconstruction(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(cfg.SchemaHash)}, raw)
			if err != nil {
				return nil, err
			}
			cache.SelectivePart = part
			cache.SelectivePartGeneration = generation
		}
		decoded, _, err := part.scanDecodedValuesSelectedRows(nil, indexes)
		if err != nil {
			return nil, err
		}
		cache.TypedPartDecodes++
		// Only this adapter part remains resident; replacing it releases the
		// previous generation before its source scratch is reused.
		cache.WindowGenerations = 1
		cache.WindowDecodedBytes += typedColumnPartDecodedValuesResidentBytes(decoded)
		for i := start; i < end; i++ {
			values := make([]columnDeclaredValue, len(fields))
			for field := range fields {
				if field >= len(decoded.Values) || i-start >= len(decoded.Values[field]) {
					return nil, fmt.Errorf("collections: typed-column selected row decode field=%d row=%d unavailable", field, i-start)
				}
				values[field] = decoded.Values[field][i-start]
			}
			out[i] = typedColumnPartVisibleValues{Values: values}
		}
		start = end
	}
	return out, nil
}
