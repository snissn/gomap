package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type TypedColumnInt64PredicateScanKind string

const (
	TypedColumnInt64PredicateEqual TypedColumnInt64PredicateScanKind = "equal"
	TypedColumnInt64PredicateRange TypedColumnInt64PredicateScanKind = "range"
)

type TypedColumnInt64PredicateScanRequest struct {
	Column                   string
	Kind                     TypedColumnInt64PredicateScanKind
	Value                    int64
	Low                      int64
	High                     int64
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

type TypedColumnInt64PredicateScanRow struct {
	Generation uint64
	PartID     uint64
	RowIndex   int
	PrimaryID  int64
	DocumentID []byte
	Value      int64
}

type TypedColumnInt64PredicateScanDiagnostics struct {
	ManifestRoot               uint64
	ManifestGeneration         uint64
	RecoveryManifestGeneration uint64
	AppliedCommandLSN          uint64
	ManifestRecords            int
	AssetRefs                  int
	MutationParts              int

	RowsScanned int
	RowsMatched int

	PartsConsidered  int
	PartsPruned      int
	PartsDecoded     int
	BlocksConsidered int
	BlocksPruned     int
	BlocksDecoded    int

	DirectTypedColumnAssetReads int
	FallbackReads               int
	MappedBytes                 uint64
	HeapCopyBytes               uint64
	DecodedMetadataBytes        uint64
	DecodedHeapCopyBytes        uint64
	PhysicalBytesScanned        int64
	RowMaterializations         int
	SegmentFileCacheHits        uint64
	SegmentFileCacheMisses      uint64
	ColumnAssetReadIntegrity    string
	Fallback                    bool
	FallbackReason              string
	ScanNanos                   int64
}

type TypedColumnInt64PredicateScanResult struct {
	Rows        []TypedColumnInt64PredicateScanRow
	Diagnostics TypedColumnInt64PredicateScanDiagnostics
}

// RunTypedColumnInt64PredicateScan executes the scoped #1757 scalar predicate MVP.
// When the requested int64 field is owned by typed_column_part, the predicate is
// evaluated directly over durable typed_column_part assets and fails closed if
// those assets are unavailable or inconsistent. Non typed-column ownership keeps
// the existing typed-row/document fallback behavior and is identified in
// Diagnostics.Fallback.
func (c *Collection) RunTypedColumnInt64PredicateScan(req TypedColumnInt64PredicateScanRequest) (TypedColumnInt64PredicateScanResult, error) {
	if err := validateTypedColumnInt64PredicateScanRequest(req); err != nil {
		return TypedColumnInt64PredicateScanResult{}, err
	}
	start := time.Now()
	hintCfg, hintColumn, hintDeclared, hintErr := c.typedColumnInt64PredicateCatalogColumn(req.Column)
	if hintErr != nil {
		return TypedColumnInt64PredicateScanResult{}, hintErr
	}
	hintTypedColumnOwner := hintDeclared && hintColumn.ValueType == ColumnStoreValueInt64 && !hintColumn.Nullable && columnStoreColumnIsTypedColumnPart(hintColumn)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		if hintTypedColumnOwner {
			return TypedColumnInt64PredicateScanResult{}, err
		}
		return c.runTypedColumnInt64PredicateScanDocumentFallback(req, hintCfg, "column_store_unavailable", start)
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column)
	if !ok {
		return c.runTypedColumnInt64PredicateScanDocumentFallback(req, cfg, "undeclared_column", start)
	}
	if col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return TypedColumnInt64PredicateScanResult{}, fmt.Errorf("%w: typed-column int64 predicate column %q has type=%q nullable=%v", ErrColumnQueryPlanUnsupported, req.Column, col.ValueType, col.Nullable)
	}
	if !columnStoreColumnIsTypedColumnPart(col) {
		if view.MutationParts != 0 {
			return c.runTypedColumnInt64PredicateScanDocumentFallback(req, cfg, "mutation_visibility_requires_document_reconstruction", start)
		}
		return c.runTypedColumnInt64PredicateScanPhysicalFallback(view, req, cfg, "typed_column_not_selected", start)
	}
	if view.MutationParts != 0 {
		return c.runTypedColumnInt64PredicateScanDocumentFallback(req, cfg, "mutation_visibility_requires_document_reconstruction", start)
	}
	return c.runTypedColumnInt64PredicateScanDirect(view, req, cfg, start)
}

func validateTypedColumnInt64PredicateScanRequest(req TypedColumnInt64PredicateScanRequest) error {
	if req.Column == "" {
		return errors.New("collections: typed-column int64 predicate scan requires column")
	}
	switch req.Kind {
	case TypedColumnInt64PredicateEqual:
	case TypedColumnInt64PredicateRange:
		if req.Low > req.High {
			return errors.New("collections: typed-column int64 predicate range low is greater than high")
		}
	default:
		return fmt.Errorf("%w: unsupported typed-column int64 predicate kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
	return nil
}

func (c *Collection) runTypedColumnInt64PredicateScanDirect(view columnPhysicalScanSnapshotView, req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	diag := typedColumnInt64PredicateDiagnosticsFromView(view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	fields := columnStoreTypedColumnPartFields(cfg)
	if ok, err := typedColumnAdapterHasInt64PredicateColumn(fields, req.Column); err != nil {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	} else if !ok {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column int64 predicate column %q is not owned by typed_column_part", req.Column)
	}
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan, len(view.TypedColumnPartRefs))
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Reason != ColumnPublishOperationInsert {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column int64 predicate scan requires insert-only typed refs, got %s", ref.Reason)
		}
		// Current durable typed-column publication emits one typed_column_part
		// locator per generation (part_id=2) paired with that generation's
		// physical row locator part (part_id=1). Duplicate generations are a
		// manifest invariant violation and fail closed here.
		if _, exists := refsByGeneration[ref.Ref.Generation]; exists {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: duplicate typed_column_part ref for generation=%d", ref.Ref.Generation)
		}
		refsByGeneration[ref.Ref.Generation] = ref
	}
	if len(refsByGeneration) == 0 {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, errors.New("collections: missing typed_column_part assets for typed-column int64 predicate scan")
	}
	for _, physical := range view.AssetRefs {
		if physical.Reason != ColumnPublishOperationInsert {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column int64 predicate scan requires insert-only physical refs, got %s", physical.Reason)
		}
		if _, ok := refsByGeneration[physical.Ref.Generation]; !ok {
			return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: missing typed_column_part asset for generation=%d", physical.Ref.Generation)
		}
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	}
	readCache.returnViews = true
	if err := readCache.useMappedResourceManager(mgr, mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-int64-predicate-scan", Namespace: view.AssetNamespace, Generation: view.Diagnostics.ManifestGeneration, Reason: "typed-column int64 predicate scan"}, "typed-column int64 predicate scan"); err != nil {
		_ = readCache.close()
		return TypedColumnInt64PredicateScanResult{Diagnostics: diag}, err
	}
	defer func() { _ = readCache.close() }()

	result := TypedColumnInt64PredicateScanResult{Diagnostics: diag}
	var rawScratch []byte
	for _, physical := range view.AssetRefs {
		typedRef := refsByGeneration[physical.Ref.Generation]
		result.Diagnostics.PartsConsidered++
		raw, err := readCache.read(typedRef.Ref, rawScratch)
		result.Diagnostics.SegmentFileCacheHits = readCache.hits
		result.Diagnostics.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		rawScratch = raw
		result.Diagnostics.DirectTypedColumnAssetReads++
		result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
		adapterPart, adapterColumn, manifestBytes, err := typedColumnAdapterPrepareInt64PredicateScanPart(fields, raw, typedRef.Ref.PartID, typedRef.Rows, physical.Rows, cfg.SchemaHash, req.Column)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		result.Diagnostics.DecodedMetadataBytes += uint64(manifestBytes)
		matchedStart := len(result.Rows)
		partPruned, err := scanTypedColumnInt64PredicatePart(adapterPart.Part, adapterColumn.Definition.Name, req, typedRef.Ref.Generation, typedRef.Ref.PartID, &result)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column int64 predicate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partPruned {
			result.Diagnostics.PartsPruned++
		} else {
			result.Diagnostics.PartsDecoded++
		}
		if len(result.Rows) > matchedStart {
			physicalRaw, err := readCache.read(physical.Ref, rawScratch)
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			if err != nil {
				return result, fmt.Errorf("collections: typed-column int64 predicate physical id read generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			rawScratch = physicalRaw
			result.Diagnostics.PhysicalBytesScanned += int64(len(physicalRaw))
			ids, err := typedColumnInt64PredicatePhysicalRowIDs(physicalRaw, physical.Ref, view.CollectionName, view.Config, result.Rows[matchedStart:])
			if err != nil {
				return result, fmt.Errorf("collections: typed-column int64 predicate physical id decode generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			for rowIdx := matchedStart; rowIdx < len(result.Rows); rowIdx++ {
				matched := &result.Rows[rowIdx]
				documentID, ok := ids[matched.RowIndex]
				if !ok {
					return result, fmt.Errorf("collections: typed-column int64 predicate missing physical document id for row_index=%d", matched.RowIndex)
				}
				matched.Generation = physical.Ref.Generation
				matched.PartID = physical.Ref.PartID
				matched.DocumentID = documentID
			}
		}
	}
	stats := mgr.Stats()
	result.Diagnostics.MappedBytes = stats.TotalMappedBytes
	result.Diagnostics.HeapCopyBytes = stats.TotalHeapCopyBytes
	result.Diagnostics.FallbackReads = int(stats.FallbackReads)
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, nil
}

func (c *Collection) typedColumnInt64PredicateCatalogColumn(column string) (ColumnStoreConfig, ColumnStoreColumn, bool, error) {
	if c == nil {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, errCollectionNil
	}
	if c.db == nil {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, errCollectionDBNil
	}
	c.catalogMu.RLock()
	defer c.catalogMu.RUnlock()
	catalog := c.catalog
	if catalog == nil || catalog.meta.Options.ColumnStore == nil || !catalog.meta.Options.ColumnStore.Enabled {
		return ColumnStoreConfig{}, ColumnStoreColumn{}, false, nil
	}
	cfg := catalog.meta.Options.ColumnStore.copy()
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, column)
	return cfg, col, ok, nil
}

func typedColumnInt64PredicatePhysicalRowIDs(raw []byte, ref ColumnAssetRef, collection string, cfg ColumnStoreConfig, matchedRows []TypedColumnInt64PredicateScanRow) (map[int][]byte, error) {
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(cfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	wanted := make(map[int]struct{}, len(matchedRows))
	for _, row := range matchedRows {
		if row.RowIndex < 0 {
			return nil, fmt.Errorf("matched row_index=%d is negative", row.RowIndex)
		}
		wanted[row.RowIndex] = struct{}{}
	}
	ids := make(map[int][]byte, len(wanted))
	_, err := scanColumnPhysicalAssetRowsWithManifestOperation(raw, ref, collection, &cfg, projection, ColumnPublishOperationInsert, func(row columnPhysicalScanRowView) error {
		if row.Deleted {
			return fmt.Errorf("physical row[%d] is deleted", row.RowIndex)
		}
		if _, ok := wanted[row.RowIndex]; ok {
			ids[row.RowIndex] = bytes.Clone(row.ID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for rowIndex := range wanted {
		if ids[rowIndex] == nil {
			return nil, fmt.Errorf("missing physical document id for row_index=%d", rowIndex)
		}
	}
	return ids, nil
}

func typedColumnInt64PredicateMayMatch(req TypedColumnInt64PredicateScanRequest, minValue, maxValue int64) bool {
	switch req.Kind {
	case TypedColumnInt64PredicateEqual:
		return req.Value >= minValue && req.Value <= maxValue
	case TypedColumnInt64PredicateRange:
		return req.High >= minValue && req.Low <= maxValue
	default:
		return false
	}
}

func typedColumnInt64PredicateMatches(req TypedColumnInt64PredicateScanRequest, value int64) bool {
	switch req.Kind {
	case TypedColumnInt64PredicateEqual:
		return value == req.Value
	case TypedColumnInt64PredicateRange:
		return value >= req.Low && value <= req.High
	default:
		return false
	}
}

func (c *Collection) runTypedColumnInt64PredicateScanPhysicalFallback(view columnPhysicalScanSnapshotView, req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, reason string, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	result := TypedColumnInt64PredicateScanResult{Diagnostics: typedColumnInt64PredicateDiagnosticsFromView(view)}
	result.Diagnostics.Fallback = true
	result.Diagnostics.FallbackReason = reason
	scanDiag, err := c.scanColumnPhysicalRowsInSnapshotView(view, columnPhysicalScanRequest{
		ProjectedColumns:  []string{req.Column},
		RequireInsertOnly: true,
		ReadIntegrity:     req.ColumnAssetReadIntegrity,
		Visitor: func(row columnPhysicalScanRowView) error {
			if row.Deleted || len(row.Values) == 0 {
				return nil
			}
			value, err := columnPhysicalQueryInt64Value(row.Values[0])
			if err != nil {
				return err
			}
			result.Diagnostics.RowsScanned++
			if typedColumnInt64PredicateMatches(req, value) {
				result.Rows = append(result.Rows, TypedColumnInt64PredicateScanRow{Generation: row.Generation, PartID: row.PartID, RowIndex: row.RowIndex, DocumentID: bytes.Clone(row.ID), Value: value})
				result.Diagnostics.RowsMatched++
			}
			return nil
		},
	})
	result.Diagnostics.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	result.Diagnostics.FallbackReads = scanDiag.DecodedBlocks
	result.Diagnostics.PhysicalBytesScanned = scanDiag.PhysicalBytesScanned
	result.Diagnostics.SegmentFileCacheHits = scanDiag.SegmentFileCacheHits
	result.Diagnostics.SegmentFileCacheMisses = scanDiag.SegmentFileCacheMisses
	if err != nil {
		return result, err
	}
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	_ = cfg
	return result, nil
}

func (c *Collection) runTypedColumnInt64PredicateScanDocumentFallback(req TypedColumnInt64PredicateScanRequest, cfg ColumnStoreConfig, reason string, start time.Time) (TypedColumnInt64PredicateScanResult, error) {
	result := TypedColumnInt64PredicateScanResult{}
	result.Diagnostics.Fallback = true
	result.Diagnostics.FallbackReason = reason
	result.Diagnostics.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	path := req.Column
	if col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column); ok {
		path = col.Path
	}
	fallbackCfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{{Name: req.Column, Path: path, ValueType: ColumnStoreValueInt64}}}
	_, err := c.ScanDocumentsFunc(maxCollectionInt, func(record DocumentRecord) (bool, error) {
		result.Diagnostics.RowMaterializations++
		result.Diagnostics.FallbackReads++
		rows, err := extractColumnDeclaredRowsFromJSONDocuments(fallbackCfg, []columnWriteDocument{{ID: record.ID, Document: record.Document}})
		if err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0].Values) != 1 {
			return false, errors.New("collections: document fallback failed to extract int64 predicate column")
		}
		value, err := columnPhysicalQueryInt64Value(rows[0].Values[0])
		if err != nil {
			return false, err
		}
		result.Diagnostics.RowsScanned++
		if typedColumnInt64PredicateMatches(req, value) {
			result.Rows = append(result.Rows, TypedColumnInt64PredicateScanRow{DocumentID: bytes.Clone(record.ID), Value: value})
			result.Diagnostics.RowsMatched++
		}
		return true, nil
	})
	result.Diagnostics.ScanNanos = time.Since(start).Nanoseconds()
	return result, err
}

func typedColumnInt64PredicateDiagnosticsFromView(view columnPhysicalScanSnapshotView) TypedColumnInt64PredicateScanDiagnostics {
	return TypedColumnInt64PredicateScanDiagnostics{
		ManifestRoot:               view.Diagnostics.ManifestRoot,
		ManifestGeneration:         view.Diagnostics.ManifestGeneration,
		RecoveryManifestGeneration: view.Diagnostics.RecoveryManifestGeneration,
		AppliedCommandLSN:          view.Diagnostics.AppliedCommandLSN,
		ManifestRecords:            view.Diagnostics.ManifestRecords,
		AssetRefs:                  view.Diagnostics.AssetRefs,
		MutationParts:              view.Diagnostics.MutationParts,
	}
}
