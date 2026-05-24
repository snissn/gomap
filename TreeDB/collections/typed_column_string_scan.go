package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type TypedColumnStringPredicateScanRequest struct {
	Column                   string
	Value                    string
	ColumnAssetReadIntegrity ColumnAssetReadIntegrity
}

type TypedColumnStringPredicateScanRow struct {
	Generation uint64
	PartID     uint64
	RowIndex   int
	PrimaryID  int64
	DocumentID []byte
	Value      string
}

type TypedColumnStringPredicateScanDiagnostics = TypedColumnInt64PredicateScanDiagnostics

type TypedColumnStringPredicateScanResult struct {
	Rows        []TypedColumnStringPredicateScanRow
	Diagnostics TypedColumnStringPredicateScanDiagnostics
}

// RunTypedColumnStringPredicateScan executes the scoped #1785 string equality path.
// It only evaluates non-null string fields owned by typed_column_part. The
// predicate is evaluated directly over durable typed_column_part dictionary-code
// assets and fails closed for unsupported columns instead of reconstructing
// documents or using row-asset dictionary sidecars.
func (c *Collection) RunTypedColumnStringPredicateScan(req TypedColumnStringPredicateScanRequest) (TypedColumnStringPredicateScanResult, error) {
	if err := validateTypedColumnStringPredicateScanRequest(req); err != nil {
		return TypedColumnStringPredicateScanResult{}, err
	}
	start := time.Now()
	hintCfg, hintColumn, hintDeclared, hintErr := c.typedColumnStringPredicateCatalogColumn(req.Column)
	if hintErr != nil {
		return TypedColumnStringPredicateScanResult{}, hintErr
	}
	if hintDeclared && columnStoreColumnIsTypedColumnPart(hintColumn) && (hintColumn.ValueType != ColumnStoreValueString || hintColumn.Nullable) {
		return TypedColumnStringPredicateScanResult{}, typedColumnStringPredicateUnsupportedColumnError(req.Column, hintColumn)
	}
	hintTypedColumnOwner := hintDeclared && hintColumn.ValueType == ColumnStoreValueString && columnStoreColumnIsTypedColumnPart(hintColumn)
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithSidecars(columnManifestScanNoSidecars())
	if closeView != nil {
		defer closeView()
	}
	if err != nil {
		if hintTypedColumnOwner {
			return TypedColumnStringPredicateScanResult{}, err
		}
		_ = hintCfg
		return TypedColumnStringPredicateScanResult{}, fmt.Errorf("%w: typed-column string predicate scan requires enabled typed_column_part column store", ErrColumnQueryPlanUnsupported)
	}
	cfg := view.FullConfig
	if !cfg.Enabled {
		cfg = view.Config
	}
	col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, req.Column)
	if !ok {
		return TypedColumnStringPredicateScanResult{}, fmt.Errorf("%w: typed-column string predicate scan requested undeclared column %q", ErrColumnQueryPlanUnsupported, req.Column)
	}
	if col.ValueType != ColumnStoreValueString || col.Nullable {
		return TypedColumnStringPredicateScanResult{}, typedColumnStringPredicateUnsupportedColumnError(req.Column, col)
	}
	if !columnStoreColumnIsTypedColumnPart(col) {
		return TypedColumnStringPredicateScanResult{}, fmt.Errorf("%w: typed-column string predicate scan column %q is not owned by typed_column_part", ErrColumnQueryPlanUnsupported, req.Column)
	}
	return c.runTypedColumnStringPredicateScanDirect(view, req, cfg, start)
}

func validateTypedColumnStringPredicateScanRequest(req TypedColumnStringPredicateScanRequest) error {
	if req.Column == "" {
		return errors.New("collections: typed-column string predicate scan requires column")
	}
	return nil
}

func typedColumnStringPredicateUnsupportedColumnError(column string, col ColumnStoreColumn) error {
	return fmt.Errorf("%w: typed-column string predicate column %q has type=%q nullable=%v", ErrColumnQueryPlanUnsupported, column, col.ValueType, col.Nullable)
}

func (c *Collection) typedColumnStringPredicateCatalogColumn(column string) (ColumnStoreConfig, ColumnStoreColumn, bool, error) {
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

func typedColumnPhysicalAssetPairingStringScanError(err error) error {
	var reasonErr typedColumnPhysicalAssetPairingReasonError
	if errors.As(err, &reasonErr) {
		return fmt.Errorf("collections: typed-column string predicate scan requires insert-only physical refs, got %s", reasonErr.reason)
	}
	return err
}

func (c *Collection) runTypedColumnStringPredicateScanDirect(view columnPhysicalScanSnapshotView, req TypedColumnStringPredicateScanRequest, cfg ColumnStoreConfig, start time.Time) (TypedColumnStringPredicateScanResult, error) {
	diag := typedColumnStringPredicateDiagnosticsFromView(view)
	diag.ColumnAssetReadIntegrity = columnAssetReadIntegrityLabel(req.ColumnAssetReadIntegrity)
	fields := columnStoreTypedColumnPartFields(cfg)
	if ok, err := typedColumnAdapterHasStringPredicateColumn(fields, req.Column); err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	} else if !ok {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, fmt.Errorf("%w: typed-column string predicate scan column %q is not owned by typed_column_part", ErrColumnQueryPlanUnsupported, req.Column)
	}
	refsByGeneration := make(map[uint64]columnManifestAssetRefForScan, len(view.TypedColumnPartRefs))
	for _, ref := range view.TypedColumnPartRefs {
		if ref.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			continue
		}
		if ref.Ref.PartID != typedColumnPartAssetPartID {
			continue
		}
		if ref.Role == ColumnManifestPartRoleTombstone || ref.Reason == ColumnPublishOperationDelete {
			return TypedColumnStringPredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: typed-column string predicate scan got tombstone typed ref generation=%d", ref.Ref.Generation)
		}
		if _, exists := refsByGeneration[ref.Ref.Generation]; exists {
			return TypedColumnStringPredicateScanResult{Diagnostics: diag}, fmt.Errorf("collections: duplicate typed_column_part ref for generation=%d", ref.Ref.Generation)
		}
		refsByGeneration[ref.Ref.Generation] = ref
	}
	if len(refsByGeneration) == 0 {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, errors.New("collections: missing typed_column_part assets for typed-column string predicate scan")
	}
	if view.MutationParts == 0 {
		if _, err := validateTypedColumnPhysicalAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
			return TypedColumnStringPredicateScanResult{Diagnostics: diag}, typedColumnPhysicalAssetPairingStringScanError(err)
		}
	} else if err := validateTypedColumnMultipartAssetPairing(refsByGeneration, view.AssetRefs); err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(view.ColumnAssetRootDir, view.AssetNamespace, req.ColumnAssetReadIntegrity)
	if err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}
	readCache.returnViews = true
	if err := readCache.useMappedResourceManager(mgr, mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "typed-column-string-predicate-scan", Namespace: view.AssetNamespace, Generation: view.Diagnostics.ManifestGeneration, Reason: "typed-column string predicate scan"}, "typed-column string predicate scan"); err != nil {
		_ = readCache.close()
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}
	defer func() { _ = readCache.close() }()

	result := TypedColumnStringPredicateScanResult{Diagnostics: diag}
	var scanScratch typedColumnStringPredicateScanScratch
	var resolver *typedColumnLatestRowResolver
	if view.MutationParts != 0 {
		resolver, err = buildTypedColumnLatestRowResolver(view, &readCache, &result.Diagnostics)
		if err != nil {
			return result, err
		}
	}
	var rawScratch []byte
	for _, physical := range view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef := refsByGeneration[physical.Ref.Generation]
		result.Diagnostics.PartsConsidered++
		raw, err := readCache.read(typedRef.Ref, rawScratch)
		result.Diagnostics.SegmentFileCacheHits = readCache.hits
		result.Diagnostics.SegmentFileCacheMisses = readCache.misses
		if err != nil {
			return result, fmt.Errorf("collections: typed-column string predicate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		rawScratch = raw
		result.Diagnostics.DirectTypedColumnAssetReads++
		result.Diagnostics.PhysicalBytesScanned += int64(len(raw))
		prepared, err := typedColumnAdapterPrepareStringPredicateScanPart(fields, raw, typedRef.Ref.PartID, typedRef.Rows, physical.Rows, cfg.SchemaHash, req.Column, req.Value)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column string predicate decode generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		result.Diagnostics.DecodedMetadataBytes += uint64(prepared.ManifestBytes)
		result.Diagnostics.DictionaryBytesDecoded += uint64(prepared.DictionaryBytes)
		if !prepared.QueryCodeFound {
			if valueCol, ok := prepared.AdapterPart.Part.Columns[prepared.Column.Definition.Name]; ok {
				result.Diagnostics.BlocksConsidered += len(valueCol.Blocks)
				result.Diagnostics.BlocksPruned += len(valueCol.Blocks)
			}
			result.Diagnostics.PartsPruned++
			continue
		}
		matchedStart := len(result.Rows)
		var visibility *typedColumnLatestPhysicalPart
		if resolver != nil {
			var ok bool
			visibility, ok = resolver.partForGeneration(physical.Ref.Generation)
			if !ok {
				return result, fmt.Errorf("collections: typed-column string predicate missing latest-visible physical generation=%d", physical.Ref.Generation)
			}
		}
		partPruned, err := scanTypedColumnStringPredicatePartWithVisibility(prepared.AdapterPart.Part, prepared.Column.Definition.Name, prepared.QueryCode, req.Value, typedRef.Ref.Generation, typedRef.Ref.PartID, &result, visibility, &scanScratch)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column string predicate scan generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partPruned {
			result.Diagnostics.PartsPruned++
		} else {
			result.Diagnostics.PartsDecoded++
		}
		if resolver == nil && len(result.Rows) > matchedStart {
			result.Diagnostics.PhysicalRowAssetReads++
			physicalRaw, err := readCache.read(physical.Ref, rawScratch)
			result.Diagnostics.SegmentFileCacheHits = readCache.hits
			result.Diagnostics.SegmentFileCacheMisses = readCache.misses
			if err != nil {
				return result, fmt.Errorf("collections: typed-column string predicate physical id read generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			rawScratch = physicalRaw
			result.Diagnostics.PhysicalBytesScanned += int64(len(physicalRaw))
			result.Diagnostics.PhysicalRowIDLookups++
			ids, err := typedColumnStringPredicatePhysicalRowIDs(physicalRaw, physical.Ref, view.CollectionName, view.Config, result.Rows[matchedStart:])
			if err != nil {
				return result, fmt.Errorf("collections: typed-column string predicate physical id decode generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			for rowIdx := matchedStart; rowIdx < len(result.Rows); rowIdx++ {
				matched := &result.Rows[rowIdx]
				documentID, ok := ids[matched.RowIndex]
				if !ok {
					return result, fmt.Errorf("collections: typed-column string predicate missing physical document id for row_index=%d", matched.RowIndex)
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

func typedColumnStringPredicatePhysicalRowIDs(raw []byte, ref ColumnAssetRef, collection string, cfg ColumnStoreConfig, matchedRows []TypedColumnStringPredicateScanRow) (map[int][]byte, error) {
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

func typedColumnStringPredicateDiagnosticsFromView(view columnPhysicalScanSnapshotView) TypedColumnStringPredicateScanDiagnostics {
	return TypedColumnStringPredicateScanDiagnostics{
		ManifestRoot:               view.Diagnostics.ManifestRoot,
		ManifestGeneration:         view.Diagnostics.ManifestGeneration,
		RecoveryManifestGeneration: view.Diagnostics.RecoveryManifestGeneration,
		AppliedCommandLSN:          view.Diagnostics.AppliedCommandLSN,
		ManifestRecords:            view.Diagnostics.ManifestRecords,
		AssetRefs:                  view.Diagnostics.AssetRefs,
		MutationParts:              view.Diagnostics.MutationParts,
	}
}
