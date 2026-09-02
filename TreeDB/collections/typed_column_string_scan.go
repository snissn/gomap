package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type TypedColumnStringPredicateScanKind string

const (
	TypedColumnStringPredicateEqual        TypedColumnStringPredicateScanKind = "equal"
	TypedColumnStringPredicateInList       TypedColumnStringPredicateScanKind = "in_list"
	TypedColumnStringPredicateCategory     TypedColumnStringPredicateScanKind = "category"
	TypedColumnStringPredicatePrefix       TypedColumnStringPredicateScanKind = "prefix"
	TypedColumnStringPredicateLexicalRange TypedColumnStringPredicateScanKind = "lexical_range"
)

type TypedColumnStringPredicateScanRequest struct {
	Column                   string
	Kind                     TypedColumnStringPredicateScanKind
	Value                    string
	Values                   []string
	Prefix                   string
	Low                      string
	High                     string
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
	switch req.Kind {
	case "", TypedColumnStringPredicateEqual, TypedColumnStringPredicatePrefix, TypedColumnStringPredicateLexicalRange:
		return nil
	case TypedColumnStringPredicateInList, TypedColumnStringPredicateCategory:
		if req.Value != "" {
			return fmt.Errorf("%w: typed-column string predicate scan %s uses Values for targets; Value is ambiguous for empty-string members", ErrColumnQueryPlanUnsupported, req.Kind)
		}
		return nil
	default:
		return fmt.Errorf("%w: typed-column string predicate scan unsupported kind %q", ErrColumnQueryPlanUnsupported, req.Kind)
	}
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
	adapterColumn, ok, err := typedColumnStringPredicateAdapterColumn(fields, req.Column)
	if err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}
	if !ok {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, fmt.Errorf("%w: typed-column string predicate scan column %q is not owned by typed_column_part", ErrColumnQueryPlanUnsupported, req.Column)
	}
	op := typedColumnStringPredicateSemanticOperation(req)
	if op == columnsemantics.OpStringPrefix || op == columnsemantics.OpStringLexicalRange {
		cap, _ := typedColumnAdapterCapability(adapterColumn, op)
		diag.Fallback = true
		diag.FallbackReason = cap.Error()
		if diag.FallbackReason == "" {
			diag.FallbackReason = string(columnsemantics.ReasonDictionaryOrderUnproven)
		}
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, fmt.Errorf("%w: typed-column string predicate scan column %q requires lexical string fallback: %s", ErrColumnQueryPlanUnsupported, req.Column, diag.FallbackReason)
	}
	if err := requireTypedColumnAdapterCapability(adapterColumn, op, fmt.Sprintf("typed-column string predicate scan column %q", req.Column)); err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}
	if err := requireTypedColumnLayoutCapability(adapterColumn, op, fmt.Sprintf("typed-column string predicate scan column %q", req.Column)); err != nil {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, err
	}
	queryValues := typedColumnStringPredicateValues(req)
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
	} else if typedColumnRefsHaveSortKey(refsByGeneration) {
		return TypedColumnStringPredicateScanResult{Diagnostics: diag}, typedColumnSortedMutationVisibilityUnsupported("typed-column string predicate scan")
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
	var typedRawScratch []byte
	useTargetedRanges := typedColumnStringUseTargetedRanges(req.ColumnAssetReadIntegrity)
	validatedRefs := make(map[ColumnAssetRef]struct{}, len(view.AssetRefs))
	for _, physical := range view.AssetRefs {
		if physical.Role == ColumnManifestPartRoleTombstone || physical.Reason == ColumnPublishOperationDelete {
			continue
		}
		typedRef := refsByGeneration[physical.Ref.Generation]
		result.Diagnostics.PartsConsidered++
		result.Diagnostics.DirectTypedColumnAssetReads++
		var readRange typedColumnPreparedRangeReader
		if useTargetedRanges {
			if err := typedColumnStringEnsureFullAssetValidated(&readCache, typedRef.Ref, req.ColumnAssetReadIntegrity, validatedRefs, &result.Diagnostics); err != nil {
				return result, fmt.Errorf("collections: typed-column string predicate validate generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
			}
			readRange = func(offset int, length int, section bool) ([]byte, error) {
				return typedColumnStringReadRange(&readCache, typedRef.Ref, offset, length, section, &result.Diagnostics)
			}
		} else {
			var err error
			typedRawScratch, err = typedColumnStringReadFullAsset(&readCache, typedRef.Ref, typedRawScratch, &result.Diagnostics)
			if err != nil {
				return result, fmt.Errorf("collections: typed-column string predicate read generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
			}
			fullRaw := typedRawScratch
			readRange = func(offset int, length int, section bool) ([]byte, error) {
				return typedColumnStringReadFullAssetRange(fullRaw, offset, length, section, &result.Diagnostics)
			}
		}
		requests := typedColumnStringPreparedColumnRequests(adapterColumn, op)
		preparedPart, partDiag, err := typedColumnPreparePartStateFromRanges(typedRef.Ref, physical.Ref, typedRef.Rows, physical.Rows, fields, cfg.SchemaHash, requests, readRange, nil)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column string predicate prepare generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		if partDiag.Fallback {
			result.Diagnostics.Fallback = true
			result.Diagnostics.FallbackReason = partDiag.FallbackReason
			return result, fmt.Errorf("%w: %s", ErrColumnQueryPlanUnsupported, partDiag.FallbackReason)
		}
		typedColumnStringAddPreparedDiagnostics(&result.Diagnostics, partDiag)
		preparedColumn := preparedPart.Columns[adapterColumn.Definition.Name]
		codes, valueByCode, found, err := typedColumnStringResolvePreparedCodes(preparedColumn, queryValues)
		if err != nil {
			return result, fmt.Errorf("collections: typed-column string predicate dictionary resolve generation=%d part_id=%d: %w", typedRef.Ref.Generation, typedRef.Ref.PartID, err)
		}
		result.Diagnostics.DictionaryBytesDecoded += uint64(typedColumnPreparedPartDictionaryBytes(preparedPart))
		if !found {
			if preparedColumn != nil {
				result.Diagnostics.BlocksConsidered += len(preparedColumn.BlockPlans)
				result.Diagnostics.BlocksPruned += len(preparedColumn.BlockPlans)
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
		if visibility != nil && typedColumnPreparedPartHasLogicalSortKey(preparedPart) {
			return result, typedColumnSortedMutationVisibilityUnsupported("typed-column string predicate scan")
		}
		partPruned, err := scanTypedColumnStringPreparedPartWithVisibility(preparedPart, adapterColumn.Definition.Name, codes, valueByCode, typedRef.Ref.Generation, typedRef.Ref.PartID, &result, visibility, readRange, &scanScratch)
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
			ids, err := typedColumnStringPredicatePhysicalRowIDs(physicalRaw, physical.Ref, view.CollectionName, view.Config, physical.Rows, result.Rows[matchedStart:])
			if err != nil {
				return result, fmt.Errorf("collections: typed-column string predicate physical id decode generation=%d part_id=%d: %w", physical.Ref.Generation, physical.Ref.PartID, err)
			}
			for rowIdx := matchedStart; rowIdx < len(result.Rows); rowIdx++ {
				matched := &result.Rows[rowIdx]
				physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(matched.PrimaryID, physical.Rows)
				if err != nil {
					return result, err
				}
				documentID, ok := ids[physicalRowIndex]
				if !ok {
					return result, fmt.Errorf("collections: typed-column string predicate missing physical document id for row_index=%d", physicalRowIndex)
				}
				matched.Generation = physical.Ref.Generation
				matched.PartID = physical.Ref.PartID
				matched.RowIndex = physicalRowIndex
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

func typedColumnStringPredicateSemanticOperation(req TypedColumnStringPredicateScanRequest) columnsemantics.Operation {
	switch req.Kind {
	case "", TypedColumnStringPredicateEqual:
		return columnsemantics.OpDictionaryEquality
	case TypedColumnStringPredicateInList:
		return columnsemantics.OpDictionaryInList
	case TypedColumnStringPredicateCategory:
		return columnsemantics.OpDictionaryCategory
	case TypedColumnStringPredicatePrefix:
		return columnsemantics.OpStringPrefix
	case TypedColumnStringPredicateLexicalRange:
		return columnsemantics.OpStringLexicalRange
	default:
		return columnsemantics.OpUnknownPredicateKind
	}
}

func typedColumnStringPredicateValues(req TypedColumnStringPredicateScanRequest) []string {
	switch req.Kind {
	case "", TypedColumnStringPredicateEqual:
		return []string{req.Value}
	case TypedColumnStringPredicateInList, TypedColumnStringPredicateCategory:
		return append([]string(nil), req.Values...)
	default:
		return nil
	}
}

func typedColumnStringUseTargetedRanges(integrity ColumnAssetReadIntegrity) bool {
	switch integrity {
	case ColumnAssetReadIntegrityCachedVerify, ColumnAssetReadIntegritySkipChecksums:
		return true
	default:
		return false
	}
}

func typedColumnStringReadFullAsset(readCache *columnPhysicalAssetReadCache, ref ColumnAssetRef, dst []byte, diag *TypedColumnStringPredicateScanDiagnostics) ([]byte, error) {
	if readCache == nil {
		return nil, errors.New("collections: typed-column string full asset read requires cache")
	}
	raw, err := readCache.read(ref, dst)
	if diag != nil {
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
	}
	if err != nil {
		return dst, err
	}
	if diag != nil {
		diag.FullAssetReads++
		diag.FullAssetBytes += uint64(len(raw))
		diag.PhysicalBytesScanned += int64(len(raw))
	}
	return raw, nil
}

func typedColumnStringReadFullAssetRange(raw []byte, offset int, length int, section bool, diag *TypedColumnStringPredicateScanDiagnostics) ([]byte, error) {
	if offset < 0 || length <= 0 || offset > len(raw) || length > len(raw)-offset {
		return nil, fmt.Errorf("collections: typed-column string full asset range offset=%d length=%d raw=%d", offset, length, len(raw))
	}
	out := raw[offset : offset+length]
	if diag != nil {
		if section {
			diag.SectionBytesRead += uint64(len(out))
		} else {
			diag.RangeBytesRead += uint64(len(out))
		}
	}
	return out, nil
}

func typedColumnStringEnsureFullAssetValidated(readCache *columnPhysicalAssetReadCache, ref ColumnAssetRef, integrity ColumnAssetReadIntegrity, validated map[ColumnAssetRef]struct{}, diag *TypedColumnStringPredicateScanDiagnostics) error {
	if integrity == ColumnAssetReadIntegritySkipChecksums {
		return nil
	}
	if _, ok := validated[ref]; ok {
		return nil
	}
	n, err := readCache.validateFullRef(ref)
	if diag != nil {
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
	}
	if err != nil {
		return err
	}
	validated[ref] = struct{}{}
	if diag != nil {
		diag.FullAssetReads++
		diag.FullAssetBytes += uint64(n)
		diag.PhysicalBytesScanned += int64(n)
	}
	return nil
}

func typedColumnStringReadRange(readCache *columnPhysicalAssetReadCache, ref ColumnAssetRef, offset int, length int, section bool, diag *TypedColumnStringPredicateScanDiagnostics) ([]byte, error) {
	if offset < 0 || length <= 0 {
		return nil, fmt.Errorf("collections: typed-column string range offset=%d length=%d is invalid", offset, length)
	}
	raw, _, err := readCache.readRangeHandle(ref, int64(offset), int64(length))
	if diag != nil {
		diag.SegmentFileCacheHits = readCache.hits
		diag.SegmentFileCacheMisses = readCache.misses
	}
	if err != nil {
		return nil, err
	}
	if diag != nil {
		if readCache.lastView {
			diag.MappedBytes += uint64(len(raw))
		} else {
			diag.HeapCopyBytes += uint64(len(raw))
		}
		if section {
			diag.SectionBytesRead += uint64(len(raw))
		} else {
			diag.RangeBytesRead += uint64(len(raw))
		}
		diag.PhysicalBytesScanned += int64(len(raw))
	}
	return raw, nil
}

func typedColumnStringAddPreparedDiagnostics(diag *TypedColumnStringPredicateScanDiagnostics, src typedColumnPreparedStateDiagnostics) {
	if diag == nil {
		return
	}
	diag.DecodedMetadataBytes += src.ManifestBytes + src.DescriptorBytes + src.ContractBytes + src.DecodedMetadataBytes
	diag.DirectViewCertified += src.DirectViewCertified
	diag.StreamingCertified += src.StreamingCertified
	diag.StatsCertified += src.StatsCertified
	diag.PruningCertified += src.PruningCertified
	diag.CertificationFailures += src.CertificationFailures
	diag.CertificationFailureReason = src.CertificationFailureReason
	diag.PruningFallbackBlocks += src.PruningFallbackBlocks
	diag.PruningFallbackReason = src.PruningFallbackReason
	diag.PruningValidationFailures += src.PruningValidationFailures
	diag.PruningValidationFailureReason = src.PruningValidationFailureReason
}

func typedColumnStringPredicatePhysicalRowIDs(raw []byte, ref ColumnAssetRef, collection string, cfg ColumnStoreConfig, physicalRows int, matchedRows []TypedColumnStringPredicateScanRow) (map[int][]byte, error) {
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(cfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	wanted := make(map[int]struct{}, len(matchedRows))
	for _, row := range matchedRows {
		physicalRowIndex, err := typedColumnPhysicalRowIndexFromPrimaryID(row.PrimaryID, physicalRows)
		if err != nil {
			return nil, err
		}
		wanted[physicalRowIndex] = struct{}{}
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
