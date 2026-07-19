package collections

import (
	"bytes"
	"errors"
	"fmt"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	columnPhysicalRowAssetPartID = uint64(1)
	typedColumnPartAssetPartID   = uint64(2)
)

func columnStoreHasTypedColumnPartOwners(cfg ColumnStoreConfig) bool {
	for _, col := range cfg.Columns {
		if columnStoreColumnIsTypedColumnPart(col) {
			return true
		}
	}
	return false
}

func columnStoreRowAssetColumns(cfg ColumnStoreConfig) []ColumnStoreColumn {
	columns := make([]ColumnStoreColumn, 0, len(cfg.Columns))
	for _, col := range cfg.Columns {
		if columnStoreColumnIsTypedRowAsset(col) {
			columns = append(columns, col)
		}
	}
	return columns
}

func columnStoreTypedColumnPartFields(cfg ColumnStoreConfig) []TypedStorageField {
	fields := make([]TypedStorageField, 0, len(cfg.Columns))
	for _, col := range cfg.Columns {
		if !columnStoreColumnIsTypedColumnPart(col) {
			continue
		}
		fields = append(fields, TypedStorageField{
			Name:               col.Name,
			Path:               col.Path,
			Owner:              TypedStorageOwnerColumnPart,
			ValueType:          col.ValueType,
			Nullable:           col.Nullable,
			Dictionary:         col.Dictionary,
			VectorDims:         col.VectorDims,
			ElementsPerRow:     col.ElementsPerRow,
			BytesPerRow:        col.BytesPerRow,
			BitsPerElement:     col.BitsPerElement,
			AdjacencyDegree:    col.AdjacencyDegree,
			AdjacencyLayout:    col.AdjacencyLayout,
			FixedWidthEncoding: col.FixedWidthEncoding,
		})
	}
	return fields
}

func columnStoreRowAssetConfig(cfg ColumnStoreConfig) ColumnStoreConfig {
	if !columnStoreHasTypedColumnPartOwners(cfg) {
		return cfg
	}
	out := cfg.copy()
	out.Columns = columnStoreRowAssetColumns(cfg)
	out.SortKey = filterColumnSortKeysForColumns(cfg.SortKey, out.Columns)
	out.AggregateMetadata = filterColumnAggregateMetadataForColumns(cfg.AggregateMetadata, out.Columns)
	// The manifest/schema identity remains the full typed-storage layout hash.
	// Row assets only carry typed_row_asset-owned fields plus row locators.
	out.SchemaHash = cfg.SchemaHash
	return out
}

func filterColumnSortKeysForColumns(sortKeys []ColumnSortKey, columns []ColumnStoreColumn) []ColumnSortKey {
	if len(sortKeys) == 0 {
		return nil
	}
	present := columnStoreColumnNameSet(columns)
	out := make([]ColumnSortKey, 0, len(sortKeys))
	for _, sortKey := range sortKeys {
		if _, ok := present[sortKey.Column]; ok {
			out = append(out, sortKey)
		}
	}
	return out
}

func filterColumnAggregateMetadataForColumns(aggregates []ColumnAggregateMetadata, columns []ColumnStoreColumn) []ColumnAggregateMetadata {
	if len(aggregates) == 0 {
		return nil
	}
	present := columnStoreColumnNameSet(columns)
	out := make([]ColumnAggregateMetadata, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.Column != "" {
			if _, ok := present[aggregate.Column]; !ok {
				continue
			}
		}
		if aggregate.GroupColumn != "" {
			if _, ok := present[aggregate.GroupColumn]; !ok {
				continue
			}
		}
		predicateColumnsPresent := true
		for _, predicate := range aggregate.Predicates {
			if _, ok := present[predicate.Column]; !ok {
				predicateColumnsPresent = false
				break
			}
		}
		if !predicateColumnsPresent {
			continue
		}
		out = append(out, aggregate)
	}
	return out
}

func columnStoreTypedColumnPartAggregateMetadata(cfg ColumnStoreConfig) []ColumnAggregateMetadata {
	if len(cfg.AggregateMetadata) == 0 || !columnStoreHasTypedColumnPartOwners(cfg) {
		return nil
	}
	ownerByColumn := make(map[string]TypedStorageFieldOwner, len(cfg.Columns))
	for _, col := range cfg.Columns {
		ownerByColumn[col.Name] = columnStoreColumnOwnerOrRowAsset(col)
	}
	out := make([]ColumnAggregateMetadata, 0, len(cfg.AggregateMetadata))
	for _, aggregate := range cfg.AggregateMetadata {
		if aggregate.GroupColumn == "" {
			continue
		}
		if ownerByColumn[aggregate.GroupColumn] != TypedStorageOwnerColumnPart {
			continue
		}
		if aggregate.Kind != ColumnAggregateCount && (aggregate.Column == "" || ownerByColumn[aggregate.Column] != TypedStorageOwnerColumnPart) {
			continue
		}
		allPredicateColumnsTyped := true
		for _, predicate := range aggregate.Predicates {
			if ownerByColumn[predicate.Column] != TypedStorageOwnerColumnPart {
				allPredicateColumnsTyped = false
				break
			}
		}
		if !allPredicateColumnsTyped {
			continue
		}
		out = append(out, aggregate)
	}
	return out
}

func columnStoreColumnNameSet(columns []ColumnStoreColumn) map[string]struct{} {
	present := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		present[col.Name] = struct{}{}
	}
	return present
}

func projectColumnDeclaredRowsForColumns(allColumns, selected []ColumnStoreColumn, rows []columnDeclaredRow) ([]columnDeclaredRow, error) {
	if len(selected) == 0 {
		out := make([]columnDeclaredRow, len(rows))
		for rowIdx, row := range rows {
			out[rowIdx] = columnDeclaredRow{ID: row.ID, Deleted: row.Deleted}
			if row.Deleted {
				continue
			}
			if len(row.Values) != len(allColumns) {
				return nil, fmt.Errorf("collections: typed-storage row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(allColumns))
			}
		}
		return out, nil
	}
	if len(selected) == len(allColumns) {
		all := true
		for i := range selected {
			if selected[i].Name != allColumns[i].Name || selected[i].Path != allColumns[i].Path {
				all = false
				break
			}
		}
		if all {
			return rows, nil
		}
	}
	indexByName := make(map[string]int, len(allColumns))
	for i, col := range allColumns {
		indexByName[col.Name] = i
	}
	out := make([]columnDeclaredRow, len(rows))
	for rowIdx, row := range rows {
		out[rowIdx] = columnDeclaredRow{ID: bytes.Clone(row.ID), Deleted: row.Deleted}
		if row.Deleted {
			continue
		}
		if len(row.Values) != len(allColumns) {
			return nil, fmt.Errorf("collections: typed-storage row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(allColumns))
		}
		out[rowIdx].Values = make([]columnDeclaredValue, len(selected))
		for selectedIdx, col := range selected {
			allIdx, ok := indexByName[col.Name]
			if !ok {
				return nil, fmt.Errorf("collections: typed-storage selected column %q not found", col.Name)
			}
			out[rowIdx].Values[selectedIdx] = row.Values[allIdx]
		}
	}
	return out, nil
}

func typedColumnPartPublicationSortKey(cfg ColumnStoreConfig, fields []TypedStorageField) ([]ColumnSortKey, error) {
	if len(cfg.SortKey) == 0 {
		return nil, nil
	}
	declared := make(map[string]struct{}, len(cfg.Columns))
	for _, col := range cfg.Columns {
		declared[col.Name] = struct{}{}
	}
	fieldByName := make(map[string]TypedStorageField, len(fields))
	for _, field := range fields {
		if field.Name != "" {
			fieldByName[field.Name] = field
		}
	}
	allOwnedByTypedPart := true
	for _, sortKey := range cfg.SortKey {
		if _, ok := declared[sortKey.Column]; !ok {
			return nil, fmt.Errorf("collections: typed-column part publication sort key references unknown column %q", sortKey.Column)
		}
		if _, ok := fieldByName[sortKey.Column]; !ok {
			allOwnedByTypedPart = false
		}
	}
	if !allOwnedByTypedPart {
		return nil, nil
	}
	if len(cfg.SortKey) > typedColumnPartSortKeyMaxColumns {
		return nil, fmt.Errorf("collections: typed-column part publication sort key columns=%d exceeds cap %d", len(cfg.SortKey), typedColumnPartSortKeyMaxColumns)
	}
	for _, sortKey := range cfg.SortKey {
		field := fieldByName[sortKey.Column]
		if sortKey.Direction != ColumnSortAscending {
			return nil, fmt.Errorf("collections: typed-column part publication sort key column %q direction %q is unsupported; only ascending is supported", sortKey.Column, sortKey.Direction)
		}
		if field.Nullable {
			return nil, fmt.Errorf("collections: typed-column part publication sort key column %q is nullable; null/default ordering is not defined", sortKey.Column)
		}
		if !columnStoreValueTypeSupportsTypedColumnPartSort(field.ValueType) {
			return nil, fmt.Errorf("collections: typed-column part publication sort key column %q value_type %q is unsupported", sortKey.Column, field.ValueType)
		}
	}
	return cloneColumnSortKeys(cfg.SortKey), nil
}

func typedColumnAdapterRowsFromDeclaredRows(allColumns []ColumnStoreColumn, fields []TypedStorageField, rows []columnDeclaredRow) ([]typedColumnAdapterRow, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	indexByPath := make(map[string]int, len(allColumns))
	for i, col := range allColumns {
		indexByPath[col.Path] = i
	}
	out := make([]typedColumnAdapterRow, len(rows))
	for rowIdx, row := range rows {
		if row.Deleted {
			return nil, fmt.Errorf("collections: typed-column part row[%d] is deleted", rowIdx)
		}
		if len(row.Values) != len(allColumns) {
			return nil, fmt.Errorf("collections: typed-column part row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(allColumns))
		}
		values := make(map[string]columnDeclaredValue, len(fields))
		for _, field := range fields {
			allIdx, ok := indexByPath[field.Path]
			if !ok {
				return nil, fmt.Errorf("collections: typed-column part field %q not found", field.Path)
			}
			values[field.Path] = row.Values[allIdx]
		}
		out[rowIdx] = typedColumnAdapterRow{PrimaryID: int64(rowIdx), Values: values}
	}
	return out, nil
}

type typedColumnPartImageBuildResult struct {
	Bytes                []byte
	Rows                 int
	TypedGranuleRowOrder []int
	Metrics              typedColumnPartImageBuildMetrics
}

type typedColumnPartImageBuildMetrics struct {
	DictionaryBuild    time.Duration
	RowMaterialization time.Duration
	PartBuild          time.Duration
	ImageBuild         time.Duration
}

func buildTypedColumnPartImageForDeclaredRows(cfg ColumnStoreConfig, generation, partID uint64, rows []columnDeclaredRow) ([]byte, int, error) {
	result, err := buildTypedColumnPartImageForDeclaredRowsWithResult(cfg, generation, partID, rows)
	if err != nil {
		return nil, 0, err
	}
	return result.Bytes, result.Rows, nil
}

func buildTypedColumnPartImageForDeclaredRowsWithResult(cfg ColumnStoreConfig, generation, partID uint64, rows []columnDeclaredRow) (typedColumnPartImageBuildResult, error) {
	if !columnStoreHasTypedColumnPartOwners(cfg) {
		return typedColumnPartImageBuildResult{}, nil
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if len(fields) == 0 {
		return typedColumnPartImageBuildResult{}, nil
	}
	sortKey, err := typedColumnPartPublicationSortKey(cfg, fields)
	if err != nil {
		return typedColumnPartImageBuildResult{}, err
	}
	adapterOpts, err := typedColumnPublicationAdapterOptionsFromConfig(cfg, partID, fields, sortKey)
	if err != nil {
		return typedColumnPartImageBuildResult{}, err
	}
	adapterOpts.DictionaryModes = typedColumnPublicationDictionaryModes(fields)
	part, err := buildTypedColumnAdapterPartFromDeclaredRows(adapterOpts, cfg.Columns, rows)
	if err != nil {
		return typedColumnPartImageBuildResult{}, err
	}
	metrics := typedColumnPartImageBuildMetrics{
		DictionaryBuild:    part.Metrics.DictionaryBuild,
		RowMaterialization: part.Metrics.BatchAllocation + part.Metrics.BatchFill,
		PartBuild:          part.Metrics.PartBuild,
	}
	imageStart := time.Now()
	image, err := part.buildImage()
	if err != nil {
		return typedColumnPartImageBuildResult{}, err
	}
	metrics.ImageBuild += time.Since(imageStart)
	rowOrder, err := typedColumnPartDeclaredRowOrder(part, image.Rows)
	if err != nil {
		return typedColumnPartImageBuildResult{}, err
	}
	return typedColumnPartImageBuildResult{Bytes: image.Bytes, Rows: image.Rows, TypedGranuleRowOrder: rowOrder, Metrics: metrics}, nil
}

func typedColumnPublicationDictionaryModes(fields []TypedStorageField) map[string]typedColumnAdapterDictionaryMode {
	modes := make(map[string]typedColumnAdapterDictionaryMode)
	for _, field := range fields {
		if field.ValueType != "string" {
			continue
		}
		name := field.Name
		if name == "" {
			name = field.Path
		}
		if name == "" {
			continue
		}
		modes[name] = typedColumnAdapterDictionaryMode{Forward: true}
	}
	if len(modes) == 0 {
		return nil
	}
	return modes
}

func typedColumnPartDeclaredRowOrder(part *typedColumnAdapterPart, rows int) ([]int, error) {
	if rows == 0 {
		return nil, nil
	}
	if part == nil || part.Part == nil {
		return nil, errors.New("collections: typed-column part row order requires built part")
	}
	if !typedColumnAdapterPartHasLogicalSortKey(part) {
		return nil, nil
	}
	if len(part.Part.Locators) != rows {
		return nil, fmt.Errorf("collections: typed-column part locators=%d want rows=%d", len(part.Part.Locators), rows)
	}
	order := make([]int, rows)
	seen := make([]bool, rows)
	for _, locator := range part.Part.Locators {
		if locator.PartRow < 0 || locator.PartRow >= rows {
			return nil, fmt.Errorf("collections: typed-column part row order part_row=%d outside rows=%d", locator.PartRow, rows)
		}
		if locator.PrimaryID < 0 || locator.PrimaryID >= int64(rows) {
			return nil, fmt.Errorf("collections: typed-column part row order primary_id=%d outside rows=%d", locator.PrimaryID, rows)
		}
		if seen[locator.PartRow] {
			return nil, fmt.Errorf("collections: typed-column part row order duplicate part_row=%d", locator.PartRow)
		}
		order[locator.PartRow] = int(locator.PrimaryID)
		seen[locator.PartRow] = true
	}
	for idx, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("collections: typed-column part row order missing part_row=%d", idx)
		}
	}
	return order, nil
}

type typedColumnPartVisibleValues struct {
	Values []columnDeclaredValue
}

type typedColumnPartSet struct {
	Generation uint64
	Refs       []columnManifestAssetRefForScan
}

func (set typedColumnPartSet) primaryRef() (columnManifestAssetRefForScan, bool) {
	for _, ref := range set.Refs {
		if ref.Ref.PartID == typedColumnPartAssetPartID {
			return ref, true
		}
	}
	return columnManifestAssetRefForScan{}, false
}

type typedColumnPartReconstructionCache struct {
	Parts        map[uint64]typedColumnPartDecodedValues
	Fields       []TypedStorageField
	Refs         map[uint64]columnManifestAssetRefForScan
	RefsLoaded   bool
	ReadCache    *columnPhysicalAssetReadCache
	SelectionKey string

	CacheHits        uint64
	CacheMisses      uint64
	PartLoads        uint64
	TypedPartDecodes uint64

	// Window* describe only the current bounded selective reconstruction
	// window. They deliberately do not accumulate across a scan.
	WindowDecodedBytes    uint64
	WindowGenerations     uint64
	WindowSourcePartBytes uint64

	// SelectivePart is a source-backed adapter retained only while the
	// certified physical stream remains on one generation. Its image aliases
	// ReadCache's reusable source scratch. Decoded values must own any data that
	// outlives a read because a later generation reuses that scratch.
	SelectivePart           *typedColumnAdapterPart
	SelectivePartGeneration uint64
}

func typedColumnPartDecodedValuesResidentBytes(values typedColumnPartDecodedValues) uint64 {
	bytes := uint64(len(values.PrimaryIDs))*uint64(unsafe.Sizeof(int64(0))) + uint64(len(values.RowByPrimaryID))*uint64(unsafe.Sizeof(int(0)))
	for _, row := range values.Values {
		bytes += uint64(cap(row)) * uint64(unsafe.Sizeof(columnDeclaredValue{}))
		for _, value := range row {
			bytes += uint64(cap(value.Float32Vector))*4 + uint64(cap(value.DenseNumericVector)) + uint64(cap(value.Uint32List))*4 + uint64(cap(value.AdjacencyList))*4 + uint64(cap(value.Bytes)) + uint64(cap(value.StringBytes)) + uint64(len(value.String))
		}
	}
	return bytes
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshot(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow) (typedColumnPartVisibleValues, error) {
	return c.typedColumnPartValuesForVisibleRowAtSnapshotWithCache(snap, manifestRootID, cfg, physicalRow, nil)
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshotWithCache(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache) (typedColumnPartVisibleValues, error) {
	return c.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap, manifestRootID, cfg, physicalRow, cache, nil)
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache, dst []columnDeclaredValue) (typedColumnPartVisibleValues, error) {
	return c.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCacheProjected(snap, manifestRootID, cfg, physicalRow, cache, dst, nil)
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCacheProjected(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache, dst []columnDeclaredValue, selected []bool) (typedColumnPartVisibleValues, error) {
	if !columnStoreHasTypedColumnPartOwners(cfg) {
		return typedColumnPartVisibleValues{}, nil
	}
	var fields []TypedStorageField
	if cache != nil && cache.Fields != nil {
		fields = cache.Fields
	} else {
		fields = columnStoreTypedColumnPartFields(cfg)
		if cache != nil {
			cache.Fields = fields
		}
	}
	if len(fields) == 0 {
		return typedColumnPartVisibleValues{}, nil
	}
	if selected != nil && len(selected) != len(fields) {
		return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction projection fields=%d want %d", len(selected), len(fields))
	}
	if physicalRow.Deleted {
		return typedColumnPartVisibleValues{}, nil
	}
	if selected != nil && !documentProjectionHasSelectedTypedColumn(selected) {
		values := dst
		if cap(values) < len(fields) {
			values = make([]columnDeclaredValue, len(fields))
		} else {
			values = values[:len(fields)]
			clear(values)
		}
		return typedColumnPartVisibleValues{Values: values}, nil
	}
	if cache != nil {
		selectionKey := documentProjectionKey(selected)
		if cache.SelectionKey != selectionKey {
			cache.Parts = make(map[uint64]typedColumnPartDecodedValues)
			cache.SelectionKey = selectionKey
		}
	}
	var decoded typedColumnPartDecodedValues
	var ok bool
	if cache != nil && cache.Parts != nil {
		decoded, ok = cache.Parts[physicalRow.Generation]
	}
	if ok {
		cache.CacheHits++
	} else {
		if cache != nil {
			cache.CacheMisses++
		}
		ref, found, err := c.typedColumnPartRefForGenerationWithCache(snap, manifestRootID, cfg, physicalRow.Generation, cache)
		if err != nil {
			return typedColumnPartVisibleValues{}, err
		}
		if !found {
			return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction missing typed_column_part asset for generation=%d", physicalRow.Generation)
		}
		readCache := (*columnPhysicalAssetReadCache)(nil)
		closeReadCache := false
		if cache != nil && cache.ReadCache != nil {
			readCache = cache.ReadCache
		} else {
			localReadCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
			if err != nil {
				return typedColumnPartVisibleValues{}, err
			}
			readCache = &localReadCache
			closeReadCache = true
		}
		raw, readErr := readCache.read(ref.Ref, nil)
		if readErr != nil {
			if closeReadCache {
				if closeErr := readCache.close(); closeErr != nil {
					readErr = errors.Join(readErr, closeErr)
				}
			}
			return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction read generation=%d part_id=%d: %w", ref.Ref.Generation, ref.Ref.PartID, readErr)
		}
		if cache != nil {
			cache.PartLoads++
		}
		part, err := typedColumnAdapterPartFromBytesForReconstruction(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(cfg.SchemaHash)}, raw)
		if err != nil {
			if closeReadCache {
				if closeErr := readCache.close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
			}
			return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction decode generation=%d part_id=%d: %w", ref.Ref.Generation, ref.Ref.PartID, err)
		}
		decoded, err = part.scanDecodedValuesSelectedForReconstruction(selected)
		var closeErr error
		if closeReadCache {
			closeErr = readCache.close()
		}
		if err != nil {
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return typedColumnPartVisibleValues{}, err
		}
		if closeErr != nil {
			return typedColumnPartVisibleValues{}, closeErr
		}
		if cache != nil {
			cache.TypedPartDecodes++
		}
		if cache != nil {
			if cache.Parts == nil {
				cache.Parts = make(map[uint64]typedColumnPartDecodedValues)
			}
			cache.Parts[physicalRow.Generation] = decoded
		}
	}
	if len(decoded.Values) != len(fields) {
		return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction decoded fields=%d want %d", len(decoded.Values), len(fields))
	}
	values, err := decoded.valuesForRowInto(physicalRow.RowIndex, dst)
	if err != nil {
		return typedColumnPartVisibleValues{}, err
	}
	return typedColumnPartVisibleValues{Values: values}, nil
}

func (c *Collection) typedColumnPartRefForGeneration(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, generation uint64) (columnManifestAssetRefForScan, bool, error) {
	return c.typedColumnPartRefForGenerationWithCache(snap, rootID, cfg, generation, nil)
}

func (c *Collection) typedColumnPartRefForGenerationWithCache(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig, generation uint64, cache *typedColumnPartReconstructionCache) (columnManifestAssetRefForScan, bool, error) {
	if cache != nil && cache.RefsLoaded {
		ref, found := cache.Refs[generation]
		return ref, found, nil
	}
	refs, err := c.typedColumnPartRefsByGeneration(snap, rootID, cfg)
	if err != nil {
		return columnManifestAssetRefForScan{}, false, err
	}
	if cache != nil {
		cache.Refs = refs
		cache.RefsLoaded = true
	}
	ref, found := refs[generation]
	return ref, found, nil
}

func (c *Collection) typedColumnPartRefsByGeneration(snap *backenddb.Snapshot, rootID uint64, cfg ColumnStoreConfig) (map[uint64]columnManifestAssetRefForScan, error) {
	if rootID == 0 {
		return nil, errors.New("collections: typed-column reconstruction missing manifest root")
	}
	if snap == nil {
		return nil, errCollectionDBNil
	}
	if cfg.AssetManager == nil {
		return nil, errors.New("collections: typed-column reconstruction requires column asset manager metadata")
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return nil, err
	}
	return typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace)
}

func typedColumnPartRefsByGenerationFromManifestRecords(records []columnManifestRecord, expectedNamespace string) (map[uint64]columnManifestAssetRefForScan, error) {
	sets, physicalRowsByGeneration, err := typedColumnPartSetsByGenerationFromManifestRecords(records, expectedNamespace)
	if err != nil {
		return nil, err
	}
	refs := make(map[uint64]columnManifestAssetRefForScan, len(sets))
	for generation, set := range sets {
		ref, ok := set.primaryRef()
		if !ok {
			return nil, fmt.Errorf("collections: typed-column manifest generation=%d missing primary typed_column_part ref part_id=%d", generation, typedColumnPartAssetPartID)
		}
		physicalRows, ok := physicalRowsByGeneration[generation]
		if !ok {
			return nil, fmt.Errorf("collections: typed-column manifest missing typed_row_asset ref for generation=%d", generation)
		}
		if ref.Rows != physicalRows {
			return nil, fmt.Errorf("collections: typed-column manifest rows=%d does not match physical rows=%d for generation=%d", ref.Rows, physicalRows, generation)
		}
		refs[generation] = ref
	}
	return refs, nil
}

func typedColumnPartSetsByGenerationFromManifestRecords(records []columnManifestRecord, expectedNamespace string) (map[uint64]typedColumnPartSet, map[uint64]int, error) {
	sets := make(map[uint64]typedColumnPartSet, len(records)/2)
	seenPartIDs := make(map[[2]uint64]struct{}, len(records)/2)
	physicalRowsByGeneration := make(map[uint64]int, len(records)/2)
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		keyGeneration, keyPartID, err := columnManifestPartKeyFromRecordKeyForScan(record.key)
		if err != nil {
			return nil, nil, err
		}
		ref, rows, _, _, _, reason, err := decodeColumnManifestPartFieldsForScan(record.value, expectedNamespace)
		if err != nil {
			return nil, nil, err
		}
		switch ref.Kind {
		case ColumnAssetKindTCS1PartImage:
			if ref.Generation != keyGeneration || ref.PartID != keyPartID {
				return nil, nil, fmt.Errorf("collections: typed-row manifest key generation/part mismatch")
			}
			if ref.PartID == columnPhysicalRowAssetPartID {
				physicalRowsByGeneration[ref.Generation] = rows
			}
			continue
		case ColumnAssetKindTCS1TypedColumnPart:
		default:
			continue
		}
		if ref.Generation != keyGeneration || ref.PartID != keyPartID {
			return nil, nil, fmt.Errorf("collections: typed-column manifest key generation/part mismatch")
		}
		operation, ok := columnPhysicalScanOperationFromBytes(reason)
		if !ok {
			return nil, nil, fmt.Errorf("collections: unsupported typed-column manifest reason %q", string(reason))
		}
		role, err := decodeColumnManifestPartRoleForScan(record.value, ref, reason)
		if err != nil {
			return nil, nil, err
		}
		sortKey, err := decodeColumnManifestPartSortKeyForScan(record.value)
		if err != nil {
			return nil, nil, err
		}
		if role == ColumnManifestPartRoleTombstone {
			return nil, nil, fmt.Errorf("collections: typed-column manifest ref generation=%d part_id=%d cannot be tombstone role", ref.Generation, ref.PartID)
		}
		partKey := [2]uint64{ref.Generation, ref.PartID}
		if _, exists := seenPartIDs[partKey]; exists {
			return nil, nil, fmt.Errorf("collections: duplicate typed-column manifest ref for generation=%d part_id=%d", ref.Generation, ref.PartID)
		}
		seenPartIDs[partKey] = struct{}{}
		set := sets[ref.Generation]
		if set.Generation == 0 {
			set.Generation = ref.Generation
			set.Refs = make([]columnManifestAssetRefForScan, 0, 4)
		}
		set.Refs = append(set.Refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows, SortKey: sortKey})
		sets[ref.Generation] = set
	}
	return sets, physicalRowsByGeneration, nil
}
