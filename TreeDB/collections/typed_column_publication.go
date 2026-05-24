package collections

import (
	"bytes"
	"errors"
	"fmt"

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
			AdjacencyDegree:    col.AdjacencyDegree,
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
		if aggregate.Column == "" || aggregate.GroupColumn == "" {
			continue
		}
		if ownerByColumn[aggregate.Column] != TypedStorageOwnerColumnPart || ownerByColumn[aggregate.GroupColumn] != TypedStorageOwnerColumnPart {
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

func buildTypedColumnPartImageForDeclaredRows(cfg ColumnStoreConfig, generation, partID uint64, rows []columnDeclaredRow) ([]byte, int, error) {
	if !columnStoreHasTypedColumnPartOwners(cfg) {
		return nil, 0, nil
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if len(fields) == 0 {
		return nil, 0, nil
	}
	adapterRows, err := typedColumnAdapterRowsFromDeclaredRows(cfg.Columns, fields, rows)
	if err != nil {
		return nil, 0, err
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{
		Collection:    "",
		Namespace:     cfg.AssetManager.Namespace,
		SchemaVersion: uint32(cfg.SchemaHash),
		PartID:        partID,
		Fields:        fields,
	}, adapterRows)
	if err != nil {
		return nil, 0, err
	}
	image, err := part.buildImage()
	if err != nil {
		return nil, 0, err
	}
	return image.Bytes, image.Rows, nil
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
	Parts      map[uint64]typedColumnPartDecodedValues
	Fields     []TypedStorageField
	Refs       map[uint64]columnManifestAssetRefForScan
	RefsLoaded bool

	CacheHits        uint64
	CacheMisses      uint64
	PartLoads        uint64
	TypedPartDecodes uint64
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshot(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow) (typedColumnPartVisibleValues, error) {
	return c.typedColumnPartValuesForVisibleRowAtSnapshotWithCache(snap, manifestRootID, cfg, physicalRow, nil)
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshotWithCache(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache) (typedColumnPartVisibleValues, error) {
	return c.typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap, manifestRootID, cfg, physicalRow, cache, nil)
}

func (c *Collection) typedColumnPartValuesForVisibleRowAtSnapshotIntoWithCache(snap *backenddb.Snapshot, manifestRootID uint64, cfg ColumnStoreConfig, physicalRow columnPhysicalVisibleRow, cache *typedColumnPartReconstructionCache, dst []columnDeclaredValue) (typedColumnPartVisibleValues, error) {
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
	if physicalRow.Deleted {
		return typedColumnPartVisibleValues{}, nil
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
		readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
		if err != nil {
			return typedColumnPartVisibleValues{}, err
		}
		raw, readErr := readCache.read(ref.Ref, nil)
		if readErr != nil {
			if closeErr := readCache.close(); closeErr != nil {
				readErr = errors.Join(readErr, closeErr)
			}
			return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction read generation=%d part_id=%d: %w", ref.Ref.Generation, ref.Ref.PartID, readErr)
		}
		if cache != nil {
			cache.PartLoads++
		}
		part, err := typedColumnAdapterPartFromBytesForReconstruction(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(cfg.SchemaHash)}, raw)
		if err != nil {
			if closeErr := readCache.close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return typedColumnPartVisibleValues{}, fmt.Errorf("collections: typed-column reconstruction decode generation=%d part_id=%d: %w", ref.Ref.Generation, ref.Ref.PartID, err)
		}
		decoded, err = part.scanDecodedValues()
		closeErr := readCache.close()
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
		set.Refs = append(set.Refs, columnManifestAssetRefForScan{Ref: ref, Reason: operation, Role: role, Rows: rows})
		sets[ref.Generation] = set
	}
	return sets, physicalRowsByGeneration, nil
}
