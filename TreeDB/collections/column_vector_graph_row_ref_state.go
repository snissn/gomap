package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnVectorGraphRowRefStateAssetIDPrefix = "base_row_ref/"
	columnVectorGraphRowRefStatePathSuffix    = "_base_row_ref_"
	columnVectorGraphRowRefStateScopeID       = "column-vector-graph-row-ref-state"
)

const maxColumnVectorGraphRowRefInt64Uint64 = uint64(1<<63 - 1)

type columnVectorGraphRowRefStateField string

const (
	columnVectorGraphRowRefStateFieldGeneration        columnVectorGraphRowRefStateField = "generation"
	columnVectorGraphRowRefStateFieldPartID            columnVectorGraphRowRefStateField = "part_id"
	columnVectorGraphRowRefStateFieldRowIndex          columnVectorGraphRowRefStateField = "row_index"
	columnVectorGraphRowRefStateFieldAppliedCommandLSN columnVectorGraphRowRefStateField = "applied_command_lsn"
)

var columnVectorGraphRowRefStateFields = []columnVectorGraphRowRefStateField{
	columnVectorGraphRowRefStateFieldGeneration,
	columnVectorGraphRowRefStateFieldPartID,
	columnVectorGraphRowRefStateFieldRowIndex,
	columnVectorGraphRowRefStateFieldAppliedCommandLSN,
}

type columnVectorGraphPreparedRowRefStateAsset struct {
	Field        columnVectorGraphRowRefStateField
	Config       ColumnStoreConfig
	Ref          ColumnAssetRef
	Bytes        int64
	Rows         int
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphPreparedRowRefStatePayload struct {
	Field        columnVectorGraphRowRefStateField
	Config       ColumnStoreConfig
	PartID       uint64
	Payload      []byte
	Bytes        int64
	Rows         int
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphRowRefStateSource struct {
	rows               int
	generations        typeddecode.PreparedInt64DirectView
	partIDs            typeddecode.PreparedInt64DirectView
	rowIndexes         typeddecode.PreparedInt64DirectView
	appliedCommandLSNs typeddecode.PreparedInt64DirectView

	manager          *mappedresource.Manager
	mmapDirectFields uint64
	mappedBytes      uint64
	activeHandles    int64
	deniedResources  uint64
	closed           bool
}

func columnVectorGraphRowRefStateAssetID(field columnVectorGraphRowRefStateField) string {
	return columnVectorGraphRowRefStateAssetIDPrefix + string(field)
}

func columnVectorGraphRowRefStateFieldFromAssetID(assetID string) (columnVectorGraphRowRefStateField, error) {
	for _, field := range columnVectorGraphRowRefStateFields {
		if assetID == columnVectorGraphRowRefStateAssetID(field) {
			return field, nil
		}
	}
	return "", fmt.Errorf("collections: vector-index row-ref state asset id %q is not a known base row-ref coordinate", assetID)
}

func columnVectorGraphRowRefStateColumnName(field columnVectorGraphRowRefStateField) string {
	return "base_row_ref_" + string(field)
}

func columnVectorGraphRowRefStatePath(def VectorIndexDefinition, field columnVectorGraphRowRefStateField) string {
	return def.Field + columnVectorGraphRowRefStatePathSuffix + string(field)
}

func columnVectorGraphRowRefStateColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, field columnVectorGraphRowRefStateField) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	if _, err := columnVectorGraphRowRefStateFieldFromAssetID(columnVectorGraphRowRefStateAssetID(field)); err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if normalizedDef.Strategy != VectorIndexStrategyColumnGraph {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", normalizedDef.Name, normalizedDef.Strategy)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph row-ref state requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph row-ref state requires base asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:               columnVectorGraphRowRefStateColumnName(field),
			Path:               columnVectorGraphRowRefStatePath(normalizedDef, field),
			Owner:              TypedStorageOwnerColumnPart,
			ValueType:          ColumnStoreValueInt64,
			FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
		}},
		RetainedPayload: ColumnRetainedPayloadNone,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      base.AssetManager.Kind,
			Namespace: base.AssetManager.Namespace,
		},
	})
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	fields := columnStoreTypedColumnPartFields(*cfg)
	if len(fields) != 1 {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph row-ref state %s fields=%d want 1", field, len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || columns[0].Field.ValueType != ColumnStoreValueInt64 || columns[0].Definition.Type != typedcolumn.ColumnTypeInt64 || columns[0].Definition.Encoding != typedcolumn.EncodingRawInt64 || columns[0].Definition.Compression != typedcolumn.CompressionNone || columns[0].FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph row-ref state %s is not native raw_int64", field)
	}
	return *cfg, columns[0], nil
}

func prepareColumnVectorGraphRowRefStateAssets(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedRowRefStateAsset, error) {
	return prepareColumnVectorGraphRowRefStateAssetsWithStableAuthority(assetRootDir, collection, base, def, generation, firstPartID, rows, nil)
}

func prepareColumnVectorGraphRowRefStateAssetsWithStableAuthority(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) ([]columnVectorGraphPreparedRowRefStateAsset, error) {
	payloads, err := prepareColumnVectorGraphRowRefStatePayloads(collection, base, def, generation, firstPartID, rows)
	if err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	if assetRootDir == "" {
		return nil, errors.New("collections: column_graph row-ref state requires asset root dir")
	}
	appender, err := newColumnVectorGraphAssetAppender(assetRootDir, payloads[0].Config, authority)
	if err != nil {
		return nil, err
	}
	assets, appendErr := appendColumnVectorGraphRowRefStatePayloads(appender, generation, payloads)
	closeErr := closeColumnVectorGraphAssetAppender(appender, authority)
	if appendErr != nil {
		return nil, errors.Join(appendErr, closeErr)
	}
	return assets, closeErr
}

func prepareColumnVectorGraphRowRefStatePayloads(collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedRowRefStatePayload, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if generation == 0 || firstPartID == 0 {
		return nil, errors.New("collections: column_graph row-ref state requires generation and first part_id")
	}
	payloads := make([]columnVectorGraphPreparedRowRefStatePayload, 0, len(columnVectorGraphRowRefStateFields))
	partID := firstPartID
	for i, field := range columnVectorGraphRowRefStateFields {
		values, err := columnVectorGraphRowRefStateValues(field, rows, generation)
		if err != nil {
			return nil, err
		}
		payload, err := prepareColumnVectorGraphRowRefStatePayloadFromValues(collection, base, def, partID, field, values)
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, payload)
		if i != len(columnVectorGraphRowRefStateFields)-1 {
			if partID == ^uint64(0) {
				return nil, errors.New("collections: column_graph row-ref state part_id overflow")
			}
			partID = nextColumnVectorGraphPartIDAfter(partID, partID)
		}
	}
	return payloads, nil
}

func columnVectorGraphRowRefStateValues(field columnVectorGraphRowRefStateField, rows []columnVectorGraphAssetRow, generation uint64) ([]int64, error) {
	values := make([]int64, len(rows))
	for ordinal, row := range rows {
		ref := row.BaseRowRef
		if err := validateColumnVectorGraphRowRefForState(ordinal, ref, generation); err != nil {
			return nil, err
		}
		switch field {
		case columnVectorGraphRowRefStateFieldGeneration:
			values[ordinal] = int64(ref.Generation)
		case columnVectorGraphRowRefStateFieldPartID:
			values[ordinal] = int64(ref.PartID)
		case columnVectorGraphRowRefStateFieldRowIndex:
			values[ordinal] = int64(ref.RowIndex)
		case columnVectorGraphRowRefStateFieldAppliedCommandLSN:
			values[ordinal] = int64(ref.AppliedCommandLSN)
		default:
			return nil, fmt.Errorf("collections: unknown column_graph row-ref state field %q", field)
		}
	}
	return values, nil
}

func validateColumnVectorGraphRowRefForState(ordinal int, ref DocumentRowRef, baseGeneration uint64) error {
	if ref.Generation == 0 || ref.PartID == 0 || ref.AppliedCommandLSN == 0 {
		return fmt.Errorf("collections: column_graph row-ref state ordinal=%d missing generation/part_id/applied_lsn: %+v", ordinal, ref)
	}
	if baseGeneration != 0 && ref.Generation > baseGeneration {
		return fmt.Errorf("collections: column_graph row-ref state ordinal=%d row generation=%d is newer than base manifest generation=%d", ordinal, ref.Generation, baseGeneration)
	}
	if ref.RowIndex < 0 {
		return fmt.Errorf("collections: column_graph row-ref state ordinal=%d row_index=%d is negative", ordinal, ref.RowIndex)
	}
	if ref.Generation > maxColumnVectorGraphRowRefInt64Uint64 || ref.PartID > maxColumnVectorGraphRowRefInt64Uint64 || ref.AppliedCommandLSN > maxColumnVectorGraphRowRefInt64Uint64 {
		return fmt.Errorf("collections: column_graph row-ref state ordinal=%d coordinate exceeds int64 storage range: %+v", ordinal, ref)
	}
	return nil
}

func prepareColumnVectorGraphRowRefStatePayloadFromValues(collection string, base ColumnStoreConfig, def VectorIndexDefinition, partID uint64, field columnVectorGraphRowRefStateField, values []int64) (columnVectorGraphPreparedRowRefStatePayload, error) {
	if partID == 0 {
		return columnVectorGraphPreparedRowRefStatePayload{}, errors.New("collections: column_graph row-ref state requires non-zero part_id")
	}
	sourceCfg, adapterColumn, err := columnVectorGraphRowRefStateColumnStoreConfig(collection, base, def, field)
	if err != nil {
		return columnVectorGraphPreparedRowRefStatePayload{}, err
	}
	primaryIDs := make([]int64, len(values))
	for rowIdx := range primaryIDs {
		primaryIDs[rowIdx] = int64(rowIdx)
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{
				Name:           typedColumnAdapterPrimaryIDColumn,
				Type:           typedcolumn.ColumnTypeInt64,
				Encoding:       typedcolumn.EncodingRawInt64,
				Compression:    typedcolumn.CompressionNone,
				CompressionSet: true,
				StatsDisabled:  true,
			},
			adapterColumn.Definition,
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows: len(values),
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
			adapterColumn.Definition.Name:     values,
		},
	})
	if err != nil {
		return columnVectorGraphPreparedRowRefStatePayload{}, err
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
		Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}),
		LayoutLogicalTypes: map[string]string{
			typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64),
			adapterColumn.Definition.Name:     string(ColumnStoreValueInt64),
		},
	})
	if err != nil {
		return columnVectorGraphPreparedRowRefStatePayload{}, err
	}
	if image.Rows != len(values) || image.PartID != partID {
		return columnVectorGraphPreparedRowRefStatePayload{}, fmt.Errorf("collections: column_graph row-ref state %s image rows/part=(%d,%d) want (%d,%d)", field, image.Rows, image.PartID, len(values), partID)
	}
	accounting := part.ByteAccountingFromImage(image)
	return columnVectorGraphPreparedRowRefStatePayload{
		Field:        field,
		Config:       sourceCfg,
		PartID:       partID,
		Payload:      image.Bytes,
		Bytes:        int64(len(image.Bytes)),
		Rows:         image.Rows,
		PaddingBytes: int64(accounting.SerializedPaddingBytes),
		SchemaHash:   sourceCfg.SchemaHash,
	}, nil
}

func appendColumnVectorGraphRowRefStatePayloads(appender *columnPhysicalAssetSegmentAppender, generation uint64, payloads []columnVectorGraphPreparedRowRefStatePayload) ([]columnVectorGraphPreparedRowRefStateAsset, error) {
	assets := make([]columnVectorGraphPreparedRowRefStateAsset, len(payloads))
	for i, payload := range payloads {
		alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, payload.Config)
		ref, err := appender.appendKindWithAlignment(payload.Payload, ColumnAssetKindTCS1TypedColumnPart, generation, payload.PartID, alignment)
		if err != nil {
			return nil, err
		}
		if err := validateColumnVectorGraphPreparedRowRefStateRef(payload, ref, generation); err != nil {
			appender.failed = true
			return nil, err
		}
		assets[i] = columnVectorGraphPreparedRowRefStateAsset{
			Field:        payload.Field,
			Config:       payload.Config,
			Ref:          ref,
			Bytes:        ref.Length,
			Rows:         payload.Rows,
			PaddingBytes: payload.PaddingBytes,
			SchemaHash:   payload.SchemaHash,
		}
	}
	return assets, nil
}

func validateColumnVectorGraphPreparedRowRefStateRef(payload columnVectorGraphPreparedRowRefStatePayload, ref ColumnAssetRef, generation uint64) error {
	if ref.Namespace != payload.Config.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != payload.PartID || ref.Length != payload.Bytes {
		return fmt.Errorf("collections: invalid column_graph row-ref state asset ref %+v", ref)
	}
	if ref.Offset%8 != 0 {
		return fmt.Errorf("collections: column_graph row-ref state absolute asset offset=%d is not 8-byte aligned", ref.Offset)
	}
	return nil
}

func columnVectorGraphRowRefStateAssetSnapshots(prepared []columnVectorGraphPreparedRowRefStateAsset) []columnVectorIndexStateAssetSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, len(prepared))
	for i, prepared := range prepared {
		assets[i] = columnVectorIndexStateAssetSnapshot{
			Role:             columnVectorIndexStateAssetRoleRowRefs,
			AssetID:          columnVectorGraphRowRefStateAssetID(prepared.Field),
			LogicalType:      columnVectorIndexStateLogicalTypeInt64,
			PhysicalEncoding: columnVectorIndexStateEncodingRawInt64,
			RowCount:         prepared.Rows,
			SourceSchemaHash: prepared.SchemaHash,
			Ref:              prepared.Ref,
			AssetBytes:       prepared.Bytes,
		}
	}
	return assets
}

func validateColumnVectorGraphRowRefStateManifestAssets(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot) error {
	assets, found, err := columnVectorGraphRowRefStateAssetsByField(state)
	if err != nil || !found {
		return err
	}
	for _, field := range columnVectorGraphRowRefStateFields {
		asset := assets[field]
		sourceCfg, _, err := columnVectorGraphRowRefStateColumnStoreConfig(collection, cfg, def, field)
		if err != nil {
			return err
		}
		if asset.SourceSchemaHash != sourceCfg.SchemaHash {
			return fmt.Errorf("collections: vector-index row-ref state %s schema_hash=%d want %d", field, asset.SourceSchemaHash, sourceCfg.SchemaHash)
		}
	}
	return nil
}

func columnVectorGraphRowRefStateAssetsByField(state columnVectorIndexStateSnapshot) (map[columnVectorGraphRowRefStateField]columnVectorIndexStateAssetSnapshot, bool, error) {
	assets := make(map[columnVectorGraphRowRefStateField]columnVectorIndexStateAssetSnapshot, len(columnVectorGraphRowRefStateFields))
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleRowRefs {
			continue
		}
		field, err := columnVectorGraphRowRefStateFieldFromAssetID(asset.AssetID)
		if err != nil {
			return nil, true, err
		}
		if asset.LogicalType != columnVectorIndexStateLogicalTypeInt64 || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawInt64 {
			return nil, true, fmt.Errorf("collections: vector-index row-ref state %s type/encoding=(%q,%q) want (%q,%q)", field, asset.LogicalType, asset.PhysicalEncoding, columnVectorIndexStateLogicalTypeInt64, columnVectorIndexStateEncodingRawInt64)
		}
		if _, exists := assets[field]; exists {
			return nil, true, fmt.Errorf("collections: duplicate vector-index row-ref state field %s", field)
		}
		assets[field] = asset
	}
	if len(assets) == 0 {
		return nil, false, nil
	}
	if len(assets) != len(columnVectorGraphRowRefStateFields) {
		missing := make([]string, 0, len(columnVectorGraphRowRefStateFields)-len(assets))
		for _, field := range columnVectorGraphRowRefStateFields {
			if _, ok := assets[field]; !ok {
				missing = append(missing, string(field))
			}
		}
		sort.Strings(missing)
		return nil, true, fmt.Errorf("collections: vector-index row-ref state missing fields %v", missing)
	}
	return assets, true, nil
}

func columnVectorGraphRowRefStatePresent(state columnVectorIndexStateSnapshot) bool {
	_, found, _ := columnVectorGraphRowRefStateAssetsByField(state)
	return found
}

func (c *Collection) openColumnVectorGraphRowRefStateSourceForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, records []columnManifestRecord) (*columnVectorGraphRowRefStateSource, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	return newColumnVectorGraphRowRefStateSourceFromRoot(c.db.ColumnAssetRootDir(), collection, cfg, def, graph, state, records)
}

func newColumnVectorGraphRowRefStateSourceFromRoot(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, records []columnManifestRecord) (*columnVectorGraphRowRefStateSource, error) {
	assets, found, err := columnVectorGraphRowRefStateAssetsByField(state)
	if err != nil || !found {
		return nil, err
	}
	if !columnVectorIndexStateDefinitionParametersMatch(&state, &def) || !columnVectorIndexStateMatchesGraph(state, graph) {
		return nil, fmt.Errorf("collections: column_graph %q row-ref state identity mismatch", def.Name)
	}
	if err := validateColumnVectorGraphRowRefStateManifestAssets(collection, cfg, def, state); err != nil {
		return nil, err
	}
	baseRows, err := columnVectorGraphRowRefBasePartRows(records, graph.BaseManifestGeneration, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	manager := mappedresource.NewManager()
	source := &columnVectorGraphRowRefStateSource{rows: state.RowCount, manager: manager}
	success := false
	defer func() {
		if !success {
			_ = source.Close()
		}
	}()
	for _, field := range columnVectorGraphRowRefStateFields {
		view, mmapDirect, err := openColumnVectorGraphRowRefStateFieldDirectView(rootDir, collection, cfg, def, state, assets[field], field, manager)
		if err != nil {
			return nil, fmt.Errorf("collections: column_graph %q row-ref state %s: %w", def.Name, field, err)
		}
		if mmapDirect {
			source.mmapDirectFields++
		}
		source.setFieldView(field, view)
	}
	for ordinal := 0; ordinal < state.RowCount; ordinal++ {
		ref, ok := source.rowRefForOrdinal(ordinal)
		if !ok {
			return nil, fmt.Errorf("collections: column_graph %q row-ref state ordinal=%d unavailable", def.Name, ordinal)
		}
		if err := validateColumnVectorGraphRowRefForState(ordinal, ref, graph.BaseManifestGeneration); err != nil {
			return nil, err
		}
		if err := validateColumnVectorGraphRowRefStateBounds(def.Name, ordinal, ref, baseRows); err != nil {
			return nil, err
		}
	}
	source.captureResourceStats()
	success = true
	return source, nil
}

func validateColumnVectorGraphRowRefStateBounds(indexName string, ordinal int, ref DocumentRowRef, baseRows map[documentRowPartKey]int) error {
	rows, ok := baseRows[documentRowPartKey{Generation: ref.Generation, PartID: ref.PartID}]
	if !ok {
		return fmt.Errorf("collections: column_graph %q row-ref state ordinal=%d generation=%d part_id=%d is not present in base manifest", indexName, ordinal, ref.Generation, ref.PartID)
	}
	if ref.RowIndex >= rows {
		return fmt.Errorf("collections: column_graph %q row-ref state ordinal=%d row_index=%d outside base rows=%d generation=%d part_id=%d", indexName, ordinal, ref.RowIndex, rows, ref.Generation, ref.PartID)
	}
	return nil
}

func columnVectorGraphRowRefBasePartRows(records []columnManifestRecord, generation uint64, namespace string) (map[documentRowPartKey]int, error) {
	physicalRefs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, generation, namespace)
	if err != nil {
		return nil, err
	}
	if mutationParts != 0 {
		return nil, errors.New("collections: column_graph row-ref state requires insert-only base physical refs")
	}
	if len(physicalRefs) == 0 {
		return nil, errors.New("collections: column_graph row-ref state missing base physical refs")
	}
	rows := make(map[documentRowPartKey]int, len(physicalRefs))
	for _, asset := range physicalRefs {
		if asset.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return nil, fmt.Errorf("collections: column_graph row-ref state physical ref kind=%q", asset.Ref.Kind)
		}
		if asset.Reason != ColumnPublishOperationInsert {
			return nil, fmt.Errorf("collections: column_graph row-ref state requires insert-only physical refs, got %s", asset.Reason)
		}
		key := documentRowPartKey{Generation: asset.Ref.Generation, PartID: asset.Ref.PartID}
		if _, exists := rows[key]; exists {
			return nil, fmt.Errorf("collections: duplicate column_graph row-ref base part generation=%d part_id=%d", key.Generation, key.PartID)
		}
		rows[key] = asset.Rows
	}
	return rows, nil
}

func (s *columnVectorGraphRowRefStateSource) setFieldView(field columnVectorGraphRowRefStateField, view typeddecode.PreparedInt64DirectView) {
	if s == nil {
		return
	}
	switch field {
	case columnVectorGraphRowRefStateFieldGeneration:
		s.generations = view
	case columnVectorGraphRowRefStateFieldPartID:
		s.partIDs = view
	case columnVectorGraphRowRefStateFieldRowIndex:
		s.rowIndexes = view
	case columnVectorGraphRowRefStateFieldAppliedCommandLSN:
		s.appliedCommandLSNs = view
	}
}

func openColumnVectorGraphRowRefStateFieldDirectView(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, field columnVectorGraphRowRefStateField, manager *mappedresource.Manager) (typeddecode.PreparedInt64DirectView, bool, error) {
	sourceCfg, adapterColumn, err := columnVectorGraphRowRefStateColumnStoreConfig(collection, cfg, def, field)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return typeddecode.PreparedInt64DirectView{}, false, fmt.Errorf("schema_hash=%d want %d", asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return typeddecode.PreparedInt64DirectView{}, false, fmt.Errorf("bytes=%d manifest=%d ref=%d", len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount || image.Rows != state.RowCount {
		return typeddecode.PreparedInt64DirectView{}, false, fmt.Errorf("image part/rows=(%d,%d) asset/state=(%d,%d)", image.PartID, image.Rows, asset.Ref.PartID, state.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	section, err := columnVectorGraphRowRefStateSection(image, adapterColumn.Definition.Name)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, fmt.Errorf("layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return typeddecode.PreparedInt64DirectView{}, false, fmt.Errorf("missing layout certification for column %q", adapterColumn.Definition.Name)
	}
	if err := validateColumnVectorGraphRowRefStateSection(adapterPart.Part, section, certColumn, adapterColumn.Definition.Name, state.RowCount); err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	handle, key, err := acquireColumnVectorGraphPreparedStateSection(rootDir, collection, columnVectorGraphRowRefStateScopeID, "column_graph row-ref state", "column_graph row-ref state "+string(field), asset.Ref, image.Version, section, page.Checksum(sectionBytes), manager)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, false, err
	}
	expectation := columnVectorGraphDirectViewExpectation(columnVectorIndexStateAssetRoleRowRefs, asset.Role, adapterColumn.Definition.Name, state.RowCount, asset.Ref)
	view, status := typeddecode.CertifyGraphInt64DirectView(typeddecode.GraphInt64DirectViewRequest{
		Expectation:   expectation,
		Certification: certColumn,
		Section:       section,
		ExpectedKey:   key,
		Handle:        handle,
		Manager:       manager,
	})
	if status.Direct() {
		return view, true, nil
	}
	view, fallbackErr := columnVectorGraphRowRefStatePreparedViewFromFallbackHandle(expectation, manager, handle, state.RowCount, status)
	if fallbackErr == nil {
		return view, false, nil
	}
	releaseErr := handle.Release()
	return typeddecode.PreparedInt64DirectView{}, false, errors.Join(fallbackErr, releaseErr)
}

func columnVectorGraphRowRefStatePreparedViewFromFallbackHandle(expectation typeddecode.GraphDirectViewExpectation, manager *mappedresource.Manager, handle *mappedresource.Handle, rows int, directStatus typeddecode.Status) (typeddecode.PreparedInt64DirectView, error) {
	if !columnVectorGraphPreparedStateDirectFallbackAllowed(directStatus) {
		return typeddecode.PreparedInt64DirectView{}, fmt.Errorf("direct-view certification failed: %s", directStatus.String())
	}
	values, status := typeddecode.Int64View(manager, handle, typeddecode.ResourceViewOptions{ExpectedElements: rows, RequireMapped: false})
	if status.Direct() {
		return typeddecode.PreparedInt64DirectView{Expectation: expectation, Rows: rows, Values: values, Handle: handle}, nil
	}
	if !columnVectorGraphPreparedStateDirectFallbackAllowed(status) {
		return typeddecode.PreparedInt64DirectView{}, errors.Join(fmt.Errorf("direct-view certification failed: %s", directStatus.String()), fmt.Errorf("heap typed-view fallback failed: %s", status.String()))
	}
	decoded, err := decodeColumnVectorGraphRowRefStateInt64Values(handle.Bytes(), rows)
	if err != nil {
		return typeddecode.PreparedInt64DirectView{}, errors.Join(fmt.Errorf("direct-view certification failed: %s", directStatus.String()), fmt.Errorf("heap typed-view fallback failed: %s", status.String()), err)
	}
	return typeddecode.PreparedInt64DirectView{Expectation: expectation, Rows: rows, Values: decoded, Handle: handle}, nil
}

func columnVectorGraphRowRefFromPreparedValues(ordinal int, generation, partID, rowIndex, appliedLSN int64) (DocumentRowRef, error) {
	if generation <= 0 || partID <= 0 || appliedLSN <= 0 {
		return DocumentRowRef{}, fmt.Errorf("collections: column_graph row-ref state ordinal=%d has non-positive coordinate generation=%d part_id=%d applied_lsn=%d", ordinal, generation, partID, appliedLSN)
	}
	if rowIndex < 0 || rowIndex > int64(math.MaxInt) {
		return DocumentRowRef{}, fmt.Errorf("collections: column_graph row-ref state ordinal=%d row_index=%d outside int range", ordinal, rowIndex)
	}
	return DocumentRowRef{Generation: uint64(generation), PartID: uint64(partID), RowIndex: int(rowIndex), AppliedCommandLSN: uint64(appliedLSN)}, nil
}

func columnVectorGraphRowRefFromStateValues(values map[columnVectorGraphRowRefStateField][]int64, ordinal int) (DocumentRowRef, error) {
	get := func(field columnVectorGraphRowRefStateField) (int64, error) {
		column := values[field]
		if ordinal < 0 || ordinal >= len(column) {
			return 0, fmt.Errorf("collections: column_graph row-ref state ordinal=%d outside field %s rows=%d", ordinal, field, len(column))
		}
		return column[ordinal], nil
	}
	generation, err := get(columnVectorGraphRowRefStateFieldGeneration)
	if err != nil {
		return DocumentRowRef{}, err
	}
	partID, err := get(columnVectorGraphRowRefStateFieldPartID)
	if err != nil {
		return DocumentRowRef{}, err
	}
	rowIndex, err := get(columnVectorGraphRowRefStateFieldRowIndex)
	if err != nil {
		return DocumentRowRef{}, err
	}
	appliedLSN, err := get(columnVectorGraphRowRefStateFieldAppliedCommandLSN)
	if err != nil {
		return DocumentRowRef{}, err
	}
	if generation <= 0 || partID <= 0 || appliedLSN <= 0 {
		return DocumentRowRef{}, fmt.Errorf("collections: column_graph row-ref state ordinal=%d has non-positive coordinate generation=%d part_id=%d applied_lsn=%d", ordinal, generation, partID, appliedLSN)
	}
	if rowIndex < 0 || rowIndex > int64(math.MaxInt) {
		return DocumentRowRef{}, fmt.Errorf("collections: column_graph row-ref state ordinal=%d row_index=%d outside int range", ordinal, rowIndex)
	}
	return DocumentRowRef{Generation: uint64(generation), PartID: uint64(partID), RowIndex: int(rowIndex), AppliedCommandLSN: uint64(appliedLSN)}, nil
}

func loadColumnVectorGraphRowRefStateValuesInto(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, field columnVectorGraphRowRefStateField, rawScratch []byte) ([]int64, []byte, error) {
	sourceCfg, adapterColumn, err := columnVectorGraphRowRefStateColumnStoreConfig(collection, cfg, def, field)
	if err != nil {
		return nil, rawScratch, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, rawScratch, fmt.Errorf("schema_hash=%d want %d", asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return nil, rawScratch, err
	}
	raw, err := readColumnPhysicalAssetFromManagerInto(rootDir, asset.Ref, rawScratch)
	if err != nil {
		return nil, rawScratch, err
	}
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return nil, raw, fmt.Errorf("bytes=%d manifest=%d ref=%d", len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, raw, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount || image.Rows != state.RowCount {
		return nil, raw, fmt.Errorf("image part/rows=(%d,%d) asset/state=(%d,%d)", image.PartID, image.Rows, asset.Ref.PartID, state.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return nil, raw, err
	}
	section, err := columnVectorGraphRowRefStateSection(image, adapterColumn.Definition.Name)
	if err != nil {
		return nil, raw, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, raw, fmt.Errorf("layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, raw, fmt.Errorf("missing layout certification for column %q", adapterColumn.Definition.Name)
	}
	if err := validateColumnVectorGraphRowRefStateSection(adapterPart.Part, section, certColumn, adapterColumn.Definition.Name, state.RowCount); err != nil {
		return nil, raw, err
	}
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		return nil, raw, err
	}
	values, err := decodeColumnVectorGraphRowRefStateInt64Values(sectionBytes, state.RowCount)
	if err != nil {
		return nil, raw, err
	}
	return values, raw, nil
}

func columnVectorGraphRowRefStateSection(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, error) {
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section, nil
		}
	}
	return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("collections: column_graph row-ref state missing column data section %q", column)
}

func validateColumnVectorGraphRowRefStateSection(part *typedcolumn.ColumnPart, section typedcolumn.ColumnPartImageSection, certColumn typedcolumn.ColumnPartLayoutContractColumn, columnName string, rows int) error {
	if part == nil {
		return errors.New("collections: column_graph row-ref state nil typed_column_part")
	}
	if rows <= 0 {
		return fmt.Errorf("collections: column_graph row-ref state rows=%d", rows)
	}
	if part.Descriptor.RowCount != rows {
		return fmt.Errorf("collections: column_graph row-ref state descriptor rows=%d want %d", part.Descriptor.RowCount, rows)
	}
	column, ok := part.Columns[columnName]
	if !ok {
		return fmt.Errorf("collections: column_graph row-ref state missing column %q", columnName)
	}
	if column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 || column.Definition.Compression != typedcolumn.CompressionNone || column.Definition.FixedWidthElements != 0 {
		return fmt.Errorf("collections: column_graph row-ref state column %q schema mismatch type=%s encoding=%s compression=%s fixed_width_elements=%d", columnName, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression, column.Definition.FixedWidthElements)
	}
	wantBytes, err := columnVectorGraphRowRefStateByteLen(rows)
	if err != nil {
		return err
	}
	if section.Kind != typedcolumn.ColumnPartImageSectionColumnData || section.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumns || section.Column != columnName {
		return fmt.Errorf("collections: column_graph row-ref state section identity mismatch kind=%s category=%s column=%q", section.Kind, section.Category, section.Column)
	}
	if section.Encoding != typedcolumn.EncodingRawInt64 || section.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: column_graph row-ref state section encoding=%s compression=%s", section.Encoding, section.Compression)
	}
	if section.Rows != rows || section.Length != wantBytes {
		return fmt.Errorf("collections: column_graph row-ref state section rows=%d length=%d want rows=%d length=%d", section.Rows, section.Length, rows, wantBytes)
	}
	if certColumn.LogicalType != columnVectorIndexStateLogicalTypeInt64 || certColumn.Type != typedcolumn.ColumnTypeInt64 || certColumn.Encoding != typedcolumn.EncodingRawInt64 || certColumn.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: column_graph row-ref state logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", certColumn.LogicalType, certColumn.Type, certColumn.Encoding, columnVectorIndexStateLogicalTypeInt64, typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64)
	}
	nextRow := 0
	totalBytes := 0
	for i, block := range column.Blocks {
		if block.Descriptor.FirstRow != nextRow {
			return fmt.Errorf("collections: column_graph row-ref state block %d first_row=%d want %d", i, block.Descriptor.FirstRow, nextRow)
		}
		blockBytes, err := columnVectorGraphRowRefStateByteLen(block.Descriptor.RowCount)
		if err != nil {
			return err
		}
		if block.Descriptor.Encoding != typedcolumn.EncodingRawInt64 || block.Descriptor.Compression != typedcolumn.CompressionNone || block.Descriptor.RawBytes != blockBytes || block.Descriptor.StoredBytes != blockBytes {
			return fmt.Errorf("collections: column_graph row-ref state block %d descriptor mismatch encoding=%s compression=%s raw=%d stored=%d want_bytes=%d", i, block.Descriptor.Encoding, block.Descriptor.Compression, block.Descriptor.RawBytes, block.Descriptor.StoredBytes, blockBytes)
		}
		g := block.Granule
		if g.Encoding != typedcolumn.EncodingRawInt64 || g.Compression != typedcolumn.CompressionNone || g.Rows != block.Descriptor.RowCount || g.RawBytes != blockBytes || g.StoredBytes != blockBytes || len(g.Payload) != blockBytes || g.NullCount != 0 || g.DefaultCount != 0 {
			return fmt.Errorf("collections: column_graph row-ref state block %d granule mismatch", i)
		}
		nextRow += block.Descriptor.RowCount
		totalBytes += blockBytes
	}
	if nextRow != rows || totalBytes != section.Length {
		return fmt.Errorf("collections: column_graph row-ref state blocks rows=%d bytes=%d want rows=%d bytes=%d", nextRow, totalBytes, rows, section.Length)
	}
	return nil
}

func columnVectorGraphRowRefStateByteLen(rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("collections: column_graph row-ref state invalid rows=%d", rows)
	}
	if rows > math.MaxInt/8 {
		return 0, errors.New("collections: column_graph row-ref state bytes overflow")
	}
	return rows * 8, nil
}

func decodeColumnVectorGraphRowRefStateInt64Values(raw []byte, rows int) ([]int64, error) {
	want, err := columnVectorGraphRowRefStateByteLen(rows)
	if err != nil {
		return nil, err
	}
	if len(raw) != want {
		return nil, fmt.Errorf("collections: column_graph row-ref state raw bytes=%d want %d", len(raw), want)
	}
	values := make([]int64, rows)
	for i := 0; i < rows; i++ {
		values[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
	}
	return values, nil
}

func (r *columnVectorGraphPhysicalRowReader) rowRefForOrdinal(ordinal int) (DocumentRowRef, bool) {
	if r == nil || r.rowRefSource == nil {
		return DocumentRowRef{}, false
	}
	return r.rowRefSource.rowRefForOrdinal(ordinal)
}

func (s *columnVectorGraphRowRefStateSource) rowRefForOrdinal(ordinal int) (DocumentRowRef, bool) {
	if s == nil || s.closed || !s.preparedViewActive() || ordinal < 0 || ordinal >= s.rows {
		return DocumentRowRef{}, false
	}
	generation, ok := s.generations.Value(ordinal)
	if !ok {
		return DocumentRowRef{}, false
	}
	partID, ok := s.partIDs.Value(ordinal)
	if !ok {
		return DocumentRowRef{}, false
	}
	rowIndex, ok := s.rowIndexes.Value(ordinal)
	if !ok {
		return DocumentRowRef{}, false
	}
	appliedLSN, ok := s.appliedCommandLSNs.Value(ordinal)
	if !ok {
		return DocumentRowRef{}, false
	}
	ref, err := columnVectorGraphRowRefFromPreparedValues(ordinal, generation, partID, rowIndex, appliedLSN)
	if err != nil {
		return DocumentRowRef{}, false
	}
	return ref, true
}

func (s *columnVectorGraphRowRefStateSource) rowRefs() ([]DocumentRowRef, bool) {
	if s == nil || s.closed || !s.preparedViewActive() {
		return nil, false
	}
	refs := make([]DocumentRowRef, s.rows)
	for ordinal := range refs {
		ref, ok := s.rowRefForOrdinal(ordinal)
		if !ok {
			return nil, false
		}
		refs[ordinal] = ref
	}
	return refs, true
}

func (s *columnVectorGraphRowRefStateSource) preparedViewActive() bool {
	return s != nil && !s.closed && s.generations.Alive() && s.partIDs.Alive() && s.rowIndexes.Alive() && s.appliedCommandLSNs.Alive() && s.generations.Rows == s.rows && s.partIDs.Rows == s.rows && s.rowIndexes.Rows == s.rows && s.appliedCommandLSNs.Rows == s.rows
}

func (s *columnVectorGraphRowRefStateSource) mmapDirectFieldCount() uint64 {
	if !s.preparedViewActive() {
		return 0
	}
	return s.mmapDirectFields
}

func (s *columnVectorGraphRowRefStateSource) captureResourceStats() {
	if s == nil || s.manager == nil {
		return
	}
	stats := s.manager.Stats()
	if stats.ActiveMappedBytes > 0 {
		s.mappedBytes = uint64(stats.ActiveMappedBytes)
	}
	s.activeHandles = stats.ActiveHandles
	for _, count := range stats.DeniedByReason {
		s.deniedResources += count
	}
}

func (s *columnVectorGraphRowRefStateSource) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	closeErr := errors.Join(s.generations.Close(), s.partIDs.Close(), s.rowIndexes.Close(), s.appliedCommandLSNs.Close())
	s.rows = 0
	s.manager = nil
	s.mmapDirectFields = 0
	s.mappedBytes = 0
	s.activeHandles = 0
	return closeErr
}

func (c *Collection) assignColumnVectorGraphRowRefsFromBaseManifest(collection string, cfg ColumnStoreConfig, records []columnManifestRecord, generation uint64, rows []columnVectorGraphAssetRow) error {
	if len(rows) == 0 {
		return nil
	}
	physicalRefs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, generation, cfg.AssetManager.Namespace)
	if err != nil {
		return err
	}
	if mutationParts != 0 {
		return errors.New("collections: column_graph row-ref state requires insert-only base physical refs")
	}
	locations, _, err := c.columnVectorGraphTypedColumnPhysicalLocations(collection, cfg, physicalRefs)
	if err != nil {
		return err
	}
	for ordinal := range rows {
		loc, ok := locations[string(rows[ordinal].ID)]
		if !ok {
			return fmt.Errorf("collections: column_graph row-ref state missing base physical row for ordinal=%d id=%q", ordinal, string(rows[ordinal].ID))
		}
		rows[ordinal].BaseRowRef = DocumentRowRef{
			Generation:        loc.generation,
			PartID:            loc.partID,
			RowIndex:          loc.rowIndex,
			AppliedCommandLSN: loc.appliedCommandLSN,
		}
		if err := validateColumnVectorGraphRowRefForState(ordinal, rows[ordinal].BaseRowRef, generation); err != nil {
			return err
		}
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) populateRowRefStateSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil {
		return
	}
	if r.rowRefSource != nil {
		if r.rowRefSource.preparedViewActive() {
			stats.RowRefStatePreparedViews = 1
			stats.RowRefStateMmapDirectFields = r.rowRefSource.mmapDirectFieldCount()
		}
		return
	}
	if r.rowRefStateUnavailable || r.rowRefStateFallbackReason != "" {
		stats.RowRefStateSourceUnavailable = 1
		stats.RowRefStateSourceFallbacks = 1
	}
}
