package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

const (
	columnVectorGraphDocumentIDStateAssetID    = "document_ids"
	columnVectorGraphDocumentIDStateColumnName = "document_ids"
	columnVectorGraphDocumentIDStatePathSuffix = "_document_ids"
)

type columnVectorGraphPreparedDocumentIDStateAsset struct {
	Present      bool
	Config       ColumnStoreConfig
	Ref          ColumnAssetRef
	Bytes        int64
	Rows         int
	OffsetsBytes int64
	ValuesBytes  int64
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphPreparedDocumentIDStatePayload struct {
	Config       ColumnStoreConfig
	PartID       uint64
	Payload      []byte
	Bytes        int64
	Rows         int
	OffsetsBytes int64
	ValuesBytes  int64
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphDocumentIDStateSource struct {
	rows   int
	ids    typedcolumn.RawBytesOffsets
	closed bool
}

func prepareColumnVectorGraphDocumentIDStateAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedDocumentIDStateAsset, error) {
	payload, err := prepareColumnVectorGraphDocumentIDStatePayload(collection, base, def, partID, rows)
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, err
	}
	if assetRootDir == "" {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, errors.New("collections: column_graph document-id state requires asset root dir")
	}
	if generation == 0 || partID == 0 {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, errors.New("collections: column_graph document-id state requires generation and part_id")
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(assetRootDir, payload.Config)
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, err
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, payload.Config)
	ref, appendErr := appender.appendKindWithAlignment(payload.Payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, alignment)
	closeErr := appender.close()
	if appendErr != nil {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, errors.Join(appendErr, closeErr)
	}
	if closeErr != nil {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, closeErr
	}
	if err := validateColumnVectorGraphPreparedDocumentIDStateRef(payload, ref, generation); err != nil {
		return columnVectorGraphPreparedDocumentIDStateAsset{}, err
	}
	return columnVectorGraphPreparedDocumentIDStateAsset{
		Present:      true,
		Config:       payload.Config,
		Ref:          ref,
		Bytes:        ref.Length,
		Rows:         payload.Rows,
		OffsetsBytes: payload.OffsetsBytes,
		ValuesBytes:  payload.ValuesBytes,
		PaddingBytes: payload.PaddingBytes,
		SchemaHash:   payload.SchemaHash,
	}, nil
}

func prepareColumnVectorGraphDocumentIDStatePayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedDocumentIDStatePayload, error) {
	if partID == 0 {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, errors.New("collections: column_graph document-id state requires non-zero part_id")
	}
	sourceCfg, adapterColumn, err := columnVectorGraphDocumentIDStateColumnStoreConfig(collection, base, def)
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, err
	}
	ids, err := buildColumnVectorGraphDocumentIDStateBytes(rows)
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, err
	}
	primaryIDs := make([]int64, len(rows))
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
		Rows: len(rows),
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
		},
		BytesColumns: map[string]typedcolumn.RawBytesOffsets{
			adapterColumn.Definition.Name: ids,
		},
	})
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, err
	}
	logicalTypes := make(map[string]string, 1)
	if logical, ok := columnStoreSemanticLogicalType(ColumnStoreValueBytes); ok {
		logicalTypes[adapterColumn.Definition.Name] = string(logical)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}), LayoutLogicalTypes: logicalTypes})
	if err != nil {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, err
	}
	if image.Rows != len(rows) || image.PartID != partID {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, fmt.Errorf("collections: column_graph document-id state image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, len(rows), partID)
	}
	accounting := part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes <= 0 {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, errors.New("collections: column_graph document-id state missing offsets bytes")
	}
	if accounting.DeclaredColumnValuesBytes < 0 {
		return columnVectorGraphPreparedDocumentIDStatePayload{}, errors.New("collections: column_graph document-id state invalid values bytes")
	}
	return columnVectorGraphPreparedDocumentIDStatePayload{
		Config:       sourceCfg,
		PartID:       partID,
		Payload:      image.Bytes,
		Bytes:        int64(len(image.Bytes)),
		Rows:         image.Rows,
		OffsetsBytes: int64(accounting.DeclaredColumnOffsetsBytes),
		ValuesBytes:  int64(accounting.DeclaredColumnValuesBytes),
		PaddingBytes: int64(accounting.SerializedPaddingBytes),
		SchemaHash:   sourceCfg.SchemaHash,
	}, nil
}

func buildColumnVectorGraphDocumentIDStateBytes(rows []columnVectorGraphAssetRow) (typedcolumn.RawBytesOffsets, error) {
	offsets := make([]uint64, len(rows)+1)
	values := make([]byte, 0)
	for ordinal, row := range rows {
		if len(row.ID) == 0 {
			return typedcolumn.RawBytesOffsets{}, fmt.Errorf("collections: column_graph document-id state ordinal=%d missing document id", ordinal)
		}
		if len(row.ID) > math.MaxInt-len(values) {
			return typedcolumn.RawBytesOffsets{}, errors.New("collections: column_graph document-id state values overflow")
		}
		values = append(values, row.ID...)
		offsets[ordinal+1] = uint64(len(values))
	}
	ids := typedcolumn.RawBytesOffsets{Rows: len(rows), Offsets: offsets, Values: values}
	if err := ids.Validate(); err != nil {
		return typedcolumn.RawBytesOffsets{}, err
	}
	return ids, nil
}

func columnVectorGraphDocumentIDStateColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if normalizedDef.Strategy != VectorIndexStrategyColumnGraph {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", normalizedDef.Name, normalizedDef.Strategy)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph document-id state requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph document-id state requires base asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:      columnVectorGraphDocumentIDStateColumnName,
			Path:      normalizedDef.Field + columnVectorGraphDocumentIDStatePathSuffix,
			Owner:     TypedStorageOwnerColumnPart,
			ValueType: ColumnStoreValueBytes,
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
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph document-id state fields=%d want 1", len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || !typedColumnAdapterBytesDirectPayloadSupported(columns[0]) {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph document-id state is not generic bytes raw_bytes_offsets")
	}
	return *cfg, columns[0], nil
}

func validateColumnVectorGraphPreparedDocumentIDStateRef(payload columnVectorGraphPreparedDocumentIDStatePayload, ref ColumnAssetRef, generation uint64) error {
	if ref.Namespace != payload.Config.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != payload.PartID || ref.Length != payload.Bytes {
		return fmt.Errorf("collections: invalid column_graph document-id state asset ref %+v", ref)
	}
	if ref.Offset%8 != 0 {
		return fmt.Errorf("collections: column_graph document-id state absolute asset offset=%d is not 8-byte aligned", ref.Offset)
	}
	return nil
}

func columnVectorGraphDocumentIDStateAssetSnapshot(prepared columnVectorGraphPreparedDocumentIDStateAsset) (columnVectorIndexStateAssetSnapshot, bool) {
	if !prepared.Present {
		return columnVectorIndexStateAssetSnapshot{}, false
	}
	return columnVectorIndexStateAssetSnapshot{
		Role:             columnVectorIndexStateAssetRoleDocumentIDs,
		AssetID:          columnVectorGraphDocumentIDStateAssetID,
		LogicalType:      columnVectorIndexStateLogicalTypeBytes,
		PhysicalEncoding: columnVectorIndexStateEncodingRawBytesOffsets,
		RowCount:         prepared.Rows,
		SourceSchemaHash: prepared.SchemaHash,
		Ref:              prepared.Ref,
		AssetBytes:       prepared.Bytes,
	}, true
}

func findColumnVectorGraphDocumentIDStateAsset(state columnVectorIndexStateSnapshot) (columnVectorIndexStateAssetSnapshot, bool, error) {
	var found columnVectorIndexStateAssetSnapshot
	seen := false
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleDocumentIDs {
			continue
		}
		if asset.AssetID != columnVectorGraphDocumentIDStateAssetID {
			return columnVectorIndexStateAssetSnapshot{}, true, fmt.Errorf("collections: vector-index document-id state asset id %q is not %q", asset.AssetID, columnVectorGraphDocumentIDStateAssetID)
		}
		if seen {
			return columnVectorIndexStateAssetSnapshot{}, true, errors.New("collections: duplicate vector-index document-id state asset")
		}
		found = asset
		seen = true
	}
	return found, seen, nil
}

func columnVectorGraphDocumentIDStatePresent(state columnVectorIndexStateSnapshot) bool {
	_, found, _ := findColumnVectorGraphDocumentIDStateAsset(state)
	return found
}

func validateColumnVectorGraphDocumentIDStateManifestAsset(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot) error {
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil || !found {
		return err
	}
	sourceCfg, _, err := columnVectorGraphDocumentIDStateColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return fmt.Errorf("collections: vector-index document-id state schema_hash=%d want %d", asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	return nil
}

func validateColumnVectorGraphDocumentIDStateAssetPayload(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) error {
	source, _, err := newColumnVectorGraphDocumentIDStateSourceFromRoot(rootDir, collection, cfg, def, graph, state)
	if source != nil {
		_ = source.Close()
	}
	return err
}

func (c *Collection) openColumnVectorGraphDocumentIDStateSourceForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphDocumentIDStateSource, typeddecode.Reason, error) {
	if c == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionNil
	}
	if c.db == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionDBNil
	}
	return newColumnVectorGraphDocumentIDStateSourceFromRoot(c.db.ColumnAssetRootDir(), collection, cfg, def, graph, state)
}

func newColumnVectorGraphDocumentIDStateSourceFromRoot(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphDocumentIDStateSource, typeddecode.Reason, error) {
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if !found {
		return nil, "", nil
	}
	if !columnVectorIndexStateDefinitionParametersMatch(&state, &def) || !columnVectorIndexStateMatchesGraph(state, graph) {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state identity mismatch", def.Name)
	}
	if asset.LogicalType != columnVectorIndexStateLogicalTypeBytes || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawBytesOffsets {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state type/encoding=(%q,%q) want (%q,%q)", def.Name, asset.LogicalType, asset.PhysicalEncoding, columnVectorIndexStateLogicalTypeBytes, columnVectorIndexStateEncodingRawBytesOffsets)
	}
	if asset.RowCount != graph.RowCount || asset.Ref.Generation != graph.BaseManifestGeneration || asset.Ref.Namespace != cfg.AssetManager.Namespace || asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart || asset.AssetBytes != asset.Ref.Length {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state asset identity mismatch", def.Name)
	}
	sourceCfg, adapterColumn, err := columnVectorGraphDocumentIDStateColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state schema_hash=%d want %d", def.Name, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorGraphAssetRefAvailable(rootDir, asset.Ref); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	ids, reason, err := decodeColumnVectorGraphDocumentIDStateFromRaw(def, sourceCfg, adapterColumn, state, asset, raw)
	if err != nil {
		return nil, reason, err
	}
	return &columnVectorGraphDocumentIDStateSource{rows: asset.RowCount, ids: ids}, "", nil
}

func decodeColumnVectorGraphDocumentIDStateFromRaw(def VectorIndexDefinition, sourceCfg ColumnStoreConfig, adapterColumn typedColumnAdapterColumn, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, raw []byte) (typedcolumn.RawBytesOffsets, typeddecode.Reason, error) {
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph %q document-id state bytes=%d manifest=%d ref=%d", def.Name, len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount || image.Rows != state.RowCount {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonRowCountMismatch, fmt.Errorf("collections: column_graph %q document-id state image part/rows=(%d,%d) asset/state=(%d,%d)", def.Name, image.PartID, image.Rows, asset.Ref.PartID, state.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(sourceCfg.SchemaHash) {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state schema_version=%d want %d", def.Name, adapterPart.Part.Descriptor.SchemaVersion, uint32(sourceCfg.SchemaHash))
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state missing offsets/value sections for column %q", def.Name, adapterColumn.Definition.Name)
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	if err := typedcolumn.ValidateRawBytesOffsetsSections(offsetsSection, valuesSection, offsetsRaw, valuesRaw, state.RowCount); err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state layout certification: %w", def.Name, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state missing layout certification for column %q", def.Name, adapterColumn.Definition.Name)
	}
	if certColumn.LogicalType != columnVectorIndexStateLogicalTypeBytes || certColumn.Type != typedcolumn.ColumnTypeBytes || certColumn.Encoding != typedcolumn.EncodingRawBytesOffsets || certColumn.Compression != typedcolumn.CompressionNone {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", def.Name, certColumn.LogicalType, certColumn.Type, certColumn.Encoding, columnVectorIndexStateLogicalTypeBytes, typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets)
	}
	plan := typeddecode.BytesPlan(certColumn)
	status := typeddecode.ValidateBytesDirectViewSections(typeddecode.BytesDirectViewRequest{
		Plan:           plan,
		Certification:  certColumn,
		Rows:           state.RowCount,
		OffsetsBytes:   offsetsSection.Length,
		ValuesBytes:    valuesSection.Length,
		AssetOffset:    asset.Ref.Offset,
		HasAssetOffset: true,
	})
	if !status.Direct() {
		return typedcolumn.RawBytesOffsets{}, status.Reason, fmt.Errorf("collections: column_graph %q document-id state direct-view section validation failed: %s", def.Name, status.String())
	}
	ids, err := typedcolumn.DecodeRawBytesOffsetsFallback(nil, nil, offsetsRaw, valuesRaw, state.RowCount)
	if err != nil {
		return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
	}
	for ordinal := 0; ordinal < ids.Rows; ordinal++ {
		id, err := ids.Row(ordinal)
		if err != nil {
			return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, err
		}
		if len(id) == 0 {
			return typedcolumn.RawBytesOffsets{}, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q document-id state ordinal=%d missing document id", def.Name, ordinal)
		}
	}
	return ids, "", nil
}

func (r *columnVectorGraphPhysicalRowReader) documentIDForOrdinal(ordinal int) ([]byte, bool) {
	if r == nil || r.documentIDSource == nil {
		return nil, false
	}
	return r.documentIDSource.documentIDForOrdinal(ordinal)
}

func (s *columnVectorGraphDocumentIDStateSource) documentIDForOrdinal(ordinal int) ([]byte, bool) {
	if s == nil || s.closed || ordinal < 0 || ordinal >= s.rows || ordinal >= s.ids.Rows {
		return nil, false
	}
	id, err := s.ids.Row(ordinal)
	if err != nil || len(id) == 0 {
		return nil, false
	}
	return id, true
}

func (s *columnVectorGraphDocumentIDStateSource) documentIDs() (typedcolumn.RawBytesOffsets, bool) {
	if s == nil || s.closed || s.ids.Rows != s.rows {
		return typedcolumn.RawBytesOffsets{}, false
	}
	return s.ids, true
}

func (s *columnVectorGraphDocumentIDStateSource) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	s.rows = 0
	s.ids = typedcolumn.RawBytesOffsets{}
	return nil
}
