package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

const (
	columnVectorGraphLayer0AdjacencySourceSchema     = "column_graph_layer0_adjacency/raw_uint32_offsets_list/v1"
	columnVectorGraphLayer0AdjacencySourceColumnName = "layer0_adjacency"
	columnVectorGraphLayer0AdjacencySourcePathSuffix = "_layer0_neighbors"
)

type columnVectorGraphPreparedLayer0AdjacencySource struct {
	Present      bool
	Config       ColumnStoreConfig
	Ref          ColumnAssetRef
	Bytes        int64
	Rows         int
	Values       int
	OffsetsBytes int64
	ValuesBytes  int64
	PaddingBytes int64
	SchemaHash   uint64
}

func columnVectorGraphLayer0AdjacencySourceColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph layer-0 adjacency source requires column asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:            columnVectorGraphLayer0AdjacencySourceColumnName,
			Path:            def.Field + columnVectorGraphLayer0AdjacencySourcePathSuffix,
			Owner:           TypedStorageOwnerColumnPart,
			ValueType:       ColumnStoreValueAdjacencyList,
			AdjacencyLayout: ColumnAdjacencyListLayoutUint32OffsetsList,
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
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph layer-0 adjacency source fields=%d want 1", len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || !typedColumnAdapterOffsetsListAdjacencyDirectPayloadSupported(columns[0]) {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph layer-0 adjacency source is not raw_uint32_offsets_list adjacency")
	}
	return *cfg, columns[0], nil
}

func prepareColumnVectorGraphLayer0AdjacencySourceAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedLayer0AdjacencySource, error) {
	if assetRootDir == "" {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, errors.New("collections: column_graph layer-0 adjacency source requires asset root dir")
	}
	if generation == 0 || partID == 0 {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, errors.New("collections: column_graph layer-0 adjacency source requires generation and part_id")
	}
	sourceCfg, _, err := columnVectorGraphLayer0AdjacencySourceColumnStoreConfig(collection, base, def)
	if err != nil {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, err
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterRows := make([]typedColumnAdapterRow, len(rows))
	valuesCount := 0
	for rowIdx := range rows {
		layer0, err := columnVectorGraphAdjacencyLayer(rows[rowIdx].Adjacency, 0)
		if err != nil {
			return columnVectorGraphPreparedLayer0AdjacencySource{}, fmt.Errorf("collections: column_graph layer-0 adjacency row %d: %w", rowIdx, err)
		}
		if len(layer0) > math.MaxInt-valuesCount {
			return columnVectorGraphPreparedLayer0AdjacencySource{}, errors.New("collections: column_graph layer-0 adjacency values overflow int")
		}
		valuesCount += len(layer0)
		adapterRows[rowIdx] = typedColumnAdapterRow{
			PrimaryID: int64(rowIdx),
			Values: map[string]columnDeclaredValue{
				fields[0].Path: {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: append([]uint32(nil), layer0...)},
			},
		}
	}
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{
		Collection:    collection,
		Namespace:     sourceCfg.AssetManager.Namespace,
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		PartID:        partID,
		Fields:        fields,
	}, adapterRows)
	if err != nil {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, err
	}
	image, err := part.buildImage()
	if err != nil {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, err
	}
	if image.Rows != len(rows) || image.PartID != partID {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, fmt.Errorf("collections: column_graph layer-0 adjacency source image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, len(rows), partID)
	}
	accounting := part.Part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes <= 0 {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, errors.New("collections: column_graph layer-0 adjacency source missing offsets bytes")
	}
	if accounting.DeclaredColumnValuesBytes < 0 {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, errors.New("collections: column_graph layer-0 adjacency source invalid values bytes")
	}
	ref, err := writeColumnVectorGraphLayer0AdjacencySourceAssetToManager(assetRootDir, sourceCfg, image.Bytes, generation, partID)
	if err != nil {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, err
	}
	if ref.Namespace != sourceCfg.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != partID || ref.Length != int64(len(image.Bytes)) {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, fmt.Errorf("collections: invalid column_graph layer-0 adjacency source asset ref %+v", ref)
	}
	if ref.Offset%8 != 0 {
		return columnVectorGraphPreparedLayer0AdjacencySource{}, fmt.Errorf("collections: column_graph layer-0 adjacency source absolute asset offset=%d is not 8-byte aligned", ref.Offset)
	}
	return columnVectorGraphPreparedLayer0AdjacencySource{
		Present:      true,
		Config:       sourceCfg,
		Ref:          ref,
		Bytes:        ref.Length,
		Rows:         image.Rows,
		Values:       valuesCount,
		OffsetsBytes: int64(accounting.DeclaredColumnOffsetsBytes),
		ValuesBytes:  int64(accounting.DeclaredColumnValuesBytes),
		PaddingBytes: int64(accounting.SerializedPaddingBytes),
		SchemaHash:   sourceCfg.SchemaHash,
	}, nil
}

func writeColumnVectorGraphLayer0AdjacencySourceAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	if len(payload) == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph layer-0 adjacency source payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph layer-0 adjacency source append requires generation and part_id")
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(rootDir, cfg)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	ref, appendErr := appender.appendKind(payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID)
	closeErr := appender.close()
	if appendErr != nil {
		return ColumnAssetRef{}, errors.Join(appendErr, closeErr)
	}
	return ref, closeErr
}

func validateColumnVectorGraphLayer0AdjacencySourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) error {
	if !graph.Layer0AdjacencySource.Present {
		return nil
	}
	_, _, err := decodeColumnVectorGraphLayer0AdjacencySourceAsset(rootDir, collection, cfg, def, graph)
	return err
}

func decodeColumnVectorGraphLayer0AdjacencySourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) (typedcolumn.RawUint32OffsetsList, typedcolumn.ColumnPartImage, error) {
	source := graph.Layer0AdjacencySource
	if !source.Present {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, errors.New("collections: column_graph layer-0 adjacency source is absent")
	}
	sourceCfg, adapterColumn, err := columnVectorGraphLayer0AdjacencySourceColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if err := validateColumnVectorGraphLayer0AdjacencySourceMatchesGraph(graph, sourceCfg); err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if err := validateColumnVectorGraphAssetRefAvailable(rootDir, source.Ref); err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, source.Ref)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if int64(len(raw)) != source.AssetBytes || int64(len(raw)) != source.Ref.Length {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source bytes=%d manifest=%d ref=%d", len(raw), source.AssetBytes, source.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if image.PartID != source.Ref.PartID || image.Rows != source.RowCount {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source image part/rows=(%d,%d) want (%d,%d)", image.PartID, image.Rows, source.Ref.PartID, source.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source missing layout certification for column %q", adapterColumn.Definition.Name)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source missing offsets-list sections for column %q", adapterColumn.Definition.Name)
	}
	if int64(offsetsSection.Length) != source.OffsetsBytes || int64(valuesSection.Length) != source.ValuesBytes {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source section bytes offsets=%d/%d values=%d/%d", offsetsSection.Length, source.OffsetsBytes, valuesSection.Length, source.ValuesBytes)
	}
	plan := typeddecode.AdjacencyOffsetsListPlan(certColumn)
	status := typeddecode.ValidateUint32OffsetsListDirectViewSections(typeddecode.Uint32OffsetsListDirectViewRequest{
		Plan:           plan,
		Certification:  certColumn,
		Rows:           source.RowCount,
		OffsetsBytes:   offsetsSection.Length,
		ValuesBytes:    valuesSection.Length,
		AssetOffset:    source.Ref.Offset,
		HasAssetOffset: true,
	})
	if !status.Direct() {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source direct-view section validation failed: %s", status.String())
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	list, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, source.RowCount)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if len(list.Values) != source.ValuesCount {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph layer-0 adjacency source values=%d want %d", len(list.Values), source.ValuesCount)
	}
	decoded, err := adapterPart.Part.Uint32OffsetsListColumn(adapterColumn.Definition.Name, nil, nil)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if decoded.Rows != list.Rows || !uint64SlicesEqual(decoded.Offsets, list.Offsets) || !columnVectorGraphUint32SlicesEqual(decoded.Values, list.Values) {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, errors.New("collections: column_graph layer-0 adjacency source image sections do not match decoded column")
	}
	return list, image, nil
}

func validateColumnVectorGraphLayer0AdjacencySourceMatchesGraph(graph columnVectorGraphManifestSnapshot, sourceCfg ColumnStoreConfig) error {
	source := graph.Layer0AdjacencySource
	if !source.Present {
		return nil
	}
	if source.Schema != columnVectorGraphLayer0AdjacencySourceSchema {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source schema=%q want %q", source.Schema, columnVectorGraphLayer0AdjacencySourceSchema)
	}
	if source.ColumnName != columnVectorGraphLayer0AdjacencySourceColumnName {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source column=%q want %q", source.ColumnName, columnVectorGraphLayer0AdjacencySourceColumnName)
	}
	if source.Encoding != typedcolumn.EncodingRawUint32OffsetsList.String() || source.ValueType != string(ColumnStoreValueAdjacencyList) {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source type/encoding=(%q,%q) want (%q,%q)", source.ValueType, source.Encoding, ColumnStoreValueAdjacencyList, typedcolumn.EncodingRawUint32OffsetsList)
	}
	if source.Layer != 0 {
		return fmt.Errorf("collections: column_graph adjacency source layer=%d want 0", source.Layer)
	}
	if source.RowCount != graph.RowCount {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source rows=%d want graph rows=%d", source.RowCount, graph.RowCount)
	}
	if source.SourceSchemaHash != sourceCfg.SchemaHash {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source schema_hash=%d want %d", source.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if source.BaseManifestGeneration != graph.BaseManifestGeneration || source.BaseManifestChecksum != graph.BaseManifestChecksum || source.BaseSchemaHash != graph.BaseSchemaHash || source.GraphSchemaHash != graph.GraphSchemaHash {
		return errors.New("collections: column_graph layer-0 adjacency source stale base/graph identity")
	}
	if source.GraphAssetGeneration != graph.AssetRef.Generation || source.GraphAssetPartID != graph.AssetRef.PartID || source.GraphAssetFileID != graph.AssetRef.FileID || source.GraphAssetOffset != graph.AssetRef.Offset || source.GraphAssetLength != graph.AssetRef.Length || source.GraphAssetChecksum != graph.AssetRef.Checksum {
		return errors.New("collections: column_graph layer-0 adjacency source stale graph asset identity")
	}
	if source.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source kind=%q want %q", source.Ref.Kind, ColumnAssetKindTCS1TypedColumnPart)
	}
	if source.Ref.Namespace != graph.AssetRef.Namespace || source.Ref.Generation != graph.BaseManifestGeneration {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source namespace/generation=(%q,%d) want (%q,%d)", source.Ref.Namespace, source.Ref.Generation, graph.AssetRef.Namespace, graph.BaseManifestGeneration)
	}
	if err := validateColumnAssetRefForPlan(source.Ref); err != nil {
		return err
	}
	if source.AssetBytes <= 0 || source.AssetBytes != source.Ref.Length {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source asset bytes=%d ref length=%d", source.AssetBytes, source.Ref.Length)
	}
	if source.RowCount == math.MaxInt {
		return errors.New("collections: column_graph layer-0 adjacency source row_count+1 overflows int")
	}
	if int64(source.RowCount) > math.MaxInt64/8-1 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source row_count=%d offsets byte count overflows int64", source.RowCount)
	}
	wantOffsetsBytes := int64(source.RowCount+1) * 8
	if source.OffsetsBytes != wantOffsetsBytes {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source offsets_bytes=%d want %d", source.OffsetsBytes, wantOffsetsBytes)
	}
	if source.ValuesCount < 0 || source.ValuesCount > math.MaxInt/4 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source values_count=%d outside supported range", source.ValuesCount)
	}
	if source.ValuesBytes != int64(source.ValuesCount)*4 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source values_bytes=%d want values_count*4=%d", source.ValuesBytes, int64(source.ValuesCount)*4)
	}
	if source.PaddingBytes < 0 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source padding_bytes=%d", source.PaddingBytes)
	}
	return nil
}

func columnVectorGraphLayer0AdjacencySourceFromPrepared(graph columnVectorGraphManifestSnapshot, prepared columnVectorGraphPreparedLayer0AdjacencySource) columnVectorGraphLayer0AdjacencySourceSnapshot {
	if !prepared.Present {
		return columnVectorGraphLayer0AdjacencySourceSnapshot{}
	}
	return columnVectorGraphLayer0AdjacencySourceSnapshot{
		Present:                true,
		Schema:                 columnVectorGraphLayer0AdjacencySourceSchema,
		ColumnName:             columnVectorGraphLayer0AdjacencySourceColumnName,
		ValueType:              string(ColumnStoreValueAdjacencyList),
		Encoding:               typedcolumn.EncodingRawUint32OffsetsList.String(),
		Layer:                  0,
		SourceSchemaHash:       prepared.SchemaHash,
		RowCount:               prepared.Rows,
		ValuesCount:            prepared.Values,
		OffsetsBytes:           prepared.OffsetsBytes,
		ValuesBytes:            prepared.ValuesBytes,
		PaddingBytes:           prepared.PaddingBytes,
		Ref:                    prepared.Ref,
		AssetBytes:             prepared.Bytes,
		BaseManifestGeneration: graph.BaseManifestGeneration,
		BaseManifestChecksum:   graph.BaseManifestChecksum,
		BaseSchemaHash:         graph.BaseSchemaHash,
		GraphSchemaHash:        graph.GraphSchemaHash,
		GraphAssetGeneration:   graph.AssetRef.Generation,
		GraphAssetPartID:       graph.AssetRef.PartID,
		GraphAssetFileID:       graph.AssetRef.FileID,
		GraphAssetOffset:       graph.AssetRef.Offset,
		GraphAssetLength:       graph.AssetRef.Length,
		GraphAssetChecksum:     graph.AssetRef.Checksum,
	}
}

func columnVectorGraphStorageBytes(graph columnVectorGraphManifestSnapshot) int64 {
	bytes := graph.AssetBytes
	if graph.Layer0AdjacencySource.Present {
		bytes += graph.Layer0AdjacencySource.AssetBytes
	}
	return bytes
}

func columnVectorGraphUint32SlicesEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
