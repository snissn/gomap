package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

const (
	columnVectorGraphLayer0AdjacencySourceSchema     = "column_graph_layer0_adjacency/raw_uint32_offsets_list/v1"
	columnVectorGraphAdjacencyLayerSourceSchema      = "column_graph_adjacency_layer/raw_uint32_offsets_list/v1"
	columnVectorGraphLayer0AdjacencySourceColumnName = "layer0_adjacency"
	columnVectorGraphLayer0AdjacencySourcePathSuffix = "_layer0_neighbors"
)

type columnVectorGraphPreparedAdjacencySource struct {
	Present      bool
	Layer        int
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

type columnVectorGraphPreparedLayer0AdjacencySource = columnVectorGraphPreparedAdjacencySource

func columnVectorGraphLayer0AdjacencySourceColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	return columnVectorGraphAdjacencySourceColumnStoreConfig(collection, base, def, 0)
}

func columnVectorGraphAdjacencySourceColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, layer int) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	if layer < 0 {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph adjacency source layer=%d must be non-negative", layer)
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph adjacency source requires column asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:            columnVectorGraphAdjacencySourceColumnName(layer),
			Path:            def.Field + columnVectorGraphAdjacencySourcePathSuffix(layer),
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
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph adjacency layer %d source fields=%d want 1", layer, len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || !typedColumnAdapterOffsetsListAdjacencyDirectPayloadSupported(columns[0]) {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph adjacency layer %d source is not raw_uint32_offsets_list adjacency", layer)
	}
	return *cfg, columns[0], nil
}

func columnVectorGraphAdjacencySourceColumnName(layer int) string {
	if layer == 0 {
		return columnVectorGraphLayer0AdjacencySourceColumnName
	}
	return fmt.Sprintf("layer%d_adjacency", layer)
}

func columnVectorGraphAdjacencySourcePathSuffix(layer int) string {
	if layer == 0 {
		return columnVectorGraphLayer0AdjacencySourcePathSuffix
	}
	return fmt.Sprintf("_layer%d_neighbors", layer)
}

func columnVectorGraphAdjacencySourceSchema(layer int) string {
	if layer == 0 {
		return columnVectorGraphLayer0AdjacencySourceSchema
	}
	return columnVectorGraphAdjacencyLayerSourceSchema
}

func prepareColumnVectorGraphLayer0AdjacencySourceAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedLayer0AdjacencySource, error) {
	return prepareColumnVectorGraphAdjacencySourceAsset(assetRootDir, collection, base, def, generation, partID, 0, rows)
}

func prepareColumnVectorGraphAdjacencySourcesAssets(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedAdjacencySource, error) {
	maxLayer := 0
	for rowIdx := range rows {
		rowMaxLayer, err := columnVectorGraphAdjacencyMaxLayer(rows[rowIdx].Adjacency)
		if err != nil {
			return nil, fmt.Errorf("collections: column_graph adjacency row %d max layer: %w", rowIdx, err)
		}
		if rowMaxLayer > maxLayer {
			maxLayer = rowMaxLayer
		}
	}
	layers := make([]columnVectorGraphPreparedAdjacencySource, maxLayer+1)
	partID := firstPartID
	for layer := 0; layer <= maxLayer; layer++ {
		if partID == 0 {
			return nil, errors.New("collections: column_graph adjacency source part_id overflow")
		}
		prepared, err := prepareColumnVectorGraphAdjacencySourceAsset(assetRootDir, collection, base, def, generation, partID, layer, rows)
		if err != nil {
			return nil, err
		}
		layers[layer] = prepared
		if layer != maxLayer {
			if partID == ^uint64(0) {
				return nil, errors.New("collections: column_graph adjacency source part_id overflow")
			}
			partID = nextColumnVectorGraphPartIDAfter(partID, partID)
		}
	}
	return layers, nil
}

func prepareColumnVectorGraphAdjacencySourceAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, layer int, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedAdjacencySource, error) {
	if assetRootDir == "" {
		return columnVectorGraphPreparedAdjacencySource{}, errors.New("collections: column_graph adjacency source requires asset root dir")
	}
	if generation == 0 || partID == 0 {
		return columnVectorGraphPreparedAdjacencySource{}, errors.New("collections: column_graph adjacency source requires generation and part_id")
	}
	if layer < 0 {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency source layer=%d must be non-negative", layer)
	}
	sourceCfg, _, err := columnVectorGraphAdjacencySourceColumnStoreConfig(collection, base, def, layer)
	if err != nil {
		return columnVectorGraphPreparedAdjacencySource{}, err
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterRows := make([]typedColumnAdapterRow, len(rows))
	valuesCount := 0
	for rowIdx := range rows {
		layerAdjacency, err := columnVectorGraphAdjacencyLayer(rows[rowIdx].Adjacency, layer)
		if err != nil {
			return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d row %d: %w", layer, rowIdx, err)
		}
		if len(layerAdjacency) > math.MaxInt-valuesCount {
			return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d values overflow int", layer)
		}
		valuesCount += len(layerAdjacency)
		adapterRows[rowIdx] = typedColumnAdapterRow{
			PrimaryID: int64(rowIdx),
			Values: map[string]columnDeclaredValue{
				fields[0].Path: {Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: append([]uint32(nil), layerAdjacency...)},
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
		return columnVectorGraphPreparedAdjacencySource{}, err
	}
	image, err := part.buildImage()
	if err != nil {
		return columnVectorGraphPreparedAdjacencySource{}, err
	}
	if image.Rows != len(rows) || image.PartID != partID {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d source image rows/part=(%d,%d) want (%d,%d)", layer, image.Rows, image.PartID, len(rows), partID)
	}
	accounting := part.Part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes <= 0 {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d source missing offsets bytes", layer)
	}
	if accounting.DeclaredColumnValuesBytes < 0 {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d source invalid values bytes", layer)
	}
	ref, err := writeColumnVectorGraphLayer0AdjacencySourceAssetToManager(assetRootDir, sourceCfg, image.Bytes, generation, partID)
	if err != nil {
		return columnVectorGraphPreparedAdjacencySource{}, err
	}
	if ref.Namespace != sourceCfg.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != partID || ref.Length != int64(len(image.Bytes)) {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: invalid column_graph adjacency layer %d source asset ref %+v", layer, ref)
	}
	if ref.Offset%8 != 0 {
		return columnVectorGraphPreparedAdjacencySource{}, fmt.Errorf("collections: column_graph adjacency layer %d source absolute asset offset=%d is not 8-byte aligned", layer, ref.Offset)
	}
	return columnVectorGraphPreparedAdjacencySource{
		Present:      true,
		Layer:        layer,
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
	return validateColumnVectorGraphAdjacencySourceAsset(rootDir, collection, cfg, def, graph, graph.Layer0AdjacencySource)
}

func validateColumnVectorGraphAdjacencyLayerSourcesAssets(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) error {
	if len(graph.AdjacencyLayerSources) == 0 {
		return nil
	}
	if graph.AdjacencyLayerCount != len(graph.AdjacencyLayerSources) {
		return fmt.Errorf("collections: column_graph adjacency layer sources count=%d want %d", len(graph.AdjacencyLayerSources), graph.AdjacencyLayerCount)
	}
	for layer, source := range graph.AdjacencyLayerSources {
		if !source.Present || source.Layer != layer {
			return fmt.Errorf("collections: column_graph adjacency layer source[%d] present/layer=(%t,%d)", layer, source.Present, source.Layer)
		}
		if err := validateColumnVectorGraphAdjacencySourceAsset(rootDir, collection, cfg, def, graph, source); err != nil {
			return fmt.Errorf("collections: column_graph adjacency layer %d source: %w", layer, err)
		}
	}
	return nil
}

func validateColumnVectorGraphAdjacencySourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, source columnVectorGraphLayer0AdjacencySourceSnapshot) error {
	if !source.Present {
		return nil
	}
	sourceCfg, adapterColumn, err := columnVectorGraphAdjacencySourceColumnStoreConfig(collection, cfg, def, source.Layer)
	if err != nil {
		return err
	}
	if err := validateColumnVectorGraphAdjacencySourceMatchesGraph(graph, source, sourceCfg); err != nil {
		return err
	}
	if err := validateColumnVectorGraphAssetRefAvailable(rootDir, source.Ref); err != nil {
		return err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, source.Ref)
	if err != nil {
		return err
	}
	if int64(len(raw)) != source.AssetBytes || int64(len(raw)) != source.Ref.Length {
		return fmt.Errorf("collections: column_graph adjacency layer %d source bytes=%d manifest=%d ref=%d", source.Layer, len(raw), source.AssetBytes, source.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return err
	}
	if image.PartID != source.Ref.PartID || image.Rows != source.RowCount {
		return fmt.Errorf("collections: column_graph adjacency layer %d source image part/rows=(%d,%d) want (%d,%d)", source.Layer, image.PartID, image.Rows, source.Ref.PartID, source.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	if _, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image); err != nil {
		return err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return fmt.Errorf("collections: column_graph adjacency layer %d source layout certification: %w", source.Layer, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return fmt.Errorf("collections: column_graph adjacency layer %d source missing layout certification for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return fmt.Errorf("collections: column_graph adjacency layer %d source missing offsets-list sections for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	if int64(offsetsSection.Length) != source.OffsetsBytes || int64(valuesSection.Length) != source.ValuesBytes {
		return fmt.Errorf("collections: column_graph adjacency layer %d source section bytes offsets=%d/%d values=%d/%d", source.Layer, offsetsSection.Length, source.OffsetsBytes, valuesSection.Length, source.ValuesBytes)
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
		return fmt.Errorf("collections: column_graph adjacency layer %d source direct-view section validation failed: %s", source.Layer, status.String())
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return err
	}
	return validateColumnVectorGraphLayer0AdjacencySourceOffsets(offsetsRaw, source.RowCount, source.ValuesCount)
}

func validateColumnVectorGraphLayer0AdjacencySourceOffsets(offsetsRaw []byte, rows, values int) error {
	if rows < 0 || values < 0 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source rows/values=(%d,%d) must be non-negative", rows, values)
	}
	if len(offsetsRaw)%8 != 0 {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source offsets bytes=%d want multiple of 8", len(offsetsRaw))
	}
	offsets := make([]uint64, len(offsetsRaw)/8)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint64(offsetsRaw[i*8:])
	}
	if err := typedcolumn.ValidateRawUint32OffsetsListShape(rows, offsets, uint64(values)); err != nil {
		return fmt.Errorf("collections: column_graph layer-0 adjacency source offsets validation: %w", err)
	}
	return nil
}

func decodeColumnVectorGraphLayer0AdjacencySourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) (typedcolumn.RawUint32OffsetsList, typedcolumn.ColumnPartImage, error) {
	return decodeColumnVectorGraphAdjacencySourceAsset(rootDir, collection, cfg, def, graph, graph.Layer0AdjacencySource)
}

func decodeColumnVectorGraphAdjacencyLayerSourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, layer int) (typedcolumn.RawUint32OffsetsList, typedcolumn.ColumnPartImage, error) {
	if layer < 0 || layer >= len(graph.AdjacencyLayerSources) {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer source %d outside layer count %d", layer, len(graph.AdjacencyLayerSources))
	}
	return decodeColumnVectorGraphAdjacencySourceAsset(rootDir, collection, cfg, def, graph, graph.AdjacencyLayerSources[layer])
}

func decodeColumnVectorGraphAdjacencySourceAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, source columnVectorGraphLayer0AdjacencySourceSnapshot) (typedcolumn.RawUint32OffsetsList, typedcolumn.ColumnPartImage, error) {
	if !source.Present {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, errors.New("collections: column_graph adjacency source is absent")
	}
	sourceCfg, adapterColumn, err := columnVectorGraphAdjacencySourceColumnStoreConfig(collection, cfg, def, source.Layer)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if err := validateColumnVectorGraphAdjacencySourceMatchesGraph(graph, source, sourceCfg); err != nil {
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
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source bytes=%d manifest=%d ref=%d", source.Layer, len(raw), source.AssetBytes, source.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if image.PartID != source.Ref.PartID || image.Rows != source.RowCount {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source image part/rows=(%d,%d) want (%d,%d)", source.Layer, image.PartID, image.Rows, source.Ref.PartID, source.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source layout certification: %w", source.Layer, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source missing layout certification for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source missing offsets-list sections for column %q", source.Layer, adapterColumn.Definition.Name)
	}
	if int64(offsetsSection.Length) != source.OffsetsBytes || int64(valuesSection.Length) != source.ValuesBytes {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source section bytes offsets=%d/%d values=%d/%d", source.Layer, offsetsSection.Length, source.OffsetsBytes, valuesSection.Length, source.ValuesBytes)
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
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source direct-view section validation failed: %s", source.Layer, status.String())
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
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source values=%d want %d", source.Layer, len(list.Values), source.ValuesCount)
	}
	decoded, err := adapterPart.Part.Uint32OffsetsListColumn(adapterColumn.Definition.Name, nil, nil)
	if err != nil {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, err
	}
	if decoded.Rows != list.Rows || !uint64SlicesEqual(decoded.Offsets, list.Offsets) || !columnVectorGraphUint32SlicesEqual(decoded.Values, list.Values) {
		return typedcolumn.RawUint32OffsetsList{}, typedcolumn.ColumnPartImage{}, fmt.Errorf("collections: column_graph adjacency layer %d source image sections do not match decoded column", source.Layer)
	}
	return list, image, nil
}

func validateColumnVectorGraphLayer0AdjacencySourceMatchesGraph(graph columnVectorGraphManifestSnapshot, sourceCfg ColumnStoreConfig) error {
	return validateColumnVectorGraphAdjacencySourceMatchesGraph(graph, graph.Layer0AdjacencySource, sourceCfg)
}

func validateColumnVectorGraphAdjacencySourceMatchesGraph(graph columnVectorGraphManifestSnapshot, source columnVectorGraphLayer0AdjacencySourceSnapshot, sourceCfg ColumnStoreConfig) error {
	if !source.Present {
		return nil
	}
	wantSchema := columnVectorGraphAdjacencySourceSchema(source.Layer)
	if source.Schema != wantSchema {
		return fmt.Errorf("collections: column_graph adjacency layer %d source schema=%q want %q", source.Layer, source.Schema, wantSchema)
	}
	wantColumn := columnVectorGraphAdjacencySourceColumnName(source.Layer)
	if source.ColumnName != wantColumn {
		return fmt.Errorf("collections: column_graph adjacency layer %d source column=%q want %q", source.Layer, source.ColumnName, wantColumn)
	}
	if source.Encoding != typedcolumn.EncodingRawUint32OffsetsList.String() || source.ValueType != string(ColumnStoreValueAdjacencyList) {
		return fmt.Errorf("collections: column_graph adjacency layer %d source type/encoding=(%q,%q) want (%q,%q)", source.Layer, source.ValueType, source.Encoding, ColumnStoreValueAdjacencyList, typedcolumn.EncodingRawUint32OffsetsList)
	}
	if source.Layer < 0 {
		return fmt.Errorf("collections: column_graph adjacency source layer=%d must be non-negative", source.Layer)
	}
	if source.RowCount != graph.RowCount {
		return fmt.Errorf("collections: column_graph adjacency layer %d source rows=%d want graph rows=%d", source.Layer, source.RowCount, graph.RowCount)
	}
	if source.SourceSchemaHash != sourceCfg.SchemaHash {
		return fmt.Errorf("collections: column_graph adjacency layer %d source schema_hash=%d want %d", source.Layer, source.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if source.BaseManifestGeneration != graph.BaseManifestGeneration || source.BaseManifestChecksum != graph.BaseManifestChecksum || source.BaseSchemaHash != graph.BaseSchemaHash || source.GraphSchemaHash != graph.GraphSchemaHash {
		return fmt.Errorf("collections: column_graph adjacency layer %d source stale base/graph identity", source.Layer)
	}
	if source.GraphAssetGeneration != graph.AssetRef.Generation || source.GraphAssetPartID != graph.AssetRef.PartID || source.GraphAssetFileID != graph.AssetRef.FileID || source.GraphAssetOffset != graph.AssetRef.Offset || source.GraphAssetLength != graph.AssetRef.Length || source.GraphAssetChecksum != graph.AssetRef.Checksum {
		return fmt.Errorf("collections: column_graph adjacency layer %d source stale graph asset identity", source.Layer)
	}
	if source.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		return fmt.Errorf("collections: column_graph adjacency layer %d source kind=%q want %q", source.Layer, source.Ref.Kind, ColumnAssetKindTCS1TypedColumnPart)
	}
	if source.Ref.Namespace != graph.AssetRef.Namespace || source.Ref.Generation != graph.BaseManifestGeneration {
		return fmt.Errorf("collections: column_graph adjacency layer %d source namespace/generation=(%q,%d) want (%q,%d)", source.Layer, source.Ref.Namespace, source.Ref.Generation, graph.AssetRef.Namespace, graph.BaseManifestGeneration)
	}
	if err := validateColumnAssetRefForPlan(source.Ref); err != nil {
		return err
	}
	if source.AssetBytes <= 0 || source.AssetBytes != source.Ref.Length {
		return fmt.Errorf("collections: column_graph adjacency layer %d source asset bytes=%d ref length=%d", source.Layer, source.AssetBytes, source.Ref.Length)
	}
	if source.RowCount == math.MaxInt {
		return fmt.Errorf("collections: column_graph adjacency layer %d source row_count+1 overflows int", source.Layer)
	}
	if int64(source.RowCount) > math.MaxInt64/8-1 {
		return fmt.Errorf("collections: column_graph adjacency layer %d source row_count=%d offsets byte count overflows int64", source.Layer, source.RowCount)
	}
	wantOffsetsBytes := int64(source.RowCount+1) * 8
	if source.OffsetsBytes != wantOffsetsBytes {
		return fmt.Errorf("collections: column_graph adjacency layer %d source offsets_bytes=%d want %d", source.Layer, source.OffsetsBytes, wantOffsetsBytes)
	}
	if source.ValuesCount < 0 || source.ValuesCount > math.MaxInt/4 {
		return fmt.Errorf("collections: column_graph adjacency layer %d source values_count=%d outside supported range", source.Layer, source.ValuesCount)
	}
	if source.ValuesBytes != int64(source.ValuesCount)*4 {
		return fmt.Errorf("collections: column_graph adjacency layer %d source values_bytes=%d want values_count*4=%d", source.Layer, source.ValuesBytes, int64(source.ValuesCount)*4)
	}
	if source.PaddingBytes < 0 {
		return fmt.Errorf("collections: column_graph adjacency layer %d source padding_bytes=%d", source.Layer, source.PaddingBytes)
	}
	return nil
}

func columnVectorGraphLayer0AdjacencySourceFromPrepared(graph columnVectorGraphManifestSnapshot, prepared columnVectorGraphPreparedLayer0AdjacencySource) columnVectorGraphLayer0AdjacencySourceSnapshot {
	return columnVectorGraphAdjacencySourceFromPrepared(graph, prepared)
}

func columnVectorGraphAdjacencyLayerSourcesFromPrepared(graph columnVectorGraphManifestSnapshot, prepared []columnVectorGraphPreparedAdjacencySource) []columnVectorGraphLayer0AdjacencySourceSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	sources := make([]columnVectorGraphLayer0AdjacencySourceSnapshot, len(prepared))
	for i := range prepared {
		sources[i] = columnVectorGraphAdjacencySourceFromPrepared(graph, prepared[i])
	}
	return sources
}

func columnVectorGraphAdjacencySourceFromPrepared(graph columnVectorGraphManifestSnapshot, prepared columnVectorGraphPreparedAdjacencySource) columnVectorGraphLayer0AdjacencySourceSnapshot {
	if !prepared.Present {
		return columnVectorGraphLayer0AdjacencySourceSnapshot{}
	}
	return columnVectorGraphLayer0AdjacencySourceSnapshot{
		Present:                true,
		Schema:                 columnVectorGraphAdjacencySourceSchema(prepared.Layer),
		ColumnName:             columnVectorGraphAdjacencySourceColumnName(prepared.Layer),
		ValueType:              string(ColumnStoreValueAdjacencyList),
		Encoding:               typedcolumn.EncodingRawUint32OffsetsList.String(),
		Layer:                  prepared.Layer,
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
	if len(graph.AdjacencyLayerSources) > 0 {
		for _, source := range graph.AdjacencyLayerSources {
			if source.Present {
				bytes += source.AssetBytes
			}
		}
		return bytes
	}
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
