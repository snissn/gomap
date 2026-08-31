package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
)

const (
	columnVectorIndexStateAdjacencyAssetIDPrefix = "hnsw/layer/"
	columnVectorIndexStateAdjacencyColumnName    = "adjacency"
)

type columnVectorIndexStatePreparedAdjacencyAsset struct {
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

type columnVectorIndexStatePreparedAdjacencyPayload struct {
	Layer        int
	Config       ColumnStoreConfig
	PartID       uint64
	Payload      []byte
	Bytes        int64
	Rows         int
	Values       int
	OffsetsBytes int64
	ValuesBytes  int64
	PaddingBytes int64
	SchemaHash   uint64
}

func columnVectorIndexStateAdjacencyAssetID(layer int) string {
	return columnVectorIndexStateAdjacencyAssetIDPrefix + strconv.Itoa(layer)
}

func columnVectorIndexStateAdjacencyLayerFromAssetID(assetID string) (int, error) {
	layerText, ok := strings.CutPrefix(assetID, columnVectorIndexStateAdjacencyAssetIDPrefix)
	if !ok || layerText == "" {
		return 0, fmt.Errorf("collections: vector-index state adjacency asset id %q must have prefix %q", assetID, columnVectorIndexStateAdjacencyAssetIDPrefix)
	}
	layer, err := strconv.Atoi(layerText)
	if err != nil || layer < 0 {
		if err == nil {
			err = fmt.Errorf("negative layer=%d", layer)
		}
		return 0, fmt.Errorf("collections: vector-index state adjacency asset id %q has invalid layer: %w", assetID, err)
	}
	return layer, nil
}

func columnVectorIndexStateAdjacencyPath(def VectorIndexDefinition, layer int) string {
	return fmt.Sprintf("%s_hnsw_adjacency_layer_%d", def.Field, layer)
}

func columnVectorIndexStateAdjacencyColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, layer int) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	if layer < 0 {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector-index state adjacency layer=%d must be non-negative", layer)
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: vector-index state adjacency requires column asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:      columnVectorIndexStateAdjacencyColumnName,
			Path:      columnVectorIndexStateAdjacencyPath(def, layer),
			Owner:     TypedStorageOwnerColumnPart,
			ValueType: ColumnStoreValueUint32List,
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
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector-index state adjacency layer %d fields=%d want 1", layer, len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || !typedColumnAdapterUint32ListDirectPayloadSupported(columns[0]) {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector-index state adjacency layer %d is not generic uint32_list raw_uint32_offsets_list", layer)
	}
	return *cfg, columns[0], nil
}

func buildColumnVectorIndexStateAdjacencyLists(rows []columnVectorGraphAssetRow) ([]typedcolumn.Uint32List, error) {
	maxLayer := 0
	layerValueCounts := []int{0}
	for rowIdx := range rows {
		adjacency := rows[rowIdx].Adjacency
		rowMaxLayer, err := columnVectorGraphAdjacencyMaxLayer(adjacency)
		if err != nil {
			return nil, fmt.Errorf("collections: vector-index state adjacency row %d max layer: %w", rowIdx, err)
		}
		if rowMaxLayer > maxLayer {
			maxLayer = rowMaxLayer
			layerValueCounts = append(layerValueCounts, make([]int, rowMaxLayer+1-len(layerValueCounts))...)
		}
		if !columnVectorGraphAdjacencyIsLayered(adjacency) {
			if len(adjacency) > math.MaxInt-layerValueCounts[0] {
				return nil, fmt.Errorf("collections: vector-index state adjacency layer 0 values overflow int")
			}
			layerValueCounts[0] += len(adjacency)
			continue
		}
		pos := 2
		for layer := 0; layer <= rowMaxLayer; layer++ {
			if pos >= len(adjacency) {
				return nil, fmt.Errorf("collections: vector-index state adjacency row %d layer=%d missing count", rowIdx, layer)
			}
			count := int(adjacency[pos])
			pos++
			if count < 0 || count > len(adjacency)-pos {
				return nil, fmt.Errorf("collections: vector-index state adjacency row %d layer=%d count=%d exceeds remaining=%d", rowIdx, layer, count, len(adjacency)-pos)
			}
			if count > math.MaxInt-layerValueCounts[layer] {
				return nil, fmt.Errorf("collections: vector-index state adjacency layer %d values overflow int", layer)
			}
			layerValueCounts[layer] += count
			pos += count
		}
		if pos != len(adjacency) {
			return nil, fmt.Errorf("collections: vector-index state adjacency row %d trailing values=%d", rowIdx, len(adjacency)-pos)
		}
	}
	layers := make([]typedcolumn.Uint32List, maxLayer+1)
	for layer := range layers {
		layers[layer].Rows = len(rows)
		layers[layer].Offsets = make([]uint64, len(rows)+1)
		layers[layer].Values = make([]uint32, 0, layerValueCounts[layer])
	}
	for rowIdx := range rows {
		adjacency := rows[rowIdx].Adjacency
		if !columnVectorGraphAdjacencyIsLayered(adjacency) {
			if len(adjacency) > math.MaxInt-len(layers[0].Values) {
				return nil, fmt.Errorf("collections: vector-index state adjacency layer 0 values overflow int")
			}
			layers[0].Values = append(layers[0].Values, adjacency...)
		} else {
			rowMaxLayer := int(adjacency[1])
			pos := 2
			for layer := 0; layer <= rowMaxLayer; layer++ {
				if pos >= len(adjacency) {
					return nil, fmt.Errorf("collections: vector-index state adjacency row %d layer=%d missing count", rowIdx, layer)
				}
				count := int(adjacency[pos])
				pos++
				if count > len(adjacency)-pos {
					return nil, fmt.Errorf("collections: vector-index state adjacency row %d layer=%d count=%d exceeds remaining=%d", rowIdx, layer, count, len(adjacency)-pos)
				}
				if count > math.MaxInt-len(layers[layer].Values) {
					return nil, fmt.Errorf("collections: vector-index state adjacency layer %d values overflow int", layer)
				}
				layers[layer].Values = append(layers[layer].Values, adjacency[pos:pos+count]...)
				pos += count
			}
			if pos != len(adjacency) {
				return nil, fmt.Errorf("collections: vector-index state adjacency row %d trailing values=%d", rowIdx, len(adjacency)-pos)
			}
		}
		for layer := range layers {
			layers[layer].Offsets[rowIdx+1] = uint64(len(layers[layer].Values))
		}
	}
	return layers, nil
}

func prepareColumnVectorIndexStateAdjacencyAssets(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorIndexStatePreparedAdjacencyAsset, error) {
	return prepareColumnVectorIndexStateAdjacencyAssetsWithStableAuthority(assetRootDir, collection, base, def, generation, firstPartID, rows, nil)
}

func prepareColumnVectorIndexStateAdjacencyAssetsWithStableAuthority(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, firstPartID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) ([]columnVectorIndexStatePreparedAdjacencyAsset, error) {
	payloads, err := prepareColumnVectorIndexStateAdjacencyPayloads(assetRootDir, collection, base, def, firstPartID, rows)
	if err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}
	appender, err := newColumnVectorGraphAssetAppender(assetRootDir, payloads[0].Config, authority)
	if err != nil {
		return nil, err
	}
	layers, appendErr := appendColumnVectorIndexStateAdjacencyPayloads(appender, generation, payloads)
	closeErr := closeColumnVectorGraphAssetAppender(appender, authority)
	if appendErr != nil {
		return nil, errors.Join(appendErr, closeErr)
	}
	return layers, closeErr
}

func prepareColumnVectorIndexStateAdjacencyPayloads(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorIndexStatePreparedAdjacencyPayload, error) {
	lists, err := buildColumnVectorIndexStateAdjacencyLists(rows)
	if err != nil {
		return nil, err
	}
	payloads := make([]columnVectorIndexStatePreparedAdjacencyPayload, len(lists))
	partID := firstPartID
	for layer, list := range lists {
		if partID == 0 {
			return nil, errors.New("collections: vector-index state adjacency part_id overflow")
		}
		prepared, err := prepareColumnVectorIndexStateAdjacencyPayloadFromList(assetRootDir, collection, base, def, partID, layer, list)
		if err != nil {
			return nil, err
		}
		payloads[layer] = prepared
		if layer != len(lists)-1 {
			if partID == ^uint64(0) {
				return nil, errors.New("collections: vector-index state adjacency part_id overflow")
			}
			partID = nextColumnVectorGraphPartIDAfter(partID, partID)
		}
	}
	return payloads, nil
}

func prepareColumnVectorIndexStateAdjacencyPayloadFromList(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, partID uint64, layer int, list typedcolumn.Uint32List) (columnVectorIndexStatePreparedAdjacencyPayload, error) {
	if assetRootDir == "" {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, errors.New("collections: vector-index state adjacency requires asset root dir")
	}
	if partID == 0 {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, errors.New("collections: vector-index state adjacency requires non-zero part_id")
	}
	if layer < 0 {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, fmt.Errorf("collections: vector-index state adjacency layer=%d must be non-negative", layer)
	}
	if err := validateColumnVectorIndexStateAdjacencyList(layer, list); err != nil {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, err
	}
	sourceCfg, adapterColumn, err := columnVectorIndexStateAdjacencyColumnStoreConfig(collection, base, def, layer)
	if err != nil {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, err
	}
	primaryIDs := make([]int64, list.Rows)
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
		Rows: list.Rows,
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
		},
		Uint32OffsetsLists: map[string]typedcolumn.RawUint32OffsetsList{
			adapterColumn.Definition.Name: list,
		},
	})
	if err != nil {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, err
	}
	logicalTypes := make(map[string]string, 1)
	if logical, ok := columnStoreSemanticLogicalType(ColumnStoreValueUint32List); ok {
		logicalTypes[adapterColumn.Definition.Name] = string(logical)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}), LayoutLogicalTypes: logicalTypes})
	if err != nil {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, err
	}
	if image.Rows != list.Rows || image.PartID != partID {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, fmt.Errorf("collections: vector-index state adjacency layer %d image rows/part=(%d,%d) want (%d,%d)", layer, image.Rows, image.PartID, list.Rows, partID)
	}
	accounting := part.ByteAccountingFromImage(image)
	if accounting.DeclaredColumnOffsetsBytes <= 0 {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, fmt.Errorf("collections: vector-index state adjacency layer %d missing offsets bytes", layer)
	}
	if accounting.DeclaredColumnValuesBytes < 0 {
		return columnVectorIndexStatePreparedAdjacencyPayload{}, fmt.Errorf("collections: vector-index state adjacency layer %d invalid values bytes", layer)
	}
	return columnVectorIndexStatePreparedAdjacencyPayload{
		Layer:        layer,
		Config:       sourceCfg,
		PartID:       partID,
		Payload:      image.Bytes,
		Bytes:        int64(len(image.Bytes)),
		Rows:         image.Rows,
		Values:       len(list.Values),
		OffsetsBytes: int64(accounting.DeclaredColumnOffsetsBytes),
		ValuesBytes:  int64(accounting.DeclaredColumnValuesBytes),
		PaddingBytes: int64(accounting.SerializedPaddingBytes),
		SchemaHash:   sourceCfg.SchemaHash,
	}, nil
}

func appendColumnVectorIndexStateAdjacencyPayloads(appender *columnPhysicalAssetSegmentAppender, generation uint64, payloads []columnVectorIndexStatePreparedAdjacencyPayload) ([]columnVectorIndexStatePreparedAdjacencyAsset, error) {
	layers := make([]columnVectorIndexStatePreparedAdjacencyAsset, len(payloads))
	for i, payload := range payloads {
		alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, payload.Config)
		ref, err := appender.appendKindWithAlignment(payload.Payload, ColumnAssetKindTCS1TypedColumnPart, generation, payload.PartID, alignment)
		if err != nil {
			return nil, err
		}
		if err := validateColumnVectorIndexStatePreparedAdjacencyRef(payload, ref, generation); err != nil {
			appender.failed = true
			return nil, err
		}
		layers[i] = columnVectorIndexStatePreparedAdjacencyAsset{
			Layer:        payload.Layer,
			Config:       payload.Config,
			Ref:          ref,
			Bytes:        ref.Length,
			Rows:         payload.Rows,
			Values:       payload.Values,
			OffsetsBytes: payload.OffsetsBytes,
			ValuesBytes:  payload.ValuesBytes,
			PaddingBytes: payload.PaddingBytes,
			SchemaHash:   payload.SchemaHash,
		}
	}
	return layers, nil
}

func validateColumnVectorIndexStatePreparedAdjacencyRef(payload columnVectorIndexStatePreparedAdjacencyPayload, ref ColumnAssetRef, generation uint64) error {
	if ref.Namespace != payload.Config.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != payload.PartID || ref.Length != payload.Bytes {
		return fmt.Errorf("collections: invalid vector-index state adjacency layer %d asset ref %+v", payload.Layer, ref)
	}
	if ref.Offset%8 != 0 {
		return fmt.Errorf("collections: vector-index state adjacency layer %d absolute asset offset=%d is not 8-byte aligned", payload.Layer, ref.Offset)
	}
	return nil
}

func columnVectorIndexStateAdjacencyAssetsFromPrepared(prepared []columnVectorIndexStatePreparedAdjacencyAsset) []columnVectorIndexStateAssetSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, len(prepared))
	for i, prepared := range prepared {
		assets[i] = columnVectorIndexStateAssetSnapshot{
			Role:             columnVectorIndexStateAssetRoleAdjacency,
			AssetID:          columnVectorIndexStateAdjacencyAssetID(prepared.Layer),
			LogicalType:      columnVectorIndexStateLogicalTypeUint32List,
			PhysicalEncoding: columnVectorIndexStateEncodingRawUint32List,
			RowCount:         prepared.Rows,
			SourceSchemaHash: prepared.SchemaHash,
			Ref:              prepared.Ref,
			AssetBytes:       prepared.Bytes,
		}
	}
	return assets
}

func validateColumnVectorIndexStateAssets(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, graph columnVectorGraphManifestSnapshot) error {
	return validateColumnVectorIndexStateAssetsWithMode(rootDir, collection, cfg, def, state, graph, true)
}

func validateColumnVectorIndexStateAssetsForStatus(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, graph columnVectorGraphManifestSnapshot) error {
	return validateColumnVectorIndexStateAssetsWithMode(rootDir, collection, cfg, def, state, graph, false)
}

func validateColumnVectorIndexStateAssetsWithMode(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, graph columnVectorGraphManifestSnapshot, validatePayload bool) error {
	seenAdjacencyLayers := make(map[int]string)
	maxAdjacencyLayer := -1
	var rawScratch []byte
	for _, asset := range state.Assets {
		if validatePayload || asset.Role != columnVectorIndexStateAssetRoleDocumentIDs {
			if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
				return fmt.Errorf("collections: vector-index state asset role=%q id=%q unavailable: %w", asset.Role, asset.AssetID, err)
			}
		}
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		layer, err := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if err != nil {
			return err
		}
		if previous, ok := seenAdjacencyLayers[layer]; ok {
			return fmt.Errorf("collections: vector-index state duplicate adjacency layer=%d asset ids %q and %q", layer, previous, asset.AssetID)
		}
		seenAdjacencyLayers[layer] = asset.AssetID
		if layer > maxAdjacencyLayer {
			maxAdjacencyLayer = layer
		}
		sourceCfg, _, err := columnVectorIndexStateAdjacencyColumnStoreConfig(collection, cfg, def, layer)
		if err != nil {
			return err
		}
		if asset.SourceSchemaHash != sourceCfg.SchemaHash {
			return fmt.Errorf("collections: vector-index state adjacency layer %d schema_hash=%d want %d", layer, asset.SourceSchemaHash, sourceCfg.SchemaHash)
		}
		if validatePayload {
			var validateErr error
			rawScratch, validateErr = validateColumnVectorIndexStateAdjacencyAssetInto(rootDir, collection, cfg, def, state, asset, layer, rawScratch)
			if validateErr != nil {
				return fmt.Errorf("collections: vector-index state adjacency layer %d asset %q: %w", layer, asset.AssetID, validateErr)
			}
		}
	}
	if len(seenAdjacencyLayers) == 0 {
		return errors.New("collections: vector-index state missing adjacency uint32_list assets")
	}
	expectedLayers := state.AdjacencyLayerCount
	if expectedLayers <= 0 {
		expectedLayers = graph.AdjacencyLayerCount
	}
	if expectedLayers <= 0 {
		expectedLayers = maxAdjacencyLayer + 1
	}
	if len(seenAdjacencyLayers) != expectedLayers {
		return fmt.Errorf("collections: vector-index state adjacency layers=%d want %d", len(seenAdjacencyLayers), expectedLayers)
	}
	for layer := 0; layer < expectedLayers; layer++ {
		if _, ok := seenAdjacencyLayers[layer]; !ok {
			return fmt.Errorf("collections: vector-index state missing adjacency layer %d", layer)
		}
	}
	if err := validateColumnVectorGraphRowRefStateManifestAssets(collection, cfg, def, state); err != nil {
		return err
	}
	if err := validateColumnVectorGraphDocumentIDStateManifestAsset(collection, cfg, def, state); err != nil {
		return err
	}
	if validatePayload {
		if err := validateColumnVectorGraphDocumentIDStateAssetPayload(rootDir, collection, cfg, def, graph, state); err != nil {
			return err
		}
	}
	if err := validateColumnVectorGraphQuantizedStateAssets(collection, cfg, def, state); err != nil {
		return err
	}
	if err := validateColumnHNSWSearchPackStateAssetIfPresentWithMode(rootDir, cfg, def, graph, state, validatePayload); err != nil {
		return err
	}
	return nil
}

func validateColumnVectorIndexStateAssetRefAvailable(rootDir string, asset columnVectorIndexStateAssetSnapshot) error {
	return validateColumnVectorGraphAssetRefAvailable(rootDir, asset.Ref)
}

func validateColumnVectorIndexStateAdjacencyAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, layer int) error {
	_, err := validateColumnVectorIndexStateAdjacencyAssetInto(rootDir, collection, cfg, def, state, asset, layer, nil)
	return err
}

func validateColumnVectorIndexStateAdjacencyAssetInto(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, layer int, rawScratch []byte) ([]byte, error) {
	sourceCfg, adapterColumn, err := columnVectorIndexStateAdjacencyColumnStoreConfig(collection, cfg, def, layer)
	if err != nil {
		return rawScratch, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return rawScratch, fmt.Errorf("schema_hash=%d want %d", asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	raw, err := readColumnPhysicalAssetFromManagerInto(rootDir, asset.Ref, rawScratch)
	if err != nil {
		return rawScratch, err
	}
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return raw, fmt.Errorf("bytes=%d manifest=%d ref=%d", len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return raw, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount || image.Rows != state.RowCount {
		return raw, fmt.Errorf("image part/rows=(%d,%d) asset/state=(%d,%d)", image.PartID, image.Rows, asset.Ref.PartID, state.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	if _, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image); err != nil {
		return raw, err
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return raw, fmt.Errorf("missing offsets-list sections for column %q", adapterColumn.Definition.Name)
	}
	wantOffsetsBytes, err := columnVectorIndexStateAdjacencyOffsetsBytes(state.RowCount)
	if err != nil {
		return raw, err
	}
	if offsetsSection.Length != wantOffsetsBytes {
		return raw, fmt.Errorf("offsets bytes=%d want %d", offsetsSection.Length, wantOffsetsBytes)
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return raw, err
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		return raw, err
	}
	if err := validateColumnVectorIndexStateAdjacencySections(layer, offsetsSection, valuesSection, offsetsRaw, valuesRaw, state.RowCount); err != nil {
		return raw, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return raw, fmt.Errorf("layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return raw, fmt.Errorf("missing layout certification for column %q", adapterColumn.Definition.Name)
	}
	if certColumn.LogicalType != columnVectorIndexStateLogicalTypeUint32List || certColumn.Type != typedcolumn.ColumnTypeUint32List || certColumn.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		return raw, fmt.Errorf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", certColumn.LogicalType, certColumn.Type, certColumn.Encoding, columnVectorIndexStateLogicalTypeUint32List, typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList)
	}
	plan := typeddecode.Uint32ListPlan(certColumn)
	status := typeddecode.ValidateUint32OffsetsListDirectViewSections(typeddecode.Uint32OffsetsListDirectViewRequest{
		Plan:           plan,
		Certification:  certColumn,
		Rows:           state.RowCount,
		OffsetsBytes:   offsetsSection.Length,
		ValuesBytes:    valuesSection.Length,
		AssetOffset:    asset.Ref.Offset,
		HasAssetOffset: true,
	})
	if !status.Direct() {
		return raw, fmt.Errorf("direct-view section validation failed: %s", status.String())
	}
	return raw, nil
}

func validateColumnVectorIndexStateAdjacencySections(layer int, offsetsSection typedcolumn.ColumnPartImageSection, valuesSection typedcolumn.ColumnPartImageSection, offsetsRaw, valuesRaw []byte, rows int) error {
	if err := typedcolumn.ValidateRawUint32OffsetsListSections(offsetsSection, valuesSection, offsetsRaw, valuesRaw, rows); err != nil {
		return fmt.Errorf("collections: vector-index state adjacency layer %d shape: %w", layer, err)
	}
	for row := 0; row < rows; row++ {
		begin := binary.LittleEndian.Uint64(offsetsRaw[row*8:])
		end := binary.LittleEndian.Uint64(offsetsRaw[(row+1)*8:])
		beginInt := int(begin)
		endInt := int(end)
		for idx := beginInt; idx < endInt; idx++ {
			neighbor := binary.LittleEndian.Uint32(valuesRaw[idx*4:])
			if uint64(neighbor) >= uint64(rows) {
				return fmt.Errorf("collections: vector-index state adjacency layer %d row %d value[%d]=%d outside row_count=%d: %w", layer, row, idx-beginInt, neighbor, rows, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
		}
	}
	return nil
}

func columnVectorIndexStateAdjacencyOffsetsBytes(rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("negative rows=%d", rows)
	}
	if rows == math.MaxInt {
		return 0, errors.New("row_count+1 overflows int")
	}
	if rows > math.MaxInt/8-1 {
		return 0, fmt.Errorf("row_count=%d offsets byte count overflows int", rows)
	}
	return (rows + 1) * 8, nil
}

func validateColumnVectorIndexStateAdjacencyList(layer int, list typedcolumn.Uint32List) error {
	if err := typedcolumn.ValidateRawUint32OffsetsListShape(list.Rows, list.Offsets, uint64(len(list.Values))); err != nil {
		return fmt.Errorf("collections: vector-index state adjacency layer %d shape: %w", layer, err)
	}
	for row := 0; row < list.Rows; row++ {
		begin := int(list.Offsets[row])
		end := int(list.Offsets[row+1])
		for idx := begin; idx < end; idx++ {
			neighbor := list.Values[idx]
			if uint64(neighbor) >= uint64(list.Rows) {
				return fmt.Errorf("collections: vector-index state adjacency layer %d row %d value[%d]=%d outside row_count=%d: %w", layer, row, idx-begin, neighbor, list.Rows, errColumnVectorGraphAdjacencyOrdinalOutOfBounds)
			}
		}
	}
	return nil
}
