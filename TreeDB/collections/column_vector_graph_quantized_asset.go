package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const columnVectorGraphQuantizedCodesColumnName = "codes"

type columnVectorGraphPreparedQuantizedAsset struct {
	Definition QuantizedVectorIndexDefinition
	AssetID    string
	Config     ColumnStoreConfig
	Ref        ColumnAssetRef
	Bytes      int64
	Rows       int
	SchemaHash uint64
}

type columnVectorGraphQuantizedAssetLoadStatus struct {
	Definition QuantizedVectorIndexDefinition
	Asset      columnVectorIndexStateAssetSnapshot
	Prepared   *quantizedasset.Prepared
	Err        error
}

func columnVectorGraphQuantizedCodesAssetID(q QuantizedVectorIndexDefinition) string {
	return "quantized/" + q.Name + "/codes"
}

func columnVectorIndexStateAssetIsQuantized(asset columnVectorIndexStateAssetSnapshot) bool {
	return asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes
}

func columnVectorGraphQuantizedAssetRefIdentity(ref ColumnAssetRef) quantizedasset.AssetRefIdentity {
	return quantizedasset.AssetRefIdentity{Present: true, Kind: string(ref.Kind), Namespace: ref.Namespace, Generation: ref.Generation, PartID: ref.PartID, FileID: ref.FileID, Offset: ref.Offset, Length: ref.Length, Checksum: ref.Checksum}
}

func prepareColumnVectorGraphQuantizedAssets(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedQuantizedAsset, error) {
	if len(def.QuantizedIndexes) == 0 {
		return nil, nil
	}
	if assetRootDir == "" {
		return nil, errors.New("collections: column_graph quantized assets require asset root dir")
	}
	if generation == 0 || firstPartID == 0 {
		return nil, errors.New("collections: column_graph quantized assets require generation and first part_id")
	}
	out := make([]columnVectorGraphPreparedQuantizedAsset, 0, len(def.QuantizedIndexes))
	partID := firstPartID
	for _, q := range def.QuantizedIndexes {
		prepared, err := prepareColumnVectorGraphQuantizedAsset(assetRootDir, collection, base, def, graph, q, generation, partID, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, prepared)
		partID = nextColumnVectorGraphPartIDAfter(partID, prepared.Ref.PartID)
	}
	return out, nil
}

func prepareColumnVectorGraphQuantizedAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedQuantizedAsset, error) {
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 {
		return columnVectorGraphPreparedQuantizedAsset{}, fmt.Errorf("collections: column_graph quantized index %q codec/version=(%q,%d) is not scalar_u8 v1", q.Name, q.Codec, q.Version)
	}
	payload, sourceCfg, err := prepareColumnVectorGraphQuantizedCodesPayload(collection, base, def, q, partID, rows)
	if err != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, err
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(assetRootDir, sourceCfg)
	if err != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, err
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, alignment)
	closeErr := appender.close()
	if appendErr != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, errors.Join(appendErr, closeErr)
	}
	if closeErr != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, closeErr
	}
	if ref.Namespace != sourceCfg.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != partID || ref.Length != int64(len(payload)) {
		return columnVectorGraphPreparedQuantizedAsset{}, fmt.Errorf("collections: invalid column_graph quantized asset ref %+v", ref)
	}
	return columnVectorGraphPreparedQuantizedAsset{Definition: q, AssetID: columnVectorGraphQuantizedCodesAssetID(q), Config: sourceCfg, Ref: ref, Bytes: ref.Length, Rows: len(rows), SchemaHash: sourceCfg.SchemaHash}, nil
}

func prepareColumnVectorGraphQuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	if partID == 0 {
		return nil, ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires non-zero part_id")
	}
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, base, def, q)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	primaryIDs := make([]int64, len(rows))
	for rowIdx := range primaryIDs {
		primaryIDs[rowIdx] = int64(rowIdx)
	}
	codes, err := buildColumnVectorGraphScalarU8Codes(def, rows)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	fixedRows, err := typedcolumn.NewFixedBytesRows(len(rows), def.Dimensions, codes)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
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
			{
				Name:               columnVectorGraphQuantizedCodesColumnName,
				Type:               typedcolumn.ColumnTypeFixedBytes,
				Encoding:           typedcolumn.EncodingRawFixedBytes,
				FixedWidthElements: def.Dimensions,
				Compression:        typedcolumn.CompressionNone,
				CompressionSet:     true,
				StatsDisabled:      true,
			},
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
		FixedBytesColumns: map[string]typedcolumn.FixedBytesRows{
			columnVectorGraphQuantizedCodesColumnName: fixedRows,
		},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
		LayoutLogicalTypes: map[string]string{columnVectorGraphQuantizedCodesColumnName: string(columnsemantics.LogicalByteVector)},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if image.Rows != len(rows) || image.PartID != partID {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized asset image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, len(rows), partID)
	}
	return image.Bytes, sourceCfg, nil
}

func columnVectorGraphQuantizedCodesColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition) (ColumnStoreConfig, error) {
	if !base.Enabled {
		return ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires base asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:        columnVectorGraphQuantizedCodesColumnName,
			Path:        def.Field + "_quantized_codes",
			Owner:       TypedStorageOwnerColumnPart,
			ValueType:   ColumnStoreValueByteVector,
			BytesPerRow: def.Dimensions,
		}},
		RetainedPayload: ColumnRetainedPayloadNone,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      base.AssetManager.Kind,
			Namespace: base.AssetManager.Namespace,
		},
	})
	if err != nil {
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q typed-column config: %w", q.Name, err)
	}
	return *cfg, nil
}

func buildColumnVectorGraphScalarU8Codes(def VectorIndexDefinition, rows []columnVectorGraphAssetRow) ([]byte, error) {
	if def.Dimensions <= 0 {
		return nil, errors.New("collections: column_graph quantized asset dimensions must be positive")
	}
	if len(rows) != 0 && def.Dimensions > math.MaxInt/len(rows) {
		return nil, errors.New("collections: column_graph quantized asset codes bytes overflow")
	}
	codes := make([]byte, 0, len(rows)*def.Dimensions)
	for rowIdx, row := range rows {
		if len(row.Vector) != def.Dimensions {
			return nil, fmt.Errorf("collections: column_graph quantized asset row %d vector dimensions=%d want %d", rowIdx, len(row.Vector), def.Dimensions)
		}
		if def.Metric == VectorMetricCosine && (row.InvNorm <= 0 || math.IsNaN(float64(row.InvNorm)) || math.IsInf(float64(row.InvNorm), 0)) {
			return nil, fmt.Errorf("collections: column_graph quantized asset row %d inverse norm is invalid", rowIdx)
		}
		for _, value := range row.Vector {
			codeValue := value
			if def.Metric == VectorMetricCosine {
				codeValue *= row.InvNorm
			}
			codes = append(codes, columnVectorGraphScalarU8Code(codeValue))
		}
	}
	return codes, nil
}

func columnVectorGraphScalarU8Code(value float32) byte {
	if math.IsNaN(float64(value)) {
		return 0
	}
	scaled := math.Round((float64(value) + 1.0) * 127.5)
	if scaled < 0 {
		return 0
	}
	if scaled > 255 {
		return 255
	}
	return byte(scaled)
}

func columnVectorGraphQuantizedAssetSnapshotsFromPrepared(prepared []columnVectorGraphPreparedQuantizedAsset) []columnVectorIndexStateAssetSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, len(prepared))
	for i, prepared := range prepared {
		assets[i] = columnVectorIndexStateAssetSnapshot{
			Role:             columnVectorIndexStateAssetRoleQuantizedCodes,
			AssetID:          prepared.AssetID,
			LogicalType:      columnVectorIndexStateLogicalTypeByteVector,
			PhysicalEncoding: columnVectorIndexStateEncodingRawFixedBytes,
			RowCount:         prepared.Rows,
			SourceSchemaHash: prepared.SchemaHash,
			Ref:              prepared.Ref,
			AssetBytes:       prepared.Bytes,
		}
	}
	return assets
}

func columnVectorGraphQuantizedAssetByName(state columnVectorIndexStateSnapshot, def VectorIndexDefinition) map[string]columnVectorIndexStateAssetSnapshot {
	out := make(map[string]columnVectorIndexStateAssetSnapshot, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		wantID := columnVectorGraphQuantizedCodesAssetID(q)
		for _, asset := range state.Assets {
			if asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes && asset.AssetID == wantID {
				out[q.Name] = asset
				break
			}
		}
	}
	return out
}

func columnVectorGraphQuantizedStateAssetIDSetMatches(def VectorIndexDefinition, state columnVectorIndexStateSnapshot) bool {
	expected := make(map[string]struct{}, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		expected[columnVectorGraphQuantizedCodesAssetID(q)] = struct{}{}
	}
	seen := make(map[string]struct{}, len(expected))
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes {
			continue
		}
		if _, ok := expected[asset.AssetID]; !ok {
			return false
		}
		if _, ok := seen[asset.AssetID]; ok {
			return false
		}
		seen[asset.AssetID] = struct{}{}
	}
	return len(seen) == len(expected)
}

func validateColumnVectorGraphQuantizedStateAssets(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot) error {
	expected := make(map[string]QuantizedVectorIndexDefinition, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		expected[columnVectorGraphQuantizedCodesAssetID(q)] = q
	}
	seen := make(map[string]struct{}, len(expected))
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes {
			continue
		}
		q, ok := expected[asset.AssetID]
		if !ok {
			return fmt.Errorf("collections: vector-index state unexpected quantized asset id=%q", asset.AssetID)
		}
		if _, ok := seen[asset.AssetID]; ok {
			return fmt.Errorf("collections: vector-index state duplicate quantized asset id=%q", asset.AssetID)
		}
		seen[asset.AssetID] = struct{}{}
		sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, cfg, def, q)
		if err != nil {
			return err
		}
		if asset.SourceSchemaHash != sourceCfg.SchemaHash {
			return fmt.Errorf("collections: vector-index state quantized asset %q schema_hash=%d want %d", q.Name, asset.SourceSchemaHash, sourceCfg.SchemaHash)
		}
	}
	for assetID, q := range expected {
		if _, ok := seen[assetID]; !ok {
			return fmt.Errorf("collections: vector-index state missing quantized asset %q", q.Name)
		}
	}
	return nil
}

func (c *Collection) prepareColumnVectorGraphQuantizedAssetsForReader(graphReader *columnVectorGraphPhysicalRowReader, view columnPhysicalScanSnapshotView) {
	if c == nil || c.db == nil || graphReader == nil || graphReader.catalog == nil || graphReader.catalog.meta.Options.ColumnStore == nil || len(graphReader.def.QuantizedIndexes) == 0 || !view.VectorIndexStateFound {
		return
	}
	graphReader.quantizedAssetStatus = loadColumnVectorGraphQuantizedAssetsForReader(c.db.ColumnAssetRootDir(), graphReader.catalog.meta.Name, *graphReader.catalog.meta.Options.ColumnStore, graphReader.def, graphReader.graph, view.VectorIndexState)
}

func loadColumnVectorGraphQuantizedAssetsForReader(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) map[string]columnVectorGraphQuantizedAssetLoadStatus {
	if len(def.QuantizedIndexes) == 0 {
		return nil
	}
	byName := columnVectorGraphQuantizedAssetByName(state, def)
	out := make(map[string]columnVectorGraphQuantizedAssetLoadStatus, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		status := columnVectorGraphQuantizedAssetLoadStatus{Definition: q}
		asset, ok := byName[q.Name]
		if !ok {
			status.Err = fmt.Errorf("quantized asset %q is missing", q.Name)
			out[q.Name] = status
			continue
		}
		status.Asset = asset
		prepared, err := loadColumnVectorGraphQuantizedAsset(rootDir, collection, cfg, def, graph, q, asset)
		if err != nil {
			status.Err = err
		} else {
			status.Prepared = prepared
		}
		out[q.Name] = status
	}
	return out
}

func loadColumnVectorGraphQuantizedAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot) (*quantizedasset.Prepared, error) {
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, cfg, def, q)
	if err != nil {
		return nil, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, fmt.Errorf("quantized asset %q schema_hash=%d want %d", q.Name, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return nil, fmt.Errorf("quantized asset %q unavailable: %w", q.Name, err)
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return nil, fmt.Errorf("quantized asset %q read: %w", q.Name, err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, fmt.Errorf("quantized asset %q parse typed-column image: %w", q.Name, err)
	}
	ref := columnVectorGraphQuantizedAssetRefIdentity(asset.Ref)
	schema := columnVectorGraphQuantizedAssetSchema(def, graph, q, asset, ref)
	return quantizedasset.Prepare(quantizedasset.PrepareRequest{
		Schema: schema,
		Expected: quantizedasset.ExpectedSchema{
			Metric:           schema.Metric,
			VectorDimensions: schema.VectorDimensions,
			CodeDimensions:   schema.CodeDimensions,
			CodeWidthBits:    schema.CodeWidthBits,
			RowCount:         schema.RowCount,
			OrdinalOrder:     schema.OrdinalOrder,
			Codec:            schema.Codec,
			BaseGraph:        schema.BaseGraph,
			RequiredRoles:    []quantizedasset.Role{quantizedasset.RoleCodes},
		},
		Parts: []quantizedasset.PartImageSource{{Image: image, Ref: ref, AssetBytes: asset.AssetBytes, SourceSchemaHash: asset.SourceSchemaHash}},
	})
}

func columnVectorGraphQuantizedAssetSchema(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot, ref quantizedasset.AssetRefIdentity) quantizedasset.SchemaDescriptor {
	base := quantizedasset.BaseGraphIdentity{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric.String(),
		Dimensions:             def.Dimensions,
		RowCount:               graph.RowCount,
		BaseManifestGeneration: graph.BaseManifestGeneration,
		BaseManifestChecksum:   graph.BaseManifestChecksum,
		BaseSchemaHash:         graph.BaseSchemaHash,
		GraphSchemaHash:        graph.GraphSchemaHash,
	}
	return quantizedasset.SchemaDescriptor{
		Name:             q.Name,
		Metric:           def.Metric.String(),
		VectorDimensions: def.Dimensions,
		CodeDimensions:   def.Dimensions,
		CodeWidthBits:    8,
		RowCount:         graph.RowCount,
		OrdinalOrder:     quantizedasset.GraphOrdinalOrderVectorOrdinal,
		Codec:            quantizedasset.CodecDescriptor{Name: q.Codec, Version: q.Version},
		BaseGraph:        base,
		Columns: []quantizedasset.ColumnDescriptor{{
			Role:             quantizedasset.RoleCodes,
			Column:           columnVectorGraphQuantizedCodesColumnName,
			Required:         true,
			LogicalType:      string(columnsemantics.LogicalByteVector),
			Type:             typedcolumn.ColumnTypeFixedBytes,
			Encoding:         typedcolumn.EncodingRawFixedBytes,
			BytesPerRow:      def.Dimensions,
			SourceSchemaHash: asset.SourceSchemaHash,
			AssetBytes:       asset.AssetBytes,
			Ref:              ref,
		}},
	}
}
