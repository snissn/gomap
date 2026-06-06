package collections

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

const (
	columnVectorGraphQuantizedCodesColumnName                = "codes"
	columnVectorGraphQuantizedPackedCodesColumnName          = "packed_codes"
	columnVectorGraphQuantizedCodeCountColumnName            = "code_count"
	columnVectorGraphQuantizedDotProductInvColumnName        = "quantized_dot_product_inv"
	columnVectorGraphQuantizedRabitQPathConfigHashFormat     = "%s_quantized_rabitq_1bit_%016x_%s"
	columnVectorGraphQuantizedScalarU8UnsupportedVersionText = "scalar_u8 version=%d is unsupported"
)

type columnVectorGraphPreparedQuantizedAsset struct {
	Definition QuantizedVectorIndexDefinition
	AssetID    string
	Config     ColumnStoreConfig
	Ref        ColumnAssetRef
	Bytes      int64
	Rows       int
	SchemaHash uint64
}

type columnVectorGraphQuantizedAssetHealth uint8

const (
	columnVectorGraphQuantizedAssetHealthUnknown columnVectorGraphQuantizedAssetHealth = iota
	columnVectorGraphQuantizedAssetHealthHeapCopy
	columnVectorGraphQuantizedAssetHealthMmapDirect
	columnVectorGraphQuantizedAssetHealthMissing
	columnVectorGraphQuantizedAssetHealthInvalid
	columnVectorGraphQuantizedAssetHealthStale
	columnVectorGraphQuantizedAssetHealthClosed
)

var (
	errColumnVectorGraphQuantizedAssetMissing = errors.New("collections: column_graph quantized asset missing")
	errColumnVectorGraphQuantizedAssetInvalid = errors.New("collections: column_graph quantized asset invalid")
	errColumnVectorGraphQuantizedAssetStale   = errors.New("collections: column_graph quantized asset stale")
	errColumnVectorGraphQuantizedAssetClosed  = errors.New("collections: column_graph quantized asset closed")
)

type columnVectorGraphQuantizedAssetLoadStatus struct {
	Definition    QuantizedVectorIndexDefinition
	Asset         columnVectorIndexStateAssetSnapshot
	Prepared      *quantizedasset.Prepared
	Err           error
	Health        columnVectorGraphQuantizedAssetHealth
	OpenNanos     uint64
	MappedBytes   uint64
	HeapCopyBytes uint64
	ActiveHandles int64
}

func columnVectorGraphQuantizedAssetID(q QuantizedVectorIndexDefinition) string {
	switch q.Codec {
	case rabitq.CodecName:
		return "quantized/" + q.Name + "/packed_codes"
	default:
		return "quantized/" + q.Name + "/codes"
	}
}

func columnVectorGraphQuantizedCodesAssetID(q QuantizedVectorIndexDefinition) string {
	return columnVectorGraphQuantizedAssetID(q)
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
	return columnVectorGraphPreparedQuantizedAsset{Definition: q, AssetID: columnVectorGraphQuantizedAssetID(q), Config: sourceCfg, Ref: ref, Bytes: ref.Length, Rows: len(rows), SchemaHash: sourceCfg.SchemaHash}, nil
}

func prepareColumnVectorGraphQuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	if partID == 0 {
		return nil, ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires non-zero part_id")
	}
	switch q.Codec {
	case QuantizedVectorCodecScalarU8:
		if q.Version != 1 {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q "+columnVectorGraphQuantizedScalarU8UnsupportedVersionText, q.Name, q.Version)
		}
		return prepareColumnVectorGraphScalarU8QuantizedCodesPayload(collection, base, def, q, partID, rows)
	case rabitq.CodecName:
		if q.Version != rabitq.CodecVersion {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q rabitq_1bit version=%d is unsupported", q.Name, q.Version)
		}
		return prepareColumnVectorGraphRabitQQuantizedCodesPayload(collection, base, def, q, partID, rows)
	default:
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q codec %q is unsupported", q.Name, q.Codec)
	}
}

func prepareColumnVectorGraphScalarU8QuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, base, def, q)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(len(rows), 8, "scalar_u8 primary_id"); err != nil {
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
			columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
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

func prepareColumnVectorGraphRabitQQuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	if def.Metric != VectorMetricCosine {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q metric %q is unsupported for rabitq_1bit", q.Name, def.Metric)
	}
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, base, def, q)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	rowCount := len(rows)
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 8, "rabitq_1bit primary_id"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	codesBytes, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, plan.BytesPerCode(), "rabitq_1bit codes")
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 4, "rabitq_1bit code_count"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 4, "rabitq_1bit quantized_dot_product_inv"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	primaryIDs := make([]int64, rowCount)
	codes := make([]byte, codesBytes)
	codeCounts := make([]uint32, rowCount)
	qdpInv := make([]float32, rowCount)
	var ws rabitq.Workspace
	var codeScratch []byte
	for rowIdx, row := range rows {
		primaryIDs[rowIdx] = int64(rowIdx)
		encoded, err := plan.Encode(codeScratch, row.Vector, &ws)
		if err != nil {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q rabitq_1bit row %d encode: %w", q.Name, rowIdx, err)
		}
		codeScratch = encoded.Code
		copy(codes[rowIdx*plan.BytesPerCode():(rowIdx+1)*plan.BytesPerCode()], encoded.Code)
		codeCounts[rowIdx] = encoded.CodeCount
		qdpInv[rowIdx] = encoded.QuantizedDotProductInv
	}
	packedRows, err := typedcolumn.NewPackedUintRows(rowCount, plan.CodeDimensions(), rabitq.CodeWidthBits, codes)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
			{
				Name:               columnVectorGraphQuantizedPackedCodesColumnName,
				Type:               typedcolumn.ColumnTypePackedBitVector,
				Encoding:           typedcolumn.EncodingRawPackedBitVector,
				FixedWidthElements: plan.CodeDimensions(),
				BitsPerElement:     rabitq.CodeWidthBits,
				Compression:        typedcolumn.CompressionNone,
				CompressionSet:     true,
				StatsDisabled:      true,
			},
			{
				Name:           columnVectorGraphQuantizedCodeCountColumnName,
				Type:           typedcolumn.ColumnTypeUint32,
				Encoding:       typedcolumn.EncodingRawUint32,
				Compression:    typedcolumn.CompressionNone,
				CompressionSet: true,
				StatsDisabled:  true,
			},
			{
				Name:           columnVectorGraphQuantizedDotProductInvColumnName,
				Type:           typedcolumn.ColumnTypeFloat32,
				Encoding:       typedcolumn.EncodingRawFloat32,
				Compression:    typedcolumn.CompressionNone,
				CompressionSet: true,
				StatsDisabled:  true,
			},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows: rowCount,
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
		},
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{
			columnVectorGraphQuantizedPackedCodesColumnName: packedRows,
		},
		Uint32Columns: map[string][]uint32{
			columnVectorGraphQuantizedCodeCountColumnName: codeCounts,
		},
		Float32Columns: map[string][]float32{
			columnVectorGraphQuantizedDotProductInvColumnName: qdpInv,
		},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
		LayoutLogicalTypes: map[string]string{
			columnVectorGraphQuantizedPackedCodesColumnName:   string(columnsemantics.LogicalPackedBitVector),
			columnVectorGraphQuantizedCodeCountColumnName:     string(columnsemantics.LogicalUint32),
			columnVectorGraphQuantizedDotProductInvColumnName: string(columnsemantics.LogicalFloat32),
		},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if image.Rows != rowCount || image.PartID != partID {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph rabitq quantized asset image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, rowCount, partID)
	}
	return image.Bytes, sourceCfg, nil
}

func columnVectorGraphQuantizedPrimaryIDColumnDefinition() typedcolumn.ColumnDefinition {
	return typedcolumn.ColumnDefinition{
		Name:           typedColumnAdapterPrimaryIDColumn,
		Type:           typedcolumn.ColumnTypeInt64,
		Encoding:       typedcolumn.EncodingRawInt64,
		Compression:    typedcolumn.CompressionNone,
		CompressionSet: true,
		StatsDisabled:  true,
	}
}

func columnVectorGraphQuantizedCodesColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition) (ColumnStoreConfig, error) {
	if !base.Enabled {
		return ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, errors.New("collections: column_graph quantized asset requires base asset manager")
	}
	var columns []ColumnStoreColumn
	switch q.Codec {
	case QuantizedVectorCodecScalarU8:
		if q.Version != 1 {
			return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q "+columnVectorGraphQuantizedScalarU8UnsupportedVersionText, q.Name, q.Version)
		}
		columns = []ColumnStoreColumn{{
			Name:        columnVectorGraphQuantizedCodesColumnName,
			Path:        def.Field + "_quantized_codes",
			Owner:       TypedStorageOwnerColumnPart,
			ValueType:   ColumnStoreValueByteVector,
			BytesPerRow: def.Dimensions,
		}}
	case rabitq.CodecName:
		if q.Version != rabitq.CodecVersion {
			return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q rabitq_1bit version=%d is unsupported", q.Name, q.Version)
		}
		plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
		if err != nil {
			return ColumnStoreConfig{}, err
		}
		cfgHash := rabitq.DefaultConfig().Hash64()
		columns = []ColumnStoreColumn{
			{
				Name:           columnVectorGraphQuantizedPackedCodesColumnName,
				Path:           fmt.Sprintf(columnVectorGraphQuantizedRabitQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedPackedCodesColumnName),
				Owner:          TypedStorageOwnerColumnPart,
				ValueType:      ColumnStoreValuePackedBitVector,
				ElementsPerRow: plan.CodeDimensions(),
				BitsPerElement: rabitq.CodeWidthBits,
			},
			{
				Name:               columnVectorGraphQuantizedCodeCountColumnName,
				Path:               fmt.Sprintf(columnVectorGraphQuantizedRabitQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedCodeCountColumnName),
				Owner:              TypedStorageOwnerColumnPart,
				ValueType:          ColumnStoreValueUint32,
				FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
			},
			{
				Name:               columnVectorGraphQuantizedDotProductInvColumnName,
				Path:               fmt.Sprintf(columnVectorGraphQuantizedRabitQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedDotProductInvColumnName),
				Owner:              TypedStorageOwnerColumnPart,
				ValueType:          ColumnStoreValueFloat32,
				FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
			},
		}
	default:
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q codec %q is unsupported", q.Name, q.Codec)
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled:         true,
		Columns:         columns,
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

func checkedColumnVectorGraphQuantizedRowBytes(rowCount, bytesPerRow int, label string) (int, error) {
	if rowCount < 0 || bytesPerRow < 0 {
		return 0, fmt.Errorf("collections: column_graph quantized asset %s negative row byte count rows=%d bytes_per_row=%d", label, rowCount, bytesPerRow)
	}
	if rowCount != 0 && bytesPerRow > math.MaxInt/rowCount {
		return 0, fmt.Errorf("collections: column_graph quantized asset %s bytes overflow rows=%d bytes_per_row=%d", label, rowCount, bytesPerRow)
	}
	return rowCount * bytesPerRow, nil
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

func columnVectorGraphQuantizedAssetStateType(q QuantizedVectorIndexDefinition) (logicalType, physicalEncoding string) {
	switch q.Codec {
	case rabitq.CodecName:
		return columnVectorIndexStateLogicalTypePackedBitVector, columnVectorIndexStateEncodingRawPackedBitVector
	default:
		return columnVectorIndexStateLogicalTypeByteVector, columnVectorIndexStateEncodingRawFixedBytes
	}
}

func columnVectorGraphQuantizedAssetSnapshotsFromPrepared(prepared []columnVectorGraphPreparedQuantizedAsset) []columnVectorIndexStateAssetSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, len(prepared))
	for i, prepared := range prepared {
		logicalType, physicalEncoding := columnVectorGraphQuantizedAssetStateType(prepared.Definition)
		assets[i] = columnVectorIndexStateAssetSnapshot{
			Role:             columnVectorIndexStateAssetRoleQuantizedCodes,
			AssetID:          prepared.AssetID,
			LogicalType:      logicalType,
			PhysicalEncoding: physicalEncoding,
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
		wantLogical, wantEncoding := columnVectorGraphQuantizedAssetStateType(q)
		if asset.LogicalType != wantLogical || asset.PhysicalEncoding != wantEncoding {
			return fmt.Errorf("collections: vector-index state quantized asset %q type/encoding=(%q,%q) want (%q,%q)", q.Name, asset.LogicalType, asset.PhysicalEncoding, wantLogical, wantEncoding)
		}
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
			status.Health = columnVectorGraphQuantizedAssetHealthMissing
			status.Err = fmt.Errorf("%w: quantized asset %q is missing", errColumnVectorGraphQuantizedAssetMissing, q.Name)
			out[q.Name] = status
			continue
		}
		status.Asset = asset
		start := time.Now()
		prepared, err := loadColumnVectorGraphQuantizedAsset(rootDir, collection, cfg, def, graph, q, asset)
		status.OpenNanos = uint64(time.Since(start).Nanoseconds())
		if err != nil {
			status.Err = err
			status.Health = columnVectorGraphQuantizedAssetHealthFromError(err)
		} else {
			status.Prepared = prepared
			status.Health = columnVectorGraphQuantizedAssetHealthHeapCopy
			if asset.AssetBytes > 0 {
				status.HeapCopyBytes = uint64(asset.AssetBytes)
			} else if prepared != nil {
				fp := prepared.Footprint()
				if fp.AssetBytes > 0 {
					status.HeapCopyBytes = uint64(fp.AssetBytes)
				}
			}
		}
		out[q.Name] = status
	}
	return out
}

func columnVectorGraphQuantizedAssetHealthFromError(err error) columnVectorGraphQuantizedAssetHealth {
	switch {
	case errors.Is(err, errColumnVectorGraphQuantizedAssetMissing):
		return columnVectorGraphQuantizedAssetHealthMissing
	case errors.Is(err, errColumnVectorGraphQuantizedAssetStale):
		return columnVectorGraphQuantizedAssetHealthStale
	case errors.Is(err, errColumnVectorGraphQuantizedAssetClosed):
		return columnVectorGraphQuantizedAssetHealthClosed
	case errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid):
		return columnVectorGraphQuantizedAssetHealthInvalid
	default:
		return columnVectorGraphQuantizedAssetHealthInvalid
	}
}

func (r *columnVectorGraphPhysicalRowReader) populateQuantizedAssetSearchStats(indexName string, stats *columnVectorGraphNativeSearchStats) {
	if stats == nil || r == nil {
		return
	}
	status, ok := r.quantizedAssetStatus[indexName]
	if !ok {
		status = columnVectorGraphQuantizedAssetLoadStatus{
			Definition: QuantizedVectorIndexDefinition{Name: indexName},
			Health:     columnVectorGraphQuantizedAssetHealthMissing,
			Err:        fmt.Errorf("%w: quantized asset %q is not loaded", errColumnVectorGraphQuantizedAssetMissing, indexName),
		}
	}
	status.populateSearchStats(stats)
}

func (s columnVectorGraphQuantizedAssetLoadStatus) populateSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if stats == nil {
		return
	}
	stats.QuantizedAssetOpenNanos = s.OpenNanos
	stats.QuantizedAssetMappedBytes = s.MappedBytes
	stats.QuantizedAssetHeapCopyBytes = s.HeapCopyBytes
	stats.QuantizedAssetActiveHandles = s.ActiveHandles
	if s.Prepared != nil && s.Err == nil {
		switch s.Health {
		case columnVectorGraphQuantizedAssetHealthMmapDirect:
			stats.QuantizedAssetMmapDirect = 1
		default:
			stats.QuantizedAssetHeapCopy = 1
		}
		return
	}
	health := s.Health
	if health == columnVectorGraphQuantizedAssetHealthUnknown {
		if s.Err == nil {
			health = columnVectorGraphQuantizedAssetHealthMissing
		} else {
			health = columnVectorGraphQuantizedAssetHealthFromError(s.Err)
		}
	}
	recordColumnVectorGraphQuantizedAssetUnavailable(stats, health)
}

func recordColumnVectorGraphQuantizedAssetUnavailable(stats *columnVectorGraphNativeSearchStats, health columnVectorGraphQuantizedAssetHealth) {
	if stats == nil {
		return
	}
	stats.QuantizedAssetMmapDirect = 0
	stats.QuantizedAssetHeapCopy = 0
	stats.QuantizedAssetMappedBytes = 0
	stats.QuantizedAssetHeapCopyBytes = 0
	stats.QuantizedAssetActiveHandles = 0
	stats.QuantizedAssetMissing = 0
	stats.QuantizedAssetInvalid = 0
	stats.QuantizedAssetStale = 0
	stats.QuantizedAssetClosed = 0
	stats.QuantizedAssetUnavailable = 1
	switch health {
	case columnVectorGraphQuantizedAssetHealthStale:
		stats.QuantizedAssetStale = 1
	case columnVectorGraphQuantizedAssetHealthClosed:
		stats.QuantizedAssetClosed = 1
	case columnVectorGraphQuantizedAssetHealthInvalid:
		stats.QuantizedAssetInvalid = 1
	default:
		stats.QuantizedAssetMissing = 1
	}
}

func recordColumnVectorGraphQuantizedAssetErrorStats(stats *columnVectorGraphNativeSearchStats, err error) {
	if stats == nil || err == nil {
		return
	}
	if !errors.Is(err, errColumnVectorGraphQuantizedAssetMissing) && !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) && !errors.Is(err, errColumnVectorGraphQuantizedAssetStale) && !errors.Is(err, errColumnVectorGraphQuantizedAssetClosed) {
		return
	}
	recordColumnVectorGraphQuantizedAssetUnavailable(stats, columnVectorGraphQuantizedAssetHealthFromError(err))
}

func loadColumnVectorGraphQuantizedAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot) (*quantizedasset.Prepared, error) {
	wantLogical, wantEncoding := columnVectorGraphQuantizedAssetStateType(q)
	if asset.LogicalType != wantLogical || asset.PhysicalEncoding != wantEncoding {
		return nil, fmt.Errorf("%w: quantized asset %q type/encoding=(%q,%q) want (%q,%q)", errColumnVectorGraphQuantizedAssetStale, q.Name, asset.LogicalType, asset.PhysicalEncoding, wantLogical, wantEncoding)
	}
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, cfg, def, q)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q config: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, fmt.Errorf("%w: quantized asset %q schema_hash=%d want %d", errColumnVectorGraphQuantizedAssetStale, q.Name, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if asset.RowCount != graph.RowCount {
		return nil, fmt.Errorf("%w: quantized asset %q row_count=%d want graph row_count=%d", errColumnVectorGraphQuantizedAssetStale, q.Name, asset.RowCount, graph.RowCount)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q unavailable: %v", errColumnVectorGraphQuantizedAssetMissing, q.Name, err)
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q read: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q parse typed-column image: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	ref := columnVectorGraphQuantizedAssetRefIdentity(asset.Ref)
	schema, err := columnVectorGraphQuantizedAssetSchema(def, graph, q, asset, ref)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q schema: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	prepared, err := quantizedasset.Prepare(quantizedasset.PrepareRequest{
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
			RequiredRoles:    columnVectorGraphQuantizedRequiredRoles(q),
		},
		Parts: []quantizedasset.PartImageSource{{Image: image, Ref: ref, AssetBytes: asset.AssetBytes, SourceSchemaHash: asset.SourceSchemaHash}},
	})
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q prepare: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	if err := validateColumnVectorGraphQuantizedPreparedAsset(def, q, prepared); err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q validate: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	return prepared, nil
}

func columnVectorGraphQuantizedRequiredRoles(q QuantizedVectorIndexDefinition) []quantizedasset.Role {
	switch q.Codec {
	case rabitq.CodecName:
		return []quantizedasset.Role{quantizedasset.RolePackedCodes, quantizedasset.RoleCodeCount, quantizedasset.RoleQuantizedDotProductInv}
	default:
		return []quantizedasset.Role{quantizedasset.RoleCodes}
	}
}

func validateColumnVectorGraphQuantizedPreparedAsset(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, prepared *quantizedasset.Prepared) error {
	if prepared == nil {
		return errors.New("prepared asset is nil")
	}
	if prepared.Rows() < 0 {
		return fmt.Errorf("prepared rows=%d", prepared.Rows())
	}
	if q.Codec != rabitq.CodecName {
		return nil
	}
	plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
	if err != nil {
		return err
	}
	if prepared.Rows() == 0 {
		return nil
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		return errors.New("rabitq_1bit packed code row view unavailable")
	}
	if codeRows.Rows() != prepared.Rows() || codeRows.BytesPerRow() != plan.BytesPerCode() || codeRows.ElementsPerRow() != plan.CodeDimensions() {
		return fmt.Errorf("rabitq_1bit packed code shape rows/bytes/elements=(%d,%d,%d) want (%d,%d,%d)", codeRows.Rows(), codeRows.BytesPerRow(), codeRows.ElementsPerRow(), prepared.Rows(), plan.BytesPerCode(), plan.CodeDimensions())
	}
	for ordinal := 0; ordinal < prepared.Rows(); ordinal++ {
		code, ok := codeRows.RowBytes(ordinal)
		if !ok {
			return fmt.Errorf("rabitq_1bit code row ordinal=%d unavailable", ordinal)
		}
		count, ok := prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok {
			return fmt.Errorf("rabitq_1bit code_count ordinal=%d unavailable", ordinal)
		}
		if err := plan.ValidateCode(code, count); err != nil {
			return fmt.Errorf("rabitq_1bit code ordinal=%d: %w", ordinal, err)
		}
		qdp, ok := prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok {
			return fmt.Errorf("rabitq_1bit quantized_dot_product_inv ordinal=%d unavailable", ordinal)
		}
		if err := plan.ValidateQuantizedDotProductInv(qdp); err != nil {
			return fmt.Errorf("rabitq_1bit quantized_dot_product_inv ordinal=%d: %w", ordinal, err)
		}
	}
	return nil
}

func columnVectorGraphQuantizedAssetSchema(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot, ref quantizedasset.AssetRefIdentity) (quantizedasset.SchemaDescriptor, error) {
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
	schema := quantizedasset.SchemaDescriptor{
		Name:             q.Name,
		Metric:           def.Metric.String(),
		VectorDimensions: def.Dimensions,
		RowCount:         graph.RowCount,
		OrdinalOrder:     quantizedasset.GraphOrdinalOrderVectorOrdinal,
		BaseGraph:        base,
	}
	switch q.Codec {
	case QuantizedVectorCodecScalarU8:
		if q.Version != 1 {
			return quantizedasset.SchemaDescriptor{}, fmt.Errorf(columnVectorGraphQuantizedScalarU8UnsupportedVersionText, q.Version)
		}
		schema.CodeDimensions = def.Dimensions
		schema.CodeWidthBits = 8
		schema.Codec = quantizedasset.CodecDescriptor{Name: q.Codec, Version: q.Version}
		schema.Columns = []quantizedasset.ColumnDescriptor{{
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
		}}
	case rabitq.CodecName:
		if q.Version != rabitq.CodecVersion {
			return quantizedasset.SchemaDescriptor{}, fmt.Errorf("rabitq_1bit version=%d is unsupported", q.Version)
		}
		plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
		if err != nil {
			return quantizedasset.SchemaDescriptor{}, err
		}
		cfg := rabitq.DefaultConfig()
		schema.CodeDimensions = plan.CodeDimensions()
		schema.CodeWidthBits = rabitq.CodeWidthBits
		schema.Codec = quantizedasset.CodecDescriptor{Name: rabitq.CodecName, Version: rabitq.CodecVersion, ConfigHash: cfg.Hash64(), Config: cfg.CanonicalBytes()}
		schema.Columns = []quantizedasset.ColumnDescriptor{
			{
				Role:             quantizedasset.RolePackedCodes,
				Column:           columnVectorGraphQuantizedPackedCodesColumnName,
				Required:         true,
				LogicalType:      string(columnsemantics.LogicalPackedBitVector),
				Type:             typedcolumn.ColumnTypePackedBitVector,
				Encoding:         typedcolumn.EncodingRawPackedBitVector,
				ElementsPerRow:   plan.CodeDimensions(),
				BytesPerRow:      plan.BytesPerCode(),
				BitsPerElement:   rabitq.CodeWidthBits,
				SourceSchemaHash: asset.SourceSchemaHash,
				AssetBytes:       asset.AssetBytes,
				Ref:              ref,
			},
			{
				Role:             quantizedasset.RoleCodeCount,
				Column:           columnVectorGraphQuantizedCodeCountColumnName,
				Required:         true,
				LogicalType:      string(columnsemantics.LogicalUint32),
				Type:             typedcolumn.ColumnTypeUint32,
				Encoding:         typedcolumn.EncodingRawUint32,
				SourceSchemaHash: asset.SourceSchemaHash,
				AssetBytes:       asset.AssetBytes,
				Ref:              ref,
			},
			{
				Role:             quantizedasset.RoleQuantizedDotProductInv,
				Column:           columnVectorGraphQuantizedDotProductInvColumnName,
				Required:         true,
				LogicalType:      string(columnsemantics.LogicalFloat32),
				Type:             typedcolumn.ColumnTypeFloat32,
				Encoding:         typedcolumn.EncodingRawFloat32,
				SourceSchemaHash: asset.SourceSchemaHash,
				AssetBytes:       asset.AssetBytes,
				Ref:              ref,
			},
		}
	default:
		return quantizedasset.SchemaDescriptor{}, fmt.Errorf("codec %q is unsupported", q.Codec)
	}
	return schema, nil
}
