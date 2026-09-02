package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/golang/snappy"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnVectorIndexStateRecordPrefix = "\x06vector-index-state/v1/index/"
	columnVectorIndexStateMagic        = uint32(0x54564953) // TVIS
	columnVectorIndexStateVersionV1    = uint16(1)
	columnVectorIndexStateVersionV2    = uint16(2)
	columnVectorIndexStateVersion      = uint16(3)

	// Vector-index state is a manifest control record and must remain inline.
	// Reserve ample leaf-entry/key/revision headroom instead of relying on the
	// exact layout of one current leaf encoding.
	columnVectorIndexStateMaxInlineRecordBytes = page.PageSize - page.PageHeaderSize - 256
	columnVectorIndexStateMaxDecodedBytes      = 1 << 20

	columnVectorIndexStateAssetRoleAdjacency         = "adjacency"
	columnVectorIndexStateAssetRoleInverseNorm       = "inverse_norm"
	columnVectorIndexStateAssetRoleNormalizedVectors = "normalized_vectors"
	columnVectorIndexStateAssetRoleRowRefs           = "row_refs"
	columnVectorIndexStateAssetRoleDocumentIDs       = "document_ids"
	columnVectorIndexStateAssetRoleQuantizedCodes    = "quantized_codes"
	columnVectorIndexStateAssetRoleQuantizedAlpha    = "quantized_alpha"
	columnVectorIndexStateAssetRoleHNSWSearchPack    = "hnsw_search_pack"
	columnVectorIndexStateHNSWSearchPackAssetID      = "hnsw_search_pack_v1"

	columnVectorIndexStateLogicalTypeUint32List      = "uint32_list"
	columnVectorIndexStateLogicalTypeInt64           = "int64"
	columnVectorIndexStateLogicalTypeFloat32         = "float32"
	columnVectorIndexStateLogicalTypeFloat32Vector   = "float32_vector"
	columnVectorIndexStateLogicalTypeBytes           = "bytes"
	columnVectorIndexStateLogicalTypeByteVector      = "byte_vector"
	columnVectorIndexStateLogicalTypePackedBitVector = "packed_bit_vector"
	columnVectorIndexStateLogicalTypeScalarU8Alpha   = "scalar_u8_alpha"
	columnVectorIndexStateLogicalTypeSearchPack      = "hnsw_search_pack"
	columnVectorIndexStateEncodingRawUint32List      = "raw_uint32_offsets_list"
	columnVectorIndexStateEncodingRawInt64           = "raw_int64"
	columnVectorIndexStateEncodingRawFloat32         = "raw_float32"
	columnVectorIndexStateEncodingRawFloat32Vector   = "raw_float32_vector"
	columnVectorIndexStateEncodingRawBytesOffsets    = "raw_bytes_offsets"
	columnVectorIndexStateEncodingRawFixedBytes      = "raw_fixed_bytes"
	columnVectorIndexStateEncodingRawPackedBitVector = "raw_packed_bit_vector"
	columnVectorIndexStateEncodingRawFloat32Uint32   = "raw_float32_uint32"
	columnVectorIndexStateEncodingHNSWSearchPackV1   = "hnsw_search_pack_v1"
)

var columnVectorIndexStateRecordPrefixBytes = []byte(columnVectorIndexStateRecordPrefix)

// columnVectorIndexStateSnapshot is the vector-index control record for derived
// search state. It intentionally lives outside the base field/part manifest
// records and references typed-column assets by generic logical type plus
// physical encoding. Current column_graph row-asset records remain a legacy
// compatibility/search fallback until follow-up issues publish and consume all
// derived state through this layer.
type columnVectorIndexStateSnapshot struct {
	IndexName              string
	Field                  string
	Metric                 VectorMetric
	Encoding               VectorIndexEncoding
	Dimensions             int
	M                      int
	EfConstruction         int
	EfSearch               int
	RowCount               int
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	AdjacencyLayerCount    int
	Assets                 []columnVectorIndexStateAssetSnapshot
}

// columnVectorIndexStateAssetSnapshot identifies one typed-column asset owned by
// vector-index state, such as HNSW adjacency, inverse norms, optional normalized
// vectors, or row/document references. Role and AssetID are consumer metadata;
// LogicalType and PhysicalEncoding are the datastore contract.
type columnVectorIndexStateAssetSnapshot struct {
	Role             string
	AssetID          string
	LogicalType      string
	PhysicalEncoding string
	RowCount         int
	SourceSchemaHash uint64
	Ref              ColumnAssetRef
	AssetBytes       int64
}

func columnVectorIndexStateRecordKey(indexName string) []byte {
	key := make([]byte, len(columnVectorIndexStateRecordPrefix)+len(indexName))
	copy(key, columnVectorIndexStateRecordPrefix)
	copy(key[len(columnVectorIndexStateRecordPrefix):], indexName)
	return key
}

func findColumnVectorIndexStateRecord(records []columnManifestRecord, indexName string) (columnManifestRecord, bool) {
	key := columnVectorIndexStateRecordKey(indexName)
	for _, record := range records {
		if bytes.Equal(record.key, key) {
			return record, true
		}
	}
	return columnManifestRecord{}, false
}

func encodeColumnVectorIndexStateRecord(snapshot columnVectorIndexStateSnapshot) ([]byte, error) {
	var err error
	snapshot, err = normalizeColumnVectorIndexStateSnapshotForEncode(snapshot)
	if err != nil {
		return nil, err
	}
	if err := validateColumnVectorIndexStateSnapshot(snapshot); err != nil {
		return nil, err
	}
	raw := encodeColumnVectorIndexStateRecordV2(snapshot)
	if len(raw) <= columnVectorIndexStateMaxInlineRecordBytes {
		return raw, nil
	}
	if len(raw) > columnVectorIndexStateMaxDecodedBytes {
		return nil, fmt.Errorf(
			"collections: vector-index state decoded bytes=%d exceed limit=%d",
			len(raw),
			columnVectorIndexStateMaxDecodedBytes,
		)
	}
	compressed := snappy.Encode(nil, raw)
	var b bytes.Buffer
	writeManifestUint32(&b, columnVectorIndexStateMagic)
	writeManifestUint16(&b, columnVectorIndexStateVersion)
	writeManifestUint64(&b, uint64(len(raw)))
	_, _ = b.Write(compressed)
	if b.Len() > columnVectorIndexStateMaxInlineRecordBytes {
		return nil, fmt.Errorf(
			"collections: compressed vector-index state bytes=%d exceed inline manifest limit=%d (decoded=%d)",
			b.Len(),
			columnVectorIndexStateMaxInlineRecordBytes,
			len(raw),
		)
	}
	return b.Bytes(), nil
}

func encodeColumnVectorIndexStateRecordV2(snapshot columnVectorIndexStateSnapshot) []byte {
	var b bytes.Buffer
	writeManifestUint32(&b, columnVectorIndexStateMagic)
	writeManifestUint16(&b, columnVectorIndexStateVersionV2)
	writeManifestString(&b, snapshot.IndexName)
	writeManifestString(&b, snapshot.Field)
	writeManifestString(&b, snapshot.Metric.String())
	writeManifestString(&b, snapshot.Encoding.String())
	writeManifestUint64(&b, uint64(snapshot.Dimensions))
	writeManifestUint64(&b, uint64(snapshot.M))
	writeManifestUint64(&b, uint64(snapshot.EfConstruction))
	writeManifestUint64(&b, uint64(snapshot.EfSearch))
	writeManifestUint64(&b, uint64(snapshot.RowCount))
	writeManifestUint64(&b, snapshot.BaseManifestGeneration)
	writeManifestUint64(&b, snapshot.BaseManifestChecksum)
	writeManifestUint64(&b, snapshot.BaseSchemaHash)
	writeManifestUint64(&b, uint64(snapshot.AdjacencyLayerCount))
	writeManifestUint64(&b, uint64(len(snapshot.Assets)))
	for _, asset := range snapshot.Assets {
		writeManifestString(&b, asset.Role)
		writeManifestString(&b, asset.AssetID)
		writeManifestString(&b, asset.LogicalType)
		writeManifestString(&b, asset.PhysicalEncoding)
		writeManifestUint64(&b, uint64(asset.RowCount))
		writeManifestUint64(&b, asset.SourceSchemaHash)
		writeManifestString(&b, string(asset.Ref.Kind))
		writeManifestString(&b, asset.Ref.Namespace)
		writeManifestUint64(&b, asset.Ref.Generation)
		writeManifestUint64(&b, asset.Ref.PartID)
		writeManifestUint64(&b, uint64(asset.Ref.FileID))
		writeManifestUint64(&b, uint64(asset.Ref.Offset))
		writeManifestUint64(&b, uint64(asset.Ref.Length))
		writeManifestUint64(&b, uint64(asset.Ref.Checksum))
		writeManifestUint64(&b, uint64(asset.AssetBytes))
	}
	return b.Bytes()
}

func decodeColumnVectorIndexStateRecord(raw []byte) (columnVectorIndexStateSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnVectorIndexStateMagic {
		return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: bad vector-index state magic=0x%08x", magic)
	}
	version := cur.u16()
	if version == columnVectorIndexStateVersion {
		decodedBytes := cur.u64()
		if err := cur.err; err != nil {
			return columnVectorIndexStateSnapshot{}, err
		}
		if decodedBytes > columnVectorIndexStateMaxDecodedBytes {
			return columnVectorIndexStateSnapshot{}, fmt.Errorf(
				"collections: compressed vector-index state decoded bytes=%d exceed limit=%d",
				decodedBytes,
				columnVectorIndexStateMaxDecodedBytes,
			)
		}
		compressed := raw[cur.pos:]
		snappyDecodedBytes, err := snappy.DecodedLen(compressed)
		if err != nil {
			return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: decode compressed vector-index state length: %w", err)
		}
		if uint64(snappyDecodedBytes) != decodedBytes {
			return columnVectorIndexStateSnapshot{}, fmt.Errorf(
				"collections: compressed vector-index state decoded length=%d want %d",
				snappyDecodedBytes,
				decodedBytes,
			)
		}
		decoded, err := snappy.Decode(make([]byte, 0, snappyDecodedBytes), compressed)
		if err != nil {
			return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: decode compressed vector-index state: %w", err)
		}
		return decodeColumnVectorIndexStateRecordUncompressed(decoded)
	}
	return decodeColumnVectorIndexStateRecordUncompressed(raw)
}

func decodeColumnVectorIndexStateRecordUncompressed(raw []byte) (columnVectorIndexStateSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnVectorIndexStateMagic {
		return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: bad vector-index state magic=0x%08x", magic)
	}
	version := cur.u16()
	if version != columnVectorIndexStateVersionV2 && version != columnVectorIndexStateVersionV1 {
		return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: unsupported vector-index state version=%d", version)
	}
	snapshot := columnVectorIndexStateSnapshot{
		IndexName: cur.string(),
		Field:     cur.string(),
	}
	metric, err := parseVectorMetric(cur.string())
	if err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	encoding, err := parseVectorIndexEncoding(cur.string())
	if err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	dims64 := cur.u64()
	m64 := cur.u64()
	efConstruction64 := cur.u64()
	efSearch64 := cur.u64()
	rowCount64 := cur.u64()
	snapshot.Metric = metric
	snapshot.Encoding = encoding
	snapshot.BaseManifestGeneration = cur.u64()
	snapshot.BaseManifestChecksum = cur.u64()
	snapshot.BaseSchemaHash = cur.u64()
	adjacencyLayerCount64 := uint64(0)
	if version != columnVectorIndexStateVersionV1 {
		adjacencyLayerCount64 = cur.u64()
	}
	assetCount64 := cur.u64()
	if err := cur.err; err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	if dims64 > uint64(math.MaxInt) || m64 > uint64(math.MaxInt) || efConstruction64 > uint64(math.MaxInt) || efSearch64 > uint64(math.MaxInt) || rowCount64 > uint64(math.MaxInt) || adjacencyLayerCount64 > uint64(math.MaxInt) || assetCount64 > uint64(math.MaxInt) {
		return columnVectorIndexStateSnapshot{}, errors.New("collections: vector-index state integer overflow")
	}
	snapshot.Dimensions = int(dims64)
	snapshot.M = int(m64)
	snapshot.EfConstruction = int(efConstruction64)
	snapshot.EfSearch = int(efSearch64)
	snapshot.RowCount = int(rowCount64)
	snapshot.AdjacencyLayerCount = int(adjacencyLayerCount64)
	remaining := uint64(len(raw) - cur.pos)
	// Each asset has six length-prefixed strings and nine uint64 fields before
	// any string payload bytes. Keep this lower bound exact so corrupt records
	// cannot advertise more assets than the remaining bytes can encode.
	const minEncodedVectorIndexStateAssetBytes = uint64(6*8 + 9*8)
	if assetCount64 > 0 && assetCount64 > remaining/minEncodedVectorIndexStateAssetBytes {
		return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: vector-index state asset_count=%d exceeds remaining record bytes=%d", assetCount64, remaining)
	}
	if assetCount64 > 0 {
		snapshot.Assets = make([]columnVectorIndexStateAssetSnapshot, int(assetCount64))
	}
	for i := range snapshot.Assets {
		asset, err := decodeColumnVectorIndexStateAsset(&cur)
		if err != nil {
			return columnVectorIndexStateSnapshot{}, fmt.Errorf("collections: vector-index state asset[%d]: %w", i, err)
		}
		snapshot.Assets[i] = asset
	}
	if err := cur.err; err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	if cur.pos != len(raw) {
		return columnVectorIndexStateSnapshot{}, errors.New("collections: trailing bytes in vector-index state record")
	}
	if err := validateColumnVectorIndexStateSnapshot(snapshot); err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	return snapshot, nil
}

func normalizeColumnVectorIndexStateSnapshotForEncode(snapshot columnVectorIndexStateSnapshot) (columnVectorIndexStateSnapshot, error) {
	if snapshot.AdjacencyLayerCount > 0 {
		return snapshot, nil
	}
	layerCount, err := columnVectorIndexStateAdjacencyLayerCountFromAssets(snapshot.Assets)
	if err != nil {
		return columnVectorIndexStateSnapshot{}, err
	}
	if layerCount > 0 {
		snapshot.AdjacencyLayerCount = layerCount
	}
	return snapshot, nil
}

func columnVectorIndexStateAdjacencyLayerCountFromAssets(assets []columnVectorIndexStateAssetSnapshot) (int, error) {
	layerCount := 0
	for _, asset := range assets {
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		layer, err := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if err != nil {
			return 0, err
		}
		if layer+1 > layerCount {
			layerCount = layer + 1
		}
	}
	return layerCount, nil
}

func decodeColumnVectorIndexStateAsset(cur *manifestCursor) (columnVectorIndexStateAssetSnapshot, error) {
	asset := columnVectorIndexStateAssetSnapshot{
		Role:             cur.string(),
		AssetID:          cur.string(),
		LogicalType:      cur.string(),
		PhysicalEncoding: cur.string(),
	}
	rowCount64 := cur.u64()
	asset.SourceSchemaHash = cur.u64()
	asset.Ref = ColumnAssetRef{
		Kind:       ColumnAssetKind(cur.string()),
		Namespace:  cur.string(),
		Generation: cur.u64(),
		PartID:     cur.u64(),
	}
	fileID64 := cur.u64()
	offset64 := cur.u64()
	length64 := cur.u64()
	checksum64 := cur.u64()
	assetBytes64 := cur.u64()
	if err := cur.err; err != nil {
		return columnVectorIndexStateAssetSnapshot{}, err
	}
	if rowCount64 > uint64(math.MaxInt) {
		return columnVectorIndexStateAssetSnapshot{}, errors.New("row count overflows int")
	}
	if fileID64 > uint64(math.MaxUint32) {
		return columnVectorIndexStateAssetSnapshot{}, errors.New("file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return columnVectorIndexStateAssetSnapshot{}, errors.New("checksum overflows uint32")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || assetBytes64 > uint64(math.MaxInt64) {
		return columnVectorIndexStateAssetSnapshot{}, errors.New("offsets or byte counts overflow int64")
	}
	asset.RowCount = int(rowCount64)
	asset.Ref.FileID = uint32(fileID64)
	asset.Ref.Offset = int64(offset64)
	asset.Ref.Length = int64(length64)
	asset.Ref.Checksum = uint32(checksum64)
	asset.AssetBytes = int64(assetBytes64)
	return asset, nil
}

func validateColumnVectorIndexStateSnapshot(snapshot columnVectorIndexStateSnapshot) error {
	if err := ValidateIndexName(snapshot.IndexName); err != nil {
		return fmt.Errorf("collections: invalid vector-index state index name: %w", err)
	}
	if err := ValidateIndexPath(snapshot.Field); err != nil {
		return fmt.Errorf("collections: invalid vector-index state field: %w", err)
	}
	if metric, err := normalizeVectorMetric(snapshot.Metric); err != nil || metric != snapshot.Metric {
		if err == nil {
			err = fmt.Errorf("unsupported metric %q", snapshot.Metric)
		}
		return fmt.Errorf("collections: invalid vector-index state metric: %w", err)
	}
	if encoding, err := normalizeVectorIndexEncoding(snapshot.Encoding); err != nil || encoding != snapshot.Encoding {
		if err == nil {
			err = fmt.Errorf("unsupported encoding %q", snapshot.Encoding)
		}
		return fmt.Errorf("collections: invalid vector-index state encoding: %w", err)
	}
	if snapshot.Dimensions <= 0 {
		return errors.New("collections: vector-index state dimensions must be positive")
	}
	if snapshot.M <= 0 || snapshot.EfConstruction <= 0 || snapshot.EfSearch <= 0 {
		return errors.New("collections: vector-index state graph parameters must be positive")
	}
	if snapshot.RowCount < 0 {
		return errors.New("collections: vector-index state row count must be non-negative")
	}
	if snapshot.BaseManifestGeneration == 0 || snapshot.BaseManifestChecksum == 0 || snapshot.BaseSchemaHash == 0 {
		return errors.New("collections: vector-index state missing base manifest identity")
	}
	if snapshot.AdjacencyLayerCount < 0 {
		return errors.New("collections: vector-index state adjacency layer count must be non-negative")
	}
	seen := make(map[string]struct{}, len(snapshot.Assets))
	for i, asset := range snapshot.Assets {
		if err := validateColumnVectorIndexStateAssetSnapshot(snapshot, asset); err != nil {
			return fmt.Errorf("collections: vector-index state asset[%d]: %w", i, err)
		}
		key := asset.Role + "\x00" + asset.AssetID
		if _, ok := seen[key]; ok {
			return fmt.Errorf("collections: vector-index state duplicate asset role=%q id=%q", asset.Role, asset.AssetID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateColumnVectorIndexStateAssetSnapshot(snapshot columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot) error {
	if asset.Role == "" || asset.AssetID == "" || asset.LogicalType == "" || asset.PhysicalEncoding == "" {
		return errors.New("missing role, asset id, logical type, or physical encoding")
	}
	if asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes {
		if !columnVectorIndexStateQuantizedAssetTypeEncodingKnown(asset.LogicalType, asset.PhysicalEncoding) {
			return fmt.Errorf("role=%q type/encoding=(%q,%q) want one of (%q,%q), (%q,%q)", asset.Role, asset.LogicalType, asset.PhysicalEncoding, columnVectorIndexStateLogicalTypeByteVector, columnVectorIndexStateEncodingRawFixedBytes, columnVectorIndexStateLogicalTypePackedBitVector, columnVectorIndexStateEncodingRawPackedBitVector)
		}
	} else if logical, encoding, strict := columnVectorIndexStateAssetTypeContract(asset.Role); strict {
		if asset.LogicalType != logical || asset.PhysicalEncoding != encoding {
			return fmt.Errorf("role=%q type/encoding=(%q,%q) want (%q,%q)", asset.Role, asset.LogicalType, asset.PhysicalEncoding, logical, encoding)
		}
	} else if !columnVectorIndexStateAssetRoleKnown(asset.Role) {
		return fmt.Errorf("unknown role %q", asset.Role)
	}
	if asset.Role == columnVectorIndexStateAssetRoleHNSWSearchPack && asset.AssetID != columnVectorIndexStateHNSWSearchPackAssetID {
		return fmt.Errorf("hnsw_search_pack asset id=%q want %q", asset.AssetID, columnVectorIndexStateHNSWSearchPackAssetID)
	}
	if asset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
		if asset.RowCount < 0 || (snapshot.RowCount > 0 && asset.RowCount == 0) || asset.RowCount > snapshot.RowCount {
			return fmt.Errorf("quantized_alpha row_count=%d is invalid for vector-index row_count=%d", asset.RowCount, snapshot.RowCount)
		}
	} else if asset.RowCount != snapshot.RowCount {
		return fmt.Errorf("row_count=%d want vector-index row_count=%d", asset.RowCount, snapshot.RowCount)
	}
	if asset.SourceSchemaHash == 0 {
		return errors.New("missing typed-column schema identity")
	}
	if err := validateColumnAssetRefForPlan(asset.Ref); err != nil {
		return err
	}
	wantKind := columnVectorIndexStateAssetRefKindContract(asset.Role)
	if asset.Ref.Kind != wantKind {
		return fmt.Errorf("ref kind=%q want %q", asset.Ref.Kind, wantKind)
	}
	if asset.Ref.Generation != snapshot.BaseManifestGeneration {
		return fmt.Errorf("ref generation=%d want base manifest generation=%d", asset.Ref.Generation, snapshot.BaseManifestGeneration)
	}
	if asset.AssetBytes <= 0 {
		return errors.New("asset bytes must be positive")
	}
	if asset.AssetBytes != asset.Ref.Length {
		return fmt.Errorf("asset bytes=%d does not match ref length=%d", asset.AssetBytes, asset.Ref.Length)
	}
	return nil
}

func columnVectorIndexStateQuantizedAssetTypeEncodingKnown(logicalType, physicalEncoding string) bool {
	return (logicalType == columnVectorIndexStateLogicalTypeByteVector && physicalEncoding == columnVectorIndexStateEncodingRawFixedBytes) ||
		(logicalType == columnVectorIndexStateLogicalTypePackedBitVector && physicalEncoding == columnVectorIndexStateEncodingRawPackedBitVector)
}

func columnVectorIndexStateAssetTypeContract(role string) (logicalType, physicalEncoding string, strict bool) {
	switch role {
	case columnVectorIndexStateAssetRoleAdjacency:
		return columnVectorIndexStateLogicalTypeUint32List, columnVectorIndexStateEncodingRawUint32List, true
	case columnVectorIndexStateAssetRoleInverseNorm:
		return columnVectorIndexStateLogicalTypeFloat32, columnVectorIndexStateEncodingRawFloat32, true
	case columnVectorIndexStateAssetRoleNormalizedVectors:
		return columnVectorIndexStateLogicalTypeFloat32Vector, columnVectorIndexStateEncodingRawFloat32Vector, true
	case columnVectorIndexStateAssetRoleRowRefs:
		return columnVectorIndexStateLogicalTypeInt64, columnVectorIndexStateEncodingRawInt64, true
	case columnVectorIndexStateAssetRoleDocumentIDs:
		return columnVectorIndexStateLogicalTypeBytes, columnVectorIndexStateEncodingRawBytesOffsets, true
	case columnVectorIndexStateAssetRoleQuantizedCodes:
		return columnVectorIndexStateLogicalTypeByteVector, columnVectorIndexStateEncodingRawFixedBytes, true
	case columnVectorIndexStateAssetRoleQuantizedAlpha:
		return columnVectorIndexStateLogicalTypeScalarU8Alpha, columnVectorIndexStateEncodingRawFloat32Uint32, true
	case columnVectorIndexStateAssetRoleHNSWSearchPack:
		return columnVectorIndexStateLogicalTypeSearchPack, columnVectorIndexStateEncodingHNSWSearchPackV1, true
	default:
		return "", "", false
	}
}

func columnVectorIndexStateAssetRefKindContract(role string) ColumnAssetKind {
	switch role {
	case columnVectorIndexStateAssetRoleHNSWSearchPack:
		return ColumnAssetKindTCS1HNSWSearchPack
	default:
		return ColumnAssetKindTCS1TypedColumnPart
	}
}

func columnVectorIndexStateAssetRoleKnown(role string) bool {
	switch role {
	case columnVectorIndexStateAssetRoleAdjacency,
		columnVectorIndexStateAssetRoleInverseNorm,
		columnVectorIndexStateAssetRoleNormalizedVectors,
		columnVectorIndexStateAssetRoleRowRefs,
		columnVectorIndexStateAssetRoleDocumentIDs,
		columnVectorIndexStateAssetRoleQuantizedCodes,
		columnVectorIndexStateAssetRoleQuantizedAlpha,
		columnVectorIndexStateAssetRoleHNSWSearchPack:
		return true
	default:
		return false
	}
}

type columnVectorIndexStateMatch uint8

const (
	columnVectorIndexStateMatchMismatch columnVectorIndexStateMatch = iota
	columnVectorIndexStateMatchLoaded
	columnVectorIndexStateMatchUnsupportedVisibility
)

func columnVectorIndexStateMatchStatus(state columnVectorIndexStateSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord) columnVectorIndexStateMatch {
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifest, records, cfg)
	if err != nil {
		return columnVectorIndexStateMatchMismatch
	}
	return columnVectorIndexStateMatchStatusWithBaseChecksum(state, def, cfg, baseChecksum)
}

func columnVectorIndexStateMatchStatusWithBaseChecksum(state columnVectorIndexStateSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, baseChecksum uint64) columnVectorIndexStateMatch {
	if cfg.ActiveManifest == nil {
		return columnVectorIndexStateMatchMismatch
	}
	if !columnVectorIndexStateDefinitionParametersMatch(&state, &def) || state.BaseSchemaHash != cfg.SchemaHash {
		return columnVectorIndexStateMatchMismatch
	}
	if state.BaseManifestGeneration != cfg.ActiveManifest.Generation {
		return columnVectorIndexStateMatchUnsupportedVisibility
	}
	if state.BaseManifestChecksum != baseChecksum {
		return columnVectorIndexStateMatchMismatch
	}
	for _, asset := range state.Assets {
		if !columnVectorIndexStateAssetRefMatchesManifestForDefinition(asset, state, cfg, def) {
			return columnVectorIndexStateMatchMismatch
		}
	}
	return columnVectorIndexStateMatchLoaded
}

func columnVectorIndexStateDefinitionParametersMatch(state *columnVectorIndexStateSnapshot, def *VectorIndexDefinition) bool {
	return state.IndexName == def.Name &&
		state.Field == def.Field &&
		state.Metric == def.Metric &&
		state.Encoding == def.Encoding &&
		state.Dimensions == def.Dimensions &&
		state.M == def.M &&
		state.EfConstruction == def.EfConstruction &&
		state.EfSearch == def.EfSearch &&
		columnVectorGraphQuantizedStateAssetIDSetMatches(*def, *state)
}

func columnVectorIndexStateMatchesGraph(state columnVectorIndexStateSnapshot, graph columnVectorGraphManifestSnapshot) bool {
	return state.IndexName == graph.IndexName &&
		state.Field == graph.Field &&
		state.Metric == graph.Metric &&
		state.Encoding == graph.Encoding &&
		state.Dimensions == graph.Dimensions &&
		state.M == graph.M &&
		state.EfConstruction == graph.EfConstruction &&
		state.EfSearch == graph.EfSearch &&
		state.RowCount == graph.RowCount &&
		state.BaseManifestGeneration == graph.BaseManifestGeneration &&
		state.BaseManifestChecksum == graph.BaseManifestChecksum &&
		state.BaseSchemaHash == graph.BaseSchemaHash
}

func columnVectorIndexStateAssetRefMatchesManifest(asset columnVectorIndexStateAssetSnapshot, state columnVectorIndexStateSnapshot, cfg ColumnStoreConfig) bool {
	rowCountOK := asset.RowCount == state.RowCount
	if asset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
		rowCountOK = asset.RowCount >= 0 && (state.RowCount == 0 || (asset.RowCount > 0 && asset.RowCount <= state.RowCount))
	}
	return asset.Ref.Kind == columnVectorIndexStateAssetRefKindContract(asset.Role) &&
		asset.Ref.Namespace == cfg.AssetManager.Namespace &&
		asset.Ref.Generation == state.BaseManifestGeneration &&
		rowCountOK &&
		asset.AssetBytes == asset.Ref.Length &&
		asset.SourceSchemaHash != 0 &&
		validateColumnAssetRefForPlan(asset.Ref) == nil
}

func columnVectorIndexStateAssetRefMatchesManifestForDefinition(asset columnVectorIndexStateAssetSnapshot, state columnVectorIndexStateSnapshot, cfg ColumnStoreConfig, def VectorIndexDefinition) bool {
	if !columnVectorIndexStateAssetRefMatchesManifest(asset, state, cfg) {
		return false
	}
	if asset.Role != columnVectorIndexStateAssetRoleQuantizedAlpha {
		return true
	}
	q, ok := columnVectorGraphQuantizedDefinitionForStateAsset(def, asset)
	if !ok {
		return false
	}
	wantRowCounts, err := columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q, state.RowCount)
	return err == nil && asset.RowCount == len(wantRowCounts)
}

func columnVectorIndexStateSnapshotFromGraph(graph columnVectorGraphManifestSnapshot) columnVectorIndexStateSnapshot {
	return columnVectorIndexStateSnapshot{
		IndexName:              graph.IndexName,
		Field:                  graph.Field,
		Metric:                 graph.Metric,
		Encoding:               graph.Encoding,
		Dimensions:             graph.Dimensions,
		M:                      graph.M,
		EfConstruction:         graph.EfConstruction,
		EfSearch:               graph.EfSearch,
		RowCount:               graph.RowCount,
		BaseManifestGeneration: graph.BaseManifestGeneration,
		BaseManifestChecksum:   graph.BaseManifestChecksum,
		BaseSchemaHash:         graph.BaseSchemaHash,
		AdjacencyLayerCount:    graph.AdjacencyLayerCount,
	}
}

func retainColumnVectorIndexStateRecordForWrite(key []byte, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) bool {
	if !bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes) {
		return false
	}
	if !activeVectorIndexesKnown {
		return true
	}
	indexName := key[len(columnVectorIndexStateRecordPrefixBytes):]
	for _, def := range activeVectorIndexes {
		if def.Strategy == VectorIndexStrategyColumnGraph && columnManifestBytesEqualString(indexName, def.Name) {
			return true
		}
	}
	return false
}

func validateRetainedColumnVectorIndexStateRecordForWrite(record columnManifestRecord, generation uint64) (bool, error) {
	if !bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
		return false, errors.New("collections: invalid vector-index state record key prefix")
	}
	state, err := decodeColumnVectorIndexStateRecord(record.value)
	if err != nil {
		return false, err
	}
	keyIndexName := string(record.key[len(columnVectorIndexStateRecordPrefixBytes):])
	if state.IndexName != keyIndexName {
		return false, fmt.Errorf("collections: vector-index state key index=%q does not match record index=%q", keyIndexName, state.IndexName)
	}
	if state.BaseManifestGeneration >= generation {
		return false, nil
	}
	return true, nil
}

func columnVectorIndexStateAssetRefsFromManifestRecordsForReachability(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) ([]ColumnAssetRef, error) {
	var refs []ColumnAssetRef
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			continue
		}
		if !retainColumnVectorIndexStateRecordForWrite(record.key, activeVectorIndexesKnown, activeVectorIndexes) {
			continue
		}
		state, err := decodeColumnVectorIndexStateRecord(record.value)
		if err != nil {
			return nil, err
		}
		stateRefs, err := columnVectorIndexStateManifestAssetRefsForScan(state, activeGeneration, expectedNamespace)
		if err != nil {
			return nil, err
		}
		refs = append(refs, stateRefs...)
	}
	return refs, nil
}

func validateColumnVectorIndexStateAssetRefsAvailable(rootDir string, state columnVectorIndexStateSnapshot) error {
	for _, asset := range state.Assets {
		if err := validateColumnVectorGraphAssetRefAvailable(rootDir, asset.Ref); err != nil {
			return fmt.Errorf("collections: vector-index state asset role=%q id=%q unavailable: %w", asset.Role, asset.AssetID, err)
		}
	}
	return nil
}

func columnVectorIndexStateManifestAssetRefsForScan(state columnVectorIndexStateSnapshot, activeGeneration uint64, expectedNamespace string) ([]ColumnAssetRef, error) {
	if state.BaseManifestGeneration > activeGeneration {
		return nil, fmt.Errorf("collections: vector-index state base manifest generation=%d is newer than active manifest generation=%d", state.BaseManifestGeneration, activeGeneration)
	}
	refs := make([]ColumnAssetRef, 0, len(state.Assets))
	for _, asset := range state.Assets {
		if asset.Ref.Generation > activeGeneration {
			return nil, fmt.Errorf("collections: vector-index state asset generation=%d is newer than active manifest generation=%d", asset.Ref.Generation, activeGeneration)
		}
		if asset.Ref.Namespace != expectedNamespace {
			return nil, fmt.Errorf("collections: vector-index state asset namespace=%q want %q", asset.Ref.Namespace, expectedNamespace)
		}
		if asset.Ref.Generation != state.BaseManifestGeneration {
			return nil, fmt.Errorf("collections: vector-index state asset generation=%d does not match base manifest generation=%d", asset.Ref.Generation, state.BaseManifestGeneration)
		}
		if err := validateColumnAssetRefForPlan(asset.Ref); err != nil {
			return nil, err
		}
		refs = append(refs, asset.Ref)
	}
	return refs, nil
}
