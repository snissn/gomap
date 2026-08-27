package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const (
	columnManifestVectorGraphRecordPrefix       = "\x03column-manifest/v1/vector-graph/"
	columnManifestVectorGraphMagic              = uint32(0x54434d47) // TCMG
	columnVectorGraphLayer0SourceMagic          = uint32(0x54434741) // TCGA
	columnVectorGraphLayer0SourceVersion        = uint16(1)
	columnVectorGraphAdjacencyLayerSourcesMagic = uint32(0x5443474c) // TCGL
	columnVectorGraphAdjacencyLayerSourcesV1    = uint16(1)

	columnVectorGraphVectorColumnName    = "vector"
	columnVectorGraphInvNormColumnName   = "inv_norm"
	columnVectorGraphAdjacencyColumnName = "adjacency"
)

var columnManifestVectorGraphRecordPrefixBytes = []byte(columnManifestVectorGraphRecordPrefix)

// Quarantine: graph-specific adjacency-source storage refs embedded in this
// manifest (TCGA/TCGL) are legacy compatibility only. New graph builds leave
// these fields empty and publish HNSW adjacency through vector-index state
// uint32_list assets instead.
type columnVectorGraphManifestSnapshot struct {
	IndexName              string
	Field                  string
	Metric                 VectorMetric
	Encoding               VectorIndexEncoding
	Dimensions             int
	M                      int
	EfConstruction         int
	EfSearch               int
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	GraphSchemaHash        uint64
	RowCount               int
	AssetRef               ColumnAssetRef
	AssetBytes             int64
	Layer0AdjacencySource  columnVectorGraphLayer0AdjacencySourceSnapshot
	AdjacencyLayerCount    int
	AdjacencyLayerSources  []columnVectorGraphAdjacencySourceSnapshot
}

// columnVectorGraphAdjacencySourceSnapshot binds a graph-layer adjacency
// offsets-list source to the graph manifest identity.
type columnVectorGraphAdjacencySourceSnapshot struct {
	Present                bool
	Schema                 string
	ColumnName             string
	ValueType              string
	Encoding               string
	Layer                  int
	SourceSchemaHash       uint64
	RowCount               int
	ValuesCount            int
	OffsetsBytes           int64
	ValuesBytes            int64
	PaddingBytes           int64
	Ref                    ColumnAssetRef
	AssetBytes             int64
	BaseManifestGeneration uint64
	BaseManifestChecksum   uint64
	BaseSchemaHash         uint64
	GraphSchemaHash        uint64
	GraphAssetGeneration   uint64
	GraphAssetPartID       uint64
	GraphAssetFileID       uint32
	GraphAssetOffset       int64
	GraphAssetLength       int64
	GraphAssetChecksum     uint32
}

// columnVectorGraphLayer0AdjacencySourceSnapshot binds the optional #1918
// layer-0 adjacency offsets-list source to the graph manifest identity. Old
// graph records have Present=false and continue to use row-asset adjacency as
// the compatibility/fallback source.
type columnVectorGraphLayer0AdjacencySourceSnapshot = columnVectorGraphAdjacencySourceSnapshot

type columnVectorGraphAssetRow struct {
	ID         []byte
	Vector     []float32
	InvNorm    float32
	Adjacency  []uint32
	BaseRowRef DocumentRowRef
}

type columnVectorGraphPreparedPhysicalAsset struct {
	AssetRootDir         string
	Config               ColumnStoreConfig
	Ref                  ColumnAssetRef
	Bytes                int64
	RowCount             int
	stableResources      *rootpublication.StableResourceSet
	stableSegments       uint64
	stableContentSyncs   uint64
	stableNamespaceSyncs uint64
	stableFileSync       time.Duration
	stableNamespaceSync  time.Duration
}

func columnVectorGraphManifestHasPhysicalAsset(snapshot columnVectorGraphManifestSnapshot) bool {
	return snapshot.AssetBytes != 0 ||
		snapshot.AssetRef.Kind != "" ||
		snapshot.AssetRef.Namespace != "" ||
		snapshot.AssetRef.Generation != 0 ||
		snapshot.AssetRef.PartID != 0 ||
		snapshot.AssetRef.FileID != 0 ||
		snapshot.AssetRef.Offset != 0 ||
		snapshot.AssetRef.Length != 0 ||
		snapshot.AssetRef.Checksum != 0
}

func columnVectorGraphManifestRecordKey(indexName string) []byte {
	key := make([]byte, len(columnManifestVectorGraphRecordPrefix)+len(indexName))
	copy(key, columnManifestVectorGraphRecordPrefix)
	copy(key[len(columnManifestVectorGraphRecordPrefix):], indexName)
	return key
}

func columnManifestRecordKeyKnownForScan(key []byte) bool {
	return bytes.Equal(key, columnManifestHeaderRecordKeyBytes) ||
		bytes.HasPrefix(key, columnManifestPartRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes) ||
		bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes)
}

func findColumnVectorGraphManifestRecord(records []columnManifestRecord, indexName string) (columnManifestRecord, bool) {
	key := columnVectorGraphManifestRecordKey(indexName)
	for _, record := range records {
		if bytes.Equal(record.key, key) {
			return record, true
		}
	}
	return columnManifestRecord{}, false
}

func encodeColumnVectorGraphManifestRecord(snapshot columnVectorGraphManifestSnapshot) ([]byte, error) {
	if err := validateColumnVectorGraphManifestSnapshot(snapshot); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestVectorGraphMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, snapshot.IndexName)
	writeManifestString(&b, snapshot.Field)
	writeManifestString(&b, snapshot.Metric.String())
	writeManifestString(&b, snapshot.Encoding.String())
	writeManifestUint64(&b, uint64(snapshot.Dimensions))
	writeManifestUint64(&b, uint64(snapshot.M))
	writeManifestUint64(&b, uint64(snapshot.EfConstruction))
	writeManifestUint64(&b, uint64(snapshot.EfSearch))
	writeManifestUint64(&b, snapshot.BaseManifestGeneration)
	writeManifestUint64(&b, snapshot.BaseManifestChecksum)
	writeManifestUint64(&b, snapshot.BaseSchemaHash)
	writeManifestUint64(&b, snapshot.GraphSchemaHash)
	writeManifestUint64(&b, uint64(snapshot.RowCount))
	writeManifestString(&b, string(snapshot.AssetRef.Kind))
	writeManifestString(&b, snapshot.AssetRef.Namespace)
	writeManifestUint64(&b, snapshot.AssetRef.Generation)
	writeManifestUint64(&b, snapshot.AssetRef.PartID)
	writeManifestUint64(&b, uint64(snapshot.AssetRef.FileID))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Offset))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Length))
	writeManifestUint64(&b, uint64(snapshot.AssetRef.Checksum))
	writeManifestUint64(&b, uint64(snapshot.AssetBytes))
	if len(snapshot.AdjacencyLayerSources) > 0 {
		encodeColumnVectorGraphAdjacencyLayerSources(&b, snapshot.AdjacencyLayerCount, snapshot.AdjacencyLayerSources)
	} else if snapshot.Layer0AdjacencySource.Present {
		encodeColumnVectorGraphLayer0AdjacencySource(&b, snapshot.Layer0AdjacencySource)
	}
	return b.Bytes(), nil
}

func isSupportedColumnVectorGraphManifestRecordVersion(version uint16) bool {
	return version == columnManifestRecordVersionV4 || version == columnManifestRecordVersionV3
}

func decodeColumnVectorGraphManifestRecord(raw []byte) (columnVectorGraphManifestSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestVectorGraphMagic {
		return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: bad column vector graph manifest magic=0x%08x", magic)
	}
	if version := cur.u16(); !isSupportedColumnVectorGraphManifestRecordVersion(version) {
		return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: unsupported column vector graph manifest version=%d", version)
	}
	snapshot := columnVectorGraphManifestSnapshot{
		IndexName: cur.string(),
		Field:     cur.string(),
	}
	metric, err := parseVectorMetric(cur.string())
	if err != nil {
		return columnVectorGraphManifestSnapshot{}, err
	}
	encoding, err := parseVectorIndexEncoding(cur.string())
	if err != nil {
		return columnVectorGraphManifestSnapshot{}, err
	}
	snapshot.Metric = metric
	snapshot.Encoding = encoding
	snapshot.Dimensions = int(cur.u64())
	snapshot.M = int(cur.u64())
	snapshot.EfConstruction = int(cur.u64())
	snapshot.EfSearch = int(cur.u64())
	snapshot.BaseManifestGeneration = cur.u64()
	snapshot.BaseManifestChecksum = cur.u64()
	snapshot.BaseSchemaHash = cur.u64()
	snapshot.GraphSchemaHash = cur.u64()
	snapshot.RowCount = int(cur.u64())
	snapshot.AssetRef = ColumnAssetRef{
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
		return columnVectorGraphManifestSnapshot{}, err
	}
	if snapshot.Dimensions < 0 || snapshot.M < 0 || snapshot.EfConstruction < 0 || snapshot.EfSearch < 0 || snapshot.RowCount < 0 {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest integer overflow")
	}
	if fileID64 > uint64(math.MaxUint32) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest checksum overflows uint32")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || assetBytes64 > uint64(math.MaxInt64) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest offsets or byte counts overflow int64")
	}
	snapshot.AssetRef.FileID = uint32(fileID64)
	snapshot.AssetRef.Offset = int64(offset64)
	snapshot.AssetRef.Length = int64(length64)
	snapshot.AssetRef.Checksum = uint32(checksum64)
	snapshot.AssetBytes = int64(assetBytes64)
	seenLegacyLayer0AdjacencySourceBlock := false
	for cur.pos < len(raw) {
		if len(raw)-cur.pos < 6 {
			return columnVectorGraphManifestSnapshot{}, errors.New("collections: trailing bytes in column vector graph manifest record")
		}
		magicCur := cur
		magic := magicCur.u32()
		if err := magicCur.err; err != nil {
			return columnVectorGraphManifestSnapshot{}, err
		}
		switch magic {
		case columnVectorGraphAdjacencyLayerSourcesMagic:
			if seenLegacyLayer0AdjacencySourceBlock {
				return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest has both legacy layer-0 and all-layer adjacency source blocks")
			}
			if len(snapshot.AdjacencyLayerSources) > 0 {
				return columnVectorGraphManifestSnapshot{}, errors.New("collections: duplicate column vector graph adjacency layer sources block")
			}
			sourcesCur := cur
			layerCount, sources, err := decodeColumnVectorGraphAdjacencyLayerSources(&sourcesCur)
			if err != nil {
				return columnVectorGraphManifestSnapshot{}, err
			}
			if err := validateColumnVectorGraphAdjacencyLayerSourcesSnapshot(layerCount, sources); err != nil {
				return columnVectorGraphManifestSnapshot{}, err
			}
			snapshot.AdjacencyLayerCount = layerCount
			snapshot.AdjacencyLayerSources = sources
			if len(sources) > 0 {
				snapshot.Layer0AdjacencySource = sources[0]
			}
			cur = sourcesCur
			continue
		case columnVectorGraphLayer0SourceMagic:
			if len(snapshot.AdjacencyLayerSources) > 0 {
				return columnVectorGraphManifestSnapshot{}, errors.New("collections: column vector graph manifest has both all-layer and legacy layer-0 adjacency source blocks")
			}
			sourceCur := cur
			source, err := decodeColumnVectorGraphAdjacencySource(&sourceCur)
			if err != nil {
				if columnVectorGraphManifestRecordHasDecodableAdjacencyLayerSourcesAtOrAfter(raw, cur.pos+6) {
					return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: malformed legacy layer-0 adjacency source before all-layer adjacency sources block: %w", err)
				}
				cur.pos = len(raw)
				cur.err = nil
				continue
			}
			if seenLegacyLayer0AdjacencySourceBlock {
				return columnVectorGraphManifestSnapshot{}, errors.New("collections: duplicate column vector graph legacy layer-0 adjacency source block")
			}
			seenLegacyLayer0AdjacencySourceBlock = true
			if err := validateColumnVectorGraphLayer0AdjacencySourceSnapshot(source); err == nil {
				snapshot.Layer0AdjacencySource = source
			}
			cur = sourceCur
			continue
		default:
			return columnVectorGraphManifestSnapshot{}, fmt.Errorf("collections: unrecognized trailing bytes in column vector graph manifest record magic=0x%08x", magic)
		}
	}
	if cur.pos != len(raw) {
		return columnVectorGraphManifestSnapshot{}, errors.New("collections: trailing bytes in column vector graph manifest record")
	}
	if err := validateColumnVectorGraphManifestSnapshotCore(snapshot); err != nil {
		return columnVectorGraphManifestSnapshot{}, err
	}
	return snapshot, nil
}

func columnVectorGraphManifestRecordMagicAt(raw []byte, pos int) uint32 {
	if pos < 0 || len(raw)-pos < 4 {
		return 0
	}
	cur := manifestCursor{raw: raw, pos: pos}
	magic := cur.u32()
	if cur.err != nil {
		return 0
	}
	return magic
}

func columnVectorGraphManifestRecordHasDecodableAdjacencyLayerSourcesAtOrAfter(raw []byte, start int) bool {
	if start < 0 {
		start = 0
	}
	for pos := start; pos <= len(raw)-6; pos++ {
		if columnVectorGraphManifestRecordMagicAt(raw, pos) != columnVectorGraphAdjacencyLayerSourcesMagic {
			continue
		}
		candidate := manifestCursor{raw: raw, pos: pos}
		layerCount, sources, err := decodeColumnVectorGraphAdjacencyLayerSources(&candidate)
		if err == nil && validateColumnVectorGraphAdjacencyLayerSourcesSnapshot(layerCount, sources) == nil {
			return true
		}
	}
	return false
}

func encodeColumnVectorGraphLayer0AdjacencySource(b *bytes.Buffer, source columnVectorGraphLayer0AdjacencySourceSnapshot) {
	writeManifestUint32(b, columnVectorGraphLayer0SourceMagic)
	writeManifestUint16(b, columnVectorGraphLayer0SourceVersion)
	writeManifestString(b, source.Schema)
	writeManifestString(b, source.ColumnName)
	writeManifestString(b, source.ValueType)
	writeManifestString(b, source.Encoding)
	writeManifestUint64(b, uint64(source.Layer))
	writeManifestUint64(b, source.SourceSchemaHash)
	writeManifestUint64(b, uint64(source.RowCount))
	writeManifestUint64(b, uint64(source.ValuesCount))
	writeManifestUint64(b, uint64(source.OffsetsBytes))
	writeManifestUint64(b, uint64(source.ValuesBytes))
	writeManifestUint64(b, uint64(source.PaddingBytes))
	writeManifestString(b, string(source.Ref.Kind))
	writeManifestString(b, source.Ref.Namespace)
	writeManifestUint64(b, source.Ref.Generation)
	writeManifestUint64(b, source.Ref.PartID)
	writeManifestUint64(b, uint64(source.Ref.FileID))
	writeManifestUint64(b, uint64(source.Ref.Offset))
	writeManifestUint64(b, uint64(source.Ref.Length))
	writeManifestUint64(b, uint64(source.Ref.Checksum))
	writeManifestUint64(b, uint64(source.AssetBytes))
	writeManifestUint64(b, source.BaseManifestGeneration)
	writeManifestUint64(b, source.BaseManifestChecksum)
	writeManifestUint64(b, source.BaseSchemaHash)
	writeManifestUint64(b, source.GraphSchemaHash)
	writeManifestUint64(b, source.GraphAssetGeneration)
	writeManifestUint64(b, source.GraphAssetPartID)
	writeManifestUint64(b, uint64(source.GraphAssetFileID))
	writeManifestUint64(b, uint64(source.GraphAssetOffset))
	writeManifestUint64(b, uint64(source.GraphAssetLength))
	writeManifestUint64(b, uint64(source.GraphAssetChecksum))
}

func encodeColumnVectorGraphAdjacencyLayerSources(b *bytes.Buffer, layerCount int, sources []columnVectorGraphAdjacencySourceSnapshot) {
	writeManifestUint32(b, columnVectorGraphAdjacencyLayerSourcesMagic)
	writeManifestUint16(b, columnVectorGraphAdjacencyLayerSourcesV1)
	writeManifestUint64(b, uint64(layerCount))
	writeManifestUint64(b, uint64(len(sources)))
	for _, source := range sources {
		encodeColumnVectorGraphLayer0AdjacencySource(b, source)
	}
}

func decodeColumnVectorGraphAdjacencyLayerSources(cur *manifestCursor) (int, []columnVectorGraphAdjacencySourceSnapshot, error) {
	if magic := cur.u32(); magic != columnVectorGraphAdjacencyLayerSourcesMagic {
		return 0, nil, fmt.Errorf("collections: bad column vector graph adjacency layer sources magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnVectorGraphAdjacencyLayerSourcesV1 {
		return 0, nil, fmt.Errorf("collections: unsupported column vector graph adjacency layer sources version=%d", version)
	}
	layerCount64 := cur.u64()
	sourceCount64 := cur.u64()
	if err := cur.err; err != nil {
		return 0, nil, err
	}
	if layerCount64 == 0 {
		return 0, nil, errors.New("collections: column vector graph adjacency layer sources layer_count must be positive")
	}
	if sourceCount64 != layerCount64 {
		return 0, nil, fmt.Errorf("collections: column vector graph adjacency layer sources source_count=%d want layer_count=%d", sourceCount64, layerCount64)
	}
	if layerCount64 > uint64(math.MaxInt) {
		return 0, nil, errors.New("collections: column vector graph adjacency layer sources count overflows int")
	}
	remaining := uint64(len(cur.raw) - cur.pos)
	const minEncodedAdjacencySourceBytes = uint64(6 + 4*8 + 7*8 + 2*8 + 7*8 + 4*8 + 6*8)
	if sourceCount64 > remaining/minEncodedAdjacencySourceBytes {
		return 0, nil, fmt.Errorf("collections: column vector graph adjacency layer sources source_count=%d exceeds remaining record bytes=%d", sourceCount64, remaining)
	}
	layerCount := int(layerCount64)
	sourceCount := int(sourceCount64)
	sources := make([]columnVectorGraphAdjacencySourceSnapshot, sourceCount)
	for i := range sources {
		source, err := decodeColumnVectorGraphAdjacencySource(cur)
		if err != nil {
			return 0, nil, err
		}
		sources[i] = source
	}
	return layerCount, sources, nil
}

func decodeColumnVectorGraphLayer0AdjacencySource(cur *manifestCursor) (columnVectorGraphLayer0AdjacencySourceSnapshot, error) {
	return decodeColumnVectorGraphAdjacencySource(cur)
}

func decodeColumnVectorGraphAdjacencySource(cur *manifestCursor) (columnVectorGraphAdjacencySourceSnapshot, error) {
	if magic := cur.u32(); magic != columnVectorGraphLayer0SourceMagic {
		return columnVectorGraphAdjacencySourceSnapshot{}, fmt.Errorf("collections: bad column vector graph adjacency source magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnVectorGraphLayer0SourceVersion {
		return columnVectorGraphAdjacencySourceSnapshot{}, fmt.Errorf("collections: unsupported column vector graph adjacency source version=%d", version)
	}
	source := columnVectorGraphAdjacencySourceSnapshot{
		Present:    true,
		Schema:     cur.string(),
		ColumnName: cur.string(),
		ValueType:  cur.string(),
		Encoding:   cur.string(),
	}
	layer64 := cur.u64()
	sourceSchemaHash := cur.u64()
	rowCount64 := cur.u64()
	valuesCount64 := cur.u64()
	source.SourceSchemaHash = sourceSchemaHash
	offsetsBytes64 := cur.u64()
	valuesBytes64 := cur.u64()
	paddingBytes64 := cur.u64()
	source.Ref = ColumnAssetRef{
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
	source.BaseManifestGeneration = cur.u64()
	source.BaseManifestChecksum = cur.u64()
	source.BaseSchemaHash = cur.u64()
	source.GraphSchemaHash = cur.u64()
	source.GraphAssetGeneration = cur.u64()
	source.GraphAssetPartID = cur.u64()
	graphAssetFileID64 := cur.u64()
	graphAssetOffset64 := cur.u64()
	graphAssetLength64 := cur.u64()
	graphAssetChecksum64 := cur.u64()
	if err := cur.err; err != nil {
		return columnVectorGraphAdjacencySourceSnapshot{}, err
	}
	if layer64 > uint64(math.MaxInt) || rowCount64 > uint64(math.MaxInt) || valuesCount64 > uint64(math.MaxInt) {
		return columnVectorGraphAdjacencySourceSnapshot{}, errors.New("collections: column vector graph adjacency source integer overflow")
	}
	if fileID64 > uint64(math.MaxUint32) || graphAssetFileID64 > uint64(math.MaxUint32) {
		return columnVectorGraphAdjacencySourceSnapshot{}, errors.New("collections: column vector graph adjacency source file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) || graphAssetChecksum64 > uint64(math.MaxUint32) {
		return columnVectorGraphAdjacencySourceSnapshot{}, errors.New("collections: column vector graph adjacency source checksum overflows uint32")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || assetBytes64 > uint64(math.MaxInt64) || offsetsBytes64 > uint64(math.MaxInt64) || valuesBytes64 > uint64(math.MaxInt64) || paddingBytes64 > uint64(math.MaxInt64) || graphAssetOffset64 > uint64(math.MaxInt64) || graphAssetLength64 > uint64(math.MaxInt64) {
		return columnVectorGraphAdjacencySourceSnapshot{}, errors.New("collections: column vector graph adjacency source offsets or byte counts overflow int64")
	}
	source.Layer = int(layer64)
	source.RowCount = int(rowCount64)
	source.ValuesCount = int(valuesCount64)
	source.OffsetsBytes = int64(offsetsBytes64)
	source.ValuesBytes = int64(valuesBytes64)
	source.PaddingBytes = int64(paddingBytes64)
	source.Ref.FileID = uint32(fileID64)
	source.Ref.Offset = int64(offset64)
	source.Ref.Length = int64(length64)
	source.Ref.Checksum = uint32(checksum64)
	source.AssetBytes = int64(assetBytes64)
	source.GraphAssetFileID = uint32(graphAssetFileID64)
	source.GraphAssetOffset = int64(graphAssetOffset64)
	source.GraphAssetLength = int64(graphAssetLength64)
	source.GraphAssetChecksum = uint32(graphAssetChecksum64)
	return source, nil
}

func validateColumnVectorGraphManifestSnapshot(snapshot columnVectorGraphManifestSnapshot) error {
	if err := validateColumnVectorGraphManifestSnapshotCore(snapshot); err != nil {
		return err
	}
	if snapshot.AdjacencyLayerCount != 0 || len(snapshot.AdjacencyLayerSources) > 0 {
		if err := validateColumnVectorGraphAdjacencyLayerSourcesSnapshot(snapshot.AdjacencyLayerCount, snapshot.AdjacencyLayerSources); err != nil {
			return err
		}
		if !snapshot.Layer0AdjacencySource.Present {
			return errors.New("collections: column vector graph all-layer adjacency sources missing layer-0 adjacency source alias")
		}
		if snapshot.Layer0AdjacencySource != snapshot.AdjacencyLayerSources[0] {
			return errors.New("collections: column vector graph layer-0 adjacency source does not match all-layer source[0]")
		}
		return nil
	}
	if snapshot.Layer0AdjacencySource.Present {
		if err := validateColumnVectorGraphLayer0AdjacencySourceSnapshot(snapshot.Layer0AdjacencySource); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnVectorGraphManifestSnapshotCore(snapshot columnVectorGraphManifestSnapshot) error {
	if err := ValidateIndexName(snapshot.IndexName); err != nil {
		return fmt.Errorf("collections: invalid column vector graph manifest index name: %w", err)
	}
	if err := ValidateIndexPath(snapshot.Field); err != nil {
		return fmt.Errorf("collections: invalid column vector graph manifest field: %w", err)
	}
	if metric, err := normalizeVectorMetric(snapshot.Metric); err != nil || metric != snapshot.Metric {
		if err == nil {
			err = fmt.Errorf("unsupported metric %q", snapshot.Metric)
		}
		return fmt.Errorf("collections: invalid column vector graph manifest metric: %w", err)
	}
	if encoding, err := normalizeVectorIndexEncoding(snapshot.Encoding); err != nil || encoding != snapshot.Encoding {
		if err == nil {
			err = fmt.Errorf("unsupported encoding %q", snapshot.Encoding)
		}
		return fmt.Errorf("collections: invalid column vector graph manifest encoding: %w", err)
	}
	if snapshot.Dimensions <= 0 {
		return errors.New("collections: column vector graph manifest dimensions must be positive")
	}
	if snapshot.M <= 0 || snapshot.EfConstruction <= 0 || snapshot.EfSearch <= 0 {
		return errors.New("collections: column vector graph manifest graph parameters must be positive")
	}
	if snapshot.BaseManifestGeneration == 0 || snapshot.BaseManifestChecksum == 0 || snapshot.BaseSchemaHash == 0 || snapshot.GraphSchemaHash == 0 {
		return errors.New("collections: column vector graph manifest missing base or graph identity")
	}
	if snapshot.RowCount < 0 {
		return errors.New("collections: column vector graph manifest row count must be non-negative")
	}
	if !columnVectorGraphManifestHasPhysicalAsset(snapshot) {
		return nil
	}
	if err := validateColumnAssetRefForPlan(snapshot.AssetRef); err != nil {
		return err
	}
	if snapshot.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
		return fmt.Errorf("collections: column vector graph manifest asset kind=%q want %q", snapshot.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
	}
	if snapshot.AssetBytes <= 0 {
		return errors.New("collections: column vector graph manifest asset bytes must be positive")
	}
	if snapshot.AssetBytes != snapshot.AssetRef.Length {
		return fmt.Errorf("collections: column vector graph manifest asset bytes=%d does not match ref length=%d", snapshot.AssetBytes, snapshot.AssetRef.Length)
	}
	return nil
}

func validateColumnVectorGraphAdjacencyLayerSourcesSnapshot(layerCount int, sources []columnVectorGraphAdjacencySourceSnapshot) error {
	if layerCount <= 0 {
		return fmt.Errorf("collections: column vector graph adjacency layer count=%d must be positive", layerCount)
	}
	if len(sources) != layerCount {
		return fmt.Errorf("collections: column vector graph adjacency layer sources=%d want layer_count=%d", len(sources), layerCount)
	}
	for layer, source := range sources {
		if !source.Present {
			return fmt.Errorf("collections: column vector graph adjacency layer source[%d] is absent", layer)
		}
		if source.Layer != layer {
			return fmt.Errorf("collections: column vector graph adjacency layer source[%d] has layer=%d", layer, source.Layer)
		}
		if err := validateColumnVectorGraphAdjacencySourceSnapshot(source); err != nil {
			return fmt.Errorf("collections: column vector graph adjacency layer source[%d]: %w", layer, err)
		}
	}
	return nil
}

func validateColumnVectorGraphLayer0AdjacencySourceSnapshot(source columnVectorGraphLayer0AdjacencySourceSnapshot) error {
	if source.Present && source.Layer != 0 {
		return fmt.Errorf("collections: column vector graph layer-0 adjacency source layer=%d want 0", source.Layer)
	}
	return validateColumnVectorGraphAdjacencySourceSnapshot(source)
}

func validateColumnVectorGraphAdjacencySourceSnapshot(source columnVectorGraphAdjacencySourceSnapshot) error {
	if !source.Present {
		return nil
	}
	if source.Schema == "" || source.ColumnName == "" || source.ValueType == "" || source.Encoding == "" {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d missing schema metadata", source.Layer)
	}
	if source.Layer < 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d must be non-negative", source.Layer)
	}
	if source.SourceSchemaHash == 0 || source.BaseManifestGeneration == 0 || source.BaseManifestChecksum == 0 || source.BaseSchemaHash == 0 || source.GraphSchemaHash == 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d missing identity", source.Layer)
	}
	if source.RowCount < 0 || source.ValuesCount < 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d row/value count must be non-negative", source.Layer)
	}
	if source.RowCount == math.MaxInt || int64(source.RowCount) > math.MaxInt64/8-1 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d row_count=%d offsets byte count overflows int64", source.Layer, source.RowCount)
	}
	if source.OffsetsBytes <= 0 || source.ValuesBytes < 0 || source.PaddingBytes < 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d invalid byte accounting", source.Layer)
	}
	if source.AssetBytes <= 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d asset bytes must be positive", source.Layer)
	}
	if source.GraphAssetGeneration == 0 || source.GraphAssetPartID == 0 || source.GraphAssetFileID == 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d missing graph asset identity", source.Layer)
	}
	if source.GraphAssetOffset < 0 || source.GraphAssetLength <= 0 {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d invalid graph asset byte identity", source.Layer)
	}
	if err := validateColumnAssetRefForPlan(source.Ref); err != nil {
		return err
	}
	if source.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d ref kind=%q want %q", source.Layer, source.Ref.Kind, ColumnAssetKindTCS1TypedColumnPart)
	}
	if source.AssetBytes != source.Ref.Length {
		return fmt.Errorf("collections: column vector graph adjacency source layer=%d asset bytes=%d does not match ref length=%d", source.Layer, source.AssetBytes, source.Ref.Length)
	}
	return nil
}

func columnVectorGraphPhysicalColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, error) {
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return ColumnStoreConfig{}, err
	}
	if normalizedDef.Strategy != VectorIndexStrategyColumnGraph {
		return ColumnStoreConfig{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", normalizedDef.Name, normalizedDef.Strategy)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, errors.New("collections: column vector graph physical config requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, errors.New("collections: column vector graph physical config requires base asset manager")
	}
	// Legacy physical graph row assets kept adjacency in the adjacency_list
	// row-image format for compatibility/fallback only. Current rebuilds publish
	// HNSW adjacency through vector-index state uint32_list assets and do not need
	// this physical graph config for healthy search.
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: columnVectorGraphVectorColumnName, Path: normalizedDef.Field, ValueType: ColumnStoreValueFloat32Vector, VectorDims: normalizedDef.Dimensions, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
			{Name: columnVectorGraphInvNormColumnName, Path: normalizedDef.Field + "_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: columnVectorGraphAdjacencyColumnName, Path: normalizedDef.Field + "_neighbors", ValueType: ColumnStoreValueAdjacencyList, FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian},
		},
		RetainedPayload: ColumnRetainedPayloadNone,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      base.AssetManager.Kind,
			Namespace: base.AssetManager.Namespace,
		},
	})
	if err != nil {
		return ColumnStoreConfig{}, err
	}
	return *cfg, nil
}

func prepareColumnVectorGraphPhysicalAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID, appliedCommandLSN uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedPhysicalAsset, error) {
	if assetRootDir == "" {
		return columnVectorGraphPreparedPhysicalAsset{}, errors.New("collections: column vector graph physical asset requires asset root dir")
	}
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, base, def)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	declared := make([]columnDeclaredRow, len(rows))
	for i, row := range rows {
		if len(row.ID) == 0 {
			return columnVectorGraphPreparedPhysicalAsset{}, fmt.Errorf("collections: column vector graph row[%d] missing document id", i)
		}
		if len(row.Vector) != normalizedDef.Dimensions {
			return columnVectorGraphPreparedPhysicalAsset{}, fmt.Errorf("collections: column vector graph row[%d] vector dims=%d want %d", i, len(row.Vector), normalizedDef.Dimensions)
		}
		declared[i] = columnDeclaredRow{
			ID: bytes.Clone(row.ID),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: append([]float32(nil), row.Vector...)},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: row.InvNorm},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: append([]uint32(nil), row.Adjacency...)},
			},
		}
	}
	encoded, summary, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        collection,
		Namespace:         graphCfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        graphCfg.SchemaHash,
		Columns:           graphCfg.Columns,
		Rows:              declared,
	})
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	ref, err := writeColumnVectorGraphPhysicalAssetToManager(assetRootDir, graphCfg, encoded, generation, partID)
	if err != nil {
		return columnVectorGraphPreparedPhysicalAsset{}, err
	}
	return columnVectorGraphPreparedPhysicalAsset{
		AssetRootDir: assetRootDir,
		Config:       graphCfg,
		Ref:          ref,
		Bytes:        summary.PayloadBytes,
		RowCount:     summary.RowCount,
	}, nil
}

func writeColumnVectorGraphPhysicalAssetToManager(rootDir string, cfg ColumnStoreConfig, payload []byte, generation, partID uint64) (ColumnAssetRef, error) {
	if len(payload) == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph physical asset payload is empty")
	}
	if generation == 0 || partID == 0 {
		return ColumnAssetRef{}, errors.New("collections: column_graph physical asset append requires generation and part_id")
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(rootDir, cfg)
	if err != nil {
		return ColumnAssetRef{}, err
	}
	ref, appendErr := appender.append(payload, generation, partID)
	closeErr := appender.close()
	if appendErr != nil {
		return ColumnAssetRef{}, errors.Join(appendErr, closeErr)
	}
	return ref, closeErr
}

func (c *Collection) columnGraphVectorIndexStatus(name string) (VectorIndexStatus, error) {
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	defer func() { _ = snap.Close() }()
	return c.columnGraphVectorIndexStatusAtSnapshot(name, snap)
}

func (c *Collection) columnGraphVectorIndexStatusAtSnapshot(name string, snap *backenddb.Snapshot) (VectorIndexStatus, error) {
	if c == nil {
		return VectorIndexStatus{}, errCollectionNil
	}
	if c.db == nil {
		return VectorIndexStatus{}, errCollectionDBNil
	}
	if snap == nil {
		return VectorIndexStatus{}, backenddb.ErrClosed
	}
	status := VectorIndexStatus{
		Name:     name,
		Strategy: VectorIndexStrategyColumnGraph,
	}
	catalog, err := c.catalogForSnapshot(snap)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	if catalog == nil {
		return VectorIndexStatus{}, errCollectionNotFound
	}
	def, ok := findVectorIndex(catalog.meta.VectorIndexes, name)
	if ok {
		status.Name = def.Name
		status.Strategy = def.Strategy
	} else {
		return VectorIndexStatus{}, ErrIndexNotFound
	}
	switch def.Strategy {
	case VectorIndexStrategyNativeRuntime:
		status.State = VectorIndexStateNativeRuntime
		status.Reason = VectorIndexReasonNativeRuntime
		return status, nil
	case VectorIndexStrategyColumnGraph:
	default:
		return VectorIndexStatus{}, fmt.Errorf("collections: unsupported vector index strategy %q", def.Strategy)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || !cfg.Enabled || cfg.AssetManager == nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonPhysicalColumnAssetSupportMissing
		status.RebuildNeeded = true
		return status, nil
	}
	if cfg.ActiveManifest == nil || cfg.RecoveryAuthoritativeManifest == nil {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	if !columnManifestIdentityValueEqual(*cfg.ActiveManifest, *cfg.RecoveryAuthoritativeManifest) {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	rootID := catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name))
	if rootID == 0 {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	if err := validateColumnManifestIdentityAtRoot(snap, rootID, *cfg.ActiveManifest); err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, rootID)
	if err != nil {
		return VectorIndexStatus{}, err
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	if err := validateColumnManifestSnapshot(manifest, records, *cfg, *cfg.ActiveManifest, catalog.meta.Name, "column vector graph status"); err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphRebuildNeeded
		status.RebuildNeeded = true
		return status, nil
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		status.State = VectorIndexStateColumnGraphUnavailable
		status.Reason = VectorIndexReasonColumnGraphCorrupt
		status.RebuildNeeded = true
		return columnGraphVectorIndexStatusError(status, err)
	}
	baseChecksum, baseChecksumOK := uint64(0), false
	if computed, err := columnVectorGraphBaseManifestChecksum(manifest, records, *cfg); err == nil {
		baseChecksum = computed
		baseChecksumOK = true
	}
	var loadedState *columnVectorIndexStateSnapshot
	if stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name); ok {
		state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
		if err != nil {
			status.State = VectorIndexStateColumnGraphUnavailable
			status.Reason = VectorIndexReasonColumnGraphCorrupt
			status.RebuildNeeded = true
			return columnGraphVectorIndexStatusError(status, err)
		}
		stateMatch := columnVectorIndexStateMatchMismatch
		if baseChecksumOK {
			stateMatch = columnVectorIndexStateMatchStatusWithBaseChecksum(state, def, *cfg, baseChecksum)
		}
		switch stateMatch {
		case columnVectorIndexStateMatchLoaded:
			if !columnVectorIndexStateMatchesGraph(state, graph) {
				status.State = VectorIndexStateColumnGraphRebuildNeeded
				status.Reason = VectorIndexReasonColumnGraphAssetMismatch
				status.RebuildNeeded = true
				return status, nil
			}
			if err := validateColumnVectorIndexStateAssetsForStatus(c.db.ColumnAssetRootDir(), catalog.meta.Name, *cfg, def, state, graph); err != nil {
				status.State = VectorIndexStateColumnGraphUnavailable
				status.Reason = VectorIndexReasonColumnGraphCorrupt
				status.RebuildNeeded = true
				return columnGraphVectorIndexStatusError(status, err)
			}
			if err := validateColumnVectorGraphInvNormStateAssetIfPresent(c.db.ColumnAssetRootDir(), catalog.meta.Name, *cfg, def, graph, state); err != nil {
				status.State = VectorIndexStateColumnGraphUnavailable
				status.Reason = VectorIndexReasonColumnGraphCorrupt
				status.RebuildNeeded = true
				return columnGraphVectorIndexStatusError(status, err)
			}
			loadedState = &state
		case columnVectorIndexStateMatchUnsupportedVisibility:
			status.State = VectorIndexStateColumnGraphRebuildNeeded
			status.Reason = VectorIndexReasonColumnGraphUnsupportedVisibility
			status.RebuildNeeded = true
			return status, nil
		default:
			status.State = VectorIndexStateColumnGraphRebuildNeeded
			status.Reason = VectorIndexReasonColumnGraphAssetMismatch
			status.RebuildNeeded = true
			return status, nil
		}
	}
	graphMatch := columnVectorGraphManifestMatchMismatch
	if baseChecksumOK {
		graphMatch = columnVectorGraphManifestMatchStatusWithBaseChecksum(catalog.meta.Name, graph, def, *cfg, baseChecksum)
	}
	switch graphMatch {
	case columnVectorGraphManifestMatchLoaded:
	case columnVectorGraphManifestMatchUnsupportedVisibility:
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphUnsupportedVisibility
		status.RebuildNeeded = true
		return status, nil
	default:
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphAssetMismatch
		status.RebuildNeeded = true
		return status, nil
	}
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		if err := validateColumnVectorGraphAssetRefAvailable(c.db.ColumnAssetRootDir(), graph.AssetRef); err != nil {
			status.State = VectorIndexStateColumnGraphUnavailable
			status.Reason = VectorIndexReasonColumnGraphCorrupt
			status.RebuildNeeded = true
			return columnGraphVectorIndexStatusError(status, err)
		}
	}
	if loadedState == nil {
		status.State = VectorIndexStateColumnGraphRebuildNeeded
		status.Reason = VectorIndexReasonColumnGraphAssetMismatch
		status.RebuildNeeded = true
		return status, nil
	}
	if !columnVectorGraphManifestHasPhysicalAsset(graph) && graph.RowCount > 0 {
		if _, ok := findColumnVectorGraphInvNormStateAsset(*loadedState); !ok {
			status.State = VectorIndexStateColumnGraphRebuildNeeded
			status.Reason = VectorIndexReasonColumnGraphAssetMismatch
			status.RebuildNeeded = true
			return status, nil
		}
		if !columnVectorGraphRowRefStatePresent(*loadedState) || !columnVectorGraphDocumentIDStatePresent(*loadedState) {
			status.State = VectorIndexStateColumnGraphRebuildNeeded
			status.Reason = VectorIndexReasonColumnGraphAssetMismatch
			status.RebuildNeeded = true
			return status, nil
		}
		if _, _, typedVectorOwner, err := columnVectorGraphTypedColumnVectorField(*cfg, graph.Field, graph.Dimensions); err != nil || !typedVectorOwner {
			status.State = VectorIndexStateColumnGraphRebuildNeeded
			status.Reason = VectorIndexReasonColumnGraphAssetMismatch
			status.RebuildNeeded = true
			return status, nil
		}
	}
	// Healthy loaded status is gated by TVIS/base typed-column state. A remaining
	// graph row asset, when present, is compatibility storage and is not required
	// for current-format search.
	bytesDisk := columnVectorGraphStorageBytesWithState(graph, *loadedState)
	status.State = VectorIndexStateColumnGraphLoaded
	status.Loaded = true
	status.Stats = VectorIndexStats{
		Name:           def.Name,
		Field:          def.Field,
		Metric:         def.Metric,
		Encoding:       def.Encoding,
		Dimensions:     def.Dimensions,
		M:              def.M,
		EfConstruction: def.EfConstruction,
		EfSearch:       def.EfSearch,
		Nodes:          graph.RowCount,
		LiveDocs:       graph.RowCount,
		BytesDisk:      bytesDisk,
		Epoch:          graph.BaseManifestGeneration,
	}
	return status, nil
}

func columnGraphVectorIndexStatusError(status VectorIndexStatus, err error) (VectorIndexStatus, error) {
	if err == nil {
		return status, nil
	}
	return status, fmt.Errorf("collections: column_graph vector index %q status=%s reason=%s: %w", status.Name, status.State, status.Reason, err)
}

type columnVectorGraphManifestMatch uint8

const (
	columnVectorGraphManifestMatchMismatch columnVectorGraphManifestMatch = iota
	columnVectorGraphManifestMatchLoaded
	columnVectorGraphManifestMatchUnsupportedVisibility
)

func columnVectorGraphManifestMatchesDefinition(collection string, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord) bool {
	return columnVectorGraphManifestMatchStatus(collection, graph, def, cfg, manifest, records) == columnVectorGraphManifestMatchLoaded
}

func columnVectorGraphManifestMatchStatus(collection string, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord) columnVectorGraphManifestMatch {
	baseChecksum, err := columnVectorGraphBaseManifestChecksum(manifest, records, cfg)
	if err != nil {
		return columnVectorGraphManifestMatchMismatch
	}
	return columnVectorGraphManifestMatchStatusWithBaseChecksum(collection, graph, def, cfg, baseChecksum)
}

func columnVectorGraphManifestMatchStatusWithBaseChecksum(collection string, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, cfg ColumnStoreConfig, baseChecksum uint64) columnVectorGraphManifestMatch {
	if cfg.ActiveManifest == nil {
		return columnVectorGraphManifestMatchMismatch
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return columnVectorGraphManifestMatchMismatch
	}
	if !columnVectorGraphCoreParametersMatch(&graph, &def, &cfg, &graphCfg) {
		return columnVectorGraphManifestMatchMismatch
	}
	if graph.BaseManifestGeneration != cfg.ActiveManifest.Generation {
		return columnVectorGraphManifestMatchUnsupportedVisibility
	}
	if graph.BaseManifestChecksum != baseChecksum {
		return columnVectorGraphManifestMatchMismatch
	}
	// Legacy graph-specific adjacency-source metadata is intentionally not part of
	// the cheap loaded-status match. Healthy search state is validated through the
	// vector-index state manifest; corrupt legacy source metadata must not make the
	// row-asset compatibility record look like the target architecture.
	return columnVectorGraphManifestMatchLoaded
}

func columnVectorGraphCoreParametersMatch(graph *columnVectorGraphManifestSnapshot, def *VectorIndexDefinition, cfg *ColumnStoreConfig, graphCfg *ColumnStoreConfig) bool {
	return columnVectorGraphDefinitionParametersMatch(graph, def) &&
		columnVectorGraphAssetParametersMatch(graph, cfg, graphCfg)
}

func columnVectorGraphDefinitionParametersMatch(graph *columnVectorGraphManifestSnapshot, def *VectorIndexDefinition) bool {
	return graph.IndexName == def.Name &&
		graph.Field == def.Field &&
		graph.Metric == def.Metric &&
		graph.Encoding == def.Encoding &&
		graph.Dimensions == def.Dimensions &&
		graph.M == def.M &&
		graph.EfConstruction == def.EfConstruction &&
		graph.EfSearch == def.EfSearch
}

func columnVectorGraphAssetParametersMatch(graph *columnVectorGraphManifestSnapshot, cfg *ColumnStoreConfig, graphCfg *ColumnStoreConfig) bool {
	if graph.BaseSchemaHash != cfg.SchemaHash || graph.GraphSchemaHash != graphCfg.SchemaHash {
		return false
	}
	if !columnVectorGraphManifestHasPhysicalAsset(*graph) {
		return true
	}
	return graph.AssetRef.Kind == ColumnAssetKindTCS1PartImage &&
		graph.AssetRef.Namespace == graphCfg.AssetManager.Namespace &&
		graph.AssetRef.Generation == graph.BaseManifestGeneration
}

func columnVectorGraphBaseManifestChecksum(manifest columnManifestSnapshot, records []columnManifestRecord, cfg ColumnStoreConfig) (uint64, error) {
	activeRecords, err := activeColumnManifestRecordsForScan(records, manifest.Generation)
	if err != nil {
		return 0, err
	}
	baseRecords := activeRecords[:0]
	for _, record := range activeRecords {
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) || bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			continue
		}
		baseRecords = append(baseRecords, record)
	}
	sortColumnManifestRecords(baseRecords)
	return checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{
		Collection:        manifest.Collection,
		ColumnStore:       cfg,
		Operation:         manifest.Operation,
		AppliedCommandLSN: manifest.AppliedCommandLSN,
	}, manifest.Generation, baseRecords), nil
}

func columnVectorGraphAssetRefsFromManifestRecordsForReachability(records []columnManifestRecord, activeGeneration uint64, expectedNamespace string, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) ([]ColumnAssetRef, error) {
	var refs []ColumnAssetRef
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			continue
		}
		if !retainColumnManifestVectorGraphRecordForWrite(record.key, activeVectorIndexesKnown, activeVectorIndexes) {
			continue
		}
		graph, err := decodeColumnVectorGraphManifestRecord(record.value)
		if err != nil {
			return nil, err
		}
		graphRefs, err := columnVectorGraphManifestAssetRefsForScan(graph, activeGeneration, expectedNamespace)
		if err != nil {
			return nil, err
		}
		refs = append(refs, graphRefs...)
	}
	stateRefs, err := columnVectorIndexStateAssetRefsFromManifestRecordsForReachability(records, activeGeneration, expectedNamespace, activeVectorIndexesKnown, activeVectorIndexes)
	if err != nil {
		return nil, err
	}
	refs = append(refs, stateRefs...)
	return refs, nil
}

func columnVectorGraphManifestAssetRefsForScan(graph columnVectorGraphManifestSnapshot, activeGeneration uint64, expectedNamespace string) ([]ColumnAssetRef, error) {
	refs := make([]ColumnAssetRef, 0, 1+len(graph.AdjacencyLayerSources))
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		if graph.AssetRef.Generation > activeGeneration {
			return nil, fmt.Errorf("collections: column vector graph asset generation=%d is newer than active manifest generation=%d", graph.AssetRef.Generation, activeGeneration)
		}
		if graph.BaseManifestGeneration != graph.AssetRef.Generation {
			return nil, fmt.Errorf("collections: column vector graph base manifest generation=%d does not match asset generation=%d", graph.BaseManifestGeneration, graph.AssetRef.Generation)
		}
		if graph.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
			return nil, fmt.Errorf("collections: column vector graph asset kind=%q want %q", graph.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
		}
		if graph.AssetRef.Namespace != expectedNamespace {
			return nil, fmt.Errorf("collections: column vector graph asset namespace=%q want %q", graph.AssetRef.Namespace, expectedNamespace)
		}
		if err := validateColumnAssetRefForPlan(graph.AssetRef); err != nil {
			return nil, err
		}
		refs = append(refs, graph.AssetRef)
	}
	if len(graph.AdjacencyLayerSources) > 0 {
		for _, source := range graph.AdjacencyLayerSources {
			if columnVectorGraphAdjacencySourceRefEligibleForScan(graph, source, activeGeneration, expectedNamespace) {
				refs = append(refs, source.Ref)
			}
		}
	} else if columnVectorGraphLayer0AdjacencySourceRefEligibleForScan(graph, activeGeneration, expectedNamespace) {
		refs = append(refs, graph.Layer0AdjacencySource.Ref)
	}
	return refs, nil
}

func columnVectorGraphLayer0AdjacencySourceRefEligibleForScan(graph columnVectorGraphManifestSnapshot, activeGeneration uint64, expectedNamespace string) bool {
	return columnVectorGraphAdjacencySourceRefEligibleForScan(graph, graph.Layer0AdjacencySource, activeGeneration, expectedNamespace)
}

func columnVectorGraphAdjacencySourceRefEligibleForScan(graph columnVectorGraphManifestSnapshot, source columnVectorGraphLayer0AdjacencySourceSnapshot, activeGeneration uint64, expectedNamespace string) bool {
	if !source.Present || source.Ref.Generation > activeGeneration || source.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart || source.Ref.Namespace != expectedNamespace || source.Ref.Generation != graph.BaseManifestGeneration {
		return false
	}
	return validateColumnAssetRefForPlan(source.Ref) == nil
}

func validateColumnVectorGraphAssetRefAvailable(rootDir string, ref ColumnAssetRef) error {
	if err := validateColumnAssetRefForPlan(ref); err != nil {
		return err
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("collections: column vector graph asset segment %q is a directory", path)
	}
	if ref.Offset < 0 || ref.Length <= 0 || ref.Offset > math.MaxInt64-ref.Length {
		return fmt.Errorf("collections: invalid column vector graph asset range offset=%d length=%d", ref.Offset, ref.Length)
	}
	if info.Size() < ref.Offset+ref.Length {
		return fmt.Errorf("collections: column vector graph asset segment size=%d shorter than ref end=%d", info.Size(), ref.Offset+ref.Length)
	}
	return nil
}
