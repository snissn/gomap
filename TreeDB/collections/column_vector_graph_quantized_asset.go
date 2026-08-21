package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/brq"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/quantizedasset"
	"github.com/snissn/gomap/TreeDB/internal/rabitq"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

const (
	columnVectorGraphQuantizedCodesColumnName                = "codes"
	columnVectorGraphQuantizedPackedCodesColumnName          = "packed_codes"
	columnVectorGraphQuantizedCodeCountColumnName            = "code_count"
	columnVectorGraphQuantizedDotProductInvColumnName        = "quantized_dot_product_inv"
	columnVectorGraphQuantizedScalarU8AlphaColumnName        = "scalar_u8_alpha"
	columnVectorGraphQuantizedGranuleRowCountColumnName      = "granule_row_count"
	columnVectorGraphQuantizedRabitQPathConfigHashFormat     = "%s_quantized_rabitq_1bit_%016x_%s"
	columnVectorGraphQuantizedBRQPathConfigHashFormat        = "%s_quantized_brq_1bit_%016x_%s"
	columnVectorGraphQuantizedScalarU8UnsupportedVersionText = "scalar_u8 version=%d is unsupported"
)

type columnVectorGraphPreparedQuantizedAsset struct {
	Definition QuantizedVectorIndexDefinition
	Role       string
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

func (r *columnVectorGraphQuantizedAssetResource) close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	handle := r.handle
	r.handle = nil
	readCache := r.readCache
	r.readCache = nil
	r.mu.Unlock()
	var closeErr error
	if handle != nil {
		closeErr = errors.Join(closeErr, handle.Release())
	}
	if readCache != nil {
		closeErr = errors.Join(closeErr, readCache.close())
	}
	return closeErr
}

func (s columnVectorGraphQuantizedAssetLoadStatus) close() error {
	if !s.ownsResource || s.resource == nil {
		return nil
	}
	return s.resource.close()
}

func closeColumnVectorGraphQuantizedAssetLoadStatuses(statuses map[string]columnVectorGraphQuantizedAssetLoadStatus) error {
	var closeErr error
	for name, status := range statuses {
		closeErr = errors.Join(closeErr, status.close())
		delete(statuses, name)
	}
	return closeErr
}

type columnVectorGraphQuantizedAssetLoadStatus struct {
	Definition       QuantizedVectorIndexDefinition
	Asset            columnVectorIndexStateAssetSnapshot
	Prepared         *quantizedasset.Prepared
	ScalarU8Alpha    *columnVectorGraphScalarU8AlphaLookup
	ScalarU8CodeSums []uint32
	RabitQPlan       *rabitq.Plan
	BRQPlan          *brq.Plan
	Err              error
	Health           columnVectorGraphQuantizedAssetHealth
	OpenNanos        uint64
	MappedBytes      uint64
	HeapCopyBytes    uint64
	ActiveHandles    int64

	resource     *columnVectorGraphQuantizedAssetResource
	ownsResource bool
}

type columnVectorGraphQuantizedAssetResource struct {
	mu        sync.Mutex
	closed    bool
	manager   *mappedresource.Manager
	handle    *mappedresource.Handle
	readCache *columnPhysicalAssetReadCache
}

type columnVectorGraphScalarU8AlphaLookup struct {
	rows               int
	granules           int
	alphaPayload       []byte
	rowCountPayload    []byte
	firstRows          []int
	uniformGranuleRows int
}

func (l *columnVectorGraphScalarU8AlphaLookup) Rows() int {
	if l == nil {
		return 0
	}
	return l.rows
}

func (l *columnVectorGraphScalarU8AlphaLookup) Granules() int {
	if l == nil {
		return 0
	}
	return l.granules
}

func (l *columnVectorGraphScalarU8AlphaLookup) AlphaForGranule(granule int) (float32, bool) {
	if l == nil || granule < 0 || granule >= l.granules || len(l.alphaPayload) != l.granules*4 {
		return 0, false
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(l.alphaPayload[granule*4 : granule*4+4])), true
}

func (l *columnVectorGraphScalarU8AlphaLookup) RowCountForGranule(granule int) (uint32, bool) {
	if l == nil || granule < 0 || granule >= l.granules || len(l.rowCountPayload) != l.granules*4 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(l.rowCountPayload[granule*4 : granule*4+4]), true
}

func (l *columnVectorGraphScalarU8AlphaLookup) AlphaForRow(row int) (float32, int, bool) {
	granule, ok := l.granuleForRow(row)
	if !ok {
		return 0, 0, false
	}
	alpha, ok := l.AlphaForGranule(granule)
	return alpha, granule, ok
}

func (l *columnVectorGraphScalarU8AlphaLookup) granuleForRow(row int) (int, bool) {
	if l == nil || row < 0 || row >= l.rows || l.granules < 0 || len(l.firstRows) != l.granules+1 {
		return 0, false
	}
	if l.uniformGranuleRows > 0 {
		granule := row / l.uniformGranuleRows
		if granule >= l.granules {
			granule = l.granules - 1
		}
		if granule >= 0 && row >= l.firstRows[granule] && row < l.firstRows[granule+1] {
			return granule, true
		}
	}
	lo, hi := 0, l.granules
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if l.firstRows[mid+1] > row {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	granule := lo
	if granule < 0 || granule >= l.granules || row < l.firstRows[granule] || row >= l.firstRows[granule+1] {
		return 0, false
	}
	return granule, true
}

func (l *columnVectorGraphScalarU8AlphaLookup) validateForScoring(rows int) error {
	if l == nil {
		return fmt.Errorf("%w: scalar_u8 alpha lookup is missing", errColumnVectorGraphQuantizedAssetMissing)
	}
	if rows < 0 || l.rows != rows {
		return fmt.Errorf("%w: scalar_u8 alpha rows=%d want graph rows=%d", errColumnVectorGraphQuantizedAssetStale, l.rows, rows)
	}
	if !l.validShapeForRows(rows) {
		return fmt.Errorf("%w: scalar_u8 alpha lookup shape is invalid", errColumnVectorGraphQuantizedAssetInvalid)
	}
	for granule := 0; granule < l.granules; granule++ {
		alpha, ok := l.AlphaForGranule(granule)
		if !ok || !validColumnVectorGraphScalarU8Alpha(alpha) {
			return fmt.Errorf("%w: scalar_u8 alpha granule %d value=%v is invalid", errColumnVectorGraphQuantizedAssetInvalid, granule, alpha)
		}
		count, ok := l.RowCountForGranule(granule)
		if !ok || count == 0 {
			return fmt.Errorf("%w: scalar_u8 alpha granule %d row_count=%d is invalid", errColumnVectorGraphQuantizedAssetInvalid, granule, count)
		}
	}
	return nil
}

func (l *columnVectorGraphScalarU8AlphaLookup) validShapeForRows(rows int) bool {
	if l == nil || rows < 0 || l.rows != rows || l.granules < 0 || len(l.alphaPayload) != l.granules*4 || len(l.rowCountPayload) != l.granules*4 || len(l.firstRows) != l.granules+1 {
		return false
	}
	if rows > 0 && l.granules == 0 {
		return false
	}
	if l.granules == 0 {
		return rows == 0
	}
	return l.firstRows[0] == 0 && l.firstRows[l.granules] == rows
}

var columnVectorGraphQuantizedAssetForceReadAtFallbackForTest atomic.Bool

func columnVectorGraphQuantizedAssetID(q QuantizedVectorIndexDefinition) string {
	assetID, err := columnVectorGraphQuantizedAssetIDChecked(q)
	if err != nil {
		return "quantized/" + q.Name + "/scalar_u8/invalid_calibration/codes"
	}
	return assetID
}

func columnVectorGraphQuantizedAssetIDChecked(q QuantizedVectorIndexDefinition) (string, error) {
	switch q.Codec {
	case brq.CodecName:
		return "quantized/" + q.Name + "/brq_1bit/packed_codes", nil
	case rabitq.CodecName:
		return "quantized/" + q.Name + "/packed_codes", nil
	case "", QuantizedVectorCodecScalarU8:
		cfgHash, err := scalarU8CalibrationConfigHashForAssetID(q)
		if err != nil {
			return "", err
		}
		if cfgHash != 0 {
			return fmt.Sprintf("quantized/%s/scalar_u8/%016x/codes", q.Name, cfgHash), nil
		}
		return "quantized/" + q.Name + "/codes", nil
	default:
		return "quantized/" + q.Name + "/codes", nil
	}
}

func columnVectorGraphQuantizedCodesAssetID(q QuantizedVectorIndexDefinition) string {
	return columnVectorGraphQuantizedAssetID(q)
}

func columnVectorGraphScalarU8AlphaAssetID(q QuantizedVectorIndexDefinition) string {
	assetID, err := columnVectorGraphScalarU8AlphaAssetIDChecked(q)
	if err != nil {
		return ""
	}
	return assetID
}

func columnVectorGraphScalarU8AlphaAssetIDChecked(q QuantizedVectorIndexDefinition) (string, error) {
	if q.Codec != QuantizedVectorCodecScalarU8 || scalarU8CalibrationIsLegacy(q) {
		return "", nil
	}
	cfgHash, err := scalarU8CalibrationConfigHashForAssetID(q)
	if err != nil {
		return "", err
	}
	if cfgHash != 0 {
		return fmt.Sprintf("quantized/%s/scalar_u8/%016x/alpha", q.Name, cfgHash), nil
	}
	return "", nil
}

func columnVectorIndexStateAssetIsQuantized(asset columnVectorIndexStateAssetSnapshot) bool {
	return asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes || asset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha
}

func columnVectorGraphQuantizedAssetRefIdentity(ref ColumnAssetRef) quantizedasset.AssetRefIdentity {
	return quantizedasset.AssetRefIdentity{Present: true, Kind: string(ref.Kind), Namespace: ref.Namespace, Generation: ref.Generation, PartID: ref.PartID, FileID: ref.FileID, Offset: ref.Offset, Length: ref.Length, Checksum: ref.Checksum}
}

func prepareColumnVectorGraphQuantizedAssets(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, generation, firstPartID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedQuantizedAsset, error) {
	return prepareColumnVectorGraphQuantizedAssetsWithStableAuthority(assetRootDir, collection, base, def, graph, generation, firstPartID, rows, nil)
}

func prepareColumnVectorGraphQuantizedAssetsWithStableAuthority(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, generation, firstPartID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) ([]columnVectorGraphPreparedQuantizedAsset, error) {
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
		prepared, err := prepareColumnVectorGraphQuantizedAssetWithStableAuthority(assetRootDir, collection, base, def, graph, q, generation, partID, rows, authority)
		if err != nil {
			return nil, err
		}
		out = append(out, prepared...)
		if len(prepared) != 0 {
			partID = nextColumnVectorGraphPartIDAfter(partID, prepared[len(prepared)-1].Ref.PartID)
		}
	}
	return out, nil
}

func prepareColumnVectorGraphQuantizedAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) ([]columnVectorGraphPreparedQuantizedAsset, error) {
	return prepareColumnVectorGraphQuantizedAssetWithStableAuthority(assetRootDir, collection, base, def, graph, q, generation, partID, rows, nil)
}

func prepareColumnVectorGraphQuantizedAssetWithStableAuthority(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) ([]columnVectorGraphPreparedQuantizedAsset, error) {
	var alphaMetadata columnVectorGraphScalarU8AlphaMetadata
	var payload []byte
	var sourceCfg ColumnStoreConfig
	var err error
	if q.Codec == QuantizedVectorCodecScalarU8 && !scalarU8CalibrationIsLegacy(q) {
		alphaMetadata, err = buildColumnVectorGraphScalarU8AlphaMetadata(def, q, rows)
		if err != nil {
			return nil, err
		}
		payload, sourceCfg, err = prepareColumnVectorGraphScalarU8QuantizedCodesPayloadWithAlphaMetadata(collection, base, def, q, partID, rows, alphaMetadata, true)
	} else {
		payload, sourceCfg, err = prepareColumnVectorGraphQuantizedCodesPayload(collection, base, def, q, partID, rows)
	}
	if err != nil {
		return nil, err
	}
	codesAssetID, err := columnVectorGraphQuantizedAssetIDChecked(q)
	if err != nil {
		return nil, fmt.Errorf("collections: column_graph quantized index %q asset identity: %w", q.Name, err)
	}
	prepared, err := appendColumnVectorGraphQuantizedPreparedAssetWithStableAuthority(assetRootDir, sourceCfg, q, columnVectorIndexStateAssetRoleQuantizedCodes, codesAssetID, generation, partID, len(rows), payload, authority)
	if err != nil {
		return nil, err
	}
	out := []columnVectorGraphPreparedQuantizedAsset{prepared}
	if q.Codec == QuantizedVectorCodecScalarU8 && !scalarU8CalibrationIsLegacy(q) {
		alphaPartID := nextColumnVectorGraphPartIDAfter(partID, prepared.Ref.PartID)
		alphaPayload, alphaCfg, err := prepareColumnVectorGraphScalarU8AlphaPayload(collection, base, def, q, alphaPartID, alphaMetadata)
		if err != nil {
			return nil, err
		}
		alphaAssetID, err := columnVectorGraphScalarU8AlphaAssetIDChecked(q)
		if err != nil {
			return nil, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha asset identity: %w", q.Name, err)
		}
		alphaPrepared, err := appendColumnVectorGraphQuantizedPreparedAssetWithStableAuthority(assetRootDir, alphaCfg, q, columnVectorIndexStateAssetRoleQuantizedAlpha, alphaAssetID, generation, alphaPartID, len(alphaMetadata.Alphas), alphaPayload, authority)
		if err != nil {
			return nil, err
		}
		out = append(out, alphaPrepared)
	}
	return out, nil
}

func appendColumnVectorGraphQuantizedPreparedAsset(assetRootDir string, sourceCfg ColumnStoreConfig, q QuantizedVectorIndexDefinition, role, assetID string, generation, partID uint64, rows int, payload []byte) (columnVectorGraphPreparedQuantizedAsset, error) {
	return appendColumnVectorGraphQuantizedPreparedAssetWithStableAuthority(assetRootDir, sourceCfg, q, role, assetID, generation, partID, rows, payload, nil)
}

func appendColumnVectorGraphQuantizedPreparedAssetWithStableAuthority(assetRootDir string, sourceCfg ColumnStoreConfig, q QuantizedVectorIndexDefinition, role, assetID string, generation, partID uint64, rows int, payload []byte, authority *columnVectorGraphStableResourceAccumulator) (columnVectorGraphPreparedQuantizedAsset, error) {
	if assetID == "" {
		return columnVectorGraphPreparedQuantizedAsset{}, fmt.Errorf("collections: column_graph quantized index %q role %q missing asset id", q.Name, role)
	}
	appender, err := newColumnVectorGraphAssetAppender(assetRootDir, sourceCfg, authority)
	if err != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, err
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, alignment)
	closeErr := closeColumnVectorGraphAssetAppender(appender, authority)
	if appendErr != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, errors.Join(appendErr, closeErr)
	}
	if closeErr != nil {
		return columnVectorGraphPreparedQuantizedAsset{}, closeErr
	}
	if ref.Namespace != sourceCfg.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != partID || ref.Length != int64(len(payload)) {
		return columnVectorGraphPreparedQuantizedAsset{}, fmt.Errorf("collections: invalid column_graph quantized asset ref %+v", ref)
	}
	return columnVectorGraphPreparedQuantizedAsset{Definition: q, Role: role, AssetID: assetID, Config: sourceCfg, Ref: ref, Bytes: ref.Length, Rows: rows, SchemaHash: sourceCfg.SchemaHash}, nil
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
	case brq.CodecName:
		if q.Version != brq.CodecVersion {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q brq_1bit version=%d is unsupported", q.Name, q.Version)
		}
		return prepareColumnVectorGraphBRQQuantizedCodesPayload(collection, base, def, q, partID, rows)
	default:
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q codec %q is unsupported", q.Name, q.Codec)
	}
}

func prepareColumnVectorGraphScalarU8QuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	return prepareColumnVectorGraphScalarU8QuantizedCodesPayloadWithAlphaMetadata(collection, base, def, q, partID, rows, columnVectorGraphScalarU8AlphaMetadata{}, false)
}

func prepareColumnVectorGraphScalarU8QuantizedCodesPayloadWithAlphaMetadata(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow, metadata columnVectorGraphScalarU8AlphaMetadata, metadataSet bool) ([]byte, ColumnStoreConfig, error) {
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
	codes, err := buildColumnVectorGraphScalarU8CodesForDefinitionWithAlphaMetadata(def, q, rows, metadata, metadataSet)
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

func prepareColumnVectorGraphScalarU8AlphaPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, metadata columnVectorGraphScalarU8AlphaMetadata) ([]byte, ColumnStoreConfig, error) {
	if partID == 0 {
		return nil, ColumnStoreConfig{}, errors.New("collections: column_graph scalar_u8 alpha asset requires non-zero part_id")
	}
	if len(metadata.Alphas) != len(metadata.RowCounts) {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph scalar_u8 alpha metadata alphas=%d row_counts=%d", len(metadata.Alphas), len(metadata.RowCounts))
	}
	sourceCfg, err := columnVectorGraphScalarU8AlphaColumnStoreConfig(collection, base, def, q)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	rowCount := len(metadata.Alphas)
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 8, "scalar_u8 alpha primary_id"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	primaryIDs := make([]int64, rowCount)
	for rowIdx := range primaryIDs {
		primaryIDs[rowIdx] = int64(rowIdx)
		if !validColumnVectorGraphScalarU8Alpha(metadata.Alphas[rowIdx]) {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph scalar_u8 alpha row %d value=%v is invalid", rowIdx, metadata.Alphas[rowIdx])
		}
		if metadata.RowCounts[rowIdx] == 0 {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph scalar_u8 alpha row %d row_count=0", rowIdx)
		}
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			columnVectorGraphQuantizedPrimaryIDColumnDefinition(),
			{
				Name:           columnVectorGraphQuantizedScalarU8AlphaColumnName,
				Type:           typedcolumn.ColumnTypeFloat32,
				Encoding:       typedcolumn.EncodingRawFloat32,
				Compression:    typedcolumn.CompressionNone,
				CompressionSet: true,
				StatsDisabled:  true,
			},
			{
				Name:           columnVectorGraphQuantizedGranuleRowCountColumnName,
				Type:           typedcolumn.ColumnTypeUint32,
				Encoding:       typedcolumn.EncodingRawUint32,
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
		Float32Columns: map[string][]float32{
			columnVectorGraphQuantizedScalarU8AlphaColumnName: metadata.Alphas,
		},
		Uint32Columns: map[string][]uint32{
			columnVectorGraphQuantizedGranuleRowCountColumnName: metadata.RowCounts,
		},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
		LayoutLogicalTypes: map[string]string{
			columnVectorGraphQuantizedScalarU8AlphaColumnName:   string(columnsemantics.LogicalFloat32),
			columnVectorGraphQuantizedGranuleRowCountColumnName: string(columnsemantics.LogicalUint32),
		},
	})
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if image.Rows != rowCount || image.PartID != partID {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph scalar_u8 alpha asset image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, rowCount, partID)
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

func prepareColumnVectorGraphBRQQuantizedCodesPayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) ([]byte, ColumnStoreConfig, error) {
	if def.Metric != VectorMetricCosine {
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q metric %q is unsupported for brq_1bit", q.Name, def.Metric)
	}
	sourceCfg, err := columnVectorGraphQuantizedCodesColumnStoreConfig(collection, base, def, q)
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	rowCount := len(rows)
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 8, "brq_1bit primary_id"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	codesBytes, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, plan.BytesPerCode(), "brq_1bit codes")
	if err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 4, "brq_1bit code_count"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	if _, err := checkedColumnVectorGraphQuantizedRowBytes(rowCount, 4, "brq_1bit quantized_dot_product_inv"); err != nil {
		return nil, ColumnStoreConfig{}, err
	}
	primaryIDs := make([]int64, rowCount)
	codes := make([]byte, codesBytes)
	codeCounts := make([]uint32, rowCount)
	qdpInv := make([]float32, rowCount)
	var ws brq.Workspace
	var codeScratch []byte
	for rowIdx, row := range rows {
		primaryIDs[rowIdx] = int64(rowIdx)
		encoded, err := plan.Encode(codeScratch, row.Vector, &ws)
		if err != nil {
			return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q brq_1bit row %d encode: %w", q.Name, rowIdx, err)
		}
		codeScratch = encoded.Code
		copy(codes[rowIdx*plan.BytesPerCode():(rowIdx+1)*plan.BytesPerCode()], encoded.Code)
		codeCounts[rowIdx] = encoded.CodeCount
		qdpInv[rowIdx] = encoded.QuantizedDotProductInv
	}
	packedRows, err := typedcolumn.NewPackedUintRows(rowCount, plan.CodeDimensions(), brq.CodeWidthBits, codes)
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
				BitsPerElement:     brq.CodeWidthBits,
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
		return nil, ColumnStoreConfig{}, fmt.Errorf("collections: column_graph brq quantized asset image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, rowCount, partID)
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
		path := def.Field + "_quantized_codes"
		cfgHash, err := scalarU8CalibrationConfigHashForAssetID(q)
		if err != nil {
			return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 calibration identity: %w", q.Name, err)
		}
		if cfgHash != 0 {
			path = fmt.Sprintf("%s_quantized_scalar_u8_%016x_%s", def.Field, cfgHash, columnVectorGraphQuantizedCodesColumnName)
		}
		columns = []ColumnStoreColumn{{
			Name:        columnVectorGraphQuantizedCodesColumnName,
			Path:        path,
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
	case brq.CodecName:
		if q.Version != brq.CodecVersion {
			return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q brq_1bit version=%d is unsupported", q.Name, q.Version)
		}
		plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
		if err != nil {
			return ColumnStoreConfig{}, err
		}
		cfgHash := brq.DefaultConfig().Hash64()
		columns = []ColumnStoreColumn{
			{
				Name:           columnVectorGraphQuantizedPackedCodesColumnName,
				Path:           fmt.Sprintf(columnVectorGraphQuantizedBRQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedPackedCodesColumnName),
				Owner:          TypedStorageOwnerColumnPart,
				ValueType:      ColumnStoreValuePackedBitVector,
				ElementsPerRow: plan.CodeDimensions(),
				BitsPerElement: brq.CodeWidthBits,
			},
			{
				Name:               columnVectorGraphQuantizedCodeCountColumnName,
				Path:               fmt.Sprintf(columnVectorGraphQuantizedBRQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedCodeCountColumnName),
				Owner:              TypedStorageOwnerColumnPart,
				ValueType:          ColumnStoreValueUint32,
				FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
			},
			{
				Name:               columnVectorGraphQuantizedDotProductInvColumnName,
				Path:               fmt.Sprintf(columnVectorGraphQuantizedBRQPathConfigHashFormat, def.Field, cfgHash, columnVectorGraphQuantizedDotProductInvColumnName),
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

func columnVectorGraphScalarU8AlphaColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition) (ColumnStoreConfig, error) {
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 {
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha config requires scalar_u8 v1", q.Name)
	}
	if scalarU8CalibrationIsLegacy(q) {
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q legacy scalar_u8 has no alpha metadata", q.Name)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, errors.New("collections: column_graph scalar_u8 alpha asset requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, errors.New("collections: column_graph scalar_u8 alpha asset requires base asset manager")
	}
	cfgHash, err := scalarU8CalibrationConfigHashForAssetID(q)
	if err != nil {
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha calibration identity: %w", q.Name, err)
	}
	if cfgHash == 0 {
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha asset requires non-zero config hash", q.Name)
	}
	columns := []ColumnStoreColumn{
		{
			Name:               columnVectorGraphQuantizedScalarU8AlphaColumnName,
			Path:               fmt.Sprintf("%s_quantized_scalar_u8_%016x_%s", def.Field, cfgHash, columnVectorGraphQuantizedScalarU8AlphaColumnName),
			Owner:              TypedStorageOwnerColumnPart,
			ValueType:          ColumnStoreValueFloat32,
			FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
		},
		{
			Name:               columnVectorGraphQuantizedGranuleRowCountColumnName,
			Path:               fmt.Sprintf("%s_quantized_scalar_u8_%016x_%s", def.Field, cfgHash, columnVectorGraphQuantizedGranuleRowCountColumnName),
			Owner:              TypedStorageOwnerColumnPart,
			ValueType:          ColumnStoreValueUint32,
			FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
		},
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
		return ColumnStoreConfig{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha typed-column config: %w", q.Name, err)
	}
	return *cfg, nil
}

func columnVectorGraphQuantizedAssetColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition, q QuantizedVectorIndexDefinition, role string) (ColumnStoreConfig, error) {
	if role == columnVectorIndexStateAssetRoleQuantizedAlpha {
		return columnVectorGraphScalarU8AlphaColumnStoreConfig(collection, base, def, q)
	}
	return columnVectorGraphQuantizedCodesColumnStoreConfig(collection, base, def, q)
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

type columnVectorGraphScalarU8AlphaMetadata struct {
	Alphas    []float32
	RowCounts []uint32
}

func buildColumnVectorGraphScalarU8CodesForDefinition(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, rows []columnVectorGraphAssetRow) ([]byte, error) {
	return buildColumnVectorGraphScalarU8CodesForDefinitionWithAlphaMetadata(def, q, rows, columnVectorGraphScalarU8AlphaMetadata{}, false)
}

func buildColumnVectorGraphScalarU8CodesForDefinitionWithAlphaMetadata(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, rows []columnVectorGraphAssetRow, metadata columnVectorGraphScalarU8AlphaMetadata, metadataSet bool) ([]byte, error) {
	if scalarU8CalibrationIsLegacy(q) {
		if metadataSet && (len(metadata.Alphas) != 0 || len(metadata.RowCounts) != 0) {
			return nil, fmt.Errorf("collections: column_graph quantized index %q legacy scalar_u8 cannot use alpha metadata", q.Name)
		}
		return buildColumnVectorGraphScalarU8Codes(def, rows)
	}
	if !metadataSet {
		var err error
		metadata, err = buildColumnVectorGraphScalarU8AlphaMetadata(def, q, rows)
		if err != nil {
			return nil, err
		}
	} else if len(rows) != 0 && len(metadata.Alphas) == 0 {
		return nil, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha metadata is empty for %d rows", q.Name, len(rows))
	}
	return buildColumnVectorGraphScalarU8CodesWithAlpha(def, rows, metadata)
}

func buildColumnVectorGraphScalarU8Codes(def VectorIndexDefinition, rows []columnVectorGraphAssetRow) ([]byte, error) {
	return buildColumnVectorGraphScalarU8CodesWithAlpha(def, rows, columnVectorGraphScalarU8AlphaMetadata{})
}

func buildColumnVectorGraphScalarU8CodesWithAlpha(def VectorIndexDefinition, rows []columnVectorGraphAssetRow, metadata columnVectorGraphScalarU8AlphaMetadata) ([]byte, error) {
	if def.Dimensions <= 0 {
		return nil, errors.New("collections: column_graph quantized asset dimensions must be positive")
	}
	if len(rows) != 0 && def.Dimensions > math.MaxInt/len(rows) {
		return nil, errors.New("collections: column_graph quantized asset codes bytes overflow")
	}
	if len(metadata.Alphas) != len(metadata.RowCounts) {
		return nil, fmt.Errorf("collections: column_graph scalar_u8 alpha metadata alphas=%d row_counts=%d", len(metadata.Alphas), len(metadata.RowCounts))
	}
	codes := make([]byte, 0, len(rows)*def.Dimensions)
	rowBase := 0
	granule := 0
	granuleEnd := len(rows)
	alpha := float32(1)
	if len(metadata.Alphas) != 0 {
		granuleEnd = int(metadata.RowCounts[0])
		alpha = metadata.Alphas[0]
	}
	for rowIdx, row := range rows {
		if len(metadata.Alphas) != 0 {
			for rowIdx >= granuleEnd && granule+1 < len(metadata.Alphas) {
				rowBase = granuleEnd
				granule++
				granuleEnd += int(metadata.RowCounts[granule])
				alpha = metadata.Alphas[granule]
			}
			if rowIdx < rowBase || rowIdx >= granuleEnd || !validColumnVectorGraphScalarU8Alpha(alpha) {
				return nil, fmt.Errorf("collections: column_graph scalar_u8 alpha metadata invalid for row %d granule %d", rowIdx, granule)
			}
		}
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
			if len(metadata.Alphas) != 0 {
				codeValue /= alpha
			}
			codes = append(codes, columnVectorGraphScalarU8Code(codeValue))
		}
	}
	if len(metadata.Alphas) != 0 && granuleEnd != len(rows) {
		return nil, fmt.Errorf("collections: column_graph scalar_u8 alpha row_counts sum=%d want rows=%d", granuleEnd, len(rows))
	}
	return codes, nil
}

func buildColumnVectorGraphScalarU8AlphaMetadata(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, rows []columnVectorGraphAssetRow) (columnVectorGraphScalarU8AlphaMetadata, error) {
	if q.Codec != QuantizedVectorCodecScalarU8 || q.Version != 1 || scalarU8CalibrationIsLegacy(q) {
		return columnVectorGraphScalarU8AlphaMetadata{}, fmt.Errorf("collections: column_graph quantized index %q has no scalar_u8 alpha metadata", q.Name)
	}
	if def.Metric != VectorMetricCosine {
		return columnVectorGraphScalarU8AlphaMetadata{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha metric %q is unsupported", q.Name, def.Metric)
	}
	if def.Dimensions <= 0 {
		return columnVectorGraphScalarU8AlphaMetadata{}, errors.New("collections: column_graph scalar_u8 alpha dimensions must be positive")
	}
	rowCounts, err := columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q, len(rows))
	if err != nil {
		return columnVectorGraphScalarU8AlphaMetadata{}, err
	}
	metadata := columnVectorGraphScalarU8AlphaMetadata{Alphas: make([]float32, len(rowCounts)), RowCounts: rowCounts}
	rowStart := 0
	for granule, count := range rowCounts {
		rowEnd := rowStart + int(count)
		alpha, err := computeColumnVectorGraphScalarU8GranuleAlpha(def, q, rows[rowStart:rowEnd])
		if err != nil {
			return columnVectorGraphScalarU8AlphaMetadata{}, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha granule %d: %w", q.Name, granule, err)
		}
		metadata.Alphas[granule] = alpha
		rowStart = rowEnd
	}
	return metadata, nil
}

func columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q QuantizedVectorIndexDefinition, rows int) ([]uint32, error) {
	cfg, err := normalizedScalarU8CalibrationConfigForIdentity(q)
	if err != nil {
		return nil, err
	}
	if cfg == nil || cfg.Mode != ScalarU8CalibrationModePerGranuleAlpha {
		return nil, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha mode %q is not %q", q.Name, scalarU8CalibrationMode(q), ScalarU8CalibrationModePerGranuleAlpha)
	}
	if cfg.Grouping != ScalarU8CalibrationGroupingStorageLayoutGranule {
		return nil, fmt.Errorf("collections: column_graph quantized index %q scalar_u8 alpha grouping=%q want %q", q.Name, cfg.Grouping, ScalarU8CalibrationGroupingStorageLayoutGranule)
	}
	return columnVectorGraphScalarU8AlphaGranuleRowCounts(rows, typedColumnDefaultRowsPerGranule())
}

func columnVectorGraphScalarU8AlphaGranuleRowCounts(rows, rowsPerGranule int) ([]uint32, error) {
	if rows < 0 || rowsPerGranule <= 0 {
		return nil, fmt.Errorf("collections: column_graph scalar_u8 alpha invalid granule rows=%d rows_per_granule=%d", rows, rowsPerGranule)
	}
	if rows == 0 {
		return nil, nil
	}
	granules := (rows + rowsPerGranule - 1) / rowsPerGranule
	counts := make([]uint32, granules)
	remaining := rows
	for i := range counts {
		count := rowsPerGranule
		if remaining < count {
			count = remaining
		}
		counts[i] = uint32(count)
		remaining -= count
	}
	return counts, nil
}

func computeColumnVectorGraphScalarU8GranuleAlpha(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, rows []columnVectorGraphAssetRow) (float32, error) {
	cfg, err := normalizedScalarU8CalibrationConfigForIdentity(q)
	if err != nil {
		return 0, err
	}
	if cfg == nil || cfg.Mode != ScalarU8CalibrationModePerGranuleAlpha {
		return 0, fmt.Errorf("mode %q is not per_granule_alpha", scalarU8CalibrationMode(q))
	}
	var absValues []float64
	if cfg.AlphaPolicy.Name == ScalarU8AlphaPolicyAbsQuantile {
		if def.Dimensions <= 0 {
			return 0, fmt.Errorf("dimensions=%d must be positive", def.Dimensions)
		}
		if len(rows) != 0 && def.Dimensions > math.MaxInt/len(rows) {
			return 0, errors.New("abs_quantile coordinate count overflow")
		}
		absValues = make([]float64, 0, len(rows)*def.Dimensions)
	}
	maxAbs := float64(0)
	for rowIdx, row := range rows {
		if len(row.Vector) != def.Dimensions {
			return 0, fmt.Errorf("row %d vector dimensions=%d want %d", rowIdx, len(row.Vector), def.Dimensions)
		}
		if row.InvNorm <= 0 || math.IsNaN(float64(row.InvNorm)) || math.IsInf(float64(row.InvNorm), 0) {
			return 0, fmt.Errorf("row %d inverse norm is invalid", rowIdx)
		}
		for _, value := range row.Vector {
			normalized := float64(value) * float64(row.InvNorm)
			if math.IsNaN(normalized) || math.IsInf(normalized, 0) {
				return 0, fmt.Errorf("row %d normalized coordinate is not finite", rowIdx)
			}
			abs := math.Abs(normalized)
			if abs > maxAbs {
				maxAbs = abs
			}
			if cfg.AlphaPolicy.Name == ScalarU8AlphaPolicyAbsQuantile {
				absValues = append(absValues, abs)
			}
		}
	}
	alpha64 := maxAbs
	if cfg.AlphaPolicy.Name == ScalarU8AlphaPolicyAbsQuantile {
		if len(absValues) == 0 {
			return 0, errors.New("abs_quantile has no coordinates")
		}
		sort.Float64s(absValues)
		idx := int((uint64(len(absValues))*uint64(cfg.AlphaPolicy.QuantilePPM) + 999999) / 1000000)
		if idx <= 0 {
			idx = 1
		}
		if idx > len(absValues) {
			idx = len(absValues)
		}
		alpha64 = absValues[idx-1]
		if !validColumnVectorGraphScalarU8Alpha(float32(alpha64)) && maxAbs > 0 {
			for _, candidate := range absValues {
				if validColumnVectorGraphScalarU8Alpha(float32(candidate)) {
					alpha64 = candidate
					break
				}
			}
			if !validColumnVectorGraphScalarU8Alpha(float32(alpha64)) && validColumnVectorGraphScalarU8Alpha(float32(maxAbs)) {
				alpha64 = maxAbs
			}
		}
	}
	alpha := float32(alpha64)
	if !validColumnVectorGraphScalarU8Alpha(alpha) {
		return 0, fmt.Errorf("computed alpha=%v is not positive finite", alpha)
	}
	return alpha, nil
}

func validColumnVectorGraphScalarU8Alpha(alpha float32) bool {
	return alpha > 0 && !math.IsNaN(float64(alpha)) && !math.IsInf(float64(alpha), 0)
}

func columnVectorGraphScalarU8Code(value float32) byte {
	if math.IsNaN(float64(value)) {
		return 0
	}
	scaled := (float64(value) + 1.0) * 127.5
	if scaled <= 0 {
		return 0
	}
	if scaled >= 255 {
		return 255
	}
	// For the non-negative unclamped range, math.Round(scaled) is exactly
	// floor(scaled+0.5). Avoid the generic math.Round call in query-prep hot
	// loops while preserving the scalar_u8 v1 byte mapping.
	return byte(scaled + 0.5)
}

func columnVectorGraphQuantizedAssetStateType(q QuantizedVectorIndexDefinition) (logicalType, physicalEncoding string) {
	switch q.Codec {
	case brq.CodecName, rabitq.CodecName:
		return columnVectorIndexStateLogicalTypePackedBitVector, columnVectorIndexStateEncodingRawPackedBitVector
	default:
		return columnVectorIndexStateLogicalTypeByteVector, columnVectorIndexStateEncodingRawFixedBytes
	}
}

func columnVectorGraphQuantizedPreparedAssetStateType(prepared columnVectorGraphPreparedQuantizedAsset) (logicalType, physicalEncoding string) {
	return columnVectorGraphQuantizedStateTypeForRole(prepared.Definition, prepared.Role)
}

func columnVectorGraphQuantizedStateTypeForRole(q QuantizedVectorIndexDefinition, role string) (logicalType, physicalEncoding string) {
	if role == columnVectorIndexStateAssetRoleQuantizedAlpha {
		return columnVectorIndexStateLogicalTypeScalarU8Alpha, columnVectorIndexStateEncodingRawFloat32Uint32
	}
	return columnVectorGraphQuantizedAssetStateType(q)
}

func columnVectorGraphQuantizedAssetSnapshotsFromPrepared(prepared []columnVectorGraphPreparedQuantizedAsset) []columnVectorIndexStateAssetSnapshot {
	if len(prepared) == 0 {
		return nil
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, len(prepared))
	for i, prepared := range prepared {
		logicalType, physicalEncoding := columnVectorGraphQuantizedPreparedAssetStateType(prepared)
		role := prepared.Role
		if role == "" {
			role = columnVectorIndexStateAssetRoleQuantizedCodes
		}
		assets[i] = columnVectorIndexStateAssetSnapshot{
			Role:             role,
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

type columnVectorGraphQuantizedAssetSet struct {
	Codes    columnVectorIndexStateAssetSnapshot
	Alpha    columnVectorIndexStateAssetSnapshot
	HasCodes bool
	HasAlpha bool
}

func columnVectorGraphQuantizedAssetByName(state columnVectorIndexStateSnapshot, def VectorIndexDefinition) map[string]columnVectorIndexStateAssetSnapshot {
	sets := columnVectorGraphQuantizedAssetSetsByName(state, def)
	out := make(map[string]columnVectorIndexStateAssetSnapshot, len(sets))
	for name, set := range sets {
		if set.HasCodes {
			out[name] = set.Codes
		}
	}
	return out
}

func columnVectorGraphQuantizedAssetSetsByName(state columnVectorIndexStateSnapshot, def VectorIndexDefinition) map[string]columnVectorGraphQuantizedAssetSet {
	out := make(map[string]columnVectorGraphQuantizedAssetSet, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		wantCodesID := columnVectorGraphQuantizedCodesAssetID(q)
		wantAlphaID := columnVectorGraphScalarU8AlphaAssetID(q)
		var set columnVectorGraphQuantizedAssetSet
		for _, asset := range state.Assets {
			switch {
			case asset.Role == columnVectorIndexStateAssetRoleQuantizedCodes && asset.AssetID == wantCodesID:
				set.Codes = asset
				set.HasCodes = true
			case wantAlphaID != "" && asset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha && asset.AssetID == wantAlphaID:
				set.Alpha = asset
				set.HasAlpha = true
			}
		}
		if set.HasCodes || set.HasAlpha {
			out[q.Name] = set
		}
	}
	return out
}

func columnVectorGraphQuantizedStateAssetIDSetMatches(def VectorIndexDefinition, state columnVectorIndexStateSnapshot) bool {
	expected := columnVectorGraphQuantizedExpectedStateAssetIDs(def)
	seen := make(map[string]struct{}, len(expected))
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes && asset.Role != columnVectorIndexStateAssetRoleQuantizedAlpha {
			continue
		}
		key := asset.Role + "\x00" + asset.AssetID
		if _, ok := expected[key]; !ok {
			return false
		}
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
	}
	return len(seen) == len(expected)
}

func columnVectorGraphQuantizedExpectedStateAssetIDs(def VectorIndexDefinition) map[string]QuantizedVectorIndexDefinition {
	expected := make(map[string]QuantizedVectorIndexDefinition, len(def.QuantizedIndexes)*2)
	for _, q := range def.QuantizedIndexes {
		expected[columnVectorIndexStateAssetRoleQuantizedCodes+"\x00"+columnVectorGraphQuantizedCodesAssetID(q)] = q
		if alphaID := columnVectorGraphScalarU8AlphaAssetID(q); alphaID != "" {
			expected[columnVectorIndexStateAssetRoleQuantizedAlpha+"\x00"+alphaID] = q
		}
	}
	return expected
}

func columnVectorGraphQuantizedDefinitionForStateAsset(def VectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot) (QuantizedVectorIndexDefinition, bool) {
	for _, q := range def.QuantizedIndexes {
		switch asset.Role {
		case columnVectorIndexStateAssetRoleQuantizedCodes:
			if asset.AssetID == columnVectorGraphQuantizedCodesAssetID(q) {
				return q, true
			}
		case columnVectorIndexStateAssetRoleQuantizedAlpha:
			if alphaID := columnVectorGraphScalarU8AlphaAssetID(q); alphaID != "" && asset.AssetID == alphaID {
				return q, true
			}
		}
	}
	return QuantizedVectorIndexDefinition{}, false
}

func validateColumnVectorGraphQuantizedStateAssets(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot) error {
	expected := columnVectorGraphQuantizedExpectedStateAssetIDs(def)
	seen := make(map[string]struct{}, len(expected))
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes && asset.Role != columnVectorIndexStateAssetRoleQuantizedAlpha {
			continue
		}
		key := asset.Role + "\x00" + asset.AssetID
		q, ok := expected[key]
		if !ok {
			return fmt.Errorf("collections: vector-index state unexpected quantized asset role=%q id=%q", asset.Role, asset.AssetID)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("collections: vector-index state duplicate quantized asset role=%q id=%q", asset.Role, asset.AssetID)
		}
		seen[key] = struct{}{}
		wantLogical, wantEncoding := columnVectorGraphQuantizedStateTypeForRole(q, asset.Role)
		if asset.LogicalType != wantLogical || asset.PhysicalEncoding != wantEncoding {
			return fmt.Errorf("collections: vector-index state quantized asset %q role=%q type/encoding=(%q,%q) want (%q,%q)", q.Name, asset.Role, asset.LogicalType, asset.PhysicalEncoding, wantLogical, wantEncoding)
		}
		sourceCfg, err := columnVectorGraphQuantizedAssetColumnStoreConfig(collection, cfg, def, q, asset.Role)
		if err != nil {
			return err
		}
		if asset.SourceSchemaHash != sourceCfg.SchemaHash {
			return fmt.Errorf("collections: vector-index state quantized asset %q role=%q schema_hash=%d want %d", q.Name, asset.Role, asset.SourceSchemaHash, sourceCfg.SchemaHash)
		}
		if asset.Role == columnVectorIndexStateAssetRoleQuantizedAlpha {
			wantRowCounts, err := columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q, state.RowCount)
			if err != nil {
				return fmt.Errorf("collections: vector-index state quantized asset %q role=%q expected alpha granules: %w", q.Name, asset.Role, err)
			}
			if asset.RowCount != len(wantRowCounts) {
				return fmt.Errorf("collections: vector-index state quantized asset %q role=%q granule_count=%d want %d", q.Name, asset.Role, asset.RowCount, len(wantRowCounts))
			}
		}
	}
	for key, q := range expected {
		if _, ok := seen[key]; !ok {
			return fmt.Errorf("collections: vector-index state missing quantized asset %q", q.Name)
		}
	}
	return nil
}

func (c *Collection) prepareColumnVectorGraphQuantizedAssetsForReader(graphReader *columnVectorGraphPhysicalRowReader, view columnPhysicalScanSnapshotView) {
	if c == nil || c.db == nil || graphReader == nil || graphReader.skipQuantizedAssets || graphReader.catalog == nil || graphReader.catalog.meta.Options.ColumnStore == nil || len(graphReader.def.QuantizedIndexes) == 0 || !view.VectorIndexStateFound {
		return
	}
	graphReader.quantizedAssetStatus = loadColumnVectorGraphQuantizedAssetsForReader(c.db.ColumnAssetRootDir(), graphReader.catalog.meta.Name, *graphReader.catalog.meta.Options.ColumnStore, graphReader.def, graphReader.graph, view.VectorIndexState, graphReader.useResourceQuantizedAssets)
}

func loadColumnVectorGraphQuantizedAssetsForReader(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, useResourceScalarU8 bool) map[string]columnVectorGraphQuantizedAssetLoadStatus {
	if len(def.QuantizedIndexes) == 0 {
		return nil
	}
	byName := columnVectorGraphQuantizedAssetSetsByName(state, def)
	out := make(map[string]columnVectorGraphQuantizedAssetLoadStatus, len(def.QuantizedIndexes))
	for _, q := range def.QuantizedIndexes {
		status := columnVectorGraphQuantizedAssetLoadStatus{Definition: q}
		assetSet, ok := byName[q.Name]
		if !ok || !assetSet.HasCodes {
			status.Health = columnVectorGraphQuantizedAssetHealthMissing
			status.Err = fmt.Errorf("%w: quantized asset %q is missing", errColumnVectorGraphQuantizedAssetMissing, q.Name)
			out[q.Name] = status
			continue
		}
		status.Asset = assetSet.Codes
		start := time.Now()
		if q.Codec == QuantizedVectorCodecScalarU8 && useResourceScalarU8 {
			loaded, err := loadColumnVectorGraphQuantizedAssetResourceStatus(rootDir, collection, cfg, def, graph, q, assetSet)
			status = loaded
			status.OpenNanos = uint64(time.Since(start).Nanoseconds())
			if err != nil {
				out[q.Name] = status
				continue
			}
		} else {
			prepared, err := loadColumnVectorGraphQuantizedAssetSet(rootDir, collection, cfg, def, graph, q, assetSet)
			status.OpenNanos = uint64(time.Since(start).Nanoseconds())
			if err != nil {
				status.Err = err
				status.Health = columnVectorGraphQuantizedAssetHealthFromError(err)
			} else {
				status.Prepared = prepared
				if lookup, err := columnVectorGraphScalarU8AlphaLookupFromPrepared(q, prepared); err != nil {
					status.Prepared = nil
					status.Err = fmt.Errorf("%w: quantized asset %q scalar_u8 alpha: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
					status.Health = columnVectorGraphQuantizedAssetHealthInvalid
					out[q.Name] = status
					continue
				} else {
					status.ScalarU8Alpha = lookup
				}
				if q.Codec == QuantizedVectorCodecScalarU8 {
					status.ScalarU8CodeSums = prepareColumnVectorGraphScalarU8CodeSums(prepared, def.Dimensions)
				}
				switch q.Codec {
				case rabitq.CodecName:
					plan, planErr := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
					if planErr != nil {
						status.Prepared = nil
						status.Err = fmt.Errorf("%w: quantized asset %q rabitq_1bit plan: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, planErr)
						status.Health = columnVectorGraphQuantizedAssetHealthInvalid
						out[q.Name] = status
						continue
					}
					status.RabitQPlan = plan
				case brq.CodecName:
					plan, planErr := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
					if planErr != nil {
						status.Prepared = nil
						status.Err = fmt.Errorf("%w: quantized asset %q brq_1bit plan: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, planErr)
						status.Health = columnVectorGraphQuantizedAssetHealthInvalid
						out[q.Name] = status
						continue
					}
					status.BRQPlan = plan
				}
				status.Health = columnVectorGraphQuantizedAssetHealthHeapCopy
				if assetSet.Codes.AssetBytes > 0 {
					status.HeapCopyBytes = uint64(assetSet.Codes.AssetBytes)
					if assetSet.HasAlpha && assetSet.Alpha.AssetBytes > 0 {
						status.HeapCopyBytes += uint64(assetSet.Alpha.AssetBytes)
					}
				} else if prepared != nil {
					fp := prepared.Footprint()
					if fp.AssetBytes > 0 {
						status.HeapCopyBytes = uint64(fp.AssetBytes)
					}
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

func validateColumnVectorGraphQuantizedAssetLoadInputs(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot) error {
	return validateColumnVectorGraphQuantizedAssetSetLoadInputs(rootDir, collection, cfg, def, graph, q, columnVectorGraphQuantizedAssetSet{Codes: asset, HasCodes: true})
}

func validateColumnVectorGraphQuantizedAssetSetLoadInputs(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, assets columnVectorGraphQuantizedAssetSet) error {
	if !assets.HasCodes {
		return fmt.Errorf("%w: quantized asset %q codes are missing", errColumnVectorGraphQuantizedAssetMissing, q.Name)
	}
	if err := validateColumnVectorGraphQuantizedOneAssetLoadInput(rootDir, collection, cfg, def, graph, q, assets.Codes, columnVectorIndexStateAssetRoleQuantizedCodes, graph.RowCount); err != nil {
		return err
	}
	if q.Codec == QuantizedVectorCodecScalarU8 && !scalarU8CalibrationIsLegacy(q) {
		if !assets.HasAlpha {
			return fmt.Errorf("%w: quantized asset %q scalar_u8 alpha metadata is missing", errColumnVectorGraphQuantizedAssetMissing, q.Name)
		}
		if err := validateColumnVectorGraphQuantizedOneAssetLoadInput(rootDir, collection, cfg, def, graph, q, assets.Alpha, columnVectorIndexStateAssetRoleQuantizedAlpha, 0); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnVectorGraphQuantizedOneAssetLoadInput(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot, role string, expectedRows int) error {
	if asset.Role != role {
		return fmt.Errorf("%w: quantized asset %q role=%q want %q", errColumnVectorGraphQuantizedAssetStale, q.Name, asset.Role, role)
	}
	wantLogical, wantEncoding := columnVectorGraphQuantizedStateTypeForRole(q, role)
	if asset.LogicalType != wantLogical || asset.PhysicalEncoding != wantEncoding {
		return fmt.Errorf("%w: quantized asset %q role=%q type/encoding=(%q,%q) want (%q,%q)", errColumnVectorGraphQuantizedAssetStale, q.Name, role, asset.LogicalType, asset.PhysicalEncoding, wantLogical, wantEncoding)
	}
	sourceCfg, err := columnVectorGraphQuantizedAssetColumnStoreConfig(collection, cfg, def, q, role)
	if err != nil {
		return fmt.Errorf("%w: quantized asset %q role=%q config: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, role, err)
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return fmt.Errorf("%w: quantized asset %q role=%q schema_hash=%d want %d", errColumnVectorGraphQuantizedAssetStale, q.Name, role, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if expectedRows != 0 {
		if asset.RowCount != expectedRows {
			return fmt.Errorf("%w: quantized asset %q role=%q row_count=%d want graph row_count=%d", errColumnVectorGraphQuantizedAssetStale, q.Name, role, asset.RowCount, expectedRows)
		}
	} else if asset.RowCount < 0 || (graph.RowCount > 0 && asset.RowCount == 0) || asset.RowCount > graph.RowCount {
		return fmt.Errorf("%w: quantized asset %q role=%q row_count=%d invalid for graph row_count=%d", errColumnVectorGraphQuantizedAssetStale, q.Name, role, asset.RowCount, graph.RowCount)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return fmt.Errorf("%w: quantized asset %q role=%q unavailable: %v", errColumnVectorGraphQuantizedAssetMissing, q.Name, role, err)
	}
	return nil
}

func loadColumnVectorGraphQuantizedAsset(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot) (*quantizedasset.Prepared, error) {
	return loadColumnVectorGraphQuantizedAssetSet(rootDir, collection, cfg, def, graph, q, columnVectorGraphQuantizedAssetSet{Codes: asset, HasCodes: true})
}

func loadColumnVectorGraphQuantizedAssetSet(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, assets columnVectorGraphQuantizedAssetSet) (*quantizedasset.Prepared, error) {
	if err := validateColumnVectorGraphQuantizedAssetSetLoadInputs(rootDir, collection, cfg, def, graph, q, assets); err != nil {
		return nil, err
	}
	codeRaw, err := readColumnPhysicalAssetFromManager(rootDir, assets.Codes.Ref)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q read: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	codeImage, err := typedcolumn.ParseColumnPartImage(codeRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q parse typed-column image: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	var alphaImage typedcolumn.ColumnPartImage
	if assets.HasAlpha {
		alphaRaw, err := readColumnPhysicalAssetFromManager(rootDir, assets.Alpha.Ref)
		if err != nil {
			return nil, fmt.Errorf("%w: quantized asset %q alpha read: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		}
		alphaImage, err = typedcolumn.ParseColumnPartImage(alphaRaw)
		if err != nil {
			return nil, fmt.Errorf("%w: quantized asset %q parse alpha typed-column image: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		}
	}
	return prepareColumnVectorGraphQuantizedAssetFromImages(def, graph, q, assets, codeImage, alphaImage)
}

func prepareColumnVectorGraphQuantizedAssetFromImage(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot, image typedcolumn.ColumnPartImage) (*quantizedasset.Prepared, error) {
	return prepareColumnVectorGraphQuantizedAssetFromImages(def, graph, q, columnVectorGraphQuantizedAssetSet{Codes: asset, HasCodes: true}, image, typedcolumn.ColumnPartImage{})
}

func prepareColumnVectorGraphQuantizedAssetFromImages(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, assets columnVectorGraphQuantizedAssetSet, codeImage, alphaImage typedcolumn.ColumnPartImage) (*quantizedasset.Prepared, error) {
	codeRef := columnVectorGraphQuantizedAssetRefIdentity(assets.Codes.Ref)
	alphaRef := quantizedasset.AssetRefIdentity{}
	if assets.HasAlpha {
		alphaRef = columnVectorGraphQuantizedAssetRefIdentity(assets.Alpha.Ref)
	}
	schema, err := columnVectorGraphQuantizedAssetSchemaFromAssets(def, graph, q, assets, codeRef, alphaRef)
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q schema: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	parts := []quantizedasset.PartImageSource{{AssetID: assets.Codes.AssetID, Image: codeImage, Ref: codeRef, AssetBytes: assets.Codes.AssetBytes, SourceSchemaHash: assets.Codes.SourceSchemaHash}}
	if assets.HasAlpha {
		parts = append(parts, quantizedasset.PartImageSource{AssetID: assets.Alpha.AssetID, Image: alphaImage, Ref: alphaRef, AssetBytes: assets.Alpha.AssetBytes, SourceSchemaHash: assets.Alpha.SourceSchemaHash})
	}
	expectedGranuleCount := schema.GranuleCount
	if q.Codec == QuantizedVectorCodecScalarU8 && !scalarU8CalibrationIsLegacy(q) {
		expectedRowCounts, err := columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q, schema.RowCount)
		if err != nil {
			return nil, fmt.Errorf("%w: quantized asset %q scalar_u8 alpha expected granules: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		}
		expectedGranuleCount = len(expectedRowCounts)
	}
	prepared, err := quantizedasset.Prepare(quantizedasset.PrepareRequest{
		Schema: schema,
		Expected: quantizedasset.ExpectedSchema{
			Metric:           schema.Metric,
			VectorDimensions: schema.VectorDimensions,
			CodeDimensions:   schema.CodeDimensions,
			CodeWidthBits:    schema.CodeWidthBits,
			RowCount:         schema.RowCount,
			GranuleCount:     expectedGranuleCount,
			OrdinalOrder:     schema.OrdinalOrder,
			Codec:            schema.Codec,
			BaseGraph:        schema.BaseGraph,
			RequiredRoles:    columnVectorGraphQuantizedRequiredRoles(q),
		},
		Parts: parts,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q prepare: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	if err := validateColumnVectorGraphQuantizedPreparedAsset(def, q, prepared); err != nil {
		return nil, fmt.Errorf("%w: quantized asset %q validate: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
	}
	return prepared, nil
}

func loadColumnVectorGraphQuantizedAssetResourceStatus(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, assets columnVectorGraphQuantizedAssetSet) (columnVectorGraphQuantizedAssetLoadStatus, error) {
	status := columnVectorGraphQuantizedAssetLoadStatus{Definition: q, Asset: assets.Codes}
	if err := validateColumnVectorGraphQuantizedAssetSetLoadInputs(rootDir, collection, cfg, def, graph, q, assets); err != nil {
		status.Err = err
		status.Health = columnVectorGraphQuantizedAssetHealthFromError(err)
		return status, err
	}
	raw, resource, source, err := readColumnVectorGraphQuantizedAssetResourceBytes(rootDir, assets.Codes.Ref)
	if err != nil {
		status.Err = fmt.Errorf("%w: quantized asset %q read: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		status.Health = columnVectorGraphQuantizedAssetHealthInvalid
		return status, status.Err
	}
	status.resource = resource
	status.ownsResource = true
	defer func() {
		if status.Err != nil && status.resource != nil {
			_ = status.resource.close()
			status.resource = nil
			status.ownsResource = false
		}
	}()
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		status.Err = fmt.Errorf("%w: quantized asset %q parse typed-column image: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		status.Health = columnVectorGraphQuantizedAssetHealthInvalid
		return status, status.Err
	}
	var alphaImage typedcolumn.ColumnPartImage
	if assets.HasAlpha {
		alphaRaw, err := readColumnPhysicalAssetFromManager(rootDir, assets.Alpha.Ref)
		if err != nil {
			status.Err = fmt.Errorf("%w: quantized asset %q alpha read: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
			status.Health = columnVectorGraphQuantizedAssetHealthInvalid
			return status, status.Err
		}
		alphaImage, err = typedcolumn.ParseColumnPartImage(alphaRaw)
		if err != nil {
			status.Err = fmt.Errorf("%w: quantized asset %q parse alpha typed-column image: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
			status.Health = columnVectorGraphQuantizedAssetHealthInvalid
			return status, status.Err
		}
	}
	prepared, err := prepareColumnVectorGraphQuantizedAssetFromImages(def, graph, q, assets, image, alphaImage)
	if err != nil {
		status.Err = err
		status.Health = columnVectorGraphQuantizedAssetHealthFromError(err)
		return status, err
	}
	status.Prepared = prepared
	lookup, err := columnVectorGraphScalarU8AlphaLookupFromPrepared(q, prepared)
	if err != nil {
		status.Err = fmt.Errorf("%w: quantized asset %q scalar_u8 alpha: %v", errColumnVectorGraphQuantizedAssetInvalid, q.Name, err)
		status.Health = columnVectorGraphQuantizedAssetHealthInvalid
		return status, status.Err
	}
	status.ScalarU8Alpha = lookup
	if q.Codec == QuantizedVectorCodecScalarU8 {
		status.ScalarU8CodeSums = prepareColumnVectorGraphScalarU8CodeSums(prepared, def.Dimensions)
	}
	stats := resource.manager.Stats()
	status.ActiveHandles = stats.ActiveHandles
	status.MappedBytes = uint64(stats.ActiveMappedBytes)
	status.HeapCopyBytes = uint64(stats.ActiveHeapCopyBytes)
	if assets.HasAlpha && assets.Alpha.AssetBytes > 0 {
		status.HeapCopyBytes += uint64(assets.Alpha.AssetBytes)
	}
	if source == mappedresource.SourceMapped {
		status.Health = columnVectorGraphQuantizedAssetHealthMmapDirect
	} else {
		status.Health = columnVectorGraphQuantizedAssetHealthHeapCopy
	}
	return status, nil
}

func readColumnVectorGraphQuantizedAssetResourceBytes(rootDir string, ref ColumnAssetRef) ([]byte, *columnVectorGraphQuantizedAssetResource, mappedresource.Source, error) {
	readCache, err := newColumnPhysicalAssetReadCache(rootDir, ref.Namespace)
	if err != nil {
		return nil, nil, "", err
	}
	readCache.returnViews = true
	readCache.forceReadAtFallback = columnVectorGraphQuantizedAssetForceReadAtFallbackForTest.Load()
	reader, err := readCache.fileForRef(ref)
	if err != nil {
		_ = readCache.close()
		return nil, nil, "", err
	}
	manager := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopePreparedSearch, ID: fmt.Sprintf("column-graph-quantized-%s-%d-%d-%d", ref.Namespace, ref.Generation, ref.PartID, ref.FileID), Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph quantized asset"}
	key := mappedResourceKeyForColumnAssetRef(ref)
	opts := mappedresource.AcquireOptions{Reason: "column_graph quantized asset", ValidationMode: mappedResourceValidationModeForColumnAssetIntegrity(ColumnAssetReadIntegrityVerify), ResourceRoot: rootDir}
	if raw, ok, err := reader.readView(ref); err != nil {
		_ = readCache.close()
		return nil, nil, "", err
	} else if ok {
		if err := readCache.verifyReadChecksum(raw, ref, reader); err != nil {
			_ = readCache.close()
			return nil, nil, "", err
		}
		handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceMapped, raw, opts)
		if err != nil {
			_ = readCache.close()
			return nil, nil, "", err
		}
		return raw, &columnVectorGraphQuantizedAssetResource{manager: manager, handle: handle, readCache: &readCache}, mappedresource.SourceMapped, nil
	}
	raw, err := readColumnPhysicalAssetFromFileWithChecksum(reader.file, ref, nil, false)
	if err != nil {
		_ = readCache.close()
		return nil, nil, "", err
	}
	if err := readCache.verifyReadChecksum(raw, ref, reader); err != nil {
		_ = readCache.close()
		return nil, nil, "", err
	}
	if err := readCache.close(); err != nil {
		return nil, nil, "", err
	}
	handle, err := manager.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, raw, opts)
	if err != nil {
		return nil, nil, "", err
	}
	return raw, &columnVectorGraphQuantizedAssetResource{manager: manager, handle: handle}, mappedresource.SourceHeapCopy, nil
}

func columnVectorGraphQuantizedRequiredRoles(q QuantizedVectorIndexDefinition) []quantizedasset.Role {
	switch q.Codec {
	case brq.CodecName, rabitq.CodecName:
		return []quantizedasset.Role{quantizedasset.RolePackedCodes, quantizedasset.RoleCodeCount, quantizedasset.RoleQuantizedDotProductInv}
	case QuantizedVectorCodecScalarU8:
		if !scalarU8CalibrationIsLegacy(q) {
			return []quantizedasset.Role{quantizedasset.RoleCodes, quantizedasset.RoleScalarU8Alpha, quantizedasset.RoleGranuleRowCount}
		}
		return []quantizedasset.Role{quantizedasset.RoleCodes}
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
	switch q.Codec {
	case rabitq.CodecName:
		plan, err := rabitq.NewPlan(def.Dimensions, rabitq.DefaultConfig())
		if err != nil {
			return err
		}
		return validateColumnVectorGraphRabitQPreparedAsset(prepared, plan)
	case brq.CodecName:
		plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
		if err != nil {
			return err
		}
		return validateColumnVectorGraphBRQPreparedAsset(prepared, plan)
	case QuantizedVectorCodecScalarU8:
		return validateColumnVectorGraphScalarU8PreparedAsset(def, q, prepared)
	default:
		return nil
	}
}

func validateColumnVectorGraphScalarU8PreparedAsset(def VectorIndexDefinition, q QuantizedVectorIndexDefinition, prepared *quantizedasset.Prepared) error {
	if prepared == nil {
		return errors.New("prepared asset is nil")
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok {
		return errors.New("scalar_u8 code row view unavailable")
	}
	if codeRows.Rows() != prepared.Rows() || codeRows.BytesPerRow() != def.Dimensions || codeRows.ElementsPerRow() != def.Dimensions {
		return fmt.Errorf("scalar_u8 code shape rows/bytes/elements=(%d,%d,%d) want (%d,%d,%d)", codeRows.Rows(), codeRows.BytesPerRow(), codeRows.ElementsPerRow(), prepared.Rows(), def.Dimensions, def.Dimensions)
	}
	_, err := columnVectorGraphScalarU8AlphaLookupFromPrepared(q, prepared)
	return err
}

func prepareColumnVectorGraphScalarU8CodeSums(prepared *quantizedasset.Prepared, dims int) []uint32 {
	if !vectorops.DotScalarU8CenteredIndexedPreparedByteEligible(dims) || prepared == nil {
		return nil
	}
	rows, ok := prepared.CodeRowView(quantizedasset.RoleCodes)
	if !ok || rows.BytesPerRow() != dims {
		return nil
	}
	codes, ok := rows.PayloadBytes()
	if !ok || len(codes) != rows.Rows()*dims {
		return nil
	}
	sums := make([]uint32, rows.Rows())
	for row := range sums {
		for _, code := range codes[row*dims : (row+1)*dims] {
			sums[row] += uint32(code)
		}
	}
	return sums
}

func columnVectorGraphScalarU8AlphaLookupFromPrepared(q QuantizedVectorIndexDefinition, prepared *quantizedasset.Prepared) (*columnVectorGraphScalarU8AlphaLookup, error) {
	if scalarU8CalibrationIsLegacy(q) {
		return nil, nil
	}
	if prepared == nil {
		return nil, errors.New("prepared asset is nil")
	}
	alphaRows, ok := prepared.RoleRows(quantizedasset.RoleScalarU8Alpha)
	if !ok {
		return nil, errors.New("scalar_u8 alpha role unavailable")
	}
	countRows, ok := prepared.RoleRows(quantizedasset.RoleGranuleRowCount)
	if !ok {
		return nil, errors.New("scalar_u8 granule row-count role unavailable")
	}
	if alphaRows != countRows {
		return nil, fmt.Errorf("scalar_u8 alpha rows=%d row_count rows=%d", alphaRows, countRows)
	}
	alphaPayload, ok := prepared.Float32Payload(quantizedasset.RoleScalarU8Alpha)
	if !ok || len(alphaPayload) != alphaRows*4 {
		return nil, fmt.Errorf("scalar_u8 alpha payload bytes=%d ok=%v want %d", len(alphaPayload), ok, alphaRows*4)
	}
	rowCountPayload, ok := prepared.Uint32Payload(quantizedasset.RoleGranuleRowCount)
	if !ok || len(rowCountPayload) != countRows*4 {
		return nil, fmt.Errorf("scalar_u8 granule row-count payload bytes=%d ok=%v want %d", len(rowCountPayload), ok, countRows*4)
	}
	expectedRowCounts, err := columnVectorGraphScalarU8AlphaExpectedGranuleRowCounts(q, prepared.Rows())
	if err != nil {
		return nil, err
	}
	if alphaRows != len(expectedRowCounts) {
		return nil, fmt.Errorf("scalar_u8 alpha granules=%d want %d for rows=%d rows_per_granule=%d", alphaRows, len(expectedRowCounts), prepared.Rows(), typedColumnDefaultRowsPerGranule())
	}
	firstRows := make([]int, alphaRows+1)
	rowSum := 0
	uniformGranuleRows := 0
	for granule := 0; granule < alphaRows; granule++ {
		alpha := math.Float32frombits(binary.LittleEndian.Uint32(alphaPayload[granule*4 : granule*4+4]))
		if !validColumnVectorGraphScalarU8Alpha(alpha) {
			return nil, fmt.Errorf("scalar_u8 alpha granule %d value=%v is invalid", granule, alpha)
		}
		count := binary.LittleEndian.Uint32(rowCountPayload[granule*4 : granule*4+4])
		if count != expectedRowCounts[granule] {
			return nil, fmt.Errorf("scalar_u8 alpha granule %d row_count=%d want %d for rows=%d rows_per_granule=%d", granule, count, expectedRowCounts[granule], prepared.Rows(), typedColumnDefaultRowsPerGranule())
		}
		if count == 0 || uint64(count) > uint64(math.MaxInt-rowSum) {
			return nil, fmt.Errorf("scalar_u8 alpha granule %d row_count=%d is invalid", granule, count)
		}
		if granule == 0 {
			uniformGranuleRows = int(count)
		} else if granule < alphaRows-1 && int(count) != uniformGranuleRows {
			uniformGranuleRows = 0
		}
		firstRows[granule] = rowSum
		rowSum += int(count)
	}
	firstRows[alphaRows] = rowSum
	if rowSum != prepared.Rows() {
		return nil, fmt.Errorf("scalar_u8 alpha row_counts sum=%d want prepared rows=%d", rowSum, prepared.Rows())
	}
	return &columnVectorGraphScalarU8AlphaLookup{rows: prepared.Rows(), granules: alphaRows, alphaPayload: alphaPayload, rowCountPayload: rowCountPayload, firstRows: firstRows, uniformGranuleRows: uniformGranuleRows}, nil
}

func validateColumnVectorGraphRabitQPreparedAsset(prepared *quantizedasset.Prepared, plan *rabitq.Plan) error {
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

func validateColumnVectorGraphBRQPreparedAsset(prepared *quantizedasset.Prepared, plan *brq.Plan) error {
	if prepared.Rows() == 0 {
		return nil
	}
	codeRows, ok := prepared.CodeRowView(quantizedasset.RolePackedCodes)
	if !ok {
		return errors.New("brq_1bit packed code row view unavailable")
	}
	if codeRows.Rows() != prepared.Rows() || codeRows.BytesPerRow() != plan.BytesPerCode() || codeRows.ElementsPerRow() != plan.CodeDimensions() {
		return fmt.Errorf("brq_1bit packed code shape rows/bytes/elements=(%d,%d,%d) want (%d,%d,%d)", codeRows.Rows(), codeRows.BytesPerRow(), codeRows.ElementsPerRow(), prepared.Rows(), plan.BytesPerCode(), plan.CodeDimensions())
	}
	for ordinal := 0; ordinal < prepared.Rows(); ordinal++ {
		code, ok := codeRows.RowBytes(ordinal)
		if !ok {
			return fmt.Errorf("brq_1bit code row ordinal=%d unavailable", ordinal)
		}
		count, ok := prepared.Uint32(quantizedasset.RoleCodeCount, ordinal)
		if !ok {
			return fmt.Errorf("brq_1bit code_count ordinal=%d unavailable", ordinal)
		}
		if err := plan.ValidateCode(code, count); err != nil {
			return fmt.Errorf("brq_1bit code ordinal=%d: %w", ordinal, err)
		}
		qdp, ok := prepared.Float32(quantizedasset.RoleQuantizedDotProductInv, ordinal)
		if !ok {
			return fmt.Errorf("brq_1bit quantized_dot_product_inv ordinal=%d unavailable", ordinal)
		}
		if err := plan.ValidateQuantizedDotProductInv(qdp); err != nil {
			return fmt.Errorf("brq_1bit quantized_dot_product_inv ordinal=%d: %w", ordinal, err)
		}
	}
	return nil
}

func columnVectorGraphQuantizedAssetSchema(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, asset columnVectorIndexStateAssetSnapshot, ref quantizedasset.AssetRefIdentity) (quantizedasset.SchemaDescriptor, error) {
	return columnVectorGraphQuantizedAssetSchemaFromAssets(def, graph, q, columnVectorGraphQuantizedAssetSet{Codes: asset, HasCodes: true}, ref, quantizedasset.AssetRefIdentity{})
}

func columnVectorGraphQuantizedAssetSchemaFromAssets(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, q QuantizedVectorIndexDefinition, assets columnVectorGraphQuantizedAssetSet, codeRef, alphaRef quantizedasset.AssetRefIdentity) (quantizedasset.SchemaDescriptor, error) {
	asset := assets.Codes
	ref := codeRef
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
		codecConfig, codecConfigHash, err := scalarU8CalibrationCodecConfig(q)
		if err != nil {
			return quantizedasset.SchemaDescriptor{}, err
		}
		schema.CodeDimensions = def.Dimensions
		schema.CodeWidthBits = 8
		schema.Codec = quantizedasset.CodecDescriptor{Name: q.Codec, Version: q.Version, ConfigHash: codecConfigHash, Config: codecConfig}
		schema.Columns = []quantizedasset.ColumnDescriptor{{
			Role:             quantizedasset.RoleCodes,
			Column:           columnVectorGraphQuantizedCodesColumnName,
			AssetID:          asset.AssetID,
			Required:         true,
			LogicalType:      string(columnsemantics.LogicalByteVector),
			Type:             typedcolumn.ColumnTypeFixedBytes,
			Encoding:         typedcolumn.EncodingRawFixedBytes,
			BytesPerRow:      def.Dimensions,
			SourceSchemaHash: asset.SourceSchemaHash,
			AssetBytes:       asset.AssetBytes,
			Ref:              ref,
		}}
		if !scalarU8CalibrationIsLegacy(q) {
			if !assets.HasAlpha {
				return quantizedasset.SchemaDescriptor{}, fmt.Errorf("scalar_u8 alpha asset is required")
			}
			schema.GranuleCount = assets.Alpha.RowCount
			schema.Columns = append(schema.Columns,
				quantizedasset.ColumnDescriptor{
					Role:             quantizedasset.RoleScalarU8Alpha,
					Column:           columnVectorGraphQuantizedScalarU8AlphaColumnName,
					AssetID:          assets.Alpha.AssetID,
					Required:         true,
					LogicalType:      string(columnsemantics.LogicalFloat32),
					Type:             typedcolumn.ColumnTypeFloat32,
					Encoding:         typedcolumn.EncodingRawFloat32,
					RowCount:         assets.Alpha.RowCount,
					SourceSchemaHash: assets.Alpha.SourceSchemaHash,
					AssetBytes:       assets.Alpha.AssetBytes,
					Ref:              alphaRef,
				},
				quantizedasset.ColumnDescriptor{
					Role:             quantizedasset.RoleGranuleRowCount,
					Column:           columnVectorGraphQuantizedGranuleRowCountColumnName,
					AssetID:          assets.Alpha.AssetID,
					Required:         true,
					LogicalType:      string(columnsemantics.LogicalUint32),
					Type:             typedcolumn.ColumnTypeUint32,
					Encoding:         typedcolumn.EncodingRawUint32,
					RowCount:         assets.Alpha.RowCount,
					SourceSchemaHash: assets.Alpha.SourceSchemaHash,
					AssetBytes:       assets.Alpha.AssetBytes,
					Ref:              alphaRef,
				})
		}
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
	case brq.CodecName:
		if q.Version != brq.CodecVersion {
			return quantizedasset.SchemaDescriptor{}, fmt.Errorf("brq_1bit version=%d is unsupported", q.Version)
		}
		plan, err := brq.NewPlan(def.Dimensions, brq.DefaultConfig())
		if err != nil {
			return quantizedasset.SchemaDescriptor{}, err
		}
		cfg := brq.DefaultConfig()
		schema.CodeDimensions = plan.CodeDimensions()
		schema.CodeWidthBits = brq.CodeWidthBits
		schema.Codec = quantizedasset.CodecDescriptor{Name: brq.CodecName, Version: brq.CodecVersion, ConfigHash: cfg.Hash64(), Config: cfg.CanonicalBytes()}
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
				BitsPerElement:   brq.CodeWidthBits,
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
