package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnInt64ValuesAssetMagic         = uint32(0x54434938) // TCI8
	columnInt64ValuesAssetVersionV1     = uint16(1)
	columnInt64ValuesAssetVersion       = uint16(2)
	columnInt64ValuesPayloadElemBytes   = 8
	columnInt64ValuesPayloadAlignment   = 8
	columnInt64ValuesPayloadEndianLabel = "little_endian"
)

type columnInt64ValuesAsset struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	SchemaHash        uint64
	ColumnName        string
	ColumnIndex       int
	Values            []int64
}

type columnInt64ValuesAssetPayload struct {
	rowCount  int
	offset    int
	byteLen   int
	alignment int
}

func buildColumnInt64ValuesAssets(cfg ColumnStoreConfig, rows []columnDeclaredRow, collection, namespace string, generation, partID, appliedCommandLSN uint64) ([]columnInt64ValuesAsset, error) {
	var assets []columnInt64ValuesAsset
	for colIdx, col := range cfg.Columns {
		if col.ValueType != ColumnStoreValueInt64 || col.Nullable {
			continue
		}
		asset, ok, err := buildColumnInt64ValuesAssetForColumn(cfg, rows, collection, namespace, generation, partID, appliedCommandLSN, colIdx)
		if err != nil {
			return nil, err
		}
		if ok {
			assets = append(assets, asset)
		}
	}
	return assets, nil
}

func buildColumnInt64ValuesAssetForColumn(cfg ColumnStoreConfig, rows []columnDeclaredRow, collection, namespace string, generation, partID, appliedCommandLSN uint64, colIdx int) (columnInt64ValuesAsset, bool, error) {
	if colIdx < 0 || colIdx >= len(cfg.Columns) {
		return columnInt64ValuesAsset{}, false, fmt.Errorf("collections: int64 values column index=%d outside columns=%d", colIdx, len(cfg.Columns))
	}
	col := cfg.Columns[colIdx]
	if col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return columnInt64ValuesAsset{}, false, nil
	}
	if len(rows) == 0 {
		return columnInt64ValuesAsset{}, false, nil
	}
	builder := newColumnInt64ValuesAssetBuilder(col, colIdx, len(rows))
	for rowIdx, row := range rows {
		if row.Deleted {
			return columnInt64ValuesAsset{}, false, nil
		}
		if len(row.Values) != len(cfg.Columns) {
			return columnInt64ValuesAsset{}, false, fmt.Errorf("collections: int64 values row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		if err := builder.appendValue(rowIdx, row.Values[colIdx]); err != nil {
			return columnInt64ValuesAsset{}, false, err
		}
		if !builder.valid {
			return columnInt64ValuesAsset{}, false, nil
		}
	}
	asset, ok := builder.asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedCommandLSN)
	return asset, ok, nil
}

type columnInt64ValuesAssetBuilder struct {
	column      ColumnStoreColumn
	columnIndex int
	values      []int64
	valid       bool
}

func newColumnInt64ValuesAssetBuilder(col ColumnStoreColumn, colIdx, rows int) columnInt64ValuesAssetBuilder {
	return columnInt64ValuesAssetBuilder{
		column:      col,
		columnIndex: colIdx,
		values:      make([]int64, rows),
		valid:       true,
	}
}

func (b *columnInt64ValuesAssetBuilder) appendValue(rowIdx int, value columnDeclaredValue) error {
	if !b.valid {
		return nil
	}
	if value.Type != ColumnStoreValueInt64 {
		return fmt.Errorf("collections: int64 values row[%d] column[%d] type=%q want int64", rowIdx, b.columnIndex, value.Type)
	}
	if !value.Present || value.Null {
		b.valid = false
		return nil
	}
	b.values[rowIdx] = value.Int64
	return nil
}

func (b *columnInt64ValuesAssetBuilder) asset(schemaHash uint64, collection, namespace string, generation, partID, appliedCommandLSN uint64) (columnInt64ValuesAsset, bool) {
	if !b.valid || len(b.values) == 0 {
		return columnInt64ValuesAsset{}, false
	}
	return columnInt64ValuesAsset{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		SchemaHash:        schemaHash,
		ColumnName:        b.column.Name,
		ColumnIndex:       b.columnIndex,
		Values:            b.values,
	}, true
}

func encodeColumnInt64ValuesAsset(asset columnInt64ValuesAsset) ([]byte, error) {
	if asset.Collection == "" || asset.Namespace == "" || asset.Generation == 0 || asset.PartID == 0 || asset.ColumnName == "" {
		return nil, errors.New("collections: int64 values asset missing collection, namespace, generation, part_id, or column")
	}
	if asset.ColumnIndex < 0 {
		return nil, errors.New("collections: int64 values asset column index is negative")
	}
	if len(asset.Values) == 0 {
		return nil, errors.New("collections: int64 values asset requires values")
	}
	var b bytes.Buffer
	b.Grow(columnInt64ValuesEncodedSize(asset))
	writeManifestUint32(&b, columnInt64ValuesAssetMagic)
	writeManifestUint16(&b, columnInt64ValuesAssetVersion)
	writeManifestString(&b, asset.Collection)
	writeManifestString(&b, asset.Namespace)
	writeManifestUint64(&b, asset.Generation)
	writeManifestUint64(&b, asset.PartID)
	writeManifestUint64(&b, asset.AppliedCommandLSN)
	writeManifestUint64(&b, asset.SchemaHash)
	writeManifestString(&b, asset.ColumnName)
	writeManifestUint64(&b, uint64(asset.ColumnIndex))
	writeManifestUint64(&b, uint64(len(asset.Values)))
	writeColumnSidecarPayloadPadding(&b, columnInt64ValuesPayloadAlignment)
	var valueBuf [columnInt64ValuesPayloadElemBytes]byte
	for _, value := range asset.Values {
		binary.LittleEndian.PutUint64(valueBuf[:], uint64(value))
		_, _ = b.Write(valueBuf[:])
	}
	return b.Bytes(), nil
}

func columnInt64ValuesEncodedSize(asset columnInt64ValuesAsset) int {
	size := 4 + 2
	size += manifestStringEncodedSize(asset.Collection)
	size += manifestStringEncodedSize(asset.Namespace)
	size += 4 * 8
	size += manifestStringEncodedSize(asset.ColumnName)
	size += 2 * 8
	size += columnSidecarPayloadPadding(size, columnInt64ValuesPayloadAlignment)
	size += len(asset.Values) * columnInt64ValuesPayloadElemBytes
	return size
}

func decodeColumnInt64ValuesAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnInt64ValuesAsset, error) {
	asset, payload, err := decodeColumnInt64ValuesAssetPayload(raw, ref, cfg, expectedCollection, expectedColumn, verifyChecksum)
	if err != nil {
		return columnInt64ValuesAsset{}, err
	}
	values, err := copyColumnInt64ValuesPayload(raw, payload)
	if err != nil {
		return columnInt64ValuesAsset{}, err
	}
	asset.Values = values
	return asset, nil
}

func decodeColumnInt64ValuesAssetPayload(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnInt64ValuesAsset, columnInt64ValuesAssetPayload, error) {
	if ref.Kind != ColumnAssetKindTCS1Int64Values {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset ref kind=%q want %q", ref.Kind, ColumnAssetKindTCS1Int64Values)
	}
	if int64(len(raw)) != ref.Length {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if verifyChecksum {
		if checksum := page.Checksum(raw); checksum != ref.Checksum {
			return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
		}
	}
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnInt64ValuesAssetMagic {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: bad int64 values asset magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnInt64ValuesAssetVersion {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: unsupported int64 values asset version=%d", version)
	}
	collectionBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	asset := columnInt64ValuesAsset{
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		SchemaHash:        cur.u64(),
	}
	columnNameBytes := cur.stringBytes()
	columnIndex := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, err
	}
	if columnIndex > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, errors.New("collections: int64 values asset dimensions overflow int")
	}
	payload, err := columnInt64ValuesPayloadAfterHeader(raw, ref, &cur, int(rowCount))
	if err != nil {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, err
	}
	asset.ColumnIndex = int(columnIndex)
	if !manifestBytesEqualString(collectionBytes, expectedCollection) {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset collection=%q want %q", string(collectionBytes), expectedCollection)
	}
	asset.Collection = expectedCollection
	if cfg.AssetManager == nil {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, errors.New("collections: int64 values asset validation requires asset manager")
	}
	if !manifestBytesEqualString(namespaceBytes, cfg.AssetManager.Namespace) || ref.Namespace != cfg.AssetManager.Namespace {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset namespace=%q ref_namespace=%q want %q", string(namespaceBytes), ref.Namespace, cfg.AssetManager.Namespace)
	}
	asset.Namespace = cfg.AssetManager.Namespace
	if asset.Generation != ref.Generation || asset.PartID != ref.PartID {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset generation/part does not match ref")
	}
	if asset.SchemaHash != cfg.SchemaHash {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset schema_hash=%d want %d", asset.SchemaHash, cfg.SchemaHash)
	}
	if asset.AppliedCommandLSN > cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset applied_command_lsn=%d is newer than recovery applied_command_lsn=%d", asset.AppliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	if expectedColumn != "" && !manifestBytesEqualString(columnNameBytes, expectedColumn) {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset column=%q want %q", string(columnNameBytes), expectedColumn)
	}
	if expectedColumn != "" {
		asset.ColumnName = expectedColumn
	} else {
		asset.ColumnName = string(columnNameBytes)
	}
	if asset.ColumnIndex < 0 || asset.ColumnIndex >= len(cfg.Columns) {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset column_index=%d outside columns=%d", asset.ColumnIndex, len(cfg.Columns))
	}
	col := cfg.Columns[asset.ColumnIndex]
	if col.Name != asset.ColumnName || col.ValueType != ColumnStoreValueInt64 || col.Nullable {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset column %q is not a non-null declared int64 column", asset.ColumnName)
	}
	return asset, payload, nil
}

func columnInt64ValuesPayloadAfterHeader(raw []byte, ref ColumnAssetRef, cur *manifestCursor, rowCount int) (columnInt64ValuesAssetPayload, error) {
	if cur == nil {
		return columnInt64ValuesAssetPayload{}, errors.New("collections: nil int64 values asset cursor")
	}
	if cur.err != nil {
		return columnInt64ValuesAssetPayload{}, cur.err
	}
	if rowCount < 0 {
		return columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset negative row count=%d", rowCount)
	}
	padding := columnSidecarPayloadPadding(cur.pos, columnInt64ValuesPayloadAlignment)
	if padding > len(raw)-cur.pos {
		return columnInt64ValuesAssetPayload{}, errors.New("collections: short int64 values asset payload padding")
	}
	for i := 0; i < padding; i++ {
		if raw[cur.pos+i] != 0 {
			return columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: non-zero int64 values asset payload padding byte[%d]", i)
		}
	}
	cur.pos += padding
	if rowCount > maxCollectionInt/columnInt64ValuesPayloadElemBytes {
		return columnInt64ValuesAssetPayload{}, errors.New("collections: int64 values asset row payload overflows int")
	}
	byteLen := rowCount * columnInt64ValuesPayloadElemBytes
	if byteLen > len(raw)-cur.pos {
		return columnInt64ValuesAssetPayload{}, errors.New("collections: int64 values asset row count exceeds payload bytes")
	}
	if byteLen != len(raw)-cur.pos {
		return columnInt64ValuesAssetPayload{}, errors.New("collections: trailing bytes in int64 values asset")
	}
	payload := columnInt64ValuesAssetPayload{
		rowCount:  rowCount,
		offset:    cur.pos,
		byteLen:   byteLen,
		alignment: columnInt64ValuesPayloadAlignment,
	}
	if (ref.Offset+int64(payload.offset))%int64(columnInt64ValuesPayloadAlignment) != 0 {
		return columnInt64ValuesAssetPayload{}, fmt.Errorf("collections: int64 values asset payload absolute offset=%d is not %d-byte aligned", ref.Offset+int64(payload.offset), columnInt64ValuesPayloadAlignment)
	}
	return payload, nil
}

func columnInt64ValuesPayloadBytes(raw []byte, payload columnInt64ValuesAssetPayload) ([]byte, error) {
	if payload.rowCount < 0 || payload.offset < 0 || payload.byteLen < 0 {
		return nil, errors.New("collections: invalid int64 values asset payload bounds")
	}
	if payload.alignment != 0 && payload.alignment != columnInt64ValuesPayloadAlignment {
		return nil, fmt.Errorf("collections: unsupported int64 values asset payload alignment=%d", payload.alignment)
	}
	if payload.rowCount > maxCollectionInt/columnInt64ValuesPayloadElemBytes {
		return nil, errors.New("collections: int64 values asset row payload overflows int")
	}
	wantBytes := payload.rowCount * columnInt64ValuesPayloadElemBytes
	if payload.byteLen != wantBytes {
		return nil, fmt.Errorf("collections: int64 values asset payload bytes=%d want rows*8=%d", payload.byteLen, wantBytes)
	}
	end := payload.offset + payload.byteLen
	if end < payload.offset || end > len(raw) {
		return nil, errors.New("collections: int64 values asset payload outside raw bytes")
	}
	return raw[payload.offset:end], nil
}

// viewColumnInt64ValuesPayload returns dense int64 row values from the v2
// little-endian payload. When directView is true, the returned []int64 aliases
// raw and must not be retained beyond raw's mmap/cache lifetime; callers that
// need owned data must use copyColumnInt64ValuesPayload or copy the slice.
// Header, schema/ref, checksum, row-count, padding, length, and absolute
// alignment validation happen before this helper is used.
func viewColumnInt64ValuesPayload(raw []byte, payload columnInt64ValuesAssetPayload) ([]int64, bool, error) {
	payloadBytes, err := columnInt64ValuesPayloadBytes(raw, payload)
	if err != nil {
		return nil, false, err
	}
	if values, ok := columnInt64ValuesLittleEndianDirectView(payloadBytes, payload.rowCount); ok {
		return values, true, nil
	}
	values := make([]int64, payload.rowCount)
	copyColumnInt64ValuesLittleEndian(values, payloadBytes)
	return values, false, nil
}

// copyColumnInt64ValuesPayload always returns owned row values decoded from the
// v2 little-endian payload, regardless of host byte order or alignment.
func copyColumnInt64ValuesPayload(raw []byte, payload columnInt64ValuesAssetPayload) ([]int64, error) {
	payloadBytes, err := columnInt64ValuesPayloadBytes(raw, payload)
	if err != nil {
		return nil, err
	}
	values := make([]int64, payload.rowCount)
	copyColumnInt64ValuesLittleEndian(values, payloadBytes)
	return values, nil
}

// columnInt64ValuesLittleEndianDirectView is the width-specific direct-view
// primitive for int64 value sidecars. It only checks native little-endian
// support, exact byte/count agreement, and pointer alignment; callers must do
// asset-level validation first and must respect the returned slice's raw-data
// lifetime.
func columnInt64ValuesLittleEndianDirectView(raw []byte, count int) ([]int64, bool) {
	if count < 0 || len(raw)%columnInt64ValuesPayloadElemBytes != 0 || len(raw)/columnInt64ValuesPayloadElemBytes != count {
		return nil, false
	}
	if count == 0 {
		return nil, true
	}
	ptr := unsafe.Pointer(unsafe.SliceData(raw))
	if !columnPhysicalNativeLittleEndian || uintptr(ptr)%unsafe.Alignof(int64(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*int64)(ptr), count), true
}

func copyColumnInt64ValuesLittleEndian(dst []int64, raw []byte) {
	if len(dst)*columnInt64ValuesPayloadElemBytes != len(raw) {
		panic("collections: invalid int64 values little-endian copy length")
	}
	if len(dst) == 0 {
		return
	}
	if columnPhysicalNativeLittleEndian {
		dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(dst))), len(raw))
		copy(dstBytes, raw)
		return
	}
	for i := range dst {
		dst[i] = int64(binary.LittleEndian.Uint64(raw[i*columnInt64ValuesPayloadElemBytes:]))
	}
}
