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
	columnDictionaryCodesAssetMagic         = uint32(0x54434443) // TCDC
	columnDictionaryCodesAssetVersionV1     = uint16(1)
	columnDictionaryCodesAssetVersion       = uint16(2)
	columnDictionaryCodesPayloadElemBytes   = 4
	columnDictionaryCodesPayloadAlignment   = 4
	columnDictionaryCodesPayloadEndianLabel = "little_endian"
)

type columnDictionaryCodesAsset struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	SchemaHash        uint64
	ColumnName        string
	ColumnIndex       int
	Dictionary        []string
	Codes             []uint32
}

type columnDictionaryCodesAssetPayload struct {
	rowCount  int
	offset    int
	byteLen   int
	alignment int
}

func buildColumnDictionaryCodesAssets(cfg ColumnStoreConfig, rows []columnDeclaredRow, collection, namespace string, generation, partID, appliedCommandLSN uint64) ([]columnDictionaryCodesAsset, error) {
	var assets []columnDictionaryCodesAsset
	for colIdx, col := range cfg.Columns {
		if !col.Dictionary || col.ValueType != ColumnStoreValueString {
			continue
		}
		asset, ok, err := buildColumnDictionaryCodesAssetForColumn(cfg, rows, collection, namespace, generation, partID, appliedCommandLSN, colIdx)
		if err != nil {
			return nil, err
		}
		if ok {
			assets = append(assets, asset)
		}
	}
	return assets, nil
}

func buildColumnDictionaryCodesAssetForColumn(cfg ColumnStoreConfig, rows []columnDeclaredRow, collection, namespace string, generation, partID, appliedCommandLSN uint64, colIdx int) (columnDictionaryCodesAsset, bool, error) {
	if colIdx < 0 || colIdx >= len(cfg.Columns) {
		return columnDictionaryCodesAsset{}, false, fmt.Errorf("collections: dictionary code column index=%d outside columns=%d", colIdx, len(cfg.Columns))
	}
	col := cfg.Columns[colIdx]
	if !col.Dictionary || col.ValueType != ColumnStoreValueString {
		return columnDictionaryCodesAsset{}, false, nil
	}
	byValue := make(map[string]uint32)
	dict := make([]string, 0)
	codes := make([]uint32, 0, len(rows))
	for rowIdx, row := range rows {
		if row.Deleted {
			return columnDictionaryCodesAsset{}, false, nil
		}
		if len(row.Values) != len(cfg.Columns) {
			return columnDictionaryCodesAsset{}, false, fmt.Errorf("collections: dictionary code row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		value := row.Values[colIdx]
		if value.Type != ColumnStoreValueString {
			return columnDictionaryCodesAsset{}, false, fmt.Errorf("collections: dictionary code row[%d] column[%d] type=%q want string", rowIdx, colIdx, value.Type)
		}
		if !value.Present || value.Null {
			return columnDictionaryCodesAsset{}, false, nil
		}
		code, ok := byValue[value.String]
		if !ok {
			if uint64(len(dict)) == uint64(^uint32(0)) {
				return columnDictionaryCodesAsset{}, false, errors.New("collections: dictionary code cardinality exceeds uint32")
			}
			code = uint32(len(dict))
			byValue[value.String] = code
			dict = append(dict, value.String)
		}
		codes = append(codes, code)
	}
	if len(codes) == 0 {
		return columnDictionaryCodesAsset{}, false, nil
	}
	return columnDictionaryCodesAsset{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		SchemaHash:        cfg.SchemaHash,
		ColumnName:        col.Name,
		ColumnIndex:       colIdx,
		Dictionary:        dict,
		Codes:             codes,
	}, true, nil
}

func encodeColumnDictionaryCodesAsset(asset columnDictionaryCodesAsset) ([]byte, error) {
	if asset.Collection == "" || asset.Namespace == "" || asset.Generation == 0 || asset.PartID == 0 || asset.ColumnName == "" {
		return nil, errors.New("collections: dictionary codes asset missing collection, namespace, generation, part_id, or column")
	}
	if asset.ColumnIndex < 0 {
		return nil, errors.New("collections: dictionary codes asset column index is negative")
	}
	if len(asset.Dictionary) == 0 || len(asset.Codes) == 0 {
		return nil, errors.New("collections: dictionary codes asset requires dictionary and codes")
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnDictionaryCodesAssetMagic)
	writeManifestUint16(&b, columnDictionaryCodesAssetVersion)
	writeManifestString(&b, asset.Collection)
	writeManifestString(&b, asset.Namespace)
	writeManifestUint64(&b, asset.Generation)
	writeManifestUint64(&b, asset.PartID)
	writeManifestUint64(&b, asset.AppliedCommandLSN)
	writeManifestUint64(&b, asset.SchemaHash)
	writeManifestString(&b, asset.ColumnName)
	writeManifestUint64(&b, uint64(asset.ColumnIndex))
	writeManifestUint64(&b, uint64(len(asset.Dictionary)))
	writeManifestUint64(&b, uint64(len(asset.Codes)))
	for _, value := range asset.Dictionary {
		writeManifestString(&b, value)
	}
	writeColumnSidecarPayloadPadding(&b, columnDictionaryCodesPayloadAlignment)
	var codeBuf [columnDictionaryCodesPayloadElemBytes]byte
	for idx, code := range asset.Codes {
		if int(code) >= len(asset.Dictionary) {
			return nil, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", idx, code, len(asset.Dictionary))
		}
		binary.LittleEndian.PutUint32(codeBuf[:], code)
		_, _ = b.Write(codeBuf[:])
	}
	return b.Bytes(), nil
}

func decodeColumnDictionaryCodesAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnDictionaryCodesAsset, error) {
	asset, payload, err := decodeColumnDictionaryCodesAssetPayload(raw, ref, cfg, expectedCollection, expectedColumn, verifyChecksum)
	if err != nil {
		return columnDictionaryCodesAsset{}, err
	}
	codes, err := copyColumnDictionaryCodesPayload(raw, payload)
	if err != nil {
		return columnDictionaryCodesAsset{}, err
	}
	for i, code := range codes {
		if int(code) >= len(asset.Dictionary) {
			return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, code, len(asset.Dictionary))
		}
	}
	asset.Codes = codes
	return asset, nil
}

func decodeColumnDictionaryCodesAssetPayload(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnDictionaryCodesAsset, columnDictionaryCodesAssetPayload, error) {
	if ref.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset ref kind=%q want %q", ref.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if int64(len(raw)) != ref.Length {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if verifyChecksum {
		if checksum := page.Checksum(raw); checksum != ref.Checksum {
			return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
		}
	}
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnDictionaryCodesAssetMagic {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: bad dictionary codes asset magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnDictionaryCodesAssetVersion {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: unsupported dictionary codes asset version=%d", version)
	}
	collectionBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	asset := columnDictionaryCodesAsset{
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		SchemaHash:        cur.u64(),
	}
	columnNameBytes := cur.stringBytes()
	columnIndex := cur.u64()
	cardinality := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, err
	}
	if columnIndex > uint64(maxCollectionInt) || cardinality > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, errors.New("collections: dictionary codes asset dimensions overflow int")
	}
	if rowCount > uint64((len(raw)-cur.pos)/columnDictionaryCodesPayloadElemBytes) {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, errors.New("collections: dictionary codes asset row count exceeds payload bytes")
	}
	asset.ColumnIndex = int(columnIndex)
	asset.Dictionary = make([]string, int(cardinality))
	for i := range asset.Dictionary {
		asset.Dictionary[i] = cur.string()
	}
	payload, err := columnDictionaryCodesPayloadAfterDictionary(raw, ref, &cur, int(rowCount))
	if err != nil {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, err
	}
	if !manifestBytesEqualString(collectionBytes, expectedCollection) {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset collection=%q want %q", string(collectionBytes), expectedCollection)
	}
	asset.Collection = expectedCollection
	if cfg.AssetManager == nil {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, errors.New("collections: dictionary codes asset validation requires asset manager")
	}
	if !manifestBytesEqualString(namespaceBytes, cfg.AssetManager.Namespace) || ref.Namespace != cfg.AssetManager.Namespace {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset namespace=%q ref_namespace=%q want %q", string(namespaceBytes), ref.Namespace, cfg.AssetManager.Namespace)
	}
	asset.Namespace = cfg.AssetManager.Namespace
	if asset.Generation != ref.Generation || asset.PartID != ref.PartID {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset generation/part does not match ref")
	}
	if asset.SchemaHash != cfg.SchemaHash {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset schema_hash=%d want %d", asset.SchemaHash, cfg.SchemaHash)
	}
	if asset.AppliedCommandLSN > cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset applied_command_lsn=%d is newer than recovery applied_command_lsn=%d", asset.AppliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	if expectedColumn != "" && !manifestBytesEqualString(columnNameBytes, expectedColumn) {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset column=%q want %q", string(columnNameBytes), expectedColumn)
	}
	if expectedColumn != "" {
		asset.ColumnName = expectedColumn
	} else {
		asset.ColumnName = string(columnNameBytes)
	}
	if asset.ColumnIndex < 0 || asset.ColumnIndex >= len(cfg.Columns) {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset column_index=%d outside columns=%d", asset.ColumnIndex, len(cfg.Columns))
	}
	col := cfg.Columns[asset.ColumnIndex]
	if col.Name != asset.ColumnName || col.ValueType != ColumnStoreValueString || !col.Dictionary {
		return columnDictionaryCodesAsset{}, columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset column %q is not a declared dictionary string column", asset.ColumnName)
	}
	return asset, payload, nil
}

func decodeColumnDictionaryCodesAssetHeader(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (manifestCursor, int, int, error) {
	if ref.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset ref kind=%q want %q", ref.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if int64(len(raw)) != ref.Length {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if verifyChecksum {
		if checksum := page.Checksum(raw); checksum != ref.Checksum {
			return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
		}
	}
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnDictionaryCodesAssetMagic {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: bad dictionary codes asset magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnDictionaryCodesAssetVersion {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: unsupported dictionary codes asset version=%d", version)
	}
	collectionBytes := cur.stringBytes()
	namespaceBytes := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	appliedCommandLSN := cur.u64()
	schemaHash := cur.u64()
	columnNameBytes := cur.stringBytes()
	columnIndex := cur.u64()
	cardinality := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return manifestCursor{}, 0, 0, err
	}
	if columnIndex > uint64(maxCollectionInt) || cardinality > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return manifestCursor{}, 0, 0, errors.New("collections: dictionary codes asset dimensions overflow int")
	}
	if rowCount > uint64((len(raw)-cur.pos)/columnDictionaryCodesPayloadElemBytes) {
		return manifestCursor{}, 0, 0, errors.New("collections: dictionary codes asset row count exceeds payload bytes")
	}
	if !manifestBytesEqualString(collectionBytes, expectedCollection) {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset collection=%q want %q", string(collectionBytes), expectedCollection)
	}
	if cfg.AssetManager == nil {
		return manifestCursor{}, 0, 0, errors.New("collections: dictionary codes asset validation requires asset manager")
	}
	if !manifestBytesEqualString(namespaceBytes, cfg.AssetManager.Namespace) || ref.Namespace != cfg.AssetManager.Namespace {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset namespace=%q ref_namespace=%q want %q", string(namespaceBytes), ref.Namespace, cfg.AssetManager.Namespace)
	}
	if generation != ref.Generation || partID != ref.PartID {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset generation/part does not match ref")
	}
	if schemaHash != cfg.SchemaHash {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset schema_hash=%d want %d", schemaHash, cfg.SchemaHash)
	}
	if appliedCommandLSN > cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset applied_command_lsn=%d is newer than recovery applied_command_lsn=%d", appliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	if expectedColumn != "" && !manifestBytesEqualString(columnNameBytes, expectedColumn) {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset column=%q want %q", string(columnNameBytes), expectedColumn)
	}
	columnIndexInt := int(columnIndex)
	if columnIndexInt < 0 || columnIndexInt >= len(cfg.Columns) {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset column_index=%d outside columns=%d", columnIndexInt, len(cfg.Columns))
	}
	col := cfg.Columns[columnIndexInt]
	if !manifestBytesEqualString(columnNameBytes, col.Name) || col.ValueType != ColumnStoreValueString || !col.Dictionary {
		return manifestCursor{}, 0, 0, fmt.Errorf("collections: dictionary codes asset column %q is not a declared dictionary string column", string(columnNameBytes))
	}
	return cur, int(cardinality), int(rowCount), nil
}

func writeColumnSidecarPayloadPadding(b *bytes.Buffer, alignment int) {
	if b == nil || alignment <= 1 {
		return
	}
	padding := columnSidecarPayloadPadding(b.Len(), alignment)
	for i := 0; i < padding; i++ {
		_ = b.WriteByte(0)
	}
}

func columnSidecarPayloadPadding(offset int, alignment int) int {
	if alignment <= 1 {
		return 0
	}
	rem := offset % alignment
	if rem == 0 {
		return 0
	}
	return alignment - rem
}

func columnDictionaryCodesPayloadAfterDictionary(raw []byte, ref ColumnAssetRef, cur *manifestCursor, rowCount int) (columnDictionaryCodesAssetPayload, error) {
	if cur == nil {
		return columnDictionaryCodesAssetPayload{}, errors.New("collections: nil dictionary codes asset cursor")
	}
	if cur.err != nil {
		return columnDictionaryCodesAssetPayload{}, cur.err
	}
	if rowCount < 0 {
		return columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset negative row count=%d", rowCount)
	}
	padding := columnSidecarPayloadPadding(cur.pos, columnDictionaryCodesPayloadAlignment)
	if padding > len(raw)-cur.pos {
		return columnDictionaryCodesAssetPayload{}, errors.New("collections: short dictionary codes asset payload padding")
	}
	for i := 0; i < padding; i++ {
		if raw[cur.pos+i] != 0 {
			return columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: non-zero dictionary codes asset payload padding byte[%d]", i)
		}
	}
	cur.pos += padding
	byteLen64 := uint64(rowCount) * columnDictionaryCodesPayloadElemBytes
	if byteLen64 > uint64(maxCollectionInt) {
		return columnDictionaryCodesAssetPayload{}, errors.New("collections: dictionary codes asset row payload overflows int")
	}
	byteLen := int(byteLen64)
	if byteLen > len(raw)-cur.pos {
		return columnDictionaryCodesAssetPayload{}, errors.New("collections: dictionary codes asset row count exceeds payload bytes")
	}
	if byteLen != len(raw)-cur.pos {
		return columnDictionaryCodesAssetPayload{}, errors.New("collections: trailing bytes in dictionary codes asset")
	}
	payload := columnDictionaryCodesAssetPayload{
		rowCount:  rowCount,
		offset:    cur.pos,
		byteLen:   byteLen,
		alignment: columnDictionaryCodesPayloadAlignment,
	}
	if (ref.Offset+int64(payload.offset))%int64(columnDictionaryCodesPayloadAlignment) != 0 {
		return columnDictionaryCodesAssetPayload{}, fmt.Errorf("collections: dictionary codes asset payload absolute offset=%d is not %d-byte aligned", ref.Offset+int64(payload.offset), columnDictionaryCodesPayloadAlignment)
	}
	return payload, nil
}

func columnDictionaryCodesPayloadBytes(raw []byte, payload columnDictionaryCodesAssetPayload) ([]byte, error) {
	if payload.rowCount < 0 || payload.offset < 0 || payload.byteLen < 0 {
		return nil, errors.New("collections: invalid dictionary codes asset payload bounds")
	}
	if payload.alignment != 0 && payload.alignment != columnDictionaryCodesPayloadAlignment {
		return nil, fmt.Errorf("collections: unsupported dictionary codes asset payload alignment=%d", payload.alignment)
	}
	if payload.rowCount > maxCollectionInt/columnDictionaryCodesPayloadElemBytes {
		return nil, errors.New("collections: dictionary codes asset row payload overflows int")
	}
	wantBytes := payload.rowCount * columnDictionaryCodesPayloadElemBytes
	if payload.byteLen != wantBytes {
		return nil, fmt.Errorf("collections: dictionary codes asset payload bytes=%d want rows*4=%d", payload.byteLen, wantBytes)
	}
	end := payload.offset + payload.byteLen
	if end < payload.offset || end > len(raw) {
		return nil, errors.New("collections: dictionary codes asset payload outside raw bytes")
	}
	return raw[payload.offset:end], nil
}

func viewColumnDictionaryCodesPayload(raw []byte, payload columnDictionaryCodesAssetPayload) ([]uint32, bool, error) {
	payloadBytes, err := columnDictionaryCodesPayloadBytes(raw, payload)
	if err != nil {
		return nil, false, err
	}
	if codes, ok := columnDictionaryCodesLittleEndianDirectView(payloadBytes, payload.rowCount); ok {
		return codes, true, nil
	}
	codes := make([]uint32, payload.rowCount)
	copyColumnDictionaryCodesLittleEndian(codes, payloadBytes)
	return codes, false, nil
}

func copyColumnDictionaryCodesPayload(raw []byte, payload columnDictionaryCodesAssetPayload) ([]uint32, error) {
	payloadBytes, err := columnDictionaryCodesPayloadBytes(raw, payload)
	if err != nil {
		return nil, err
	}
	codes := make([]uint32, payload.rowCount)
	copyColumnDictionaryCodesLittleEndian(codes, payloadBytes)
	return codes, nil
}

func columnDictionaryCodesLittleEndianDirectView(raw []byte, count int) ([]uint32, bool) {
	if count < 0 || len(raw)%columnDictionaryCodesPayloadElemBytes != 0 || len(raw)/columnDictionaryCodesPayloadElemBytes != count {
		return nil, false
	}
	if count == 0 {
		return nil, true
	}
	ptr := unsafe.Pointer(unsafe.SliceData(raw))
	if !columnPhysicalNativeLittleEndian || uintptr(ptr)%unsafe.Alignof(uint32(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*uint32)(ptr), count), true
}

func copyColumnDictionaryCodesLittleEndian(dst []uint32, raw []byte) {
	if len(dst)*columnDictionaryCodesPayloadElemBytes != len(raw) {
		panic("collections: invalid dictionary codes little-endian copy length")
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
		dst[i] = binary.LittleEndian.Uint32(raw[i*columnDictionaryCodesPayloadElemBytes:])
	}
}

func columnDictionaryCodeOwnedString(value []byte) string {
	return string(value)
}
