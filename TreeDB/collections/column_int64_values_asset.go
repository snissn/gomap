package collections

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnInt64ValuesAssetMagic   = uint32(0x54434938) // TCI8
	columnInt64ValuesAssetVersion = uint16(1)
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
	rowCount int
	offset   int
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
	for rowIdx, row := range rows {
		if row.Deleted {
			return columnInt64ValuesAsset{}, false, nil
		}
		if len(row.Values) != len(cfg.Columns) {
			return columnInt64ValuesAsset{}, false, fmt.Errorf("collections: int64 values row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		value := row.Values[colIdx]
		if value.Type != ColumnStoreValueInt64 {
			return columnInt64ValuesAsset{}, false, fmt.Errorf("collections: int64 values row[%d] column[%d] type=%q want int64", rowIdx, colIdx, value.Type)
		}
		if !value.Present || value.Null {
			return columnInt64ValuesAsset{}, false, nil
		}
	}
	if len(rows) == 0 {
		return columnInt64ValuesAsset{}, false, nil
	}
	values := make([]int64, len(rows))
	for rowIdx, row := range rows {
		values[rowIdx] = row.Values[colIdx].Int64
	}
	return columnInt64ValuesAsset{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		SchemaHash:        cfg.SchemaHash,
		ColumnName:        col.Name,
		ColumnIndex:       colIdx,
		Values:            values,
	}, true, nil
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
	for _, value := range asset.Values {
		writeManifestUint64(&b, uint64(value))
	}
	return b.Bytes(), nil
}

func decodeColumnInt64ValuesAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnInt64ValuesAsset, error) {
	asset, payload, err := decodeColumnInt64ValuesAssetPayload(raw, ref, cfg, expectedCollection, expectedColumn, verifyChecksum)
	if err != nil {
		return columnInt64ValuesAsset{}, err
	}
	cur := manifestCursor{raw: raw, pos: payload.offset}
	asset.Values = make([]int64, payload.rowCount)
	for i := range asset.Values {
		asset.Values[i] = int64(cur.u64())
	}
	if cur.err != nil {
		return columnInt64ValuesAsset{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnInt64ValuesAsset{}, errors.New("collections: trailing bytes in int64 values asset")
	}
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
	if rowCount > uint64((len(raw)-cur.pos)/8) {
		return columnInt64ValuesAsset{}, columnInt64ValuesAssetPayload{}, errors.New("collections: int64 values asset row count exceeds payload bytes")
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
	return asset, columnInt64ValuesAssetPayload{rowCount: int(rowCount), offset: cur.pos}, nil
}
