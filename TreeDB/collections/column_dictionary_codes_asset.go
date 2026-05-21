package collections

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnDictionaryCodesAssetMagic   = uint32(0x54434443) // TCDC
	columnDictionaryCodesAssetVersion = uint16(1)
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
	for idx, code := range asset.Codes {
		if int(code) >= len(asset.Dictionary) {
			return nil, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", idx, code, len(asset.Dictionary))
		}
		writeManifestUint32(&b, code)
	}
	return b.Bytes(), nil
}

func decodeColumnDictionaryCodesAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, expectedCollection, expectedColumn string, verifyChecksum bool) (columnDictionaryCodesAsset, error) {
	if ref.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset ref kind=%q want %q", ref.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if int64(len(raw)) != ref.Length {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset length=%d does not match ref length=%d", len(raw), ref.Length)
	}
	if verifyChecksum {
		if checksum := page.Checksum(raw); checksum != ref.Checksum {
			return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset checksum=%d does not match ref checksum=%d", checksum, ref.Checksum)
		}
	}
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnDictionaryCodesAssetMagic {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: bad dictionary codes asset magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnDictionaryCodesAssetVersion {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: unsupported dictionary codes asset version=%d", version)
	}
	asset := columnDictionaryCodesAsset{
		Collection:        cur.string(),
		Namespace:         cur.string(),
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		SchemaHash:        cur.u64(),
		ColumnName:        cur.string(),
	}
	columnIndex := cur.u64()
	cardinality := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return columnDictionaryCodesAsset{}, err
	}
	if columnIndex > uint64(maxCollectionInt) || cardinality > uint64(maxCollectionInt) || rowCount > uint64(maxCollectionInt) {
		return columnDictionaryCodesAsset{}, errors.New("collections: dictionary codes asset dimensions overflow int")
	}
	asset.ColumnIndex = int(columnIndex)
	asset.Dictionary = make([]string, int(cardinality))
	for i := range asset.Dictionary {
		asset.Dictionary[i] = cur.string()
	}
	asset.Codes = make([]uint32, int(rowCount))
	for i := range asset.Codes {
		code := cur.u32()
		if int(code) >= len(asset.Dictionary) {
			return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset code[%d]=%d outside cardinality=%d", i, code, len(asset.Dictionary))
		}
		asset.Codes[i] = code
	}
	if cur.err != nil {
		return columnDictionaryCodesAsset{}, cur.err
	}
	if cur.pos != len(raw) {
		return columnDictionaryCodesAsset{}, errors.New("collections: trailing bytes in dictionary codes asset")
	}
	if asset.Collection != expectedCollection {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset collection=%q want %q", asset.Collection, expectedCollection)
	}
	if cfg.AssetManager == nil {
		return columnDictionaryCodesAsset{}, errors.New("collections: dictionary codes asset validation requires asset manager")
	}
	if asset.Namespace != cfg.AssetManager.Namespace || ref.Namespace != cfg.AssetManager.Namespace {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset namespace=%q ref_namespace=%q want %q", asset.Namespace, ref.Namespace, cfg.AssetManager.Namespace)
	}
	if asset.Generation != ref.Generation || asset.PartID != ref.PartID {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset generation/part does not match ref")
	}
	if asset.SchemaHash != cfg.SchemaHash {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset schema_hash=%d want %d", asset.SchemaHash, cfg.SchemaHash)
	}
	if expectedColumn != "" && asset.ColumnName != expectedColumn {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset column=%q want %q", asset.ColumnName, expectedColumn)
	}
	if asset.ColumnIndex < 0 || asset.ColumnIndex >= len(cfg.Columns) {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset column_index=%d outside columns=%d", asset.ColumnIndex, len(cfg.Columns))
	}
	col := cfg.Columns[asset.ColumnIndex]
	if col.Name != asset.ColumnName || col.ValueType != ColumnStoreValueString || !col.Dictionary {
		return columnDictionaryCodesAsset{}, fmt.Errorf("collections: dictionary codes asset column %q is not a declared dictionary string column", asset.ColumnName)
	}
	return asset, nil
}
