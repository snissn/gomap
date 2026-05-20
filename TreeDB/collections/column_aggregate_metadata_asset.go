package collections

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

const (
	columnAggregateMetadataAssetMagic   = uint32(0x5443414d) // TCAM
	columnAggregateMetadataAssetVersion = uint16(1)
)

type columnAggregateMetadataEntry struct {
	Group string
	Count int
	Min   int64
	Max   int64
}

type columnAggregateMetadataAsset struct {
	Collection        string
	Namespace         string
	Generation        uint64
	PartID            uint64
	AppliedCommandLSN uint64
	SchemaHash        uint64
	AggregateName     string
	GroupColumn       string
	ValueColumn       string
	Rows              int
	Entries           []columnAggregateMetadataEntry
}

func encodeColumnAggregateMetadataAsset(asset columnAggregateMetadataAsset) ([]byte, error) {
	if asset.Collection == "" || asset.Namespace == "" || asset.AggregateName == "" || asset.GroupColumn == "" || asset.ValueColumn == "" {
		return nil, errors.New("collections: aggregate metadata asset requires collection, namespace, name, group column, and value column")
	}
	if asset.Generation == 0 || asset.PartID == 0 {
		return nil, errors.New("collections: aggregate metadata asset requires generation and part_id")
	}
	if asset.Rows < 0 {
		return nil, errors.New("collections: aggregate metadata asset rows must be non-negative")
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnAggregateMetadataAssetMagic)
	writeManifestUint16(&b, columnAggregateMetadataAssetVersion)
	writeManifestString(&b, asset.Collection)
	writeManifestString(&b, asset.Namespace)
	writeManifestUint64(&b, asset.Generation)
	writeManifestUint64(&b, asset.PartID)
	writeManifestUint64(&b, asset.AppliedCommandLSN)
	writeManifestUint64(&b, asset.SchemaHash)
	writeManifestString(&b, asset.AggregateName)
	writeManifestString(&b, asset.GroupColumn)
	writeManifestString(&b, asset.ValueColumn)
	writeManifestUint64(&b, uint64(asset.Rows))
	writeManifestUint64(&b, uint64(len(asset.Entries)))
	for _, entry := range asset.Entries {
		if entry.Count <= 0 {
			return nil, errors.New("collections: aggregate metadata asset entry count must be positive")
		}
		writeManifestString(&b, entry.Group)
		writeManifestUint64(&b, uint64(entry.Count))
		writeManifestUint64(&b, uint64(entry.Min))
		writeManifestUint64(&b, uint64(entry.Max))
	}
	return b.Bytes(), nil
}

func decodeColumnAggregateMetadataAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, collection, name string) (columnAggregateMetadataAsset, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnAggregateMetadataAssetMagic {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: bad aggregate metadata asset magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnAggregateMetadataAssetVersion {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: unsupported aggregate metadata asset version=%d", version)
	}
	asset := columnAggregateMetadataAsset{
		Collection:        cur.string(),
		Namespace:         cur.string(),
		Generation:        cur.u64(),
		PartID:            cur.u64(),
		AppliedCommandLSN: cur.u64(),
		SchemaHash:        cur.u64(),
		AggregateName:     cur.string(),
		GroupColumn:       cur.string(),
		ValueColumn:       cur.string(),
	}
	rows := cur.u64()
	entryCount := cur.u64()
	if err := cur.err; err != nil {
		return columnAggregateMetadataAsset{}, err
	}
	if rows > uint64(maxCollectionInt) || entryCount > uint64(maxCollectionInt) {
		return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset counts overflow int")
	}
	asset.Rows = int(rows)
	asset.Entries = make([]columnAggregateMetadataEntry, 0, int(entryCount))
	for i := 0; i < int(entryCount); i++ {
		group := cur.string()
		count := cur.u64()
		min := int64(cur.u64())
		max := int64(cur.u64())
		if err := cur.err; err != nil {
			return columnAggregateMetadataAsset{}, err
		}
		if count == 0 || count > uint64(maxCollectionInt) {
			return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset entry count overflows int or is zero")
		}
		asset.Entries = append(asset.Entries, columnAggregateMetadataEntry{Group: group, Count: int(count), Min: min, Max: max})
	}
	if cur.pos != len(raw) {
		return columnAggregateMetadataAsset{}, errors.New("collections: trailing bytes in aggregate metadata asset")
	}
	if ref.Kind != ColumnAssetKindTCS1AggregateMetadata {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset ref kind=%q want %q", ref.Kind, ColumnAssetKindTCS1AggregateMetadata)
	}
	if asset.Collection != collection {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset collection=%q want %q", asset.Collection, collection)
	}
	if cfg.AssetManager == nil || asset.Namespace != cfg.AssetManager.Namespace {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset namespace=%q want %q", asset.Namespace, cfg.AssetManager.Namespace)
	}
	if asset.Generation != ref.Generation || asset.PartID != ref.PartID {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset generation/part mismatch generation=%d/%d part=%d/%d", asset.Generation, ref.Generation, asset.PartID, ref.PartID)
	}
	if asset.SchemaHash != cfg.SchemaHash {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset schema_hash=%d want %d", asset.SchemaHash, cfg.SchemaHash)
	}
	if asset.AppliedCommandLSN > cfg.RecoveryAuthoritativeAppliedCommandLSN {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset applied_command_lsn=%d is newer than recovery applied_command_lsn=%d", asset.AppliedCommandLSN, cfg.RecoveryAuthoritativeAppliedCommandLSN)
	}
	if asset.AggregateName != name {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset name=%q want %q", asset.AggregateName, name)
	}
	return asset, nil
}

// buildColumnAggregateMetadataAsset supports the current insert-only metadata
// shape: every non-deleted row must have present, non-null string group values
// and int64 aggregate values. Rows counts the full input row set, including
// deleted rows, while Entries reflects only non-deleted, type-validated rows.
func buildColumnAggregateMetadataAsset(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregate ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64) (columnAggregateMetadataAsset, bool, error) {
	if aggregate.GroupColumn == "" || aggregate.Column == "" {
		return columnAggregateMetadataAsset{}, false, nil
	}
	switch aggregate.Kind {
	case ColumnAggregateMin, ColumnAggregateMax:
	default:
		return columnAggregateMetadataAsset{}, false, nil
	}
	groupIdx, valueIdx := -1, -1
	for i, col := range cfg.Columns {
		switch col.Name {
		case aggregate.GroupColumn:
			if col.ValueType != ColumnStoreValueString {
				return columnAggregateMetadataAsset{}, false, fmt.Errorf("collections: aggregate metadata %q group column %q has type %q, want %q", aggregate.Name, aggregate.GroupColumn, col.ValueType, ColumnStoreValueString)
			}
			groupIdx = i
		case aggregate.Column:
			if col.ValueType != ColumnStoreValueInt64 {
				return columnAggregateMetadataAsset{}, false, fmt.Errorf("collections: aggregate metadata %q value column %q has type %q, want %q", aggregate.Name, aggregate.Column, col.ValueType, ColumnStoreValueInt64)
			}
			valueIdx = i
		}
	}
	if groupIdx < 0 || valueIdx < 0 {
		return columnAggregateMetadataAsset{}, false, nil
	}
	entriesByGroup := make(map[string]columnAggregateMetadataEntry)
	for _, row := range rows {
		if row.Deleted {
			continue
		}
		if groupIdx >= len(row.Values) || valueIdx >= len(row.Values) {
			return columnAggregateMetadataAsset{}, false, errors.New("collections: aggregate metadata row is missing declared values")
		}
		groupValue := row.Values[groupIdx]
		valueValue := row.Values[valueIdx]
		if groupValue.Null || !groupValue.Present || valueValue.Null || !valueValue.Present {
			return columnAggregateMetadataAsset{}, false, fmt.Errorf("%w: aggregate metadata %q does not support null or missing values", ErrColumnQueryPlanUnsupported, aggregate.Name)
		}
		if groupValue.Type != ColumnStoreValueString || valueValue.Type != ColumnStoreValueInt64 {
			return columnAggregateMetadataAsset{}, false, fmt.Errorf("%w: aggregate metadata %q encountered incompatible row value types", ErrColumnQueryPlanUnsupported, aggregate.Name)
		}
		group := groupValue.String
		if group == "" && groupValue.StringBytes != nil {
			group = string(groupValue.StringBytes)
		}
		cur, ok := entriesByGroup[group]
		if !ok {
			entriesByGroup[group] = columnAggregateMetadataEntry{Group: group, Count: 1, Min: valueValue.Int64, Max: valueValue.Int64}
			continue
		}
		cur.Count++
		if valueValue.Int64 < cur.Min {
			cur.Min = valueValue.Int64
		}
		if valueValue.Int64 > cur.Max {
			cur.Max = valueValue.Int64
		}
		entriesByGroup[group] = cur
	}
	entries := make([]columnAggregateMetadataEntry, 0, len(entriesByGroup))
	for _, entry := range entriesByGroup {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Group < entries[j].Group })
	return columnAggregateMetadataAsset{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedLSN,
		SchemaHash:        cfg.SchemaHash,
		AggregateName:     aggregate.Name,
		GroupColumn:       aggregate.GroupColumn,
		ValueColumn:       aggregate.Column,
		Rows:              len(rows),
		Entries:           entries,
	}, true, nil
}
