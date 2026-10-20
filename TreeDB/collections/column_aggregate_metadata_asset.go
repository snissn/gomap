package collections

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	columnAggregateMetadataAssetMagic   = uint32(0x5443414d) // TCAM
	columnAggregateMetadataAssetVersion = uint16(4)
)

type columnAggregateMetadataEntry struct {
	Group string
	Hour  int
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
	Predicates        []ColumnPhysicalQueryPredicate
	Rows              int
	Entries           []columnAggregateMetadataEntry
}

type columnAggregateMetadataBuildSpec struct {
	aggregate         ColumnAggregateMetadata
	groupIdx          int
	valueIdx          int
	predicateSpecs    []columnAggregateMetadataPredicateSpec
	predicateCoverage []ColumnPhysicalQueryPredicate
	predicateKey      string
}

type columnAggregateMetadataEntryKey struct {
	group string
	hour  int
}

type columnAggregateMetadataAccumulatorKind uint8

const (
	columnAggregateMetadataAccumulatorCount columnAggregateMetadataAccumulatorKind = iota + 1
	columnAggregateMetadataAccumulatorGroupHourCount
	columnAggregateMetadataAccumulatorMinMax
)

type columnAggregateMetadataAccumulatorKey struct {
	kind         columnAggregateMetadataAccumulatorKind
	groupIdx     int
	valueIdx     int
	predicateKey string
}

type columnAggregateMetadataAccumulator struct {
	spec    columnAggregateMetadataBuildSpec
	entries map[columnAggregateMetadataEntryKey]columnAggregateMetadataEntry
}

func encodeColumnAggregateMetadataAsset(asset columnAggregateMetadataAsset) ([]byte, error) {
	if asset.Collection == "" || asset.Namespace == "" || asset.AggregateName == "" || asset.GroupColumn == "" {
		return nil, errors.New("collections: aggregate metadata asset requires collection, namespace, name, and group column")
	}
	if asset.Generation == 0 || asset.PartID == 0 {
		return nil, errors.New("collections: aggregate metadata asset requires generation and part_id")
	}
	if asset.Rows < 0 {
		return nil, errors.New("collections: aggregate metadata asset rows must be non-negative")
	}
	var b bytes.Buffer
	b.Grow(columnAggregateMetadataEncodedSize(asset))
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
	writeManifestUint64(&b, uint64(len(asset.Predicates)))
	for _, predicate := range asset.Predicates {
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		writeManifestString(&b, predicate.Column)
		writeManifestString(&b, string(kind))
		if kind == ColumnPhysicalQueryPredicateInList {
			writeManifestUint64(&b, uint64(len(predicate.Values)))
			for _, value := range predicate.Values {
				writeManifestString(&b, value)
			}
		} else {
			writeManifestUint64(&b, 1)
			writeManifestString(&b, predicate.Value)
		}
	}
	writeManifestUint64(&b, uint64(asset.Rows))
	writeManifestUint64(&b, uint64(len(asset.Entries)))
	for _, entry := range asset.Entries {
		if entry.Count <= 0 {
			return nil, errors.New("collections: aggregate metadata asset entry count must be positive")
		}
		if entry.Hour < 0 || entry.Hour >= 24 {
			return nil, errors.New("collections: aggregate metadata asset entry hour must be in [0, 23]")
		}
		writeManifestString(&b, entry.Group)
		writeManifestUint64(&b, uint64(entry.Hour))
		writeManifestUint64(&b, uint64(entry.Count))
		writeManifestUint64(&b, uint64(entry.Min))
		writeManifestUint64(&b, uint64(entry.Max))
	}
	return b.Bytes(), nil
}

func columnAggregateMetadataEncodedSize(asset columnAggregateMetadataAsset) int {
	size := 4 + 2
	size += manifestStringEncodedSize(asset.Collection)
	size += manifestStringEncodedSize(asset.Namespace)
	size += 4 * 8
	size += manifestStringEncodedSize(asset.AggregateName)
	size += manifestStringEncodedSize(asset.GroupColumn)
	size += manifestStringEncodedSize(asset.ValueColumn)
	size += 8
	for _, predicate := range asset.Predicates {
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		size += manifestStringEncodedSize(predicate.Column)
		size += manifestStringEncodedSize(string(kind))
		size += 8
		if kind == ColumnPhysicalQueryPredicateInList {
			for _, value := range predicate.Values {
				size += manifestStringEncodedSize(value)
			}
		} else {
			size += manifestStringEncodedSize(predicate.Value)
		}
	}
	size += 2 * 8
	for _, entry := range asset.Entries {
		size += manifestStringEncodedSize(entry.Group)
		size += 4 * 8
	}
	return size
}

func decodeColumnAggregateMetadataAsset(raw []byte, ref ColumnAssetRef, cfg ColumnStoreConfig, collection, name string) (columnAggregateMetadataAsset, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnAggregateMetadataAssetMagic {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: bad aggregate metadata asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if version == 0 || version > columnAggregateMetadataAssetVersion {
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
	if version >= 2 {
		predicateCount := cur.u64()
		if err := cur.err; err != nil {
			return columnAggregateMetadataAsset{}, err
		}
		if predicateCount > uint64(maxCollectionInt) {
			return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset predicate count overflows int")
		}
		asset.Predicates = make([]ColumnPhysicalQueryPredicate, 0, int(predicateCount))
		for i := 0; i < int(predicateCount); i++ {
			column := cur.string()
			kind := ColumnPhysicalQueryPredicateKind(cur.string())
			valueCount := cur.u64()
			if err := cur.err; err != nil {
				return columnAggregateMetadataAsset{}, err
			}
			if valueCount == 0 || valueCount > uint64(columnPhysicalQueryMaxPredicateValues) {
				return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset predicate value count is zero or too large")
			}
			values := make([]string, int(valueCount))
			for valueIdx := range values {
				values[valueIdx] = cur.string()
			}
			if err := cur.err; err != nil {
				return columnAggregateMetadataAsset{}, err
			}
			predicate := ColumnPhysicalQueryPredicate{Column: column, Kind: kind}
			if kind == ColumnPhysicalQueryPredicateInList {
				predicate.Values = values
			} else {
				if valueCount != 1 {
					return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset equality predicate has multiple values")
				}
				predicate.Value = values[0]
			}
			asset.Predicates = append(asset.Predicates, predicate)
		}
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
		hour := uint64(0)
		if version >= 4 {
			hour = cur.u64()
		}
		count := cur.u64()
		min := int64(cur.u64())
		max := int64(cur.u64())
		if err := cur.err; err != nil {
			return columnAggregateMetadataAsset{}, err
		}
		if count == 0 || count > uint64(maxCollectionInt) {
			return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset entry count overflows int or is zero")
		}
		if hour >= 24 {
			return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset entry hour is outside [0, 23]")
		}
		asset.Entries = append(asset.Entries, columnAggregateMetadataEntry{Group: group, Hour: int(hour), Count: int(count), Min: min, Max: max})
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
	if cfg.AssetManager == nil {
		return columnAggregateMetadataAsset{}, errors.New("collections: aggregate metadata asset requires column asset manager")
	}
	if asset.Namespace != cfg.AssetManager.Namespace {
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
	predicateCoverage, err := columnAggregateMetadataCanonicalPredicates(cfg, asset.Predicates)
	if err != nil {
		return columnAggregateMetadataAsset{}, fmt.Errorf("collections: aggregate metadata asset predicate coverage is invalid: %w", err)
	}
	asset.Predicates = predicateCoverage
	return asset, nil
}

// buildColumnAggregateMetadataAsset supports the current insert-only metadata
// shape: every non-deleted row must have present, non-null string group values
// and any required int64 aggregate values. Rows counts the full input row set,
// including deleted rows, while Entries reflects only non-deleted,
// type-validated rows.
func buildColumnAggregateMetadataAsset(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregate ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64) (columnAggregateMetadataAsset, bool, error) {
	spec, ok, err := newColumnAggregateMetadataBuildSpec(cfg, aggregate)
	if err != nil || !ok {
		return columnAggregateMetadataAsset{}, ok, err
	}
	metadataRows, err := columnAggregateMetadataRowsForTypedColumnGranules(cfg, aggregate, rows)
	if err != nil {
		return columnAggregateMetadataAsset{}, false, fmt.Errorf("collections: aggregate metadata %q typed-column granule rows: %w", aggregate.Name, err)
	}
	rowsPerMetadataEntrySet := len(metadataRows)
	if rowsPerMetadataEntrySet == 0 {
		rowsPerMetadataEntrySet = typedColumnDefaultRowsPerGranule()
	}
	if columnAggregateMetadataUsesTypedColumnGranules(cfg, aggregate) {
		rowsPerMetadataEntrySet = typedColumnDefaultRowsPerGranule()
	}
	entries, err := buildColumnAggregateMetadataEntriesForSpec(spec, metadataRows, rowsPerMetadataEntrySet)
	if err != nil {
		return columnAggregateMetadataAsset{}, false, err
	}
	return spec.asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedLSN, len(metadataRows), entries), true, nil
}

type columnAggregateMetadataAssetBuildOptions struct {
	TypedGranuleRowOrder []int
}

func buildColumnAggregateMetadataAssets(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregates []ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64) ([]columnAggregateMetadataAsset, error) {
	return buildColumnAggregateMetadataAssetsWithOptions(cfg, rows, aggregates, collection, namespace, generation, partID, appliedLSN, columnAggregateMetadataAssetBuildOptions{})
}

func buildColumnAggregateMetadataAssetsWithOptions(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregates []ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64, opts columnAggregateMetadataAssetBuildOptions) ([]columnAggregateMetadataAsset, error) {
	if len(aggregates) == 0 {
		return nil, nil
	}
	specs := make([]columnAggregateMetadataBuildSpec, 0, len(aggregates))
	usesTypedGranules := false
	typedGranuleModeSet := false
	for _, aggregate := range aggregates {
		spec, ok, err := newColumnAggregateMetadataBuildSpec(cfg, aggregate)
		if err != nil {
			return nil, err
		}
		if !ok {
			return buildColumnAggregateMetadataAssetsSequential(cfg, rows, aggregates, collection, namespace, generation, partID, appliedLSN)
		}
		aggregateUsesTypedGranules := columnAggregateMetadataUsesTypedColumnGranules(cfg, aggregate)
		if !typedGranuleModeSet {
			usesTypedGranules = aggregateUsesTypedGranules
			typedGranuleModeSet = true
		} else if usesTypedGranules != aggregateUsesTypedGranules {
			return buildColumnAggregateMetadataAssetsSequential(cfg, rows, aggregates, collection, namespace, generation, partID, appliedLSN)
		}
		specs = append(specs, spec)
	}
	rowsPerMetadataEntrySet := len(rows)
	metadataRows := rows
	var typedGranuleRowOrder []int
	if usesTypedGranules {
		if len(opts.TypedGranuleRowOrder) != 0 {
			if err := validateColumnAggregateMetadataTypedGranuleRowOrder(opts.TypedGranuleRowOrder, len(rows)); err != nil {
				return nil, fmt.Errorf("collections: aggregate metadata %q typed-column granule row order: %w", aggregates[0].Name, err)
			}
			typedGranuleRowOrder = opts.TypedGranuleRowOrder
		} else {
			var err error
			metadataRows, err = columnAggregateMetadataRowsForTypedColumnGranules(cfg, aggregates[0], rows)
			if err != nil {
				return nil, fmt.Errorf("collections: aggregate metadata %q typed-column granule rows: %w", aggregates[0].Name, err)
			}
		}
		rowsPerMetadataEntrySet = typedColumnDefaultRowsPerGranule()
	} else if rowsPerMetadataEntrySet == 0 {
		rowsPerMetadataEntrySet = typedColumnDefaultRowsPerGranule()
	}
	var entriesBySpec [][]columnAggregateMetadataEntry
	var err error
	if typedGranuleRowOrder != nil {
		entriesBySpec, err = buildColumnAggregateMetadataEntriesForSpecsByRowOrder(specs, rows, typedGranuleRowOrder, rowsPerMetadataEntrySet)
	} else {
		entriesBySpec, err = buildColumnAggregateMetadataEntriesForSpecs(specs, metadataRows, rowsPerMetadataEntrySet)
	}
	if err != nil {
		return nil, err
	}
	assets := make([]columnAggregateMetadataAsset, 0, len(entriesBySpec))
	for idx, entries := range entriesBySpec {
		assets = append(assets, specs[idx].asset(cfg.SchemaHash, collection, namespace, generation, partID, appliedLSN, len(metadataRows), entries))
	}
	return assets, nil
}

func validateColumnAggregateMetadataTypedGranuleRowOrder(order []int, rows int) error {
	if len(order) != rows {
		return fmt.Errorf("order rows=%d want %d", len(order), rows)
	}
	seen := make([]bool, rows)
	for partRow, rowIdx := range order {
		if rowIdx < 0 || rowIdx >= rows {
			return fmt.Errorf("order[%d]=%d outside rows=%d", partRow, rowIdx, rows)
		}
		if seen[rowIdx] {
			return fmt.Errorf("duplicate source row=%d", rowIdx)
		}
		seen[rowIdx] = true
	}
	return nil
}

func buildColumnAggregateMetadataAssetsSequential(cfg ColumnStoreConfig, rows []columnDeclaredRow, aggregates []ColumnAggregateMetadata, collection, namespace string, generation, partID, appliedLSN uint64) ([]columnAggregateMetadataAsset, error) {
	assets := make([]columnAggregateMetadataAsset, 0, len(aggregates))
	for _, aggregate := range aggregates {
		metadata, ok, err := buildColumnAggregateMetadataAsset(cfg, rows, aggregate, collection, namespace, generation, partID, appliedLSN)
		if err != nil {
			return nil, err
		}
		if ok {
			assets = append(assets, metadata)
		}
	}
	return assets, nil
}

func newColumnAggregateMetadataBuildSpec(cfg ColumnStoreConfig, aggregate ColumnAggregateMetadata) (columnAggregateMetadataBuildSpec, bool, error) {
	switch aggregate.Kind {
	case ColumnAggregateCount, ColumnAggregateGroupHourCount, ColumnAggregateMin, ColumnAggregateMax:
	default:
		return columnAggregateMetadataBuildSpec{}, false, nil
	}
	if aggregate.GroupColumn == "" {
		return columnAggregateMetadataBuildSpec{}, false, nil
	}
	if aggregate.Kind != ColumnAggregateCount && aggregate.Column == "" {
		return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q requires column", aggregate.Name)
	}
	groupIdx, valueIdx := -1, -1
	for i, col := range cfg.Columns {
		switch col.Name {
		case aggregate.GroupColumn:
			if col.ValueType != ColumnStoreValueString {
				return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q group column %q has type %q, want %q", aggregate.Name, aggregate.GroupColumn, col.ValueType, ColumnStoreValueString)
			}
			groupIdx = i
		case aggregate.Column:
			if aggregate.Kind == ColumnAggregateCount {
				continue
			}
			if col.ValueType != ColumnStoreValueInt64 {
				return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q value column %q has type %q, want %q", aggregate.Name, aggregate.Column, col.ValueType, ColumnStoreValueInt64)
			}
			valueIdx = i
		}
	}
	if groupIdx < 0 || (aggregate.Kind != ColumnAggregateCount && valueIdx < 0) {
		return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q references unknown column(s)", aggregate.Name)
	}
	predicateSpecs, err := columnAggregateMetadataPredicateSpecs(cfg, aggregate.Predicates)
	if err != nil {
		return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q predicate coverage: %w", aggregate.Name, err)
	}
	predicateCoverage, err := columnAggregateMetadataCanonicalPredicates(cfg, aggregate.Predicates)
	if err != nil {
		return columnAggregateMetadataBuildSpec{}, false, fmt.Errorf("collections: aggregate metadata %q predicate coverage: %w", aggregate.Name, err)
	}
	return columnAggregateMetadataBuildSpec{
		aggregate:         aggregate,
		groupIdx:          groupIdx,
		valueIdx:          valueIdx,
		predicateSpecs:    predicateSpecs,
		predicateCoverage: predicateCoverage,
		predicateKey:      columnAggregateMetadataPredicateAccumulatorKey(predicateCoverage),
	}, true, nil
}

func columnAggregateMetadataPredicateAccumulatorKey(predicates []ColumnPhysicalQueryPredicate) string {
	if len(predicates) == 0 {
		return ""
	}
	var b strings.Builder
	writeColumnAggregateMetadataPredicateAccumulatorKeyInt(&b, len(predicates))
	for _, predicate := range predicates {
		kind := columnPhysicalQueryPredicateKindOrDefault(predicate.Kind)
		writeColumnAggregateMetadataPredicateAccumulatorKeyString(&b, predicate.Column)
		writeColumnAggregateMetadataPredicateAccumulatorKeyString(&b, string(kind))
		if kind == ColumnPhysicalQueryPredicateInList {
			writeColumnAggregateMetadataPredicateAccumulatorKeyInt(&b, len(predicate.Values))
			for _, value := range predicate.Values {
				writeColumnAggregateMetadataPredicateAccumulatorKeyString(&b, value)
			}
		} else {
			writeColumnAggregateMetadataPredicateAccumulatorKeyInt(&b, 1)
			writeColumnAggregateMetadataPredicateAccumulatorKeyString(&b, predicate.Value)
		}
	}
	return b.String()
}

func writeColumnAggregateMetadataPredicateAccumulatorKeyInt(b *strings.Builder, value int) {
	b.WriteByte('#')
	b.WriteString(strconv.Itoa(value))
	b.WriteByte(';')
}

func writeColumnAggregateMetadataPredicateAccumulatorKeyString(b *strings.Builder, value string) {
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(':')
	b.WriteString(value)
	b.WriteByte(';')
}

func newColumnAggregateMetadataAccumulators(specs []columnAggregateMetadataBuildSpec) ([]columnAggregateMetadataAccumulator, []int) {
	accumulators := make([]columnAggregateMetadataAccumulator, 0, len(specs))
	specAccumulatorIdx := make([]int, len(specs))
	byKey := make(map[columnAggregateMetadataAccumulatorKey]int, len(specs))
	for specIdx, spec := range specs {
		key := spec.accumulatorKey()
		if accumulatorIdx, ok := byKey[key]; ok {
			specAccumulatorIdx[specIdx] = accumulatorIdx
			continue
		}
		accumulatorIdx := len(accumulators)
		byKey[key] = accumulatorIdx
		specAccumulatorIdx[specIdx] = accumulatorIdx
		accumulators = append(accumulators, columnAggregateMetadataAccumulator{
			spec:    spec,
			entries: make(map[columnAggregateMetadataEntryKey]columnAggregateMetadataEntry),
		})
	}
	return accumulators, specAccumulatorIdx
}

func (spec columnAggregateMetadataBuildSpec) accumulatorKey() columnAggregateMetadataAccumulatorKey {
	kind := columnAggregateMetadataAccumulatorMinMax
	switch spec.aggregate.Kind {
	case ColumnAggregateCount:
		kind = columnAggregateMetadataAccumulatorCount
	case ColumnAggregateGroupHourCount:
		kind = columnAggregateMetadataAccumulatorGroupHourCount
	case ColumnAggregateMin, ColumnAggregateMax:
		kind = columnAggregateMetadataAccumulatorMinMax
	}
	return columnAggregateMetadataAccumulatorKey{
		kind:         kind,
		groupIdx:     spec.groupIdx,
		valueIdx:     spec.valueIdx,
		predicateKey: spec.predicateKey,
	}
}

func buildColumnAggregateMetadataEntriesForSpecs(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, rowsPerMetadataEntrySet int) ([][]columnAggregateMetadataEntry, error) {
	if rowsPerMetadataEntrySet <= 0 {
		return nil, errors.New("collections: aggregate metadata rows per entry set must be positive")
	}
	entrySets := columnAggregateMetadataEntrySetCount(len(rows), rowsPerMetadataEntrySet)
	workers := columnAggregateMetadataEntrySetWorkerCount(entrySets)
	if workers > 1 {
		return buildColumnAggregateMetadataEntriesForSpecsParallel(specs, rows, rowsPerMetadataEntrySet, entrySets, workers)
	}
	entriesBySpec := make([][]columnAggregateMetadataEntry, len(specs))
	for start := 0; start < len(rows); start += rowsPerMetadataEntrySet {
		end := start + rowsPerMetadataEntrySet
		if end > len(rows) {
			end = len(rows)
		}
		entrySet, err := buildColumnAggregateMetadataEntrySetForSpecs(specs, rows[start:end])
		if err != nil {
			return nil, err
		}
		for idx := range specs {
			entriesBySpec[idx] = append(entriesBySpec[idx], entrySet[idx]...)
		}
	}
	return entriesBySpec, nil
}

func buildColumnAggregateMetadataEntriesForSpecsByRowOrder(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, order []int, rowsPerMetadataEntrySet int) ([][]columnAggregateMetadataEntry, error) {
	if rowsPerMetadataEntrySet <= 0 {
		return nil, errors.New("collections: aggregate metadata rows per entry set must be positive")
	}
	entrySets := columnAggregateMetadataEntrySetCount(len(order), rowsPerMetadataEntrySet)
	workers := columnAggregateMetadataEntrySetWorkerCount(entrySets)
	if workers > 1 {
		return buildColumnAggregateMetadataEntriesForSpecsByRowOrderParallel(specs, rows, order, rowsPerMetadataEntrySet, entrySets, workers)
	}
	entriesBySpec := make([][]columnAggregateMetadataEntry, len(specs))
	for start := 0; start < len(order); start += rowsPerMetadataEntrySet {
		end := start + rowsPerMetadataEntrySet
		if end > len(order) {
			end = len(order)
		}
		entrySet, err := buildColumnAggregateMetadataEntrySetForSpecsByRowOrder(specs, rows, order[start:end])
		if err != nil {
			return nil, err
		}
		for idx := range specs {
			entriesBySpec[idx] = append(entriesBySpec[idx], entrySet[idx]...)
		}
	}
	return entriesBySpec, nil
}

type columnAggregateMetadataEntrySetResult struct {
	entriesBySpec [][]columnAggregateMetadataEntry
	err           error
}

func columnAggregateMetadataEntrySetCount(rows, rowsPerMetadataEntrySet int) int {
	if rows <= 0 || rowsPerMetadataEntrySet <= 0 {
		return 0
	}
	return (rows + rowsPerMetadataEntrySet - 1) / rowsPerMetadataEntrySet
}

func columnAggregateMetadataEntrySetWorkerCount(entrySets int) int {
	if entrySets < 2 {
		return 1
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8
	}
	if workers > entrySets {
		workers = entrySets
	}
	if workers < 1 {
		return 1
	}
	return workers
}

func columnAggregateMetadataEntrySetRange(setIdx, rows, rowsPerMetadataEntrySet int) (int, int) {
	start := setIdx * rowsPerMetadataEntrySet
	end := start + rowsPerMetadataEntrySet
	if end > rows {
		end = rows
	}
	return start, end
}

func buildColumnAggregateMetadataEntriesForSpecsParallel(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, rowsPerMetadataEntrySet, entrySets, workers int) ([][]columnAggregateMetadataEntry, error) {
	results := make([]columnAggregateMetadataEntrySetResult, entrySets)
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for workerIdx := 0; workerIdx < workers; workerIdx++ {
		go func() {
			defer wg.Done()
			for setIdx := range jobs {
				start, end := columnAggregateMetadataEntrySetRange(setIdx, len(rows), rowsPerMetadataEntrySet)
				results[setIdx].entriesBySpec, results[setIdx].err = buildColumnAggregateMetadataEntrySetForSpecs(specs, rows[start:end])
			}
		}()
	}
	for setIdx := 0; setIdx < entrySets; setIdx++ {
		jobs <- setIdx
	}
	close(jobs)
	wg.Wait()
	return mergeColumnAggregateMetadataEntrySetResults(specs, results)
}

func buildColumnAggregateMetadataEntriesForSpecsByRowOrderParallel(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, order []int, rowsPerMetadataEntrySet, entrySets, workers int) ([][]columnAggregateMetadataEntry, error) {
	results := make([]columnAggregateMetadataEntrySetResult, entrySets)
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for workerIdx := 0; workerIdx < workers; workerIdx++ {
		go func() {
			defer wg.Done()
			for setIdx := range jobs {
				start, end := columnAggregateMetadataEntrySetRange(setIdx, len(order), rowsPerMetadataEntrySet)
				results[setIdx].entriesBySpec, results[setIdx].err = buildColumnAggregateMetadataEntrySetForSpecsByRowOrder(specs, rows, order[start:end])
			}
		}()
	}
	for setIdx := 0; setIdx < entrySets; setIdx++ {
		jobs <- setIdx
	}
	close(jobs)
	wg.Wait()
	return mergeColumnAggregateMetadataEntrySetResults(specs, results)
}

func mergeColumnAggregateMetadataEntrySetResults(specs []columnAggregateMetadataBuildSpec, results []columnAggregateMetadataEntrySetResult) ([][]columnAggregateMetadataEntry, error) {
	entriesBySpec := make([][]columnAggregateMetadataEntry, len(specs))
	for setIdx := range results {
		if results[setIdx].err != nil {
			return nil, results[setIdx].err
		}
		for specIdx := range specs {
			entriesBySpec[specIdx] = append(entriesBySpec[specIdx], results[setIdx].entriesBySpec[specIdx]...)
		}
	}
	return entriesBySpec, nil
}

func buildColumnAggregateMetadataEntrySetForSpecs(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow) ([][]columnAggregateMetadataEntry, error) {
	accumulators, specAccumulatorIdx := newColumnAggregateMetadataAccumulators(specs)
	for _, row := range rows {
		if row.Deleted {
			continue
		}
		for idx := range accumulators {
			if err := accumulators[idx].spec.appendRow(accumulators[idx].entries, row); err != nil {
				return nil, err
			}
		}
	}
	return sortedColumnAggregateMetadataEntriesBySpec(specs, accumulators, specAccumulatorIdx), nil
}

func buildColumnAggregateMetadataEntrySetForSpecsByRowOrder(specs []columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, order []int) ([][]columnAggregateMetadataEntry, error) {
	accumulators, specAccumulatorIdx := newColumnAggregateMetadataAccumulators(specs)
	for _, rowIdx := range order {
		row := rows[rowIdx]
		if row.Deleted {
			continue
		}
		for idx := range accumulators {
			if err := accumulators[idx].spec.appendRow(accumulators[idx].entries, row); err != nil {
				return nil, err
			}
		}
	}
	return sortedColumnAggregateMetadataEntriesBySpec(specs, accumulators, specAccumulatorIdx), nil
}

func sortedColumnAggregateMetadataEntriesBySpec(specs []columnAggregateMetadataBuildSpec, accumulators []columnAggregateMetadataAccumulator, specAccumulatorIdx []int) [][]columnAggregateMetadataEntry {
	entriesByAccumulator := make([][]columnAggregateMetadataEntry, len(accumulators))
	for idx := range accumulators {
		entriesByAccumulator[idx] = sortedColumnAggregateMetadataEntries(accumulators[idx].entries)
	}
	entriesBySpec := make([][]columnAggregateMetadataEntry, len(specs))
	for idx := range specs {
		entriesBySpec[idx] = entriesByAccumulator[specAccumulatorIdx[idx]]
	}
	return entriesBySpec
}

func buildColumnAggregateMetadataEntriesForSpec(spec columnAggregateMetadataBuildSpec, rows []columnDeclaredRow, rowsPerMetadataEntrySet int) ([]columnAggregateMetadataEntry, error) {
	if rowsPerMetadataEntrySet <= 0 {
		return nil, errors.New("collections: aggregate metadata rows per entry set must be positive")
	}
	entries := make([]columnAggregateMetadataEntry, 0)
	for start := 0; start < len(rows); start += rowsPerMetadataEntrySet {
		end := start + rowsPerMetadataEntrySet
		if end > len(rows) {
			end = len(rows)
		}
		entriesByKey := make(map[columnAggregateMetadataEntryKey]columnAggregateMetadataEntry)
		for _, row := range rows[start:end] {
			if row.Deleted {
				continue
			}
			if err := spec.appendRow(entriesByKey, row); err != nil {
				return nil, err
			}
		}
		entries = append(entries, sortedColumnAggregateMetadataEntries(entriesByKey)...)
	}
	return entries, nil
}

func (spec columnAggregateMetadataBuildSpec) appendRow(entriesByKey map[columnAggregateMetadataEntryKey]columnAggregateMetadataEntry, row columnDeclaredRow) error {
	aggregate := spec.aggregate
	if spec.groupIdx >= len(row.Values) || (aggregate.Kind != ColumnAggregateCount && spec.valueIdx >= len(row.Values)) {
		return errors.New("collections: aggregate metadata row is missing declared values")
	}
	matched, err := columnAggregateMetadataPredicatesMatchRow(spec.predicateSpecs, row.Values)
	if err != nil {
		return fmt.Errorf("collections: aggregate metadata %q predicate evaluation: %w", aggregate.Name, err)
	}
	if !matched {
		return nil
	}
	groupValue := row.Values[spec.groupIdx]
	if groupValue.Null || !groupValue.Present {
		groupValue = columnDeclaredValue{Type: ColumnStoreValueString, Present: true, String: ""}
	}
	if groupValue.Type != ColumnStoreValueString {
		return fmt.Errorf("%w: aggregate metadata %q encountered incompatible row value types", ErrColumnQueryPlanUnsupported, aggregate.Name)
	}
	group := groupValue.String
	if group == "" && groupValue.StringBytes != nil {
		group = string(groupValue.StringBytes)
	}
	key := columnAggregateMetadataEntryKey{group: group}
	if aggregate.Kind == ColumnAggregateCount {
		cur := entriesByKey[key]
		cur.Group = group
		cur.Count++
		entriesByKey[key] = cur
		return nil
	}
	valueValue := row.Values[spec.valueIdx]
	if valueValue.Null || !valueValue.Present {
		return fmt.Errorf("%w: aggregate metadata %q does not support null or missing values", ErrColumnQueryPlanUnsupported, aggregate.Name)
	}
	if valueValue.Type != ColumnStoreValueInt64 {
		return fmt.Errorf("%w: aggregate metadata %q encountered incompatible row value types", ErrColumnQueryPlanUnsupported, aggregate.Name)
	}
	if aggregate.Kind == ColumnAggregateGroupHourCount {
		key.hour = columnPhysicalQueryUTCHour(valueValue.Int64)
		cur := entriesByKey[key]
		cur.Group = group
		cur.Hour = key.hour
		cur.Count++
		entriesByKey[key] = cur
		return nil
	}
	cur, ok := entriesByKey[key]
	if !ok {
		entriesByKey[key] = columnAggregateMetadataEntry{Group: group, Count: 1, Min: valueValue.Int64, Max: valueValue.Int64}
		return nil
	}
	cur.Count++
	if valueValue.Int64 < cur.Min {
		cur.Min = valueValue.Int64
	}
	if valueValue.Int64 > cur.Max {
		cur.Max = valueValue.Int64
	}
	entriesByKey[key] = cur
	return nil
}

func sortedColumnAggregateMetadataEntries(entriesByKey map[columnAggregateMetadataEntryKey]columnAggregateMetadataEntry) []columnAggregateMetadataEntry {
	entries := make([]columnAggregateMetadataEntry, 0, len(entriesByKey))
	for _, entry := range entriesByKey {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Group != entries[j].Group {
			return entries[i].Group < entries[j].Group
		}
		return entries[i].Hour < entries[j].Hour
	})
	return entries
}

func (spec columnAggregateMetadataBuildSpec) asset(schemaHash uint64, collection, namespace string, generation, partID, appliedLSN uint64, rows int, entries []columnAggregateMetadataEntry) columnAggregateMetadataAsset {
	return columnAggregateMetadataAsset{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedLSN,
		SchemaHash:        schemaHash,
		AggregateName:     spec.aggregate.Name,
		GroupColumn:       spec.aggregate.GroupColumn,
		ValueColumn:       spec.aggregate.Column,
		Predicates:        spec.predicateCoverage,
		Rows:              rows,
		Entries:           entries,
	}
}

func columnAggregateMetadataUsesTypedColumnGranules(cfg ColumnStoreConfig, aggregate ColumnAggregateMetadata) bool {
	columns := make([]string, 0, 2+len(aggregate.Predicates))
	if aggregate.Column != "" {
		columns = append(columns, aggregate.Column)
	}
	if aggregate.GroupColumn != "" {
		columns = append(columns, aggregate.GroupColumn)
	}
	for _, predicate := range aggregate.Predicates {
		columns = append(columns, predicate.Column)
	}
	for _, name := range columns {
		col, _, ok := columnPhysicalQueryDeclaredColumn(cfg, name)
		if !ok || !columnStoreColumnIsTypedColumnPart(col) {
			return false
		}
	}
	return len(columns) > 0
}

type columnAggregateMetadataSortRow struct {
	row    columnDeclaredRow
	index  int
	values []columnDeclaredValue
}

func columnAggregateMetadataRowsForTypedColumnGranules(cfg ColumnStoreConfig, aggregate ColumnAggregateMetadata, rows []columnDeclaredRow) ([]columnDeclaredRow, error) {
	if !columnAggregateMetadataUsesTypedColumnGranules(cfg, aggregate) || len(rows) <= 1 {
		return rows, nil
	}
	sortKey, err := typedColumnPartPublicationSortKey(cfg, columnStoreTypedColumnPartFields(cfg))
	if err != nil {
		return nil, err
	}
	if len(sortKey) == 0 {
		return rows, nil
	}
	columnIdxByName := make(map[string]int, len(cfg.Columns))
	columnByName := make(map[string]ColumnStoreColumn, len(cfg.Columns))
	for idx, col := range cfg.Columns {
		columnIdxByName[col.Name] = idx
		columnByName[col.Name] = col
	}
	sortRows := make([]columnAggregateMetadataSortRow, len(rows))
	for rowIdx, row := range rows {
		sortRows[rowIdx] = columnAggregateMetadataSortRow{row: row, index: rowIdx, values: make([]columnDeclaredValue, len(sortKey))}
		if len(row.Values) != len(cfg.Columns) {
			return nil, fmt.Errorf("row[%d] values=%d columns=%d", rowIdx, len(row.Values), len(cfg.Columns))
		}
		for keyIdx, key := range sortKey {
			columnIdx, ok := columnIdxByName[key.Column]
			if !ok {
				return nil, fmt.Errorf("sort key references unknown column %q", key.Column)
			}
			value := row.Values[columnIdx]
			if err := columnAggregateMetadataValidateSortValue(columnByName[key.Column], value); err != nil {
				return nil, fmt.Errorf("row[%d] sort key column %q: %w", rowIdx, key.Column, err)
			}
			sortRows[rowIdx].values[keyIdx] = value
		}
	}
	sort.SliceStable(sortRows, func(i, j int) bool {
		for keyIdx, key := range sortKey {
			cmp := columnAggregateMetadataCompareSortValues(columnByName[key.Column], sortRows[i].values[keyIdx], sortRows[j].values[keyIdx])
			if cmp != 0 {
				return cmp < 0
			}
		}
		return sortRows[i].index < sortRows[j].index
	})
	out := make([]columnDeclaredRow, len(rows))
	for idx, sortRow := range sortRows {
		out[idx] = sortRow.row
	}
	return out, nil
}

func columnAggregateMetadataValidateSortValue(col ColumnStoreColumn, value columnDeclaredValue) error {
	if value.Null || !value.Present {
		return fmt.Errorf("null/default ordering is not defined for typed-column aggregate metadata")
	}
	if value.Type != col.ValueType {
		return fmt.Errorf("value type %q does not match column type %q", value.Type, col.ValueType)
	}
	switch col.ValueType {
	case ColumnStoreValueBool, ColumnStoreValueInt64, ColumnStoreValueString:
		return nil
	default:
		return fmt.Errorf("value_type %q is not supported for typed-column aggregate metadata sort ordering", col.ValueType)
	}
}

func columnAggregateMetadataCompareSortValues(col ColumnStoreColumn, left, right columnDeclaredValue) int {
	switch col.ValueType {
	case ColumnStoreValueBool:
		if left.Bool == right.Bool {
			return 0
		}
		if !left.Bool {
			return -1
		}
		return 1
	case ColumnStoreValueInt64:
		if left.Int64 < right.Int64 {
			return -1
		}
		if left.Int64 > right.Int64 {
			return 1
		}
		return 0
	case ColumnStoreValueString:
		leftString := left.String
		if leftString == "" && left.StringBytes != nil {
			leftString = string(left.StringBytes)
		}
		rightString := right.String
		if rightString == "" && right.StringBytes != nil {
			rightString = string(right.StringBytes)
		}
		if leftString < rightString {
			return -1
		}
		if leftString > rightString {
			return 1
		}
	}
	return 0
}
