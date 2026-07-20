package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
)

const (
	// Manifest record keys keep their v1 keyspace names for stable ordering and
	// prefix scans; columnManifestRecordVersion versions the binary values.
	columnManifestHeaderRecordKey               = "\x01column-manifest/v1/header"
	columnManifestPartRecordPrefix              = "\x02column-manifest/v1/part/"
	columnManifestAggregateMetadataRecordPrefix = "\x03column-manifest/v1/aggregate/"
	columnManifestDictionaryCodesRecordPrefix   = "\x04column-manifest/v1/dictionary/"
	columnManifestInt64ValuesRecordPrefix       = "\x05column-manifest/v1/int64/"

	columnManifestHeaderMagic       = uint32(0x54434d48) // TCMH
	columnManifestPartMagic         = uint32(0x54434d50) // TCMP
	columnManifestRecordVersionV1   = uint16(1)
	columnManifestRecordVersionV2   = uint16(2)
	columnManifestRecordVersionV3   = uint16(3)
	columnManifestRecordVersionV4   = uint16(4)
	columnManifestRecordVersion     = columnManifestRecordVersionV4
	columnManifestSortKeyMaxColumns = uint64(64)
)

var (
	// Treat these shared key sentinels as read-only.
	columnManifestHeaderRecordKeyBytes               = []byte(columnManifestHeaderRecordKey)
	columnManifestPartRecordPrefixBytes              = []byte(columnManifestPartRecordPrefix)
	columnManifestAggregateMetadataRecordPrefixBytes = []byte(columnManifestAggregateMetadataRecordPrefix)
	columnManifestDictionaryCodesRecordPrefixBytes   = []byte(columnManifestDictionaryCodesRecordPrefix)
	columnManifestInt64ValuesRecordPrefixBytes       = []byte(columnManifestInt64ValuesRecordPrefix)
)

type columnManifestRecord struct {
	key   []byte
	value []byte
}

// columnManifestMutation is one root-local change between two complete logical
// manifest snapshots. A deleted mutation carries only the removed record key.
// The complete post-state remains available separately for checksum and
// correctness validation.
type columnManifestMutation struct {
	record  columnManifestRecord
	deleted bool
}

type columnManifestSnapshot struct {
	Collection         string
	Operation          ColumnPublishOperation
	Generation         uint64
	AppliedCommandLSN  uint64
	SchemaHash         uint64
	ExpectedParts      uint64
	RowCount           int
	CommandBytes       int64
	RowRemainderBytes  int64
	ColumnPayloadBytes int64
	ManifestBytes      int64
	Parts              []columnManifestPartSnapshot
	AggregateMetadata  []columnManifestAggregateMetadataSnapshot
	DictionaryCodes    []columnManifestDictionaryCodesSnapshot
	Int64Values        []columnManifestInt64ValuesSnapshot
}

type columnManifestPartSnapshot struct {
	AssetRef     ColumnAssetRef
	Rows         int
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
	Reason       string
	PartRole     ColumnManifestPartRole
	SortKey      []ColumnSortKey
}

type columnManifestAggregateMetadataSnapshot struct {
	AssetRef     ColumnAssetRef
	Name         string
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
}

type columnManifestDictionaryCodesSnapshot struct {
	AssetRef     ColumnAssetRef
	ColumnName   string
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
}

type columnManifestInt64ValuesSnapshot struct {
	AssetRef     ColumnAssetRef
	ColumnName   string
	Rows         int
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
}

func columnManifestPartRecordKey(generation, partID uint64) []byte {
	key := make([]byte, len(columnManifestPartRecordPrefix)+16)
	copy(key, columnManifestPartRecordPrefix)
	binary.BigEndian.PutUint64(key[len(columnManifestPartRecordPrefix):], generation)
	binary.BigEndian.PutUint64(key[len(columnManifestPartRecordPrefix)+8:], partID)
	return key
}

func columnManifestAggregateMetadataRecordKey(generation, partID uint64, name string) []byte {
	key := make([]byte, len(columnManifestAggregateMetadataRecordPrefix)+16+len(name))
	copy(key, columnManifestAggregateMetadataRecordPrefix)
	binary.BigEndian.PutUint64(key[len(columnManifestAggregateMetadataRecordPrefix):], generation)
	binary.BigEndian.PutUint64(key[len(columnManifestAggregateMetadataRecordPrefix)+8:], partID)
	copy(key[len(columnManifestAggregateMetadataRecordPrefix)+16:], name)
	return key
}

func columnManifestDictionaryCodesRecordKey(generation, partID uint64, columnName string) []byte {
	key := make([]byte, len(columnManifestDictionaryCodesRecordPrefix)+16+len(columnName))
	copy(key, columnManifestDictionaryCodesRecordPrefix)
	binary.BigEndian.PutUint64(key[len(columnManifestDictionaryCodesRecordPrefix):], generation)
	binary.BigEndian.PutUint64(key[len(columnManifestDictionaryCodesRecordPrefix)+8:], partID)
	copy(key[len(columnManifestDictionaryCodesRecordPrefix)+16:], columnName)
	return key
}

func columnManifestInt64ValuesRecordKey(generation, partID uint64, columnName string) []byte {
	key := make([]byte, len(columnManifestInt64ValuesRecordPrefix)+16+len(columnName))
	copy(key, columnManifestInt64ValuesRecordPrefix)
	binary.BigEndian.PutUint64(key[len(columnManifestInt64ValuesRecordPrefix):], generation)
	binary.BigEndian.PutUint64(key[len(columnManifestInt64ValuesRecordPrefix)+8:], partID)
	copy(key[len(columnManifestInt64ValuesRecordPrefix)+16:], columnName)
	return key
}

func encodeColumnManifestForWrite(input ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
	if err := validateColumnPublishPreparedAssets(input.Prepared); err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	generation := uint64(1)
	if input.CurrentManifest != nil {
		generation = input.CurrentManifest.Generation + 1
	}
	if len(input.Prepared.Assets) != 0 {
		generation = input.Prepared.Assets[0].Ref.Generation
		if input.CurrentManifest != nil && generation <= input.CurrentManifest.Generation {
			return ColumnPublishManifestEncodeResult{}, fmt.Errorf("collections: column manifest generation=%d must advance current generation=%d", generation, input.CurrentManifest.Generation)
		}
		for i, asset := range input.Prepared.Assets {
			if asset.Ref.Generation != generation {
				return ColumnPublishManifestEncodeResult{}, fmt.Errorf("collections: column manifest asset[%d] generation=%d does not match manifest generation=%d", i, asset.Ref.Generation, generation)
			}
		}
	}

	records := make([]columnManifestRecord, 0, 1+len(input.CurrentManifestRecords)+len(input.Prepared.Assets))
	header, err := encodeColumnManifestHeaderRecord(input, generation)
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	records = append(records, columnManifestRecord{
		key:   []byte(columnManifestHeaderRecordKey),
		value: header,
	})
	retained, err := retainedColumnManifestRecordsForWrite(input.CurrentManifestRecords, generation, input.ActiveVectorIndexesKnown, input.ActiveVectorIndexes)
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	records = append(records, retained...)
	for _, asset := range input.Prepared.Assets {
		partValue, err := encodeColumnManifestPartRecord(asset)
		if err != nil {
			return ColumnPublishManifestEncodeResult{}, err
		}
		switch asset.Ref.Kind {
		case ColumnAssetKindTCS1AggregateMetadata:
			if asset.Reason == "" {
				return ColumnPublishManifestEncodeResult{}, errors.New("collections: aggregate metadata manifest record requires aggregate name")
			}
			records = append(records, columnManifestRecord{
				key:   columnManifestAggregateMetadataRecordKey(asset.Ref.Generation, asset.Ref.PartID, asset.Reason),
				value: partValue,
			})
			continue
		case ColumnAssetKindTCS1DictionaryCodes:
			if asset.Reason == "" {
				return ColumnPublishManifestEncodeResult{}, errors.New("collections: dictionary codes manifest record requires column name")
			}
			records = append(records, columnManifestRecord{
				key:   columnManifestDictionaryCodesRecordKey(asset.Ref.Generation, asset.Ref.PartID, asset.Reason),
				value: partValue,
			})
			continue
		case ColumnAssetKindTCS1Int64Values:
			if asset.Reason == "" {
				return ColumnPublishManifestEncodeResult{}, errors.New("collections: int64 values manifest record requires column name")
			}
			records = append(records, columnManifestRecord{
				key:   columnManifestInt64ValuesRecordKey(asset.Ref.Generation, asset.Ref.PartID, asset.Reason),
				value: partValue,
			})
			continue
		}
		records = append(records, columnManifestRecord{
			key:   columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID),
			value: partValue,
		})
	}
	sortColumnManifestRecords(records)
	checksum := checksumColumnManifestRecords(input, generation, records)
	identity := ColumnManifestIdentity{
		Generation: generation,
		Format:     columnManifestFormatTCS1,
		Version:    columnManifestIdentityVersion,
		Checksum:   checksum,
	}
	return ColumnPublishManifestEncodeResult{
		Identity:      identity,
		ManifestBytes: columnManifestRecordsBytes(records),
		Records:       records,
	}, nil
}

// buildColumnManifestMutationDelta returns the minimal sorted mutation stream
// that transforms current into next. Both inputs must be strictly sorted by
// unique key; this is also the ordering required by ordered-root publication.
func buildColumnManifestMutationDelta(current, next []columnManifestRecord) ([]columnManifestMutation, error) {
	if err := validateSortedUniqueColumnManifestRecords(current, "current"); err != nil {
		return nil, err
	}
	if err := validateSortedUniqueColumnManifestRecords(next, "next"); err != nil {
		return nil, err
	}
	mutationCapacity := len(current) + len(next)
	if mutationCapacity > 8 {
		mutationCapacity = 8
	}
	mutations := make([]columnManifestMutation, 0, mutationCapacity)
	for currentIndex, nextIndex := 0, 0; currentIndex < len(current) || nextIndex < len(next); {
		if currentIndex == len(current) {
			mutations = append(mutations, cloneColumnManifestMutation(next[nextIndex], false))
			nextIndex++
			continue
		}
		if nextIndex == len(next) {
			mutations = append(mutations, cloneColumnManifestMutation(current[currentIndex], true))
			currentIndex++
			continue
		}
		switch comparison := bytes.Compare(current[currentIndex].key, next[nextIndex].key); {
		case comparison < 0:
			mutations = append(mutations, cloneColumnManifestMutation(current[currentIndex], true))
			currentIndex++
		case comparison > 0:
			mutations = append(mutations, cloneColumnManifestMutation(next[nextIndex], false))
			nextIndex++
		default:
			if !bytes.Equal(current[currentIndex].value, next[nextIndex].value) {
				mutations = append(mutations, cloneColumnManifestMutation(next[nextIndex], false))
			}
			currentIndex++
			nextIndex++
		}
	}
	return mutations, nil
}

func validateSortedUniqueColumnManifestRecords(records []columnManifestRecord, name string) error {
	for i, record := range records {
		if len(record.key) == 0 {
			return fmt.Errorf("collections: %s column manifest record[%d] has empty key", name, i)
		}
		if i > 0 && bytes.Compare(records[i-1].key, record.key) >= 0 {
			return fmt.Errorf("collections: %s column manifest records are not strictly sorted at index %d", name, i)
		}
	}
	return nil
}

func cloneColumnManifestMutation(record columnManifestRecord, deleted bool) columnManifestMutation {
	mutation := columnManifestMutation{
		record:  columnManifestRecord{key: bytes.Clone(record.key)},
		deleted: deleted,
	}
	if !deleted {
		mutation.record.value = bytes.Clone(record.value)
	}
	return mutation
}

func columnManifestMutationsEqual(left, right []columnManifestMutation) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].deleted != right[i].deleted ||
			!bytes.Equal(left[i].record.key, right[i].record.key) ||
			!bytes.Equal(left[i].record.value, right[i].record.value) {
			return false
		}
	}
	return true
}

func columnManifestMutationBytes(mutations []columnManifestMutation) int64 {
	var total int64
	for _, mutation := range mutations {
		total += int64(len(mutation.record.key))
		if !mutation.deleted {
			total += int64(len(mutation.record.value))
		}
	}
	return total
}

func columnManifestRootMutationRecordCount(mutations []columnManifestMutation) int {
	return 1 + len(mutations) // independently stored manifest identity record
}

func columnManifestRootMutationBytes(mutations []columnManifestMutation) int64 {
	return int64(len(columnManifestIdentityRecordKey)+columnManifestIdentityRecordSize) + columnManifestMutationBytes(mutations)
}

func retainedColumnManifestRecordsForWrite(records []columnManifestRecord, generation uint64, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) ([]columnManifestRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	retained := make([]columnManifestRecord, 0, len(records))
	for _, record := range records {
		if bytes.HasPrefix(record.key, columnManifestVectorGraphRecordPrefixBytes) {
			if !retainColumnManifestVectorGraphRecordForWrite(record.key, activeVectorIndexesKnown, activeVectorIndexes) {
				continue
			}
			retain, err := validateRetainedColumnManifestVectorGraphRecordForWrite(record, generation)
			if err != nil {
				return nil, err
			}
			if !retain {
				continue
			}
			retained = append(retained, columnManifestRecord{
				key:   bytes.Clone(record.key),
				value: bytes.Clone(record.value),
			})
			continue
		}
		if bytes.HasPrefix(record.key, columnVectorIndexStateRecordPrefixBytes) {
			if !retainColumnVectorIndexStateRecordForWrite(record.key, activeVectorIndexesKnown, activeVectorIndexes) {
				continue
			}
			retain, err := validateRetainedColumnVectorIndexStateRecordForWrite(record, generation)
			if err != nil {
				return nil, err
			}
			if !retain {
				continue
			}
			retained = append(retained, columnManifestRecord{
				key:   bytes.Clone(record.key),
				value: bytes.Clone(record.value),
			})
			continue
		}
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes) &&
			!bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			return nil, err
		}
		if part.AssetRef.Generation >= generation {
			continue
		}
		retained = append(retained, columnManifestRecord{
			key:   bytes.Clone(record.key),
			value: bytes.Clone(record.value),
		})
	}
	return retained, nil
}

func validateRetainedColumnManifestVectorGraphRecordForWrite(record columnManifestRecord, generation uint64) (bool, error) {
	graph, err := decodeColumnVectorGraphManifestRecord(record.value)
	if err != nil {
		return false, err
	}
	keyIndexName := string(record.key[len(columnManifestVectorGraphRecordPrefixBytes):])
	if graph.IndexName != keyIndexName {
		return false, fmt.Errorf("collections: column vector graph manifest key index=%q does not match record index=%q", keyIndexName, graph.IndexName)
	}
	if !columnVectorGraphManifestHasPhysicalAsset(graph) {
		return graph.BaseManifestGeneration < generation, nil
	}
	if graph.AssetRef.Kind != ColumnAssetKindTCS1PartImage {
		return false, fmt.Errorf("collections: column vector graph asset kind=%q want %q", graph.AssetRef.Kind, ColumnAssetKindTCS1PartImage)
	}
	if graph.BaseManifestGeneration != graph.AssetRef.Generation {
		return false, fmt.Errorf("collections: column vector graph base manifest generation=%d does not match asset generation=%d", graph.BaseManifestGeneration, graph.AssetRef.Generation)
	}
	if graph.AssetRef.Generation >= generation {
		return false, nil
	}
	return true, nil
}

func retainColumnManifestVectorGraphRecordForWrite(key []byte, activeVectorIndexesKnown bool, activeVectorIndexes []VectorIndexDefinition) bool {
	if !activeVectorIndexesKnown {
		return true
	}
	if !bytes.HasPrefix(key, columnManifestVectorGraphRecordPrefixBytes) {
		return false
	}
	indexName := key[len(columnManifestVectorGraphRecordPrefixBytes):]
	for _, def := range activeVectorIndexes {
		if def.Strategy == VectorIndexStrategyColumnGraph && columnManifestBytesEqualString(indexName, def.Name) {
			return true
		}
	}
	return false
}

func columnManifestBytesEqualString(left []byte, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i, b := range left {
		if b != right[i] {
			return false
		}
	}
	return true
}

func encodeColumnManifestHeaderRecord(input ColumnPublishManifestEncodeInput, generation uint64) ([]byte, error) {
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestHeaderMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, input.Collection)
	writeManifestString(&b, string(input.Operation))
	writeManifestUint64(&b, generation)
	writeManifestUint64(&b, input.AppliedCommandLSN)
	writeManifestUint64(&b, input.ColumnStore.SchemaHash)
	writeManifestUint64(&b, uint64(input.Prepared.RowCount))
	writeManifestUint64(&b, uint64(input.Prepared.CommandBytes))
	writeManifestUint64(&b, uint64(input.Prepared.RowRemainderBytes))
	writeManifestUint64(&b, uint64(input.Prepared.ColumnPayloadBytes))
	writeManifestUint64(&b, uint64(columnManifestPreparedPartCount(input.Prepared.Assets)))
	return b.Bytes(), nil
}

func columnManifestPreparedPartCount(assets []ColumnPreparedAsset) int {
	count := 0
	for _, asset := range assets {
		if asset.Ref.Kind == ColumnAssetKindTCS1PartImage || asset.Ref.Kind == ColumnAssetKindTCS1TypedColumnPart {
			count++
		}
	}
	return count
}

func encodeColumnManifestPartRecord(asset ColumnPreparedAsset) ([]byte, error) {
	if asset.PartRole == "" {
		asset.PartRole = inferColumnManifestPartRole(asset.Ref.Kind, asset.Reason)
	}
	if err := validateColumnPreparedAssetForPlan(asset); err != nil {
		return nil, err
	}
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestPartMagic)
	writeManifestUint16(&b, columnManifestRecordVersion)
	writeManifestString(&b, string(asset.Ref.Kind))
	writeManifestString(&b, asset.Ref.Namespace)
	writeManifestUint64(&b, asset.Ref.Generation)
	writeManifestUint64(&b, asset.Ref.PartID)
	writeManifestUint64(&b, uint64(asset.Ref.FileID))
	writeManifestUint64(&b, uint64(asset.Ref.Offset))
	writeManifestUint64(&b, uint64(asset.Ref.Length))
	writeManifestUint64(&b, uint64(asset.Ref.Checksum))
	writeManifestUint64(&b, uint64(asset.Rows))
	writeManifestUint64(&b, uint64(asset.Bytes))
	writeManifestUint64(&b, asset.PublishID)
	writeManifestUint64(&b, asset.GenerationID)
	writeManifestString(&b, asset.Reason)
	writeManifestString(&b, string(asset.PartRole))
	sortKey, err := columnSortKeysFromMatchString(asset.SortKey)
	if err != nil {
		return nil, err
	}
	if uint64(len(sortKey)) > columnManifestSortKeyMaxColumns {
		return nil, fmt.Errorf("collections: column manifest sort key columns=%d exceeds cap %d", len(sortKey), columnManifestSortKeyMaxColumns)
	}
	writeColumnManifestSortKey(&b, sortKey)
	return b.Bytes(), nil
}

func decodeColumnManifestRecords(records []columnManifestRecord) (columnManifestSnapshot, error) {
	var snapshot columnManifestSnapshot
	sawHeader := false
	partRecords := 0
	metadataRecords := 0
	dictionaryRecords := 0
	int64ValueRecords := 0
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, columnManifestHeaderRecordKeyBytes):
			if sawHeader {
				return columnManifestSnapshot{}, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecord(record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			snapshot = header
			sawHeader = true
		case bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes):
			partRecords++
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			metadataRecords++
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			dictionaryRecords++
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			int64ValueRecords++
		}
	}
	if !sawHeader {
		return columnManifestSnapshot{}, errors.New("collections: column manifest missing binary header record")
	}

	partCap := partRecords
	if snapshot.ExpectedParts < uint64(partCap) {
		partCap = int(snapshot.ExpectedParts)
	}
	if partCap > 0 {
		snapshot.Parts = make([]columnManifestPartSnapshot, 0, partCap)
	}
	if metadataRecords > 0 {
		snapshot.AggregateMetadata = make([]columnManifestAggregateMetadataSnapshot, 0, metadataRecords)
	}
	if dictionaryRecords > 0 {
		snapshot.DictionaryCodes = make([]columnManifestDictionaryCodesSnapshot, 0, dictionaryRecords)
	}
	if int64ValueRecords > 0 {
		snapshot.Int64Values = make([]columnManifestInt64ValuesSnapshot, 0, int64ValueRecords)
	}
	livePartNamespaces := make(map[[2]uint64]string, partRecords)
	livePartRows := make(map[[2]uint64]int, partRecords)
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		keyGeneration, keyPartID, err := decodeColumnManifestPartRecordKey(record.key)
		if err != nil {
			return columnManifestSnapshot{}, err
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			return columnManifestSnapshot{}, err
		}
		if part.AssetRef.Generation != keyGeneration || part.AssetRef.PartID != keyPartID {
			return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest part key generation=%d part_id=%d does not match payload generation=%d part_id=%d", keyGeneration, keyPartID, part.AssetRef.Generation, part.AssetRef.PartID)
		}
		if part.AssetRef.Generation > snapshot.Generation {
			return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", part.AssetRef.Generation, snapshot.Generation)
		}
		if part.AssetRef.Generation <= snapshot.Generation {
			livePartNamespaces[[2]uint64{part.AssetRef.Generation, part.AssetRef.PartID}] = part.AssetRef.Namespace
			livePartRows[[2]uint64{part.AssetRef.Generation, part.AssetRef.PartID}] = part.Rows
		}
		if part.AssetRef.Generation == snapshot.Generation {
			snapshot.Parts = append(snapshot.Parts, part)
		}
	}
	for _, record := range records {
		switch {
		case bytes.HasPrefix(record.key, columnManifestAggregateMetadataRecordPrefixBytes):
			aggregate, err := decodeColumnManifestAggregateMetadataRecord(record.key, record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if aggregate.AssetRef.Generation > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d is newer than header generation=%d", aggregate.AssetRef.Generation, snapshot.Generation)
			}
			partNamespace, ok := livePartNamespaces[[2]uint64{aggregate.AssetRef.Generation, aggregate.AssetRef.PartID}]
			if !ok {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata generation=%d part_id=%d has no matching live part record", aggregate.AssetRef.Generation, aggregate.AssetRef.PartID)
			}
			if aggregate.AssetRef.Namespace != partNamespace {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata namespace=%q does not match part namespace=%q", aggregate.AssetRef.Namespace, partNamespace)
			}
			snapshot.AggregateMetadata = append(snapshot.AggregateMetadata, aggregate)
		case bytes.HasPrefix(record.key, columnManifestDictionaryCodesRecordPrefixBytes):
			dictionary, err := decodeColumnManifestDictionaryCodesRecord(record.key, record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if dictionary.AssetRef.Generation > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d is newer than header generation=%d", dictionary.AssetRef.Generation, snapshot.Generation)
			}
			partNamespace, ok := livePartNamespaces[[2]uint64{dictionary.AssetRef.Generation, dictionary.AssetRef.PartID}]
			if !ok {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes generation=%d part_id=%d has no matching live part record", dictionary.AssetRef.Generation, dictionary.AssetRef.PartID)
			}
			if dictionary.AssetRef.Namespace != partNamespace {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes namespace=%q does not match part namespace=%q", dictionary.AssetRef.Namespace, partNamespace)
			}
			snapshot.DictionaryCodes = append(snapshot.DictionaryCodes, dictionary)
		case bytes.HasPrefix(record.key, columnManifestInt64ValuesRecordPrefixBytes):
			values, err := decodeColumnManifestInt64ValuesRecord(record.key, record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			if values.AssetRef.Generation > snapshot.Generation {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest int64 values generation=%d is newer than header generation=%d", values.AssetRef.Generation, snapshot.Generation)
			}
			partKey := [2]uint64{values.AssetRef.Generation, values.AssetRef.PartID}
			partNamespace, ok := livePartNamespaces[partKey]
			if !ok {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest int64 values generation=%d part_id=%d has no matching live part record", values.AssetRef.Generation, values.AssetRef.PartID)
			}
			if values.AssetRef.Namespace != partNamespace {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest int64 values namespace=%q does not match part namespace=%q", values.AssetRef.Namespace, partNamespace)
			}
			partRows := livePartRows[partKey]
			if values.Rows != partRows {
				return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest int64 values rows=%d does not match part rows=%d", values.Rows, partRows)
			}
			snapshot.Int64Values = append(snapshot.Int64Values, values)
		}
	}
	if uint64(len(snapshot.Parts)) != snapshot.ExpectedParts {
		return columnManifestSnapshot{}, fmt.Errorf("collections: invalid column manifest part count=%d want %d", len(snapshot.Parts), snapshot.ExpectedParts)
	}
	return snapshot, nil
}

func decodeColumnManifestDictionaryCodesRecord(key, raw []byte) (columnManifestDictionaryCodesSnapshot, error) {
	generation, partID, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestDictionaryCodesSnapshot{}, err
	}
	part, err := decodeColumnManifestPartRecord(raw)
	if err != nil {
		return columnManifestDictionaryCodesSnapshot{}, err
	}
	if part.AssetRef.Kind != ColumnAssetKindTCS1DictionaryCodes {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes asset kind=%q want %q", part.AssetRef.Kind, ColumnAssetKindTCS1DictionaryCodes)
	}
	if part.AssetRef.Generation != generation || part.AssetRef.PartID != partID {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes key generation/part does not match ref")
	}
	if !manifestBytesEqualString(columnName, part.Reason) {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes key column=%q does not match record column=%q", string(columnName), part.Reason)
	}
	return columnManifestDictionaryCodesSnapshot{
		AssetRef:     part.AssetRef,
		ColumnName:   part.Reason,
		Bytes:        part.Bytes,
		PublishID:    part.PublishID,
		GenerationID: part.GenerationID,
	}, nil
}

func decodeColumnManifestInt64ValuesRecord(key, raw []byte) (columnManifestInt64ValuesSnapshot, error) {
	generation, partID, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestInt64ValuesSnapshot{}, err
	}
	part, err := decodeColumnManifestPartRecord(raw)
	if err != nil {
		return columnManifestInt64ValuesSnapshot{}, err
	}
	if part.AssetRef.Kind != ColumnAssetKindTCS1Int64Values {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values asset kind=%q want %q", part.AssetRef.Kind, ColumnAssetKindTCS1Int64Values)
	}
	if part.AssetRef.Generation != generation || part.AssetRef.PartID != partID {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values key generation/part does not match ref")
	}
	if !manifestBytesEqualString(columnName, part.Reason) {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values key column=%q does not match record column=%q", string(columnName), part.Reason)
	}
	return columnManifestInt64ValuesSnapshot{
		AssetRef:     part.AssetRef,
		ColumnName:   part.Reason,
		Rows:         part.Rows,
		Bytes:        part.Bytes,
		PublishID:    part.PublishID,
		GenerationID: part.GenerationID,
	}, nil
}

func decodeColumnManifestAggregateMetadataRecord(key, raw []byte) (columnManifestAggregateMetadataSnapshot, error) {
	generation, partID, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
	if err != nil {
		return columnManifestAggregateMetadataSnapshot{}, err
	}
	part, err := decodeColumnManifestPartRecord(raw)
	if err != nil {
		return columnManifestAggregateMetadataSnapshot{}, err
	}
	if part.AssetRef.Kind != ColumnAssetKindTCS1AggregateMetadata {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata asset kind=%q want %q", part.AssetRef.Kind, ColumnAssetKindTCS1AggregateMetadata)
	}
	if part.AssetRef.Generation != generation || part.AssetRef.PartID != partID {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata key generation/part does not match ref")
	}
	if !manifestBytesEqualString(name, part.Reason) {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata key name=%q does not match record name=%q", string(name), part.Reason)
	}
	return columnManifestAggregateMetadataSnapshot{
		AssetRef:     part.AssetRef,
		Name:         part.Reason,
		Bytes:        part.Bytes,
		PublishID:    part.PublishID,
		GenerationID: part.GenerationID,
	}, nil
}

func decodeColumnManifestHeaderRecord(raw []byte) (columnManifestSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestHeaderMagic {
		return columnManifestSnapshot{}, fmt.Errorf("collections: bad column manifest header magic=0x%08x", magic)
	}
	if version := cur.u16(); !isSupportedColumnManifestRecordVersion(version) {
		return columnManifestSnapshot{}, fmt.Errorf("collections: unsupported column manifest header version=%d", version)
	}
	collection := cur.string()
	operation := ColumnPublishOperation(cur.string())
	generation := cur.u64()
	appliedLSN := cur.u64()
	schemaHash := cur.u64()
	rowCount := cur.u64()
	commandBytes := cur.u64()
	rowRemainderBytes := cur.u64()
	columnPayloadBytes := cur.u64()
	partCount := cur.u64()
	if err := cur.err; err != nil {
		return columnManifestSnapshot{}, err
	}
	if rowCount > uint64(maxCollectionInt) {
		return columnManifestSnapshot{}, errors.New("collections: column manifest row count overflows int")
	}
	if commandBytes > uint64(math.MaxInt64) || rowRemainderBytes > uint64(math.MaxInt64) || columnPayloadBytes > uint64(math.MaxInt64) {
		return columnManifestSnapshot{}, errors.New("collections: column manifest byte counts overflow int64")
	}
	if cur.pos != len(raw) {
		return columnManifestSnapshot{}, errors.New("collections: trailing bytes in column manifest header record")
	}
	return columnManifestSnapshot{
		Collection:         collection,
		Operation:          operation,
		Generation:         generation,
		AppliedCommandLSN:  appliedLSN,
		SchemaHash:         schemaHash,
		ExpectedParts:      partCount,
		RowCount:           int(rowCount),
		CommandBytes:       int64(commandBytes),
		RowRemainderBytes:  int64(rowRemainderBytes),
		ColumnPayloadBytes: int64(columnPayloadBytes),
	}, nil
}

func decodeColumnManifestPartRecord(raw []byte) (columnManifestPartSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestPartMagic {
		return columnManifestPartSnapshot{}, fmt.Errorf("collections: bad column manifest part magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnManifestRecordVersion(version) {
		return columnManifestPartSnapshot{}, fmt.Errorf("collections: unsupported column manifest part version=%d", version)
	}
	kind := ColumnAssetKind(cur.string())
	namespace := cur.string()
	generation := cur.u64()
	partID := cur.u64()
	fileID64 := cur.u64()
	offset64 := cur.u64()
	length64 := cur.u64()
	checksum64 := cur.u64()
	rows64 := uint64(0)
	if version >= columnManifestRecordVersionV2 {
		rows64 = cur.u64()
	}
	bytes64 := cur.u64()
	publishID := cur.u64()
	generationID := cur.u64()
	reason := cur.string()
	role := ColumnManifestPartRole("")
	if version >= columnManifestRecordVersionV3 {
		role = ColumnManifestPartRole(cur.string())
	}
	sortKey := []ColumnSortKey(nil)
	if version >= columnManifestRecordVersionV4 {
		sortKey = readColumnManifestSortKey(&cur)
	}
	if err := cur.err; err != nil {
		return columnManifestPartSnapshot{}, err
	}
	if err := validateColumnManifestPartSortKeyForScan(kind, sortKey); err != nil {
		return columnManifestPartSnapshot{}, err
	}
	if role == "" {
		if version >= columnManifestRecordVersionV3 && (kind == ColumnAssetKindTCS1PartImage || kind == ColumnAssetKindTCS1TypedColumnPart) {
			return columnManifestPartSnapshot{}, errors.New("collections: column manifest part role is required for v3 typed-storage part record")
		}
		role = inferColumnManifestPartRole(kind, reason)
	}
	if fileID64 > uint64(math.MaxUint32) {
		return columnManifestPartSnapshot{}, errors.New("collections: column manifest part file_id overflows uint32")
	}
	if checksum64 > uint64(math.MaxUint32) {
		return columnManifestPartSnapshot{}, errors.New("collections: column manifest part checksum overflows uint32")
	}
	if rows64 > uint64(maxCollectionInt) {
		return columnManifestPartSnapshot{}, errors.New("collections: column manifest part rows overflows int")
	}
	if offset64 > uint64(math.MaxInt64) || length64 > uint64(math.MaxInt64) || bytes64 > uint64(math.MaxInt64) {
		return columnManifestPartSnapshot{}, errors.New("collections: column manifest part offsets or byte counts overflow int64")
	}
	if cur.pos != len(raw) {
		return columnManifestPartSnapshot{}, errors.New("collections: trailing bytes in column manifest part record")
	}
	ref := ColumnAssetRef{
		Kind:       kind,
		Namespace:  namespace,
		Generation: generation,
		PartID:     partID,
		FileID:     uint32(fileID64),
		Offset:     int64(offset64),
		Length:     int64(length64),
		Checksum:   uint32(checksum64),
	}
	asset := ColumnPreparedAsset{Ref: ref, Rows: int(rows64), Bytes: int64(bytes64), PublishID: publishID, GenerationID: generationID, Reason: reason, PartRole: role, SortKey: columnSortKeyMatchString(sortKey)}
	if err := validateColumnPreparedAssetForPlan(asset); err != nil {
		return columnManifestPartSnapshot{}, err
	}
	return columnManifestPartSnapshot{
		AssetRef:     ref,
		Rows:         int(rows64),
		Bytes:        int64(bytes64),
		PublishID:    publishID,
		GenerationID: generationID,
		Reason:       reason,
		PartRole:     role,
		SortKey:      sortKey,
	}, nil
}

func isSupportedColumnManifestRecordVersion(version uint16) bool {
	if version == columnManifestRecordVersion {
		return true
	}
	return version == columnManifestRecordVersionV4 ||
		version == columnManifestRecordVersionV3 ||
		version == columnManifestRecordVersionV2 ||
		version == columnManifestRecordVersionV1
}

func inferColumnManifestPartRole(kind ColumnAssetKind, reason string) ColumnManifestPartRole {
	if kind != ColumnAssetKindTCS1PartImage && kind != ColumnAssetKindTCS1TypedColumnPart {
		return ""
	}
	switch reason {
	case string(ColumnPublishOperationDelete):
		return ColumnManifestPartRoleTombstone
	case string(ColumnPublishOperationUpdate):
		return ColumnManifestPartRoleDelta
	case string(ColumnPublishOperationInsert):
		return ColumnManifestPartRoleBase
	default:
		return ""
	}
}

func decodeColumnManifestPartRecordKey(key []byte) (uint64, uint64, error) {
	if !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
		return 0, 0, fmt.Errorf("collections: invalid column manifest part key prefix %q", string(key))
	}
	if len(key) != len(columnManifestPartRecordPrefix)+16 {
		return 0, 0, fmt.Errorf("collections: invalid column manifest part key length=%d", len(key))
	}
	keySuffix := key[len(columnManifestPartRecordPrefix):]
	return binary.BigEndian.Uint64(keySuffix[:8]), binary.BigEndian.Uint64(keySuffix[8:]), nil
}

func enumerateColumnManifestAssetRefs(iter iterator.UnsafeIterator) ([]ColumnAssetRef, error) {
	if iter == nil {
		return nil, nil
	}
	var refs []ColumnAssetRef
	partRefs, err := enumerateColumnManifestAssetRefsForPrefix(iter, columnManifestPartRecordPrefixBytes, func(key, value []byte) (ColumnAssetRef, error) {
		keyGeneration, keyPartID, err := decodeColumnManifestPartRecordKey(key)
		if err != nil {
			return ColumnAssetRef{}, err
		}
		part, err := decodeColumnManifestPartRecord(value)
		if err != nil {
			return ColumnAssetRef{}, err
		}
		if part.AssetRef.Generation != keyGeneration || part.AssetRef.PartID != keyPartID {
			return ColumnAssetRef{}, fmt.Errorf("collections: column manifest part key generation=%d part_id=%d does not match payload generation=%d part_id=%d", keyGeneration, keyPartID, part.AssetRef.Generation, part.AssetRef.PartID)
		}
		return part.AssetRef, nil
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, partRefs...)
	metadataRefs, err := enumerateColumnManifestAssetRefsForPrefix(iter, columnManifestAggregateMetadataRecordPrefixBytes, func(key, value []byte) (ColumnAssetRef, error) {
		metadata, err := decodeColumnManifestAggregateMetadataRecord(key, value)
		return metadata.AssetRef, err
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, metadataRefs...)
	dictionaryRefs, err := enumerateColumnManifestAssetRefsForPrefix(iter, columnManifestDictionaryCodesRecordPrefixBytes, func(key, value []byte) (ColumnAssetRef, error) {
		dictionary, err := decodeColumnManifestDictionaryCodesRecord(key, value)
		return dictionary.AssetRef, err
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, dictionaryRefs...)
	int64Refs, err := enumerateColumnManifestAssetRefsForPrefix(iter, columnManifestInt64ValuesRecordPrefixBytes, func(key, value []byte) (ColumnAssetRef, error) {
		values, err := decodeColumnManifestInt64ValuesRecord(key, value)
		return values.AssetRef, err
	})
	if err != nil {
		return nil, err
	}
	refs = append(refs, int64Refs...)
	stateRefs, err := enumerateColumnVectorIndexStateAssetRefs(iter)
	if err != nil {
		return nil, err
	}
	refs = append(refs, stateRefs...)
	return refs, iter.Error()
}

func enumerateColumnVectorIndexStateAssetRefs(iter iterator.UnsafeIterator) ([]ColumnAssetRef, error) {
	iter.Seek(columnVectorIndexStateRecordPrefixBytes)
	var refs []ColumnAssetRef
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.HasPrefix(key, columnVectorIndexStateRecordPrefixBytes) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, errors.New("collections: vector-index state record must be inline")
		}
		state, err := decodeColumnVectorIndexStateRecord(value)
		if err != nil {
			return nil, err
		}
		for _, asset := range state.Assets {
			refs = append(refs, asset.Ref)
		}
		iter.Next()
	}
	return refs, iter.Error()
}

func enumerateColumnManifestAssetRefsForPrefix(iter iterator.UnsafeIterator, prefix []byte, decode func([]byte, []byte) (ColumnAssetRef, error)) ([]ColumnAssetRef, error) {
	iter.Seek(prefix)
	var refs []ColumnAssetRef
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, errors.New("collections: column manifest asset record must be inline")
		}
		ref, err := decode(key, value)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
		iter.Next()
	}
	return refs, iter.Error()
}

func columnManifestAggregateMetadataKeyFromRecordKey(key []byte) (uint64, uint64, string, error) {
	generation, partID, name, err := columnManifestAggregateMetadataKeyPartsFromRecordKey(key)
	if err != nil {
		return 0, 0, "", err
	}
	return generation, partID, string(name), nil
}

func columnManifestAggregateMetadataKeyPartsFromRecordKey(key []byte) (uint64, uint64, []byte, error) {
	if !bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes) {
		return 0, 0, nil, fmt.Errorf("collections: column manifest aggregate metadata key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestAggregateMetadataRecordPrefix)+16 {
		return 0, 0, nil, fmt.Errorf("collections: column manifest aggregate metadata key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestAggregateMetadataRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestAggregateMetadataRecordPrefix)+8:]),
		key[len(columnManifestAggregateMetadataRecordPrefix)+16:], nil
}

func columnManifestDictionaryCodesKeyFromRecordKey(key []byte) (uint64, uint64, string, error) {
	generation, partID, columnName, err := columnManifestDictionaryCodesKeyPartsFromRecordKey(key)
	if err != nil {
		return 0, 0, "", err
	}
	return generation, partID, string(columnName), nil
}

func columnManifestDictionaryCodesKeyPartsFromRecordKey(key []byte) (uint64, uint64, []byte, error) {
	if !bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes) {
		return 0, 0, nil, fmt.Errorf("collections: column manifest dictionary codes key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestDictionaryCodesRecordPrefix)+16 {
		return 0, 0, nil, fmt.Errorf("collections: column manifest dictionary codes key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestDictionaryCodesRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestDictionaryCodesRecordPrefix)+8:]),
		key[len(columnManifestDictionaryCodesRecordPrefix)+16:], nil
}

func columnManifestInt64ValuesKeyFromRecordKey(key []byte) (uint64, uint64, string, error) {
	generation, partID, columnName, err := columnManifestInt64ValuesKeyPartsFromRecordKey(key)
	if err != nil {
		return 0, 0, "", err
	}
	return generation, partID, string(columnName), nil
}

func columnManifestInt64ValuesKeyPartsFromRecordKey(key []byte) (uint64, uint64, []byte, error) {
	if !bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes) {
		return 0, 0, nil, fmt.Errorf("collections: column manifest int64 values key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestInt64ValuesRecordPrefix)+16 {
		return 0, 0, nil, fmt.Errorf("collections: column manifest int64 values key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestInt64ValuesRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestInt64ValuesRecordPrefix)+8:]),
		key[len(columnManifestInt64ValuesRecordPrefix)+16:], nil
}

func columnManifestRootRecordIterator(identityRecord [columnManifestIdentityRecordSize]byte, records []columnManifestRecord) *systemTargetIterator {
	entries := make([]systemTargetEntry, 0, 1+len(records))
	identityValue := make([]byte, len(identityRecord))
	copy(identityValue, identityRecord[:])
	entries = append(entries, systemTargetEntry{
		key:   newColumnManifestIdentityRecordKey(),
		value: identityValue,
	})
	for _, record := range records {
		entries = append(entries, systemTargetEntry{
			key:   bytes.Clone(record.key),
			value: bytes.Clone(record.value),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &systemTargetIterator{entries: entries}
}

func columnManifestRootMutationIterator(identityRecord [columnManifestIdentityRecordSize]byte, mutations []columnManifestMutation) *systemTargetIterator {
	entries := make([]systemTargetEntry, 0, 1+len(mutations))
	identityValue := make([]byte, len(identityRecord))
	copy(identityValue, identityRecord[:])
	entries = append(entries, systemTargetEntry{
		key:   newColumnManifestIdentityRecordKey(),
		value: identityValue,
	})
	for _, mutation := range mutations {
		entry := systemTargetEntry{key: bytes.Clone(mutation.record.key)}
		if mutation.deleted {
			entry.flags = node.FlagTombstone
		} else {
			entry.value = bytes.Clone(mutation.record.value)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &systemTargetIterator{entries: entries}
}

// columnManifestRootRecordIteratorOwned avoids cloning record key/value slices
// when the caller owns the records for the whole synchronous publish.
func columnManifestRootRecordIteratorOwned(identityRecord [columnManifestIdentityRecordSize]byte, records []columnManifestRecord) *systemTargetIterator {
	entries := make([]systemTargetEntry, 0, 1+len(records))
	identityValue := make([]byte, len(identityRecord))
	copy(identityValue, identityRecord[:])
	entries = append(entries, systemTargetEntry{
		key:   newColumnManifestIdentityRecordKey(),
		value: identityValue,
	})
	for _, record := range records {
		entries = append(entries, systemTargetEntry{
			key:   record.key,
			value: record.value,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	return &systemTargetIterator{entries: entries}
}

func cloneColumnManifestRecords(records []columnManifestRecord) []columnManifestRecord {
	if len(records) == 0 {
		return nil
	}
	out := make([]columnManifestRecord, len(records))
	for i, record := range records {
		out[i] = columnManifestRecord{
			key:   bytes.Clone(record.key),
			value: bytes.Clone(record.value),
		}
	}
	return out
}

func sortColumnManifestRecords(records []columnManifestRecord) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].key, records[j].key) < 0
	})
}

func columnManifestRecordsBytes(records []columnManifestRecord) int64 {
	total := int64(len(columnManifestIdentityRecordKey) + columnManifestIdentityRecordSize)
	for _, record := range records {
		total += int64(len(record.key) + len(record.value))
	}
	return total
}

func checksumColumnManifestRecords(input ColumnPublishManifestEncodeInput, generation uint64, records []columnManifestRecord) uint64 {
	var d xxhash.Digest
	d.Reset()
	writeHashString(&d, input.Collection)
	writeHashString(&d, string(input.Operation))
	writeHashUint64(&d, generation)
	writeHashUint64(&d, input.AppliedCommandLSN)
	writeHashUint64(&d, input.ColumnStore.SchemaHash)
	for _, record := range records {
		writeHashBytes(&d, record.key)
		writeHashBytes(&d, record.value)
	}
	sum := d.Sum64()
	if sum == 0 {
		return 1
	}
	return sum
}

func writeManifestUint16(b *bytes.Buffer, value uint16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	_, _ = b.Write(buf[:])
}

func writeManifestUint32(b *bytes.Buffer, value uint32) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	_, _ = b.Write(buf[:])
}

func writeManifestUint64(b *bytes.Buffer, value uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	_, _ = b.Write(buf[:])
}

func writeManifestString(b *bytes.Buffer, value string) {
	writeManifestUint64(b, uint64(len(value)))
	_, _ = b.WriteString(value)
}

func manifestStringEncodedSize(value string) int {
	return 8 + len(value)
}

func writeColumnManifestSortKey(b *bytes.Buffer, sortKeys []ColumnSortKey) {
	// Callers validate the cap before encoding; keep this defensive marker local
	// to the manifest format so readers and writers share one bound.
	writeManifestUint64(b, uint64(len(sortKeys)))
	for _, sortKey := range sortKeys {
		writeManifestString(b, sortKey.Column)
		writeManifestString(b, string(sortKey.Direction))
	}
}

func readColumnManifestSortKey(cur *manifestCursor) []ColumnSortKey {
	count := cur.u64()
	if cur.err != nil {
		return nil
	}
	if count > columnManifestSortKeyMaxColumns {
		cur.err = fmt.Errorf("collections: column manifest sort key columns=%d exceeds cap %d", count, columnManifestSortKeyMaxColumns)
		return nil
	}
	if count == 0 {
		return nil
	}
	out := make([]ColumnSortKey, 0, count)
	for i := uint64(0); i < count; i++ {
		column := cur.string()
		direction := ColumnSortDirection(cur.string())
		out = append(out, ColumnSortKey{Column: column, Direction: direction})
	}
	return out
}

func skipColumnManifestSortKey(cur *manifestCursor) {
	count := cur.u64()
	if cur.err != nil {
		return
	}
	if count > columnManifestSortKeyMaxColumns {
		cur.err = fmt.Errorf("collections: column manifest sort key columns=%d exceeds cap %d", count, columnManifestSortKeyMaxColumns)
		return
	}
	for i := uint64(0); i < count; i++ {
		cur.skipStringBytes()
		cur.skipStringBytes()
	}
}

type manifestCursor struct {
	raw []byte
	pos int
	err error
}

func (c *manifestCursor) u16() uint16 {
	if c.err != nil {
		return 0
	}
	if len(c.raw)-c.pos < 2 {
		c.err = errors.New("collections: short column manifest record")
		return 0
	}
	value := binary.BigEndian.Uint16(c.raw[c.pos:])
	c.pos += 2
	return value
}

func (c *manifestCursor) u32() uint32 {
	if c.err != nil {
		return 0
	}
	if len(c.raw)-c.pos < 4 {
		c.err = errors.New("collections: short column manifest record")
		return 0
	}
	value := binary.BigEndian.Uint32(c.raw[c.pos:])
	c.pos += 4
	return value
}

func (c *manifestCursor) u64() uint64 {
	if c.err != nil {
		return 0
	}
	if len(c.raw)-c.pos < 8 {
		c.err = errors.New("collections: short column manifest record")
		return 0
	}
	value := binary.BigEndian.Uint64(c.raw[c.pos:])
	c.pos += 8
	return value
}

func (c *manifestCursor) string() string {
	value := c.stringBytes()
	if value == nil {
		return ""
	}
	return string(value)
}

func (c *manifestCursor) stringBytes() []byte {
	if c.err != nil {
		return nil
	}
	n := c.u64()
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.raw)-c.pos) {
		c.err = errors.New("collections: short column manifest string")
		return nil
	}
	value := c.raw[c.pos : c.pos+int(n)]
	c.pos += int(n)
	return value
}

func manifestBytesEqualString(value []byte, expected string) bool {
	if len(value) != len(expected) {
		return false
	}
	for i, b := range value {
		if b != expected[i] {
			return false
		}
	}
	return true
}

func (c *manifestCursor) skip(n uint64) {
	if c.err != nil {
		return
	}
	if n > uint64(len(c.raw)-c.pos) {
		c.err = errors.New("collections: short column manifest record")
		return
	}
	c.pos += int(n)
}

func (c *manifestCursor) skipStringBytes() {
	if c.err != nil {
		return
	}
	n := c.u64()
	if c.err != nil {
		return
	}
	if n > uint64(len(c.raw)-c.pos) {
		c.err = errors.New("collections: short column manifest string")
		return
	}
	c.pos += int(n)
}
