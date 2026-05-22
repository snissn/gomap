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

	columnManifestHeaderMagic     = uint32(0x54434d48) // TCMH
	columnManifestPartMagic       = uint32(0x54434d50) // TCMP
	columnManifestRecordVersionV1 = uint16(1)
	columnManifestRecordVersion   = uint16(2)
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
	retained, err := retainedColumnManifestRecordsForWrite(input.CurrentManifestRecords, generation)
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
		Records:       cloneColumnManifestRecords(records),
	}, nil
}

func retainedColumnManifestRecordsForWrite(records []columnManifestRecord, generation uint64) ([]columnManifestRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	retained := make([]columnManifestRecord, 0, len(records))
	for _, record := range records {
		if !bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)) &&
			!bytes.HasPrefix(record.key, []byte(columnManifestAggregateMetadataRecordPrefix)) &&
			!bytes.HasPrefix(record.key, []byte(columnManifestDictionaryCodesRecordPrefix)) &&
			!bytes.HasPrefix(record.key, []byte(columnManifestInt64ValuesRecordPrefix)) {
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
		if asset.Ref.Kind == ColumnAssetKindTCS1PartImage {
			count++
		}
	}
	return count
}

func encodeColumnManifestPartRecord(asset ColumnPreparedAsset) ([]byte, error) {
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
	generation, partID, columnName, err := columnManifestDictionaryCodesKeyFromRecordKey(key)
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
	if part.Reason != columnName {
		return columnManifestDictionaryCodesSnapshot{}, fmt.Errorf("collections: column manifest dictionary codes key column=%q does not match record column=%q", columnName, part.Reason)
	}
	return columnManifestDictionaryCodesSnapshot{
		AssetRef:     part.AssetRef,
		ColumnName:   columnName,
		Bytes:        part.Bytes,
		PublishID:    part.PublishID,
		GenerationID: part.GenerationID,
	}, nil
}

func decodeColumnManifestInt64ValuesRecord(key, raw []byte) (columnManifestInt64ValuesSnapshot, error) {
	generation, partID, columnName, err := columnManifestInt64ValuesKeyFromRecordKey(key)
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
	if part.Reason != columnName {
		return columnManifestInt64ValuesSnapshot{}, fmt.Errorf("collections: column manifest int64 values key column=%q does not match record column=%q", columnName, part.Reason)
	}
	return columnManifestInt64ValuesSnapshot{
		AssetRef:     part.AssetRef,
		ColumnName:   columnName,
		Rows:         part.Rows,
		Bytes:        part.Bytes,
		PublishID:    part.PublishID,
		GenerationID: part.GenerationID,
	}, nil
}

func decodeColumnManifestAggregateMetadataRecord(key, raw []byte) (columnManifestAggregateMetadataSnapshot, error) {
	generation, partID, name, err := columnManifestAggregateMetadataKeyFromRecordKey(key)
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
	if part.Reason != name {
		return columnManifestAggregateMetadataSnapshot{}, fmt.Errorf("collections: column manifest aggregate metadata key name=%q does not match record name=%q", name, part.Reason)
	}
	return columnManifestAggregateMetadataSnapshot{
		AssetRef:     part.AssetRef,
		Name:         name,
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
	if version := cur.u16(); version != columnManifestRecordVersion && version != columnManifestRecordVersionV1 {
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
	if version != columnManifestRecordVersion && version != columnManifestRecordVersionV1 {
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
	if version >= columnManifestRecordVersion {
		rows64 = cur.u64()
	}
	bytes64 := cur.u64()
	publishID := cur.u64()
	generationID := cur.u64()
	reason := cur.string()
	if err := cur.err; err != nil {
		return columnManifestPartSnapshot{}, err
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
	asset := ColumnPreparedAsset{Ref: ref, Rows: int(rows64), Bytes: int64(bytes64), PublishID: publishID, GenerationID: generationID, Reason: reason}
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
	}, nil
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
	if !bytes.HasPrefix(key, columnManifestAggregateMetadataRecordPrefixBytes) {
		return 0, 0, "", fmt.Errorf("collections: column manifest aggregate metadata key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestAggregateMetadataRecordPrefix)+16 {
		return 0, 0, "", fmt.Errorf("collections: column manifest aggregate metadata key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestAggregateMetadataRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestAggregateMetadataRecordPrefix)+8:]),
		string(key[len(columnManifestAggregateMetadataRecordPrefix)+16:]), nil
}

func columnManifestDictionaryCodesKeyFromRecordKey(key []byte) (uint64, uint64, string, error) {
	if !bytes.HasPrefix(key, columnManifestDictionaryCodesRecordPrefixBytes) {
		return 0, 0, "", fmt.Errorf("collections: column manifest dictionary codes key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestDictionaryCodesRecordPrefix)+16 {
		return 0, 0, "", fmt.Errorf("collections: column manifest dictionary codes key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestDictionaryCodesRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestDictionaryCodesRecordPrefix)+8:]),
		string(key[len(columnManifestDictionaryCodesRecordPrefix)+16:]), nil
}

func columnManifestInt64ValuesKeyFromRecordKey(key []byte) (uint64, uint64, string, error) {
	if !bytes.HasPrefix(key, columnManifestInt64ValuesRecordPrefixBytes) {
		return 0, 0, "", fmt.Errorf("collections: column manifest int64 values key %q missing prefix", string(key))
	}
	if len(key) <= len(columnManifestInt64ValuesRecordPrefix)+16 {
		return 0, 0, "", fmt.Errorf("collections: column manifest int64 values key length=%d too short", len(key))
	}
	return binary.BigEndian.Uint64(key[len(columnManifestInt64ValuesRecordPrefix):]),
		binary.BigEndian.Uint64(key[len(columnManifestInt64ValuesRecordPrefix)+8:]),
		string(key[len(columnManifestInt64ValuesRecordPrefix)+16:]), nil
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
