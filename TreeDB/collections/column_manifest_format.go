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
	columnManifestHeaderRecordKey  = "\x01column-manifest/v1/header"
	columnManifestPartRecordPrefix = "\x02column-manifest/v1/part/"

	columnManifestHeaderMagic   = uint32(0x54434d48) // TCMH
	columnManifestPartMagic     = uint32(0x54434d50) // TCMP
	columnManifestRecordVersion = uint16(1)
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
}

type columnManifestPartSnapshot struct {
	AssetRef     ColumnAssetRef
	Rows         int
	Bytes        int64
	PublishID    uint64
	GenerationID uint64
	Reason       string
}

func columnManifestPartRecordKey(generation, partID uint64) []byte {
	key := make([]byte, len(columnManifestPartRecordPrefix)+16)
	copy(key, columnManifestPartRecordPrefix)
	binary.BigEndian.PutUint64(key[len(columnManifestPartRecordPrefix):], generation)
	binary.BigEndian.PutUint64(key[len(columnManifestPartRecordPrefix)+8:], partID)
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
	retained, err := retainedColumnManifestPartRecordsForWrite(input.CurrentManifestRecords, generation)
	if err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	records = append(records, retained...)
	for _, asset := range input.Prepared.Assets {
		partValue, err := encodeColumnManifestPartRecord(asset)
		if err != nil {
			return ColumnPublishManifestEncodeResult{}, err
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

func retainedColumnManifestPartRecordsForWrite(records []columnManifestRecord, generation uint64) ([]columnManifestRecord, error) {
	if len(records) == 0 {
		return nil, nil
	}
	retained := make([]columnManifestRecord, 0, len(records))
	for _, record := range records {
		if !bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)) {
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
	writeManifestUint64(&b, uint64(len(input.Prepared.Assets)))
	return b.Bytes(), nil
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
	writeManifestUint64(&b, uint64(asset.Bytes))
	writeManifestUint64(&b, asset.PublishID)
	writeManifestUint64(&b, asset.GenerationID)
	writeManifestString(&b, asset.Reason)
	return b.Bytes(), nil
}

func decodeColumnManifestRecords(records []columnManifestRecord) (columnManifestSnapshot, error) {
	var snapshot columnManifestSnapshot
	var parts []columnManifestPartSnapshot
	sawHeader := false
	for _, record := range records {
		switch {
		case bytes.Equal(record.key, []byte(columnManifestHeaderRecordKey)):
			if sawHeader {
				return columnManifestSnapshot{}, errors.New("collections: duplicate column manifest binary header record")
			}
			header, err := decodeColumnManifestHeaderRecord(record.value)
			if err != nil {
				return columnManifestSnapshot{}, err
			}
			snapshot = header
			sawHeader = true
		case bytes.HasPrefix(record.key, []byte(columnManifestPartRecordPrefix)):
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
			parts = append(parts, part)
		}
	}
	if !sawHeader {
		return columnManifestSnapshot{}, errors.New("collections: column manifest missing binary header record")
	}
	for _, part := range parts {
		if part.AssetRef.Generation > snapshot.Generation {
			return columnManifestSnapshot{}, fmt.Errorf("collections: column manifest part generation=%d is newer than header generation=%d", part.AssetRef.Generation, snapshot.Generation)
		}
		if part.AssetRef.Generation == snapshot.Generation {
			snapshot.Parts = append(snapshot.Parts, part)
		}
	}
	if uint64(len(snapshot.Parts)) != snapshot.ExpectedParts {
		return columnManifestSnapshot{}, fmt.Errorf("collections: invalid column manifest part count=%d want %d", len(snapshot.Parts), snapshot.ExpectedParts)
	}
	return snapshot, nil
}

func decodeColumnManifestHeaderRecord(raw []byte) (columnManifestSnapshot, error) {
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnManifestHeaderMagic {
		return columnManifestSnapshot{}, fmt.Errorf("collections: bad column manifest header magic=0x%08x", magic)
	}
	if version := cur.u16(); version != columnManifestRecordVersion {
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
	if version := cur.u16(); version != columnManifestRecordVersion {
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
	asset := ColumnPreparedAsset{Ref: ref, Bytes: int64(bytes64), PublishID: publishID, GenerationID: generationID, Reason: reason}
	if err := validateColumnPreparedAssetForPlan(asset); err != nil {
		return columnManifestPartSnapshot{}, err
	}
	return columnManifestPartSnapshot{
		AssetRef:     ref,
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
	iter.Seek([]byte(columnManifestPartRecordPrefix))
	var refs []ColumnAssetRef
	for iter.Valid() {
		key := iter.UnsafeKey()
		if !bytes.HasPrefix(key, []byte(columnManifestPartRecordPrefix)) {
			break
		}
		if iter.IsDeleted() {
			iter.Next()
			continue
		}
		value, _, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer != 0 {
			return nil, errors.New("collections: column manifest part record must be inline")
		}
		keyGeneration, keyPartID, err := decodeColumnManifestPartRecordKey(key)
		if err != nil {
			return nil, err
		}
		part, err := decodeColumnManifestPartRecord(value)
		if err != nil {
			return nil, err
		}
		if part.AssetRef.Generation != keyGeneration || part.AssetRef.PartID != keyPartID {
			return nil, fmt.Errorf("collections: column manifest part key generation=%d part_id=%d does not match payload generation=%d part_id=%d", keyGeneration, keyPartID, part.AssetRef.Generation, part.AssetRef.PartID)
		}
		refs = append(refs, part.AssetRef)
		iter.Next()
	}
	return refs, iter.Error()
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
