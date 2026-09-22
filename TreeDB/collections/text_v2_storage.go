package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	textV2FormatVersion uint32 = 1

	textV2KeyVersion byte = 2

	textV2RootFormatValueVersion byte = 1
	textV2StatusValueVersion     byte = 1
	textV2DocIDValueVersion      byte = 1
	textV2DocMapValueVersion     byte = 1
	textV2NormBlockValueVersion  byte = 1
	textV2StatsValueVersion      byte = 1

	textV2KeyKindFormat      byte = 0x00
	textV2KeyKindStatus      byte = 0x01
	textV2KeyKindCorpusStats byte = 0x02
	textV2KeyKindFieldStats  byte = 0x03
	textV2KeyKindTermStats   byte = 0x04
	textV2KeyKindDocID       byte = 0x10
	textV2KeyKindBlock       byte = 0x20

	textV2DefaultDocMapBlockSize uint32 = 128
	textV2DefaultNormBlockSize   uint32 = 128

	textV2DocFlagTombstone byte = 1 << 0
)

type textV2RootFamily byte

const (
	textV2RootFamilyDocID textV2RootFamily = iota + 1
	textV2RootFamilyDocMap
	textV2RootFamilyTerms
	textV2RootFamilyPostingBlocks
	textV2RootFamilyNormBlocks
	textV2RootFamilyPositions
	textV2RootFamilyGenerations
)

func (f textV2RootFamily) String() string {
	switch f {
	case textV2RootFamilyDocID:
		return "docid"
	case textV2RootFamilyDocMap:
		return "docmap"
	case textV2RootFamilyTerms:
		return "terms"
	case textV2RootFamilyPostingBlocks:
		return "posting_blocks"
	case textV2RootFamilyNormBlocks:
		return "norm_blocks"
	case textV2RootFamilyPositions:
		return "positions"
	case textV2RootFamilyGenerations:
		return "generations"
	default:
		return fmt.Sprintf("unknown(%d)", byte(f))
	}
}

func textV2RootFamilyForName(collection, indexName, rootName string) (textV2RootFamily, bool) {
	switch rootName {
	case collectionTextV2DocIDRootName(collection, indexName):
		return textV2RootFamilyDocID, true
	case collectionTextV2DocMapRootName(collection, indexName):
		return textV2RootFamilyDocMap, true
	case collectionTextV2TermsRootName(collection, indexName):
		return textV2RootFamilyTerms, true
	case collectionTextV2PostingBlocksRootName(collection, indexName):
		return textV2RootFamilyPostingBlocks, true
	case collectionTextV2NormBlocksRootName(collection, indexName):
		return textV2RootFamilyNormBlocks, true
	case collectionTextV2PositionsRootName(collection, indexName):
		return textV2RootFamilyPositions, true
	case collectionTextV2GenerationsRootName(collection, indexName):
		return textV2RootFamilyGenerations, true
	default:
		return 0, false
	}
}

type textV2RootFormatValue struct {
	FormatVersion   uint32
	Family          textV2RootFamily
	DocMapBlockSize uint32
	NormBlockSize   uint32
	Fields          []string
}

type textV2IndexStatusValue struct {
	FormatVersion    uint32
	RootGeneration   uint64
	StatsGeneration  uint64
	DocMapGeneration uint64
	NormGeneration   uint64
	TermGeneration   uint64
	NextOrdinal      uint64
	LiveDocuments    uint64
	DeletedDocuments uint64
}

type textV2DocIDValue struct {
	Ordinal    uint64
	Generation uint64
	Flags      byte
}

func (v textV2DocIDValue) tombstoned() bool { return v.Flags&textV2DocFlagTombstone != 0 }

type textV2DocMapBlockValue struct {
	BlockStart uint64
	BlockSize  uint32
	Entries    []textV2DocMapEntry
}

type textV2DocMapEntry struct {
	Ordinal    uint64
	Generation uint64
	Flags      byte
	DocumentID []byte
}

func (e textV2DocMapEntry) tombstoned() bool { return e.Flags&textV2DocFlagTombstone != 0 }

type textV2NormBlockValue struct {
	BlockStart uint64
	BlockSize  uint32
	FieldCount uint32
	Entries    []textV2NormBlockEntry
}

type textV2NormBlockEntry struct {
	Ordinal      uint64
	Generation   uint64
	Flags        byte
	FieldLengths []uint32
}

func (e textV2NormBlockEntry) tombstoned() bool { return e.Flags&textV2DocFlagTombstone != 0 }

type textV2CorpusStatsValue struct {
	StatsGeneration uint64
	DocumentCount   uint64
}

type textV2TermStatsValue struct {
	StatsGeneration    uint64
	DocumentFrequency  uint64
	TotalTermFrequency uint64
	PostingBlockCount  uint64
}

type textV2FieldStatsValue struct {
	StatsGeneration uint64
	DocumentCount   uint64
	TotalTokenCount uint64
}

type textV2StatsKey struct {
	Kind  byte
	Value string
}

func encodeTextV2FormatKey() []byte {
	return []byte{textV2KeyVersion, textV2KeyKindFormat}
}

func encodeTextV2StatusKey() []byte {
	return []byte{textV2KeyVersion, textV2KeyKindStatus}
}

func encodeTextV2DocIDKey(documentID []byte) []byte {
	out := make([]byte, 0, 2+len(documentID))
	out = append(out, textV2KeyVersion, textV2KeyKindDocID)
	out = append(out, documentID...)
	return out
}

func decodeTextV2DocIDKey(raw []byte) ([]byte, error) {
	if len(raw) < 2 {
		return nil, errMalformedTextStorage("short text-v2 docid key")
	}
	if raw[0] != textV2KeyVersion {
		return nil, errUnsupportedTextStorageVersion("text-v2 docid key", raw[0])
	}
	if raw[1] != textV2KeyKindDocID {
		return nil, errMalformedTextStorage("text-v2 docid key kind %d", raw[1])
	}
	if len(raw) == 2 {
		return nil, errMalformedTextStorage("text-v2 docid key missing document id")
	}
	return bytes.Clone(raw[2:]), nil
}

func encodeTextV2BlockKey(blockStart uint64) []byte {
	out := make([]byte, 10)
	out[0] = textV2KeyVersion
	out[1] = textV2KeyKindBlock
	binary.BigEndian.PutUint64(out[2:], blockStart)
	return out
}

func decodeTextV2BlockKey(raw []byte) (uint64, error) {
	if len(raw) != 10 {
		return 0, errMalformedTextStorage("text-v2 block key length %d", len(raw))
	}
	if raw[0] != textV2KeyVersion {
		return 0, errUnsupportedTextStorageVersion("text-v2 block key", raw[0])
	}
	if raw[1] != textV2KeyKindBlock {
		return 0, errMalformedTextStorage("text-v2 block key kind %d", raw[1])
	}
	blockStart := binary.BigEndian.Uint64(raw[2:])
	if blockStart == 0 {
		return 0, errMalformedTextStorage("text-v2 block key has zero block start")
	}
	return blockStart, nil
}

func encodeTextV2CorpusStatsKey() []byte {
	return []byte{textV2KeyVersion, textV2KeyKindCorpusStats}
}

func encodeTextV2FieldStatsKey(field string) []byte {
	out := []byte{textV2KeyVersion, textV2KeyKindFieldStats}
	return appendTextString(out, field)
}

func encodeTextV2TermStatsKey(term string) []byte {
	out := []byte{textV2KeyVersion, textV2KeyKindTermStats}
	return appendTextString(out, term)
}

func decodeTextV2StatsKey(raw []byte) (textV2StatsKey, error) {
	if len(raw) < 2 {
		return textV2StatsKey{}, errMalformedTextStorage("short text-v2 stats key")
	}
	if raw[0] != textV2KeyVersion {
		return textV2StatsKey{}, errUnsupportedTextStorageVersion("text-v2 stats key", raw[0])
	}
	key := textV2StatsKey{Kind: raw[1]}
	cur := textCursor{buf: raw[2:]}
	switch key.Kind {
	case textV2KeyKindCorpusStats:
		if cur.remaining() != 0 {
			return textV2StatsKey{}, errMalformedTextStorage("text-v2 corpus stats key has trailing bytes")
		}
	case textV2KeyKindFieldStats, textV2KeyKindTermStats:
		value, err := cur.readString()
		if err != nil {
			return textV2StatsKey{}, errMalformedTextStorage("text-v2 stats key value: %v", err)
		}
		if cur.remaining() != 0 {
			return textV2StatsKey{}, errMalformedTextStorage("text-v2 stats key has trailing bytes")
		}
		key.Value = value
	default:
		return textV2StatsKey{}, errMalformedTextStorage("unsupported text-v2 stats key kind %d", key.Kind)
	}
	return key, nil
}

func encodeTextV2RootFormatValue(value textV2RootFormatValue) []byte {
	fields := append([]string(nil), value.Fields...)
	out := make([]byte, 0, 16+len(fields)*8)
	out = append(out, textV2RootFormatValueVersion)
	out = appendTextUvarint(out, uint64(value.FormatVersion))
	out = append(out, byte(value.Family))
	out = appendTextUvarint(out, uint64(value.DocMapBlockSize))
	out = appendTextUvarint(out, uint64(value.NormBlockSize))
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		out = appendTextString(out, field)
	}
	return out
}

func decodeTextV2RootFormatValue(raw []byte) (textV2RootFormatValue, error) {
	if len(raw) == 0 {
		return textV2RootFormatValue{}, errMalformedTextStorage("empty text-v2 root format value")
	}
	if raw[0] != textV2RootFormatValueVersion {
		return textV2RootFormatValue{}, errUnsupportedTextStorageVersion("text-v2 root format value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	formatVersion, err := cur.readUvarint()
	if err != nil {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format version: %v", err)
	}
	if formatVersion != uint64(textV2FormatVersion) {
		return textV2RootFormatValue{}, errMalformedTextStorage("unsupported text-v2 format version %d", formatVersion)
	}
	if cur.remaining() == 0 {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format missing family")
	}
	family := textV2RootFamily(cur.buf[cur.pos])
	cur.pos++
	docMapBlockSize, err := cur.readUvarint()
	if err != nil {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format docmap block size: %v", err)
	}
	normBlockSize, err := cur.readUvarint()
	if err != nil {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format norm block size: %v", err)
	}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format field count: %v", err)
	}
	if fieldCount > uint64(cur.remaining()+1) {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format field count too large")
	}
	value := textV2RootFormatValue{
		FormatVersion:   uint32(formatVersion),
		Family:          family,
		DocMapBlockSize: checkedTextUint32(docMapBlockSize),
		NormBlockSize:   checkedTextUint32(normBlockSize),
		Fields:          make([]string, 0, fieldCount),
	}
	if uint64(value.DocMapBlockSize) != docMapBlockSize || value.DocMapBlockSize == 0 {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format invalid docmap block size")
	}
	if uint64(value.NormBlockSize) != normBlockSize || value.NormBlockSize == 0 {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format invalid norm block size")
	}
	if family < textV2RootFamilyDocID || family > textV2RootFamilyGenerations {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format invalid family %d", byte(family))
	}
	for i := uint64(0); i < fieldCount; i++ {
		field, err := cur.readString()
		if err != nil {
			return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format field[%d]: %v", i, err)
		}
		value.Fields = append(value.Fields, field)
	}
	if cur.remaining() != 0 {
		return textV2RootFormatValue{}, errMalformedTextStorage("text-v2 root format trailing bytes")
	}
	return value, nil
}

func encodeTextV2IndexStatusValue(value textV2IndexStatusValue) []byte {
	out := make([]byte, 0, 80)
	out = append(out, textV2StatusValueVersion)
	out = appendTextUvarint(out, uint64(value.FormatVersion))
	out = appendTextUvarint(out, value.RootGeneration)
	out = appendTextUvarint(out, value.StatsGeneration)
	out = appendTextUvarint(out, value.DocMapGeneration)
	out = appendTextUvarint(out, value.NormGeneration)
	out = appendTextUvarint(out, value.TermGeneration)
	out = appendTextUvarint(out, value.NextOrdinal)
	out = appendTextUvarint(out, value.LiveDocuments)
	out = appendTextUvarint(out, value.DeletedDocuments)
	return out
}

func decodeTextV2IndexStatusValue(raw []byte) (textV2IndexStatusValue, error) {
	cur, err := textV2ValueCursor(raw, textV2StatusValueVersion, "status")
	if err != nil {
		return textV2IndexStatusValue{}, err
	}
	formatVersion, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status format version: %v", err)
	}
	if formatVersion != uint64(textV2FormatVersion) {
		return textV2IndexStatusValue{}, errMalformedTextStorage("unsupported text-v2 status format version %d", formatVersion)
	}
	rootGeneration, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status root generation: %v", err)
	}
	statsGeneration, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status stats generation: %v", err)
	}
	docMapGeneration, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status docmap generation: %v", err)
	}
	normGeneration, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status norm generation: %v", err)
	}
	termGeneration, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status term generation: %v", err)
	}
	nextOrdinal, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status next ordinal: %v", err)
	}
	liveDocuments, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status live documents: %v", err)
	}
	deletedDocuments, err := cur.readUvarint()
	if err != nil {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status deleted documents: %v", err)
	}
	if cur.remaining() != 0 {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status trailing bytes")
	}
	if rootGeneration == 0 || statsGeneration == 0 || docMapGeneration == 0 || normGeneration == 0 || termGeneration == 0 {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status generation cannot be zero")
	}
	if statsGeneration > rootGeneration || docMapGeneration > rootGeneration || normGeneration > rootGeneration || termGeneration > rootGeneration {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status component generation exceeds root generation")
	}
	if nextOrdinal == 0 {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status next ordinal cannot be zero")
	}
	allocatedOrdinals := nextOrdinal - 1
	if liveDocuments > allocatedOrdinals || deletedDocuments > allocatedOrdinals-liveDocuments {
		return textV2IndexStatusValue{}, errMalformedTextStorage("text-v2 status live/deleted documents exceed allocated ordinals")
	}
	return textV2IndexStatusValue{
		FormatVersion:    uint32(formatVersion),
		RootGeneration:   rootGeneration,
		StatsGeneration:  statsGeneration,
		DocMapGeneration: docMapGeneration,
		NormGeneration:   normGeneration,
		TermGeneration:   termGeneration,
		NextOrdinal:      nextOrdinal,
		LiveDocuments:    liveDocuments,
		DeletedDocuments: deletedDocuments,
	}, nil
}

func encodeTextV2DocIDValue(value textV2DocIDValue) []byte {
	out := make([]byte, 0, 24)
	out = append(out, textV2DocIDValueVersion)
	out = appendTextUvarint(out, value.Ordinal)
	out = appendTextUvarint(out, value.Generation)
	out = append(out, value.Flags)
	return out
}

func decodeTextV2DocIDValue(raw []byte) (textV2DocIDValue, error) {
	cur, err := textV2ValueCursor(raw, textV2DocIDValueVersion, "docid")
	if err != nil {
		return textV2DocIDValue{}, err
	}
	ordinal, err := cur.readUvarint()
	if err != nil {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid ordinal: %v", err)
	}
	generation, err := cur.readUvarint()
	if err != nil {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid generation: %v", err)
	}
	if cur.remaining() == 0 {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid missing flags")
	}
	flags := cur.buf[cur.pos]
	cur.pos++
	if cur.remaining() != 0 {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid trailing bytes")
	}
	if ordinal == 0 || generation == 0 {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid ordinal/generation cannot be zero")
	}
	if flags&^textV2DocFlagTombstone != 0 {
		return textV2DocIDValue{}, errMalformedTextStorage("text-v2 docid unsupported flags 0x%x", flags)
	}
	return textV2DocIDValue{Ordinal: ordinal, Generation: generation, Flags: flags}, nil
}

func encodeTextV2DocMapBlockValue(value textV2DocMapBlockValue) []byte {
	entries := append([]textV2DocMapEntry(nil), value.Entries...)
	slices.SortFunc(entries, func(a, b textV2DocMapEntry) int {
		return compareUint64(a.Ordinal, b.Ordinal)
	})
	out := make([]byte, 0, 24+len(entries)*24)
	out = append(out, textV2DocMapValueVersion)
	out = appendTextUvarint(out, value.BlockStart)
	out = appendTextUvarint(out, uint64(value.BlockSize))
	out = appendTextUvarint(out, uint64(len(entries)))
	for _, entry := range entries {
		out = appendTextUvarint(out, entry.Ordinal)
		out = appendTextUvarint(out, entry.Generation)
		out = append(out, entry.Flags)
		out = appendTextBytes(out, entry.DocumentID)
	}
	return out
}

func decodeTextV2DocMapBlockValue(raw []byte) (textV2DocMapBlockValue, error) {
	cur, err := textV2ValueCursor(raw, textV2DocMapValueVersion, "docmap block")
	if err != nil {
		return textV2DocMapBlockValue{}, err
	}
	blockStart, err := cur.readUvarint()
	if err != nil {
		return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap block start: %v", err)
	}
	blockSizeRaw, err := cur.readUvarint()
	if err != nil {
		return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap block size: %v", err)
	}
	entryCount, err := cur.readUvarint()
	if err != nil {
		return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry count: %v", err)
	}
	if entryCount > uint64(cur.remaining()+1) {
		return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry count too large")
	}
	blockSize := checkedTextUint32(blockSizeRaw)
	value := textV2DocMapBlockValue{BlockStart: blockStart, BlockSize: blockSize, Entries: make([]textV2DocMapEntry, 0, entryCount)}
	if err := validateTextV2BlockHeader(blockStart, blockSizeRaw, blockSize, "docmap"); err != nil {
		return textV2DocMapBlockValue{}, err
	}
	var prev uint64
	for i := uint64(0); i < entryCount; i++ {
		ordinal, err := cur.readUvarint()
		if err != nil {
			return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry[%d] ordinal: %v", i, err)
		}
		generation, err := cur.readUvarint()
		if err != nil {
			return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry[%d] generation: %v", i, err)
		}
		if cur.remaining() == 0 {
			return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry[%d] missing flags", i)
		}
		flags := cur.buf[cur.pos]
		cur.pos++
		documentID, err := cur.readBytes()
		if err != nil {
			return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry[%d] document id: %v", i, err)
		}
		if err := validateTextV2BlockEntry(blockStart, blockSize, ordinal, generation, flags, prev, i, "docmap"); err != nil {
			return textV2DocMapBlockValue{}, err
		}
		if len(documentID) == 0 {
			return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap entry[%d] missing document id", i)
		}
		value.Entries = append(value.Entries, textV2DocMapEntry{Ordinal: ordinal, Generation: generation, Flags: flags, DocumentID: bytes.Clone(documentID)})
		prev = ordinal
	}
	if cur.remaining() != 0 {
		return textV2DocMapBlockValue{}, errMalformedTextStorage("text-v2 docmap trailing bytes")
	}
	return value, nil
}

func encodeTextV2NormBlockValue(value textV2NormBlockValue) []byte {
	entries := append([]textV2NormBlockEntry(nil), value.Entries...)
	slices.SortFunc(entries, func(a, b textV2NormBlockEntry) int {
		return compareUint64(a.Ordinal, b.Ordinal)
	})
	out := make([]byte, 0, 24+len(entries)*(16+int(value.FieldCount)*2))
	out = append(out, textV2NormBlockValueVersion)
	out = appendTextUvarint(out, value.BlockStart)
	out = appendTextUvarint(out, uint64(value.BlockSize))
	out = appendTextUvarint(out, uint64(value.FieldCount))
	out = appendTextUvarint(out, uint64(len(entries)))
	for _, entry := range entries {
		out = appendTextUvarint(out, entry.Ordinal)
		out = appendTextUvarint(out, entry.Generation)
		out = append(out, entry.Flags)
		for _, length := range entry.FieldLengths {
			out = appendTextUvarint(out, uint64(length))
		}
	}
	return out
}

func decodeTextV2NormBlockValue(raw []byte) (textV2NormBlockValue, error) {
	cur, err := textV2ValueCursor(raw, textV2NormBlockValueVersion, "norm block")
	if err != nil {
		return textV2NormBlockValue{}, err
	}
	blockStart, err := cur.readUvarint()
	if err != nil {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm block start: %v", err)
	}
	blockSizeRaw, err := cur.readUvarint()
	if err != nil {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm block size: %v", err)
	}
	fieldCountRaw, err := cur.readUvarint()
	if err != nil {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm field count: %v", err)
	}
	entryCount, err := cur.readUvarint()
	if err != nil {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry count: %v", err)
	}
	if entryCount > uint64(cur.remaining()+1) {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry count too large")
	}
	blockSize := checkedTextUint32(blockSizeRaw)
	fieldCount := checkedTextUint32(fieldCountRaw)
	value := textV2NormBlockValue{BlockStart: blockStart, BlockSize: blockSize, FieldCount: fieldCount, Entries: make([]textV2NormBlockEntry, 0, entryCount)}
	if err := validateTextV2BlockHeader(blockStart, blockSizeRaw, blockSize, "norm"); err != nil {
		return textV2NormBlockValue{}, err
	}
	if uint64(fieldCount) != fieldCountRaw || fieldCount == 0 {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm field count invalid")
	}
	if entryCount > 0 && fieldCountRaw > 0 && entryCount > uint64(cur.remaining())/fieldCountRaw {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm field count exceeds remaining payload")
	}
	var prev uint64
	for i := uint64(0); i < entryCount; i++ {
		ordinal, err := cur.readUvarint()
		if err != nil {
			return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry[%d] ordinal: %v", i, err)
		}
		generation, err := cur.readUvarint()
		if err != nil {
			return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry[%d] generation: %v", i, err)
		}
		if cur.remaining() == 0 {
			return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry[%d] missing flags", i)
		}
		flags := cur.buf[cur.pos]
		cur.pos++
		if err := validateTextV2BlockEntry(blockStart, blockSize, ordinal, generation, flags, prev, i, "norm"); err != nil {
			return textV2NormBlockValue{}, err
		}
		lengths := make([]uint32, 0, fieldCount)
		for j := uint32(0); j < fieldCount; j++ {
			length, err := cur.readUvarint()
			if err != nil {
				return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry[%d] field[%d] length: %v", i, j, err)
			}
			if uint64(checkedTextUint32(length)) != length {
				return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm entry[%d] field[%d] length overflows uint32", i, j)
			}
			lengths = append(lengths, uint32(length))
		}
		value.Entries = append(value.Entries, textV2NormBlockEntry{Ordinal: ordinal, Generation: generation, Flags: flags, FieldLengths: lengths})
		prev = ordinal
	}
	if cur.remaining() != 0 {
		return textV2NormBlockValue{}, errMalformedTextStorage("text-v2 norm trailing bytes")
	}
	return value, nil
}

func encodeTextV2CorpusStatsValue(value textV2CorpusStatsValue) []byte {
	out := []byte{textV2StatsValueVersion}
	out = appendTextUvarint(out, value.StatsGeneration)
	out = appendTextUvarint(out, value.DocumentCount)
	return out
}

func decodeTextV2CorpusStatsValue(raw []byte) (textV2CorpusStatsValue, error) {
	cur, err := textV2ValueCursor(raw, textV2StatsValueVersion, "corpus stats")
	if err != nil {
		return textV2CorpusStatsValue{}, err
	}
	generation, err := cur.readUvarint()
	if err != nil {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("text-v2 corpus stats generation: %v", err)
	}
	documents, err := cur.readUvarint()
	if err != nil {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("text-v2 corpus document count: %v", err)
	}
	if cur.remaining() != 0 {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("text-v2 corpus trailing bytes")
	}
	if generation == 0 {
		return textV2CorpusStatsValue{}, errMalformedTextStorage("text-v2 corpus generation cannot be zero")
	}
	return textV2CorpusStatsValue{StatsGeneration: generation, DocumentCount: documents}, nil
}

func encodeTextV2TermStatsValue(value textV2TermStatsValue) []byte {
	out := []byte{textV2StatsValueVersion}
	out = appendTextUvarint(out, value.StatsGeneration)
	out = appendTextUvarint(out, value.DocumentFrequency)
	out = appendTextUvarint(out, value.TotalTermFrequency)
	out = appendTextUvarint(out, value.PostingBlockCount)
	return out
}

func decodeTextV2TermStatsValue(raw []byte) (textV2TermStatsValue, error) {
	cur, err := textV2ValueCursor(raw, textV2StatsValueVersion, "term stats")
	if err != nil {
		return textV2TermStatsValue{}, err
	}
	generation, err := cur.readUvarint()
	if err != nil {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term stats generation: %v", err)
	}
	df, err := cur.readUvarint()
	if err != nil {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term document frequency: %v", err)
	}
	tf, err := cur.readUvarint()
	if err != nil {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term total frequency: %v", err)
	}
	blocks, err := cur.readUvarint()
	if err != nil {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term posting block count: %v", err)
	}
	if cur.remaining() != 0 {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term trailing bytes")
	}
	if generation == 0 {
		return textV2TermStatsValue{}, errMalformedTextStorage("text-v2 term generation cannot be zero")
	}
	return textV2TermStatsValue{StatsGeneration: generation, DocumentFrequency: df, TotalTermFrequency: tf, PostingBlockCount: blocks}, nil
}

func encodeTextV2FieldStatsValue(value textV2FieldStatsValue) []byte {
	out := []byte{textV2StatsValueVersion}
	out = appendTextUvarint(out, value.StatsGeneration)
	out = appendTextUvarint(out, value.DocumentCount)
	out = appendTextUvarint(out, value.TotalTokenCount)
	return out
}

func decodeTextV2FieldStatsValue(raw []byte) (textV2FieldStatsValue, error) {
	cur, err := textV2ValueCursor(raw, textV2StatsValueVersion, "field stats")
	if err != nil {
		return textV2FieldStatsValue{}, err
	}
	generation, err := cur.readUvarint()
	if err != nil {
		return textV2FieldStatsValue{}, errMalformedTextStorage("text-v2 field stats generation: %v", err)
	}
	documents, err := cur.readUvarint()
	if err != nil {
		return textV2FieldStatsValue{}, errMalformedTextStorage("text-v2 field document count: %v", err)
	}
	tokens, err := cur.readUvarint()
	if err != nil {
		return textV2FieldStatsValue{}, errMalformedTextStorage("text-v2 field token count: %v", err)
	}
	if cur.remaining() != 0 {
		return textV2FieldStatsValue{}, errMalformedTextStorage("text-v2 field trailing bytes")
	}
	if generation == 0 {
		return textV2FieldStatsValue{}, errMalformedTextStorage("text-v2 field generation cannot be zero")
	}
	return textV2FieldStatsValue{StatsGeneration: generation, DocumentCount: documents, TotalTokenCount: tokens}, nil
}

func textV2ValueCursor(raw []byte, wantVersion byte, name string) (textCursor, error) {
	if len(raw) == 0 {
		return textCursor{}, errMalformedTextStorage("empty text-v2 %s value", name)
	}
	if raw[0] != wantVersion {
		return textCursor{}, errUnsupportedTextStorageVersion("text-v2 "+name+" value", raw[0])
	}
	return textCursor{buf: raw[1:]}, nil
}

func validateTextV2BlockHeader(blockStart, blockSizeRaw uint64, blockSize uint32, name string) error {
	if blockStart == 0 {
		return errMalformedTextStorage("text-v2 %s block start cannot be zero", name)
	}
	if uint64(blockSize) != blockSizeRaw || blockSize == 0 {
		return errMalformedTextStorage("text-v2 %s block size invalid", name)
	}
	return nil
}

func validateTextV2BlockEntry(blockStart uint64, blockSize uint32, ordinal, generation uint64, flags byte, prev uint64, entryIndex uint64, name string) error {
	if ordinal == 0 || generation == 0 {
		return errMalformedTextStorage("text-v2 %s entry[%d] ordinal/generation cannot be zero", name, entryIndex)
	}
	if flags&^textV2DocFlagTombstone != 0 {
		return errMalformedTextStorage("text-v2 %s entry[%d] unsupported flags 0x%x", name, entryIndex, flags)
	}
	if ordinal < blockStart || ordinal >= blockStart+uint64(blockSize) {
		return errMalformedTextStorage("text-v2 %s entry[%d] ordinal %d outside block [%d,%d)", name, entryIndex, ordinal, blockStart, blockStart+uint64(blockSize))
	}
	if entryIndex > 0 && ordinal <= prev {
		return errMalformedTextStorage("text-v2 %s entry[%d] ordinal not strictly increasing", name, entryIndex)
	}
	return nil
}

func textV2OrdinalBlockStart(ordinal uint64, blockSize uint32) uint64 {
	if ordinal == 0 || blockSize == 0 {
		return 0
	}
	return ((ordinal - 1) / uint64(blockSize) * uint64(blockSize)) + 1
}

func (b *textV2DocMapBlockValue) upsert(entry textV2DocMapEntry) {
	if b == nil {
		return
	}
	entry.DocumentID = bytes.Clone(entry.DocumentID)
	for i := range b.Entries {
		if b.Entries[i].Ordinal == entry.Ordinal {
			b.Entries[i] = entry
			return
		}
	}
	b.Entries = append(b.Entries, entry)
	slices.SortFunc(b.Entries, func(a, b textV2DocMapEntry) int {
		return compareUint64(a.Ordinal, b.Ordinal)
	})
}

func (b textV2DocMapBlockValue) find(ordinal uint64) (textV2DocMapEntry, bool) {
	for _, entry := range b.Entries {
		if entry.Ordinal == ordinal {
			return textV2DocMapEntry{Ordinal: entry.Ordinal, Generation: entry.Generation, Flags: entry.Flags, DocumentID: bytes.Clone(entry.DocumentID)}, true
		}
	}
	return textV2DocMapEntry{}, false
}

func (b *textV2NormBlockValue) upsert(entry textV2NormBlockEntry) {
	if b == nil {
		return
	}
	entry.FieldLengths = append([]uint32(nil), entry.FieldLengths...)
	for i := range b.Entries {
		if b.Entries[i].Ordinal == entry.Ordinal {
			b.Entries[i] = entry
			return
		}
	}
	b.Entries = append(b.Entries, entry)
	slices.SortFunc(b.Entries, func(a, b textV2NormBlockEntry) int {
		return compareUint64(a.Ordinal, b.Ordinal)
	})
}

func (b textV2NormBlockValue) find(ordinal uint64) (textV2NormBlockEntry, bool) {
	for _, entry := range b.Entries {
		if entry.Ordinal == ordinal {
			return textV2NormBlockEntry{Ordinal: entry.Ordinal, Generation: entry.Generation, Flags: entry.Flags, FieldLengths: append([]uint32(nil), entry.FieldLengths...)}, true
		}
	}
	return textV2NormBlockEntry{}, false
}

func compareUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
