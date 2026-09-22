package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"slices"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	textV2KeyKindPostingBlock byte = 0x21

	textV2PostingBlockValueVersionV1 byte = 1
	textV2PostingBlockValueVersion   byte = 2
	textV2PostingBlockChecksumBytes       = 4

	// textV2PostingBlockTargetPostings keeps ordinary sealed blocks small enough
	// to stay cache-local in B-tree leaves. The root storage policy still owns the
	// physical inline-vs-value-log-leaf decision; oversized values remain ordinary
	// root values and therefore visible to existing TreeDB maintenance scanning.
	textV2PostingBlockTargetPostings    uint32 = 128
	textV2PostingBlockMicroPostings     uint32 = 32
	textV2PostingBlockMaxPostings       uint32 = 64 * 1024
	textV2PostingBlockMaxFieldCount     uint32 = 1024
	textV2PostingBlockInlineTargetBytes        = 16 << 10
	textV2PostingBlockMaxValueBytes            = 4 << 20

	textV2PostingBlockValueSupportedFlags byte = 0
	textV2PostingBlockEntrySupportedFlags byte = 0

	textV2PostingUpperBoundKindBM25FLaneMax byte = 1
)

type textV2PostingBlockKind byte

const (
	textV2PostingBlockKindSealed textV2PostingBlockKind = iota + 1
	textV2PostingBlockKindDelta
	textV2PostingBlockKindMicro
)

func (k textV2PostingBlockKind) String() string {
	switch k {
	case textV2PostingBlockKindSealed:
		return "sealed"
	case textV2PostingBlockKindDelta:
		return "delta"
	case textV2PostingBlockKindMicro:
		return "micro"
	default:
		return fmt.Sprintf("unknown(%d)", byte(k))
	}
}

type textV2PostingBlockKey struct {
	Term       string
	BlockStart uint64
	BlockID    uint64
}

type textV2PostingBlockSummary struct {
	FirstOrdinal            uint64
	LastOrdinal             uint64
	DocCount                uint32
	MaxTermFrequency        uint32
	MaxFieldTermFrequencies []uint32
	UpperBoundKind          byte
}

type textV2PostingBlockEntry struct {
	Ordinal          uint64
	Generation       uint64
	TermFrequency    uint32
	FieldFrequencies []uint32
	Flags            byte
}

type textV2PostingBlockValue struct {
	FormatVersion uint32
	Kind          textV2PostingBlockKind
	Flags         byte
	BlockStart    uint64
	BlockID       uint64
	Summary       textV2PostingBlockSummary
	Entries       []textV2PostingBlockEntry
}

type textV2PostingBlockKV struct {
	Key   []byte
	Value []byte
}

type textV2PostingBlockBuildOptions struct {
	Kind              textV2PostingBlockKind
	TargetPostings    uint32
	BlockIDStart      uint64
	FixedBlockID      bool
	InlineTargetBytes int
}

func textV2PostingBlockStoragePolicy(def TextIndexDefinition) (backenddb.OrderedRootStoragePolicy, error) {
	return backendRootStoragePolicy(def.StoragePolicy)
}

func encodeTextV2PostingBlockTermPrefix(term string) []byte {
	out := make([]byte, 0, 2+binary.MaxVarintLen64+len(term))
	out = append(out, textV2KeyVersion, textV2KeyKindPostingBlock)
	out = appendTextString(out, term)
	return out
}

func encodeTextV2PostingBlockKey(term string, blockStart, blockID uint64) []byte {
	out := encodeTextV2PostingBlockTermPrefix(term)
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], blockStart)
	binary.BigEndian.PutUint64(buf[8:16], blockID)
	out = append(out, buf[:]...)
	return out
}

func decodeTextV2PostingBlockKey(raw []byte) (textV2PostingBlockKey, error) {
	if len(raw) < 2 {
		return textV2PostingBlockKey{}, errMalformedTextStorage("short text-v2 posting block key")
	}
	if raw[0] != textV2KeyVersion {
		return textV2PostingBlockKey{}, errUnsupportedTextStorageVersion("text-v2 posting block key", raw[0])
	}
	if raw[1] != textV2KeyKindPostingBlock {
		return textV2PostingBlockKey{}, errMalformedTextStorage("text-v2 posting block key kind %d", raw[1])
	}
	cur := textCursor{buf: raw[2:]}
	term, err := cur.readString()
	if err != nil {
		return textV2PostingBlockKey{}, errMalformedTextStorage("text-v2 posting block key term: %v", err)
	}
	if cur.remaining() != 16 {
		return textV2PostingBlockKey{}, errMalformedTextStorage("text-v2 posting block key suffix length %d", cur.remaining())
	}
	blockStart := binary.BigEndian.Uint64(cur.buf[cur.pos : cur.pos+8])
	blockID := binary.BigEndian.Uint64(cur.buf[cur.pos+8 : cur.pos+16])
	if blockStart == 0 || blockID == 0 {
		return textV2PostingBlockKey{}, errMalformedTextStorage("text-v2 posting block key blockStart/blockID cannot be zero")
	}
	return textV2PostingBlockKey{Term: term, BlockStart: blockStart, BlockID: blockID}, nil
}

func decodeTextV2PostingBlockKeyForPrefix(raw, prefix []byte) (textV2PostingBlockKey, error) {
	if !bytes.HasPrefix(raw, prefix) {
		return textV2PostingBlockKey{}, errMalformedTextStorage("text-v2 posting block key outside requested term prefix")
	}
	return decodeTextV2PostingBlockKey(raw)
}

func decodeTextV2PostingBlockKeySuffixForPrefix(raw, prefix []byte) (uint64, uint64, error) {
	if !bytes.HasPrefix(raw, prefix) {
		return 0, 0, errMalformedTextStorage("text-v2 posting block key outside requested term prefix")
	}
	suffix := raw[len(prefix):]
	if len(suffix) != 16 {
		return 0, 0, errMalformedTextStorage("text-v2 posting block key suffix length %d", len(suffix))
	}
	blockStart := binary.BigEndian.Uint64(suffix[:8])
	blockID := binary.BigEndian.Uint64(suffix[8:])
	if blockStart == 0 || blockID == 0 {
		return 0, 0, errMalformedTextStorage("text-v2 posting block key blockStart/blockID cannot be zero")
	}
	return blockStart, blockID, nil
}

func encodeTextV2PostingBlockValue(value textV2PostingBlockValue) []byte {
	entries := cloneTextV2PostingBlockEntries(value.Entries)
	slices.SortFunc(entries, func(a, b textV2PostingBlockEntry) int {
		return compareUint64(a.Ordinal, b.Ordinal)
	})
	return encodeTextV2PostingBlockValueFromSorted(value, entries)
}

func encodeTextV2PostingBlockValueFromSorted(value textV2PostingBlockValue, entries []textV2PostingBlockEntry) []byte {
	fieldCount := uint32(len(value.Summary.MaxFieldTermFrequencies))
	out := make([]byte, 0, estimateTextV2PostingBlockValueLen(value, entries, fieldCount))
	out = append(out, textV2PostingBlockValueVersion)
	out = appendTextUvarint(out, uint64(value.FormatVersion))
	out = append(out, byte(value.Kind), value.Flags)
	out = appendTextUvarint(out, value.BlockStart)
	out = appendTextUvarint(out, value.BlockID)
	out = appendTextUvarint(out, value.Summary.FirstOrdinal)
	out = appendTextUvarint(out, value.Summary.LastOrdinal)
	out = appendTextUvarint(out, uint64(value.Summary.DocCount))
	out = appendTextUvarint(out, uint64(value.Summary.MaxTermFrequency))
	out = appendTextUvarint(out, uint64(fieldCount))
	for _, maxTF := range value.Summary.MaxFieldTermFrequencies {
		out = appendTextUvarint(out, uint64(maxTF))
	}
	out = append(out, value.Summary.UpperBoundKind)
	out = appendTextUvarint(out, uint64(len(entries)))
	prev := value.BlockStart - 1
	for _, entry := range entries {
		gap := entry.Ordinal - prev
		out = appendTextUvarint(out, gap)
		out = appendTextUvarint(out, entry.Generation)
		out = append(out, entry.Flags)
		out = appendTextUvarint(out, uint64(entry.TermFrequency))
		for _, freq := range entry.FieldFrequencies {
			out = appendTextUvarint(out, uint64(freq))
		}
		prev = entry.Ordinal
	}
	var checksum [textV2PostingBlockChecksumBytes]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(out))
	out = append(out, checksum[:]...)
	return out
}

func decodeTextV2PostingBlockValue(raw []byte) (textV2PostingBlockValue, error) {
	scanner, err := newTextV2PostingBlockEntryScanner(raw, nil)
	if err != nil {
		return textV2PostingBlockValue{}, err
	}
	value := scanner.block
	value.Entries = make([]textV2PostingBlockEntry, 0, scanner.block.Summary.DocCount)
	fieldCount := len(scanner.block.Summary.MaxFieldTermFrequencies)
	fieldArena := make([]uint32, 0, int(scanner.block.Summary.DocCount)*fieldCount)
	var entry textV2PostingBlockEntry
	for scanner.Next(&entry) {
		fieldOffset := len(fieldArena)
		fieldArena = append(fieldArena, entry.FieldFrequencies...)
		value.Entries = append(value.Entries, textV2PostingBlockEntry{
			Ordinal:          entry.Ordinal,
			Generation:       entry.Generation,
			TermFrequency:    entry.TermFrequency,
			FieldFrequencies: fieldArena[fieldOffset:len(fieldArena):len(fieldArena)],
			Flags:            entry.Flags,
		})
	}
	if err := scanner.Err(); err != nil {
		return textV2PostingBlockValue{}, err
	}
	return value, nil
}

func buildTextV2PostingBlockKVs(term string, entries []textV2PostingBlockEntry, fieldCount uint32, opts textV2PostingBlockBuildOptions) ([]textV2PostingBlockKV, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	if fieldCount == 0 || fieldCount > textV2PostingBlockMaxFieldCount {
		return nil, errMalformedTextStorage("text-v2 posting block builder invalid field count %d", fieldCount)
	}
	kind := opts.Kind
	if kind == 0 {
		kind = textV2PostingBlockKindSealed
	}
	if !isSupportedTextV2PostingBlockKind(kind) {
		return nil, errMalformedTextStorage("text-v2 posting block builder unsupported kind %d", byte(kind))
	}
	target := opts.TargetPostings
	if target == 0 {
		if kind == textV2PostingBlockKindMicro {
			target = textV2PostingBlockMicroPostings
		} else {
			target = textV2PostingBlockTargetPostings
		}
	}
	if target > textV2PostingBlockMaxPostings {
		target = textV2PostingBlockMaxPostings
	}
	inlineTarget := opts.InlineTargetBytes
	if inlineTarget <= 0 {
		inlineTarget = textV2PostingBlockInlineTargetBytes
	}
	blockID := opts.BlockIDStart
	if blockID == 0 {
		blockID = 1
	}
	sorted := cloneTextV2PostingBlockEntryHeaders(entries)
	slices.SortFunc(sorted, func(a, b textV2PostingBlockEntry) int { return compareUint64(a.Ordinal, b.Ordinal) })
	if err := validateTextV2PostingBlockBuilderEntries(sorted, fieldCount); err != nil {
		return nil, err
	}
	out := make([]textV2PostingBlockKV, 0, (len(sorted)+int(target)-1)/int(target))
	for start := 0; start < len(sorted); {
		chunkLen := min(int(target), len(sorted)-start)
		var block textV2PostingBlockValue
		var encoded []byte
		for {
			candidate, err := newTextV2PostingBlockValueFromSorted(kind, sorted[start:start+chunkLen], fieldCount, blockID)
			if err != nil {
				return nil, err
			}
			encoded = encodeTextV2PostingBlockValueFromSorted(candidate, candidate.Entries)
			if len(encoded) <= textV2PostingBlockMaxValueBytes && (len(encoded) <= inlineTarget || chunkLen == 1) {
				block = candidate
				break
			}
			if chunkLen == 1 {
				return nil, errMalformedTextStorage("text-v2 posting block encoded bytes %d exceed max %d", len(encoded), textV2PostingBlockMaxValueBytes)
			}
			chunkLen = max(1, chunkLen/2)
		}
		out = append(out, textV2PostingBlockKV{
			Key:   encodeTextV2PostingBlockKey(term, block.BlockStart, block.BlockID),
			Value: encoded,
		})
		start += chunkLen
		if !opts.FixedBlockID {
			if blockID == math.MaxUint64 {
				return nil, errMalformedTextStorage("text-v2 posting block id overflow")
			}
			blockID++
		}
	}
	return out, nil
}

type textV2PostingBlockEntryScanner struct {
	block        textV2PostingBlockValue
	cur          textCursor
	remaining    uint32
	seen         uint32
	prevOrdinal  uint64
	fieldScratch []uint32
	// scratch is caller-owned decode storage reused only within one iterator/query.
	scratch          []uint32
	computed         textV2PostingBlockSummary
	computedFieldTF  []uint32
	checksumVerified bool
	err              error
	finished         bool
}

func newTextV2PostingBlockEntryScanner(raw []byte, scratch []uint32) (*textV2PostingBlockEntryScanner, error) {
	return initTextV2PostingBlockEntryScanner(nil, raw, scratch)
}

func initTextV2PostingBlockEntryScanner(dst *textV2PostingBlockEntryScanner, raw []byte, scratch []uint32) (*textV2PostingBlockEntryScanner, error) {
	if len(raw) == 0 {
		return nil, errMalformedTextStorage("empty text-v2 posting block value")
	}
	if len(raw) > textV2PostingBlockMaxValueBytes {
		return nil, errMalformedTextStorage("text-v2 posting block value bytes %d exceed max %d", len(raw), textV2PostingBlockMaxValueBytes)
	}
	valueVersion := raw[0]
	if valueVersion != textV2PostingBlockValueVersion && valueVersion != textV2PostingBlockValueVersionV1 {
		return nil, errUnsupportedTextStorageVersion("text-v2 posting block value", raw[0])
	}
	payloadEnd := len(raw)
	checksumVerified := false
	if valueVersion == textV2PostingBlockValueVersion {
		if len(raw) <= textV2PostingBlockChecksumBytes {
			return nil, errMalformedTextStorage("short text-v2 posting block checksum")
		}
		payloadEnd -= textV2PostingBlockChecksumBytes
		wantChecksum := binary.BigEndian.Uint32(raw[payloadEnd:])
		gotChecksum := crc32.ChecksumIEEE(raw[:payloadEnd])
		if gotChecksum != wantChecksum {
			return nil, errMalformedTextStorage("text-v2 posting block checksum mismatch")
		}
		checksumVerified = true
	}
	cur := textCursor{buf: raw[1:payloadEnd]}
	formatVersion, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block format version: %v", err)
	}
	if formatVersion != uint64(textV2FormatVersion) {
		return nil, errMalformedTextStorage("unsupported text-v2 posting block format version %d", formatVersion)
	}
	if cur.remaining() < 2 {
		return nil, errMalformedTextStorage("text-v2 posting block missing kind/flags")
	}
	kind := textV2PostingBlockKind(cur.buf[cur.pos])
	cur.pos++
	if !isSupportedTextV2PostingBlockKind(kind) {
		return nil, errMalformedTextStorage("text-v2 posting block unsupported kind %d", byte(kind))
	}
	flags := cur.buf[cur.pos]
	cur.pos++
	if flags&^textV2PostingBlockValueSupportedFlags != 0 {
		return nil, errMalformedTextStorage("text-v2 posting block unsupported flags 0x%x", flags)
	}
	blockStart, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block start: %v", err)
	}
	blockID, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block id: %v", err)
	}
	first, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block first ordinal: %v", err)
	}
	last, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block last ordinal: %v", err)
	}
	docCountRaw, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block doc count: %v", err)
	}
	maxTFRaw, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block max term frequency: %v", err)
	}
	fieldCountRaw, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block field count: %v", err)
	}
	fieldCount := checkedTextUint32(fieldCountRaw)
	docCount := checkedTextUint32(docCountRaw)
	maxTF := checkedTextUint32(maxTFRaw)
	if blockStart == 0 || blockID == 0 || first == 0 || last == 0 {
		return nil, errMalformedTextStorage("text-v2 posting block zero block id or ordinal")
	}
	if first != blockStart || first > last {
		return nil, errMalformedTextStorage("text-v2 posting block invalid ordinal range [%d,%d] for start %d", first, last, blockStart)
	}
	if uint64(docCount) != docCountRaw || docCount == 0 || docCount > textV2PostingBlockMaxPostings {
		return nil, errMalformedTextStorage("text-v2 posting block doc count invalid")
	}
	if uint64(maxTF) != maxTFRaw || maxTF == 0 {
		return nil, errMalformedTextStorage("text-v2 posting block max term frequency invalid")
	}
	if uint64(fieldCount) != fieldCountRaw || fieldCount == 0 || fieldCount > textV2PostingBlockMaxFieldCount {
		return nil, errMalformedTextStorage("text-v2 posting block field count invalid")
	}
	fieldCountInt := int(fieldCount)
	scratchNeeded := fieldCountInt * 3
	if cap(scratch) < scratchNeeded {
		scratch = make([]uint32, scratchNeeded)
	}
	scratch = scratch[:scratchNeeded]
	fieldScratch := scratch[:fieldCountInt:fieldCountInt]
	maxFieldTF := scratch[fieldCountInt : fieldCountInt*2 : fieldCountInt*2]
	computedFieldTF := scratch[fieldCountInt*2 : scratchNeeded : scratchNeeded]
	clear(computedFieldTF)
	for i := uint32(0); i < fieldCount; i++ {
		fieldTF, err := cur.readUvarint()
		if err != nil {
			return nil, errMalformedTextStorage("text-v2 posting block max field frequency[%d]: %v", i, err)
		}
		if uint64(checkedTextUint32(fieldTF)) != fieldTF {
			return nil, errMalformedTextStorage("text-v2 posting block max field frequency[%d] overflows uint32", i)
		}
		maxFieldTF[i] = uint32(fieldTF)
	}
	if cur.remaining() == 0 {
		return nil, errMalformedTextStorage("text-v2 posting block missing upper-bound kind")
	}
	upperBoundKind := cur.buf[cur.pos]
	cur.pos++
	if upperBoundKind != textV2PostingUpperBoundKindBM25FLaneMax {
		return nil, errMalformedTextStorage("text-v2 posting block unsupported upper-bound kind %d", upperBoundKind)
	}
	entryCount, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-v2 posting block entry count: %v", err)
	}
	if entryCount != uint64(docCount) {
		return nil, errMalformedTextStorage("text-v2 posting block doc count %d does not match entry count %d", docCount, entryCount)
	}
	if entryCount > uint64(cur.remaining()+1) {
		return nil, errMalformedTextStorage("text-v2 posting block entry count too large")
	}
	minEntryBytes := uint64(fieldCount) + 4 // ordinal delta, generation, flags, term frequency, and one byte per field lane.
	if entryCount > uint64(cur.remaining())/minEntryBytes {
		return nil, errMalformedTextStorage("text-v2 posting block entry payload too short")
	}
	if dst == nil {
		dst = &textV2PostingBlockEntryScanner{}
	}
	*dst = textV2PostingBlockEntryScanner{
		block: textV2PostingBlockValue{
			FormatVersion: uint32(formatVersion),
			Kind:          kind,
			Flags:         flags,
			BlockStart:    blockStart,
			BlockID:       blockID,
			Summary: textV2PostingBlockSummary{
				FirstOrdinal:            first,
				LastOrdinal:             last,
				DocCount:                docCount,
				MaxTermFrequency:        maxTF,
				MaxFieldTermFrequencies: maxFieldTF,
				UpperBoundKind:          upperBoundKind,
			},
		},
		cur:              cur,
		remaining:        docCount,
		prevOrdinal:      blockStart - 1,
		fieldScratch:     fieldScratch,
		scratch:          scratch,
		computedFieldTF:  computedFieldTF,
		checksumVerified: checksumVerified,
	}
	return dst, nil
}

func (s *textV2PostingBlockEntryScanner) Summary() textV2PostingBlockSummary {
	if s == nil {
		return textV2PostingBlockSummary{}
	}
	return cloneTextV2PostingBlockSummary(s.block.Summary)
}

func (s *textV2PostingBlockEntryScanner) Block() textV2PostingBlockValue {
	if s == nil {
		return textV2PostingBlockValue{}
	}
	block := s.block
	block.Summary = cloneTextV2PostingBlockSummary(block.Summary)
	return block
}

func (s *textV2PostingBlockEntryScanner) ChecksumVerified() bool {
	return s != nil && s.checksumVerified
}

func (s *textV2PostingBlockEntryScanner) Next(dst *textV2PostingBlockEntry) bool {
	if s == nil || s.err != nil || s.finished {
		return false
	}
	if s.remaining == 0 {
		s.finish()
		return false
	}
	gap, err := s.cur.readUvarint()
	if err != nil {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] ordinal delta: %v", s.seen, err)
		return false
	}
	if gap == 0 || gap > math.MaxUint64-s.prevOrdinal {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] invalid ordinal delta", s.seen)
		return false
	}
	ordinal := s.prevOrdinal + gap
	generation, err := s.cur.readUvarint()
	if err != nil {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] generation: %v", s.seen, err)
		return false
	}
	if s.cur.remaining() == 0 {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] missing flags", s.seen)
		return false
	}
	flags := s.cur.buf[s.cur.pos]
	s.cur.pos++
	if flags&^textV2PostingBlockEntrySupportedFlags != 0 {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] unsupported flags 0x%x", s.seen, flags)
		return false
	}
	tfRaw, err := s.cur.readUvarint()
	if err != nil {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] term frequency: %v", s.seen, err)
		return false
	}
	tf := checkedTextUint32(tfRaw)
	if ordinal == 0 || generation == 0 || uint64(tf) != tfRaw || tf == 0 {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] invalid ordinal/generation/frequency", s.seen)
		return false
	}
	var fieldSum uint64
	for i := range s.fieldScratch {
		fieldTFRaw, err := s.cur.readUvarint()
		if err != nil {
			s.err = errMalformedTextStorage("text-v2 posting block entry[%d] field[%d] frequency: %v", s.seen, i, err)
			return false
		}
		fieldTF := checkedTextUint32(fieldTFRaw)
		if uint64(fieldTF) != fieldTFRaw {
			s.err = errMalformedTextStorage("text-v2 posting block entry[%d] field[%d] frequency overflows uint32", s.seen, i)
			return false
		}
		s.fieldScratch[i] = fieldTF
		fieldSum += uint64(fieldTF)
	}
	if fieldSum != uint64(tf) {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] field frequencies sum %d does not match term frequency %d", s.seen, fieldSum, tf)
		return false
	}
	if s.seen == 0 {
		s.computed.FirstOrdinal = ordinal
	} else if ordinal <= s.prevOrdinal {
		s.err = errMalformedTextStorage("text-v2 posting block entry[%d] ordinal not strictly increasing", s.seen)
		return false
	}
	s.computed.LastOrdinal = ordinal
	s.computed.DocCount++
	if tf > s.computed.MaxTermFrequency {
		s.computed.MaxTermFrequency = tf
	}
	for i, fieldTF := range s.fieldScratch {
		if fieldTF > s.computedFieldTF[i] {
			s.computedFieldTF[i] = fieldTF
		}
	}
	s.prevOrdinal = ordinal
	s.seen++
	s.remaining--
	if dst != nil {
		dst.Ordinal = ordinal
		dst.Generation = generation
		dst.TermFrequency = tf
		dst.FieldFrequencies = s.fieldScratch
		dst.Flags = flags
	}
	return true
}

func (s *textV2PostingBlockEntryScanner) Err() error {
	if s == nil {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	if !s.finished && s.remaining == 0 {
		s.finish()
	}
	return s.err
}

func (s *textV2PostingBlockEntryScanner) finish() {
	if s.finished {
		return
	}
	s.finished = true
	if s.cur.remaining() != 0 {
		s.err = errMalformedTextStorage("text-v2 posting block trailing bytes")
		return
	}
	s.computed.UpperBoundKind = s.block.Summary.UpperBoundKind
	s.computed.MaxFieldTermFrequencies = s.computedFieldTF
	if !textV2PostingBlockSummaryEqual(s.computed, s.block.Summary) {
		s.err = errMalformedTextStorage("text-v2 posting block summary/upper-bound metadata mismatch")
	}
}

func scanTextV2PostingBlocksForTerm(
	snap *backenddb.Snapshot,
	catalog *collectionCatalog,
	rootName string,
	term string,
	fn func(key textV2PostingBlockKey, summary textV2PostingBlockSummary, entries *textV2PostingBlockEntryScanner) error,
) error {
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil || it == nil {
		return err
	}
	defer func() { _ = it.Close() }()
	var scratch []uint32
	for it.Valid() {
		keyBytes := it.UnsafeKey()
		if !bytes.HasPrefix(keyBytes, prefix) {
			break
		}
		if it.IsDeleted() {
			it.Next()
			continue
		}
		key, err := decodeTextV2PostingBlockKeyForPrefix(keyBytes, prefix)
		if err != nil {
			return err
		}
		scanner, err := newTextV2PostingBlockEntryScanner(it.UnsafeValue(), scratch)
		if err != nil {
			return err
		}
		if scanner.block.BlockStart != key.BlockStart || scanner.block.BlockID != key.BlockID {
			return errMalformedTextStorage("text-v2 posting block key/value identity mismatch")
		}
		if err := fn(key, scanner.Summary(), scanner); err != nil {
			return err
		}
		var entry textV2PostingBlockEntry
		for scanner.Next(&entry) {
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		scratch = scanner.scratch
		it.Next()
	}
	return it.Error()
}

func validateTextV2PostingBlockBuilderEntries(entries []textV2PostingBlockEntry, fieldCount uint32) error {
	var prev uint64
	for i, entry := range entries {
		if entry.Ordinal == 0 || entry.Generation == 0 || entry.TermFrequency == 0 {
			return errMalformedTextStorage("text-v2 posting block entry[%d] invalid ordinal/generation/frequency", i)
		}
		if i > 0 && entry.Ordinal == prev {
			return errMalformedTextStorage("text-v2 posting block duplicate ordinal %d", entry.Ordinal)
		}
		if len(entry.FieldFrequencies) != int(fieldCount) {
			return errMalformedTextStorage("text-v2 posting block entry[%d] field count %d want %d", i, len(entry.FieldFrequencies), fieldCount)
		}
		var fieldSum uint64
		for _, fieldTF := range entry.FieldFrequencies {
			fieldSum += uint64(fieldTF)
		}
		if fieldSum != uint64(entry.TermFrequency) {
			return errMalformedTextStorage("text-v2 posting block entry[%d] field frequencies sum %d does not match term frequency %d", i, fieldSum, entry.TermFrequency)
		}
		prev = entry.Ordinal
	}
	return nil
}

func newTextV2PostingBlockValue(kind textV2PostingBlockKind, entries []textV2PostingBlockEntry, fieldCount uint32, blockID uint64) (textV2PostingBlockValue, error) {
	cloned := cloneTextV2PostingBlockEntries(entries)
	slices.SortFunc(cloned, func(a, b textV2PostingBlockEntry) int { return compareUint64(a.Ordinal, b.Ordinal) })
	return newTextV2PostingBlockValueFromSorted(kind, cloned, fieldCount, blockID)
}

// newTextV2PostingBlockValueFromSorted builds metadata around caller-owned
// entries that are already sorted by ordinal. Callers that retain the returned
// block must pass owned entries; encoding-only callers can pass a transient
// shallow copy to avoid per-entry field-frequency clones.
func newTextV2PostingBlockValueFromSorted(kind textV2PostingBlockKind, entries []textV2PostingBlockEntry, fieldCount uint32, blockID uint64) (textV2PostingBlockValue, error) {
	if len(entries) == 0 {
		return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block cannot be empty")
	}
	if uint64(len(entries)) > uint64(textV2PostingBlockMaxPostings) {
		return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entries exceed max")
	}
	if fieldCount == 0 || fieldCount > textV2PostingBlockMaxFieldCount {
		return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block invalid field count %d", fieldCount)
	}
	if blockID == 0 {
		return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block id cannot be zero")
	}
	if !isSupportedTextV2PostingBlockKind(kind) {
		return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block unsupported kind %d", byte(kind))
	}
	summary := textV2PostingBlockSummary{
		FirstOrdinal:            entries[0].Ordinal,
		LastOrdinal:             entries[len(entries)-1].Ordinal,
		DocCount:                uint32(len(entries)),
		MaxFieldTermFrequencies: make([]uint32, fieldCount),
		UpperBoundKind:          textV2PostingUpperBoundKindBM25FLaneMax,
	}
	var prev uint64
	for i, entry := range entries {
		if entry.Ordinal == 0 || entry.Generation == 0 || entry.TermFrequency == 0 {
			return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entry[%d] invalid ordinal/generation/frequency", i)
		}
		if entry.Flags&^textV2PostingBlockEntrySupportedFlags != 0 {
			return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entry[%d] unsupported flags 0x%x", i, entry.Flags)
		}
		if i > 0 && entry.Ordinal <= prev {
			return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entry[%d] duplicate or out-of-order ordinal", i)
		}
		if len(entry.FieldFrequencies) != int(fieldCount) {
			return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entry[%d] field count %d want %d", i, len(entry.FieldFrequencies), fieldCount)
		}
		var fieldSum uint64
		for j, fieldTF := range entry.FieldFrequencies {
			fieldSum += uint64(fieldTF)
			if fieldTF > summary.MaxFieldTermFrequencies[j] {
				summary.MaxFieldTermFrequencies[j] = fieldTF
			}
		}
		if fieldSum != uint64(entry.TermFrequency) {
			return textV2PostingBlockValue{}, errMalformedTextStorage("text-v2 posting block entry[%d] field frequencies sum %d does not match term frequency %d", i, fieldSum, entry.TermFrequency)
		}
		if entry.TermFrequency > summary.MaxTermFrequency {
			summary.MaxTermFrequency = entry.TermFrequency
		}
		prev = entry.Ordinal
	}
	return textV2PostingBlockValue{
		FormatVersion: textV2FormatVersion,
		Kind:          kind,
		BlockStart:    summary.FirstOrdinal,
		BlockID:       blockID,
		Summary:       summary,
		Entries:       entries,
	}, nil
}

func isSupportedTextV2PostingBlockKind(kind textV2PostingBlockKind) bool {
	switch kind {
	case textV2PostingBlockKindSealed, textV2PostingBlockKindDelta, textV2PostingBlockKindMicro:
		return true
	default:
		return false
	}
}

func estimateTextV2PostingBlockValueLen(value textV2PostingBlockValue, entries []textV2PostingBlockEntry, fieldCount uint32) int {
	n := 1 + 2 + 10*10 + int(fieldCount)*binary.MaxVarintLen64
	for _, entry := range entries {
		n += 3*binary.MaxVarintLen64 + 1 + len(entry.FieldFrequencies)*binary.MaxVarintLen64
	}
	return n + len(value.Summary.MaxFieldTermFrequencies) + textV2PostingBlockChecksumBytes
}

func cloneTextV2PostingBlockEntryHeaders(entries []textV2PostingBlockEntry) []textV2PostingBlockEntry {
	// Used by block builders that only need to sort and encode immediately; the
	// field-frequency slices remain read-only and are not retained in output KVs.
	return append([]textV2PostingBlockEntry(nil), entries...)
}

func cloneTextV2PostingBlockEntries(entries []textV2PostingBlockEntry) []textV2PostingBlockEntry {
	out := make([]textV2PostingBlockEntry, len(entries))
	for i, entry := range entries {
		out[i] = textV2PostingBlockEntry{
			Ordinal:          entry.Ordinal,
			Generation:       entry.Generation,
			TermFrequency:    entry.TermFrequency,
			FieldFrequencies: append([]uint32(nil), entry.FieldFrequencies...),
			Flags:            entry.Flags,
		}
	}
	return out
}

func cloneTextV2PostingBlockSummary(summary textV2PostingBlockSummary) textV2PostingBlockSummary {
	return textV2PostingBlockSummary{
		FirstOrdinal:            summary.FirstOrdinal,
		LastOrdinal:             summary.LastOrdinal,
		DocCount:                summary.DocCount,
		MaxTermFrequency:        summary.MaxTermFrequency,
		MaxFieldTermFrequencies: append([]uint32(nil), summary.MaxFieldTermFrequencies...),
		UpperBoundKind:          summary.UpperBoundKind,
	}
}

func textV2PostingBlockSummaryEqual(a, b textV2PostingBlockSummary) bool {
	return a.FirstOrdinal == b.FirstOrdinal &&
		a.LastOrdinal == b.LastOrdinal &&
		a.DocCount == b.DocCount &&
		a.MaxTermFrequency == b.MaxTermFrequency &&
		a.UpperBoundKind == b.UpperBoundKind &&
		slices.Equal(a.MaxFieldTermFrequencies, b.MaxFieldTermFrequencies)
}
