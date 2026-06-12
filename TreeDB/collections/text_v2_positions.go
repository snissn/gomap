package collections

import (
	"bytes"
	"encoding/binary"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	textV2KeyKindPosition byte = 0x22

	textV2PositionValueVersion byte = 1
)

type textV2PositionFieldValue struct {
	FieldIndex uint32
	Frequency  uint32
	Positions  []uint32
	Offsets    []textTokenOffset
}

type textV2PositionValue struct {
	FormatVersion uint32
	Ordinal       uint64
	Generation    uint64
	Term          string
	Fields        []textV2PositionFieldValue
}

func encodeTextV2PositionKey(ordinal uint64, term string) []byte {
	out := make([]byte, 0, 2+8+binary.MaxVarintLen64+len(term))
	out = append(out, textV2KeyVersion, textV2KeyKindPosition)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], ordinal)
	out = append(out, buf[:]...)
	out = appendTextString(out, term)
	return out
}

func decodeTextV2PositionKey(raw []byte) (uint64, string, error) {
	if len(raw) < 10 {
		return 0, "", errMalformedTextStorage("short text-v2 position key")
	}
	if raw[0] != textV2KeyVersion {
		return 0, "", errUnsupportedTextStorageVersion("text-v2 position key", raw[0])
	}
	if raw[1] != textV2KeyKindPosition {
		return 0, "", errMalformedTextStorage("text-v2 position key kind %d", raw[1])
	}
	ordinal := binary.BigEndian.Uint64(raw[2:10])
	if ordinal == 0 {
		return 0, "", errMalformedTextStorage("text-v2 position key has zero ordinal")
	}
	cur := textCursor{buf: raw[10:]}
	term, err := cur.readString()
	if err != nil {
		return 0, "", errMalformedTextStorage("text-v2 position key term: %v", err)
	}
	if term == "" {
		return 0, "", errMalformedTextStorage("text-v2 position key missing term")
	}
	if cur.remaining() != 0 {
		return 0, "", errMalformedTextStorage("text-v2 position key trailing bytes")
	}
	return ordinal, term, nil
}

func encodeTextV2PositionValue(value textV2PositionValue) []byte {
	fields := cloneTextV2PositionFields(value.Fields)
	sort.Slice(fields, func(i, j int) bool { return fields[i].FieldIndex < fields[j].FieldIndex })
	out := make([]byte, 0, estimateTextV2PositionValueLen(value, fields))
	out = append(out, textV2PositionValueVersion)
	out = appendTextUvarint(out, uint64(value.FormatVersion))
	out = appendTextUvarint(out, value.Ordinal)
	out = appendTextUvarint(out, value.Generation)
	out = appendTextString(out, value.Term)
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		out = appendTextUvarint(out, uint64(field.FieldIndex))
		out = appendTextUvarint(out, uint64(field.Frequency))
		out = appendTextUint32Slice(out, field.Positions)
		out = appendTextOffsetSlice(out, field.Offsets)
	}
	return out
}

func decodeTextV2PositionValue(raw []byte) (textV2PositionValue, error) {
	if len(raw) == 0 {
		return textV2PositionValue{}, errMalformedTextStorage("empty text-v2 position value")
	}
	if raw[0] != textV2PositionValueVersion {
		return textV2PositionValue{}, errUnsupportedTextStorageVersion("text-v2 position value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	formatVersion, err := cur.readUvarint()
	if err != nil {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position format version: %v", err)
	}
	if formatVersion != uint64(textV2FormatVersion) {
		return textV2PositionValue{}, errMalformedTextStorage("unsupported text-v2 position format version %d", formatVersion)
	}
	ordinal, err := cur.readUvarint()
	if err != nil {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position ordinal: %v", err)
	}
	generation, err := cur.readUvarint()
	if err != nil {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position generation: %v", err)
	}
	term, err := cur.readString()
	if err != nil {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position term: %v", err)
	}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field count: %v", err)
	}
	if ordinal == 0 || generation == 0 || term == "" {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position value missing ordinal/generation/term")
	}
	if fieldCount == 0 || fieldCount > uint64(textV2PostingBlockMaxFieldCount) || fieldCount > uint64(cur.remaining()+1) {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field count invalid")
	}
	value := textV2PositionValue{FormatVersion: uint32(formatVersion), Ordinal: ordinal, Generation: generation, Term: term, Fields: make([]textV2PositionFieldValue, 0, int(fieldCount))}
	var prevField uint32
	for i := uint64(0); i < fieldCount; i++ {
		fieldIndexRaw, err := cur.readUvarint()
		if err != nil {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] index: %v", i, err)
		}
		frequencyRaw, err := cur.readUvarint()
		if err != nil {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] frequency: %v", i, err)
		}
		positions, err := cur.readUint32Slice()
		if err != nil {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] positions: %v", i, err)
		}
		offsets, err := cur.readOffsetSlice()
		if err != nil {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] offsets: %v", i, err)
		}
		fieldIndex := checkedTextUint32(fieldIndexRaw)
		frequency := checkedTextUint32(frequencyRaw)
		if uint64(fieldIndex) != fieldIndexRaw || uint64(frequency) != frequencyRaw || frequency == 0 {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] invalid index/frequency", i)
		}
		if i > 0 && fieldIndex <= prevField {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position fields not strictly increasing")
		}
		if uint32(len(positions)) != frequency {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] position count %d does not match frequency %d", i, len(positions), frequency)
		}
		if len(offsets) != 0 && len(offsets) != len(positions) {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] offsets/positions length mismatch", i)
		}
		for j := 1; j < len(positions); j++ {
			if positions[j] <= positions[j-1] {
				return textV2PositionValue{}, errMalformedTextStorage("text-v2 position field[%d] positions not strictly increasing", i)
			}
		}
		value.Fields = append(value.Fields, textV2PositionFieldValue{FieldIndex: fieldIndex, Frequency: frequency, Positions: positions, Offsets: offsets})
		prevField = fieldIndex
	}
	if cur.remaining() != 0 {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position trailing bytes")
	}
	return value, nil
}

func deleteTextV2PositionEntriesForDocument(table memtable.Table, def TextIndexDefinition, ordinal uint64, state textDocumentStateValue) int {
	if table == nil || !def.StorePositions || ordinal == 0 {
		return 0
	}
	terms := make(map[string]struct{})
	for _, field := range state.Fields {
		for _, term := range field.Terms {
			if term.Term != "" {
				terms[term.Term] = struct{}{}
			}
		}
	}
	ordered := make([]string, 0, len(terms))
	for term := range terms {
		ordered = append(ordered, term)
	}
	sort.Strings(ordered)
	for _, term := range ordered {
		table.DeleteSteal(encodeTextV2PositionKey(ordinal, term))
	}
	return len(ordered)
}

func addTextV2PositionEntriesForDocument(table memtable.Table, def TextIndexDefinition, ordinal, generation uint64, analysis textAnalyzedDocument) (int, uint64, error) {
	if table == nil || !def.StorePositions {
		return 0, 0, nil
	}
	if ordinal == 0 || generation == 0 {
		return 0, 0, errMalformedTextStorage("text-v2 position document ordinal/generation cannot be zero")
	}
	byTerm := make(map[string][]textV2PositionFieldValue)
	for _, field := range analysis.Fields {
		fieldIndex := textV2FieldIndex(def, field.Field)
		if fieldIndex < 0 {
			return 0, 0, errMalformedTextStorage("text-v2 position field %q missing from definition", field.Field)
		}
		terms := make([]string, 0, len(field.Terms))
		for term := range field.Terms {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		for _, termName := range terms {
			term := field.Terms[termName]
			if term == nil || term.Frequency == 0 {
				continue
			}
			if uint32(len(term.Positions)) != term.Frequency {
				return 0, 0, errMalformedTextStorage("text-v2 position term %q field %q positions=%d frequency=%d", termName, field.Field, len(term.Positions), term.Frequency)
			}
			offsets := term.Offsets
			if !def.StoreOffsets {
				offsets = nil
			} else if uint32(len(offsets)) != term.Frequency {
				return 0, 0, errMalformedTextStorage("text-v2 position term %q field %q offsets=%d frequency=%d", termName, field.Field, len(offsets), term.Frequency)
			}
			byTerm[termName] = append(byTerm[termName], textV2PositionFieldValue{
				FieldIndex: uint32(fieldIndex),
				Frequency:  term.Frequency,
				Positions:  append([]uint32(nil), term.Positions...),
				Offsets:    append([]textTokenOffset(nil), offsets...),
			})
		}
	}
	terms := make([]string, 0, len(byTerm))
	for term := range byTerm {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	var bytesWritten uint64
	for _, term := range terms {
		key := encodeTextV2PositionKey(ordinal, term)
		value := encodeTextV2PositionValue(textV2PositionValue{FormatVersion: textV2FormatVersion, Ordinal: ordinal, Generation: generation, Term: term, Fields: byTerm[term]})
		table.SetSteal(key, value)
		bytesWritten += uint64(len(key) + len(value))
	}
	return len(terms), bytesWritten, nil
}

func validateTextV2PositionEntryAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, key []byte, value textV2PositionValue, status textV2IndexStatusValue) error {
	if catalog == nil {
		return errCollectionNotFound
	}
	ordinal, term, err := decodeTextV2PositionKey(key)
	if err != nil {
		return err
	}
	if value.Ordinal != ordinal || value.Term != term {
		return errMalformedTextStorage("text-v2 position key/value mismatch")
	}
	if value.Ordinal >= status.NextOrdinal || value.Generation > status.RootGeneration {
		return errMalformedTextStorage("text-v2 position entry outside status snapshot")
	}
	if err := validateTextV2PositionValueForDefinition(value, def); err != nil {
		return err
	}
	docMap, ok, err := readTextV2PositionDocMapEntryAtRoot(snap, catalog, collectionTextV2DocMapRootName(catalog.meta.Name, def.Name), value.Ordinal)
	if err != nil {
		return err
	}
	if !ok || docMap.tombstoned() || docMap.Generation != value.Generation {
		return errMalformedTextStorage("text-v2 position entry ordinal %d generation %d is not current", value.Ordinal, value.Generation)
	}
	posting, ok, err := readTextV2PositionPostingAtRoot(snap, catalog, collectionTextV2PostingBlocksRootName(catalog.meta.Name, def.Name), value.Term, value.Ordinal, value.Generation, len(def.Fields))
	if err != nil {
		return err
	}
	if !ok {
		return errMalformedTextStorage("text-v2 position entry ordinal %d term %q has no matching scoring posting", value.Ordinal, value.Term)
	}
	return validateTextV2PositionValueMatchesPosting(value, def, posting)
}

func readTextV2PositionDocMapEntryAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, ordinal uint64) (textV2DocMapEntry, bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
	if blockStart == 0 {
		return textV2DocMapEntry{}, false, errMalformedTextStorage("text-v2 invalid position ordinal %d", ordinal)
	}
	raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2BlockKey(blockStart), nil)
	if err != nil || !ok {
		return textV2DocMapEntry{}, false, err
	}
	block, err := decodeTextV2DocMapBlockValue(raw)
	if err != nil {
		return textV2DocMapEntry{}, false, err
	}
	if block.BlockStart != blockStart || block.BlockSize != textV2DefaultDocMapBlockSize {
		return textV2DocMapEntry{}, false, errMalformedTextStorage("text-v2 position docmap block key/value mismatch")
	}
	entry, found := block.find(ordinal)
	return entry, found, nil
}

func readTextV2PositionPostingAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string, ordinal, generation uint64, fieldCount int) (textV2SearchPostingValue, bool, error) {
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil || it == nil {
		return textV2SearchPostingValue{}, false, err
	}
	defer func() { _ = it.Close() }()
	var scratch []uint32
	var found bool
	var out textV2SearchPostingValue
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
			return textV2SearchPostingValue{}, false, err
		}
		scanner, err := newTextV2PostingBlockEntryScanner(it.UnsafeValue(), scratch)
		if err != nil {
			return textV2SearchPostingValue{}, false, err
		}
		if scanner.block.BlockStart != key.BlockStart || scanner.block.BlockID != key.BlockID {
			return textV2SearchPostingValue{}, false, errMalformedTextStorage("text-v2 position posting block key/value identity mismatch")
		}
		if ordinal < scanner.block.Summary.FirstOrdinal || ordinal > scanner.block.Summary.LastOrdinal {
			var discard textV2PostingBlockEntry
			for scanner.Next(&discard) {
			}
			if err := scanner.Err(); err != nil {
				return textV2SearchPostingValue{}, false, err
			}
			scratch = scanner.fieldScratch
			it.Next()
			continue
		}
		var entry textV2PostingBlockEntry
		for scanner.Next(&entry) {
			if entry.Ordinal != ordinal || entry.Generation != generation {
				continue
			}
			posting, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
			if err != nil {
				return textV2SearchPostingValue{}, false, err
			}
			if found {
				return textV2SearchPostingValue{}, false, errMalformedTextStorage("duplicate text-v2 scoring posting for position ordinal %d term %q generation %d", ordinal, term, generation)
			}
			out = posting
			found = true
		}
		if err := scanner.Err(); err != nil {
			return textV2SearchPostingValue{}, false, err
		}
		scratch = scanner.fieldScratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return textV2SearchPostingValue{}, false, err
	}
	return out, found, nil
}

func validateTextV2PositionValueForDefinition(value textV2PositionValue, def TextIndexDefinition) error {
	if !def.StorePositions {
		return errMalformedTextStorage("text-v2 position entry present for index without store_positions")
	}
	if len(value.Fields) == 0 {
		return errMalformedTextStorage("text-v2 position value has no fields")
	}
	for _, field := range value.Fields {
		if int(field.FieldIndex) >= len(def.Fields) {
			return errMalformedTextStorage("text-v2 position field index %d outside field count %d", field.FieldIndex, len(def.Fields))
		}
		if uint32(len(field.Positions)) != field.Frequency {
			return errMalformedTextStorage("text-v2 position field %d position count %d does not match frequency %d", field.FieldIndex, len(field.Positions), field.Frequency)
		}
		if def.StoreOffsets {
			if uint32(len(field.Offsets)) != field.Frequency {
				return errMalformedTextStorage("text-v2 position field %d offset count %d does not match frequency %d", field.FieldIndex, len(field.Offsets), field.Frequency)
			}
		} else if len(field.Offsets) != 0 {
			return errMalformedTextStorage("text-v2 position field %d has offsets for index without store_offsets", field.FieldIndex)
		}
	}
	return nil
}

func validateTextV2PositionValueMatchesPosting(value textV2PositionValue, def TextIndexDefinition, posting textV2SearchPostingValue) error {
	if err := validateTextV2PositionValueForDefinition(value, def); err != nil {
		return err
	}
	seen := make(map[uint32]struct{}, len(value.Fields))
	for _, field := range value.Fields {
		if _, dup := seen[field.FieldIndex]; dup {
			return errMalformedTextStorage("text-v2 position duplicate field index %d", field.FieldIndex)
		}
		seen[field.FieldIndex] = struct{}{}
		freq := posting.fieldFrequency(int(field.FieldIndex))
		if freq == 0 {
			return errMalformedTextStorage("text-v2 position field %d not present in scoring posting", field.FieldIndex)
		}
		if field.Frequency != freq {
			return errMalformedTextStorage("text-v2 position field %d frequency %d does not match scoring frequency %d", field.FieldIndex, field.Frequency, freq)
		}
	}
	for fieldIdx := 0; fieldIdx < posting.fieldCount; fieldIdx++ {
		if posting.fieldFrequency(fieldIdx) == 0 {
			continue
		}
		if _, ok := seen[uint32(fieldIdx)]; !ok {
			return errMalformedTextStorage("text-v2 position missing field %d present in scoring posting", fieldIdx)
		}
	}
	return nil
}

func cloneTextV2PositionFields(fields []textV2PositionFieldValue) []textV2PositionFieldValue {
	if len(fields) == 0 {
		return nil
	}
	out := make([]textV2PositionFieldValue, len(fields))
	for i := range fields {
		out[i] = fields[i]
		out[i].Positions = append([]uint32(nil), fields[i].Positions...)
		out[i].Offsets = append([]textTokenOffset(nil), fields[i].Offsets...)
	}
	return out
}

func estimateTextV2PositionValueLen(value textV2PositionValue, fields []textV2PositionFieldValue) int {
	n := 1 + binary.MaxVarintLen64*4 + len(value.Term)
	for _, field := range fields {
		n += binary.MaxVarintLen64 * 2
		n += encodedTextUint32SliceLen(field.Positions)
		n += encodedTextOffsetSliceLen(field.Offsets)
	}
	return n
}
