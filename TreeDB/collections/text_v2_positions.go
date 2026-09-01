package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	textV2KeyKindPosition byte = 0x22

	textV2PositionValueVersionV1 byte = 1
	textV2PositionValueVersion   byte = 2
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
	out = appendTextV2PositionTermBinding(out, value.Term)
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		out = appendTextUvarint(out, uint64(field.FieldIndex))
		out = appendTextUvarint(out, uint64(field.Frequency))
		out = appendTextV2PositionDeltas(out, field.Positions)
		out = appendTextOffsetSlice(out, field.Offsets)
	}
	return out
}

func decodeTextV2PositionValue(raw []byte) (textV2PositionValue, error) {
	return decodeTextV2PositionValueForTerm(raw, "")
}

func decodeTextV2PositionValueForTerm(raw []byte, keyTerm string) (textV2PositionValue, error) {
	if len(raw) == 0 {
		return textV2PositionValue{}, errMalformedTextStorage("empty text-v2 position value")
	}
	valueVersion := raw[0]
	if valueVersion != textV2PositionValueVersion && valueVersion != textV2PositionValueVersionV1 {
		return textV2PositionValue{}, errUnsupportedTextStorageVersion("text-v2 position value", raw[0])
	}
	if valueVersion == textV2PositionValueVersion && keyTerm == "" {
		return textV2PositionValue{}, errMalformedTextStorage("text-v2 position value v2 requires key term")
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
	term := keyTerm
	if valueVersion == textV2PositionValueVersion {
		if err := readTextV2PositionTermBinding(&cur, keyTerm); err != nil {
			return textV2PositionValue{}, err
		}
	}
	if valueVersion == textV2PositionValueVersionV1 {
		term, err = cur.readString()
		if err != nil {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position term: %v", err)
		}
		if keyTerm != "" && term != keyTerm {
			return textV2PositionValue{}, errMalformedTextStorage("text-v2 position key/value term mismatch")
		}
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
		var positions []uint32
		if valueVersion == textV2PositionValueVersionV1 {
			positions, err = cur.readUint32Slice()
		} else {
			positions, err = cur.readTextV2PositionDeltas()
		}
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

// textV2PositionValidation is built while TextIndexStorageStats scans the
// posting-block root. It caches one doc-map block while position keys advance
// by ordinal, avoiding repeated doc-map decodes without retaining every doc.
type textV2PositionValidation struct {
	postings map[textV2PositionPostingValidationKey]textV2PositionPostingValidationEntry
	docMap   *textV2DocMapBlockValue
}

type textV2PositionPostingValidationKey struct {
	term       string
	ordinal    uint64
	generation uint64
}

type textV2PositionPostingValidationEntry struct {
	posting   textV2SearchPostingValue
	duplicate bool
}

func newTextV2PositionValidation() *textV2PositionValidation {
	return &textV2PositionValidation{
		postings: make(map[textV2PositionPostingValidationKey]textV2PositionPostingValidationEntry),
	}
}

func (v *textV2PositionValidation) docMapCurrentAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName string, ordinal, generation uint64) (bool, error) {
	blockStart := textV2OrdinalBlockStart(ordinal, textV2DefaultDocMapBlockSize)
	if blockStart == 0 {
		return false, errMalformedTextStorage("text-v2 invalid position ordinal %d", ordinal)
	}
	if v == nil || v.docMap == nil || v.docMap.BlockStart != blockStart {
		raw, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, rootName, encodeTextV2BlockKey(blockStart), nil)
		if err != nil || !ok {
			return false, err
		}
		block, err := decodeTextV2DocMapBlockValue(raw)
		if err != nil {
			return false, err
		}
		if block.BlockStart != blockStart || block.BlockSize != textV2DefaultDocMapBlockSize {
			return false, errMalformedTextStorage("text-v2 position docmap block key/value mismatch")
		}
		if v != nil {
			v.docMap = &block
		}
	}
	if v == nil || v.docMap == nil {
		return false, nil
	}
	entry, ok := v.docMap.find(ordinal)
	return ok && !entry.tombstoned() && entry.Generation == generation, nil
}

func (v *textV2PositionValidation) add(term string, entry textV2PostingBlockEntry, fieldCount int) error {
	if v == nil {
		return nil
	}
	posting, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
	if err != nil {
		return err
	}
	key := textV2PositionPostingValidationKey{term: term, ordinal: entry.Ordinal, generation: entry.Generation}
	if existing, exists := v.postings[key]; exists {
		existing.duplicate = true
		v.postings[key] = existing
		return nil
	}
	v.postings[key] = textV2PositionPostingValidationEntry{posting: posting}
	return nil
}

func (v *textV2PositionValidation) lookup(term string, ordinal, generation uint64) (textV2SearchPostingValue, bool, bool) {
	if v == nil {
		return textV2SearchPostingValue{}, false, false
	}
	entry, ok := v.postings[textV2PositionPostingValidationKey{term: term, ordinal: ordinal, generation: generation}]
	return entry.posting, ok, entry.duplicate
}

func validateTextV2PositionEntryAtSnapshot(snap *backenddb.Snapshot, catalog *collectionCatalog, def TextIndexDefinition, key []byte, value textV2PositionValue, status textV2IndexStatusValue, validation *textV2PositionValidation) error {
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
	docMapCurrent, err := validation.docMapCurrentAtRoot(snap, catalog, collectionTextV2DocMapRootName(catalog.meta.Name, def.Name), value.Ordinal, value.Generation)
	if err != nil {
		return err
	}
	if !docMapCurrent {
		return errMalformedTextStorage("text-v2 position entry ordinal %d generation %d is not current", value.Ordinal, value.Generation)
	}
	posting, ok, duplicate := validation.lookup(value.Term, value.Ordinal, value.Generation)
	if duplicate {
		return errMalformedTextStorage("duplicate text-v2 scoring posting for position ordinal %d term %q generation %d", value.Ordinal, value.Term, value.Generation)
	}
	if !ok {
		return errMalformedTextStorage("text-v2 position entry ordinal %d term %q has no matching scoring posting", value.Ordinal, value.Term)
	}
	return validateTextV2PositionValueMatchesPosting(value, def, posting)
}

func readTextV2PositionPostingAtRoot(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string, ordinal, generation uint64, fieldCount int) (textV2SearchPostingValue, bool, error) {
	posting, found, _, err := readTextV2PositionPostingAtRootCounted(snap, catalog, rootName, term, ordinal, generation, fieldCount)
	return posting, found, err
}

func readTextV2PositionPostingAtRootCounted(snap *backenddb.Snapshot, catalog *collectionCatalog, rootName, term string, ordinal, generation uint64, fieldCount int) (textV2SearchPostingValue, bool, int, error) {
	prefix := encodeTextV2PostingBlockTermPrefix(term)
	it, err := collectionIteratorAtCatalogRoot(snap, catalog, rootName, prefix, textSearchPrefixEnd(prefix), true)
	if err != nil || it == nil {
		return textV2SearchPostingValue{}, false, 0, err
	}
	defer func() { _ = it.Close() }()
	var scratch []uint32
	var found bool
	var out textV2SearchPostingValue
	var scanned int
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
			return textV2SearchPostingValue{}, false, scanned, err
		}
		scanner, err := newTextV2PostingBlockEntryScanner(it.UnsafeValue(), scratch)
		if err != nil {
			return textV2SearchPostingValue{}, false, scanned, err
		}
		if scanner.block.BlockStart != key.BlockStart || scanner.block.BlockID != key.BlockID {
			return textV2SearchPostingValue{}, false, scanned, errMalformedTextStorage("text-v2 position posting block key/value identity mismatch")
		}
		if ordinal < scanner.block.Summary.FirstOrdinal || ordinal > scanner.block.Summary.LastOrdinal {
			var discard textV2PostingBlockEntry
			for scanner.Next(&discard) {
				scanned++
			}
			if err := scanner.Err(); err != nil {
				return textV2SearchPostingValue{}, false, scanned, err
			}
			scratch = scanner.scratch
			it.Next()
			continue
		}
		var entry textV2PostingBlockEntry
		for scanner.Next(&entry) {
			scanned++
			if entry.Ordinal != ordinal || entry.Generation != generation {
				continue
			}
			posting, err := textV2SearchPostingValueFromEntry(entry, fieldCount)
			if err != nil {
				return textV2SearchPostingValue{}, false, scanned, err
			}
			if found {
				return textV2SearchPostingValue{}, false, scanned, errMalformedTextStorage("duplicate text-v2 scoring posting for position ordinal %d term %q generation %d", ordinal, term, generation)
			}
			out = posting
			found = true
		}
		if err := scanner.Err(); err != nil {
			return textV2SearchPostingValue{}, false, scanned, err
		}
		scratch = scanner.scratch
		it.Next()
	}
	if err := it.Error(); err != nil {
		return textV2SearchPostingValue{}, false, scanned, err
	}
	return out, found, scanned, nil
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
	n := 1 + binary.MaxVarintLen64*4 + encodedTextV2PositionTermBindingLen(value.Term)
	for _, field := range fields {
		n += binary.MaxVarintLen64 * 2
		n += encodedTextV2PositionDeltasLen(field.Positions)
		n += encodedTextOffsetSliceLen(field.Offsets)
	}
	return n
}

const textV2PositionTermFingerprintSize = 5

func appendTextV2PositionTermBinding(out []byte, term string) []byte {
	out = appendTextUvarint(out, uint64(len(term)))
	fingerprint := textV2PositionTermFingerprint(term)
	return append(out, fingerprint[:]...)
}

func readTextV2PositionTermBinding(cur *textCursor, keyTerm string) error {
	termLen, err := cur.readUvarint()
	if err != nil {
		return errMalformedTextStorage("text-v2 position term length: %v", err)
	}
	if termLen != uint64(len(keyTerm)) {
		return errMalformedTextStorage("text-v2 position key/value term mismatch")
	}
	if cur.remaining() < textV2PositionTermFingerprintSize {
		return errMalformedTextStorage("text-v2 position term fingerprint: %v", io.ErrUnexpectedEOF)
	}
	termFingerprint := cur.buf[cur.pos : cur.pos+textV2PositionTermFingerprintSize]
	cur.pos += textV2PositionTermFingerprintSize
	wantFingerprint := textV2PositionTermFingerprint(keyTerm)
	if !bytes.Equal(termFingerprint, wantFingerprint[:]) {
		return errMalformedTextStorage("text-v2 position key/value term mismatch")
	}
	return nil
}

func encodedTextV2PositionTermBindingLen(term string) int {
	return textUvarintLen(uint64(len(term))) + textV2PositionTermFingerprintSize
}

func textV2PositionTermFingerprint(term string) [textV2PositionTermFingerprintSize]byte {
	// Store the exact term length plus a SHA-256/40 fingerprint so the compressed
	// v2 payload keeps an independent key/value term binding without
	// reintroducing the full duplicate term string.
	sum := sha256.Sum256([]byte(term))
	var out [textV2PositionTermFingerprintSize]byte
	copy(out[:], sum[:textV2PositionTermFingerprintSize])
	return out
}

func encodedTextV2PositionDeltasLen(values []uint32) int {
	n := textUvarintLen(uint64(len(values)))
	var prev uint32
	for i, value := range values {
		delta := value
		if i > 0 {
			if value <= prev {
				delta = 0
			} else {
				delta = value - prev
			}
		}
		n += textUvarintLen(uint64(delta))
		prev = value
	}
	return n
}

func appendTextV2PositionDeltas(dst []byte, values []uint32) []byte {
	dst = appendTextUvarint(dst, uint64(len(values)))
	var prev uint32
	for i, value := range values {
		delta := value
		if i > 0 {
			if value <= prev {
				delta = 0
			} else {
				delta = value - prev
			}
		}
		dst = appendTextUvarint(dst, uint64(delta))
		prev = value
	}
	return dst
}

func (c *textCursor) readTextV2PositionDeltas() ([]uint32, error) {
	count, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(c.remaining()+1) {
		return nil, errors.New("count too large")
	}
	out := make([]uint32, 0, count)
	var prev uint32
	for i := uint64(0); i < count; i++ {
		delta, err := c.readUvarint()
		if err != nil {
			return nil, err
		}
		if delta > uint64(^uint32(0)) {
			return nil, errors.New("uint32 overflow")
		}
		var value uint32
		if i == 0 {
			value = uint32(delta)
		} else {
			if delta == 0 {
				return nil, errors.New("non-increasing position delta")
			}
			if delta > uint64(^uint32(0)-prev) {
				return nil, errors.New("uint32 overflow")
			}
			value = prev + uint32(delta)
		}
		out = append(out, value)
		prev = value
	}
	return out, nil
}
