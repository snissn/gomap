package collections

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

var ErrTextIndexStorageCorrupt = errors.New("collections: malformed text index storage")

const (
	textPostingKeyVersion byte = 1
	textStateKeyVersion   byte = 1
	textStatsKeyVersion   byte = 1

	textPostingValueVersion byte = 1
	textStateValueVersion   byte = 1
	textStatsValueVersion   byte = 1

	textStatsKeyKindCorpus byte = 1
	textStatsKeyKindTerm   byte = 2
	textStatsKeyKindField  byte = 3
)

type textTokenOffset struct {
	Start uint32
	End   uint32
}

type textPostingValue struct {
	TermFrequency uint32
	Fields        []textPostingFieldValue
}

type textSearchPostingValue struct {
	TermFrequency uint32
	fields        [2]textSearchPostingFieldValue
	overflow      []textSearchPostingFieldValue
	fieldsN       int
}

type textSearchPostingFieldValue struct {
	Field     string
	Frequency uint32
}

func (v *textSearchPostingValue) addField(field textSearchPostingFieldValue) {
	if v.fieldsN < len(v.fields) {
		v.fields[v.fieldsN] = field
	} else {
		v.overflow = append(v.overflow, field)
	}
	v.fieldsN++
}

func (v textSearchPostingValue) fieldCount() int { return v.fieldsN }

func (v textSearchPostingValue) fieldAt(i int) textSearchPostingFieldValue {
	if i < len(v.fields) {
		return v.fields[i]
	}
	return v.overflow[i-len(v.fields)]
}

type textPostingFieldValue struct {
	Field     string
	Frequency uint32
	Positions []uint32
	Offsets   []textTokenOffset
}

type textDocumentStateValue struct {
	Fields []textDocumentFieldState
}

type textDocumentFieldState struct {
	Field  string
	Length uint32
	Terms  []textDocumentTermState
}

type textDocumentTermState struct {
	Term      string
	Frequency uint32
	Positions []uint32
	Offsets   []textTokenOffset
}

type textStatsCorpusValue struct {
	DocumentCount uint64
}

type textStatsTermValue struct {
	DocumentFrequency  uint64
	TotalTermFrequency uint64
}

type textStatsFieldValue struct {
	DocumentCount   uint64
	TotalTokenCount uint64
}

func compareTextStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func encodeTextPostingKey(term string, documentID []byte) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(term)+len(documentID))
	out = append(out, textPostingKeyVersion)
	out = appendTextUvarint(out, uint64(len(term)))
	out = append(out, term...)
	out = append(out, documentID...)
	return out
}

func encodeTextPostingTermPrefix(term string) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64+len(term))
	out = append(out, textPostingKeyVersion)
	out = appendTextUvarint(out, uint64(len(term)))
	out = append(out, term...)
	return out
}

func decodeTextPostingKey(raw []byte) (string, []byte, error) {
	if len(raw) == 0 {
		return "", nil, errMalformedTextStorage("empty postings key")
	}
	if raw[0] != textPostingKeyVersion {
		return "", nil, errUnsupportedTextStorageVersion("postings key", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	termBytes, err := cur.readBytes()
	if err != nil {
		return "", nil, errMalformedTextStorage("postings key term: %v", err)
	}
	if cur.remaining() == 0 {
		return "", nil, errMalformedTextStorage("postings key missing document id")
	}
	return string(termBytes), bytes.Clone(cur.buf[cur.pos:]), nil
}

func decodeTextPostingKeyDocumentIDForPrefix(raw, prefix []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errMalformedTextStorage("empty postings key")
	}
	if !bytes.HasPrefix(raw, prefix) {
		return nil, errMalformedTextStorage("postings key outside requested term prefix")
	}
	if len(raw) == len(prefix) {
		return nil, errMalformedTextStorage("postings key missing document id")
	}
	return raw[len(prefix):], nil
}

func encodeTextStateKey(documentID []byte) []byte {
	out := make([]byte, 0, 1+len(documentID))
	out = append(out, textStateKeyVersion)
	out = append(out, documentID...)
	return out
}

func appendTextStateKeyString(dst []byte, documentID string) []byte {
	dst = append(dst, textStateKeyVersion)
	return append(dst, documentID...)
}

func decodeTextStateKey(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errMalformedTextStorage("empty text-state key")
	}
	if raw[0] != textStateKeyVersion {
		return nil, errUnsupportedTextStorageVersion("text-state key", raw[0])
	}
	if len(raw) == 1 {
		return nil, errMalformedTextStorage("text-state key missing document id")
	}
	return bytes.Clone(raw[1:]), nil
}

func encodeTextStatsCorpusKey() []byte {
	return []byte{textStatsKeyVersion, textStatsKeyKindCorpus}
}

func encodeTextStatsTermKey(term string) []byte {
	out := []byte{textStatsKeyVersion, textStatsKeyKindTerm}
	out = appendTextBytes(out, []byte(term))
	return out
}

func encodeTextStatsFieldKey(field string) []byte {
	out := []byte{textStatsKeyVersion, textStatsKeyKindField}
	out = appendTextBytes(out, []byte(field))
	return out
}

type textStatsKey struct {
	Kind  byte
	Value string
}

func decodeTextStatsKey(raw []byte) (textStatsKey, error) {
	if len(raw) < 2 {
		return textStatsKey{}, errMalformedTextStorage("short text-stats key")
	}
	if raw[0] != textStatsKeyVersion {
		return textStatsKey{}, errUnsupportedTextStorageVersion("text-stats key", raw[0])
	}
	key := textStatsKey{Kind: raw[1]}
	cur := textCursor{buf: raw[2:]}
	switch key.Kind {
	case textStatsKeyKindCorpus:
		if cur.remaining() != 0 {
			return textStatsKey{}, errMalformedTextStorage("corpus text-stats key has trailing bytes")
		}
	case textStatsKeyKindTerm, textStatsKeyKindField:
		value, err := cur.readBytes()
		if err != nil {
			return textStatsKey{}, errMalformedTextStorage("text-stats key value: %v", err)
		}
		if cur.remaining() != 0 {
			return textStatsKey{}, errMalformedTextStorage("text-stats key has trailing bytes")
		}
		key.Value = string(value)
	default:
		return textStatsKey{}, errMalformedTextStorage("unsupported text-stats key kind %d", key.Kind)
	}
	return key, nil
}

func encodeTextPostingValue(value textPostingValue) []byte {
	fields := value.Fields
	if len(fields) > 1 {
		var stack [4]textPostingFieldValue
		if len(fields) <= len(stack) {
			fields = stack[:len(fields)]
			copy(fields, value.Fields)
		} else {
			fields = append([]textPostingFieldValue(nil), value.Fields...)
		}
		slices.SortFunc(fields, func(a, b textPostingFieldValue) int { return compareTextStrings(a.Field, b.Field) })
	}
	out := make([]byte, 0, encodedTextPostingValueLen(value, fields))
	out = append(out, textPostingValueVersion)
	out = appendTextUvarint(out, uint64(value.TermFrequency))
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		out = appendTextString(out, field.Field)
		out = appendTextUvarint(out, uint64(field.Frequency))
		out = appendTextUint32Slice(out, field.Positions)
		out = appendTextOffsetSlice(out, field.Offsets)
	}
	return out
}

func decodeTextPostingValue(raw []byte) (textPostingValue, error) {
	if len(raw) == 0 {
		return textPostingValue{}, errMalformedTextStorage("empty postings value")
	}
	if raw[0] != textPostingValueVersion {
		return textPostingValue{}, errUnsupportedTextStorageVersion("postings value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	freq, err := cur.readUvarint()
	if err != nil {
		return textPostingValue{}, errMalformedTextStorage("postings value frequency: %v", err)
	}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return textPostingValue{}, errMalformedTextStorage("postings value field count: %v", err)
	}
	value := textPostingValue{TermFrequency: checkedTextUint32(freq)}
	if uint64(value.TermFrequency) != freq {
		return textPostingValue{}, errMalformedTextStorage("postings value frequency overflows uint32")
	}
	if fieldCount > uint64(cur.remaining()+1) {
		return textPostingValue{}, errMalformedTextStorage("postings value field count too large")
	}
	value.Fields = make([]textPostingFieldValue, 0, fieldCount)
	for i := uint64(0); i < fieldCount; i++ {
		field, err := cur.readString()
		if err != nil {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] name: %v", i, err)
		}
		fieldFreq, err := cur.readUvarint()
		if err != nil {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] frequency: %v", i, err)
		}
		positions, err := cur.readUint32Slice()
		if err != nil {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] positions: %v", i, err)
		}
		offsets, err := cur.readOffsetSlice()
		if err != nil {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] offsets: %v", i, err)
		}
		if uint64(checkedTextUint32(fieldFreq)) != fieldFreq {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] frequency overflows uint32", i)
		}
		if len(offsets) != 0 && len(offsets) != len(positions) {
			return textPostingValue{}, errMalformedTextStorage("postings value field[%d] offsets/positions length mismatch", i)
		}
		value.Fields = append(value.Fields, textPostingFieldValue{
			Field:     field,
			Frequency: uint32(fieldFreq),
			Positions: positions,
			Offsets:   offsets,
		})
	}
	if cur.remaining() != 0 {
		return textPostingValue{}, errMalformedTextStorage("postings value trailing bytes")
	}
	return value, nil
}

func decodeTextPostingValueForSearch(raw []byte, fieldNames []string) (textSearchPostingValue, error) {
	if len(raw) == 0 {
		return textSearchPostingValue{}, errMalformedTextStorage("empty postings value")
	}
	if raw[0] != textPostingValueVersion {
		return textSearchPostingValue{}, errUnsupportedTextStorageVersion("postings value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	freq, err := cur.readUvarint()
	if err != nil {
		return textSearchPostingValue{}, errMalformedTextStorage("postings value frequency: %v", err)
	}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return textSearchPostingValue{}, errMalformedTextStorage("postings value field count: %v", err)
	}
	value := textSearchPostingValue{TermFrequency: checkedTextUint32(freq)}
	if uint64(value.TermFrequency) != freq {
		return textSearchPostingValue{}, errMalformedTextStorage("postings value frequency overflows uint32")
	}
	if fieldCount > uint64(cur.remaining()+1) {
		return textSearchPostingValue{}, errMalformedTextStorage("postings value field count too large")
	}
	for i := uint64(0); i < fieldCount; i++ {
		field, err := cur.readStringIntern(fieldNames)
		if err != nil {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] name: %v", i, err)
		}
		fieldFreq, err := cur.readUvarint()
		if err != nil {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] frequency: %v", i, err)
		}
		positions, err := cur.skipUint32Slice()
		if err != nil {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] positions: %v", i, err)
		}
		offsets, err := cur.skipOffsetSlice()
		if err != nil {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] offsets: %v", i, err)
		}
		if uint64(checkedTextUint32(fieldFreq)) != fieldFreq {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] frequency overflows uint32", i)
		}
		if offsets != 0 && offsets != positions {
			return textSearchPostingValue{}, errMalformedTextStorage("postings value field[%d] offsets/positions length mismatch", i)
		}
		value.addField(textSearchPostingFieldValue{Field: field, Frequency: uint32(fieldFreq)})
	}
	if cur.remaining() != 0 {
		return textSearchPostingValue{}, errMalformedTextStorage("postings value trailing bytes")
	}
	return value, nil
}

func encodeTextDocumentStateValue(value textDocumentStateValue) []byte {
	fields := value.Fields
	if len(fields) > 1 {
		var stack [4]textDocumentFieldState
		if len(fields) <= len(stack) {
			fields = stack[:len(fields)]
			copy(fields, value.Fields)
		} else {
			fields = append([]textDocumentFieldState(nil), value.Fields...)
		}
		slices.SortFunc(fields, func(a, b textDocumentFieldState) int { return compareTextStrings(a.Field, b.Field) })
	}
	out := make([]byte, 0, encodedTextDocumentStateValueLen(fields))
	out = append(out, textStateValueVersion)
	out = appendTextUvarint(out, uint64(len(fields)))
	for _, field := range fields {
		terms := field.Terms
		if len(terms) > 1 {
			var stack [16]textDocumentTermState
			if len(terms) <= len(stack) {
				terms = stack[:len(terms)]
				copy(terms, field.Terms)
			} else {
				terms = append([]textDocumentTermState(nil), field.Terms...)
			}
			slices.SortFunc(terms, func(a, b textDocumentTermState) int { return compareTextStrings(a.Term, b.Term) })
		}
		out = appendTextString(out, field.Field)
		out = appendTextUvarint(out, uint64(field.Length))
		out = appendTextUvarint(out, uint64(len(terms)))
		for _, term := range terms {
			out = appendTextString(out, term.Term)
			out = appendTextUvarint(out, uint64(term.Frequency))
			out = appendTextUint32Slice(out, term.Positions)
			out = appendTextOffsetSlice(out, term.Offsets)
		}
	}
	return out
}

func decodeTextDocumentStateValue(raw []byte) (textDocumentStateValue, error) {
	if len(raw) == 0 {
		return textDocumentStateValue{}, errMalformedTextStorage("empty text-state value")
	}
	if raw[0] != textStateValueVersion {
		return textDocumentStateValue{}, errUnsupportedTextStorageVersion("text-state value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return textDocumentStateValue{}, errMalformedTextStorage("text-state field count: %v", err)
	}
	if fieldCount > uint64(cur.remaining()+1) {
		return textDocumentStateValue{}, errMalformedTextStorage("text-state field count too large")
	}
	value := textDocumentStateValue{Fields: make([]textDocumentFieldState, 0, fieldCount)}
	for i := uint64(0); i < fieldCount; i++ {
		fieldName, err := cur.readString()
		if err != nil {
			return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] name: %v", i, err)
		}
		length, err := cur.readUvarint()
		if err != nil {
			return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] length: %v", i, err)
		}
		if uint64(checkedTextUint32(length)) != length {
			return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] length overflows uint32", i)
		}
		termCount, err := cur.readUvarint()
		if err != nil {
			return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term count: %v", i, err)
		}
		if termCount > uint64(cur.remaining()+1) {
			return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term count too large", i)
		}
		field := textDocumentFieldState{Field: fieldName, Length: uint32(length), Terms: make([]textDocumentTermState, 0, termCount)}
		for j := uint64(0); j < termCount; j++ {
			termName, err := cur.readString()
			if err != nil {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] name: %v", i, j, err)
			}
			freq, err := cur.readUvarint()
			if err != nil {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] frequency: %v", i, j, err)
			}
			if uint64(checkedTextUint32(freq)) != freq {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] frequency overflows uint32", i, j)
			}
			positions, err := cur.readUint32Slice()
			if err != nil {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] positions: %v", i, j, err)
			}
			offsets, err := cur.readOffsetSlice()
			if err != nil {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] offsets: %v", i, j, err)
			}
			if len(offsets) != 0 && len(offsets) != len(positions) {
				return textDocumentStateValue{}, errMalformedTextStorage("text-state field[%d] term[%d] offsets/positions length mismatch", i, j)
			}
			field.Terms = append(field.Terms, textDocumentTermState{
				Term:      termName,
				Frequency: uint32(freq),
				Positions: positions,
				Offsets:   offsets,
			})
		}
		value.Fields = append(value.Fields, field)
	}
	if cur.remaining() != 0 {
		return textDocumentStateValue{}, errMalformedTextStorage("text-state value trailing bytes")
	}
	return value, nil
}

type textDocumentFieldLength struct {
	Field  string
	Length uint32
}

func decodeTextDocumentStateFieldLengths(raw []byte, dst []textDocumentFieldLength, fieldNames []string) ([]textDocumentFieldLength, error) {
	if len(raw) == 0 {
		return nil, errMalformedTextStorage("empty text-state value")
	}
	if raw[0] != textStateValueVersion {
		return nil, errUnsupportedTextStorageVersion("text-state value", raw[0])
	}
	cur := textCursor{buf: raw[1:]}
	fieldCount, err := cur.readUvarint()
	if err != nil {
		return nil, errMalformedTextStorage("text-state field count: %v", err)
	}
	if fieldCount > uint64(cur.remaining()+1) {
		return nil, errMalformedTextStorage("text-state field count too large")
	}
	if uint64(cap(dst)) < fieldCount {
		dst = make([]textDocumentFieldLength, 0, fieldCount)
	} else {
		dst = dst[:0]
	}
	for i := uint64(0); i < fieldCount; i++ {
		fieldName, err := cur.readStringIntern(fieldNames)
		if err != nil {
			return nil, errMalformedTextStorage("text-state field[%d] name: %v", i, err)
		}
		length, err := cur.readUvarint()
		if err != nil {
			return nil, errMalformedTextStorage("text-state field[%d] length: %v", i, err)
		}
		if uint64(checkedTextUint32(length)) != length {
			return nil, errMalformedTextStorage("text-state field[%d] length overflows uint32", i)
		}
		termCount, err := cur.readUvarint()
		if err != nil {
			return nil, errMalformedTextStorage("text-state field[%d] term count: %v", i, err)
		}
		if termCount > uint64(cur.remaining()+1) {
			return nil, errMalformedTextStorage("text-state field[%d] term count too large", i)
		}
		dst = append(dst, textDocumentFieldLength{Field: fieldName, Length: uint32(length)})
		for j := uint64(0); j < termCount; j++ {
			if _, err := cur.readBytes(); err != nil {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] name: %v", i, j, err)
			}
			freq, err := cur.readUvarint()
			if err != nil {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] frequency: %v", i, j, err)
			}
			if uint64(checkedTextUint32(freq)) != freq {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] frequency overflows uint32", i, j)
			}
			positions, err := cur.skipUint32Slice()
			if err != nil {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] positions: %v", i, j, err)
			}
			offsets, err := cur.skipOffsetSlice()
			if err != nil {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] offsets: %v", i, j, err)
			}
			if offsets != 0 && offsets != positions {
				return nil, errMalformedTextStorage("text-state field[%d] term[%d] offsets/positions length mismatch", i, j)
			}
		}
	}
	if cur.remaining() != 0 {
		return nil, errMalformedTextStorage("text-state value trailing bytes")
	}
	return dst, nil
}

func textDocumentFieldLengthByName(fields []textDocumentFieldLength, name string) (uint32, bool) {
	for _, field := range fields {
		if field.Field == name {
			return field.Length, true
		}
	}
	return 0, false
}

func encodeTextStatsCorpusValue(value textStatsCorpusValue) []byte {
	out := make([]byte, 0, 1+textUvarintLen(value.DocumentCount))
	out = append(out, textStatsValueVersion)
	out = appendTextUvarint(out, value.DocumentCount)
	return out
}

func decodeTextStatsCorpusValue(raw []byte) (textStatsCorpusValue, error) {
	cur, err := textStatsValueCursor(raw, "corpus")
	if err != nil {
		return textStatsCorpusValue{}, err
	}
	documents, err := cur.readUvarint()
	if err != nil {
		return textStatsCorpusValue{}, errMalformedTextStorage("text-stats corpus document count: %v", err)
	}
	if cur.remaining() != 0 {
		return textStatsCorpusValue{}, errMalformedTextStorage("text-stats corpus trailing bytes")
	}
	return textStatsCorpusValue{DocumentCount: documents}, nil
}

func encodeTextStatsTermValue(value textStatsTermValue) []byte {
	out := make([]byte, 0, 1+textUvarintLen(value.DocumentFrequency)+textUvarintLen(value.TotalTermFrequency))
	out = append(out, textStatsValueVersion)
	out = appendTextUvarint(out, value.DocumentFrequency)
	out = appendTextUvarint(out, value.TotalTermFrequency)
	return out
}

func decodeTextStatsTermValue(raw []byte) (textStatsTermValue, error) {
	cur, err := textStatsValueCursor(raw, "term")
	if err != nil {
		return textStatsTermValue{}, err
	}
	df, err := cur.readUvarint()
	if err != nil {
		return textStatsTermValue{}, errMalformedTextStorage("text-stats term document frequency: %v", err)
	}
	tf, err := cur.readUvarint()
	if err != nil {
		return textStatsTermValue{}, errMalformedTextStorage("text-stats term total frequency: %v", err)
	}
	if cur.remaining() != 0 {
		return textStatsTermValue{}, errMalformedTextStorage("text-stats term trailing bytes")
	}
	return textStatsTermValue{DocumentFrequency: df, TotalTermFrequency: tf}, nil
}

func encodeTextStatsFieldValue(value textStatsFieldValue) []byte {
	out := make([]byte, 0, 1+textUvarintLen(value.DocumentCount)+textUvarintLen(value.TotalTokenCount))
	out = append(out, textStatsValueVersion)
	out = appendTextUvarint(out, value.DocumentCount)
	out = appendTextUvarint(out, value.TotalTokenCount)
	return out
}

func decodeTextStatsFieldValue(raw []byte) (textStatsFieldValue, error) {
	cur, err := textStatsValueCursor(raw, "field")
	if err != nil {
		return textStatsFieldValue{}, err
	}
	documents, err := cur.readUvarint()
	if err != nil {
		return textStatsFieldValue{}, errMalformedTextStorage("text-stats field document count: %v", err)
	}
	tokens, err := cur.readUvarint()
	if err != nil {
		return textStatsFieldValue{}, errMalformedTextStorage("text-stats field token count: %v", err)
	}
	if cur.remaining() != 0 {
		return textStatsFieldValue{}, errMalformedTextStorage("text-stats field trailing bytes")
	}
	return textStatsFieldValue{DocumentCount: documents, TotalTokenCount: tokens}, nil
}

func textStatsValueCursor(raw []byte, name string) (textCursor, error) {
	if len(raw) == 0 {
		return textCursor{}, errMalformedTextStorage("empty text-stats %s value", name)
	}
	if raw[0] != textStatsValueVersion {
		return textCursor{}, errUnsupportedTextStorageVersion("text-stats "+name+" value", raw[0])
	}
	return textCursor{buf: raw[1:]}, nil
}

func encodedTextPostingValueLen(value textPostingValue, fields []textPostingFieldValue) int {
	n := 1 + textUvarintLen(uint64(value.TermFrequency)) + textUvarintLen(uint64(len(fields)))
	for _, field := range fields {
		n += encodedTextStringLen(field.Field)
		n += textUvarintLen(uint64(field.Frequency))
		n += encodedTextUint32SliceLen(field.Positions)
		n += encodedTextOffsetSliceLen(field.Offsets)
	}
	return n
}

func encodedTextDocumentStateValueLen(fields []textDocumentFieldState) int {
	n := 1 + textUvarintLen(uint64(len(fields)))
	for _, field := range fields {
		n += encodedTextStringLen(field.Field)
		n += textUvarintLen(uint64(field.Length))
		n += textUvarintLen(uint64(len(field.Terms)))
		for _, term := range field.Terms {
			n += encodedTextStringLen(term.Term)
			n += textUvarintLen(uint64(term.Frequency))
			n += encodedTextUint32SliceLen(term.Positions)
			n += encodedTextOffsetSliceLen(term.Offsets)
		}
	}
	return n
}

func encodedTextStringLen(value string) int {
	return textUvarintLen(uint64(len(value))) + len(value)
}

func encodedTextUint32SliceLen(values []uint32) int {
	n := textUvarintLen(uint64(len(values)))
	for _, value := range values {
		n += textUvarintLen(uint64(value))
	}
	return n
}

func encodedTextOffsetSliceLen(values []textTokenOffset) int {
	n := textUvarintLen(uint64(len(values)))
	for _, value := range values {
		n += textUvarintLen(uint64(value.Start))
		n += textUvarintLen(uint64(value.End))
	}
	return n
}

func textUvarintLen(value uint64) int {
	n := 1
	for value >= 0x80 {
		value >>= 7
		n++
	}
	return n
}

func appendTextBytes(dst []byte, value []byte) []byte {
	dst = appendTextUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendTextString(dst []byte, value string) []byte {
	return appendTextBytes(dst, []byte(value))
}

func appendTextUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func appendTextUint32Slice(dst []byte, values []uint32) []byte {
	dst = appendTextUvarint(dst, uint64(len(values)))
	for _, value := range values {
		dst = appendTextUvarint(dst, uint64(value))
	}
	return dst
}

func appendTextOffsetSlice(dst []byte, values []textTokenOffset) []byte {
	dst = appendTextUvarint(dst, uint64(len(values)))
	for _, value := range values {
		dst = appendTextUvarint(dst, uint64(value.Start))
		dst = appendTextUvarint(dst, uint64(value.End))
	}
	return dst
}

type textCursor struct {
	buf []byte
	pos int
}

func (c *textCursor) remaining() int {
	if c == nil || c.pos >= len(c.buf) {
		return 0
	}
	return len(c.buf) - c.pos
}

func (c *textCursor) readUvarint() (uint64, error) {
	if c == nil || c.pos >= len(c.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	value, n := binary.Uvarint(c.buf[c.pos:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("varint overflow")
	}
	c.pos += n
	return value, nil
}

func (c *textCursor) readBytes() ([]byte, error) {
	length, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	if length > uint64(c.remaining()) {
		return nil, io.ErrUnexpectedEOF
	}
	start := c.pos
	c.pos += int(length)
	return c.buf[start:c.pos], nil
}

func (c *textCursor) readString() (string, error) {
	value, err := c.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (c *textCursor) readStringIntern(interns []string) (string, error) {
	value, err := c.readBytes()
	if err != nil {
		return "", err
	}
	for _, intern := range interns {
		if textBytesEqualString(value, intern) {
			return intern, nil
		}
	}
	return string(value), nil
}

func textBytesEqualString(value []byte, intern string) bool {
	if len(value) != len(intern) {
		return false
	}
	for i, b := range value {
		if intern[i] != b {
			return false
		}
	}
	return true
}

func (c *textCursor) readUint32Slice() ([]uint32, error) {
	count, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(c.remaining()+1) {
		return nil, errors.New("count too large")
	}
	out := make([]uint32, 0, count)
	for i := uint64(0); i < count; i++ {
		value, err := c.readUvarint()
		if err != nil {
			return nil, err
		}
		if uint64(checkedTextUint32(value)) != value {
			return nil, errors.New("uint32 overflow")
		}
		out = append(out, uint32(value))
	}
	return out, nil
}

func (c *textCursor) skipUint32Slice() (uint64, error) {
	count, err := c.readUvarint()
	if err != nil {
		return 0, err
	}
	if count > uint64(c.remaining()+1) {
		return 0, errors.New("count too large")
	}
	for i := uint64(0); i < count; i++ {
		value, err := c.readUvarint()
		if err != nil {
			return 0, err
		}
		if uint64(checkedTextUint32(value)) != value {
			return 0, errors.New("uint32 overflow")
		}
	}
	return count, nil
}

func (c *textCursor) readOffsetSlice() ([]textTokenOffset, error) {
	count, err := c.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > uint64(c.remaining()+1) {
		return nil, errors.New("count too large")
	}
	out := make([]textTokenOffset, 0, count)
	for i := uint64(0); i < count; i++ {
		start, err := c.readUvarint()
		if err != nil {
			return nil, err
		}
		end, err := c.readUvarint()
		if err != nil {
			return nil, err
		}
		if uint64(checkedTextUint32(start)) != start || uint64(checkedTextUint32(end)) != end {
			return nil, errors.New("uint32 overflow")
		}
		if end < start {
			return nil, errors.New("offset end before start")
		}
		out = append(out, textTokenOffset{Start: uint32(start), End: uint32(end)})
	}
	return out, nil
}

func (c *textCursor) skipOffsetSlice() (uint64, error) {
	count, err := c.readUvarint()
	if err != nil {
		return 0, err
	}
	if count > uint64(c.remaining()+1) {
		return 0, errors.New("count too large")
	}
	for i := uint64(0); i < count; i++ {
		start, err := c.readUvarint()
		if err != nil {
			return 0, err
		}
		end, err := c.readUvarint()
		if err != nil {
			return 0, err
		}
		if uint64(checkedTextUint32(start)) != start || uint64(checkedTextUint32(end)) != end {
			return 0, errors.New("uint32 overflow")
		}
		if end < start {
			return 0, errors.New("offset end before start")
		}
	}
	return count, nil
}

func checkedTextUint32(value uint64) uint32 {
	return uint32(value)
}

func errMalformedTextStorage(format string, args ...any) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: %s", ErrTextIndexStorageCorrupt, format)
	}
	return fmt.Errorf("%w: %s", ErrTextIndexStorageCorrupt, fmt.Sprintf(format, args...))
}

func errUnsupportedTextStorageVersion(component string, version byte) error {
	return fmt.Errorf("%w: unsupported %s version %d", ErrTextIndexStorageCorrupt, component, version)
}
