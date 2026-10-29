package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"unsafe"

	"github.com/buger/jsonparser"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type nativeTextStopWordSink struct {
	word  string
	count int
	admit func(uint64) error
}

func (s *nativeTextStopWordSink) AddTextToken(token TextToken) error {
	s.word = token.Term
	s.count++
	return nil
}
func (s *nativeTextStopWordSink) textNormalizerAdmission() func(uint64) error { return s.admit }

type nativeTextStopFilter struct {
	sink  TextTokenSink
	words []string
}

func (s nativeTextStopFilter) AddTextToken(token TextToken) error {
	i := sort.SearchStrings(s.words, token.Term)
	if i < len(s.words) && s.words[i] == token.Term {
		return nil
	}
	return s.sink.AddTextToken(token)
}

// Native normalization never constructs the legacy declaration Swiss maps.
// The resulting sorted lane has the identical normalized stopword semantics.
func nativeTextStopWords(analyzer TextAnalyzer, options *TextAnalyzerOptions, admit func(uint64) error) ([]string, error) {
	if options == nil {
		return nil, nil
	}
	if _, err := normalizeTextAnalyzer(analyzer); err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.Stemmer) != "" {
		return nil, fmt.Errorf("%w: text analyzer stemmer %q is not implemented", ErrTextIndexUnavailable, options.Stemmer)
	}
	if len(options.Synonyms) != 0 {
		return nil, fmt.Errorf("%w: text analyzer synonyms are not implemented", ErrTextIndexUnavailable)
	}
	if len(options.StopWords) == 0 {
		return nil, nil
	}
	if err := admitLocalIDArray(admit, len(options.StopWords), uint64(unsafe.Sizeof(string("")))); err != nil {
		return nil, err
	}
	if err := admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextStopWordSink{})))); err != nil {
		return nil, err
	}
	words := make([]string, 0, len(options.StopWords))
	sink := &nativeTextStopWordSink{admit: admit}
	for i, raw := range options.StopWords {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		sink.word = ""
		sink.count = 0
		if err := analyzeSimpleTextToSinkAdmitted(raw, sink, admit); err != nil {
			return nil, fmt.Errorf("text analyzer stop_words[%d]: %w", i, err)
		}
		if sink.count != 1 {
			return nil, fmt.Errorf("text analyzer stop_words[%d] %q must analyze to exactly one simple token", i, raw)
		}
		words = append(words, sink.word)
	}
	sort.Strings(words)
	n := 0
	for _, word := range words {
		if n == 0 || words[n-1] != word {
			words[n] = word
			n++
		}
	}
	return words[:n], nil
}

// RawMessage retains the maintained decoder's complete first and second Decode
// validation, UseNumber setting, and object-only contract without materializing
// recursive map[string]any objects. Per-field projections then borrow raw spans.
func nativeTextNestedDocumentState(def TextIndexDefinition, document []byte, opts collectionOptions) (textDocumentStateValue, error) {
	admit := opts.textMaterializationAdmit
	charge, err := nativeTextNestedDecoderCharge(def, uint64(len(document)))
	if err != nil {
		return textDocumentStateValue{}, err
	}
	if err = admit(charge); err != nil {
		return textDocumentStateValue{}, err
	}
	var raw json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err = decoder.Decode(&raw); err != nil {
		return textDocumentStateValue{}, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("unexpected trailing JSON value")
		}
		return textDocumentStateValue{}, fmt.Errorf("collections: text index extraction requires JSON document: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return textDocumentStateValue{}, errors.New("collections: text index extraction requires JSON object document")
	}
	var headers nativeTextAdmissionSum
	headers.array(uint64(len(def.Fields)), uint64(unsafe.Sizeof(textDocumentFieldState{})))
	headers.array(1, uint64(unsafe.Sizeof(textDocumentStateValue{})))
	if headers.bad {
		return textDocumentStateValue{}, memtable.ErrCOWCapacity
	}
	if err = admit(headers.total); err != nil {
		return textDocumentStateValue{}, err
	}
	state := textDocumentStateValue{Fields: make([]textDocumentFieldState, 0, len(def.Fields))}
	for _, field := range def.Fields {
		parts := splitIndexPath(field.Field)
		selected := jsonParserIndexValue{raw: raw, valueType: jsonparser.Object}
		for _, part := range parts {
			if selected.valueType != jsonparser.Object {
				selected = jsonParserIndexValue{}
				break
			}
			// Each visited object receives its own decoder/source envelope before
			// Token or RawMessage can allocate. No recursive Go map is constructed.
			selected, err = nativeTextObjectMember(selected.raw, part, def, admit)
			if err != nil {
				return textDocumentStateValue{}, err
			}
		}
		text, found, err := nativeTextSelectedText(selected, admit)
		if err != nil {
			return textDocumentStateValue{}, err
		}
		if !found {
			continue
		}
		if err = admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textTermAccumulator{})))); err != nil {
			return textDocumentStateValue{}, err
		}
		acc := &textTermAccumulator{storePositions: def.StorePositions, storeOffsets: def.StoreOffsets, admit: admit}
		acc.terms = acc.inline[:0]
		if err = analyzeTextToTermAccumulator(def.Analyzer, def.AnalyzerOptions, text, acc); err != nil {
			return textDocumentStateValue{}, err
		}
		state.Fields = append(state.Fields, acc.takeDocumentField(field.Field))
	}
	slices.SortFunc(state.Fields, func(a, b textDocumentFieldState) int { return compareTextStrings(a.Field, b.Field) })
	return state, nil
}

func nativeTextSelectedText(value jsonParserIndexValue, admit func(uint64) error) (string, bool, error) {
	// Raw spans contain no recursive object allocation. Decode only scalar strings
	// so invalid UTF-8 and surrogate replacement match encoding/json exactly.
	decode := func(raw []byte) (string, error) {
		// ObjectEach/Get returns string contents without quotes. Construct a quoted
		// image and admit its complete backing before either image/string exists.
		n := uint64(len(raw))
		var charge nativeTextAdmissionSum
		charge.array(n+2, 1)
		charge.array(n, 1)
		if charge.bad {
			return "", memtable.ErrCOWCapacity
		}
		if err := admit(charge.total); err != nil {
			return "", err
		}
		quoted := make([]byte, 0, len(raw)+2)
		quoted = append(quoted, '"')
		quoted = append(quoted, raw...)
		quoted = append(quoted, '"')
		var out string
		err := json.Unmarshal(quoted, &out)
		return out, err
	}
	switch value.valueType {
	case jsonparser.String:
		out, err := decode(value.raw)
		return out, true, err
	case jsonparser.Array:
		var out []byte
		found := false
		var failure error
		_, err := jsonparser.ArrayEach(value.raw, func(raw []byte, kind jsonparser.ValueType, _ int, parseErr error) {
			if failure != nil {
				return
			}
			if parseErr != nil {
				failure = parseErr
				return
			}
			if kind != jsonparser.String {
				return
			}
			part, err := decode(raw)
			if err != nil {
				failure = err
				return
			}
			extra := len(part)
			if found {
				extra++
			}
			if extra > int(^uint(0)>>1)-len(out) {
				failure = memtable.ErrCOWCapacity
				return
			}
			need := len(out) + extra
			if need > cap(out) {
				if err := admitLocalIDArray(admit, need, 1); err != nil {
					failure = err
					return
				}
				next := make([]byte, len(out), need)
				copy(next, out)
				out = next
			}
			if found {
				out = append(out, ' ')
			}
			out = append(out, part...)
			found = true
		})
		if err != nil {
			return "", false, err
		}
		if failure != nil {
			return "", false, failure
		}
		if !found {
			return "", false, nil
		}
		if err = admit(memtable.COWAllocationCharge(uint64(len(out)))); err != nil {
			return "", false, err
		}
		return string(out), true, nil
	default:
		return "", false, nil
	}
}

// Decode keys with encoding/json itself, including replacement of malformed
// UTF-8 and unpaired UTF-16 escapes. Selecting the LAST whole member before
// descending preserves duplicate-last ancestor replacement, not object merge.
func nativeTextObjectMember(raw []byte, name string, def TextIndexDefinition, admit func(uint64) error) (jsonParserIndexValue, error) {
	charge, err := nativeTextNestedDecoderCharge(def, uint64(len(raw)))
	if err != nil {
		return jsonParserIndexValue{}, err
	}
	if err = admit(charge); err != nil {
		return jsonParserIndexValue{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if _, err = decoder.Token(); err != nil {
		return jsonParserIndexValue{}, err
	}
	var selected json.RawMessage
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return jsonParserIndexValue{}, err
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return jsonParserIndexValue{}, err
		}
		if key == name {
			selected = value
		}
	}
	if _, err = decoder.Token(); err != nil {
		return jsonParserIndexValue{}, err
	}
	if selected == nil {
		return jsonParserIndexValue{}, nil
	}
	value, kind, _, err := jsonparser.Get(selected)
	return jsonParserIndexValue{raw: value, valueType: kind}, err
}

// Root fields retain the maintained span selector and string/array conversion
// semantics (including their exact escape errors). Only nested declarations use
// the full decoder. All native routes still emit the same concrete state.
func nativeTextDocumentState(def TextIndexDefinition, document []byte, opts collectionOptions) (textDocumentStateValue, error) {
	admit := opts.textMaterializationAdmit
	var charge nativeTextAdmissionSum
	charge.array(uint64(len(document)), 2)
	charge.array(uint64(len(def.Fields)), uint64(unsafe.Sizeof(jsonParserIndexValue{})))
	if charge.bad {
		return textDocumentStateValue{}, memtable.ErrCOWCapacity
	}
	if err := admit(charge.total); err != nil {
		return textDocumentStateValue{}, err
	}
	values := make([]jsonParserIndexValue, len(def.Fields))
	handled, err := selectTextIndexDocumentJSONRootValues(def, document, values)
	if err != nil {
		return textDocumentStateValue{}, err
	}
	if handled {
		return nativeTextPrepareSelected(def, values, admit)
	}
	return nativeTextNestedDocumentState(def, document, opts)
}
func nativeTextPrepareSelected(def TextIndexDefinition, values []jsonParserIndexValue, admit func(uint64) error) (textDocumentStateValue, error) {
	var charge nativeTextAdmissionSum
	charge.array(1, uint64(unsafe.Sizeof(textDocumentStateValue{})))
	charge.array(uint64(len(def.Fields)), uint64(unsafe.Sizeof(textDocumentFieldState{})))
	if charge.bad {
		return textDocumentStateValue{}, memtable.ErrCOWCapacity
	}
	if err := admit(charge.total); err != nil {
		return textDocumentStateValue{}, err
	}
	state := textDocumentStateValue{Fields: make([]textDocumentFieldState, 0, len(def.Fields))}
	for i, value := range values {
		cohort, err := nativeTextNestedDecoderCharge(def, uint64(len(value.raw)))
		if err != nil {
			return textDocumentStateValue{}, err
		}
		if err = admit(cohort); err != nil {
			return textDocumentStateValue{}, err
		}
		var scratch []byte
		text, found, err := textIndexFieldTextFromJSONParser(value, &scratch)
		if err != nil {
			return textDocumentStateValue{}, err
		}
		if !found {
			continue
		}
		if err = admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textTermAccumulator{})))); err != nil {
			return textDocumentStateValue{}, err
		}
		acc := &textTermAccumulator{storePositions: def.StorePositions, storeOffsets: def.StoreOffsets, admit: admit}
		acc.terms = acc.inline[:0]
		if err = analyzeTextToTermAccumulator(def.Analyzer, def.AnalyzerOptions, text, acc); err != nil {
			return textDocumentStateValue{}, err
		}
		state.Fields = append(state.Fields, acc.takeDocumentField(def.Fields[i].Field))
	}
	slices.SortFunc(state.Fields, func(a, b textDocumentFieldState) int { return compareTextStrings(a.Field, b.Field) })
	return state, nil
}
