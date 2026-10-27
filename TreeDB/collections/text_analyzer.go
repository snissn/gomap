package collections

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// TextToken is one analyzed token. Position is zero-based token order; offsets
// are byte offsets in the source string.
type TextToken struct {
	Term        string
	Position    int
	StartOffset int
	EndOffset   int
}

// TextTokenSink receives analyzed tokens from AnalyzeTextToSink. Implementations
// must not retain mutable scratch owned by the analyzer; TextToken values are
// self-contained.
type TextTokenSink interface {
	AddTextToken(TextToken) error
}

// TextTokenSinkFunc adapts a function to TextTokenSink.
type TextTokenSinkFunc func(TextToken) error

func (f TextTokenSinkFunc) AddTextToken(token TextToken) error {
	if f == nil {
		return errors.New("collections: text analyzer sink function is nil")
	}
	return f(token)
}

// AnalyzeText runs a named collection text analyzer over text. The simple
// analyzer lowercases Unicode letters and treats Unicode letters, Unicode
// digits, and '_' as token characters so code-ish identifiers such as
// "HTTP_500" remain searchable without stemming.
func AnalyzeText(analyzer TextAnalyzer, text string) ([]TextToken, error) {
	return AnalyzeTextWithOptions(analyzer, nil, text)
}

// AnalyzeTextWithOptions runs a named collection text analyzer with persisted
// analyzer options. It is intended for callers that need the same token stream
// used by text-index write and search paths.
func AnalyzeTextWithOptions(analyzer TextAnalyzer, options *TextAnalyzerOptions, text string) ([]TextToken, error) {
	var tokens []TextToken
	if err := AnalyzeTextToSinkWithOptions(analyzer, options, text, TextTokenSinkFunc(func(token TextToken) error {
		tokens = append(tokens, token)
		return nil
	})); err != nil {
		return nil, err
	}
	if tokens == nil {
		return []TextToken{}, nil
	}
	return tokens, nil
}

// AnalyzeTextToSink streams tokens from a named analyzer directly into sink. It
// avoids allocating the intermediate []TextToken used by AnalyzeText and is the
// preferred API for write-path collectors.
func AnalyzeTextToSink(analyzer TextAnalyzer, text string, sink TextTokenSink) error {
	return AnalyzeTextToSinkWithOptions(analyzer, nil, text, sink)
}

// AnalyzeTextToSinkWithOptions streams tokens using persisted analyzer options.
// Stopword filtering preserves original token positions so phrase/proximity
// matching can still account for gaps introduced by removed terms.
func AnalyzeTextToSinkWithOptions(analyzer TextAnalyzer, options *TextAnalyzerOptions, text string, sink TextTokenSink) error {
	if sink == nil {
		return errors.New("collections: text analyzer sink is nil")
	}
	// Capture the private capability before the stopword wrapper: stopped
	// tokens allocate normalizer backing too. Reuse the sink's existing
	// callback, without a closure or a second owner/lease.
	var admit func(uint64) error
	if owner, ok := sink.(textNormalizerAdmissionSink); ok {
		admit = owner.textNormalizerAdmission()
	}
	normalized, err := normalizeTextAnalyzer(analyzer)
	if err != nil {
		return err
	}
	if admit != nil {
		words, err := nativeTextStopWords(normalized, options, admit)
		if err != nil {
			return err
		}
		if len(words) > 0 {
			if err = admit(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextStopFilter{})))); err != nil {
				return err
			}
			sink = nativeTextStopFilter{sink: sink, words: words}
		}
		return analyzeSimpleTextToSinkAdmitted(text, sink, admit)
	}
	normalizedOptions, err := normalizeTextAnalyzerOptions(normalized, options)
	if err != nil {
		return err
	}
	if stopWords := textAnalyzerStopWordSet(normalizedOptions); len(stopWords) != 0 {
		sink = textStopWordFilterSink{sink: sink, stopWords: stopWords}
	}
	switch normalized {
	case TextAnalyzerSimple:
		return analyzeSimpleTextToSinkAdmitted(text, sink, admit)
	default:
		return fmt.Errorf("unsupported analyzer %q", normalized)
	}
}

// Only internal sinks can opt in. A nil callback keeps legacy Builder growth.
type textNormalizerAdmissionSink interface {
	textNormalizerAdmission() func(uint64) error
}

type textStopWordFilterSink struct {
	sink      TextTokenSink
	stopWords map[string]struct{}
}

func (s textStopWordFilterSink) AddTextToken(token TextToken) error {
	if _, skip := s.stopWords[token.Term]; skip {
		return nil
	}
	return s.sink.AddTextToken(token)
}

func analyzeSimpleTextToSink(text string, sink TextTokenSink) error {
	return analyzeSimpleTextToSinkAdmitted(text, sink, nil)
}

// textNormalizerGrowthCharge checks the Go 1.26 Builder.Grow allocation target
// 2*cap+width before any growth. Q covers allocator rounding for the complete new
// backing; obsolete cohorts are never subtracted. Width is the encoded rune size.
func textNormalizerGrowthCharge(length, capacity, width int) (uint64, error) {
	maxInt := int(^uint(0) >> 1)
	if length < 0 || capacity < length || width < 1 || width > utf8.UTFMax || length > maxInt-width {
		return 0, memtable.ErrCOWCapacity
	}
	if capacity-length >= width {
		return 0, nil
	}
	if capacity > (maxInt-width)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	charge := memtable.COWAllocationCharge(uint64(2*capacity + width))
	if charge == ^uint64(0) {
		return 0, memtable.ErrCOWCapacity
	}
	return charge, nil
}

func growTextNormalizer(builder *strings.Builder, r rune, admit func(uint64) error) error {
	width := utf8.RuneLen(r)
	charge, err := textNormalizerGrowthCharge(builder.Len(), builder.Cap(), width)
	if err != nil || charge == 0 {
		return err
	}
	if err := admit(charge); err != nil {
		return err
	}
	// Go 1.26 WriteRune uses utf8.AppendRune's exact-width append. Grow
	// guarantees that width free, so the following WriteRune cannot allocate.
	// Do not observe/charge capacity afterward.
	builder.Grow(width)
	return nil
}

func analyzeSimpleTextToSinkAdmitted(text string, sink TextTokenSink, admit func(uint64) error) error {
	var builder strings.Builder
	start := -1
	position := 0
	flush := func(end int) error {
		if start < 0 {
			return nil
		}
		term := builder.String()
		if term != "" {
			if err := sink.AddTextToken(TextToken{
				Term:        term,
				Position:    position,
				StartOffset: start,
				EndOffset:   end,
			}); err != nil {
				return err
			}
			position++
		}
		builder.Reset()
		start = -1
		return nil
	}
	for offset, r := range text {
		if simpleTextTokenRune(r) {
			r = unicode.ToLower(r)
			if admit != nil {
				if err := growTextNormalizer(&builder, r, admit); err != nil {
					return err
				}
			}
			if start < 0 {
				start = offset
			}
			builder.WriteRune(r)
			continue
		}
		if err := flush(offset); err != nil {
			return err
		}
	}
	return flush(len(text))
}

func simpleTextTokenRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
