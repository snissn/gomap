package collections

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
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
	normalized, err := normalizeTextAnalyzer(analyzer)
	if err != nil {
		return err
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
		return analyzeSimpleTextToSink(text, sink)
	default:
		return fmt.Errorf("unsupported analyzer %q", normalized)
	}
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
			if start < 0 {
				start = offset
			}
			builder.WriteRune(unicode.ToLower(r))
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
