package collections

import (
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

// AnalyzeText runs a named collection text analyzer over text. The simple
// analyzer lowercases Unicode letters and treats Unicode letters, Unicode
// digits, and '_' as token characters so code-ish identifiers such as
// "HTTP_500" remain searchable without stemming.
func AnalyzeText(analyzer TextAnalyzer, text string) ([]TextToken, error) {
	normalized, err := normalizeTextAnalyzer(analyzer)
	if err != nil {
		return nil, err
	}
	switch normalized {
	case TextAnalyzerSimple:
		return analyzeSimpleText(text), nil
	default:
		return nil, fmt.Errorf("unsupported analyzer %q", normalized)
	}
}

func analyzeSimpleText(text string) []TextToken {
	var tokens []TextToken
	var builder strings.Builder
	start := -1
	position := 0
	flush := func(end int) {
		if start < 0 {
			return
		}
		term := builder.String()
		if term != "" {
			tokens = append(tokens, TextToken{
				Term:        term,
				Position:    position,
				StartOffset: start,
				EndOffset:   end,
			})
			position++
		}
		builder.Reset()
		start = -1
	}
	for offset, r := range text {
		if simpleTextTokenRune(r) {
			if start < 0 {
				start = offset
			}
			builder.WriteRune(unicode.ToLower(r))
			continue
		}
		flush(offset)
	}
	flush(len(text))
	if tokens == nil {
		return []TextToken{}
	}
	return tokens
}

func simpleTextTokenRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
