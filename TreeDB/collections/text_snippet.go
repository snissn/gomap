package collections

import (
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	defaultSnippetMaxSnippets = 1
	defaultSnippetWindowRunes = 160
)

// TextSnippetOptions configures ExtractTextSnippets.
type TextSnippetOptions struct {
	// MaxSnippets caps the number of returned snippet windows (default 1).
	MaxSnippets int
	// WindowRunes is the approximate rune width of each snippet window (default 160).
	WindowRunes int
	// HighlightPre and HighlightPost wrap each matched token span when non-empty.
	HighlightPre  string
	HighlightPost string
}

// TextSnippet is one extracted excerpt aligned to exact byte offsets in text.
type TextSnippet struct {
	Text         string   `json:"text"`
	StartOffset  int      `json:"start_offset"`
	EndOffset    int      `json:"end_offset"`
	MatchedTerms []string `json:"matched_terms,omitempty"`
}

type snippetTokenHit struct {
	term        string
	startOffset int
	endOffset   int
}

type snippetWindow struct {
	startHit      int
	endHit        int // exclusive
	distinctTerms int
	totalHits     int
	startByte     int
	endByte       int
}

// ExtractTextSnippets extracts up to opts.MaxSnippets high-density excerpts
// from text matching query using the exact collection analyzer and options
// (including StopWords). Returned byte offsets refer to the original text.
func ExtractTextSnippets(analyzer TextAnalyzer, analyzerOpts *TextAnalyzerOptions, text string, query string, opts TextSnippetOptions) ([]TextSnippet, error) {
	if opts.MaxSnippets <= 0 {
		opts.MaxSnippets = defaultSnippetMaxSnippets
	}
	if opts.WindowRunes <= 0 {
		opts.WindowRunes = defaultSnippetWindowRunes
	}
	if text == "" || strings.TrimSpace(query) == "" {
		return []TextSnippet{}, nil
	}

	queryTerms := make(map[string]struct{}, 8)
	if err := AnalyzeTextToSinkWithOptions(analyzer, analyzerOpts, query, TextTokenSinkFunc(func(tok TextToken) error {
		queryTerms[tok.Term] = struct{}{}
		return nil
	})); err != nil {
		return nil, err
	}
	if len(queryTerms) == 0 {
		return []TextSnippet{}, nil
	}

	var hits []snippetTokenHit
	if err := AnalyzeTextToSinkWithOptions(analyzer, analyzerOpts, text, TextTokenSinkFunc(func(tok TextToken) error {
		if _, ok := queryTerms[tok.Term]; ok {
			hits = append(hits, snippetTokenHit{
				term:        tok.Term,
				startOffset: tok.StartOffset,
				endOffset:   tok.EndOffset,
			})
		}
		return nil
	})); err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return []TextSnippet{}, nil
	}

	// Build candidate windows anchored at each hit, spanning roughly WindowRunes bytes/runes.
	windowBytesApprox := opts.WindowRunes
	candidates := make([]snippetWindow, 0, len(hits))
	seenTerm := make(map[string]int, len(queryTerms))
	epoch := 0
	for i := range hits {
		epoch++
		distinct := 0
		j := i
		limitByte := advanceRunesByteOffset(text, hits[i].startOffset, opts.WindowRunes)
		for j < len(hits) && hits[j].endOffset <= limitByte {
			if seenTerm[hits[j].term] != epoch {
				seenTerm[hits[j].term] = epoch
				distinct++
			}
			j++
		}
		if j == i {
			j = i + 1
			distinct = 1
		}
		startB, endB := computeSnippetBounds(text, hits[i].startOffset, hits[j-1].endOffset, opts.WindowRunes, windowBytesApprox)
		candidates = append(candidates, snippetWindow{
			startHit:      i,
			endHit:        j,
			distinctTerms: distinct,
			totalHits:     j - i,
			startByte:     startB,
			endByte:       endB,
		})
	}

	sort.Slice(candidates, func(a, b int) bool {
		if candidates[a].distinctTerms != candidates[b].distinctTerms {
			return candidates[a].distinctTerms > candidates[b].distinctTerms
		}
		if candidates[a].totalHits != candidates[b].totalHits {
			return candidates[a].totalHits > candidates[b].totalHits
		}
		return candidates[a].startByte < candidates[b].startByte
	})

	selected := make([]snippetWindow, 0, opts.MaxSnippets)
	for _, cand := range candidates {
		if len(selected) >= opts.MaxSnippets {
			break
		}
		overlaps := false
		for _, prev := range selected {
			if cand.startByte < prev.endByte && cand.endByte > prev.startByte {
				overlaps = true
				break
			}
		}
		if overlaps {
			continue
		}
		selected = append(selected, cand)
	}

	result := make([]TextSnippet, 0, len(selected))
	for _, win := range selected {
		rendered, matched := renderSnippetWindow(text, hits, win.startByte, win.endByte, opts.HighlightPre, opts.HighlightPost)
		result = append(result, TextSnippet{
			Text:         rendered,
			StartOffset:  win.startByte,
			EndOffset:    win.endByte,
			MatchedTerms: matched,
		})
	}
	return result, nil
}

func advanceRunesByteOffset(s string, start int, runes int) int {
	pos := start
	for n := 0; n < runes && pos < len(s); n++ {
		_, size := utf8.DecodeRuneInString(s[pos:])
		if size <= 0 {
			break
		}
		pos += size
	}
	return pos
}

func retreatRunesByteOffset(s string, start int, runes int) int {
	pos := start
	for n := 0; n < runes && pos > 0; n++ {
		_, size := utf8.DecodeLastRuneInString(s[:pos])
		if size <= 0 {
			break
		}
		pos -= size
	}
	return pos
}

func computeSnippetBounds(text string, matchStart, matchEnd, windowRunes, _ int) (int, int) {
	spanRunes := utf8.RuneCountInString(text[matchStart:matchEnd])
	pad := (windowRunes - spanRunes) / 2
	if pad < 0 {
		pad = 0
	}
	start := retreatRunesByteOffset(text, matchStart, pad)
	end := advanceRunesByteOffset(text, matchEnd, pad)

	// Snap start forward to a whitespace boundary if inside a word (bounded lookahead).
	if start > 0 && start < matchStart {
		if idx := strings.IndexAny(text[start:matchStart], " \t\n\r"); idx >= 0 && idx <= 24 {
			start += idx + 1
		}
	}
	// Snap end backward to a whitespace boundary if inside a word (bounded lookback).
	if end < len(text) && end > matchEnd {
		if idx := strings.LastIndexAny(text[matchEnd:end], " \t\n\r"); idx >= 0 && (end-(matchEnd+idx)) <= 24 {
			end = matchEnd + idx
		}
	}
	if start < 0 {
		start = 0
	}
	if end > len(text) {
		end = len(text)
	}
	if end < start {
		end = start
	}
	return start, end
}

func renderSnippetWindow(text string, hits []snippetTokenHit, startByte, endByte int, pre, post string) (string, []string) {
	// Binary search first hit with startOffset >= startByte.
	lo := sort.Search(len(hits), func(i int) bool { return hits[i].startOffset >= startByte })
	var b strings.Builder
	b.Grow((endByte - startByte) + 32)
	termSet := make(map[string]struct{}, 4)
	cursor := startByte
	for i := lo; i < len(hits) && hits[i].startOffset < endByte; i++ {
		h := hits[i]
		if h.endOffset > endByte || h.startOffset < cursor {
			continue
		}
		b.WriteString(text[cursor:h.startOffset])
		if pre != "" {
			b.WriteString(pre)
		}
		b.WriteString(text[h.startOffset:h.endOffset])
		if post != "" {
			b.WriteString(post)
		}
		cursor = h.endOffset
		termSet[h.term] = struct{}{}
	}
	if cursor < endByte {
		b.WriteString(text[cursor:endByte])
	}
	terms := make([]string, 0, len(termSet))
	for t := range termSet {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	return b.String(), terms
}
