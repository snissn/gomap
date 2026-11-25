package collections

import (
	"strings"
	"testing"
)

func TestExtractTextSnippetsSimpleAndStopwords(t *testing.T) {
	text := "Introduction paragraph with unrelated words. " +
		"In the elastic electron phase medium, local-MD CHSH approaches 2*sqrt(2) under A_phys_v2. " +
		"Trailing appendix with many other unrelated terms about lattice cosmology."
	opts := TextSnippetOptions{
		MaxSnippets:   2,
		WindowRunes:   60,
		HighlightPre:  "<mark>",
		HighlightPost: "</mark>",
	}
	snips, err := ExtractTextSnippets(
		TextAnalyzerSimple,
		&TextAnalyzerOptions{StopWords: []string{"the", "in"}},
		text,
		"the A_phys_v2 CHSH",
		opts,
	)
	if err != nil {
		t.Fatalf("ExtractTextSnippets: %v", err)
	}
	if len(snips) == 0 {
		t.Fatalf("expected >=1 snippet")
	}
	got := snips[0]
	if !strings.Contains(got.Text, "<mark>CHSH</mark>") || !strings.Contains(got.Text, "<mark>A_phys_v2</mark>") {
		t.Fatalf("expected highlighted CHSH and A_phys_v2 in snippet, got %q", got.Text)
	}
	if strings.Contains(got.Text, "<mark>the</mark>") {
		t.Fatalf("stopword must not be highlighted: %q", got.Text)
	}
	if got.StartOffset < 0 || got.EndOffset <= got.StartOffset || got.EndOffset > len(text) {
		t.Fatalf("invalid byte offsets [%d,%d) for len %d", got.StartOffset, got.EndOffset, len(text))
	}
	if len(got.MatchedTerms) != 2 {
		t.Fatalf("MatchedTerms=%v want 2 terms [a_phys_v2 chsh]", got.MatchedTerms)
	}
}

func TestExtractTextSnippetsUTF8AndNoMatch(t *testing.T) {
	text := "Schrödinger equation ψ(x) — café naïve B_6 holonomy."
	snips, err := ExtractTextSnippets(TextAnalyzerSimple, nil, text, "schrödinger holonomy", TextSnippetOptions{
		MaxSnippets:   1,
		WindowRunes:   40,
		HighlightPre:  "[",
		HighlightPost: "]",
	})
	if err != nil {
		t.Fatalf("ExtractTextSnippets UTF8: %v", err)
	}
	if len(snips) != 1 || !strings.Contains(snips[0].Text, "[Schrödinger]") {
		t.Fatalf("UTF-8 highlight failed: %+v", snips)
	}
	none, err := ExtractTextSnippets(TextAnalyzerSimple, nil, text, "nonexistent_term", TextSnippetOptions{MaxSnippets: 2})
	if err != nil {
		t.Fatalf("no-match err: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected empty snippets for non-matching query, got %+v", none)
	}
}

func BenchmarkExtractTextSnippets(b *testing.B) {
	text := strings.Repeat("Background equations and narrative prose about fields and phases. ", 20) +
		"The local-MD CHSH witness reaches 2*sqrt(2) under A_phys_v2 with bounded causal support. " +
		strings.Repeat("Additional appendix commentary and verification receipts. ", 20)
	opts := TextSnippetOptions{
		MaxSnippets:   2,
		WindowRunes:   160,
		HighlightPre:  "**",
		HighlightPost: "**",
	}
	analyzerOpts := &TextAnalyzerOptions{StopWords: []string{"the", "and", "with"}}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snips, err := ExtractTextSnippets(TextAnalyzerSimple, analyzerOpts, text, "local-MD CHSH A_phys_v2", opts)
		if err != nil || len(snips) == 0 {
			b.Fatalf("snips=%v err=%v", snips, err)
		}
	}
}
