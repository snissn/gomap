package chunking

import (
	"bytes"
	"strings"
	"testing"
	"unicode"
)

func assertChunkInvariants(t *testing.T, text string, cfg Config, chunks []Chunk) {
	t.Helper()
	runes := []rune(text)
	if len(runes) == 0 {
		if len(chunks) != 0 {
			t.Fatalf("empty input produced %d chunks", len(chunks))
		}
		return
	}
	if len(chunks) == 0 {
		t.Fatal("non-empty input produced 0 chunks")
	}

	covered := make([]bool, len(runes))
	for i, ch := range chunks {
		if ch.Ordinal != i {
			t.Fatalf("chunk %d ordinal=%d", i, ch.Ordinal)
		}
		if ch.Text == "" || ch.StartOffset >= ch.EndOffset {
			t.Fatalf("chunk %d is empty: %+v", i, ch)
		}
		if ch.StartOffset < 0 || ch.EndOffset > len(runes) {
			t.Fatalf("chunk %d offsets [%d,%d) out of range [0,%d)", i, ch.StartOffset, ch.EndOffset, len(runes))
		}
		got := string(runes[ch.StartOffset:ch.EndOffset])
		if got != ch.Text {
			t.Fatalf("chunk %d text=%q does not match slice %q", i, ch.Text, got)
		}
		if n := ch.EndOffset - ch.StartOffset; n > cfg.Size {
			t.Fatalf("chunk %d has %d runes > size %d", i, n, cfg.Size)
		}
		if i > 0 {
			prev := chunks[i-1]
			if ch.StartOffset <= prev.StartOffset {
				t.Fatalf("chunk %d failed strict forward progress: start=%d <= prev.start=%d", i, ch.StartOffset, prev.StartOffset)
			}
			if ch.StartOffset > prev.EndOffset {
				t.Fatalf("chunk %d left uncovered gap: start=%d > prev.end=%d", i, ch.StartOffset, prev.EndOffset)
			}
			actualOverlap := prev.EndOffset - ch.StartOffset
			if cfg.SnapOverlapToBoundary {
				if actualOverlap > cfg.Overlap {
					t.Fatalf("chunk %d overlap=%d exceeds configured max %d", i, actualOverlap, cfg.Overlap)
				}
			} else if actualOverlap != cfg.Overlap {
				t.Fatalf("chunk %d overlap=%d want exact %d", i, actualOverlap, cfg.Overlap)
			}
		}
		for off := ch.StartOffset; off < ch.EndOffset; off++ {
			covered[off] = true
		}
	}
	if chunks[0].StartOffset != 0 {
		t.Fatalf("first chunk starts at %d, want 0", chunks[0].StartOffset)
	}
	if last := chunks[len(chunks)-1]; last.EndOffset != len(runes) {
		t.Fatalf("last chunk ends at %d, want %d", last.EndOffset, len(runes))
	}
	for off, ok := range covered {
		if !ok {
			t.Fatalf("rune offset %d not covered by any chunk", off)
		}
	}
}

func TestConfigValidationSeparatorPlacementAndSnapOverlap(t *testing.T) {
	validCases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "fixed_window default placement",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4},
		},
		{
			name: "fixed_window explicit end placement",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4, SeparatorPlacement: SeparatorPlacementEnd},
		},
		{
			name: "recursive default placement",
			cfg:  Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4},
		},
		{
			name: "recursive end placement",
			cfg:  Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4, SeparatorPlacement: SeparatorPlacementEnd},
		},
		{
			name: "recursive start placement",
			cfg:  Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4, SeparatorPlacement: SeparatorPlacementStart},
		},
		{
			name: "recursive snap overlap",
			cfg:  Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4, SnapOverlapToBoundary: true},
		},
		{
			name: "recursive start placement and snap overlap",
			cfg: Config{
				Strategy:              StrategyRecursive,
				SizeUnit:              SizeUnitRunes,
				Size:                  32,
				Overlap:               4,
				SeparatorPlacement:    SeparatorPlacementStart,
				SnapOverlapToBoundary: true,
			},
		},
	}
	for _, tc := range validCases {
		if err := tc.cfg.Validate(); err != nil {
			t.Fatalf("%s: expected valid config, got error: %v", tc.name, err)
		}
	}

	invalidCases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "recursive unknown placement",
			cfg:  Config{Strategy: StrategyRecursive, SizeUnit: SizeUnitRunes, Size: 32, SeparatorPlacement: "middle"},
		},
		{
			name: "fixed_window unknown placement",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, SeparatorPlacement: "START"},
		},
		{
			name: "fixed_window rejects start placement",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, SeparatorPlacement: SeparatorPlacementStart},
		},
		{
			name: "fixed_window rejects snap overlap",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 4, SnapOverlapToBoundary: true},
		},
		{
			name: "fixed_window rejects snap overlap even when overlap is 0",
			cfg:  Config{Strategy: StrategyFixedWindow, SizeUnit: SizeUnitRunes, Size: 32, Overlap: 0, SnapOverlapToBoundary: true},
		},
	}
	for _, tc := range invalidCases {
		if err := tc.cfg.Validate(); err == nil {
			t.Fatalf("%s: expected fail-closed error, got nil", tc.name)
		}
	}
}

func TestIsDelimiterOnlySeparator(t *testing.T) {
	delimiters := []string{"\n\n", "\n", " ", "\t", "\r\n", ". ", "! ", "? ", "; ", ", ", "\u00a0"}
	for _, sep := range delimiters {
		if !isDelimiterOnlySeparator([]rune(sep)) {
			t.Fatalf("isDelimiterOnlySeparator(%q) = false, want true", sep)
		}
	}
	structural := []string{"", "\n# ", "\n## ", "\n### ", "\nfunc ", "\ndef ", "\n\\section", "## ", "|", "---"}
	for _, sep := range structural {
		if isDelimiterOnlySeparator([]rune(sep)) {
			t.Fatalf("isDelimiterOnlySeparator(%q) = true, want false", sep)
		}
	}
}

func TestSeparatorPresetsAreExactAndFresh(t *testing.T) {
	tests := []struct {
		name string
		fn   func() []string
		want []string
	}{
		{
			name: "MarkdownSeparators",
			fn:   MarkdownSeparators,
			want: []string{"\n# ", "\n## ", "\n### ", "\n#### ", "\n\n", "\n", ". ", " ", ""},
		},
		{
			name: "CodeSeparators",
			fn:   CodeSeparators,
			want: []string{"\nfunc ", "\ntype ", "\nclass ", "\ndef ", "\nasync def ", "\ntheorem ", "\nlemma ", "\nstructure ", "\nnamespace ", "\n\n", "\n", " ", ""},
		},
		{
			name: "LaTeXSeparators",
			fn:   LaTeXSeparators,
			want: []string{"\n\\section", "\n\\subsection", "\n\\subsubsection", "\n\\begin{", "\n\n", "\n", ". ", " ", ""},
		},
	}
	for _, tc := range tests {
		got := tc.fn()
		if len(got) != len(tc.want) {
			t.Fatalf("%s len=%d want %d (%q vs %q)", tc.name, len(got), len(tc.want), got, tc.want)
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("%s[%d]=%q want %q", tc.name, i, got[i], tc.want[i])
			}
		}
		if got[len(got)-1] != "" {
			t.Fatalf("%s must end with hard-split sentinel \"\"", tc.name)
		}
		// Mutate returned slice and ensure subsequent calls are unaffected.
		got[0] = "MUTATED"
		again := tc.fn()
		if again[0] != tc.want[0] {
			t.Fatalf("%s returned shared backing slice: got[0]=%q want %q", tc.name, again[0], tc.want[0])
		}
	}
}

func TestSeparatorPlacementStartAttachesHeadingsToNextChunkAndKeepsSentencePunctuationAtEnd(t *testing.T) {
	doc := "Intro summary before headings.\n## Section One\nFirst section body here.\n## Section Two\nSecond section body here."
	cfgStart := Config{
		Strategy:           StrategyRecursive,
		SizeUnit:           SizeUnitRunes,
		Size:               60,
		Overlap:            0,
		Separators:         MarkdownSeparators(),
		SeparatorPlacement: SeparatorPlacementStart,
	}
	chunks := mustChunk(t, "md", doc, cfgStart)
	assertChunkInvariants(t, doc, cfgStart, chunks)

	if len(chunks) < 3 {
		t.Fatalf("expected at least 3 chunks, got %d: %#v", len(chunks), chunks)
	}
	if strings.HasSuffix(chunks[0].Text, "## ") {
		t.Fatalf("chunk 0 ended with detached heading prefix: %q", chunks[0].Text)
	}
	if !strings.HasPrefix(chunks[1].Text, "## Section One") {
		t.Fatalf("chunk 1 did not start with heading: %q", chunks[1].Text)
	}
	if !strings.HasPrefix(chunks[2].Text, "## Section Two") {
		t.Fatalf("chunk 2 did not start with heading: %q", chunks[2].Text)
	}

	// Legacy default attaches "\n## " to the end of chunk 0.
	cfgEnd := cfgStart
	cfgEnd.SeparatorPlacement = SeparatorPlacementEnd
	legacyChunks := mustChunk(t, "md", doc, cfgEnd)
	assertChunkInvariants(t, doc, cfgEnd, legacyChunks)
	if !strings.HasSuffix(legacyChunks[0].Text, "\n## ") {
		t.Fatalf("expected legacy placement to keep trailing separator on chunk 0, got %q", legacyChunks[0].Text)
	}

	// Delimiter-only separator ". " must remain attached to chunk N under SeparatorPlacementStart.
	prose := "First sentence about vectors. Second sentence about chunks. Third sentence about budgets."
	cfgProse := Config{
		Strategy:           StrategyRecursive,
		SizeUnit:           SizeUnitRunes,
		Size:               40,
		Overlap:            0,
		Separators:         []string{". ", " ", ""},
		SeparatorPlacement: SeparatorPlacementStart,
	}
	proseChunks := mustChunk(t, "prose", prose, cfgProse)
	assertChunkInvariants(t, prose, cfgProse, proseChunks)
	if len(proseChunks) < 3 {
		t.Fatalf("expected >=3 sentence chunks, got %d: %#v", len(proseChunks), proseChunks)
	}
	if !strings.HasSuffix(proseChunks[0].Text, ". ") {
		t.Fatalf("delimiter-only separator \". \" should stay on chunk 0, got %q", proseChunks[0].Text)
	}
	if strings.HasPrefix(proseChunks[1].Text, ". ") {
		t.Fatalf("chunk 1 must not begin with dangling \". \": %q", proseChunks[1].Text)
	}
}

func TestSeparatorPlacementStartNonNewlinePrefixTerminatesCleanly(t *testing.T) {
	cases := []struct {
		name string
		text string
		size int
	}{
		{name: "starts with prefix", text: "## alpha## beta## gamma", size: 12},
		{name: "mid prefix", text: "abc## def## ghi", size: 12},
		{name: "repeated prefix only", text: "## ## ## ## ## ", size: 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				Strategy:           StrategyRecursive,
				SizeUnit:           SizeUnitRunes,
				Size:               tc.size,
				Overlap:            0,
				Separators:         []string{"## ", ""},
				SeparatorPlacement: SeparatorPlacementStart,
			}
			chunks := mustChunk(t, "p", tc.text, cfg)
			assertChunkInvariants(t, tc.text, cfg, chunks)
			for i := 1; i < len(chunks); i++ {
				if !strings.HasPrefix(chunks[i].Text, "## ") {
					t.Fatalf("chunk %d did not start with structural prefix: %q", i, chunks[i].Text)
				}
				if chunks[i-1].Text != "## " && strings.HasSuffix(chunks[i-1].Text, "## ") {
					t.Fatalf("chunk %d ended with detached structural prefix: %q", i-1, chunks[i-1].Text)
				}
			}
		})
	}
}

func TestSeparatorPlacementStartCodeAndLaTeX(t *testing.T) {
	code := "package demo\n\nfunc Alpha() int {\n\treturn 1\n}\nfunc Beta() int {\n\treturn 2\n}\n"
	cfgCode := Config{
		Strategy:           StrategyRecursive,
		SizeUnit:           SizeUnitRunes,
		Size:               48,
		Overlap:            0,
		Separators:         CodeSeparators(),
		SeparatorPlacement: SeparatorPlacementStart,
	}
	codeChunks := mustChunk(t, "code", code, cfgCode)
	assertChunkInvariants(t, code, cfgCode, codeChunks)
	foundFuncAlpha := false
	foundFuncBeta := false
	for _, ch := range codeChunks {
		if strings.HasPrefix(ch.Text, "func Alpha") {
			foundFuncAlpha = true
		}
		if strings.HasPrefix(ch.Text, "func Beta") {
			foundFuncBeta = true
		}
	}
	if !foundFuncAlpha || !foundFuncBeta {
		t.Fatalf("expected func declarations at chunk starts, got %#v", codeChunks)
	}

	latex := "\\documentclass{article}\n\\section{Intro}\nShort intro text.\n\\section{Proof}\nTheorem details here."
	cfgLatex := Config{
		Strategy:           StrategyRecursive,
		SizeUnit:           SizeUnitRunes,
		Size:               52,
		Overlap:            0,
		Separators:         LaTeXSeparators(),
		SeparatorPlacement: SeparatorPlacementStart,
	}
	latexChunks := mustChunk(t, "tex", latex, cfgLatex)
	assertChunkInvariants(t, latex, cfgLatex, latexChunks)
	foundIntro := false
	foundProof := false
	for _, ch := range latexChunks {
		if strings.HasPrefix(ch.Text, "\\section{Intro}") {
			foundIntro = true
		}
		if strings.HasPrefix(ch.Text, "\\section{Proof}") {
			foundProof = true
		}
	}
	if !foundIntro || !foundProof {
		t.Fatalf("expected \\section commands at chunk starts, got %#v", latexChunks)
	}
}

func TestSnapOverlapToBoundaryPreservesCleanLineStart(t *testing.T) {
	// Chunk 0 splits on "\n## " with SeparatorPlacementStart -> ends right after "line2\n".
	// "line1\n" is 6 runes, "line2\n" is 6 runes, total chunk 0 = 12 runes.
	// With Overlap = 6, rawStart = 12 - 6 = 6, so runes[rawStart-1] == '\n'.
	// SnapOverlapToBoundary must keep rawStart = 6 ("line2\n...") and NOT skip "line2\n".
	doc := "line1\nline2\n## Heading\nline3\nline4\n"
	cfg := Config{
		Strategy:              StrategyRecursive,
		SizeUnit:              SizeUnitRunes,
		Size:                  20,
		Overlap:               6,
		Separators:            MarkdownSeparators(),
		SeparatorPlacement:    SeparatorPlacementStart,
		SnapOverlapToBoundary: true,
	}
	chunks := mustChunk(t, "line-snap", doc, cfg)
	assertChunkInvariants(t, doc, cfg, chunks)
	if len(chunks) < 2 {
		t.Fatalf("expected >=2 chunks, got %#v", chunks)
	}
	if chunks[0].Text != "line1\nline2\n" {
		t.Fatalf("chunk 0=%q want %q", chunks[0].Text, "line1\nline2\n")
	}
	if chunks[1].StartOffset != 6 || !strings.HasPrefix(chunks[1].Text, "line2\n## Heading") {
		t.Fatalf("chunk 1 skipped clean line boundary: start=%d text=%q", chunks[1].StartOffset, chunks[1].Text)
	}
}

func TestSnapOverlapToBoundarySnapsForwardToLineThenWordThenFallback(t *testing.T) {
	t.Run("mid-word snaps forward to newline before space", func(t *testing.T) {
		// Chunk 0: "alpha beta\ngamma\n" (len 17). With Overlap=10, rawStart=7 ("eta\ngamma\n").
		// First '\n' in [7, 17) with idx+1 < 17 is at idx=10 -> nextStart=11 ("gamma\n").
		doc := "alpha beta\ngamma\n## Next Section\nbody text"
		cfg := Config{
			Strategy:              StrategyRecursive,
			SizeUnit:              SizeUnitRunes,
			Size:                  26,
			Overlap:               10,
			Separators:            MarkdownSeparators(),
			SeparatorPlacement:    SeparatorPlacementStart,
			SnapOverlapToBoundary: true,
		}
		chunks := mustChunk(t, "p", doc, cfg)
		assertChunkInvariants(t, doc, cfg, chunks)
		if chunks[0].Text != "alpha beta\ngamma\n" {
			t.Fatalf("chunk 0=%q", chunks[0].Text)
		}
		if !strings.HasPrefix(chunks[1].Text, "gamma\n## Next") {
			t.Fatalf("chunk 1=%q want prefix %q", chunks[1].Text, "gamma\n## Next")
		}
	})

	t.Run("clean word boundary preserved when no newline ahead", func(t *testing.T) {
		// "aaaa bbbb cccc dddd" -> Chunk 0 with Size=14, Overlap=5:
		// splits after "cccc " at 15? Wait: 14 caps at "aaaa bbbb cccc". Last " " <= 14 is at 9 -> end=10 ("aaaa bbbb ").
		// rawStart = 10 - 5 = 5 -> runes[4] == ' ', runes[5] == 'b' ("bbbb ").
		doc := "aaaa bbbb cccc dddd eeee"
		cfg := Config{
			Strategy:              StrategyRecursive,
			SizeUnit:              SizeUnitRunes,
			Size:                  14,
			Overlap:               5,
			Separators:            []string{" ", ""},
			SnapOverlapToBoundary: true,
		}
		chunks := mustChunk(t, "p", doc, cfg)
		assertChunkInvariants(t, doc, cfg, chunks)
		if chunks[0].Text != "aaaa bbbb " {
			t.Fatalf("chunk 0=%q want %q", chunks[0].Text, "aaaa bbbb ")
		}
		if chunks[1].StartOffset != 5 || !strings.HasPrefix(chunks[1].Text, "bbbb ") {
			t.Fatalf("chunk 1=%+v want StartOffset=5 prefix \"bbbb \"", chunks[1])
		}
	})

	t.Run("mid-word snaps forward to next word boundary", func(t *testing.T) {
		// Chunk 0: "alpha beta gamma " (end=17? With Size=18, "alpha beta gamma " is 17).
		// Wait: "alpha_long beta gamma delta".
		// Chunk 0 with Size=16: last " " is after "beta" at idx=15 -> end=16 ("alpha_long beta ").
		// With Overlap=9, rawStart = 16 - 9 = 7 ("ong beta ").
		// No '\n', runes[6]=='l' (not space), first space at idx=10 -> nextStart=11 ("beta ").
		doc := "alpha_long beta gamma delta epsilon"
		cfg := Config{
			Strategy:              StrategyRecursive,
			SizeUnit:              SizeUnitRunes,
			Size:                  16,
			Overlap:               9,
			Separators:            []string{" ", ""},
			SnapOverlapToBoundary: true,
		}
		chunks := mustChunk(t, "p", doc, cfg)
		assertChunkInvariants(t, doc, cfg, chunks)
		if chunks[0].Text != "alpha_long beta " {
			t.Fatalf("chunk 0=%q", chunks[0].Text)
		}
		if chunks[1].StartOffset != 11 || !strings.HasPrefix(chunks[1].Text, "beta ") {
			t.Fatalf("chunk 1 did not snap to word start: start=%d text=%q", chunks[1].StartOffset, chunks[1].Text)
		}
		if r := []rune(chunks[1].Text)[0]; unicode.IsSpace(r) {
			t.Fatalf("chunk 1 starts with whitespace: %q", chunks[1].Text)
		}
	})

	t.Run("single unbroken token falls back to rawStart", func(t *testing.T) {
		doc := strings.Repeat("Z", 120)
		cfg := Config{
			Strategy:              StrategyRecursive,
			SizeUnit:              SizeUnitRunes,
			Size:                  40,
			Overlap:               10,
			Separators:            DefaultSeparators(),
			SeparatorPlacement:    SeparatorPlacementStart,
			SnapOverlapToBoundary: true,
		}
		chunks := mustChunk(t, "unbroken", doc, cfg)
		assertChunkInvariants(t, doc, cfg, chunks)
		if chunks[1].StartOffset != 30 {
			t.Fatalf("unbroken token chunk 1 start=%d want fallback rawStart=30", chunks[1].StartOffset)
		}
	})
}

func TestBoundaryOptionsEdgeCasesDeterminismAndUTF8(t *testing.T) {
	corpus := []struct {
		name string
		text string
		cfg  Config
	}{
		{
			name: "huge unbroken token",
			text: strings.Repeat("a", 500) + "\n## Tail\nend",
			cfg: Config{
				Strategy:              StrategyRecursive,
				SizeUnit:              SizeUnitRunes,
				Size:                  80,
				Overlap:               16,
				Separators:            MarkdownSeparators(),
				SeparatorPlacement:    SeparatorPlacementStart,
				SnapOverlapToBoundary: true,
			},
		},
		{
			name: "trailing heading",
			text: "some body text\n## ",
			cfg: Config{
				Strategy:              StrategyRecursive,
				SizeUnit:              SizeUnitRunes,
				Size:                  24,
				Overlap:               4,
				Separators:            MarkdownSeparators(),
				SeparatorPlacement:    SeparatorPlacementStart,
				SnapOverlapToBoundary: true,
			},
		},
		{
			name: "multibyte UTF-8 markdown",
			text: "序言段落内容。\n## 标题一 αβγ\n第一段包含多字节字符🚀🔥✨和公式说明。\n## 标题二 δϵζ\n第二段继续展开讨论。",
			cfg: Config{
				Strategy:              StrategyRecursive,
				SizeUnit:              SizeUnitRunes,
				Size:                  25,
				Overlap:               6,
				Separators:            MarkdownSeparators(),
				SeparatorPlacement:    SeparatorPlacementStart,
				SnapOverlapToBoundary: true,
			},
		},
		{
			name: "trailing spaces and newlines",
			text: "word1 word2   \n\n\nword3 word4\n",
			cfg: Config{
				Strategy:              StrategyRecursive,
				SizeUnit:              SizeUnitRunes,
				Size:                  12,
				Overlap:               4,
				Separators:            DefaultSeparators(),
				SeparatorPlacement:    SeparatorPlacementStart,
				SnapOverlapToBoundary: true,
			},
		},
	}

	for _, tc := range corpus {
		t.Run(tc.name, func(t *testing.T) {
			chunks := mustChunk(t, "edge", tc.text, tc.cfg)
			assertChunkInvariants(t, tc.text, tc.cfg, chunks)

			first := chunkStreamDigest(t, "edge", tc.text, tc.cfg)
			for i := 0; i < 3; i++ {
				if again := chunkStreamDigest(t, "edge", tc.text, tc.cfg); again != first {
					t.Fatalf("non-deterministic output on attempt %d: %s vs %s", i, first, again)
				}
			}
		})
	}
}

func TestDefaultPlacementBitIdenticalToLegacy(t *testing.T) {
	doc := "Intro.\n## One\nFirst body sentence. Second body sentence.\n## Two\nThird body sentence."
	cfgZero := Config{
		Strategy:   StrategyRecursive,
		SizeUnit:   SizeUnitRunes,
		Size:       36,
		Overlap:    8,
		Separators: MarkdownSeparators(),
	}
	cfgEnd := cfgZero
	cfgEnd.SeparatorPlacement = SeparatorPlacementEnd

	dZero := chunkStreamDigest(t, "doc", doc, cfgZero)
	dEnd := chunkStreamDigest(t, "doc", doc, cfgEnd)
	if dZero != dEnd {
		t.Fatalf("zero-value placement digest %s != SeparatorPlacementEnd digest %s", dZero, dEnd)
	}
}

func benchMarkdownCorpus(docCount int) []string {
	sections := []string{
		"# Overview\nRetrieval quality in a RAG pipeline is bounded by the granularity of the units that reach the embedder. " +
			"Oversized chunks dilute the signal per token; undersized chunks lose the local context needed to disambiguate terms.",
		"## Storage Layout\nTreeDB collections store documents as opaque JSON payloads with optional text, scalar, and vector indexes attached.\n" +
			"The chunking seam sits between raw source ingestion and index maintenance: it derives child documents cleanly.",
		"## Determinism Contract\nDeterminism matters because golden fixtures compare chunk streams across runs and across machines.\n" +
			"A chunker that consults clocks, randomness, or map iteration cannot produce hash-stable output.",
		"### Re-Chunking Lifecycle\nRe-chunking an updated parent must tombstone stale children before inserting replacements so indexes never mix generations.",
	}
	docs := make([]string, docCount)
	var b bytes.Buffer
	for i := range docs {
		b.Reset()
		for s := 0; s < 4; s++ {
			b.WriteString(sections[(i+s)%len(sections)])
			if s < 3 {
				b.WriteString("\n\n")
			}
		}
		docs[i] = b.String()
	}
	return docs
}

func BenchmarkChunkRecursiveBoundaryStartSnap10K(b *testing.B) {
	docs := benchMarkdownCorpus(benchDocCount)
	cfg := Config{
		Strategy:              StrategyRecursive,
		SizeUnit:              SizeUnitRunes,
		Size:                  512,
		Overlap:               64,
		Separators:            MarkdownSeparators(),
		SeparatorPlacement:    SeparatorPlacementStart,
		SnapOverlapToBoundary: true,
	}
	benchmarkChunkCorpus(b, docs, cfg)
}
