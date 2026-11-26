package collections

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func openOrdinaryBlobTestCollection(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	mgr := NewCollectionManager(d)
	closeAll := func() error {
		err1 := mgr.Close()
		err2 := cleanup()
		return errors.Join(err1, err2)
	}
	if create {
		if _, err := mgr.CreateCollection(meta); err != nil {
			_ = closeAll()
			t.Fatalf("CreateCollection: %v", err)
		}
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = closeAll()
		t.Fatalf("OpenCollection(%s): %v", meta.Name, err)
	}
	return mgr, col, closeAll
}

func openNativeCOWBlobTestCollection(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error) {
	return openNativeCOWBlobTestCollectionWithLimits(t, dir, create, meta, 0)
}

func openNativeCOWBlobTestCollectionWithLimits(t *testing.T, dir string, create bool, meta *CollectionMeta, minInFlightBytes uint64) (*CollectionManager, *Collection, func() error) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	if minInFlightBytes > 0 {
		limits := treedb.DefaultCOWMemtableLimits()
		if limits.MaxGenerationBytes < minInFlightBytes {
			limits.MaxGenerationBytes = minInFlightBytes
		}
		if limits.MaxInFlightBytes < minInFlightBytes {
			limits.MaxInFlightBytes = minInFlightBytes
		}
		if limits.MaxTotalBytes < 2*minInFlightBytes {
			limits.MaxTotalBytes = 2 * minInFlightBytes
		}
		if limits.MaxRetiredBytes < 2*minInFlightBytes {
			limits.MaxRetiredBytes = 2 * minInFlightBytes
		}
		opts.COWMemtableLimits = limits
	}
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("treedb.Open(cow_btree): %v", err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		_ = owner.Close()
		t.Fatalf("NativeSemanticBackend: %v", err)
	}
	mgr := NewCollectionManager(backend)
	closeAll := func() error {
		err1 := mgr.Close()
		err2 := owner.Close()
		return errors.Join(err1, err2)
	}
	if create {
		if _, err := mgr.CreateCollection(meta); err != nil {
			_ = closeAll()
			t.Fatalf("CreateCollection(cow_btree): %v", err)
		}
	}
	col, err := mgr.OpenCollection(meta.Name)
	if err != nil {
		_ = closeAll()
		t.Fatalf("OpenCollection(%s): %v", meta.Name, err)
	}
	return mgr, col, closeAll
}

func analyzeWithParityCheck(t *testing.T, options *TextAnalyzerOptions, input string) []TextToken {
	t.Helper()
	got, err := AnalyzeTextWithOptions(TextAnalyzerSimple, options, input)
	if err != nil {
		t.Fatalf("AnalyzeTextWithOptions(%+v): %v", options, err)
	}
	sink := &normalizerGrowthTestSink{admit: func(uint64) error { return nil }}
	if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, options, input, sink); err != nil {
		t.Fatalf("AnalyzeTextToSinkWithOptions(admitted, %+v): %v", options, err)
	}
	gotAdmitted := sink.tokens
	if gotAdmitted == nil {
		gotAdmitted = []TextToken{}
	}
	if !slices.Equal(got, gotAdmitted) {
		t.Fatalf("ordinary vs admitted token mismatch:\nordinary=%+v\nadmitted=%+v", got, gotAdmitted)
	}
	return got
}

func TestSimpleTextAnalyzerMaxTokenBytesBoundaries(t *testing.T) {
	tok256 := strings.Repeat("a", DefaultSimpleTextMaxTokenBytes)
	tok257 := strings.Repeat("b", DefaultSimpleTextMaxTokenBytes+1)
	tok1024 := strings.Repeat("c", MaxSimpleTextMaxTokenBytes)
	tok1025 := strings.Repeat("d", MaxSimpleTextMaxTokenBytes+1)

	t.Run("default_256_boundary", func(t *testing.T) {
		input := tok256 + " " + tok257 + " tail"
		for _, opts := range []*TextAnalyzerOptions{nil, {}} {
			tokens := analyzeWithParityCheck(t, opts, input)
			want := []TextToken{
				{Term: tok256, Position: 0, StartOffset: 0, EndOffset: 256},
				{Term: "tail", Position: 2, StartOffset: 256 + 1 + 257 + 1, EndOffset: len(input)},
			}
			if !slices.Equal(tokens, want) {
				t.Fatalf("opts=%+v got=%+v want=%+v", opts, tokens, want)
			}
		}
	})

	t.Run("explicit_max_1024_boundary", func(t *testing.T) {
		input := tok1024 + " " + tok1025 + " end"
		opts := &TextAnalyzerOptions{MaxTokenBytes: MaxSimpleTextMaxTokenBytes}
		tokens := analyzeWithParityCheck(t, opts, input)
		want := []TextToken{
			{Term: tok1024, Position: 0, StartOffset: 0, EndOffset: 1024},
			{Term: "end", Position: 2, StartOffset: 1024 + 1 + 1025 + 1, EndOffset: len(input)},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("got=%+v want=%+v", tokens, want)
		}
	})

	t.Run("oversized_at_eof_and_consecutive_gaps", func(t *testing.T) {
		input := "alpha " + tok257 + " " + tok257 + " beta " + tok257
		tokens := analyzeWithParityCheck(t, nil, input)
		want := []TextToken{
			{Term: "alpha", Position: 0, StartOffset: 0, EndOffset: 5},
			{Term: "beta", Position: 3, StartOffset: 6 + len(tok257) + 1 + len(tok257) + 1, EndOffset: 6 + len(tok257) + 1 + len(tok257) + 1 + 4},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("got=%+v want=%+v", tokens, want)
		}
	})
}

func TestSimpleTextAnalyzerUTF8Boundaries(t *testing.T) {
	t.Run("first_rune_2byte_overflow_does_not_latch", func(t *testing.T) {
		opts := &TextAnalyzerOptions{MaxTokenBytes: 1}
		tokens := analyzeWithParityCheck(t, opts, "é a")
		want := []TextToken{
			{Term: "a", Position: 1, StartOffset: len("é "), EndOffset: len("é a")},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("got=%+v want=%+v", tokens, want)
		}

		// Admitted path must not charge builder growth for the leading oversized rune.
		var charges int
		sink := &normalizerGrowthTestSink{admit: func(uint64) error {
			charges++
			return nil
		}}
		if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, opts, "é", sink); err != nil {
			t.Fatalf("AnalyzeTextToSinkWithOptions: %v", err)
		}
		if len(sink.tokens) != 0 || charges != 0 {
			t.Fatalf("leading oversized rune tokens=%+v charges=%d want 0/0", sink.tokens, charges)
		}
	})

	t.Run("first_rune_3byte_overflow_does_not_latch", func(t *testing.T) {
		opts := &TextAnalyzerOptions{MaxTokenBytes: 2}
		tokens := analyzeWithParityCheck(t, opts, "中 ok")
		want := []TextToken{
			{Term: "ok", Position: 1, StartOffset: len("中 "), EndOffset: len("中 ok")},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("got=%+v want=%+v", tokens, want)
		}
	})

	t.Run("first_rune_4byte_overflow_does_not_latch", func(t *testing.T) {
		opts := &TextAnalyzerOptions{MaxTokenBytes: 3}
		tokens := analyzeWithParityCheck(t, opts, "𐐀 ok")
		want := []TextToken{
			{Term: "ok", Position: 1, StartOffset: len("𐐀 "), EndOffset: len("𐐀 ok")},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("got=%+v want=%+v", tokens, want)
		}
	})

	t.Run("mid_token_utf8_and_case_expansion_boundary", func(t *testing.T) {
		// "ab中" -> 2 + 3 = 5 bytes (> 4), skipped; "cd" emitted at pos 1.
		opts4 := &TextAnalyzerOptions{MaxTokenBytes: 4}
		tokens := analyzeWithParityCheck(t, opts4, "ab中 cd")
		want := []TextToken{
			{Term: "cd", Position: 1, StartOffset: len("ab中 "), EndOffset: len("ab中 cd")},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("mid-token CJK got=%+v want=%+v", tokens, want)
		}

		// "aaȺ": uppercase Ⱥ is 2 bytes, lowercased ⱥ is 3 bytes -> total 5 bytes.
		// Under MaxTokenBytes=4 it must be skipped; under MaxTokenBytes=5 it is emitted.
		tokens4 := analyzeWithParityCheck(t, opts4, "aaȺ cd")
		want4 := []TextToken{
			{Term: "cd", Position: 1, StartOffset: len("aaȺ "), EndOffset: len("aaȺ cd")},
		}
		if !slices.Equal(tokens4, want4) {
			t.Fatalf("case-expansion cap=4 got=%+v want=%+v", tokens4, want4)
		}

		opts5 := &TextAnalyzerOptions{MaxTokenBytes: 5}
		tokens5 := analyzeWithParityCheck(t, opts5, "aaȺ cd")
		want5 := []TextToken{
			{Term: "aaⱥ", Position: 0, StartOffset: 0, EndOffset: len("aaȺ")},
			{Term: "cd", Position: 1, StartOffset: len("aaȺ "), EndOffset: len("aaȺ cd")},
		}
		if !slices.Equal(tokens5, want5) {
			t.Fatalf("case-expansion cap=5 got=%+v want=%+v", tokens5, want5)
		}
	})
}

func TestSimpleTextAnalyzerOversizedBlobsOrdinaryAndNativeCOW(t *testing.T) {
	cases := []struct {
		name     string
		version  TextIndexVersion
		backfill bool
		sizes    []int
		open     func(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error)
	}{
		{name: "ordinary/v2/backfill=false", version: TextIndexVersionV2, backfill: false, sizes: []int{4 * 1024, 64 * 1024, 473 * 1024}, open: openOrdinaryBlobTestCollection},
		{name: "ordinary/v2/backfill=true", version: TextIndexVersionV2, backfill: true, sizes: []int{4 * 1024, 64 * 1024, 473 * 1024}, open: openOrdinaryBlobTestCollection},
		{name: "ordinary/v1/backfill=false", version: TextIndexVersionV1, backfill: false, sizes: []int{4 * 1024, 64 * 1024, 473 * 1024}, open: openOrdinaryBlobTestCollection},
		{name: "native_cow/v2/backfill=false", version: TextIndexVersionV2, backfill: false, sizes: []int{4 * 1024, 64 * 1024, 473 * 1024}, open: openNativeCOWBlobTestCollection},
		{name: "native_cow/v2/backfill=true/default_limits_4k", version: TextIndexVersionV2, backfill: true, sizes: []int{4 * 1024}, open: openNativeCOWBlobTestCollection},
		{
			// Native backfill scans primary rows through the conservative general
			// record envelope (~720x raw bytes per scan) plus nativeTextSourceCharge;
			// raise COW in-flight budget (matching native_ordinary_columngraph_optimize_test)
			// so 4 KiB/64 KiB/473 KiB backfill exercises the bounded analyzer and
			// text-v2 B-tree key path end-to-end.
			name:     "native_cow/v2/backfill=true/4k_64k_473k",
			version:  TextIndexVersionV2,
			backfill: true,
			sizes:    []int{4 * 1024, 64 * 1024, 473 * 1024},
			open: func(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error) {
				return openNativeCOWBlobTestCollectionWithLimits(t, dir, create, meta, 2<<30)
			},
		},
		{
			name:     "native_cow/v1/backfill=false/4k_64k_473k",
			version:  TextIndexVersionV1,
			backfill: false,
			sizes:    []int{4 * 1024, 64 * 1024, 473 * 1024},
			open: func(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error) {
				return openNativeCOWBlobTestCollectionWithLimits(t, dir, create, meta, 1<<30)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sizes := tc.sizes
			version := tc.version
			backfill := tc.backfill
			mode := tc
			idxDef := TextIndexDefinition{
				Name:            "lexical",
				Version:         version,
				Fields:          []TextIndexField{{Field: "body"}},
				Analyzer:        TextAnalyzerSimple,
				AnalyzerOptions: &TextAnalyzerOptions{},
				StorePositions:  true,
				StoreOffsets:    true,
			}
			meta := &CollectionMeta{Name: "docs"}
			if !backfill {
				meta.TextIndexes = []TextIndexDefinition{idxDef}
			}
			_, col, closeAll := mode.open(t, t.TempDir(), true, meta)
			defer func() {
				if err := closeAll(); err != nil {
					t.Errorf("close: %v", err)
				}
			}()

			ids := make([][]byte, 0, len(sizes))
			docs := make([][]byte, 0, len(sizes))
			for i, sz := range sizes {
				// Contiguous base64/alphanumeric-like token with letters, digits, and _.
				blob := strings.Repeat("AbCd0123_XyZ9876", sz/16)
				if rem := sz - len(blob); rem > 0 {
					blob += strings.Repeat("Q", rem)
				}
				ids = append(ids, []byte(fmt.Sprintf("doc-%d", i)))
				docs = append(docs, []byte(fmt.Sprintf(`{"body":"alpha_%d %s beta_%d"}`, i, blob, i)))
			}

			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatalf("InsertBatch(sizes=%v): %v", sizes, err)
			}
			if backfill {
				if _, _, err := col.CreateTextIndex(idxDef); err != nil {
					t.Fatalf("CreateTextIndex backfill(sizes=%v): %v", sizes, err)
				}
			}

			for i := range sizes {
				wantID := fmt.Sprintf("doc-%d", i)
				for _, q := range []string{fmt.Sprintf("alpha_%d", i), fmt.Sprintf("beta_%d", i)} {
					resp, err := col.SearchText(TextSearchOptions{
						IndexName: "lexical",
						Query:     q,
						TopK:      5,
					})
					if err != nil {
						t.Fatalf("SearchText(%s): %v", q, err)
					}
					if len(resp.Results) != 1 || string(resp.Results[0].DocumentID) != wantID {
						t.Fatalf("SearchText(%s) results=%+v want %s", q, resp.Results, wantID)
					}
				}
			}

			// Oversized query token returns empty results cleanly.
			oversizedQuery := strings.Repeat("A", 1024)
			for _, qmode := range []TextSearchQueryMode{TextSearchQueryModeBoolean, TextSearchQueryModeLiteral} {
				resp, err := col.SearchText(TextSearchOptions{
					IndexName: "lexical",
					Query:     oversizedQuery,
					QueryMode: qmode,
					TopK:      5,
				})
				if err != nil {
					t.Fatalf("SearchText(oversized, mode=%s): %v", qmode, err)
				}
				if len(resp.Results) != 0 || resp.Stats.QueryTerms != 0 {
					t.Fatalf("SearchText(oversized, mode=%s) resp=%+v want empty", qmode, resp)
				}
			}
		})
	}
}

func TestSimpleTextAnalyzerPhraseGapAndOversizedQuery(t *testing.T) {
	modes := []struct {
		name string
		open func(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error)
	}{
		{name: "ordinary", open: openOrdinaryBlobTestCollection},
		{name: "native_cow", open: openNativeCOWBlobTestCollection},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			meta := &CollectionMeta{
				Name: "docs",
				TextIndexes: []TextIndexDefinition{
					{
						Name:           "lexical",
						Version:        TextIndexVersionV2,
						Fields:         []TextIndexField{{Field: "body"}},
						Analyzer:       TextAnalyzerSimple,
						StorePositions: true,
					},
				},
			}
			_, col, closeAll := mode.open(t, t.TempDir(), true, meta)
			defer func() {
				if err := closeAll(); err != nil {
					t.Errorf("close: %v", err)
				}
			}()

			blob512 := strings.Repeat("B", 512)
			if _, err := col.InsertBatch(
				[][]byte{[]byte("gapped"), []byte("adjacent")},
				[][]byte{
					[]byte(fmt.Sprintf(`{"body":"alpha %s beta"}`, blob512)),
					[]byte(`{"body":"alpha beta"}`),
				},
			); err != nil {
				t.Fatalf("InsertBatch: %v", err)
			}

			// Exact phrase "alpha beta" (Slop 0) matches only "adjacent", NOT "gapped".
			slop0, err := col.SearchText(TextSearchOptions{
				IndexName: "lexical",
				Phrase:    &TextSearchPhraseQuery{Query: "alpha beta", Slop: 0},
				TopK:      5,
			})
			if err != nil {
				t.Fatalf("Phrase(Slop=0): %v", err)
			}
			if len(slop0.Results) != 1 || string(slop0.Results[0].DocumentID) != "adjacent" {
				t.Fatalf("Phrase(Slop=0) results=%+v want [adjacent]", slop0.Results)
			}

			// Slop 1 matches both "adjacent" and "gapped".
			slop1, err := col.SearchText(TextSearchOptions{
				IndexName: "lexical",
				Phrase:    &TextSearchPhraseQuery{Query: "alpha beta", Slop: 1},
				TopK:      5,
			})
			if err != nil {
				t.Fatalf("Phrase(Slop=1): %v", err)
			}
			gotIDs := make([]string, 0, len(slop1.Results))
			for _, r := range slop1.Results {
				gotIDs = append(gotIDs, string(r.DocumentID))
			}
			slices.Sort(gotIDs)
			if !slices.Equal(gotIDs, []string{"adjacent", "gapped"}) {
				t.Fatalf("Phrase(Slop=1) ids=%v want [adjacent gapped]", gotIDs)
			}

			// Phrase query with embedded oversized token preserves gap=1: matches "gapped" at Slop=0, NOT "adjacent".
			queryOversized := strings.Repeat("Z", 512)
			gappedQuery, err := col.SearchText(TextSearchOptions{
				IndexName: "lexical",
				Phrase:    &TextSearchPhraseQuery{Query: "alpha " + queryOversized + " beta", Slop: 0},
				TopK:      5,
			})
			if err != nil {
				t.Fatalf("Phrase(embedded oversized): %v", err)
			}
			if len(gappedQuery.Results) != 1 || string(gappedQuery.Results[0].DocumentID) != "gapped" {
				t.Fatalf("Phrase(embedded oversized) results=%+v want [gapped]", gappedQuery.Results)
			}

			// Phrase query consisting solely of oversized token returns empty cleanly.
			onlyOversized, err := col.SearchText(TextSearchOptions{
				IndexName: "lexical",
				Phrase:    &TextSearchPhraseQuery{Query: queryOversized, Slop: 0},
				TopK:      5,
			})
			if err != nil {
				t.Fatalf("Phrase(only oversized): %v", err)
			}
			if len(onlyOversized.Results) != 0 {
				t.Fatalf("Phrase(only oversized) results=%+v want empty", onlyOversized.Results)
			}
		})
	}
}

func TestSimpleTextAnalyzerOptionsValidationAndPersistence(t *testing.T) {
	t.Run("clone_and_equality", func(t *testing.T) {
		a := &TextAnalyzerOptions{MaxTokenBytes: 16}
		b := cloneTextAnalyzerOptions(a)
		if !textAnalyzerOptionsEqual(a, b) || b.MaxTokenBytes != 16 {
			t.Fatalf("clone/equal failed: a=%+v b=%+v", a, b)
		}
		b.MaxTokenBytes = 32
		if textAnalyzerOptionsEqual(a, b) {
			t.Fatalf("expected inequality for differing MaxTokenBytes: a=%+v b=%+v", a, b)
		}
		if textAnalyzerOptionsEqual(a, nil) {
			t.Fatal("expected inequality against nil")
		}
	})

	t.Run("normalize_preserves_max_token_bytes_without_stopwords", func(t *testing.T) {
		got, err := normalizeTextAnalyzerOptions(TextAnalyzerSimple, &TextAnalyzerOptions{MaxTokenBytes: 16})
		if err != nil {
			t.Fatalf("normalizeTextAnalyzerOptions: %v", err)
		}
		if got == nil || got.MaxTokenBytes != 16 || len(got.StopWords) != 0 {
			t.Fatalf("got=%+v want &TextAnalyzerOptions{MaxTokenBytes: 16}", got)
		}
		empty, err := normalizeTextAnalyzerOptions(TextAnalyzerSimple, &TextAnalyzerOptions{})
		if err != nil || empty != nil {
			t.Fatalf("empty options got=%+v err=%v want nil, nil", empty, err)
		}
	})

	t.Run("validation_ordinary_and_native_admitted_paths", func(t *testing.T) {
		denied := errors.New("unexpected allocation on invalid options")
		denyAdmit := func(uint64) error { return denied }
		okAdmit := func(uint64) error { return nil }

		// Reject -1 and 1025 with empty StopWords on BOTH paths, BEFORE any storage admission.
		for _, badMax := range []int{-1, MaxSimpleTextMaxTokenBytes + 1} {
			opts := &TextAnalyzerOptions{MaxTokenBytes: badMax}
			if _, err := normalizeTextAnalyzerOptions(TextAnalyzerSimple, opts); err == nil {
				t.Fatalf("normalizeTextAnalyzerOptions accepted MaxTokenBytes=%d", badMax)
			}
			if _, err := AnalyzeTextWithOptions(TextAnalyzerSimple, opts, "hello"); err == nil {
				t.Fatalf("AnalyzeTextWithOptions accepted MaxTokenBytes=%d", badMax)
			}
			if _, err := nativeTextStopWords(TextAnalyzerSimple, opts, denyAdmit); err == nil || errors.Is(err, denied) {
				t.Fatalf("nativeTextStopWords(%d) err=%v want validation error before admission", badMax, err)
			}
			sink := &normalizerGrowthTestSink{admit: denyAdmit}
			if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, opts, "hello", sink); err == nil || errors.Is(err, denied) {
				t.Fatalf("AnalyzeTextToSinkWithOptions(admitted, %d) err=%v want validation error before admission", badMax, err)
			}
		}

		// Valid MaxTokenBytes=16 with empty StopWords allocates nothing in nativeTextStopWords.
		if words, err := nativeTextStopWords(TextAnalyzerSimple, &TextAnalyzerOptions{MaxTokenBytes: 16}, denyAdmit); err != nil || len(words) != 0 {
			t.Fatalf("nativeTextStopWords(MaxTokenBytes=16, empty StopWords) words=%v err=%v", words, err)
		}

		// Oversized stopwords rejected on BOTH ordinary and native admitted paths,
		// including mixed strings containing both an oversized token and a short token.
		overCases := []struct {
			name string
			opts *TextAnalyzerOptions
		}{
			{name: "default_257", opts: &TextAnalyzerOptions{StopWords: []string{strings.Repeat("a", 257)}}},
			{name: "default_mixed_prefix", opts: &TextAnalyzerOptions{StopWords: []string{strings.Repeat("a", 257) + " the"}}},
			{name: "default_mixed_suffix", opts: &TextAnalyzerOptions{StopWords: []string{"the " + strings.Repeat("a", 257)}}},
			{name: "custom_16_over_17", opts: &TextAnalyzerOptions{MaxTokenBytes: 16, StopWords: []string{strings.Repeat("x", 17)}}},
			{name: "custom_1_utf8_2byte", opts: &TextAnalyzerOptions{MaxTokenBytes: 1, StopWords: []string{"é"}}},
		}
		for _, tc := range overCases {
			t.Run(tc.name, func(t *testing.T) {
				ordErr := func() error {
					_, err := normalizeTextAnalyzerOptions(TextAnalyzerSimple, tc.opts)
					return err
				}()
				if ordErr == nil {
					t.Fatalf("normalizeTextAnalyzerOptions accepted oversized stopword %+v", tc.opts)
				}
				_, natErr := nativeTextStopWords(TextAnalyzerSimple, tc.opts, okAdmit)
				if natErr == nil {
					t.Fatalf("nativeTextStopWords accepted oversized stopword %+v", tc.opts)
				}
				if ordErr.Error() != natErr.Error() {
					t.Fatalf("error mismatch ordinary=%q native=%q", ordErr.Error(), natErr.Error())
				}
				sink := &normalizerGrowthTestSink{admit: okAdmit}
				if err := AnalyzeTextToSinkWithOptions(TextAnalyzerSimple, tc.opts, "hello", sink); err == nil {
					t.Fatalf("AnalyzeTextToSinkWithOptions(admitted) accepted oversized stopword %+v", tc.opts)
				}
			})
		}

		// Valid 300-byte stopword under MaxTokenBytes: 1024 accepted on BOTH paths and filters cleanly.
		stop300 := strings.Repeat("s", 300)
		validOpts := &TextAnalyzerOptions{MaxTokenBytes: MaxSimpleTextMaxTokenBytes, StopWords: []string{strings.ToUpper(stop300)}}
		norm, err := normalizeTextAnalyzerOptions(TextAnalyzerSimple, validOpts)
		if err != nil {
			t.Fatalf("normalizeTextAnalyzerOptions(300-byte stopword under 1024): %v", err)
		}
		if norm == nil || norm.MaxTokenBytes != MaxSimpleTextMaxTokenBytes || !slices.Equal(norm.StopWords, []string{stop300}) {
			t.Fatalf("normalized=%+v", norm)
		}
		words, err := nativeTextStopWords(TextAnalyzerSimple, validOpts, okAdmit)
		if err != nil || !slices.Equal(words, []string{stop300}) {
			t.Fatalf("nativeTextStopWords(300-byte stopword under 1024) words=%v err=%v", words, err)
		}
		tokens := analyzeWithParityCheck(t, validOpts, "alpha "+stop300+" beta")
		want := []TextToken{
			{Term: "alpha", Position: 0, StartOffset: 0, EndOffset: 5},
			{Term: "beta", Position: 2, StartOffset: 6 + len(stop300) + 1, EndOffset: 6 + len(stop300) + 1 + 4},
		}
		if !slices.Equal(tokens, want) {
			t.Fatalf("300-byte stopword tokens=%+v want=%+v", tokens, want)
		}
	})

	t.Run("persistence_across_reopen_and_ddl_rejection", func(t *testing.T) {
		modes := []struct {
			name string
			open func(t *testing.T, dir string, create bool, meta *CollectionMeta) (*CollectionManager, *Collection, func() error)
		}{
			{name: "ordinary", open: openOrdinaryBlobTestCollection},
			{name: "native_cow", open: openNativeCOWBlobTestCollection},
		}

		for _, mode := range modes {
			t.Run(mode.name, func(t *testing.T) {
				dir := t.TempDir()
				meta := &CollectionMeta{
					Name: "docs",
					TextIndexes: []TextIndexDefinition{
						{
							Name:            "short_tokens",
							Version:         TextIndexVersionV2,
							Fields:          []TextIndexField{{Field: "body"}},
							Analyzer:        TextAnalyzerSimple,
							AnalyzerOptions: &TextAnalyzerOptions{MaxTokenBytes: 16},
							StorePositions:  true,
						},
					},
				}

				_, col, closeAll := mode.open(t, dir, true, meta)

				// Invalid MaxTokenBytes and oversized stopwords rejected by CreateTextIndex.
				for _, badDef := range []TextIndexDefinition{
					{Name: "neg", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}, AnalyzerOptions: &TextAnalyzerOptions{MaxTokenBytes: -1}},
					{Name: "over", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}, AnalyzerOptions: &TextAnalyzerOptions{MaxTokenBytes: MaxSimpleTextMaxTokenBytes + 1}},
					{Name: "overstop", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}, AnalyzerOptions: &TextAnalyzerOptions{MaxTokenBytes: 16, StopWords: []string{strings.Repeat("x", 17)}}},
				} {
					if _, _, err := col.CreateTextIndex(badDef); err == nil {
						t.Fatalf("CreateTextIndex accepted invalid def %+v", badDef)
					}
				}

				// Valid 1024 + 300-byte stopword index creation succeeds.
				stop300 := strings.Repeat("k", 300)
				if _, _, err := col.CreateTextIndex(TextIndexDefinition{
					Name:            "wide_tokens",
					Version:         TextIndexVersionV2,
					Fields:          []TextIndexField{{Field: "body"}},
					Analyzer:        TextAnalyzerSimple,
					AnalyzerOptions: &TextAnalyzerOptions{MaxTokenBytes: MaxSimpleTextMaxTokenBytes, StopWords: []string{stop300}},
				}); err != nil {
					t.Fatalf("CreateTextIndex(MaxTokenBytes=1024, 300B stopword): %v", err)
				}

				if err := closeAll(); err != nil {
					t.Fatalf("close before reopen: %v", err)
				}

				// Reopen and verify AnalyzerOptions{MaxTokenBytes: 16} (empty StopWords!) persisted intact.
				_, col2, closeAll2 := mode.open(t, dir, false, &CollectionMeta{Name: "docs"})
				defer func() {
					if err := closeAll2(); err != nil {
						t.Errorf("close after reopen: %v", err)
					}
				}()

				reopenedMeta := col2.Meta()
				shortIdx, ok := findTextIndex(reopenedMeta.TextIndexes, "short_tokens")
				if !ok {
					t.Fatalf("missing short_tokens after reopen: %+v", reopenedMeta.TextIndexes)
				}
				wantShortOpts := &TextAnalyzerOptions{MaxTokenBytes: 16}
				if !reflect.DeepEqual(shortIdx.AnalyzerOptions, wantShortOpts) {
					t.Fatalf("reopened short_tokens AnalyzerOptions=%+v want %+v", shortIdx.AnalyzerOptions, wantShortOpts)
				}

				wideIdx, ok := findTextIndex(reopenedMeta.TextIndexes, "wide_tokens")
				if !ok {
					t.Fatalf("missing wide_tokens after reopen: %+v", reopenedMeta.TextIndexes)
				}
				wantWideOpts := &TextAnalyzerOptions{MaxTokenBytes: MaxSimpleTextMaxTokenBytes, StopWords: []string{stop300}}
				if !reflect.DeepEqual(wideIdx.AnalyzerOptions, wantWideOpts) {
					t.Fatalf("reopened wide_tokens AnalyzerOptions=%+v want %+v", wideIdx.AnalyzerOptions, wantWideOpts)
				}

				// Insert after reopen and verify effective MaxTokenBytes=16 behavior.
				tok16 := strings.Repeat("m", 16)
				tok17 := strings.Repeat("n", 17)
				if _, err := col2.InsertBatch([][]byte{[]byte("d1")}, [][]byte{
					[]byte(fmt.Sprintf(`{"body":"%s %s %s"}`, tok16, tok17, stop300)),
				}); err != nil {
					t.Fatalf("InsertBatch after reopen: %v", err)
				}

				hit16, err := col2.SearchText(TextSearchOptions{IndexName: "short_tokens", Query: tok16, TopK: 5})
				if err != nil || len(hit16.Results) != 1 {
					t.Fatalf("SearchText(tok16) results=%+v err=%v", hit16, err)
				}
				miss17, err := col2.SearchText(TextSearchOptions{IndexName: "short_tokens", Query: tok17, TopK: 5})
				if err != nil || len(miss17.Results) != 0 {
					t.Fatalf("SearchText(tok17 under cap=16) results=%+v err=%v want empty", miss17, err)
				}
				hit17Wide, err := col2.SearchText(TextSearchOptions{IndexName: "wide_tokens", Query: tok17, TopK: 5})
				if err != nil || len(hit17Wide.Results) != 1 {
					t.Fatalf("SearchText(tok17 under cap=1024) results=%+v err=%v", hit17Wide, err)
				}
				stopWide, err := col2.SearchText(TextSearchOptions{IndexName: "wide_tokens", Query: stop300, TopK: 5})
				if err != nil || len(stopWide.Results) != 0 {
					t.Fatalf("SearchText(stop300 under cap=1024) results=%+v err=%v want empty", stopWide, err)
				}
			})
		}
	})
}

func TestExtractTextSnippetsOversizedTokens(t *testing.T) {
	blob := strings.Repeat("A", 64*1024)
	text := "prefix alpha " + blob + " beta suffix"

	snips, err := ExtractTextSnippets(TextAnalyzerSimple, nil, text, "alpha", TextSnippetOptions{
		MaxSnippets:   1,
		WindowRunes:   40,
		HighlightPre:  "[",
		HighlightPost: "]",
	})
	if err != nil {
		t.Fatalf("ExtractTextSnippets(alpha): %v", err)
	}
	if len(snips) != 1 || !strings.Contains(snips[0].Text, "[alpha]") || !slices.Equal(snips[0].MatchedTerms, []string{"alpha"}) {
		t.Fatalf("ExtractTextSnippets(alpha) got=%+v", snips)
	}

	// Query with only an oversized token yields empty snippets cleanly.
	emptySnips, err := ExtractTextSnippets(TextAnalyzerSimple, nil, text, strings.Repeat("A", 1024), TextSnippetOptions{})
	if err != nil || len(emptySnips) != 0 {
		t.Fatalf("ExtractTextSnippets(oversized query) snips=%+v err=%v want empty", emptySnips, err)
	}

	// Custom MaxTokenBytes=16 skips 17-byte token in both query and text.
	tok16 := strings.Repeat("p", 16)
	tok17 := strings.Repeat("q", 17)
	opts16 := &TextAnalyzerOptions{MaxTokenBytes: 16}
	snips16, err := ExtractTextSnippets(TextAnalyzerSimple, opts16, tok16+" "+tok17, tok16+" "+tok17, TextSnippetOptions{
		HighlightPre:  "<",
		HighlightPost: ">",
	})
	if err != nil {
		t.Fatalf("ExtractTextSnippets(cap=16): %v", err)
	}
	if len(snips16) != 1 || !slices.Equal(snips16[0].MatchedTerms, []string{tok16}) {
		t.Fatalf("ExtractTextSnippets(cap=16) got=%+v want matched [%s]", snips16, tok16)
	}

	// Invalid MaxTokenBytes returns an error.
	if _, err := ExtractTextSnippets(TextAnalyzerSimple, &TextAnalyzerOptions{MaxTokenBytes: -1}, text, "alpha", TextSnippetOptions{}); err == nil {
		t.Fatal("ExtractTextSnippets accepted MaxTokenBytes=-1")
	}
}

func BenchmarkSimpleTextAnalyzerOversizedBlob(b *testing.B) {
	text := "alpha " + strings.Repeat("A", 64*1024) + " beta"
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		tokens, err := AnalyzeText(TextAnalyzerSimple, text)
		if err != nil {
			b.Fatalf("AnalyzeText: %v", err)
		}
		if len(tokens) != 2 {
			b.Fatalf("len(tokens)=%d want 2", len(tokens))
		}
	}
}
