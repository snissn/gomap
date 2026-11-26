package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func assertTextV2PhraseOracleParity(t *testing.T, got, want TextSearchResponse) {
	t.Helper()
	assertTextSearchParity2627(t, got, want)
	for i := range want.Results {
		if !reflect.DeepEqual(got.Results[i].TextMatches, want.Results[i].TextMatches) {
			t.Fatalf("result[%d] TextMatches got=%+v want=%+v", i, got.Results[i].TextMatches, want.Results[i].TextMatches)
		}
		if !slicesEqualStrings(got.Results[i].MatchedTerms, want.Results[i].MatchedTerms) {
			t.Fatalf("result[%d] MatchedTerms got=%v want=%v", i, got.Results[i].MatchedTerms, want.Results[i].MatchedTerms)
		}
		if !slicesEqualStrings(got.Results[i].MatchedFields, want.Results[i].MatchedFields) {
			t.Fatalf("result[%d] MatchedFields got=%v want=%v", i, got.Results[i].MatchedFields, want.Results[i].MatchedFields)
		}
		if !bytes.Equal(got.Results[i].Document, want.Results[i].Document) {
			t.Fatalf("result[%d] Document got=%q want=%q", i, got.Results[i].Document, want.Results[i].Document)
		}
	}
	if got.Explain != nil && want.Explain != nil {
		if len(got.Explain.Results) != len(want.Explain.Results) {
			t.Fatalf("explain results len got=%d want=%d", len(got.Explain.Results), len(want.Explain.Results))
		}
		for i := range want.Explain.Results {
			gr := got.Explain.Results[i]
			wr := want.Explain.Results[i]
			if !bytes.Equal(gr.DocumentID, wr.DocumentID) || gr.Rank != wr.Rank || math.Abs(gr.Score-wr.Score) > 1e-12 {
				t.Fatalf("explain result[%d] got=%+v want=%+v", i, gr, wr)
			}
			if len(gr.Terms) != len(wr.Terms) {
				t.Fatalf("explain result[%d] terms len got=%d want=%d", i, len(gr.Terms), len(wr.Terms))
			}
			for j := range wr.Terms {
				if gr.Terms[j].Term != wr.Terms[j].Term || math.Abs(gr.Terms[j].Score-wr.Terms[j].Score) > 1e-12 {
					t.Fatalf("explain result[%d].Terms[%d] got=%+v want=%+v", i, j, gr.Terms[j], wr.Terms[j])
				}
			}
		}
	}
}

func runTextV2PhraseWithOracle(t *testing.T, col *Collection, opts TextSearchOptions, totalDocs int) (TextSearchResponse, TextSearchResponse) {
	t.Helper()
	got, err := col.SearchText(opts)
	if err != nil {
		t.Fatalf("SearchText(%+v) failed: %v (stats=%+v)", opts, err, got.Stats)
	}
	oracleOpts := opts
	if oracleOpts.CandidateLimit < totalDocs {
		oracleOpts.CandidateLimit = totalDocs
	}
	if oracleOpts.MaxPostingsScanned < totalDocs*8 {
		oracleOpts.MaxPostingsScanned = totalDocs * 8
	}
	oracleOpts.textV2DisableBlockMax = true
	oracle, err := col.SearchText(oracleOpts)
	if err != nil {
		t.Fatalf("oracle SearchText(%+v) failed: %v (stats=%+v)", oracleOpts, err, oracle.Stats)
	}
	assertTextV2PhraseOracleParity(t, got, oracle)
	return got, oracle
}

func TestTextV2PhraseHighDFBlockMaxLateHits(t *testing.T) {
	const (
		nonAdjacentDocs = 2500
		exactMatchDocs  = 40
		totalDocs       = nonAdjacentDocs + exactMatchDocs
	)
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	exactIndices := make(map[int]struct{}, exactMatchDocs)
	for i := 0; i < exactMatchDocs; i++ {
		// Place all 40 exact hits strictly at ordinals 1501..2476 (well after default CandidateLimit=1024).
		exactIndices[1500+i*25] = struct{}{}
	}
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		if _, ok := exactIndices[i]; ok {
			if i%50 == 0 {
				docs[i] = []byte(fmt.Sprintf(`{"title":"elastic electron","body":"elastic electron carrier note %d"}`, i))
			} else {
				docs[i] = []byte(fmt.Sprintf(`{"title":"carrier note","body":"elastic electron note %d"}`, i))
			}
		} else if i%100 == 17 {
			// Slop=1 proximity match (one token between elastic and electron).
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic carrier note","body":"elastic gap electron note %d"}`, i))
		} else if i%100 == 33 {
			// Slop=2 proximity match (two tokens between elastic and electron).
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic carrier note","body":"elastic one two electron note %d"}`, i))
		} else {
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic carrier note","body":"elastic sep1 sep2 sep3 electron note %d"}`, i))
		}
	}

	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
	}, ids, docs)

	for _, tc := range []struct {
		name        string
		slop        int
		mode        TextSearchResultMode
		includeDocs bool
		explain     bool
	}{
		{name: "exact_slop0_detailed", slop: 0, mode: TextSearchResultModeDetailed, explain: true},
		{name: "exact_slop0_score_only", slop: 0, mode: TextSearchResultModeScoreOnly},
		{name: "exact_slop0_compact_with_docs", slop: 0, mode: TextSearchResultModeCompact, includeDocs: true},
		{name: "proximity_slop1_detailed", slop: 1, mode: TextSearchResultModeDetailed, explain: true},
		{name: "proximity_slop2_score_only", slop: 2, mode: TextSearchResultModeScoreOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := TextSearchOptions{
				IndexName:        "lexical",
				Phrase:           &TextSearchPhraseQuery{Query: "elastic electron", Slop: tc.slop},
				TopK:             10,
				ResultMode:       tc.mode,
				IncludeDocuments: tc.includeDocs,
				Explain:          tc.explain,
			}
			got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
			if len(got.Results) != 10 {
				t.Fatalf("results=%d want 10", len(got.Results))
			}
			if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 {
				t.Fatalf("stats=%+v want zero fallbacks and zero fail-closed", got.Stats)
			}
			if tc.mode == TextSearchResultModeScoreOnly {
				if got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.TextStateLookups != 0 {
					t.Fatalf("score_only stats=%+v want zero docs/details/state lookups", got.Stats)
				}
			}
			if tc.includeDocs {
				if got.Stats.DocumentsFetched != uint64(len(got.Results)) {
					t.Fatalf("DocumentsFetched=%d want %d", got.Stats.DocumentsFetched, len(got.Results))
				}
			}
			if tc.explain {
				if got.Explain == nil || got.Explain.Serving.Path != TextSearchExplainPathPhrase || !got.Explain.Serving.BlockMaxEnabled {
					t.Fatalf("explain=%+v want path=%q block_max_enabled=true", got.Explain, TextSearchExplainPathPhrase)
				}
				assertExplainScoreComponents2838(t, got)
			}
		})
	}
}

func TestTextV2PhraseHighDFBlockMaxSpreadHitsAndPruningProof(t *testing.T) {
	const (
		earlyHighScoreHits = 16
		spreadWeakHits     = 32
		fillerDocs         = 2500
		totalDocs          = earlyHighScoreHits + fillerDocs
	)
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		switch {
		case i < earlyHighScoreHits:
			// High-scoring phrase hits in the very first block (heavy title TF, short fields).
			docs[i] = []byte(`{"title":"elastic electron elastic electron","body":"elastic electron"}`)
		case i%75 == 0 && (i/75) <= spreadWeakHits:
			// Lower-scoring phrase hits spread across later blocks (long body, no title match).
			docs[i] = []byte(fmt.Sprintf(`{"title":"filler heading %d","body":"padding one two three four five six seven eight nine ten eleven twelve elastic electron trailing %d"}`, i, i))
		default:
			// Lower-scoring non-adjacent intersecting docs across all later blocks.
			docs[i] = []byte(fmt.Sprintf(`{"title":"filler heading %d","body":"elastic padding one two three four five six seven eight nine ten eleven twelve electron trailing %d"}`, i, i))
		}
	}

	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{
		IndexName:  "lexical",
		Phrase:     &TextSearchPhraseQuery{Query: "elastic electron"},
		TopK:       10,
		ResultMode: TextSearchResultModeScoreOnly,
	}
	got, oracle := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
	if len(got.Results) != 10 {
		t.Fatalf("results=%d want 10", len(got.Results))
	}
	if got.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("TextBlockMaxFallbacks=%d want 0", got.Stats.TextBlockMaxFallbacks)
	}
	if got.Stats.TextPostingBlocksSkipped == 0 {
		t.Fatalf("stats=%+v want TextPostingBlocksSkipped > 0", got.Stats)
	}
	if got.Stats.TextPhraseCandidatesChecked >= uint64(totalDocs) {
		t.Fatalf("TextPhraseCandidatesChecked=%d want < %d", got.Stats.TextPhraseCandidatesChecked, totalDocs)
	}
	if got.Stats.TextPhraseCandidatesChecked >= oracle.Stats.TextPhraseCandidatesChecked {
		t.Fatalf("streaming checked=%d should be < legacy oracle checked=%d", got.Stats.TextPhraseCandidatesChecked, oracle.Stats.TextPhraseCandidatesChecked)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.TextStateLookups != 0 {
		t.Fatalf("score-only stats=%+v want zero docs/details/state lookups", got.Stats)
	}

	// Verify Explain does not add extra posting scans compared to no-explain.
	explainOpts := opts
	explainOpts.Explain = true
	explained, err := col.SearchText(explainOpts)
	if err != nil {
		t.Fatalf("explained SearchText: %v", err)
	}
	assertTextV2PhraseOracleParity(t, explained, oracle)
	if explained.Stats.TextPostingsScanned != got.Stats.TextPostingsScanned {
		t.Fatalf("explain postings scanned=%d no-explain=%d want identical", explained.Stats.TextPostingsScanned, got.Stats.TextPostingsScanned)
	}
	if explained.Explain == nil || explained.Explain.Serving.Path != TextSearchExplainPathPhrase || !explained.Explain.Serving.BlockMaxEnabled {
		t.Fatalf("explain=%+v want phrase path with BlockMaxEnabled=true", explained.Explain)
	}
}

func TestTextV2PhraseBlockMaxRepeatedTerm(t *testing.T) {
	const totalDocs = 1800
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		if i >= 1200 && i%25 == 0 {
			docs[i] = []byte(fmt.Sprintf(`{"title":"alpha alpha","body":"alpha alpha repeated %d"}`, i))
		} else {
			docs[i] = []byte(fmt.Sprintf(`{"title":"alpha note","body":"alpha spacer1 spacer2 alpha doc %d"}`, i))
		}
	}

	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "title", Weight: 4}, {Field: "body"}},
	}, ids, docs)

	for _, mode := range []TextSearchResultMode{TextSearchResultModeScoreOnly, TextSearchResultModeDetailed} {
		opts := TextSearchOptions{
			IndexName:  "lexical",
			Phrase:     &TextSearchPhraseQuery{Query: "alpha alpha"},
			TopK:       10,
			ResultMode: mode,
			Explain:    mode == TextSearchResultModeDetailed,
		}
		got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
		if len(got.Results) != 10 {
			t.Fatalf("mode=%q results=%d want 10", mode, len(got.Results))
		}
		if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 {
			t.Fatalf("mode=%q stats=%+v want zero fallbacks", mode, got.Stats)
		}
	}
}

func TestTextV2PhraseBlockMaxThreeTermAndStopwordGap(t *testing.T) {
	const totalDocs = 1600
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		switch {
		case i >= 1100 && i%40 == 0:
			// Exact 3-term match and stopword-gap match ("elastic of electron").
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic electron spin","body":"elastic of electron spin carrier %d"}`, i))
		case i%2 == 0:
			// Different DF across terms: spin appears less often than electron, electron less often than elastic.
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic note","body":"elastic spacer electron spacer1 spacer2 spin %d"}`, i))
		default:
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic note","body":"elastic spacer electron %d"}`, i))
		}
	}

	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:            "lexical",
		Version:         TextIndexVersionV2,
		StorePositions:  true,
		AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"of"}},
		Fields:          []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
	}, ids, docs)

	for _, tc := range []struct {
		name   string
		phrase TextSearchPhraseQuery
	}{
		{name: "three_term_exact", phrase: TextSearchPhraseQuery{Query: "elastic electron spin", Slop: 0}},
		{name: "three_term_slop1", phrase: TextSearchPhraseQuery{Query: "elastic electron spin", Slop: 1}},
		{name: "stopword_gap_exact", phrase: TextSearchPhraseQuery{Query: "elastic of electron", Slop: 0}},
		{name: "stopword_gap_slop1", phrase: TextSearchPhraseQuery{Query: "elastic of electron", Slop: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := TextSearchOptions{
				IndexName:  "lexical",
				Phrase:     &tc.phrase,
				TopK:       10,
				ResultMode: TextSearchResultModeDetailed,
				Explain:    true,
			}
			got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
			if len(got.Results) == 0 {
				t.Fatalf("%s returned zero results", tc.name)
			}
			if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 {
				t.Fatalf("%s stats=%+v want zero fallbacks", tc.name, got.Stats)
			}
		})
	}
}

func TestTextV2PhraseBlockMaxTieBreakAcrossBlockBoundaries(t *testing.T) {
	const totalDocs = 1600
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		switch {
		case i >= 1200 && i < 1206:
			// Lexicographically smallest IDs placed in a LATE sealed block!
			ids[i] = []byte(fmt.Sprintf("a-winner-%04d", i))
		case i >= 600 && i < 606:
			// Middle-ranked IDs placed in a MIDDLE sealed block!
			ids[i] = []byte(fmt.Sprintf("b-winner-%04d", i))
		default:
			// Larger IDs placed in early and tail sealed blocks.
			ids[i] = []byte(fmt.Sprintf("z-doc-%05d", i))
		}
		// Identical content so every doc has the EXACT same BM25F score, forcing pure docID tie-break!
		docs[i] = []byte(`{"body":"elastic electron constant text"}`)
	}

	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{
		IndexName:  "lexical",
		Phrase:     &TextSearchPhraseQuery{Query: "elastic electron"},
		TopK:       10,
		ResultMode: TextSearchResultModeScoreOnly,
	}
	got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
	if len(got.Results) != 10 {
		t.Fatalf("results=%d want 10", len(got.Results))
	}
	if got.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("TextBlockMaxFallbacks=%d want 0", got.Stats.TextBlockMaxFallbacks)
	}
	// First 6 must be a-winner-1200..1205, next 4 must be b-winner-0600..0603.
	for i := 0; i < 6; i++ {
		want := fmt.Sprintf("a-winner-%04d", 1200+i)
		if string(got.Results[i].DocumentID) != want {
			t.Fatalf("result[%d]=%q want %q", i, got.Results[i].DocumentID, want)
		}
	}
	for i := 0; i < 4; i++ {
		want := fmt.Sprintf("b-winner-%04d", 600+i)
		if string(got.Results[6+i].DocumentID) != want {
			t.Fatalf("result[%d]=%q want %q", 6+i, got.Results[6+i].DocumentID, want)
		}
	}
}

func TestTextV2PhraseBlockMaxScalarPrefilter(t *testing.T) {
	const totalDocs = 1600
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "tenant", Field: "tenant", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	allow := make(hybridScalarAllowSet)
	for i := 0; i < totalDocs; i++ {
		id := fmt.Sprintf("doc-%05d", i)
		ids[i] = []byte(id)
		tenant := "tenant-other"
		// Only place tenant-target in a small slice of middle/late blocks so other blocks are scalar-skipped.
		if i >= 1300 && i < 1340 {
			tenant = "tenant-target"
			allow[id] = struct{}{}
		}
		if i%2 == 0 {
			docs[i] = []byte(fmt.Sprintf(`{"tenant":%q,"body":"elastic electron note %d"}`, tenant, i))
		} else {
			docs[i] = []byte(fmt.Sprintf(`{"tenant":%q,"body":"elastic spacer electron note %d"}`, tenant, i))
		}
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "body"}},
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}

	opts := TextSearchOptions{
		IndexName:                "lexical",
		Phrase:                   &TextSearchPhraseQuery{Query: "elastic electron"},
		TopK:                     10,
		ResultMode:               TextSearchResultModeScoreOnly,
		Explain:                  true,
		textV2AllowedDocumentIDs: allow,
	}
	got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
	if len(got.Results) != 10 {
		t.Fatalf("results=%d want 10", len(got.Results))
	}
	if got.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("TextBlockMaxFallbacks=%d want 0", got.Stats.TextBlockMaxFallbacks)
	}
	if got.Stats.TextScalarPostingBlocksSkipped == 0 {
		t.Fatalf("stats=%+v want TextScalarPostingBlocksSkipped > 0", got.Stats)
	}
	for _, r := range got.Results {
		if _, ok := allow[string(r.DocumentID)]; !ok {
			t.Fatalf("result %q outside scalar allow set", r.DocumentID)
		}
	}
}

func TestTextV2PhraseBlockMaxMidScanFallback(t *testing.T) {
	t.Run("stale_posting_update_mid_scan", func(t *testing.T) {
		const totalDocs = 400
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()

		ids := make([][]byte, totalDocs)
		docs := make([][]byte, totalDocs)
		for i := 0; i < totalDocs; i++ {
			ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
			if i%20 == 0 {
				docs[i] = []byte(fmt.Sprintf(`{"title":"note","body":"elastic electron item %d"}`, i))
			} else {
				docs[i] = []byte(fmt.Sprintf(`{"title":"note","body":"elastic spacer electron item %d"}`, i))
			}
		}
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
		}, ids, docs)

		// Update a document in block 3 (ordinal 300) so blocks 1..2 scan sealed postings,
		// then block 3 encounters a stale posting / overlapping block and falls back mid-scan.
		if _, _, err := col.Update([]byte("doc-00299"), func([]byte) ([]byte, bool, error) {
			return []byte(`{"title":"elastic electron elastic electron","body":"elastic electron updated"}`), true, nil
		}); err != nil {
			t.Fatalf("Update: %v", err)
		}

		opts := TextSearchOptions{
			IndexName:          "lexical",
			Phrase:             &TextSearchPhraseQuery{Query: "elastic electron"},
			TopK:               5,
			CandidateLimit:     totalDocs + 16,
			MaxPostingsScanned: (totalDocs + 16) * 8,
			ResultMode:         TextSearchResultModeDetailed,
			Explain:            true,
		}
		got, oracle := runTextV2PhraseWithOracle(t, col, opts, totalDocs+16)
		if len(got.Results) == 0 || string(got.Results[0].DocumentID) != "doc-00299" {
			t.Fatalf("results=%+v want updated doc-00299 ranked first", got.Results)
		}
		if got.Stats.TextBlockMaxFallbacks != 1 {
			t.Fatalf("TextBlockMaxFallbacks=%d want 1", got.Stats.TextBlockMaxFallbacks)
		}
		if got.Explain == nil || got.Explain.Serving.BlockMaxFallbacks != 1 || got.Explain.Counters.BlockMaxFallbacks != 1 || got.Explain.Serving.BlockMaxEnabled {
			t.Fatalf("explain=%+v want fallback recorded and BlockMaxEnabled=false", got.Explain)
		}
		if !slices.Contains(got.Explain.FallbackReasons, "blockmax_phrase_unsealed_stale_or_overlapping_block") {
			t.Fatalf("FallbackReasons=%v want blockmax_phrase_unsealed_stale_or_overlapping_block", got.Explain.FallbackReasons)
		}
		if got.Stats.TextPostingsScanned <= oracle.Stats.TextPostingsScanned {
			t.Fatalf("mid-scan fallback TextPostingsScanned=%d should exceed legacy-only=%d (monotonic across pre-fallback scan)", got.Stats.TextPostingsScanned, oracle.Stats.TextPostingsScanned)
		}
		if got.Stats.TextPhraseCandidatesChecked != oracle.Stats.TextPhraseCandidatesChecked {
			t.Fatalf("TextPhraseCandidatesChecked=%d want exact oracle=%d (no double counting)", got.Stats.TextPhraseCandidatesChecked, oracle.Stats.TextPhraseCandidatesChecked)
		}
		if got.Stats.TextPhraseCandidatesMatched != oracle.Stats.TextPhraseCandidatesMatched {
			t.Fatalf("TextPhraseCandidatesMatched=%d want exact oracle=%d", got.Stats.TextPhraseCandidatesMatched, oracle.Stats.TextPhraseCandidatesMatched)
		}

		// Monotonic budget enforcement across fallback: cap at exact legacy postings count must fail closed.
		assertTextV2FallbackBudgetIsMonotonic4766(t, col, opts, oracle)
	})

	t.Run("overlapping_block_update_mid_scan", func(t *testing.T) {
		const totalDocs = 400
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()

		ids := make([][]byte, totalDocs)
		docs := make([][]byte, totalDocs)
		for i := 0; i < totalDocs; i++ {
			ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
			if i == 299 {
				// Initially lacks the phrase terms so sealed block 3 has no stale posting for ordinal 300.
				docs[i] = []byte(`{"title":"unrelated","body":"placeholder document"}`)
			} else {
				docs[i] = []byte(fmt.Sprintf(`{"title":"note","body":"elastic spacer electron item %d"}`, i))
			}
		}
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
		}, ids, docs)

		// Updating doc-00299 adds a micro block at BlockStart=300 overlapping sealed block [257..385],
		// triggering mid-scan fallback via overlapping block bounds without any stale posting.
		if _, _, err := col.Update([]byte("doc-00299"), func([]byte) ([]byte, bool, error) {
			return []byte(`{"title":"elastic electron","body":"elastic electron overlapping"}`), true, nil
		}); err != nil {
			t.Fatalf("Update overlapping doc: %v", err)
		}

		opts := TextSearchOptions{
			IndexName:          "lexical",
			Phrase:             &TextSearchPhraseQuery{Query: "elastic electron"},
			TopK:               5,
			CandidateLimit:     totalDocs + 16,
			MaxPostingsScanned: (totalDocs + 16) * 8,
			ResultMode:         TextSearchResultModeScoreOnly,
			Explain:            true,
		}
		got, oracle := runTextV2PhraseWithOracle(t, col, opts, totalDocs+16)
		if len(got.Results) != 1 || string(got.Results[0].DocumentID) != "doc-00299" {
			t.Fatalf("results=%+v want doc-00299", got.Results)
		}
		if got.Stats.TextBlockMaxFallbacks != 1 {
			t.Fatalf("TextBlockMaxFallbacks=%d want 1", got.Stats.TextBlockMaxFallbacks)
		}
		if got.Explain == nil || got.Explain.Serving.BlockMaxFallbacks != 1 || got.Explain.Serving.BlockMaxEnabled {
			t.Fatalf("explain=%+v want fallback recorded", got.Explain)
		}
		if !slices.Contains(got.Explain.FallbackReasons, "blockmax_phrase_unsealed_stale_or_overlapping_block") {
			t.Fatalf("FallbackReasons=%v want blockmax_phrase_unsealed_stale_or_overlapping_block", got.Explain.FallbackReasons)
		}
		if got.Stats.TextPhraseCandidatesChecked != oracle.Stats.TextPhraseCandidatesChecked {
			t.Fatalf("TextPhraseCandidatesChecked=%d want %d (no double count)", got.Stats.TextPhraseCandidatesChecked, oracle.Stats.TextPhraseCandidatesChecked)
		}
		assertTextV2FallbackBudgetIsMonotonic4766(t, col, opts, oracle)
	})

	t.Run("non_overlapping_appended_micro_block_streams_without_fallback", func(t *testing.T) {
		const totalDocs = 300
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()

		ids := make([][]byte, totalDocs)
		docs := make([][]byte, totalDocs)
		for i := 0; i < totalDocs; i++ {
			ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
			docs[i] = []byte(fmt.Sprintf(`{"title":"note","body":"elastic spacer electron item %d"}`, i))
		}
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
		}, ids, docs)

		// Appending a brand new ordinal strictly after sealed blocks creates a non-overlapping micro block,
		// which textV2PostingBlockKindSupportsBlockMax supports natively with zero fallback.
		if _, err := col.Insert([]byte("doc-00300"), []byte(`{"title":"elastic electron","body":"elastic electron appended"}`)); err != nil {
			t.Fatalf("Insert appended doc: %v", err)
		}

		opts := TextSearchOptions{
			IndexName:  "lexical",
			Phrase:     &TextSearchPhraseQuery{Query: "elastic electron"},
			TopK:       5,
			ResultMode: TextSearchResultModeScoreOnly,
			Explain:    true,
		}
		got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs+16)
		if len(got.Results) != 1 || string(got.Results[0].DocumentID) != "doc-00300" {
			t.Fatalf("results=%+v want doc-00300", got.Results)
		}
		if got.Stats.TextBlockMaxFallbacks != 0 {
			t.Fatalf("TextBlockMaxFallbacks=%d want 0 for non-overlapping micro block", got.Stats.TextBlockMaxFallbacks)
		}
	})
}

func TestTextV2PhraseBlockMaxFailClosedParity(t *testing.T) {
	t.Run("max_postings_scanned_exhaustion", func(t *testing.T) {
		const totalDocs = 400
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()

		ids := make([][]byte, totalDocs)
		docs := make([][]byte, totalDocs)
		for i := 0; i < totalDocs; i++ {
			ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
			docs[i] = []byte(fmt.Sprintf(`{"body":"elastic spacer electron doc %d"}`, i))
		}
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}, ids, docs)

		const limit = 150
		got, err := col.SearchText(TextSearchOptions{
			IndexName:          "lexical",
			Phrase:             &TextSearchPhraseQuery{Query: "elastic electron"},
			TopK:               10,
			MaxPostingsScanned: limit,
			ResultMode:         TextSearchResultModeScoreOnly,
			Explain:            true,
		})
		if !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("err=%v want ErrTextIndexUnavailable", err)
		}
		if errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("err=%v must not be ErrTextIndexStorageCorrupt", err)
		}
		if got.Stats.FailClosed != 1 || !got.Stats.Truncated || got.Stats.FailClosedReason != textSearchFailClosedPostingsLimit {
			t.Fatalf("stats=%+v want FailClosed=1 Truncated=true reason=%q", got.Stats, textSearchFailClosedPostingsLimit)
		}
		if got.Stats.TextPostingsScanned != limit || got.Stats.PostingsScanned != limit {
			t.Fatalf("postings scanned=%d/%d want %d", got.Stats.TextPostingsScanned, got.Stats.PostingsScanned, limit)
		}
		if got.Explain == nil || got.Explain.FailClosedReason != textSearchFailClosedPostingsLimit {
			t.Fatalf("explain=%+v want fail_closed_reason=%q", got.Explain, textSearchFailClosedPostingsLimit)
		}
	})

	t.Run("position_match_budget_exhaustion", func(t *testing.T) {
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()

		// Compact combinatorial fixture: "b" at position 0 followed by 32 occurrences of "a".
		// Searching "a a a a b" with Slop=16 explores C(20,4)=4845 > 4096 recursive states
		// on the first "a" start alone, exhausting textV2PhrasePositionMatchBudget on a tiny doc.
		var body strings.Builder
		body.WriteString("b")
		for i := 0; i < 32; i++ {
			body.WriteString(" a")
		}
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "body"}},
		}, [][]byte{[]byte("d1")}, [][]byte{[]byte(fmt.Sprintf(`{"body":%q}`, body.String()))})

		got, err := col.SearchText(TextSearchOptions{
			IndexName:  "lexical",
			Phrase:     &TextSearchPhraseQuery{Query: "a a a a b", Slop: 16},
			TopK:       5,
			ResultMode: TextSearchResultModeScoreOnly,
			Explain:    true,
		})
		if !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("err=%v want ErrTextIndexUnavailable", err)
		}
		if errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("err=%v must not be ErrTextIndexStorageCorrupt", err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedPhrasePosition {
			t.Fatalf("stats=%+v want FailClosed=1 reason=%q", got.Stats, textSearchFailClosedPhrasePosition)
		}
		if got.Explain == nil || got.Explain.FailClosedReason != textSearchFailClosedPhrasePosition {
			t.Fatalf("explain=%+v want fail_closed_reason=%q", got.Explain, textSearchFailClosedPhrasePosition)
		}
	})
}

func TestTextV2PhraseBlockMaxNativeCOWHighDF(t *testing.T) {
	const (
		nonAdjacentDocs = 2500
		exactMatchDocs  = 40
		totalDocs       = nonAdjacentDocs + exactMatchDocs
	)
	dbOpts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	dbOpts.MemtableMode = "cow_btree"
	limits := treedb.StandaloneCOWMemtableLimits()
	limits.MaxGenerationBytes = 256 << 20
	dbOpts.COWMemtableLimits = limits
	dbOpts.BackgroundCheckpointInterval = -1
	dbOpts.BackgroundCheckpointIdleDuration = -1
	owner, err := treedb.Open(dbOpts)
	if err != nil {
		t.Fatalf("open native COW db: %v", err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Errorf("close native COW db: %v", err)
		}
	})
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatalf("NativeSemanticBackend: %v", err)
	}
	mgr := NewCollectionManager(backend)
	t.Cleanup(func() {
		if err := mgr.Close(); err != nil {
			t.Errorf("close CollectionManager: %v", err)
		}
	})
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		TextIndexes: []TextIndexDefinition{{
			Name:           "lexical",
			Version:        TextIndexVersionV2,
			StorePositions: true,
			Fields:         []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
		}},
	}); err != nil {
		t.Fatalf("CreateCollection native COW: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection native COW: %v", err)
	}

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	exactIndices := make(map[int]struct{}, exactMatchDocs)
	for i := 0; i < exactMatchDocs; i++ {
		exactIndices[1500+i*25] = struct{}{}
	}
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		if _, ok := exactIndices[i]; ok {
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic electron","body":"elastic electron note %d"}`, i))
		} else {
			docs[i] = []byte(fmt.Sprintf(`{"title":"elastic carrier","body":"elastic sep1 sep2 electron note %d"}`, i))
		}
	}
	for start := 0; start < totalDocs; start += 128 {
		end := start + 128
		if end > totalDocs {
			end = totalDocs
		}
		if _, err := col.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			t.Fatalf("InsertBatch native COW [%d:%d]: %v", start, end, err)
		}
		if (start/128)%4 == 3 {
			if err := owner.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint native COW at %d: %v", end, err)
			}
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint native COW final: %v", err)
	}

	for _, mode := range []TextSearchResultMode{TextSearchResultModeScoreOnly, TextSearchResultModeDetailed} {
		opts := TextSearchOptions{
			IndexName:  "lexical",
			Phrase:     &TextSearchPhraseQuery{Query: "elastic electron"},
			TopK:       10,
			ResultMode: mode,
		}
		got, _ := runTextV2PhraseWithOracle(t, col, opts, totalDocs)
		if len(got.Results) != 10 {
			t.Fatalf("native COW mode=%q results=%d want 10", mode, len(got.Results))
		}
		if got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 {
			t.Fatalf("native COW mode=%q stats=%+v want zero fallbacks", mode, got.Stats)
		}
		if mode == TextSearchResultModeScoreOnly && (got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0) {
			t.Fatalf("native COW score_only stats=%+v want zero docs/details", got.Stats)
		}
	}
}

func TestTextV2PhraseBlockMaxCanonicalScoreOrderParity(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	// Exact Codex reproducer: "alpha" and "beta" have DF=3 while "gamma" has DF=2,
	// so DF-ordered scanTerms puts "gamma" first whereas canonical alphabetical
	// uniqueTerms order is ["alpha", "beta", "gamma"]. IEEE-754 float64 addition
	// is non-associative across 3 terms, so summing in scanTerms order can perturb
	// equal-score ties between "a" and "b".
	ids := [][]byte{[]byte("b"), []byte("a"), []byte("c")}
	docs := [][]byte{
		[]byte(`{"body":"alpha alpha beta gamma"}`),
		[]byte(`{"body":"alpha beta beta gamma"}`),
		[]byte(`{"body":"alpha beta note note note note note note note note note note note"}`),
	}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "body"}},
	}, ids, docs)

	opts := TextSearchOptions{
		IndexName:  "lexical",
		Phrase:     &TextSearchPhraseQuery{Query: "alpha beta gamma", Slop: 1},
		TopK:       10,
		ResultMode: TextSearchResultModeDetailed,
		Explain:    true,
	}
	got, oracle := runTextV2PhraseWithOracle(t, col, opts, len(ids))
	if len(got.Results) != 2 {
		t.Fatalf("results=%+v want 2", got.Results)
	}
	if string(got.Results[0].DocumentID) != "a" || string(got.Results[1].DocumentID) != "b" {
		t.Fatalf("order=[%q %q] want [a b] (scores=[%.17g %.17g], oracle=[%q:%.17g %q:%.17g])",
			got.Results[0].DocumentID, got.Results[1].DocumentID,
			got.Results[0].Score, got.Results[1].Score,
			oracle.Results[0].DocumentID, oracle.Results[0].Score,
			oracle.Results[1].DocumentID, oracle.Results[1].Score)
	}
	for i := range oracle.Results {
		if got.Results[i].Score != oracle.Results[i].Score {
			t.Fatalf("result[%d] score got=%.17g want bit-exact %.17g", i, got.Results[i].Score, oracle.Results[i].Score)
		}
	}
	if got.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("TextBlockMaxFallbacks=%d want 0", got.Stats.TextBlockMaxFallbacks)
	}
}

func TestTextV2PhraseBlockMaxBoundedSidecarCache(t *testing.T) {
	// 33 full norm/docMap blocks (4,224 docs > 2,000 ordinals across 32+ blocks)
	// where every doc contains both terms non-adjacently (zero phrase matches).
	const (
		blocks    = 33
		totalDocs = blocks * int(textV2DefaultNormBlockSize)
	)
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		docs[i] = []byte(fmt.Sprintf(`{"body":"elastic separator token electron item %d"}`, i))
	}
	def := TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "body"}},
	}
	col := createTextSearchCollection2627(t, d, "docs", def, ids, docs)

	// 1. Public end-to-end streaming phrase search over all 4,224 non-matching ordinals.
	got, err := col.SearchText(TextSearchOptions{
		IndexName:          "lexical",
		Phrase:             &TextSearchPhraseQuery{Query: "elastic electron"},
		TopK:               10,
		MaxPostingsScanned: totalDocs * 4,
		ResultMode:         TextSearchResultModeScoreOnly,
	})
	if err != nil {
		t.Fatalf("SearchText zero-match high-DF failed: %v (stats=%+v)", err, got.Stats)
	}
	if len(got.Results) != 0 || got.Stats.TextBlockMaxFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("unexpected response results=%d stats=%+v", len(got.Results), got.Stats)
	}
	if got.Stats.TextNormLookups < blocks {
		t.Fatalf("TextNormLookups=%d want >= %d", got.Stats.TextNormLookups, blocks)
	}

	// 2. Direct snapshot verification that cache.normBlocks and cache.docMapBlocks stay <= 2
	// across every overlap step and every docMap/norm lookup over all 33 blocks.
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		uniqueTerms := []string{"elastic", "electron"}
		ctx, err := newTextV2SearchContext(snap, catalog, def, uniqueTerms)
		if err != nil {
			t.Fatalf("newTextV2SearchContext: %v", err)
		}
		fieldCount := len(ctx.fieldNames)
		scanTerms := orderTextV2SearchScanTerms(uniqueTerms, TextSearchOperatorAND, ctx.termStats)
		phraseTermIndexes := textV2PhraseTermIndexes([]string{"elastic", "electron"}, scanTerms)
		states := make([]*textV2ANDBlockMaxTermState, 0, len(scanTerms))
		defer func() {
			for _, state := range states {
				_ = state.close()
			}
		}()
		for _, term := range scanTerms {
			state, err := newTextV2ANDBlockMaxTermState(snap, catalog, ctx, term, fieldCount)
			if err != nil {
				t.Fatalf("newTextV2ANDBlockMaxTermState(%q): %v", term, err)
			}
			states = append(states, state)
		}
		canonicalStates := make([]*textV2ANDBlockMaxTermState, len(uniqueTerms))
		for i, term := range uniqueTerms {
			for _, state := range states {
				if state.term == term {
					canonicalStates[i] = state
					break
				}
			}
		}

		cache := textV2SearchBlockCache{maxBlocks: 2}
		top := textV2SearchTopK{limit: 10, candidates: make([]textV2SearchTopCandidate, 0, 10)}
		positionScratch := make([]textV2PositionValue, len(scanTerms))
		var posKeyScratch, posValScratch []byte
		var hasLastOrdinal bool
		var lastOrdinal uint64
		var stats TextSearchStats
		overlaps := 0
		peakNormBlocks := 0
		peakDocMapBlocks := 0

		for {
			if err := normalizeTextV2ANDBlockMaxStates(ctx, states, fieldCount); err != nil {
				t.Fatalf("normalize states: %v", err)
			}
			if textV2ANDBlockMaxAnyExhausted(states) {
				break
			}
			first, last := textV2ANDBlockMaxCurrentOverlap(states)
			if first > last {
				progressed, err := skipTextV2ANDBlockMaxNonOverlappingBlocks(ctx, states, fieldCount, first, &stats)
				if err != nil || !progressed {
					t.Fatalf("skip non-overlapping progressed=%v err=%v", progressed, err)
				}
				continue
			}
			cache.evictBefore(first)
			overlaps++
			truncated, stale, err := visitTextV2PhraseBlockMaxOverlap(
				snap, catalog, ctx, def, states, canonicalStates, &cache, nil,
				phraseTermIndexes, []int{0}, positionScratch, &posKeyScratch, &posValScratch,
				0, totalDocs*4, false, first, last, &top,
				&hasLastOrdinal, &lastOrdinal, &stats,
			)
			if err != nil || truncated || stale {
				t.Fatalf("visit overlap[%d] [%d,%d]: truncated=%v stale=%v err=%v", overlaps, first, last, truncated, stale, err)
			}
			// Also exercise docMap lookups in each visited block to prove docMapBlocks stays bounded too.
			if _, ok, err := cache.identityEntry(snap, catalog, ctx, first); err != nil || !ok {
				t.Fatalf("identityEntry(%d) ok=%v err=%v", first, ok, err)
			}
			if len(cache.normBlocks) > peakNormBlocks {
				peakNormBlocks = len(cache.normBlocks)
			}
			if len(cache.docMapBlocks) > peakDocMapBlocks {
				peakDocMapBlocks = len(cache.docMapBlocks)
			}
			if len(cache.normBlocks) > 2 || len(cache.docMapBlocks) > 2 {
				t.Fatalf("overlap[%d] [%d,%d] unbounded cache: normBlocks=%d docMapBlocks=%d want <= 2",
					overlaps, first, last, len(cache.normBlocks), len(cache.docMapBlocks))
			}
		}
		if overlaps < blocks || stats.TextNormLookups < blocks {
			t.Fatalf("overlaps=%d normLookups=%d want >= %d", overlaps, stats.TextNormLookups, blocks)
		}
		if stats.TextPhraseCandidatesChecked != uint64(totalDocs) {
			t.Fatalf("TextPhraseCandidatesChecked=%d want %d", stats.TextPhraseCandidatesChecked, totalDocs)
		}
		if peakNormBlocks == 0 || peakDocMapBlocks == 0 || peakNormBlocks > 2 || peakDocMapBlocks > 2 {
			t.Fatalf("peakNormBlocks=%d peakDocMapBlocks=%d want in [1,2]", peakNormBlocks, peakDocMapBlocks)
		}
		// Advancing frontier past all ordinals evicts all remaining cached blocks.
		cache.evictBefore(uint64(totalDocs) + uint64(textV2DefaultNormBlockSize) + 1)
		if len(cache.normBlocks) != 0 || len(cache.docMapBlocks) != 0 {
			t.Fatalf("post-evict normBlocks=%d docMapBlocks=%d want 0", len(cache.normBlocks), len(cache.docMapBlocks))
		}
	})
}

func BenchmarkTextV2PhraseHighDFBlockMax(b *testing.B) {
	const (
		earlyHighScoreHits = 24
		latePhraseHits     = 40
		fillerDocs         = 2560
		totalDocs          = earlyHighScoreHits + fillerDocs
	)
	d := openTextV2ContractDB2623(b)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}

	ids := make([][]byte, totalDocs)
	docs := make([][]byte, totalDocs)
	for i := 0; i < totalDocs; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%05d", i))
		switch {
		case i < earlyHighScoreHits:
			docs[i] = []byte(`{"title":"elastic electron elastic electron","body":"elastic electron"}`)
		case i >= 1500 && i < 1500+latePhraseHits*20 && (i-1500)%20 == 0:
			docs[i] = []byte(fmt.Sprintf(`{"title":"carrier heading %d","body":"padding one two three four five six seven eight nine ten elastic electron trailing %d"}`, i, i))
		default:
			docs[i] = []byte(fmt.Sprintf(`{"title":"carrier heading %d","body":"elastic padding one two three four five six seven eight nine ten electron trailing %d"}`, i, i))
		}
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}},
	}); err != nil {
		b.Fatalf("CreateTextIndex: %v", err)
	}

	streamOpts := TextSearchOptions{
		IndexName:  "lexical",
		Phrase:     &TextSearchPhraseQuery{Query: "elastic electron"},
		TopK:       10,
		ResultMode: TextSearchResultModeScoreOnly,
	}
	oracleOpts := streamOpts
	oracleOpts.CandidateLimit = totalDocs
	oracleOpts.MaxPostingsScanned = totalDocs * 4
	oracleOpts.textV2DisableBlockMax = true

	warmStream, err := col.SearchText(streamOpts)
	if err != nil {
		b.Fatalf("warm streaming phrase search: %v", err)
	}
	warmOracle, err := col.SearchText(oracleOpts)
	if err != nil {
		b.Fatalf("warm legacy oracle phrase search: %v", err)
	}
	if len(warmStream.Results) != len(warmOracle.Results) || len(warmStream.Results) != 10 {
		b.Fatalf("warm results stream=%d oracle=%d want 10", len(warmStream.Results), len(warmOracle.Results))
	}
	for i := range warmOracle.Results {
		if !bytes.Equal(warmStream.Results[i].DocumentID, warmOracle.Results[i].DocumentID) || math.Abs(warmStream.Results[i].Score-warmOracle.Results[i].Score) > 1e-12 {
			b.Fatalf("warm parity mismatch[%d] stream=%+v oracle=%+v", i, warmStream.Results[i], warmOracle.Results[i])
		}
	}
	if warmStream.Stats.TextBlockMaxFallbacks != 0 || warmStream.Stats.TextPostingBlocksSkipped == 0 {
		b.Fatalf("warm stream stats=%+v want fallbacks=0 and blocks_skipped>0", warmStream.Stats)
	}

	for _, tc := range []struct {
		name string
		opts TextSearchOptions
	}{
		{name: "streaming_blockmax", opts: streamOpts},
		{name: "legacy_oracle", opts: oracleOpts},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.SearchText(tc.opts)
				if err != nil {
					b.Fatalf("%s SearchText: %v", tc.name, err)
				}
				if len(got.Results) != 10 || got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
					b.Fatalf("%s unexpected response stats=%+v", tc.name, got.Stats)
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(totalDocs), "docs_fixture")
			textV2ContractReportTextStats2623(b, sink)
		})
	}
}
