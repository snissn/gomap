package collections

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestTextV2QueryExplainShapes2838(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids := [][]byte{[]byte("d1"), []byte("d2"), []byte("d3"), []byte("d4"), []byte("d5"), []byte("d6")}
	docs := [][]byte{
		[]byte(`{"title":"refund policy","body":"refund refund policy"}`),
		[]byte(`{"title":"refund","body":"shipping refund"}`),
		[]byte(`{"title":"policy","body":"policy shipping"}`),
		[]byte(`{"title":"unicorn","body":"rare unicorn refund"}`),
		[]byte(`{"title":"shipping","body":"tracking policy"}`),
		[]byte(`{"title":"other","body":"support"}`),
	}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
		Name:           "lexical",
		Version:        TextIndexVersionV2,
		StorePositions: true,
		Fields:         []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
	}, ids, docs)

	tests := []struct {
		name       string
		opts       TextSearchOptions
		wantPath   TextSearchExplainServingPath
		wantTerms  []string
		wantScalar bool
		wantPhrase bool
		golden     string
	}{
		{name: "common", opts: TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 3, CandidateLimit: 16, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, wantPath: TextSearchExplainPathBlockMaxSingle, wantTerms: []string{"refund"}, golden: `{"path":"blockmax_single_term","terms":["refund"],"scalar":false,"phrase":false}`},
		{name: "rare", opts: TextSearchOptions{IndexName: "lexical", Query: "unicorn", TopK: 3, CandidateLimit: 16, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, wantPath: TextSearchExplainPathBlockMaxSingle, wantTerms: []string{"unicorn"}, golden: `{"path":"blockmax_single_term","terms":["unicorn"],"scalar":false,"phrase":false}`},
		{name: "and", opts: TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorAND, TopK: 3, CandidateLimit: 16, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, wantPath: TextSearchExplainPathBlockMaxAND, wantTerms: []string{"policy", "refund"}, golden: `{"path":"blockmax_and","terms":["policy","refund"],"scalar":false,"phrase":false}`},
		{name: "or", opts: TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorOR, TopK: 3, CandidateLimit: 16, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, wantPath: TextSearchExplainPathBlockMaxORWAND, wantTerms: []string{"policy", "refund"}, golden: `{"path":"blockmax_or_wand","terms":["policy","refund"],"scalar":false,"phrase":false}`},
		{name: "phrase", opts: TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 3, CandidateLimit: 16, MaxPostingsScanned: 64, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, wantPath: TextSearchExplainPathPhrase, wantTerms: []string{"policy", "refund"}, wantPhrase: true, golden: `{"path":"phrase_validation","terms":["policy","refund"],"scalar":false,"phrase":true}`},
		{name: "filtered", opts: TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 3, CandidateLimit: 16, ResultMode: TextSearchResultModeScoreOnly, Explain: true, textV2AllowedDocumentIDs: hybridScalarAllowSet{"d2": {}}}, wantPath: TextSearchExplainPathBlockMaxSingle, wantTerms: []string{"refund"}, wantScalar: true, golden: `{"path":"blockmax_single_term","terms":["refund"],"scalar":true,"phrase":false}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := col.searchText(tc.opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("searchText explain %s: %v", tc.name, err)
			}
			if got.Explain == nil || !got.Explain.Enabled {
				t.Fatalf("Explain=nil for %s", tc.name)
			}
			if got.Explain.Serving.Path != tc.wantPath {
				t.Fatalf("path=%q want %q explain=%s", got.Explain.Serving.Path, tc.wantPath, mustMarshalExplainSummary2838(t, got.Explain))
			}
			if golden := explainShapeGoldenJSON2838(t, got.Explain); golden != tc.golden {
				t.Fatalf("shape golden=%s want %s", golden, tc.golden)
			}
			if terms := explainTermNames2838(got.Explain); !slicesEqualStrings(terms, tc.wantTerms) {
				t.Fatalf("terms=%v want %v explain=%s", terms, tc.wantTerms, mustMarshalExplainSummary2838(t, got.Explain))
			}
			if got.Explain.Snapshot.RootGeneration == 0 || got.Explain.Snapshot.LiveDocuments != uint64(len(ids)) || got.Explain.Snapshot.CorpusDocuments != uint64(len(ids)) || len(got.Explain.Snapshot.ActiveRootNames) == 0 {
				t.Fatalf("snapshot=%+v want root/status identity", got.Explain.Snapshot)
			}
			if got.Explain.Counters.CandidatesReturned != uint64(len(got.Results)) || got.Explain.Counters.PostingsScanned != got.Stats.TextPostingsScanned {
				t.Fatalf("explain counters=%+v stats=%+v results=%d", got.Explain.Counters, got.Stats, len(got.Results))
			}
			if tc.wantScalar {
				if !got.Explain.Serving.ScalarPruning.Enabled || got.Explain.Serving.ScalarPruning.AllowSetSize != 1 || got.Stats.TextScalarPrefilterIDs != 1 {
					t.Fatalf("scalar explain=%+v stats=%+v want one-id prefilter", got.Explain.Serving.ScalarPruning, got.Stats)
				}
				if len(got.Results) != 1 || string(got.Results[0].DocumentID) != "d2" {
					t.Fatalf("filtered results=%+v want only d2", got.Results)
				}
			}
			if tc.wantPhrase {
				if got.Explain.Phrase == nil || !slicesEqualStrings(got.Explain.Phrase.Terms, []string{"refund", "policy"}) || got.Explain.Serving.PhraseValidation.CandidatesChecked == 0 || got.Explain.Serving.PhraseValidation.CandidatesMatched == 0 {
					t.Fatalf("phrase explain=%+v serving=%+v", got.Explain.Phrase, got.Explain.Serving.PhraseValidation)
				}
			}
			if tc.wantPath == TextSearchExplainPathBlockMaxORWAND && got.Explain.Serving.WANDPivots == 0 {
				t.Fatalf("OR/WAND explain=%+v want pivot counter", got.Explain.Serving)
			}
			if len(got.Results) > 0 {
				assertExplainScoreComponents2838(t, got)
			}
		})
	}
}

func TestTextV2QueryExplainNoExplainPathAndCounters2838(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMultiTermDocs2688(256, 16)
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)

	opts := TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorOR, TopK: 8, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4, ResultMode: TextSearchResultModeScoreOnly}
	before, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("no-explain before: %v", err)
	}
	after, err := col.searchText(opts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("no-explain after: %v", err)
	}
	if before.Explain != nil || after.Explain != nil {
		t.Fatalf("Explain before=%+v after=%+v want nil by default", before.Explain, after.Explain)
	}
	assertTextSearchParity2627(t, after, before)
	if textExplainStableCounters2838(before.Stats) != textExplainStableCounters2838(after.Stats) {
		t.Fatalf("no-explain counters changed before=%+v after=%+v", before.Stats, after.Stats)
	}
	withExplain := opts
	withExplain.Explain = true
	explained, err := col.searchText(withExplain, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("explain enabled: %v", err)
	}
	assertTextSearchParity2627(t, explained, before)
	if explained.Explain == nil || len(explained.Explain.Results) != len(explained.Results) {
		t.Fatalf("explained=%+v want topK score components", explained.Explain)
	}
	if explained.Stats.TextPostingsScanned != before.Stats.TextPostingsScanned {
		t.Fatalf("explain postings scanned=%d no-explain=%d; explain should reuse WAND postings instead of rescanning topK terms", explained.Stats.TextPostingsScanned, before.Stats.TextPostingsScanned)
	}
	if explained.Stats.TextPostingsScanned > uint64(withExplain.MaxPostingsScanned) {
		t.Fatalf("explain postings scanned=%d exceeds MaxPostingsScanned=%d", explained.Stats.TextPostingsScanned, withExplain.MaxPostingsScanned)
	}

	andOpts := opts
	andOpts.Operator = TextSearchOperatorAND
	andBefore, err := col.searchText(andOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("AND no-explain: %v", err)
	}
	andExplainOpts := andOpts
	andExplainOpts.Explain = true
	andExplained, err := col.searchText(andExplainOpts, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("AND explain: %v", err)
	}
	assertTextSearchParity2627(t, andExplained, andBefore)
	if andExplained.Stats.TextPostingsScanned != andBefore.Stats.TextPostingsScanned {
		t.Fatalf("AND explain postings scanned=%d no-explain=%d; explain should reuse block-max postings instead of rescanning topK terms", andExplained.Stats.TextPostingsScanned, andBefore.Stats.TextPostingsScanned)
	}
}

func TestTextV2QueryExplainFailClosedReasons2838(t *testing.T) {
	t.Run("unsupported_phrase_without_positions", func(t *testing.T) {
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}, [][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)})

		got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 1, Explain: true})
		if !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("SearchText err=%v want ErrTextIndexUnavailable", err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedUnsupported || got.Explain == nil || got.Explain.FailClosedReason != textSearchFailClosedUnsupported || got.Explain.Serving.Path != TextSearchExplainPathFailClosed {
			t.Fatalf("response=%+v explain=%+v want unsupported fail-closed", got.Stats, got.Explain)
		}
	})

	t.Run("candidate_limit", func(t *testing.T) {
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()
		ids, docs := textV2BlockMaxFixtureDocs2628(64, 8)
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}}, ids, docs)

		got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 8, CandidateLimit: 1, MaxPostingsScanned: 256, ResultMode: TextSearchResultModeScoreOnly, Explain: true}, textSearchResultScoreOnly)
		if !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("searchText err=%v want ErrTextIndexUnavailable", err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedCandidateLimit || got.Explain == nil || got.Explain.FailClosedReason != textSearchFailClosedCandidateLimit {
			t.Fatalf("response=%+v explain=%+v want candidate-limit fail-closed", got.Stats, got.Explain)
		}
	})
}

func TestTextV2QueryExplainSnapshotReopenStatus2838(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	ids := [][]byte{[]byte("d1"), []byte("d2")}
	docs := [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"shipping policy"}`)}
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}, ids, docs)
	before, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "policy", TopK: 2, ResultMode: TextSearchResultModeScoreOnly, Explain: true})
	if err != nil {
		t.Fatalf("before explain: %v", err)
	}
	if before.Explain == nil || before.Explain.Snapshot.RootGeneration == 0 || before.Explain.Snapshot.LiveDocuments != 2 {
		t.Fatalf("before explain=%+v want root status", before.Explain)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openTextV2TestDB(t, dir, false)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	after, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "policy", TopK: 2, ResultMode: TextSearchResultModeScoreOnly, Explain: true})
	if err != nil {
		t.Fatalf("after explain: %v", err)
	}
	assertTextSearchParity2627(t, after, before)
	if after.Explain == nil || after.Explain.Snapshot.RootGeneration != before.Explain.Snapshot.RootGeneration || after.Explain.Snapshot.LiveDocuments != before.Explain.Snapshot.LiveDocuments || len(after.Explain.Snapshot.ActiveRootNames) != len(before.Explain.Snapshot.ActiveRootNames) {
		t.Fatalf("before snapshot=%+v after=%+v", before.Explain.Snapshot, after.Explain.Snapshot)
	}
	status, err := reopenedCol.TextIndexStatus("lexical")
	if err != nil {
		t.Fatalf("TextIndexStatus reopened: %v", err)
	}
	if !status.Readable || status.FailClosed || len(status.ActiveRootNames) != len(after.Explain.Snapshot.ActiveRootNames) {
		t.Fatalf("status=%+v explain snapshot=%+v want reopen-readable status", status, after.Explain.Snapshot)
	}
}

func BenchmarkTextV2QueryExplain2838(b *testing.B) {
	d := openTextV2TestDB(b, b.TempDir(), false)
	defer func() { _ = d.Close() }()
	ids, docs := textV2BlockMaxMultiTermDocs2688(1024, 64)
	col := createTextSearchCollection2627(b, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "title", Weight: 5}, {Field: "body"}}}, ids, docs)
	base := TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorOR, TopK: 16, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4, ResultMode: TextSearchResultModeScoreOnly}
	for _, tc := range []struct {
		name string
		opts TextSearchOptions
	}{
		{name: "no_explain", opts: base},
		{name: "explain", opts: func() TextSearchOptions { opts := base; opts.Explain = true; return opts }()},
		{name: "phrase_explain", opts: TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "refund policy"}, TopK: 16, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 4, ResultMode: TextSearchResultModeScoreOnly, Explain: true}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			warm, err := col.searchText(tc.opts, textSearchResultScoreOnly)
			if err != nil || len(warm.Results) == 0 {
				b.Fatalf("warm search response=%+v err=%v", warm, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var sink TextSearchResponse
			for i := 0; i < b.N; i++ {
				got, err := col.searchText(tc.opts, textSearchResultScoreOnly)
				if err != nil {
					b.Fatalf("search: %v", err)
				}
				sink = got
			}
			b.StopTimer()
			b.ReportMetric(float64(sink.Stats.TextPostingsScanned), "postings/search")
			b.ReportMetric(float64(sink.Stats.TextCandidatesScored), "candidates_scored/search")
			b.ReportMetric(float64(sink.Stats.TextWANDPivots), "wand_pivots/search")
			b.ReportMetric(float64(sink.Stats.TextScalarPrefilterIDs), "scalar_prefilter_ids/search")
			b.ReportMetric(float64(sink.Stats.TextScalarPostingBlocksSkipped), "scalar_posting_blocks_skipped/search")
			b.ReportMetric(float64(sink.Stats.TextScalarPostingsRejected), "scalar_postings_rejected/search")
			if sink.Explain != nil {
				b.ReportMetric(float64(len(sink.Explain.Results)), "explain_results/search")
			}
		})
	}
}

type explainShapeGolden2838 struct {
	Path   TextSearchExplainServingPath `json:"path"`
	Terms  []string                     `json:"terms"`
	Scalar bool                         `json:"scalar"`
	Phrase bool                         `json:"phrase"`
}

func explainShapeGoldenJSON2838(t *testing.T, explain *TextSearchExplain) string {
	t.Helper()
	if explain == nil {
		return "null"
	}
	raw, err := json.Marshal(explainShapeGolden2838{
		Path:   explain.Serving.Path,
		Terms:  explainTermNames2838(explain),
		Scalar: explain.Serving.ScalarPruning.Enabled,
		Phrase: explain.Phrase != nil && len(explain.Phrase.Terms) != 0,
	})
	if err != nil {
		t.Fatalf("marshal explain shape golden: %v", err)
	}
	return string(raw)
}

type explainSummary2838 struct {
	Path     TextSearchExplainServingPath `json:"path"`
	Terms    []string                     `json:"terms"`
	Results  int                          `json:"results"`
	Counters TextSearchExplainCounters    `json:"counters"`
}

func mustMarshalExplainSummary2838(t *testing.T, explain *TextSearchExplain) string {
	t.Helper()
	if explain == nil {
		return "null"
	}
	raw, err := json.Marshal(explainSummary2838{Path: explain.Serving.Path, Terms: explainTermNames2838(explain), Results: len(explain.Results), Counters: explain.Counters})
	if err != nil {
		t.Fatalf("marshal explain summary: %v", err)
	}
	return string(raw)
}

func explainTermNames2838(explain *TextSearchExplain) []string {
	if explain == nil {
		return nil
	}
	terms := make([]string, len(explain.Terms))
	for i, term := range explain.Terms {
		terms[i] = term.Term
	}
	return terms
}

func assertExplainScoreComponents2838(t *testing.T, response TextSearchResponse) {
	t.Helper()
	if response.Explain == nil || len(response.Explain.Results) == 0 {
		t.Fatalf("missing explain results: %+v", response.Explain)
	}
	first := response.Explain.Results[0]
	if len(first.Terms) == 0 {
		t.Fatalf("first explain result=%+v want score terms", first)
	}
	var sum float64
	for _, term := range first.Terms {
		if term.Term == "" || term.DocumentFrequency == 0 || term.IDF <= 0 || len(term.Fields) == 0 {
			t.Fatalf("term component=%+v want BM25F fields", term)
		}
		sum += term.Score
	}
	if math.Abs(sum-first.Score) > 1e-9 {
		t.Fatalf("score components sum=%0.12f score=%0.12f result=%+v", sum, first.Score, first)
	}
}

type stableTextExplainCounters2838 struct {
	PostingsScanned            uint64
	PostingBlocksVisited       uint64
	PostingBlocksSkipped       uint64
	BlockMaxFallbacks          uint64
	BlockMaxThresholds         uint64
	WANDPivots                 uint64
	ScalarPrefilterIDs         uint64
	ScalarPostingBlocksSkipped uint64
	ScalarPostingsRejected     uint64
	CandidatesScored           uint64
	CandidatesReturned         uint64
	NormLookups                uint64
	MatchDetailsBuilt          uint64
	DocumentsFetched           uint64
	FailClosed                 uint64
	FailClosedReason           string
}

func textExplainStableCounters2838(stats TextSearchStats) stableTextExplainCounters2838 {
	return stableTextExplainCounters2838{
		PostingsScanned:            stats.TextPostingsScanned,
		PostingBlocksVisited:       stats.TextPostingBlocksVisited,
		PostingBlocksSkipped:       stats.TextPostingBlocksSkipped,
		BlockMaxFallbacks:          stats.TextBlockMaxFallbacks,
		BlockMaxThresholds:         stats.TextBlockMaxThresholds,
		WANDPivots:                 stats.TextWANDPivots,
		ScalarPrefilterIDs:         stats.TextScalarPrefilterIDs,
		ScalarPostingBlocksSkipped: stats.TextScalarPostingBlocksSkipped,
		ScalarPostingsRejected:     stats.TextScalarPostingsRejected,
		CandidatesScored:           stats.TextCandidatesScored,
		CandidatesReturned:         stats.TextCandidatesReturned,
		NormLookups:                stats.TextNormLookups,
		MatchDetailsBuilt:          stats.TextMatchDetailsBuilt,
		DocumentsFetched:           stats.DocumentsFetched,
		FailClosed:                 stats.FailClosed,
		FailClosedReason:           stats.FailClosedReason,
	}
}
