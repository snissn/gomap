package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSearchHybridTextCandidatesNoDocumentsStableIDs2503(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "title", Weight: 4}, {Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"refund","body":"refund refund policy"}`),
		[]byte(`{"title":"plain","body":"refund policy"}`),
		[]byte(`{"title":"shipping","body":"policy"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	opts := HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2}
	got, err := col.SearchHybridTextCandidates(opts)
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates: %v", err)
	}

	assertHybridTextCandidateBasics2503(t, got.Candidates, "lexical", false)
	if len(got.Candidates) != 2 {
		t.Fatalf("candidates=%d want 2", len(got.Candidates))
	}
	if string(got.Candidates[0].ID) != "d1" || string(got.Candidates[1].ID) != "d2" || got.Candidates[0].Score <= got.Candidates[1].Score {
		t.Fatalf("candidates=%+v want d1 above d2 by BM25F", got.Candidates)
	}
	if len(got.Candidates[0].TextMatches) != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("candidate[0] matches=%+v stats=%+v want default score-only hybrid text candidates", got.Candidates[0].TextMatches, got.Stats)
	}
	explicitMatches, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2, IncludeTextMatches: true})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates IncludeTextMatches: %v", err)
	}
	assertHybridTextCandidateBasics2503(t, explicitMatches.Candidates, "lexical", true)
	if explicitMatches.Candidates[0].TextMatches[0].Field != "body" || !slicesEqualStrings(explicitMatches.Candidates[0].TextMatches[0].Terms, []string{"refund"}) {
		t.Fatalf("explicit candidate[0] matches=%+v want body/refund attribution", explicitMatches.Candidates[0].TextMatches)
	}
	if explicitMatches.Stats.TextMatchDetailsBuilt != uint64(len(explicitMatches.Candidates)) || explicitMatches.Stats.DocumentsFetched != 0 || explicitMatches.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("explicit stats=%+v want bounded opt-in match details and no docs", explicitMatches.Stats)
	}
	if got.Stats.TextCandidatesRequested != uint64(opts.CandidateLimit) || got.Stats.TextCandidatesReturned != 2 || got.Stats.TextPostingsScanned != 2 || got.Stats.TextCandidatesScored != 2 {
		t.Fatalf("stats=%+v want requested=2 returned=2 postings=2 scored=2", got.Stats)
	}
	if got.Stats.DocumentsFetched != 0 || got.Stats.DocumentsMissing != 0 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 || got.Stats.Truncated != 0 {
		t.Fatalf("stats=%+v want no document fetch/fallback/fail-closed/truncation", got.Stats)
	}
	firstID := append([]byte(nil), got.Candidates[0].ID...)
	got.Candidates[0].ID[0] = 'X'

	again, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1})
	if err != nil {
		t.Fatalf("second SearchHybridTextCandidates: %v", err)
	}
	if len(again.Candidates) != 1 || !bytes.Equal(again.Candidates[0].ID, firstID) || again.Candidates[0].SourceRank != 1 {
		t.Fatalf("second candidates=%+v want stable response-owned top candidate %q", again.Candidates, firstID)
	}
	if again.Stats.TextCandidatesReturned != 1 || again.Stats.Truncated != 1 || again.Stats.DocumentsFetched != 0 || again.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("second stats=%+v want top-N bound with zero document work", again.Stats)
	}
}

func TestSearchHybridTextCandidatesExplicitPostingsBudget4329(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d1"), []byte("d2")},
		[][]byte{[]byte(`{"body":"refund"}`), []byte(`{"body":"refund"}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	base := HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2}
	if got, err := col.SearchHybridTextCandidates(base); err != nil || len(got.Candidates) != 2 {
		t.Fatalf("default postings contract response=%+v err=%v want two candidates", got, err)
	}
	base.MaxPostingsScanned = 1
	got, err := col.SearchHybridTextCandidates(base)
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || got.Stats.FailClosed != 1 || got.Stats.TextPostingsScanned == 0 {
		t.Fatalf("explicit postings ceiling response=%+v err=%v want fail-closed bounded work", got, err)
	}
}

func TestSearchHybridTextCandidatesUnsupportedAndUnavailableFailClosed2503(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	tests := []struct {
		name       string
		query      HybridTextQuery
		wantErr    error
		wantReason HybridFailClosedReason
	}{
		{name: "missing_index", query: HybridTextQuery{IndexName: "missing", Query: "refund", CandidateLimit: 1}, wantErr: ErrHybridSearchIndexUnavailable, wantReason: HybridFailClosedReasonTextIndexUnavailable},
		{name: "unsupported_phrase", query: HybridTextQuery{IndexName: "lexical", Query: `"refund policy"`, CandidateLimit: 1}, wantErr: ErrHybridSearchUnsupported, wantReason: HybridFailClosedReasonUnsupported},
		{name: "zero_candidate_limit", query: HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 0}, wantErr: ErrHybridSearchUnsupported, wantReason: HybridFailClosedReasonUnsupported},
		{name: "negative_candidate_limit", query: HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: -1}, wantErr: ErrHybridSearchUnsupported, wantReason: HybridFailClosedReasonUnsupported},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := col.SearchHybridTextCandidates(tc.query)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("SearchHybridTextCandidates err=%v want %v", err, tc.wantErr)
			}
			if tc.wantErr == ErrHybridSearchUnsupported && errors.Is(err, ErrHybridSearchIndexUnavailable) {
				t.Fatalf("SearchHybridTextCandidates err=%v must not wrap ErrHybridSearchIndexUnavailable for unsupported text shape", err)
			}
			if tc.wantErr == ErrHybridSearchIndexUnavailable && errors.Is(err, ErrHybridSearchUnsupported) {
				t.Fatalf("SearchHybridTextCandidates err=%v must not wrap ErrHybridSearchUnsupported for unavailable text index", err)
			}
			if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != tc.wantReason {
				t.Fatalf("response=%+v want fail-closed reason %q and no candidates", got, tc.wantReason)
			}
			if got.Stats.DocumentsFetched != 0 || got.Stats.DocumentsMissing != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("stats=%+v want no document fetch or fallback on fail-closed text candidate path", got.Stats)
			}
		})
	}
}

func TestSearchHybridTextCandidatesBudgetAndStorageFailClosed2503(t *testing.T) {
	t.Run("budget_truncation", func(t *testing.T) {
		d := openTextTestDB(t)
		defer func() { _ = d.Close() }()
		col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
		ids := make([][]byte, hybridTextCandidateDefaultScanCandidateLimit+1)
		docs := make([][]byte, len(ids))
		for i := range ids {
			ids[i] = []byte(fmt.Sprintf("doc-%04d", i))
			docs[i] = []byte(`{"body":"refund"}`)
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}

		got, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1})
		if !errors.Is(err, ErrHybridSearchIndexUnavailable) || !errors.Is(err, ErrTextIndexUnavailable) {
			t.Fatalf("SearchHybridTextCandidates err=%v want hybrid/text unavailable", err)
		}
		if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable || got.Stats.Truncated == 0 {
			t.Fatalf("response=%+v want budget fail-closed truncation", got)
		}
		if got.Stats.TextPostingsScanned == 0 || got.Stats.TextCandidatesScored != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("stats=%+v want postings work, no scored/doc/fallback work", got.Stats)
		}
	})

	t.Run("storage_corrupt", func(t *testing.T) {
		d := openTextTestDB(t)
		defer func() { _ = d.Close() }()
		col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
		if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
			t.Fatalf("Insert: %v", err)
		}
		corruptTextRootValue(t, d, "docs", collectionTextStatsRootName("docs", "lexical"), encodeTextStatsCorpusKey(), []byte{99})

		got, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1})
		if !errors.Is(err, ErrHybridSearchIndexUnavailable) || !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("SearchHybridTextCandidates err=%v want hybrid unavailable and text storage corrupt", err)
		}
		if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
			t.Fatalf("response=%+v want storage-corrupt fail-closed stats and no document work", got)
		}
	})
}

func TestSearchHybridTextCandidatesReopenNoDocuments2503(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	firstClosed := false
	defer func() {
		if !firstClosed {
			_ = d.Close()
		}
	}()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{
		[]byte(`{"title":"refund","body":"policy refund"}`),
		[]byte(`{"title":"policy","body":"refund"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	before, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates before reopen: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	firstClosed = true

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	after, err := reopenedCol.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates after reopen: %v", err)
	}
	if len(before.Candidates) != len(after.Candidates) {
		t.Fatalf("reopen candidate length before=%d after=%d", len(before.Candidates), len(after.Candidates))
	}
	for i := range before.Candidates {
		if !bytes.Equal(before.Candidates[i].ID, after.Candidates[i].ID) || before.Candidates[i].SourceRank != after.Candidates[i].SourceRank || math.Abs(before.Candidates[i].Score-after.Candidates[i].Score) > 1e-12 {
			t.Fatalf("candidate[%d] before=%+v after=%+v", i, before.Candidates[i], after.Candidates[i])
		}
	}
	if before.Stats.DocumentsFetched != 0 || after.Stats.DocumentsFetched != 0 || before.Stats.FullDocumentScanFallbacks != 0 || after.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("stats before=%+v after=%+v want no document work", before.Stats, after.Stats)
	}
}

func TestHybridTextCandidatesRejectDocumentMaterialization2503(t *testing.T) {
	got, err := hybridTextCandidatesFromSearchResponse(1, "lexical", TextSearchResponse{
		IndexName: "lexical",
		Stats: TextSearchStats{
			TextCandidatesReturned: 1,
			DocumentsFetched:       1,
			DocumentFetchNanos:     7,
		},
		Results: []TextSearchResult{{DocumentID: []byte("doc-a"), Rank: 1, Score: 1, ScoreKind: HybridScoreKindBM25F, Document: []byte(`{"body":"refund"}`)}},
	})
	if !errors.Is(err, ErrHybridSearchUnsupported) {
		t.Fatalf("hybridTextCandidatesFromSearchResponse err=%v want ErrHybridSearchUnsupported", err)
	}
	if len(got.Candidates) != 0 || got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != HybridFailClosedReasonFullDocumentScanForbidden {
		t.Fatalf("response=%+v want fail-closed document materialization guard", got)
	}
	if got.Stats.DocumentsFetched != 1 || got.Stats.TextCandidatesReturned != 0 {
		t.Fatalf("stats=%+v want document-fetch counter preserved and no returned candidates", got.Stats)
	}
}

func assertHybridTextCandidateBasics2503(tb testing.TB, got []HybridSearchCandidate, indexName string, wantMatches bool) {
	tb.Helper()
	for i, candidate := range got {
		if candidate.Source != HybridCandidateSourceText || candidate.IndexName != indexName || candidate.SourceRank != i+1 || candidate.ScoreKind != HybridScoreKindBM25F {
			tb.Fatalf("candidate[%d]=%+v want text source/index/rank/bm25f score kind", i, candidate)
		}
		if candidate.Score <= 0 {
			tb.Fatalf("candidate[%d]=%+v want positive BM25F score", i, candidate)
		}
		if len(candidate.ID) == 0 || cap(candidate.ID) != len(candidate.ID) {
			tb.Fatalf("candidate[%d] id len/cap=%d/%d want response-owned cap-isolated stable ID", i, len(candidate.ID), cap(candidate.ID))
		}
		if wantMatches && len(candidate.TextMatches) == 0 {
			tb.Fatalf("candidate[%d]=%+v want text match attribution", i, candidate)
		}
		if !wantMatches && len(candidate.TextMatches) != 0 {
			tb.Fatalf("candidate[%d]=%+v want score-only text candidate", i, candidate)
		}
	}
}

var hybridTextCandidateBenchmarkSink2503 HybridCandidateResponse

func BenchmarkSearchHybridTextCandidates2503(b *testing.B) {
	const docCount = 512
	d := openTextBenchDB(b)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		b.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}); err != nil {
		b.Fatalf("CreateTextIndex: %v", err)
	}
	ids := make([][]byte, docCount)
	docs := make([][]byte, docCount)
	for i := 0; i < docCount; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"Ticket %d refund policy","body":"HTTP_500 retry refund policy customer %d shard %d"}`, i, i%17, i%8))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch setup: %v", err)
	}
	opts := HybridTextQuery{IndexName: "lexical", Query: "refund policy", CandidateLimit: 20}
	warm, err := col.SearchHybridTextCandidates(opts)
	if err != nil {
		b.Fatalf("warm SearchHybridTextCandidates: %v", err)
	}
	if len(warm.Candidates) != opts.CandidateLimit || warm.Stats.DocumentsFetched != 0 || warm.Stats.FullDocumentScanFallbacks != 0 {
		b.Fatalf("warm response=%+v want %d no-document candidates", warm, opts.CandidateLimit)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink HybridCandidateResponse
	for i := 0; i < b.N; i++ {
		got, err := col.SearchHybridTextCandidates(opts)
		if err != nil {
			b.Fatalf("SearchHybridTextCandidates: %v", err)
		}
		if len(got.Candidates) != opts.CandidateLimit {
			b.Fatalf("candidates=%d want %d", len(got.Candidates), opts.CandidateLimit)
		}
		sink = got
	}
	b.StopTimer()
	hybridTextCandidateBenchmarkSink2503 = sink
	b.ReportMetric(float64(sink.Stats.TextCandidatesReturned), "candidates/search")
	b.ReportMetric(float64(sink.Stats.TextPostingsScanned), "postings_scanned/search")
	b.ReportMetric(float64(sink.Stats.TextPostingBlocksVisited), "posting_blocks_visited/search")
	b.ReportMetric(float64(sink.Stats.TextPostingBlocksSkipped), "posting_blocks_skipped/search")
	b.ReportMetric(float64(sink.Stats.TextCandidatesScored), "candidates_scored/search")
	b.ReportMetric(float64(sink.Stats.TextStateLookups), "state_lookups/search")
	b.ReportMetric(float64(sink.Stats.TextNormLookups), "norm_lookups/search")
	b.ReportMetric(float64(sink.Stats.TextMatchDetailsBuilt), "match_details/search")
	b.ReportMetric(float64(sink.Stats.DocumentsFetched), "docs_fetched/search")
}
