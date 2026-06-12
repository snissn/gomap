package collections

import (
	"bytes"
	"errors"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTextV2SearchScoreParity2627(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	fields := []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}
	ids := [][]byte{[]byte("d1"), []byte("d2"), []byte("d3"), []byte("d4")}
	docs := [][]byte{
		[]byte(`{"title":"refund policy","body":"refund refund chargeback"}`),
		[]byte(`{"title":"policy","body":"refund shipping"}`),
		[]byte(`{"title":"refund","body":"shipping"}`),
		[]byte(`{"title":"other","body":"policy policy"}`),
	}
	v1 := createTextSearchCollection2627(t, d, "docs_v1", TextIndexDefinition{Name: "lexical", Fields: fields}, ids, docs)
	v2 := createTextSearchCollection2627(t, d, "docs_v2", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: fields}, ids, docs)

	cases := []TextSearchOptions{
		{IndexName: "lexical", Query: "refund", TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64},
		{IndexName: "lexical", Query: "refund AND policy", Operator: TextSearchOperatorAND, TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64},
		{IndexName: "lexical", Query: "shipping OR policy", TopK: 4, CandidateLimit: 16, MaxPostingsScanned: 64},
	}
	for _, opts := range cases {
		t.Run(opts.Query, func(t *testing.T) {
			want, err := v1.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("v1 search: %v", err)
			}
			got, err := v2.searchText(opts, textSearchResultScoreOnly)
			if err != nil {
				t.Fatalf("v2 search: %v", err)
			}
			assertTextSearchParity2627(t, got, want)
			if got.Stats.TextPostingBlocksVisited == 0 || got.Stats.TextPostingsScanned == 0 || got.Stats.TextNormLookups == 0 {
				t.Fatalf("v2 stats=%+v want posting block scans and norm-block lookups", got.Stats)
			}
			if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("v2 stats=%+v want score-only/no-doc/no-state path", got.Stats)
			}
		})
	}
}

func TestTextV2HybridTextCandidatesNoDocCounters2627(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 4}, {Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"refund","body":"refund refund policy","city":"sea"}`),
		[]byte(`{"title":"plain","body":"refund policy","city":"pdx"}`),
		[]byte(`{"title":"shipping","body":"policy","city":"sea"}`),
	})

	got, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates v2: %v", err)
	}
	if len(got.Candidates) != 2 || string(got.Candidates[0].ID) != "d1" || string(got.Candidates[1].ID) != "d2" {
		t.Fatalf("candidates=%+v want d1,d2", got.Candidates)
	}
	if got.Candidates[0].Score <= got.Candidates[1].Score {
		t.Fatalf("candidates=%+v want d1 higher score", got.Candidates)
	}
	for i, candidate := range got.Candidates {
		if len(candidate.TextMatches) != 0 {
			t.Fatalf("candidate[%d] matches=%+v want score-only v2 candidate", i, candidate.TextMatches)
		}
	}
	stats := got.Stats
	if stats.DocumentsFetched != 0 || stats.DocumentsMissing != 0 || stats.FullDocumentScanFallbacks != 0 || stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want no documents/fallback/fail-closed", stats)
	}
	if stats.TextStateLookups != 0 || stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("stats=%+v want zero state lookups and match-detail builds", stats)
	}
	if stats.TextPostingBlocksVisited == 0 || stats.TextPostingsScanned == 0 || stats.TextCandidatesScored == 0 || stats.TextNormLookups == 0 {
		t.Fatalf("stats=%+v want v2 block/norm/scoring counters", stats)
	}
}

func TestTextV2SearchIncludeDocumentsFetchesOnlyTopK2627(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 4}, {Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"refund","body":"refund policy"}`),
		[]byte(`{"title":"plain","body":"refund"}`),
		[]byte(`{"title":"shipping","body":"policy"}`),
	})

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: 8, IncludeDocuments: true})
	if err != nil {
		t.Fatalf("SearchText IncludeDocuments v2: %v", err)
	}
	if len(got.Results) != 1 || len(got.Results[0].Document) == 0 {
		t.Fatalf("results=%+v want one materialized topK document", got.Results)
	}
	if !bytes.Contains(got.Results[0].Document, []byte("refund")) {
		t.Fatalf("document=%s want fetched refund document", got.Results[0].Document)
	}
	if len(got.Results[0].TextMatches) != 0 || len(got.Results[0].MatchedTerms) != 0 || len(got.Results[0].MatchedFields) != 0 {
		t.Fatalf("result=%+v want v2 M4 score-only match details", got.Results[0])
	}
	if got.Stats.DocumentsFetched == 0 || got.Stats.DocumentsFetched > uint64(len(got.Results)) || got.Stats.DocumentsFetched > 1 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want bounded topK document fetch only", got.Stats)
	}
	if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("stats=%+v want score-only v2 search despite final fetch", got.Stats)
	}
}

func TestTextV2SearchSkipsStaleGenerationsAndTombstones2627(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"refund shipping"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"updated only"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}

	refund, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: 1, MaxPostingsScanned: 1}, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("refund search with stale-only blocks and low budgets: %v", err)
	}
	if len(refund.Results) != 0 || refund.Stats.TextPostingsScanned != 0 || refund.Stats.TextPostingBlocksVisited != 0 || refund.Stats.TextCandidatesScored != 0 || refund.Stats.FailClosed != 0 {
		t.Fatalf("refund response=%+v want stale zero-df blocks ignored without consuming budget", refund)
	}
	updated, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "updated", TopK: 10, CandidateLimit: 10, MaxPostingsScanned: 64}, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("updated search: %v", err)
	}
	if len(updated.Results) != 1 || string(updated.Results[0].DocumentID) != "d1" {
		t.Fatalf("updated results=%+v want live updated d1", updated.Results)
	}
}

func TestTextV2SearchStalePostingsDoNotConsumeCandidateBudget2627(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2")}, [][]byte{
		[]byte(`{"body":"refund stale"}`),
		[]byte(`{"body":"refund live"}`),
	})
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"updated only"}`), true, nil
	}); err != nil {
		t.Fatalf("Update d1 away from refund: %v", err)
	}

	got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10, CandidateLimit: 1, MaxPostingsScanned: 64}, textSearchResultScoreOnly)
	if err != nil {
		t.Fatalf("refund search with stale-before-live posting and candidateLimit=1: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].DocumentID) != "d2" {
		t.Fatalf("results=%+v want only live d2", got.Results)
	}
	if got.Stats.FailClosed != 0 || got.Stats.TextCandidatesScored != 1 || got.Stats.TextCandidatesReturned != 1 {
		t.Fatalf("stats=%+v want one scored/returned candidate without fail-closed", got.Stats)
	}
}

func TestTextV2SearchFailClosedOnMissingAndCorruptRoots2627(t *testing.T) {
	t.Run("missing docmap descriptor", func(t *testing.T) {
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}, [][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)})
		clearTextRootDescriptor2624(t, d, collectionTextV2DocMapRootName("docs", "lexical"))
		got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1}, textSearchResultScoreOnly)
		if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("SearchText err=%v want unavailable/storage corrupt", err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
			t.Fatalf("stats=%+v want storage fail-closed without docs", got.Stats)
		}
	})
	t.Run("corrupt posting block", func(t *testing.T) {
		d := openTextV2TestDB(t, t.TempDir(), false)
		defer func() { _ = d.Close() }()
		col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}, [][]byte{[]byte("d1")}, [][]byte{[]byte(`{"body":"refund policy"}`)})
		rootName := collectionTextV2PostingBlocksRootName("docs", "lexical")
		blockKey := firstTextV2PostingBlockKeyForTerm2626(t, d, "docs", rootName, "refund")
		corruptTextRootValue(t, d, "docs", rootName, blockKey, []byte{99})
		got, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1}, textSearchResultScoreOnly)
		if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
			t.Fatalf("SearchText err=%v want unavailable/storage corrupt", err)
		}
		if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || got.Stats.DocumentsFetched != 0 {
			t.Fatalf("stats=%+v want storage fail-closed without docs", got.Stats)
		}
	})
}

func TestTextV2SearchSnapshotBindingAndReopen2627(t *testing.T) {
	dir := t.TempDir()
	d := openTextV2TestDB(t, dir, false)
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{[]byte(`{"body":"refund policy"}`), []byte(`{"body":"shipping"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	before, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}, textSearchResultScoreOnly)
	if err != nil || len(before.Results) != 1 || string(before.Results[0].DocumentID) != "d1" {
		t.Fatalf("before response=%+v err=%v want d1", before, err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot nil")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("catalog: %v", err)
	}
	idx, ok := findTextIndex(catalog.meta.TextIndexes, "lexical")
	if !ok {
		_ = snap.Close()
		t.Fatal("missing lexical index")
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"updated only"}`), true, nil
	}); err != nil {
		_ = snap.Close()
		t.Fatalf("Update: %v", err)
	}
	currentRefund, err := col.searchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}, textSearchResultScoreOnly)
	if err != nil || len(currentRefund.Results) != 0 {
		_ = snap.Close()
		t.Fatalf("current refund response=%+v err=%v want no live refund", currentRefund, err)
	}
	oldResponse := TextSearchResponse{IndexName: "lexical"}
	oldResponse.Stats.QueryTerms = 1
	oldResponse.Stats.TextCandidatesRequested = 10
	old, err := executeTextV2SearchAtSnapshot(col, snap, catalog, idx, TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 10}, []string{"refund"}, TextSearchOperatorOR, 10, 64, textSearchResultScoreOnly, oldResponse)
	if closeErr := snap.Close(); closeErr != nil {
		t.Fatalf("snapshot close: %v", closeErr)
	}
	if err != nil || len(old.Results) != 1 || string(old.Results[0].DocumentID) != "d1" {
		t.Fatalf("old snapshot response=%+v err=%v want original d1", old, err)
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
	after, err := reopenedCol.searchText(TextSearchOptions{IndexName: "lexical", Query: "updated", TopK: 10}, textSearchResultScoreOnly)
	if err != nil || len(after.Results) != 1 || string(after.Results[0].DocumentID) != "d1" {
		t.Fatalf("after reopen response=%+v err=%v want updated d1", after, err)
	}
}

func createTextSearchCollection2627(t *testing.T, d *backenddb.DB, collection string, def TextIndexDefinition, ids, docs [][]byte) *Collection {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: collection}); err != nil {
		t.Fatalf("CreateCollection %s: %v", collection, err)
	}
	col, err := mgr.OpenCollection(collection)
	if err != nil {
		t.Fatalf("OpenCollection %s: %v", collection, err)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch %s: %v", collection, err)
	}
	if _, _, err := col.CreateTextIndex(def); err != nil {
		t.Fatalf("CreateTextIndex %s: %v", collection, err)
	}
	return col
}

func assertTextSearchParity2627(t *testing.T, got, want TextSearchResponse) {
	t.Helper()
	if len(got.Results) != len(want.Results) {
		t.Fatalf("result length got=%d want=%d got=%+v want=%+v", len(got.Results), len(want.Results), got.Results, want.Results)
	}
	for i := range want.Results {
		if !bytes.Equal(got.Results[i].DocumentID, want.Results[i].DocumentID) || got.Results[i].Rank != want.Results[i].Rank || math.Abs(got.Results[i].Score-want.Results[i].Score) > 1e-12 {
			t.Fatalf("result[%d] got=%+v want=%+v", i, got.Results[i], want.Results[i])
		}
	}
}
