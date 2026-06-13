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

func TestTextV2DefaultSafe10KCandidateAndScalarBudgets2687(t *testing.T) {
	fixture := openIndexInsertSearchInsertedTextV2Fixture2564(t, 10000, 16, 8)
	defer func() { _ = fixture.db.Close() }()

	text, err := fixture.col.SearchHybridTextCandidates(HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64})
	if err != nil {
		t.Fatalf("SearchHybridTextCandidates 10k v2: %v", err)
	}
	if len(text.Candidates) != 64 {
		t.Fatalf("text candidates=%d want 64", len(text.Candidates))
	}
	if text.Stats.FailClosed != 0 || text.Stats.DocumentsFetched != 0 || text.Stats.FullDocumentScanFallbacks != 0 || text.Stats.TextStateLookups != 0 || text.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("text stats=%+v want default-safe score-only no-doc path", text.Stats)
	}
	if text.Stats.TextCandidatesScored == 0 || text.Stats.TextCandidatesScored > hybridTextCandidateDefaultScanCandidateLimit || text.Stats.TextPostingsScanned == 0 || text.Stats.TextPostingsScanned > 10000 || text.Stats.TextBlockMaxFallbacks > 1 {
		t.Fatalf("text stats=%+v want bounded 10k multi-term work under safe budget", text.Stats)
	}

	hybrid, err := fixture.col.SearchHybrid(HybridSearchOptions{
		TopK:         10,
		Text:         &HybridTextQuery{IndexName: hybridCloseoutTextIndexName2506, Query: "refund policy", CandidateLimit: 64},
		ScalarFilter: &HybridScalarFilter{IndexName: hybridCloseoutTenantIndexName2506, Value: "tenant-rare-06pct"},
	})
	if err != nil {
		t.Fatalf("SearchHybrid 10k v2 scalar: %v", err)
	}
	if len(hybrid.Results) != 10 {
		t.Fatalf("hybrid results=%d want 10", len(hybrid.Results))
	}
	if hybrid.Stats.FailClosed != 0 || hybrid.Stats.DocumentsFetched != 0 || hybrid.Stats.FullDocumentScanFallbacks != 0 || hybrid.Stats.TextStateLookups != 0 || hybrid.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("hybrid stats=%+v want no-doc score-only fail-closed-free path", hybrid.Stats)
	}
	if hybrid.Stats.ScalarPrefilterIDs != 625 || hybrid.Stats.TextCandidatesReturned != 64 || hybrid.Stats.TextCandidatesScored == 0 || hybrid.Stats.TextCandidatesScored > hybrid.Stats.ScalarPrefilterIDs || hybrid.Stats.TextPostingsScanned == 0 || hybrid.Stats.TextPostingsScanned > 10000 || hybrid.Stats.TextBlockMaxFallbacks > 1 {
		t.Fatalf("hybrid stats=%+v want scalar-pruned v2 scoring under safe budgets", hybrid.Stats)
	}
}

func TestTextV2SearchIncludeDocumentsFetchesOnlyTopKAndDetailsLazy2627(t *testing.T) {
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
	if len(got.Results[0].TextMatches) == 0 || !slicesEqualStrings(got.Results[0].MatchedTerms, []string{"refund"}) {
		t.Fatalf("result=%+v want v2 lazy detailed match summary", got.Results[0])
	}
	if got.Stats.DocumentsFetched == 0 || got.Stats.DocumentsFetched > uint64(len(got.Results)) || got.Stats.DocumentsFetched > 1 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want bounded topK document fetch only", got.Stats)
	}
	if got.Stats.TextStateLookups != 0 || got.Stats.TextMatchDetailsBuilt != uint64(len(got.Results)) || got.Stats.TextMatchDetailsBuilt > 1 {
		t.Fatalf("stats=%+v want topK-bounded lazy detail materialization and no text-state lookup", got.Stats)
	}
}

func TestTextV2SearchResultModesAndTopKLazyDetails2629(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, Fields: []TextIndexField{{Field: "title", Weight: 4}, {Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"title":"refund","body":"refund refund policy"}`),
		[]byte(`{"title":"plain","body":"refund policy"}`),
		[]byte(`{"title":"shipping","body":"refund"}`),
	})

	scoreOnly, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: 8, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil {
		t.Fatalf("SearchText score-only: %v", err)
	}
	if len(scoreOnly.Results) != 1 || len(scoreOnly.Results[0].TextMatches) != 0 || len(scoreOnly.Results[0].MatchedTerms) != 0 || scoreOnly.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("score-only response=%+v want no match details", scoreOnly)
	}

	detailed, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: 8})
	if err != nil {
		t.Fatalf("SearchText detailed: %v", err)
	}
	if len(detailed.Results) != 1 || !bytes.Equal(detailed.Results[0].DocumentID, scoreOnly.Results[0].DocumentID) {
		t.Fatalf("detailed results=%+v scoreOnly=%+v want same top result", detailed.Results, scoreOnly.Results)
	}
	if detailed.Stats.TextMatchDetailsBuilt != uint64(len(detailed.Results)) || detailed.Stats.TextMatchDetailsBuilt > detailed.Stats.TextCandidatesScored {
		t.Fatalf("detailed stats=%+v want details bounded to returned topK and scored candidates", detailed.Stats)
	}
	if len(detailed.Results[0].TextMatches) == 0 || !slicesEqualStrings(detailed.Results[0].MatchedTerms, []string{"refund"}) || len(detailed.Results[0].MatchedFields) == 0 {
		t.Fatalf("detailed result=%+v want field/term attribution", detailed.Results[0])
	}

	compact, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: 8, ResultMode: TextSearchResultModeCompact})
	if err != nil {
		t.Fatalf("SearchText compact: %v", err)
	}
	if len(compact.Results) != 1 || len(compact.Results[0].TextMatches) == 0 || len(compact.Results[0].MatchedTerms) != 0 || len(compact.Results[0].MatchedFields) != 0 {
		t.Fatalf("compact result=%+v want TextMatches without legacy matched lists", compact.Results)
	}
	if compact.Stats.TextMatchDetailsBuilt != uint64(len(compact.Results)) || compact.Stats.DocumentsFetched != 0 || compact.Stats.TextStateLookups != 0 {
		t.Fatalf("compact stats=%+v want topK detail build, no docs, no text-state", compact.Stats)
	}
}

func TestTextV2PositionsLaneCorruptionFailsClosedOnlyForDetailedMode2629(t *testing.T) {
	d := openTextV2TestDB(t, t.TempDir(), false)
	defer func() { _ = d.Close() }()
	col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, StoreOffsets: true, Fields: []TextIndexField{{Field: "body"}}}, [][]byte{[]byte("d1"), []byte("d2")}, [][]byte{
		[]byte(`{"body":"unique2629 refund"}`),
		[]byte(`{"body":"other refund"}`),
	})

	before, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "unique2629", TopK: 1})
	if err != nil {
		t.Fatalf("SearchText before corruption: %v", err)
	}
	if len(before.Results) != 1 || string(before.Results[0].DocumentID) != "d1" || before.Stats.TextMatchDetailsBuilt != 1 {
		t.Fatalf("before response=%+v want detailed d1", before)
	}

	var positionKey []byte
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		doc, ok, err := readTextV2DocIDAtRoot(snap, catalog, collectionTextV2DocIDRootName("docs", "lexical"), []byte("d1"))
		if err != nil || !ok {
			t.Fatalf("read docid ok=%v err=%v", ok, err)
		}
		positionKey = encodeTextV2PositionKey(doc.Ordinal, "unique2629")
	})
	raw := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2PositionsRootName("docs", "lexical"), positionKey)
	position, err := decodeTextV2PositionValue(raw)
	if err != nil {
		t.Fatalf("decode position value before corruption: %v", err)
	}
	position.Fields[0].Frequency++
	position.Fields[0].Positions = append(position.Fields[0].Positions, position.Fields[0].Positions[len(position.Fields[0].Positions)-1]+1)
	position.Fields[0].Offsets = append(position.Fields[0].Offsets, textTokenOffset{Start: 0, End: 1})
	corruptTextRootValue(t, d, "docs", collectionTextV2PositionsRootName("docs", "lexical"), positionKey, encodeTextV2PositionValue(position))
	if _, err := col.TextIndexStorageStats("lexical"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("TextIndexStorageStats after position/scoring mismatch err=%v want storage corrupt", err)
	}

	scoreOnly, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "unique2629", TopK: 1, ResultMode: TextSearchResultModeScoreOnly})
	if err != nil || len(scoreOnly.Results) != 1 || scoreOnly.Stats.TextMatchDetailsBuilt != 0 || scoreOnly.Stats.DocumentsFetched != 0 {
		t.Fatalf("score-only after corruption response=%+v err=%v want no position-lane read", scoreOnly, err)
	}
	compact, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "unique2629", TopK: 1, ResultMode: TextSearchResultModeCompact})
	if err != nil || len(compact.Results) != 1 || len(compact.Results[0].TextMatches) == 0 || compact.Stats.TextMatchDetailsBuilt != 1 || compact.Stats.DocumentsFetched != 0 {
		t.Fatalf("compact after corruption response=%+v err=%v want scoring-posting attribution without position-lane read", compact, err)
	}
	hybridCompact, err := col.SearchHybridTextCandidates(HybridTextQuery{IndexName: "lexical", Query: "unique2629", CandidateLimit: 1, IncludeTextMatches: true})
	if err != nil || len(hybridCompact.Candidates) != 1 || len(hybridCompact.Candidates[0].TextMatches) == 0 || hybridCompact.Stats.TextMatchDetailsBuilt != 1 || hybridCompact.Stats.DocumentsFetched != 0 || hybridCompact.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("hybrid compact after corruption response=%+v err=%v want zero-doc compact attribution without position-lane read", hybridCompact, err)
	}
	detailed, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "unique2629", TopK: 1, IncludeDocuments: true})
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("detailed after corruption err=%v want text index storage corrupt", err)
	}
	if detailed.Stats.FailClosed != 1 || detailed.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || detailed.Stats.DocumentsFetched != 0 || detailed.Stats.TextMatchDetailsBuilt != 0 {
		t.Fatalf("detailed stats=%+v want fail-closed before document fetch/detail count", detailed.Stats)
	}
}

func TestTextV2PositionsLaneUpdateDeleteRemovesStaleEntries2629(t *testing.T) {
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
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV2, StorePositions: true, Fields: []TextIndexField{{Field: "body"}}}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"oldtoken2629 keep"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	oldKey := textV2PositionKeyForDocumentTerm2629(t, d, "docs", "lexical", []byte("d1"), "oldtoken2629")
	if raw := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2PositionsRootName("docs", "lexical"), oldKey); len(raw) == 0 {
		t.Fatalf("old position entry missing before update")
	}
	if _, _, err := col.Update([]byte("d1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"body":"newtoken2629 keep"}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		if _, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2PositionsRootName("docs", "lexical"), oldKey, nil); err != nil || ok {
			t.Fatalf("old position entry after update ok=%v err=%v want deleted", ok, err)
		}
	})
	newKey := textV2PositionKeyForDocumentTerm2629(t, d, "docs", "lexical", []byte("d1"), "newtoken2629")
	if raw := textV2ReadRootBytes2624(t, d, "docs", collectionTextV2PositionsRootName("docs", "lexical"), newKey); len(raw) == 0 {
		t.Fatalf("new position entry missing after update")
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("d1")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	withTextCatalog(t, d, "docs", func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		if _, ok, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionTextV2PositionsRootName("docs", "lexical"), newKey, nil); err != nil || ok {
			t.Fatalf("new position entry after delete ok=%v err=%v want deleted", ok, err)
		}
	})
	if _, err := col.TextIndexStorageStats("lexical"); err != nil {
		t.Fatalf("TextIndexStorageStats after update/delete cleanup: %v", err)
	}
}

func textV2PositionKeyForDocumentTerm2629(t *testing.T, d *backenddb.DB, collection, index string, documentID []byte, term string) []byte {
	t.Helper()
	var key []byte
	withTextCatalog(t, d, collection, func(snap *backenddb.Snapshot, catalog *collectionCatalog) {
		doc, ok, err := readTextV2DocIDAtRoot(snap, catalog, collectionTextV2DocIDRootName(collection, index), documentID)
		if err != nil || !ok {
			t.Fatalf("read docid %q ok=%v err=%v", string(documentID), ok, err)
		}
		key = encodeTextV2PositionKey(doc.Ordinal, term)
	})
	return key
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
