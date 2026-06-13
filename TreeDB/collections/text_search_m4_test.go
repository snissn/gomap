package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestSearchTextSingleTermRankedSearchM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2"), []byte("d3")}, [][]byte{
		[]byte(`{"body":"refund refund refund"}`),
		[]byte(`{"body":"refund"}`),
		[]byte(`{"body":"shipping policy"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 2, IncludeDocuments: true})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if got.IndexName != "lexical" || len(got.Results) != 2 {
		t.Fatalf("SearchText response=%+v want two lexical results", got)
	}
	if string(got.Results[0].DocumentID) != "d1" || got.Results[0].Rank != 1 || got.Results[0].ScoreKind != HybridScoreKindBM25F {
		t.Fatalf("result[0]=%+v want d1 rank=1 bm25f", got.Results[0])
	}
	if string(got.Results[1].DocumentID) != "d2" || got.Results[1].Rank != 2 || got.Results[0].Score <= got.Results[1].Score {
		t.Fatalf("results=%+v want d1 above d2", got.Results)
	}
	if !bytes.Contains(got.Results[0].Document, []byte("refund refund")) || !slicesEqualStrings(got.Results[0].MatchedTerms, []string{"refund"}) || !slicesEqualStrings(got.Results[0].MatchedFields, []string{"body"}) {
		t.Fatalf("result[0] document/matches=%+v", got.Results[0])
	}
	if got.Stats.QueryTerms != 1 || got.Stats.TextPostingsScanned != 2 || got.Stats.PostingsScanned != 2 || got.Stats.TextCandidatesScored != 2 || got.Stats.CandidatesScored != 2 || got.Stats.DocumentsFetched != 2 || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want postings=2 scored=2 fetched=2 no fallback", got.Stats)
	}
}

func TestSearchTextANDOROperatorsM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("both"), []byte("refund"), []byte("policy")}, [][]byte{
		[]byte(`{"body":"refund policy"}`),
		[]byte(`{"body":"refund"}`),
		[]byte(`{"body":"policy"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	andResult, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund AND policy", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText AND: %v", err)
	}
	if len(andResult.Results) != 1 || string(andResult.Results[0].DocumentID) != "both" {
		t.Fatalf("AND results=%+v want only both", andResult.Results)
	}
	orResult, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund OR policy", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText OR: %v", err)
	}
	if len(orResult.Results) != 3 || orResult.Stats.TextPostingsScanned != 4 || orResult.Stats.TextCandidatesScored != 3 {
		t.Fatalf("OR response=%+v want three docs postings=4 scored=3", orResult)
	}
	missingAnd, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "missing AND refund", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText missing AND refund: %v", err)
	}
	if len(missingAnd.Results) != 0 || missingAnd.Stats.TextPostingsScanned != 0 || missingAnd.Stats.TextCandidatesScored != 0 || missingAnd.Stats.TextCandidatesReturned != 0 || missingAnd.Stats.Truncated || missingAnd.Stats.FailClosed != 0 {
		t.Fatalf("missing AND response=%+v want empty without scanning common term or fail-closed", missingAnd)
	}
}

func TestSearchTextFieldWeightAffectsRankingM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "title", Weight: 8}, {Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("body-heavy"), []byte("title-hit")}, [][]byte{
		[]byte(`{"title":"plain","body":"refund refund refund refund refund refund"}`),
		[]byte(`{"title":"refund","body":"other"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 2})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if len(got.Results) != 2 || string(got.Results[0].DocumentID) != "title-hit" || got.Results[0].Score <= got.Results[1].Score {
		t.Fatalf("weighted results=%+v want title-hit first", got.Results)
	}
	if len(got.Results[0].TextMatches) != 1 || got.Results[0].TextMatches[0].Field != "title" || !slicesEqualStrings(got.Results[0].TextMatches[0].Terms, []string{"refund"}) {
		t.Fatalf("title-hit matches=%+v want title/refund", got.Results[0].TextMatches)
	}
}

func TestSearchTextMissingIndexUnsupportedSyntaxAndTruncationM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("d1"), []byte("d2")}, [][]byte{
		[]byte(`{"body":"refund"}`),
		[]byte(`{"body":"refund"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	if _, err := col.SearchText(TextSearchOptions{IndexName: "missing", Query: "refund", TopK: 1}); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("missing index err=%v want ErrIndexNotFound", err)
	}
	if _, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: `"refund policy"`, TopK: 1}); err == nil {
		t.Fatal("phrase query err=nil want unsupported syntax")
	}
	truncated, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, CandidateLimit: 1})
	if !errors.Is(err, ErrTextIndexUnavailable) {
		t.Fatalf("truncated SearchText err=%v want ErrTextIndexUnavailable", err)
	}
	if truncated.Stats.Truncated != true || truncated.Stats.FailClosed != 1 || truncated.Stats.FailClosedReason != textSearchFailClosedCandidateLimit || truncated.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("truncated stats=%+v want candidate-limit fail closed", truncated.Stats)
	}
}

func TestSearchTextSeesUnflushedTextIndexedInsertM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"visible without caller flush"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "visible", TopK: 10})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].DocumentID) != "d1" || got.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("SearchText response=%+v want just-written text-indexed document without explicit Flush", got)
	}
}

func TestSearchTextTombstonedPostingsConsumeScanBudgetM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
		[]byte(`{"body":"refund"}`),
		[]byte(`{"body":"refund"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	publishTextPostingOverlayTombstone(t, d, "docs", "lexical", "refund", []byte("a"))

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1, MaxPostingsScanned: 1})
	if !errors.Is(err, ErrTextIndexUnavailable) {
		t.Fatalf("SearchText err=%v want ErrTextIndexUnavailable", err)
	}
	if got.Stats.TextPostingsScanned != 1 || !got.Stats.Truncated || got.Stats.FailClosedReason != textSearchFailClosedPostingsLimit {
		t.Fatalf("stats=%+v want tombstoned posting charged to scan budget", got.Stats)
	}
}

func TestSearchTextFailClosedWrapsStorageCorruptionM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	if _, err := col.Insert([]byte("d1"), []byte(`{"body":"refund policy"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	corruptTextRootValue(t, d, "docs", collectionTextStatsRootName("docs", "lexical"), encodeTextStatsCorpusKey(), []byte{99})

	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 1})
	if !errors.Is(err, ErrTextIndexUnavailable) || !errors.Is(err, ErrTextIndexStorageCorrupt) {
		t.Fatalf("SearchText err=%v want ErrTextIndexUnavailable and ErrTextIndexStorageCorrupt", err)
	}
	if got.Stats.FailClosed != 1 || got.Stats.FailClosedReason != textSearchFailClosedStorageCorrupt || !got.Stats.Unavailable {
		t.Fatalf("stats=%+v want storage-corrupt fail-closed", got.Stats)
	}
}

func TestSearchTextTopKBoundsDocumentFetchM4(t *testing.T) {
	d := openTextTestDB(t)
	defer func() { _ = d.Close() }()
	col := createTextSearchM4Collection(t, d, []TextIndexField{{Field: "body"}})
	ids := make([][]byte, 5)
	docs := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		ids[i] = []byte{byte('a' + i)}
		docs[i] = []byte(`{"body":"refund policy"}`)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	got, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 2, IncludeDocuments: true})
	if err != nil {
		t.Fatalf("SearchText: %v", err)
	}
	if len(got.Results) != 2 || got.Stats.TextCandidatesScored != 5 || got.Stats.TextCandidatesReturned != 2 || got.Stats.DocumentsFetched != 2 || got.Stats.DocumentsMissing != 0 {
		t.Fatalf("response=%+v want scored=5 returned/fetched=2", got)
	}
	for _, result := range got.Results {
		if len(result.Document) == 0 {
			t.Fatalf("result %+v missing fetched document", result)
		}
	}
}

func TestSearchTextReopenParityM4(t *testing.T) {
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
	before, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 2})
	if err != nil {
		t.Fatalf("SearchText before reopen: %v", err)
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
	after, err := reopenedCol.SearchText(TextSearchOptions{IndexName: "lexical", Query: "refund", TopK: 2})
	if err != nil {
		t.Fatalf("SearchText after reopen: %v", err)
	}
	if len(before.Results) != len(after.Results) {
		t.Fatalf("reopen result length before=%d after=%d", len(before.Results), len(after.Results))
	}
	for i := range before.Results {
		if !bytes.Equal(before.Results[i].DocumentID, after.Results[i].DocumentID) || before.Results[i].Rank != after.Results[i].Rank || math.Abs(before.Results[i].Score-after.Results[i].Score) > 1e-12 {
			t.Fatalf("result[%d] before=%+v after=%+v", i, before.Results[i], after.Results[i])
		}
	}
	if before.Stats.TextPostingsScanned != after.Stats.TextPostingsScanned || before.Stats.TextCandidatesScored != after.Stats.TextCandidatesScored {
		t.Fatalf("stats before=%+v after=%+v", before.Stats, after.Stats)
	}
}

func BenchmarkSearchTextM4(b *testing.B) {
	const docCount = 1024
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
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}); err != nil {
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
	cases := []struct {
		name string
		opts TextSearchOptions
	}{
		{name: "candidate_generation_scoring", opts: TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorAND, TopK: 20, CandidateLimit: docCount}},
		{name: "end_to_end_with_documents", opts: TextSearchOptions{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorAND, TopK: 20, CandidateLimit: docCount, IncludeDocuments: true}},
		{name: "or_query_bounded_topk", opts: TextSearchOptions{IndexName: "lexical", Query: "HTTP_500 OR customer", TopK: 10, CandidateLimit: docCount}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			var last TextSearchResponse
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := col.SearchText(tc.opts)
				if err != nil {
					b.Fatalf("SearchText: %v", err)
				}
				if len(got.Results) == 0 {
					b.Fatal("SearchText returned no results")
				}
				last = got
			}
			b.ReportMetric(float64(last.Stats.TextPostingsScanned), "postings_scanned/search")
			b.ReportMetric(float64(last.Stats.TextCandidatesScored), "candidates_scored/search")
			b.ReportMetric(float64(last.Stats.DocumentsFetched), "docs_fetched/search")
			b.ReportMetric(float64(last.Stats.PostingsScanNanos), "postings_scan_ns/search")
			b.ReportMetric(float64(last.Stats.CandidateScoreNanos), "candidate_score_ns/search")
			b.ReportMetric(float64(last.Stats.DocumentFetchNanos), "document_fetch_ns/search")
		})
	}
}

func publishTextPostingOverlayTombstone(t *testing.T, d *backenddb.DB, collection, indexName, term string, documentID []byte) {
	t.Helper()
	rootName := collectionTextIndexRootName(collection, indexName)
	table := newCollectionRunTable(1)
	table.DeleteSteal(encodeTextPostingKey(term, documentID))
	table.Freeze()
	defer resetCollectionRunTable(table)
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{BaseRoot: 0, Iter: iter}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if len(rootIDs) != 1 {
			return nil, fmt.Errorf("unexpected overlay root ids %d", len(rootIDs))
		}
		snap := d.AcquireSnapshot()
		if snap == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = snap.Close() }()
		return buildSystemTargetIterator(snap, map[string][]byte{systemCollectionRootOverlayKey(rootName): encodeRootIDList([]uint64{rootIDs[0]})})
	})
	if err != nil {
		t.Fatalf("publish text posting overlay tombstone: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("overlay root ids=%d want 1", len(rootIDs))
	}
}

func createTextSearchM4Collection(t *testing.T, d *backenddb.DB, fields []TextIndexField) *Collection {
	t.Helper()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Version: TextIndexVersionV1, Fields: fields, StorePositions: true}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	return col
}

func slicesEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
