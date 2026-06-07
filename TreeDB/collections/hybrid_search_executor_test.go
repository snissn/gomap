package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type hybridSearchExecutorFixtureRow2505 struct {
	id     string
	title  string
	body   string
	city   string
	score  int64
	vector []float32
}

func TestSearchHybridExecutorTextVectorOverlapAndBoundedFetch2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-shared", title: "refund", body: "refund refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-text", title: "refund", body: "refund customer policy", city: "sea", score: 20, vector: []float32{0.2, 0.8, 0}},
		{id: "doc-vector", title: "shipping", body: "shipping update", city: "sea", score: 30, vector: []float32{0.99, 0.01, 0}},
		{id: "doc-filtered", title: "refund", body: "refund", city: "sfo", score: 40, vector: []float32{0.4, 0.6, 0}},
		{id: "doc-background", title: "other", body: "other", city: "sea", score: 50, vector: []float32{0, 0, 1}},
	})
	defer func() { _ = d.Close() }()

	opts := HybridSearchOptions{
		TopK:             3,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		Vector:           &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0, 0}, CandidateLimit: 4, EfSearch: 5, QueryMode: VectorIndexQueryModeExact},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
		Debug: HybridSearchDebugOptions{IncludeCandidates: true},
	}
	got, err := col.SearchHybrid(opts)
	if err != nil {
		t.Fatalf("SearchHybrid: %v", err)
	}
	if got.Plan.FinalTopK != opts.TopK || got.Plan.TextCandidateLimit != 4 || got.Plan.VectorCandidateLimit != 4 || got.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyUnionFusion || got.Plan.FusionMethod != HybridFusionMethodRRF {
		t.Fatalf("plan=%+v want bounded union-fusion RRF plan", got.Plan)
	}
	if got.Snapshot.Consistency != HybridConsistencyCurrentSnapshot || got.Snapshot.CommitSeq == 0 || got.Snapshot.SystemRootPageID == 0 {
		t.Fatalf("snapshot=%+v want current snapshot identity", got.Snapshot)
	}
	if len(got.Results) != 3 {
		t.Fatalf("results=%d want 3: %+v", len(got.Results), got.Results)
	}
	if gotID := string(got.Results[0].ID); gotID != "doc-shared" {
		t.Fatalf("top result=%q want overlapping doc-shared; results=%+v", gotID, got.Results)
	}
	if !hybridResultHasSource2505(got.Results[0], HybridCandidateSourceText) || !hybridResultHasSource2505(got.Results[0], HybridCandidateSourceVector) {
		t.Fatalf("top sources=%+v want text+vector overlap attribution", got.Results[0].Sources)
	}
	for _, result := range got.Results {
		if !result.DocumentFound || len(result.Document) == 0 || bytes.Contains(result.Document, []byte("embedding")) {
			t.Fatalf("result=%+v want bounded fetched document respecting projection", result)
		}
	}
	if len(got.Candidates) == 0 {
		t.Fatalf("debug candidates missing")
	}
	if got.Stats.TextCandidatesReturned == 0 || got.Stats.VectorCandidatesReturned == 0 || got.Stats.CandidatesFused != uint64(len(got.Candidates)) || got.Stats.FusionBoth == 0 {
		t.Fatalf("stats=%+v want text/vector/fusion counters", got.Stats)
	}
	if got.Stats.ScalarPrefilterIDs != 0 || got.Stats.ScalarFilterRejected != 0 || got.Stats.DocumentsFetched != uint64(len(got.Results)) || got.Stats.DocumentsFetched > uint64(opts.TopK) || got.Stats.FullDocumentScanFallbacks != 0 || got.Stats.FailClosed != 0 {
		t.Fatalf("stats=%+v want bounded final fetch only", got.Stats)
	}

	noDocsOpts := opts
	noDocsOpts.IncludeDocuments = false
	noDocsOpts.Debug.IncludeCandidates = false
	noDocs, err := col.SearchHybrid(noDocsOpts)
	if err != nil {
		t.Fatalf("SearchHybrid IncludeDocuments=false: %v", err)
	}
	if len(noDocs.Candidates) != 0 || noDocs.Stats.DocumentsFetched != 0 || noDocs.Stats.DocumentsMissing != 0 {
		t.Fatalf("no-doc response candidates=%d stats=%+v want no debug echo and zero document fetch", len(noDocs.Candidates), noDocs.Stats)
	}
	if gotIDs, noDocIDs := hybridResultIDs2505(got.Results), hybridResultIDs2505(noDocs.Results); !slicesEqualStrings(gotIDs, noDocIDs) {
		t.Fatalf("deterministic topK changed with IncludeDocuments=false: docs=%v no_docs=%v", gotIDs, noDocIDs)
	}
	for _, result := range noDocs.Results {
		if result.DocumentFound || len(result.Document) != 0 {
			t.Fatalf("IncludeDocuments=false result=%+v carried document", result)
		}
	}
}

func TestSearchHybridExecutorTextOnlyAndVectorOnly2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-b", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0, 1, 0}},
		{id: "doc-c", title: "other", body: "other", city: "sea", score: 30, vector: []float32{0, 0, 1}},
	})
	defer func() { _ = d.Close() }()

	textOnly, err := col.SearchHybrid(HybridSearchOptions{TopK: 2, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2}})
	if err != nil {
		t.Fatalf("SearchHybrid text-only: %v", err)
	}
	if len(textOnly.Results) != 2 || textOnly.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyTextFirst || textOnly.Stats.VectorCandidatesReturned != 0 || textOnly.Stats.FusionVectorOnly != 0 {
		t.Fatalf("text-only response=%+v stats=%+v", textOnly, textOnly.Stats)
	}
	for _, result := range textOnly.Results {
		if len(result.Sources) != 1 || result.Sources[0].Source != HybridCandidateSourceText {
			t.Fatalf("text-only result=%+v want text source only", result)
		}
	}

	vectorOnly, err := col.SearchHybrid(HybridSearchOptions{TopK: 2, Vector: &HybridVectorQuery{IndexName: def.Name, Query: []float32{0, 1, 0}, CandidateLimit: 2, EfSearch: 3}})
	if err != nil {
		t.Fatalf("SearchHybrid vector-only: %v", err)
	}
	if len(vectorOnly.Results) != 2 || vectorOnly.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyVectorFirst || vectorOnly.Stats.TextCandidatesReturned != 0 || vectorOnly.Stats.FusionTextOnly != 0 {
		t.Fatalf("vector-only response=%+v stats=%+v", vectorOnly, vectorOnly.Stats)
	}
	for _, result := range vectorOnly.Results {
		if len(result.Sources) != 1 || result.Sources[0].Source != HybridCandidateSourceVector {
			t.Fatalf("vector-only result=%+v want vector source only", result)
		}
	}
}

func TestSearchHybridScalarFilterRangeAndFailClosed2505(t *testing.T) {
	_, d, col := openHybridScalarSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-10", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "doc-20", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
		{id: "doc-30", title: "refund", body: "refund", city: "sea", score: 30, vector: []float32{0.8, 0.2, 0}},
		{id: "doc-40", title: "refund", body: "refund", city: "sea", score: 40, vector: []float32{0.7, 0.3, 0}},
	})
	defer func() { _ = d.Close() }()

	ranged, err := col.SearchHybrid(HybridSearchOptions{
		TopK: 2,
		Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilter: &HybridScalarFilter{IndexName: "score", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
			Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
		}},
	})
	if err != nil {
		t.Fatalf("SearchHybrid range scalar: %v", err)
	}
	if gotIDs := hybridResultIDs2505(ranged.Results); !slicesEqualStrings(gotIDs, []string{"doc-10", "doc-20"}) {
		t.Fatalf("range result ids=%v want doc-10/doc-20 response=%+v", gotIDs, ranged)
	}
	if ranged.Stats.ScalarPrefilterIDs != 2 || ranged.Stats.ScalarFilterRejected != 2 || ranged.Stats.FailClosed != 0 {
		t.Fatalf("range stats=%+v want bounded scalar include/exclude", ranged.Stats)
	}

	postfiltered, err := col.SearchHybrid(HybridSearchOptions{
		TopK:                 2,
		Text:                 &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 4},
		ScalarFilterStrategy: HybridScalarFilterStrategyPostfilter,
		ScalarFilter: &HybridScalarFilter{IndexName: "score", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: int64(10), Inclusive: true},
			Upper: IndexRangeBound{Value: int64(20), Inclusive: true},
		}},
	})
	if err != nil {
		t.Fatalf("SearchHybrid postfilter scalar: %v", err)
	}
	if gotIDs := hybridResultIDs2505(postfiltered.Results); !slicesEqualStrings(gotIDs, []string{"doc-10", "doc-20"}) || postfiltered.Plan.ScalarFilterStrategy != HybridScalarFilterStrategyPostfilter {
		t.Fatalf("postfilter ids=%v plan=%+v want doc-10/doc-20 postfilter", gotIDs, postfiltered.Plan)
	}
	if postfiltered.Stats.ScalarPrefilterIDs != 0 || postfiltered.Stats.ScalarPostfilterChecks != 4 || postfiltered.Stats.ScalarFilterMatched != 2 || postfiltered.Stats.ScalarFilterRejected != 2 {
		t.Fatalf("postfilter stats=%+v want bounded fused-result checks", postfiltered.Stats)
	}

	broad, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         1,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1},
		ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("broad scalar err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if broad.Stats.FailClosed != 1 || broad.Stats.FailClosedReason != HybridFailClosedReasonScalarFilterUnbounded || broad.Stats.Truncated == 0 || broad.Stats.TextCandidatesReturned != 0 {
		t.Fatalf("broad response=%+v want scalar unbounded before candidate generation", broad)
	}

	missing, err := col.SearchHybrid(HybridSearchOptions{
		TopK:         2,
		Text:         &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 2},
		ScalarFilter: &HybridScalarFilter{IndexName: "missing_city", Value: "sea"},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) {
		t.Fatalf("missing scalar err=%v want ErrHybridSearchIndexUnavailable", err)
	}
	if missing.Stats.FailClosed != 1 || missing.Stats.FailClosedReason != HybridFailClosedReasonScalarFilterUnbounded || missing.Stats.FullDocumentScanFallbacks != 0 {
		t.Fatalf("missing scalar response=%+v want fail-closed scalar reason and no fallback", missing)
	}
}

func TestSearchHybridMissingSourcesAndSnapshotModeFailClosed2505(t *testing.T) {
	_, d, col, def := openHybridSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "doc-a", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
	})
	defer func() { _ = d.Close() }()

	missingText, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Text: &HybridTextQuery{IndexName: "missing", Query: "refund", CandidateLimit: 1}})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || missingText.Stats.FailClosedReason != HybridFailClosedReasonTextIndexUnavailable {
		t.Fatalf("missing text response=%+v err=%v", missingText, err)
	}

	missingVector, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Vector: &HybridVectorQuery{IndexName: "missing", Query: []float32{1, 0, 0}, CandidateLimit: 1, EfSearch: 1}})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || missingVector.Stats.FailClosedReason != HybridFailClosedReasonVectorIndexUnavailable {
		t.Fatalf("missing vector response=%+v err=%v (valid def was %q)", missingVector, err, def.Name)
	}

	badFetch, err := col.SearchHybrid(HybridSearchOptions{
		TopK:             1,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			IncludePaths: []string{"body.nested"},
		},
	})
	if !errors.Is(err, ErrHybridSearchIndexUnavailable) || badFetch.Stats.FailClosedReason != HybridFailClosedReasonDocumentFetchUnavailable || badFetch.Stats.DocumentsFetched != 0 {
		t.Fatalf("bad fetch response=%+v err=%v want document-fetch unavailable without fetched docs", badFetch, err)
	}

	boundSnapshot, err := col.SearchHybrid(HybridSearchOptions{TopK: 1, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 1}, Consistency: HybridConsistencyOptions{Mode: HybridConsistencyBoundSnapshot}})
	if !errors.Is(err, ErrHybridSearchUnsupported) || boundSnapshot.Stats.FailClosedReason != HybridFailClosedReasonSnapshotMismatch {
		t.Fatalf("bound snapshot response=%+v err=%v want unsupported snapshot mismatch", boundSnapshot, err)
	}
}

func TestSearchHybridInsertUpdateDeleteReopenConsistency2505(t *testing.T) {
	dir, d, col := openHybridScalarSearchExecutorFixture2505(t, []hybridSearchExecutorFixtureRow2505{
		{id: "mutable", title: "refund", body: "refund", city: "sea", score: 10, vector: []float32{1, 0, 0}},
		{id: "deleted", title: "refund", body: "refund", city: "sea", score: 20, vector: []float32{0.9, 0.1, 0}},
		{id: "stable", title: "shipping", body: "shipping", city: "sea", score: 30, vector: []float32{0, 1, 0}},
	})
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	before, err := col.SearchHybrid(HybridSearchOptions{TopK: 10, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10}, ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"}})
	if err != nil {
		t.Fatalf("SearchHybrid before mutations: %v", err)
	}
	if gotIDs := hybridResultIDs2505(before.Results); !slicesEqualStrings(gotIDs, []string{"deleted", "mutable"}) {
		t.Fatalf("before ids=%v want deleted/mutable", gotIDs)
	}

	updatedDoc := mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "mutable", title: "shipping", body: "shipping", city: "sfo", score: 11, vector: []float32{1, 0, 0}}, 10)
	if _, changed, err := col.Update([]byte("mutable"), func(current []byte) ([]byte, bool, error) {
		if len(current) == 0 {
			return nil, false, fmt.Errorf("missing mutable")
		}
		return updatedDoc, true, nil
	}); err != nil || !changed {
		t.Fatalf("Update mutable changed=%v err=%v", changed, err)
	}
	if _, err := col.Insert([]byte("new"), mustHybridFixtureDocument2505(t, hybridSearchExecutorFixtureRow2505{id: "new", title: "refund", body: "refund", city: "sea", score: 12, vector: []float32{0.8, 0.2, 0}}, 11)); err != nil {
		t.Fatalf("Insert new: %v", err)
	}
	if err := col.Delete([]byte("deleted")); err != nil {
		t.Fatalf("Delete deleted: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush mutations: %v", err)
	}

	wantAfter := []string{"new"}
	if gotIDs := searchHybridTextScalarIDs2505(t, col); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after mutation ids=%v want %v", gotIDs, wantAfter)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	closed = true

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	if gotIDs := searchHybridTextScalarIDs2505(t, reopenedCol); !slicesEqualStrings(gotIDs, wantAfter) {
		t.Fatalf("after reopen ids=%v want %v", gotIDs, wantAfter)
	}
}

var hybridSearchExecutorBenchmarkSink2505 HybridSearchResponse

func BenchmarkSearchHybridExecutor2505(b *testing.B) {
	rows := make([]hybridSearchExecutorFixtureRow2505, 128)
	for i := range rows {
		city := "city-rare"
		if i%4 != 0 {
			city = "city-common"
		}
		rows[i] = hybridSearchExecutorFixtureRow2505{
			id:     fmt.Sprintf("doc-%03d", i),
			title:  "refund policy",
			body:   fmt.Sprintf("refund policy shard %d customer %d", i%8, i%17),
			city:   city,
			score:  int64(i),
			vector: []float32{1 + float32(i%11)*0.01, float32((i*7)%17) * 0.01, float32((i*13)%19) * 0.01},
		}
	}
	_, d, col, def := openHybridSearchExecutorFixture2505(b, rows)
	defer func() { _ = d.Close() }()
	opts := HybridSearchOptions{
		TopK:             10,
		Text:             &HybridTextQuery{IndexName: "lexical", Query: "refund policy", CandidateLimit: 32},
		Vector:           &HybridVectorQuery{IndexName: def.Name, Query: []float32{1, 0.1, 0.05}, CandidateLimit: 32, EfSearch: 64},
		IncludeDocuments: true,
		DocumentFetchOptions: DocumentFetchOptions{
			ExcludePaths: []string{"embedding"},
		},
	}
	warm, err := col.SearchHybrid(opts)
	if err != nil {
		b.Fatalf("warm SearchHybrid: %v", err)
	}
	if len(warm.Results) == 0 || warm.Stats.DocumentsFetched == 0 || warm.Stats.DocumentsFetched > uint64(opts.TopK) {
		b.Fatalf("warm response=%+v want bounded fetched hybrid results", warm)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink HybridSearchResponse
	for i := 0; i < b.N; i++ {
		got, err := col.SearchHybrid(opts)
		if err != nil {
			b.Fatalf("SearchHybrid: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatal("SearchHybrid returned no results")
		}
		sink = got
	}
	b.StopTimer()
	hybridSearchExecutorBenchmarkSink2505 = sink
	b.ReportMetric(float64(sink.Stats.TextCandidatesReturned), "text_candidates/search")
	b.ReportMetric(float64(sink.Stats.VectorCandidatesReturned), "vector_candidates/search")
	b.ReportMetric(float64(sink.Stats.CandidatesFused), "candidates_fused/search")
	b.ReportMetric(float64(sink.Stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(sink.Stats.FusionBoth), "fusion_both/search")
}

func openHybridSearchExecutorFixture2505(tb testing.TB, rows []hybridSearchExecutorFixtureRow2505) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 4)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(3),
		},
		VectorIndexes: []VectorIndexDefinition{def},
		TextIndexes:   []TextIndexDefinition{{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}},
	}
	col := createHybridFixtureCollection2505(tb, d, meta, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	return dir, d, col, def
}

func openHybridScalarSearchExecutorFixture2505(tb testing.TB, rows []hybridSearchExecutorFixtureRow2505) (string, *backenddb.DB, *Collection) {
	tb.Helper()
	dir := tb.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		tb.Fatalf("open db: %v", err)
	}
	meta := CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON},
	}
	col := createHybridFixtureCollection2505(tb, d, meta, rows)
	if _, err := col.CreateIndex(IndexDefinition{Name: "city", Field: "city", ValueType: IndexValueString}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateIndex city: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "score", Field: "score", ValueType: IndexValueInt64}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateIndex score: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{Name: "lexical", Fields: []TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}}, StorePositions: true}); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateTextIndex: %v", err)
	}
	return dir, d, col
}

func createHybridFixtureCollection2505(tb testing.TB, d *backenddb.DB, meta CollectionMeta, rows []hybridSearchExecutorFixtureRow2505) *Collection {
	tb.Helper()
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		ids[i] = []byte(row.id)
		docs[i] = mustHybridFixtureDocument2505(tb, row, i+1)
	}
	if len(rows) > 0 {
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = d.Close()
			tb.Fatalf("InsertBatch: %v", err)
		}
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		tb.Fatalf("Flush: %v", err)
	}
	return col
}

func mustHybridFixtureDocument2505(tb testing.TB, row hybridSearchExecutorFixtureRow2505, timeUS int) []byte {
	tb.Helper()
	raw, err := json.Marshal(map[string]any{
		"time_us":   int64(timeUS),
		"kind":      "hybrid",
		"did":       row.id,
		"embedding": row.vector,
		"title":     row.title,
		"body":      row.body,
		"city":      row.city,
		"score":     row.score,
	})
	if err != nil {
		tb.Fatalf("json.Marshal row %q: %v", row.id, err)
	}
	return raw
}

func hybridResultHasSource2505(result HybridSearchResult, source HybridCandidateSource) bool {
	for _, contribution := range result.Sources {
		if contribution.Source == source {
			return true
		}
	}
	return false
}

func hybridResultIDs2505(results []HybridSearchResult) []string {
	ids := make([]string, len(results))
	for i := range results {
		ids[i] = string(results[i].ID)
	}
	return ids
}

func searchHybridTextScalarIDs2505(tb testing.TB, col *Collection) []string {
	tb.Helper()
	got, err := col.SearchHybrid(HybridSearchOptions{TopK: 10, Text: &HybridTextQuery{IndexName: "lexical", Query: "refund", CandidateLimit: 10}, ScalarFilter: &HybridScalarFilter{IndexName: "city", Value: "sea"}})
	if err != nil {
		tb.Fatalf("SearchHybrid text+scalar: %v", err)
	}
	return hybridResultIDs2505(got.Results)
}
