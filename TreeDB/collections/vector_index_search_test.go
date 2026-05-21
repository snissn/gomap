package collections

import (
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSearchVectorIndexColumnGraphNativeReaderReopenV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	query := []float32{0, 0.2, 1}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	if got.Stats.Candidates != uint64(len(rows)) || got.Stats.ResultFetches != 2 {
		t.Fatalf("stats=%+v want public search to expose native graph traversal accounting", got.Stats)
	}
	if got.Results[0].Document != nil {
		t.Fatalf("document materialized without IncludeDocuments: %q", got.Results[0].Document)
	}
}

func TestSearchVectorIndexColumnGraphMaterializesDocumentsAfterTopKV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	if len(got.Results[0].Document) == 0 || !strings.Contains(string(got.Results[0].Document), `"did":"doc-c"`) {
		t.Fatalf("top result document=%q want doc-c JSON materialized after top-k", got.Results[0].Document)
	}
	if cap(got.Results[0].Document) != len(got.Results[0].Document) {
		t.Fatalf("top result document cap=%d len=%d want owned tightly-sized response bytes", cap(got.Results[0].Document), len(got.Results[0].Document))
	}
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) {
		t.Fatalf("DocumentsFetched=%d want %d", got.Stats.DocumentsFetched, len(got.Results))
	}
}

func TestOpenVectorIndexSearcherFetchesDocumentsFromBoundSnapshotV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a after opening searcher: %v", err)
	}
	if live, err := col.Get([]byte("doc-a")); err != nil || live != nil {
		t.Fatalf("live Get doc-a after delete=%q err=%v want missing", live, err)
	}

	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("Search after delete on bound searcher: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("top result id=%q want doc-a from bound graph snapshot", got.Results[0].ID)
	}
	if !strings.Contains(string(got.Results[0].Document), `"did":"doc-a"`) {
		t.Fatalf("bound snapshot document=%q want pre-delete doc-a", got.Results[0].Document)
	}
	if got.Stats.DocumentsFetched != 1 {
		t.Fatalf("DocumentsFetched=%d want 1", got.Stats.DocumentsFetched)
	}
}

func TestOpenVectorIndexSearcherReusesNativeReaderV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{0, 0, 1}, TopK: 2, EfSearch: len(rows)}
	first, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("first Search: %v", err)
	}
	second, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("second Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, first, def.Name, 2)
	assertColumnGraphSearchResponseLoadedV4(t, second, def.Name, 2)
	if len(first.Results) != len(second.Results) {
		t.Fatalf("second results=%d want %d", len(second.Results), len(first.Results))
	}
	for i := range first.Results {
		if string(first.Results[i].ID) != string(second.Results[i].ID) || first.Results[i].Ordinal != second.Results[i].Ordinal || first.Results[i].Score != second.Results[i].Score {
			t.Fatalf("second result[%d]=%+v want %+v", i, second.Results[i], first.Results[i])
		}
	}
	if first.Stats.OpenGranulesRead == 0 || first.Stats.OpenPhysicalBytesRead == 0 {
		t.Fatalf("first open stats granules=%d physical_bytes=%d want non-zero bound-reader setup telemetry", first.Stats.OpenGranulesRead, first.Stats.OpenPhysicalBytesRead)
	}
	if second.Stats.OpenGranulesRead != first.Stats.OpenGranulesRead || second.Stats.OpenPhysicalBytesRead != first.Stats.OpenPhysicalBytesRead {
		t.Fatalf("open stats changed first=(%d,%d) second=(%d,%d); want stable bound-reader setup telemetry", first.Stats.OpenGranulesRead, first.Stats.OpenPhysicalBytesRead, second.Stats.OpenGranulesRead, second.Stats.OpenPhysicalBytesRead)
	}
	if first.Stats.RowFetches == 0 || second.Stats.RowFetches != first.Stats.RowFetches {
		t.Fatalf("row fetch stats first=%d second=%d want per-search deltas", first.Stats.RowFetches, second.Stats.RowFetches)
	}
	if second.Stats.PhysicalBytesRead != 0 {
		t.Fatalf("second PhysicalBytesRead=%d want cached per-search reader delta", second.Stats.PhysicalBytesRead)
	}
}

func TestSearchVectorIndexColumnGraphUnavailableStatusV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want rebuild-needed failure")
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded || got.Status.Loaded {
		t.Fatalf("status=%+v want rebuild-needed fail-closed status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unavailable index", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexColumnGraphStaleAfterMutationV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-c", vector: []float32{0, 0, 1}},
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{0, 0, 1},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want stale/rebuild-needed failure")
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded {
		t.Fatalf("status=%+v want stale graph to require rebuild", got.Status)
	}
}

func TestSearchVectorIndexNativeRuntimeDoesNotFallbackToColumnGraphV4(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_native",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyNativeRuntime,
		}},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: "embedding_native",
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want explicit native-runtime unsupported status")
	}
	if got.Status.State != VectorIndexStateNativeRuntime || got.Status.Reason != VectorIndexReasonNativeRuntime {
		t.Fatalf("status=%+v want native runtime status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no column graph fallback", got.Path, len(got.Results))
	}
}

func assertColumnGraphSearchResponseLoadedV4(tb testing.TB, got VectorIndexSearchResponse, name string, wantResults int) {
	tb.Helper()
	if got.IndexName != name || got.Strategy != VectorIndexStrategyColumnGraph {
		tb.Fatalf("response index=%q strategy=%q want %q/%q", got.IndexName, got.Strategy, name, VectorIndexStrategyColumnGraph)
	}
	if got.Path != VectorIndexSearchPathColumnGraphNativeReader {
		tb.Fatalf("path=%q want %q", got.Path, VectorIndexSearchPathColumnGraphNativeReader)
	}
	if got.Status.State != VectorIndexStateColumnGraphLoaded || !got.Status.Loaded || got.Status.RebuildNeeded {
		tb.Fatalf("status=%+v want loaded column graph", got.Status)
	}
	if len(got.Results) != wantResults {
		tb.Fatalf("results=%d want %d", len(got.Results), wantResults)
	}
}

func assertVectorIndexSearchResultsV4(tb testing.TB, got []VectorIndexSearchResult, want []columnVectorGraphNativeSearchResult, wantDocs bool) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("results=%d want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i].ID) != string(want[i].ID) || got[i].Ordinal != want[i].Ordinal || math.Abs(got[i].Score-want[i].Score) > 1e-12 {
			tb.Fatalf("result[%d]=%+v want id=%q ordinal=%d score=%v", i, got[i], want[i].ID, want[i].Ordinal, want[i].Score)
		}
		if wantDocs && len(got[i].Document) == 0 {
			tb.Fatalf("result[%d] missing materialized document", i)
		}
	}
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		MaxDecodedBlocks: 1,
	}
	if _, err := col.SearchVectorIndex(opts); err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	if _, err := col.SearchVectorIndex(opts); err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	var stats VectorIndexSearchStats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
		stats.Candidates += got.Stats.Candidates
		stats.Edges += got.Stats.Edges
		stats.CandidateFetches += got.Stats.CandidateFetches
		stats.ExpansionFetches += got.Stats.ExpansionFetches
		stats.ResultFetches += got.Stats.ResultFetches
		stats.DocumentsFetched += got.Stats.DocumentsFetched
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats)
}

func reportVectorIndexSearchBenchMetricsV4(b *testing.B, n int, stats VectorIndexSearchStats) {
	b.Helper()
	if n <= 0 {
		return
	}
	b.ReportMetric(float64(stats.Candidates)/float64(n), "candidates/search")
	b.ReportMetric(float64(stats.Edges)/float64(n), "edges/search")
	if stats.Candidates > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(stats.CandidateFetches)/float64(n), "candidate_fetches/search")
	b.ReportMetric(float64(stats.ExpansionFetches)/float64(n), "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches)/float64(n), "result_fetches/search")
	b.ReportMetric(float64(stats.DocumentsFetched)/float64(n), "docs_fetched/search")
}

var vectorSearchBenchSinkOrdinalV4 int
