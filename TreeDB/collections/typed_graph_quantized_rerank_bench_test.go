package collections

import (
	"context"
	"fmt"
	"os"
	"testing"
)

const typedGraphQuantizedRerankBenchmark768Env = "TREEDB_MINIMA_Q2_BENCH_768"

// BenchmarkTypedGraphQuantizedRerankReadView measures the public selected
// Minima route after serving admission, the captured-base keeper, and the
// named legacy scalar_u8 v1 code plane have all been warmed. The default is a
// small 8D smoke shape; set TREEDB_MINIMA_Q2_BENCH_768=1 to add the realistic
// 768D shape without making ordinary benchmark runs pay its setup cost.
//
// Every search uses the public Collection read-view API. The caller-owned
// buffer is reset only after the returned view is closed, because result IDs
// alias that buffer and the read view owns the coherent base/suffix lifetime.
func BenchmarkTypedGraphQuantizedRerankReadView(b *testing.B) {
	requireTypedGraphPublicServingTest(b)
	cases := []struct {
		name string
		dims int
		rows int
	}{
		{name: "smoke-8d", dims: 8, rows: 128},
	}
	if os.Getenv(typedGraphQuantizedRerankBenchmark768Env) != "" {
		cases = append(cases, struct {
			name string
			dims int
			rows int
		}{name: "opt-in-768d", dims: 768, rows: 128})
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkTypedGraphQuantizedRerankReadView(b, tc.dims, tc.rows)
		})
	}
}

func benchmarkTypedGraphQuantizedRerankReadView(b *testing.B, dims, rows int) {
	b.Helper()
	const (
		topK             = 10
		efSearch         = 64
		rerankCandidates = 32
		quantizedName    = "embedding.scalar_u8.legacy"
	)
	if dims <= 0 || rows < topK {
		b.Fatalf("invalid Q2 benchmark shape dims=%d rows=%d", dims, rows)
	}

	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].Dimensions = dims
	meta.VectorIndexes[0].M = 16
	meta.VectorIndexes[0].EfSearch = efSearch
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: quantizedName}}
	meta.Options.ColumnStore.Columns[0].VectorDims = dims
	_, db, col := openTypedMinimaCollectionMeta(b, meta)
	defer func() { _ = db.Close() }()

	ids := make([][]byte, rows)
	retained := make([][]byte, rows)
	vectors := make([][]float32, rows)
	contents := make([]string, rows)
	users := make([]string, rows)
	paths := make([]string, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, string(ids[i])))
		vectors[i] = vectorBenchmarkEmbedding(i, dims)
		contents[i], users[i], paths[i] = "content", "u", "source"
	}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: vectors},
		{Name: "content", Strings: contents},
		{Name: "user", Strings: users},
		{Name: "path", Strings: paths},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		b.Fatalf("InsertTypedBatchWithStats: %v", err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
		b.Fatalf("EnsureColumnGraphServing: %v", err)
	}

	opts := VectorIndexSearchOptions{
		IndexName:                 "embedding_graph",
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        quantizedName,
		QuantizedRerankCandidates: rerankCandidates,
		TopK:                      topK,
		EfSearch:                  efSearch,
		StatsMode:                 VectorIndexSearchStatsModeProduction,
	}
	var buffer VectorIndexSearchBuffer
	searchAndClose := func(query []float32) (VectorIndexSearchResponse, error) {
		opts.Query = query
		response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
		if err != nil {
			if view != nil {
				_ = view.Close()
			}
			return response, err
		}
		if view == nil {
			return response, fmt.Errorf("selected Q2 search returned no read view")
		}
		if err := view.Close(); err != nil {
			return response, fmt.Errorf("close selected Q2 read view: %w", err)
		}
		return response, nil
	}
	assertSmoke := func(stage string, response VectorIndexSearchResponse, queryOrdinal int) {
		if len(response.Results) != topK || string(response.Results[0].ID) != string(ids[queryOrdinal]) {
			b.Fatalf("%s results=%d top1=%q want %q", stage, len(response.Results), response.Results[0].ID, ids[queryOrdinal])
		}
		stats := response.Stats
		proof := stats.ColumnGraphWork.ScorePlane
		if stats.SearchRouteQuantizedRerank != 1 || stats.SearchRouteQuantizedOnly != 0 || stats.QuantizedScorerActive != 1 || stats.QuantizedScoreCalls == 0 || stats.QuantizedRerankCandidates == 0 || stats.QuantizedRerankExactScoreCalls != stats.QuantizedRerankCandidates || !proof.Completed || proof.Route != "quantized_rerank" || proof.QuantizedIndexName != quantizedName {
			b.Fatalf("%s selected Q2 stats=%+v proof=%+v", stage, stats, proof)
		}
	}

	// This first request attaches the immutable scalar code plane to the shared
	// holder. Keep it and the sample request outside the timed steady state.
	warm, err := searchAndClose(vectors[0])
	if err != nil {
		b.Fatalf("warm SearchVectorIndexWithBufferReadView: %v", err)
	}
	assertSmoke("warm", warm, 0)
	buffer.Reset()
	sample, err := searchAndClose(vectors[1])
	if err != nil {
		b.Fatalf("sample SearchVectorIndexWithBufferReadView: %v", err)
	}
	assertSmoke("sample", sample, 1)
	buffer.Reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ordinal := i % len(vectors)
		response, err := searchAndClose(vectors[ordinal])
		if err != nil {
			b.Fatalf("iteration %d SearchVectorIndexWithBufferReadView: %v", i, err)
		}
		if len(response.Results) != topK {
			b.Fatalf("iteration %d results=%d want %d", i, len(response.Results), topK)
		}
		vectorSearchBenchSinkOrdinalV4 += response.Results[0].Ordinal
		buffer.Reset()
	}
	b.StopTimer()

	shared := col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, shared, 1)
	reportVectorIndexSearchStatsModeBenchMetric2126(b, opts.StatsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, sample.Stats, false)
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(rows), "graph_rows")
	b.ReportMetric(float64(topK), "top_k")
	b.ReportMetric(float64(efSearch), "ef_search")
	b.ReportMetric(float64(rerankCandidates), "rerank_candidates")
}
