package collections

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
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
			benchmarkTypedGraphVectorReadView(b, tc.dims, tc.rows, "", VectorIndexQueryModeQuantizedRerank)
		})
	}
}

// BenchmarkCosineNormalizedF32V1PublicCollectionGate compares the two public
// collection arms on one deterministic owner. The opt-in 10K x 768D case is
// the Q2 bounded performance seam; the fresh 500K qualification remains owned
// by Q4.
func BenchmarkCosineNormalizedF32V1PublicCollectionGate(b *testing.B) {
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
		}{name: "opt-in-10k-768d", dims: 768, rows: 10_000})
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			fixture := openTypedGraphVectorReadViewBenchFixture(b, tc.dims, tc.rows, VectorIndexRepresentationCosineNormalizedF32V1)
			defer fixture.close()
			for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedRerank} {
				mode := mode
				b.Run(string(mode), func(b *testing.B) {
					fixture.benchmark(b, mode)
				})
			}
		})
	}
}

// BenchmarkTypedGraphHybridPublicRoutes4767 measures the production hybrid
// executor over one admitted unfiltered base so the selected arm cannot take
// the deliberate small-filter exact shortcut.
func BenchmarkTypedGraphHybridPublicRoutes4767(b *testing.B) {
	requireTypedGraphPublicServingTest(b)
	fixture := openTypedGraphVectorReadViewBenchFixture(b, 64, 1024, VectorIndexRepresentationCosineNormalizedF32V1)
	defer fixture.close()
	for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedRerank} {
		mode := mode
		b.Run(string(mode), func(b *testing.B) {
			request := HybridSearchOptions{
				TopK: 10,
				Text: &HybridTextQuery{IndexName: "content", Query: "content", CandidateLimit: 64},
				Vector: &HybridVectorQuery{
					IndexName: "embedding_graph", Query: fixture.vectors[0], CandidateLimit: 64,
					EfSearch: typedGraphVectorReadViewBenchEfSearch, QueryMode: mode,
				},
				IncludeDocuments:     true,
				DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
			}
			if mode == VectorIndexQueryModeQuantizedRerank {
				request.Vector.QuantizedIndexName = typedGraphVectorReadViewBenchQuantizedName
				request.Vector.QuantizedRerankCandidates = fixture.rerankCandidates
			}
			search := func(query []float32) HybridSearchResponse {
				request.Vector.Query = query
				response, err := fixture.col.SearchHybrid(request)
				if err != nil {
					b.Fatal(err)
				}
				if len(response.Results) != request.TopK || response.Stats.VectorRoute == nil || response.Stats.VectorRoute.Route != "typed_hnsw" || response.Stats.DocumentsFetched != uint64(len(response.Results)) || response.Stats.EmbeddingOutputBytes != 0 || response.Stats.FullDocumentScanFallbacks != 0 {
					b.Fatalf("hybrid response=%+v", response)
				}
				if mode == VectorIndexQueryModeQuantizedRerank {
					if response.Stats.VectorRoute.QueryMode != mode || response.Stats.VectorQuantizedScoreCalls == 0 || response.Stats.VectorQuantizedRerankCandidates == 0 || response.Stats.VectorPackedExactScoreCalls == 0 || response.Stats.VectorPackedExactVectorBytesRead == 0 {
						b.Fatalf("selected hybrid stats=%+v", response.Stats)
					}
				} else if response.Stats.VectorQuantizedScoreCalls != 0 || response.Stats.VectorQuantizedRerankCandidates != 0 {
					b.Fatalf("exact hybrid used quantized work: %+v", response.Stats)
				}
				return response
			}
			_ = search(fixture.vectors[0])
			sample := search(fixture.vectors[1])
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response := search(fixture.vectors[i%len(fixture.vectors)])
				vectorSearchBenchSinkOrdinalV4 += int(response.Stats.CandidatesFused)
			}
			b.StopTimer()
			b.ReportMetric(float64(sample.Stats.VectorQuantizedScoreCalls), "quantized_scores/search")
			b.ReportMetric(float64(sample.Stats.VectorQuantizedRerankCandidates), "rerank_candidates/search")
			b.ReportMetric(float64(sample.Stats.VectorPackedExactScoreCalls), "packed_calls/search")
			b.ReportMetric(float64(sample.Stats.VectorPackedExactVectorBytesRead), "packed_bytes/search")
			b.ReportMetric(float64(sample.Stats.CandidatesFused), "candidates_fused/search")
			b.ReportMetric(float64(sample.Stats.DocumentsFetched), "docs_fetched/search")
			b.ReportMetric(float64(sample.Stats.EmbeddingOutputBytes), "embedding_output_bytes/search")
		})
	}
}

const (
	typedGraphVectorReadViewBenchTopK          = 10
	typedGraphVectorReadViewBenchEfSearch      = 64
	typedGraphVectorReadViewBenchQuantizedName = "embedding.scalar_u8.legacy"
)

type typedGraphVectorReadViewBenchFixture struct {
	close            func()
	col              *Collection
	ids              [][]byte
	vectors          [][]float32
	dims             int
	rows             int
	representation   VectorIndexRepresentation
	rerankCandidates int
}

func benchmarkTypedGraphVectorReadView(b *testing.B, dims, rows int, representation VectorIndexRepresentation, mode VectorIndexQueryMode) {
	b.Helper()
	fixture := openTypedGraphVectorReadViewBenchFixture(b, dims, rows, representation)
	defer fixture.close()
	fixture.benchmark(b, mode)
}

func openTypedGraphVectorReadViewBenchFixture(tb testing.TB, dims, rows int, representation VectorIndexRepresentation) *typedGraphVectorReadViewBenchFixture {
	tb.Helper()
	rerankCandidates := 32
	if representation == VectorIndexRepresentationCosineNormalizedF32V1 {
		rerankCandidates = 64
	}
	if dims <= 0 || rows < typedGraphVectorReadViewBenchTopK {
		tb.Fatalf("invalid Q2 benchmark shape dims=%d rows=%d", dims, rows)
	}

	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].Dimensions = dims
	meta.VectorIndexes[0].M = 16
	meta.VectorIndexes[0].EfSearch = typedGraphVectorReadViewBenchEfSearch
	meta.VectorIndexes[0].Representation = representation
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: typedGraphVectorReadViewBenchQuantizedName}}
	meta.Options.ColumnStore.Columns[0].VectorDims = dims
	_, db, col := openTypedMinimaCollectionMeta(tb, meta)
	serving := typedGraphPublicTestOptions()
	if rows > serving.Publication.Rows {
		serving.Publication = ColumnGraphPublicationLimits{
			Rows: rows * 2, Tombstones: rows * 2, ValueSlots: rows * 8,
			OwnedBytes: 128 << 20, EncodedOutputBytes: 64 << 20,
		}
		serving.FoldRows = rows * 2
		serving.SearchCandidates = rows + 4096
		serving.Owners.StateBytes = 512 << 20
		serving.Owners.AssetBytes = 512 << 20
		serving.Owners.Cold.AssetBytes = 512 << 20
		serving.Owners.Cold.DecodedTermBytes = 4 << 30
		serving.Owners.Physical.InventoryBytes = 256 << 20
		serving.Maintenance.NativeBytes = 512 << 20
		serving.Maintenance.ColumnBytes = 256 << 20
		serving.Maintenance.RetainedBytes = 512 << 20
	}
	if representation == VectorIndexRepresentationCosineNormalizedF32V1 {
		// Admit the empty representation first so the subsequent operational
		// rebuild follows the real typed-graph fold path at every fixture size.
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			tb.Fatalf("RebuildVectorIndex empty: %v", err)
		}
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
			tb.Fatalf("EnsureColumnGraphServing empty: %v", err)
		}
	}

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
		tb.Fatalf("InsertTypedBatchWithStats: %v", err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
		tb.Fatalf("EnsureColumnGraphServing: %v", err)
	}
	return &typedGraphVectorReadViewBenchFixture{
		close:            func() { _ = db.Close() },
		col:              col,
		ids:              ids,
		vectors:          vectors,
		dims:             dims,
		rows:             rows,
		representation:   representation,
		rerankCandidates: rerankCandidates,
	}
}

func (fixture *typedGraphVectorReadViewBenchFixture) benchmark(b *testing.B, mode VectorIndexQueryMode) {
	b.Helper()
	opts := VectorIndexSearchOptions{
		IndexName: "embedding_graph", QueryMode: mode, TopK: typedGraphVectorReadViewBenchTopK,
		EfSearch: typedGraphVectorReadViewBenchEfSearch, StatsMode: VectorIndexSearchStatsModeProduction,
	}
	if mode == VectorIndexQueryModeQuantizedRerank {
		opts.QuantizedIndexName = typedGraphVectorReadViewBenchQuantizedName
		opts.QuantizedRerankCandidates = fixture.rerankCandidates
	}
	var buffer VectorIndexSearchBuffer
	searchAndClose := func(query []float32) (VectorIndexSearchResponse, error) {
		opts.Query = query
		response, view, err := fixture.col.SearchVectorIndexWithBufferReadView(opts, &buffer)
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
		if len(response.Results) != typedGraphVectorReadViewBenchTopK || string(response.Results[0].ID) != string(fixture.ids[queryOrdinal]) {
			b.Fatalf("%s results=%d top1=%q want %q", stage, len(response.Results), response.Results[0].ID, fixture.ids[queryOrdinal])
		}
		stats := response.Stats
		proof := stats.ColumnGraphWork.ScorePlane
		if mode == VectorIndexQueryModeQuantizedRerank {
			if stats.SearchRouteQuantizedRerank != 1 || stats.SearchRouteQuantizedOnly != 0 || stats.QuantizedScorerActive != 1 || stats.QuantizedScoreCalls == 0 || stats.QuantizedRerankCandidates == 0 || stats.QuantizedRerankExactScoreCalls != stats.QuantizedRerankCandidates || !proof.Completed || proof.Route != "quantized_rerank" || proof.QuantizedIndexName != typedGraphVectorReadViewBenchQuantizedName {
				b.Fatalf("%s selected Q2 stats=%+v proof=%+v", stage, stats, proof)
			}
		} else if stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScoreCalls != 0 {
			b.Fatalf("%s exact route used quantized work: stats=%+v", stage, stats)
		}
		if fixture.representation == VectorIndexRepresentationCosineNormalizedF32V1 {
			if stats.NormBytesRead != 0 || proof.ForbiddenStableScoreCalls != 0 {
				b.Fatalf("%s normalized route used norm/stable fallback: stats=%+v proof=%+v", stage, stats, proof)
			}
			if mode == VectorIndexQueryModeQuantizedRerank && (proof.PackedScoreBatchCalls != 1 || proof.PackedScoreCandidates != stats.QuantizedRerankCandidates || proof.PackedVectorBytesRead == 0) {
				b.Fatalf("%s normalized route omitted packed rerank proof: stats=%+v proof=%+v", stage, stats, proof)
			}
		}
	}

	// This first request attaches the immutable scalar code plane to the shared
	// holder. Keep it and the sample request outside the timed steady state.
	warm, err := searchAndClose(fixture.vectors[0])
	if err != nil {
		b.Fatalf("warm SearchVectorIndexWithBufferReadView: %v", err)
	}
	assertSmoke("warm", warm, 0)
	buffer.Reset()
	sample, err := searchAndClose(fixture.vectors[1])
	if err != nil {
		b.Fatalf("sample SearchVectorIndexWithBufferReadView: %v", err)
	}
	assertSmoke("sample", sample, 1)
	buffer.Reset()

	b.ReportAllocs()
	latencies := make([]time.Duration, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ordinal := i % len(fixture.vectors)
		started := time.Now()
		response, err := searchAndClose(fixture.vectors[ordinal])
		latencies[i] = time.Since(started)
		if err != nil {
			b.Fatalf("iteration %d SearchVectorIndexWithBufferReadView: %v", i, err)
		}
		if len(response.Results) != typedGraphVectorReadViewBenchTopK {
			b.Fatalf("iteration %d results=%d want %d", i, len(response.Results), typedGraphVectorReadViewBenchTopK)
		}
		vectorSearchBenchSinkOrdinalV4 += response.Results[0].Ordinal
		buffer.Reset()
	}
	b.StopTimer()
	slices.Sort(latencies)

	shared := fixture.col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, shared, 1)
	reportVectorIndexSearchStatsModeBenchMetric2126(b, opts.StatsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, sample.Stats, false)
	b.ReportMetric(float64(fixture.dims), "dims")
	b.ReportMetric(float64(fixture.rows), "graph_rows")
	b.ReportMetric(float64(typedGraphVectorReadViewBenchTopK), "top_k")
	b.ReportMetric(float64(typedGraphVectorReadViewBenchEfSearch), "ef_search")
	b.ReportMetric(float64(fixture.rerankCandidates), "rerank_candidates")
	b.ReportMetric(float64(latencies[(len(latencies)-1)/2].Nanoseconds()), "p50-ns/op")
	b.ReportMetric(float64(latencies[(len(latencies)-1)*95/100].Nanoseconds()), "p95-ns/op")
}

// BenchmarkCosineNormalizedF32V1PackedRerank64 isolates the production
// topology-pack -> exact score-plane -> indexed vectorops seam over a frozen
// 64-row shortlist at 768 dimensions.
func BenchmarkCosineNormalizedF32V1PackedRerank64(b *testing.B) {
	requireTypedGraphPublicServingTest(b)
	const (
		dims = 768
		rows = 128
		r    = 64
	)
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].Dimensions = dims
	meta.VectorIndexes[0].M = 16
	meta.VectorIndexes[0].Representation = VectorIndexRepresentationCosineNormalizedF32V1
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	meta.Options.ColumnStore.Columns[0].VectorDims = dims
	_, db, col := openTypedMinimaCollectionMeta(b, meta)
	defer func() { _ = db.Close() }()

	ids, retained, vectors := make([][]byte, rows), make([][]byte, rows), make([][]float32, rows)
	content, users, paths := make([]string, rows), make([]string, rows), make([]string, rows)
	for i := range rows {
		ids[i] = []byte(fmt.Sprintf("row-%05d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, string(ids[i])))
		vectors[i] = vectorBenchmarkEmbedding(i, dims)
		content[i], users[i], paths[i] = "content", "u", "source"
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: vectors}, {Name: "content", Strings: content},
		{Name: "user", Strings: users}, {Name: "path", Strings: paths},
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		b.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
		b.Fatal(err)
	}
	owner, err := col.openTypedGraphReadOwner(serving.Owners)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = owner.Close() }()
	pack := owner.overlay.pack
	query, err := normalizeCosineNormalizedF32V1(vectors[0], dims)
	if err != nil {
		b.Fatal(err)
	}
	var scratch columnVectorGraphNativeSearchScratch
	fill := func() {
		scratch.top = resizeColumnVectorGraphNativeCandidateScratch(scratch.top, r)[:0]
		for ordinal := range r {
			scratch.top = append(scratch.top, columnVectorGraphSearchCandidate{ordinal: ordinal, score: float64(r - ordinal)})
		}
	}
	var sample columnVectorGraphNativeSearchStats
	fill()
	if err := pack.exactRerankPreparedTraversalRowIDCandidatesWithQuery(query, true, false, 10, r, columnVectorGraphScoreBatchModeDefault, &scratch, &sample); err != nil {
		b.Fatal(err)
	}
	wantBytes := uint64(r * dims * 4)
	if sample.PackedExactScoreCalls != 1 || sample.PackedExactScoreCandidates != r || sample.PackedExactVectorBytesRead != wantBytes || sample.NormBytesRead != 0 {
		b.Fatalf("packed sample stats=%+v want candidates=%d bytes=%d", sample, r, wantBytes)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		fill()
		var stats columnVectorGraphNativeSearchStats
		if err := pack.exactRerankPreparedTraversalRowIDCandidatesWithQuery(query, true, false, 10, r, columnVectorGraphScoreBatchModeDefault, &scratch, &stats); err != nil {
			b.Fatal(err)
		}
		vectorSearchBenchSinkOrdinalV4 += scratch.top[0].ordinal
	}
	b.StopTimer()
	b.ReportMetric(float64(r), "rerank_candidates")
	b.ReportMetric(float64(dims), "dims")
}
