package collections

import "testing"

func BenchmarkVectorSearchReusableBufferSerialTypedColumnIndexedScoring1969(b *testing.B) {
	for _, tc := range []struct {
		name string
		mode columnVectorGraphScoreBatchMode
	}{
		{name: "scalar", mode: columnVectorGraphScoreBatchModeScalar},
		{name: "indexed", mode: columnVectorGraphScoreBatchModeIndexed},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkVectorSearchReusableBufferSerialTypedColumnIndexedScoring1969(b, tc.mode)
		})
	}
}

func benchmarkVectorSearchReusableBufferSerialTypedColumnIndexedScoring1969(b *testing.B, mode columnVectorGraphScoreBatchMode) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{Query: query, TopK: topK, EfSearch: efSearch, scoreBatchMode: mode}
	var buffer VectorIndexSearchBuffer
	if _, err := searcher.SearchWithBuffer(opts, &buffer); err != nil {
		b.Fatalf("warm SearchWithBuffer: %v", err)
	}
	measuredStats, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		b.Fatalf("measure SearchWithBuffer stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column indexed-scoring benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	if mode != columnVectorGraphScoreBatchModeDefault {
		if mode.indexedEnabled() {
			b.ReportMetric(1, "score_batch_mode_indexed")
		} else {
			b.ReportMetric(1, "score_batch_mode_scalar")
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.SearchWithBuffer(opts, &buffer)
		if err != nil {
			b.Fatalf("SearchWithBuffer: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}
