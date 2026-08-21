package collections

import (
	"bytes"
	"fmt"
	"testing"
)

func TestColumnHNSWPreparedScoreTileCapacityHint4227(t *testing.T) {
	const (
		rowCount = 1000
		degree   = 32
		rerank   = 200
	)
	var rerankScratch columnVectorGraphNativeSearchScratch
	if err := rerankScratch.prepareHNSWSearchPack(rowCount, 768, degree, 100, 600, rerank, degree, degree); err != nil {
		t.Fatalf("prepare rerank scratch: %v", err)
	}
	for name, capacity := range map[string]int{
		"scores":  cap(rerankScratch.scoreTileScores),
		"row IDs": cap(rerankScratch.scoreTileRowIDs),
		"dots":    cap(rerankScratch.scoreTileDots),
	} {
		if capacity < rerank {
			t.Fatalf("rerank %s capacity=%d want >=%d", name, capacity, rerank)
		}
	}

	var quantizedOnlyScratch columnVectorGraphNativeSearchScratch
	quantizedOnlyScratch.scoreTileScores = make([]float64, 0, rerank)
	quantizedOnlyScratch.scoreTileRowIDs = make([]uint32, 0, rerank)
	quantizedOnlyScratch.scoreTileDots = make([]float32, 0, rerank)
	if err := quantizedOnlyScratch.prepareHNSWSearchPack(rowCount, 768, degree, 100, 600, 0, degree, degree); err != nil {
		t.Fatalf("prepare quantized-only scratch: %v", err)
	}
	for name, capacity := range map[string]int{
		"scores":  cap(quantizedOnlyScratch.scoreTileScores),
		"row IDs": cap(quantizedOnlyScratch.scoreTileRowIDs),
		"dots":    cap(quantizedOnlyScratch.scoreTileDots),
	} {
		if capacity != degree {
			t.Fatalf("quantized-only %s capacity=%d want unchanged degree=%d sizing", name, capacity, degree)
		}
	}
	if err := rerankScratch.prepareHNSWSearchPack(rowCount, 768, degree, 100, 600, -1, degree, degree); err == nil {
		t.Fatal("negative score-tile capacity succeeded")
	}
}

func TestColumnHNSWPreparedScalarU8RerankScratchReuse4227(t *testing.T) {
	reader, pack, query, quantizedIndexName := columnHNSWPreparedScalarU8RerankFixture4227(t)
	opts := columnVectorGraphNativeSearchOptions{
		TopK:                      100,
		EfSearch:                  256,
		QueryMode:                 columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		QuantizedIndexName:        quantizedIndexName,
		QuantizedRerankCandidates: 200,
		StatsMode:                 columnVectorGraphNativeSearchStatsModeMinimal,
	}
	var scratch columnVectorGraphNativeSearchScratch
	wantResults, wantStats, err := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch)
	if err != nil {
		t.Fatalf("warm prepared rerank: %v", err)
	}
	if len(wantResults) != opts.TopK || wantStats.QuantizedRerankCandidates != uint64(opts.QuantizedRerankCandidates) || wantStats.QuantizedRerankExactScoreCalls != uint64(opts.QuantizedRerankCandidates) {
		t.Fatalf("warm results=%d stats=%+v want topK=%d rerank=%d", len(wantResults), wantStats, opts.TopK, opts.QuantizedRerankCandidates)
	}
	want := append([]columnVectorGraphNativeSearchResult(nil), wantResults...)
	for i := range want {
		want[i].ID = append([]byte(nil), want[i].ID...)
	}

	allocs := testing.AllocsPerRun(100, func() {
		got, stats, searchErr := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch)
		if searchErr != nil {
			panic(searchErr)
		}
		if len(got) != len(want) || stats.QuantizedScoreCalls != wantStats.QuantizedScoreCalls || stats.QuantizedRerankExactScoreCalls != wantStats.QuantizedRerankExactScoreCalls || stats.VisitedEdges != wantStats.VisitedEdges {
			panic("prepared rerank result/work counters changed")
		}
		for i := range want {
			if !bytes.Equal(got[i].ID, want[i].ID) || got[i].Ordinal != want[i].Ordinal || got[i].Score != want[i].Score {
				panic("prepared rerank IDs/scores/ties changed")
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("steady-state prepared rerank allocs/run=%v want 0", allocs)
	}
	for name, capacity := range map[string]int{
		"scores":  cap(scratch.scoreTileScores),
		"row IDs": cap(scratch.scoreTileRowIDs),
		"dots":    cap(scratch.scoreTileDots),
	} {
		if capacity < opts.QuantizedRerankCandidates {
			t.Fatalf("reused %s capacity=%d want >=%d", name, capacity, opts.QuantizedRerankCandidates)
		}
	}

	opts.QuantizedRerankCandidates = 0
	if _, stats, err := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch); err != nil {
		t.Fatalf("default rerank limit: %v", err)
	} else if stats.QuantizedRerankCandidates != uint64(opts.EfSearch) {
		t.Fatalf("default rerank candidates=%d want efSearch=%d", stats.QuantizedRerankCandidates, opts.EfSearch)
	}
	if cap(scratch.scoreTileScores) < opts.EfSearch || cap(scratch.scoreTileRowIDs) < opts.EfSearch || cap(scratch.scoreTileDots) < opts.EfSearch {
		t.Fatalf("default rerank capacities scores/rows/dots=%d/%d/%d want >=%d", cap(scratch.scoreTileScores), cap(scratch.scoreTileRowIDs), cap(scratch.scoreTileDots), opts.EfSearch)
	}

	opts.QuantizedRerankCandidates = 1000
	if _, stats, err := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch); err != nil {
		t.Fatalf("bounded rerank limit: %v", err)
	} else if stats.QuantizedRerankCandidates != uint64(pack.Header.Rows) {
		t.Fatalf("bounded rerank candidates=%d want rows=%d", stats.QuantizedRerankCandidates, pack.Header.Rows)
	}
}

func TestColumnHNSWPreparedScalarU8RawDotRerankScratchReuse4227(t *testing.T) {
	reader, pack, query, quantizedIndexName := columnHNSWPreparedScalarU8RerankFixture4227(t)
	const (
		topK   = 100
		ef     = 256
		rerank = 200
	)
	var scratch columnVectorGraphNativeSearchScratch
	queryInvNorm, err := columnVectorGraphInvNorm(query)
	if err != nil {
		t.Fatalf("query norm: %v", err)
	}
	scorer, err := reader.prepareScalarU8QuantizedScorer(columnVectorGraphNativeSearchQueryModeQuantizedRerank, quantizedIndexName, query, queryInvNorm, &scratch)
	if err != nil {
		t.Fatalf("prepare scalar-u8 scorer: %v", err)
	}
	scratch.preparedScalarU8Plane.scorer = scorer
	scratch.preparedScalarU8Plane.ready = true
	opts := columnHNSWPreparedTraversalOptions{
		TopK:                                 topK,
		EfSearch:                             ef,
		ScoreTileCapacity:                    rerank,
		StatsMode:                            columnVectorGraphNativeSearchStatsModeFullDiagnostics,
		OmitResultMaterialization:            true,
		SuppressOmittedResultMaterialization: true,
	}
	wantResults, wantStats, err := runColumnHNSWPreparedScalarU8RawDotRerank4227(pack, query, opts, rerank, &scratch)
	if err != nil {
		t.Fatalf("warm raw-dot rerank: %v", err)
	}
	want := append([]columnVectorGraphNativeSearchResult(nil), wantResults...)
	for i := range want {
		want[i].ID = append([]byte(nil), want[i].ID...)
	}

	allocs := testing.AllocsPerRun(100, func() {
		got, stats, searchErr := runColumnHNSWPreparedScalarU8RawDotRerank4227(pack, query, opts, rerank, &scratch)
		if searchErr != nil {
			panic(searchErr)
		}
		if len(got) != len(want) || stats.QuantizedScoreCalls != wantStats.QuantizedScoreCalls || stats.QuantizedRerankExactScoreCalls != wantStats.QuantizedRerankExactScoreCalls || stats.VisitedEdges != wantStats.VisitedEdges {
			panic("raw-dot rerank result/work counters changed")
		}
		for i := range want {
			if !bytes.Equal(got[i].ID, want[i].ID) || got[i].Ordinal != want[i].Ordinal || got[i].Score != want[i].Score {
				panic("raw-dot rerank IDs/scores/ties changed")
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("steady-state raw-dot rerank allocs/run=%v want 0", allocs)
	}
	if cap(scratch.scoreTileScores) < rerank || cap(scratch.scoreTileRowIDs) < rerank || cap(scratch.scoreTileDots) < rerank {
		t.Fatalf("raw-dot rerank capacities scores/rows/dots=%d/%d/%d want >=%d", cap(scratch.scoreTileScores), cap(scratch.scoreTileRowIDs), cap(scratch.scoreTileDots), rerank)
	}
}

func runColumnHNSWPreparedScalarU8RawDotRerank4227(pack *columnHNSWSearchPackPreparedView, query []float32, opts columnHNSWPreparedTraversalOptions, rerank int, scratch *columnVectorGraphNativeSearchScratch) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats, error) {
	_, stats, err := pack.searchCosinePreparedRawDotTraversal(query, opts, scratch, &scratch.preparedScalarU8Plane)
	if err != nil {
		return nil, stats, err
	}
	scratch.results = scratch.results[:0]
	if err := pack.exactRerankPreparedTraversalRowIDCandidates(query, opts.TopK, rerank, columnVectorGraphScoreBatchModeDefault, scratch, &stats); err != nil {
		return nil, stats, err
	}
	if err := pack.fetchTopSearchResults(scratch, &stats); err != nil {
		return nil, stats, err
	}
	return scratch.results, stats, nil
}

func BenchmarkColumnHNSWPreparedScalarU8RerankScratchReuse4227(b *testing.B) {
	reader, pack, query, quantizedIndexName := columnHNSWPreparedScalarU8RerankFixture4227(b)
	opts := columnVectorGraphNativeSearchOptions{
		TopK:                      100,
		EfSearch:                  256,
		QueryMode:                 columnVectorGraphNativeSearchQueryModeQuantizedRerank,
		QuantizedIndexName:        quantizedIndexName,
		QuantizedRerankCandidates: 200,
		StatsMode:                 columnVectorGraphNativeSearchStatsModeMinimal,
	}
	var scratch columnVectorGraphNativeSearchScratch
	if _, _, err := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch); err != nil {
		b.Fatalf("warm prepared rerank: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := reader.SearchCosineScalarU8PreparedTraversal(pack, query, opts, &scratch); err != nil {
			b.Fatal(err)
		}
	}
}

func columnHNSWPreparedScalarU8RerankFixture4227(tb testing.TB) (*columnVectorGraphPhysicalRowReader, *columnHNSWSearchPackPreparedView, []float32, string) {
	tb.Helper()
	const (
		rows = 256
		dims = 64
	)
	input := make([]columnGraphRebuildInputRowV2A, rows)
	for i := range input {
		vector := make([]float32, dims)
		vector[0] = 1
		vector[1] = float32(i%32) / 1000
		input[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%03d", i), vector: vector}
	}
	_, db, collection, def := openColumnGraphQuantizedGuardrailTestCollection1926(tb, input)
	tb.Cleanup(func() { _ = db.Close() })
	if _, err := collection.RebuildVectorIndex(def.Name); err != nil {
		tb.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := collection.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		tb.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	tb.Cleanup(func() { _ = searcher.Close() })
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil {
		tb.Fatal("searcher missing prepared hnsw pack")
	}
	query := make([]float32, dims)
	query[0] = 1
	query[1] = 0.01
	return searcher.reader, searcher.reader.hnswSearchPack, query, def.QuantizedIndexes[0].Name
}
