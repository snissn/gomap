package collections

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
)

func TestColumnVectorGraphIndexedScoringSearchCosineParity1969(t *testing.T) {
	const dims = 128
	rows := columnVectorGraphNativeSearchBenchAssetRowsV3(t, 64, dims, 8)
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, dims, 8, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	attachColumnVectorGraphIndexedScoreSources1969(t, reader, rows)
	query := append([]float32(nil), rows[17].Vector...)

	scalarResults, scalarStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeScalar, 10, 32)
	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeIndexed, 10, 32)
	assertColumnVectorGraphIndexedScoringResultsEqual1969(t, indexedResults, scalarResults)
	if indexedStats.Candidates != scalarStats.Candidates {
		t.Fatalf("indexed candidates=%d scalar=%d indexedStats=%+v scalarStats=%+v", indexedStats.Candidates, scalarStats.Candidates, indexedStats, scalarStats)
	}
	if indexedStats.ScoreBatchCandidates != indexedStats.CandidateFetches || indexedStats.ScoreBatchCalls == 0 || indexedStats.ScoreBatchMaxTileSize < 2 {
		t.Fatalf("indexed stats=%+v want tiled score-batch candidates covering score fetches", indexedStats)
	}
	if indexedStats.VectorScratchDecodes != 0 || indexedStats.VectorMmapDirectViews+indexedStats.VectorHeapCopyTypedViews == 0 {
		t.Fatalf("indexed stats=%+v want direct typed vector source without scratch decodes", indexedStats)
	}
	assertColumnVectorGraphPreparedIndexedBackendCounters2125(t, indexedStats.ScoreBatchOptimizedCalls, indexedStats.ScoreBatchScalarFallbackCalls, int(indexedStats.ScoreBatchMaxTileSize), dims)
}

func TestColumnVectorGraphIndexedScoringUpperLayerGreedyParity1969(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 3, 2, 1, 2}},
		{ID: []byte("doc-upper-mid"), Vector: []float32{0.8, 0.2}, InvNorm: 1, Adjacency: []uint32{3}},
		{ID: []byte("doc-upper-best"), Vector: []float32{1, 0}, InvNorm: 1, Adjacency: []uint32{3}},
		{ID: []byte("doc-base"), Vector: []float32{0.1, 0.9}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 2, 4, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	attachColumnVectorGraphIndexedScoreSources1969(t, reader, rows)

	query := []float32{1, 0}
	scalarResults, scalarStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeScalar, 1, 1)
	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeIndexed, 1, 1)
	assertColumnVectorGraphIndexedScoringResultsEqual1969(t, indexedResults, scalarResults)
	if len(indexedResults) != 1 || indexedResults[0].Ordinal != 2 {
		t.Fatalf("indexed results=%+v want upper-layer greedy best ordinal 2", indexedResults)
	}
	if indexedStats.Candidates != scalarStats.Candidates || indexedStats.ScoreBatchMaxTileSize < 2 {
		t.Fatalf("indexedStats=%+v scalarStats=%+v want upper-layer tile scoring parity", indexedStats, scalarStats)
	}
}

func TestColumnVectorGraphIndexedScoringLayer0ExpansionParity1969(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{1, 2, 3, 4}},
		{ID: []byte("doc-a"), Vector: []float32{0.2, 0.8, 0}, InvNorm: 1},
		{ID: []byte("doc-b"), Vector: []float32{0.9, 0.1, 0}, InvNorm: 1},
		{ID: []byte("doc-c"), Vector: []float32{0.1, 0.1, 0.8}, InvNorm: 1},
		{ID: []byte("doc-d"), Vector: []float32{1, 0, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 3, 4, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	attachColumnVectorGraphIndexedScoreSources1969(t, reader, rows)

	query := []float32{1, 0, 0}
	scalarResults, scalarStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeScalar, 2, len(rows))
	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, query, columnVectorGraphScoreBatchModeIndexed, 2, len(rows))
	assertColumnVectorGraphIndexedScoringResultsEqual1969(t, indexedResults, scalarResults)
	if indexedStats.Candidates != scalarStats.Candidates || indexedStats.ScoreBatchMaxTileSize < 4 {
		t.Fatalf("indexedStats=%+v scalarStats=%+v want layer-0 expansion tile parity", indexedStats, scalarStats)
	}
}

func TestColumnVectorGraphIndexedScoringTieAndApplicationOrder1969(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{2, 1, 3}},
		{ID: []byte("doc-tie-low-ordinal"), Vector: []float32{0.5, 0}, InvNorm: 1},
		{ID: []byte("doc-tie-high-ordinal"), Vector: []float32{0.5, 0}, InvNorm: 1},
		{ID: []byte("doc-best-neighbor-scored-despite-ef-cap"), Vector: []float32{1, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 2, 3, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	attachColumnVectorGraphIndexedScoreSources1969(t, reader, rows)

	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, []float32{1, 0}, columnVectorGraphScoreBatchModeIndexed, 2, 3)
	if indexedStats.Candidates != 4 || indexedStats.ScoreBatchMaxTileSize != 3 {
		t.Fatalf("indexed stats=%+v want entry plus full expanded adjacency tile", indexedStats)
	}
	if len(indexedResults) != 2 || indexedResults[0].Ordinal != 3 || indexedResults[1].Ordinal != 1 {
		t.Fatalf("indexed results=%+v want best neighbor plus ordinal tie after full HNSW expansion", indexedResults)
	}
}

func TestColumnVectorGraphIndexedScoringEfOneExpandsEntry1969(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-best-neighbor"), Vector: []float32{1, 0}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 2, 1, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	attachColumnVectorGraphIndexedScoreSources1969(t, reader, rows)

	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, []float32{1, 0}, columnVectorGraphScoreBatchModeIndexed, 1, 1)
	if len(indexedResults) != 1 || indexedResults[0].Ordinal != 1 {
		t.Fatalf("indexed results=%+v want efSearch=1 to expand the entry and find best neighbor", indexedResults)
	}
	if indexedStats.Candidates != 2 {
		t.Fatalf("indexed stats=%+v want entry plus one adjacency candidate", indexedStats)
	}
}

func TestColumnVectorGraphIndexedScoringUnsupportedFallback1969(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-entry"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{1, 2, 3}},
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1},
		{ID: []byte("doc-b"), Vector: []float32{0.5, 0.5, 0}, InvNorm: 1},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1},
	}
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithShapeV2B(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("openColumnVectorGraphPhysicalRowReader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	scalarResults, _ := searchColumnVectorGraphIndexedScoring1969(t, reader, []float32{1, 0, 0}, columnVectorGraphScoreBatchModeScalar, 2, len(rows))
	indexedResults, indexedStats := searchColumnVectorGraphIndexedScoring1969(t, reader, []float32{1, 0, 0}, columnVectorGraphScoreBatchModeIndexed, 2, len(rows))
	assertColumnVectorGraphIndexedScoringResultsEqual1969(t, indexedResults, scalarResults)
	if indexedStats.ScoreBatchOptimizedCalls != 0 || indexedStats.ScoreBatchScalarFallbackCalls == 0 || indexedStats.VectorScratchDecodes == 0 {
		t.Fatalf("indexed fallback stats=%+v want unsupported layout to use scalar graph-row fallback", indexedStats)
	}
}

func TestColumnVectorGraphIndexedScoringParallelReadersAndSearchers1969(t *testing.T) {
	const (
		rows = 128
		dims = 32
		m    = 16
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[17].vector...)
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}

	t.Run("readers", func(t *testing.T) {
		readers := make([]*columnVectorGraphPhysicalRowReader, workers)
		for i := range readers {
			reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
			if err != nil {
				t.Fatalf("open reader %d: %v", i, err)
			}
			defer func() { _ = reader.Close() }()
			readers[i] = reader
		}
		want, _, err := readers[0].SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64, ScoreBatchMode: columnVectorGraphScoreBatchModeIndexed}, &columnVectorGraphNativeSearchScratch{})
		if err != nil {
			t.Fatalf("baseline SearchCosine: %v", err)
		}
		want = cloneColumnVectorGraphIndexedScoringResults1969(want)
		var wg sync.WaitGroup
		errs := make(chan string, workers)
		for worker := range readers {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				var scratch columnVectorGraphNativeSearchScratch
				for i := 0; i < 50; i++ {
					got, stats, err := readers[worker].SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 10, EfSearch: 64, ScoreBatchMode: columnVectorGraphScoreBatchModeIndexed}, &scratch)
					if err != nil {
						errs <- fmt.Sprintf("worker %d iteration %d SearchCosine: %v", worker, i, err)
						return
					}
					if stats.ScoreBatchCandidates == 0 || stats.VectorScratchDecodes != 0 {
						errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want indexed typed source without vector scratch", worker, i, stats)
						return
					}
					if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(got, want); mismatch != "" {
						errs <- fmt.Sprintf("worker %d iteration %d: %s", worker, i, mismatch)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})

	t.Run("searchers", func(t *testing.T) {
		searchers := make([]*VectorIndexSearcher, workers)
		for i := range searchers {
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				t.Fatalf("OpenVectorIndexSearcher %d: %v", i, err)
			}
			defer func() { _ = searcher.Close() }()
			searchers[i] = searcher
		}
		opts := VectorIndexSearcherSearchOptions{Query: query, TopK: 10, EfSearch: 64, scoreBatchMode: columnVectorGraphScoreBatchModeIndexed}
		var baselineBuffer VectorIndexSearchBuffer
		baseline, err := searchers[0].SearchWithBuffer(opts, &baselineBuffer)
		if err != nil {
			t.Fatalf("baseline SearchWithBuffer: %v", err)
		}
		want := cloneVectorIndexSearchResults1969(baseline.Results)
		var wg sync.WaitGroup
		errs := make(chan string, workers)
		for worker := range searchers {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				var buffer VectorIndexSearchBuffer
				for i := 0; i < 50; i++ {
					got, err := searchers[worker].SearchWithBuffer(opts, &buffer)
					if err != nil {
						errs <- fmt.Sprintf("worker %d iteration %d SearchWithBuffer: %v", worker, i, err)
						return
					}
					if got.Stats.ScoreBatchCandidates == 0 || got.Stats.VectorScratchDecodes != 0 {
						errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want indexed typed source without vector scratch", worker, i, got.Stats)
						return
					}
					if mismatch := vectorIndexSearchResultsMismatch1969(got.Results, want); mismatch != "" {
						errs <- fmt.Sprintf("worker %d iteration %d: %s", worker, i, mismatch)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})
}

func attachColumnVectorGraphIndexedScoreSources1969(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, rows []columnVectorGraphAssetRow) {
	tb.Helper()
	if reader == nil {
		tb.Fatal("nil reader")
	}
	dims := reader.def.Dimensions
	part := &columnVectorGraphTypedColumnVectorPart{
		generation: 1,
		partID:     1,
		rows:       len(rows),
		values:     make([]float32, 0, len(rows)*dims),
		outcome:    columnVectorGraphTypedColumnVectorOutcomeMmapDirect,
	}
	locations := make([]columnVectorGraphTypedColumnVectorLocation, len(rows))
	invNorms := make([]float32, len(rows))
	for ordinal, row := range rows {
		if len(row.Vector) != dims {
			tb.Fatalf("row %d dims=%d want %d", ordinal, len(row.Vector), dims)
		}
		part.values = append(part.values, row.Vector...)
		locations[ordinal] = columnVectorGraphTypedColumnVectorLocation{part: part, generation: 1, rowIndex: ordinal}
		invNorm := row.InvNorm
		if invNorm == 0 {
			var err error
			invNorm, err = columnVectorGraphInvNorm(row.Vector)
			if err != nil {
				tb.Fatalf("row %d inv_norm: %v", ordinal, err)
			}
		}
		invNorms[ordinal] = invNorm
	}
	reader.typedVectorSource = &columnVectorGraphTypedColumnVectorSource{
		dims:           dims,
		locationSource: columnVectorGraphTypedColumnVectorLocationSourceRowRefState,
		locations:      locations,
		parts:          []*columnVectorGraphTypedColumnVectorPart{part},
	}
	reader.invNormSource = &columnVectorGraphInvNormStateSource{
		rows:    len(rows),
		values:  invNorms,
		outcome: columnVectorGraphInvNormStateOutcomeMmapDirect,
	}
}

func searchColumnVectorGraphIndexedScoring1969(tb testing.TB, reader *columnVectorGraphPhysicalRowReader, query []float32, mode columnVectorGraphScoreBatchMode, topK int, efSearch int) ([]columnVectorGraphNativeSearchResult, columnVectorGraphNativeSearchStats) {
	tb.Helper()
	var scratch columnVectorGraphNativeSearchScratch
	results, stats, err := reader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: topK, EfSearch: efSearch, ScoreBatchMode: mode}, &scratch)
	if err != nil {
		tb.Fatalf("SearchCosine mode=%s: %v", mode.String(), err)
	}
	return cloneColumnVectorGraphIndexedScoringResults1969(results), stats
}

func cloneColumnVectorGraphIndexedScoringResults1969(in []columnVectorGraphNativeSearchResult) []columnVectorGraphNativeSearchResult {
	out := make([]columnVectorGraphNativeSearchResult, len(in))
	for i, result := range in {
		out[i] = result
		out[i].ID = append([]byte(nil), result.ID...)
	}
	return out
}

func assertColumnVectorGraphIndexedScoringResultsEqual1969(tb testing.TB, got, want []columnVectorGraphNativeSearchResult) {
	tb.Helper()
	if mismatch := columnVectorGraphIndexedScoringResultsMismatch1969(got, want); mismatch != "" {
		tb.Fatal(mismatch)
	}
}

func columnVectorGraphIndexedScoringResultsMismatch1969(got, want []columnVectorGraphNativeSearchResult) string {
	if len(got) != len(want) {
		return fmt.Sprintf("results len=%d want %d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			return fmt.Sprintf("result[%d]=%s want %s", i, columnGraphNativeSearchResultStringV3(got[i]), columnGraphNativeSearchResultStringV3(want[i]))
		}
	}
	return ""
}

func cloneVectorIndexSearchResults1969(in []VectorIndexSearchResult) []VectorIndexSearchResult {
	out := make([]VectorIndexSearchResult, len(in))
	for i, result := range in {
		out[i] = result
		out[i].ID = append([]byte(nil), result.ID...)
		out[i].Document = append([]byte(nil), result.Document...)
	}
	return out
}

func vectorIndexSearchResultsMismatch1969(got, want []VectorIndexSearchResult) string {
	if len(got) != len(want) {
		return fmt.Sprintf("results len=%d want %d got=%+v want=%+v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i].Ordinal != want[i].Ordinal || !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			return fmt.Sprintf("result[%d]={ordinal:%d id:%q score:%.9f} want {ordinal:%d id:%q score:%.9f}", i, got[i].Ordinal, string(got[i].ID), got[i].Score, want[i].Ordinal, string(want[i].ID), want[i].Score)
		}
	}
	return ""
}
