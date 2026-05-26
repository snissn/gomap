package collections

import (
	"fmt"
	"testing"
)

func BenchmarkCollectionReadViewFetchDocumentsByRowRefMaterializerRows1874(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		rows := rows
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			benchmarkCollectionReadViewFetchDocumentsByRowRefMaterializerRows1874(b, rows)
		})
	}
}

func benchmarkCollectionReadViewFetchDocumentsByRowRefMaterializerRows1874(b *testing.B, rows int) {
	b.Helper()
	d, col := newDocumentMaterializerTestCollection(b)
	defer func() { _ = d.Close() }()
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		ids[i] = []byte(fmt.Sprintf("e%05d", i))
		docs[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"kind-%d","score":%0.1f,"payload":"retained-%d"}`, i, i%8, float64(i)+0.5, i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		b.Fatalf("InsertBatch: %v", err)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		b.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	fetchIDs := documentRowLocatorBenchmarkFetchIDs1874(ids)
	resolved, err := view.FetchDocumentsByID(fetchIDs, DocumentFetchOptions{})
	if err != nil {
		b.Fatalf("resolve row refs: %v", err)
	}
	refs := make([]DocumentRowRef, len(resolved.Results))
	for i := range resolved.Results {
		if !resolved.Results[i].Found {
			b.Fatalf("resolve row refs result[%d] missing", i)
		}
		refs[i] = resolved.Results[i].RowRef
	}
	measured, err := view.FetchDocumentsByRowRef(refs, DocumentFetchOptions{})
	if err != nil {
		b.Fatalf("measure FetchDocumentsByRowRef: %v", err)
	}
	stats := measured.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := view.FetchDocumentsByRowRef(refs, DocumentFetchOptions{})
		if err != nil {
			b.Fatalf("FetchDocumentsByRowRef: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatalf("FetchDocumentsByRowRef returned 0 results")
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportDocumentMaterializerBenchMetrics(b, stats)
}

func documentRowLocatorBenchmarkFetchIDs1874(ids [][]byte) [][]byte {
	if len(ids) == 0 {
		return nil
	}
	positions := []int{37, 128, 255, 512, 700, 900, 1000, 3, 44, 88}
	out := make([][]byte, len(positions))
	for i, pos := range positions {
		out[i] = ids[pos%len(ids)]
	}
	return out
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsRows1874(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		rows := rows
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			benchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsRows1874(b, rows)
		})
	}
}

func benchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsRows1874(b *testing.B, rows int) {
	b.Helper()
	const (
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
	query := append([]float32(nil), input[37%len(input)].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatalf("SearchVectorIndex returned 0 results")
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsRows1874(b *testing.B) {
	for _, rows := range []int{1024, 8192} {
		rows := rows
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsRows1874(b, rows)
		})
	}
}

func benchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsRows1874(b *testing.B, rows int) {
	b.Helper()
	const (
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
	query := append([]float32(nil), input[37%len(input)].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measured, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measured.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		if len(got.Results) == 0 {
			b.Fatalf("Search returned 0 results")
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}
