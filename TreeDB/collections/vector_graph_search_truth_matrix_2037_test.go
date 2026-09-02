package collections

import (
	"fmt"
	"testing"
)

type vectorGraphSearchTruthMatrixMode2037 string

const (
	vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037 vectorGraphSearchTruthMatrixMode2037 = "legacy_direct_graph_row"
	vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037   vectorGraphSearchTruthMatrixMode2037 = "current_tvis_base_typed_column"
	vectorGraphSearchTruthMatrixModeCombinedPrepared2037     vectorGraphSearchTruthMatrixMode2037 = "combined_prepared_typed_column"
)

type vectorGraphSearchTruthMatrixBoundary2037 string

const (
	vectorGraphSearchTruthMatrixBoundarySetupOpenPrepare2037    vectorGraphSearchTruthMatrixBoundary2037 = "setup_open_prepare"
	vectorGraphSearchTruthMatrixBoundaryGraphOnly2037           vectorGraphSearchTruthMatrixBoundary2037 = "graph_only"
	vectorGraphSearchTruthMatrixBoundaryResultID2037            vectorGraphSearchTruthMatrixBoundary2037 = "result_id"
	vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037 vectorGraphSearchTruthMatrixBoundary2037 = "document_materialization"
)

type vectorGraphSearchTruthMatrixConcurrency2037 string

const (
	vectorGraphSearchTruthMatrixConcurrencySerial2037   vectorGraphSearchTruthMatrixConcurrency2037 = "serial"
	vectorGraphSearchTruthMatrixConcurrencyParallel2037 vectorGraphSearchTruthMatrixConcurrency2037 = "parallel"
)

type vectorGraphSearchTruthMatrixFixture2037 string

const (
	vectorGraphSearchTruthMatrixFixtureServing10242037    vectorGraphSearchTruthMatrixFixture2037 = "serving1024"
	vectorGraphSearchTruthMatrixFixtureProduction81922037 vectorGraphSearchTruthMatrixFixture2037 = "production8192"
)

type vectorGraphSearchTruthMatrixRow2037 struct {
	Mode               vectorGraphSearchTruthMatrixMode2037
	Boundary           vectorGraphSearchTruthMatrixBoundary2037
	Concurrency        vectorGraphSearchTruthMatrixConcurrency2037
	Fixture            vectorGraphSearchTruthMatrixFixture2037
	StatsMode          columnVectorGraphNativeSearchStatsMode
	CanonicalBenchmark string
	UnsupportedReason  string
}

func (r vectorGraphSearchTruthMatrixRow2037) Name() string {
	name := fmt.Sprintf("mode=%s/boundary=%s/concurrency=%s/fixture=%s", r.Mode, r.Boundary, r.Concurrency, r.Fixture)
	if r.StatsMode != columnVectorGraphNativeSearchStatsModeDefault {
		name += "/stats=" + r.StatsMode.String()
	}
	return name
}

func (r vectorGraphSearchTruthMatrixRow2037) Supported() bool {
	return r.UnsupportedReason == ""
}

func vectorGraphSearchTruthMatrixRows2037() []vectorGraphSearchTruthMatrixRow2037 {
	return []vectorGraphSearchTruthMatrixRow2037{
		{Mode: vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037, Boundary: vectorGraphSearchTruthMatrixBoundarySetupOpenPrepare2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037"},
		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundarySetupOpenPrepare2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundarySetupOpenPrepare2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared setup row"},

		{Mode: vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphNativeSearchCosineProduction8192V3 with graph_only_no_result_materialization"},
		{Mode: vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphNativeSearchCosineParallelProduction8192V3 with graph_only_no_result_materialization"},
		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3 with graph_only_no_result_materialization"},
		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnProduction8192V3 with graph_only_no_result_materialization"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared graph-only serial row"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, StatsMode: columnVectorGraphNativeSearchStatsModeMinimal, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared graph-only serial minimal-stats row"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared graph-only parallel row"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryGraphOnly2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureProduction81922037, StatsMode: columnVectorGraphNativeSearchStatsModeMinimal, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared graph-only parallel minimal-stats row"},

		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryResultID2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4"},
		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryResultID2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryResultID2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared result-ID serial row"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryResultID2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared result-ID parallel row"},

		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsV4"},
		{Mode: vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, Boundary: vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithDocumentsV4"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencySerial2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared document-materialization serial row"},
		{Mode: vectorGraphSearchTruthMatrixModeCombinedPrepared2037, Boundary: vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037, Concurrency: vectorGraphSearchTruthMatrixConcurrencyParallel2037, Fixture: vectorGraphSearchTruthMatrixFixtureServing10242037, StatsMode: columnVectorGraphNativeSearchStatsModeBenchmarkDebug, CanonicalBenchmark: "BenchmarkColumnVectorGraphSearchTruthMatrix2037 combined prepared document-materialization parallel row"},
	}
}

func vectorGraphSearchTruthMatrixRequiredMetricLabels2037() []string {
	return []string{
		"ns/op",
		"ops/sec",
		"B/op",
		"allocs/op",
		"graph_rows",
		"parallel_workers",
		"candidate_rows/search",
		"candidates/search",
		"edges/search",
		"visited_edges/search",
		"docs_fetched/search",
		"doc_fetch_ns/search",
		"result_fetches/search",
		"result_id_typed_bytes_state/search",
		"result_id_graph_fallbacks/search",
		"row_ref_vector_source_state/search",
		"row_ref_vector_source_legacy_graph_ids/search",
		"row_ref_state_prepared_views/search",
		"row_ref_state_mmap_direct_fields/search",
		"row_ref_state_result_refs/search",
		"row_ref_state_source_fallbacks/search",
		"result_id_prepared_bytes_views/search",
		"prepared_graph_search_views/search",
		"graph_row_fallbacks/search",
		"doc_row_ref_state_fetches/search",
		"doc_row_ref_lookup_fallbacks/search",
		"vector_mmap_direct/search",
		"vector_heap_copy_typed_view/search",
		"vector_scratch_decodes/search",
		"vector_prepared_direct/search",
		"vector_prepared_identity_mapping/search",
		"vector_prepared_row_ref_mapping/search",
		"typed_column_vector_fallbacks/search",
		"norm_mmap_direct/search",
		"norm_heap_copy_typed_view/search",
		"norm_scratch_decodes/search",
		"norm_prepared_direct/search",
		"norm_source_fallbacks/search",
		"prepared_score_calls/search",
		"score_float64_fallbacks/search",
		"adjacency_prepared_csr_mmap_direct/search",
		"adjacency_prepared_csr_direct_views/search",
		"adjacency_typed_list_mmap_direct/search",
		"adjacency_typed_list_heap_copy_typed_view/search",
		"adjacency_typed_list_scratch_decodes/search",
		"adjacency_legacy_fallbacks/search",
		"adjacency_source_fallbacks/search",
		"benchmark_debug_searches/search",
		"neighbor_tiles/search",
		"neighbor_tile_avg_size",
		"score_batch_singletons/search",
		"score_batch_size_2_4/search",
		"score_batch_size_5_8/search",
		"score_batch_size_9_16/search",
		"score_batch_size_17_plus/search",
		"already_visited_skips/search",
		"frontier_pushes/search",
		"frontier_pops/search",
		"top_k_insert_rejections/search",
		"visited_mark_hits/search",
		"visited_mark_misses/search",
		"exact_candidate_order_observations/search",
		"exact_candidate_order_backward_jumps/search",
	}
}

func TestVectorGraphSearchTruthMatrixRows2037(t *testing.T) {
	rows := vectorGraphSearchTruthMatrixRows2037()
	wantNames := []string{
		"mode=legacy_direct_graph_row/boundary=setup_open_prepare/concurrency=serial/fixture=serving1024",
		"mode=current_tvis_base_typed_column/boundary=setup_open_prepare/concurrency=serial/fixture=serving1024",
		"mode=combined_prepared_typed_column/boundary=setup_open_prepare/concurrency=serial/fixture=serving1024",
		"mode=legacy_direct_graph_row/boundary=graph_only/concurrency=serial/fixture=production8192",
		"mode=legacy_direct_graph_row/boundary=graph_only/concurrency=parallel/fixture=production8192",
		"mode=current_tvis_base_typed_column/boundary=graph_only/concurrency=serial/fixture=production8192",
		"mode=current_tvis_base_typed_column/boundary=graph_only/concurrency=parallel/fixture=production8192",
		"mode=combined_prepared_typed_column/boundary=graph_only/concurrency=serial/fixture=production8192",
		"mode=combined_prepared_typed_column/boundary=graph_only/concurrency=serial/fixture=production8192/stats=minimal",
		"mode=combined_prepared_typed_column/boundary=graph_only/concurrency=parallel/fixture=production8192",
		"mode=combined_prepared_typed_column/boundary=graph_only/concurrency=parallel/fixture=production8192/stats=minimal",
		"mode=current_tvis_base_typed_column/boundary=result_id/concurrency=serial/fixture=serving1024/stats=benchmark_debug",
		"mode=current_tvis_base_typed_column/boundary=result_id/concurrency=parallel/fixture=serving1024/stats=benchmark_debug",
		"mode=combined_prepared_typed_column/boundary=result_id/concurrency=serial/fixture=serving1024/stats=benchmark_debug",
		"mode=combined_prepared_typed_column/boundary=result_id/concurrency=parallel/fixture=serving1024/stats=benchmark_debug",
		"mode=current_tvis_base_typed_column/boundary=document_materialization/concurrency=serial/fixture=serving1024/stats=benchmark_debug",
		"mode=current_tvis_base_typed_column/boundary=document_materialization/concurrency=parallel/fixture=serving1024/stats=benchmark_debug",
		"mode=combined_prepared_typed_column/boundary=document_materialization/concurrency=serial/fixture=serving1024/stats=benchmark_debug",
		"mode=combined_prepared_typed_column/boundary=document_materialization/concurrency=parallel/fixture=serving1024/stats=benchmark_debug",
	}
	if len(rows) != len(wantNames) {
		t.Fatalf("rows=%d want %d", len(rows), len(wantNames))
	}
	seen := make(map[string]struct{}, len(rows))
	for i, row := range rows {
		if got := row.Name(); got != wantNames[i] {
			t.Fatalf("row[%d] name=%q want %q", i, got, wantNames[i])
		}
		if _, ok := seen[row.Name()]; ok {
			t.Fatalf("duplicate row name %q", row.Name())
		}
		seen[row.Name()] = struct{}{}
		if row.CanonicalBenchmark == "" {
			t.Fatalf("row %q missing canonical benchmark", row.Name())
		}
		if !row.Supported() {
			t.Fatalf("row %q unexpectedly unsupported: %s", row.Name(), row.UnsupportedReason)
		}
	}
}

func TestVectorGraphSearchTruthMatrixMetricContract2037(t *testing.T) {
	labels := vectorGraphSearchTruthMatrixRequiredMetricLabels2037()
	if len(labels) == 0 {
		t.Fatal("metric labels are empty")
	}
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label == "" {
			t.Fatal("empty metric label")
		}
		if _, ok := seen[label]; ok {
			t.Fatalf("duplicate metric label %q", label)
		}
		seen[label] = struct{}{}
	}
	for _, required := range []string{
		"ops/sec",
		"graph_rows",
		"candidates/search",
		"edges/search",
		"docs_fetched/search",
		"result_id_typed_bytes_state/search",
		"result_id_prepared_bytes_views/search",
		"prepared_graph_search_views/search",
		"graph_row_fallbacks/search",
		"row_ref_state_prepared_views/search",
		"row_ref_state_source_fallbacks/search",
		"vector_mmap_direct/search",
		"vector_prepared_direct/search",
		"norm_mmap_direct/search",
		"norm_prepared_direct/search",
		"adjacency_prepared_csr_mmap_direct/search",
		"adjacency_typed_list_mmap_direct/search",
		"typed_column_vector_fallbacks/search",
		"benchmark_debug_searches/search",
		"score_batch_singletons/search",
		"exact_candidate_order_observations/search",
	} {
		if _, ok := seen[required]; !ok {
			t.Fatalf("missing required metric label %q", required)
		}
	}
}

func BenchmarkColumnVectorGraphSearchTruthMatrix2037(b *testing.B) {
	for _, row := range vectorGraphSearchTruthMatrixRows2037() {
		row := row
		b.Run(row.Name(), func(b *testing.B) {
			if !row.Supported() {
				b.Skip(row.UnsupportedReason)
			}
			switch row.Boundary {
			case vectorGraphSearchTruthMatrixBoundarySetupOpenPrepare2037:
				benchmarkVectorGraphSearchTruthMatrixSetup2037(b, row)
			case vectorGraphSearchTruthMatrixBoundaryGraphOnly2037:
				benchmarkVectorGraphSearchTruthMatrixGraphOnly2037(b, row)
			case vectorGraphSearchTruthMatrixBoundaryResultID2037:
				benchmarkVectorGraphSearchTruthMatrixResultID2037(b, row)
			case vectorGraphSearchTruthMatrixBoundaryDocumentMaterialize2037:
				benchmarkVectorGraphSearchTruthMatrixDocumentMaterialization2037(b, row)
			default:
				b.Fatalf("unsupported truth-matrix boundary %q", row.Boundary)
			}
			reportVectorGraphSearchTruthMatrixRowLabels2037(b, row)
		})
	}
}

func reportVectorGraphSearchTruthMatrixRowLabels2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) {
	b.Helper()
	shape := vectorGraphSearchTruthMatrixFixtureShape2037(row.Fixture)
	b.ReportMetric(2037, "truth_matrix_issue")
	b.ReportMetric(1, "matrix_mode_"+string(row.Mode))
	b.ReportMetric(1, "matrix_boundary_"+string(row.Boundary))
	b.ReportMetric(1, "matrix_concurrency_"+string(row.Concurrency))
	b.ReportMetric(1, "matrix_fixture_"+string(row.Fixture))
	b.ReportMetric(1, "matrix_stats_"+row.StatsMode.String())
	b.ReportMetric(float64(shape.rows), "rows")
	b.ReportMetric(float64(shape.dims), "dims")
	b.ReportMetric(float64(shape.degree), "degree")
	b.ReportMetric(float64(shape.topK), "top_k")
	b.ReportMetric(float64(shape.efSearch), "ef_search")
}

type vectorGraphSearchTruthMatrixShape2037 struct {
	rows     int
	dims     int
	degree   int
	topK     int
	efSearch int
}

func vectorGraphSearchTruthMatrixFixtureShape2037(fixture vectorGraphSearchTruthMatrixFixture2037) vectorGraphSearchTruthMatrixShape2037 {
	switch fixture {
	case vectorGraphSearchTruthMatrixFixtureServing10242037:
		return vectorGraphSearchTruthMatrixShape2037{rows: 1024, dims: 128, degree: 16, topK: 10, efSearch: 128}
	case vectorGraphSearchTruthMatrixFixtureProduction81922037:
		return vectorGraphSearchTruthMatrixShape2037{rows: 8192, dims: 128, degree: 16, topK: 10, efSearch: 128}
	default:
		return vectorGraphSearchTruthMatrixShape2037{}
	}
}

func benchmarkVectorGraphSearchTruthMatrixSetup2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) {
	b.Helper()
	closeFn, col, def, _ := openVectorGraphSearchTruthMatrixCollection2037(b, row)
	defer closeFn()
	statsSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		b.Fatalf("stats OpenVectorIndexSearcher: %v", err)
	}
	readerStats := statsSearcher.reader.Stats()
	stats := VectorIndexSearchStats{
		GraphRows:             uint64(readerStats.Rows),
		OpenGranulesRead:      uint64(readerStats.OpenGranulesRead),
		OpenPhysicalBytesRead: readerStats.OpenPhysicalBytesRead,
		MaxResidentBytes:      readerStats.MaxResidentBytes,
	}
	if statsSearcher.reader != nil && statsSearcher.reader.preparedSearch != nil {
		stats.PreparedGraphSearchViews = 1
	}
	if err := statsSearcher.Close(); err != nil {
		b.Fatalf("Close stats searcher: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += searcher.reader.RowCount()
		if err := searcher.Close(); err != nil {
			b.Fatalf("Close searcher: %v", err)
		}
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func benchmarkVectorGraphSearchTruthMatrixGraphOnly2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) {
	b.Helper()
	shape := columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	shape.omitResultMaterialization = true
	shape.statsMode = row.StatsMode
	switch row.Mode {
	case vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037:
		shape.typedColumnVector = false
	case vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, vectorGraphSearchTruthMatrixModeCombinedPrepared2037:
		shape.typedColumnVector = true
	default:
		b.Fatalf("unsupported graph-only mode %q", row.Mode)
	}
	if row.Concurrency == vectorGraphSearchTruthMatrixConcurrencyParallel2037 {
		benchmarkColumnVectorGraphNativeSearchCosineParallelV3(b, shape)
		return
	}
	benchmarkColumnVectorGraphNativeSearchCosineV3(b, shape)
}

func benchmarkVectorGraphSearchTruthMatrixResultID2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) {
	b.Helper()
	if row.Mode != vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037 && row.Mode != vectorGraphSearchTruthMatrixModeCombinedPrepared2037 {
		b.Fatalf("result-ID boundary currently supports only current/combined prepared typed-column modes, got %q", row.Mode)
	}
	if row.Concurrency == vectorGraphSearchTruthMatrixConcurrencyParallel2037 {
		benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, false, DocumentFetchOptions{}, vectorGraphSearchTruthMatrixPublicStatsMode2037(row.StatsMode))
		return
	}
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, false, DocumentFetchOptions{}, vectorGraphSearchTruthMatrixPublicStatsMode2037(row.StatsMode))
}

func benchmarkVectorGraphSearchTruthMatrixDocumentMaterialization2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) {
	b.Helper()
	if row.Mode != vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037 && row.Mode != vectorGraphSearchTruthMatrixModeCombinedPrepared2037 {
		b.Fatalf("document-materialization boundary currently supports only current/combined prepared typed-column modes, got %q", row.Mode)
	}
	if row.Concurrency == vectorGraphSearchTruthMatrixConcurrencyParallel2037 {
		benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, true, DocumentFetchOptions{}, vectorGraphSearchTruthMatrixPublicStatsMode2037(row.StatsMode))
		return
	}
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, true, DocumentFetchOptions{}, vectorGraphSearchTruthMatrixPublicStatsMode2037(row.StatsMode))
}

func vectorGraphSearchTruthMatrixPublicStatsMode2037(mode columnVectorGraphNativeSearchStatsMode) VectorIndexSearchStatsMode {
	switch mode {
	case columnVectorGraphNativeSearchStatsModeDefault:
		return VectorIndexSearchStatsModeDefault
	case columnVectorGraphNativeSearchStatsModeMinimal:
		return VectorIndexSearchStatsModeMinimal
	case columnVectorGraphNativeSearchStatsModeBenchmarkDebug:
		return VectorIndexSearchStatsModeBenchmarkDebug
	default:
		return VectorIndexSearchStatsModeDefault
	}
}

func openVectorGraphSearchTruthMatrixCollection2037(b *testing.B, row vectorGraphSearchTruthMatrixRow2037) (func(), *Collection, VectorIndexDefinition, []float32) {
	b.Helper()
	shape := columnVectorGraphNativeSearchSmallBenchShapeV3()
	switch row.Fixture {
	case vectorGraphSearchTruthMatrixFixtureServing10242037:
		// default small shape
	case vectorGraphSearchTruthMatrixFixtureProduction81922037:
		shape = columnVectorGraphNativeSearchProduction8192BenchShapeV3()
	default:
		b.Fatalf("unsupported truth-matrix fixture %q", row.Fixture)
	}
	switch row.Mode {
	case vectorGraphSearchTruthMatrixModeLegacyDirectGraphRow2037:
		shape.directPhysicalAsset = true
		shape.typedColumnVector = false
	case vectorGraphSearchTruthMatrixModeCurrentTypedColumn2037, vectorGraphSearchTruthMatrixModeCombinedPrepared2037:
		shape.directPhysicalAsset = false
		shape.typedColumnVector = true
	default:
		b.Fatalf("unsupported truth-matrix fixture mode %q", row.Mode)
	}
	return openColumnVectorGraphNativeSearchBenchFixtureV3(b, shape)
}
