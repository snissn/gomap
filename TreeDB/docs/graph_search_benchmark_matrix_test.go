package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_GraphSearchBenchmarkTruthMatrix2037(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-benchmark-matrix.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read benchmark matrix spec: %v", err)
	}
	text := string(content)
	required := []string{
		"BenchmarkColumnVectorGraphSearchTruthMatrix2037",
		"mode=<mode>/boundary=<boundary>/concurrency=<serial|parallel>/fixture=<fixture>",
		"legacy_direct_graph_row",
		"current_tvis_base_typed_column",
		"combined_prepared_typed_column",
		"setup_open_prepare",
		"graph_only",
		"result_id",
		"document_materialization",
		"production8192",
		"serving1024",
		"Supported #2045 combined prepared graph-only rows",
		"prepared_graph_search_views/search",
		"graph_row_fallbacks/search",
		"ops/sec",
		"graph_rows",
		"candidates/search",
		"edges/search",
		"docs_fetched/search",
		"result_id_typed_bytes_state/search",
		"row_ref_state_source_fallbacks/search",
		"vector_mmap_direct/search",
		"norm_mmap_direct/search",
		"adjacency_prepared_csr_mmap_direct/search",
		"adjacency_typed_list_mmap_direct/search",
		"#2043 closeout evidence",
		"BenchmarkColumnVectorGraphAdjacencyAccessApplesToApples2043",
		"not an end-to-end search benchmark",
		"612 edges/search",
		"3340 edges/search",
		"not as storage-path proof",
		"#1979 is the natural owner",
		"not a full wall-time promotion",
		"#1980 remains an evidence-backed follow-up",
		"#1977 remains deferred",
		"BenchmarkColumnVectorGraphSearchTopologyParity2091",
		"TestColumnVectorGraphSearchTopologyParity2091",
		"topology-parity search benchmark",
		"legacy_graph_row_direct",
		"current_prepared_typed_column",
		"both paths visit exactly 612 edges/search",
		"not yet throughput-superior",
		"BenchmarkColumnVectorGraphSearchBatchability1979",
		"StatsMode=benchmark_debug",
		"neighbor_tile_avg_size=16",
		"score_batch_singletons",
		"already_visited_skips",
		"top_k_insert_rejections",
		"exact_order_observations=8192",
		"keep #2035 open as not performance-satisfied",
		"#2103 prepared indexed-scoring promotion matrix",
		"BenchmarkColumnVectorGraphSearchPromotion2103",
		"ScoreBatchMode=default",
		"eligible prepared typed-column path",
		"legacy graph-row/direct compatibility",
		"#2035 should remain open as the broader promotion tracker",
		"#2105 public VectorIndexSearcher promotion matrix",
		"BenchmarkVectorIndexSearcherSearchPromotion2105",
		"TestColumnVectorGraphPublicVectorIndexSearcherSearchPromotion2105",
		"Public result-ID response",
		"document_materialization",
		"exact",
		"benchmark_debug_searches/search",
		"exact_candidate_order_backward_jumps/search",
		"legacy/direct `default` remains scalar",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("benchmark matrix spec missing %q", needle)
		}
	}
	forbidden := []string{
		"prepared_typed_column_placeholder",
		"Skipped placeholders; not performance data",
		"skipped placeholder is performance data",
	}
	for _, needle := range forbidden {
		if strings.Contains(text, needle) {
			t.Fatalf("benchmark matrix spec contains misleading phrase %q", needle)
		}
	}
}
