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
		"prepared_typed_column_placeholder",
		"setup_open_prepare",
		"graph_only",
		"result_id",
		"document_materialization",
		"production8192",
		"serving1024",
		"Skipped placeholders; not performance data",
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
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("benchmark matrix spec missing %q", needle)
		}
	}
	forbidden := []string{
		"prepared_typed_column_placeholder` | `graph_only` | serial/parallel | `production8192` | Supported",
		"skipped placeholder is performance data",
	}
	for _, needle := range forbidden {
		if strings.Contains(text, needle) {
			t.Fatalf("benchmark matrix spec contains misleading phrase %q", needle)
		}
	}
}
