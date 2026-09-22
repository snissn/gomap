package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_GraphSearchPreparedViewPolicy(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-prepared-views.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read graph-search prepared-view spec: %v", err)
	}
	doc := string(data)
	normalized := strings.Join(strings.Fields(doc), " ")

	for _, want := range []string{
		"Healthy current-format graph-search state requires the #2047 `mmap_direct` tier",
		"Certification is an open/searcher-time operation",
		"#2044 enforcement recognizes the role and fails closed",
		"#2046 owns reusable certifier APIs",
		"#2037 owns the benchmark matrix",
		"Graph-row fallback prohibition",
		"No future typed-column logical type, physical type, encoding, or wrapper may become a healthy current-format graph-search dependency",
		"`ns/op`, `ops/sec`, `B/op`, `allocs/op`",
		"#2043 closeout status",
		"not an unconditional wall-time win over the old legacy graph-row direct control",
		"612` versus `3340` visited_edges/search",
		"#1979 now adds opt-in benchmark-debug control-flow counters",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("graph-search prepared-view spec missing required contract text %q", want)
		}
	}

	rows := []struct {
		state    string
		logical  string
		encoding string
		counter  string
		fallback string
	}{
		{state: "Base vectors", logical: "float32_vector", encoding: "raw_float32_vector", counter: "vector_mmap_direct/search", fallback: "typed_column_vector_fallbacks/search"},
		{state: "HNSW adjacency", logical: "uint32_list", encoding: "raw_uint32_offsets_list", counter: "adjacency_prepared_csr_mmap_direct/search", fallback: "adjacency_legacy_fallbacks/search"},
		{state: "Inverse norms", logical: "float32", encoding: "raw_float32", counter: "norm_mmap_direct/search", fallback: "norm_source_fallbacks/search"},
		{state: "Row refs", logical: "int64", encoding: "raw_int64", counter: "row_ref_state_result_refs", fallback: "row_ref_vector_source_legacy_graph_ids"},
		{state: "Document IDs", logical: "bytes", encoding: "raw_bytes_offsets", counter: "result_id_typed_bytes_state", fallback: "result_id_graph_fallbacks"},
	}
	for _, row := range rows {
		line := findGraphSearchPreparedViewRow(doc, row.state)
		if line == "" {
			t.Fatalf("graph-search prepared-view matrix missing state row %q", row.state)
		}
		for _, want := range []string{row.logical, row.encoding, "`mmap_direct`", row.counter, row.fallback, "MUST NOT"} {
			if !strings.Contains(line, want) {
				t.Fatalf("graph-search prepared-view row %q missing %q: %s", row.state, want, line)
			}
		}
	}
}

func TestDocs_GraphSearchPreparedViewSpecLinkedFromOwners(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	links := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"typed-column-graph-search-prepared-views.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"typed-column-graph-search-prepared-views.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "vector-index-state-manifest.md"): {
			"typed-column-graph-search-prepared-views.md",
		},
		filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"): {
			"typed-column-graph-search-prepared-views.md",
		},
	}
	for path, wants := range links {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing graph-search prepared-view spec link %q", path, want)
			}
		}
	}
}

func findGraphSearchPreparedViewRow(doc, state string) string {
	for _, line := range strings.Split(doc, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") || strings.Contains(line, "---") {
			continue
		}
		cells := markdownTableCells(line)
		if len(cells) < 8 {
			continue
		}
		if strings.TrimSpace(cells[0]) == state {
			return line
		}
	}
	return ""
}
