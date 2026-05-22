package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_ColumnGraphNativeBlockPlanner(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	planPath := filepath.Join(treeRoot, "docs", "spec", "column-graph-native-block-planner.md")
	content, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read block planner: %v", err)
	}
	text := string(content)
	required := []string{
		"Granule/block-oriented typed reader",
		"ordinal -> granule/block -> typed vector/invNorm/adjacency spans",
		"ColumnGraphSearchPlan",
		"ColumnGraphBlockView",
		"Batched scoring loop",
		"Lazy adjacency expansion",
		"Final result materialization",
		"must not become a hidden full graph decode",
		"decoded_blocks/search == 0",
		"physical_B/search == 0",
		"docs_fetched/search == 0",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("block planner missing %q", needle)
		}
	}
}

func TestDocs_ColumnGraphNativeVectorSearchGuide(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	guidePath := filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md")
	content, err := os.ReadFile(guidePath)
	if err != nil {
		t.Fatalf("read guide: %v", err)
	}
	text := string(content)
	required := []string{
		"`column_graph_native_reader`",
		"must not materialize a full decoded `ColumnVectorGraph` copy",
		"OpenVectorIndexSearcher",
		"SearchVectorIndex",
		"BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6",
		"BenchmarkColumnVectorGraphNativeSearchCosineParallelV3",
		"row_fetches/search",
		"cache_hit_ratio",
		"open_physical_B/op",
		"docs_fetched/search",
		"scripts/treedb_column_graph_glove_demo.sh --run",
		"not vendor the dataset",
		"one searcher per concurrent worker",
		"document-fetch-free",
	}
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			t.Fatalf("guide missing %q", needle)
		}
	}
	forbidden := []string{
		"decoded path is native",
		"decoded `ColumnVectorGraph` path is native",
		"SearchVectorIndex is the steady-state path",
	}
	for _, needle := range forbidden {
		if strings.Contains(text, needle) {
			t.Fatalf("guide contains misleading phrase %q", needle)
		}
	}

	scriptPath := filepath.Join(repoRoot, "scripts", "treedb_column_graph_glove_demo.sh")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read dataset script: %v", err)
	}
	scriptText := string(script)
	scriptRequired := []string{
		"dry_run=true",
		"https://nlp.stanford.edu/data/glove.6B.zip",
		"go run ./cmd/treedb_column_graph_demo",
		"-glove",
		"-max-decoded-blocks",
	}
	for _, needle := range scriptRequired {
		if !strings.Contains(scriptText, needle) {
			t.Fatalf("dataset script missing %q", needle)
		}
	}
}
