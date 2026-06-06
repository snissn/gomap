package docs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocs_QuantizedVectorIndex1926Closeout(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "quantized-vector-index.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read quantized vector index spec: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"`exact` / zero value",
		"quantized_only",
		"quantized_rerank",
		"`QuantizedRerankCandidates=0` means the normalized `ef_search` candidate set",
		"Missing, stale, corrupt, mismatched, unsupported, or unprepared assets return `ErrVectorIndexSearchUnavailable`",
		"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
		"BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414",
		"BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926",
		"recall_at_k_pct",
		"quantized_code_B/search",
		"vector_B/search",
		"norm_B/search",
		"quantized_asset_B/vector",
		"does **not** claim a current speedup",
		"Batch scorer kernels, SIMD/popcount integration, graph control-flow changes, block-planner/windowing changes",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("quantized vector index spec missing %q", want)
		}
	}
}

func TestDocs_QuantizedVectorIndex1926LinkedOwners(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	checks := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"quantized-vector-index.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"quantized-vector-index.md",
			"BenchmarkColumnGraphScalarU8Quantized(ScorePlanes|RebuildStorage)1926",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-asset-schema.md"): {
			"quantized-vector-index.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"): {
			"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
			"current scalar Go scorer evidence makes no speedup claim",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-benchmark-matrix.md"): {
			"BenchmarkColumnGraphScalarU8Quantized(ScorePlanes|RebuildStorage)1926",
			"must not be relabeled",
		},
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"): {
			"declared scalar quantized code score planes",
			"quantized/<name>/codes",
		},
		filepath.Join(treeRoot, "docs", "spec", "verification.md"): {
			"Quantized Vector Score Planes",
			"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
		},
		filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"): {
			"Optional quantized query modes",
			"quantized-vector-index.md",
		},
	}
	for path, wants := range checks {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing quantized vector index closeout text %q", path, want)
			}
		}
	}
}
