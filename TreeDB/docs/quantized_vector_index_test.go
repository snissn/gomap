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
		"Missing, stale, corrupt, mismatched, unsupported, closed, or unprepared assets return `ErrVectorIndexSearchUnavailable`",
		"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
		"BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414",
		"BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		"Collection.SearchVectorIndexWithBuffer",
		"BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926",
		"BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450",
		"recall_at_k_pct",
		"quantized_code_B/search",
		"vector_B/search",
		"norm_B/search",
		"quantized_asset_B/vector",
		"rabitq-closeout-2454.md",
		"RaBitQ go-highway acceleration did not land",
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
			"rabitq-closeout-2454.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"quantized-vector-index.md",
			"rabitq-closeout-2454.md",
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-asset-schema.md"): {
			"quantized-vector-index.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"): {
			"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
			"Current scalar Go scorer evidence makes no speedup claim",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-benchmark-matrix.md"): {
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
			"must not be relabeled",
		},
		filepath.Join(treeRoot, "docs", "spec", "storage-format.md"): {
			"declared quantized code score planes",
			"quantized/<name>/codes",
		},
		filepath.Join(treeRoot, "docs", "spec", "verification.md"): {
			"Quantized Vector Score Planes",
			"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
			"BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450",
		},
		filepath.Join(treeRoot, "docs", "guides", "vector-search-typed-column.md"): {
			"Optional quantized query modes",
			"quantized-vector-index.md",
			"pure-Go `rabitq_1bit` score planes are behavior/storage/recall evidence",
			"BRQ/PQ/OPQ/residual codecs, accelerated RaBitQ backends",
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

func TestDocs_RaBitQCloseout2454(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "rabitq-closeout-2454.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rabitq closeout spec: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"RaBitQ pure-Go",
		"go-highway acceleration",
		"fail closed with `ErrVectorIndexSearchUnavailable`",
		"Logical bit `i` is LSB-first at byte `i/8`, bit `i%8`",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		"BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450",
		"profile artifacts",
		"recall@K vs exact",
		"selected score-plane code B/vector",
		"selected score-plane asset B/vector",
		"raw line still emits `quantized_code_B/vector=128`",
		"exact mode selects no quantized score plane",
		"pure-Go RaBitQ weighted scorer is slower than exact FP32 and scalar_u8",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("rabitq closeout spec missing %q", want)
		}
	}
	for _, stale := range []string{
		"explicit RaBitQ quantized searches still return `ErrVectorIndexSearchUnavailable`",
		"RaBitQ search integration and SIMD/popcount acceleration remain follow-up work",
	} {
		if strings.Contains(normalized, strings.Join(strings.Fields(stale), " ")) {
			t.Fatalf("rabitq closeout spec still contains stale deferred-search wording %q", stale)
		}
	}
}
