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
		"BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedAlphaSearchWithBuffer2414",
		"BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451",
		"BenchmarkVectorIndexSearcherColumnGraphBRQQuantizedSearchWithBuffer2481",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415",
		"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		"Collection.SearchVectorIndexWithBuffer",
		"BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926",
		"BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450",
		"BenchmarkColumnGraphBRQQuantizedRebuildStorage2481",
		"brq_1bit_estimated_cosine_q4",
		"quantized_score_codec_brq_1bit/search=1",
		"recall_at_k_pct",
		"quantized_code_B/search",
		"vector_B/search",
		"norm_B/search",
		"quantized_asset_B/vector",
		"quantized_score_codec_scalar_u8_alpha/search",
		"scalar-u8-alpha-default-gate-2845.md",
		"per-granule-alpha remains explicit opt-in",
		"rabitq-closeout-2454.md",
		"vector-search-closeout-2483.md",
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
			"scalar-u8-alpha-default-gate-2845.md",
			"rabitq-closeout-2454.md",
			"vector-search-closeout-2483.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"quantized-vector-index.md",
			"rabitq-closeout-2454.md",
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-asset-schema.md"): {
			"quantized-vector-index.md",
			"prototype `brq_1bit` v1",
		},
		filepath.Join(treeRoot, "docs", "spec", "typed-column-graph-search-admission.md"): {
			"BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926",
			"BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415",
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
			"prototype `brq_1bit` score planes are behavior/storage/recall evidence",
			"PQ/OPQ/residual codecs, accelerated RaBitQ backends",
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

func TestDocs_ScalarU8AlphaDefaultGate2845(t *testing.T) {
	treeRoot, _ := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "scalar-u8-alpha-default-gate-2845.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read scalar_u8 alpha default gate: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"Status: **no-promote / explicit opt-in**",
		"Omitted `scalar_u8_calibration` continues to mean legacy `scalar_u8` v1",
		"Collection.SearchVectorIndexWithBuffer",
		"81.25%",
		"100%",
		"quantized_score_codec_scalar_u8_alpha/search=1",
		"0 B/op`, `0 allocs/op",
		"alpha min/mean/max `0.1495`",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("scalar_u8 alpha default gate missing %q", want)
		}
	}
}

func TestDocs_VectorSearchCloseout2483(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "vector-search-closeout-2483.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vector search closeout: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"# TreeDB vector search closeout guidance (#2483)",
		"#2487 unified current-main snapshot",
		"#2507 added lower-level BRQ asset/search runtime",
		"#2494 crossover evidence is pending",
		"TreeDB_SearchWithBufferParallel",
		"collection `quantized_only`, c=8",
		"rabitq_1bit",
		"brq_1bit_estimated_cosine_q4",
		"/tmp/2481_runtime_bench_20260606_165236",
		"quantized_score_codec_brq_1bit/search=1",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("vector search closeout missing %q", want)
		}
	}
	for path, wants := range map[string][]string{
		filepath.Join(treeRoot, "docs", "guides", "vector-search-high-qps-collection-api.md"): {
			"vector-search-closeout-2483.md",
			"Quantized collection buffered search is supported as a separate route state",
		},
		filepath.Join(treeRoot, "docs", "guides", "vector-search-benchmark-workflow.md"): {
			"vector-search-closeout-2483.md",
			"#2494",
		},
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"vector-search-closeout-2483.md",
			"#2494 crossover-pending status",
		},
		filepath.Join(repoRoot, "docs", "README.md"): {
			"TreeDB Vector Search Closeout",
			"vector-search-closeout-2483.md",
		},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing #2483 closeout link text %q", path, want)
			}
		}
	}
}

func TestDocs_QuantizedPreparedHNSWCloseout2588(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "quantized-prepared-hnsw-closeout-2588.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read quantized prepared HNSW closeout: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"# TreeDB quantized prepared HNSW fast-path closeout (#2588)",
		"#2591 | #2592 | promoted",
		"#2585 | #2593 | promoted",
		"#2586 | #2596 | promoted",
		"#2587 | #2606 | promoted",
		"/tmp/issue2587_10k768_final2_20260611_000046.txt",
		"exact_fp32 | SearchWithBuffer | 1 | 24917",
		"scalar_u8 | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 8",
		"rabitq_1bit | CollectionSearchVectorIndexWithBuffer quantized_rerank cand=32 | 8",
		"All hot rows are `0 B/op`, `0 allocs/op`",
		"no statistically significant exact FP32 regressions",
		"/tmp/issue2588_scalar_profiles_20260611_004414",
		"/tmp/issue2587_profiles_final2b_20260611_000238",
		"rabitq_1bit` quantized modes use prepared `hnsw_search_pack_v1` traversal",
		"must not be relabeled as exact FP32",
		"did **not** change `rabitq_1bit` v1 storage, LSB bit order, high-bit padding, weighted sign-dot score formula",
		"QuantizedRerankCandidates` limits only the exact rerank shortlist",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("quantized prepared HNSW closeout missing %q", want)
		}
	}
	for path, wants := range map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"quantized-prepared-hnsw-closeout-2588.md",
			"#2591 10k x 768 gate rows",
		},
		filepath.Join(repoRoot, "docs", "README.md"): {
			"TreeDB Quantized Prepared HNSW Closeout",
			"quantized-prepared-hnsw-closeout-2588.md",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-vector-index.md"): {
			"quantized-prepared-hnsw-closeout-2588.md",
			"rabitq_1bit` reports `search_route_hnsw_search_pack/search=1`",
		},
		filepath.Join(treeRoot, "docs", "spec", "column-graph-native-vector-search.md"): {
			"quantized-prepared-hnsw-closeout-2588.md",
			"rabitq_1bit` quantized rows may use prepared",
		},
		filepath.Join(treeRoot, "docs", "spec", "rabitq-1bit-v1.md"): {
			"quantized-prepared-hnsw-closeout-2588.md",
			"search_route_hnsw_search_pack/search=1",
		},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(data)
		for _, want := range wants {
			if !strings.Contains(text, want) {
				t.Fatalf("%s missing #2588 closeout link text %q", path, want)
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

func TestDocs_RaBitQPerformanceLaneCloseout2482(t *testing.T) {
	treeRoot, repoRoot := repoRoots(t)
	path := filepath.Join(treeRoot, "docs", "spec", "rabitq-performance-lane-closeout-2482.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rabitq performance lane closeout spec: %v", err)
	}
	text := string(data)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, want := range []string{
		"# TreeDB RaBitQ performance lane closeout (#2482)",
		"closes **Sublane A**, the `rabitq_1bit` v1 performance sweep",
		"**Sublane B** was deferred; it has since completed through #2480",
		"`rabitq_1bit` v1 durable storage layout",
		"LSB-first bit order",
		"weighted sign-dot score formula",
		"durable asset identity",
		"codec name/version/config identity",
		"fail-closed behavior",
		"semantics-preserving #2477 query-byte-table scorer evidence",
		"/tmp/gomap_2477_final2_baseline_main_435a1c06_20260606_112457",
		"/tmp/gomap_2477_final2_candidate_17857e779_20260606_112935",
		"/tmp/gomap_2477_interleaved_scalar_lower_qonly_c8_20260606_114454",
		"lower `quantized_only`, c=1",
		"collection `quantized_rerank` candidates=32, c=8",
		"search_route_quantized_only/search=1",
		"search_route_quantized_rerank/search=1",
		"fallback / unavailable counters `0`",
		"vector/norm `16,384/128`, exact calls `32`",
		"code B/search",
		"asset B/vector",
		"Scalar guardrail row",
		"Exact FP32 `hnsw_search_pack_v1` rows were not required for #2477",
		"no code PR or AI review was opened",
		"Sublane B is complete for design/prototype purposes",
		"#2507 merge `8e7377cc995ffc928ac99a42ed8aec769f8f72fb`",
	} {
		if !strings.Contains(normalized, strings.Join(strings.Fields(want), " ")) {
			t.Fatalf("rabitq performance lane closeout missing %q", want)
		}
	}

	checks := map[string][]string{
		filepath.Join(treeRoot, "docs", "spec", "README.md"): {
			"rabitq-performance-lane-closeout-2482.md",
			"completed Sublane B status",
		},
		filepath.Join(treeRoot, "docs", "spec", "rabitq-1bit-v1.md"): {
			"rabitq-performance-lane-closeout-2482.md",
			"#2478/#2479 no-promote decisions",
		},
		filepath.Join(treeRoot, "docs", "spec", "quantized-vector-index.md"): {
			"rabitq-performance-lane-closeout-2482.md",
			"does not change storage, bit order, score formula, codec/asset identity, or fail-closed behavior",
		},
		filepath.Join(repoRoot, "docs", "README.md"): {
			"TreeDB RaBitQ Performance Lane Closeout",
			"rabitq-performance-lane-closeout-2482.md",
			"TreeDB Vector Search Closeout",
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
				t.Fatalf("%s missing #2482 closeout link text %q", path, want)
			}
		}
	}
}
