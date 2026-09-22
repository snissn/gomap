package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

func requireFloat64Metric(t *testing.T, value *float64, name string) float64 {
	t.Helper()
	if value == nil {
		t.Fatalf("missing %s metric", name)
	}
	return *value
}

func TestExecuteSmokeCompactsReopensValidatesAndBenchmarks(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  128,
		dimensions:            16,
		queries:               8,
		searchConcurrency:     []int{2},
		validateQueries:       4,
		validateDocs:          4,
		topK:                  5,
		batchSize:             32,
		m:                     8,
		efConstruction:        64,
		efSearch:              64,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Validation.DocumentsChecked != 4 || res.Validation.QueriesChecked != 4 {
		t.Fatalf("validation counts=%+v, want 4 docs and 4 queries", res.Validation)
	}
	if res.Validation.Recall < res.Validation.MinRecall {
		t.Fatalf("recall=%f below min=%f", res.Validation.Recall, res.Validation.MinRecall)
	}
	if res.Search.Queries != 8 || res.Search.AvgNanos <= 0 || res.Search.ExactFallbacks != 0 {
		t.Fatalf("unexpected search benchmark result: %+v", res.Search)
	}
	if len(res.SearchBenchmarks) != 2 || res.SearchBenchmarks[0].Concurrency != 1 || res.SearchBenchmarks[1].Concurrency != 2 {
		t.Fatalf("unexpected search benchmarks: %+v", res.SearchBenchmarks)
	}
	if res.Profile != "bench_unsafe" {
		t.Fatalf("profile=%q want bench_unsafe", res.Profile)
	}
	if res.ValuePointerThreshold != defaultValuePointerThreshold {
		t.Fatalf("value pointer threshold=%d want %d", res.ValuePointerThreshold, defaultValuePointerThreshold)
	}
	if res.LeafGenerationTarget != defaultLeafGenerationTarget {
		t.Fatalf("leaf generation target=%d want %d", res.LeafGenerationTarget, defaultLeafGenerationTarget)
	}
	if res.StorageAfterCompact.TotalBytes <= 0 || res.StorageAfterCompact.BytesPerDoc <= 0 {
		t.Fatalf("missing compacted storage report: %+v", res.StorageAfterCompact)
	}
	if res.FormatConfig == nil {
		t.Fatal("missing format config report")
	}
	if !res.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("index_outer_leaves_in_vlog=false, want true: %+v", res.FormatConfig)
	}
	if !res.FormatConfig.LeafPrefixCompression {
		t.Fatalf("leaf_prefix_compression=false, want true: %+v", res.FormatConfig)
	}
	if res.StorageExpectation.IndexBytes <= 0 {
		t.Fatalf("missing storage expectation index bytes: %+v", res.StorageExpectation)
	}
	if res.StorageExpectation.LeafVLogBytes <= 0 {
		t.Fatalf("missing leaf value-log bytes: %+v", res.StorageExpectation)
	}
	if res.Memory.IndexBytesMemory <= 0 {
		t.Fatalf("missing index memory report: %+v", res.Memory)
	}
	if res.CompactStorage == nil || !res.CompactStorage.FullyCompacted {
		t.Fatalf("compact storage stats=%+v, want fully compacted", res.CompactStorage)
	}
	if res.IndexStatsLoaded.LiveDocs != 128 {
		t.Fatalf("loaded live docs=%d want 128", res.IndexStatsLoaded.LiveDocs)
	}
}

func TestExecuteColumnGraphSearchProfileDirWritesProfiles(t *testing.T) {
	profileDir := t.TempDir()
	oldMemProfileRate := runtime.MemProfileRate
	res, err := execute(context.Background(), config{
		dir:                       t.TempDir(),
		keepDir:                   true,
		docs:                      64,
		dimensions:                8,
		queries:                   4,
		searchConcurrency:         []int{2},
		validateQueries:           0,
		validateDocs:              0,
		topK:                      3,
		batchSize:                 16,
		m:                         4,
		efConstruction:            32,
		efSearch:                  32,
		valuePointerThreshold:     defaultValuePointerThreshold,
		leafGenerationTarget:      defaultLeafGenerationTarget,
		minRecall:                 0,
		disableExactFallback:      true,
		vectorIndexStrategy:       collections.VectorIndexStrategyColumnGraph,
		vectorQueryMode:           collections.VectorIndexQueryModeQuantizedRerank,
		quantizedIndexName:        defaultQuantizedIndexName,
		quantizedRerankCandidates: 4,
		searchProfileDir:          profileDir,
	})
	if err != nil {
		t.Fatalf("execute with search profile dir: %v", err)
	}
	if runtime.MemProfileRate != oldMemProfileRate {
		t.Fatalf("MemProfileRate=%d want restored %d", runtime.MemProfileRate, oldMemProfileRate)
	}
	if res.SearchProfileDir != profileDir {
		t.Fatalf("search profile dir=%q want %q", res.SearchProfileDir, profileDir)
	}
	if len(res.SearchBenchmarks) != 2 || res.SearchBenchmarks[0].Concurrency != 1 || res.SearchBenchmarks[1].Concurrency != 2 {
		t.Fatalf("unexpected search benchmarks: %+v", res.SearchBenchmarks)
	}
	for _, concurrency := range []int{1, 2} {
		for _, kind := range []string{"cpu", "heap", "allocs", "block", "mutex"} {
			path := filepath.Join(profileDir, fmt.Sprintf("search_quantized_rerank_c%d_%s.pprof", concurrency, kind))
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat profile %s: %v", path, err)
			}
			if info.Size() == 0 {
				t.Fatalf("profile %s is empty", path)
			}
		}
	}
}

func TestExecuteMatrixSearchProfileDirUsesCaseSubdirectories(t *testing.T) {
	profileDir := t.TempDir()
	res, err := executeMatrix(context.Background(), config{
		docs:                  48,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
		validateQueries:       0,
		validateDocs:          0,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0,
		disableExactFallback:  true,
		vectorIndexStrategy:   collections.VectorIndexStrategyColumnGraph,
		searchProfileDir:      filepath.Join(profileDir, "."),
	})
	if err != nil {
		t.Fatalf("executeMatrix with search profile dir: %v", err)
	}
	if res.SearchProfileDir != profileDir {
		t.Fatalf("matrix search profile dir=%q want %q", res.SearchProfileDir, profileDir)
	}
	if len(res.Cases) != 3 {
		t.Fatalf("matrix cases=%d want 3", len(res.Cases))
	}
	for _, testCase := range res.Cases {
		caseProfileDir := filepath.Join(profileDir, testCase.Name)
		if testCase.Result.SearchProfileDir != caseProfileDir {
			t.Fatalf("case %s profile dir=%q want %q", testCase.Name, testCase.Result.SearchProfileDir, caseProfileDir)
		}
		for _, concurrency := range []int{1, 2} {
			for _, kind := range []string{"cpu", "heap", "allocs", "block", "mutex"} {
				path := filepath.Join(caseProfileDir, fmt.Sprintf("search_exact_c%d_%s.pprof", concurrency, kind))
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat profile %s: %v", path, err)
				}
				if info.Size() == 0 {
					t.Fatalf("profile %s is empty", path)
				}
			}
		}
	}
	entries, err := os.ReadDir(profileDir)
	if err != nil {
		t.Fatalf("read profile dir: %v", err)
	}
	if len(entries) != len(res.Cases) {
		t.Fatalf("profile dir entries=%d want %d case directories", len(entries), len(res.Cases))
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			t.Fatalf("profile root entry %s is not a directory", entry.Name())
		}
	}
}

func TestDemoCommandWALFormatConfigPreservesProfileKnobs(t *testing.T) {
	opts := demoBackendOptions(config{}, t.TempDir())
	cfg := demoCommandWALFormatConfig(opts)
	if cfg.DurabilityProfile != opts.ResolvedProfile {
		t.Fatalf("durability profile=%q want %q", cfg.DurabilityProfile, opts.ResolvedProfile)
	}
	if len(cfg.RequiredFeatures) != 1 || cfg.RequiredFeatures[0] != "command_wal_v2" {
		t.Fatalf("required features=%v, want command_wal_v2", cfg.RequiredFeatures)
	}
	if !cfg.IndexOuterLeavesInValueLog || !cfg.LeafPrefixCompression || !cfg.IndexColumnarLeaves || !cfg.IndexPackedValuePtr || cfg.IndexInternalBaseDelta {
		t.Fatalf("index format config lost bench profile knobs: %+v", cfg)
	}
	if cfg.ValueLogCompression != "auto" || cfg.ValueLogBlockCodec != "snappy" || cfg.ValueLogAutoPolicy != "balanced" {
		t.Fatalf("value-log format config lost bench profile knobs: %+v", cfg)
	}
}

func TestExecuteConsumesDatasetDir(t *testing.T) {
	datasetDir := writeDemoDataset(t, 64, 8, 4, 3)
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		datasetDir:            datasetDir,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               4,
		searchConcurrency:     []int{2},
		validateQueries:       2,
		validateDocs:          2,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute with dataset: %v", err)
	}
	if res.DatasetDir != datasetDir {
		t.Fatalf("dataset_dir=%q want %q", res.DatasetDir, datasetDir)
	}
	if res.Validation.DocumentsChecked != 2 || res.Validation.QueriesChecked != 2 {
		t.Fatalf("validation counts=%+v, want 2 docs and 2 queries", res.Validation)
	}
	if res.Search.Queries != 4 {
		t.Fatalf("search queries=%d want 4", res.Search.Queries)
	}
}

func TestExecuteDatasetExactValidationNativeRuntime(t *testing.T) {
	datasetDir := writeDemoDataset(t, 96, 8, 6, 3)
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		datasetDir:            datasetDir,
		keepDir:               true,
		docs:                  96,
		dimensions:            8,
		queries:               4,
		searchConcurrency:     []int{2},
		validateQueries:       3,
		validateDocs:          2,
		validationExactSource: validationExactSourceDataset,
		topK:                  3,
		batchSize:             24,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute dataset exact native_runtime: %v", err)
	}
	if res.ValidationExactSource != validationExactSourceDataset || res.Validation.ExactSource != validationExactSourceDataset {
		t.Fatalf("validation exact source result=%q validation=%q, want dataset", res.ValidationExactSource, res.Validation.ExactSource)
	}
	if res.Validation.ExactTotal != 9 || res.Validation.ANNTotal == 0 {
		t.Fatalf("unexpected dataset exact validation totals: %+v", res.Validation)
	}
	if res.Validation.Recall < res.Validation.MinRecall {
		t.Fatalf("recall=%f below min=%f", res.Validation.Recall, res.Validation.MinRecall)
	}
}

func TestExecuteColumnGraphDatasetExactValidationThreadsQueryMode(t *testing.T) {
	datasetDir := writeDemoDataset(t, 96, 8, 6, 3)
	res, err := execute(context.Background(), config{
		dir:                       t.TempDir(),
		datasetDir:                datasetDir,
		keepDir:                   true,
		docs:                      96,
		dimensions:                8,
		queries:                   4,
		searchConcurrency:         []int{2},
		validateQueries:           3,
		validateDocs:              2,
		validationExactSource:     validationExactSourceDataset,
		topK:                      3,
		batchSize:                 24,
		m:                         4,
		efConstruction:            32,
		efSearch:                  32,
		valuePointerThreshold:     defaultValuePointerThreshold,
		leafGenerationTarget:      defaultLeafGenerationTarget,
		minRecall:                 0,
		disableExactFallback:      true,
		vectorIndexStrategy:       collections.VectorIndexStrategyColumnGraph,
		vectorQueryMode:           collections.VectorIndexQueryModeQuantizedRerank,
		quantizedIndexName:        defaultQuantizedIndexName,
		quantizedRerankCandidates: 8,
	})
	if err != nil {
		t.Fatalf("execute column_graph dataset exact quantized_rerank: %v", err)
	}
	if res.ValidationExactSource != validationExactSourceDataset || res.Validation.ExactSource != validationExactSourceDataset {
		t.Fatalf("validation exact source result=%q validation=%q, want dataset", res.ValidationExactSource, res.Validation.ExactSource)
	}
	if res.QueryMode != collections.VectorIndexQueryModeQuantizedRerank || res.Search.QueryMode != collections.VectorIndexQueryModeQuantizedRerank {
		t.Fatalf("query mode not threaded: result=%q search=%q", res.QueryMode, res.Search.QueryMode)
	}
	if res.Validation.ExactTotal != 9 || res.Validation.ANNTotal == 0 {
		t.Fatalf("unexpected dataset exact validation totals: %+v", res.Validation)
	}
	if res.Search.AvgQuantizedRerankExactScoreCalls <= 0 {
		t.Fatalf("quantized_rerank search stats missing exact rerank: %+v", res.Search)
	}
}

func TestExecuteColumnGraphSearchPath(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               4,
		searchConcurrency:     []int{2},
		validateQueries:       2,
		validateDocs:          2,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
		vectorIndexStrategy:   collections.VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("execute column_graph: %v", err)
	}
	if res.Backend != "treedb_column_graph" {
		t.Fatalf("backend=%q want treedb_column_graph", res.Backend)
	}
	if res.VectorIndexStrategy != collections.VectorIndexStrategyColumnGraph {
		t.Fatalf("strategy=%q want column_graph", res.VectorIndexStrategy)
	}
	if res.VectorIndexSearchPath != collections.VectorIndexSearchPathColumnGraphNativeReader {
		t.Fatalf("search path=%q want %q", res.VectorIndexSearchPath, collections.VectorIndexSearchPathColumnGraphNativeReader)
	}
	if res.QueryMode != collections.VectorIndexQueryModeExact || res.QuantizedIndexName != "" || res.QuantizedRerankCandidates != 0 {
		t.Fatalf("exact query config leaked quantized settings: mode=%q name=%q rerank=%d", res.QueryMode, res.QuantizedIndexName, res.QuantizedRerankCandidates)
	}
	if res.Search.AvgQuantizedScoreCalls != 0 || res.Search.AvgQuantizedCodeBytes != 0 || res.Search.AvgQuantizedRerankCandidates != 0 {
		t.Fatalf("exact search reported quantized counters: %+v", res.Search)
	}
	if res.Validation.Recall < res.Validation.MinRecall {
		t.Fatalf("recall=%f below min=%f", res.Validation.Recall, res.Validation.MinRecall)
	}
	if res.Search.Queries != 4 || res.Search.ExactFallbacks != 0 {
		t.Fatalf("unexpected column_graph search result: %+v", res.Search)
	}
	if requireFloat64Metric(t, res.Search.AvgResponseOwnedResultAllocs, "avg_response_owned_result_allocs") != 0 || requireFloat64Metric(t, res.Search.AvgDocumentsFetched, "avg_documents_fetched") != 0 {
		t.Fatalf("column_graph benchmark should use no-doc buffered results: %+v", res.Search)
	}
	if requireFloat64Metric(t, res.Search.AvgSearchRouteHNSWSearchPack, "avg_search_route_hnsw_search_pack") != 1 || requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedOnly, "avg_search_route_quantized_only") != 0 || requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedRerank, "avg_search_route_quantized_rerank") != 0 {
		t.Fatalf("unexpected exact column_graph route counters: %+v", res.Search)
	}
	if requireFloat64Metric(t, res.Search.AvgGraphRowFallbacks, "avg_graph_row_fallbacks") != 0 || requireFloat64Metric(t, res.Search.AvgTypedColumnFallbacks, "avg_typed_column_fallbacks") != 0 || requireFloat64Metric(t, res.Search.AvgVectorScratchDecodes, "avg_vector_scratch_decodes") != 0 {
		t.Fatalf("unexpected exact column_graph fallback counters: %+v", res.Search)
	}
	var out bytes.Buffer
	printText(&out, res)
	if !strings.Contains(out.String(), "avg_response_owned_result_allocs=0.0") {
		t.Fatalf("column_graph text output missing buffered guardrails:\n%s", out.String())
	}
}

func TestExecuteColumnGraphQuantizedModes(t *testing.T) {
	codecCases := []struct {
		name             string
		codec            string
		indexName        string
		wantBackendCodec string
	}{
		{
			name:             "scalar_u8_default",
			indexName:        defaultQuantizedIndexName,
			wantBackendCodec: collections.QuantizedVectorCodecScalarU8,
		},
		{
			name:             "rabitq_1bit",
			codec:            quantizedVectorCodecRabitQ1Bit,
			indexName:        defaultRabitQQuantizedIndexName,
			wantBackendCodec: quantizedVectorCodecRabitQ1Bit,
		},
	}
	modeCases := []struct {
		name        string
		mode        collections.VectorIndexQueryMode
		rerank      int
		assertStats func(t *testing.T, res result)
	}{
		{
			name: "quantized_only",
			mode: collections.VectorIndexQueryModeQuantizedOnly,
			assertStats: func(t *testing.T, res result) {
				t.Helper()
				if res.Search.AvgQuantizedScoreCalls <= 0 || res.Search.AvgQuantizedCodeBytes <= 0 {
					t.Fatalf("quantized_only stats missing quantized scoring: %+v", res.Search)
				}
				if res.Search.AvgQuantizedRerankCandidates != 0 || res.Search.AvgQuantizedRerankExactScoreCalls != 0 {
					t.Fatalf("quantized_only unexpectedly reranked: %+v", res.Search)
				}
				if res.Search.AvgVectorBytes != 0 || res.Search.AvgNormBytes != 0 {
					t.Fatalf("quantized_only unexpectedly read exact vectors/norms: %+v", res.Search)
				}
				wantPackRoute := res.QuantizedCodec == quantizedVectorCodecRabitQ1Bit
				if requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedOnly, "avg_search_route_quantized_only") != 1 || requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedRerank, "avg_search_route_quantized_rerank") != 0 {
					t.Fatalf("quantized_only query-mode route counters not isolated: %+v", res.Search)
				}
				if got := requireFloat64Metric(t, res.Search.AvgSearchRouteHNSWSearchPack, "avg_search_route_hnsw_search_pack"); (got == 1) != wantPackRoute {
					t.Fatalf("quantized_only pack route=%v want_pack=%v stats=%+v", got, wantPackRoute, res.Search)
				}
				if wantPackRoute {
					if requireFloat64Metric(t, res.Search.AvgSearchRouteColumnGraphPrepared, "avg_search_route_column_graph_prepared") != 0 || requireFloat64Metric(t, res.Search.AvgSearchRouteColumnGraphFallback, "avg_search_route_column_graph_fallback") != 0 {
						t.Fatalf("rabitq quantized_only should use pack route only: %+v", res.Search)
					}
				}
			},
		},
		{
			name:   "quantized_rerank",
			mode:   collections.VectorIndexQueryModeQuantizedRerank,
			rerank: 8,
			assertStats: func(t *testing.T, res result) {
				t.Helper()
				if res.Search.AvgQuantizedScoreCalls <= 0 || res.Search.AvgQuantizedCodeBytes <= 0 {
					t.Fatalf("quantized_rerank stats missing quantized scoring: %+v", res.Search)
				}
				if res.Search.AvgQuantizedRerankCandidates <= 0 || res.Search.AvgQuantizedRerankExactScoreCalls <= 0 {
					t.Fatalf("quantized_rerank stats missing exact rerank: %+v", res.Search)
				}
				if res.Search.AvgQuantizedRerankExactScoreCalls > float64(res.QuantizedRerankCandidates) {
					t.Fatalf("quantized_rerank exact score calls exceeded limit: %+v", res.Search)
				}
				if res.Search.AvgVectorBytes <= 0 {
					t.Fatalf("quantized_rerank stats missing exact vector reads: %+v", res.Search)
				}
				wantPackRoute := res.QuantizedCodec == quantizedVectorCodecRabitQ1Bit
				if wantPackRoute {
					if res.Search.AvgNormBytes != 0 {
						t.Fatalf("rabitq quantized_rerank should rerank from pack vectors without norm reads: %+v", res.Search)
					}
				} else if res.Search.AvgNormBytes <= 0 {
					t.Fatalf("quantized_rerank stats missing exact norm reads: %+v", res.Search)
				}
				maxVectorBytes := float64(res.QuantizedRerankCandidates * res.Dimensions * 4)
				maxNormBytes := float64(res.QuantizedRerankCandidates * 4)
				if wantPackRoute {
					maxNormBytes = 0
				}
				if res.Search.AvgVectorBytes > maxVectorBytes || res.Search.AvgNormBytes > maxNormBytes {
					t.Fatalf("quantized_rerank exact reads exceeded rerank bound: %+v", res.Search)
				}
				if requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedOnly, "avg_search_route_quantized_only") != 0 || requireFloat64Metric(t, res.Search.AvgSearchRouteQuantizedRerank, "avg_search_route_quantized_rerank") != 1 {
					t.Fatalf("quantized_rerank query-mode route counters not isolated: %+v", res.Search)
				}
				if got := requireFloat64Metric(t, res.Search.AvgSearchRouteHNSWSearchPack, "avg_search_route_hnsw_search_pack"); (got == 1) != wantPackRoute {
					t.Fatalf("quantized_rerank pack route=%v want_pack=%v stats=%+v", got, wantPackRoute, res.Search)
				}
				if wantPackRoute {
					if requireFloat64Metric(t, res.Search.AvgSearchRouteColumnGraphPrepared, "avg_search_route_column_graph_prepared") != 0 || requireFloat64Metric(t, res.Search.AvgSearchRouteColumnGraphFallback, "avg_search_route_column_graph_fallback") != 0 {
						t.Fatalf("rabitq quantized_rerank should use pack route only: %+v", res.Search)
					}
				}
			},
		},
	}
	for _, codecCase := range codecCases {
		for _, tc := range modeCases {
			t.Run(codecCase.name+"/"+tc.name, func(t *testing.T) {
				res, err := execute(context.Background(), config{
					dir:                       t.TempDir(),
					keepDir:                   true,
					docs:                      96,
					dimensions:                8,
					queries:                   4,
					searchConcurrency:         []int{2},
					validateQueries:           2,
					validateDocs:              2,
					topK:                      3,
					batchSize:                 32,
					m:                         4,
					efConstruction:            32,
					efSearch:                  32,
					valuePointerThreshold:     defaultValuePointerThreshold,
					leafGenerationTarget:      defaultLeafGenerationTarget,
					minRecall:                 0,
					disableExactFallback:      true,
					vectorIndexStrategy:       collections.VectorIndexStrategyColumnGraph,
					vectorQueryMode:           tc.mode,
					quantizedCodec:            codecCase.codec,
					quantizedIndexName:        codecCase.indexName,
					quantizedRerankCandidates: tc.rerank,
				})
				if err != nil {
					t.Fatalf("execute %s/%s: %v", codecCase.name, tc.name, err)
				}
				wantBackend := "treedb_column_graph_" + codecCase.wantBackendCodec + "_" + tc.name
				if res.Backend != wantBackend {
					t.Fatalf("backend=%q want %s", res.Backend, wantBackend)
				}
				if res.QueryMode != tc.mode || res.QuantizedCodec != codecCase.wantBackendCodec {
					t.Fatalf("query config mode=%q codec=%q", res.QueryMode, res.QuantizedCodec)
				}
				if res.QuantizedIndexName != codecCase.indexName || res.QuantizedRerankCandidates != tc.rerank {
					t.Fatalf("quantized config name=%q rerank=%d", res.QuantizedIndexName, res.QuantizedRerankCandidates)
				}
				if res.Search.ExactFallbacks != 0 || !res.Search.DisableExactFallback {
					t.Fatalf("quantized row did not stay fail-closed: %+v", res.Search)
				}
				if len(res.SearchBenchmarks) != 2 || res.SearchBenchmarks[0].QueryMode != tc.mode || res.SearchBenchmarks[1].QueryMode != tc.mode || res.SearchBenchmarks[0].QuantizedCodec != codecCase.wantBackendCodec || res.SearchBenchmarks[1].QuantizedCodec != codecCase.wantBackendCodec {
					t.Fatalf("search benchmark modes/codecs not threaded: %+v", res.SearchBenchmarks)
				}
				if requireFloat64Metric(t, res.Search.AvgResponseOwnedResultAllocs, "avg_response_owned_result_allocs") != 0 || requireFloat64Metric(t, res.Search.AvgDocumentsFetched, "avg_documents_fetched") != 0 {
					t.Fatalf("quantized benchmark should use no-doc buffered results: %+v", res.Search)
				}
				if requireFloat64Metric(t, res.Search.AvgGraphRowFallbacks, "avg_graph_row_fallbacks") != 0 || requireFloat64Metric(t, res.Search.AvgTypedColumnFallbacks, "avg_typed_column_fallbacks") != 0 || requireFloat64Metric(t, res.Search.AvgVectorScratchDecodes, "avg_vector_scratch_decodes") != 0 {
					t.Fatalf("unexpected quantized fallback counters: %+v", res.Search)
				}
				var out bytes.Buffer
				printText(&out, res)
				for _, want := range []string{"backend=" + wantBackend, "quantized_codec=" + codecCase.wantBackendCodec, "quantized_index_name=" + codecCase.indexName} {
					if !strings.Contains(out.String(), want) {
						t.Fatalf("text output missing %q:\n%s", want, out.String())
					}
				}
				tc.assertStats(t, res)
			})
		}
	}
}

func TestExecuteColumnGraphRecallBenchmarkShape(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  1000,
		dimensions:            64,
		queries:               4,
		searchConcurrency:     []int{2},
		validateQueries:       32,
		validateDocs:          2,
		topK:                  10,
		batchSize:             128,
		m:                     16,
		efConstruction:        128,
		efSearch:              128,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.95,
		disableExactFallback:  true,
		vectorIndexStrategy:   collections.VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("execute column_graph benchmark shape: %v", err)
	}
	if res.Validation.Recall < 0.95 {
		t.Fatalf("recall=%f want >=0.95", res.Validation.Recall)
	}
	if res.Search.AvgCandidates < float64(res.EfSearch) {
		t.Fatalf("avg candidates=%f want at least ef_search=%d", res.Search.AvgCandidates, res.EfSearch)
	}
}

func TestExecuteDatasetDirClampsValidateQueries(t *testing.T) {
	datasetDir := writeDemoDataset(t, 64, 8, 2, 3)
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		datasetDir:            datasetDir,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
		validateQueries:       64,
		validateDocs:          1,
		topK:                  3,
		batchSize:             16,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("execute with dataset: %v", err)
	}
	if res.ValidateQueries != 2 || res.Validation.QueriesChecked != 2 {
		t.Fatalf("validate queries result=%d validation=%+v, want clamped to 2", res.ValidateQueries, res.Validation)
	}
}

func TestLoadWorkloadAcceptsPartialExactTruthCoverage(t *testing.T) {
	datasetDir := writeDemoDataset(t, 64, 8, 5, 3)
	cfg := config{datasetDir: datasetDir, docs: 64, dimensions: 8, queries: 5, validateQueries: 4}
	work, err := loadWorkload(&cfg)
	if err != nil {
		t.Fatalf("partial exact-truth coverage rejected: %v", err)
	}
	if work.manifest.ExactTruthQueries != 1 || work.manifest.ExactTruthQueries == work.manifest.Queries {
		t.Fatalf("consumer did not preserve partial exact-truth coverage: %+v", work.manifest)
	}
	if cfg.validateQueries != 4 {
		t.Fatalf("consumer incorrectly clamped validation to truth rows: %d", cfg.validateQueries)
	}
}

func TestRunJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-matrix=false",
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "64",
		"-dims", "8",
		"-queries", "4",
		"-validate-queries", "2",
		"-validate-docs", "2",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
		"-json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v stderr=%s", err, stderr.String())
	}
	var res result
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, stdout.String())
	}
	if res.Profile != "bench_unsafe" || res.Docs != 64 || res.Search.Queries != 4 || res.StorageAfterCompact.TotalBytes <= 0 {
		t.Fatalf("unexpected JSON result: %+v", res)
	}
	if res.VectorIndexSearchPath != nativeRuntimeSnapshotPath {
		t.Fatalf("JSON vector index search path=%q want %q", res.VectorIndexSearchPath, nativeRuntimeSnapshotPath)
	}
	if res.ValuePointerThreshold != defaultValuePointerThreshold {
		t.Fatalf("JSON value pointer threshold=%d want %d", res.ValuePointerThreshold, defaultValuePointerThreshold)
	}
	if res.LeafGenerationTarget != defaultLeafGenerationTarget {
		t.Fatalf("JSON leaf generation target=%d want %d", res.LeafGenerationTarget, defaultLeafGenerationTarget)
	}
	if res.FormatConfig == nil || !res.FormatConfig.IndexOuterLeavesInValueLog || !res.FormatConfig.LeafPrefixCompression {
		t.Fatalf("unexpected JSON format config: %+v", res.FormatConfig)
	}
	if res.MinRecall != 0.5 || res.Compact || !res.DisableExactFallback {
		t.Fatalf("missing reproducibility config in JSON result: %+v", res)
	}
}

func TestExecuteRequireLeafVLogBytesPassesWithDefaultBenchProfile(t *testing.T) {
	res, err := execute(context.Background(), config{
		dir:                   t.TempDir(),
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             32,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
		requireLeafVLogBytes:  true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Profile != "bench_unsafe" {
		t.Fatalf("profile=%q want bench_unsafe", res.Profile)
	}
	if res.StorageExpectation.LeafVLogBytes <= 0 {
		t.Fatalf("missing leaf value-log bytes: %+v", res.StorageExpectation)
	}
}

func TestExecuteRejectsMainDBDir(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo-root")
	maindb := filepath.Join(root, "maindb")
	if err := os.MkdirAll(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir stale dictdb: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "dictdb", "stale"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale side-store file: %v", err)
	}

	_, err := execute(context.Background(), config{
		dir:                   maindb,
		keepDir:               true,
		docs:                  64,
		dimensions:            8,
		queries:               2,
		searchConcurrency:     []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             32,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err == nil {
		t.Fatal("execute accepted maindb path, want error")
	}
	if !strings.Contains(err.Error(), "TreeDB root directory") {
		t.Fatalf("error=%v, want TreeDB root directory guidance", err)
	}
	if _, err := os.Stat(filepath.Join(root, "dictdb", "stale")); err != nil {
		t.Fatalf("stale side-store file err=%v, want untouched after rejected maindb path", err)
	}
}

func TestParseProfileRejectsUnsupportedProfile(t *testing.T) {
	_, err := parseProfile("raw")
	if err == nil {
		t.Fatal("parseProfile succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unsupported -profile") {
		t.Fatalf("error=%v, want unsupported -profile", err)
	}
}

func TestParseProfileRejectsDeprecatedProfiles(t *testing.T) {
	if got, err := parseProfile("command_wal_durable"); err != nil || got != treedb.ProfileCommandWALDurable {
		t.Fatalf("parseProfile command WAL durable = %q err=%v", got, err)
	}
	for _, raw := range []string{"fast", "wal_on_fast", "durable", "legacy_wal_durable", "legacy_wal_relaxed_fast", "bench", "command-wal-durable"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseProfile(raw)
			if err == nil {
				t.Fatal("parseProfile succeeded, want error")
			}
			if !strings.Contains(err.Error(), treedb.BenchmarkProfileFlagHelp) {
				t.Fatalf("error=%v, want profile help", err)
			}
		})
	}
}

func TestParseConfigValuePointerThreshold(t *testing.T) {
	cfg, err := parseConfig([]string{"-value-pointer-threshold", "2048"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.valuePointerThreshold != 2048 {
		t.Fatalf("value pointer threshold=%d want 2048", cfg.valuePointerThreshold)
	}
	if _, err := parseConfig([]string{"-value-pointer-threshold", "-1"}); err == nil {
		t.Fatal("parseConfig accepted negative value pointer threshold")
	}
}

func TestParseConfigVectorQueryMode(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-matrix=false",
		"-vector-index-strategy", "column_graph",
		"-vector-query-mode", "quantized_rerank",
		"-quantized-codec", " RaBiTQ_1BiT ",
		"-quantized-index-name", defaultRabitQQuantizedIndexName,
		"-quantized-rerank-candidates", "32",
	})
	if err != nil {
		t.Fatalf("parseConfig quantized_rerank: %v", err)
	}
	if cfg.vectorQueryMode != collections.VectorIndexQueryModeQuantizedRerank || cfg.quantizedCodec != quantizedVectorCodecRabitQ1Bit || cfg.quantizedIndexName != defaultRabitQQuantizedIndexName || cfg.quantizedRerankCandidates != 32 {
		t.Fatalf("parsed quantized config=%+v", cfg)
	}
	defaultCodecCfg, err := parseConfig([]string{
		"-matrix=false",
		"-vector-index-strategy", "column_graph",
		"-vector-query-mode", "quantized_only",
		"-quantized-index-name", defaultQuantizedIndexName,
		"-min-recall", "0",
	})
	if err != nil {
		t.Fatalf("parseConfig default quantized codec: %v", err)
	}
	if defaultCodecCfg.quantizedCodec != defaultQuantizedCodec {
		t.Fatalf("default quantized codec=%q want %q", defaultCodecCfg.quantizedCodec, defaultQuantizedCodec)
	}

	datasetCfg, err := parseConfig([]string{
		"-dataset-dir", filepath.Join("tmp", "dataset"),
		"-validation-exact-source", " Dataset ",
	})
	if err != nil {
		t.Fatalf("parseConfig dataset exact source: %v", err)
	}
	if datasetCfg.validationExactSource != validationExactSourceDataset {
		t.Fatalf("validation exact source=%q want dataset", datasetCfg.validationExactSource)
	}
	defaultCfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig defaults: %v", err)
	}
	if defaultCfg.validationExactSource != validationExactSourceTreeDB {
		t.Fatalf("default validation exact source=%q want treedb", defaultCfg.validationExactSource)
	}
	profileCfg, err := parseConfig([]string{
		"-vector-index-strategy", "column_graph",
		"-search-profile-dir", filepath.Join("tmp", ".", "profiles"),
	})
	if err != nil {
		t.Fatalf("parseConfig search profile dir: %v", err)
	}
	if profileCfg.searchProfileDir != filepath.Join("tmp", "profiles") {
		t.Fatalf("search profile dir=%q want tmp/profiles", profileCfg.searchProfileDir)
	}

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "invalid mode",
			args: []string{"-vector-query-mode", "binary_quantize"},
			want: "unsupported -vector-query-mode",
		},
		{
			name: "invalid validation source",
			args: []string{"-validation-exact-source", "pgvector"},
			want: "unsupported -validation-exact-source",
		},
		{
			name: "search profile dir requires column graph",
			args: []string{"-search-profile-dir", filepath.Join("tmp", "profiles")},
			want: "requires -vector-index-strategy column_graph",
		},
		{
			name: "dataset validation source requires dataset dir",
			args: []string{"-validation-exact-source", "dataset"},
			want: "requires -dataset-dir",
		},
		{
			name: "quantized requires column graph",
			args: []string{"-vector-query-mode", "quantized_only", "-quantized-index-name", defaultQuantizedIndexName},
			want: "require -vector-index-strategy column_graph",
		},
		{
			name: "quantized requires index name",
			args: []string{"-vector-index-strategy", "column_graph", "-vector-query-mode", "quantized_only"},
			want: "-quantized-index-name is required",
		},
		{
			name: "exact rejects quantized codec",
			args: []string{"-vector-query-mode", "exact", "-quantized-codec", quantizedVectorCodecRabitQ1Bit},
			want: "-quantized-codec requires",
		},
		{
			name: "exact rejects quantized name",
			args: []string{"-vector-query-mode", "exact", "-quantized-index-name", defaultQuantizedIndexName},
			want: "-quantized-index-name requires",
		},
		{
			name: "rejects unsupported quantized codec",
			args: []string{"-vector-index-strategy", "column_graph", "-vector-query-mode", "quantized_only", "-quantized-codec", "binary_quantize", "-quantized-index-name", defaultQuantizedIndexName},
			want: "unsupported -quantized-codec",
		},
		{
			name: "quantized only rejects rerank candidates",
			args: []string{"-vector-index-strategy", "column_graph", "-vector-query-mode", "quantized_only", "-quantized-index-name", defaultQuantizedIndexName, "-quantized-rerank-candidates", "4"},
			want: "requires -vector-query-mode quantized_rerank",
		},
		{
			name: "rerank candidates below top k",
			args: []string{"-vector-index-strategy", "column_graph", "-vector-query-mode", "quantized_rerank", "-quantized-index-name", defaultQuantizedIndexName, "-quantized-rerank-candidates", "2", "-top-k", "3"},
			want: "cannot be less than -top-k",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args)
			if err == nil {
				t.Fatal("parseConfig succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseConfigLeafGenerationSegmentTarget(t *testing.T) {
	cfg, err := parseConfig([]string{"-leaf-generation-segment-target", "8388608"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.leafGenerationTarget != 8388608 {
		t.Fatalf("leaf generation target=%d want 8388608", cfg.leafGenerationTarget)
	}
	if _, err := parseConfig([]string{"-leaf-generation-segment-target", "-1"}); err == nil {
		t.Fatal("parseConfig accepted negative leaf generation target")
	}
}

func TestRunMatrixJSONOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "48",
		"-dims", "8",
		"-queries", "2",
		"-search-concurrency", "2,4",
		"-validate-queries", "1",
		"-validate-docs", "1",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
		"-json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run matrix: %v stderr=%s", err, stderr.String())
	}
	var res matrixResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("decode matrix JSON output: %v\n%s", err, stdout.String())
	}
	if len(res.Cases) != 3 {
		t.Fatalf("matrix cases=%d want 3", len(res.Cases))
	}
	wantNames := []string{"index_db_outer_leaves", "leaf_vlog_before_compact", "leaf_vlog_after_compact"}
	for i, want := range wantNames {
		if res.Cases[i].Name != want {
			t.Fatalf("case %d name=%q want %q", i, res.Cases[i].Name, want)
		}
		searches := res.Cases[i].Result.SearchBenchmarks
		if len(searches) != 3 || searches[0].Concurrency != 1 || searches[1].Concurrency != 2 || searches[2].Concurrency != 4 {
			t.Fatalf("case %s search benchmarks=%+v", want, searches)
		}
	}
	if res.Cases[0].Result.FormatConfig == nil || res.Cases[0].Result.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("index_db_outer_leaves format=%+v, want outer leaves in index.db", res.Cases[0].Result.FormatConfig)
	}
	if res.Cases[1].Result.FormatConfig == nil || !res.Cases[1].Result.FormatConfig.IndexOuterLeavesInValueLog {
		t.Fatalf("leaf_vlog_before_compact format=%+v, want outer leaves in leaf_vlog", res.Cases[1].Result.FormatConfig)
	}
	if res.Cases[2].Result.CompactStorage == nil || !res.Cases[2].Result.CompactStorage.FullyCompacted {
		t.Fatalf("leaf_vlog_after_compact compact stats=%+v", res.Cases[2].Result.CompactStorage)
	}
}

func TestExecuteMatrixReportsNestedKeptDirFromRoot(t *testing.T) {
	res, err := executeMatrix(context.Background(), config{
		docs:                  24,
		dimensions:            8,
		queries:               1,
		searchConcurrency:     []int{2},
		validateQueries:       1,
		validateDocs:          1,
		topK:                  3,
		batchSize:             12,
		m:                     4,
		efConstruction:        32,
		efSearch:              32,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.5,
		compact:               true,
		disableExactFallback:  true,
	})
	if err != nil {
		t.Fatalf("executeMatrix: %v", err)
	}
	if res.KeptDir {
		t.Fatalf("matrix kept_dir=true, want false")
	}
	for _, testCase := range res.Cases {
		if testCase.Result.KeptDir {
			t.Fatalf("case %s kept_dir=true, want false inherited from matrix root", testCase.Name)
		}
	}
	if _, err := os.Stat(res.Dir); !os.IsNotExist(err) {
		t.Fatalf("matrix dir stat err=%v, want removed temp dir", err)
	}
}

func TestExecuteRequireValueLogBytesFailsOnPagerBackedDefault(t *testing.T) {
	_, err := execute(context.Background(), config{
		dir:                  t.TempDir(),
		keepDir:              true,
		docs:                 64,
		dimensions:           8,
		queries:              2,
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
		requireValueLogBytes: true,
	})
	if err == nil {
		t.Fatal("execute succeeded, want value-log requirement failure")
	}
	// These assertions intentionally describe the current default backend
	// profile, where this demo does not force value-log storage.
	if !strings.Contains(err.Error(), "zero value_vlog bytes") {
		t.Fatalf("error=%v, want zero value_vlog bytes", err)
	}
}

func TestExecuteRejectsNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	_, err := execute(context.Background(), config{
		dir:                  dir,
		keepDir:              true,
		docs:                 64,
		dimensions:           8,
		queries:              2,
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
	})
	if err == nil {
		t.Fatal("execute accepted non-empty -dir, want error")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("error=%v, want not-empty directory error", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatalf("unrelated file err=%v, want untouched", err)
	}
}

func TestExecuteMatrixRejectsNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "unrelated"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}
	_, err := executeMatrix(context.Background(), config{
		dir:                  dir,
		docs:                 24,
		dimensions:           8,
		queries:              1,
		searchConcurrency:    []int{2},
		validateQueries:      1,
		validateDocs:         1,
		topK:                 3,
		batchSize:            12,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              true,
		disableExactFallback: true,
	})
	if err == nil {
		t.Fatal("executeMatrix accepted non-empty -dir, want error")
	}
	if !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("error=%v, want not-empty directory error", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated")); err != nil {
		t.Fatalf("unrelated file err=%v, want untouched", err)
	}
}

func TestExecuteRemovesTemporaryDirWhenNotKept(t *testing.T) {
	res, err := execute(context.Background(), config{
		docs:                 64,
		dimensions:           8,
		queries:              4,
		validateQueries:      2,
		validateDocs:         2,
		topK:                 3,
		batchSize:            32,
		m:                    4,
		efConstruction:       32,
		efSearch:             32,
		minRecall:            0.5,
		compact:              false,
		disableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.KeptDir {
		t.Fatalf("KeptDir=%t want false", res.KeptDir)
	}
	if _, err := os.Stat(res.Dir); !os.IsNotExist(err) {
		t.Fatalf("temporary dir stat err=%v, want not exist", err)
	}
}

func TestParseConfigRejectsInvalidValidationCombinations(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "recall gate without validation queries",
			args: []string{"-validate-queries", "0"},
			want: "-min-recall must be 0",
		},
		{
			name: "topk exceeds docs",
			args: []string{"-docs", "2", "-top-k", "3"},
			want: "-top-k cannot exceed -docs",
		},
		{
			name: "synthetic validate queries exceeds docs",
			args: []string{"-docs", "2", "-top-k", "2", "-validate-queries", "3"},
			want: "-validate-queries cannot exceed -docs",
		},
		{
			name: "negative validate docs",
			args: []string{"-validate-docs", "-1"},
			want: "-validate-docs cannot be negative",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args)
			if err == nil {
				t.Fatal("parseConfig succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseConfigDatasetDirAllowsValidateQueriesAboveDocs(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-dataset-dir", filepath.Join("tmp", "dataset"),
		"-docs", "2",
		"-top-k", "2",
		"-validate-queries", "7",
		"-validate-docs", "2",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.validateQueries != 7 {
		t.Fatalf("validateQueries=%d want 7", cfg.validateQueries)
	}
	if cfg.datasetDir != filepath.Join("tmp", "dataset") {
		t.Fatalf("datasetDir=%q", cfg.datasetDir)
	}
}

func TestParseSearchConcurrencyAcceptsSerialAndPreservesOrder(t *testing.T) {
	got, err := parseSearchConcurrency("4,1,2,4,8")
	if err != nil {
		t.Fatalf("parseSearchConcurrency: %v", err)
	}
	want := []int{4, 2, 8}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("concurrency=%v want %v", got, want)
	}
}

func TestDemoDatasetJSONEmbeddingsMatchBinaryVectors(t *testing.T) {
	const docs = 8
	const dims = 4
	dir := writeDemoDataset(t, docs, dims, 2, 2)
	vectors, err := readFloat32Vectors(filepath.Join(dir, "documents.f32"), docs, dims)
	if err != nil {
		t.Fatalf("read documents.f32: %v", err)
	}
	f, err := os.Open(filepath.Join(dir, "documents.jsonl"))
	if err != nil {
		t.Fatalf("open documents.jsonl: %v", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for i := 0; scanner.Scan(); i++ {
		var doc struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &doc); err != nil {
			t.Fatalf("decode document %d: %v", i, err)
		}
		if doc.Index != i {
			t.Fatalf("document index=%d at row %d", doc.Index, i)
		}
		if !reflect.DeepEqual(doc.Embedding, vectors[i]) {
			t.Fatalf("document %d embedding=%v binary=%v", i, doc.Embedding, vectors[i])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan documents.jsonl: %v", err)
	}
}

func TestParseConfigDoesNotWriteFlagErrorsToProcessStderr(t *testing.T) {
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	originalStderr := os.Stderr
	os.Stderr = writePipe
	t.Cleanup(func() {
		os.Stderr = originalStderr
		_ = readPipe.Close()
	})
	_, err = parseConfig([]string{"-not-a-real-flag"})
	_ = writePipe.Close()
	if err == nil {
		t.Fatal("parseConfig accepted unknown flag")
	}
	var stderr bytes.Buffer
	if _, err := stderr.ReadFrom(readPipe); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("parseConfig wrote to stderr: %q", stderr.String())
	}
}

func TestSyntheticQueriesDoNotOverlapValidationQueries(t *testing.T) {
	docs := 97
	validateCount := 17
	benchmarkCount := 53
	stride := queryDocStride(docs)
	seen := make(map[int]struct{}, validateCount)
	for i := 0; i < validateCount; i++ {
		seen[syntheticQueryID(i, docs, 0, docs, stride)] = struct{}{}
	}
	benchmarkStart := validateCount
	benchmarkSpan := docs - benchmarkStart
	for i := 0; i < benchmarkCount; i++ {
		queryID := syntheticQueryID(i, docs, benchmarkStart, benchmarkSpan, stride)
		if _, ok := seen[queryID]; ok {
			t.Fatalf("benchmark query id %d overlapped validation set", queryID)
		}
	}
}

func TestSyntheticQueryIDSpillsPastCorpusAfterFullValidation(t *testing.T) {
	docs := 17
	stride := queryDocStride(docs)
	for i := 0; i < 5; i++ {
		if got := syntheticQueryID(i, docs, docs, 0, stride); got != docs+i {
			t.Fatalf("syntheticQueryID(%d)=%d want %d", i, got, docs+i)
		}
	}
}

func TestValidationDocIndexSamplesDistinctDocsWhenStrideWouldCollapse(t *testing.T) {
	docs := 1543
	seen := make(map[int]struct{}, 16)
	for i := 0; i < 16; i++ {
		docIndex := validationDocIndex(i, docs)
		if _, ok := seen[docIndex]; ok {
			t.Fatalf("validation doc index repeated %d for docs=%d", docIndex, docs)
		}
		seen[docIndex] = struct{}{}
	}
}

func TestRunTextOutput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"-matrix=false",
		"-dir", t.TempDir(),
		"-keep-dir",
		"-docs", "64",
		"-dims", "8",
		"-queries", "4",
		"-validate-queries", "2",
		"-validate-docs", "2",
		"-top-k", "3",
		"-m", "4",
		"-ef-construction", "32",
		"-ef-search", "32",
		"-min-recall", "0.5",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"TreeDB vector search demo",
		"profile=bench_unsafe",
		"backend=treedb",
		"vector_index_strategy=native_runtime",
		"vector_index_search_path=native_runtime_snapshot",
		"value_pointer_threshold=1024",
		"leaf_generation_segment_target=4194304",
		"Storage",
		"format index_outer_leaves_in_vlog=",
		"storage_domains index_db=",
		"Memory",
		"avg=",
		"Parallel Search Benchmark",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
}

func writeDemoDataset(t *testing.T, docs, dims, queries, topK int) string {
	t.Helper()
	dir := t.TempDir()
	m := map[string]any{
		"version":               1,
		"docs":                  docs,
		"dimensions":            dims,
		"queries":               queries,
		"exact_truth_queries":   min(1, queries),
		"top_k":                 topK,
		"metric":                "cosine",
		"document_vectors_file": "documents.f32",
		"query_vectors_file":    "queries.f32",
		"documents_jsonl_file":  "documents.jsonl",
		"float_format":          "float32_le_row_major",
	}
	manifest, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(manifest, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	writeTestVectorFile(t, filepath.Join(dir, "documents.f32"), docs, dims, func(i int) []float32 {
		return embedding(i, dims)
	})
	stride := queryDocStride(docs)
	writeTestVectorFile(t, filepath.Join(dir, "queries.f32"), queries, dims, func(i int) []float32 {
		return embedding(queryDocIndex(i, docs, stride), dims)
	})
	f, err := os.Create(filepath.Join(dir, "documents.jsonl"))
	if err != nil {
		t.Fatalf("create documents jsonl: %v", err)
	}
	enc := json.NewEncoder(f)
	for i := 0; i < docs; i++ {
		if err := enc.Encode(struct {
			Index     int       `json:"index"`
			ID        string    `json:"id"`
			Group     int       `json:"group"`
			Source    string    `json:"source"`
			Embedding []float32 `json:"embedding"`
		}{
			Index:     i,
			ID:        fmt.Sprintf("dataset-doc-%06d", i),
			Group:     i % 16,
			Source:    "exported-test-dataset",
			Embedding: embedding(i, dims),
		}); err != nil {
			_ = f.Close()
			t.Fatalf("write document %d: %v", i, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close documents jsonl: %v", err)
	}
	return dir
}

func writeTestVectorFile(t *testing.T, path string, count, dims int, vector func(int) []float32) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create vector file: %v", err)
	}
	for i := 0; i < count; i++ {
		v := vector(i)
		if len(v) != dims {
			_ = f.Close()
			t.Fatalf("vector %d dims=%d want %d", i, len(v), dims)
		}
		for _, value := range v {
			if err := binary.Write(f, binary.LittleEndian, value); err != nil {
				_ = f.Close()
				t.Fatalf("write vector %d: %v", i, err)
			}
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close vector file: %v", err)
	}
}
