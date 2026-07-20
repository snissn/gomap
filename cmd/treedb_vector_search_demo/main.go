package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultDocs                     = 10000
	defaultDimensions               = 64
	defaultQueries                  = 10000
	defaultSearchConcurrency        = "2,4,8,16,32,64,128"
	defaultTopK                     = 10
	defaultBatchSize                = 512
	defaultM                        = 16
	defaultEfConstruct              = 128
	defaultEfSearch                 = 128
	defaultValuePointerThreshold    = 1024
	defaultLeafGenerationTarget     = 4 << 20
	defaultQuantizedCodec           = collections.QuantizedVectorCodecScalarU8
	defaultQuantizedIndexName       = "embedding.scalar_u8.fast"
	defaultRabitQQuantizedIndexName = "embedding.rabitq_1bit.fast"
	quantizedVectorCodecRabitQ1Bit  = "rabitq_1bit"
	nativeRuntimeSnapshotPath       = collections.VectorIndexSearchPath("native_runtime_snapshot")
)

type validationExactSource string

const (
	validationExactSourceTreeDB  validationExactSource = "treedb"
	validationExactSourceDataset validationExactSource = "dataset"
)

type config struct {
	dir                   string
	datasetDir            string
	keepDir               bool
	matrix                bool
	profile               treedb.Profile
	docs                  int
	dimensions            int
	queries               int
	searchConcurrency     []int
	validateQueries       int
	validateDocs          int
	validationExactSource validationExactSource
	topK                  int
	batchSize             int
	m                     int
	efConstruction        int
	efSearch              int
	valuePointerThreshold int
	leafGenerationTarget  int64
	minRecall             float64
	compact               bool
	compactSyncEachPhase  bool
	vacuumIndex           bool
	disableExactFallback  bool
	requireValueLogBytes  bool
	requireLeafVLogBytes  bool
	searchProfileDir      string
	jsonOut               bool

	indexOuterLeavesInValueLog *bool
	vectorIndexStrategy        collections.VectorIndexStrategy
	vectorQueryMode            collections.VectorIndexQueryMode
	quantizedCodec             string
	quantizedIndexName         string
	quantizedRerankCandidates  int
}

type result struct {
	Backend                   string                            `json:"backend"`
	Engine                    string                            `json:"engine"`
	VectorIndexStrategy       collections.VectorIndexStrategy   `json:"vector_index_strategy"`
	VectorIndexSearchPath     collections.VectorIndexSearchPath `json:"vector_index_search_path,omitempty"`
	QueryMode                 collections.VectorIndexQueryMode  `json:"query_mode"`
	QuantizedCodec            string                            `json:"quantized_codec"`
	QuantizedIndexName        string                            `json:"quantized_index_name"`
	QuantizedRerankCandidates int                               `json:"quantized_rerank_candidates"`
	Dir                       string                            `json:"dir"`
	DatasetDir                string                            `json:"dataset_dir,omitempty"`
	KeptDir                   bool                              `json:"kept_dir"`
	Profile                   string                            `json:"profile"`
	Docs                      int                               `json:"docs"`
	Dimensions                int                               `json:"dimensions"`
	Queries                   int                               `json:"queries"`
	SearchConcurrency         []int                             `json:"search_concurrency"`
	ValidateQueries           int                               `json:"validate_queries"`
	ValidateDocs              int                               `json:"validate_docs"`
	ValidationExactSource     validationExactSource             `json:"validation_exact_source"`
	SearchProfileDir          string                            `json:"search_profile_dir,omitempty"`
	TopK                      int                               `json:"top_k"`
	M                         int                               `json:"m"`
	EfConstruction            int                               `json:"ef_construction"`
	EfSearch                  int                               `json:"ef_search"`
	ValuePointerThreshold     int                               `json:"value_pointer_threshold"`
	LeafGenerationTarget      int64                             `json:"leaf_generation_segment_target"`
	MinRecall                 float64                           `json:"min_recall"`
	Compact                   bool                              `json:"compact"`
	CompactSyncEachPhase      bool                              `json:"compact_sync_each_phase"`
	DisableExactFallback      bool                              `json:"disable_exact_fallback"`
	Insert                    phaseResult                       `json:"insert"`
	Rebuild                   phaseResult                       `json:"rebuild"`
	CompactPhase              phaseResult                       `json:"compact_phase"`
	IndexVacuum               phaseResult                       `json:"index_vacuum"`
	ReopenLoad                phaseResult                       `json:"reopen_load"`
	Validation                validationResult                  `json:"validation"`
	Search                    searchBenchmarkResult             `json:"search"`
	SearchBenchmarks          []searchBenchmarkResult           `json:"search_benchmarks"`
	StorageBeforeCompact      storageReport                     `json:"storage_before_compact"`
	StorageAfterCompact       storageReport                     `json:"storage_after_compact"`
	StorageAfterClose         storageReport                     `json:"storage_after_close"`
	StorageAfterVacuum        storageReport                     `json:"storage_after_index_vacuum"`
	IndexStatsBefore          collections.VectorIndexStats      `json:"index_stats_before_compact"`
	IndexStatsLoaded          collections.VectorIndexStats      `json:"index_stats_loaded"`
	NativeRootBytes           int64                             `json:"native_root_bytes"`
	VectorIndexStatus         collections.VectorIndexStatus     `json:"vector_index_status"`
	CompactStorage            *backenddb.CompactStorageStats    `json:"compact_storage,omitempty"`
	FormatConfig              *backenddb.FormatConfig           `json:"format_config,omitempty"`
	StorageExpectation        storageExpectationReport          `json:"storage_expectation"`
	Memory                    memoryReport                      `json:"memory"`
}

type matrixResult struct {
	Dir               string             `json:"dir"`
	KeptDir           bool               `json:"kept_dir"`
	Profile           string             `json:"profile"`
	SearchProfileDir  string             `json:"search_profile_dir,omitempty"`
	Docs              int                `json:"docs"`
	Dimensions        int                `json:"dimensions"`
	Queries           int                `json:"queries"`
	SearchConcurrency []int              `json:"search_concurrency"`
	TopK              int                `json:"top_k"`
	Cases             []matrixCaseResult `json:"cases"`
}

type matrixCaseResult struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Result      result `json:"result"`
}

type phaseResult struct {
	DurationNanos int64   `json:"duration_nanos"`
	Seconds       float64 `json:"seconds"`
}

type validationResult struct {
	DocumentsChecked int                   `json:"documents_checked"`
	QueriesChecked   int                   `json:"queries_checked"`
	ExactSource      validationExactSource `json:"exact_source"`
	ExactTotal       int                   `json:"exact_total"`
	ANNTotal         int                   `json:"ann_total"`
	Overlap          int                   `json:"overlap"`
	Recall           float64               `json:"recall"`
	MinRecall        float64               `json:"min_recall"`
	DurationNanos    int64                 `json:"duration_nanos"`
	Seconds          float64               `json:"seconds"`
}

type searchBenchmarkResult struct {
	Concurrency                       int                              `json:"concurrency"`
	Queries                           int                              `json:"queries"`
	QueryMode                         collections.VectorIndexQueryMode `json:"query_mode"`
	QuantizedCodec                    string                           `json:"quantized_codec"`
	QuantizedIndexName                string                           `json:"quantized_index_name"`
	QuantizedRerankCandidates         int                              `json:"quantized_rerank_candidates"`
	TotalDurationNanos                int64                            `json:"total_duration_nanos"`
	AvgNanos                          float64                          `json:"avg_nanos"`
	AvgMicros                         float64                          `json:"avg_micros"`
	OpsPerSecond                      float64                          `json:"ops_per_second"`
	P50Nanos                          int64                            `json:"p50_nanos"`
	P95Nanos                          int64                            `json:"p95_nanos"`
	P99Nanos                          int64                            `json:"p99_nanos"`
	AvgCandidates                     float64                          `json:"avg_candidates"`
	AvgRerank                         float64                          `json:"avg_rerank"`
	AvgQuantizedScoreCalls            float64                          `json:"avg_quantized_score_calls"`
	AvgQuantizedCodeBytes             float64                          `json:"avg_quantized_code_bytes"`
	AvgQuantizedRerankCandidates      float64                          `json:"avg_quantized_rerank_candidates"`
	AvgQuantizedRerankExactScoreCalls float64                          `json:"avg_quantized_rerank_exact_score_calls"`
	AvgVectorBytes                    float64                          `json:"avg_vector_bytes"`
	AvgNormBytes                      float64                          `json:"avg_norm_bytes"`
	AvgPreparedScoreCalls             float64                          `json:"avg_prepared_score_calls"`
	AvgScoreBatchCandidates           float64                          `json:"avg_score_batch_candidates"`
	AvgDocumentsFetched               *float64                         `json:"avg_documents_fetched,omitempty"`
	AvgResponseOwnedResultAllocs      *float64                         `json:"avg_response_owned_result_allocs,omitempty"`
	AvgSearchRouteHNSWSearchPack      *float64                         `json:"avg_search_route_hnsw_search_pack,omitempty"`
	AvgSearchRouteQuantizedOnly       *float64                         `json:"avg_search_route_quantized_only,omitempty"`
	AvgSearchRouteQuantizedRerank     *float64                         `json:"avg_search_route_quantized_rerank,omitempty"`
	AvgSearchRouteColumnGraphPrepared *float64                         `json:"avg_search_route_column_graph_prepared,omitempty"`
	AvgSearchRouteColumnGraphFallback *float64                         `json:"avg_search_route_column_graph_fallback,omitempty"`
	AvgGraphRowFallbacks              *float64                         `json:"avg_graph_row_fallbacks,omitempty"`
	AvgTypedColumnFallbacks           *float64                         `json:"avg_typed_column_fallbacks,omitempty"`
	AvgVectorScratchDecodes           *float64                         `json:"avg_vector_scratch_decodes,omitempty"`
	ExactFallbacks                    int                              `json:"exact_fallbacks"`
	DisableExactFallback              bool                             `json:"disable_exact_fallback"`
}

func float64Ptr(value float64) *float64 {
	return &value
}

func float64Value(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func searchBenchmarkHasGuardrailMetrics(bench searchBenchmarkResult) bool {
	return bench.AvgDocumentsFetched != nil ||
		bench.AvgResponseOwnedResultAllocs != nil ||
		bench.AvgSearchRouteHNSWSearchPack != nil ||
		bench.AvgSearchRouteQuantizedOnly != nil ||
		bench.AvgSearchRouteQuantizedRerank != nil ||
		bench.AvgSearchRouteColumnGraphPrepared != nil ||
		bench.AvgSearchRouteColumnGraphFallback != nil ||
		bench.AvgGraphRowFallbacks != nil ||
		bench.AvgTypedColumnFallbacks != nil ||
		bench.AvgVectorScratchDecodes != nil
}

type storageReport struct {
	TotalBytes  int64            `json:"total_bytes"`
	BytesPerDoc float64          `json:"bytes_per_doc"`
	Domains     map[string]int64 `json:"domains"`
	Files       int              `json:"files"`
}

type storageExpectationReport struct {
	RequireValueLogBytes bool  `json:"require_value_log_bytes"`
	RequireLeafVLogBytes bool  `json:"require_leaf_vlog_bytes"`
	ValueLogBytes        int64 `json:"value_log_bytes"`
	LeafVLogBytes        int64 `json:"leaf_vlog_bytes"`
	IndexBytes           int64 `json:"index_bytes"`
}

type memoryReport struct {
	AllocBeforeLoadBytes uint64 `json:"alloc_before_load_bytes"`
	AllocAfterLoadBytes  uint64 `json:"alloc_after_load_bytes"`
	LoadAllocDeltaBytes  int64  `json:"load_alloc_delta_bytes"`
	IndexBytesMemory     int64  `json:"index_bytes_memory"`
}

type datasetManifest struct {
	Version             int    `json:"version"`
	Docs                int    `json:"docs"`
	Dimensions          int    `json:"dimensions"`
	Queries             int    `json:"queries"`
	ExactTruthQueries   int    `json:"exact_truth_queries"`
	TopK                int    `json:"top_k"`
	Metric              string `json:"metric"`
	DocumentVectorsFile string `json:"document_vectors_file"`
	QueryVectorsFile    string `json:"query_vectors_file"`
	DocumentsJSONLFile  string `json:"documents_jsonl_file"`
	FloatFormat         string `json:"float_format"`
}

type workload struct {
	datasetDir string
	manifest   datasetManifest
}

type datasetDocumentHeader struct {
	Index int    `json:"index"`
	ID    string `json:"id"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "treedb-vector-search-demo: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	if cfg.matrix {
		res, err := executeMatrix(context.Background(), cfg)
		if err != nil {
			return err
		}
		if cfg.jsonOut {
			enc := json.NewEncoder(stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(res)
		}
		printMatrixText(stdout, res)
		if !res.KeptDir {
			fmt.Fprintf(stderr, "temporary db removed; rerun with -keep-dir to inspect files\n")
		}
		return nil
	}
	res, err := execute(context.Background(), cfg)
	if err != nil {
		return err
	}
	if cfg.jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	printText(stdout, res)
	if !res.KeptDir {
		fmt.Fprintf(stderr, "temporary db removed; rerun with -keep-dir to inspect files\n")
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		docs:                  defaultDocs,
		dimensions:            defaultDimensions,
		queries:               defaultQueries,
		validateQueries:       32,
		validateDocs:          16,
		validationExactSource: validationExactSourceTreeDB,
		topK:                  defaultTopK,
		batchSize:             defaultBatchSize,
		m:                     defaultM,
		efConstruction:        defaultEfConstruct,
		efSearch:              defaultEfSearch,
		valuePointerThreshold: defaultValuePointerThreshold,
		leafGenerationTarget:  defaultLeafGenerationTarget,
		minRecall:             0.95,
		matrix:                true,
		compact:               false,
		vacuumIndex:           true,
		disableExactFallback:  true,
		profile:               treedb.ProfileBenchUnsafe,
		vectorIndexStrategy:   collections.VectorIndexStrategyNativeRuntime,
		vectorQueryMode:       collections.VectorIndexQueryModeExact,
	}
	profileRaw := string(cfg.profile)
	vectorIndexStrategyRaw := string(cfg.vectorIndexStrategy)
	vectorQueryModeRaw := string(cfg.vectorQueryMode)
	validationExactSourceRaw := string(cfg.validationExactSource)
	searchConcurrencyRaw := defaultSearchConcurrency
	fs := flag.NewFlagSet("treedb_vector_search_demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.dir, "dir", "", "TreeDB directory to create; empty uses a temporary directory; explicit directories are kept")
	fs.StringVar(&cfg.datasetDir, "dataset-dir", "", "Optional exported vector dataset directory to load documents and queries from")
	fs.BoolVar(&cfg.keepDir, "keep-dir", false, "Keep the DB directory after the run")
	fs.BoolVar(&cfg.matrix, "matrix", cfg.matrix, "Run the storage/search benchmark matrix instead of a single storage case")
	fs.StringVar(&profileRaw, "profile", profileRaw, "TreeDB profile: "+treedb.BenchmarkProfileFlagHelp)
	fs.StringVar(&vectorIndexStrategyRaw, "vector-index-strategy", vectorIndexStrategyRaw, "TreeDB vector index strategy: native_runtime or column_graph")
	fs.StringVar(&vectorQueryModeRaw, "vector-query-mode", vectorQueryModeRaw, "TreeDB vector query mode: exact, quantized_only, or quantized_rerank")
	fs.StringVar(&validationExactSourceRaw, "validation-exact-source", validationExactSourceRaw, "Exact baseline source for recall validation: treedb or dataset")
	fs.StringVar(&cfg.quantizedCodec, "quantized-codec", cfg.quantizedCodec, "Quantized codec for column_graph quantized query modes: scalar_u8 or rabitq_1bit; empty defaults to scalar_u8 for quantized modes")
	fs.StringVar(&cfg.quantizedIndexName, "quantized-index-name", cfg.quantizedIndexName, "Named quantized index to use for column_graph quantized query modes, for example "+defaultQuantizedIndexName+" or "+defaultRabitQQuantizedIndexName)
	fs.IntVar(&cfg.quantizedRerankCandidates, "quantized-rerank-candidates", cfg.quantizedRerankCandidates, "Candidate limit exact-reranked by column_graph quantized_rerank mode; 0 uses the normalized ef_search set")
	fs.IntVar(&cfg.docs, "docs", cfg.docs, "Number of synthetic documents to load")
	fs.IntVar(&cfg.dimensions, "dims", cfg.dimensions, "Vector dimensions per document")
	fs.IntVar(&cfg.queries, "queries", cfg.queries, "Number of ANN search queries to benchmark")
	fs.StringVar(&searchConcurrencyRaw, "search-concurrency", searchConcurrencyRaw, "Comma-separated parallel ANN search concurrency levels; serial concurrency=1 is always included")
	fs.IntVar(&cfg.validateQueries, "validate-queries", cfg.validateQueries, "Number of queries to validate against the selected exact baseline")
	fs.IntVar(&cfg.validateDocs, "validate-docs", cfg.validateDocs, "Number of documents to read and byte-validate after compaction/reopen")
	fs.IntVar(&cfg.topK, "top-k", cfg.topK, "Nearest-neighbor result count")
	fs.IntVar(&cfg.batchSize, "batch-size", cfg.batchSize, "Insert batch size")
	fs.IntVar(&cfg.m, "m", cfg.m, "HNSW max neighbor parameter")
	fs.IntVar(&cfg.efConstruction, "ef-construction", cfg.efConstruction, "HNSW efConstruction")
	fs.IntVar(&cfg.efSearch, "ef-search", cfg.efSearch, "HNSW efSearch for ANN queries")
	fs.IntVar(&cfg.valuePointerThreshold, "value-pointer-threshold", cfg.valuePointerThreshold, "Value-log pointer threshold for the demo DB in bytes; 0 uses the selected TreeDB profile default")
	fs.Int64Var(&cfg.leafGenerationTarget, "leaf-generation-segment-target", cfg.leafGenerationTarget, "Leaf value-log generation segment target for the demo DB in bytes; a positive value opts the demo into leaf generation rolling, 0 uses the selected TreeDB profile default")
	fs.Float64Var(&cfg.minRecall, "min-recall", cfg.minRecall, "Minimum validation recall@topK")
	fs.BoolVar(&cfg.compact, "compact", cfg.compact, "Run CompactStorageFull after insert/index build and before reopen/validation/search")
	fs.BoolVar(&cfg.compactSyncEachPhase, "compact-sync-each-phase", false, "Ask CompactStorage to fsync each rewrite/pack phase")
	fs.BoolVar(&cfg.vacuumIndex, "vacuum-index", cfg.vacuumIndex, "Run offline index.db vacuum after close before final storage/reopen")
	fs.BoolVar(&cfg.disableExactFallback, "disable-exact-fallback", cfg.disableExactFallback, "Disable exact fallback during ANN benchmark queries")
	fs.BoolVar(&cfg.requireValueLogBytes, "require-value-log-bytes", false, "Fail if compacted storage has no value-log bytes")
	fs.BoolVar(&cfg.requireLeafVLogBytes, "require-leaf-vlog-bytes", false, "Fail if compacted storage has no leaf value-log bytes")
	fs.StringVar(&cfg.searchProfileDir, "search-profile-dir", "", "Write per-concurrency column_graph search CPU/heap/alloc/block/mutex profiles under this directory")
	fs.BoolVar(&cfg.jsonOut, "json", false, "Emit JSON instead of text")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	profileExplicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "profile" {
			profileExplicit = true
		}
	})
	searchConcurrency, err := parseSearchConcurrency(searchConcurrencyRaw)
	if err != nil {
		return config{}, err
	}
	cfg.searchConcurrency = searchConcurrency
	strategy, err := parseVectorIndexStrategy(vectorIndexStrategyRaw)
	if err != nil {
		return config{}, err
	}
	cfg.vectorIndexStrategy = strategy
	profile, err := parseProfile(profileRaw)
	if err != nil {
		return config{}, err
	}
	if !profileExplicit && strategy == collections.VectorIndexStrategyColumnGraph {
		profile = treedb.ProfileCommandWALDurable
	}
	cfg.profile = profile
	if err := validateDemoProfile(cfg.profile, cfg.vectorIndexStrategy); err != nil {
		return config{}, err
	}
	queryMode, err := parseVectorQueryMode(vectorQueryModeRaw)
	if err != nil {
		return config{}, err
	}
	cfg.vectorQueryMode = queryMode
	if err := applyQuantizedConfigDefaults(&cfg); err != nil {
		return config{}, err
	}
	validationExactSource, err := parseValidationExactSource(validationExactSourceRaw)
	if err != nil {
		return config{}, err
	}
	cfg.validationExactSource = validationExactSource
	if err := validateVectorQueryConfig(cfg); err != nil {
		return config{}, err
	}
	if cfg.docs <= 0 {
		return config{}, errors.New("-docs must be positive")
	}
	if cfg.dimensions <= 0 {
		return config{}, errors.New("-dims must be positive")
	}
	if cfg.queries <= 0 {
		return config{}, errors.New("-queries must be positive")
	}
	if cfg.topK <= 0 {
		return config{}, errors.New("-top-k must be positive")
	}
	if cfg.topK > cfg.docs {
		return config{}, errors.New("-top-k cannot exceed -docs")
	}
	if cfg.batchSize <= 0 {
		return config{}, errors.New("-batch-size must be positive")
	}
	if cfg.m <= 0 {
		return config{}, errors.New("-m must be positive")
	}
	if cfg.efConstruction <= 0 {
		return config{}, errors.New("-ef-construction must be positive")
	}
	if cfg.efSearch <= 0 {
		return config{}, errors.New("-ef-search must be positive")
	}
	if cfg.valuePointerThreshold < 0 {
		return config{}, errors.New("-value-pointer-threshold cannot be negative")
	}
	if cfg.leafGenerationTarget < 0 {
		return config{}, errors.New("-leaf-generation-segment-target cannot be negative")
	}
	if cfg.minRecall < 0 || cfg.minRecall > 1 {
		return config{}, errors.New("-min-recall must be in [0,1]")
	}
	if cfg.validateQueries < 0 {
		return config{}, errors.New("-validate-queries cannot be negative")
	}
	if cfg.validateDocs < 0 {
		return config{}, errors.New("-validate-docs cannot be negative")
	}
	if cfg.datasetDir != "" {
		cfg.datasetDir = filepath.Clean(cfg.datasetDir)
	}
	if cfg.searchProfileDir != "" {
		cfg.searchProfileDir = filepath.Clean(cfg.searchProfileDir)
		if cfg.vectorIndexStrategy != collections.VectorIndexStrategyColumnGraph {
			return config{}, errors.New("-search-profile-dir requires -vector-index-strategy column_graph")
		}
	}
	if err := validateValidationExactSourceConfig(cfg); err != nil {
		return config{}, err
	}
	if cfg.datasetDir == "" && cfg.validateQueries > cfg.docs {
		return config{}, errors.New("-validate-queries cannot exceed -docs")
	}
	if cfg.validateDocs > cfg.docs {
		return config{}, errors.New("-validate-docs cannot exceed -docs")
	}
	if cfg.validateQueries == 0 && cfg.minRecall > 0 {
		return config{}, errors.New("-min-recall must be 0 when -validate-queries is 0")
	}
	return cfg, nil
}

func parseVectorIndexStrategy(raw string) (collections.VectorIndexStrategy, error) {
	switch collections.VectorIndexStrategy(strings.ToLower(strings.TrimSpace(raw))) {
	case "", collections.VectorIndexStrategyNativeRuntime:
		return collections.VectorIndexStrategyNativeRuntime, nil
	case collections.VectorIndexStrategyColumnGraph:
		return collections.VectorIndexStrategyColumnGraph, nil
	default:
		return "", fmt.Errorf("unsupported -vector-index-strategy %q", raw)
	}
}

func parseVectorQueryMode(raw string) (collections.VectorIndexQueryMode, error) {
	switch collections.VectorIndexQueryMode(strings.ToLower(strings.TrimSpace(raw))) {
	case "", collections.VectorIndexQueryModeExact:
		return collections.VectorIndexQueryModeExact, nil
	case collections.VectorIndexQueryModeQuantizedOnly:
		return collections.VectorIndexQueryModeQuantizedOnly, nil
	case collections.VectorIndexQueryModeQuantizedRerank:
		return collections.VectorIndexQueryModeQuantizedRerank, nil
	default:
		return "", fmt.Errorf("unsupported -vector-query-mode %q", raw)
	}
}

func parseValidationExactSource(raw string) (validationExactSource, error) {
	switch validationExactSource(strings.ToLower(strings.TrimSpace(raw))) {
	case "", validationExactSourceTreeDB:
		return validationExactSourceTreeDB, nil
	case validationExactSourceDataset:
		return validationExactSourceDataset, nil
	default:
		return "", fmt.Errorf("unsupported -validation-exact-source %q", raw)
	}
}

func validateValidationExactSourceConfig(cfg config) error {
	if cfg.validationExactSource == validationExactSourceDataset && cfg.datasetDir == "" {
		return errors.New("-validation-exact-source=dataset requires -dataset-dir")
	}
	return nil
}

func applyQuantizedConfigDefaults(cfg *config) error {
	codec, err := parseQuantizedCodec(cfg.quantizedCodec)
	if err != nil {
		return err
	}
	cfg.quantizedCodec = codec
	if vectorQueryModeIsQuantized(normalizedVectorQueryMode(cfg.vectorQueryMode)) && cfg.quantizedCodec == "" {
		cfg.quantizedCodec = defaultQuantizedCodec
	}
	return nil
}

func parseQuantizedCodec(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case collections.QuantizedVectorCodecScalarU8:
		return collections.QuantizedVectorCodecScalarU8, nil
	case quantizedVectorCodecRabitQ1Bit:
		return quantizedVectorCodecRabitQ1Bit, nil
	default:
		return "", fmt.Errorf("unsupported -quantized-codec %q; allowed: %s,%s", raw, collections.QuantizedVectorCodecScalarU8, quantizedVectorCodecRabitQ1Bit)
	}
}

func validateVectorQueryConfig(cfg config) error {
	mode := normalizedVectorQueryMode(cfg.vectorQueryMode)
	if cfg.quantizedRerankCandidates < 0 {
		return errors.New("-quantized-rerank-candidates cannot be negative")
	}
	if !vectorQueryModeIsQuantized(mode) {
		if cfg.quantizedCodec != "" {
			return errors.New("-quantized-codec requires -vector-query-mode quantized_only or quantized_rerank")
		}
		if cfg.quantizedIndexName != "" {
			return errors.New("-quantized-index-name requires -vector-query-mode quantized_only or quantized_rerank")
		}
		if cfg.quantizedRerankCandidates != 0 {
			return errors.New("-quantized-rerank-candidates requires -vector-query-mode quantized_rerank")
		}
		return nil
	}
	if cfg.vectorIndexStrategy != collections.VectorIndexStrategyColumnGraph {
		return errors.New("quantized vector query modes require -vector-index-strategy column_graph")
	}
	if cfg.quantizedCodec == "" {
		return errors.New("-quantized-codec is required for quantized vector query modes")
	}
	if _, err := parseQuantizedCodec(cfg.quantizedCodec); err != nil {
		return err
	}
	if cfg.quantizedIndexName == "" {
		return errors.New("-quantized-index-name is required for quantized vector query modes")
	}
	if err := collections.ValidateIndexName(cfg.quantizedIndexName); err != nil {
		return fmt.Errorf("-quantized-index-name: %w", err)
	}
	if mode == collections.VectorIndexQueryModeQuantizedOnly && cfg.quantizedRerankCandidates != 0 {
		return errors.New("-quantized-rerank-candidates requires -vector-query-mode quantized_rerank")
	}
	if mode == collections.VectorIndexQueryModeQuantizedRerank && cfg.quantizedRerankCandidates != 0 && cfg.quantizedRerankCandidates < cfg.topK {
		return errors.New("-quantized-rerank-candidates cannot be less than -top-k")
	}
	return nil
}

func normalizedVectorQueryMode(mode collections.VectorIndexQueryMode) collections.VectorIndexQueryMode {
	if mode == "" {
		return collections.VectorIndexQueryModeExact
	}
	return mode
}

func vectorQueryModeIsQuantized(mode collections.VectorIndexQueryMode) bool {
	return mode == collections.VectorIndexQueryModeQuantizedOnly || mode == collections.VectorIndexQueryModeQuantizedRerank
}

func parseProfile(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParseBenchmarkProfile(raw, treedb.ProfileBenchUnsafe)
	if !ok {
		return "", fmt.Errorf("unsupported -profile %q; allowed: %s", raw, treedb.BenchmarkProfileFlagHelp)
	}
	return profile, nil
}

func parseSearchConcurrency(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts)+1)
	seen[1] = struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid -search-concurrency value %q", part)
		}
		if value <= 0 {
			return nil, fmt.Errorf("-search-concurrency values must be positive: %d", value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, errors.New("-search-concurrency must include at least one value greater than 1")
	}
	return out, nil
}

func applySearchBenchmarkDefaults(cfg *config) error {
	if len(cfg.searchConcurrency) == 0 {
		concurrency, err := parseSearchConcurrency(defaultSearchConcurrency)
		if err != nil {
			return err
		}
		cfg.searchConcurrency = concurrency
	}
	return nil
}

func defaultProfileForStrategy(profile treedb.Profile, strategy collections.VectorIndexStrategy) treedb.Profile {
	if profile == "" && strategy == collections.VectorIndexStrategyColumnGraph {
		return treedb.ProfileCommandWALDurable
	}
	if profile == "" {
		return treedb.ProfileBenchUnsafe
	}
	return profile
}

func validateDemoProfile(profile treedb.Profile, strategy collections.VectorIndexStrategy) error {
	if strategy != collections.VectorIndexStrategyColumnGraph {
		return nil
	}
	if !treedb.OptionsForBenchmark(profile, "").CommandWAL {
		return fmt.Errorf("-vector-index-strategy column_graph requires a command-WAL profile; got %q", profile)
	}
	return nil
}

func normalizeDemoDir(dir string) (string, error) {
	if dir == "" {
		return "", nil
	}
	clean := filepath.Clean(dir)
	if filepath.Base(clean) == "maindb" {
		return "", fmt.Errorf("demo -dir must be a TreeDB root directory, not a maindb directory; pass %q instead", filepath.Dir(clean))
	}
	return clean, nil
}

func openDemoBackend(cfg config, dir string) (*backenddb.DB, func() error, error) {
	opts := demoBackendOptions(cfg, dir)
	if opts.IndexOuterLeavesInValueLog {
		return treedb.OpenBackendWithCachedLeafLog(opts)
	}
	return treedb.OpenBackend(opts)
}

func demoBackendOptions(cfg config, dir string) treedb.Options {
	opts := treedb.OptionsForBenchmark(defaultProfileForStrategy(cfg.profile, cfg.vectorIndexStrategy), dir)
	if cfg.indexOuterLeavesInValueLog != nil {
		opts.IndexOuterLeavesInValueLog = *cfg.indexOuterLeavesInValueLog
		opts.IndexInternalBaseDelta = !*cfg.indexOuterLeavesInValueLog
	}
	if cfg.valuePointerThreshold > 0 {
		opts.ValueLog.PointerThreshold = cfg.valuePointerThreshold
	}
	if cfg.leafGenerationTarget > 0 {
		// A positive demo-local target is an explicit opt-in to sealed leaf
		// generations, including under the deterministic bench profile.
		opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
		opts.ValueLog.Generational.LeafSegmentTargetBytes = cfg.leafGenerationTarget
	}
	return opts
}

func demoCommandWALFormatConfig(opts treedb.Options) backenddb.FormatConfig {
	return backenddb.FormatConfig{
		RequiredFeatures:  []string{backenddb.RequiredFeatureCommandWALV1},
		DurabilityProfile: opts.ResolvedProfile,

		IndexOuterLeavesInValueLog: opts.IndexOuterLeavesInValueLog,

		LeafPrefixCompression:     opts.LeafPrefixCompression,
		IndexColumnarLeaves:       opts.IndexColumnarLeaves,
		IndexPackedValuePtr:       opts.IndexPackedValuePtr,
		IndexInternalBaseDelta:    opts.IndexInternalBaseDelta,
		IndexAdaptiveLeafEncoding: opts.IndexAdaptiveLeafEncoding,

		ValueLogCompression: demoValueLogCompressionMode(opts.ValueLog.Compression),
		ValueLogBlockCodec:  demoValueLogBlockCodec(opts.ValueLog.BlockCodec),
		ValueLogAutoPolicy:  demoValueLogAutoPolicy(opts.ValueLog.AutoPolicy),
	}
}

func demoValueLogCompressionMode(mode backenddb.ValueLogCompressionMode) string {
	switch mode {
	case backenddb.ValueLogCompressionOff:
		return "off"
	case backenddb.ValueLogCompressionBlock:
		return "block"
	case backenddb.ValueLogCompressionDict:
		return "dict"
	case backenddb.ValueLogCompressionAuto:
		return "auto"
	default:
		return fmt.Sprintf("mode_%d", mode)
	}
}

func demoValueLogBlockCodec(codec backenddb.ValueLogBlockCodec) string {
	switch codec {
	case backenddb.ValueLogBlockSnappy:
		return "snappy"
	case backenddb.ValueLogBlockLZ4:
		return "lz4"
	case backenddb.ValueLogBlockZSTD:
		return "zstd"
	default:
		return fmt.Sprintf("codec_%d", codec)
	}
}

func demoValueLogAutoPolicy(policy backenddb.ValueLogAutoPolicy) string {
	switch policy {
	case backenddb.ValueLogAutoBalanced:
		return "balanced"
	case backenddb.ValueLogAutoThroughput:
		return "throughput"
	case backenddb.ValueLogAutoSize:
		return "size"
	default:
		return fmt.Sprintf("policy_%d", policy)
	}
}

func resultBackendForStrategy(strategy collections.VectorIndexStrategy) string {
	if strategy == collections.VectorIndexStrategyColumnGraph {
		return "treedb_column_graph"
	}
	return "treedb"
}

func resultBackendForQuery(strategy collections.VectorIndexStrategy, mode collections.VectorIndexQueryMode, quantizedCodec string) string {
	if strategy != collections.VectorIndexStrategyColumnGraph {
		return resultBackendForStrategy(strategy)
	}
	switch normalizedVectorQueryMode(mode) {
	case collections.VectorIndexQueryModeQuantizedOnly:
		return "treedb_column_graph_" + quantizedBackendCodecLabel(quantizedCodec) + "_quantized_only"
	case collections.VectorIndexQueryModeQuantizedRerank:
		return "treedb_column_graph_" + quantizedBackendCodecLabel(quantizedCodec) + "_quantized_rerank"
	default:
		return "treedb_column_graph"
	}
}

func quantizedBackendCodecLabel(codec string) string {
	switch codec {
	case collections.QuantizedVectorCodecScalarU8:
		return collections.QuantizedVectorCodecScalarU8
	case quantizedVectorCodecRabitQ1Bit:
		return quantizedVectorCodecRabitQ1Bit
	default:
		return "quantized"
	}
}

func effectiveQuantizedCodec(cfg config) string {
	if vectorQueryModeIsQuantized(normalizedVectorQueryMode(cfg.vectorQueryMode)) {
		return cfg.quantizedCodec
	}
	return ""
}

func effectiveQuantizedIndexName(cfg config) string {
	if vectorQueryModeIsQuantized(normalizedVectorQueryMode(cfg.vectorQueryMode)) {
		return cfg.quantizedIndexName
	}
	return ""
}

func effectiveQuantizedRerankCandidates(cfg config) int {
	if normalizedVectorQueryMode(cfg.vectorQueryMode) == collections.VectorIndexQueryModeQuantizedRerank {
		return cfg.quantizedRerankCandidates
	}
	return 0
}

func columnGraphSearchOptions(query []float32, cfg config) collections.VectorIndexSearcherSearchOptions {
	return collections.VectorIndexSearcherSearchOptions{
		Query:                     query,
		QueryMode:                 normalizedVectorQueryMode(cfg.vectorQueryMode),
		QuantizedIndexName:        effectiveQuantizedIndexName(cfg),
		QuantizedRerankCandidates: effectiveQuantizedRerankCandidates(cfg),
		TopK:                      cfg.topK,
		EfSearch:                  cfg.efSearch,
	}
}

func columnGraphDemoColumnStoreConfig(dims int) *collections.ColumnStoreConfig {
	return &collections.ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
		ProfileSupport:  collections.ColumnStoreProfileBenchmarkRelaxed,
		Columns: []collections.ColumnStoreColumn{
			{
				Name:       "embedding",
				Path:       "embedding",
				Owner:      collections.TypedStorageOwnerColumnPart,
				ValueType:  collections.ColumnStoreValueFloat32Vector,
				VectorDims: dims,
			},
		},
	}
}

func execute(ctx context.Context, cfg config) (result, error) {
	if cfg.vectorIndexStrategy == "" {
		cfg.vectorIndexStrategy = collections.VectorIndexStrategyNativeRuntime
	}
	cfg.profile = defaultProfileForStrategy(cfg.profile, cfg.vectorIndexStrategy)
	if err := validateDemoProfile(cfg.profile, cfg.vectorIndexStrategy); err != nil {
		return result{}, err
	}
	if cfg.vectorQueryMode == "" {
		cfg.vectorQueryMode = collections.VectorIndexQueryModeExact
	}
	validationExactSource, err := parseValidationExactSource(string(cfg.validationExactSource))
	if err != nil {
		return result{}, err
	}
	cfg.validationExactSource = validationExactSource
	if err := validateValidationExactSourceConfig(cfg); err != nil {
		return result{}, err
	}
	if err := applyQuantizedConfigDefaults(&cfg); err != nil {
		return result{}, err
	}
	if err := validateVectorQueryConfig(cfg); err != nil {
		return result{}, err
	}
	if err := applySearchBenchmarkDefaults(&cfg); err != nil {
		return result{}, err
	}
	work, err := loadWorkload(&cfg)
	if err != nil {
		return result{}, err
	}
	if work.datasetDir != "" {
		if err := validateDatasetDocuments(cfg, work); err != nil {
			return result{}, err
		}
	}
	dir, err := normalizeDemoDir(cfg.dir)
	if err != nil {
		return result{}, err
	}
	explicitDir := dir != ""
	cleanup := func() {}
	if dir == "" {
		tmp, err := os.MkdirTemp("", "treedb-vector-search-demo-*")
		if err != nil {
			return result{}, err
		}
		dir = tmp
		if !cfg.keepDir {
			cleanup = func() { _ = os.RemoveAll(tmp) }
		}
	} else {
		info, err := os.Stat(dir)
		if err == nil {
			if !info.IsDir() {
				return result{}, fmt.Errorf("-dir %q exists and is not a directory", dir)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				return result{}, err
			}
			if len(entries) > 0 {
				return result{}, fmt.Errorf("-dir %q already exists and is not empty; choose a new directory", dir)
			}
		} else if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return result{}, err
			}
		} else {
			return result{}, err
		}
	}
	if !cfg.keepDir {
		defer cleanup()
	}

	def := collections.VectorIndexDefinition{
		Name:           "embedding",
		Field:          "embedding",
		Metric:         collections.VectorMetricCosine,
		Dimensions:     cfg.dimensions,
		M:              cfg.m,
		EfConstruction: cfg.efConstruction,
		EfSearch:       cfg.efSearch,
		Strategy:       cfg.vectorIndexStrategy,
	}
	if vectorQueryModeIsQuantized(cfg.vectorQueryMode) {
		def.QuantizedIndexes = []collections.QuantizedVectorIndexDefinition{{
			Name:    cfg.quantizedIndexName,
			Codec:   cfg.quantizedCodec,
			Version: 1,
		}}
	}
	res := result{
		Backend:                   resultBackendForQuery(def.Strategy, cfg.vectorQueryMode, cfg.quantizedCodec),
		Engine:                    "treedb",
		VectorIndexStrategy:       def.Strategy,
		QueryMode:                 normalizedVectorQueryMode(cfg.vectorQueryMode),
		QuantizedCodec:            effectiveQuantizedCodec(cfg),
		QuantizedIndexName:        effectiveQuantizedIndexName(cfg),
		QuantizedRerankCandidates: effectiveQuantizedRerankCandidates(cfg),
		Dir:                       dir,
		DatasetDir:                cfg.datasetDir,
		KeptDir:                   cfg.keepDir || explicitDir,
		Profile:                   string(cfg.profile),
		Docs:                      cfg.docs,
		Dimensions:                cfg.dimensions,
		Queries:                   cfg.queries,
		SearchConcurrency:         append([]int(nil), cfg.searchConcurrency...),
		ValidateQueries:           cfg.validateQueries,
		ValidateDocs:              cfg.validateDocs,
		ValidationExactSource:     cfg.validationExactSource,
		SearchProfileDir:          cfg.searchProfileDir,
		TopK:                      cfg.topK,
		M:                         cfg.m,
		EfConstruction:            cfg.efConstruction,
		EfSearch:                  cfg.efSearch,
		ValuePointerThreshold:     cfg.valuePointerThreshold,
		LeafGenerationTarget:      cfg.leafGenerationTarget,
		MinRecall:                 cfg.minRecall,
		Compact:                   cfg.compact,
		CompactSyncEachPhase:      cfg.compactSyncEachPhase,
		DisableExactFallback:      cfg.disableExactFallback,
	}

	if def.Strategy == collections.VectorIndexStrategyColumnGraph {
		mainDir := filepath.Join(dir, "maindb")
		if err := os.MkdirAll(mainDir, 0o755); err != nil {
			return result{}, err
		}
		if err := backenddb.SaveFormatConfig(mainDir, demoCommandWALFormatConfig(demoBackendOptions(cfg, dir))); err != nil {
			return result{}, fmt.Errorf("enable command WAL format for column_graph: %w", err)
		}
	}
	d, cleanupBackend, err := openDemoBackend(cfg, dir)
	if err != nil {
		return result{}, err
	}
	mgr := collections.NewCollectionManager(d)
	meta := &collections.CollectionMeta{Name: "docs"}
	if def.Strategy == collections.VectorIndexStrategyColumnGraph {
		meta.Options.DocumentFormat = collections.DocumentFormatJSON
		meta.Options.ColumnStore = columnGraphDemoColumnStoreConfig(cfg.dimensions)
		meta.VectorIndexes = []collections.VectorIndexDefinition{def}
	}
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}

	insertStart := time.Now()
	if err := insertDocuments(col, cfg, work); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	if err := col.Flush(); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	res.Insert = phaseSince(insertStart)

	if def.Strategy != collections.VectorIndexStrategyColumnGraph {
		if _, err := col.CreateVectorIndex(def); err != nil {
			_ = cleanupBackend()
			return result{}, err
		}
	}
	rebuildStart := time.Now()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	if def.Strategy == collections.VectorIndexStrategyColumnGraph {
		if !status.Loaded || status.State != collections.VectorIndexStateColumnGraphLoaded {
			_ = cleanupBackend()
			return result{}, fmt.Errorf("unexpected column_graph vector status: %+v", status)
		}
	} else if !status.NativeRootLoaded || status.RootID == 0 {
		_ = cleanupBackend()
		return result{}, fmt.Errorf("unexpected native vector root status: %+v", status)
	}
	res.Rebuild = phaseSince(rebuildStart)
	res.NativeRootBytes = status.NativeRootBytes
	res.IndexStatsBefore = status.Stats
	res.VectorIndexStatus = status

	if err := d.Checkpoint(); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	res.StorageBeforeCompact, err = storageUsage(dir, cfg.docs)
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}

	if cfg.compact {
		compactStart := time.Now()
		stats, err := d.CompactStorage(ctx, backenddb.CompactStorageOptions{
			Mode:          backenddb.CompactStorageFull,
			SyncEachPhase: cfg.compactSyncEachPhase,
		})
		if err != nil {
			_ = cleanupBackend()
			return result{}, err
		}
		res.CompactPhase = phaseSince(compactStart)
		res.CompactStorage = &stats
		if err := d.Checkpoint(); err != nil {
			_ = cleanupBackend()
			return result{}, err
		}
	}
	res.StorageAfterCompact, err = storageUsage(dir, cfg.docs)
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	if format, ok, err := loadFormatConfig(dir); err != nil {
		_ = cleanupBackend()
		return result{}, err
	} else if ok {
		res.FormatConfig = &format
	}
	res.StorageExpectation = storageExpectationReport{
		RequireValueLogBytes: cfg.requireValueLogBytes,
		RequireLeafVLogBytes: cfg.requireLeafVLogBytes,
		ValueLogBytes:        res.StorageAfterCompact.Domains["value_vlog"],
		LeafVLogBytes:        res.StorageAfterCompact.Domains["leaf_vlog"],
		IndexBytes:           res.StorageAfterCompact.Domains["index.db"],
	}
	if cfg.requireValueLogBytes && res.StorageExpectation.ValueLogBytes == 0 {
		_ = cleanupBackend()
		return result{}, errors.New("compacted storage has zero value_vlog bytes")
	}
	if cfg.requireLeafVLogBytes && res.StorageExpectation.LeafVLogBytes == 0 {
		_ = cleanupBackend()
		return result{}, errors.New("compacted storage has zero leaf_vlog bytes")
	}
	mgr = nil
	col = nil
	if err := cleanupBackend(); err != nil {
		return result{}, err
	}
	cleanupBackend = nil
	d = nil
	res.StorageAfterClose, err = storageUsage(dir, cfg.docs)
	if err != nil {
		return result{}, err
	}
	if cfg.vacuumIndex {
		vacuumStart := time.Now()
		vacuumOpts := demoBackendOptions(cfg, dir)
		vacuumOpts.KeepRecent = 1
		if err := treedb.VacuumIndexOffline(vacuumOpts); err != nil {
			return result{}, fmt.Errorf("index vacuum: %w", err)
		}
		res.IndexVacuum = phaseSince(vacuumStart)
		res.StorageAfterVacuum, err = storageUsage(dir, cfg.docs)
		if err != nil {
			return result{}, err
		}
	} else {
		res.StorageAfterVacuum = res.StorageAfterClose
	}
	runtime.GC()

	reopenStart := time.Now()
	d, cleanupBackend, err = openDemoBackend(cfg, dir)
	if err != nil {
		return result{}, err
	}
	closeReopened := func() error {
		if cleanupBackend == nil {
			return nil
		}
		err := cleanupBackend()
		cleanupBackend = nil
		d = nil
		return err
	}
	defer func() { _ = closeReopened() }()
	col, err = collections.NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		return result{}, err
	}
	var beforeLoad runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&beforeLoad)
	var loaded *collections.VectorIndex
	var columnGraphSearcher *collections.VectorIndexSearcher
	if def.Strategy == collections.VectorIndexStrategyColumnGraph {
		columnGraphSearcher, err = col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: def.Name})
		if err != nil {
			return result{}, err
		}
		defer func() {
			if columnGraphSearcher != nil {
				_ = columnGraphSearcher.Close()
			}
		}()
		response, err := columnGraphSearcher.Search(columnGraphSearchOptions(embedding(0, cfg.dimensions), cfg))
		if err != nil {
			return result{}, err
		}
		if response.Strategy != collections.VectorIndexStrategyColumnGraph || response.Path != collections.VectorIndexSearchPathColumnGraphNativeReader {
			return result{}, fmt.Errorf("unexpected column_graph search path after reopen: strategy=%s path=%s", response.Strategy, response.Path)
		}
		res.VectorIndexSearchPath = response.Path
		res.IndexStatsLoaded = response.Status.Stats
		res.VectorIndexStatus = response.Status
	} else {
		var loadStatus collections.VectorIndexLoadStatus
		loaded, loadStatus, err = col.LoadVectorIndexSnapshot(vectorIndexOptions(def))
		if err != nil {
			return result{}, err
		}
		if loaded == nil {
			return result{}, fmt.Errorf("native vector root load returned no runtime: %+v", loadStatus)
		}
		if !loadStatus.Loaded {
			return result{}, fmt.Errorf("native vector root not marked loaded: %+v", loadStatus)
		}
		if loadStatus.RootID == 0 {
			return result{}, fmt.Errorf("native vector root loaded with zero root id: %+v", loadStatus)
		}
		res.VectorIndexSearchPath = nativeRuntimeSnapshotPath
		res.IndexStatsLoaded = loaded.Stats()
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			return result{}, err
		}
		res.VectorIndexStatus = status
	}
	res.ReopenLoad = phaseSince(reopenStart)
	var afterLoad runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&afterLoad)
	res.Memory = memoryReport{
		AllocBeforeLoadBytes: beforeLoad.Alloc,
		AllocAfterLoadBytes:  afterLoad.Alloc,
		LoadAllocDeltaBytes:  allocDeltaBytes(afterLoad.Alloc, beforeLoad.Alloc),
		IndexBytesMemory:     res.IndexStatsLoaded.BytesMemory,
	}
	if columnGraphSearcher != nil {
		if closeErr := columnGraphSearcher.Close(); closeErr != nil {
			return result{}, closeErr
		}
		columnGraphSearcher = nil
	}

	if def.Strategy == collections.VectorIndexStrategyColumnGraph {
		validation, err := validateCompactedColumnGraphData(col, def.Name, cfg, work)
		if err != nil {
			return result{}, err
		}
		res.Validation = validation
		searchBenchmarks, err := benchmarkColumnGraphSearchMatrix(col, def.Name, cfg, work)
		if err != nil {
			return result{}, err
		}
		res.SearchBenchmarks = searchBenchmarks
	} else {
		validation, err := validateCompactedData(col, loaded, cfg, work)
		if err != nil {
			return result{}, err
		}
		res.Validation = validation
		searchBenchmarks, err := benchmarkSearchMatrix(loaded, cfg, work)
		if err != nil {
			return result{}, err
		}
		res.SearchBenchmarks = searchBenchmarks
	}
	if len(res.SearchBenchmarks) > 0 {
		res.Search = res.SearchBenchmarks[0]
	}
	if err := closeReopened(); err != nil {
		return result{}, err
	}
	return res, nil
}

func allocDeltaBytes(after, before uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if after >= before {
		delta := after - before
		if delta > maxInt64 {
			return int64(maxInt64)
		}
		return int64(delta)
	}
	delta := before - after
	if delta > maxInt64 {
		return -int64(maxInt64)
	}
	return -int64(delta)
}

func executeMatrix(ctx context.Context, cfg config) (matrixResult, error) {
	if cfg.vectorIndexStrategy == "" {
		cfg.vectorIndexStrategy = collections.VectorIndexStrategyNativeRuntime
	}
	cfg.profile = defaultProfileForStrategy(cfg.profile, cfg.vectorIndexStrategy)
	if err := validateDemoProfile(cfg.profile, cfg.vectorIndexStrategy); err != nil {
		return matrixResult{}, err
	}
	if err := applySearchBenchmarkDefaults(&cfg); err != nil {
		return matrixResult{}, err
	}
	if cfg.searchProfileDir != "" {
		cfg.searchProfileDir = filepath.Clean(cfg.searchProfileDir)
	}
	root, err := normalizeDemoDir(cfg.dir)
	if err != nil {
		return matrixResult{}, err
	}
	explicitRoot := root != ""
	cleanup := func() {}
	if root == "" {
		tmp, err := os.MkdirTemp("", "treedb-vector-search-matrix-*")
		if err != nil {
			return matrixResult{}, err
		}
		root = tmp
		if !cfg.keepDir {
			cleanup = func() { _ = os.RemoveAll(tmp) }
		}
	} else {
		info, err := os.Stat(root)
		if err == nil {
			if !info.IsDir() {
				return matrixResult{}, fmt.Errorf("-dir %q exists and is not a directory", root)
			}
			entries, err := os.ReadDir(root)
			if err != nil {
				return matrixResult{}, err
			}
			if len(entries) > 0 {
				return matrixResult{}, fmt.Errorf("-dir %q already exists and is not empty; choose a new directory", root)
			}
		} else if errors.Is(err, os.ErrNotExist) {
			if err := os.MkdirAll(root, 0o755); err != nil {
				return matrixResult{}, err
			}
		} else {
			return matrixResult{}, err
		}
	}
	if !cfg.keepDir {
		defer cleanup()
	}

	inlineLeaves := false
	leafVLog := true
	cases := []struct {
		name        string
		description string
		compact     bool
		outerLeaves *bool
	}{
		{
			name:        "index_db_outer_leaves",
			description: "1558-style layout: outer B-tree leaves stay in index.db",
			compact:     false,
			outerLeaves: &inlineLeaves,
		},
		{
			name:        "leaf_vlog_before_compact",
			description: "1560 layout: outer B-tree leaves in leaf_vlog before CompactStorageFull",
			compact:     false,
			outerLeaves: &leafVLog,
		},
		{
			name:        "leaf_vlog_after_compact",
			description: "1560 layout: outer B-tree leaves in leaf_vlog after CompactStorageFull",
			compact:     true,
			outerLeaves: &leafVLog,
		},
	}
	out := matrixResult{
		Dir:               root,
		KeptDir:           cfg.keepDir || explicitRoot,
		Profile:           string(cfg.profile),
		SearchProfileDir:  cfg.searchProfileDir,
		Docs:              cfg.docs,
		Dimensions:        cfg.dimensions,
		Queries:           cfg.queries,
		SearchConcurrency: append([]int(nil), cfg.searchConcurrency...),
		TopK:              cfg.topK,
		Cases:             make([]matrixCaseResult, 0, len(cases)),
	}
	for _, testCase := range cases {
		caseCfg := cfg
		caseCfg.matrix = false
		caseCfg.keepDir = true
		caseCfg.compact = testCase.compact
		caseCfg.dir = filepath.Join(root, testCase.name)
		if cfg.searchProfileDir != "" {
			caseCfg.searchProfileDir = filepath.Join(cfg.searchProfileDir, testCase.name)
		}
		caseCfg.indexOuterLeavesInValueLog = testCase.outerLeaves
		if testCase.name == "index_db_outer_leaves" {
			caseCfg.requireLeafVLogBytes = false
		}
		res, err := execute(ctx, caseCfg)
		if err != nil {
			return matrixResult{}, fmt.Errorf("%s: %w", testCase.name, err)
		}
		res.KeptDir = out.KeptDir
		out.Cases = append(out.Cases, matrixCaseResult{
			Name:        testCase.name,
			Description: testCase.description,
			Result:      res,
		})
	}
	return out, nil
}

func loadWorkload(cfg *config) (workload, error) {
	if cfg.datasetDir == "" {
		return workload{}, nil
	}
	m, err := loadDatasetManifest(cfg.datasetDir)
	if err != nil {
		return workload{}, err
	}
	if m.Version != 1 {
		return workload{}, fmt.Errorf("unsupported dataset manifest version %d", m.Version)
	}
	if m.Docs != cfg.docs {
		return workload{}, fmt.Errorf("dataset docs=%d does not match -docs=%d", m.Docs, cfg.docs)
	}
	if m.Dimensions != cfg.dimensions {
		return workload{}, fmt.Errorf("dataset dims=%d does not match -dims=%d", m.Dimensions, cfg.dimensions)
	}
	if cfg.queries > m.Queries {
		return workload{}, fmt.Errorf("dataset queries=%d is less than -queries=%d", m.Queries, cfg.queries)
	}
	if m.ExactTruthQueries < 0 || m.ExactTruthQueries > m.Queries {
		return workload{}, fmt.Errorf("dataset exact_truth_queries=%d is outside 0..queries=%d", m.ExactTruthQueries, m.Queries)
	}
	if cfg.validateQueries > m.Queries {
		cfg.validateQueries = m.Queries
	}
	if m.Metric != "" && m.Metric != "cosine" {
		return workload{}, fmt.Errorf("dataset metric=%q, want cosine", m.Metric)
	}
	if m.FloatFormat != "" && m.FloatFormat != "float32_le_row_major" {
		return workload{}, fmt.Errorf("dataset float_format=%q, want float32_le_row_major", m.FloatFormat)
	}
	return workload{datasetDir: cfg.datasetDir, manifest: m}, nil
}

func loadDatasetManifest(dir string) (datasetManifest, error) {
	path := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return datasetManifest{}, fmt.Errorf("read dataset manifest: %w", err)
	}
	var m datasetManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return datasetManifest{}, fmt.Errorf("decode dataset manifest: %w", err)
	}
	if m.DocumentsJSONLFile == "" {
		m.DocumentsJSONLFile = "documents.jsonl"
	}
	if m.DocumentVectorsFile == "" {
		m.DocumentVectorsFile = "documents.f32"
	}
	if m.QueryVectorsFile == "" {
		m.QueryVectorsFile = "queries.f32"
	}
	return m, nil
}

func datasetPath(work workload, name, fallback string) string {
	if name == "" {
		name = fallback
	}
	return filepath.Join(work.datasetDir, filepath.Clean(name))
}

func insertDocuments(col *collections.Collection, cfg config, work workload) error {
	if work.datasetDir != "" {
		return insertDatasetDocuments(col, cfg, work)
	}
	for start := 0; start < cfg.docs; start += cfg.batchSize {
		end := start + cfg.batchSize
		if end > cfg.docs {
			end = cfg.docs
		}
		ids := make([][]byte, end-start)
		documents := make([][]byte, end-start)
		for i := start; i < end; i++ {
			j := i - start
			ids[j] = documentID(i)
			documents[j] = documentJSON(i, cfg.dimensions)
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			return err
		}
	}
	return nil
}

func insertDatasetDocuments(col *collections.Collection, cfg config, work workload) error {
	f, err := os.Open(datasetPath(work, work.manifest.DocumentsJSONLFile, "documents.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	ids := make([][]byte, 0, cfg.batchSize)
	documents := make([][]byte, 0, cfg.batchSize)
	rows := 0
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		_, err := col.InsertBatch(ids, documents)
		ids = ids[:0]
		documents = documents[:0]
		return err
	}
	for i := 0; scanner.Scan(); i++ {
		if i >= cfg.docs {
			return fmt.Errorf("dataset documents file has more than %d rows", cfg.docs)
		}
		line := append([]byte(nil), scanner.Bytes()...)
		var header datasetDocumentHeader
		if err := json.Unmarshal(line, &header); err != nil {
			return fmt.Errorf("decode dataset document %d: %w", i, err)
		}
		if header.Index != i {
			return fmt.Errorf("dataset document index=%d at row %d", header.Index, i)
		}
		if header.ID == "" {
			return fmt.Errorf("dataset document %d has empty id", i)
		}
		ids = append(ids, []byte(header.ID))
		documents = append(documents, line)
		rows++
		if len(ids) == cfg.batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if rows != cfg.docs {
		return fmt.Errorf("dataset documents rows=%d, want %d", rows, cfg.docs)
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}

func validateDatasetDocuments(cfg config, work workload) error {
	f, err := os.Open(datasetPath(work, work.manifest.DocumentsJSONLFile, "documents.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	rows := 0
	for i := 0; scanner.Scan(); i++ {
		if i >= cfg.docs {
			return fmt.Errorf("dataset documents file has more than %d rows", cfg.docs)
		}
		var header datasetDocumentHeader
		if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
			return fmt.Errorf("decode dataset document %d: %w", i, err)
		}
		if header.Index != i {
			return fmt.Errorf("dataset document index=%d at row %d", header.Index, i)
		}
		if header.ID == "" {
			return fmt.Errorf("dataset document %d has empty id", i)
		}
		rows++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if rows != cfg.docs {
		return fmt.Errorf("dataset documents rows=%d, want %d", rows, cfg.docs)
	}
	return nil
}

func validateCompactedData(col *collections.Collection, idx *collections.VectorIndex, cfg config, work workload) (out validationResult, err error) {
	start := time.Now()
	defer func() {
		elapsed := phaseSince(start)
		out.DurationNanos = elapsed.DurationNanos
		out.Seconds = elapsed.Seconds
	}()
	out = validationResult{
		DocumentsChecked: cfg.validateDocs,
		QueriesChecked:   cfg.validateQueries,
		ExactSource:      cfg.validationExactSource,
		MinRecall:        cfg.minRecall,
	}
	expectedDocs, err := expectedValidationDocuments(cfg, work)
	if err != nil {
		return out, err
	}
	for i := 0; i < cfg.validateDocs; i++ {
		expected := expectedDocs[i]
		id := expected.id
		want := expected.line
		got, err := col.Get(id)
		if err != nil {
			return out, fmt.Errorf("get compacted doc %q: %w", id, err)
		}
		if !bytes.Equal(got, want) {
			return out, fmt.Errorf("compacted doc %d mismatch", expected.index)
		}
	}
	if cfg.validateQueries == 0 {
		return out, nil
	}
	queries, err := loadQueries(cfg.validateQueries, cfg, work, 0)
	if err != nil {
		return out, err
	}
	if cfg.validationExactSource == validationExactSourceDataset {
		exactSets, err := datasetExactIDSets(queries, cfg, work)
		if err != nil {
			return out, err
		}
		for i, query := range queries {
			ann, _, err := idx.Search(query, collections.VectorIndexSearchOptions{
				TopK:                 cfg.topK,
				EfSearch:             cfg.efSearch,
				DisableExactFallback: true,
			})
			if err != nil {
				return out, err
			}
			exactIDs := exactSets[i]
			out.ExactTotal += len(exactIDs)
			out.ANNTotal += len(ann)
			for _, result := range ann {
				if _, ok := exactIDs[string(result.DocumentID)]; ok {
					out.Overlap++
				}
			}
		}
	} else {
		// Recall validation disables exact fallback on the ANN side so it measures
		// the graph result against the exact baseline computed inside CheckRecall.
		recall, err := idx.CheckRecall(queries, collections.VectorIndexSearchOptions{
			TopK:                 cfg.topK,
			EfSearch:             cfg.efSearch,
			DisableExactFallback: true,
		})
		if err != nil {
			return out, err
		}
		out.ExactTotal = recall.ExactTotal
		out.ANNTotal = recall.ANNTotal
		out.Overlap = recall.Overlap
		out.Recall = recall.Recall
	}
	if out.ExactTotal > 0 {
		out.Recall = float64(out.Overlap) / float64(out.ExactTotal)
	}
	if out.Recall < cfg.minRecall {
		return out, fmt.Errorf("recall %.4f below minimum %.4f", out.Recall, cfg.minRecall)
	}
	return out, nil
}

func validateCompactedColumnGraphData(col *collections.Collection, indexName string, cfg config, work workload) (out validationResult, err error) {
	start := time.Now()
	defer func() {
		elapsed := phaseSince(start)
		out.DurationNanos = elapsed.DurationNanos
		out.Seconds = elapsed.Seconds
	}()
	out = validationResult{
		DocumentsChecked: cfg.validateDocs,
		QueriesChecked:   cfg.validateQueries,
		ExactSource:      cfg.validationExactSource,
		MinRecall:        cfg.minRecall,
	}
	expectedDocs, err := expectedValidationDocuments(cfg, work)
	if err != nil {
		return out, err
	}
	for i := 0; i < cfg.validateDocs; i++ {
		expected := expectedDocs[i]
		got, err := col.Get(expected.id)
		if err != nil {
			return out, fmt.Errorf("get compacted doc %q: %w", expected.id, err)
		}
		if !jsonDocumentsEqual(got, expected.line) {
			return out, fmt.Errorf("compacted doc %d mismatch", expected.index)
		}
	}
	if cfg.validateQueries == 0 {
		return out, nil
	}
	queries, err := loadQueries(cfg.validateQueries, cfg, work, 0)
	if err != nil {
		return out, err
	}
	var datasetExact []map[string]struct{}
	if cfg.validationExactSource == validationExactSourceDataset {
		datasetExact, err = datasetExactIDSets(queries, cfg, work)
		if err != nil {
			return out, err
		}
	}
	searcher, err := col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: indexName})
	if err != nil {
		return out, err
	}
	defer func() {
		if closeErr := searcher.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	for i, query := range queries {
		var exactIDs map[string]struct{}
		if cfg.validationExactSource == validationExactSourceDataset {
			exactIDs = datasetExact[i]
		} else {
			exact, err := col.SearchVectorsExact(query, collections.VectorSearchOptions{
				Field:  "embedding",
				Metric: collections.VectorMetricCosine,
				TopK:   cfg.topK,
			})
			if err != nil {
				return out, err
			}
			exactIDs = make(map[string]struct{}, len(exact))
			for _, result := range exact {
				exactIDs[string(result.DocumentID)] = struct{}{}
			}
		}
		response, err := searcher.Search(columnGraphSearchOptions(query, cfg))
		if err != nil {
			return out, err
		}
		if response.Path != collections.VectorIndexSearchPathColumnGraphNativeReader {
			return out, fmt.Errorf("unexpected column_graph validation path %q", response.Path)
		}
		out.ExactTotal += len(exactIDs)
		out.ANNTotal += len(response.Results)
		for _, result := range response.Results {
			if _, ok := exactIDs[string(result.ID)]; ok {
				out.Overlap++
			}
		}
	}
	if out.ExactTotal > 0 {
		out.Recall = float64(out.Overlap) / float64(out.ExactTotal)
	}
	if out.Recall < cfg.minRecall {
		return out, fmt.Errorf("recall %.4f below minimum %.4f", out.Recall, cfg.minRecall)
	}
	return out, nil
}

type datasetExactCandidate struct {
	id    string
	index int
	score float64
}

func datasetExactIDSets(queries [][]float32, cfg config, work workload) ([]map[string]struct{}, error) {
	if work.datasetDir == "" {
		return nil, errors.New("dataset exact validation requires -dataset-dir")
	}
	documentIDs, err := datasetDocumentIDs(cfg, work)
	if err != nil {
		return nil, err
	}
	documentVectors, err := readFloat32Matrix(datasetPath(work, work.manifest.DocumentVectorsFile, "documents.f32"), cfg.docs, cfg.dimensions)
	if err != nil {
		return nil, err
	}
	documentNorms, err := vectorNorms(documentVectors, cfg.docs, cfg.dimensions)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]struct{}, len(queries))
	for i, query := range queries {
		ids, err := datasetExactTopKIDs(documentVectors, documentNorms, documentIDs, query, cfg.topK, cfg.dimensions)
		if err != nil {
			return nil, fmt.Errorf("dataset exact query %d: %w", i, err)
		}
		out[i] = ids
	}
	return out, nil
}

func datasetDocumentIDs(cfg config, work workload) ([]string, error) {
	ids := make([]string, cfg.docs)
	f, err := os.Open(datasetPath(work, work.manifest.DocumentsJSONLFile, "documents.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	rows := 0
	for i := 0; scanner.Scan(); i++ {
		if i >= cfg.docs {
			return nil, fmt.Errorf("dataset documents file has more than %d rows", cfg.docs)
		}
		var header datasetDocumentHeader
		if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
			return nil, fmt.Errorf("decode dataset document %d: %w", i, err)
		}
		if header.Index != i {
			return nil, fmt.Errorf("dataset document index=%d at row %d", header.Index, i)
		}
		if header.ID == "" {
			return nil, fmt.Errorf("dataset document %d has empty id", i)
		}
		ids[i] = header.ID
		rows++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if rows != cfg.docs {
		return nil, fmt.Errorf("dataset documents rows=%d, want %d", rows, cfg.docs)
	}
	return ids, nil
}

func datasetExactTopKIDs(documentVectors []float32, documentNorms []float64, documentIDs []string, query []float32, topK, dims int) (map[string]struct{}, error) {
	if len(query) != dims {
		return nil, fmt.Errorf("query dimensions=%d want %d", len(query), dims)
	}
	if len(documentIDs)*dims != len(documentVectors) {
		return nil, fmt.Errorf("document vector matrix has %d floats for %d ids and %d dims", len(documentVectors), len(documentIDs), dims)
	}
	if len(documentNorms) != len(documentIDs) {
		return nil, fmt.Errorf("document norm count=%d want %d", len(documentNorms), len(documentIDs))
	}
	queryNorm, err := vectorNorm(query)
	if err != nil {
		return nil, fmt.Errorf("query vector: %w", err)
	}
	top := make([]datasetExactCandidate, 0, topK)
	for i, id := range documentIDs {
		row := documentVectors[i*dims : (i+1)*dims]
		score := cosineScore(query, queryNorm, row, documentNorms[i])
		candidate := datasetExactCandidate{id: id, index: i, score: score}
		top = appendDatasetExactCandidate(top, candidate, topK)
	}
	sort.Slice(top, func(i, j int) bool { return datasetExactCandidateBetter(top[i], top[j]) })
	ids := make(map[string]struct{}, len(top))
	for _, candidate := range top {
		ids[candidate.id] = struct{}{}
	}
	return ids, nil
}

func appendDatasetExactCandidate(top []datasetExactCandidate, candidate datasetExactCandidate, limit int) []datasetExactCandidate {
	if len(top) < limit {
		return append(top, candidate)
	}
	worst := 0
	for i := 1; i < len(top); i++ {
		if datasetExactCandidateBetter(top[worst], top[i]) {
			worst = i
		}
	}
	if datasetExactCandidateBetter(candidate, top[worst]) {
		top[worst] = candidate
	}
	return top
}

func datasetExactCandidateBetter(a, b datasetExactCandidate) bool {
	if a.score == b.score {
		return a.index < b.index
	}
	return a.score > b.score
}

func vectorNorm(v []float32) (float64, error) {
	var sum float64
	for _, value := range v {
		f := float64(value)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("non-finite value %v", value)
		}
		sum += f * f
	}
	if sum == 0 {
		return 0, errors.New("zero magnitude")
	}
	return math.Sqrt(sum), nil
}

func vectorNorms(matrix []float32, count, dims int) ([]float64, error) {
	if len(matrix) != count*dims {
		return nil, fmt.Errorf("vector matrix has %d floats for %d rows and %d dims", len(matrix), count, dims)
	}
	out := make([]float64, count)
	for i := 0; i < count; i++ {
		norm, err := vectorNorm(matrix[i*dims : (i+1)*dims])
		if err != nil {
			return nil, fmt.Errorf("document vector %d: %w", i, err)
		}
		out[i] = norm
	}
	return out, nil
}

func cosineScore(query []float32, queryNorm float64, document []float32, documentNorm float64) float64 {
	var dot float64
	for i, value := range query {
		dot += float64(value) * float64(document[i])
	}
	return dot / (queryNorm * documentNorm)
}

func readFloat32Matrix(path string, count, dims int) ([]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	rowBytes := dims * 4
	buf := make([]byte, rowBytes)
	out := make([]float32, count*dims)
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read vector %d from %s: %w", i, path, err)
		}
		base := i * dims
		for j := 0; j < dims; j++ {
			out[base+j] = math.Float32frombits(binary.LittleEndian.Uint32(buf[j*4:]))
		}
	}
	return out, nil
}

func jsonDocumentsEqual(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	var av any
	var bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

func expectedDocument(docIndex int, cfg config, work workload) ([]byte, []byte, error) {
	if work.datasetDir == "" {
		return documentID(docIndex), documentJSON(docIndex, cfg.dimensions), nil
	}
	line, header, err := datasetDocument(docIndex, work)
	if err != nil {
		return nil, nil, err
	}
	return []byte(header.ID), line, nil
}

type expectedValidationDocument struct {
	index int
	id    []byte
	line  []byte
}

func expectedValidationDocuments(cfg config, work workload) ([]expectedValidationDocument, error) {
	out := make([]expectedValidationDocument, cfg.validateDocs)
	if cfg.validateDocs == 0 {
		return out, nil
	}
	if work.datasetDir == "" {
		for i := 0; i < cfg.validateDocs; i++ {
			docIndex := validationDocIndex(i, cfg.docs)
			out[i] = expectedValidationDocument{
				index: docIndex,
				id:    documentID(docIndex),
				line:  documentJSON(docIndex, cfg.dimensions),
			}
		}
		return out, nil
	}
	positions := make(map[int][]int, cfg.validateDocs)
	indexes := make([]int, cfg.validateDocs)
	for i := 0; i < cfg.validateDocs; i++ {
		docIndex := validationDocIndex(i, cfg.docs)
		indexes[i] = docIndex
		positions[docIndex] = append(positions[docIndex], i)
	}
	if err := datasetDocuments(indexes, work, func(docIndex int, line []byte, header datasetDocumentHeader) {
		for _, pos := range positions[docIndex] {
			out[pos] = expectedValidationDocument{
				index: docIndex,
				id:    []byte(header.ID),
				line:  append([]byte(nil), line...),
			}
		}
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func datasetDocument(docIndex int, work workload) ([]byte, datasetDocumentHeader, error) {
	var line []byte
	var header datasetDocumentHeader
	err := datasetDocuments([]int{docIndex}, work, func(_ int, gotLine []byte, gotHeader datasetDocumentHeader) {
		line = append([]byte(nil), gotLine...)
		header = gotHeader
	})
	if err != nil {
		return nil, datasetDocumentHeader{}, err
	}
	return line, header, nil
}

func datasetDocuments(docIndexes []int, work workload, visit func(docIndex int, line []byte, header datasetDocumentHeader)) error {
	if len(docIndexes) == 0 {
		return nil
	}
	remaining := make(map[int]struct{}, len(docIndexes))
	for _, docIndex := range docIndexes {
		remaining[docIndex] = struct{}{}
	}
	f, err := os.Open(datasetPath(work, work.manifest.DocumentsJSONLFile, "documents.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for i := 0; scanner.Scan(); i++ {
		if _, ok := remaining[i]; !ok {
			continue
		}
		line := append([]byte(nil), scanner.Bytes()...)
		var header datasetDocumentHeader
		if err := json.Unmarshal(line, &header); err != nil {
			return fmt.Errorf("decode dataset document %d: %w", i, err)
		}
		if header.Index != i {
			return fmt.Errorf("dataset document index=%d at row %d", header.Index, i)
		}
		if header.ID == "" {
			return fmt.Errorf("dataset document %d has empty id", i)
		}
		visit(i, line, header)
		delete(remaining, i)
		if len(remaining) == 0 {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	missing := make([]int, 0, len(remaining))
	for docIndex := range remaining {
		missing = append(missing, docIndex)
	}
	sort.Ints(missing)
	return fmt.Errorf("dataset document %d not found", missing[0])
}

func benchmarkSearch(idx *collections.VectorIndex, cfg config, work workload) (searchBenchmarkResult, error) {
	return benchmarkSearchConcurrent(idx, cfg, work, 1)
}

func benchmarkSearchMatrix(idx *collections.VectorIndex, cfg config, work workload) ([]searchBenchmarkResult, error) {
	levels := make([]int, 0, len(cfg.searchConcurrency)+1)
	levels = append(levels, 1)
	levels = append(levels, cfg.searchConcurrency...)
	queries, err := loadQueries(cfg.queries, cfg, work, cfg.validateQueries)
	if err != nil {
		return nil, err
	}
	out := make([]searchBenchmarkResult, 0, len(levels))
	for _, concurrency := range levels {
		bench, err := benchmarkSearchLoadedConcurrent(idx, cfg, queries, concurrency)
		if err != nil {
			return nil, err
		}
		out = append(out, bench)
	}
	return out, nil
}

func benchmarkSearchConcurrent(idx *collections.VectorIndex, cfg config, work workload, concurrency int) (searchBenchmarkResult, error) {
	if concurrency <= 0 {
		return searchBenchmarkResult{}, errors.New("search concurrency must be positive")
	}
	queries, err := loadQueries(cfg.queries, cfg, work, cfg.validateQueries)
	if err != nil {
		return searchBenchmarkResult{}, err
	}
	return benchmarkSearchLoadedConcurrent(idx, cfg, queries, concurrency)
}

func benchmarkSearchLoadedConcurrent(idx *collections.VectorIndex, cfg config, queries [][]float32, concurrency int) (searchBenchmarkResult, error) {
	if concurrency <= 0 {
		return searchBenchmarkResult{}, errors.New("search concurrency must be positive")
	}
	latencies := make([]int64, len(queries))
	var next atomic.Int64
	var candidatesTotal int64
	var rerankTotal int64
	var exactFallbacks int64
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	worker := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= len(queries) {
				return
			}
			start := time.Now()
			results, trace, err := idx.Search(queries[i], collections.VectorIndexSearchOptions{
				TopK:                 cfg.topK,
				EfSearch:             cfg.efSearch,
				DisableExactFallback: cfg.disableExactFallback,
			})
			if err != nil {
				setErr(err)
				return
			}
			latencies[i] = time.Since(start).Nanoseconds()
			if len(results) == 0 {
				setErr(errors.New("vector search returned no results"))
				return
			}
			atomic.AddInt64(&candidatesTotal, int64(trace.CandidatesExamined))
			atomic.AddInt64(&rerankTotal, int64(trace.RerankCount))
			if trace.ExactFallbackReason != "" {
				atomic.AddInt64(&exactFallbacks, 1)
			}
		}
	}
	startAll := time.Now()
	if concurrency == 1 {
		worker()
	} else {
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				worker()
			}()
		}
		wg.Wait()
	}
	total := time.Since(startAll)
	if firstErr != nil {
		return searchBenchmarkResult{}, firstErr
	}
	var latencyTotal int64
	for _, latency := range latencies {
		latencyTotal += latency
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	avg := float64(latencyTotal) / float64(len(queries))
	opsPerSecond := 0.0
	if total > 0 {
		opsPerSecond = float64(len(queries)) / total.Seconds()
	}
	return searchBenchmarkResult{
		Concurrency:          concurrency,
		Queries:              len(queries),
		QueryMode:            collections.VectorIndexQueryModeExact,
		TotalDurationNanos:   total.Nanoseconds(),
		AvgNanos:             avg,
		AvgMicros:            avg / 1000,
		OpsPerSecond:         opsPerSecond,
		P50Nanos:             percentile(latencies, 0.50),
		P95Nanos:             percentile(latencies, 0.95),
		P99Nanos:             percentile(latencies, 0.99),
		AvgCandidates:        float64(candidatesTotal) / float64(len(queries)),
		AvgRerank:            float64(rerankTotal) / float64(len(queries)),
		ExactFallbacks:       int(exactFallbacks),
		DisableExactFallback: cfg.disableExactFallback,
	}, nil
}

func benchmarkColumnGraphSearchMatrix(col *collections.Collection, indexName string, cfg config, work workload) ([]searchBenchmarkResult, error) {
	levels := make([]int, 0, len(cfg.searchConcurrency)+1)
	levels = append(levels, 1)
	levels = append(levels, cfg.searchConcurrency...)
	queries, err := loadQueries(cfg.queries, cfg, work, cfg.validateQueries)
	if err != nil {
		return nil, err
	}
	out := make([]searchBenchmarkResult, 0, len(levels))
	for _, concurrency := range levels {
		bench, err := benchmarkColumnGraphSearchLoadedConcurrent(col, indexName, cfg, queries, concurrency)
		if err != nil {
			return nil, err
		}
		out = append(out, bench)
	}
	return out, nil
}

func benchmarkColumnGraphSearchLoadedConcurrent(col *collections.Collection, indexName string, cfg config, queries [][]float32, concurrency int) (searchBenchmarkResult, error) {
	if concurrency <= 0 {
		return searchBenchmarkResult{}, errors.New("search concurrency must be positive")
	}
	searchers := make([]*collections.VectorIndexSearcher, concurrency)
	for i := range searchers {
		searcher, err := col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: indexName})
		if err != nil {
			for _, opened := range searchers[:i] {
				_ = opened.Close()
			}
			return searchBenchmarkResult{}, err
		}
		searchers[i] = searcher
	}
	defer func() {
		for _, searcher := range searchers {
			_ = searcher.Close()
		}
	}()
	buffers := make([]collections.VectorIndexSearchBuffer, concurrency)

	latencies := make([]int64, len(queries))
	var next atomic.Int64
	var candidatesTotal uint64
	var quantizedScoreCallsTotal uint64
	var quantizedCodeBytesTotal uint64
	var quantizedRerankCandidatesTotal uint64
	var quantizedRerankExactScoreCallsTotal uint64
	var vectorBytesTotal uint64
	var normBytesTotal uint64
	var preparedScoreCallsTotal uint64
	var scoreBatchCandidatesTotal uint64
	type columnGraphGuardrailTotals struct {
		documentsFetched               uint64
		responseOwnedResultAllocs      uint64
		searchRouteHNSWSearchPack      uint64
		searchRouteQuantizedOnly       uint64
		searchRouteQuantizedRerank     uint64
		searchRouteColumnGraphPrepared uint64
		searchRouteColumnGraphFallback uint64
		graphRowFallbacks              uint64
		typedColumnFallbacks           uint64
		vectorScratchDecodes           uint64
	}
	guardrailTotals := make([]columnGraphGuardrailTotals, concurrency)
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	worker := func(searcher *collections.VectorIndexSearcher, buffer *collections.VectorIndexSearchBuffer, totals *columnGraphGuardrailTotals) {
		var local columnGraphGuardrailTotals
		defer func() {
			if totals != nil {
				*totals = local
			}
		}()
		for {
			i := int(next.Add(1) - 1)
			if i >= len(queries) {
				return
			}
			start := time.Now()
			response, err := searcher.SearchWithBuffer(columnGraphSearchOptions(queries[i], cfg), buffer)
			if err != nil {
				setErr(err)
				return
			}
			latencies[i] = time.Since(start).Nanoseconds()
			if response.Strategy != collections.VectorIndexStrategyColumnGraph || response.Path != collections.VectorIndexSearchPathColumnGraphNativeReader {
				setErr(fmt.Errorf("unexpected column_graph benchmark path: strategy=%s path=%s", response.Strategy, response.Path))
				return
			}
			if len(response.Results) == 0 {
				setErr(errors.New("vector search returned no results"))
				return
			}
			atomic.AddUint64(&candidatesTotal, response.Stats.Candidates)
			atomic.AddUint64(&quantizedScoreCallsTotal, response.Stats.QuantizedScoreCalls)
			atomic.AddUint64(&quantizedCodeBytesTotal, response.Stats.QuantizedCodeBytesRead)
			atomic.AddUint64(&quantizedRerankCandidatesTotal, response.Stats.QuantizedRerankCandidates)
			atomic.AddUint64(&quantizedRerankExactScoreCallsTotal, response.Stats.QuantizedRerankExactScoreCalls)
			atomic.AddUint64(&vectorBytesTotal, response.Stats.VectorBytesRead)
			atomic.AddUint64(&normBytesTotal, response.Stats.NormBytesRead)
			atomic.AddUint64(&preparedScoreCallsTotal, response.Stats.PreparedScoreCalls)
			atomic.AddUint64(&scoreBatchCandidatesTotal, response.Stats.ScoreBatchCandidates)
			local.documentsFetched += response.Stats.DocumentsFetched
			local.responseOwnedResultAllocs += response.Stats.ResponseOwnedResultAllocs
			local.searchRouteHNSWSearchPack += response.Stats.SearchRouteHNSWSearchPack
			local.searchRouteQuantizedOnly += response.Stats.SearchRouteQuantizedOnly
			local.searchRouteQuantizedRerank += response.Stats.SearchRouteQuantizedRerank
			local.searchRouteColumnGraphPrepared += response.Stats.SearchRouteColumnGraphPrepared
			local.searchRouteColumnGraphFallback += response.Stats.SearchRouteColumnGraphFallback
			local.graphRowFallbacks += response.Stats.GraphRowFallbacks
			local.typedColumnFallbacks += response.Stats.TypedColumnFallbacks
			local.vectorScratchDecodes += response.Stats.VectorScratchDecodes
		}
	}
	stopProfile, err := startColumnGraphSearchProfile(cfg, concurrency)
	if err != nil {
		return searchBenchmarkResult{}, err
	}
	startAll := time.Now()
	if concurrency == 1 {
		worker(searchers[0], &buffers[0], &guardrailTotals[0])
	} else {
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			searcher := searchers[i]
			buffer := &buffers[i]
			totals := &guardrailTotals[i]
			go func() {
				defer wg.Done()
				worker(searcher, buffer, totals)
			}()
		}
		wg.Wait()
	}
	total := time.Since(startAll)
	if stopProfile != nil {
		if err := stopProfile(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return searchBenchmarkResult{}, firstErr
	}
	var latencyTotal int64
	for _, latency := range latencies {
		latencyTotal += latency
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	avg := float64(latencyTotal) / float64(len(queries))
	opsPerSecond := 0.0
	if total > 0 {
		opsPerSecond = float64(len(queries)) / total.Seconds()
	}
	var documentsFetchedTotal uint64
	var responseOwnedResultAllocsTotal uint64
	var searchRouteHNSWSearchPackTotal uint64
	var searchRouteQuantizedOnlyTotal uint64
	var searchRouteQuantizedRerankTotal uint64
	var searchRouteColumnGraphPreparedTotal uint64
	var searchRouteColumnGraphFallbackTotal uint64
	var graphRowFallbacksTotal uint64
	var typedColumnFallbacksTotal uint64
	var vectorScratchDecodesTotal uint64
	for _, totals := range guardrailTotals {
		documentsFetchedTotal += totals.documentsFetched
		responseOwnedResultAllocsTotal += totals.responseOwnedResultAllocs
		searchRouteHNSWSearchPackTotal += totals.searchRouteHNSWSearchPack
		searchRouteQuantizedOnlyTotal += totals.searchRouteQuantizedOnly
		searchRouteQuantizedRerankTotal += totals.searchRouteQuantizedRerank
		searchRouteColumnGraphPreparedTotal += totals.searchRouteColumnGraphPrepared
		searchRouteColumnGraphFallbackTotal += totals.searchRouteColumnGraphFallback
		graphRowFallbacksTotal += totals.graphRowFallbacks
		typedColumnFallbacksTotal += totals.typedColumnFallbacks
		vectorScratchDecodesTotal += totals.vectorScratchDecodes
	}
	queryCount := float64(len(queries))
	avgQuantizedRerankCandidates := float64(quantizedRerankCandidatesTotal) / queryCount
	return searchBenchmarkResult{
		Concurrency:                       concurrency,
		Queries:                           len(queries),
		QueryMode:                         normalizedVectorQueryMode(cfg.vectorQueryMode),
		QuantizedCodec:                    effectiveQuantizedCodec(cfg),
		QuantizedIndexName:                effectiveQuantizedIndexName(cfg),
		QuantizedRerankCandidates:         effectiveQuantizedRerankCandidates(cfg),
		TotalDurationNanos:                total.Nanoseconds(),
		AvgNanos:                          avg,
		AvgMicros:                         avg / 1000,
		OpsPerSecond:                      opsPerSecond,
		P50Nanos:                          percentile(latencies, 0.50),
		P95Nanos:                          percentile(latencies, 0.95),
		P99Nanos:                          percentile(latencies, 0.99),
		AvgCandidates:                     float64(candidatesTotal) / queryCount,
		AvgRerank:                         avgQuantizedRerankCandidates,
		AvgQuantizedScoreCalls:            float64(quantizedScoreCallsTotal) / queryCount,
		AvgQuantizedCodeBytes:             float64(quantizedCodeBytesTotal) / queryCount,
		AvgQuantizedRerankCandidates:      avgQuantizedRerankCandidates,
		AvgQuantizedRerankExactScoreCalls: float64(quantizedRerankExactScoreCallsTotal) / queryCount,
		AvgVectorBytes:                    float64(vectorBytesTotal) / queryCount,
		AvgNormBytes:                      float64(normBytesTotal) / queryCount,
		AvgPreparedScoreCalls:             float64(preparedScoreCallsTotal) / queryCount,
		AvgScoreBatchCandidates:           float64(scoreBatchCandidatesTotal) / queryCount,
		AvgDocumentsFetched:               float64Ptr(float64(documentsFetchedTotal) / queryCount),
		AvgResponseOwnedResultAllocs:      float64Ptr(float64(responseOwnedResultAllocsTotal) / queryCount),
		AvgSearchRouteHNSWSearchPack:      float64Ptr(float64(searchRouteHNSWSearchPackTotal) / queryCount),
		AvgSearchRouteQuantizedOnly:       float64Ptr(float64(searchRouteQuantizedOnlyTotal) / queryCount),
		AvgSearchRouteQuantizedRerank:     float64Ptr(float64(searchRouteQuantizedRerankTotal) / queryCount),
		AvgSearchRouteColumnGraphPrepared: float64Ptr(float64(searchRouteColumnGraphPreparedTotal) / queryCount),
		AvgSearchRouteColumnGraphFallback: float64Ptr(float64(searchRouteColumnGraphFallbackTotal) / queryCount),
		AvgGraphRowFallbacks:              float64Ptr(float64(graphRowFallbacksTotal) / queryCount),
		AvgTypedColumnFallbacks:           float64Ptr(float64(typedColumnFallbacksTotal) / queryCount),
		AvgVectorScratchDecodes:           float64Ptr(float64(vectorScratchDecodesTotal) / queryCount),
		ExactFallbacks:                    0,
		DisableExactFallback:              cfg.disableExactFallback,
	}, nil
}

func startColumnGraphSearchProfile(cfg config, concurrency int) (func() error, error) {
	if cfg.searchProfileDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(cfg.searchProfileDir, 0o755); err != nil {
		return nil, fmt.Errorf("create search profile dir: %w", err)
	}
	mode := string(normalizedVectorQueryMode(cfg.vectorQueryMode))
	if mode == "" {
		mode = "exact"
	}
	prefix := fmt.Sprintf("search_%s_c%d", mode, concurrency)
	cpuFile, err := os.Create(filepath.Join(cfg.searchProfileDir, prefix+"_cpu.pprof"))
	if err != nil {
		return nil, fmt.Errorf("create search CPU profile: %w", err)
	}
	oldMutexProfileFraction := runtime.SetMutexProfileFraction(1)
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		restoreRuntimeSearchProfileSettings(oldMutexProfileFraction)
		return nil, fmt.Errorf("start search CPU profile: %w", err)
	}
	return func() error {
		pprof.StopCPUProfile()
		var errs []error
		if err := cpuFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close search CPU profile: %w", err))
		}
		runtime.GC()
		for _, name := range []string{"heap", "allocs", "block", "mutex"} {
			path := filepath.Join(cfg.searchProfileDir, prefix+"_"+name+".pprof")
			if err := writeRuntimeProfile(path, name); err != nil {
				errs = append(errs, err)
			}
		}
		restoreRuntimeSearchProfileSettings(oldMutexProfileFraction)
		return errors.Join(errs...)
	}, nil
}

func restoreRuntimeSearchProfileSettings(mutexProfileFraction int) {
	runtime.SetMutexProfileFraction(mutexProfileFraction)
}

func writeRuntimeProfile(path, name string) error {
	profile := pprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("runtime profile %q is unavailable", name)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s profile: %w", name, err)
	}
	writeErr := profile.WriteTo(f, 0)
	closeErr := f.Close()
	if writeErr != nil && closeErr != nil {
		return errors.Join(fmt.Errorf("write %s profile: %w", name, writeErr), fmt.Errorf("close %s profile: %w", name, closeErr))
	}
	if writeErr != nil {
		return fmt.Errorf("write %s profile: %w", name, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close %s profile: %w", name, closeErr)
	}
	return nil
}

func vectorIndexOptions(def collections.VectorIndexDefinition) collections.VectorIndexOptions {
	return collections.VectorIndexOptions{
		Name:           def.Name,
		Field:          def.Field,
		Metric:         def.Metric,
		Dimensions:     def.Dimensions,
		M:              def.M,
		EfConstruction: def.EfConstruction,
		EfSearch:       def.EfSearch,
		Encoding:       def.Encoding,
	}
}

func loadQueries(count int, cfg config, work workload, syntheticOffset int) ([][]float32, error) {
	if work.datasetDir == "" {
		return syntheticQueries(count, cfg.docs, cfg.dimensions, syntheticOffset), nil
	}
	return readFloat32Vectors(datasetPath(work, work.manifest.QueryVectorsFile, "queries.f32"), count, cfg.dimensions)
}

func readFloat32Vectors(path string, count, dims int) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	rowBytes := dims * 4
	buf := make([]byte, rowBytes)
	out := make([][]float32, count)
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read vector %d from %s: %w", i, path, err)
		}
		v := make([]float32, dims)
		for j := 0; j < dims; j++ {
			v[j] = math.Float32frombits(binary.LittleEndian.Uint32(buf[j*4:]))
		}
		out[i] = v
	}
	return out, nil
}

func validationQueries(count, docs, dims int) [][]float32 {
	return syntheticQueries(count, docs, dims, 0)
}

func syntheticQueries(count, docs, dims, offset int) [][]float32 {
	queries := make([][]float32, count)
	stride := queryDocStride(docs)
	start := 0
	span := docs
	if offset > 0 {
		if offset < docs {
			start = offset
			span = docs - offset
		} else {
			start = docs
			span = 0
		}
	}
	for i := 0; i < count; i++ {
		queries[i] = embedding(syntheticQueryID(i, docs, start, span, stride), dims)
	}
	return queries
}

func syntheticQueryID(i, docs, start, span, stride int) int {
	if docs <= 0 || span <= 0 {
		return start + i
	}
	if i < span {
		return queryDocIndex(start+i, docs, stride)
	}
	return docs + i - span
}

func queryDocStride(docs int) int {
	stride := 7919
	if docs > 0 {
		stride %= docs
	}
	if stride <= 0 {
		stride = 1
	}
	for gcd(stride, docs) != 1 {
		stride++
	}
	return stride
}

func queryDocIndex(i, docs, stride int) int {
	return (i*stride + docs/3 + 17) % docs
}

func gcd(a, b int) int {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func validationDocIndex(i, docs int) int {
	return (i*validationDocStride(docs) + docs/5 + 11) % docs
}

func validationDocStride(docs int) int {
	stride := 1543
	if docs > 0 {
		stride %= docs
	}
	if stride <= 0 {
		stride = 1
	}
	for gcd(stride, docs) != 1 {
		stride++
	}
	return stride
}

func documentID(id int) []byte {
	return []byte(fmt.Sprintf("doc-%06d", id))
}

func documentJSON(id, dims int) []byte {
	vector := embedding(id, dims)
	out := make([]byte, 0, 48+dims*16)
	out = append(out, `{"group":`...)
	out = strconv.AppendInt(out, int64(id%16), 10)
	out = append(out, `,"embedding":[`...)
	for i, value := range vector {
		if i > 0 {
			out = append(out, ',')
		}
		out = strconv.AppendFloat(out, float64(value), 'g', 7, 32)
	}
	out = append(out, ']', '}')
	return out
}

func embedding(id, dims int) []float32 {
	out := make([]float32, dims)
	var norm float64
	x := float64(id + 1)
	for i := range out {
		d := float64(i + 1)
		value := math.Sin(x*d*0.013) + math.Cos((x+17)*d*0.007) + math.Sin(float64((id%31)+1)*d*0.019)
		out[i] = float32(value)
		norm += value * value
	}
	if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		out[0] = 1
		return out
	}
	scale := 1 / math.Sqrt(norm)
	for i := range out {
		out[i] = float32(float64(out[i]) * scale)
	}
	return out
}

func storageUsage(dir string, docs int) (storageReport, error) {
	report := storageReport{Domains: make(map[string]int64)}
	err := filepath.WalkDir(dir, func(path string, ent os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ent.IsDir() {
			return nil
		}
		info, err := ent.Info()
		if err != nil {
			return err
		}
		size := info.Size()
		report.TotalBytes += size
		report.Files++
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		report.Domains[storageDomain(rel)] += size
		return nil
	})
	if err != nil {
		return storageReport{}, err
	}
	if docs > 0 {
		report.BytesPerDoc = float64(report.TotalBytes) / float64(docs)
	}
	return report, nil
}

func storageDomain(rel string) string {
	rel = filepath.ToSlash(rel)
	parts := strings.Split(rel, "/")
	if len(parts) == 0 {
		return rel
	}
	if len(parts) > 1 && parts[0] == "maindb" {
		return storageDomain(strings.Join(parts[1:], "/"))
	}
	if len(parts) > 1 && (parts[0] == "dictdb" || parts[0] == "templatedb") {
		return parts[0]
	}
	switch parts[0] {
	case "leaf_vlog", "value_vlog", "wal":
		return parts[0]
	case "index.db", "journal.wal", "format.json", "LOCK", "vlog_ref_counts.meta":
		return parts[0]
	default:
		return parts[0]
	}
}

func loadFormatConfig(dir string) (backenddb.FormatConfig, bool, error) {
	format, ok, err := backenddb.LoadFormatConfig(dir)
	if err != nil || ok {
		return format, ok, err
	}
	return backenddb.LoadFormatConfig(filepath.Join(dir, "maindb"))
}

func phaseSince(start time.Time) phaseResult {
	d := time.Since(start)
	return phaseResult{DurationNanos: d.Nanoseconds(), Seconds: d.Seconds()}
}

func percentile(sorted []int64, p float64) int64 {
	// Nearest-rank percentile over an already sorted sample.
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printText(w io.Writer, res result) {
	fmt.Fprintf(w, "TreeDB vector search demo\n")
	fmt.Fprintf(w, "dir=%s kept=%t profile=%s backend=%s vector_index_strategy=%s vector_index_search_path=%s query_mode=%s quantized_codec=%s quantized_index_name=%s quantized_rerank_candidates=%d docs=%d dims=%d queries=%d top_k=%d m=%d ef_construction=%d ef_search=%d value_pointer_threshold=%d leaf_generation_segment_target=%d\n",
		res.Dir, res.KeptDir, res.Profile, resultBackend(res), resultStrategy(res), resultSearchPath(res), resultQueryMode(res), res.QuantizedCodec, res.QuantizedIndexName, res.QuantizedRerankCandidates, res.Docs, res.Dimensions, res.Queries, res.TopK, res.M, res.EfConstruction, res.EfSearch, res.ValuePointerThreshold, res.LeafGenerationTarget)
	if res.SearchProfileDir != "" {
		fmt.Fprintf(w, "search_profile_dir=%s\n", res.SearchProfileDir)
	}
	fmt.Fprintf(w, "\nPhases\n")
	fmt.Fprintf(w, "insert: %.3fs\n", res.Insert.Seconds)
	fmt.Fprintf(w, "rebuild_vector_index strategy=%s: %.3fs native_root_bytes=%d\n", resultStrategy(res), res.Rebuild.Seconds, res.NativeRootBytes)
	if res.Compact {
		fully := false
		if res.CompactStorage != nil {
			fully = res.CompactStorage.FullyCompacted
		}
		fmt.Fprintf(w, "compact_storage_full: %.3fs fully_compacted=%t\n", res.CompactPhase.Seconds, fully)
	} else {
		fmt.Fprintf(w, "compact_storage_full: skipped\n")
	}
	if res.IndexVacuum.DurationNanos > 0 {
		fmt.Fprintf(w, "index_vacuum: %.3fs\n", res.IndexVacuum.Seconds)
	} else {
		fmt.Fprintf(w, "index_vacuum: skipped\n")
	}
	fmt.Fprintf(w, "reopen_and_load_vector_index path=%s: %.3fs\n", resultSearchPath(res), res.ReopenLoad.Seconds)
	fmt.Fprintf(w, "\nValidation\n")
	fmt.Fprintf(w, "documents_checked=%d queries_checked=%d exact_source=%s recall_at_%d=%.4f overlap=%d/%d\n",
		res.Validation.DocumentsChecked, res.Validation.QueriesChecked, res.Validation.ExactSource, res.TopK, res.Validation.Recall, res.Validation.Overlap, res.Validation.ExactTotal)
	fmt.Fprintf(w, "\nSearch Benchmark\n")
	fmt.Fprintf(w, "avg=%.2fus p50=%.2fus p95=%.2fus p99=%.2fus ops/sec=%.1f exact_fallbacks=%d\n",
		res.Search.AvgMicros,
		float64(res.Search.P50Nanos)/1000,
		float64(res.Search.P95Nanos)/1000,
		float64(res.Search.P99Nanos)/1000,
		res.Search.OpsPerSecond,
		res.Search.ExactFallbacks)
	fmt.Fprintf(w, "avg_candidates=%.1f avg_rerank=%.1f avg_quantized_score_calls=%.1f avg_quantized_code_bytes=%.1f avg_quantized_rerank_candidates=%.1f avg_quantized_rerank_exact_score_calls=%.1f avg_vector_bytes=%.1f avg_norm_bytes=%.1f\n",
		res.Search.AvgCandidates,
		res.Search.AvgRerank,
		res.Search.AvgQuantizedScoreCalls,
		res.Search.AvgQuantizedCodeBytes,
		res.Search.AvgQuantizedRerankCandidates,
		res.Search.AvgQuantizedRerankExactScoreCalls,
		res.Search.AvgVectorBytes,
		res.Search.AvgNormBytes)
	if searchBenchmarkHasGuardrailMetrics(res.Search) {
		fmt.Fprintf(w, "avg_documents_fetched=%.1f avg_response_owned_result_allocs=%.1f avg_route_hnsw_search_pack=%.1f avg_route_quantized_only=%.1f avg_route_quantized_rerank=%.1f avg_route_column_graph_prepared=%.1f avg_route_column_graph_fallback=%.1f avg_graph_row_fallbacks=%.1f avg_typed_column_fallbacks=%.1f avg_vector_scratch_decodes=%.1f\n",
			float64Value(res.Search.AvgDocumentsFetched),
			float64Value(res.Search.AvgResponseOwnedResultAllocs),
			float64Value(res.Search.AvgSearchRouteHNSWSearchPack),
			float64Value(res.Search.AvgSearchRouteQuantizedOnly),
			float64Value(res.Search.AvgSearchRouteQuantizedRerank),
			float64Value(res.Search.AvgSearchRouteColumnGraphPrepared),
			float64Value(res.Search.AvgSearchRouteColumnGraphFallback),
			float64Value(res.Search.AvgGraphRowFallbacks),
			float64Value(res.Search.AvgTypedColumnFallbacks),
			float64Value(res.Search.AvgVectorScratchDecodes))
	}
	fmt.Fprintf(w, "\nParallel Search Benchmark\n")
	printSearchBenchmarks(w, res.SearchBenchmarks)
	fmt.Fprintf(w, "\nStorage\n")
	fmt.Fprintf(w, "before_compact_total=%d bytes (%.1f/doc)\n", res.StorageBeforeCompact.TotalBytes, res.StorageBeforeCompact.BytesPerDoc)
	fmt.Fprintf(w, "after_compact_total=%d bytes (%.1f/doc)\n", res.StorageAfterCompact.TotalBytes, res.StorageAfterCompact.BytesPerDoc)
	if res.StorageAfterClose.TotalBytes > 0 {
		fmt.Fprintf(w, "after_close_total=%d bytes (%.1f/doc)\n", res.StorageAfterClose.TotalBytes, res.StorageAfterClose.BytesPerDoc)
	}
	if res.StorageAfterVacuum.TotalBytes > 0 {
		fmt.Fprintf(w, "after_index_vacuum_total=%d bytes (%.1f/doc)\n", res.StorageAfterVacuum.TotalBytes, res.StorageAfterVacuum.BytesPerDoc)
	}
	if res.FormatConfig != nil {
		fmt.Fprintf(w, "format index_outer_leaves_in_vlog=%t leaf_prefix_compression=%t vlog_compression=%s\n",
			res.FormatConfig.IndexOuterLeavesInValueLog,
			res.FormatConfig.LeafPrefixCompression,
			res.FormatConfig.ValueLogCompression)
	}
	fmt.Fprintf(w, "storage_domains index_db=%d value_vlog=%d leaf_vlog=%d\n",
		res.StorageExpectation.IndexBytes,
		res.StorageExpectation.ValueLogBytes,
		res.StorageExpectation.LeafVLogBytes)
	printDomains(w, "after_compact", res.StorageAfterCompact.Domains)
	fmt.Fprintf(w, "\nMemory\n")
	fmt.Fprintf(w, "index_bytes_memory=%d load_alloc_delta=%d alloc_after_load=%d\n",
		res.Memory.IndexBytesMemory, res.Memory.LoadAllocDeltaBytes, res.Memory.AllocAfterLoadBytes)
}

func printMatrixText(w io.Writer, res matrixResult) {
	fmt.Fprintf(w, "TreeDB vector search matrix\n")
	fmt.Fprintf(w, "dir=%s kept=%t profile=%s docs=%d dims=%d queries=%d top_k=%d search_concurrency=%s\n",
		res.Dir, res.KeptDir, res.Profile, res.Docs, res.Dimensions, res.Queries, res.TopK, joinInts(res.SearchConcurrency))
	if res.SearchProfileDir != "" {
		fmt.Fprintf(w, "search_profile_dir=%s\n", res.SearchProfileDir)
	}
	for _, testCase := range res.Cases {
		fmt.Fprintf(w, "\nCase %s\n", testCase.Name)
		fmt.Fprintf(w, "%s\n", testCase.Description)
		fmt.Fprintf(w, "backend=%s vector_index_strategy=%s vector_index_search_path=%s query_mode=%s quantized_codec=%s\n",
			resultBackend(testCase.Result),
			resultStrategy(testCase.Result),
			resultSearchPath(testCase.Result),
			resultQueryMode(testCase.Result),
			testCase.Result.QuantizedCodec)
		if testCase.Result.SearchProfileDir != "" {
			fmt.Fprintf(w, "search_profile_dir=%s\n", testCase.Result.SearchProfileDir)
		}
		fmt.Fprintf(w, "storage_after_compact_total=%d bytes (%.1f/doc)\n",
			testCase.Result.StorageAfterCompact.TotalBytes,
			testCase.Result.StorageAfterCompact.BytesPerDoc)
		if testCase.Result.StorageAfterVacuum.TotalBytes > 0 {
			fmt.Fprintf(w, "storage_after_index_vacuum_total=%d bytes (%.1f/doc)\n",
				testCase.Result.StorageAfterVacuum.TotalBytes,
				testCase.Result.StorageAfterVacuum.BytesPerDoc)
		}
		fmt.Fprintf(w, "storage_domains index_db=%d value_vlog=%d leaf_vlog=%d\n",
			testCase.Result.StorageExpectation.IndexBytes,
			testCase.Result.StorageExpectation.ValueLogBytes,
			testCase.Result.StorageExpectation.LeafVLogBytes)
		if testCase.Result.FormatConfig != nil {
			fmt.Fprintf(w, "format index_outer_leaves_in_vlog=%t leaf_prefix_compression=%t vlog_compression=%s\n",
				testCase.Result.FormatConfig.IndexOuterLeavesInValueLog,
				testCase.Result.FormatConfig.LeafPrefixCompression,
				testCase.Result.FormatConfig.ValueLogCompression)
		}
		fmt.Fprintf(w, "vector_search avg=%.2fus p95=%.2fus recall_at_%d=%.4f\n",
			testCase.Result.Search.AvgMicros,
			float64(testCase.Result.Search.P95Nanos)/1000,
			testCase.Result.TopK,
			testCase.Result.Validation.Recall)
		printSearchBenchmarks(w, testCase.Result.SearchBenchmarks)
	}
}

func resultBackend(res result) string {
	if res.Backend != "" {
		return res.Backend
	}
	return resultBackendForQuery(resultStrategy(res), resultQueryMode(res), res.QuantizedCodec)
}

func resultStrategy(res result) collections.VectorIndexStrategy {
	if res.VectorIndexStrategy != "" {
		return res.VectorIndexStrategy
	}
	return collections.VectorIndexStrategyNativeRuntime
}

func resultSearchPath(res result) string {
	if res.VectorIndexSearchPath != "" {
		return string(res.VectorIndexSearchPath)
	}
	return string(nativeRuntimeSnapshotPath)
}

func resultQueryMode(res result) collections.VectorIndexQueryMode {
	if res.QueryMode != "" {
		return res.QueryMode
	}
	return collections.VectorIndexQueryModeExact
}

func printSearchBenchmarks(w io.Writer, benchmarks []searchBenchmarkResult) {
	for _, bench := range benchmarks {
		fmt.Fprintf(w, "search concurrency=%d queries=%d query_mode=%s quantized_codec=%s avg=%.2fus p50=%.2fus p95=%.2fus p99=%.2fus ops/sec=%.1f exact_fallbacks=%d avg_candidates=%.1f avg_quantized_score_calls=%.1f avg_quantized_code_bytes=%.1f avg_quantized_rerank_candidates=%.1f avg_quantized_rerank_exact_score_calls=%.1f avg_vector_bytes=%.1f avg_norm_bytes=%.1f",
			bench.Concurrency,
			bench.Queries,
			bench.QueryMode,
			bench.QuantizedCodec,
			bench.AvgMicros,
			float64(bench.P50Nanos)/1000,
			float64(bench.P95Nanos)/1000,
			float64(bench.P99Nanos)/1000,
			bench.OpsPerSecond,
			bench.ExactFallbacks,
			bench.AvgCandidates,
			bench.AvgQuantizedScoreCalls,
			bench.AvgQuantizedCodeBytes,
			bench.AvgQuantizedRerankCandidates,
			bench.AvgQuantizedRerankExactScoreCalls,
			bench.AvgVectorBytes,
			bench.AvgNormBytes)
		if searchBenchmarkHasGuardrailMetrics(bench) {
			fmt.Fprintf(w, " avg_documents_fetched=%.1f avg_response_owned_result_allocs=%.1f avg_route_hnsw_search_pack=%.1f avg_route_quantized_only=%.1f avg_route_quantized_rerank=%.1f avg_route_column_graph_prepared=%.1f avg_route_column_graph_fallback=%.1f avg_graph_row_fallbacks=%.1f avg_typed_column_fallbacks=%.1f avg_vector_scratch_decodes=%.1f",
				float64Value(bench.AvgDocumentsFetched),
				float64Value(bench.AvgResponseOwnedResultAllocs),
				float64Value(bench.AvgSearchRouteHNSWSearchPack),
				float64Value(bench.AvgSearchRouteQuantizedOnly),
				float64Value(bench.AvgSearchRouteQuantizedRerank),
				float64Value(bench.AvgSearchRouteColumnGraphPrepared),
				float64Value(bench.AvgSearchRouteColumnGraphFallback),
				float64Value(bench.AvgGraphRowFallbacks),
				float64Value(bench.AvgTypedColumnFallbacks),
				float64Value(bench.AvgVectorScratchDecodes))
		}
		fmt.Fprintln(w)
	}
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func printDomains(w io.Writer, label string, domains map[string]int64) {
	keys := make([]string, 0, len(domains))
	for key := range domains {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(w, "%s_domain name=%s bytes=%d\n", label, key, domains[key])
	}
}
