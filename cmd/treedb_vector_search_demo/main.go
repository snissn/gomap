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
	"runtime"
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
	defaultDocs                  = 10000
	defaultDimensions            = 64
	defaultQueries               = 10000
	defaultSearchConcurrency     = "2,4,8,16,32,64,128"
	defaultTopK                  = 10
	defaultBatchSize             = 512
	defaultM                     = 16
	defaultEfConstruct           = 128
	defaultEfSearch              = 128
	defaultValuePointerThreshold = 1024
	defaultLeafGenerationTarget  = 4 << 20
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
	disableExactFallback  bool
	requireValueLogBytes  bool
	requireLeafVLogBytes  bool
	jsonOut               bool

	indexOuterLeavesInValueLog *bool
}

type result struct {
	Dir                   string                         `json:"dir"`
	DatasetDir            string                         `json:"dataset_dir,omitempty"`
	KeptDir               bool                           `json:"kept_dir"`
	Profile               string                         `json:"profile"`
	Docs                  int                            `json:"docs"`
	Dimensions            int                            `json:"dimensions"`
	Queries               int                            `json:"queries"`
	SearchConcurrency     []int                          `json:"search_concurrency"`
	ValidateQueries       int                            `json:"validate_queries"`
	ValidateDocs          int                            `json:"validate_docs"`
	TopK                  int                            `json:"top_k"`
	M                     int                            `json:"m"`
	EfConstruction        int                            `json:"ef_construction"`
	EfSearch              int                            `json:"ef_search"`
	ValuePointerThreshold int                            `json:"value_pointer_threshold"`
	LeafGenerationTarget  int64                          `json:"leaf_generation_segment_target"`
	MinRecall             float64                        `json:"min_recall"`
	Compact               bool                           `json:"compact"`
	CompactSyncEachPhase  bool                           `json:"compact_sync_each_phase"`
	DisableExactFallback  bool                           `json:"disable_exact_fallback"`
	Insert                phaseResult                    `json:"insert"`
	Rebuild               phaseResult                    `json:"rebuild"`
	CompactPhase          phaseResult                    `json:"compact_phase"`
	ReopenLoad            phaseResult                    `json:"reopen_load"`
	Validation            validationResult               `json:"validation"`
	Search                searchBenchmarkResult          `json:"search"`
	SearchBenchmarks      []searchBenchmarkResult        `json:"search_benchmarks"`
	StorageBeforeCompact  storageReport                  `json:"storage_before_compact"`
	StorageAfterCompact   storageReport                  `json:"storage_after_compact"`
	IndexStatsBefore      collections.VectorIndexStats   `json:"index_stats_before_compact"`
	IndexStatsLoaded      collections.VectorIndexStats   `json:"index_stats_loaded"`
	NativeRootBytes       int64                          `json:"native_root_bytes"`
	CompactStorage        *backenddb.CompactStorageStats `json:"compact_storage,omitempty"`
	FormatConfig          *backenddb.FormatConfig        `json:"format_config,omitempty"`
	StorageExpectation    storageExpectationReport       `json:"storage_expectation"`
	Memory                memoryReport                   `json:"memory"`
}

type matrixResult struct {
	Dir               string             `json:"dir"`
	KeptDir           bool               `json:"kept_dir"`
	Profile           string             `json:"profile"`
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
	DocumentsChecked int     `json:"documents_checked"`
	QueriesChecked   int     `json:"queries_checked"`
	ExactTotal       int     `json:"exact_total"`
	ANNTotal         int     `json:"ann_total"`
	Overlap          int     `json:"overlap"`
	Recall           float64 `json:"recall"`
	MinRecall        float64 `json:"min_recall"`
	DurationNanos    int64   `json:"duration_nanos"`
	Seconds          float64 `json:"seconds"`
}

type searchBenchmarkResult struct {
	Concurrency          int     `json:"concurrency"`
	Queries              int     `json:"queries"`
	TotalDurationNanos   int64   `json:"total_duration_nanos"`
	AvgNanos             float64 `json:"avg_nanos"`
	AvgMicros            float64 `json:"avg_micros"`
	OpsPerSecond         float64 `json:"ops_per_second"`
	P50Nanos             int64   `json:"p50_nanos"`
	P95Nanos             int64   `json:"p95_nanos"`
	P99Nanos             int64   `json:"p99_nanos"`
	AvgCandidates        float64 `json:"avg_candidates"`
	AvgRerank            float64 `json:"avg_rerank"`
	ExactFallbacks       int     `json:"exact_fallbacks"`
	DisableExactFallback bool    `json:"disable_exact_fallback"`
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
		disableExactFallback:  true,
		profile:               treedb.ProfileBench,
	}
	profileRaw := string(cfg.profile)
	searchConcurrencyRaw := defaultSearchConcurrency
	fs := flag.NewFlagSet("treedb_vector_search_demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.dir, "dir", "", "TreeDB directory to create; empty uses a temporary directory; explicit directories are kept")
	fs.StringVar(&cfg.datasetDir, "dataset-dir", "", "Optional exported vector dataset directory to load documents and queries from")
	fs.BoolVar(&cfg.keepDir, "keep-dir", false, "Keep the DB directory after the run")
	fs.BoolVar(&cfg.matrix, "matrix", cfg.matrix, "Run the storage/search benchmark matrix instead of a single storage case")
	fs.StringVar(&profileRaw, "profile", profileRaw, "TreeDB profile: durable, fast, wal_on_fast, or bench")
	fs.IntVar(&cfg.docs, "docs", cfg.docs, "Number of synthetic documents to load")
	fs.IntVar(&cfg.dimensions, "dims", cfg.dimensions, "Vector dimensions per document")
	fs.IntVar(&cfg.queries, "queries", cfg.queries, "Number of ANN search queries to benchmark")
	fs.StringVar(&searchConcurrencyRaw, "search-concurrency", searchConcurrencyRaw, "Comma-separated parallel ANN search concurrency levels; serial concurrency=1 is always included")
	fs.IntVar(&cfg.validateQueries, "validate-queries", cfg.validateQueries, "Number of queries to validate against exact search")
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
	fs.BoolVar(&cfg.disableExactFallback, "disable-exact-fallback", cfg.disableExactFallback, "Disable exact fallback during ANN benchmark queries")
	fs.BoolVar(&cfg.requireValueLogBytes, "require-value-log-bytes", false, "Fail if compacted storage has no value-log bytes")
	fs.BoolVar(&cfg.requireLeafVLogBytes, "require-leaf-vlog-bytes", false, "Fail if compacted storage has no leaf value-log bytes")
	fs.BoolVar(&cfg.jsonOut, "json", false, "Emit JSON instead of text")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	searchConcurrency, err := parseSearchConcurrency(searchConcurrencyRaw)
	if err != nil {
		return config{}, err
	}
	cfg.searchConcurrency = searchConcurrency
	profile, err := parseProfile(profileRaw)
	if err != nil {
		return config{}, err
	}
	cfg.profile = profile
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

func parseProfile(raw string) (treedb.Profile, error) {
	switch treedb.Profile(strings.ToLower(strings.TrimSpace(raw))) {
	case "", treedb.ProfileBench:
		return treedb.ProfileBench, nil
	case treedb.ProfileDurable:
		return treedb.ProfileDurable, nil
	case treedb.ProfileFast:
		return treedb.ProfileFast, nil
	case treedb.ProfileWALOnFast:
		return treedb.ProfileWALOnFast, nil
	default:
		return "", fmt.Errorf("unsupported -profile %q", raw)
	}
}

func parseSearchConcurrency(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts)+1)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid -search-concurrency value %q", part)
		}
		if value <= 1 {
			return nil, fmt.Errorf("-search-concurrency values must be greater than 1: %d", value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Ints(out)
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

func defaultProfile(profile treedb.Profile) treedb.Profile {
	if profile == "" {
		return treedb.ProfileBench
	}
	return profile
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
	opts := treedb.OptionsFor(defaultProfile(cfg.profile), dir)
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
	if opts.IndexOuterLeavesInValueLog {
		return treedb.OpenBackendWithCachedLeafLog(opts)
	}
	return treedb.OpenBackend(opts)
}

func execute(ctx context.Context, cfg config) (result, error) {
	cfg.profile = defaultProfile(cfg.profile)
	if err := applySearchBenchmarkDefaults(&cfg); err != nil {
		return result{}, err
	}
	work, err := loadWorkload(&cfg)
	if err != nil {
		return result{}, err
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
	}
	res := result{
		Dir:                   dir,
		DatasetDir:            cfg.datasetDir,
		KeptDir:               cfg.keepDir || explicitDir,
		Profile:               string(cfg.profile),
		Docs:                  cfg.docs,
		Dimensions:            cfg.dimensions,
		Queries:               cfg.queries,
		SearchConcurrency:     append([]int(nil), cfg.searchConcurrency...),
		ValidateQueries:       cfg.validateQueries,
		ValidateDocs:          cfg.validateDocs,
		TopK:                  cfg.topK,
		M:                     cfg.m,
		EfConstruction:        cfg.efConstruction,
		EfSearch:              cfg.efSearch,
		ValuePointerThreshold: cfg.valuePointerThreshold,
		LeafGenerationTarget:  cfg.leafGenerationTarget,
		MinRecall:             cfg.minRecall,
		Compact:               cfg.compact,
		CompactSyncEachPhase:  cfg.compactSyncEachPhase,
		DisableExactFallback:  cfg.disableExactFallback,
	}

	d, cleanupBackend, err := openDemoBackend(cfg, dir)
	if err != nil {
		return result{}, err
	}
	mgr := collections.NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{Name: "docs"}); err != nil {
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

	if _, err := col.CreateVectorIndex(def); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	rebuildStart := time.Now()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	if !status.NativeRootLoaded || status.RootID == 0 {
		_ = cleanupBackend()
		return result{}, fmt.Errorf("unexpected native vector root status: %+v", status)
	}
	res.Rebuild = phaseSince(rebuildStart)
	res.NativeRootBytes = status.NativeRootBytes
	res.IndexStatsBefore = status.Stats

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
	if err := cleanupBackend(); err != nil {
		return result{}, err
	}

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
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptions(def))
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
	res.ReopenLoad = phaseSince(reopenStart)
	res.IndexStatsLoaded = loaded.Stats()
	var afterLoad runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&afterLoad)
	res.Memory = memoryReport{
		AllocBeforeLoadBytes: beforeLoad.Alloc,
		AllocAfterLoadBytes:  afterLoad.Alloc,
		LoadAllocDeltaBytes:  allocDeltaBytes(afterLoad.Alloc, beforeLoad.Alloc),
		IndexBytesMemory:     res.IndexStatsLoaded.BytesMemory,
	}

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
	if len(searchBenchmarks) > 0 {
		res.Search = searchBenchmarks[0]
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
	cfg.profile = defaultProfile(cfg.profile)
	if err := applySearchBenchmarkDefaults(&cfg); err != nil {
		return matrixResult{}, err
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
			return fmt.Errorf("dataset documents has more than %d rows", cfg.docs)
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
		MinRecall:        cfg.minRecall,
	}
	for i := 0; i < cfg.validateDocs; i++ {
		docIndex := validationDocIndex(i, cfg.docs)
		id, want, err := expectedDocument(docIndex, cfg, work)
		if err != nil {
			return out, err
		}
		got, err := col.Get(id)
		if err != nil {
			return out, fmt.Errorf("get compacted doc %q: %w", id, err)
		}
		if !bytes.Equal(got, want) {
			return out, fmt.Errorf("compacted doc %d mismatch", docIndex)
		}
	}
	if cfg.validateQueries == 0 {
		return out, nil
	}
	queries, err := loadQueries(cfg.validateQueries, cfg, work, 0)
	if err != nil {
		return out, err
	}
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
	if out.Recall < cfg.minRecall {
		return out, fmt.Errorf("recall %.4f below minimum %.4f", out.Recall, cfg.minRecall)
	}
	return out, nil
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

func datasetDocument(docIndex int, work workload) ([]byte, datasetDocumentHeader, error) {
	f, err := os.Open(datasetPath(work, work.manifest.DocumentsJSONLFile, "documents.jsonl"))
	if err != nil {
		return nil, datasetDocumentHeader{}, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for i := 0; scanner.Scan(); i++ {
		if i != docIndex {
			continue
		}
		line := append([]byte(nil), scanner.Bytes()...)
		var header datasetDocumentHeader
		if err := json.Unmarshal(line, &header); err != nil {
			return nil, datasetDocumentHeader{}, fmt.Errorf("decode dataset document %d: %w", docIndex, err)
		}
		if header.Index != docIndex {
			return nil, datasetDocumentHeader{}, fmt.Errorf("dataset document index=%d at row %d", header.Index, docIndex)
		}
		if header.ID == "" {
			return nil, datasetDocumentHeader{}, fmt.Errorf("dataset document %d has empty id", docIndex)
		}
		return line, header, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, datasetDocumentHeader{}, err
	}
	return nil, datasetDocumentHeader{}, fmt.Errorf("dataset document %d not found", docIndex)
}

func benchmarkSearch(idx *collections.VectorIndex, cfg config, work workload) (searchBenchmarkResult, error) {
	return benchmarkSearchConcurrent(idx, cfg, work, 1)
}

func benchmarkSearchMatrix(idx *collections.VectorIndex, cfg config, work workload) ([]searchBenchmarkResult, error) {
	levels := make([]int, 0, len(cfg.searchConcurrency)+1)
	levels = append(levels, 1)
	levels = append(levels, cfg.searchConcurrency...)
	out := make([]searchBenchmarkResult, 0, len(levels))
	for _, concurrency := range levels {
		bench, err := benchmarkSearchConcurrent(idx, cfg, work, concurrency)
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
	rowBytes := dims * 4
	buf := make([]byte, rowBytes)
	out := make([][]float32, count)
	for i := 0; i < count; i++ {
		if _, err := io.ReadFull(f, buf); err != nil {
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
	fmt.Fprintf(w, "dir=%s kept=%t profile=%s docs=%d dims=%d queries=%d top_k=%d m=%d ef_construction=%d ef_search=%d value_pointer_threshold=%d leaf_generation_segment_target=%d\n",
		res.Dir, res.KeptDir, res.Profile, res.Docs, res.Dimensions, res.Queries, res.TopK, res.M, res.EfConstruction, res.EfSearch, res.ValuePointerThreshold, res.LeafGenerationTarget)
	fmt.Fprintf(w, "\nPhases\n")
	fmt.Fprintf(w, "insert: %.3fs\n", res.Insert.Seconds)
	fmt.Fprintf(w, "rebuild_native_vector_index: %.3fs native_root_bytes=%d\n", res.Rebuild.Seconds, res.NativeRootBytes)
	if res.Compact {
		fully := false
		if res.CompactStorage != nil {
			fully = res.CompactStorage.FullyCompacted
		}
		fmt.Fprintf(w, "compact_storage_full: %.3fs fully_compacted=%t\n", res.CompactPhase.Seconds, fully)
	} else {
		fmt.Fprintf(w, "compact_storage_full: skipped\n")
	}
	fmt.Fprintf(w, "reopen_and_load_native_index: %.3fs\n", res.ReopenLoad.Seconds)
	fmt.Fprintf(w, "\nValidation\n")
	fmt.Fprintf(w, "documents_checked=%d queries_checked=%d recall_at_%d=%.4f overlap=%d/%d\n",
		res.Validation.DocumentsChecked, res.Validation.QueriesChecked, res.TopK, res.Validation.Recall, res.Validation.Overlap, res.Validation.ExactTotal)
	fmt.Fprintf(w, "\nSearch Benchmark\n")
	fmt.Fprintf(w, "avg=%.2fus p50=%.2fus p95=%.2fus p99=%.2fus ops/sec=%.1f exact_fallbacks=%d\n",
		res.Search.AvgMicros,
		float64(res.Search.P50Nanos)/1000,
		float64(res.Search.P95Nanos)/1000,
		float64(res.Search.P99Nanos)/1000,
		res.Search.OpsPerSecond,
		res.Search.ExactFallbacks)
	fmt.Fprintf(w, "avg_candidates=%.1f avg_rerank=%.1f\n", res.Search.AvgCandidates, res.Search.AvgRerank)
	fmt.Fprintf(w, "\nParallel Search Benchmark\n")
	printSearchBenchmarks(w, res.SearchBenchmarks)
	fmt.Fprintf(w, "\nStorage\n")
	fmt.Fprintf(w, "before_compact_total=%d bytes (%.1f/doc)\n", res.StorageBeforeCompact.TotalBytes, res.StorageBeforeCompact.BytesPerDoc)
	fmt.Fprintf(w, "after_compact_total=%d bytes (%.1f/doc)\n", res.StorageAfterCompact.TotalBytes, res.StorageAfterCompact.BytesPerDoc)
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
	for _, testCase := range res.Cases {
		fmt.Fprintf(w, "\nCase %s\n", testCase.Name)
		fmt.Fprintf(w, "%s\n", testCase.Description)
		fmt.Fprintf(w, "storage_after_compact_total=%d bytes (%.1f/doc)\n",
			testCase.Result.StorageAfterCompact.TotalBytes,
			testCase.Result.StorageAfterCompact.BytesPerDoc)
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

func printSearchBenchmarks(w io.Writer, benchmarks []searchBenchmarkResult) {
	for _, bench := range benchmarks {
		fmt.Fprintf(w, "search concurrency=%d queries=%d avg=%.2fus p50=%.2fus p95=%.2fus p99=%.2fus ops/sec=%.1f exact_fallbacks=%d\n",
			bench.Concurrency,
			bench.Queries,
			bench.AvgMicros,
			float64(bench.P50Nanos)/1000,
			float64(bench.P95Nanos)/1000,
			float64(bench.P99Nanos)/1000,
			bench.OpsPerSecond,
			bench.ExactFallbacks)
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
