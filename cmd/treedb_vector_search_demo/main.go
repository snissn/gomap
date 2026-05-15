package main

import (
	"bytes"
	"context"
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
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultDocs                  = 10000
	defaultDimensions            = 64
	defaultQueries               = 1000
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
	keepDir               bool
	profile               treedb.Profile
	docs                  int
	dimensions            int
	queries               int
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
}

type result struct {
	Dir                   string                         `json:"dir"`
	KeptDir               bool                           `json:"kept_dir"`
	Profile               string                         `json:"profile"`
	Docs                  int                            `json:"docs"`
	Dimensions            int                            `json:"dimensions"`
	Queries               int                            `json:"queries"`
	ValidateQueries       int                            `json:"validate_queries"`
	ValidateDocs          int                            `json:"validate_docs"`
	TopK                  int                            `json:"top_k"`
	M                     int                            `json:"m"`
	EfConstruction        int                            `json:"ef_construction"`
	EfSearch              int                            `json:"ef_search"`
	ValuePointerThreshold int                            `json:"value_pointer_threshold"`
	LeafGenerationTarget  int64                          `json:"leaf_generation_segment_target"`
	Compact               bool                           `json:"compact"`
	Insert                phaseResult                    `json:"insert"`
	Rebuild               phaseResult                    `json:"rebuild"`
	CompactPhase          phaseResult                    `json:"compact_phase"`
	ReopenLoad            phaseResult                    `json:"reopen_load"`
	Validation            validationResult               `json:"validation"`
	Search                searchBenchmarkResult          `json:"search"`
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
}

type searchBenchmarkResult struct {
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
	if !cfg.keepDir {
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
		compact:               true,
		disableExactFallback:  true,
		profile:               treedb.ProfileBench,
	}
	profileRaw := string(cfg.profile)
	fs := flag.NewFlagSet("treedb_vector_search_demo", flag.ContinueOnError)
	fs.StringVar(&cfg.dir, "dir", "", "TreeDB directory to create; empty uses a temporary directory")
	fs.BoolVar(&cfg.keepDir, "keep-dir", false, "Keep the DB directory after the run")
	fs.StringVar(&profileRaw, "profile", profileRaw, "TreeDB profile: durable, fast, wal_on_fast, or bench")
	fs.IntVar(&cfg.docs, "docs", cfg.docs, "Number of synthetic documents to load")
	fs.IntVar(&cfg.dimensions, "dims", cfg.dimensions, "Vector dimensions per document")
	fs.IntVar(&cfg.queries, "queries", cfg.queries, "Number of ANN search queries to benchmark")
	fs.IntVar(&cfg.validateQueries, "validate-queries", cfg.validateQueries, "Number of queries to validate against exact search")
	fs.IntVar(&cfg.validateDocs, "validate-docs", cfg.validateDocs, "Number of documents to read and byte-validate after compaction/reopen")
	fs.IntVar(&cfg.topK, "top-k", cfg.topK, "Nearest-neighbor result count")
	fs.IntVar(&cfg.batchSize, "batch-size", cfg.batchSize, "Insert batch size")
	fs.IntVar(&cfg.m, "m", cfg.m, "HNSW max neighbor parameter")
	fs.IntVar(&cfg.efConstruction, "ef-construction", cfg.efConstruction, "HNSW efConstruction")
	fs.IntVar(&cfg.efSearch, "ef-search", cfg.efSearch, "HNSW efSearch for ANN queries")
	fs.IntVar(&cfg.valuePointerThreshold, "value-pointer-threshold", cfg.valuePointerThreshold, "Value-log pointer threshold for the demo DB in bytes; 0 uses the selected TreeDB profile default")
	fs.Int64Var(&cfg.leafGenerationTarget, "leaf-generation-segment-target", cfg.leafGenerationTarget, "Leaf value-log generation segment target for the demo DB in bytes; 0 uses the selected TreeDB profile default")
	fs.Float64Var(&cfg.minRecall, "min-recall", cfg.minRecall, "Minimum validation recall@topK")
	fs.BoolVar(&cfg.compact, "compact", cfg.compact, "Run CompactStorageFull after insert/index build and before reads")
	fs.BoolVar(&cfg.compactSyncEachPhase, "compact-sync-each-phase", false, "Ask CompactStorage to fsync each rewrite/pack phase")
	fs.BoolVar(&cfg.disableExactFallback, "disable-exact-fallback", cfg.disableExactFallback, "Disable exact fallback during ANN benchmark queries")
	fs.BoolVar(&cfg.requireValueLogBytes, "require-value-log-bytes", false, "Fail if compacted storage has no value-log bytes")
	fs.BoolVar(&cfg.requireLeafVLogBytes, "require-leaf-vlog-bytes", false, "Fail if compacted storage has no leaf value-log bytes")
	fs.BoolVar(&cfg.jsonOut, "json", false, "Emit JSON instead of text")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
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
	if cfg.validateQueries > cfg.docs {
		cfg.validateQueries = cfg.docs
	}
	if cfg.validateDocs > cfg.docs {
		cfg.validateDocs = cfg.docs
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

func defaultProfile(profile treedb.Profile) treedb.Profile {
	if profile == "" {
		return treedb.ProfileBench
	}
	return profile
}

func openDemoBackend(profile treedb.Profile, dir string, valuePointerThreshold int, leafGenerationTarget int64) (*backenddb.DB, func() error, error) {
	opts := treedb.OptionsFor(defaultProfile(profile), dir)
	if valuePointerThreshold > 0 {
		opts.ValueLog.PointerThreshold = valuePointerThreshold
	}
	if leafGenerationTarget > 0 {
		opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
		opts.ValueLog.Generational.LeafSegmentTargetBytes = leafGenerationTarget
	}
	if opts.IndexOuterLeavesInValueLog {
		return treedb.OpenBackendWithCachedLeafLog(opts)
	}
	return treedb.OpenBackend(opts)
}

func execute(ctx context.Context, cfg config) (result, error) {
	cfg.profile = defaultProfile(cfg.profile)
	dir := cfg.dir
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
		if err := os.RemoveAll(dir); err != nil {
			return result{}, err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
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
		KeptDir:               cfg.keepDir,
		Profile:               string(cfg.profile),
		Docs:                  cfg.docs,
		Dimensions:            cfg.dimensions,
		Queries:               cfg.queries,
		ValidateQueries:       cfg.validateQueries,
		ValidateDocs:          cfg.validateDocs,
		TopK:                  cfg.topK,
		M:                     cfg.m,
		EfConstruction:        cfg.efConstruction,
		EfSearch:              cfg.efSearch,
		ValuePointerThreshold: cfg.valuePointerThreshold,
		LeafGenerationTarget:  cfg.leafGenerationTarget,
		Compact:               cfg.compact,
	}

	d, cleanupBackend, err := openDemoBackend(cfg.profile, dir, cfg.valuePointerThreshold, cfg.leafGenerationTarget)
	if err != nil {
		return result{}, err
	}
	mgr := collections.NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&collections.CollectionMeta{
		Name:          "docs",
		VectorIndexes: []collections.VectorIndexDefinition{def},
	}); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = cleanupBackend()
		return result{}, err
	}

	insertStart := time.Now()
	if err := insertDocuments(col, cfg.docs, cfg.dimensions, cfg.batchSize); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	if err := col.Flush(); err != nil {
		_ = cleanupBackend()
		return result{}, err
	}
	res.Insert = phaseSince(insertStart)

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

	var beforeLoad runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&beforeLoad)
	reopenStart := time.Now()
	d, cleanupBackend, err = openDemoBackend(cfg.profile, dir, cfg.valuePointerThreshold, cfg.leafGenerationTarget)
	if err != nil {
		return result{}, err
	}
	defer cleanupBackend()
	col, err = collections.NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		return result{}, err
	}
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptions(def))
	if err != nil {
		return result{}, err
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.RootID == 0 {
		return result{}, fmt.Errorf("failed to load compacted native vector root: %+v", loadStatus)
	}
	res.ReopenLoad = phaseSince(reopenStart)
	res.IndexStatsLoaded = loaded.Stats()
	var afterLoad runtime.MemStats
	runtime.ReadMemStats(&afterLoad)
	res.Memory = memoryReport{
		AllocBeforeLoadBytes: beforeLoad.Alloc,
		AllocAfterLoadBytes:  afterLoad.Alloc,
		LoadAllocDeltaBytes:  int64(afterLoad.Alloc) - int64(beforeLoad.Alloc),
		IndexBytesMemory:     res.IndexStatsLoaded.BytesMemory,
	}

	validation, err := validateCompactedData(col, loaded, cfg)
	if err != nil {
		return result{}, err
	}
	res.Validation = validation

	search, err := benchmarkSearch(loaded, cfg)
	if err != nil {
		return result{}, err
	}
	res.Search = search
	return res, nil
}

func insertDocuments(col *collections.Collection, docs, dims, batchSize int) error {
	for start := 0; start < docs; start += batchSize {
		end := start + batchSize
		if end > docs {
			end = docs
		}
		ids := make([][]byte, end-start)
		documents := make([][]byte, end-start)
		for i := start; i < end; i++ {
			j := i - start
			ids[j] = documentID(i)
			documents[j] = documentJSON(i, dims)
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			return err
		}
	}
	return nil
}

func validateCompactedData(col *collections.Collection, idx *collections.VectorIndex, cfg config) (validationResult, error) {
	out := validationResult{
		DocumentsChecked: cfg.validateDocs,
		QueriesChecked:   cfg.validateQueries,
		MinRecall:        cfg.minRecall,
	}
	for i := 0; i < cfg.validateDocs; i++ {
		docIndex := validationDocIndex(i, cfg.docs)
		got, err := col.Get(documentID(docIndex))
		if err != nil {
			return out, fmt.Errorf("get compacted doc %d: %w", docIndex, err)
		}
		want := documentJSON(docIndex, cfg.dimensions)
		if !bytes.Equal(got, want) {
			return out, fmt.Errorf("compacted doc %d mismatch", docIndex)
		}
	}
	if cfg.validateQueries == 0 {
		out.Recall = 1
		return out, nil
	}
	recall, err := idx.CheckRecall(validationQueries(cfg.validateQueries, cfg.docs, cfg.dimensions), collections.VectorIndexSearchOptions{
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

func benchmarkSearch(idx *collections.VectorIndex, cfg config) (searchBenchmarkResult, error) {
	queries := validationQueries(cfg.queries, cfg.docs, cfg.dimensions)
	latencies := make([]int64, len(queries))
	var candidatesTotal int64
	var rerankTotal int64
	var exactFallbacks int
	startAll := time.Now()
	for i, query := range queries {
		start := time.Now()
		results, trace, err := idx.Search(query, collections.VectorIndexSearchOptions{
			TopK:                 cfg.topK,
			EfSearch:             cfg.efSearch,
			DisableExactFallback: cfg.disableExactFallback,
		})
		latencies[i] = time.Since(start).Nanoseconds()
		if err != nil {
			return searchBenchmarkResult{}, err
		}
		if len(results) == 0 {
			return searchBenchmarkResult{}, errors.New("vector search returned no results")
		}
		candidatesTotal += int64(trace.CandidatesExamined)
		rerankTotal += int64(trace.RerankCount)
		if trace.ExactFallbackReason != "" {
			exactFallbacks++
		}
	}
	total := time.Since(startAll)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	avg := float64(total.Nanoseconds()) / float64(len(queries))
	return searchBenchmarkResult{
		Queries:              len(queries),
		TotalDurationNanos:   total.Nanoseconds(),
		AvgNanos:             avg,
		AvgMicros:            avg / 1000,
		OpsPerSecond:         float64(len(queries)) / total.Seconds(),
		P50Nanos:             percentile(latencies, 0.50),
		P95Nanos:             percentile(latencies, 0.95),
		P99Nanos:             percentile(latencies, 0.99),
		AvgCandidates:        float64(candidatesTotal) / float64(len(queries)),
		AvgRerank:            float64(rerankTotal) / float64(len(queries)),
		ExactFallbacks:       exactFallbacks,
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

func validationQueries(count, docs, dims int) [][]float32 {
	queries := make([][]float32, count)
	for i := 0; i < count; i++ {
		queries[i] = embedding(queryDocIndex(i, docs), dims)
	}
	return queries
}

func queryDocIndex(i, docs int) int {
	return (i*7919 + docs/3 + 17) % docs
}

func validationDocIndex(i, docs int) int {
	return (i*1543 + docs/5 + 11) % docs
}

func documentID(id int) []byte {
	return []byte(fmt.Sprintf("doc-%06d", id))
}

func documentJSON(id, dims int) []byte {
	vector := embedding(id, dims)
	out := make([]byte, 0, 48+dims*10)
	out = append(out, fmt.Sprintf(`{"group":%d,"embedding":[`, id%16)...)
	for i, value := range vector {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, fmt.Sprintf("%.7g", value)...)
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
