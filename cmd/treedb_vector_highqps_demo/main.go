package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	collectionName = "docs"
	vectorField    = "embedding"
	vectorIndex    = "embedding_graph"

	defaultDocs                 = 1000
	defaultDims                 = 64
	defaultQueries              = 1000
	defaultWarmupQueries        = 16
	defaultTopK                 = 10
	defaultBatchSize            = 256
	defaultM                    = 16
	defaultEfConstruction       = 128
	defaultEfSearch             = 128
	defaultSeed           int64 = 2409

	apiName           = "Collection.SearchVectorIndexWithBuffer"
	benchmarkGuide    = "TreeDB/docs/guides/vector-search-benchmark-workflow.md"
	highQPSAPIGuide   = "TreeDB/docs/guides/vector-search-high-qps-collection-api.md"
	defaultMetricName = "cosine"
)

type config struct {
	Dir            string
	KeepDir        bool
	Docs           int
	Dims           int
	Queries        int
	WarmupQueries  int
	TopK           int
	BatchSize      int
	M              int
	EfConstruction int
	EfSearch       int
	Seed           int64
	JSON           bool
}

type vectorRow struct {
	ID       string
	Tenant   string
	Title    string
	Category string
	Vector   []float32
}

type demoDocument struct {
	ID        string    `json:"id"`
	Tenant    string    `json:"tenant"`
	Title     string    `json:"title"`
	Category  string    `json:"category"`
	Body      string    `json:"body"`
	Embedding []float32 `json:"embedding"`
}

type result struct {
	API                   string        `json:"api"`
	Dir                   string        `json:"dir"`
	KeptDir               bool          `json:"kept_dir"`
	Docs                  int           `json:"docs"`
	Dims                  int           `json:"dims"`
	Metric                string        `json:"metric"`
	Queries               int           `json:"queries"`
	WarmupQueries         int           `json:"warmup_queries"`
	TopK                  int           `json:"top_k"`
	BatchSize             int           `json:"batch_size"`
	M                     int           `json:"m"`
	EfConstruction        int           `json:"ef_construction"`
	EfSearch              int           `json:"ef_search"`
	CheckpointReopen      bool          `json:"checkpoint_reopen"`
	BufferReused          bool          `json:"buffer_reused"`
	StatsModeTimed        string        `json:"stats_mode_timed"`
	SetupMillis           float64       `json:"setup_ms"`
	ReopenMillis          float64       `json:"reopen_ms"`
	WarmupMillis          float64       `json:"warmup_ms"`
	SearchMillis          float64       `json:"search_ms"`
	AvgMicros             float64       `json:"avg_us"`
	OpsPerSecond          float64       `json:"ops_per_sec"`
	ResultsPerSearch      float64       `json:"results_per_search"`
	FirstResults          []searchHit   `json:"first_results"`
	RouteEvidence         routeEvidence `json:"route_evidence"`
	HighQPSAPIGuide       string        `json:"high_qps_api_guide"`
	BenchmarkWorkflow     string        `json:"benchmark_workflow"`
	InstructionalNonBench bool          `json:"instructional_non_benchmark"`
}

type routeEvidence struct {
	SearchRouteHNSWSearchPack  uint64 `json:"search_route_hnsw_search_pack_per_search"`
	HNSWSearchPackActive       uint64 `json:"hnsw_search_pack_active_per_search"`
	DocumentsFetched           uint64 `json:"docs_fetched_per_search"`
	GraphRowFallbacks          uint64 `json:"graph_row_fallbacks_per_search"`
	TypedColumnVectorFallbacks uint64 `json:"typed_column_vector_fallbacks_per_search"`
	VectorScratchDecodes       uint64 `json:"vector_scratch_decodes_per_search"`
}

type searchHit struct {
	Rank    int     `json:"rank"`
	ID      string  `json:"id"`
	Ordinal int     `json:"ordinal"`
	Score   float64 `json:"score"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "treedb_vector_highqps_demo: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	res, err := execute(context.Background(), cfg)
	if err != nil {
		return err
	}
	if cfg.JSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	printText(stdout, res)
	if !res.KeptDir {
		fmt.Fprintln(stderr, "temporary db removed; rerun with -keep-dir or -dir to inspect files")
	}
	return nil
}

func parseConfig(args []string, stderr io.Writer) (config, error) {
	cfg := config{
		Docs:           defaultDocs,
		Dims:           defaultDims,
		Queries:        defaultQueries,
		WarmupQueries:  defaultWarmupQueries,
		TopK:           defaultTopK,
		BatchSize:      defaultBatchSize,
		M:              defaultM,
		EfConstruction: defaultEfConstruction,
		EfSearch:       defaultEfSearch,
		Seed:           defaultSeed,
	}
	fs := flag.NewFlagSet("treedb_vector_highqps_demo", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Dir, "dir", cfg.Dir, "TreeDB directory; empty uses a temporary directory")
	fs.BoolVar(&cfg.KeepDir, "keep-dir", cfg.KeepDir, "keep an automatically-created temporary DB directory after the run")
	fs.IntVar(&cfg.Docs, "docs", cfg.Docs, "number of deterministic documents/vectors to load")
	fs.IntVar(&cfg.Dims, "dims", cfg.Dims, "embedding dimensions")
	fs.IntVar(&cfg.Queries, "queries", cfg.Queries, "number of timed no-document searches")
	fs.IntVar(&cfg.WarmupQueries, "warmup-queries", cfg.WarmupQueries, "warmup searches before route evidence and timed loop")
	fs.IntVar(&cfg.TopK, "top-k", cfg.TopK, "nearest neighbors per query")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "documents per InsertBatch call")
	fs.IntVar(&cfg.M, "m", cfg.M, "HNSW M parameter")
	fs.IntVar(&cfg.EfConstruction, "ef-construction", cfg.EfConstruction, "HNSW efConstruction parameter")
	fs.IntVar(&cfg.EfSearch, "ef-search", cfg.EfSearch, "HNSW efSearch/query exploration bound")
	fs.Int64Var(&cfg.Seed, "seed", cfg.Seed, "deterministic fixture seed")
	fs.BoolVar(&cfg.JSON, "json", cfg.JSON, "emit JSON instead of text")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	return cfg, validateConfig(cfg)
}

func validateConfig(cfg config) error {
	if cfg.Docs <= 0 {
		return errors.New("-docs must be positive")
	}
	if cfg.Dims <= 0 {
		return errors.New("-dims must be positive")
	}
	if cfg.Queries <= 0 {
		return errors.New("-queries must be positive")
	}
	if cfg.WarmupQueries <= 0 {
		return errors.New("-warmup-queries must be positive so setup stays outside the timed loop")
	}
	if cfg.TopK <= 0 {
		return errors.New("-top-k must be positive")
	}
	if cfg.TopK > cfg.Docs {
		return fmt.Errorf("-top-k=%d exceeds -docs=%d", cfg.TopK, cfg.Docs)
	}
	if cfg.BatchSize <= 0 {
		return errors.New("-batch-size must be positive")
	}
	if cfg.M <= 0 {
		return errors.New("-m must be positive")
	}
	if cfg.EfConstruction <= 0 {
		return errors.New("-ef-construction must be positive")
	}
	if cfg.EfSearch <= 0 {
		return errors.New("-ef-search must be positive")
	}
	return nil
}

func execute(ctx context.Context, cfg config) (result, error) {
	if err := ctx.Err(); err != nil {
		return result{}, err
	}
	dir, kept, cleanup, err := prepareFreshDir(cfg.Dir, cfg.KeepDir)
	if err != nil {
		return result{}, err
	}
	defer cleanup()

	setupStart := time.Now()
	rows := generateRows(cfg.Docs, cfg.Dims, cfg.Seed)
	queryCount := maxInt(cfg.Queries, cfg.WarmupQueries)
	queries := generateQueries(rows, queryCount, cfg.Seed+7919)
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return result{}, fmt.Errorf("save format config: %w", err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return result{}, fmt.Errorf("open db: %w", err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(collectionMeta(cfg)); err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("create collection: %w", err)
	}
	col, err := manager.OpenCollection(collectionName)
	if err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("open collection: %w", err)
	}
	if err := insertRows(col, rows, cfg.BatchSize); err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("insert rows: %w", err)
	}
	if err := col.Flush(); err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("flush collection: %w", err)
	}
	status, err := col.RebuildVectorIndex(vectorIndex)
	if err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("rebuild vector index: %w", err)
	}
	if !status.Loaded || status.State != collections.VectorIndexStateColumnGraphLoaded {
		_ = db.Close()
		return result{}, fmt.Errorf("vector index did not load column_graph state: state=%s reason=%s", status.State, status.Reason)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return result{}, fmt.Errorf("checkpoint: %w", err)
	}
	if err := db.Close(); err != nil {
		return result{}, fmt.Errorf("close after checkpoint: %w", err)
	}
	setupElapsed := time.Since(setupStart)

	reopenStart := time.Now()
	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return result{}, fmt.Errorf("reopen db: %w", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := collections.NewCollectionManager(reopened).OpenCollection(collectionName)
	if err != nil {
		return result{}, fmt.Errorf("open collection after reopen: %w", err)
	}
	reopenElapsed := time.Since(reopenStart)

	var buffer collections.VectorIndexSearchBuffer
	opts := collections.VectorIndexSearchOptions{
		IndexName: vectorIndex,
		QueryMode: collections.VectorIndexQueryModeExact,
		TopK:      cfg.TopK,
		EfSearch:  cfg.EfSearch,
		StatsMode: collections.VectorIndexSearchStatsModeProduction,
	}
	warmupStart := time.Now()
	for i := 0; i < cfg.WarmupQueries; i++ {
		opts.Query = queries[i%len(queries)].Vector
		response, err := reopenedCol.SearchVectorIndexWithBuffer(opts, &buffer)
		if err != nil {
			return result{}, fmt.Errorf("warmup search %d: %w", i, err)
		}
		if len(response.Results) == 0 {
			return result{}, fmt.Errorf("warmup search %d returned no results", i)
		}
	}
	warmupElapsed := time.Since(warmupStart)

	opts.Query = queries[0].Vector
	guardrailResponse, err := reopenedCol.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		return result{}, fmt.Errorf("route evidence search: %w", err)
	}
	if len(guardrailResponse.Results) == 0 {
		return result{}, errors.New("route evidence search returned no results")
	}
	evidence := routeEvidenceFromStats(guardrailResponse.Stats)
	if err := validateRouteEvidence(evidence); err != nil {
		return result{}, err
	}
	firstResults := copyHits(guardrailResponse.Results)

	searchStart := time.Now()
	resultsReturned := 0
	for i := 0; i < cfg.Queries; i++ {
		opts.Query = queries[i%len(queries)].Vector
		response, err := reopenedCol.SearchVectorIndexWithBuffer(opts, &buffer)
		if err != nil {
			return result{}, fmt.Errorf("timed search %d: %w", i, err)
		}
		if len(response.Results) == 0 {
			return result{}, fmt.Errorf("timed search %d returned no results", i)
		}
		resultsReturned += len(response.Results)
	}
	searchElapsed := time.Since(searchStart)
	if searchElapsed <= 0 {
		searchElapsed = time.Nanosecond
	}

	return result{
		API:                   apiName,
		Dir:                   dir,
		KeptDir:               kept,
		Docs:                  cfg.Docs,
		Dims:                  cfg.Dims,
		Metric:                defaultMetricName,
		Queries:               cfg.Queries,
		WarmupQueries:         cfg.WarmupQueries,
		TopK:                  cfg.TopK,
		BatchSize:             cfg.BatchSize,
		M:                     cfg.M,
		EfConstruction:        cfg.EfConstruction,
		EfSearch:              cfg.EfSearch,
		CheckpointReopen:      true,
		BufferReused:          true,
		StatsModeTimed:        string(collections.VectorIndexSearchStatsModeProduction),
		SetupMillis:           millis(setupElapsed),
		ReopenMillis:          millis(reopenElapsed),
		WarmupMillis:          millis(warmupElapsed),
		SearchMillis:          millis(searchElapsed),
		AvgMicros:             float64(searchElapsed.Nanoseconds()) / float64(cfg.Queries) / 1000,
		OpsPerSecond:          float64(cfg.Queries) / searchElapsed.Seconds(),
		ResultsPerSearch:      float64(resultsReturned) / float64(cfg.Queries),
		FirstResults:          firstResults,
		RouteEvidence:         evidence,
		HighQPSAPIGuide:       highQPSAPIGuide,
		BenchmarkWorkflow:     benchmarkGuide,
		InstructionalNonBench: true,
	}, nil
}

func collectionMeta(cfg config) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: collectionName,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Columns: []collections.ColumnStoreColumn{{
					Name:       vectorField,
					Path:       vectorField,
					Owner:      collections.TypedStorageOwnerColumnPart,
					ValueType:  collections.ColumnStoreValueFloat32Vector,
					VectorDims: cfg.Dims,
				}},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:           vectorIndex,
			Field:          vectorField,
			Metric:         collections.VectorMetricCosine,
			Dimensions:     cfg.Dims,
			M:              cfg.M,
			EfConstruction: cfg.EfConstruction,
			EfSearch:       cfg.EfSearch,
			Encoding:       collections.VectorIndexEncodingFloat32,
			Strategy:       collections.VectorIndexStrategyColumnGraph,
		}},
	}
}

func insertRows(col *collections.Collection, rows []vectorRow, batchSize int) error {
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			row := rows[i]
			doc, err := json.Marshal(demoDocument{
				ID:        row.ID,
				Tenant:    row.Tenant,
				Title:     row.Title,
				Category:  row.Category,
				Body:      "deterministic no-document search fixture; materialization is intentionally not timed",
				Embedding: row.Vector,
			})
			if err != nil {
				return err
			}
			ids[i-start] = []byte(row.ID)
			docs[i-start] = doc
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return err
		}
	}
	return nil
}

func generateRows(n, dims int, seed int64) []vectorRow {
	rng := rand.New(rand.NewSource(seed))
	rows := make([]vectorRow, n)
	for i := range rows {
		cluster := i % 32
		v := make([]float32, dims)
		for d := range v {
			base := math.Sin(float64((cluster+1)*(d+3))) + math.Cos(float64((i+1)*(d+5)%37))
			noise := (rng.Float64() - 0.5) * 0.02
			v[d] = float32(base + noise)
		}
		normalize(v)
		rows[i] = vectorRow{
			ID:       fmt.Sprintf("doc-%06d", i),
			Tenant:   fmt.Sprintf("tenant-%02d", i%8),
			Title:    fmt.Sprintf("deterministic vector document %06d", i),
			Category: fmt.Sprintf("topic-%02d", cluster),
			Vector:   v,
		}
	}
	return rows
}

func generateQueries(rows []vectorRow, n int, seed int64) []vectorRow {
	rng := rand.New(rand.NewSource(seed))
	queries := make([]vectorRow, n)
	for i := range queries {
		base := rows[(i*37+11)%len(rows)].Vector
		v := append([]float32(nil), base...)
		for d := range v {
			v[d] += float32((rng.Float64() - 0.5) * 0.01)
		}
		normalize(v)
		queries[i] = vectorRow{ID: fmt.Sprintf("query-%04d", i), Vector: v}
	}
	return queries
}

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

func routeEvidenceFromStats(stats collections.VectorIndexSearchStats) routeEvidence {
	return routeEvidence{
		SearchRouteHNSWSearchPack:  stats.SearchRouteHNSWSearchPack,
		HNSWSearchPackActive:       stats.HNSWSearchPackActive,
		DocumentsFetched:           stats.DocumentsFetched,
		GraphRowFallbacks:          stats.GraphRowFallbacks,
		TypedColumnVectorFallbacks: stats.TypedColumnFallbacks,
		VectorScratchDecodes:       stats.VectorScratchDecodes,
	}
}

func validateRouteEvidence(e routeEvidence) error {
	if e.SearchRouteHNSWSearchPack != 1 || e.HNSWSearchPackActive != 1 {
		return fmt.Errorf("exact no-document route evidence failed: search_route_hnsw_search_pack/search=%d hnsw_search_pack_active/search=%d", e.SearchRouteHNSWSearchPack, e.HNSWSearchPackActive)
	}
	if e.DocumentsFetched != 0 || e.GraphRowFallbacks != 0 || e.TypedColumnVectorFallbacks != 0 || e.VectorScratchDecodes != 0 {
		return fmt.Errorf("exact no-document guardrails failed: docs_fetched/search=%d graph_row_fallbacks/search=%d typed_column_vector_fallbacks/search=%d vector_scratch_decodes/search=%d", e.DocumentsFetched, e.GraphRowFallbacks, e.TypedColumnVectorFallbacks, e.VectorScratchDecodes)
	}
	return nil
}

func copyHits(results []collections.VectorIndexSearchResult) []searchHit {
	hits := make([]searchHit, len(results))
	for i, r := range results {
		hits[i] = searchHit{Rank: i + 1, ID: string(r.ID), Ordinal: r.Ordinal, Score: r.Score}
	}
	return hits
}

func printText(w io.Writer, res result) {
	fmt.Fprintln(w, "TreeDB high-QPS exact vector search demo")
	fmt.Fprintf(w, "api: %s\n", res.API)
	fmt.Fprintf(w, "scope: exact no-document collection buffered search; instructional demo, not a benchmark replacement\n")
	fmt.Fprintf(w, "fixture: docs=%d dims=%d metric=%s top_k=%d m=%d ef_construction=%d ef_search=%d\n", res.Docs, res.Dims, res.Metric, res.TopK, res.M, res.EfConstruction, res.EfSearch)
	fmt.Fprintf(w, "checkpoint_reopen: %t\n", res.CheckpointReopen)
	fmt.Fprintf(w, "buffer_reuse: one caller-owned VectorIndexSearchBuffer reused for warmup and timed searches\n")
	fmt.Fprintf(w, "warmup: queries=%d stats_mode=%s elapsed_ms=%.3f\n", res.WarmupQueries, res.StatsModeTimed, res.WarmupMillis)
	fmt.Fprintf(w, "timed_loop: queries=%d stats_mode=%s elapsed_ms=%.3f avg_us=%.3f ops_sec=%.1f results_per_search=%.1f\n", res.Queries, res.StatsModeTimed, res.SearchMillis, res.AvgMicros, res.OpsPerSecond, res.ResultsPerSearch)
	fmt.Fprintln(w, "top_k:")
	for _, hit := range res.FirstResults {
		fmt.Fprintf(w, "  %2d. id=%s ordinal=%d score=%.6f\n", hit.Rank, hit.ID, hit.Ordinal, hit.Score)
	}
	fmt.Fprintln(w, "route_evidence:")
	fmt.Fprintf(w, "  search_route_hnsw_search_pack/search=%d\n", res.RouteEvidence.SearchRouteHNSWSearchPack)
	fmt.Fprintf(w, "  hnsw_search_pack_active/search=%d\n", res.RouteEvidence.HNSWSearchPackActive)
	fmt.Fprintf(w, "  docs_fetched/search=%d\n", res.RouteEvidence.DocumentsFetched)
	fmt.Fprintf(w, "  graph_row_fallbacks/search=%d\n", res.RouteEvidence.GraphRowFallbacks)
	fmt.Fprintf(w, "  typed_column_vector_fallbacks/search=%d\n", res.RouteEvidence.TypedColumnVectorFallbacks)
	fmt.Fprintf(w, "  vector_scratch_decodes/search=%d\n", res.RouteEvidence.VectorScratchDecodes)
	fmt.Fprintln(w, "api_boundaries:")
	fmt.Fprintln(w, "  Collection.SearchVectorIndex: no-document convenience call with response-owned allocation")
	fmt.Fprintln(w, "  IncludeDocuments=true: separate document materialization path, intentionally not timed here")
	fmt.Fprintln(w, "  OpenVectorIndexSearcher + SearchWithBuffer: reusable low-level serving when callers own searcher lifetime")
	fmt.Fprintf(w, "guidance: %s\n", res.HighQPSAPIGuide)
	fmt.Fprintf(w, "performance_evidence: %s\n", res.BenchmarkWorkflow)
}

func prepareFreshDir(dir string, keep bool) (string, bool, func(), error) {
	if strings.TrimSpace(dir) == "" {
		tmp, err := os.MkdirTemp("", "treedb-vector-highqps-demo-*")
		if err != nil {
			return "", false, nil, err
		}
		if keep {
			return tmp, true, func() {}, nil
		}
		return tmp, false, func() { _ = os.RemoveAll(tmp) }, nil
	}
	abs, err := validateFreshDemoDir(dir)
	if err != nil {
		return "", false, nil, err
	}
	if err := ensureFreshDemoDir(abs); err != nil {
		return "", false, nil, err
	}
	return abs, true, func() {}, nil
}

func validateFreshDemoDir(dir string) (string, error) {
	rawInput := strings.TrimSpace(dir)
	if rawInput == "" || demoDirHasParentTraversal(rawInput) {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	cleanInput := filepath.Clean(rawInput)
	if cleanInput == "." || cleanInput == ".." {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	abs, err := filepath.Abs(cleanInput)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	root := filepath.VolumeName(abs) + string(os.PathSeparator)
	if abs == root || abs == filepath.Clean(os.TempDir()) {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	return abs, nil
}

func demoDirHasParentTraversal(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return true
		}
	}
	return false
}

func ensureFreshDemoDir(abs string) error {
	info, err := os.Stat(abs)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("-dir %q exists and is not a directory", abs)
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("-dir %q already exists and is not empty; choose a fresh directory", abs)
		}
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(abs, 0o755)
	}
	return err
}

func millis(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
