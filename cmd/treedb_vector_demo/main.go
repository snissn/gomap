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
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	vectorIndexName        = "embedding_graph"
	collectionName         = "docs"
	defaultPreset          = "vector-rag"
	defaultFilterTenant    = "tenant-00"
	defaultVectorBatchSize = 512
)

type config struct {
	Rows           int
	Dims           int
	VectorMode     string
	MetadataMode   string
	Queries        int
	TopK           int
	BatchSize      int
	MetadataFilter bool
	FinalFetch     bool
	Dir            string
	KeepDir        bool
	Seed           int64
	ProfileDir     string
	Preset         string
	EfSearch       int
	MaxDecoded     int
}

type vectorRow struct {
	ID       string
	Tenant   string
	Title    string
	TimeUS   int64
	Category string
	Vector   []float32
}

type queryFixture struct {
	Name   string
	Vector []float32
}

type searchHit struct {
	ID      string  `json:"id"`
	Ordinal int     `json:"ordinal"`
	Score   float64 `json:"score"`
}

type result struct {
	Preset                    string      `json:"preset"`
	Dir                       string      `json:"dir"`
	KeptDir                   bool        `json:"kept_dir"`
	Rows                      int         `json:"rows"`
	Dims                      int         `json:"dims"`
	Queries                   int         `json:"queries"`
	TopK                      int         `json:"top_k"`
	BatchSize                 int         `json:"batch_size"`
	VectorMode                string      `json:"vector_mode"`
	MetadataMode              string      `json:"metadata_mode"`
	MetadataFilter            bool        `json:"metadata_filter"`
	MetadataFilterDescription string      `json:"metadata_filter_description,omitempty"`
	FinalFetch                bool        `json:"final_fetch"`
	FinalFetchShape           string      `json:"final_fetch_shape"`
	SearchPath                string      `json:"search_path"`
	SetupMillis               float64     `json:"setup_ms"`
	SearchMillis              float64     `json:"search_ms"`
	FetchMillis               float64     `json:"fetch_ms"`
	OpsPerSecond              float64     `json:"ops_sec"`
	CandidateRows             uint64      `json:"candidate_rows"`
	Candidates                uint64      `json:"candidates"`
	CandidatesPerSearch       float64     `json:"candidates_per_search"`
	VisitedNodes              uint64      `json:"visited_nodes"`
	VisitedEdges              uint64      `json:"visited_edges"`
	VectorBytesRead           uint64      `json:"vector_bytes_read"`
	AdjacencyBytesRead        uint64      `json:"adjacency_bytes_read"`
	ScoredVectors             uint64      `json:"scored_vectors"`
	DocsFetched               uint64      `json:"docs_fetched"`
	DocumentBytes             uint64      `json:"document_bytes"`
	MappedBytesPeak           uint64      `json:"mapped_bytes_peak"`
	HeapCopyBytesPeak         uint64      `json:"heap_copy_bytes_peak"`
	DecodedBytesPeak          uint64      `json:"decoded_bytes_peak"`
	PhysicalBytesRead         int64       `json:"physical_bytes_read"`
	VectorDirectViews         uint64      `json:"vector_direct_views"`
	VectorScratchDecodes      uint64      `json:"vector_scratch_decodes"`
	AdjacencyDirectViews      uint64      `json:"adjacency_direct_views"`
	AdjacencyScratchDecodes   uint64      `json:"adjacency_scratch_decodes"`
	TypedColumnFallbacks      uint64      `json:"typed_column_fallbacks"`
	ProfileDir                string      `json:"profile_dir,omitempty"`
	CPUProfile                string      `json:"cpu_profile,omitempty"`
	AllocsProfile             string      `json:"allocs_profile,omitempty"`
	SummaryJSON               string      `json:"summary_json,omitempty"`
	SummaryMarkdown           string      `json:"summary_markdown,omitempty"`
	FirstResults              []searchHit `json:"first_results,omitempty"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "treedb_vector_demo: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	var cpuFile *os.File
	if cfg.ProfileDir != "" {
		if err := os.MkdirAll(cfg.ProfileDir, 0o755); err != nil {
			return err
		}
		path := filepath.Join(cfg.ProfileDir, "vector_demo_cpu.pprof")
		cpuFile, err = os.Create(path)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return err
		}
	}
	res, execErr := execute(context.Background(), cfg)
	if cpuFile != nil {
		pprof.StopCPUProfile()
		if err := cpuFile.Close(); execErr == nil && err != nil {
			execErr = err
		}
	}
	if execErr != nil {
		return execErr
	}
	if cfg.ProfileDir != "" {
		if err := writeProfileArtifacts(cfg.ProfileDir, &res); err != nil {
			return err
		}
	}
	printText(stdout, res)
	if !res.KeptDir {
		fmt.Fprintln(stderr, "temporary db removed; rerun with -keep-dir to inspect files")
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	preset, err := scanPreset(args)
	if err != nil {
		return config{}, err
	}
	cfg, err := presetConfig(preset)
	if err != nil {
		return config{}, err
	}
	fs := flag.NewFlagSet("treedb_vector_demo", flag.ContinueOnError)
	fs.IntVar(&cfg.Rows, "rows", cfg.Rows, "number of deterministic documents/vectors to load")
	fs.IntVar(&cfg.Dims, "dims", cfg.Dims, "embedding dimensions")
	fs.StringVar(&cfg.VectorMode, "vectors", cfg.VectorMode, "vector storage mode: document, typed-row, typed-column")
	fs.StringVar(&cfg.MetadataMode, "metadata", cfg.MetadataMode, "metadata storage mode: document, typed-row, typed-column")
	fs.IntVar(&cfg.Queries, "queries", cfg.Queries, "number of vector queries to run")
	fs.IntVar(&cfg.TopK, "top-k", cfg.TopK, "nearest neighbors per query")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "documents per InsertBatch call")
	fs.BoolVar(&cfg.MetadataFilter, "metadata-filter", cfg.MetadataFilter, "restrict scoring/search output to a deterministic tenant filter when supported")
	fs.BoolVar(&cfg.FinalFetch, "final-fetch", cfg.FinalFetch, "fetch explicit full documents (including embeddings) after top-k selection and time that separately")
	fs.StringVar(&cfg.Dir, "dir", cfg.Dir, "TreeDB directory; empty uses a temporary directory")
	fs.BoolVar(&cfg.KeepDir, "keep-dir", cfg.KeepDir, "keep an automatically-created temporary DB directory after the run")
	fs.Int64Var(&cfg.Seed, "seed", cfg.Seed, "deterministic fixture seed")
	fs.StringVar(&cfg.ProfileDir, "profile-dir", cfg.ProfileDir, "optional directory for vector_demo_cpu.pprof, vector_demo_allocs.pprof, and summaries")
	fs.StringVar(&cfg.Preset, "preset", cfg.Preset, "persona preset: vector-rag or perf-engineer")
	fs.IntVar(&cfg.EfSearch, "ef-search", cfg.EfSearch, "column_graph graph exploration bound; 0 uses index default")
	fs.IntVar(&cfg.MaxDecoded, "max-decoded-blocks", cfg.MaxDecoded, "bounded physical column decoded-block cache size")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	return cfg, validateConfig(cfg)
}

func scanPreset(args []string) (string, error) {
	preset := defaultPreset
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-preset" {
			if i+1 >= len(args) {
				return "", errors.New("missing value for -preset")
			}
			preset = args[i+1]
			i++
			continue
		}
		if arg == "--preset" {
			if i+1 >= len(args) {
				return "", errors.New("missing value for --preset")
			}
			preset = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "-preset=") {
			preset = strings.TrimPrefix(arg, "-preset=")
		}
		if strings.HasPrefix(arg, "--preset=") {
			preset = strings.TrimPrefix(arg, "--preset=")
		}
	}
	return preset, nil
}

func presetConfig(preset string) (config, error) {
	switch preset {
	case "", "vector-rag":
		return config{Rows: 1000, Dims: 128, VectorMode: "typed-column", MetadataMode: "typed-row", Queries: 10, TopK: 10, BatchSize: defaultVectorBatchSize, Seed: 1, Preset: "vector-rag", EfSearch: 128, MaxDecoded: 4}, nil
	case "perf-engineer":
		return config{Rows: 10000, Dims: 128, VectorMode: "typed-column", MetadataMode: "typed-row", Queries: 100, TopK: 10, BatchSize: defaultVectorBatchSize, Seed: 1, Preset: "perf-engineer", EfSearch: 128, MaxDecoded: 8}, nil
	default:
		return config{}, fmt.Errorf("unsupported preset %q (supported: vector-rag, perf-engineer)", preset)
	}
}

func validateConfig(cfg config) error {
	if cfg.Rows <= 0 {
		return errors.New("rows must be positive")
	}
	if cfg.Dims <= 0 {
		return errors.New("dims must be positive")
	}
	if cfg.Queries <= 0 {
		return errors.New("queries must be positive")
	}
	if cfg.TopK <= 0 {
		return errors.New("top-k must be positive")
	}
	if cfg.TopK > cfg.Rows {
		return errors.New("top-k cannot exceed rows")
	}
	if cfg.BatchSize <= 0 {
		return errors.New("batch-size must be positive")
	}
	if cfg.EfSearch < 0 || cfg.MaxDecoded < 0 {
		return errors.New("ef-search and max-decoded-blocks must be non-negative")
	}
	if cfg.VectorMode != "typed-column" {
		return fmt.Errorf("unsupported -vectors %q: this pre-alpha demo currently publishes/searches typed-column dense vector sections only", cfg.VectorMode)
	}
	switch cfg.MetadataMode {
	case "document", "typed-row":
		return nil
	case "typed-column":
		return errors.New("unsupported -metadata typed-column: scalar typed-column metadata filtering is not wired into public vector search yet; use document or typed-row")
	default:
		return fmt.Errorf("unsupported -metadata %q (supported: document, typed-row)", cfg.MetadataMode)
	}
}

func execute(ctx context.Context, cfg config) (result, error) {
	if err := ctx.Err(); err != nil {
		return result{}, err
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultVectorBatchSize
	}
	dir, kept, cleanup, err := prepareFreshDir(cfg.Dir, cfg.KeepDir)
	if err != nil {
		return result{}, err
	}
	defer cleanup()
	cfg.Dir = dir

	setupStart := time.Now()
	rows := generateRows(cfg.Rows, cfg.Dims, cfg.Seed)
	queries := generateQueries(rows, cfg.Queries, cfg.Seed+7919)
	if err := backenddb.SaveFormatConfig(cfg.Dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return result{}, err
	}
	db, err := backenddb.Open(backenddb.Options{Dir: cfg.Dir, DisableBackgroundPrune: true})
	if err != nil {
		return result{}, err
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(collectionMeta(cfg)); err != nil {
		_ = db.Close()
		return result{}, err
	}
	col, err := manager.OpenCollection(collectionName)
	if err != nil {
		_ = db.Close()
		return result{}, err
	}
	if err := insertRows(col, rows, cfg.BatchSize); err != nil {
		_ = db.Close()
		return result{}, err
	}
	if status, err := col.RebuildVectorIndex(vectorIndexName); err != nil {
		_ = db.Close()
		return result{}, err
	} else if !status.Loaded {
		_ = db.Close()
		return result{}, fmt.Errorf("vector index rebuild did not load column_graph: state=%s reason=%s", status.State, status.Reason)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return result{}, err
	}
	if err := db.Close(); err != nil {
		return result{}, err
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: cfg.Dir, DisableBackgroundPrune: true})
	if err != nil {
		return result{}, err
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := collections.NewCollectionManager(reopened).OpenCollection(collectionName)
	if err != nil {
		return result{}, err
	}
	setupElapsed := time.Since(setupStart)

	res := result{
		Preset:                    cfg.Preset,
		Dir:                       cfg.Dir,
		KeptDir:                   kept,
		Rows:                      cfg.Rows,
		Dims:                      cfg.Dims,
		Queries:                   cfg.Queries,
		TopK:                      cfg.TopK,
		BatchSize:                 cfg.BatchSize,
		VectorMode:                cfg.VectorMode,
		MetadataMode:              cfg.MetadataMode,
		MetadataFilter:            cfg.MetadataFilter,
		MetadataFilterDescription: metadataFilterDescription(cfg),
		FinalFetch:                cfg.FinalFetch,
		FinalFetchShape:           vectorDemoFinalFetchShape(cfg.FinalFetch),
		SetupMillis:               millis(setupElapsed),
		ProfileDir:                cfg.ProfileDir,
	}
	if cfg.MetadataFilter {
		if err := runExactFilteredQueries(reopenedCol, rows, queries, cfg, &res); err != nil {
			return result{}, err
		}
	} else {
		if err := runColumnGraphQueries(reopenedCol, queries, cfg, &res); err != nil {
			return result{}, err
		}
	}
	return res, nil
}

func collectionMeta(cfg config) *collections.CollectionMeta {
	columns := []collections.ColumnStoreColumn{
		{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: cfg.Dims},
	}
	var sortKey []collections.ColumnSortKey
	if cfg.MetadataMode == "typed-row" {
		columns = append([]collections.ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", Owner: collections.TypedStorageOwnerRowAsset, ValueType: collections.ColumnStoreValueInt64},
			{Name: "tenant", Path: "tenant", Owner: collections.TypedStorageOwnerRowAsset, ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "category", Path: "category", Owner: collections.TypedStorageOwnerRowAsset, ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "title", Path: "title", Owner: collections.TypedStorageOwnerRowAsset, ValueType: collections.ColumnStoreValueString, Dictionary: true},
		}, columns...)
		sortKey = []collections.ColumnSortKey{{Column: "time_us"}}
	}
	return &collections.CollectionMeta{
		Name: collectionName,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Columns:         columns,
				SortKey:         sortKey,
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:           vectorIndexName,
			Field:          "embedding",
			Metric:         collections.VectorMetricCosine,
			Dimensions:     cfg.Dims,
			M:              16,
			EfConstruction: 128,
			EfSearch:       cfg.EfSearch,
			Encoding:       collections.VectorIndexEncodingFloat32,
			Strategy:       collections.VectorIndexStrategyColumnGraph,
		}},
	}
}

func generateRows(n, dims int, seed int64) []vectorRow {
	rng := rand.New(rand.NewSource(seed))
	rows := make([]vectorRow, n)
	for i := range rows {
		v := make([]float32, dims)
		cluster := i % 16
		for d := range v {
			base := math.Sin(float64((cluster+1)*(d+3))) + math.Cos(float64((i+1)*(d+1)%29))
			noise := (rng.Float64() - 0.5) * 0.025
			v[d] = float32(base + noise)
		}
		normalize(v)
		rows[i] = vectorRow{
			ID:       fmt.Sprintf("doc-%06d", i),
			Tenant:   fmt.Sprintf("tenant-%02d", i%8),
			Title:    fmt.Sprintf("deterministic document %06d", i),
			TimeUS:   int64(1_700_000_000_000_000 + i*1000),
			Category: fmt.Sprintf("topic-%02d", cluster),
			Vector:   v,
		}
	}
	return rows
}

func generateQueries(rows []vectorRow, n int, seed int64) []queryFixture {
	rng := rand.New(rand.NewSource(seed))
	queries := make([]queryFixture, n)
	for i := range queries {
		base := rows[(i*37+11)%len(rows)].Vector
		v := append([]float32(nil), base...)
		for d := range v {
			v[d] += float32((rng.Float64() - 0.5) * 0.01)
		}
		normalize(v)
		queries[i] = queryFixture{Name: fmt.Sprintf("query-%04d", i), Vector: v}
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

func insertRows(col *collections.Collection, rows []vectorRow, batchSize int) error {
	if batchSize <= 0 {
		batchSize = defaultVectorBatchSize
	}
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			row := rows[i]
			raw, err := json.Marshal(map[string]any{
				"did":       row.ID,
				"tenant":    row.Tenant,
				"title":     row.Title,
				"time_us":   row.TimeUS,
				"category":  row.Category,
				"body":      fmt.Sprintf("RAG fixture body for %s in %s", row.ID, row.Category),
				"embedding": row.Vector,
			})
			if err != nil {
				return err
			}
			ids[i-start] = []byte(row.ID)
			docs[i-start] = raw
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return err
		}
	}
	return nil
}

func vectorDemoFinalFetchShape(finalFetch bool) string {
	if finalFetch {
		return "full_document_embedding_echo"
	}
	return "none"
}

func runColumnGraphQueries(col *collections.Collection, queries []queryFixture, cfg config, res *result) error {
	searcher, err := col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: vectorIndexName, MaxDecodedBlocks: cfg.MaxDecoded})
	if err != nil {
		return err
	}
	defer func() { _ = searcher.Close() }()
	res.SearchPath = string(collections.VectorIndexSearchPathColumnGraphNativeReader)
	var fetchElapsed time.Duration
	searchStart := time.Now()
	for i, q := range queries {
		got, err := searcher.Search(collections.VectorIndexSearcherSearchOptions{Query: q.Vector, TopK: cfg.TopK, EfSearch: cfg.EfSearch, IncludeDocuments: false})
		if err != nil {
			return err
		}
		if len(got.Results) == 0 {
			return fmt.Errorf("query %s returned no results", q.Name)
		}
		res.CandidateRows += got.Stats.CandidateRows
		res.Candidates += got.Stats.Candidates
		res.VisitedNodes += got.Stats.VisitedNodes
		res.VisitedEdges += got.Stats.VisitedEdges
		res.VectorBytesRead += got.Stats.VectorBytesRead
		res.AdjacencyBytesRead += got.Stats.AdjacencyBytesRead
		res.ScoredVectors += got.Stats.CandidateFetches
		res.PhysicalBytesRead += got.Stats.PhysicalBytesRead
		res.VectorDirectViews += got.Stats.VectorDirectViews
		res.VectorScratchDecodes += got.Stats.VectorScratchDecodes
		res.AdjacencyDirectViews += got.Stats.AdjacencyDirectViews
		res.AdjacencyScratchDecodes += got.Stats.AdjacencyScratchDecodes
		res.TypedColumnFallbacks += got.Stats.TypedColumnFallbacks
		res.MappedBytesPeak = max(res.MappedBytesPeak, got.Stats.TypedColumnMappedBytes)
		res.HeapCopyBytesPeak = max(res.HeapCopyBytesPeak, got.Stats.TypedColumnHeapCopyBytes)
		res.DecodedBytesPeak = max(res.DecodedBytesPeak, got.Stats.TypedColumnDecodedBytes)
		if i == 0 {
			res.FirstResults = hitsFromSearchResults(got.Results)
		}
		if cfg.FinalFetch {
			start := time.Now()
			for _, hit := range got.Results {
				doc, err := col.Get(hit.ID)
				if err != nil {
					return err
				}
				res.DocsFetched++
				res.DocumentBytes += uint64(len(doc))
			}
			fetchElapsed += time.Since(start)
		}
	}
	searchElapsed := time.Since(searchStart) - fetchElapsed
	setSearchTiming(res, cfg.Queries, searchElapsed, fetchElapsed)
	res.CandidatesPerSearch = float64(res.Candidates) / float64(cfg.Queries)
	return nil
}

func runExactFilteredQueries(col *collections.Collection, rows []vectorRow, queries []queryFixture, cfg config, res *result) error {
	res.SearchPath = "exact_scoring_metadata_filter"
	filteredRows := 0
	for _, row := range rows {
		if row.Tenant == defaultFilterTenant {
			filteredRows++
		}
	}
	var fetchElapsed time.Duration
	searchStart := time.Now()
	for i, q := range queries {
		hits := exactTopK(rows, q.Vector, cfg.TopK, func(row vectorRow) bool { return row.Tenant == defaultFilterTenant })
		if len(hits) == 0 {
			return fmt.Errorf("query %s returned no exact filtered results", q.Name)
		}
		res.CandidateRows += uint64(filteredRows)
		res.Candidates += uint64(filteredRows)
		res.VisitedNodes += uint64(filteredRows)
		res.VectorBytesRead += uint64(filteredRows * len(q.Vector) * 4)
		res.ScoredVectors += uint64(filteredRows)
		if i == 0 {
			res.FirstResults = hits
		}
		if cfg.FinalFetch {
			start := time.Now()
			for _, hit := range hits {
				doc, err := col.Get([]byte(hit.ID))
				if err != nil {
					return err
				}
				res.DocsFetched++
				res.DocumentBytes += uint64(len(doc))
			}
			fetchElapsed += time.Since(start)
		}
	}
	searchElapsed := time.Since(searchStart) - fetchElapsed
	setSearchTiming(res, cfg.Queries, searchElapsed, fetchElapsed)
	res.CandidatesPerSearch = float64(res.Candidates) / float64(cfg.Queries)
	return nil
}

func setSearchTiming(res *result, queries int, searchElapsed, fetchElapsed time.Duration) {
	if searchElapsed <= 0 {
		searchElapsed = time.Nanosecond
	}
	res.SearchMillis = millis(searchElapsed)
	res.FetchMillis = millis(fetchElapsed)
	if queries > 0 {
		res.OpsPerSecond = float64(queries) / searchElapsed.Seconds()
	}
}

func exactTopK(rows []vectorRow, query []float32, topK int, filter func(vectorRow) bool) []searchHit {
	hits := make([]searchHit, 0, len(rows))
	for i, row := range rows {
		if filter != nil && !filter(row) {
			continue
		}
		hits = append(hits, searchHit{ID: row.ID, Ordinal: i, Score: dot(row.Vector, query)})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Score == hits[j].Score {
			return hits[i].ID < hits[j].ID
		}
		return hits[i].Score > hits[j].Score
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	return hits
}

func dot(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func hitsFromSearchResults(results []collections.VectorIndexSearchResult) []searchHit {
	hits := make([]searchHit, len(results))
	for i, r := range results {
		hits[i] = searchHit{ID: string(r.ID), Ordinal: r.Ordinal, Score: r.Score}
	}
	return hits
}

func prepareFreshDir(dir string, keep bool) (string, bool, func(), error) {
	if strings.TrimSpace(dir) == "" {
		tmp, err := os.MkdirTemp("", "treedb-vector-demo-*")
		if err != nil {
			return "", false, nil, err
		}
		if !keep {
			return tmp, false, func() { _ = os.RemoveAll(tmp) }, nil
		}
		return tmp, true, func() {}, nil
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

func writeProfileArtifacts(dir string, res *result) error {
	res.ProfileDir = dir
	res.CPUProfile = filepath.Join(dir, "vector_demo_cpu.pprof")
	res.AllocsProfile = filepath.Join(dir, "vector_demo_allocs.pprof")
	res.SummaryJSON = filepath.Join(dir, "vector_demo_summary.json")
	res.SummaryMarkdown = filepath.Join(dir, "vector_demo_summary.md")
	runtime.GC()
	allocs, err := os.Create(res.AllocsProfile)
	if err != nil {
		return err
	}
	if err := pprof.Lookup("allocs").WriteTo(allocs, 0); err != nil {
		_ = allocs.Close()
		return err
	}
	if err := allocs.Close(); err != nil {
		return err
	}
	jsonBytes, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(res.SummaryJSON, append(jsonBytes, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(res.SummaryMarkdown, []byte(summaryMarkdown(*res)), 0o644)
}

func summaryMarkdown(res result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# TreeDB vector demo summary\n\n")
	fmt.Fprintf(&b, "- preset: `%s`\n- vectors: `%s`\n- metadata: `%s`\n- rows: `%d`\n- dims: `%d`\n- queries: `%d`\n- top_k: `%d`\n- batch_size: `%d`\n- search_path: `%s`\n- final_fetch_shape: `%s`\n- setup_ms: `%.3f`\n- search_ms: `%.3f`\n- fetch_ms: `%.3f`\n- ops_sec: `%.2f`\n- candidate_rows: `%d`\n- visited_nodes: `%d`\n- visited_edges: `%d`\n- vector_bytes_read: `%d`\n- adjacency_bytes_read: `%d`\n- vector_direct_views: `%d`\n- vector_scratch_decodes: `%d`\n- adjacency_direct_views: `%d`\n- adjacency_scratch_decodes: `%d`\n- docs_fetched: `%d`\n- mapped_bytes_peak: `%d`\n- heap_copy_bytes_peak: `%d`\n- decoded_bytes_peak: `%d`\n", res.Preset, res.VectorMode, res.MetadataMode, res.Rows, res.Dims, res.Queries, res.TopK, res.BatchSize, res.SearchPath, res.FinalFetchShape, res.SetupMillis, res.SearchMillis, res.FetchMillis, res.OpsPerSecond, res.CandidateRows, res.VisitedNodes, res.VisitedEdges, res.VectorBytesRead, res.AdjacencyBytesRead, res.VectorDirectViews, res.VectorScratchDecodes, res.AdjacencyDirectViews, res.AdjacencyScratchDecodes, res.DocsFetched, res.MappedBytesPeak, res.HeapCopyBytesPeak, res.DecodedBytesPeak)
	fmt.Fprintf(&b, "\nArtifacts: `vector_demo_cpu.pprof`, `vector_demo_allocs.pprof`, `vector_demo_summary.json`, `vector_demo_summary.md`.\n")
	return b.String()
}

func printText(w io.Writer, res result) {
	fmt.Fprintf(w, "TreeDB vector/RAG demo (pre-alpha collections)\n")
	fmt.Fprintf(w, "preset=%s vectors=%s metadata=%s metadata_filter=%t final_fetch=%t final_fetch_shape=%s\n", res.Preset, res.VectorMode, res.MetadataMode, res.MetadataFilter, res.FinalFetch, res.FinalFetchShape)
	fmt.Fprintf(w, "db_dir=%s rows=%d dims=%d queries=%d top_k=%d batch_size=%d search_path=%s\n", res.Dir, res.Rows, res.Dims, res.Queries, res.TopK, res.BatchSize, res.SearchPath)
	fmt.Fprintf(w, "setup_ms=%.3f search_ms=%.3f fetch_ms=%.3f ops_sec=%.2f\n", res.SetupMillis, res.SearchMillis, res.FetchMillis, res.OpsPerSecond)
	fmt.Fprintf(w, "candidate_rows=%d candidates=%d candidates_per_search=%.2f visited_nodes=%d visited_edges=%d scored_vectors=%d docs_fetched=%d document_bytes=%d\n", res.CandidateRows, res.Candidates, res.CandidatesPerSearch, res.VisitedNodes, res.VisitedEdges, res.ScoredVectors, res.DocsFetched, res.DocumentBytes)
	fmt.Fprintf(w, "vector_bytes_read=%d adjacency_bytes_read=%d mapped_bytes_peak=%d heap_copy_bytes_peak=%d decoded_bytes_peak=%d physical_bytes_read=%d vector_direct_views=%d vector_scratch_decodes=%d adjacency_direct_views=%d adjacency_scratch_decodes=%d typed_column_fallbacks=%d\n", res.VectorBytesRead, res.AdjacencyBytesRead, res.MappedBytesPeak, res.HeapCopyBytesPeak, res.DecodedBytesPeak, res.PhysicalBytesRead, res.VectorDirectViews, res.VectorScratchDecodes, res.AdjacencyDirectViews, res.AdjacencyScratchDecodes, res.TypedColumnFallbacks)
	if res.MetadataFilterDescription != "" {
		fmt.Fprintf(w, "metadata_filter_description=%s\n", res.MetadataFilterDescription)
	}
	if res.ProfileDir != "" {
		fmt.Fprintf(w, "profile_dir=%s\n", res.ProfileDir)
		fmt.Fprintf(w, "profile_artifacts=vector_demo_cpu.pprof,vector_demo_allocs.pprof,vector_demo_summary.json,vector_demo_summary.md\n")
	}
	for i, hit := range res.FirstResults {
		if i >= 5 {
			break
		}
		fmt.Fprintf(w, "result[%d] id=%s ordinal=%d score=%.6f\n", i, hit.ID, hit.Ordinal, hit.Score)
	}
}

func metadataFilterDescription(cfg config) string {
	if !cfg.MetadataFilter {
		return ""
	}
	return "tenant=" + defaultFilterTenant + "; exact_scoring is used because public column_graph search does not yet expose metadata predicates"
}

func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
