package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const defaultIndexName = "embedding_graph"
const minDemoResetDirLen = 8

type demoConfig struct {
	Dir                 string
	Reset               bool
	Rows                int
	Dims                int
	Degree              int
	TopK                int
	EfSearch            int
	MaxDecodedBlocks    int
	IncludeDocs         bool
	IncludeDocEmbedding bool
	GlovePath           string
}

type demoRow struct {
	ID     string
	Label  string
	Vector []float32
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "treedb column_graph demo: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	if cfg.TopK < 0 || cfg.EfSearch < 0 || cfg.MaxDecodedBlocks < 0 {
		return errors.New("top-k, ef-search, and max-decoded-blocks must be non-negative")
	}
	if cfg.Rows <= 0 {
		return errors.New("rows must be positive")
	}
	if cfg.Degree < 0 {
		return errors.New("degree must be non-negative")
	}
	if cfg.IncludeDocEmbedding && !cfg.IncludeDocs {
		return errors.New("include-doc-embedding requires include-docs")
	}

	cleanup := func() {}
	if cfg.Dir == "" {
		dir, err := os.MkdirTemp("", "treedb-column-graph-demo-*")
		if err != nil {
			return err
		}
		cfg.Dir = dir
		cleanup = func() { _ = os.RemoveAll(dir) }
	}
	defer cleanup()
	if cfg.Reset {
		resetDir, err := validateDemoResetDir(cfg.Dir)
		if err != nil {
			return err
		}
		cfg.Dir = resetDir
		if err := os.RemoveAll(cfg.Dir); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return err
	}

	rows, dims, err := loadDemoRows(cfg)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("dataset is empty")
	}
	query := append([]float32(nil), rows[0].Vector...)
	if cfg.TopK == 0 {
		cfg.TopK = min(10, len(rows))
	}
	if cfg.EfSearch == 0 {
		cfg.EfSearch = 128
	}
	if cfg.TopK > len(rows) {
		cfg.TopK = len(rows)
	}

	if err := backenddb.SaveFormatConfig(cfg.Dir, backenddb.FormatConfig{
		RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
	}); err != nil {
		return err
	}
	db, err := backenddb.Open(backenddb.Options{Dir: cfg.Dir, DisableBackgroundPrune: true})
	if err != nil {
		return err
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(demoCollectionMeta(dims, cfg.Degree)); err != nil {
		_ = db.Close()
		return err
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		_ = db.Close()
		return err
	}
	if err := insertDemoRows(col, rows); err != nil {
		_ = db.Close()
		return err
	}
	status, err := col.RebuildVectorIndex(defaultIndexName)
	if err != nil {
		_ = db.Close()
		return err
	}
	if !status.Loaded {
		_ = db.Close()
		return fmt.Errorf("rebuild status=%+v, want loaded column_graph", status)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: cfg.Dir, DisableBackgroundPrune: true})
	if err != nil {
		return err
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := collections.NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		return err
	}
	searcher, err := reopenedCol.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{
		IndexName:        defaultIndexName,
		MaxDecodedBlocks: cfg.MaxDecodedBlocks,
	})
	if err != nil {
		return err
	}
	defer func() { _ = searcher.Close() }()
	searchOpts, docProjection, err := demoVectorSearchOptions(cfg, query)
	if err != nil {
		return err
	}
	got, err := searcher.Search(searchOpts)
	if err != nil {
		return err
	}
	if len(got.Results) == 0 {
		return errors.New("search returned no results")
	}

	fmt.Fprintf(stdout, "TreeDB column_graph native-reader demo\n")
	fmt.Fprintf(stdout, "db_dir=%s rows=%d dims=%d degree=%d top_k=%d ef_search=%d\n", cfg.Dir, len(rows), dims, cfg.Degree, cfg.TopK, cfg.EfSearch)
	fmt.Fprintf(stdout, "rebuild status=%s loaded=%t reason=%s\n", status.State, status.Loaded, status.Reason)
	fmt.Fprintf(stdout, "search path=%s status=%s loaded=%t results=%d include_docs=%t doc_projection=%s\n", got.Path, got.Status.State, got.Status.Loaded, len(got.Results), cfg.IncludeDocs, docProjection)
	fmt.Fprintf(stdout, "stats candidate_rows=%d candidates=%d edges=%d visited_nodes=%d visited_edges=%d vector_B=%d adjacency_B=%d row_fetches=%d cache_hits=%d cache_misses=%d decoded_blocks=%d granules_touched=%d physical_B=%d max_resident_B=%d docs_fetched=%d doc_output_B=%d doc_fields_skipped=%d\n",
		got.Stats.CandidateRows,
		got.Stats.Candidates,
		got.Stats.Edges,
		got.Stats.VisitedNodes,
		got.Stats.VisitedEdges,
		got.Stats.VectorBytesRead,
		got.Stats.AdjacencyBytesRead,
		got.Stats.RowFetches,
		got.Stats.CacheHits,
		got.Stats.CacheMisses,
		got.Stats.DecodedBlocks,
		got.Stats.GranulesTouched,
		got.Stats.PhysicalBytesRead,
		got.Stats.MaxResidentBytes,
		got.Stats.DocumentsFetched,
		got.Stats.DocumentOutputBytes,
		got.Stats.DocumentFieldsSkipped,
	)
	for i, result := range got.Results {
		if i >= 5 {
			break
		}
		fmt.Fprintf(stdout, "result[%d] id=%s ordinal=%d score=%.6f", i, result.ID, result.Ordinal, result.Score)
		if cfg.IncludeDocs {
			fmt.Fprintf(stdout, " document_B=%d", len(result.Document))
		}
		fmt.Fprintln(stdout)
	}
	_, _ = fmt.Fprintln(stderr, "tip: use OpenVectorIndexSearcher for steady-state queries; SearchVectorIndex is a one-shot path that pays open/setup per call.")
	return nil
}

func validateDemoResetDir(dir string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", errors.New("reset directory is empty")
	}
	cleanInput := filepath.Clean(strings.TrimSpace(dir))
	if cleanInput == "." || cleanInput == ".." {
		return "", fmt.Errorf("refusing to reset unsafe directory %q", dir)
	}
	if demoResetDirHasParentTraversal(dir) {
		return "", fmt.Errorf("refusing to reset unsafe directory %q", dir)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	root := filepath.VolumeName(abs) + string(os.PathSeparator)
	if abs == root || len(abs) < minDemoResetDirLen {
		return "", fmt.Errorf("refusing to reset unsafe directory %q", dir)
	}
	base := filepath.Base(abs)
	if base == "." || base == ".." {
		return "", fmt.Errorf("refusing to reset unsafe directory %q", dir)
	}

	allowedBases := make([]string, 0, 3)
	if cwd, err := os.Getwd(); err == nil && !demoResetDirIsRoot(cwd) {
		allowedBases = append(allowedBases, cwd)
	}
	allowedBases = append(allowedBases, os.TempDir())
	if filepath.VolumeName(abs) == "" {
		allowedBases = append(allowedBases, "/tmp")
	}
	for _, allowedBase := range allowedBases {
		if demoResetDirWithinBase(abs, allowedBase) {
			return abs, nil
		}
	}
	return "", fmt.Errorf("refusing to reset %q: directory must be under the current working directory or temp directory", dir)
}

func demoResetDirWithinBase(abs, base string) bool {
	if demoResetDirHasParentTraversal(abs) || demoResetDirHasParentTraversal(base) {
		return false
	}
	resolvedAbs, err := demoResetDirPhysicalPath(abs)
	if err != nil {
		return false
	}
	resolvedBase, err := demoResetDirPhysicalPath(base)
	if err != nil {
		return false
	}
	if demoResetDirIsRoot(resolvedBase) {
		return false
	}
	if resolvedAbs == resolvedBase {
		return false
	}
	rel, err := filepath.Rel(resolvedBase, resolvedAbs)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func demoResetDirPhysicalPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	missing := []string{}
	cur := abs
	for {
		resolved, err := filepath.EvalSymlinks(cur)
		if err == nil {
			parts := append([]string{filepath.Clean(resolved)}, missing...)
			return filepath.Clean(filepath.Join(parts...)), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", err
		}
		missing = append([]string{filepath.Base(cur)}, missing...)
		cur = parent
	}
}

func demoResetDirHasParentTraversal(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if part == ".." {
			return true
		}
	}
	return false
}

func demoResetDirIsRoot(path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	abs = filepath.Clean(abs)
	return abs == filepath.VolumeName(abs)+string(os.PathSeparator)
}

func parseConfig(args []string) (demoConfig, error) {
	cfg := demoConfig{
		Rows:             256,
		Dims:             32,
		Degree:           16,
		TopK:             10,
		EfSearch:         128,
		MaxDecodedBlocks: 4,
	}
	fs := flag.NewFlagSet("treedb_column_graph_demo", flag.ContinueOnError)
	fs.StringVar(&cfg.Dir, "dir", cfg.Dir, "TreeDB directory; empty uses a temporary directory")
	fs.BoolVar(&cfg.Reset, "reset", cfg.Reset, "delete -dir before loading")
	fs.IntVar(&cfg.Rows, "rows", cfg.Rows, "rows to load from the selected dataset")
	fs.IntVar(&cfg.Dims, "dims", cfg.Dims, "synthetic vector dimensions; ignored for GloVe input after inference")
	fs.IntVar(&cfg.Degree, "degree", cfg.Degree, "graph out-degree used by RebuildVectorIndex")
	fs.IntVar(&cfg.TopK, "top-k", cfg.TopK, "number of nearest results")
	fs.IntVar(&cfg.EfSearch, "ef-search", cfg.EfSearch, "graph search exploration bound")
	fs.IntVar(&cfg.MaxDecodedBlocks, "max-decoded-blocks", cfg.MaxDecodedBlocks, "bounded physical column decoded-block cache size")
	fs.BoolVar(&cfg.IncludeDocs, "include-docs", cfg.IncludeDocs, "materialize projected documents after top-k, excluding the embedding field by default")
	fs.BoolVar(&cfg.IncludeDocEmbedding, "include-doc-embedding", cfg.IncludeDocEmbedding, "with -include-docs, return full documents including the embedding field (explicit embedding echo/full-doc opt-in)")
	fs.StringVar(&cfg.GlovePath, "glove", cfg.GlovePath, "optional GloVe text file to load as a real public embedding dataset")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	return cfg, nil
}

func demoVectorSearchOptions(cfg demoConfig, query []float32) (collections.VectorIndexSearcherSearchOptions, string, error) {
	opts := collections.VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     cfg.TopK,
		EfSearch: cfg.EfSearch,
	}
	if !cfg.IncludeDocs {
		return opts, "none", nil
	}
	if cfg.IncludeDocEmbedding {
		opts.IncludeDocuments = true
		return opts, "full_document_embedding_echo", nil
	}
	preset, err := collections.ProjectionOrientedVectorDocumentFetchPresetForField("embedding")
	if err != nil {
		return opts, "", err
	}
	preset.ApplyToSearcherSearchOptions(&opts)
	return opts, "exclude_embedding", nil
}

func demoCollectionMeta(dims, degree int) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: "docs",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Columns: []collections.ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
					{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Dictionary: true},
					{Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Dictionary: true},
					{Name: "label", Path: "label", ValueType: collections.ColumnStoreValueString, Dictionary: true, Nullable: true},
					{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: dims},
				},
				SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:       defaultIndexName,
			Field:      "embedding",
			Metric:     collections.VectorMetricCosine,
			Dimensions: dims,
			M:          degree,
			EfSearch:   128,
			Encoding:   collections.VectorIndexEncodingFloat32,
			Strategy:   collections.VectorIndexStrategyColumnGraph,
		}},
	}
}

func loadDemoRows(cfg demoConfig) ([]demoRow, int, error) {
	if cfg.GlovePath != "" {
		return loadGloveRows(cfg.GlovePath, cfg.Rows)
	}
	if cfg.Dims <= 0 {
		return nil, 0, errors.New("dims must be positive")
	}
	rows := make([]demoRow, cfg.Rows)
	for i := range rows {
		vector := make([]float32, cfg.Dims)
		for j := range vector {
			vector[j] = float32(((i+3)*(j+5))%23+1) / 23
		}
		rows[i] = demoRow{
			ID:     fmt.Sprintf("doc-%06d", i),
			Label:  fmt.Sprintf("synthetic-%06d", i),
			Vector: vector,
		}
	}
	return rows, cfg.Dims, nil
}

func loadGloveRows(path string, limit int) ([]demoRow, int, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	rows := make([]demoRow, 0, limit)
	dims := 0
	for scanner.Scan() {
		if len(rows) >= limit {
			break
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		if dims == 0 {
			dims = len(fields) - 1
		}
		if len(fields)-1 != dims {
			return nil, 0, fmt.Errorf("GloVe row %q has %d dims, want %d", fields[0], len(fields)-1, dims)
		}
		vector := make([]float32, dims)
		for i := 0; i < dims; i++ {
			value, err := strconv.ParseFloat(fields[i+1], 32)
			if err != nil {
				return nil, 0, fmt.Errorf("GloVe row %q dim %d parse: %w", fields[0], i, err)
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, 0, fmt.Errorf("GloVe row %q dim %d has non-finite value %g", fields[0], i, value)
			}
			vector[i] = float32(value)
		}
		rows = append(rows, demoRow{
			ID:     fmt.Sprintf("glove-%06d", len(rows)),
			Label:  fields[0],
			Vector: vector,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return rows, dims, nil
}

func insertDemoRows(col *collections.Collection, rows []demoRow) error {
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		raw, err := json.Marshal(map[string]any{
			"time_us":   int64(i + 1),
			"kind":      "vector",
			"did":       row.ID,
			"label":     row.Label,
			"embedding": row.Vector,
		})
		if err != nil {
			return err
		}
		ids[i] = []byte(row.ID)
		docs[i] = raw
	}
	_, err := col.InsertBatch(ids, docs)
	return err
}
