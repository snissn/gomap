package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	collections "github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func ragClock() time.Time { return time.Now() }
func ragSince(t time.Time) float64 {
	return time.Since(t).Seconds()
}

// benchConfig pins one benchmark invocation.
type benchConfig struct {
	Docs           int
	Dims           int
	M              int
	EfSearch       int
	TopK           int
	CandidateLimit int
	Reps           int
	Warmup         int
	Dir            string
	KeepDir        bool
	BatchSize      int
}

const (
	ragCollectionName   = "docs"
	ragVectorIndexName  = "embedding_graph"
	ragTextField        = "body"
	ragTitleField       = "title"
	ragTextIndexName    = "lexical"
	ragTenantIndexName  = "tenant"
	ragRegionIndexName  = "region"
	ragEmbeddingField   = "embedding"
	ragDefaultBatchSize = 256
)

// rowKey identifies one benchmark row.
type rowKey struct {
	Route      string `json:"route"`       // text_only | vector_only | hybrid
	ResultMode string `json:"result_mode"` // score_only | fetch_topk
	Filter     string `json:"filter"`      // legacy hashing regression retains unfiltered only
}

var ragRoutes = []string{"text_only", "vector_only", "hybrid"}
var ragResultModes = []string{"score_only", "fetch_topk"}

var ragFilterCases = []struct {
	Name   string
	Tenant string
}{
	{"none_100pct", ""},
}

func filterTenant(name string) string {
	for _, fc := range ragFilterCases {
		if fc.Name == name {
			return fc.Tenant
		}
	}
	return ""
}

// rowResult carries quality, latency, and counter aggregates for one row.
type rowResult struct {
	rowKey
	TopK                int                `json:"top_k"`
	CandidateLimit      int                `json:"candidate_limit"`
	FilterSelectivityPc float64            `json:"filter_selectivity_pct"`
	Queries             int                `json:"queries"`
	Reps                int                `json:"reps"`
	RecallAt5           float64            `json:"recall_at_5"`
	RecallAt10          float64            `json:"recall_at_10"`
	MRRAt10             float64            `json:"mrr_at_10"`
	LatencyMSMean       float64            `json:"latency_ms_mean"`
	LatencyMSP50        float64            `json:"latency_ms_p50"`
	LatencyMSP99        float64            `json:"latency_ms_p99"`
	Counters            map[string]float64 `json:"counters"`
}

type ingestStats struct {
	EmbedSeconds             float64 `json:"embed_seconds"`
	EmbeddedChunksPerSec     float64 `json:"embedded_chunks_per_sec"`
	ChunkRowInsertSeconds    float64 `json:"chunk_row_insert_seconds"`
	GeneratedChunkRowsPerSec float64 `json:"generated_chunk_rows_per_sec"`
	IndexBuildSeconds        float64 `json:"index_build_seconds"`
	StorageBytes             int64   `json:"storage_bytes"`
	StorageBytesPerD         float64 `json:"storage_bytes_per_doc"`
	Docs                     int     `json:"docs"`
	Chunks                   int     `json:"chunks"`
	Dims                     int     `json:"dims"`
	VectorM                  int     `json:"vector_m"`
	VectorEfSearch           int     `json:"vector_ef_search"`
	TopK                     int     `json:"top_k"`
	CandidateLimit           int     `json:"candidate_limit"`
}

type runOutput struct {
	Corpus        *ragCorpus
	CorpusStats   corpusBuildStats
	Fingerprint   string
	Ingest        ingestStats
	Rows          []rowResult
	Dir           string
	KeptDir       bool
	SetupSeconds  float64
	SearchSeconds float64
}

// run executes the full benchmark: build fixture, ingest, index, run rows,
// validate the counter contract, and return the report payload.
func runBenchmark(cfg benchConfig) (*runOutput, error) {
	if cfg.Docs <= 0 {
		return nil, fmt.Errorf("fail-closed: zero/negative document corpus (%d docs) is not benchmarkable", cfg.Docs)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = ragDefaultBatchSize
	}
	if cfg.Reps <= 0 || cfg.Warmup < 0 {
		return nil, fmt.Errorf("fail-closed: need reps>0 (got %d) and warmup>=0 (got %d)", cfg.Reps, cfg.Warmup)
	}

	corpus, corpusStats, err := buildRagCorpus(cfg.Docs, cfg.Dims)
	if err != nil {
		return nil, fmt.Errorf("build corpus: %w", err)
	}

	dir := cfg.Dir
	if dir == "" {
		var cleanupErr error
		dir, cleanupErr = os.MkdirTemp("", "treedb_rag_benchmark_*")
		if cleanupErr != nil {
			return nil, fmt.Errorf("mktemp dir: %w", cleanupErr)
		}
	} else {
		if err := os.RemoveAll(dir); err != nil {
			return nil, fmt.Errorf("prepare dir: %w", err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("prepare dir: %w", err)
		}
	}
	out := &runOutput{
		Corpus:      corpus,
		CorpusStats: corpusStats,
		Fingerprint: corpusFingerprint(corpus),
		Dir:         dir,
		KeptDir:     cfg.KeepDir || cfg.Dir != "",
	}
	if !cfg.KeepDir && cfg.Dir == "" {
		defer func() { _ = os.RemoveAll(dir) }()
	}

	// ---- setup (excluded from query timing) ----
	setupStart := ragClock()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		return nil, fmt.Errorf("save format config: %w", err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()

	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(collectionMeta(cfg)); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}
	col, err := manager.OpenCollection(ragCollectionName)
	if err != nil {
		return nil, fmt.Errorf("open collection: %w", err)
	}

	// Ingest timing covers InsertBatch + Flush only; embedding time is
	// reported separately (see corpusStats.EmbedSeconds).
	ingestStart := ragClock()
	if err := ingestChunks(col, corpus.Chunks, cfg.BatchSize); err != nil {
		return nil, fmt.Errorf("ingest: %w", err)
	}
	ingestSeconds := ragSince(ingestStart)

	indexStart := ragClock()
	if _, err := col.RebuildVectorIndex(ragVectorIndexName); err != nil {
		return nil, fmt.Errorf("rebuild vector index: %w", err)
	}
	indexBuildSeconds := ragSince(indexStart)
	if err := db.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	setupSeconds := ragSince(setupStart)

	storageBytes, err := dirSize(dir)
	if err != nil {
		return nil, fmt.Errorf("measure storage: %w", err)
	}

	out.Ingest = ingestStats{
		EmbedSeconds:             corpusStats.EmbedSeconds,
		EmbeddedChunksPerSec:     float64(len(corpus.Chunks)) / corpusStats.EmbedSeconds,
		ChunkRowInsertSeconds:    ingestSeconds,
		GeneratedChunkRowsPerSec: float64(len(corpus.Chunks)) / ingestSeconds,
		IndexBuildSeconds:        indexBuildSeconds,
		StorageBytes:             storageBytes,
		StorageBytesPerD:         float64(storageBytes) / float64(len(corpus.Chunks)),
		Docs:                     corpus.Docs,
		Chunks:                   len(corpus.Chunks),
		Dims:                     cfg.Dims,
		VectorM:                  cfg.M,
		VectorEfSearch:           cfg.EfSearch,
		TopK:                     cfg.TopK,
		CandidateLimit:           cfg.CandidateLimit,
	}
	out.SetupSeconds = setupSeconds

	// ---- rows ----
	relevance := relevanceMap(corpus.GroundTruth)
	queryByID := make(map[string]ragQuery, len(corpus.Queries))
	for _, q := range corpus.Queries {
		queryByID[q.ID] = q
	}
	selectivity := map[string]float64{}
	for _, fc := range ragFilterCases {
		if fc.Tenant == "" {
			selectivity[fc.Name] = 100
			continue
		}
		var matched int
		for _, ch := range corpus.Chunks {
			if ch.Tenant == fc.Tenant {
				matched++
			}
		}
		selectivity[fc.Name] = float64(matched) * 100 / float64(len(corpus.Chunks))
	}

	searchStart := ragClock()
	for _, route := range ragRoutes {
		for _, mode := range ragResultModes {
			for _, fc := range ragFilterCases {
				row, err := runRow(cfg, col, corpus, route, mode, fc.Name, relevance)
				if err != nil {
					return nil, fmt.Errorf("row %s/%s/%s: %w", route, mode, fc.Name, err)
				}
				row.FilterSelectivityPc = selectivity[fc.Name]
				out.Rows = append(out.Rows, *row)
			}
		}
	}
	out.SearchSeconds = ragSince(searchStart)
	return out, nil
}

func collectionMeta(cfg benchConfig) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: ragCollectionName,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:                 true,
				RetainedPayload:         collections.ColumnRetainedPayloadFull,
				RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
				Columns: []collections.ColumnStoreColumn{{
					Name:       ragEmbeddingField,
					Path:       ragEmbeddingField,
					Owner:      collections.TypedStorageOwnerColumnPart,
					ValueType:  collections.ColumnStoreValueFloat32Vector,
					VectorDims: cfg.Dims,
				}},
			},
		},
		Indexes: []collections.IndexDefinition{
			{Name: ragTenantIndexName, Field: "tenant", ValueType: collections.IndexValueString},
			{Name: ragRegionIndexName, Field: "region", ValueType: collections.IndexValueString},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:       ragVectorIndexName,
			Field:      ragEmbeddingField,
			Metric:     collections.VectorMetricCosine,
			Dimensions: cfg.Dims,
			M:          cfg.M,
			Encoding:   collections.VectorIndexEncodingFloat32,
			Strategy:   collections.VectorIndexStrategyColumnGraph,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name: ragTextIndexName,
			Fields: []collections.TextIndexField{
				{Field: ragTitleField, Weight: 3},
				{Field: ragTextField},
			},
			StorePositions: true,
		}},
	}
}

func ingestChunks(col *collections.Collection, chunks []ragChunk, batchSize int) error {
	for start := 0; start < len(chunks); start += batchSize {
		end := start + batchSize
		if end > len(chunks) {
			end = len(chunks)
		}
		ids := make([][]byte, 0, end-start)
		docs := make([][]byte, 0, end-start)
		for _, ch := range chunks[start:end] {
			raw, err := json.Marshal(struct {
				Did       string    `json:"did"`
				ParentDoc string    `json:"parent_doc"`
				Topic     string    `json:"topic"`
				Title     string    `json:"title"`
				Body      string    `json:"body"`
				Tenant    string    `json:"tenant"`
				Region    string    `json:"region"`
				Embedding []float32 `json:"embedding"`
			}{
				Did: ch.ID, ParentDoc: ch.ParentDoc, Topic: ch.Topic,
				Title: ch.Title, Body: ch.Body, Tenant: ch.Tenant,
				Region: ch.Region, Embedding: ch.Embedding,
			})
			if err != nil {
				return err
			}
			ids = append(ids, []byte(ch.ID))
			docs = append(docs, raw)
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return fmt.Errorf("insert batch at %d: %w", start, err)
		}
	}
	return col.Flush()
}

// runRow executes one (route, result mode, filter) row: warmup queries outside
// the timing window, then reps passes over the committed query set with
// per-query wall timing around the SearchHybrid call only.
func runRow(cfg benchConfig, col *collections.Collection, corpus *ragCorpus, route, resultMode, filter string, relevance map[string]map[string]bool) (*rowResult, error) {
	row := &rowResult{
		rowKey:         rowKey{Route: route, ResultMode: resultMode, Filter: filter},
		TopK:           cfg.TopK,
		CandidateLimit: cfg.CandidateLimit,
		Queries:        len(corpus.Queries),
		Reps:           cfg.Reps,
		Counters:       map[string]float64{},
	}

	buildOpts := func(q ragQuery) collections.HybridSearchOptions {
		opts := collections.HybridSearchOptions{
			TopK: cfg.TopK,
		}
		if tenant := filterTenant(filter); tenant != "" {
			opts.ScalarFilter = &collections.HybridScalarFilter{IndexName: ragTenantIndexName, Value: tenant}
		}
		if route == "text_only" || route == "hybrid" {
			opts.Text = &collections.HybridTextQuery{
				IndexName:      ragTextIndexName,
				Query:          q.Text,
				CandidateLimit: cfg.CandidateLimit,
			}
		}
		if route == "vector_only" || route == "hybrid" {
			opts.Vector = &collections.HybridVectorQuery{
				IndexName:      ragVectorIndexName,
				Query:          q.Embedding,
				CandidateLimit: cfg.CandidateLimit,
				EfSearch:       cfg.EfSearch,
				QueryMode:      collections.VectorIndexQueryModeExact,
			}
		}
		if resultMode == "fetch_topk" {
			opts.ResultMode = collections.HybridResultModeFull
			opts.IncludeDocuments = true
			opts.DocumentFetchOptions = collections.DocumentFetchOptions{ExcludePaths: []string{ragEmbeddingField}}
		} else {
			opts.ResultMode = collections.HybridResultModeScoreOnly
		}
		return opts
	}

	// Warmup (untimed).
	for i := 0; i < cfg.Warmup; i++ {
		q := corpus.Queries[i%len(corpus.Queries)]
		if _, err := col.SearchHybrid(buildOpts(q)); err != nil {
			return nil, fmt.Errorf("warmup query %s: %w", q.ID, err)
		}
	}

	var latencies []float64
	for rep := 0; rep < cfg.Reps; rep++ {
		for _, q := range corpus.Queries {
			opts := buildOpts(q)
			start := ragClock()
			resp, err := col.SearchHybrid(opts)
			elapsed := ragSince(start)
			if err != nil {
				return nil, fmt.Errorf("query %s: %w", q.ID, err)
			}
			if resp.Stats.FailClosed != 0 || resp.Stats.FullDocumentScanFallbacks != 0 {
				return nil, fmt.Errorf("query %s: fail-closed stats=%+v", q.ID, resp.Stats)
			}
			latencies = append(latencies, elapsed*1000)

			rel := relevance[q.ID]
			ranked := make([]string, 0, len(resp.Results))
			for _, r := range resp.Results {
				ranked = append(ranked, string(r.ID))
			}
			if err := accumulateQuality(row, ranked, rel); err != nil {
				return nil, fmt.Errorf("query %s: %w", q.ID, err)
			}
			accumulateCounters(row.Counters, resp.Stats)
		}
	}
	samples := float64(cfg.Reps * len(corpus.Queries))
	row.RecallAt5 /= samples
	row.RecallAt10 /= samples
	row.MRRAt10 /= samples
	for k, v := range row.Counters {
		row.Counters[k] = v / samples
	}
	row.LatencyMSMean = mean(latencies)
	p50, err := percentile(latencies, 50)
	if err != nil {
		return nil, err
	}
	p99, err := percentile(latencies, 99)
	if err != nil {
		return nil, err
	}
	row.LatencyMSP50 = p50
	row.LatencyMSP99 = p99
	return row, nil
}

var ragCounterKeys = []struct {
	Key   string
	Value func(s collections.HybridSearchStats) uint64
}{
	{"documents_fetched", func(s collections.HybridSearchStats) uint64 { return s.DocumentsFetched }},
	{"documents_missing", func(s collections.HybridSearchStats) uint64 { return s.DocumentsMissing }},
	{"full_document_scan_fallbacks", func(s collections.HybridSearchStats) uint64 { return s.FullDocumentScanFallbacks }},
	{"fail_closed", func(s collections.HybridSearchStats) uint64 { return s.FailClosed }},
	{"text_candidates_returned", func(s collections.HybridSearchStats) uint64 { return s.TextCandidatesReturned }},
	{"text_postings_scanned", func(s collections.HybridSearchStats) uint64 { return s.TextPostingsScanned }},
	{"text_candidates_scored", func(s collections.HybridSearchStats) uint64 { return s.TextCandidatesScored }},
	{"vector_candidates_returned", func(s collections.HybridSearchStats) uint64 { return s.VectorCandidatesReturned }},
	{"vector_candidates_examined", func(s collections.HybridSearchStats) uint64 { return s.VectorCandidatesExamined }},
	{"scalar_prefilter_ids", func(s collections.HybridSearchStats) uint64 { return s.ScalarPrefilterIDs }},
	{"scalar_filter_matched", func(s collections.HybridSearchStats) uint64 { return s.ScalarFilterMatched }},
	{"scalar_filter_lookups", func(s collections.HybridSearchStats) uint64 { return s.ScalarFilterLookups }},
	{"scalar_filter_input_ids", func(s collections.HybridSearchStats) uint64 { return s.ScalarFilterInputIDs }},
	{"scalar_filter_intersection_steps", func(s collections.HybridSearchStats) uint64 { return s.ScalarFilterIntersectionSteps }},
	{"scalar_filter_final_ids", func(s collections.HybridSearchStats) uint64 { return s.ScalarFilterFinalIDs }},
	{"candidates_fused", func(s collections.HybridSearchStats) uint64 { return s.CandidatesFused }},
	{"candidates_after_fusion", func(s collections.HybridSearchStats) uint64 { return s.CandidatesAfterFusion }},
	{"fusion_text_only", func(s collections.HybridSearchStats) uint64 { return s.FusionTextOnly }},
	{"fusion_vector_only", func(s collections.HybridSearchStats) uint64 { return s.FusionVectorOnly }},
	{"fusion_both", func(s collections.HybridSearchStats) uint64 { return s.FusionBoth }},
	{"truncated", func(s collections.HybridSearchStats) uint64 { return s.Truncated }},
	{"collapse_rejections", func(s collections.HybridSearchStats) uint64 { return s.CollapseRejections }},
	{"collapse_exhaustions", func(s collections.HybridSearchStats) uint64 { return s.CollapseExhaustions }},
}

func accumulateCounters(dst map[string]float64, s collections.HybridSearchStats) {
	for _, k := range ragCounterKeys {
		dst[k.Key] += float64(k.Value(s))
	}
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var s float64
	for _, v := range values {
		s += v
	}
	return s / float64(len(values))
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
