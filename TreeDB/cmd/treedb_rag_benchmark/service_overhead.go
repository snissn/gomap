package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

// Service-level RAG parity rows (#4273): identical fixture, identical query
// set, and equivalent retrieval work through (a) direct TreeDB collection API
// calls and (b) the cmd/treedb-document-service HTTP contract. The delta is
// observational service overhead (per-query p50/p99), comparable with the
// engine-only rows in docs/benchmarks/treedb_rag_benchmark_baseline_*.
//
// Both lanes use the service-shaped collection schema so the only difference
// between the two paths is the HTTP/JSON layer itself.

const (
	svcOverheadSchema       = "treedb_rag_service_overhead/v1"
	svcOverheadCollection   = "svcparity"
	svcOverheadTenantIndex  = "meta_tenant"
	svcOverheadVectorIndex  = "embedding"
	svcOverheadTextField    = "content"
	svcOverheadEfSearch     = 64
	directTimingIterations  = 256
	serviceTimingIterations = 1
)

type serviceOverheadConfig struct {
	Docs           int
	Dims           int
	M              int
	TopK           int
	CandidateLimit int
	Reps           int
	Warmup         int
	Dir            string
}

type serviceOverheadRow struct {
	Lane       string  `json:"lane"`
	Path       string  `json:"path"`
	Queries    int     `json:"queries"`
	Reps       int     `json:"reps"`
	Samples    int     `json:"samples"`
	P50Millis  float64 `json:"p50_ms"`
	P99Millis  float64 `json:"p99_ms"`
	MeanMillis float64 `json:"mean_ms"`
	Hits       int     `json:"hits_last_query"`
}

type serviceOverheadReport struct {
	Schema         string               `json:"schema"`
	Docs           int                  `json:"docs"`
	Dims           int                  `json:"dims"`
	TopK           int                  `json:"top_k"`
	EfSearch       int                  `json:"ef_search"`
	CandidateLimit int                  `json:"candidate_limit"`
	Rows           []serviceOverheadRow `json:"rows"`
}

func runServiceOverheadBenchmark(cfg serviceOverheadConfig) (*serviceOverheadReport, error) {
	if cfg.Docs <= 0 || cfg.Reps <= 0 || cfg.TopK <= 0 {
		return nil, fmt.Errorf("docs, reps, and top_k must be positive")
	}
	corpus, _, err := buildRagCorpus(cfg.Docs, cfg.Dims)
	if err != nil {
		return nil, fmt.Errorf("build fixture: %w", err)
	}
	ctx := context.Background()

	// --- Direct collection-API lane -------------------------------------
	db, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(cfg.Dir, "direct"), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return nil, fmt.Errorf("open direct db: %w", err)
	}
	defer func() { _ = db.Close() }()
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(serviceOverheadCollectionMeta(cfg)); err != nil {
		return nil, fmt.Errorf("create direct collection: %w", err)
	}
	col, err := manager.OpenCollection(svcOverheadCollection)
	if err != nil {
		return nil, fmt.Errorf("open direct collection: %w", err)
	}
	if err := ingestServiceOverheadDocs(col, corpus.Chunks); err != nil {
		return nil, fmt.Errorf("ingest direct docs: %w", err)
	}
	if _, err := col.RebuildVectorIndex(svcOverheadVectorIndex); err != nil {
		return nil, fmt.Errorf("rebuild direct vector index: %w", err)
	}

	// --- HTTP service lane ----------------------------------------------
	serviceDB, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(cfg.Dir, "service"), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		return nil, fmt.Errorf("open service db: %w", err)
	}
	defer func() { _ = serviceDB.Close() }()
	service := documentservice.New(collections.NewCollectionManager(serviceDB))
	defer func() { _ = service.Close() }()
	info, err := service.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name:         svcOverheadCollection,
		Dimension:    cfg.Dims,
		ScalarFields: []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant"}},
	})
	if err != nil {
		return nil, fmt.Errorf("service create index: %w", err)
	}
	if !info.Capabilities.HybridMetadataFilters {
		return nil, fmt.Errorf("service index did not declare hybrid metadata filter capability")
	}
	documents := make([]documentservice.Document, len(corpus.Chunks))
	for i, ch := range corpus.Chunks {
		documents[i] = documentservice.Document{
			ID:        ch.ID,
			Content:   ch.Title + " " + ch.Body,
			Embedding: append([]float32(nil), ch.Embedding...),
			Meta:      map[string]any{"tenant": ch.Tenant, "region": ch.Region},
		}
	}
	if _, err := service.UpsertDocuments(ctx, svcOverheadCollection, documentservice.UpsertDocumentsRequest{
		Documents:               documents,
		DeferVectorIndexRebuild: true,
	}); err != nil {
		return nil, fmt.Errorf("service upsert: %w", err)
	}
	if _, err := service.OptimizeIndex(ctx, svcOverheadCollection, documentservice.OptimizeIndexRequest{}); err != nil {
		return nil, fmt.Errorf("service optimize: %w", err)
	}
	server := httptest.NewServer(documentservice.NewHandler(service))
	defer server.Close()
	client := server.Client()

	report := &serviceOverheadReport{
		Schema:         svcOverheadSchema,
		Docs:           cfg.Docs,
		Dims:           cfg.Dims,
		TopK:           cfg.TopK,
		EfSearch:       svcOverheadEfSearch,
		CandidateLimit: cfg.CandidateLimit,
	}

	filterTenant := ragTenantNarrow

	directHybridSamples, directHybridHits, err := timeServiceOverheadQueries(corpus, cfg, directTimingIterations, func(q ragQuery) (int, error) {
		resp, err := col.SearchHybrid(collections.HybridSearchOptions{
			TopK:             cfg.TopK,
			Text:             &collections.HybridTextQuery{IndexName: svcOverheadTextField, Query: q.Text, CandidateLimit: cfg.CandidateLimit},
			Vector:           &collections.HybridVectorQuery{IndexName: svcOverheadVectorIndex, Query: q.Embedding, CandidateLimit: cfg.CandidateLimit, EfSearch: svcOverheadEfSearch, QueryMode: collections.VectorIndexQueryModeExact},
			ScalarFilter:     &collections.HybridScalarFilter{IndexName: svcOverheadTenantIndex, Value: filterTenant},
			IncludeDocuments: true,
		})
		if err != nil {
			return 0, err
		}
		return len(resp.Results), nil
	})
	if err != nil {
		return nil, fmt.Errorf("direct filtered hybrid row: %w", err)
	}
	report.Rows = append(report.Rows, serviceOverheadRowFromSamples("filtered_hybrid", "direct_collection_api", corpus, cfg, directHybridSamples, directHybridHits))

	serviceHybridSamples, serviceHybridHits, err := timeServiceOverheadQueries(corpus, cfg, serviceTimingIterations, func(q ragQuery) (int, error) {
		body := map[string]any{
			"query":                  q.Text,
			"query_embedding":        q.Embedding,
			"top_k":                  cfg.TopK,
			"text_candidate_limit":   cfg.CandidateLimit,
			"vector_candidate_limit": cfg.CandidateLimit,
			"ef_search":              svcOverheadEfSearch,
			"filter":                 map[string]any{"field": "meta.tenant", "operator": "==", "value": filterTenant},
		}
		var parsed struct {
			Documents []json.RawMessage `json:"documents"`
		}
		if err := postServiceOverheadJSON(client, server.URL+"/v1/indexes/"+svcOverheadCollection+"/search/hybrid", body, &parsed); err != nil {
			return 0, err
		}
		return len(parsed.Documents), nil
	})
	if err != nil {
		return nil, fmt.Errorf("service filtered hybrid row: %w", err)
	}
	report.Rows = append(report.Rows, serviceOverheadRowFromSamples("filtered_hybrid", "http_service", corpus, cfg, serviceHybridSamples, serviceHybridHits))
	directAnnSamples, directAnnHits, err := timeServiceOverheadQueries(corpus, cfg, directTimingIterations, func(q ragQuery) (int, error) {
		resp, err := col.SearchVectorIndex(collections.VectorIndexSearchOptions{
			IndexName:            svcOverheadVectorIndex,
			Query:                q.Embedding,
			QueryMode:            collections.VectorIndexQueryModeExact,
			TopK:                 cfg.TopK,
			EfSearch:             svcOverheadEfSearch,
			IncludeDocuments:     true,
			DocumentFetchOptions: collections.DocumentFetchOptions{},
		})
		if err != nil {
			return 0, err
		}
		return len(resp.Results), nil
	})
	if err != nil {
		return nil, fmt.Errorf("direct ann dense row: %w", err)
	}
	report.Rows = append(report.Rows, serviceOverheadRowFromSamples("ann_dense", "direct_collection_api", corpus, cfg, directAnnSamples, directAnnHits))

	serviceAnnSamples, serviceAnnHits, err := timeServiceOverheadQueries(corpus, cfg, serviceTimingIterations, func(q ragQuery) (int, error) {
		body := map[string]any{
			"query_embedding": q.Embedding,
			"top_k":           cfg.TopK,
			"route":           "ann",
			"ef_search":       svcOverheadEfSearch,
		}
		var parsed struct {
			Documents []json.RawMessage `json:"documents"`
		}
		if err := postServiceOverheadJSON(client, server.URL+"/v1/indexes/"+svcOverheadCollection+"/search/vector", body, &parsed); err != nil {
			return 0, err
		}
		return len(parsed.Documents), nil
	})
	if err != nil {
		return nil, fmt.Errorf("service ann dense row: %w", err)
	}
	report.Rows = append(report.Rows, serviceOverheadRowFromSamples("ann_dense", "http_service", corpus, cfg, serviceAnnSamples, serviceAnnHits))

	for _, row := range report.Rows {
		if row.Hits <= 0 {
			return nil, fmt.Errorf("row %s/%s returned no results; refusing to publish empty-overhead rows", row.Lane, row.Path)
		}
	}
	return report, nil
}

func serviceOverheadCollectionMeta(cfg serviceOverheadConfig) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: svcOverheadCollection,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:                 true,
				RetainedPayload:         collections.ColumnRetainedPayloadFull,
				RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
				Columns: []collections.ColumnStoreColumn{{
					Name:       svcOverheadVectorIndex,
					Path:       svcOverheadVectorIndex,
					Owner:      collections.TypedStorageOwnerColumnPart,
					ValueType:  collections.ColumnStoreValueFloat32Vector,
					VectorDims: cfg.Dims,
				}},
			},
		},
		Indexes: []collections.IndexDefinition{
			{Name: svcOverheadTenantIndex, Field: "meta.tenant", ValueType: collections.IndexValueString},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:       svcOverheadVectorIndex,
			Field:      svcOverheadVectorIndex,
			Metric:     collections.VectorMetricCosine,
			Dimensions: cfg.Dims,
			M:          cfg.M,
			Encoding:   collections.VectorIndexEncodingFloat32,
			Strategy:   collections.VectorIndexStrategyColumnGraph,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name:           svcOverheadTextField,
			Fields:         []collections.TextIndexField{{Field: svcOverheadTextField}},
			Analyzer:       collections.TextAnalyzerSimple,
			StorePositions: true,
		}},
	}
}

func ingestServiceOverheadDocs(col *collections.Collection, chunks []ragChunk) error {
	ids := make([][]byte, 0, len(chunks))
	docs := make([][]byte, 0, len(chunks))
	for _, ch := range chunks {
		raw, err := json.Marshal(map[string]any{
			"id":        ch.ID,
			"content":   ch.Title + " " + ch.Body,
			"embedding": ch.Embedding,
			"meta":      map[string]any{"tenant": ch.Tenant, "region": ch.Region},
		})
		if err != nil {
			return err
		}
		ids = append(ids, []byte(ch.ID))
		docs = append(docs, raw)
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		return err
	}
	return col.Flush()
}

// timeServiceOverheadQueries mirrors the C1 measurement boundary: warmup
// queries outside the timing window, then reps passes over the committed query
// set with per-query wall timing around the retrieval call only. Each measured
// sample batches a caller-selected bounded number of identical calls so fast
// direct collection queries remain above coarse Windows timer resolution; the
// reported duration is divided back to one query. HTTP service samples use one
// call because network/JSON work is already above that resolution. One final
// untimed call records the result count for the sanity gate.
func timeServiceOverheadQueries(corpus *ragCorpus, cfg serviceOverheadConfig, timingIterations int, call func(ragQuery) (int, error)) ([]float64, int, error) {
	for i := range cfg.Warmup {
		q := corpus.Queries[i%len(corpus.Queries)]
		if _, err := call(q); err != nil {
			return nil, 0, fmt.Errorf("warmup query %s: %w", q.ID, err)
		}
	}
	samples := make([]float64, 0, cfg.Reps*len(corpus.Queries))
	maxHits := 0
	for range cfg.Reps {
		for _, q := range corpus.Queries {
			start := time.Now()
			hits := 0
			for range timingIterations {
				var err error
				hits, err = call(q)
				if err != nil {
					return nil, 0, fmt.Errorf("timed query %s: %w", q.ID, err)
				}
			}
			if hits > maxHits {
				maxHits = hits
			}
			millis := time.Since(start).Seconds() * 1000.0 / float64(timingIterations)
			samples = append(samples, millis)
		}
	}
	return samples, maxHits, nil
}

func serviceOverheadRowFromSamples(lane string, path string, corpus *ragCorpus, cfg serviceOverheadConfig, samples []float64, hits int) serviceOverheadRow {
	p50, _ := percentile(samples, 50)
	p99, _ := percentile(samples, 99)
	mean := 0.0
	for _, sample := range samples {
		mean += sample
	}
	if len(samples) > 0 {
		mean /= float64(len(samples))
	}
	return serviceOverheadRow{
		Lane:       lane,
		Path:       path,
		Queries:    len(corpus.Queries),
		Reps:       cfg.Reps,
		Samples:    len(samples),
		P50Millis:  p50,
		P99Millis:  p99,
		MeanMillis: mean,
		Hits:       hits,
	}
}

func postServiceOverheadJSON(client *http.Client, url string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(payload))
	}
	return json.Unmarshal(payload, out)
}
