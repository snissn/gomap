package documentservice

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestHTTPMalformedJSONKeywordHybridAndErrorPayloads(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/indexes", bytes.NewBufferString(`{"name":"docs","dimension":2`))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeMalformedJSON)

	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "docs", Dimension: 2}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/docs/documents/upsert", UpsertDocumentsRequest{Documents: []Document{
		{ID: "shared", Content: "refund refund", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap"}},
		{ID: "vector", Content: "shipping", Embedding: []float32{0.99, 0.01}, Meta: map[string]any{"repo": "gomap"}},
	}}, http.StatusOK, nil)

	var keyword KeywordSearchResponse
	postJSON(t, handler, "/v1/indexes/docs/search/keyword", KeywordSearchRequest{Query: "refund", TopK: 1}, http.StatusOK, &keyword)
	if len(keyword.Documents) != 1 || keyword.Documents[0].ID != "shared" || keyword.Documents[0].Score == nil {
		t.Fatalf("keyword response=%+v", keyword)
	}

	var hybrid HybridSearchResponse
	postJSON(t, handler, "/v1/indexes/docs/search/hybrid", HybridSearchRequest{Query: "refund", QueryEmbedding: []float32{1, 0}, TopK: 2, EfSearch: 4}, http.StatusOK, &hybrid)
	if len(hybrid.Documents) == 0 || hybrid.Documents[0].ID != "shared" || hybrid.Documents[0].Score == nil {
		t.Fatalf("hybrid response=%+v", hybrid)
	}

	// v1alpha2: filters against undeclared meta fields keep failing closed,
	// with a typed invalid_request naming the missing scalar schema entry.
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/indexes/docs/search/keyword", bytes.NewBufferString(`{"query":"refund","top_k":1,"filter":{"field":"meta.repo","operator":"==","value":"gomap"}}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("undeclared keyword filter status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeInvalidRequest)
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/indexes/docs/search/hybrid", bytes.NewBufferString(`{"query":"refund","top_k":1,"filter":{"field":"meta.repo","operator":"==","value":"gomap"}}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("undeclared hybrid filter status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeInvalidRequest)
}

func TestHTTPDefaultMaxBodyBytesDoesNotMutateHandler(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := &Handler{Service: svc}

	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "docs", Dimension: 2}, http.StatusOK, nil)
	if handler.MaxBodyBytes != 0 {
		t.Fatalf("MaxBodyBytes mutated to %d, want zero", handler.MaxBodyBytes)
	}
}

func TestHTTPWriteJSONEncodeErrorKeepsErrorShape(t *testing.T) {
	rr := httptest.NewRecorder()
	writeJSON(rr, http.StatusOK, map[string]any{"bad": make(chan int)})

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeInternal)
}

func TestHTTPL2LargeFiniteEmbeddingsReturnFiniteScore(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "l2docs", Dimension: 1, Metric: "l2"}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/l2docs/documents/upsert", UpsertDocumentsRequest{Documents: []Document{{
		ID:        "large",
		Embedding: []float32{-3e38},
	}}}, http.StatusOK, nil)

	var search DenseVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/l2docs/search/vector", DenseVectorSearchRequest{QueryEmbedding: []float32{3e38}, TopK: 1}, http.StatusOK, &search)
	if len(search.Documents) != 1 || search.Documents[0].Score == nil {
		t.Fatalf("search=%+v", search)
	}
	score := *search.Documents[0].Score
	if math.IsInf(score, 0) || math.IsNaN(score) {
		t.Fatalf("score=%v, want finite", score)
	}
}

func TestHTTPDocumentVectorRoundTrip(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "docs", Dimension: 2}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/docs/documents/upsert", UpsertDocumentsRequest{Documents: []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}, Meta: map[string]any{"repo": "gomap"}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}, Meta: map[string]any{"repo": "other"}},
	}}, http.StatusOK, nil)
	var count CountDocumentsResponse
	postJSON(t, handler, "/v1/indexes/docs/documents/count", CountDocumentsRequest{Filter: &Filter{Field: "meta.repo", Operator: "==", Value: "gomap"}}, http.StatusOK, &count)
	if count.Count != 1 {
		t.Fatalf("count=%+v", count)
	}
	var search DenseVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/docs/search/vector", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, ReturnEmbedding: true}, http.StatusOK, &search)
	if len(search.Documents) != 1 || search.Documents[0].ID != "a" || len(search.Documents[0].Embedding) != 2 || search.Documents[0].Score == nil {
		t.Fatalf("search=%+v", search)
	}
}

func TestHTTPBenchmarkLifecycleRoutesAndExactVectorShape(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	var reset ResetIndexResponse
	postJSON(t, handler, "/v1/indexes/bench/reset", ResetIndexRequest{
		Dimension: 2,
		DropOld:   true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedIndexInfo{{
				Name:  "embedding.scalar_u8.fast",
				Codec: collections.QuantizedVectorCodecScalarU8,
			}},
		},
	}, http.StatusOK, &reset)
	if !reset.Index.Capabilities.BenchmarkLifecycle || !reset.Index.Capabilities.NoDocumentVectorSearch || !reset.Index.Capabilities.ScalarU8QuantizedRerank {
		t.Fatalf("reset capabilities=%+v", reset.Index.Capabilities)
	}
	postJSON(t, handler, "/v1/indexes/bench/documents/upsert", UpsertDocumentsRequest{Documents: []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	}}, http.StatusOK, nil)
	var optimize OptimizeIndexResponse
	postJSON(t, handler, "/v1/indexes/bench/optimize", OptimizeIndexRequest{}, http.StatusOK, &optimize)
	if !optimize.Status.Loaded || optimize.VectorIndexName != defaultVectorIndexName {
		t.Fatalf("optimize=%+v", optimize)
	}
	var benchmark BenchmarkVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/bench/search/vector-index", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8}, http.StatusOK, &benchmark)
	if !benchmark.NoDocuments || len(benchmark.Results) != 1 || benchmark.Results[0].ID != "a" || benchmark.Stats.DocumentsFetched != 0 {
		t.Fatalf("benchmark vector response=%+v stats=%+v", benchmark, benchmark.Stats)
	}

	var exactShape map[string]any
	postJSON(t, handler, "/v1/indexes/bench/search/vector", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1}, http.StatusOK, &exactShape)
	for _, unexpected := range []string{"results", "stats", "diagnostics", "no_documents", "query_mode"} {
		if _, ok := exactShape[unexpected]; ok {
			t.Fatalf("exact /search/vector included benchmark field %q in shape=%+v", unexpected, exactShape)
		}
	}
	for _, required := range []string{"index", "documents", "metric", "exact", "candidates"} {
		if _, ok := exactShape[required]; !ok {
			t.Fatalf("exact /search/vector missing field %q in shape=%+v", required, exactShape)
		}
	}
}

func TestHTTPBenchmarkNativeRuntimeLiveRoute(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	postJSON(t, handler, "/v1/indexes/native/reset", ResetIndexRequest{
		Dimension:          2,
		DropOld:            true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime},
	}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/native/documents/upsert", UpsertDocumentsRequest{
		Documents:               []Document{{ID: "a", Embedding: []float32{1, 0}}},
		DeferVectorIndexRebuild: true,
	}, http.StatusOK, nil)

	var response BenchmarkVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/native/search/vector-index", BenchmarkVectorSearchRequest{
		QueryEmbedding: []float32{1, 0},
		TopK:           1,
		EfSearch:       8,
		StatsMode:      collections.VectorIndexSearchStatsModeProduction,
	}, http.StatusOK, &response)
	if !response.NoDocuments || len(response.Results) != 1 || response.Results[0].ID != "a" || response.Diagnostics.Route != collections.VectorIndexSearchRouteNativeRuntime || !response.Diagnostics.LiveANN.Enabled || response.Diagnostics.LiveANN.ExactFallbacks != 0 || response.Diagnostics.LiveANN.FullRebuilds != 0 {
		t.Fatalf("native benchmark response=%+v", response)
	}
	liveJSON, err := json.Marshal(response.Diagnostics.LiveANN)
	if err != nil || !bytes.Contains(liveJSON, []byte(`"exact_fallbacks":0`)) {
		t.Fatalf("native live diagnostics JSON=%s err=%v want exact_fallbacks wire field", liveJSON, err)
	}
}

func TestHTTPBenchmarkVectorSearchCompactIDsParityTopK100(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	const index = "compact_ids"
	const quantized = "embedding.scalar_u8.fast"
	postJSON(t, handler, "/v1/indexes/"+index+"/reset", ResetIndexRequest{
		Dimension:          2,
		DropOld:            true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, QuantizedIndexes: []QuantizedIndexInfo{{Name: quantized, Codec: collections.QuantizedVectorCodecScalarU8}}},
	}, http.StatusOK, nil)
	docs := make([]Document, 100)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("doc-%03d", i), Embedding: []float32{float32(100 - i), float32(i + 1)}}
	}
	postJSON(t, handler, "/v1/indexes/"+index+"/documents/upsert", UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/"+index+"/optimize", OptimizeIndexRequest{}, http.StatusOK, nil)

	for _, tc := range []BenchmarkVectorSearchRequest{
		{QueryEmbedding: []float32{1, 0}, TopK: 100, EfSearch: 100},
		{QueryEmbedding: []float32{1, 0}, TopK: 100, EfSearch: 100, QueryMode: BenchmarkVectorQueryModeQuantizedOnly, QuantizedIndexName: quantized},
		{QueryEmbedding: []float32{1, 0}, TopK: 100, EfSearch: 100, QueryMode: BenchmarkVectorQueryModeQuantizedRerank, QuantizedIndexName: quantized, QuantizedRerankCandidates: 100},
	} {
		var full BenchmarkVectorSearchResponse
		postJSON(t, handler, "/v1/indexes/"+index+"/search/vector-index", tc, http.StatusOK, &full)
		compactRequest := tc
		compactRequest.ResponseFormat = BenchmarkVectorResponseFormatIDs
		var compact BenchmarkVectorSearchIDsResponse
		postJSON(t, handler, "/v1/indexes/"+index+"/search/vector-index", compactRequest, http.StatusOK, &compact)
		ids := make([]string, len(full.Results))
		for i := range full.Results {
			ids[i] = full.Results[i].ID
		}
		if compact.ResponseFormat != BenchmarkVectorResponseFormatIDs || !slices.Equal(compact.IDs, ids) {
			t.Fatalf("mode=%q compact=%+v full IDs=%v", tc.QueryMode, compact, ids)
		}
		base64Request := tc
		base64Request.QueryEmbedding = nil
		base64Request.QueryEmbeddingF32LEBase64 = encodeFloat32LEBase64ForTest([]float32{1, 0})
		var base64Response BenchmarkVectorSearchResponse
		postJSON(t, handler, "/v1/indexes/"+index+"/search/vector-index", base64Request, http.StatusOK, &base64Response)
		base64IDs := make([]string, len(base64Response.Results))
		for i := range base64Response.Results {
			base64IDs[i] = base64Response.Results[i].ID
		}
		if !slices.Equal(base64IDs, ids) {
			t.Fatalf("mode=%q base64 IDs=%v full IDs=%v", tc.QueryMode, base64IDs, ids)
		}
		mode := tc.QueryMode
		if mode == "" {
			mode = BenchmarkVectorQueryModeExact
		}
		binaryQuery := url.Values{
			"top_k":           {"100"},
			"ef_search":       {"100"},
			"query_mode":      {string(mode)},
			"stats_mode":      {"production"},
			"response_format": {"ids"},
		}
		if tc.QuantizedIndexName != "" {
			binaryQuery.Set("quantized_index_name", tc.QuantizedIndexName)
		}
		if tc.QuantizedRerankCandidates != 0 {
			binaryQuery.Set("quantized_rerank_candidates", strconv.Itoa(tc.QuantizedRerankCandidates))
		}
		var binaryCompact BenchmarkVectorSearchIDsResponse
		postBinaryVectorSearch(t, handler, "/v1/indexes/"+index+"/search/vector-index:binary?"+binaryQuery.Encode(), encodeFloat32LERawForTest([]float32{1, 0}), benchmarkVectorSearchBinaryContentType, http.StatusOK, &binaryCompact)
		if binaryCompact.ResponseFormat != BenchmarkVectorResponseFormatIDs || !slices.Equal(binaryCompact.IDs, ids) {
			t.Fatalf("mode=%q binary compact=%+v full IDs=%v", tc.QueryMode, binaryCompact, ids)
		}
		fullBody, err := json.Marshal(full)
		if err != nil {
			t.Fatal(err)
		}
		compactBody, err := json.Marshal(compact)
		if err != nil {
			t.Fatal(err)
		}
		if len(compactBody)*10 > len(fullBody)*3 {
			t.Fatalf("mode=%q compact bytes=%d full bytes=%d: want compact at least 70%% smaller", tc.QueryMode, len(compactBody), len(fullBody))
		}
	}
	productionQuery := url.Values{
		"top_k":                {"100"},
		"ef_search":            {"100"},
		"query_mode":           {string(BenchmarkVectorQueryModeQuantizedOnly)},
		"quantized_index_name": {quantized},
		"stats_mode":           {"production"},
	}
	var production BenchmarkVectorSearchResponse
	postBinaryVectorSearch(t, handler, "/v1/indexes/"+index+"/search/vector-index:binary?"+productionQuery.Encode(), encodeFloat32LERawForTest([]float32{1, 0}), benchmarkVectorSearchBinaryContentType, http.StatusOK, &production)
	if production.Stats.Candidates != 0 || production.Stats.Edges != 0 || production.Stats.VisitedEdges != 0 {
		t.Fatalf("raw production stats=%+v want no traversal diagnostics", production.Stats)
	}
	productionQuery.Set("stats_mode", "full_diagnostics")
	var fullDiagnostics BenchmarkVectorSearchResponse
	postBinaryVectorSearch(t, handler, "/v1/indexes/"+index+"/search/vector-index:binary?"+productionQuery.Encode(), encodeFloat32LERawForTest([]float32{1, 0}), benchmarkVectorSearchBinaryContentType, http.StatusOK, &fullDiagnostics)
	if fullDiagnostics.Stats.Candidates == 0 || fullDiagnostics.Stats.Edges == 0 || fullDiagnostics.Stats.VisitedEdges == 0 {
		t.Fatalf("raw full diagnostics stats=%+v want traversal diagnostics", fullDiagnostics.Stats)
	}

	postJSON(t, handler, "/v1/indexes/"+index+"/search/vector-index", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, ResponseFormat: BenchmarkVectorResponseFormat("unknown")}, http.StatusBadRequest, nil)
}

func TestHTTPBenchmarkVectorSearchAcceptsF32LEBase64Embedding(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	postJSON(t, handler, "/v1/indexes/bench_b64/reset", ResetIndexRequest{
		Dimension: 2,
		DropOld:   true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
		},
	}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/bench_b64/documents/upsert", UpsertDocumentsRequest{Documents: []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	}, DeferVectorIndexRebuild: true}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/bench_b64/optimize", OptimizeIndexRequest{}, http.StatusOK, nil)

	var benchmark BenchmarkVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0}),
		"top_k":                      1,
		"ef_search":                  8,
	}, http.StatusOK, &benchmark)
	if !benchmark.NoDocuments || len(benchmark.Results) != 1 || benchmark.Results[0].ID != "a" || benchmark.Stats.DocumentsFetched != 0 {
		t.Fatalf("benchmark vector response=%+v stats=%+v", benchmark, benchmark.Stats)
	}
	var compact BenchmarkVectorSearchIDsResponse
	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0}),
		"top_k":                      1,
		"ef_search":                  8,
		"response_format":            "ids",
	}, http.StatusOK, &compact)
	if compact.ResponseFormat != BenchmarkVectorResponseFormatIDs || !slices.Equal(compact.IDs, []string{"a"}) {
		t.Fatalf("compact base64 response=%+v", compact)
	}

	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding":            []float32{1, 0},
		"query_embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0}),
		"top_k":                      1,
	}, http.StatusBadRequest, nil)
	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding":            []float32{},
		"query_embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0}),
		"top_k":                      1,
	}, http.StatusBadRequest, nil)
	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding_f32_le_b64": "not-base64",
		"top_k":                      1,
	}, http.StatusBadRequest, nil)
	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding_f32_le_b64": base64.StdEncoding.EncodeToString([]byte{1, 2}),
		"top_k":                      1,
	}, http.StatusBadRequest, nil)

	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "unknown field", body: `{"query_embedding_f32_le_b64":"` + encodeFloat32LEBase64ForTest([]float32{1, 0}) + `","top_k":1,"unexpected":true}`},
		{name: "multiple JSON values", body: `{"query_embedding_f32_le_b64":"` + encodeFloat32LEBase64ForTest([]float32{1, 0}) + `","top_k":1} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/indexes/bench_b64/search/vector-index", bytes.NewBufferString(tc.body))
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			assertHTTPErrorCode(t, rr.Body.Bytes(), CodeMalformedJSON)
		})
	}
}

func TestHTTPBenchmarkVectorSearchBinaryF32LE(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	postJSON(t, handler, "/v1/indexes/bench_binary/reset", ResetIndexRequest{
		Dimension: 2,
		DropOld:   true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph,
		},
	}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/bench_binary/documents/upsert", UpsertDocumentsRequest{Documents: []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	}, DeferVectorIndexRebuild: true}, http.StatusOK, nil)
	postJSON(t, handler, "/v1/indexes/bench_binary/optimize", OptimizeIndexRequest{}, http.StatusOK, nil)

	var jsonResponse BenchmarkVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/bench_binary/search/vector-index", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8}, http.StatusOK, &jsonResponse)
	assertExactBenchmarkNoDocumentRoute(t, jsonResponse)

	rawQuery := encodeFloat32LERawForTest([]float32{1, 0})
	var binaryResponse BenchmarkVectorSearchResponse
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact&vector_index_name=embedding", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusOK, &binaryResponse)
	assertExactBenchmarkNoDocumentRoute(t, binaryResponse)
	if len(binaryResponse.Results) != 1 || len(jsonResponse.Results) != 1 || binaryResponse.Results[0].ID != jsonResponse.Results[0].ID || binaryResponse.Results[0].Score != jsonResponse.Results[0].Score {
		t.Fatalf("binary response results=%+v, json results=%+v", binaryResponse.Results, jsonResponse.Results)
	}
	var binaryOptionsResponse BenchmarkVectorSearchResponse
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact&expected_generation="+strconv.FormatUint(jsonResponse.Index.Generation, 10)+"&stats_mode=full_diagnostics", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusOK, &binaryOptionsResponse)
	assertExactBenchmarkNoDocumentRoute(t, binaryOptionsResponse)

	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", rawQuery, "application/octet-stream", http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?query_mode=exact", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&top_k=2&query_mode=exact", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", []byte{1, 2, 3}, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", encodeFloat32LERawForTest([]float32{1, 0, 0}), benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=unknown", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=quantized_only", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=quantized_rerank", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=quantized_rerank&quantized_index_name=embedding.scalar_u8.fast&quantized_rerank_candidates=-1", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact&quantized_index_name=embedding.scalar_u8.fast", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact&stats_mode=unsupported", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact&response_format=unknown", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearchWithRawQuery(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary", "top_k=1&ef_search=%zz", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
	postBinaryVectorSearch(t, handler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", nil, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)

	smallBodyHandler := NewHandler(svc)
	smallBodyHandler.MaxBodyBytes = int64(len(rawQuery) - 1)
	postBinaryVectorSearch(t, smallBodyHandler, "/v1/indexes/bench_binary/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", rawQuery, benchmarkVectorSearchBinaryContentType, http.StatusBadRequest, nil)
}

func assertExactBenchmarkNoDocumentRoute(t *testing.T, response BenchmarkVectorSearchResponse) {
	t.Helper()
	if !response.NoDocuments || response.Stats.DocumentsFetched != 0 || response.Diagnostics.Route != collections.VectorIndexSearchRouteExactHNSWSearchPackV1 || response.Diagnostics.FallbackReason != collections.VectorIndexSearchFallbackReasonNone || !response.Diagnostics.ExactHNSWSearchPackNoDocRoute || !response.Diagnostics.NoDocumentGuardrailsOK {
		t.Fatalf("benchmark vector response left exact no-document route: no_documents=%v stats=%+v diagnostics=%+v", response.NoDocuments, response.Stats, response.Diagnostics)
	}
}

func encodeFloat32LEBase64ForTest(values []float32) string {
	return base64.StdEncoding.EncodeToString(encodeFloat32LERawForTest(values))
}

func encodeFloat32LERawForTest(values []float32) []byte {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return raw
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, wantStatus int, out any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(raw))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != wantStatus {
		t.Fatalf("POST %s status=%d want %d body=%s", path, rr.Code, wantStatus, rr.Body.String())
	}
	if out != nil {
		if err := json.Unmarshal(rr.Body.Bytes(), out); err != nil {
			t.Fatalf("decode response: %v body=%s", err, rr.Body.String())
		}
	}
}

func postBinaryVectorSearch(t *testing.T, handler http.Handler, path string, body []byte, contentType string, wantStatus int, out any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(body))
	serveBinaryVectorSearchRequest(t, handler, req, path, contentType, wantStatus, out)
}

func postBinaryVectorSearchWithRawQuery(t *testing.T, handler http.Handler, path, rawQuery string, body []byte, contentType string, wantStatus int, out any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(body))
	req.URL.RawQuery = rawQuery
	serveBinaryVectorSearchRequest(t, handler, req, path+"?"+rawQuery, contentType, wantStatus, out)
}

func serveBinaryVectorSearchRequest(t *testing.T, handler http.Handler, req *http.Request, label string, contentType string, wantStatus int, out any) {
	t.Helper()
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != wantStatus {
		t.Fatalf("POST %s status=%d want %d body=%s", label, rr.Code, wantStatus, rr.Body.String())
	}
	if out != nil {
		if err := json.Unmarshal(rr.Body.Bytes(), out); err != nil {
			t.Fatalf("decode response: %v body=%s", err, rr.Body.String())
		}
	}
}

func assertHTTPErrorCode(t *testing.T, raw []byte, want ErrorCode) {
	t.Helper()
	var decoded struct {
		Error Error `json:"error"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, raw)
	}
	if decoded.Error.Code != want {
		t.Fatalf("error code=%s want %s body=%s", decoded.Error.Code, want, raw)
	}
}
