package documentservice

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestHTTPMalformedJSONKeywordHybridAndErrorPayloads(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	req := httptest.NewRequest(http.MethodPost, "/v1/indexes", bytes.NewBufferString(`{"name":"docs","dimension":2`))
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

	req = httptest.NewRequest(http.MethodPost, "/v1/indexes/docs/search/keyword", bytes.NewBufferString(`{"query":"refund","top_k":1,"filter":{"field":"meta.repo","operator":"==","value":"gomap"}}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("keyword filter status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeUnsupported)
	req = httptest.NewRequest(http.MethodPost, "/v1/indexes/docs/search/hybrid", bytes.NewBufferString(`{"query":"refund","top_k":1,"filter":{"field":"meta.repo","operator":"==","value":"gomap"}}`))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("hybrid filter status=%d body=%s", rr.Code, rr.Body.String())
	}
	assertHTTPErrorCode(t, rr.Body.Bytes(), CodeUnsupported)
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

	postJSON(t, handler, "/v1/indexes/bench_b64/search/vector-index", map[string]any{
		"query_embedding":            []float32{1, 0},
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
}

func encodeFloat32LEBase64ForTest(values []float32) string {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func postJSON(t *testing.T, handler http.Handler, path string, body any, wantStatus int, out any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
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
