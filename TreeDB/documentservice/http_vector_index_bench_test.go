package documentservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

var benchmarkVectorDecodeSink BenchmarkVectorSearchRequest
var benchmarkVectorBytesSink []byte
var benchmarkVectorResponseSink BenchmarkVectorSearchResponse

func BenchmarkBenchmarkVectorSearchDecode(b *testing.B) {
	query := benchmarkVectorQuery(1536)
	jsonPayload := benchmarkVectorSearchJSONPayload(query)
	b64Payload := benchmarkVectorSearchF32LEBase64Payload(query)
	rawPayload := encodeFloat32LERawForTest(query)
	encoded := encodeFloat32LEBase64ForTest(query)

	b.Run("json_float_array_request", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonPayload)))
		for i := 0; i < b.N; i++ {
			req, err := decodeBenchmarkVectorSearchJSONGeneric(jsonPayload)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVectorDecodeSink = req
		}
	})
	b.Run("f32_le_base64_request", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(b64Payload)))
		for i := 0; i < b.N; i++ {
			req, err := decodeBenchmarkVectorSearchJSONForBenchmark(b64Payload)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVectorDecodeSink = req
		}
	})
	b.Run("f32_le_base64_request_generic", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(b64Payload)))
		for i := 0; i < b.N; i++ {
			req, err := decodeBenchmarkVectorSearchJSONGenericForBenchmark(b64Payload)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVectorDecodeSink = req
		}
	})
	b.Run("f32_le_base64_raw", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(encoded)))
		for i := 0; i < b.N; i++ {
			req := BenchmarkVectorSearchRequest{QueryEmbeddingF32LEBase64: encoded}
			if err := normalizeBenchmarkVectorQueryEmbedding(&req); err != nil {
				b.Fatal(err)
			}
			benchmarkVectorDecodeSink = req
		}
	})
	b.Run("raw_f32le_request", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rawPayload)))
		for i := 0; i < b.N; i++ {
			query, err := decodeBenchmarkVectorQueryEmbeddingF32LERawWithLabel(rawPayload, "binary vector search request body")
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVectorDecodeSink = BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 10, EfSearch: 128, QueryMode: BenchmarkVectorQueryModeExact}
		}
	})
}

func BenchmarkBenchmarkVectorSearchHTTPPredecodedRequest(b *testing.B) {
	svc, db := newTestService(b)
	b.Cleanup(func() { _ = db.Close() })
	ctx := b.Context()
	index := "bench_http_predecoded"
	query := benchmarkVectorQuery(1536)
	createBenchmarkColumnGraphIndexWithDimension(b, svc, index, len(query))
	docs := make([]Document, 100)
	for i := range docs {
		embedding := append([]float32(nil), query...)
		embedding[0] += float32(i + 1)
		docs[i] = Document{ID: fmt.Sprintf("doc-%03d", i), Embedding: embedding}
	}
	loadBenchmarkDocsDeferred(b, svc, index, docs)
	if _, err := svc.OptimizeIndex(ctx, index, OptimizeIndexRequest{}); err != nil {
		b.Fatalf("OptimizeIndex: %v", err)
	}
	request := BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 100, EfSearch: 100}
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/indexes/bench_http_predecoded/search/vector-index", nil)

	for _, tc := range []struct {
		name   string
		format BenchmarkVectorResponseFormat
	}{{"full", BenchmarkVectorResponseFormatFull}, {"ids", BenchmarkVectorResponseFormatIDs}} {
		b.Run(tc.name, func(b *testing.B) {
			req := request
			req.ResponseFormat = tc.format
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rr := httptest.NewRecorder()
				response, err := svc.SearchBenchmarkVector(httpRequest.Context(), index, req)
				if err != nil {
					b.Fatal(err)
				}
				writeBenchmarkVectorSearchResponse(rr, response, tc.format)
				if rr.Code != http.StatusOK {
					b.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
				}
				benchmarkVectorResponseSink = response
			}
		})
	}
}

func BenchmarkBenchmarkVectorSearchHTTPHandler(b *testing.B) {
	svc, db := newTestService(b)
	b.Cleanup(func() { _ = db.Close() })
	ctx := b.Context()
	index := "bench_http_handler"
	query := benchmarkVectorQuery(1536)
	createBenchmarkColumnGraphIndexWithDimension(b, svc, index, len(query))
	loadBenchmarkDocsDeferred(b, svc, index, benchmarkVectorDocumentsForQuery(query))
	if _, err := svc.OptimizeIndex(ctx, index, OptimizeIndexRequest{}); err != nil {
		b.Fatalf("OptimizeIndex: %v", err)
	}
	handler := NewHandler(svc)
	b64Payload, err := json.Marshal(BenchmarkVectorSearchRequest{QueryEmbeddingF32LEBase64: encodeFloat32LEBase64ForTest(query), TopK: 1, EfSearch: 8})
	if err != nil {
		b.Fatal(err)
	}
	rawPayload := encodeFloat32LERawForTest(query)

	b.Run("json_f32_b64", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(b64Payload)))
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest(http.MethodPost, "/v1/indexes/bench_http_handler/search/vector-index", bytes.NewReader(b64Payload))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				b.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			benchmarkVectorBytesSink = rr.Body.Bytes()
		}
	})
	b.Run("raw_f32le_binary", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rawPayload)))
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest(http.MethodPost, "/v1/indexes/bench_http_handler/search/vector-index:binary?top_k=1&ef_search=8&query_mode=exact", bytes.NewReader(rawPayload))
			req.Header.Set("Content-Type", benchmarkVectorSearchBinaryContentType)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				b.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
			benchmarkVectorBytesSink = rr.Body.Bytes()
		}
	})
}

func BenchmarkBenchmarkVectorSearchResponseEncode(b *testing.B) {
	response := benchmarkVectorSearchResponseForEncode()
	ids := make([]string, len(response.Results))
	for i := range response.Results {
		ids[i] = response.Results[i].ID
	}
	compact := BenchmarkVectorSearchIDsResponse{ResponseFormat: BenchmarkVectorResponseFormatIDs, IDs: ids}
	benchEncode := func(b *testing.B, value any) {
		payload, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			payload, err := json.Marshal(value)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkVectorBytesSink = payload
		}
	}
	b.Run("full", func(b *testing.B) { benchEncode(b, response) })
	b.Run("ids", func(b *testing.B) { benchEncode(b, compact) })
}

func decodeBenchmarkVectorSearchJSONForBenchmark(raw []byte) (BenchmarkVectorSearchRequest, error) {
	return decodeBenchmarkVectorSearchJSON(raw)
}

func decodeBenchmarkVectorSearchJSONGenericForBenchmark(raw []byte) (BenchmarkVectorSearchRequest, error) {
	req, err := decodeBenchmarkVectorSearchJSONGeneric(raw)
	if err != nil {
		return BenchmarkVectorSearchRequest{}, err
	}
	if err := normalizeBenchmarkVectorQueryEmbedding(&req); err != nil {
		return BenchmarkVectorSearchRequest{}, err
	}
	return req, nil
}

func benchmarkVectorQuery(dim int) []float32 {
	query := make([]float32, dim)
	for i := range query {
		query[i] = float32(math.Sin(float64(i+1)) * 0.01)
	}
	return query
}

func benchmarkVectorDocumentsForQuery(query []float32) []Document {
	first := append([]float32(nil), query...)
	second := make([]float32, len(query))
	for i, value := range query {
		second[i] = -value
	}
	return []Document{
		{ID: "a", Content: "alpha", Embedding: first},
		{ID: "b", Content: "beta", Embedding: second},
	}
}

func createBenchmarkColumnGraphIndexWithDimension(t testing.TB, svc *Service, index string, dimension int) {
	t.Helper()
	if _, err := svc.ResetIndex(t.Context(), index, ResetIndexRequest{Dimension: dimension, Metric: MetricCosine, DropOld: true, VectorIndexOptions: benchmarkColumnGraphOptions()}); err != nil {
		t.Fatalf("ResetIndex %s create: %v", index, err)
	}
}

func benchmarkVectorSearchJSONPayload(query []float32) []byte {
	payload, err := json.Marshal(BenchmarkVectorSearchRequest{QueryEmbedding: query, TopK: 10, EfSearch: 128})
	if err != nil {
		panic(err)
	}
	return payload
}

func benchmarkVectorSearchF32LEBase64Payload(query []float32) []byte {
	payload, err := json.Marshal(BenchmarkVectorSearchRequest{QueryEmbeddingF32LEBase64: encodeFloat32LEBase64ForTest(query), TopK: 10, EfSearch: 128})
	if err != nil {
		panic(err)
	}
	return payload
}

func benchmarkVectorSearchResponseForEncode() BenchmarkVectorSearchResponse {
	results := make([]BenchmarkVectorSearchResult, 100)
	for i := range results {
		results[i] = BenchmarkVectorSearchResult{ID: fmt.Sprintf("doc-%06d", i), Ordinal: i + 1, Score: 1 - float64(i)*0.001}
	}
	return BenchmarkVectorSearchResponse{
		Index: IndexInfo{
			Name:            "bench",
			Dimension:       1536,
			Metric:          MetricCosine,
			VectorIndexName: defaultVectorIndexName,
			VectorStrategy:  collections.VectorIndexStrategyColumnGraph,
		},
		Results:         results,
		Metric:          MetricCosine,
		VectorIndexName: defaultVectorIndexName,
		QueryMode:       BenchmarkVectorQueryModeExact,
		NoDocuments:     true,
		Stats: collections.VectorIndexSearchStats{
			DocumentsFetched:          0,
			HNSWSearchPackCacheHits:   1,
			SearchRouteHNSWSearchPack: 1,
		},
		Diagnostics: collections.VectorIndexSearchDiagnostics{
			Route:                         collections.VectorIndexSearchRouteExactHNSWSearchPackV1,
			ExactHNSWSearchPackNoDocRoute: true,
			NoDocumentGuardrailsOK:        true,
			FallbackReason:                collections.VectorIndexSearchFallbackReasonNone,
		},
	}
}
