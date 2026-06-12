package documentservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	encoded := encodeFloat32LEBase64ForTest(query)

	b.Run("json_float_array_request", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonPayload)))
		for i := 0; i < b.N; i++ {
			req, err := decodeBenchmarkVectorSearchJSONForBenchmark(jsonPayload)
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
			if err := normalizeBenchmarkVectorQueryEmbedding(&req); err != nil {
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
}

func BenchmarkBenchmarkVectorSearchHTTPPredecodedRequest(b *testing.B) {
	svc, db := newTestService(b)
	b.Cleanup(func() { _ = db.Close() })
	ctx := b.Context()
	index := "bench_http_predecoded"
	createBenchmarkColumnGraphIndex(b, svc, index)
	loadBenchmarkDocsDeferred(b, svc, index, []Document{
		{ID: "a", Content: "alpha", Embedding: []float32{1, 0}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 1}},
	})
	if _, err := svc.OptimizeIndex(ctx, index, OptimizeIndexRequest{}); err != nil {
		b.Fatalf("OptimizeIndex: %v", err)
	}
	request := BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8}
	httpRequest := httptest.NewRequest(http.MethodPost, "/v1/indexes/bench_http_predecoded/search/vector-index", nil)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		response, err := svc.SearchBenchmarkVector(httpRequest.Context(), index, request)
		if err != nil {
			b.Fatal(err)
		}
		writeJSON(rr, http.StatusOK, response)
		if rr.Code != http.StatusOK {
			b.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
		}
		benchmarkVectorResponseSink = response
	}
}

func BenchmarkBenchmarkVectorSearchResponseEncode(b *testing.B) {
	response := benchmarkVectorSearchResponseForEncode()
	payload, err := json.Marshal(response)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := json.Marshal(response)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkVectorBytesSink = payload
	}
}

func decodeBenchmarkVectorSearchJSONForBenchmark(raw []byte) (BenchmarkVectorSearchRequest, error) {
	var req BenchmarkVectorSearchRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return BenchmarkVectorSearchRequest{}, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values in request body")
		}
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
	results := make([]BenchmarkVectorSearchResult, 10)
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
