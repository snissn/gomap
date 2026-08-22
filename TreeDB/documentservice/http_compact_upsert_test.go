package documentservice

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestHTTPUpsertAcceptsCompactF32LEEmbeddingAndPersistsOrdinaryDocument(t *testing.T) {
	dir := t.TempDir()
	db, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := New(collections.NewCollectionManager(db))
	handler := NewHandler(svc)
	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "docs", Dimension: 2}, http.StatusOK, nil)

	first := []float32{0.25, -0.5}
	var upsert map[string]any
	postJSON(t, handler, "/v1/indexes/docs/documents/upsert", map[string]any{
		"documents": []any{map[string]any{
			"id":                   "compact",
			"content":              "persistent",
			"embedding_f32_le_b64": encodeFloat32LEBase64ForTest(first),
			"meta":                 map[string]any{"route": "compact"},
		}},
	}, http.StatusOK, &upsert)
	if upsert["upserted"] != float64(1) || upsert["inserted"] != float64(1) || upsert["updated"] != float64(0) || upsert["compact_embeddings"] != float64(1) {
		t.Fatalf("upsert response=%+v", upsert)
	}
	assertHTTPCompactDocumentReads(t, handler, first)

	second := []float32{-0.5, 0.25}
	postJSON(t, handler, "/v1/indexes/docs/documents/upsert", map[string]any{
		"documents": []any{map[string]any{
			"id":                   "compact",
			"content":              "persistent updated",
			"embedding_f32_le_b64": encodeFloat32LEBase64ForTest(second),
		}},
	}, http.StatusOK, &upsert)
	if upsert["upserted"] != float64(1) || upsert["inserted"] != float64(0) || upsert["updated"] != float64(1) || upsert["compact_embeddings"] != float64(1) {
		t.Fatalf("update response=%+v", upsert)
	}
	assertHTTPCompactDocumentReads(t, handler, second)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(testBackendOptions(dir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	reopenedSvc := New(collections.NewCollectionManager(reopened))
	defer reopenedSvc.Close()
	assertHTTPCompactDocumentReads(t, NewHandler(reopenedSvc), second)
}

func TestHTTPUpsertCompactF32LEEmbeddingValidationAndNumericCompatibility(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	postJSON(t, handler, "/v1/indexes", CreateIndexRequest{Name: "docs", Dimension: 2}, http.StatusOK, nil)

	var numeric map[string]any
	postJSON(t, handler, "/v1/indexes/docs/documents/upsert", UpsertDocumentsRequest{Documents: []Document{{ID: "numeric", Embedding: []float32{1, 0}}}}, http.StatusOK, &numeric)
	if _, ok := numeric["compact_embeddings"]; ok {
		t.Fatalf("numeric response unexpectedly reported compact route: %+v", numeric)
	}

	tests := []struct {
		name      string
		document  map[string]any
		wantError string
	}{
		{name: "malformed base64", document: map[string]any{"id": "bad-base64", "embedding_f32_le_b64": "not-base64"}, wantError: "decode"},
		{name: "partial float", document: map[string]any{"id": "partial", "embedding_f32_le_b64": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})}, wantError: "multiple of 4"},
		{name: "dimension mismatch", document: map[string]any{"id": "dimension", "embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1})}, wantError: "dimension"},
		{name: "non finite", document: map[string]any{"id": "non-finite", "embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{float32(math.Inf(1)), 0})}, wantError: "not finite"},
		{name: "both", document: map[string]any{"id": "both", "embedding": []float32{1, 0}, "embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0})}, wantError: "either"},
		{name: "empty numeric and compact", document: map[string]any{"id": "empty-both", "embedding": []float32{}, "embedding_f32_le_b64": encodeFloat32LEBase64ForTest([]float32{1, 0})}, wantError: "either"},
		{name: "neither", document: map[string]any{"id": "neither"}, wantError: "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{"documents": []any{tc.document}})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/indexes/docs/documents/upsert", bytes.NewReader(raw))
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), tc.wantError) {
				t.Fatalf("status=%d body=%s, want error containing %q", rr.Code, rr.Body.String(), tc.wantError)
			}
			assertHTTPErrorCode(t, rr.Body.Bytes(), CodeInvalidRequest)
		})
	}

	var count CountDocumentsResponse
	postJSON(t, handler, "/v1/indexes/docs/documents/count", CountDocumentsRequest{}, http.StatusOK, &count)
	if count.Count != 1 {
		t.Fatalf("count=%d, validation failures wrote documents", count.Count)
	}
}

func assertHTTPCompactDocumentReads(t *testing.T, handler http.Handler, want []float32) {
	t.Helper()
	var filtered FilterDocumentsResponse
	postJSON(t, handler, "/v1/indexes/docs/documents/filter", FilterDocumentsRequest{ReturnEmbedding: true}, http.StatusOK, &filtered)
	if len(filtered.Documents) != 1 || filtered.Documents[0].ID != "compact" || !reflect.DeepEqual(filtered.Documents[0].Embedding, want) {
		t.Fatalf("filtered documents=%+v", filtered.Documents)
	}
	if raw, err := json.Marshal(filtered.Documents[0]); err != nil {
		t.Fatal(err)
	} else if strings.Contains(string(raw), "embedding_f32_le_b64") {
		t.Fatalf("transport-only embedding persisted or echoed: %s", raw)
	}

	var searched DenseVectorSearchResponse
	postJSON(t, handler, "/v1/indexes/docs/search/vector", DenseVectorSearchRequest{QueryEmbedding: want, TopK: 1, ReturnEmbedding: true}, http.StatusOK, &searched)
	if len(searched.Documents) != 1 || searched.Documents[0].ID != "compact" || !reflect.DeepEqual(searched.Documents[0].Embedding, want) {
		t.Fatalf("searched documents=%+v", searched.Documents)
	}
}
