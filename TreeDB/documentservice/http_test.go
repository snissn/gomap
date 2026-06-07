package documentservice

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPMalformedJSONAndUnsupportedSearchFailClosed(t *testing.T) {
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

	if _, err := svc.CreateIndex(req.Context(), CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	for _, path := range []string{"/v1/indexes/docs/search/keyword", "/v1/indexes/docs/search/hybrid"} {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"query":"refund"}`))
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotImplemented {
			t.Fatalf("%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
		assertHTTPErrorCode(t, rr.Body.Bytes(), CodeUnsupported)
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
