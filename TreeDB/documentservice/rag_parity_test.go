package documentservice

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// C6 (#4273) RAG parity contract tests. These run at the HTTP wire level so the
// service request/response schema itself is what is under test: filtered
// keyword/hybrid retrieval, the dense route ann|exact selection, fail-closed
// scalar truncation, and declaration-time scalar field schema echo.

func ragParityPost(t *testing.T, handler *Handler, path string, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response body %q: %v", rr.Body.String(), err)
	}
	return rr.Code, payload
}

func ragParityError(t *testing.T, payload map[string]any) (string, string) {
	t.Helper()
	errObj, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("response is not an error envelope: %#v", payload)
	}
	code, _ := errObj["code"].(string)
	message, _ := errObj["message"].(string)
	return code, message
}

func ragParityCreateIndex(t *testing.T, handler *Handler, name string) {
	t.Helper()
	body := `{"name":"` + name + `","dimension":4,"scalar_fields":[{"field":"meta.tenant","value_type":"string"},{"field":"meta.priority","value_type":"int64"}]}`
	status, payload := ragParityPost(t, handler, "/v1/indexes", body)
	if status != http.StatusOK {
		t.Fatalf("create index status=%d body=%s", status, mustJSON(payload))
	}
	info := payload["index"].(map[string]any)
	if got := info["contract_version"]; got != ContractVersion {
		t.Fatalf("contract_version=%v want %s", got, ContractVersion)
	}
	fields := info["scalar_fields"].([]any)
	if len(fields) != 2 {
		t.Fatalf("scalar_fields=%#v want 2 declared entries", fields)
	}
	byField := map[string]map[string]any{}
	for _, raw := range fields {
		entry := raw.(map[string]any)
		byField[entry["field"].(string)] = entry
	}
	first := byField["meta.tenant"]
	if first == nil || first["index_name"] != "meta_tenant" || first["value_type"] != "string" {
		t.Fatalf("meta.tenant scalar entry missing: %#v", first)
	}
	if second := byField["meta.priority"]; second == nil || second["value_type"] != "int64" {
		t.Fatalf("meta.priority scalar entry missing: %#v", second)
	}
	caps := info["capabilities"].(map[string]any)
	if caps["keyword_metadata_filters"] != true || caps["hybrid_metadata_filters"] != true {
		t.Fatalf("capabilities=%#v want keyword/hybrid metadata filters declared", caps)
	}
}

func ragParityUpsertTenantDocs(t *testing.T, handler *Handler, index string, docs []Document) {
	t.Helper()
	req := UpsertDocumentsRequest{Documents: docs}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal upsert: %v", err)
	}
	status, payload := ragParityPost(t, handler, "/v1/indexes/"+index+"/documents/upsert", string(raw))
	if status != http.StatusOK {
		t.Fatalf("upsert status=%d body=%s", status, mustJSON(payload))
	}
}

func ragParityTenantDocs() []Document {
	return []Document{
		{ID: "t1-a", Content: "alpha refund policy", Embedding: []float32{1, 0, 0, 0}, Meta: map[string]any{"tenant": "t1", "priority": int64(1)}},
		{ID: "t1-b", Content: "beta refund timeline", Embedding: []float32{0.9, 0.1, 0, 0}, Meta: map[string]any{"tenant": "t1", "priority": int64(2)}},
		{ID: "t2-a", Content: "gamma refund policy", Embedding: []float32{0, 1, 0, 0}, Meta: map[string]any{"tenant": "t2", "priority": int64(3)}},
		{ID: "t2-b", Content: "delta shipping update", Embedding: []float32{0, 0, 1, 0}, Meta: map[string]any{"tenant": "t2", "priority": int64(4)}},
	}
}
func TestHTTPFilteredKeywordSearchReturnsOnlyMatchingTenantDocs(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())

	status, payload := ragParityPost(t, handler, "/v1/indexes/docs/search/keyword",
		`{"query":"refund","top_k":10,"filter":{"field":"meta.tenant","operator":"==","value":"t2"}}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, payload)
		t.Fatalf("filtered keyword search status=%d code=%s message=%q", status, code, message)
	}
	docs := payload["documents"].([]any)
	if len(docs) != 1 {
		t.Fatalf("filtered keyword documents=%s want only t2 refund doc", mustJSON(docs))
	}
	doc := docs[0].(map[string]any)
	if doc["id"] != "t2-a" {
		t.Fatalf("filtered keyword hit id=%v want t2-a", doc["id"])
	}
	stats := payload["stats"].(map[string]any)
	if stats["scalar_prefilter_ids"].(float64) < 1 {
		t.Fatalf("stats=%s want bounded scalar allow-set counters", mustJSON(stats))
	}
}

func TestHTTPFilteredHybridSearchReturnsOnlyMatchingTenantDocs(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())

	status, payload := ragParityPost(t, handler, "/v1/indexes/docs/search/hybrid",
		`{"query":"refund","query_embedding":[0.95,0.05,0,0],"top_k":10,"ef_search":16,"filter":{"field":"meta.tenant","operator":"==","value":"t1"}}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, payload)
		t.Fatalf("filtered hybrid search status=%d code=%s message=%q", status, code, message)
	}
	docs := payload["documents"].([]any)
	if len(docs) == 0 {
		t.Fatalf("filtered hybrid documents empty")
	}
	for _, raw := range docs {
		doc := raw.(map[string]any)
		meta := doc["meta"].(map[string]any)
		if meta["tenant"] != "t1" {
			t.Fatalf("filtered hybrid leaked document %v tenant=%v", doc["id"], meta["tenant"])
		}
	}
	plan := payload["plan"].(map[string]any)
	if plan["scalar_filter_strategy"] != "prefilter" {
		t.Fatalf("plan=%s want prefilter strategy", mustJSON(plan))
	}
}

func TestHTTPFilteredKeywordRangeFilterBoundedAllowSet(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())

	status, payload := ragParityPost(t, handler, "/v1/indexes/docs/search/keyword",
		`{"query":"refund","top_k":10,"filter":{"field":"meta.priority","operator":">=","value":3}}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, payload)
		t.Fatalf("range-filtered keyword status=%d code=%s message=%q", status, code, message)
	}
	docs := payload["documents"].([]any)
	if len(docs) != 1 || docs[0].(map[string]any)["id"] != "t2-a" {
		t.Fatalf("range-filtered documents=%s want t2-a only", mustJSON(docs))
	}
}

func TestHTTPFilteredSearchUnsupportedShapesFailClosedTyped(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())

	cases := []struct {
		name string
		path string
		body string
		want ErrorCode
	}{
		{
			name: "undeclared field",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"filter":{"field":"meta.repo","operator":"==","value":"x"}}`,
			want: CodeInvalidRequest,
		},
		{
			name: "multi-field AND cannot be one bounded allow-set",
			path: "/v1/indexes/docs/search/hybrid",
			body: `{"query":"refund","top_k":5,"filter":{"operator":"and","conditions":[{"field":"meta.tenant","operator":"==","value":"t1"},{"field":"meta.priority","operator":">=","value":1}]}}`,
			want: CodeUnsupported,
		},
		{
			name: "OR filter unsupported",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"filter":{"operator":"or","conditions":[{"field":"meta.tenant","operator":"==","value":"t1"},{"field":"meta.tenant","operator":"==","value":"t2"}]}}`,
			want: CodeUnsupported,
		},
		{
			name: "NOT filter unsupported",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"filter":{"operator":"not","conditions":[{"field":"meta.tenant","operator":"==","value":"t1"}]}}`,
			want: CodeUnsupported,
		},
		{
			name: "inequality filter unsupported",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"filter":{"field":"meta.tenant","operator":"!=","value":"t1"}}`,
			want: CodeUnsupported,
		},
		{
			name: "keyword and-operator with filter unsupported",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"operator":"and","filter":{"field":"meta.tenant","operator":"==","value":"t1"}}`,
			want: CodeUnsupported,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, payload := ragParityPost(t, handler, tc.path, tc.body)
			code, _ := ragParityError(t, payload)
			if code != string(tc.want) {
				t.Fatalf("status=%d code=%s want %s", status, code, tc.want)
			}
		})
	}
}

func TestHTTPScalarTruncationFailsClosedNeverPartial(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")

	const bulkCount = 4200
	docs := make([]Document, 0, bulkCount+2)
	for i := 0; i < bulkCount; i++ {
		docs = append(docs, Document{
			ID:        fmt.Sprintf("bulk-%04d", i),
			Content:   "shared bulk topic text",
			Embedding: []float32{1, 0, 0, 0},
			Meta:      map[string]any{"tenant": "bulk", "priority": int64(i)},
		})
	}
	docs = append(docs,
		Document{ID: "rare-1", Content: "unique rare topic text", Embedding: []float32{0, 1, 0, 0}, Meta: map[string]any{"tenant": "rare", "priority": int64(1)}},
		Document{ID: "rare-2", Content: "unique rare topic text", Embedding: []float32{0, 0, 1, 0}, Meta: map[string]any{"tenant": "rare", "priority": int64(2)}},
	)
	req := UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal bulk upsert: %v", err)
	}
	status, payload := ragParityPost(t, handler, "/v1/indexes/docs/documents/upsert", string(raw))
	if status != http.StatusOK {
		t.Fatalf("bulk upsert status=%d body=%s", status, mustJSON(payload))
	}

	// Control: a selective allow-set succeeds and returns only matching docs.
	status, payload = ragParityPost(t, handler, "/v1/indexes/docs/search/keyword",
		`{"query":"rare","top_k":5,"filter":{"field":"meta.tenant","operator":"==","value":"rare"}}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, payload)
		t.Fatalf("selective filtered keyword status=%d code=%s message=%q", status, code, message)
	}
	if docs := payload["documents"].([]any); len(docs) != 2 {
		t.Fatalf("selective filtered documents=%s want rare-1 and rare-2", mustJSON(docs))
	}

	// The broad allow-set exceeds the bounded lookup limit: typed error, never
	// a partial ranking.
	status, payload = ragParityPost(t, handler, "/v1/indexes/docs/search/keyword",
		`{"query":"bulk","top_k":5,"filter":{"field":"meta.tenant","operator":"==","value":"bulk"}}`)
	code, message := ragParityError(t, payload)
	if code != string(CodeIndexUnavailable) {
		t.Fatalf("truncated filter status=%d code=%s message=%q want typed fail-closed error", status, code, message)
	}
	if !strings.Contains(message, "scalar_filter_unbounded") {
		t.Fatalf("message=%q want scalar_filter_unbounded reason surfaced", message)
	}
	if _, ok := payload["documents"]; ok {
		t.Fatalf("truncated filter returned documents payload: %s", mustJSON(payload))
	}

	status, payload = ragParityPost(t, handler, "/v1/indexes/docs/search/hybrid",
		`{"query":"bulk","top_k":5,"filter":{"field":"meta.tenant","operator":"==","value":"bulk"}}`)
	code, message = ragParityError(t, payload)
	if code != string(CodeIndexUnavailable) || !strings.Contains(message, "scalar_filter_unbounded") {
		t.Fatalf("hybrid truncated filter status=%d code=%s message=%q", status, code, message)
	}
}

func TestHTTPDenseAnnRouteParityWithExactRoute(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	createBody := `{"name":"docs","dimension":4}`
	status, payload := ragParityPost(t, handler, "/v1/indexes", createBody)
	if status != http.StatusOK {
		t.Fatalf("create index status=%d body=%s", status, mustJSON(payload))
	}
	docs := make([]Document, 0, 24)
	for i := 0; i < 24; i++ {
		content := "topic shared filler"
		switch i % 3 {
		case 0:
			content = "alpha retrieval corpus entry"
		case 1:
			content = "beta retrieval corpus entry"
		default:
			content = "gamma retrieval corpus entry"
		}
		embedding := []float32{float32(i%7) / 7, float32((i*3)%5) / 5, float32(i%11) / 11, 0.25}
		docs = append(docs, Document{ID: fmt.Sprintf("doc-%02d", i), Content: content, Embedding: embedding})
	}
	ragParityUpsertTenantDocs(t, handler, "docs", docs)

	query := `[0.85,0.15,0.05,0.25]`
	exactStatus, exactPayload := ragParityPost(t, handler, "/v1/indexes/docs/search/vector",
		`{"query_embedding":`+query+`,"top_k":8,"route":"exact"}`)
	if exactStatus != http.StatusOK {
		code, message := ragParityError(t, exactPayload)
		t.Fatalf("exact route status=%d code=%s message=%q", exactStatus, code, message)
	}
	if exactPayload["route"] != "exact" || exactPayload["exact"] != true {
		t.Fatalf("exact route echo=%s", mustJSON(exactPayload))
	}
	annStatus, annPayload := ragParityPost(t, handler, "/v1/indexes/docs/search/vector",
		`{"query_embedding":`+query+`,"top_k":8,"route":"ann","ef_search":64}`)
	if annStatus != http.StatusOK {
		code, message := ragParityError(t, annPayload)
		t.Fatalf("ann route status=%d code=%s message=%q", annStatus, code, message)
	}
	if annPayload["route"] != "ann" || annPayload["exact"] != false {
		t.Fatalf("ann route echo=%s", mustJSON(annPayload))
	}

	exactDocs := exactPayload["documents"].([]any)
	annDocs := annPayload["documents"].([]any)
	if len(annDocs) == 0 {
		t.Fatalf("ann route returned no documents")
	}
	annScores := map[string]float64{}
	for _, raw := range annDocs {
		doc := raw.(map[string]any)
		annScores[doc["id"].(string)] = doc["score"].(float64)
	}
	for _, raw := range exactDocs {
		doc := raw.(map[string]any)
		id := doc["id"].(string)
		score, ok := annScores[id]
		if !ok {
			t.Fatalf("exact result %s missing from ann candidates at ef_search=64", id)
		}
		if math.Abs(score-doc["score"].(float64)) > 1e-6 {
			t.Fatalf("score mismatch for %s: exact=%f ann=%f", id, doc["score"].(float64), score)
		}
	}
}

func TestHTTPDenseAnnRouteValidationFailsClosed(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())

	cases := []struct {
		name     string
		index    string
		body     string
		wantCode ErrorCode
	}{
		{name: "unknown route value", index: "docs", body: `{"query_embedding":[1,0,0,0],"top_k":2,"route":"fast"}`, wantCode: CodeInvalidRequest},
		{name: "ann with filter requires exact", index: "docs", body: `{"query_embedding":[1,0,0,0],"top_k":2,"route":"ann","filter":{"field":"meta.tenant","operator":"==","value":"t1"}}`, wantCode: CodeInvalidRequest},
		{name: "negative ef_search", index: "docs", body: `{"query_embedding":[1,0,0,0],"top_k":2,"route":"ann","ef_search":-1}`, wantCode: CodeInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, payload := ragParityPost(t, handler, "/v1/indexes/"+tc.index+"/search/vector", tc.body)
			code, _ := ragParityError(t, payload)
			if code != string(tc.wantCode) {
				t.Fatalf("status=%d code=%s want %s", status, code, tc.wantCode)
			}
		})
	}

	// Non-column_graph indexes keep exact dense scoring and reject ann.
	body := `{"name":"l2docs","dimension":4,"metric":"l2"}`
	status, payload := ragParityPost(t, handler, "/v1/indexes", body)
	if status != http.StatusOK {
		t.Fatalf("create l2 index status=%d body=%s", status, mustJSON(payload))
	}
	status, payload = ragParityPost(t, handler, "/v1/indexes/l2docs/search/vector",
		`{"query_embedding":[1,0,0,0],"top_k":2,"route":"ann"}`)
	code, _ := ragParityError(t, payload)
	if code != string(CodeUnsupported) {
		t.Fatalf("l2 ann status=%d code=%s want unsupported", status, code)
	}
	status, payload = ragParityPost(t, handler, "/v1/indexes/l2docs/search/vector",
		`{"query_embedding":[1,0,0,0],"top_k":2}`)
	if status != http.StatusOK {
		t.Fatalf("l2 default route status=%d body=%s", status, mustJSON(payload))
	}
	if payload["route"] != "exact" {
		t.Fatalf("l2 default route=%v want exact fallback documented as deterministic defaulting", payload["route"])
	}
}

func TestHTTPContractVersionReportsV1Alpha2(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	var health map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if health["contract_version"] != "treedb-document-service/v1alpha2" {
		t.Fatalf("health contract_version=%v want v1alpha2", health["contract_version"])
	}
}

func mustJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(raw)
}
