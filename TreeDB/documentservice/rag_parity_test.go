package documentservice

import (
	"context"
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
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
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
	errObj := ragParityObjectValue(t, payload["error"], "error")
	code := ragParityStringValue(t, errObj["code"], "error.code")
	message := ragParityStringValue(t, errObj["message"], "error.message")
	return code, message
}

func ragParityObjectValue(t *testing.T, value any, label string) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is not an object: %#v", label, value)
	}
	return object
}

func ragParityArrayValue(t *testing.T, value any, label string) []any {
	t.Helper()
	array, ok := value.([]any)
	if !ok {
		t.Fatalf("%s is not an array: %#v", label, value)
	}
	return array
}

func ragParityStringValue(t *testing.T, value any, label string) string {
	t.Helper()
	text, ok := value.(string)
	if !ok {
		t.Fatalf("%s is not a string: %#v", label, value)
	}
	return text
}

func ragParityNumberValue(t *testing.T, value any, label string) float64 {
	t.Helper()
	number, ok := value.(float64)
	if !ok {
		t.Fatalf("%s is not a number: %#v", label, value)
	}
	return number
}

func ragParityCreateIndex(t *testing.T, handler *Handler, name string) {
	t.Helper()
	body := `{"name":"` + name + `","dimension":4,"scalar_fields":[{"field":"meta.tenant","value_type":"string"},{"field":"meta.priority","value_type":"int64"}]}`
	status, payload := ragParityPost(t, handler, "/v1/indexes", body)
	if status != http.StatusOK {
		t.Fatalf("create index status=%d body=%s", status, mustJSON(payload))
	}
	info := ragParityObjectValue(t, payload["index"], "index")
	if got := info["contract_version"]; got != ContractVersion {
		t.Fatalf("contract_version=%v want %s", got, ContractVersion)
	}
	fields := ragParityArrayValue(t, info["scalar_fields"], "scalar_fields")
	if len(fields) != 2 {
		t.Fatalf("scalar_fields=%#v want 2 declared entries", fields)
	}
	byField := map[string]map[string]any{}
	for i, raw := range fields {
		entry := ragParityObjectValue(t, raw, fmt.Sprintf("scalar_fields[%d]", i))
		byField[ragParityStringValue(t, entry["field"], fmt.Sprintf("scalar_fields[%d].field", i))] = entry
	}
	first := byField["meta.tenant"]
	if first == nil || first["index_name"] != "meta_tenant" || first["value_type"] != "string" {
		t.Fatalf("meta.tenant scalar entry missing: %#v", first)
	}
	if second := byField["meta.priority"]; second == nil || second["value_type"] != "int64" {
		t.Fatalf("meta.priority scalar entry missing: %#v", second)
	}
	caps := ragParityObjectValue(t, info["capabilities"], "capabilities")
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
	docs := ragParityArrayValue(t, payload["documents"], "keyword documents")
	if len(docs) != 1 {
		t.Fatalf("filtered keyword documents=%s want only t2 refund doc", mustJSON(docs))
	}
	doc := ragParityObjectValue(t, docs[0], "keyword document")
	if doc["id"] != "t2-a" {
		t.Fatalf("filtered keyword hit id=%v want t2-a", doc["id"])
	}
	stats := ragParityObjectValue(t, payload["stats"], "keyword stats")
	if ragParityNumberValue(t, stats["scalar_prefilter_ids"], "scalar_prefilter_ids") < 1 {
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
	docs := ragParityArrayValue(t, payload["documents"], "hybrid documents")
	if len(docs) == 0 {
		t.Fatalf("filtered hybrid documents empty")
	}
	for i, raw := range docs {
		doc := ragParityObjectValue(t, raw, fmt.Sprintf("hybrid document[%d]", i))
		meta := ragParityObjectValue(t, doc["meta"], fmt.Sprintf("hybrid document[%d].meta", i))
		if meta["tenant"] != "t1" {
			t.Fatalf("filtered hybrid leaked document %v tenant=%v", doc["id"], meta["tenant"])
		}
	}
	plan := ragParityObjectValue(t, payload["plan"], "hybrid plan")
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
	docs := ragParityArrayValue(t, payload["documents"], "range-filtered documents")
	if len(docs) != 1 || ragParityObjectValue(t, docs[0], "range-filtered document")["id"] != "t2-a" {
		t.Fatalf("range-filtered documents=%s want t2-a only", mustJSON(docs))
	}
}

func TestHTTPMultiFieldANDKeywordHybridParity4292(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	handler := NewHandler(svc)
	ragParityCreateIndex(t, handler, "docs")
	ragParityUpsertTenantDocs(t, handler, "docs", ragParityTenantDocs())
	filter := `"filter":{"operator":"and","conditions":[{"field":"meta.tenant","operator":"==","value":"t1"},{"field":"meta.priority","operator":">=","value":2}]}`

	status, keyword := ragParityPost(t, handler, "/v1/indexes/docs/search/keyword", `{"query":"refund","top_k":10,`+filter+`}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, keyword)
		t.Fatalf("keyword status=%d code=%s message=%q", status, code, message)
	}
	keywordDocs := ragParityArrayValue(t, keyword["documents"], "keyword documents")
	if len(keywordDocs) != 1 || ragParityObjectValue(t, keywordDocs[0], "keyword document")["id"] != "t1-b" {
		t.Fatalf("keyword documents=%s want t1-b", mustJSON(keywordDocs))
	}
	keywordStats := ragParityObjectValue(t, keyword["stats"], "keyword stats")
	if keywordStats["scalar_filter_lookups"] != float64(2) || keywordStats["scalar_filter_intersection_steps"] != float64(1) || keywordStats["scalar_filter_final_ids"] != float64(1) {
		t.Fatalf("keyword stats=%s", mustJSON(keywordStats))
	}

	status, hybrid := ragParityPost(t, handler, "/v1/indexes/docs/search/hybrid", `{"query":"refund","query_embedding":[0.95,0.05,0,0],"top_k":10,"ef_search":16,`+filter+`}`)
	if status != http.StatusOK {
		code, message := ragParityError(t, hybrid)
		t.Fatalf("hybrid status=%d code=%s message=%q", status, code, message)
	}
	hybridDocs := ragParityArrayValue(t, hybrid["documents"], "hybrid documents")
	if len(hybridDocs) != 1 || ragParityObjectValue(t, hybridDocs[0], "hybrid document")["id"] != "t1-b" {
		t.Fatalf("hybrid documents=%s want t1-b", mustJSON(hybridDocs))
	}
	plan := ragParityObjectValue(t, hybrid["plan"], "hybrid plan")
	stats := ragParityObjectValue(t, hybrid["stats"], "hybrid stats")
	if plan["scalar_filter_lookup_count"] != float64(2) || stats["scalar_filter_lookups"] != float64(2) || stats["scalar_filter_final_ids"] != float64(1) || stats["full_document_scan_fallbacks"] != nil {
		t.Fatalf("hybrid plan=%s stats=%s", mustJSON(plan), mustJSON(stats))
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
			name: "membership filter unsupported",
			path: "/v1/indexes/docs/search/keyword",
			body: `{"query":"refund","top_k":5,"filter":{"field":"meta.tenant","operator":"in","value":["t1"]}}`,
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
	// The collection scalar allow-set lookup is bounded at 4096 IDs; keep this
	// fixture above that bound so the broad tenant filter must fail closed.
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
	if docs := ragParityArrayValue(t, payload["documents"], "selective filtered documents"); len(docs) != 2 {
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

	exactDocs := ragParityArrayValue(t, exactPayload["documents"], "exact documents")
	annDocs := ragParityArrayValue(t, annPayload["documents"], "ann documents")
	if len(annDocs) == 0 {
		t.Fatalf("ann route returned no documents")
	}
	annScores := map[string]float64{}
	for i, raw := range annDocs {
		doc := ragParityObjectValue(t, raw, fmt.Sprintf("ann document[%d]", i))
		id := ragParityStringValue(t, doc["id"], fmt.Sprintf("ann document[%d].id", i))
		annScores[id] = ragParityNumberValue(t, doc["score"], fmt.Sprintf("ann document[%d].score", i))
	}
	for i, raw := range exactDocs {
		doc := ragParityObjectValue(t, raw, fmt.Sprintf("exact document[%d]", i))
		id := ragParityStringValue(t, doc["id"], fmt.Sprintf("exact document[%d].id", i))
		score := ragParityNumberValue(t, doc["score"], fmt.Sprintf("exact document[%d].score", i))
		annScore, ok := annScores[id]
		if !ok {
			t.Fatalf("exact result %s missing from ann candidates at ef_search=64", id)
		}
		if math.Abs(annScore-score) > 1e-6 {
			t.Fatalf("score mismatch for %s: exact=%f ann=%f", id, score, annScore)
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
