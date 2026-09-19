package documentservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Same bounded service fixture as source replacement. Transport setup is not
// timed; service validation, collection planning and durable publication are.
func BenchmarkServiceTypedMetadataMutation(b *testing.B) {
	for _, rows := range []int{1, 32, 128} {
		for _, dims := range []int{8, 768} {
			b.Run(fmt.Sprintf("rows%d/dims%d", rows, dims), func(b *testing.B) {
				if b.N > 16 {
					b.Skip("bounded metadata diagnostic: use -benchtime=10x")
				}
				svc, db := newTestService(b)
				defer db.Close()
				defer svc.Close()
				ctx := context.Background()
				info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "metadata-cost", Dimension: dims, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.acl", ValueType: ScalarFieldString}}})
				if err != nil {
					b.Fatal(err)
				}
				ids, docs := make([]string, rows), make([]Document, rows)
				for i := range ids {
					ids[i] = fmt.Sprintf("metadata-%03d", i)
					v := make([]float32, dims)
					v[i%dims] = 3
					v[(i+1)%dims] = 4
					docs[i] = Document{ID: ids[i], Content: "immutable searchable content", Embedding: v, Meta: map[string]any{"acl": "old"}}
				}
				if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{ExpectedGeneration: info.Generation, Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
					b.Fatal(err)
				}
				requests := []UpdateMetadataByIDRequest{{ExpectedGeneration: info.Generation, IDs: ids, Set: map[string]any{"meta.acl": "new-0"}, Unset: []string{}}, {ExpectedGeneration: info.Generation, IDs: ids, Set: map[string]any{"meta.acl": "new-1"}, Unset: []string{}}}
				// Drain buffered ingestion before measuring the mutation boundary.
				if _, err := svc.UpdateMetadataByID(ctx, info.Name, requests[1]); err != nil {
					b.Fatal(err)
				}
				wire, err := json.Marshal(requests[0])
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					out, err := svc.UpdateMetadataByID(ctx, info.Name, requests[i%2])
					if err != nil || out.ModifiedCount != rows {
						b.Fatalf("out=%+v err=%v", out, err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(wire)), "request-json-B/op")
			})
		}
	}
}

func TestTypedMetadataUpdatePublicLifecycle(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name: "typed-metadata", Dimension: 2, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.acl", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	doc := Document{ID: "a", Content: "unchanged", Embedding: []float32{0.25, 0.75}, Meta: map[string]any{"acl": "old", "residual": map[string]any{"keep": true}}}
	if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{ExpectedGeneration: info.Generation, Documents: []Document{doc}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	out, err := svc.UpdateMetadataByID(ctx, info.Name, UpdateMetadataByIDRequest{
		ExpectedGeneration: info.Generation,
		IDs:                []string{"a", "missing"},
		Set:                map[string]any{"meta.acl": "new", "meta.residual.changed": json.Number("1")},
		Unset:              []string{},
	})
	if err != nil || out.MatchedCount != 1 || out.ModifiedCount != 1 {
		t.Fatalf("update=%+v err=%v", out, err)
	}
	got, err := svc.FetchTypedDocuments(ctx, info.Name, info.Generation, [][]byte{[]byte("a")})
	if err != nil || len(got.Results) != 1 || !got.Results[0].Found {
		t.Fatalf("get=%+v err=%v", got, err)
	}
	var fetched Document
	if err := json.Unmarshal(got.Results[0].Document, &fetched); err != nil {
		t.Fatal(err)
	}
	if fetched.ID != "a" || fetched.Content != "unchanged" || len(fetched.Embedding) != 2 || fetched.Embedding[0] != 0.25 || fetched.Meta["acl"] != "new" {
		t.Fatalf("document=%+v", fetched)
	}
	noop, err := svc.UpdateMetadataByID(ctx, info.Name, UpdateMetadataByIDRequest{ExpectedGeneration: info.Generation, IDs: []string{"a"}, Set: map[string]any{"meta.acl": "new"}, Unset: []string{}})
	if err != nil || noop.MatchedCount != 1 || noop.ModifiedCount != 0 {
		t.Fatalf("noop=%+v err=%v", noop, err)
	}
	_, err = svc.UpdateMetadataByID(ctx, info.Name, UpdateMetadataByIDRequest{ExpectedGeneration: info.Generation, IDs: []string{"a"}, Set: map[string]any{"meta.chunk_parent": "immutable"}, Unset: []string{}})
	if ErrorCodeOf(err) != CodeInvalidRequest || !errors.Is(err, collections.ErrTypedMetadataInvalid) {
		t.Fatalf("reserved metadata err=%v code=%s", err, ErrorCodeOf(err))
	}
}

func TestTypedMetadataUpdateValidationAndHTTPUnknownKeys(t *testing.T) {
	valid := UpdateMetadataByIDRequest{ExpectedGeneration: 1, IDs: []string{"a"}, Set: map[string]any{"meta.acl": "new"}, Unset: []string{}}
	for name, mutate := range map[string]func(*UpdateMetadataByIDRequest){
		"empty_ids":     func(r *UpdateMetadataByIDRequest) { r.IDs = nil },
		"missing_set":   func(r *UpdateMetadataByIDRequest) { r.Set = nil },
		"missing_unset": func(r *UpdateMetadataByIDRequest) { r.Unset = nil },
		"duplicate_ids": func(r *UpdateMetadataByIDRequest) { r.IDs = []string{"a", "a"} },
		"non_meta":      func(r *UpdateMetadataByIDRequest) { r.Set = map[string]any{"content": "x"} },
		"same_path":     func(r *UpdateMetadataByIDRequest) { r.Unset = []string{"meta.acl"} },
		"ancestor":      func(r *UpdateMetadataByIDRequest) { r.Unset = []string{"meta.acl.child"} },
		"punctuation": func(r *UpdateMetadataByIDRequest) {
			r.Set = map[string]any{"meta.a": 1, "meta.a-child": 2}
			r.Unset = []string{"meta.a.child"}
		},
		"duplicate_unset": func(r *UpdateMetadataByIDRequest) { r.Set = map[string]any{}; r.Unset = []string{"meta.a", "meta.a"} },
		"invalid_value":   func(r *UpdateMetadataByIDRequest) { r.Set = map[string]any{"meta.x": make(chan int)} },
	} {
		t.Run(name, func(t *testing.T) {
			req := valid
			mutate(&req)
			if err := validateMetadataUpdateShape(req); ErrorCodeOf(err) != CodeInvalidRequest {
				t.Fatalf("error=%v", err)
			}
		})
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/indexes/docs/documents/update_metadata_by_id", strings.NewReader(`{"expected_generation":1,"ids":["a"],"set":{},"unset":[],"vector":[1,2]}`))
	NewHandler(&Service{}).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), string(CodeMalformedJSON)) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func BenchmarkTypedMetadataHTTPRequestDecode(b *testing.B) {
	raw := []byte(`{"expected_generation":1,"ids":["a"],"set":{"meta.x":{"nested":1}},"unset":[]}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var req UpdateMetadataByIDRequest
		r := &http.Request{Body: io.NopCloser(bytes.NewReader(raw))}
		if !NewHandler(nil).decodeJSON(httptest.NewRecorder(), r, defaultMaxRequestBytes, &req) {
			b.Fatal("decode metadata HTTP request")
		}
	}
}

func TestTypedMetadataHTTPRejectsLossyUnicode(t *testing.T) {
	for _, value := range []string{`"\ud800"`, `{"\udc00":1}`, `["\ud800"]`, "\"\xff\""} {
		recorder := httptest.NewRecorder()
		body := `{"expected_generation":1,"ids":["a"],"set":{"meta.x":` + value + `},"unset":[]}`
		NewHandler(&Service{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/indexes/docs/documents/update_metadata_by_id", strings.NewReader(body)))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), string(CodeMalformedJSON)) {
			t.Fatalf("lossy Unicode %q: status=%d body=%s", value, recorder.Code, recorder.Body.String())
		}
	}
}

func TestTypedMetadataHTTPDecodePreservesUnicodeAndBounds(t *testing.T) {
	for _, value := range []string{`"\ud83d\ude00"`, `"\\ud800"`, `"\ufffd"`} {
		body := `{"expected_generation":1,"ids":["a"],"set":{"meta.x":` + value + `,"meta.n":9007199254740993},"unset":[]}`
		var want string
		if err := json.Unmarshal([]byte(value), &want); err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int64{int64(len(body)), int64(len(body) - 1)} {
			var req UpdateMetadataByIDRequest
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			ok := NewHandler(nil).decodeJSON(httptest.NewRecorder(), r, limit, &req)
			if ok != (limit == int64(len(body))) {
				t.Fatalf("limit=%d accepted=%v", limit, ok)
			}
			if ok && (req.Set["meta.x"] != want || req.Set["meta.n"] != json.Number("9007199254740993")) {
				t.Fatalf("lossy decode: %+v", req.Set)
			}
		}
	}
}

func TestTypedMetadataUpdateManyPathsPreservesInput(t *testing.T) {
	paths := make([]string, 4096)
	for i := range paths {
		paths[i] = fmt.Sprintf("meta.field%04d", len(paths)-i)
	}
	before := slices.Clone(paths)
	req := UpdateMetadataByIDRequest{IDs: []string{"a"}, Set: map[string]any{"meta.a-child": 1, "meta.a.child": 2}, Unset: paths}
	if err := validateMetadataUpdateShape(req); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paths, before) {
		t.Fatal("validation reordered caller paths")
	}
	req.Set["meta.a"] = 3
	if err := validateMetadataUpdateShape(req); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("ancestor hidden by punctuation error=%v", err)
	}
}

func BenchmarkTypedMetadataUpdatePathValidation(b *testing.B) {
	for _, count := range []int{1, 256, 4096} {
		b.Run(fmt.Sprintf("paths%d", count), func(b *testing.B) {
			paths := make([]string, count)
			for i := range paths {
				paths[i] = fmt.Sprintf("meta.field%04d", count-i)
			}
			req := UpdateMetadataByIDRequest{IDs: []string{"a"}, Set: map[string]any{}, Unset: paths}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := validateMetadataUpdateShape(req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestTypedMetadataUpdateOutcomeErrorsRemainStructured(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code ErrorCode
	}{
		{collections.ErrCommitAmbiguous, CodeCommitAmbiguous},
		{collections.ErrRecoveryRequired, CodeRecoveryRequired},
		{backenddb.ErrRecoveryRequired, CodeRecoveryRequired},
		{collections.ErrDurabilityUnavailable, CodeIndexUnavailable},
		{collections.ErrConcurrentMutation, CodeConflict},
		{collections.ErrHybridSearchStaleIndex, CodeIndexStale},
		{collections.ErrHybridSearchUnsupported, CodeUnsupported},
		{collections.ErrDuplicateDocumentID, CodeInvalidRequest},
		{collections.ErrTypedMetadataInvalid, CodeInvalidRequest},
	} {
		mapped := mapTypedMetadataUpdateError(errors.Join(tc.err, errors.New("detail")))
		if ErrorCodeOf(mapped) != tc.code || !errors.Is(mapped, tc.err) {
			t.Fatalf("mapped=%v code=%s want=%s", mapped, ErrorCodeOf(mapped), tc.code)
		}
	}
	priority := mapTypedMetadataUpdateError(errors.Join(collections.ErrRecoveryRequired, collections.ErrHybridSearchStaleIndex))
	if ErrorCodeOf(priority) != CodeRecoveryRequired {
		t.Fatalf("priority=%v code=%s", priority, ErrorCodeOf(priority))
	}
}
