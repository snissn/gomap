package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

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
}

func TestTypedMetadataUpdateValidationAndHTTPUnknownKeys(t *testing.T) {
	valid := UpdateMetadataByIDRequest{ExpectedGeneration: 1, IDs: []string{"a"}, Set: map[string]any{"meta.acl": "new"}, Unset: []string{}}
	for name, mutate := range map[string]func(*UpdateMetadataByIDRequest){
		"empty_ids":       func(r *UpdateMetadataByIDRequest) { r.IDs = nil },
		"missing_set":     func(r *UpdateMetadataByIDRequest) { r.Set = nil },
		"missing_unset":   func(r *UpdateMetadataByIDRequest) { r.Unset = nil },
		"duplicate_ids":   func(r *UpdateMetadataByIDRequest) { r.IDs = []string{"a", "a"} },
		"non_meta":        func(r *UpdateMetadataByIDRequest) { r.Set = map[string]any{"content": "x"} },
		"same_path":       func(r *UpdateMetadataByIDRequest) { r.Unset = []string{"meta.acl"} },
		"ancestor":        func(r *UpdateMetadataByIDRequest) { r.Unset = []string{"meta.acl.child"} },
		"duplicate_unset": func(r *UpdateMetadataByIDRequest) { r.Set = nil; r.Unset = []string{"meta.a", "meta.a"} },
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

func TestTypedMetadataUpdateOutcomeErrorsRemainStructured(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code ErrorCode
	}{
		{collections.ErrCommitAmbiguous, CodeCommitAmbiguous},
		{collections.ErrRecoveryRequired, CodeRecoveryRequired},
		{backenddb.ErrRecoveryRequired, CodeRecoveryRequired},
		{collections.ErrDurabilityUnavailable, CodeIndexUnavailable},
		{collections.ErrDuplicateDocumentID, CodeInvalidRequest},
	} {
		mapped := mapTypedMetadataUpdateError(errors.Join(tc.err, errors.New("detail")))
		if ErrorCodeOf(mapped) != tc.code || !errors.Is(mapped, tc.err) {
			t.Fatalf("mapped=%v code=%s want=%s", mapped, ErrorCodeOf(mapped), tc.code)
		}
	}
}
