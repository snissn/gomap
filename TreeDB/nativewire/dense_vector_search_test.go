package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestDenseVectorSearchNativewireParityAndBorrowing(t *testing.T) {
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx := context.Background()
	_, err = svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name:      "docs",
		Dimension: 2,
		Metric:    documentservice.MetricCosine,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyNativeRuntime,
		},
		ScalarFields: []documentservice.ScalarFieldDeclaration{
			{Field: "meta.user_id", ValueType: documentservice.ScalarFieldString},
			{Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	documents := []documentservice.Document{
		{ID: "a", Content: "a", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "alpha", "rank": int64(2)}},
		{ID: "b", Content: "b", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "beta", "rank": int64(1)}},
		{ID: "c", Content: "c", Embedding: []float32{0.8, 0.2}, Meta: map[string]any{"user_id": "alpha", "rank": int64(3)}},
	}
	if _, err = svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: documents}); err != nil {
		t.Fatal(err)
	}
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err = client.Hello(ctx); err != nil {
		t.Fatal(err)
	}

	filters := []*documentservice.Filter{
		{Field: "meta.user_id", Operator: "==", Value: "alpha"},
		{Operator: "AND", Conditions: []documentservice.Filter{
			{Field: "meta.user_id", Operator: "==", Value: "alpha"},
			{Field: "meta.rank", Operator: ">=", Value: int64(3)},
		}},
	}
	for _, filter := range filters {
		request := DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 2, EfSearch: 8, Filter: filter}
		got, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		want, err := svc.SearchDenseVector(ctx, request.Index, documentservice.DenseVectorSearchRequest{
			QueryEmbedding: request.Query,
			TopK:           request.TopK,
			EfSearch:       request.EfSearch,
			Route:          documentservice.RouteAnn,
			Filter:         request.Filter,
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Route != documentservice.RouteAnn || !got.NativeBasePlusLiveDelta || got.ExactFallbacks != 0 || got.FullDocumentScanFallbacks != 0 {
			t.Fatalf("native route proof missing: %+v", got)
		}
		if len(got.Results) != len(want.Documents) || got.Candidates != want.Candidates {
			t.Fatalf("native results/candidates=%d/%d service=%d/%d", len(got.Results), got.Candidates, len(want.Documents), want.Candidates)
		}
		for i := range got.Results {
			wantDoc := want.Documents[i]
			wantScore := *wantDoc.Score
			wantDoc.Score = nil
			wantRaw, err := json.Marshal(wantDoc)
			if err != nil {
				t.Fatal(err)
			}
			var gotJSON, wantJSON any
			if err := json.Unmarshal(got.Results[i].Document, &gotJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(wantRaw, &wantJSON); err != nil {
				t.Fatal(err)
			}
			if string(got.Results[i].ID) != wantDoc.ID || math.Abs(got.Results[i].Score-wantScore) > 1e-12 || !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("result[%d]=%q/%g/%s want %q/%g/%s", i, got.Results[i].ID, got.Results[i].Score, got.Results[i].Document, wantDoc.ID, wantScore, wantRaw)
			}
			if bytes.Contains(got.Results[i].Document, []byte(`"embedding"`)) {
				t.Fatalf("result[%d] retained excluded embedding: %s", i, got.Results[i].Document)
			}
		}
	}

	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1, 0}, TopK: 0}); err == nil {
		t.Fatal("invalid top_k succeeded")
	}
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{
		Index: "docs", Query: []float32{1, 0}, TopK: 1,
		Filter: &documentservice.Filter{Operator: "OR", Conditions: []documentservice.Filter{
			{Field: "meta.user_id", Operator: "==", Value: "alpha"},
			{Field: "meta.user_id", Operator: "==", Value: "beta"},
		}},
	}); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
		t.Fatalf("OR filter error=%v code=%d", err, nativeCodeOf(err))
	}
}

func TestDenseVectorSearchCodecFailsClosed(t *testing.T) {
	limits := iwire.DefaultLimits()
	request := DenseVectorSearchRequest{
		Index: "docs", Query: []float32{1, 0}, TopK: 3, EfSearch: 8,
		Filter: &documentservice.Filter{Operator: "AND", Conditions: []documentservice.Filter{
			{Field: "meta.tenant", Operator: "==", Value: "alpha"},
			{Field: "meta.rank", Operator: ">", Value: json.Number("2")},
		}},
	}
	raw, err := appendDenseVectorSearchRequest(nil, request, limits)
	if err != nil {
		t.Fatal(err)
	}
	var root documentservice.Filter
	decoded, leaves, err := decodeDenseVectorSearchRequest(raw, limits, nil, &root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Index != request.Index || decoded.TopK != request.TopK || decoded.EfSearch != request.EfSearch || len(leaves) != 2 || decoded.Filter == nil || decoded.Filter.Operator != "AND" {
		t.Fatalf("decoded=%+v leaves=%+v", decoded, leaves)
	}
	if _, _, err := decodeDenseVectorSearchRequest(raw[:len(raw)-1], limits, nil, &root, nil); err == nil {
		t.Fatal("truncated dense request decoded")
	}
	tooMany := make([]documentservice.Filter, denseFilterMaxLeaves+1)
	for i := range tooMany {
		tooMany[i] = documentservice.Filter{Field: "meta.x", Operator: "==", Value: "x"}
	}
	request.Filter = &documentservice.Filter{Operator: "AND", Conditions: tooMany}
	if _, err := appendDenseVectorSearchRequest(nil, request, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized filter error=%v code=%d", err, nativeCodeOf(err))
	}
	limits.MaxSectionLen = 64
	request.Filter = &documentservice.Filter{Field: "meta.x", Operator: "==", Value: string(make([]byte, 60))}
	if _, err := appendDenseVectorSearchRequest(nil, request, limits); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("oversized request error=%v code=%d", err, nativeCodeOf(err))
	}
}

func TestDenseVectorSearchUnconfiguredFailsClosed(t *testing.T) {
	server := NewServer(ServerOptions{})
	defer server.Close()
	ctx := context.Background()
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	_ = client.Hello(ctx)
	if _, err = client.DenseVectorSearch(ctx, DenseVectorSearchRequest{Index: "docs", Query: []float32{1}, TopK: 1}); err == nil {
		t.Fatal("unconfigured search succeeded")
	}
}
