package documentservice

import (
	"context"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestServiceTypedInputOwnership(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	req := CreateIndexRequest{Name: "typed", Dimension: 8, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph},
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}, {Field: "meta.fpath", ValueType: ScalarFieldString}},
	}
	info, err := svc.CreateIndex(context.Background(), req)
	if err != nil || !info.TypedInput {
		t.Fatalf("typed create info=%+v err=%v", info, err)
	}
	col, _, err := svc.openIndex(context.Background(), req.Name, 0)
	if err != nil {
		t.Fatal(err)
	}
	meta := col.Meta()
	if !serviceUsesTypedInput(meta) || meta.Options.ColumnStore.RetainedPayload != collections.ColumnRetainedPayloadNonColumn || len(meta.TextIndexes) != 1 {
		t.Fatalf("typed schema=%+v", meta)
	}
	// Independent service handles derive selection from persistent metadata.
	other := New(collections.NewCollectionManager(db))
	defer other.Close()
	if info, err := other.OpenIndex(context.Background(), req.Name); err != nil || !info.TypedInput {
		t.Fatalf("independent open info=%+v err=%v", info, err)
	}
	if _, err := other.CreateIndex(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	req.TypedInput = false
	if _, err := other.CreateIndex(context.Background(), req); ErrorCodeOf(err) != CodeConflict {
		t.Fatalf("silent ownership downgrade: %v", err)
	}
	// Initial typed load is separate from explicit graph build/admission.
	doc := Document{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u", "fpath": "/a", "extra": "residual"}}
	if out, err := svc.UpsertDocuments(context.Background(), req.Name, UpsertDocumentsRequest{Documents: []Document{doc}, DeferVectorIndexRebuild: true}); err != nil || out.Inserted != 1 {
		t.Fatalf("typed load: %+v %v", out, err)
	}
}

func typedServiceTestOptions() collections.ColumnGraphServingOptions {
	return collections.ColumnGraphServingOptions{
		Publication:     collections.ColumnGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 4096, OwnedBytes: 16 << 20, EncodedOutputBytes: 16 << 20},
		Owners:          collections.ColumnGraphReadOwnerLimits{Owners: 8, States: 8, StateBytes: 128 << 20, AssetBytes: 128 << 20, Cold: collections.ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 8 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1 << 30, AppenderAttempts: 4096},
		Maintenance:     collections.ColumnGraphMaintenanceLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 64 << 20, ManifestBytes: 8 << 20, RetainedBytes: 256 << 20, PagerPages: 32768},
		Filter:          collections.ColumnGraphFilterLimits{SourceIDs: 4096, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 100000, InspectedEntries: 4096}, FoldRows: 4096, SearchCandidates: 4096,
	}
}

func TestServiceTypedInputServingLifecycle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("selected serving fixture requires Linux namespace authority and mmap")
	}
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()
	create := CreateIndexRequest{Name: "selected", Dimension: 8, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 2}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	docs := []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "u"}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Meta: map[string]any{"user_id": "v"}}}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	query := DenseVectorSearchRequest{QueryEmbedding: docs[0].Embedding, TopK: 1, Route: RouteAnn, ReturnEmbedding: true, Filter: &Filter{Field: "meta.user_id", Operator: "==", Value: "u"}}
	if _, err := svc.SearchDenseVector(ctx, create.Name, query); err == nil {
		t.Fatal("unadmitted search succeeded")
	}
	options := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("missing limits accepted: %v", err)
	}
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	different := options
	different.FoldRows++
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "ensure", ColumnGraphServing: &different}); err == nil {
		t.Fatal("changed admitted limits accepted")
	}
	out, err := svc.SearchDenseVector(ctx, create.Name, query)
	if err != nil || len(out.Documents) != 1 || out.Documents[0].ID != "a" || out.Documents[0].Content != "alpha" {
		t.Fatalf("search=%+v err=%v", out, err)
	}
	if out.NativeBasePlusLiveDelta || out.ColumnGraphPreparedSearch != 1 {
		t.Fatalf("wrong route proof: %+v", out)
	}
	// A publication between search and fetch cannot replace the returned
	// view's authority. The existing hook runs with the real owner still held.
	svc.denseVectorNativeAfterSearch = func(_ int, _ collections.VectorIndexSearchResponse) error {
		docs[0].Content = "between"
		_, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs[:1]})
		return err
	}
	out, err = svc.SearchDenseVector(ctx, create.Name, query)
	svc.denseVectorNativeAfterSearch = nil
	if err != nil || len(out.Documents) != 1 || out.Documents[0].Content != "alpha" {
		t.Fatalf("held view changed: %+v %v", out, err)
	}
	docs[0].Content = "updated"
	if out, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{Documents: docs[:1]}); err != nil || out.Updated != 1 {
		t.Fatalf("update=%+v err=%v", out, err)
	}
	if _, err := svc.OptimizeIndex(ctx, create.Name, OptimizeIndexRequest{ColumnGraphAction: "fold"}); err != nil {
		t.Fatal(err)
	}
	out, err = svc.SearchDenseVector(ctx, create.Name, query)
	if err != nil || len(out.Documents) != 1 || out.Documents[0].Content != "updated" {
		t.Fatalf("updated=%+v err=%v", out, err)
	}
	text, err := svc.SearchKeyword(ctx, create.Name, KeywordSearchRequest{Query: "updated", TopK: 4})
	if err != nil || len(text.Documents) != 1 || text.Documents[0].ID != "a" {
		t.Fatalf("typed text update=%+v %v", text, err)
	}
	old, err := svc.SearchKeyword(ctx, create.Name, KeywordSearchRequest{Query: "alpha", TopK: 4})
	if err != nil || len(old.Documents) != 0 {
		t.Fatalf("stale text posting=%+v %v", old, err)
	}
}
