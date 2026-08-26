package documentservice

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestServiceNativeScalarDenseAndVectorOnlyHybridParity(t *testing.T) {
	svc, db := newTestService(t)
	defer func() {
		_ = svc.Close()
		_ = db.Close()
	}()
	ctx := context.Background()
	create := CreateIndexRequest{
		Name:               "native_scalar",
		Dimension:          2,
		Metric:             MetricCosine,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime},
		ScalarFields: []ScalarFieldDeclaration{
			{Field: "meta.user_id", ValueType: ScalarFieldString},
			{Field: "meta.fpath", ValueType: ScalarFieldString},
		},
	}
	if _, err := svc.CreateIndex(ctx, create); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, create.Name, UpsertDocumentsRequest{
		Documents: []Document{
			{ID: "alpha-near", Embedding: []float32{0.99, 0.01}, Meta: map[string]any{"user_id": "alpha", "fpath": "a"}},
			{ID: "beta-nearest", Embedding: []float32{1, 0}, Meta: map[string]any{"user_id": "beta", "fpath": "b"}},
			{ID: "alpha-far", Embedding: []float32{0, 1}, Meta: map[string]any{"user_id": "alpha", "fpath": "c"}},
		},
		DeferVectorIndexRebuild: true,
	}); err != nil {
		t.Fatal(err)
	}
	filter := &Filter{Field: "meta.user_id", Operator: "==", Value: "alpha"}
	dense, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if dense.Route != RouteAnn || dense.Exact || dense.ScalarFilterPlan != collections.NativeScalarFilterPlanCompleteExact {
		t.Fatalf("dense=%+v", dense)
	}
	hybrid, err := svc.SearchHybrid(ctx, create.Name, HybridSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, VectorCandidateLimit: 2, EfSearch: 16, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if hybrid.Stats.ScalarFilterPlan != dense.ScalarFilterPlan || len(hybrid.Documents) != len(dense.Documents) {
		t.Fatalf("dense=%+v hybrid=%+v", dense, hybrid)
	}
	for i := range dense.Documents {
		if dense.Documents[i].ID != hybrid.Documents[i].ID || dense.Documents[i].ID == "beta-nearest" {
			t.Fatalf("dense=%+v hybrid=%+v", dense.Documents, hybrid.Documents)
		}
	}
	if _, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{
		QueryEmbedding: []float32{1, 0}, TopK: 1, Route: RouteAnn,
		Filter: &Filter{Operator: "OR", Conditions: []Filter{{Field: "meta.user_id", Operator: "==", Value: "alpha"}, {Field: "meta.user_id", Operator: "==", Value: "beta"}}},
	}); ErrorCodeOf(err) != CodeUnsupported {
		t.Fatalf("unsupported OR err=%v code=%s", err, ErrorCodeOf(err))
	}
}
