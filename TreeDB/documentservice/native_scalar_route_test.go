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
	if dense.Route != RouteAnn ||
		dense.Exact ||
		!dense.NativeBasePlusLiveDelta ||
		dense.ScalarFilterMembershipSource != "bounded_complete_set" ||
		dense.ScalarFilterPlan != collections.NativeScalarFilterPlanCompleteExact ||
		dense.ScalarFilterCandidates != 2 ||
		dense.ScalarFilterCandidateIDs != 2 ||
		dense.ScalarFilterRetainedCandidateIDs != 2 ||
		dense.ScalarFilterRefinedCandidateIDs != 2 ||
		dense.ScalarFilterVisited != 2 ||
		dense.ScalarFilterScored != 2 ||
		dense.ScalarFilterAdmitted != 2 ||
		!dense.ScalarFilterExactScoring ||
		dense.ScalarFilterPlanCacheMisses != 1 ||
		dense.ScalarFilterPlanCacheHits != 0 ||
		dense.ScalarFilterPlanCacheEntries == 0 ||
		dense.ScalarFilterPlanCacheRetainedBytes == 0 ||
		dense.ScalarFilterUnbounded != 0 ||
		dense.ExactFallbacks != 0 ||
		dense.FullDocumentScanFallbacks != 0 ||
		dense.AllowedIDMaterializationRows != 2 ||
		dense.PrimaryDocumentScans != 0 ||
		dense.DocumentMaterializationRows != 2 ||
		dense.VisibilityMismatchCount != 0 ||
		dense.VisibilityRetryCount != 0 {
		t.Fatalf("dense=%+v", dense)
	}
	hybrid, err := svc.SearchHybrid(ctx, create.Name, HybridSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 2, VectorCandidateLimit: 2, EfSearch: 16, Filter: filter})
	if err != nil {
		t.Fatal(err)
	}
	if hybrid.Stats.ScalarFilterPlan != dense.ScalarFilterPlan ||
		hybrid.Stats.ScalarFilterExactScoring != 1 ||
		!dense.ScalarFilterExactScoring ||
		hybrid.Stats.ScalarFilterPlanCacheHits != 1 ||
		hybrid.Stats.ScalarFilterPlanCacheMisses != 0 ||
		len(hybrid.Documents) != len(dense.Documents) {
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
	malformed := []*Filter{
		{
			Operator: "AND",
			Field:    "meta.user_id",
			Conditions: []Filter{
				{Field: "meta.user_id", Operator: "==", Value: "alpha"},
			},
		},
		{
			Field:    "meta.user_id",
			Operator: "==",
			Value:    "alpha",
			Conditions: []Filter{
				{Field: "meta.fpath", Operator: "==", Value: "a"},
			},
		},
	}
	for _, invalid := range malformed {
		if _, err := svc.SearchDenseVector(ctx, create.Name, DenseVectorSearchRequest{
			QueryEmbedding: []float32{1, 0}, TopK: 1, Route: RouteAnn, Filter: invalid,
		}); ErrorCodeOf(err) != CodeInvalidRequest {
			t.Fatalf("malformed native ANN filter=%+v err=%v code=%s", invalid, err, ErrorCodeOf(err))
		}
	}
}
