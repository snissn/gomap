package documentservice

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func normalizedF32V1ServiceOptions() *BenchmarkVectorIndexOptions {
	return &BenchmarkVectorIndexOptions{
		Strategy:       collections.VectorIndexStrategyColumnGraph,
		Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
	}
}

func TestServiceCosineNormalizedF32V1CreateAndAdmission(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()

	for name, req := range map[string]CreateIndexRequest{
		"requires_typed_input": {
			Name: "normalized-untyped", Dimension: 2, Metric: MetricCosine,
			VectorIndexOptions: normalizedF32V1ServiceOptions(),
		},
		"requires_column_graph": {
			Name: "normalized-native", Dimension: 2, Metric: MetricCosine, TypedInput: true,
			VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1},
		},
		"requires_cosine": {
			Name: "normalized-l2", Dimension: 2, Metric: MetricL2, TypedInput: true,
			VectorIndexOptions: normalizedF32V1ServiceOptions(),
		},
		"rejects_unknown": {
			Name: "normalized-future", Dimension: 2, Metric: MetricCosine, TypedInput: true,
			VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, Representation: "future"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := svc.CreateIndex(ctx, req); ErrorCodeOf(err) != CodeInvalidRequest {
				t.Fatalf("create error=%v code=%s", err, ErrorCodeOf(err))
			}
		})
	}

	req := CreateIndexRequest{
		Name: "normalized", Dimension: 2, Metric: MetricCosine, TypedInput: true,
		VectorIndexOptions: normalizedF32V1ServiceOptions(),
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}},
	}
	info, err := svc.CreateIndex(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !info.TypedInput || info.VectorRepresentation != collections.VectorIndexRepresentationCosineNormalizedF32V1 {
		t.Fatalf("create info=%+v", info)
	}
	col, opened, err := svc.openIndex(ctx, req.Name, 0)
	if err != nil {
		t.Fatal(err)
	}
	if opened.VectorRepresentation != info.VectorRepresentation || col.Meta().VectorIndexes[0].Representation != info.VectorRepresentation {
		t.Fatalf("representation did not round trip: created=%+v opened=%+v meta=%+v", info, opened, col.Meta().VectorIndexes)
	}

	input := []float32{3, 4}
	doc := Document{ID: "a", Content: "alpha", Embedding: input, Meta: map[string]any{"user_id": "u1"}}
	if _, err := svc.UpsertDocuments(ctx, req.Name, UpsertDocumentsRequest{Documents: []Document{doc}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	if input[0] != 3 || input[1] != 4 {
		t.Fatalf("service admission mutated caller embedding: %v", input)
	}
	raw, err := col.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	var stored Document
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatal(err)
	}
	want := []float32{float32(3 / math.Sqrt(25)), float32(4 / math.Sqrt(25))}
	if len(stored.Embedding) != len(want) {
		t.Fatalf("stored embedding=%v", stored.Embedding)
	}
	for i := range want {
		if math.Float32bits(stored.Embedding[i]) != math.Float32bits(want[i]) {
			t.Fatalf("stored embedding[%d]=%08x want=%08x", i, math.Float32bits(stored.Embedding[i]), math.Float32bits(want[i]))
		}
	}
}

func TestServiceCosineNormalizedF32V1ResetCreatesTypedSchema(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	reset, err := svc.ResetIndex(context.Background(), "normalized-reset", ResetIndexRequest{
		TypedInput:         true,
		Dimension:          2,
		Metric:             MetricCosine,
		DropOld:            true,
		VectorIndexOptions: normalizedF32V1ServiceOptions(),
		ScalarFields:       []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reset.Created || !reset.Index.TypedInput || reset.Index.VectorRepresentation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || len(reset.Index.ScalarFields) != 1 {
		t.Fatalf("reset response=%+v", reset)
	}
}
