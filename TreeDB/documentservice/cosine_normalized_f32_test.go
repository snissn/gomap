package documentservice

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireCosineNormalizedF32V1Serving(t *testing.T) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("normalized serving requires exact relative namespace support")
	}
}

func TestServiceCosineNormalizedF32V1HNSWExactReceipt(t *testing.T) {
	requireCosineNormalizedF32V1Serving(t)
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name: "normalized-hnsw", Dimension: 2, Metric: MetricCosine, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
			QuantizedIndexes: []QuantizedIndexInfo{{Name: "embedding.scalar_u8.v1", Codec: collections.QuantizedVectorCodecScalarU8}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < 4097; start += 256 {
		end := min(4097, start+256)
		documents := make([]Document, end-start)
		for ordinal := start; ordinal < end; ordinal++ {
			documents[ordinal-start] = Document{ID: fmt.Sprintf("row-%06d", ordinal), Content: "content", Embedding: []float32{1, float32(ordinal) / 8192}}
		}
		if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: documents, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatal(err)
		}
	}
	serving := typedServiceTestOptions()
	serving.Publication.Rows, serving.Publication.Tombstones, serving.Publication.ValueSlots = 8192, 8192, 16384
	serving.SearchCandidates = 16384
	if _, err := svc.OptimizeIndex(ctx, info.Name, OptimizeIndexRequest{ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	var observed collections.VectorIndexSearchResponse
	svc.denseVectorNativeAfterSearch = func(_ int, response collections.VectorIndexSearchResponse) error {
		observed = response
		return nil
	}
	request := DenseVectorSearchRequest{
		ExpectedGeneration: info.Generation, QueryEmbedding: []float32{1, 0}, TopK: 5, EfSearch: 64, Route: RouteAnn,
		QueryMode: collections.VectorIndexQueryModeExact, VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		RequireVectorRepresentation: true,
	}
	response, err := svc.SearchDenseVector(ctx, info.Name, request)
	if err != nil {
		t.Fatalf("normalized HNSW exact search: %v; stats=%+v receipt=%+v", err, observed.Stats, observed.Stats.ColumnGraphReceipt)
	}
	if len(response.Documents) != 5 || response.RouteIdentity == nil || response.RouteIdentity.ExecutionRoute != "typed_hnsw" ||
		response.RouteIdentity.PackedScoreCalls <= 1 || response.RouteIdentity.PackedScoreCalls > response.RouteIdentity.PackedScoreCandidates {
		t.Fatalf("normalized HNSW exact response=%+v", response)
	}
}

func normalizedF32V1ServiceOptions() *BenchmarkVectorIndexOptions {
	return &BenchmarkVectorIndexOptions{
		Strategy:       collections.VectorIndexStrategyColumnGraph,
		Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
	}
}

func TestServiceCosineNormalizedF32V1ProductionAndDiagnostics(t *testing.T) {
	requireCosineNormalizedF32V1Serving(t)
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{
		Name: "normalized-serving", Dimension: 2, Metric: MetricCosine, TypedInput: true,
		VectorIndexOptions: &BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
			QuantizedIndexes: []QuantizedIndexInfo{{Name: "embedding.scalar_u8.v1", Codec: collections.QuantizedVectorCodecScalarU8}},
		},
		ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	docs := []Document{{ID: "a", Content: "alpha", Embedding: []float32{3, 4}, Meta: map[string]any{"tenant": "one"}}, {ID: "b", Content: "beta", Embedding: []float32{0, 2}, Meta: map[string]any{"tenant": "two"}}}
	if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	serving := typedServiceTestOptions()
	if _, err := svc.OptimizeIndex(ctx, info.Name, OptimizeIndexRequest{ColumnGraphServing: &serving}); err != nil {
		t.Fatal(err)
	}
	base := DenseVectorSearchRequest{
		ExpectedGeneration: info.Generation, QueryEmbedding: []float32{3, 4}, TopK: 1, EfSearch: 8, Route: RouteAnn,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1, RequireVectorRepresentation: true,
	}
	production, err := svc.SearchDenseVector(ctx, info.Name, base)
	if err != nil {
		t.Fatal(err)
	}
	if production.DenseWork != nil || production.ScorePlane != nil || production.RouteIdentity == nil || production.RouteIdentity.Diagnostics || len(production.Documents) != 1 || production.Documents[0].Embedding != nil {
		t.Fatalf("normalized production response=%+v", production)
	}
	identity := production.RouteIdentity
	if identity.Representation != collections.VectorIndexRepresentationCosineNormalizedF32V1 || identity.QueryMode != collections.VectorIndexQueryModeExact || identity.FP32ScoreCalls == 0 || identity.FP32VectorBytesRead != identity.FP32ScoreCalls*8 || identity.EmbeddingVectorReads != 0 || identity.EmbeddingVectorBytes != 0 || identity.EmbeddingOutputBytes != 0 {
		t.Fatalf("normalized production identity=%+v", identity)
	}
	forbiddenEmbeddingIdentity := *identity
	forbiddenEmbeddingIdentity.EmbeddingVectorReads = 1
	if err := validateDenseSearchRouteIdentity(forbiddenEmbeddingIdentity, base, production.Index, len(production.Documents)); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("production identity accepted forbidden embedding materialization: %v", err)
	}
	zeroScoreIdentity := *identity
	zeroScoreIdentity.FP32ScoreCalls, zeroScoreIdentity.FP32VectorBytesRead = 0, 0
	if err := validateDenseSearchRouteIdentity(zeroScoreIdentity, base, production.Index, len(production.Documents)); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("production identity accepted nonempty zero-score work: %v", err)
	}
	invalidOwnerIdentity := *identity
	invalidOwnerIdentity.CurrentCoverageLSN = 0
	if err := validateDenseSearchRouteIdentity(invalidOwnerIdentity, base, production.Index, len(production.Documents)); ErrorCodeOf(err) != CodeIndexUnavailable {
		t.Fatalf("production identity accepted zero owner coverage: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, info.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "c", Content: "mutable", Embedding: []float32{4, 3}, Meta: map[string]any{"tenant": "one"}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	mutableRequest := base
	mutableRequest.QueryEmbedding, mutableRequest.TopK = []float32{4, 3}, 2
	mutable, err := svc.SearchDenseVector(ctx, info.Name, mutableRequest)
	if err != nil || mutable.RouteIdentity == nil || len(mutable.Documents) != 2 || mutable.Documents[0].ID != "c" || mutable.RouteIdentity.FP32VectorBytesRead != mutable.RouteIdentity.FP32ScoreCalls*8 {
		t.Fatalf("normalized mutable suffix response=%+v err=%v", mutable, err)
	}
	diagnosticRequest := base
	diagnosticRequest.Diagnostics = true
	diagnostic, err := svc.SearchDenseVector(ctx, info.Name, diagnosticRequest)
	if err != nil || diagnostic.DenseWork == nil || !diagnostic.DenseWork.Completed || diagnostic.ScorePlane != nil || diagnostic.RouteIdentity == nil || !diagnostic.RouteIdentity.Diagnostics {
		t.Fatalf("normalized exact diagnostics=%+v err=%v", diagnostic, err)
	}
	embeddingRequest := base
	embeddingRequest.ReturnEmbedding = true
	withEmbedding, err := svc.SearchDenseVector(ctx, info.Name, embeddingRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(withEmbedding.Documents) != 1 || len(withEmbedding.Documents[0].Embedding) != 2 || withEmbedding.RouteIdentity == nil || withEmbedding.RouteIdentity.EmbeddingVectorReads != 1 || withEmbedding.RouteIdentity.EmbeddingVectorBytes != 8 || withEmbedding.RouteIdentity.EmbeddingOutputBytes == 0 {
		t.Fatalf("normalized embedding response=%+v", withEmbedding)
	}
	quantizedRequest := base
	quantizedRequest.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	quantizedRequest.QuantizedIndexName = "embedding.scalar_u8.v1"
	quantizedRequest.QuantizedRerankCandidates = 2
	quantized, err := svc.SearchDenseVector(ctx, info.Name, quantizedRequest)
	if err != nil {
		t.Fatal(err)
	}
	if quantized.DenseWork != nil || quantized.ScorePlane != nil || quantized.RouteIdentity == nil || quantized.RouteIdentity.QuantizedScoreCalls == 0 || quantized.RouteIdentity.PackedScoreCalls != 1 || quantized.RouteIdentity.PackedScoreCandidates == 0 {
		t.Fatalf("normalized quantized production=%+v", quantized)
	}
	filteredQuantizedRequest := quantizedRequest
	filteredQuantizedRequest.Filter = &Filter{Field: "meta.tenant", Operator: "==", Value: "two"}
	filteredQuantized, err := svc.SearchDenseVector(ctx, info.Name, filteredQuantizedRequest)
	if err != nil || filteredQuantized.RouteIdentity == nil || filteredQuantized.RouteIdentity.ExecutionRoute != "typed_exact" || filteredQuantized.RouteIdentity.QuantizedScoreCalls != 0 || filteredQuantized.RouteIdentity.PackedScoreCandidates == 0 {
		t.Fatalf("normalized filtered quantized production=%+v err=%v", filteredQuantized, err)
	}
	filteredQuantizedRequest.Diagnostics = true
	filteredQuantizedDiagnostic, err := svc.SearchDenseVector(ctx, info.Name, filteredQuantizedRequest)
	if err != nil || filteredQuantizedDiagnostic.RouteIdentity == nil || filteredQuantizedDiagnostic.DenseWork == nil || filteredQuantizedDiagnostic.ScorePlane == nil || filteredQuantizedDiagnostic.ScorePlane.Route != "typed_exact" || filteredQuantizedDiagnostic.ScorePlane.ExactSmallFilterScoreCalls == 0 {
		t.Fatalf("normalized filtered quantized diagnostics=%+v err=%v", filteredQuantizedDiagnostic, err)
	}
	quantizedRequest.Diagnostics = true
	quantizedDiagnostic, err := svc.SearchDenseVector(ctx, info.Name, quantizedRequest)
	if err != nil || quantizedDiagnostic.DenseWork == nil || quantizedDiagnostic.ScorePlane == nil || !quantizedDiagnostic.ScorePlane.Completed || quantizedDiagnostic.ScorePlane.PackedScoreBatchCalls != 1 || quantizedDiagnostic.ScorePlane.ForbiddenStableScoreCalls != 0 {
		t.Fatalf("normalized quantized diagnostics=%+v err=%v", quantizedDiagnostic, err)
	}
	var httpProduction DenseVectorSearchResponse
	postJSON(t, NewHandler(svc), "/v1/indexes/"+info.Name+"/search/vector", base, http.StatusOK, &httpProduction)
	if httpProduction.RouteIdentity == nil || httpProduction.DenseWork != nil || httpProduction.ScorePlane != nil || len(httpProduction.Documents) != 1 || httpProduction.Documents[0].ID != production.Documents[0].ID || *httpProduction.Documents[0].Score != *production.Documents[0].Score {
		t.Fatalf("normalized HTTP production=%+v service=%+v", httpProduction, production)
	}
	var httpDiagnostic DenseVectorSearchResponse
	postJSON(t, NewHandler(svc), "/v1/indexes/"+info.Name+"/search/vector", quantizedRequest, http.StatusOK, &httpDiagnostic)
	if httpDiagnostic.RouteIdentity == nil || !httpDiagnostic.RouteIdentity.Diagnostics || httpDiagnostic.DenseWork == nil || httpDiagnostic.ScorePlane == nil || len(httpDiagnostic.Documents) != len(quantizedDiagnostic.Documents) {
		t.Fatalf("normalized HTTP diagnostics=%+v service=%+v", httpDiagnostic, quantizedDiagnostic)
	}

	called := false
	svc.denseVectorNativeAfterSearch = func(_ int, _ collections.VectorIndexSearchResponse) error {
		called = true
		return nil
	}
	mismatch := base
	mismatch.VectorRepresentation = ""
	if _, err := svc.SearchDenseVector(ctx, info.Name, mismatch); ErrorCodeOf(err) != CodeUnsupported || called {
		t.Fatalf("representation mismatch reached search: called=%v err=%v", called, err)
	}
}

func TestServiceCosineNormalizedF32V1CreateAndAdmission(t *testing.T) {
	requireCosineNormalizedF32V1Serving(t)
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
	requireCosineNormalizedF32V1Serving(t)
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
