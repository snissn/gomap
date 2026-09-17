package nativewire

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func newDenseNormalizedV4Test(t *testing.T) (*Server, *Client, documentservice.IndexInfo, context.Context) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("selected serving requires exact relative namespace support")
	}
	var native [2]byte
	binary.NativeEndian.PutUint16(native[:], 1)
	if native[0] != 1 {
		t.Skip("selected serving requires little-endian prepared views")
	}
	db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	mgr := collections.NewCollectionManager(db)
	svc := documentservice.New(mgr)
	server := NewServer(ServerOptions{Collections: mgr, Backend: db, DocumentService: svc})
	t.Cleanup(func() { _ = server.Close(); _ = svc.Close(); _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	info, err := svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name: "normalized-v4", Dimension: 2, Metric: documentservice.MetricCosine, TypedInput: true,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
			QuantizedIndexes: []documentservice.QuantizedIndexInfo{{Name: "embedding.scalar_u8.v1", Codec: collections.QuantizedVectorCodecScalarU8}},
		},
		ScalarFields: []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	docs := []documentservice.Document{
		{ID: "a", Content: "alpha", Embedding: []float32{3, 4}, Meta: map[string]any{"tenant": "one"}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 2}, Meta: map[string]any{"tenant": "two"}},
	}
	if _, err := svc.UpsertDocuments(ctx, info.Name, documentservice.UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	options := denseNormalizedV4ServingOptions()
	if _, err := svc.OptimizeIndex(ctx, info.Name, documentservice.OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, info.Name, documentservice.UpsertDocumentsRequest{Documents: []documentservice.Document{{ID: "c", Content: "mutable", Embedding: []float32{4, 3}, Meta: map[string]any{"tenant": "one"}}}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	client, cleanup, err := NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanup() })
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	return server, client, info, ctx
}

func denseNormalizedV4ServingOptions() collections.ColumnGraphServingOptions {
	return collections.ColumnGraphServingOptions{
		Publication:     collections.ColumnGraphPublicationLimits{Rows: 512, Tombstones: 512, ValueSlots: 4096, OwnedBytes: 16 << 20, EncodedOutputBytes: 16 << 20},
		Owners:          collections.ColumnGraphReadOwnerLimits{Owners: 8, States: 8, StateBytes: 128 << 20, AssetBytes: 128 << 20, Cold: collections.ColumnGraphColdLimits{ManifestRecords: 4096, ManifestBytes: 8 << 20, AssetBytes: 64 << 20, DecodedTermBytes: 64 << 20}, Physical: collections.ColumnGraphPhysicalResourceLimits{Segments: 4096, Descriptors: 4096, MappedBytes: 1 << 30, FallbackBytes: 1 << 30, InventoryBytes: 64 << 20}},
		CandidateOutput: collections.ColumnGraphCandidateOutputLimits{Bytes: 1 << 30, AppenderAttempts: 4096},
		Maintenance:     collections.ColumnGraphMaintenanceLimits{NativeEntries: 4096, ColumnSegments: 4096, ManifestRecords: 4096, LifecycleEntries: 4096, NativeBytes: 128 << 20, ColumnBytes: 64 << 20, ManifestBytes: 8 << 20, RetainedBytes: 256 << 20, PagerPages: 32768},
		Filter:          collections.ColumnGraphFilterLimits{SourceIDs: 4096, SourceBytes: 4 << 20, RetainedBytes: 4 << 20, MappingWork: 100000, InspectedEntries: 4096}, FoldRows: 4096, SearchCandidates: 4096,
	}
}

func TestDenseNormalizedV4ProductionDiagnosticsAndVersionIsolation(t *testing.T) {
	_, client, info, ctx := newDenseNormalizedV4Test(t)
	base := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{3, 4}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
	}
	production, err := client.DenseVectorSearch(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if !production.TypedColumnGraph || production.DenseWork.Version != 0 || production.ScorePlane != nil || production.RouteIdentity == nil || production.RouteIdentity.Diagnostics || len(production.Results) != 1 {
		t.Fatalf("normalized v4 production=%+v", production)
	}
	if production.RouteIdentity.EmbeddingVectorReads != 0 || production.RouteIdentity.EmbeddingVectorBytes != 0 || production.RouteIdentity.EmbeddingOutputBytes != 0 {
		t.Fatalf("normalized v4 production read an excluded embedding: %+v", production.RouteIdentity)
	}
	zeroScoreIdentity := *production.RouteIdentity
	zeroScoreIdentity.FP32ScoreCalls, zeroScoreIdentity.FP32VectorBytesRead = 0, 0
	if err := validateDenseNormalizedRouteIdentity(zeroScoreIdentity, base, len(production.Results)); nativeCodeOf(err) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("normalized v4 accepted nonempty zero-score work: %v", err)
	}
	invalidOwnerIdentity := *production.RouteIdentity
	invalidOwnerIdentity.CurrentCoverageLSN = 0
	if err := validateDenseNormalizedRouteIdentity(invalidOwnerIdentity, base, len(production.Results)); nativeCodeOf(err) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("normalized v4 accepted zero owner coverage: %v", err)
	}
	productionDecisions := snapshotDenseNormalizedV4Decisions(production)
	diagnosticRequest := base
	diagnosticRequest.Diagnostics = true
	diagnostic, err := client.DenseVectorSearch(ctx, diagnosticRequest)
	if err != nil || diagnostic.DenseWork.Version != 1 || !diagnostic.DenseWork.Completed || diagnostic.ScorePlane != nil || diagnostic.RouteIdentity == nil || !diagnostic.RouteIdentity.Diagnostics {
		t.Fatalf("normalized v4 exact diagnostics=%+v err=%v", diagnostic, err)
	}
	assertDenseNormalizedV4SameDecisions(t, productionDecisions, diagnostic)
	embeddingRequest := base
	embeddingRequest.ReturnEmbedding = true
	withEmbedding, err := client.DenseVectorSearch(ctx, embeddingRequest)
	if err != nil {
		t.Fatal(err)
	}
	if withEmbedding.RouteIdentity == nil || withEmbedding.RouteIdentity.EmbeddingVectorReads != 1 || withEmbedding.RouteIdentity.EmbeddingVectorBytes != 8 || withEmbedding.RouteIdentity.EmbeddingOutputBytes == 0 {
		t.Fatalf("normalized v4 embedding accounting=%+v", withEmbedding.RouteIdentity)
	}
	var document documentservice.Document
	if err := json.Unmarshal(withEmbedding.Results[0].Document, &document); err != nil || len(document.Embedding) != 2 {
		t.Fatalf("normalized v4 embedding document=%s err=%v", withEmbedding.Results[0].Document, err)
	}
	quantized := base
	quantized.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	quantized.QuantizedIndexName = "embedding.scalar_u8.v1"
	quantized.QuantizedRerankCandidates = 2
	quantizedProduction, err := client.DenseVectorSearch(ctx, quantized)
	if err != nil {
		t.Fatal(err)
	}
	if quantizedProduction.DenseWork.Version != 0 || quantizedProduction.ScorePlane != nil || quantizedProduction.RouteIdentity == nil || quantizedProduction.RouteIdentity.QuantizedScoreCalls == 0 || quantizedProduction.RouteIdentity.PackedScoreCalls != 1 {
		t.Fatalf("normalized v4 quantized production=%+v", quantizedProduction)
	}
	quantizedProductionDecisions := snapshotDenseNormalizedV4Decisions(quantizedProduction)
	quantized.Diagnostics = true
	quantizedDiagnostic, err := client.DenseVectorSearch(ctx, quantized)
	if err != nil || quantizedDiagnostic.DenseWork.Version != 1 || quantizedDiagnostic.ScorePlane == nil || quantizedDiagnostic.ScorePlane.PackedScoreBatchCalls != 1 || quantizedDiagnostic.ScorePlane.ForbiddenStableScoreCalls != 0 {
		t.Fatalf("normalized v4 quantized diagnostics=%+v err=%v", quantizedDiagnostic, err)
	}
	assertDenseNormalizedV4SameDecisions(t, quantizedProductionDecisions, quantizedDiagnostic)
	filtered := base
	filtered.Filter = &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "two"}
	filteredResult, err := client.DenseVectorSearch(ctx, filtered)
	if err != nil || len(filteredResult.Results) != 1 || string(filteredResult.Results[0].ID) != "b" || filteredResult.RouteIdentity == nil || !filteredResult.RouteIdentity.Filter {
		t.Fatalf("normalized v4 filtered=%+v err=%v", filteredResult, err)
	}
	filteredQuantized := quantized
	filteredQuantized.Diagnostics = false
	filteredQuantized.Filter = filtered.Filter
	filteredQuantizedResult, err := client.DenseVectorSearch(ctx, filteredQuantized)
	if err != nil || len(filteredQuantizedResult.Results) != 1 || string(filteredQuantizedResult.Results[0].ID) != "b" || filteredQuantizedResult.RouteIdentity == nil || filteredQuantizedResult.RouteIdentity.ExecutionRoute != "typed_exact" || filteredQuantizedResult.RouteIdentity.QuantizedScoreCalls != 0 || filteredQuantizedResult.RouteIdentity.PackedScoreCandidates == 0 {
		t.Fatalf("normalized v4 filtered quantized=%+v err=%v", filteredQuantizedResult, err)
	}
	invalidFilteredIdentity := *filteredQuantizedResult.RouteIdentity
	invalidFilteredIdentity.QuantizedScoreCalls, invalidFilteredIdentity.QuantizedCodeBytesRead = 1, 2
	if err := validateDenseNormalizedRouteIdentity(invalidFilteredIdentity, filteredQuantized, len(filteredQuantizedResult.Results)); nativeCodeOf(err) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("normalized v4 exact shortcut accepted candidate-code work: %v", err)
	}
	filteredQuantized.Diagnostics = true
	if filteredQuantizedDiagnostic, err := client.DenseVectorSearch(ctx, filteredQuantized); err != nil || filteredQuantizedDiagnostic.DenseWork.Version != 1 || filteredQuantizedDiagnostic.ScorePlane == nil || filteredQuantizedDiagnostic.ScorePlane.Route != "typed_exact" || filteredQuantizedDiagnostic.ScorePlane.ExactSmallFilterScoreCalls == 0 {
		t.Fatalf("normalized v4 filtered quantized diagnostics=%+v err=%v", filteredQuantizedDiagnostic, err)
	}
	empty := base
	empty.Filter = &documentservice.Filter{Field: "meta.tenant", Operator: "==", Value: "missing"}
	emptyResult, err := client.DenseVectorSearch(ctx, empty)
	if err != nil || len(emptyResult.Results) != 0 || emptyResult.RouteIdentity == nil || emptyResult.RouteIdentity.ExecutionRoute != "typed_empty" || emptyResult.RouteIdentity.ResultCount != 0 {
		t.Fatalf("normalized v4 empty=%+v err=%v", emptyResult, err)
	}
	invalidEmptyIdentity := *emptyResult.RouteIdentity
	invalidEmptyIdentity.FP32ScoreCalls, invalidEmptyIdentity.FP32VectorBytesRead = 1, 8
	if err := validateDenseNormalizedRouteIdentity(invalidEmptyIdentity, empty, len(emptyResult.Results)); nativeCodeOf(err) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("normalized v4 empty route accepted score work: %v", err)
	}
	legacy := base
	legacy.VectorRepresentation = ""
	legacy.TypedColumnGraph = true
	if _, err := client.DenseVectorSearch(ctx, legacy); !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("v2 accepted a normalized index: %v", err)
	}
	legacy.TypedColumnGraph = false
	if _, err := client.DenseVectorSearch(ctx, legacy); !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("v1 accepted a normalized index: %v", err)
	}
	legacy.TypedColumnGraph = true
	legacy.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	legacy.QuantizedIndexName = "embedding.scalar_u8.v1"
	legacy.QuantizedRerankCandidates = 2
	if _, err := client.DenseVectorSearch(ctx, legacy); !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("v3 accepted a normalized index: %v", err)
	}
	if math.Abs(productionDecisions[0].score-quantizedProductionDecisions[0].score) > 0.25 {
		t.Fatalf("normalized v4 exact/quantized scores diverged: exact=%g quantized=%g", productionDecisions[0].score, quantizedProductionDecisions[0].score)
	}
}

func TestDenseNormalizedV4BorrowedResultsAndOwnedRouteIdentity(t *testing.T) {
	_, client, info, ctx := newDenseNormalizedV4Test(t)
	request := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{3, 4}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
	}
	first, err := client.DenseVectorSearch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Results) != 1 || len(first.Results[0].ID) == 0 || len(first.Results[0].Document) == 0 || first.RouteIdentity == nil {
		t.Fatalf("first normalized response=%+v", first)
	}
	ownedRoute := *first.RouteIdentity

	request.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	request.QuantizedIndexName = "embedding.scalar_u8.v1"
	request.QuantizedRerankCandidates = 2
	second, err := client.DenseVectorSearch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Results) != 1 || second.RouteIdentity == nil || second.RouteIdentity.QuantizedScoreCalls == 0 {
		t.Fatalf("second normalized response=%+v", second)
	}
	if *first.RouteIdentity != ownedRoute {
		t.Fatalf("owned route identity changed after the next round trip: got=%+v want=%+v", *first.RouteIdentity, ownedRoute)
	}
}

type denseNormalizedV4Decision struct {
	id    string
	score float64
}

func snapshotDenseNormalizedV4Decisions(response DenseVectorSearchResponse) []denseNormalizedV4Decision {
	out := make([]denseNormalizedV4Decision, len(response.Results))
	for i := range response.Results {
		out[i] = denseNormalizedV4Decision{id: string(response.Results[i].ID), score: response.Results[i].Score}
	}
	return out
}

func assertDenseNormalizedV4SameDecisions(t *testing.T, production []denseNormalizedV4Decision, diagnostic DenseVectorSearchResponse) {
	t.Helper()
	if len(production) != len(diagnostic.Results) {
		t.Fatalf("production results=%d diagnostics=%d", len(production), len(diagnostic.Results))
	}
	for i := range production {
		if production[i].id != string(diagnostic.Results[i].ID) || production[i].score != diagnostic.Results[i].Score {
			t.Fatalf("result[%d] production=%q/%g diagnostics=%q/%g", i, production[i].id, production[i].score, diagnostic.Results[i].ID, diagnostic.Results[i].Score)
		}
	}
}

func TestDenseNormalizedV4RequestValidation(t *testing.T) {
	_, client, info, ctx := newDenseNormalizedV4Test(t)
	base := DenseVectorSearchRequest{Index: info.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation, VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1}
	for name, mutate := range map[string]func(*DenseVectorSearchRequest){
		"zero generation": func(r *DenseVectorSearchRequest) { r.ExpectedGeneration = 0 },
		"zero E":          func(r *DenseVectorSearchRequest) { r.EfSearch = 0 },
		"unknown repr":    func(r *DenseVectorSearchRequest) { r.VectorRepresentation = "future" },
		"quantized zero R": func(r *DenseVectorSearchRequest) {
			r.QueryMode, r.QuantizedIndexName = collections.VectorIndexQueryModeQuantizedRerank, "embedding.scalar_u8.v1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			if _, err := client.DenseVectorSearch(ctx, request); nativeCodeOf(err) != iwire.ErrInvalidCommand && nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
				t.Fatalf("invalid normalized request err=%v", err)
			}
		})
	}
}

func denseNormalizedV4Sections(t *testing.T, server *Server, ctx context.Context, request DenseVectorSearchRequest) []iwire.Section {
	t.Helper()
	body, err := denseNormalizedV4BodyForTest(server, ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := iwire.DecodeSections(body, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	return cloneDenseNormalizedV4Sections(decoded)
}

func denseNormalizedV4BodyForTest(server *Server, ctx context.Context, request DenseVectorSearchRequest) ([]byte, error) {
	requestRaw, err := appendDenseVectorSearchRequest(nil, request, server.limits)
	if err != nil {
		return nil, err
	}
	options, err := appendDenseNormalizedOptions(nil, request, server.limits)
	if err != nil {
		return nil, err
	}
	sections := []iwire.Section{
		{ID: iwire.SectionDenseSearchRequest, Bytes: requestRaw},
		{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))},
		{ID: iwire.SectionDenseSearchNormalizedOptions, Flags: iwire.SectionFlagCritical, Bytes: options},
	}
	if request.Diagnostics {
		sections = append(sections, iwire.Section{ID: iwire.SectionDenseSearchDiagnostics, Flags: iwire.SectionFlagCritical, Bytes: binary.AppendUvarint(nil, denseNormalizedDiagnosticsVersion)})
	}
	return server.handleVersionedDenseVectorSearch(ctx, &connState{}, iwire.DenseVectorSearchNormalizedVersion, sections, nil)
}

func TestDenseNormalizedV4ResponseLimitsMatchActualEnvelope(t *testing.T) {
	server, _, info, ctx := newDenseNormalizedV4Test(t)
	request := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1, QueryMode: collections.VectorIndexQueryModeExact,
	}
	original := server.limits
	t.Cleanup(func() { server.limits = original })

	server.limits.MaxSections = 4
	if _, err := denseNormalizedV4BodyForTest(server, ctx, request); err != nil {
		t.Fatalf("four-section production response rejected by diagnostic ceiling: %v", err)
	}
	request.Diagnostics = true
	if _, err := denseNormalizedV4BodyForTest(server, ctx, request); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("five-section diagnostic response ignored section limit: %v", err)
	}

	request.Diagnostics = false
	server.limits = original
	server.limits.MaxByteVectorBytes = 1
	if _, err := denseNormalizedV4BodyForTest(server, ctx, request); nativeCodeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("normalized response ignored byte-vector payload limit: %v", err)
	}
}

func cloneDenseNormalizedV4Sections(sections []iwire.Section) []iwire.Section {
	out := make([]iwire.Section, len(sections))
	for i := range sections {
		out[i] = sections[i]
		out[i].Bytes = bytes.Clone(sections[i].Bytes)
	}
	return out
}

func denseNormalizedV4Section(t *testing.T, sections []iwire.Section, id iwire.SectionID) *iwire.Section {
	t.Helper()
	for i := range sections {
		if sections[i].ID == id {
			return &sections[i]
		}
	}
	t.Fatalf("section %d is missing", id)
	return nil
}

func decodeDenseNormalizedV4ForTest(sections []iwire.Section, request DenseVectorSearchRequest) error {
	_, _, _, _, err := decodeDenseNormalizedResponse(sections, request, iwire.DefaultLimits(), nil, nil, nil)
	return err
}

func TestDenseNormalizedV4ResponseFailsClosed(t *testing.T) {
	server, _, info, ctx := newDenseNormalizedV4Test(t)
	request := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{3, 4}, TopK: 2, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		QueryMode:            collections.VectorIndexQueryModeExact,
	}
	valid := denseNormalizedV4Sections(t, server, ctx, request)
	if err := decodeDenseNormalizedV4ForTest(valid, request); err != nil {
		t.Fatalf("valid response: %v", err)
	}

	invalidMeta := func(scores ...float64) []byte {
		raw := binary.AppendUvarint(nil, denseNormalizedResponseVersion)
		raw = binary.AppendUvarint(raw, uint64(len(scores)))
		for _, score := range scores {
			raw = binary.LittleEndian.AppendUint64(raw, math.Float64bits(score))
		}
		return raw
	}
	for name, mutate := range map[string]func([]iwire.Section) []iwire.Section{
		"missing route": func(sections []iwire.Section) []iwire.Section {
			for i := range sections {
				if sections[i].ID == iwire.SectionDenseSearchRouteIdentity {
					return append(sections[:i], sections[i+1:]...)
				}
			}
			return sections
		},
		"duplicate route": func(sections []iwire.Section) []iwire.Section {
			return append(sections, *denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchRouteIdentity))
		},
		"route not critical": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchRouteIdentity).Flags = 0
			return sections
		},
		"unknown noncritical flags": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDocumentIDs).Flags = 2
			return sections
		},
		"unsolicited production work": func(sections []iwire.Section) []iwire.Section {
			return append(sections, iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: []byte{1}})
		},
		"unknown response section": func(sections []iwire.Section) []iwire.Section {
			return append(sections, iwire.Section{ID: 999})
		},
		"truncated metadata": func(sections []iwire.Section) []iwire.Section {
			meta := denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchResponse)
			meta.Bytes = meta.Bytes[:len(meta.Bytes)-1]
			return sections
		},
		"nan score": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchResponse).Bytes = invalidMeta(math.NaN(), 0)
			return sections
		},
		"out of range score": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchResponse).Bytes = invalidMeta(2, 0)
			return sections
		},
		"unordered scores": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchResponse).Bytes = invalidMeta(-1, 1)
			return sections
		},
		"duplicate IDs": func(sections []iwire.Section) []iwire.Section {
			denseNormalizedV4Section(t, sections, iwire.SectionDocumentIDs).Bytes = iwire.AppendByteVector(nil, []byte("a"), []byte("a"))
			return sections
		},
		"malformed document": func(sections []iwire.Section) []iwire.Section {
			docs := denseNormalizedV4Section(t, sections, iwire.SectionDocuments)
			docs.Bytes = iwire.AppendByteVector(nil, []byte(`{"id":"a"}`), []byte(`{"id":"wrong"}`))
			return sections
		},
		"embedding when excluded": func(sections []iwire.Section) []iwire.Section {
			docs := denseNormalizedV4Section(t, sections, iwire.SectionDocuments)
			docs.Bytes = iwire.AppendByteVector(nil, []byte(`{"id":"a","embedding":[0.6,0.8]}`), []byte(`{"id":"b"}`))
			return sections
		},
		"route version": func(sections []iwire.Section) []iwire.Section {
			route := denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchRouteIdentity)
			route.Bytes[0]++
			return sections
		},
	} {
		t.Run(name, func(t *testing.T) {
			sections := mutate(cloneDenseNormalizedV4Sections(valid))
			if err := decodeDenseNormalizedV4ForTest(sections, request); err == nil {
				t.Fatal("hostile normalized response decoded")
			}
		})
	}
}

func TestDenseNormalizedV4RouteIdentityAndDiagnosticProofAreBound(t *testing.T) {
	server, _, info, ctx := newDenseNormalizedV4Test(t)
	base := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{3, 4}, TopK: 2, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		QueryMode:            collections.VectorIndexQueryModeExact,
	}
	for _, diagnostic := range []bool{false, true} {
		request := base
		request.Diagnostics = diagnostic
		sections := denseNormalizedV4Sections(t, server, ctx, request)
		routeSection := denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchRouteIdentity)
		identity, err := decodeDenseNormalizedRouteIdentity(routeSection.Bytes, server.limits)
		if err != nil {
			t.Fatal(err)
		}
		for name, mutate := range map[string]func(*documentservice.DenseSearchRouteIdentity){
			"result count": func(identity *documentservice.DenseSearchRouteIdentity) { identity.ResultCount++ },
			"top k":        func(identity *documentservice.DenseSearchRouteIdentity) { identity.TopK++ },
			"FP32 bytes":   func(identity *documentservice.DenseSearchRouteIdentity) { identity.FP32VectorBytesRead++ },
			"packed calls exceed candidates": func(identity *documentservice.DenseSearchRouteIdentity) {
				identity.PackedScoreCalls = identity.PackedScoreCandidates + 1
			},
			"embedding read": func(identity *documentservice.DenseSearchRouteIdentity) {
				identity.EmbeddingVectorReads = 1
			},
			"empty route with results": func(identity *documentservice.DenseSearchRouteIdentity) { identity.ExecutionRoute = "typed_empty" },
		} {
			t.Run(name+map[bool]string{false: "/production", true: "/diagnostic"}[diagnostic], func(t *testing.T) {
				hostile := cloneDenseNormalizedV4Sections(sections)
				changed := identity
				mutate(&changed)
				raw, err := appendDenseNormalizedRouteIdentity(nil, changed, server.limits)
				if err != nil {
					t.Fatal(err)
				}
				denseNormalizedV4Section(t, hostile, iwire.SectionDenseSearchRouteIdentity).Bytes = raw
				if err := decodeDenseNormalizedV4ForTest(hostile, request); err == nil {
					t.Fatal("mismatched route identity decoded")
				}
			})
		}
		if diagnostic {
			t.Run("diagnostic snapshot", func(t *testing.T) {
				hostile := cloneDenseNormalizedV4Sections(sections)
				workSection := denseNormalizedV4Section(t, hostile, iwire.SectionDenseSearchWork)
				work, err := decodeDenseWork(workSection.Bytes)
				if err != nil {
					t.Fatal(err)
				}
				work.Graph.Snapshot.SchemaHash++
				workSection.Bytes, err = appendDenseWork(nil, work)
				if err != nil {
					t.Fatal(err)
				}
				if err := decodeDenseNormalizedV4ForTest(hostile, request); err == nil {
					t.Fatal("route/proof snapshot mismatch decoded")
				}
			})
			t.Run("diagnostic FP32 counters", func(t *testing.T) {
				hostile := cloneDenseNormalizedV4Sections(sections)
				routeSection := denseNormalizedV4Section(t, hostile, iwire.SectionDenseSearchRouteIdentity)
				changed := identity
				changed.FP32ScoreCalls++
				changed.FP32VectorBytesRead += uint64(len(request.Query)) * 4
				var err error
				routeSection.Bytes, err = appendDenseNormalizedRouteIdentity(nil, changed, server.limits)
				if err != nil {
					t.Fatal(err)
				}
				if err := decodeDenseNormalizedV4ForTest(hostile, request); err == nil {
					t.Fatal("route/proof FP32 counter mismatch decoded")
				}
			})
		}
	}

	quantized := base
	quantized.Diagnostics = true
	quantized.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	quantized.QuantizedIndexName = "embedding.scalar_u8.v1"
	quantized.QuantizedRerankCandidates = 2
	sections := denseNormalizedV4Sections(t, server, ctx, quantized)
	routeSection := denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchRouteIdentity)
	identity, err := decodeDenseNormalizedRouteIdentity(routeSection.Bytes, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	identity.QuantizedScoreCalls++
	identity.QuantizedCodeBytesRead += uint64(len(quantized.Query))
	routeSection.Bytes, err = appendDenseNormalizedRouteIdentity(nil, identity, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeDenseNormalizedV4ForTest(sections, quantized); err == nil {
		t.Fatal("route/score-plane counter mismatch decoded")
	}
}

func TestDenseNormalizedV4RequestSectionsAndDeadlineFailClosed(t *testing.T) {
	server, client, info, ctx := newDenseNormalizedV4Test(t)
	request := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		QueryMode:            collections.VectorIndexQueryModeExact,
	}
	requestRaw, err := appendDenseVectorSearchRequest(nil, request, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	options, err := appendDenseNormalizedOptions(nil, request, server.limits)
	if err != nil {
		t.Fatal(err)
	}
	valid := []iwire.Section{
		{ID: iwire.SectionDenseSearchRequest, Bytes: requestRaw},
		{ID: iwire.SectionDeadline, Bytes: binary.AppendUvarint(nil, uint64(time.Now().Add(time.Minute).UnixNano()))},
		{ID: iwire.SectionDenseSearchNormalizedOptions, Flags: iwire.SectionFlagCritical, Bytes: options},
	}
	for name, mutate := range map[string]func([]iwire.Section) []iwire.Section{
		"options not critical": func(sections []iwire.Section) []iwire.Section { sections[2].Flags = 0; return sections },
		"duplicate options":    func(sections []iwire.Section) []iwire.Section { return append(sections, sections[2]) },
		"truncated options": func(sections []iwire.Section) []iwire.Section {
			sections[2].Bytes = sections[2].Bytes[:len(sections[2].Bytes)-1]
			return sections
		},
		"legacy representation tag": func(sections []iwire.Section) []iwire.Section { sections[2].Bytes[1] = 0; return sections },
	} {
		t.Run(name, func(t *testing.T) {
			sections := mutate(cloneDenseNormalizedV4Sections(valid))
			if _, err := server.handleVersionedDenseVectorSearch(ctx, &connState{}, iwire.DenseVectorSearchNormalizedVersion, sections, nil); err == nil {
				t.Fatal("hostile normalized request executed")
			}
		})
	}
	expired := cloneDenseNormalizedV4Sections(valid)
	denseNormalizedV4Section(t, expired, iwire.SectionDeadline).Bytes = binary.AppendUvarint(nil, uint64(time.Now().Add(-time.Second).UnixNano()))
	if _, err := server.handleVersionedDenseVectorSearch(context.Background(), &connState{}, iwire.DenseVectorSearchNormalizedVersion, expired, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired normalized request err=%v", err)
	}
	client.denseNormalizedNegotiated = false
	if _, err := client.DenseVectorSearch(ctx, request); nativeCodeOf(err) != iwire.ErrUnsupportedFeature {
		t.Fatalf("unnegotiated normalized request err=%v", err)
	}
}

func TestDenseNormalizedV4ClientCancellationInterruptsActiveRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	client := NewClient(left)
	client.denseNormalizedNegotiated = true
	started := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		_, _, err := readFrame(right, iwire.DefaultLimits())
		close(started)
		if err != nil {
			serverErr <- err
			return
		}
		_, _, err = readFrame(right, iwire.DefaultLimits())
		serverErr <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	errCh := make(chan error, 1)
	go func() {
		_, err := client.DenseVectorSearch(ctx, DenseVectorSearchRequest{
			Index: "normalized", Query: []float32{1, 0}, TopK: 1, EfSearch: 8, ExpectedGeneration: 1,
			VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		})
		errCh <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("server did not receive normalized request")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("normalized cancellation err=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("normalized request did not return after cancellation")
	}
	select {
	case err := <-serverErr:
		if err == nil {
			t.Fatal("server read after normalized cancellation succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe normalized client close")
	}
}

func TestDenseNormalizedV4ErrorInventoryFailsClosed(t *testing.T) {
	server, _, info, ctx := newDenseNormalizedV4Test(t)
	production := DenseVectorSearchRequest{
		Index: info.Name, Query: []float32{1, 0}, TopK: 1, EfSearch: 8, ExpectedGeneration: info.Generation,
		VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
		QueryMode:            collections.VectorIndexQueryModeExact,
	}
	errorSection := iwire.Section{ID: iwire.SectionError, Bytes: appendErrorPayload(nil, iwire.ErrInvalidCommand, false, "test")}
	body, err := iwire.AppendSection(nil, errorSection)
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeWireErrorVersion(body, server.limits, iwire.DenseVectorSearchNormalizedVersion)
	if validated := validateDenseNormalizedFailure(decoded, production); !isRemoteError(decoded, iwire.ErrInvalidCommand) || !isRemoteError(validated, iwire.ErrInvalidCommand) {
		t.Fatalf("bare normalized error=%v", decoded)
	}
	unknown, err := iwire.AppendSection(bytes.Clone(body), iwire.Section{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeWireErrorVersion(unknown, server.limits, iwire.DenseVectorSearchNormalizedVersion); nativeCodeOf(got) != iwire.ErrMalformedFrame {
		t.Fatalf("unknown normalized error section=%v", got)
	}
	flaggedError := errorSection
	flaggedError.Flags = iwire.SectionFlagCritical
	flaggedBody, err := iwire.AppendSection(nil, flaggedError)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeWireErrorVersion(flaggedBody, server.limits, iwire.DenseVectorSearchNormalizedVersion); nativeCodeOf(got) != iwire.ErrMalformedFrame {
		t.Fatalf("normalized error accepted unknown flags: %v", got)
	}
	duplicate, err := iwire.AppendSection(bytes.Clone(body), errorSection)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeWireErrorVersion(duplicate, server.limits, iwire.DenseVectorSearchNormalizedVersion); nativeCodeOf(got) != iwire.ErrInvalidCommand {
		t.Fatalf("duplicate normalized error section=%v", got)
	}

	diagnostic := production
	diagnostic.Diagnostics = true
	sections := denseNormalizedV4Sections(t, server, ctx, diagnostic)
	work := *denseNormalizedV4Section(t, sections, iwire.SectionDenseSearchWork)
	withWork, err := iwire.AppendSection(bytes.Clone(body), work)
	if err != nil {
		t.Fatal(err)
	}
	decoded = decodeWireErrorVersion(withWork, server.limits, iwire.DenseVectorSearchNormalizedVersion)
	if got := validateDenseNormalizedFailure(decoded, production); nativeCodeOf(got) != iwire.ErrMalformedFrame {
		t.Fatalf("production accepted diagnostic error proof: %v", got)
	}
	if got := validateDenseNormalizedFailure(decoded, diagnostic); nativeCodeOf(got) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("exact diagnostic error proof was treated as request-authenticated: %v", got)
	}
	decodedWork, err := decodeDenseWork(work.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	decodedWork.Graph.Snapshot.SchemaGeneration = production.ExpectedGeneration + 1
	mismatchedWorkBytes, err := appendDenseWork(nil, decodedWork)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedBody, err := iwire.AppendSection(bytes.Clone(body), iwire.Section{ID: iwire.SectionDenseSearchWork, Flags: iwire.SectionFlagCritical, Bytes: mismatchedWorkBytes})
	if err != nil {
		t.Fatal(err)
	}
	mismatched := decodeWireErrorVersion(mismatchedBody, server.limits, iwire.DenseVectorSearchNormalizedVersion)
	if got := validateDenseNormalizedFailure(mismatched, diagnostic); nativeCodeOf(got) != iwire.ErrConsistencyUnavailable {
		t.Fatalf("exact diagnostics accepted another generation's failure proof: %v", got)
	}

	quantized := production
	quantized.Diagnostics = true
	quantized.QueryMode = collections.VectorIndexQueryModeQuantizedRerank
	quantized.QuantizedIndexName = "embedding.scalar_u8.v1"
	quantized.QuantizedRerankCandidates = 2
	quantizedSections := denseNormalizedV4Sections(t, server, ctx, quantized)
	scorePlane := *denseNormalizedV4Section(t, quantizedSections, iwire.SectionDenseSearchScorePlaneProof)
	withScore, err := iwire.AppendSection(bytes.Clone(withWork), scorePlane)
	if err != nil {
		t.Fatal(err)
	}
	decoded = decodeWireErrorVersion(withScore, server.limits, iwire.DenseVectorSearchNormalizedVersion)
	if got := validateDenseNormalizedFailure(decoded, diagnostic); nativeCodeOf(got) != iwire.ErrMalformedFrame {
		t.Fatalf("exact diagnostics accepted score-plane error proof: %v", got)
	}
}

func TestDenseNormalizedV4SurvivesServerRestart(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	open := func() (*backenddb.DB, *documentservice.Service, *Server, *Client, func() error) {
		database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(database)
		service := documentservice.New(manager)
		server := NewServer(ServerOptions{Collections: manager, Backend: database, DocumentService: service})
		client, cleanup, err := NewInProcessClient(ctx, server)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Hello(ctx); err != nil {
			t.Fatal(err)
		}
		return database, service, server, client, cleanup
	}
	database, service, server, client, cleanup := open()
	info, err := service.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name: "normalized-restart", Dimension: 2, Metric: documentservice.MetricCosine, TypedInput: true,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{
			Strategy: collections.VectorIndexStrategyColumnGraph, Representation: collections.VectorIndexRepresentationCosineNormalizedF32V1,
			QuantizedIndexes: []documentservice.QuantizedIndexInfo{{Name: "embedding.scalar_u8.v1", Codec: collections.QuantizedVectorCodecScalarU8}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.UpsertDocuments(ctx, info.Name, documentservice.UpsertDocumentsRequest{Documents: []documentservice.Document{
		{ID: "a", Content: "alpha", Embedding: []float32{3, 4}},
		{ID: "b", Content: "beta", Embedding: []float32{0, 2}},
	}, DeferVectorIndexRebuild: true}); err != nil {
		t.Fatal(err)
	}
	options := denseNormalizedV4ServingOptions()
	if _, err := service.OptimizeIndex(ctx, info.Name, documentservice.OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	requests := []DenseVectorSearchRequest{
		{Index: info.Name, Query: []float32{3, 4}, TopK: 2, EfSearch: 8, ExpectedGeneration: info.Generation, VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1, QueryMode: collections.VectorIndexQueryModeExact},
		{Index: info.Name, Query: []float32{3, 4}, TopK: 2, EfSearch: 8, ExpectedGeneration: info.Generation, VectorRepresentation: collections.VectorIndexRepresentationCosineNormalizedF32V1, QueryMode: collections.VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "embedding.scalar_u8.v1", QuantizedRerankCandidates: 2},
	}
	want := make([][]denseNormalizedV4Decision, len(requests))
	for i, request := range requests {
		response, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		want[i] = snapshotDenseNormalizedV4Decisions(response)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, service, server, client, cleanup = open()
	defer func() {
		_ = cleanup()
		_ = server.Close()
		_ = service.Close()
		_ = database.Close()
	}()
	// Serving admission is intentionally process-local. A restarted service
	// re-applies the same bounded policy; it does not rebuild persistent assets.
	if _, err := service.OptimizeIndex(ctx, info.Name, documentservice.OptimizeIndexRequest{ColumnGraphServing: &options}); err != nil {
		t.Fatalf("re-admit serving after restart: %v", err)
	}
	for i, request := range requests {
		response, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			t.Fatalf("restarted request %d: native=%v", i, err)
		}
		assertDenseNormalizedV4SameDecisions(t, want[i], response)
		if response.RouteIdentity == nil || response.DenseWork.Version != 0 || response.ScorePlane != nil {
			t.Fatalf("restarted response %d=%+v", i, response)
		}
	}
}
