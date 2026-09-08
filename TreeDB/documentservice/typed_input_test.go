package documentservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
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

func requireTypedServiceServingTest(t testing.TB) {
	t.Helper()
	// Exact namespace support and column asset mmap share the supported Unix
	// platforms; prepared numeric views also require native little-endian data.
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("selected serving requires exact relative namespace support")
	}
	var native [2]byte
	binary.NativeEndian.PutUint16(native[:], 1)
	if native[0] != 1 {
		t.Skip("selected serving requires little-endian mmap-direct prepared views")
	}
}

func TestServiceTypedInputServingLifecycle(t *testing.T) {
	requireTypedServiceServingTest(t)

	beforeWork := workstats.Read()
	defer func() {
		after := workstats.Read()
		if after.IndexedJSON != beforeWork.IndexedJSON || after.Runtime != beforeWork.Runtime || after.Scans.DenseExact != beforeWork.Scans.DenseExact {
			t.Errorf("selected route performed forbidden work: before=%+v after=%+v", beforeWork, after)
		}
		if after.Typed.ScalarRows <= beforeWork.Typed.ScalarRows || after.Typed.TextRows <= beforeWork.Typed.TextRows || after.Typed.TextOldRows <= beforeWork.Typed.TextOldRows {
			t.Errorf("typed producers did not observe lifecycle: before=%+v after=%+v", beforeWork.Typed, after.Typed)
		}
	}()

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
	options.SearchCandidates = 1
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
	encoded, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &public); err != nil || len(public["dense_work"]) == 0 {
		t.Fatalf("selected public response omitted local work proof: %s err=%v", encoded, err)
	}
	work := out.DenseWork
	if work == nil || !work.Completed || !work.Graph.Completed || work.Graph.Route != "typed_exact" || work.Graph.ExactBaseScored != 1 || !work.Graph.Filter.Completed || work.Graph.Filter.EligibleRows != 1 || !work.Graph.Snapshot.Available || work.Graph.Snapshot.SchemaHash == 0 || work.Output.Fetched != 1 || !work.Output.Completed || work.Output.OutputBytes == 0 {
		t.Fatalf("missing exact/snapshot/output work: %+v", work)
	}
	// Reuse the service counting context to stop inside typed filter work.
	// The ordinary HTTP handler must preserve that same request context and
	// serialize the owned positive error prefix without partial documents.
	counted := &cancelAfterContextChecks{Context: ctx, cancelAfter: int(^uint(0) >> 1)}
	if _, err := svc.SearchDenseVector(counted, create.Name, query); err != nil {
		t.Fatal(err)
	}
	prefixCheck := 0
	for check := 1; check < counted.checks; check++ {
		cancel := &cancelAfterContextChecks{Context: ctx, cancelAfter: check}
		got, err := svc.SearchDenseVector(cancel, create.Name, query)
		if !errors.Is(err, context.Canceled) || len(got.Documents) != 0 {
			t.Fatalf("dense request cancellation check=%d err=%v result=%+v", check, err, got)
		}
		var observed *Error
		if errors.As(err, &observed) && observed.DenseWork != nil {
			work := observed.DenseWork
			if work.Graph.Filter.SourceIDs > 0 && !work.Graph.Filter.Completed {
				if work.Completed || work.Graph.Completed || work.Output.Attempted {
					t.Fatalf("canceled filter claimed completion: %+v", work)
				}
				prefixCheck = check
				break
			}
		}
	}
	if prefixCheck == 0 {
		t.Fatalf("no service filter cancellation prefix in %d checks", counted.checks)
	}
	requestJSON, err := json.Marshal(query)
	if err != nil {
		t.Fatal(err)
	}
	cancel := &cancelAfterContextChecks{Context: ctx, cancelAfter: prefixCheck}
	httpRequest := httptest.NewRequestWithContext(cancel, http.MethodPost, "/v1/indexes/"+create.Name+"/search/vector", bytes.NewReader(requestJSON))
	recorder := httptest.NewRecorder()
	NewHandler(svc).ServeHTTP(recorder, httpRequest)
	var errorEnvelope struct {
		Error struct {
			DenseWork *DenseSearchWork `json:"dense_work"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &errorEnvelope); err != nil {
		t.Fatal(err)
	}
	httpWork := errorEnvelope.Error.DenseWork
	if recorder.Code < 400 || httpWork == nil || httpWork.Completed || httpWork.Graph.Completed || httpWork.Graph.Filter.Completed || httpWork.Graph.Filter.SourceIDs == 0 || httpWork.Output.Attempted {
		t.Fatalf("HTTP canceled prefix: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := svc.SearchDenseVector(ctx, create.Name, query); err != nil {
		t.Fatalf("retry after service/HTTP cancellation: %v", err)
	}
	unfiltered := query
	unfiltered.Filter = nil
	failed, err := svc.SearchDenseVector(ctx, create.Name, unfiltered)
	var observed *Error
	if !errors.Is(err, collections.ErrColumnGraphSearchBudget) || len(failed.Documents) != 0 || !errors.As(err, &observed) || observed.DenseWork == nil || observed.DenseWork.Completed || observed.DenseWork.Graph.Completed || observed.DenseWork.Graph.BaseCandidates != 1 || observed.DenseWork.Graph.BaseANNScored != 1 || observed.DenseWork.Graph.Route != "typed_hnsw" || !observed.DenseWork.Graph.Snapshot.Available || observed.DenseWork.Output.Attempted {
		t.Fatalf("candidate-budget error lost local prefix: response=%+v err=%+v", failed, err)
	}
	// Failure after successful scoring retains graph completion and no output.
	svc.denseVectorNativeAfterSearch = func(_ int, _ collections.VectorIndexSearchResponse) error { return context.Canceled }
	failed, err = svc.SearchDenseVector(ctx, create.Name, query)
	svc.denseVectorNativeAfterSearch = nil
	if !errors.Is(err, context.Canceled) || len(failed.Documents) != 0 || !errors.As(err, &observed) || observed.DenseWork == nil || observed.DenseWork.Completed || !observed.DenseWork.Graph.Completed || observed.DenseWork.Graph.ExactBaseScored != 1 || observed.DenseWork.Output.Attempted {
		t.Fatalf("post-search failure lost completed graph: response=%+v err=%+v", failed, err)
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
	if out.DenseWork == nil || out.DenseWork.Graph.Snapshot != work.Graph.Snapshot {
		t.Fatalf("held response proof followed newer publication: before=%+v after=%+v", work, out.DenseWork)
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
