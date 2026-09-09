package documentservice

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestServiceTypedNativeUpsertContract(t *testing.T) {
	s, db := newTestService(t)
	defer db.Close()
	defer s.Close()
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "binary", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.user_id", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	req := TypedDocumentsRequest{ExpectedGeneration: info.Generation, IDs: [][]byte{[]byte("a")}, Retained: [][]byte{[]byte(`{"id":"a","meta":{"extra":"owned"}}`)}, Columns: []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}}, {Name: "content", Strings: []string{"alpha"}}, {Name: "meta.user_id", Strings: []string{"u"}}}}
	out, err := s.UpsertTypedDocuments(ctx, info.Name, req)
	if err != nil || out.Inserted != 1 {
		t.Fatalf("insert=%+v %v", out, err)
	}
	out, err = s.UpsertTypedDocuments(ctx, info.Name, req)
	if err != nil || out.Updated != 1 || out.Inserted != 0 {
		t.Fatalf("noop=%+v %v", out, err)
	}
	for _, name := range []string{"generation", "missing", "duplicate", "unknown", "dimension", "residual"} {
		t.Run(name, func(t *testing.T) {
			bad := req
			bad.Columns = append([]collections.TypedColumnBatch(nil), req.Columns...)
			switch name {
			case "generation":
				bad.ExpectedGeneration++
			case "missing":
				bad.Columns = bad.Columns[:2]
			case "duplicate":
				bad.Columns[2].Name = "content"
			case "unknown":
				bad.Columns[2].Name = "meta.other"
			case "dimension":
				bad.Columns[0].Float32Vectors = [][]float32{{1}}
			case "residual":
				bad.Retained = [][]byte{[]byte(`{"id":"a","content":"sneaked"}`)}
			}
			if _, err := s.UpsertTypedDocuments(ctx, info.Name, bad); err == nil {
				t.Fatal("invalid typed request accepted")
			}
		})
	}
}

func TestServiceTypedNativeUpsertPublishesLastCompletedInsertStats(t *testing.T) {
	s, db := newTestService(t)
	defer db.Close()
	defer s.Close()
	s.DiagnosticsHandler(nil)
	ctx := context.Background()
	info, err := s.CreateIndex(ctx, CreateIndexRequest{Name: "typed-stats", Dimension: 2, TypedInput: true, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph}})
	if err != nil {
		t.Fatal(err)
	}
	request := func(id string) TypedDocumentsRequest {
		return TypedDocumentsRequest{
			ExpectedGeneration: info.Generation,
			IDs:                [][]byte{[]byte(id)},
			Retained:           [][]byte{[]byte(`{"id":"` + id + `"}`)},
			Columns: []collections.TypedColumnBatch{
				{Name: "embedding", Float32Vectors: [][]float32{{1, 0}}},
				{Name: "content", Strings: []string{"text"}},
			},
		}
	}
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, request("first")); err != nil {
		t.Fatal(err)
	}
	first := s.DiagnosticsSnapshot(nil).LastOpened
	if first == nil || first.Insert.Documents != 1 || first.Insert.SourceReplacementPlan <= 0 || first.Insert.Publish <= 0 || first.Insert.ColumnPublishRows != 1 || first.Insert.ColumnPublishBuildColumnDelta <= 0 || first.Insert.ColumnPublishCommit <= 0 || first.Insert.ColumnPublishCommitExclusiveTotal() <= 0 || first.Insert.ColumnPublishManifestBytes <= 0 || first.Insert.ColumnPublishFinalizeCandidateResourceWork.SourceEntriesInspected == 0 {
		t.Fatalf("first typed insert diagnostics=%+v", first)
	}
	secondRequest := request("second")
	secondRequest.IDs = append(secondRequest.IDs, []byte("third"))
	secondRequest.Retained = append(secondRequest.Retained, []byte(`{"id":"third"}`))
	secondRequest.Columns[0].Float32Vectors = append(secondRequest.Columns[0].Float32Vectors, []float32{0, 1})
	secondRequest.Columns[1].Strings = append(secondRequest.Columns[1].Strings, "more text")
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, secondRequest); err != nil {
		t.Fatal(err)
	}
	second := s.DiagnosticsSnapshot(nil).LastOpened
	if second == nil || second.Insert.Documents != 2 || second.Insert.SourceReplacementPlan <= 0 || second.Insert.Publish <= 0 || second.Insert.ColumnPublishRows != 2 || second.Insert.ColumnPublishBuildColumnDelta <= 0 || second.Insert.ColumnPublishCommit <= 0 || second.Insert.ColumnPublishCommitExclusiveTotal() <= 0 || second.Insert.ColumnPublishManifestBytes <= 0 || second.Insert.ColumnPublishFinalizeCandidateResourceWork.SourceEntriesInspected == 0 {
		t.Fatalf("second typed insert diagnostics=%+v", second)
	}
	if first.Insert.Documents != 1 {
		t.Fatalf("first snapshot changed after second completion: %+v", first.Insert)
	}
	failed := request("failed")
	failed.ExpectedGeneration++
	if _, err := s.UpsertTypedDocuments(ctx, info.Name, failed); err == nil {
		t.Fatal("stale typed upsert succeeded")
	}
	if after := s.DiagnosticsSnapshot(nil).LastOpened; after == nil || after.Insert.Documents != 2 {
		t.Fatalf("failed request replaced last completed diagnostics: %+v", after)
	}
}
