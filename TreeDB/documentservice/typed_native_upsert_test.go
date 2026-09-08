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
