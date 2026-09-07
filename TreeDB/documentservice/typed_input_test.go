package documentservice

import (
	"context"
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
	// Until typed admission is connected, never fall into JSON preparation.
	if _, err := svc.UpsertDocuments(context.Background(), req.Name, UpsertDocumentsRequest{Documents: []Document{{ID: "a"}}}); ErrorCodeOf(err) != CodeUnsupported {
		t.Fatalf("unavailable typed input must fail closed: %v", err)
	}
}
