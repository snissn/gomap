package collections

import (
	"bytes"
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTemplateV1UpdateBatchPooledScratchDifferentIndexedSchemas(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "col_a",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "idx_a", Field: "a", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create col_a: %v", err)
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "col_b",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatTemplateV1,
		},
		Indexes: []IndexDefinition{
			{Name: "idx_b", Field: "b", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create col_b: %v", err)
	}

	colA, err := mgr.OpenCollection("col_a")
	if err != nil {
		t.Fatalf("open col_a: %v", err)
	}
	colB, err := mgr.OpenCollection("col_b")
	if err != nil {
		t.Fatalf("open col_b: %v", err)
	}

	initDoc := mustTemplateV1Document(t, []string{"a", "b"}, []any{"a0", "b0"})
	if _, err := colA.InsertBatch([][]byte{[]byte("doc1")}, [][]byte{initDoc}); err != nil {
		t.Fatalf("insert col_a: %v", err)
	}
	if _, err := colB.InsertBatch([][]byte{[]byte("doc1")}, [][]byte{initDoc}); err != nil {
		t.Fatalf("insert col_b: %v", err)
	}

	// Run multiple rounds so updateBatchPlanScratchPool is populated by col_a
	// and reused by col_b (and vice versa) with identical templateID=1 and
	// index count=1 but different indexed fields ("a" vs "b").
	for round := 1; round <= 8; round++ {
		nextA := fmt.Sprintf("a%d", round)
		nextB := fmt.Sprintf("b%d", round)
		prevA := fmt.Sprintf("a%d", round-1)
		prevB := fmt.Sprintf("b%d", round-1)

		repA := mustTemplateV1Document(t, []string{"a", "b"}, []any{nextA, prevB})
		resA, err := colA.UpdateBatch([]UpdateBatchItem{{
			DocumentID: []byte("doc1"),
			Update: func([]byte) ([]byte, bool, error) {
				return repA, true, nil
			},
		}})
		if err != nil {
			t.Fatalf("round %d col_a UpdateBatch: %v", round, err)
		}
		if len(resA) != 1 || !resA[0].Matched || !resA[0].Modified {
			t.Fatalf("round %d col_a UpdateBatch results=%+v want matched+modified", round, resA)
		}

		repB := mustTemplateV1Document(t, []string{"a", "b"}, []any{prevA, nextB})
		resB, err := colB.UpdateBatch([]UpdateBatchItem{{
			DocumentID: []byte("doc1"),
			Update: func([]byte) ([]byte, bool, error) {
				return repB, true, nil
			},
		}})
		if err != nil {
			t.Fatalf("round %d col_b UpdateBatch: %v", round, err)
		}
		if len(resB) != 1 || !resB[0].Matched || !resB[0].Modified {
			t.Fatalf("round %d col_b UpdateBatch results=%+v want matched+modified", round, resB)
		}

		idsA, err := colA.FindByIndex("idx_a", nextA)
		if err != nil {
			t.Fatalf("round %d col_a FindByIndex(%q): %v", round, nextA, err)
		}
		if len(idsA) != 1 || !bytes.Equal(idsA[0], []byte("doc1")) {
			t.Fatalf("round %d col_a FindByIndex(%q)=%q want [doc1]", round, nextA, idsA)
		}
		staleA, err := colA.FindByIndex("idx_a", prevA)
		if err != nil {
			t.Fatalf("round %d col_a FindByIndex(%q): %v", round, prevA, err)
		}
		if len(staleA) != 0 {
			t.Fatalf("round %d col_a stale FindByIndex(%q)=%q want empty", round, prevA, staleA)
		}

		idsB, err := colB.FindByIndex("idx_b", nextB)
		if err != nil {
			t.Fatalf("round %d col_b FindByIndex(%q): %v", round, nextB, err)
		}
		if len(idsB) != 1 || !bytes.Equal(idsB[0], []byte("doc1")) {
			t.Fatalf("round %d col_b FindByIndex(%q)=%q want [doc1]", round, nextB, idsB)
		}
		staleB, err := colB.FindByIndex("idx_b", prevB)
		if err != nil {
			t.Fatalf("round %d col_b FindByIndex(%q): %v", round, prevB, err)
		}
		if len(staleB) != 0 {
			t.Fatalf("round %d col_b stale FindByIndex(%q)=%q want empty", round, prevB, staleB)
		}
	}
}

func TestTemplateV1RootIndexPlanInvalidatesOnSchemaOrResolverChange(t *testing.T) {
	doc1 := mustTemplateV1Document(t, []string{"a", "b"}, []any{"val_a", "val_b"})
	prepared1, _, _, res1, err := prepareTemplateV1InsertDocuments([][]byte{doc1}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare doc1: %v", err)
	}
	doc2 := mustTemplateV1Document(t, []string{"b", "c"}, []any{"val_b2", "val_c2"})
	prepared2, _, _, res2, err := prepareTemplateV1InsertDocuments([][]byte{doc2}, nil, false, true)
	if err != nil {
		t.Fatalf("prepare doc2: %v", err)
	}
	raw1 := prepared1[0]
	raw2 := prepared2[0]
	root1, err := parseTemplateV1StoredDocument(raw1)
	if err != nil {
		t.Fatalf("parse raw1: %v", err)
	}
	root2, err := parseTemplateV1StoredDocument(raw2)
	if err != nil {
		t.Fatalf("parse raw2: %v", err)
	}
	if root1.templateID != root2.templateID {
		t.Fatalf("expected matching template IDs across fresh resolvers, got %d vs %d", root1.templateID, root2.templateID)
	}

	runtimesA := []indexRuntime{{
		def:  indexDefinition{name: "idx_a", field: "a", valueType: IndexValueString},
		path: []string{"a"},
	}}
	runtimesB := []indexRuntime{{
		def:  indexDefinition{name: "idx_b", field: "b", valueType: IndexValueString},
		path: []string{"b"},
	}}
	runtimesNested := []indexRuntime{{
		def:  indexDefinition{name: "idx_nested", field: "a.nested", valueType: IndexValueString},
		path: []string{"a", "nested"},
	}}

	var arena indexEncodeArena

	stateA, err := templateV1OrderedIndexStateForDocumentWithArena(raw1, runtimesA, collectionOptions{templateResolver: res1}, &arena)
	if err != nil {
		t.Fatalf("stateA: %v", err)
	}
	wantA := appendIndexStringComponent(nil, []byte("val_a"))
	if len(stateA) != 1 || len(stateA[0]) != 1 || !bytes.Equal(stateA[0][0], wantA) {
		t.Fatalf("stateA=%x want %x", stateA, wantA)
	}

	// Same arena, same templateID, same resolver, same runtime count, DIFFERENT field ("b").
	stateB, err := templateV1OrderedIndexStateForDocumentWithArena(raw1, runtimesB, collectionOptions{templateResolver: res1}, &arena)
	if err != nil {
		t.Fatalf("stateB: %v", err)
	}
	wantB := appendIndexStringComponent(nil, []byte("val_b"))
	if len(stateB) != 1 || len(stateB[0]) != 1 || !bytes.Equal(stateB[0][0], wantB) {
		t.Fatalf("stateB=%x want %x", stateB, wantB)
	}

	// Same arena, same templateID, same runtimes ("b"), DIFFERENT resolver with swapped field order.
	stateB2, err := templateV1OrderedIndexStateForDocumentWithArena(raw2, runtimesB, collectionOptions{templateResolver: res2}, &arena)
	if err != nil {
		t.Fatalf("stateB2: %v", err)
	}
	wantB2 := appendIndexStringComponent(nil, []byte("val_b2"))
	if len(stateB2) != 1 || len(stateB2[0]) != 1 || !bytes.Equal(stateB2[0][0], wantB2) {
		t.Fatalf("stateB2=%x want %x", stateB2, wantB2)
	}

	// Same arena, nested path must bypass cached root plan cleanly.
	stateNested, err := templateV1OrderedIndexStateForDocumentWithArena(raw1, runtimesNested, collectionOptions{templateResolver: res1}, &arena)
	if err != nil {
		t.Fatalf("stateNested: %v", err)
	}
	if len(stateNested) != 1 || len(stateNested[0]) != 0 {
		t.Fatalf("stateNested=%x want empty", stateNested)
	}
}
