package collections

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

// Repeated public semantic publications must request the existing checkpoint
// owner when source slots exhaust, even below the byte maintenance watermark.
func TestNativeHNSWPublicSourcePressure(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 8, 2, profile)
			defer closeAll()
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			held, err := c.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			ids := [][]byte{[]byte("row-00000")}
			before, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			budget := held.nativeRead.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
			for i := 0; i < treedb.DefaultCOWMemtableLimits().MaxSources+4; i++ {
				beforeSave := budget.Stats()
				if _, err := idx.SaveSnapshot(); err != nil {
					t.Fatalf("save %d: %v; before=%+v stats=%+v limits=%+v", i, err, beforeSave, budget.Stats(), budget.Limits())
				}
			}
			after, err := held.FetchDocumentsByID(ids, DocumentFetchOptions{})
			if err != nil || !before.Results[0].Found || !after.Results[0].Found || !bytes.Equal(before.Results[0].Document, after.Results[0].Document) {
				t.Fatalf("held document changed: before=%+v after=%+v err=%v", before, after, err)
			}
			if rows, _, err := idx.Search([]float32{1, 1}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true}); err != nil || len(rows) != 1 {
				t.Fatalf("search after source pressure: rows=%v err=%v", rows, err)
			}
		})
	}
}
