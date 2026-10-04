package collections

import (
	"fmt"
	"testing"
)

func TestNativePublishedDuplicateVectorsRemainSearchableAfterDeletes(t *testing.T) {
	for _, order := range []string{"ascending", "descending"} {
		t.Run(order, func(t *testing.T) {
			index, err := newVectorIndex(nil, VectorIndexOptions{
				Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine,
				Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			index.setNativePersistent(true)
			index.sourceDocumentRootsValid = true
			insert := func(id string, vector []float32) {
				t.Helper()
				if err := index.insertVectorLocked([]byte(id), vector); err != nil {
					t.Fatal(err)
				}
			}
			// Match the public partition delta: 32 changed replacements, restore,
			// then 32 distinct IDs with the same vector. The immutable base is separate.
			for i := 0; i < 32; i++ {
				vector := []float32{0, 1}
				if i%2 != 0 {
					vector = []float32{1, 0}
				}
				insert("base-minus-x", vector)
			}
			insert("base-minus-x", []float32{-1, 0})
			for i := 0; i < 32; i++ {
				insert(fmt.Sprintf("cost-delete-%02d", i), []float32{0, 1})
			}
			for i := 0; i < 32; i++ {
				deleted := i
				if order == "descending" {
					deleted = 31 - i
				}
				index.tombstoneDocumentIDLocked([]byte(fmt.Sprintf("cost-delete-%02d", deleted)))
				index.acknowledgeSearchViewStateLocked()
				index.publishSearchViewLocked(false)
				view := index.acquireSearchView()
				var buffer VectorIndexSearchBuffer
				got, err := view.searchGraphOnlyWithBuffer([]float32{0, 1}, 3, 16, &buffer)
				index.releaseSearchView(view)
				if err != nil {
					t.Fatalf("delete %d (%s): %v", i, order, err)
				}
				wantCount := minInt(3, len(index.currentNode))
				if len(got) != wantCount {
					t.Fatalf("delete %d: got %d results, want %d", i, len(got), wantCount)
				}
				for _, result := range got {
					ordinal, ok := index.currentNode[string(result.ID)]
					if !ok || index.nodes[ordinal].deleted {
						t.Fatalf("delete %d returned retired ID %q", i, result.ID)
					}
				}
			}
		})
	}
}
