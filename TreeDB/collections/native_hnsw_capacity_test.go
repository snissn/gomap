package collections

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Actual public owners exercise descriptor growth, deterministic upper layers,
// a degree beyond the legacy eager cap, mutation, and lazy immutable images.
func TestNativeHNSWRetainedCapacity(t *testing.T) {
	for _, m := range []int{4, 16, 65} {
		t.Run(fmt.Sprintf("M%d", m), func(t *testing.T) {
			c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 257, 8)
			defer closeAll()
			if _, err := c.DropVectorIndex(def.Name); err != nil {
				t.Fatal(err)
			}
			def.M, def.EfConstruction, def.EfSearch = m, 137, 93
			if _, err := c.CreateVectorIndex(def); err != nil {
				t.Fatal(err)
			}
			idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
			if err != nil {
				t.Fatal(err)
			}
			witness := func() {
				t.Helper()
				idx.mu.RLock()
				defer idx.mu.RUnlock()
				var concrete uint64
				add := func(n uint64) { concrete += memtable.COWAllocationCharge(n) }
				add(uint64(cap(idx.nodes)) * uint64(unsafe.Sizeof(vectorIndexNode{})))
				upper := false
				for _, plane := range []*VectorIndex{idx, idx.liveDelta} {
					if plane == nil {
						continue
					}
					if plane.vectorRows != nil {
						t.Fatal("native graph retained optional matrix")
					}
					for _, row := range plane.nodes {
						upper = upper || row.level > 0
						add(uint64(cap(row.documentID)))
						add(uint64(cap(row.vector)) * 4)
						add(uint64(cap(row.neighbors)) * uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))
						for layer, neighbors := range row.neighbors {
							if cap(neighbors) != plane.maxNeighborsForLayer(layer)+1 {
								t.Fatalf("layer%d capacity=%d limit=%d", layer, cap(neighbors), idx.maxNeighborsForLayer(layer))
							}
							add(uint64(cap(neighbors)) * uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
						}
					}
				}
				if !upper {
					t.Fatal("fixture did not reach an upper layer")
				}
				if idx.searchViewSpare.Load() != nil {
					t.Fatal("native runtime retained spare graph image")
				}
				if view := idx.searchView.Load(); view != nil {
					add(uint64(cap(view.nodes)) * uint64(unsafe.Sizeof(vectorIndexNode{})))
					for _, row := range view.nodes {
						add(uint64(cap(row.neighbors)) * uint64(unsafe.Sizeof([]vectorIndexNeighbor{})))
						for _, neighbors := range row.neighbors {
							add(uint64(cap(neighbors)) * uint64(unsafe.Sizeof(vectorIndexNeighbor{})))
						}
					}
				}
				scratch := &idx.insertScratch
				scratchBytes := uint64(cap(scratch.visitedEpochs)) * 4
				for _, n := range []int{cap(scratch.queue), cap(scratch.best), cap(scratch.liveBest), cap(scratch.out), cap(scratch.resumeCandidates), cap(scratch.resumeDeferred)} {
					scratchBytes += uint64(n) * uint64(unsafe.Sizeof(vectorIndexCandidate{}))
				}
				if scratchBytes > idx.nativeHNSWConstructionPeak {
					t.Fatalf("retained scratch=%d peak=%d", scratchBytes, idx.nativeHNSWConstructionPeak)
				}
				if concrete+scratchBytes > idx.nativeHNSWCharge {
					t.Fatalf("concrete retained backings=%d scratch=%d charge=%d", concrete, scratchBytes, idx.nativeHNSWCharge)
				}
			}
			witness()
			query := []float32{1, 2, 3, 4, 5, 6, 7, 8}
			if _, _, err = idx.Search(query, VectorIndexSearchOptions{TopK: 7, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			witness()
			doc, _ := bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{8., 7., 6., 5., 4., 3., 2., 1.}}})
			if _, err = c.Insert([]byte("additional"), doc); err != nil {
				t.Fatal(err)
			}
			if idx.searchViewSpare.Load() != nil || idx.searchViewCurrent.Load() {
				t.Fatal("native mutation retained spare or marked old image current")
			}
			if _, _, err = idx.Search(query, VectorIndexSearchOptions{TopK: 7, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			witness()
			ids, docs := make([][]byte, 32), make([][]byte, 32)
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("fold-%02d", i))
				docs[i] = doc
			}
			if _, err = c.InsertBatch(ids, docs); err != nil {
				t.Fatal(err)
			}
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if idx.liveDelta != nil {
				t.Fatal("save did not fold live delta")
			}
			witness()
		})
	}
}
