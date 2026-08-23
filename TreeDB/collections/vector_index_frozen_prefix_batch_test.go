package collections

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
)

func TestVectorIndexFrozenPrefixBatchIsDeterministicAndSearchable4297(t *testing.T) {
	rows := vectorIndexReciprocalParityRows4257(192, 16, false)
	want := buildVectorIndexFrozenPrefixBatch4297(t, rows)
	wantTopology := snapshotVectorIndexTopology4257(want)
	wantSearch := snapshotVectorIndexSearches4257(t, want, rows)
	for run := 0; run < 3; run++ {
		got := buildVectorIndexFrozenPrefixBatch4297(t, rows)
		if topology := snapshotVectorIndexTopology4257(got); !reflect.DeepEqual(topology, wantTopology) {
			t.Fatalf("run %d changed frozen-prefix topology", run)
		}
		if search := snapshotVectorIndexSearches4257(t, got, rows); !reflect.DeepEqual(search, wantSearch) {
			t.Fatalf("run %d changed frozen-prefix search", run)
		}
	}
}

func TestVectorIndexFrozenPrefixBatchFallsBackForUpdates4297(t *testing.T) {
	rows := vectorIndexReciprocalParityRows4257(64, 8, false)
	index := buildVectorIndexFrozenPrefixBatch4297(t, rows)
	beforeBatches := index.frozenPrefixBatches
	oldNode := index.currentNode["doc-0000"]
	ids := [][]byte{[]byte("doc-0000"), []byte("new")}
	vectors := [][]float32{append([]float32(nil), rows[1]...), append([]float32(nil), rows[2]...)}
	vectors[0][0] += 7
	vectors[1][0] += 11
	if err := index.insertVectorBatchLocked(ids, vectors); err != nil {
		t.Fatal(err)
	}
	if index.frozenPrefixBatches != beforeBatches {
		t.Fatal("update group used frozen-prefix construction")
	}
	if !index.nodes[oldNode].deleted || index.currentNode["doc-0000"] == oldNode {
		t.Fatal("serial fallback did not replace the existing document")
	}
	if _, ok := index.currentNode["new"]; !ok {
		t.Fatal("serial fallback did not insert the new document")
	}
}

func TestVectorIndexFrozenPrefixBatchValidatesBeforeMutation4297(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	err = index.insertVectorBatchLocked(
		[][]byte{[]byte("valid"), []byte("invalid")},
		[][]float32{{1, 0}, {1}},
	)
	if err == nil {
		t.Fatal("invalid vector batch succeeded")
	}
	if len(index.nodes) != 0 || index.entry != -1 || len(index.currentNode) != 0 {
		t.Fatalf("invalid batch mutated graph: nodes=%d entry=%d current=%d", len(index.nodes), index.entry, len(index.currentNode))
	}
}

func TestVectorIndexFrozenPrefixBatchKeepsSmallBatchesSerial4297(t *testing.T) {
	rows := vectorIndexReciprocalParityRows4257(nativeVectorFrozenPrefixBatchMinimum-1, 8, false)
	ids := make([][]byte, len(rows))
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%04d", row))
	}
	indexes := make([]*VectorIndex, 2)
	for i := range indexes {
		var err error
		indexes[i], err = newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 8, M: 16, EfConstruction: 64})
		if err != nil {
			t.Fatal(err)
		}
		indexes[i].setNativePersistent(true)
	}
	if err := indexes[0].insertVectorBatchLocked(ids, rows); err != nil {
		t.Fatal(err)
	}
	for row := range rows {
		if err := indexes[1].insertVectorLocked(ids[row], rows[row]); err != nil {
			t.Fatal(err)
		}
	}
	if indexes[0].frozenPrefixBatches != 0 {
		t.Fatal("small batch used frozen-prefix construction")
	}
	if !reflect.DeepEqual(snapshotVectorIndexTopology4257(indexes[0]), snapshotVectorIndexTopology4257(indexes[1])) {
		t.Fatal("small batch changed serial topology")
	}
}

func BenchmarkVectorIndexFrozenPrefixBatch4297(b *testing.B) {
	const dimensions = 768
	rows := vectorIndexReciprocalParityRows4257(10_000, dimensions, false)
	ids := make([][]byte, len(rows))
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%05d", row))
	}
	for _, candidate := range []bool{false, true} {
		b.Run(fmt.Sprintf("candidate=%v", candidate), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dimensions, M: 16, EfConstruction: 128})
				if err != nil {
					b.Fatal(err)
				}
				index.setNativePersistent(true)
				if candidate {
					err = index.insertVectorBatchLocked(ids, rows)
				} else {
					for row := range rows {
						if err = index.insertVectorLocked(ids[row], rows[row]); err != nil {
							break
						}
					}
				}
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(index)
			}
		})
	}
}

func buildVectorIndexFrozenPrefixBatch4297(t *testing.T, rows [][]float32) *VectorIndex {
	t.Helper()
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: len(rows[0]), M: 16, EfConstruction: 64})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	ids := make([][]byte, len(rows))
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%04d", row))
	}
	if err := index.insertVectorBatchLocked(ids, rows); err != nil {
		t.Fatal(err)
	}
	if index.frozenPrefixBatches == 0 {
		t.Fatal("frozen-prefix path was not used")
	}
	return index
}
