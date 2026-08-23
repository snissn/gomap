package collections

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
)

func TestVectorIndexParallelReciprocalLinksPreserveExactTopology4257(t *testing.T) {
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	tests := []struct {
		name            string
		rows, dims      int
		m, construction int
		tieHeavy        bool
	}{
		{name: "tie-heavy", rows: 96, dims: 8, m: 4, construction: 16, tieHeavy: true},
		{name: "multilevel-m8", rows: 192, dims: 8, m: 8, construction: 32},
		{name: "multilevel-m16", rows: 192, dims: 8, m: 16, construction: 64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows := vectorIndexReciprocalParityRows4257(tc.rows, tc.dims, tc.tieHeavy)
			serial := buildVectorIndexReciprocalParity4257(t, rows, tc.m, tc.construction, false, false)
			if serial.maxLevel < 1 {
				t.Fatalf("fixture max level=%d want multilevel graph", serial.maxLevel)
			}
			want := snapshotVectorIndexTopology4257(serial)
			wantSearch := snapshotVectorIndexSearches4257(t, serial, rows)
			for run := 0; run < 3; run++ {
				parallel := buildVectorIndexReciprocalParity4257(t, rows, tc.m, tc.construction, true, false)
				if got := snapshotVectorIndexTopology4257(parallel); !reflect.DeepEqual(got, want) {
					t.Fatalf("parallel run %d changed exact topology", run)
				}
				if got := snapshotVectorIndexSearches4257(t, parallel, rows); !reflect.DeepEqual(got, wantSearch) {
					t.Fatalf("parallel run %d changed exact search results", run)
				}
			}
		})
	}
}

func TestVectorIndexNativeParallelReciprocalLinksPreserveDirtyNodes4243(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	rows := vectorIndexReciprocalParityRows4257(512, 32, false)
	serial := buildVectorIndexReciprocalParity4257(t, rows, 16, 128, false, true)
	parallel := buildVectorIndexReciprocalParity4257(t, rows, 16, 128, true, true)
	if got, want := snapshotVectorIndexTopology4257(parallel), snapshotVectorIndexTopology4257(serial); !reflect.DeepEqual(got, want) {
		t.Fatal("native parallel reciprocal links changed exact topology")
	}
	if len(serial.dirtyNodes) == 0 {
		t.Fatal("native serial fixture did not record dirty nodes")
	}
	if !reflect.DeepEqual(parallel.dirtyNodes, serial.dirtyNodes) {
		t.Fatalf("native parallel dirty nodes differ: got %d want %d", len(parallel.dirtyNodes), len(serial.dirtyNodes))
	}
	if !reflect.DeepEqual(parallel.dirtyDocs, serial.dirtyDocs) {
		t.Fatalf("native parallel dirty documents differ: got %d want %d", len(parallel.dirtyDocs), len(serial.dirtyDocs))
	}
}

func TestVectorIndexReciprocalLinkWorkersAreBounded4257(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	for _, tc := range []struct{ neighbors, workers int }{{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 4}, {20, 8}} {
		if got := vectorIndexReciprocalLinkWorkerCount(tc.neighbors); got != tc.workers {
			t.Fatalf("neighbors=%d workers=%d want %d", tc.neighbors, got, tc.workers)
		}
	}
}

type vectorIndexTopologySnapshot4257 struct {
	entry, maxLevel int
	nodes           []vectorIndexNodeTopologySnapshot4257
}

type vectorIndexNodeTopologySnapshot4257 struct {
	documentID string
	level      int
	neighbors  [][]vectorIndexNeighborTopologySnapshot4257
}

type vectorIndexNeighborTopologySnapshot4257 struct {
	nodeID       int
	distanceBits uint32
}

func snapshotVectorIndexTopology4257(index *VectorIndex) vectorIndexTopologySnapshot4257 {
	out := vectorIndexTopologySnapshot4257{entry: index.entry, maxLevel: index.maxLevel, nodes: make([]vectorIndexNodeTopologySnapshot4257, len(index.nodes))}
	for nodeID := range index.nodes {
		node := &index.nodes[nodeID]
		out.nodes[nodeID] = vectorIndexNodeTopologySnapshot4257{documentID: string(node.documentID), level: node.level, neighbors: make([][]vectorIndexNeighborTopologySnapshot4257, len(node.neighbors))}
		for layer := range node.neighbors {
			out.nodes[nodeID].neighbors[layer] = make([]vectorIndexNeighborTopologySnapshot4257, len(node.neighbors[layer]))
			for neighbor := range node.neighbors[layer] {
				edge := node.neighbors[layer][neighbor]
				out.nodes[nodeID].neighbors[layer][neighbor] = vectorIndexNeighborTopologySnapshot4257{nodeID: edge.nodeID, distanceBits: math.Float32bits(edge.distance)}
			}
		}
	}
	return out
}

func snapshotVectorIndexSearches4257(t *testing.T, index *VectorIndex, rows [][]float32) [][]vectorIndexNeighborTopologySnapshot4257 {
	t.Helper()
	out := make([][]vectorIndexNeighborTopologySnapshot4257, 3)
	for query, row := range []int{0, len(rows) / 2, len(rows) - 1} {
		norm := vectorNormSquared(rows[row])
		prepared, err := prepareFloat32CosineQuery(rows[row], norm)
		if err != nil {
			t.Fatal(err)
		}
		var scratch vectorIndexSearchScratch
		candidates := index.searchCandidatesLocked(rows[row], norm, &prepared, minInt(64, len(rows)), &scratch)
		out[query] = make([]vectorIndexNeighborTopologySnapshot4257, len(candidates))
		for candidate := range candidates {
			out[query][candidate] = vectorIndexNeighborTopologySnapshot4257{nodeID: candidates[candidate].nodeID, distanceBits: math.Float32bits(candidates[candidate].distance)}
		}
	}
	return out
}

func buildVectorIndexReciprocalParity4257(t *testing.T, rows [][]float32, m, construction int, parallel, native bool) *VectorIndex {
	t.Helper()
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: len(rows[0]), M: m, EfConstruction: construction})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(native)
	index.parallelReciprocalLinks = parallel
	for row := range rows {
		if err := index.insertVectorLocked([]byte(fmt.Sprintf("doc-%04d", row)), rows[row]); err != nil {
			t.Fatalf("insert row %d: %v", row, err)
		}
	}
	return index
}

func vectorIndexReciprocalParityRows4257(rows, dims int, tieHeavy bool) [][]float32 {
	out := make([][]float32, rows)
	for row := range out {
		out[row] = make([]float32, dims)
		for dim := range out[row] {
			seed := row
			if tieHeavy {
				seed %= 8
			}
			out[row][dim] = float32(((seed+1)*(dim+3))%17 - 8)
		}
		out[row][row%dims] += 16
	}
	return out
}
