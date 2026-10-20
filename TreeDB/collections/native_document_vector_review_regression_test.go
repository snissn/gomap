package collections

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A small algorithm fixture isolates routing/result limits without asserting
// import authority. The public control below uses the actual durable producer.
func nativeDocumentReviewGraph(t *testing.T) *nativeDocumentGraph {
	t.Helper()
	g := &nativeDocumentGraph{
		config: &VectorIndex{metric: VectorMetricCosine, m: 2},
		nodes:  make(map[int]*nativeDocumentGraphNode),
		admit:  func(uint64) error { return nil },
	}
	g.meta.NativeDocument = &nativeDocumentGraphSummary{Nodes: 3, Live: 1}
	for i, vector := range [][]float32{{1, 0}, {1, 0.1}, {-1, 0}} {
		n := &nativeDocumentGraphNode{value: vectorIndexNode{documentID: []byte(fmt.Sprint(i)), vector: vector, deleted: i != 2, neighbors: make([][]vectorIndexNeighbor, 1)}}
		n.value.cacheVectorNorms()
		g.nodes[i] = n
	}
	g.nodes[0].value.neighbors[0] = []vectorIndexNeighbor{{nodeID: 1}, {nodeID: 2}}
	return g
}

func TestNativeDocumentVectorStaleRoutingBudget(t *testing.T) {
	g := nativeDocumentReviewGraph(t)
	limit := g.currentSearchExplorationLimit(1)
	if limit != 3 {
		t.Fatalf("routing limit=%d, want original live limit1 plus2 stale nodes", limit)
	}
	got, err := g.searchLayerMode([]float32{1, 0}, 0, 1, limit, 0, true)
	if err != nil || len(got) != 1 || got[0].nodeID != 2 {
		t.Fatalf("close tombstones displaced reachable live answer: %+v: %v", got, err)
	}
	// Historical churn may add at most one original layer-0 degree fanout per
	// requested live result; no graph-high-water-sized search allocation/scan.
	g.meta.NativeDocument.Nodes, g.meta.NativeDocument.Live = 1000000, 1
	if got := g.currentSearchExplorationLimit(2); got != 2*g.config.maxNeighborsForLayer(0) {
		t.Fatalf("unbounded stale allowance=%d", got)
	}
}

func TestNativeDocumentVectorUpperRoutingBudget(t *testing.T) {
	for _, chain := range []bool{false, true} {
		t.Run(fmt.Sprintf("chain=%v", chain), func(t *testing.T) {
			g := nativeDocumentReviewGraph(t)
			g.config.m = 4
			g.meta.Entry, g.meta.MaxLevel = 0, 1
			g.meta.NativeDocument.Nodes, g.meta.NativeDocument.Live = 1000, 2
			// The live original entry has a useful layer-0 route. Its closer
			// upper-layer tombstone is a dead end unless both routes are seeded.
			g.nodes[0].value.deleted = false
			g.nodes[0].value.vector = []float32{-1, 0}
			g.nodes[0].value.level = 1
			g.nodes[0].value.neighbors = [][]vectorIndexNeighbor{{{nodeID: 2}}, {{nodeID: 1}}}
			g.nodes[1].value.vector = []float32{1, 0}
			g.nodes[1].value.level = 1
			g.nodes[1].value.neighbors = make([][]vectorIndexNeighbor, 2)
			g.nodes[2].value.vector = []float32{0, 1}
			if chain {
				g.meta.NativeDocument.Live = 1
				g.nodes[2].value.deleted = true
				// Strictly improving level-1 tombstones would make construction's
				// unbounded greedy walk visit the entire historical chain.
				for i := 1; i < 1000; i++ {
					n := &nativeDocumentGraphNode{value: vectorIndexNode{vector: []float32{1, 1 / float32(i)}, deleted: true, level: 1, neighbors: make([][]vectorIndexNeighbor, 2)}}
					if i+1 < 1000 {
						n.value.neighbors[1] = []vectorIndexNeighbor{{nodeID: uint32(i + 1)}}
					}
					g.nodes[i] = n
				}
			}
			for _, n := range g.nodes {
				n.value.cacheVectorNorms()
			}
			// Nodes are already decoded in this algorithm fixture, so each
			// callback is traversal admission, not cold import validation.
			admissions := 0
			g.admit = func(uint64) error { admissions++; return nil }
			got, err := g.currentSearchCandidates([]float32{1, 0}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if admissions > 4*g.currentSearchExplorationLimit(1) {
				t.Fatalf("upper traversal escaped shared allowance: admissions=%d", admissions)
			}
			if !chain && (len(got) != 1 || got[0].nodeID != 2) {
				t.Fatalf("upper tombstone displaced original live route: %+v", got)
			}
		})
	}
}

func TestNativeDocumentVectorDeletedNeighborBackfill(t *testing.T) {
	g := nativeDocumentReviewGraph(t)
	candidates := []vectorIndexCandidate{{nodeID: 0, distance: 0}, {nodeID: 1, distance: 0.1}, {nodeID: 2, distance: 2}}
	got, err := g.selectNeighbors(candidates, 3, -1)
	if err != nil || len(got) != 3 || got[0].nodeID != 2 || got[1].nodeID != 0 || got[2].nodeID != 1 {
		t.Fatalf("live-first selection lost deleted routing backfill: %+v: %v", got, err)
	}
	// This same selector is used for reciprocal pruning. Even an all-deleted
	// candidate pool must retain its bounded original waypoints.
	got, err = g.selectNeighbors(candidates[:2], 1, -1)
	if err != nil || len(got) != 1 || got[0].nodeID != 0 {
		t.Fatalf("reciprocal pruning severed only routing waypoint: %+v: %v", got, err)
	}
}

func TestNativeDocumentVectorChurnSearchAndMaterialization(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			database, _, _, col := openNativeDocumentContract(t, format)
			document := func(x, y float64) []byte {
				t.Helper()
				var raw []byte
				var err error
				if format == DocumentFormatBSON {
					raw, err = bson.Marshal(bson.D{{Key: "embedding", Value: bson.A{x, y}}})
				} else {
					raw, err = json.Marshal(map[string]any{"embedding": []float64{x, y}})
				}
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			a, b := document(1, 0), document(0, 1)
			if _, err := col.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{a, b}); err != nil {
				t.Fatal(err)
			}
			held, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: "embedding"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Error(err)
				}
			})
			for i := 0; i < 12; i++ {
				next := document(1, float64(i+1)/100)
				if _, changed, err := col.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return next, true, nil }); err != nil || !changed {
					t.Fatalf("churn%d changed=%v: %v", i, changed, err)
				}
			}
			currentA := document(-1, 0)
			if _, changed, err := col.Update([]byte("a"), func([]byte) ([]byte, bool, error) { return currentA, true, nil }); err != nil || !changed {
				t.Fatalf("final change=%v: %v", changed, err)
			}
			assertResults := func(response VectorIndexSearchResponse, wantA []byte) {
				t.Helper()
				if response.Path != VectorIndexSearchPathNativeRuntime || len(response.Results) != 2 {
					t.Fatalf("bounded live results=%+v", response)
				}
				want := map[string][]byte{"a": wantA, "b": b}
				var bytesFetched uint64
				for _, hit := range response.Results {
					doc, ok := want[string(hit.ID)]
					if !ok || !bytes.Equal(hit.Document, doc) {
						t.Fatalf("wrong/stale materialized hit=%+v", hit)
					}
					delete(want, string(hit.ID))
					bytesFetched += uint64(len(doc))
				}
				if response.Stats.DocumentsFetched != 2 || response.Stats.DocumentBytes != bytesFetched || response.Stats.DocumentOutputBytes != bytesFetched {
					t.Fatalf("materialization stats=%+v, want2 documents/%d bytes", response.Stats, bytesFetched)
				}
			}
			for _, checkpoint := range []bool{false, true} {
				if checkpoint {
					if err := database.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				current, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 2, EfSearch: 2, IncludeDocuments: true})
				if err != nil {
					t.Fatal(err)
				}
				assertResults(current, currentA)
				old, err := held.Search(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 2, EfSearch: 2, IncludeDocuments: true})
				if err != nil {
					t.Fatal(err)
				}
				assertResults(old, a)
				var buffer VectorIndexSearchBuffer
				buffered, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: "embedding", Query: []float32{1, 0}, TopK: 2, EfSearch: 2}, &buffer)
				if err != nil || len(buffered.Results) != 2 || buffered.Stats.DocumentsFetched != 0 || buffered.Stats.DocumentBytes != 0 || buffered.Stats.DocumentOutputBytes != 0 {
					t.Fatalf("buffered live results/stats=%+v: %v", buffered, err)
				}
			}
		})
	}
}
