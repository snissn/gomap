package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestColumnGraphCoverageAdmissionRemainsShared(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	c, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := c.CreateVectorIndex(VectorIndexDefinition{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyColumnGraph,
	}); err != nil {
		t.Fatalf("create column graph: %v", err)
	}
	if len(c.registeredColumnGraphPartitionLiveCarriers()) != 0 {
		t.Fatal("metadata-only graph unexpectedly registered a carrier")
	}

	unlockFirst := c.lockVectorIndexCoverageMutation()
	firstLocked := true
	defer func() {
		if firstLocked {
			unlockFirst()
		}
	}()
	secondAcquired := make(chan func(), 1)
	go func() { secondAcquired <- c.lockVectorIndexCoverageMutation() }()
	select {
	case unlockSecond := <-secondAcquired:
		unlockSecond()
	case <-time.After(5 * time.Second):
		t.Fatal("column-graph write admission was serialized")
	}
	unlockFirst()
	firstLocked = false
}

func TestCommitAmbiguousErrorPreservesExistingAmbiguousError(t *testing.T) {
	inner := commitAmbiguousError("flush", errors.New("boom"))
	got := commitAmbiguousError("insert", inner)
	if got != inner {
		t.Fatalf("nested ambiguous error=%v want original %v", got, inner)
	}
	if !errors.Is(got, ErrCommitAmbiguous) {
		t.Fatalf("ambiguous error=%v want ErrCommitAmbiguous", got)
	}
}

func TestCollectionVectorSearchExactReadsBSONNumericVectors(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{int32(1), int64(0)}}, {Key: "name", Value: "a"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{int32(0), int64(1)}}, {Key: "name", Value: "b"}}),
		},
	); err != nil {
		t.Fatalf("insert BSON docs: %v", err)
	}
	results, err := col.SearchVectorsExact([]float32{0, 1}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("exact BSON vector search: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "a")
}

func TestVectorIndexStatusRootIDUsesNewestOverlayRoot(t *testing.T) {
	rootName := collectionVectorIndexRootName("docs", "embedding")
	catalog := &collectionCatalog{
		rootOverlays: map[string][]uint64{
			rootName: {30, 20},
		},
	}
	rootID, overlayRootIDs := vectorIndexStatusRootID(catalog, rootName)
	if rootID != 30 {
		t.Fatalf("status root=%d want newest overlay root 30", rootID)
	}
	if len(overlayRootIDs) != 2 || overlayRootIDs[0] != 30 || overlayRootIDs[1] != 20 {
		t.Fatalf("overlay roots=%v want [30 20]", overlayRootIDs)
	}
}

func TestVectorIndexLoadPersistSnapshotRecomputesNonFiniteEdgeDistances(t *testing.T) {
	index := newColumnGraphCodecFixture5275(t)
	snapshot, _ := index.persistSnapshot()
	for i := range snapshot.Edges {
		if len(snapshot.Edges[i].Distances) > 0 {
			snapshot.Edges[i].Distances[0] = float32(math.Inf(1))
			break
		}
	}
	loaded, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if reason := loaded.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("load snapshot with non-finite distance reason=%q", reason)
	}
	for _, edge := range loaded.nodes {
		for _, layer := range edge.neighbors {
			for _, neighbor := range layer {
				if math.IsNaN(float64(neighbor.distance)) || math.IsInf(float64(neighbor.distance), 0) {
					t.Fatal("non-finite decoded edge")
				}
			}
		}
	}
}

func TestVectorIndexLoadPersistSnapshotRejectsExtraEdgeDistances(t *testing.T) {
	index := newColumnGraphCodecFixture5275(t)
	snapshot, _ := index.persistSnapshot()
	for i := range snapshot.Edges {
		if len(snapshot.Edges[i].Neighbor) > 0 {
			snapshot.Edges[i].Distances = append(snapshot.Edges[i].Distances, 0)
			break
		}
	}
	loaded, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if reason := loaded.loadPersistSnapshot(snapshot); reason != "invalid_edge_distance_count" {
		t.Fatalf("load snapshot reason=%q want invalid_edge_distance_count", reason)
	}
}

func TestVectorIndexPersistSnapshotClampsNegativeInfiniteEdgeDistances(t *testing.T) {
	index := newColumnGraphCodecFixture5275(t)
	index.mu.Lock()
	for i := range index.nodes {
		for layer := range index.nodes[i].neighbors {
			if len(index.nodes[i].neighbors[layer]) > 0 {
				index.nodes[i].neighbors[layer][0].distance = float32(math.Inf(-1))
				index.mu.Unlock()
				snapshot, _ := index.persistSnapshot()
				for _, edge := range snapshot.Edges {
					for _, distance := range edge.Distances {
						if math.IsInf(float64(distance), 0) || math.IsNaN(float64(distance)) {
							t.Fatalf("persisted non-finite edge distance: %v", distance)
						}
					}
				}
				if _, err := json.Marshal(snapshot.Edges); err != nil {
					t.Fatalf("marshal snapshot edges: %v", err)
				}
				return
			}
		}
	}
	index.mu.Unlock()
	t.Fatal("index has no edge distances to mutate")
}

// Domain-only publishers must survive another collection's commit and preserve
// the stored-document parser while repairing an intentionally unnotified row.

func newColumnGraphCodecFixture5275(t *testing.T) *columnGraphPartitionLiveCarrier {
	t.Helper()
	index, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = index.insertBatch(t.Context(), [][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]float32{{1, 0}, {.8, .2}, {0, 1}}); err != nil {
		t.Fatal(err)
	}
	return index
}

func TestColumnGraphCarrierCodecRepairsMissingAndShortDistances5275(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprintf("short=%v", short), func(t *testing.T) {
			source := newColumnGraphCodecFixture5275(t)
			snapshot, _ := source.persistSnapshot()
			changed := false
			for i := range snapshot.Edges {
				if len(snapshot.Edges[i].Distances) > 1 {
					if short {
						snapshot.Edges[i].Distances = snapshot.Edges[i].Distances[:1]
					} else {
						snapshot.Edges[i].Distances = nil
					}
					changed = true
				}
			}
			if !changed {
				t.Fatal("fixture has no multi-edge distance list")
			}
			target, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4})
			if err != nil {
				t.Fatal(err)
			}
			if reason := target.loadPersistSnapshot(snapshot); reason != "" {
				t.Fatalf("decode reason=%q", reason)
			}
			if !reflect.DeepEqual(snapshotVectorIndexTopology4257(target), snapshotVectorIndexTopology4257(source)) {
				t.Fatal("distance repair changed canonical topology")
			}
		})
	}
}
