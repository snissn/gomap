package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"math"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionSearchVectorsExactTopKAndTieOrder(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c"), []byte("a"), []byte("b"), []byte("missing")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"tag":"keep"}`),
			[]byte(`{"embedding":[0,1],"tag":"keep"}`),
			[]byte(`{"embedding":[0,1],"tag":"keep"}`),
			[]byte(`{"tag":"keep"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := col.SearchVectorsExact([]float32{0, 1}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("search vectors: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
	if results[0].Distance != 0 || results[1].Distance != 0 {
		t.Fatalf("distances=%v,%v want exact zero ties", results[0].Distance, results[1].Distance)
	}
}

func TestVectorFromBSONFieldPropagatesMalformedTraversal(t *testing.T) {
	missingDoc := mustBSONCollectionDocument(t, bson.D{{Key: "name", Value: "ada"}})
	if _, ok, err := vectorFromBSONField(missingDoc, []string{"payload", "embedding"}); err != nil || ok {
		t.Fatalf("missing BSON vector ok=%v err=%v want missing without error", ok, err)
	}

	malformedNested := []byte{10, 0, 0, 0, 0}
	doc := make([]byte, 0, 4+1+len("payload")+1+len(malformedNested)+1)
	doc = append(doc, 0, 0, 0, 0)
	doc = append(doc, 0x03)
	doc = append(doc, "payload"...)
	doc = append(doc, 0)
	doc = append(doc, malformedNested...)
	doc = append(doc, 0)
	binary.LittleEndian.PutUint32(doc[:4], uint32(len(doc)))

	if _, ok, err := vectorFromBSONField(doc, []string{"payload", "embedding"}); err == nil || ok {
		t.Fatalf("malformed BSON traversal ok=%v err=%v want error", ok, err)
	}
}

func TestCollectionSearchVectorsExactFiltersAndDeletes(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("deleted"), []byte("keep"), []byte("skip")},
		[][]byte{
			[]byte(`{"embedding":[0,0],"tag":"keep"}`),
			[]byte(`{"embedding":[3,4],"tag":"keep"}`),
			[]byte(`{"embedding":[0,0],"tag":"skip"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("deleted")}); err != nil || deleted != 1 {
		t.Fatalf("delete deleted=%d err=%v", deleted, err)
	}

	results, err := col.SearchVectorsExact([]float32{3, 5}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricL2,
		TopK:   10,
		Filter: func(record DocumentRecord) (bool, error) {
			var doc struct {
				Tag string `json:"tag"`
			}
			if err := json.Unmarshal(record.Document, &doc); err != nil {
				return false, err
			}
			return doc.Tag == "keep", nil
		},
	})
	if err != nil {
		t.Fatalf("search vectors: %v", err)
	}
	requireVectorResultIDs(t, results, "keep")
	if got, want := results[0].Distance, float32(1); got != want {
		t.Fatalf("distance=%v want %v", got, want)
	}
}

func TestCollectionSearchVectorsExactIndexRangeFilter(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Indexes: []IndexDefinition{
			{Name: "city_idx", Field: "city", ValueType: IndexValueString},
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[1,0],"city":"sea"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	results, err := col.SearchVectorsExact([]float32{1, 0}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   10,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: "city_idx",
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("search vectors: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
}

func TestCollectionSearchVectorsExactReopen(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("x"), []byte("y")},
		[][]byte{
			[]byte(`{"payload":{"embedding":[2,0]}}`),
			[]byte(`{"payload":{"embedding":[0,2]}}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	results, err := reopenedCol.SearchVectorsExact([]float32{2, 0}, VectorSearchOptions{
		Field:  "payload.embedding",
		Metric: VectorMetricInnerProduct,
		TopK:   1,
	})
	if err != nil {
		t.Fatalf("search vectors after reopen: %v", err)
	}
	requireVectorResultIDs(t, results, "x")
}

func TestCollectionSearchVectorsExactRejectsWrongDimension(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("bad")},
		[][]byte{[]byte(`{"embedding":[1,2,3]}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err = col.SearchVectorsExact([]float32{1, 2}, VectorSearchOptions{
		Field: "embedding",
		TopK:  1,
	})
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("dimension 3, want 2")) {
		t.Fatalf("wrong-dimension err=%v", err)
	}
}

func TestExactVectorDistance(t *testing.T) {
	l2, err := exactVectorDistance([]float32{1, 2}, []float32{4, 6}, VectorMetricL2)
	if err != nil {
		t.Fatalf("l2: %v", err)
	}
	if l2 != 25 {
		t.Fatalf("l2=%v want 25", l2)
	}

	cosine, err := exactVectorDistance([]float32{1, 0}, []float32{0, 1}, VectorMetricCosine)
	if err != nil {
		t.Fatalf("cosine: %v", err)
	}
	if math.Abs(float64(cosine-1)) > 1e-6 {
		t.Fatalf("cosine=%v want 1", cosine)
	}

	inner, err := exactVectorDistance([]float32{1, 2}, []float32{3, 4}, VectorMetricInnerProduct)
	if err != nil {
		t.Fatalf("inner product: %v", err)
	}
	if inner != -11 {
		t.Fatalf("inner=%v want -11", inner)
	}

	if _, err := exactVectorDistance([]float32{0, 0}, []float32{1, 0}, VectorMetricCosine); err == nil {
		t.Fatal("expected zero-vector cosine error")
	}
}

func TestVectorFromJSONFieldMissingAndInvalid(t *testing.T) {
	if _, ok, err := vectorFromJSONField([]byte(`{"payload":{}}`), []string{"payload", "embedding"}); err != nil || ok {
		t.Fatalf("missing vector ok=%v err=%v", ok, err)
	}
	if _, ok, err := vectorFromJSONField([]byte(`{"embedding":null}`), []string{"embedding"}); err != nil || ok {
		t.Fatalf("null vector ok=%v err=%v", ok, err)
	}
	_, _, err := vectorFromJSONField([]byte(`{"embedding":["bad"]}`), []string{"embedding"})
	if err == nil {
		t.Fatal("expected invalid vector error")
	}
}

func requireVectorResultIDs(tb testing.TB, results []VectorSearchResult, want ...string) {
	tb.Helper()
	if len(results) != len(want) {
		tb.Fatalf("result count=%d want %d: %#v", len(results), len(want), results)
	}
	for i := range want {
		if string(results[i].DocumentID) != want[i] {
			tb.Fatalf("result[%d] id=%q want %q", i, results[i].DocumentID, want[i])
		}
	}
}
