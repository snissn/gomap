package collections

import (
	"encoding/binary"
	"hash/fnv"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionCommandWALBSONYCSBPointLookupAfterLoad(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer func() { _ = cleanup() }()

	mgr := NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "ycsb.usertable",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatBSON,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("ycsb.usertable")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	const (
		docCount  = 30_000
		batchSize = 1
	)
	ids := make([][]byte, docCount)
	docs := make([][]byte, docCount)
	for i := 0; i < docCount; i++ {
		id := []byte("user" + strconv.FormatUint(ycsbHash64(uint64(i)), 10))
		ids[i] = id
		docs[i] = ycsbBSONDocumentForID(t, string(id), i)
	}
	for start := 0; start < docCount; start += batchSize {
		end := start + batchSize
		if end > docCount {
			end = docCount
		}
		if _, err := col.InsertBatchValidatedBSON(ids[start:end], docs[start:end]); err != nil {
			t.Fatalf("insert batch [%d:%d]: %v", start, end, err)
		}
	}

	samples := []int{0, 1, 2, 10, 100, 999, 1_000, 5_000, 9_999, 10_000, 20_000, docCount - 1}
	for _, i := range samples {
		got, found, err := col.GetInto(ids[i], nil)
		if err != nil {
			t.Fatalf("buffered GetInto %s: %v", ids[i], err)
		}
		if !found {
			t.Fatalf("buffered GetInto %s found=false", ids[i])
		}
		_, field0 := bson.Raw(got).Lookup("field0").Binary()
		if string(field0) != "value"+strconv.Itoa(i)+"-0" {
			t.Fatalf("buffered GetInto %s field0=%q", ids[i], field0)
		}
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	scanned := 0
	truncated, err := col.ScanDocumentsFunc(docCount+1, func(DocumentRecord) (bool, error) {
		scanned++
		return true, nil
	})
	if err != nil {
		t.Fatalf("scan documents: %v", err)
	}
	if truncated {
		t.Fatal("scan documents unexpectedly truncated")
	}
	if scanned != docCount {
		t.Fatalf("scan count=%d want %d", scanned, docCount)
	}

	for _, i := range samples {
		got, found, err := col.GetInto(ids[i], nil)
		if err != nil {
			t.Fatalf("GetInto %s: %v", ids[i], err)
		}
		if !found {
			t.Fatalf("GetInto %s found=false", ids[i])
		}
		_, field0 := bson.Raw(got).Lookup("field0").Binary()
		if string(field0) != "value"+strconv.Itoa(i)+"-0" {
			t.Fatalf("GetInto %s field0=%q", ids[i], field0)
		}
	}
}

func ycsbHash64(value uint64) uint64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	h := fnv.New64a()
	_, _ = h.Write(b[:])
	return h.Sum64()
}

func ycsbBSONDocumentForID(t testing.TB, id string, i int) []byte {
	t.Helper()
	values := make(bson.D, 0, 11)
	values = append(values, bson.E{Key: "_id", Value: id})
	for field := 0; field < 10; field++ {
		values = append(values, bson.E{
			Key:   "field" + strconv.Itoa(field),
			Value: []byte("value" + strconv.Itoa(i) + "-" + strconv.Itoa(field)),
		})
	}
	return mustBSONCollectionDocument(t, values)
}
