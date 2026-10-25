package treedb

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Tiny semantic writes used to exhaust the default reservation budget at about
// 444 rows even though an ordinary checkpoint reclaimed most of that debt.
// Keep all background triggers disabled so only producer pressure can help.
func TestNativeCollectionPressureDrainsBeforeOpening(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.FlushThreshold = 1 << 30
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	document := func(id string) []byte {
		doc := bson.D{{Key: "_id", Value: id}}
		for field := 0; field < 10; field++ {
			doc = append(doc, bson.E{Key: fmt.Sprintf("field%d", field), Value: []byte("value")})
		}
		raw, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	if _, err := collection.InsertBatch([][]byte{[]byte("first")}, [][]byte{document("first")}); err != nil {
		t.Fatal(err)
	}
	held, err := collection.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	const rows = 1500
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("row%06d", i)
		if _, err := collection.InsertBatch([][]byte{[]byte(id)}, [][]byte{document(id)}); err != nil {
			t.Fatalf("insert %d: %v; stats=%+v", i, err, database.cached.COWMemoryStats())
		}
	}
	handoffs, err := strconv.ParseUint(database.Stats()["treedb.cache.cow.handoffs_total"], 10, 64)
	if err != nil || handoffs < 3 {
		t.Fatalf("expected several automatic handoffs: %d/%v", handoffs, err)
	}
	old, err := held.FetchDocumentsByID([][]byte{[]byte("first"), []byte("row001499")}, collections.DocumentFetchOptions{})
	if err != nil || len(old.Results) != 2 || !old.Results[0].Found || !bytes.Equal(old.Results[0].Document, document("first")) || old.Results[1].Found {
		t.Fatalf("old opening changed across handoffs: %+v/%v", old, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	cache := database.cached
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	requirePublicCOWFinalZero(t, cache)
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	defer manager.Close()
	collection, err = manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "row000000", "row000443", "row001499"} {
		got, err := collection.Get([]byte(id))
		if err != nil || !bytes.Equal(got, document(id)) {
			t.Fatalf("reopen %s: %x/%v", id, got, err)
		}
	}
}
