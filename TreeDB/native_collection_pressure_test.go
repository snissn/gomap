package treedb

import (
	"bytes"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Pause the real checkpoint cutover, then exercise the ordinary mutation gate
// and graph query. Producer preflight must hold neither while waiting for IO.
func TestNativeProducerPressurePrecedesCollectionLocks(t *testing.T) {
	for _, operation := range []string{"update", "save", "scalar-ddl", "rebuild", "nativewire-insert"} {
		t.Run(operation, func(t *testing.T) {
			opts := OptionsFor(ProfileNoWALFast, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			defer func() {
				database.cached.SetCommandWALCheckpointCutoverHook(nil)
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				cache := database.cached
				if err := database.Close(); err != nil {
					t.Error(err)
				}
				requirePublicCOWFinalZero(t, cache)
			}()
			def := collections.VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: collections.VectorMetricCosine, M: 4}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}, VectorIndexes: []collections.VectorIndexDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			document := []byte(`{"embedding":[1,0],"marker":1}`)
			if _, err := c.Insert([]byte("first"), document); err != nil {
				t.Fatal(err)
			}
			idx, err := c.BuildVectorIndex(collections.VectorIndexOptions{Name: def.Name, Field: def.Field, Dimensions: def.Dimensions, Metric: def.Metric, M: def.M})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; database.cached.COWMemoryStats().Sources < DefaultCOWMemtableLimits().MaxSources; i++ {
				if i > DefaultCOWMemtableLimits().MaxSources+4 {
					t.Fatal("source ceiling was not reached")
				}
				if _, err := idx.SaveSnapshot(); err != nil {
					t.Fatal(err)
				}
			}
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			database.cached.SetCommandWALCheckpointCutoverHook(func() { close(entered); <-release })
			var callbacks atomic.Int32
			go func() {
				var err error
				switch operation {
				case "update":
					_, _, err = c.Update([]byte("first"), func([]byte) ([]byte, bool, error) {
						callbacks.Add(1)
						return []byte(`{"embedding":[1,0],"marker":2}`), true, nil
					})
				case "save":
					_, err = idx.SaveSnapshot()
				case "scalar-ddl":
					_, err = c.CreateIndex(collections.IndexDefinition{Name: "marker", Field: "marker", ValueType: collections.IndexValueInt64})
				case "rebuild":
					_, err = c.RebuildVectorIndex(def.Name)
				case "nativewire-insert":
					err = c.NativewireInsertBatchNoResultIDs([][]byte{[]byte("second")}, [][]byte{document}, false)
				}
				finished <- err
			}()
			joined := false
			var available chan error
			availableJoined := false
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
				if !joined {
					<-finished
				}
				if available != nil && !availableJoined {
					<-available
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("producer did not enter pressure checkpoint")
			}
			available = make(chan error, 1)
			go func() {
				c.UnregisterVectorIndex("absent")
				_, _, err := idx.Search([]float32{1, 0}, collections.VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
				available <- err
			}()
			select {
			case err := <-available:
				availableJoined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("checkpoint preflight held collection or graph query gate")
			}
			if callbacks.Load() != 0 {
				t.Fatal("callback ran before maintenance completed")
			}
			close(release)
			err = <-finished
			joined = true
			if err != nil {
				t.Fatal(err)
			}
			if operation == "update" && callbacks.Load() != 1 {
				t.Fatal("callback was replayed")
			}
		})
	}
}

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
