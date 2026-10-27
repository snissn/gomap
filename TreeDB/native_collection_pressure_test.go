package treedb

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Pause the real checkpoint cutover, then exercise ordinary scalar reads.
// Producer preflight must leave the committed collection readable during IO.
func TestNativeProducerPressurePreservesCollectionReads(t *testing.T) {
	for _, operation := range []string{"update", "insert", "scalar-ddl", "delete", "nativewire-insert"} {
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
			def := collections.IndexDefinition{Name: "marker", Field: "marker", ValueType: collections.IndexValueInt64}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}, Indexes: []collections.IndexDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			document := []byte(`{"marker":1}`)
			if _, err := c.Insert([]byte("first"), document); err != nil {
				t.Fatal(err)
			}
			// Retain the real accepted cuts so their original COW sources cannot
			// retire before the contender reaches preflight. A transient byte
			// watermark alone can disappear through deferred cleanup and let the
			// mutation run before an unrelated checkpoint enters the hook.
			var held []backenddb.NativeSemanticCapture
			var heldOnce sync.Once
			closeHeld := func() {
				heldOnce.Do(func() {
					for _, capture := range held {
						if err := capture.Close(); err != nil {
							t.Error(err)
						}
					}
				})
			}
			defer closeHeld()
			var prefixSequence uint64
			for i := 0; ; i++ {
				capture, err := backend.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, capture)
				observation, err := backend.ObserveNativeProducer()
				if err != nil {
					t.Fatal(err)
				}
				if observation.UnderPressure {
					// Join the original checkpoint and queued cleanup frontier while
					// retaining every accepted cut. Remaining pressure now belongs
					// to live owners, rather than asynchronously retiring scratch.
					drained, err := backend.PrepareNativeProducerObservation()
					if err != nil {
						t.Fatal(err)
					}
					if drained.UnderPressure {
						// Advance the original logical prefix once under the completed
						// drain. The tested producer cannot reuse that remembered drain,
						// and its next checkpoint has genuine new committed work.
						if _, err := c.Insert([]byte(fmt.Sprintf("filler-%d", i)), document); err != nil {
							t.Fatal(err)
						}
						capture, err := backend.CaptureNativeSemanticState()
						if err != nil {
							t.Fatal(err)
						}
						held = append(held, capture)
						pressure, err := backend.ObserveNativeProducer()
						if err != nil || !pressure.UnderPressure || pressure.LogicalSequence != drained.LogicalSequence+1 {
							t.Fatalf("retained owners lost pressure or exact accepted suffix: drained=%+v current=%+v err=%v", drained, pressure, err)
						}
						prefixSequence = pressure.LogicalSequence
						t.Logf("live retained cuts preserved producer pressure after original cleanup join: drained=%+v prefix=%+v limits=%+v captures=%d", drained, pressure, DefaultCOWMemtableLimits(), len(held))
						break
					}
				}
				if i >= 2048 {
					t.Fatalf("bounded accepted prefix did not reach producer pressure: %+v", observation)
				}
				if _, err := c.Insert([]byte(fmt.Sprintf("filler-%d", i)), document); err != nil {
					t.Fatal(err)
				}
			}
			type checkpointEntry struct {
				sequence uint64
				err      error
			}
			entered, release, finished := make(chan checkpointEntry, 1), make(chan struct{}), make(chan error, 1)
			var hookOnce sync.Once
			database.cached.SetCommandWALCheckpointCutoverHook(func() {
				hookOnce.Do(func() {
					observation, err := backend.ObserveNativeProducer()
					entered <- checkpointEntry{sequence: observation.LogicalSequence, err: err}
					<-release
				})
			})
			var callbacks atomic.Int32
			go func() {
				var err error
				switch operation {
				case "update":
					_, _, err = c.Update([]byte("first"), func([]byte) ([]byte, bool, error) {
						callbacks.Add(1)
						return []byte(`{"marker":2}`), true, nil
					})
				case "insert":
					_, err = c.Insert([]byte("second"), document)
				case "scalar-ddl":
					_, err = c.CreateIndex(collections.IndexDefinition{Name: "marker_copy", Field: "marker", ValueType: collections.IndexValueInt64})
				case "delete":
					err = c.Delete([]byte("first"))
				case "nativewire-insert":
					err = c.NativewireInsertBatchNoResultIDs([][]byte{[]byte("second")}, [][]byte{document}, false)
				}
				finished <- err
			}()
			joined := false
			var available chan error
			availableJoined := false
			defer func() {
				closeHeld()
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
			case entry := <-entered:
				if entry.err != nil || entry.sequence != prefixSequence {
					t.Fatalf("checkpoint did not pause the original pre-mutation prefix: sequence=%d want=%d err=%v", entry.sequence, prefixSequence, entry.err)
				}
			case err := <-finished:
				joined = true
				t.Fatalf("producer completed before its pressure checkpoint: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("producer did not enter pressure checkpoint")
			}
			select {
			case err := <-finished:
				joined = true
				t.Fatalf("producer completed while its pressure checkpoint was paused: %v", err)
			default:
			}
			available = make(chan error, 1)
			go func() {
				got, err := c.Get([]byte("first"))
				if err == nil && !bytes.Equal(got, document) {
					err = fmt.Errorf("committed document changed during preflight: %q", got)
				}
				if err == nil {
					var ids [][]byte
					ids, err = c.FindByIndexValue("marker", int64(1))
					found := false
					for _, id := range ids {
						found = found || string(id) == "first"
					}
					if err == nil && !found {
						err = fmt.Errorf("committed scalar membership absent during preflight")
					}
				}
				available <- err
			}()
			select {
			case err := <-available:
				availableJoined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("checkpoint preflight blocked committed collection reads")
			}
			if callbacks.Load() != 0 {
				t.Fatal("callback ran before maintenance completed")
			}
			closeHeld()
			close(release)
			err = <-finished
			joined = true
			if err != nil {
				t.Fatal(err)
			}
			if operation == "update" && callbacks.Load() != 1 {
				t.Fatal("callback was replayed")
			}
			want := document
			if operation == "update" {
				want = []byte(`{"marker":2}`)
			} else if operation == "delete" {
				want = nil
			}
			if got, err := c.Get([]byte("first")); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("completed %s changed the wrong document state: got=%q want=%q err=%v", operation, got, want, err)
			}
			if operation == "insert" || operation == "nativewire-insert" {
				if got, err := c.Get([]byte("second")); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("completed insert absent: %q %v", got, err)
				}
			}
			if operation == "scalar-ddl" {
				ids, err := c.FindByIndexValue("marker_copy", int64(1))
				found := false
				for _, id := range ids {
					found = found || string(id) == "first"
				}
				if err != nil || !found {
					t.Fatalf("completed DDL lost original membership: %q %v", ids, err)
				}
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
