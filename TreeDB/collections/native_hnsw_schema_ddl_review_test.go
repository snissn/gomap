package collections

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// The same public collection/vector calls run through the maintained backend
// constructor and the selected native owner. Scalar DDL must not strand an
// already registered ordinary HNSW receiver on the former schema.
func TestNativeHNSWPublicScalarDDLReview(t *testing.T) {
	for _, route := range []string{"legacy", "native"} {
		for _, operation := range []string{"create", "drop"} {
			t.Run(route+"/"+operation, func(t *testing.T) {
				opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
				opts.MemtableMode = "cow_btree"
				opts.BackgroundCheckpointInterval = -1
				opts.BackgroundCheckpointIdleDuration = -1
				var manager *CollectionManager
				var closeOwner func() error
				if route == "legacy" {
					backend, close, err := treedb.OpenBackendWithCachedLeafLog(opts)
					if err != nil {
						t.Fatal(err)
					}
					manager, closeOwner = NewCollectionManager(backend), close
				} else {
					owner, err := treedb.Open(opts)
					if err != nil {
						t.Fatal(err)
					}
					closeOwner = owner.Close
					backend, err := owner.NativeSemanticBackend()
					if err != nil {
						_ = closeOwner()
						t.Fatal(err)
					}
					manager = NewCollectionManager(backend)
				}
				defer func() {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
					if err := closeOwner(); err != nil {
						t.Error(err)
					}
				}()
				def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}
				scalar := IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}
				meta := &CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}
				if operation == "drop" {
					meta.Indexes = []IndexDefinition{scalar}
				}
				if _, err := manager.CreateCollection(meta); err != nil {
					t.Fatal(err)
				}
				c, err := manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = c.InsertBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{
					[]byte(`{"tenant":"a","embedding":[1,0]}`),
					[]byte(`{"tenant":"b","embedding":[0,1]}`),
				}); err != nil {
					t.Fatal(err)
				}
				idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
				if err != nil {
					t.Fatal(err)
				}
				search := func(stage string) {
					t.Helper()
					rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
					if err != nil || len(rows) != 2 {
						t.Fatalf("held HNSW %s scalar %s: rows=%d err=%v", stage, operation, len(rows), err)
					}
				}
				search("before")
				if operation == "create" {
					_, err = c.CreateIndex(scalar)
				} else {
					_, err = c.DropIndex(scalar.Name)
				}
				if err != nil {
					t.Fatal(err)
				}
				search("after")
				if operation == "create" {
					rows, _, err := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, IndexRangeFilter: &VectorIndexRangeFilter{
						IndexName: scalar.Name, Range: IndexRangeOptions{Lower: IndexRangeBound{Value: "a", Inclusive: true}, Upper: IndexRangeBound{Value: "a", Inclusive: true}},
					}})
					if err != nil || len(rows) != 1 || string(rows[0].DocumentID) != "a" {
						t.Fatalf("new scalar filter rows=%v err=%v", rows, err)
					}
				}
				if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
					return []byte(`{"tenant":"b","embedding":[0.9,0.1]}`), true, nil
				}); err != nil {
					t.Fatal(err)
				}
				search("after subsequent update and")
			})
		}
	}
}

func TestNativeHNSWPublicSchemaDDLPreservesGraph(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileNoWALFast, treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			var db *treedb.DB
			var manager *CollectionManager
			var c *Collection
			var budget nativeHNSWAdmissionBudget
			open := func(create bool) {
				t.Helper()
				var err error
				db, err = treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err := db.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				capture, err := backend.CaptureNativeSemanticState()
				if err != nil {
					t.Fatal(err)
				}
				budget = capture.NativeAllocator().(nativeHNSWAdmissionBudget)
				if err = capture.Close(); err != nil {
					t.Fatal(err)
				}
				manager = NewCollectionManager(backend)
				if create {
					_, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{{Name: "age", Field: "age", ValueType: IndexValueInt64}}, VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}}})
					if err != nil {
						t.Fatal(err)
					}
				}
				c, err = manager.OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
			}
			closeAll := func() {
				t.Helper()
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := db.Close(); err != nil {
					t.Error(err)
				}
				manager, db = nil, nil
				if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
					t.Errorf("full Close: %+v", stats)
				}
			}
			open(true)
			defer func() {
				if db != nil {
					closeAll()
				}
			}()
			for _, id := range []string{"a", "b", "c"} {
				if _, err := c.Insert([]byte(id), []byte(`{"age":42,"tenant":"x","active":true,"score":0.5,"embedding":[1,0]}`)); err != nil {
					t.Fatal(err)
				}
			}
			options := VectorIndexOptions{Name: "embedding", RebuildDeletedRatio: .75}
			idx, err := c.BuildVectorIndex(options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			// A real cold load installs the maintained immutable base view; the
			// following public Search/Update naturally creates a delta plane.
			c.UnregisterVectorIndex("embedding")
			var loaded VectorIndexLoadStatus
			idx, loaded, err = c.LoadVectorIndexSnapshot(options)
			if err != nil || !loaded.Loaded {
				t.Fatalf("opening cold Load: %v %+v", err, loaded)
			}
			if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true}); err != nil {
				t.Fatal(err)
			}
			if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
				return []byte(`{"age":43,"tenant":"x","active":false,"score":0.75,"embedding":[0.9,0.1]}`), true, nil
			}); err != nil {
				t.Fatal(err)
			}
			idx.TombstoneDocumentID([]byte("b")) // canonical b intentionally remains
			if idx.liveDelta == nil {
				t.Fatal("public Search+Update did not activate delta")
			}
			before, _ := idx.persistSnapshot()
			deltaBefore, _ := idx.liveDelta.persistSnapshot()
			sequence, dirty, version := idx.mutationSeq, idx.persistedSnapshotDirty, idx.nativeHNSWVersion
			if _, err = c.DropIndex("age"); err != nil {
				t.Fatal(err)
			}
			after, _ := idx.persistSnapshot()
			deltaAfter, _ := idx.liveDelta.persistSnapshot()
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(deltaBefore, deltaAfter) || idx.mutationSeq != sequence || idx.persistedSnapshotDirty != dirty || idx.nativeHNSWVersion != version || idx.rebuildDeletedRatio != .75 || c.registeredVectorIndex("embedding") != idx {
				t.Fatal("DDL changed graph/receiver/tuning/dirty/version state")
			}
			if _, _, err = c.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
				return []byte(`{"age":"now valid","tenant":"x","active":false,"score":0.75,"embedding":[0.8,0.2]}`), true, nil
			}); err != nil {
				t.Fatalf("dropped int64 still parsed: %v", err)
			}
			if err = idx.InsertDocument([]byte("a")); err != nil {
				t.Fatal(err)
			}
			for _, def := range []IndexDefinition{{Name: "tenant", Field: "tenant", ValueType: IndexValueString}, {Name: "active", Field: "active", ValueType: IndexValueBool}, {Name: "score", Field: "score", ValueType: IndexValueDouble}, {Name: "age", Field: "tenant", ValueType: IndexValueString}} {
				if _, err = c.CreateIndex(def); err != nil {
					t.Fatal(err)
				}
				if idx.liveDelta != nil && (!reflect.DeepEqual(idx.scalarDefinitions, idx.liveDelta.scalarDefinitions) || idx.liveDelta.validateNativeScalarColumnLengthsLocked() != nil) {
					t.Fatal("delta scalar schema/rows differ after DDL")
				}
				if _, err = c.DropIndex(def.Name); err != nil {
					t.Fatal(err)
				}
			}
			// Another vector declaration must not strand the surviving receiver.
			if _, err = c.CreateVectorIndex(VectorIndexDefinition{Name: "second", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, M: 4}); err != nil {
				t.Fatal(err)
			}
			if _, err = c.DropVectorIndex("second"); err != nil {
				t.Fatal(err)
			}
			check := func(index *VectorIndex) {
				t.Helper()
				rows, _, err := index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
				if err != nil {
					t.Fatal(err)
				}
				requireVectorResultIDs(t, rows, "c", "a")
			}
			check(idx)
			if _, err = idx.SaveSnapshot(); err != nil {
				t.Fatal(err)
			}
			if _, err = c.CreateIndex(IndexDefinition{Name: "tenant", Field: "tenant", ValueType: IndexValueString}); err != nil {
				t.Fatal(err)
			}
			hot, status, err := c.LoadVectorIndexSnapshot(options)
			if err != nil || !status.Loaded || hot != idx {
				t.Fatalf("clean cache lost identity: %v %+v", err, status)
			}
			if err = db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			closeAll()
			open(false)
			cold, status, err := c.LoadVectorIndexSnapshot(options)
			if err != nil || !status.Loaded {
				t.Fatalf("cold Load: %v %+v", err, status)
			}
			check(cold)
			if cold.rebuildDeletedRatio != .75 || len(cold.scalarDefinitions) != 1 || cold.scalarDefinitions[0].Name != "tenant" {
				t.Fatal("reopen lost tuning/current scalar schema")
			}
		})
	}
}

func TestNativeHNSWPublicSchemaDDLAdmission(t *testing.T) {
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, []byte("manual-row"))
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	// DDL's original capture/schema scratch fit; the admitted second graph does
	// not. Pinned semantic cuts do not prevent preflight from refunding older
	// physical checkpoint custody, but refusal must add no retained debt.
	var schemaScratch uint64
	proposed := IndexDefinition{Name: "new", Field: "new", ValueType: IndexValueString}
	if err = nativeIndexSchemaScratch(read.meta, &proposed, func(n uint64) error { schemaScratch += memtable.COWAllocationCharge(n); return nil }); err != nil {
		t.Fatal(err)
	}
	leave := captureBytes + schemaScratch + 16<<10
	if available <= leave {
		t.Fatal("setup exhausted budget")
	}
	if err = blocker.Resize(available - leave); err != nil {
		t.Fatal(err)
	}
	oldSchema, oldLease, oldCharge := idx.nativeHNSWSchemaState, idx.nativeHNSWLease, idx.nativeHNSWCharge
	beforeGraph, _ := idx.persistSnapshot()
	beforeLSN := c.nativeJSON.db.CommandWALNextLSN()
	for i := 0; i < 3; i++ {
		before := budget.Stats()
		_, err = c.CreateIndex(IndexDefinition{Name: "new", Field: "new", ValueType: IndexValueString})
		if !errors.Is(err, memtable.ErrCOWCapacity) || !strings.Contains(err.Error(), "schema preparation") {
			t.Fatalf("designated preassignment graph refusal: %v", err)
		}
		after := budget.Stats()
		latest, e := c.nativeJSON.Capture(c.name)
		if e != nil {
			t.Fatal(e)
		}
		if latest.capture.LogicalSequence() != read.capture.LogicalSequence() ||
			latest.collection.incarnation != read.collection.incarnation ||
			latest.collection.documentGeneration != read.collection.documentGeneration ||
			latest.collection.schema != read.collection.schema || !collectionMetaValuesEqual(latest.meta, read.meta) ||
			c.nativeJSON.db.CommandWALNextLSN() != beforeLSN {
			latest.Close()
			t.Fatal("refused DDL published metadata")
		}
		latest.Close()
		graph, _ := idx.persistSnapshot()
		if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases || idx.nativeHNSWSchemaState != oldSchema || idx.nativeHNSWLease != oldLease || idx.nativeHNSWCharge != oldCharge || !reflect.DeepEqual(beforeGraph, graph) {
			t.Fatalf("refusal changed custody: before=%+v after=%+v", before, after)
		}
	}
	blocker.Close()
	if _, err = c.CreateIndex(IndexDefinition{Name: "new", Field: "new", ValueType: IndexValueString}); err != nil {
		t.Fatal(err)
	}
	if c.registeredVectorIndex(def.Name) != idx {
		t.Fatal("accepted DDL replaced receiver")
	}
}

func TestNativeHNSWPublicSchemaDDLStaleCoverage(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 2, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	// NativeJSONTxn is a maintained public writer; it intentionally does not
	// promise ordinary facade graph reconciliation for this separately held idx.
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.DeleteBatch([][]byte{[]byte("row-00000")}); err != nil {
		tx.Close()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		tx.Close()
		t.Fatal(err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	generation := idx.sourceDocumentGeneration
	if _, err = c.CreateIndex(IndexDefinition{Name: "payload", Field: "payload", ValueType: IndexValueString}); err != nil {
		t.Fatalf("stale coverage blocked legal DDL: %v", err)
	}
	if idx.sourceDocumentGeneration != generation {
		t.Fatal("DDL blessed stale graph coverage")
	}
	if _, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1}); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("stale graph served after DDL: %v", err)
	}
	if _, err = c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeHNSWPublicSchemaDDLConcurrent(t *testing.T) {
	c, closeAll, _, def, _ := openHNSWPublicFixture(t, "native", 3, 2)
	defer closeAll()
	idx, err := c.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 4)
	go func() {
		for range 6 {
			if _, _, e := idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true}); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	go func() {
		for range 6 {
			idx.TombstoneDocumentID([]byte("row-00000"))
			if e := idx.InsertDocument([]byte("row-00000")); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	go func() {
		for range 6 {
			if _, e := idx.SaveSnapshot(); e != nil {
				done <- e
				return
			}
			if e := c.RegisterVectorIndex(idx); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	go func() {
		for range 6 {
			if _, e := c.CreateIndex(IndexDefinition{Name: "payload", Field: "payload", ValueType: IndexValueString}); e != nil {
				done <- e
				return
			}
			if _, e := c.DropIndex("payload"); e != nil {
				done <- e
				return
			}
		}
		done <- nil
	}()
	for range 4 {
		if e := <-done; e != nil {
			t.Error(e)
		}
	}
	if c.registeredVectorIndex(def.Name) != idx {
		t.Fatal("concurrent DDL replaced public receiver")
	}
}
