package collections

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestNativeInsertBatchStatsPublic(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e := owner.Close(); e != nil {
					t.Error(e)
				}
			}()
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := NewCollectionManager(backend)
			defer func() {
				if e := manager.Close(); e != nil {
					t.Error(e)
				}
			}()
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "docs", Indexes: []IndexDefinition{
				{Name: "email", Field: "email", ValueType: IndexValueString, Unique: true},
				{Name: "city", Field: "city", ValueType: IndexValueString},
			}}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("docs")
			if err != nil {
				t.Fatal(err)
			}
			ids := [][]byte{[]byte("b"), []byte("a")}
			docs := [][]byte{[]byte(`{"email":"b@example.org","city":"west"}`), []byte(`{"email":"a@example.org","city":"east"}`)}
			stored, stats, err := c.InsertBatchWithStats(ids, docs)
			if err != nil {
				t.Fatalf("ordinary native InsertBatchWithStats: %v", err)
			}
			if !reflect.DeepEqual(stored, ids) || stats.Documents != 2 || stats.Indexes != 2 || len(stats.SecondaryRuns) != 2 || stats.SecondaryEntries != 4 || stats.SecondaryKeyBytes == 0 || stats.Runs < 3 {
				t.Fatalf("request stats=%+v ids=%q", stats, stored)
			}
			last := c.LastInsertStats()
			if !reflect.DeepEqual(last, stats) {
				t.Fatalf("LastInsertStats=%+v request=%+v", last, stats)
			}
			stats.SecondaryRuns[0].IndexName = "request-mutated"
			stored[0][0] = 'x'
			ids[1][0] = 'y'
			if got := c.LastInsertStats(); !reflect.DeepEqual(got, last) {
				t.Fatal("request output mutated last stats")
			}
			last.SecondaryRuns[0].IndexName = "last-mutated"
			if c.LastInsertStats().SecondaryRuns[0].IndexName == "last-mutated" {
				t.Fatal("LastInsertStats aliases caller slice")
			}
			good := c.LastInsertStats()
			read, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			beforeLSN := backend.CommandWALNextLSN()
			for _, test := range []struct {
				id   string
				doc  string
				want error
			}{
				{"a", `{"email":"fresh@example.org","city":"other"}`, ErrDocumentExists},
				{"c", `{"email":"a@example.org","city":"other"}`, ErrUniqueIndexConflict},
				{"d", `{"email":"new@example.org","city":123}`, nil},
			} {
				_, failed, e := c.InsertBatchWithStats([][]byte{[]byte(test.id)}, [][]byte{[]byte(test.doc)})
				if e == nil || (test.want != nil && !errors.Is(e, test.want)) || backenddb.CommitPublicationAccepted(e) {
					t.Fatalf("refusal %s: %v", test.id, e)
				}
				if !reflect.DeepEqual(failed, CollectionInsertStats{}) || !reflect.DeepEqual(c.LastInsertStats(), good) {
					t.Fatalf("refusal published success stats: failed=%+v last=%+v", failed, c.LastInsertStats())
				}
			}
			current, err := c.nativeJSON.Capture(c.name)
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			if current.capture.LogicalSequence() != read.capture.LogicalSequence() || backend.CommandWALNextLSN() != beforeLSN {
				t.Fatal("refused inserts changed logical/command identity")
			}
			for _, id := range []string{"a", "b"} {
				if got, e := c.Get([]byte(id)); e != nil || got == nil {
					t.Fatalf("stored %s: %s %v", id, got, e)
				}
			}
			if got, e := c.Get([]byte("c")); e != nil || got != nil {
				t.Fatalf("refused row: %s %v", got, e)
			}
			_, empty, err := c.InsertBatchWithStats(nil, nil)
			if err != nil || empty.Documents != 0 || empty.Indexes != 2 || empty.Runs != 0 || len(empty.SecondaryRuns) != 0 || !reflect.DeepEqual(empty, c.LastInsertStats()) {
				t.Fatalf("empty stats=%+v err=%v", empty, err)
			}
			// Plain insertion owns the same most-recent-success contract. Readers
			// may take and mutate their own diagnostic copy concurrently.
			var readers sync.WaitGroup
			stop := make(chan struct{})
			readers.Add(1)
			go func() {
				defer readers.Done()
				for {
					select {
					case <-stop:
						return
					default:
						got := c.LastInsertStats()
						if len(got.SecondaryRuns) != 0 {
							got.SecondaryRuns[0].IndexName = "reader-owned"
						}
					}
				}
			}()
			_, err = c.InsertBatch([][]byte{[]byte("plain")}, [][]byte{[]byte(`{"email":"plain@example.org","city":"east"}`)})
			close(stop)
			readers.Wait()
			plain := c.LastInsertStats()
			if err != nil || plain.Documents != 1 || plain.Indexes != 2 || plain.SecondaryEntries != 2 || len(plain.SecondaryRuns) != 2 || plain.SecondaryRuns[0].IndexName == "reader-owned" {
				t.Fatalf("plain insert stats=%+v err=%v", plain, err)
			}
			if !reflect.DeepEqual(empty, CollectionInsertStats{Indexes: 2}) {
				t.Fatal("later insertion changed request-owned empty stats")
			}
			if _, err = c.InsertBatch(nil, nil); err != nil || !reflect.DeepEqual(c.LastInsertStats(), empty) {
				t.Fatalf("plain empty did not replace LastInsertStats: %v", err)
			}
		})
	}
}

func TestNativeInsertBatchStatsCapacityCleanup(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	var pins []backenddb.NativeSemanticCapture
	var manager *CollectionManager
	var budget interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	}
	t.Cleanup(func() {
		for i := len(pins) - 1; i >= 0; i-- {
			if e := pins[i].Close(); e != nil {
				t.Error(e)
			}
		}
		if manager != nil {
			if e := manager.Close(); e != nil {
				t.Error(e)
			}
		}
		if e := owner.Close(); e != nil {
			t.Error(e)
		}
		if budget != nil {
			if s := budget.Stats(); s.TotalBytes != 0 || s.ExternalLeases != 0 {
				t.Errorf("full close retained resources: %+v", s)
			}
		}
	})
	pin := func() {
		t.Helper()
		cut, e := backend.CaptureNativeSemanticState()
		if e != nil {
			t.Fatal(e)
		}
		pins = append(pins, cut)
	}
	pin() // Hold every setup cut so old-cut retirement cannot hide a leak.
	budget = pins[0].NativeAllocator().(interface {
		Stats() memtable.COWStats
		Limits() memtable.COWLimits
	})
	manager = NewCollectionManager(backend)
	_, err = manager.CreateCollection(&CollectionMeta{Name: "stats", Indexes: []IndexDefinition{{Name: "city", Field: "city", ValueType: IndexValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	pin()
	c, err := manager.OpenCollection("stats")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.InsertBatch([][]byte{[]byte("initial")}, [][]byte{[]byte(`{"city":"east"}`)}); err != nil {
		t.Fatal(err)
	}
	pin()
	good := c.LastInsertStats()
	logical := pins[len(pins)-1].LogicalSequence()
	lsn := backend.CommandWALNextLSN()
	blocker, err := pins[0].NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if err = blocker.Resize(budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		before := budget.Stats()
		_, stats, e := c.InsertBatchWithStats([][]byte{[]byte("blocked")}, [][]byte{[]byte(`{"city":"west"}`)})
		if !errors.Is(e, memtable.ErrCOWCapacity) || backenddb.CommitPublicationAccepted(e) || !reflect.DeepEqual(stats, CollectionInsertStats{}) {
			t.Fatalf("finite refusal: stats=%+v err=%v", stats, e)
		}
		if !reflect.DeepEqual(good, c.LastInsertStats()) {
			t.Fatal("refusal changed last successful stats")
		}
		after := budget.Stats()
		// Producer preflight may retire earlier checkpoint ownership even with
		// all semantic cuts held. It must add no private retained reservation.
		if after.ReservedBytes > before.ReservedBytes || after.ExternalLeases > before.ExternalLeases {
			t.Fatalf("refusal resources before=%+v after=%+v", before, after)
		}
	}
	blocker.Close()
	current, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	if current.LogicalSequence() != logical || backend.CommandWALNextLSN() != lsn {
		t.Fatal("refusal changed logical or command identity")
	}
	if err = current.Close(); err != nil {
		t.Fatal(err)
	}
	if got, e := c.Get([]byte("blocked")); e != nil || got != nil {
		t.Fatalf("refused row present: %q %v", got, e)
	}
	if _, stats, e := c.InsertBatchWithStats([][]byte{[]byte("blocked")}, [][]byte{[]byte(`{"city":"west"}`)}); e != nil || stats.Documents != 1 || stats.SecondaryEntries != 1 {
		t.Fatalf("release recovery: %+v %v", stats, e)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, stats, e := c.InsertBatchWithStats(nil, nil); !errors.Is(e, backenddb.ErrClosed) || !reflect.DeepEqual(stats, CollectionInsertStats{}) || c.LastInsertStats().Documents != 1 {
		t.Fatalf("closed manager: %+v %v", stats, e)
	}
}

func TestNativeInsertBatchStatsTypedEligibility(t *testing.T) {
	owner, backend, manager, c := openNativeTypedGroup(t, t.TempDir(), true)
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, lsn := c.LastInsertStats(), backend.CommandWALNextLSN()
	ids, documents := [][]byte{[]byte("plain")}, [][]byte{[]byte(`{"id":"plain","payload":"original","label":"label-plain","embedding":[1,2,3]}`)}
	for _, batch := range []struct{ ids, documents [][]byte }{{ids, documents}, {nil, nil}} {
		_, stats, err := c.InsertBatchWithStats(batch.ids, batch.documents)
		if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || backenddb.CommitPublicationAccepted(err) || !reflect.DeepEqual(stats, CollectionInsertStats{}) || !reflect.DeepEqual(c.LastInsertStats(), before) || backend.CommandWALNextLSN() != lsn {
			t.Fatalf("typed WithStats eligibility: %+v %v", stats, err)
		}
	}
	if _, err := c.InsertBatch(ids, documents); err != nil {
		t.Fatalf("prior plain typed insert: %v", err)
	}
	if !reflect.DeepEqual(c.LastInsertStats(), before) {
		t.Fatal("typed Last behavior changed")
	}
}
