package collections

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCollectionStatsStorage(t *testing.T) {
	t.Logf("raw bytes: Collection=%d INSERT=%d UPDATE=%d scan=%d", unsafe.Sizeof(Collection{}), unsafe.Sizeof(CollectionInsertStats{}), unsafe.Sizeof(CollectionUpdateStats{}), unsafe.Sizeof(CollectionDocumentScanStats{}))
	var nilCollection *Collection
	for _, c := range []*Collection{nilCollection, new(Collection)} {
		if !reflect.DeepEqual(c.LastInsertStats(), CollectionInsertStats{}) || !reflect.DeepEqual(c.LastUpdateStats(), CollectionUpdateStats{}) || c.LastDocumentScanStats() != (CollectionDocumentScanStats{}) {
			t.Fatal("unused stats are not zero")
		}
		if n := testing.AllocsPerRun(100, func() {
			_ = c.LastInsertStats()
			_ = c.LastUpdateStats()
			_ = c.LastDocumentScanStats()
		}); n != 0 {
			t.Fatalf("unused getters allocate: %g", n)
		}
	}
	nilCollection.setLastInsertStats(CollectionInsertStats{})
	nilCollection.setLastInsertStatsOwned(CollectionInsertStats{})
	nilCollection.setLastUpdateStats(CollectionUpdateStats{})
	nilCollection.setLastDocumentScanStats(CollectionDocumentScanStats{})

	c, other := new(Collection), new(Collection)
	insert := CollectionInsertStats{Documents: 3, SecondaryRuns: []CollectionSecondaryRunStats{{IndexName: "city", Entries: 3}}}
	c.setLastInsertStats(insert)
	insert.SecondaryRuns[0].IndexName = "input-mutated"
	insertGot := c.LastInsertStats()
	if insertGot.Documents != 3 || insertGot.SecondaryRuns[0].IndexName != "city" {
		t.Fatalf("input aliases stored insert stats: %+v", insertGot)
	}
	insertGot.SecondaryRuns[0].IndexName = "output-mutated"
	if c.LastInsertStats().SecondaryRuns[0].IndexName != "city" {
		t.Fatal("output aliases stored insert stats")
	}
	if !reflect.DeepEqual(other.LastInsertStats(), CollectionInsertStats{}) {
		t.Fatal("separate handles share insert stats")
	}
	c.setLastInsertStatsOwned(CollectionInsertStats{Documents: 7})
	if !reflect.DeepEqual(c.LastInsertStats(), CollectionInsertStats{Documents: 7}) {
		t.Fatal("replacement retained old insert fields")
	}
	if n := testing.AllocsPerRun(100, func() { c.setLastInsertStats(insert) }); n != 1 {
		t.Fatalf("ordinary insert setter must clone its secondary array: %g allocations", n)
	}
	// The owned setter transfers the array, whereas the ordinary setter clones
	// it. Read-only repeated transfer must not add another array allocation.
	if n := testing.AllocsPerRun(100, func() { c.setLastInsertStatsOwned(insert) }); n != 0 {
		t.Fatalf("owned insert setter added a copy: %g allocations", n)
	}
	input := CollectionUpdateStats{Items: 3, SecondaryRuns: []CollectionUpdateSecondaryRunStats{{IndexName: "city", Sets: 3}}}
	c.setLastUpdateStats(input)
	input.SecondaryRuns[0].IndexName = "input-mutated"
	got := c.LastUpdateStats()
	if got.Items != 3 || got.SecondaryRuns[0].IndexName != "city" {
		t.Fatalf("input aliases stored stats: %+v", got)
	}
	got.SecondaryRuns[0].IndexName = "output-mutated"
	if c.LastUpdateStats().SecondaryRuns[0].IndexName != "city" {
		t.Fatal("output aliases stored stats")
	}
	if !reflect.DeepEqual(other.LastUpdateStats(), CollectionUpdateStats{}) {
		t.Fatal("separate handles share update stats")
	}
	c.setLastUpdateStats(CollectionUpdateStats{Items: 7})
	if !reflect.DeepEqual(c.LastUpdateStats(), CollectionUpdateStats{Items: 7}) {
		t.Fatal("replacement retained old update fields")
	}
	first := CollectionDocumentScanStats{CertifiedMonotonicPath: true, PhysicalRows: 12}
	c.setLastDocumentScanStats(first)
	if c.LastDocumentScanStats() != first || other.LastDocumentScanStats() != (CollectionDocumentScanStats{}) {
		t.Fatal("scan snapshot is not handle-owned")
	}
	c.setLastDocumentScanStats(CollectionDocumentScanStats{GenericFallback: true})
	if c.LastDocumentScanStats() != (CollectionDocumentScanStats{GenericFallback: true}) {
		t.Fatal("replacement retained old scan fields")
	}
	if n := testing.AllocsPerRun(100, func() {
		c.setLastInsertStatsOwned(CollectionInsertStats{Documents: 1})
		c.setLastUpdateStats(CollectionUpdateStats{Items: 1})
		c.setLastDocumentScanStats(first)
		_ = c.LastUpdateStats()
		_ = c.LastInsertStats()
		_ = c.LastDocumentScanStats()
	}); n != 0 {
		t.Fatalf("replacement of initialized backing allocates: %g", n)
	}
}

func TestCollectionStatsStorageConcurrent(t *testing.T) {
	c := new(Collection)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Go(func() {
			for i := 0; i < 100; i++ {
				c.setLastInsertStats(CollectionInsertStats{Documents: i, SecondaryRuns: []CollectionSecondaryRunStats{{Entries: i}}})
				c.setLastUpdateStats(CollectionUpdateStats{Items: i, SecondaryRuns: []CollectionUpdateSecondaryRunStats{{Sets: i}}})
				c.setLastDocumentScanStats(CollectionDocumentScanStats{PhysicalRows: uint64(i), PhysicalBytes: uint64(i * 2)})
				got := c.LastUpdateStats()
				if len(got.SecondaryRuns) != 1 || got.SecondaryRuns[0].Sets != got.Items {
					t.Error("incoherent update snapshot")
					return
				}
				got.SecondaryRuns[0].Sets = -1
				insert := c.LastInsertStats()
				if len(insert.SecondaryRuns) != 1 || insert.SecondaryRuns[0].Entries != insert.Documents {
					t.Error("incoherent insert snapshot")
					return
				}
				insert.SecondaryRuns[0].Entries = -1
				scan := c.LastDocumentScanStats()
				if scan.PhysicalBytes != scan.PhysicalRows*2 {
					t.Error("incoherent scan snapshot")
					return
				}
			}
		})
	}
	workers.Wait()
}

var collectionStatsStorageSink *Collection

// Measure the public native open, including its original metadata capture. The
// sink forces each returned handle to escape; the fixture is outside timing.
func BenchmarkCollectionStatsStorage(b *testing.B) {
	for _, mode := range []string{"Open", "OpenInsert", "OpenUpdate", "OpenScan", "OpenAllStats"} {
		b.Run(mode, func(b *testing.B) {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, b.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
			owner, err := treedb.Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			var manager *CollectionManager
			b.Cleanup(func() {
				collectionStatsStorageSink = nil
				var err error
				if manager != nil {
					err = manager.Close()
				}
				if err = errors.Join(err, owner.Close()); err != nil {
					b.Error(err)
				}
			})
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				b.Fatal(err)
			}
			manager = NewCollectionManager(backend)
			if !manager.UsesNativeCommandWALApply() {
				b.Fatal("native public owner inactive")
			}
			if _, err = manager.CreateCollection(&CollectionMeta{Name: "stats", Options: CollectionOptions{DocumentFormat: DocumentFormatBSON}}); err != nil {
				b.Fatal(err)
			}
			warm, err := manager.OpenCollection("stats")
			if err != nil || warm.nativeJSON == nil || len(warm.meta.Indexes) != 0 || warm.meta.Options.DocumentFormat != DocumentFormatBSON {
				b.Fatalf("ordinary BSON native open: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := manager.OpenCollection("stats")
				if err != nil {
					b.Fatal(err)
				}
				switch mode {
				case "OpenInsert":
					c.setLastInsertStatsOwned(CollectionInsertStats{Documents: 1})
				case "OpenUpdate":
					c.setLastUpdateStats(CollectionUpdateStats{Items: 1})
				case "OpenScan":
					c.setLastDocumentScanStats(CollectionDocumentScanStats{PhysicalRows: 1})
				case "OpenAllStats":
					c.setLastInsertStatsOwned(CollectionInsertStats{Documents: 1})
					c.setLastUpdateStats(CollectionUpdateStats{Items: 1})
					c.setLastDocumentScanStats(CollectionDocumentScanStats{PhysicalRows: 1})
				}
				collectionStatsStorageSink = c
			}
			b.StopTimer()
		})
	}
}
