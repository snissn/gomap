package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func cowReadWorkspaceFixture(tb testing.TB, mode string, shards int) *DB {
	tb.Helper()
	dir := tb.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        filepath.Join(dir, "backend"),
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		tb.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL:                 true,
		AllowUnsafe:                true,
		MemtableMode:               mode,
		MemtableShards:             shards,
		FlushThreshold:             1 << 30,
		IndexOuterLeavesInValueLog: true,
		ValueLogPointerThreshold:   256,
		ValueLogCompression:        uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				tb.Error(err)
			}
		}
	})
	return db
}

func TestCOWGetManyViewReusesResultAcrossMisses(t *testing.T) {
	for _, mode := range []string{"cow_btree", "cow_ordered"} {
		t.Run(mode, func(t *testing.T) {
			db := cowReadWorkspaceFixture(t, mode, 2)
			valA := bytes.Repeat([]byte{'a'}, 128)
			valB := bytes.Repeat([]byte{'b'}, 128)
			valLarge := bytes.Repeat([]byte{'L'}, 2*page.PageSize)

			if err := db.Set([]byte("hit-a"), valA); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("hit-b"), valB); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("large"), valLarge); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("tomb"), valA); err != nil {
				t.Fatal(err)
			}
			if err := db.Delete([]byte("tomb")); err != nil {
				t.Fatal(err)
			}

			snap, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()

			budget := db.cow.budget

			// 1. Direct cowViewCopy check: reusable lease and buffer survive misses.
			// Warm snap's lazy read workspace on a backend miss and pointer read first
			// so subsequent budget deltas isolate cowViewResult leases cleanly.
			var warmRes cowViewResult
			if _, err := snap.cowViewCopy([]byte("missing"), &warmRes); err != nil {
				t.Fatal(err)
			}
			if _, err := snap.cowViewCopy([]byte("large"), &warmRes); err != nil {
				t.Fatal(err)
			}
			warmRes.close()
			beforeStats := budget.Stats()

			var res cowViewResult
			found, err := snap.cowViewCopy([]byte("hit-a"), &res)
			if err != nil || !found || !bytes.Equal(res.value, valA) {
				res.close()
				t.Fatalf("first hit: found=%t len=%d err=%v", found, len(res.value), err)
			}
			firstLease := res.lease
			firstBufPtr := &res.value[0]

			for _, missKey := range [][]byte{[]byte("missing"), []byte("tomb")} {
				found, err = snap.cowViewCopy(missKey, &res)
				if err != nil || found {
					res.close()
					t.Fatalf("miss %q: found=%t err=%v", missKey, found, err)
				}
				if res.lease != firstLease || len(res.value) != len(valA) || &res.value[0] != firstBufPtr {
					res.close()
					t.Fatalf("miss %q destroyed reusable cowViewResult: lease=%p (want %p)", missKey, res.lease, firstLease)
				}
			}

			found, err = snap.cowViewCopy([]byte("hit-b"), &res)
			if err != nil || !found || !bytes.Equal(res.value, valB) {
				res.close()
				t.Fatalf("second hit: found=%t len=%d err=%v", found, len(res.value), err)
			}
			if res.lease != firstLease || &res.value[0] != firstBufPtr {
				res.close()
				t.Fatalf("second hit after miss reallocated cowViewResult: lease=%p (want %p)", res.lease, firstLease)
			}
			res.close()

			// Non-reusable (> PageSize) result must still close on miss.
			found, err = snap.cowViewCopy([]byte("large"), &res)
			if err != nil || !found || !bytes.Equal(res.value, valLarge) {
				res.close()
				t.Fatalf("large hit: found=%t len=%d err=%v", found, len(res.value), err)
			}
			found, err = snap.cowViewCopy([]byte("missing"), &res)
			if err != nil || found || res.lease != nil || res.value != nil {
				res.close()
				t.Fatalf("miss after large hit retained non-reusable result: found=%t lease=%p len=%d err=%v", found, res.lease, len(res.value), err)
			}
			res.close()

			if got := budget.Stats(); got.ReservedBytes != beforeStats.ReservedBytes || got.ExternalLeases != beforeStats.ExternalLeases {
				t.Fatalf("cowViewCopy leaked budget: before=%+v after=%+v", beforeStats, got)
			}

			// 2. Snapshot.GetManyView: alternating hits & misses pass nil on miss and reuse O(1) buffer.
			keys := make([][]byte, 64)
			for i := range keys {
				switch i % 4 {
				case 0:
					keys[i] = []byte("hit-a")
				case 1:
					keys[i] = []byte("missing")
				case 2:
					keys[i] = []byte("hit-b")
				case 3:
					keys[i] = []byte("tomb")
				}
			}

			var calls int
			visit := func(index int, key, value []byte, found bool) error {
				if index != calls {
					return fmt.Errorf("index=%d want=%d", index, calls)
				}
				switch index % 4 {
				case 0:
					if !found || !bytes.Equal(value, valA) {
						return fmt.Errorf("hit-a mismatch: found=%t len=%d", found, len(value))
					}
				case 2:
					if !found || !bytes.Equal(value, valB) {
						return fmt.Errorf("hit-b mismatch: found=%t len=%d", found, len(value))
					}
				default:
					if found || value != nil {
						return fmt.Errorf("miss %q exposed value: found=%t value=%v", key, found, value)
					}
				}
				calls++
				return nil
			}

			// Warm snapshot and verify batch ending on a miss refunds lease completely.
			calls = 0
			if err := snap.GetManyView(keys, visit); err != nil || calls != len(keys) {
				t.Fatalf("GetManyView(64): calls=%d err=%v", calls, err)
			}
			afterView := budget.Stats()
			if afterView.ReservedBytes != beforeStats.ReservedBytes || afterView.ExternalLeases != beforeStats.ExternalLeases {
				t.Fatalf("GetManyView leaked lease after trailing miss: before=%+v after=%+v", beforeStats, afterView)
			}

			measure := func(n int) float64 {
				return testing.AllocsPerRun(20, func() {
					calls = 0
					if err := snap.GetManyView(keys[:n], visit); err != nil || calls != n {
						t.Fatalf("GetManyView(%d): calls=%d err=%v", n, calls, err)
					}
				})
			}
			short, long := measure(8), measure(len(keys))
			t.Logf("%s GetManyView alternating hit/miss allocs: 8 keys=%.0f, 64 keys=%.0f", mode, short, long)
			if long-short > 4 {
				t.Fatalf("56 extra alternating hit/miss keys added %.0f allocations (want <= 4)", long-short)
			}

			// 3. Large hit -> miss -> small hit still passes nil on miss and does not leak.
			mixed := [][]byte{[]byte("large"), []byte("missing"), []byte("hit-a"), []byte("tomb")}
			if err := snap.GetManyView(mixed, func(i int, _ []byte, value []byte, found bool) error {
				switch i {
				case 0:
					if !found || !bytes.Equal(value, valLarge) {
						return fmt.Errorf("mixed[0] large mismatch")
					}
				case 1, 3:
					if found || value != nil {
						return fmt.Errorf("mixed[%d] miss exposed value: found=%t value=%v", i, found, value)
					}
				case 2:
					if !found || !bytes.Equal(value, valA) {
						return fmt.Errorf("mixed[2] hit-a mismatch")
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("mixed GetManyView: %v", err)
			}

			// 4. Tight capacity retry still closes reusable result when workspace needs headroom.
			if err := db.Set([]byte("backend-hit"), valA); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if err := db.Set([]byte("inline-hit"), valA); err != nil {
				t.Fatal(err)
			}
			tightSnap, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer tightSnap.Close()

			cowPressureEvictIdleReadScratch(t, budget)
			baseTight := budget.Stats()
			header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			// Leave just enough room for a single 128B cowViewResult, but not enough for
			// cowReadWorkspace + cowPointScratch alongside it, to confirm retry releases result.
			// Here test that ErrCOWCapacity retry leaves no leaked lease.
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - baseTight.ReservedBytes - header - (memtable.COWAllocationCharge(128) + header))
			if err != nil {
				t.Fatal(err)
			}
			err = tightSnap.GetManyView([][]byte{[]byte("inline-hit"), []byte("missing"), []byte("backend-hit")}, func(i int, _ []byte, value []byte, found bool) error {
				if i == 1 && (found || value != nil) {
					return fmt.Errorf("tight miss exposed value")
				}
				return nil
			})
			pressure.Close()
			if !errors.Is(err, memtable.ErrCOWCapacity) {
				t.Fatalf("tight GetManyView err=%v want ErrCOWCapacity", err)
			}
			afterTight := budget.Stats()
			if afterTight.ReservedBytes != baseTight.ReservedBytes || afterTight.ExternalLeases != baseTight.ExternalLeases {
				t.Fatalf("tight retry leaked lease: before=%+v after=%+v", baseTight, afterTight)
			}
		})
	}
}

func TestCOWSnapshotMissParksPointScratch(t *testing.T) {
	db := cowReadWorkspaceFixture(t, "cow_btree", 4)
	if err := db.Set([]byte("present"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	missing := []byte("definitely-missing")
	var dst []byte
	runOne := func() {
		snap := db.AcquireSnapshot()
		if snap == nil {
			t.Fatal("nil snapshot")
		}
		var err error
		dst, err = snap.GetAppend(missing, dst[:0])
		if !errors.Is(err, tree.ErrKeyNotFound) {
			_ = snap.Close()
			t.Fatalf("GetAppend(missing) err=%v want ErrKeyNotFound", err)
		}
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cowPressureEvictIdleReadScratch(t, db.cow.budget)
	runOne()
	idle1, leases1 := db.cow.budget.TakeIdleReadScratch()
	if idle1 == nil || leases1[0] == nil || leases1[1] == nil {
		t.Fatalf("miss snapshot did not park workspace+pointScratch in idleRead: idle=%v leases=%+v", idle1 != nil, leases1)
	}
	w1 := idle1.(*cowReadWorkspace)
	if w1.point == nil {
		t.Fatal("parked workspace missing pointScratch")
	}
	firstPoint := w1.point
	if !db.cow.budget.PutIdleReadScratch(idle1, leases1) {
		t.Fatal("could not return parked workspace to idleRead")
	}

	for range 32 {
		runOne()
	}
	idle2, leases2 := db.cow.budget.TakeIdleReadScratch3()
	if idle2 == nil || leases2[0] == nil || leases2[1] == nil {
		t.Fatalf("repeated miss snapshots did not retain parked workspace: idle=%v leases=%+v", idle2 != nil, leases2)
	}
	w2 := idle2.(*cowReadWorkspace)
	if w2.point != firstPoint {
		t.Fatalf("miss snapshots reallocated pointScratch: got %p want %p", w2.point, firstPoint)
	}
	if !db.cow.budget.PutIdleReadScratch3(idle2, leases2) {
		t.Fatal("could not restore parked workspace")
	}

	// Verify compressed outer leaves retain and reuse stateless Snappy/LZ4 block codecs across Snapshot.Close().
	compDB, _, compWant := cowLeafCacheFixture(t, 64, true)
	var compDst []byte
	readComp := func() {
		snap := compDB.AcquireSnapshot()
		if snap == nil {
			t.Fatal("nil compressed snapshot")
		}
		var err error
		compDst, err = snap.GetAppend([]byte("a000"), compDst[:0])
		if err != nil || !bytes.Equal(compDst, compWant) {
			_ = snap.Close()
			t.Fatalf("compressed GetAppend=(%d,%v) want len=%d", len(compDst), err, len(compWant))
		}
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
	}
	cowPressureEvictIdleReadScratch(t, compDB.cow.budget)
	readComp()
	compIdle1, compLeases1 := compDB.cow.budget.TakeIdleReadScratch3()
	if compIdle1 == nil || compLeases1[0] == nil || compLeases1[1] == nil || compLeases1[2] == nil {
		t.Fatalf("compressed snapshot did not park workspace+point+blockCodec: idle=%v leases=%+v", compIdle1 != nil, compLeases1)
	}
	compW1 := compIdle1.(*cowReadWorkspace)
	firstCodec := compW1.parkedCodec
	if firstCodec == nil {
		t.Fatal("parked workspace missing stateless blockCodec")
	}
	if !compDB.cow.budget.PutIdleReadScratch3(compIdle1, compLeases1) {
		t.Fatal("could not restore parked compressed workspace")
	}
	for range 16 {
		readComp()
	}
	compIdle2, compLeases2 := compDB.cow.budget.TakeIdleReadScratch3()
	if compIdle2 == nil {
		t.Fatal("compressed snapshots lost parked workspace")
	}
	compW2 := compIdle2.(*cowReadWorkspace)
	if compW2.parkedCodec != firstCodec {
		t.Fatalf("compressed snapshots reallocated block codec: got %p want %p", compW2.parkedCodec, firstCodec)
	}
	if !compDB.cow.budget.PutIdleReadScratch3(compIdle2, compLeases2) {
		t.Fatal("could not restore compressed workspace")
	}
}

func BenchmarkCOWSnapshotPerKeyParallel(b *testing.B) {
	db := cowReadWorkspaceFixture(b, "cow_btree", 4)
	const numKeys = 4096
	keys := make([][]byte, numKeys)
	val := bytes.Repeat([]byte("v"), 128)
	batch := db.NewBatch()
	for i := range keys {
		k := make([]byte, 16)
		copy(k[:8], "key-par-")
		binary.BigEndian.PutUint64(k[8:], uint64(i))
		keys[i] = k
		if err := batch.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	// Warm settled backend leaf cache before timing.
	warmSnap := db.AcquireSnapshot()
	if warmSnap == nil {
		b.Fatal("nil warm snapshot")
	}
	var warmDst []byte
	for _, k := range keys {
		var err error
		warmDst, err = warmSnap.GetAppend(k, warmDst[:0])
		if err != nil {
			_ = warmSnap.Close()
			b.Fatal(err)
		}
	}
	if err := warmSnap.Close(); err != nil {
		b.Fatal(err)
	}

	if p := runtime.GOMAXPROCS(0); p < 8 {
		b.SetParallelism((8 + p - 1) / p)
	}
	var workerSeq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := workerSeq.Add(1) * 2654435761
		dst := make([]byte, 0, 256)
		for pb.Next() {
			idx = idx*6364136223846793005 + 1442695040888963407
			k := keys[idx&(numKeys-1)]
			snap := db.AcquireSnapshot()
			if snap == nil {
				b.Error("nil snapshot")
				return
			}
			var err error
			dst, err = snap.GetAppend(k, dst[:0])
			if err != nil {
				_ = snap.Close()
				b.Error(err)
				return
			}
			if len(dst) != len(val) {
				_ = snap.Close()
				b.Errorf("len=%d want=%d", len(dst), len(val))
				return
			}
			if err := snap.Close(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkCOWGetManyViewWithMisses(b *testing.B) {
	db := cowReadWorkspaceFixture(b, "cow_btree", 4)
	val := bytes.Repeat([]byte("m"), 128)
	const batchSize = 64
	keys := make([][]byte, batchSize)
	batch := db.NewBatch()
	for i := range keys {
		k := make([]byte, 16)
		copy(k[:8], "key-gmv-")
		binary.BigEndian.PutUint64(k[8:], uint64(i))
		keys[i] = k
		if i%2 == 0 {
			if err := batch.Set(k, val); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := batch.Write(); err != nil {
		b.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		b.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		b.Fatal("nil snapshot")
	}
	defer snap.Close()

	visit := func(i int, _ []byte, v []byte, found bool) error {
		if i%2 == 0 {
			if !found || len(v) != len(val) {
				return fmt.Errorf("unexpected hit at %d: found=%t len=%d", i, found, len(v))
			}
		} else if found || v != nil {
			return fmt.Errorf("unexpected miss at %d: found=%t v=%v", i, found, v)
		}
		return nil
	}
	// Warm the snapshot's read workspace before measuring GetManyView.
	if err := snap.GetManyView(keys, visit); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := snap.GetManyView(keys, visit); err != nil {
			b.Fatal(err)
		}
	}
}
