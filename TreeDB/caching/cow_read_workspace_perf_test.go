package caching

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/compress/zstd"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
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

type cowPerfDictionaryLeafLifecycle interface {
	backenddb.LeafPageLog
	backenddb.LeafPageLogCreatedSegmentProvider
	backenddb.LeafPageLogCurrentSegmentProvider
	backenddb.LeafPageLogSegmentRegistrationObserver
	backenddb.LeafPageLogCachedWrapperOwner
	backenddb.LeafPageLogSequenceReserver
	backenddb.LeafPageLogPrivateCOWPackHandoff
	CurrentValueLogSegment() (string, uint32, bool)
	ProtectedLeafGenerationRootIDs() []uint64
	ProtectedLeafGenerationSystemRootIDs() []uint64
	ProtectedLeafGenerationRootIDPair() ([]uint64, []uint64)
	ProtectedLeafGenerationRootIDPairSnapshot() ([]uint64, []uint64, uint64)
}

// cowPerfDictionaryLeafLog is a cross-platform copy of the dictionary leaf
// appender helper so cow_read_workspace_perf_test.go compiles on Windows too.
type cowPerfDictionaryLeafLog struct {
	cowPerfDictionaryLeafLifecycle
	db         *DB
	dictID     uint64
	dictionary []byte
}

func (l *cowPerfDictionaryLeafLog) LeafPageLogLane(workerIndex int) (backenddb.LeafPageLog, bool) {
	if l == nil || workerIndex != 0 {
		return nil, false
	}
	return l, true
}

func (l *cowPerfDictionaryLeafLog) AppendLeafPage(leaf []byte) (page.LeafLogPtr, error) {
	ptr, resources, err := l.AppendLeafPageWithStableResources(leaf)
	resources.Release()
	return ptr, err
}

func (l *cowPerfDictionaryLeafLog) AppendLeafPages(leaves [][]byte) ([]page.LeafLogPtr, error) {
	ptrs, resources, err := l.AppendLeafPagesWithStableResources(leaves)
	resources.Release()
	return ptrs, err
}

func (l *cowPerfDictionaryLeafLog) AppendLeafPageWithStableResources(leaf []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	ptrs, resources, err := l.AppendLeafPagesWithStableResources([][]byte{leaf})
	if err != nil {
		return page.LeafLogPtr{}, nil, err
	}
	return ptrs[0], resources, nil
}

func (l *cowPerfDictionaryLeafLog) AppendLeafPagesWithStableResources(leaves [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if len(leaves) == 0 {
		return nil, nil, nil
	}
	start, err := l.db.ReserveValueLogRIDs(len(leaves))
	if err != nil {
		return nil, nil, err
	}
	records := make([]valuelog.Record, len(leaves))
	for i, leaf := range leaves {
		payload, _, err := valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			return nil, nil, err
		}
		records[i] = valuelog.Record{RID: start + uint64(i), Value: payload}
	}
	ptrs, resources, err := l.db.appendValueLogWithStableResources(&l.db.leafLog, l.dictID, l.dictionary, records, journalDurabilityNone)
	if err != nil {
		return nil, nil, err
	}
	defer putValueLogPtrs(ptrs)
	if len(ptrs) != len(leaves) {
		resources.Release()
		return nil, nil, fmt.Errorf("dictionary leaf append returned %d pointers for %d pages", len(ptrs), len(leaves))
	}
	refs := make([]page.LeafLogPtr, len(ptrs))
	for i, ptr := range ptrs {
		refs[i], err = page.LeafLogPtrFromValuePtr(ptr)
		if err != nil {
			resources.Release()
			return nil, nil, err
		}
		l.db.noteLeafGenerationRecordLength(ptr)
	}
	return refs, resources, nil
}

type cowCountingDictProvider struct {
	*dictdb.Store
	prepareCalls atomic.Int64
}

func (p *cowCountingDictProvider) PrepareDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, resources int, admit func(dictdb.DictionaryReadAllocationSizes) (func(), error)) (*dictdb.DictionaryReadDefinition, error) {
	p.prepareCalls.Add(1)
	return p.Store.PrepareDictionaryReadDefinition(id, limits, resources, admit)
}

func cowStandaloneLimitsForTest() memtable.COWLimits {
	limits := memtable.DefaultCOWLimits()
	limits.MaxInFlightBytes = 512 << 20
	limits.MaxTotalBytes = 1 << 30
	limits.MaxRetiredBytes = 1 << 30
	return limits
}

type cowDictLeafReadFixture struct {
	db      *DB
	backend *backenddb.DB
	capture *NativeSemanticCapture
	store   *cowCountingDictProvider
	rootID  uint64
	keys    [][]byte
	want    []byte
	valPtr  page.ValuePtr
	valWant []byte
	dictID  uint64
}

func newCOWConcurrentDictionaryLeafFixture(tb testing.TB, limits memtable.COWLimits, numKeys int) *cowDictLeafReadFixture {
	tb.Helper()
	dir := tb.TempDir()
	var store *dictdb.Store
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                        filepath.Join(dir, "backend"),
		Durability:                 backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		LeafPageReadCacheEntries:   -1, // disable verified leaf cache so every read exercises physical dictionary decode
		ValueLog: backenddb.ValueLogOptions{
			DictLookup: func(id uint64) ([]byte, error) {
				if store == nil {
					return nil, valuelog.ErrMissingDict
				}
				return store.GetDictBytes(context.Background(), id)
			},
		},
	})
	if err != nil {
		tb.Fatal(err)
	}
	owner := transactionowner.New()
	if err := backend.BindPrivateCOWOwner(owner); err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	opts := Options{
		TransactionOwner:                    owner,
		DisableWAL:                          true,
		AllowUnsafe:                         true,
		MemtableMode:                        "cow_btree",
		COWMemtableLimits:                   limits,
		MemtableShards:                      4,
		FlushThreshold:                      1 << 30,
		IndexOuterLeavesInValueLog:          true,
		ValueLogPointerThreshold:            256,
		ValueLogCompression:                 uint8(vlogCompressionDict),
		ValueLogAutoPolicy:                  uint8(vlogAutoSize),
		ValueLogCompressionAutotune:         valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
		ValueLogDictIncompressibleHoldBytes: -1,
	}
	opts.ValueLogDictTrain.TrainBytes = -1
	db, err := Open(dir, backend, opts)
	if err != nil {
		_ = backend.Close()
		tb.Fatal(err)
	}
	db.valueLogDictMaxK = 1
	tb.Cleanup(func() {
		if !db.closing.Load() {
			if err := db.Close(); err != nil {
				tb.Error(err)
			}
		}
	})
	if err := backend.BindNativeSemanticBridge(owner, db); err != nil {
		tb.Fatal(err)
	}

	want := bytes.Repeat([]byte("secondary-index-email-leaf-val|"), 4)
	valWant := bytes.Repeat([]byte("secondary-index-dict-value-ptr|"), 128)
	samples := make([][]byte, 8)
	var history []byte
	for sample := range samples {
		leaf := make([]byte, page.PageSize)
		builder := node.NewBuilderWithOptions(leaf, page.PageTypeLeaf, node.BuilderOptions{})
		for i := 0; i < 16; i++ {
			k := []byte(fmt.Sprintf("email-%06d", sample*16+i))
			if err := builder.AddLeafEntry(k, want, node.FlagInline, page.ValuePtr{}); err != nil {
				tb.Fatal(err)
			}
		}
		builder.FinishNoNode()
		samples[sample], _, err = valuelog.MaybeCompactLeafLogPayload(leaf)
		if err != nil {
			tb.Fatal(err)
		}
		history = append(history, samples[sample]...)
	}
	// Build a realistic ~112 KiB dictionary matching production outer-leaf dicts.
	for len(history) < 112<<10 {
		history = append(history, samples[len(history)%len(samples)]...)
	}
	if len(history) > 112<<10 {
		history = history[:112<<10]
	}
	dictionary, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       73,
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    zstd.SpeedFastest,
	})
	if err != nil {
		tb.Fatal(err)
	}
	store, err = dictdb.Open(filepath.Join(dir, "dictdb"), backenddb.Options{ChunkSize: 64 << 10})
	if err != nil {
		tb.Fatal(err)
	}
	dictID, err := store.PutDictBytes(context.Background(), dictionary)
	if err == nil {
		err = store.SetCurrent(context.Background(), dictID)
	}
	if err == nil {
		err = store.SetK(context.Background(), dictID, 1)
	}
	if err != nil {
		_ = store.Close()
		tb.Fatal(err)
	}
	counting := &cowCountingDictProvider{Store: store}
	db.SetDictStore(counting)
	backend.SetLeafPageLog(&cowPerfDictionaryLeafLog{
		cowPerfDictionaryLeafLifecycle: &cachingLeafPageLog{db: db, lane: &db.leafLog},
		db:                             db,
		dictID:                         dictID,
		dictionary:                     dictionary,
	})

	keys := make([][]byte, numKeys)
	batch := db.NewBatch()
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("email-%06d", i))
		keys[i] = k
		if err := batch.Set(k, want); err != nil {
			tb.Fatal(err)
		}
	}
	if err := batch.Set([]byte("dict-value-ptr"), valWant); err != nil {
		tb.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		tb.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		tb.Fatal(err)
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		tb.Fatal(err)
	}
	capRaw, err := backend.CaptureNativeSemanticState()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = capRaw.Close()
	})
	capture := capRaw.(*NativeSemanticCapture)
	token, ok := capture.Snapshot().StateToken()
	if !ok {
		tb.Fatal("missing basis state token")
	}

	// Verify real dictionary-compressed outer leaf provenance.
	var ks, ls [page.PageSize]byte
	var leafPtr page.LeafLogPtr
	stop := errors.New("stop")
	_, err = capture.Snapshot().GetEntryExactWithFixedScratch(keys[0], ks[:], ls[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		leafPtr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || leafPtr.FileID == 0 {
		tb.Fatalf("expected outer leaf ptr, got %+v err=%v", leafPtr, err)
	}
	f, _, err := capture.Snapshot().PinnedValueLogFile(leafPtr.ValueLogFileID())
	if err != nil {
		tb.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, leafPtr.ValuePtr(), db.cow.readLimits())
	if err != nil || !shape.Compressed || shape.DictID != dictID {
		tb.Fatalf("expected dictionary leaf shape dictID=%d, got %+v err=%v", dictID, shape, err)
	}

	ptrEntry, err := capture.Snapshot().GetEntryAtRoot(token.RootPageID, []byte("dict-value-ptr"))
	if err != nil || ptrEntry.Flags&node.FlagPointer == 0 {
		tb.Fatalf("expected value pointer entry, got %+v err=%v", ptrEntry, err)
	}
	vf, _, err := capture.Snapshot().PinnedValueLogFile(ptrEntry.ValuePtr.FileID)
	if err != nil {
		tb.Fatal(err)
	}
	vshape, err := valuelog.InspectCOWRecord(vf, ptrEntry.ValuePtr, db.cow.readLimits())
	if err != nil || !vshape.Compressed || vshape.DictID != dictID {
		tb.Fatalf("expected dictionary value shape dictID=%d, got %+v err=%v", dictID, vshape, err)
	}

	return &cowDictLeafReadFixture{
		db:      db,
		backend: backend,
		capture: capture,
		store:   counting,
		rootID:  token.RootPageID,
		keys:    keys,
		want:    want,
		valPtr:  ptrEntry.ValuePtr,
		valWant: valWant,
		dictID:  dictID,
	}
}

func TestCOWPhysicalDictionaryMultiLaneReuseAndClose(t *testing.T) {
	fix := newCOWConcurrentDictionaryLeafFixture(t, cowStandaloneLimitsForTest(), 512)
	budget := fix.db.cow.budget

	// 1. 16 concurrent readers (matching the 16 physical decoder lanes) performing
	// 64 dictionary-compressed leaf reads each = 1024 decodes. With 16 cached lanes,
	// dictionary preparation must be bounded to ~1 per lane rather than per-read churn.
	const laneReaders = 16
	const readsPerLane = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, laneReaders)
	for g := 0; g < laneReaders; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			dst := make([]byte, 0, len(fix.want))
			for j := 0; j < readsPerLane; j++ {
				k := fix.keys[(id*31+j*7)%len(fix.keys)]
				e, err := fix.capture.NativeEntryAtRootInto(fix.rootID, k, dst[:0])
				if err != nil {
					errs <- fmt.Errorf("g=%d j=%d leaf err=%v", id, j, err)
					return
				}
				if !bytes.Equal(e.Value, fix.want) {
					errs <- fmt.Errorf("g=%d j=%d leaf value mismatch len=%d", id, j, len(e.Value))
					return
				}
				dst = e.Value[:0]
			}
		}(g)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	prepAfter16 := fix.store.prepareCalls.Load()
	t.Logf("16 readers x %d dict leaf reads: PrepareDictionaryReadDefinition calls = %d", readsPerLane, prepAfter16)
	if prepAfter16 == 0 || prepAfter16 > 64 {
		t.Fatalf("excessive per-read dictionary preparation churn under 16 readers: calls=%d (want 1..64 for %d reads)", prepAfter16, laneReaders*readsPerLane)
	}

	// 2. 32 concurrent readers (> 16 lanes) exercising both dictionary-compressed
	// outer leaves and dictionary-compressed value pointers simultaneously.
	const readers32 = 32
	const readsPer32 = 64
	prepBefore32 := fix.store.prepareCalls.Load()
	start32 := make(chan struct{})
	errs32 := make(chan error, readers32)
	for g := 0; g < readers32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start32
			dst := make([]byte, 0, len(fix.want))
			vdst := make([]byte, 0, len(fix.valWant))
			for j := 0; j < readsPer32; j++ {
				k := fix.keys[(id*37+j*11)%len(fix.keys)]
				e, err := fix.capture.NativeEntryAtRootInto(fix.rootID, k, dst[:0])
				if err != nil || !bytes.Equal(e.Value, fix.want) {
					errs32 <- fmt.Errorf("r32 g=%d j=%d leaf err=%v len=%d", id, j, err, len(e.Value))
					return
				}
				dst = e.Value[:0]
				if j%8 == 0 {
					v, err := fix.capture.NativeValueAtPointerInto(fix.valPtr, vdst[:0], nil)
					if err != nil || !bytes.Equal(v, fix.valWant) {
						errs32 <- fmt.Errorf("r32 g=%d j=%d value err=%v len=%d", id, j, err, len(v))
						return
					}
					vdst = v[:0]
				}
			}
		}(g)
	}
	close(start32)
	wg.Wait()
	close(errs32)
	for err := range errs32 {
		t.Fatal(err)
	}
	prepDelta32 := fix.store.prepareCalls.Load() - prepBefore32
	t.Logf("32 readers x %d dict leaf+value reads: additional PrepareDictionaryReadDefinition calls = %d", readsPer32, prepDelta32)
	if prepDelta32 > 512 {
		t.Fatalf("excessive dictionary preparation churn under 32 readers: delta=%d (want <= 512 for %d+ reads)", prepDelta32, readers32*readsPer32)
	}

	// 3. Deterministically force lane-0 contention before asserting spillover
	// slot population (so single-CPU / GOMAXPROCS=1 runners never flake), and
	// verify holding Lane 0 (physicalMu) does NOT block
	// tryReleaseIdleGenericReadScratch() from draining cached dictionary codecs
	// in idle spillover lanes.
	if got := fix.db.cow.maxRetainedPhysicalSlots(); got != len(fix.db.cow.physicalSlots) {
		t.Fatalf("Standalone maxRetainedPhysicalSlots=%d want %d", got, len(fix.db.cow.physicalSlots))
	}
	fix.db.cow.physicalMu.Lock()
	var spillDone sync.WaitGroup
	spillDone.Add(1)
	var spillErr error
	go func() {
		defer spillDone.Done()
		_, spillErr = fix.capture.NativeEntryAtRoot(fix.rootID, fix.keys[0], true)
	}()
	spillDone.Wait()
	if spillErr != nil {
		fix.db.cow.physicalMu.Unlock()
		t.Fatalf("spillover read while lane 0 held: %v", spillErr)
	}
	warmSpillSlots := 0
	for i := range fix.db.cow.physicalSlots {
		fix.db.cow.physicalSlots[i].mu.Lock()
		if fix.db.cow.physicalSlots[i].reader != nil {
			warmSpillSlots++
		}
		fix.db.cow.physicalSlots[i].mu.Unlock()
	}
	if warmSpillSlots == 0 {
		fix.db.cow.physicalMu.Unlock()
		t.Fatal("expected at least one physicalSlots spillover lane to be populated while lane 0 held")
	}
	if !fix.capture.TryReleaseIdleNativeReadScratch() {
		fix.db.cow.physicalMu.Unlock()
		t.Fatal("TryReleaseIdleNativeReadScratch returned false while idle spillover lane held dictionary codec")
	}
	// Lane 0's cached dictionary codec must still be intact because physicalMu was held.
	lane0HasDict := false
	if fix.db.cow.physicalReader != nil {
		for _, c := range fix.db.cow.physicalReader.codecs {
			if c != nil && c.id == fix.dictID {
				lane0HasDict = true
			}
		}
	}
	fix.db.cow.physicalMu.Unlock()
	if !lane0HasDict {
		t.Fatal("TryReleaseIdleNativeReadScratch disturbed busy lane-0 dictionary codec")
	}

	// Now Lane 0 itself is idle and must also be releasable.
	if !fix.capture.TryReleaseIdleNativeReadScratch() {
		t.Fatal("TryReleaseIdleNativeReadScratch did not release idle lane-0 dictionary codec")
	}
	if fix.capture.TryReleaseIdleNativeReadScratch() {
		t.Fatal("TryReleaseIdleNativeReadScratch reported release when all lanes were already drained")
	}

	// 4. Headroom guard: when non-maintenance ReservedBytes reaches half of the
	// usable (non-maintenance) in-flight budget, cold spillover slots must NOT be
	// lazily admitted; reads spill to ephemeral temp workspaces and refund 100% of
	// temporary reservations on return.
	fix.db.cow.closePhysicalReader()
	beforeGuard := budget.Stats()
	maxInFlight := budget.Limits().MaxInFlightBytes
	maintBytes := fix.db.cow.maintenance.Limits().MaxInFlightBytes
	guardThreshold := maintBytes + (maxInFlight-maintBytes)/2
	var headroomPressure *memtable.COWExternalLease
	if beforeGuard.ReservedBytes < guardThreshold {
		var err error
		headroomPressure, err = budget.AcquireExternal(guardThreshold - beforeGuard.ReservedBytes + (1 << 20))
		if err != nil {
			t.Fatal(err)
		}
	}
	fix.db.cow.physicalMu.Lock()
	beforeTemp := budget.Stats()
	if _, err := fix.capture.NativeEntryAtRoot(fix.rootID, fix.keys[0], true); err != nil {
		fix.db.cow.physicalMu.Unlock()
		headroomPressure.Close()
		t.Fatalf("headroom-guarded temp spill read: %v", err)
	}
	afterTemp := budget.Stats()
	fix.db.cow.physicalMu.Unlock()
	headroomPressure.Close()
	if afterTemp.ReservedBytes != beforeTemp.ReservedBytes || afterTemp.ExternalLeases != beforeTemp.ExternalLeases {
		t.Fatalf("headroom-guarded read retained spillover slot instead of temp spill: before=%+v after=%+v", beforeTemp, afterTemp)
	}
	for i := range fix.db.cow.physicalSlots {
		fix.db.cow.physicalSlots[i].mu.Lock()
		r := fix.db.cow.physicalSlots[i].reader
		fix.db.cow.physicalSlots[i].mu.Unlock()
		if r != nil {
			t.Fatalf("physicalSlots[%d] was lazily admitted despite ReservedBytes >= MaxInFlightBytes/2", i)
		}
	}

	// 5. Shared-lane ErrCOWCapacity reclamation (:408): warm Lane 0 and Slot 0
	// with dictionary codecs, then fill the remaining budget so Slot 1 cannot
	// admit its dictionary decoder without reclaiming idle codecs on other lanes.
	if _, err := fix.capture.NativeEntryAtRoot(fix.rootID, fix.keys[0], true); err != nil {
		t.Fatal(err)
	}
	fix.db.cow.physicalMu.Lock()
	if _, err := fix.capture.NativeEntryAtRoot(fix.rootID, fix.keys[1], true); err != nil {
		fix.db.cow.physicalMu.Unlock()
		t.Fatal(err)
	}
	// Pre-admit Slot 1's workspace (without codec) while headroom exists.
	slot1Idx := -1
	for i := range fix.db.cow.physicalSlots {
		fix.db.cow.physicalSlots[i].mu.Lock()
		if fix.db.cow.physicalSlots[i].reader == nil && slot1Idx < 0 {
			w, err := fix.db.cow.physicalWorkspaceSlotLocked(&fix.db.cow.physicalSlots[i].reader, fix.db.cow.readLimits())
			if err != nil {
				fix.db.cow.physicalSlots[i].mu.Unlock()
				fix.db.cow.physicalMu.Unlock()
				t.Fatal(err)
			}
			_ = w
			slot1Idx = i
		}
		fix.db.cow.physicalSlots[i].mu.Unlock()
	}
	fix.db.cow.physicalMu.Unlock()
	if slot1Idx < 0 {
		t.Fatal("failed to prepare empty spillover slot for ErrCOWCapacity reclamation test")
	}
	// Leave enough room for dictionary definition (~112 KiB) + overhead, but NOT
	// enough for a second ~500+ KiB bounded decoder without reclaiming idle lanes.
	beforeReclaim := budget.Stats()
	headroom := uint64(256 << 10)
	leaseOverhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	if budget.Limits().MaxInFlightBytes > beforeReclaim.ReservedBytes+headroom+leaseOverhead {
		reclaimPressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - beforeReclaim.ReservedBytes - headroom - leaseOverhead)
		if err != nil {
			t.Fatal(err)
		}
		// Lock all lanes except slot1Idx so the read lands on slot1Idx and triggers
		// :408 idle reclamation, wait—leave Lane 0 unlocked so slot1Idx can reclaim
		// Lane 0's idle dictionary codec! Lock all OTHER slots that aren't Lane 0 or slot1Idx,
		// and acquire slot1Idx directly through readInspectedPhysicalLeaf? Simpler: hold
		// every slot except Lane 0 and slot1Idx, hold Lane 0? No, if Lane 0 is unlocked,
		// tryLockPhysicalLane picks Lane 0! So to test slot1Idx reclaiming idle Lane 0:
		// directly invoke codecWithDictionary via readNativeValueWithWorkspace on slot1Idx
		// while holding slot1Idx.mu (Lane 0 unlocked and idle with cached dict codec).
		slot1 := &fix.db.cow.physicalSlots[slot1Idx]
		slot1.mu.Lock()
		gotVal, err := fix.capture.cut.basis.readNativeValueWithWorkspace(fix.db.cow, slot1.reader, fix.store, fix.valPtr, nil, false, nil)
		slot1.mu.Unlock()
		reclaimPressure.Close()
		if err != nil || !bytes.Equal(gotVal, fix.valWant) {
			t.Fatalf("shared physical lane failed to reclaim idle codecs on ErrCOWCapacity: len=%d err=%v", len(gotVal), err)
		}
	}

	// 6. Close capture and DB: 100% of lane 0 + spillover leases must be refunded.
	if err := fix.capture.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fix.db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("close leaked physical lane budget: %+v", stats)
	}

	// 7. Dynamic slot cap under DefaultCOWLimits (64 MiB -> 3 extra slots = 4 lanes).
	defFix := newCOWConcurrentDictionaryLeafFixture(t, memtable.DefaultCOWLimits(), 64)
	if got := defFix.db.cow.maxRetainedPhysicalSlots(); got != 3 {
		t.Fatalf("DefaultCOWLimits maxRetainedPhysicalSlots=%d want 3", got)
	}
}

func BenchmarkCOWConcurrentDictionaryLeafRead32(b *testing.B) {
	fix := newCOWConcurrentDictionaryLeafFixture(b, cowStandaloneLimitsForTest(), 512)
	if p := runtime.GOMAXPROCS(0); p < 32 {
		b.SetParallelism((32 + p - 1) / p)
	}
	// Warm lane 0 once outside timer.
	if _, err := fix.capture.NativeEntryAtRoot(fix.rootID, fix.keys[0], true); err != nil {
		b.Fatal(err)
	}
	var workerSeq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := workerSeq.Add(1) * 2654435761
		dst := make([]byte, 0, len(fix.want))
		for pb.Next() {
			idx = idx*6364136223846793005 + 1442695040888963407
			k := fix.keys[idx%uint64(len(fix.keys))]
			e, err := fix.capture.NativeEntryAtRootInto(fix.rootID, k, dst[:0])
			if err != nil {
				b.Error(err)
				return
			}
			if len(e.Value) != len(fix.want) {
				b.Errorf("len=%d want=%d", len(e.Value), len(fix.want))
				return
			}
			dst = e.Value[:0]
		}
	})
}
