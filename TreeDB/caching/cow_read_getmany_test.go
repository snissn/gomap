package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func cowGetManyTestFixture(t testing.TB, count int, pointerThreshold int) (*DB, [][]byte, [][]byte) {
	return cowGetManyTestFixtureWithAutoClose(t, count, pointerThreshold, true)
}

func cowGetManyTestFixtureWithAutoClose(t testing.TB, count int, pointerThreshold int, autoClose bool) (*DB, [][]byte, [][]byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL:               true,
		AllowUnsafe:              true,
		MemtableMode:             "cow_btree",
		FlushThreshold:           1 << 30,
		MemtableShards:           4,
		ValueLogPointerThreshold: pointerThreshold,
		ValueLogCompression:      uint8(vlogCompressionOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	if autoClose {
		t.Cleanup(func() { _ = db.Close() })
	}

	keys := make([][]byte, count)
	vals := make([][]byte, count)
	b := db.NewBatch()
	for i := 0; i < count; i++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], uint64(i*2+2))
		keys[i] = append([]byte(nil), k[:]...)
		v := make([]byte, 128)
		binary.BigEndian.PutUint64(v[:8], uint64(i))
		for j := 8; j < len(v); j++ {
			v[j] = byte('a' + ((i + j) % 26))
		}
		vals[i] = v
		if err := b.Set(keys[i], vals[i]); err != nil {
			_ = b.Close()
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return db, keys, vals
}

func TestCOWGetManyAndGetManyViewParityMixedSources(t *testing.T) {
	// Include inline backend keys, memtable overrides, tombstones, misses, empty values, and pointers.
	db, keys, vals := cowGetManyTestFixture(t, 600, 256)

	// Override a few keys in memtable (inline, empty, pointer, tombstone).
	memInline := bytes.Repeat([]byte{'M'}, 64)
	memPointer := bytes.Repeat([]byte{'P'}, 512)
	if err := db.Set(keys[5], memInline); err != nil {
		t.Fatal(err)
	}
	vals[5] = memInline
	if err := db.Set(keys[10], []byte{}); err != nil {
		t.Fatal(err)
	}
	vals[10] = []byte{}
	if err := db.Set(keys[15], memPointer); err != nil {
		t.Fatal(err)
	}
	vals[15] = memPointer
	if err := db.Delete(keys[20]); err != nil {
		t.Fatal(err)
	}
	vals[20] = nil
	// Persist a backend pointer key as well.
	diskPointerKey := make([]byte, 8)
	binary.BigEndian.PutUint64(diskPointerKey, 999999)
	diskPointerVal := bytes.Repeat([]byte{'D'}, 512)
	if err := db.Set(diskPointerKey, diskPointerVal); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Build a shuffled/unsorted query batch of > 256 keys (hits parallel path) with duplicates and misses.
	query := make([][]byte, 0, len(keys)+20)
	expected := make([][]byte, 0, len(keys)+20)
	for i := len(keys) - 1; i >= 0; i-- {
		query = append(query, keys[i])
		expected = append(expected, vals[i])
	}
	query = append(query, diskPointerKey)
	expected = append(expected, diskPointerVal)
	for i := 0; i < 16; i++ {
		var miss [8]byte
		binary.BigEndian.PutUint64(miss[:], uint64(i*2+1))
		query = append(query, miss[:])
		expected = append(expected, nil)
	}
	// Duplicate hot keys.
	query = append(query, keys[5], keys[15], keys[100])
	expected = append(expected, vals[5], vals[15], vals[100])

	// 1. DB.GetMany parity and exact index order.
	got, err := db.GetMany(query)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != len(expected) {
		t.Fatalf("GetMany len=%d want=%d", len(got), len(expected))
	}
	for i := range expected {
		if expected[i] == nil {
			if got[i] != nil {
				t.Fatalf("GetMany[%d] want nil, got len=%d", i, len(got[i]))
			}
			continue
		}
		if got[i] == nil || !bytes.Equal(got[i], expected[i]) {
			t.Fatalf("GetMany[%d] mismatch: got len=%d want len=%d", i, len(got[i]), len(expected[i]))
		}
	}

	// 2. DB.GetManyView parity (parallel path >= 128 keys): every index visited exactly once.
	seen := make([]atomic.Int32, len(query))
	var viewMu sync.Mutex
	viewGot := make([][]byte, len(query))
	err = db.GetManyView(query, func(index int, key, value []byte, found bool) error {
		if index < 0 || index >= len(query) {
			return fmt.Errorf("out of range index %d", index)
		}
		if c := seen[index].Add(1); c != 1 {
			return fmt.Errorf("duplicate callback for index %d (count=%d)", index, c)
		}
		if !bytes.Equal(key, query[index]) {
			return fmt.Errorf("index %d key mismatch", index)
		}
		if found {
			cp := append([]byte{}, value...)
			viewMu.Lock()
			viewGot[index] = cp
			viewMu.Unlock()
		} else if value != nil {
			return fmt.Errorf("index %d miss had non-nil value", index)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("GetManyView: %v", err)
	}
	for i := range expected {
		if seen[i].Load() != 1 {
			t.Fatalf("GetManyView index %d seen=%d want=1", i, seen[i].Load())
		}
		if expected[i] == nil {
			if viewGot[i] != nil {
				t.Fatalf("GetManyView[%d] want miss, got len=%d", i, len(viewGot[i]))
			}
			continue
		}
		if viewGot[i] == nil || !bytes.Equal(viewGot[i], expected[i]) {
			t.Fatalf("GetManyView[%d] mismatch", i)
		}
	}

	// 3. Early-stop error propagation on parallel GetManyView.
	stopErr := errors.New("stop early")
	var errOnce atomic.Int32
	gotErr := db.GetManyView(query, func(index int, key, value []byte, found bool) error {
		if errOnce.Add(1) == 5 {
			return stopErr
		}
		return nil
	})
	if !errors.Is(gotErr, stopErr) {
		t.Fatalf("GetManyView early stop err=%v want %v", gotErr, stopErr)
	}
}

func TestCOWGetManyViewParallelTightBudgetNoDuplicateCallbacks(t *testing.T) {
	db, keys, vals := cowGetManyTestFixtureWithAutoClose(t, 300, 512, false)
	budget := db.cow.budget
	cowPressureEvictIdleReadScratch(t, budget)
	before := budget.Stats()

	// Apply tight budget pressure leaving enough headroom for snapshot + scalar fallback
	// while forcing parallel micro-chunk arena admission refusal on some/all workers.
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	avail := budget.Limits().MaxInFlightBytes - before.ReservedBytes - header
	// Leave ~48 KiB headroom: enough for Snapshot + 1 workspace/pointScratch + 1 cowViewResult,
	// tight enough that 4 parallel workers competing for arenas hit ErrCOWCapacity and fall back cleanly.
	var pressure *memtable.COWExternalLease
	if avail > 64<<10 {
		var err error
		pressure, err = budget.AcquireExternal(avail - (48 << 10))
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make([]atomic.Int32, len(keys))
	err := db.GetManyView(keys, func(index int, key, value []byte, found bool) error {
		if c := seen[index].Add(1); c != 1 {
			return fmt.Errorf("duplicate callback index=%d count=%d", index, c)
		}
		if !found || !bytes.Equal(value, vals[index]) {
			return fmt.Errorf("index=%d value mismatch found=%t", index, found)
		}
		return nil
	})
	if pressure != nil {
		pressure.Close()
	}
	if err != nil && !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("unexpected error under tight budget: %v", err)
	}
	if err == nil {
		for i := range keys {
			if seen[i].Load() != 1 {
				t.Fatalf("index %d seen=%d want 1", i, seen[i].Load())
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Confirm zero view or external-lease leaks after full DB close.
	after := budget.Stats()
	if after.TotalBytes != 0 || after.ExternalLeases != 0 || after.Views != 0 {
		t.Fatalf("tight budget leaked budget state after Close: before=%+v after=%+v", before, after)
	}
}

func TestCOWGetManyAndViewTightBudgetPointerFallback(t *testing.T) {
	// Reproduces tight COW budget with persisted value-log pointers where sequential
	// Snapshot.Get fits on a single workspace, ensuring DB.GetMany and DB.GetManyView
	// refund freshly admitted basis lane scratch when scalar fallback needs capacity.
	for _, mode := range []string{"GetMany", "GetManyView"} {
		t.Run(mode, func(t *testing.T) {
			db, keys, vals := cowGetManyTestFixtureWithAutoClose(t, 200, 128, false)
			// Persist 200 pointer values (1024B > 128B pointer threshold) into backend.
			for i := range keys {
				vals[i] = bytes.Repeat([]byte{byte('A' + i%26)}, 1024)
				if err := db.Set(keys[i], vals[i]); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			budget := db.cow.budget
			cowPressureEvictIdleReadScratch(t, budget)
			before := budget.Stats()

			// Leave tight headroom where sequential Snapshot.Get / Snapshot.GetManyView fits
			// on a single workspace (~32 KiB) while keeping a fresh basis lane workspace charged
			// (~20 KiB) would starve the cut-local fallback workspace if not refunded.
			const headroom = uint64(32 << 10)
			header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			avail := budget.Limits().MaxInFlightBytes - before.ReservedBytes - header
			if avail <= headroom+(4<<10) {
				t.Fatalf("unexpectedly small available budget: %d", avail)
			}
			pressure, err := budget.AcquireExternal(avail - headroom)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if pressure != nil {
					pressure.Close()
				}
			}()

			// Verify the sequential baseline succeeds under this exact pressure.
			snap := db.AcquireSnapshot()
			if mode == "GetMany" {
				gotSeq, gerr := snap.Get(keys[0])
				snapCloseErr := snap.Close()
				if gerr != nil || snapCloseErr != nil {
					t.Fatalf("baseline sequential Snapshot.Get failed under %d headroom: get=%v close=%v", headroom, gerr, snapCloseErr)
				}
				if !bytes.Equal(gotSeq, vals[0]) {
					t.Fatalf("baseline sequential value mismatch")
				}
			} else {
				gerr := snap.GetManyView(keys[:4], func(index int, key, value []byte, found bool) error {
					if !found || !bytes.Equal(value, vals[index]) {
						return fmt.Errorf("baseline view mismatch at %d", index)
					}
					return nil
				})
				snapCloseErr := snap.Close()
				if gerr != nil || snapCloseErr != nil {
					t.Fatalf("baseline sequential Snapshot.GetManyView failed under %d headroom: get=%v close=%v", headroom, gerr, snapCloseErr)
				}
			}
			// Evict idle scratch so the parallel lane starts cold (fresh=true).
			pressure.Close()
			pressure = nil
			cowPressureEvictIdleReadScratch(t, budget)
			before2 := budget.Stats()
			avail2 := budget.Limits().MaxInFlightBytes - before2.ReservedBytes - header
			pressure, err = budget.AcquireExternal(avail2 - headroom)
			if err != nil {
				t.Fatal(err)
			}

			if mode == "GetMany" {
				got, err := db.GetMany(keys)
				if err != nil {
					t.Fatalf("DB.GetMany failed under tight pointer-fallback budget: %v", err)
				}
				if len(got) != len(keys) {
					t.Fatalf("GetMany len=%d want %d", len(got), len(keys))
				}
				for i := range keys {
					if !bytes.Equal(got[i], vals[i]) {
						t.Fatalf("GetMany[%d] mismatch", i)
					}
				}
			} else {
				seen := make([]atomic.Int32, len(keys))
				err := db.GetManyView(keys, func(index int, key, value []byte, found bool) error {
					if c := seen[index].Add(1); c != 1 {
						return fmt.Errorf("duplicate callback index=%d count=%d", index, c)
					}
					if !found || !bytes.Equal(value, vals[index]) {
						return fmt.Errorf("index=%d value mismatch found=%t", index, found)
					}
					return nil
				})
				if err != nil {
					t.Fatalf("DB.GetManyView failed under tight pointer-fallback budget: %v", err)
				}
				for i := range keys {
					if seen[i].Load() != 1 {
						t.Fatalf("index %d seen=%d want 1", i, seen[i].Load())
					}
				}
			}
			pressure.Close()
			pressure = nil
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if after := budget.Stats(); after.TotalBytes != 0 || after.ExternalLeases != 0 || after.Views != 0 {
				t.Fatalf("pointer fallback leaked budget state: %+v", after)
			}
		})
	}
}

func TestCOWGetManyViewCallbackCanCloseDB(t *testing.T) {
	db, keys, _ := cowGetManyTestFixtureWithAutoClose(t, 200, 512, false)
	var closed atomic.Bool
	t.Cleanup(func() {
		if !closed.Swap(true) {
			_ = db.Close()
		}
	})
	err := db.GetManyView(keys, func(index int, key, value []byte, found bool) error {
		if !closed.Swap(true) {
			_ = db.Close()
		}
		return nil
	})
	if err != nil && !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("GetManyView callback Close err=%v", err)
	}
}

func TestCOWGetManyViewCallbackPanicJoinsWorkers(t *testing.T) {
	db, keys, vals := cowGetManyTestFixtureWithAutoClose(t, 1024, 512, false)
	budget := db.cow.budget
	workers, parallel := cowGetManyWorkerCount(len(keys))
	perWorker := (len(keys) + workers - 1) / workers

	siblingReady := make(chan struct{})
	var (
		siblingOnce         sync.Once
		panicStarted        atomic.Bool
		recovered           atomic.Bool
		activeSiblings      atomic.Int32
		postRecoverCallback atomic.Int32
	)

	var panicVal any
	func() {
		defer func() {
			panicVal = recover()
			recovered.Store(true)
			if n := activeSiblings.Load(); n != 0 {
				t.Errorf("sibling workers still active after panic unwind: %d", n)
			}
		}()
		_ = db.GetManyView(keys, func(index int, key, value []byte, found bool) error {
			if recovered.Load() {
				postRecoverCallback.Add(1)
				return nil
			}
			// keys is sorted ascending, so index 0 is always processed by inline worker 0
			// and indices >= perWorker are processed by spawned sibling workers when parallel.
			if index == 0 {
				if parallel {
					select {
					case <-siblingReady:
					case <-time.After(2 * time.Second):
						t.Errorf("timed out waiting for sibling worker to enter callback")
					}
				}
				panicStarted.Store(true)
				panic("cowGetManyView callback panic on worker 0")
			}
			if parallel && index >= perWorker {
				activeSiblings.Add(1)
				defer activeSiblings.Add(-1)
				siblingOnce.Do(func() { close(siblingReady) })
				deadline := time.Now().Add(2 * time.Second)
				for !panicStarted.Load() && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				// Hold sibling briefly while worker 0 unwinds so wg.Wait() must join us.
				time.Sleep(10 * time.Millisecond)
				if recovered.Load() {
					postRecoverCallback.Add(1)
				}
			}
			return nil
		})
	}()

	if panicVal != "cowGetManyView callback panic on worker 0" {
		t.Fatalf("unexpected panic value: %v", panicVal)
	}
	if n := activeSiblings.Load(); n != 0 {
		t.Fatalf("sibling workers remained active after recover: %d", n)
	}
	if n := postRecoverCallback.Load(); n != 0 {
		t.Fatalf("callbacks ran after panic recovery: %d", n)
	}

	// Verify pooled buffers and snapshot state remain clean on subsequent calls.
	seen := make([]atomic.Int32, len(keys))
	if err := db.GetManyView(keys, func(index int, key, value []byte, found bool) error {
		if c := seen[index].Add(1); c != 1 {
			return fmt.Errorf("duplicate callback index=%d count=%d", index, c)
		}
		if !found || !bytes.Equal(value, vals[index]) {
			return fmt.Errorf("index=%d value mismatch found=%t", index, found)
		}
		return nil
	}); err != nil {
		t.Fatalf("post-panic GetManyView: %v", err)
	}
	for i := range keys {
		if seen[i].Load() != 1 {
			t.Fatalf("post-panic index %d seen=%d want 1", i, seen[i].Load())
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if after := budget.Stats(); after.TotalBytes != 0 || after.ExternalLeases != 0 || after.Views != 0 {
		t.Fatalf("callback panic leaked budget state after Close: %+v", after)
	}
}

func TestCOWGetManyProbePoolClearsAndBounds(t *testing.T) {
	if got := unsafe.Sizeof(cowGetManyViewStaged{}); got > 32 {
		t.Fatalf("cowGetManyViewStaged size=%d want <= 32", got)
	}

	// Populate a probe slice with caller key slices, shrink len, and release.
	backing := make([]cowGetManyProbe, 8, 16)
	for i := range backing {
		backing[i] = cowGetManyProbe{origIdx: i + 1, key: []byte(fmt.Sprintf("secret-%d", i))}
	}
	sub := backing[:2]
	releaseCOWGetManyProbes(sub)
	full := backing[:cap(backing)]
	for i, p := range full {
		if p.origIdx != 0 || p.key != nil {
			t.Fatalf("probe slot %d not cleared after release: %+v", i, p)
		}
	}

	// Oversized slices (> cowGetManyProbePoolMaxCap) must not be pooled.
	for cowGetManyProbePool.Get() != nil {
	}
	oversized := make([]cowGetManyProbe, 1, cowGetManyProbePoolMaxCap+1)
	oversized[0] = cowGetManyProbe{origIdx: 42, key: []byte("big")}
	releaseCOWGetManyProbes(oversized)
	if raw := cowGetManyProbePool.Get(); raw != nil {
		t.Fatalf("oversized probe slice unexpectedly pooled: %T", raw)
	}
}

func BenchmarkCOWGetMany(b *testing.B) {
	db, keys, _ := cowGetManyTestFixture(b, 8000, 512)
	// Shuffle query keys deterministically to mirror random_read_batch.
	query := make([][]byte, len(keys))
	for i := range keys {
		query[i] = keys[(i*48271)%len(keys)]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		vals, err := db.GetMany(query)
		if err != nil || len(vals) != len(query) {
			b.Fatalf("GetMany: len=%d err=%v", len(vals), err)
		}
	}
}

func BenchmarkCOWGetManyViewParallel(b *testing.B) {
	db, keys, _ := cowGetManyTestFixture(b, 8000, 512)
	// Shuffle query keys deterministically to mirror random_read_batch.
	query := make([][]byte, len(keys))
	for i := range keys {
		query[i] = keys[(i*48271)%len(keys)]
	}
	fn := func(index int, _ []byte, value []byte, found bool) error {
		if index < 0 || index >= len(query) {
			return fmt.Errorf("out of range index %d", index)
		}
		if !found || len(value) != 128 {
			return fmt.Errorf("unexpected view at %d: found=%t len=%d", index, found, len(value))
		}
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := db.GetManyView(query, fn); err != nil {
			b.Fatalf("GetManyView: %v", err)
		}
	}
}
