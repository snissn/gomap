package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
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
	db, keys, vals := cowGetManyTestFixture(t, 300, 512)
	budget := db.cow.budget
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
	cowPressureEvictIdleReadScratch(t, budget)
	// Confirm zero lease leaks.
	after := budget.Stats()
	if after.ExternalLeases < 0 || after.Views != before.Views {
		t.Fatalf("tight budget leaked views/leases: before=%+v after=%+v", before, after)
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
