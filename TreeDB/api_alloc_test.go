package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestGetAppend_ReusesBuffer(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("value")
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// 1. Buffer with capacity
	buf := make([]byte, 0, 100)
	res, err := db.GetAppend(key, buf)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(res, val) {
		t.Fatalf("content mismatch")
	}
	// Check if it reused the array (address check would be better but cap check is a strong signal)
	if cap(res) != 100 {
		t.Errorf("expected cap 100, got %d", cap(res))
	}
}

func TestGetAppend_AppendsToExistingData(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("world")
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// Buffer with existing data: "hello "
	buf := []byte("hello ")
	res, err := db.GetAppend(key, buf)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}

	expected := []byte("hello world")
	if !bytes.Equal(res, expected) {
		t.Fatalf("expected %q, got %q", expected, res)
	}
}

func TestGetAppend_EmptyValue(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte{} // Empty value
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	buf := []byte("prefix")
	res, err := db.GetAppend(key, buf)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(res, buf) {
		t.Fatalf("expected %q, got %q", buf, res)
	}
}

func TestGetAppend_Grow(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := bytes.Repeat([]byte("x"), 1000)
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// Small buffer, needs growth
	buf := make([]byte, 0, 10)
	res, err := db.GetAppend(key, buf)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(res, val) {
		t.Fatalf("content mismatch")
	}
	if len(res) != 1000 {
		t.Fatalf("length mismatch")
	}
}

func TestGet_SafeCopy(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("original")
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	// Get returns a copy
	got, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if cap(got) != len(got) {
		t.Fatalf("Get should cap result to length: len=%d cap=%d", len(got), cap(got))
	}

	// Mutate the returned slice
	got[0] = 'X'

	// Verify DB is unchanged
	got2, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "original" {
		t.Fatalf("DB corrupted by modification of Get result")
	}
	if cap(got2) != len(got2) {
		t.Fatalf("Get should cap result to length: len=%d cap=%d", len(got2), cap(got2))
	}
}

func TestGetUnsafe_ReturnsValue(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("original")
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetUnsafe(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Fatalf("GetUnsafe: got %q, want %q", string(got), "original")
	}
	got[0] = 'X'

	got2, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "original" {
		t.Fatalf("GetUnsafe should return a safe copy (backend), got %q", string(got2))
	}
}

func TestGetUnsafe_CachedReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("original")
	if err := db.Set(key, val); err != nil {
		t.Fatal(err)
	}

	got, err := db.GetUnsafe(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Fatalf("GetUnsafe: got %q, want %q", string(got), "original")
	}
	got[0] = 'X'

	got2, err := db.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != "original" {
		t.Fatalf("GetUnsafe should return a safe copy (cached), got %q", string(got2))
	}
}

func TestBatchSet_CopiesInput(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatch()
	if b == nil {
		t.Fatal("NewBatch returned nil")
	}

	key := []byte("k")
	val := []byte("original")
	if err := b.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}

	key[0] = 'x'
	val[0] = 'X'

	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("expected %q, got %q", "original", string(got))
	}
}

func TestBatchDelete_CopiesKey(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.SetSync([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	b := db.NewBatch()
	if b == nil {
		t.Fatal("NewBatch returned nil")
	}

	key := []byte("a")
	if err := b.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	key[0] = 'b'

	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected key to be deleted, got %q", string(got))
	}
}

func TestGetAppend_NotFound(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	buf := make([]byte, 0, 10)
	res, err := db.GetAppend([]byte("missing"), buf)
	if !errors.Is(err, treedb.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("expected empty result")
	}
}

func TestCachingGetAppend_FallsBackToBackend(t *testing.T) {
	dir := t.TempDir()
	// FlushThreshold small to force data to disk
	db, err := treedb.Open(treedb.Options{Dir: dir, FlushThreshold: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("value_on_disk")

	// 1. Write and Flush to Backend
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}
	// Force flush
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. GetAppend should find it in Backend
	res, err := db.GetAppend(key, nil)
	if err != nil {
		t.Fatalf("GetAppend: %v", err)
	}
	if !bytes.Equal(res, val) {
		t.Fatalf("mismatch: got %q", res)
	}
}

func TestCachingGetAppend_TombstoneInMemtable(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := []byte("initial")

	// 1. Write to Disk
	if err := db.SetSync(key, val); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 2. Delete in Memtable
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}

	// 3. GetAppend should see the delete (Not Found)
	_, err = db.GetAppend(key, nil)
	if !errors.Is(err, treedb.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for tombstone, got %v", err)
	}
}

func TestGetAppend_ZeroCopyMemtable(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := bytes.Repeat([]byte("x"), 100)
	if err := db.Set(key, val); err != nil {
		t.Fatal(err)
	}

	// GetAppend from memtable
	// We want to verify we got the bytes.
	// We can't strictly verify "Zero Copy" from outside without unsafe pointer checks,
	// but we can verify correctness.
	res, err := db.GetAppend(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(res, val) {
		t.Fatal("mismatch")
	}
}

func TestConsistency_Get_GetUnsafe_GetAppend(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keys := []string{"k1", "k2", "k3"}
	vals := []string{"v1", "v2", "v3"}

	for i, k := range keys {
		db.Set([]byte(k), []byte(vals[i]))
	}

	for i, k := range keys {
		kb := []byte(k)
		want := []byte(vals[i])

		// Get
		g1, _ := db.Get(kb)
		if !bytes.Equal(g1, want) {
			t.Errorf("Get(%s) mismatch", k)
		}

		// GetUnsafe
		g2, _ := db.GetUnsafe(kb)
		if !bytes.Equal(g2, want) {
			t.Errorf("GetUnsafe(%s) mismatch", k)
		}

		// GetAppend
		g3, _ := db.GetAppend(kb, nil)
		if !bytes.Equal(g3, want) {
			t.Errorf("GetAppend(%s) mismatch", k)
		}
	}
}

func TestConcurrency_GetAppend(t *testing.T) {
	// Simple race check
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("race")
	db.Set(key, []byte("start"))

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			db.Set(key, []byte(fmt.Sprintf("v%d", i)))
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			_, err := db.GetAppend(key, nil)
			if err != nil && !errors.Is(err, treedb.ErrKeyNotFound) {
				t.Fatalf("GetAppend error: %v", err)
			}
		}
	}
}
