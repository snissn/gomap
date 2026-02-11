package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestCRUD(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get mismatch: %s", val)
	}

	has, err := db.Has([]byte("key1"))
	if err != nil || !has {
		t.Fatalf("Has failed: err=%v has=%v", err, has)
	}

	if err := db.Set([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("Iterator expected 2 items, got %d", count)
	}

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get deleted key returned error: %v", err)
	}
	if val != nil {
		t.Fatalf("Get deleted key should return nil")
	}

	has, err = db.Has([]byte("key1"))
	if err != nil {
		t.Fatalf("Has deleted key returned error: %v", err)
	}
	if has {
		t.Fatalf("Has deleted key should be false")
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v1")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	snap := db.AcquireSnapshot()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer snap.Close()

		for i := 0; i < 100; i++ {
			val, err := snap.Get([]byte(fmt.Sprintf("k%d", i)))
			if err != nil {
				t.Errorf("Snapshot.Get failed: %v", err)
				return
			}
			if !bytes.Equal(val, []byte("v1")) {
				t.Errorf("Snapshot isolation failed for k%d: got %q, want v1", i, val)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v2"))
		}
	}()

	wg.Wait()

	val, err := db.Get([]byte("k0"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Final state should be v2, got %q", val)
	}
}

func TestIteratorKeyValueAreDefensiveCopies(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("k1"), []byte("value1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid")
	}

	unsafeKeyBefore := append([]byte(nil), it.UnsafeKey()...)
	unsafeValBefore := append([]byte(nil), it.UnsafeValue()...)
	key := it.Key()
	val := it.Value()

	if len(key) > 0 && len(it.UnsafeKey()) > 0 && &key[0] == &it.UnsafeKey()[0] {
		t.Fatalf("Key returned unsafe alias")
	}
	if len(val) > 0 && len(it.UnsafeValue()) > 0 && &val[0] == &it.UnsafeValue()[0] {
		t.Fatalf("Value returned unsafe alias")
	}

	if len(key) > 0 {
		key[0] ^= 0x1
	}
	if len(val) > 0 {
		val[0] ^= 0x1
	}
	if !bytes.Equal(it.UnsafeKey(), unsafeKeyBefore) {
		t.Fatalf("mutating Key() changed iterator state")
	}
	if !bytes.Equal(it.UnsafeValue(), unsafeValBefore) {
		t.Fatalf("mutating Value() changed iterator state")
	}
}

func TestStatsIncludesWatermarkLagDriftMetric(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stats := db.Stats()
	if _, ok := stats["treedb.publish.watermark.lag_drift_bytes_per_sec"]; !ok {
		t.Fatalf("missing treedb.publish.watermark.lag_drift_bytes_per_sec")
	}
}
