package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

type simpleIterator interface {
	Valid() bool
	Next()
	Key() []byte
	Close() error
}

func openTempDB(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	opts.Dir = dir
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, dir
}

func TestPublicCRUDAndPersistence(t *testing.T) {
	db, dir := openTempDB(t, Options{})
	defer db.Close()

	// Nonexistent key returns nil.
	if v, err := db.Get([]byte("a")); err != nil || v != nil {
		t.Fatalf("expected nil for missing key, got %q err=%v", v, err)
	}

	// Basic set/get/delete.
	if err := db.Set([]byte("a"), []byte{1}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if ok, err := db.Has([]byte("a")); err != nil || !ok {
		t.Fatalf("has: %v ok=%v", err, ok)
	}
	if v, err := db.Get([]byte("a")); err != nil || !bytes.Equal(v, []byte{1}) {
		t.Fatalf("get: %v v=%q", err, v)
	}

	if err := db.Set([]byte("a"), []byte{2}); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if v, _ := db.Get([]byte("a")); !bytes.Equal(v, []byte{2}) {
		t.Fatalf("expected overwrite to win, got %q", v)
	}

	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if v, err := db.Get([]byte("a")); err != nil || v != nil {
		t.Fatalf("expected nil after delete, got %q err=%v", v, err)
	}

	// Persistence across reopen.
	if err := db.SetSync([]byte("p"), []byte("v")); err != nil {
		t.Fatalf("setsync: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if v, err := db2.Get([]byte("p")); err != nil || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("persistent get: %v v=%q", err, v)
	}
}

func TestPublicInputValidationMatchesCosmosDB(t *testing.T) {
	db, _ := openTempDB(t, Options{})
	defer db.Close()

	// Empty or nil keys are invalid.
	if _, err := db.Get([]byte{}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Get(empty), got %v", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Get(nil), got %v", err)
	}
	if _, err := db.Has([]byte{}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Has(empty), got %v", err)
	}
	if _, err := db.Has(nil); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Has(nil), got %v", err)
	}
	if err := db.Set([]byte{}, []byte{1}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Set(empty), got %v", err)
	}
	if err := db.Set(nil, []byte{1}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Set(nil), got %v", err)
	}
	if err := db.Delete([]byte{}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Delete(empty), got %v", err)
	}
	if err := db.Delete(nil); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty on Delete(nil), got %v", err)
	}

	// Nil values are invalid, empty values are fine.
	if err := db.Set([]byte("k"), nil); !errors.Is(err, ErrValueNil) {
		t.Fatalf("expected ErrValueNil on Set(nil value), got %v", err)
	}
	if err := db.Set([]byte("k"), []byte{}); err != nil {
		t.Fatalf("expected empty value ok, got %v", err)
	}
}

func TestCosmosStyleIteratorSuite(t *testing.T) {
	db, _ := openTempDB(t, Options{})
	defer db.Close()

	// Insert 0..9 skipping 6.
	for i := 0; i < 10; i++ {
		if i == 6 {
			continue
		}
		key := int642Bytes(int64(i))
		if err := db.Set(key, []byte{}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	// Empty iterator bounds should error.
	if _, err := db.Iterator([]byte{}, nil); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty for Iterator(empty,start)")
	}
	if _, err := db.Iterator(nil, []byte{}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty for Iterator(empty,end)")
	}
	if _, err := db.ReverseIterator([]byte{}, nil); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty for ReverseIterator(empty,start)")
	}
	if _, err := db.ReverseIterator(nil, []byte{}); !errors.Is(err, ErrKeyEmpty) {
		t.Fatalf("expected ErrKeyEmpty for ReverseIterator(empty,end)")
	}

	verifyIter := func(it simpleIterator, want []int64) {
		var got []int64
		for it.Valid() {
			got = append(got, bytes2Int64(it.Key()))
			it.Next()
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iterator mismatch got=%v want=%v", got, want)
		}
		if err := it.Close(); err != nil {
			t.Fatalf("close iterator: %v", err)
		}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	verifyIter(it, []int64{0, 1, 2, 3, 4, 5, 7, 8, 9})

	rit, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	verifyIter(rit, []int64{9, 8, 7, 5, 4, 3, 2, 1, 0})

	it, err = db.Iterator(nil, int642Bytes(0))
	if err != nil {
		t.Fatalf("iterator to 0: %v", err)
	}
	verifyIter(it, nil)

	rit, err = db.ReverseIterator(int642Bytes(10), nil)
	if err != nil {
		t.Fatalf("reverse iterator from 10: %v", err)
	}
	verifyIter(rit, nil)

	it, err = db.Iterator(int642Bytes(5), int642Bytes(8))
	if err != nil {
		t.Fatalf("iterator 5..8: %v", err)
	}
	verifyIter(it, []int64{5, 7})

	rit, err = db.ReverseIterator(int642Bytes(4), int642Bytes(2))
	if err != nil {
		t.Fatalf("reverse iterator 4..2: %v", err)
	}
	verifyIter(rit, nil)
}

func TestCosmosStyleBatchGetByteSize(t *testing.T) {
	db, _ := openTempDB(t, Options{})
	defer db.Close()

	b := db.NewBatch().(*Batch)
	if sz, err := b.GetByteSize(); err != nil || sz > 32 {
		t.Fatalf("expected small empty batch size, got %d err=%v", sz, err)
	}

	r := rand.New(rand.NewSource(1))
	totalKV := 0
	for i := 0; i < 100; i++ {
		ksz := r.Intn(32) + 1
		vsz := r.Intn(32) + 1
		key := make([]byte, ksz)
		val := make([]byte, vsz)
		r.Read(key)
		r.Read(val)
		totalKV += ksz + vsz
		if err := b.Set(key, val); err != nil {
			t.Fatalf("batch set: %v", err)
		}
	}
	if sz, err := b.GetByteSize(); err != nil {
		t.Fatalf("getbytesize: %v", err)
	} else if sz/totalKV != 1 {
		t.Fatalf("expected batch size ratio ~1, got %d/%d", sz, totalKV)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := b.GetByteSize(); err == nil {
		t.Fatalf("expected error on GetByteSize after write")
	}
}

func TestCosmosStyleBatchOperations(t *testing.T) {
	db, _ := openTempDB(t, Options{})
	defer db.Close()

	b2 := db.NewBatch().(*Batch)
	_ = b2.Set([]byte("a"), []byte{1})
	_ = b2.Set([]byte("b"), []byte{2})
	_ = b2.Set([]byte("c"), []byte{3})
	if v, _ := db.Get([]byte("a")); v != nil {
		t.Fatalf("unwritten batch visible")
	}
	if err := b2.Write(); err != nil {
		t.Fatalf("write2: %v", err)
	}
	assertKeyValues(t, db, map[string][]byte{"a": {1}, "b": {2}, "c": {3}})

	if err := b2.Set([]byte("a"), []byte{9}); err == nil {
		t.Fatalf("expected error on Set after write")
	}
	if err := b2.Delete([]byte("a")); err == nil {
		t.Fatalf("expected error on Delete after write")
	}
	if err := b2.Write(); err == nil {
		t.Fatalf("expected error on second Write")
	}
	if err := b2.WriteSync(); err == nil {
		t.Fatalf("expected error on WriteSync after write")
	}
	if err := b2.Close(); err != nil {
		t.Fatalf("close batch: %v", err)
	}

	b3 := db.NewBatch().(*Batch)
	if err := b3.Write(); err != nil {
		t.Fatalf("empty write: %v", err)
	}
}

func assertKeyValues(t *testing.T, db *DB, expect map[string][]byte) {
	t.Helper()
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	actual := make(map[string][]byte)
	for ; it.Valid(); it.Next() {
		actual[string(it.Key())] = it.Value()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if !reflect.DeepEqual(actual, expect) {
		t.Fatalf("expected %v, got %v", expect, actual)
	}
}

func TestPruningScenarioKeepsPinnedIteratorSafe(t *testing.T) {
	db, _ := openTempDB(t, Options{KeepRecent: 5})
	defer db.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("k%03d", i))
		if err := db.SetSync(key, []byte("v0")); err != nil {
			t.Fatalf("seed set: %v", err)
		}
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	// Produce many commits while iterator is pinned.
	for c := 0; c < 50; c++ {
		key := []byte(fmt.Sprintf("k%03d", c%100))
		if err := db.Set(key, []byte(fmt.Sprintf("v%02d", c+1))); err != nil {
			t.Fatalf("commit %d: %v", c, err)
		}
	}

	count := 0
	for ; it.Valid(); it.Next() {
		_ = it.Key()
		_ = it.Value()
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if count != 100 {
		t.Fatalf("expected 100 items in pinned snapshot, got %d", count)
	}
}

func TestModelRandomizedOpsAgainstMap(t *testing.T) {
	db, dir := openTempDB(t, Options{})
	defer db.Close()

	oracle := make(map[string][]byte)
	r := rand.New(rand.NewSource(99))

	for i := 0; i < 500; i++ {
		keyLen := r.Intn(8) + 1
		key := make([]byte, keyLen)
		r.Read(key)
		op := r.Intn(3)

		switch op {
		case 0: // Set
			valLen := r.Intn(64)
			val := make([]byte, valLen)
			r.Read(val)
			oracle[string(key)] = append([]byte(nil), val...)
			if err := db.Set(key, val); err != nil {
				t.Fatalf("set: %v", err)
			}
		case 1: // Delete
			delete(oracle, string(key))
			if err := db.Delete(key); err != nil {
				t.Fatalf("delete: %v", err)
			}
		case 2: // Reopen
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			var err error
			db, err = Open(Options{Dir: dir})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
		}

		if r.Intn(7) == 0 {
			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			exp := oracle[string(key)]
			if exp == nil && got != nil {
				t.Fatalf("expected nil for %x, got %x", key, got)
			}
			if exp != nil && !bytes.Equal(exp, got) {
				t.Fatalf("mismatch for %x exp=%x got=%x", key, exp, got)
			}
		}
	}

	// Final state check via iteration.
	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	actual := make(map[string][]byte)
	for ; it.Valid(); it.Next() {
		actual[string(it.Key())] = it.Value()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iter error: %v", err)
	}
	if !reflect.DeepEqual(actual, oracle) {
		t.Fatalf("oracle mismatch")
	}
}

func int642Bytes(i int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func bytes2Int64(buf []byte) int64 {
	if len(buf) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(buf))
}
