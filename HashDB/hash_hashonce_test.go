package hashdb

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
)

func TestPutHashesOncePerInsert(t *testing.T) {
	orig := hashFn
	var calls atomic.Int64
	hashFn = func(b []byte) uint64 {
		calls.Add(1)
		return orig(b)
	}
	defer func() { hashFn = orig }()

	dir := t.TempDir()
	var db DB
	if err := db.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.SetCompression(false)

	const n = 10_000
	key := make([]byte, 8)
	val := make([]byte, 16)
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint64(val[:8], uint64(i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if got, want := calls.Load(), int64(n); got != want {
		t.Fatalf("hash calls=%d, want=%d", got, want)
	}
}

func TestPutManyHashesOncePerItem(t *testing.T) {
	orig := hashFn
	var calls atomic.Int64
	hashFn = func(b []byte) uint64 {
		calls.Add(1)
		return orig(b)
	}
	defer func() { hashFn = orig }()

	dir := t.TempDir()
	var db DB
	if err := db.Open(dir); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	db.SetCompression(false)

	const n = 5_000
	items := make([]Item, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 8)
		val := make([]byte, 16)
		binary.LittleEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint64(val[:8], uint64(i))
		items[i] = Item{Key: key, Value: val}
	}

	if err := db.PutMany(items); err != nil {
		t.Fatalf("putmany: %v", err)
	}

	if got, want := calls.Load(), int64(n); got != want {
		t.Fatalf("hash calls=%d, want=%d", got, want)
	}
}
