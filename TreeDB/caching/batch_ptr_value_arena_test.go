package caching

import (
	"bytes"
	"testing"
)

func TestBatchPtrValueArena_RetainedWhenPointersDenied(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:                  true,
		DisableWAL:                   true,
		MemtableMode:                 "btree",
		MemtableShards:               1,
		FlushThreshold:               1 << 30,
		ValueLogPointerThreshold:     1,
		MaxValueLogRetainedBytesHard: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Force allowValueLogPointers() to return false even though values are eligible.
	db.valueLogRetainedClosedBytes.Store(db.maxValueLogRetainedBytesHard)

	firstKey := []byte("k1")
	firstVal := bytes.Repeat([]byte{0x11}, 64)
	secondKey := []byte("k2")
	secondVal := bytes.Repeat([]byte{0x22}, 64)

	b := db.NewBatchWithSize(1)
	if err := b.Set(firstKey, firstVal); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write first: %v", err)
	}

	// Reuse the same batch object; ensure any ptr-value arena chunks are not
	// recycled if they were still required by the memtable (because pointers were denied).
	b.Reset()
	if err := b.Set(secondKey, secondVal); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write second: %v", err)
	}
	defer b.Close()

	got, err := db.Get(firstKey)
	if err != nil {
		t.Fatalf("get first: %v", err)
	}
	if !bytes.Equal(got, firstVal) {
		t.Fatalf("first value corrupted: got=%x want=%x", got, firstVal)
	}
}
