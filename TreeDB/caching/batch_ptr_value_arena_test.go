package caching

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
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
	if len(b.ptrCopyArenaChunks) != 0 {
		t.Fatalf("expected ptr copy arena chunks to drain after write, got %d", len(b.ptrCopyArenaChunks))
	}
	if count := countBatchArenaLeaseChunks(db, db.mutableShards[0].mem); count == 0 {
		t.Fatal("expected pointer-value arena chunks to be retained by memtable lease")
	}

	// Reuse the same batch object; ensure any ptr-value arena chunks are not
	// recycled if they were still required by the memtable (because pointers were denied).
	for i := 0; i < 256; i++ {
		b.Reset()
		iterKey := []byte(fmt.Sprintf("%s-%03d", secondKey, i))
		if err := b.Set(iterKey, secondVal); err != nil {
			t.Fatalf("set second (iter %d): %v", i, err)
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write second (iter %d): %v", i, err)
		}
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

func countBatchArenaLeaseChunks(db *DB, mt memtable.Table) int {
	db.batchArenaLeaseMu.Lock()
	defer db.batchArenaLeaseMu.Unlock()

	total := 0
	for _, lease := range db.batchArenaLeasesByMem[mt] {
		if lease == nil {
			continue
		}
		total += len(lease.chunks)
	}
	return total
}
