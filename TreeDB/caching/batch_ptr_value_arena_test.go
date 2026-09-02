package caching

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestBatchPtrValueArena_RetainedWhenPointersDenied(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:                  true,
		DisableWAL:                   true,
		MemtableMode:                 "btree",
		MemtableShards:               2,
		FlushThreshold:               1 << 20,
		ValueLogPointerThreshold:     32,
		MaxValueLogRetainedBytesHard: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Force allowValueLogPointers() to return false even though values are eligible.
	db.valueLogRetainedClosedBytes.Store(db.maxValueLogRetainedBytesHard + 1)

	firstKey, secondKey := keysOnDistinctMutableShards(t, db)
	firstVal := bytes.Repeat([]byte{0x11}, 64)
	inlineVal := []byte{0x22}
	secondVal := bytes.Repeat([]byte{0x22}, 64)

	b := db.NewBatchWithSize(2)
	if err := b.Set(firstKey, firstVal); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if got := len(b.ptrValueIdxs); got != 1 {
		t.Fatalf("expected exactly one pointer-value entry after first Set, got %d", got)
	}
	if len(b.ptrCopyArenaChunks) == 0 && b.ptrCopyBytes == 0 {
		t.Fatal("expected pointer-value arena path to allocate copy storage after first Set")
	}
	if err := b.Set(secondKey, inlineVal); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if got := len(b.ptrValueIdxs); got != 1 {
		t.Fatalf("expected second Set to stay inline and keep exactly one pointer-value entry, got %d", got)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if len(b.ptrCopyArenaChunks) != 0 {
		t.Fatalf("expected ptr copy arena chunks to drain after write, got %d", len(b.ptrCopyArenaChunks))
	}
	firstShard := db.shardIndex(firstKey)
	secondShard := db.shardIndex(secondKey)
	if firstShard == secondShard {
		t.Fatalf("expected distinct shards for test keys, got %d", firstShard)
	}
	if count := countBatchArenaLeaseChunks(db, db.mutableShards[firstShard].mem); count != 2 {
		t.Fatalf("expected pointer shard to retain main+ptr arena chunks, got %d", count)
	}
	if count := countBatchArenaLeaseChunks(db, db.mutableShards[secondShard].mem); count != 1 {
		t.Fatalf("expected non-pointer shard to retain only main arena chunk, got %d", count)
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

func TestBatchPtrValueArena_RecycledWhenPointersAssigned(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		MemtableMode:             "btree",
		MemtableShards:           1,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 32,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("k1")
	val := bytes.Repeat([]byte{0x33}, 64)

	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := len(b.ptrValueIdxs); got != 1 {
		t.Fatalf("expected exactly one pointer-value entry after Set, got %d", got)
	}
	if len(b.ptrCopyArenaChunks) == 0 && b.ptrCopyBytes == 0 {
		t.Fatal("expected pointer-value arena path to allocate copy storage after Set")
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	if len(b.ptrValueIdxs) != 0 {
		t.Fatalf("expected pointer entry indices to be cleared after write, got %d", len(b.ptrValueIdxs))
	}
	if len(b.ptrCopyArenaChunks) != 0 {
		t.Fatalf("expected ptr copy arena chunks to drain after write, got %d", len(b.ptrCopyArenaChunks))
	}
	if count := countBatchArenaLeaseChunks(db, db.mutableShards[0].mem); count != 1 {
		t.Fatalf("expected only the main batch arena lease to remain; got %d chunks", count)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value corrupted: got=%x want=%x", got, val)
	}
}

func TestBatchPtrValueArena_RecycledAfterMaterializedCommandWALAppend(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		ExternalCommandWAL:       true,
		MemtableMode:             "btree",
		MemtableShards:           1,
		FlushThreshold:           1 << 20,
		ValueLogPointerThreshold: 32,
		ForceValueLogPointers:    true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("materialized-command-wal")
	val := bytes.Repeat([]byte{0x44}, 64)

	b := db.NewBatchWithSize(1)
	defer b.Close()
	if err := b.Set(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := len(b.ptrValueIdxs); got != 1 {
		t.Fatalf("expected exactly one pointer-value entry after Set, got %d", got)
	}
	if len(b.ptrCopyArenaChunks) == 0 && b.ptrCopyBytes == 0 {
		t.Fatal("expected pointer-value arena path to allocate copy storage after Set")
	}

	if err := b.WriteAfterCommandWALAppend(true, func() error {
		if !b.commandWALMaterializedRID {
			t.Fatal("expected command WAL materialized-RID mode before append")
		}
		return b.Replay(func(entry batch.Entry) error {
			if !entry.IsPtr || entry.ValuePtr.FileID == 0 || entry.ValuePtr.Length == 0 {
				t.Fatalf("command WAL replay entry lacks materialized pointer: %+v", entry)
			}
			if !bytes.Equal(entry.Value, val) {
				t.Fatalf("command WAL replay value=%x, want %x", entry.Value, val)
			}
			return nil
		})
	}); err != nil {
		t.Fatalf("write after command WAL append: %v", err)
	}

	if len(b.ptrValueIdxs) != 0 {
		t.Fatalf("expected pointer entry indices to be cleared after write, got %d", len(b.ptrValueIdxs))
	}
	if len(b.ptrCopyArenaChunks) != 0 {
		t.Fatalf("expected ptr copy arena chunks to drain after write, got %d", len(b.ptrCopyArenaChunks))
	}
	if count := countBatchArenaLeaseChunks(db, db.mutableShards[0].mem); count != 1 {
		t.Fatalf("expected only the main batch arena lease after materialized command WAL append; got %d chunks", count)
	}
	for i := 0; i < 256; i++ {
		b.Reset()
		iterKey := []byte(fmt.Sprintf("materialized-command-wal-%03d", i))
		iterVal := bytes.Repeat([]byte{byte(i)}, 64)
		if err := b.Set(iterKey, iterVal); err != nil {
			t.Fatalf("set reuse %d: %v", i, err)
		}
		if err := b.WriteAfterCommandWALAppend(true, func() error {
			return b.Replay(func(batch.Entry) error { return nil })
		}); err != nil {
			t.Fatalf("write reuse %d: %v", i, err)
		}
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value corrupted: got=%x want=%x", got, val)
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

func keysOnDistinctMutableShards(t *testing.T, db *DB) ([]byte, []byte) {
	t.Helper()
	if db == nil || len(db.mutableShards) < 2 {
		t.Fatalf("need at least two mutable shards")
	}
	first := []byte("k-000")
	firstShard := db.shardIndex(first)
	for i := 1; i < 4096; i++ {
		candidate := []byte(fmt.Sprintf("k-%03d", i))
		if db.shardIndex(candidate) == firstShard {
			continue
		}
		return first, candidate
	}
	t.Fatalf("failed to find keys on distinct shards")
	return nil, nil
}
