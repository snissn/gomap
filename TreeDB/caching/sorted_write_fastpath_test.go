package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

type recordingCopySortedMem struct {
	memtable.Table
	calls int
}

func (m *recordingCopySortedMem) ApplyCopySortedBatchTrusted(entries []batch.Entry, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	m.calls++
	if applier, ok := m.Table.(memtable.CopySortedBatchApplier); ok {
		return applier.ApplyCopySortedBatchTrusted(entries, borrowValues, storeInlinePtrValues, onKey)
	}
	for _, op := range entries {
		if op.Type == batch.OpDelete {
			m.Table.Delete(op.Key)
		} else if op.IsPtr {
			m.Table.SetEntry(op.Key, op.Value, op.ValuePtr, node.FlagPointer)
		} else {
			m.Table.Set(op.Key, op.Value)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	return false
}

func newSortedWriteFastPathDB(t *testing.T) (*DB, *recordingCopySortedMem) {
	t.Helper()
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 8 << 20,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	shard := &db.mutableShards[0]
	shard.mu.Lock()
	rec := &recordingCopySortedMem{Table: shard.mem}
	shard.mem = rec
	shard.mu.Unlock()
	db.mu.Lock()
	db.publishMemtablesLocked()
	db.mu.Unlock()
	return db, rec
}

func requireCachedValue(t *testing.T, db *DB, key, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
}

func TestBatchWriteSortedUniqueUsesCopySortedFastPath(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set(c): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 1 {
		t.Fatalf("copy sorted fast path calls=%d want 1", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
	requireCachedValue(t, db, []byte("c"), []byte("vc"))
}

func TestBatchWriteSortedDuplicateFallsBackToGenericLatestWins(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatalf("Set(a old): %v", err)
	}
	if err := b.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatalf("Set(a new): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("copy sorted fast path used for duplicate-key batch: calls=%d", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("new"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
}

func TestBatchWriteOutOfOrderFallsBackToGeneric(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(2)
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("copy sorted fast path used for out-of-order batch: calls=%d", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
}
