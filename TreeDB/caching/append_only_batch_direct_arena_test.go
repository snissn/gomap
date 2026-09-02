package caching

import (
	"bytes"
	"testing"
)

func TestAppendOnlyBatchSetViewCopiesValuesToDirectArenaAcrossMutationAndCheckpoint(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	keyA := []byte("a")
	keyB := []byte("b")
	valueA := bytes.Repeat([]byte("A"), 96)
	valueB := bytes.Repeat([]byte("B"), 96)

	b := db.NewBatchWithSize(2)
	if err := b.SetView(keyA, valueA); err != nil {
		t.Fatalf("set view a: %v", err)
	}
	if err := b.SetView(keyB, valueB); err != nil {
		t.Fatalf("set view b: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	// SetView callers only promise stability until Write/Close returns. The
	// append-only memtable must own keys and values after that point.
	keyA[0] = 'x'
	keyB[0] = 'y'
	valueA[0] = 'x'
	valueB[0] = 'y'

	if got := countMutableAppendOnlyDirectArenaActiveChunks(&db.mutableShards[0]); got == 0 {
		t.Fatalf("expected direct append-only arena chunks for SetView value copies")
	}
	gotA, err := db.Get([]byte("a"))
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	if !bytes.Equal(gotA, bytes.Repeat([]byte("A"), 96)) {
		t.Fatalf("mutable value a changed after source mutation: got prefix=%q", gotA[:1])
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	iteratorClosed := false
	defer func() {
		if !iteratorClosed {
			_ = it.Close()
		}
	}()

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint while iterator open: %v", err)
	}

	seen := map[string]string{}
	for it.Valid() {
		seen[string(it.Key())] = string(it.Value())
		it.Next()
	}
	if seen["a"] != string(bytes.Repeat([]byte("A"), 96)) || seen["b"] != string(bytes.Repeat([]byte("B"), 96)) {
		t.Fatalf("iterator saw mutated values after checkpoint: %#v", seen)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	iteratorClosed = true

	gotB, err := db.Get([]byte("b"))
	if err != nil {
		t.Fatalf("get b after checkpoint: %v", err)
	}
	if !bytes.Equal(gotB, bytes.Repeat([]byte("B"), 96)) {
		t.Fatalf("checkpoint value b changed after source mutation: got prefix=%q", gotB[:1])
	}
}
