package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestGetMany_MemtablePointerValues_DefensiveCopyAndCapacityCapped(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	backendOwnedByDB := false
	t.Cleanup(func() {
		if !backendOwnedByDB {
			_ = backend.Close()
		}
	})

	cdb, err := Open(dir, backend, Options{
		AllowUnsafe:           true,
		DisableWAL:            true,
		FlushThreshold:        1 << 60,
		ForceValueLogPointers: true,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	backendOwnedByDB = true
	t.Cleanup(func() { _ = cdb.Close() })

	if !cdb.memtableValueLogPointers {
		t.Fatalf("expected memtableValueLogPointers enabled under ForceValueLogPointers")
	}

	b := cdb.NewBatch()
	if err := b.Set([]byte("k1"), []byte("ABCD")); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("WXYZ")); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}
	if err := b.Set([]byte("empty"), []byte{}); err != nil {
		t.Fatalf("Set(empty): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close batch: %v", err)
	}

	// Validate we actually hit the pointer+nil inline-value path so GetMany must
	// consult the value log during memtable reads.
	shardIdx := cdb.shardIndex([]byte("k1"))
	shard := &cdb.mutableShards[shardIdx]
	shard.mu.Lock()
	val, _, flags, found := shard.mem.GetEntry([]byte("k1"))
	shard.mu.Unlock()
	if !found {
		t.Fatalf("expected memtable entry for k1")
	}
	if flags&node.FlagPointer == 0 {
		t.Fatalf("expected k1 to be stored as a pointer, flags=%#x", flags)
	}
	if val != nil {
		t.Fatalf("expected k1 inline bytes to be omitted for pointer entry, got len=%d", len(val))
	}

	keys := [][]byte{
		[]byte("k1"),
		[]byte("missing"),
		[]byte("empty"),
		[]byte("k1"),
		[]byte("k2"),
	}
	got, err := cdb.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != len(keys) {
		t.Fatalf("len(GetMany)=%d want %d", len(got), len(keys))
	}
	if !bytes.Equal(got[0], []byte("ABCD")) {
		t.Fatalf("got[0]=%q want %q", got[0], []byte("ABCD"))
	}
	if got[1] != nil {
		t.Fatalf("got[1]=%q want nil for missing key", got[1])
	}
	if got[2] == nil || len(got[2]) != 0 {
		t.Fatalf("got[2]=%q want empty (non-nil) value", got[2])
	}
	if !bytes.Equal(got[3], []byte("ABCD")) {
		t.Fatalf("got[3]=%q want %q", got[3], []byte("ABCD"))
	}
	if !bytes.Equal(got[4], []byte("WXYZ")) {
		t.Fatalf("got[4]=%q want %q", got[4], []byte("WXYZ"))
	}

	// Duplicate values must not alias.
	got[0][0] = 'x'
	if !bytes.Equal(got[3], []byte("ABCD")) {
		t.Fatalf("duplicate value aliased after mutation: got[3]=%q", got[3])
	}
	latest, err := cdb.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get(k1): %v", err)
	}
	if !bytes.Equal(latest, []byte("ABCD")) {
		t.Fatalf("db value changed after caller mutation: got=%q want %q", latest, []byte("ABCD"))
	}

	// Values must be capacity-capped even if they share a backing arena.
	beforeSecond := append([]byte(nil), got[4]...)
	_ = append(got[0], []byte("ZZZZ")...)
	if !bytes.Equal(got[4], beforeSecond) {
		t.Fatalf("append to first value corrupted second value: got=%q want=%q", got[4], beforeSecond)
	}
}
