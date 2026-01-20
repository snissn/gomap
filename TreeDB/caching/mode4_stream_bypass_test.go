package caching

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestBatch_Mode4StreamBypass_WritesDirectToBackend(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableJournal: true,
		SplitValueLog:  true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	const n = 1100
	valSize := cached.valueLogThreshold + 1
	if valSize < 1024 {
		valSize = 1024
	}
	val := bytes.Repeat([]byte{0xAA}, valSize)
	b := cached.NewBatchWithSize(n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if err := b.SetView(key, val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Streaming bypass should avoid mutating memtables.
	for i := range cached.mutableShards {
		shard := &cached.mutableShards[i]
		shard.mu.Lock()
		n := shard.mem.Len()
		shard.mu.Unlock()
		if n != 0 {
			t.Fatalf("expected empty memtable after bypass write; shard %d len=%d", i, n)
		}
	}

	// Values should be readable from the backend/value-log path.
	got, err := cached.Get([]byte("k00000042"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("unexpected value: got %d bytes", len(got))
	}
}

func TestBatch_Mode4StreamBypass_SkipsWhenOverlappingMemtables(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableJournal: true,
		SplitValueLog:  true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	if err := cached.Set([]byte("k00000010"), []byte("small")); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	const n = 1100
	valSize := cached.valueLogThreshold + 1
	if valSize < 1024 {
		valSize = 1024
	}
	val := bytes.Repeat([]byte{0xBB}, valSize)
	b := cached.NewBatchWithSize(n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if err := b.SetView(key, val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Since batch overlaps the in-memory range, it should not have been committed
	// directly to the backend.
	if got, err := backend.Get([]byte("k00000042")); err != nil {
		t.Fatalf("backend get: %v", err)
	} else if got != nil {
		t.Fatalf("expected backend to remain empty; got %d bytes", len(got))
	}
}
