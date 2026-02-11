package caching

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestBatch_WALOffStreamBypass_DisabledWithValueLog(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
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

	// Streaming bypass is disabled when the value log is enabled; writes should
	// land in memtables until a flush/close.
	total := 0
	for i := range cached.mutableShards {
		shard := &cached.mutableShards[i]
		shard.mu.Lock()
		total += shard.mem.Len()
		shard.mu.Unlock()
	}
	if total == 0 {
		t.Fatalf("expected memtable entries after write with WAL-off/value-log")
	}

	if got, err := backend.Get([]byte("k00000042")); err != nil {
		t.Fatalf("backend get: %v", err)
	} else if got != nil {
		t.Fatalf("expected backend to be empty before flush; got %d bytes", len(got))
	}
}

func TestBatch_WALOffStreamBypass_SkipsWhenOverlappingMemtables(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
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

func TestBatch_WALOffValueLogMultiLaneFanout(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{
		AllowUnsafe:                      true,
		DisableWAL:                       true,
		FlushThreshold:                   1 << 30,
		MemtableMode:                     "skiplist",
		MemtableShards:                   8,
		JournalLanes:                     4,
		ValueLogPointerThreshold:         1,
		ForceValueLogPointers:            true,
		ValueLogCompression:              uint8(vlogCompressionOff),
		ValueLogRawWritevMinBatchRecords: 0,
	})
	if err != nil {
		t.Fatalf("open cached: %v", err)
	}
	defer cached.Close()

	const n = 2048
	val := bytes.Repeat([]byte{0xCD}, 128)
	b := cached.NewBatchWithSize(n)
	for i := 0; i < n; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := b.SetView(key[:], val); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	var probe [8]byte
	binary.BigEndian.PutUint64(probe[:], 1024)
	got, err := cached.Get(probe[:])
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch: got=%d want=%d", len(got), len(val))
	}

	used := 0
	for i := range cached.lanes {
		if cached.lanes[i].vlogLiveBytes.Load() > 0 {
			used++
		}
	}
	if used < 2 {
		t.Fatalf("expected multi-lane value-log fanout, used_lanes=%d", used)
	}
}
