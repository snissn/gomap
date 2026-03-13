package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyDirectWriterArena_RetainsAcrossRotateCheckpointAndReuse(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	firstKey := []byte("k1")
	firstVal := bytes.Repeat([]byte{0x11}, 128)
	if err := db.Set(firstKey, firstVal); err != nil {
		t.Fatalf("set first: %v", err)
	}

	shard := &db.mutableShards[0]
	oldMutable, ok := shard.mem.(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("expected append-only mutable memtable, got %T", shard.mem)
	}
	if got := countMutableAppendOnlyDirectArenaActiveChunks(shard); got == 0 {
		t.Fatalf("expected active direct writer chunks after first set")
	}
	if got := countMutableAppendOnlyDirectArenaRetainedChunks(shard); got != 0 {
		t.Fatalf("expected no retained chunks before rotate, got=%d", got)
	}

	db.mu.Lock()
	err = db.rotateMemtableLocked(false)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("rotate first: %v", err)
	}
	if count := countAppendOnlyDirectArenaLeaseChunks(db, oldMutable); count == 0 {
		t.Fatalf("expected pinned direct-writer lease after rotate")
	}
	if got := countMutableAppendOnlyDirectArenaActiveChunks(&db.mutableShards[0]); got != 0 {
		t.Fatalf("expected new mutable writer arena to start empty, got=%d", got)
	}

	got, err := db.Get(firstKey)
	if err != nil {
		t.Fatalf("get first after rotate: %v", err)
	}
	if !bytes.Equal(got, firstVal) {
		t.Fatalf("first value corrupted after rotate: got=%x want=%x", got, firstVal)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if count := countAppendOnlyDirectArenaLeaseChunks(db, oldMutable); count != 0 {
		t.Fatalf("expected direct-writer lease released after checkpoint, got=%d", count)
	}
	retainedAfterCheckpoint := countMutableAppendOnlyDirectArenaRetainedChunks(&db.mutableShards[0])
	if retainedAfterCheckpoint == 0 {
		t.Fatalf("expected checkpoint to return pinned chunks to shard-local retained pool")
	}

	secondKey := []byte("k2")
	secondVal := bytes.Repeat([]byte{0x22}, 128)
	if err := db.Set(secondKey, secondVal); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if got := countMutableAppendOnlyDirectArenaActiveChunks(&db.mutableShards[0]); got == 0 {
		t.Fatalf("expected active direct-writer chunks after second set")
	}
	if got := countMutableAppendOnlyDirectArenaRetainedChunks(&db.mutableShards[0]); got >= retainedAfterCheckpoint {
		t.Fatalf("expected second set to consume retained writer capacity; before=%d after=%d", retainedAfterCheckpoint, got)
	}

	db.mu.Lock()
	err = db.rotateMemtableLocked(false)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("rotate second: %v", err)
	}
	if db.mutableShards[0].mem != oldMutable {
		t.Fatalf("expected checkpointed append-only mutable to be reused")
	}

	got, err = db.Get(secondKey)
	if err != nil {
		t.Fatalf("get second after reuse: %v", err)
	}
	if !bytes.Equal(got, secondVal) {
		t.Fatalf("second value corrupted after reuse: got=%x want=%x", got, secondVal)
	}
}

func TestAppendOnlyDirectWriterArena_SkipsPointerOnlyMemtableValues(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		MemtableMode:             "append_only",
		MemtableShards:           1,
		FlushThreshold:           1 << 30,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("ptr-key")
	value := bytes.Repeat([]byte{0x33}, 256)
	if err := db.Set(key, value); err != nil {
		t.Fatalf("set: %v", err)
	}

	shard := &db.mutableShards[0]
	if got := countMutableAppendOnlyDirectArenaActiveChunks(shard); got != 0 {
		t.Fatalf("expected no direct writer chunks for pointer-only memtable entry, got=%d", got)
	}
	if got := countMutableAppendOnlyDirectArenaRetainedChunks(shard); got != 0 {
		t.Fatalf("expected no retained direct writer chunks for pointer-only entry, got=%d", got)
	}
	if count := countAppendOnlyDirectArenaLeaseChunks(db, shard.mem); count != 0 {
		t.Fatalf("expected no pinned direct writer lease for pointer-only write, got=%d", count)
	}

	shard.mu.Lock()
	memVal, ptr, flags, ok := shard.mem.GetEntry(key)
	shard.mu.Unlock()
	if !ok {
		t.Fatalf("expected memtable entry for pointer write")
	}
	if ptr == (page.ValuePtr{}) {
		t.Fatalf("expected non-zero value pointer")
	}
	if flags != node.FlagPointer {
		t.Fatalf("expected pointer flags, got=%d", flags)
	}
	if memVal != nil {
		t.Fatalf("expected pointer-only memtable entry to avoid borrowed inline bytes")
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("value mismatch: got=%x want=%x", got, value)
	}
}

func TestAppendOnlyDirectWriterArena_ReusesRetainedChunksOnReset(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("before-reset")
	val := bytes.Repeat([]byte{0x44}, 128)
	if err := db.Set(key, val); err != nil {
		t.Fatalf("set before reset: %v", err)
	}
	shard := &db.mutableShards[0]
	oldMutable := shard.mem
	if got := countMutableAppendOnlyDirectArenaActiveChunks(shard); got == 0 {
		t.Fatalf("expected active direct writer chunks before reset")
	}

	db.mu.Lock()
	err = db.resetMutableShardsLocked(db.memtableMode, true)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("reset mutable shards: %v", err)
	}
	if shard.mem != oldMutable {
		t.Fatalf("expected reset reuse to keep the same mutable memtable")
	}
	if got := countMutableAppendOnlyDirectArenaActiveChunks(shard); got != 0 {
		t.Fatalf("expected reset to clear active direct writer chunks, got=%d", got)
	}
	retainedAfterReset := countMutableAppendOnlyDirectArenaRetainedChunks(shard)
	if retainedAfterReset == 0 {
		t.Fatalf("expected reset to retain chunks for the next mutable lifetime")
	}

	afterKey := []byte("after-reset")
	afterVal := bytes.Repeat([]byte{0x55}, 128)
	if err := db.Set(afterKey, afterVal); err != nil {
		t.Fatalf("set after reset: %v", err)
	}
	if got := countMutableAppendOnlyDirectArenaActiveChunks(shard); got == 0 {
		t.Fatalf("expected active direct writer chunks after reset reuse write")
	}
	if got := countMutableAppendOnlyDirectArenaRetainedChunks(shard); got >= retainedAfterReset {
		t.Fatalf("expected retained chunk reuse after reset; before=%d after=%d", retainedAfterReset, got)
	}

	got, err := db.Get(afterKey)
	if err != nil {
		t.Fatalf("get after reset: %v", err)
	}
	if !bytes.Equal(got, afterVal) {
		t.Fatalf("after-reset value mismatch: got=%x want=%x", got, afterVal)
	}
}

func countMutableAppendOnlyDirectArenaActiveChunks(shard *memShard) int {
	if shard == nil {
		return 0
	}
	return len(shard.appendOnlyDirectValueArena.active)
}

func countMutableAppendOnlyDirectArenaRetainedChunks(shard *memShard) int {
	if shard == nil {
		return 0
	}
	return len(shard.appendOnlyDirectValueArena.retained)
}

func countAppendOnlyDirectArenaLeaseChunks(db *DB, mt memtable.Table) int {
	if db == nil || mt == nil {
		return 0
	}
	db.appendOnlyDirectArenaLeaseMu.Lock()
	defer db.appendOnlyDirectArenaLeaseMu.Unlock()
	if db.appendOnlyDirectArenaLeasesByMem == nil {
		return 0
	}
	lease := db.appendOnlyDirectArenaLeasesByMem[mt]
	if lease == nil {
		return 0
	}
	return len(lease.chunks)
}
