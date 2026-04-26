package caching

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAdaptiveMemtableMode_SwitchesAfterWarmupRotation(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	// Use a very large flush threshold so we only ever rotate once (at the warmup
	// threshold). This reproduces the historical case where adaptive mode needed
	// two rotations to switch and therefore never took effect.
	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive:skiplist",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	rng := rand.New(rand.NewSource(1))
	value := make([]byte, 8<<10) // 8KiB
	for i := 0; i < adaptiveMinWrites*2; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[0:8], rng.Uint64())
		binary.BigEndian.PutUint64(key[8:16], uint64(i))
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	// Add enough payload to exceed the 16MiB warmup threshold and trigger a single
	// rotation.
	for i := 0; i < 600; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[0:8], rng.Uint64())
		binary.BigEndian.PutUint64(key[8:16], uint64(adaptiveMinWrites*2+i))
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode_config"]; got != "adaptive" {
		t.Fatalf("expected adaptive memtable config, got %q", got)
	}
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected low-overwrite warmup workload to switch to append_only after warmup rotation, got %q", got)
	}
}

func TestAdaptiveMemtableMode_SequentialWritesSwitchToAppendOnly(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	value := make([]byte, 8<<10)
	for i := 0; i < adaptiveMinWrites*2+600; i++ {
		// Break "strictly increasing" so Batch never switches to backend streaming
		// bypass (which would skip memtable stats/rotation entirely).
		k := uint64(i)
		if i == 1 {
			k = 0
		}
		var key [16]byte
		binary.BigEndian.PutUint64(key[8:16], k)
		if err := b.Set(key[:], value); err != nil {
			t.Fatalf("Batch.Set: %v", err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected sequential workload to switch to append_only, got %q", got)
	}
}

func TestAdaptiveMemtableMode_AppendOnlyRemainsObservableAfterWarmup(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := make([]byte, 8<<10)
	writeSequential := func(start, count int) {
		t.Helper()
		for i := 0; i < count; i++ {
			var key [16]byte
			binary.BigEndian.PutUint64(key[8:16], uint64(start+i))
			if err := db.Set(key[:], value); err != nil {
				t.Fatalf("Set(%d): %v", start+i, err)
			}
		}
	}

	writeSequential(0, adaptiveMinWrites*2+600)

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after initial sequential phase, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected append_only after initial sequential phase, got %q", got)
	}
	if !db.memtableAdaptiveObserve.Load() {
		t.Fatalf("expected adaptive observation to remain enabled after switching to append_only")
	}

	beforeWrites := db.memtableStats.writes.Load()
	beforeSeqWrites := db.memtableStats.seqWrites.Load()

	writeSequential(adaptiveMinWrites*2+600, 256)

	if got := db.memtableStats.writes.Load(); got <= beforeWrites {
		t.Fatalf("writes did not grow after append_only became active: before=%d after=%d", beforeWrites, got)
	}
	if got := db.memtableStats.seqWrites.Load(); got <= beforeSeqWrites {
		t.Fatalf("seqWrites did not grow after append_only became active: before=%d after=%d", beforeSeqWrites, got)
	}
}

func TestAdaptiveMemtableMode_RotationResetsObservedStats(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := make([]byte, 32)
	for i := 0; i < 64; i++ {
		var key [16]byte
		binary.BigEndian.PutUint64(key[8:16], uint64(i))
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}

	if got := db.memtableStats.writes.Load(); got == 0 {
		t.Fatalf("expected writes to be observed before rotation")
	}

	db.mu.Lock()
	if err := db.rotateMemtableLockedWithCapacity(false, db.memtableCap); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLockedWithCapacity: %v", err)
	}
	db.mu.Unlock()

	if got := db.memtableStats.writes.Load(); got != 0 {
		t.Fatalf("writes=%d want 0 after rotation", got)
	}
	if got := db.memtableStats.seqWrites.Load(); got != 0 {
		t.Fatalf("seqWrites=%d want 0 after rotation", got)
	}
	if got := db.memtableStats.overwriteWrites.Load(); got != 0 {
		t.Fatalf("overwriteWrites=%d want 0 after rotation", got)
	}
}

func TestAdaptiveMemtableMode_RecentSequentialWritesWinWithinOneMutable(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := []byte("v")
	for i := adaptiveTailSequentialWriteMin - 1; i >= 0; i-- {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("descending Set(%d): %v", i, err)
		}
	}
	for i := 0; i < adaptiveTailSequentialWriteMin; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(adaptiveTailSequentialWriteMin+i+1))
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("sequential Set(%d): %v", adaptiveTailSequentialWriteMin+i+1, err)
		}
	}

	db.mu.Lock()
	if err := db.rotateMemtableLockedWithCapacity(false, db.memtableCap); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLockedWithCapacity: %v", err)
	}
	db.mu.Unlock()

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected recent sequential writes to drive append_only, got %q", got)
	}
}

func TestAdaptiveMemtableMode_SharedSortedBatchStatsAcrossShards(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           16,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := []byte("v")
	batchSize := 100
	total := adaptiveTailSequentialWriteMin * 2
	for start := 0; start < total; start += batchSize {
		b := db.NewBatch()
		for i := start; i < start+batchSize && i < total; i++ {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(i))
			if err := b.Set(key[:], value); err != nil {
				_ = b.Close()
				t.Fatalf("Batch.Set(%d): %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("Batch.Write(%d): %v", start, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close(%d): %v", start, err)
		}
	}

	db.mu.Lock()
	if err := db.rotateMemtableLockedWithCapacity(false, db.memtableCap); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLockedWithCapacity: %v", err)
	}
	db.mu.Unlock()

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode"]; got != "append_only" {
		t.Fatalf("expected sharded sorted batches to drive append_only, got %q", got)
	}
}

func TestAdaptiveMemtableMode_TailSequentialBatchesRotateUnorderedAppendOnly(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive:append_only",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	value := []byte("v")
	for i := adaptiveMinWrites * 2; i > 0; i-- {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("descending Set(%d): %v", i, err)
		}
	}

	db.mutableShards[0].mu.Lock()
	before, ok := db.mutableShards[0].mem.(*memtable.AppendOnly)
	db.mutableShards[0].mu.Unlock()
	if !ok {
		t.Fatalf("expected append_only mutable before recovery, got %T", db.mutableShards[0].mem)
	}
	if before.Ordered() {
		t.Fatalf("expected descending phase to leave append_only mutable unordered")
	}

	const batchSize = 100
	startKey := adaptiveMinWrites * 4
	total := adaptiveMinWrites * 2
	for start := startKey; start < startKey+total; start += batchSize {
		b := db.NewBatch()
		for i := start; i < start+batchSize && i < startKey+total; i++ {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(i))
			if err := b.Set(key[:], value); err != nil {
				_ = b.Close()
				t.Fatalf("Batch.Set(%d): %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("Batch.Write(%d): %v", start, err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Batch.Close(%d): %v", start, err)
		}
	}

	db.mutableShards[0].mu.Lock()
	after, ok := db.mutableShards[0].mem.(*memtable.AppendOnly)
	db.mutableShards[0].mu.Unlock()
	if !ok {
		t.Fatalf("expected append_only mutable after recovery, got %T", db.mutableShards[0].mem)
	}
	if !after.Ordered() {
		t.Fatalf("expected tail-sequential recovery to rotate to an ordered append_only mutable")
	}
	if got := db.memtableStats.currentSeqRun.Load(); got == 0 {
		t.Fatalf("expected sequential run stats to continue after recovery rotation")
	}
}

func TestAdaptiveMemtableMode_DefaultAdaptiveStartsWarmup(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_mode_config"]; got != "adaptive" {
		t.Fatalf("expected adaptive memtable config, got %q", got)
	}
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "true" {
		t.Fatalf("expected default adaptive mode to start with warmup active, got %q", got)
	}
}

func TestAdaptiveSequentialSetRotatesFullShardEpoch(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           256 << 10,
		MaxQueuedMemtables:       -1,
		MemtableMode:             "adaptive",
		MemtableShards:           8,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	flushLocked := true
	db.flushMu.Lock()
	defer func() {
		if flushLocked {
			db.flushMu.Unlock()
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	value := bytes.Repeat([]byte("v"), 128)
	rotated := false
	for i := 0; i < adaptiveMinWrites*8; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := db.Set(key[:], value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
		db.mu.RLock()
		rotated = len(db.queue) > 0
		db.mu.RUnlock()
		if rotated {
			break
		}
	}

	db.mu.RLock()
	queueLen := len(db.queue)
	queueShardIDs := append([]uint16(nil), db.queueShardIDs...)
	db.mu.RUnlock()
	db.flushMu.Unlock()
	flushLocked = false

	if !rotated {
		t.Fatal("expected sequential writes to rotate a mutable epoch")
	}
	if queueLen != len(db.mutableShards) {
		t.Fatalf("queued shards=%d want %d; shardIDs=%v", queueLen, len(db.mutableShards), queueShardIDs)
	}
	seen := make(map[uint16]struct{}, len(queueShardIDs))
	for _, shardID := range queueShardIDs {
		seen[shardID] = struct{}{}
	}
	for shardID := range db.mutableShards {
		if _, ok := seen[uint16(shardID)]; !ok {
			t.Fatalf("missing queued shard %d; shardIDs=%v", shardID, queueShardIDs)
		}
	}
}

func TestAdaptiveMemtableMode_DefaultAdaptiveOverwriteHeavySwitchesToHashSorted(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 30,
		MemtableMode:             "adaptive",
		MemtableShards:           1,
		ValueLogPointerThreshold: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	const uniqueKeys = adaptiveMinWrites + 1200
	value := make([]byte, 8<<10) // 8KiB
	keys := make([][16]byte, uniqueKeys)

	for i := 0; i < uniqueKeys; i++ {
		binary.BigEndian.PutUint64(keys[i][0:8], uint64(i))
		binary.BigEndian.PutUint64(keys[i][8:16], uint64(i))
		if err := b.Set(keys[i][:], value); err != nil {
			t.Fatalf("Batch.Set(first %d): %v", i, err)
		}
		if err := b.Set(keys[i][:], value); err != nil {
			t.Fatalf("Batch.Set(overwrite %d): %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Batch.Write: %v", err)
	}

	stats := db.Stats()
	if got := stats["treedb.cache.memtable_warmup_active"]; got != "false" {
		t.Fatalf("expected warmup to be finished after rotation, got %q", got)
	}
	if got := stats["treedb.cache.memtable_mode"]; got != "hash_sorted" {
		t.Fatalf("expected overwrite-heavy adaptive workload to switch to hash_sorted, got %q", got)
	}
}
