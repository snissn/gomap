package caching

import (
	"bytes"
	"runtime"
	"testing"
	"time"

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
	statsAfterFirstSet := db.Stats()
	if got := mustStatInt64(t, statsAfterFirstSet, "treedb.cache.append_only_direct_arena.active_chunks"); got == 0 {
		t.Fatalf("active direct arena chunks stat=0 after first set")
	}
	if got := mustStatInt64(t, statsAfterFirstSet, "treedb.cache.append_only_direct_arena.active_bytes"); got == 0 {
		t.Fatalf("active direct arena bytes stat=0 after first set")
	}
	if got := mustStatInt64(t, statsAfterFirstSet, "treedb.cache.append_only_direct_arena.active_used_bytes"); got == 0 {
		t.Fatalf("active direct arena used bytes stat=0 after first set")
	}
	if got := mustStatInt64(t, statsAfterFirstSet, "treedb.cache.append_only_direct_arena.retained_bytes"); got != 0 {
		t.Fatalf("retained direct arena bytes before rotate=%d want 0", got)
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
	statsAfterRotate := db.Stats()
	if got := mustStatInt64(t, statsAfterRotate, "treedb.cache.append_only_direct_arena.active_chunks"); got != 0 {
		t.Fatalf("active direct arena chunks after rotate=%d want 0", got)
	}
	if got := mustStatInt64(t, statsAfterRotate, "treedb.cache.append_only_direct_arena.lease_count"); got != 1 {
		t.Fatalf("direct arena lease count after rotate=%d want 1", got)
	}
	if got := mustStatInt64(t, statsAfterRotate, "treedb.cache.append_only_direct_arena.lease_bytes"); got == 0 {
		t.Fatalf("direct arena lease bytes after rotate=0")
	}
	if got := mustStatInt64(t, statsAfterRotate, "treedb.process.append_only_direct_arena.lease_bytes"); got != mustStatInt64(t, statsAfterRotate, "treedb.cache.append_only_direct_arena.lease_bytes") {
		t.Fatalf("direct arena process/cache lease bytes mismatch process=%d cache=%d", got, mustStatInt64(t, statsAfterRotate, "treedb.cache.append_only_direct_arena.lease_bytes"))
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
	statsAfterCheckpoint := db.Stats()
	if got := mustStatInt64(t, statsAfterCheckpoint, "treedb.cache.append_only_direct_arena.lease_count"); got != 0 {
		t.Fatalf("direct arena lease count after checkpoint=%d want 0", got)
	}
	if got := mustStatInt64(t, statsAfterCheckpoint, "treedb.cache.append_only_direct_arena.retained_bytes"); got == 0 {
		t.Fatalf("direct arena retained bytes after checkpoint=0")
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

func TestAppendOnlyDirectWriterArenaStatsExcludeAbandonedTail(t *testing.T) {
	var arena appendOnlyDirectValueArena
	first := arena.alloc(20 << 10)
	second := arena.alloc(20 << 10)

	stats := arena.backingStats()
	wantUsed := int64(len(first) + len(second))
	if got := stats.activeUsedBytes; got != wantUsed {
		t.Fatalf("active used bytes=%d want %d", got, wantUsed)
	}
	if got := stats.activeBytes; got <= stats.activeUsedBytes {
		t.Fatalf("active capacity bytes=%d want > used bytes %d to expose abandoned tail", got, stats.activeUsedBytes)
	}

	arena.recycleAll()
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
	err = db.resetMutableShardsLocked(db.currentMemtableMode(), true)
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

func TestAppendOnlyDirectWriterArena_RetainChunks_DefaultChunkByteBudget(t *testing.T) {
	var arena appendOnlyDirectValueArena

	chunks := make([][]byte, 0, appendOnlyDirectValueArenaRetainMaxChunks+64)
	for i := 0; i < appendOnlyDirectValueArenaRetainMaxChunks+64; i++ {
		chunks = append(chunks, make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk))
	}

	arena.retainChunks(chunks)
	t.Cleanup(func() { arena.recycleAll() })

	if got, want := arena.retainedBytes, int64(appendOnlyDirectValueArenaRetainMaxBytes); got != want {
		t.Fatalf("retained bytes=%d want=%d", got, want)
	}
	if got, want := len(arena.retained), appendOnlyDirectValueArenaRetainMaxChunks; got != want {
		t.Fatalf("retained chunks=%d want=%d", got, want)
	}
}

func TestAppendOnlyDirectWriterArena_RetainChunks_LargeChunkByteBudget(t *testing.T) {
	var arena appendOnlyDirectValueArena
	const largeChunkCap = 1 << 20

	chunks := make([][]byte, 0, 32)
	for i := 0; i < cap(chunks); i++ {
		chunks = append(chunks, make([]byte, 0, largeChunkCap))
	}

	arena.retainChunks(chunks)
	t.Cleanup(func() { arena.recycleAll() })

	if got, want := arena.retainedBytes, int64(appendOnlyDirectValueArenaRetainMaxBytes); got != want {
		t.Fatalf("retained bytes=%d want=%d", got, want)
	}
	if got, want := len(arena.retained), appendOnlyDirectValueArenaRetainMaxBytes/largeChunkCap; got != want {
		t.Fatalf("retained chunks=%d want=%d", got, want)
	}
}

func TestAppendOnlyDirectWriterArena_RetainChunks_DropsOversizeChunksImmediately(t *testing.T) {
	var arena appendOnlyDirectValueArena
	t.Cleanup(func() { arena.recycleAll() })

	oversize := make([]byte, 0, appendOnlyDirectValueArenaPoolMaxCap+1)
	normal := make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk)
	arena.retainChunks([][]byte{oversize, normal})

	if got := len(arena.retained); got != 1 {
		t.Fatalf("retained chunks=%d want=1", got)
	}
	if got := cap(arena.retained[0]); got != appendOnlyDirectValueArenaDefaultChunk {
		t.Fatalf("retained chunk cap=%d want=%d", got, appendOnlyDirectValueArenaDefaultChunk)
	}
}

func TestAppendOnlyDirectWriterArena_TrimRetained_EnforcesCaps(t *testing.T) {
	var arena appendOnlyDirectValueArena
	t.Cleanup(func() { arena.recycleAll() })

	chunks := make([][]byte, 0, 16)
	for i := 0; i < cap(chunks); i++ {
		chunks = append(chunks, make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk))
	}
	arena.retainChunks(chunks)
	if len(arena.retained) < 8 {
		t.Fatalf("test setup retained=%d want >= 8", len(arena.retained))
	}

	dropped := arena.trimRetained(2, int64(appendOnlyDirectValueArenaDefaultChunk*2), appendOnlyDirectValueArenaPoolMaxCap)
	if dropped <= 0 {
		t.Fatalf("trim dropped=%d want > 0", dropped)
	}
	if got := len(arena.retained); got > 2 {
		t.Fatalf("retained chunks=%d want <= 2", got)
	}
	if got := arena.retainedBytes; got > int64(appendOnlyDirectValueArenaDefaultChunk*2) {
		t.Fatalf("retained bytes=%d want <= %d", got, appendOnlyDirectValueArenaDefaultChunk*2)
	}
}

func TestAppendOnlyDirectWriterArena_RetainChunks_PressureAwareLimits(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	var arena appendOnlyDirectValueArena
	t.Cleanup(func() { arena.recycleAll() })

	highChunks := append([][]byte(nil), make([][]byte, appendOnlyDirectValueArenaRetainMaxChunks)...)
	for i := range highChunks {
		highChunks[i] = make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk)
	}
	fake.HeapInuse = 5 << 30 // high
	arena.retainChunks(highChunks)

	wantHighBytes := int64(appendOnlyDirectValueArenaRetainMaxBytes) / appendOnlyDirectArenaHighPressureDivisor
	if got := arena.retainedBytes; got != wantHighBytes {
		t.Fatalf("high-pressure retained bytes=%d want=%d", got, wantHighBytes)
	}

	criticalChunks := make([][]byte, 32)
	for i := range criticalChunks {
		criticalChunks[i] = make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk)
	}
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 9 << 30 // critical
	arena.retainChunks(criticalChunks)

	if got := arena.retainedBytes; got != 0 {
		t.Fatalf("critical-pressure retained bytes=%d want=0", got)
	}
	if got := len(arena.retained); got != 0 {
		t.Fatalf("critical-pressure retained chunks=%d want=0", got)
	}
}

func TestAppendOnlyDirectWriterArena_RetainChunks_EvictsOldestKeepsNewest(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }
	fake.HeapInuse = 512 << 20 // keep pressure in normal band

	var arena appendOnlyDirectValueArena
	t.Cleanup(func() { arena.recycleAll() })

	maxChunks, _ := appendOnlyDirectArenaRetentionLimitsForPressure(poolPressureNormal)
	if maxChunks < 4 {
		t.Fatalf("unexpected max retained chunks=%d", maxChunks)
	}
	for i := 0; i < maxChunks; i++ {
		chunk := make([]byte, 1, appendOnlyDirectValueArenaDefaultChunk)
		chunk[0] = 1 // old generation marker
		chunk = chunk[:0]
		arena.retained = append(arena.retained, chunk)
		arena.retainedBytes += int64(cap(chunk))
	}

	const incoming = 64
	newChunks := make([][]byte, incoming)
	for i := range newChunks {
		chunk := make([]byte, 1, appendOnlyDirectValueArenaDefaultChunk)
		chunk[0] = 2 // new generation marker
		newChunks[i] = chunk[:0]
	}

	arena.retainChunks(newChunks)

	var oldCount, newCount int
	for _, chunk := range arena.retained {
		if cap(chunk) == 0 {
			continue
		}
		switch chunk[:1][0] {
		case 1:
			oldCount++
		case 2:
			newCount++
		default:
			t.Fatalf("unexpected marker byte=%d", chunk[:1][0])
		}
	}
	if got, want := len(arena.retained), maxChunks; got != want {
		t.Fatalf("retained chunk count=%d want=%d", got, want)
	}
	if got, want := newCount, incoming; got != want {
		t.Fatalf("retained newest chunk count=%d want=%d (old=%d)", got, want, oldCount)
	}
}
