package caching

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var poolPressureTestMu sync.Mutex

func resetPoolPressureStateForTest() {
	poolPressureState.Store(poolPressureSnapshot{})
	resetPoolPressureLevelState(poolPressureNormal)
	poolPressureLastLeaseTrimUnixNano.Store(0)
}

func TestEntrySliceBudgetScalesWithMemoryPressure(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	savedBudget := entrySlicePoolBudgetBytes
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		entrySlicePoolBudgetBytes = savedBudget
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	entrySlicePoolBudgetBytes = 256 << 20

	fake.HeapInuse = 2 << 30
	if got := currentEntrySlicePoolBudgetBytes(); got != entrySlicePoolBudgetBytes {
		t.Fatalf("normal pressure budget=%d want %d", got, entrySlicePoolBudgetBytes)
	}

	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 5 << 30
	if got := currentEntrySlicePoolBudgetBytes(); got != entrySlicePoolBudgetBytes/2 {
		t.Fatalf("high pressure budget=%d want %d", got, entrySlicePoolBudgetBytes/2)
	}

	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 9 << 30
	if got := currentEntrySlicePoolBudgetBytes(); got != 0 {
		t.Fatalf("critical pressure budget=%d want 0", got)
	}

	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 1 << 30
	if got := currentEntrySlicePoolBudgetBytes(); got != entrySlicePoolBudgetBytes {
		t.Fatalf("recovered budget=%d want %d", got, entrySlicePoolBudgetBytes)
	}
}

func TestPoolPressureIncludesUnreleasedIdleHeap(t *testing.T) {
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

	fake.HeapInuse = 2 << 30
	fake.HeapAlloc = 2 << 30
	fake.HeapIdle = 7 << 30
	fake.HeapReleased = 0
	if got := currentPoolPressureSnapshot().level; got != poolPressureCritical {
		t.Fatalf("pressure level=%v want critical when unreleased idle is included", got)
	}
}

func TestPoolPressureTrimsEntrySliceLeases(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	savedPoolBytes := entrySlicePoolBytes.Load()
	savedLeases := entrySliceLeases
	savedTrimRuns := entrySlicePoolTrimRunsTotal.Load()
	savedTrimDrop := entrySlicePoolTrimDropBytesTotal.Load()
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		entrySlicePoolBytes.Store(savedPoolBytes)
		entrySliceLeases = savedLeases
		entrySlicePoolTrimRunsTotal.Store(savedTrimRuns)
		entrySlicePoolTrimDropBytesTotal.Store(savedTrimDrop)
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	for i := range entrySliceLeases {
		entrySliceLeases[i] = nil
	}
	for i := 0; i < maxEntrySliceLeasesPerBucket; i++ {
		entrySliceLeases[0] = append(entrySliceLeases[0], make([]batch.Entry, 0, 64))
	}

	fake.HeapInuse = 5 << 30
	_ = currentPoolPressureSnapshot()
	if got, wantMax := len(entrySliceLeases[0]), maxEntrySliceLeasesPerBucket/8; got > wantMax {
		t.Fatalf("high pressure leases=%d want <= %d", got, wantMax)
	}

	for i := 0; i < maxEntrySliceLeasesPerBucket; i++ {
		entrySliceLeases[0] = append(entrySliceLeases[0], make([]batch.Entry, 0, 64))
	}
	now = now.Add(poolPressureTrimInterval + poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 9 << 30
	_ = currentPoolPressureSnapshot()
	if got := len(entrySliceLeases[0]); got != 0 {
		t.Fatalf("critical pressure leases=%d want 0", got)
	}
}

func TestPoolPressureDropsAppendOnlyEntryPools(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	memtable.DropAppendOnlyEntryPools()
	memtable.DropAppendOnlyValueArenaPools()
	t.Cleanup(func() {
		memtable.DropAppendOnlyEntryPools()
		memtable.DropAppendOnlyValueArenaPools()
		resetPoolPressureStateForTest()
	})

	mt := memtable.NewAppendOnlyWithEntryCapacity(128)
	for i := 0; i < 1024; i++ {
		key := []byte{byte(i), byte(i >> 8)}
		mt.Set(key, []byte("value"))
	}
	mt.Reset()

	before := memtable.AppendOnlyEntryPoolStatsSnapshot()
	if before.RetainedBytesEstimate == 0 {
		t.Fatal("test setup did not retain append-only entry pool bytes")
	}
	valueArena := memtable.NewAppendOnlyWithCapacity(0)
	valueArena.Set([]byte("value-arena-key"), make([]byte, 4096))
	valueArena.ResetDropEntries()
	beforeValueArena := memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	if beforeValueArena.RetainedBytesEstimate == 0 {
		t.Fatal("test setup did not retain append-only value arena pool bytes")
	}

	maybeTrimEntrySliceLeasesUnderPressure(poolPressureCritical, time.Unix(1, 0))

	after := memtable.AppendOnlyEntryPoolStatsSnapshot()
	if got := after.RetainedBytesEstimate; got != 0 {
		t.Fatalf("append-only entry pool retained bytes=%d want 0", got)
	}
	if got := after.DropsTotal; got != before.DropsTotal+1 {
		t.Fatalf("append-only entry pool drops=%d want %d", got, before.DropsTotal+1)
	}
	if got := after.DropBytesTotal; got < before.DropBytesTotal+before.RetainedBytesEstimate {
		t.Fatalf("append-only entry pool drop bytes=%d want at least %d", got, before.DropBytesTotal+before.RetainedBytesEstimate)
	}
	afterValueArena := memtable.AppendOnlyValueArenaPoolStatsSnapshot()
	if got := afterValueArena.RetainedBytesEstimate; got != 0 {
		t.Fatalf("append-only value arena pool retained bytes=%d want 0", got)
	}
	if got := afterValueArena.DropsTotal; got != beforeValueArena.DropsTotal+1 {
		t.Fatalf("append-only value arena pool drops=%d want %d", got, beforeValueArena.DropsTotal+1)
	}
	if got := afterValueArena.DropBytesTotal; got < beforeValueArena.DropBytesTotal+beforeValueArena.RetainedBytesEstimate {
		t.Fatalf("append-only value arena pool drop bytes=%d want at least %d", got, beforeValueArena.DropBytesTotal+beforeValueArena.RetainedBytesEstimate)
	}
}

func TestPoolPressureTrimPreservesNonLeasePoolAccounting(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedPoolBytes := entrySlicePoolBytes.Load()
	savedLeases := entrySliceLeases
	savedTrimRuns := entrySlicePoolTrimRunsTotal.Load()
	savedTrimDrop := entrySlicePoolTrimDropBytesTotal.Load()
	t.Cleanup(func() {
		poolPressureNow = savedNow
		entrySlicePoolBytes.Store(savedPoolBytes)
		entrySliceLeases = savedLeases
		entrySlicePoolTrimRunsTotal.Store(savedTrimRuns)
		entrySlicePoolTrimDropBytesTotal.Store(savedTrimDrop)
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	for i := range entrySliceLeases {
		entrySliceLeases[i] = nil
	}

	keepPerBucket := maxEntrySliceLeasesPerBucket / 8
	if keepPerBucket < 2 {
		keepPerBucket = 2
	}
	totalLeases := keepPerBucket + 4
	for i := 0; i < totalLeases; i++ {
		entrySliceLeases[0] = append(entrySliceLeases[0], make([]batch.Entry, 0, 64))
	}
	dropped := int64(totalLeases-keepPerBucket) * 64 * entrySliceEntrySizeBytes
	const extraPoolBytes int64 = 1 << 20
	leaseBytesBefore := int64(totalLeases) * 64 * entrySliceEntrySizeBytes
	entrySlicePoolBytes.Store(leaseBytesBefore + extraPoolBytes)
	entrySlicePoolTrimRunsTotal.Store(0)
	entrySlicePoolTrimDropBytesTotal.Store(0)

	maybeTrimEntrySliceLeasesUnderPressure(poolPressureHigh, now)
	if got := len(entrySliceLeases[0]); got != keepPerBucket {
		t.Fatalf("high pressure leases=%d want %d", got, keepPerBucket)
	}
	if got, want := entrySlicePoolBytes.Load(), leaseBytesBefore+extraPoolBytes-dropped; got != want {
		t.Fatalf("entrySlicePoolBytes=%d want %d", got, want)
	}
	if got := entrySlicePoolTrimRunsTotal.Load(); got != 1 {
		t.Fatalf("trim runs=%d want 1", got)
	}
	if got, want := entrySlicePoolTrimDropBytesTotal.Load(), uint64(dropped); got != want {
		t.Fatalf("trim drop bytes=%d want %d", got, want)
	}
}

func TestBatchArenaRetentionBudgetScalesWithMemoryPressure(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		resetPoolPressureStateForTest()
		batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	base := currentBatchArenaPoolBudgetBytes()
	if base <= 0 {
		t.Fatalf("base batch arena budget=%d want >0", base)
	}

	fake.HeapInuse = 5 << 30
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	if got := currentBatchArenaRetentionBudgetBytes(); got != base/2 {
		t.Fatalf("high pressure batch budget=%d want %d", got, base/2)
	}

	fake.HeapInuse = 9 << 30
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	if got := currentBatchArenaRetentionBudgetBytes(); got != 0 {
		t.Fatalf("critical pressure batch budget=%d want 0", got)
	}
}

func TestBatchArenaPoolSkipsRetentionWhenBudgetIsZero(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	savedPoolBytes := batchArenaPoolBytes.Load()
	savedSkip := batchArenaPoolSkipZeroBudgetTotal.Load()
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		batchArenaPoolBytes.Store(savedPoolBytes)
		batchArenaPoolSkipZeroBudgetTotal.Store(savedSkip)
		resetPoolPressureStateForTest()
		batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	fake.HeapInuse = 9 << 30 // critical pressure -> zero effective budget
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	if got := currentBatchArenaRetentionBudgetBytes(); got != 0 {
		t.Fatalf("critical pressure batch budget=%d want 0", got)
	}

	batchArenaPoolBytes.Store(0)
	batchArenaPoolSkipZeroBudgetTotal.Store(0)
	putBatchArena(make([]byte, 0, batchCopyArenaMinChunk))
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes=%d want 0", got)
	}
	if got := batchArenaPoolSkipZeroBudgetTotal.Load(); got != 1 {
		t.Fatalf("skip-zero-budget=%d want 1", got)
	}
}

func TestBatchAuxPoolsSkipRetentionUnderCriticalPressure(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	savedEntriesDrop := batchEntriesPoolDropUnderPressureTotal.Load()
	savedShardEntriesDrop := batchShardEntriesPoolDropUnderPressureTotal.Load()
	savedIntDrop := batchIntPoolDropUnderPressureTotal.Load()
	savedInt64Drop := batchInt64PoolDropUnderPressureTotal.Load()
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		batchEntriesPoolDropUnderPressureTotal.Store(savedEntriesDrop)
		batchShardEntriesPoolDropUnderPressureTotal.Store(savedShardEntriesDrop)
		batchIntPoolDropUnderPressureTotal.Store(savedIntDrop)
		batchInt64PoolDropUnderPressureTotal.Store(savedInt64Drop)
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	var fake runtime.MemStats
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	fake.HeapInuse = 9 << 30 // critical
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	if got := currentPoolPressureSnapshot().level; got != poolPressureCritical {
		t.Fatalf("pressure level=%v want critical", got)
	}

	batchEntriesPoolDropUnderPressureTotal.Store(0)
	batchShardEntriesPoolDropUnderPressureTotal.Store(0)
	batchIntPoolDropUnderPressureTotal.Store(0)
	batchInt64PoolDropUnderPressureTotal.Store(0)

	db := &DB{}
	db.putBatchEntries(make([]batch.Entry, 0, 32))
	db.putBatchShardEntries(make([]batch.Entry, 0, 32))
	db.putBatchIntSlice(make([]int, 0, 32))
	db.putBatchInt64Slice(make([]int64, 0, 32))

	if got := batchEntriesPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("entries pool drop total=%d want 1", got)
	}
	if got := batchShardEntriesPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("shard entries pool drop total=%d want 1", got)
	}
	if got := batchIntPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("int pool drop total=%d want 1", got)
	}
	if got := batchInt64PoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("int64 pool drop total=%d want 1", got)
	}
}

func TestScaleMutableFlushThresholdForPressure(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		base := int64(256 << 20)
		if got := scaleMutableFlushThresholdForPressure(base, poolPressureNormal); got != base {
			t.Fatalf("normal scaled=%d want=%d", got, base)
		}
	})

	t.Run("high", func(t *testing.T) {
		base := int64(256 << 20)
		want := int64(128 << 20)
		if got := scaleMutableFlushThresholdForPressure(base, poolPressureHigh); got != want {
			t.Fatalf("high scaled=%d want=%d", got, want)
		}
	})

	t.Run("critical", func(t *testing.T) {
		base := int64(256 << 20)
		want := int64(64 << 20)
		if got := scaleMutableFlushThresholdForPressure(base, poolPressureCritical); got != want {
			t.Fatalf("critical scaled=%d want=%d", got, want)
		}
	})

	t.Run("critical floor", func(t *testing.T) {
		base := int64(64 << 20)
		want := mutableFlushThresholdPressureFloorBytes
		if got := scaleMutableFlushThresholdForPressure(base, poolPressureCritical); got != want {
			t.Fatalf("critical floor scaled=%d want=%d", got, want)
		}
	})

	t.Run("small base no floor up", func(t *testing.T) {
		base := int64(8 << 20)
		want := int64(2 << 20)
		if got := scaleMutableFlushThresholdForPressure(base, poolPressureCritical); got != want {
			t.Fatalf("small base critical scaled=%d want=%d", got, want)
		}
	})
}

func TestMutableFlushThresholdUsesCachedEffectivePressure(t *testing.T) {
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
	nowCalls := 0
	poolPressureNow = func() time.Time {
		nowCalls++
		return now
	}
	var fake runtime.MemStats
	readMemStatsCalls := 0
	poolPressureReadMemStats = func(ms *runtime.MemStats) {
		readMemStatsCalls++
		*ms = fake
	}
	poolPressureMemoryLimit = func() int64 { return -1 }

	const base = int64(256 << 20)
	db := &DB{flushThreshold: base}
	db.updateMutableThresholdLocked()
	for i := 0; i < 1000; i++ {
		if got := db.mutableFlushThreshold(); got != base {
			t.Fatalf("normal cached threshold=%d want=%d", got, base)
		}
	}
	if nowCalls != 0 {
		t.Fatalf("mutableFlushThreshold called poolPressureNow %d times", nowCalls)
	}
	if readMemStatsCalls != 0 {
		t.Fatalf("mutableFlushThreshold sampled memstats %d times", readMemStatsCalls)
	}

	fake.HeapInuse = 5 << 30
	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	snap := samplePoolPressureSnapshot(now)
	publishPoolPressureSnapshot(snap, now)
	if readMemStatsCalls != 1 {
		t.Fatalf("pressure publish memstats calls=%d want=1", readMemStatsCalls)
	}
	want := scaleMutableFlushThresholdForPressure(base, poolPressureHigh)
	if got := db.mutableFlushThreshold(); got != want {
		t.Fatalf("high cached threshold=%d want=%d", got, want)
	}
	nowCallsBefore := nowCalls
	readMemStatsCallsBefore := readMemStatsCalls
	for i := 0; i < 1000; i++ {
		if got := db.mutableFlushThreshold(); got != want {
			t.Fatalf("repeated high cached threshold=%d want=%d", got, want)
		}
	}
	if nowCalls != nowCallsBefore {
		t.Fatalf("repeated mutableFlushThreshold called poolPressureNow %d times", nowCalls-nowCallsBefore)
	}
	if readMemStatsCalls != readMemStatsCallsBefore {
		t.Fatalf("repeated mutableFlushThreshold sampled memstats %d times", readMemStatsCalls-readMemStatsCallsBefore)
	}
}

func TestMutableFlushThresholdTracksPressureTransitions(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	t.Cleanup(resetPoolPressureStateForTest)

	const base = int64(256 << 20)
	db := &DB{flushThreshold: base}
	db.updateMutableThresholdLocked()

	now := time.Unix(1, 0)
	for _, tc := range []struct {
		name  string
		level poolPressureLevel
	}{
		{name: "normal", level: poolPressureNormal},
		{name: "high", level: poolPressureHigh},
		{name: "critical", level: poolPressureCritical},
		{name: "recovered", level: poolPressureNormal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now = now.Add(poolPressureRefreshInterval + time.Millisecond)
			publishPoolPressureSnapshot(poolPressureSnapshot{
				sampledUnixNano: now.UnixNano(),
				level:           tc.level,
			}, now)
			want := scaleMutableFlushThresholdForPressure(base, tc.level)
			if got := db.mutableFlushThreshold(); got != want {
				t.Fatalf("cached threshold=%d want=%d", got, want)
			}
			if got := currentPoolPressureLevelFast(); got != tc.level {
				t.Fatalf("fast pressure level=%v want=%v", got, tc.level)
			}
		})
	}
}

func TestMutableFlushThresholdBaseChangeUnderPressure(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()

	resetPoolPressureStateForTest()
	t.Cleanup(resetPoolPressureStateForTest)

	now := time.Unix(1, 0)
	publishPoolPressureSnapshot(poolPressureSnapshot{
		sampledUnixNano: now.UnixNano(),
		level:           poolPressureCritical,
	}, now)

	db := &DB{flushThreshold: 256 << 20}
	db.updateMutableThresholdLocked()
	if got, want := db.mutableFlushThreshold(), int64(64<<20); got != want {
		t.Fatalf("initial critical threshold=%d want=%d", got, want)
	}

	db.flushThreshold = 64 << 20
	db.updateMutableThresholdLocked()
	if got, want := db.mutableFlushThreshold(), mutableFlushThresholdPressureFloorBytes; got != want {
		t.Fatalf("critical floor threshold=%d want=%d", got, want)
	}

	db.memtableWarmupActive = true
	db.memtableWarmupThreshold = 8 << 20
	db.updateMutableThresholdLocked()
	if got, want := db.mutableFlushThreshold(), int64(2<<20); got != want {
		t.Fatalf("small warmup threshold=%d want=%d", got, want)
	}
}

func TestSampleProcessMemoryPeaksPublishesFreshPressureSnapshot(t *testing.T) {
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
	publishPoolPressureSnapshot(poolPressureSnapshot{
		sampledUnixNano: now.UnixNano(),
		level:           poolPressureNormal,
		usedBytes:       1 << 20,
	}, now)

	var fake runtime.MemStats
	fake.HeapInuse = 9 << 30
	readMemStatsCalls := 0
	poolPressureReadMemStats = func(ms *runtime.MemStats) {
		readMemStatsCalls++
		*ms = fake
	}
	poolPressureMemoryLimit = func() int64 { return -1 }

	db := &DB{}
	db.sampleProcessMemoryPeaks(vlogMmapStatsSnapshot{})
	if readMemStatsCalls != 1 {
		t.Fatalf("sampleProcessMemoryPeaks memstats calls=%d want=1", readMemStatsCalls)
	}
	if got := currentPoolPressureLevelFast(); got != poolPressureCritical {
		t.Fatalf("fast pressure level=%v want critical", got)
	}
	if got := currentPoolPressureSnapshot().level; got != poolPressureCritical {
		t.Fatalf("cached pressure level=%v want critical", got)
	}
	if readMemStatsCalls != 1 {
		t.Fatalf("fresh currentPoolPressureSnapshot resampled memstats calls=%d want=1", readMemStatsCalls)
	}
	if got := db.processPeakHeapInuseBytes.Load(); got != fake.HeapInuse {
		t.Fatalf("peak heap inuse=%d want=%d", got, fake.HeapInuse)
	}
}

func TestPoolPressureSamplerPublishesInitialPressureSnapshot(t *testing.T) {
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
	var readMemStatsCalls atomic.Uint64
	poolPressureReadMemStats = func(ms *runtime.MemStats) {
		readMemStatsCalls.Add(1)
		ms.HeapInuse = 9 << 30
	}
	poolPressureMemoryLimit = func() int64 { return -1 }

	release := retainPoolPressureSampler()
	t.Cleanup(release)

	if got := currentPoolPressureLevelFast(); got != poolPressureCritical {
		t.Fatalf("fast pressure level=%v want critical after initial sampler retain", got)
	}
	release()
	if got := readMemStatsCalls.Load(); got == 0 {
		t.Fatalf("initial sampler did not read memstats")
	}
}

func TestPoolPressureSamplerPublishesFreshPressureSnapshot(t *testing.T) {
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
	var heapInuse atomic.Uint64
	var readMemStatsCalls atomic.Uint64
	poolPressureReadMemStats = func(ms *runtime.MemStats) {
		readMemStatsCalls.Add(1)
		ms.HeapInuse = heapInuse.Load()
	}
	poolPressureMemoryLimit = func() int64 { return -1 }

	release := retainPoolPressureSampler()
	t.Cleanup(release)

	heapInuse.Store(9 << 30)
	deadline := time.Now().Add(2*poolPressureRefreshInterval + 500*time.Millisecond)
	for time.Now().Before(deadline) {
		if currentPoolPressureLevelFast() == poolPressureCritical {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := currentPoolPressureLevelFast(); got != poolPressureCritical {
		t.Fatalf("fast pressure level=%v want critical after sampler refresh", got)
	}
	if got := readMemStatsCalls.Load(); got == 0 {
		t.Fatalf("sampler did not read memstats")
	}
}
