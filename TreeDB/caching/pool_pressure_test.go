package caching

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

var poolPressureTestMu sync.Mutex

func resetPoolPressureStateForTest() {
	poolPressureState.Store(poolPressureSnapshot{})
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
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		batchEntriesPoolDropUnderPressureTotal.Store(savedEntriesDrop)
		batchShardEntriesPoolDropUnderPressureTotal.Store(savedShardEntriesDrop)
		batchIntPoolDropUnderPressureTotal.Store(savedIntDrop)
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

	db := &DB{}
	db.putBatchEntries(make([]batch.Entry, 0, 32))
	db.putBatchShardEntries(make([]batch.Entry, 0, 32))
	db.putBatchIntSlice(make([]int, 0, 32))

	if got := batchEntriesPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("entries pool drop total=%d want 1", got)
	}
	if got := batchShardEntriesPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("shard entries pool drop total=%d want 1", got)
	}
	if got := batchIntPoolDropUnderPressureTotal.Load(); got != 1 {
		t.Fatalf("int pool drop total=%d want 1", got)
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
