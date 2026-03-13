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
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		entrySlicePoolBytes.Store(savedPoolBytes)
		entrySliceLeases = savedLeases
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
