package caching

import (
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestBatchArenaCopy_IgnoresEntrySliceCapacityForFirstChunk(t *testing.T) {
	b := &Batch{
		// Simulate a large pooled entry backing array from prior workload phase.
		entries:      make([]batch.Entry, 0, 8192),
		copyArenaCap: batchCopyArenaUnsizedInit,
	}

	_ = b.arenaCopy(136)

	if got, want := cap(b.copyArena), batchCopyArenaUnsizedInit; got != want {
		t.Fatalf("copyArena first chunk cap=%d want=%d", got, want)
	}
}

func TestBatchCopyArenaInitCapForEntries(t *testing.T) {
	t.Run("unsized", func(t *testing.T) {
		if got, want := batchCopyArenaInitCapForEntries(0), batchCopyArenaUnsizedInit; got != want {
			t.Fatalf("unsized init cap=%d want=%d", got, want)
		}
	})

	t.Run("sized", func(t *testing.T) {
		entries := 100
		got := batchCopyArenaInitCapForEntries(entries)
		want := entries * batchCopyArenaBytesPerEntry
		if got != want {
			t.Fatalf("sized init cap=%d want=%d", got, want)
		}
	})

	t.Run("clamped", func(t *testing.T) {
		got := batchCopyArenaInitCapForEntries(1 << 30)
		if got != batchCopyArenaInitMax {
			t.Fatalf("clamped init cap=%d want=%d", got, batchCopyArenaInitMax)
		}
	})
}

func TestBatchCopyArenaHint_DecaysAfterLargeToSmall(t *testing.T) {
	var db DB

	db.observeBatchCopyBytes(1 << 20)
	initial := db.batchCopyArenaInitCap(0)
	if initial < (1 << 19) {
		t.Fatalf("initial cap too small after large observe: %d", initial)
	}

	for i := 0; i < 8; i++ {
		db.observeBatchCopyBytes(16 << 10)
	}
	decayed := db.batchCopyArenaInitCap(0)
	if decayed >= initial {
		t.Fatalf("expected decayed cap < initial, initial=%d decayed=%d", initial, decayed)
	}
	if decayed > (128 << 10) {
		t.Fatalf("expected decayed cap to drop near small workload, got=%d", decayed)
	}
}

func TestBatchCopyArenaHint_NewBatchWithSizeCanRaiseInit(t *testing.T) {
	var db DB

	db.observeBatchCopyBytes(16 << 10)
	unsized := db.batchCopyArenaInitCap(0)
	if unsized != 16<<10 {
		t.Fatalf("unsized init cap=%d want=%d", unsized, 16<<10)
	}

	const entries = 8000
	sized := db.batchCopyArenaInitCap(entries)
	wantMin := batchCopyArenaInitCapForEntries(entries)
	if maxChunk := currentBatchCopyArenaMaxChunk(); maxChunk > 0 && wantMin > maxChunk {
		wantMin = maxChunk
	}
	if sized < wantMin {
		t.Fatalf("sized init cap=%d want >= %d", sized, wantMin)
	}
}

func TestBatchCopyArenaHint_UsesTotalCopiedBytes(t *testing.T) {
	var db DB
	b := &Batch{
		db:           &db,
		copyArenaCap: batchCopyArenaMinChunk,
	}

	// Force multiple chunk switches so len(copyArena) at the end is much smaller
	// than total copied bytes.
	for i := 0; i < 1024; i++ {
		_ = b.arenaCopy(1024)
	}
	b.updateBatchCopyHint()

	got := db.batchCopyArenaInitCap(0)
	if got < (512 << 10) {
		t.Fatalf("expected init hint to reflect total copied bytes, got=%d", got)
	}
}

func TestBatchArenaInFlightBytesTracksBatchChunkLifecycle(t *testing.T) {
	var db DB
	b := &Batch{
		db:           &db,
		copyArenaCap: batchCopyArenaMinChunk,
	}

	before := batchArenaInFlightBytes.Load()
	_ = b.arenaCopy(batchCopyArenaMinChunk)
	_ = b.arenaCopy(batchCopyArenaMinChunk) // force at least one chunk rollover

	if b.arenaInFlightBytes <= 0 {
		t.Fatalf("batch arenaInFlightBytes=%d want > 0", b.arenaInFlightBytes)
	}
	mid := batchArenaInFlightBytes.Load()
	if mid <= before {
		t.Fatalf("global in_flight bytes mid=%d before=%d want increase", mid, before)
	}

	chunks := b.drainCopyArenaChunks()
	if len(chunks) == 0 {
		t.Fatalf("expected drained copy arena chunks")
	}
	if b.arenaInFlightBytes != 0 {
		t.Fatalf("batch arenaInFlightBytes after drain=%d want 0", b.arenaInFlightBytes)
	}
	afterDrain := batchArenaInFlightBytes.Load()
	if afterDrain > mid {
		t.Fatalf("global in_flight bytes after drain=%d mid=%d want <= mid", afterDrain, mid)
	}
	putBatchArenas(chunks)
}

func TestBatchReset_RefreshesCopyArenaCapFromDecayedHint(t *testing.T) {
	var db DB

	// Simulate a historical high-water mark.
	db.observeBatchCopyBytes(1 << 20)
	staleCap := db.batchCopyArenaInitCap(0)
	if staleCap < (512 << 10) {
		t.Fatalf("expected stale cap to reflect high-water mark, got=%d", staleCap)
	}

	// Simulate sustained small batches causing hint decay.
	for i := 0; i < 8; i++ {
		db.observeBatchCopyBytes(16 << 10)
	}
	decayedCap := db.batchCopyArenaInitCap(0)
	if decayedCap >= staleCap {
		t.Fatalf("expected decayed cap < stale cap, stale=%d decayed=%d", staleCap, decayedCap)
	}

	b := &Batch{
		db:           &db,
		copyArenaCap: staleCap,
	}
	b.Reset()

	if got := b.copyArenaCap; got != decayedCap {
		t.Fatalf("reset copyArenaCap=%d want=%d", got, decayedCap)
	}
}

func TestBatchCopyArenaInitCap_ClampsUnderCriticalPressure(t *testing.T) {
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
	fake.HeapInuse = 9 << 30 // critical
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	var db DB
	db.observeBatchCopyBytes(1 << 20)
	got := db.batchCopyArenaInitCap(0)
	if got > batchCopyArenaCriticalPressureMaxChunk {
		t.Fatalf("critical-pressure init cap=%d want <=%d", got, batchCopyArenaCriticalPressureMaxChunk)
	}
}

func TestBatchArenaCopy_GrowthClampsUnderCriticalPressureButAllowsLargeWrites(t *testing.T) {
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
	fake.HeapInuse = 9 << 30 // critical
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = fake }
	poolPressureMemoryLimit = func() int64 { return -1 }

	b := &Batch{copyArenaCap: batchCopyArenaInitMax}
	_ = b.arenaCopy(batchCopyArenaMinChunk)
	if got := cap(b.copyArena); got > batchCopyArenaCriticalPressureMaxChunk {
		t.Fatalf("critical-pressure chunk cap=%d want <=%d", got, batchCopyArenaCriticalPressureMaxChunk)
	}

	b.copyArena = nil
	b.copyArenaChunks = nil
	large := batchCopyArenaCriticalPressureMaxChunk + (64 << 10)
	_ = b.arenaCopy(large)
	if got := cap(b.copyArena); got < large {
		t.Fatalf("large write chunk cap=%d want >=%d", got, large)
	}
}
