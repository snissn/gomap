package caching

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAppendOnlyEntryHint_AdaptiveDecayAndCapacityClamp(t *testing.T) {
	var cache DB

	baseCapacity := 128 << 20
	if got := cache.appendOnlyMemtableCapacityHint(baseCapacity, appendOnlyEstimatedBytesPerEntryDefault); got != baseCapacity {
		t.Fatalf("initial capacity hint=%d want %d", got, baseCapacity)
	}

	cache.observeAppendOnlyMutableEntries(appendOnlyEntryHintMaxEntries)
	high := int(cache.appendOnlyEntryHint.Load())
	if high != appendOnlyEntryHintMaxEntries {
		t.Fatalf("high hint=%d want %d", high, appendOnlyEntryHintMaxEntries)
	}

	for i := 0; i < 24; i++ {
		cache.observeAppendOnlyMutableEntries(appendOnlyEntryHintMinEntries)
	}
	low := int(cache.appendOnlyEntryHint.Load())
	if low >= high {
		t.Fatalf("hint did not decay: high=%d low=%d", high, low)
	}
	if low < appendOnlyEntryHintMinEntries {
		t.Fatalf("hint dropped below min: got=%d min=%d", low, appendOnlyEntryHintMinEntries)
	}

	hinted := cache.appendOnlyMemtableCapacityHint(baseCapacity, appendOnlyEstimatedBytesPerEntryDefault)
	if hinted >= baseCapacity {
		t.Fatalf("hinted capacity did not shrink: got=%d base=%d", hinted, baseCapacity)
	}
	if hinted < minMemtablePrealloc {
		t.Fatalf("hinted capacity=%d want >=%d", hinted, minMemtablePrealloc)
	}

	cache.appendOnlyEntryHint.Store(appendOnlyEntryHintMaxEntries)
	if got := cache.appendOnlyMemtableCapacityHint(4<<20, appendOnlyEstimatedBytesPerEntryDefault); got != 4<<20 {
		t.Fatalf("hinted capacity exceeded caller cap: got=%d want=%d", got, 4<<20)
	}
}

func TestAppendOnlyEntriesToCapacity_OverflowSafe(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if got := appendOnlyEntriesToCapacity(maxInt, 2); got != maxInt {
		t.Fatalf("overflow clamp=%d want=%d", got, maxInt)
	}
	if got := appendOnlyEntriesToCapacity(0, appendOnlyEstimatedBytesPerEntryDefault); got != 0 {
		t.Fatalf("zero entries capacity=%d want 0", got)
	}
}

func TestAppendOnlyEntryHint_CapacityTracksPressureScaledMutableThreshold(t *testing.T) {
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

	var cache DB
	const baseThreshold int64 = 128 << 20
	const requestedCap = 256 << 20
	cache.mutableThreshold.Store(baseThreshold)
	cache.appendOnlyEntryHint.Store(appendOnlyEntryHintMaxEntries)

	expectedCapFor := func(level poolPressureLevel) int {
		effectiveThreshold := scaleMutableFlushThresholdForPressure(baseThreshold, level)
		want := memtableCapacity(effectiveThreshold)
		hintCapacity := appendOnlyEntriesToCapacity(appendOnlyEntryHintMaxEntries, appendOnlyEstimatedBytesPerEntryDefault)
		if hintCapacity < minMemtablePrealloc {
			hintCapacity = minMemtablePrealloc
		}
		if hintCapacity < want {
			want = hintCapacity
		}
		if want > requestedCap {
			want = requestedCap
		}
		return want
	}

	fake.HeapInuse = 1 << 30 // normal
	_ = currentPoolPressureSnapshot()
	normal := cache.appendOnlyMemtableCapacityHint(requestedCap, appendOnlyEstimatedBytesPerEntryDefault)
	if want := expectedCapFor(poolPressureNormal); normal != want {
		t.Fatalf("normal pressure cap=%d want=%d", normal, want)
	}

	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 5 << 30 // high
	_ = currentPoolPressureSnapshot()
	high := cache.appendOnlyMemtableCapacityHint(requestedCap, appendOnlyEstimatedBytesPerEntryDefault)
	if want := expectedCapFor(poolPressureHigh); high != want {
		t.Fatalf("high pressure cap=%d want=%d", high, want)
	}

	now = now.Add(poolPressureRefreshInterval + time.Millisecond)
	fake.HeapInuse = 9 << 30 // critical
	_ = currentPoolPressureSnapshot()
	critical := cache.appendOnlyMemtableCapacityHint(requestedCap, appendOnlyEstimatedBytesPerEntryDefault)
	if want := expectedCapFor(poolPressureCritical); critical != want {
		t.Fatalf("critical pressure cap=%d want=%d", critical, want)
	}

	if !(critical < high && high < normal) {
		t.Fatalf("expected stricter caps under pressure: normal=%d high=%d critical=%d", normal, high, critical)
	}
}

func TestAppendOnlyReserveDemandTeachesFutureCapacityHint(t *testing.T) {
	var cache DB
	cache.memtableCap = 8 << 20
	cache.mutableThreshold.Store(int64(cache.memtableCap))

	first := memtable.NewAppendOnlyWithEntryCapacity(appendOnlyEntryHintMinEntries)
	const demand = 4096
	reserveMemtableEntries(&cache, first, demand)
	hint := int(cache.appendOnlyEntryHint.Load())
	if hint < demand {
		t.Fatalf("reserve demand hint=%d want >=%d", hint, demand)
	}

	next, err := cache.newMutableMemtableWithCapacityMode(cache.memtableCap, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	appendOnly, ok := next.(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("new mutable type=%T, want append-only", next)
	}
	if got := appendOnly.EntryCapacity(); got < hint {
		t.Fatalf("future mutable capacity=%d want >= learned hint %d", got, hint)
	}
}

func TestAppendOnlyRotateWithCapacityObservesMutableEntryHint(t *testing.T) {
	cache, err := Open(t.TempDir(), NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = cache.Close() }()

	cache.appendOnlyEntryHint.Store(0)
	const entries = 4096
	for i := 0; i < entries; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		if err := cache.Set(key, []byte("value")); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	cache.mu.Lock()
	err = cache.rotateMemtableLockedWithCapacity(false, minMemtablePrealloc)
	cache.mu.Unlock()
	if err != nil {
		t.Fatalf("rotate with capacity: %v", err)
	}

	if got := int(cache.appendOnlyEntryHint.Load()); got < entries {
		t.Fatalf("entry hint after rotate=%d want >=%d", got, entries)
	}
}

func TestRecycleAppendOnlyMemtableRetainsSteadyForegroundCapacity(t *testing.T) {
	var cache DB
	cache.storeMemtableMode(memtable.ModeAppendOnly)
	cache.memtableCap = 64 << 20
	cache.mutableThreshold.Store(int64(cache.memtableCap))

	mt := memtable.NewAppendOnlyWithEntryCapacity(appendOnlyEntryHintMinEntries)
	const entries = 200_000
	reserveAppendOnlyMemtableEntries(&cache, mt, entries)
	warmCap := mt.EntryCapacity()
	if warmCap < entries {
		t.Fatalf("test setup capacity=%d want >=%d", warmCap, entries)
	}

	cache.recycleMemtables([]memtable.Table{mt})
	if got := len(cache.appendOnlyMemLeases); got != 1 {
		t.Fatalf("append-only mem leases=%d want 1", got)
	}
	if got := mt.EntryCapacity(); got < entries {
		t.Fatalf("recycled capacity=%d want >=%d", got, entries)
	}

	next, err := cache.newMutableMemtableWithCapacityMode(cache.memtableCap, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("new mutable: %v", err)
	}
	appendOnly, ok := next.(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("new mutable type=%T, want append-only", next)
	}
	if appendOnly != mt {
		t.Fatalf("new mutable did not reuse recycled append-only table")
	}
	if got := appendOnly.EntryCapacity(); got < entries {
		t.Fatalf("reused capacity=%d want >=%d", got, entries)
	}

	before := memtable.AppendOnlyEntryReserveStatsSnapshot()
	reserveAppendOnlyMemtableEntries(&cache, appendOnly, entries)
	after := memtable.AppendOnlyEntryReserveStatsSnapshot()
	if got := after.GrowCallsTotal - before.GrowCallsTotal; got != 0 {
		t.Fatalf("reserve grow calls after reuse=%d want 0", got)
	}
}

func BenchmarkRecycleAppendOnlyMemtableSteadyForegroundCapacity(b *testing.B) {
	const entries = 200_000
	b.ReportAllocs()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		var cache DB
		cache.storeMemtableMode(memtable.ModeAppendOnly)
		cache.memtableCap = 64 << 20
		cache.mutableThreshold.Store(int64(cache.memtableCap))
		mt := memtable.NewAppendOnlyWithEntryCapacity(appendOnlyEntryHintMinEntries)
		reserveAppendOnlyMemtableEntries(&cache, mt, entries)
		b.StartTimer()
		cache.recycleMemtables([]memtable.Table{mt})
		next, err := cache.newMutableMemtableWithCapacityMode(cache.memtableCap, memtable.ModeAppendOnly)
		if err != nil {
			b.Fatalf("new mutable: %v", err)
		}
		appendOnly := next.(*memtable.AppendOnly)
		reserveAppendOnlyMemtableEntries(&cache, appendOnly, entries)
		b.StopTimer()
	}
}

func TestAppendOnlyReserveDemandHintClampsToPressureCapacity(t *testing.T) {
	var cache DB
	cache.memtableCap = 256 << 20
	const threshold = 512 << 10
	cache.mutableThreshold.Store(threshold)

	cache.observeAppendOnlyReserveDemandEntries(appendOnlyEntryHintMaxEntries)
	hint := int(cache.appendOnlyEntryHint.Load())
	wantMax := appendOnlyEntryCapacityForBytes(memtableCapacity(threshold), appendOnlyEstimatedBytesPerEntryDefault)
	if hint > wantMax {
		t.Fatalf("reserve demand hint=%d want <= pressure capacity entries %d", hint, wantMax)
	}
	if hint < appendOnlyEntryHintMinEntries {
		t.Fatalf("reserve demand hint=%d want >=%d", hint, appendOnlyEntryHintMinEntries)
	}
}

func TestAppendOnlyReserveDemandHintClampsToShardScaledPressureCapacity(t *testing.T) {
	var cache DB
	const shards = 16
	const baseThreshold = int64(256 << 20)
	const pressureThreshold = int64(64 << 20)
	cache.mutableShards = make([]memShard, shards)
	cache.memtableCap = shardCapacity(memtableCapacity(baseThreshold), shards)
	cache.mutableThreshold.Store(pressureThreshold)
	cache.appendOnlyEntryHint.Store(appendOnlyEntryHintMaxEntries)

	gotCapacity := cache.appendOnlyMemtableCapacityHint(cache.memtableCap, appendOnlyEstimatedBytesPerEntryDefault)
	wantCapacity := shardCapacity(memtableCapacity(pressureThreshold), shards)
	if wantCapacity >= cache.memtableCap {
		t.Fatalf("test setup invalid: pressure capacity=%d base shard capacity=%d", wantCapacity, cache.memtableCap)
	}
	if gotCapacity != wantCapacity {
		t.Fatalf("pressure-limited capacity=%d want shard-scaled %d", gotCapacity, wantCapacity)
	}

	cache.appendOnlyEntryHint.Store(0)
	cache.observeAppendOnlyReserveDemandEntries(appendOnlyEntryHintMaxEntries)
	hint := int(cache.appendOnlyEntryHint.Load())
	wantMax := appendOnlyEntryCapacityForBytes(wantCapacity, appendOnlyEstimatedBytesPerEntryDefault)
	if hint > wantMax {
		t.Fatalf("reserve demand hint=%d want <= shard-scaled pressure entries %d", hint, wantMax)
	}
}
