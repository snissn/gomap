package caching

import (
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPoolPressureSnapshotScalarRoundTrip(t *testing.T) {
	var cache poolPressureSnapshotCache
	if got := cache.Load(); got != (poolPressureSnapshot{}) {
		t.Fatal("zero-value cache did not return a zero snapshot")
	}
	for _, bits := range []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000,
		0x7ff8000000000001, 0x7ff0000000000001} {
		want := pressurePublicationPattern(^uint64(0))
		want.level = poolPressureCritical
		want.sampledUnixNano = -1 << 63
		want.memoryLimitBytes = -1
		want.gcCPUFraction = math.Float64frombits(bits)
		cache.Store(want)
		got := cache.Load()
		if math.Float64bits(got.gcCPUFraction) != bits {
			t.Fatalf("float bits changed: got %x want %x", math.Float64bits(got.gcCPUFraction), bits)
		}
		got.gcCPUFraction, want.gcCPUFraction = 0, 0
		if got != want {
			t.Fatal("scalar snapshot field changed during publication")
		}
	}
}

// Use the actual production memstats reader and publication path. Sampling alone
// already has an allocation control; this also exercises snapshot publication.
func TestPoolPressureRealSampleAndPublicationAllocation(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()
	saved := poolPressureState.Load()
	defer poolPressureState.Store(saved)
	if poolPressureReadMemStats != nil {
		t.Fatal("allocation proof requires the production memstats reader")
	}
	now := time.Now()
	var last poolPressureSnapshot
	allocs := testing.AllocsPerRun(32, func() {
		last = samplePoolPressureSnapshot(now)
		publishPoolPressureSnapshot(last, now)
	})
	if last.heapSysBytes == 0 || last.totalSysBytes == 0 {
		t.Fatal("production sampler did not read runtime memory statistics")
	}
	if allocs != 0 {
		t.Fatalf("real sample+publish allocs/sample=%g want 0", allocs)
	}
	got := poolPressureState.Load()
	if got != last {
		t.Fatal("published snapshot does not match the real sample")
	}
}

func TestPoolPressureCachedSnapshotReadAllocation(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()
	saved := poolPressureState.Load()
	savedNow := poolPressureNow
	defer func() {
		poolPressureNow = savedNow
		poolPressureState.Store(saved)
	}()
	now := time.Now()
	poolPressureNow = func() time.Time { return now }
	want := samplePoolPressureSnapshot(now)
	poolPressureState.Store(want)
	var got poolPressureSnapshot
	allocs := testing.AllocsPerRun(32, func() { got = currentPoolPressureSnapshot() })
	if allocs != 0 || got != want {
		t.Fatalf("cached read allocs/read=%g want 0; coherent copy=%t", allocs, got == want)
	}
	allocs = testing.AllocsPerRun(32, func() { _ = currentZipperParallelMergePressure() })
	if allocs != 0 {
		t.Fatalf("cached policy allocs/read=%g want 0", allocs)
	}
}

func pressurePublicationPattern(n uint64) poolPressureSnapshot {
	return poolPressureSnapshot{
		sampledUnixNano: int64(n), level: poolPressureNormal, usedBytes: n,
		heapAllocBytes: n, heapInuseBytes: n, heapSysBytes: n, heapIdleBytes: n,
		heapReleasedBytes: n, heapIdleUnreleased: n, stackInuseBytes: n,
		stackSysBytes: n, nextGCBytes: n, numGC: uint32(n), gcCPUFraction: float64(n),
		totalSysBytes: n, nonHeapSysBytes: n, memoryLimitBytes: int64(n),
	}
}

func TestPoolPressurePublicationConcurrentCoherentCopies(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()
	saved := poolPressureState.Load()
	defer poolPressureState.Store(saved)
	poolPressureState.Store(pressurePublicationPattern(0))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for writer := uint64(0); writer < 2; writer++ {
		wg.Add(1)
		go func(writer uint64) {
			defer wg.Done()
			<-start
			for n := uint64(1); n <= 4096; n++ {
				publishPoolPressureSnapshot(pressurePublicationPattern(n+writer*4096), time.Time{})
			}
		}(writer)
	}
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < 8192; n++ {
				got := poolPressureState.Load()
				if got != pressurePublicationPattern(got.usedBytes) {
					t.Error("snapshot fields came from different publications")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	retained := poolPressureState.Load()
	want := retained
	poolPressureState.Store(pressurePublicationPattern(10000))
	if retained != want {
		t.Fatal("later publication modified an already returned snapshot")
	}
}

func TestPoolPressurePolicyPublicationCoherence(t *testing.T) {
	var cache poolPressureSnapshotCache
	var wg sync.WaitGroup
	start := make(chan struct{})
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			<-start
			for n := int64(1); n <= 4096; n++ {
				stamp := n + int64(writer)*4096
				cache.Store(poolPressureSnapshot{sampledUnixNano: stamp, level: poolPressureLevel(stamp % 3)})
			}
		}(writer)
	}
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < 8192; n++ {
				stamp, level := cache.LoadPressure()
				if level != poolPressureLevel(stamp%3) {
					t.Error("pressure level and freshness timestamp came from different publications")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestPoolPressurePolicyRefreshMatchesSnapshot(t *testing.T) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()
	savedNow, savedReader, savedLimit := poolPressureNow, poolPressureReadMemStats, poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow, poolPressureReadMemStats, poolPressureMemoryLimit = savedNow, savedReader, savedLimit
		resetPoolPressureStateForTest()
	})
	for _, policyFirst := range []bool{false, true} {
		resetPoolPressureStateForTest()
		now := time.Unix(1, 0)
		poolPressureNow = func() time.Time { return now }
		poolPressureMemoryLimit = func() int64 { return -1 }
		reads := 0
		fake := runtime.MemStats{HeapInuse: 1 << 30, HeapAlloc: 1 << 30}
		poolPressureReadMemStats = func(ms *runtime.MemStats) { reads++; *ms = fake }
		if policyFirst {
			_ = currentPoolPressureLevel()
		} else {
			_ = currentPoolPressureSnapshot()
		}
		want := currentPoolPressureSnapshot()
		if currentPoolPressureLevel() != want.level || reads != 1 {
			t.Fatal("initial policy/snapshot readers did not share one fresh publication")
		}
		fake.HeapInuse, fake.HeapAlloc = 9<<30, 9<<30
		now = now.Add(poolPressureRefreshInterval)
		if currentPoolPressureLevel() != want.level || reads != 1 {
			t.Fatal("policy refreshed at the inclusive freshness boundary")
		}
		now = now.Add(time.Nanosecond)
		if policyFirst {
			_ = currentPoolPressureLevel()
		} else {
			_ = currentPoolPressureSnapshot()
		}
		got := currentPoolPressureSnapshot()
		if currentPoolPressureLevel() != poolPressureCritical || got.level != poolPressureCritical ||
			got.sampledUnixNano != now.UnixNano() || reads != 2 {
			t.Fatal("expired policy/snapshot readers did not share the refreshed critical sample")
		}
	}
}
