package caching

import (
	"runtime"
	"testing"
	"time"
)

func BenchmarkPoolPressurePublishSnapshot(b *testing.B) {
	poolPressureTestMu.Lock()
	defer poolPressureTestMu.Unlock()
	saved := poolPressureState.Load()
	defer poolPressureState.Store(saved)
	now := time.Now()
	snap := samplePoolPressureSnapshot(now)
	snap.level = poolPressureNormal
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.sampledUnixNano++
		publishPoolPressureSnapshot(snap, now)
	}
}

// Compare -cpu=1,4,16 against the same fixture on the base. A fixed clock keeps
// this a cached-read contention control, separate from real sampling/publication
// and the public durable PRIMARY benchmark with all telemetry still running.
func BenchmarkPoolPressureCachedSnapshotRead(b *testing.B) {
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
	publishPoolPressureSnapshot(samplePoolPressureSnapshot(now), now)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var snap poolPressureSnapshot
		for pb.Next() {
			snap = currentPoolPressureSnapshot()
		}
		runtime.KeepAlive(snap)
	})
}

// This maintained policy caller is identical on baseline and candidate. The
// candidate avoids copying diagnostic fields, while retaining freshness checks.
func BenchmarkPoolPressureCachedPolicy(b *testing.B) {
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
	publishPoolPressureSnapshot(samplePoolPressureSnapshot(now), now)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			runtime.KeepAlive(currentZipperParallelMergePressure())
		}
	})
}
