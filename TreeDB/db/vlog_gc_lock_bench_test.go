package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkValueLogGCPublishDuringPausedFullScan isolates the lock-scope
// regression fixed by PL-04. The deterministic pause represents an expensive
// fallback reachability scan: on the old implementation a concurrent publish
// waits for the pause, while the split implementation publishes before the
// GC enters its short exclusive phase.
func BenchmarkValueLogGCPublishDuringPausedFullScan(b *testing.B) {
	const scanPause = 25 * time.Millisecond
	type gcResult struct {
		stats ValueLogGCStats
		err   error
	}

	var publishElapsed time.Duration
	var segmentsDeleted int
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := Open(Options{Dir: b.TempDir()})
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		appendPointersInNewSegmentBench(b, db.dir, 0, 1, 8_000, 1, func(int) []byte { return []byte("old") })
		appendPointersInNewSegmentBench(b, db.dir, 0, 2, 9_000, 1, func(int) []byte { return []byte("active") })
		if err := db.RefreshValueLogSet(); err != nil {
			_ = db.Close()
			b.Fatalf("refresh value-log set: %v", err)
		}
		db.valueLogRefTracker.invalidate()

		scanStarted := make(chan struct{})
		releaseScan := make(chan struct{})
		var hookOnce sync.Once
		restore := registerScanValueLogRefCountsHook(func() {
			hookOnce.Do(func() {
				close(scanStarted)
				<-releaseScan
			})
		})
		gcDone := make(chan gcResult, 1)
		go func() {
			stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
			gcDone <- gcResult{stats: stats, err: err}
		}()
		<-scanStarted
		key := []byte(fmt.Sprintf("publish-during-scan-%d", i))
		time.AfterFunc(scanPause, func() { close(releaseScan) })

		b.StartTimer()
		started := time.Now()
		err = db.Set(key, []byte("ok"))
		publishElapsed += time.Since(started)
		b.StopTimer()
		if err != nil {
			restore()
			_ = db.Close()
			b.Fatalf("publish during scan: %v", err)
		}
		result := <-gcDone
		restore()
		if result.err != nil {
			_ = db.Close()
			b.Fatalf("ValueLogGC: %v", result.err)
		}
		segmentsDeleted += result.stats.SegmentsDeleted
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}

	b.ReportMetric(float64(publishElapsed.Nanoseconds())/float64(b.N), "publish-ns/op")
	b.ReportMetric(float64(scanPause.Nanoseconds()), "scan-pause-ns/op")
	b.ReportMetric(float64(segmentsDeleted)/float64(b.N), "segments-deleted/op")
}
