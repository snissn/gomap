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
		stats   ValueLogGCStats
		elapsed time.Duration
		err     error
	}

	var publishElapsed time.Duration
	var gcElapsed time.Duration
	var segmentsDeleted int
	var bytesScanned, bytesReclaimed int64
	var bytesReferenced, bytesActive, bytesProtected, bytesPending int64
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
			started := time.Now()
			stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
			gcDone <- gcResult{stats: stats, elapsed: time.Since(started), err: err}
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
		gcElapsed += result.elapsed
		segmentsDeleted += result.stats.SegmentsDeleted
		bytesScanned += result.stats.BytesTotal
		bytesReclaimed += result.stats.BytesDeleted
		bytesReferenced += result.stats.BytesReferenced
		bytesActive += result.stats.BytesActive
		bytesProtected += result.stats.BytesProtected
		bytesPending += result.stats.BytesPending
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}

	b.ReportMetric(float64(publishElapsed.Nanoseconds())/float64(b.N), "publish-ns/op")
	b.ReportMetric(float64(gcElapsed.Nanoseconds())/float64(b.N), "gc-convergence-ns/op")
	b.ReportMetric(float64(scanPause.Nanoseconds()), "scan-pause-ns/op")
	b.ReportMetric(float64(segmentsDeleted)/float64(b.N), "segments-deleted/op")
	b.ReportMetric(float64(bytesScanned)/float64(b.N), "bytes-scanned/op")
	b.ReportMetric(float64(bytesReclaimed)/float64(b.N), "bytes-reclaimed/op")
	b.ReportMetric(float64(bytesReferenced)/float64(b.N), "bytes-retained-referenced/op")
	b.ReportMetric(float64(bytesActive)/float64(b.N), "bytes-retained-active/op")
	b.ReportMetric(float64(bytesProtected)/float64(b.N), "bytes-retained-protected/op")
	b.ReportMetric(float64(bytesPending)/float64(b.N), "bytes-retained-pending/op")
}
