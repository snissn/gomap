package db

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const leafGenerationGCContentionFixtureKeys = 16 * 1024
const leafGenerationGCContentionScanWindow = 50 * time.Millisecond
const leafGenerationGCExclusiveQueueWindow = time.Millisecond

type leafGenerationGCBenchmarkWriteResult struct {
	duration time.Duration
	err      error
}

type leafGenerationGCBenchmarkWrite struct {
	start   chan struct{}
	started chan struct{}
	done    chan leafGenerationGCBenchmarkWriteResult
}

type leafGenerationGCBenchmarkGCResult struct {
	duration time.Duration
	err      error
}

type leafGenerationGCBenchmarkExclusiveWaiter struct {
	started chan struct{}
	done    chan leafGenerationGCBenchmarkWriteResult
}

func openLeafGenerationGCContentionBenchDB(b *testing.B) (*DB, *rewriteWriter) {
	b.Helper()
	db, leafLog := openLeafGenerationPlanRootChurnBenchDB(b)
	writeLeafGenerationBenchKeyRange(b, db, "leaf-gc-contention", 0, leafGenerationGCContentionFixtureKeys, 'a')
	if err := leafLog.rotateLeaf(); err != nil {
		b.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationBenchKeyRange(b, db, "leaf-gc-contention", 0, leafGenerationGCContentionFixtureKeys, 'b')
	return db, leafLog
}

func leafGenerationGCBenchmarkPercentile(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func launchLeafGenerationGCBenchmarkWrite(db *DB, key []byte) leafGenerationGCBenchmarkWrite {
	write := leafGenerationGCBenchmarkWrite{
		start:   make(chan struct{}),
		started: make(chan struct{}),
		done:    make(chan leafGenerationGCBenchmarkWriteResult, 1),
	}
	go func() {
		<-write.start
		start := time.Now()
		close(write.started)
		err := db.Set(key, []byte("value"))
		write.done <- leafGenerationGCBenchmarkWriteResult{duration: time.Since(start), err: err}
	}()
	return write
}

func (write leafGenerationGCBenchmarkWrite) begin() {
	close(write.start)
	<-write.started
}

func launchLeafGenerationGCBenchmarkExclusiveWaiter(db *DB) leafGenerationGCBenchmarkExclusiveWaiter {
	waiter := leafGenerationGCBenchmarkExclusiveWaiter{
		started: make(chan struct{}),
		done:    make(chan leafGenerationGCBenchmarkWriteResult, 1),
	}
	go func() {
		close(waiter.started)
		start := time.Now()
		db.writeMu.Lock()
		duration := time.Since(start)
		db.writeMu.Unlock()
		waiter.done <- leafGenerationGCBenchmarkWriteResult{duration: duration}
	}()
	return waiter
}

func TestLeafGenerationGCBenchmarkPercentile(t *testing.T) {
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}
	if got, want := leafGenerationGCBenchmarkPercentile(samples, 95), 95*time.Millisecond; got != want {
		t.Fatalf("p95=%s, want %s", got, want)
	}
	if got, want := leafGenerationGCBenchmarkPercentile(samples, 99), 99*time.Millisecond; got != want {
		t.Fatalf("p99=%s, want %s", got, want)
	}
	if got := leafGenerationGCBenchmarkPercentile(nil, 99); got != 0 {
		t.Fatalf("empty p99=%s, want 0", got)
	}
}

func BenchmarkLeafGenerationGCWriterContention(b *testing.B) {
	db, _ := openLeafGenerationGCContentionBenchDB(b)
	idleWrites := make([]time.Duration, 0, b.N)
	gcWrites := make([]time.Duration, 0, b.N)
	gcDurations := make([]time.Duration, 0, b.N)
	exclusiveWaiterDurations := make([]time.Duration, 0, b.N)
	var scannedPages atomic.Uint64
	unregisterPageCounter := registerLeafGenerationSubtreeCacheMissHook(func(uint64) {
		scannedPages.Add(1)
	})
	b.Cleanup(unregisterPageCounter)
	var totalScans uint64
	var totalScannedPages uint64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idleWrite := launchLeafGenerationGCBenchmarkWrite(db, []byte(fmt.Sprintf("leaf-gc-write-i-%08d", i)))
		idleWrite.begin()
		idleResult := <-idleWrite.done
		if idleResult.err != nil {
			b.Fatalf("idle Set: %v", idleResult.err)
		}
		idleWrites = append(idleWrites, idleResult.duration)

		enteredScan := make(chan struct{})
		releaseScan := make(chan struct{})
		var scanOnce sync.Once
		var scanCount atomic.Uint64
		unregister := registerLeafGenerationLiveScanHook(func() {
			scanCount.Add(1)
			scanOnce.Do(func() {
				close(enteredScan)
				<-releaseScan
			})
		})
		pagesBefore := scannedPages.Load()
		gcDone := make(chan leafGenerationGCBenchmarkGCResult, 1)
		go func() {
			start := time.Now()
			_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
			gcDone <- leafGenerationGCBenchmarkGCResult{duration: time.Since(start), err: err}
		}()
		select {
		case <-enteredScan:
		case <-time.After(30 * time.Second):
			unregister()
			b.Fatal("LeafGenerationGC did not enter live scan")
		}

		exclusiveWaiter := launchLeafGenerationGCBenchmarkExclusiveWaiter(db)
		<-exclusiveWaiter.started
		// Give the exclusive request a stable opportunity to queue behind the
		// paused scan before launching the later foreground write.
		time.Sleep(leafGenerationGCExclusiveQueueWindow)

		gcWrite := launchLeafGenerationGCBenchmarkWrite(db, []byte(fmt.Sprintf("leaf-gc-write-g-%08d", i)))
		gcWrite.begin()
		time.Sleep(leafGenerationGCContentionScanWindow)
		close(releaseScan)
		exclusiveWaiterResult := <-exclusiveWaiter.done
		exclusiveWaiterDurations = append(exclusiveWaiterDurations, exclusiveWaiterResult.duration)
		writeResult := <-gcWrite.done
		if writeResult.err != nil {
			unregister()
			b.Fatalf("Set during leaf GC: %v", writeResult.err)
		}
		gcWrites = append(gcWrites, writeResult.duration)
		gcResult := <-gcDone
		if gcResult.err != nil {
			unregister()
			b.Fatalf("LeafGenerationGC dry run: %v", gcResult.err)
		}
		gcDurations = append(gcDurations, gcResult.duration)
		totalScans += scanCount.Load()
		totalScannedPages += scannedPages.Load() - pagesBefore
		unregister()
	}
	b.StopTimer()

	idleP95 := leafGenerationGCBenchmarkPercentile(idleWrites, 95)
	idleP99 := leafGenerationGCBenchmarkPercentile(idleWrites, 99)
	gcP95 := leafGenerationGCBenchmarkPercentile(gcWrites, 95)
	gcP99 := leafGenerationGCBenchmarkPercentile(gcWrites, 99)
	b.ReportMetric(float64(idleP95.Nanoseconds()), "idle-write-p95-ns")
	b.ReportMetric(float64(idleP99.Nanoseconds()), "idle-write-p99-ns")
	b.ReportMetric(float64(gcP95.Nanoseconds()), "gc-write-p95-ns")
	b.ReportMetric(float64(gcP99.Nanoseconds()), "gc-write-p99-ns")
	b.ReportMetric(float64(leafGenerationGCBenchmarkPercentile(exclusiveWaiterDurations, 95).Nanoseconds()), "exclusive-waiter-p95-ns")
	b.ReportMetric(float64(leafGenerationGCBenchmarkPercentile(exclusiveWaiterDurations, 99).Nanoseconds()), "exclusive-waiter-p99-ns")
	b.ReportMetric(float64(leafGenerationGCContentionScanWindow.Nanoseconds()), "scan-window-ns")
	b.ReportMetric(float64(leafGenerationGCExclusiveQueueWindow.Nanoseconds()), "exclusive-queue-window-ns")
	if idleP95 > 0 {
		b.ReportMetric(float64(gcP95)/float64(idleP95), "gc/idle-p95")
	}
	if idleP99 > 0 {
		b.ReportMetric(float64(gcP99)/float64(idleP99), "gc/idle-p99")
	}
	b.ReportMetric(float64(leafGenerationGCBenchmarkPercentile(gcDurations, 95).Nanoseconds()), "gc-wall-p95-ns")
	b.ReportMetric(float64(leafGenerationGCBenchmarkPercentile(gcDurations, 99).Nanoseconds()), "gc-wall-p99-ns")
	if b.N > 0 {
		b.ReportMetric(float64(totalScans)/float64(b.N), "live-scans/op")
		b.ReportMetric(float64(totalScannedPages)/float64(b.N), "live-scan-pages/op")
	}
	if totalScans > 0 {
		b.ReportMetric(float64(totalScannedPages)/float64(totalScans), "live-scan-pages/scan")
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}

func BenchmarkLeafGenerationGCExclusivePhases(b *testing.B) {
	db, _ := openLeafGenerationGCContentionBenchDB(b)
	phaseDurations := make([]time.Duration, 0, 2*b.N)
	var phaseStart time.Time
	unregister := registerLeafGenerationGCExclusivePhaseHook(func(entering bool) {
		if entering {
			phaseStart = time.Now()
			return
		}
		phaseDurations = append(phaseDurations, time.Since(phaseStart))
	})
	b.Cleanup(unregister)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
			b.Fatalf("LeafGenerationGC: %v", err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(leafGenerationGCBenchmarkPercentile(phaseDurations, 99).Nanoseconds()), "exclusive-p99-ns")
	var maxPhase time.Duration
	for _, duration := range phaseDurations {
		if duration > maxPhase {
			maxPhase = duration
		}
	}
	b.ReportMetric(float64(maxPhase.Nanoseconds()), "exclusive-max-ns")
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}
