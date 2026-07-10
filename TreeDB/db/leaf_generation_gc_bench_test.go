package db

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

const leafGenerationGCContentionFixtureKeys = 16 * 1024
const leafGenerationGCContentionScanWindow = 50 * time.Millisecond

type leafGenerationGCBenchmarkWriteResult struct {
	duration time.Duration
	err      error
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

func BenchmarkLeafGenerationGCWriterContention(b *testing.B) {
	db, _ := openLeafGenerationGCContentionBenchDB(b)
	idleWrites := make([]time.Duration, 0, b.N)
	gcWrites := make([]time.Duration, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idleStart := time.Now()
		if err := db.Set([]byte(fmt.Sprintf("leaf-gc-idle-%08d", i)), []byte("value")); err != nil {
			b.Fatalf("idle Set: %v", err)
		}
		idleWrites = append(idleWrites, time.Since(idleStart))

		enteredScan := make(chan struct{})
		releaseScan := make(chan struct{})
		var scanOnce sync.Once
		unregister := registerLeafGenerationLiveScanHook(func() {
			scanOnce.Do(func() {
				close(enteredScan)
				<-releaseScan
			})
		})
		gcDone := make(chan error, 1)
		go func() {
			_, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true})
			gcDone <- err
		}()
		select {
		case <-enteredScan:
		case <-time.After(30 * time.Second):
			unregister()
			b.Fatal("LeafGenerationGC did not enter live scan")
		}

		writeStarted := make(chan struct{})
		writeDone := make(chan leafGenerationGCBenchmarkWriteResult, 1)
		writeStart := time.Now()
		go func() {
			close(writeStarted)
			err := db.Set([]byte(fmt.Sprintf("leaf-gc-during-%08d", i)), []byte("value"))
			writeDone <- leafGenerationGCBenchmarkWriteResult{duration: time.Since(writeStart), err: err}
		}()
		<-writeStarted
		time.Sleep(leafGenerationGCContentionScanWindow)
		close(releaseScan)
		writeResult := <-writeDone
		if writeResult.err != nil {
			unregister()
			b.Fatalf("Set during leaf GC: %v", writeResult.err)
		}
		gcWrites = append(gcWrites, writeResult.duration)
		if err := <-gcDone; err != nil {
			unregister()
			b.Fatalf("LeafGenerationGC dry run: %v", err)
		}
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
	if gcP95 > idleP95 {
		b.ReportMetric(float64((gcP95 - idleP95).Nanoseconds()), "scan-block-p95-ns")
	} else {
		b.ReportMetric(0, "scan-block-p95-ns")
	}
	if gcP99 > idleP99 {
		b.ReportMetric(float64((gcP99 - idleP99).Nanoseconds()), "scan-block-p99-ns")
	} else {
		b.ReportMetric(0, "scan-block-p99-ns")
	}
	b.ReportMetric(float64(leafGenerationGCContentionScanWindow.Nanoseconds()), "scan-window-ns")
	if idleP99 > 0 {
		b.ReportMetric(float64(gcP99)/float64(idleP99), "gc/idle-p99")
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
