package db

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationPackBenchmarkKeys            = 1024 * 1024
	leafGenerationPackBenchmarkWriterCount     = 8
	leafGenerationPackBenchmarkWritesPerWriter = 64
)

// Run five externally alternating base/head invocations with
// -benchtime=1x -count=1 -benchmem. This file is intentionally self-contained
// and uses reflection only for PL-01 stats so the exact committed harness can
// be overlaid on the pinned pre-PL-01 base.
func BenchmarkLeafGenerationPackCopyPublish(b *testing.B) {
	for _, mode := range []string{"foreground_idle", "pack_idle", "pack_contended"} {
		b.Run(mode, func(b *testing.B) {
			benchmarkLeafGenerationPackCopyPublish(b, mode)
		})
	}
}

func benchmarkLeafGenerationPackCopyPublish(b *testing.B, mode string) {
	b.ReportAllocs()
	var (
		allWriteLatencies  []time.Duration
		totalWriteWall     time.Duration
		totalBytes         int64
		totalFrames        int64
		totalPackWall      time.Duration
		totalCopyTime      int64
		totalPublishWait   int64
		totalPublishHold   int64
		totalCopyAttempts  int64
		totalCopyAborts    int64
		totalRetryCopyTime int64
		totalPrivatePages  int64
	)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, leafLog, generationID := newLeafGenerationPackBenchmarkFixture(b)
		b.StartTimer()

		var stats LeafGenerationPackStats
		var err error
		switch mode {
		case "foreground_idle":
			writers := startLeafGenerationPackBenchmarkWriters(db, i)
			started := time.Now()
			close(writers.release)
			result := <-writers.done
			totalWriteWall += time.Since(started)
			allWriteLatencies = append(allWriteLatencies, result.latencies...)
			err = result.err
		case "pack_idle":
			started := time.Now()
			stats, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{generationID},
				Force:         true,
				Sync:          true,
			})
			totalPackWall += time.Since(started)
		case "pack_contended":
			writers := startLeafGenerationPackBenchmarkWriters(db, i)
			packDone := make(chan leafGenerationPackBenchmarkPackResult, 1)
			copyStarted := make(chan struct{})
			copyResume := make(chan struct{})
			ridReserve := newLeafGenerationPackBenchmarkRIDReserve(db, copyStarted, copyResume)
			started := time.Now()
			go func() {
				stats, err := db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
					GenerationIDs: []uint64{generationID},
					Force:         true,
					Sync:          true,
					ReserveRIDs:   ridReserve,
				})
				packDone <- leafGenerationPackBenchmarkPackResult{stats: stats, err: err}
			}()
			select {
			case <-copyStarted:
			case result := <-packDone:
				b.Fatalf("pack completed before copy barrier: %v", result.err)
			case <-time.After(30 * time.Second):
				b.Fatal("timed out waiting for pack copy barrier")
			}
			writeStarted := time.Now()
			close(writers.release)
			writeResult := <-writers.done
			totalWriteWall += time.Since(writeStarted)
			allWriteLatencies = append(allWriteLatencies, writeResult.latencies...)
			close(copyResume)
			if writeResult.err != nil {
				err = writeResult.err
			}
			packResult := <-packDone
			totalPackWall += time.Since(started)
			stats = packResult.stats
			if err == nil {
				err = packResult.err
			}
		default:
			b.Fatalf("unknown benchmark mode %q", mode)
		}
		if err != nil {
			b.Fatalf("%s: %v", mode, err)
		}
		if mode != "foreground_idle" {
			totalBytes += stats.BytesCopied
			totalFrames += int64(stats.LeafFramesWritten)
			totalCopyTime += leafGenerationPackStatsInt64(stats, "CopyTimeNanos")
			totalPublishWait += leafGenerationPackStatsInt64(stats, "PublishWaitNanos")
			totalPublishHold += leafGenerationPackStatsInt64(stats, "PublishHoldNanos")
			totalCopyAttempts += leafGenerationPackStatsInt64(stats, "CopyAttempts")
			totalCopyAborts += leafGenerationPackStatsInt64(stats, "CopyAborts")
			totalRetryCopyTime += leafGenerationPackStatsInt64(stats, "RetryCopyTimeNanos")
			totalPrivatePages += leafGenerationPackStatsInt64(stats, "PrivatePagesAllocated")
		}

		b.StopTimer()
		if err := db.Close(); err != nil {
			b.Fatalf("Close DB: %v", err)
		}
		if err := leafLog.Close(); err != nil {
			b.Fatalf("Close leaf log: %v", err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	if totalPackWall > 0 {
		b.ReportMetric(float64(totalBytes)/totalPackWall.Seconds(), "pack_bytes/s")
		b.ReportMetric(float64(totalPackWall.Nanoseconds())/float64(b.N), "pack_wall_ns/op")
	}
	if totalWriteWall > 0 {
		b.ReportMetric(float64(len(allWriteLatencies))/totalWriteWall.Seconds(), "foreground_writes/s")
	}
	b.ReportMetric(float64(totalFrames)/float64(b.N), "frames/op")
	b.ReportMetric(float64(totalCopyTime)/float64(b.N), "copy_time_ns/op")
	b.ReportMetric(float64(totalPublishWait)/float64(b.N), "publish_wait_ns/op")
	b.ReportMetric(float64(totalPublishHold)/float64(b.N), "publish_lock_hold_ns/op")
	b.ReportMetric(float64(totalCopyAttempts)/float64(b.N), "copy_attempts/op")
	b.ReportMetric(float64(totalCopyAborts)/float64(b.N), "copy_aborts/op")
	b.ReportMetric(float64(totalRetryCopyTime)/float64(b.N), "retry_copy_ns/op")
	b.ReportMetric(float64(totalPrivatePages)/float64(b.N), "private_pages/op")
	b.ReportMetric(float64(len(allWriteLatencies))/float64(b.N), "foreground_samples/op")
	b.ReportMetric(float64(leafGenerationPackBenchmarkPercentile(allWriteLatencies, 0.95).Nanoseconds()), "foreground_p95_ns/op")
	b.ReportMetric(float64(leafGenerationPackBenchmarkPercentile(allWriteLatencies, 0.99).Nanoseconds()), "foreground_p99_ns/op")
}

type leafGenerationPackBenchmarkWriterResult struct {
	latencies []time.Duration
	err       error
}

type leafGenerationPackBenchmarkWriters struct {
	release chan struct{}
	done    chan leafGenerationPackBenchmarkWriterResult
}

func startLeafGenerationPackBenchmarkWriters(db *DB, iteration int) leafGenerationPackBenchmarkWriters {
	release := make(chan struct{})
	done := make(chan leafGenerationPackBenchmarkWriterResult, 1)
	ready := sync.WaitGroup{}
	ready.Add(leafGenerationPackBenchmarkWriterCount)
	go func() {
		latencies := make([]time.Duration, leafGenerationPackBenchmarkWriterCount*leafGenerationPackBenchmarkWritesPerWriter)
		errs := make(chan error, leafGenerationPackBenchmarkWriterCount)
		workers := sync.WaitGroup{}
		workers.Add(leafGenerationPackBenchmarkWriterCount)
		for worker := 0; worker < leafGenerationPackBenchmarkWriterCount; worker++ {
			worker := worker
			go func() {
				defer workers.Done()
				ready.Done()
				<-release
				for write := 0; write < leafGenerationPackBenchmarkWritesPerWriter; write++ {
					key := []byte(fmt.Sprintf("bench-fg-%02d-%02d-%04d", iteration, worker, write))
					started := time.Now()
					err := db.Set(key, []byte("foreground-value"))
					latencies[worker*leafGenerationPackBenchmarkWritesPerWriter+write] = time.Since(started)
					if err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		workers.Wait()
		close(errs)
		var err error
		for workerErr := range errs {
			if err == nil {
				err = workerErr
			}
		}
		done <- leafGenerationPackBenchmarkWriterResult{latencies: latencies, err: err}
	}()
	ready.Wait()
	return leafGenerationPackBenchmarkWriters{release: release, done: done}
}

type leafGenerationPackBenchmarkPackResult struct {
	stats LeafGenerationPackStats
	err   error
}

func newLeafGenerationPackBenchmarkRIDReserve(db *DB, copyStarted chan struct{}, copyResume <-chan struct{}) func(int) (uint64, error) {
	var (
		next uint64 = 1 << 48
		once sync.Once
	)
	return func(count int) (uint64, error) {
		once.Do(func() {
			unlockedCopy := db.writeMu.TryRLock()
			if unlockedCopy {
				db.writeMu.RUnlock()
			}
			close(copyStarted)
			if unlockedCopy {
				<-copyResume
			}
		})
		end := atomic.AddUint64(&next, uint64(count))
		return end - uint64(count), nil
	}
}

func newLeafGenerationPackBenchmarkFixture(tb testing.TB) (*DB, *rewriteWriter, uint64) {
	tb.Helper()
	dir := tb.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
		FlushAdmissionPolicy:       FlushAdmissionPolicyOff,
		IndexOuterLeavesInValueLog: true,
		LeafPrefixCompression:      true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		ValueLog: ValueLogOptions{
			Compression: ValueLogCompressionBlock,
			BlockCodec:  ValueLogBlockLZ4,
		},
	})
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 1<<30)
	leafLog.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	db.SetLeafPageLog(leafLog)

	writeLeafGenerationPackBenchmarkRange(tb, db, 0, leafGenerationPackBenchmarkKeys, 'a')
	_, sourceFileID, ok := leafLog.CurrentValueLogSegment()
	if !ok || sourceFileID == 0 {
		tb.Fatal("missing benchmark source leaf segment")
	}
	if err := leafLog.rotateLeaf(); err != nil {
		tb.Fatalf("rotateLeaf: %v", err)
	}
	writeLeafGenerationPackBenchmarkRange(tb, db, 0, leafGenerationPackBenchmarkKeys/2, 'b')
	writeLeafGenerationPackBenchmarkRange(tb, db, leafGenerationPackBenchmarkKeys, 64, 'z')
	state := db.State()
	if state == nil || state.LeafGenerations == nil {
		tb.Fatal("missing benchmark leaf generation state")
	}
	generationID := state.LeafGenerations.FileToGeneration[page.ValueLogSegmentID(sourceFileID)]
	if generationID == 0 {
		tb.Fatalf("source file %d has no generation", sourceFileID)
	}
	return db, leafLog, generationID
}

func writeLeafGenerationPackBenchmarkRange(tb testing.TB, db *DB, start, count int, fill byte) {
	tb.Helper()
	batch := db.NewBatch().(*Batch)
	batch.Reserve(count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("bench-pack-%06d", start+i))
		if err := batch.Set(key, bytes.Repeat([]byte{fill}, 512)); err != nil {
			_ = batch.Close()
			tb.Fatalf("Set: %v", err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		tb.Fatalf("WriteSync: %v", err)
	}
	if err := batch.Close(); err != nil {
		tb.Fatalf("Close batch: %v", err)
	}
}

func leafGenerationPackBenchmarkPercentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*percentile)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ordered) {
		index = len(ordered) - 1
	}
	return ordered[index]
}

func leafGenerationPackStatsInt64(stats LeafGenerationPackStats, field string) int64 {
	value := reflect.ValueOf(stats).FieldByName(field)
	if !value.IsValid() {
		return 0
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(value.Uint())
	default:
		return 0
	}
}
