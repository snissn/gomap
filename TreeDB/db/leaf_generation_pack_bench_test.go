package db

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationPackBenchmarkKeys        = 16 * 1024
	leafGenerationPackBenchmarkWriteWindow = 10 * time.Millisecond
)

// Run with -benchtime=1x -count=5 -benchmem. The reflection-based optional
// metrics keep this exact harness buildable against the pre-PL-01 base.
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
		allWriteLatencies []time.Duration
		totalBytes        int64
		totalFrames       int64
		totalPackWall     time.Duration
		totalPublishHold  int64
		totalCopyAttempts int64
	)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, leafLog, generationID := newLeafGenerationPackBenchmarkFixture(b)
		b.StartTimer()

		var stats LeafGenerationPackStats
		var err error
		switch mode {
		case "foreground_idle":
			allWriteLatencies = append(allWriteLatencies, runLeafGenerationPackBenchmarkWrites(db, leafGenerationPackBenchmarkWriteWindow, nil)...)
		case "pack_idle":
			started := time.Now()
			stats, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{generationID},
				Force:         true,
				Sync:          true,
			})
			totalPackWall += time.Since(started)
		case "pack_contended":
			firstWrite := make(chan struct{})
			writesDone := make(chan []time.Duration, 1)
			go func() {
				writesDone <- runLeafGenerationPackBenchmarkWrites(db, leafGenerationPackBenchmarkWriteWindow, firstWrite)
			}()
			<-firstWrite
			started := time.Now()
			stats, err = db.LeafGenerationPack(context.Background(), LeafGenerationPackOptions{
				GenerationIDs: []uint64{generationID},
				Force:         true,
				Sync:          true,
			})
			totalPackWall += time.Since(started)
			allWriteLatencies = append(allWriteLatencies, <-writesDone...)
		default:
			b.Fatalf("unknown benchmark mode %q", mode)
		}
		if err != nil {
			b.Fatalf("LeafGenerationPack: %v", err)
		}
		if mode != "foreground_idle" {
			totalBytes += stats.BytesCopied
			totalFrames += int64(stats.LeafFramesWritten)
			totalPublishHold += leafGenerationPackStatsInt64(stats, "PublishHoldNanos")
			totalCopyAttempts += leafGenerationPackStatsInt64(stats, "CopyAttempts")
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
	b.ReportMetric(float64(totalFrames)/float64(b.N), "frames/op")
	b.ReportMetric(float64(totalPublishHold)/float64(b.N), "publish_lock_hold_ns/op")
	b.ReportMetric(float64(totalCopyAttempts)/float64(b.N), "copy_attempts/op")
	b.ReportMetric(float64(len(allWriteLatencies))/float64(b.N), "foreground_writes/op")
	b.ReportMetric(float64(leafGenerationPackBenchmarkPercentile(allWriteLatencies, 0.95).Nanoseconds()), "foreground_p95_ns/op")
	b.ReportMetric(float64(leafGenerationPackBenchmarkPercentile(allWriteLatencies, 0.99).Nanoseconds()), "foreground_p99_ns/op")
}

func newLeafGenerationPackBenchmarkFixture(tb testing.TB) (*DB, *rewriteWriter, uint64) {
	tb.Helper()
	dir := tb.TempDir()
	db, err := Open(Options{
		Dir:                        dir,
		Durability:                 DurabilityWALOffRelaxed,
		DisableBackgroundPrune:     true,
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
	leafLog := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
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
	b := db.NewBatch().(*Batch)
	b.Reserve(count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("bench-pack-%06d", start+i))
		if err := b.Set(key, bytes.Repeat([]byte{fill}, 64)); err != nil {
			_ = b.Close()
			tb.Fatalf("Set: %v", err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		tb.Fatalf("WriteSync: %v", err)
	}
	if err := b.Close(); err != nil {
		tb.Fatalf("Close batch: %v", err)
	}
}

func runLeafGenerationPackBenchmarkWrites(db *DB, window time.Duration, firstWrite chan<- struct{}) []time.Duration {
	deadline := time.Now().Add(window)
	latencies := make([]time.Duration, 0, 128)
	for i := 0; ; i++ {
		started := time.Now()
		err := db.Set([]byte(fmt.Sprintf("bench-foreground-%03d", i%64)), []byte("foreground-value"))
		latencies = append(latencies, time.Since(started))
		if firstWrite != nil {
			close(firstWrite)
			firstWrite = nil
		}
		if err != nil || time.Now().After(deadline) {
			return latencies
		}
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
