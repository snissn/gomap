package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkCommandWALParallelSync measures public SetSync completion with fixed
// worker counts. Use fixed counts (e.g. -benchtime=256x). Each worker repeatedly
// replaces its own preloaded key. Inline64 and Pointer4096 use identical default
// compression. One operation includes mutation preparation, publication and the
// actual durable ACK; fixture checkpoint, worker launch, verification and Close
// are excluded. Percentiles are per-call wall latency, including queueing.
// This finite contention diagnostic is not sustained resource qualification.
func BenchmarkCommandWALParallelSync(b *testing.B) {
	for _, mode := range []string{"btree", "cow_ordered"} {
		for _, layout := range []string{"Inline64", "Pointer4096"} {
			for _, workers := range []int{1, 4, 16} {
				b.Run(fmt.Sprintf("%s/%s/G=%d", mode, layout, workers), func(b *testing.B) {
					benchmarkCommandWALParallelSync(b, mode, layout, workers)
				})
			}
		}
	}
}

func benchmarkCommandWALParallelSync(b *testing.B, mode, layout string, workers int) {
	b.StopTimer()
	opts := syncBoundaryIsolatedOptions(ProfileCommandWALRelaxed, b.TempDir())
	opts.MemtableMode, opts.MemtableShards = mode, 2
	opts.FlushThreshold = 1 << 30
	opts.ValueLog.PointerThreshold = 1 << 20
	size := 64
	if layout == "Pointer4096" {
		size = 4096
		opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	}
	database, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				b.Error(err)
			}
		}
	}()
	if database.ResolvedProfile() != ProfileCommandWALRelaxed || database.Stats()["treedb.cache.memtable_mode"] != mode || !database.commandWALCached {
		b.Fatal("requested public command-WAL path inactive")
	}
	keys := make([][]byte, workers)
	values := make([][]byte, workers)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("parallel-sync/%08d", i))
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, size)
		if err := database.Set(keys[i], values[i]); err != nil {
			b.Fatal(err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	before := database.Stats()
	latencies := make([]int64, b.N)
	var next atomic.Int64
	var failed atomic.Bool
	errors := make(chan error, 1)
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	start := make(chan struct{})
	for worker := range workers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			for !failed.Load() {
				i := int(next.Add(1)) - 1
				if i >= b.N {
					return
				}
				binary.LittleEndian.PutUint64(values[worker], uint64(i+1))
				began := time.Now()
				err := database.SetSync(keys[worker], values[worker])
				latencies[i] = time.Since(began).Nanoseconds()
				if err != nil {
					if failed.CompareAndSwap(false, true) {
						errors <- err
					}
					return
				}
			}
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	if failed.Load() {
		b.Fatal(<-errors)
	}
	after := database.Stats()
	for _, metric := range []struct{ key, unit string }{
		{"treedb.command_wal.file_sync.calls_total", "physical_wal_syncs/op"},
		{"treedb.command_wal.group_commit.syncs_total", "captured_group_syncs/op"},
		{"treedb.command_wal.group_commit.commits_total", "grouped_commits/op"},
		{"treedb.cache.value_log.sync.calls_total", "value_syncs/op"},
	} {
		prior := cowPublicCostStat(b, before, metric.key)
		current := cowPublicCostStat(b, after, metric.key)
		if current < prior {
			b.Fatalf("counter moved backwards: %s", metric.key)
		}
		b.ReportMetric(float64(current-prior)/float64(b.N), metric.unit)
	}
	slices.Sort(latencies)
	for _, percentile := range []int{50, 95, 99} {
		i := (len(latencies)*percentile + 99) / 100
		b.ReportMetric(float64(latencies[i-1]), fmt.Sprintf("p%d-ns", percentile))
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "durable_writes/s")
	if err := database.Close(); err != nil {
		b.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range keys {
		got, err := database.Get(key)
		if err != nil || !bytes.Equal(got, values[i]) {
			b.Fatalf("reopen worker=%d value=%d bytes err=%v", i, len(got), err)
		}
	}
}
