package db_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	dbpkg "github.com/snissn/gomap/TreeDB/db"
)

func BenchmarkPL06ExternalVacuumCollectionForegroundChurn(b *testing.B) {
	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "bytes_1x", valueSize: 16},
		{name: "bytes_64x", valueSize: 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			database := openPL06PublicChurnBenchmarkDB(b, tc.valueSize)
			defer func() { _ = database.Close() }()

			b.ReportAllocs()
			b.ResetTimer()
			foreground := pl06PublicChurnResult{latencies: make([]time.Duration, 0, b.N*pl06PublicChurnOperationsPerVacuum)}
			var vacuumErrors, exposureMisses uint64
			for i := 0; i < b.N; i++ {
				round, vacuumErr, exposureMiss := runPL06PublicFixedChurnRound(database, i)
				foreground.latencies = append(foreground.latencies, round.latencies...)
				foreground.points += round.points
				foreground.ranges += round.ranges
				if round.err != nil && foreground.err == nil {
					foreground.err = round.err
				}
				if vacuumErr != nil {
					vacuumErrors++
				}
				if exposureMiss {
					exposureMisses++
				}
			}
			b.StopTimer()
			if foreground.err != nil {
				b.Fatalf("foreground churn: %v", foreground.err)
			}
			if exposureMisses != 0 {
				b.Fatalf("foreground exposure misses=%d want 0", exposureMisses)
			}
			wantSamples := b.N * pl06PublicChurnOperationsPerVacuum
			if len(foreground.latencies) != wantSamples || foreground.points != uint64(wantSamples/2) || foreground.ranges != uint64(wantSamples/2) {
				b.Fatalf("foreground fixed work samples=%d points=%d ranges=%d want %d/%d/%d", len(foreground.latencies), foreground.points, foreground.ranges, wantSamples, wantSamples/2, wantSamples/2)
			}

			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 95).Nanoseconds()), "foreground-p95-ns")
			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 99).Nanoseconds()), "foreground-p99-ns")
			b.ReportMetric(float64(len(foreground.latencies))/float64(b.N), "foreground-samples/op")
			b.ReportMetric(float64(foreground.points)/float64(b.N), "foreground-points/op")
			b.ReportMetric(float64(foreground.ranges)/float64(b.N), "foreground-ranges/op")
			b.ReportMetric(float64(exposureMisses)/float64(b.N), "foreground-exposure-misses/op")
			b.ReportMetric(float64(vacuumErrors)/float64(b.N), "vacuum-errors/op")
		})
	}
}

func openPL06PublicChurnBenchmarkDB(b *testing.B, valueSize int) *dbpkg.DB {
	b.Helper()
	database, err := dbpkg.Open(dbpkg.Options{
		Dir:       b.TempDir(),
		ChunkSize: 1 << 20,
		ValueLog: dbpkg.ValueLogOptions{
			PointerThreshold: 4096,
		},
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: "pl06_public_churn",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
		},
	}); err != nil {
		_ = database.Close()
		b.Fatalf("create collection: %v", err)
	}
	collection, err := manager.OpenCollection("pl06_public_churn")
	if err != nil {
		_ = database.Close()
		b.Fatalf("open collection: %v", err)
	}
	ids := make([][]byte, 1024)
	documents := make([][]byte, len(ids))
	document := []byte(`{"v":"` + strings.Repeat("x", valueSize-8) + `"}`)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		documents[i] = document
	}
	if _, err := collection.InsertBatch(ids, documents); err != nil {
		_ = database.Close()
		b.Fatalf("seed collection: %v", err)
	}
	return database
}

type pl06PublicChurnResult struct {
	latencies []time.Duration
	points    uint64
	ranges    uint64
	err       error
}

const (
	pl06PublicChurnWorkers             = 4
	pl06PublicChurnOperationsPerWorker = 8
	pl06PublicChurnOperationsPerVacuum = pl06PublicChurnWorkers * pl06PublicChurnOperationsPerWorker
	pl06PublicChurnVacuumLead          = time.Millisecond
)

type pl06PublicChurnWorkerResult struct {
	latencies [pl06PublicChurnOperationsPerWorker]time.Duration
	points    uint64
	ranges    uint64
	err       error
}

func runPL06PublicFixedChurnRound(database *dbpkg.DB, round int) (pl06PublicChurnResult, error, bool) {
	vacuumDone := make(chan error, 1)
	go func() {
		vacuumDone <- database.VacuumIndexOnline(context.Background())
	}()

	timer := time.NewTimer(pl06PublicChurnVacuumLead)
	exposureMiss := false
	var vacuumErr error
	select {
	case vacuumErr = <-vacuumDone:
		exposureMiss = true
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
	}

	start := make(chan struct{})
	workerDone := make(chan pl06PublicChurnWorkerResult, pl06PublicChurnWorkers)
	for worker := 0; worker < pl06PublicChurnWorkers; worker++ {
		go runPL06PublicFixedChurnWorker(database, round, worker, start, workerDone)
	}
	close(start)

	result := pl06PublicChurnResult{latencies: make([]time.Duration, 0, pl06PublicChurnOperationsPerVacuum)}
	for worker := 0; worker < pl06PublicChurnWorkers; worker++ {
		current := <-workerDone
		result.latencies = append(result.latencies, current.latencies[:]...)
		result.points += current.points
		result.ranges += current.ranges
		if current.err != nil && result.err == nil {
			result.err = current.err
		}
	}
	if !exposureMiss {
		vacuumErr = <-vacuumDone
	}
	return result, vacuumErr, exposureMiss
}

func runPL06PublicFixedChurnWorker(database *dbpkg.DB, round, worker int, start <-chan struct{}, done chan<- pl06PublicChurnWorkerResult) {
	<-start
	var result pl06PublicChurnWorkerResult
	for operation := 0; operation < pl06PublicChurnOperationsPerWorker; operation++ {
		sequence := round*pl06PublicChurnOperationsPerVacuum + worker*pl06PublicChurnOperationsPerWorker + operation
		started := time.Now()
		if (worker+operation)%2 == 0 {
			key := []byte(fmt.Sprintf("foreground/point/%06d", sequence%256))
			value := []byte(fmt.Sprintf("value/%06d", sequence))
			result.err = database.Set(key, value)
			result.points++
		} else {
			batch := database.NewBatch()
			rangeID := sequence % 64
			startKey := []byte(fmt.Sprintf("foreground/range/%03d/a", rangeID))
			endKey := []byte(fmt.Sprintf("foreground/range/%03d/z", rangeID))
			result.err = batch.DeleteRange(startKey, endKey)
			if result.err == nil {
				result.err = batch.Write()
			}
			if closeErr := batch.Close(); result.err == nil {
				result.err = closeErr
			}
			result.ranges++
		}
		result.latencies[operation] = time.Since(started)
		if result.err != nil {
			break
		}
	}
	done <- result
}

func pl06PublicChurnPercentile(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}
