package db_test

import (
	"context"
	"errors"
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
			var unsupported, concurrentRetries, unexpected, exposureMisses uint64
			for i := 0; i < b.N; i++ {
				round, vacuumErr, exposureMiss := runPL06PublicFixedChurnRound(database, i)
				foreground.latencies = append(foreground.latencies, round.latencies...)
				foreground.points += round.points
				foreground.ranges += round.ranges
				foreground.overlap += round.overlap
				if round.err != nil && foreground.err == nil {
					foreground.err = round.err
				}
				switch {
				case vacuumErr == nil:
				case errors.Is(vacuumErr, dbpkg.ErrVacuumRecoverableRootSetRequired), errors.Is(vacuumErr, dbpkg.ErrVacuumUnsupported):
					unsupported++
				case pl06PublicVacuumConcurrentRetry(vacuumErr):
					concurrentRetries++
				default:
					unexpected++
					b.Logf("unexpected vacuum error: %T: %v", vacuumErr, vacuumErr)
				}
				if exposureMiss {
					exposureMisses++
				}
			}
			b.StopTimer()
			if foreground.err != nil {
				b.Fatalf("foreground churn: %v", foreground.err)
			}
			successfulAttempts := uint64(b.N) - unsupported - concurrentRetries - unexpected
			if successfulAttempts > 0 && exposureMisses != 0 {
				b.Fatalf("foreground exposure misses=%d want 0", exposureMisses)
			}
			wantSamples := b.N * pl06PublicChurnOperationsPerVacuum
			if len(foreground.latencies) != wantSamples || foreground.points != uint64(wantSamples/2) || foreground.ranges != uint64(wantSamples/2) {
				b.Fatalf("foreground fixed work samples=%d points=%d ranges=%d want %d/%d/%d", len(foreground.latencies), foreground.points, foreground.ranges, wantSamples, wantSamples/2, wantSamples/2)
			}
			if successfulAttempts > 0 && foreground.overlap == 0 {
				b.Fatal("foreground fixed work did not overlap vacuum")
			}

			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 95).Nanoseconds()), "foreground-p95-ns")
			b.ReportMetric(float64(pl06PublicChurnPercentile(foreground.latencies, 99).Nanoseconds()), "foreground-p99-ns")
			b.ReportMetric(float64(len(foreground.latencies))/float64(b.N), "foreground-samples/op")
			b.ReportMetric(float64(foreground.points)/float64(b.N), "foreground-points/op")
			b.ReportMetric(float64(foreground.ranges)/float64(b.N), "foreground-ranges/op")
			b.ReportMetric(float64(foreground.overlap)/float64(b.N), "foreground-overlap-samples/op")
			b.ReportMetric(float64(exposureMisses)/float64(b.N), "foreground-exposure-misses/op")
			b.ReportMetric(float64(unsupported)/float64(b.N), "vacuum-unsupported/op")
			b.ReportMetric(float64(concurrentRetries)/float64(b.N), "vacuum-concurrent-retries/op")
			b.ReportMetric(float64(unexpected)/float64(b.N), "vacuum-unexpected-errors/op")
		})
	}
}

func pl06PublicVacuumConcurrentRetry(err error) bool {
	return errors.Is(err, dbpkg.ErrVacuumConcurrentMutation) ||
		errors.Is(err, dbpkg.ErrRecoverableRootSetStale) ||
		errors.Is(err, dbpkg.ErrDurableWALCleanupProofStale)
}

func TestPL06PublicVacuumRetryClassification(t *testing.T) {
	for _, err := range []error{
		dbpkg.ErrVacuumConcurrentMutation,
		dbpkg.ErrRecoverableRootSetStale,
		dbpkg.ErrDurableWALCleanupProofStale,
	} {
		if !pl06PublicVacuumConcurrentRetry(err) {
			t.Fatalf("error %v was not classified as a concurrent retry", err)
		}
	}
	if pl06PublicVacuumConcurrentRetry(errors.New("I/O failure")) {
		t.Fatal("permanent error was classified as a concurrent retry")
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
	overlap   uint64
	err       error
}

const (
	pl06PublicChurnWorkers             = 4
	pl06PublicChurnOperationsPerWorker = 40
	pl06PublicChurnOperationsPerVacuum = pl06PublicChurnWorkers * pl06PublicChurnOperationsPerWorker
	pl06PublicChurnOperationInterval   = time.Millisecond
)

type pl06PublicChurnWorkerResult struct {
	latencies [pl06PublicChurnOperationsPerWorker]time.Duration
	points    uint64
	ranges    uint64
	overlap   uint64
	err       error
}

func runPL06PublicFixedChurnRound(database *dbpkg.DB, round int) (pl06PublicChurnResult, error, bool) {
	vacuumDone := make(chan error, 1)
	vacuumStarted := make(chan struct{})
	vacuumFinished := make(chan struct{})
	go func() {
		close(vacuumStarted)
		vacuumErr := database.VacuumIndexOnline(context.Background())
		close(vacuumFinished)
		vacuumDone <- vacuumErr
	}()
	<-vacuumStarted

	start := make(chan struct{})
	workerDone := make(chan pl06PublicChurnWorkerResult, pl06PublicChurnWorkers)
	for worker := 0; worker < pl06PublicChurnWorkers; worker++ {
		go runPL06PublicFixedChurnWorker(database, round, worker, start, vacuumFinished, workerDone)
	}
	close(start)

	result := pl06PublicChurnResult{latencies: make([]time.Duration, 0, pl06PublicChurnOperationsPerVacuum)}
	for worker := 0; worker < pl06PublicChurnWorkers; worker++ {
		current := <-workerDone
		result.latencies = append(result.latencies, current.latencies[:]...)
		result.points += current.points
		result.ranges += current.ranges
		result.overlap += current.overlap
		if current.err != nil && result.err == nil {
			result.err = current.err
		}
	}
	vacuumErr := <-vacuumDone
	return result, vacuumErr, result.overlap == 0
}

func runPL06PublicFixedChurnWorker(database *dbpkg.DB, round, worker int, start, vacuumFinished <-chan struct{}, done chan<- pl06PublicChurnWorkerResult) {
	<-start
	var result pl06PublicChurnWorkerResult
	for operation := 0; operation < pl06PublicChurnOperationsPerWorker; operation++ {
		if operation > 0 {
			time.Sleep(pl06PublicChurnOperationInterval)
		}
		select {
		case <-vacuumFinished:
		default:
			result.overlap++
		}
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
