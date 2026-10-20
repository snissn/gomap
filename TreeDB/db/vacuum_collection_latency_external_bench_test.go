package db_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
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
				round, vacuumErr, exposureMiss := runPL06PublicFixedChurnRound(context.Background(), database, i)
				b.Logf("M0 diagnostic round=%d vacuum_error_type=%T vacuum_error=%q concurrent_mutation=%t root_set_stale=%t wal_cleanup_proof_stale=%t unsupported=%t root_set_required=%t", i, vacuumErr, fmt.Sprint(vacuumErr),
					errors.Is(vacuumErr, dbpkg.ErrVacuumConcurrentMutation),
					errors.Is(vacuumErr, dbpkg.ErrRecoverableRootSetStale),
					errors.Is(vacuumErr, dbpkg.ErrDurableWALCleanupProofStale),
					errors.Is(vacuumErr, dbpkg.ErrVacuumUnsupported),
					errors.Is(vacuumErr, dbpkg.ErrVacuumRecoverableRootSetRequired))
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

func openPL06PublicChurnBenchmarkDB(b testing.TB, valueSize int) *dbpkg.DB {
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

// This is an admitted-paused vacuum status fixture. Foreground writes enter the
// real recorder after its base snapshot, then the SAME vacuum resumes with its
// original validation. These timings do not certify advancing rebuild latency.
// The existing package-global hook requires serial fixture execution.
func runPL06PublicFixedChurnRound(ctx context.Context, database *dbpkg.DB, round int) (pl06PublicChurnResult, error, bool) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	baseSnapshotReady := make(chan struct{})
	continueVacuum := make(chan struct{})
	released := false
	releaseVacuum := func() {
		if !released {
			close(continueVacuum)
			released = true
		}
	}
	defer releaseVacuum()
	restoreHook := dbpkg.SetVacuumAfterBaseSnapshotHookForTesting(func() {
		close(baseSnapshotReady)
		select {
		case <-continueVacuum:
		case <-ctx.Done():
		}
	})
	defer restoreHook()

	vacuumDone := make(chan error, 1)
	vacuumFinished := make(chan struct{})
	go func() {
		vacuumErr := database.VacuumIndexOnline(ctx)
		close(vacuumFinished)
		vacuumDone <- vacuumErr
	}()
	var vacuumErr error
	vacuumReturned := false
	baseCaptured := false
	select {
	case <-baseSnapshotReady:
		baseCaptured = true
	case vacuumErr = <-vacuumDone:
		// Refusals and failures before admission still execute the exact fixed
		// foreground work below, with an explicit exposure miss.
		vacuumReturned = true
	case <-ctx.Done():
		releaseVacuum()
		vacuumErr = <-vacuumDone
		vacuumReturned = true
	}

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
	releaseVacuum()
	if !vacuumReturned {
		vacuumErr = <-vacuumDone
	}
	return result, vacuumErr, !baseCaptured || result.overlap == 0
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

func TestPL06PublicVacuumAdmittedTailFixture(t *testing.T) {
	database := openPL06PublicChurnBenchmarkDB(t, 1024)
	defer func() { _ = database.Close() }()
	result, vacuumErr, exposureMiss := runPL06PublicFixedChurnRound(context.Background(), database, 0)
	if result.err != nil || len(result.latencies) != 160 || result.points != 80 || result.ranges != 80 {
		t.Fatalf("fixed foreground work: samples=%d points=%d ranges=%d err=%v", len(result.latencies), result.points, result.ranges, result.err)
	}
	if runtime.GOOS == "windows" {
		if !errors.Is(vacuumErr, dbpkg.ErrVacuumUnsupported) || !exposureMiss || result.overlap != 0 {
			t.Fatalf("unsupported refusal: err=%v miss=%t overlap=%d", vacuumErr, exposureMiss, result.overlap)
		}
	} else if vacuumErr != nil || exposureMiss || result.overlap != 160 {
		t.Fatalf("admitted tail: err=%v miss=%t overlap=%d", vacuumErr, exposureMiss, result.overlap)
	}
	for worker := 0; worker < pl06PublicChurnWorkers; worker++ {
		for operation := 0; operation < pl06PublicChurnOperationsPerWorker; operation++ {
			if (worker+operation)%2 != 0 {
				continue
			}
			sequence := worker*pl06PublicChurnOperationsPerWorker + operation
			key := []byte(fmt.Sprintf("foreground/point/%06d", sequence%256))
			want := fmt.Sprintf("value/%06d", sequence)
			got, err := database.Get(key)
			if err != nil || string(got) != want {
				t.Fatalf("replayed key %q=(%q,%v) want %q", key, got, err, want)
			}
		}
	}
}

func TestPL06PublicVacuumCanceledBeforeBaseSnapshot(t *testing.T) {
	database := openPL06PublicChurnBenchmarkDB(t, 1024)
	defer func() { _ = database.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, vacuumErr, exposureMiss := runPL06PublicFixedChurnRound(ctx, database, 0)
	wantErr := context.Canceled
	if runtime.GOOS == "windows" {
		// The existing platform refusal precedes context admission.
		wantErr = dbpkg.ErrVacuumUnsupported
	}
	if !errors.Is(vacuumErr, wantErr) || !exposureMiss || result.overlap != 0 {
		t.Fatalf("canceled refusal: err=%v want=%v miss=%t overlap=%d", vacuumErr, wantErr, exposureMiss, result.overlap)
	}
	if result.err != nil || len(result.latencies) != 160 || result.points != 80 || result.ranges != 80 {
		t.Fatalf("drained foreground work: samples=%d points=%d ranges=%d err=%v", len(result.latencies), result.points, result.ranges, result.err)
	}
}
