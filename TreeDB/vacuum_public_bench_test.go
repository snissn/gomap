package treedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestPublicVacuumIndexOnlineHighDebtShrinkAndReopen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, beforeBytes, beforeDigest := openPublicVacuumComparisonFixture(t)
	dir := d.dir
	if err := d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("public online vacuum: %v", err)
	}
	afterBytes := publicVacuumIndexBytes(t, dir)
	if afterBytes*100 > beforeBytes*60 {
		t.Fatalf("public online shrink before=%d after=%d want >=40%%", beforeBytes, afterBytes)
	}
	if got := publicVacuumDigest(t, d); got != beforeDigest {
		t.Fatalf("digest after public vacuum=%x want %x", got, beforeDigest)
	}
	if err := d.SetSync([]byte("public-vacuum/post"), []byte("post-vacuum")); err != nil {
		t.Fatalf("post-vacuum write: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(publicVacuumComparisonOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("public-vacuum/post")); err != nil || !bytes.Equal(got, []byte("post-vacuum")) {
		t.Fatalf("post-vacuum value after reopen=%q err=%v", got, err)
	}
}

func TestPublicVacuumIndexOnlineNilReceiverReturnsErrClosed(t *testing.T) {
	var d *DB
	if err := d.VacuumIndexOnline(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("VacuumIndexOnline nil receiver error=%v want ErrClosed", err)
	}
}

func TestCloseOptInVacuumIndexOnlineShrinksAndReopens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum not supported on windows")
	}
	d, beforeBytes, beforeDigest := openPublicVacuumComparisonFixture(t)
	dir := d.dir
	t.Setenv(envCloseVacuumIndexOnline, "1")
	t.Setenv(envCloseVacuumTimeout, "2m")
	if err := d.Close(); err != nil {
		t.Fatalf("close with online vacuum: %v", err)
	}
	t.Setenv(envCloseVacuumIndexOnline, "0")

	afterBytes := publicVacuumIndexBytes(t, dir)
	if afterBytes*100 > beforeBytes*60 {
		t.Fatalf("close online vacuum shrink before=%d after=%d want >=40%%", beforeBytes, afterBytes)
	}
	reopened, err := Open(publicVacuumComparisonOptions(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := publicVacuumDigest(t, reopened); got != beforeDigest {
		t.Fatalf("reopen digest after close vacuum=%x want %x", got, beforeDigest)
	}
}

func BenchmarkPublicVacuumVsDirectBackend(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("online vacuum not supported on windows")
	}
	for _, tc := range []struct {
		name string
		run  func(*DB) error
	}{
		{name: "public", run: func(d *DB) error { return d.VacuumIndexOnline(context.Background()) }},
		{name: "direct_backend", run: func(d *DB) error { return d.backend.VacuumIndexOnline(context.Background()) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			d, _, digest := openPublicVacuumComparisonFixture(b)
			defer d.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := tc.run(d); err != nil {
					b.Fatalf("vacuum: %v", err)
				}
			}
			b.StopTimer()
			if got := publicVacuumDigest(b, d); got != digest {
				b.Fatalf("digest after vacuum=%x want %x", got, digest)
			}
		})
	}
}

func BenchmarkPublicVacuumForegroundChurn(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("online vacuum not supported on windows")
	}
	d, _, _ := openPublicVacuumComparisonFixture(b)
	dir := d.dir
	b.ReportAllocs()
	b.ResetTimer()
	latencies := make([]time.Duration, 0, b.N*publicVacuumChurnOperations)
	var overlap, exposureMisses, vacuumErrors uint64
	for round := 0; round < b.N; round++ {
		result := runPublicVacuumChurnRound(d, round)
		latencies = append(latencies, result.latencies...)
		overlap += result.overlap
		exposureMisses += result.exposureMisses
		if result.vacuumErr != nil {
			vacuumErrors++
		}
		if result.foregroundErr != nil {
			b.Fatalf("foreground churn: %v", result.foregroundErr)
		}
	}
	b.StopTimer()
	if vacuumErrors != 0 || exposureMisses != 0 || overlap == 0 {
		b.Fatalf("vacuum errors=%d exposure misses=%d overlap=%d", vacuumErrors, exposureMisses, overlap)
	}
	if len(latencies) != b.N*publicVacuumChurnOperations {
		b.Fatalf("foreground samples=%d want %d", len(latencies), b.N*publicVacuumChurnOperations)
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatalf("final checkpoint: %v", err)
	}
	digest := publicVacuumDigest(b, d)
	if err := d.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}
	reopened, err := Open(publicVacuumComparisonOptions(dir))
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	if got := publicVacuumDigest(b, reopened); got != digest {
		_ = reopened.Close()
		b.Fatalf("reopen digest=%x want %x", got, digest)
	}
	if err := reopened.Close(); err != nil {
		b.Fatalf("reopen close: %v", err)
	}
	b.ReportMetric(float64(publicVacuumPercentile(latencies, 95).Nanoseconds()), "foreground-p95-ns")
	b.ReportMetric(float64(publicVacuumPercentile(latencies, 99).Nanoseconds()), "foreground-p99-ns")
	b.ReportMetric(float64(overlap)/float64(b.N), "foreground-overlap-samples/op")
	b.ReportMetric(float64(exposureMisses)/float64(b.N), "foreground-exposure-misses/op")
	b.ReportMetric(float64(vacuumErrors)/float64(b.N), "vacuum-errors/op")
}

const (
	publicVacuumChurnWorkers             = 4
	publicVacuumChurnOperationsPerWorker = 40
	publicVacuumChurnOperations          = publicVacuumChurnWorkers * publicVacuumChurnOperationsPerWorker
)

type publicVacuumChurnResult struct {
	latencies      []time.Duration
	overlap        uint64
	exposureMisses uint64
	vacuumErr      error
	foregroundErr  error
}

func runPublicVacuumChurnRound(d *DB, round int) publicVacuumChurnResult {
	vacuumDone := make(chan error, 1)
	vacuumFinished := make(chan struct{})
	go func() {
		vacuumDone <- d.VacuumIndexOnline(context.Background())
		close(vacuumFinished)
	}()

	start := make(chan struct{})
	results := make(chan publicVacuumChurnResult, publicVacuumChurnWorkers)
	var ready sync.WaitGroup
	ready.Add(publicVacuumChurnWorkers)
	for worker := 0; worker < publicVacuumChurnWorkers; worker++ {
		go func(worker int) {
			ready.Done()
			<-start
			result := publicVacuumChurnResult{latencies: make([]time.Duration, 0, publicVacuumChurnOperationsPerWorker)}
			for operation := 0; operation < publicVacuumChurnOperationsPerWorker; operation++ {
				sequence := round*publicVacuumChurnOperations + worker*publicVacuumChurnOperationsPerWorker + operation
				key := []byte(fmt.Sprintf("public-vacuum/foreground/%06d", sequence%256))
				value := []byte(fmt.Sprintf("value/%06d", sequence))
				started := time.Now()
				if err := d.Set(key, value); err != nil {
					result.foregroundErr = err
					break
				}
				got, err := d.Get(key)
				result.latencies = append(result.latencies, time.Since(started))
				if err != nil {
					result.foregroundErr = err
					break
				}
				if !bytes.Equal(got, value) {
					result.exposureMisses++
				}
				select {
				case <-vacuumFinished:
				default:
					result.overlap++
				}
			}
			results <- result
		}(worker)
	}
	ready.Wait()
	close(start)

	out := publicVacuumChurnResult{latencies: make([]time.Duration, 0, publicVacuumChurnOperations)}
	for worker := 0; worker < publicVacuumChurnWorkers; worker++ {
		result := <-results
		out.latencies = append(out.latencies, result.latencies...)
		out.overlap += result.overlap
		out.exposureMisses += result.exposureMisses
		if out.foregroundErr == nil {
			out.foregroundErr = result.foregroundErr
		}
	}
	out.vacuumErr = <-vacuumDone
	return out
}

func publicVacuumPercentile(values []time.Duration, percentile int) time.Duration {
	sorted := append([]time.Duration(nil), values...)
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

func openPublicVacuumComparisonFixture(tb testing.TB) (*DB, int64, [32]byte) {
	tb.Helper()
	dir := tb.TempDir()
	d, err := Open(publicVacuumComparisonOptions(dir))
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	value := bytes.Repeat([]byte("v"), 512)
	for generation := 0; generation < 3; generation++ {
		batch := d.NewBatch()
		for key := 0; key < 2048; key++ {
			value[0] = byte(generation + key%251)
			if err := batch.Set([]byte(fmt.Sprintf("public-vacuum/%04d", key)), value); err != nil {
				tb.Fatalf("set generation=%d key=%d: %v", generation, key, err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			tb.Fatalf("write generation=%d: %v", generation, err)
		}
		if err := batch.Close(); err != nil {
			tb.Fatalf("close generation=%d: %v", generation, err)
		}
	}
	if err := d.CompactIndex(); err != nil {
		tb.Fatalf("compact fixture: %v", err)
	}
	for generation := 0; generation < 2; generation++ {
		if err := d.SetSync([]byte("public-vacuum/0000"), bytes.Repeat([]byte{byte(240 + generation)}, 512)); err != nil {
			tb.Fatalf("advance durable slot=%d: %v", generation, err)
		}
	}
	return d, publicVacuumIndexBytes(tb, dir), publicVacuumDigest(tb, d)
}

func publicVacuumComparisonOptions(dir string) Options {
	return Options{
		Dir:                           dir,
		ChunkSize:                     32 << 10,
		KeepRecent:                    1,
		PreferAppendAlloc:             true,
		BackgroundIndexVacuumInterval: -1,
		ResolvedProfile:               backenddb.ProfileNoWALFast,
		Durability:                    DurabilityWALOffRelaxed,
	}
}

func publicVacuumIndexBytes(tb testing.TB, dir string) int64 {
	tb.Helper()
	path := filepath.Join(dir, "maindb", "index.db")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = filepath.Join(dir, "index.db")
	}
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("stat index: %v", err)
	}
	return info.Size()
}

func publicVacuumDigest(tb testing.TB, d *DB) [32]byte {
	tb.Helper()
	h := sha256.New()
	it, err := d.Iterator(nil, nil)
	if err != nil {
		tb.Fatalf("iterator: %v", err)
	}
	defer it.Close()
	for it.Valid() {
		h.Write(it.Key())
		h.Write(it.Value())
		it.Next()
	}
	if err := it.Error(); err != nil {
		tb.Fatalf("iterator: %v", err)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
