package collections

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkVectorIndexMixedSearchInsert4300 isolates graph construction and
// search-view publication from storage and HTTP overhead. Run one mode at a
// time so CPU profiles remain attributable:
//
//	TREEDB_VECTOR_MIXED_MODE=current go test ./TreeDB/collections -run '^$' \
//	  -bench '^BenchmarkVectorIndexMixedSearchInsert4300$' -benchtime=1x
func BenchmarkVectorIndexMixedSearchInsert4300(b *testing.B) {
	const (
		baseRows   = 10_000
		insertRows = 2_000
		dimensions = 768
		batchRows  = 100
		searchers  = 10
	)
	mode := os.Getenv("TREEDB_VECTOR_MIXED_MODE")
	if mode == "" {
		mode = "current"
	}
	validModes := map[string]bool{
		"current": true, "serial-plan": true, "serial-reciprocal": true,
		"all-serial": true, "no-publish": true,
	}
	if !validModes[mode] {
		b.Fatalf("unknown TREEDB_VECTOR_MIXED_MODE %q", mode)
	}

	b.StopTimer()
	rng := rand.New(rand.NewSource(4300))
	rows := make([][]float32, baseRows+insertRows)
	for row := range rows {
		rows[row] = make([]float32, dimensions)
		for dimension := range rows[row] {
			rows[row][dimension] = float32(rng.NormFloat64())
		}
	}
	ids := make([][]byte, len(rows))
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%05d", row))
	}
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: dimensions, M: 16, EfConstruction: 128, EfSearch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	index.setNativePersistent(true)
	if err := index.insertVectorBatchLocked(ids[:baseRows], rows[:baseRows]); err != nil {
		b.Fatal(err)
	}
	index.sourceDocumentRootsValid = true
	index.publishSearchViewLocked(false)
	if value := os.Getenv("TREEDB_VECTOR_MIXED_WORKERS"); value != "" {
		workers, err := strconv.Atoi(value)
		if err != nil || workers < 1 {
			b.Fatalf("invalid TREEDB_VECTOR_MIXED_WORKERS %q", value)
		}
		index.constructionWorkers = workers
	}
	if mode == "serial-reciprocal" || mode == "all-serial" {
		index.parallelReciprocalLinks = false
	}
	b.StartTimer()

	baseline := runVectorIndexMixedSearchWindow4300(b, index, rows[:256], searchers, 3*time.Second, nil)
	insert := func() error {
		started := time.Now()
		for start := baseRows; start < len(rows); start += batchRows {
			batchStarted := time.Now()
			end := minInt(start+batchRows, len(rows))
			index.mu.Lock()
			if mode == "serial-plan" || mode == "all-serial" {
				for row := start; row < end; row++ {
					if err := index.insertVectorLocked(ids[row], rows[row]); err != nil {
						index.mu.Unlock()
						return err
					}
				}
			} else if err := index.insertVectorBatchLocked(ids[start:end], rows[start:end]); err != nil {
				index.mu.Unlock()
				return err
			}
			if mode != "no-publish" {
				index.publishSearchViewLocked(false)
			}
			index.mu.Unlock()
			if wait := 200*time.Millisecond - time.Since(batchStarted); wait > 0 {
				time.Sleep(wait)
			}
		}
		b.ReportMetric(float64(insertRows)/time.Since(started).Seconds(), "insert_rows/s")
		return nil
	}
	finishProfile := startVectorIndexMixedProfile4300(b)
	mixed := runVectorIndexMixedSearchWindow4300(b, index, rows[:256], searchers, 0, insert)
	finishProfile()
	view := index.acquireSearchView()
	if view == nil || !view.sourceDocumentRootsValid {
		b.Fatal("native search view is unavailable")
	}
	visibleDocs := view.liveDocs
	index.releaseSearchView(view)
	wantVisible := len(rows)
	if mode == "no-publish" {
		wantVisible = baseRows
	}
	if visibleDocs != wantVisible {
		b.Fatalf("visible docs=%d want %d", visibleDocs, wantVisible)
	}
	b.ReportMetric(baseline.qps, "baseline_qps")
	b.ReportMetric(mixed.qps, "mixed_qps")
	b.ReportMetric(100*mixed.qps/baseline.qps, "mixed_qps_pct")
	b.ReportMetric(float64(baseline.p99.Microseconds()), "baseline_p99_us")
	b.ReportMetric(float64(mixed.p99.Microseconds()), "mixed_p99_us")
	b.ReportMetric(float64(index.frozenPrefixBatches), "frozen_batches")
	b.ReportMetric(float64(index.constructionWorkers), "worker_limit")
	b.ReportMetric(float64(visibleDocs), "visible_docs")
	b.ReportMetric(1, "native_route")
}

func startVectorIndexMixedProfile4300(b *testing.B) func() {
	b.Helper()
	dir := os.Getenv("TREEDB_VECTOR_MIXED_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	write := func(name string) {
		file, err := os.Create(filepath.Join(dir, name+".pprof"))
		if err != nil {
			b.Fatal(err)
		}
		if err := pprof.Lookup(name).WriteTo(file, 0); err != nil {
			_ = file.Close()
			b.Fatal(err)
		}
		if err := file.Close(); err != nil {
			b.Fatal(err)
		}
	}
	previousRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	runtime.GC()
	write("allocs")
	if err := os.Rename(filepath.Join(dir, "allocs.pprof"), filepath.Join(dir, "allocs_before.pprof")); err != nil {
		b.Fatal(err)
	}
	cpu, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		_ = cpu.Close()
		b.Fatal(err)
	}
	previousMutex := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	return func() {
		pprof.StopCPUProfile()
		_ = cpu.Close()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(previousMutex)
		runtime.GC()
		for _, name := range []string{"allocs", "heap", "mutex", "block"} {
			write(name)
		}
		runtime.MemProfileRate = previousRate
	}
}

type vectorIndexMixedSearchWindow4300 struct {
	qps float64
	p99 time.Duration
}

func runVectorIndexMixedSearchWindow4300(b *testing.B, index *VectorIndex, queries [][]float32, searchers int, duration time.Duration, work func() error) vectorIndexMixedSearchWindow4300 {
	b.Helper()
	stop := make(chan struct{})
	latencies := make([][]time.Duration, searchers)
	var count atomic.Uint64
	var wg sync.WaitGroup
	errCh := make(chan error, searchers)
	for worker := 0; worker < searchers; worker++ {
		worker := worker
		wg.Go(func() {
			buffer := new(VectorIndexSearchBuffer)
			for query := worker; ; query++ {
				select {
				case <-stop:
					return
				default:
				}
				started := time.Now()
				if _, _, err := index.searchGraphOnlyWithBuffer(queries[query%len(queries)], 100, 64, buffer); err != nil {
					errCh <- err
					return
				}
				latencies[worker] = append(latencies[worker], time.Since(started))
				count.Add(1)
			}
		})
	}
	started := time.Now()
	var workErr error
	if work != nil {
		workErr = work()
	} else {
		time.Sleep(duration)
	}
	elapsed := time.Since(started)
	close(stop)
	wg.Wait()
	close(errCh)
	if workErr != nil {
		b.Fatal(workErr)
	}
	for err := range errCh {
		b.Errorf("search: %v", err)
	}
	all := latencies[0]
	for worker := 1; worker < len(latencies); worker++ {
		all = append(all, latencies[worker]...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	if len(all) == 0 {
		b.Fatal("no successful searches")
	}
	p99 := all[minInt(len(all)-1, (len(all)*99)/100)]
	return vectorIndexMixedSearchWindow4300{qps: float64(count.Load()) / elapsed.Seconds(), p99: p99}
}
