package collections

import (
	"fmt"
	"math/rand"
	"os"
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
//
// Use live-delta-cutover only when the live-delta fold cost is the intended
// measurement; it inserts enough rows to cross the production bound once.
func BenchmarkVectorIndexMixedSearchInsert4300(b *testing.B) {
	if b.N != 1 {
		b.Fatalf("run with -benchtime=1x; fixed mixed window got b.N=%d", b.N)
	}
	const (
		baseRows   = 10_000
		dimensions = 768
		batchRows  = 100
		searchers  = 10
	)
	mode := os.Getenv("TREEDB_VECTOR_MIXED_MODE")
	if mode == "" {
		mode = "current"
	}
	validModes := map[string]bool{
		"current": true, "serial-reciprocal": true, "no-publish": true, "live-delta": true, "live-delta-cutover": true,
	}
	if !validModes[mode] {
		b.Fatalf("unknown TREEDB_VECTOR_MIXED_MODE %q", mode)
	}
	insertRows := 2_000
	if mode == "live-delta-cutover" {
		insertRows = defaultVectorIndexLiveDeltaRows + batchRows
	}
	if value := os.Getenv("TREEDB_VECTOR_MIXED_INSERT_ROWS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			b.Fatalf("invalid TREEDB_VECTOR_MIXED_INSERT_ROWS %q", value)
		}
		insertRows = parsed
	}
	batchPace := 200 * time.Millisecond
	if value := os.Getenv("TREEDB_VECTOR_MIXED_BATCH_PACE"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 0 {
			b.Fatalf("invalid TREEDB_VECTOR_MIXED_BATCH_PACE %q", value)
		}
		batchPace = parsed
	}
	accountSearchWork := os.Getenv("TREEDB_VECTOR_MIXED_ACCOUNT_SEARCH_WORK") == "1"

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
	if mode == "serial-reciprocal" {
		index.parallelReciprocalLinks = false
	}
	b.StartTimer()

	baseline := runVectorIndexMixedSearchWindow4300(b, index, rows[:256], searchers, 3*time.Second, nil, accountSearchWork)
	insert := func() error {
		started := time.Now()
		for start := baseRows; start < len(rows); start += batchRows {
			batchStarted := time.Now()
			end := minInt(start+batchRows, len(rows))
			index.mu.Lock()
			var err error
			if mode == "live-delta" || mode == "live-delta-cutover" {
				err = index.insertLiveVectorBatchLocked(ids[start:end], rows[start:end])
			} else {
				err = index.insertVectorBatchLocked(ids[start:end], rows[start:end])
			}
			if err != nil {
				index.mu.Unlock()
				return err
			}
			if mode != "no-publish" {
				index.publishSearchViewLocked(false)
			}
			index.mu.Unlock()
			if wait := batchPace - time.Since(batchStarted); batchPace > 0 && wait > 0 {
				time.Sleep(wait)
			}
		}
		b.ReportMetric(float64(insertRows)/time.Since(started).Seconds(), "insert_rows/s")
		return nil
	}
	mixed := runVectorIndexMixedSearchWindow4300(b, index, rows[:256], searchers, 0, insert, accountSearchWork)
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
	b.ReportMetric(float64(batchPace.Microseconds())/1000, "batch_pace_ms")
	b.ReportMetric(float64(index.frozenPrefixBatches), "frozen_batches")
	b.ReportMetric(float64(index.constructionWorkers), "worker_limit")
	b.ReportMetric(float64(visibleDocs), "visible_docs")
	stats := index.Stats()
	if mode == "live-delta-cutover" && stats.LiveDeltaCutovers == 0 {
		b.Fatal("live-delta-cutover did not cross the configured bound")
	}
	b.ReportMetric(float64(stats.LiveDeltaDocs), "live_delta_docs")
	b.ReportMetric(float64(stats.LiveDeltaCutovers), "live_delta_cutovers")
	b.ReportMetric(1, "native_route")
	if accountSearchWork {
		b.ReportMetric(100*float64(mixed.deltaSearches)/float64(mixed.searches), "delta_search_pct")
		b.ReportMetric(float64(mixed.deltaVisited)/float64(mixed.searches), "delta_visited/search")
		b.ReportMetric(100*float64(mixed.retryChanged)/float64(mixed.searches), "retry_changed_topk_pct")
		if mixed.deltaSearches > 0 {
			b.ReportMetric(100*float64(mixed.retrySearches)/float64(mixed.deltaSearches), "retry_incidence_pct")
			b.ReportMetric(float64(mixed.initialTopK)/float64(mixed.deltaSearches), "initial_topk")
			b.ReportMetric(float64(mixed.terminalTopK)/float64(mixed.deltaSearches), "terminal_topk")
			b.ReportMetric(float64(mixed.terminalEfSearch)/float64(mixed.deltaSearches), "terminal_ef_search")
		}
		if mixed.retrySearches > 0 {
			b.ReportMetric(float64(mixed.deltaRetries)/float64(mixed.retrySearches), "retry_depth/retry")
			b.ReportMetric(100*float64(mixed.deltaResumes)/float64(mixed.deltaRetries), "retry_resumed_pct")
		}
	}
}

type vectorIndexMixedSearchWindow4300 struct {
	qps              float64
	p99              time.Duration
	searches         uint64
	deltaSearches    uint64
	retrySearches    uint64
	deltaPasses      uint64
	deltaRetries     uint64
	deltaResumes     uint64
	deltaVisited     uint64
	retryChanged     uint64
	initialTopK      uint64
	terminalTopK     uint64
	terminalEfSearch uint64
}

func runVectorIndexMixedSearchWindow4300(b *testing.B, index *VectorIndex, queries [][]float32, searchers int, duration time.Duration, work func() error, accountSearchWork bool) vectorIndexMixedSearchWindow4300 {
	b.Helper()
	stop := make(chan struct{})
	latencies := make([][]time.Duration, searchers)
	var count atomic.Uint64
	var deltaSearches, retrySearches, deltaPasses, deltaRetries, deltaResumes, deltaVisited, retryChanged atomic.Uint64
	var initialTopK, terminalTopK, terminalEfSearch atomic.Uint64
	var wg sync.WaitGroup
	errCh := make(chan error, searchers)
	for worker := 0; worker < searchers; worker++ {
		worker := worker
		wg.Go(func() {
			buffer := new(VectorIndexSearchBuffer)
			buffer.nativeSearchWorkEnabled = accountSearchWork
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
				if accountSearchWork {
					searchWork := buffer.nativeSearchWork
					if searchWork.deltaPasses > 0 {
						deltaSearches.Add(1)
						deltaPasses.Add(uint64(searchWork.deltaPasses))
						deltaRetries.Add(uint64(searchWork.deltaRetries))
						deltaResumes.Add(uint64(searchWork.deltaResumes))
						deltaVisited.Add(uint64(searchWork.deltaVisited))
						initialTopK.Add(uint64(searchWork.deltaInitialTopK))
						terminalTopK.Add(uint64(searchWork.deltaTerminalTopK))
						terminalEfSearch.Add(uint64(searchWork.deltaTerminalEfSearch))
						if searchWork.deltaRetries > 0 {
							retrySearches.Add(1)
						}
						if searchWork.retryChangedMergedTopK {
							retryChanged.Add(1)
						}
					}
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
	close(stop)
	wg.Wait()
	elapsed := time.Since(started)
	total := count.Load()
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
	return vectorIndexMixedSearchWindow4300{
		qps:              float64(total) / elapsed.Seconds(),
		p99:              p99,
		searches:         total,
		deltaSearches:    deltaSearches.Load(),
		retrySearches:    retrySearches.Load(),
		deltaPasses:      deltaPasses.Load(),
		deltaRetries:     deltaRetries.Load(),
		deltaResumes:     deltaResumes.Load(),
		deltaVisited:     deltaVisited.Load(),
		retryChanged:     retryChanged.Load(),
		initialTopK:      initialTopK.Load(),
		terminalTopK:     terminalTopK.Load(),
		terminalEfSearch: terminalEfSearch.Load(),
	}
}
