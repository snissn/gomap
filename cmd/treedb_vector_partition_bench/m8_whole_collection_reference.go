package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const m8WholeCollectionReferenceV1 = "ordinary_whole_collection_context_not_partition_qualification_v1"

type m8WholeCollectionHitV1 struct {
	ID        string `json:"id"`
	Ordinal   int    `json:"ordinal"`
	ScoreBits uint64 `json:"score_float64_bits"`
}

type m8WholeCollectionAttemptV1 struct {
	Class       string                             `json:"class"`
	Error       string                             `json:"error,omitempty"`
	SearchNanos uint64                             `json:"search_nanos"`
	ResultCount int                                `json:"result_count"`
	Results     []m8WholeCollectionHitV1           `json:"results"`
	Strategy    collections.VectorIndexStrategy    `json:"strategy"`
	Path        collections.VectorIndexSearchPath  `json:"path"`
	Stats       collections.VectorIndexSearchStats `json:"stats"`
}

type m8WholeCollectionCellV1 struct {
	Kind           string                       `json:"kind"`
	Repetition     int                          `json:"repetition"`
	EfSearch       int                          `json:"ef_search"`
	Concurrency    int                          `json:"concurrency"`
	SetupNanos     uint64                       `json:"setup_nanos"`
	WarmupNanos    uint64                       `json:"warmup_nanos"`
	SetupError     string                       `json:"setup_or_warmup_error,omitempty"`
	ElapsedNanos   uint64                       `json:"elapsed_nanos"`
	AllocatedBytes uint64                       `json:"phase_allocated_bytes"`
	Allocations    uint64                       `json:"phase_allocations"`
	Attempts       []m8WholeCollectionAttemptV1 `json:"attempts"`
	Summary        m8MeasurementSummaryV1       `json:"summary"`
	P50Nanos       uint64                       `json:"successful_p50_nanos"`
	P95Nanos       uint64                       `json:"successful_p95_nanos"`
	P99Nanos       uint64                       `json:"successful_p99_nanos"`
}

// The JSONL stream is header, every planned cell in order, then a completion
// footer. Each line is synced outside measurement. Missing footer/cells are an
// incomplete run, not a successful subset. Existing outputs are never replaced.
func runM8WholeCollectionReferenceV1(args []string, stdout io.Writer) (runErr error) {
	fs := flag.NewFlagSet("whole-collection-reference", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var out, efRaw, concurrencyRaw string
	var repetitions, warmup int
	fs.StringVar(&out, "out", "", "fresh JSONL reference receipt")
	fs.StringVar(&efRaw, "ef-search", "", "one to three prospectively declared ordinary HNSW ef values")
	fs.StringVar(&concurrencyRaw, "concurrency", "1,32", "declared concurrency: 1, 32, or 1,32")
	fs.IntVar(&repetitions, "repetitions", 5, "measured repetitions, 1..5 (final comparison requires 5)")
	fs.IntVar(&warmup, "warmup", 64, "warmup calls per coordinate, 0..4096")
	if err := fs.Parse(args); err != nil {
		return err
	}
	efs, err := parseInts(efRaw)
	if err != nil || len(efs) < 1 || len(efs) > 3 || !allUnique(efs) {
		return errors.New("reference requires one to three distinct ef-search values")
	}
	for _, ef := range efs {
		if ef < 10 || ef > 4096 {
			return errors.New("reference ef-search must be in [10,4096]")
		}
	}
	concurrencies, err := parseInts(concurrencyRaw)
	if err != nil || len(concurrencies) < 1 || len(concurrencies) > 2 || !allUnique(concurrencies) {
		return errors.New("reference requires distinct concurrency 1 and/or 32")
	}
	for _, concurrency := range concurrencies {
		if concurrency != 1 && concurrency != 32 {
			return errors.New("reference concurrency must be 1 or 32")
		}
	}
	if !filepath.IsAbs(out) || repetitions < 1 || repetitions > 5 || warmup < 0 || warmup > 4096 || len(fs.Args()) == 0 {
		return errors.New("reference requires fresh -out, bounded repetitions/warmup, and -- followed by all replay-m8-report arguments")
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		return errors.New("reference output must not already exist")
	}
	var parent m8ProductionReportV1
	var replay bytes.Buffer
	if err := replayM8ReportV1(fs.Args(), &replay, &parent); err != nil {
		return err
	}
	if parent.Dataset.Queries > 4096 || parent.Config.TopK != 10 || parent.Variant == nil {
		return errors.New("reference requires retained M3, top-k 10 and at most 4096 queries")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executableSHA, err := m8BenchmarkExecutableSHA256V1(executable)
	if err != nil || executableSHA != parent.ExecutableSHA256 {
		return errors.New("reference executable must equal the strictly replayed producer/builder executable")
	}
	queries64, err := loadFixtureQueriesV1(parent.DatasetDirectory, parent.Dataset)
	if err != nil {
		return err
	}
	queries := make([][]float32, len(queries64))
	for i, query := range queries64 {
		queries[i] = make([]float32, len(query))
		for j, value := range query {
			queries[i][j] = float32(value)
		}
	}
	truthPath := m8TruthCacheArtifactPathV1(parent.TruthCacheDirectory, parent.TruthCache.Identity)
	truth, _, err := m8ReadTruthCacheV1(truthPath, parent.Dataset, len(queries), 10, uint64(parent.Dataset.Vectors), parent.TruthCache.ArtifactSHA256)
	if err != nil {
		return err
	}
	assets, err := openM8ProductionExistingAssetSetV1(parent.Variant.DatabaseDirectory)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, assets.Close()) }()
	if err := m8BindRetainedM3DescriptorWithPolicyV1(assets, parent.Dataset, true); err != nil {
		return err
	}
	if !reflect.DeepEqual(assets.descriptor, parent.Variant) {
		return errors.New("reference source changed after replay")
	}
	var definition collections.VectorIndexDefinition
	for _, index := range assets.collection.MetaView().VectorIndexes {
		if index.Name == partitionHNSWIndex {
			definition = index
		}
	}
	if definition.Strategy != collections.VectorIndexStrategyColumnGraph || definition.Metric != collections.VectorMetricCosine {
		return errors.New("reference requires the ordinary persisted cosine column_graph index")
	}
	host := m8ProductionHostV1(config{out: filepath.Dir(out), dataset: parent.DatasetDirectory}, assets.dir)
	if host != parent.Host || runtime.Version() != parent.GoVersion || runtime.GOOS != parent.GOOS || runtime.GOARCH != parent.GOARCH || runtime.NumCPU() != parent.LogicalCPUs || runtime.GOMAXPROCS(0) != parent.GOMAXPROCS || debug.SetMemoryLimit(-1) != parent.GoMemoryLimitBytes {
		return errors.New("reference requires the replayed host, mounts and Go runtime settings")
	}
	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, file.Close()) }()
	encoder := json.NewEncoder(file)
	write := func(value any) error {
		if err := encoder.Encode(value); err != nil {
			return err
		}
		return file.Sync()
	}
	// Do not call this a transport-equivalent baseline: source HNSW and ordinary
	// cosine normalization also differ from the partition-local Vamana contract.
	header := struct {
		Kind               string                            `json:"kind"`
		Contract           string                            `json:"contract"`
		ReplayReceipt      string                            `json:"replay_receipt"`
		ParentExecutionID  string                            `json:"parent_execution_id"`
		HeadSHA            string                            `json:"head_sha"`
		ExecutableSHA256   string                            `json:"executable_sha256"`
		Command            []string                          `json:"exact_command"`
		Dataset            fixtureManifest                   `json:"dataset"`
		Truth              m8TruthCacheEvidenceV1            `json:"truth"`
		Variant            m3VariantDescriptorV1             `json:"variant"`
		Index              collections.VectorIndexDefinition `json:"index"`
		Host               m8ProductionHostEvidenceV1        `json:"host"`
		GoVersion          string                            `json:"go_version"`
		GOOS               string                            `json:"goos"`
		GOARCH             string                            `json:"goarch"`
		TimingScope        string                            `json:"timing_scope"`
		ScoreScope         string                            `json:"score_scope"`
		AllocationScope    string                            `json:"allocation_scope"`
		ProfileScope       string                            `json:"profile_scope"`
		GOMAXPROCS         int                               `json:"gomaxprocs"`
		LogicalCPUs        int                               `json:"logical_cpus"`
		Repetitions        int                               `json:"repetitions"`
		Warmup             int                               `json:"warmup"`
		GoMemoryLimitBytes int64                             `json:"go_memory_limit_bytes"`
		EfSearch           []int                             `json:"ef_search"`
		Concurrency        []int                             `json:"concurrency"`
	}{
		Kind: "header", Contract: m8WholeCollectionReferenceV1, ReplayReceipt: replay.String(), ParentExecutionID: parent.ExecutionID,
		HeadSHA: parent.HeadSHA, ExecutableSHA256: executableSHA, Command: append([]string{executable, "whole-collection-reference"}, args...),
		Dataset: parent.Dataset, Truth: parent.TruthCache, Variant: *assets.descriptor, Index: definition,
		Host:      host,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0), GoMemoryLimitBytes: debug.SetMemoryLimit(-1),
		Repetitions: repetitions, Warmup: warmup, EfSearch: efs, Concurrency: concurrencies,
		TimingScope:     "unprofiled in-process public SearchWithBuffer; per-call latency excludes receipt copying; phase wall includes dispatch and bounded copying; setup/warmup separate; no TCP/Raft",
		ScoreScope:      "ordinary HNSW cosine scores retained as returned float64 bits; quality uses pinned canonical global FP32 truth IDs, not an ordinary-score exact oracle",
		AllocationScope: "process TotalAlloc/Mallocs delta across complete measured phase including receipt ID ownership and worker dispatch; excludes preallocated receipt slots, setup and warmup",
		ProfileScope:    "none; do not ratio against profiled TCP windows as a causal partition speedup",
	}
	if err := write(header); err != nil {
		return err
	}
	type coordinate struct{ ef, concurrency int }
	var coordinates []coordinate
	for _, ef := range efs {
		for _, concurrency := range concurrencies {
			coordinates = append(coordinates, coordinate{ef, concurrency})
		}
	}
	for repetition := range repetitions {
		for i := range coordinates {
			index := i
			if repetition%2 != 0 {
				index = len(coordinates) - 1 - i
			}
			coordinate := coordinates[index]
			cell := m8MeasureWholeCollectionV1(assets.collection, queries, coordinate.ef, coordinate.concurrency, warmup)
			cell.Kind, cell.Repetition = "cell", repetition
			if err := m8SummarizeWholeCollectionV1(&cell, truth, parent.Dataset.Vectors); err != nil {
				// Keep raw completed outcomes even if summary derivation rejects.
				return errors.Join(err, write(cell))
			}
			if err := write(cell); err != nil {
				return err
			}
		}
	}
	peak, measured := vectorPartitionBenchmarkPeakRSS()
	if err := write(struct {
		Kind            string `json:"kind"`
		Status          string `json:"status"`
		PeakRSSScope    string `json:"peak_rss_scope"`
		Cells           int    `json:"cells"`
		PeakRSSBytes    int64  `json:"peak_rss_bytes"`
		PeakRSSMeasured bool   `json:"peak_rss_measured"`
	}{"footer", "complete_not_qualification", "whole process including strict replay, source open, warmup and measurements; not per-query heap", repetitions * len(coordinates), peak, measured}); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "WHOLE_COLLECTION_REFERENCE_RETAINED_NOT_QUALIFICATION path=%s cells=%d\n", out, repetitions*len(coordinates))
	return err
}

func m8MeasureWholeCollectionV1(col *collections.Collection, queries [][]float32, ef, concurrency, warmup int) (cell m8WholeCollectionCellV1) {
	cell.EfSearch, cell.Concurrency = ef, concurrency
	cell.Attempts = make([]m8WholeCollectionAttemptV1, len(queries))
	for i := range cell.Attempts {
		cell.Attempts[i].Class = "not_dispatched"
		cell.Attempts[i].Results = make([]m8WholeCollectionHitV1, 0, 10)
	}
	if len(queries) == 0 || concurrency < 1 || concurrency > 32 || ef < 10 || warmup < 0 {
		cell.SetupError = "invalid bounded reference coordinate"
		return
	}
	setup := time.Now()
	searchers := make([]*collections.VectorIndexSearcher, concurrency)
	buffers := make([]collections.VectorIndexSearchBuffer, concurrency)
	defer func() {
		for _, searcher := range searchers {
			if searcher != nil {
				if err := searcher.Close(); err != nil {
					cell.SetupError += "close: " + err.Error()
				}
			}
		}
	}()
	for i := range searchers {
		var err error
		searchers[i], err = col.OpenVectorIndexSearcher(collections.VectorIndexSearcherOptions{IndexName: partitionHNSWIndex})
		if err != nil {
			cell.SetupError, cell.SetupNanos = err.Error(), uint64(time.Since(setup))
			return
		}
	}
	cell.SetupNanos = uint64(time.Since(setup))
	opts := func(i int) collections.VectorIndexSearcherSearchOptions {
		return collections.VectorIndexSearcherSearchOptions{Query: queries[i], TopK: 10, EfSearch: ef, StatsMode: collections.VectorIndexSearchStatsModeWorkAccounting}
	}
	startWarmup := time.Now()
	for i := range warmup {
		if _, err := searchers[i%concurrency].SearchWithBuffer(opts(i%len(queries)), &buffers[i%concurrency]); err != nil {
			cell.SetupError, cell.WarmupNanos = err.Error(), uint64(time.Since(startWarmup))
			return
		}
	}
	cell.WarmupNanos = uint64(time.Since(startWarmup))
	var next atomic.Int64
	var wg sync.WaitGroup
	worker := func(workerID int) {
		defer wg.Done()
		for {
			i := int(next.Add(1) - 1)
			if i >= len(queries) {
				return
			}
			a := &cell.Attempts[i]
			start := time.Now()
			response, err := searchers[workerID].SearchWithBuffer(opts(i), &buffers[workerID])
			a.SearchNanos = max(1, uint64(time.Since(start)))
			a.Class = "returned"
			if err != nil {
				a.Class, a.Error = "error", err.Error()
			}
			a.Strategy, a.Path, a.Stats, a.ResultCount = response.Strategy, response.Path, response.Stats, len(response.Results)
			// Borrowed API results cannot outlive this worker's next query. Bound
			// malformed results without silently treating truncation as valid.
			for _, result := range response.Results[:min(10, len(response.Results))] {
				a.Results = append(a.Results, m8WholeCollectionHitV1{string(result.ID), result.Ordinal, math.Float64bits(result.Score)})
			}
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	wg.Add(concurrency)
	for workerID := range concurrency {
		go worker(workerID)
	}
	wg.Wait()
	cell.ElapsedNanos = max(1, uint64(time.Since(start)))
	runtime.ReadMemStats(&after)
	cell.AllocatedBytes, cell.Allocations = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
	return
}

func m8SummarizeWholeCollectionV1(cell *m8WholeCollectionCellV1, truth [][]m8CanonicalResultV1, vectors int) error {
	if len(truth) == 0 || len(cell.Attempts) != len(truth) || vectors < 1 || cell.Concurrency < 1 {
		return errors.New("invalid whole-collection population")
	}
	summary := m8MeasurementSummaryV1{Declared: len(truth)}
	cell.P50Nanos, cell.P95Nanos, cell.P99Nanos = 0, 0, 0
	var durations []uint64
	for i := range cell.Attempts {
		a := &cell.Attempts[i]
		if len(truth[i]) != min(10, vectors) {
			return errors.New("invalid whole-collection truth width")
		}
		summary.TruthSlots += len(truth[i])
		if a.Class == "not_dispatched" {
			if a.SearchNanos != 0 || len(a.Results) != 0 || a.ResultCount != 0 || a.Error != "" || a.Strategy != "" || a.Path != "" || !reflect.DeepEqual(a.Stats, collections.VectorIndexSearchStats{}) {
				return errors.New("undispatched reference claims work")
			}
			summary.NotDispatched++
			continue
		}
		if a.SearchNanos == 0 || a.SearchNanos > cell.ElapsedNanos {
			return errors.New("invalid reference terminal duration")
		}
		summary.Dispatched++
		if a.Class == "error" {
			if a.Error == "" {
				return errors.New("reference error lacks a retained cause")
			}
			summary.Errors++
			continue
		}
		if a.Class != "returned" && a.Class != "success" && a.Class != "invalid_response" {
			return errors.New("unknown reference terminal class")
		}
		valid := a.Error == "" && a.ResultCount == len(truth[i]) && len(a.Results) == a.ResultCount &&
			a.Strategy == collections.VectorIndexStrategyColumnGraph && a.Path == collections.VectorIndexSearchPathColumnGraphNativeReader &&
			a.Stats.DocumentsFetched == 0 && a.Stats.SearchRouteColumnGraphFallback == 0 && a.Stats.GraphRowFallbacks == 0 && a.Stats.TypedColumnFallbacks == 0
		var ids []string
		var ordinals []int
		for _, result := range a.Results {
			score := math.Float64frombits(result.ScoreBits)
			// Public ordinals address the persisted graph, not fixture order.
			// Quality is joined by document ID, as in the native report validator.
			if result.Ordinal < 0 || result.Ordinal >= vectors || !m8FixtureDocumentIDValidV1(result.ID, vectors) || math.IsNaN(score) || math.IsInf(score, 0) || slices.Contains(ids, result.ID) || slices.Contains(ordinals, result.Ordinal) {
				valid = false
			}
			ids = append(ids, result.ID)
			ordinals = append(ordinals, result.Ordinal)
		}
		if !valid {
			a.Class = "invalid_response"
			summary.Invalid++
			continue
		}
		a.Class = "success"
		summary.Succeeded++
		summary.TruthHits += m8IDHitCountV1(m8CanonicalIDsV1(truth[i]), ids)
		durations = append(durations, a.SearchNanos)
	}
	if summary.NotDispatched > 0 && cell.SetupError == "" {
		return errors.New("undispatched reference population lacks setup failure")
	}
	if summary.Dispatched > 0 {
		if cell.ElapsedNanos == 0 {
			return errors.New("reference window is missing")
		}
		summary.CompletionRate = float64(summary.Succeeded) / float64(summary.Dispatched)
		summary.AttemptRate = float64(summary.Dispatched) * 1e9 / float64(cell.ElapsedNanos)
		summary.Goodput = float64(summary.Succeeded) * 1e9 / float64(cell.ElapsedNanos)
	}
	if summary.Succeeded > 0 {
		summary.SuccessRecall = float64(summary.TruthHits) / float64(summary.Succeeded*min(10, vectors))
	}
	summary.ServiceRecall = float64(summary.TruthHits) / float64(summary.TruthSlots)
	cell.Summary = summary
	if len(durations) > 0 {
		cell.P50Nanos, cell.P95Nanos, cell.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	}
	return nil
}
