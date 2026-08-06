package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

type vectorPartitionSystemBenchMetricsV1 struct {
	Queries          int     `json:"queries"`
	CompletedQueries int     `json:"completed_queries"`
	ResultCount      int     `json:"result_count"`
	Errors           int     `json:"errors"`
	Timeouts         int     `json:"timeouts"`
	RecallAt10       float64 `json:"recall_at_10"`
	QPS              float64 `json:"qps"`
	P50Nanos         uint64  `json:"p50_nanos"`
	P95Nanos         uint64  `json:"p95_nanos"`
	P99Nanos         uint64  `json:"p99_nanos"`
}

type vectorPartitionSystemBenchCellV1 struct {
	Status      string                              `json:"status"`
	Error       string                              `json:"error,omitempty"`
	Budget      map[string]int                      `json:"budget"`
	Concurrency int                                 `json:"concurrency"`
	Metrics     vectorPartitionSystemBenchMetricsV1 `json:"metrics"`
	Counters    map[string]uint64                   `json:"counters"`
	TotalNanos  []uint64                            `json:"total_nanos"`
}

type vectorPartitionSystemBenchResultV1 struct {
	SchemaVersion       int                                `json:"schema_version"`
	ResultKind          string                             `json:"result_kind"`
	Endpoint            string                             `json:"endpoint"`
	DatasetChecksum     string                             `json:"dataset_checksum"`
	TruthArtifactSHA256 string                             `json:"truth_artifact_sha256"`
	TopK                int                                `json:"top_k"`
	EfSearch            int                                `json:"ef_search"`
	WarmupQueries       int                                `json:"warmup_queries"`
	StartedAt           time.Time                          `json:"started_at"`
	CompletedAt         time.Time                          `json:"completed_at"`
	Cells               []vectorPartitionSystemBenchCellV1 `json:"cells"`
}

func runVectorPartitionSystemBenchV1(args []string, stdout io.Writer) error {
	return runVectorPartitionSystemBenchWithCellV1(args, stdout, vectorPartitionSystemBenchCell)
}

func runVectorPartitionSystemBenchWithCellV1(args []string, stdout io.Writer, runCell func(context.Context, string, [][]float32, [][]m8CanonicalResultV1, int, int, int, int, int) (vectorPartitionSystemBenchCellV1, error)) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-bench", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var endpoint, dataset, truthCache, truthSHA, probeText, concurrencyText, out string
	var topK, efSearch, warmup int
	fs.StringVar(&endpoint, "endpoint", "", "production operations TCP endpoint")
	fs.StringVar(&dataset, "dataset", "", "fixture manifest directory")
	fs.StringVar(&truthCache, "truth-cache", "", "trusted truth-cache directory")
	fs.StringVar(&truthSHA, "truth-cache-sha256", "", "trusted truth-cache artifact SHA256")
	fs.StringVar(&probeText, "probes", "", "ordered comma-separated probe budgets")
	fs.StringVar(&concurrencyText, "concurrency", "", "ordered comma-separated concurrency values")
	fs.StringVar(&out, "out", "", "exclusive result JSON path")
	fs.IntVar(&topK, "top-k", 10, "neighbors per query")
	fs.IntVar(&efSearch, "ef-search", 128, "partition-local HNSW ef-search")
	fs.IntVar(&warmup, "warmup", 1000, "warmup queries per cell")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || endpoint == "" || dataset == "" || truthCache == "" || truthSHA == "" || probeText == "" || concurrencyText == "" || out == "" || topK != 10 || efSearch < topK || warmup < 0 {
		return errors.New("system-bench requires bounded endpoint, dataset, truth, probes, concurrency, and output")
	}
	probes, err := vectorPartitionSystemPositiveListV1(probeText)
	if err != nil {
		return fmt.Errorf("system-bench probes: %w", err)
	}
	concurrency, err := vectorPartitionSystemPositiveListV1(concurrencyText)
	if err != nil {
		return fmt.Errorf("system-bench concurrency: %w", err)
	}
	if len(probes)*len(concurrency) > 64 {
		return errors.New("system-bench matrix exceeds 64 cells")
	}
	for _, value := range probes {
		if value > 16 {
			return errors.New("system-bench probes exceed 16")
		}
	}
	for _, value := range concurrency {
		if value > 64 {
			return errors.New("system-bench concurrency exceeds 64")
		}
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	_, queries64 := fixtureData(fixture)
	queries := make([][]float32, len(queries64))
	for i := range queries64 {
		queries[i] = make([]float32, len(queries64[i]))
		for d, value := range queries64[i] {
			queries[i][d] = float32(value)
		}
	}
	truthPath := m8TruthCacheArtifactPathV1(truthCache, m8TruthCacheIdentityV1(fixture, topK))
	truth, artifactSHA, err := m8ReadTruthCacheV1(truthPath, fixture, len(queries), topK, uint64(fixture.Vectors), truthSHA)
	if err != nil {
		return fmt.Errorf("system-bench truth: %w", err)
	}
	result := vectorPartitionSystemBenchResultV1{SchemaVersion: 1, ResultKind: "vector_partition_system_bench_v1", Endpoint: endpoint, DatasetChecksum: fixture.Checksum, TruthArtifactSHA256: artifactSHA, TopK: topK, EfSearch: efSearch, WarmupQueries: warmup, StartedAt: time.Now().UTC()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, probes := range probes {
		for _, workers := range concurrency {
			cell, runErr := runCell(ctx, endpoint, queries, truth, topK, probes, efSearch, workers, warmup)
			if runErr != nil {
				cell.Status = "failed"
				cell.Error = runErr.Error()
				cell.Metrics.Errors++
				if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runErr, os.ErrDeadlineExceeded) {
					cell.Metrics.Timeouts++
				}
				result.Cells = append(result.Cells, cell)
				result.CompletedAt = time.Now().UTC()
				writeErr := writeVectorPartitionSystemJSONExclusiveV1(out, result)
				if writeErr != nil {
					return errors.Join(fmt.Errorf("system-bench probes=%d concurrency=%d: %w", probes, workers, runErr), fmt.Errorf("retain failed system-bench result: %w", writeErr))
				}
				return fmt.Errorf("system-bench probes=%d concurrency=%d failed result retained at %s: %w", probes, workers, out, runErr)
			}
			result.Cells = append(result.Cells, cell)
		}
	}
	result.CompletedAt = time.Now().UTC()
	if !result.CompletedAt.After(result.StartedAt) {
		return errors.New("system-bench completion timestamp is invalid")
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(out, result); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "result=%s cells=%d\n", out, len(result.Cells))
	return err
}

func vectorPartitionSystemBenchCell(ctx context.Context, endpoint string, queries [][]float32, truth [][]m8CanonicalResultV1, topK, probes, efSearch, workers, warmup int) (vectorPartitionSystemBenchCellV1, error) {
	cell := vectorPartitionSystemBenchCellV1{Status: "valid", Budget: map[string]int{"probes": probes}, Concurrency: workers, Metrics: vectorPartitionSystemBenchMetricsV1{Queries: len(queries)}}
	clients := make([]*vectorPartitionOperationsTCPClientV1, workers)
	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()
	for i := range clients {
		client, err := dialVectorPartitionOperationsV1(ctx, endpoint)
		if err != nil {
			return cell, err
		}
		clients[i] = client
	}
	status, err := clients[0].call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "status"})
	if err != nil {
		return cell, fmt.Errorf("system-bench status: %w", err)
	}
	if status.Health == nil || !status.Health.Ready {
		return cell, errors.New("system-bench status is not ready")
	}
	generation := status.Health.Generation
	request := func(query []float32) public.SearchRequestV1 {
		return public.SearchRequestV1{
			Version: 1, Generation: generation, Query: query,
			Metric: public.MetricCosineV1, TopK: topK, Probes: probes, EfSearch: efSearch, Consistency: public.ConsistencyGenerationSnapshotV1,
			Limits: public.SearchLimitsV1{RequestBytes: 4 << 20, CandidateBytes: 64 << 20, ResponseBytes: 16 << 20, MergeEntries: 256 * topK}, Deadline: time.Now().Add(30 * time.Second),
		}
	}
	if err := vectorPartitionSystemRunQueriesV1(ctx, clients, warmup, func(index int) error {
		_, err := clients[index%workers].call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: request(queries[index%len(queries)])})
		return err
	}); err != nil {
		return cell, fmt.Errorf("warmup: %w", err)
	}
	outcomes := make([]*public.SearchResponseV1, len(queries))
	durations := make([]uint64, len(queries))
	started := time.Now()
	err = vectorPartitionSystemRunQueriesV1(ctx, clients, len(queries), func(index int) error {
		queryStarted := time.Now()
		wire, callErr := clients[index%workers].call(vectorPartitionOperationsWireRequestV1{SchemaVersion: 1, Operation: "search", Search: request(queries[index])})
		durations[index] = uint64(time.Since(queryStarted))
		if callErr != nil {
			return callErr
		}
		outcomes[index] = wire.Search
		return nil
	})
	elapsed := time.Since(started)
	if err != nil {
		return cell, err
	}
	var recall float64
	counters := map[string]uint64{"selected_partitions": 0, "selected_groups": 0, "requests": 0, "rpcs": 0, "retries": 0, "redirects": 0, "candidates": 0, "edges": 0, "query_bytes": 0, "request_bytes": 0, "candidate_bytes": 0, "response_bytes": 0}
	for index, outcome := range outcomes {
		if outcome == nil {
			return cell, fmt.Errorf("query %d returned no search response", index)
		}
		if len(outcome.Neighbors) != topK {
			return cell, fmt.Errorf("query %d returned %d neighbors, want %d", index, len(outcome.Neighbors), topK)
		}
		got := make([]m8CanonicalResultV1, len(outcome.Neighbors))
		for i, neighbor := range outcome.Neighbors {
			got[i] = m8CanonicalResultV1{ID: neighbor.ID, Score: neighbor.Score}
		}
		recall += m8CanonicalRecallV1(truth[index], got)
		c := outcome.Counters
		for key, value := range map[string]uint64{"selected_partitions": c.SelectedPartitions, "selected_groups": c.SelectedGroups, "requests": c.Requests, "rpcs": c.RPCs, "retries": c.Retries, "redirects": c.Redirects, "candidates": c.Candidates, "edges": c.Edges, "query_bytes": c.QueryBytes, "request_bytes": c.RequestBytes, "candidate_bytes": c.CandidateBytes, "response_bytes": c.ResponseBytes} {
			if math.MaxUint64-counters[key] < value {
				return cell, errors.New("system-bench counter overflow")
			}
			counters[key] += value
		}
	}
	cell.Metrics.CompletedQueries, cell.Metrics.ResultCount = len(queries), len(queries)*topK
	cell.Metrics.RecallAt10, cell.Metrics.QPS = recall/float64(len(queries)), float64(len(queries))/elapsed.Seconds()
	cell.Metrics.P50Nanos, cell.Metrics.P95Nanos, cell.Metrics.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	cell.Counters, cell.TotalNanos = counters, durations
	return cell, nil
}

func vectorPartitionSystemRunQueriesV1(ctx context.Context, clients []*vectorPartitionOperationsTCPClientV1, count int, run func(int) error) error {
	if count == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, len(clients))
	for worker := range clients {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for index := worker; index < count && ctx.Err() == nil; index += len(clients) {
				if err := run(index); err != nil {
					select {
					case errs <- err:
					default:
					}
					cancel()
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return ctx.Err()
	}
}

func vectorPartitionSystemPositiveListV1(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]bool{}
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value <= 0 || seen[value] {
			return nil, errors.New("values must be distinct positive integers")
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

func writeVectorPartitionSystemJSONExclusiveV1(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(raw, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
