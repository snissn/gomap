package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const m8ProductionMultiGroupModeV1 = "production_multi_group"

type m8ProductionReportV1 struct {
	SchemaVersion      int                                                        `json:"schema_version"`
	ResultKind         string                                                     `json:"result_kind"`
	Status             string                                                     `json:"status"`
	Mode               string                                                     `json:"mode"`
	ProductionEvidence bool                                                       `json:"production_evidence"`
	GeneratedAt        time.Time                                                  `json:"generated_at"`
	Command            []string                                                   `json:"exact_command"`
	BaseSHA            string                                                     `json:"base_sha"`
	HeadSHA            string                                                     `json:"head_sha"`
	Dirty              bool                                                       `json:"dirty"`
	GoVersion          string                                                     `json:"go_version"`
	GOOS               string                                                     `json:"goos"`
	GOARCH             string                                                     `json:"goarch"`
	LogicalCPUs        int                                                        `json:"logical_cpus"`
	Host               m8ProductionHostEvidenceV1                                 `json:"host"`
	Dataset            fixtureManifest                                            `json:"dataset"`
	Config             m8ProductionConfigEvidenceV1                               `json:"config"`
	BuildNanos         int64                                                      `json:"build_nanos"`
	Topology           nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 `json:"topology"`
	Rows               []m8ProductionRowV1                                        `json:"rows"`
	Failure            m8ProductionFailureEvidenceV1                              `json:"failure"`
	GateLedger         m8ProductionGateLedgerV1                                   `json:"gate_ledger"`
	Profiles           m8ProductionProfileEvidenceV1                              `json:"profiles"`
	Resources          m8ProductionResourceEvidenceV1                             `json:"resources"`
	TimedBoundary      string                                                     `json:"timed_boundary"`
	Limitations        []string                                                   `json:"limitations"`
}

type m8ProductionConfigEvidenceV1 struct {
	RaftGroups        int       `json:"raft_groups"`
	RaftNodesPerGroup int       `json:"raft_nodes_per_group"`
	Partitions        int       `json:"partitions"`
	Probes            []int     `json:"probes"`
	Overlap           []float64 `json:"overlap"`
	TopK              int       `json:"top_k"`
	RecallTarget      float64   `json:"recall_target"`
	Concurrency       []int     `json:"concurrency"`
	Warmup            int       `json:"warmup_requests"`
	EfSearch          []int     `json:"ef_search"`
	Seed              int64     `json:"seed"`
}

type m8ProductionRowV1 struct {
	Status             string  `json:"status"`
	UnsupportedReason  string  `json:"unsupported_reason,omitempty"`
	Overlap            float64 `json:"overlap"`
	Probes             int     `json:"probes,omitempty"`
	EfSearch           int     `json:"ef_search,omitempty"`
	Concurrency        int     `json:"concurrency,omitempty"`
	RouterMode         string  `json:"router_mode,omitempty"`
	RouterCandidates   int     `json:"router_candidate_budget,omitempty"`
	Samples            int     `json:"samples,omitempty"`
	RecallAtK          float64 `json:"recall_at_k,omitempty"`
	QPS                float64 `json:"qps,omitempty"`
	P50Nanos           uint64  `json:"p50_nanos,omitempty"`
	P95Nanos           uint64  `json:"p95_nanos,omitempty"`
	P99Nanos           uint64  `json:"p99_nanos,omitempty"`
	RequestBytes       uint64  `json:"request_bytes,omitempty"`
	ResponseBytes      uint64  `json:"response_bytes,omitempty"`
	CandidateBytes     uint64  `json:"candidate_bytes,omitempty"`
	RPCs               uint64  `json:"rpcs,omitempty"`
	ExactParityChecked bool    `json:"exact_all_partition_parity_checked"`
	ExactParityPassed  bool    `json:"exact_all_partition_parity_passed"`
	NoPartialResults   bool    `json:"no_partial_results"`
}

type m8ProductionFailureEvidenceV1 struct {
	Class             string `json:"class"`
	StoppedGroup      string `json:"stopped_group"`
	Error             string `json:"error"`
	ReturnedNeighbors int    `json:"returned_neighbors"`
	ReturnedGroups    int    `json:"returned_groups"`
	Passed            bool   `json:"passed"`
}

type m8ProductionGateLedgerV1 struct {
	ExhaustiveParity string `json:"exhaustive_correctness"`
	FailureHonesty   string `json:"failure_honesty"`
	Recall           string `json:"recall"`
	ProbeReduction   string `json:"probe_reduction"`
	EndToEndQPS      string `json:"end_to_end_qps"`
	TailLatency      string `json:"tail_latency"`
	Balance          string `json:"balance"`
	OverlapStorage   string `json:"overlap_storage"`
	ResourceBounds   string `json:"resource_bounds"`
	ExistingBehavior string `json:"existing_behavior"`
}

type m8ProductionProfileEvidenceV1 struct {
	Directory string   `json:"directory,omitempty"`
	Captured  []string `json:"captured"`
	Status    string   `json:"status"`
	Scope     string   `json:"scope"`
}

type m8ProductionHostEvidenceV1 struct {
	CPUModel      string `json:"cpu_model"`
	MemoryBytes   uint64 `json:"memory_bytes,omitempty"`
	NUMANodes     string `json:"numa_nodes,omitempty"`
	Kernel        string `json:"kernel,omitempty"`
	ArtifactMount string `json:"artifact_mount,omitempty"`
	DatasetMount  string `json:"dataset_mount,omitempty"`
}

type m8ProductionResourceEvidenceV1 struct {
	PersistentAssetBytes uint64 `json:"persistent_asset_bytes"`
	PeakRSSBytes         int64  `json:"peak_rss_bytes,omitempty"`
	PeakRSSMeasured      bool   `json:"peak_rss_measured"`
	OverlapMemberships   int    `json:"overlap_memberships"`
	MaxPartitionLoad     uint64 `json:"max_partition_load"`
	BalanceHardCap       uint64 `json:"balance_hard_cap"`
	MmapStatus           string `json:"mmap_status"`
}

func runM8ProductionMultiGroupV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) (runErr error) {
	groups := make([]string, cfg.raftGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("m8-data-group-%02d", i)
	}
	started := time.Now()
	var assets *m8ProductionMultiGroupAssetsV1
	var err error
	if cfg.m8ExistingDB != "" {
		assets, err = openM8ProductionMultiGroupExistingAssetsV1(cfg.m8ExistingDB, groups, cfg.partitions, fixture, vectors)
	} else {
		assets, err = newM8ProductionMultiGroupAssetsV1(vectors, groups, cfg.partitions)
	}
	if err != nil {
		return fmt.Errorf("open M8 production assets: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, assets.Close()) }()
	topologyCtx, cancelTopology := context.WithTimeout(context.Background(), 2*time.Minute)
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(topologyCtx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{
		Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(),
		GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default",
	})
	cancelTopology()
	if err != nil {
		return fmt.Errorf("build M8 production topology: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, topology.Close()) }()
	buildNanos := time.Since(started).Nanoseconds()

	truth, err := m8ExactTruthV1(assets.collection, queries, cfg.topK)
	if err != nil {
		return err
	}
	report := m8ProductionReportV1{
		SchemaVersion: 1, ResultKind: "m8_production_multi_group_evidence_v1", Status: "incomplete",
		Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now().UTC(),
		Command: cfg.command, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dirty: m8GitDirtyV1(),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), Host: m8ProductionHostV1(cfg), Dataset: fixture,
		Config:        m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: append([]int(nil), cfg.probes...), Overlap: append([]float64(nil), cfg.overlaps...), TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: append([]int(nil), cfg.concurrency...), Warmup: cfg.warmup, EfSearch: append([]int(nil), cfg.efSearch...), Seed: cfg.seed},
		BuildNanos:    buildNanos,
		Profiles:      m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Status: "not_captured", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs requires the captured baseline for differential analysis"},
		Resources:     m8ProductionResourcesV1(assets),
		TimedBoundary: "wall-clock query cells after topology, exhaustive endpoint preflight, and generation warmup; includes router, coordinator, TCP M5 serialization, Raft read-index/apply, persistent HNSW search, response merge, and caller scheduling; excludes topology construction, exact truth, preflight, warmup, artifact encoding, and shutdown",
		Limitations: []string{
			"loopback TCP with real serialized M5 messages and real in-memory HashiCorp Raft consensus; not a multi-host deployment",
			"the checked-in 10k path materializes disjoint round-robin packs; -m8-existing-db reuses the retained graph-built M3 packs read-only",
			"overlap 0.20 and stable-hash attribution are reported unsupported; the broader lifecycle matrix is separate test evidence and matched-recall acceptance remains gated",
		},
	}
	if cfg.profiles != "" {
		if err := os.MkdirAll(cfg.profiles, 0o755); err != nil {
			return fmt.Errorf("create M8 profiles directory: %w", err)
		}
	}
	if err := m8WarmProductionTopologyV1(context.Background(), topology.Coordinator(), assets, queries, cfg); err != nil {
		return err
	}
	profileCapture, err := startM8ProfileCaptureV1(cfg.profiles)
	if err != nil {
		return err
	}
	defer func() {
		if profileCapture != nil {
			_, closeErr := profileCapture.Stop()
			runErr = errors.Join(runErr, closeErr)
		}
	}()
	for _, overlap := range cfg.overlaps {
		if overlap != 0 {
			report.Rows = append(report.Rows, m8ProductionRowV1{Status: "unsupported", UnsupportedReason: "overlap assets are not materialized by the initial M8 production topology checkpoint", Overlap: overlap})
			continue
		}
		for _, probes := range cfg.probes {
			for _, ef := range cfg.efSearch {
				for _, concurrency := range cfg.concurrency {
					row, rowErr := m8RunProductionCellV1(context.Background(), topology.Coordinator(), assets, queries, truth, probes, ef, concurrency, cfg.topK)
					if rowErr != nil {
						return fmt.Errorf("M8 production cell probes=%d ef=%d concurrency=%d: %w", probes, ef, concurrency, rowErr)
					}
					row.Overlap = overlap
					report.Rows = append(report.Rows, row)
				}
			}
		}
	}
	report.Topology = topology.Evidence()
	report.Failure = m8RunUnavailableGroupV1(context.Background(), topology, assets, queries[0], cfg.topK)
	if profileCapture != nil {
		captured, stopErr := profileCapture.Stop()
		if stopErr != nil {
			return stopErr
		}
		report.Profiles = m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Captured: captured, Status: "captured_production_query_and_fault_boundary", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs.pprof is cumulative and must be compared with allocs_baseline.pprof"}
	}
	report.Resources = m8ProductionResourcesV1(assets)
	report.GateLedger = m8ProductionGateLedgerForReportV1(report)
	if m8ProductionAllGatesPassV1(report.GateLedger) {
		report.Status = "pass"
	} else if m8ProductionAnyGateFailsV1(report.GateLedger) {
		report.Status = "experimental_gate_failures"
	}
	if err := validateM8ProductionReportV1(report); err != nil {
		return fmt.Errorf("validate M8 production report: %w", err)
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	name := fmt.Sprintf("vector_partition_m8_%s.json", cfg.headSHA[:provenanceSuffixBytes])
	path := filepath.Join(cfg.out, name)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return err
	}
	if cfg.format == "json" {
		_, err = fmt.Fprintln(stdout, string(raw))
	} else {
		_, err = fmt.Fprintf(stdout, "M8 status=%s artifact=%s rows=%d\n", report.Status, path, len(report.Rows))
	}
	return err
}

type m8ProfileCaptureV1 struct {
	dir            string
	cpu, traceFile *os.File
	oldMutex       int
	once           sync.Once
	paths          []string
	err            error
}

func startM8ProfileCaptureV1(dir string) (*m8ProfileCaptureV1, error) {
	if dir == "" {
		return nil, nil
	}
	capture := &m8ProfileCaptureV1{dir: dir}
	baseline := filepath.Join(dir, "allocs_baseline.pprof")
	if err := writeM8RuntimeProfileV1("allocs", baseline); err != nil {
		return nil, fmt.Errorf("write M8 allocation baseline: %w", err)
	}
	capture.paths = append(capture.paths, baseline)
	var err error
	capture.traceFile, err = os.Create(filepath.Join(dir, "trace.out"))
	if err != nil {
		return nil, fmt.Errorf("create M8 trace: %w", err)
	}
	if err = trace.Start(capture.traceFile); err != nil {
		_ = capture.traceFile.Close()
		return nil, fmt.Errorf("start M8 trace: %w", err)
	}
	capture.cpu, err = os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		trace.Stop()
		_ = capture.traceFile.Close()
		return nil, fmt.Errorf("create M8 CPU profile: %w", err)
	}
	if err = pprof.StartCPUProfile(capture.cpu); err != nil {
		trace.Stop()
		_ = capture.traceFile.Close()
		_ = capture.cpu.Close()
		return nil, fmt.Errorf("start M8 CPU profile: %w", err)
	}
	capture.oldMutex = runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	return capture, nil
}

func (c *m8ProfileCaptureV1) Stop() ([]string, error) {
	if c == nil {
		return nil, nil
	}
	c.once.Do(func() {
		pprof.StopCPUProfile()
		trace.Stop()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(c.oldMutex)
		c.err = errors.Join(c.cpu.Close(), c.traceFile.Close())
		c.paths = append(c.paths, filepath.Join(c.dir, "cpu.pprof"), filepath.Join(c.dir, "trace.out"))
		for _, item := range []struct {
			name, file string
		}{{"heap", "heap.pprof"}, {"allocs", "allocs.pprof"}, {"block", "block.pprof"}, {"mutex", "mutex.pprof"}} {
			path := filepath.Join(c.dir, item.file)
			file, err := os.Create(path)
			if err == nil {
				profile := pprof.Lookup(item.name)
				if profile == nil {
					err = fmt.Errorf("M8 runtime profile %s unavailable", item.name)
				} else {
					err = profile.WriteTo(file, 0)
				}
			}
			if file != nil {
				err = errors.Join(err, file.Close())
			}
			if err != nil {
				c.err = errors.Join(c.err, fmt.Errorf("write M8 %s profile: %w", item.name, err))
			} else {
				c.paths = append(c.paths, path)
			}
		}
	})
	return append([]string(nil), c.paths...), c.err
}

func writeM8RuntimeProfileV1(name, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	profile := pprof.Lookup(name)
	if profile == nil {
		err = fmt.Errorf("M8 runtime profile %s unavailable", name)
	} else {
		err = profile.WriteTo(file, 0)
	}
	return errors.Join(err, file.Close())
}

func m8ExactTruthV1(collection *collections.Collection, queries [][]float64, topK int) ([][]string, error) {
	truth := make([][]string, len(queries))
	for i, query64 := range queries {
		query := m8Query32V1(query64)
		results, err := collection.SearchVectorsExact(query, collections.VectorSearchOptions{Field: "embedding", Metric: collections.VectorMetricCosine, TopK: topK})
		if err != nil {
			return nil, fmt.Errorf("M8 exact truth query %d: %w", i, err)
		}
		truth[i] = make([]string, len(results))
		for rank := range results {
			truth[i][rank] = string(results[rank].DocumentID)
		}
	}
	return truth, nil
}

func m8WarmProductionTopologyV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, cfg config) error {
	if len(queries) == 0 {
		return errors.New("M8 warmup requires a query")
	}
	efSearch := cfg.topK
	for _, value := range cfg.efSearch {
		efSearch = max(efSearch, value)
	}
	// This untimed request is deliberately independent of -warmup: every
	// advertised data group must be exercised before its evidence can support a
	// production result, including runs with no user-configured warmup or only
	// low-probe measured rows.
	requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	_, err := coordinator.Search(requestCtx, m8ProductionRequestV1(assets, m8Query32V1(queries[0]), "m8-endpoint-preflight", len(assets.manifest.Placements), efSearch, cfg.topK))
	cancel()
	if err != nil {
		return fmt.Errorf("M8 exhaustive endpoint preflight: %w", err)
	}
	for i := 0; i < cfg.warmup; i++ {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := coordinator.Search(requestCtx, m8ProductionRequestV1(assets, m8Query32V1(queries[i%len(queries)]), fmt.Sprintf("m8-warmup-%06d", i), len(assets.manifest.Placements), efSearch, cfg.topK))
		cancel()
		if err != nil {
			return fmt.Errorf("M8 topology warmup %d: %w", i, err)
		}
	}
	return nil
}

func m8ProductionResourcesV1(assets *m8ProductionMultiGroupAssetsV1) m8ProductionResourceEvidenceV1 {
	var out m8ProductionResourceEvidenceV1
	if assets == nil {
		return out
	}
	for _, asset := range assets.manifest.Assets {
		out.PersistentAssetBytes += asset.Bytes
	}
	out.PersistentAssetBytes += assets.manifest.RouterAsset.Bytes
	out.OverlapMemberships = len(assets.manifest.OverlapMemberships)
	loads := make([]uint64, assets.manifest.PartitionCount)
	for _, membership := range assets.manifest.Memberships {
		loads[membership.PartitionID]++
	}
	for _, load := range loads {
		out.MaxPartitionLoad = max(out.MaxPartitionLoad, load)
	}
	// Integer ceiling of mean * 1.05, matching the default balance epsilon.
	rows, partitions := assets.manifest.SourceRowCount, uint64(assets.manifest.PartitionCount)
	out.BalanceHardCap = (rows*105 + partitions*100 - 1) / (partitions * 100)
	out.MmapStatus = "not_captured_by_m8_runner; retained M3/M5 artifacts own mapped-pack evidence"
	if peak, ok := vectorPartitionBenchmarkPeakRSS(); ok {
		out.PeakRSSBytes, out.PeakRSSMeasured = peak, true
	}
	return out
}

func m8RunProductionCellV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, truth [][]string, probes, efSearch, concurrency, topK int) (m8ProductionRowV1, error) {
	type outcome struct {
		response nativewire.VectorPartitionCoordinatorResponseV1
		err      error
	}
	outcomes := make([]outcome, len(queries))
	started := time.Now()
	m8RunBoundedWorkV1(len(queries), concurrency, func(index int) {
		query := m8Query32V1(queries[index])
		select {
		case <-ctx.Done():
			outcomes[index].err = ctx.Err()
			return
		default:
		}
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		outcomes[index].response, outcomes[index].err = coordinator.Search(requestCtx, m8ProductionRequestV1(assets, query, fmt.Sprintf("m8-q-%06d-p-%04d-ef-%06d-c-%03d", index, probes, efSearch, concurrency), probes, efSearch, topK))
	})
	elapsed := time.Since(started)
	row := m8ProductionRowV1{Status: "pass", Probes: probes, EfSearch: efSearch, Concurrency: concurrency, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidates: max(1, int(assets.status.Representatives)), Samples: len(queries), ExactParityChecked: probes == len(assets.manifest.Placements), ExactParityPassed: probes == len(assets.manifest.Placements), NoPartialResults: true}
	durations := make([]uint64, 0, len(outcomes))
	var recallSum float64
	for index, outcome := range outcomes {
		if outcome.err != nil {
			return row, fmt.Errorf("query %d: %w", index, outcome.err)
		}
		got := make([]string, len(outcome.response.Neighbors))
		for rank := range outcome.response.Neighbors {
			got[rank] = outcome.response.Neighbors[rank].ID
		}
		recallSum += m8IDRecallV1(truth[index], got)
		if row.ExactParityChecked && !m8EqualIDsV1(truth[index], got) {
			row.ExactParityPassed = false
			row.Status = "fail"
		}
		durations = append(durations, outcome.response.Timing.TotalNanos)
		row.RequestBytes += outcome.response.Counters.RequestBytes
		row.ResponseBytes += outcome.response.Counters.ResponseBytes
		row.CandidateBytes += outcome.response.Counters.CandidateBytes
		row.RPCs += outcome.response.Counters.RPCs
	}
	row.RecallAtK = recallSum / float64(len(outcomes))
	row.QPS = float64(len(outcomes)) / elapsed.Seconds()
	row.P50Nanos, row.P95Nanos, row.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	return row, nil
}

// m8RunBoundedWorkV1 starts no more than concurrency workers, rather than one
// goroutine per query waiting behind a semaphore.
func m8RunBoundedWorkV1(count, concurrency int, run func(int)) {
	if count == 0 {
		return
	}
	if concurrency < 1 {
		concurrency = 1
	}
	workers := min(count, concurrency)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				run(index)
			}
		}()
	}
	for index := range count {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
}

func m8ProductionRequestV1(assets *m8ProductionMultiGroupAssetsV1, query []float32, requestID string, probes, efSearch, topK int) nativewire.VectorPartitionCoordinatorRequestV1 {
	return nativewire.VectorPartitionCoordinatorRequestV1{
		Version: nativewire.VectorPartitionCoordinatorVersionV1, RequestID: requestID, CancellationID: requestID + "-cancel",
		Database: "default", Catalog: "default", Collection: assets.manifest.Collection, IndexName: assets.manifest.IndexName,
		IndexDefinitionDigest: assets.manifest.IndexDefinitionDigest, Query: query, Metric: nativewire.VectorPartitionShardSearchMetricCosineV1,
		RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidateBudget: max(1, int(assets.status.Representatives)), PartitionProbes: probes,
		Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: nativewire.VectorPartitionShardSearchStatsBasicV1,
		TopK: topK, EfSearch: efSearch, DeadlineUnixNano: time.Now().Add(30 * time.Second).UnixNano(), RequestBytesLimit: 4 << 20,
		CandidateBytesLimit: 64 << 20, ResponseBytesLimit: 64 << 20, MergeEntriesLimit: probes * topK,
	}
}

func m8RunUnavailableGroupV1(ctx context.Context, topology *nativewire.VectorPartitionM8ProductionMultiGroupV1, assets *m8ProductionMultiGroupAssetsV1, query64 []float64, topK int) m8ProductionFailureEvidenceV1 {
	evidence := topology.Evidence()
	result := m8ProductionFailureEvidenceV1{Class: "unavailable_group_endpoint"}
	if len(evidence.Groups) == 0 {
		result.Error = "topology exposed no groups"
		return result
	}
	result.StoppedGroup = evidence.Groups[0].GroupID
	if err := topology.StopGroup(result.StoppedGroup); err != nil {
		result.Error = err.Error()
		return result
	}
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	response, err := topology.Coordinator().Search(requestCtx, m8ProductionRequestV1(assets, m8Query32V1(query64), "m8-unavailable-group", len(assets.manifest.Placements), 4096, topK))
	if err != nil {
		result.Error = err.Error()
	}
	result.ReturnedNeighbors, result.ReturnedGroups = len(response.Neighbors), len(response.ProbedGroups)
	result.Passed = err != nil && result.ReturnedNeighbors == 0 && result.ReturnedGroups == 0
	return result
}

func m8ProductionGateLedgerForReportV1(report m8ProductionReportV1) m8ProductionGateLedgerV1 {
	ledger := m8ProductionGateLedgerV1{ExhaustiveParity: "not_run", FailureHonesty: "fail", Recall: "fail", ProbeReduction: "fail", EndToEndQPS: "fail", TailLatency: "fail", Balance: "fail", OverlapStorage: "fail", ResourceBounds: "fail", ExistingBehavior: "pending_full_required_suites"}
	var exhaustive []m8ProductionRowV1
	var candidates []m8ProductionRowV1
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			continue
		}
		if row.ExactParityChecked {
			exhaustive = append(exhaustive, row)
			if !row.ExactParityPassed {
				ledger.ExhaustiveParity = "fail"
			} else if ledger.ExhaustiveParity != "fail" {
				ledger.ExhaustiveParity = "pass"
			}
		}
		if row.RecallAtK >= report.Config.RecallTarget {
			ledger.Recall = "pass"
			if row.Probes*4 <= report.Config.Partitions {
				ledger.ProbeReduction = "pass"
				candidates = append(candidates, row)
			}
		}
	}
	for _, candidate := range candidates {
		for _, base := range exhaustive {
			if candidate.EfSearch != base.EfSearch || candidate.Concurrency != base.Concurrency {
				continue
			}
			if candidate.QPS >= base.QPS*1.15 {
				ledger.EndToEndQPS = "pass"
			}
			if candidate.P95Nanos <= base.P95Nanos {
				ledger.TailLatency = "pass"
			}
		}
	}
	if report.Failure.Passed {
		ledger.FailureHonesty = "pass"
	}
	if report.Resources.BalanceHardCap > 0 && report.Resources.MaxPartitionLoad <= report.Resources.BalanceHardCap {
		ledger.Balance = "pass"
	}
	if report.Resources.PersistentAssetBytes > 0 && report.Resources.PeakRSSMeasured {
		// The runner observes RSS but has no declared process-RSS ceiling. Do
		// not convert an observation into a resource-bound pass claim.
		ledger.ResourceBounds = "measured_not_bounded"
	}
	return ledger
}

func m8ProductionGateValuesV1(ledger m8ProductionGateLedgerV1) []string {
	return []string{ledger.ExhaustiveParity, ledger.FailureHonesty, ledger.Recall, ledger.ProbeReduction, ledger.EndToEndQPS, ledger.TailLatency, ledger.Balance, ledger.OverlapStorage, ledger.ResourceBounds, ledger.ExistingBehavior}
}

func m8ProductionAllGatesPassV1(ledger m8ProductionGateLedgerV1) bool {
	for _, value := range m8ProductionGateValuesV1(ledger) {
		if value != "pass" {
			return false
		}
	}
	return true
}

func m8ProductionAnyGateFailsV1(ledger m8ProductionGateLedgerV1) bool {
	for _, value := range m8ProductionGateValuesV1(ledger) {
		if value == "fail" {
			return true
		}
	}
	return false
}

func m8Query32V1(in []float64) []float32 {
	out := make([]float32, len(in))
	for i := range in {
		out[i] = float32(in[i])
	}
	return out
}

func m8IDRecallV1(want, got []string) float64 {
	if len(want) == 0 {
		return 0
	}
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		seen[id] = true
	}
	found := 0
	for _, id := range want {
		if seen[id] {
			found++
		}
	}
	return float64(found) / float64(len(want))
}

func m8EqualIDsV1(want, got []string) bool {
	if len(want) != len(got) {
		return false
	}
	for i := range want {
		if want[i] != got[i] {
			return false
		}
	}
	return true
}

func m8PercentileV1(values []uint64, percentile int) uint64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]uint64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (percentile*len(ordered)+99)/100 - 1
	return ordered[index]
}

func m8GitDirtyV1() bool {
	command := exec.Command("git", "status", "--porcelain")
	raw, err := command.Output()
	return err != nil || strings.TrimSpace(string(raw)) != ""
}

func validateM8ProductionReportV1(report m8ProductionReportV1) error {
	if report.SchemaVersion != 1 || report.ResultKind != "m8_production_multi_group_evidence_v1" ||
		report.Mode != m8ProductionMultiGroupModeV1 || !report.ProductionEvidence ||
		report.GeneratedAt.IsZero() || len(report.Command) == 0 || !validSHA(report.BaseSHA) || !validSHA(report.HeadSHA) ||
		report.Config.RaftGroups < 2 || report.Config.RaftNodesPerGroup != 3 || report.Config.Partitions < 4 ||
		report.Config.Warmup < 0 || report.BuildNanos <= 0 || report.TimedBoundary == "" || len(report.Limitations) == 0 {
		return errors.New("missing or invalid M8 identity, topology, or timing metadata")
	}
	if err := validateM3FixtureWithCaps(report.Dataset, maxVectors, maxFixtureBytes); err != nil {
		return fmt.Errorf("dataset: %w", err)
	}
	if len(report.Topology.Groups) != report.Config.RaftGroups || report.Topology.Network != "tcp_loopback_serialized_m5_v1" ||
		report.Topology.LifecycleState != "active" || !m8SHA256V1(report.Topology.ReadySetDigest) || len(report.Topology.MetaNodes) != 3 {
		return errors.New("incomplete M8 production topology evidence")
	}
	owners, leaders := map[string]bool{}, map[string]bool{}
	for _, group := range report.Topology.Groups {
		if group.GroupID == "" || owners[group.GroupID] || group.LeaderID == "" || len(group.NodeIDs) != 3 ||
			group.CommitIndex == 0 || group.ReadIndex == 0 || group.AppliedIndex == 0 || !group.ProvesProductionConsensus ||
			group.ReadEvidenceKind != "production" || group.EndpointHits == 0 {
			return errors.New("invalid M8 data-group evidence")
		}
		owners[group.GroupID], leaders[group.LeaderID] = true, true
	}
	if report.Config.RaftGroups >= 4 && len(leaders) < 3 {
		return errors.New("deep M8 topology did not distribute leaders")
	}
	if len(report.Rows) == 0 {
		return errors.New("M8 report has no measurement rows")
	}
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			if row.UnsupportedReason == "" || row.Overlap == 0 {
				return errors.New("malformed unsupported M8 row")
			}
			continue
		}
		if row.Status != "pass" && row.Status != "fail" || row.Probes < 1 || row.Probes > report.Config.Partitions ||
			row.EfSearch < report.Config.TopK || row.Concurrency < 1 || row.Samples != report.Dataset.Queries || row.QPS <= 0 ||
			row.RouterMode == "" || row.RouterCandidates < 1 || row.ExactParityChecked != (row.Probes == report.Config.Partitions) {
			return errors.New("malformed measured M8 row")
		}
	}
	if !report.Failure.Passed || report.Failure.Error == "" || report.Failure.ReturnedNeighbors != 0 || report.Failure.ReturnedGroups != 0 ||
		report.GateLedger.FailureHonesty != "pass" || report.Resources.PersistentAssetBytes == 0 {
		return errors.New("incomplete M8 failure or resource evidence")
	}
	if report.Profiles.Status == "captured_production_query_and_fault_boundary" {
		if len(report.Profiles.Captured) != 7 || report.Profiles.Scope == "" {
			return errors.New("incomplete M8 profile evidence")
		}
		for _, path := range report.Profiles.Captured {
			info, err := os.Stat(path)
			if err != nil || info.Size() == 0 {
				return fmt.Errorf("M8 profile %q is missing or empty", path)
			}
		}
	}
	return nil
}

func m8SHA256V1(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
