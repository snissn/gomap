package main

import (
	"context"
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
	Dataset            fixtureManifest                                            `json:"dataset"`
	Config             m8ProductionConfigEvidenceV1                               `json:"config"`
	BuildNanos         int64                                                      `json:"build_nanos"`
	Topology           nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 `json:"topology"`
	Rows               []m8ProductionRowV1                                        `json:"rows"`
	Failure            m8ProductionFailureEvidenceV1                              `json:"failure"`
	GateLedger         m8ProductionGateLedgerV1                                   `json:"gate_ledger"`
	Profiles           m8ProductionProfileEvidenceV1                              `json:"profiles"`
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
}

func runM8ProductionMultiGroupV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) (runErr error) {
	groups := make([]string, cfg.raftGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("m8-data-group-%02d", i)
	}
	started := time.Now()
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, groups, cfg.partitions)
	if err != nil {
		return fmt.Errorf("build M8 production assets: %w", err)
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

	truth, err := m8ExactTruthV1(assets.collection, queries, cfg.topK)
	if err != nil {
		return err
	}
	report := m8ProductionReportV1{
		SchemaVersion: 1, ResultKind: "m8_production_multi_group_evidence_v1", Status: "incomplete",
		Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now().UTC(),
		Command: cfg.command, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dirty: m8GitDirtyV1(),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), Dataset: fixture,
		Config:     m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: append([]int(nil), cfg.probes...), Overlap: append([]float64(nil), cfg.overlaps...), TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: append([]int(nil), cfg.concurrency...), EfSearch: append([]int(nil), cfg.efSearch...), Seed: cfg.seed},
		BuildNanos: time.Since(started).Nanoseconds(),
		Profiles:   m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Status: "not_captured_by_initial_topology_checkpoint"},
		Limitations: []string{
			"loopback TCP with real serialized M5 messages and real in-memory HashiCorp Raft consensus; not a multi-host deployment",
			"initial runner materializes disjoint round-robin local packs; graph partition, overlap, stable-hash attribution, lifecycle matrix, deep profiles, and matched-recall acceptance remain pending",
		},
	}
	if cfg.profiles != "" {
		if err := os.MkdirAll(cfg.profiles, 0o755); err != nil {
			return fmt.Errorf("create M8 profiles directory: %w", err)
		}
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
		report.Profiles = m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Captured: captured, Status: "captured_production_query_and_fault_boundary"}
	}
	report.GateLedger = m8InitialGateLedgerV1(report.Rows, report.Failure)
	if report.GateLedger.ExhaustiveParity == "pass" && report.GateLedger.FailureHonesty == "pass" {
		report.Status = "partial_pass_pending_deep_gates"
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
		c.paths = []string{filepath.Join(c.dir, "cpu.pprof"), filepath.Join(c.dir, "trace.out")}
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

func m8RunProductionCellV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, truth [][]string, probes, efSearch, concurrency, topK int) (m8ProductionRowV1, error) {
	type outcome struct {
		response nativewire.VectorPartitionCoordinatorResponseV1
		err      error
	}
	outcomes := make([]outcome, len(queries))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	started := time.Now()
	for index, query64 := range queries {
		index, query := index, m8Query32V1(query64)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				outcomes[index].err = ctx.Err()
				return
			}
			requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			outcomes[index].response, outcomes[index].err = coordinator.Search(requestCtx, m8ProductionRequestV1(assets, query, fmt.Sprintf("m8-q-%06d-p-%04d-ef-%06d-c-%03d", index, probes, efSearch, concurrency), probes, efSearch, topK))
		}()
	}
	wg.Wait()
	elapsed := time.Since(started)
	row := m8ProductionRowV1{Status: "pass", Probes: probes, EfSearch: efSearch, Concurrency: concurrency, Samples: len(queries), ExactParityChecked: probes == len(assets.manifest.Placements), ExactParityPassed: probes == len(assets.manifest.Placements), NoPartialResults: true}
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

func m8InitialGateLedgerV1(rows []m8ProductionRowV1, failure m8ProductionFailureEvidenceV1) m8ProductionGateLedgerV1 {
	ledger := m8ProductionGateLedgerV1{ExhaustiveParity: "not_run", FailureHonesty: "fail", Recall: "not_evaluated", ProbeReduction: "not_evaluated", EndToEndQPS: "not_evaluated", TailLatency: "not_evaluated", Balance: "not_evaluated", OverlapStorage: "not_evaluated", ResourceBounds: "not_evaluated", ExistingBehavior: "pending_full_required_suites"}
	for _, row := range rows {
		if row.ExactParityChecked {
			if row.ExactParityPassed {
				ledger.ExhaustiveParity = "pass"
			} else {
				ledger.ExhaustiveParity = "fail"
				break
			}
		}
	}
	if failure.Passed {
		ledger.FailureHonesty = "pass"
	}
	return ledger
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
	command := exec.Command("git", "status", "--porcelain", "--untracked-files=no")
	raw, err := command.Output()
	return err != nil || strings.TrimSpace(string(raw)) != ""
}
