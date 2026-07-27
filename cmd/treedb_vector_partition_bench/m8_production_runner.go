package main

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	m8ProductionMultiGroupModeV1 = "production_multi_group"
	m8PeakRSSScopeV1             = "process lifetime through measured query and endpoint-loss fault boundary; includes retained top-k coordinator results; excludes post-measurement attribution"
)

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
	Variant            *m3VariantDescriptorV1                                     `json:"variant,omitempty"`
	Config             m8ProductionConfigEvidenceV1                               `json:"config"`
	BuildNanos         int64                                                      `json:"build_nanos"`
	Topology           nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 `json:"topology"`
	Rows               []m8ProductionRowV1                                        `json:"rows"`
	Failure            m8ProductionFailureEvidenceV1                              `json:"failure"`
	GateLedger         m8ProductionGateLedgerV1                                   `json:"gate_ledger"`
	Profiles           m8ProductionProfileEvidenceV1                              `json:"profiles"`
	RouterSessions     m8ProductionRouterSessionEvidenceV1                        `json:"router_sessions"`
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
	RouterCandidates  int       `json:"approximate_router_candidate_budget"`
	Seed              int64     `json:"seed"`
}

// m8ProductionAttributionV1 keeps each lossy boundary visible. Recall is
// always measured against the same canonical global FP32-score oracle.
type m8ProductionAttributionV1 struct {
	Contract                                   string   `json:"contract"`
	GlobalExactRecallAtK                       float64  `json:"global_exact_recall_at_k"`
	ExhaustivePartitionRecallAtK               float64  `json:"exhaustive_partition_union_recall_at_k"`
	ExhaustivePartitionIDParity                bool     `json:"exhaustive_partition_union_id_parity"`
	ExhaustivePartitionScoreParity             bool     `json:"exhaustive_partition_union_score_parity"`
	ExactRepresentativeRecallAtK               float64  `json:"exact_representative_routing_recall_at_k"`
	ApproximateRepresentativeRecallAtK         float64  `json:"approximate_representative_routing_recall_at_k"`
	LocalHNSWRecallAtK                         float64  `json:"partition_local_hnsw_recall_at_k"`
	EndToEndRecallAtK                          float64  `json:"end_to_end_recall_at_k"`
	CoordinatorMergeIDParity                   bool     `json:"coordinator_merge_id_parity"`
	CoordinatorMergeScoreParity                bool     `json:"coordinator_merge_score_parity"`
	ApproximateRouterCandidateBudget           int      `json:"approximate_router_candidate_budget"`
	ApproximateRouterPartitionCoverageComplete bool     `json:"approximate_router_partition_coverage_complete"`
	ResidualLossOwners                         []string `json:"residual_loss_owners"`
}

type m8ProductionRowV1 struct {
	VariantID              string                    `json:"variant_id,omitempty"`
	Status                 string                    `json:"status"`
	UnsupportedReason      string                    `json:"unsupported_reason,omitempty"`
	Overlap                float64                   `json:"overlap"`
	Probes                 int                       `json:"probes,omitempty"`
	EfSearch               int                       `json:"ef_search,omitempty"`
	Concurrency            int                       `json:"concurrency,omitempty"`
	RouterMode             string                    `json:"router_mode,omitempty"`
	RouterCandidates       int                       `json:"router_candidate_budget,omitempty"`
	Samples                int                       `json:"samples,omitempty"`
	RecallAtK              float64                   `json:"recall_at_k"`
	QPS                    float64                   `json:"qps,omitempty"`
	P50Nanos               uint64                    `json:"p50_nanos,omitempty"`
	P95Nanos               uint64                    `json:"p95_nanos,omitempty"`
	P99Nanos               uint64                    `json:"p99_nanos,omitempty"`
	RequestBytes           uint64                    `json:"request_bytes,omitempty"`
	ResponseBytes          uint64                    `json:"response_bytes,omitempty"`
	CandidateBytes         uint64                    `json:"candidate_bytes,omitempty"`
	RPCs                   uint64                    `json:"rpcs,omitempty"`
	MaxRequests            uint64                    `json:"max_requests_per_query,omitempty"`
	MaxRPCs                uint64                    `json:"max_rpcs_per_query,omitempty"`
	MaxRetries             uint64                    `json:"max_retries_per_query,omitempty"`
	MaxRedirects           uint64                    `json:"max_redirects_per_query,omitempty"`
	MaxRequestBytes        uint64                    `json:"max_request_bytes_per_query,omitempty"`
	MaxResponseBytes       uint64                    `json:"max_response_bytes_per_query,omitempty"`
	MaxCandidateBytes      uint64                    `json:"max_candidate_bytes_per_query,omitempty"`
	MaxMergeEntries        uint64                    `json:"max_merge_entries_per_query,omitempty"`
	MaxShardRequestBytes   uint64                    `json:"max_shard_request_bytes,omitempty"`
	MaxShardResponseBytes  uint64                    `json:"max_shard_response_bytes,omitempty"`
	MaxShardCandidateBytes uint64                    `json:"max_shard_candidate_bytes,omitempty"`
	ExactParityChecked     bool                      `json:"exact_all_partition_parity_checked"`
	ExactParityPassed      bool                      `json:"exact_all_partition_parity_passed"`
	NoPartialResults       bool                      `json:"no_partial_results"`
	Attribution            m8ProductionAttributionV1 `json:"recall_attribution"`
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

// m8ProductionRouterSessionEvidenceV1 makes the router-manifest cold/warm
// boundary auditable without pretending that one run is a base/head comparison.
type m8ProductionRouterSessionEvidenceV1 struct {
	BeforeWarmup  []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1 `json:"before_warmup"`
	AfterWarmup   []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1 `json:"after_warmup"`
	AfterMeasured []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1 `json:"after_measured"`
}

type m8ProductionHostEvidenceV1 struct {
	CPUModel      string `json:"cpu_model"`
	MemoryBytes   uint64 `json:"memory_bytes,omitempty"`
	NUMANodes     string `json:"numa_nodes,omitempty"`
	Kernel        string `json:"kernel,omitempty"`
	ArtifactMount string `json:"artifact_mount,omitempty"`
	DatasetMount  string `json:"dataset_mount,omitempty"`
	AssetMount    string `json:"persistent_asset_mount,omitempty"`
}

type m8ProductionResourceEvidenceV1 struct {
	PersistentAssetBytes uint64                                  `json:"persistent_asset_bytes"`
	PersistentAssetCap   uint64                                  `json:"persistent_asset_cap_bytes"`
	PeakRSSBytes         int64                                   `json:"peak_rss_bytes,omitempty"`
	PeakRSSCapBytes      uint64                                  `json:"peak_rss_cap_bytes"`
	PeakRSSMeasured      bool                                    `json:"peak_rss_measured"`
	PeakRSSScope         string                                  `json:"peak_rss_scope,omitempty"`
	OverlapMemberships   int                                     `json:"overlap_memberships"`
	MaxPartitionLoad     uint64                                  `json:"max_partition_load"`
	BalanceHardCap       uint64                                  `json:"balance_hard_cap"`
	MmapStatus           string                                  `json:"mmap_status"`
	LimitComparisons     []m8ProductionResourceLimitComparisonV1 `json:"limit_comparisons"`
}

type m8ProductionResourceLimitComparisonV1 struct {
	Name       string `json:"name"`
	Configured uint64 `json:"configured"`
	Observed   uint64 `json:"observed"`
	Unit       string `json:"unit"`
	Enforced   bool   `json:"enforced"`
	Passed     bool   `json:"passed"`
}

type m8MeasuredCellV1 struct {
	rowIndex         int
	probes, efSearch int
	results          [][]m8CanonicalResultV1
}

type m8ProductionResourceObservedMaximaV1 struct {
	Requests, RPCs, Retries, Redirects                         uint64
	RequestBytes, CandidateBytes, ResponseBytes                uint64
	MergeEntries                                               uint64
	ShardRequestBytes, ShardCandidateBytes, ShardResponseBytes uint64
}

func runM8ProductionSingleVariantV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) (runErr error) {
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
	if assets.descriptor != nil && (len(cfg.overlaps) != 1 || cfg.overlaps[0] != assets.descriptor.OverlapRatio) {
		return fmt.Errorf("M8 configured overlap does not match retained variant %s", assets.descriptor.VariantID)
	}
	var persistentAssetBytes uint64
	for _, asset := range assets.manifest.Assets {
		if asset.Bytes > ^uint64(0)-persistentAssetBytes {
			return errors.New("M8 persistent asset byte accounting overflow")
		}
		persistentAssetBytes += asset.Bytes
	}
	if assets.manifest.RouterAsset.Bytes > ^uint64(0)-persistentAssetBytes {
		return errors.New("M8 persistent router byte accounting overflow")
	}
	persistentAssetBytes += assets.manifest.RouterAsset.Bytes
	if persistentAssetBytes > cfg.m8MaxAssetBytes {
		return fmt.Errorf("M8 persistent assets=%d exceed configured cap=%d", persistentAssetBytes, cfg.m8MaxAssetBytes)
	}
	topologyCtx, cancelTopology := context.WithTimeout(context.Background(), 2*time.Minute)
	topology, err := nativewire.NewVectorPartitionM8ProductionMultiGroupV1(topologyCtx, nativewire.VectorPartitionM8ProductionMultiGroupOptionsV1{
		Collection: assets.collection, Manifest: assets.manifest, RouterSource: assets.RouterSource(),
		GroupAssetSetDigests: assets.assetSetDigests, Database: "default", Catalog: "default",
		CoordinatorLimits: cfg.m8CoordinatorLimits, ShardLimits: cfg.m8ShardLimits,
	})
	cancelTopology()
	if err != nil {
		return fmt.Errorf("build M8 production topology: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, topology.Close()) }()
	buildNanos := time.Since(started).Nanoseconds()

	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, queries, cfg.topK)
	if err != nil {
		return err
	}
	report := m8ProductionReportV1{
		SchemaVersion: 2, ResultKind: "m8_production_multi_group_evidence_v2", Status: "incomplete",
		Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now().UTC(),
		Command: cfg.command, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dirty: m8GitDirtyV1(),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), Host: m8ProductionHostV1(cfg, assets.dir), Dataset: fixture, Variant: assets.descriptor,
		Config:        m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: append([]int(nil), cfg.probes...), Overlap: append([]float64(nil), cfg.overlaps...), TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: append([]int(nil), cfg.concurrency...), Warmup: cfg.warmup, EfSearch: append([]int(nil), cfg.efSearch...), RouterCandidates: cfg.routerCandidates, Seed: cfg.seed},
		BuildNanos:    buildNanos,
		Profiles:      m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Status: "not_captured", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs requires the captured baseline for differential analysis"},
		TimedBoundary: "wall-clock query cells after topology, exhaustive endpoint preflight, and generation warmup; includes router, coordinator, TCP M5 serialization, Raft read-index/apply, persistent HNSW search, response merge, and caller scheduling; excludes topology construction, exact truth, preflight, warmup, post-measurement attribution, artifact encoding, and shutdown",
		Limitations: []string{
			"loopback TCP with real serialized M5 messages and real in-memory HashiCorp Raft consensus; not a multi-host deployment",
			"the checked-in 10k path materializes disjoint round-robin packs; -m8-existing-db reuses the retained graph-built M3 packs read-only",
			"multi-host qualification and external-system comparisons are explicitly outside this local gate",
		},
	}
	report.RouterSessions.BeforeWarmup = topology.Coordinator().Stats().RouterSessions
	if cfg.profiles != "" {
		if err := os.MkdirAll(cfg.profiles, 0o755); err != nil {
			return fmt.Errorf("create M8 profiles directory: %w", err)
		}
	}
	if err := m8WarmProductionTopologyV1(context.Background(), topology.Coordinator(), assets, queries, cfg); err != nil {
		return err
	}
	report.RouterSessions.AfterWarmup = topology.Coordinator().Stats().RouterSessions
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
	measuredCells := make([]m8MeasuredCellV1, 0, len(cfg.overlaps)*len(cfg.probes)*len(cfg.efSearch)*len(cfg.concurrency))
	for _, overlap := range cfg.overlaps {
		if assets.descriptor == nil && overlap != 0 {
			report.Rows = append(report.Rows, m8ProductionRowV1{Status: "unsupported", UnsupportedReason: "nonzero overlap requires an immutable retained M3 variant descriptor", Overlap: overlap})
			continue
		}
		for _, probes := range cfg.probes {
			for _, ef := range cfg.efSearch {
				for _, concurrency := range cfg.concurrency {
					row, results, rowErr := m8RunProductionCellV1(context.Background(), topology.Coordinator(), assets, queries, truth, probes, ef, concurrency, cfg.topK)
					if rowErr != nil {
						return fmt.Errorf("M8 production cell probes=%d ef=%d concurrency=%d: %w", probes, ef, concurrency, rowErr)
					}
					row.Overlap = overlap
					if assets.descriptor != nil {
						row.VariantID = assets.descriptor.VariantID
					}
					report.Rows = append(report.Rows, row)
					measuredCells = append(measuredCells, m8MeasuredCellV1{rowIndex: len(report.Rows) - 1, probes: probes, efSearch: ef, results: results})
				}
			}
		}
	}
	report.Topology = topology.Evidence()
	report.RouterSessions.AfterMeasured = topology.Coordinator().Stats().RouterSessions
	report.Failure = m8RunUnavailableGroupV1(context.Background(), topology, assets, queries[0], cfg.topK)
	if profileCapture != nil {
		captured, stopErr := profileCapture.Stop()
		if stopErr != nil {
			return stopErr
		}
		report.Profiles = m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Captured: captured, Status: "captured_production_query_and_fault_boundary", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs.pprof is cumulative and must be compared with allocs_baseline.pprof"}
	}
	report.Resources = m8ProductionResourcesV1(cfg, fixture, assets, report.Rows, report.Topology)

	// Attribution deliberately runs after the timed query/fault boundary,
	// profile capture, and peak-RSS snapshot. Its exhaustive mmap scans must not
	// pre-fault the corpus or contaminate measured process-resource evidence.
	// The snapshot does include the bounded top-k coordinator results retained
	// from measured cells so post-measurement attribution can verify parity.
	if len(measuredCells) > 0 {
		approximateCandidates := min(cfg.routerCandidates, int(assets.status.Representatives))
		if approximateCandidates < 1 {
			return errors.New("M8 attribution requires an approximate router candidate budget")
		}
		attributionHarness, err := newM8AttributionHarnessV1(assets)
		if err != nil {
			return fmt.Errorf("open M8 attribution harness: %w", err)
		}
		attributionHarnessClosed := false
		defer func() {
			if !attributionHarnessClosed {
				runErr = errors.Join(runErr, attributionHarness.Close())
			}
		}()
		attribution := make(map[string]m8AttributionCellV1, len(cfg.probes)*len(cfg.efSearch))
		exhaustive := make([][]m8CanonicalResultV1, len(queries))
		for _, probes := range cfg.probes {
			for _, efSearch := range cfg.efSearch {
				cell, buildErr := m8BuildAttributionV1(context.Background(), assets, queries, truth, probes, efSearch, cfg.topK, approximateCandidates, exhaustive, attributionHarness)
				if buildErr != nil {
					return fmt.Errorf("build M8 attribution probes=%d ef=%d: %w", probes, efSearch, buildErr)
				}
				attribution[m8AttributionKeyV1(probes, efSearch)] = cell
			}
		}
		closeErr := attributionHarness.Close()
		attributionHarnessClosed = true
		if closeErr != nil {
			return fmt.Errorf("close M8 attribution harness: %w", closeErr)
		}
		for _, measured := range measuredCells {
			cell, ok := attribution[m8AttributionKeyV1(measured.probes, measured.efSearch)]
			if !ok {
				return fmt.Errorf("missing M8 attribution probes=%d ef=%d", measured.probes, measured.efSearch)
			}
			if err := m8AttachAttributionV1(&report.Rows[measured.rowIndex], cell, measured.results); err != nil {
				return fmt.Errorf("attach M8 attribution probes=%d ef=%d: %w", measured.probes, measured.efSearch, err)
			}
		}
	}
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
	name, err := m8ArtifactNameV1(cfg, fixture, assets.manifest)
	if err != nil {
		return err
	}
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

type m8ArtifactAssetIdentityV1 struct {
	IntegrityDigest  string
	ReadySetDigest   string
	Generation       uint64
	RouterGeneration uint64
}

func m8ArtifactNameV1(cfg config, fixture fixtureManifest, manifest collections.VectorPartitionManifestV1) (string, error) {
	identity, err := json.Marshal(struct {
		Fixture fixtureManifest
		Config  m8ProductionConfigEvidenceV1
		Assets  m8ArtifactAssetIdentityV1
	}{
		Fixture: fixture,
		Config:  m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: cfg.probes, Overlap: cfg.overlaps, TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: cfg.concurrency, Warmup: cfg.warmup, EfSearch: cfg.efSearch, RouterCandidates: cfg.routerCandidates, Seed: cfg.seed},
		Assets: m8ArtifactAssetIdentityV1{
			IntegrityDigest:  manifest.IntegrityDigest,
			ReadySetDigest:   manifest.ReadySetDigest,
			Generation:       manifest.Generation,
			RouterGeneration: manifest.RouterGeneration,
		},
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(identity)
	return fmt.Sprintf("vector_partition_m8_%s_%x.json", cfg.headSHA[:provenanceSuffixBytes], digest[:6]), nil
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

type m8CanonicalResultV1 struct {
	ID    string
	Score float32
}

type m8CanonicalRefResultV1 struct {
	ID    []byte
	Score float32
}

type m8CanonicalRefHeapEntryV1 struct {
	result m8CanonicalRefResultV1
	hash   uint64
	index  int
	next   *m8CanonicalRefHeapEntryV1
}

type m8CanonicalRefHeapV1 []*m8CanonicalRefHeapEntryV1

func (h m8CanonicalRefHeapV1) Len() int { return len(h) }
func (h m8CanonicalRefHeapV1) Less(i, j int) bool {
	return m8CanonicalRefBetterV1(h[j].result, h[i].result)
}
func (h m8CanonicalRefHeapV1) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *m8CanonicalRefHeapV1) Push(value any) {
	entry := value.(*m8CanonicalRefHeapEntryV1)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *m8CanonicalRefHeapV1) Pop() any {
	old := *h
	entry := old[len(old)-1]
	old[len(old)-1] = nil
	entry.index = -1
	*h = old[:len(old)-1]
	return entry
}

type m8CanonicalRefTopKV1 struct {
	topK          int
	heap          m8CanonicalRefHeapV1
	byHash        map[uint64]*m8CanonicalRefHeapEntryV1
	idComparisons uint64
}

const m8CanonicalResultContractV1 = collections.VectorPartitionCanonicalScoreContractV1

func m8CanonicalResultsV1(in []m8CanonicalResultV1, topK int) []m8CanonicalResultV1 {
	if topK <= 0 {
		return nil
	}
	for _, result := range in {
		if result.ID == "" || math.IsNaN(float64(result.Score)) || math.IsInf(float64(result.Score), 0) {
			return nil
		}
	}
	ordered := append([]m8CanonicalResultV1(nil), in...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Score > ordered[j].Score || ordered[i].Score == ordered[j].Score && ordered[i].ID < ordered[j].ID
	})
	seen := make(map[string]struct{}, min(topK, len(ordered)))
	out := make([]m8CanonicalResultV1, 0, min(topK, len(ordered)))
	for _, result := range ordered {
		if _, ok := seen[result.ID]; ok {
			continue
		}
		seen[result.ID] = struct{}{}
		out = append(out, result)
		if len(out) == topK {
			break
		}
	}
	return out
}

func m8AppendBoundedCanonicalV1(results []m8CanonicalResultV1, candidate m8CanonicalResultV1, topK int) []m8CanonicalResultV1 {
	if topK <= 0 || candidate.ID == "" || math.IsNaN(float64(candidate.Score)) || math.IsInf(float64(candidate.Score), 0) {
		return nil
	}
	for i := range results {
		if results[i].ID == candidate.ID {
			if candidate.Score > results[i].Score {
				results[i] = candidate
			}
			return m8CanonicalResultsV1(results, topK)
		}
	}
	if len(results) == topK {
		worst := results[len(results)-1]
		if candidate.Score < worst.Score || candidate.Score == worst.Score && candidate.ID >= worst.ID {
			return results
		}
		results[len(results)-1] = candidate
		return m8CanonicalResultsV1(results, topK)
	}
	return m8CanonicalResultsV1(append(results, candidate), topK)
}

func m8CanonicalRefResultsV1(results []m8CanonicalRefResultV1, topK int) []m8CanonicalRefResultV1 {
	slices.SortFunc(results, func(a, b m8CanonicalRefResultV1) int {
		if a.Score > b.Score {
			return -1
		}
		if a.Score < b.Score {
			return 1
		}
		return bytes.Compare(a.ID, b.ID)
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

func m8CanonicalRefBetterV1(a, b m8CanonicalRefResultV1) bool {
	return a.Score > b.Score || a.Score == b.Score && bytes.Compare(a.ID, b.ID) < 0
}

func m8CanonicalRefIDHashV1(id []byte) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for _, value := range id {
		hash ^= uint64(value)
		hash *= prime
	}
	return hash
}

func newM8CanonicalRefTopKV1(topK int) *m8CanonicalRefTopKV1 {
	return &m8CanonicalRefTopKV1{topK: topK, heap: make(m8CanonicalRefHeapV1, 0, max(0, topK)), byHash: make(map[uint64]*m8CanonicalRefHeapEntryV1, max(0, topK))}
}

func (top *m8CanonicalRefTopKV1) add(candidate m8CanonicalRefResultV1) bool {
	if top == nil || top.topK <= 0 || len(candidate.ID) == 0 || math.IsNaN(float64(candidate.Score)) || math.IsInf(float64(candidate.Score), 0) {
		return false
	}
	hash := m8CanonicalRefIDHashV1(candidate.ID)
	for entry := top.byHash[hash]; entry != nil; entry = entry.next {
		top.idComparisons++
		if bytes.Equal(entry.result.ID, candidate.ID) {
			if candidate.Score > entry.result.Score {
				entry.result.Score = candidate.Score
				heap.Fix(&top.heap, entry.index)
			}
			return true
		}
	}
	if len(top.heap) < top.topK {
		entry := &m8CanonicalRefHeapEntryV1{result: candidate, hash: hash, index: -1, next: top.byHash[hash]}
		top.byHash[hash] = entry
		heap.Push(&top.heap, entry)
		return true
	}
	worst := top.heap[0]
	if !m8CanonicalRefBetterV1(candidate, worst.result) {
		return true
	}
	chain := top.byHash[worst.hash]
	if chain == worst {
		if worst.next == nil {
			delete(top.byHash, worst.hash)
		} else {
			top.byHash[worst.hash] = worst.next
		}
	} else {
		for chain != nil && chain.next != worst {
			chain = chain.next
		}
		if chain == nil {
			return false
		}
		chain.next = worst.next
	}
	worst.result, worst.hash, worst.next = candidate, hash, top.byHash[hash]
	top.byHash[hash] = worst
	heap.Fix(&top.heap, worst.index)
	return true
}

func (top *m8CanonicalRefTopKV1) results() []m8CanonicalRefResultV1 {
	if top == nil || top.topK <= 0 {
		return nil
	}
	results := make([]m8CanonicalRefResultV1, len(top.heap))
	for i := range top.heap {
		results[i] = top.heap[i].result
	}
	return m8CanonicalRefResultsV1(results, top.topK)
}

func m8MaterializeCanonicalRefsV1(refs []m8CanonicalRefResultV1) []m8CanonicalResultV1 {
	out := make([]m8CanonicalResultV1, len(refs))
	for i := range refs {
		out[i] = m8CanonicalResultV1{ID: string(refs[i].ID), Score: refs[i].Score}
	}
	return out
}

func m8ExactTruthV1(collection *collections.Collection, manifest collections.VectorPartitionManifestV1, queries [][]float64, topK int) ([][]m8CanonicalResultV1, error) {
	if topK < 1 {
		return nil, errors.New("M8 canonical source oracle requires positive top_k")
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, fmt.Errorf("M8 canonical source oracle: %w", err)
	}
	if source.Generation != manifest.SourceGeneration || source.Checksum != manifest.SourceChecksum || source.SchemaHash != manifest.SourceSchemaHash || source.RowCount != manifest.SourceRowCount || len(rows) != int(source.RowCount) {
		return nil, errors.New("M8 canonical source oracle identity does not match the pinned partition generation")
	}
	truth := make([][]m8CanonicalResultV1, len(queries))
	for i, query64 := range queries {
		query := m8Query32V1(query64)
		scorer, scoreErr := collections.NewCanonicalVectorPartitionCosineScorerV1(query)
		if scoreErr != nil {
			return nil, fmt.Errorf("M8 canonical source oracle query=%d: %w", i, scoreErr)
		}
		refs := newM8CanonicalRefTopKV1(min(topK, len(rows)))
		for ordinal, row := range rows {
			score, scoreErr := scorer.ScoreV1(row.Values)
			if scoreErr != nil {
				return nil, fmt.Errorf("M8 canonical source oracle query=%d ordinal=%d: %w", i, ordinal, scoreErr)
			}
			if !refs.add(m8CanonicalRefResultV1{ID: row.DocumentID, Score: score}) {
				return nil, fmt.Errorf("M8 canonical source oracle query=%d ordinal=%d: retain canonical result", i, ordinal)
			}
		}
		truth[i] = m8MaterializeCanonicalRefsV1(refs.results())
		if len(truth[i]) != min(topK, len(rows)) {
			return nil, fmt.Errorf("M8 canonical source oracle query=%d results=%d", i, len(truth[i]))
		}
	}
	return truth, nil
}

func m8CanonicalIDsV1(results []m8CanonicalResultV1) []string {
	ids := make([]string, len(results))
	for i := range results {
		ids[i] = results[i].ID
	}
	return ids
}

func m8CanonicalParityV1(want, got []m8CanonicalResultV1) (idParity, scoreParity bool) {
	if len(want) != len(got) {
		return false, false
	}
	idParity, scoreParity = true, true
	for i := range want {
		if want[i].ID != got[i].ID {
			idParity = false
		}
		if math.Float32bits(want[i].Score) != math.Float32bits(got[i].Score) {
			scoreParity = false
		}
	}
	return idParity, scoreParity
}

func m8CanonicalRecallV1(want, got []m8CanonicalResultV1) float64 {
	return m8IDRecallV1(m8CanonicalIDsV1(want), m8CanonicalIDsV1(got))
}

type m8AttributionHarnessV1 struct {
	assets    *m8ProductionMultiGroupAssetsV1
	searchers []*collections.VectorPartitionLocalSearcherV1
}

func newM8AttributionHarnessV1(assets *m8ProductionMultiGroupAssetsV1) (_ *m8AttributionHarnessV1, resultErr error) {
	if assets == nil || assets.collection == nil || assets.router == nil || assets.manifest.PartitionCount == 0 || len(assets.manifest.Placements) != int(assets.manifest.PartitionCount) {
		return nil, errors.New("incomplete M8 attribution assets")
	}
	seen := make([]bool, assets.manifest.PartitionCount)
	for _, placement := range assets.manifest.Placements {
		if placement.PartitionID >= assets.manifest.PartitionCount || seen[placement.PartitionID] {
			return nil, errors.New("incomplete or duplicate M8 attribution placement")
		}
		seen[placement.PartitionID] = true
	}
	h := &m8AttributionHarnessV1{assets: assets, searchers: make([]*collections.VectorPartitionLocalSearcherV1, assets.manifest.PartitionCount)}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, h.Close())
		}
	}()
	for partition := range h.searchers {
		searcher, err := assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(partition))
		if err != nil {
			return nil, fmt.Errorf("open M8 attribution partition %d: %w", partition, err)
		}
		h.searchers[partition] = searcher
	}
	return h, nil
}

func (h *m8AttributionHarnessV1) Close() error {
	if h == nil {
		return nil
	}
	var err error
	for i, searcher := range h.searchers {
		if searcher != nil {
			err = errors.Join(err, searcher.Close())
			h.searchers[i] = nil
		}
	}
	return err
}

func (h *m8AttributionHarnessV1) route(ctx context.Context, query []float32, probes int, mode string, candidates int) ([]uint32, error) {
	result, err := h.assets.router.SearchWithContextV1(ctx, query, collections.VectorPartitionRouterSearchOptionsV1{Mode: mode, CandidateBudget: candidates, PartitionProbes: probes})
	if err != nil {
		return nil, err
	}
	partitions := make([]uint32, len(result.Partitions))
	seen := make(map[uint32]struct{}, len(result.Partitions))
	for i, partition := range result.Partitions {
		if partition.PartitionID >= uint32(len(h.searchers)) {
			return nil, errors.New("M8 attribution router returned out-of-range partition")
		}
		if _, ok := seen[partition.PartitionID]; ok {
			return nil, errors.New("M8 attribution router returned duplicate partition")
		}
		seen[partition.PartitionID] = struct{}{}
		partitions[i] = partition.PartitionID
	}
	if len(partitions) != probes {
		return nil, fmt.Errorf("M8 attribution router selected %d partitions, want %d", len(partitions), probes)
	}
	return partitions, nil
}

// m8ApproximateRouterCoverageV1 converts only the typed bounded-candidate
// shortfall into an attributable approximate-routing loss. Every other router
// error remains fail-closed evidence construction failure.
func m8ApproximateRouterCoverageV1(err error) (bool, error) {
	if err == nil {
		return true, nil
	}
	if errors.Is(err, collections.ErrVectorPartitionRouterCandidateCoverageV1) {
		return false, nil
	}
	return false, err
}

func (h *m8AttributionHarnessV1) search(ctx context.Context, query []float32, partitions []uint32, topK, efSearch int, exact bool) ([]m8CanonicalResultV1, error) {
	merged := make([]m8CanonicalResultV1, 0, len(partitions)*topK)
	for _, partition := range partitions {
		if partition >= uint32(len(h.searchers)) || h.searchers[partition] == nil {
			return nil, errors.New("M8 attribution partition coverage is incomplete")
		}
		opts := collections.VectorPartitionSearchOptionsV1{TopK: topK, EfSearch: efSearch}
		var results []collections.VectorPartitionSearchResultV1
		var err error
		if exact {
			results, _, err = h.searchers[partition].SearchExactWithOptionsV1(ctx, query, opts)
		} else {
			results, _, err = h.searchers[partition].SearchWithOptionsV1(ctx, query, opts)
		}
		if err != nil {
			return nil, fmt.Errorf("M8 attribution partition %d: %w", partition, err)
		}
		for _, result := range results {
			merged = append(merged, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	return m8CanonicalResultsV1(merged, topK), nil
}

type m8AttributionCellV1 struct {
	Evidence m8ProductionAttributionV1
	Local    [][]m8CanonicalResultV1
}

func m8AttributionKeyV1(probes, efSearch int) string {
	return fmt.Sprintf("%d/%d", probes, efSearch)
}

func m8BuildAttributionV1(ctx context.Context, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, truth [][]m8CanonicalResultV1, probes, efSearch, topK, approximateCandidates int, exhaustive [][]m8CanonicalResultV1, harness *m8AttributionHarnessV1) (m8AttributionCellV1, error) {
	cell := m8AttributionCellV1{Evidence: m8ProductionAttributionV1{
		Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1,
		ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
		CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
		ApproximateRouterCandidateBudget:           approximateCandidates,
		ApproximateRouterPartitionCoverageComplete: true,
	}, Local: make([][]m8CanonicalResultV1, len(queries))}
	allPartitions := make([]uint32, len(harness.searchers))
	for i := range allPartitions {
		allPartitions[i] = uint32(i)
	}
	var exhaustiveRecall, exactRecall, approximateRecall, localRecall float64
	for i, query64 := range queries {
		if err := ctx.Err(); err != nil {
			return cell, err
		}
		query := m8Query32V1(query64)
		if exhaustive[i] == nil {
			var err error
			exhaustive[i], err = harness.search(ctx, query, allPartitions, topK, efSearch, true)
			if err != nil {
				return cell, err
			}
		}
		idParity, scoreParity := m8CanonicalParityV1(truth[i], exhaustive[i])
		cell.Evidence.ExhaustivePartitionIDParity = cell.Evidence.ExhaustivePartitionIDParity && idParity
		cell.Evidence.ExhaustivePartitionScoreParity = cell.Evidence.ExhaustivePartitionScoreParity && scoreParity
		exhaustiveRecall += m8CanonicalRecallV1(truth[i], exhaustive[i])

		exactPartitions, err := harness.route(ctx, query, probes, collections.VectorPartitionRouterModeExactV1, int(assets.status.Representatives))
		if err != nil {
			return cell, err
		}
		approximatePartitions, approximateRouteErr := harness.route(ctx, query, probes, collections.VectorPartitionRouterModeApproxV1, approximateCandidates)
		approximateCoverageComplete, err := m8ApproximateRouterCoverageV1(approximateRouteErr)
		if err != nil {
			return cell, err
		}
		cell.Evidence.ApproximateRouterPartitionCoverageComplete = cell.Evidence.ApproximateRouterPartitionCoverageComplete && approximateCoverageComplete
		exactResults, err := harness.search(ctx, query, exactPartitions, topK, efSearch, true)
		if err != nil {
			return cell, err
		}
		var approximateResults []m8CanonicalResultV1
		if approximateCoverageComplete {
			approximateResults, err = harness.search(ctx, query, approximatePartitions, topK, efSearch, true)
			if err != nil {
				return cell, err
			}
		}
		cell.Local[i], err = harness.search(ctx, query, exactPartitions, topK, efSearch, false)
		if err != nil {
			return cell, err
		}
		exactRecall += m8CanonicalRecallV1(truth[i], exactResults)
		approximateRecall += m8CanonicalRecallV1(truth[i], approximateResults)
		localRecall += m8CanonicalRecallV1(truth[i], cell.Local[i])
	}
	n := float64(len(queries))
	if !cell.Evidence.ApproximateRouterPartitionCoverageComplete {
		// A shortfall means this approximate attribution cell did not evaluate
		// its requested partition coverage. Do not average successful partial
		// queries into a deceptively nonzero approximate-routing recall.
		approximateRecall = 0
	}
	cell.Evidence.ExhaustivePartitionRecallAtK = exhaustiveRecall / n
	cell.Evidence.ExactRepresentativeRecallAtK = exactRecall / n
	cell.Evidence.ApproximateRepresentativeRecallAtK = approximateRecall / n
	cell.Evidence.LocalHNSWRecallAtK = localRecall / n
	return cell, nil
}

// m8ExactPartitionUnionV1 scans every generation-pinned partition pack and
// fails closed unless the caller supplies the complete manifest partition set.
func m8ExactPartitionUnionV1(ctx context.Context, assets *m8ProductionMultiGroupAssetsV1, query []float64, topK int) ([]neighbor, error) {
	if assets == nil || len(assets.manifest.Placements) != int(assets.manifest.PartitionCount) {
		return nil, errors.New("incomplete M8 partition manifest")
	}
	seen := make(map[uint32]struct{}, assets.manifest.PartitionCount)
	for _, placement := range assets.manifest.Placements {
		if placement.PartitionID >= assets.manifest.PartitionCount {
			return nil, errors.New("invalid M8 partition placement")
		}
		if _, duplicate := seen[placement.PartitionID]; duplicate {
			return nil, errors.New("duplicate M8 partition placement")
		}
		seen[placement.PartitionID] = struct{}{}
	}
	merged := make([]neighbor, 0, len(assets.manifest.Placements)*topK)
	for partition := 0; partition < len(assets.manifest.Placements); partition++ {
		searcher, err := assets.collection.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, assets.manifest.Generation, uint32(partition))
		if err != nil {
			return nil, err
		}
		results, _, searchErr := searcher.SearchExactWithOptionsV1(ctx, m8Query32V1(query), collections.VectorPartitionSearchOptionsV1{TopK: topK})
		closeErr := searcher.Close()
		if searchErr != nil || closeErr != nil {
			return nil, errors.Join(searchErr, closeErr)
		}
		for _, result := range results {
			merged = append(merged, neighbor{ID: result.ID, Distance: 1 - float64(result.Score)})
		}
	}
	return canonicalExactNeighborsV1(merged, topK), nil
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

func m8ProductionResourcesV1(cfg config, fixture fixtureManifest, assets *m8ProductionMultiGroupAssetsV1, rows []m8ProductionRowV1, topology nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1) m8ProductionResourceEvidenceV1 {
	out := m8ProductionResourceEvidenceV1{PersistentAssetCap: cfg.m8MaxAssetBytes, PeakRSSCapBytes: cfg.m8MaxRSSBytes}
	if assets == nil {
		return out
	}
	for _, asset := range assets.manifest.Assets {
		out.PersistentAssetBytes += asset.Bytes
	}
	out.PersistentAssetBytes += assets.manifest.RouterAsset.Bytes
	out.OverlapMemberships = len(assets.manifest.OverlapMemberships)
	loads, loadErr := m8PartitionLoadsV1(assets.manifest)
	if loadErr != nil {
		// The serving path validates the manifest before this evidence pass. If
		// that invariant ever regresses, force the balance comparison red rather
		// than publishing a zero-load success.
		out.MaxPartitionLoad = ^uint64(0)
	} else {
		for _, load := range loads {
			out.MaxPartitionLoad = max(out.MaxPartitionLoad, load)
		}
	}
	// Integer ceiling of mean * 1.05, matching the default balance epsilon.
	sourceRows, partitions := assets.manifest.SourceRowCount, uint64(assets.manifest.PartitionCount)
	if partitions > 0 {
		out.BalanceHardCap = (sourceRows*105 + partitions*100 - 1) / (partitions * 100)
	}
	out.MmapStatus = "not_captured_by_m8_runner; retained M3/M5 artifacts own mapped-pack evidence"
	out.PeakRSSScope = m8PeakRSSScopeV1
	if peak, ok := vectorPartitionBenchmarkPeakRSS(); ok {
		out.PeakRSSBytes, out.PeakRSSMeasured = peak, true
	}
	var maxProbes, maxEf, maxP99 uint64
	for _, row := range rows {
		if row.Status == "unsupported" {
			continue
		}
		maxProbes = max(maxProbes, uint64(row.Probes))
		maxEf = max(maxEf, uint64(row.EfSearch))
		maxP99 = max(maxP99, row.P99Nanos)
	}
	observed := m8ObservedResourceMaximaV1(rows)
	add := func(name string, configured, observed uint64, unit string, enforced bool) {
		out.LimitComparisons = append(out.LimitComparisons, m8ProductionResourceLimitComparisonV1{Name: name, Configured: configured, Observed: observed, Unit: unit, Enforced: enforced, Passed: configured > 0 && observed <= configured})
	}
	add("persistent_asset_bytes", cfg.m8MaxAssetBytes, out.PersistentAssetBytes, "bytes", true)
	peak := uint64(0)
	if out.PeakRSSMeasured && out.PeakRSSBytes > 0 {
		peak = uint64(out.PeakRSSBytes)
	}
	out.LimitComparisons = append(out.LimitComparisons, m8ProductionResourceLimitComparisonV1{
		Name: "process_peak_rss", Configured: cfg.m8MaxRSSBytes, Observed: peak, Unit: "bytes", Enforced: true,
		Passed: out.PeakRSSMeasured && cfg.m8MaxRSSBytes > 0 && peak <= cfg.m8MaxRSSBytes,
	})
	add("coordinator_selected_partitions", uint64(cfg.m8CoordinatorLimits.MaxSelectedPartitions), maxProbes, "count", true)
	add("coordinator_groups", uint64(cfg.m8CoordinatorLimits.MaxGroups), uint64(cfg.raftGroups), "count", true)
	add("coordinator_requests", uint64(cfg.m8CoordinatorLimits.MaxRequests), observed.Requests, "count", true)
	configuredConcurrentRequests, _ := m8ConfiguredConcurrentShardRequestsV1(cfg.m8CoordinatorLimits.MaxConcurrentRequests, cfg.concurrency)
	add("coordinator_concurrent_requests_across_clients", configuredConcurrentRequests, topology.MaxConcurrentShardRequests, "count", true)
	add("coordinator_retries", uint64(cfg.m8CoordinatorLimits.MaxRetries), observed.Retries, "count", true)
	add("coordinator_redirects", uint64(cfg.m8CoordinatorLimits.MaxRedirects), observed.Redirects, "count", true)
	add("coordinator_router_candidates", uint64(cfg.m8CoordinatorLimits.MaxRouterCandidates), uint64(cfg.routerCandidates), "count", true)
	add("coordinator_query_bytes", uint64(cfg.m8CoordinatorLimits.MaxQueryBytes), uint64(fixture.Dimensions*4), "bytes", true)
	add("coordinator_top_k", uint64(cfg.m8CoordinatorLimits.MaxTopK), uint64(cfg.topK), "count", true)
	add("coordinator_ef_search", uint64(cfg.m8CoordinatorLimits.MaxEfSearch), maxEf, "count", true)
	add("coordinator_partitions_per_request", uint64(cfg.m8CoordinatorLimits.MaxPartitionsPerRequest), maxProbes, "count", true)
	identityBytes := uint64(len("default")*2 + len(assets.manifest.Collection) + len(assets.manifest.IndexName) + len(assets.manifest.IndexDefinitionDigest) + len(assets.manifest.ReadySetDigest))
	stableIDBytes := uint64(len(fmt.Sprintf("doc-%06d", max(0, fixture.Vectors-1))))
	add("coordinator_identity_bytes", uint64(cfg.m8CoordinatorLimits.MaxIdentityBytes), identityBytes, "bytes", true)
	add("coordinator_stable_id_bytes", uint64(cfg.m8CoordinatorLimits.MaxStableIDBytes), stableIDBytes, "bytes", true)
	add("coordinator_merge_entries", uint64(cfg.m8CoordinatorLimits.MaxMergeEntries), observed.MergeEntries, "count", true)
	add("coordinator_request_bytes", cfg.m8CoordinatorLimits.MaxRequestBytes, observed.RequestBytes, "bytes", true)
	add("coordinator_candidate_bytes", cfg.m8CoordinatorLimits.MaxCandidateBytes, observed.CandidateBytes, "bytes", true)
	add("coordinator_response_bytes", cfg.m8CoordinatorLimits.MaxResponseBytes, observed.ResponseBytes, "bytes", true)
	add("coordinator_wall_clock", uint64(cfg.m8CoordinatorLimits.MaxWallClock), maxP99, "nanoseconds", true)
	add("shard_dimensions", uint64(cfg.m8ShardLimits.MaxDimensions), uint64(fixture.Dimensions), "count", true)
	add("shard_query_bytes", uint64(cfg.m8ShardLimits.MaxQueryBytes), uint64(fixture.Dimensions*4), "bytes", true)
	add("shard_partitions", uint64(cfg.m8ShardLimits.MaxPartitions), maxProbes, "count", true)
	add("shard_top_k", uint64(cfg.m8ShardLimits.MaxTopK), uint64(cfg.topK), "count", true)
	add("shard_ef_search", uint64(cfg.m8ShardLimits.MaxEfSearch), maxEf, "count", true)
	add("shard_identity_bytes", uint64(cfg.m8ShardLimits.MaxIdentityBytes), identityBytes, "bytes", true)
	add("shard_stable_id_bytes", uint64(cfg.m8ShardLimits.MaxStableIDBytes), stableIDBytes, "bytes", true)
	add("shard_request_bytes", cfg.m8ShardLimits.MaxRequestBytes, observed.ShardRequestBytes, "bytes", true)
	add("shard_candidate_bytes", cfg.m8ShardLimits.MaxCandidateBytes, observed.ShardCandidateBytes, "bytes", true)
	add("shard_response_bytes", cfg.m8ShardLimits.MaxResponseBytes, observed.ShardResponseBytes, "bytes", true)
	return out
}

func m8ObservedResourceMaximaV1(rows []m8ProductionRowV1) m8ProductionResourceObservedMaximaV1 {
	var out m8ProductionResourceObservedMaximaV1
	for _, row := range rows {
		if row.Status == "unsupported" {
			continue
		}
		out.Requests = max(out.Requests, row.MaxRequests)
		out.RPCs = max(out.RPCs, row.MaxRPCs)
		out.Retries = max(out.Retries, row.MaxRetries)
		out.Redirects = max(out.Redirects, row.MaxRedirects)
		out.RequestBytes = max(out.RequestBytes, row.MaxRequestBytes)
		out.CandidateBytes = max(out.CandidateBytes, row.MaxCandidateBytes)
		out.ResponseBytes = max(out.ResponseBytes, row.MaxResponseBytes)
		out.MergeEntries = max(out.MergeEntries, row.MaxMergeEntries)
		out.ShardRequestBytes = max(out.ShardRequestBytes, row.MaxShardRequestBytes)
		out.ShardCandidateBytes = max(out.ShardCandidateBytes, row.MaxShardCandidateBytes)
		out.ShardResponseBytes = max(out.ShardResponseBytes, row.MaxShardResponseBytes)
	}
	return out
}

func m8PartitionLoadsV1(manifest collections.VectorPartitionManifestV1) ([]uint64, error) {
	if manifest.PartitionCount == 0 {
		return nil, errors.New("M8 partition loads require a positive partition count")
	}
	loads := make([]uint64, manifest.PartitionCount)
	add := func(memberships []collections.VectorPartitionMembershipV1) error {
		for _, membership := range memberships {
			if membership.PartitionID >= manifest.PartitionCount {
				return errors.New("M8 membership partition is out of range")
			}
			loads[membership.PartitionID]++
		}
		return nil
	}
	if err := add(manifest.Memberships); err != nil {
		return nil, err
	}
	if err := add(manifest.OverlapMemberships); err != nil {
		return nil, err
	}
	return loads, nil
}

func m8ConfiguredConcurrentShardRequestsV1(perRequest int, clientConcurrency []int) (uint64, error) {
	if perRequest < 1 || len(clientConcurrency) == 0 {
		return 0, errors.New("M8 concurrent shard-request bound requires positive per-request and client concurrency")
	}
	maxClients := 0
	for _, concurrency := range clientConcurrency {
		if concurrency < 1 {
			return 0, errors.New("M8 client concurrency must be positive")
		}
		maxClients = max(maxClients, concurrency)
	}
	configured, err := memoryMul(int64(perRequest), int64(maxClients))
	if err != nil {
		return 0, err
	}
	return uint64(configured), nil
}

type m8ProductionCellOutcomeV1 struct {
	response nativewire.VectorPartitionCoordinatorResponseV1
	err      error
}

func m8AccumulateProductionRowCountersV1(row *m8ProductionRowV1, counters nativewire.VectorPartitionCoordinatorCountersV1) {
	if row == nil {
		return
	}
	row.RequestBytes += counters.RequestBytes
	row.ResponseBytes += counters.ResponseBytes
	row.CandidateBytes += counters.CandidateBytes
	row.RPCs += counters.RPCs
	row.MaxRequests = max(row.MaxRequests, counters.Requests)
	row.MaxRPCs = max(row.MaxRPCs, counters.RPCs)
	row.MaxRetries = max(row.MaxRetries, counters.Retries)
	row.MaxRedirects = max(row.MaxRedirects, counters.Redirects)
	row.MaxRequestBytes = max(row.MaxRequestBytes, counters.RequestBytes)
	row.MaxResponseBytes = max(row.MaxResponseBytes, counters.ResponseBytes)
	row.MaxCandidateBytes = max(row.MaxCandidateBytes, counters.CandidateBytes)
	row.MaxMergeEntries = max(row.MaxMergeEntries, counters.MergeEntries)
	row.MaxShardRequestBytes = max(row.MaxShardRequestBytes, counters.MaxShardRequestBytes)
	row.MaxShardResponseBytes = max(row.MaxShardResponseBytes, counters.MaxShardResponseBytes)
	row.MaxShardCandidateBytes = max(row.MaxShardCandidateBytes, counters.MaxShardCandidateBytes)
}

func m8RunProductionCellV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, truth [][]m8CanonicalResultV1, probes, efSearch, concurrency, topK int) (m8ProductionRowV1, [][]m8CanonicalResultV1, error) {
	outcomes := make([]m8ProductionCellOutcomeV1, len(queries))
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
	row := m8ProductionRowV1{Status: "pass", Probes: probes, EfSearch: efSearch, Concurrency: concurrency, RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidates: max(1, int(assets.status.Representatives)), Samples: len(queries), ExactParityChecked: probes == len(assets.manifest.Placements), ExactParityPassed: probes == len(assets.manifest.Placements)}
	canonicalResults := make([][]m8CanonicalResultV1, len(outcomes))
	durations := make([]uint64, 0, len(outcomes))
	var recallSum float64
	for index, outcome := range outcomes {
		if outcome.err != nil {
			return row, nil, fmt.Errorf("query %d: %w", index, outcome.err)
		}
		got, shapeErr := m8ValidateCoordinatorResponseV1(outcome.response, assets.manifest, probes, topK)
		if shapeErr != nil {
			return row, nil, fmt.Errorf("query %d response shape: %w", index, shapeErr)
		}
		canonicalResults[index] = got
		recallSum += m8CanonicalRecallV1(truth[index], got)
		globalIDParity, globalScoreParity := m8CanonicalParityV1(truth[index], got)
		if row.ExactParityChecked && (!globalIDParity || !globalScoreParity) {
			row.ExactParityPassed = false
			row.Status = "fail"
		}
		durations = append(durations, outcome.response.Timing.TotalNanos)
		m8AccumulateProductionRowCountersV1(&row, outcome.response.Counters)
	}
	row.NoPartialResults = true
	row.RecallAtK = recallSum / float64(len(outcomes))
	row.QPS = float64(len(outcomes)) / elapsed.Seconds()
	row.P50Nanos, row.P95Nanos, row.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	return row, canonicalResults, nil
}

func m8AttachAttributionV1(row *m8ProductionRowV1, attribution m8AttributionCellV1, coordinatorResults [][]m8CanonicalResultV1) error {
	if row == nil || row.Samples < 1 || len(attribution.Local) != row.Samples || len(coordinatorResults) != row.Samples {
		return errors.New("M8 attribution result cardinality mismatch")
	}
	row.Attribution = attribution.Evidence
	for i := range coordinatorResults {
		idParity, scoreParity := m8CanonicalParityV1(attribution.Local[i], coordinatorResults[i])
		row.Attribution.CoordinatorMergeIDParity = row.Attribution.CoordinatorMergeIDParity && idParity
		row.Attribution.CoordinatorMergeScoreParity = row.Attribution.CoordinatorMergeScoreParity && scoreParity
	}
	row.Attribution.EndToEndRecallAtK = row.RecallAtK
	row.Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(row.Attribution)
	return nil
}

func m8ValidateCoordinatorResponseV1(response nativewire.VectorPartitionCoordinatorResponseV1, manifest collections.VectorPartitionManifestV1, probes, topK int) ([]m8CanonicalResultV1, error) {
	partitionCount := int(manifest.PartitionCount)
	if probes < 1 || partitionCount < probes || len(manifest.Placements) != partitionCount || topK < 1 || len(response.Neighbors) > topK || len(response.ProbedPartitions) != probes || len(response.ProbedGroups) == 0 || len(response.ProbedGroups) > probes {
		return nil, errors.New("truncated or dimensionally invalid coordinator response")
	}
	raw := make([]m8CanonicalResultV1, len(response.Neighbors))
	for i := range response.Neighbors {
		raw[i] = m8CanonicalResultV1{ID: response.Neighbors[i].ID, Score: response.Neighbors[i].Score}
	}
	canonical := m8CanonicalResultsV1(raw, topK)
	if len(canonical) != len(raw) {
		return nil, errors.New("coordinator response contains empty, nonfinite, or duplicate neighbors")
	}
	if idParity, scoreParity := m8CanonicalParityV1(canonical, raw); !idParity || !scoreParity {
		return nil, errors.New("coordinator response violates canonical score/stable-ID order")
	}
	seenPartitions := make(map[uint32]struct{}, probes)
	owners := make(map[uint32]string, len(manifest.Placements))
	for _, placement := range manifest.Placements {
		if placement.PartitionID >= manifest.PartitionCount || placement.GroupID == "" {
			return nil, errors.New("manifest contains invalid coordinator ownership")
		}
		if _, duplicate := owners[placement.PartitionID]; duplicate {
			return nil, errors.New("manifest contains duplicate coordinator ownership")
		}
		owners[placement.PartitionID] = placement.GroupID
	}
	expectedGroups := make(map[string]struct{}, probes)
	for _, partition := range response.ProbedPartitions {
		if partition >= uint32(partitionCount) {
			return nil, errors.New("coordinator response contains an out-of-range partition")
		}
		if _, duplicate := seenPartitions[partition]; duplicate {
			return nil, errors.New("coordinator response contains a duplicate partition")
		}
		seenPartitions[partition] = struct{}{}
		expectedGroups[owners[partition]] = struct{}{}
	}
	if len(expectedGroups) != len(response.ProbedGroups) {
		return nil, errors.New("coordinator response group coverage is incomplete")
	}
	seenGroups := make(map[string]struct{}, len(response.ProbedGroups))
	for _, group := range response.ProbedGroups {
		if group == "" {
			return nil, errors.New("coordinator response contains an empty group")
		}
		if _, duplicate := seenGroups[string(group)]; duplicate {
			return nil, errors.New("coordinator response contains a duplicate group")
		}
		if _, expected := expectedGroups[string(group)]; !expected {
			return nil, errors.New("coordinator response contains a non-owner group")
		}
		seenGroups[string(group)] = struct{}{}
	}
	selectedOrdinals := make(map[uint64]struct{})
	for _, memberships := range [][]collections.VectorPartitionMembershipV1{manifest.Memberships, manifest.OverlapMemberships} {
		for _, membership := range memberships {
			if membership.PartitionID >= manifest.PartitionCount || membership.VectorOrdinal >= manifest.SourceRowCount {
				return nil, errors.New("manifest contains an invalid coordinator membership")
			}
			if _, selected := seenPartitions[membership.PartitionID]; selected {
				selectedOrdinals[membership.VectorOrdinal] = struct{}{}
			}
		}
	}
	expectedNeighbors := min(topK, len(selectedOrdinals))
	if len(response.Neighbors) != expectedNeighbors {
		return nil, fmt.Errorf("truncated coordinator response: neighbors=%d want=%d", len(response.Neighbors), expectedNeighbors)
	}
	return raw, nil
}

func m8AttributionLossOwnersV1(attribution m8ProductionAttributionV1) []string {
	const epsilon = 1e-12
	owners := make([]string, 0, 5)
	if !attribution.ExhaustivePartitionIDParity || !attribution.ExhaustivePartitionScoreParity || attribution.ExhaustivePartitionRecallAtK < 1-epsilon {
		owners = append(owners, "partition_membership_or_score_contract")
	}
	if attribution.ExactRepresentativeRecallAtK+epsilon < attribution.ExhaustivePartitionRecallAtK {
		owners = append(owners, "exact_representative_routing")
	}
	if !attribution.ApproximateRouterPartitionCoverageComplete || attribution.ApproximateRepresentativeRecallAtK+epsilon < attribution.ExactRepresentativeRecallAtK {
		owners = append(owners, "approximate_representative_routing")
	}
	if attribution.LocalHNSWRecallAtK+epsilon < attribution.ExactRepresentativeRecallAtK {
		owners = append(owners, "partition_local_hnsw")
	}
	if !attribution.CoordinatorMergeIDParity || !attribution.CoordinatorMergeScoreParity || attribution.EndToEndRecallAtK+epsilon < attribution.LocalHNSWRecallAtK {
		owners = append(owners, "coordinator_merge_or_transport")
	}
	if len(owners) == 0 {
		return []string{"none_observed"}
	}
	return owners
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
			if !row.Attribution.ExhaustivePartitionIDParity || !row.Attribution.ExhaustivePartitionScoreParity || row.Attribution.ExhaustivePartitionRecallAtK != 1 {
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
	if report.Resources.PersistentAssetBytes > 0 && report.Resources.PeakRSSMeasured && len(report.Resources.LimitComparisons) > 0 {
		ledger.ResourceBounds = "pass"
		for _, comparison := range report.Resources.LimitComparisons {
			if comparison.Configured == 0 || !comparison.Passed {
				ledger.ResourceBounds = "fail"
				break
			}
		}
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
	if report.SchemaVersion != 2 || report.ResultKind != "m8_production_multi_group_evidence_v2" ||
		report.Mode != m8ProductionMultiGroupModeV1 || !report.ProductionEvidence ||
		report.GeneratedAt.IsZero() || len(report.Command) == 0 || !validSHA(report.BaseSHA) || !validSHA(report.HeadSHA) ||
		report.Config.RaftGroups < 2 || report.Config.RaftNodesPerGroup != 3 || report.Config.Partitions < 4 || report.Config.Partitions > maxPartitions ||
		report.Config.Warmup < 0 || report.Config.RouterCandidates < 1 || report.BuildNanos <= 0 || report.TimedBoundary == "" || len(report.Limitations) == 0 {
		return errors.New("missing or invalid M8 identity, topology, or timing metadata")
	}
	if err := validateM3FixtureWithCaps(report.Dataset, maxVectors, maxFixtureBytes); err != nil {
		return fmt.Errorf("dataset: %w", err)
	}
	if report.Variant != nil {
		if err := validateM3VariantDescriptorV1(*report.Variant); err != nil || len(report.Config.Overlap) != 1 ||
			report.Config.Overlap[0] != report.Variant.OverlapRatio || report.Variant.FixtureChecksum != report.Dataset.Checksum ||
			uint64(report.Variant.Partitions) != uint64(report.Config.Partitions) || report.Variant.PersistentAssetBytes != report.Resources.PersistentAssetBytes {
			return errors.New("M8 report variant identity is not bound to its configuration and resources")
		}
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
	var measuredSamples uint64
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			if row.UnsupportedReason == "" || row.Overlap == 0 {
				return errors.New("malformed unsupported M8 row")
			}
			continue
		}
		if row.Status != "pass" && row.Status != "fail" || row.Probes < 1 || row.Probes > report.Config.Partitions ||
			row.EfSearch < report.Config.TopK || row.Concurrency < 1 || row.Samples != report.Dataset.Queries || row.QPS <= 0 ||
			row.RouterMode == "" || row.RouterCandidates < 1 || row.ExactParityChecked != (row.Probes == report.Config.Partitions) ||
			(!row.ExactParityChecked && row.ExactParityPassed) || !row.NoPartialResults ||
			math.Float64bits(row.RecallAtK) != math.Float64bits(row.Attribution.EndToEndRecallAtK) ||
			!validM8AttributionV1(row.Attribution) {
			return errors.New("malformed measured M8 row")
		}
		if report.Variant != nil && row.VariantID != report.Variant.VariantID {
			return errors.New("M8 row variant identity mismatch")
		}
		rowSamples := uint64(row.Samples)
		if rowSamples > ^uint64(0)-measuredSamples {
			return errors.New("M8 measured sample count overflow")
		}
		measuredSamples += rowSamples
	}
	if !validM8RouterSessionEvidenceV1(report.RouterSessions, measuredSamples) {
		return errors.New("incomplete M8 router-session evidence")
	}
	if !report.Failure.Passed || report.Failure.Error == "" || report.Failure.ReturnedNeighbors != 0 || report.Failure.ReturnedGroups != 0 ||
		report.GateLedger.FailureHonesty != "pass" || report.Resources.PersistentAssetBytes == 0 ||
		report.Resources.PeakRSSMeasured && report.Resources.PeakRSSScope != m8PeakRSSScopeV1 {
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

func validM8AttributionV1(attribution m8ProductionAttributionV1) bool {
	if attribution.Contract != m8CanonicalResultContractV1 || attribution.GlobalExactRecallAtK != 1 ||
		attribution.ApproximateRouterCandidateBudget < 1 ||
		(!attribution.ApproximateRouterPartitionCoverageComplete && attribution.ApproximateRepresentativeRecallAtK != 0) ||
		!slices.Equal(attribution.ResidualLossOwners, m8AttributionLossOwnersV1(attribution)) {
		return false
	}
	for _, recall := range []float64{
		attribution.ExhaustivePartitionRecallAtK,
		attribution.ExactRepresentativeRecallAtK,
		attribution.ApproximateRepresentativeRecallAtK,
		attribution.LocalHNSWRecallAtK,
		attribution.EndToEndRecallAtK,
	} {
		if math.IsNaN(recall) || math.IsInf(recall, 0) || recall < 0 || recall > 1 {
			return false
		}
	}
	return true
}

func validM8RouterSessionEvidenceV1(evidence m8ProductionRouterSessionEvidenceV1, expectedMeasuredSamples uint64) bool {
	if len(evidence.BeforeWarmup) != 0 || len(evidence.AfterWarmup) == 0 || len(evidence.AfterMeasured) == 0 {
		return false
	}
	warmed := make(map[nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1, len(evidence.AfterWarmup))
	for _, session := range evidence.AfterWarmup {
		identity := session.Identity
		if identity.Database == "" || identity.Catalog == "" || identity.Collection == "" || identity.IndexName == "" || identity.IndexDefinitionDigest == "" ||
			identity.SourceGeneration == 0 || identity.SourceChecksum == 0 || identity.SourceSchemaHash == 0 || identity.SourceRowCount == 0 || identity.PartitionGeneration == 0 ||
			identity.ReadySetDigest == "" || identity.RouterModelDigest == "" ||
			session.ColdOpens != 1 || session.ManifestOpenAttempts != 1 || session.Misses != 1 || session.ReaderPins != 1 ||
			session.OpenFailures != 0 || session.Invalidations != 0 || session.Closes != 0 || session.ReaderReleases != 0 ||
			session.LeasePins == 0 || session.LeasePins != session.LeaseReleases {
			return false
		}
		if _, duplicate := warmed[identity]; duplicate {
			return false
		}
		warmed[identity] = session
	}
	if len(warmed) != len(evidence.AfterMeasured) {
		return false
	}
	seen := make(map[nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1]bool, len(evidence.AfterMeasured))
	var measuredHits, measuredLeasePins, measuredLeaseReleases uint64
	for _, measured := range evidence.AfterMeasured {
		warm, ok := warmed[measured.Identity]
		if !ok || seen[measured.Identity] ||
			measured.ColdOpens != warm.ColdOpens || measured.ManifestOpenAttempts != warm.ManifestOpenAttempts || measured.Misses != warm.Misses ||
			measured.ReaderPins != warm.ReaderPins || measured.ReaderReleases != warm.ReaderReleases || measured.OpenFailures != warm.OpenFailures ||
			measured.Invalidations != warm.Invalidations || measured.Closes != warm.Closes ||
			measured.LeasePins != measured.LeaseReleases {
			return false
		}
		if measured.Hits < warm.Hits || measured.LeasePins < warm.LeasePins || measured.LeaseReleases < warm.LeaseReleases {
			return false
		}
		hitDelta := measured.Hits - warm.Hits
		pinDelta := measured.LeasePins - warm.LeasePins
		releaseDelta := measured.LeaseReleases - warm.LeaseReleases
		if hitDelta > ^uint64(0)-measuredHits || pinDelta > ^uint64(0)-measuredLeasePins ||
			releaseDelta > ^uint64(0)-measuredLeaseReleases {
			return false
		}
		measuredHits += hitDelta
		measuredLeasePins += pinDelta
		measuredLeaseReleases += releaseDelta
		seen[measured.Identity] = true
	}
	return measuredHits == expectedMeasuredSamples && measuredLeasePins == expectedMeasuredSamples &&
		measuredLeaseReleases == expectedMeasuredSamples
}

func m8SHA256V1(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
