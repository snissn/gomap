package main

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	m8ProductionMultiGroupModeV1 = "production_multi_group"
	m8PeakRSSScopeV1             = "process lifetime through preflight, warmup, measured query, endpoint-loss fault, and post-measurement attribution boundaries; includes retained top-k coordinator results and cached truth-membership attribution mappings"
)

const m8CanonicalTruthContractV1 = collections.VectorPartitionCanonicalScoreContractV1

type m8TruthCacheEvidenceV1 struct {
	Status         string `json:"status"`
	Identity       string `json:"identity"`
	ArtifactSHA256 string `json:"artifact_sha256"`
	ComputeNanos   int64  `json:"compute_nanos"`
	LoadNanos      int64  `json:"load_nanos"`
}

type m8TruthCacheFileV1 struct {
	SchemaVersion   int                     `json:"schema_version"`
	Identity        string                  `json:"identity"`
	Contract        string                  `json:"contract"`
	DatasetChecksum string                  `json:"dataset_checksum"`
	Dimensions      int                     `json:"dimensions"`
	Metric          string                  `json:"metric"`
	TopK            int                     `json:"top_k"`
	TruthSHA256     string                  `json:"truth_sha256"`
	Truth           [][]m8CanonicalResultV1 `json:"truth"`
}

type m8ProductionReportV1 struct {
	SchemaVersion           int                                                        `json:"schema_version"`
	ResultKind              string                                                     `json:"result_kind"`
	Status                  string                                                     `json:"status"`
	Mode                    string                                                     `json:"mode"`
	ProductionEvidence      bool                                                       `json:"production_evidence"`
	GeneratedAt             time.Time                                                  `json:"generated_at"`
	ExecutionID             string                                                     `json:"execution_id"`
	ExecutionEvidenceDigest string                                                     `json:"execution_evidence_digest,omitempty"`
	MeasurementTranscript   m8ProductionMeasurementTranscriptEvidenceV1                `json:"measurement_transcript"`
	RouterRepresentatives   uint64                                                     `json:"router_representatives"`
	Command                 []string                                                   `json:"exact_command"`
	ExecutableSHA256        string                                                     `json:"executable_sha256"`
	BaseSHA                 string                                                     `json:"base_sha"`
	HeadSHA                 string                                                     `json:"head_sha"`
	Dirty                   bool                                                       `json:"dirty"`
	GoVersion               string                                                     `json:"go_version"`
	GOOS                    string                                                     `json:"goos"`
	GOARCH                  string                                                     `json:"goarch"`
	LogicalCPUs             int                                                        `json:"logical_cpus"`
	GOMAXPROCS              int                                                        `json:"gomaxprocs"`
	GoMemoryLimitBytes      int64                                                      `json:"go_memory_limit_bytes"`
	Host                    m8ProductionHostEvidenceV1                                 `json:"host"`
	Dataset                 fixtureManifest                                            `json:"dataset"`
	DatasetDirectory        string                                                     `json:"dataset_directory,omitempty"`
	TruthCacheDirectory     string                                                     `json:"canonical_truth_cache_directory,omitempty"`
	Variant                 *m3VariantDescriptorV1                                     `json:"variant,omitempty"`
	Config                  m8ProductionConfigEvidenceV1                               `json:"config"`
	BuildNanos              int64                                                      `json:"build_nanos"`
	Topology                nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1 `json:"topology"`
	Rows                    []m8ProductionRowV1                                        `json:"rows"`
	PackDiagnostics         []m8PartitionPackDiagnosticsV1                             `json:"partition_pack_diagnostics,omitempty"`
	Failure                 m8ProductionFailureEvidenceV1                              `json:"failure"`
	GateLedger              m8ProductionGateLedgerV1                                   `json:"gate_ledger"`
	Profiles                m8ProductionProfileEvidenceV1                              `json:"profiles"`
	RouterSessions          m8ProductionRouterSessionEvidenceV1                        `json:"router_sessions"`
	UntimedBoundary         m8ProductionResourceBoundaryV1                             `json:"untimed_resource_boundary"`
	Resources               m8ProductionResourceEvidenceV1                             `json:"resources"`
	TruthCache              m8TruthCacheEvidenceV1                                     `json:"canonical_truth_cache"`
	TimedBoundary           string                                                     `json:"timed_boundary"`
	Limitations             []string                                                   `json:"limitations"`
}

type m8ProductionConfigEvidenceV1 struct {
	RaftGroups          int       `json:"raft_groups"`
	RaftNodesPerGroup   int       `json:"raft_nodes_per_group"`
	Partitions          int       `json:"partitions"`
	Probes              []int     `json:"probes"`
	Overlap             []float64 `json:"overlap"`
	TopK                int       `json:"top_k"`
	RecallTarget        float64   `json:"recall_target"`
	Concurrency         []int     `json:"concurrency"`
	Warmup              int       `json:"warmup_requests"`
	EffectiveWarmup     int       `json:"effective_warmup_requests"`
	EfSearch            []int     `json:"ef_search"`
	RouterCandidates    int       `json:"approximate_router_candidate_budget"`
	MaxExactTruthVisits int64     `json:"max_exact_truth_visits,omitempty"`
	Seed                int64     `json:"seed"`
}

// m8ProductionMeasurementTranscriptEvidenceV1 binds the rows measured before
// the report is written to a separately hashed runner output.
type m8ProductionMeasurementTranscriptEvidenceV1 struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type m8ProductionMeasurementTranscriptV1 struct {
	SchemaVersion       int                          `json:"schema_version"`
	ExecutionID         string                       `json:"execution_id"`
	Dataset             fixtureManifest              `json:"dataset"`
	Variant             *m3VariantDescriptorV1       `json:"variant"`
	Config              m8ProductionConfigEvidenceV1 `json:"config"`
	Rows                []m8ProductionRowV1          `json:"rows"`
	Outcomes            []m8ProductionRowOutcomesV1  `json:"outcomes"`
	PeakRSSObservations []uint64                     `json:"peak_rss_observations"`
}

// m8ProductionRowOutcomesV1 retains the measured coordinator document IDs,
// rather than another self-reported recall aggregate. Scores are deliberately
// omitted: canonical recall is an ID-set metric.
type m8ProductionRowOutcomesV1 struct {
	Overlap                      float64    `json:"overlap"`
	Probes                       int        `json:"probes"`
	EfSearch                     int        `json:"ef_search"`
	Concurrency                  int        `json:"concurrency"`
	Status                       string     `json:"status"`
	Samples                      int        `json:"samples"`
	TopKIDs                      [][]string `json:"top_k_document_ids"`
	TopKScoreBits                [][]uint32 `json:"top_k_score_bits"`
	TotalNanos                   []uint64   `json:"total_nanos"`
	ExactRepresentativeTruthHits []uint16   `json:"exact_representative_truth_hits,omitempty"`
}

// m8ProductionAttributionV1 keeps each lossy boundary visible. Recall is
// always measured against the same canonical global FP32-score oracle.
type m8ProductionAttributionV1 struct {
	Contract             string  `json:"contract"`
	GlobalExactRecallAtK float64 `json:"global_exact_recall_at_k"`
	// OracleStagesComplete distinguishes the V1 retained ladder from older
	// report fixtures while keeping the report decoder backwards-readable.
	OracleStagesComplete              bool    `json:"oracle_stages_complete"`
	PrimaryHomeOracleRecallAtK        float64 `json:"primary_home_oracle_recall_at_k"`
	FinalMembershipOracleRecallAtK    float64 `json:"final_membership_oracle_recall_at_k"`
	PrimaryHomeOracleRegretAtK        float64 `json:"primary_home_oracle_regret_at_k"`
	FinalMembershipOracleRegretAtK    float64 `json:"final_membership_oracle_regret_at_k"`
	PrimaryToFinalMembershipGainAtK   float64 `json:"primary_to_final_membership_gain_at_k"`
	FinalMembershipToExactLossAtK     float64 `json:"final_membership_to_exact_routing_loss_at_k"`
	ExactToApproximateLossAtK         float64 `json:"exact_to_approximate_routing_loss_at_k"`
	ExactToLocalHNSWLossAtK           float64 `json:"exact_to_local_hnsw_loss_at_k"`
	ApproximateToLocalHNSWLossAtK     float64 `json:"approximate_to_local_hnsw_loss_at_k"`
	ApproximateLocalToEndToEndLossAtK float64 `json:"approximate_local_hnsw_to_end_to_end_loss_at_k"`
	ExhaustivePartitionRecallAtK      float64 `json:"exhaustive_partition_union_recall_at_k"`
	ExhaustivePartitionIDParity       bool    `json:"exhaustive_partition_union_id_parity"`
	ExhaustivePartitionScoreParity    bool    `json:"exhaustive_partition_union_score_parity"`
	ExactRepresentativeRecallAtK      float64 `json:"exact_representative_routing_recall_at_k"`
	// Truth-home diagnostics separate partition placement from representative
	// ranking: coverage is the share of exact truth neighbors whose primary
	// home was selected, while pair co-location describes how concentrated the
	// exact truth set already is in that primary partitioning.
	ExactRepresentativeTruthHomeCoverageAtK           float64                     `json:"exact_representative_truth_home_partition_coverage_at_k"`
	TruthNeighborHomePartitionsAtK                    float64                     `json:"truth_neighbor_primary_home_partitions_at_k"`
	TruthNeighborHomePairColocationAtK                float64                     `json:"truth_neighbor_primary_home_pair_colocation_at_k"`
	ExactRepresentativeFinalMembershipCoverageAtK     float64                     `json:"exact_representative_truth_final_membership_coverage_at_k"`
	TruthNeighborFinalMembershipPartitionsAtK         float64                     `json:"truth_neighbor_final_membership_partitions_at_k"`
	TruthNeighborFinalMembershipPairColocationAtK     float64                     `json:"truth_neighbor_final_membership_pair_colocation_at_k"`
	ExactRepresentativeOverlapTruthContributionAtK    float64                     `json:"exact_representative_overlap_truth_contribution_at_k"`
	ExactRepresentativeDuplicateMembershipCoverageAtK float64                     `json:"exact_representative_duplicate_membership_coverage_at_k"`
	TruthNeighborRankRetentionAtK                     []float64                   `json:"truth_neighbor_rank_final_membership_retention_at_k"`
	ApproximateRepresentativeRecallAtK                float64                     `json:"approximate_representative_routing_recall_at_k"`
	LocalHNSWRecallAtK                                float64                     `json:"partition_local_hnsw_recall_at_k"`
	ApproximateLocalHNSWRecallAtK                     float64                     `json:"approximate_partition_local_hnsw_recall_at_k"`
	LocalHNSWSearches                                 uint64                      `json:"partition_local_hnsw_searches"`
	LocalHNSWCandidates                               uint64                      `json:"partition_local_hnsw_candidates"`
	LocalHNSWEdges                                    uint64                      `json:"partition_local_hnsw_edges"`
	ApproximateLocalHNSWSearches                      uint64                      `json:"approximate_partition_local_hnsw_searches"`
	ApproximateLocalHNSWCandidates                    uint64                      `json:"approximate_partition_local_hnsw_candidates"`
	ApproximateLocalHNSWEdges                         uint64                      `json:"approximate_partition_local_hnsw_edges"`
	EndToEndRecallAtK                                 float64                     `json:"end_to_end_recall_at_k"`
	CoordinatorMergeIDParity                          bool                        `json:"coordinator_merge_id_parity"`
	CoordinatorMergeScoreParity                       bool                        `json:"coordinator_merge_score_parity"`
	ApproximateRouterCandidateBudget                  int                         `json:"approximate_router_candidate_budget"`
	ApproximateRouterPartitionCoverageComplete        bool                        `json:"approximate_router_partition_coverage_complete"`
	ResidualLossOwners                                []string                    `json:"residual_loss_owners"`
	StageOwners                                       []m8AttributionStageOwnerV1 `json:"stage_owners"`
}

type m8AttributionStageOwnerV1 struct {
	Stage      string  `json:"stage"`
	Owner      string  `json:"owner"`
	FromRecall float64 `json:"from_recall_at_k"`
	ToRecall   float64 `json:"to_recall_at_k"`
	Delta      float64 `json:"delta_at_k"`
	Active     bool    `json:"active"`
}

// m8PartitionPackDiagnosticsV1 records offline topology facts for each
// generation-pinned local pack. It is deliberately separate from recall: a
// directed layer-0 traversal can reveal an unreachable entry-reachable subset,
// but is not a substitute for the exact-oracle quality measurement.
type m8PartitionPackDiagnosticsV1 struct {
	PartitionID           uint32   `json:"partition_id"`
	Rows                  uint64   `json:"rows"`
	ReachableRows         uint64   `json:"reachable_rows"`
	TraversalRoots        uint64   `json:"traversal_roots"`
	MaxLayer              int      `json:"max_layer"`
	RowsByLayer           []uint64 `json:"rows_by_layer"`
	EdgesByLayer          []uint64 `json:"edges_by_layer"`
	Layer0DegreeLimit     uint64   `json:"layer0_degree_limit"`
	Layer0SaturatedRows   uint64   `json:"layer0_saturated_rows"`
	AuxiliaryEdges        uint64   `json:"auxiliary_edges"`
	AuxiliaryCSRBytes     uint64   `json:"auxiliary_csr_bytes"`
	AuxiliaryMaxDegree    uint64   `json:"auxiliary_max_degree"`
	CombinedReachableRows uint64   `json:"combined_reachable_rows"`
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
	ElapsedNanos           uint64                    `json:"elapsed_nanos,omitempty"`
	P50Nanos               uint64                    `json:"p50_nanos,omitempty"`
	P95Nanos               uint64                    `json:"p95_nanos,omitempty"`
	P99Nanos               uint64                    `json:"p99_nanos,omitempty"`
	MaxTotalNanos          uint64                    `json:"max_total_nanos,omitempty"`
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
	MaxShardPartitions     uint64                    `json:"max_shard_partitions,omitempty"`
	MaxShardRequestBytes   uint64                    `json:"max_shard_request_bytes,omitempty"`
	MaxShardResponseBytes  uint64                    `json:"max_shard_response_bytes,omitempty"`
	MaxShardCandidateBytes uint64                    `json:"max_shard_candidate_bytes,omitempty"`
	ExactParityChecked     bool                      `json:"exact_all_partition_parity_checked"`
	ExactParityPassed      bool                      `json:"exact_all_partition_parity_passed"`
	NoPartialResults       bool                      `json:"no_partial_results"`
	Attribution            m8ProductionAttributionV1 `json:"recall_attribution"`
}

type m8ProductionFailureEvidenceV1 struct {
	Class             string                         `json:"class"`
	StoppedGroup      string                         `json:"stopped_group"`
	Error             string                         `json:"error"`
	ReturnedNeighbors int                            `json:"returned_neighbors"`
	ReturnedGroups    int                            `json:"returned_groups"`
	Passed            bool                           `json:"passed"`
	ResourceBoundary  m8ProductionResourceBoundaryV1 `json:"resource_boundary"`
}

// m8ProductionResourceBoundaryV1 keeps untimed requests out of recall and
// throughput rows while still making their resource maxima authoritative for
// the resource gate.
type m8ProductionResourceBoundaryV1 struct {
	SelectedPartitions int                                  `json:"selected_partitions"`
	EfSearch           int                                  `json:"ef_search"`
	WallClockNanos     uint64                               `json:"wall_clock_nanos"`
	Maxima             m8ProductionResourceObservedMaximaV1 `json:"observed_maxima"`
}

// Retain the old name for source compatibility with existing focused tests.
type m8ProductionFaultResourceBoundaryV1 = m8ProductionResourceBoundaryV1

type m8ProductionGateLedgerV1 struct {
	ExhaustiveParity          string `json:"exhaustive_correctness"`
	FailureHonesty            string `json:"failure_honesty"`
	PartitionPackReachability string `json:"partition_pack_reachability"`
	Recall                    string `json:"recall"`
	ProbeReduction            string `json:"probe_reduction"`
	EndToEndQPS               string `json:"end_to_end_qps"`
	TailLatency               string `json:"tail_latency"`
	Balance                   string `json:"balance"`
	OverlapStorage            string `json:"overlap_storage"`
	ResourceBounds            string `json:"resource_bounds"`
	ExistingBehavior          string `json:"existing_behavior"`
}

type m8ProductionProfileEvidenceV1 struct {
	Directory string                          `json:"directory,omitempty"`
	Captured  []string                        `json:"captured"`
	Artifacts []m8ProductionProfileArtifactV1 `json:"artifacts,omitempty"`
	Status    string                          `json:"status"`
	Scope     string                          `json:"scope"`
}

type m8ProductionProfileArtifactV1 struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
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
	PartitionLoads       []uint64                                `json:"partition_loads"`
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
	durations        []uint64
	routingHits      []uint16
}

type m8ProductionResourceObservedMaximaV1 struct {
	Requests            uint64 `json:"requests"`
	RPCs                uint64 `json:"rpcs"`
	Retries             uint64 `json:"retries"`
	Redirects           uint64 `json:"redirects"`
	RequestBytes        uint64 `json:"request_bytes"`
	CandidateBytes      uint64 `json:"candidate_bytes"`
	ResponseBytes       uint64 `json:"response_bytes"`
	MergeEntries        uint64 `json:"merge_entries"`
	ShardPartitions     uint64 `json:"shard_partitions"`
	ShardRequestBytes   uint64 `json:"shard_request_bytes"`
	ShardCandidateBytes uint64 `json:"shard_candidate_bytes"`
	ShardResponseBytes  uint64 `json:"shard_response_bytes"`
}

func runM8ProductionSingleVariantV1(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) (runErr error) {
	profileReportPublished := false
	datasetDirectory, err := m8CanonicalPathV1(cfg.dataset)
	if err != nil {
		return fmt.Errorf("resolve M8 dataset directory: %w", err)
	}
	groups := make([]string, cfg.raftGroups)
	for i := range groups {
		groups[i] = fmt.Sprintf("m8-data-group-%02d", i)
	}
	started := time.Now()
	executableSHA256, err := m8BenchmarkExecutableSHA256V1(cfg.command[0])
	if err != nil {
		return fmt.Errorf("hash M8 benchmark executable: %w", err)
	}
	if cfg.m8ExistingDB != "" {
		descriptor, err := m3ReadVariantDescriptorV1(cfg.m8ExistingDB)
		if err != nil {
			return fmt.Errorf("read retained M3 descriptor: %w", err)
		}
		if err := m8ValidateRetainedM3ProvenanceV1(cfg, descriptor, executableSHA256); err != nil {
			return err
		}
	}
	var assets *m8ProductionMultiGroupAssetsV1
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
	if assets.descriptor != nil {
		if err := m8ValidateRetainedM3ProvenanceV1(cfg, *assets.descriptor, executableSHA256); err != nil {
			return err
		}
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

	truth, truthCache, err := m8LoadOrComputeTruthV1(cfg.m8TruthCache, assets.collection, assets.manifest, fixture, queries, cfg.topK, cfg.m8TruthCacheSHA256)
	if err != nil {
		return err
	}
	replayCommand := append([]string(nil), cfg.command...)
	if truthCache.ArtifactSHA256 != "" {
		replayCommand, err = m8ReplayCommandWithTruthCacheDigestV1(replayCommand, truthCache.ArtifactSHA256)
		if err != nil {
			return err
		}
	}
	truthCacheDirectory := ""
	if cfg.m8TruthCache != "" {
		truthCacheDirectory, err = m8CanonicalPathV1(cfg.m8TruthCache)
		if err != nil {
			return fmt.Errorf("resolve M8 truth-cache directory: %w", err)
		}
	}
	executionID, err := m8ProductionExecutionIDV1()
	if err != nil {
		return err
	}
	goMaxProcs, goMemoryLimitBytes := benchmarkRuntimeLimits()
	report := m8ProductionReportV1{
		SchemaVersion: 4, ResultKind: "m8_production_multi_group_evidence_v4", Status: "incomplete",
		Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now().UTC(),
		ExecutionID: executionID, RouterRepresentatives: assets.status.Representatives,
		Command: replayCommand, ExecutableSHA256: executableSHA256, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, Dirty: m8GitDirtyInV1(cfg.sourceCheckout, cfg.out, cfg.profiles, cfg.m8MatrixOut, cfg.m8MatrixProfiles),
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: goMaxProcs, GoMemoryLimitBytes: goMemoryLimitBytes, Host: m8ProductionHostV1(cfg, assets.dir), Dataset: fixture, DatasetDirectory: datasetDirectory, TruthCacheDirectory: truthCacheDirectory, Variant: assets.descriptor,
		Config:        m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: append([]int(nil), cfg.probes...), Overlap: append([]float64(nil), cfg.overlaps...), TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: append([]int(nil), cfg.concurrency...), Warmup: cfg.warmup, EfSearch: append([]int(nil), cfg.efSearch...), RouterCandidates: cfg.routerCandidates, MaxExactTruthVisits: cfg.m8MaxExactTruthVisits, Seed: cfg.seed},
		BuildNanos:    buildNanos,
		TruthCache:    truthCache,
		Profiles:      m8ProductionProfileEvidenceV1{Directory: cfg.profiles, Status: "not_captured", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs requires the captured baseline for differential analysis"},
		TimedBoundary: "wall-clock query cells after topology, exhaustive endpoint preflight, and generation warmup; includes router, coordinator, TCP M5 serialization, Raft read-index/apply, persistent HNSW search, response merge, and caller scheduling; excludes topology construction, exact truth, preflight, warmup, post-measurement attribution, artifact encoding, and shutdown",
		Limitations: []string{
			"loopback TCP with real serialized M5 messages and real in-memory HashiCorp Raft consensus; not a multi-host deployment",
			"the checked-in 10k path materializes disjoint round-robin packs; -m8-existing-db reuses the retained graph-built M3 packs read-only",
			"multi-host qualification and external-system comparisons are explicitly outside this local gate",
		},
	}
	report.Config.EffectiveWarmup, _ = m8WarmupCountAndConcurrencyV1(cfg)
	report.RouterSessions.BeforeWarmup = topology.Coordinator().Stats().RouterSessions
	if cfg.profiles != "" {
		if err := os.MkdirAll(cfg.profiles, 0o755); err != nil {
			return fmt.Errorf("create M8 profiles directory: %w", err)
		}
	}
	report.UntimedBoundary, err = m8WarmProductionTopologyV1(context.Background(), topology.Coordinator(), assets, queries, cfg)
	if err != nil {
		return err
	}
	report.RouterSessions.AfterWarmup = topology.Coordinator().Stats().RouterSessions
	profileCapture, err := startM8ProfileCaptureV1(cfg.profiles)
	if err != nil {
		return err
	}
	defer func() {
		if profileCapture != nil {
			if cfg.m8MatrixOut == "" {
				runErr = errors.Join(runErr, m8FinishDirectProfileCaptureV1(profileCapture, profileReportPublished))
			} else {
				_, closeErr := profileCapture.Stop()
				runErr = errors.Join(runErr, closeErr)
			}
		}
	}()
	measuredCells := make([]m8MeasuredCellV1, 0, len(cfg.overlaps)*len(cfg.probes)*len(cfg.efSearch)*len(cfg.concurrency))
	for _, overlap := range cfg.overlaps {
		if assets.descriptor == nil && overlap != 0 {
			for _, probes := range cfg.probes {
				for _, ef := range cfg.efSearch {
					for _, concurrency := range cfg.concurrency {
						report.Rows = append(report.Rows, m8ProductionRowV1{Status: "unsupported", UnsupportedReason: "nonzero overlap requires an immutable retained M3 variant descriptor", Overlap: overlap, Probes: probes, EfSearch: ef, Concurrency: concurrency})
					}
				}
			}
			continue
		}
		for _, probes := range cfg.probes {
			for _, ef := range cfg.efSearch {
				for _, concurrency := range cfg.concurrency {
					row, results, durations, rowErr := m8RunProductionCellV1(context.Background(), topology.Coordinator(), assets, queries, truth, probes, ef, concurrency, cfg.topK, cfg.routerCandidates, cfg.m8CoordinatorLimits.MaxCandidateBytes)
					if rowErr != nil {
						return fmt.Errorf("M8 production cell probes=%d ef=%d concurrency=%d: %w", probes, ef, concurrency, rowErr)
					}
					row.Overlap = overlap
					if assets.descriptor != nil {
						row.VariantID = assets.descriptor.VariantID
					}
					report.Rows = append(report.Rows, row)
					measuredCells = append(measuredCells, m8MeasuredCellV1{rowIndex: len(report.Rows) - 1, probes: probes, efSearch: ef, results: results, durations: durations})
				}
			}
		}
	}
	report.RouterSessions.AfterMeasured = topology.Coordinator().Stats().RouterSessions
	report.Failure, report.Topology = m8RunUnavailableGroupV1(context.Background(), topology, assets, queries[0], cfg.topK, cfg.m8CoordinatorLimits.MaxCandidateBytes)
	if profileCapture != nil {
		captured, stopErr := profileCapture.Stop()
		if stopErr != nil {
			return stopErr
		}
		artifacts, artifactErr := m8ProfileArtifactsV1(captured)
		if artifactErr != nil {
			return artifactErr
		}
		captured = make([]string, len(artifacts))
		for i := range artifacts {
			captured[i] = artifacts[i].Path
		}
		directory, directoryErr := m8CanonicalPathV1(profileCapture.dir)
		if directoryErr != nil {
			return fmt.Errorf("resolve M8 profiles directory: %w", directoryErr)
		}
		report.Profiles = m8ProductionProfileEvidenceV1{Directory: directory, Captured: captured, Artifacts: artifacts, Status: "captured_production_query_and_fault_boundary", Scope: "CPU, block, mutex, and trace cover measured query cells plus the endpoint-loss fault; heap is an end snapshot; allocs.pprof is cumulative and must be compared with allocs_baseline.pprof"}
	}
	allUntimed := m8MergeProductionResourceBoundariesV1(report.UntimedBoundary, report.Failure.ResourceBoundary)

	// Diagnostics and attribution deliberately run after the timed query/fault boundary
	// and profile capture. Their exhaustive mmap scans must not
	// pre-fault the corpus or contaminate measured process-resource evidence.
	// Peak RSS remains process-lifetime evidence and is captured after attribution,
	// including the bounded top-k results and cached truth-membership maps retained here.
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
	diagnostics, diagnosticsErr := attributionHarness.packDiagnostics()
	if diagnosticsErr != nil {
		return fmt.Errorf("collect M8 partition-pack diagnostics: %w", diagnosticsErr)
	}
	report.PackDiagnostics = diagnostics
	if len(measuredCells) > 0 {
		primaryHomes, finalMemberships, homesErr := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
		if homesErr != nil {
			return fmt.Errorf("build M8 truth-membership attribution mapping: %w", homesErr)
		}
		approximateCandidates := min(cfg.routerCandidates, int(assets.status.Representatives))
		if approximateCandidates < 1 {
			return errors.New("M8 attribution requires an approximate router candidate budget")
		}
		attribution := make(map[string]m8AttributionCellV1, len(cfg.probes)*len(cfg.efSearch))
		exhaustive := make([][]m8CanonicalResultV1, len(queries))
		for _, probes := range cfg.probes {
			membershipOracles, oracleErr := m8MembershipOracleRecallCacheV1(truth, primaryHomes, finalMemberships, len(attributionHarness.searchers), probes)
			if oracleErr != nil {
				return fmt.Errorf("build M8 membership oracles probes=%d: %w", probes, oracleErr)
			}
			for _, efSearch := range cfg.efSearch {
				cell, buildErr := m8BuildAttributionV1(context.Background(), assets, primaryHomes, finalMemberships, queries, truth, membershipOracles, probes, efSearch, cfg.topK, approximateCandidates, exhaustive, attributionHarness)
				if buildErr != nil {
					return fmt.Errorf("build M8 attribution probes=%d ef=%d: %w", probes, efSearch, buildErr)
				}
				attribution[m8AttributionKeyV1(probes, efSearch)] = cell
			}
		}
		for i := range measuredCells {
			measured := &measuredCells[i]
			cell, ok := attribution[m8AttributionKeyV1(measured.probes, measured.efSearch)]
			if !ok {
				return fmt.Errorf("missing M8 attribution probes=%d ef=%d", measured.probes, measured.efSearch)
			}
			if len(cell.RoutingHits) != report.Rows[measured.rowIndex].Samples {
				return fmt.Errorf("missing M8 routing outcomes probes=%d ef=%d", measured.probes, measured.efSearch)
			}
			measured.routingHits = append([]uint16(nil), cell.RoutingHits...)
			if err := m8AttachAttributionV1(&report.Rows[measured.rowIndex], cell, measured.results); err != nil {
				return fmt.Errorf("attach M8 attribution probes=%d ef=%d: %w", measured.probes, measured.efSearch, err)
			}
		}
	}
	closeErr := attributionHarness.Close()
	attributionHarnessClosed = true
	if closeErr != nil {
		return fmt.Errorf("close M8 attribution harness: %w", closeErr)
	}
	report.Resources = m8ProductionResourcesV1(cfg, fixture, assets, report.Rows, allUntimed, report.Topology, report.Failure.ResourceBoundary)
	m8ApplyRouterSessionIdentityResourceMaxV1(&report.Resources, report.RouterSessions)
	report.GateLedger = m8ProductionGateLedgerForReportV1(report)
	if m8ProductionAllGatesPassV1(report.GateLedger) {
		report.Status = "pass"
	} else if m8ProductionAnyGateFailsV1(report.GateLedger) {
		report.Status = "experimental_gate_failures"
	}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return err
	}
	transcript, err := m8WriteProductionMeasurementTranscriptV1(cfg.out, report, measuredCells)
	if err != nil {
		return err
	}
	report.MeasurementTranscript = transcript
	if report.Profiles.Status == "captured_production_query_and_fault_boundary" {
		evidenceDigest, digestErr := m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, report.Profiles.Artifacts, transcript.SHA256)
		if digestErr != nil {
			return digestErr
		}
		report.ExecutionEvidenceDigest = evidenceDigest
	}
	if err := validateM8ProductionReportV1(report, m8ProductionResourceCapsV1{PersistentAssetBytes: cfg.m8MaxAssetBytes, PeakRSSBytes: cfg.m8MaxRSSBytes}); err != nil {
		return fmt.Errorf("validate M8 production report: %w", err)
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	name, err := m8ArtifactNameV1(cfg, fixture, assets.manifest, report.ExecutionID)
	if err != nil {
		return err
	}
	path := filepath.Join(cfg.out, name)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return err
	}
	profileReportPublished = true
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

const m8ProductionExecutionIDBytesV1 = 16

func m8ProductionExecutionIDV1() (string, error) {
	var bytes [m8ProductionExecutionIDBytesV1]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("read M8 execution identity: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func validM8ProductionExecutionIDV1(id string) bool {
	if len(id) != 2*m8ProductionExecutionIDBytesV1 {
		return false
	}
	bytes, err := hex.DecodeString(id)
	return err == nil && len(bytes) == m8ProductionExecutionIDBytesV1 && hex.EncodeToString(bytes) == id
}

func m8ProductionProfileSetDigestV1(artifacts []m8ProductionProfileArtifactV1) (string, error) {
	byName := make(map[string]m8ProductionProfileArtifactV1, len(artifacts))
	for _, artifact := range artifacts {
		name := filepath.Base(artifact.Path)
		if artifact.Path == "" || artifact.Bytes <= 0 || !m8QualificationSHA256V1(artifact.SHA256) || byName[name].Path != "" {
			return "", errors.New("invalid M8 profile artifact identity")
		}
		byName[name] = artifact
	}
	type profileIdentity struct {
		Name   string
		Bytes  int64
		SHA256 string
	}
	ordered := make([]profileIdentity, 0, len(m8ProfileArtifactNamesV1))
	for _, name := range m8ProfileArtifactNamesV1 {
		artifact, ok := byName[name]
		if !ok {
			return "", errors.New("incomplete M8 profile artifact identity")
		}
		ordered = append(ordered, profileIdentity{Name: name, Bytes: artifact.Bytes, SHA256: artifact.SHA256})
	}
	if len(byName) != len(ordered) {
		return "", errors.New("unexpected M8 profile artifact identity")
	}
	raw, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func m8ProductionExecutionEvidenceDigestV1(executionID string, artifacts []m8ProductionProfileArtifactV1, transcriptSHA256 string) (string, error) {
	if !validM8ProductionExecutionIDV1(executionID) {
		return "", errors.New("invalid M8 execution identity")
	}
	if !m8QualificationSHA256V1(transcriptSHA256) {
		return "", errors.New("invalid M8 measurement transcript identity")
	}
	profileDigest, err := m8ProductionProfileSetDigestV1(artifacts)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(struct{ ExecutionID, ProfileSetDigest, TranscriptSHA256 string }{executionID, profileDigest, transcriptSHA256})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func m8WriteProductionMeasurementTranscriptV1(dir string, report m8ProductionReportV1, measuredCells []m8MeasuredCellV1) (m8ProductionMeasurementTranscriptEvidenceV1, error) {
	if !validM8ProductionExecutionIDV1(report.ExecutionID) {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, errors.New("invalid M8 execution identity")
	}
	outcomes, err := m8ProductionMeasurementTranscriptOutcomesV1(report, measuredCells)
	if err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	maxBytes, err := m8ProductionMeasurementTranscriptMaxBytesV1(report)
	if err != nil || maxBytes > m8QualificationTranscriptMaxBytesV1 {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, errors.New("M8 measurement transcript outcomes exceed the retained byte cap")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	path := filepath.Join(dir, "vector_partition_m8_measurements_"+report.ExecutionID+".json")
	if !report.Resources.PeakRSSMeasured || report.Resources.PeakRSSBytes <= 0 {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, errors.New("M8 measurement transcript requires a positive peak RSS observation")
	}
	transcript := m8ProductionMeasurementTranscriptV1{SchemaVersion: 5, ExecutionID: report.ExecutionID, Dataset: report.Dataset, Variant: report.Variant, Config: report.Config, Rows: report.Rows, Outcomes: outcomes, PeakRSSObservations: []uint64{uint64(report.Resources.PeakRSSBytes)}}
	raw, err := json.Marshal(transcript)
	if err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	if int64(len(raw)) > maxBytes || int64(len(raw)) > m8QualificationTranscriptMaxBytesV1 {
		_ = file.Close()
		return m8ProductionMeasurementTranscriptEvidenceV1{}, errors.New("M8 measurement transcript exceeds retained byte cap")
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	if err := file.Close(); err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	path, err = m8CanonicalPathV1(path)
	if err != nil {
		return m8ProductionMeasurementTranscriptEvidenceV1{}, err
	}
	digest := sha256.Sum256(raw)
	return m8ProductionMeasurementTranscriptEvidenceV1{Path: path, Bytes: int64(len(raw)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func m8ReadProductionMeasurementTranscriptV1(report m8ProductionReportV1) (m8ProductionMeasurementTranscriptV1, error) {
	evidence := report.MeasurementTranscript
	if !filepath.IsAbs(evidence.Path) || evidence.Bytes <= 0 || !m8QualificationSHA256V1(evidence.SHA256) {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("invalid M8 measurement transcript evidence")
	}
	path, err := m8CanonicalPathV1(evidence.Path)
	if err != nil || path != evidence.Path {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("M8 measurement transcript path is not canonical")
	}
	raw, err := readBoundedRegularFileV1(evidence.Path, m8QualificationTranscriptMaxBytesV1)
	if err != nil || int64(len(raw)) != evidence.Bytes {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("read M8 measurement transcript")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != evidence.SHA256 {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("M8 measurement transcript digest mismatch")
	}
	var transcript m8ProductionMeasurementTranscriptV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&transcript) != nil || transcript.SchemaVersion != 5 {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("invalid M8 measurement transcript schema")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("M8 measurement transcript has trailing JSON")
	}
	if transcript.ExecutionID != report.ExecutionID || transcript.Dataset != report.Dataset || !reflect.DeepEqual(transcript.Variant, report.Variant) || !reflect.DeepEqual(transcript.Config, report.Config) || !reflect.DeepEqual(transcript.Rows, report.Rows) {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("M8 measurement transcript identity mismatch")
	}
	if err := m8ValidateProductionMeasurementTranscriptOutcomesV1(transcript, report); err != nil {
		return m8ProductionMeasurementTranscriptV1{}, err
	}
	if _, ok := m8ProductionMeasurementTranscriptPeakRSSV1(transcript); !ok {
		return m8ProductionMeasurementTranscriptV1{}, errors.New("invalid M8 measurement transcript peak RSS observations")
	}
	return transcript, nil
}

func m8ProductionMeasurementTranscriptPeakRSSV1(transcript m8ProductionMeasurementTranscriptV1) (uint64, bool) {
	if len(transcript.PeakRSSObservations) != 1 || transcript.PeakRSSObservations[0] == 0 || transcript.PeakRSSObservations[0] > uint64(^uint64(0)>>1) {
		return 0, false
	}
	return transcript.PeakRSSObservations[0], true
}

func validM8ProductionMeasurementTranscriptV1(report m8ProductionReportV1) bool {
	_, err := m8ReadProductionMeasurementTranscriptV1(report)
	return err == nil
}

func m8ProductionRowOutcomeIdentityV1(row m8ProductionRowV1) m8ProductionRowOutcomesV1 {
	return m8ProductionRowOutcomesV1{Overlap: row.Overlap, Probes: row.Probes, EfSearch: row.EfSearch, Concurrency: row.Concurrency, Status: row.Status, Samples: row.Samples}
}

func m8ProductionMeasurementTranscriptOutcomesV1(report m8ProductionReportV1, measuredCells []m8MeasuredCellV1) ([]m8ProductionRowOutcomesV1, error) {
	byRow := make(map[int]m8MeasuredCellV1, len(measuredCells))
	for _, measured := range measuredCells {
		if measured.rowIndex < 0 || measured.rowIndex >= len(report.Rows) {
			return nil, errors.New("M8 measurement transcript has invalid measured row")
		}
		if _, duplicate := byRow[measured.rowIndex]; duplicate {
			return nil, errors.New("M8 measurement transcript has duplicate measured row")
		}
		byRow[measured.rowIndex] = measured
	}
	outcomes := make([]m8ProductionRowOutcomesV1, len(report.Rows))
	for i, row := range report.Rows {
		outcome := m8ProductionRowOutcomeIdentityV1(row)
		if row.Status == "pass" || row.Status == "fail" {
			measured, ok := byRow[i]
			if !ok || len(measured.results) != row.Samples || len(measured.durations) != row.Samples || (measured.routingHits != nil && len(measured.routingHits) != row.Samples) {
				return nil, errors.New("M8 measurement transcript has incomplete query outcomes")
			}
			outcome.TopKIDs = make([][]string, len(measured.results))
			outcome.TopKScoreBits = make([][]uint32, len(measured.results))
			for query := range measured.results {
				outcome.TopKIDs[query] = m8CanonicalIDsV1(measured.results[query])
				outcome.TopKScoreBits[query] = make([]uint32, len(measured.results[query]))
				for result := range measured.results[query] {
					outcome.TopKScoreBits[query][result] = math.Float32bits(measured.results[query][result].Score)
				}
			}
			outcome.TotalNanos = append([]uint64(nil), measured.durations...)
			if measured.routingHits != nil {
				outcome.ExactRepresentativeTruthHits = append([]uint16(nil), measured.routingHits...)
			}
		} else if _, ok := byRow[i]; ok && row.Status != "candidate_coverage_shortfall" {
			return nil, errors.New("M8 measurement transcript has outcomes for unmeasured row")
		} else {
			outcome.TopKIDs = make([][]string, 0)
			outcome.TopKScoreBits = make([][]uint32, 0)
			outcome.TotalNanos = make([]uint64, 0)
		}
		outcomes[i] = outcome
	}
	return outcomes, nil
}

func m8ProductionMeasurementTranscriptMaxBytesV1(report m8ProductionReportV1) (int64, error) {
	if report.Dataset.Vectors < 1 || report.Config.TopK < 1 {
		return 0, errors.New("invalid M8 measurement transcript shape")
	}
	idBytes := int64(len(fmt.Sprintf("doc-%06d", report.Dataset.Vectors-1)))
	var resultCount, durationCount int64
	for _, row := range report.Rows {
		if row.Status != "pass" && row.Status != "fail" {
			continue
		}
		if row.Samples < 0 {
			return 0, errors.New("invalid M8 measurement transcript samples")
		}
		count, err := memoryMul(int64(row.Samples), int64(min(report.Config.TopK, report.Dataset.Vectors)))
		if err != nil {
			return 0, err
		}
		resultCount, err = memoryAdd(resultCount, count)
		if err != nil {
			return 0, err
		}
		durationCount, err = memoryAdd(durationCount, int64(row.Samples))
		if err != nil {
			return 0, err
		}
	}
	// ID quotes, comma/bracket punctuation, row keys, and the copied rows.
	resultBytes, err := memoryMul(resultCount, idBytes+3)
	if err != nil {
		return 0, err
	}
	// A uint64 JSON value is at most 20 digits plus a comma. The fixed
	// overhead also covers the field names, brackets, and copied rows.
	durationBytes, err := memoryMul(durationCount, 21)
	if err != nil {
		return 0, err
	}
	// Score bits retain exact coordinator float32 evidence without JSON float
	// conversion. A uint32 JSON token is at most ten digits plus a comma.
	scoreBytes, err := memoryMul(resultCount, 11)
	if err != nil {
		return 0, err
	}
	// Routing hits are uint16 values: at most five digits plus a comma.
	routingBytes, err := memoryMul(durationCount, 6)
	if err != nil {
		return 0, err
	}
	withResults, err := memoryAdd(64<<10, resultBytes)
	if err != nil {
		return 0, err
	}
	withScores, err := memoryAdd(withResults, scoreBytes)
	if err != nil {
		return 0, err
	}
	withDurations, err := memoryAdd(withScores, durationBytes)
	if err != nil {
		return 0, err
	}
	return memoryAdd(withDurations, routingBytes)
}

func m8ValidateProductionMeasurementTranscriptOutcomesV1(transcript m8ProductionMeasurementTranscriptV1, report m8ProductionReportV1) error {
	if len(transcript.Outcomes) != len(report.Rows) {
		return errors.New("M8 measurement transcript outcome rows do not match report rows")
	}
	for i, row := range report.Rows {
		outcome := transcript.Outcomes[i]
		identity := m8ProductionRowOutcomeIdentityV1(row)
		if identity.Overlap != outcome.Overlap || identity.Probes != outcome.Probes || identity.EfSearch != outcome.EfSearch || identity.Concurrency != outcome.Concurrency || identity.Status != outcome.Status || identity.Samples != outcome.Samples {
			return errors.New("M8 measurement transcript outcome cell does not match report row")
		}
		if row.Status == "pass" || row.Status == "fail" {
			if len(outcome.TopKIDs) != row.Samples {
				return errors.New("M8 measurement transcript outcome sample count mismatch")
			}
			if len(outcome.TopKScoreBits) != row.Samples {
				return errors.New("M8 measurement transcript outcome score sample count mismatch")
			}
			if len(outcome.TotalNanos) != row.Samples {
				return errors.New("M8 measurement transcript timing sample count mismatch")
			}
			for sample, ids := range outcome.TopKIDs {
				if len(ids) != min(report.Config.TopK, report.Dataset.Vectors) {
					return errors.New("M8 measurement transcript outcome top-k count mismatch")
				}
				if len(outcome.TopKScoreBits[sample]) != len(ids) {
					return errors.New("M8 measurement transcript outcome ID/score count mismatch")
				}
				seen := make(map[string]bool, len(ids))
				for _, id := range ids {
					if !m8FixtureDocumentIDValidV1(id, report.Dataset.Vectors) || seen[id] {
						return errors.New("M8 measurement transcript has invalid query outcome ID")
					}
					seen[id] = true
				}
				for _, bits := range outcome.TopKScoreBits[sample] {
					score := math.Float32frombits(bits)
					if math.IsNaN(float64(score)) || math.IsInf(float64(score), 0) {
						return errors.New("M8 measurement transcript has nonfinite score bits")
					}
				}
			}
			for _, duration := range outcome.TotalNanos {
				if duration == 0 {
					return errors.New("M8 measurement transcript has zero timing sample")
				}
			}
			if len(outcome.ExactRepresentativeTruthHits) != 0 {
				if len(outcome.ExactRepresentativeTruthHits) != row.Samples {
					return errors.New("M8 measurement transcript routing hit sample count mismatch")
				}
				var hits uint64
				for _, hit := range outcome.ExactRepresentativeTruthHits {
					if int(hit) > report.Config.TopK {
						return errors.New("M8 measurement transcript routing hit exceeds top-k")
					}
					hits += uint64(hit)
				}
				if math.Abs(row.Attribution.ExactRepresentativeRecallAtK-float64(hits)/float64(row.Samples*report.Config.TopK)) > 1e-12 {
					return errors.New("M8 measurement transcript routing hits disagree with attribution")
				}
			}
			p50, p95, p99 := m8PercentileV1(outcome.TotalNanos, 50), m8PercentileV1(outcome.TotalNanos, 95), m8PercentileV1(outcome.TotalNanos, 99)
			var maximum uint64
			for _, duration := range outcome.TotalNanos {
				maximum = max(maximum, duration)
			}
			if row.P50Nanos != p50 || row.P95Nanos != p95 || row.P99Nanos != p99 || row.MaxTotalNanos != maximum {
				return errors.New("M8 measurement transcript timings do not reproduce retained percentiles")
			}
			minimumElapsed, ok := m8TotalNanosElapsedLowerBoundV1(outcome.TotalNanos, row.Concurrency)
			if !ok {
				return errors.New("M8 measurement transcript timing aggregate overflows")
			}
			if row.ElapsedNanos < minimumElapsed {
				return errors.New("M8 measurement transcript timings exceed retained elapsed time")
			}
		} else if outcome.TopKIDs == nil || len(outcome.TopKIDs) != 0 || outcome.TopKScoreBits == nil || len(outcome.TopKScoreBits) != 0 || outcome.TotalNanos == nil || len(outcome.TotalNanos) != 0 || len(outcome.ExactRepresentativeTruthHits) != 0 {
			return errors.New("M8 measurement transcript has outcomes for shortfall or unsupported row")
		}
	}
	return nil
}

// m8TotalNanosElapsedLowerBoundV1 is the least wall time compatible with
// retained request totals when at most concurrency requests run at once.
func m8TotalNanosElapsedLowerBoundV1(durations []uint64, concurrency int) (uint64, bool) {
	if len(durations) == 0 || concurrency < 1 {
		return 0, false
	}
	var total uint64
	for _, duration := range durations {
		if duration == 0 {
			return 0, false
		}
		next, carry := bits.Add64(total, duration, 0)
		if carry != 0 {
			return 0, false
		}
		total = next
	}
	workers := uint64(min(concurrency, len(durations)))
	minimum := total / workers
	if total%workers != 0 {
		if minimum == ^uint64(0) {
			return 0, false
		}
		minimum++
	}
	return minimum, true
}

func m8ArtifactNameV1(cfg config, fixture fixtureManifest, manifest collections.VectorPartitionManifestV1, executionID string) (string, error) {
	if !validM8ProductionExecutionIDV1(executionID) {
		return "", errors.New("invalid M8 execution identity")
	}
	identity, err := json.Marshal(struct {
		Fixture     fixtureManifest
		Config      m8ProductionConfigEvidenceV1
		Assets      m8ArtifactAssetIdentityV1
		ExecutionID string
	}{
		Fixture: fixture,
		Config: func() m8ProductionConfigEvidenceV1 {
			count, _ := m8WarmupCountAndConcurrencyV1(cfg)
			return m8ProductionConfigEvidenceV1{RaftGroups: cfg.raftGroups, RaftNodesPerGroup: cfg.raftNodes, Partitions: cfg.partitions, Probes: cfg.probes, Overlap: cfg.overlaps, TopK: cfg.topK, RecallTarget: cfg.recallTarget, Concurrency: cfg.concurrency, Warmup: cfg.warmup, EffectiveWarmup: count, EfSearch: cfg.efSearch, RouterCandidates: cfg.routerCandidates, MaxExactTruthVisits: cfg.m8MaxExactTruthVisits, Seed: cfg.seed}
		}(),
		Assets: m8ArtifactAssetIdentityV1{
			IntegrityDigest:  manifest.IntegrityDigest,
			ReadySetDigest:   manifest.ReadySetDigest,
			Generation:       manifest.Generation,
			RouterGeneration: manifest.RouterGeneration,
		},
		ExecutionID: executionID,
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
	canonicalDir, err := m8CanonicalPathV1(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve M8 profiles directory: %w", err)
	}
	if err := os.MkdirAll(canonicalDir, 0o755); err != nil {
		return nil, fmt.Errorf("create M8 profiles directory: %w", err)
	}
	capture := &m8ProfileCaptureV1{dir: canonicalDir}
	baseline := filepath.Join(canonicalDir, "allocs_baseline.pprof")
	if err := writeM8RuntimeProfileExclusiveV1("allocs", baseline); err != nil {
		return nil, fmt.Errorf("write M8 allocation baseline: %w", err)
	}
	capture.paths = append(capture.paths, baseline)
	capture.traceFile, err = os.OpenFile(filepath.Join(canonicalDir, "trace.out"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create M8 trace: %w", err), capture.Cleanup())
	}
	capture.paths = append(capture.paths, capture.traceFile.Name())
	if err = trace.Start(capture.traceFile); err != nil {
		_ = capture.traceFile.Close()
		return nil, errors.Join(fmt.Errorf("start M8 trace: %w", err), capture.Cleanup())
	}
	capture.cpu, err = os.OpenFile(filepath.Join(canonicalDir, "cpu.pprof"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		trace.Stop()
		_ = capture.traceFile.Close()
		return nil, errors.Join(fmt.Errorf("create M8 CPU profile: %w", err), capture.Cleanup())
	}
	capture.paths = append(capture.paths, capture.cpu.Name())
	if err = pprof.StartCPUProfile(capture.cpu); err != nil {
		trace.Stop()
		_ = capture.traceFile.Close()
		_ = capture.cpu.Close()
		return nil, errors.Join(fmt.Errorf("start M8 CPU profile: %w", err), capture.Cleanup())
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
		for _, item := range []struct {
			name, file string
		}{{"heap", "heap.pprof"}, {"allocs", "allocs.pprof"}, {"block", "block.pprof"}, {"mutex", "mutex.pprof"}} {
			path := filepath.Join(c.dir, item.file)
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
			if err == nil {
				c.paths = append(c.paths, path)
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
			}
		}
	})
	return append([]string(nil), c.paths...), c.err
}

// Cleanup removes only the O_EXCL artifacts this capture created. It never
// removes the caller-provided profile directory.
func (c *m8ProfileCaptureV1) Cleanup() (err error) {
	if c == nil {
		return nil
	}
	for _, path := range c.paths {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}

func m8FinishDirectProfileCaptureV1(capture *m8ProfileCaptureV1, published bool) error {
	_, err := capture.Stop()
	if !published {
		err = errors.Join(err, capture.Cleanup())
	}
	return err
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

func writeM8RuntimeProfileExclusiveV1(name, path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	profile := pprof.Lookup(name)
	if profile == nil {
		err = fmt.Errorf("M8 runtime profile %s unavailable", name)
	} else {
		err = profile.WriteTo(file, 0)
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		_ = os.Remove(path)
	}
	return err
}

var m8ProfileArtifactNamesV1 = [...]string{"allocs_baseline.pprof", "cpu.pprof", "trace.out", "heap.pprof", "allocs.pprof", "block.pprof", "mutex.pprof"}

const m8BenchmarkExecutableMaxBytesV1 = 512 << 20
const m8ProfileArtifactMaxBytesV1 = 512 << 20
const m8ProfileArtifactDecodeBaseTimeoutV1 = time.Minute
const m8ProfileArtifactDecodePerMiBTimeoutV1 = 500 * time.Millisecond

func m8BenchmarkExecutableSHA256V1(path string) (string, error) {
	canonical, err := m8CanonicalPathV1(path)
	if err != nil {
		return "", err
	}
	file, err := os.Open(canonical)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Size() > m8BenchmarkExecutableMaxBytesV1 {
		return "", errors.New("invalid or oversized M8 benchmark executable")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, m8BenchmarkExecutableMaxBytesV1+1)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func m8ProfileArtifactsV1(paths []string) ([]m8ProductionProfileArtifactV1, error) {
	if len(paths) != len(m8ProfileArtifactNamesV1) {
		return nil, errors.New("incomplete M8 profile artifact set")
	}
	artifacts := make([]m8ProductionProfileArtifactV1, 0, len(paths))
	expected, seen := make(map[string]bool, len(paths)), make(map[string]bool, len(paths))
	for _, name := range m8ProfileArtifactNamesV1 {
		expected[name] = true
	}
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve M8 profile path: %w", err)
		}
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return nil, fmt.Errorf("resolve M8 profile %q: %w", path, err)
		}
		info, err := os.Stat(resolved)
		name := filepath.Base(resolved)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > m8ProfileArtifactMaxBytesV1 || seen[resolved] || !expected[name] {
			return nil, fmt.Errorf("invalid M8 profile %q", path)
		}
		file, err := os.Open(resolved)
		if err != nil {
			return nil, fmt.Errorf("read M8 profile %q: %w", path, err)
		}
		hash := sha256.New()
		bytesRead, copyErr := io.Copy(hash, io.LimitReader(file, m8ProfileArtifactMaxBytesV1+1))
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return nil, fmt.Errorf("hash M8 profile %q: %w", path, errors.Join(copyErr, closeErr))
		}
		if bytesRead > m8ProfileArtifactMaxBytesV1 {
			return nil, fmt.Errorf("oversized M8 profile %q", path)
		}
		if err := m8ValidateProfileArtifactV1(resolved, name, info.Size()); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, m8ProductionProfileArtifactV1{Path: resolved, Bytes: info.Size(), SHA256: hex.EncodeToString(hash.Sum(nil))})
		seen[resolved] = true
		delete(expected, name)
	}
	if len(expected) != 0 {
		return nil, errors.New("incomplete M8 profile artifact set")
	}
	return artifacts, nil
}

func m8ProfileArtifactDecodeTimeoutV1(bytes int64) time.Duration {
	bytes = min(max(bytes, int64(0)), int64(m8ProfileArtifactMaxBytesV1))
	return m8ProfileArtifactDecodeBaseTimeoutV1 + time.Duration((bytes+(1<<20)-1)/(1<<20))*m8ProfileArtifactDecodePerMiBTimeoutV1
}

func m8ValidateProfileArtifactV1(path, name string, bytes int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), m8ProfileArtifactDecodeTimeoutV1(bytes))
	defer cancel()
	args := []string{"tool"}
	if name == "trace.out" {
		args = append(args, "trace", "-d=wire", path)
	} else {
		args = append(args, "pprof", "-raw", path)
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("decode M8 profile %q: %w", path, ctx.Err())
		}
		return fmt.Errorf("decode M8 profile %q: %w", path, err)
	}
	return nil
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

func m8TruthCacheIdentityV1(fixture fixtureManifest, topK int) string {
	b, _ := json.Marshal(struct {
		Checksum         string
		Dimensions, TopK int
		Metric, Contract string
	}{fixture.Checksum, fixture.Dimensions, topK, fixture.Metric, collections.VectorPartitionCanonicalScoreContractV1})
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func m8TruthCacheArtifactPathV1(cacheDir, identity string) string {
	return filepath.Join(cacheDir, "m8_canonical_truth_"+identity+".json")
}

func m8LoadOrComputeTruthV1(cacheDir string, collection *collections.Collection, manifest collections.VectorPartitionManifestV1, fixture fixtureManifest, queries [][]float64, topK int, expectedDigest string) ([][]m8CanonicalResultV1, m8TruthCacheEvidenceV1, error) {
	identity := m8TruthCacheIdentityV1(fixture, topK)
	evidence := m8TruthCacheEvidenceV1{Identity: identity}
	path := ""
	if cacheDir != "" {
		path = m8TruthCacheArtifactPathV1(cacheDir, identity)
	}
	if path != "" {
		started := time.Now()
		truth, artifactSHA, err := m8ReadTruthCacheV1(path, fixture, len(queries), topK, manifest.SourceRowCount, expectedDigest)
		if err == nil {
			evidence.Status, evidence.LoadNanos, evidence.ArtifactSHA256 = "reused", time.Since(started).Nanoseconds(), artifactSHA
			return truth, evidence, nil
		}
		if !os.IsNotExist(err) {
			return nil, evidence, fmt.Errorf("read canonical truth cache: %w", err)
		}
	}
	if collection == nil {
		return nil, evidence, errors.New("canonical truth cache miss requires a collection")
	}
	started := time.Now()
	truth, err := m8ExactTruthV1(collection, manifest, queries, topK)
	if err != nil {
		return nil, evidence, err
	}
	evidence.ComputeNanos = time.Since(started).Nanoseconds()
	evidence.Status = "computed"
	if path != "" {
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return nil, evidence, err
		}
		truthSHA256, err := m8TruthContentSHA256V1(truth)
		if err != nil {
			return nil, evidence, err
		}
		artifactSHA256, linked, err := m8PublishTruthCacheV1(path, expectedDigest, func(w io.Writer) error {
			return m8WriteTruthCacheJSONV1(w, m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: fixture.Dimensions, Metric: fixture.Metric, TopK: topK, TruthSHA256: truthSHA256, Truth: truth})
		})
		evidence.ArtifactSHA256 = artifactSHA256
		if err != nil {
			if linked {
				return nil, evidence, fmt.Errorf("canonical truth cache linked at %s with artifact_sha256=%s but publication did not complete: %w", path, artifactSHA256, err)
			}
			return nil, evidence, err
		}
	}
	return truth, evidence, nil
}

// m8PublishTruthCacheV1 exposes a complete cache only after it is closed and
// atomically linked into its final no-replace name.
func m8PublishTruthCacheV1(path, expectedDigest string, write func(io.Writer) error) (string, bool, error) {
	return m8PublishTruthCacheWithDirectorySyncV1(path, expectedDigest, write, m8SyncDirectoryV1)
}

func m8PublishTruthCacheWithDirectorySyncV1(path, expectedDigest string, write func(io.Writer) error, syncDirectory func(string) error) (string, bool, error) {
	file, err := os.CreateTemp(filepath.Dir(path), ".m8_canonical_truth_*.tmp")
	if err != nil {
		return "", false, err
	}
	tempPath := file.Name()
	defer os.Remove(tempPath)
	defer file.Close()
	hash := sha256.New()
	if err := write(io.MultiWriter(file, hash)); err != nil {
		return "", false, err
	}
	if err := file.Chmod(0o644); err != nil {
		return "", false, err
	}
	if err := file.Sync(); err != nil {
		return "", false, err
	}
	if err := file.Close(); err != nil {
		return "", false, err
	}
	artifactSHA256 := hex.EncodeToString(hash.Sum(nil))
	if expectedDigest != "" && artifactSHA256 != expectedDigest {
		return "", false, errors.New("computed canonical truth cache artifact does not match independently trusted digest")
	}
	if err := os.Link(tempPath, path); err != nil {
		return "", false, err
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return artifactSHA256, true, fmt.Errorf("sync canonical truth-cache directory: %w", err)
	}
	return artifactSHA256, true, nil
}

// m8ReadTruthCacheV1 performs the bounded, streaming cache validation shared
// by live replay and retained qualification evidence.
func m8ReadTruthCacheV1(path string, fixture fixtureManifest, queryCount, topK int, sourceRows uint64, expectedDigest string) ([][]m8CanonicalResultV1, string, error) {
	maxBytes, err := m8TruthCacheMaxBytesV1(queryCount, topK, fixture.Vectors)
	if err != nil {
		return nil, "", err
	}
	fileHandle, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	counter := &m8CountingReaderV1{Reader: io.LimitReader(fileHandle, maxBytes+1)}
	hasher := sha256.New()
	stream := io.TeeReader(counter, hasher)
	decoder := json.NewDecoder(stream)
	decoder.UseNumber()
	var file m8TruthCacheFileV1
	err = m8DecodeTruthCacheStreamV1(decoder, &file, queryCount, topK)
	if err == nil {
		if _, trailingErr := decoder.Token(); trailingErr != io.EOF {
			if trailingErr == nil {
				err = errors.New("canonical truth cache contains trailing JSON")
			} else {
				err = trailingErr
			}
		}
	}
	if _, copyErr := io.Copy(io.Discard, stream); err == nil && copyErr != nil {
		err = copyErr
	}
	closeErr := fileHandle.Close()
	if err == nil && closeErr != nil {
		err = closeErr
	}
	if counter.N > maxBytes {
		return nil, "", fmt.Errorf("canonical truth cache exceeds %d-byte bound before decode", maxBytes)
	}
	if err != nil {
		return nil, "", err
	}
	identity := m8TruthCacheIdentityV1(fixture, topK)
	if file.SchemaVersion != 1 || file.Identity != identity || file.Contract != collections.VectorPartitionCanonicalScoreContractV1 || file.DatasetChecksum != fixture.Checksum || file.Dimensions != fixture.Dimensions || file.Metric != fixture.Metric || file.TopK != topK || len(file.Truth) != queryCount {
		return nil, "", errors.New("canonical truth cache identity/schema mismatch")
	}
	if err := m8ValidateCachedTruthV1(file.Truth, file.TruthSHA256, topK, sourceRows, fixture.Vectors); err != nil {
		return nil, "", fmt.Errorf("canonical truth cache semantic mismatch: %w", err)
	}
	artifactSHA := hex.EncodeToString(hasher.Sum(nil))
	if expectedDigest == "" || artifactSHA != expectedDigest {
		return nil, "", errors.New("canonical truth cache artifact digest is absent or does not match independently trusted digest")
	}
	return file.Truth, artifactSHA, nil
}

// m8DecodeTruthCacheStreamV1 parses every container token explicitly. In
// particular, it never Decode's the cache or truth arrays as one buffered value.
func m8DecodeTruthCacheStreamV1(d *json.Decoder, file *m8TruthCacheFileV1, queryCount, topK int) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return errors.New("canonical truth cache must be an object")
	}
	seen := map[string]bool{}
	for d.More() {
		keyToken, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok || seen[key] {
			return errors.New("canonical truth cache has duplicate or invalid field")
		}
		seen[key] = true
		switch key {
		case "schema_version":
			file.SchemaVersion, err = m8DecodeIntTokenV1(d)
		case "identity":
			file.Identity, err = m8DecodeStringTokenV1(d)
		case "contract":
			file.Contract, err = m8DecodeStringTokenV1(d)
		case "dataset_checksum":
			file.DatasetChecksum, err = m8DecodeStringTokenV1(d)
		case "dimensions":
			file.Dimensions, err = m8DecodeIntTokenV1(d)
		case "metric":
			file.Metric, err = m8DecodeStringTokenV1(d)
		case "top_k":
			file.TopK, err = m8DecodeIntTokenV1(d)
		case "truth_sha256":
			file.TruthSHA256, err = m8DecodeStringTokenV1(d)
		case "truth":
			file.Truth, err = m8DecodeTruthRowsStreamV1(d, queryCount, topK)
		default:
			return fmt.Errorf("canonical truth cache has unknown field %q", key)
		}
		if err != nil {
			return err
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if len(seen) != 9 {
		return errors.New("canonical truth cache missing required field")
	}
	return nil
}
func m8DecodeStringTokenV1(d *json.Decoder) (string, error) {
	token, err := d.Token()
	if err != nil {
		return "", err
	}
	value, ok := token.(string)
	if !ok {
		return "", errors.New("canonical truth cache field must be string")
	}
	return value, nil
}
func m8DecodeIntTokenV1(d *json.Decoder) (int, error) {
	token, err := d.Token()
	if err != nil {
		return 0, err
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, errors.New("canonical truth cache field must be integer")
	}
	value, err := strconv.Atoi(string(number))
	return value, err
}
func m8DecodeTruthRowsStreamV1(d *json.Decoder, queryCount, topK int) ([][]m8CanonicalResultV1, error) {
	token, err := d.Token()
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, nil
	}
	if token != json.Delim('[') {
		return nil, errors.New("canonical truth cache truth must be array")
	}
	truth := make([][]m8CanonicalResultV1, 0, queryCount)
	for d.More() {
		if len(truth) >= queryCount {
			return nil, errors.New("canonical truth cache has too many query rows")
		}
		row, err := m8DecodeTruthRowStreamV1(d, topK)
		if err != nil {
			return nil, err
		}
		truth = append(truth, row)
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	return truth, nil
}
func m8DecodeTruthRowStreamV1(d *json.Decoder, topK int) ([]m8CanonicalResultV1, error) {
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return nil, errors.New("canonical truth cache row must be array")
	}
	row := make([]m8CanonicalResultV1, 0, topK)
	for d.More() {
		if len(row) >= topK {
			return nil, errors.New("canonical truth cache row exceeds top_k")
		}
		token, err := d.Token()
		if err != nil || token != json.Delim('{') {
			return nil, errors.New("canonical truth cache result must be object")
		}
		var result m8CanonicalResultV1
		fields := map[string]bool{}
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok || fields[key] {
				return nil, errors.New("canonical truth cache result has duplicate field")
			}
			fields[key] = true
			if key == "ID" {
				result.ID, err = m8DecodeStringTokenV1(d)
			} else if key == "Score" {
				var token any
				token, err = d.Token()
				number, ok := token.(json.Number)
				if err == nil && !ok {
					err = errors.New("canonical truth cache score must be number")
				}
				if err == nil {
					value, parseErr := strconv.ParseFloat(string(number), 32)
					if parseErr != nil {
						err = parseErr
					} else {
						result.Score = float32(value)
					}
				}
			} else {
				return nil, fmt.Errorf("canonical truth cache result unknown field %q", key)
			}
			if err != nil {
				return nil, err
			}
		}
		if _, err := d.Token(); err != nil {
			return nil, err
		}
		if len(fields) != 2 {
			return nil, errors.New("canonical truth cache result missing field")
		}
		row = append(row, result)
	}
	if _, err := d.Token(); err != nil {
		return nil, err
	}
	return row, nil
}

// m8TruthCacheMaxBytesV1 bounds untrusted cache input before streaming JSON
// decode. The raw encoding is never retained alongside decoded truth.
type m8CountingReaderV1 struct {
	io.Reader
	N int64
}

func (r *m8CountingReaderV1) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.N += int64(n)
	return n, err
}

// m8TruthCacheMaxBytesV1 tightly bounds canonical JSON from the declared
// deterministic ID domain; raw cache bytes are streamed, never retained.
func m8TruthCacheMaxBytesV1(queryCount, topK, fixtureVectors int) (int64, error) {
	if queryCount < 0 || topK < 1 || fixtureVectors < 1 {
		return 0, errors.New("invalid canonical truth cache shape")
	}
	perQuery := min(topK, fixtureVectors)
	results, err := memoryMul(int64(queryCount), int64(perQuery))
	if err != nil {
		return 0, err
	}
	idBytes := int64(len(fmt.Sprintf("doc-%06d", fixtureVectors-1)))
	// 64 covers JSON punctuation/key names plus a maximal float32 spelling.
	resultBytes, err := memoryMul(results, 64+idBytes)
	if err != nil {
		return 0, err
	}
	queryBytes, err := memoryMul(int64(queryCount), 4)
	if err != nil {
		return 0, err
	}
	return memoryAdd(4<<10, resultBytes, queryBytes)
}

func m8TruthContentSHA256V1(truth [][]m8CanonicalResultV1) (string, error) {
	h := sha256.New()
	if err := m8WriteTruthJSONV1(h, truth); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func m8WriteTruthJSONV1(w io.Writer, truth [][]m8CanonicalResultV1) error {
	if _, err := io.WriteString(w, "["); err != nil {
		return err
	}
	for i, row := range truth {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "["); err != nil {
			return err
		}
		for j, result := range row {
			if j > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			raw, err := json.Marshal(result)
			if err != nil {
				return err
			}
			if _, err := w.Write(raw); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "]"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "]")
	return err
}
func m8WriteTruthCacheJSONV1(w io.Writer, file m8TruthCacheFileV1) error {
	quote := func(value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		_, err = w.Write(raw)
		return err
	}
	if _, err := io.WriteString(w, `{"schema_version":`); err != nil {
		return err
	}
	if err := quote(file.SchemaVersion); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value any
	}{{"identity", file.Identity}, {"contract", file.Contract}, {"dataset_checksum", file.DatasetChecksum}, {"dimensions", file.Dimensions}, {"metric", file.Metric}, {"top_k", file.TopK}, {"truth_sha256", file.TruthSHA256}} {
		if _, err := io.WriteString(w, `,"`+field.name+`":`); err != nil {
			return err
		}
		if err := quote(field.value); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, `,"truth":`); err != nil {
		return err
	}
	if err := m8WriteTruthJSONV1(w, file.Truth); err != nil {
		return err
	}
	_, err := io.WriteString(w, "}")
	return err
}

func m8ValidateCachedTruthV1(truth [][]m8CanonicalResultV1, wantSHA256 string, topK int, sourceRows uint64, fixtureVectors int) error {
	if wantSHA256 == "" {
		return errors.New("missing truth_sha256")
	}
	gotSHA256, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		return err
	}
	if gotSHA256 != wantSHA256 {
		return errors.New("truth_sha256 mismatch")
	}
	expected := topK
	if sourceRows > 0 && uint64(expected) > sourceRows {
		expected = int(sourceRows)
	}
	for query, row := range truth {
		if len(row) == 0 || len(row) > topK || (sourceRows > 0 && len(row) != expected) {
			return fmt.Errorf("query=%d result count=%d", query, len(row))
		}
		seen := make(map[string]struct{}, len(row))
		for i, result := range row {
			if result.ID == "" || math.IsNaN(float64(result.Score)) || math.IsInf(float64(result.Score), 0) {
				return fmt.Errorf("query=%d result=%d invalid ID or score", query, i)
			}
			if _, duplicate := seen[result.ID]; duplicate {
				return fmt.Errorf("query=%d duplicate ID %q", query, result.ID)
			}
			if !m8FixtureDocumentIDValidV1(result.ID, fixtureVectors) {
				return fmt.Errorf("query=%d result=%d ID %q outside deterministic fixture domain", query, i, result.ID)
			}
			seen[result.ID] = struct{}{}
			if i > 0 && (row[i-1].Score < result.Score || (row[i-1].Score == result.Score && row[i-1].ID > result.ID)) {
				return fmt.Errorf("query=%d noncanonical result order", query)
			}
		}
	}
	return nil
}

func m8FixtureDocumentIDValidV1(id string, vectors int) bool {
	if vectors < 1 {
		return false
	}
	if !strings.HasPrefix(id, "doc-") {
		return false
	}
	ordinal, err := strconv.Atoi(strings.TrimPrefix(id, "doc-"))
	return err == nil && ordinal >= 0 && ordinal < vectors && id == fmt.Sprintf("doc-%06d", ordinal)
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
	return m8ExactTruthRowsV1(rows, queries, topK)
}

func m8ExactTruthFixtureV1(vectors, queries [][]float64, topK int) ([][]m8CanonicalResultV1, error) {
	rows := make([]collections.VectorPartitionRouterSourceRowV1, len(vectors))
	for i, vector := range vectors {
		values := make([]float32, len(vector))
		for j := range vector {
			values[j] = float32(vector[j])
		}
		rows[i] = collections.VectorPartitionRouterSourceRowV1{VectorOrdinal: uint64(i), DocumentID: []byte(fmt.Sprintf("doc-%06d", i)), Values: values}
	}
	return m8ExactTruthRowsV1(rows, queries, topK)
}

func m8ExactTruthRowsV1(rows []collections.VectorPartitionRouterSourceRowV1, queries [][]float64, topK int) ([][]m8CanonicalResultV1, error) {
	if topK < 1 || len(rows) == 0 {
		return nil, errors.New("M8 canonical source oracle requires positive rows and top_k")
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
	results, _, err := h.searchWithMetrics(ctx, query, partitions, topK, efSearch, exact)
	return results, err
}

func (h *m8AttributionHarnessV1) searchWithMetrics(ctx context.Context, query []float32, partitions []uint32, topK, efSearch int, exact bool) ([]m8CanonicalResultV1, collections.VectorPartitionSearchMetricsV1, error) {
	merged := make([]m8CanonicalResultV1, 0, len(partitions)*topK)
	var metrics collections.VectorPartitionSearchMetricsV1
	for _, partition := range partitions {
		if partition >= uint32(len(h.searchers)) || h.searchers[partition] == nil {
			return nil, metrics, errors.New("M8 attribution partition coverage is incomplete")
		}
		opts := collections.VectorPartitionSearchOptionsV1{TopK: topK, EfSearch: efSearch}
		var results []collections.VectorPartitionSearchResultV1
		var err error
		var partitionMetrics collections.VectorPartitionSearchMetricsV1
		if exact {
			results, _, err = h.searchers[partition].SearchExactWithOptionsV1(ctx, query, opts)
		} else {
			results, partitionMetrics, err = h.searchers[partition].SearchWithOptionsV1(ctx, query, opts)
		}
		if err != nil {
			return nil, metrics, fmt.Errorf("M8 attribution partition %d: %w", partition, err)
		}
		if !exact {
			metrics.Candidates += partitionMetrics.Candidates
			metrics.Edges += partitionMetrics.Edges
		}
		for _, result := range results {
			merged = append(merged, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	return m8CanonicalResultsV1(merged, topK), metrics, nil
}

func (h *m8AttributionHarnessV1) packDiagnostics() ([]m8PartitionPackDiagnosticsV1, error) {
	diagnostics := make([]m8PartitionPackDiagnosticsV1, len(h.searchers))
	for partition, searcher := range h.searchers {
		if searcher == nil {
			return nil, fmt.Errorf("M8 partition %d diagnostics: missing searcher", partition)
		}
		pack, err := searcher.PackDiagnosticsV1()
		if err != nil {
			return nil, fmt.Errorf("M8 partition %d diagnostics: %w", partition, err)
		}
		diagnostics[partition] = m8PartitionPackDiagnosticsV1{
			PartitionID: uint32(partition), Rows: pack.Rows, ReachableRows: pack.ReachableRows,
			TraversalRoots: pack.TraversalRoots, MaxLayer: pack.MaxLayer,
			RowsByLayer: append([]uint64(nil), pack.RowsByLayer...), EdgesByLayer: append([]uint64(nil), pack.EdgesByLayer...),
			Layer0DegreeLimit: pack.Layer0DegreeLimit, Layer0SaturatedRows: pack.Layer0SaturatedRows,
			AuxiliaryEdges: pack.AuxiliaryEdges, AuxiliaryCSRBytes: pack.AuxiliaryCSRBytes,
			AuxiliaryMaxDegree: pack.AuxiliaryMaxDegree, CombinedReachableRows: pack.CombinedReachableRows,
		}
	}
	return diagnostics, nil
}

type m8AttributionCellV1 struct {
	Evidence m8ProductionAttributionV1
	// Local is the approximate route's local-HNSW result, matching the measured
	// coordinator request. Exact-local recall remains separately in Evidence.
	Local       [][]m8CanonicalResultV1
	RoutingHits []uint16
}

type m8MembershipOracleRecallV1 struct {
	primary, final float64
}

func m8AttributionKeyV1(probes, efSearch int) string {
	return fmt.Sprintf("%d/%d", probes, efSearch)
}

// m8TruthPartitionMembershipsByDocumentIDV1 binds only canonical truth IDs to
// the pinned generation's primary and final membership relations.
func m8TruthPartitionMembershipsByDocumentIDV1(assets *m8ProductionMultiGroupAssetsV1, truth [][]m8CanonicalResultV1) (map[string]uint32, map[string][]uint32, error) {
	if assets == nil || assets.collection == nil || assets.manifest.SourceRowCount == 0 {
		return nil, nil, errors.New("missing M8 assets for truth-membership diagnostics")
	}
	wanted := make(map[string]string)
	for _, results := range truth {
		for _, result := range results {
			if result.ID == "" {
				return nil, nil, errors.New("canonical truth contains an empty document ID")
			}
			wanted[result.ID] = result.ID
		}
	}
	if len(wanted) == 0 {
		return nil, nil, errors.New("canonical truth contains no document IDs")
	}
	source, rows, err := assets.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, nil, err
	}
	if source.Generation != assets.manifest.SourceGeneration || source.Checksum != assets.manifest.SourceChecksum || source.SchemaHash != assets.manifest.SourceSchemaHash || source.RowCount != assets.manifest.SourceRowCount || len(rows) != int(source.RowCount) {
		return nil, nil, errors.New("source identity does not match pinned partition generation")
	}
	ordinalIDs := make(map[uint64]string, len(wanted))
	found := make(map[string]struct{}, len(wanted))
	for ordinal, row := range rows {
		id, ok := wanted[string(row.DocumentID)]
		if !ok {
			continue
		}
		if _, duplicate := found[id]; duplicate {
			return nil, nil, fmt.Errorf("duplicate canonical truth document ID %q in source", id)
		}
		found[id] = struct{}{}
		ordinalIDs[uint64(ordinal)] = id
	}
	if len(found) != len(wanted) {
		return nil, nil, errors.New("canonical truth document ID is absent from pinned source")
	}
	homes := make(map[string]uint32, len(wanted))
	memberships := make(map[string][]uint32, len(wanted))
	for _, membership := range assets.manifest.Memberships {
		if membership.VectorOrdinal >= source.RowCount || membership.PartitionID >= assets.manifest.PartitionCount {
			return nil, nil, errors.New("invalid primary partition membership")
		}
		id, ok := ordinalIDs[membership.VectorOrdinal]
		if !ok {
			continue
		}
		if _, duplicate := homes[id]; duplicate {
			return nil, nil, fmt.Errorf("duplicate primary partition membership for %q", id)
		}
		homes[id] = membership.PartitionID
		memberships[id] = append(memberships[id], membership.PartitionID)
	}
	for _, member := range assets.manifest.OverlapMemberships {
		if member.VectorOrdinal >= source.RowCount || member.PartitionID >= assets.manifest.PartitionCount {
			return nil, nil, errors.New("invalid overlap partition membership")
		}
		id, ok := ordinalIDs[member.VectorOrdinal]
		if !ok {
			continue
		}
		for _, existing := range memberships[id] {
			if existing == member.PartitionID {
				return nil, nil, fmt.Errorf("duplicate final partition membership for %q", id)
			}
		}
		memberships[id] = append(memberships[id], member.PartitionID)
	}
	for id := range wanted {
		if _, ok := homes[id]; !ok || len(memberships[id]) == 0 {
			return nil, nil, fmt.Errorf("canonical truth document ID %q has incomplete membership", id)
		}
		sort.Slice(memberships[id], func(i, j int) bool { return memberships[id][i] < memberships[id][j] })
	}
	return homes, memberships, nil
}

func m8BestPrimaryHomeOracleRecallV1(truth []m8CanonicalResultV1, homes map[string]uint32, partitions, probes int) (float64, error) {
	if len(truth) == 0 || partitions < 1 || probes < 1 || probes > partitions {
		return 0, errors.New("invalid primary-home oracle bounds")
	}
	counts := make([]int, partitions)
	for _, result := range truth {
		home, ok := homes[result.ID]
		if !ok || home >= uint32(partitions) {
			return 0, fmt.Errorf("canonical truth ID %q has no valid primary home", result.ID)
		}
		counts[home]++
	}
	sort.Sort(sort.Reverse(sort.IntSlice(counts)))
	covered := 0
	for _, count := range counts[:probes] {
		covered += count
	}
	return float64(covered) / float64(len(truth)), nil
}

// m8BestMembershipOracleRecallV1 exhausts the small, fixed partition domain
// using packed truth coverage, so each subset costs bounded uint64 operations
// instead of rescanning every truth result's membership slice.
func m8BestMembershipOracleRecallV1(truth []m8CanonicalResultV1, memberships map[string][]uint32, partitions, probes int) (float64, error) {
	if len(truth) == 0 || partitions < 1 || probes < 1 || probes > partitions {
		return 0, errors.New("invalid membership oracle bounds")
	}
	truthWords := (len(truth) + 63) / 64
	partitionCoverage := make([]uint64, partitions*truthWords)
	for rank, result := range truth {
		parts, ok := memberships[result.ID]
		if !ok || len(parts) == 0 {
			return 0, fmt.Errorf("canonical truth ID %q has no partition membership", result.ID)
		}
		seen := make(map[uint32]struct{}, len(parts))
		for _, partition := range parts {
			if partition >= uint32(partitions) {
				return 0, fmt.Errorf("canonical truth ID %q has out-of-range partition membership", result.ID)
			}
			if _, duplicate := seen[partition]; duplicate {
				return 0, fmt.Errorf("canonical truth ID %q has duplicate partition membership", result.ID)
			}
			seen[partition] = struct{}{}
			partitionCoverage[int(partition)*truthWords+rank/64] |= uint64(1) << (rank % 64)
		}
	}
	if probes == partitions {
		return 1, nil
	}
	if _, err := m8MembershipOracleCombinationCountV1(partitions, probes, maxBenchmarkWorkUnits); err != nil {
		return 0, err
	}
	best := 0
	coverageByDepth := make([]uint64, (probes+1)*truthWords)
	var visit func(int, int, int)
	visit = func(next, remaining, depth int) {
		if remaining == 0 {
			covered := 0
			for _, word := range coverageByDepth[depth*truthWords : (depth+1)*truthWords] {
				covered += bits.OnesCount64(word)
			}
			best = max(best, covered)
			return
		}
		for partition := next; partition <= partitions-remaining; partition++ {
			current := coverageByDepth[depth*truthWords : (depth+1)*truthWords]
			combined := coverageByDepth[(depth+1)*truthWords : (depth+2)*truthWords]
			partitionWords := partitionCoverage[partition*truthWords : (partition+1)*truthWords]
			for word := range combined {
				combined[word] = current[word] | partitionWords[word]
			}
			visit(partition+1, remaining-1, depth+1)
		}
	}
	visit(0, probes, 0)
	return float64(best) / float64(len(truth)), nil
}

func m8MembershipOracleCombinationCountV1(partitions, probes int, cap int64) (int64, error) {
	if partitions < 1 || probes < 1 || probes > partitions || cap < 1 {
		return 0, errors.New("invalid membership oracle combination bound")
	}
	combinations := int64(1)
	k := min(probes, partitions-probes)
	for i := 1; i <= k; i++ {
		numerator, denominator := int64(partitions-k+i), int64(i)
		a, b := numerator, denominator
		for b != 0 {
			a, b = b, a%b
		}
		numerator /= a
		denominator /= a
		if combinations%denominator != 0 {
			return 0, errors.New("invalid membership oracle binomial reduction")
		}
		combinations /= denominator
		if combinations > cap/numerator {
			return 0, fmt.Errorf("membership oracle C(%d,%d) exceeds %d-subset benchmark work cap", partitions, probes, cap)
		}
		combinations *= numerator
		if combinations > cap {
			return 0, fmt.Errorf("membership oracle C(%d,%d) exceeds %d-subset benchmark work cap", partitions, probes, cap)
		}
	}
	return combinations, nil
}

func m8MembershipOracleRecallCacheV1(truth [][]m8CanonicalResultV1, primaryHomes map[string]uint32, finalMemberships map[string][]uint32, partitions, probes int) ([]m8MembershipOracleRecallV1, error) {
	out := make([]m8MembershipOracleRecallV1, len(truth))
	for i := range truth {
		primary, err := m8BestPrimaryHomeOracleRecallV1(truth[i], primaryHomes, partitions, probes)
		if err != nil {
			return nil, fmt.Errorf("M8 primary-home oracle query=%d: %w", i, err)
		}
		final, err := m8BestMembershipOracleRecallV1(truth[i], finalMemberships, partitions, probes)
		if err != nil {
			return nil, fmt.Errorf("M8 final-membership oracle query=%d: %w", i, err)
		}
		out[i] = m8MembershipOracleRecallV1{primary: primary, final: final}
	}
	return out, nil
}

// m8TruthHomePartitionDiagnosticsV1 measures the selected-partition coverage
// of the canonical truth set and the truth set's primary-home concentration.
func m8TruthHomePartitionDiagnosticsV1(truth []m8CanonicalResultV1, selected []uint32, homes map[string]uint32) (coverage, distinctHomes, pairColocation float64, err error) {
	if len(truth) == 0 || len(selected) == 0 || len(homes) == 0 {
		return 0, 0, 0, errors.New("empty truth-home diagnostic input")
	}
	selectedSet := make(map[uint32]struct{}, len(selected))
	for _, partition := range selected {
		selectedSet[partition] = struct{}{}
	}
	homeCounts := make(map[uint32]int, len(truth))
	covered := 0
	for _, result := range truth {
		home, ok := homes[result.ID]
		if !ok {
			return 0, 0, 0, fmt.Errorf("canonical truth ID %q has no primary home", result.ID)
		}
		homeCounts[home]++
		if _, ok := selectedSet[home]; ok {
			covered++
		}
	}
	pairs := len(truth) * (len(truth) - 1) / 2
	matchingPairs := 0
	for _, count := range homeCounts {
		matchingPairs += count * (count - 1) / 2
	}
	if pairs > 0 {
		pairColocation = float64(matchingPairs) / float64(pairs)
	}
	return float64(covered) / float64(len(truth)), float64(len(homeCounts)), pairColocation, nil
}

func m8TruthFinalMembershipDiagnosticsV1(truth []m8CanonicalResultV1, selected []uint32, memberships map[string][]uint32, homes map[string]uint32) (coverage, distinct, pairColocation, overlapContribution, duplicateCoverage float64, retained []float64, err error) {
	if len(truth) == 0 || len(selected) == 0 || len(memberships) == 0 || len(homes) == 0 {
		return 0, 0, 0, 0, 0, nil, errors.New("empty truth final-membership diagnostic input")
	}
	selectedSet := make(map[uint32]struct{}, len(selected))
	for _, partition := range selected {
		selectedSet[partition] = struct{}{}
	}
	all := make(map[uint32]struct{})
	retained = make([]float64, len(truth))
	for rank, result := range truth {
		parts, ok := memberships[result.ID]
		if !ok || len(parts) == 0 {
			return 0, 0, 0, 0, 0, nil, fmt.Errorf("canonical truth ID %q has no final membership", result.ID)
		}
		if !slices.IsSorted(parts) {
			return 0, 0, 0, 0, 0, nil, fmt.Errorf("canonical truth ID %q has noncanonical final memberships", result.ID)
		}
		home, ok := homes[result.ID]
		if !ok {
			return 0, 0, 0, 0, 0, nil, fmt.Errorf("canonical truth ID %q has no primary home", result.ID)
		}
		selectedMemberships := 0
		for _, partition := range parts {
			all[partition] = struct{}{}
			if _, ok := selectedSet[partition]; ok {
				retained[rank] = 1
				selectedMemberships++
			}
		}
		if retained[rank] == 1 {
			if _, selectedHome := selectedSet[home]; !selectedHome {
				overlapContribution++
			}
			if selectedMemberships > 1 {
				duplicateCoverage++
			}
		}
		coverage += retained[rank]
	}
	pairs := len(truth) * (len(truth) - 1) / 2
	for i := range truth {
		for j := i + 1; j < len(truth); j++ {
			if m8MembershipsIntersectV1(memberships[truth[i].ID], memberships[truth[j].ID]) {
				pairColocation++
			}
		}
	}
	if pairs > 0 {
		pairColocation /= float64(pairs)
	}
	n := float64(len(truth))
	return coverage / n, float64(len(all)), pairColocation, overlapContribution / n, duplicateCoverage / n, retained, nil
}

func m8MembershipsIntersectV1(a, b []uint32) bool {
	for left, right := 0, 0; left < len(a) && right < len(b); {
		switch {
		case a[left] == b[right]:
			return true
		case a[left] < b[right]:
			left++
		default:
			right++
		}
	}
	return false
}

func m8BuildAttributionV1(ctx context.Context, assets *m8ProductionMultiGroupAssetsV1, primaryHomes map[string]uint32, finalMemberships map[string][]uint32, queries [][]float64, truth [][]m8CanonicalResultV1, membershipOracles []m8MembershipOracleRecallV1, probes, efSearch, topK, approximateCandidates int, exhaustive [][]m8CanonicalResultV1, harness *m8AttributionHarnessV1) (m8AttributionCellV1, error) {
	if len(primaryHomes) == 0 || len(finalMemberships) == 0 || len(membershipOracles) != len(queries) {
		return m8AttributionCellV1{}, errors.New("M8 attribution requires a cached truth-home mapping")
	}
	cell := m8AttributionCellV1{Evidence: m8ProductionAttributionV1{
		Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1,
		OracleStagesComplete:        true,
		ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
		CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
		ApproximateRouterCandidateBudget:           approximateCandidates,
		ApproximateRouterPartitionCoverageComplete: true,
	}, Local: make([][]m8CanonicalResultV1, len(queries))}
	allPartitions := make([]uint32, len(harness.searchers))
	for i := range allPartitions {
		allPartitions[i] = uint32(i)
	}
	// Route every query before starting either approximate search stage. A
	// candidate-coverage shortfall invalidates the cell's approximate evidence,
	// so retaining complete-query approximate work would be misleading.
	approximatePartitions := make([][]uint32, len(queries))
	for i, query64 := range queries {
		if err := ctx.Err(); err != nil {
			return cell, err
		}
		partitions, routeErr := harness.route(ctx, m8Query32V1(query64), probes, collections.VectorPartitionRouterModeApproxV1, approximateCandidates)
		coverageComplete, err := m8ApproximateRouterCoverageV1(routeErr)
		if err != nil {
			return cell, err
		}
		cell.Evidence.ApproximateRouterPartitionCoverageComplete = cell.Evidence.ApproximateRouterPartitionCoverageComplete && coverageComplete
		if coverageComplete {
			approximatePartitions[i] = partitions
		}
	}
	var primaryOracle, finalOracle, exhaustiveRecall, exactRecall, exactTruthHomeCoverage, exactFinalCoverage, truthHomePartitions, truthFinalPartitions, truthHomePairColocation, truthFinalPairColocation, overlapTruthContribution, duplicateMembershipCoverage, approximateRecall, exactLocalRecall, approximateLocalRecall float64
	for i, query64 := range queries {
		if err := ctx.Err(); err != nil {
			return cell, err
		}
		query := m8Query32V1(query64)
		primaryOracle += membershipOracles[i].primary
		finalOracle += membershipOracles[i].final
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
		coverage, homes, pairColocation, err := m8TruthHomePartitionDiagnosticsV1(truth[i], exactPartitions, primaryHomes)
		if err != nil {
			return cell, fmt.Errorf("M8 exact routing truth-home diagnostics query=%d: %w", i, err)
		}
		exactTruthHomeCoverage += coverage
		truthHomePartitions += homes
		truthHomePairColocation += pairColocation
		finalCoverage, finalPartitions, finalPairColocation, overlapContribution, duplicateCoverage, retained, err := m8TruthFinalMembershipDiagnosticsV1(truth[i], exactPartitions, finalMemberships, primaryHomes)
		if err != nil {
			return cell, fmt.Errorf("M8 exact routing final-membership diagnostics query=%d: %w", i, err)
		}
		exactFinalCoverage += finalCoverage
		truthFinalPartitions += finalPartitions
		truthFinalPairColocation += finalPairColocation
		overlapTruthContribution += overlapContribution
		duplicateMembershipCoverage += duplicateCoverage
		if cell.Evidence.TruthNeighborRankRetentionAtK == nil {
			cell.Evidence.TruthNeighborRankRetentionAtK = make([]float64, topK)
		}
		if len(retained) > len(cell.Evidence.TruthNeighborRankRetentionAtK) {
			return cell, fmt.Errorf("M8 truth rank retention exceeds top-k query=%d", i)
		}
		for rank := range retained {
			cell.Evidence.TruthNeighborRankRetentionAtK[rank] += retained[rank]
		}
		exactResults, err := harness.search(ctx, query, exactPartitions, topK, efSearch, true)
		if err != nil {
			return cell, err
		}
		cell.RoutingHits = append(cell.RoutingHits, uint16(m8IDHitCountV1(m8CanonicalIDsV1(truth[i]), m8CanonicalIDsV1(exactResults))))
		var approximateResults []m8CanonicalResultV1
		if cell.Evidence.ApproximateRouterPartitionCoverageComplete {
			approximateResults, err = harness.search(ctx, query, approximatePartitions[i], topK, efSearch, true)
			if err != nil {
				return cell, err
			}
		}
		exactLocal, exactLocalMetrics, err := harness.searchWithMetrics(ctx, query, exactPartitions, topK, efSearch, false)
		if err != nil {
			return cell, err
		}
		// These counters belong to LocalHNSWRecallAtK's exact route.
		cell.Evidence.LocalHNSWSearches += uint64(len(exactPartitions))
		cell.Evidence.LocalHNSWCandidates += exactLocalMetrics.Candidates
		cell.Evidence.LocalHNSWEdges += exactLocalMetrics.Edges
		if cell.Evidence.ApproximateRouterPartitionCoverageComplete {
			var approximateLocalMetrics collections.VectorPartitionSearchMetricsV1
			cell.Local[i], approximateLocalMetrics, err = harness.searchWithMetrics(ctx, query, approximatePartitions[i], topK, efSearch, false)
			if err != nil {
				return cell, err
			}
			cell.Evidence.ApproximateLocalHNSWSearches += uint64(len(approximatePartitions[i]))
			cell.Evidence.ApproximateLocalHNSWCandidates += approximateLocalMetrics.Candidates
			cell.Evidence.ApproximateLocalHNSWEdges += approximateLocalMetrics.Edges
		}
		exactRecall += m8CanonicalRecallV1(truth[i], exactResults)
		approximateRecall += m8CanonicalRecallV1(truth[i], approximateResults)
		exactLocalRecall += m8CanonicalRecallV1(truth[i], exactLocal)
		approximateLocalRecall += m8CanonicalRecallV1(truth[i], cell.Local[i])
	}
	n := float64(len(queries))
	if !cell.Evidence.ApproximateRouterPartitionCoverageComplete {
		// A shortfall means this approximate attribution cell did not evaluate
		// its requested partition coverage. Do not average successful partial
		// queries into deceptively nonzero approximate-routing or local recall.
		approximateRecall = 0
		approximateLocalRecall = 0
	}
	cell.Evidence.ExhaustivePartitionRecallAtK = exhaustiveRecall / n
	cell.Evidence.PrimaryHomeOracleRecallAtK = primaryOracle / n
	cell.Evidence.FinalMembershipOracleRecallAtK = finalOracle / n
	cell.Evidence.PrimaryHomeOracleRegretAtK = 1 - cell.Evidence.PrimaryHomeOracleRecallAtK
	cell.Evidence.FinalMembershipOracleRegretAtK = 1 - cell.Evidence.FinalMembershipOracleRecallAtK
	cell.Evidence.PrimaryToFinalMembershipGainAtK = cell.Evidence.FinalMembershipOracleRecallAtK - cell.Evidence.PrimaryHomeOracleRecallAtK
	cell.Evidence.FinalMembershipToExactLossAtK = cell.Evidence.FinalMembershipOracleRecallAtK - exactRecall/n
	cell.Evidence.ExactToApproximateLossAtK = exactRecall/n - approximateRecall/n
	cell.Evidence.ExactToLocalHNSWLossAtK = exactRecall/n - exactLocalRecall/n
	cell.Evidence.ApproximateToLocalHNSWLossAtK = approximateRecall/n - approximateLocalRecall/n
	cell.Evidence.ExactRepresentativeRecallAtK = exactRecall / n
	cell.Evidence.ExactRepresentativeTruthHomeCoverageAtK = exactTruthHomeCoverage / n
	cell.Evidence.TruthNeighborHomePartitionsAtK = truthHomePartitions / n
	cell.Evidence.TruthNeighborHomePairColocationAtK = truthHomePairColocation / n
	cell.Evidence.ExactRepresentativeFinalMembershipCoverageAtK = exactFinalCoverage / n
	cell.Evidence.TruthNeighborFinalMembershipPartitionsAtK = truthFinalPartitions / n
	cell.Evidence.TruthNeighborFinalMembershipPairColocationAtK = truthFinalPairColocation / n
	cell.Evidence.ExactRepresentativeOverlapTruthContributionAtK = overlapTruthContribution / n
	cell.Evidence.ExactRepresentativeDuplicateMembershipCoverageAtK = duplicateMembershipCoverage / n
	for rank := range cell.Evidence.TruthNeighborRankRetentionAtK {
		cell.Evidence.TruthNeighborRankRetentionAtK[rank] /= n
	}
	cell.Evidence.ApproximateRepresentativeRecallAtK = approximateRecall / n
	cell.Evidence.LocalHNSWRecallAtK = exactLocalRecall / n
	cell.Evidence.ApproximateLocalHNSWRecallAtK = approximateLocalRecall / n
	return cell, nil
}

// m8ExactPartitionUnionV1 scans every generation-pinned partition pack and
// returns canonical FP32 score results. It fails closed unless the caller
// supplies the complete manifest partition set.
func m8ExactPartitionUnionV1(ctx context.Context, assets *m8ProductionMultiGroupAssetsV1, query []float64, topK int) ([]m8CanonicalResultV1, error) {
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
	merged := make([]m8CanonicalResultV1, 0, len(assets.manifest.Placements)*topK)
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
			merged = append(merged, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	return m8CanonicalResultsV1(merged, topK), nil
}

func m8WarmProductionTopologyV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, cfg config) (m8ProductionResourceBoundaryV1, error) {
	var boundary m8ProductionResourceBoundaryV1
	if len(queries) == 0 {
		return boundary, errors.New("M8 warmup requires a query")
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
	response, err := coordinator.Search(requestCtx, m8ProductionRequestV1(assets, m8Query32V1(queries[0]), "m8-endpoint-preflight", len(assets.manifest.Placements), efSearch, cfg.topK, cfg.m8CoordinatorLimits.MaxCandidateBytes))
	cancel()
	if err != nil {
		return boundary, fmt.Errorf("M8 exhaustive endpoint preflight: %w", err)
	}
	m8AccumulateProductionResourceBoundaryV1(&boundary, response, efSearch)
	warmupCount, warmupConcurrency := m8WarmupCountAndConcurrencyV1(cfg)
	warmupWorkers := min(warmupCount, warmupConcurrency)
	var warmupStart sync.WaitGroup
	warmupStart.Add(warmupWorkers)
	var warmupMu sync.Mutex
	firstOrdinaryIndex := warmupCount
	var firstOrdinaryErr error
	m8RunBoundedWorkV1(warmupCount, warmupConcurrency, func(i int) {
		if i < warmupWorkers {
			warmupStart.Done()
			warmupStart.Wait()
		}
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		response, err := coordinator.Search(requestCtx, m8ProductionWarmupRequestV1(assets, m8Query32V1(queries[i%len(queries)]), fmt.Sprintf("m8-warmup-%06d", i), efSearch, cfg))
		cancel()
		responseSummary := nativewire.VectorPartitionCoordinatorResponseV1{Counters: response.Counters, Timing: response.Timing}
		response = nativewire.VectorPartitionCoordinatorResponseV1{}
		warmupMu.Lock()
		defer warmupMu.Unlock()
		if err != nil {
			if errors.Is(err, collections.ErrVectorPartitionRouterCandidateCoverageV1) {
				// Search returns a zero response on errors, but its typed
				// coordinator error retains the observed untimed work.
				var coordinatorErr *nativewire.VectorPartitionCoordinatorErrorV1
				if errors.As(err, &coordinatorErr) {
					m8AccumulateProductionResourceBoundaryV1(&boundary, nativewire.VectorPartitionCoordinatorResponseV1{
						Counters: coordinatorErr.Counters,
						Timing:   coordinatorErr.Timing,
					}, efSearch)
				}
				return
			}
			if i < firstOrdinaryIndex {
				firstOrdinaryIndex, firstOrdinaryErr = i, err
			}
			return
		}
		m8AccumulateProductionResourceBoundaryV1(&boundary, responseSummary, efSearch)
	})
	if firstOrdinaryErr != nil {
		return boundary, fmt.Errorf("M8 topology warmup %d: %w", firstOrdinaryIndex, firstOrdinaryErr)
	}
	return boundary, nil
}

func m8ProductionResourcesV1(cfg config, fixture fixtureManifest, assets *m8ProductionMultiGroupAssetsV1, rows []m8ProductionRowV1, untimed m8ProductionResourceBoundaryV1, topology nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1, failure m8ProductionResourceBoundaryV1) m8ProductionResourceEvidenceV1 {
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
		out.PartitionLoads = loads
		for _, load := range loads {
			out.MaxPartitionLoad = max(out.MaxPartitionLoad, load)
		}
	}
	// The manifest-covered overlap policy owns the capacity admission used when
	// the retained variant was built. Recomputing a source-row-only epsilon here
	// would make any fully materialized overlap variant impossible to pass even
	// when its persisted capacity deliberately admits those memberships.
	if policy, ok := collections.ParseVectorPartitionOverlapPolicyV1(assets.manifest.BalancePolicy); ok {
		out.BalanceHardCap = policy.Capacity
	} else if assets.manifest.BalancePolicy == "round_robin_disjoint_v1" {
		// Direct, non-retained production_multi_group runs use the built-in
		// disjoint policy rather than an M3 persisted overlap policy. Preserve
		// their original source-row-derived five-percent balance allowance.
		sourceRows, partitions := assets.manifest.SourceRowCount, uint64(assets.manifest.PartitionCount)
		if partitions > 0 {
			out.BalanceHardCap = (sourceRows*105 + partitions*100 - 1) / (partitions * 100)
		}
	}
	out.MmapStatus = "not_captured_by_m8_runner; retained M3/M5 artifacts own mapped-pack evidence"
	out.PeakRSSScope = m8PeakRSSScopeV1
	if peak, ok := vectorPartitionBenchmarkPeakRSS(); ok {
		out.PeakRSSBytes, out.PeakRSSMeasured = peak, true
	}
	var maxProbes, maxEf, maxTotalNanos uint64
	for _, row := range rows {
		if row.Status == "unsupported" {
			continue
		}
		maxProbes = max(maxProbes, uint64(row.Probes))
		maxEf = max(maxEf, uint64(row.EfSearch))
		maxTotalNanos = max(maxTotalNanos, row.MaxTotalNanos)
	}
	maxProbes = max(maxProbes, uint64(untimed.SelectedPartitions))
	maxProbes = max(maxProbes, uint64(failure.SelectedPartitions))
	maxEf = max(maxEf, uint64(untimed.EfSearch))
	maxEf = max(maxEf, uint64(failure.EfSearch))
	maxTotalNanos = max(maxTotalNanos, untimed.WallClockNanos)
	maxTotalNanos = max(maxTotalNanos, failure.WallClockNanos)
	resourceRows := append([]m8ProductionRowV1(nil), rows...)
	resourceRows = append(resourceRows, untimed.resourceRowV1(), failure.resourceRowV1())
	observed := m8ObservedResourceMaximaV1(resourceRows)
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
	configuredConcurrentRequests, concurrentRequestsErr := m8ConfiguredConcurrentShardRequestsV1(cfg.m8CoordinatorLimits.MaxConcurrentRequests, cfg.concurrency)
	add("coordinator_concurrent_requests_across_clients", configuredConcurrentRequests, topology.MaxConcurrentShardRequests, "count", concurrentRequestsErr == nil)
	configuredRetries, retriesOK := m8ConfiguredAggregateTaskLimitV1(cfg.m8CoordinatorLimits.MaxRetries, observed.Requests)
	configuredRedirects, redirectsOK := m8ConfiguredAggregateTaskLimitV1(cfg.m8CoordinatorLimits.MaxRedirects, observed.Requests)
	configuredRPCs, rpcsOK := m8ConfiguredRPCsV1(observed.Requests, configuredRetries, retriesOK)
	add("coordinator_rpcs_across_shard_requests", configuredRPCs, observed.RPCs, "count", rpcsOK)
	add("coordinator_retries_across_shard_requests", configuredRetries, observed.Retries, "count", retriesOK)
	add("coordinator_redirects_across_shard_requests", configuredRedirects, observed.Redirects, "count", redirectsOK)
	observedRouterCandidates := assets.status.Representatives
	add("coordinator_router_candidates", uint64(cfg.m8CoordinatorLimits.MaxRouterCandidates), uint64(observedRouterCandidates), "count", true)
	add("coordinator_query_bytes", uint64(cfg.m8CoordinatorLimits.MaxQueryBytes), uint64(fixture.Dimensions*4), "bytes", true)
	add("coordinator_top_k", uint64(cfg.m8CoordinatorLimits.MaxTopK), uint64(cfg.topK), "count", true)
	add("coordinator_ef_search", uint64(cfg.m8CoordinatorLimits.MaxEfSearch), maxEf, "count", true)
	add("coordinator_partitions_per_request", uint64(cfg.m8CoordinatorLimits.MaxPartitionsPerRequest), observed.ShardPartitions, "count", true)
	identityBytes := uint64(len("default")*2 + len(assets.manifest.Collection) + len(assets.manifest.IndexName) + len(assets.manifest.IndexDefinitionDigest) + len(assets.manifest.ReadySetDigest))
	stableIDBytes := uint64(len(fmt.Sprintf("doc-%06d", max(0, fixture.Vectors-1))))
	add("coordinator_identity_bytes", uint64(cfg.m8CoordinatorLimits.MaxIdentityBytes), identityBytes, "bytes", true)
	add("coordinator_stable_id_bytes", uint64(cfg.m8CoordinatorLimits.MaxStableIDBytes), stableIDBytes, "bytes", true)
	add("coordinator_merge_entries", uint64(cfg.m8CoordinatorLimits.MaxMergeEntries), observed.MergeEntries, "count", true)
	add("coordinator_request_bytes", cfg.m8CoordinatorLimits.MaxRequestBytes, observed.RequestBytes, "bytes", true)
	add("coordinator_candidate_bytes", cfg.m8CoordinatorLimits.MaxCandidateBytes, observed.CandidateBytes, "bytes", true)
	add("coordinator_response_bytes", cfg.m8CoordinatorLimits.MaxResponseBytes, observed.ResponseBytes, "bytes", true)
	add("coordinator_wall_clock", uint64(cfg.m8CoordinatorLimits.MaxWallClock), maxTotalNanos, "nanoseconds", true)
	add("shard_dimensions", uint64(cfg.m8ShardLimits.MaxDimensions), uint64(fixture.Dimensions), "count", true)
	add("shard_query_bytes", uint64(cfg.m8ShardLimits.MaxQueryBytes), uint64(fixture.Dimensions*4), "bytes", true)
	add("shard_partitions", uint64(cfg.m8ShardLimits.MaxPartitions), observed.ShardPartitions, "count", true)
	add("shard_top_k", uint64(cfg.m8ShardLimits.MaxTopK), uint64(cfg.topK), "count", true)
	add("shard_ef_search", uint64(cfg.m8ShardLimits.MaxEfSearch), maxEf, "count", true)
	add("shard_identity_bytes", uint64(cfg.m8ShardLimits.MaxIdentityBytes), identityBytes, "bytes", true)
	add("shard_stable_id_bytes", uint64(cfg.m8ShardLimits.MaxStableIDBytes), stableIDBytes, "bytes", true)
	add("shard_request_bytes", cfg.m8ShardLimits.MaxRequestBytes, observed.ShardRequestBytes, "bytes", true)
	add("shard_candidate_bytes", cfg.m8ShardLimits.MaxCandidateBytes, observed.ShardCandidateBytes, "bytes", true)
	add("shard_response_bytes", cfg.m8ShardLimits.MaxResponseBytes, observed.ShardResponseBytes, "bytes", true)
	return out
}

func (e m8ProductionResourceBoundaryV1) resourceRowV1() m8ProductionRowV1 {
	return m8ProductionRowV1{
		Status: "untimed", Probes: e.SelectedPartitions, EfSearch: e.EfSearch, MaxTotalNanos: e.WallClockNanos,
		MaxRequests: e.Maxima.Requests, MaxRPCs: e.Maxima.RPCs, MaxRetries: e.Maxima.Retries, MaxRedirects: e.Maxima.Redirects,
		MaxRequestBytes: e.Maxima.RequestBytes, MaxCandidateBytes: e.Maxima.CandidateBytes, MaxResponseBytes: e.Maxima.ResponseBytes,
		MaxMergeEntries: e.Maxima.MergeEntries, MaxShardPartitions: e.Maxima.ShardPartitions,
		MaxShardRequestBytes: e.Maxima.ShardRequestBytes, MaxShardCandidateBytes: e.Maxima.ShardCandidateBytes,
		MaxShardResponseBytes: e.Maxima.ShardResponseBytes,
	}
}

func m8AccumulateProductionResourceBoundaryV1(boundary *m8ProductionResourceBoundaryV1, response nativewire.VectorPartitionCoordinatorResponseV1, efSearch int) {
	if boundary == nil {
		return
	}
	row := m8ProductionRowV1{Status: "untimed"}
	m8AccumulateProductionRowCountersV1(&row, response.Counters)
	boundary.SelectedPartitions = max(boundary.SelectedPartitions, int(response.Counters.SelectedPartitions))
	boundary.EfSearch = max(boundary.EfSearch, efSearch)
	boundary.WallClockNanos = max(boundary.WallClockNanos, response.Timing.TotalNanos)
	boundary.Maxima = m8ObservedResourceMaximaV1([]m8ProductionRowV1{boundary.resourceRowV1(), row})
}

func m8MergeProductionResourceBoundariesV1(boundaries ...m8ProductionResourceBoundaryV1) m8ProductionResourceBoundaryV1 {
	var out m8ProductionResourceBoundaryV1
	rows := make([]m8ProductionRowV1, 0, len(boundaries))
	for _, boundary := range boundaries {
		out.SelectedPartitions = max(out.SelectedPartitions, boundary.SelectedPartitions)
		out.EfSearch = max(out.EfSearch, boundary.EfSearch)
		out.WallClockNanos = max(out.WallClockNanos, boundary.WallClockNanos)
		rows = append(rows, boundary.resourceRowV1())
	}
	out.Maxima = m8ObservedResourceMaximaV1(rows)
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
		out.ShardPartitions = max(out.ShardPartitions, row.MaxShardPartitions)
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

func m8ConfiguredAggregateTaskLimitV1(perTask int, observedTasks uint64) (uint64, bool) {
	if perTask < 0 || observedTasks == 0 || uint64(perTask) > ^uint64(0)/observedTasks {
		return 0, false
	}
	return uint64(perTask) * observedTasks, true
}

func m8ConfiguredRPCsV1(requests, retries uint64, retriesOK bool) (uint64, bool) {
	if !retriesOK || requests > ^uint64(0)-retries {
		return 0, false
	}
	return requests + retries, true
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
	row.MaxShardPartitions = max(row.MaxShardPartitions, counters.MaxShardPartitions)
	row.MaxShardRequestBytes = max(row.MaxShardRequestBytes, counters.MaxShardRequestBytes)
	row.MaxShardResponseBytes = max(row.MaxShardResponseBytes, counters.MaxShardResponseBytes)
	row.MaxShardCandidateBytes = max(row.MaxShardCandidateBytes, counters.MaxShardCandidateBytes)
}

func m8RunProductionCellV1(ctx context.Context, coordinator *nativewire.VectorPartitionCoordinatorV1, assets *m8ProductionMultiGroupAssetsV1, queries [][]float64, truth [][]m8CanonicalResultV1, probes, efSearch, concurrency, topK, routerCandidates int, candidateBytesLimit uint64) (m8ProductionRowV1, [][]m8CanonicalResultV1, []uint64, error) {
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
		outcomes[index].response, outcomes[index].err = coordinator.Search(requestCtx, m8ProductionApproximateRequestV1(assets, query, fmt.Sprintf("m8-q-%06d-p-%04d-ef-%06d-c-%03d", index, probes, efSearch, concurrency), probes, efSearch, topK, routerCandidates, candidateBytesLimit))
	})
	elapsedNanos := uint64(time.Since(started))
	if elapsedNanos == 0 {
		return m8ProductionRowV1{}, nil, nil, errors.New("M8 production cell elapsed time is zero")
	}
	// Coordinator.Search is an ANN/HNSW path even when every partition is
	// selected. Exact V1 parity is owned by m8ExactPartitionUnionV1 during
	// attribution, never by this measured all-partition ANN row.
	row := m8ProductionRowV1{Status: "pass", Probes: probes, EfSearch: efSearch, Concurrency: concurrency, RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterCandidates: m8ProductionApproximateRouterCandidateBudgetV1(assets, routerCandidates), Samples: len(queries)}
	canonicalResults := make([][]m8CanonicalResultV1, len(outcomes))
	durations := make([]uint64, 0, len(outcomes))
	var recallSum float64
	coverageShortfall := false
	for index, outcome := range outcomes {
		if outcome.err != nil {
			if errors.Is(outcome.err, collections.ErrVectorPartitionRouterCandidateCoverageV1) {
				var coordinatorErr *nativewire.VectorPartitionCoordinatorErrorV1
				if errors.As(outcome.err, &coordinatorErr) {
					m8AccumulateProductionRowCountersV1(&row, coordinatorErr.Counters)
					row.MaxTotalNanos = max(row.MaxTotalNanos, coordinatorErr.Timing.TotalNanos)
				}
				coverageShortfall = true
				continue
			}
			return row, nil, nil, fmt.Errorf("query %d: %w", index, outcome.err)
		}
		got, shapeErr := m8ValidateCoordinatorResponseV1(outcome.response, assets.manifest, probes, topK)
		if shapeErr != nil {
			return row, nil, nil, fmt.Errorf("query %d response shape: %w", index, shapeErr)
		}
		canonicalResults[index] = got
		recallSum += m8CanonicalRecallV1(truth[index], got)
		durations = append(durations, outcome.response.Timing.TotalNanos)
		m8AccumulateProductionRowCountersV1(&row, outcome.response.Counters)
	}
	if coverageShortfall {
		row.Status = "candidate_coverage_shortfall"
		row.ElapsedNanos = elapsedNanos
		for _, outcome := range outcomes {
			row.MaxTotalNanos = max(row.MaxTotalNanos, outcome.response.Timing.TotalNanos)
		}
		return row, make([][]m8CanonicalResultV1, len(outcomes)), nil, nil
	}
	row.NoPartialResults = true
	row.RecallAtK = recallSum / float64(len(outcomes))
	row.ElapsedNanos = elapsedNanos
	row.QPS, _ = m8ProductionQPSV1(row.Samples, row.ElapsedNanos)
	row.P50Nanos, row.P95Nanos, row.P99Nanos = m8PercentileV1(durations, 50), m8PercentileV1(durations, 95), m8PercentileV1(durations, 99)
	for _, duration := range durations {
		row.MaxTotalNanos = max(row.MaxTotalNanos, duration)
	}
	return row, canonicalResults, durations, nil
}

func m8ProductionQPSV1(samples int, elapsedNanos uint64) (float64, bool) {
	if samples < 1 || elapsedNanos == 0 {
		return 0, false
	}
	qps := float64(samples) * float64(time.Second) / float64(elapsedNanos)
	return qps, !math.IsNaN(qps) && !math.IsInf(qps, 0) && qps > 0
}

func m8AttachAttributionV1(row *m8ProductionRowV1, attribution m8AttributionCellV1, coordinatorResults [][]m8CanonicalResultV1) error {
	if row == nil || row.Samples < 1 || len(attribution.Local) != row.Samples || len(coordinatorResults) != row.Samples {
		return errors.New("M8 attribution result cardinality mismatch")
	}
	row.Attribution = attribution.Evidence
	if row.Status == "candidate_coverage_shortfall" {
		row.Attribution.ApproximateRouterPartitionCoverageComplete = false
		row.Attribution.ApproximateRepresentativeRecallAtK = 0
		row.Attribution.ApproximateLocalHNSWRecallAtK = 0
		row.Attribution.ApproximateLocalHNSWSearches = 0
		row.Attribution.ApproximateLocalHNSWCandidates = 0
		row.Attribution.ApproximateLocalHNSWEdges = 0
		row.Attribution.EndToEndRecallAtK = 0
		row.Attribution.CoordinatorMergeIDParity = false
		row.Attribution.CoordinatorMergeScoreParity = false
		row.Attribution.ExactToApproximateLossAtK = row.Attribution.ExactRepresentativeRecallAtK
		row.Attribution.ApproximateToLocalHNSWLossAtK = 0
		row.Attribution.ApproximateLocalToEndToEndLossAtK = 0
		row.Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(row.Attribution)
		row.Attribution.StageOwners = m8AttributionStageOwnersV1(row.Attribution)
		return nil
	}
	if !row.Attribution.ApproximateRouterPartitionCoverageComplete {
		row.Attribution.CoordinatorMergeIDParity = false
		row.Attribution.CoordinatorMergeScoreParity = false
		row.Attribution.EndToEndRecallAtK = 0
		row.Attribution.ApproximateLocalToEndToEndLossAtK = 0
		row.Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(row.Attribution)
		row.Attribution.StageOwners = m8AttributionStageOwnersV1(row.Attribution)
		return nil
	}
	for i := range coordinatorResults {
		idParity, scoreParity := m8CanonicalParityV1(attribution.Local[i], coordinatorResults[i])
		row.Attribution.CoordinatorMergeIDParity = row.Attribution.CoordinatorMergeIDParity && idParity
		row.Attribution.CoordinatorMergeScoreParity = row.Attribution.CoordinatorMergeScoreParity && scoreParity
	}
	row.Attribution.EndToEndRecallAtK = row.RecallAtK
	if row.Attribution.OracleStagesComplete {
		row.Attribution.ApproximateLocalToEndToEndLossAtK = row.Attribution.ApproximateLocalHNSWRecallAtK - row.Attribution.EndToEndRecallAtK
	}
	row.Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(row.Attribution)
	row.Attribution.StageOwners = m8AttributionStageOwnersV1(row.Attribution)
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
	owners := make([]string, 0, 7)
	if attribution.OracleStagesComplete && attribution.PrimaryHomeOracleRecallAtK+epsilon < attribution.GlobalExactRecallAtK {
		owners = append(owners, "primary_placement")
	}
	if attribution.OracleStagesComplete && attribution.FinalMembershipOracleRecallAtK+epsilon < attribution.GlobalExactRecallAtK {
		owners = append(owners, "overlap_or_placement_membership")
	}
	if !attribution.ExhaustivePartitionIDParity || !attribution.ExhaustivePartitionScoreParity || attribution.ExhaustivePartitionRecallAtK < 1-epsilon {
		owners = append(owners, "partition_membership_or_score_contract")
	}
	exactRoutingCeiling := attribution.ExhaustivePartitionRecallAtK
	if attribution.OracleStagesComplete {
		exactRoutingCeiling = attribution.FinalMembershipOracleRecallAtK
	}
	if attribution.ExactRepresentativeRecallAtK+epsilon < exactRoutingCeiling {
		owners = append(owners, "exact_representative_routing")
	}
	if !attribution.ApproximateRouterPartitionCoverageComplete || attribution.ApproximateRepresentativeRecallAtK+epsilon < attribution.ExactRepresentativeRecallAtK {
		owners = append(owners, "approximate_representative_routing")
	}
	if !attribution.ApproximateRouterPartitionCoverageComplete {
		return owners
	}
	if attribution.ApproximateLocalHNSWRecallAtK+epsilon < attribution.ApproximateRepresentativeRecallAtK {
		owners = append(owners, "partition_local_hnsw")
	}
	if !attribution.CoordinatorMergeIDParity || !attribution.CoordinatorMergeScoreParity || attribution.EndToEndRecallAtK+epsilon < attribution.ApproximateLocalHNSWRecallAtK {
		owners = append(owners, "coordinator_merge_or_transport")
	}
	if len(owners) == 0 {
		return []string{"none_observed"}
	}
	return owners
}

func m8AttributionStageOwnersV1(attribution m8ProductionAttributionV1) []m8AttributionStageOwnerV1 {
	if !attribution.OracleStagesComplete {
		return nil
	}
	const epsilon = 1e-12
	stage := func(name, owner string, from, to float64) m8AttributionStageOwnerV1 {
		delta := from - to
		return m8AttributionStageOwnerV1{Stage: name, Owner: owner, FromRecall: from, ToRecall: to, Delta: delta, Active: delta > epsilon}
	}
	out := []m8AttributionStageOwnerV1{
		stage("global_to_primary_home", "primary_placement", attribution.GlobalExactRecallAtK, attribution.PrimaryHomeOracleRecallAtK),
		stage("primary_home_to_final_membership", "overlap_materialization", attribution.PrimaryHomeOracleRecallAtK, attribution.FinalMembershipOracleRecallAtK),
		stage("global_to_final_membership_ceiling", "overlap_or_placement_membership", attribution.GlobalExactRecallAtK, attribution.FinalMembershipOracleRecallAtK),
		stage("global_exact_to_exhaustive_partition_union", "partition_membership_or_score_contract", attribution.GlobalExactRecallAtK, attribution.ExhaustivePartitionRecallAtK),
		stage("final_membership_to_exact_routing", "exact_representative_routing", attribution.FinalMembershipOracleRecallAtK, attribution.ExactRepresentativeRecallAtK),
		stage("exact_to_approximate_routing", "approximate_representative_routing", attribution.ExactRepresentativeRecallAtK, attribution.ApproximateRepresentativeRecallAtK),
		stage("approximate_routing_to_local_hnsw", "partition_local_hnsw", attribution.ApproximateRepresentativeRecallAtK, attribution.ApproximateLocalHNSWRecallAtK),
		stage("approximate_local_hnsw_to_end_to_end", "coordinator_merge_or_transport", attribution.ApproximateLocalHNSWRecallAtK, attribution.EndToEndRecallAtK),
	}
	out[1].Active = math.Abs(out[1].Delta) > epsilon
	out[3].Active = out[3].Active || !attribution.ExhaustivePartitionIDParity || !attribution.ExhaustivePartitionScoreParity
	return out
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

func m8ProductionRouterCandidateBudgetV1(assets *m8ProductionMultiGroupAssetsV1) int {
	return max(1, int(assets.status.Representatives))
}

func m8ProductionApproximateRouterCandidateBudgetV1(assets *m8ProductionMultiGroupAssetsV1, requested int) int {
	return min(max(1, requested), m8ProductionRouterCandidateBudgetV1(assets))
}

func m8ProductionRequestV1(assets *m8ProductionMultiGroupAssetsV1, query []float32, requestID string, probes, efSearch, topK int, candidateBytesLimit uint64) nativewire.VectorPartitionCoordinatorRequestV1 {
	if candidateBytesLimit == 0 {
		candidateBytesLimit = nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxCandidateBytes
	}
	return nativewire.VectorPartitionCoordinatorRequestV1{
		Version: nativewire.VectorPartitionCoordinatorVersionV1, RequestID: requestID, CancellationID: requestID + "-cancel",
		Database: "default", Catalog: "default", Collection: assets.manifest.Collection, IndexName: assets.manifest.IndexName,
		IndexDefinitionDigest: assets.manifest.IndexDefinitionDigest, Query: query, Metric: nativewire.VectorPartitionShardSearchMetricCosineV1,
		RouterMode: collections.VectorPartitionRouterModeExactV1, RouterCandidateBudget: m8ProductionRouterCandidateBudgetV1(assets), PartitionProbes: probes,
		Consistency: nativewire.VectorPartitionShardSearchConsistencySnapshotV1, StatsMode: nativewire.VectorPartitionShardSearchStatsBasicV1,
		TopK: topK, EfSearch: efSearch, DeadlineUnixNano: time.Now().Add(30 * time.Second).UnixNano(), RequestBytesLimit: 4 << 20,
		CandidateBytesLimit: candidateBytesLimit, ResponseBytesLimit: 64 << 20, MergeEntriesLimit: probes * topK,
	}
}

func m8ProductionApproximateRequestV1(assets *m8ProductionMultiGroupAssetsV1, query []float32, requestID string, probes, efSearch, topK, routerCandidates int, candidateBytesLimit uint64) nativewire.VectorPartitionCoordinatorRequestV1 {
	request := m8ProductionRequestV1(assets, query, requestID, probes, efSearch, topK, candidateBytesLimit)
	request.RouterMode = collections.VectorPartitionRouterModeApproxV1
	request.RouterCandidateBudget = m8ProductionApproximateRouterCandidateBudgetV1(assets, routerCandidates)
	return request
}

func m8ProductionWarmupRequestV1(assets *m8ProductionMultiGroupAssetsV1, query []float32, requestID string, efSearch int, cfg config) nativewire.VectorPartitionCoordinatorRequestV1 {
	probes := 1
	for _, value := range cfg.probes {
		probes = max(probes, value)
	}
	return m8ProductionApproximateRequestV1(assets, query, requestID, probes, efSearch, cfg.topK, cfg.routerCandidates, cfg.m8CoordinatorLimits.MaxCandidateBytes)
}

func m8RunUnavailableGroupV1(ctx context.Context, topology *nativewire.VectorPartitionM8ProductionMultiGroupV1, assets *m8ProductionMultiGroupAssetsV1, query64 []float64, topK int, candidateBytesLimit uint64) (m8ProductionFailureEvidenceV1, nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1) {
	evidence := topology.Evidence()
	result := m8ProductionFailureEvidenceV1{Class: "unavailable_group_endpoint"}
	if len(evidence.Groups) == 0 {
		result.Error = "topology exposed no groups"
		return result, topology.Evidence()
	}
	result.StoppedGroup = evidence.Groups[0].GroupID
	if err := topology.StopGroup(result.StoppedGroup); err != nil {
		result.Error = err.Error()
		return result, topology.Evidence()
	}
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	started := time.Now()
	response, err := topology.Coordinator().Search(requestCtx, m8ProductionRequestV1(assets, m8Query32V1(query64), "m8-unavailable-group", len(assets.manifest.Placements), 4096, topK, candidateBytesLimit))
	result.ResourceBoundary.WallClockNanos = uint64(time.Since(started))
	if err != nil {
		result.Error = err.Error()
		var coordinatorErr *nativewire.VectorPartitionCoordinatorErrorV1
		if errors.As(err, &coordinatorErr) {
			resourceRow := m8ProductionRowV1{}
			m8AccumulateProductionRowCountersV1(&resourceRow, coordinatorErr.Counters)
			result.ResourceBoundary.SelectedPartitions = int(coordinatorErr.Counters.SelectedPartitions)
			result.ResourceBoundary.EfSearch = 4096
			if coordinatorErr.Timing.TotalNanos > 0 {
				result.ResourceBoundary.WallClockNanos = coordinatorErr.Timing.TotalNanos
			}
			result.ResourceBoundary.Maxima = m8ObservedResourceMaximaV1([]m8ProductionRowV1{resourceRow})
		}
	}
	result.ReturnedNeighbors, result.ReturnedGroups = len(response.Neighbors), len(response.ProbedGroups)
	result.Passed = err != nil && result.ReturnedNeighbors == 0 && result.ReturnedGroups == 0
	// Capture topology evidence only after the stopped-group request completes.
	// The resource ledger explicitly covers this fault boundary, including any
	// higher shard-request concurrency it produces.
	return result, topology.Evidence()
}

func m8ProductionGateLedgerForReportV1(report m8ProductionReportV1) m8ProductionGateLedgerV1 {
	ledger := m8ProductionGateLedgerV1{ExhaustiveParity: "not_run", FailureHonesty: "fail", PartitionPackReachability: "fail", Recall: "fail", ProbeReduction: "fail", EndToEndQPS: "fail", TailLatency: "fail", Balance: "fail", OverlapStorage: "fail", ResourceBounds: "fail", ExistingBehavior: "pending_full_required_suites"}
	if validM8PartitionPackDiagnosticsV1(report.PackDiagnostics, report.Config.Partitions, report.Resources.PartitionLoads) {
		ledger.PartitionPackReachability = "pass"
	}
	var exhaustive []m8ProductionRowV1
	var candidates []m8ProductionRowV1
	for _, row := range report.Rows {
		if row.Status != "pass" {
			continue
		}
		if row.Probes == report.Config.Partitions {
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
			if comparison.Configured == 0 || comparison.Observed > comparison.Configured {
				ledger.ResourceBounds = "fail"
				break
			}
		}
	}
	return ledger
}

type m8ProductionResourceLimitConfigV1 struct {
	Name       string
	Configured uint64
	Unit       string
	Enforced   bool
}

type m8ProductionResourceCapsV1 struct {
	PersistentAssetBytes uint64
	PeakRSSBytes         uint64
}

func m8ExpectedResourceLimitConfigsV1(report m8ProductionReportV1, caps m8ProductionResourceCapsV1) ([]m8ProductionResourceLimitConfigV1, bool) {
	if len(report.Config.Concurrency) == 0 {
		return nil, false
	}
	coordinator := nativewire.DefaultVectorPartitionCoordinatorLimitsV1()
	coordinator.MaxCandidateBytes = m8ProductionCandidateBudgetBytesV1
	shard := nativewire.DefaultVectorPartitionShardSearchLimitsV1()
	shard.MaxCandidateBytes = m8ProductionCandidateBudgetBytesV1
	rows := append([]m8ProductionRowV1(nil), report.Rows...)
	rows = append(rows, report.UntimedBoundary.resourceRowV1(), report.Failure.ResourceBoundary.resourceRowV1())
	observed := m8ObservedResourceMaximaV1(rows)
	concurrent, concurrentErr := m8ConfiguredConcurrentShardRequestsV1(coordinator.MaxConcurrentRequests, report.Config.Concurrency)
	retries, retriesOK := m8ConfiguredAggregateTaskLimitV1(coordinator.MaxRetries, observed.Requests)
	redirects, redirectsOK := m8ConfiguredAggregateTaskLimitV1(coordinator.MaxRedirects, observed.Requests)
	rpcs, rpcsOK := m8ConfiguredRPCsV1(observed.Requests, retries, retriesOK)
	if concurrentErr != nil || !retriesOK || !redirectsOK || !rpcsOK {
		return nil, false
	}
	return []m8ProductionResourceLimitConfigV1{
		{"persistent_asset_bytes", caps.PersistentAssetBytes, "bytes", true},
		{"process_peak_rss", caps.PeakRSSBytes, "bytes", true},
		{"coordinator_selected_partitions", uint64(coordinator.MaxSelectedPartitions), "count", true},
		{"coordinator_groups", uint64(coordinator.MaxGroups), "count", true},
		{"coordinator_requests", uint64(coordinator.MaxRequests), "count", true},
		{"coordinator_concurrent_requests_across_clients", concurrent, "count", true},
		{"coordinator_rpcs_across_shard_requests", rpcs, "count", true},
		{"coordinator_retries_across_shard_requests", retries, "count", true},
		{"coordinator_redirects_across_shard_requests", redirects, "count", true},
		{"coordinator_router_candidates", uint64(coordinator.MaxRouterCandidates), "count", true},
		{"coordinator_query_bytes", uint64(coordinator.MaxQueryBytes), "bytes", true},
		{"coordinator_top_k", uint64(coordinator.MaxTopK), "count", true},
		{"coordinator_ef_search", uint64(coordinator.MaxEfSearch), "count", true},
		{"coordinator_partitions_per_request", uint64(coordinator.MaxPartitionsPerRequest), "count", true},
		{"coordinator_identity_bytes", uint64(coordinator.MaxIdentityBytes), "bytes", true},
		{"coordinator_stable_id_bytes", uint64(coordinator.MaxStableIDBytes), "bytes", true},
		{"coordinator_merge_entries", uint64(coordinator.MaxMergeEntries), "count", true},
		{"coordinator_request_bytes", coordinator.MaxRequestBytes, "bytes", true},
		{"coordinator_candidate_bytes", coordinator.MaxCandidateBytes, "bytes", true},
		{"coordinator_response_bytes", coordinator.MaxResponseBytes, "bytes", true},
		{"coordinator_wall_clock", uint64(coordinator.MaxWallClock), "nanoseconds", true},
		{"shard_dimensions", uint64(shard.MaxDimensions), "count", true},
		{"shard_query_bytes", uint64(shard.MaxQueryBytes), "bytes", true},
		{"shard_partitions", uint64(shard.MaxPartitions), "count", true},
		{"shard_top_k", uint64(shard.MaxTopK), "count", true},
		{"shard_ef_search", uint64(shard.MaxEfSearch), "count", true},
		{"shard_identity_bytes", uint64(shard.MaxIdentityBytes), "bytes", true},
		{"shard_stable_id_bytes", uint64(shard.MaxStableIDBytes), "bytes", true},
		{"shard_request_bytes", shard.MaxRequestBytes, "bytes", true},
		{"shard_candidate_bytes", shard.MaxCandidateBytes, "bytes", true},
		{"shard_response_bytes", shard.MaxResponseBytes, "bytes", true},
	}, true
}

func validM8ResourceLimitComparisonsV1(report m8ProductionReportV1, caps m8ProductionResourceCapsV1) bool {
	if report.Resources.PersistentAssetCap != caps.PersistentAssetBytes || report.Resources.PeakRSSCapBytes != caps.PeakRSSBytes {
		return false
	}
	expected, ok := m8ExpectedResourceLimitConfigsV1(report, caps)
	if !ok || len(report.Resources.LimitComparisons) != len(expected) {
		return false
	}
	want := make(map[string]m8ProductionResourceLimitConfigV1, len(expected))
	for _, comparison := range expected {
		want[comparison.Name] = comparison
	}
	observed, observedOK := m8ExpectedResourceLimitObservationsV1(report)
	if !observedOK {
		return false
	}
	for _, comparison := range report.Resources.LimitComparisons {
		expectation, ok := want[comparison.Name]
		if !ok || comparison.Configured != expectation.Configured || comparison.Unit != expectation.Unit || comparison.Enforced != expectation.Enforced {
			return false
		}
		delete(want, comparison.Name)
		if expectedObserved, ok := observed[comparison.Name]; ok && comparison.Observed != expectedObserved {
			return false
		}
		passed := comparison.Configured > 0 && comparison.Observed <= comparison.Configured
		if comparison.Name == "process_peak_rss" {
			passed = report.Resources.PeakRSSMeasured && passed
		}
		if comparison.Passed != passed {
			return false
		}
	}
	return len(want) == 0
}

// m8ExpectedResourceLimitObservationsV1 recomputes every report-observable
// limit measurement. Router-model candidate capacity is intentionally absent:
// its exact exhaustive-preflight value belongs to the immutable persisted
// model and is not serialized in the report boundary.
func m8ExpectedResourceLimitObservationsV1(report m8ProductionReportV1) (map[string]uint64, bool) {
	if len(report.RouterSessions.AfterWarmup) == 0 {
		return nil, false
	}
	rows := append([]m8ProductionRowV1(nil), report.Rows...)
	rows = append(rows, report.UntimedBoundary.resourceRowV1(), report.Failure.ResourceBoundary.resourceRowV1())
	maxima := m8ObservedResourceMaximaV1(rows)
	var probes, efSearch uint64
	for _, row := range rows {
		if row.Status == "unsupported" {
			continue
		}
		probes = max(probes, uint64(row.Probes))
		efSearch = max(efSearch, uint64(row.EfSearch))
	}
	identityBytes, ok := m8RouterSessionIdentityMaxBytesV1(report.RouterSessions)
	if !ok {
		return nil, false
	}
	peakRSS := uint64(0)
	if report.Resources.PeakRSSMeasured && report.Resources.PeakRSSBytes > 0 {
		peakRSS = uint64(report.Resources.PeakRSSBytes)
	}
	stableIDBytes := uint64(len(fmt.Sprintf("doc-%06d", max(0, report.Dataset.Vectors-1))))
	maxTotalNanos := report.UntimedBoundary.WallClockNanos
	maxTotalNanos = max(maxTotalNanos, report.Failure.ResourceBoundary.WallClockNanos)
	for _, row := range report.Rows {
		if row.Status != "unsupported" {
			maxTotalNanos = max(maxTotalNanos, row.MaxTotalNanos)
		}
	}
	return map[string]uint64{
		"persistent_asset_bytes":                         report.Resources.PersistentAssetBytes,
		"process_peak_rss":                               peakRSS,
		"coordinator_selected_partitions":                probes,
		"coordinator_groups":                             uint64(report.Config.RaftGroups),
		"coordinator_requests":                           maxima.Requests,
		"coordinator_concurrent_requests_across_clients": uint64(report.Topology.MaxConcurrentShardRequests),
		"coordinator_rpcs_across_shard_requests":         maxima.RPCs,
		"coordinator_retries_across_shard_requests":      maxima.Retries,
		"coordinator_redirects_across_shard_requests":    maxima.Redirects,
		"coordinator_router_candidates":                  report.RouterRepresentatives,
		"coordinator_query_bytes":                        uint64(report.Dataset.Dimensions * 4),
		"coordinator_top_k":                              uint64(report.Config.TopK),
		"coordinator_ef_search":                          efSearch,
		"coordinator_partitions_per_request":             maxima.ShardPartitions,
		"coordinator_identity_bytes":                     identityBytes,
		"coordinator_stable_id_bytes":                    stableIDBytes,
		"coordinator_merge_entries":                      maxima.MergeEntries,
		"coordinator_request_bytes":                      maxima.RequestBytes,
		"coordinator_candidate_bytes":                    maxima.CandidateBytes,
		"coordinator_response_bytes":                     maxima.ResponseBytes,
		"coordinator_wall_clock":                         maxTotalNanos,
		"shard_dimensions":                               uint64(report.Dataset.Dimensions),
		"shard_query_bytes":                              uint64(report.Dataset.Dimensions * 4),
		"shard_partitions":                               maxima.ShardPartitions,
		"shard_top_k":                                    uint64(report.Config.TopK),
		"shard_ef_search":                                efSearch,
		"shard_identity_bytes":                           identityBytes,
		"shard_stable_id_bytes":                          stableIDBytes,
		"shard_request_bytes":                            maxima.ShardRequestBytes,
		"shard_candidate_bytes":                          maxima.ShardCandidateBytes,
		"shard_response_bytes":                           maxima.ShardResponseBytes,
	}, true
}

func m8RouterSessionIdentityBytesV1(identity nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1) uint64 {
	return uint64(len(identity.Database) + len(identity.Catalog) + len(identity.Collection) + len(identity.IndexName) + len(identity.IndexDefinitionDigest) + len(identity.ReadySetDigest))
}

func m8RouterSessionIdentityMaxBytesV1(evidence m8ProductionRouterSessionEvidenceV1) (uint64, bool) {
	var maximum uint64
	seen := false
	for _, sessions := range [][]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{evidence.AfterWarmup, evidence.AfterMeasured} {
		for _, session := range sessions {
			maximum = max(maximum, m8RouterSessionIdentityBytesV1(session.Identity))
			seen = true
		}
	}
	return maximum, seen
}

func m8ApplyRouterSessionIdentityResourceMaxV1(resources *m8ProductionResourceEvidenceV1, evidence m8ProductionRouterSessionEvidenceV1) {
	identityBytes, ok := m8RouterSessionIdentityMaxBytesV1(evidence)
	if !ok {
		return
	}
	for i := range resources.LimitComparisons {
		comparison := &resources.LimitComparisons[i]
		if comparison.Name == "coordinator_identity_bytes" || comparison.Name == "shard_identity_bytes" {
			comparison.Observed = identityBytes
			comparison.Passed = comparison.Configured > 0 && identityBytes <= comparison.Configured
		}
	}
}

func m8ProductionGateValuesV1(ledger m8ProductionGateLedgerV1) []string {
	return []string{ledger.ExhaustiveParity, ledger.FailureHonesty, ledger.PartitionPackReachability, ledger.Recall, ledger.ProbeReduction, ledger.EndToEndQPS, ledger.TailLatency, ledger.Balance, ledger.OverlapStorage, ledger.ResourceBounds, ledger.ExistingBehavior}
}

// validM8PartitionPackDiagnosticsV1 makes the partition-local persistent graph
// topology an acceptance condition, not merely an informational artifact. A
// structurally readable older pack can still contain disconnected directed
// layer-0 components, so every configured partition must be reported exactly
// once as either a fully reachable native pack or a fully reachable V3
// native-plus-auxiliary pack.
func validM8PartitionPackDiagnosticsV1(diagnostics []m8PartitionPackDiagnosticsV1, partitions int, loads []uint64) bool {
	if partitions < 1 || len(diagnostics) != partitions || len(loads) != partitions {
		return false
	}
	seen := make([]bool, partitions)
	for _, diagnostic := range diagnostics {
		partition := int(diagnostic.PartitionID)
		if partition < 0 || partition >= partitions || seen[partition] || diagnostic.Rows == 0 ||
			diagnostic.Rows != loads[partition] || diagnostic.ReachableRows == 0 || diagnostic.ReachableRows > diagnostic.Rows ||
			diagnostic.TraversalRoots == 0 || diagnostic.TraversalRoots > diagnostic.Rows ||
			(diagnostic.ReachableRows < diagnostic.Rows && diagnostic.TraversalRoots == 1) {
			return false
		}
		nativeReachable := diagnostic.ReachableRows == diagnostic.Rows && diagnostic.TraversalRoots == 1
		auxiliaryPresent := diagnostic.AuxiliaryEdges != 0 || diagnostic.AuxiliaryCSRBytes != 0 || diagnostic.AuxiliaryMaxDegree != 0
		if auxiliaryPresent {
			maxUint64 := ^uint64(0)
			if diagnostic.Rows == maxUint64 || diagnostic.Rows+1 > maxUint64/8 || diagnostic.AuxiliaryEdges > maxUint64/4 ||
				diagnostic.TraversalRoots-1 > diagnostic.Rows-diagnostic.ReachableRows ||
				diagnostic.TraversalRoots-1 > maxUint64/2 || diagnostic.MaxLayer < 0 || len(diagnostic.RowsByLayer) == 0 ||
				diagnostic.MaxLayer != len(diagnostic.RowsByLayer)-1 || diagnostic.RowsByLayer[0] != diagnostic.Rows {
				return false
			}
			for layer := 1; layer < len(diagnostic.RowsByLayer); layer++ {
				if diagnostic.RowsByLayer[layer] == 0 || diagnostic.RowsByLayer[layer] > diagnostic.RowsByLayer[layer-1] {
					return false
				}
			}
			upperRows := uint64(0)
			if len(diagnostic.RowsByLayer) > 1 {
				upperRows = diagnostic.RowsByLayer[1]
			}
			bridgeEdges := 2 * (diagnostic.TraversalRoots - 1)
			minAnchorEdges, maxAnchorEdges := uint64(0), min(upperRows, diagnostic.Rows-diagnostic.TraversalRoots)
			if upperRows > diagnostic.TraversalRoots {
				minAnchorEdges = upperRows - diagnostic.TraversalRoots
			}
			if diagnostic.MaxLayer > 0 {
				maxAnchorEdges = min(maxAnchorEdges, upperRows-1)
			}
			if diagnostic.AuxiliaryEdges < bridgeEdges {
				return false
			}
			anchorEdges := diagnostic.AuxiliaryEdges - bridgeEdges
			const auxiliaryBranchV1 = uint64(8)
			expectedMaxDegree := min(diagnostic.TraversalRoots-1, auxiliaryBranchV1)
			if diagnostic.TraversalRoots > 2*auxiliaryBranchV1 {
				expectedMaxDegree++
			}
			if anchorEdges > 0 {
				expectedMaxDegree = max(expectedMaxDegree, 1)
			}
			if anchorEdges < minAnchorEdges || anchorEdges > maxAnchorEdges ||
				diagnostic.AuxiliaryMaxDegree != expectedMaxDegree ||
				(diagnostic.AuxiliaryMaxDegree != 0 && (diagnostic.Rows > maxUint64/diagnostic.AuxiliaryMaxDegree ||
					diagnostic.AuxiliaryEdges > diagnostic.Rows*diagnostic.AuxiliaryMaxDegree)) {
				return false
			}
			offsetBytes, edgeBytes := (diagnostic.Rows+1)*8, diagnostic.AuxiliaryEdges*4
			if edgeBytes > maxUint64-offsetBytes || diagnostic.AuxiliaryCSRBytes != offsetBytes+edgeBytes ||
				diagnostic.CombinedReachableRows != diagnostic.Rows || diagnostic.AuxiliaryMaxDegree > 9 ||
				(diagnostic.AuxiliaryEdges == 0) != (diagnostic.AuxiliaryMaxDegree == 0) {
				return false
			}
		} else if !nativeReachable || (diagnostic.CombinedReachableRows != 0 && diagnostic.CombinedReachableRows != diagnostic.Rows) {
			return false
		}
		seen[partition] = true
	}
	return true
}

// validM8PartitionLoadsV1 binds diagnostic row counts to manifest-derived
// evidence. Retained variants have an independently validated descriptor; for
// non-variant runs the manifest-derived total must still account for every
// fixture row and declared overlap membership, rather than accepting a small
// self-consistent fabricated subset.
func validM8PartitionLoadsV1(report m8ProductionReportV1) bool {
	loads := report.Resources.PartitionLoads
	if report.Config.Partitions < 1 || len(loads) != report.Config.Partitions || report.Dataset.Vectors < 1 || report.Resources.OverlapMemberships < 0 {
		return false
	}
	expected := uint64(report.Dataset.Vectors)
	overlap := uint64(report.Resources.OverlapMemberships)
	if overlap > ^uint64(0)-expected {
		return false
	}
	expected += overlap
	var total uint64
	for _, load := range loads {
		if load == 0 || load > ^uint64(0)-total {
			return false
		}
		total += load
	}
	if total != expected {
		return false
	}
	if report.Variant == nil {
		return true
	}
	if len(report.Variant.PartitionLoads) != len(loads) {
		return false
	}
	for partition, load := range loads {
		if report.Variant.PartitionLoads[partition] < 0 || load != uint64(report.Variant.PartitionLoads[partition]) {
			return false
		}
	}
	return true
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
	return float64(m8IDHitCountV1(want, got)) / float64(len(want))
}

func m8IDHitCountV1(want, got []string) int {
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
	return found
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
	index, ok := m8NearestRankPercentileIndexV1(uint64(len(ordered)), uint64(percentile))
	if !ok {
		return 0
	}
	return ordered[index]
}

func m8NearestRankPercentileIndexV1(samples, percentile uint64) (uint64, bool) {
	if samples == 0 || percentile == 0 || percentile > 100 {
		return 0, false
	}
	high, low := bits.Mul64(samples, percentile)
	quotient, remainder := bits.Div64(high, low, 100)
	if remainder != 0 {
		quotient++
	}
	if quotient == 0 {
		return 0, false
	}
	return quotient - 1, true
}

// m8PercentileAggregateElapsedLowerBoundV1 derives the least possible total
// request duration consistent with the retained nearest-rank percentiles.
func m8PercentileAggregateElapsedLowerBoundV1(samples, concurrency int, p50, p95, p99, maximum uint64) (uint64, bool) {
	if samples < 1 || concurrency < 1 {
		return 0, false
	}
	n := uint64(samples)
	i50, ok50 := m8NearestRankPercentileIndexV1(n, 50)
	i95, ok95 := m8NearestRankPercentileIndexV1(n, 95)
	i99, ok99 := m8NearestRankPercentileIndexV1(n, 99)
	if !ok50 || !ok95 || !ok99 || i50 > i95 || i95 > i99 || i99 >= n {
		return 0, false
	}
	var total uint64
	addProduct := func(value, count uint64) bool {
		high, low := bits.Mul64(value, count)
		if high != 0 {
			return false
		}
		next, carry := bits.Add64(total, low, 0)
		if carry != 0 {
			return false
		}
		total = next
		return true
	}
	if !addProduct(p50, i95-i50) || !addProduct(p95, i99-i95) || !addProduct(p99, n-1-i99) || !addProduct(maximum, 1) {
		return 0, false
	}
	workers := uint64(concurrency)
	if workers > n {
		workers = n
	}
	minimum := total / workers
	if total%workers != 0 {
		if minimum == ^uint64(0) {
			return 0, false
		}
		minimum++
	}
	return minimum, true
}

func m8GitDirtyV1(ignoredPaths ...string) bool {
	return m8GitDirtyInV1("", ignoredPaths...)
}

func m8GitDirtyInV1(workDir string, ignoredPaths ...string) bool {
	rootCommand := exec.Command("git", "rev-parse", "--show-toplevel")
	rootCommand.Dir = workDir
	rootRaw, err := rootCommand.Output()
	if err != nil {
		return true
	}
	root, err := m8CanonicalPathV1(strings.TrimSpace(string(rootRaw)))
	if err != nil {
		return true
	}
	args := []string{"status", "--porcelain", "--untracked-files=normal", "--", "."}
	for _, path := range ignoredPaths {
		if path == "" {
			continue
		}
		if !filepath.IsAbs(path) && workDir != "" {
			path = filepath.Join(workDir, path)
		}
		absolute, err := m8CanonicalPathV1(path)
		if err != nil {
			return true
		}
		relative, err := filepath.Rel(root, absolute)
		if err != nil {
			return true
		}
		if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		// An ignored subtree inside the repository can hide modified tracked
		// source or unrelated pre-existing untracked files. Evidence roots must
		// therefore live outside the source repository; external roots never
		// appear in this repository-scoped status command and need no pathspec.
		return true
	}
	command := exec.Command("git", args...)
	command.Dir = root
	raw, err := command.Output()
	return err != nil || strings.TrimSpace(string(raw)) != ""
}

func m8CanonicalPathV1(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	current := absolute
	var suffix []string
	for {
		resolved, resolveErr := filepath.EvalSymlinks(current)
		if resolveErr == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", resolveErr
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

type m8ProductionProfileVerifierV1 func(m8ProductionProfileEvidenceV1) bool

func validateM8ProductionReportV1(report m8ProductionReportV1, caps m8ProductionResourceCapsV1) error {
	return validateM8ProductionReportWithProfilesV1(report, caps, validM8ProductionProfilesV1)
}

// validateM8ProductionReportWithProfilesV1 keeps the production report
// validator bound to semantic profile validation while allowing qualification
// evidence tests to use their existing unexported verifier seam.
func validateM8ProductionReportWithProfilesV1(report m8ProductionReportV1, caps m8ProductionResourceCapsV1, profileVerifier m8ProductionProfileVerifierV1) error {
	if profileVerifier == nil {
		return errors.New("M8 profile verifier is required")
	}
	if report.SchemaVersion != 4 || report.ResultKind != "m8_production_multi_group_evidence_v4" ||
		report.Mode != m8ProductionMultiGroupModeV1 || !report.ProductionEvidence ||
		report.GeneratedAt.IsZero() || !validM8ProductionExecutionIDV1(report.ExecutionID) || len(report.Command) == 0 || !m8QualificationSHA256V1(report.ExecutableSHA256) || !validSHA(report.BaseSHA) || !validSHA(report.HeadSHA) ||
		report.GoVersion == "" || report.GOOS == "" || report.GOARCH == "" || report.LogicalCPUs < 1 || report.GOMAXPROCS < 1 || report.GoMemoryLimitBytes < 1 ||
		report.Config.RaftGroups < 2 || report.Config.RaftNodesPerGroup != 3 || report.Config.Partitions < 4 || report.Config.Partitions > maxPartitions ||
		report.Config.Warmup < 0 || report.Config.RouterCandidates < 1 || report.RouterRepresentatives == 0 || report.BuildNanos <= 0 || report.TimedBoundary == "" || len(report.Limitations) == 0 {
		return errors.New("missing or invalid M8 identity, topology, or timing metadata")
	}
	expectedWarmup, _ := m8WarmupCountAndConcurrencyV1(config{warmup: report.Config.Warmup, concurrency: report.Config.Concurrency})
	if report.Config.EffectiveWarmup != expectedWarmup {
		return errors.New("invalid M8 effective warmup count")
	}
	if err := validateM3FixtureWithCaps(report.Dataset, maxVectors, maxFixtureBytes); err != nil {
		return fmt.Errorf("dataset: %w", err)
	}
	if !validM8TruthCacheEvidenceV1(report.TruthCache, report.Dataset, report.Config.TopK) {
		return errors.New("M8 canonical truth-cache evidence is not identity-bound")
	}
	if report.Variant != nil {
		if err := validateM3VariantDescriptorV1(*report.Variant); err != nil || len(report.Config.Overlap) != 1 ||
			report.Config.Overlap[0] != report.Variant.OverlapRatio || report.Variant.FixtureChecksum != report.Dataset.Checksum ||
			report.Variant.RouterRepresentatives != report.RouterRepresentatives ||
			uint64(report.Variant.Partitions) != uint64(report.Config.Partitions) || report.Variant.PersistentAssetBytes != report.Resources.PersistentAssetBytes ||
			!m8RouterSessionsMatchVariantV1(report.RouterSessions, *report.Variant, report.Topology.ReadySetDigest) {
			return errors.New("M8 report variant identity is not bound to its configuration and resources")
		}
	}
	if len(report.Topology.Groups) != report.Config.RaftGroups || report.Topology.Network != "tcp_loopback_serialized_m5_v1" ||
		report.Topology.LifecycleState != "active" || !m8SHA256V1(report.Topology.ReadySetDigest) || report.Topology.MetaGroup == "" || report.Topology.MetaLeader == "" || len(report.Topology.MetaNodes) != 3 || report.Topology.MaxConcurrentShardRequests == 0 {
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
	if err := validateM8ProductionMeasurementCellsV1(report.Config, report.Rows); err != nil {
		return err
	}
	if !validM8PartitionLoadsV1(report) || !validM8PartitionPackDiagnosticsV1(report.PackDiagnostics, report.Config.Partitions, report.Resources.PartitionLoads) || report.GateLedger.PartitionPackReachability != "pass" {
		return errors.New("M8 report has incomplete or unreachable partition-pack diagnostics")
	}
	var measuredSamples uint64
	for _, row := range report.Rows {
		if row.Status == "unsupported" {
			if row.UnsupportedReason == "" || row.Overlap == 0 {
				return errors.New("malformed unsupported M8 row")
			}
			continue
		}
		if report.Variant != nil && row.VariantID != report.Variant.VariantID {
			return errors.New("M8 row variant identity mismatch")
		}
		expectedLocalSearches, ok := m8ExpectedLocalSearchesV1(row.Samples, row.Probes)
		if !ok {
			return errors.New("M8 local search count overflow")
		}
		if row.ElapsedNanos < row.MaxTotalNanos {
			return errors.New("M8 cell elapsed is shorter than its slowest request")
		}
		if row.Status == "candidate_coverage_shortfall" {
			if row.Probes < 1 || row.Probes > report.Config.Partitions ||
				row.EfSearch < report.Config.TopK || row.Concurrency < 1 || row.Samples != report.Dataset.Queries ||
				row.RouterMode != collections.VectorPartitionRouterModeApproxV1 || row.RouterCandidates < row.Probes || row.RouterCandidates > report.Config.RouterCandidates || row.RouterCandidates != row.Attribution.ApproximateRouterCandidateBudget || row.NoPartialResults || row.ExactParityChecked || row.ExactParityPassed ||
				row.RecallAtK != 0 || row.QPS != 0 || row.ElapsedNanos == 0 || row.P50Nanos != 0 || row.P95Nanos != 0 || row.P99Nanos != 0 || row.MaxTotalNanos == 0 ||
				row.Attribution.LocalHNSWSearches != expectedLocalSearches || row.Attribution.LocalHNSWCandidates == 0 ||
				row.Attribution.ApproximateRouterPartitionCoverageComplete || row.Attribution.ApproximateRepresentativeRecallAtK != 0 || row.Attribution.ApproximateLocalHNSWRecallAtK != 0 || row.Attribution.ApproximateLocalHNSWSearches != 0 || row.Attribution.ApproximateLocalHNSWCandidates != 0 || row.Attribution.ApproximateLocalHNSWEdges != 0 || row.Attribution.EndToEndRecallAtK != 0 ||
				row.Attribution.CoordinatorMergeIDParity || row.Attribution.CoordinatorMergeScoreParity || !validM8AttributionV1(row.Attribution, report.Config.TopK) {
				return errors.New("malformed M8 candidate-coverage shortfall row")
			}
			rowSamples := uint64(row.Samples)
			if rowSamples > ^uint64(0)-measuredSamples {
				return errors.New("M8 measured sample count overflow")
			}
			measuredSamples += rowSamples
			continue
		}
		expectedQPS, qpsOK := m8ProductionQPSV1(row.Samples, row.ElapsedNanos)
		if row.Status != "pass" && row.Status != "fail" || row.Probes < 1 || row.Probes > report.Config.Partitions ||
			row.EfSearch < report.Config.TopK || row.Concurrency < 1 || row.Samples != report.Dataset.Queries || math.IsNaN(row.QPS) || math.IsInf(row.QPS, 0) || row.QPS <= 0 ||
			!qpsOK || math.Float64bits(row.QPS) != math.Float64bits(expectedQPS) ||
			row.P50Nanos == 0 || row.P50Nanos > row.P95Nanos || row.P95Nanos > row.P99Nanos || row.P99Nanos > row.MaxTotalNanos ||
			row.RouterMode != collections.VectorPartitionRouterModeApproxV1 || row.RouterCandidates < row.Probes || row.RouterCandidates > report.Config.RouterCandidates || row.RouterCandidates != row.Attribution.ApproximateRouterCandidateBudget || !row.Attribution.ApproximateRouterPartitionCoverageComplete || row.ExactParityPassed && !row.ExactParityChecked || row.ExactParityChecked && row.Probes != report.Config.Partitions || !row.NoPartialResults ||
			math.Float64bits(row.RecallAtK) != math.Float64bits(row.Attribution.EndToEndRecallAtK) || row.Attribution.LocalHNSWSearches != expectedLocalSearches || row.Attribution.LocalHNSWCandidates == 0 || row.Attribution.ApproximateLocalHNSWSearches != expectedLocalSearches || row.Attribution.ApproximateLocalHNSWCandidates == 0 ||
			!validM8AttributionV1(row.Attribution, report.Config.TopK) {
			return errors.New("malformed measured M8 row")
		}
		minimumElapsed, ok := m8PercentileAggregateElapsedLowerBoundV1(row.Samples, row.Concurrency, row.P50Nanos, row.P95Nanos, row.P99Nanos, row.MaxTotalNanos)
		if !ok {
			return errors.New("M8 cell percentile aggregate lower bound overflows")
		}
		if row.ElapsedNanos < minimumElapsed {
			return errors.New("M8 cell elapsed is shorter than its percentile-derived aggregate lower bound")
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
	if !validM8ResourceLimitComparisonsV1(report, caps) {
		return errors.New("incomplete or forged M8 resource-limit evidence")
	}
	if !report.Failure.Passed || report.Failure.Error == "" || report.Failure.ReturnedNeighbors != 0 || report.Failure.ReturnedGroups != 0 ||
		report.UntimedBoundary.SelectedPartitions != report.Config.Partitions || report.UntimedBoundary.EfSearch < report.Config.TopK ||
		report.UntimedBoundary.WallClockNanos == 0 || report.UntimedBoundary.Maxima.Requests == 0 ||
		report.UntimedBoundary.Maxima.RPCs == 0 || report.UntimedBoundary.Maxima.RequestBytes == 0 ||
		report.UntimedBoundary.Maxima.ShardPartitions == 0 || report.UntimedBoundary.Maxima.ShardRequestBytes == 0 ||
		report.Failure.ResourceBoundary.SelectedPartitions != report.Config.Partitions || report.Failure.ResourceBoundary.EfSearch != 4096 ||
		report.Failure.ResourceBoundary.WallClockNanos == 0 || report.Failure.ResourceBoundary.Maxima.Requests == 0 ||
		report.Failure.ResourceBoundary.Maxima.RPCs == 0 || report.Failure.ResourceBoundary.Maxima.RequestBytes == 0 ||
		report.Failure.ResourceBoundary.Maxima.ShardPartitions == 0 || report.Failure.ResourceBoundary.Maxima.ShardRequestBytes == 0 ||
		report.GateLedger.FailureHonesty != "pass" || report.Resources.PersistentAssetBytes == 0 ||
		report.Resources.PeakRSSMeasured && report.Resources.PeakRSSScope != m8PeakRSSScopeV1 {
		return errors.New("incomplete M8 failure or resource evidence")
	}
	if !profileVerifier(report.Profiles) {
		return errors.New("incomplete M8 profile evidence")
	}
	if !validM8ProductionMeasurementTranscriptV1(report) {
		return errors.New("incomplete M8 measurement transcript evidence")
	}
	if report.Profiles.Status == "captured_production_query_and_fault_boundary" {
		want, err := m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, report.Profiles.Artifacts, report.MeasurementTranscript.SHA256)
		if err != nil || report.ExecutionEvidenceDigest != want {
			return errors.New("M8 execution identity is not bound to profile artifacts")
		}
	} else if report.ExecutionEvidenceDigest != "" {
		return errors.New("M8 uncaptured profiles have execution evidence digest")
	}
	return nil
}

func validM8ProductionProfilesV1(profiles m8ProductionProfileEvidenceV1) bool {
	if profiles.Status == "" || profiles.Status == "not_captured" {
		return len(profiles.Captured) == 0 && len(profiles.Artifacts) == 0
	}
	if profiles.Status != "captured_production_query_and_fault_boundary" || profiles.Directory == "" || profiles.Scope == "" || len(profiles.Captured) != len(m8ProfileArtifactNamesV1) || len(profiles.Artifacts) != len(m8ProfileArtifactNamesV1) || !filepath.IsAbs(profiles.Directory) {
		return false
	}
	directory, err := m8CanonicalPathV1(profiles.Directory)
	if err != nil || directory != profiles.Directory {
		return false
	}
	actual, err := m8ProfileArtifactsV1(profiles.Captured)
	if err != nil {
		return false
	}
	expectedNames, captured, retained := make(map[string]bool, len(m8ProfileArtifactNamesV1)), make(map[string]bool, len(profiles.Captured)), make(map[string]m8ProductionProfileArtifactV1, len(profiles.Artifacts))
	for _, name := range m8ProfileArtifactNamesV1 {
		expectedNames[name] = true
	}
	for _, path := range profiles.Captured {
		if !filepath.IsAbs(path) || filepath.Dir(path) != directory || captured[path] {
			return false
		}
		captured[path] = true
	}
	for _, artifact := range profiles.Artifacts {
		if artifact.Path == "" || retained[artifact.Path].Path != "" || artifact.Bytes <= 0 || !m8QualificationSHA256V1(artifact.SHA256) {
			return false
		}
		retained[artifact.Path] = artifact
	}
	for _, artifact := range actual {
		if !expectedNames[filepath.Base(artifact.Path)] || !captured[artifact.Path] || retained[artifact.Path] != artifact {
			return false
		}
		delete(expectedNames, filepath.Base(artifact.Path))
	}
	return len(expectedNames) == 0
}

type m8ProductionMeasurementCellKeyV1 struct {
	overlap     uint64
	probes      int
	efSearch    int
	concurrency int
}

func validateM8ProductionMeasurementCellsV1(cfg m8ProductionConfigEvidenceV1, rows []m8ProductionRowV1) error {
	if len(cfg.Probes) == 0 || len(cfg.EfSearch) == 0 || len(cfg.Concurrency) == 0 || len(cfg.Overlap) == 0 ||
		!allUnique(cfg.Probes) || !allUnique(cfg.EfSearch) || !allUnique(cfg.Concurrency) || !allUnique(cfg.Overlap) {
		return errors.New("M8 measurement axes must be non-empty and unique")
	}
	for _, probes := range cfg.Probes {
		if probes < 1 || probes > cfg.Partitions || probes > cfg.RouterCandidates {
			return errors.New("M8 configured probe axis is invalid")
		}
	}
	for _, efSearch := range cfg.EfSearch {
		if efSearch < cfg.TopK || efSearch > nativewire.DefaultVectorPartitionShardSearchLimitsV1().MaxEfSearch {
			return errors.New("M8 configured ef-search axis is invalid")
		}
	}
	for _, concurrency := range cfg.Concurrency {
		if concurrency < 1 || concurrency > 256 {
			return errors.New("M8 configured concurrency axis is invalid")
		}
	}
	for _, overlap := range cfg.Overlap {
		if math.IsNaN(overlap) || math.IsInf(overlap, 0) || overlap < 0 || overlap > 1 {
			return errors.New("M8 configured overlap axis is invalid")
		}
	}
	expected := 1
	for _, axis := range []int{len(cfg.Overlap), len(cfg.Probes), len(cfg.EfSearch), len(cfg.Concurrency)} {
		if expected > math.MaxInt/axis {
			return errors.New("M8 measurement cell count overflow")
		}
		expected *= axis
	}
	if len(rows) != expected {
		return errors.New("M8 rows do not exactly cover configured measurement cells")
	}
	configured := make(map[m8ProductionMeasurementCellKeyV1]struct{}, expected)
	for _, overlap := range cfg.Overlap {
		for _, probes := range cfg.Probes {
			for _, efSearch := range cfg.EfSearch {
				for _, concurrency := range cfg.Concurrency {
					configured[m8ProductionMeasurementCellKeyV1{math.Float64bits(overlap), probes, efSearch, concurrency}] = struct{}{}
				}
			}
		}
	}
	for _, row := range rows {
		key := m8ProductionMeasurementCellKeyV1{math.Float64bits(row.Overlap), row.Probes, row.EfSearch, row.Concurrency}
		if _, ok := configured[key]; !ok {
			return errors.New("M8 row uses an unconfigured measurement cell")
		}
		delete(configured, key)
	}
	if len(configured) != 0 {
		return errors.New("M8 rows omit configured measurement cells")
	}
	return nil
}

func validM8TruthCacheEvidenceV1(evidence m8TruthCacheEvidenceV1, fixture fixtureManifest, topK int) bool {
	if evidence.Identity != m8TruthCacheIdentityV1(fixture, topK) {
		return false
	}
	switch evidence.Status {
	case "computed":
		return evidence.ComputeNanos > 0 && evidence.LoadNanos == 0 &&
			(evidence.ArtifactSHA256 == "" || m8SHA256V1(evidence.ArtifactSHA256))
	case "reused":
		return evidence.ComputeNanos == 0 && evidence.LoadNanos > 0 && m8SHA256V1(evidence.ArtifactSHA256)
	default:
		return false
	}
}

func m8ExpectedLocalSearchesV1(samples, probes int) (uint64, bool) {
	if samples < 1 || probes < 1 || uint64(samples) > ^uint64(0)/uint64(probes) {
		return 0, false
	}
	return uint64(samples) * uint64(probes), true
}

func validM8AttributionV1(attribution m8ProductionAttributionV1, topK int) bool {
	if attribution.Contract != m8CanonicalResultContractV1 || attribution.GlobalExactRecallAtK != 1 ||
		attribution.ApproximateRouterCandidateBudget < 1 ||
		(!attribution.ApproximateRouterPartitionCoverageComplete && (attribution.ApproximateRepresentativeRecallAtK != 0 || attribution.ApproximateLocalHNSWRecallAtK != 0 || attribution.ApproximateLocalHNSWSearches != 0 || attribution.ApproximateLocalHNSWCandidates != 0 || attribution.ApproximateLocalHNSWEdges != 0)) ||
		!slices.Equal(attribution.ResidualLossOwners, m8AttributionLossOwnersV1(attribution)) || !slices.Equal(attribution.StageOwners, m8AttributionStageOwnersV1(attribution)) {
		return false
	}
	for _, recall := range []float64{
		attribution.ExhaustivePartitionRecallAtK,
		attribution.ExactRepresentativeRecallAtK,
		attribution.ApproximateRepresentativeRecallAtK,
		attribution.LocalHNSWRecallAtK,
		attribution.ApproximateLocalHNSWRecallAtK,
		attribution.EndToEndRecallAtK,
	} {
		if math.IsNaN(recall) || math.IsInf(recall, 0) || recall < 0 || recall > 1 {
			return false
		}
	}
	if attribution.OracleStagesComplete {
		for _, value := range []float64{attribution.PrimaryHomeOracleRecallAtK, attribution.FinalMembershipOracleRecallAtK, attribution.PrimaryHomeOracleRegretAtK, attribution.FinalMembershipOracleRegretAtK, attribution.PrimaryToFinalMembershipGainAtK, attribution.FinalMembershipToExactLossAtK, attribution.ExactToLocalHNSWLossAtK, attribution.ApproximateToLocalHNSWLossAtK, attribution.ApproximateLocalToEndToEndLossAtK, attribution.ExactRepresentativeTruthHomeCoverageAtK, attribution.TruthNeighborHomePairColocationAtK, attribution.ExactRepresentativeFinalMembershipCoverageAtK, attribution.TruthNeighborFinalMembershipPairColocationAtK, attribution.ExactRepresentativeOverlapTruthContributionAtK, attribution.ExactRepresentativeDuplicateMembershipCoverageAtK} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < -1e-12 || value > 1 {
				return false
			}
		}
		if topK < 1 || len(attribution.TruthNeighborRankRetentionAtK) != topK {
			return false
		}
		for _, value := range attribution.TruthNeighborRankRetentionAtK {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
				return false
			}
		}
		if math.IsNaN(attribution.ExactToApproximateLossAtK) || math.IsInf(attribution.ExactToApproximateLossAtK, 0) || attribution.ExactToApproximateLossAtK < -1 || attribution.ExactToApproximateLossAtK > 1 {
			return false
		}
		near := func(a, b float64) bool { return math.Abs(a-b) <= 1e-12 }
		if attribution.FinalMembershipOracleRecallAtK+1e-12 < attribution.PrimaryHomeOracleRecallAtK || attribution.FinalMembershipOracleRecallAtK+1e-12 < attribution.ExactRepresentativeRecallAtK || attribution.ExactRepresentativeRecallAtK+1e-12 < attribution.LocalHNSWRecallAtK || attribution.ApproximateRepresentativeRecallAtK+1e-12 < attribution.ApproximateLocalHNSWRecallAtK || !near(attribution.PrimaryHomeOracleRegretAtK, 1-attribution.PrimaryHomeOracleRecallAtK) || !near(attribution.FinalMembershipOracleRegretAtK, 1-attribution.FinalMembershipOracleRecallAtK) || !near(attribution.PrimaryToFinalMembershipGainAtK, attribution.FinalMembershipOracleRecallAtK-attribution.PrimaryHomeOracleRecallAtK) || !near(attribution.FinalMembershipToExactLossAtK, attribution.FinalMembershipOracleRecallAtK-attribution.ExactRepresentativeRecallAtK) || !near(attribution.ExactToApproximateLossAtK, attribution.ExactRepresentativeRecallAtK-attribution.ApproximateRepresentativeRecallAtK) || !near(attribution.ExactToLocalHNSWLossAtK, attribution.ExactRepresentativeRecallAtK-attribution.LocalHNSWRecallAtK) || !near(attribution.ApproximateToLocalHNSWLossAtK, attribution.ApproximateRepresentativeRecallAtK-attribution.ApproximateLocalHNSWRecallAtK) || !near(attribution.ApproximateLocalToEndToEndLossAtK, attribution.ApproximateLocalHNSWRecallAtK-attribution.EndToEndRecallAtK) {
			return false
		}
	}
	return true
}

func validM8RouterSessionEvidenceV1(evidence m8ProductionRouterSessionEvidenceV1, expectedMeasuredSamples uint64) bool {
	if len(evidence.BeforeWarmup) != 0 || len(evidence.AfterWarmup) == 0 || len(evidence.AfterMeasured) == 0 {
		return false
	}
	if _, ok := m8CanonicalRouterSessionIdentityV1(evidence); !ok {
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

func m8CanonicalRouterSessionIdentityV1(evidence m8ProductionRouterSessionEvidenceV1) (nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1, bool) {
	if len(evidence.AfterWarmup) == 0 || len(evidence.AfterMeasured) == 0 {
		return nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{}, false
	}
	identity := evidence.AfterWarmup[0].Identity
	for _, sessions := range [][]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{evidence.AfterWarmup, evidence.AfterMeasured} {
		for _, session := range sessions {
			if session.Identity != identity {
				return nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{}, false
			}
		}
	}
	return identity, true
}

func m8RouterSessionsMatchVariantV1(evidence m8ProductionRouterSessionEvidenceV1, variant m3VariantDescriptorV1, readySetDigest string) bool {
	for _, sessions := range [][]nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{evidence.AfterWarmup, evidence.AfterMeasured} {
		for _, session := range sessions {
			if !m8RouterSessionIdentityMatchesVariantV1(session.Identity, variant) || session.Identity.ReadySetDigest != readySetDigest {
				return false
			}
		}
	}
	return true
}

func m8RouterSessionIdentityMatchesVariantV1(identity nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1, variant m3VariantDescriptorV1) bool {
	return identity.IndexDefinitionDigest == variant.IndexDefinitionDigest &&
		identity.SourceGeneration == variant.SourceGeneration && identity.SourceChecksum == variant.SourceChecksum &&
		identity.SourceSchemaHash == variant.SourceSchemaHash && identity.SourceRowCount == variant.SourceRows &&
		identity.PartitionGeneration == variant.PartitionGeneration &&
		identity.RouterModelDigest == variant.RouterModelDigest
}

func m8SHA256V1(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
