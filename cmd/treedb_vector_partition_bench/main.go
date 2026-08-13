// treedb_vector_partition_bench is the pre-alpha deterministic oracle and
// bounded qualification harness. Its system subcommands use the public
// OperationsV1 TCP boundary and the production Raft topology; the older M8
// loopback remains separate and cannot be selected by system-node configs.
package main

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

const (
	schemaVersion                           = 1
	maxVectors                              = 1_000_000
	maxDimensions                           = 4_096
	maxPartitions                           = 16_384
	kahipMaxDirectedEdges             int64 = 16_000_000
	kahipAdapterMaxBytes                    = 64 << 10
	kahipAdapterSHA256                      = "ae4ca8f5f26bd510a507a0f4ba50adaf1e5514ee9e20340cb9d494aba8f54825"
	kahipDefaultTimeout                     = 5 * time.Minute
	kahipMinSeed                      int64 = -1 << 31
	kahipMaxSeed                      int64 = 1<<31 - 1
	maxFixtureBytes                   int64 = 4 << 30
	maxBenchmarkWorkUnits             int64 = 200_000_000
	maxManifestBytes                  int64 = 64 << 10
	maxGitHubEventBytes               int64 = 2 << 20
	partitionHNSWIndex                      = "embedding_graph"
	partitionHNSWDegree                     = 16
	maxSourceHNSWDegree                     = partitionHNSWDegree
	maxPartitionLocalHNSWM                  = 32
	partitionHNSWDefaultEfC                 = 128
	maxPartitionHNSWEfC                     = 4_096
	fixtureGenerator                        = "treedb_vector_partition_fixture_v2"
	qualificationSyntheticGeneratorV1       = "treedb_vector_partition_high_entropy_synthetic_v1"
	qualificationEmbeddingGeneratorV1       = "treedb_vector_partition_embedding_mixture_v1"
	fixtureArithmetic                       = "ieee754_binary64_explicit_fma_v1"
	documentIDStorageBytes                  = 16
	hnswJSONFloatBytes                      = 24
	hnswJSONFixedBytes                      = 64
	hnswDecodedDimensionBytes               = 32
	memoryMapEntryBytes                     = 64
	vectorPartitionInsertBatchRows          = 8_192
	memorySlackNumerator                    = 5
	memorySlackDenominator                  = 4
	memoryBudgetScope                       = "modeled_peak_live_bytes_v1: contiguous generated float64 fixture/query matrices and row headers; exact/selected top-k candidates; representative routing; persisted router ingest JSON/IDs/slices and source-row capture; HNSW partition JSON plus decoded JSON/vector batch; HNSW query merge and cache; 25% allocation slack; excludes TreeDB engine/index internals, Go runtime/GC metadata, and artifact/CLI encoding"
	benchmarkWorkScope                      = "benchmark_owned_vector_query_corpus_visits_v1: checksum exact truth once; mandatory truth plus enabled exhaustive/routing corpus passes for every probe/overlap row; excludes TreeDB HNSW engine-internal search work"
	m8BenchmarkWorkScope                    = "m8_query_passes_v1: measured coordinator requests, warmup/endpoint preflight, cached exhaustive attribution, and exact/approximate/local-HNSW attribution passes; excludes the pre-existing canonical source oracle and engine-internal HNSW work"
	partitionAssignmentGraphV1              = "graph"
	partitionAssignmentStableIDHashV1       = "stable_id_hash"
	// The canonical M8 corpus permits up to maxVectors primary memberships and
	// up to one full duplicate membership per source row. At 64 conservative
	// candidate bytes per membership, 128 MiB keeps that complete comparison
	// bounded while allowing the required 1M + 20% overlap preflight to run.
	m8ProductionCandidateBudgetBytesV1 uint64 = 128 << 20
)

type config struct {
	dataset               string
	partitions            int
	probes                []int
	overlaps              []float64
	topK                  int
	recallTarget          float64
	seed                  int64
	format                string
	out                   string
	stages                map[string]bool
	command               []string
	maxVectors            int
	maxBytes              int64
	baseSHA               string
	headSHA               string
	sourceCheckout        string
	m3BuildDirty          bool
	hnsw                  *treeDBPartitionHNSW
	memory                benchmarkMemoryPlan
	stage                 string
	m3PersistDir          string
	m8ExistingDB          string
	m8VariantDBs          []string
	partitionAssignment   string
	partitionTruthOracle  bool
	kahipPython           string
	kahipPythonSHA256     string
	kahipScript           string
	kahipAdapterSHA256    string
	kahipSource           string
	kahipTimeout          time.Duration
	partition             vectorpartition.Config
	partitionHNSWM        int
	partitionHNSWEfC      int
	router                *treeDBRepresentativeRouter
	coordinator           *m6CoordinatorHarnessV1
	routerConfig          vectorpartition.RouterConfigV1
	routerCandidates      int
	sourceHNSWDegree      int
	mode                  string
	raftGroups            int
	raftNodes             int
	concurrency           []int
	warmup                int
	profiles              string
	m8MatrixOut           string
	m8MatrixProfiles      string
	efSearch              []int
	m8MaxRSSBytes         uint64
	m8MaxAssetBytes       uint64
	m8MaxExactTruthVisits int64
	m8TruthCache          string
	m8TruthCacheSHA256    string
	m3MaxBenchmarkVisits  int64
	m8CoordinatorLimits   nativewire.VectorPartitionCoordinatorLimitsV1
	m8ShardLimits         nativewire.VectorPartitionShardSearchLimitsV1
}

type kahipRequestPartitioner struct{}

func (kahipRequestPartitioner) Name() string    { return "kahip_request_seed_v1" }
func (kahipRequestPartitioner) License() string { return "request-only" }
func (kahipRequestPartitioner) Partition(g vectorpartition.Graph, parts, cap int) ([]int, error) {
	if parts < 1 || cap < 1 || len(g.Neighbors) < parts {
		return nil, errors.New("invalid KaHIP request shape")
	}
	out := make([]int, len(g.Neighbors))
	for i := range out {
		out[i] = i % parts
	}
	return out, nil
}

type partitionRun struct {
	SchemaVersion  int                     `json:"schema_version"`
	ResultKind     string                  `json:"result_kind"`
	Dataset        fixtureManifest         `json:"dataset"`
	Source         vectorpartition.Source  `json:"source"`
	Config         vectorpartition.Config  `json:"config"`
	Metrics        vectorpartition.Metrics `json:"metrics"`
	BuildNanos     int64                   `json:"build_nanos"`
	ArtifactSHA256 string                  `json:"artifact_sha256"`
	BaseSHA        string                  `json:"base_sha"`
	HeadSHA        string                  `json:"head_sha"`
	ArtifactPath   string                  `json:"artifact_path"`
	ArtifactBytes  int64                   `json:"artifact_bytes"`
	ReportBytes    int64                   `json:"report_bytes"`
	FinalBytes     int64                   `json:"final_bytes"`
	TruthOracle    *partitionTruthOracleV1 `json:"truth_partition_oracle,omitempty"`
}

type partitionTruthOracleV1 struct {
	TopK                         int                         `json:"top_k"`
	ProbeBudget                  int                         `json:"probe_budget"`
	BestProbeCoverageAtK         float64                     `json:"best_probe_primary_partition_coverage_at_k"`
	TruthPrimaryHomePairColocate float64                     `json:"truth_primary_home_pair_colocation_at_k"`
	UniqueVectorBitPatterns      int                         `json:"unique_normalized_vector_bit_patterns"`
	ZeroDistanceTruthShareAtK    float64                     `json:"zero_distance_exact_truth_share_at_k"`
	Probes                       []partitionTruthProbeV1     `json:"probe_ladder"`
	Graph                        partitionGraphDiagnosticsV1 `json:"graph_diagnostics"`
}

type partitionTruthProbeV1 struct {
	ProbeBudget                   int       `json:"probe_budget"`
	BestPrimaryHomeCoverageAtK    float64   `json:"best_primary_home_coverage_at_k"`
	TruthNeighborRankRetentionAtK []float64 `json:"truth_neighbor_rank_primary_home_retention_at_k"`
}

type partitionGraphDiagnosticsV1 struct {
	DirectedEdges                                     int       `json:"directed_edges"`
	DirectedCutEdges                                  int       `json:"directed_cut_edges"`
	DirectedCutRatio                                  float64   `json:"directed_cut_ratio"`
	SymmetricEdges                                    int       `json:"symmetric_edges"`
	SymmetricCutEdges                                 int       `json:"symmetric_cut_edges"`
	SymmetricCutRatio                                 float64   `json:"symmetric_cut_ratio"`
	ApproximateGraphRecallByTruthNeighborRank         []float64 `json:"approximate_graph_recall_by_truth_neighbor_rank_2_through_k"`
	RetainedApproximateGraphTruthNeighborRank         []float64 `json:"retained_approximate_graph_truth_neighbor_rank_2_through_k"`
	PartitionLoads                                    []int     `json:"partition_loads"`
	TruthNeighborPairs                                int       `json:"truth_neighbor_pairs"`
	RetainedTruthNeighborPairs                        int       `json:"retained_truth_neighbor_pairs"`
	RetainedTruthNeighborPairShareAtK                 float64   `json:"retained_truth_neighbor_pair_share_at_k"`
	ApproximateGraphTruthNeighborEdges                int       `json:"approximate_graph_truth_neighbor_edges"`
	RetainedApproximateGraphTruthNeighborEdges        int       `json:"retained_approximate_graph_truth_neighbor_edges"`
	RetainedApproximateGraphTruthNeighborEdgeShareAtK float64   `json:"retained_approximate_graph_truth_neighbor_edge_share_at_k"`
}

type fixtureManifest struct {
	SchemaVersion int    `json:"schema_version"`
	Fixture       string `json:"fixture"`
	Generator     string `json:"generator"`
	Arithmetic    string `json:"arithmetic"`
	Vectors       int    `json:"vectors"`
	Queries       int    `json:"queries"`
	Dimensions    int    `json:"dimensions"`
	Metric        string `json:"metric"`
	Seed          int64  `json:"seed"`
	Checksum      string `json:"checksum"`
}

type neighbor struct {
	ID       string  `json:"id"`
	Distance float64 `json:"distance"`
	Ordinal  int     `json:"-"`
}
type stageResult struct {
	Name                      string  `json:"name"`
	Method                    string  `json:"method"`
	Enabled                   bool    `json:"enabled"`
	Lossy                     bool    `json:"lossy"`
	RecallAtK                 float64 `json:"recall_at_k"`
	Queries                   int     `json:"queries"`
	Probes                    int     `json:"probes"`
	Available                 bool    `json:"available"`
	UnavailableReason         string  `json:"unavailable_reason,omitempty"`
	RouteKind                 string  `json:"route_kind,omitempty"`
	Searches                  uint64  `json:"searches"`
	ExecutedSearches          uint64  `json:"executed_searches"`
	CachedSearches            uint64  `json:"cached_searches"`
	SearchRouteHNSWSearchPack uint64  `json:"search_route_hnsw_search_pack"`
	HNSWSearchPackActive      uint64  `json:"hnsw_search_pack_active"`
	HNSWSearchPackFallbacks   uint64  `json:"hnsw_search_pack_fallbacks"`
	RepresentativeCount       uint64  `json:"representative_count,omitempty"`
	CandidateBudget           uint64  `json:"candidate_budget,omitempty"`
	Candidates                uint64  `json:"candidates,omitempty"`
	Edges                     uint64  `json:"edges,omitempty"`
	SearchNanos               uint64  `json:"search_nanos,omitempty"`
	P50Nanos                  uint64  `json:"p50_nanos,omitempty"`
	P95Nanos                  uint64  `json:"p95_nanos,omitempty"`
	P99Nanos                  uint64  `json:"p99_nanos,omitempty"`
	BytesPerOp                float64 `json:"bytes_per_op,omitempty"`
	AllocsPerOp               float64 `json:"allocs_per_op,omitempty"`
}

type scoredNeighbor struct {
	Index    int
	Distance float64
}

type scoredNeighborMaxHeap []scoredNeighbor

func (h scoredNeighborMaxHeap) Len() int { return len(h) }
func (h scoredNeighborMaxHeap) Less(i, j int) bool {
	return scoredNeighborLess(h[j], h[i])
}
func (h scoredNeighborMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *scoredNeighborMaxHeap) Push(x any) {
	*h = append(*h, x.(scoredNeighbor))
}
func (h *scoredNeighborMaxHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

type hnswSearchEvidence struct {
	Searches                  uint64
	ExecutedSearches          uint64
	CachedSearches            uint64
	SearchRouteHNSWSearchPack uint64
	HNSWSearchPackActive      uint64
	HNSWSearchPackFallbacks   uint64
}

type hnswSearchOutcome struct {
	Neighbors []neighbor
	Evidence  hnswSearchEvidence
}

type hnswCacheKey struct {
	Query              int
	SelectedPartitions string
	TopK               int
}

type treeDBPartitionHNSW struct {
	dir              string
	db               *backenddb.DB
	partitions       []*collections.Collection
	rowCounts        []int
	cache            map[hnswCacheKey]hnswSearchOutcome
	physicalSearches uint64
}

type benchmarkMemoryPlan struct {
	FixtureResidentBytes int64
	SimulationWorkBytes  int64
	RouterBuildWorkBytes int64
	HNSWInsertWorkBytes  int64
	HNSWCacheBytes       int64
	ModeledPeakBytes     int64
}

// metricsV1 reserves every M0 evidence field. Simulation leaves production-only
// measurements at zero and labels them not_measured instead of implying a Raft result.
type metricsV1 struct {
	MeasurementStatus       string  `json:"measurement_status"`
	BuildWallNanos          int64   `json:"build_wall_nanos"`
	BuildCPUNanos           int64   `json:"build_cpu_nanos"`
	BuildCPUAvailable       bool    `json:"build_cpu_available"`
	PeakRSSBytes            int64   `json:"peak_rss_bytes"`
	PeakRSSAvailable        bool    `json:"peak_rss_available"`
	TemporaryBytes          int64   `json:"temporary_bytes"`
	FinalBytes              int64   `json:"final_bytes"`
	BytesPerVector          float64 `json:"bytes_per_vector"`
	Balance                 float64 `json:"balance"`
	MaxPartitionSize        int     `json:"max_partition_size"`
	EdgeCut                 int64   `json:"edge_cut"`
	ReplicationFactor       float64 `json:"replication_factor"`
	UnassignedOverlapBudget float64 `json:"unassigned_overlap_budget"`
	RepresentativeCount     int     `json:"representative_count"`
	SourceHNSWDegree        int     `json:"source_hnsw_degree"`
	RouterBytes             int64   `json:"router_bytes"`
	RoutingLatencyNanos     int64   `json:"routing_latency_nanos"`
	RoutedPartitionRecall   float64 `json:"routed_partition_recall"`
	CoarseningRecall        float64 `json:"coarsening_recall"`
	CoarseningMeasured      bool    `json:"coarsening_recall_available"`
	ApproximateRouterRecall float64 `json:"approximate_router_recall"`
	ApproximateMeasured     bool    `json:"approximate_router_recall_available"`
	HNSWRecallLoss          float64 `json:"hnsw_recall_loss"`
	HNSWLossMeasured        bool    `json:"hnsw_recall_loss_available"`
	LloydIterations         int     `json:"router_lloyd_iterations"`
	EmptyRepairs            int     `json:"router_empty_repairs"`
	MinRepresentatives      int     `json:"min_representatives_per_partition"`
	MaxRepresentatives      int     `json:"max_representatives_per_partition"`
	HeapCopyBytes           int64   `json:"heap_copy_bytes"`
	SelectedPartitions      int     `json:"selected_partitions"`
	SelectedGroups          int     `json:"selected_groups"`
	RPCs                    int     `json:"rpcs"`
	RequestBytes            int64   `json:"request_bytes"`
	ResponseBytes           int64   `json:"response_bytes"`
	ShardP50Nanos           int64   `json:"per_shard_p50_nanos"`
	ShardP95Nanos           int64   `json:"per_shard_p95_nanos"`
	ShardP99Nanos           int64   `json:"per_shard_p99_nanos"`
	MergeDedupeNanos        int64   `json:"merge_dedupe_nanos"`
	Cancellations           int64   `json:"cancellations"`
	Timeouts                int64   `json:"timeouts"`
	Failures                int64   `json:"failures"`
	QPS                     float64 `json:"end_to_end_qps"`
	P50Nanos                int64   `json:"end_to_end_p50_nanos"`
	P95Nanos                int64   `json:"end_to_end_p95_nanos"`
	P99Nanos                int64   `json:"end_to_end_p99_nanos"`
	RecallAt1               float64 `json:"recall_at_1"`
	RecallAt10              float64 `json:"recall_at_10"`
	RecallAt100             float64 `json:"recall_at_100"`
	BytesPerOp              float64 `json:"bytes_per_op"`
	AllocsPerOp             float64 `json:"allocs_per_op"`
	ResidentBytes           int64   `json:"resident_bytes"`
	MappedBytes             int64   `json:"mapped_bytes"`
}
type runResult struct {
	SchemaVersion      int                      `json:"schema_version"`
	ResultKind         string                   `json:"result_kind"`
	ProductionEvidence bool                     `json:"production_evidence"`
	Command            []string                 `json:"command"`
	BaseSHA            string                   `json:"base_sha"`
	HeadSHA            string                   `json:"head_sha"`
	GoVersion          string                   `json:"go_version"`
	Hardware           string                   `json:"hardware_context"`
	GOMAXPROCS         int                      `json:"gomaxprocs"`
	GoMemoryLimitBytes int64                    `json:"go_memory_limit_bytes"`
	Dataset            fixtureManifest          `json:"dataset"`
	Partitions         int                      `json:"partitions"`
	Overlap            float64                  `json:"overlap"`
	Probes             int                      `json:"probes"`
	TopK               int                      `json:"top_k"`
	RecallTarget       float64                  `json:"recall_target"`
	Seed               int64                    `json:"seed"`
	MemoryBudgetBytes  int64                    `json:"memory_budget_bytes"`
	ModeledPeakBytes   int64                    `json:"modeled_peak_bytes"`
	MemoryBudgetScope  string                   `json:"memory_budget_scope"`
	Warmup             int                      `json:"warmup"`
	Samples            int                      `json:"samples"`
	TimedBoundary      string                   `json:"timed_boundary"`
	Stages             []stageResult            `json:"stages"`
	Metrics            metricsV1                `json:"metrics"`
	Coordinator        *m6CoordinatorEvidenceV1 `json:"coordinator,omitempty"`
}

type treeDBRepresentativeRouter struct {
	dir              string
	db               *backenddb.DB
	collection       *collections.Collection
	router           *collections.VectorPartitionRouterV1
	build            collections.VectorPartitionRouterBuildStatusV1
	open             collections.VectorPartitionRouterOpenStatusV1
	candidates       int
	sourceHNSWDegree int
	buildCPU         int64
	buildCPUOK       bool
	peakRSS          int64
	peakRSSOK        bool
}

type routerSearchEvidence struct {
	Searches    uint64
	Candidates  uint64
	Edges       uint64
	SearchNanos uint64
	AllocBytes  uint64
	Allocs      uint64
	Durations   []uint64
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "treedb-vector-partition-bench:", err)
		os.Exit(1)
	}
}

type benchmarkRuntimeCapabilities struct {
	vectorPartitionNamespacePersistence bool
}

func currentBenchmarkRuntimeCapabilities() benchmarkRuntimeCapabilities {
	return benchmarkRuntimeCapabilities{
		vectorPartitionNamespacePersistence: collections.VectorPartitionNamespacePersistenceSupportedV1(),
	}
}

func benchmarkRuntimeLimits() (int, int64) {
	return runtime.GOMAXPROCS(0), debug.SetMemoryLimit(-1)
}

func run(args []string, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "m0-locality-capture" {
		return runM0LocalityCaptureV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "m0-locality-simulate" {
		return runM0LocalitySimulateV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "m0-membership-account" {
		return runM0MembershipAccountV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "m0-materialize-membership" {
		return runM0MaterializeMembershipV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-attribution" {
		return runLocalHNSWAttributionV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-calibration" {
		return runLocalHNSWRepairCalibrationV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-ef-curve" {
		return runLocalHNSWRepairEFCurveV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-m18-ef-curve" {
		return runLocalHNSWRepairM18EFCurveV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-construction-curve" {
		return runLocalHNSWRepairConstructionCurveV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-m-curve" {
		return runLocalHNSWRepairMCurveV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-repair-m-timing" {
		return runLocalHNSWRepairMTimingV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "local-hnsw-final-qualification" {
		return runLocalHNSWFinalQualificationV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "system-node" {
		return runVectorPartitionSystemNodeV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "system-check-topology" {
		return runVectorPartitionSystemCheckTopologyV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "system-search" {
		return runVectorPartitionSystemSearchV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "system-bench" {
		return runVectorPartitionSystemBenchV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "system-export-dataset" {
		return runVectorPartitionSystemExportDatasetV1(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "generate-truth-cache" {
		return runGenerateTruthCache(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "generate-fixture" {
		return runGenerateFixture(args[1:], stdout)
	}
	if len(args) > 0 && args[0] == "validate-qualification" {
		return runValidateQualification(args[1:], stdout)
	}
	return runWithRuntimeCapabilities(args, stdout, currentBenchmarkRuntimeCapabilities())
}

func runGenerateTruthCache(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench generate-truth-cache", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, out string
	var topK, capVectors int
	var seed, capVisits int64
	var capBytes int64
	fs.StringVar(&dataset, "dataset", "", "fixture directory")
	fs.StringVar(&out, "out", "", "canonical truth-cache directory")
	fs.IntVar(&topK, "top-k", 10, "canonical truth top-k")
	fs.Int64Var(&seed, "seed", 0, "fixture seed")
	fs.IntVar(&capVectors, "max-vectors", maxVectors, "fixture vector cap")
	fs.Int64Var(&capBytes, "max-fixture-bytes", maxFixtureBytes, "fixture byte cap")
	fs.Int64Var(&capVisits, "max-exact-truth-visits", 0, "exact source-query visit cap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	seedSet := false
	fs.Visit(func(f *flag.Flag) {
		seedSet = seedSet || f.Name == "seed"
	})
	if fs.NArg() != 0 || dataset == "" || out == "" || topK < 1 || !seedSet || capVisits < 1 || capVectors < 1 || capVectors > maxVectors || capBytes < 8 || capBytes > maxFixtureBytes {
		return errors.New("generate-truth-cache requires dataset, out, explicit seed, positive top-k/caps, and no positional arguments")
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	if err := validateM3FixtureWithCaps(fixture, capVectors, capBytes); err != nil {
		return err
	}
	if seed != fixture.Seed || topK > fixture.Vectors {
		return errors.New("truth-cache seed/top-k does not match the bounded fixture")
	}
	visits, err := memoryMul(int64(fixture.Vectors), int64(fixture.Queries))
	if err != nil || visits > capVisits {
		return fmt.Errorf("canonical exact truth visits exceed cap: visits=%d cap=%d", visits, capVisits)
	}
	corpus, queries := fixtureData(fixture)
	truth, err := m8ExactTruthFixtureV1(corpus, queries, topK)
	if err != nil {
		return err
	}
	truthSHA, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		return err
	}
	identity := m8TruthCacheIdentityV1(fixture, topK)
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	path := m8TruthCacheArtifactPathV1(out, identity)
	if _, err := os.Lstat(path); err == nil {
		return errors.New("canonical truth-cache artifact already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	artifactSHA, linked, err := m8PublishTruthCacheV1(path, "", func(w io.Writer) error {
		return m8WriteTruthCacheJSONV1(w, m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: fixture.Dimensions, Metric: fixture.Metric, TopK: topK, TruthSHA256: truthSHA, Truth: truth})
	})
	if err != nil {
		if linked {
			_, _ = fmt.Fprintf(stdout, "truth_cache=%s artifact_sha256=%s truth_sha256=%s identity=%s visits=%d publication=linked_directory_sync_failed\n", path, artifactSHA, truthSHA, identity, visits)
			return fmt.Errorf("canonical truth cache linked at %s with artifact_sha256=%s but publication did not complete: %w", path, artifactSHA, err)
		}
		return err
	}
	_, err = fmt.Fprintf(stdout, "truth_cache=%s artifact_sha256=%s truth_sha256=%s identity=%s visits=%d\n", path, artifactSHA, truthSHA, identity, visits)
	return err
}

func runGenerateFixture(args []string, stdout io.Writer) error {
	var (
		out               string
		vectors           int
		queries           int
		dimensions        int
		seed              int64
		generator         string
		maxChecksumVisits int64
	)
	fs := flag.NewFlagSet("treedb_vector_partition_bench generate-fixture", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&out, "out", "", "fixture directory")
	fs.IntVar(&vectors, "vectors", 0, "number of deterministic corpus vectors")
	fs.IntVar(&queries, "queries", 1, "number of deterministic queries")
	fs.IntVar(&dimensions, "dimensions", 16, "vector dimensions")
	fs.Int64Var(&seed, "seed", 1, "fixture generation seed")
	fs.StringVar(&generator, "generator", fixtureGenerator, "fixture generator identity")
	fs.Int64Var(&maxChecksumVisits, "max-checksum-visits", maxBenchmarkWorkUnits, "explicit exact checksum visit bound for fixture generation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || out == "" || vectors < 1 || vectors > maxVectors || queries < 1 || queries > maxVectors || dimensions < 1 || dimensions > maxDimensions {
		return errors.New("generate-fixture requires -out and positive bounded vectors, queries, and dimensions")
	}
	if !supportedFixtureGeneratorV1(generator) {
		return fmt.Errorf("unsupported fixture generator %q; supported generators are %s, %s, and %s", generator, fixtureGenerator, qualificationSyntheticGeneratorV1, qualificationEmbeddingGeneratorV1)
	}
	rows := int64(vectors) + int64(queries)
	if rows > maxFixtureBytes/(int64(dimensions)*8) {
		return fmt.Errorf("generated fixture float64 data exceeds %d-byte cap", maxFixtureBytes)
	}
	if visits, err := memoryMul(int64(vectors), int64(queries)); err != nil || maxChecksumVisits < 1 || visits > maxChecksumVisits {
		return fmt.Errorf("generated fixture checksum exact work exceeds %d-visit cap; set -max-checksum-visits explicitly for a declared qualification generation", maxChecksumVisits)
	}
	manifest := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       fmt.Sprintf("deterministic_%d", vectors),
		Generator:     generator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       vectors,
		Queries:       queries,
		Dimensions:    dimensions,
		Metric:        "cosine",
		Seed:          seed,
	}
	corpus, querySet := fixtureData(manifest)
	manifest.Checksum = fixtureChecksumFromData(corpus, querySet)
	if err := validateM3FixtureWithCaps(manifest, maxVectors, maxFixtureBytes); err != nil {
		return err
	}
	dir, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	path := filepath.Join(dir, "fixture_manifest.json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create fixture manifest: %w", err)
	}
	_, writeErr := file.Write(raw)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.Join(writeErr, closeErr)
	}
	_, err = fmt.Fprintf(stdout, "fixture=%s vectors=%d queries=%d dimensions=%d checksum=%s\n", path, vectors, queries, dimensions, manifest.Checksum)
	return err
}

func runWithRuntimeCapabilities(args []string, stdout io.Writer, capabilities benchmarkRuntimeCapabilities) (runErr error) {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	if (cfg.stage == "router" || cfg.stage == m6CoordinatorStageV1 || cfg.stage == m8ProductionMultiGroupModeV1) && !capabilities.vectorPartitionNamespacePersistence {
		return fmt.Errorf("%w: persisted M4/M6 evidence requires durable vector-partition lifecycle publication", collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1)
	}
	if cfg.stage == "overlap,partition_index" && !capabilities.vectorPartitionNamespacePersistence {
		return fmt.Errorf("%w: M3 partition-index evidence requires durable vector-partition lifecycle publication", collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1)
	}
	command := "treedb_vector_partition_bench"
	if cfg.stage == "overlap,partition_index" || cfg.stage == m8ProductionMultiGroupModeV1 {
		command, err = os.Executable()
		if err != nil {
			return fmt.Errorf("resolve benchmark executable: %w", err)
		}
		command, err = m8CanonicalPathV1(command)
		if err != nil {
			return fmt.Errorf("canonicalize benchmark executable: %w", err)
		}
	}
	if cfg.baseSHA, cfg.headSHA, err = provenanceWithExplicitV1(cfg.baseSHA, cfg.headSHA); err != nil {
		return err
	}
	if cfg.sourceCheckout != "" {
		cfg.sourceCheckout, err = m8SourceCheckoutV1(cfg.sourceCheckout, cfg.headSHA)
		if err != nil {
			return fmt.Errorf("-source-checkout: %w", err)
		}
		cfg.command = commandWithProvenanceAndSourceCheckoutV1(command, args, cfg.baseSHA, cfg.headSHA, cfg.sourceCheckout)
	} else {
		cfg.command = commandWithProvenanceV1(command, args, cfg.baseSHA, cfg.headSHA)
	}
	if cfg.stage == "overlap,partition_index" {
		cfg.m3BuildDirty = m8GitDirtyInV1(cfg.sourceCheckout, cfg.out, cfg.m3PersistDir)
	}
	fixture, err := loadFixture(cfg.dataset)
	if err != nil {
		return err
	}
	if cfg.stage == "partition" {
		err = validatePartitionFixtureWithCaps(fixture, cfg.maxVectors, cfg.maxBytes)
		if err == nil && cfg.partitionTruthOracle {
			err = validatePartitionTruthOracleWithCaps(fixture, cfg.partitions, cfg.topK, cfg.partition.Degree, cfg.maxVectors, maxBenchmarkWorkUnits, cfg.maxBytes)
		}
	} else if cfg.stage == "overlap,partition_index" || cfg.stage == m6CoordinatorStageV1 || cfg.stage == m8ProductionMultiGroupModeV1 {
		err = validateM3FixtureWithCaps(fixture, cfg.maxVectors, cfg.maxBytes)
	} else {
		err = validateFixtureWithCaps(fixture, cfg.maxVectors, cfg.maxBytes)
	}
	if err != nil {
		return err
	}
	if cfg.kahipPython != "" {
		if err := validateKaHIPSeedV1(fixture.Seed); err != nil {
			return err
		}
	}
	if cfg.partitions > fixture.Vectors {
		return errors.New("partitions cannot exceed fixture vectors")
	}
	if cfg.seed != fixture.Seed {
		return fmt.Errorf("-seed %d does not match fixture seed %d", cfg.seed, fixture.Seed)
	}
	if cfg.stage == m8ProductionMultiGroupModeV1 {
		if cfg.topK > fixture.Vectors {
			return errors.New("top-k cannot exceed fixture vectors")
		}
		if cfg.topK > nativewire.DefaultVectorPartitionShardSearchLimitsV1().MaxTopK {
			return fmt.Errorf("top-k cannot exceed M8 shard limit %d", nativewire.DefaultVectorPartitionShardSearchLimitsV1().MaxTopK)
		}
		if _, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, cfg.maxBytes); err != nil {
			return err
		}
		vectors, queries, err := m8ProductionFixtureDataV1(cfg, fixture)
		if err != nil {
			return err
		}
		return runM8ProductionMultiGroupV1(cfg, fixture, vectors, queries, stdout)
	}
	if cfg.stage == "partition" || cfg.stage == "overlap,partition_index" {
		// The partition builder owns only the frozen vector corpus and its own
		// graph controls. Simulation probes, stage selection, and simulation
		// memory planning are intentionally irrelevant here; M3 separately
		// consumes explicit overlap ratios, queries, and top-k.
		validateShape := vectorpartition.ValidateReferenceInputShape
		if cfg.kahipPython != "" {
			validateShape = vectorpartition.ValidateInputShape
		}
		if err := validateShape(cfg.partition, fixture.Vectors, fixture.Dimensions); err != nil {
			return err
		}
		if cfg.kahipPython != "" {
			if err := validateKaHIPFinalGraphEnvelopeV1(fixture.Vectors, cfg.partition.Degree); err != nil {
				return err
			}
		}
		// The artifact's Source.Checksum is computed over these generated vectors
		// by BuildWithPartitioner. The manifest checksum additionally commits the
		// simulation-only query/truth stream, so verifying it here would recreate
		// queries and exact truth that this stage deliberately does not own.
		if cfg.stage == "partition" {
			vectors := fixtureVectors(fixture)
			var queries [][]float64
			if cfg.partitionTruthOracle {
				vectors, queries = fixtureData(fixture)
				if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
					return errors.New("fixture checksum does not match generated vector/query/truth stream")
				}
			}
			return runPartitionStage(cfg, fixture, vectors, queries, stdout)
		}
		if cfg.topK > fixture.Vectors {
			return errors.New("top-k cannot exceed fixture vectors")
		}
		if _, err := validateM3BenchmarkWork(cfg, fixture, cfg.m3MaxBenchmarkVisits); err != nil {
			return err
		}
		vectors, queries := fixtureData(fixture)
		if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
			return errors.New("fixture checksum does not match generated vector/query/truth stream")
		}
		return runPartitionStage(cfg, fixture, vectors, queries, stdout)
	}
	if cfg.topK > fixture.Vectors {
		return errors.New("top-k cannot exceed fixture vectors")
	}
	if _, err := validateBenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits); err != nil {
		return err
	}
	cfg.memory, err = planBenchmarkMemory(cfg, fixture)
	if err != nil {
		return err
	}
	if cfg.memory.ModeledPeakBytes > cfg.maxBytes {
		return fmt.Errorf("modeled peak benchmark-owned memory %d exceeds -max-fixture-bytes %d", cfg.memory.ModeledPeakBytes, cfg.maxBytes)
	}
	vectors, queries := fixtureData(fixture)
	if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
		return errors.New("fixture checksum does not match generated vector/query/truth stream")
	}
	if cfg.stages["treedb_partition_local_hnsw"] {
		cfg.hnsw, err = newTreeDBPartitionHNSW(vectors, cfg.partitions)
		if err != nil {
			return fmt.Errorf("build TreeDB partition-local HNSW stage: %w", err)
		}
		defer func() {
			runErr = errors.Join(runErr, cfg.hnsw.Close())
		}()
	}
	if cfg.stage == "router" || cfg.stage == m6CoordinatorStageV1 {
		buildCPUStart, buildCPUAvailable := vectorPartitionBenchmarkCPUNanos()
		cfg.router, err = newTreeDBRepresentativeRouter(vectors, cfg.partitions, cfg.routerConfig, cfg.routerCandidates, cfg.sourceHNSWDegree)
		if err != nil {
			return fmt.Errorf("build TreeDB representative router stage: %w", err)
		}
		if buildCPUEnd, available := vectorPartitionBenchmarkCPUNanos(); buildCPUAvailable && available && buildCPUEnd >= buildCPUStart {
			cfg.router.buildCPU = buildCPUEnd - buildCPUStart
			cfg.router.buildCPUOK = true
		}
		if peakRSS, available := vectorPartitionBenchmarkPeakRSS(); available {
			cfg.router.peakRSS = peakRSS
			cfg.router.peakRSSOK = true
		}
		defer func() {
			runErr = errors.Join(runErr, cfg.router.Close())
		}()
		if cfg.stage == m6CoordinatorStageV1 {
			cfg.coordinator, err = newM6CoordinatorHarnessV1(cfg.router, vectors, cfg.partitions)
			if err != nil {
				return fmt.Errorf("build M6 local-service coordinator stage: %w", err)
			}
			coordinator := cfg.coordinator
			defer func() {
				runErr = errors.Join(runErr, coordinator.Close())
			}()
		}
	}
	for _, overlap := range cfg.overlaps {
		for _, probes := range cfg.probes {
			result, err := simulate(cfg, fixture, vectors, queries, probes, overlap)
			if err != nil {
				return err
			}
			if err := validateResult(result); err != nil {
				return err
			}
			if err := writeArtifacts(cfg.out, result); err != nil {
				return err
			}
			if cfg.format == "json" {
				b, err := json.Marshal(result)
				if err != nil {
					return fmt.Errorf("encode JSON result: %w", err)
				}
				fmt.Fprintln(stdout, string(b))
			} else {
				fmt.Fprintf(stdout, "simulation probes=%d overlap=%.2f recall@%d=%.4f\n", probes, overlap, cfg.topK, result.Stages[len(result.Stages)-1].RecallAtK)
			}
		}
	}
	return nil
}

func m8ProductionFixtureDataV1(cfg config, fixture fixtureManifest) ([][]float64, [][]float64, error) {
	// The matrix parent owns descriptor validation and subprocess orchestration
	// only. Each fresh child reconstructs and verifies the corpus, so retaining a
	// second copy in the blocked parent would make machine peak memory exceed the
	// independently reported child RSS.
	if len(cfg.m8VariantDBs) > 0 {
		return nil, nil, nil
	}
	vectors, queries := fixtureData(fixture)
	if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
		return nil, nil, errors.New("fixture checksum does not match generated vector/query/truth stream")
	}
	return vectors, queries, nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		format: "json", topK: 10, recallTarget: .9, seed: 1, stage: "simulation",
		warmup:              1,
		partitionAssignment: partitionAssignmentGraphV1,
		kahipTimeout:        kahipDefaultTimeout,
		partition:           vectorpartition.DefaultConfig(), routerConfig: vectorpartition.DefaultRouterConfigV1(),
		routerCandidates: 1024, sourceHNSWDegree: partitionHNSWDegree,
		m8MaxRSSBytes: uint64(maxFixtureBytes), m8MaxAssetBytes: uint64(maxFixtureBytes), m8MaxExactTruthVisits: maxBenchmarkWorkUnits,
		m8CoordinatorLimits: nativewire.DefaultVectorPartitionCoordinatorLimitsV1(),
		m8ShardLimits:       nativewire.DefaultVectorPartitionShardSearchLimitsV1(),
	}
	cfg.m8CoordinatorLimits.MaxCandidateBytes = m8ProductionCandidateBudgetBytesV1
	cfg.m8ShardLimits.MaxCandidateBytes = m8ProductionCandidateBudgetBytesV1
	var probes, overlap, concurrency, efSearch, m8VariantDBs string
	var stages string
	var routerMaxVectors int
	fs := flag.NewFlagSet("treedb_vector_partition_bench", flag.ContinueOnError)
	fs.StringVar(&cfg.dataset, "dataset", "", "fixture directory")
	fs.IntVar(&cfg.partitions, "partitions", 0, "logical partition count")
	fs.StringVar(&probes, "probes", "", "comma-separated probe counts")
	fs.StringVar(&overlap, "overlap", "0", "comma-separated derived overlap budgets")
	fs.IntVar(&cfg.topK, "top-k", cfg.topK, "top-k")
	fs.Float64Var(&cfg.recallTarget, "recall-target", cfg.recallTarget, "recall target")
	fs.Int64Var(&cfg.seed, "seed", cfg.seed, "fixture generation seed (must match manifest)")
	fs.StringVar(&cfg.baseSHA, "base-sha", "", "explicit immutable base revision for retained replay provenance")
	fs.StringVar(&cfg.headSHA, "head-sha", "", "explicit immutable head revision for retained replay provenance")
	fs.StringVar(&cfg.sourceCheckout, "source-checkout", "", "canonical Git toplevel used for retained M3/M8 replay provenance")
	fs.StringVar(&cfg.format, "format", cfg.format, "json or text")
	fs.StringVar(&cfg.out, "out", "", "artifact directory")
	fs.StringVar(&cfg.stage, "stage", cfg.stage, "simulation, partition, overlap,partition_index, router, or distributed_simulation_or_cluster")
	fs.StringVar(&cfg.mode, "mode", "", "production_multi_group (canonical M8 alias for -stage)")
	fs.IntVar(&cfg.raftGroups, "raft-groups", 0, "M8 data Raft group count")
	fs.IntVar(&cfg.raftNodes, "raft-nodes-per-group", 3, "M8 Raft members per data group (currently exactly 3)")
	fs.StringVar(&concurrency, "concurrency", "1", "comma-separated M8 query concurrency")
	fs.IntVar(&cfg.warmup, "warmup", cfg.warmup, "M8 minimum untimed topology warmup requests before the measured sweep (0 disables approximate warmup)")
	fs.StringVar(&efSearch, "ef-search", "128", "comma-separated M8 local HNSW ef_search values")
	fs.StringVar(&cfg.profiles, "profiles", "", "M8 profile artifact directory")
	fs.StringVar(&cfg.m8MatrixOut, "m8-matrix-out", "", "internal matrix-wide output root for child cleanliness checks")
	fs.StringVar(&cfg.m8MatrixProfiles, "m8-matrix-profiles", "", "internal matrix-wide profile root for child cleanliness checks")
	fs.StringVar(&cfg.m3PersistDir, "m3-persist-db", "", "retain the single overlap,partition_index row as a persistent TreeDB directory for downstream service benchmarks")
	fs.StringVar(&cfg.m8ExistingDB, "m8-existing-db", "", "read-only existing TreeDB M3 asset directory for production_multi_group; never rebuilt or deleted")
	fs.StringVar(&m8VariantDBs, "m8-variant-dbs", "", "comma-separated retained M3 directories for the strict three-variant production matrix")
	fs.Uint64Var(&cfg.m8MaxRSSBytes, "m8-max-rss-bytes", cfg.m8MaxRSSBytes, "hard process peak-RSS acceptance bound for production_multi_group")
	fs.Uint64Var(&cfg.m8MaxAssetBytes, "m8-max-persistent-asset-bytes", cfg.m8MaxAssetBytes, "hard persistent derived-asset byte bound for production_multi_group")
	fs.Int64Var(&cfg.m8MaxExactTruthVisits, "m8-max-exact-truth-visits", cfg.m8MaxExactTruthVisits, "hard exact source-query visit bound for production_multi_group")
	fs.StringVar(&cfg.m8TruthCache, "m8-truth-cache", "", "external canonical exact-truth cache directory; identity-bound and fail-closed")
	fs.StringVar(&cfg.m8TruthCacheSHA256, "m8-truth-cache-sha256", "", "independently trusted SHA-256 of the canonical truth-cache artifact required for cache reuse")
	fs.StringVar(&cfg.partitionAssignment, "partition-assignment", cfg.partitionAssignment, "partition assignment for partition/M3 stages: graph or stable_id_hash")
	fs.BoolVar(&cfg.partitionTruthOracle, "partition-truth-oracle", false, "emit exact truth primary-partition coverage diagnostic for -stage partition")
	fs.StringVar(&cfg.kahipPython, "partition-kahip-python", "", "trusted local offline Python for KaHIP 3.25 partition/M3 build stages; never used online; adapter is scripts/treedb_kahip_partition.py")
	fs.StringVar(&cfg.kahipScript, "partition-kahip-script", "", "explicit offline KaHIP adapter script path required with -partition-kahip-python")
	fs.DurationVar(&cfg.kahipTimeout, "partition-kahip-timeout", cfg.kahipTimeout, "positive offline KaHIP execution timeout")
	fs.IntVar(&cfg.partition.Repetitions, "partition-repetitions", cfg.partition.Repetitions, "dense-ball graph sketch repetitions")
	fs.Int64Var(&cfg.partition.MaxDistanceWork, "partition-max-distance-work", cfg.partition.MaxDistanceWork, "explicit reference graph scalar-work bound")
	fs.Int64Var(&cfg.partition.MaxPartitionWork, "partition-max-partition-work", cfg.partition.MaxPartitionWork, "qualification-only reference partition work bound; default remains conservative")
	fs.Int64Var(&cfg.m3MaxBenchmarkVisits, "m3-max-benchmark-visits", maxBenchmarkWorkUnits, "explicit M3 benchmark work bound")
	fs.IntVar(&cfg.partition.Pivots, "partition-pivots", cfg.partition.Pivots, "dense-ball pivots per recursive level")
	fs.IntVar(&cfg.partition.MaxLeafBucket, "partition-max-leaf-bucket", cfg.partition.MaxLeafBucket, "maximum dense-ball leaf bucket")
	fs.IntVar(&cfg.partition.Degree, "partition-degree", cfg.partition.Degree, "maximum canonical graph degree")
	fs.IntVar(&cfg.partitionHNSWM, "partition-hnsw-m", 0, "persistent partition-local HNSW M for M3; zero inherits 16")
	fs.IntVar(&cfg.partitionHNSWEfC, "partition-hnsw-ef-construction", 0, "persistent partition-local HNSW efConstruction for M3; zero inherits 128")
	fs.Float64Var(&cfg.partition.Imbalance, "imbalance", cfg.partition.Imbalance, "partition imbalance epsilon")
	fs.IntVar(&cfg.routerConfig.BranchFactor, "router-branch-factor", cfg.routerConfig.BranchFactor, "router hierarchical k-means branch factor")
	fs.IntVar(&cfg.routerConfig.LeafSize, "router-leaf-size", cfg.routerConfig.LeafSize, "router leaf stop size")
	fs.IntVar(&cfg.routerConfig.RepresentativesPerPartition, "router-representatives", cfg.routerConfig.RepresentativesPerPartition, "router representative budget per partition")
	fs.IntVar(&cfg.routerConfig.MaxDepth, "router-max-depth", cfg.routerConfig.MaxDepth, "router hierarchy depth bound")
	fs.IntVar(&cfg.routerConfig.MaxIterations, "router-max-iterations", cfg.routerConfig.MaxIterations, "router Lloyd iteration bound")
	fs.IntVar(&routerMaxVectors, "router-max-vectors", 0, "router final-membership cap; zero inherits -max-vectors")
	fs.Int64Var(&cfg.routerConfig.MaxScalarWork, "router-max-scalar-work", cfg.routerConfig.MaxScalarWork, "offline router scalar-work cap (1..50000000000)")
	fs.Uint64Var(&cfg.routerConfig.MaxRouterBytes, "router-max-bytes", cfg.routerConfig.MaxRouterBytes, "hard conservative persisted router-pack byte cap")
	fs.IntVar(&cfg.routerCandidates, "router-candidates", cfg.routerCandidates, "explicit approximate representative candidate budget")
	fs.IntVar(&cfg.sourceHNSWDegree, "source-hnsw-degree", cfg.sourceHNSWDegree, "source column_graph HNSW degree (1..16; default 16)")
	fs.StringVar(&stages, "stages", "all", "comma-separated independently enabled loss stages, or all")
	fs.IntVar(&cfg.maxVectors, "max-vectors", maxVectors, "maximum combined fixture vector/query count before allocation")
	fs.Int64Var(&cfg.maxBytes, "max-fixture-bytes", maxFixtureBytes, "maximum modeled peak bytes for benchmark-owned fixture and working material")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	routerCandidatesSet, routerMaxVectorsSet := false, false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "router-candidates":
			routerCandidatesSet = true
		case "router-max-vectors":
			routerMaxVectorsSet = true
		}
	})
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %q", fs.Args())
	}
	if (cfg.baseSHA == "") != (cfg.headSHA == "") || (cfg.baseSHA != "" && (!validLowerSHA(cfg.baseSHA) || !validLowerSHA(cfg.headSHA))) {
		return config{}, errors.New("-base-sha and -head-sha must be supplied together as lowercase 40-hex revisions")
	}
	if cfg.partition.MaxDistanceWork < 1 || cfg.partition.MaxPartitionWork < 1 || cfg.m3MaxBenchmarkVisits < 1 {
		return config{}, errors.New("partition work and M3 benchmark-visit limits must be positive")
	}
	if cfg.m8TruthCacheSHA256 != "" && (cfg.m8TruthCache == "" || len(cfg.m8TruthCacheSHA256) != sha256.Size*2 || strings.ToLower(cfg.m8TruthCacheSHA256) != cfg.m8TruthCacheSHA256) {
		return config{}, errors.New("-m8-truth-cache-sha256 must be lowercase 64-hex")
	}
	if cfg.m8TruthCacheSHA256 != "" {
		if decoded, err := hex.DecodeString(cfg.m8TruthCacheSHA256); err != nil || len(decoded) != sha256.Size {
			return config{}, errors.New("-m8-truth-cache-sha256 must be lowercase 64-hex")
		}
	}
	if cfg.mode != "" {
		if cfg.mode != m8ProductionMultiGroupModeV1 || cfg.stage != "simulation" {
			return config{}, errors.New("-mode supports only production_multi_group and cannot be combined with -stage")
		}
		cfg.stage = cfg.mode
	}
	// The offline M2 builder does not execute M0 probe simulations. Preserve
	// the simulation requirement while allowing the issue's partition-stage
	// invocation to omit a meaningless -probes flag.
	if (cfg.stage == "partition" || cfg.stage == "overlap,partition_index" || cfg.stage == "router" || cfg.stage == m6CoordinatorStageV1) && probes == "" {
		probes = "1"
	}
	if cfg.stage == m8ProductionMultiGroupModeV1 && probes == "" {
		probes = strconv.Itoa(cfg.partitions)
	}
	var err error
	if cfg.probes, err = parseInts(probes); err != nil {
		return config{}, fmt.Errorf("probes: %w", err)
	}
	if cfg.stage == m8ProductionMultiGroupModeV1 && !routerCandidatesSet {
		cfg.routerCandidates = 64
		for _, probes := range cfg.probes {
			if probes > cfg.routerCandidates {
				cfg.routerCandidates = probes
			}
		}
	}
	if cfg.overlaps, err = parseFloats(overlap); err != nil {
		return config{}, fmt.Errorf("overlap: %w", err)
	}
	if cfg.concurrency, err = parseInts(concurrency); err != nil {
		return config{}, fmt.Errorf("concurrency: %w", err)
	}
	if cfg.efSearch, err = parseInts(efSearch); err != nil {
		return config{}, fmt.Errorf("ef-search: %w", err)
	}
	if m8VariantDBs != "" {
		seen := map[string]bool{}
		for _, item := range strings.Split(m8VariantDBs, ",") {
			item = strings.TrimSpace(item)
			if item == "" || seen[item] {
				return config{}, errors.New("-m8-variant-dbs requires distinct nonempty paths")
			}
			seen[item] = true
			cfg.m8VariantDBs = append(cfg.m8VariantDBs, item)
		}
	}
	if cfg.dataset == "" || cfg.out == "" || cfg.partitions < 1 || cfg.partitions > maxPartitions || cfg.topK < 1 || cfg.maxVectors < 1 || cfg.maxVectors > maxVectors || cfg.maxBytes < 8 || cfg.maxBytes > maxFixtureBytes || cfg.format != "json" && cfg.format != "text" || cfg.stage != "simulation" && cfg.stage != "partition" && cfg.stage != "overlap,partition_index" && cfg.stage != "router" && cfg.stage != m6CoordinatorStageV1 && cfg.stage != m8ProductionMultiGroupModeV1 || cfg.routerCandidates < 1 {
		return config{}, errors.New("dataset, out, positive bounded partitions/top-k, and json|text format are required")
	}
	if err := validateSourceHNSWDegree(cfg.sourceHNSWDegree); err != nil {
		return config{}, err
	}
	if len(cfg.probes) == 0 || len(cfg.overlaps) == 0 || math.IsNaN(cfg.recallTarget) || math.IsInf(cfg.recallTarget, 0) || cfg.recallTarget < 0 || cfg.recallTarget > 1 {
		return config{}, errors.New("probes/overlap must be non-empty and recall target must be in [0,1]")
	}
	for _, p := range cfg.probes {
		if p < 1 || p > cfg.partitions {
			return config{}, errors.New("each probe count must be within partitions")
		}
	}
	for _, x := range cfg.overlaps {
		if x < 0 || x > 1 || math.IsNaN(x) || math.IsInf(x, 0) {
			return config{}, errors.New("overlap must be finite in [0,1]")
		}
	}
	if cfg.stage == m8ProductionMultiGroupModeV1 {
		if cfg.m8MaxRSSBytes == 0 || cfg.m8MaxAssetBytes == 0 || cfg.m8MaxExactTruthVisits < 1 {
			return config{}, errors.New("production_multi_group requires positive RSS, persistent-asset, and exact-truth bounds")
		}
		if cfg.raftGroups < 2 || cfg.raftGroups > 64 || cfg.raftGroups > cfg.partitions || cfg.raftNodes != 3 || cfg.partitions < 4 {
			return config{}, errors.New("production_multi_group requires 2..64 groups, exactly 3 nodes/group, at least 4 partitions, and groups <= partitions")
		}
		coordinatorLimits := nativewire.DefaultVectorPartitionCoordinatorLimitsV1()
		if cfg.partitions > coordinatorLimits.MaxSelectedPartitions {
			return config{}, fmt.Errorf("production_multi_group requires at most %d partitions", coordinatorLimits.MaxSelectedPartitions)
		}
		if len(cfg.concurrency) == 0 || len(cfg.efSearch) == 0 || cfg.warmup < 0 || cfg.warmup > 10_000 {
			return config{}, errors.New("production_multi_group requires non-empty concurrency and ef-search sweeps")
		}
		for _, value := range cfg.concurrency {
			if value < 1 || value > 256 {
				return config{}, errors.New("each concurrency must be in [1,256]")
			}
		}
		shardLimits := nativewire.DefaultVectorPartitionShardSearchLimitsV1()
		for _, value := range cfg.efSearch {
			if value < cfg.topK || value > shardLimits.MaxEfSearch {
				return config{}, fmt.Errorf("each ef-search must be in [%d,%d]", cfg.topK, shardLimits.MaxEfSearch)
			}
		}
		for _, probes := range cfg.probes {
			if probes > cfg.routerCandidates {
				return config{}, errors.New("production_multi_group requires router-candidates >= every probe count")
			}
		}
		if !allUnique(cfg.probes) || !allUnique(cfg.efSearch) || !allUnique(cfg.concurrency) || !allUnique(cfg.overlaps) {
			return config{}, errors.New("production_multi_group requires distinct probes, ef-search, concurrency, and overlap values")
		}
	}
	if cfg.m3PersistDir != "" && (cfg.stage != "overlap,partition_index" || len(cfg.overlaps) != 1) {
		return config{}, errors.New("-m3-persist-db requires stage overlap,partition_index with exactly one overlap ratio")
	}
	if cfg.m8ExistingDB != "" && cfg.stage != m8ProductionMultiGroupModeV1 {
		return config{}, errors.New("-m8-existing-db requires production_multi_group")
	}
	if len(cfg.m8VariantDBs) > 0 && (cfg.stage != m8ProductionMultiGroupModeV1 || cfg.m8ExistingDB != "" || len(cfg.m8VariantDBs) != 3) {
		return config{}, errors.New("-m8-variant-dbs requires production_multi_group, exactly three directories, and no -m8-existing-db")
	}
	if cfg.partitionAssignment != partitionAssignmentGraphV1 && cfg.partitionAssignment != partitionAssignmentStableIDHashV1 {
		return config{}, errors.New("-partition-assignment must be graph or stable_id_hash")
	}
	if cfg.kahipPython != "" && ((cfg.stage != "partition" && cfg.stage != "overlap,partition_index") || cfg.partitionAssignment != partitionAssignmentGraphV1) {
		return config{}, errors.New("-partition-kahip-python requires partition or overlap,partition_index stage with graph assignment")
	}
	if cfg.kahipPython != "" && cfg.partition.Imbalance != 0.05 {
		return config{}, errors.New("-partition-kahip-python requires -imbalance 0.05")
	}
	if (cfg.kahipPython == "") != (cfg.kahipScript == "") {
		return config{}, errors.New("-partition-kahip-python and -partition-kahip-script must be provided together")
	}
	if cfg.kahipPython != "" && cfg.kahipTimeout <= 0 {
		return config{}, errors.New("-partition-kahip-timeout must be positive")
	}
	if cfg.kahipPython != "" {
		if err := validateKaHIPSeedV1(cfg.seed); err != nil {
			return config{}, err
		}
	}
	if cfg.kahipPython != "" {
		python, err := exec.LookPath(cfg.kahipPython)
		if err != nil {
			return config{}, fmt.Errorf("-partition-kahip-python: %w", err)
		}
		if cfg.kahipPython, err = filepath.Abs(python); err != nil {
			return config{}, fmt.Errorf("-partition-kahip-python: %w", err)
		}
		// Hash the executable selected for this run without replacing the configured
		// invocation path: a venv entry point may intentionally be a symlink.
		if cfg.kahipPythonSHA256, err = m8BenchmarkExecutableSHA256V1(cfg.kahipPython); err != nil {
			return config{}, fmt.Errorf("-partition-kahip-python: %w", err)
		}
		script, err := filepath.Abs(cfg.kahipScript)
		if err != nil {
			return config{}, fmt.Errorf("-partition-kahip-script: %w", err)
		}
		info, err := os.Stat(script)
		if err != nil || !info.Mode().IsRegular() {
			return config{}, errors.New("-partition-kahip-script must be a regular file")
		}
		file, err := os.Open(script)
		if err != nil {
			return config{}, fmt.Errorf("-partition-kahip-script: %w", err)
		}
		scriptBytes, readErr := io.ReadAll(io.LimitReader(file, kahipAdapterMaxBytes+1))
		closeErr := file.Close()
		if readErr != nil {
			return config{}, fmt.Errorf("-partition-kahip-script: %w", readErr)
		}
		if closeErr != nil {
			return config{}, fmt.Errorf("-partition-kahip-script: %w", closeErr)
		}
		if len(scriptBytes) > kahipAdapterMaxBytes {
			return config{}, fmt.Errorf("-partition-kahip-script exceeds %d-byte cap", kahipAdapterMaxBytes)
		}
		sum := sha256.Sum256(scriptBytes)
		cfg.kahipAdapterSHA256 = hex.EncodeToString(sum[:])
		if cfg.kahipAdapterSHA256 != kahipAdapterSHA256 {
			return config{}, errors.New("-partition-kahip-script does not match the pinned adapter")
		}
		cfg.kahipScript = script
		cfg.kahipSource = string(scriptBytes)
	}
	if cfg.partitionAssignment != partitionAssignmentGraphV1 && cfg.stage != "partition" && cfg.stage != "overlap,partition_index" {
		return config{}, errors.New("-partition-assignment applies only to partition and overlap,partition_index stages")
	}
	if cfg.partitionTruthOracle && cfg.stage != "partition" {
		return config{}, errors.New("-partition-truth-oracle requires -stage partition")
	}
	if cfg.partitionAssignment == partitionAssignmentStableIDHashV1 && cfg.stage == "overlap,partition_index" && (len(cfg.overlaps) != 1 || cfg.overlaps[0] != 0) {
		return config{}, errors.New("stable_id_hash M3 materialization requires exactly zero overlap")
	}
	if cfg.stage == "overlap,partition_index" {
		if _, _, err := m3PartitionLocalHNSWConfigV1(cfg); err != nil {
			return config{}, err
		}
	} else if cfg.partitionHNSWM != 0 || cfg.partitionHNSWEfC != 0 {
		return config{}, errors.New("-partition-hnsw-m and -partition-hnsw-ef-construction apply only to M3")
	}
	if cfg.stage == "router" && stages == "all" {
		stages = "exact_representative_routing,approximate_representative_routing"
	}
	if cfg.stage == m6CoordinatorStageV1 && stages == "all" {
		stages = m6CoordinatorAttributionStageV1
	}
	cfg.stages = stageSet(stages)
	if len(cfg.stages) == 0 {
		return config{}, errors.New("stages must name known stages or all")
	}
	if cfg.stage == m6CoordinatorStageV1 &&
		(len(cfg.stages) != 1 || !cfg.stages[m6CoordinatorAttributionStageV1]) {
		return config{}, errors.New("distributed_simulation_or_cluster requires only the M6 local-service coordinator attribution stage")
	}
	if cfg.stage == m6CoordinatorStageV1 {
		shardLimits := nativewire.DefaultVectorPartitionShardSearchLimitsV1()
		if cfg.partitions > m6CoordinatorMaxGroupsV1 || cfg.topK > shardLimits.MaxTopK ||
			len(cfg.overlaps) != 1 || cfg.overlaps[0] != 0 {
			return config{}, fmt.Errorf(
				"distributed_simulation_or_cluster local-service evidence requires at most %d partitions, top-k at most %d, and exactly one zero overlap row",
				m6CoordinatorMaxGroupsV1, shardLimits.MaxTopK,
			)
		}
	}
	cfg.partition.Seed = cfg.seed
	cfg.partition.Partitions = cfg.partitions
	cfg.partition.MaxVectors = cfg.maxVectors
	cfg.routerConfig.Seed = cfg.seed
	if routerMaxVectorsSet {
		cfg.routerConfig.MaxVectors = routerMaxVectors
	} else {
		cfg.routerConfig.MaxVectors = cfg.maxVectors
	}
	cfg.routerConfig.MaxDimensions = maxDimensions
	cfg.routerConfig.MaxRepresentatives = maxVectors
	if err := vectorpartition.ValidateRouterConfigV1(cfg.routerConfig); err != nil {
		return config{}, fmt.Errorf("router config: %w", err)
	}
	// The graph validator reserves the edge budget for every repetition. Keep
	// that capacity when constraining the default by the configured input cap.
	// All operands are independently bounded by parse/build validation, but use
	// int64 so this preflight cannot overflow on narrower integer targets.
	maxEdgesForInput := int64(cfg.maxVectors) * int64(cfg.partition.Degree) * int64(cfg.partition.Repetitions)
	if maxEdgesForInput > 0 && maxEdgesForInput < int64(cfg.partition.MaxEdges) {
		cfg.partition.MaxEdges = int(maxEdgesForInput)
	}
	return cfg, nil
}

func m3PartitionLocalHNSWConfigV1(cfg config) (int, int, error) {
	m := cfg.partitionHNSWM
	if m == 0 {
		m = partitionHNSWDegree
	}
	if m < 2 || m > maxPartitionLocalHNSWM {
		return 0, 0, fmt.Errorf("effective partition HNSW M must be in [2,%d]", maxPartitionLocalHNSWM)
	}
	efConstruction := cfg.partitionHNSWEfC
	if efConstruction == 0 {
		efConstruction = partitionHNSWDefaultEfC
	}
	if efConstruction < m || efConstruction > maxPartitionHNSWEfC {
		return 0, 0, fmt.Errorf("effective partition HNSW efConstruction must be in [%d,%d]", m, maxPartitionHNSWEfC)
	}
	return m, efConstruction, nil
}

func m3PartitionLocalGraphVariantV1(m, efConstruction int) (collections.VectorPartitionLocalGraphVariantV1, error) {
	switch {
	case m == partitionHNSWDegree && efConstruction == partitionHNSWDefaultEfC:
		return collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, nil
	case m == 18 && efConstruction == 256:
		return collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, nil
	default:
		return "", fmt.Errorf("unsupported production partition-local HNSW M/efConstruction=%d/%d", m, efConstruction)
	}
}

func runPartitionStage(cfg config, fixture fixtureManifest, vectors, queries [][]float64, stdout io.Writer) error {
	input := make([]vectorpartition.Vector, len(vectors))
	for i, values := range vectors {
		input[i] = vectorpartition.Vector{ID: fmt.Sprintf("doc-%06d", i), Values: values}
	}
	started := time.Now()
	backend := vectorpartition.Partitioner(vectorpartition.ReferencePartitioner{})
	if cfg.kahipPython != "" {
		backend = kahipRequestPartitioner{}
	}
	artifact, err := vectorpartition.BuildWithPartitioner(input, cfg.partition, vectorpartition.Source{SourceID: "m0_fixture:" + fixture.Checksum}, backend)
	if err != nil {
		return fmt.Errorf("build validated vector partition artifact: %w", err)
	}
	if cfg.kahipPython != "" {
		request := artifact
		input, err := vectorpartition.CanonicalJSON(request)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), cfg.kahipTimeout)
		defer cancel()
		artifact, err = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, kahipAdapterCommand(cfg), input, vectorpartition.ExternalJSONLimits{MaxInput: len(input), MaxOutput: kahipOutputCap(input, request)}, request)
		if err != nil {
			return fmt.Errorf("KaHIP 3.25 offline partition: %w", err)
		}
	}
	graphArtifactDigest, err := vectorpartition.Digest(artifact)
	if err != nil {
		return err
	}
	graphBuildDigest, err := m3GraphBuildSHA256V1(artifact)
	if err != nil {
		return err
	}
	if cfg.partitionAssignment == partitionAssignmentStableIDHashV1 {
		artifact, err = vectorpartition.BuildStableIDHashBaseline(artifact)
		if err != nil {
			return fmt.Errorf("build stable-ID hash partition baseline: %w", err)
		}
	}
	bytes, err := vectorpartition.CanonicalJSON(artifact)
	if err != nil {
		return err
	}
	digest, err := vectorpartition.Digest(artifact)
	if err != nil {
		return err
	}
	suffix, err := provenanceSuffix(digest, cfg.baseSHA, cfg.headSHA)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.out, 0755); err != nil {
		return err
	}
	name := "vector_partition_" + suffix + ".json"
	path := filepath.Join(cfg.out, name)
	if err := os.WriteFile(path, bytes, 0644); err != nil {
		return err
	}
	report := partitionRun{SchemaVersion: 1, ResultKind: "offline_partition_builder", Dataset: fixture, Source: artifact.Source, Config: artifact.Config, Metrics: artifact.Metrics, BuildNanos: time.Since(started).Nanoseconds(), ArtifactSHA256: digest, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, ArtifactPath: path, ArtifactBytes: int64(len(bytes))}
	if len(queries) != 0 {
		oracle, err := partitionTruthOracleForArtifactV1(vectors, queries, artifact.Assignment, artifact.Graph.Neighbors, cfg.partitions, cfg.topK)
		if err != nil {
			return err
		}
		report.TruthOracle = &oracle
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	for i := 0; i < 8; i++ {
		report.ReportBytes = int64(len(raw))
		report.FinalBytes = report.ArtifactBytes + report.ReportBytes
		raw, err = json.Marshal(report)
		if err != nil {
			return err
		}
		if int64(len(raw)) == report.ReportBytes {
			break
		}
	}
	if report.ReportBytes != int64(len(raw)) {
		return errors.New("compact partition report byte accounting did not converge")
	}
	if err := os.WriteFile(filepath.Join(cfg.out, "vector_partition_build_"+suffix+".json"), raw, 0644); err != nil {
		return err
	}
	if cfg.stage == "overlap,partition_index" {
		return runM3PartitionIndexStage(cfg, fixture, artifact, digest, graphArtifactDigest, graphBuildDigest, suffix, vectors, queries, stdout)
	}
	if cfg.format == "json" {
		_, err = fmt.Fprintln(stdout, string(raw))
		return err
	}
	_, err = fmt.Fprintf(stdout, "partition artifact=%s edges=%d cut=%d cap=%d\n", path, artifact.Metrics.GraphEdges, artifact.Metrics.EdgeCut, artifact.Metrics.Cap)
	return err
}

func kahipOutputCap(input []byte, a vectorpartition.Artifact) int {
	// Each canonical JSON assignment label may grow from one digit to five
	// (partitions are capped at 16384); reserve a small fixed metadata delta.
	return len(input) + len(a.IDs)*5 + 1024
}

func kahipAdapterCommand(cfg config) []string {
	return []string{cfg.kahipPython, "-c", cfg.kahipSource}
}

func validateKaHIPFinalGraphEnvelopeV1(vectors, degree int) error {
	directedEdges, err := memoryMul(int64(vectors), int64(degree))
	if err != nil || directedEdges > kahipMaxDirectedEdges {
		return fmt.Errorf("KaHIP final directed-edge envelope exceeds %d", kahipMaxDirectedEdges)
	}
	return nil
}

func validateKaHIPSeedV1(seed int64) error {
	if seed < kahipMinSeed || seed > kahipMaxSeed {
		return fmt.Errorf("KaHIP seed must be in [%d,%d]", kahipMinSeed, kahipMaxSeed)
	}
	return nil
}

func partitionTruthOracleForArtifactV1(vectors, queries [][]float64, assignment []int, graph [][]int, partitions, topK int) (partitionTruthOracleV1, error) {
	if len(vectors) == 0 || len(queries) == 0 || len(assignment) != len(vectors) || len(graph) != len(vectors) || partitions < 1 || topK < 1 || topK > len(vectors) {
		return partitionTruthOracleV1{}, errors.New("invalid partition truth oracle input")
	}
	quarterProbes := max(1, partitions/4)
	probeBudgets := partitionTruthProbeBudgets(partitions)
	probeRows := make([]partitionTruthProbeV1, len(probeBudgets))
	for i, probes := range probeBudgets {
		probeRows[i] = partitionTruthProbeV1{ProbeBudget: probes, TruthNeighborRankRetentionAtK: make([]float64, topK)}
	}
	var pairColocation float64
	truthPairs, retainedTruthPairs := 0, 0
	graphTruthRankHits, retainedGraphTruthRankHits := make([]int, max(0, topK-1)), make([]int, max(0, topK-1))
	var zeroDistance int
	classes := make(map[[32]byte]struct{}, len(vectors))
	for _, vector := range vectors {
		classes[vectorpartition.VectorBitsFingerprintV1(vector)] = struct{}{}
	}
	for _, query := range queries {
		truth := exactTopK(vectors, query, topK)
		counts := make([]int, partitions)
		truthPartitions := make([]int, len(truth))
		for rank, result := range truth {
			if result.Distance <= 1e-12 {
				zeroDistance++
			}
			if result.Ordinal < 0 || result.Ordinal >= len(assignment) || assignment[result.Ordinal] < 0 || assignment[result.Ordinal] >= partitions {
				return partitionTruthOracleV1{}, errors.New("invalid truth partition assignment")
			}
			partition := assignment[result.Ordinal]
			counts[partition]++
			truthPartitions[rank] = partition
		}
		ordered := make([]int, partitions)
		for partition := range ordered {
			ordered[partition] = partition
		}
		sort.Slice(ordered, func(i, j int) bool {
			if counts[ordered[i]] != counts[ordered[j]] {
				return counts[ordered[i]] > counts[ordered[j]]
			}
			return ordered[i] < ordered[j]
		})
		for row := range probeRows {
			selected := make(map[int]struct{}, probeRows[row].ProbeBudget)
			for i := 0; i < probeRows[row].ProbeBudget; i++ {
				selected[ordered[i]] = struct{}{}
			}
			covered := 0
			for rank, partition := range truthPartitions {
				if _, ok := selected[partition]; ok {
					covered++
					probeRows[row].TruthNeighborRankRetentionAtK[rank]++
				}
			}
			probeRows[row].BestPrimaryHomeCoverageAtK += float64(covered) / float64(len(truth))
		}
		pairs, matched := len(truth)*(len(truth)-1)/2, 0
		for _, count := range counts {
			matched += count * (count - 1) / 2
		}
		truthPairs += pairs
		retainedTruthPairs += matched
		// Treat the held-out query's exact top-1 document as the graph anchor;
		// ranks 2..k then measure approximate adjacency and partition retention.
		anchor := truth[0].Ordinal
		for rank := 1; rank < len(truth); rank++ {
			neighbor := truth[rank].Ordinal
			neighbors := graph[anchor]
			if at := sort.SearchInts(neighbors, neighbor); at < len(neighbors) && neighbors[at] == neighbor {
				graphTruthRankHits[rank-1]++
				if assignment[anchor] == assignment[neighbor] {
					retainedGraphTruthRankHits[rank-1]++
				}
			}
		}
		if pairs > 0 {
			pairColocation += float64(matched) / float64(pairs)
		}
	}
	for i := range probeRows {
		probeRows[i].BestPrimaryHomeCoverageAtK /= float64(len(queries))
		for rank := range probeRows[i].TruthNeighborRankRetentionAtK {
			probeRows[i].TruthNeighborRankRetentionAtK[rank] /= float64(len(queries))
		}
	}
	graphDiagnostics, err := partitionGraphDiagnosticsForArtifactV1(graph, assignment, partitions, truthPairs, retainedTruthPairs)
	if err != nil {
		return partitionTruthOracleV1{}, err
	}
	graphDiagnostics.ApproximateGraphRecallByTruthNeighborRank = make([]float64, len(graphTruthRankHits))
	graphDiagnostics.RetainedApproximateGraphTruthNeighborRank = make([]float64, len(graphTruthRankHits))
	for rank := range graphTruthRankHits {
		graphDiagnostics.ApproximateGraphRecallByTruthNeighborRank[rank] = float64(graphTruthRankHits[rank]) / float64(len(queries))
		graphDiagnostics.RetainedApproximateGraphTruthNeighborRank[rank] = float64(retainedGraphTruthRankHits[rank]) / float64(len(queries))
		graphDiagnostics.ApproximateGraphTruthNeighborEdges += graphTruthRankHits[rank]
		graphDiagnostics.RetainedApproximateGraphTruthNeighborEdges += retainedGraphTruthRankHits[rank]
	}
	if graphDiagnostics.ApproximateGraphTruthNeighborEdges > 0 {
		graphDiagnostics.RetainedApproximateGraphTruthNeighborEdgeShareAtK = float64(graphDiagnostics.RetainedApproximateGraphTruthNeighborEdges) / float64(graphDiagnostics.ApproximateGraphTruthNeighborEdges)
	}
	quarterCoverage := 0.0
	for _, row := range probeRows {
		if row.ProbeBudget == quarterProbes {
			quarterCoverage = row.BestPrimaryHomeCoverageAtK
			break
		}
	}
	return partitionTruthOracleV1{TopK: topK, ProbeBudget: quarterProbes, BestProbeCoverageAtK: quarterCoverage, TruthPrimaryHomePairColocate: pairColocation / float64(len(queries)), UniqueVectorBitPatterns: len(classes), ZeroDistanceTruthShareAtK: float64(zeroDistance) / float64(len(queries)*topK), Probes: probeRows, Graph: graphDiagnostics}, nil
}

func partitionGraphDiagnosticsForArtifactV1(graph [][]int, assignment []int, partitions, truthPairs, retainedTruthPairs int) (partitionGraphDiagnosticsV1, error) {
	if len(graph) == 0 || len(graph) != len(assignment) || partitions < 1 || truthPairs < 0 || retainedTruthPairs < 0 || retainedTruthPairs > truthPairs {
		return partitionGraphDiagnosticsV1{}, errors.New("invalid partition graph diagnostic input")
	}
	out := partitionGraphDiagnosticsV1{PartitionLoads: make([]int, partitions)}
	for node, neighbors := range graph {
		if assignment[node] < 0 || assignment[node] >= partitions {
			return partitionGraphDiagnosticsV1{}, errors.New("invalid graph partition assignment")
		}
		out.PartitionLoads[assignment[node]]++
		previous := -1
		for _, neighbor := range neighbors {
			if neighbor < 0 || neighbor >= len(graph) || neighbor == node || neighbor <= previous {
				return partitionGraphDiagnosticsV1{}, errors.New("invalid approximate graph edge")
			}
			previous = neighbor
		}
	}
	for node, neighbors := range graph {
		for _, neighbor := range neighbors {
			cut := assignment[node] != assignment[neighbor]
			out.DirectedEdges++
			if cut {
				out.DirectedCutEdges++
			}
			reverse := graph[neighbor]
			at := sort.SearchInts(reverse, node)
			if node < neighbor || at == len(reverse) || reverse[at] != node {
				out.SymmetricEdges++
				if cut {
					out.SymmetricCutEdges++
				}
			}
		}
	}
	if out.DirectedEdges > 0 {
		out.DirectedCutRatio = float64(out.DirectedCutEdges) / float64(out.DirectedEdges)
	}
	if out.SymmetricEdges > 0 {
		out.SymmetricCutRatio = float64(out.SymmetricCutEdges) / float64(out.SymmetricEdges)
	}
	out.TruthNeighborPairs = truthPairs
	out.RetainedTruthNeighborPairs = retainedTruthPairs
	if truthPairs > 0 {
		out.RetainedTruthNeighborPairShareAtK = float64(retainedTruthPairs) / float64(truthPairs)
	}
	return out, nil
}

const provenanceSuffixBytes = 12

func provenanceSuffix(digest, baseSHA, headSHA string) (string, error) {
	if len(digest) < provenanceSuffixBytes || len(baseSHA) < provenanceSuffixBytes || len(headSHA) < provenanceSuffixBytes {
		return "", errors.New("partition artifact provenance digest/SHA is too short for filename")
	}
	return digest[:provenanceSuffixBytes] + "_" + baseSHA[:provenanceSuffixBytes] + "_" + headSHA[:provenanceSuffixBytes], nil
}

var knownStages = []string{"exact_global_top_k", "partition_oracle", "exact_representative_routing", "approximate_representative_routing", "exact_partition_local", "treedb_partition_local_hnsw", "end_to_end_distributed_simulation"}

func stageSet(raw string) map[string]bool {
	out := map[string]bool{}
	if raw == "all" {
		for _, s := range knownStages {
			out[s] = true
		}
		return out
	}
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s == m6CoordinatorAttributionStageV1 {
			out[s] = true
			continue
		}
		matched := false
		for _, known := range knownStages {
			if s == known {
				out[s] = true
				matched = true
			}
		}
		if !matched {
			return map[string]bool{}
		}
	}
	return out
}
func parseInts(raw string) ([]int, error) {
	var out []int
	for _, s := range strings.Split(raw, ",") {
		n, e := strconv.Atoi(strings.TrimSpace(s))
		if e != nil {
			return nil, e
		}
		out = append(out, n)
	}
	return out, nil
}

func allUnique[T comparable](values []T) bool {
	seen := make(map[T]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

// m8WarmupCountAndConcurrencyV1 preserves an explicit zero-warmup request;
// otherwise it primes the approximate router at the greatest measured client
// concurrency before timed cells begin.
func m8WarmupCountAndConcurrencyV1(cfg config) (count, concurrency int) {
	concurrency = 1
	for _, value := range cfg.concurrency {
		concurrency = max(concurrency, value)
	}
	if cfg.warmup == 0 {
		return 0, concurrency
	}
	return max(cfg.warmup, concurrency), concurrency
}

func parseFloats(raw string) ([]float64, error) {
	var out []float64
	for _, s := range strings.Split(raw, ",") {
		n, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if e != nil {
			return nil, e
		}
		out = append(out, n)
	}
	return out, nil
}
func provenance() (string, string, error) {
	return provenanceWithExplicitV1("", "")
}

func provenanceWithExplicitV1(base, head string) (string, string, error) {
	if (base == "") != (head == "") || (base != "" && (!validLowerSHA(base) || !validLowerSHA(head))) {
		return "", "", errors.New("explicit provenance requires lowercase base/head SHAs")
	}
	if base != "" {
		return base, head, nil
	}
	base, head = os.Getenv("BASE_SHA"), os.Getenv("GITHUB_SHA")
	if eventPath := os.Getenv("GITHUB_EVENT_PATH"); eventPath != "" {
		eventBase, eventHead, isPullRequest, err := pullRequestSHAsFromEvent(eventPath)
		if err != nil {
			return "", "", fmt.Errorf("GitHub event provenance: %w", err)
		}
		if isPullRequest {
			base, head = eventBase, eventHead
		}
	}
	if head == "" {
		out, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			return "", "", errors.New("git head unavailable: set GITHUB_SHA")
		}
		head = strings.TrimSpace(string(out))
	}
	if base == "" {
		out, err := exec.Command("git", "merge-base", "HEAD", "origin/main").Output()
		if err != nil {
			return "", "", errors.New("git merge-base unavailable: set BASE_SHA")
		}
		base = strings.TrimSpace(string(out))
	}
	if !validSHA(base) {
		return "", "", errors.New("invalid BASE_SHA")
	}
	if !validSHA(head) {
		return "", "", errors.New("invalid GITHUB_SHA")
	}
	return base, head, nil
}

func validSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validLowerSHA(value string) bool {
	return value == strings.ToLower(value) && validSHA(value)
}

// commandWithProvenanceV1 records the resolved provenance instead of relying
// on the replay caller's checkout or environment.
func commandWithProvenanceV1(command string, args []string, base, head string) []string {
	out := make([]string, 0, len(args)+5)
	out = append(out, command)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if commandProvenanceFlagV1(arg, "base-sha") || commandProvenanceFlagV1(arg, "head-sha") {
			if !strings.Contains(arg, "=") && i+1 < len(args) {
				i++
			}
			continue
		}
		out = append(out, arg)
	}
	return append(out, "-base-sha", base, "-head-sha", head)
}

func commandWithProvenanceAndSourceCheckoutV1(command string, args []string, base, head, sourceCheckout string) []string {
	out := commandWithProvenanceV1(command, args, base, head)
	filtered := out[:1]
	for i := 1; i < len(out); i++ {
		arg := out[i]
		if commandProvenanceFlagV1(arg, "source-checkout") {
			if !strings.Contains(arg, "=") && i+1 < len(out) {
				i++
			}
			continue
		}
		filtered = append(filtered, arg)
	}
	return append(filtered, "-source-checkout", sourceCheckout)
}

func m8SourceCheckoutV1(path, head string) (string, error) {
	checkout, err := m8CanonicalPathV1(path)
	if err != nil {
		return "", err
	}
	rootRaw, err := exec.Command("git", "-C", checkout, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", errors.New("must be a Git checkout")
	}
	root, err := m8CanonicalPathV1(strings.TrimSpace(string(rootRaw)))
	if err != nil || root != checkout {
		return "", errors.New("must be the Git toplevel")
	}
	headRaw, err := exec.Command("git", "-C", checkout, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(headRaw)) != head {
		return "", errors.New("HEAD does not match -head-sha")
	}
	return checkout, nil
}

func commandProvenanceFlagV1(arg, name string) bool {
	return arg == "-"+name || arg == "--"+name || strings.HasPrefix(arg, "-"+name+"=") || strings.HasPrefix(arg, "--"+name+"=")
}

func pullRequestSHAsFromEvent(path string) (string, string, bool, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", "", false, err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxGitHubEventBytes+1))
	if err != nil {
		return "", "", false, err
	}
	if int64(len(raw)) > maxGitHubEventBytes {
		return "", "", false, fmt.Errorf("event payload exceeds %d-byte cap", maxGitHubEventBytes)
	}
	var event struct {
		PullRequest *struct {
			Base struct {
				SHA string `json:"sha"`
			} `json:"base"`
			Head struct {
				SHA string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&event); err != nil {
		return "", "", false, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", "", false, errors.New("GitHub event has trailing JSON")
	}
	if event.PullRequest == nil {
		return "", "", false, nil
	}
	return strings.TrimSpace(event.PullRequest.Base.SHA), strings.TrimSpace(event.PullRequest.Head.SHA), true, nil
}

func loadFixture(dir string) (fixtureManifest, error) {
	f, err := os.Open(filepath.Join(dir, "fixture_manifest.json"))
	if err != nil {
		return fixtureManifest{}, err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, maxManifestBytes+1))
	if err != nil {
		return fixtureManifest{}, err
	}
	if int64(len(b)) > maxManifestBytes {
		return fixtureManifest{}, fmt.Errorf("fixture manifest exceeds %d-byte cap", maxManifestBytes)
	}
	var m fixtureManifest
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return m, err
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return m, errors.New("fixture manifest has trailing JSON")
	}
	return m, nil
}
func decodeResult(raw []byte) (runResult, error) {
	var r runResult
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return r, errors.New("trailing JSON")
	}
	return r, validateResult(r)
}
func validateFixture(m fixtureManifest) error {
	return validateFixtureWithCaps(m, maxVectors, maxFixtureBytes)
}
func validateFixtureWithCaps(m fixtureManifest, capVectors int, capBytes int64) error {
	if err := validateFixtureSyntax(m, capVectors); err != nil {
		return err
	}
	if m.Queries > capVectors {
		return errors.New("unsupported or malformed fixture manifest")
	}
	if int64(m.Vectors)+int64(m.Queries) > int64(capVectors) {
		return errors.New("combined fixture vector/query count exceeds pre-allocation cap")
	}
	if int64(m.Vectors)+int64(m.Queries) > capBytes/(int64(m.Dimensions)*8) {
		return errors.New("fixture float64 data alone exceeds pre-allocation memory cap")
	}
	return nil
}

// validatePartitionFixtureWithCaps applies only syntax/integrity and vector
// corpus allocation caps. Partition mode never generates the query/truth
// stream, so simulation-only query count and byte limits do not apply here.
func validatePartitionFixtureWithCaps(m fixtureManifest, capVectors int, capBytes int64) error {
	if err := validateFixtureSyntax(m, capVectors); err != nil {
		return err
	}
	if int64(m.Vectors) > capBytes/(int64(m.Dimensions)*8) {
		return errors.New("partition vector data exceeds pre-allocation memory cap")
	}
	return nil
}

// validatePartitionTruthOracleWithCaps preflights the query matrix, exact scan,
// and bounded binary searches over canonical graph rows. It must run before
// deterministicFixture allocates either matrix.
func validatePartitionTruthOracleWithCaps(m fixtureManifest, partitions, topK, graphDegree, capVectors int, capWork, capBytes int64) error {
	if partitions < 1 || topK < 1 || topK > m.Vectors {
		return errors.New("partition truth oracle requires positive partitions/top-k and top-k cannot exceed fixture vectors")
	}
	if err := validateM3FixtureWithCaps(m, capVectors, capBytes); err != nil {
		return fmt.Errorf("partition truth oracle fixture: %w", err)
	}
	exactWork, err := memoryMul(int64(m.Vectors), int64(m.Queries))
	if err != nil {
		return fmt.Errorf("partition truth oracle work: %w", err)
	}
	adjacencyWork, err := partitionTruthAdjacencyComparisons(m.Queries, topK, graphDegree)
	if err != nil {
		return fmt.Errorf("partition truth oracle work: %w", err)
	}
	graphWork, err := partitionTruthGraphDiagnosticWork(m.Vectors, graphDegree)
	if err != nil {
		return fmt.Errorf("partition truth oracle work: %w", err)
	}
	rankingWork, err := partitionTruthRankingWork(m.Queries, partitions, topK)
	if err != nil {
		return fmt.Errorf("partition truth oracle work: %w", err)
	}
	work, err := memoryAdd(exactWork, adjacencyWork, graphWork, rankingWork)
	if err != nil {
		return fmt.Errorf("partition truth oracle work: %w", err)
	}
	if capWork < 1 || work > capWork {
		return fmt.Errorf("partition truth oracle exceeds %d-unit cap: exact_vector_query_pairs=%d adjacency_comparisons=%d graph_diagnostic_work=%d partition_ranking_work=%d total=%d", capWork, exactWork, adjacencyWork, graphWork, rankingWork, work)
	}
	return nil
}

func partitionTruthAdjacencyComparisons(queries, topK, graphDegree int) (int64, error) {
	if queries < 1 || topK < 1 || graphDegree < 1 {
		return 0, errors.New("invalid partition truth adjacency work input")
	}
	return memoryMul(int64(queries), int64(topK-1), int64(bits.Len(uint(graphDegree))))
}

func partitionTruthGraphDiagnosticWork(vectors, graphDegree int) (int64, error) {
	if vectors < 1 || graphDegree < 1 {
		return 0, errors.New("invalid partition graph diagnostic work input")
	}
	edges, err := memoryMul(int64(vectors), int64(graphDegree))
	if err != nil {
		return 0, err
	}
	edgeWork, err := memoryMul(edges, int64(2+bits.Len(uint(graphDegree))))
	if err != nil {
		return 0, err
	}
	return memoryAdd(int64(vectors), edgeWork)
}

func partitionTruthProbeBudgets(partitions int) []int {
	quarterProbes := max(1, partitions/4)
	candidates := []int{1, 2, 4, 8, 16, quarterProbes}
	sort.Ints(candidates)
	budgets := make([]int, 0, len(candidates))
	for _, probes := range candidates {
		probes = min(probes, partitions)
		if len(budgets) == 0 || budgets[len(budgets)-1] != probes {
			budgets = append(budgets, probes)
		}
	}
	return budgets
}

func partitionTruthRankingWork(queries, partitions, topK int) (int64, error) {
	if queries < 1 || partitions < 1 || topK < 1 {
		return 0, errors.New("invalid partition truth ranking work input")
	}
	probeBudgets := partitionTruthProbeBudgets(partitions)
	var probeSelections int64
	for _, probes := range probeBudgets {
		var err error
		probeSelections, err = memoryAdd(probeSelections, int64(probes))
		if err != nil {
			return 0, err
		}
	}
	// Charge a conservative quadratic comparison bound for each partition sort,
	// three complete partition scans, and every probe-row selection/truth walk.
	sortWork, err := memoryMul(int64(partitions), int64(partitions))
	if err != nil {
		return 0, err
	}
	partitionScans, err := memoryMul(3, int64(partitions))
	if err != nil {
		return 0, err
	}
	probeTruthWalks, err := memoryMul(int64(len(probeBudgets)), int64(topK))
	if err != nil {
		return 0, err
	}
	perQuery, err := memoryAdd(
		sortWork,
		partitionScans,
		probeSelections,
		probeTruthWalks,
	)
	if err != nil {
		return 0, err
	}
	return memoryMul(int64(queries), perQuery)
}

// validateM3FixtureWithCaps permits the declared 1M-vector corpus plus a
// separately bounded query set. Unlike simulation mode, M3's corpus cap is a
// vector count rather than a combined vector/query count; the byte cap still
// covers both generated matrices before allocation.
func validateM3FixtureWithCaps(m fixtureManifest, capVectors int, capBytes int64) error {
	if err := validateFixtureSyntax(m, capVectors); err != nil {
		return err
	}
	if m.Queries > capVectors {
		return errors.New("unsupported or malformed fixture manifest")
	}
	rows := int64(m.Vectors) + int64(m.Queries)
	if rows > capBytes/(int64(m.Dimensions)*8) {
		return errors.New("M3 fixture float64 data exceeds pre-allocation memory cap")
	}
	return nil
}

func validateFixtureSyntax(m fixtureManifest, capVectors int) error {
	if m.SchemaVersion != schemaVersion || m.Fixture == "" || !supportedFixtureGeneratorV1(m.Generator) || m.Arithmetic != fixtureArithmetic || m.Vectors < 1 || m.Vectors > capVectors || m.Queries < 1 || m.Dimensions < 1 || m.Dimensions > maxDimensions || m.Metric != "cosine" || len(m.Checksum) != 64 {
		return errors.New("unsupported or malformed fixture manifest")
	}
	_, e := hex.DecodeString(m.Checksum)
	return e
}

func supportedFixtureGeneratorV1(generator string) bool {
	return generator == fixtureGenerator || generator == qualificationSyntheticGeneratorV1 || generator == qualificationEmbeddingGeneratorV1
}

type benchmarkWorkPlan struct {
	VectorQueryPairs  int64
	CorpusPasses      int64
	VectorQueryVisits int64
}

type m3BenchmarkWorkPlan struct {
	ChecksumVectorQueryVisits   int64
	MembershipVectorQueryVisits int64
	TruthAdjacencyComparisons   int64
	GraphDiagnosticWorkUnits    int64
	TruthPartitionRankingWork   int64
	VectorQueryVisits           int64
}

type m8BenchmarkWorkPlan struct {
	FixtureChecksumVectorVisits       int64
	ExactTruthVectorVisits            int64
	ExactWorkVectorVisits             int64
	MeasuredQueryRequests             int64
	WarmupAndPreflightQueryRequests   int64
	AttributionQueryPasses            int64
	MaxMembershipOracleSubsets        int64
	MembershipOracleSubsetEvaluations int64
	MembershipOracleWorkUnits         int64
	SelectedPartitionSetupWorkUnits   int64
	AttributionLinearWorkUnits        int64
	FinalMembershipLinearScans        int64
	FinalMembershipPairComparisons    int64
	AttributionDiagnosticWorkUnits    int64
	QueryRequests                     int64
	RetainedCoordinatorCells          int64
	RetainedCoordinatorResults        int64
	PreflightResponseBytes            int64
	PreflightQueryConversionBytes     int64
	CurrentCellOutcomes               int64
	CurrentCellOutcomeBytes           int64
	CurrentQueryConversionBytes       int64
	FixtureResidentBytes              int64
	SourceSnapshotBytes               int64
	ExactTruthBytes                   int64
	RetainedCoordinatorBytes          int64
	RetainedAttributionMatrices       int64
	RetainedAttributionResults        int64
	RetainedAttributionBytes          int64
	AttributionMergeScratchResults    int64
	AttributionMergeScratchBytes      int64
	AttributionLiveResultSets         int64
	AttributionLiveResultBytes        int64
	AttributionApproximateRouteBytes  int64
	AttributionQueryConversionBytes   int64
	AttributionPrimaryHomeMapBytes    int64
	AttributionFinalMembershipBytes   int64
	AttributionHomeBuildScratchBytes  int64
	ModeledPeakBytes                  int64
}

func validateM8BenchmarkWork(cfg config, m fixtureManifest, capUnits, capBytes int64) (m8BenchmarkWorkPlan, error) {
	if capUnits < 1 || capBytes < 1 || cfg.partitions < 1 || cfg.topK < 1 || m.Vectors < 1 || m.Queries < 1 || m.Dimensions < 1 || len(cfg.overlaps) == 0 || len(cfg.probes) == 0 || len(cfg.efSearch) == 0 || len(cfg.concurrency) == 0 {
		return m8BenchmarkWorkPlan{}, errors.New("cannot plan M8 benchmark work without positive caps, a valid fixture/top-k, and complete sweeps")
	}
	plan := m8BenchmarkWorkPlan{}
	variantRuns := int64(1)
	var supportedOverlaps int64
	for _, overlap := range cfg.overlaps {
		if overlap == 0 {
			supportedOverlaps++
		}
	}
	if cfg.m8ExistingDB != "" {
		// A retained descriptor supplies the materialized overlap policy. The
		// single-variant runner validates that the one configured overlap equals
		// that descriptor, including for a nonzero overlap child.
		supportedOverlaps = 1
	}
	if len(cfg.m8VariantDBs) > 0 {
		// Each immutable matrix variant executes the complete single-overlap
		// child path in its own process. Work is cumulative across children,
		// while all retained result and attribution matrices below model the
		// peak of one sequential child.
		variantRuns = int64(len(cfg.m8VariantDBs))
		supportedOverlaps = 1
	}
	var membershipOracleSubsetsPerSweep int64
	var membershipOracleWorkPerQuerySweep int64
	var probeSum int64
	truthWords := int64((cfg.topK + 63) / 64)
	for _, probes := range cfg.probes {
		if probes < 1 || probes > cfg.partitions {
			return plan, errors.New("cannot plan M8 benchmark work with an invalid probe count")
		}
		nextProbeSum, err := memoryAdd(probeSum, int64(probes))
		if err != nil {
			return plan, err
		}
		probeSum = nextProbeSum
		if supportedOverlaps == 0 {
			continue
		}
		combinations, err := m8MembershipOracleCombinationCountV1(cfg.partitions, probes, maxBenchmarkWorkUnits)
		if err != nil {
			return plan, err
		}
		plan.MaxMembershipOracleSubsets = max(plan.MaxMembershipOracleSubsets, combinations)
		membershipOracleSubsetsPerSweep, err = memoryAdd(membershipOracleSubsetsPerSweep, combinations)
		if err != nil {
			return plan, err
		}
		membershipPreparation, err := memoryMul(int64(cfg.topK), int64(cfg.partitions))
		if err != nil {
			return plan, err
		}
		oracleWork := membershipPreparation
		if probes != cfg.partitions {
			// The pruned combination walk has C(partitions+1, probes)
			// calls including its root and one popcount per leaf.
			nodes, err := m8MembershipOracleCombinationCountV1(cfg.partitions+1, probes, maxBenchmarkWorkUnits)
			if err != nil {
				return plan, err
			}
			wordOperations, err := memoryMul(nodes-1+combinations, truthWords)
			if err != nil {
				return plan, err
			}
			oracleWork, err = memoryAdd(oracleWork, wordOperations)
			if err != nil {
				return plan, err
			}
		}
		// The primary-home oracle counts each truth result, sorts every
		// partition count, and walks the selected probe counts. Charge a
		// conservative quadratic sort bound so the shared oracle cap covers
		// both the primary-home and final-membership ladders.
		primarySortWork, err := memoryMul(int64(cfg.partitions), int64(cfg.partitions))
		if err != nil {
			return plan, err
		}
		primaryOracleWork, err := memoryAdd(int64(cfg.topK), primarySortWork, int64(probes))
		if err != nil {
			return plan, err
		}
		oracleWork, err = memoryAdd(oracleWork, primaryOracleWork)
		if err != nil {
			return plan, err
		}
		membershipOracleWorkPerQuerySweep, err = memoryAdd(membershipOracleWorkPerQuerySweep, oracleWork)
		if err != nil {
			return plan, err
		}
	}
	truthCap := cfg.m8MaxExactTruthVisits
	if truthCap == 0 { // direct planner callers retain an independent conservative cap.
		truthCap = maxBenchmarkWorkUnits
	}
	exactTruthVisits, err := memoryMul(int64(m.Vectors), int64(m.Queries))
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	maxFinalMemberships := int64(1)
	if cfg.m8ExistingDB != "" || len(cfg.m8VariantDBs) > 0 {
		maxFinalMemberships += vectorpartition.MaxOverlapMembershipsPerVector
	}
	truthPairs, err := memoryMul(int64(cfg.topK), int64(cfg.topK-1))
	if err != nil {
		return plan, err
	}
	truthPairs /= 2
	attributionCells, err := memoryMul(supportedOverlaps, variantRuns, int64(len(cfg.probes)), int64(len(cfg.efSearch)))
	if err != nil {
		return plan, err
	}
	// Every attribution cell builds selected-partition sets twice. Approximate
	// routing is preflighted across the complete query set before approximate
	// searches, so query conversion also runs once for that preflight and once
	// for the exact/diagnostic pass. The remaining
	// linear work charges query conversion, result parity/recall, primary-home
	// truth/count scans, final-membership truth/rank scans, cell setup, and the
	// coordinator parity pass. The 25 truth-result passes are 1 parity pass,
	// five 4-pass recall calculations, and two passes in each diagnostic.
	plan.SelectedPartitionSetupWorkUnits, err = memoryMul(int64(m.Queries), supportedOverlaps, variantRuns, int64(len(cfg.efSearch)), 2, probeSum)
	if err != nil {
		return plan, err
	}
	queryProjectionWork, err := memoryMul(2, int64(m.Queries), int64(m.Dimensions), attributionCells)
	if err != nil {
		return plan, err
	}
	truthResultWork, err := memoryMul(int64(m.Queries), int64(cfg.topK), attributionCells, 25)
	if err != nil {
		return plan, err
	}
	cellSetupPerCell, err := memoryAdd(int64(cfg.partitions), int64(cfg.topK))
	if err != nil {
		return plan, err
	}
	cellSetupWork, err := memoryMul(attributionCells, cellSetupPerCell)
	if err != nil {
		return plan, err
	}
	coordinatorParityWork, err := memoryMul(int64(m.Queries), int64(cfg.topK), supportedOverlaps, variantRuns, int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)))
	if err != nil {
		return plan, err
	}
	plan.AttributionLinearWorkUnits, err = memoryAdd(queryProjectionWork, truthResultWork, cellSetupWork, coordinatorParityWork)
	if err != nil {
		return plan, err
	}
	// Each final-membership list is first validated as sorted, then visited to
	// compute coverage. Pair intersections remain a separate two-list bound.
	plan.FinalMembershipLinearScans, err = memoryMul(int64(m.Queries), int64(cfg.topK), attributionCells, 2*maxFinalMemberships)
	if err != nil {
		return plan, err
	}
	plan.FinalMembershipPairComparisons, err = memoryMul(int64(m.Queries), truthPairs, attributionCells, 2*maxFinalMemberships)
	if err != nil {
		return plan, err
	}
	plan.AttributionDiagnosticWorkUnits, err = memoryAdd(plan.SelectedPartitionSetupWorkUnits, plan.AttributionLinearWorkUnits, plan.FinalMembershipLinearScans, plan.FinalMembershipPairComparisons)
	if err != nil {
		return plan, err
	}
	if plan.AttributionDiagnosticWorkUnits > capUnits {
		return plan, fmt.Errorf("modeled M8 attribution diagnostics exceed %d-operation cap: truth_results_per_query=%d truth_pairs_per_query=%d attribution_cells=%d max_memberships_per_truth_result=%d selected_partition_setup=%d linear_bookkeeping=%d linear_membership_scans=%d pair_comparisons=%d total=%d", capUnits, cfg.topK, truthPairs, attributionCells, maxFinalMemberships, plan.SelectedPartitionSetupWorkUnits, plan.AttributionLinearWorkUnits, plan.FinalMembershipLinearScans, plan.FinalMembershipPairComparisons, plan.AttributionDiagnosticWorkUnits)
	}
	plan.MembershipOracleSubsetEvaluations, err = memoryMul(membershipOracleSubsetsPerSweep, int64(m.Queries), supportedOverlaps, variantRuns)
	if err != nil {
		return plan, err
	}
	plan.MembershipOracleWorkUnits, err = memoryMul(membershipOracleWorkPerQuerySweep, int64(m.Queries), supportedOverlaps, variantRuns)
	if err != nil {
		return plan, err
	}
	if plan.MembershipOracleWorkUnits > capUnits {
		return plan, fmt.Errorf("modeled M8 membership-oracle work exceeds %d-operation cap: max_subsets_per_query=%d subset_evaluations=%d bounded_operations=%d", capUnits, plan.MaxMembershipOracleSubsets, plan.MembershipOracleSubsetEvaluations, plan.MembershipOracleWorkUnits)
	}
	// Every child reconstructs and checksum-binds its fixture, then computes the
	// canonical exact oracle. Cache reuse avoids a compute in the common path but
	// never weakens the preflight bound for the worst admissible execution.
	fixtureChecksumVisits, err := memoryMul(exactTruthVisits, variantRuns)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	canonicalTruthVisits, err := memoryMul(exactTruthVisits, variantRuns)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	exactWorkVisits, err := memoryAdd(fixtureChecksumVisits, canonicalTruthVisits)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	if exactWorkVisits > truthCap {
		return m8BenchmarkWorkPlan{FixtureChecksumVectorVisits: fixtureChecksumVisits, ExactTruthVectorVisits: canonicalTruthVisits, ExactWorkVectorVisits: exactWorkVisits}, fmt.Errorf("modeled M8 exact source-query work exceeds %d-visit cap: fixture_checksum_vector_visits=%d exact_truth_vector_visits=%d exact_work_vector_visits=%d; set -m8-max-exact-truth-visits explicitly for a declared qualification run", truthCap, fixtureChecksumVisits, canonicalTruthVisits, exactWorkVisits)
	}
	measuredPerRun, err := memoryMul(supportedOverlaps, int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)), int64(m.Queries))
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	measured, err := memoryMul(measuredPerRun, variantRuns)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	// Production M8 also performs one exhaustive endpoint preflight and, when
	// enabled, enough approximate warmup requests to prime the largest measured
	// client pool.
	warmupCount, warmupConcurrency := m8WarmupCountAndConcurrencyV1(cfg)
	warmupAndPreflightPerRun, err := memoryAdd(int64(warmupCount), 1)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	warmupAndPreflight, err := memoryMul(warmupAndPreflightPerRun, variantRuns)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	// Attribution runs once after measured overlap/concurrency rows. For every
	// query/probe/ef cell it executes exact-route exact, approximate-route exact,
	// exact-route local-HNSW, and approximate-route local-HNSW passes;
	// the exhaustive partition-union pass is cached once per query.
	var attributionPerRun int64
	if supportedOverlaps > 0 {
		attributionStages, stageErr := memoryMul(4, int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(m.Queries))
		if stageErr != nil {
			return m8BenchmarkWorkPlan{}, stageErr
		}
		attributionPerRun, err = memoryAdd(int64(m.Queries), attributionStages)
		if err != nil {
			return m8BenchmarkWorkPlan{}, err
		}
	}
	attribution, err := memoryMul(attributionPerRun, variantRuns)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	requests, err := memoryAdd(measured, warmupAndPreflight, attribution)
	if err != nil {
		return m8BenchmarkWorkPlan{}, err
	}
	plan.FixtureChecksumVectorVisits = fixtureChecksumVisits
	plan.ExactTruthVectorVisits = canonicalTruthVisits
	plan.ExactWorkVectorVisits = exactWorkVisits
	plan.MeasuredQueryRequests = measured
	plan.WarmupAndPreflightQueryRequests = warmupAndPreflight
	plan.AttributionQueryPasses = attribution
	plan.QueryRequests = requests
	if requests > capUnits {
		return plan, fmt.Errorf("modeled M8 benchmark work exceeds %d-unit cap (%s): measured_query_requests=%d warmup_and_preflight_query_requests=%d attribution_query_passes=%d query_requests=%d", capUnits, m8BenchmarkWorkScope, measured, warmupAndPreflight, attribution, requests)
	}

	rows, err := memoryAdd(int64(m.Vectors), int64(m.Queries))
	if err != nil {
		return plan, err
	}
	fixtureValues, err := memoryMul(rows, int64(m.Dimensions), 8)
	if err != nil {
		return plan, err
	}
	fixtureRows, err := memoryMul(rows, int64(unsafe.Sizeof([]float64{})))
	if err != nil {
		return plan, err
	}
	plan.FixtureResidentBytes, err = memoryAdd(fixtureValues, fixtureRows)
	if err != nil {
		return plan, err
	}
	sourceRowBytes, err := memoryAdd(
		int64(unsafe.Sizeof(collections.VectorPartitionRouterSourceRowV1{})),
		documentIDStorageBytes,
		int64(m.Dimensions)*4,
	)
	if err != nil {
		return plan, err
	}
	plan.SourceSnapshotBytes, err = memoryMul(int64(m.Vectors), sourceRowBytes)
	if err != nil {
		return plan, err
	}

	// M8 owns the deterministic doc-%06d corpus even when reopening retained
	// assets, so documentIDStorageBytes bounds its result string storage. The
	// much larger transport stable-ID limit is not reachable by this harness.
	resultBytes, err := memoryAdd(int64(unsafe.Sizeof(m8CanonicalResultV1{})), documentIDStorageBytes)
	if err != nil {
		return plan, err
	}
	perQueryResults, err := memoryMul(int64(cfg.topK), resultBytes)
	if err != nil {
		return plan, err
	}
	perQueryResults, err = memoryAdd(int64(unsafe.Sizeof([]m8CanonicalResultV1{})), perQueryResults)
	if err != nil {
		return plan, err
	}
	plan.ExactTruthBytes, err = memoryMul(int64(m.Queries), perQueryResults)
	if err != nil {
		return plan, err
	}

	plan.RetainedCoordinatorCells, err = memoryMul(supportedOverlaps, int64(len(cfg.probes)), int64(len(cfg.efSearch)), int64(len(cfg.concurrency)))
	if err != nil {
		return plan, err
	}
	plan.RetainedCoordinatorResults, err = memoryMul(plan.RetainedCoordinatorCells, int64(m.Queries), int64(cfg.topK))
	if err != nil {
		return plan, err
	}
	perResultMatrix, err := memoryMul(int64(m.Queries), perQueryResults)
	if err != nil {
		return plan, err
	}
	perCellResults, err := memoryAdd(int64(unsafe.Sizeof(m8MeasuredCellV1{})), perResultMatrix)
	if err != nil {
		return plan, err
	}
	plan.RetainedCoordinatorBytes, err = memoryMul(plan.RetainedCoordinatorCells, perCellResults)
	if err != nil {
		return plan, err
	}
	maxMeasuredProbes, maxEFSearch, maxConcurrency := 0, 0, 0
	for _, probes := range cfg.probes {
		maxMeasuredProbes = max(maxMeasuredProbes, probes)
	}
	for _, efSearch := range cfg.efSearch {
		maxEFSearch = max(maxEFSearch, efSearch)
	}
	for _, concurrency := range cfg.concurrency {
		maxConcurrency = max(maxConcurrency, concurrency)
	}
	// The exhaustive endpoint preflight is a distinct, single-response phase.
	// Timed and warmup requests use at most the configured probe count.
	neighborBytes, err := memoryMul(int64(cfg.topK), int64(unsafe.Sizeof(nativewire.VectorPartitionCoordinatorNeighborV1{}))+documentIDStorageBytes)
	if err != nil {
		return plan, err
	}
	responseBytes := func(base int64, probes, requestIDBytes int) (int64, error) {
		partitionBytes, err := memoryMul(int64(probes), int64(unsafe.Sizeof(uint32(0))))
		if err != nil {
			return 0, err
		}
		groups := min(probes, cfg.raftGroups)
		if groups < 1 {
			groups = probes // conservative for direct planner callers without parsed defaults
		}
		groupBytes, err := memoryMul(int64(groups), int64(unsafe.Sizeof("")))
		if err != nil {
			return 0, err
		}
		return memoryAdd(base, neighborBytes, partitionBytes, groupBytes, int64(requestIDBytes))
	}
	plan.PreflightResponseBytes, err = responseBytes(int64(unsafe.Sizeof(nativewire.VectorPartitionCoordinatorResponseV1{})), cfg.partitions, len("m8-endpoint-preflight"))
	if err != nil {
		return plan, err
	}
	perOutcomeBytes, err := responseBytes(int64(unsafe.Sizeof(m8ProductionCellOutcomeV1{})), maxMeasuredProbes, len(fmt.Sprintf("m8-q-%06d-p-%04d-ef-%06d-c-%03d", max(0, m.Queries-1), maxMeasuredProbes, maxEFSearch, maxConcurrency)))
	if err != nil {
		return plan, err
	}
	// Timed cells retain one response per query. Enabled warmup workers can
	// concurrently hold full responses before each worker summarizes one, so
	// charge that larger transient response set too.
	measuredOutcomes, activeMeasuredWorkers := 0, 0
	if supportedOverlaps > 0 {
		measuredOutcomes, activeMeasuredWorkers = m.Queries, min(m.Queries, maxConcurrency)
	}
	plan.CurrentCellOutcomes = int64(max(measuredOutcomes, min(warmupCount, warmupConcurrency)))
	plan.CurrentCellOutcomeBytes, err = memoryMul(plan.CurrentCellOutcomes, perOutcomeBytes)
	if err != nil {
		return plan, err
	}
	activeWarmupWorkers := min(warmupCount, warmupConcurrency)
	plan.CurrentQueryConversionBytes, err = memoryMul(int64(max(activeMeasuredWorkers, activeWarmupWorkers)), int64(m.Dimensions), 4)
	if err != nil {
		return plan, err
	}
	plan.PreflightQueryConversionBytes, err = memoryMul(int64(m.Dimensions), 4)
	if err != nil {
		return plan, err
	}
	if supportedOverlaps > 0 {
		plan.AttributionQueryConversionBytes, err = memoryMul(int64(m.Dimensions), 4)
		if err != nil {
			return plan, err
		}
		maxProbes := 0
		for _, probes := range cfg.probes {
			maxProbes = max(maxProbes, probes)
		}
		truthIDs, truthIDErr := memoryMul(int64(m.Queries), int64(cfg.topK))
		if truthIDErr != nil {
			return plan, truthIDErr
		}
		truthIDs = min(truthIDs, int64(m.Vectors))
		plan.AttributionPrimaryHomeMapBytes, err = memoryMul(truthIDs, memoryMapEntryBytes)
		if err != nil {
			return plan, err
		}
		// Conservatively allow every overlap membership in the corpus to land on
		// a truth result; actual M1 per-vector caps only reduce this bound.
		membershipValues, membershipErr := memoryAdd(truthIDs, int64(m.Vectors))
		if membershipErr != nil {
			return plan, membershipErr
		}
		finalMapEntries, mapErr := memoryMul(truthIDs, memoryMapEntryBytes+int64(unsafe.Sizeof([]uint32{})))
		if mapErr != nil {
			return plan, mapErr
		}
		// append growth keeps backing capacity below twice the retained length.
		membershipStorage, storageErr := memoryMul(2, membershipValues, int64(unsafe.Sizeof(uint32(0))))
		if storageErr != nil {
			return plan, storageErr
		}
		plan.AttributionFinalMembershipBytes, err = memoryAdd(finalMapEntries, membershipStorage)
		if err != nil {
			return plan, err
		}
		plan.AttributionHomeBuildScratchBytes, err = memoryMul(truthIDs, 3*memoryMapEntryBytes)
		if err != nil {
			return plan, err
		}
		attributionCells, attributionErr := memoryMul(int64(len(cfg.probes)), int64(len(cfg.efSearch)))
		if attributionErr != nil {
			return plan, attributionErr
		}
		plan.RetainedAttributionMatrices, err = memoryAdd(attributionCells, 1) // local matrices plus cached exhaustive union
		if err != nil {
			return plan, err
		}
		plan.RetainedAttributionResults, err = memoryMul(plan.RetainedAttributionMatrices, int64(m.Queries), int64(cfg.topK))
		if err != nil {
			return plan, err
		}
		plan.RetainedAttributionBytes, err = memoryMul(plan.RetainedAttributionMatrices, perResultMatrix)
		if err != nil {
			return plan, err
		}
		attributionMetadata, err := memoryMul(attributionCells, int64(unsafe.Sizeof(m8AttributionCellV1{}))+memoryMapEntryBytes+32)
		if err != nil {
			return plan, err
		}
		plan.RetainedAttributionBytes, err = memoryAdd(plan.RetainedAttributionBytes, attributionMetadata)
		if err != nil {
			return plan, err
		}
		// Exact-route exact, approximate-route exact, and exact-route local
		// results remain live while approximate-route local search runs. Each
		// result owns its canonical IDs; the approximate local result is retained
		// directly in its matrix and is already included above.
		plan.AttributionLiveResultSets = 3
		plan.AttributionLiveResultBytes, err = memoryMul(plan.AttributionLiveResultSets, perQueryResults)
		if err != nil {
			return plan, err
		}
		// The all-query approximate-route preflight retains one selected partition
		// slice per query until coverage is known, avoiding a second route before
		// the approximate search stages.
		routeValues, routeErr := memoryMul(int64(m.Queries), int64(maxProbes), int64(unsafe.Sizeof(uint32(0))))
		if routeErr != nil {
			return plan, routeErr
		}
		routeHeaders, routeErr := memoryMul(int64(m.Queries), int64(unsafe.Sizeof([]uint32{})))
		if routeErr != nil {
			return plan, routeErr
		}
		plan.AttributionApproximateRouteBytes, err = memoryAdd(routeValues, routeHeaders)
		if err != nil {
			return plan, err
		}
		// The exhaustive attribution search is the largest merge. It reserves one
		// partition-count by top-k input buffer, canonicalization copies that full
		// buffer, and the input retains one owned document-ID allocation per
		// candidate. Duplicate suppression holds at most top-k map entries. The
		// returned top-k is copied into a right-sized retained matrix above.
		plan.AttributionMergeScratchResults, err = memoryMul(2, int64(cfg.partitions), int64(cfg.topK))
		if err != nil {
			return plan, err
		}
		plan.AttributionMergeScratchBytes, err = memoryMul(plan.AttributionMergeScratchResults, int64(unsafe.Sizeof(m8CanonicalResultV1{})))
		if err != nil {
			return plan, err
		}
		attributionMergeIDBytes, err := memoryMul(int64(cfg.partitions), int64(cfg.topK), documentIDStorageBytes)
		if err != nil {
			return plan, err
		}
		attributionMergeMapBytes, err := memoryMul(int64(cfg.topK), memoryMapEntryBytes)
		if err != nil {
			return plan, err
		}
		plan.AttributionMergeScratchBytes, err = memoryAdd(plan.AttributionMergeScratchBytes, attributionMergeIDBytes, attributionMergeMapBytes)
		if err != nil {
			return plan, err
		}
	}
	// The owned FP32 source snapshot remains live while exact truth accumulates;
	// it is released before measured coordinator results begin accumulating.
	sourceOraclePeak, err := memoryAdd(plan.FixtureResidentBytes, plan.SourceSnapshotBytes, plan.ExactTruthBytes)
	if err != nil {
		return plan, err
	}
	measurementBase, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorBytes)
	if err != nil {
		return plan, err
	}
	measurementPeak, err := memoryAdd(measurementBase, plan.CurrentCellOutcomeBytes, plan.CurrentQueryConversionBytes)
	if err != nil {
		return plan, err
	}
	preflightPeak, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.PreflightResponseBytes, plan.PreflightQueryConversionBytes)
	if err != nil {
		return plan, err
	}
	attributionHomeBuildPeak, err := memoryAdd(measurementBase, plan.SourceSnapshotBytes, plan.AttributionPrimaryHomeMapBytes, plan.AttributionFinalMembershipBytes, plan.AttributionHomeBuildScratchBytes)
	if err != nil {
		return plan, err
	}
	attributionPeak, err := memoryAdd(measurementBase, plan.AttributionPrimaryHomeMapBytes, plan.AttributionFinalMembershipBytes, plan.RetainedAttributionBytes, plan.AttributionMergeScratchBytes, plan.AttributionLiveResultBytes, plan.AttributionApproximateRouteBytes, plan.AttributionQueryConversionBytes)
	if err != nil {
		return plan, err
	}
	peak := max(sourceOraclePeak, preflightPeak, measurementPeak, attributionHomeBuildPeak, attributionPeak)
	plan.ModeledPeakBytes, err = memoryScaleCeil(peak, memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		return plan, err
	}
	if plan.ModeledPeakBytes > capBytes {
		return plan, fmt.Errorf("modeled M8 benchmark-owned memory %d exceeds -max-fixture-bytes %d: fixture_resident_bytes=%d source_snapshot_bytes=%d exact_truth_bytes=%d retained_coordinator_cells=%d retained_coordinator_results=%d retained_coordinator_bytes=%d preflight_response_bytes=%d preflight_query_conversion_bytes=%d current_cell_outcomes=%d current_cell_outcome_bytes=%d current_query_conversion_bytes=%d retained_attribution_matrices=%d retained_attribution_results=%d retained_attribution_bytes=%d attribution_merge_scratch_results=%d attribution_merge_scratch_bytes=%d attribution_live_result_sets=%d attribution_live_result_bytes=%d attribution_approximate_route_bytes=%d attribution_query_conversion_bytes=%d attribution_primary_home_map_bytes=%d attribution_final_membership_bytes=%d attribution_home_build_scratch_bytes=%d", plan.ModeledPeakBytes, capBytes, plan.FixtureResidentBytes, plan.SourceSnapshotBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorCells, plan.RetainedCoordinatorResults, plan.RetainedCoordinatorBytes, plan.PreflightResponseBytes, plan.PreflightQueryConversionBytes, plan.CurrentCellOutcomes, plan.CurrentCellOutcomeBytes, plan.CurrentQueryConversionBytes, plan.RetainedAttributionMatrices, plan.RetainedAttributionResults, plan.RetainedAttributionBytes, plan.AttributionMergeScratchResults, plan.AttributionMergeScratchBytes, plan.AttributionLiveResultSets, plan.AttributionLiveResultBytes, plan.AttributionApproximateRouteBytes, plan.AttributionQueryConversionBytes, plan.AttributionPrimaryHomeMapBytes, plan.AttributionFinalMembershipBytes, plan.AttributionHomeBuildScratchBytes)
	}
	return plan, nil
}

func validateM3BenchmarkWork(cfg config, m fixtureManifest, capUnits int64) (m3BenchmarkWorkPlan, error) {
	if capUnits < 1 || len(cfg.overlaps) == 0 {
		return m3BenchmarkWorkPlan{}, errors.New("cannot plan M3 benchmark work without a positive cap and overlap rows")
	}
	vectors := int64(m.Vectors)
	queries := int64(m.Queries)
	checksumVisits, err := memoryMul(vectors, queries)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	var membershipUpperBound int64
	for _, ratio := range cfg.overlaps {
		if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
			return m3BenchmarkWorkPlan{}, errors.New("cannot plan M3 benchmark work with an invalid overlap ratio")
		}
		budget := int64(math.Floor(ratio * float64(m.Vectors)))
		rowMemberships, err := memoryAdd(vectors, budget)
		if err != nil {
			return m3BenchmarkWorkPlan{}, err
		}
		membershipUpperBound, err = memoryAdd(membershipUpperBound, rowMemberships)
		if err != nil {
			return m3BenchmarkWorkPlan{}, err
		}
	}
	// Each M3 row scans every local membership once for the exact partition
	// oracle and once more to validate returned IDs/scores against authority.
	membershipVisits, err := memoryMul(queries, membershipUpperBound, 2)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	adjacencyComparisons, err := partitionTruthAdjacencyComparisons(m.Queries, cfg.topK, cfg.partition.Degree)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	graphWork, err := partitionTruthGraphDiagnosticWork(m.Vectors, cfg.partition.Degree)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	rankingWork, err := partitionTruthRankingWork(m.Queries, cfg.partitions, cfg.topK)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	totalVisits, err := memoryAdd(checksumVisits, membershipVisits, adjacencyComparisons, graphWork, rankingWork)
	if err != nil {
		return m3BenchmarkWorkPlan{}, err
	}
	plan := m3BenchmarkWorkPlan{
		ChecksumVectorQueryVisits:   checksumVisits,
		MembershipVectorQueryVisits: membershipVisits,
		TruthAdjacencyComparisons:   adjacencyComparisons,
		GraphDiagnosticWorkUnits:    graphWork,
		TruthPartitionRankingWork:   rankingWork,
		VectorQueryVisits:           totalVisits,
	}
	if totalVisits > capUnits {
		return plan, fmt.Errorf("modeled M3 benchmark work exceeds %d-unit cap: checksum_visits=%d membership_visits=%d truth_adjacency_comparisons=%d graph_diagnostic_work=%d truth_partition_ranking_work=%d",
			capUnits, checksumVisits, membershipVisits, adjacencyComparisons, graphWork, rankingWork)
	}
	return plan, nil
}

func validateBenchmarkWork(cfg config, m fixtureManifest, capUnits int64) (benchmarkWorkPlan, error) {
	if capUnits < 1 || len(cfg.probes) == 0 || len(cfg.overlaps) == 0 {
		return benchmarkWorkPlan{}, errors.New("cannot plan benchmark work without a positive cap, probes, and overlap rows")
	}
	stages := cfg.stages
	if stages == nil {
		stages = stageSet("all")
	}

	// simulate always computes global exact truth once per query. Every
	// selected exhaustive/routing helper below makes the documented number of
	// additional full corpus visits for each probe/overlap artifact row.
	passesPerSimulation := int64(1)
	if stages["partition_oracle"] {
		passesPerSimulation++
	}
	for _, stage := range []string{
		"exact_representative_routing",
		"approximate_representative_routing",
		"exact_partition_local",
		"end_to_end_distributed_simulation",
	} {
		if stages[stage] {
			passesPerSimulation += 2 // representative selection plus selected exact top-k
		}
	}
	if stages["treedb_partition_local_hnsw"] {
		passesPerSimulation++ // representative selection; HNSW engine work is excluded
	}
	if stages[m6CoordinatorAttributionStageV1] {
		// Persisted M4 representative scoring and local M5 exact shard scans.
		// Reserve one additional pass for the all-partition FP32 parity oracle.
		passesPerSimulation += 3
	}

	simulationRows, err := memoryMul(int64(len(cfg.probes)), int64(len(cfg.overlaps)))
	if err != nil {
		return benchmarkWorkPlan{}, err
	}
	simulationPasses, err := memoryMul(simulationRows, passesPerSimulation)
	if err != nil {
		return benchmarkWorkPlan{}, err
	}
	corpusPasses, err := memoryAdd(1, simulationPasses) // checksum exact truth
	if err != nil {
		return benchmarkWorkPlan{}, err
	}
	vectorQueryPairs, err := memoryMul(int64(m.Vectors), int64(m.Queries))
	if err != nil {
		return benchmarkWorkPlan{}, err
	}
	plan := benchmarkWorkPlan{
		VectorQueryPairs: vectorQueryPairs,
		CorpusPasses:     corpusPasses,
	}
	if vectorQueryPairs > capUnits/corpusPasses {
		return plan, fmt.Errorf("modeled benchmark work exceeds %d-unit cap (%s): vector_query_pairs=%d corpus_passes=%d",
			capUnits, benchmarkWorkScope, vectorQueryPairs, corpusPasses)
	}
	plan.VectorQueryVisits = vectorQueryPairs * corpusPasses
	return plan, nil
}

func planBenchmarkMemory(cfg config, m fixtureManifest) (benchmarkMemoryPlan, error) {
	if cfg.topK < 1 || cfg.topK > m.Vectors || cfg.partitions < 1 || cfg.partitions > m.Vectors || len(cfg.probes) == 0 {
		return benchmarkMemoryPlan{}, errors.New("cannot plan memory for invalid top-k, partitions, or probes")
	}
	rows, err := memoryAdd(int64(m.Vectors), int64(m.Queries))
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	fixtureValues, err := memoryMul(rows, int64(m.Dimensions), 8)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	fixtureRows, err := memoryMul(rows, int64(unsafe.Sizeof([]float64{})))
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	fixtureResident, err := memoryAdd(fixtureValues, fixtureRows)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}

	k := int64(cfg.topK)
	scoredBytes, err := memoryMul(k, int64(unsafe.Sizeof(scoredNeighbor{})))
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	resultBytes, err := memoryMul(k, int64(unsafe.Sizeof(neighbor{}))+documentIDStorageBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	// One truth result remains live while a second bounded top-k or recall
	// operation executes. This is deliberately conservative for single-stage runs.
	simulationWork, err := memoryAdd(scoredBytes, resultBytes, resultBytes, 2*k*memoryMapEntryBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}

	maxProbes := 0
	uniqueProbes := make(map[int]struct{}, len(cfg.probes))
	for _, probes := range cfg.probes {
		if probes > maxProbes {
			maxProbes = probes
		}
		uniqueProbes[probes] = struct{}{}
	}
	needsSelectedTopK := cfg.stages["partition_oracle"] ||
		cfg.stages["exact_representative_routing"] ||
		cfg.stages["approximate_representative_routing"] ||
		cfg.stages["exact_partition_local"] ||
		cfg.stages["end_to_end_distributed_simulation"] ||
		cfg.stages[m6CoordinatorAttributionStageV1]
	if needsSelectedTopK {
		simulationWork, err = memoryAdd(simulationWork, int64(cfg.partitions), int64(maxProbes)*8)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
	}
	needsRepresentatives := cfg.stages["exact_representative_routing"] ||
		cfg.stages["approximate_representative_routing"] ||
		cfg.stages["exact_partition_local"] ||
		cfg.stages["treedb_partition_local_hnsw"] ||
		cfg.stages["end_to_end_distributed_simulation"] ||
		cfg.stages[m6CoordinatorAttributionStageV1]
	if needsRepresentatives {
		representativeCount := int64(cfg.partitions)
		if cfg.stage == "router" || cfg.stage == m6CoordinatorStageV1 {
			representativeCount, err = memoryMul(representativeCount, int64(cfg.routerConfig.RepresentativesPerPartition))
			if err != nil {
				return benchmarkMemoryPlan{}, err
			}
			if representativeCount > int64(m.Vectors) {
				representativeCount = int64(m.Vectors)
			}
		}
		representativeValues, mulErr := memoryMul(representativeCount, int64(m.Dimensions), 8)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		representativeRows, mulErr := memoryMul(representativeCount, int64(unsafe.Sizeof([]float64{})))
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		representativeScores, mulErr := memoryMul(representativeCount, int64(unsafe.Sizeof(neighbor{}))+documentIDStorageBytes+8)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		simulationWork, err = memoryAdd(simulationWork, representativeValues, representativeRows, representativeScores, int64(maxProbes)*8)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
	}
	if cfg.stages[m6CoordinatorAttributionStageV1] {
		mergeCandidates, mulErr := memoryMul(k, int64(maxProbes))
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		coordinatorWork, mulErr := memoryMul(
			mergeCandidates,
			int64(unsafe.Sizeof(nativewire.VectorPartitionCoordinatorNeighborV1{}))+documentIDStorageBytes+memoryMapEntryBytes,
		)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		simulationWork, err = memoryAdd(
			simulationWork,
			coordinatorWork,
			int64(maxProbes)*256,
			int64(m.Dimensions)*4,
		)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
	}

	plan := benchmarkMemoryPlan{
		FixtureResidentBytes: fixtureResident,
		SimulationWorkBytes:  simulationWork,
	}
	if cfg.stage == "router" || cfg.stage == m6CoordinatorStageV1 {
		jsonRowBytes, mulErr := memoryMul(int64(m.Dimensions), hnswJSONFloatBytes+1)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		jsonRowBytes, err = memoryAdd(jsonRowBytes, hnswJSONFixedBytes)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		perInsertRow, addErr := memoryAdd(
			2*int64(unsafe.Sizeof([]byte{})),
			documentIDStorageBytes,
			2*jsonRowBytes,
			int64(m.Dimensions)*hnswDecodedDimensionBytes,
			int64(unsafe.Sizeof([]float32{})),
			memoryMapEntryBytes,
		)
		if addErr != nil {
			return benchmarkMemoryPlan{}, addErr
		}
		routerInsertRows := min(int64(m.Vectors), int64(vectorPartitionInsertBatchRows))
		routerInsertWork, mulErr := memoryMul(routerInsertRows, perInsertRow)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		perSourceRow, addErr := memoryAdd(
			int64(m.Dimensions)*4,
			documentIDStorageBytes,
			int64(unsafe.Sizeof(collections.VectorPartitionRouterSourceRowV1{})),
			int64(unsafe.Sizeof(collections.VectorPartitionMembershipV1{})),
			int64(unsafe.Sizeof(vectorpartition.RouterVectorV1{})),
			1, // seenDocuments
		)
		if addErr != nil {
			return benchmarkMemoryPlan{}, addErr
		}
		routerSourceWork, mulErr := memoryMul(int64(m.Vectors), perSourceRow)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		plan.RouterBuildWorkBytes = max(routerInsertWork, routerSourceWork)
	}
	hnswBaseBytes := int64(0)
	if cfg.stages["treedb_partition_local_hnsw"] {
		hnswBaseBytes, err = memoryAdd(
			int64(cfg.partitions)*int64(unsafe.Sizeof((*collections.Collection)(nil))),
			int64(cfg.partitions)*8,
			int64(unsafe.Sizeof(treeDBPartitionHNSW{})),
		)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		maxPartitionRows := int64((m.Vectors + cfg.partitions - 1) / cfg.partitions)
		jsonRowBytes, mulErr := memoryMul(int64(m.Dimensions), hnswJSONFloatBytes+1)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		jsonRowBytes, err = memoryAdd(jsonRowBytes, hnswJSONFixedBytes)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		perInsertRow, err := memoryAdd(
			2*int64(unsafe.Sizeof([]byte{})),
			documentIDStorageBytes,
			2*jsonRowBytes,
			int64(m.Dimensions)*hnswDecodedDimensionBytes,
			int64(unsafe.Sizeof([]float32{})),
			memoryMapEntryBytes,
		)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		insertRows := min(maxPartitionRows, int64(vectorPartitionInsertBatchRows))
		plan.HNSWInsertWorkBytes, err = memoryMul(insertRows, perInsertRow)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		plan.HNSWInsertWorkBytes, err = memoryAdd(plan.HNSWInsertWorkBytes, int64(m.Dimensions)*4)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}

		perQueryCache := int64(0)
		for probes := range uniqueProbes {
			cachedCandidates, mulErr := memoryMul(k, int64(probes), int64(unsafe.Sizeof(neighbor{}))+documentIDStorageBytes)
			if mulErr != nil {
				return benchmarkMemoryPlan{}, mulErr
			}
			entryBytes, addErr := memoryAdd(
				int64(unsafe.Sizeof(hnswCacheKey{})),
				int64(unsafe.Sizeof(hnswSearchOutcome{})),
				memoryMapEntryBytes,
				int64(probes)*6,
				cachedCandidates,
			)
			if addErr != nil {
				return benchmarkMemoryPlan{}, addErr
			}
			perQueryCache, err = memoryAdd(perQueryCache, entryBytes)
			if err != nil {
				return benchmarkMemoryPlan{}, err
			}
		}
		plan.HNSWCacheBytes, err = memoryMul(int64(m.Queries), perQueryCache)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
		searchMergeBytes, mulErr := memoryMul(k, int64(maxProbes), int64(unsafe.Sizeof(neighbor{}))+documentIDStorageBytes+memoryMapEntryBytes)
		if mulErr != nil {
			return benchmarkMemoryPlan{}, mulErr
		}
		plan.SimulationWorkBytes, err = memoryAdd(plan.SimulationWorkBytes, int64(m.Dimensions)*4, searchMergeBytes, int64(maxProbes)*8)
		if err != nil {
			return benchmarkMemoryPlan{}, err
		}
	}

	checksumPhase, err := memoryAdd(plan.FixtureResidentBytes, scoredBytes, resultBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	hnswBuildPhase, err := memoryAdd(plan.FixtureResidentBytes, hnswBaseBytes, plan.HNSWInsertWorkBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	routerBuildPhase, err := memoryAdd(plan.FixtureResidentBytes, plan.RouterBuildWorkBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	simulationPhase, err := memoryAdd(plan.FixtureResidentBytes, hnswBaseBytes, plan.HNSWCacheBytes, plan.SimulationWorkBytes)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	peak := max(checksumPhase, max(routerBuildPhase, max(hnswBuildPhase, simulationPhase)))
	plan.ModeledPeakBytes, err = memoryScaleCeil(peak, memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		return benchmarkMemoryPlan{}, err
	}
	return plan, nil
}

func memoryAdd(values ...int64) (int64, error) {
	var total int64
	for _, value := range values {
		if value < 0 || total > math.MaxInt64-value {
			return 0, errors.New("benchmark memory accounting overflow")
		}
		total += value
	}
	return total, nil
}

func memoryMul(values ...int64) (int64, error) {
	product := int64(1)
	for _, value := range values {
		if value < 0 || value != 0 && product > math.MaxInt64/value {
			return 0, errors.New("benchmark memory accounting overflow")
		}
		product *= value
	}
	return product, nil
}

func memoryScaleCeil(value, numerator, denominator int64) (int64, error) {
	scaled, err := memoryMul(value, numerator)
	if err != nil {
		return 0, err
	}
	scaled, err = memoryAdd(scaled, denominator-1)
	if err != nil {
		return 0, err
	}
	return scaled / denominator, nil
}

// deterministicFixture has intentionally visible cluster/boundary pairs and a
// duplicate pair, so tie ordering remains part of the executable M0 contract.
func deterministicFixture(m fixtureManifest) ([][]float64, [][]float64) {
	v := deterministicVectors(m)
	return v, deterministicQueries(v, m)
}

// fixtureData dispatches a fixture's durable generator identity. The
// qualification generators deliberately derive corpus and held-out queries
// from separate domains; unlike the legacy fixture they never copy a corpus
// row into a query.
func fixtureData(m fixtureManifest) ([][]float64, [][]float64) {
	if m.Generator == fixtureGenerator {
		return deterministicFixture(m)
	}
	return fixtureVectors(m), qualificationQueriesV1(m)
}

func fixtureVectors(m fixtureManifest) [][]float64 {
	if m.Generator == fixtureGenerator {
		return deterministicVectors(m)
	}
	return qualificationVectorsV1(m, 0x9e3779b97f4a7c15)
}

func qualificationVectorsV1(m fixtureManifest, domain uint64) [][]float64 {
	v := contiguousFloat64Matrix(m.Vectors, m.Dimensions)
	for i := range v {
		qualificationVectorV1(v[i], m, uint64(i), domain)
	}
	return v
}

func qualificationQueriesV1(m fixtureManifest) [][]float64 {
	q := contiguousFloat64Matrix(m.Queries, m.Dimensions)
	for i := range q {
		qualificationVectorV1(q[i], m, uint64(i), 0xd1b54a32d192ed03)
	}
	return q
}

func qualificationVectorV1(dst []float64, m fixtureManifest, ordinal, domain uint64) {
	state := uint64(m.Seed) ^ domain ^ (ordinal * 0x94d049bb133111eb)
	cluster := int((state >> 32) % 32)
	for d := range dst {
		state += 0x9e3779b97f4a7c15
		u := float64(splitMix64V1(state)>>11) * (1.0 / (1 << 53))
		state += 0x9e3779b97f4a7c15
		v := float64(splitMix64V1(state)>>11)*(1.0/(1<<53))*2 - 1
		if m.Generator == qualificationEmbeddingGeneratorV1 {
			// Directional topics plus continuous noise model an embedding-shaped
			// distribution; the manifest names it accurately rather than claiming
			// it is a licensed external embedding corpus.
			topic := -0.35
			if d%32 == cluster {
				topic = 1
			}
			dst[d] = topic + 0.18*v + 0.04*(u-0.5)
		} else {
			dst[d] = v + 0.02*(u-0.5)
		}
	}
	normalize(dst)
}

func splitMix64V1(x uint64) uint64 {
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// deterministicVectors is the partition-stage corpus generator. It must not
// inspect m.Queries: partition artifacts bind the vector corpus only, while
// simulation mode separately generates and verifies queries and exact truth.
func deterministicVectors(m fixtureManifest) [][]float64 {
	v := contiguousFloat64Matrix(m.Vectors, m.Dimensions)
	for i := range v {
		row := v[i]
		cluster := (i / 97) % 4
		row[cluster%m.Dimensions] = 1
		for d := 4; d < m.Dimensions; d++ {
			row[d] = float64(((i+1)*(d+3)+int(m.Seed))%31) / 310
		}
		normalize(row)
	}
	if len(v) > 1 {
		copy(v[1], v[0])
	}
	return v
}

func deterministicQueries(v [][]float64, m fixtureManifest) [][]float64 {
	q := contiguousFloat64Matrix(m.Queries, m.Dimensions)
	for i := range q {
		copy(q[i], v[(i*7919+17)%len(v)])
	}
	return q
}

func contiguousFloat64Matrix(rows, dimensions int) [][]float64 {
	matrix := make([][]float64, rows)
	values := make([]float64, rows*dimensions)
	for row := range matrix {
		matrix[row] = values[row*dimensions : (row+1)*dimensions]
	}
	return matrix
}
func normalize(v []float64) []float64 {
	var n float64
	for _, x := range v {
		n = math.FMA(x, x, n)
	}
	n = math.Sqrt(n)
	for i := range v {
		v[i] /= n
	}
	return v
}
func exactTopK(v [][]float64, q []float64, k int) []neighbor {
	return boundedVectorTopK(v, q, k, func(int) bool { return true })
}

func selectedPartitionTopK(v [][]float64, q []float64, k, partitions int, selected []int) []neighbor {
	wanted := make([]bool, partitions)
	for _, partition := range selected {
		wanted[partition] = true
	}
	return boundedVectorTopK(v, q, k, func(index int) bool {
		return wanted[index%partitions]
	})
}

func boundedVectorTopK(v [][]float64, q []float64, k int, include func(int) bool) []neighbor {
	candidates := make(scoredNeighborMaxHeap, 0, min(k, len(v)))
	for i, row := range v {
		if !include(i) {
			continue
		}
		var dot float64
		for d := range row {
			dot = math.FMA(row[d], q[d], dot)
		}
		candidate := scoredNeighbor{Index: i, Distance: 1 - dot}
		if len(candidates) < k {
			heap.Push(&candidates, candidate)
		} else if scoredNeighborLess(candidate, candidates[0]) {
			candidates[0] = candidate
			heap.Fix(&candidates, 0)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		return scoredNeighborLess(candidates[i], candidates[j])
	})
	out := make([]neighbor, len(candidates))
	for i, candidate := range candidates {
		out[i] = neighbor{ID: fmt.Sprintf("doc-%06d", candidate.Index), Distance: candidate.Distance, Ordinal: candidate.Index}
	}
	return out
}

func scoredNeighborLess(a, b scoredNeighbor) bool {
	if a.Distance == b.Distance {
		return a.Index < b.Index
	}
	return a.Distance < b.Distance
}

func partitionTopK(v [][]float64, q []float64, k, partitions, probes int) []neighbor {
	selected := make([]int, probes)
	for i := range selected {
		selected[i] = i
	}
	return selectedPartitionTopK(v, q, k, partitions, selected)
}
func orderedEqual(a, b []neighbor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func representativePartitions(v [][]float64, q []float64, partitions, probes int, approx bool) []int {
	sums := contiguousFloat64Matrix(partitions, len(q))
	counts := make([]int, partitions)
	for i, row := range v {
		p := i % partitions
		for d := range row {
			sums[p][d] += row[d]
		}
		counts[p]++
	}
	scores := make([]neighbor, partitions)
	for p, sum := range sums {
		var dot float64
		for d := range sum {
			dot = math.FMA(q[d], sum[d]/float64(counts[p]), dot)
		}
		scores[p] = neighbor{ID: fmt.Sprintf("p-%06d", p), Distance: 1 - dot}
	}
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].Distance == scores[j].Distance {
			return scores[i].ID < scores[j].ID
		}
		return scores[i].Distance < scores[j].Distance
	})
	out := make([]int, probes)
	for i := range out {
		idx := i
		if approx && probes < partitions && i == probes-1 {
			idx = probes
		}
		out[i] = int(parsePartitionID(scores[idx].ID))
	}
	return out
}
func parsePartitionID(id string) int { n, _ := strconv.Atoi(strings.TrimPrefix(id, "p-")); return n }

func validateSourceHNSWDegree(degree int) error {
	if degree < 1 || degree > maxSourceHNSWDegree {
		return fmt.Errorf("-source-hnsw-degree must be in [1,%d]", maxSourceHNSWDegree)
	}
	return nil
}

func newTreeDBRepresentativeRouter(vectors [][]float64, partitions int, routerConfig vectorpartition.RouterConfigV1, candidateBudget, sourceHNSWDegree int) (_ *treeDBRepresentativeRouter, err error) {
	if len(vectors) == 0 || partitions < 1 || partitions > len(vectors) || candidateBudget < 1 {
		return nil, errors.New("invalid vector/partition/candidate count for TreeDB router stage")
	}
	if err := validateSourceHNSWDegree(sourceHNSWDegree); err != nil {
		return nil, err
	}
	dimensions := len(vectors[0])
	if dimensions < 1 {
		return nil, errors.New("TreeDB router stage requires positive dimensions")
	}
	dir, err := os.MkdirTemp("", "treedb-vector-partition-router-*")
	if err != nil {
		return nil, err
	}
	h := &treeDBRepresentativeRouter{dir: dir, sourceHNSWDegree: sourceHNSWDegree}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	if err = backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
		RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
	}); err != nil {
		return nil, err
	}
	h.db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, err
	}
	meta := partitionCollectionMetaWithDegree("router_source", dimensions, sourceHNSWDegree)
	meta.Options.ColumnStore.AssetManager = &collections.ColumnAssetManagerConfig{
		Kind: collections.ColumnAssetManagerValueLogShaped, IsolatedNamespace: true,
		Namespace: "router_bench_assets",
	}
	manager := collections.NewCollectionManager(h.db)
	if _, err = manager.CreateCollection(meta); err != nil {
		return nil, err
	}
	h.collection, err = manager.OpenCollection(meta.Name)
	if err != nil {
		return nil, err
	}
	if err = insertPartitionRows(h.collection, vectors, 0, 1); err != nil {
		return nil, err
	}
	if err = h.collection.Flush(); err != nil {
		return nil, err
	}
	if _, err = h.collection.RebuildVectorIndex(partitionHNSWIndex); err != nil {
		return nil, err
	}
	source, err := h.collection.VectorPartitionSourceIdentityV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	snapshotIdentity, sourceRows, err := h.collection.ReadVectorPartitionRouterSourceRowsV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	if snapshotIdentity != source {
		return nil, errors.New("router benchmark source changed while capturing authoritative rows")
	}
	lease, err := h.db.AcquireStableResourceCaptureLease()
	if err != nil {
		return nil, err
	}
	items := make([]collections.StableColumnPhysicalAssetAppend, partitions)
	for partition := range items {
		payload := sha256.Sum256([]byte(fmt.Sprintf("router-partition/%d/source/%s", partition, meta.Name)))
		items[partition] = collections.StableColumnPhysicalAssetAppend{
			Payload: payload[:], Kind: collections.ColumnAssetKindTCS1PartImage,
			Generation: source.Generation, PartID: uint64(partition) + 1,
		}
	}
	refs, resources, appendErr := collections.AppendColumnPhysicalAssetsWithStableResources(
		h.db.ColumnAssetRootDir(), *meta.Options.ColumnStore, 3908, items,
		h.db.StableResourceIdentityPinRegistry(), lease,
	)
	lease.Release()
	if appendErr != nil {
		return nil, appendErr
	}
	if resources == nil || len(refs) != partitions {
		if resources != nil {
			resources.Release()
		}
		return nil, errors.New("router benchmark partition asset append returned incomplete authority")
	}
	defer resources.Release()
	generation := source.Generation + 1
	if generation == 0 {
		return nil, errors.New("router benchmark generation overflow")
	}
	building := collections.VectorPartitionManifestV1{
		State: "building", Collection: meta.Name, IndexName: partitionHNSWIndex,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum,
		SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: generation, PartitionCount: uint32(partitions), BalancePolicy: "round_robin_disjoint_v1",
	}
	routerPartitions := make([]vectorpartition.RouterPartitionV1, partitions)
	for partition := 0; partition < partitions; partition++ {
		building.Placements = append(building.Placements, collections.VectorPartitionPlacementV1{
			PartitionID: uint32(partition), GroupID: fmt.Sprintf("benchmark-group-%06d", partition),
		})
		raw := items[partition].Payload
		sum := sha256.Sum256(raw)
		building.Assets = append(building.Assets, collections.VectorPartitionAssetV1{
			ID: fmt.Sprintf("partition/%06d", partition), PartitionID: uint32(partition),
			Checksum: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw)), Ref: refs[partition],
		})
	}
	seenDocuments := make([]bool, len(vectors))
	for _, sourceRow := range sourceRows {
		documentID := string(sourceRow.DocumentID)
		if !strings.HasPrefix(documentID, "doc-") {
			return nil, fmt.Errorf("router benchmark source ordinal %d has malformed document ID %q", sourceRow.VectorOrdinal, documentID)
		}
		documentOrdinal, parseErr := strconv.Atoi(strings.TrimPrefix(documentID, "doc-"))
		if parseErr != nil || documentOrdinal < 0 || documentOrdinal >= len(vectors) || seenDocuments[documentOrdinal] {
			return nil, fmt.Errorf("router benchmark source ordinal %d has invalid document ID %q", sourceRow.VectorOrdinal, documentID)
		}
		seenDocuments[documentOrdinal] = true
		if len(sourceRow.Values) != len(vectors[documentOrdinal]) {
			return nil, fmt.Errorf("router benchmark source ordinal %d dimensions=%d want %d", sourceRow.VectorOrdinal, len(sourceRow.Values), len(vectors[documentOrdinal]))
		}
		for dimension, value := range vectors[documentOrdinal] {
			if math.IsNaN(value) || math.IsInf(value, 0) ||
				math.Float32bits(sourceRow.Values[dimension]) != math.Float32bits(float32(value)) {
				return nil, fmt.Errorf("router benchmark source ordinal %d dimension %d differs from fixture document %d", sourceRow.VectorOrdinal, dimension, documentOrdinal)
			}
		}
		partition := documentOrdinal % partitions
		building.Memberships = append(building.Memberships, collections.VectorPartitionMembershipV1{
			VectorOrdinal: sourceRow.VectorOrdinal, PartitionID: uint32(partition),
		})
		routerPartitions[partition].PartitionID = uint32(partition)
		routerPartitions[partition].Vectors = append(routerPartitions[partition].Vectors, vectorpartition.RouterVectorV1{
			Ordinal:        sourceRow.VectorOrdinal,
			Values:         sourceRow.Values,
			MembershipKind: string(collections.VectorPartitionMembershipHomeV1),
		})
	}
	for documentOrdinal, seen := range seenDocuments {
		if !seen {
			return nil, fmt.Errorf("router benchmark source omitted fixture document %d", documentOrdinal)
		}
	}
	building.Canonicalize()
	if err = h.collection.PublishVectorPartitionManifestV1(building, nil); err != nil {
		return nil, err
	}
	h.build, err = h.collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), building, routerPartitions, collections.VectorPartitionRouterBuildOptionsV1{
		Config: routerConfig, AssetFileID: 3909, AssetPartID: uint64(partitions) + 1,
		M: partitionHNSWDegree, EfConstruction: 128, EfSearch: 128,
	})
	if err != nil {
		return nil, err
	}
	h.router, h.open, err = h.collection.OpenVectorPartitionRouterV1(partitionHNSWIndex)
	if err != nil {
		return nil, err
	}
	h.candidates = min(candidateBudget, int(h.open.Representatives))
	if h.candidates < 1 {
		return nil, errors.New("router benchmark produced no representatives")
	}
	return h, nil
}

func (h *treeDBRepresentativeRouter) search(query []float64, probes int, approximate bool) ([]int, collections.VectorPartitionRouterSearchStatusV1, error) {
	if h == nil || h.router == nil {
		return nil, collections.VectorPartitionRouterSearchStatusV1{}, errors.New("TreeDB router stage is not initialized")
	}
	query32 := make([]float32, len(query))
	for i, value := range query {
		query32[i] = float32(value)
	}
	mode := collections.VectorPartitionRouterModeExactV1
	candidates := int(h.open.Representatives)
	if approximate {
		mode = collections.VectorPartitionRouterModeApproxV1
		candidates = h.candidates
	}
	result, err := h.router.Search(query32, collections.VectorPartitionRouterSearchOptionsV1{
		Mode: mode, CandidateBudget: candidates, PartitionProbes: probes,
	})
	if err != nil {
		return nil, result.Status, err
	}
	selected := make([]int, len(result.Partitions))
	for i, partition := range result.Partitions {
		selected[i] = int(partition.PartitionID)
	}
	return selected, result.Status, nil
}

func (h *treeDBRepresentativeRouter) Close() error {
	if h == nil {
		return nil
	}
	var errs []error
	if h.router != nil {
		errs = append(errs, h.router.Close())
		h.router = nil
	}
	if h.db != nil {
		errs = append(errs, h.db.Close())
		h.db = nil
	}
	if h.dir != "" {
		errs = append(errs, os.RemoveAll(h.dir))
		h.dir = ""
	}
	return errors.Join(errs...)
}

func newTreeDBPartitionHNSW(vectors [][]float64, partitions int) (_ *treeDBPartitionHNSW, err error) {
	if len(vectors) == 0 || partitions < 1 || partitions > len(vectors) {
		return nil, errors.New("invalid vector/partition count for TreeDB HNSW stage")
	}
	dims := len(vectors[0])
	if dims < 1 {
		return nil, errors.New("TreeDB HNSW stage requires positive dimensions")
	}
	dir, err := os.MkdirTemp("", "treedb-vector-partition-hnsw-*")
	if err != nil {
		return nil, err
	}
	h := &treeDBPartitionHNSW{
		dir:        dir,
		partitions: make([]*collections.Collection, partitions),
		rowCounts:  make([]int, partitions),
		cache:      make(map[hnswCacheKey]hnswSearchOutcome),
	}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	if err = backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
		RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
	}); err != nil {
		return nil, err
	}
	h.db, err = backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return nil, err
	}
	manager := collections.NewCollectionManager(h.db)
	for partition := 0; partition < partitions; partition++ {
		name := partitionCollectionName(partition)
		if _, err = manager.CreateCollection(partitionCollectionMeta(name, dims)); err != nil {
			return nil, fmt.Errorf("create partition %d collection: %w", partition, err)
		}
		col, openErr := manager.OpenCollection(name)
		if openErr != nil {
			return nil, fmt.Errorf("open partition %d collection: %w", partition, openErr)
		}
		if err = insertPartitionRows(col, vectors, partition, partitions); err != nil {
			return nil, fmt.Errorf("insert partition %d rows: %w", partition, err)
		}
		if err = col.Flush(); err != nil {
			return nil, fmt.Errorf("flush partition %d rows: %w", partition, err)
		}
		status, rebuildErr := col.RebuildVectorIndex(partitionHNSWIndex)
		if rebuildErr != nil {
			return nil, fmt.Errorf("rebuild partition %d HNSW: %w", partition, rebuildErr)
		}
		if !status.Loaded {
			return nil, fmt.Errorf("rebuild partition %d HNSW status=%+v, want loaded", partition, status)
		}
		h.rowCounts[partition] = moduloPartitionSize(len(vectors), partitions, partition)
	}
	if err = h.db.Checkpoint(); err != nil {
		return nil, fmt.Errorf("checkpoint partition-local HNSW DB: %w", err)
	}
	for partition := 0; partition < partitions; partition++ {
		col, openErr := manager.OpenCollection(partitionCollectionName(partition))
		if openErr != nil {
			return nil, fmt.Errorf("reopen partition %d collection: %w", partition, openErr)
		}
		h.partitions[partition] = col
	}
	return h, nil
}

func partitionCollectionName(partition int) string {
	return fmt.Sprintf("partition_%06d", partition)
}

func partitionCollectionMeta(name string, dims int) *collections.CollectionMeta {
	return partitionCollectionMetaWithDegree(name, dims, partitionHNSWDegree)
}

func partitionCollectionMetaWithDegree(name string, dims, degree int) *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Columns: []collections.ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
					{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: dims},
				},
				SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name:           partitionHNSWIndex,
			Field:          "embedding",
			Metric:         collections.VectorMetricCosine,
			Dimensions:     dims,
			M:              degree,
			EfConstruction: 128,
			EfSearch:       128,
			Encoding:       collections.VectorIndexEncodingFloat32,
			Strategy:       collections.VectorIndexStrategyColumnGraph,
		}},
	}
}

func insertPartitionRows(col *collections.Collection, vectors [][]float64, partition, partitions int) error {
	rowCount := moduloPartitionSize(len(vectors), partitions, partition)
	inserted := 0
	for next := partition; next < len(vectors); {
		batchRows := min(vectorPartitionInsertBatchRows, rowCount-inserted)
		ids := make([][]byte, 0, batchRows)
		documents := make([][]byte, 0, batchRows)
		for len(ids) < batchRows && next < len(vectors) {
			vector := make([]float32, len(vectors[next]))
			for dimension, value := range vectors[next] {
				if math.IsNaN(value) || math.IsInf(value, 0) {
					return fmt.Errorf("vector %d dimension %d is non-finite", next, dimension)
				}
				vector[dimension] = float32(value)
			}
			id := fmt.Sprintf("doc-%06d", next)
			raw, err := json.Marshal(struct {
				TimeUS    int64     `json:"time_us"`
				Embedding []float32 `json:"embedding"`
			}{
				TimeUS:    int64(next + 1),
				Embedding: vector,
			})
			if err != nil {
				return err
			}
			ids = append(ids, []byte(id))
			documents = append(documents, raw)
			next += partitions
		}
		if len(ids) == 0 {
			return fmt.Errorf("partition %d produced an empty insert batch", partition)
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			return err
		}
		inserted += len(ids)
	}
	if inserted != rowCount {
		return fmt.Errorf("partition %d rows=%d want %d", partition, inserted, rowCount)
	}
	return nil
}

func moduloPartitionSize(vectors, partitions, partition int) int {
	if partition >= vectors {
		return 0
	}
	return 1 + (vectors-1-partition)/partitions
}

func (h *treeDBPartitionHNSW) search(queryIndex int, query []float64, selected []int, topK int) (hnswSearchOutcome, error) {
	if h == nil || h.db == nil {
		return hnswSearchOutcome{}, errors.New("TreeDB partition-local HNSW stage is not initialized")
	}
	key := hnswCacheKey{Query: queryIndex, SelectedPartitions: canonicalPartitionSet(selected), TopK: topK}
	if cached, ok := h.cache[key]; ok {
		cached.Evidence.ExecutedSearches = 0
		cached.Evidence.CachedSearches = cached.Evidence.Searches
		return cached, nil
	}
	query32 := make([]float32, len(query))
	for i, value := range query {
		query32[i] = float32(value)
	}
	merged := make([]neighbor, 0, topK*len(selected))
	var evidence hnswSearchEvidence
	for _, partition := range selected {
		if partition < 0 || partition >= len(h.partitions) || h.partitions[partition] == nil || h.rowCounts[partition] == 0 {
			return hnswSearchOutcome{}, fmt.Errorf("selected HNSW partition %d is unavailable", partition)
		}
		response, err := h.partitions[partition].SearchVectorIndex(collections.VectorIndexSearchOptions{
			IndexName: partitionHNSWIndex,
			Query:     query32,
			TopK:      min(topK, h.rowCounts[partition]),
			EfSearch:  h.rowCounts[partition],
		})
		if err != nil {
			return hnswSearchOutcome{}, fmt.Errorf("search HNSW partition %d: %w", partition, err)
		}
		h.physicalSearches++
		diagnostics := response.Diagnostics()
		if response.Stats.SearchRouteHNSWSearchPack != 1 ||
			response.Stats.HNSWSearchPackActive != 1 ||
			response.Stats.HNSWSearchPackFallbacks != 0 ||
			diagnostics.Route != collections.VectorIndexSearchRouteExactHNSWSearchPackV1 ||
			!diagnostics.ExactHNSWSearchPackNoDocRoute {
			return hnswSearchOutcome{}, fmt.Errorf("partition %d did not use exact HNSW search-pack route: diagnostics=%+v stats_route=%d active=%d fallbacks=%d",
				partition,
				diagnostics,
				response.Stats.SearchRouteHNSWSearchPack,
				response.Stats.HNSWSearchPackActive,
				response.Stats.HNSWSearchPackFallbacks,
			)
		}
		evidence.Searches++
		evidence.ExecutedSearches++
		evidence.SearchRouteHNSWSearchPack += response.Stats.SearchRouteHNSWSearchPack
		evidence.HNSWSearchPackActive += response.Stats.HNSWSearchPackActive
		evidence.HNSWSearchPackFallbacks += response.Stats.HNSWSearchPackFallbacks
		for _, result := range response.Results {
			distance := 1 - result.Score
			if math.IsNaN(distance) || math.IsInf(distance, 0) {
				return hnswSearchOutcome{}, fmt.Errorf("partition %d returned non-finite score for %q", partition, result.ID)
			}
			merged = append(merged, neighbor{ID: string(result.ID), Distance: distance})
		}
	}
	sortNeighbors(merged)
	merged = dedupeSortedNeighbors(merged)
	if len(merged) > topK {
		merged = merged[:topK]
	}
	outcome := hnswSearchOutcome{Neighbors: merged, Evidence: evidence}
	h.cache[key] = outcome
	return outcome, nil
}

func canonicalPartitionSet(selected []int) string {
	partitions := append([]int(nil), selected...)
	sort.Ints(partitions)
	var b strings.Builder
	for i, partition := range partitions {
		if i != 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(partition))
	}
	return b.String()
}

func (h *treeDBPartitionHNSW) Close() error {
	if h == nil {
		return nil
	}
	var errs []error
	clear(h.partitions)
	h.partitions = nil
	if h.db != nil {
		if err := h.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close partition-local HNSW DB: %w", err))
		}
		h.db = nil
	}
	if h.dir != "" {
		if err := os.RemoveAll(h.dir); err != nil {
			errs = append(errs, fmt.Errorf("remove partition-local HNSW DB: %w", err))
		}
		h.dir = ""
	}
	return errors.Join(errs...)
}

func sortNeighbors(in []neighbor) {
	sort.Slice(in, func(i, j int) bool {
		if in[i].Distance == in[j].Distance {
			return in[i].ID < in[j].ID
		}
		return in[i].Distance < in[j].Distance
	})
}

func dedupeSortedNeighbors(in []neighbor) []neighbor {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, n := range in {
		if _, ok := seen[n.ID]; ok {
			continue
		}
		seen[n.ID] = struct{}{}
		out = append(out, n)
	}
	return out
}

// canonicalExactNeighborsV1 keeps the legacy simulation distance representation
// deterministic. Production M8 evidence uses m8CanonicalResultsV1 and the
// executable collections.VectorPartitionCanonicalScoreContractV1 instead.
func canonicalExactNeighborsV1(in []neighbor, topK int) []neighbor {
	if topK <= 0 {
		return nil
	}
	ordered := append([]neighbor(nil), in...)
	for i := range ordered {
		if ordered[i].ID == "" || math.IsNaN(ordered[i].Distance) || math.IsInf(ordered[i].Distance, 0) {
			return nil
		}
		ordered[i].Distance = float64(float32(ordered[i].Distance))
	}
	sortNeighbors(ordered)
	ordered = dedupeSortedNeighbors(ordered)
	if topK < len(ordered) {
		ordered = ordered[:topK]
	}
	return ordered
}

func recall(want, got []neighbor) float64 {
	found := map[string]bool{}
	for _, x := range got {
		found[x.ID] = true
	}
	n := 0
	for _, x := range want {
		if found[x.ID] {
			n++
		}
	}
	return float64(n) / float64(len(want))
}

func routerPercentileNanos(durations []uint64, percentile int) uint64 {
	if len(durations) == 0 || percentile < 1 || percentile > 100 {
		return 0
	}
	ordered := append([]uint64(nil), durations...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	rank := (percentile*len(ordered) + 99) / 100
	return ordered[rank-1]
}

func routeRepresentativePartitions(cfg config, vectors [][]float64, query []float64, probes int, approximate bool) ([]int, collections.VectorPartitionRouterSearchStatusV1, error) {
	if cfg.router != nil {
		return cfg.router.search(query, probes, approximate)
	}
	return representativePartitions(vectors, query, cfg.partitions, probes, approximate), collections.VectorPartitionRouterSearchStatusV1{}, nil
}

func routeRepresentativePartitionsMeasured(cfg config, vectors [][]float64, query []float64, probes int, approximate bool) ([]int, collections.VectorPartitionRouterSearchStatusV1, uint64, uint64, error) {
	if cfg.router == nil {
		partitions, status, err := routeRepresentativePartitions(cfg, vectors, query, probes, approximate)
		return partitions, status, 0, 0, err
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	partitions, status, err := routeRepresentativePartitions(cfg, vectors, query, probes, approximate)
	runtime.ReadMemStats(&after)
	return partitions, status, after.TotalAlloc - before.TotalAlloc, after.Mallocs - before.Mallocs, err
}

func simulate(cfg config, m fixtureManifest, v, q [][]float64, probes int, overlap float64) (runResult, error) {
	if cfg.stages == nil {
		cfg.stages = stageSet("all")
	}
	if cfg.stage == m6CoordinatorStageV1 {
		return simulateM6CoordinatorV1(cfg, m, v, q, probes, overlap)
	}
	if overlap != 0 { /* M0 reports budget only; M3 owns membership copies. */
	}
	totals := map[string]float64{}
	var hnswEvidence hnswSearchEvidence
	var exactRouterEvidence, approximateRouterEvidence routerSearchEvidence
	exactParity := true
	for queryIndex, query := range q {
		truth := exactTopK(v, query, cfg.topK)
		if cfg.stages["exact_global_top_k"] {
			totals["exact_global_top_k"] += 1
		}
		if cfg.stages["partition_oracle"] {
			got := partitionTopK(v, query, cfg.topK, cfg.partitions, probes)
			totals["partition_oracle"] += recall(truth, got)
			if probes == cfg.partitions && !orderedEqual(truth, got) {
				exactParity = false
			}
		}
		if cfg.stages["exact_representative_routing"] {
			parts, routeStatus, allocBytes, allocs, err := routeRepresentativePartitionsMeasured(cfg, v, query, probes, false)
			if err != nil {
				return runResult{}, err
			}
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["exact_representative_routing"] += recall(truth, got)
			exactRouterEvidence.Searches++
			exactRouterEvidence.Candidates += routeStatus.Candidates
			exactRouterEvidence.Edges += routeStatus.Edges
			exactRouterEvidence.SearchNanos += routeStatus.SearchNanos
			exactRouterEvidence.AllocBytes += allocBytes
			exactRouterEvidence.Allocs += allocs
			exactRouterEvidence.Durations = append(exactRouterEvidence.Durations, routeStatus.SearchNanos)
		}
		if cfg.stages["approximate_representative_routing"] {
			parts, routeStatus, allocBytes, allocs, err := routeRepresentativePartitionsMeasured(cfg, v, query, probes, true)
			if err != nil {
				return runResult{}, err
			}
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["approximate_representative_routing"] += recall(truth, got)
			approximateRouterEvidence.Searches++
			approximateRouterEvidence.Candidates += routeStatus.Candidates
			approximateRouterEvidence.Edges += routeStatus.Edges
			approximateRouterEvidence.SearchNanos += routeStatus.SearchNanos
			approximateRouterEvidence.AllocBytes += allocBytes
			approximateRouterEvidence.Allocs += allocs
			approximateRouterEvidence.Durations = append(approximateRouterEvidence.Durations, routeStatus.SearchNanos)
		}
		if cfg.stages["exact_partition_local"] {
			parts, _, err := routeRepresentativePartitions(cfg, v, query, probes, false)
			if err != nil {
				return runResult{}, err
			}
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["exact_partition_local"] += recall(truth, got)
		}
		if cfg.stages["treedb_partition_local_hnsw"] {
			if cfg.hnsw == nil {
				return runResult{}, errors.New("TreeDB partition-local HNSW stage selected without initialized TreeDB indexes")
			}
			parts, _, err := routeRepresentativePartitions(cfg, v, query, probes, false)
			if err != nil {
				return runResult{}, err
			}
			outcome, err := cfg.hnsw.search(queryIndex, query, parts, cfg.topK)
			if err != nil {
				return runResult{}, err
			}
			totals["treedb_partition_local_hnsw"] += recall(truth, outcome.Neighbors)
			hnswEvidence.Searches += outcome.Evidence.Searches
			hnswEvidence.ExecutedSearches += outcome.Evidence.ExecutedSearches
			hnswEvidence.CachedSearches += outcome.Evidence.CachedSearches
			hnswEvidence.SearchRouteHNSWSearchPack += outcome.Evidence.SearchRouteHNSWSearchPack
			hnswEvidence.HNSWSearchPackActive += outcome.Evidence.HNSWSearchPackActive
			hnswEvidence.HNSWSearchPackFallbacks += outcome.Evidence.HNSWSearchPackFallbacks
		}
		if cfg.stages["end_to_end_distributed_simulation"] {
			parts, _, err := routeRepresentativePartitions(cfg, v, query, probes, true)
			if err != nil {
				return runResult{}, err
			}
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["end_to_end_distributed_simulation"] += recall(truth, dedupeStable(got))
		}
	}
	n := float64(len(q))
	stage := func(name, method string, lossy bool, value float64, p int) stageResult {
		return stageResult{Name: name, Method: method, Enabled: cfg.stages[name], Lossy: lossy, RecallAtK: value, Queries: len(q), Probes: p, Available: true}
	}
	hnswStage := stage("treedb_partition_local_hnsw", "treedb_column_graph_hnsw_recall_qualified_v1", true, totals["treedb_partition_local_hnsw"]/n, probes)
	hnswStage.RouteKind = string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1)
	hnswStage.Searches = hnswEvidence.Searches
	hnswStage.ExecutedSearches = hnswEvidence.ExecutedSearches
	hnswStage.CachedSearches = hnswEvidence.CachedSearches
	hnswStage.SearchRouteHNSWSearchPack = hnswEvidence.SearchRouteHNSWSearchPack
	hnswStage.HNSWSearchPackActive = hnswEvidence.HNSWSearchPackActive
	hnswStage.HNSWSearchPackFallbacks = hnswEvidence.HNSWSearchPackFallbacks
	exactMethod := "centroid_representative_routing"
	approximateMethod := "deterministic_last_representative_perturbation"
	if cfg.router != nil {
		exactMethod = "persisted_krt_exact_representative_oracle_v1"
		approximateMethod = "persisted_krt_native_hnsw_search_pack_v1"
	}
	exactRouterStage := stage("exact_representative_routing", exactMethod, probes < cfg.partitions, totals["exact_representative_routing"]/n, probes)
	approximateRouterStage := stage("approximate_representative_routing", approximateMethod, probes < cfg.partitions, totals["approximate_representative_routing"]/n, probes)
	if cfg.router != nil {
		exactRouterStage.Searches = exactRouterEvidence.Searches
		exactRouterStage.RepresentativeCount = cfg.router.open.Representatives
		exactRouterStage.CandidateBudget = cfg.router.open.Representatives
		exactRouterStage.Candidates = exactRouterEvidence.Candidates
		exactRouterStage.Edges = exactRouterEvidence.Edges
		exactRouterStage.SearchNanos = exactRouterEvidence.SearchNanos
		exactRouterStage.P50Nanos = routerPercentileNanos(exactRouterEvidence.Durations, 50)
		exactRouterStage.P95Nanos = routerPercentileNanos(exactRouterEvidence.Durations, 95)
		exactRouterStage.P99Nanos = routerPercentileNanos(exactRouterEvidence.Durations, 99)
		if exactRouterEvidence.Searches > 0 {
			exactRouterStage.BytesPerOp = float64(exactRouterEvidence.AllocBytes) / float64(exactRouterEvidence.Searches)
			exactRouterStage.AllocsPerOp = float64(exactRouterEvidence.Allocs) / float64(exactRouterEvidence.Searches)
		}
		approximateRouterStage.Searches = approximateRouterEvidence.Searches
		approximateRouterStage.RepresentativeCount = cfg.router.open.Representatives
		approximateRouterStage.CandidateBudget = uint64(cfg.router.candidates)
		approximateRouterStage.Candidates = approximateRouterEvidence.Candidates
		approximateRouterStage.Edges = approximateRouterEvidence.Edges
		approximateRouterStage.SearchNanos = approximateRouterEvidence.SearchNanos
		approximateRouterStage.P50Nanos = routerPercentileNanos(approximateRouterEvidence.Durations, 50)
		approximateRouterStage.P95Nanos = routerPercentileNanos(approximateRouterEvidence.Durations, 95)
		approximateRouterStage.P99Nanos = routerPercentileNanos(approximateRouterEvidence.Durations, 99)
		if approximateRouterEvidence.Searches > 0 {
			approximateRouterStage.BytesPerOp = float64(approximateRouterEvidence.AllocBytes) / float64(approximateRouterEvidence.Searches)
			approximateRouterStage.AllocsPerOp = float64(approximateRouterEvidence.Allocs) / float64(approximateRouterEvidence.Searches)
		}
	}
	allStages := []stageResult{stage("exact_global_top_k", "global_exhaustive", false, totals["exact_global_top_k"]/n, cfg.partitions), stage("partition_oracle", "round_robin_partition_exhaustive", probes < cfg.partitions, totals["partition_oracle"]/n, probes), exactRouterStage, approximateRouterStage, stage("exact_partition_local", "representative_selected_partition_exhaustive", probes < cfg.partitions, totals["exact_partition_local"]/n, probes), hnswStage, stage("end_to_end_distributed_simulation", "approximate_router_then_local_exhaustive_dedupe", probes < cfg.partitions, totals["end_to_end_distributed_simulation"]/n, probes)}
	selected := make([]stageResult, 0, len(allStages))
	for _, s := range allStages {
		if s.Enabled {
			selected = append(selected, s)
		}
	}
	metrics := metricsV1{MeasurementStatus: "simulation_not_measured", Balance: 1, MaxPartitionSize: (len(v) + cfg.partitions - 1) / cfg.partitions, ReplicationFactor: 1, UnassignedOverlapBudget: overlap, RoutedPartitionRecall: totals["end_to_end_distributed_simulation"] / n, SelectedPartitions: probes}
	if cfg.router != nil {
		metrics.MeasurementStatus = "router_local_production_path_no_raft"
		metrics.BuildWallNanos = int64(cfg.router.build.BuildNanos)
		metrics.BuildCPUNanos = cfg.router.buildCPU
		metrics.BuildCPUAvailable = cfg.router.buildCPUOK
		metrics.PeakRSSBytes = cfg.router.peakRSS
		metrics.PeakRSSAvailable = cfg.router.peakRSSOK
		metrics.TemporaryBytes = int64(cfg.router.build.RouterBytes)
		metrics.FinalBytes = int64(cfg.router.build.RouterBytes)
		metrics.BytesPerVector = float64(cfg.router.build.RouterBytes) / float64(len(v))
		metrics.RepresentativeCount = int(cfg.router.open.Representatives)
		metrics.SourceHNSWDegree = cfg.router.sourceHNSWDegree
		metrics.LloydIterations = int(cfg.router.build.LloydIterations)
		metrics.EmptyRepairs = int(cfg.router.build.EmptyRepairs)
		metrics.MinRepresentatives = int(cfg.router.build.MinRepresentativesPerPartition)
		metrics.MaxRepresentatives = int(cfg.router.build.MaxRepresentativesPerPartition)
		metrics.RouterBytes = int64(cfg.router.build.RouterBytes)
		metrics.MappedBytes = int64(cfg.router.open.MappedBytes)
		metrics.HeapCopyBytes = int64(cfg.router.open.HeapCopyBytes)
		searches := exactRouterEvidence.Searches + approximateRouterEvidence.Searches
		if searches > 0 {
			metrics.RoutingLatencyNanos = int64((exactRouterEvidence.SearchNanos + approximateRouterEvidence.SearchNanos) / searches)
		}
		if cfg.stages["exact_representative_routing"] {
			metrics.CoarseningRecall = totals["exact_representative_routing"] / n
			metrics.CoarseningMeasured = true
		}
		if cfg.stages["approximate_representative_routing"] {
			metrics.ApproximateRouterRecall = totals["approximate_representative_routing"] / n
			metrics.ApproximateMeasured = true
		}
		if metrics.CoarseningMeasured && metrics.ApproximateMeasured {
			metrics.HNSWRecallLoss = metrics.CoarseningRecall - metrics.ApproximateRouterRecall
			metrics.HNSWLossMeasured = true
		}
	}
	goMaxProcs, goMemoryLimitBytes := benchmarkRuntimeLimits()
	r := runResult{
		SchemaVersion:      schemaVersion,
		ResultKind:         "simulation_only",
		ProductionEvidence: false,
		Command:            cfg.command,
		BaseSHA:            cfg.baseSHA,
		HeadSHA:            cfg.headSHA,
		GoVersion:          runtime.Version(),
		Hardware:           runtime.GOARCH + "/" + runtime.GOOS,
		GOMAXPROCS:         goMaxProcs,
		GoMemoryLimitBytes: goMemoryLimitBytes,
		Dataset:            m,
		Partitions:         cfg.partitions,
		Overlap:            overlap,
		Probes:             probes,
		TopK:               cfg.topK,
		RecallTarget:       cfg.recallTarget,
		Seed:               cfg.seed,
		MemoryBudgetBytes:  cfg.maxBytes,
		ModeledPeakBytes:   cfg.memory.ModeledPeakBytes,
		MemoryBudgetScope:  memoryBudgetScope,
		Warmup:             0,
		Samples:            len(q),
		TimedBoundary:      "untimed M0 attribution: in-memory oracles plus temporary local TreeDB column_graph build/search; excludes network, RPC, coordinator, and Raft",
		Stages:             selected,
		Metrics:            metrics,
	}
	if cfg.router != nil {
		r.ResultKind = "router_local_path_evidence"
		r.ProductionEvidence = false
		r.TimedBoundary = "local persisted M4 router build/open/search path; exact coarsening oracle and native HNSW loss are separate; excludes RPC, coordinator, Raft, shard search, and M8 acceptance"
	}
	if cfg.stages["partition_oracle"] && probes == cfg.partitions && !exactParity {
		return r, errors.New("all-partition oracle parity failure")
	}
	return r, nil
}
func dedupeStable(in []neighbor) []neighbor {
	seen := map[string]bool{}
	out := make([]neighbor, 0, len(in))
	for _, n := range in {
		if !seen[n.ID] {
			seen[n.ID] = true
			out = append(out, n)
		}
	}
	return out
}
func validateResult(r runResult) error {
	if r.SchemaVersion != schemaVersion ||
		r.ResultKind != "simulation_only" &&
			r.ResultKind != "router_local_path_evidence" &&
			r.ResultKind != m6CoordinatorResultKindV1 ||
		r.ProductionEvidence || len(r.Stages) == 0 {
		return errors.New("invalid result schema or production labeling")
	}
	for _, s := range r.Stages {
		if s.Method == "" {
			return errors.New("unlabelled attribution stage")
		}
		if !s.Available {
			if s.UnavailableReason == "" {
				return errors.New("unavailable stage without reason")
			}
			continue
		}
		if math.IsNaN(s.RecallAtK) || math.IsInf(s.RecallAtK, 0) || s.RecallAtK < 0 || s.RecallAtK > 1 {
			return errors.New("non-finite metric")
		}
		if math.IsNaN(s.BytesPerOp) || math.IsInf(s.BytesPerOp, 0) || s.BytesPerOp < 0 ||
			math.IsNaN(s.AllocsPerOp) || math.IsInf(s.AllocsPerOp, 0) || s.AllocsPerOp < 0 {
			return errors.New("invalid stage allocation metric")
		}
		if (r.ResultKind == "router_local_path_evidence" || r.ResultKind == m6CoordinatorResultKindV1) &&
			(s.Name == "exact_representative_routing" || s.Name == "approximate_representative_routing") {
			if s.Searches != uint64(s.Queries) || s.RepresentativeCount == 0 ||
				s.CandidateBudget == 0 || s.CandidateBudget > s.RepresentativeCount ||
				s.Candidates > s.Searches*s.CandidateBudget ||
				s.P50Nanos == 0 || s.P50Nanos > s.P95Nanos || s.P95Nanos > s.P99Nanos {
				return fmt.Errorf("router stage lacks bounded timing/search evidence: %+v", s)
			}
			if s.Name == "exact_representative_routing" &&
				(s.CandidateBudget != s.RepresentativeCount || s.Candidates != s.Searches*s.RepresentativeCount) {
				return fmt.Errorf("exact router stage lacks full representative evidence: %+v", s)
			}
		}
		if s.Name == "treedb_partition_local_hnsw" {
			if s.RouteKind != string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1) ||
				s.Searches == 0 ||
				s.ExecutedSearches+s.CachedSearches != s.Searches ||
				s.SearchRouteHNSWSearchPack != s.Searches ||
				s.HNSWSearchPackActive != s.Searches ||
				s.HNSWSearchPackFallbacks != 0 {
				return fmt.Errorf("TreeDB HNSW stage lacks declared native search-pack route evidence: %+v", s)
			}
		}
	}
	if !validSHA(r.BaseSHA) || !validSHA(r.HeadSHA) {
		return errors.New("result provenance must contain exact 40-hex base/head SHAs")
	}
	if r.GOMAXPROCS < 1 || r.GoMemoryLimitBytes < 1 {
		return errors.New("result must record positive Go runtime concurrency and memory limits")
	}
	if len(r.Dataset.Checksum) != 64 {
		return errors.New("result dataset must contain an exact SHA-256 checksum")
	}
	if _, err := hex.DecodeString(r.Dataset.Checksum); err != nil {
		return errors.New("result dataset checksum must be hexadecimal")
	}
	if r.Seed != r.Dataset.Seed {
		return errors.New("result seed does not match fixture generation seed")
	}
	if r.MemoryBudgetBytes < 1 || r.ModeledPeakBytes < 1 || r.ModeledPeakBytes > r.MemoryBudgetBytes || r.MemoryBudgetScope != memoryBudgetScope {
		return errors.New("invalid benchmark-owned memory budget evidence")
	}
	if r.Metrics.MeasurementStatus != "simulation_not_measured" &&
		r.Metrics.MeasurementStatus != "router_local_production_path_no_raft" &&
		r.Metrics.MeasurementStatus != m6CoordinatorMeasurementV1 {
		return errors.New("unknown metric measurement status")
	}
	for _, x := range []float64{r.Metrics.BytesPerVector, r.Metrics.Balance, r.Metrics.ReplicationFactor, r.Metrics.UnassignedOverlapBudget, r.Metrics.RoutedPartitionRecall, r.Metrics.CoarseningRecall, r.Metrics.ApproximateRouterRecall, r.Metrics.HNSWRecallLoss, r.Metrics.QPS, r.Metrics.RecallAt1, r.Metrics.RecallAt10, r.Metrics.RecallAt100, r.Metrics.BytesPerOp, r.Metrics.AllocsPerOp} {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return errors.New("non-finite metric")
		}
	}
	if r.ResultKind == "router_local_path_evidence" || r.ResultKind == m6CoordinatorResultKindV1 {
		if r.Metrics.BuildWallNanos <= 0 ||
			r.Metrics.SourceHNSWDegree < 1 ||
			r.Metrics.SourceHNSWDegree > maxSourceHNSWDegree ||
			r.Metrics.RepresentativeCount < r.Partitions ||
			r.Metrics.MinRepresentatives < 1 ||
			r.Metrics.MaxRepresentatives < r.Metrics.MinRepresentatives ||
			r.Metrics.RouterBytes <= 0 ||
			r.Metrics.MappedBytes+r.Metrics.HeapCopyBytes <= 0 ||
			r.Metrics.BuildCPUAvailable && r.Metrics.BuildCPUNanos <= 0 ||
			r.Metrics.PeakRSSAvailable && r.Metrics.PeakRSSBytes <= 0 {
			return errors.New("router result lacks build/storage evidence")
		}
	}
	if r.ResultKind == m6CoordinatorResultKindV1 {
		if err := validateM6CoordinatorEvidenceV1(r); err != nil {
			return err
		}
	} else if r.Coordinator != nil {
		return errors.New("non-M6 result contains coordinator evidence")
	}
	return nil
}
func writeArtifacts(out string, r runResult) error {
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	name := artifactBasename(r)
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, name+".json"), append(b, '\n'), 0644); e != nil {
		return e
	}
	stageSummary := "unavailable"
	hnswSummary := "not selected"
	exactRouterSummary := "not selected"
	approximateRouterSummary := "not selected"
	coordinatorSummary := "not selected"
	for _, s := range r.Stages {
		if s.Name == "end_to_end_distributed_simulation" && s.Available {
			stageSummary = fmt.Sprintf("recall@%d: %.4f", r.TopK, s.RecallAtK)
		}
		if s.Name == "treedb_partition_local_hnsw" && s.Available {
			hnswSummary = fmt.Sprintf("recall@%d: %.4f; route=%s; logical_searches=%d; executed=%d; cached=%d; fallbacks=%d", r.TopK, s.RecallAtK, s.RouteKind, s.Searches, s.ExecutedSearches, s.CachedSearches, s.HNSWSearchPackFallbacks)
		}
		if s.Name == "exact_representative_routing" && s.Available {
			exactRouterSummary = fmt.Sprintf("recall@%d: %.4f; representatives=%d; candidates=%d; p50/p95/p99=%d/%d/%d ns; bytes/op=%.1f; allocs/op=%.1f", r.TopK, s.RecallAtK, s.RepresentativeCount, s.Candidates, s.P50Nanos, s.P95Nanos, s.P99Nanos, s.BytesPerOp, s.AllocsPerOp)
		}
		if s.Name == "approximate_representative_routing" && s.Available {
			approximateRouterSummary = fmt.Sprintf("recall@%d: %.4f; candidate_budget=%d; candidates=%d; edges=%d; p50/p95/p99=%d/%d/%d ns; bytes/op=%.1f; allocs/op=%.1f", r.TopK, s.RecallAtK, s.CandidateBudget, s.Candidates, s.Edges, s.P50Nanos, s.P95Nanos, s.P99Nanos, s.BytesPerOp, s.AllocsPerOp)
		}
		if s.Name == m6CoordinatorAttributionStageV1 && s.Available {
			coordinatorSummary = fmt.Sprintf(
				"recall@%d: %.4f; local-service p50/p95/p99=%d/%d/%d ns; candidates=%d",
				r.TopK, s.RecallAtK, s.P50Nanos, s.P95Nanos, s.P99Nanos, s.Candidates,
			)
		}
	}
	title := "TreeDB vector partition M0 simulation"
	disclaimer := "Simulation only; not production Raft evidence."
	if r.ResultKind == "router_local_path_evidence" {
		title = "TreeDB vector partition M4 local router evidence"
		disclaimer = "Local persisted router-path evidence only; not production Raft or M8 acceptance evidence."
	}
	if r.ResultKind == m6CoordinatorResultKindV1 {
		title = "TreeDB vector partition M6 local-service simulation"
		disclaimer = "Local in-process M5 contract simulation only; not production network, Raft read-proof, remote-service, or M8 acceptance evidence."
	}
	md := fmt.Sprintf("# %s\n\n**%s**\n\n- fixture: `%s` (%s)\n- seed: %d\n- GOMAXPROCS: %d\n- Go memory limit: %d bytes\n- source HNSW degree: %d\n- modeled benchmark-owned peak: %d/%d bytes\n- memory budget scope: %s\n- probes: %d/%d\n- overlap budget: %.6g\n- exact representative routing: %s\n- approximate representative routing: %s\n- TreeDB partition-local HNSW: %s\n- end-to-end simulation: %s\n- M6 coordinator local-service simulation: %s\n- timed boundary: %s\n", title, disclaimer, r.Dataset.Fixture, r.Dataset.Checksum, r.Seed, r.GOMAXPROCS, r.GoMemoryLimitBytes, r.Metrics.SourceHNSWDegree, r.ModeledPeakBytes, r.MemoryBudgetBytes, r.MemoryBudgetScope, r.Probes, r.Partitions, r.Overlap, exactRouterSummary, approximateRouterSummary, hnswSummary, stageSummary, coordinatorSummary, r.TimedBoundary)
	return os.WriteFile(filepath.Join(out, name+".md"), []byte(md), 0644)
}
func artifactBasename(r runResult) string {
	prefix := "simulation"
	if r.ResultKind == m6CoordinatorResultKindV1 {
		prefix = "m6_local_service"
	}
	return fmt.Sprintf("%s_f%s_s%016x_n%d_p%d_o%016x_t%s_k%d", prefix, r.Dataset.Checksum, uint64(r.Seed), r.Partitions, r.Probes, math.Float64bits(r.Overlap), artifactStageSetChecksum(r.Stages), r.TopK)
}

func artifactStageSetChecksum(stages []stageResult) string {
	names := make([]string, 0, len(stages))
	for _, stage := range stages {
		names = append(names, stage.Name)
	}
	sort.Strings(names)
	h := sha256.New()
	var length [8]byte
	for _, name := range names {
		binary.LittleEndian.PutUint64(length[:], uint64(len(name)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(name))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func fixtureChecksum(vectors, queries, dims int, seed int64) string {
	m := fixtureManifest{Vectors: vectors, Queries: queries, Dimensions: dims, Seed: seed}
	v, q := deterministicFixture(m)
	return fixtureChecksumFromData(v, q)
}

func fixtureChecksumFromData(v, q [][]float64) string {
	h := sha256.New()
	var b [8]byte
	for _, set := range [][][]float64{v, q} {
		for _, row := range set {
			for _, x := range row {
				binary.LittleEndian.PutUint64(b[:], math.Float64bits(x))
				_, _ = h.Write(b[:])
			}
		}
	}
	// Bind the canonical exact-truth stream too, including stable-ID tie order.
	// Small fixtures bind every available neighbor; canonical fixtures bind top-10.
	truthK := 10
	if len(v) < truthK {
		truthK = len(v)
	}
	for _, query := range q {
		for _, n := range exactTopK(v, query, truthK) {
			_, _ = h.Write([]byte(n.ID))
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(n.Distance))
			_, _ = h.Write(b[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
