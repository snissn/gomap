package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	servicewire "github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	m5ReportSchemaVersion = 1
	m5Collection          = "m3_partition_source"
	m5Index               = "embedding_graph"
	m5Generation          = uint64(1)
	m5RequiredVectors     = 1_000_000
	m5RequiredRoute       = collections.VectorPartitionSearchRouteHNSWSearchPackV1
)

type config struct {
	m3Report       string
	dbDir          string
	out            string
	partition      uint
	topK           int
	efSearch       int
	coldSamples    int
	warmup         int
	warmSamples    int
	baselineWarmup int
	baseline       int
	timeout        time.Duration
	failOnGate     bool
	command        []string
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

type m3Report struct {
	SchemaVersion  int             `json:"schema_version"`
	ResultKind     string          `json:"result_kind"`
	Dataset        fixtureManifest `json:"dataset"`
	ArtifactSHA256 string          `json:"artifact_sha256"`
	BaseSHA        string          `json:"base_sha"`
	HeadSHA        string          `json:"head_sha"`
	Partitions     int             `json:"partitions"`
	TopK           int             `json:"top_k"`
	Rows           []m3Row         `json:"rows"`
}

type m3Row struct {
	Ratio            float64 `json:"ratio"`
	PartitionHNSWM   int     `json:"partition_hnsw_m"`
	ManifestDigest   string  `json:"manifest_digest"`
	SourceGeneration uint64  `json:"source_generation"`
	SourceChecksum   uint64  `json:"source_checksum"`
	SourceSchemaHash uint64  `json:"source_schema_hash"`
	SourceRows       uint64  `json:"source_rows"`
	SearchRoute      string  `json:"search_route"`
	MissingAssets    uint64  `json:"missing_assets"`
	CorruptAssets    uint64  `json:"corrupt_assets"`
	StaleAssets      uint64  `json:"stale_assets"`
	PersistentDBDir  string  `json:"persistent_db_dir"`
}

type distribution struct {
	Samples   int     `json:"samples"`
	MeanNanos float64 `json:"mean_nanos"`
	P50Nanos  uint64  `json:"p50_nanos"`
	P95Nanos  uint64  `json:"p95_nanos"`
	P99Nanos  uint64  `json:"p99_nanos"`
	QPS       float64 `json:"qps"`
}

type allocationMetrics struct {
	BytesPerOp  float64 `json:"bytes_per_op"`
	AllocsPerOp float64 `json:"allocs_per_op"`
	Scope       string  `json:"scope"`
}

type stageDistributions struct {
	RouteOwner     distribution `json:"route_owner"`
	ReadIndexApply distribution `json:"read_index_apply"`
	GenerationOpen distribution `json:"generation_open"`
	Search         distribution `json:"search"`
	ResponseCopy   distribution `json:"response_copy"`
	Total          distribution `json:"total"`
}

type servicePhaseReport struct {
	Latency       distribution       `json:"latency"`
	Stages        stageDistributions `json:"stages"`
	Allocations   allocationMetrics  `json:"allocations"`
	RequestBytes  uint64             `json:"request_bytes"`
	ResponseBytes float64            `json:"mean_response_bytes"`
	Candidates    float64            `json:"mean_candidates"`
	Edges         float64            `json:"mean_edges"`
	MappedBytes   uint64             `json:"mapped_bytes"`
	HeapBytes     uint64             `json:"heap_bytes"`
	PackBytes     uint64             `json:"pack_bytes"`
	SearchRoute   string             `json:"search_route"`
}

type baselineReport struct {
	Latency     distribution      `json:"latency"`
	Allocations allocationMetrics `json:"allocations"`
	Candidates  float64           `json:"mean_candidates"`
	Edges       float64           `json:"mean_edges"`
	SearchRoute string            `json:"search_route"`
}

type sourceCacheReport struct {
	GenerationHits   uint64 `json:"generation_hits"`
	GenerationMisses uint64 `json:"generation_misses"`
	PartitionHits    uint64 `json:"partition_hits"`
	PartitionMisses  uint64 `json:"partition_misses"`
	Invalidations    uint64 `json:"invalidations"`
}

type provenanceReport struct {
	RunnerHeadSHA  string   `json:"runner_head_sha"`
	RunnerBranch   string   `json:"runner_branch"`
	RunnerDirty    bool     `json:"runner_dirty"`
	M3ReportPath   string   `json:"m3_report_path"`
	M3ReportSHA256 string   `json:"m3_report_sha256"`
	M3BaseSHA      string   `json:"m3_base_sha"`
	M3HeadSHA      string   `json:"m3_head_sha"`
	M3ArtifactSHA  string   `json:"m3_artifact_sha256"`
	PersistentDB   string   `json:"persistent_db_dir"`
	ExactCommand   []string `json:"exact_command"`
}

type hostReport struct {
	GoVersion string `json:"go_version"`
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	CPUs      int    `json:"logical_cpus"`
	CPUModel  string `json:"cpu_model"`
}

type raftReport struct {
	Provider          string   `json:"provider"`
	Peers             []string `json:"peers"`
	Transport         string   `json:"transport"`
	LogStableSnapshot string   `json:"log_stable_snapshot_stores"`
	GroupID           string   `json:"group_id"`
	LeaderID          string   `json:"leader_id"`
	CommittedTerm     uint64   `json:"committed_term"`
	CommittedIndex    uint64   `json:"committed_index"`
	CommitEvidence    string   `json:"commit_evidence"`
	ReadIndexEvidence string   `json:"read_index_evidence"`
	TreeDBMutation    string   `json:"treedb_mutation"`
}

type manifestReport struct {
	Collection            string `json:"collection"`
	Index                 string `json:"index"`
	IndexDefinitionDigest string `json:"index_definition_digest"`
	SourceGeneration      uint64 `json:"source_generation"`
	SourceChecksum        uint64 `json:"source_checksum"`
	SourceSchemaHash      uint64 `json:"source_schema_hash"`
	SourceRows            uint64 `json:"source_rows"`
	PartitionGeneration   uint64 `json:"partition_generation"`
	RouterGeneration      uint64 `json:"router_generation"`
	ManifestDigest        string `json:"manifest_digest"`
	PartitionID           uint32 `json:"partition_id"`
	PartitionCount        uint32 `json:"partition_count"`
	PartitionHNSWM        int    `json:"partition_hnsw_m"`
	OwnerGroup            string `json:"owner_group"`
}

type gateReport struct {
	ThresholdRatio               float64 `json:"threshold_ratio"`
	WarmMeanServiceOverheadNanos float64 `json:"warm_mean_service_overhead_nanos"`
	DirectMeanHNSWSearchNanos    float64 `json:"direct_mean_hnsw_search_nanos"`
	ObservedRatio                float64 `json:"observed_ratio"`
	Status                       string  `json:"status"`
	MeasurementValid             bool    `json:"measurement_valid"`
	FailureReason                string  `json:"failure_reason,omitempty"`
	NumeratorDefinition          string  `json:"numerator_definition"`
	DenominatorDefinition        string  `json:"denominator_definition"`
}

type report struct {
	SchemaVersion int              `json:"schema_version"`
	ResultKind    string           `json:"result_kind"`
	GeneratedAt   string           `json:"generated_at"`
	Status        string           `json:"status"`
	Provenance    provenanceReport `json:"provenance"`
	Host          hostReport       `json:"host"`
	Dataset       fixtureManifest  `json:"dataset"`
	Manifest      manifestReport   `json:"manifest"`
	Raft          raftReport       `json:"raft"`
	Config        struct {
		TopK            int `json:"top_k"`
		EfSearch        int `json:"ef_search"`
		ColdSamples     int `json:"cold_samples"`
		Warmup          int `json:"warmup"`
		WarmSamples     int `json:"warm_samples"`
		BaselineWarmup  int `json:"baseline_warmup"`
		BaselineSamples int `json:"baseline_samples"`
	} `json:"config"`
	Cold     servicePhaseReport `json:"cold"`
	Warm     servicePhaseReport `json:"warm"`
	Baseline baselineReport     `json:"direct_partition_local_hnsw"`
	Cache    sourceCacheReport  `json:"warm_source_cache"`
	Gate     gateReport         `json:"overhead_gate"`
	Caveats  []string           `json:"caveats"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "treedb-vector-partition-m5-bench:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	head, branch, dirty, err := gitState()
	if err != nil {
		return err
	}
	if dirty {
		return errors.New("runner requires a clean worktree for exact code provenance")
	}
	m3, m3Raw, err := loadM3Report(cfg.m3Report)
	if err != nil {
		return err
	}
	row, err := validateM3Input(m3, cfg.dbDir, cfg.partition)
	if err != nil {
		return err
	}
	dbDir := cfg.dbDir
	if dbDir == "" {
		dbDir = row.PersistentDBDir
	}
	dbDir, err = filepath.Abs(dbDir)
	if err != nil {
		return err
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dbDir, DisableBackgroundPrune: true})
	if err != nil {
		return fmt.Errorf("open persistent M3 DB: %w", err)
	}
	defer db.Close()
	collection, err := collections.NewCollectionManager(db).OpenCollection(m5Collection)
	if err != nil {
		return fmt.Errorf("open collection %q: %w", m5Collection, err)
	}
	status, err := collection.VectorPartitionStatusV1(m5Index, m5Generation)
	if err != nil {
		return fmt.Errorf("load generation status: %w", err)
	}
	if err := validatePersistentStatus(status, row, uint32(cfg.partition)); err != nil {
		return err
	}
	query, err := deterministicQuery(m3.Dataset, 0)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	cluster, err := openLocalRaftCluster(ctx, status.Manifest.Placements[cfg.partition].GroupID)
	if err != nil {
		return err
	}
	defer cluster.Close()
	commit, err := cluster.commitProofCommand(ctx)
	if err != nil {
		return err
	}
	env, err := newBenchmarkEnvironment(collection, status, cluster, uint32(cfg.partition), query, cfg)
	if err != nil {
		return err
	}
	defer env.Close()
	cold, err := env.measureCold(ctx, cfg.coldSamples)
	if err != nil {
		return err
	}
	warm, baseline, cache, err := env.measureWarmAndBaseline(ctx, cfg)
	if err != nil {
		return err
	}
	gate := evaluateGate(warm.stages, baseline.phase.Latency)
	result := report{
		SchemaVersion: m5ReportSchemaVersion,
		ResultKind:    "m5_real_raft_persistent_1m_hnsw_evidence",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Status:        strings.ToLower(gate.Status),
		Provenance: provenanceReport{
			RunnerHeadSHA:  head,
			RunnerBranch:   branch,
			RunnerDirty:    dirty,
			M3ReportPath:   mustAbs(cfg.m3Report),
			M3ReportSHA256: sha256Hex(m3Raw),
			M3BaseSHA:      m3.BaseSHA,
			M3HeadSHA:      m3.HeadSHA,
			M3ArtifactSHA:  m3.ArtifactSHA256,
			PersistentDB:   dbDir,
			ExactCommand:   cfg.command,
		},
		Host: hostReport{
			GoVersion: runtime.Version(),
			GOOS:      runtime.GOOS,
			GOARCH:    runtime.GOARCH,
			CPUs:      runtime.NumCPU(),
			CPUModel:  cpuModel(),
		},
		Dataset: m3.Dataset,
		Manifest: manifestReport{
			Collection:            status.Manifest.Collection,
			Index:                 status.Manifest.IndexName,
			IndexDefinitionDigest: status.Manifest.IndexDefinitionDigest,
			SourceGeneration:      status.Manifest.SourceGeneration,
			SourceChecksum:        status.Manifest.SourceChecksum,
			SourceSchemaHash:      status.Manifest.SourceSchemaHash,
			SourceRows:            status.Manifest.SourceRowCount,
			PartitionGeneration:   status.Manifest.Generation,
			RouterGeneration:      status.Manifest.RouterGeneration,
			ManifestDigest:        status.Manifest.IntegrityDigest,
			PartitionID:           uint32(cfg.partition),
			PartitionCount:        status.Manifest.PartitionCount,
			PartitionHNSWM:        row.PartitionHNSWM,
			OwnerGroup:            status.Manifest.Placements[cfg.partition].GroupID,
		},
		Raft: raftReport{
			Provider:          "github.com/hashicorp/raft via raftcluster.HashicorpRaftProvider",
			Peers:             sortedNodeIDs(cluster.nodes),
			Transport:         "three-node in-process HashiCorp Raft in-memory transport with live quorum exchange",
			LogStableSnapshot: "caller-owned in-memory log, stable, and snapshot stores",
			GroupID:           string(cluster.groupID),
			LeaderID:          string(cluster.leader),
			CommittedTerm:     commit.Entry.Term,
			CommittedIndex:    commit.Entry.Index,
			CommitEvidence:    string(commit.Evidence.Kind),
			ReadIndexEvidence: "production",
			TreeDBMutation:    "none: the structurally valid proof command is applied only by the benchmark progress appliers",
		},
		Cold:     cold.phase,
		Warm:     warm.phase,
		Baseline: baseline.phase,
		Cache:    cache,
		Gate:     gate,
		Caveats: []string{
			"cold samples reopen the generation source and mmap per sample; only the first sample is OS-page-cache cold",
			"Raft transport and Raft log/stable/snapshot stores are in-memory; the 1M TreeDB M3 database and HNSW packs are persistent on disk",
			"B/op and allocs/op use process-wide runtime counters and can include live Raft background goroutine activity",
			"the request is one partition-local no-document HNSW shard search and performs no network serialization",
		},
	}
	result.Config.TopK = cfg.topK
	result.Config.EfSearch = cfg.efSearch
	result.Config.ColdSamples = cfg.coldSamples
	result.Config.Warmup = cfg.warmup
	result.Config.WarmSamples = cfg.warmSamples
	result.Config.BaselineWarmup = cfg.baselineWarmup
	result.Config.BaselineSamples = cfg.baseline
	if err := writeReport(cfg.out, result); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "m5 report=%s status=%s overhead_ratio=%.6f threshold=%.2f\n", mustAbs(cfg.out), result.Status, result.Gate.ObservedRatio, result.Gate.ThresholdRatio)
	if cfg.failOnGate && result.Gate.Status != "PASS" {
		if !result.Gate.MeasurementValid {
			return fmt.Errorf("M5 overhead gate unmeasurable: %s (artifact written)", result.Gate.FailureReason)
		}
		return fmt.Errorf("M5 overhead gate failed: observed %.6f > %.6f (artifact written)", result.Gate.ObservedRatio, result.Gate.ThresholdRatio)
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	fs := flag.NewFlagSet("treedb-vector-partition-m5-bench", flag.ContinueOnError)
	var cfg config
	fs.StringVar(&cfg.m3Report, "m3-report", "", "M3 JSON report for the retained 1M persistent generation")
	fs.StringVar(&cfg.dbDir, "db", "", "persistent M3 DB directory (defaults to the report row)")
	fs.StringVar(&cfg.out, "out", "", "output JSON path")
	fs.UintVar(&cfg.partition, "partition", 0, "logical partition to search")
	fs.IntVar(&cfg.topK, "top-k", 10, "partition-local result count")
	fs.IntVar(&cfg.efSearch, "ef-search", 64, "native HNSW traversal budget")
	fs.IntVar(&cfg.coldSamples, "cold-samples", 5, "fresh generation-source samples")
	fs.IntVar(&cfg.warmup, "warmup", 20, "service warmup requests")
	fs.IntVar(&cfg.warmSamples, "warm-samples", 500, "timed warm service requests")
	fs.IntVar(&cfg.baselineWarmup, "baseline-warmup", 20, "direct HNSW warmup searches")
	fs.IntVar(&cfg.baseline, "baseline-samples", 500, "timed direct HNSW searches")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Minute, "whole-run deadline")
	fs.BoolVar(&cfg.failOnGate, "fail-on-gate", true, "return non-zero after writing a failing artifact")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 || cfg.m3Report == "" || cfg.out == "" {
		return config{}, errors.New("-m3-report and -out are required and positional arguments are unsupported")
	}
	if cfg.topK < 1 || cfg.efSearch < cfg.topK || cfg.coldSamples < 1 || cfg.warmup < 0 || cfg.warmSamples < 1 || cfg.baselineWarmup < 0 || cfg.baseline < 1 || cfg.timeout <= 0 {
		return config{}, errors.New("invalid benchmark bounds")
	}
	cfg.command = append([]string{"GOWORK=off", "go", "run", "./TreeDB/cmd/treedb_vector_partition_m5_bench"}, args...)
	return cfg, nil
}

func loadM3Report(path string) (m3Report, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return m3Report{}, nil, err
	}
	var report m3Report
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&report); err != nil {
		return m3Report{}, nil, err
	}
	return report, raw, nil
}

func validateM3Input(report m3Report, dbOverride string, partition uint) (m3Row, error) {
	if report.SchemaVersion != 3 || report.ResultKind != "m3_native_partition_hnsw_evidence" {
		return m3Row{}, errors.New("unsupported M3 report identity")
	}
	if report.Dataset.Vectors != m5RequiredVectors || report.Dataset.Queries < 1 || report.Dataset.Dimensions < 4 || report.Dataset.Metric != "cosine" {
		return m3Row{}, fmt.Errorf("M5 acceptance requires exactly %d cosine vectors and at least one query", m5RequiredVectors)
	}
	if report.Partitions < 1 || partition >= uint(report.Partitions) || len(report.Rows) != 1 {
		return m3Row{}, errors.New("M5 acceptance requires one M3 row and an in-range partition")
	}
	row := report.Rows[0]
	if row.Ratio != 0 || row.PartitionHNSWM < 2 || row.SourceRows != m5RequiredVectors || row.SearchRoute != m5RequiredRoute ||
		row.MissingAssets != 0 || row.CorruptAssets != 0 || row.StaleAssets != 0 ||
		row.SourceGeneration == 0 || row.SourceChecksum == 0 || row.SourceSchemaHash == 0 ||
		len(row.ManifestDigest) != 64 {
		return m3Row{}, errors.New("M3 row does not prove the retained healthy 1M native-HNSW generation")
	}
	if dbOverride == "" && row.PersistentDBDir == "" {
		return m3Row{}, errors.New("persistent M3 DB path is absent")
	}
	return row, nil
}

func validatePersistentStatus(status collections.VectorPartitionStatusV1, row m3Row, partition uint32) error {
	manifest := status.Manifest
	if !status.Ready || !status.Active || status.StaleReason != "" ||
		status.MissingAssets != 0 || status.CorruptAssets != 0 || status.StaleAssets != 0 {
		return errors.New("persistent M3 generation is not active, ready, and healthy")
	}
	if manifest.Collection != m5Collection || manifest.IndexName != m5Index ||
		manifest.Generation != m5Generation || manifest.SourceRowCount != m5RequiredVectors ||
		manifest.SourceGeneration != row.SourceGeneration || manifest.SourceChecksum != row.SourceChecksum ||
		manifest.SourceSchemaHash != row.SourceSchemaHash || manifest.IntegrityDigest != row.ManifestDigest ||
		manifest.PartitionCount == 0 || len(manifest.Placements) != int(manifest.PartitionCount) ||
		partition >= manifest.PartitionCount || manifest.RouterGeneration == 0 {
		return errors.New("persistent M3 manifest does not match report identity")
	}
	for i, placement := range manifest.Placements {
		if placement.PartitionID != uint32(i) || placement.GroupID == "" {
			return errors.New("persistent M3 manifest placement is incomplete")
		}
	}
	return nil
}

func deterministicQuery(fixture fixtureManifest, queryIndex int) ([]float32, error) {
	if fixture.Generator != "treedb_vector_partition_fixture_v2" ||
		fixture.Arithmetic != "ieee754_binary64_explicit_fma_v1" ||
		fixture.Dimensions < 4 || fixture.Seed < 0 || queryIndex != 0 {
		return nil, errors.New("unsupported deterministic fixture query contract")
	}
	vectorIndex := 17
	vector := make([]float64, fixture.Dimensions)
	cluster := (vectorIndex / 97) % 4
	vector[cluster%fixture.Dimensions] = 1
	var norm float64
	for dimension := 4; dimension < fixture.Dimensions; dimension++ {
		vector[dimension] = float64(((vectorIndex+1)*(dimension+3)+int(fixture.Seed))%31) / 310
	}
	for _, value := range vector {
		norm = math.FMA(value, value, norm)
	}
	norm = math.Sqrt(norm)
	query := make([]float32, len(vector))
	for i, value := range vector {
		query[i] = float32(value / norm)
	}
	return query, nil
}

func distributionOf(samples []uint64, elapsed time.Duration) distribution {
	if len(samples) == 0 {
		return distribution{}
	}
	sorted := append([]uint64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total uint64
	for _, sample := range samples {
		total += sample
	}
	qps := 0.0
	if elapsed > 0 {
		qps = float64(len(samples)) / elapsed.Seconds()
	}
	return distribution{
		Samples:   len(samples),
		MeanNanos: float64(total) / float64(len(samples)),
		P50Nanos:  percentile(sorted, 0.50),
		P95Nanos:  percentile(sorted, 0.95),
		P99Nanos:  percentile(sorted, 0.99),
		QPS:       qps,
	}
}

func percentile(sorted []uint64, quantile float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func evaluateGate(stages stageSamples, baseline distribution) gateReport {
	report := gateReport{
		ThresholdRatio:        0.10,
		Status:                "FAIL",
		NumeratorDefinition:   "mean warm response TotalNanos minus ReadIndexApplyNanos minus SearchNanos, saturated at zero per request",
		DenominatorDefinition: "mean wall time of direct SearchWithOptionsV1 on the same pinned persistent partition and query",
	}
	if len(stages.total) == 0 {
		report.FailureReason = "no warm stage samples"
		return report
	}
	if len(stages.readIndexApply) != len(stages.total) || len(stages.search) != len(stages.total) {
		report.FailureReason = "incomplete warm stage samples"
		return report
	}
	var overhead uint64
	for i := range stages.total {
		value := stages.total[i]
		excluded := stages.readIndexApply[i] + stages.search[i]
		if value > excluded {
			overhead += value - excluded
		}
	}
	report.WarmMeanServiceOverheadNanos = float64(overhead) / float64(len(stages.total))
	report.DirectMeanHNSWSearchNanos = baseline.MeanNanos
	if baseline.MeanNanos <= 0 {
		report.FailureReason = "non-positive direct HNSW baseline"
		return report
	}
	report.MeasurementValid = true
	report.ObservedRatio = report.WarmMeanServiceOverheadNanos / baseline.MeanNanos
	if report.ObservedRatio <= report.ThresholdRatio {
		report.Status = "PASS"
	}
	return report
}

func writeReport(path string, report report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func gitState() (head, branch string, dirty bool, err error) {
	run := func(args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		raw, commandErr := cmd.Output()
		return strings.TrimSpace(string(raw)), commandErr
	}
	if head, err = run("rev-parse", "HEAD"); err != nil {
		return "", "", false, err
	}
	if branch, err = run("branch", "--show-current"); err != nil {
		return "", "", false, err
	}
	status, err := run("status", "--porcelain")
	if err != nil {
		return "", "", false, err
	}
	return head, branch, status != "", nil
}

func cpuModel() string {
	raw, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func requestBytes(request servicewire.VectorPartitionShardSearchRequestV1) uint64 {
	total := uint64(256 + len(request.Query)*4 + len(request.PartitionIDs)*4)
	for _, identity := range []string{
		request.RequestID,
		request.CancellationID,
		request.Database,
		request.Catalog,
		request.Collection,
		request.IndexName,
		request.IndexDefinitionDigest,
		string(request.TargetGroupID),
		string(request.TargetNodeID),
	} {
		total += uint64(len(identity))
	}
	return total
}

func sourceCache(stats servicewire.CollectionVectorPartitionGenerationCacheStatsV1) sourceCacheReport {
	return sourceCacheReport{
		GenerationHits:   stats.GenerationHits,
		GenerationMisses: stats.GenerationMisses,
		PartitionHits:    stats.PartitionHits,
		PartitionMisses:  stats.PartitionMisses,
		Invalidations:    stats.Invalidations,
	}
}

func sha256Hex(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func mustAbs(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absolute
}
