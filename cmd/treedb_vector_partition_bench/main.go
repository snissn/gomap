// treedb_vector_partition_bench is the M0 deterministic oracle and sequential
// simulation harness. Its partition-local HNSW attribution stage uses real
// temporary TreeDB column_graph indexes; it does not implement routed Raft.
package main

import (
	"crypto/sha256"
	"encoding/binary"
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
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	schemaVersion             = 1
	maxVectors                = 1_000_000
	maxDimensions             = 4_096
	maxPartitions             = 16_384
	maxFixtureBytes     int64 = 4 << 30
	maxManifestBytes    int64 = 64 << 10
	maxGitHubEventBytes int64 = 2 << 20
	partitionHNSWIndex        = "embedding_graph"
	partitionHNSWDegree       = 16
)

type config struct {
	dataset      string
	partitions   int
	probes       []int
	overlaps     []float64
	topK         int
	recallTarget float64
	seed         int64
	format       string
	out          string
	stages       map[string]bool
	command      []string
	maxVectors   int
	maxBytes     int64
	baseSHA      string
	headSHA      string
	hnsw         *treeDBPartitionHNSW
}

type fixtureManifest struct {
	SchemaVersion int    `json:"schema_version"`
	Fixture       string `json:"fixture"`
	Generator     string `json:"generator"`
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
	Searches                  uint64  `json:"searches,omitempty"`
	ExecutedSearches          uint64  `json:"executed_searches,omitempty"`
	CachedSearches            uint64  `json:"cached_searches,omitempty"`
	SearchRouteHNSWSearchPack uint64  `json:"search_route_hnsw_search_pack,omitempty"`
	HNSWSearchPackActive      uint64  `json:"hnsw_search_pack_active,omitempty"`
	HNSWSearchPackFallbacks   uint64  `json:"hnsw_search_pack_fallbacks,omitempty"`
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

// metricsV1 reserves every M0 evidence field. Simulation leaves production-only
// measurements at zero and labels them not_measured instead of implying a Raft result.
type metricsV1 struct {
	MeasurementStatus       string  `json:"measurement_status"`
	BuildWallNanos          int64   `json:"build_wall_nanos"`
	BuildCPUNanos           int64   `json:"build_cpu_nanos"`
	PeakRSSBytes            int64   `json:"peak_rss_bytes"`
	TemporaryBytes          int64   `json:"temporary_bytes"`
	FinalBytes              int64   `json:"final_bytes"`
	BytesPerVector          float64 `json:"bytes_per_vector"`
	Balance                 float64 `json:"balance"`
	MaxPartitionSize        int     `json:"max_partition_size"`
	EdgeCut                 int64   `json:"edge_cut"`
	ReplicationFactor       float64 `json:"replication_factor"`
	UnassignedOverlapBudget float64 `json:"unassigned_overlap_budget"`
	RepresentativeCount     int     `json:"representative_count"`
	RouterBytes             int64   `json:"router_bytes"`
	RoutingLatencyNanos     int64   `json:"routing_latency_nanos"`
	RoutedPartitionRecall   float64 `json:"routed_partition_recall"`
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
	SchemaVersion      int             `json:"schema_version"`
	ResultKind         string          `json:"result_kind"`
	ProductionEvidence bool            `json:"production_evidence"`
	Command            []string        `json:"command"`
	BaseSHA            string          `json:"base_sha"`
	HeadSHA            string          `json:"head_sha"`
	GoVersion          string          `json:"go_version"`
	Hardware           string          `json:"hardware_context"`
	Dataset            fixtureManifest `json:"dataset"`
	Partitions         int             `json:"partitions"`
	Overlap            float64         `json:"overlap"`
	Probes             int             `json:"probes"`
	TopK               int             `json:"top_k"`
	RecallTarget       float64         `json:"recall_target"`
	Seed               int64           `json:"seed"`
	Warmup             int             `json:"warmup"`
	Samples            int             `json:"samples"`
	TimedBoundary      string          `json:"timed_boundary"`
	Stages             []stageResult   `json:"stages"`
	Metrics            metricsV1       `json:"metrics"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "treedb-vector-partition-bench:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) (runErr error) {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	cfg.command = append([]string{"treedb_vector_partition_bench"}, args...)
	if cfg.baseSHA, cfg.headSHA, err = provenance(); err != nil {
		return err
	}
	fixture, err := loadFixture(cfg.dataset)
	if err != nil {
		return err
	}
	if err := validateFixtureWithCaps(fixture, cfg.maxVectors, cfg.maxBytes); err != nil {
		return err
	}
	if cfg.topK > fixture.Vectors {
		return errors.New("top-k cannot exceed fixture vectors")
	}
	if cfg.partitions > fixture.Vectors {
		return errors.New("partitions cannot exceed fixture vectors")
	}
	vectors, queries := deterministicFixture(fixture)
	if fixtureChecksum(fixture.Vectors, fixture.Queries, fixture.Dimensions, fixture.Seed) != fixture.Checksum {
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
				b, _ := json.Marshal(result)
				fmt.Fprintln(stdout, string(b))
			} else {
				fmt.Fprintf(stdout, "simulation probes=%d overlap=%.2f recall@%d=%.4f\n", probes, overlap, cfg.topK, result.Stages[len(result.Stages)-1].RecallAtK)
			}
		}
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{format: "json", topK: 10, recallTarget: .9, seed: 1}
	var probes, overlap string
	var stages string
	fs := flag.NewFlagSet("treedb_vector_partition_bench", flag.ContinueOnError)
	fs.StringVar(&cfg.dataset, "dataset", "", "fixture directory")
	fs.IntVar(&cfg.partitions, "partitions", 0, "logical partition count")
	fs.StringVar(&probes, "probes", "", "comma-separated probe counts")
	fs.StringVar(&overlap, "overlap", "0", "comma-separated derived overlap budgets")
	fs.IntVar(&cfg.topK, "top-k", cfg.topK, "top-k")
	fs.Float64Var(&cfg.recallTarget, "recall-target", cfg.recallTarget, "recall target")
	fs.Int64Var(&cfg.seed, "seed", cfg.seed, "simulation seed")
	fs.StringVar(&cfg.format, "format", cfg.format, "json or text")
	fs.StringVar(&cfg.out, "out", "", "artifact directory")
	fs.StringVar(&stages, "stages", "all", "comma-separated independently enabled loss stages, or all")
	fs.IntVar(&cfg.maxVectors, "max-vectors", maxVectors, "maximum combined fixture vector/query count before allocation")
	fs.Int64Var(&cfg.maxBytes, "max-fixture-bytes", maxFixtureBytes, "maximum vector or query bytes before allocation")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	var err error
	if cfg.probes, err = parseInts(probes); err != nil {
		return config{}, fmt.Errorf("probes: %w", err)
	}
	if cfg.overlaps, err = parseFloats(overlap); err != nil {
		return config{}, fmt.Errorf("overlap: %w", err)
	}
	if cfg.dataset == "" || cfg.out == "" || cfg.partitions < 1 || cfg.partitions > maxPartitions || cfg.topK < 1 || cfg.maxVectors < 1 || cfg.maxVectors > maxVectors || cfg.maxBytes < 8 || cfg.maxBytes > maxFixtureBytes || cfg.format != "json" && cfg.format != "text" {
		return config{}, errors.New("dataset, out, positive bounded partitions/top-k, and json|text format are required")
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
	cfg.stages = stageSet(stages)
	if len(cfg.stages) == 0 {
		return config{}, errors.New("stages must name known stages or all")
	}
	return cfg, nil
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
	base, head := os.Getenv("BASE_SHA"), os.Getenv("GITHUB_SHA")
	if head == "" {
		out, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			return "", "", errors.New("git head unavailable: set GITHUB_SHA")
		}
		head = strings.TrimSpace(string(out))
	}
	if base == "" {
		if eventPath := os.Getenv("GITHUB_EVENT_PATH"); eventPath != "" {
			eventBase, err := pullRequestBaseSHAFromEvent(eventPath)
			if err != nil {
				return "", "", fmt.Errorf("GitHub event base SHA: %w", err)
			}
			base = eventBase
		}
	}
	if base == "" {
		out, err := exec.Command("git", "merge-base", "HEAD", "origin/main").Output()
		if err != nil {
			return "", "", errors.New("git merge-base unavailable: set BASE_SHA")
		}
		base = strings.TrimSpace(string(out))
	}
	if _, err := hex.DecodeString(base); err != nil || len(base) != 40 {
		return "", "", errors.New("invalid BASE_SHA")
	}
	if _, err := hex.DecodeString(head); err != nil || len(head) != 40 {
		return "", "", errors.New("invalid GITHUB_SHA")
	}
	return base, head, nil
}

func pullRequestBaseSHAFromEvent(path string) (string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxGitHubEventBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > maxGitHubEventBytes {
		return "", fmt.Errorf("event payload exceeds %d-byte cap", maxGitHubEventBytes)
	}
	var event struct {
		PullRequest *struct {
			Base struct {
				SHA string `json:"sha"`
			} `json:"base"`
		} `json:"pull_request"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&event); err != nil {
		return "", err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return "", errors.New("GitHub event has trailing JSON")
	}
	if event.PullRequest == nil {
		return "", nil
	}
	return strings.TrimSpace(event.PullRequest.Base.SHA), nil
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
	if m.SchemaVersion != schemaVersion || m.Fixture == "" || m.Generator != "treedb_vector_partition_fixture_v1" || m.Vectors < 1 || m.Vectors > capVectors || m.Queries < 1 || m.Queries > capVectors || m.Dimensions < 1 || m.Dimensions > maxDimensions || m.Metric != "cosine" || len(m.Checksum) != 64 {
		return errors.New("unsupported or malformed fixture manifest")
	}
	if int64(m.Vectors)+int64(m.Queries) > int64(capVectors) {
		return errors.New("combined fixture vector/query count exceeds pre-allocation cap")
	}
	if int64(m.Vectors)+int64(m.Queries) > capBytes/(int64(m.Dimensions)*8) {
		return errors.New("fixture byte product exceeds pre-allocation cap")
	}
	_, e := hex.DecodeString(m.Checksum)
	return e
}

// deterministicFixture has intentionally visible cluster/boundary pairs and a
// duplicate pair, so tie ordering remains part of the executable M0 contract.
func deterministicFixture(m fixtureManifest) ([][]float64, [][]float64) {
	v := make([][]float64, m.Vectors)
	for i := range v {
		row := make([]float64, m.Dimensions)
		cluster := (i / 97) % 4
		row[cluster%m.Dimensions] = 1
		for d := 4; d < m.Dimensions; d++ {
			row[d] = float64(((i+1)*(d+3)+int(m.Seed))%31) / 310
		}
		v[i] = normalize(row)
	}
	if len(v) > 1 {
		v[1] = append([]float64(nil), v[0]...)
	}
	q := make([][]float64, m.Queries)
	for i := range q {
		q[i] = append([]float64(nil), v[(i*7919+17)%len(v)]...)
	}
	return v, q
}
func normalize(v []float64) []float64 {
	var n float64
	for _, x := range v {
		n += x * x
	}
	n = math.Sqrt(n)
	for i := range v {
		v[i] /= n
	}
	return v
}
func exactTopK(v [][]float64, q []float64, k int) []neighbor {
	out := make([]neighbor, len(v))
	for i, row := range v {
		var dot float64
		for d := range row {
			dot += row[d] * q[d]
		}
		out[i] = neighbor{ID: fmt.Sprintf("doc-%06d", i), Distance: 1 - dot}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Distance == out[j].Distance {
			return out[i].ID < out[j].ID
		}
		return out[i].Distance < out[j].Distance
	})
	return out[:k]
}
func partitionTopK(v [][]float64, q []float64, k, partitions, probes int) []neighbor {
	selected := make([]int, probes)
	for i := range selected {
		selected[i] = i
	}
	return selectedPartitionTopK(v, q, k, partitions, selected)
}
func selectedPartitionTopK(v [][]float64, q []float64, k, partitions int, selected []int) []neighbor {
	want := map[int]bool{}
	for _, p := range selected {
		want[p] = true
	}
	all := make([]neighbor, 0, len(v)*len(selected)/partitions+1)
	for i, row := range v {
		if !want[i%partitions] {
			continue
		}
		var dot float64
		for d := range row {
			dot += row[d] * q[d]
		}
		all = append(all, neighbor{fmt.Sprintf("doc-%06d", i), 1 - dot})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Distance == all[j].Distance {
			return all[i].ID < all[j].ID
		}
		return all[i].Distance < all[j].Distance
	})
	if len(all) < k {
		return all
	}
	return all[:k]
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
	sums := make([][]float64, partitions)
	counts := make([]int, partitions)
	for i, row := range v {
		p := i % partitions
		if sums[p] == nil {
			sums[p] = make([]float64, len(q))
		}
		for d := range row {
			sums[p][d] += row[d]
		}
		counts[p]++
	}
	scores := make([]neighbor, partitions)
	for p, sum := range sums {
		var dot float64
		for d := range sum {
			dot += q[d] * (sum[d] / float64(counts[p]))
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
			M:              partitionHNSWDegree,
			EfConstruction: 128,
			EfSearch:       128,
			Encoding:       collections.VectorIndexEncodingFloat32,
			Strategy:       collections.VectorIndexStrategyColumnGraph,
		}},
	}
}

func insertPartitionRows(col *collections.Collection, vectors [][]float64, partition, partitions int) error {
	rowCount := moduloPartitionSize(len(vectors), partitions, partition)
	ids := make([][]byte, 0, rowCount)
	documents := make([][]byte, 0, rowCount)
	for i := partition; i < len(vectors); i += partitions {
		vector := make([]float32, len(vectors[i]))
		for d, value := range vectors[i] {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return fmt.Errorf("vector %d dimension %d is non-finite", i, d)
			}
			vector[d] = float32(value)
		}
		id := fmt.Sprintf("doc-%06d", i)
		raw, err := json.Marshal(struct {
			TimeUS    int64     `json:"time_us"`
			Embedding []float32 `json:"embedding"`
		}{
			TimeUS:    int64(i + 1),
			Embedding: vector,
		})
		if err != nil {
			return err
		}
		ids = append(ids, []byte(id))
		documents = append(documents, raw)
	}
	if len(ids) != rowCount {
		return fmt.Errorf("partition %d rows=%d want %d", partition, len(ids), rowCount)
	}
	_, err := col.InsertBatch(ids, documents)
	return err
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
func simulate(cfg config, m fixtureManifest, v, q [][]float64, probes int, overlap float64) (runResult, error) {
	if cfg.stages == nil {
		cfg.stages = stageSet("all")
	}
	if overlap != 0 { /* M0 reports budget only; M3 owns membership copies. */
	}
	totals := map[string]float64{}
	var hnswEvidence hnswSearchEvidence
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
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, representativePartitions(v, query, cfg.partitions, probes, false))
			totals["exact_representative_routing"] += recall(truth, got)
		}
		if cfg.stages["approximate_representative_routing"] {
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, representativePartitions(v, query, cfg.partitions, probes, true))
			totals["approximate_representative_routing"] += recall(truth, got)
		}
		if cfg.stages["exact_partition_local"] {
			parts := representativePartitions(v, query, cfg.partitions, probes, false)
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["exact_partition_local"] += recall(truth, got)
		}
		if cfg.stages["treedb_partition_local_hnsw"] {
			if cfg.hnsw == nil {
				return runResult{}, errors.New("TreeDB partition-local HNSW stage selected without initialized TreeDB indexes")
			}
			parts := representativePartitions(v, query, cfg.partitions, probes, false)
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
			parts := representativePartitions(v, query, cfg.partitions, probes, true)
			got := selectedPartitionTopK(v, query, cfg.topK, cfg.partitions, parts)
			totals["end_to_end_distributed_simulation"] += recall(truth, dedupeStable(got))
		}
	}
	n := float64(len(q))
	stage := func(name, method string, lossy bool, value float64, p int) stageResult {
		return stageResult{Name: name, Method: method, Enabled: cfg.stages[name], Lossy: lossy, RecallAtK: value, Queries: len(q), Probes: p, Available: true}
	}
	hnswStage := stage("treedb_partition_local_hnsw", "treedb_column_graph_exact_hnsw_search_pack_v1", true, totals["treedb_partition_local_hnsw"]/n, probes)
	hnswStage.RouteKind = string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1)
	hnswStage.Searches = hnswEvidence.Searches
	hnswStage.ExecutedSearches = hnswEvidence.ExecutedSearches
	hnswStage.CachedSearches = hnswEvidence.CachedSearches
	hnswStage.SearchRouteHNSWSearchPack = hnswEvidence.SearchRouteHNSWSearchPack
	hnswStage.HNSWSearchPackActive = hnswEvidence.HNSWSearchPackActive
	hnswStage.HNSWSearchPackFallbacks = hnswEvidence.HNSWSearchPackFallbacks
	allStages := []stageResult{stage("exact_global_top_k", "global_exhaustive", false, totals["exact_global_top_k"]/n, cfg.partitions), stage("partition_oracle", "round_robin_partition_exhaustive", probes < cfg.partitions, totals["partition_oracle"]/n, probes), stage("exact_representative_routing", "centroid_representative_routing", probes < cfg.partitions, totals["exact_representative_routing"]/n, probes), stage("approximate_representative_routing", "deterministic_last_representative_perturbation", probes < cfg.partitions, totals["approximate_representative_routing"]/n, probes), stage("exact_partition_local", "centroid_selected_partition_exhaustive", probes < cfg.partitions, totals["exact_partition_local"]/n, probes), hnswStage, stage("end_to_end_distributed_simulation", "approximate_router_then_local_exhaustive_dedupe", probes < cfg.partitions, totals["end_to_end_distributed_simulation"]/n, probes)}
	selected := make([]stageResult, 0, len(allStages))
	for _, s := range allStages {
		if s.Enabled {
			selected = append(selected, s)
		}
	}
	metrics := metricsV1{MeasurementStatus: "simulation_not_measured", Balance: 1, MaxPartitionSize: (len(v) + cfg.partitions - 1) / cfg.partitions, ReplicationFactor: 1, UnassignedOverlapBudget: overlap, RoutedPartitionRecall: totals["end_to_end_distributed_simulation"] / n, SelectedPartitions: probes}
	r := runResult{SchemaVersion: schemaVersion, ResultKind: "simulation_only", ProductionEvidence: false, Command: cfg.command, BaseSHA: cfg.baseSHA, HeadSHA: cfg.headSHA, GoVersion: runtime.Version(), Hardware: runtime.GOARCH + "/" + runtime.GOOS, Dataset: m, Partitions: cfg.partitions, Overlap: overlap, Probes: probes, TopK: cfg.topK, RecallTarget: cfg.recallTarget, Seed: cfg.seed, Warmup: 0, Samples: len(q), TimedBoundary: "untimed M0 attribution: in-memory oracles plus temporary local TreeDB column_graph build/search; excludes network, RPC, coordinator, and Raft", Stages: selected, Metrics: metrics}
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
	if r.SchemaVersion != schemaVersion || r.ResultKind != "simulation_only" || r.ProductionEvidence || len(r.Stages) == 0 {
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
		if s.Name == "treedb_partition_local_hnsw" {
			if s.RouteKind != string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1) ||
				s.Searches == 0 ||
				s.ExecutedSearches+s.CachedSearches != s.Searches ||
				s.SearchRouteHNSWSearchPack != s.Searches ||
				s.HNSWSearchPackActive != s.Searches ||
				s.HNSWSearchPackFallbacks != 0 {
				return fmt.Errorf("TreeDB HNSW stage lacks exact search-pack route evidence: %+v", s)
			}
		}
	}
	if len(r.BaseSHA) < 7 || len(r.HeadSHA) < 7 {
		return errors.New("missing git provenance")
	}
	if r.Metrics.MeasurementStatus != "simulation_not_measured" {
		return errors.New("unknown metric measurement status")
	}
	for _, x := range []float64{r.Metrics.BytesPerVector, r.Metrics.Balance, r.Metrics.ReplicationFactor, r.Metrics.UnassignedOverlapBudget, r.Metrics.RoutedPartitionRecall, r.Metrics.QPS, r.Metrics.RecallAt1, r.Metrics.RecallAt10, r.Metrics.RecallAt100, r.Metrics.BytesPerOp, r.Metrics.AllocsPerOp} {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return errors.New("non-finite metric")
		}
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
	for _, s := range r.Stages {
		if s.Name == "end_to_end_distributed_simulation" && s.Available {
			stageSummary = fmt.Sprintf("recall@%d: %.4f", r.TopK, s.RecallAtK)
		}
		if s.Name == "treedb_partition_local_hnsw" && s.Available {
			hnswSummary = fmt.Sprintf("recall@%d: %.4f; route=%s; logical_searches=%d; executed=%d; cached=%d; fallbacks=%d", r.TopK, s.RecallAtK, s.RouteKind, s.Searches, s.ExecutedSearches, s.CachedSearches, s.HNSWSearchPackFallbacks)
		}
	}
	md := fmt.Sprintf("# TreeDB vector partition M0 simulation\n\n**Simulation only; not production Raft evidence.**\n\n- fixture: `%s` (%s)\n- probes: %d/%d\n- overlap budget: %.6g\n- TreeDB partition-local HNSW: %s\n- end-to-end simulation: %s\n- timed boundary: %s\n", r.Dataset.Fixture, r.Dataset.Checksum, r.Probes, r.Partitions, r.Overlap, hnswSummary, stageSummary, r.TimedBoundary)
	return os.WriteFile(filepath.Join(out, name+".md"), []byte(md), 0644)
}
func artifactBasename(r runResult) string {
	return fmt.Sprintf("simulation_p%d_o%016x_k%d", r.Probes, math.Float64bits(r.Overlap), r.TopK)
}
func fixtureChecksum(vectors, queries, dims int, seed int64) string {
	m := fixtureManifest{Vectors: vectors, Queries: queries, Dimensions: dims, Seed: seed}
	v, q := deterministicFixture(m)
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
	for _, query := range q {
		for _, n := range exactTopK(v, query, 10) {
			_, _ = h.Write([]byte(n.ID))
			binary.LittleEndian.PutUint64(b[:], math.Float64bits(n.Distance))
			_, _ = h.Write(b[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
