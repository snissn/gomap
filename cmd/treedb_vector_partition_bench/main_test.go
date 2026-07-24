package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func fixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "testdata", "vector_partition_10k")
}

func artifactBasenameForFixture(t *testing.T, dataset string, result runResult) string {
	t.Helper()
	fixture, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	result.Dataset = fixture
	if len(result.Stages) == 0 {
		result.Stages = stageResultsForSet("all")
	}
	return artifactBasename(result)
}

func stageResultsForSet(raw string) []stageResult {
	selected := stageSet(raw)
	stages := make([]stageResult, 0, len(selected))
	for _, name := range knownStages {
		if selected[name] {
			stages = append(stages, stageResult{Name: name})
		}
	}
	return stages
}

func runWithHermeticProvenance(t *testing.T, args []string, stdout io.Writer) error {
	t.Helper()
	t.Setenv("BASE_SHA", strings.Repeat("a", 40))
	t.Setenv("GITHUB_SHA", strings.Repeat("b", 40))
	t.Setenv("GITHUB_EVENT_PATH", "")
	return run(args, stdout)
}

func writeFixtureForTest(t *testing.T, vectors, queries, dims int) string {
	t.Helper()
	m, _, _ := smallFixtureForTest(vectors, queries, dims)
	m.Checksum = fixtureChecksum(vectors, queries, dims, m.Seed)
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func smallFixtureForTest(vectors, queries, dims int) (fixtureManifest, [][]float64, [][]float64) {
	m := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "small",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       vectors,
		Queries:       queries,
		Dimensions:    dims,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	v, q := deterministicFixture(m)
	return m, v, q
}

func TestFixtureArithmeticUsesExplicitFMA(t *testing.T) {
	a := 1 + math.Ldexp(1, -27)
	b := 1 - math.Ldexp(1, -27)
	if got, want := math.FMA(a, b, -1), -math.Ldexp(1, -54); got != want {
		t.Fatalf("%s result=%x want %x", fixtureArithmetic, math.Float64bits(got), math.Float64bits(want))
	}
	if fixtureGenerator != "treedb_vector_partition_fixture_v2" || fixtureArithmetic != "ieee754_binary64_explicit_fma_v1" {
		t.Fatalf("unexpected fixture contract generator=%q arithmetic=%q", fixtureGenerator, fixtureArithmetic)
	}
	m, vectors, queries := smallFixtureForTest(10, 2, 4)
	m.Checksum = fixtureChecksumFromData(vectors, queries)
	if err := validateFixture(m); err != nil {
		t.Fatalf("explicit arithmetic fixture rejected: %v", err)
	}
	m.Arithmetic = "implicit_multiply_add"
	if err := validateFixture(m); err == nil {
		t.Fatal("fixture with unspecified contraction behavior accepted")
	}
}

func TestPartitionStageWritesValidatedDeterministicArtifact(t *testing.T) {
	dataset := writeFixtureForTest(t, 16, 2, 4)
	args := []string{"-dataset", dataset, "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-stage", "partition", "-partition-repetitions", "1", "-partition-pivots", "2", "-partition-max-leaf-bucket", "4", "-partition-degree", "2"}
	var first bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &first); err != nil {
		t.Fatal(err)
	}
	var report partitionRun
	if err := json.Unmarshal(first.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.ResultKind != "offline_partition_builder" || report.Metrics.MaxPartitionSize > report.Metrics.Cap || report.ArtifactBytes <= 0 || report.ReportBytes <= 0 || report.FinalBytes != report.ArtifactBytes+report.ReportBytes {
		t.Fatalf("report=%+v", report)
	}
	raw, err := os.ReadFile(report.ArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := vectorpartition.DecodeArtifact(raw, len(raw))
	if err != nil || artifact.Source != report.Source || artifact.Metrics != report.Metrics {
		t.Fatalf("artifact does not match compact report: artifact=%+v err=%v", artifact, err)
	}
	if bytes.Contains(first.Bytes(), []byte("\"assignment\"")) || bytes.Contains(first.Bytes(), []byte("\"neighbors\"")) {
		t.Fatalf("stdout contains duplicate artifact payload: %s", first.Bytes())
	}
	if _, err := os.Stat(report.ArtifactPath); err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	args[3] = t.TempDir()
	if err := runWithHermeticProvenance(t, args, &second); err != nil {
		t.Fatal(err)
	}
	var again partitionRun
	if err := json.Unmarshal(second.Bytes(), &again); err != nil {
		t.Fatal(err)
	}
	if report.ArtifactSHA256 != again.ArtifactSHA256 {
		t.Fatalf("digest changed: %s %s", report.ArtifactSHA256, again.ArtifactSHA256)
	}
}

func TestRouterStageUsesPersistedExactAndNativeHNSWPaths(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("durable M1 lifecycle publication is unsupported; router codec and model coverage remain platform-neutral")
	}
	dataset := writeFixtureForTest(t, 32, 4, 8)
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", t.TempDir(),
		"-partitions", "4",
		"-probes", "2",
		"-stage", "router",
		"-router-representatives", "2",
		"-router-leaf-size", "1",
		"-router-max-bytes", "1",
	}, io.Discard); err == nil || !strings.Contains(err.Error(), "estimated bytes") {
		t.Fatalf("persisted router byte cap error=%v", err)
	}
	out := t.TempDir()
	var stdout bytes.Buffer
	args := []string{
		"-dataset", dataset,
		"-out", out,
		"-partitions", "4",
		"-probes", "2",
		"-stage", "router",
		"-router-representatives", "2",
		"-router-leaf-size", "1",
		"-router-candidates", "8",
	}
	if err := runWithHermeticProvenance(t, args, &stdout); err != nil {
		t.Fatal(err)
	}
	var result runResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if err := validateResult(result); err != nil {
		t.Fatal(err)
	}
	if result.ResultKind != "router_local_path_evidence" ||
		result.ProductionEvidence ||
		result.Metrics.MeasurementStatus != "router_local_production_path_no_raft" ||
		result.Metrics.RepresentativeCount != 8 ||
		result.Metrics.MinRepresentatives != 2 ||
		result.Metrics.MaxRepresentatives != 2 ||
		result.Metrics.RouterBytes <= 0 ||
		result.Metrics.MappedBytes+result.Metrics.HeapCopyBytes <= 0 {
		t.Fatalf("router result=%+v", result)
	}
	var exact, approximate stageResult
	for _, stage := range result.Stages {
		switch stage.Name {
		case "exact_representative_routing":
			exact = stage
		case "approximate_representative_routing":
			approximate = stage
		}
	}
	if exact.Searches != 4 || approximate.Searches != 4 ||
		exact.CandidateBudget != 8 || approximate.CandidateBudget != 8 ||
		exact.Candidates != 32 || approximate.Candidates != 32 ||
		exact.RecallAtK != approximate.RecallAtK ||
		result.Metrics.HNSWRecallLoss != 0 ||
		exact.P50Nanos == 0 || approximate.P50Nanos == 0 ||
		exact.AllocsPerOp == 0 || approximate.AllocsPerOp == 0 {
		t.Fatalf("exact=%+v approximate=%+v metrics=%+v", exact, approximate, result.Metrics)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("router artifacts=%d want 2", len(entries))
	}
	var markdownPath string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".md") {
			markdownPath = filepath.Join(out, entry.Name())
		}
	}
	markdown, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(markdown, []byte("M4 local router evidence")) ||
		!bytes.Contains(markdown, []byte("not production Raft or M8 acceptance evidence")) {
		t.Fatalf("router Markdown has incorrect evidence boundary:\n%s", markdown)
	}
}

func TestRouterStageFailsBeforePartialEvidenceWithoutNamespacePersistence(t *testing.T) {
	out := filepath.Join(t.TempDir(), "evidence")
	args := []string{
		"-dataset", "must-not-be-read",
		"-out", out,
		"-partitions", "4",
		"-probes", "1",
		"-stage", "router",
	}
	var stdout bytes.Buffer
	err := runWithRuntimeCapabilities(args, &stdout, benchmarkRuntimeCapabilities{})
	if !errors.Is(err, collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1) {
		t.Fatalf("unsupported M4 router stage err=%v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unsupported M4 router stage emitted stdout: %q", stdout.String())
	}
	if _, statErr := os.Stat(out); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unsupported M4 router stage created partial evidence directory: %v", statErr)
	}
}

func TestPartitionStageEdgeClampPreservesRepetitionCapacity(t *testing.T) {
	cfg, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-probes", "1", "-stage", "partition", "-partition-repetitions", "4", "-partition-degree", "16"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cfg.partition.MaxEdges, 64_000_000; got != want {
		t.Fatalf("MaxEdges=%d want %d; repetition capacity was lost", got, want)
	}
	capacity := cfg.partition.MaxEdges / cfg.partition.Repetitions / cfg.partition.Degree
	if capacity < 250_001 {
		t.Fatalf("large valid partition shape capacity=%d want at least 250001", capacity)
	}
	if int64(maxVectors+1)*int64(cfg.partition.Degree) <= int64(cfg.partition.MaxEdges)/int64(cfg.partition.Repetitions) {
		t.Fatalf("true over-cap shape accepted by edge budget: capacity=%d", capacity)
	}
}

func TestPartitionProvenanceSuffixPreservesFilenameAndRejectsShortInputs(t *testing.T) {
	digest := strings.Repeat("d", 64)
	base := strings.Repeat("a", 40)
	head := strings.Repeat("b", 40)
	got, err := provenanceSuffix(digest, base, head)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.Repeat("d", 12) + "_" + strings.Repeat("a", 12) + "_" + strings.Repeat("b", 12); got != want {
		t.Fatalf("suffix=%q want %q", got, want)
	}
	if _, err := provenanceSuffix("short", base, head); err == nil {
		t.Fatal("short digest accepted")
	}
}
func TestCheckedIn10kFixtureGraphCutBeatsStableHash(t *testing.T) {
	args := []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-probes", "1", "-stage", "partition", "-partition-repetitions", "4", "-partition-pivots", "8", "-partition-max-leaf-bucket", "128", "-partition-degree", "16"}
	var stdout bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &stdout); err != nil {
		t.Fatal(err)
	}
	var report partitionRun
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Dataset.Checksum != "2413ef7c2f65a4b5ce8ecc3846f473fd85d337a87511538f962af7cdf6aec291" || report.Source.Checksum != "6515025f540b955d453de99cf13f1efc002fd91135b2745b722c19e8d736e386" || report.ArtifactSHA256 != "9af8ddca00b42caa04f06f69dcc5991532193a3e95642d723cddaff664a04a11" || report.Metrics.EdgeCut != 5184 || report.Metrics.StableIDHashEdgeCut != 149877 {
		t.Fatalf("frozen 10k regression changed: report=%+v", report)
	}
}
func TestPartitionStageSkipsSimulationOnlyValidation(t *testing.T) {
	args := []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-probes", "16", "-overlap", "1", "-top-k", "10001", "-stages", "treedb_partition_local_hnsw", "-stage", "partition", "-partition-repetitions", "1", "-partition-pivots", "2", "-partition-max-leaf-bucket", "64", "-partition-degree", "4"}
	var stdout bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &stdout); err != nil {
		t.Fatal(err)
	}
	var report partitionRun
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil || report.ArtifactPath == "" {
		t.Fatalf("partition artifact missing: report=%+v err=%v", report, err)
	}
}

func TestPartitionStageIgnoresLargeSimulationOnlyQueryStream(t *testing.T) {
	const vectors = 20_000
	m := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "partition-only-large-query-stream",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       vectors,
		Queries:       maxVectors - vectors,
		Dimensions:    1,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	dataset := t.TempDir()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataset, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-dataset", dataset, "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-stage", "partition", "-partition-repetitions", "1", "-partition-pivots", "2", "-partition-max-leaf-bucket", "2", "-partition-degree", "1"}
	var first bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &first); err != nil {
		t.Fatalf("partition stage charged simulation-only queries: %v", err)
	}
	var report partitionRun
	if err := json.Unmarshal(first.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	artifactRaw, err := os.ReadFile(report.ArtifactPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := vectorpartition.DecodeArtifact(artifactRaw, len(artifactRaw))
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Source != report.Source || artifact.Source.Vectors != vectors || artifact.Source.Dimensions != 1 {
		t.Fatalf("partition artifact lost vector-corpus identity: %+v", artifact.Source)
	}
	args[3] = t.TempDir()
	var second bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &second); err != nil {
		t.Fatal(err)
	}
	var again partitionRun
	if err := json.Unmarshal(second.Bytes(), &again); err != nil {
		t.Fatal(err)
	}
	if report.ArtifactSHA256 != again.ArtifactSHA256 || report.Source != again.Source {
		t.Fatalf("partition-only output changed with irrelevant query stream: first=%+v second=%+v", report, again)
	}
}

func TestPartitionFixtureValidationUsesVectorOnlyCaps(t *testing.T) {
	base := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "partition-vector-boundary",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       maxVectors,
		Queries:       1,
		Dimensions:    1,
		Metric:        "cosine",
		Checksum:      strings.Repeat("0", 64),
	}
	if err := validatePartitionFixtureWithCaps(base, maxVectors, maxFixtureBytes); err != nil {
		t.Fatalf("max vector corpus plus required query rejected in partition mode: %v", err)
	}
	queryHeavy := base
	queryHeavy.Queries = maxVectors * 4
	if err := validatePartitionFixtureWithCaps(queryHeavy, maxVectors, maxFixtureBytes); err != nil {
		t.Fatalf("irrelevant query count affected partition mode: %v", err)
	}
	if err := validateFixtureWithCaps(queryHeavy, maxVectors, maxFixtureBytes); err == nil {
		t.Fatal("simulation accepted query-heavy fixture")
	}
	overCount := base
	overCount.Vectors++
	if err := validatePartitionFixtureWithCaps(overCount, maxVectors, maxFixtureBytes); err == nil {
		t.Fatal("partition mode accepted over-count vector corpus")
	}
	overBytes := base
	overBytes.Vectors = 2
	overBytes.Dimensions = 2
	if err := validatePartitionFixtureWithCaps(overBytes, maxVectors, 8); err == nil {
		t.Fatal("partition mode accepted over-byte vector corpus")
	}
}

func TestFixtureTruthDeterministicAndChecksumStable(t *testing.T) {
	m, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFixture(m); err != nil {
		t.Fatal(err)
	}
	a, aq := deterministicFixture(m)
	if got := fixtureChecksumFromData(a, aq); got != m.Checksum {
		t.Fatalf("checksum=%s want %s", got, m.Checksum)
	}
	b, bq := deterministicFixture(m)
	if len(a) != len(b) || len(aq) != len(bq) || a[0][0] != b[0][0] || aq[0][0] != bq[0][0] {
		t.Fatal("fixture is not deterministic")
	}
}

func TestFixtureChecksumSupportsFewerThanTenVectors(t *testing.T) {
	for vectors := 1; vectors < 10; vectors++ {
		first := fixtureChecksum(vectors, 3, 4, 1)
		second := fixtureChecksum(vectors, 3, 4, 1)
		if first != second || len(first) != 64 {
			t.Fatalf("vectors=%d checksum first=%q second=%q", vectors, first, second)
		}
	}
}

func TestTruthOracleTieOrderingAndAllPartitionParity(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	vectors, queries := deterministicFixture(m)
	_ = vectors
	_ = queries
	got := exactTopK([][]float64{{1, 0}, {1, 0}, {0, 1}}, []float64{1, 0}, 2)
	if got[0].ID != "doc-000000" || got[1].ID != "doc-000001" {
		t.Fatalf("ties not ordered by stable ID: %#v", got[:2])
	}
	cfg := config{partitions: 4, topK: 10, recallTarget: .9, seed: 1, stages: stageSet("exact_global_top_k,partition_oracle")}
	r, err := simulate(cfg, m, vectors, queries, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Stages[1].RecallAtK != 1 {
		t.Fatalf("all partition oracle recall=%f", r.Stages[1].RecallAtK)
	}
	if r.Stages[0].RecallAtK != r.Stages[1].RecallAtK {
		t.Fatal("oracle parity differs")
	}
	for i, query := range queries {
		want := exactTopK(vectors, query, cfg.topK)
		got := partitionTopK(vectors, query, cfg.topK, cfg.partitions, cfg.partitions)
		if !orderedEqual(want, got) {
			t.Fatalf("query %d all-partition ordered ID+distance mismatch:\nwant=%+v\ngot=%+v", i, want, got)
		}
	}
}

func TestEveryLossStageIndependentlyLabeled(t *testing.T) {
	m, vectors, queries := smallFixtureForTest(32, 4, 8)
	hnsw, err := newTreeDBPartitionHNSW(vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hnsw.Close() }()
	for _, name := range knownStages {
		cfg := config{partitions: 4, topK: 4, recallTarget: .9, seed: 1, stages: stageSet(name)}
		if name == "treedb_partition_local_hnsw" {
			cfg.hnsw = hnsw
		}
		r, err := simulate(cfg, m, vectors, queries, 1, .2)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(r.Stages) != 1 || r.Stages[0].Name != name || !r.Stages[0].Enabled || !r.Stages[0].Available {
			t.Fatalf("%s independently reported stages=%+v", name, r.Stages)
		}
		if r.ResultKind != "simulation_only" || r.ProductionEvidence {
			t.Fatalf("%s simulation label=%+v", name, r)
		}
	}
}

func TestSingleEnabledStageIsTheOnlyReportedStage(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	v, q := deterministicFixture(m)
	r, err := simulate(config{partitions: 4, topK: 10, recallTarget: .9, seed: 1, stages: stageSet("partition_oracle")}, m, v, q, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Stages) != 1 || r.Stages[0].Name != "partition_oracle" {
		t.Fatalf("stages=%+v", r.Stages)
	}
}

func TestTreeDBHNSWStageUsesExactSearchPackAndMatchesHighEFLocalTruth(t *testing.T) {
	m, vectors, queries := smallFixtureForTest(48, 6, 8)
	hnsw, err := newTreeDBPartitionHNSW(vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hnsw.Close() }()

	allPartitions := []int{0, 1, 2, 3}
	for queryIndex, query := range queries {
		got, err := hnsw.search(queryIndex, query, allPartitions, 6)
		if err != nil {
			t.Fatalf("query %d: %v", queryIndex, err)
		}
		want := exactTopK(vectors, query, 6)
		if len(got.Neighbors) != len(want) {
			t.Fatalf("query %d neighbors=%d want %d", queryIndex, len(got.Neighbors), len(want))
		}
		for i := range want {
			if got.Neighbors[i].ID != want[i].ID || math.Abs(got.Neighbors[i].Distance-want[i].Distance) > 1e-5 {
				t.Fatalf("query %d rank %d HNSW=%+v exact=%+v", queryIndex, i, got.Neighbors[i], want[i])
			}
		}
		if got.Evidence.SearchRouteHNSWSearchPack != 4 || got.Evidence.HNSWSearchPackActive != 4 || got.Evidence.HNSWSearchPackFallbacks != 0 {
			t.Fatalf("query %d route evidence=%+v", queryIndex, got.Evidence)
		}
	}

	cfg := config{
		partitions:   4,
		topK:         6,
		recallTarget: .9,
		seed:         1,
		stages:       stageSet("treedb_partition_local_hnsw"),
		hnsw:         hnsw,
	}
	first, err := simulate(cfg, m, vectors, queries, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	physicalAfterFirst := hnsw.physicalSearches
	second, err := simulate(cfg, m, vectors, queries, 2, .2)
	if err != nil {
		t.Fatal(err)
	}
	if hnsw.physicalSearches != physicalAfterFirst {
		t.Fatalf("overlap-only row repeated HNSW work: before=%d after=%d", physicalAfterFirst, hnsw.physicalSearches)
	}
	if first.Stages[0].ExecutedSearches == 0 || first.Stages[0].CachedSearches != 0 {
		t.Fatalf("first HNSW row evidence=%+v", first.Stages[0])
	}
	if second.Stages[0].ExecutedSearches != 0 || second.Stages[0].CachedSearches != second.Stages[0].Searches {
		t.Fatalf("cached HNSW row evidence=%+v", second.Stages[0])
	}

	beforeDifferentSet := hnsw.physicalSearches
	if _, err := hnsw.search(0, queries[0], []int{0}, 6); err != nil {
		t.Fatal(err)
	}
	afterFirstSet := hnsw.physicalSearches
	if _, err := hnsw.search(0, queries[0], []int{1}, 6); err != nil {
		t.Fatal(err)
	}
	if afterFirstSet <= beforeDifferentSet || hnsw.physicalSearches <= afterFirstSet {
		t.Fatal("same-sized selected partition sets aliased in HNSW cache")
	}
	dir := hnsw.dir
	if err := hnsw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("temporary HNSW DB was not removed: stat error=%v", err)
	}
}

func TestMalformedCapAndFiniteInputsRejectBeforeSimulation(t *testing.T) {
	for _, raw := range [][]string{{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "0", "-probes", "1"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "5"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-overlap", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-recall-target", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-stages", "unknown"}} {
		if _, err := parseConfig(raw); err == nil {
			t.Fatalf("accepted malformed config %#v", raw)
		}
	}
	bad := fixtureManifest{SchemaVersion: 1, Fixture: "bad", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: maxVectors + 1, Queries: 1, Dimensions: 1, Metric: "cosine", Checksum: strings.Repeat("0", 64)}
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted capped fixture")
	}
	bad.Vectors = 1
	bad.Dimensions = 1
	bad.Checksum = "bad"
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted malformed checksum")
	}
}

func TestRunRejectsTopKAboveFixtureBeforeOracleAllocation(t *testing.T) {
	err := runWithHermeticProvenance(t, []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-top-k", "10001"}, io.Discard)
	if err == nil {
		t.Fatal("accepted top-k above fixture")
	}
}

func TestConfigurableFixtureCapsRejectBeforeAllocation(t *testing.T) {
	cfg, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-vectors", "9999"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := loadFixture(fixturePath(t))
	if err := validateFixtureWithCaps(m, cfg.maxVectors, cfg.maxBytes); err == nil {
		t.Fatal("accepted fixture beyond configured count cap")
	}
	if _, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-fixture-bytes", "7"}); err == nil {
		t.Fatal("accepted unsafe byte cap")
	}
	m.Vectors = 6
	m.Queries = 5
	m.Dimensions = 1
	if err := validateFixtureWithCaps(m, 10, 1024); err == nil || !strings.Contains(err.Error(), "combined") {
		t.Fatalf("combined vector/query count cap error=%v", err)
	}
}

func TestModeledPeakMemoryBudgetEnforcedBeforeGeneration(t *testing.T) {
	const (
		vectors = 32
		queries = 4
		dims    = 8
	)
	dataset := writeFixtureForTest(t, vectors, queries, dims)
	m, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config{
		partitions: 4,
		probes:     []int{1, 4},
		topK:       4,
		stages:     stageSet("all"),
	}
	plan, err := planBenchmarkMemory(cfg, m)
	if err != nil {
		t.Fatal(err)
	}
	rawFixtureBytes := int64(vectors+queries) * dims * 8
	if plan.FixtureResidentBytes <= rawFixtureBytes || plan.HNSWInsertWorkBytes == 0 || plan.HNSWCacheBytes == 0 || plan.ModeledPeakBytes <= plan.FixtureResidentBytes {
		t.Fatalf("incomplete memory plan: %+v raw_fixture=%d", plan, rawFixtureBytes)
	}
	rejectedOut := t.TempDir()
	err = runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", rejectedOut,
		"-partitions", "4",
		"-probes", "1,4",
		"-top-k", "4",
		"-max-fixture-bytes", strconv.FormatInt(plan.ModeledPeakBytes-1, 10),
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "modeled peak") {
		t.Fatalf("below-peak cap error=%v plan=%+v", err, plan)
	}
	entries, readErr := os.ReadDir(rejectedOut)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected memory budget emitted evidence: %+v", entries)
	}

	acceptedOut := t.TempDir()
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", acceptedOut,
		"-partitions", "4",
		"-probes", "1,4",
		"-top-k", "4",
		"-max-fixture-bytes", strconv.FormatInt(plan.ModeledPeakBytes, 10),
	}, io.Discard); err != nil {
		t.Fatalf("exact modeled peak cap rejected: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(acceptedOut, artifactBasenameForFixture(t, dataset, runResult{Seed: 1, Partitions: 4, Probes: 4, TopK: 4})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.MemoryBudgetBytes != plan.ModeledPeakBytes || result.ModeledPeakBytes != plan.ModeledPeakBytes || result.MemoryBudgetScope != memoryBudgetScope {
		t.Fatalf("memory evidence=%+v plan=%+v", result, plan)
	}
}

func TestBenchmarkWorkBudgetBoundaryAndCanonicalShape(t *testing.T) {
	boundary := fixtureManifest{Vectors: 10, Queries: 10}
	cfg := config{
		probes:   []int{1},
		overlaps: []float64{0},
		stages:   stageSet("exact_global_top_k"),
	}
	plan, err := validateBenchmarkWork(cfg, boundary, 200)
	if err != nil {
		t.Fatalf("exact work-budget boundary rejected: %v", err)
	}
	if plan.VectorQueryPairs != 100 || plan.CorpusPasses != 2 || plan.VectorQueryVisits != 200 {
		t.Fatalf("boundary work plan=%+v", plan)
	}
	cfg.overlaps = []float64{0, .2}
	if _, err := validateBenchmarkWork(cfg, boundary, 200); err == nil {
		t.Fatal("accepted work plan above exact boundary")
	}

	canonical, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	canonicalPlan, err := validateBenchmarkWork(config{
		probes:   []int{1, 2, 4},
		overlaps: []float64{0, .2},
		stages:   stageSet("all"),
	}, canonical, maxBenchmarkWorkUnits)
	if err != nil {
		t.Fatalf("canonical work plan rejected: %v", err)
	}
	if canonicalPlan.CorpusPasses != 67 || canonicalPlan.VectorQueryVisits != 85_760_000 {
		t.Fatalf("canonical work plan=%+v", canonicalPlan)
	}
}

func TestPathologicalBenchmarkWorkRejectsBeforeGenerationOrEvidence(t *testing.T) {
	dataset := t.TempDir()
	fixture := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "pathological-work",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       500_000,
		Queries:       500_000,
		Dimensions:    1,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataset, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	err = runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-out", out,
		"-partitions", "1",
		"-probes", "1",
		"-overlap", "0",
		"-top-k", "1",
		"-stages", "exact_global_top_k",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "modeled benchmark work") {
		t.Fatalf("pathological work error=%v", err)
	}
	entries, readErr := os.ReadDir(out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected work budget emitted evidence: %+v", entries)
	}
}

func TestRunRejectsSeedThatDoesNotMatchFixtureBeforeEvidence(t *testing.T) {
	out := t.TempDir()
	err := runWithHermeticProvenance(t, []string{
		"-dataset", fixturePath(t),
		"-out", out,
		"-partitions", "4",
		"-probes", "1",
		"-top-k", "10",
		"-seed", "2",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "does not match fixture seed") {
		t.Fatalf("seed mismatch error=%v", err)
	}
	entries, readErr := os.ReadDir(out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("seed mismatch emitted evidence: %+v", entries)
	}
}

func TestManifestRejectsTrailingJSON(t *testing.T) {
	d := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(fixturePath(t), "fixture_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), append(raw, []byte("{}")...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixture(d); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestManifestReadCapRejectsOversizedFile(t *testing.T) {
	d := t.TempDir()
	raw := bytes.Repeat([]byte(" "), int(maxManifestBytes)+1)
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixture(d); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("oversized manifest error=%v", err)
	}
}

func TestFixtureChecksumMismatchFailsBeforeEvidence(t *testing.T) {
	d := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(fixturePath(t), "fixture_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m fixtureManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m.Checksum = strings.Repeat("0", 64)
	raw, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	err = runWithHermeticProvenance(t, []string{"-dataset", d, "-out", out, "-partitions", "4", "-probes", "1", "-stages", "treedb_partition_local_hnsw"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum mismatch error=%v", err)
	}
	entries, readErr := os.ReadDir(out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("checksum mismatch emitted evidence: %+v", entries)
	}
}

func TestProvenanceAutomaticEnvironmentAndInvalidSHA(t *testing.T) {
	t.Run("automatic", func(t *testing.T) {
		t.Setenv("BASE_SHA", "")
		t.Setenv("GITHUB_SHA", "")
		t.Setenv("GITHUB_EVENT_PATH", "")
		wantHead, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			t.Skip("local git provenance unavailable")
		}
		if _, err := exec.Command("git", "merge-base", "HEAD", "origin/main").Output(); err != nil {
			t.Skip("origin/main merge-base unavailable")
		}
		base, head, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if head != strings.TrimSpace(string(wantHead)) || len(base) != 40 {
			t.Fatalf("automatic provenance base=%q head=%q want_head=%q", base, head, strings.TrimSpace(string(wantHead)))
		}
	})
	t.Run("environment", func(t *testing.T) {
		base := strings.Repeat("a", 40)
		head := strings.Repeat("b", 40)
		t.Setenv("BASE_SHA", base)
		t.Setenv("GITHUB_SHA", head)
		t.Setenv("GITHUB_EVENT_PATH", "")
		gotBase, gotHead, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if gotBase != base || gotHead != head {
			t.Fatalf("environment provenance base=%q head=%q", gotBase, gotHead)
		}
	})
	t.Run("pull_request_event", func(t *testing.T) {
		base := strings.Repeat("c", 40)
		head := strings.Repeat("e", 40)
		path := filepath.Join(t.TempDir(), "event.json")
		raw := `{"pull_request":{"base":{"sha":"` + base + `"},"head":{"sha":"` + head + `"}},"ignored_by_typed_projection":true}`
		if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BASE_SHA", strings.Repeat("a", 40))
		t.Setenv("GITHUB_SHA", strings.Repeat("d", 40))
		t.Setenv("GITHUB_EVENT_PATH", path)
		gotBase, gotHead, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if gotBase != base || gotHead != head {
			t.Fatalf("event base/head=%q/%q want %q/%q", gotBase, gotHead, base, head)
		}
	})
	for _, tc := range []struct {
		name string
		base string
		head string
	}{
		{name: "base", base: "invalid", head: strings.Repeat("e", 40)},
		{name: "head", base: strings.Repeat("c", 40), head: "invalid"},
	} {
		t.Run("event_invalid_"+tc.name+"_sha", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "event.json")
			raw := `{"pull_request":{"base":{"sha":"` + tc.base + `"},"head":{"sha":"` + tc.head + `"}}}`
			if err := os.WriteFile(path, []byte(raw), 0644); err != nil {
				t.Fatal(err)
			}
			t.Setenv("BASE_SHA", "")
			t.Setenv("GITHUB_SHA", strings.Repeat("d", 40))
			t.Setenv("GITHUB_EVENT_PATH", path)
			if _, _, err := provenance(); err == nil {
				t.Fatalf("accepted invalid event %s SHA", tc.name)
			}
		})
	}
	for _, tc := range []struct {
		name string
		base string
		head string
	}{
		{name: "base", base: "not-a-sha", head: strings.Repeat("b", 40)},
		{name: "head", base: strings.Repeat("a", 40), head: strings.Repeat("g", 40)},
	} {
		t.Run("invalid_"+tc.name, func(t *testing.T) {
			t.Setenv("BASE_SHA", tc.base)
			t.Setenv("GITHUB_SHA", tc.head)
			t.Setenv("GITHUB_EVENT_PATH", "")
			if _, _, err := provenance(); err == nil {
				t.Fatal("accepted invalid SHA provenance")
			}
		})
	}
	t.Run("non_pull_request_event_preserves_explicit_pair", func(t *testing.T) {
		base := strings.Repeat("a", 40)
		head := strings.Repeat("b", 40)
		path := filepath.Join(t.TempDir(), "event.json")
		if err := os.WriteFile(path, []byte(`{"event":"workflow_dispatch"}`), 0644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("BASE_SHA", base)
		t.Setenv("GITHUB_SHA", head)
		t.Setenv("GITHUB_EVENT_PATH", path)
		gotBase, gotHead, err := provenance()
		if err != nil {
			t.Fatal(err)
		}
		if gotBase != base || gotHead != head {
			t.Fatalf("non-PR event changed explicit provenance base=%q head=%q", gotBase, gotHead)
		}
	})
}

func TestGitHubEventPayloadIsBoundedAndSingleValue(t *testing.T) {
	oversized := filepath.Join(t.TempDir(), "oversized.json")
	if err := os.WriteFile(oversized, bytes.Repeat([]byte(" "), int(maxGitHubEventBytes)+1), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := pullRequestSHAsFromEvent(oversized); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("oversized event error=%v", err)
	}
	trailing := filepath.Join(t.TempDir(), "trailing.json")
	if err := os.WriteFile(trailing, []byte(`{} {}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := pullRequestSHAsFromEvent(trailing); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing event error=%v", err)
	}
}

func TestCloseOverlapValuesHaveCollisionFreeArtifacts(t *testing.T) {
	a := 0.2
	b := math.Nextafter(a, 1)
	base := runResult{
		Dataset:    fixtureManifest{Checksum: strings.Repeat("1", 64)},
		Seed:       1,
		Partitions: 4,
		Probes:     1,
		Overlap:    a,
		TopK:       10,
		Stages:     stageResultsForSet("all"),
	}
	adjacentOverlap := base
	adjacentOverlap.Overlap = b
	if artifactBasename(base) == artifactBasename(adjacentOverlap) {
		t.Fatal("adjacent float64 overlap values collide")
	}
	differentSeed := base
	differentSeed.Seed = 2
	if artifactBasename(base) == artifactBasename(differentSeed) {
		t.Fatal("different fixture seeds collide")
	}
	out := t.TempDir()
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", fixturePath(t),
		"-partitions", "4",
		"-probes", "1",
		"-overlap", "0.2," + strconv.FormatFloat(b, 'g', -1, 64),
		"-top-k", "10",
		"-stages", "exact_global_top_k",
		"-format", "json",
		"-out", out,
	}, io.Discard); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("close overlap artifacts=%d want 4", len(entries))
	}
}

func TestArtifactBasenamePreventsDatasetAndPartitionOverwrite(t *testing.T) {
	firstDataset := writeFixtureForTest(t, 8, 2, 4)
	secondDataset := writeFixtureForTest(t, 9, 2, 4)
	out := t.TempDir()
	for _, testRun := range []struct {
		dataset    string
		partitions int
	}{
		{dataset: firstDataset, partitions: 4},
		{dataset: secondDataset, partitions: 4},
		{dataset: firstDataset, partitions: 8},
	} {
		if err := runWithHermeticProvenance(t, []string{
			"-dataset", testRun.dataset,
			"-partitions", strconv.Itoa(testRun.partitions),
			"-probes", "1",
			"-overlap", "0",
			"-top-k", "2",
			"-stages", "exact_global_top_k",
			"-format", "json",
			"-out", out,
		}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("dataset/partition identity artifacts=%d want 6 without overwrite", len(entries))
	}
	first4 := artifactBasenameForFixture(t, firstDataset, runResult{Seed: 1, Partitions: 4, Probes: 1, TopK: 2})
	second4 := artifactBasenameForFixture(t, secondDataset, runResult{Seed: 1, Partitions: 4, Probes: 1, TopK: 2})
	first8 := artifactBasenameForFixture(t, firstDataset, runResult{Seed: 1, Partitions: 8, Probes: 1, TopK: 2})
	if first4 == second4 || first4 == first8 || second4 == first8 {
		t.Fatalf("artifact identities collide first4=%q second4=%q first8=%q", first4, second4, first8)
	}
}

func TestArtifactBasenamePreventsStageSetOverwrite(t *testing.T) {
	if got, want := artifactStageSetChecksum([]stageResult{{Name: "partition_oracle"}, {Name: "exact_global_top_k"}}), artifactStageSetChecksum([]stageResult{{Name: "exact_global_top_k"}, {Name: "partition_oracle"}}); got != want {
		t.Fatalf("stage-set checksum depends on input order: got=%s want=%s", got, want)
	}
	dataset := writeFixtureForTest(t, 8, 2, 4)
	out := t.TempDir()
	stageSets := []string{"exact_global_top_k", "partition_oracle"}
	for _, stages := range stageSets {
		if err := runWithHermeticProvenance(t, []string{
			"-dataset", dataset,
			"-partitions", "4",
			"-probes", "1",
			"-overlap", "0",
			"-top-k", "2",
			"-stages", stages,
			"-format", "json",
			"-out", out,
		}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("stage-set identity artifacts=%d want 4 without overwrite", len(entries))
	}
	first := artifactBasenameForFixture(t, dataset, runResult{
		Seed:       1,
		Partitions: 4,
		Probes:     1,
		TopK:       2,
		Stages:     stageResultsForSet(stageSets[0]),
	})
	second := artifactBasenameForFixture(t, dataset, runResult{
		Seed:       1,
		Partitions: 4,
		Probes:     1,
		TopK:       2,
		Stages:     stageResultsForSet(stageSets[1]),
	})
	if first == second {
		t.Fatalf("stage-set identities collide: %q", first)
	}
	for _, base := range []string{first, second} {
		for _, ext := range []string{".json", ".md"} {
			if _, err := os.Stat(filepath.Join(out, base+ext)); err != nil {
				t.Fatalf("missing stage-set artifact %s%s: %v", base, ext, err)
			}
		}
	}
}

func TestRunWithExplicitProvenanceOutsideGitCheckout(t *testing.T) {
	dataset := writeFixtureForTest(t, 9, 3, 4)
	out := t.TempDir()
	t.Chdir(t.TempDir())
	if err := runWithHermeticProvenance(t, []string{
		"-dataset", dataset,
		"-partitions", "3",
		"-probes", "3",
		"-overlap", "0",
		"-top-k", "9",
		"-stages", "exact_global_top_k",
		"-format", "json",
		"-out", out,
	}, io.Discard); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(out, artifactBasenameForFixture(t, dataset, runResult{Seed: 1, Partitions: 3, Probes: 3, TopK: 9, Stages: stageResultsForSet("exact_global_top_k")})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.BaseSHA != strings.Repeat("a", 40) || result.HeadSHA != strings.Repeat("b", 40) {
		t.Fatalf("explicit provenance not preserved: base=%q head=%q", result.BaseSHA, result.HeadSHA)
	}
}

func TestCanonicalRunWritesJSONAndMarkdown(t *testing.T) {
	out := t.TempDir()
	var stdout bytes.Buffer
	if err := runWithHermeticProvenance(t, []string{"-dataset", fixturePath(t), "-partitions", "4", "-probes", "1,2,4", "-overlap", "0,0.20", "-top-k", "10", "-recall-target", "0.90", "-seed", "1", "-format", "json", "-out", out}, &stdout); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 12 {
		t.Fatalf("artifacts=%d want 12", len(entries))
	}
	raw, err := os.ReadFile(filepath.Join(out, artifactBasenameForFixture(t, fixturePath(t), runResult{Seed: 1, Partitions: 4, Probes: 4, Overlap: .2, TopK: 10})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command[0] != "treedb_vector_partition_bench" || result.Metrics.MeasurementStatus != "simulation_not_measured" || result.Metrics.SelectedPartitions != 4 {
		t.Fatalf("artifact metadata=%+v", result)
	}
	var hnsw stageResult
	for _, stage := range result.Stages {
		if stage.Name == "treedb_partition_local_hnsw" {
			hnsw = stage
		}
	}
	if !hnsw.Available || hnsw.RouteKind != string(collections.VectorIndexSearchRouteExactHNSWSearchPackV1) || hnsw.SearchRouteHNSWSearchPack == 0 || hnsw.HNSWSearchPackFallbacks != 0 {
		t.Fatalf("HNSW stage evidence=%+v", hnsw)
	}
	for _, field := range []string{`"executed_searches": 0`, `"hnsw_search_pack_fallbacks": 0`} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("artifact omits explicit zero HNSW evidence field %s", field)
		}
	}
	md, err := os.ReadFile(filepath.Join(out, artifactBasenameForFixture(t, fixturePath(t), runResult{Seed: 1, Partitions: 4, Probes: 4, Overlap: .2, TopK: 10})+".md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "Simulation only; not production Raft evidence.") {
		t.Fatal("missing simulation disclaimer")
	}
	if !strings.Contains(string(md), "exact_hnsw_search_pack_v1") || !strings.Contains(string(md), "cached=") {
		t.Fatal("missing separately attributed HNSW route/cache evidence")
	}
}

func TestResultDecoderRejectsUnknownVersionAndTrailingValue(t *testing.T) {
	r := runResult{SchemaVersion: 99, ResultKind: "simulation_only", Stages: []stageResult{{Method: "x"}}, Metrics: metricsV1{MeasurementStatus: "simulation_not_measured"}}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeResult(b); err == nil {
		t.Fatal("accepted unknown schema")
	}
	r.SchemaVersion = 1
	b, err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeResult(append(b, []byte("{}")...)); err == nil {
		t.Fatal("accepted trailing result")
	}
}

func TestValidateResultRejectsNonFiniteMetrics(t *testing.T) {
	r := runResult{SchemaVersion: 1, ResultKind: "simulation_only", Stages: []stageResult{{RecallAtK: math.NaN()}}}
	if err := validateResult(r); err == nil {
		t.Fatal("accepted nonfinite metric")
	}
}

func TestHNSWEvidenceJSONIncludesExplicitZeroCounters(t *testing.T) {
	raw, err := json.Marshal(stageResult{
		Name:      "treedb_partition_local_hnsw",
		Method:    "test",
		Enabled:   true,
		Available: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"searches":0`,
		`"executed_searches":0`,
		`"cached_searches":0`,
		`"search_route_hnsw_search_pack":0`,
		`"hnsw_search_pack_active":0`,
		`"hnsw_search_pack_fallbacks":0`,
	} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("explicit zero field %s absent from %s", field, raw)
		}
	}
}

func TestValidateResultRequiresExactSHASeedAndMemoryEvidence(t *testing.T) {
	r := runResult{
		SchemaVersion:     schemaVersion,
		ResultKind:        "simulation_only",
		BaseSHA:           strings.Repeat("a", 40),
		HeadSHA:           strings.Repeat("b", 40),
		Dataset:           fixtureManifest{Seed: 1, Checksum: strings.Repeat("c", 64)},
		Seed:              1,
		MemoryBudgetBytes: 1024,
		ModeledPeakBytes:  512,
		MemoryBudgetScope: memoryBudgetScope,
		Stages:            []stageResult{{Name: "exact_global_top_k", Method: "test", Enabled: true, Available: true}},
		Metrics:           metricsV1{MeasurementStatus: "simulation_not_measured"},
	}
	if err := validateResult(r); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	for _, mutate := range []func(*runResult){
		func(result *runResult) { result.BaseSHA = "abcdef0" },
		func(result *runResult) { result.HeadSHA = strings.Repeat("g", 40) },
		func(result *runResult) { result.Dataset.Checksum = "not-a-checksum" },
		func(result *runResult) { result.Seed++ },
		func(result *runResult) { result.ModeledPeakBytes = result.MemoryBudgetBytes + 1 },
	} {
		bad := r
		mutate(&bad)
		if err := validateResult(bad); err == nil {
			t.Fatalf("accepted invalid result: %+v", bad)
		}
	}
}
