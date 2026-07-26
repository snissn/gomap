package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
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

func TestGenerateFixtureWritesValidatedDeterministicManifestV1(t *testing.T) {
	out := filepath.Join(t.TempDir(), "fixture")
	var stdout bytes.Buffer
	if err := run([]string{
		"generate-fixture",
		"-out", out,
		"-vectors", "32",
		"-queries", "2",
		"-dimensions", "8",
		"-seed", "7",
	}, &stdout); err != nil {
		t.Fatal(err)
	}
	manifest, err := loadFixture(out)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Vectors != 32 || manifest.Queries != 2 || manifest.Dimensions != 8 || manifest.Seed != 7 {
		t.Fatalf("manifest=%+v", manifest)
	}
	vectors, queries := deterministicFixture(manifest)
	if got := fixtureChecksumFromData(vectors, queries); got != manifest.Checksum {
		t.Fatalf("checksum=%s want %s", manifest.Checksum, got)
	}
	if err := validateM3FixtureWithCaps(manifest, maxVectors, maxFixtureBytes); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), filepath.Join(out, "fixture_manifest.json")) {
		t.Fatalf("stdout=%q", stdout.String())
	}
	if err := run([]string{"generate-fixture", "-out", out, "-vectors", "32"}, io.Discard); err == nil {
		t.Fatal("fixture generator overwrote an existing manifest")
	}
}

func TestM3FixtureAllowsMillionVectorsPlusBoundedQueriesV1(t *testing.T) {
	manifest := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "million",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       1_000_000,
		Queries:       1,
		Dimensions:    16,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	if err := validateM3FixtureWithCaps(manifest, maxVectors, maxFixtureBytes); err != nil {
		t.Fatalf("declared 1M M3 fixture rejected: %v", err)
	}
	if err := validateFixtureWithCaps(manifest, maxVectors, maxFixtureBytes); err == nil {
		t.Fatal("simulation combined-count cap unexpectedly accepted 1M plus a query")
	}
}

func TestM6PreflightAllowsBoundedMillionVectorEvidenceV1(t *testing.T) {
	manifest := fixtureManifest{
		SchemaVersion: schemaVersion,
		Fixture:       "million-m6",
		Generator:     fixtureGenerator,
		Arithmetic:    fixtureArithmetic,
		Vectors:       1_000_000,
		Queries:       32,
		Dimensions:    16,
		Metric:        "cosine",
		Seed:          1,
		Checksum:      strings.Repeat("0", 64),
	}
	if err := validateM3FixtureWithCaps(manifest, maxVectors, maxFixtureBytes); err != nil {
		t.Fatalf("declared 1M M6 fixture rejected: %v", err)
	}
	cfg := config{
		stage: m6CoordinatorStageV1, partitions: 16, topK: 10,
		probes: []int{16}, overlaps: []float64{0},
		stages:           stageSet(m6CoordinatorAttributionStageV1),
		routerConfig:     vectorpartition.DefaultRouterConfigV1(),
		sourceHNSWDegree: 4,
	}
	work, err := validateBenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits)
	if err != nil {
		t.Fatalf("bounded 1M M6 work rejected: %v", err)
	}
	if work.CorpusPasses != 5 || work.VectorQueryVisits != 160_000_000 {
		t.Fatalf("1M M6 work plan=%+v", work)
	}
	memory, err := planBenchmarkMemory(cfg, manifest)
	if err != nil {
		t.Fatalf("bounded 1M M6 memory rejected: %v", err)
	}
	if memory.RouterBuildWorkBytes == 0 ||
		memory.ModeledPeakBytes <= memory.FixtureResidentBytes ||
		memory.ModeledPeakBytes > maxFixtureBytes {
		t.Fatalf("1M M6 memory plan=%+v cap=%d", memory, maxFixtureBytes)
	}
}

func TestM3SourceLoadBoundsColumnGraphPublicationsV1(t *testing.T) {
	const acceptanceRows = 1_000_000
	publications := (acceptanceRows + m3SourceInsertBatchRows - 1) / m3SourceInsertBatchRows
	if publications != 123 {
		t.Fatalf("1M-row M3 source publications=%d want 123", publications)
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
		result.Metrics.SourceHNSWDegree != partitionHNSWDegree ||
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
		!result.Metrics.CoarseningMeasured ||
		!result.Metrics.ApproximateMeasured ||
		!result.Metrics.HNSWLossMeasured ||
		result.Metrics.HNSWRecallLoss != 0 ||
		exact.P50Nanos == 0 || approximate.P50Nanos == 0 ||
		exact.AllocsPerOp == 0 || approximate.AllocsPerOp == 0 {
		t.Fatalf("exact=%+v approximate=%+v metrics=%+v", exact, approximate, result.Metrics)
	}
	var exactOnlyOut bytes.Buffer
	exactOnlyArgs := append(append([]string(nil), args...), "-stages", "exact_representative_routing")
	exactOnlyArgs[3] = t.TempDir()
	if err := runWithHermeticProvenance(t, exactOnlyArgs, &exactOnlyOut); err != nil {
		t.Fatal(err)
	}
	var exactOnly runResult
	if err := json.Unmarshal(exactOnlyOut.Bytes(), &exactOnly); err != nil {
		t.Fatal(err)
	}
	if !exactOnly.Metrics.CoarseningMeasured ||
		exactOnly.Metrics.ApproximateMeasured ||
		exactOnly.Metrics.HNSWLossMeasured ||
		exactOnly.Metrics.ApproximateRouterRecall != 0 ||
		exactOnly.Metrics.HNSWRecallLoss != 0 {
		t.Fatalf("exact-only metrics=%+v", exactOnly.Metrics)
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

func TestM6CoordinatorStageUsesRealM4M6AndLabelsLocalSimulation(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("durable M1 lifecycle publication is unsupported; M6 coordinator coverage remains platform-neutral in TreeDB/nativewire")
	}
	dataset := writeFixtureForTest(t, 32, 4, 8)
	out := t.TempDir()
	var stdout bytes.Buffer
	args := []string{
		"-dataset", dataset,
		"-out", out,
		"-partitions", "4",
		"-probes", "4",
		"-top-k", "10",
		"-stage", m6CoordinatorStageV1,
		"-source-hnsw-degree", "4",
		"-router-representatives", "2",
		"-router-leaf-size", "1",
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
	evidence := result.Coordinator
	if result.ResultKind != m6CoordinatorResultKindV1 ||
		result.ProductionEvidence || evidence == nil ||
		evidence.ProductionEvidence ||
		!evidence.ExactParityChecked || !evidence.ExactParityPassed ||
		evidence.EvidenceKind != m6CoordinatorEvidenceKindV1 ||
		evidence.Transport != "in_process_transport_neutral_m5_contract_simulation" ||
		evidence.ReadProof != "synthetic_local_proof_not_measured" ||
		evidence.SourceHNSWDegree != 4 ||
		evidence.Queries != 4 || evidence.Probes != 4 ||
		evidence.Counters.SelectedPartitions != 16 ||
		evidence.Counters.SelectedGroups != 16 ||
		evidence.Counters.Requests != 16 ||
		evidence.Counters.RPCs != 16 ||
		evidence.Counters.Candidates != 128 ||
		result.Metrics.MeasurementStatus != m6CoordinatorMeasurementV1 ||
		result.Metrics.SourceHNSWDegree != 4 ||
		result.GOMAXPROCS < 1 ||
		result.GoMemoryLimitBytes < 1 ||
		result.Metrics.ShardP50Nanos <= 0 ||
		result.Metrics.P50Nanos <= 0 {
		t.Fatalf("M6 result=%+v evidence=%+v", result, evidence)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("M6 artifacts=%d want=2", len(entries))
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "m6_local_service_") {
			t.Fatalf("M6 artifact lacks isolated identity: %q", entry.Name())
		}
		if !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		markdown, err := os.ReadFile(filepath.Join(out, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(markdown, []byte("M6 local-service simulation")) ||
			!bytes.Contains(markdown, []byte("not production network, Raft read-proof, remote-service, or M8 acceptance evidence")) ||
			!bytes.Contains(markdown, []byte("GOMAXPROCS:")) ||
			!bytes.Contains(markdown, []byte("Go memory limit:")) ||
			!bytes.Contains(markdown, []byte("source HNSW degree: 4")) {
			t.Fatalf("M6 Markdown has incorrect evidence boundary:\n%s", markdown)
		}
	}
}

func TestM6LocalDispatcherUsesCosineForFP32Vectors(t *testing.T) {
	vectors := [][]float64{{1, 0}, {2, 0}}
	norms, err := m6VectorNormsV1(vectors, 2)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &m6LocalShardDispatcherV1{
		vectors: vectors, vectorNorms: norms, partitions: 1,
	}
	query := []float32{1, 0}
	queryNorm, err := m6QueryNormV1(query)
	if err != nil {
		t.Fatal(err)
	}

	partition, visited, err := dispatcher.partitionTopKV1(context.Background(), query, queryNorm, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	global, err := dispatcher.globalTopKV1(context.Background(), query, 2)
	if err != nil {
		t.Fatal(err)
	}
	if visited != 2 || len(partition) != 2 || len(global) != 2 {
		t.Fatalf("partition=%+v visited=%d global=%+v", partition, visited, global)
	}
	for name, got := range map[string][]string{
		"partition": {partition[0].ID, partition[1].ID},
		"global":    {global[0].ID, global[1].ID},
	} {
		if got[0] != "doc-000000" || got[1] != "doc-000001" {
			t.Fatalf("%s order=%v; raw-dot scoring would incorrectly rank doc-000001 first", name, got)
		}
	}
	for _, got := range []float32{partition[0].Score, partition[1].Score, global[0].Score, global[1].Score} {
		if math.Float32bits(got) != math.Float32bits(1) {
			t.Fatalf("cosine score=%v want=1", got)
		}
	}
}

func TestM6CoordinatorStageFailsBeforePartialEvidenceWithoutNamespacePersistence(t *testing.T) {
	out := filepath.Join(t.TempDir(), "evidence")
	args := []string{
		"-dataset", "must-not-be-read",
		"-out", out,
		"-partitions", "4",
		"-probes", "4",
		"-stage", m6CoordinatorStageV1,
	}
	var stdout bytes.Buffer
	err := runWithRuntimeCapabilities(args, &stdout, benchmarkRuntimeCapabilities{})
	if !errors.Is(err, collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1) {
		t.Fatalf("unsupported M6 coordinator stage err=%v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unsupported M6 coordinator stage emitted stdout: %q", stdout.String())
	}
	if _, statErr := os.Stat(out); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unsupported M6 coordinator stage created partial evidence directory: %v", statErr)
	}
}

func TestM3OverlapPartitionIndexBuildsReopensAndSearchesNativePacks(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("durable M1 lifecycle publication is unsupported; native pack codec coverage remains platform-neutral in TreeDB/collections")
	}
	dataset := writeFixtureForTest(t, 64, 8, 8)
	out := t.TempDir()
	args := []string{
		"-dataset", dataset,
		"-out", out,
		"-partitions", "4",
		"-probes", "1",
		"-overlap", "0,0.20",
		"-top-k", "4",
		"-stage", "overlap,partition_index",
		"-partition-repetitions", "1",
		"-partition-pivots", "2",
		"-partition-max-leaf-bucket", "8",
		"-partition-degree", "4",
	}
	var stdout bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &stdout); err != nil {
		t.Fatal(err)
	}
	var report m3PartitionIndexReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.ResultKind != "m3_native_partition_hnsw_evidence" || len(report.Rows) != 2 || !strings.HasPrefix(report.ReplicationGate, "passed:") {
		t.Fatalf("report=%+v", report)
	}
	for _, row := range report.Rows {
		if row.SourcePhysicalBytes <= 0 || row.PeakDerivedTemporaryBytes < row.FinalDerivedPhysicalBytes || row.FinalDerivedPhysicalBytes < int64(row.PackBytes) || row.PackBytes == 0 || row.PartitionHNSWM != 4 || row.LocalSearches != 8*4 || row.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || row.MissingAssets != 0 || row.CorruptAssets != 0 || row.StaleAssets != 0 || row.ExactLocalRecallAtK <= 0 || row.EdgesPerOp <= 0 {
			t.Fatalf("M3 row=%+v", row)
		}
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("M3 artifacts=%d want M2 artifact, M2 report, M3 report", len(entries))
	}
}

func TestM3PartitionIndexFailsBeforePartialEvidenceWithoutNamespacePersistence(t *testing.T) {
	out := filepath.Join(t.TempDir(), "evidence")
	args := []string{
		"-dataset", "must-not-be-read",
		"-out", out,
		"-partitions", "4",
		"-probes", "1",
		"-overlap", "0,0.20",
		"-top-k", "4",
		"-stage", "overlap,partition_index",
	}
	var stdout bytes.Buffer
	err := runWithRuntimeCapabilities(args, &stdout, benchmarkRuntimeCapabilities{})
	if !errors.Is(err, collections.ErrVectorPartitionNamespacePersistenceUnsupportedV1) {
		t.Fatalf("unsupported M3 stage err=%v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unsupported M3 stage emitted stdout: %q", stdout.String())
	}
	if _, statErr := os.Stat(out); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unsupported M3 stage created partial evidence directory: %v", statErr)
	}
}

func TestM3PartitionAssetFileIDBounds(t *testing.T) {
	if got, err := m3PartitionAssetFileID(1); err != nil || got != 40_001 {
		t.Fatalf("first M3 file ID=%d err=%v", got, err)
	}
	maxGeneration := uint64(^uint32(0)) - m3PartitionAssetFileIDBase
	if got, err := m3PartitionAssetFileID(maxGeneration); err != nil || got != ^uint32(0) {
		t.Fatalf("last M3 file ID=%d err=%v", got, err)
	}
	if _, err := m3PartitionAssetFileID(0); err == nil {
		t.Fatal("zero generation accepted")
	}
	if _, err := m3PartitionAssetFileID(maxGeneration + 1); err == nil {
		t.Fatal("overflowing generation accepted")
	}
}

func TestM3RouterAssetFileIDBounds(t *testing.T) {
	if got, err := m3RouterAssetFileID(1); err != nil || got != 50_001 {
		t.Fatalf("first M3 router file ID=%d err=%v", got, err)
	}
	maxGeneration := uint64(^uint32(0)) - m3RouterAssetFileIDBase
	if got, err := m3RouterAssetFileID(maxGeneration); err != nil || got != ^uint32(0) {
		t.Fatalf("last M3 router file ID=%d err=%v", got, err)
	}
	if _, err := m3RouterAssetFileID(0); err == nil {
		t.Fatal("zero router generation accepted")
	}
	if _, err := m3RouterAssetFileID(maxGeneration + 1); err == nil {
		t.Fatal("overflowing router generation accepted")
	}
}

func TestM3RouterPartitionsBindArtifactToNativeOrdinalsV1(t *testing.T) {
	artifact := vectorpartition.Artifact{
		Config:     vectorpartition.Config{Partitions: 2},
		Assignment: []int{1, 0, 1},
	}
	partitions, err := m3RouterPartitions(
		artifact,
		[]int{2, 0, 1},
		[][]float64{{1, 2}, {3, 4}, {5, 6}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 2 || partitions[0].PartitionID != 0 || partitions[1].PartitionID != 1 ||
		len(partitions[0].Vectors) != 1 || len(partitions[1].Vectors) != 2 {
		t.Fatalf("router partitions=%+v", partitions)
	}
	if got := partitions[0].Vectors[0]; got.Ordinal != 0 || len(got.Values) != 2 || got.Values[0] != 3 || got.Values[1] != 4 {
		t.Fatalf("partition 0 vector=%+v", got)
	}
	if got := partitions[1].Vectors[0]; got.Ordinal != 2 || got.Values[0] != 1 || got.Values[1] != 2 {
		t.Fatalf("partition 1 first vector=%+v", got)
	}
	if got := partitions[1].Vectors[1]; got.Ordinal != 1 || got.Values[0] != 5 || got.Values[1] != 6 {
		t.Fatalf("partition 1 second vector=%+v", got)
	}
}

func TestM3PartitionIndexPersistentDirectoryIsExplicitAndEmptyV1(t *testing.T) {
	root := t.TempDir()
	persist := filepath.Join(root, "persistent")
	got, cleanup, err := m3PartitionIndexDirectory(persist)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup || got != persist {
		t.Fatalf("persistent directory=%q cleanup=%t", got, cleanup)
	}
	if err := os.WriteFile(filepath.Join(persist, "occupied"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m3PartitionIndexDirectory(persist); err == nil {
		t.Fatal("non-empty persistent directory accepted")
	}
	if _, err := parseConfig([]string{
		"-dataset", fixturePath(t),
		"-out", t.TempDir(),
		"-partitions", "4",
		"-probes", "1",
		"-overlap", "0,0.2",
		"-stage", "overlap,partition_index",
		"-m3-persist-db", filepath.Join(root, "invalid-multiple"),
	}); err == nil {
		t.Fatal("persistent M3 DB accepted multiple overlap rows")
	}
}

func TestOpenM3PartitionSearchersClosesPartialSuccess(t *testing.T) {
	openFailure := errors.New("injected partition open failure")
	var first *collections.VectorPartitionLocalSearcherV1
	searchers, err := openM3PartitionSearchers(2, func(partition uint32) (*collections.VectorPartitionLocalSearcherV1, error) {
		if partition == 1 {
			return nil, openFailure
		}
		searcher, openErr := collections.OpenVectorPartitionLocalSearcherV1(collections.VectorPartitionSearchAssetV1{
			ManifestChecksum: strings.Repeat("c", 64),
			Generation:       1,
			PartitionID:      partition,
			Dimensions:       1,
			IDs:              []string{"row-0"},
			Vectors:          [][]float32{{1}},
			Kinds:            []collections.VectorPartitionMembershipKindV1{collections.VectorPartitionMembershipHomeV1},
		})
		first = searcher
		return searcher, openErr
	})
	if !errors.Is(err, openFailure) {
		t.Fatalf("partial open err=%v", err)
	}
	if searchers != nil {
		t.Fatalf("partial open returned searchers=%v", searchers)
	}
	if first == nil || !first.Status().Retired {
		t.Fatalf("first searcher not closed after partial failure: %+v", first)
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

func TestM8ProductionModeParsesCanonicalTopologyAndSweepsV1(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-mode", m8ProductionMultiGroupModeV1,
		"-dataset", fixturePath(t),
		"-out", t.TempDir(),
		"-partitions", "16",
		"-raft-groups", "4",
		"-raft-nodes-per-group", "3",
		"-probes", "1,4,16",
		"-overlap", "0,0.20",
		"-concurrency", "1,16,64",
		"-warmup", "3",
		"-ef-search", "64,4096",
		"-m8-existing-db", "/retained/m8-assets",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.stage != m8ProductionMultiGroupModeV1 || cfg.raftGroups != 4 || cfg.raftNodes != 3 ||
		fmt.Sprint(cfg.probes) != "[1 4 16]" || fmt.Sprint(cfg.overlaps) != "[0 0.2]" ||
		fmt.Sprint(cfg.concurrency) != "[1 16 64]" || cfg.warmup != 3 || fmt.Sprint(cfg.efSearch) != "[64 4096]" || cfg.m8ExistingDB != "/retained/m8-assets" {
		t.Fatalf("M8 config=%+v", cfg)
	}
	limit := nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxSelectedPartitions
	if _, err := parseConfig([]string{
		"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(),
		"-partitions", strconv.Itoa(limit), "-raft-groups", "2",
	}); err != nil {
		t.Fatalf("rejected M8 coordinator partition boundary %d: %v", limit, err)
	}
	for _, args := range [][]string{
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4", "-raft-nodes-per-group", "2"},
		{"-mode", m8ProductionMultiGroupModeV1, "-stage", "router", "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"},
		{"-stage", "router", "-m8-existing-db", "/retained/m8-assets", "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-probes", "1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4", "-warmup", "-1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", strconv.Itoa(limit + 1), "-raft-groups", "2"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted malformed M8 config %#v", args)
		}
	}
}

func TestM8BenchmarkWorkCapAndOverflowV1(t *testing.T) {
	cfg := config{overlaps: []float64{0, .2}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, warmup: 3}
	plan, err := validateM8BenchmarkWork(cfg, fixtureManifest{Queries: 5}, 84)
	if err != nil || plan.QueryRequests != 84 {
		t.Fatalf("M8 work plan=%+v err=%v", plan, err)
	}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Queries: 5}, 83); err == nil {
		t.Fatal("accepted oversized M8 sweep")
	}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Queries: math.MaxInt}, maxBenchmarkWorkUnits); err == nil {
		t.Fatal("accepted overflowing M8 sweep")
	}
	if _, err := validateM8BenchmarkWork(config{overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}}, fixtureManifest{Queries: math.MaxInt}, maxBenchmarkWorkUnits); err == nil {
		t.Fatal("accepted overflowing M8 preflight accounting")
	}
}

func TestM8ProductionEvidenceJSONKeepsEveryTopologyDimensionV1(t *testing.T) {
	raw, err := json.Marshal(m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, RouterCandidates: 256},
		Rows: []m8ProductionRowV1{{Probes: 4, EfSearch: 128, Concurrency: 16, Samples: 32, RecallAtK: 0, Attribution: m8ProductionAttributionV1{
			Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, ExhaustivePartitionRecallAtK: 1,
			ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
			ExactRepresentativeRecallAtK: .9, ApproximateRepresentativeRecallAtK: .8,
			LocalHNSWRecallAtK: .7, EndToEndRecallAtK: 0,
			CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
			ApproximateRouterCandidateBudget: 256, ResidualLossOwners: []string{"partition_local_hnsw"},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"raft_groups":4`, `"raft_nodes_per_group":3`, `"partitions":16`, `"approximate_router_candidate_budget":256`, `"probes":4`, `"ef_search":128`, `"concurrency":16`, `"samples":32`, `"recall_at_k":0`, `"contract":"` + m8CanonicalResultContractV1 + `"`, `"global_exact_recall_at_k":1`, `"exhaustive_partition_union_score_parity":true`, `"residual_loss_owners":["partition_local_hnsw"]`} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("missing %s in %s", field, raw)
		}
	}
}

func TestM8ArtifactNameIncludesConfigurationV1(t *testing.T) {
	fixture := fixtureManifest{Fixture: "x", Checksum: "y"}
	base := config{headSHA: strings.Repeat("a", 40), raftGroups: 2, raftNodes: 3, partitions: 4, probes: []int{4}, overlaps: []float64{0}, topK: 10, concurrency: []int{1}, efSearch: []int{64}, routerCandidates: 64}
	first, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"})
	if err != nil {
		t.Fatal(err)
	}
	base.partitions = 8
	second, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"})
	if err != nil || first == second {
		t.Fatalf("names %q %q err=%v", first, second, err)
	}
	base.partitions = 4
	third, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "different"})
	if err != nil || first == third {
		t.Fatalf("asset identities collided %q %q err=%v", first, third, err)
	}
	base.routerCandidates = 32
	fourth, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"})
	if err != nil || first == fourth {
		t.Fatalf("router candidate budgets collided %q %q err=%v", first, fourth, err)
	}
}

func TestCanonicalExactNeighborsContractTiePrecisionAndDedupeV1(t *testing.T) {
	got := canonicalExactNeighborsV1([]neighbor{
		{ID: "duplicate", Distance: 0.2}, {ID: "z", Distance: 0.1}, {ID: "a", Distance: 0.1},
		{ID: "duplicate", Distance: 0.05}, {ID: "near", Distance: 0.100000001},
	}, 4)
	want := []string{"duplicate", "a", "near", "z"}
	if len(got) != len(want) {
		t.Fatalf("got=%+v", got)
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("rank %d got=%+v want=%s", i, got, want[i])
		}
	}
	if got[0].Distance != float64(float32(0.05)) {
		t.Fatalf("duplicate retained wrong score: %+v", got[0])
	}
}

func TestM8CanonicalFP32ScoreContractTiePrecisionAndDedupeV1(t *testing.T) {
	near := math.Float32frombits(math.Float32bits(0.9))
	got := m8CanonicalResultsV1([]m8CanonicalResultV1{
		{ID: "duplicate", Score: 0.1},
		{ID: "z", Score: near},
		{ID: "a", Score: near},
		{ID: "duplicate", Score: 0.95},
		{ID: "lower", Score: math.Float32frombits(math.Float32bits(near) - 1)},
	}, 4)
	want := []string{"duplicate", "a", "z", "lower"}
	if len(got) != len(want) {
		t.Fatalf("got=%+v", got)
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("rank %d got=%+v want=%s", i, got, want[i])
		}
	}
	if got[0].Score != float32(0.95) {
		t.Fatalf("duplicate retained wrong score: %+v", got[0])
	}
}

func TestM8CanonicalContractRejectsInvalidBoundsAndScoresV1(t *testing.T) {
	if got := m8CanonicalResultsV1([]m8CanonicalResultV1{{ID: "a", Score: 1}}, -1); got != nil {
		t.Fatalf("negative top-k result=%+v", got)
	}
	if got := m8CanonicalResultsV1([]m8CanonicalResultV1{{ID: "a", Score: float32(math.NaN())}}, 1); got != nil {
		t.Fatalf("nonfinite result=%+v", got)
	}
}

func TestValidM8AttributionPersistsExhaustiveUnionFailureV1(t *testing.T) {
	attribution := m8ProductionAttributionV1{
		Contract:                           m8CanonicalResultContractV1,
		GlobalExactRecallAtK:               1,
		ExhaustivePartitionRecallAtK:       .5,
		ExhaustivePartitionIDParity:        false,
		ExhaustivePartitionScoreParity:     false,
		ExactRepresentativeRecallAtK:       .5,
		ApproximateRepresentativeRecallAtK: .5,
		LocalHNSWRecallAtK:                 .5,
		EndToEndRecallAtK:                  .5,
		CoordinatorMergeIDParity:           true,
		CoordinatorMergeScoreParity:        true,
		ApproximateRouterCandidateBudget:   1,
	}
	attribution.ResidualLossOwners = m8AttributionLossOwnersV1(attribution)
	if !validM8AttributionV1(attribution) {
		t.Fatalf("rejected attributable exhaustive-union failure: %+v", attribution)
	}
	if !slices.Equal(attribution.ResidualLossOwners, []string{"partition_membership_or_score_contract"}) {
		t.Fatalf("loss owners=%v", attribution.ResidualLossOwners)
	}
	attribution.ResidualLossOwners = nil
	if validM8AttributionV1(attribution) {
		t.Fatal("accepted exhaustive-union failure without its loss owner")
	}
}

func TestM8CoordinatorResponseCanonicalShapeFailsClosedV1(t *testing.T) {
	response := nativewire.VectorPartitionCoordinatorResponseV1{
		Neighbors:        []nativewire.VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: .9}, {ID: "b", Score: .8}},
		ProbedPartitions: []uint32{0, 1},
	}
	response.ProbedGroups = append(response.ProbedGroups, "group-a")
	manifest := collections.VectorPartitionManifestV1{PartitionCount: 4, Placements: []collections.VectorPartitionPlacementV1{
		{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-a"},
		{PartitionID: 2, GroupID: "group-b"}, {PartitionID: 3, GroupID: "group-b"},
	}}
	if got, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err != nil || len(got) != 2 {
		t.Fatalf("valid response got=%+v err=%v", got, err)
	}
	response.Neighbors[0], response.Neighbors[1] = response.Neighbors[1], response.Neighbors[0]
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted noncanonical response order")
	}
	response.Neighbors = []nativewire.VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: .9}, {ID: "a", Score: .8}}
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted duplicate response neighbor")
	}
	response.Neighbors = []nativewire.VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: .9}, {ID: "b", Score: .8}}
	response.ProbedPartitions = []uint32{0, 2}
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted response missing an owning group")
	}
	response.ProbedPartitions = []uint32{0, 1}
	response.ProbedGroups = append(response.ProbedGroups[:0], "group-b")
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted a non-owner group")
	}
	response.ProbedGroups = append(response.ProbedGroups[:0], "group-a")
	response.Neighbors = response.Neighbors[:1]
	if got, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err != nil || len(got) != 1 {
		t.Fatalf("valid short response got=%+v err=%v", got, err)
	}
	response.Neighbors = append(response.Neighbors,
		nativewire.VectorPartitionCoordinatorNeighborV1{ID: "b", Score: .8},
		nativewire.VectorPartitionCoordinatorNeighborV1{ID: "c", Score: .7},
	)
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted response longer than top-k")
	}
}

func TestM8ProfileCaptureWritesRequiredRuntimeArtifactsV1(t *testing.T) {
	dir := t.TempDir()
	capture, err := startM8ProfileCaptureV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	runtime.Gosched()
	paths, err := capture.Stop()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 7 {
		t.Fatalf("profile paths=%v", paths)
	}
	for _, path := range paths {
		info, statErr := os.Stat(path)
		if statErr != nil || info.Size() == 0 {
			t.Fatalf("profile %s info=%v err=%v", path, info, statErr)
		}
	}
	if again, err := capture.Stop(); err != nil || fmt.Sprint(again) != fmt.Sprint(paths) {
		t.Fatalf("idempotent stop paths=%v err=%v", again, err)
	}
}

func TestM8GateLedgerRequiresMatchedRecallQPSAndTailV1(t *testing.T) {
	report := m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: 0.9},
		Rows: []m8ProductionRowV1{
			{Status: "pass", Probes: 16, EfSearch: 128, Concurrency: 16, RecallAtK: 0.99, QPS: 100, P95Nanos: 1000, ExactParityChecked: true, ExactParityPassed: true},
			{Status: "pass", Probes: 4, EfSearch: 128, Concurrency: 16, RecallAtK: 0.92, QPS: 116, P95Nanos: 999},
		},
		Failure:   m8ProductionFailureEvidenceV1{Passed: true},
		Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 1, PeakRSSMeasured: true, MaxPartitionLoad: 65_000, BalanceHardCap: 65_625},
	}
	ledger := m8ProductionGateLedgerForReportV1(report)
	if ledger.ExhaustiveParity != "pass" || ledger.FailureHonesty != "pass" || ledger.Recall != "pass" || ledger.ProbeReduction != "pass" || ledger.EndToEndQPS != "pass" || ledger.TailLatency != "pass" || ledger.Balance != "pass" || ledger.ResourceBounds != "measured_not_bounded" || ledger.OverlapStorage != "fail" {
		t.Fatalf("ledger=%+v", ledger)
	}
	report.Rows[1].QPS = 114.9
	report.Rows[1].P95Nanos = 1001
	ledger = m8ProductionGateLedgerForReportV1(report)
	if ledger.EndToEndQPS != "fail" || ledger.TailLatency != "fail" {
		t.Fatalf("unmatched performance ledger=%+v", ledger)
	}
}

func TestSourceHNSWDegreeIsExplicitlyBoundedWithLegacyDefaultV1(t *testing.T) {
	base := []string{
		"-dataset", fixturePath(t),
		"-out", t.TempDir(),
		"-partitions", "4",
		"-probes", "1",
		"-stage", "router",
	}
	cfg, err := parseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.sourceHNSWDegree != partitionHNSWDegree {
		t.Fatalf("default source HNSW degree=%d want %d", cfg.sourceHNSWDegree, partitionHNSWDegree)
	}
	selected, err := parseConfig(append(append([]string(nil), base...), "-source-hnsw-degree", "4"))
	if err != nil {
		t.Fatal(err)
	}
	if selected.sourceHNSWDegree != 4 {
		t.Fatalf("selected source HNSW degree=%d want 4", selected.sourceHNSWDegree)
	}
	for _, degree := range []string{"0", strconv.Itoa(maxSourceHNSWDegree + 1)} {
		args := append(append([]string(nil), base...), "-source-hnsw-degree", degree)
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted out-of-bounds source HNSW degree %s", degree)
		}
	}
}

func TestRunRejectsTopKAboveFixtureBeforeOracleAllocation(t *testing.T) {
	err := runWithHermeticProvenance(t, []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-top-k", "10001"}, io.Discard)
	if err == nil {
		t.Fatal("accepted top-k above fixture")
	}
}

func TestM8RunRejectsTopKAboveFixtureAndShardLimitsBeforeTopologyV1(t *testing.T) {
	t.Setenv("BASE_SHA", strings.Repeat("a", 40))
	t.Setenv("GITHUB_SHA", strings.Repeat("b", 40))
	t.Setenv("GITHUB_EVENT_PATH", "")
	fixture := writeFixtureForTest(t, 32, 1, 8)
	shardFixture := writeFixtureForTest(t, 512, 1, 8)
	base := []string{
		"-mode", m8ProductionMultiGroupModeV1,
		"-out", t.TempDir(),
		"-partitions", "4",
		"-raft-groups", "2",
		"-concurrency", "1",
		"-warmup", "0",
	}
	for _, test := range []struct {
		name    string
		dataset string
		topK    string
		ef      string
		want    string
	}{
		{name: "fixture", dataset: fixture, topK: "33", ef: "64", want: "top-k cannot exceed fixture vectors"},
		{name: "shard", dataset: shardFixture, topK: "257", ef: "512", want: "top-k cannot exceed M8 shard limit 256"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append(append([]string(nil), base...), "-dataset", test.dataset, "-top-k", test.topK, "-ef-search", test.ef)
			err := runWithRuntimeCapabilities(args, io.Discard, benchmarkRuntimeCapabilities{vectorPartitionNamespacePersistence: true})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run error=%v, want %q", err, test.want)
			}
		})
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

func TestM3BenchmarkWorkBudgetBoundary(t *testing.T) {
	fixture := fixtureManifest{Vectors: 10, Queries: 10}
	cfg := config{overlaps: []float64{0, .2}}
	plan, err := validateM3BenchmarkWork(cfg, fixture, 540)
	if err != nil {
		t.Fatalf("exact M3 work-budget boundary rejected: %v", err)
	}
	if plan.ChecksumVectorQueryVisits != 100 || plan.MembershipVectorQueryVisits != 440 || plan.VectorQueryVisits != 540 {
		t.Fatalf("M3 boundary work plan=%+v", plan)
	}
	if _, err := validateM3BenchmarkWork(cfg, fixture, 539); err == nil {
		t.Fatal("accepted M3 work plan above exact boundary")
	}
	canonical, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	canonicalPlan, err := validateM3BenchmarkWork(cfg, canonical, maxBenchmarkWorkUnits)
	if err != nil {
		t.Fatalf("canonical M3 work plan rejected: %v", err)
	}
	if canonicalPlan.VectorQueryVisits != 6_912_000 {
		t.Fatalf("canonical M3 work plan=%+v", canonicalPlan)
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

	m3Out := t.TempDir()
	err = runWithRuntimeCapabilities([]string{
		"-dataset", dataset,
		"-out", m3Out,
		"-partitions", "1",
		"-probes", "1",
		"-overlap", "0",
		"-top-k", "1",
		"-stage", "overlap,partition_index",
	}, io.Discard, benchmarkRuntimeCapabilities{vectorPartitionNamespacePersistence: true})
	if err == nil || !strings.Contains(err.Error(), "modeled M3 benchmark work") {
		t.Fatalf("pathological M3 work error=%v", err)
	}
	entries, readErr = os.ReadDir(m3Out)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("rejected M3 work budget emitted evidence: %+v", entries)
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
		SchemaVersion:      schemaVersion,
		ResultKind:         "simulation_only",
		BaseSHA:            strings.Repeat("a", 40),
		HeadSHA:            strings.Repeat("b", 40),
		Dataset:            fixtureManifest{Seed: 1, Checksum: strings.Repeat("c", 64)},
		Seed:               1,
		MemoryBudgetBytes:  1024,
		ModeledPeakBytes:   512,
		MemoryBudgetScope:  memoryBudgetScope,
		GOMAXPROCS:         1,
		GoMemoryLimitBytes: 1,
		Stages:             []stageResult{{Name: "exact_global_top_k", Method: "test", Enabled: true, Available: true}},
		Metrics:            metricsV1{MeasurementStatus: "simulation_not_measured"},
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
		func(result *runResult) { result.GOMAXPROCS = 0 },
		func(result *runResult) { result.GoMemoryLimitBytes = 0 },
	} {
		bad := r
		mutate(&bad)
		if err := validateResult(bad); err == nil {
			t.Fatalf("accepted invalid result: %+v", bad)
		}
	}
}
