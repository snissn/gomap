package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func fixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "testdata", "vector_partition_10k")
}

func TestGenerateTruthCacheBoundedRoundTripV1(t *testing.T) {
	dataset, cache := t.TempDir(), t.TempDir()
	if err := run([]string{"generate-fixture", "-out", dataset, "-vectors", "3", "-queries", "2", "-dimensions", "2", "-seed", "7"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	args := []string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-top-k", "2", "-seed", "7", "-max-vectors", "3", "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10), "-max-exact-truth-visits", "6"}
	if err := run(append([]string(nil), args[:len(args)-1]...), io.Discard); err == nil {
		t.Fatal("accepted insufficient exact-truth visit cap")
	}
	var out strings.Builder
	if err := run(args, &out); err != nil {
		t.Fatal(err)
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	identity := m8TruthCacheIdentityV1(fixture, 2)
	path := m8TruthCacheArtifactPathV1(cache, identity)
	fields := strings.Fields(out.String())
	if len(fields) < 5 || !strings.Contains(out.String(), "visits=6") {
		t.Fatalf("generator output=%q", out.String())
	}
	digest := strings.TrimPrefix(fields[1], "artifact_sha256=")
	truth, got, err := m8ReadTruthCacheV1(path, fixture, fixture.Queries, 2, uint64(fixture.Vectors), digest)
	if err != nil || got != digest || len(truth) != fixture.Queries {
		t.Fatalf("truth round trip got=%q err=%v rows=%d", got, err, len(truth))
	}
	t.Run("explicit_zero_seed", func(t *testing.T) {
		zeroDataset, zeroCache := t.TempDir(), t.TempDir()
		if err := run([]string{"generate-fixture", "-out", zeroDataset, "-vectors", "3", "-queries", "2", "-dimensions", "2", "-seed", "0"}, io.Discard); err != nil {
			t.Fatal(err)
		}
		zeroArgs := []string{"generate-truth-cache", "-dataset", zeroDataset, "-out", zeroCache, "-top-k", "2", "-seed", "0", "-max-vectors", "3", "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10), "-max-exact-truth-visits", "6"}
		if err := run(zeroArgs, io.Discard); err != nil {
			t.Fatalf("generate seed-zero truth cache: %v", err)
		}
		withoutSeed := append([]string(nil), zeroArgs[:7]...)
		withoutSeed = append(withoutSeed, zeroArgs[9:]...)
		if err := run(withoutSeed, io.Discard); err == nil {
			t.Fatal("accepted truth-cache generation without explicit seed")
		}
	})
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

func TestQualificationFixtureGeneratorsHaveIndependentHeldoutQueriesV1(t *testing.T) {
	for _, generator := range []string{qualificationSyntheticGeneratorV1, qualificationEmbeddingGeneratorV1} {
		m := fixtureManifest{SchemaVersion: schemaVersion, Fixture: "qualification", Generator: generator, Arithmetic: fixtureArithmetic, Vectors: 128, Queries: 64, Dimensions: 32, Metric: "cosine", Seed: 17}
		vectors, queries := fixtureData(m)
		if len(vectors) != m.Vectors || len(queries) != m.Queries || !supportedFixtureGeneratorV1(generator) {
			t.Fatalf("generator=%s generated malformed shape", generator)
		}
		for qi, query := range queries {
			for vi, vector := range vectors {
				if slices.Equal(query, vector) {
					t.Fatalf("generator=%s heldout query %d copied corpus vector %d", generator, qi, vi)
				}
			}
		}
		m.Checksum = fixtureChecksumFromData(vectors, queries)
		if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
			t.Fatalf("generator=%s fixture rejected: %v", generator, err)
		}
		vectorsAgain, queriesAgain := fixtureData(m)
		if fixtureChecksumFromData(vectorsAgain, queriesAgain) != m.Checksum {
			t.Fatalf("generator=%s not reproducible", generator)
		}
	}
}

func TestCommittedHighEntropyQualificationIdentityV1(t *testing.T) {
	m, err := loadFixture(filepath.Join("..", "..", "testdata", "vector_partition_qualification_high_entropy_1m"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		t.Fatal(err)
	}
	if m.Generator != qualificationSyntheticGeneratorV1 || m.Vectors != 1_000_000 || m.Queries < 1_000 || m.Dimensions != 128 || m.Seed != 4015 {
		t.Fatalf("unexpected qualification identity: %+v", m)
	}
}

func TestCommittedEmbeddingQualificationIdentityV1(t *testing.T) {
	m, err := loadFixture(filepath.Join("..", "..", "testdata", "vector_partition_qualification_embedding_mixture_250k"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		t.Fatal(err)
	}
	if m.Generator != qualificationEmbeddingGeneratorV1 || m.Vectors != 250_000 || m.Queries < 1_000 || m.Dimensions != 128 || m.Seed != 4016 {
		t.Fatalf("unexpected qualification identity: %+v", m)
	}
}

func TestCommittedM3QualificationShapesRequireExplicitVisitOverrideV1(t *testing.T) {
	for _, name := range []string{"vector_partition_qualification_high_entropy_1m", "vector_partition_qualification_embedding_mixture_250k"} {
		m, err := loadFixture(filepath.Join("..", "..", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateM3BenchmarkWork(config{partitions: 16, overlaps: []float64{0}, topK: 10, partition: vectorpartition.Config{Degree: 16}}, m, maxBenchmarkWorkUnits); err == nil {
			t.Fatalf("%s accepted without explicit M3 visit override", name)
		}
		if _, err := validateM3BenchmarkWork(config{partitions: 16, overlaps: []float64{0}, topK: 10, partition: vectorpartition.Config{Degree: 16}}, m, 3_113_430_000); err != nil {
			t.Fatalf("%s explicit M3 visit override: %v", name, err)
		}
	}
}

func TestM8DirectPlannerUsesIndependentExactWorkCapV1(t *testing.T) {
	cfg := config{partitions: 1, probes: []int{1}, overlaps: []float64{0}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 1_000_000, Queries: 1_000, Dimensions: 1}, math.MaxInt64, math.MaxInt64); err == nil {
		t.Fatal("direct planner inherited unrelated request cap")
	}
}

func TestM8ExactTruthVisitBudgetRequiresQualificationOverrideV1(t *testing.T) {
	cfg := config{partitions: 16, probes: []int{1, 2, 4, 8, 16}, overlaps: []float64{0}, efSearch: []int{64}, concurrency: []int{1}, topK: 10, m8MaxExactTruthVisits: maxBenchmarkWorkUnits}
	m := fixtureManifest{Vectors: 1_000_000, Queries: 1_000, Dimensions: 128}
	if _, err := validateM8BenchmarkWork(cfg, m, maxBenchmarkWorkUnits, maxFixtureBytes); err == nil || !strings.Contains(err.Error(), "exact_truth_vector_visits=1000000000") {
		t.Fatalf("missing exact truth refusal: %v", err)
	}
	cfg.m8MaxExactTruthVisits = 2_000_000_000
	plan, err := validateM8BenchmarkWork(cfg, m, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ExactTruthVectorVisits != 1_000_000_000 || plan.FixtureChecksumVectorVisits != 1_000_000_000 || plan.ExactWorkVectorVisits != 2_000_000_000 {
		t.Fatalf("exact truth visits=%d", plan.ExactTruthVectorVisits)
	}
}

func TestTruthHomePartitionDiagnosticsV1(t *testing.T) {
	truth := []m8CanonicalResultV1{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	homes := map[string]uint32{"a": 0, "b": 0, "c": 1, "d": 2}
	coverage, distinct, colocation, err := m8TruthHomePartitionDiagnosticsV1(truth, []uint32{0, 2}, homes)
	if err != nil {
		t.Fatal(err)
	}
	if coverage != .75 || distinct != 3 || colocation != 1.0/6.0 {
		t.Fatalf("coverage=%v distinct=%v colocation=%v", coverage, distinct, colocation)
	}
	if _, _, _, err := m8TruthHomePartitionDiagnosticsV1(truth, []uint32{0}, map[string]uint32{"a": 0}); err == nil {
		t.Fatal("missing truth home accepted")
	}
}

func TestM8MembershipOraclesSeparatePrimaryAndOverlapCeilingsV1(t *testing.T) {
	truth := []m8CanonicalResultV1{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	primary := map[string]uint32{"a": 0, "b": 1, "c": 2, "d": 3}
	final := map[string][]uint32{"a": {0, 1}, "b": {1}, "c": {2}, "d": {3}}
	gotPrimary, err := m8BestPrimaryHomeOracleRecallV1(truth, primary, 4, 1)
	if err != nil || gotPrimary != .25 {
		t.Fatalf("primary oracle=%v err=%v", gotPrimary, err)
	}
	gotFinal, err := m8BestMembershipOracleRecallV1(truth, final, 4, 1)
	if err != nil || gotFinal != .5 {
		t.Fatalf("final oracle=%v err=%v", gotFinal, err)
	}
	if _, err := m8BestMembershipOracleRecallV1(truth, final, 4, 5); err == nil {
		t.Fatal("accepted probe budget above partition count")
	}
	malformed := map[string][]uint32{"a": {0}, "b": {1}, "c": {2}, "d": {4}}
	if _, err := m8BestMembershipOracleRecallV1(truth, malformed, 4, 1); err == nil {
		t.Fatal("accepted out-of-range truth membership")
	}
	wideTruth := make([]m8CanonicalResultV1, 70)
	wideMemberships := make(map[string][]uint32, len(wideTruth))
	for i := range wideTruth {
		wideTruth[i].ID = fmt.Sprintf("wide-%d", i)
		wideMemberships[wideTruth[i].ID] = []uint32{uint32(i % 4)}
	}
	if got, err := m8BestMembershipOracleRecallV1(wideTruth, wideMemberships, 4, 2); err != nil || got != 36.0/70.0 {
		t.Fatalf("multiword final oracle=%v err=%v", got, err)
	}
}

func TestM8MembershipOracleCombinationBoundIsPreflightedV1(t *testing.T) {
	if got, err := m8MembershipOracleCombinationCountV1(27, 12, 20_000_000); err != nil || got != 17_383_860 {
		t.Fatalf("C(27,12)=%d err=%v", got, err)
	}
	cfg := config{partitions: 64, overlaps: []float64{0}, probes: []int{32}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	_, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 64, Queries: 1, Dimensions: 1}, math.MaxInt64, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "membership oracle C(64,32)") {
		t.Fatalf("missing combination preflight refusal: %v", err)
	}
	cfg.overlaps = []float64{.2}
	plan, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 64, Queries: 1, Dimensions: 1}, math.MaxInt64, math.MaxInt64)
	if err != nil || plan.MaxMembershipOracleSubsets != 0 || plan.MembershipOracleWorkUnits != 0 {
		t.Fatalf("unsupported-only run planned membership-oracle work: plan=%+v err=%v", plan, err)
	}
	cfg = config{partitions: 16, overlaps: []float64{0}, probes: []int{8}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	_, err = validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 16, Queries: 20_000, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "membership-oracle work") {
		t.Fatalf("missing aggregate membership-oracle refusal: %v", err)
	}
	cfg.efSearch = []int{64, 128}
	plan, err = validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 16, Queries: 5_000, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil || plan.MembershipOracleSubsetEvaluations != 64_350_000 || plan.MembershipOracleWorkUnits != 187_300_000 {
		t.Fatalf("ef-independent oracle plan=%+v err=%v", plan, err)
	}
	cfg = config{partitions: 16, overlaps: []float64{0}, probes: []int{8}, efSearch: []int{64}, concurrency: []int{1}, topK: 256}
	plan, err = validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 256, Queries: 1_000, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil || plan.MembershipOracleWorkUnits != 153_332_000 {
		t.Fatalf("bounded bitset oracle plan=%+v err=%v", plan, err)
	}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 256, Queries: 2_000, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "bounded_operations=") {
		t.Fatalf("missing bounded bitset-oracle refusal: %v", err)
	}
	cfg = config{partitions: 256, overlaps: []float64{0}, probes: []int{256}, efSearch: []int{64}, concurrency: []int{1}, topK: 1, m8MaxExactTruthVisits: math.MaxInt64}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 256, Queries: 3_100, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "membership-oracle work") {
		t.Fatalf("primary-home oracle work escaped aggregate cap: %v", err)
	}
	cfg = config{partitions: 16, overlaps: []float64{0}, probes: []int{16}, efSearch: []int{64}, concurrency: []int{1}, topK: 256, m8MaxExactTruthVisits: math.MaxInt64}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 256, Queries: 30_000, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "attribution diagnostics") {
		t.Fatalf("attribution diagnostics escaped aggregate cap: %v", err)
	}
}

func TestM8AttributionOwnersKeepRouterAndLocalLossSeparateV1(t *testing.T) {
	for _, test := range []struct {
		name                         string
		primary, final, exact, local float64
		owner, stage                 string
		delta                        float64
	}{
		{"primary ceiling", .6, .6, .6, .6, "primary_placement", "global_to_primary_home", .4},
		{"representative router", 1, 1, .75, .75, "exact_representative_routing", "final_membership_to_exact_routing", .25},
		{"local HNSW only", 1, 1, 1, .75, "partition_local_hnsw", "approximate_routing_to_local_hnsw", .25},
	} {
		t.Run(test.name, func(t *testing.T) {
			attribution := m8ProductionAttributionV1{GlobalExactRecallAtK: 1, OracleStagesComplete: true, PrimaryHomeOracleRecallAtK: test.primary, FinalMembershipOracleRecallAtK: test.final, ExactRepresentativeRecallAtK: test.exact, ApproximateRepresentativeRecallAtK: test.exact, LocalHNSWRecallAtK: test.local, ApproximateLocalHNSWRecallAtK: test.local, EndToEndRecallAtK: test.local, ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true, ApproximateRouterPartitionCoverageComplete: true}
			if !slices.Contains(m8AttributionLossOwnersV1(attribution), test.owner) {
				t.Fatalf("owners=%v want %q", m8AttributionLossOwnersV1(attribution), test.owner)
			}
			for _, got := range m8AttributionStageOwnersV1(attribution) {
				if got.Stage == test.stage && got.Owner == test.owner && got.Active && got.Delta == test.delta {
					return
				}
			}
			t.Fatalf("stages=%+v want active %q delta=%v", m8AttributionStageOwnersV1(attribution), test.stage, test.delta)
		})
	}
}

func TestM8PartitionContractFailureHasExplicitStageV1(t *testing.T) {
	attribution := m8ProductionAttributionV1{GlobalExactRecallAtK: 1, OracleStagesComplete: true, PrimaryHomeOracleRecallAtK: 1, FinalMembershipOracleRecallAtK: 1, ExhaustivePartitionRecallAtK: 1, ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: 1, EndToEndRecallAtK: 1}
	for _, stage := range m8AttributionStageOwnersV1(attribution) {
		if stage.Owner == "partition_membership_or_score_contract" && stage.Active {
			return
		}
	}
	t.Fatalf("stages=%+v", m8AttributionStageOwnersV1(attribution))
}

func TestM8FinalMembershipDiagnosticsRetainsRankAndCoverageV1(t *testing.T) {
	truth := []m8CanonicalResultV1{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	coverage, distinct, colocated, overlapOnly, duplicated, retained, err := m8TruthFinalMembershipDiagnosticsV1(truth, []uint32{0, 1}, map[string][]uint32{"a": {0, 1}, "b": {2}, "c": {1, 3}}, map[string]uint32{"a": 0, "b": 2, "c": 3})
	if err != nil || coverage != 2.0/3.0 || distinct != 4 || colocated != 1.0/3.0 || overlapOnly != 1.0/3.0 || duplicated != 1.0/3.0 || !slices.Equal(retained, []float64{1, 0, 1}) {
		t.Fatalf("coverage=%v distinct=%v colocated=%v overlap=%v duplicate=%v retained=%v err=%v", coverage, distinct, colocated, overlapOnly, duplicated, retained, err)
	}
	if _, _, _, _, _, _, err := m8TruthFinalMembershipDiagnosticsV1(truth, []uint32{0, 1}, map[string][]uint32{"a": {1, 0}, "b": {2}, "c": {1, 3}}, map[string]uint32{"a": 0, "b": 2, "c": 3}); err == nil || !strings.Contains(err.Error(), "noncanonical final memberships") {
		t.Fatalf("accepted unsorted final memberships: %v", err)
	}
}

func TestPartitionTruthOracleForArtifactV1(t *testing.T) {
	vectors := [][]float64{{1, 0}, {.99, .01}, {0, 1}, {-.9, .1}}
	graph := [][]int{{1, 2}, {0}, {0, 3}, {2}}
	oracle, err := partitionTruthOracleForArtifactV1(vectors, [][]float64{{1, 0}}, []int{0, 0, 1, 1}, graph, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if oracle.ProbeBudget != 1 || oracle.BestProbeCoverageAtK != 2.0/3.0 || oracle.TruthPrimaryHomePairColocate != 1.0/3.0 || len(oracle.Probes) != 2 || oracle.Probes[0].BestPrimaryHomeCoverageAtK != 2.0/3.0 || oracle.Graph.DirectedEdges != 6 || oracle.Graph.DirectedCutEdges != 2 || oracle.Graph.SymmetricEdges != 3 || oracle.Graph.SymmetricCutEdges != 1 || oracle.Graph.ApproximateGraphTruthNeighborEdges != 2 || oracle.Graph.RetainedApproximateGraphTruthNeighborEdges != 1 || !slices.Equal(oracle.Graph.PartitionLoads, []int{2, 2}) {
		t.Fatalf("oracle=%+v", oracle)
	}
	wide, err := partitionTruthOracleForArtifactV1(vectors, [][]float64{{1, 0}}, []int{0, 0, 1, 1}, graph, 12, 3)
	if err != nil {
		t.Fatal(err)
	}
	if wide.ProbeBudget != 3 || len(wide.Probes) != 6 || wide.Probes[2].ProbeBudget != 3 || wide.BestProbeCoverageAtK != 1 {
		t.Fatalf("derived quarter probe oracle=%+v", wide)
	}
	if _, err := partitionTruthOracleForArtifactV1(vectors, [][]float64{{1, 0}}, []int{0, 0, 1, 1}, graph, 2, len(vectors)+1); err == nil {
		t.Fatal("truth oracle accepted top-k above corpus size")
	}
	if _, err := partitionTruthOracleForArtifactV1(vectors, [][]float64{{1, 0}}, []int{0, 0, 1, 1}, [][]int{{2, 1}, {0}, {0, 3}, {2}}, 2, 3); err == nil || !strings.Contains(err.Error(), "invalid approximate graph edge") {
		t.Fatalf("truth oracle accepted unsorted graph adjacency: %v", err)
	}
}

func TestDuplicateAwareGraphBeatsStableHashTruthCoverageAtQuarterProbeBudget(t *testing.T) {
	const (
		partitions = 16
		classSize  = 16
		topK       = 10
	)
	vectors := make([][]float64, partitions*classSize)
	input := make([]vectorpartition.Vector, len(vectors))
	queries := make([][]float64, partitions)
	candidateID := 0
	for class := 0; class < partitions; class++ {
		values := make([]float64, partitions)
		values[class] = 1
		queries[class] = values
		for targetHashPartition := 0; targetHashPartition < classSize; targetHashPartition++ {
			ordinal := class*classSize + targetHashPartition
			var id string
			for {
				id = fmt.Sprintf("doc-%06d", candidateID)
				candidateID++
				sum := sha256.Sum256([]byte(id))
				if int(binary.BigEndian.Uint64(sum[:8])%uint64(partitions)) == targetHashPartition {
					break
				}
			}
			vectors[ordinal] = values
			input[ordinal] = vectorpartition.Vector{ID: id, Values: values}
		}
	}
	cfg := vectorpartition.DefaultConfig()
	cfg.Partitions, cfg.Repetitions, cfg.Pivots = partitions, 1, 4
	cfg.MaxLeafBucket, cfg.Degree = 8, 4
	cfg.MaxVectors, cfg.MaxEdges = len(vectors), len(vectors)*cfg.Degree
	graph, err := vectorpartition.Build(input, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := vectorpartition.BuildStableIDHashBaseline(graph)
	if err != nil {
		t.Fatal(err)
	}
	graphOracle, err := partitionTruthOracleForArtifactV1(vectors, queries, graph.Assignment, graph.Graph.Neighbors, partitions, topK)
	if err != nil {
		t.Fatal(err)
	}
	hashOracle, err := partitionTruthOracleForArtifactV1(vectors, queries, hash.Assignment, hash.Graph.Neighbors, partitions, topK)
	if err != nil {
		t.Fatal(err)
	}
	if graphOracle.ProbeBudget != 4 || graphOracle.BestProbeCoverageAtK < .90 || graphOracle.BestProbeCoverageAtK <= hashOracle.BestProbeCoverageAtK+.20 {
		t.Fatalf("quarter-probe coverage graph=%+v stable_hash=%+v", graphOracle, hashOracle)
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

func TestGenerateFixtureRejectsChecksumWorkBeforeWritingV1(t *testing.T) {
	out := filepath.Join(t.TempDir(), "refused")
	err := run([]string{"generate-fixture", "-out", out, "-vectors", "32", "-queries", "2", "-dimensions", "8", "-max-checksum-visits", "1"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum exact work") {
		t.Fatalf("generation cap err=%v", err)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("refused generation created output: %v", statErr)
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

func TestM6CoordinatorHarnessCloseReleasesCachedRouterSession(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("durable M1 lifecycle publication is unsupported")
	}
	_, vectors, queries := smallFixtureForTest(32, 1, 8)
	routerConfig := vectorpartition.DefaultRouterConfigV1()
	routerConfig.LeafSize = 1
	routerConfig.RepresentativesPerPartition = 2
	routerConfig.MaxVectors = len(vectors)
	router, err := newTreeDBRepresentativeRouter(vectors, 4, routerConfig, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := router.Close(); err != nil {
			t.Errorf("close backing router: %v", err)
		}
	})
	harness, err := newM6CoordinatorHarnessV1(router, vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.search(t.Context(), queries[0], 4, 10, 0); err != nil {
		t.Fatal(err)
	}
	sessions := harness.coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].ReaderPins != 1 || sessions[0].ReaderReleases != 0 ||
		sessions[0].Closes != 0 || sessions[0].LeasePins != sessions[0].LeaseReleases {
		t.Fatalf("open router session stats=%+v", sessions)
	}
	if err := harness.Close(); err != nil {
		t.Fatal(err)
	}
	sessions = harness.coordinator.Stats().RouterSessions
	if len(sessions) != 1 || sessions[0].ReaderPins != 1 || sessions[0].ReaderReleases != 1 ||
		sessions[0].Closes != 1 || sessions[0].LeasePins != sessions[0].LeaseReleases {
		t.Fatalf("closed router session stats=%+v", sessions)
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
		"-partition-hnsw-m", "16",
		"-partition-hnsw-ef-construction", "128",
		"-router-max-scalar-work", "50000000000",
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
	for name, mutate := range map[string]func(*m3PartitionIndexRow){
		"negative":       func(row *m3PartitionIndexRow) { row.PartitionLoads[0] = -1 },
		"over capacity":  func(row *m3PartitionIndexRow) { row.PartitionLoads[0] = row.Capacity + 1 },
		"total mismatch": func(row *m3PartitionIndexRow) { row.PartitionLoads[0]-- },
	} {
		t.Run("invalid loads "+name, func(t *testing.T) {
			candidate := report
			candidate.Rows = append([]m3PartitionIndexRow(nil), report.Rows...)
			candidate.Rows[0].PartitionLoads = append([]int(nil), report.Rows[0].PartitionLoads...)
			mutate(&candidate.Rows[0])
			if err := validateM3PartitionIndexReport(candidate); err == nil {
				t.Fatalf("accepted malformed loads %v", candidate.Rows[0].PartitionLoads)
			}
		})
	}
	for _, row := range report.Rows {
		if row.SourcePhysicalBytes <= 0 || row.PeakDerivedTemporaryBytes < row.FinalDerivedPhysicalBytes || row.FinalDerivedPhysicalBytes < int64(row.PackBytes) || row.PackBytes == 0 || row.PartitionHNSWM != partitionHNSWDegree || row.LocalSearches != 8*4 || row.SearchRoute != collections.VectorPartitionSearchRouteHNSWSearchPackV1 || row.MissingAssets != 0 || row.CorruptAssets != 0 || row.StaleAssets != 0 || row.ExactLocalRecallAtK <= 0 || row.EdgesPerOp <= 0 || len(row.OverlapReplicas) != row.OverlapRealized || len(row.OverlapDestinationDiversity) != len(row.PartitionLoads) {
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

func TestM3ConfiguredPartitionLocalHNSWBuildsProductionV3Packs(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("durable M1 lifecycle publication is unsupported; native pack codec coverage remains platform-neutral in TreeDB/collections")
	}
	dataset := writeFixtureForTest(t, 64, 8, 8)
	args := []string{
		"-dataset", dataset,
		"-out", t.TempDir(),
		"-partitions", "16",
		"-probes", "1",
		"-overlap", "0",
		"-top-k", "4",
		"-stage", "overlap,partition_index",
		"-partition-repetitions", "1",
		"-partition-pivots", "2",
		"-partition-max-leaf-bucket", "8",
		"-partition-degree", "4",
		"-partition-hnsw-m", "16",
		"-partition-hnsw-ef-construction", "128",
		"-router-max-scalar-work", "50000000000",
	}
	type builtM3 struct {
		descriptor m3VariantDescriptorV1
		source     collections.VectorPartitionSourceIdentityV1
		rows       []collections.VectorPartitionSourceOrdinalV1
		manifest   collections.VectorPartitionManifestV1
		routes     [][]uint32
	}
	queries := [][]float32{make([]float32, 8), make([]float32, 8)}
	queries[0][0], queries[1][1] = 1, 1
	build := func(name string, localArgs ...string) builtM3 {
		persist := filepath.Join(t.TempDir(), name)
		runArgs := append(append([]string(nil), args...), "-m3-persist-db", persist)
		runArgs = append(runArgs, localArgs...)
		if err := runWithHermeticProvenance(t, runArgs, io.Discard); err != nil {
			t.Fatal(err)
		}
		db, err := backenddb.Open(backenddb.Options{Dir: persist, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		col, err := collections.NewCollectionManager(db).OpenCollection(m3BenchmarkCollection)
		if err != nil {
			t.Fatal(err)
		}
		meta := col.Meta()
		if len(meta.VectorIndexes) != 1 || meta.VectorIndexes[0].M != partitionHNSWDegree || meta.VectorIndexes[0].EfConstruction != partitionHNSWDefaultEfC {
			t.Fatalf("source index definition=%+v", meta.VectorIndexes)
		}
		descriptor, err := m3ReadVariantDescriptorV1(persist)
		if err != nil {
			t.Fatal(err)
		}
		if descriptor.IndexDefinitionDigest != collections.VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]) {
			t.Fatalf("descriptor source index digest=%s", descriptor.IndexDefinitionDigest)
		}
		searcher, err := col.OpenVectorPartitionLocalSearcherForGenerationV1(partitionHNSWIndex, descriptor.PartitionGeneration, 0)
		if err != nil {
			t.Fatalf("production-open configured V3 pack: %v", err)
		}
		if err := searcher.Close(); err != nil {
			t.Fatal(err)
		}
		source, rows, err := col.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
		if err != nil {
			t.Fatal(err)
		}
		router, _, err := col.OpenVectorPartitionRouterV1(partitionHNSWIndex)
		if err != nil {
			t.Fatal(err)
		}
		routerStatus := router.Status()
		routes := make([][]uint32, 0, len(queries)*4)
		for _, query := range queries {
			for _, options := range []collections.VectorPartitionRouterSearchOptionsV1{
				{Mode: collections.VectorPartitionRouterModeExactV1, CandidateBudget: int(routerStatus.Representatives), PartitionProbes: 2},
				{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: int(routerStatus.Representatives), PartitionProbes: 2},
				{Mode: collections.VectorPartitionRouterModeExactV1, CandidateBudget: int(routerStatus.Representatives), PartitionProbes: 16},
				{Mode: collections.VectorPartitionRouterModeApproxV1, CandidateBudget: int(routerStatus.Representatives), PartitionProbes: 16},
			} {
				result, err := router.Search(query, options)
				if err != nil {
					t.Fatal(err)
				}
				route := make([]uint32, len(result.Partitions))
				for i := range result.Partitions {
					route[i] = result.Partitions[i].PartitionID
				}
				routes = append(routes, route)
			}
		}
		if err := router.Close(); err != nil {
			t.Fatal(err)
		}
		return builtM3{descriptor: descriptor, source: source, rows: rows, manifest: routerStatus.Manifest, routes: routes}
	}
	baseline := build("m16")
	candidate := build("m18", "-partition-hnsw-m", "18", "-partition-hnsw-ef-construction", "256")
	if baseline.descriptor.PartitionHNSWM != 16 || m3DescriptorPartitionHNSWEfCV1(baseline.descriptor) != 128 || candidate.descriptor.PartitionHNSWM != 18 || m3DescriptorPartitionHNSWEfCV1(candidate.descriptor) != 256 || baseline.descriptor.IndexDefinitionDigest != candidate.descriptor.IndexDefinitionDigest || baseline.descriptor.Source != candidate.descriptor.Source || !m8SHA256V1(baseline.descriptor.SourceOrdinalDigest) || baseline.descriptor.SourceOrdinalDigest != candidate.descriptor.SourceOrdinalDigest || baseline.descriptor.RouterAssetChecksum != candidate.descriptor.RouterAssetChecksum || baseline.descriptor.RouterModelDigest != candidate.descriptor.RouterModelDigest || baseline.descriptor.BuildIdentityDigest == candidate.descriptor.BuildIdentityDigest || baseline.source != candidate.source || !slices.Equal(baseline.rows, candidate.rows) || !slices.Equal(baseline.manifest.Memberships, candidate.manifest.Memberships) || !slices.Equal(baseline.manifest.OverlapMemberships, candidate.manifest.OverlapMemberships) || !slices.Equal(baseline.manifest.Placements, candidate.manifest.Placements) || !slices.Equal(baseline.manifest.Representatives, candidate.manifest.Representatives) || !slices.EqualFunc(baseline.routes, candidate.routes, slices.Equal[[]uint32]) {
		t.Fatalf("source/router/local construction drift baseline=%+v candidate=%+v", baseline.descriptor, candidate.descriptor)
	}
	packChecksumChanged := false
	for i := range baseline.manifest.Assets {
		if baseline.manifest.Assets[i].MembershipDigest == candidate.manifest.Assets[i].MembershipDigest {
			t.Fatalf("partition=%d retained canonical local membership identity", i)
		}
		packChecksumChanged = packChecksumChanged || baseline.manifest.Assets[i].Checksum != candidate.manifest.Assets[i].Checksum
	}
	if !packChecksumChanged {
		t.Fatal("local pack checksums did not change")
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
	overlap := vectorpartition.OverlapResult{Memberships: []vectorpartition.Membership{
		{VectorOrdinal: 0, Partition: 1, Home: true}, {VectorOrdinal: 1, Partition: 0, Home: true}, {VectorOrdinal: 1, Partition: 1}, {VectorOrdinal: 2, Partition: 1, Home: true},
	}}
	partitions, err := m3RouterPartitions(
		artifact,
		overlap,
		[]int{2, 0, 1},
		[][]float64{{1, 2}, {3, 4}, {5, 6}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(partitions) != 2 || partitions[0].PartitionID != 0 || partitions[1].PartitionID != 1 ||
		len(partitions[0].Vectors) != 1 || len(partitions[1].Vectors) != 3 {
		t.Fatalf("router partitions=%+v", partitions)
	}
	if got := partitions[0].Vectors[0]; got.Ordinal != 0 || len(got.Values) != 2 || got.Values[0] != 3 || got.Values[1] != 4 {
		t.Fatalf("partition 0 vector=%+v", got)
	}
	if got := partitions[1].Vectors[0]; got.Ordinal != 2 || got.Values[0] != 1 || got.Values[1] != 2 {
		t.Fatalf("partition 1 first vector=%+v", got)
	}
	if got := partitions[1].Vectors[1]; got.Ordinal != 0 || got.Values[0] != 3 || got.Values[1] != 4 || got.MembershipKind != string(collections.VectorPartitionMembershipOverlapV1) {
		t.Fatalf("partition 1 overlap vector=%+v", got)
	}
	if got := partitions[1].Vectors[2]; got.Ordinal != 1 || got.Values[0] != 5 || got.Values[1] != 6 {
		t.Fatalf("partition 1 third vector=%+v", got)
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
	// Required exact-duplicate links intentionally change the frozen graph
	// artifact while retaining the graph-vs-hash cut advantage.
	// The artifact digest changes because MaxDistanceWork is now serialized as
	// deliberate construction provenance; graph quality metrics do not change.
	if report.Dataset.Checksum != "2413ef7c2f65a4b5ce8ecc3846f473fd85d337a87511538f962af7cdf6aec291" || report.Source.Checksum != "6515025f540b955d453de99cf13f1efc002fd91135b2745b722c19e8d736e386" || report.ArtifactSHA256 != "17ddb6cda87306b148ae4aceee91a2ed99671fb78cbc1905eb2050f8ec73f8aa" || report.Metrics.EdgeCut != 4928 || report.Metrics.StableIDHashEdgeCut != 149873 {
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

func TestPartitionTruthOraclePreflightsQueriesBytesAndExactWork(t *testing.T) {
	manifest := fixtureManifest{
		SchemaVersion: schemaVersion, Fixture: "partition-truth-preflight",
		Generator: fixtureGenerator, Arithmetic: fixtureArithmetic,
		Vectors: 50, Queries: 3, Dimensions: 2, Metric: "cosine",
		Checksum: strings.Repeat("0", 64),
	}
	if err := validatePartitionTruthOracleWithCaps(manifest, 16, 10, 16, 100, 7090, 53*2*8); err != nil {
		t.Fatalf("valid truth-oracle plan rejected: %v", err)
	}
	queryHeavy := manifest
	queryHeavy.Queries = 101
	if err := validatePartitionTruthOracleWithCaps(queryHeavy, 16, 10, 16, 100, math.MaxInt64, math.MaxInt64); err == nil {
		t.Fatal("partition truth oracle accepted query count above the pre-allocation cap")
	}
	if err := validatePartitionTruthOracleWithCaps(manifest, 16, 10, 16, 100, math.MaxInt64, 53*2*8-1); err == nil {
		t.Fatal("partition truth oracle accepted query matrix above the byte cap")
	}
	if err := validatePartitionTruthOracleWithCaps(manifest, 16, 10, 16, 100, 7089, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "partition_ranking_work=1155") {
		t.Fatalf("partition truth oracle accepted excessive aggregate work: %v", err)
	}
	if err := validatePartitionTruthOracleWithCaps(fixtureManifest{SchemaVersion: schemaVersion, Fixture: "large-top-k", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 1000, Queries: 200000, Dimensions: 1, Metric: "cosine", Checksum: strings.Repeat("0", 64)}, 16, 1000, 500, 200000, maxBenchmarkWorkUnits, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "adjacency_comparisons=") {
		t.Fatalf("partition truth oracle accepted unbounded adjacency scans: %v", err)
	}
	if err := validatePartitionTruthOracleWithCaps(fixtureManifest{SchemaVersion: schemaVersion, Fixture: "partition-sort-heavy", Generator: fixtureGenerator, Arithmetic: fixtureArithmetic, Vectors: 10000, Queries: 18000, Dimensions: 1, Metric: "cosine", Checksum: strings.Repeat("0", 64)}, 10000, 1000, 1, 20000, maxBenchmarkWorkUnits, math.MaxInt64); err == nil || !strings.Contains(err.Error(), "partition_ranking_work=") {
		t.Fatalf("partition truth oracle accepted unbounded partition ranking: %v", err)
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

func TestPartitionLocalHNSWStageIsRecallQualifiedNotExact(t *testing.T) {
	m, vectors, queries := smallFixtureForTest(32, 4, 8)
	hnsw, err := newTreeDBPartitionHNSW(vectors, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := hnsw.Close(); err != nil {
			t.Errorf("close partition-local HNSW: %v", err)
		}
	})
	r, err := simulate(config{
		partitions:   4,
		topK:         4,
		recallTarget: .9,
		seed:         1,
		stages:       stageSet("treedb_partition_local_hnsw"),
		hnsw:         hnsw,
	}, m, vectors, queries, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Stages) != 1 {
		t.Fatalf("stages=%+v", r.Stages)
	}
	stage := r.Stages[0]
	if !stage.Lossy || stage.Method != "treedb_column_graph_hnsw_recall_qualified_v1" {
		t.Fatalf("partition-local HNSW must remain recall-qualified, got %+v", stage)
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

func TestPartitionAssignmentParsesOnlyMaterializationStagesV1(t *testing.T) {
	base := []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-assignment", partitionAssignmentStableIDHashV1}
	cfg, err := parseConfig(base)
	if err != nil || cfg.partitionAssignment != partitionAssignmentStableIDHashV1 {
		t.Fatalf("stable assignment config=%+v err=%v", cfg, err)
	}
	for _, args := range [][]string{
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0.2", "-partition-assignment", partitionAssignmentStableIDHashV1},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "simulation", "-probes", "1", "-partition-assignment", partitionAssignmentStableIDHashV1},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "partition", "-partition-assignment", "unknown"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted malformed assignment config %#v", args)
		}
	}
}

func TestKaHIPOfflineSelectorIsLimitedToGraphMaterializationV1(t *testing.T) {
	python := filepath.Join(t.TempDir(), "kahip-python")
	if err := os.WriteFile(python, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	adapterSource, err := filepath.Abs(filepath.Join("..", "..", "scripts", "treedb_kahip_partition.py"))
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := os.ReadFile(adapterSource)
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "kahip.py")
	if err := os.WriteFile(script, adapter, 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-partition-kahip-python", python, "-partition-kahip-script", script}
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		t.Run("unreadable_script", func(t *testing.T) {
			unreadable := filepath.Join(t.TempDir(), "unreadable.py")
			if err := os.WriteFile(unreadable, adapter, 0o600); err != nil {
				t.Fatal(err)
			}
			args := append([]string(nil), base...)
			args[len(args)-1] = unreadable
			args = append(args, "-stage", "partition")
			if _, err := parseConfig(args); err != nil {
				t.Fatalf("readable KaHIP adapter rejected: %v", err)
			}
			if err := os.Chmod(unreadable, 0); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(unreadable)
			if err == nil {
				_ = file.Close()
				t.Skip("current credentials can read a mode-000 regular file")
			}
			if _, err := parseConfig(args); err == nil {
				t.Fatal("accepted unreadable KaHIP adapter")
			}
		})
	}
	for _, stage := range []string{"partition", "overlap,partition_index"} {
		cfg, err := parseConfig(append(append([]string(nil), base...), "-stage", stage))
		wantPythonSHA, hashErr := m8BenchmarkExecutableSHA256V1(python)
		if err != nil || hashErr != nil || cfg.kahipPython != python || cfg.kahipPythonSHA256 != wantPythonSHA || cfg.kahipScript != script || cfg.kahipSource != string(adapter) || cfg.kahipAdapterSHA256 != kahipAdapterSHA256 || cfg.kahipTimeout != kahipDefaultTimeout {
			t.Fatalf("stage=%s cfg=%+v err=%v", stage, cfg, err)
		}
		if command := kahipAdapterCommand(cfg); len(command) != 3 || command[0] != python || command[1] != "-c" || command[2] != string(adapter) {
			t.Fatalf("KaHIP command=%q", command)
		}
	}
	for _, args := range [][]string{
		append(append([]string(nil), base...), "-stage", "simulation"),
		append(append([]string(nil), base...), "-stage", "partition", "-partition-assignment", partitionAssignmentStableIDHashV1),
		append(append([]string(nil), base...), "-stage", "partition", "-imbalance", "0.04"),
		append(append([]string(nil), base...), "-stage", "partition", "-partition-kahip-python", filepath.Join(t.TempDir(), "missing-python")),
		append(append([]string(nil), base...), "-stage", "partition", "-partition-kahip-script", t.TempDir()),
		append(append([]string(nil), base...), "-stage", "partition", "-partition-kahip-timeout", "0s"),
		append(append([]string(nil), base...), "-stage", "partition", "-seed", "2147483648"),
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted KaHIP outside graph materialization: %#v", args)
		}
	}
	configured, err := parseConfig(append(append([]string(nil), base...), "-stage", "partition", "-partition-kahip-timeout", "45m"))
	if err != nil || configured.kahipTimeout != 45*time.Minute {
		t.Fatalf("KaHIP timeout config=%+v err=%v", configured, err)
	}
	tampered := filepath.Join(t.TempDir(), "kahip-tampered.py")
	if err := os.WriteFile(tampered, append(adapter, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	args := append([]string(nil), base...)
	args[len(args)-1] = tampered
	args = append(args, "-stage", "partition")
	if _, err := parseConfig(args); err == nil {
		t.Fatal("accepted modified KaHIP adapter")
	}
}

func TestKaHIPOutputCapAllowsCanonicalLabelGrowthV1(t *testing.T) {
	a := vectorpartition.Artifact{IDs: make([]string, 1_000_000)}
	if got, want := kahipOutputCap(make([]byte, 10), a), 5_001_034; got != want {
		t.Fatalf("cap=%d want=%d", got, want)
	}
}

func TestKaHIPFinalGraphEnvelopePreflightsBeforeBuildV1(t *testing.T) {
	manifest := fixtureManifest{
		SchemaVersion: schemaVersion, Fixture: "kahip-envelope", Generator: fixtureGenerator,
		Arithmetic: fixtureArithmetic, Vectors: 250_001, Queries: 1, Dimensions: 1,
		Metric: "cosine", Seed: 1, Checksum: strings.Repeat("0", 64),
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	dataset := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataset, "fixture_manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(t.TempDir(), "kahip-python")
	if err := os.WriteFile(python, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "treedb_kahip_partition.py"))
	if err != nil {
		t.Fatal(err)
	}
	err = runWithHermeticProvenance(t, []string{
		"-dataset", dataset, "-out", t.TempDir(), "-stage", "partition", "-partitions", "1",
		"-partition-degree", "64", "-partition-repetitions", "1",
		"-partition-kahip-python", python, "-partition-kahip-script", script,
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "KaHIP final directed-edge envelope") {
		t.Fatalf("over-envelope KaHIP fixture reached build: %v", err)
	}
	if err := validateKaHIPFinalGraphEnvelopeV1(250_000, 64); err != nil {
		t.Fatalf("16M degree-64 KaHIP shape rejected: %v", err)
	}
}

func TestKaHIPAdapterRoundTripV1(t *testing.T) {
	python := os.Getenv("TREEDB_KAHIP_PYTHON")
	if python == "" {
		t.Skip("set TREEDB_KAHIP_PYTHON to run the pinned offline KaHIP adapter")
	}
	cfg := vectorpartition.DefaultConfig()
	cfg.Partitions, cfg.Pivots, cfg.MaxLeafBucket, cfg.Degree, cfg.Repetitions, cfg.MaxVectors, cfg.MaxEdges = 2, 2, 2, 2, 1, 16, 32
	v := []vectorpartition.Vector{{ID: "a", Values: []float64{1, 0}}, {ID: "b", Values: []float64{.99, .01}}, {ID: "c", Values: []float64{0, 1}}, {ID: "d", Values: []float64{.01, .99}}}
	request, err := vectorpartition.Build(v, cfg)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := vectorpartition.CanonicalJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "scripts", "treedb_kahip_partition.py"))
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	got, err := vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{python, "-c", string(adapter)}, raw, vectorpartition.ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) + 1024}, request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Backend != "kahip_python_3.25_eco_symmetrized_v1_seed_1" || got.Metrics.MaxPartitionSize > got.Metrics.Cap {
		t.Fatalf("invalid KaHIP artifact: %+v", got)
	}
	wrongRecord := strings.Replace(string(adapter), "RECORD_SHA256 = \"7ff011253147286fcebc9185573662bf31dbcfbab1944f9b4940032f49ea5217\"", "RECORD_SHA256 = \"0000000000000000000000000000000000000000000000000000000000000000\"", 1)
	if wrongRecord == string(adapter) {
		t.Fatal("test did not replace pinned KaHIP distribution identity")
	}
	badScript := filepath.Join(t.TempDir(), "kahip_wrong_record.py")
	if err := os.WriteFile(badScript, []byte(wrongRecord), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{python, badScript}, raw, vectorpartition.ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) + 1024}, request)
	if err == nil || !strings.Contains(err.Error(), "pinned kahip") {
		t.Fatalf("same-version wrong distribution identity accepted: %v", err)
	}
	badPayload := strings.Replace(string(adapter), "payload = distribution.locate_file(path).read_bytes()", "payload = distribution.locate_file(path).read_bytes() + b\"x\"", 1)
	if badPayload == string(adapter) {
		t.Fatal("test did not replace KaHIP payload integrity check")
	}
	badScript = filepath.Join(t.TempDir(), "kahip_wrong_payload.py")
	if err := os.WriteFile(badScript, []byte(badPayload), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{python, badScript}, raw, vectorpartition.ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) + 1024}, request)
	if err == nil || !strings.Contains(err.Error(), "payload integrity") {
		t.Fatalf("tampered installed payload accepted: %v", err)
	}
	for _, forgedCase := range []struct {
		name, want string
		partitions int
	}{
		{"partition_cap", "configuration mismatch", 16_385},
		{"more_partitions_than_nodes", "invalid graph", len(request.IDs) + 1},
	} {
		var forged map[string]any
		if err := json.Unmarshal(raw, &forged); err != nil {
			t.Fatal(err)
		}
		forged["config"].(map[string]any)["partitions"] = forgedCase.partitions
		forgedRaw, err := json.Marshal(forged)
		if err != nil {
			t.Fatal(err)
		}
		input := filepath.Join(t.TempDir(), forgedCase.name+".json")
		if err := os.WriteFile(input, forgedRaw, 0o600); err != nil {
			t.Fatal(err)
		}
		output, err := exec.CommandContext(ctx, python, script, input, filepath.Join(t.TempDir(), "out.json")).CombinedOutput()
		if err == nil || !bytes.Contains(output, []byte(forgedCase.want)) {
			t.Fatalf("forged %s request reached KaHIP: err=%v output=%s", forgedCase.name, err, output)
		}
	}
	var forgedSeed map[string]any
	if err := json.Unmarshal(raw, &forgedSeed); err != nil {
		t.Fatal(err)
	}
	forgedSeed["config"].(map[string]any)["seed"] = int64(2_147_483_648)
	forgedRaw, err := json.Marshal(forgedSeed)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "seed_out_of_range.json")
	if err := os.WriteFile(input, forgedRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(ctx, python, script, input, filepath.Join(t.TempDir(), "out.json")).CombinedOutput()
	if err == nil || !bytes.Contains(output, []byte("configuration mismatch")) {
		t.Fatalf("out-of-range KaHIP seed reached native call: err=%v output=%s", err, output)
	}
	shadowDir := t.TempDir()
	shadowMarker := filepath.Join(shadowDir, "loaded")
	if err := os.WriteFile(filepath.Join(shadowDir, "kahip.py"), []byte("open(__import__('os').environ['KAHIP_SHADOW_MARKER'], 'w').write('loaded')\nraise RuntimeError('shadow imported')\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PYTHONPATH", shadowDir)
	t.Setenv("KAHIP_SHADOW_MARKER", shadowMarker)
	got, err = vectorpartition.RunExternalJSONForRequestWithLimits(ctx, []string{python, script}, raw, vectorpartition.ExternalJSONLimits{MaxInput: len(raw), MaxOutput: len(raw) + 1024}, request)
	if err != nil || got.Backend != "kahip_python_3.25_eco_symmetrized_v1_seed_1" {
		t.Fatalf("verified KaHIP adapter failed with shadow module: artifact=%+v err=%v", got, err)
	}
	if _, err := os.Stat(shadowMarker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shadow kahip module was imported: %v", err)
	}
}

func TestPartitionLocalHNSWConfigIsIndependentAndM3OnlyV1(t *testing.T) {
	base := []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-degree", "4", "-partition-hnsw-m", "18", "-partition-hnsw-ef-construction", "256"}
	cfg, err := parseConfig(base)
	if err != nil || cfg.partition.Degree != 4 || cfg.partitionHNSWM != 18 || cfg.partitionHNSWEfC != 256 {
		t.Fatalf("partition config=%+v err=%v", cfg, err)
	}
	if m, efConstruction, err := m3PartitionLocalHNSWConfigV1(cfg); err != nil || m != 18 || efConstruction != 256 {
		t.Fatalf("configured local HNSW M/eFC=%d/%d err=%v", m, efConstruction, err)
	}
	defaultCfg, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0"})
	if err != nil {
		t.Fatal(err)
	}
	if m, efConstruction, err := m3PartitionLocalHNSWConfigV1(defaultCfg); err != nil || m != partitionHNSWDegree || efConstruction != partitionHNSWDefaultEfC {
		t.Fatalf("default local HNSW M/eFC=%d/%d err=%v", m, efConstruction, err)
	}
	if variant, err := m3PartitionLocalGraphVariantV1(18, 256); err != nil || variant != collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1 {
		t.Fatalf("M18/eFC256 local variant=%q err=%v", variant, err)
	}
	if variant, err := m3PartitionLocalGraphVariantV1(20, 256); err != nil || variant != collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM20EfConstruction256V1 {
		t.Fatalf("M20/eFC256 local variant=%q err=%v", variant, err)
	}
	if _, err := m3PartitionLocalGraphVariantV1(22, 256); err == nil {
		t.Fatal("accepted unselected local construction variant")
	}
	router := m3RouterBuildOptionsV1(cfg.routerConfig, 1, 2)
	if router.M != partitionHNSWDegree || router.EfConstruction != partitionHNSWDefaultEfC || router.EfSearch != 128 {
		t.Fatalf("router HNSW M/eFC/search=%d/%d/%d", router.M, router.EfConstruction, router.EfSearch)
	}
	for _, args := range [][]string{
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "partition", "-partition-hnsw-m", "16"},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-hnsw-m", "1"},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-hnsw-m", "33"},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-hnsw-m", "18", "-partition-hnsw-ef-construction", "17"},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "overlap,partition_index", "-overlap", "0", "-partition-hnsw-ef-construction", "4097"},
		{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-stage", "partition", "-partition-hnsw-ef-construction", "256"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted malformed local HNSW config %#v", args)
		}
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
		fmt.Sprint(cfg.concurrency) != "[1 16 64]" || cfg.warmup != 3 || fmt.Sprint(cfg.efSearch) != "[64 4096]" || cfg.routerCandidates != 64 || cfg.m8ExistingDB != "/retained/m8-assets" {
		t.Fatalf("M8 config=%+v", cfg)
	}
	limit := nativewire.DefaultVectorPartitionCoordinatorLimitsV1().MaxSelectedPartitions
	boundary, err := parseConfig([]string{
		"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(),
		"-partitions", strconv.Itoa(limit), "-raft-groups", "2",
	})
	if err != nil {
		t.Fatalf("rejected M8 coordinator partition boundary %d: %v", limit, err)
	}
	if boundary.routerCandidates != limit {
		t.Fatalf("M8 coordinator partition boundary candidates=%d want %d", boundary.routerCandidates, limit)
	}
	for _, args := range [][]string{
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4", "-raft-nodes-per-group", "2"},
		{"-mode", m8ProductionMultiGroupModeV1, "-stage", "router", "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"},
		{"-stage", "router", "-m8-existing-db", "/retained/m8-assets", "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-probes", "1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4", "-warmup", "-1"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", strconv.Itoa(limit + 1), "-raft-groups", "2"},
		{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4", "-probes", "1,4", "-router-candidates", "2"},
	} {
		if _, err := parseConfig(args); err == nil {
			t.Fatalf("accepted malformed M8 config %#v", args)
		}
	}
}

func TestM8BenchmarkWorkCapAndOverflowV1(t *testing.T) {
	cfg := config{partitions: 4, overlaps: []float64{0, .2}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, warmup: 3, topK: 2}
	manifest := fixtureManifest{Vectors: 10, Queries: 5, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, 1644, math.MaxInt64)
	if err != nil || plan.QueryRequests != 129 || plan.MeasuredQueryRequests != 40 || plan.WarmupAndPreflightQueryRequests != 4 || plan.AttributionQueryPasses != 85 {
		t.Fatalf("M8 work plan=%+v err=%v", plan, err)
	}
	if plan.SelectedPartitionSetupWorkUnits != 100 || plan.AttributionLinearWorkUnits != 1424 || plan.FinalMembershipLinearScans != 80 || plan.FinalMembershipPairComparisons != 40 || plan.AttributionDiagnosticWorkUnits != 1644 || plan.RetainedCoordinatorCells != 8 || plan.RetainedCoordinatorResults != 80 || plan.CurrentCellOutcomes != 5 || plan.CurrentCellOutcomeBytes == 0 || plan.FixtureResidentBytes == 0 || plan.SourceSnapshotBytes == 0 || plan.ExactTruthBytes == 0 || plan.RetainedCoordinatorBytes == 0 || plan.RetainedAttributionMatrices != 5 || plan.RetainedAttributionResults != 50 || plan.RetainedAttributionBytes == 0 || plan.AttributionMergeScratchResults != 16 || plan.AttributionMergeScratchBytes == 0 || plan.AttributionLiveResultSets != 3 || plan.AttributionLiveResultBytes == 0 || plan.AttributionApproximateRouteBytes == 0 || plan.ModeledPeakBytes == 0 {
		t.Fatalf("incomplete M8 memory plan=%+v", plan)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, 1643, math.MaxInt64); err == nil {
		t.Fatal("accepted oversized M8 sweep")
	}
	if _, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 1, Queries: math.MaxInt, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64); err == nil {
		t.Fatal("accepted overflowing M8 sweep")
	}
	if _, err := validateM8BenchmarkWork(config{partitions: 1, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}, fixtureManifest{Vectors: 1, Queries: math.MaxInt, Dimensions: 1}, maxBenchmarkWorkUnits, math.MaxInt64); err == nil {
		t.Fatal("accepted overflowing M8 preflight accounting")
	}
}

func TestM8FinalMembershipLinearScansArePreflightedV1(t *testing.T) {
	efSearch := make([]int, 100)
	for i := range efSearch {
		efSearch[i] = 64
	}
	cfg := config{partitions: 16, overlaps: []float64{.2}, probes: []int{1}, efSearch: efSearch, concurrency: []int{1}, topK: 1, m8ExistingDB: "/retained/overlap", m8MaxExactTruthVisits: math.MaxInt64}
	_, err := validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 1, Queries: 200_000, Dimensions: 1}, 300_000_000, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "linear_membership_scans=680000000") {
		t.Fatalf("accepted uncharged final-membership scans: %v", err)
	}
	cfg = config{partitions: 16, overlaps: []float64{0}, probes: []int{16}, efSearch: efSearch, concurrency: []int{1}, topK: 1, m8MaxExactTruthVisits: math.MaxInt64}
	_, err = validateM8BenchmarkWork(cfg, fixtureManifest{Vectors: 1, Queries: 200_000, Dimensions: 1}, 300_000_000, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "selected_partition_setup=640000000") {
		t.Fatalf("accepted uncharged selected-partition setup: %v", err)
	}
}

func TestM8UnsupportedOverlapSkipsMeasuredAndAttributionWorkV1(t *testing.T) {
	cfg := config{partitions: 4, overlaps: []float64{.2}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, warmup: 3, topK: 2, m8MaxExactTruthVisits: math.MaxInt64}
	manifest := fixtureManifest{Vectors: 10, Queries: 5, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, 4, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.QueryRequests != 4 || plan.MeasuredQueryRequests != 0 || plan.WarmupAndPreflightQueryRequests != 4 || plan.AttributionQueryPasses != 0 || plan.RetainedCoordinatorCells != 0 || plan.RetainedCoordinatorResults != 0 || plan.CurrentCellOutcomes != 2 || plan.CurrentCellOutcomeBytes != 1444 || plan.CurrentQueryConversionBytes != 64 || plan.RetainedAttributionMatrices != 0 || plan.RetainedAttributionResults != 0 || plan.RetainedAttributionBytes != 0 || plan.AttributionMergeScratchResults != 0 || plan.AttributionMergeScratchBytes != 0 {
		t.Fatalf("unsupported-only M8 work plan=%+v", plan)
	}
}

func TestM8RetainedOverlapCountsMeasuredAndAttributionWorkV1(t *testing.T) {
	cfg := config{partitions: 4, overlaps: []float64{.2}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, warmup: 3, topK: 2, m8ExistingDB: "/retained/overlap"}
	manifest := fixtureManifest{Vectors: 10, Queries: 5, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, 3564, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.QueryRequests != 129 || plan.MeasuredQueryRequests != 40 || plan.WarmupAndPreflightQueryRequests != 4 || plan.AttributionQueryPasses != 85 || plan.SelectedPartitionSetupWorkUnits != 100 || plan.AttributionLinearWorkUnits != 1424 || plan.FinalMembershipLinearScans != 1360 || plan.FinalMembershipPairComparisons != 680 || plan.AttributionDiagnosticWorkUnits != 3564 {
		t.Fatalf("retained-overlap M8 work plan=%+v", plan)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, 3563, math.MaxInt64); err == nil {
		t.Fatal("accepted retained overlap above complete work cap")
	}
}

func TestM8VariantMatrixCountsCompleteChildWorkAndOneChildPeakV1(t *testing.T) {
	cfg := config{partitions: 4, overlaps: []float64{0}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, warmup: 3, topK: 2}
	manifest := fixtureManifest{Vectors: 10, Queries: 5, Dimensions: 8}
	single, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	cfg.m8VariantDBs = []string{"/a", "/b", "/c"}
	matrix, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.MeasuredQueryRequests != single.MeasuredQueryRequests*3 || matrix.WarmupAndPreflightQueryRequests != single.WarmupAndPreflightQueryRequests*3 || matrix.AttributionQueryPasses != single.AttributionQueryPasses*3 || matrix.QueryRequests != single.QueryRequests*3 {
		t.Fatalf("matrix cumulative work=%+v single=%+v", matrix, single)
	}
	if matrix.ExactWorkVectorVisits != single.ExactWorkVectorVisits*3 {
		t.Fatalf("matrix exact scans=%d single=%d", matrix.ExactWorkVectorVisits, single.ExactWorkVectorVisits)
	}
	if single.SelectedPartitionSetupWorkUnits != 100 || single.AttributionLinearWorkUnits != 1424 || single.FinalMembershipLinearScans != 80 || single.FinalMembershipPairComparisons != 40 || single.AttributionDiagnosticWorkUnits != 1644 || matrix.SelectedPartitionSetupWorkUnits != 300 || matrix.AttributionLinearWorkUnits != 4272 || matrix.FinalMembershipLinearScans != 4080 || matrix.FinalMembershipPairComparisons != 2040 || matrix.AttributionDiagnosticWorkUnits != 10692 {
		t.Fatalf("matrix final-membership diagnostics=%+v single=%+v", matrix, single)
	}
	if matrix.RetainedCoordinatorCells != single.RetainedCoordinatorCells || matrix.RetainedCoordinatorResults != single.RetainedCoordinatorResults || matrix.RetainedAttributionResults != single.RetainedAttributionResults || matrix.ModeledPeakBytes != single.ModeledPeakBytes {
		t.Fatalf("matrix peak inflated matrix=%+v single=%+v", matrix, single)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, matrix.QueryRequests-1, math.MaxInt64); err == nil {
		t.Fatal("accepted matrix above cumulative complete-child work cap")
	}
}

func TestM8RetainedAttributionResultsRespectMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 16, overlaps: []float64{0}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1}, topK: 256, m8MaxExactTruthVisits: math.MaxInt64}
	manifest := fixtureManifest{Vectors: 10_000, Queries: 50_000, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	measurementBytes, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorBytes)
	if err != nil {
		t.Fatal(err)
	}
	measurementPeak, err := memoryScaleCeil(measurementBytes, memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		t.Fatal(err)
	}
	if measurementPeak >= maxFixtureBytes || plan.RetainedAttributionMatrices != 5 || plan.RetainedAttributionResults != 64_000_000 || plan.RetainedAttributionBytes < 2_500_000_000 || plan.ModeledPeakBytes <= maxFixtureBytes {
		t.Fatalf("M8 attribution retention plan=%+v measurement_peak=%d", plan, measurementPeak)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, maxFixtureBytes); err == nil || !strings.Contains(err.Error(), "retained_attribution_results=64000000") {
		t.Fatalf("accepted oversized retained attribution sweep: %v", err)
	}
}

func TestM8AttributionMergeScratchRespectsMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 256, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 256}
	manifest := fixtureManifest{Vectors: 256, Queries: 1, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	withoutScratch, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorBytes, plan.RetainedAttributionBytes)
	if err != nil {
		t.Fatal(err)
	}
	sourceOracle, err := memoryAdd(plan.FixtureResidentBytes, plan.SourceSnapshotBytes, plan.ExactTruthBytes)
	if err != nil {
		t.Fatal(err)
	}
	withoutScratch, err = memoryScaleCeil(max(sourceOracle, withoutScratch), memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		t.Fatal(err)
	}
	resultStructBytes, err := memoryMul(plan.AttributionMergeScratchResults, int64(unsafe.Sizeof(m8CanonicalResultV1{})))
	if err != nil {
		t.Fatal(err)
	}
	ownedIDBytes, err := memoryMul(int64(cfg.partitions), int64(cfg.topK), documentIDStorageBytes)
	if err != nil {
		t.Fatal(err)
	}
	mapBytes, err := memoryMul(int64(cfg.topK), memoryMapEntryBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantScratchBytes, err := memoryAdd(resultStructBytes, ownedIDBytes, mapBytes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.AttributionMergeScratchResults != 131_072 || plan.AttributionMergeScratchBytes != wantScratchBytes || ownedIDBytes != 1_048_576 || plan.ModeledPeakBytes <= withoutScratch {
		t.Fatalf("M8 attribution scratch plan=%+v without_scratch=%d", plan, withoutScratch)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, withoutScratch); err == nil || !strings.Contains(err.Error(), "attribution_merge_scratch_results=131072") {
		t.Fatalf("accepted unmodeled attribution merge scratch: %v", err)
	}
}

func TestM8CurrentCellOutcomesRespectMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 256, raftGroups: 4, overlaps: []float64{0}, probes: []int{256}, efSearch: []int{64}, concurrency: []int{64}, topK: 1, m8MaxExactTruthVisits: math.MaxInt64}
	manifest := fixtureManifest{Vectors: 256, Queries: 1_000_000, Dimensions: 1}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	withoutOutcomes, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorBytes)
	if err != nil {
		t.Fatal(err)
	}
	attributionWithoutOutcomes, err := memoryAdd(withoutOutcomes, plan.RetainedAttributionBytes, plan.AttributionMergeScratchBytes)
	if err != nil {
		t.Fatal(err)
	}
	sourceOracle, err := memoryAdd(plan.FixtureResidentBytes, plan.SourceSnapshotBytes, plan.ExactTruthBytes)
	if err != nil {
		t.Fatal(err)
	}
	withoutOutcomes, err = memoryScaleCeil(max(sourceOracle, withoutOutcomes, attributionWithoutOutcomes), memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CurrentCellOutcomes != 1_000_000 || plan.CurrentCellOutcomeBytes <= 1_500_000_000 || plan.ModeledPeakBytes <= withoutOutcomes {
		t.Fatalf("M8 current-cell outcome plan=%+v without_outcomes=%d", plan, withoutOutcomes)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, withoutOutcomes); err == nil || !strings.Contains(err.Error(), "current_cell_outcomes=1000000") {
		t.Fatalf("accepted unmodeled current-cell outcomes: %v", err)
	}
}

func TestM8WarmupResponsesRespectMemoryCapV1(t *testing.T) {
	manifest := fixtureManifest{Vectors: 1, Queries: 1, Dimensions: 128}
	base := config{partitions: 1, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{256}, topK: 1}
	withoutWarmup, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	base.warmup = 1
	withWarmup, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if withoutWarmup.CurrentCellOutcomes != 1 || withWarmup.CurrentCellOutcomes != 256 || withWarmup.CurrentCellOutcomeBytes <= withoutWarmup.CurrentCellOutcomeBytes || withoutWarmup.CurrentQueryConversionBytes != 512 || withWarmup.CurrentQueryConversionBytes != 131072 {
		t.Fatalf("warmup response plan without=%+v with=%+v", withoutWarmup, withWarmup)
	}
	if _, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, withWarmup.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "current_query_conversion_bytes=131072") {
		t.Fatalf("accepted unmodeled warmup query conversions: %v", err)
	}
	base.overlaps = []float64{0.2}
	unsupported, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if unsupported.CurrentCellOutcomes != 256 || unsupported.CurrentQueryConversionBytes != 131072 {
		t.Fatalf("unsupported-overlap warmup plan=%+v", unsupported)
	}
	if _, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, unsupported.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "current_query_conversion_bytes=131072") {
		t.Fatalf("accepted unmodeled unsupported-overlap warmup: %v", err)
	}
	base.warmup = 0
	preflightOnly, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if preflightOnly.CurrentCellOutcomes != 0 || preflightOnly.CurrentQueryConversionBytes != 0 || preflightOnly.PreflightResponseBytes == 0 || preflightOnly.PreflightQueryConversionBytes != 512 {
		t.Fatalf("unsupported-overlap preflight plan=%+v", preflightOnly)
	}
	if _, err := validateM8BenchmarkWork(base, manifest, math.MaxInt64, preflightOnly.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "preflight_query_conversion_bytes=512") {
		t.Fatalf("accepted unmodeled unsupported-overlap preflight: %v", err)
	}
}

func TestM8AttributionQueryConversionRespectsMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 1, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	manifest := fixtureManifest{Vectors: 1, Queries: 1, Dimensions: 1 << 20}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.AttributionQueryConversionBytes != 4<<20 {
		t.Fatalf("attribution query conversion bytes=%d, want %d", plan.AttributionQueryConversionBytes, 4<<20)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, plan.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "attribution_query_conversion_bytes=4194304") {
		t.Fatalf("accepted unmodeled attribution query conversion: %v", err)
	}
}

func TestM8PreflightResponseRespectsMemoryCapV1(t *testing.T) {
	manifest := fixtureManifest{Vectors: 16, Queries: 1, Dimensions: 1}
	cfg := config{partitions: 16, raftGroups: 4, overlaps: []float64{0.2}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	lowProbe := cfg
	lowProbe.partitions = 1
	lowProbePlan, err := validateM8BenchmarkWork(lowProbe, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CurrentCellOutcomes != 0 || plan.PreflightResponseBytes <= lowProbePlan.PreflightResponseBytes {
		t.Fatalf("preflight response plan=%+v low_probe=%+v", plan, lowProbePlan)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, plan.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "preflight_response_bytes=") {
		t.Fatalf("accepted unmodeled preflight response: %v", err)
	}
}

func TestM8MeasuredResponsesDoNotUseExhaustivePreflightShapeV1(t *testing.T) {
	manifest := fixtureManifest{Vectors: 256, Queries: 100_000, Dimensions: 1}
	cfg := config{partitions: 256, raftGroups: 4, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 1, m8MaxExactTruthVisits: math.MaxInt64}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	lowProbe := cfg
	lowProbe.partitions = 1
	lowProbePlan, err := validateM8BenchmarkWork(lowProbe, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CurrentCellOutcomes != int64(manifest.Queries) || plan.CurrentCellOutcomeBytes != lowProbePlan.CurrentCellOutcomeBytes || plan.PreflightResponseBytes <= lowProbePlan.PreflightResponseBytes {
		t.Fatalf("separate measured/preflight response plan=%+v low_probe=%+v", plan, lowProbePlan)
	}
	measurementBase, err := memoryAdd(plan.FixtureResidentBytes, plan.ExactTruthBytes, plan.RetainedCoordinatorBytes)
	if err != nil {
		t.Fatal(err)
	}
	legacyOutcomeBytes, err := memoryMul(plan.CurrentCellOutcomes, plan.PreflightResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	legacyPeak, err := memoryAdd(measurementBase, legacyOutcomeBytes, plan.CurrentQueryConversionBytes)
	if err != nil {
		t.Fatal(err)
	}
	legacyPeak, err = memoryScaleCeil(legacyPeak, memorySlackNumerator, memorySlackDenominator)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ModeledPeakBytes >= legacyPeak {
		t.Fatalf("measured responses still charged at exhaustive shape: plan=%+v legacy_peak=%d", plan, legacyPeak)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, plan.ModeledPeakBytes); err != nil {
		t.Fatalf("rejected bounded measured-response plan: %v", err)
	}
}

func TestM8SourceOracleSnapshotRespectsMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 1, overlaps: []float64{0}, probes: []int{1}, efSearch: []int{64}, concurrency: []int{1}, topK: 1}
	manifest := fixtureManifest{Vectors: 1_000_000, Queries: 1, Dimensions: 512}
	plan, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FixtureResidentBytes >= maxFixtureBytes || plan.SourceSnapshotBytes < 2_000_000_000 {
		t.Fatalf("M8 source snapshot plan=%+v", plan)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, maxFixtureBytes); err == nil || !strings.Contains(err.Error(), "source_snapshot_bytes=") {
		t.Fatalf("accepted oversized source snapshot: %v", err)
	}
}

func TestM8AttributionPrimaryHomeMappingIsModeledV1(t *testing.T) {
	cfg := config{partitions: 16, overlaps: []float64{0}, probes: []int{4}, efSearch: []int{128}, concurrency: []int{1}, topK: 10}
	manifest := fixtureManifest{Vectors: 1_000_000, Queries: 32, Dimensions: 16}
	plan, err := validateM8BenchmarkWork(cfg, manifest, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	truthIDs := int64(manifest.Queries * cfg.topK)
	wantMap, err := memoryMul(truthIDs, memoryMapEntryBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantFinal, err := memoryAdd(truthIDs*(memoryMapEntryBytes+int64(unsafe.Sizeof([]uint32{}))), 2*(truthIDs+int64(manifest.Vectors))*int64(unsafe.Sizeof(uint32(0))))
	if err != nil {
		t.Fatal(err)
	}
	wantScratch, err := memoryMul(truthIDs, 3*memoryMapEntryBytes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.AttributionPrimaryHomeMapBytes != wantMap || plan.AttributionFinalMembershipBytes != wantFinal || plan.AttributionHomeBuildScratchBytes != wantScratch {
		t.Fatalf("truth-membership attribution memory is not fully modeled: plan=%+v", plan)
	}
}

func TestM8RetainedCoordinatorResultsRespectMemoryCapV1(t *testing.T) {
	cfg := config{partitions: 16, overlaps: []float64{0}, probes: []int{1, 4}, efSearch: []int{64, 128}, concurrency: []int{1, 2}, topK: 256, m8MaxExactTruthVisits: math.MaxInt64}
	manifest := fixtureManifest{Vectors: 10_000, Queries: 50_000, Dimensions: 8}
	plan, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RetainedCoordinatorResults != 102_400_000 || plan.RetainedCoordinatorBytes < 4_000_000_000 {
		t.Fatalf("M8 retained-result plan=%+v", plan)
	}
	if _, err := validateM8BenchmarkWork(cfg, manifest, math.MaxInt64, maxFixtureBytes); err == nil || !strings.Contains(err.Error(), "retained_coordinator_results=102400000") {
		t.Fatalf("accepted oversized retained result sweep: %v", err)
	}
}

func TestM8ProductionEvidenceJSONKeepsEveryTopologyDimensionV1(t *testing.T) {
	routerSession := nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{
		Identity: nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{
			Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding",
			IndexDefinitionDigest: "index-digest", SourceGeneration: 1, SourceChecksum: 2,
			SourceSchemaHash: 3, SourceRowCount: 4, PartitionGeneration: 5,
			ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest",
		},
		ColdOpens: 1, ManifestOpenAttempts: 2, Misses: 3, Hits: 4, OpenFailures: 5,
		ReaderPins: 6, ReaderReleases: 7, LeasePins: 8, LeaseReleases: 9,
		Invalidations: 10, Closes: 11,
	}
	raw, err := json.Marshal(m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, RouterCandidates: 256},
		UntimedBoundary: m8ProductionResourceBoundaryV1{
			SelectedPartitions: 16, EfSearch: 4096, WallClockNanos: 88,
			Maxima: m8ProductionResourceObservedMaximaV1{Requests: 4, RPCs: 4, RequestBytes: 6, CandidateBytes: 7},
		},
		Failure: m8ProductionFailureEvidenceV1{ResourceBoundary: m8ProductionFaultResourceBoundaryV1{
			SelectedPartitions: 16, EfSearch: 4096, WallClockNanos: 99,
			Maxima: m8ProductionResourceObservedMaximaV1{Requests: 4, RPCs: 5, RequestBytes: 6, CandidateBytes: 7},
		}},
		Rows: []m8ProductionRowV1{{Probes: 4, EfSearch: 128, Concurrency: 16, Samples: 32, RecallAtK: 0, Attribution: m8ProductionAttributionV1{
			Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, ExhaustivePartitionRecallAtK: 1,
			ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
			ExactRepresentativeRecallAtK: .9, ApproximateRepresentativeRecallAtK: .8,
			LocalHNSWRecallAtK: .7, ApproximateLocalHNSWRecallAtK: .7, EndToEndRecallAtK: 0,
			CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
			ApproximateRouterCandidateBudget: 256, ApproximateRouterPartitionCoverageComplete: true, ResidualLossOwners: []string{"partition_local_hnsw"},
		}}},
		RouterSessions: m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{routerSession}, AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{{Hits: 32}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"raft_groups":4`, `"raft_nodes_per_group":3`, `"partitions":16`, `"router_sessions"`, `"after_warmup"`, `"after_measured"`, `"identity":{"database":"default","catalog":"default","collection":"docs","index_name":"embedding","index_definition_digest":"index-digest","source_generation":1,"source_checksum":2,"source_schema_hash":3,"source_row_count":4,"partition_generation":5,"ready_set_digest":"ready-digest","router_model_digest":"model-digest"}`, `"cold_opens":1`, `"manifest_open_attempts":2`, `"misses":3`, `"hits":4`, `"open_failures":5`, `"reader_pins":6`, `"reader_releases":7`, `"lease_pins":8`, `"lease_releases":9`, `"invalidations":10`, `"closes":11`, `"untimed_resource_boundary":{"selected_partitions":16,"ef_search":4096,"wall_clock_nanos":88`, `"resource_boundary":{"selected_partitions":16,"ef_search":4096,"wall_clock_nanos":99,"observed_maxima":{"requests":4,"rpcs":5,"retries":0,"redirects":0,"request_bytes":6,"candidate_bytes":7`, `"approximate_router_candidate_budget":256`, `"approximate_router_partition_coverage_complete":true`, `"probes":4`, `"ef_search":128`, `"concurrency":16`, `"samples":32`, `"recall_at_k":0`, `"contract":"` + m8CanonicalResultContractV1 + `"`, `"global_exact_recall_at_k":1`, `"exhaustive_partition_union_score_parity":true`, `"residual_loss_owners":["partition_local_hnsw"]`} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Fatalf("missing %s in %s", field, raw)
		}
	}
	for _, legacy := range []string{`"Identity"`, `"Database"`, `"ColdOpens"`, `"ReaderPins"`, `"LeaseReleases"`} {
		if bytes.Contains(raw, []byte(legacy)) {
			t.Fatalf("legacy router-session JSON field %s in %s", legacy, raw)
		}
	}
}

func TestM8ArtifactNameIncludesConfigurationV1(t *testing.T) {
	fixture := fixtureManifest{Fixture: "x", Checksum: "y"}
	base := config{headSHA: strings.Repeat("a", 40), raftGroups: 2, raftNodes: 3, partitions: 4, probes: []int{4}, overlaps: []float64{0}, topK: 10, concurrency: []int{1}, efSearch: []int{64}, routerCandidates: 64}
	executionID := strings.Repeat("a", 32)
	first, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"}, executionID)
	if err != nil {
		t.Fatal(err)
	}
	base.partitions = 8
	second, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"}, executionID)
	if err != nil || first == second {
		t.Fatalf("names %q %q err=%v", first, second, err)
	}
	base.partitions = 4
	third, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "different"}, executionID)
	if err != nil || first == third {
		t.Fatalf("asset identities collided %q %q err=%v", first, third, err)
	}
	base.routerCandidates = 32
	fourth, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"}, executionID)
	if err != nil || first == fourth {
		t.Fatalf("router candidate budgets collided %q %q err=%v", first, fourth, err)
	}
	fifth, err := m8ArtifactNameV1(base, fixture, collections.VectorPartitionManifestV1{ReadySetDigest: "a"}, strings.Repeat("b", 32))
	if err != nil || fourth == fifth {
		t.Fatalf("execution identities collided %q %q err=%v", fourth, fifth, err)
	}
}

func TestM8ProductionExecutionIDV1(t *testing.T) {
	id, err := m8ProductionExecutionIDV1()
	if err != nil || !validM8ProductionExecutionIDV1(id) {
		t.Fatalf("execution ID=%q err=%v", id, err)
	}
	if validM8ProductionExecutionIDV1(id+" ") || validM8ProductionExecutionIDV1(strings.ToUpper(id)) {
		t.Fatalf("accepted malformed execution ID %q", id)
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

func TestM8CanonicalRefPathMatchesOwnedTieDedupeAndLifetimeV1(t *testing.T) {
	candidates := []m8CanonicalResultV1{
		{ID: "duplicate", Score: 0.1},
		{ID: "z", Score: 0.9},
		{ID: "a", Score: 0.9},
		{ID: "duplicate", Score: 0.95},
		{ID: "lower", Score: math.Float32frombits(math.Float32bits(0.9) - 1)},
	}
	var owned []m8CanonicalResultV1
	refs := newM8CanonicalRefTopKV1(4)
	refIDs := make([][]byte, len(candidates))
	for i, candidate := range candidates {
		owned = m8AppendBoundedCanonicalV1(owned, candidate, 4)
		refIDs[i] = []byte(candidate.ID)
		if !refs.add(m8CanonicalRefResultV1{ID: refIDs[i], Score: candidate.Score}) {
			t.Fatalf("rejected ref candidate %+v", candidate)
		}
	}
	got := m8MaterializeCanonicalRefsV1(refs.results())
	if len(got) != len(owned) {
		t.Fatalf("ref results=%+v owned=%+v", got, owned)
	}
	for i := range owned {
		if got[i] != owned[i] {
			t.Fatalf("rank=%d ref=%+v owned=%+v", i, got, owned)
		}
	}
	for i := range refIDs {
		for j := range refIDs[i] {
			refIDs[i][j] = 'x'
		}
	}
	for i := range owned {
		if got[i] != owned[i] {
			t.Fatalf("materialized result changed after source mutation rank=%d ref=%+v owned=%+v", i, got, owned)
		}
	}
}

func TestM8CanonicalRefTopKUsesKeyedBoundedHeapV1(t *testing.T) {
	const candidates = 10_000
	top := newM8CanonicalRefTopKV1(256)
	for i := 0; i < candidates; i++ {
		id := []byte(fmt.Sprintf("doc-%06d", i))
		if !top.add(m8CanonicalRefResultV1{ID: id, Score: float32(i)}) {
			t.Fatalf("rejected candidate %d", i)
		}
	}
	if len(top.heap) != 256 || len(top.results()) != 256 {
		t.Fatalf("unbounded or short ref heap len=%d results=%d", len(top.heap), len(top.results()))
	}
	if top.idComparisons >= candidates {
		t.Fatalf("ref duplicate comparisons=%d candidates=%d", top.idComparisons, candidates)
	}
}

func TestM8CanonicalRefTopKMatchesCanonicalOrderWithDuplicatesV1(t *testing.T) {
	const (
		candidates = 10_000
		topK       = 256
	)
	var want []m8CanonicalResultV1
	top := newM8CanonicalRefTopKV1(topK)
	for i := 0; i < candidates; i++ {
		candidate := m8CanonicalResultV1{
			ID:    fmt.Sprintf("doc-%06d", (i*37)%777),
			Score: float32((i * 41) % 997),
		}
		want = m8AppendBoundedCanonicalV1(want, candidate, topK)
		if !top.add(m8CanonicalRefResultV1{ID: []byte(candidate.ID), Score: candidate.Score}) {
			t.Fatalf("rejected candidate %d", i)
		}
	}
	got := m8MaterializeCanonicalRefsV1(top.results())
	if !slices.Equal(got, want) {
		t.Fatalf("ref top-k differs from canonical order\ngot=%+v\nwant=%+v", got, want)
	}
}

func TestM8CanonicalContractRejectsInvalidBoundsAndScoresV1(t *testing.T) {
	if got := m8CanonicalResultsV1([]m8CanonicalResultV1{{ID: "a", Score: 1}}, -1); got != nil {
		t.Fatalf("negative top-k result=%+v", got)
	}
	if got := m8CanonicalResultsV1([]m8CanonicalResultV1{{ID: "a", Score: float32(math.NaN())}}, 1); got != nil {
		t.Fatalf("nonfinite result=%+v", got)
	}
	oversized := make([]m8CanonicalResultV1, 256*256)
	for i := range oversized {
		oversized[i] = m8CanonicalResultV1{ID: fmt.Sprintf("doc-%06d", i), Score: float32(i)}
	}
	if got := m8CanonicalResultsV1(oversized, 256); len(got) != 256 || cap(got) != 256 {
		t.Fatalf("canonical top-k retained oversized merge backing len=%d cap=%d", len(got), cap(got))
	}
}

func TestValidM8AttributionPersistsExhaustiveUnionFailureV1(t *testing.T) {
	attribution := m8ProductionAttributionV1{
		Contract:                                   m8CanonicalResultContractV1,
		GlobalExactRecallAtK:                       1,
		ExhaustivePartitionRecallAtK:               .5,
		ExhaustivePartitionIDParity:                false,
		ExhaustivePartitionScoreParity:             false,
		ExactRepresentativeRecallAtK:               .5,
		ApproximateRepresentativeRecallAtK:         .5,
		LocalHNSWRecallAtK:                         .5,
		ApproximateLocalHNSWRecallAtK:              .5,
		EndToEndRecallAtK:                          .5,
		CoordinatorMergeIDParity:                   true,
		CoordinatorMergeScoreParity:                true,
		ApproximateRouterCandidateBudget:           1,
		ApproximateRouterPartitionCoverageComplete: true,
	}
	attribution.ResidualLossOwners = m8AttributionLossOwnersV1(attribution)
	if !validM8AttributionV1(attribution, 10) {
		t.Fatalf("rejected attributable exhaustive-union failure: %+v", attribution)
	}
	if !slices.Equal(attribution.ResidualLossOwners, []string{"partition_membership_or_score_contract"}) {
		t.Fatalf("loss owners=%v", attribution.ResidualLossOwners)
	}
	attribution.ResidualLossOwners = nil
	if validM8AttributionV1(attribution, 10) {
		t.Fatal("accepted exhaustive-union failure without its loss owner")
	}
	legacy := m8ProductionAttributionV1{
		Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1,
		ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
		ExactRepresentativeRecallAtK: .5, ApproximateRepresentativeRecallAtK: .5, LocalHNSWRecallAtK: .5, ApproximateLocalHNSWRecallAtK: .5, EndToEndRecallAtK: .5,
		CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
		ApproximateRouterCandidateBudget: 1, ApproximateRouterPartitionCoverageComplete: true,
	}
	legacy.ResidualLossOwners = m8AttributionLossOwnersV1(legacy)
	if !validM8AttributionV1(legacy, 10) || !slices.Equal(legacy.ResidualLossOwners, []string{"exact_representative_routing"}) {
		t.Fatalf("legacy attribution=%+v", legacy)
	}
}

func TestValidM8AttributionRequiresEveryTruthRankV1(t *testing.T) {
	attribution := m8ProductionAttributionV1{
		Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, OracleStagesComplete: true,
		PrimaryHomeOracleRecallAtK: 1, FinalMembershipOracleRecallAtK: 1,
		ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
		ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: 1, ApproximateLocalHNSWRecallAtK: 1, EndToEndRecallAtK: 1,
		CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
		ApproximateRouterCandidateBudget: 1, ApproximateRouterPartitionCoverageComplete: true,
		TruthNeighborRankRetentionAtK: []float64{1, 1},
	}
	attribution.ResidualLossOwners = m8AttributionLossOwnersV1(attribution)
	attribution.StageOwners = m8AttributionStageOwnersV1(attribution)
	if !validM8AttributionV1(attribution, 2) {
		t.Fatal("rejected complete truth-rank retention")
	}
	if validM8AttributionV1(attribution, 1) || validM8AttributionV1(attribution, 3) {
		t.Fatal("accepted truth-rank retention with the wrong top-k cardinality")
	}
	favorable := attribution
	favorable.ExactRepresentativeRecallAtK = .9
	favorable.ApproximateRepresentativeRecallAtK = 1
	favorable.LocalHNSWRecallAtK = .9
	favorable.ApproximateLocalHNSWRecallAtK = .9
	favorable.EndToEndRecallAtK = .9
	favorable.FinalMembershipToExactLossAtK = .1
	favorable.ExactToApproximateLossAtK = -.1
	favorable.ApproximateToLocalHNSWLossAtK = .1
	favorable.ResidualLossOwners = m8AttributionLossOwnersV1(favorable)
	favorable.StageOwners = m8AttributionStageOwnersV1(favorable)
	if !validM8AttributionV1(favorable, 2) {
		t.Fatalf("rejected favorable approximate routing=%+v", favorable)
	}
	favorable.ExactToLocalHNSWLossAtK = -.1
	if validM8AttributionV1(favorable, 2) {
		t.Fatalf("accepted negative non-routing loss=%+v", favorable)
	}
}

func TestM8AttachAttributionAfterMeasurementV1(t *testing.T) {
	row := m8ProductionRowV1{Samples: 2, RecallAtK: .5}
	cell := m8AttributionCellV1{
		Evidence: m8ProductionAttributionV1{
			Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1,
			ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
			ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: .5, ApproximateLocalHNSWRecallAtK: .5,
			CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
			ApproximateRouterCandidateBudget: 2, ApproximateRouterPartitionCoverageComplete: true,
			LocalHNSWSearches: 4, LocalHNSWCandidates: 2,
		},
		Local: [][]m8CanonicalResultV1{{{ID: "a", Score: 1}}, {{ID: "b", Score: 1}}},
	}
	coordinator := [][]m8CanonicalResultV1{{{ID: "a", Score: 1}}, {{ID: "z", Score: 1}}}
	if err := m8AttachAttributionV1(&row, cell, coordinator); err != nil {
		t.Fatal(err)
	}
	if row.Attribution.EndToEndRecallAtK != row.RecallAtK || row.Attribution.CoordinatorMergeIDParity || !row.Attribution.CoordinatorMergeScoreParity ||
		!slices.Contains(row.Attribution.ResidualLossOwners, "coordinator_merge_or_transport") {
		t.Fatalf("post-measurement attribution=%+v", row.Attribution)
	}
	if err := m8AttachAttributionV1(&m8ProductionRowV1{Samples: 1}, cell, coordinator); err == nil {
		t.Fatal("accepted post-measurement attribution cardinality mismatch")
	}
	shortfall := m8ProductionRowV1{Status: "candidate_coverage_shortfall", Samples: 2}
	if err := m8AttachAttributionV1(&shortfall, cell, coordinator); err != nil {
		t.Fatal(err)
	}
	if shortfall.Attribution.ApproximateRouterPartitionCoverageComplete || shortfall.Attribution.ApproximateRepresentativeRecallAtK != 0 ||
		shortfall.Attribution.ApproximateLocalHNSWRecallAtK != 0 || shortfall.Attribution.ApproximateLocalHNSWSearches != 0 || shortfall.Attribution.ApproximateLocalHNSWCandidates != 0 || shortfall.Attribution.ApproximateLocalHNSWEdges != 0 ||
		shortfall.Attribution.EndToEndRecallAtK != 0 || shortfall.Attribution.CoordinatorMergeIDParity || shortfall.Attribution.CoordinatorMergeScoreParity ||
		shortfall.Attribution.ExactToApproximateLossAtK != shortfall.Attribution.ExactRepresentativeRecallAtK || shortfall.Attribution.ApproximateToLocalHNSWLossAtK != 0 ||
		shortfall.Attribution.ApproximateLocalToEndToEndLossAtK != 0 || shortfall.Attribution.LocalHNSWRecallAtK != .5 || shortfall.Attribution.LocalHNSWSearches != 4 || !validM8AttributionV1(shortfall.Attribution, 1) {
		t.Fatalf("shortfall attribution=%+v", shortfall.Attribution)
	}
}

func TestM8AttributionApproximateCoverageShortfallIsOwnedV1(t *testing.T) {
	attribution := m8ProductionAttributionV1{
		Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1,
		ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true,
		ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 0, LocalHNSWRecallAtK: 1, ApproximateLocalHNSWRecallAtK: 0, EndToEndRecallAtK: 1,
		CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true,
		ApproximateRouterCandidateBudget: 2, ApproximateRouterPartitionCoverageComplete: false,
	}
	attribution.ResidualLossOwners = m8AttributionLossOwnersV1(attribution)
	if !validM8AttributionV1(attribution, 10) || !slices.Equal(attribution.ResidualLossOwners, []string{"approximate_representative_routing"}) {
		t.Fatalf("coverage-shortfall attribution=%+v", attribution)
	}
	if complete, err := m8ApproximateRouterCoverageV1(fmt.Errorf("wrapped: %w", collections.ErrVectorPartitionRouterCandidateCoverageV1)); err != nil || complete {
		t.Fatalf("typed coverage result complete=%t err=%v", complete, err)
	}
	invalid := attribution
	invalid.ApproximateRepresentativeRecallAtK = .5
	invalid.ResidualLossOwners = m8AttributionLossOwnersV1(invalid)
	if validM8AttributionV1(invalid, 10) {
		t.Fatalf("accepted nonzero approximate recall with incomplete coverage: %+v", invalid)
	}
	invalid = attribution
	invalid.ApproximateLocalHNSWRecallAtK = .5
	if validM8AttributionV1(invalid, 10) {
		t.Fatalf("accepted approximate local recall with incomplete coverage: %+v", invalid)
	}
	if _, err := m8ApproximateRouterCoverageV1(errors.New("router corruption")); err == nil {
		t.Fatal("accepted non-coverage router error")
	}
}

func TestM8CoordinatorResponseCanonicalShapeFailsClosedV1(t *testing.T) {
	response := nativewire.VectorPartitionCoordinatorResponseV1{
		Neighbors:        []nativewire.VectorPartitionCoordinatorNeighborV1{{ID: "a", Score: .9}, {ID: "b", Score: .8}},
		ProbedPartitions: []uint32{0, 1},
	}
	response.ProbedGroups = append(response.ProbedGroups, "group-a")
	manifest := collections.VectorPartitionManifestV1{SourceRowCount: 3, PartitionCount: 4, Placements: []collections.VectorPartitionPlacementV1{
		{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-a"},
		{PartitionID: 2, GroupID: "group-b"}, {PartitionID: 3, GroupID: "group-b"},
	}, Memberships: []collections.VectorPartitionMembershipV1{
		{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1},
		{VectorOrdinal: 2, PartitionID: 2},
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
	if _, err := m8ValidateCoordinatorResponseV1(response, manifest, 2, 2); err == nil {
		t.Fatal("accepted truncated response for two unique selected rows")
	}
	sparseManifest := manifest
	sparseManifest.SourceRowCount = 1
	sparseManifest.Memberships = []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}
	sparseManifest.OverlapMemberships = []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}}
	if got, err := m8ValidateCoordinatorResponseV1(response, sparseManifest, 2, 2); err != nil || len(got) != 1 {
		t.Fatalf("valid sparse response got=%+v err=%v", got, err)
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
	artifacts, err := m8ProfileArtifactsV1(paths)
	if err != nil {
		t.Fatal(err)
	}
	canonicalDir, err := m8CanonicalPathV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !validM8ProductionProfilesV1(m8ProductionProfileEvidenceV1{Directory: canonicalDir, Captured: paths, Artifacts: artifacts, Status: "captured_production_query_and_fault_boundary", Scope: "test"}) {
		t.Fatal("captured profile evidence rejected")
	}
	if again, err := capture.Stop(); err != nil || fmt.Sprint(again) != fmt.Sprint(paths) {
		t.Fatalf("idempotent stop paths=%v err=%v", again, err)
	}
}

func TestM8ProfileCaptureCreatesDirectoryV1(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	capture, err := startM8ProfileCaptureV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m8FinishDirectProfileCaptureV1(capture, false); err != nil {
		t.Fatal(err)
	}
}

func TestM8ProfileCaptureDoesNotReplaceExistingArtifactV1(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "allocs_baseline.pprof")
	const sentinel = "retain"
	if err := os.WriteFile(path, []byte(sentinel), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := startM8ProfileCaptureV1(dir); err == nil {
		t.Fatal("profile capture replaced existing artifact")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != sentinel {
		t.Fatalf("profile sentinel=%q err=%v", raw, err)
	}
}

func TestM8ProfileCaptureCleansPartialSetupV1(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "cpu.pprof")
	if err := os.WriteFile(sentinel, []byte("retain"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := startM8ProfileCaptureV1(dir); err == nil {
		t.Fatal("profile capture accepted a CPU profile collision")
	}
	for _, name := range []string{"allocs_baseline.pprof", "trace.out"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial profile %s remains: %v", name, err)
		}
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "retain" {
		t.Fatalf("CPU sentinel=%q err=%v", raw, err)
	}
}

func TestM8ProfileCaptureCleanupPolicyV1(t *testing.T) {
	dir := t.TempDir()
	capture, err := startM8ProfileCaptureV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m8FinishDirectProfileCaptureV1(capture, false); err != nil {
		t.Fatal(err)
	}
	retry, err := startM8ProfileCaptureV1(dir)
	if err != nil {
		t.Fatalf("retry after unsuccessful direct capture: %v", err)
	}
	if err := m8FinishDirectProfileCaptureV1(retry, false); err != nil {
		t.Fatal(err)
	}

	retained, err := startM8ProfileCaptureV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := m8FinishDirectProfileCaptureV1(retained, true); err != nil {
		t.Fatal(err)
	}
	paths, err := retained.Stop()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("successful capture did not retain %s: %v", path, err)
		}
	}
}

func TestM8ProfileArtifactDecodeTimeoutIsBoundedBySizeV1(t *testing.T) {
	for _, test := range []struct {
		name  string
		bytes int64
		want  time.Duration
	}{
		{name: "tiny", bytes: 1, want: time.Minute + 500*time.Millisecond},
		{name: "one MiB", bytes: 1 << 20, want: time.Minute + 500*time.Millisecond},
		{name: "twenty eight MiB", bytes: 28 << 20, want: time.Minute + 14*time.Second},
		{name: "maximum", bytes: m8ProfileArtifactMaxBytesV1, want: 5*time.Minute + 16*time.Second},
		{name: "clamped", bytes: m8ProfileArtifactMaxBytesV1 + 1, want: 5*time.Minute + 16*time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := m8ProfileArtifactDecodeTimeoutV1(test.bytes); got != test.want {
				t.Fatalf("timeout=%s want=%s", got, test.want)
			}
		})
	}
}

func TestM8ProfileArtifactsRejectMalformedRuntimeArtifactsV1(t *testing.T) {
	for _, test := range []struct {
		name, artifact string
		contents       []byte
	}{
		{name: "garbage pprof", artifact: "cpu.pprof", contents: []byte("not a pprof")},
		{name: "truncated pprof", artifact: "cpu.pprof", contents: []byte{0}},
		{name: "garbage trace", artifact: "trace.out", contents: []byte("not a trace")},
		{name: "truncated trace", artifact: "trace.out", contents: []byte{0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			capture, err := startM8ProfileCaptureV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			runtime.Gosched()
			paths, err := capture.Stop()
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if filepath.Base(path) == test.artifact {
					if err := os.WriteFile(path, test.contents, 0o644); err != nil {
						t.Fatal(err)
					}
					break
				}
			}
			if _, err := m8ProfileArtifactsV1(paths); err == nil {
				t.Fatalf("accepted %s", test.name)
			}
		})
	}
}

func TestM8ProfileCaptureCanonicalizesAliasedDirectoryV1(t *testing.T) {
	directory, parent := t.TempDir(), t.TempDir()
	alias := filepath.Join(parent, "profiles")
	if err := os.Symlink(directory, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	capture, err := startM8ProfileCaptureV1(alias)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := capture.Stop()
	if err != nil {
		t.Fatal(err)
	}
	canonicalDir, err := m8CanonicalPathV1(directory)
	if err != nil {
		t.Fatal(err)
	}
	if capture.dir != canonicalDir {
		t.Fatalf("capture dir=%q want=%q", capture.dir, canonicalDir)
	}
	artifacts, err := m8ProfileArtifactsV1(paths)
	if err != nil {
		t.Fatal(err)
	}
	if !validM8ProductionProfilesV1(m8ProductionProfileEvidenceV1{Directory: canonicalDir, Captured: paths, Artifacts: artifacts, Status: "captured_production_query_and_fault_boundary", Scope: "test"}) {
		t.Fatal("canonicalized profile evidence rejected")
	}
}

func TestM8GateLedgerRequiresMatchedRecallQPSAndTailV1(t *testing.T) {
	report := m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: 0.9},
		PackDiagnostics: func() []m8PartitionPackDiagnosticsV1 {
			diagnostics := make([]m8PartitionPackDiagnosticsV1, 16)
			for partition := range diagnostics {
				diagnostics[partition] = m8PartitionPackDiagnosticsV1{PartitionID: uint32(partition), Rows: 1, ReachableRows: 1, TraversalRoots: 1}
			}
			return diagnostics
		}(),
		Rows: []m8ProductionRowV1{
			{Status: "pass", Probes: 16, EfSearch: 128, Concurrency: 16, RecallAtK: 0.99, QPS: 100, P95Nanos: 1000, ExactParityChecked: true, ExactParityPassed: true, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
			{Status: "pass", Probes: 4, EfSearch: 128, Concurrency: 16, RecallAtK: 0.92, QPS: 116, P95Nanos: 999},
		},
		Failure:   m8ProductionFailureEvidenceV1{Passed: true},
		Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 1, PartitionLoads: make([]uint64, 16), PeakRSSMeasured: true, MaxPartitionLoad: 65_000, BalanceHardCap: 65_625, LimitComparisons: []m8ProductionResourceLimitComparisonV1{{Name: "bytes", Configured: 2, Observed: 1, Passed: true}}},
	}
	for partition := range report.Resources.PartitionLoads {
		report.Resources.PartitionLoads[partition] = 1
	}
	ledger := m8ProductionGateLedgerForReportV1(report)
	if ledger.ExhaustiveParity != "pass" || ledger.FailureHonesty != "pass" || ledger.PartitionPackReachability != "pass" || ledger.Recall != "pass" || ledger.ProbeReduction != "pass" || ledger.EndToEndQPS != "pass" || ledger.TailLatency != "pass" || ledger.Balance != "pass" || ledger.ResourceBounds != "pass" || ledger.OverlapStorage != "fail" {
		t.Fatalf("ledger=%+v", ledger)
	}
	report.Rows[0].Attribution.ExhaustivePartitionIDParity = false
	ledger = m8ProductionGateLedgerForReportV1(report)
	if ledger.ExhaustiveParity != "fail" {
		t.Fatalf("coordinator parity failure ledger=%+v", ledger)
	}
	report.Rows[0].Attribution.ExhaustivePartitionIDParity = true
	report.Rows[1].QPS = 114.9
	report.Rows[1].P95Nanos = 1001
	ledger = m8ProductionGateLedgerForReportV1(report)
	if ledger.EndToEndQPS != "fail" || ledger.TailLatency != "fail" {
		t.Fatalf("unmatched performance ledger=%+v", ledger)
	}
}

func TestM8ConfiguredConcurrentShardRequestsCoversClientConcurrencyV1(t *testing.T) {
	got, err := m8ConfiguredConcurrentShardRequestsV1(8, []int{1, 16})
	if err != nil {
		t.Fatal(err)
	}
	if got != 128 {
		t.Fatalf("configured aggregate shard concurrency=%d want 128", got)
	}
	if _, err := m8ConfiguredConcurrentShardRequestsV1(8, []int{0, 16}); err == nil {
		t.Fatal("expected invalid client concurrency rejection")
	}
}

func TestM8WarmupCountAndConcurrencyPreservesZeroV1(t *testing.T) {
	if count, concurrency := m8WarmupCountAndConcurrencyV1(config{warmup: 0, concurrency: []int{1, 16}}); count != 0 || concurrency != 16 {
		t.Fatalf("zero warmup count/concurrency=%d/%d want 0/16", count, concurrency)
	}
	if count, concurrency := m8WarmupCountAndConcurrencyV1(config{warmup: 3, concurrency: []int{1, 16}}); count != 16 || concurrency != 16 {
		t.Fatalf("enabled warmup count/concurrency=%d/%d want 16/16", count, concurrency)
	}
}

func TestM8PartitionLoadsIncludeOverlapMembershipsV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{
		PartitionCount: 2,
		Memberships: []collections.VectorPartitionMembershipV1{
			{VectorOrdinal: 0, PartitionID: 0},
			{VectorOrdinal: 1, PartitionID: 1},
		},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{
			{VectorOrdinal: 0, PartitionID: 1},
		},
	}
	loads, err := m8PartitionLoadsV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(loads, []uint64{1, 2}) {
		t.Fatalf("partition loads=%v want [1 2]", loads)
	}
}

func TestM3DefaultPartitionHNSWMIsIndependentOfGraphDegreeV1(t *testing.T) {
	args := []string{"-stage", "overlap,partition_index", "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-overlap", "0", "-partition-degree", "33", "-m3-persist-db", t.TempDir()}
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	if m, efConstruction, err := m3PartitionLocalHNSWConfigV1(cfg); err != nil || m != partitionHNSWDegree || efConstruction != partitionHNSWDefaultEfC {
		t.Fatalf("default local M/eFC=%d/%d err=%v", m, efConstruction, err)
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
	cfg := config{partitions: 4, overlaps: []float64{0, .2}, topK: 10, partition: vectorpartition.Config{Degree: 16}}
	plan, err := validateM3BenchmarkWork(cfg, fixture, 2770)
	if err != nil {
		t.Fatalf("exact M3 work-budget boundary rejected: %v", err)
	}
	if plan.ChecksumVectorQueryVisits != 100 || plan.MembershipVectorQueryVisits != 440 || plan.TruthAdjacencyComparisons != 450 || plan.GraphDiagnosticWorkUnits != 1130 || plan.TruthPartitionRankingWork != 650 || plan.VectorQueryVisits != 2770 {
		t.Fatalf("M3 boundary work plan=%+v", plan)
	}
	if _, err := validateM3BenchmarkWork(cfg, fixture, 2769); err == nil {
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
	if canonicalPlan.VectorQueryVisits != 8_056_080 {
		t.Fatalf("canonical M3 work plan=%+v", canonicalPlan)
	}
}

func TestExplicitPartitionCapacityOverridesV1(t *testing.T) {
	base := []string{
		"-dataset", fixturePath(t),
		"-out", t.TempDir(),
		"-partitions", "4",
		"-probes", "1",
		"-stage", "partition",
	}
	args := func(extra ...string) []string {
		return append(append([]string(nil), base...), extra...)
	}
	for _, flag := range []string{"-partition-max-distance-work", "-partition-max-partition-work", "-m3-max-benchmark-visits"} {
		if _, err := parseConfig(args(flag, "0")); err == nil || !strings.Contains(err.Error(), "must be positive") {
			t.Fatalf("%s=0 error=%v; want positive-limit rejection", flag, err)
		}
	}
	if _, err := parseConfig(args("-router-max-scalar-work", "0")); err == nil || !strings.Contains(err.Error(), "router max scalar work") {
		t.Fatalf("router scalar zero-cap error=%v", err)
	}
	cfg, err := parseConfig(args(
		"-partition-max-distance-work", "134000000000",
		"-partition-max-partition-work", "400000000",
		"-router-max-scalar-work", "50000000000",
		"-m3-max-benchmark-visits", "3000000000",
	))
	if err != nil {
		t.Fatalf("explicit capacity overrides rejected: %v", err)
	}
	if got, want := cfg.partition.MaxDistanceWork, int64(134_000_000_000); got != want {
		t.Fatalf("MaxDistanceWork=%d want %d", got, want)
	}
	if got, want := cfg.partition.MaxPartitionWork, int64(400_000_000); got != want {
		t.Fatalf("MaxPartitionWork=%d want %d", got, want)
	}
	if got, want := cfg.routerConfig.MaxScalarWork, int64(50_000_000_000); got != want {
		t.Fatalf("Router MaxScalarWork=%d want %d", got, want)
	}
	if got, want := cfg.routerConfig.MaxVectors, cfg.maxVectors; got != want {
		t.Fatalf("Router MaxVectors=%d want inherited %d", got, want)
	}
	routerCfg, err := parseConfig(args("-router-max-vectors", "120000"))
	if err != nil || routerCfg.routerConfig.MaxVectors != 120000 {
		t.Fatalf("explicit router membership cap cfg=%+v err=%v", routerCfg.routerConfig, err)
	}
	for _, value := range []string{"0", "-1", "1200001"} {
		if _, err := parseConfig(args("-router-max-vectors", value)); err == nil || !strings.Contains(err.Error(), "router max vectors") {
			t.Fatalf("router membership cap %s error=%v", value, err)
		}
	}
	if _, err := parseConfig(args("-router-max-scalar-work", "50000000001")); err == nil || !strings.Contains(err.Error(), "router max scalar work") {
		t.Fatalf("router scalar hard-cap error=%v", err)
	}
	if got, want := cfg.m3MaxBenchmarkVisits, int64(3_000_000_000); got != want {
		t.Fatalf("m3MaxBenchmarkVisits=%d want %d", got, want)
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
	t.Run("explicit_overrides_ambient", func(t *testing.T) {
		base, head := strings.Repeat("c", 40), strings.Repeat("d", 40)
		t.Setenv("BASE_SHA", strings.Repeat("a", 40))
		t.Setenv("GITHUB_SHA", strings.Repeat("b", 40))
		t.Setenv("GITHUB_EVENT_PATH", "")
		gotBase, gotHead, err := provenanceWithExplicitV1(base, head)
		if err != nil || gotBase != base || gotHead != head {
			t.Fatalf("explicit provenance=%q/%q err=%v", gotBase, gotHead, err)
		}
	})
	for _, args := range [][]string{
		{"-base-sha", strings.Repeat("a", 40)},
		{"-base-sha", strings.Repeat("a", 40), "-head-sha", "BAD"},
	} {
		if _, err := parseConfig(args); err == nil || !strings.Contains(err.Error(), "base-sha") {
			t.Fatalf("accepted malformed explicit provenance args=%q err=%v", args, err)
		}
	}
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

func TestCommandWithProvenanceV1(t *testing.T) {
	base, head := strings.Repeat("a", 40), strings.Repeat("b", 40)
	source, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	command := commandWithProvenanceAndSourceCheckoutV1("bench", []string{"-base-sha", strings.Repeat("c", 40), "--head-sha=" + strings.Repeat("d", 40), "-source-checkout", "old", "--source-checkout=older", "-dataset", "fixture"}, base, head, source)
	if !m8QualificationExactFlagV1(command[1:], "-base-sha", base) || !m8QualificationExactFlagV1(command[1:], "-head-sha", head) || !m8QualificationExactFlagV1(command[1:], "-source-checkout", source) {
		t.Fatalf("command did not retain exactly one canonical provenance pair: %q", command)
	}
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
