package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM8MembershipP2ReferenceMatchesJointDPV1(t *testing.T) {
	limits := m8CoverageLimitsV1{WorkUnits: 1_000_000, Bytes: 1 << 20}
	for _, tc := range []struct {
		masks       []uint16
		costs       []int64
		domains     int
		packs, hits int
	}{
		{[]uint16{1, 2, 4, 3}, []int64{1, 1, 2, 1}, 1, 2, 2},
		{[]uint16{1, 2, 4, 8}, []int64{1, 1, 1, 1}, 2, 2, 2},
		{[]uint16{3, 12, 5}, []int64{2, 1, 1}, 2, 2, 3},
	} {
		dp, err := m8CoverageJointHitsV1(context.Background(), 4, tc.masks, tc.costs, tc.domains, int64(tc.packs), limits)
		if err != nil {
			t.Fatal(err)
		}
		got, err := m8MembershipEnumerateP2V1(tc.masks, tc.costs, tc.domains, int64(tc.packs), limits)
		if err != nil || got.hits != dp || got.hits != tc.hits {
			t.Fatalf("witness=%+v dp=%d want=%d err=%v", got, dp, tc.hits, err)
		}
	}
	tie, err := m8MembershipEnumerateP2V1([]uint16{3, 3}, []int64{1, 1}, 1, 1, limits)
	if err != nil || !reflect.DeepEqual(tie.domains, []uint32{0}) {
		t.Fatalf("deterministic tie=%+v err=%v", tie, err)
	}
	if _, err := m8MembershipEnumerateP2V1([]uint16{1, 2}, []int64{1, 1}, 2, 2, m8CoverageLimitsV1{WorkUnits: 1, Bytes: 1}); err == nil {
		t.Fatal("unbounded direct enumeration accepted")
	}
}

func TestM8MembershipFeasibilityPlanChargesWholePopulationV1(t *testing.T) {
	work, scratch, err := m8MembershipFeasibilityPlanV1(1_024, 10, 16, 2)
	if err != nil || work != 57_184_256 || scratch != 24_832 {
		t.Fatalf("work=%d scratch=%d err=%v", work, scratch, err)
	}
	if work*3 >= maxBenchmarkWorkUnits {
		t.Fatalf("selected three-variant D16 gate exceeds global work cap: %d", work*3)
	}
	if _, _, err := m8MembershipFeasibilityPlanV1(1, 10, 16, 3); err == nil {
		t.Fatal("accepted unsupported domain limit")
	}
}

func TestM8MembershipFeasibilityScratchIsChargedToPeakV1(t *testing.T) {
	cfg := config{
		partitions: 4, overlaps: []float64{0}, probes: []int{2}, efSearch: []int{96}, concurrency: []int{1}, topK: 10,
		m8MembershipProbes: 2, m8MembershipPackLimit: 2,
	}
	fixture := fixtureManifest{Vectors: 10_000, Queries: 1, Dimensions: 1}
	withScratch, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if withScratch.MembershipFeasibilityScratchBytes <= 0 || withScratch.MembershipFeasibilityPopulationBytes <= 0 || withScratch.MembershipFeasibilityRetainedBytes <= 0 || withScratch.MembershipFeasibilityRetainedBytes >= withScratch.MembershipFeasibilityPopulationBytes {
		t.Fatalf("feasibility population or scratch is unmodeled: %+v", withScratch)
	}
	phase, err := memoryAdd(withScratch.FixtureResidentBytes, withScratch.SourceSnapshotBytes, withScratch.ExactTruthBytes, withScratch.MembershipFeasibilityPopulationBytes, withScratch.MembershipFeasibilityScratchBytes)
	if err != nil {
		t.Fatal(err)
	}
	phase, err = memoryScaleCeil(phase, memorySlackNumerator, memorySlackDenominator)
	if err != nil || withScratch.ModeledPeakBytes < phase {
		t.Fatalf("feasibility phase is not charged to the modeled peak: plan=%+v phase=%d err=%v", withScratch, phase, err)
	}
	larger := fixture
	larger.Queries = 100
	largerPlan, err := validateM8BenchmarkWork(cfg, larger, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil || largerPlan.MembershipFeasibilityPopulationBytes <= withScratch.MembershipFeasibilityPopulationBytes {
		t.Fatalf("feasibility population does not scale with queries: small=%d large=%d err=%v", withScratch.MembershipFeasibilityPopulationBytes, largerPlan.MembershipFeasibilityPopulationBytes, err)
	}
	matrix := cfg
	matrix.m8VariantDBs = []string{"a", "b", "c"}
	matrix.m8OracleDomainCounts = []int{4, 4, 4}
	matrixPlan, err := validateM8BenchmarkWork(matrix, fixture, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil || matrixPlan.MembershipFeasibilityPopulationBytes != 3*withScratch.MembershipFeasibilityPopulationBytes {
		t.Fatalf("retained matrix feasibility populations are not cumulative: one=%d matrix=%d err=%v", withScratch.MembershipFeasibilityPopulationBytes, matrixPlan.MembershipFeasibilityPopulationBytes, err)
	}
	if matrixPlan.MembershipFeasibilityRetainedBytes != 3*withScratch.MembershipFeasibilityRetainedBytes {
		t.Fatalf("retained matrix feasibility receipts are not cumulative: one=%d matrix=%d", withScratch.MembershipFeasibilityRetainedBytes, matrixPlan.MembershipFeasibilityRetainedBytes)
	}
	if _, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, withScratch.ModeledPeakBytes-1); err == nil || !strings.Contains(err.Error(), "membership_feasibility_population_bytes=") || !strings.Contains(err.Error(), "membership_feasibility_scratch_bytes=") {
		t.Fatalf("near-cap feasibility run was admitted: %v", err)
	}
	retainedCfg := cfg
	retainedCfg.probes = []int{1, 2}
	retainedCfg.efSearch = []int{16, 24, 32, 40, 48, 56, 64, 72, 80, 88}
	retainedCfg.concurrency = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	retainedFixture := fixtureManifest{Vectors: 100, Queries: 1_000, Dimensions: 1}
	retainedPlan, err := validateM8BenchmarkWork(retainedCfg, retainedFixture, maxBenchmarkWorkUnits, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	retainedMeasurement, err := memoryAdd(retainedPlan.FixtureResidentBytes, retainedPlan.ExactTruthBytes, retainedPlan.MembershipFeasibilityRetainedBytes, retainedPlan.RetainedCoordinatorBytes, retainedPlan.CurrentCellOutcomeBytes, retainedPlan.CurrentQueryConversionBytes)
	if err != nil {
		t.Fatal(err)
	}
	retainedMeasurement, err = memoryScaleCeil(retainedMeasurement, memorySlackNumerator, memorySlackDenominator)
	if err != nil || retainedPlan.ModeledPeakBytes < retainedMeasurement {
		t.Fatalf("retained feasibility receipt is omitted after preflight: plan=%+v measurement=%d err=%v", retainedPlan, retainedMeasurement, err)
	}
}

func TestM8MembershipPhysicalPackExpansionV1(t *testing.T) {
	got, err := m8MembershipExpandDomainsV1([][]uint32{{1, 3}, {2, 4}}, []uint32{0, 1}, 4)
	if err != nil || !reflect.DeepEqual(got, []uint32{1, 2, 3, 4}) {
		t.Fatalf("packs=%v err=%v", got, err)
	}
	for name, tc := range map[string]struct {
		packs   [][]uint32
		domains []uint32
		limit   int64
	}{
		"over budget":        {[][]uint32{{1, 3}, {2, 4}}, []uint32{0, 1}, 3},
		"duplicate pack":     {[][]uint32{{1, 3}, {3, 4}}, []uint32{0, 1}, 4},
		"noncanonical packs": {[][]uint32{{3, 1}}, []uint32{0}, 2},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := m8MembershipExpandDomainsV1(tc.packs, tc.domains, tc.limit); err == nil {
				t.Fatal("invalid expansion accepted")
			}
		})
	}
}

func TestM8MembershipArtifactIsImmutableV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	result := m8MembershipFeasibilityV1{
		SchemaVersion: 1, ResultKind: "m8_membership_feasibility_v1", Method: m8MembershipFeasibilityMethodV1, Status: "sufficient", VariantID: "graph-overlap-020-v1",
		FixtureChecksum: digest, TruthIdentity: "truth", TruthArtifactSHA256: digest, BuildIdentityDigest: digest, ManifestIntegrity: digest, ReadySetDigest: digest, ShardGenerationDigest: digest, MembershipDigest: digest,
		SourceVectors: 1, LogicalDomains: 1, PhysicalPacks: 1, DomainLimit: 1, PackLimit: 1, RequiredRecall: 1, TotalHits: 1, PossibleHits: 1, Ceiling: 1, ActualPackBytes: 10, ActualBytesPerVector: 10,
		WorkBound: 20, ScratchBytes: 48,
		Packs:   []m8MembershipFeasibilityPackV1{{PackID: 0, DomainID: 0, PlannedBytes: 10, ActualBytes: 10}},
		Queries: []m8MembershipFeasibilityQueryV1{{QueryOrdinal: 0, TruthSHA256: digest, TruthCount: 1, Hits: 1, Recall: 1, CoverageMask: 1, SelectedDomain: []uint32{0}, ExpandedPacks: []uint32{0}, PackCost: 1}},
	}
	artifact, err := m8PublishMembershipFeasibilityV1(t.TempDir(), result)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := m8ReadMembershipFeasibilityV1(artifact); err != nil || !reflect.DeepEqual(got, result) {
		t.Fatalf("read=%+v err=%v", got, err)
	}
	reused, err := m8PublishMembershipFeasibilityV1(filepath.Dir(artifact.Path), result)
	if err != nil || !reflect.DeepEqual(reused, artifact) {
		t.Fatalf("identical retry artifact=%+v want=%+v err=%v", reused, artifact, err)
	}
	artifact.Result.Status = "insufficient"
	if _, err := m8ReadMembershipFeasibilityV1(artifact); err == nil {
		t.Fatal("matrix copy tamper accepted")
	}
	if err := os.WriteFile(artifact.Path, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m8PublishMembershipFeasibilityV1(filepath.Dir(artifact.Path), result); err == nil || !strings.Contains(err.Error(), "digest-prefix collision") {
		t.Fatalf("conflicting retry err=%v", err)
	}
}

func TestM8RetainedMembershipFeasibilityReplaysExactAssetsV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	fixture := m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 256, 8, 8
	_, queries := fixtureData(fixture)
	vectors := fixtureVectors(fixture)
	fixture.Checksum = fixtureChecksumFromData(vectors, queries)
	traversal, ok := vectorpartition.AlignedTraversalRowBytesV1(fixture.Dimensions)
	if !ok {
		t.Fatal("invalid traversal-row shape")
	}
	in := vectorpartition.DefaultShardPlanInputV1(fixture.Vectors, fixture.Dimensions)
	in.TargetHotBytes = uint64(vectorpartition.PackFixedOverheadBytesV1 + 20*(traversal+vectorpartition.GraphIdentityOverheadPerRowV1))
	plan, err := vectorpartition.PlanByteBoundedShardsV1(in)
	if err != nil || plan.Partitions != 16 {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "retained")
	descriptor := testM8QualificationRetainedDescriptorWithShardPlanV1(t, dir, strings.Repeat("a", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0, plan)
	truthDir, truth := testM8QualificationTruthCacheV1(t, root, fixture)
	cfg := config{out: filepath.Join(root, "out"), m8TruthCache: truthDir, m8TruthCacheSHA256: truth.ArtifactSHA256, topK: 10, recallTarget: 0, m8MembershipProbes: 2, m8MembershipPackLimit: 2, maxBytes: 1 << 30}
	artifact, err := m8RunMembershipFeasibilityV1(cfg, fixture, vectors, dir, descriptor)
	if err != nil || artifact.Result.Status != "sufficient" {
		t.Fatalf("artifact=%+v err=%v", artifact, err)
	}
	if err := m8ReplayMembershipFeasibilityV1(cfg, fixture, dir, descriptor, artifact); err != nil {
		t.Fatal(err)
	}
	report := m8ProductionReportV1{Dataset: fixture, TruthCache: truth, Variant: &descriptor, Config: m8ProductionConfigEvidenceV1{Partitions: 16, DomainCount: 16, TopK: 10, RecallTarget: 0}}
	if !m8MembershipFeasibilityMatchesReportV1(artifact.Result, report, 2, 2) {
		t.Fatal("exact retained feasibility identity did not bind to its report")
	}
	opened, err := openM8ProductionMultiGroupExistingAssetsWithPolicyV1(dir, []string{"g0", "g1"}, 16, fixture, fixtureVectors(fixture), false)
	if err != nil {
		t.Fatalf("normal retained admission rejected exact shard bytes: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	report.TruthCache.ArtifactSHA256 = strings.Repeat("f", 64)
	if m8MembershipFeasibilityMatchesReportV1(artifact.Result, report, 2, 2) {
		t.Fatal("stale truth identity bound to retained feasibility")
	}
	shardGenerationPath := filepath.Join(dir, m3ShardGenerationFileV1)
	shardGeneration, err := os.ReadFile(shardGenerationPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shardGenerationPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := m8ReplayMembershipFeasibilityV1(cfg, fixture, dir, descriptor, artifact); err == nil {
		t.Fatal("stale shard generation replay accepted")
	}
	if opened, err := openM8ProductionMultiGroupExistingAssetsWithPolicyV1(dir, []string{"g0", "g1"}, 16, fixture, fixtureVectors(fixture), false); err == nil {
		_ = opened.Close()
		t.Fatal("normal retained admission accepted a stale shard generation record without the optional feasibility gate")
	}
	if err := os.WriteFile(shardGenerationPath, shardGeneration, 0o644); err != nil {
		t.Fatal(err)
	}
	mismatchedVectors := make([][]float64, len(vectors))
	for i := range vectors {
		mismatchedVectors[i] = append([]float64(nil), vectors[i]...)
	}
	mismatchedVectors[0][0]++
	if _, err := m8RunMembershipFeasibilityV1(cfg, fixture, mismatchedVectors, dir, descriptor); err == nil || !strings.Contains(err.Error(), "validate retained feasibility fixture") {
		t.Fatalf("retained feasibility accepted mismatched fixture rows: %v", err)
	}
}

func TestM8MembershipFeasibilityRejectsUnplannedRetainedDBV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	fixture := m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 64, 8, 4
	_, queries := fixtureData(fixture)
	fixture.Checksum = fixtureChecksumFromData(fixtureVectors(fixture), queries)
	dir := filepath.Join(t.TempDir(), "retained")
	descriptor := testM8QualificationRetainedDescriptorV1(t, dir, strings.Repeat("a", 40), fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
	if _, err := m8ComputeRetainedMembershipFeasibilityV1(config{}, fixture, fixtureVectors(fixture), dir, descriptor); err == nil || !strings.Contains(err.Error(), "requires a byte-bounded shard plan") {
		t.Fatalf("unplanned retained DB err=%v", err)
	}
}
