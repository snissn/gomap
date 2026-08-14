package main

import (
	"io"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM0FrontierRequiresPinnedCoordinates(t *testing.T) {
	base := []string{"-db", "db", "-dataset", "dataset", "-calibration", "calibration", "-truth-cache", "truth", "-membership-report", "membership", "-assignment-artifact", "assignment", "-graph-artifact", "graph", "-out", t.TempDir() + "/report.json"}
	for _, override := range [][]string{{"-probes", "2,3,4"}, {"-ef", "64,80,128,256"}} {
		if err := runM0CalibrationFrontierV1(append(base, override...), io.Discard); err == nil || !strings.Contains(err.Error(), "requires probes 1,2,4 and EFs 64,80,96,128") {
			t.Fatalf("override %v error = %v", override, err)
		}
	}
}

func TestM0CleanBuildIdentityValidV1(t *testing.T) {
	identity := m0CleanBuildIdentityV1{BinarySHA256: strings.Repeat("a", 64), SourceRevision: strings.Repeat("b", 40)}
	if !m0CleanBuildIdentityValidV1(identity) {
		t.Fatal("clean build identity rejected")
	}
	for _, changed := range []m0CleanBuildIdentityV1{
		{BinarySHA256: "bad", SourceRevision: identity.SourceRevision},
		{BinarySHA256: identity.BinarySHA256, SourceRevision: "bad"},
		{BinarySHA256: identity.BinarySHA256, SourceRevision: identity.SourceRevision, VCSModified: true},
	} {
		if m0CleanBuildIdentityValidV1(changed) {
			t.Fatalf("invalid build identity accepted: %+v", changed)
		}
	}
}

func TestM0FrontierCellsCompleteV1RejectsDuplicateMissingAndMixed(t *testing.T) {
	probes, efs := []int{1, 2, 4}, []int{64, 80, 96, 128}
	cells := make([]m0FrontierCellV1, 0, 12)
	for _, p := range probes {
		for _, ef := range efs {
			cells = append(cells, m0FrontierCellV1{Probes: p, SelectedPartitions: p, RouterSelectedPartitions: uint64(p * 806), EFSearch: ef, Queries: 806, QPS: 1, P50Nanos: 1, P95Nanos: 2, ResultSHA256: strings.Repeat("a", 64), WorkSHA256: strings.Repeat("b", 64)})
		}
	}
	if !m0FrontierCellsCompleteV1(cells, probes, efs, 806) {
		t.Fatal("valid frontier rejected")
	}
	duplicate := append([]m0FrontierCellV1(nil), cells...)
	duplicate[11] = duplicate[0]
	if m0FrontierCellsCompleteV1(duplicate, probes, efs, 806) {
		t.Fatal("duplicate accepted")
	}
	mixed := append([]m0FrontierCellV1(nil), cells...)
	mixed[0].Queries = 805
	if m0FrontierCellsCompleteV1(mixed, probes, efs, 806) {
		t.Fatal("mixed query identity accepted")
	}
	reordered := append([]m0FrontierCellV1(nil), cells...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	if m0FrontierCellsCompleteV1(reordered, probes, efs, 806) {
		t.Fatal("reordered frontier accepted")
	}
	badIdentity := append([]m0FrontierCellV1(nil), cells...)
	badIdentity[0].ResultSHA256 = "not-a-sha"
	if m0FrontierCellsCompleteV1(badIdentity, probes, efs, 806) {
		t.Fatal("mixed result identity accepted")
	}
}

func TestM0FrontierExecutionOrderV1Counterbalances(t *testing.T) {
	points := m0FrontierPlanV1([]int{1, 2}, []int{64, 80})
	if got := m0FrontierExecutionOrderV1(points, 0); got[0] != points[0] || got[3] != points[3] {
		t.Fatal("canonical order")
	}
	if got := m0FrontierExecutionOrderV1(points, 1); got[0] != points[3] || got[3] != points[0] {
		t.Fatal("reversed order")
	}
	if got := m0FrontierExecutionOrderV1(points, 2); got[0] != points[0] || got[1] != points[3] {
		t.Fatal("interleaved order")
	}
}

func TestValidateM0FrontierReportV1RejectsMixedIdentity(t *testing.T) {
	probes, efs := []int{1, 2, 4}, []int{64, 80, 96, 128}
	sha := strings.Repeat("a", 64)
	canonical := make([]m0FrontierCellV1, 0, 12)
	measurements := make([]m0FrontierCellV1, 0, 36)
	for _, p := range probes {
		for _, ef := range efs {
			cell := m0FrontierCellV1{Probes: p, SelectedPartitions: p, RouterSelectedPartitions: uint64(p * 806), EFSearch: ef, Queries: 806, QPS: 1, P50Nanos: 1, P95Nanos: 2, ResultSHA256: sha, WorkSHA256: sha}
			canonical = append(canonical, cell)
			for repetition := 0; repetition < 3; repetition++ {
				cell.Repetition = repetition
				measurements = append(measurements, cell)
			}
		}
	}
	report := m0FrontierReportV1{Schema: "treedb_vector_partition_m0_calibration_frontier_v1", ManifestIntegrity: sha, ReadySet: sha, PackBytes: 1, AssetChecksumsSHA256: sha, SourceGeneration: 1, SourceChecksum: 1, SourceSchemaHash: 1, SourceRows: 250000, PartitionGeneration: 2, PartitionCount: 32, RouterModelDigest: sha, Mode: "zero", MembershipSHA256: sha, MembershipReportSHA256: sha, GraphArtifactSHA256: sha, AssignmentArtifactSHA256: sha, DatasetManifestSHA256: sha, BinarySHA256: sha, SourceRevision: strings.Repeat("a", 40), CalibrationSHA256: sha, TruthSHA256: sha, RouterCandidates: 256, TopK: 10, Measurements: measurements, Cells: canonical}
	if !validateM0FrontierReportV1(report, probes, efs, 256) {
		t.Fatal("valid report rejected")
	}
	if validateM0FrontierReportV1(report, probes, efs, 128) {
		t.Fatal("mismatched candidate budget accepted")
	}
	report.VCSModified = true
	if validateM0FrontierReportV1(report, probes, efs, 256) {
		t.Fatal("modified binary accepted")
	}
	report.VCSModified = false
	report.Measurements[0].ResultSHA256 = strings.Repeat("b", 64)
	if validateM0FrontierReportV1(report, probes, efs, 256) {
		t.Fatal("mixed measurement identity accepted")
	}
}

func TestM0FrontierModeV1AcceptsP40UsefulOnly(t *testing.T) {
	sha := strings.Repeat("a", 64)
	zero := m0MembershipModeV1{Name: "zero", Materialize: true, MembershipSHA256: sha}
	useful := m0MembershipModeV1{Name: "useful_only_20", Used: 50_000, Useful: 50_000, Materialize: true, MembershipSHA256: sha}
	exact := m0MembershipModeV1{Name: "exact_20", Used: 50_000, Useful: 50_000, Materialize: true, MembershipSHA256: sha}
	selected, err := m0FrontierModeV1("useful_only_20", zero, useful, exact, 50_000)
	if err != nil || selected != useful {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	useful.Filler = 1
	if _, err = m0FrontierModeV1("useful_only_20", zero, useful, exact, 50_000); err == nil {
		t.Fatal("filler useful mode accepted")
	}
}

func TestM0FrontierManifestMembershipsEqualV1RejectsDifferentTopology(t *testing.T) {
	actual := []collections.VectorPartitionMembershipV1{{VectorOrdinal: 3, PartitionID: 1}, {VectorOrdinal: 7, PartitionID: 2}}
	if err := m0FrontierManifestMembershipsEqualV1([]collections.VectorPartitionMembershipV1{{VectorOrdinal: 7, PartitionID: 2}, {VectorOrdinal: 3, PartitionID: 1}}, actual); err != nil {
		t.Fatalf("canonical topology rejected: %v", err)
	}
	if err := m0FrontierManifestMembershipsEqualV1([]collections.VectorPartitionMembershipV1{{VectorOrdinal: 3, PartitionID: 2}, {VectorOrdinal: 7, PartitionID: 2}}, actual); err == nil {
		t.Fatal("different overlap topology accepted")
	}
}

func TestM0FrontierCalibrationOrdinalsV1RequiresExactFrozenSet(t *testing.T) {
	ordinals := make([]int, 0)
	for ordinal := 0; ordinal < 1000; ordinal++ {
		if localHNSWCalibrationOrdinalV1(ordinal) {
			ordinals = append(ordinals, ordinal)
		}
	}
	if err := m0FrontierCalibrationOrdinalsV1(ordinals, 1000); err != nil {
		t.Fatal(err)
	}
	ordinals[1] = ordinals[0]
	if err := m0FrontierCalibrationOrdinalsV1(ordinals, 1000); err == nil {
		t.Fatal("accepted duplicate calibration ordinal")
	}
}

func TestM0FrontierLineageV1RejectsDifferentFixtureSource(t *testing.T) {
	hash := strings.Repeat("a", 64)
	fixture := fixtureManifest{Checksum: hash, Vectors: 8, Dimensions: 2, Metric: "cosine"}
	source := vectorpartition.Source{SourceID: "m0_fixture:" + hash, Checksum: strings.Repeat("b", 64), Vectors: fixture.Vectors, Dimensions: fixture.Dimensions, Metric: fixture.Metric}
	artifact := vectorpartition.Artifact{Source: source}
	descriptor := m3VariantDescriptorV1{FixtureChecksum: hash, GraphArtifactSHA256: hash, Source: source}
	account := m0MembershipAccountV1{GraphArtifactSHA256: hash}
	if !m0FrontierLineageV1(descriptor, artifact, account, fixture, uint64(fixture.Vectors)) {
		t.Fatal("valid frontier lineage rejected")
	}
	artifact.Source.Checksum = strings.Repeat("c", 64)
	if m0FrontierLineageV1(descriptor, artifact, account, fixture, uint64(fixture.Vectors)) {
		t.Fatal("different fixture source accepted")
	}
}

func TestM0FrontierAggregateThreeCounterbalancedRepetitions(t *testing.T) {
	plan := m0FrontierPlanV1([]int{1, 2, 4}, []int{64, 80, 96, 128})
	sha := strings.Repeat("a", 64)
	measurements := make([]m0FrontierCellV1, 0, 36)
	for repetition := 0; repetition < 3; repetition++ {
		for _, point := range m0FrontierExecutionOrderV1(plan, repetition) {
			measurements = append(measurements, m0FrontierCellV1{Repetition: repetition, Probes: point.Probes, EFSearch: point.EFSearch, SelectedPartitions: point.Probes, RouterSelectedPartitions: uint64(point.Probes * 806), Queries: 806, QPS: 100 + float64(repetition), P50Nanos: uint64(10 + repetition), P95Nanos: uint64(20 + repetition), ResultSHA256: sha, WorkSHA256: sha})
		}
	}
	aggregates, err := m0FrontierAggregateV1(measurements, plan, 806)
	if err != nil || len(aggregates) != 12 || !m0FrontierCellsCompleteV1(aggregates, []int{1, 2, 4}, []int{64, 80, 96, 128}, 806) {
		t.Fatalf("counterbalanced aggregate err=%v rows=%d", err, len(aggregates))
	}
	for _, cell := range aggregates {
		if cell.QPS != 101 || cell.P50Nanos != 11 || cell.P95Nanos != 21 {
			t.Fatalf("median aggregation=%+v", cell)
		}
	}
}
