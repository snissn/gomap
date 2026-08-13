package main

import (
	"strings"
	"testing"
)

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
	report := m0FrontierReportV1{Schema: "treedb_vector_partition_m0_calibration_frontier_v1", ManifestIntegrity: sha, ReadySet: sha, PackBytes: 1, AssetChecksumsSHA256: sha, SourceGeneration: 1, SourceChecksum: 1, SourceSchemaHash: 1, SourceRows: 250000, PartitionGeneration: 2, PartitionCount: 32, RouterModelDigest: sha, MembershipReportSHA256: sha, GraphArtifactSHA256: sha, AssignmentArtifactSHA256: sha, DatasetManifestSHA256: sha, BinarySHA256: sha, CalibrationSHA256: sha, TruthSHA256: sha, RouterCandidates: 64, TopK: 10, Measurements: measurements, Cells: canonical}
	if !validateM0FrontierReportV1(report, probes, efs) {
		t.Fatal("valid report rejected")
	}
	report.Measurements[0].ResultSHA256 = strings.Repeat("b", 64)
	if validateM0FrontierReportV1(report, probes, efs) {
		t.Fatal("mixed measurement identity accepted")
	}
}
