package main

import (
	"strings"
	"testing"
)

func TestM8ProductionMatrixRequiresLikeForLikeVariantsAndOverlapStorageV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4, 16}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
	reports := make([]m8ProductionReportV1, 0, 3)
	for _, variant := range []struct {
		id, assignment string
		overlap        float64
		bytes          uint64
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0, 100},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2, 120},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0, 101},
	} {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = variant.id, variant.assignment, variant.overlap
		config := common
		config.Overlap = []float64{variant.overlap}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: pass,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: variant.bytes},
			Rows:      []m8ProductionRowV1{{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: .95, QPS: 120, P95Nanos: 10}},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "local_gate_pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio != 1.2 || len(matrix.Comparison) != 3 {
		t.Fatalf("matrix=%+v", matrix)
	}
	reports[1].Resources.PersistentAssetBytes = 135
	matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "experimental_gate_failures" || matrix.Gates.OverlapStorage != "fail" {
		t.Fatalf("storage failure matrix=%+v", matrix)
	}
	reports[2].Config.TopK = 11
	if _, err := m8BuildProductionMatrixV1(cfg, fixture, reports); err == nil {
		t.Fatal("accepted non-like-for-like variant configuration")
	}
}

func TestM8VariantDBsParseStrictThreePathsV1(t *testing.T) {
	base := []string{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"}
	cfg, err := parseConfig(append(append([]string(nil), base...), "-m8-variant-dbs", "/a,/b,/c"))
	if err != nil || len(cfg.m8VariantDBs) != 3 {
		t.Fatalf("variant paths=%v err=%v", cfg.m8VariantDBs, err)
	}
	for _, value := range []string{"/a,/b", "/a,/a,/c", "/a,/b,/c,/d"} {
		if _, err := parseConfig(append(append([]string(nil), base...), "-m8-variant-dbs", value)); err == nil {
			t.Fatalf("accepted malformed variant paths %q", value)
		}
	}
}
