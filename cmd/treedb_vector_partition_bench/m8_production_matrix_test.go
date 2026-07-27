package main

import (
	"reflect"
	"slices"
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
		descriptor.SourceRows, descriptor.OverlapMemberships = 8, 0
		if variant.overlap > 0 {
			descriptor.OverlapMemberships = 1
		}
		config := common
		config.Overlap = []float64{variant.overlap}
		variantGates := pass
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			variantGates.Recall, variantGates.ProbeReduction, variantGates.EndToEndQPS, variantGates.TailLatency = "fail", "fail", "fail", "fail"
		}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: variantGates,
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
	if _, err := parseConfig(append(append([]string(nil), base...), "positional")); err == nil {
		t.Fatal("accepted positional argument")
	}
}

func TestM8MatrixParentDoesNotMaterializeFixtureV1(t *testing.T) {
	cfg := config{m8VariantDBs: []string{"/a", "/b", "/c"}}
	fixture := fixtureManifest{Vectors: maxVectors, Queries: maxVectors, Dimensions: 4096}
	vectors, queries, err := m8ProductionFixtureDataV1(cfg, fixture)
	if err != nil || vectors != nil || queries != nil {
		t.Fatalf("matrix parent fixture data vectors=%v queries=%v err=%v", vectors, queries, err)
	}
}

func TestM8VariantProcessArgsForceFreshSingleVariantV1(t *testing.T) {
	command := []string{"treedb_vector_partition_bench", "-mode", m8ProductionMultiGroupModeV1, "-dataset", "format", "-m8-variant-dbs", "/a,/b,/c", "-overlap=.1", "-format", "text", "-profiles", "/old"}
	got, err := m8VariantProcessArgsV1(command, "/variant", .2, "/profiles/variant")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"-m8-existing-db", "/variant", "-overlap", "0.2", "-format", "json", "-profiles", "/profiles/variant"}
	if len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("child args=%v want prefix=%v", got, wantPrefix)
	}
	for _, arg := range got {
		if strings.HasPrefix(arg, "-m8-variant-dbs") || strings.HasPrefix(arg, "--m8-variant-dbs") || arg == "/a,/b,/c" || arg == "/old" {
			t.Fatalf("child args retained matrix/old-profile argument: %v", got)
		}
	}
	if !slices.Contains(got, "format") {
		t.Fatalf("child args dropped positional value matching a filtered flag: %v", got)
	}
}

func TestM8ProductionMatrixFailsWhenOverlapBudgetIsUnderMaterializedV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
	reports := make([]m8ProductionReportV1, 0, 3)
	for _, variant := range []struct {
		id, assignment string
		overlap        float64
		memberships    int
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0, 0},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2, 1},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0, 0},
	} {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = variant.id, variant.assignment, variant.overlap
		descriptor.SourceRows, descriptor.OverlapMemberships = 10, variant.memberships
		config := common
		config.Overlap = []float64{variant.overlap}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: pass,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 100},
			Rows:      []m8ProductionRowV1{{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 1}},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Gates.RequiredVariants != "fail" || matrix.Gates.OverlapStorage != "fail" || matrix.Status != "experimental_gate_failures" || matrix.OverlapMaterializationRatio != .1 {
		t.Fatalf("under-materialized overlap matrix=%+v", matrix)
	}
}

func TestM8MatrixIdentityIncludesResourceCapsV1(t *testing.T) {
	cfg := config{headSHA: strings.Repeat("a", 40), m8MaxRSSBytes: 100, m8MaxAssetBytes: 200}
	descriptors := []m3VariantDescriptorV1{testM3VariantDescriptorV1(t.TempDir())}
	one, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	cfg.m8MaxRSSBytes++
	two, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == two {
		t.Fatal("matrix identity ignored the configured RSS acceptance bound")
	}
	cfg.m8MaxRSSBytes--
	cfg.m8MaxAssetBytes++
	three, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == three {
		t.Fatal("matrix identity ignored the configured persistent-asset acceptance bound")
	}
}
