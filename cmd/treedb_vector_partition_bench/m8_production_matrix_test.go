package main

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func refreshTestM3VariantIdentityV1(t testing.TB, descriptor *m3VariantDescriptorV1) {
	t.Helper()
	descriptor.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(*descriptor)
	descriptor.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
		Capacity: uint64(descriptor.Capacity), Budget: uint64(math.Floor(descriptor.OverlapRatio * float64(descriptor.SourceRows))),
		BuildIdentityDigest: descriptor.BuildIdentityDigest,
	})
}

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
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		if variant.overlap > 0 {
			descriptor.OverlapMemberships = 1
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		config := common
		config.Overlap = []float64{variant.overlap}
		variantGates := pass
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			variantGates.Recall, variantGates.ProbeReduction, variantGates.EndToEndQPS, variantGates.TailLatency = "fail", "fail", "fail", "fail"
		}
		reports = append(reports, m8ProductionReportV1{
			BaseSHA: hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: variantGates,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: variant.bytes},
			Rows: []m8ProductionRowV1{
				{Status: "pass", VariantID: variant.id, Probes: 16, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: 1, QPS: 100, P95Nanos: 100, ExactParityChecked: true, ExactParityPassed: true},
				{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: .95, QPS: 116, P95Nanos: 99},
			},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "local_gate_pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio != 1.2 || len(matrix.Comparison) != 6 {
		t.Fatalf("matrix=%+v", matrix)
	}
	reports[0].GateLedger.TailLatency = "fail"
	reports[1].GateLedger.Recall = "fail"
	reports[0].Rows[1].P95Nanos = 101
	reports[1].Rows[1].QPS = 114
	matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.Status != "experimental_gate_failures" || matrix.Gates.Recall != "pass" || matrix.Gates.TailLatency != "pass" || matrix.Gates.CoupledGraph != "fail" {
		t.Fatalf("split graph acceptance matrix=%+v", matrix)
	}
	reports[0].GateLedger.TailLatency = "pass"
	reports[1].GateLedger.Recall = "pass"
	reports[0].Rows[1].P95Nanos = 99
	reports[1].Rows[1].QPS = 116
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

func TestM8CoupledGraphGateRequiresOneMatchedOperatingPointV1(t *testing.T) {
	report := m8ProductionReportV1{
		Variant: &m3VariantDescriptorV1{AssignmentBasis: partitionAssignmentGraphV1},
		Config:  m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: .9},
		Rows: []m8ProductionRowV1{
			{Status: "pass", Probes: 16, EfSearch: 64, Concurrency: 1, QPS: 100, P95Nanos: 100, ExactParityChecked: true, ExactParityPassed: true},
			{Status: "pass", Probes: 4, EfSearch: 64, Concurrency: 1, RecallAtK: .95, QPS: 116, P95Nanos: 101},
			{Status: "pass", Probes: 16, EfSearch: 128, Concurrency: 1, QPS: 100, P95Nanos: 100, ExactParityChecked: true, ExactParityPassed: true},
			{Status: "pass", Probes: 4, EfSearch: 128, Concurrency: 1, RecallAtK: .95, QPS: 114, P95Nanos: 99},
		},
	}
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "fail" {
		t.Fatalf("split operating-point gate=%q want fail", got)
	}
	report.Rows[3].QPS = 116
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "pass" {
		t.Fatalf("coupled operating-point gate=%q want pass", got)
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
		if variant.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
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

func TestM8MatrixIdentityIncludesComparisonAndResourceCapsV1(t *testing.T) {
	cfg := config{baseSHA: strings.Repeat("b", 40), headSHA: strings.Repeat("a", 40), m8MaxRSSBytes: 100, m8MaxAssetBytes: 200}
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
	cfg.m8MaxAssetBytes--
	cfg.baseSHA = strings.Repeat("c", 40)
	four, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one == four {
		t.Fatal("matrix identity ignored the comparison base SHA")
	}
	cfg.baseSHA = strings.Repeat("b", 40)
	descriptors[0].DatabaseDirectory = "/relocated/variant"
	five, err := m8MatrixIdentityV1(cfg, descriptors, m8ProductionConfigEvidenceV1{Partitions: 4})
	if err != nil {
		t.Fatal(err)
	}
	if one != five {
		t.Fatal("matrix content identity changed after directory-only relocation")
	}
}

func TestM8VariantBuildCompatibilityRejectsMixedRetainedBuildsV1(t *testing.T) {
	makeVariants := func() []m3VariantDescriptorV1 {
		variants := make([]m3VariantDescriptorV1, 0, 3)
		for _, item := range []struct {
			id, assignment string
			overlap        float64
		}{
			{"graph-disjoint-v1", partitionAssignmentGraphV1, 0},
			{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2},
			{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0},
		} {
			descriptor := testM3VariantDescriptorV1(t.TempDir())
			descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = item.id, item.assignment, item.overlap
			if item.assignment == partitionAssignmentStableIDHashV1 {
				descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
			}
			refreshTestM3VariantIdentityV1(t, &descriptor)
			variants = append(variants, descriptor)
		}
		return variants
	}
	if err := m8ValidateVariantBuildCompatibilityV1(makeVariants()); err != nil {
		t.Fatalf("compatible graph/stable build rejected: %v", err)
	}
	for name, mutate := range map[string]func([]m3VariantDescriptorV1){
		"graph digest": func(variants []m3VariantDescriptorV1) { variants[2].GraphArtifactSHA256 = strings.Repeat("d", 64) },
		"graph assignment artifact": func(variants []m3VariantDescriptorV1) {
			variants[1].ArtifactSHA256 = strings.Repeat("d", 64)
			variants[1].GraphArtifactSHA256 = variants[1].ArtifactSHA256
		},
		"source":             func(variants []m3VariantDescriptorV1) { variants[2].Source.SourceID = "different" },
		"index definition":   func(variants []m3VariantDescriptorV1) { variants[2].IndexDefinitionDigest = strings.Repeat("d", 64) },
		"local HNSW M":       func(variants []m3VariantDescriptorV1) { variants[2].PartitionHNSWM-- },
		"graph router model": func(variants []m3VariantDescriptorV1) { variants[1].RouterModelDigest = strings.Repeat("d", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			variants := makeVariants()
			mutate(variants)
			for i := range variants {
				refreshTestM3VariantIdentityV1(t, &variants[i])
			}
			if err := m8ValidateVariantBuildCompatibilityV1(variants); err == nil {
				t.Fatal("accepted mixed retained variant builds")
			}
		})
	}
}
