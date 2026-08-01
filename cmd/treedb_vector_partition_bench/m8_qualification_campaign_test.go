package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestM8QualificationCampaignBindsThreeHashedRepeatsV1(t *testing.T) {
	root, head := t.TempDir(), strings.Repeat("a", 40)
	fixture := m8QualificationFixturesV1[0]
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: head, HeadSHA: head}
	write := func(name string, matrix m8ProductionMatrixV1) {
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:])})
	}
	for i := 0; i < 3; i++ {
		write("repeat-"+string(rune('1'+i))+".json", testM8QualificationMatrixV1(t, head, fixture, 120+float64(i), i == 0))
	}
	summary, err := m8ValidateQualificationCampaignV1(root, campaign)
	if err != nil {
		t.Fatal(err)
	}
	if summary.P4QPSMedian != 121 || summary.P16QPSMedian != 100 || summary.P4P95Min != 84 || summary.P4P95Median != 85 || summary.P4P95Max != 86 || summary.P16P95Min != 96 || summary.P16P95Median != 97 || summary.P16P95Max != 98 {
		t.Fatalf("summary=%+v", summary)
	}
	index, err := json.Marshal(campaign)
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(root, "campaign.json")
	if err := os.WriteFile(indexPath, index, 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout strings.Builder
	if err := run([]string{"validate-qualification", "-index", indexPath}, &stdout); err != nil || !strings.Contains(stdout.String(), `"p4_qps_median":121`) {
		t.Fatalf("CLI validation err=%v output=%q", err, stdout.String())
	}
	if !strings.Contains(stdout.String(), `"p4_p95_min":84`) || !strings.Contains(stdout.String(), `"p16_p95_max":98`) {
		t.Fatalf("CLI p95 spread output=%q", stdout.String())
	}

	for name, mutate := range map[string]func(*m8QualificationCampaignV1){
		"traversal":        func(c *m8QualificationCampaignV1) { c.Runs[0].Path = "../repeat-1.json" },
		"duplicate_path":   func(c *m8QualificationCampaignV1) { c.Runs[1].Path = c.Runs[0].Path },
		"duplicate_digest": func(c *m8QualificationCampaignV1) { c.Runs[1].SHA256 = c.Runs[0].SHA256 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := campaign
			bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
			mutate(&bad)
			if _, err := m8ValidateQualificationCampaignV1(root, bad); err == nil {
				t.Fatalf("accepted %s campaign identity", name)
			}
		})
	}
	invalid := testM8QualificationMatrixV1(t, head, fixture, 120, true)
	invalid.Variants[0].Rows[0].RouterCandidates = 0
	raw, err := json.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "invalid-child.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	bad := campaign
	bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
	bad.Runs[0] = m8QualificationCampaignRunV1{Path: "invalid-child.json", SHA256: hex.EncodeToString(digest[:])}
	if _, err := m8ValidateQualificationCampaignV1(root, bad); err == nil {
		t.Fatal("accepted an invalid child report")
	}
	derivedTamper := testM8QualificationMatrixV1(t, head, fixture, 120, true)
	derivedTamper.Variants[0].GateLedger.Balance = "fail"
	raw, err = json.Marshal(derivedTamper)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "derived-tamper.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(raw)
	bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
	bad.Runs[0] = m8QualificationCampaignRunV1{Path: "derived-tamper.json", SHA256: hex.EncodeToString(digest[:])}
	if _, err := m8ValidateQualificationCampaignV1(root, bad); err == nil {
		t.Fatal("accepted stale matrix gates after a child-ledger change")
	}
	for name, mutate := range map[string]func(*m8ProductionMatrixV1){
		"wrong_corpus": func(matrix *m8ProductionMatrixV1) {
			matrix.Dataset.Fixture = "untrusted"
			for i := range matrix.Variants {
				matrix.Variants[i].Dataset = matrix.Dataset
			}
		},
		"wrong_config": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.TopK++
			}
		},
		"wrong_repeat_schedule": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.Probes = []int{4, 8, 16}
			}
		},
		"p95_gate": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				for j := range matrix.Variants[i].Rows {
					if matrix.Variants[i].Rows[j].Probes == 4 {
						matrix.Variants[i].Rows[j].P95Nanos = 200
					}
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			matrix := testM8QualificationMatrixV1(t, head, fixture, 120, true)
			mutate(&matrix)
			raw, err := json.Marshal(matrix)
			if err != nil {
				t.Fatal(err)
			}
			path := name + ".json"
			if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			bad := campaign
			bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
			bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:])}
			if _, err := m8ValidateQualificationCampaignV1(root, bad); err == nil {
				t.Fatalf("accepted %s qualification matrix", name)
			}
		})
	}

	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape.json")); err == nil {
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0].Path = "escape.json"
		bad.Runs[0].SHA256 = hex.EncodeToString(digest[:])
		if _, err := m8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted a symlink escaping campaign root")
		}
	}

	broken := testM8QualificationMatrixV1(t, head, fixture, 100, false)
	broken.Variants[1].Rows[0].QPS = 110 // below the required 1.15x p16 comparison
	campaign.Runs = campaign.Runs[:2]
	write("broken.json", broken)
	if _, err := m8ValidateQualificationCampaignV1(root, campaign); err == nil {
		t.Fatal("accepted an under-target p4 QPS repetition")
	}
}

func TestCommitted4027StructuredQualificationPlanV1(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "TreeDB", "docs", "spec", "artifacts", "vector-partition-qualification-4027-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan struct {
		SchemaVersion int    `json:"schema_version"`
		ResultKind    string `json:"result_kind"`
		Status        string `json:"status"`
		Candidate     struct {
			Variant          string `json:"variant"`
			RouterCandidates int    `json:"router_candidates"`
			Probes           []int  `json:"probes"`
			RepeatedProbes   []int  `json:"repeated_probes"`
			Repetitions      int    `json:"repetitions"`
		} `json:"candidate"`
		Corpora []struct {
			ID, Dataset, Checksum string
			Vectors               int   `json:"vectors"`
			GraphCap              int64 `json:"partition_max_distance_work"`
			RouterCap             int64 `json:"router_max_scalar_work"`
			M3Cap                 int64 `json:"m3_max_benchmark_visits"`
			M8Cap                 int64 `json:"m8_max_exact_truth_visits"`
		} `json:"corpora"`
		Commands map[string]string `json:"commands"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.SchemaVersion != 1 || plan.ResultKind != "vector_partition_structured_qualification_campaign_plan_v1" || plan.Status != "planned_no_measurement" || plan.Candidate.Variant != "graph-overlap-020-v1" || plan.Candidate.RouterCandidates != 64 || !slices.Equal(plan.Candidate.Probes, []int{1, 2, 4, 8, 16}) || !slices.Equal(plan.Candidate.RepeatedProbes, []int{4, 16}) || plan.Candidate.Repetitions != 3 || len(plan.Corpora) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if plan.Corpora[0].GraphCap != 20000000000 || plan.Corpora[0].RouterCap != 20000000000 || plan.Corpora[1].Dataset != "testdata/vector_partition_qualification_embedding_mixture_250k" || plan.Corpora[1].Vectors != 250000 || plan.Corpora[1].Checksum != "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69" || plan.Corpora[1].GraphCap != 50000000000 || plan.Corpora[1].RouterCap != 50000000000 || plan.Corpora[1].M3Cap != 900000000 || plan.Corpora[1].M8Cap != 1500000000 {
		t.Fatalf("250k plan=%+v", plan.Corpora[1])
	}
	if !strings.Contains(plan.Commands["m3_graph_disjoint"], "-partition-max-distance-work <graph-cap>") || !strings.Contains(plan.Commands["m3_graph_disjoint"], "-router-max-scalar-work <router-cap>") || !strings.Contains(plan.Commands["m3_graph_overlap"], "-partition-max-distance-work <graph-cap>") || !strings.Contains(plan.Commands["m3_graph_overlap"], "-router-max-scalar-work <router-cap>") || !strings.Contains(plan.Commands["m3_stable_hash_disjoint"], "-router-max-scalar-work <router-cap>") {
		t.Fatalf("graph commands do not bind corpus-specific scalar cap: %+v", plan.Commands)
	}
}

func testM8QualificationMatrixV1(t *testing.T, head string, fixture fixtureManifest, p4QPS float64, fullLadder bool) m8ProductionMatrixV1 {
	t.Helper()
	variants := []struct {
		id, assignment string
		overlap        float64
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0},
	}
	matrix := m8ProductionMatrixV1{Variants: make([]m8ProductionReportV1, 0, len(variants))}
	for _, v := range variants {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = v.id, v.assignment, v.overlap
		if v.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		report := testM8QualificationReportV1(t, head, fixture, descriptor, p4QPS, fullLadder)
		matrix.Variants = append(matrix.Variants, report)
	}
	built, err := m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: []string{"m8-test"}}, fixture, matrix.Variants)
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func testM8QualificationReportV1(t *testing.T, head string, fixture fixtureManifest, descriptor m3VariantDescriptorV1, p4QPS float64, fullLadder bool) m8ProductionReportV1 {
	t.Helper()
	wantOverlap := int(float64(fixture.Vectors) * descriptor.OverlapRatio)
	loads := make([]uint64, 16)
	for row := 0; row < fixture.Vectors+wantOverlap; row++ {
		loads[row%len(loads)]++
	}
	descriptor.Partitions, descriptor.SourceRows, descriptor.Source.Vectors = 16, uint64(fixture.Vectors), fixture.Vectors
	descriptor.OverlapMemberships = wantOverlap
	descriptor.EdgeCutBefore = wantOverlap + 1
	descriptor.PartitionLoads = make([]int, len(loads))
	for i, load := range loads {
		descriptor.PartitionLoads[i] = int(load)
		descriptor.Capacity = max(descriptor.Capacity, int(load))
	}
	descriptor.FixtureChecksum, descriptor.PersistentAssetBytes = fixture.Checksum, 1
	refreshTestM3VariantIdentityV1(t, &descriptor)
	group := func(id string) nativewire.VectorPartitionM8ProductionGroupEvidenceV1 {
		return nativewire.VectorPartitionM8ProductionGroupEvidenceV1{GroupID: id, LeaderID: id + "-leader", NodeIDs: []string{id + "-a", id + "-b", id + "-c"}, CommitIndex: 1, ReadIndex: 1, AppliedIndex: 1, ReadEvidenceKind: "production", ProvesProductionConsensus: true, EndpointHits: 1}
	}
	identity := nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "index-digest", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: uint64(fixture.Vectors), PartitionGeneration: 5, ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest"}
	warm := nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{Identity: identity, ColdOpens: 1, ManifestOpenAttempts: 1, Misses: 1, ReaderPins: 1, LeasePins: 1, LeaseReleases: 1}
	measured := warm
	rowProbes := []int{4, 16}
	if fullLadder {
		rowProbes = []int{1, 2, 4, 8, 16}
	}
	measured.Hits, measured.LeasePins, measured.LeaseReleases = uint64(fixture.Queries*len(rowProbes)), uint64(fixture.Queries*len(rowProbes)+1), uint64(fixture.Queries*len(rowProbes)+1)
	row := func(probes int, qps float64) m8ProductionRowV1 {
		return m8ProductionRowV1{Status: "pass", VariantID: descriptor.VariantID, Probes: probes, EfSearch: 64, Concurrency: 1, Samples: fixture.Queries, RecallAtK: 1, QPS: qps, P95Nanos: uint64(80 + probes + int(p4QPS-120)), RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterCandidates: 64, ExactParityChecked: probes == 16, ExactParityPassed: probes == 16, NoPartialResults: true, Attribution: m8ProductionAttributionV1{Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: 1, ApproximateLocalHNSWRecallAtK: 1, EndToEndRecallAtK: 1, FinalMembershipOracleRecallAtK: 1, CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true, ApproximateRouterCandidateBudget: 64, ApproximateRouterPartitionCoverageComplete: true, LocalHNSWSearches: uint64(fixture.Queries * probes), LocalHNSWCandidates: 1, ApproximateLocalHNSWSearches: uint64(fixture.Queries * probes), ApproximateLocalHNSWCandidates: 1, ResidualLossOwners: []string{"none_observed"}}}
	}
	diagnostics := make([]m8PartitionPackDiagnosticsV1, len(loads))
	for i, load := range loads {
		diagnostics[i] = m8PartitionPackDiagnosticsV1{PartitionID: uint32(i), Rows: load, ReachableRows: load, TraversalRoots: 1}
	}
	rows := make([]m8ProductionRowV1, 0, len(rowProbes))
	for _, probes := range rowProbes {
		qps := 100.0
		if probes == 4 {
			qps = p4QPS
		}
		rows = append(rows, row(probes, qps))
	}
	report := m8ProductionReportV1{SchemaVersion: 3, ResultKind: "m8_production_multi_group_evidence_v3", Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now(), Command: []string{"m8-test"}, BaseSHA: head, HeadSHA: head, Dataset: fixture, Variant: &descriptor, Config: m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: rowProbes, Overlap: []float64{descriptor.OverlapRatio}, TopK: 10, RecallTarget: .90, Concurrency: []int{1}, Warmup: 0, EffectiveWarmup: 0, EfSearch: []int{64}, RouterCandidates: 64, Seed: fixture.Seed}, BuildNanos: 1, Topology: nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{Network: "tcp_loopback_serialized_m5_v1", LifecycleState: "active", ReadySetDigest: strings.Repeat("c", 64), MetaNodes: []string{"meta-a", "meta-b", "meta-c"}, Groups: []nativewire.VectorPartitionM8ProductionGroupEvidenceV1{group("group-a"), group("group-b"), group("group-c"), group("group-d")}}, RouterSessions: m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{warm}, AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{measured}}, Rows: rows, PackDiagnostics: diagnostics, UntimedBoundary: m8ProductionResourceBoundaryV1{SelectedPartitions: 16, EfSearch: 10, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 1, RPCs: 1, RequestBytes: 1, ShardPartitions: 1, ShardRequestBytes: 1}}, Failure: m8ProductionFailureEvidenceV1{Passed: true, Error: "unavailable", ResourceBoundary: m8ProductionFaultResourceBoundaryV1{SelectedPartitions: 16, EfSearch: 4096, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 1, RPCs: 1, RequestBytes: 1, ShardPartitions: 1, ShardRequestBytes: 1}}}, GateLedger: m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}, Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 1, PartitionLoads: loads, OverlapMemberships: wantOverlap}, TruthCache: m8TruthCacheEvidenceV1{ArtifactSHA256: strings.Repeat("d", 64)}, TimedBoundary: "measured", Limitations: []string{"test"}}
	if err := validateM3VariantDescriptorV1(descriptor); err != nil {
		t.Fatalf("qualification descriptor: %v: %+v", err, descriptor)
	}
	if err := validateM8ProductionReportV1(report); err != nil {
		t.Fatalf("valid qualification report rejected: %v", err)
	}
	return report
}
