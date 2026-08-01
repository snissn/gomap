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
)

func TestM8QualificationCampaignBindsThreeHashedRepeatsV1(t *testing.T) {
	root, head, fixture := t.TempDir(), strings.Repeat("a", 40), fixtureManifest{Checksum: strings.Repeat("b", 64)}
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, HeadSHA: head}
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
		write("repeat-"+string(rune('1'+i))+".json", testM8QualificationMatrixV1(t, head, fixture, 120+float64(i)))
	}
	summary, err := m8ValidateQualificationCampaignV1(root, campaign)
	if err != nil {
		t.Fatal(err)
	}
	if summary.P4QPSMedian != 121 || summary.P16QPSMedian != 100 || summary.P4P95Max > summary.P16P95Max {
		t.Fatalf("summary=%+v", summary)
	}

	broken := testM8QualificationMatrixV1(t, head, fixture, 100)
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
			Repetitions      int    `json:"repetitions"`
		} `json:"candidate"`
		Corpora []struct {
			ID, Dataset, Checksum string
			Vectors               int   `json:"vectors"`
			M3Cap                 int64 `json:"m3_max_benchmark_visits"`
			M8Cap                 int64 `json:"m8_max_exact_truth_visits"`
		} `json:"corpora"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.SchemaVersion != 1 || plan.ResultKind != "vector_partition_structured_qualification_campaign_plan_v1" || plan.Status != "planned_no_measurement" || plan.Candidate.Variant != "graph-overlap-020-v1" || plan.Candidate.RouterCandidates != 64 || !slices.Equal(plan.Candidate.Probes, []int{4, 16}) || plan.Candidate.Repetitions != 3 || len(plan.Corpora) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	if plan.Corpora[1].Dataset != "testdata/vector_partition_qualification_embedding_mixture_250k" || plan.Corpora[1].Vectors != 250000 || plan.Corpora[1].Checksum != "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69" || plan.Corpora[1].M3Cap != 900000000 || plan.Corpora[1].M8Cap != 1500000000 {
		t.Fatalf("250k plan=%+v", plan.Corpora[1])
	}
}

func testM8QualificationMatrixV1(t *testing.T, head string, fixture fixtureManifest, p4QPS float64) m8ProductionMatrixV1 {
	t.Helper()
	variants := []struct {
		id, assignment string
		overlap        float64
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0},
	}
	matrix := m8ProductionMatrixV1{
		HeadSHA: head, Dataset: fixture, RequiredVariants: append([]string(nil), m8RequiredVariantIDsV1...),
		Gates:               m8ProductionMatrixGatesV1{RequiredVariants: "pass", ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Balance: "pass", ResourceBounds: "pass", OverlapStorage: "pass"},
		OverlapStorageRatio: 1.2,
	}
	for _, v := range variants {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = v.id, v.assignment, v.overlap
		if v.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		p4 := m8ProductionRowV1{Status: "pass", VariantID: v.id, Probes: 4, EfSearch: 64, Concurrency: 1, RouterMode: "approximate", RouterCandidates: 64, Samples: 1, RecallAtK: .95, QPS: p4QPS, P95Nanos: 90, Attribution: m8ProductionAttributionV1{FinalMembershipOracleRecallAtK: .95, ExactToApproximateLossAtK: .005}}
		p16 := m8ProductionRowV1{Status: "pass", VariantID: v.id, Probes: 16, EfSearch: 64, Concurrency: 1, RouterMode: "approximate", RouterCandidates: 64, Samples: 1, RecallAtK: .95, QPS: 100, P95Nanos: 100}
		report := m8ProductionReportV1{HeadSHA: head, Dataset: fixture, Variant: &descriptor, TruthCache: m8TruthCacheEvidenceV1{ArtifactSHA256: strings.Repeat("d", 64)}, Rows: []m8ProductionRowV1{p4, p16}}
		matrix.Variants = append(matrix.Variants, report)
		matrix.Comparison = append(matrix.Comparison, m8ProductionComparisonForRowV1(report, p4), m8ProductionComparisonForRowV1(report, p16))
	}
	return matrix
}
