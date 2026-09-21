package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func testM8ScalingReportV1() m8ProductionReportV1 {
	r := m8ProductionReportV1{
		ExecutionID: "paired", Dataset: fixtureManifest{Vectors: 100000, Queries: 512, Dimensions: 768},
		Config:  m8ProductionConfigEvidenceV1{MeasurementAccounting: m8CompleteAttemptsV1, MeasuredRepetitions: 5, TopK: 10, RecallTarget: .95, RouterScoreBudget: 256, EfSearch: []int{96}, Concurrency: []int{1, 32}, DomainCount: 4, Partitions: 4, Probes: []int{1, 4}, Overlap: []float64{0}, GraphVariant: string(collections.VectorPartitionLocalGraphVariantConnectivityPreservingVamanaR64L256Alpha1_2V1)},
		Variant: &m3VariantDescriptorV1{Partitions: 4},
	}
	for rep := range 5 {
		for _, concurrency := range []int{1, 32} {
			for _, probes := range []int{1, 4} {
				qps, p95 := 200., uint64(100)
				if probes == 4 {
					qps, p95 = 100, 200
				}
				r.Rows = append(r.Rows, m8ProductionRowV1{Repetition: rep, Concurrency: concurrency, Probes: probes, EfSearch: 96, Samples: 512, Status: "pass", RecallAtK: .97, QPS: qps, P95Nanos: p95, Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Summary: m8MeasurementSummaryV1{Declared: 512, Dispatched: 512, Succeeded: 512, ServiceRecall: .97}}})
			}
		}
	}
	return r
}

func TestM8ScalingComparisonCompleteBlocksV1(t *testing.T) {
	r := testM8ScalingReportV1()
	pair := m8ScalingPairV1{Name: "selected", Kind: "selected_exhaustive", Baseline: "r", Candidate: "r", BaselineProbes: 4, CandidateProbes: 1}
	result, err := m8CompareScalingPairV1(pair, r, r, "", "")
	if err != nil || len(result.Blocks) != 10 || !result.AllMatchedQuality || !result.AllQPS15Percent || !result.AllP95NoRegression {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, block := range result.Blocks {
		if block.QPSRatio != 2 || block.P95Ratio != .5 {
			t.Fatal("wrong row pairing")
		}
	}
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"failed":        func(r *m8ProductionReportV1) { r.Rows[0].Status = "timeout"; r.Rows[0].Accounting.Summary.Succeeded-- },
		"quality":       func(r *m8ProductionReportV1) { r.Rows[0].Accounting.Summary.ServiceRecall = .94 },
		"higher_target": func(r *m8ProductionReportV1) { r.Config.RecallTarget = .99 },
		"throughput":    func(r *m8ProductionReportV1) { r.Rows[0].QPS = 114 },
		"tail":          func(r *m8ProductionReportV1) { r.Rows[0].P95Nanos = 201 },
	} {
		t.Run(name, func(t *testing.T) {
			r := testM8ScalingReportV1()
			mutate(&r)
			result, err := m8CompareScalingPairV1(pair, r, r, "", "")
			if err != nil || len(result.Blocks) != 10 {
				t.Fatalf("dropped rejected row: %v", err)
			}
			if result.AllMatchedQuality && result.AllQPS15Percent && result.AllP95NoRegression {
				t.Fatal("one failed block hidden by other blocks")
			}
		})
	}
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"missing":         func(r *m8ProductionReportV1) { r.Rows = r.Rows[1:] },
		"duplicate":       func(r *m8ProductionReportV1) { r.Rows = append(r.Rows, r.Rows[0]) },
		"legacy":          func(r *m8ProductionReportV1) { r.Config.MeasurementAccounting = "" },
		"one_window":      func(r *m8ProductionReportV1) { r.Config.MeasuredRepetitions = 1 },
		"different_graph": func(r *m8ProductionReportV1) { r.Config.GraphVariant = "native" },
	} {
		t.Run(name, func(t *testing.T) {
			r := testM8ScalingReportV1()
			mutate(&r)
			if _, err := m8CompareScalingPairV1(pair, r, r, "", ""); err == nil {
				t.Fatal("invalid comparison admitted")
			}
		})
	}
}

func TestM8ScalingComparisonIdentityAndPackingV1(t *testing.T) {
	base, candidate := testM8ScalingReportV1(), testM8ScalingReportV1()
	candidate.Variant.OverlapRatio, candidate.Config.Overlap = .2, []float64{.2}
	for i := range candidate.Rows {
		candidate.Rows[i].Overlap = .2
	}
	pair := m8ScalingPairV1{Name: "overlap", Kind: "disjoint_overlap", Baseline: "disjoint", Candidate: "overlap", BaselineProbes: 1, CandidateProbes: 1}
	if _, err := m8CompareScalingPairV1(pair, base, candidate, "", ""); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(candidate)
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"fixture":       func(r *m8ProductionReportV1) { r.Dataset.QueryOrdinalOffset++ },
		"truth":         func(r *m8ProductionReportV1) { r.TruthCache.Identity = "other" },
		"host":          func(r *m8ProductionReportV1) { r.Host.CPUModel = "other" },
		"runtime":       func(r *m8ProductionReportV1) { r.GOMAXPROCS++ },
		"head":          func(r *m8ProductionReportV1) { r.HeadSHA = "other" },
		"executable":    func(r *m8ProductionReportV1) { r.ExecutableSHA256 = "other" },
		"graph":         func(r *m8ProductionReportV1) { r.Variant.GraphBuildSHA256 = "other" },
		"partitioner":   func(r *m8ProductionReportV1) { r.Variant.KaHIPAdapterSHA256 = "other" },
		"warmup":        func(r *m8ProductionReportV1) { r.Config.EffectiveWarmup++ },
		"local_work":    func(r *m8ProductionReportV1) { r.Config.LocalScoreBudget++ },
		"recall_target": func(r *m8ProductionReportV1) { r.Config.RecallTarget = .9 },
	} {
		t.Run(name, func(t *testing.T) {
			var bad m8ProductionReportV1
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad)
			if _, err := m8CompareScalingPairV1(pair, base, bad, "", ""); err == nil {
				t.Fatal("mismatch admitted")
			}
		})
	}
	candidate = testM8ScalingReportV1()
	candidate.Config.Partitions, candidate.Variant.Partitions, candidate.Config.PacksPerDomain = 8, 8, []int{2, 2, 2, 2}
	pair.Kind = "logical_packing"
	digest := strings.Repeat("a", 64)
	if _, err := m8CompareScalingPairV1(pair, base, candidate, digest, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := m8CompareScalingPairV1(pair, base, candidate, digest, strings.Repeat("b", 64)); err == nil {
		t.Fatal("changed logical union accepted")
	}
	if _, err := m8CompareScalingPairV1(pair, base, candidate, "", ""); err == nil {
		t.Fatal("missing union proof accepted")
	}
}

func TestM8ScalingLogicalUnionV1(t *testing.T) {
	one := collections.VectorPartitionManifestV1{SourceRowCount: 4, DomainCount: 2, PartitionCount: 2,
		DomainPacks:        []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 1, PackID: 1}},
		Memberships:        []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 0}, {VectorOrdinal: 2, PartitionID: 1}, {VectorOrdinal: 3, PartitionID: 1}},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}},
	}
	many := collections.VectorPartitionManifestV1{SourceRowCount: 4, DomainCount: 2, PartitionCount: 4,
		DomainPacks:        []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}, {DomainID: 0, PackID: 1}, {DomainID: 1, PackID: 2}, {DomainID: 1, PackID: 3}},
		Memberships:        []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 1}, {VectorOrdinal: 2, PartitionID: 2}, {VectorOrdinal: 3, PartitionID: 3}},
		OverlapMemberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 3}},
	}
	a, err := m8LogicalMembershipDigestV1(one)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m8LogicalMembershipDigestV1(many)
	if err != nil || a != b {
		t.Fatalf("same union, different packs: %s %s %v", a, b, err)
	}
	many.OverlapMemberships[0].VectorOrdinal = 1
	b, err = m8LogicalMembershipDigestV1(many)
	if err != nil || a == b {
		t.Fatal("changed overlap member invisible")
	}
	many.OverlapMemberships[0].VectorOrdinal = 4
	if _, err := m8LogicalMembershipDigestV1(many); err == nil {
		t.Fatal("out-of-range ordinal accepted")
	}
	if _, err := m8LogicalMembershipDigestV1(collections.VectorPartitionManifestV1{}); err == nil {
		t.Fatal("empty layout accepted")
	}
}

func TestM8ScalingComparisonPlanAdmissionV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	for _, raw := range []string{`{}`, `{"reports":[],"pairs":[],"ignored":true}`, `{} {}`} {
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		sha := fmt.Sprintf("%x", sha256.Sum256([]byte(raw)))
		if err := run([]string{"compare-m8-scaling", "-plan", path, "-plan-sha256", sha}, io.Discard); err == nil {
			t.Fatal("accepted malformed plan")
		}
	}
	if err := runM8ScalingComparisonV1([]string{"-plan", path, "-plan-sha256", strings.Repeat("a", 64)}, io.Discard); err == nil {
		t.Fatal("bad plan pin accepted")
	}
}
