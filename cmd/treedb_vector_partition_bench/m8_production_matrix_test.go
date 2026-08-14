package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func refreshTestM3VariantIdentityV1(t testing.TB, descriptor *m3VariantDescriptorV1) {
	t.Helper()
	descriptor.OverlapRequested = int(math.Floor(descriptor.OverlapRatio * float64(descriptor.SourceRows)))
	descriptor.OverlapRealized = descriptor.OverlapMemberships
	descriptor.OverlapRejected = descriptor.OverlapRequested - descriptor.OverlapRealized
	descriptor.OverlapUseful = descriptor.OverlapRealized
	descriptor.OverlapFiller = 0
	descriptor.OverlapUnusedCapacity = descriptor.Capacity*int(descriptor.Partitions) - int(descriptor.SourceRows) - descriptor.OverlapRealized
	descriptor.PartitionLoads = make([]int, descriptor.Partitions)
	for i := 0; i < int(descriptor.SourceRows)+descriptor.OverlapRealized; i++ {
		descriptor.PartitionLoads[i%int(descriptor.Partitions)]++
	}
	descriptor.EdgeCutAfter = descriptor.EdgeCutBefore - descriptor.OverlapUseful
	descriptor.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(*descriptor)
	descriptor.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
		Capacity: uint64(descriptor.Capacity), Budget: uint64(math.Floor(descriptor.OverlapRatio * float64(descriptor.SourceRows))),
		Realized: uint64(descriptor.OverlapMemberships), Unspent: uint64(descriptor.OverlapRejected),
		BuildIdentityDigest: descriptor.BuildIdentityDigest,
	})
}

func TestM8ProductionMatrixOutputPreflightV1(t *testing.T) {
	root := t.TempDir()
	cfg := config{
		baseSHA:  strings.Repeat("a", 40),
		headSHA:  strings.Repeat("b", 40),
		out:      filepath.Join(root, "matrix"),
		profiles: filepath.Join(root, "profiles"),
	}
	descriptors := make([]m3VariantDescriptorV1, len(m8RequiredVariantIDsV1))
	for i, variantID := range m8RequiredVariantIDsV1 {
		descriptors[i].VariantID = variantID
	}
	evidence := m8ProductionConfigEvidenceV1{Partitions: 16}
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.profiles, 0o755); err != nil {
		t.Fatal(err)
	}
	path, err := m8PreflightProductionMatrixOutputV1(cfg, descriptors, evidence)
	if err != nil {
		t.Fatalf("fresh output rejected: %v", err)
	}
	const sentinel = "retain"
	if err := os.WriteFile(path, []byte(sentinel), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m8PreflightProductionMatrixOutputV1(cfg, descriptors, evidence); err == nil || !strings.Contains(err.Error(), "matrix output already exists") {
		t.Fatalf("matrix collision err=%v", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != sentinel {
		t.Fatalf("matrix sentinel=%q err=%v", raw, err)
	}
	if _, err := m8WriteProductionMatrixV1(path, []byte("replacement")); err == nil {
		t.Fatal("matrix writer replaced existing evidence")
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != sentinel {
		t.Fatalf("matrix writer sentinel=%q err=%v", raw, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	for _, variantID := range m8RequiredVariantIDsV1 {
		t.Run(variantID, func(t *testing.T) {
			leaf := filepath.Join(cfg.profiles, variantID)
			if err := os.MkdirAll(leaf, 0o755); err != nil {
				t.Fatal(err)
			}
			artifact := filepath.Join(leaf, "trace.out")
			if err := os.WriteFile(artifact, []byte(sentinel), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := m8PreflightProductionMatrixOutputV1(cfg, descriptors, evidence); err == nil || !strings.Contains(err.Error(), "profile output already exists") {
				t.Fatalf("profile collision err=%v", err)
			}
			if raw, err := os.ReadFile(artifact); err != nil || string(raw) != sentinel {
				t.Fatalf("profile sentinel=%q err=%v", raw, err)
			}
			if err := os.RemoveAll(leaf); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestM8ProductionMatrixProfileLeafReservationV1(t *testing.T) {
	root := t.TempDir()
	profiles := filepath.Join(root, "profiles")
	collision := filepath.Join(profiles, m8RequiredVariantIDsV1[1])
	if err := os.MkdirAll(collision, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(collision, "trace.out")
	if err := os.WriteFile(sentinel, []byte("retain"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := m8CreateProductionMatrixProfileLeavesV1(profiles); err == nil || !strings.Contains(err.Error(), "reserve M8 profile output") {
		t.Fatalf("collision err=%v", err)
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "retain" {
		t.Fatalf("collision sentinel=%q err=%v", raw, err)
	}
	if _, err := os.Stat(filepath.Join(profiles, m8RequiredVariantIDsV1[0])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned partial leaf err=%v", err)
	}
	if err := os.RemoveAll(collision); err != nil {
		t.Fatal(err)
	}
	leaves, err := m8CreateProductionMatrixProfileLeavesV1(profiles)
	if err != nil {
		t.Fatalf("fresh retry reservation: %v", err)
	}
	if len(leaves) != len(m8RequiredVariantIDsV1) {
		t.Fatalf("leaves=%v", leaves)
	}
	for _, leaf := range leaves {
		if info, err := os.Stat(leaf); err != nil || !info.IsDir() {
			t.Fatalf("reserved leaf %s info=%v err=%v", leaf, info, err)
		}
	}
	if err := m8RemoveProductionMatrixProfileLeavesV1(leaves); err != nil {
		t.Fatal(err)
	}
	for _, leaf := range leaves {
		if _, err := os.Stat(leaf); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incomplete leaf %s err=%v", leaf, err)
		}
	}
	if leaves, err = m8CreateProductionMatrixProfileLeavesV1(profiles); err != nil || len(leaves) != len(m8RequiredVariantIDsV1) {
		t.Fatalf("retry leaves=%v err=%v", leaves, err)
	}
}

func TestM8ProductionMatrixPublisherLeavesOnlyCompleteNoReplaceArtifactsV1(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "matrix.json")
	tempPaths := func(t *testing.T) {
		t.Helper()
		paths, err := filepath.Glob(filepath.Join(dir, ".m8_matrix_*.tmp"))
		if err != nil || len(paths) != 0 {
			t.Fatalf("temporary artifacts=%v err=%v", paths, err)
		}
	}
	t.Run("write failure cleans temporary and final paths", func(t *testing.T) {
		linked, err := m8PublishProductionMatrixV1(path, func(w io.Writer) error {
			if _, err := io.WriteString(w, "{"); err != nil {
				return err
			}
			return errors.New("write failure")
		})
		if err == nil {
			t.Fatal("accepted failing matrix write")
		}
		if linked {
			t.Fatal("failing write reported a linked matrix")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial final artifact err=%v", err)
		}
		tempPaths(t)
	})
	t.Run("existing final remains untouched", func(t *testing.T) {
		if err := os.WriteFile(path, []byte("sentinel"), 0o644); err != nil {
			t.Fatal(err)
		}
		if linked, err := m8WriteProductionMatrixV1(path, []byte("replacement")); err == nil || linked || !errors.Is(err, os.ErrExist) {
			t.Fatalf("existing artifact error=%v", err)
		}
		if got, err := os.ReadFile(path); err != nil || string(got) != "sentinel" {
			t.Fatalf("existing artifact=%q err=%v", got, err)
		}
		tempPaths(t)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("complete matrix publishes exact bytes", func(t *testing.T) {
		want := []byte("{\"matrix\":true}\n")
		if linked, err := m8WriteProductionMatrixV1(path, want); err != nil || !linked {
			t.Fatalf("linked=%t err=%v", linked, err)
		}
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("matrix=%q want=%q err=%v", got, want, err)
		}
		tempPaths(t)
	})
	t.Run("post-link sync failure retains published matrix", func(t *testing.T) {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		want := []byte("{\"matrix\":true}\n")
		linked, err := m8PublishProductionMatrixWithDirectorySyncV1(path, func(w io.Writer) error {
			_, err := w.Write(want)
			return err
		}, func(string) error { return errors.New("directory sync failure") })
		if err == nil || !linked {
			t.Fatalf("linked=%t err=%v", linked, err)
		}
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("published matrix=%q want=%q err=%v", got, want, err)
		}
		tempPaths(t)
	})
}

func TestM8PercentileAggregateElapsedLowerBoundV1(t *testing.T) {
	for _, test := range []struct {
		name                         string
		samples, concurrency         int
		p50, p95, p99, maximum, want uint64
		valid                        bool
	}{
		{name: "sequential_1000", samples: 1000, concurrency: 1, p50: 10, p95: 20, p99: 30, maximum: 40, want: 5640, valid: true},
		{name: "eight_workers", samples: 1000, concurrency: 8, p50: 10, p95: 20, p99: 30, maximum: 40, want: 705, valid: true},
		{name: "workers_limited_by_samples", samples: 1, concurrency: 8, p50: 10, p95: 20, p99: 30, maximum: 40, want: 40, valid: true},
		{name: "overflow", samples: int(^uint(0) >> 1), concurrency: 1, p50: ^uint64(0), p95: ^uint64(0), p99: ^uint64(0), maximum: ^uint64(0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := m8PercentileAggregateElapsedLowerBoundV1(test.samples, test.concurrency, test.p50, test.p95, test.p99, test.maximum)
			if ok != test.valid || ok && got != test.want {
				t.Fatalf("bound=%d ok=%t want=%d/%t", got, ok, test.want, test.valid)
			}
		})
	}
}

func TestM8TotalNanosElapsedLowerBoundV1(t *testing.T) {
	for _, test := range []struct {
		name        string
		durations   []uint64
		concurrency int
		want        uint64
		valid       bool
	}{
		{name: "sequential", durations: []uint64{10, 20, 30}, concurrency: 1, want: 60, valid: true},
		{name: "parallel_ceil", durations: []uint64{10, 20, 30}, concurrency: 2, want: 30, valid: true},
		{name: "workers_limited_by_samples", durations: []uint64{10}, concurrency: 8, want: 10, valid: true},
		{name: "overflow", durations: []uint64{^uint64(0), 1}, concurrency: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := m8TotalNanosElapsedLowerBoundV1(test.durations, test.concurrency)
			if ok != test.valid || ok && got != test.want {
				t.Fatalf("bound=%d ok=%t want=%d/%t", got, ok, test.want, test.valid)
			}
		})
	}
}

func TestM8ProductionMatrixRequiresLikeForLikeVariantsAndOverlapStorageV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4, 16}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
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
			ExecutableSHA256: strings.Repeat("a", 64),
			BaseSHA:          hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: variantGates,
			Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: variant.bytes},
			Rows: []m8ProductionRowV1{
				{Status: "pass", VariantID: variant.id, Probes: 16, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: 1, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
				{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 32, RecallAtK: .95, QPS: 116, P95Nanos: 99},
			},
		})
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, reports)
	if err != nil {
		t.Fatal(err)
	}
	if matrix.ExecutionStartedAt.IsZero() || !matrix.ExecutionCompletedAt.After(matrix.ExecutionStartedAt) {
		t.Fatalf("matrix did not retain a valid execution interval: %+v", matrix)
	}
	if matrix.Status != "local_gate_pass" || matrix.Gates.PartitionPackReachability != "pass" || matrix.Gates.OverlapStorage != "pass" || matrix.OverlapStorageRatio != 1.2 || len(matrix.Comparison) != 6 {
		t.Fatalf("matrix=%+v", matrix)
	}
	shortfallReports := append([]m8ProductionReportV1(nil), reports...)
	shortfallReports[0].Rows = append([]m8ProductionRowV1(nil), reports[0].Rows...)
	shortfallReports[0].Rows[1].Status = "candidate_coverage_shortfall"
	shortfallMatrix, err := m8BuildProductionMatrixV1(cfg, fixture, shortfallReports)
	if err != nil {
		t.Fatal(err)
	}
	var shortfallAt = -1
	for i, comparison := range shortfallMatrix.Comparison {
		if comparison.VariantID == shortfallReports[0].Variant.VariantID && comparison.Probes == shortfallReports[0].Rows[1].Probes && comparison.EfSearch == shortfallReports[0].Rows[1].EfSearch && comparison.Concurrency == shortfallReports[0].Rows[1].Concurrency {
			shortfallAt = i
			if comparison.Status != "candidate_coverage_shortfall" {
				t.Fatalf("shortfall comparison status=%q", comparison.Status)
			}
		}
	}
	if shortfallAt < 0 {
		t.Fatal("missing shortfall comparison")
	}
	raw, err := json.Marshal(shortfallMatrix)
	if err != nil {
		t.Fatal(err)
	}
	var decoded m8ProductionMatrixV1
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateM8ProductionMatrixV1(decoded); err != nil {
		t.Fatalf("round-tripped shortfall matrix rejected: %v", err)
	}
	for name, mutate := range map[string]func(*m8ProductionMatrixV1){
		"zero_execution_start":        func(m *m8ProductionMatrixV1) { m.ExecutionStartedAt = time.Time{} },
		"reversed_execution_interval": func(m *m8ProductionMatrixV1) { m.ExecutionCompletedAt = m.ExecutionStartedAt },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := decoded
			mutate(&invalid)
			if err := validateM8ProductionMatrixV1(invalid); err == nil || !strings.Contains(err.Error(), "execution interval") {
				t.Fatalf("interval err=%v", err)
			}
		})
	}
	for _, mutation := range []struct {
		name  string
		apply func(*m8ProductionComparisonV1)
	}{
		{"status_pass", func(comparison *m8ProductionComparisonV1) { comparison.Status = "pass" }},
		{"status_fail", func(comparison *m8ProductionComparisonV1) { comparison.Status = "fail" }},
		{"status_blank", func(comparison *m8ProductionComparisonV1) { comparison.Status = "" }},
		{"recall", func(comparison *m8ProductionComparisonV1) { comparison.RecallAtK = .5 }},
		{"qps", func(comparison *m8ProductionComparisonV1) { comparison.QPS++ }},
		{"resource", func(comparison *m8ProductionComparisonV1) { comparison.PersistentAssetBytes++ }},
		{"artifact_digest", func(comparison *m8ProductionComparisonV1) { comparison.ArtifactSHA256 = strings.Repeat("d", 64) }},
	} {
		invalid := decoded
		invalid.Comparison = append([]m8ProductionComparisonV1(nil), decoded.Comparison...)
		mutation.apply(&invalid.Comparison[shortfallAt])
		if err := validateM8ProductionMatrixV1(invalid); err == nil {
			t.Fatalf("accepted tampered shortfall comparison %s", mutation.name)
		}
	}
	for _, reachability := range []string{"", "fail"} {
		reports[0].GateLedger.PartitionPackReachability = reachability
		matrix, err = m8BuildProductionMatrixV1(cfg, fixture, reports)
		if err != nil {
			t.Fatal(err)
		}
		if matrix.Status != "experimental_gate_failures" || matrix.Gates.PartitionPackReachability != "fail" {
			t.Fatalf("reachability=%q matrix=%+v", reachability, matrix)
		}
	}
	reports[0].GateLedger.PartitionPackReachability = "pass"
	reports[0].Dirty = true
	if _, err := m8BuildProductionMatrixV1(cfg, fixture, reports); err == nil || !strings.Contains(err.Error(), "dirty") {
		t.Fatalf("dirty child report err=%v", err)
	}
	reports[0].Dirty = false
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
			{Status: "pass", Probes: 16, EfSearch: 64, Concurrency: 1, RecallAtK: .95, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
			{Status: "pass", Probes: 4, EfSearch: 64, Concurrency: 1, RecallAtK: .95, QPS: 116, P95Nanos: 101},
			{Status: "pass", Probes: 16, EfSearch: 128, Concurrency: 1, RecallAtK: .95, QPS: 100, P95Nanos: 100, Attribution: m8ProductionAttributionV1{ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true}},
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
	report.Rows[2].Status = "fail"
	report.Rows[2].ExactParityPassed = false
	report.Rows[2].RecallAtK = .95
	if got := m8AnyGraphVariantCoupledGatesPassV1([]m8ProductionReportV1{report}); got != "fail" {
		t.Fatalf("failed exhaustive baseline must be excluded: gate=%q want fail", got)
	}
}

func TestM8DecisionReportUsesLowestQuarterProbeOperatingPointV1(t *testing.T) {
	descriptor := testM3VariantDescriptorV1(t.TempDir())
	attribution := m8ProductionAttributionV1{GlobalExactRecallAtK: 1, OracleStagesComplete: true, PrimaryHomeOracleRecallAtK: .8, FinalMembershipOracleRecallAtK: .9, ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExactRepresentativeRecallAtK: .7, ApproximateRepresentativeRecallAtK: .7, LocalHNSWRecallAtK: .7, ApproximateLocalHNSWRecallAtK: .7, EndToEndRecallAtK: .7}
	attribution.StageOwners = m8AttributionStageOwnersV1(attribution)
	report := m8ProductionReportV1{Variant: &descriptor, Config: m8ProductionConfigEvidenceV1{Partitions: 32}, Rows: []m8ProductionRowV1{
		{Status: "pass", Probes: 8, EfSearch: 128, Concurrency: 1, Attribution: attribution},
		{Status: "pass", Probes: 8, EfSearch: 64, Concurrency: 1, Attribution: attribution},
		{Status: "pass", Probes: 4, EfSearch: 32, Concurrency: 1, Attribution: attribution},
	}}
	got := m8DecisionReportV1([]m8ProductionReportV1{report})
	if len(got) != 4 {
		t.Fatalf("decision=%+v", got)
	}
	for _, row := range got {
		if row.Probes != 8 || row.EfSearch != 64 || row.VariantID != descriptor.VariantID {
			t.Fatalf("decision row=%+v", row)
		}
	}
}

func TestM8DecisionReportRetainsVariantWithoutQuarterProbeRowV1(t *testing.T) {
	descriptor := testM3VariantDescriptorV1(t.TempDir())
	report := m8ProductionReportV1{Variant: &descriptor, Config: m8ProductionConfigEvidenceV1{Partitions: 16}, Rows: []m8ProductionRowV1{{Status: "fail", Probes: 4}}}
	got := m8DecisionReportV1([]m8ProductionReportV1{report})
	if len(got) != 1 || got[0].VariantID != descriptor.VariantID || got[0].Probes != 4 || got[0].Stage != "none" || got[0].Owner != "no_quarter_probe_operating_point" {
		t.Fatalf("decision=%+v", got)
	}
}

func TestM8AllPartitionANNDoesNotOwnExactParityV1(t *testing.T) {
	report := m8ProductionReportV1{
		Config: m8ProductionConfigEvidenceV1{Partitions: 16, RecallTarget: .9},
		Rows: []m8ProductionRowV1{{
			Status: "pass", Probes: 16, EfSearch: 64, Concurrency: 1, Samples: 1,
			// The measured all-partition HNSW path is approximate: it may miss a
			// neighbor without turning the exact-union correctness gate into fail.
			RecallAtK: .9998, ExactParityChecked: false, ExactParityPassed: false,
			Attribution: m8ProductionAttributionV1{ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExhaustivePartitionRecallAtK: 1},
		}},
	}
	if got := m8ProductionGateLedgerForReportV1(report).ExhaustiveParity; got != "pass" {
		t.Fatalf("all-partition ANN conflated with exact union: %q", got)
	}
}

func TestM8TruthCacheIdentityMismatchFailsClosedV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Vectors: 1, Dimensions: 128, Metric: "cosine"}
	identity := m8TruthCacheIdentityV1(fixture, 10)
	dir := t.TempDir()
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: strings.Repeat("b", 64), Dimensions: 128, Metric: "cosine", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{}, fixture, [][]float64{{1}}, 10, ""); err == nil || !strings.Contains(err.Error(), "identity/schema mismatch") {
		t.Fatalf("cache mismatch err=%v", err)
	}
}

func TestM8TruthCacheOversizedInputFailsBeforeDecodeV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	path := filepath.Join(dir, "m8_canonical_truth_"+identity+".json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 70<<10)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{}, fixture, [][]float64{{1, 0}}, 1, ""); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized cache err=%v", err)
	}
}

func TestM8TruthCacheHitReusesCanonicalRowsV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("c", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	want := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	truthSHA256, err := m8TruthContentSHA256V1(want)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: truthSHA256, Truth: want})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	artifact := sha256.Sum256(raw)
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, ""); err == nil || !strings.Contains(err.Error(), "independently trusted digest") {
		t.Fatalf("cache without trusted digest err=%v", err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, strings.Repeat("0", 64)); err == nil || !strings.Contains(err.Error(), "independently trusted digest") {
		t.Fatalf("cache with wrong trusted digest err=%v", err)
	}
	got, evidence, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, hex.EncodeToString(artifact[:]))
	if err != nil || evidence.Status != "reused" || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v evidence=%+v err=%v", got, evidence, err)
	}
}

func TestM8TruthCacheWorstCanonicalEncodingFitsBoundV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("g", 64), Vectors: 1_000_001, Dimensions: 2, Metric: "cosine"}
	identity := m8TruthCacheIdentityV1(fixture, 1)
	truth := [][]m8CanonicalResultV1{{{ID: "doc-1000000", Score: math.MaxFloat32}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := m8TruthCacheMaxBytesV1(1, 1, fixture.Vectors)
	if err != nil || int64(len(raw)) > bound {
		t.Fatalf("raw=%d bound=%d err=%v", len(raw), bound, err)
	}
}

func TestM8TruthCacheStreamEncodingMatchesJSONV1(t *testing.T) {
	truth := [][]m8CanonicalResultV1{{{ID: "doc-1000000", Score: math.MaxFloat32}, {ID: "doc-000000", Score: -math.MaxFloat32}}, {}}
	wantTruth, err := json.Marshal(truth)
	if err != nil {
		t.Fatal(err)
	}
	var gotTruth bytes.Buffer
	if err := m8WriteTruthJSONV1(&gotTruth, truth); err != nil || !bytes.Equal(gotTruth.Bytes(), wantTruth) {
		t.Fatalf("truth bytes=%q want=%q err=%v", gotTruth.Bytes(), wantTruth, err)
	}
	file := m8TruthCacheFileV1{SchemaVersion: 1, Identity: "id", Contract: m8CanonicalTruthContractV1, DatasetChecksum: strings.Repeat("a", 64), Dimensions: 2, Metric: "cosine", TopK: 2, TruthSHA256: "digest", Truth: truth}
	wantFile, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	var gotFile bytes.Buffer
	if err := m8WriteTruthCacheJSONV1(&gotFile, file); err != nil || !bytes.Equal(gotFile.Bytes(), wantFile) {
		t.Fatalf("file bytes=%q want=%q err=%v", gotFile.Bytes(), wantFile, err)
	}
	digest, err := m8TruthContentSHA256V1(truth)
	sum := sha256.Sum256(wantTruth)
	if err != nil || digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest=%q err=%v", digest, err)
	}
}

func TestM8TruthCachePublisherLeavesOnlyCompleteNoReplaceArtifactsV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	path := m8TruthCacheArtifactPathV1(dir, identity)
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	truthSHA, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	file := m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: fixture.Dimensions, Metric: fixture.Metric, TopK: 1, TruthSHA256: truthSHA, Truth: truth}
	write := func(w io.Writer) error { return m8WriteTruthCacheJSONV1(w, file) }
	t.Run("write failure cleans temporary and final paths", func(t *testing.T) {
		_, linked, err := m8PublishTruthCacheV1(path, "", func(w io.Writer) error {
			if _, err := io.WriteString(w, "{"); err != nil {
				return err
			}
			return errors.New("write failure")
		})
		if err == nil || linked {
			t.Fatalf("failing truth-cache write linked=%t err=%v", linked, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial final artifact err=%v", err)
		}
		if paths, err := filepath.Glob(filepath.Join(dir, ".m8_canonical_truth_*.tmp")); err != nil || len(paths) != 0 {
			t.Fatalf("temporary artifacts=%v err=%v", paths, err)
		}
	})
	t.Run("existing final remains untouched", func(t *testing.T) {
		if err := os.WriteFile(path, []byte("sentinel"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, linked, err := m8PublishTruthCacheV1(path, "", write); err == nil || linked || !os.IsExist(err) {
			t.Fatalf("existing artifact linked=%t err=%v", linked, err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != "sentinel" {
			t.Fatalf("existing artifact=%q err=%v", got, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("trusted digest mismatch does not publish", func(t *testing.T) {
		if _, linked, err := m8PublishTruthCacheV1(path, strings.Repeat("0", 64), write); err == nil || linked {
			t.Fatalf("mismatched trusted digest linked=%t err=%v", linked, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("mismatched digest published final artifact err=%v", err)
		}
		if paths, err := filepath.Glob(filepath.Join(dir, ".m8_canonical_truth_*.tmp")); err != nil || len(paths) != 0 {
			t.Fatalf("temporary artifacts=%v err=%v", paths, err)
		}
	})
	t.Run("complete artifact publishes and decodes", func(t *testing.T) {
		digest, linked, err := m8PublishTruthCacheV1(path, "", write)
		if err != nil || !linked {
			t.Fatalf("complete truth-cache publish linked=%t err=%v", linked, err)
		}
		got, artifact, err := m8ReadTruthCacheV1(path, fixture, 1, 1, 1, digest)
		if err != nil || artifact != digest || !reflect.DeepEqual(got, truth) {
			t.Fatalf("artifact=%q digest=%q truth=%v err=%v", artifact, digest, got, err)
		}
		if paths, err := filepath.Glob(filepath.Join(dir, ".m8_canonical_truth_*.tmp")); err != nil || len(paths) != 0 {
			t.Fatalf("temporary artifacts=%v err=%v", paths, err)
		}
	})
	t.Run("post-link sync failure retains decodable cache and digest", func(t *testing.T) {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		digest, linked, err := m8PublishTruthCacheWithDirectorySyncV1(path, "", write, func(string) error {
			return errors.New("directory sync failure")
		})
		if err == nil || !linked || digest == "" {
			t.Fatalf("post-link sync linked=%t digest=%q err=%v", linked, digest, err)
		}
		got, artifact, err := m8ReadTruthCacheV1(path, fixture, 1, 1, 1, digest)
		if err != nil || artifact != digest || !reflect.DeepEqual(got, truth) {
			t.Fatalf("artifact=%q digest=%q truth=%v err=%v", artifact, digest, got, err)
		}
		if paths, err := filepath.Glob(filepath.Join(dir, ".m8_canonical_truth_*.tmp")); err != nil || len(paths) != 0 {
			t.Fatalf("temporary artifacts=%v err=%v", paths, err)
		}
	})
}

func TestM8TruthCacheWhitespaceBoundAndExactByteDigestV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("h", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := m8TruthCacheMaxBytesV1(1, 1, fixture.Vectors)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "m8_canonical_truth_"+identity+".json")
	over := append(append([]byte(nil), raw...), []byte(strings.Repeat(" ", int(bound-int64(len(raw))+1)))...)
	if err := os.WriteFile(path, over, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, ""); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("padded cache err=%v", err)
	}
	within := append(append([]byte(nil), raw...), []byte(" \n\t")...)
	if err := os.WriteFile(path, within, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(within)
	_, evidence, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, hex.EncodeToString(sum[:]))
	if err != nil || evidence.Status != "reused" || evidence.ArtifactSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("evidence=%+v err=%v", evidence, err)
	}
}

func TestM8TruthCacheRejectsTrailingJSONV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("f", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .5}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte(" {}")...)
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, ""); err == nil || !strings.Contains(err.Error(), "trailing JSON") {
		t.Fatalf("trailing JSON err=%v", err)
	}
}

func TestM8TruthCacheRefusesContentCorruptionAndSemanticMalformationV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("d", 64), Vectors: 2, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 2), t.TempDir()
	path := filepath.Join(dir, "m8_canonical_truth_"+identity+".json")
	good := [][]m8CanonicalResultV1{{{ID: "doc-000000", Score: .9}, {ID: "doc-000001", Score: .8}}}
	contentSHA, err := m8TruthContentSHA256V1(good)
	if err != nil {
		t.Fatal(err)
	}
	file := m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: collections.VectorPartitionCanonicalScoreContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 2, TruthSHA256: contentSHA, Truth: good}
	write := func(t *testing.T, file m8TruthCacheFileV1) {
		t.Helper()
		raw, marshalErr := json.Marshal(file)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if writeErr := os.WriteFile(path, raw, 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	write(t, file)
	file.Truth[0][0].Score = .7 // JSON remains valid but the content digest does not.
	write(t, file)
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 2}, fixture, [][]float64{{1, 0}}, 2, ""); err == nil || !strings.Contains(err.Error(), "truth_sha256") {
		t.Fatalf("content corruption err=%v", err)
	}
	file.Truth[0][0].Score = .7
	file.Truth[0][1].Score = .8 // ascending scores are semantically malformed.
	file.TruthSHA256, err = m8TruthContentSHA256V1(file.Truth)
	if err != nil {
		t.Fatal(err)
	}
	write(t, file)
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 2}, fixture, [][]float64{{1, 0}}, 2, ""); err == nil || !strings.Contains(err.Error(), "noncanonical") {
		t.Fatalf("semantic malformation err=%v", err)
	}
}

func TestM8TruthCacheRefusesIDsOutsideFixtureDomainV1(t *testing.T) {
	fixture := fixtureManifest{Checksum: strings.Repeat("e", 64), Vectors: 1, Dimensions: 2, Metric: "cosine"}
	identity, dir := m8TruthCacheIdentityV1(fixture, 1), t.TempDir()
	truth := [][]m8CanonicalResultV1{{{ID: "doc-000001", Score: .5}}}
	digest, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: 2, Metric: "cosine", TopK: 1, TruthSHA256: digest, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "m8_canonical_truth_"+identity+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m8LoadOrComputeTruthV1(dir, nil, collections.VectorPartitionManifestV1{SourceRowCount: 1}, fixture, [][]float64{{1, 0}}, 1, ""); err == nil || !strings.Contains(err.Error(), "outside deterministic fixture domain") {
		t.Fatalf("out-of-range cache ID err=%v", err)
	}
}

func TestCommittedV1QualificationLedgerArtifactsV1(t *testing.T) {
	type publishedArtifact struct {
		Role   string `json:"role"`
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	type ledger struct {
		SchemaVersion    int                 `json:"schema_version"`
		Status           string              `json:"status"`
		BlockingFollowUp string              `json:"blocking_follow_up"`
		ParentAcceptance string              `json:"parent_acceptance"`
		Gates            map[string]string   `json:"gates"`
		Artifacts        []publishedArtifact `json:"raw_artifacts"`
		Commands         struct {
			Canonical string `json:"canonical"`
		} `json:"commands"`
	}
	root := filepath.Join("..", "..")
	ledgerPath := filepath.Join(root, "TreeDB", "docs", "spec", "artifacts", "vector-partition-v1-qualification-4015.json")
	raw, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	var got ledger
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Status != "experimental_off_gate_failures_not_1m_qualified" || got.BlockingFollowUp != "#4022" || got.ParentAcceptance != "https://github.com/snissn/gomap/issues/4012#issuecomment-5123955990" {
		t.Fatalf("ledger linkage/schema/status=%+v", got)
	}
	if got.Gates["quarter_probe_recall_ge_090"] == "pass" || got.Gates["required_1m"] == "pass" || !strings.HasPrefix(got.Gates["required_1m"], "deferred") {
		t.Fatalf("ledger incorrectly permits qualification: gates=%v", got.Gates)
	}
	for _, required := range []string{"-mode", "-dataset", "-out", "-m8-existing-db", "-m8-max-exact-truth-visits", "-m8-truth-cache-sha256 bf59243ed023eb4f1770a01373113b5f0f5a845d554d8b47318285dd7f8a4a62"} {
		if !strings.Contains(got.Commands.Canonical, required) {
			t.Fatalf("canonical replay command omits %s: %s", required, got.Commands.Canonical)
		}
	}
	if len(got.Artifacts) != 13 {
		t.Fatalf("published artifact count=%d want 13", len(got.Artifacts))
	}
	for _, artifact := range got.Artifacts {
		if artifact.Role == "" || artifact.Path == "" || artifact.SHA256 == "" || filepath.IsAbs(artifact.Path) {
			t.Fatalf("invalid published artifact=%+v", artifact)
		}
		content, err := os.ReadFile(filepath.Join(root, artifact.Path))
		if err != nil {
			t.Fatalf("read %s: %v", artifact.Path, err)
		}
		sum := sha256.Sum256(content)
		if hex.EncodeToString(sum[:]) != artifact.SHA256 {
			t.Fatalf("digest mismatch %s", artifact.Path)
		}
	}
}

func TestCommitted4023AttributionLedgerArtifactsV1(t *testing.T) {
	type publishedArtifact struct {
		Role   string `json:"role"`
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	}
	var got struct {
		SchemaVersion    int                 `json:"schema_version"`
		Status           string              `json:"status"`
		Disposition      string              `json:"disposition"`
		MeasuredCodeHead string              `json:"measured_code_head"`
		Gates            map[string]string   `json:"gates"`
		Commands         map[string]string   `json:"commands"`
		Artifacts        []publishedArtifact `json:"raw_artifacts"`
	}
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "TreeDB", "docs", "spec", "artifacts", "vector-partition-attribution-4023.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Status != "experimental_gate_failures" || got.Disposition != "enablement_off_follow_up_required" || got.MeasuredCodeHead != "6d69bb99ab2e30ff33045fc39e3041780fc63be3" {
		t.Fatalf("ledger linkage/schema/status=%+v", got)
	}
	if got.Gates["exhaustive_correctness"] != "pass" || got.Gates["probe_reduction"] != "fail" || got.Gates["existing_behavior"] != "pending_latest_head_required_suites" {
		t.Fatalf("ledger gates=%v", got.Gates)
	}
	for _, required := range []string{"-m8-variant-dbs", "-probes 1,2,4,8,16", "-m8-max-exact-truth-visits 600000000"} {
		if !strings.Contains(got.Commands["matrix"], required) {
			t.Fatalf("matrix replay command omits %s: %s", required, got.Commands["matrix"])
		}
	}
	if len(got.Artifacts) != 6 {
		t.Fatalf("published artifact count=%d want 6", len(got.Artifacts))
	}
	for _, artifact := range got.Artifacts {
		content, err := os.ReadFile(filepath.Join(root, artifact.Path))
		if err != nil {
			t.Fatalf("read %s: %v", artifact.Path, err)
		}
		sum := sha256.Sum256(content)
		if artifact.Role == "" || artifact.SHA256 == "" || filepath.IsAbs(artifact.Path) || hex.EncodeToString(sum[:]) != artifact.SHA256 {
			t.Fatalf("invalid published artifact=%+v", artifact)
		}
	}
}

func TestM8GitDirtyRequiresExternalOutputsAndPreservesSourceChangesV1(t *testing.T) {
	repo := t.TempDir()
	var err error
	repo, err = m8CanonicalPathV1(repo)
	if err != nil {
		t.Fatal(err)
	}
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	runGit("init", "-q")
	runGit("config", "user.name", "M8 Test")
	runGit("config", "user.email", "m8@example.invalid")
	tracked := filepath.Join(repo, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-qm", "initial")
	headCommand := exec.Command("git", "rev-parse", "HEAD")
	headCommand.Dir = repo
	headRaw, err := headCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	head := strings.TrimSpace(string(headRaw))
	checkout, err := m8SourceCheckoutV1(repo, head)
	if err != nil || checkout != repo {
		t.Fatalf("clean unrelated checkout=%q err=%v", checkout, err)
	}
	if _, err := m8SourceCheckoutV1(repo, strings.Repeat("0", 40)); err == nil {
		t.Fatal("accepted wrong checkout head")
	}
	artifactRoot := t.TempDir()
	out, profiles := filepath.Join(artifactRoot, "out"), filepath.Join(artifactRoot, "profiles")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if m8GitDirtyInV1(repo, out, profiles) {
		t.Fatal("declared benchmark output made clean repository dirty")
	}
	insideRoot := filepath.Join(repo, "TreeDB")
	if !m8GitDirtyInV1(repo, insideRoot) {
		t.Fatal("accepted an evidence root inside the source repository")
	}
	if !m8GitDirtyInV1(repo, repo) {
		t.Fatal("accepted the source repository itself as an evidence root")
	}
	if err := os.WriteFile(tracked, []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !m8GitDirtyInV1(repo, out, profiles) {
		t.Fatal("source change was hidden by benchmark output exclusions")
	}
}

func TestM8GitDirtyRejectsSymlinkedInRepositoryOutputV1(t *testing.T) {
	repo := t.TempDir()
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = repo
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	runGit("init", "-q")
	runGit("config", "user.name", "M8 Test")
	runGit("config", "user.email", "m8@example.invalid")
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-qm", "initial")
	link := filepath.Join(t.TempDir(), "repo-link")
	if err := os.Symlink(repo, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	out := filepath.Join(link, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, "report.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !m8GitDirtyInV1(link, out, filepath.Join(link, "profiles")) {
		t.Fatal("accepted a symlinked evidence root inside the source repository")
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

func TestM8ProductionSweepsRejectDuplicateCoordinatesV1(t *testing.T) {
	base := []string{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"}
	for _, sweep := range [][]string{
		{"-probes", "4,4"},
		{"-ef-search", "128,128"},
		{"-concurrency", "1,1"},
		{"-overlap", "0,0"},
	} {
		if _, err := parseConfig(append(append([]string(nil), base...), sweep...)); err == nil {
			t.Fatalf("accepted duplicate production sweep %v", sweep)
		}
	}
	if _, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1,1"}); err != nil {
		t.Fatalf("unrelated simulation duplicate probes: %v", err)
	}
}

func TestM8TruthCacheDigestConfigRequiresIndependentValidSHA256V1(t *testing.T) {
	base := []string{"-mode", m8ProductionMultiGroupModeV1, "-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "16", "-raft-groups", "4"}
	digest := strings.Repeat("a", 64)
	if _, err := parseConfig(append(append([]string(nil), base...), "-m8-truth-cache-sha256", digest)); err == nil || !strings.Contains(err.Error(), "m8-truth-cache-sha256") {
		t.Fatalf("digest without cache err=%v", err)
	}
	if _, err := parseConfig(append(append([]string(nil), base...), "-m8-truth-cache", t.TempDir(), "-m8-truth-cache-sha256", strings.Repeat("z", 64))); err == nil || !strings.Contains(err.Error(), "64-hex") {
		t.Fatalf("non-hex digest err=%v", err)
	}
	cfg, err := parseConfig(append(append([]string(nil), base...), "-m8-truth-cache", t.TempDir(), "-m8-truth-cache-sha256", digest))
	if err != nil || cfg.m8TruthCacheSHA256 != digest {
		t.Fatalf("trusted digest=%q err=%v", cfg.m8TruthCacheSHA256, err)
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
	oldDigest := strings.Repeat("a", 64)
	trustedDigest := strings.Repeat("b", 64)
	command := []string{"treedb_vector_partition_bench", "-mode", m8ProductionMultiGroupModeV1, "-dataset", "format", "-m8-variant-dbs", "/a,/b,/c", "-overlap=.1", "-format", "text", "-profiles", "/old", "-m8-matrix-out", "/old-out", "-m8-matrix-profiles", "/old-profiles", "-m8-truth-cache-sha256", oldDigest, "positional"}
	got, err := m8VariantProcessArgsV1(command, "/variant", .2, "/profiles/variant", "/matrix-out", "/matrix-profiles", trustedDigest)
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"-m8-existing-db", "/variant", "-overlap", "0.2", "-format", "json", "-m8-truth-cache-sha256", trustedDigest, "-profiles", "/profiles/variant", "-m8-matrix-out", "/matrix-out", "-m8-matrix-profiles", "/matrix-profiles"}
	if len(got) < len(wantPrefix) || !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("child args=%v want prefix=%v", got, wantPrefix)
	}
	for _, arg := range got {
		if strings.HasPrefix(arg, "-m8-variant-dbs") || strings.HasPrefix(arg, "--m8-variant-dbs") || arg == "/a,/b,/c" || arg == "/old" || arg == "/old-out" || arg == "/old-profiles" || arg == oldDigest {
			t.Fatalf("child args retained matrix/old-profile argument: %v", got)
		}
	}
	if !slices.Contains(got, "format") {
		t.Fatalf("child args dropped positional value matching a filtered flag: %v", got)
	}
	trustedAt, positionalAt := slices.Index(got, trustedDigest), slices.Index(got, "positional")
	if trustedAt < 0 || positionalAt < 0 || trustedAt > positionalAt {
		t.Fatalf("trusted digest must precede positional token: %v", got)
	}
	replay, err := m8ReplayCommandWithTruthCacheDigestV1(command, trustedDigest)
	if err != nil {
		t.Fatal(err)
	}
	digestFlags := 0
	for _, arg := range replay {
		if arg == "-m8-truth-cache-sha256" {
			digestFlags++
		}
	}
	if digestFlags != 1 || slices.Contains(replay, oldDigest) || slices.Index(replay, trustedDigest) > slices.Index(replay, "positional") {
		t.Fatalf("replay command did not replace and front-load truth digest: %v", replay)
	}
}

// TestM8ProductionMatrixSeparatesUsefulOnlyShortfallFromUnderMaterializationV1
// pins the useful-only contract boundary. A shortfall is legal: once no
// cut-reducing proposal remains, requested overlap capacity stays unused and
// zero filler is realized. Declaring more realized overlap than the retained
// membership list materialized is not legal, and remains rejected.
func TestM8ProductionMatrixSeparatesUsefulOnlyShortfallFromUnderMaterializationV1(t *testing.T) {
	hash := strings.Repeat("a", 40)
	fixture := fixtureManifest{Checksum: strings.Repeat("b", 64)}
	cfg := config{baseSHA: hash, headSHA: hash, partitions: 16, command: []string{"bench"}}
	common := m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: []int{4}, TopK: 10, RecallTarget: .9, Concurrency: []int{1}, Warmup: 1, EfSearch: []int{128}, RouterCandidates: 1024, Seed: 1}
	pass := m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}
	// Ten rows at ratio .2 request two overlap memberships; the graph variant
	// realizes one useful replica and leaves the rest of the budget unspent.
	buildReports := func(overDeclare bool) []m8ProductionReportV1 {
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
			if overDeclare && variant.overlap != 0 {
				descriptor.OverlapRealized = descriptor.OverlapMemberships + 1
				descriptor.OverlapRejected = descriptor.OverlapRequested - descriptor.OverlapRealized
				descriptor.OverlapUseful = descriptor.OverlapRealized
				// Keep the unused-capacity accounting consistent with the
				// over-declared count so the membership/realized mismatch is the
				// only rule the descriptor still breaks.
				descriptor.OverlapUnusedCapacity = descriptor.Capacity*int(descriptor.Partitions) - int(descriptor.SourceRows) - descriptor.OverlapRealized
				descriptor.BuildIdentityDigest, _ = m3VariantBuildIdentityDigestV1(descriptor)
				descriptor.OverlapPolicy, _ = collections.FormatVectorPartitionOverlapPolicyV1(collections.VectorPartitionOverlapPolicyV1{
					Capacity: uint64(descriptor.Capacity), Budget: uint64(descriptor.OverlapRequested),
					Realized: uint64(descriptor.OverlapRealized), Unspent: uint64(descriptor.OverlapRejected),
					BuildIdentityDigest: descriptor.BuildIdentityDigest,
				})
			}
			config := common
			config.Overlap = []float64{variant.overlap}
			reports = append(reports, m8ProductionReportV1{
				ExecutableSHA256: strings.Repeat("a", 64),
				BaseSHA:          hash, HeadSHA: hash, Dataset: fixture, Config: config, Variant: &descriptor, GateLedger: pass,
				Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: 100},
				Rows: []m8ProductionRowV1{
					// Candidate operating point and the exhaustive all-partition
					// baseline the coupled-graph gate compares it against, so the
					// matrix can actually reach local_gate_pass here.
					{Status: "pass", VariantID: variant.id, Probes: 4, EfSearch: 128, Concurrency: 1, Samples: 1, RecallAtK: .95, QPS: 200, P95Nanos: 1000},
					{Status: "pass", VariantID: variant.id, Probes: 16, EfSearch: 128, Concurrency: 1, Samples: 1, RecallAtK: .95, QPS: 100, P95Nanos: 2000,
						Attribution: m8ProductionAttributionV1{ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExhaustivePartitionRecallAtK: 1}},
				},
			})
		}
		return reports
	}
	shortfall, err := m8BuildProductionMatrixV1(cfg, fixture, buildReports(false))
	if err != nil {
		t.Fatalf("useful-only overlap shortfall rejected: %v", err)
	}
	// A shortfall is a legal useful-only outcome, so the matrix must account it
	// rather than treating it as an unmaterialized overlap. Requiring exact fill
	// here would make every zero-cut corpus unable to pass the matrix.
	if !shortfall.OverlapDiagnostics.Accounted {
		t.Fatalf("useful-only shortfall not accounted: %+v", shortfall.OverlapDiagnostics)
	}
	if shortfall.Gates.RequiredVariants != "pass" || shortfall.Gates.OverlapStorage != "pass" {
		t.Fatalf("useful-only shortfall gates required=%q overlap_storage=%q", shortfall.Gates.RequiredVariants, shortfall.Gates.OverlapStorage)
	}
	if shortfall.Status != "local_gate_pass" {
		t.Fatalf("useful-only shortfall matrix status=%q", shortfall.Status)
	}
	matrix, err := m8BuildProductionMatrixV1(cfg, fixture, buildReports(true))
	if err == nil || !strings.Contains(err.Error(), "malformed M3 variant descriptor") {
		t.Fatalf("under-materialized overlap err=%v matrix=%+v", err, matrix)
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
			descriptor.OverlapMemberships = int(math.Floor(item.overlap * float64(descriptor.SourceRows)))
			if item.id == "graph-overlap-020-v1" {
				descriptor.RouterModelDigest = strings.Repeat("e", 64)
			}
			if item.assignment == partitionAssignmentStableIDHashV1 {
				descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
				descriptor.GraphArtifactSHA256 = strings.Repeat("d", 64)
				descriptor.RouterModelDigest = strings.Repeat("f", 64)
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
		"base revision": func(variants []m3VariantDescriptorV1) { variants[2].BaseSHA = strings.Repeat("d", 40) },
		"head revision": func(variants []m3VariantDescriptorV1) { variants[2].HeadSHA = strings.Repeat("e", 40) },
		"graph digest":  func(variants []m3VariantDescriptorV1) { variants[1].GraphArtifactSHA256 = strings.Repeat("d", 64) },
		"graph assignment artifact": func(variants []m3VariantDescriptorV1) {
			variants[1].ArtifactSHA256 = strings.Repeat("d", 64)
			variants[1].GraphArtifactSHA256 = variants[0].GraphArtifactSHA256
		},
		"graph build":            func(variants []m3VariantDescriptorV1) { variants[2].GraphBuildSHA256 = strings.Repeat("d", 64) },
		"source":                 func(variants []m3VariantDescriptorV1) { variants[2].Source.SourceID = "different" },
		"index definition":       func(variants []m3VariantDescriptorV1) { variants[2].IndexDefinitionDigest = strings.Repeat("d", 64) },
		"local HNSW M":           func(variants []m3VariantDescriptorV1) { variants[2].PartitionHNSWM-- },
		"partition graph degree": func(variants []m3VariantDescriptorV1) { variants[2].PartitionConfig.Degree++ },
		"router representatives": func(variants []m3VariantDescriptorV1) { variants[2].RouterRepresentatives++ },
		"router scalar work":     func(variants []m3VariantDescriptorV1) { variants[2].RouterMaxScalarWork++ },
		"router config seed":     func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.Seed++ },
		"router config branch":   func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.BranchFactor++ },
		"router config leaf":     func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.LeafSize++ },
		"router config reps":     func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.RepresentativesPerPartition++ },
		"router config depth":    func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.MaxDepth-- },
		"router config iterations": func(variants []m3VariantDescriptorV1) {
			variants[2].RouterConfig.MaxIterations--
		},
		"router config vectors":    func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.MaxVectors-- },
		"router config dimensions": func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.MaxDimensions-- },
		"router config max representatives": func(variants []m3VariantDescriptorV1) {
			variants[2].RouterConfig.MaxRepresentatives--
		},
		"router config scalar cap": func(variants []m3VariantDescriptorV1) {
			variants[2].RouterConfig.MaxScalarWork++
			variants[2].RouterMaxScalarWork++
		},
		"router config bytes": func(variants []m3VariantDescriptorV1) { variants[2].RouterConfig.MaxRouterBytes-- },
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
