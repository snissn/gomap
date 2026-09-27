package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
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

func TestM8ScalingDomainGraphRuntimeV1(t *testing.T) {
	base, candidate := testM8ScalingReportV1(), testM8ScalingReportV1()
	for i, r := range []*m8ProductionReportV1{&base, &candidate} {
		r.HeadSHA = strings.Repeat(string(rune('a'+i)), 40)
		r.BaseSHA = r.HeadSHA
		r.ExecutableSHA256 = strings.Repeat(string(rune('c'+i)), 64)
		r.Config.Partitions, r.Variant.Partitions = 8, 8
		r.Config.PacksPerDomain = []int{2, 2, 2, 2}
		r.Variant.ShardPlan.PacksPerDomain = 2
		r.Variant.ArtifactSHA256 = strings.Repeat("e", 64)
		r.Variant.ShardGenerationDigest = strings.Repeat("f", 64)
		for j := range r.Rows {
			row := &r.Rows[j]
			searches := row.Probes * (2 - i)
			row.Attribution.LocalHNSWSearches = uint64(searches * row.Samples)
			row.Attribution.LocalHNSWSearchesByQuery = make([]uint32, row.Samples)
			for q := range row.Attribution.LocalHNSWSearchesByQuery {
				row.Attribution.LocalHNSWSearchesByQuery[q] = uint32(searches)
			}
			if i == 1 {
				row.QPS *= 1.2
			}
		}
	}
	pair := m8ScalingPairV1{Name: "domain", Kind: "domain_graph_runtime", Baseline: "base", Candidate: "candidate", BaselineProbes: 1, CandidateProbes: 1}
	result, err := m8CompareScalingPairV1(pair, base, candidate, "", "")
	if err != nil || len(result.Blocks) != 10 || !result.AllMatchedQuality || !result.AllQPS15Percent || !result.AllP95NoRegression {
		t.Fatalf("cross-runtime result=%+v err=%v", result, err)
	}
	raw, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"same_runtime": func(r *m8ProductionReportV1) {
			r.BaseSHA, r.HeadSHA, r.ExecutableSHA256 = base.BaseSHA, base.HeadSHA, base.ExecutableSHA256
		},
		"missing_source":     func(r *m8ProductionReportV1) { r.HeadSHA = "" },
		"changed_membership": func(r *m8ProductionReportV1) { r.Variant.ShardGenerationDigest = strings.Repeat("0", 64) },
		"changed_parent":     func(r *m8ProductionReportV1) { r.Variant.ArtifactSHA256 = strings.Repeat("0", 64) },
		"changed_fixture":    func(r *m8ProductionReportV1) { r.Dataset.QueryOrdinalOffset++ },
		"changed_host":       func(r *m8ProductionReportV1) { r.Host.CPUModel = "another host" },
		"changed_budget":     func(r *m8ProductionReportV1) { r.Config.LocalScoreBudget++ },
		"still_per_pack":     func(r *m8ProductionReportV1) { r.Rows[0].Attribution = base.Rows[0].Attribution },
		"missing_window":     func(r *m8ProductionReportV1) { r.Rows = r.Rows[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			var bad m8ProductionReportV1
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad)
			if _, err := m8CompareScalingPairV1(pair, base, bad, "", ""); err == nil {
				t.Fatal("invalid runtime comparison admitted")
			}
		})
	}
	candidate.Rows[0].Status = "timeout"
	result, err = m8CompareScalingPairV1(pair, base, candidate, "", "")
	if err != nil || len(result.Blocks) != 10 || result.AllMatchedQuality || result.AllQPS15Percent || result.AllP95NoRegression {
		t.Fatalf("failed window hidden: %+v %v", result, err)
	}
}

func TestM8ScalingRuntimeRefusesUnboundExecutableV1(t *testing.T) {
	report, pins := testM8ReportReplayPinsV1(t)
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "report.json")
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pins.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
	args := testM8ReportReplayArgsV1(root, path, pins)[1:]
	for _, runtime := range []m8ScalingReplayRuntimeV1{
		{},
		{HeadSHA: report.HeadSHA, Executable: "/bin/true", ExecutableSHA256: pins.Executable},
		{HeadSHA: report.HeadSHA, Executable: report.Command[0], ExecutableSHA256: pins.Executable},
	} {
		if _, _, err := m8ReplayScalingRuntimeV1(args, runtime); err == nil || !strings.Contains(err.Error(), "frozen clean producing binary") {
			t.Fatalf("unbound executable admitted or wrong refusal: %v", err)
		}
	}
}

func TestM8ScalingProducingRuntimeReceiptV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, _ := testM8QualificationGitCheckoutV1(t, root)
	// This clean tiny producer tests the subprocess protocol, not retained-data
	// acceptance. The actual producer's strict replay has separate tests.
	for name, content := range map[string]string{
		"go.mod": "module github.com/snissn/gomap/cmd/treedb_vector_partition_bench\n\ngo 1.26\n",
		"main.go": `package main
import ("fmt"; "os"; "strings")
func main() {
 if os.Args[1] != "replay-m8-report" { os.Exit(2) }
 if os.Getenv("M8_TEST_REPLAY_MODE") == "fail" { os.Exit(3) }
 if os.Getenv("M8_TEST_REPLAY_MODE") == "overflow" { fmt.Print(strings.Repeat("x", 8192)); return }
 if os.Getenv("M8_TEST_REPLAY_MODE") == "wrong" { fmt.Println("REPLAY_ACCEPTED_NOT_QUALIFICATION"); return }
 for i, arg := range os.Args {
  if arg == "-report-sha256" { fmt.Printf("REPLAY_ACCEPTED_NOT_QUALIFICATION report_sha256=%s rows=0\n", os.Args[i+1]) }
  if arg == "-report" && os.Getenv("M8_TEST_REPLAY_MODE") == "mutate" { os.WriteFile(os.Args[i+1], []byte("changed"), 0600) }
 }
}
`,
	} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"add", "."}, {"commit", "-qm", "test producer"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
	}
	head, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "producer")
	build := exec.Command("go", "build", "-buildvcs=true", "-o", binary, ".")
	build.Dir = source
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build producer: %v: %s", err, out)
	}
	report, pins := testM8ReportReplayPinsV1(t)
	report.HeadSHA, report.Variant.HeadSHA = strings.TrimSpace(string(head)), strings.TrimSpace(string(head))
	pins.Executable, err = m8BenchmarkExecutableSHA256V1(binary)
	if err != nil {
		t.Fatal(err)
	}
	report.ExecutableSHA256, report.Variant.ExecutableSHA256 = pins.Executable, pins.Executable
	report.Command[0] = binary
	command, _ := json.Marshal(report.Command)
	descriptor, err := m3VariantDescriptorJSONV1(*report.Variant)
	if err != nil {
		t.Fatal(err)
	}
	pins.Command, pins.Variant = fmt.Sprintf("%x", sha256.Sum256(command)), fmt.Sprintf("%x", sha256.Sum256(descriptor))
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	pins.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
	path := filepath.Join(root, "report.json")
	runtime := m8ScalingReplayRuntimeV1{HeadSHA: report.HeadSHA, Executable: binary, ExecutableSHA256: pins.Executable}
	for _, mode := range []string{"valid", "fail", "wrong", "overflow", "mutate"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("M8_TEST_REPLAY_MODE", mode)
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			_, receipt, err := m8ReplayScalingRuntimeV1(testM8ReportReplayArgsV1(root, path, pins)[1:], runtime)
			if mode == "valid" {
				if err != nil || !strings.Contains(receipt, pins.Report) {
					t.Fatalf("valid receipt: %q %v", receipt, err)
				}
			} else if err == nil {
				t.Fatal("invalid producing-runtime result accepted")
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

func TestM8ScalingSameGeometryHomePackingV1(t *testing.T) {
	plan, err := vectorpartition.PlanByteBoundedShardsV1(vectorpartition.ShardPlanInputV1{
		Vectors: 100000, Dimensions: 768, LogicalDomains: 16, OverlapRatio: .2,
		Imbalance: .05, TargetHotBytes: 7696384,
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline, candidate := testM8ScalingReportV1(), testM8ScalingReportV1()
	for _, r := range []*m8ProductionReportV1{&baseline, &candidate} {
		r.Config.DomainCount, r.Config.Partitions = 16, 64
		r.Config.Probes = []int{5, 16}
		r.Config.PacksPerDomain = make([]int, 16)
		for i := range r.Config.PacksPerDomain {
			r.Config.PacksPerDomain[i] = 4
		}
		r.Variant.Partitions, r.Variant.ShardPlan = 64, plan
		r.Variant.ArtifactSHA256 = strings.Repeat("a", 64)
		r.Variant.AssignmentBasis = partitionAssignmentGraphV1
		r.Variant.ShardGenerationDigest = strings.Repeat("b", 64)
		for i := range r.Rows {
			if r.Rows[i].Probes == 1 {
				r.Rows[i].Probes = 5
			} else {
				r.Rows[i].Probes = 16
			}
		}
	}
	baseline.Variant.KaHIPAdapterSHA256 = kahipAdapterSHA256
	candidate.Variant.KaHIPAdapterSHA256 = kahipHomePackingAdapterSHA256
	candidate.Variant.ShardGenerationDigest = strings.Repeat("c", 64)
	pair := m8ScalingPairV1{Name: "homes", Kind: "same_geometry_home_packing", Baseline: "striped", Candidate: "graph", BaselineProbes: 5, CandidateProbes: 5}
	union := strings.Repeat("d", 64)
	result, err := m8CompareScalingPairV1(pair, baseline, candidate, union, union)
	if err != nil || len(result.Blocks) != 10 || !result.AllMatchedQuality || result.AllQPS15Percent {
		t.Fatalf("neutral candidate must retain all gate misses: %+v, %v", result, err)
	}
	raw, _ := json.Marshal(candidate)
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"adapter":    func(r *m8ProductionReportV1) { r.Variant.KaHIPAdapterSHA256 = kahipAdapterSHA256 },
		"parent":     func(r *m8ProductionReportV1) { r.Variant.ArtifactSHA256 = strings.Repeat("e", 64) },
		"plan":       func(r *m8ProductionReportV1) { r.Variant.ShardPlan.TargetHotBytes++ },
		"overlap":    func(r *m8ProductionReportV1) { r.Variant.OverlapRatio = .2 },
		"generation": func(r *m8ProductionReportV1) { r.Variant.ShardGenerationDigest = "" },
		"assignment": func(r *m8ProductionReportV1) { r.Variant.AssignmentBasis = "hash" },
	} {
		t.Run(name, func(t *testing.T) {
			var bad m8ProductionReportV1
			if err := json.Unmarshal(raw, &bad); err != nil {
				t.Fatal(err)
			}
			mutate(&bad)
			if _, err := m8CompareScalingPairV1(pair, baseline, bad, union, union); err == nil {
				t.Fatal("changed construction admitted")
			}
		})
	}
	if _, err := m8CompareScalingPairV1(pair, baseline, candidate, union, strings.Repeat("e", 64)); err == nil {
		t.Fatal("changed logical union admitted")
	}
	pair.CandidateProbes++
	if _, err := m8CompareScalingPairV1(pair, baseline, candidate, union, union); err == nil {
		t.Fatal("changed selected prefix admitted")
	}
	pair.CandidateProbes = pair.BaselineProbes
	pair.Kind = "logical_packing"
	if _, err := m8CompareScalingPairV1(pair, baseline, candidate, union, union); err == nil {
		t.Fatal("new treatment relabelled as historical one-pack comparison")
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
