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
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestReplayM8ReportRequiresFrozenPinsV1(t *testing.T) {
	err := run([]string{"replay-m8-report"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "replay-m8-report requires") {
		t.Fatalf("missing replay pin admission: %v", err)
	}
}

func testM8ReportReplayPinsV1(t *testing.T) (m8ProductionReportV1, m8ReportReplayPinsV1) {
	t.Helper()
	fixture := m8QualificationFixturesV1[0]
	fixture.QueryOrdinalOffset = 1000
	report := m8ProductionReportV1{
		Dataset: fixture, Command: []string{"/not-executed/benchmark", "-m8-quality-diagnostics"},
		BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), ExecutableSHA256: strings.Repeat("c", 64),
		Variant:    &m3VariantDescriptorV1{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), ExecutableSHA256: strings.Repeat("c", 64), ArtifactSHA256: strings.Repeat("d", 64)},
		TruthCache: m8TruthCacheEvidenceV1{ArtifactSHA256: strings.Repeat("e", 64)},
	}
	fixtureRaw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	commandRaw, err := json.Marshal(report.Command)
	if err != nil {
		t.Fatal(err)
	}
	descriptorRaw, err := m3VariantDescriptorJSONV1(*report.Variant)
	if err != nil {
		t.Fatal(err)
	}
	return report, m8ReportReplayPinsV1{Report: strings.Repeat("f", 64), Fixture: fmt.Sprintf("%x", sha256.Sum256(fixtureRaw)), Command: fmt.Sprintf("%x", sha256.Sum256(commandRaw)), Executable: report.ExecutableSHA256, Variant: fmt.Sprintf("%x", sha256.Sum256(descriptorRaw)), TruthArtifact: report.TruthCache.ArtifactSHA256, TruthContent: strings.Repeat("0", 64)}
}

func testM8ReportReplayArgsV1(root, report string, p m8ReportReplayPinsV1) []string {
	return []string{"replay-m8-report", "-root", root, "-report", report,
		"-report-sha256", p.Report, "-fixture-sha256", p.Fixture, "-command-sha256", p.Command,
		"-executable-sha256", p.Executable, "-variant-descriptor-sha256", p.Variant,
		"-truth-artifact-sha256", p.TruthArtifact, "-truth-content-sha256", p.TruthContent}
}

func TestReplayM8ReportFrozenIdentityV1(t *testing.T) {
	report, pins := testM8ReportReplayPinsV1(t)
	if !pins.valid() || m8ReportReplayIdentityV1(report, pins) != nil {
		t.Fatal("matching frozen identities rejected")
	}
	for name, mutate := range map[string]func(*m8ProductionReportV1){
		"query_offset":       func(r *m8ProductionReportV1) { r.Dataset.QueryOrdinalOffset++ },
		"corpus_seed":        func(r *m8ProductionReportV1) { r.Dataset.Seed++ },
		"command":            func(r *m8ProductionReportV1) { r.Command = append(r.Command, "-ef-search", "512") },
		"dirty_source":       func(r *m8ProductionReportV1) { r.Dirty = true },
		"dirty_build":        func(r *m8ProductionReportV1) { r.Variant.BuildDirty = true },
		"source_head":        func(r *m8ProductionReportV1) { r.HeadSHA = strings.Repeat("1", 40) },
		"source_base":        func(r *m8ProductionReportV1) { r.BaseSHA = strings.Repeat("1", 40) },
		"executable":         func(r *m8ProductionReportV1) { r.ExecutableSHA256 = strings.Repeat("1", 64) },
		"variant_executable": func(r *m8ProductionReportV1) { r.Variant.ExecutableSHA256 = strings.Repeat("1", 64) },
		"variant_artifact":   func(r *m8ProductionReportV1) { r.Variant.ArtifactSHA256 = strings.Repeat("1", 64) },
		"router_model":       func(r *m8ProductionReportV1) { r.Variant.RouterModelDigest = strings.Repeat("1", 64) },
		"router_config":      func(r *m8ProductionReportV1) { r.Variant.RouterConfig.MaxDepth++ },
		"partition_policy":   func(r *m8ProductionReportV1) { r.Variant.OverlapPolicy = "changed" },
		"missing_variant":    func(r *m8ProductionReportV1) { r.Variant = nil },
		"truth_artifact":     func(r *m8ProductionReportV1) { r.TruthCache.ArtifactSHA256 = strings.Repeat("1", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed, _ := testM8ReportReplayPinsV1(t)
			mutate(&changed)
			if err := m8ReportReplayIdentityV1(changed, pins); err == nil {
				t.Fatal("accepted drift from externally frozen identity")
			}
		})
	}
}

func TestReplayM8ReportAdmissionBeforeIOV1(t *testing.T) {
	_, pins := testM8ReportReplayPinsV1(t)
	args := testM8ReportReplayArgsV1("/missing-root", "/missing-report", pins)
	for i := 5; i < len(args); i += 2 {
		t.Run(args[i], func(t *testing.T) {
			missing := append([]string(nil), args[:i]...)
			missing = append(missing, args[i+2:]...)
			if err := run(missing, io.Discard); err == nil || !strings.Contains(err.Error(), "requires") {
				t.Fatalf("missing pin reached filesystem: %v", err)
			}
			malformed := append([]string(nil), args...)
			malformed[i+1] = "not-sha256"
			if err := run(malformed, io.Discard); err == nil || !strings.Contains(err.Error(), "requires") {
				t.Fatalf("malformed pin reached filesystem: %v", err)
			}
		})
	}
}

func testM8PlannedDiagnosticReportV1(t *testing.T) (m8ProductionReportV1, m8ReportReplayPinsV1) {
	t.Helper()
	// Serialize the producer's actual schema/indentation, not string padding.
	// 512 queries x 15 cells, with all three routes filled to D=40, conservatively
	// exceeds the plan's largest p=16 route. Both top-10 cost curves and all
	// unsampled local/coordinator masks are present; trace-only masks stay nil.
	report, pins := testM8ReportReplayPinsV1(t)
	report.Dataset.Queries = 512
	report.ExecutionID = strings.Repeat("a", 32)
	report.Config.TopK = 10
	report.Config.QualityDiagnostics = true
	report.Resources.PeakRSSMeasured = true
	report.Resources.PeakRSSBytes = 1 << 20
	fixture, err := json.Marshal(report.Dataset)
	if err != nil {
		t.Fatal(err)
	}
	pins.Fixture = fmt.Sprintf("%x", sha256.Sum256(fixture))
	domains, packs := make([]uint32, 40), make([]int64, 40)
	for i := range domains {
		domains[i], packs[i] = uint32(i), 1
	}
	mask := uint16(1023)
	curve := m8CoverageCurveV1{Method: m8CoverageCostMethodV1, TruthCount: 10,
		MinimumCosts: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, WorkBound: maxBenchmarkWorkUnits, ScratchBytes: maxFixtureBytes}
	queries := make([]m8QualityQueryV1, report.Dataset.Queries)
	for i := range queries {
		queries[i] = m8QualityQueryV1{
			QuerySHA256: strings.Repeat("a", 64), TruthSHA256: strings.Repeat("b", 64), DomainCost: curve, PackCost: curve,
			NoCoarseningDomains: domains, NoCoarseningMask: mask, NoCoarseningPacks: 40,
			ExactDomains: domains, ExactMask: mask, ExactPacks: 40,
			ApproximateDomains: domains, ApproximateMask: &mask, ApproximatePacks: 40,
			NoCoarseningToExactLost: mask, NoCoarseningToExactGained: mask,
			ExactToApproximateLost: &mask, ExactToApproximateGained: &mask,
			Actual: &m8ObservedTruthMasksV1{Available: mask, Returned: mask}, CoordinatorReturned: &mask,
		}
	}
	report.Rows = make([]m8ProductionRowV1, 15)
	for i := range report.Rows {
		report.Rows[i] = m8ProductionRowV1{Status: "pass", Samples: len(queries),
			Probes: 1 << (i % 5), EfSearch: []int{64, 96, 128}[i/5], Concurrency: 1,
			P50Nanos: 1, P95Nanos: 1, P99Nanos: 1, MaxTotalNanos: 1, ElapsedNanos: uint64(len(queries))}
		report.Rows[i].Attribution.Quality = &m8QualityAttributionV1{
			Method: m8QualityAttributionMethodV1, Generation: 1, SourceGeneration: 1,
			ModelSHA256: strings.Repeat("c", 64), Domains: len(domains), PackCosts: packs, Queries: queries,
		}
	}
	return report, pins
}

func TestReplayM8ReportPlannedDiagnosticSizeV1(t *testing.T) {
	report, pins := testM8PlannedDiagnosticReportV1(t)
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	t.Logf("512-query x 15-row D40 diagnostic-shaped report: %d bytes", len(raw))
	if len(raw) <= m8QualificationMatrixMaxBytesV1 || len(raw) > m8DiagnosticRetainedMaxBytesV1 {
		t.Fatalf("planned diagnostic size %d is outside (16, 64] MiB", len(raw))
	}
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "report.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pins.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
	var output strings.Builder
	// Admission must reach the real missing-profile guard after bounded read,
	// hash, strict decoding and pin checks. This is not a positive replay claim.
	if err := run(testM8ReportReplayArgsV1(root, path, pins), &output); err == nil || !strings.Contains(err.Error(), "contained captured production profiles") || output.Len() != 0 {
		t.Fatalf("planned report did not reach profile validation: %v output=%q", err, output.String())
	}
}

func TestM8PlannedRouterPolicyReceiptSizeV1(t *testing.T) {
	// This typed serialization fixture is not empirical policy/replay evidence.
	// Cover the admitted one-cell representative shape, not an infeasible grid.
	report, pins := testM8PlannedDiagnosticReportV1(t)
	report.Dataset.Vectors, report.Dataset.Dimensions = 250000, 128
	report.Config.Partitions, report.Config.DomainCount = 16, 16
	report.Config.Probes, report.Config.EfSearch, report.Config.Concurrency = []int{2}, []int{96}, []int{1}
	report.Config.RouterPolicyDiagnostics, report.Config.RouterPolicyWidth, report.Config.RouterCandidates = true, 64, 128
	report.Rows = report.Rows[:1]
	row := &report.Rows[0]
	row.Probes, row.EfSearch = 2, 96
	quality := row.Attribution.Quality
	quality.Domains, quality.PackCosts = 16, make([]int64, 16)
	for i := range quality.PackCosts {
		quality.PackCosts[i] = 1
	}
	policies := &m8RouterPolicyEvidenceV1{Method: m8RouterPolicyExperimentMethodV1, RequestedWidth: 64, EffectiveWidth: 64, ApproximateBudget: 128, Queries: make([]m8RouterPolicyQueryV1, 512)}
	for i := range quality.Queries {
		q := &quality.Queries[i]
		q.NoCoarseningDomains, q.ExactDomains, q.ApproximateDomains = []uint32{0, 1}, []uint32{0, 1}, []uint32{0, 1}
		q.NoCoarseningPacks, q.ExactPacks, q.ApproximatePacks = 2, 2, 2
		distance := []collections.VectorPartitionRouterPolicyDomainV1{
			{Domain: 0, Distance: .01, Frequency: 1, WinningRepresentative: 0, WinningSourceOrdinal: 0},
			{Domain: 1, Distance: .02, Frequency: 2, WinningRepresentative: 1, WinningSourceOrdinal: 1},
		}
		other := []collections.VectorPartitionRouterPolicyDomainV1{distance[1], distance[0]}
		comparison := collections.VectorPartitionRouterPolicyComparisonV1{
			Method: collections.VectorPartitionRouterPolicyDiagnosticMethodV1, Generation: 1, SourceGeneration: 1,
			ModelSHA256: quality.ModelSHA256, QuerySHA256: q.QuerySHA256, Mode: collections.VectorPartitionRouterModeExactV1,
			RepresentativeCount: 256, DomainCount: 16, CandidateBudget: 256, ReturnedWidth: 64, Probes: 2,
			CollectionComplete: true, Collected: 256, UniqueReturned: 64, Candidates: 256,
			CandidateSetSHA256: strings.Repeat("d", 64), CandidateSequenceSHA256: strings.Repeat("e", 64),
			Distance: distance, Frequency: other, Hybrid: other,
		}
		coverage := &m8RouterPolicyCoverageV1{DistanceMask: 1023, FrequencyMask: 1023, HybridMask: 1023, DistancePacks: 2, FrequencyPacks: 2, HybridPacks: 2}
		exact := m8RouterPolicyOutcomeV1{Status: "pass", Comparison: comparison, Coverage: coverage}
		comparison.Mode, comparison.CandidateBudget, comparison.Collected, comparison.Candidates, comparison.Edges = collections.VectorPartitionRouterModeApproxV1, 128, 128, 128, 32768
		policies.Queries[i] = m8RouterPolicyQueryV1{QuerySHA256: q.QuerySHA256, TruthSHA256: q.TruthSHA256,
			Exact: exact, Approximate: m8RouterPolicyOutcomeV1{Status: "pass", Comparison: comparison, Coverage: coverage}}
	}
	row.Attribution.RouterPolicies = policies
	report.Command = append(report.Command, "-m8-router-policy-diagnostics", "-m8-router-policy-width", "64", "-probes", "2", "-ef-search", "96", "-router-candidates", "128")
	for _, item := range []struct {
		value any
		pin   *string
	}{{report.Dataset, &pins.Fixture}, {report.Command, &pins.Command}} {
		raw, err := json.Marshal(item.value)
		if err != nil {
			t.Fatal(err)
		}
		*item.pin = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	report.MeasurementTranscript, err = m8WriteProductionMeasurementTranscriptV1(root, report, testM8MeasurementCellsV1(report))
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(report)
	if err != nil {
		t.Fatalf("strict typed policy transcript read: %v", err)
	}
	if len(transcript.Rows) != 1 || transcript.Rows[0].Attribution.RouterPolicies == nil || len(transcript.Rows[0].Attribution.RouterPolicies.Queries) != 512 {
		t.Fatal("policy transcript dropped query evidence")
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	t.Logf("typed N250K/Q512/d128/D16 width64/P2/EF96/C128 policy report=%d transcript=%d bytes", len(raw), report.MeasurementTranscript.Bytes)
	if int64(len(raw)) > m8DiagnosticRetainedMaxBytesV1 || report.MeasurementTranscript.Bytes > m8DiagnosticRetainedMaxBytesV1 {
		t.Fatal("finite policy receipt exceeds unchanged 64MiB bound")
	}
	path := filepath.Join(root, "policy-report.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pins.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
	var output strings.Builder
	if err := run(testM8ReportReplayArgsV1(root, path, pins), &output); err == nil || !strings.Contains(err.Error(), "contained captured production profiles") || output.Len() != 0 {
		t.Fatalf("typed policy report did not reach missing-profile guard: %v output=%q", err, output.String())
	}
}

func TestM8PlannedDiagnosticTranscriptSizeV1(t *testing.T) {
	report, _ := testM8PlannedDiagnosticReportV1(t)
	var err error
	report.MeasurementTranscript, err = m8WriteProductionMeasurementTranscriptV1(t.TempDir(), report, testM8MeasurementCellsV1(report))
	if err != nil {
		t.Fatalf("write planned diagnostic transcript: %v", err)
	}
	t.Logf("512-query x 15-row D40 diagnostic transcript: %d bytes", report.MeasurementTranscript.Bytes)
	if report.MeasurementTranscript.Bytes <= m8QualificationTranscriptMaxBytesV1 || report.MeasurementTranscript.Bytes > m8DiagnosticRetainedMaxBytesV1 {
		t.Fatalf("planned transcript size %d is outside (2, 64] MiB", report.MeasurementTranscript.Bytes)
	}
	if _, err := m8ReadProductionMeasurementTranscriptV1(report); err != nil {
		t.Fatalf("strict read of planned diagnostic transcript: %v", err)
	}
	ordinary := report
	ordinary.Config.QualityDiagnostics = false
	if _, err := m8ReadProductionMeasurementTranscriptV1(ordinary); err == nil || !strings.Contains(err.Error(), "read M8 measurement transcript") {
		t.Fatalf("ordinary transcript lost its 2 MiB cap: %v", err)
	}
	if err := os.Truncate(report.MeasurementTranscript.Path, m8DiagnosticRetainedMaxBytesV1+1); err != nil {
		t.Fatal(err)
	}
	report.MeasurementTranscript.Bytes = m8DiagnosticRetainedMaxBytesV1 + 1
	if _, err := m8ReadProductionMeasurementTranscriptV1(report); err == nil || !strings.Contains(err.Error(), "read M8 measurement transcript") {
		t.Fatalf("accepted oversized diagnostic transcript: %v", err)
	}
}

func TestReplayM8ReportHistoricalTranscriptCapV1(t *testing.T) {
	if m8QualificationTranscriptMaxBytesV1 != 2<<20 || m8QualificationMatrixMaxBytesV1 != 16<<20 || m8QualificationIndexMaxBytesV1 != 1<<20 {
		t.Fatal("historical qualification byte caps changed")
	}
	root := t.TempDir()
	head := m8QualificationFrozenBaseSHAV1
	fixture := m8QualificationFixturesV1[0]
	matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
	matrix.Variants[0].Config.QualityDiagnostics = true
	matrix.Variants[0].MeasurementTranscript.Bytes = m8QualificationTranscriptMaxBytesV1 + 1
	raw, err := json.Marshal(matrix)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "repeat.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: head, HeadSHA: head,
		Runs: []m8QualificationCampaignRunV1{
			{Path: "repeat.json", SHA256: fmt.Sprintf("%x", sha256.Sum256(raw)), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)},
			{Path: "repeat-2.json", SHA256: strings.Repeat("a", 64)},
			{Path: "repeat-3.json", SHA256: strings.Repeat("b", 64)},
		}}
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "historical transcript byte cap") {
		t.Fatalf("historical campaign did not reject selected diagnostics before child validation: %v", err)
	}
}

func TestReplayM8ReportBoundedReceiptV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	report, pins := testM8ReportReplayPinsV1(t)
	path := filepath.Join(root, "report.json")
	write := func(report m8ProductionReportV1, suffix string) []string {
		t.Helper()
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, suffix...)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		p := pins
		p.Report = fmt.Sprintf("%x", sha256.Sum256(raw))
		return testM8ReportReplayArgsV1(root, path, p)
	}
	reject := func(args []string, want string) {
		t.Helper()
		var output strings.Builder
		if err := run(args, &output); err == nil || !strings.Contains(err.Error(), want) || output.Len() != 0 {
			t.Fatalf("want %q, err=%v output=%q", want, err, output.String())
		}
	}
	// This intentionally incomplete receipt must stop before trying to execute
	// the nonexistent benchmark or loading any corpus. Matching hashes alone
	// are never accepted as an artifact replay.
	reject(write(report, ""), "contained captured production profiles")
	changed := report
	changed.Dataset.QueryOrdinalOffset++
	reject(write(changed, ""), "fixture differs")
	changed = report
	changed.Command = append([]string(nil), report.Command...)
	changed.Command[0] = "/untrusted-program"
	reject(write(changed, ""), "command differs")
	reject(write(report, " {}"), "trailing JSON")
	args := write(report, "")
	args[6] = strings.Repeat("0", 64)
	reject(args, "report differs")
	args = write(report, "")
	args[2] = root + "/."
	reject(args, "root is not canonical")
	outside, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outsidePath := filepath.Join(outside, "outside.json")
	if err := os.WriteFile(outsidePath, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "escaping.json")
	t.Run("escaping_symlink", func(t *testing.T) {
		if err := os.Symlink(outsidePath, link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if err := run(testM8ReportReplayArgsV1(root, link, pins), io.Discard); err == nil || !strings.Contains(err.Error(), "contained in root") {
			t.Fatalf("escaping symlink accepted: %v", err)
		}
	})
	reject(testM8ReportReplayArgsV1(root, outsidePath, pins), "contained in root")
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Truncate(m8DiagnosticRetainedMaxBytesV1 + 1)
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("oversized receipt setup: %v %v", err, closeErr)
	}
	reject(testM8ReportReplayArgsV1(root, path, pins), "invalid byte length")
}
