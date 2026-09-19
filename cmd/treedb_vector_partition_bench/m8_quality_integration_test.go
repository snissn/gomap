package main

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func qualityCLIArgsV1(t *testing.T) []string {
	t.Helper()
	return []string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-mode", m8ProductionMultiGroupModeV1,
		"-partitions", "4", "-raft-groups", "2", "-probes", "1", "-overlap", "0", "-top-k", "10", "-ef-search", "32", "-concurrency", "1"}
}

func TestM8QualityDiagnosticsExplicitCLISelection(t *testing.T) {
	args := qualityCLIArgsV1(t)
	legacy, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.m8QualityDiagnostics || legacy.m8QualityTraceQueries != 0 {
		t.Fatal("ordinary mode opted into diagnostics")
	}
	selected, err := parseConfig(append(slices.Clone(args), "-m8-quality-diagnostics", "-m8-quality-trace-queries", "1"))
	if err != nil || !selected.m8QualityDiagnostics || selected.m8QualityTraceQueries != 1 {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
	for _, tail := range [][]string{{"-m8-quality-trace-queries", "1"}, {"-m8-quality-diagnostics", "-top-k", "11"}, {"-m8-quality-diagnostics", "-m8-quality-trace-queries", "9"}, {"-m8-quality-diagnostics", "-m8-quality-trace-queries", "-1"}, {"-m8-quality-diagnostics", "-mode", "simulation"}} {
		if _, err := parseConfig(append(slices.Clone(args), tail...)); err == nil {
			t.Fatalf("accepted %v", tail)
		}
	}
	want := m8ConfigEvidenceFromCommandV1ForQualityTest(selected)
	if reflect.DeepEqual(want, m8ConfigEvidenceFromCommandV1ForQualityTest(legacy)) {
		t.Fatal("command binding dropped quality selection")
	}
	child, err := m8VariantProcessArgsV1(append([]string{"bench"}, append(slices.Clone(args), "-m8-quality-diagnostics", "-m8-quality-trace-queries=1")...), t.TempDir(), 0, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	propagated, err := parseConfig(child)
	if err != nil || !propagated.m8QualityDiagnostics || propagated.m8QualityTraceQueries != 1 {
		t.Fatalf("child lost diagnostic flags: %v %v", child, err)
	}
}

// Keep this assertion tied to the real command-to-report constructor.
func m8ConfigEvidenceFromCommandV1ForQualityTest(c config) m8ProductionConfigEvidenceV1 {
	return m8CommandBoundProductionConfigV1(m8QualificationCommandConfigV1(c))
}

func qualityFixtureV1(t *testing.T, traces int) (*m8AttributionHarnessV1, [][]float64, [][]m8CanonicalResultV1, map[string]uint32, map[string][]uint32) {
	t.Helper()
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 96)
	for i := range vectors {
		angle := 2 * math.Pi * float64(i) / float64(len(vectors))
		vectors[i] = []float64{math.Cos(angle), math.Sin(angle), .15 + float64(i%7)/20, float64(i%3) / 9}
	}
	assets, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"quality-a", "quality-b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assets.Close(); err != nil {
			t.Error(err)
		}
	})
	h, err := newM8AttributionHarnessV1(assets)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	})
	queries := [][]float64{vectors[7], vectors[43]}
	truth, err := m8ExactTruthV1(assets.collection, assets.manifest, queries, 10)
	if err != nil {
		t.Fatal(err)
	}
	homes, members, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
	if err != nil {
		t.Fatal(err)
	}
	if traces >= 0 {
		err = h.enableQualityV1(t.Context(), queries, truth, homes, members, traces, m8CoverageLimitsV1{WorkUnits: maxBenchmarkWorkUnits, Bytes: maxFixtureBytes})
		if err != nil {
			t.Fatal(err)
		}
	}
	return h, queries, truth, homes, members
}

func TestM8QualityAttributionUsesRealPacksAndLegacyParity(t *testing.T) {
	h, queries, truth, homes, members := qualityFixtureV1(t, 1)
	exhaustive := make([][]m8CanonicalResultV1, len(queries))
	for _, probes := range []int{1, 2, 4} {
		oracles, err := h.membershipOraclesV1(truth, homes, members, probes)
		if err != nil {
			t.Fatal(err)
		}
		old, err := m8MembershipOracleRecallCacheV1(truth, homes, members, h.assets.manifest, probes)
		if err != nil || !reflect.DeepEqual(old, oracles) {
			t.Fatalf("DP differs from subset oracle p=%d: %v", probes, err)
		}
		cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, probes, 32, 10, defaultRouterScoreBudgetV3, exhaustive, h)
		if err != nil {
			t.Fatal(err)
		}
		if err := m8ValidateQualityEvidenceV1(cell.Evidence.Quality, 10, probes, len(queries), cell.Evidence.ApproximateRouterPartitionCoverageComplete); err != nil {
			t.Fatalf("quality p=%d: %v", probes, err)
		}
		if !cell.Evidence.ExhaustivePartitionIDParity || !cell.Evidence.ExhaustivePartitionScoreParity {
			t.Fatal("exact union is not canonical")
		}
		// Reopen a second ordinary prepared owner on the SAME immutable assets.
		plain, err := newM8AttributionHarnessV1(h.assets)
		if err != nil {
			t.Fatal(err)
		}
		control, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, old, probes, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), plain)
		closeErr := plain.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("legacy run=%v close=%v", err, closeErr)
		}
		q := cell.Evidence.Quality
		cell.Evidence.Quality = nil
		cell.qualityTruth = nil
		if !reflect.DeepEqual(cell, control) {
			t.Fatalf("diagnostic changed ordinary outcome/work at p=%d\nnew=%+v\nold=%+v", probes, cell, control)
		}
		// Validate persisted-owner replay, including the sampled actual masks.
		replay, err := newM8AttributionHarnessV1(h.assets)
		if err != nil {
			t.Fatal(err)
		}
		if err := replay.enableQualityV1(t.Context(), queries, truth, homes, members, 1, m8CoverageLimitsV1{WorkUnits: maxBenchmarkWorkUnits, Bytes: maxFixtureBytes}); err != nil {
			t.Fatal(err)
		}
		again, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, probes, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), replay)
		replay.Close()
		if err != nil || !m8QualityReplayEqualV1(q, again.Evidence.Quality) {
			t.Fatalf("reopen replay mismatch p=%d: %v", probes, err)
		}
		if q.Queries[0].Actual == nil || q.Queries[0].Actual.Scored == nil || q.Queries[1].Actual == nil || q.Queries[1].Actual.Scored != nil {
			t.Fatal("trace sample coverage is dishonest")
		}
		if probes == 4 && (q.Queries[0].NoCoarseningMask != 1023 || q.Queries[0].NoCoarseningPacks != 4) {
			t.Fatal("all-pack coverage lost truth")
		}
	}
}

func TestM8QualityEvidenceTamperAndSelection(t *testing.T) {
	h, queries, truth, homes, members := qualityFixtureV1(t, 0)
	oracles, _ := h.membershipOraclesV1(truth, homes, members, 2)
	cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	row := m8ProductionRowV1{Status: "pass", Samples: len(queries), Probes: 2, Attribution: cell.Evidence}
	if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
		t.Fatal(err)
	}
	cfg := m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, QualityDiagnostics: true}
	if err := m8QualityEvidenceSelectionV1(cfg, row); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cell.Evidence.Quality)
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*m8QualityAttributionV1){
		"method":          func(q *m8QualityAttributionV1) { q.Method = "old" },
		"identity":        func(q *m8QualityAttributionV1) { q.ModelSHA256 = "wrong" },
		"physical":        func(q *m8QualityAttributionV1) { q.Queries[0].NoCoarseningPacks++ },
		"duplicate-route": func(q *m8QualityAttributionV1) { q.Queries[0].ExactDomains[1] = q.Queries[0].ExactDomains[0] },
		"signed":          func(q *m8QualityAttributionV1) { q.Queries[0].NoCoarseningToExactGained ^= 1 },
		"false-sample":    func(q *m8QualityAttributionV1) { x := uint16(0); q.Queries[0].Actual.Scored = &x },
		"curve":           func(q *m8QualityAttributionV1) { q.Queries[0].DomainCost.MinimumCosts[0] = 1 },
		"absent-actual":   func(q *m8QualityAttributionV1) { q.Queries[0].Actual = nil },
	} {
		t.Run(name, func(t *testing.T) {
			var q m8QualityAttributionV1
			if err := json.Unmarshal(encoded, &q); err != nil {
				t.Fatal(err)
			}
			change(&q)
			if err := m8ValidateQualityEvidenceV1(&q, 10, 2, len(queries), true); err == nil {
				t.Fatal("forged evidence accepted")
			}
		})
	}
	cfg.QualityDiagnostics = false
	if err := m8QualityEvidenceSelectionV1(cfg, row); err == nil {
		t.Fatal("unselected evidence accepted")
	}
	row.Attribution.Quality = nil
	if err := m8QualityEvidenceSelectionV1(cfg, row); err != nil {
		t.Fatal("legacy row rejected", err)
	}
	cfg.QualityDiagnostics = true
	if err := m8QualityEvidenceSelectionV1(cfg, row); err == nil {
		t.Fatal("missing selected evidence accepted")
	}
	// Valid-looking changed identity is rejected by actual replay, not guessed
	// from schema alone.
	var altered m8QualityAttributionV1
	json.Unmarshal(encoded, &altered)
	altered.Queries[0].QuerySHA256 = strings.Repeat("a", 64)
	if m8QualityReplayEqualV1(cell.Evidence.Quality, &altered) {
		t.Fatal("query identity drift accepted")
	}
}

func TestM8QualityCacheIdentityAndCompletePackExpansion(t *testing.T) {
	h, queries, truth, homes, members := qualityFixtureV1(t, 0)
	if err := h.enableQualityV1(t.Context(), queries, truth, homes, members, 0, m8CoverageLimitsV1{maxBenchmarkWorkUnits, maxFixtureBytes}); err == nil {
		t.Fatal("double initialize")
	}
	changed := slices.Clone(truth)
	changed[0] = slices.Clone(truth[0])
	changed[0][0].Score = 0
	if _, err := h.membershipOraclesV1(changed, homes, members, 1); err == nil {
		t.Fatal("cache accepts changed truth")
	}
	c := m8QualityCacheV1{packDomains: []uint32{0, 0, 1}, packCosts: []int64{2, 1}}
	if _, err := c.domainsForPacksV1([]uint32{0}); err == nil {
		t.Fatal("partial domain accepted")
	}
	got, err := c.domainsForPacksV1([]uint32{2, 0, 1})
	if err != nil || !slices.Equal(got, []uint32{1, 0}) {
		t.Fatalf("expansion=%v %v", got, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.exactQualityUnionV1(canceled, 0, m8Query32V1(queries[0]), truth[0], 10); err == nil {
		t.Fatal("canceled scan succeeded")
	}
}

func TestM8QualityWorkPlannerChargesOnceAndRejectsBeforeAllocation(t *testing.T) {
	args := append(qualityCLIArgsV1(t), "-m8-quality-diagnostics")
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	fixture := fixtureManifest{Vectors: 96, Queries: 2, Dimensions: 4}
	plan, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if plan.QualityDiagnosticWorkUnits <= 0 || plan.QualityDiagnosticBytes <= 0 || plan.MembershipOracleSubsetEvaluations != 0 {
		t.Fatalf("wrong quality charges %+v", plan)
	}
	legacy := cfg
	legacy.m8QualityDiagnostics = false
	old, err := validateM8BenchmarkWork(legacy, fixture, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if old.QualityDiagnosticBytes != 0 || old.MembershipOracleSubsetEvaluations == 0 {
		t.Fatal("legacy accounting reinterpreted")
	}
	if _, err := validateM8BenchmarkWork(cfg, fixture, 1, maxFixtureBytes); err == nil {
		t.Fatal("work cap ignored")
	}
	if _, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, 1); err == nil {
		t.Fatal("memory cap ignored")
	}
	cfg.m8QualityTraceQueries = 3
	if _, err := validateM8BenchmarkWork(cfg, fixture, maxBenchmarkWorkUnits, maxFixtureBytes); err == nil {
		t.Fatal("sample exceeds query population")
	}
}

func TestM8QualityCoordinatorMasksDoNotMutateOtherRows(t *testing.T) {
	h, queries, truth, homes, members := qualityFixtureV1(t, 0)
	oracles, err := h.membershipOraclesV1(truth, homes, members, 2)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	good := m8ProductionRowV1{Status: "pass", Samples: len(queries), Probes: 2}
	if err := m8AttachAttributionV1(&good, cell, cell.Local); err != nil {
		t.Fatal(err)
	}
	if cell.Evidence.Quality.Queries[0].CoordinatorReturned != nil {
		t.Fatal("measured row mutated cached attribution")
	}
	bad := m8ProductionRowV1{Status: "candidate_coverage_shortfall", Samples: len(queries), Probes: 2}
	if err := m8AttachAttributionV1(&bad, cell, make([][]m8CanonicalResultV1, len(queries))); err != nil {
		t.Fatal(err)
	}
	if bad.Attribution.Quality.Queries[0].Actual != nil || bad.Attribution.Quality.Queries[0].CoordinatorReturned != nil {
		t.Fatal("failed row contains invented local/coordinator observation")
	}
	if good.Attribution.Quality.Queries[0].Actual == nil || good.Attribution.Quality.Queries[0].CoordinatorReturned == nil {
		t.Fatal("failed row erased another row's measurements")
	}
	cfg := m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, QualityDiagnostics: true}
	if err := m8QualityEvidenceSelectionV1(cfg, good); err != nil {
		t.Fatal(err)
	}
	if err := m8QualityEvidenceSelectionV1(cfg, bad); err != nil {
		t.Fatal(err)
	}
}

func TestM8QualityPlannerAllowsDPBeyondLegacyCombinations(t *testing.T) {
	// Only compare the diagnostic planner, not an executable full-corpus
	// campaign. Source/truth and engine budgets remain separately enforced.
	cfg := config{m8QualityDiagnostics: true, partitions: 40, topK: 10, probes: []int{20}, efSearch: []int{32}, concurrency: []int{1}}
	f := fixtureManifest{Vectors: 400, Queries: 2, Dimensions: 4}
	if _, err := m8MembershipOracleCombinationCountV1(40, 20, maxBenchmarkWorkUnits); err == nil {
		t.Fatal("expected legacy subset limit")
	}
	w, b, err := m8PlanQualityDiagnosticsV1(cfg, f, []int{40}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil || w <= 0 || b <= 0 {
		t.Fatalf("bounded DP shape refused: work=%d bytes=%d err=%v", w, b, err)
	}
	cfg.concurrency = []int{1, 2, 4}
	w2, b2, err := m8PlanQualityDiagnosticsV1(cfg, f, []int{40}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil || w2 <= w || b2 <= b {
		t.Fatal("extra measured rows not charged")
	}
}
