package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestM8RouterPolicyExplicitCLISelection(t *testing.T) {
	base := qualityCLIArgsV1(t)
	args := append(slices.Clone(base), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "4")
	selected, err := parseConfig(args)
	if err != nil || !selected.m8RouterPolicyDiagnostics || selected.m8RouterPolicyWidth != 4 {
		t.Fatalf("explicit policy diagnostic unavailable: %v", err)
	}
	for _, tail := range [][]string{{"-m8-router-policy-diagnostics"}, {"-m8-router-policy-width", "1"}, {"-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "-1"}, {"-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "999999999"}} {
		if _, err := parseConfig(append(slices.Clone(base), tail...)); err == nil {
			t.Fatalf("accepted invalid %v", tail)
		}
	}
	legacy, err := parseConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.m8RouterPolicyDiagnostics || legacy.m8RouterPolicyWidth != 0 {
		t.Fatal("ordinary mode changed")
	}
	want := m8CommandBoundProductionConfigV1(m8QualificationCommandConfigV1(selected))
	if !want.RouterPolicyDiagnostics || want.RouterPolicyWidth != 4 {
		t.Fatal("command/report identity omitted policy")
	}
	child, err := m8VariantProcessArgsV1(append([]string{"bench"}, args...), t.TempDir(), 0, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseConfig(child)
	if err != nil || !got.m8RouterPolicyDiagnostics || got.m8RouterPolicyWidth != 4 {
		t.Fatal("child lost policy", err)
	}
}

func policyCellFixtureV1(t *testing.T, width, probes int) (*m8AttributionHarnessV1, [][]float64, [][]m8CanonicalResultV1, map[string]uint32, map[string][]uint32, m8AttributionCellV1) {
	t.Helper()
	h, queries, truth, homes, members := qualityFixtureV1(t, 0)
	budget := int(h.assets.status.Representatives)
	if err := h.enableRouterPoliciesV1(width, budget); err != nil {
		t.Fatal(err)
	}
	oracles, err := h.membershipOraclesV1(truth, homes, members, probes)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, probes, 32, 10, budget, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	return h, queries, truth, homes, members, cell
}

func TestM8RouterPolicyRealOwnerReplayAndOrdinaryParity(t *testing.T) {
	h, queries, truth, homes, members, cell := policyCellFixtureV1(t, 0, 2)
	e := cell.Evidence.RouterPolicies
	if err := m8ValidateRouterPolicyEvidenceV1(e, cell.Evidence.Quality, 10, 2, len(queries)); err != nil {
		t.Fatal(err)
	}
	for _, q := range e.Queries {
		if q.Exact.Status != "pass" || q.Approximate.Status != "pass" {
			t.Fatal("unexpected policy refusal")
		}
		if q.Exact.Coverage.DistancePacks != 2 || q.Approximate.Coverage.HybridPacks != 2 {
			t.Fatal("wrong physical cost")
		}
	}
	control, err := newM8AttributionHarnessV1(h.assets)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := control.enableQualityV1(t.Context(), queries, truth, homes, members, 0, m8CoverageLimitsV1{WorkUnits: maxBenchmarkWorkUnits, Bytes: maxFixtureBytes}); err != nil {
		t.Fatal(err)
	}
	oracles, _ := control.membershipOraclesV1(truth, homes, members, 2)
	plain, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 32, 10, int(h.assets.status.Representatives), make([][]m8CanonicalResultV1, len(queries)), control)
	if err != nil {
		t.Fatal(err)
	}
	cell.Evidence.RouterPolicies = nil
	if !reflect.DeepEqual(cell, plain) {
		t.Fatal("opt-in comparison changed ordinary attribution/search")
	}
	if err := control.enableRouterPoliciesV1(0, int(h.assets.status.Representatives)); err != nil {
		t.Fatal(err)
	}
	replay, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 32, 10, int(h.assets.status.Representatives), make([][]m8CanonicalResultV1, len(queries)), control)
	if err != nil || !m8RouterPolicyReplayEqualV1(e, replay.Evidence.RouterPolicies) {
		t.Fatal("independent owner replay differs", err)
	}
	// Local EF cannot change candidate-set or policy evidence. Cached values are
	// immutable; a fresh replay independently recomputes before acceptance.
	again, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 16, 10, int(h.assets.status.Representatives), make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil || again.Evidence.RouterPolicies != e {
		t.Fatal("EF rebuilt the policy candidate set", err)
	}
	changed := slices.Clone(queries)
	changed[0] = slices.Clone(changed[0])
	changed[0][0] += .01
	if _, err := h.routerPolicyEvidenceV1(t.Context(), changed, truth, 2, int(h.assets.status.Representatives)); err == nil {
		t.Fatal("changed query reused cache")
	}
	if _, err := h.routerPolicyEvidenceV1(t.Context(), queries, truth, 2, 1); err == nil {
		t.Fatal("changed retrieval reused cache")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.routerPolicyEvidenceV1(ctx, queries, truth, 2, int(h.assets.status.Representatives)); err == nil {
		t.Fatal("cached diagnostic ignored cancellation")
	}
}

func TestM8RouterPolicyShortfallsRetainFullPopulation(t *testing.T) {
	_, queries, _, _, _, cell := policyCellFixtureV1(t, 1, 2)
	e := cell.Evidence.RouterPolicies
	if len(e.Queries) != len(queries) {
		t.Fatal("failed query removed")
	}
	if err := m8ValidateRouterPolicyEvidenceV1(e, cell.Evidence.Quality, 10, 2, len(queries)); err != nil {
		t.Fatal(err)
	}
	for _, q := range e.Queries {
		for _, o := range []m8RouterPolicyOutcomeV1{q.Exact, q.Approximate} {
			if o.Status != "candidate_coverage_shortfall" || o.Coverage != nil || !o.Comparison.CollectionComplete || o.Comparison.Candidates == 0 {
				t.Fatal("shortfall not explicit", o)
			}
		}
	}
	if !cell.Evidence.ApproximateRouterPartitionCoverageComplete {
		t.Fatal("diagnostic width altered ordinary serving budget")
	}
}

func TestM8RouterPolicyTamperSelectionAndCandidateIdentity(t *testing.T) {
	_, queries, truth, _, _, cell := policyCellFixtureV1(t, 0, 2)
	e := cell.Evidence.RouterPolicies
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*m8RouterPolicyEvidenceV1){
		"method":     func(e *m8RouterPolicyEvidenceV1) { e.Method = "other" },
		"query":      func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].QuerySHA256 = strings.Repeat("a", 64) },
		"model":      func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Comparison.ModelSHA256 = strings.Repeat("a", 64) },
		"generation": func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Approximate.Comparison.Generation++ },
		"signed":     func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Coverage.HybridGained ^= 1 },
		"packs":      func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Coverage.HybridPacks++ },
		"votes":      func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Comparison.Hybrid[0].Frequency++ },
		"duplicate-domain": func(e *m8RouterPolicyEvidenceV1) {
			e.Queries[0].Exact.Comparison.Distance[1] = e.Queries[0].Exact.Comparison.Distance[0]
		},
		"false-success":  func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Status = "candidate_coverage_shortfall" },
		"full-scan-cost": func(e *m8RouterPolicyEvidenceV1) { e.Queries[0].Exact.Comparison.Candidates-- },
		"dropped-query":  func(e *m8RouterPolicyEvidenceV1) { e.Queries = e.Queries[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			var altered m8RouterPolicyEvidenceV1
			if err := json.Unmarshal(raw, &altered); err != nil {
				t.Fatal(err)
			}
			mutate(&altered)
			if err := m8ValidateRouterPolicyEvidenceV1(&altered, cell.Evidence.Quality, 10, 2, len(queries)); err == nil {
				t.Fatal("forged record accepted")
			}
		})
	}
	var changed m8RouterPolicyEvidenceV1
	json.Unmarshal(raw, &changed)
	changed.Queries[0].Exact.Comparison.CandidateSetSHA256 = strings.Repeat("a", 64)
	if m8RouterPolicyReplayEqualV1(e, &changed) {
		t.Fatal("different candidate set admitted by replay")
	}
	row := m8ProductionRowV1{Status: "pass", Samples: len(queries), Probes: 2}
	if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
		t.Fatal(err)
	}
	cfg := m8ProductionConfigEvidenceV1{QualityDiagnostics: true, RouterPolicyDiagnostics: true, TopK: 10, RouterCandidates: e.ApproximateBudget}
	if err := m8RouterPolicyEvidenceSelectionV1(cfg, row); err != nil {
		t.Fatal(err)
	}
	cfg.RouterPolicyDiagnostics = false
	if err := m8RouterPolicyEvidenceSelectionV1(cfg, row); err == nil {
		t.Fatal("unselected policy data accepted")
	}
	row.Attribution.RouterPolicies = nil
	if err := m8RouterPolicyEvidenceSelectionV1(cfg, row); err != nil {
		t.Fatal("legacy row refused", err)
	}
	cfg.RouterPolicyDiagnostics = true
	if err := m8RouterPolicyEvidenceSelectionV1(cfg, row); err == nil {
		t.Fatal("missing selected policy data accepted")
	}
	if len(truth) != len(queries) {
		t.Fatal("fixture mismatch")
	}
}

func TestM8RouterPolicyResourcePlanAndRetainedModel(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics"))
	if err != nil {
		t.Fatal(err)
	}
	m := fixtureManifest{Vectors: 96, Queries: 2, Dimensions: 4}
	work, bytes, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil || work <= 0 || bytes <= 0 {
		t.Fatal(work, bytes, err)
	}
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, work-1, maxFixtureBytes); err == nil {
		t.Fatal("work cap ignored")
	}
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, bytes-1); err == nil {
		t.Fatal("byte cap ignored")
	}
	cfg.m8ExistingDB = "retained"
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes); err == nil {
		t.Fatal("assumed default model for retained input")
	}
	cfg.m8RouterPolicyRepresentativeCounts = []int{4}
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes); err != nil {
		t.Fatal(err)
	}
	cfg.m8RouterPolicyRepresentativeCounts = []int{3}
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes); err == nil {
		t.Fatal("invalid count accepted")
	}
	cfg.m8RouterPolicyDiagnostics = false
	cfg.m8RouterPolicyWidth = 0
	if w, b, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, nil, 1, 1); err != nil || w != 0 || b != 0 {
		t.Fatal("disabled mode allocated/planned work")
	}
}

func TestM8RouterPolicyRetainedAttributionReplay(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	fixture := m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 96, 8, 3
	vectors, queries := fixtureData(fixture)
	fixture.Checksum = fixtureChecksumFromData(vectors, queries)
	built, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "m3")
	built.owned = false
	builtDir := built.dir
	if err := built.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(builtDir, dir); err != nil {
		t.Fatal(err)
	}
	for _, coordinate := range []struct{ width, budget int }{{0, 0}, {1, 0}, {1, 1}} {
		width := coordinate.width
		t.Run(fmt.Sprintf("width=%d/budget=%d", width, coordinate.budget), func(t *testing.T) {
			assets, err := openM8ProductionExistingAssetSetV1(dir)
			if err != nil {
				t.Fatal(err)
			}
			h, err := newM8AttributionHarnessV1(assets)
			if err != nil {
				assets.Close()
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					h.Close()
					assets.Close()
				}
			}()
			truth, err := m8ExactTruthFixtureV1(vectors, queries, 10)
			if err != nil {
				t.Fatal(err)
			}
			homes, members, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.enableQualityV1(t.Context(), queries, truth, homes, members, 0, m8CoverageLimitsV1{WorkUnits: maxBenchmarkWorkUnits, Bytes: maxFixtureBytes}); err != nil {
				t.Fatal(err)
			}
			budget := 1024 // Full-pool replay is not a score-call budget of N.
			if coordinate.budget != 0 {
				budget = coordinate.budget
			}
			if err := h.enableRouterPoliciesV1(width, budget); err != nil {
				t.Fatal(err)
			}
			oracles, err := h.membershipOraclesV1(truth, homes, members, 2)
			if err != nil {
				t.Fatal(err)
			}
			cell, err := m8BuildAttributionV1(t.Context(), assets, homes, members, queries, truth, oracles, 2, 32, 10, budget, make([][]m8CanonicalResultV1, len(queries)), h)
			if coordinate.budget == 1 {
				if err != nil {
					t.Fatalf("exhausted score budget was not retained: %v", err)
				}
				if err := m8ValidateRouterPolicyEvidenceV1(cell.Evidence.RouterPolicies, cell.Evidence.Quality, 10, 2, len(queries)); err != nil {
					t.Fatal(err)
				}
				for _, query := range cell.Evidence.RouterPolicies.Queries {
					if query.Approximate.Status != m8ProductionRouterScoreBudgetExhaustedV1 || query.Approximate.Coverage != nil || query.Approximate.Comparison.CollectionComplete || query.Approximate.Comparison.ScoreCalls != 1 {
						t.Fatalf("score-budget refusal was not explicit: %+v", query.Approximate)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var recall float64
			for i := range truth {
				recall += m8CanonicalRecallV1(truth[i], cell.Local[i])
			}
			row := m8ProductionRowV1{Status: "pass", Probes: 2, EfSearch: 32, Samples: len(queries), RecallAtK: recall / float64(len(truth))}
			if !cell.Evidence.ApproximateRouterPartitionCoverageComplete {
				row.Status = "candidate_coverage_shortfall"
			}
			if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
				t.Fatal(err)
			}
			report := m8ProductionReportV1{Dataset: fixture, RouterRepresentatives: assets.status.Representatives, Config: m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, RouterCandidates: budget, RouterWidth: assets.routerWidth, RouterBeam: assets.routerBeam, QualityDiagnostics: true, RouterPolicyDiagnostics: true, RouterPolicyWidth: width}, Variant: &m3VariantDescriptorV1{DatabaseDirectory: dir}, Rows: []m8ProductionRowV1{row}}
			outcome := m8ProductionRowOutcomesV1{TopKIDs: make([][]string, len(queries)), TopKScoreBits: make([][]uint32, len(queries))}
			for i, rows := range cell.Local {
				outcome.TopKIDs[i] = m8CanonicalIDsV1(rows)
				for _, r := range rows {
					outcome.TopKScoreBits[i] = append(outcome.TopKScoreBits[i], math.Float32bits(r.Score))
				}
			}
			transcript := m8ProductionMeasurementTranscriptV1{Outcomes: []m8ProductionRowOutcomesV1{outcome}}
			if err := errors.Join(h.Close(), assets.Close()); err != nil {
				t.Fatal(err)
			}
			closed = true
			// Serialize and decode before independently reopening every source owner.
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			var decoded m8ProductionReportV1
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := m8QualificationRetainedAttributionV1(root, decoded, truth, transcript); err != nil {
				t.Fatal("retained public analyzer rejected valid diagnostic", err)
			}
			for name, mutate := range map[string]func(*m8ProductionReportV1){
				"candidate-set": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterPolicies.Queries[0].Exact.Comparison.CandidateSetSHA256 = strings.Repeat("a", 64)
				},
				"sequence": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterPolicies.Queries[0].Approximate.Comparison.CandidateSequenceSHA256 = strings.Repeat("b", 64)
				},
				"work": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterPolicies.Queries[0].Exact.Comparison.Candidates--
				},
				"ignored-failure": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterPolicies.Queries = r.Rows[0].Attribution.RouterPolicies.Queries[:1]
				},
				"flag":  func(r *m8ProductionReportV1) { r.Config.RouterPolicyDiagnostics = false },
				"width": func(r *m8ProductionReportV1) { r.Config.RouterPolicyWidth = 2 },
			} {
				t.Run(name, func(t *testing.T) {
					var altered m8ProductionReportV1
					if err := json.Unmarshal(raw, &altered); err != nil {
						t.Fatal(err)
					}
					mutate(&altered)
					if err := m8QualificationRetainedAttributionV1(root, altered, truth, transcript); err == nil {
						t.Fatal("forged retained diagnostic accepted")
					}
				})
			}
		})
	}
}

func TestM8RouterPolicyPlannerChargesCacheHitBookkeeping(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics"))
	if err != nil {
		t.Fatal(err)
	}
	m := fixtureManifest{Vectors: 96, Queries: 2, Dimensions: 4}
	w, b, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.efSearch = []int{32, 64}
	moreWork, moreBytes, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if moreWork <= w || moreBytes <= b {
		t.Fatalf("extra EF cache validation/copy not charged: work %d -> %d bytes %d -> %d", w, moreWork, b, moreBytes)
	}
	cfg.concurrency = []int{1, 2}
	rowWork, rowBytes, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, maxBenchmarkWorkUnits, maxFixtureBytes)
	if err != nil {
		t.Fatal(err)
	}
	if rowWork <= moreWork || rowBytes <= moreBytes {
		t.Fatal("extra retained row validation/copy not charged")
	}
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, w, maxFixtureBytes); err == nil {
		t.Fatal("old one-cell budget admitted added work")
	}
}
