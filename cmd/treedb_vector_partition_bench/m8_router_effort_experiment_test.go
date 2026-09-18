package main

import (
	"github.com/snissn/gomap/TreeDB/collections"
	"reflect"
	"slices"
	"testing"
)

func TestM8RouterEffortExplicitSelectionAndChildPropagation(t *testing.T) {
	base := append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "2")
	args := append(slices.Clone(base), "-m8-router-effort-mode", collections.VectorPartitionRouterEffortHierarchicalV1, "-m8-router-effort-width", "2", "-m8-router-effort-beam", "4", "-m8-router-effort-score-budget", "64")
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.m8RouterEffort.Mode != collections.VectorPartitionRouterEffortHierarchicalV1 || cfg.m8RouterEffort.ReturnedWidth != 2 || cfg.m8RouterEffort.Beam != 4 || cfg.m8RouterEffort.ScoreBudget != 64 {
		t.Fatal("missing effort coordinate", cfg.m8RouterEffort)
	}
	report := m8QualificationCommandConfigV1(cfg)
	if report.RouterEffort == nil || *report.RouterEffort != cfg.m8RouterEffort {
		t.Fatal("command omitted coordinate")
	}
	child, err := m8VariantProcessArgsV1(append([]string{"bench"}, args...), t.TempDir(), 0, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := parseConfig(child)
	if err != nil || replay.m8RouterEffort != cfg.m8RouterEffort {
		t.Fatal("child coordinate lost", err)
	}
	for _, bad := range [][]string{{"-m8-router-effort-width", "2"}, {"-m8-router-effort-mode", "bogus"}, {"-m8-router-effort-mode", collections.VectorPartitionRouterEffortHierarchicalV1, "-m8-router-effort-width", "2", "-m8-router-effort-beam", "1", "-m8-router-effort-score-budget", "8"}} {
		if _, err := parseConfig(append(slices.Clone(base), bad...)); err == nil {
			t.Fatal("invalid effort accepted", bad)
		}
	}
	ordinary, err := parseConfig(qualityCLIArgsV1(t))
	if err != nil || ordinary.m8RouterEffort.Mode != "" || m8QualificationCommandConfigV1(ordinary).RouterEffort != nil {
		t.Fatal("default changed", err)
	}
}

func TestM8RouterEffortNeverDropsFailedQueries(t *testing.T) {
	for _, mode := range []string{collections.VectorPartitionRouterEffortLegacyV1, collections.VectorPartitionRouterEffortHierarchicalV1} {
		h, queries, truth, homes, members := qualityFixtureV1(t, 0)
		budget := int(h.assets.status.Representatives)
		if err := h.enableRouterPoliciesV1(1, budget); err != nil {
			t.Fatal(err)
		}
		o := collections.VectorPartitionRouterEffortOptionsV1{Mode: mode, ReturnedWidth: 1, Beam: 1, ScoreBudget: 1}
		if err := h.enableRouterEffortV1(o); err != nil {
			t.Fatal(err)
		}
		oracles, err := h.membershipOraclesV1(truth, homes, members, 2)
		if err != nil {
			t.Fatal(err)
		}
		cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, queries, truth, oracles, 2, 32, 10, budget, make([][]m8CanonicalResultV1, len(queries)), h)
		if err != nil {
			t.Fatal(err)
		}
		e := cell.Evidence.RouterEffort
		if e == nil || len(e.Queries) != len(queries) {
			t.Fatal("lost population")
		}
		for _, q := range e.Queries {
			if q.Status != "candidate_coverage_shortfall" && q.Status != "score_budget_exhausted" {
				t.Fatal("failure became success", q.Status)
			}
			if q.Coverage != nil {
				t.Fatal("failed route invented coverage")
			}
		}
		row := m8ProductionRowV1{Status: "pass", Probes: 2, Samples: len(queries), Attribution: cell.Evidence}
		cfg := m8ProductionConfigEvidenceV1{TopK: 10, QualityDiagnostics: true, RouterPolicyDiagnostics: true, RouterPolicyWidth: 1, RouterCandidates: budget, RouterEffort: &o}
		if err := m8RouterEffortEvidenceSelectionV1(cfg, row); err != nil {
			t.Fatal(err)
		}
		before := *e
		again, err := h.routerEffortEvidenceV1(t.Context(), queries, truth, 2, budget)
		if err != nil || !reflect.DeepEqual(before, *again) {
			t.Fatal("cache changed", err)
		}
		altered := o
		altered.Mode = "invalid"
		cfg.RouterEffort = &altered
		if err := m8RouterEffortEvidenceSelectionV1(cfg, row); err == nil {
			t.Fatal("forged entry mode accepted")
		}
	}
}
