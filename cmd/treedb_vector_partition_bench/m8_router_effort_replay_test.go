package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestM8RouterEffortReplayBindsEntryAndBudgetUnits(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	f := m8QualificationFixturesV1[0]
	f.Vectors, f.Dimensions, f.Queries = 96, 8, 3
	vectors, queries := fixtureData(f)
	f.Checksum = fixtureChecksumFromData(vectors, queries)
	built, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "m3")
	built.owned = false
	oldDir := built.dir
	if err := built.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldDir, dir); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		o            collections.VectorPartitionRouterEffortOptionsV1
		legacyBudget int
	}{
		{"legacy", collections.VectorPartitionRouterEffortOptionsV1{Mode: collections.VectorPartitionRouterEffortLegacyV1, ReturnedWidth: 4, Beam: 4, ScoreBudget: 4}, 0},
		{"hierarchy", collections.VectorPartitionRouterEffortOptionsV1{Mode: collections.VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 2, Beam: 4, ScoreBudget: 64}, 0},
		{"strict-failure", collections.VectorPartitionRouterEffortOptionsV1{Mode: collections.VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: 1}, 0},
		{"ordinary-and-strict-failure", collections.VectorPartitionRouterEffortOptionsV1{Mode: collections.VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assets, err := openM8ProductionExistingAssetSetV1(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = assets.Close() })
			h, err := newM8AttributionHarnessV1(assets)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close() })
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
			budget := int(assets.status.Representatives)
			if tc.legacyBudget != 0 {
				budget = tc.legacyBudget
			}
			if err := h.enableRouterPoliciesV1(tc.o.ReturnedWidth, budget); err != nil {
				t.Fatal(err)
			}
			if err := h.enableRouterEffortV1(tc.o); err != nil {
				t.Fatal(err)
			}
			oracle, err := h.membershipOraclesV1(truth, homes, members, 2)
			if err != nil {
				t.Fatal(err)
			}
			cell, err := m8BuildAttributionV1(t.Context(), assets, homes, members, queries, truth, oracle, 2, 32, 10, budget, make([][]m8CanonicalResultV1, len(queries)), h)
			if err != nil {
				t.Fatal(err)
			}
			row := m8ProductionRowV1{Status: "pass", Probes: 2, EfSearch: 32, Samples: len(queries)}
			for i := range truth {
				row.RecallAtK += m8CanonicalRecallV1(truth[i], cell.Local[i]) / float64(len(truth))
			}
			if !cell.Evidence.ApproximateRouterPartitionCoverageComplete {
				row.Status = "candidate_coverage_shortfall"
			}
			if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
				t.Fatal(err)
			}
			cfg := m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, RouterCandidates: budget, QualityDiagnostics: true, RouterPolicyDiagnostics: true, RouterPolicyWidth: tc.o.ReturnedWidth, RouterEffort: &tc.o}
			report := m8ProductionReportV1{Dataset: f, RouterRepresentatives: assets.status.Representatives, Config: cfg, Variant: &m3VariantDescriptorV1{DatabaseDirectory: dir}, Rows: []m8ProductionRowV1{row}}
			outcomes := m8ProductionRowOutcomesV1{TopKIDs: make([][]string, len(queries)), TopKScoreBits: make([][]uint32, len(queries))}
			for i, results := range cell.Local {
				outcomes.TopKIDs[i] = m8CanonicalIDsV1(results)
				for _, r := range results {
					outcomes.TopKScoreBits[i] = append(outcomes.TopKScoreBits[i], math.Float32bits(r.Score))
				}
			}
			transcript := m8ProductionMeasurementTranscriptV1{Outcomes: []m8ProductionRowOutcomesV1{outcomes}}
			if err := errors.Join(h.Close(), assets.Close()); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			var decoded m8ProductionReportV1
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatal(err)
			}
			if err := m8QualificationRetainedAttributionV1(root, decoded, truth, transcript); err != nil {
				t.Fatal("valid replay", err)
			}
			for name, mutate := range map[string]func(*m8ProductionReportV1){
				"work": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.Work.VectorBytes += 4
				},
				"entry": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.Work.InitialEntryOrdinal = (r.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.Work.InitialEntryOrdinal + 1) % 4
				},
				"unit": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.Work.BudgetUnit = "batch_calls"
				},
				"identity": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.OptionsSHA256 = strings.Repeat("a", 64)
				},
				"population": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterEffort.Queries = r.Rows[0].Attribution.RouterEffort.Queries[:1]
				},
				"unselected": func(r *m8ProductionReportV1) { r.Config.RouterEffort = nil },
				"missing":    func(r *m8ProductionReportV1) { r.Rows[0].Attribution.RouterEffort = nil },
			} {
				t.Run(name, func(t *testing.T) {
					var altered m8ProductionReportV1
					if err := json.Unmarshal(raw, &altered); err != nil {
						t.Fatal(err)
					}
					mutate(&altered)
					if name == "work" || name == "entry" {
						if err := m8RouterEffortEvidenceSelectionV1(altered.Config, altered.Rows[0]); err != nil {
							t.Fatal("mutation must pass structural validation", err)
						}
					}
					if err := m8QualificationRetainedAttributionV1(root, altered, truth, transcript); err == nil {
						t.Fatal("forged retained effort accepted")
					}
				})
			}
		})
	}
}

func TestM8RouterEffortCacheRejectsChangedPopulationAndCancellation(t *testing.T) {
	h, queries, truth, _, _ := qualityFixtureV1(t, 0)
	budget := int(h.assets.status.Representatives)
	if err := h.enableRouterPoliciesV1(1, budget); err != nil {
		t.Fatal(err)
	}
	if err := h.enableRouterEffortV1(collections.VectorPartitionRouterEffortOptionsV1{Mode: collections.VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: 1}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := h.routerEffortEvidenceV1(ctx, queries, truth, 1, budget); !errors.Is(err, context.Canceled) || len(h.effort.byProbes) != 0 {
		t.Fatal("canceled cache publication", err)
	}
	if _, err := h.routerEffortEvidenceV1(t.Context(), queries, truth, 1, budget); err != nil {
		t.Fatal(err)
	}
	queries[0][0] += .01
	if _, err := h.routerEffortEvidenceV1(t.Context(), queries, truth, 1, budget); err == nil {
		t.Fatal("changed query reused receipt")
	}
}

func TestM8RouterEffortPlannerChargesEntryScoringAndCacheGrid(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "2", "-m8-router-effort-mode", collections.VectorPartitionRouterEffortHierarchicalV1, "-m8-router-effort-width", "2", "-m8-router-effort-beam", "4", "-m8-router-effort-score-budget", "8"))
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureManifest{Vectors: 96, Queries: 3, Dimensions: 8}
	cfg.m8RouterPolicyRepresentativeCounts = []int{4}
	a, b, err := m8PlanRouterEffortV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || a < 1 || b < 1 {
		t.Fatal(a, b, err)
	}
	cfg.m8RouterEffort.ScoreBudget = 64
	c, _, err := m8PlanRouterEffortV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || c <= a {
		t.Fatal("extra upper scores uncharged", err)
	}
	cfg.efSearch = []int{16, 32, 64}
	cfg.concurrency = []int{1, 2, 4}
	d, e, err := m8PlanRouterEffortV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || d <= c || e <= b {
		t.Fatal("cached grid work uncharged", err)
	}
	if _, _, err := m8PlanRouterEffortV1(cfg, f, []int{4}, d-1, math.MaxInt64); err == nil {
		t.Fatal("work cap bypass")
	}
	if _, _, err := m8PlanRouterEffortV1(cfg, f, []int{4}, math.MaxInt64, e-1); err == nil {
		t.Fatal("memory cap bypass")
	}
	cfg.m8RouterEffort.Mode = collections.VectorPartitionRouterEffortLegacyV1
	if _, _, err := m8PlanRouterEffortV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64); err == nil {
		t.Fatal("legacy C>N admitted")
	}
}
