package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Schema-valid diagnostic values are not authority. Even a failed coordinator
// row must reproduce its static observations from independently reopened assets.
func TestM8QualityRetainedShortfallReplaysStaticEvidence(t *testing.T) {
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

	assets, err := openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = assets.Close() })
	// The production hierarchy must score one root per domain before it can
	// return a route. Exercise its typed, no-partial budget refusal.
	approximateBudget := int(assets.manifest.DomainCount) - 1
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
	oracles, err := h.membershipOraclesV1(truth, homes, members, 2)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := m8BuildAttributionV1(t.Context(), assets, homes, members, queries, truth, oracles, 2, 32, 10, approximateBudget, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	if cell.Evidence.ApproximateRouterPartitionCoverageComplete {
		t.Fatal("fixture did not exhaust unique-domain coverage")
	}
	control, err := m8BuildAttributionV1(t.Context(), assets, homes, members, queries, truth, oracles, 2, 32, 10, defaultRouterScoreBudgetV3, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil || !control.Evidence.ApproximateRouterPartitionCoverageComplete {
		t.Fatalf("successful local control: %v", err)
	}
	row := m8ProductionRowV1{Status: "router_score_budget_exhausted", Probes: 2, EfSearch: 32, Samples: len(queries)}
	if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
		t.Fatal(err)
	}
	report := m8ProductionReportV1{Dataset: fixture, RouterRepresentatives: assets.status.Representatives, Config: m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, RouterScoreBudget: approximateBudget, QualityDiagnostics: true}, Variant: &m3VariantDescriptorV1{DatabaseDirectory: dir}, Rows: []m8ProductionRowV1{row}}
	// Failed producer rows carry no successful coordinator results or timings.
	transcript := m8ProductionMeasurementTranscriptV1{Outcomes: []m8ProductionRowOutcomesV1{{}}}
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
		t.Fatal("valid failed row rejected", err)
	}
	for name, mutate := range map[string]func(*m8QualityAttributionV1){
		"query":      func(a *m8QualityAttributionV1) { a.Queries[0].QuerySHA256 = strings.Repeat("a", 64) },
		"truth":      func(a *m8QualityAttributionV1) { a.Queries[0].TruthSHA256 = strings.Repeat("b", 64) },
		"model":      func(a *m8QualityAttributionV1) { a.ModelSHA256 = strings.Repeat("c", 64) },
		"generation": func(a *m8QualityAttributionV1) { a.Generation++ },
		"DP-work":    func(a *m8QualityAttributionV1) { a.Queries[0].DomainCost.WorkBound++ },
		"no-coarsening": func(a *m8QualityAttributionV1) {
			q := &a.Queries[0]
			q.NoCoarseningDomains[0], q.NoCoarseningDomains[1] = q.NoCoarseningDomains[1], q.NoCoarseningDomains[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			var altered m8ProductionReportV1
			if err := json.Unmarshal(raw, &altered); err != nil {
				t.Fatal(err)
			}
			mutate(altered.Rows[0].Attribution.Quality)
			if err := m8QualityEvidenceSelectionV1(altered.Config, altered.Rows[0]); err != nil {
				t.Fatal("test mutation must pass schema validation", err)
			}
			if err := m8QualificationRetainedAttributionV1(root, altered, truth, transcript); err == nil {
				t.Fatal("schema-valid forged static evidence was accepted for failed row")
			}
		})
	}
	// Unavailable coordinator values cannot be invented while static replay passes.
	for _, q := range decoded.Rows[0].Attribution.Quality.Queries {
		if q.Actual != nil || q.CoordinatorReturned != nil {
			t.Fatal("failed row contains local/coordinator observations")
		}
	}
	t.Run("complete_mixed_population", func(t *testing.T) {
		// Construct a mixed measured population from a real local result. The
		// separately replayed static population has no approximate local result;
		// it must neither erase that success nor invent a comparison against it.
		mixed := m8ProductionRowV1{Probes: 2, EfSearch: 32, Concurrency: 1, Samples: len(queries), ElapsedNanos: 100, Accounting: &m8MeasurementAccountingV1{Contract: m8CompleteAttemptsV1, Attempts: []m8MeasuredAttemptV1{
			{Class: "success", Dispatched: true, WorkObserved: true, TerminalNanos: 20, CoordinatorNanos: 10, TruthHits: m8IDHitCountV1(m8CanonicalIDsV1(truth[0]), m8CanonicalIDsV1(control.Local[0]))},
			{Class: m8ProductionRouterScoreBudgetExhaustedV1, Dispatched: true, TerminalNanos: 20},
			{Class: m8ProductionRouterScoreBudgetExhaustedV1, Dispatched: true, TerminalNanos: 20},
		}}}
		if err := m8SummarizeAttemptsV1(&mixed, 10); err != nil {
			t.Fatal(err)
		}
		results := [][]m8CanonicalResultV1{control.Local[0], nil, nil}
		if err := m8AttachAttributionV1(&mixed, cell, results); err != nil {
			t.Fatal(err)
		}
		a := mixed.Attribution
		last := a.StageOwners[len(a.StageOwners)-1]
		if mixed.Status != "measurement_failure" || mixed.RecallAtK <= 0 || a.EndToEndRecallAtK != mixed.RecallAtK || a.CoordinatorMergeIDParity || a.CoordinatorMergeScoreParity || a.ApproximateLocalToEndToEndLossAtK != 0 || last.Owner != "offline_local_comparison_unavailable" || last.Active || last.Delta != 0 || !validM8AttributionV1(a, 10) {
			t.Fatalf("mixed measurement lost or compared with unavailable evidence: %+v", mixed)
		}
		for i, q := range a.Quality.Queries {
			if q.Actual != nil || (q.CoordinatorReturned != nil) != (i == 0) {
				t.Fatal("invented local traversal or missing measured mask")
			}
		}
		report.Rows = []m8ProductionRowV1{mixed}
		report.Config.MeasurementAccounting = m8CompleteAttemptsV1
		outcomes, err := m8ProductionMeasurementTranscriptOutcomesV1(report, []m8MeasuredCellV1{{rowIndex: 0, results: results, durations: []uint64{10, 0, 0}}})
		if err != nil {
			t.Fatal(err)
		}
		transcript := m8ProductionMeasurementTranscriptV1{Outcomes: outcomes}
		if err := m8ValidateProductionMeasurementTranscriptOutcomesV1(transcript, report); err != nil {
			t.Fatal(err)
		}
		if err := m8QualityEvidenceSelectionV1(report.Config, mixed); err != nil {
			t.Fatal(err)
		}
		if err := m8QualificationRetainedAttributionV1(root, report, truth, transcript); err != nil {
			t.Fatal("mixed evidence failed retained replay", err)
		}
		a.CoordinatorMergeIDParity = true
		if validM8AttributionV1(a, 10) {
			t.Fatal("invented comparison parity accepted")
		}
	})
}

func TestM8QualityTracePreparationCancellationDoesNotPublishCache(t *testing.T) {
	h, _, _, _, _ := qualityFixtureV1(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.prepareQualityTraceV1(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation err=%v", err)
	}
	if h.quality.traceValidated[0] || h.quality.packIDs[0] != nil {
		t.Fatal("canceled preparation published an ordinal cache")
	}
	if err := h.prepareQualityTraceV1(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if !h.quality.traceValidated[0] || len(h.quality.packIDs[0]) == 0 {
		t.Fatal("retry did not prepare cache")
	}
	if err := h.prepareQualityTraceV1(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatal("cached trace bypassed cancellation", err)
	}
}
