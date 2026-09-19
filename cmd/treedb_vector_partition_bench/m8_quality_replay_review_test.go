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
	cell, err := m8BuildAttributionV1(t.Context(), assets, homes, members, queries, truth, oracles, 2, 32, 10, 1, make([][]m8CanonicalResultV1, len(queries)), h)
	if err != nil {
		t.Fatal(err)
	}
	if cell.Evidence.ApproximateRouterPartitionCoverageComplete {
		t.Fatal("fixture did not exhaust unique-domain coverage")
	}
	row := m8ProductionRowV1{Status: "candidate_coverage_shortfall", Probes: 2, EfSearch: 32, Samples: len(queries)}
	if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
		t.Fatal(err)
	}
	report := m8ProductionReportV1{Dataset: fixture, RouterRepresentatives: assets.status.Representatives, Config: m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, RouterCandidates: 1, QualityDiagnostics: true}, Variant: &m3VariantDescriptorV1{DatabaseDirectory: dir}, Rows: []m8ProductionRowV1{row}}
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
