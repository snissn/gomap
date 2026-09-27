package main

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestM8RouterPolicyRepresentativeCombinedAdmissionV1(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "64"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.partitions, cfg.raftGroups, cfg.raftNodes = 16, 4, 3
	cfg.probes, cfg.efSearch, cfg.concurrency = []int{2}, []int{96}, []int{1}
	cfg.overlaps, cfg.warmup = []float64{.20}, 1
	cfg.m8ExistingDB = "/not-opened/retained-variant"
	cfg.m8OracleDomainCounts = []int{16}
	cfg.m8RouterPolicyRepresentativeCounts = []int{256}
	for _, vectors := range []int{100000, 250000} {
		t.Run(fmt.Sprint(vectors), func(t *testing.T) {
			m := fixtureManifest{Vectors: vectors, Queries: 512, Dimensions: 128}
			cfg.m8MaxExactTruthVisits = 600000000
			if vectors == 250000 {
				cfg.m8MaxExactTruthVisits = 1500000000
			}
			cfg.routerCandidates = 256
			refused, err := validateM8BenchmarkWork(cfg, m, maxBenchmarkWorkUnits, maxFixtureBytes)
			if err == nil || !strings.Contains(err.Error(), "combined attribution/diagnostic work") {
				t.Fatalf("C256 did not refuse the combined work cap: %v", err)
			}
			t.Logf("C256 REFUSED before collection: attribution=%d quality=%d policy=%d: %v", refused.AttributionDiagnosticWorkUnits, refused.QualityDiagnosticWorkUnits, refused.RouterPolicyDiagnosticWorkUnits, err)
			cfg.routerCandidates = 128
			admitted, err := validateM8BenchmarkWork(cfg, m, maxBenchmarkWorkUnits, maxFixtureBytes)
			if err != nil {
				t.Fatalf("finite width64/P2/EF96/C128 refused: %v", err)
			}
			total := admitted.AttributionDiagnosticWorkUnits + admitted.QualityDiagnosticWorkUnits + admitted.RouterPolicyDiagnosticWorkUnits
			if total > maxBenchmarkWorkUnits || admitted.ModeledPeakBytes > maxFixtureBytes {
				t.Fatalf("admitted above original work/memory caps: %+v", admitted)
			}
			t.Logf("C128 ADMITTED: attribution=%d quality=%d policy=%d combined=%d modeled_peak_bytes=%d exact_visits=%d", admitted.AttributionDiagnosticWorkUnits, admitted.QualityDiagnosticWorkUnits, admitted.RouterPolicyDiagnosticWorkUnits, total, admitted.ModeledPeakBytes, admitted.ExactWorkVectorVisits)
		})
	}
}

func TestM8RouterPolicyResourcePlanChargesEveryPopulationRecheck(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.probes = []int{1, 2}
	cfg.efSearch = []int{32}
	cfg.concurrency = []int{1}
	cfg.m8RouterPolicyRepresentativeCounts = []int{16}
	m := fixtureManifest{Vectors: 96, Queries: 7, Dimensions: 128}
	baseline, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name             string
		efs, concurrency []int
	}{
		{"EF", []int{32, 64, 96}, []int{1}},
		{"concurrency", []int{32}, []int{1, 2, 4, 8}},
		{"both", []int{32, 64, 96}, []int{1, 2, 4, 8}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expanded := cfg
			expanded.efSearch = tc.efs
			expanded.concurrency = tc.concurrency
			work, _, err := m8PlanRouterPolicyDiagnosticsV1(expanded, m, []int{4}, math.MaxInt64, math.MaxInt64)
			if err != nil {
				t.Fatal(err)
			}
			// Replaying each row checks all query and truth identities twice:
			// once explicitly and once in m8BuildAttributionV1. Count the
			// actual input bytes independently, excluding fixed hash headers.
			rowsExtra := int64(len(tc.efs)*len(tc.concurrency) - 1)
			rechecksExtra := 2 * int64(m.Queries) * int64(len(cfg.probes)) * rowsExtra
			bytesPerQuery := int64(4*m.Dimensions + cfg.topK*(8+documentIDStorageBytes+4))
			if work-baseline < rechecksExtra*bytesPerQuery {
				t.Fatalf("work=%d baseline=%d omitted at least %d cache-hit input bytes", work, baseline, rechecksExtra*bytesPerQuery)
			}
			if _, _, err := m8PlanRouterPolicyDiagnosticsV1(expanded, m, []int{4}, baseline, math.MaxInt64); err == nil {
				t.Fatal("larger replay population admitted by old work cap")
			}
		})
	}
}

func TestM8RouterPolicyResourcePlanChargesQueryScratchAndRejectsOverflow(t *testing.T) {
	cfg, err := parseConfig(append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.probes = []int{1}
	cfg.efSearch = []int{32}
	cfg.concurrency = []int{1}
	cfg.m8RouterPolicyRepresentativeCounts = []int{4}
	m := fixtureManifest{Vectors: 96, Queries: 2, Dimensions: 4}
	_, small, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	m.Dimensions = 4096
	_, large, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if large-small < int64(4*(4096-4)) {
		t.Fatal("dimension-sized conversion scratch omitted", small, large)
	}
	m.Queries = int(^uint(0) >> 1)
	if _, _, err := m8PlanRouterPolicyDiagnosticsV1(cfg, m, []int{4}, math.MaxInt64, math.MaxInt64); err == nil {
		t.Fatal("overflowing shape accepted")
	}
}
