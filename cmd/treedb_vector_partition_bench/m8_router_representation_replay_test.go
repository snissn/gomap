package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/snissn/gomap/TreeDB/collections"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM8RouterRepresentationReplayBindsSourceGeometryAndFailurePopulation(t *testing.T) {
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
		o            collections.VectorPartitionRouterRepresentationOptionsV1
		legacyBudget int
	}{
		{"multilevel", collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "multilevel", ReturnedWidth: 4, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}, 0},
		{"geometry", collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 4, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}, 0},
		{"policy-failure", collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 1, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}, 0},
		{"ordinary-failure", collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 1, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}, 1},
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
			if err := h.enableRouterRepresentationV1(t.Context(), tc.o); err != nil {
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
			cfg := m8ProductionConfigEvidenceV1{TopK: 10, Partitions: 4, DomainCount: 4, PacksPerDomain: []int{1, 1, 1, 1}, RouterCandidates: budget, QualityDiagnostics: true, RouterPolicyDiagnostics: true, RouterPolicyWidth: tc.o.ReturnedWidth, RouterRepresentation: &tc.o}
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
				"model": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Info.Models[1].ModelSHA256 = strings.Repeat("a", 64)
					for i := range r.Rows[0].Attribution.RouterRepresentation.Queries {
						r.Rows[0].Attribution.RouterRepresentation.Queries[i].Arms[1].Comparison.ModelSHA256 = strings.Repeat("a", 64)
					}
				},
				"geometry": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Info.Models[1].Geometry = "invented"
				},
				"source": func(r *m8ProductionReportV1) { r.Rows[0].Attribution.RouterRepresentation.Info.SourceChecksum++ },
				"quota":  func(r *m8ProductionReportV1) { r.Rows[0].Attribution.RouterRepresentation.Info.Quotas[0].Tokens++ },
				"seed":   func(r *m8ProductionReportV1) { r.Rows[0].Attribution.RouterRepresentation.Info.ClusteringConfig.Seed++ },
				"apportionment": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Info.QuotaMethod = "different_method"
				},
				"radius": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Info.Models[1].RepresentedNodes[0].Radius += .01
				},
				"member-score": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Queries[0].TruthDomains[0].NearestMemberScore -= .01
				},
				"population": func(r *m8ProductionReportV1) {
					r.Rows[0].Attribution.RouterRepresentation.Queries = r.Rows[0].Attribution.RouterRepresentation.Queries[:1]
				},
				"unselected": func(r *m8ProductionReportV1) { r.Config.RouterRepresentation = nil },
				"missing":    func(r *m8ProductionReportV1) { r.Rows[0].Attribution.RouterRepresentation = nil },
			} {
				t.Run(name, func(t *testing.T) {
					var altered m8ProductionReportV1
					if err := json.Unmarshal(raw, &altered); err != nil {
						t.Fatal(err)
					}
					mutate(&altered)
					if name == "model" || name == "source" || name == "radius" || name == "member-score" || name == "seed" {
						if err := m8RouterRepresentationEvidenceSelectionV1(altered.Config, altered.Rows[0]); err != nil {
							t.Fatal("mutation must pass structural validation", err)
						}
					}
					if err := m8QualificationRetainedAttributionV1(root, altered, truth, transcript); err == nil {
						t.Fatal("forged retained representation accepted")
					}
				})
			}
		})
	}
}

func TestM8RouterRepresentationCacheCancellationAndChangedPopulation(t *testing.T) {
	h, q, truth, homes, members := qualityFixtureV1(t, 0)
	budget := int(h.assets.status.Representatives)
	if err := h.enableRouterPoliciesV1(2, budget); err != nil {
		t.Fatal(err)
	}
	o := collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "centroid_geometry", ReturnedWidth: 2, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.enableRouterRepresentationV1(ctx, o); !errors.Is(err, context.Canceled) || h.representation != nil {
		t.Fatal("canceled builder published cache", err)
	}
	if err := h.enableRouterRepresentationV1(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	oracle, err := h.membershipOraclesV1(truth, homes, members, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, q, truth, oracle, 1, 32, 10, budget, make([][]m8CanonicalResultV1, len(q)), h); err != nil {
		t.Fatal(err)
	}
	if _, err := h.routerRepresentationEvidenceV1(ctx, q, truth, 1, budget); !errors.Is(err, context.Canceled) {
		t.Fatal("cached diagnostics skipped cancellation", err)
	}
	q[0][0] += .01
	if _, err := h.routerRepresentationEvidenceV1(t.Context(), q, truth, 1, budget); err == nil {
		t.Fatal("changed population reused models/results")
	}
}
func TestM8RouterRepresentationPreflightSeparatesBuildAndQueryWork(t *testing.T) {
	cfg, err := parseConfig(representationCLIArgsV1(t))
	if err != nil {
		t.Fatal(err)
	}
	f := fixtureManifest{Vectors: 96, Queries: 3, Dimensions: 8}
	cfg.m8RouterPolicyRepresentativeCounts = []int{4}
	work, bytes, build, err := m8PlanRouterRepresentationV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || work < 1 || bytes < 1 || build != 200000000 {
		t.Fatal(work, bytes, build, err)
	}
	cfg.efSearch = []int{16, 32, 64}
	cfg.concurrency = []int{1, 2, 4}
	w, b, build2, err := m8PlanRouterRepresentationV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || w <= work || b <= bytes || build2 != build {
		t.Fatal("cached serialization uncharged or model rebuilt per query", err)
	}
	if _, _, _, err := m8PlanRouterRepresentationV1(cfg, f, []int{4}, w-1, math.MaxInt64); err == nil {
		t.Fatal("query work guard bypass")
	}
	if _, _, _, err := m8PlanRouterRepresentationV1(cfg, f, []int{4}, math.MaxInt64, b-1); err == nil {
		t.Fatal("memory guard bypass")
	}
	cfg.m8RouterRepresentation.Arm = "centroid_geometry"
	w2, b2, _, err := m8PlanRouterRepresentationV1(cfg, f, []int{4}, math.MaxInt64, math.MaxInt64)
	if err != nil || w2 <= w || b2 <= b {
		t.Fatal("third arm uncharged", err)
	}
	cfg.m8RouterPolicyRepresentativeCounts = []int{4, 4}
	_, _, build3, err := m8PlanRouterRepresentationV1(cfg, f, []int{4, 4}, math.MaxInt64, math.MaxInt64)
	if err != nil || build3 != 2*build {
		t.Fatal("serial variant build envelope uncharged", err)
	}
}
