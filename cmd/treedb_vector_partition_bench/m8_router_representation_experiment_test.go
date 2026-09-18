package main

import (
	"github.com/snissn/gomap/TreeDB/collections"
	"reflect"
	"slices"
	"testing"
)

func representationCLIArgsV1(t *testing.T) []string {
	return append(qualityCLIArgsV1(t), "-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "2", "-m8-router-representation-arm", "multilevel", "-m8-router-representation-width", "2", "-m8-router-representation-max-build-work", "200000000", "-m8-router-representation-max-build-bytes", "67108864")
}
func TestM8RouterRepresentationExplicitSelectionAndChild(t *testing.T) {
	args := representationCLIArgsV1(t)
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.m8RouterRepresentation.Arm != "multilevel" {
		t.Fatal("missing selection")
	}
	r := m8QualificationCommandConfigV1(cfg)
	if r.RouterRepresentation == nil || *r.RouterRepresentation != cfg.m8RouterRepresentation {
		t.Fatal("command not bound")
	}
	child, err := m8VariantProcessArgsV1(append([]string{"bench"}, args...), t.TempDir(), 0, "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	again, err := parseConfig(child)
	if err != nil || again.m8RouterRepresentation != cfg.m8RouterRepresentation {
		t.Fatal("child lost coordinates", err)
	}
	for _, bad := range [][]string{{"-m8-router-representation-width", "0"}, {"-m8-router-representation-max-build-work", "0"}, {"-m8-router-representation-arm", "bogus"}, {"-m8-router-policy-diagnostics=false"}} {
		if _, err := parseConfig(append(slices.Clone(args), bad...)); err == nil {
			t.Fatal("invalid selection accepted", bad)
		}
	}
	ordinary, err := parseConfig(qualityCLIArgsV1(t))
	if err != nil || m8QualificationCommandConfigV1(ordinary).RouterRepresentation != nil {
		t.Fatal("default changed", err)
	}
}
func TestM8RouterRepresentationNeverDropsUnderfilledArms(t *testing.T) {
	h, q, truth, homes, members := qualityFixtureV1(t, 0)
	budget := int(h.assets.status.Representatives)
	if err := h.enableRouterPoliciesV1(budget, budget); err != nil {
		t.Fatal(err)
	}
	o := collections.VectorPartitionRouterRepresentationOptionsV1{Arm: "multilevel", ReturnedWidth: budget, MaxBuildWork: 200000000, MaxBuildBytes: 64 << 20}
	if err := h.enableRouterRepresentationV1(t.Context(), o); err != nil {
		t.Fatal(err)
	}
	oracle, err := h.membershipOraclesV1(truth, homes, members, 2)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := m8BuildAttributionV1(t.Context(), h.assets, homes, members, q, truth, oracle, 2, 32, 10, budget, make([][]m8CanonicalResultV1, len(q)), h)
	if err != nil {
		t.Fatal(err)
	}
	e := cell.Evidence.RouterRepresentation
	if e == nil || len(e.Queries) != len(q) {
		t.Fatal("lost population")
	}
	for _, q := range e.Queries {
		if len(q.Arms) != 2 || q.Arms[0].Comparison.Status != "pass" {
			t.Fatal("lost control")
		}
	}
	row := m8ProductionRowV1{Status: "pass", Probes: 2, Samples: len(q), Attribution: cell.Evidence}
	cfg := m8ProductionConfigEvidenceV1{TopK: 10, QualityDiagnostics: true, RouterPolicyDiagnostics: true, RouterPolicyWidth: budget, RouterCandidates: budget, RouterRepresentation: &o}
	if err := m8RouterRepresentationEvidenceSelectionV1(cfg, row); err != nil {
		t.Fatal(err)
	}
	again, err := h.routerRepresentationEvidenceV1(t.Context(), q, truth, 2, budget)
	if err != nil || !reflect.DeepEqual(e, again) {
		t.Fatal("cache changed", err)
	}
}
