package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWRepairCalibrationQueryV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sourceDir)
	source, err = openM8ProductionExistingAssetSetV1(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	overlay, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 4106003)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()
	repair, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4106004)
	if err != nil {
		t.Fatal(err)
	}
	defer repair.Close()
	ordinal := 0
	for !localHNSWCalibrationOrdinalV1(ordinal) {
		ordinal++
	}
	query64 := []float64{1, 1, 1}
	truth, err := m8ExactTruthV1(source.collection, source.manifest, [][]float64{query64}, 10)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := localHNSWRepairCalibrationQueryV1Build(context.Background(), source, overlay, repair, ordinal, m8Query32V1(query64), truth[0])
	if err != nil {
		t.Fatal(err)
	}
	if !localHNSWRepairCalibrationQueryV1Valid(evidence, 4) || len(evidence.OverlaySearches) != 4 || len(evidence.RepairSearches) != 4 || evidence.Overlay.P2Recall < 0 || evidence.Repair.P16Recall < 0 {
		t.Fatalf("evidence=%+v", evidence)
	}
	for _, search := range evidence.OverlaySearches {
		if search.AuxiliaryEdges != 0 || search.AuxiliaryCandidates != 0 || search.AuxiliaryAdmissions != 0 {
			t.Fatalf("overlay counters=%+v", search)
		}
	}
	for _, test := range []struct {
		name    string
		harness *localHNSWVariantHarnessV1
		variant string
		all     bool
	}{
		{"overlay-p2", overlay, "overlay_current", false},
		{"repair-p16", repair, "auxiliary_navigation", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			route := evidence.P2Route
			if test.all {
				route = evidence.P16Route
			}
			found, _, err := localHNSWRepairCalibrationOrdinarySearchV1(t.Context(), test.harness, route, evidence.Query, test.variant, evidence.QueryFP32SHA256)
			if err != nil {
				t.Fatal(err)
			}
			want, err := localHNSWRepairCalibrationFrozenResultSHA256V1(evidence, test.variant, test.all)
			if err != nil || localHNSWRepairCalibrationResultsSHA256V1(found) != want {
				t.Fatalf("ordinary result=(%q,%v), want %q", localHNSWRepairCalibrationResultsSHA256V1(found), err, want)
			}
		})
	}
	if err := run([]string{"local-hnsw-repair-calibration"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}

func TestLocalHNSWRepairCalibrationDispositionV1(t *testing.T) {
	for _, test := range []struct {
		name, want       string
		p2, p16, routing float64
	}{
		{"recall", "blocker_activate_4107_recall_below_0_9500", .949, .952, .999},
		{"absolute-gap", "blocker_calibration_p2_p16_or_routing_guardrail", .951, .948, .999},
		{"eligible", "recall_eligible_stop_before_distributed_qualification", .951, .952, .998},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := localHNSWRepairCalibrationDispositionV1(test.p2, test.p16, test.routing)
			if err != nil || got != test.want {
				t.Fatalf("disposition=(%q,%v), want %q", got, err, test.want)
			}
		})
	}
}
