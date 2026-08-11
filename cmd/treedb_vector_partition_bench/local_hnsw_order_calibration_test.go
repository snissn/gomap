package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWOrderCalibrationQueryV1(t *testing.T) {
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
	control, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4107001)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	hash, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationStableIDHashV1, 4107002)
	if err != nil {
		t.Fatal(err)
	}
	defer hash.Close()
	ordinal := 0
	for !localHNSWCalibrationOrdinalV1(ordinal) {
		ordinal++
	}
	query64 := []float64{1, 1, 1}
	truth, err := m8ExactTruthV1(source.collection, source.manifest, [][]float64{query64}, 10)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := localHNSWOrderCalibrationQueryV1Build(context.Background(), source, control, hash, ordinal, m8Query32V1(query64), truth[0])
	if err != nil {
		t.Fatal(err)
	}
	if !localHNSWOrderCalibrationQueryV1Valid(evidence, 4) || evidence.Schema != localHNSWOrderCalibrationSchemaV1 || len(evidence.SourceSearches) != 4 || len(evidence.HashSearches) != 4 {
		t.Fatalf("evidence=%+v", evidence)
	}
	raw, err := json.Marshal(evidence)
	if err != nil || !strings.Contains(string(raw), `"source_order"`) || !strings.Contains(string(raw), `"stable_id_hash_order"`) || strings.Contains(string(raw), `"overlay_current"`) || strings.Contains(string(raw), `"auxiliary_navigation"`) {
		t.Fatalf("encoded evidence=%s err=%v", raw, err)
	}
	if err := run([]string{"local-hnsw-order-calibration"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}

func TestLocalHNSWOrderCalibrationGraphV1Valid(t *testing.T) {
	source := localHNSWRepairCalibrationGraphV1{Rows: 300000, NativeReachableRows: 299968, CombinedReachableRows: 300000, NativeTraversalRoots: 48, AuxiliaryEdges: 64, AuxiliaryCSRBytes: 2400384, AuxiliaryMaxDegree: 4}
	if !localHNSWOrderCalibrationGraphV1Valid(source, true) {
		t.Fatal("source-lock graph rejected")
	}
	candidate := source
	candidate.NativeReachableRows, candidate.NativeTraversalRoots, candidate.AuxiliaryEdges, candidate.AuxiliaryCSRBytes, candidate.AuxiliaryMaxDegree = 299900, 64, 96, 2400512, 4
	if !localHNSWOrderCalibrationGraphV1Valid(candidate, false) {
		t.Fatalf("candidate graph rejected: %+v", candidate)
	}
	candidate.AuxiliaryEdges--
	if localHNSWOrderCalibrationGraphV1Valid(candidate, false) {
		t.Fatalf("invalid candidate graph accepted: %+v", candidate)
	}
	candidate.NativeReachableRows, candidate.NativeTraversalRoots, candidate.AuxiliaryEdges, candidate.AuxiliaryCSRBytes, candidate.AuxiliaryMaxDegree = candidate.Rows, 16, 0, 2400128, 0
	if !localHNSWOrderCalibrationGraphV1Valid(candidate, false) {
		t.Fatalf("connected candidate graph rejected: %+v", candidate)
	}
}
