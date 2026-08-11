package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestLocalHNSWRepairMCurveV1(t *testing.T) {
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
	ordinal := 0
	for !localHNSWCalibrationOrdinalV1(ordinal) {
		ordinal++
	}
	query64 := []float64{1, 1, 1}
	truth, err := m8ExactTruthV1(source.collection, source.manifest, [][]float64{query64}, 10)
	if err != nil {
		t.Fatal(err)
	}
	points, err := localHNSWRepairMCurveV1Build(context.Background(), source, t.TempDir(), []int{ordinal}, [][]float32{m8Query32V1(query64)}, truth)
	if err != nil {
		t.Fatal(err)
	}
	for i, point := range points {
		want := localHNSWRepairMCurvePointsV1[i]
		if point.M != want.M || point.EfConstruction != 256 || point.Build.Variant != string(want.Variant) || point.Build.PackBytes == 0 || point.Graph.CombinedReachableRows != point.Graph.Rows || point.Quality.EFSearch != 128 || point.Quality.QueryCount != 1 || point.PackMembershipSHA256 == "" || point.PackChecksumsSHA256 == "" || point.DefinitionDigest == "" {
			t.Fatalf("point[%d]=%+v", i, point)
		}
	}
	if disposition, err := localHNSWRepairMCurveDispositionV1(points); err != nil || (disposition != "no_point_passes_local_quality" && !strings.HasPrefix(disposition, "smallest_m_passes_local_quality_m_")) {
		t.Fatalf("disposition=%q err=%v", disposition, err)
	}
	failed := append([]localHNSWRepairConstructionCurveCellV1(nil), points...)
	failed[0].Quality.P2Recall.Mean = .949
	failed[0].Quality.P16Recall.Mean = .949
	failed[1].Quality.P2Recall.Mean = .951
	failed[1].Quality.P16Recall.Mean = .954
	failed[2].Quality.P2Recall.Mean = .949
	failed[2].Quality.P16Recall.Mean = .949
	if disposition, err := localHNSWRepairMCurveDispositionV1(failed); err != nil || disposition != "no_point_passes_local_quality" {
		t.Fatalf("failed disposition=%q err=%v", disposition, err)
	}
	if err := run([]string{"local-hnsw-repair-m-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
