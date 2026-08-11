package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestLocalHNSWRepairConstructionCurveV1(t *testing.T) {
	if !localHNSWRepairConstructionCurvePointsValidV1([]int{128, 256, 512}) || localHNSWRepairConstructionCurvePointsValidV1([]int{128, 512}) {
		t.Fatal("construction points validation")
	}
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
	points, err := localHNSWRepairConstructionCurveV1Build(context.Background(), source, t.TempDir(), []int{ordinal}, [][]float32{m8Query32V1(query64)}, truth)
	if err != nil {
		t.Fatal(err)
	}
	for i, point := range points {
		variant, err := localHNSWRepairConstructionCurveVariantV1(point.EfConstruction)
		if err != nil || point.EfConstruction != localHNSWRepairConstructionCurvePointsV1[i] || point.Build.Variant != string(variant) || point.Build.PackBytes == 0 || point.Graph.CombinedReachableRows != point.Graph.Rows || point.Quality.EFSearch != 128 || point.Quality.QueryCount != 1 || point.PackMembershipSHA256 == "" || point.PackChecksumsSHA256 == "" || point.DefinitionDigest == "" {
			t.Fatalf("point[%d]=%+v variant=%s err=%v", i, point, variant, err)
		}
	}
	if _, err := localHNSWRepairConstructionCurveVariantV1(127); err == nil {
		t.Fatal("expected invalid construction point")
	}
	if err := run([]string{"local-hnsw-repair-construction-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
