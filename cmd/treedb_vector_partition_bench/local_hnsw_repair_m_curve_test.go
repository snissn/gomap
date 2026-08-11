package main

import (
	"context"
	"encoding/json"
	"os"
	"slices"
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
		if point.M != want.M || point.EfConstruction != 256 || point.Build.Variant != string(want.Variant) || point.Build.PackBytes == 0 || point.Graph.CombinedReachableRows != point.Graph.Rows || point.Quality.EFSearch != 128 || point.Quality.QueryCount != 1 || point.PackMembershipSHA256 == "" || point.PackChecksumsSHA256 == "" || point.DefinitionDigest == "" || point.Quality.P2HitSlots > 10 || point.Quality.P16HitSlots > 10 || point.Quality.RoutingMissSlots > 10 {
			t.Fatalf("point[%d]=%+v", i, point)
		}
	}
	if got := []int{points[0].M, points[1].M, points[2].M, points[3].M, points[4].M}; !slices.Equal(got, []int{16, 18, 20, 22, 24}) {
		t.Fatalf("M point order=%v", got)
	}
	encoded, err := json.Marshal(points[0])
	if err != nil || !strings.Contains(string(encoded), `"m":16`) {
		t.Fatalf("M cell schema: %s err=%v", encoded, err)
	}
	if disposition, err := localHNSWRepairMCurveDispositionV1(points); err != nil || (disposition != "no_point_passes_local_quality" && !strings.HasPrefix(disposition, "local_quality_crossed_smallest_m_")) {
		t.Fatalf("disposition=%q err=%v", disposition, err)
	}
	failed := append([]localHNSWRepairMCurveCellV1(nil), points...)
	for i := range failed {
		failed[i].Quality.P2Recall.Mean = .949
		failed[i].Quality.P16Recall.Mean = .949
	}
	if disposition, err := localHNSWRepairMCurveDispositionV1(failed); err != nil || disposition != "no_point_passes_local_quality" {
		t.Fatalf("failed disposition=%q err=%v", disposition, err)
	}
	accepted := append([]localHNSWRepairMCurveCellV1(nil), points...)
	accepted[0].Quality.P2Recall.Mean = .95
	accepted[0].Quality.P16Recall.Mean = .90
	accepted[0].Quality.P2HitSlots = 10
	accepted[0].Quality.P16HitSlots = 0
	accepted[0].Quality.RoutingMissSlots = 20
	if disposition, err := localHNSWRepairMCurveDispositionV1(accepted); err != nil || disposition != "local_quality_crossed_smallest_m_16" {
		t.Fatalf("count-preserving disposition=%q err=%v", disposition, err)
	}
	accepted[0].Quality.RoutingMissSlots = 21
	if disposition, err := localHNSWRepairMCurveDispositionV1(accepted); err != nil || disposition != "no_point_passes_local_quality" {
		t.Fatalf("routing-slot disposition=%q err=%v", disposition, err)
	}
	accepted[0].Quality.RoutingMissSlots = 20
	accepted[0].Quality.P2HitSlots, accepted[0].Quality.P16HitSlots = 30, 51
	if disposition, err := localHNSWRepairMCurveDispositionV1(accepted); err != nil || disposition != "no_point_passes_local_quality" {
		t.Fatalf("hit-slot disposition=%q err=%v", disposition, err)
	}
	if err := run([]string{"local-hnsw-repair-m-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
