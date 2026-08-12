package main

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWRepairM18EFCurveV1(t *testing.T) {
	if !localHNSWRepairM18EFCurveDescriptorV1(localHNSWRepairM18EFCurveDescriptorSHA256V1) || localHNSWRepairM18EFCurveDescriptorV1(localHNSWAttributionDescriptorSHA256V1) {
		t.Fatal("descriptor lock")
	}
	if !slices.Equal(localHNSWRepairM18EFCurvePointsV1, []int{96, 112, 120, 128}) {
		t.Fatalf("points=%v", localHNSWRepairM18EFCurvePointsV1)
	}
	rejected := make([]localHNSWRepairEFCurveCellV1, len(localHNSWRepairM18EFCurvePointsV1))
	for i, ef := range localHNSWRepairM18EFCurvePointsV1 {
		rejected[i] = localHNSWRepairEFCurveCellV1{EFSearch: ef, QueryCount: 806, RoutingRecall: localHNSWAttributionRecallAggregateV1{Mean: 1}, P2Recall: localHNSWAttributionRecallAggregateV1{Mean: float64(7656) / 8060}, P16Recall: localHNSWAttributionRecallAggregateV1{Mean: float64(7656) / 8060}, P2HitSlots: 7656, P16HitSlots: 7656}
	}
	if got, err := localHNSWRepairM18EFCurveDispositionV1(rejected); err != nil || got != "no_point_passes" {
		t.Fatalf("rejected=%q err=%v", got, err)
	}
	rejected[2].P2HitSlots, rejected[2].P16HitSlots, rejected[2].P2Recall.Mean, rejected[2].P16Recall.Mean = 7657, 7650, float64(7657)/8060, float64(7650)/8060
	if got, err := localHNSWRepairM18EFCurveDispositionV1(rejected); err != nil || got != "smallest_point_passes_ef_120" {
		t.Fatalf("accepted=%q err=%v", got, err)
	}
	rejected[2].P16HitSlots, rejected[2].P16Recall.Mean = 7636, float64(7636)/8060
	if got, err := localHNSWRepairM18EFCurveDispositionV1(rejected); err != nil || got != "no_point_passes" {
		t.Fatalf("gap=%q err=%v", got, err)
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
	dir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	source, err = openM8ProductionExistingAssetSetV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	repair, build, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1, 4132001)
	if err != nil {
		t.Fatal(err)
	}
	defer repair.Close()
	ordinal := 0
	for !localHNSWCalibrationOrdinalV1(ordinal) {
		ordinal++
	}
	query := []float64{1, 1, 1}
	truth, err := m8ExactTruthV1(source.collection, source.manifest, [][]float64{query}, 10)
	if err != nil {
		t.Fatal(err)
	}
	cells, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, localHNSWRepairM18EFCurvePointsV1, []int{ordinal}, [][]float32{m8Query32V1(query)}, truth)
	if err != nil {
		t.Fatal(err)
	}
	if build.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || build.VariantIdentity == "" || len(cells) != 4 {
		t.Fatalf("build=%+v cells=%d", build, len(cells))
	}
	for i, cell := range cells {
		if cell.EFSearch != localHNSWRepairM18EFCurvePointsV1[i] || cell.QueryCount != 1 || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || !localHNSWAttributionSHA256V1(cell.P2ResultsSHA256) || !localHNSWAttributionSHA256V1(cell.P16ResultsSHA256) || cell.P2Work.Candidates == 0 || cell.P16Work.Candidates == 0 || len(cell.TerminationCount) == 0 {
			t.Fatalf("cell[%d]=%+v", i, cell)
		}
	}
	if err := run([]string{"local-hnsw-repair-m18-ef-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
