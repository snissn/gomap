package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWRepairEFCurveV1(t *testing.T) {
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
	repair, _, err := localHNSWAttributionBuildVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1, 4106005)
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
	cells, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, localHNSWRepairEFCurvePointsV1, []int{ordinal}, [][]float32{m8Query32V1(query64)}, truth)
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 4 || !localHNSWRepairEFCurvePointsValidV1(localHNSWRepairEFCurvePointsV1) || cells[1].EFSearch != 128 {
		t.Fatalf("cells=%+v", cells)
	}
	for i, cell := range cells {
		if cell.EFSearch != localHNSWRepairEFCurvePointsV1[i] || cell.QueryCount != 1 || !localHNSWAttributionSHA256V1(cell.RoutesSHA256) || cell.RoutesSHA256 != cells[0].RoutesSHA256 || cell.P2Work.Candidates == 0 || cell.P2Work.NativeEdges == 0 || cell.P16Work.Candidates == 0 || cell.P16Work.NativeEdges == 0 || len(cell.TerminationCount) == 0 {
			t.Fatalf("cell[%d]=%+v", i, cell)
		}
	}
	if _, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, []int{64, 512, 128, 4096}, []int{ordinal}, [][]float32{m8Query32V1(query64)}, truth); err == nil {
		t.Fatal("expected malformed EF points rejection")
	}
	if err := run([]string{"local-hnsw-repair-ef-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
