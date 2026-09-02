package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWRepairEFCurveV1(t *testing.T) {
	for _, test := range []struct {
		points []int
		valid  bool
	}{
		{localHNSWRepairEFCurvePointsV1, true},
		{[]int{64, 128, 256}, true},
		{[]int{128, 64}, false},
		{[]int{64, 64}, false},
		{[]int{9}, false},
		{[]int{4097}, false},
	} {
		if got := localHNSWRepairEFCurvePointsValidV1(test.points); got != test.valid {
			t.Fatalf("points=%v valid=%v", test.points, got)
		}
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
	timing, err := localHNSWRepairEFCurveTimingV1Build(context.Background(), source, repair, []int{ordinal}, [][]float32{m8Query32V1(query64)})
	if err != nil {
		t.Fatal(err)
	}
	if timing.Variant != localHNSWRepairEFCurveTimingVariantV1 || len(timing.Cells) != 16 || timing.RoutesSHA256 != cells[0].RoutesSHA256 || (timing.Gate.Disposition != "calibration_timing_gate_pass" && timing.Gate.Disposition != "calibration_timing_gate_fail") || timing.Gate.P2QPS148Over128 <= 0 || timing.Gate.P16QPS148Over128 <= 0 || timing.Gate.P2P95148Over128 <= 0 || timing.Gate.P16P95148Over128 <= 0 {
		t.Fatalf("timing=%+v", timing)
	}
	for _, cell := range timing.Cells {
		if cell.QueryCount != 1 || cell.EFSearch != 128 && cell.EFSearch != 148 || cell.Probes != 2 && cell.Probes != len(repair.searchers) || cell.QPS <= 0 || cell.P50Nanos == 0 || cell.P95Nanos == 0 || cell.P99Nanos == 0 || cell.Candidates == 0 || cell.NativeEdges == 0 || len(cell.ResultSHA256) != 1 {
			t.Fatalf("timing cell=%+v", cell)
		}
	}
	badTiming := append([]localHNSWRepairEFCurveTimingCellV1(nil), timing.Cells...)
	badTiming[4].ResultSHA256 = []string{strings.Repeat("0", 64)}
	if _, err := localHNSWRepairEFCurveTimingGateV1Build(badTiming, 4); err == nil {
		t.Fatal("expected timing result drift rejection")
	}
	if _, err := localHNSWRepairEFCurveV1Build(context.Background(), source, repair, []int{64, 512, 128, 4096}, []int{ordinal}, [][]float32{m8Query32V1(query64)}, truth); err == nil {
		t.Fatal("expected malformed EF points rejection")
	}
	if err := run([]string{"local-hnsw-repair-ef-curve"}, &strings.Builder{}); err == nil {
		t.Fatal("expected missing frozen inputs rejection")
	}
}
