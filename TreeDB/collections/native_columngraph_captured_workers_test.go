package collections

import (
	"reflect"
	"testing"
)

func TestNativeColumnGraphCapturedWorkersPreserveOrdinaryTopology(t *testing.T) {
	def, ids, vectors := nativeColumnGraphScratchFixture(t, 258, false)
	makeRows := func() []columnVectorGraphAssetRow {
		rows := make([]columnVectorGraphAssetRow, len(ids))
		for i := range rows {
			rows[i] = columnVectorGraphAssetRow{ID: ids[i], Vector: vectors[i]}
		}
		return rows
	}
	want := makeRows()
	if err := buildColumnVectorGraphAdjacency(want, def); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 2, 4, 16} {
		got := makeRows()
		if err := buildColumnVectorGraphAdjacencyWithWorkers(got, def, workers); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("captured workers%d changed maintained source order/adjacency", workers)
		}
	}
	for _, workers := range []int{-1, 0, 17} {
		if err := buildColumnVectorGraphAdjacencyWithWorkers(makeRows(), def, workers); err == nil {
			t.Fatalf("invalid captured workers%d", workers)
		}
	}
}

func TestNativeColumnGraphCapturedWorkerCapacity(t *testing.T) {
	def, _, _ := nativeColumnGraphScratchFixture(t, 0, false)
	walk := func(visit func([]byte) error) error {
		for i := 0; i < 4096; i++ {
			// This source is the original protected fixture ID grammar, with no
			// N-element planning corpus. IDs remain borrowed by the capacity helper.
			var id [7]byte
			id[0] = 'd'
			n := i
			for digit := 6; digit >= 1; digit-- {
				id[digit] = byte('0' + n%10)
				n /= 10
			}
			if err := visit(id[:]); err != nil {
				return err
			}
		}
		return nil
	}
	original, err := nativeColumnGraphConstructionCapacity(def, 4096, walk)
	if err != nil {
		t.Fatal(err)
	}
	var previous uint64
	for _, workers := range []int{1, 2, 4, 16} {
		p, err := nativeColumnGraphConstructionCapacityForWorkers(def, 4096, workers, walk)
		if err != nil {
			t.Fatal(err)
		}
		if p.Rows != original.Rows || p.IDBytes != original.IDBytes || p.LevelRows != original.LevelRows || p.CallerRowsBytes != original.CallerRowsBytes || p.NativeRowsBytes != original.NativeRowsBytes || p.MatrixBytes != original.MatrixBytes || p.PlanningBytes != original.PlanningBytes {
			t.Fatalf("workers%d changed batch/source/row geometry: %+v", workers, p)
		}
		if p.Workers != workers || p.PrivateBytes < previous || p.PrivateBytes >= original.PrivateBytes {
			t.Fatalf("invalid bound: %+v original%+v", p, original)
		}
		previous = p.PrivateBytes
		t.Logf("original4096 captured worker component: %+v", p)
	}
	for _, workers := range []int{-1, 17} {
		if _, err := nativeColumnGraphConstructionCapacityForWorkers(def, 4096, workers, walk); err == nil {
			t.Fatalf("invalid ceiling%d", workers)
		}
	}
}
