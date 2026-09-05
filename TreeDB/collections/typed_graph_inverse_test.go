package collections

import (
	"fmt"
	"slices"
	"testing"
)

func BenchmarkTypedGraphInversePermutation(b *testing.B) {
	for _, n := range []int{4097, 50000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			rows := make([]columnVectorGraphAssetRow, n)
			for i := range rows {
				physical := (i * 7919) % n
				rows[i].BaseRowRef = DocumentRowRef{Generation: uint64(1 + physical/1000), PartID: 1, RowIndex: physical % 1000, AppliedCommandLSN: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				values, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow, rows, uint64(n))
				if err != nil || len(values) != n {
					b.Fatalf("permutation: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(n*8), "payload-B/build")
		})
	}
}

func TestTypedGraphInverseMappedAndOptional(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0}}, {id: "b", vector: []float32{0, 1}}, {id: "c", vector: []float32{1, 1}}}
	_, db, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	defer db.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, db, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	graph := graphManifestFromRecords1918(t, records, def)
	source, err := newColumnVectorGraphRowRefStateSourceFromRoot(db.ColumnAssetRootDir(), "docs", *cfg, def, graph, state, records)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if !source.inversePermutationActive() {
		t.Fatal("rebuilt graph missing inverse")
	}
	for ordinal := 0; ordinal < source.rows; ordinal++ {
		ref, ok := source.rowRefForOrdinal(ordinal)
		if !ok {
			t.Fatal("forward ref unavailable")
		}
		if got, ok := source.ordinalForPhysicalRow(ref); !ok || got != ordinal {
			t.Fatalf("inverse=%d/%v want %d", got, ok, ordinal)
		}
		for _, mutate := range []func(*DocumentRowRef){func(r *DocumentRowRef) { r.AppliedCommandLSN++ }, func(r *DocumentRowRef) { r.Generation += 100 }, func(r *DocumentRowRef) { r.PartID += 100 }, func(r *DocumentRowRef) { r.RowIndex += 100 }} {
			invalid := ref
			mutate(&invalid)
			if _, ok := source.ordinalForPhysicalRow(invalid); ok {
				t.Fatalf("foreign coordinate/version accepted: %+v", invalid)
			}
		}
	}
	for _, bad := range [][]int64{{0, 0, 0}, {-1, 1, 2}, {0, 1, 3}, {2, 1, 0}} {
		copy := *source
		copy.ordinalsByPhysicalRow.Values = slices.Clone(bad)
		// Ensure the unsorted case is actually the reverse of canonical order.
		if bad[0] == 2 && bad[2] == 0 {
			copy.ordinalsByPhysicalRow.Values = slices.Clone(source.ordinalsByPhysicalRow.Values)
			slices.Reverse(copy.ordinalsByPhysicalRow.Values)
		}
		if err := copy.validateInversePermutation(); err == nil {
			t.Fatalf("bad inverse accepted: %v", bad)
		}
	}
	ref, _ := source.rowRefForOrdinal(0)
	if allocs := testing.AllocsPerRun(100, func() {
		if _, ok := source.ordinalForPhysicalRow(ref); !ok {
			panic("lookup")
		}
	}); allocs != 0 {
		t.Fatalf("inverse lookup allocated: %g", allocs)
	}
	legacy := state
	legacy.Assets = slices.Clone(state.Assets)
	legacy.Assets = slices.DeleteFunc(legacy.Assets, func(asset columnVectorIndexStateAssetSnapshot) bool {
		return asset.AssetID == columnVectorGraphRowRefStateAssetID(columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow)
	})
	old, err := newColumnVectorGraphRowRefStateSourceFromRoot(db.ColumnAssetRootDir(), "docs", *cfg, def, graph, legacy, records)
	if err != nil {
		t.Fatalf("optional inverse broke base reader: %v", err)
	}
	defer old.Close()
	if !old.preparedViewActive() || old.inversePermutationActive() {
		t.Fatal("optional field confused base/inverse readiness")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := source.ordinalForPhysicalRow(ref); ok {
		t.Fatal("closed inverse remained usable")
	}
}

func TestTypedGraphInversePermutation(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{BaseRowRef: DocumentRowRef{Generation: 9, PartID: 7, RowIndex: 1 << 30, AppliedCommandLSN: 10}},
		{BaseRowRef: DocumentRowRef{Generation: 2, PartID: 3, RowIndex: 99, AppliedCommandLSN: 4}},
		{BaseRowRef: DocumentRowRef{Generation: 2, PartID: 3, RowIndex: 1, AppliedCommandLSN: 4}},
	}
	values, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateField("ordinal_by_physical_row"), rows, 9)
	if err != nil || !slices.Equal(values, []int64{2, 1, 0}) {
		t.Fatalf("sparse permutation=%v err=%v", values, err)
	}
	rows[0].BaseRowRef = rows[1].BaseRowRef
	if _, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateField("ordinal_by_physical_row"), rows, 9); err == nil {
		t.Fatal("duplicate physical coordinate accepted")
	}
}
