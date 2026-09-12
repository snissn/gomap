package collections

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
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
		for _, mutate := range []func(*DocumentRowRef){func(r *DocumentRowRef) { r.AppliedCommandLSN++ }, func(r *DocumentRowRef) { r.Generation += 100 }, func(r *DocumentRowRef) { r.PartID += 100 }, func(r *DocumentRowRef) { r.RowIndex += 100 }, func(r *DocumentRowRef) { r.Generation = ^uint64(0) }, func(r *DocumentRowRef) { r.PartID = ^uint64(0) }, func(r *DocumentRowRef) { r.RowIndex = -1 }} {
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
	t.Run("pack_forward_with_persisted_inverse", func(t *testing.T) {
		pack, _, _, err := col.openColumnHNSWSearchPackPreparedViewForReader("docs", *cfg, def, graph, state)
		if err != nil {
			t.Fatal(err)
		}
		defer pack.Close()
		omitted := state
		omitted.Assets = slices.DeleteFunc(slices.Clone(state.Assets), func(a columnVectorIndexStateAssetSnapshot) bool {
			return a.Role == columnVectorIndexStateAssetRoleRowRefs && a.AssetID != columnVectorGraphRowRefStateAssetID(columnVectorGraphRowRefStateFieldOrdinalByPhysicalRow)
		})
		open := func(pack *columnHNSWSearchPackPreparedView) (*columnVectorGraphRowRefStateSource, error) {
			return newColumnVectorGraphRowRefStateSourceFromRootWithPack(db.ColumnAssetRootDir(), "docs", *cfg, def, graph, omitted, records, pack)
		}
		borrowed, err := open(pack)
		if err != nil {
			t.Fatal(err)
		}
		defer borrowed.Close()
		for ordinal := 0; ordinal < source.rows; ordinal++ {
			ref, _ := source.rowRefForOrdinal(ordinal)
			if got, ok := borrowed.ordinalForPhysicalRow(ref); !ok || got != ordinal {
				t.Fatalf("pack inverse=%d/%v want=%d", got, ok, ordinal)
			}
		}
		if borrowed.baseMmapDirectFieldCount() != 0 || borrowed.generations.Alive() {
			t.Fatal("borrowed pack reported TCIM forward views")
		}
		if allocs := testing.AllocsPerRun(100, func() {
			if _, ok := borrowed.ordinalForPhysicalRow(ref); !ok {
				panic("pack inverse")
			}
		}); allocs != 0 {
			t.Fatalf("pack inverse allocated %g", allocs)
		}
		for _, bad := range [][]int64{{0, 0, 0}, {-1, 1, 2}, {0, 1, 3}} {
			copy := *borrowed
			copy.ordinalsByPhysicalRow.Values = slices.Clone(bad)
			if err := copy.validateInversePermutation(); err == nil {
				t.Fatalf("pack accepted inverse %v", bad)
			}
		}
		asset, raw, decoded := loadColumnHNSWSearchPackForTest2313(t, db, def, graph, state)
		for _, tc := range []struct {
			name       string
			kind       columnHNSWSearchPackSectionKind
			value      uint64
			lookupOnly bool
		}{
			{"absent_part", columnHNSWSearchPackSectionRowRefPartID, ref.PartID + 100, false},
			{"row_bounds", columnHNSWSearchPackSectionRowRefRowIndex, 1<<63 - 1, false},
			{"newer_generation", columnHNSWSearchPackSectionRowRefGeneration, graph.BaseManifestGeneration + 1, false},
			{"wrong_lsn", columnHNSWSearchPackSectionRowRefAppliedLSN, ref.AppliedCommandLSN + 1, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				changed := slices.Clone(raw)
				section := mustColumnHNSWSearchPackSectionForTest4429(t, decoded.Sections, tc.kind, 0)
				binary.LittleEndian.PutUint64(changed[section.Offset:], tc.value)
				rewriteColumnHNSWSearchPackChecksumsForTest4429(t, changed, asset.Ref, decoded.Sections)
				candidate, _, err := testColumnHNSWSearchPackPreparedViewFromBytesAllowErr2314(changed, mappedresource.SourceHeapCopy, columnHNSWSearchPackBaseIdentity{ManifestGeneration: graph.BaseManifestGeneration, ManifestChecksum: graph.BaseManifestChecksum, SchemaHash: graph.BaseSchemaHash})
				if err != nil {
					if tc.name != "newer_generation" {
						t.Fatal(err)
					}
					return
				}
				defer candidate.Close()
				s, err := open(candidate)
				if tc.lookupOnly {
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					if _, ok := s.ordinalForPhysicalRow(ref); ok {
						t.Fatal("accepted different LSN")
					}
				} else if err == nil {
					s.Close()
					t.Fatal("accepted invalid captured coordinate")
				}
			})
		}
		if err := borrowed.ordinalsByPhysicalRow.Close(); err != nil {
			t.Fatal(err)
		}
		if borrowed.preparedViewActive() || borrowed.inversePermutationActive() {
			t.Fatal("closed required inverse remained active")
		}
		if _, ok := borrowed.rowRefForOrdinal(0); ok {
			t.Fatal("forward provider survived required inverse release")
		}
		if !pack.metadataAlive() {
			t.Fatal("inverse close released borrowed pack")
		}
	})
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

func BenchmarkTypedGraphInverseLookup(b *testing.B) {
	rows := make([]columnGraphRebuildInputRowV2A, 4097)
	for i := range rows {
		rows[i] = columnGraphRebuildInputRowV2A{id: fmt.Sprintf("doc-%05d", (i*7919)%len(rows)), vector: []float32{1, float32(i + 1)}}
	}
	_, db, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, 2, 2, rows)
	defer db.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, db, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	graph := graphManifestFromRecords1918(b, records, def)
	source, err := newColumnVectorGraphRowRefStateSourceFromRoot(db.ColumnAssetRootDir(), "docs", *cfg, def, graph, state, records)
	if err != nil {
		b.Fatal(err)
	}
	defer source.Close()
	refs := make([]DocumentRowRef, source.rows)
	for i := range refs {
		var ok bool
		refs[i], ok = source.rowRefForOrdinal(i)
		if !ok {
			b.Fatal("missing forward row")
		}
	}
	for _, mode := range []string{"hit", "missing", "stale_lsn"} {
		b.Run(mode, func(b *testing.B) {
			queries := slices.Clone(refs)
			for i := range queries {
				if mode == "missing" {
					queries[i].Generation += 100
				} else if mode == "stale_lsn" {
					queries[i].AppliedCommandLSN++
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := (i * 7919) % len(queries)
				ordinal, ok := source.ordinalForPhysicalRow(queries[row])
				if ok != (mode == "hit") || (ok && ordinal != row) {
					b.Fatal("inverse result mismatch")
				}
			}
		})
	}
}
