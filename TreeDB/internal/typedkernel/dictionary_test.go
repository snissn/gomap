package typedkernel_test

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typedkernel"
)

func TestDictionaryCodePredicateHelpersSelectionParity(t *testing.T) {
	codes := []uint32{0, 2, 1, 2, 3, 1, 4, 2, 0, 3, 2, 1, 4, 4, 2, 0}
	builder := typedcolumn.NewGranuleBuilder(typedcolumn.Config{Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionNone})
	granule, err := builder.BuildUint32Codes(codes, 5)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	selections := map[string]typedcolumn.RowSelection{
		"all":    mustSelection(typedcolumn.NewAllRowSelection(len(codes))),
		"empty":  mustSelection(typedcolumn.NewEmptyRowSelection(len(codes))),
		"range":  mustSelection(typedcolumn.NewRangeRowSelection(len(codes), 2, 13)),
		"ranges": mustSelection(typedcolumn.NewRangesRowSelection(len(codes), []typedcolumn.RowRange{{Start: 0, End: 3}, {Start: 7, End: 12}})),
		"bitmap": mustSelection(typedcolumn.NewBitmapRowSelection(len(codes), []uint64{(1 << 0) | (1 << 2) | (1 << 7) | (1 << 14)})),
		"sparse": mustSelection(typedcolumn.NewSparseRowSelection(len(codes), []int{1, 4, 5, 9, 15})),
	}
	var scratch typedkernel.Scratch
	var reader typedcolumn.GranuleReader
	for shape, selection := range selections {
		t.Run(shape+"/eq", func(t *testing.T) {
			req := typedkernel.DictionaryPredicateRequest{Rows: len(codes), Selection: selection, Granule: granule, HasGranule: true, Reader: &reader, Code: 2}
			wantRows := expectedDictionaryRows(codes, selection, map[uint32]struct{}{2: {}})
			gotCount, err := typedkernel.CountDictionaryCode(req)
			if err != nil {
				t.Fatalf("CountDictionaryCode: %v", err)
			}
			if gotCount != len(wantRows) {
				t.Fatalf("CountDictionaryCode=%d want %d", gotCount, len(wantRows))
			}
			gotSelection, err := typedkernel.SelectDictionaryCode(req, &scratch)
			if err != nil {
				t.Fatalf("SelectDictionaryCode: %v", err)
			}
			if gotRows := gotSelection.AppendRows(nil); !equalCodeRowsKernel(gotRows, wantRows) {
				t.Fatalf("SelectDictionaryCode rows=%v want %v", gotRows, wantRows)
			}
		})
		t.Run(shape+"/in", func(t *testing.T) {
			req := typedkernel.DictionaryPredicateRequest{Rows: len(codes), Selection: selection, Granule: granule, HasGranule: true, Reader: &reader, Codes: []uint32{1, 4, 99}}
			wantRows := expectedDictionaryRows(codes, selection, map[uint32]struct{}{1: {}, 4: {}, 99: {}})
			gotCount, err := typedkernel.CountDictionaryCodesIn(req, &scratch)
			if err != nil {
				t.Fatalf("CountDictionaryCodesIn: %v", err)
			}
			if gotCount != len(wantRows) {
				t.Fatalf("CountDictionaryCodesIn=%d want %d", gotCount, len(wantRows))
			}
			gotSelection, err := typedkernel.SelectDictionaryCodesIn(req, &scratch)
			if err != nil {
				t.Fatalf("SelectDictionaryCodesIn: %v", err)
			}
			if gotRows := gotSelection.AppendRows(nil); !equalCodeRowsKernel(gotRows, wantRows) {
				t.Fatalf("SelectDictionaryCodesIn rows=%v want %v", gotRows, wantRows)
			}
		})
	}
}

func TestDictionaryRegistryFallbackStance(t *testing.T) {
	reg := typedkernel.DefaultRegistry()
	sem := dictionarySemantic(false, "")
	layout := dictionaryLayout(false, "")
	for _, op := range []typedkernel.AggregateOp{typedkernel.OpDictionaryGroupBy, typedkernel.OpDictionaryCount, typedkernel.OpDictionaryCountDistinct} {
		t.Run(string(op), func(t *testing.T) {
			_, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: op, Semantic: sem, Layout: layout})
			if err == nil || !strings.Contains(err.Error(), "no kernel registered") {
				t.Fatalf("dictionary dispatch err=%v want explicit no-kernel fallback", err)
			}
		})
	}
	_, err := reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.AggregateOp(columnsemantics.OpStringPrefix), Semantic: sem, Layout: layout})
	if err == nil || !strings.Contains(err.Error(), "unsupported aggregate operation") {
		t.Fatalf("string prefix dispatch err=%v want unsupported aggregate operation", err)
	}
	_, err = reg.Dispatch(typedkernel.DispatchRequest{Operation: typedkernel.AggregateOp(columnsemantics.OpStringLexicalRange), Semantic: sem, Layout: layout})
	if err == nil || !strings.Contains(err.Error(), "unsupported aggregate operation") {
		t.Fatalf("string lexical range dispatch err=%v want unsupported aggregate operation", err)
	}
}

func dictionarySemantic(ordered bool, collation string) columnsemantics.Descriptor {
	return columnsemantics.Descriptor{Logical: columnsemantics.LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, DictionaryOrder: ordered, DictionaryCollation: collation}
}

func dictionaryLayout(ordered bool, collation string) columnlayout.Capabilities {
	return columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionNone, Dictionary: true, DictionaryOrder: ordered, DictionaryCollation: collation})
}

func expectedDictionaryRows(codes []uint32, selection typedcolumn.RowSelection, want map[uint32]struct{}) []int {
	rows := selection.AppendRows(nil)
	out := rows[:0]
	for _, row := range rows {
		if _, ok := want[codes[row]]; ok {
			out = append(out, row)
		}
	}
	return out
}

func equalCodeRowsKernel(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
