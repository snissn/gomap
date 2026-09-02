package typedcolumn

import "testing"

func TestBoolCountAndSelectBitpackSelectionParity(t *testing.T) {
	values := make([]bool, 130)
	for i := range values {
		values[i] = i%3 == 0 || i%11 == 0
	}
	granule := mustBuildBoolGranule(t, values)
	if len(granule.Payload) == 0 || granule.Payload[0] != boolPayloadBitpack {
		t.Fatalf("payload mode=%v want bitpack", granule.Payload)
	}
	assertBoolCountSelectParity(t, granule, values)
}

func TestBoolCountAndSelectRLESelectionParity(t *testing.T) {
	values := make([]bool, 160)
	for i := range values {
		switch {
		case i < 40:
			values[i] = true
		case i < 75:
			values[i] = false
		case i < 140:
			values[i] = true
		default:
			values[i] = false
		}
	}
	granule := mustBuildBoolGranule(t, values)
	if len(granule.Payload) == 0 || granule.Payload[0] != boolPayloadRLE {
		t.Fatalf("payload mode=%v want rle", granule.Payload)
	}
	assertBoolCountSelectParity(t, granule, values)
}

func assertBoolCountSelectParity(t *testing.T, granule EncodedGranule, values []bool) {
	t.Helper()
	rows := len(values)
	bitmap := make([]uint64, rowSelectionBitmapWords(rows))
	for _, row := range []int{0, 2, 3, 5, 8, 13, 21, 34, 55, 89, 129} {
		if row < rows {
			bitmap[row/64] |= uint64(1) << uint(row%64)
		}
	}
	selections := map[string]RowSelection{
		"all":    mustBoolSelection(NewAllRowSelection(rows)),
		"empty":  mustBoolSelection(NewEmptyRowSelection(rows)),
		"range":  mustBoolSelection(NewRangeRowSelection(rows, 7, 97)),
		"ranges": mustBoolSelection(NewRangesRowSelection(rows, []RowRange{{Start: 1, End: 9}, {Start: 32, End: 47}, {Start: 90, End: rows}})),
		"bitmap": mustBoolSelection(NewBitmapRowSelection(rows, bitmap)),
		"sparse": mustBoolSelection(NewSparseRowSelection(rows, []int{1, 4, 7, 31, 64, 65, 88, 127})),
	}
	var reader GranuleReader
	var scratch BoolSelectionScratch
	for name, selection := range selections {
		for _, wantValue := range []bool{false, true} {
			wantRows := expectedBoolRows(values, selection, wantValue)
			gotCount, err := reader.CountBool(granule, selection, wantValue)
			if err != nil {
				t.Fatalf("%s CountBool(%v): %v", name, wantValue, err)
			}
			if gotCount != len(wantRows) {
				t.Fatalf("%s CountBool(%v)=%d want %d rows=%v", name, wantValue, gotCount, len(wantRows), wantRows)
			}
			gotSelection, err := reader.SelectBool(granule, selection, wantValue, &scratch)
			if err != nil {
				t.Fatalf("%s SelectBool(%v): %v", name, wantValue, err)
			}
			gotRows := gotSelection.AppendRows(nil)
			if !equalIntSlices(gotRows, wantRows) {
				t.Fatalf("%s SelectBool(%v) rows=%v want %v shape=%+v", name, wantValue, gotRows, wantRows, gotSelection.Shape())
			}
		}
	}
}

func mustBuildBoolGranule(t *testing.T, values []bool) EncodedGranule {
	t.Helper()
	builder := NewGranuleBuilder(Config{Encoding: EncodingBoolBitpackRLE, Compression: CompressionNone})
	granule, err := builder.BuildBool(values)
	if err != nil {
		t.Fatalf("BuildBool: %v", err)
	}
	return granule
}

func expectedBoolRows(values []bool, selection RowSelection, want bool) []int {
	rows := selection.AppendRows(nil)
	out := rows[:0]
	for _, row := range rows {
		if values[row] == want {
			out = append(out, row)
		}
	}
	return out
}

func mustBoolSelection(selection RowSelection, err error) RowSelection {
	if err != nil {
		panic(err)
	}
	return selection
}

func equalIntSlices(left, right []int) bool {
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
