package typedcolumn

import (
	"encoding/binary"
	"slices"
	"strings"
	"testing"
)

func TestUint32CodeSelectionParityShapes(t *testing.T) {
	codes := make([]uint32, 130)
	for i := range codes {
		codes[i] = uint32((i*i + 3*i) % 11)
	}
	codes[17] = 70000 // force 4-byte code width without affecting target parity.
	codes[96] = 70000
	builder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
	granule, err := builder.BuildUint32Codes(codes, 70001)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	bitmap := make([]uint64, rowSelectionBitmapWords(len(codes)))
	for _, row := range []int{0, 2, 5, 17, 31, 64, 65, 96, 127, 129} {
		bitmap[row/64] |= uint64(1) << uint(row%64)
	}
	selections := map[string]RowSelection{
		"all":    mustCodeSelection(NewAllRowSelection(len(codes))),
		"empty":  mustCodeSelection(NewEmptyRowSelection(len(codes))),
		"range":  mustCodeSelection(NewRangeRowSelection(len(codes), 7, 101)),
		"ranges": mustCodeSelection(NewRangesRowSelection(len(codes), []RowRange{{Start: 1, End: 9}, {Start: 32, End: 47}, {Start: 90, End: len(codes)}})),
		"bitmap": mustCodeSelection(NewBitmapRowSelection(len(codes), bitmap)),
		"sparse": mustCodeSelection(NewSparseRowSelection(len(codes), []int{1, 4, 7, 31, 64, 65, 88, 127})),
	}
	var reader GranuleReader
	var scratch Uint32CodeSelectionScratch
	for name, selection := range selections {
		for _, target := range []uint32{0, 3, 70000, 90000} {
			t.Run(name+"/eq", func(t *testing.T) {
				wantRows := expectedCodeRows(codes, selection, map[uint32]struct{}{target: {}})
				gotCount, err := reader.CountUint32Code(granule, selection, target)
				if err != nil {
					t.Fatalf("CountUint32Code: %v", err)
				}
				if gotCount != len(wantRows) {
					t.Fatalf("CountUint32Code=%d want %d rows=%v", gotCount, len(wantRows), wantRows)
				}
				gotSelection, err := reader.SelectUint32Code(granule, selection, target, &scratch)
				if err != nil {
					t.Fatalf("SelectUint32Code: %v", err)
				}
				if gotRows := gotSelection.AppendRows(nil); !equalCodeRows(gotRows, wantRows) {
					t.Fatalf("SelectUint32Code rows=%v want %v shape=%+v", gotRows, wantRows, gotSelection.Shape())
				}
			})
		}
		t.Run(name+"/in", func(t *testing.T) {
			wantSet := map[uint32]struct{}{2: {}, 5: {}, 70000: {}, 90000: {}}
			wantRows := expectedCodeRows(codes, selection, wantSet)
			gotCount, err := reader.CountUint32CodesIn(granule, selection, []uint32{2, 5, 70000, 90000, 5}, &scratch)
			if err != nil {
				t.Fatalf("CountUint32CodesIn: %v", err)
			}
			if gotCount != len(wantRows) {
				t.Fatalf("CountUint32CodesIn=%d want %d rows=%v", gotCount, len(wantRows), wantRows)
			}
			gotSelection, err := reader.SelectUint32CodesIn(granule, selection, []uint32{2, 5, 70000, 90000, 5}, &scratch)
			if err != nil {
				t.Fatalf("SelectUint32CodesIn: %v", err)
			}
			if gotRows := gotSelection.AppendRows(nil); !equalCodeRows(gotRows, wantRows) {
				t.Fatalf("SelectUint32CodesIn rows=%v want %v shape=%+v", gotRows, wantRows, gotSelection.Shape())
			}
		})
	}
}

func TestUint32CodePayloadUsesCompactWidths2300(t *testing.T) {
	cases := []struct {
		name        string
		codes       []uint32
		cardinality uint32
		width       byte
	}{
		{name: "one_byte", codes: []uint32{0, 1, 2, 1, 0}, cardinality: 3, width: 1},
		{name: "two_byte", codes: []uint32{0, 255, 256, 1}, cardinality: 257, width: 2},
		{name: "four_byte", codes: []uint32{0, 65535, 65536, 2}, cardinality: 65537, width: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
			granule, err := builder.BuildUint32Codes(tc.codes, tc.cardinality)
			if err != nil {
				t.Fatalf("BuildUint32Codes: %v", err)
			}
			if len(granule.Payload) == 0 || granule.Payload[0] != tc.width {
				t.Fatalf("payload width=%v want %d payload=%v", granule.Payload[:min(len(granule.Payload), 4)], tc.width, granule.Payload[:min(len(granule.Payload), 8)])
			}
			var varint [binary.MaxVarintLen64]byte
			cardinalityBytes := binary.PutUvarint(varint[:], uint64(tc.cardinality))
			wantRaw := 1 + cardinalityBytes + len(tc.codes)*int(tc.width)
			if granule.RawBytes != wantRaw || granule.StoredBytes != wantRaw || len(granule.Payload) != wantRaw {
				t.Fatalf("raw/stored/payload=%d/%d/%d want %d", granule.RawBytes, granule.StoredBytes, len(granule.Payload), wantRaw)
			}
			var reader GranuleReader
			got, err := reader.DecodeUint32Codes(granule)
			if err != nil {
				t.Fatalf("DecodeUint32Codes: %v", err)
			}
			if !slices.Equal(got, tc.codes) {
				t.Fatalf("DecodeUint32Codes=%v want %v", got, tc.codes)
			}
			count, err := reader.CountUint32Code(granule, mustCodeSelection(NewAllRowSelection(len(tc.codes))), tc.codes[len(tc.codes)-1])
			if err != nil {
				t.Fatalf("CountUint32Code: %v", err)
			}
			if count == 0 {
				t.Fatalf("CountUint32Code for present code returned 0")
			}
		})
	}
}

func TestUint32CodeSelectionRejectsStoredCodeOutsideCardinality(t *testing.T) {
	codes := []uint32{0, 1, 5, 2}
	builder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
	granule, err := builder.BuildUint32Codes(codes, 8)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	payload := append([]byte(nil), granule.Payload...)
	if len(payload) < 2 || payload[0] != 1 {
		t.Fatalf("unexpected test payload header: %v", payload[:min(len(payload), 4)])
	}
	payload[1] = 4 // Lower the encoded cardinality below stored code 5.
	granule.Payload = payload
	selection := mustCodeSelection(NewAllRowSelection(len(codes)))
	var reader GranuleReader
	var scratch Uint32CodeSelectionScratch
	for name, run := range map[string]func() error{
		"CountUint32Code": func() error {
			_, err := reader.CountUint32Code(granule, selection, 1)
			return err
		},
		"CountUint32CodesIn": func() error {
			_, err := reader.CountUint32CodesIn(granule, selection, []uint32{1, 2}, &scratch)
			return err
		},
		"SelectUint32Code": func() error {
			_, err := reader.SelectUint32Code(granule, selection, 1, &scratch)
			return err
		},
		"SelectUint32CodesIn": func() error {
			_, err := reader.SelectUint32CodesIn(granule, selection, []uint32{1, 2}, &scratch)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "outside cardinality") {
				t.Fatalf("err=%v want outside cardinality", err)
			}
		})
	}
}

func TestUint32CodeCountRejectsUnsupportedSelectionShape(t *testing.T) {
	codes := []uint32{0, 1, 2, 1}
	builder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
	granule, err := builder.BuildUint32Codes(codes, 3)
	if err != nil {
		t.Fatalf("BuildUint32Codes: %v", err)
	}
	selection := RowSelection{rows: len(codes), kind: RowSelectionKind(99), count: 1}
	var reader GranuleReader
	var scratch Uint32CodeSelectionScratch
	for name, run := range map[string]func() error{
		"CountUint32Code": func() error {
			_, err := reader.CountUint32Code(granule, selection, 1)
			return err
		},
		"CountUint32CodesIn": func() error {
			_, err := reader.CountUint32CodesIn(granule, selection, []uint32{1}, &scratch)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "unsupported code row selection shape") {
				t.Fatalf("err=%v want unsupported shape", err)
			}
		})
	}
}

func TestAggregateArenaSelectedCodeReducers(t *testing.T) {
	leftCodes := []uint32{0, 1, 2, 1, 3, 2, 1}
	rightCodes := []uint32{3, 3, 2, 0, 1, 4}
	leftBuilder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
	left, err := leftBuilder.BuildUint32Codes(leftCodes, 5)
	if err != nil {
		t.Fatalf("BuildUint32Codes left: %v", err)
	}
	rightBuilder := NewGranuleBuilder(Config{Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone})
	right, err := rightBuilder.BuildUint32Codes(rightCodes, 5)
	if err != nil {
		t.Fatalf("BuildUint32Codes right: %v", err)
	}
	selections := []RowSelection{
		mustCodeSelection(NewRangesRowSelection(len(leftCodes), []RowRange{{Start: 1, End: 4}, {Start: 5, End: 7}})),
		mustCodeSelection(NewBitmapRowSelection(len(rightCodes), []uint64{(1 << 0) | (1 << 2) | (1 << 5)})),
	}
	var arena AggregateArena
	counts, err := arena.GroupedCountCodesSelected([]EncodedGranule{left, right}, selections, 5)
	if err != nil {
		t.Fatalf("GroupedCountCodesSelected: %v", err)
	}
	wantCounts := []uint64{0, 3, 3, 1, 1}
	for code, want := range wantCounts {
		if counts[code] != want {
			t.Fatalf("count[%d]=%d want %d all=%v", code, counts[code], want, counts)
		}
	}
	distinct, err := arena.ExactDistinctCodesSelected([]EncodedGranule{left, right}, selections, 5)
	if err != nil {
		t.Fatalf("ExactDistinctCodesSelected: %v", err)
	}
	if distinct != 4 {
		t.Fatalf("distinct=%d want 4", distinct)
	}
}

func expectedCodeRows(codes []uint32, selection RowSelection, want map[uint32]struct{}) []int {
	rows := selection.AppendRows(nil)
	out := rows[:0]
	for _, row := range rows {
		if _, ok := want[codes[row]]; ok {
			out = append(out, row)
		}
	}
	return out
}

func mustCodeSelection(selection RowSelection, err error) RowSelection {
	if err != nil {
		panic(err)
	}
	return selection
}

func equalCodeRows(left, right []int) bool {
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
