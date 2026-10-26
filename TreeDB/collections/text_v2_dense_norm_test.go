package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
)

// Independent old-wire oracle: fallback must preserve even invalid V1 inputs,
// rather than normalizing away corruption used by the maintained validators.
func denseNormV1Oracle(value textV2NormBlockValue) []byte {
	entries := slices.Clone(value.Entries)
	slices.SortFunc(entries, func(a, b textV2NormBlockEntry) int { return compareUint64(a.Ordinal, b.Ordinal) })
	raw := []byte{1}
	for _, n := range []uint64{value.BlockStart, uint64(value.BlockSize), uint64(value.FieldCount), uint64(len(entries))} {
		raw = binary.AppendUvarint(raw, n)
	}
	for _, e := range entries {
		raw = binary.AppendUvarint(raw, e.Ordinal)
		raw = binary.AppendUvarint(raw, e.Generation)
		raw = append(raw, e.Flags)
		for _, n := range e.FieldLengths {
			raw = binary.AppendUvarint(raw, uint64(n))
		}
	}
	return raw
}

func denseNormFixture(count, fields int, length uint32) textV2NormBlockValue {
	b := textV2NormBlockValue{BlockStart: 129, BlockSize: 128, FieldCount: uint32(fields)}
	for i := 0; i < count; i++ {
		e := textV2NormBlockEntry{Ordinal: b.BlockStart + uint64(i), Generation: 7, FieldLengths: make([]uint32, fields)}
		for j := range e.FieldLengths {
			e.FieldLengths[j] = length
		}
		if i%3 == 1 {
			e.Flags = textV2DocFlagTombstone
		}
		b.Entries = append(b.Entries, e)
	}
	return b
}

// Wire order is version, five uvarints, generation/field widths, bitmap, generation deltas, entry-major fields.
func denseNormWire(start, size, fields, count, generation uint64, generationWidth, width byte, bitmap, payload []byte) []byte {
	raw := []byte{2}
	for _, n := range []uint64{start, size, fields, count, generation} {
		raw = binary.AppendUvarint(raw, n)
	}
	raw = append(raw, generationWidth, width)
	raw = append(raw, bitmap...)
	return append(raw, payload...)
}

func denseNormWidth(raw []byte) byte {
	cur := textCursor{buf: raw[1:]}
	for i := 0; i < 5; i++ {
		_, _ = cur.readUvarint()
	}
	return cur.buf[cur.pos+1]
}

func TestTextV2DenseNormWidthsAndSelection(t *testing.T) {
	for _, tc := range []struct {
		length uint32
		width  byte
	}{{0, 1}, {255, 1}, {256, 2}, {65535, 2}, {65536, 4}, {math.MaxUint32, 4}} {
		for _, count := range []int{9, 128} {
			for _, fields := range []int{1, 3} {
				t.Run(fmt.Sprintf("%d/%d/%d", tc.length, count, fields), func(t *testing.T) {
					value := denseNormFixture(count, fields, tc.length)
					raw := encodeTextV2NormBlockValue(value)
					if raw[0] != 2 || denseNormWidth(raw) != tc.width || len(raw) > len(denseNormV1Oracle(value)) {
						t.Fatalf("encoding version/width/size: %x", raw)
					}
					full, err := decodeTextV2NormBlockValue(raw)
					if err != nil || !reflect.DeepEqual(full, value) {
						t.Fatalf("roundtrip=%+v err=%v", full, err)
					}
					for _, allow := range []*textV2SearchOrdinalAllowSet{nil, selectedSidecarOrdinals(), selectedSidecarOrdinals(129, 132, 137, 256, 257)} {
						got, err := decodeTextV2SearchNormBlockSelected(raw, allow)
						want, oldErr := decodeTextV2SearchNormBlockSelected(denseNormV1Oracle(value), allow)
						if err != nil || oldErr != nil || !reflect.DeepEqual(got, want) {
							t.Fatalf("selected=%+v want=%+v err=%v/%v", got, want, err, oldErr)
						}
						if cap(got.Entries) != len(got.Entries) || cap(got.FieldLengths) != len(got.FieldLengths) {
							t.Fatal("selection retained excess backing")
						}
					}
					selected, err := decodeTextV2SearchNormBlockSelected(raw, selectedSidecarOrdinals(129))
					if err != nil {
						t.Fatal(err)
					}
					clear(raw)
					if selected.Entries[0].FieldLengths[0] != tc.length || selected.Entries[0].Generation != 7 {
						t.Fatal("selected output aliases wire")
					}
				})
			}
		}
	}
}

func TestTextV2DenseNormV1Fallback(t *testing.T) {
	base := denseNormFixture(8, 1, 3)
	for _, kind := range []string{"sparse", "wide_generation", "zero_generation", "flags", "duplicate", "outside", "field_shape", "empty", "overflow", "nonbeneficial"} {
		t.Run(kind, func(t *testing.T) {
			b := base
			b.Entries = slices.Clone(base.Entries)
			switch kind {
			case "sparse":
				b.Entries = append(b.Entries[:2:2], b.Entries[3:]...)
			case "wide_generation":
				b.Entries[1].Generation += 1 << 32
			case "zero_generation":
				b.Entries[1].Generation = 0
			case "flags":
				b.Entries[1].Flags = 2
			case "duplicate":
				b.Entries[1].Ordinal = b.Entries[0].Ordinal
			case "outside":
				b.Entries[7].Ordinal = 257
			case "field_shape":
				b.Entries[1].FieldLengths = nil
			case "empty":
				b.Entries = nil
			case "overflow":
				b.BlockStart = math.MaxUint64
				b.Entries = b.Entries[:1]
				b.Entries[0].Ordinal = b.BlockStart
			case "nonbeneficial":
				b = denseNormFixture(1, 1, 0)
				b.BlockStart = 1
				b.Entries[0].Ordinal = 1
			}
			got, want := encodeTextV2NormBlockValue(b), denseNormV1Oracle(b)
			if !bytes.Equal(got, want) {
				t.Fatalf("fallback changed V1: %x want %x", got, want)
			}
		})
	}
	old := denseNormV1Oracle(base)
	if _, err := decodeTextV2NormBlockValue(old); err != nil {
		t.Fatalf("old V1 reader: %v", err)
	}
}

func TestTextV2DenseNormStructuralProof(t *testing.T) {
	valid := denseNormWire(1, 128, 1, 9, 1, 0, 1, []byte{2, 0}, make([]byte, 9))
	cases := map[string][]byte{
		"zero_start":           denseNormWire(0, 128, 1, 9, 1, 0, 1, []byte{0, 0}, make([]byte, 9)),
		"zero_size":            denseNormWire(1, 0, 1, 9, 1, 0, 1, []byte{0, 0}, make([]byte, 9)),
		"size_overflow":        denseNormWire(1, 1<<32, 1, 9, 1, 0, 1, nil, nil),
		"ordinal_overflow":     denseNormWire(math.MaxUint64-2, 128, 1, 9, 1, 0, 1, []byte{0, 0}, make([]byte, 9)),
		"zero_fields":          denseNormWire(1, 128, 0, 9, 1, 0, 1, []byte{0, 0}, nil),
		"field_overflow":       denseNormWire(1, 128, 1<<32, 9, 1, 0, 1, nil, nil),
		"zero_entries":         denseNormWire(1, 128, 1, 0, 1, 0, 1, nil, nil),
		"too_many_entries":     denseNormWire(1, 128, 1, 129, 1, 0, 1, nil, nil),
		"count_overflow":       denseNormWire(1, 128, 1, math.MaxUint64, 1, 0, 1, nil, nil),
		"product_overflow":     denseNormWire(1, math.MaxUint32, math.MaxUint32, math.MaxUint32, 1, 0, 4, nil, nil),
		"zero_generation":      denseNormWire(1, 128, 1, 9, 0, 0, 1, []byte{0, 0}, make([]byte, 9)),
		"zero_width":           denseNormWire(1, 128, 1, 9, 1, 0, 0, nil, nil),
		"width_three":          denseNormWire(1, 128, 1, 9, 1, 0, 3, nil, nil),
		"unused_tombstone_bit": denseNormWire(1, 128, 1, 9, 1, 0, 1, []byte{0, 128}, make([]byte, 9)),
		"truncated_unselected": slices.Clone(valid[:len(valid)-1]),
		"trailing":             append(slices.Clone(valid), 0),
	}
	for n := 0; n < 8; n++ {
		cases[fmt.Sprintf("short_header_%d", n)] = slices.Clone(valid[:n])
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeTextV2NormBlockValue(raw); err == nil {
				t.Fatal("full accepted malformed record")
			}
			for _, allow := range []*textV2SearchOrdinalAllowSet{nil, selectedSidecarOrdinals(), selectedSidecarOrdinals(1)} {
				if _, err := decodeTextV2SearchNormBlockSelected(raw, allow); err == nil {
					t.Fatal("selected accepted malformed unselected structure")
				}
			}
		})
	}
	// Wider-than-necessary legal fields remain valid: V1 already accepts
	// nonminimal varints, so width minimality is a writer choice, not corruption.
	for _, width := range []byte{1, 2, 4} {
		raw := denseNormWire(1, 128, 1, 9, 1, 0, width, []byte{2, 0}, make([]byte, int(width)*9))
		full, err := decodeTextV2NormBlockValue(raw)
		if err != nil || len(full.Entries) != 9 || full.Entries[1].Flags != 1 {
			t.Fatalf("legal width %d: %+v %v", width, full, err)
		}
	}
}

func TestTextV2DenseNormGenerationDeltas(t *testing.T) {
	for _, tc := range []struct {
		delta uint64
		width byte
	}{{0, 0}, {1, 1}, {255, 1}, {256, 2}, {65535, 2}, {65536, 4}, {math.MaxUint32, 4}} {
		b := denseNormFixture(128, 1, 3)
		base := uint64(1) << 40
		for i := range b.Entries {
			b.Entries[i].Generation = base
		}
		b.Entries[127].Generation += tc.delta
		raw := encodeTextV2NormBlockValue(b)
		cur := textCursor{buf: raw[1:]}
		for i := 0; i < 5; i++ {
			if _, err := cur.readUvarint(); err != nil {
				t.Fatal(err)
			}
		}
		if raw[0] != 2 || cur.buf[cur.pos] != tc.width {
			t.Fatalf("generation delta %d: wire=%x", tc.delta, raw)
		}
		got, err := decodeTextV2NormBlockValue(raw)
		if err != nil || !reflect.DeepEqual(got, b) {
			t.Fatalf("delta %d: %+v %v", tc.delta, got, err)
		}
		selected, err := decodeTextV2SearchNormBlockSelected(raw, selectedSidecarOrdinals(256))
		if err != nil || len(selected.Entries) != 1 || selected.Entries[0].Generation != base+tc.delta {
			t.Fatalf("selected delta %d: %+v %v", tc.delta, selected, err)
		}
	}
	b := denseNormFixture(8, 1, 3)
	for i := range b.Entries {
		b.Entries[i].Generation = math.MaxUint64 - 2
	}
	b.Entries[1].Generation++
	if got := encodeTextV2NormBlockValue(b); !bytes.Equal(got, denseNormV1Oracle(b)) {
		t.Fatal("representable generation range overflow did not fall back")
	}
	for _, gw := range []byte{1, 2, 4} {
		// Positive base need not be the actual minimum and width need not be
		// minimal: those are writer choices, not reader validity predicates.
		payload := make([]byte, 9*(int(gw)+1))
		for i := 0; i < 9; i++ {
			payload[i*int(gw)] = 1
		}
		raw := denseNormWire(1, 128, 1, 9, 1, gw, 1, []byte{0, 0}, payload)
		got, err := decodeTextV2SearchNormBlockSelected(raw, selectedSidecarOrdinals(1))
		if err != nil || got.Entries[0].Generation != 2 {
			t.Fatalf("legal noncanonical generation width %d: %+v %v", gw, got, err)
		}
	}
	for _, raw := range [][]byte{
		denseNormWire(1, 128, 1, 9, 1, 3, 1, nil, nil),
		denseNormWire(1, 128, 1, 9, math.MaxUint64, 1, 1, []byte{0, 0}, make([]byte, 18)),
		denseNormWire(1, 128, 1, 9, math.MaxUint64-65534, 2, 1, []byte{0, 0}, make([]byte, 27)),
	} {
		if _, err := decodeTextV2NormBlockValue(raw); err == nil {
			t.Fatal("full accepted illegal generation layout")
		}
		if _, err := decodeTextV2SearchNormBlockSelected(raw, selectedSidecarOrdinals()); err == nil {
			t.Fatal("empty selection accepted illegal generation layout")
		}
	}
}
