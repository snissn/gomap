package commitlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestCollectionTypedBatchHeaderValidation(t *testing.T) {
	p := CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, Columns: []CollectionTypedColumn{{Name: "content", Type: CollectionTypedString}}, Documents: []CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{}`), Values: []CollectionTypedValue{{String: "value"}}}}}
	raw, err := EncodeCollectionTypedBatchPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		raw  []byte
		want error
	}{
		{"empty_rows", func() []byte {
			b := bytes.Clone(raw[:23+len(p.Collection)+9+len(p.Columns[0].Name)])
			binary.LittleEndian.PutUint32(b[14:], 0)
			return b
		}(), ErrCorrupt},
		{"unknown_version", func() []byte {
			b := bytes.Clone(raw)
			binary.LittleEndian.PutUint16(b, 2)
			return b
		}(), ErrCommandWALUnsupportedVersion},
		{"short_header", raw[:1], ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeCollectionTypedBatchPayload(tc.raw); !errors.Is(err, tc.want) {
				t.Errorf("decode error=%v want %v", err, tc.want)
			}
			for _, kind := range []CommandKind{CommandKindCollectionInsertBatchByID, CommandKindCollectionUpdateBatchByID} {
				if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: kind, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: tc.raw}); !errors.Is(err, tc.want) {
					t.Errorf("kind %d error=%v want %v", kind, err, tc.want)
				}
			}
		})
	}
	p.Documents = nil
	if _, err := EncodeCollectionTypedBatchPayload(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("empty batch encode error=%v want %v", err, ErrCorrupt)
	}
}

func TestCollectionTypedLegacyProjectionFlag(t *testing.T) {
	p := CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 1, LegacyProjection: true, Columns: []CollectionTypedColumn{{Name: "embedding", Type: CollectionTypedFloat32Vector, Dimensions: 1}}, Documents: []CollectionTypedDocument{{ID: []byte("a"), Retained: []byte(`{"id":"a","id":"a"}`), Values: []CollectionTypedValue{{Vector: []float32{1}}}}}}
	raw, err := EncodeCollectionTypedBatchPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCollectionTypedBatchPayload(raw)
	if err != nil || !reflect.DeepEqual(decoded, p) {
		t.Fatalf("legacy roundtrip %+v %v", decoded, err)
	}
	if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: CommandKindCollectionInsertBatchByID, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: CommandKindCollectionUpdateBatchByID, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: raw}); err == nil {
		t.Fatal("accepted legacy origin on update")
	}
}

func TestCollectionTypedBatchPayload(t *testing.T) {
	p := CollectionTypedBatchPayload{Collection: "docs", SchemaHash: 7, Columns: []CollectionTypedColumn{{Name: "a_embedding", Type: CollectionTypedFloat32Vector, Dimensions: 2}, {Name: "content", Type: CollectionTypedString}}, Documents: []CollectionTypedDocument{
		{ID: []byte("b"), Retained: []byte{0xff}, Values: []CollectionTypedValue{{Vector: []float32{0, 1}}, {String: "second café 世界"}}},
		{ID: []byte("a"), Retained: []byte(`{}`), Values: []CollectionTypedValue{{Vector: []float32{1, 0}}, {String: "first"}}},
	}}
	raw, err := EncodeCollectionTypedBatchPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := validateCollectionTypedBatchPayload(raw); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("scanner allocations=%g", allocs)
	}
	got, err := DecodeCollectionTypedBatchPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	p.Documents[0], p.Documents[1] = p.Documents[1], p.Documents[0]
	if !reflect.DeepEqual(got, p) {
		t.Fatalf("roundtrip=%+v want %+v", got, p)
	}
	canonical, err := EncodeCollectionTypedBatchPayload(p)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatalf("noncanonical encoding: %v", err)
	}
	for n := 0; n < len(raw); n++ {
		if _, err := DecodeCollectionTypedBatchPayload(raw[:n]); err == nil {
			t.Fatalf("accepted truncated payload at %d", n)
		}
		for _, kind := range []CommandKind{CommandKindCollectionInsertBatchByID, CommandKindCollectionUpdateBatchByID} {
			if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: kind, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: raw[:n]}); err == nil {
				t.Fatalf("kind %d accepted truncation %d", kind, n)
			}
		}
	}
	for _, kind := range []CommandKind{CommandKindCollectionInsertBatchByID, CommandKindCollectionUpdateBatchByID} {
		env := CommandEnvelope{LSN: 1, Kind: kind, Scope: CommandScopeCollection, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: raw}
		if err := validateCommandEnvelopeIdentity(env); err != nil {
			t.Fatal(err)
		}
		if allocs := testing.AllocsPerRun(100, func() {
			if err := validateCommandEnvelopePayload(env); err != nil {
				panic(err)
			}
		}); allocs != 0 {
			t.Fatalf("envelope scanner allocs=%g", allocs)
		}
	}
	for _, offset := range []int{10, 14, 19} {
		corrupt := bytes.Clone(raw)
		binary.LittleEndian.PutUint32(corrupt[offset:], math.MaxUint32)
		if err := validateCollectionTypedBatchPayload(corrupt); err == nil {
			t.Fatalf("accepted huge count at %d", offset)
		}
	}
	columnOffset := 23 + len(p.Collection)
	typeOffset := columnOffset + 4 + len(p.Columns[0].Name)
	rowOffset := columnOffset
	for _, col := range p.Columns {
		rowOffset += 9 + len(col.Name)
	}
	vectorOffset := rowOffset + 4 + len(p.Documents[0].ID) + 4 + len(p.Documents[0].Retained)
	secondRowOffset := vectorOffset + int(p.Columns[0].Dimensions)*4 + 4 + len(p.Documents[0].Values[1].String)
	for name, mutate := range map[string]func([]byte){
		"invalid_utf8_string":  func(raw []byte) { raw[vectorOffset+int(p.Columns[0].Dimensions)*4+4] = 0xff },
		"duplicate_id":         func(raw []byte) { raw[secondRowOffset+4] = raw[rowOffset+4] },
		"unknown_flag":         func(raw []byte) { raw[18] = 2 },
		"legacy_wrong_columns": func(raw []byte) { raw[18] = 1 },
		"unknown_type":         func(raw []byte) { raw[typeOffset] = 255 },
		"zero_dimensions":      func(raw []byte) { binary.LittleEndian.PutUint32(raw[typeOffset+1:], 0) },
		"huge_dimensions":      func(raw []byte) { binary.LittleEndian.PutUint32(raw[typeOffset+1:], math.MaxUint32) },
		"nonfinite": func(raw []byte) {
			binary.LittleEndian.PutUint32(raw[vectorOffset:], math.Float32bits(float32(math.Inf(1))))
		},
		"schema_zero": func(raw []byte) { binary.LittleEndian.PutUint64(raw[2:], 0) },
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(raw)
			mutate(corrupt)
			if err := validateCollectionTypedBatchPayload(corrupt); !errors.Is(err, ErrCorrupt) {
				t.Errorf("scanner error=%v want ErrCorrupt", err)
			}
			if _, err := DecodeCollectionTypedBatchPayload(corrupt); err == nil {
				t.Error("decoder accepted malformed payload")
			}
			for _, kind := range []CommandKind{CommandKindCollectionInsertBatchByID, CommandKindCollectionUpdateBatchByID} {
				if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: kind, PayloadFormat: PayloadFormatCollectionTypedBatchByIDV1, Payload: corrupt}); err == nil {
					t.Fatal("envelope scanner accepted malformed payload")
				}
			}
		})
	}
	owned, err := DecodeCollectionTypedBatchPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !reflect.DeepEqual(owned, p) {
		t.Fatal("decode retained input aliases")
	}
	raw = canonical
	if _, err := DecodeCollectionTypedBatchPayload(append(raw, 0)); err == nil {
		t.Fatal("accepted trailing data")
	}
	previousString := p.Documents[0].Values[1].String
	p.Documents[0].Values[1].String = "invalid\xff"
	if _, err := EncodeCollectionTypedBatchPayload(p); !errors.Is(err, ErrCorrupt) {
		t.Errorf("invalid UTF-8 encode error=%v want ErrCorrupt", err)
	}
	p.Documents[0].Values[1].String = previousString
	p.Documents[0].Values[0].Vector[0] = float32(math.Inf(1))
	if _, err := EncodeCollectionTypedBatchPayload(p); err == nil {
		t.Fatal("accepted nonfinite vector")
	}
	p.Documents[0].Values[0].Vector[0] = 1
	p.Documents[1].ID = p.Documents[0].ID
	if _, err := EncodeCollectionTypedBatchPayload(p); err == nil {
		t.Fatal("accepted duplicate ID")
	}
}
