package commitlog

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"
)

func collectionTypedMetadataFixture() CollectionTypedMetadataPayload {
	return CollectionTypedMetadataPayload{
		Collection: "docs",
		SchemaHash: 0x0102030405060708,
		Columns:    []string{"author.name", "status"},
		Documents: []CollectionTypedMetadataDocument{
			{ID: []byte("b"), Retained: []byte(`{}`), Values: []string{"Bob", "live"}},
			{ID: []byte("a"), Retained: []byte(`{"x":1}`), Values: []string{"Ada", "draft"}},
		},
	}
}

func TestCollectionTypedMetadataPayloadGoldenRoundTrip(t *testing.T) {
	p := collectionTypedMetadataFixture()
	raw, err := EncodeCollectionTypedMetadataPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	const goldenHex = "01000807060504030201020000000200000004000000646f63730b000000617574686f722e6e616d65060000007374617475730100000061070000007b2278223a317d030000004164610500000064726166740100000062020000007b7d03000000426f62040000006c697665"
	if got := hex.EncodeToString(raw); got != goldenHex {
		t.Fatalf("golden payload changed:\n got %s\nwant %s", got, goldenHex)
	}

	want := p
	want.Documents = []CollectionTypedMetadataDocument{p.Documents[1], p.Documents[0]}
	got, err := DecodeCollectionTypedMetadataPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("roundtrip=%+v want %+v", got, want)
	}
	canonical, err := EncodeCollectionTypedMetadataPayload(want)
	if err != nil || !bytes.Equal(canonical, raw) {
		t.Fatalf("canonical encoding mismatch: %v", err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := validateCollectionTypedMetadataPayload(raw); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("validator allocations=%g", allocs)
	}

	owned, err := DecodeCollectionTypedMetadataPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		raw[i] = 0
	}
	if !reflect.DeepEqual(owned, want) {
		t.Fatal("decoded payload aliases input")
	}
}

func TestCollectionTypedMetadataPayloadZeroColumns(t *testing.T) {
	p := CollectionTypedMetadataPayload{
		Collection: "residual-only",
		SchemaHash: 9,
		Documents:  []CollectionTypedMetadataDocument{{ID: []byte("id"), Retained: []byte(`{"title":"kept"}`)}},
	}
	raw, err := EncodeCollectionTypedMetadataPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCollectionTypedMetadataPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	p.Columns = []string{}
	p.Documents[0].Values = []string{}
	if !reflect.DeepEqual(got, p) {
		t.Fatalf("roundtrip=%+v want %+v", got, p)
	}
}

func TestCollectionTypedMetadataPayloadRejectsTruncationAndCorruption(t *testing.T) {
	raw, err := EncodeCollectionTypedMetadataPayload(collectionTypedMetadataFixture())
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(raw); n++ {
		if err := validateCollectionTypedMetadataPayload(raw[:n]); err == nil {
			t.Fatalf("validator accepted prefix %d", n)
		}
		if err := validateCommandEnvelopePayload(CommandEnvelope{Kind: CommandKindCollectionUpdateBatchByID, PayloadFormat: PayloadFormatCollectionTypedMetadataByIDV1, Payload: raw[:n]}); err == nil {
			t.Fatalf("envelope validator accepted prefix %d", n)
		}
		if _, err := DecodeCollectionTypedMetadataPayload(raw[:n]); err == nil {
			t.Fatalf("decoder accepted prefix %d", n)
		}
	}

	collectionOffset := collectionTypedMetadataPrefixSize
	firstColumnOffset := collectionOffset + 4 + len("docs")
	secondColumnOffset := firstColumnOffset + 4 + len("author.name")
	firstRowOffset := secondColumnOffset + 4 + len("status")
	firstRetainedOffset := firstRowOffset + 4 + len("a")
	firstValueOffset := firstRetainedOffset + 4 + len(`{"x":1}`)
	secondRowOffset := firstValueOffset + 4 + len("Ada") + 4 + len("draft")

	tests := map[string]func([]byte){
		"unknown_version":     func(b []byte) { binary.LittleEndian.PutUint16(b, 2) },
		"zero_schema_hash":    func(b []byte) { binary.LittleEndian.PutUint64(b[2:], 0) },
		"huge_column_count":   func(b []byte) { binary.LittleEndian.PutUint32(b[10:], math.MaxUint32) },
		"huge_row_count":      func(b []byte) { binary.LittleEndian.PutUint32(b[14:], math.MaxUint32) },
		"huge_collection_len": func(b []byte) { binary.LittleEndian.PutUint32(b[collectionOffset:], math.MaxUint32) },
		"huge_column_len":     func(b []byte) { binary.LittleEndian.PutUint32(b[firstColumnOffset:], math.MaxUint32) },
		"huge_id_len":         func(b []byte) { binary.LittleEndian.PutUint32(b[firstRowOffset:], math.MaxUint32) },
		"huge_retained_len":   func(b []byte) { binary.LittleEndian.PutUint32(b[firstRetainedOffset:], math.MaxUint32) },
		"huge_value_len":      func(b []byte) { binary.LittleEndian.PutUint32(b[firstValueOffset:], math.MaxUint32) },
		"unsorted_columns":    func(b []byte) { copy(b[secondColumnOffset+4:], []byte("author")) },
		"invalid_utf8_column": func(b []byte) { b[firstColumnOffset+4] = 0xff },
		"duplicate_ids":       func(b []byte) { b[secondRowOffset+4] = b[firstRowOffset+4] },
		"invalid_utf8_value":  func(b []byte) { b[firstValueOffset+4] = 0xff },
		"mismatched_columns":  func(b []byte) { binary.LittleEndian.PutUint32(b[10:], 3) },
		"trailing_data":       func(b []byte) {},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			corrupt := bytes.Clone(raw)
			mutate(corrupt)
			if name == "trailing_data" {
				corrupt = append(corrupt, 0)
			}
			if err := validateCollectionTypedMetadataPayload(corrupt); err == nil {
				t.Fatal("validator accepted corrupt payload")
			}
			if _, err := DecodeCollectionTypedMetadataPayload(corrupt); err == nil {
				t.Fatal("decoder accepted corrupt payload")
			}
		})
	}

	duplicateColumns := collectionTypedMetadataFixture()
	duplicateColumns.Columns = []string{"first1", "second"}
	duplicateRaw, err := EncodeCollectionTypedMetadataPayload(duplicateColumns)
	if err != nil {
		t.Fatal(err)
	}
	first := collectionTypedMetadataPrefixSize + 4 + len(duplicateColumns.Collection)
	second := first + 4 + len(duplicateColumns.Columns[0])
	copy(duplicateRaw[second+4:], duplicateRaw[first+4:first+4+len(duplicateColumns.Columns[0])])
	if err := validateCollectionTypedMetadataPayload(duplicateRaw); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("duplicate columns error=%v want ErrCorrupt", err)
	}
}

func TestCollectionTypedMetadataPayloadEncodeValidation(t *testing.T) {
	valid := collectionTypedMetadataFixture()
	tests := map[string]func(*CollectionTypedMetadataPayload){
		"empty_collection":   func(p *CollectionTypedMetadataPayload) { p.Collection = "" },
		"invalid_collection": func(p *CollectionTypedMetadataPayload) { p.Collection = "\xff" },
		"zero_schema":        func(p *CollectionTypedMetadataPayload) { p.SchemaHash = 0 },
		"empty_documents":    func(p *CollectionTypedMetadataPayload) { p.Documents = nil },
		"empty_column":       func(p *CollectionTypedMetadataPayload) { p.Columns[0] = "" },
		"invalid_column":     func(p *CollectionTypedMetadataPayload) { p.Columns[0] = "\xff" },
		"unsorted_columns":   func(p *CollectionTypedMetadataPayload) { p.Columns[0], p.Columns[1] = p.Columns[1], p.Columns[0] },
		"duplicate_columns":  func(p *CollectionTypedMetadataPayload) { p.Columns[1] = p.Columns[0] },
		"empty_id":           func(p *CollectionTypedMetadataPayload) { p.Documents[0].ID = nil },
		"duplicate_ids":      func(p *CollectionTypedMetadataPayload) { p.Documents[0].ID = p.Documents[1].ID },
		"mismatched_values":  func(p *CollectionTypedMetadataPayload) { p.Documents[0].Values = p.Documents[0].Values[:1] },
		"invalid_utf8_value": func(p *CollectionTypedMetadataPayload) { p.Documents[0].Values[0] = "\xff" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			p := valid
			p.Columns = append([]string(nil), valid.Columns...)
			p.Documents = append([]CollectionTypedMetadataDocument(nil), valid.Documents...)
			for i := range p.Documents {
				p.Documents[i].Values = append([]string(nil), valid.Documents[i].Values...)
			}
			mutate(&p)
			if _, err := EncodeCollectionTypedMetadataPayload(p); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error=%v want ErrCorrupt", err)
			}
		})
	}
}

func TestCollectionTypedMetadataEnvelopeRegistration(t *testing.T) {
	raw, err := EncodeCollectionTypedMetadataPayload(collectionTypedMetadataFixture())
	if err != nil {
		t.Fatal(err)
	}
	valid := CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindCollectionUpdateBatchByID,
		Scope:         CommandScopeCollection,
		PayloadFormat: PayloadFormatCollectionTypedMetadataByIDV1,
		Payload:       raw,
	}
	frame, err := EncodeCommandFrame(valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCommandFrame(frame); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*CommandEnvelope){
		"wrong_command": func(env *CommandEnvelope) { env.Kind = CommandKindCollectionInsertBatchByID },
		"wrong_scope":   func(env *CommandEnvelope) { env.Scope = CommandScopeRawKV },
	} {
		t.Run(name, func(t *testing.T) {
			env := valid
			mutate(&env)
			if _, err := EncodeCommandFrame(env); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error=%v want ErrCorrupt", err)
			}
		})
	}
}
