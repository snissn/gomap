package commitlog

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestTypedStringsCommandCanonicalOwnedAndRawBounds(t *testing.T) {
	p := CollectionTypedStringsPayload{Collection: "r1", SchemaHash: 7, SchemaCount: 4, Documents: []CollectionTypedStringPatch{
		{ID: []byte("b"), Generation: 2, PartID: 1, AppliedCommandLSN: 3, Edits: []CollectionTypedStringEdit{{Column: 0, Value: "雪<>"}}, ReplaceResidual: true, Residual: []byte(`{"id":"b","optional":null}`)},
		{ID: []byte("a"), Generation: 1, PartID: 1, AppliedCommandLSN: 2, Edits: []CollectionTypedStringEdit{{Column: 3, Value: ""}}},
	}}
	raw, err := EncodeCollectionTypedStringsPayload(p)
	if err != nil {
		t.Fatal(err)
	}
	p.Documents[0], p.Documents[1] = p.Documents[1], p.Documents[0]
	canonical, err := EncodeCollectionTypedStringsPayload(p)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatalf("canonical: %v", err)
	}
	decoded, err := DecodeCollectionTypedStringsPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range decoded.Documents {
		if cap(d.ID) != len(d.ID) || cap(d.Residual) != len(d.Residual) {
			t.Fatal("decoded command exposes byte backing beyond its admitted dimensions")
		}
	}
	frozen, err := DecodeCollectionTypedStringsPayload(bytes.Clone(raw))
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	p.Documents[1].ID[0] = 'z'
	clear(p.Documents[1].Residual)
	if !reflect.DeepEqual(decoded, frozen) {
		t.Fatal("decoded command aliases input")
	}
	for name, mutate := range map[string]func([]byte){
		"huge_docs":       func(b []byte) { binary.LittleEndian.PutUint32(b[14:], ^uint32(0)) },
		"zero_schema":     func(b []byte) { binary.LittleEndian.PutUint32(b[10:], 0) },
		"huge_id":         func(b []byte) { binary.LittleEndian.PutUint32(b[24:], ^uint32(0)) },
		"huge_edits":      func(b []byte) { binary.LittleEndian.PutUint32(b[61:], ^uint32(0)) },
		"out_of_schema":   func(b []byte) { binary.LittleEndian.PutUint32(b[65:], 4) },
		"invalid_version": func(b []byte) { binary.LittleEndian.PutUint16(b, 2) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := bytes.Clone(canonical)
			mutate(bad)
			if _, err := DecodeCollectionTypedStringsPayload(bad); err == nil {
				t.Fatal("accepted malformed command")
			}
		})
	}
	for n := 0; n < len(canonical); n++ {
		if _, err := DecodeCollectionTypedStringsPayload(canonical[:n]); err == nil {
			t.Fatalf("accepted prefix%d", n)
		}
	}
	if _, err := DecodeCollectionTypedStringsPayload(append(bytes.Clone(canonical), 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	p.Documents = []CollectionTypedStringPatch{{ID: []byte("a"), Generation: 1, PartID: 1, AppliedCommandLSN: 2}}
	if _, err := EncodeCollectionTypedStringsPayload(p); err == nil {
		t.Fatal("encoded logical noop")
	}
}
