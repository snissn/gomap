package collections

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestValidateBSONDocumentFastParity(t *testing.T) {
	validDoc := func(v any) []byte {
		b, err := bson.Marshal(v)
		if err != nil {
			t.Fatalf("bson.Marshal: %v", err)
		}
		return b
	}

	cases := []struct {
		name string
		raw  []byte
	}{
		{"empty_doc", validDoc(bson.D{})},
		{"scalars", validDoc(bson.D{
			{Key: "s", Value: "hello"},
			{Key: "i32", Value: int32(42)},
			{Key: "i64", Value: int64(-9999999999)},
			{Key: "f64", Value: 3.14159},
			{Key: "b1", Value: true},
			{Key: "b0", Value: false},
			{Key: "nil", Value: nil},
			{Key: "oid", Value: bson.NewObjectID()},
			{Key: "bin", Value: bson.Binary{Subtype: 0x00, Data: []byte{1, 2, 3}}},
		})},
		{"nested_doc_and_array", validDoc(bson.D{
			{Key: "nested", Value: bson.D{{Key: "a", Value: "x"}, {Key: "b", Value: int32(7)}}},
			{Key: "arr", Value: bson.A{"one", int32(2), true, bson.D{{Key: "k", Value: "v"}}}},
		})},
		{"regex_fallback", validDoc(bson.D{{Key: "re", Value: bson.Regex{Pattern: "abc", Options: "i"}}})},
		{"binary_subtype2_fallback", validDoc(bson.D{{Key: "b2", Value: bson.Binary{Subtype: 0x02, Data: []byte{1, 2, 3, 4}}}})},
		{"too_short", []byte{1, 2, 3}},
		{"bad_length_short", []byte{4, 0, 0, 0, 0}},
		{"bad_length_long", []byte{10, 0, 0, 0, 0}},
		{"missing_terminator", []byte{5, 0, 0, 0, 1}},
		{"trailing_bytes", append(validDoc(bson.D{{Key: "a", Value: int32(1)}}), 0x00)},
		{"invalid_bool_byte", func() []byte {
			b := validDoc(bson.D{{Key: "b", Value: true}})
			b[len(b)-2] = 0x02 // corrupt boolean value to 2
			return b
		}()},
		{"corrupt_string_nul", func() []byte {
			b := validDoc(bson.D{{Key: "s", Value: "abc"}})
			b[len(b)-2] = 'X' // corrupt trailing string NUL
			return b
		}()},
		{"corrupt_nested_doc", func() []byte {
			b := validDoc(bson.D{{Key: "n", Value: bson.D{{Key: "b", Value: true}}}})
			// Corrupt nested boolean value inside embedded document.
			idx := bytes.Index(b, []byte{'b', 0, 1})
			if idx < 0 {
				t.Fatalf("failed to locate nested bool")
			}
			b[idx+2] = 0x05
			return b
		}()},
		{"corrupt_string_zero_len", func() []byte {
			// type=0x02, key="a\x00", strLen=0 (invalid; must be >= 1)
			var buf []byte
			buf = append(buf, 0, 0, 0, 0)
			buf = append(buf, byte(bson.TypeString), 'a', 0, 0, 0, 0, 0, 0)
			binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)))
			return buf
		}()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refErr := bson.Raw(tc.raw).Validate()
			gotErr := validateBSONDocument(tc.raw)
			if (refErr == nil) != (gotErr == nil) {
				t.Fatalf("parity mismatch for %s: refErr=%v gotErr=%v", tc.name, refErr, gotErr)
			}
		})
	}
}

func TestPrepareBSONInsertDocumentsParallel(t *testing.T) {
	const n = 3000
	docs := make([][]byte, n)
	for i := 0; i < n; i++ {
		raw, err := bson.Marshal(bson.D{
			{Key: "id", Value: int32(i)},
			{Key: "email", Value: fmt.Sprintf("u%d@example.com", i)},
			{Key: "active", Value: i%2 == 0},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		docs[i] = raw
	}
	if _, _, _, _, err := prepareBSONInsertDocuments(docs); err != nil {
		t.Fatalf("prepareBSONInsertDocuments(valid): %v", err)
	}
	// Corrupt document at index 2501 and confirm deterministic error reporting.
	docs[2501] = append([]byte(nil), docs[2501]...)
	docs[2501][len(docs[2501])-1] = 0x7f
	_, _, _, _, err := prepareBSONInsertDocuments(docs)
	if err == nil {
		t.Fatalf("expected error on corrupted BSON doc")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("BSON document 2501")) {
		t.Fatalf("expected deterministic index 2501 in error, got: %v", err)
	}
}

func TestBSONOrderedIndexStateSinglePass32Indexes(t *testing.T) {
	const numIdx = 32
	docFields := make(bson.D, 0, numIdx+1)
	runtimes := make([]indexRuntime, 0, numIdx)
	for i := 0; i < numIdx; i++ {
		field := fmt.Sprintf("idx_f%02d", i)
		docFields = append(docFields, bson.E{Key: field, Value: fmt.Sprintf("val_%02d", i)})
		runtimes = append(runtimes, indexRuntime{
			def: indexDefinition{
				name:      field + "_1",
				valueType: IndexValueString,
			},
			path: []string{field},
		})
	}
	raw, err := bson.Marshal(docFields)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var encoder indexEncodeArena
	state, err := bsonOrderedIndexStateForDocumentWithArena(raw, runtimes, collectionOptions{documentFormat: DocumentFormatBSON}, &encoder)
	if err != nil {
		t.Fatalf("bsonOrderedIndexStateForDocumentWithArena(32): %v", err)
	}
	if len(state) != numIdx {
		t.Fatalf("len(state)=%d want %d", len(state), numIdx)
	}
	for i := 0; i < numIdx; i++ {
		if len(state[i]) != 1 {
			t.Fatalf("index %d entries=%d want 1", i, len(state[i]))
		}
		want := fmt.Sprintf("val_%02d\x00\x00", i)
		if got := string(state[i][0]); got != want {
			t.Fatalf("index %d got %q want %q", i, got, want)
		}
	}
}
