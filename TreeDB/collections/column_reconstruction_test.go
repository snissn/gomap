package collections

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func TestColumnDocumentReconstructionArenaDirectAppend1888(t *testing.T) {
	cfg := columnReconstructionArenaTestConfig1888()
	arena := []byte("prefix|")
	row := columnPhysicalVisibleRow{}
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 42},
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
		{Type: ColumnStoreValueString, Present: true, String: "kept"},
	}

	out, doc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena, cfg, []byte(`{"payload":"retained"}`), row, values, nil, nil)
	if err != nil {
		t.Fatalf("reconstruct into arena: %v", err)
	}
	if !bytes.Equal(out[:len(arena)], arena) {
		t.Fatalf("arena prefix=%q want %q", out[:len(arena)], arena)
	}
	if !bytes.Equal(doc, out[len(arena):]) {
		t.Fatalf("doc=%q want arena tail %q", doc, out[len(arena):])
	}
	if cap(doc) != len(doc) {
		t.Fatalf("doc cap=%d len=%d want cap-limited slice", cap(doc), len(doc))
	}
	assertJSONMapEqual1875(t, doc, map[string]any{"row_id": float64(42), "kind": "alpha", "note": "kept", "payload": "retained"})
}

func TestColumnDocumentReconstructionArenaCapLimitedResponseOwned1888(t *testing.T) {
	cfg := columnReconstructionArenaTestConfig1888()
	var arena []byte
	row := columnPhysicalVisibleRow{}
	valuesA := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
		{Type: ColumnStoreValueString, Present: true, String: "first"},
	}
	valuesB := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 2},
		{Type: ColumnStoreValueString, Present: true, String: "beta"},
		{Type: ColumnStoreValueString, Present: true, String: "second"},
	}

	var first, second []byte
	var err error
	arena, first, err = reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena, cfg, []byte(`{"payload":"one"}`), row, valuesA, nil, nil)
	if err != nil {
		t.Fatalf("first reconstruct: %v", err)
	}
	arena, second, err = reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena, cfg, []byte(`{"payload":"two"}`), row, valuesB, nil, nil)
	if err != nil {
		t.Fatalf("second reconstruct: %v", err)
	}
	if cap(first) != len(first) || cap(second) != len(second) {
		t.Fatalf("document caps first=%d/%d second=%d/%d want cap-limited", len(first), cap(first), len(second), cap(second))
	}
	secondBefore := append([]byte(nil), second...)
	_ = append(first, '!')
	if !bytes.Equal(second, secondBefore) {
		t.Fatalf("second document changed after append to first: got %s want %s", second, secondBefore)
	}
	if len(arena) <= len(first) {
		t.Fatalf("arena len=%d want both documents retained", len(arena))
	}
}

func TestColumnDocumentReconstructionArenaRollbackOnError1888(t *testing.T) {
	cfg := columnReconstructionArenaTestConfig1888()
	arena := []byte("prefix")
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 1},
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
		{Type: ColumnStoreValueString, Present: true, String: "note"},
	}

	out, doc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena, cfg, []byte(`{"payload":`), columnPhysicalVisibleRow{}, values, nil, nil)
	if err == nil {
		t.Fatal("reconstruct malformed retained payload succeeded, want error")
	}
	if doc != nil {
		t.Fatalf("doc=%q want nil on error", doc)
	}
	if !bytes.Equal(out, arena) {
		t.Fatalf("arena after rollback=%q want %q", out, arena)
	}

	partialAppendCfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble}}}
	out, doc, err = reconstructColumnDocumentFromVisibleRowValuesProjectedInto(arena, partialAppendCfg, nil, columnPhysicalVisibleRow{}, []columnDeclaredValue{{Type: ColumnStoreValueDouble, Present: true, Double: math.NaN()}}, nil, nil)
	if err == nil {
		t.Fatal("reconstruct NaN value succeeded, want JSON marshal error")
	}
	if doc != nil {
		t.Fatalf("partial-append doc=%q want nil on error", doc)
	}
	if !bytes.Equal(out, arena) {
		t.Fatalf("arena after partial-append rollback=%q want %q", out, arena)
	}
}

func TestColumnDocumentReconstructionArenaEmptyRetainedSmallNullProjectedOutputs1888(t *testing.T) {
	cfg := columnReconstructionArenaTestConfig1888()
	row := columnPhysicalVisibleRow{}
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueInt64, Present: true, Int64: 7},
		{Type: ColumnStoreValueString, Present: true, String: "alpha"},
		{Type: ColumnStoreValueString, Present: true, Null: true},
	}

	nullProjection := &documentProjection{include: map[string]struct{}{"note": {}}}
	var nullStats DocumentMaterializationStats
	arena, doc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(nil, cfg, nil, row, values, nullProjection, &nullStats)
	if err != nil {
		t.Fatalf("null projection reconstruct: %v", err)
	}
	assertJSONMapEqual1875(t, doc, map[string]any{"note": nil})
	if nullStats.FieldsReconstructed != 1 || nullStats.FieldsSkipped != 2 {
		t.Fatalf("null projection stats=%+v want one reconstructed and two skipped fields", nullStats)
	}
	if cap(doc) != len(doc) || !bytes.Equal(doc, arena) {
		t.Fatalf("null projection doc not cap-limited arena tail: len/cap=%d/%d arena=%q doc=%q", len(doc), cap(doc), arena, doc)
	}

	emptyProjection := &documentProjection{include: map[string]struct{}{"missing": {}}}
	var emptyStats DocumentMaterializationStats
	_, emptyDoc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(nil, cfg, []byte("   "), row, values, emptyProjection, &emptyStats)
	if err != nil {
		t.Fatalf("empty projection reconstruct: %v", err)
	}
	if got, want := decodeJSONDocumentMap1875(t, emptyDoc), map[string]any{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("empty projection doc=%s decoded=%v want %v", emptyDoc, got, want)
	}
	if emptyStats.FieldsReconstructed != 0 || emptyStats.FieldsSkipped != 3 {
		t.Fatalf("empty projection stats=%+v want zero reconstructed and three skipped fields", emptyStats)
	}
}

func TestColumnDocumentReconstructionBytesAsIntegerArray2010(t *testing.T) {
	cfg := ColumnStoreConfig{Columns: []ColumnStoreColumn{
		{Name: "opaque", Path: "opaque", ValueType: ColumnStoreValueBytes, Owner: TypedStorageOwnerColumnPart},
		{Name: "empty", Path: "empty", ValueType: ColumnStoreValueBytes, Owner: TypedStorageOwnerColumnPart},
	}}
	values := []columnDeclaredValue{
		{Type: ColumnStoreValueBytes, Present: true, Bytes: []byte{0, 'A', 255}},
		{Type: ColumnStoreValueBytes, Present: true},
	}

	_, doc, err := reconstructColumnDocumentFromVisibleRowValuesProjectedInto(nil, cfg, []byte(`{"payload":"kept"}`), columnPhysicalVisibleRow{}, values, nil, nil)
	if err != nil {
		t.Fatalf("reconstruct bytes: %v", err)
	}
	assertJSONMapEqual1875(t, doc, map[string]any{
		"opaque":  []any{float64(0), float64(65), float64(255)},
		"empty":   []any{},
		"payload": "kept",
	})
}

func TestProjectJSONDocumentTopLevelProjectionParityAndAllocs4606(t *testing.T) {
	raw, projection := projectJSONDocumentFixture4608()
	source := append([]byte(nil), raw...)

	var stats DocumentMaterializationStats
	got, err := projectJSONDocument(raw, projection, &stats)
	if err != nil {
		t.Fatalf("projectJSONDocument: %v", err)
	}
	assertJSONMapEqual1875(t, got, map[string]any{
		"title":  "escaped\\ttext",
		"nested": map[string]any{"array": []any{float64(1), float64(2), float64(3)}, "object": map[string]any{"ok": true}},
		"none":   nil,
		"number": float64(1250),
	})
	if want := []byte(`{"nested":{"array":[1,2,3],"object":{"ok":true}},"none":null,"number":1.25e3,"title":"escaped\\ttext"}`); !bytes.Equal(got, want) {
		t.Fatalf("deterministic projection=%q want=%q", got, want)
	}
	if cap(got) > len(got)+64 {
		t.Fatalf("projected capacity=%d want bounded near output length=%d", cap(got), len(got))
	}
	if stats.FieldsReconstructed != 4 || stats.FieldsSkipped != 1 {
		t.Fatalf("stats=%+v want four reconstructed and one skipped", stats)
	}
	owned := append([]byte(nil), got...)
	raw[0] = '['
	if !bytes.Equal(got, owned) {
		t.Fatalf("projected document aliases caller input: got=%q want=%q", got, owned)
	}
	invalidUTF8 := append([]byte(`{"title":"`), 0xff, 0xff)
	invalidUTF8 = append(invalidUTF8, `","embedding":[1]}`...)
	projected, err := projectJSONDocument(invalidUTF8, projection, nil)
	if err != nil {
		t.Fatalf("project invalid UTF-8: %v", err)
	}
	if want := []byte(`{"title":"��"}`); !bytes.Equal(projected, want) {
		t.Fatalf("invalid UTF-8 projection=%q want=%q", projected, want)
	}
	mixedInvalid := append([]byte(`{"title":"`), 0xff)
	mixedInvalid = append(mixedInvalid, `\ud800","embedding":[1]}`...)
	projected, err = projectJSONDocument(mixedInvalid, projection, nil)
	if err != nil {
		t.Fatalf("project mixed invalid Unicode: %v", err)
	}
	if want := []byte(`{"title":"��"}`); !bytes.Equal(projected, want) {
		t.Fatalf("mixed invalid Unicode projection=%q want=%q", projected, want)
	}
	escapedSurrogate := []byte(`{"title":"\ud800","embedding":[1]}`)
	projected, err = projectJSONDocument(escapedSurrogate, projection, nil)
	if err != nil {
		t.Fatalf("project escaped surrogate: %v", err)
	}
	if want := []byte(`{"title":"�"}`); !bytes.Equal(projected, want) {
		t.Fatalf("escaped surrogate projection=%q want=%q", projected, want)
	}
	numericSurrogate := []byte(`{"nested":{"s":"\ud800","large":9007199254740993,"exp":1e400},"embedding":[1]}`)
	projected, err = projectJSONDocument(numericSurrogate, projection, nil)
	if err != nil {
		t.Fatalf("project escaped surrogate with numbers: %v", err)
	}
	if want := []byte(`{"nested":{"exp":1e400,"large":9007199254740993,"s":"�"}}`); !bytes.Equal(projected, want) {
		t.Fatalf("escaped surrogate numeric projection=%q want=%q", projected, want)
	}
	invalidKeyBytes := []byte{'{', '"', 0xff, 0xff, '"', ':', '1', ',', '"', 0xef, 0xbf, 0xbd, '"', ':', '2', ',', '"', 'e', 'm', 'b', 'e', 'd', 'd', 'i', 'n', 'g', '"', ':', '3', '}'}
	projected, err = projectJSONDocument(invalidKeyBytes, projection, nil)
	if err != nil {
		t.Fatalf("project invalid UTF-8 keys: %v", err)
	}
	if want := []byte(`{"�":2,"��":1}`); !bytes.Equal(projected, want) {
		t.Fatalf("invalid UTF-8 key projection=%q want=%q", projected, want)
	}
	projected, err = projectJSONDocument([]byte(`{"\ud800":1,"embedding":3}`), projection, nil)
	if err != nil {
		t.Fatalf("project lone surrogate key: %v", err)
	}
	if want := []byte(`{"�":1}`); !bytes.Equal(projected, want) {
		t.Fatalf("lone surrogate key projection=%q want=%q", projected, want)
	}
	for _, tc := range []struct {
		input string
		want  map[string]any
	}{
		{`{"nested":{"a":1,"a":2},"embedding":[1]}`, map[string]any{"nested": map[string]any{"a": float64(2)}}},
	} {
		projected, err := projectJSONDocument([]byte(tc.input), projection, nil)
		if err != nil {
			t.Fatalf("project semantic case %q: %v", tc.input, err)
		}
		assertJSONMapEqual1875(t, projected, tc.want)
	}

	allocs := testing.AllocsPerRun(100, func() {
		if _, err := projectJSONDocument(source, projection, nil); err != nil {
			panic(err)
		}
	})
	if allocs > 32 {
		t.Fatalf("top-level projection allocs/run=%0.0f want <=32", allocs)
	}

	for _, input := range [][]byte{[]byte(`{"title":1,"title":2,"embedding":3}`), []byte(`{"title":`), []byte(`{"title":1} {}`), []byte(`[]`)} {
		projected, err := projectJSONDocument(input, projection, nil)
		if bytes.Equal(input, []byte(`{"title":1,"title":2,"embedding":3}`)) {
			if err != nil {
				t.Fatalf("duplicate keys: %v", err)
			}
			assertJSONMapEqual1875(t, projected, map[string]any{"title": float64(2)})
			continue
		}
		if err == nil {
			t.Fatalf("invalid input %q projected successfully", input)
		}
	}
}

func BenchmarkProjectJSONDocument4608(b *testing.B) {
	raw, projection := projectJSONDocumentFixture4608()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		if _, err := projectJSONDocument(raw, projection, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func projectJSONDocumentFixture4608() ([]byte, *documentProjection) {
	raw := []byte(`{"title":"escaped\\ttext","embedding":[`)
	for i := 0; i < 64; i++ {
		if i != 0 {
			raw = append(raw, ',')
		}
		raw = append(raw, `{"n":1,"nested":[true,null,{"value":"x"}]}`...)
	}
	raw = append(raw, `],"nested":{"array":[1,2,3],"object":{"ok":true}},"none":null,"number":1.25e3}`...)
	return raw, &documentProjection{exclude: map[string]struct{}{"embedding": {}}}
}

func columnReconstructionArenaTestConfig1888() ColumnStoreConfig {
	return ColumnStoreConfig{Columns: []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "note", Path: "note", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true},
	}}
}
