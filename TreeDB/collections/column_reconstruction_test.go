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

func columnReconstructionArenaTestConfig1888() ColumnStoreConfig {
	return ColumnStoreConfig{Columns: []ColumnStoreColumn{
		{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "note", Path: "note", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Nullable: true},
	}}
}
