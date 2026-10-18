package collections

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func nativeTestTypedRowSchema(t *testing.T, admit cowbtree.OwnedMapAdmit, normalized ...bool) *nativeSchemaState {
	t.Helper()
	meta := CollectionMeta{Name: "typed", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &ColumnStoreConfig{
		Enabled: true, SchemaHash: 17, RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		Columns: []ColumnStoreColumn{{Name: "name", Path: "name", ValueType: ColumnStoreValueString}, {Name: "vector", Path: "vector", ValueType: ColumnStoreValueFloat32Vector, Owner: TypedStorageOwnerColumnPart, VectorDims: 3}},
	}}, VectorIndexes: []VectorIndexDefinition{{Name: "graph", Field: "vector", Dimensions: 3, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}}
	if len(normalized) != 0 && normalized[0] {
		meta.VectorIndexes[0].Representation = VectorIndexRepresentationCosineNormalizedF32V1
	}
	schema, err := newNativeSchemaState(&meta, admit)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func nativeTestTypedDeclaredRow() columnDeclaredRow {
	return columnDeclaredRow{ID: []byte("document"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "value"}, {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 2, 3}}}}
}

func nativeTestFinishTypedRow(t *testing.T, row **nativeTypedRow) {
	t.Helper()
	done, err := (nativeTypedRowLifetime{}).Release(*row)
	if err != nil {
		t.Fatal(err)
	}
	*row = nil
	done.Complete()
}

func TestNativeTypedRowOwnsIdentityFreeBackingAndSchema(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema := nativeTestTypedRowSchema(t, admit)
	input := nativeTestTypedDeclaredRow()
	padding := strings.Repeat("unretained", 1000) + "value"
	input.Values[0].String = padding[len(padding)-5:]
	retained := []byte(`{"other":9}`)
	row, err := newNativeTypedRow(schema, input, retained, admit)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.StringData(input.Values[0].String) == unsafe.StringData(row.row.Values[0].String) ||
		unsafe.StringData(string(input.Values[0].Type)) == unsafe.StringData(string(row.row.Values[0].Type)) {
		t.Fatal("typed row retained caller string backing")
	}
	input.ID[0] = 'x'
	input.Values[0].String = "changed"
	input.Values[1].Float32Vector[0] = 99
	retained[0] = 'x'
	if row.key != "document" || string(row.row.ID) != "document" || row.row.Preserved != nil || row.row.Deleted ||
		row.row.Values[0].String != "value" || row.row.Values[1].Float32Vector[0] != 1 || string(row.retained) != `{"other":9}` {
		t.Fatal("caller changed original identity-free row")
	}
	if done := schema.release(); schema.refs.Load() != 1 {
		t.Fatal("row lost independent immutable schema edge", done)
	}
	schema = nil
	budget.Close()
	before := budget.Stats()
	if allocations := testing.AllocsPerRun(100, func() {
		(nativeTypedRowLifetime{}).Retain(row)
		done, err := (nativeTypedRowLifetime{}).Release(row)
		if err != nil || row.row.Values[0].String != "value" {
			t.Fatal(err)
		}
		done.Complete()
	}); allocations != 0 || budget.Stats() != before {
		t.Fatal("row retain/read admitted after Close", allocations)
	}
	done, err := (nativeTypedRowLifetime{}).Release(row)
	if err != nil || row.schema != nil || row.key != "" || row.retained != nil || !reflect.ValueOf(row.row).IsZero() || budget.Stats().TotalBytes != before.TotalBytes {
		t.Fatal("row refunded before containing aliases detached", err)
	}
	row = nil
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeTypedRowRefusesInvalidOrUnadmittedStage(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema := nativeTestTypedRowSchema(t, admit)
	before := budget.Stats()
	calls := 0
	refused := errors.New("original typed staging admission refused")
	refuse := func(uint64) (cowbtree.OwnedMapLease, error) { calls++; return nil, refused }
	input := nativeTestTypedDeclaredRow()
	if allocations := testing.AllocsPerRun(100, func() {
		row, err := newNativeTypedRow(schema, input, []byte(`{}`), refuse)
		if row != nil || err != refused {
			t.Fatal("admission changed first refusal", err)
		}
	}); allocations != 0 || budget.Stats() != before || schema.refs.Load() != 1 {
		t.Fatal("refused typed stage allocated or retained", allocations)
	}
	cases := []struct {
		name string
		edit func(*columnDeclaredRow)
	}{
		{"id", func(row *columnDeclaredRow) { row.ID = nil }},
		{"physical-coordinates", func(row *columnDeclaredRow) { row.Preserved = &columnRowCoordinates{} }},
		{"missing-column", func(row *columnDeclaredRow) { row.Values = row.Values[:1] }},
		{"wrong-type", func(row *columnDeclaredRow) { row.Values[0].Type = ColumnStoreValueInt64 }},
		{"null", func(row *columnDeclaredRow) { row.Values[0].Null = true }},
		{"absent", func(row *columnDeclaredRow) { row.Values[0].Present = false }},
		{"borrowed-string", func(row *columnDeclaredRow) { row.Values[0].StringBytes = []byte("asset") }},
		{"hidden-carrier", func(row *columnDeclaredRow) { row.Values[0].Bytes = []byte("hidden") }},
		{"hidden-scalar", func(row *columnDeclaredRow) { row.Values[0].Int64 = 1 }},
		{"utf8", func(row *columnDeclaredRow) { row.Values[0].String = string([]byte{255}) }},
		{"dimensions", func(row *columnDeclaredRow) { row.Values[1].Float32Vector = []float32{1} }},
		{"nan", func(row *columnDeclaredRow) { row.Values[1].Float32Vector[0] = float32(math.NaN()) }},
		{"zero-cosine", func(row *columnDeclaredRow) { row.Values[1].Float32Vector = []float32{0, 0, 0} }},
		{"delete-values", func(row *columnDeclaredRow) { row.Deleted = true }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			input := nativeTestTypedDeclaredRow()
			test.edit(&input)
			oldCalls := calls
			row, err := newNativeTypedRow(schema, input, nil, refuse)
			if row != nil || err == nil || calls != oldCalls || budget.Stats() != before || schema.refs.Load() != 1 {
				t.Fatal("invalid stage reached admission or changed old state", err)
			}
		})
	}
	budget.Close()
	if row, err := newNativeTypedRow(schema, nativeTestTypedDeclaredRow(), nil, admit); row != nil || !errors.Is(err, memtable.ErrCOWClosed) {
		t.Fatal("closed budget created typed stage", err)
	}
	done := schema.release()
	schema = nil
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeTypedRowOverlayNewestTombstoneAndRecreation(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema := nativeTestTypedRowSchema(t, admit)
	var views []*cowbtree.OwnedMap[string, *nativeTypedRow]
	var current *cowbtree.OwnedMap[string, *nativeTypedRow]
	for _, value := range []string{"old", "updated", "", "recreated"} {
		input := nativeTestTypedDeclaredRow()
		input.Values[0].String = value
		if value == "" {
			input.Deleted, input.Values = true, nil
		}
		row, err := newNativeTypedRow(schema, input, nil, admit)
		if err != nil {
			t.Fatal(err)
		}
		builder, err := cowbtree.NewOwnedMapBuilder(current, 1, nativeTypedRowLifetime{}, admit)
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Set(row.key, row); err != nil {
			t.Fatal(err)
		}
		current = builder.Seal()
		builder = nil
		nativeTestFinishTypedRow(t, &row)
		views = append(views, current)
	}
	done := schema.release()
	schema = nil
	done.Complete()
	budget.Close()
	expected := [...]string{"old", "updated", "", "recreated"}
	for i, view := range views {
		row, found := view.Get("document")
		if !found || row.row.Preserved != nil || string(row.row.ID) != "document" || row.row.Deleted != (i == 2) {
			t.Fatal("typed tombstone/identity precedence changed", i)
		}
		if !row.row.Deleted && row.row.Values[0].String != expected[i] {
			t.Fatal("newer typed stage changed old view", i)
		}
	}
	current = nil
	for i := range views {
		view := views[i]
		views[i] = nil
		if !view.Release() {
			t.Fatal("unexpected extra typed overlay owner")
		}
		done, err := view.Finish()
		if err != nil {
			t.Fatal(err)
		}
		view = nil
		done.Complete()
	}
	views = nil
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeTypedRowPreservesCanonicalVectorBits(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
	schema := nativeTestTypedRowSchema(t, admit, true)
	input := nativeTestTypedDeclaredRow()
	before := budget.Stats()
	calls := 0
	if row, err := newNativeTypedRow(schema, input, nil, func(n uint64) (cowbtree.OwnedMapLease, error) {
		calls++
		return admit(n)
	}); row != nil || err == nil || calls != 0 || budget.Stats() != before {
		t.Fatal("uncanonical vector reached admission", err)
	}
	input.Values[1].Float32Vector, err = normalizeCosineNormalizedF32V1(input.Values[1].Float32Vector, 3)
	if err != nil {
		t.Fatal(err)
	}
	row, err := newNativeTypedRow(schema, input, nil, admit)
	if err != nil {
		t.Fatal(err)
	}
	for i, value := range input.Values[1].Float32Vector {
		if math.Float32bits(value) != math.Float32bits(row.row.Values[1].Float32Vector[i]) {
			t.Fatal("staging normalized canonical bytes again", i)
		}
	}
	nativeTestFinishTypedRow(t, &row)
	done := schema.release()
	schema = nil
	done.Complete()
	budget.Close()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}
