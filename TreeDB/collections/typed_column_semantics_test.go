package collections

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestTypedColumnSemanticAdapterCoversCurrentColumnStoreValueTypes(t *testing.T) {
	fields := []TypedStorageField{
		semanticField("flag", ColumnStoreValueBool),
		semanticField("count", ColumnStoreValueInt64),
		semanticField("score", ColumnStoreValueFloat32),
		semanticField("ratio", ColumnStoreValueDouble),
		semanticField("kind", ColumnStoreValueString),
		semanticVectorField("embedding"),
		semanticAdjacencyField("neighbors"),
	}
	for _, field := range fields {
		column, err := typedColumnAdapterMapField(field)
		if err != nil {
			t.Fatalf("typedColumnAdapterMapField(%s): %v", field.ValueType, err)
		}
		desc, err := typedColumnAdapterSemanticDescriptor(column)
		if err != nil {
			t.Fatalf("typedColumnAdapterSemanticDescriptor(%s): %v", field.ValueType, err)
		}
		if !columnsemantics.IsKnownLogicalType(desc.Logical) {
			t.Fatalf("value_type=%s logical=%s not covered", field.ValueType, desc.Logical)
		}
		if !columnsemantics.IsKnownColumnType(desc.Physical) || !columnsemantics.IsKnownEncoding(desc.Encoding) {
			t.Fatalf("value_type=%s descriptor=%+v not covered", field.ValueType, desc)
		}
	}
}

func TestTypedColumnSemanticAdapterDangerousCapabilitiesFailClosed(t *testing.T) {
	floatColumn, err := typedColumnAdapterMapField(semanticField("score", ColumnStoreValueFloat32))
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(floatColumn, columnsemantics.OpOrderedRange); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonFloatRawInt64BitPattern {
		t.Fatalf("float range capability=%+v", cap)
	}

	stringColumn, err := typedColumnAdapterMapField(semanticField("kind", ColumnStoreValueString))
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(stringColumn, columnsemantics.OpStringPrefix); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
		t.Fatalf("string prefix capability=%+v", cap)
	}

	nullable := semanticField("maybe_count", ColumnStoreValueInt64)
	nullable.Nullable = true
	nullableColumn, err := typedColumnAdapterMapField(nullable)
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(nullableColumn, columnsemantics.OpSum); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonNullableCarrierAggregateSemantics {
		t.Fatalf("nullable sum capability=%+v", cap)
	}
	if cap, _ := typedColumnAdapterCapability(nullableColumn, columnsemantics.OpCountNonNull); cap.Status != columnsemantics.StatusSupported {
		t.Fatalf("nullable count non-null capability=%+v", cap)
	}

	vectorColumn, err := typedColumnAdapterMapField(semanticVectorField("embedding"))
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(vectorColumn, columnsemantics.OpSum); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonVectorScalarOperationUnsupported {
		t.Fatalf("vector sum capability=%+v", cap)
	}

	adjacencyColumn, err := typedColumnAdapterMapField(semanticAdjacencyField("neighbors"))
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(adjacencyColumn, columnsemantics.OpOrderedRange); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonAdjacencyScalarOperationUnsupported {
		t.Fatalf("adjacency range capability=%+v", cap)
	}
}

func TestTypedColumnAdapterPrepareInt64SemanticCapabilityRejectsFloatRawInt64Carrier(t *testing.T) {
	field := semanticField("score", ColumnStoreValueFloat32)
	_, _, _, err := typedColumnAdapterPrepareInt64PredicateScanPart([]TypedStorageField{field}, nil, 0, 0, 0, 0, "score")
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(columnsemantics.ReasonFloatRawInt64BitPattern)) {
		t.Fatalf("prepare float-as-int64 scan err=%v", err)
	}
	_, _, _, err = typedColumnAdapterPrepareInt64PredicateAggregatePart([]TypedStorageField{field}, nil, 0, 0, 0, 0, "score")
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(columnsemantics.ReasonFloatRawInt64BitPattern)) {
		t.Fatalf("prepare float-as-int64 aggregate err=%v", err)
	}

	column, err := typedColumnAdapterMapField(semanticField("flag", ColumnStoreValueBool))
	if err != nil {
		t.Fatalf("typedColumnAdapterMapField: %v", err)
	}
	err = requireTypedColumnAdapterCapability(column, columnsemantics.OpOrderedRange, "bool predicate scan")
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), "bool ordering is not exposed as scalar range semantics") {
		t.Fatalf("semantic detail err=%v", err)
	}
}

func TestTypedColumnInt64PredicateSemanticOperationUnknownKindFailsClosed(t *testing.T) {
	op := typedColumnInt64PredicateSemanticOperation(TypedColumnInt64PredicateScanKind("future_kind"))
	if op == columnsemantics.OpOrderedRange || op != columnsemantics.OpUnknownPredicateKind {
		t.Fatalf("unknown predicate kind operation=%s", op)
	}
	desc := columnsemantics.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint}
	cap := columnsemantics.CapabilityFor(desc, op)
	if cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonOperationUnsupported {
		t.Fatalf("unknown predicate operation capability=%+v", cap)
	}
}

func TestTypedColumnAdapterPrepareInt64SemanticCapabilityRejectsNullableCarrier(t *testing.T) {
	field := semanticField("count", ColumnStoreValueInt64)
	field.Nullable = true
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 44, SchemaVersion: 777, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, []typedColumnAdapterRow{
		{PrimaryID: 0, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Int64: 10}}},
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"count": {Type: ColumnStoreValueInt64, Present: true, Null: true}}},
	})
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	_, _, _, err = typedColumnAdapterPrepareInt64PredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "count")
	if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(columnsemantics.ReasonNullableCarrierValueSemantics)) {
		t.Fatalf("prepare nullable int64 err=%v", err)
	}
}

func TestTypedColumnAdapterStringSemanticCapabilityMatrix(t *testing.T) {
	stringColumn, err := typedColumnAdapterMapField(semanticField("kind", ColumnStoreValueString))
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpEquality, columnsemantics.OpInList, columnsemantics.OpDictionaryEquality, columnsemantics.OpDictionaryInList, columnsemantics.OpDictionaryCategory, columnsemantics.OpDictionaryGroupBy, columnsemantics.OpDictionaryCount, columnsemantics.OpDictionaryCountDistinct} {
		if cap, _ := typedColumnAdapterCapability(stringColumn, op); cap.Status != columnsemantics.StatusSupported {
			t.Fatalf("%s capability=%+v want supported", op, cap)
		}
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpStringPrefix, columnsemantics.OpStringLexicalRange} {
		if cap, _ := typedColumnAdapterCapability(stringColumn, op); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
			t.Fatalf("%s capability=%+v want dictionary order proof fallback", op, cap)
		}
	}
	if cap, _ := typedColumnAdapterCapability(stringColumn, columnsemantics.OpDictionaryRange); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
		t.Fatalf("%s capability=%+v want dictionary order proof rejection", columnsemantics.OpDictionaryRange, cap)
	}
}

func TestTypedColumnAdapterPrepareStringSemanticCapabilityAllowsDictionaryEqualityOnly(t *testing.T) {
	field := semanticField("kind", ColumnStoreValueString)
	part, err := buildTypedColumnAdapterPart(typedColumnAdapterOptions{PartID: 45, SchemaVersion: 778, RowsPerGranule: 2, Fields: []TypedStorageField{field}}, []typedColumnAdapterRow{
		{PrimaryID: 0, Values: map[string]columnDeclaredValue{"kind": {Type: ColumnStoreValueString, Present: true, String: "alpha"}}},
		{PrimaryID: 1, Values: map[string]columnDeclaredValue{"kind": {Type: ColumnStoreValueString, Present: true, String: "beta"}}},
	})
	if err != nil {
		t.Fatalf("buildTypedColumnAdapterPart: %v", err)
	}
	image, err := part.buildImage()
	if err != nil {
		t.Fatalf("buildImage: %v", err)
	}
	prepared, err := typedColumnAdapterPrepareStringPredicateScanPart([]TypedStorageField{field}, image.Bytes, image.PartID, image.Rows, image.Rows, uint64(part.Part.Descriptor.SchemaVersion), "kind", "beta")
	if err != nil {
		t.Fatalf("typedColumnAdapterPrepareStringPredicateScanPart: %v", err)
	}
	if !prepared.QueryCodeFound || prepared.Column.Definition.Type != typedcolumn.ColumnTypeLowCardinalityCode {
		t.Fatalf("prepared=%+v", prepared)
	}
	cap, _ := typedColumnAdapterCapability(prepared.Column, columnsemantics.OpStringLexicalRange)
	if cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
		t.Fatalf("string lexical range capability=%+v", cap)
	}
}

func semanticField(name string, valueType ColumnStoreValueType) TypedStorageField {
	field := typedColumnAdapterField(name, valueType)
	field.Owner = TypedStorageOwnerColumnPart
	return field
}

func semanticVectorField(name string) TypedStorageField {
	field := semanticField(name, ColumnStoreValueFloat32Vector)
	field.VectorDims = 3
	return field
}

func semanticAdjacencyField(name string) TypedStorageField {
	field := semanticField(name, ColumnStoreValueAdjacencyList)
	field.AdjacencyDegree = 2
	return field
}
