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
	if cap, _ := typedColumnAdapterCapability(stringColumn, columnsemantics.OpStringPrefix); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
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
	if cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonDictionaryOrderUnproven {
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
