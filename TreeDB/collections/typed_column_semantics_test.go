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
		semanticField("tags", ColumnStoreValueUint32List),
		semanticField("opaque", ColumnStoreValueBytes),
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
	for _, op := range []columnsemantics.Operation{columnsemantics.OpVectorSimilarity, columnsemantics.OpVectorDotProduct, columnsemantics.OpVectorDirectPayload, columnsemantics.OpVectorMetrics} {
		if cap, _ := typedColumnAdapterCapability(vectorColumn, op); cap.Status != columnsemantics.StatusSupported || cap.Reason != columnsemantics.ReasonSupported {
			t.Fatalf("vector %s capability=%+v want supported", op, cap)
		}
	}
	if cap, _ := typedColumnAdapterCapability(vectorColumn, columnsemantics.OpSum); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonVectorScalarOperationUnsupported {
		t.Fatalf("vector sum capability=%+v", cap)
	}

	adjacencyColumn, err := typedColumnAdapterMapField(semanticAdjacencyField("neighbors"))
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpAdjacencyTraversal, columnsemantics.OpAdjacencyMetrics} {
		if cap, _ := typedColumnAdapterCapability(adjacencyColumn, op); cap.Status != columnsemantics.StatusSupported || cap.Reason != columnsemantics.ReasonSupported {
			t.Fatalf("adjacency %s capability=%+v want supported", op, cap)
		}
	}
	if cap, _ := typedColumnAdapterCapability(adjacencyColumn, columnsemantics.OpAdjacencyDirectPayload); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonAdjacencyCapabilityDeferred {
		t.Fatalf("dense adjacency direct payload capability=%+v want deferred fallback", cap)
	}
	offsetsAdjacencyColumn, err := typedColumnAdapterMapField(semanticAdjacencyOffsetsListField("neighbors_offsets"))
	if err != nil {
		t.Fatal(err)
	}
	if cap, _ := typedColumnAdapterCapability(offsetsAdjacencyColumn, columnsemantics.OpAdjacencyDirectPayload); cap.Status != columnsemantics.StatusSupported || cap.Reason != columnsemantics.ReasonSupported {
		t.Fatalf("offsets-list adjacency direct payload capability=%+v want supported", cap)
	}
	if cap, _ := typedColumnAdapterCapability(adjacencyColumn, columnsemantics.OpOrderedRange); cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonAdjacencyScalarOperationUnsupported {
		t.Fatalf("adjacency range capability=%+v", cap)
	}
}

func TestTypedColumnSemanticAdapterFloatFallbackContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		valueType ColumnStoreValueType
	}{
		{name: "float32", valueType: ColumnStoreValueFloat32},
		{name: "double", valueType: ColumnStoreValueDouble},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column, err := typedColumnAdapterMapField(semanticField("score", tc.valueType))
			if err != nil {
				t.Fatal(err)
			}
			if column.Definition.Type != typedcolumn.ColumnTypeInt64 || column.Definition.Encoding != typedcolumn.EncodingRawInt64 || !column.Definition.StatsDisabled {
				t.Fatalf("float column definition=%+v want raw int64 carrier with stats disabled", column.Definition)
			}
			for _, op := range []columnsemantics.Operation{columnsemantics.OpEquality, columnsemantics.OpInequality, columnsemantics.OpInList} {
				cap, err := typedColumnAdapterCapability(column, op)
				if err != nil || cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonNativeFloatLayoutMissing || !strings.Contains(cap.Message, "NaN") || !strings.Contains(cap.Message, "signed-zero") || !strings.Contains(cap.Message, "infinity") || !strings.Contains(cap.Message, "precision/accumulation") {
					t.Fatalf("%s %s capability=%+v err=%v want explicit float fallback", tc.name, op, cap, err)
				}
			}
			for _, op := range []columnsemantics.Operation{columnsemantics.OpOrderedRange, columnsemantics.OpSum, columnsemantics.OpAvg, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpStatsMinMax, columnsemantics.OpStatsSum, columnsemantics.OpPruneEquality, columnsemantics.OpPruneOrderedRange, columnsemantics.OpDirectScalarValueCarrier} {
				cap, err := typedColumnAdapterCapability(column, op)
				if err != nil || cap.Status != columnsemantics.StatusUnsupported || cap.Reason != columnsemantics.ReasonFloatRawInt64BitPattern {
					t.Fatalf("%s %s capability=%+v err=%v want raw-bit fail closed", tc.name, op, cap, err)
				}
				requireErr := requireTypedColumnAdapterCapability(column, op, "float fail-closed")
				if !errors.Is(requireErr, ErrColumnQueryPlanUnsupported) || !strings.Contains(requireErr.Error(), string(columnsemantics.ReasonFloatRawInt64BitPattern)) {
					t.Fatalf("%s %s require err=%v want semantic capability rejection", tc.name, op, requireErr)
				}
			}
			for _, op := range []columnsemantics.Operation{columnsemantics.OpCountRows, columnsemantics.OpCountNonNull} {
				cap, err := typedColumnAdapterCapability(column, op)
				if err != nil || cap.Status != columnsemantics.StatusSupported || cap.Result.ResultType != "int64" {
					t.Fatalf("%s %s capability=%+v err=%v want supported count", tc.name, op, cap, err)
				}
			}
		})
	}
}

func TestTypedColumnSemanticAdapterNullableFloatFallbackContract(t *testing.T) {
	for _, valueType := range []ColumnStoreValueType{ColumnStoreValueFloat32, ColumnStoreValueDouble} {
		field := semanticField("maybe_score", valueType)
		field.Nullable = true
		column, err := typedColumnAdapterMapField(field)
		if err != nil {
			t.Fatalf("typedColumnAdapterMapField(%s): %v", valueType, err)
		}
		if column.Definition.Encoding != typedcolumn.EncodingNullableInt64 || !column.Definition.StatsDisabled {
			t.Fatalf("nullable float definition=%+v want nullable carrier with stats disabled", column.Definition)
		}
		for _, op := range []columnsemantics.Operation{columnsemantics.OpCountRows, columnsemantics.OpCountNonNull, columnsemantics.OpIsNull, columnsemantics.OpIsNotNull} {
			if cap, _ := typedColumnAdapterCapability(column, op); cap.Status != columnsemantics.StatusSupported {
				t.Fatalf("%s %s capability=%+v want supported count/null operation", valueType, op, cap)
			}
		}
		for _, op := range []columnsemantics.Operation{columnsemantics.OpEquality, columnsemantics.OpOrderedRange, columnsemantics.OpDirectScalarValueCarrier} {
			if cap, _ := typedColumnAdapterCapability(column, op); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonNullableCarrierValueSemantics {
				t.Fatalf("%s %s capability=%+v want nullable value fallback", valueType, op, cap)
			}
		}
		for _, op := range []columnsemantics.Operation{columnsemantics.OpSum, columnsemantics.OpAvg, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpStatsMinMax, columnsemantics.OpStatsSum, columnsemantics.OpPruneEquality, columnsemantics.OpPruneOrderedRange} {
			if cap, _ := typedColumnAdapterCapability(column, op); cap.Status != columnsemantics.StatusFallback || cap.Reason != columnsemantics.ReasonNullableCarrierAggregateSemantics {
				t.Fatalf("%s %s capability=%+v want nullable aggregate fallback", valueType, op, cap)
			}
		}
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

func TestTypedColumnAdapterPrepareInt64SemanticCapabilityRejectsVectorAndAdjacency(t *testing.T) {
	for _, tc := range []struct {
		name   string
		field  TypedStorageField
		reason columnsemantics.ReasonCode
	}{
		{name: "vector", field: semanticVectorField("embedding"), reason: columnsemantics.ReasonVectorScalarOperationUnsupported},
		{name: "adjacency", field: semanticAdjacencyField("neighbors"), reason: columnsemantics.ReasonAdjacencyScalarOperationUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := typedColumnAdapterPrepareInt64PredicateScanPart([]TypedStorageField{tc.field}, nil, 0, 0, 0, 0, tc.field.Name)
			if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(tc.reason)) {
				t.Fatalf("prepare %s-as-int64 scan err=%v want %s", tc.name, err, tc.reason)
			}
			_, _, _, err = typedColumnAdapterPrepareInt64PredicateAggregatePart([]TypedStorageField{tc.field}, nil, 0, 0, 0, 0, tc.field.Name)
			if !errors.Is(err, ErrColumnQueryPlanUnsupported) || !strings.Contains(err.Error(), string(tc.reason)) {
				t.Fatalf("prepare %s-as-int64 aggregate err=%v want %s", tc.name, err, tc.reason)
			}
		})
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

func semanticAdjacencyOffsetsListField(name string) TypedStorageField {
	field := semanticField(name, ColumnStoreValueAdjacencyList)
	field.AdjacencyLayout = ColumnAdjacencyListLayoutUint32OffsetsList
	return field
}
