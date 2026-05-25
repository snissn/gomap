package columnsemantics

import (
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestSemanticMatrixCoversCurrentLogicalTypesColumnTypesAndEncodings(t *testing.T) {
	for _, logical := range []LogicalType{LogicalBool, LogicalInt64, LogicalFloat32, LogicalDouble, LogicalString, LogicalFloat32Vector, LogicalAdjacencyList} {
		if !IsKnownLogicalType(logical) {
			t.Fatalf("logical type %q not covered", logical)
		}
	}
	for _, columnType := range []typedcolumn.ColumnType{typedcolumn.ColumnTypeInt64, typedcolumn.ColumnTypeLowCardinalityCode, typedcolumn.ColumnTypeBool, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.ColumnTypeAdjacencyList} {
		if !slices.Contains(ColumnTypes(), columnType) || !IsKnownColumnType(columnType) {
			t.Fatalf("typedcolumn column type %q not covered", columnType)
		}
	}
	for _, encoding := range []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint, typedcolumn.EncodingNullableInt64, typedcolumn.EncodingBoolBitpackRLE, typedcolumn.EncodingLowCardinalityUint32, typedcolumn.EncodingRawFloat32Vector, typedcolumn.EncodingRawUint32Dense} {
		if !slices.Contains(Encodings(), encoding) || !IsKnownEncoding(encoding) {
			t.Fatalf("typedcolumn encoding %s not covered", encoding)
		}
	}
}

func TestCapabilityInt64SupportsPreparedPredicateAndAggregateSemantics(t *testing.T) {
	desc := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint}
	for _, op := range []Operation{OpAllRows, OpEquality, OpOrderedRange, OpCountRows, OpCountNonNull, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
			t.Fatalf("op %s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
}

func TestCapabilityFloatRawInt64DoesNotClaimInt64NumericSemantics(t *testing.T) {
	for _, logical := range []LogicalType{LogicalFloat32, LogicalDouble} {
		desc := Descriptor{Logical: logical, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64}
		for _, op := range []Operation{OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != ReasonFloatRawInt64BitPattern {
				t.Fatalf("%s %s status=%s reason=%s", logical, op, cap.Status, cap.Reason)
			}
		}
	}
}

func TestCapabilityStringDictionaryCodesDoNotImplyLexicalRangeWithoutProof(t *testing.T) {
	desc := Descriptor{Logical: LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32}
	if cap := CapabilityFor(desc, OpDictionaryEquality); cap.Status != StatusSupported {
		t.Fatalf("dictionary equality status=%s reason=%s", cap.Status, cap.Reason)
	}
	for _, op := range []Operation{OpOrderedRange, OpDictionaryRange, OpStringPrefix, OpStringLexicalRange, OpPruneOrderedRange} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusUnsupported || cap.Reason != ReasonDictionaryOrderUnproven {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	desc.DictionaryOrder = true
	if cap := CapabilityFor(desc, OpStringLexicalRange); cap.Reason != ReasonDictionaryCollationUnproven {
		t.Fatalf("ordered dictionary without collation reason=%s", cap.Reason)
	}
	desc.DictionaryCollation = "unicode-codepoint-v1"
	if cap := CapabilityFor(desc, OpStringLexicalRange); cap.Status != StatusSupported {
		t.Fatalf("ordered dictionary with collation status=%s reason=%s", cap.Status, cap.Reason)
	}
}

func TestCapabilityNullableCarrierDistinguishesCountAndValueAggregateSemantics(t *testing.T) {
	desc := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Nullable: true}
	for _, op := range []Operation{OpCountRows, OpCountNonNull, OpIsNull, OpIsNotNull} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	for _, op := range []Operation{OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusFallback || cap.Reason != ReasonNullableCarrierAggregateSemantics {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	if cap := CapabilityFor(desc, OpEquality); cap.Status != StatusFallback || cap.Reason != ReasonNullableCarrierValueSemantics {
		t.Fatalf("equality status=%s reason=%s", cap.Status, cap.Reason)
	}
}

func TestCapabilityVectorAndAdjacencyRejectScalarShortcutSemantics(t *testing.T) {
	cases := []struct {
		name   string
		desc   Descriptor
		reason ReasonCode
	}{
		{"vector", Descriptor{Logical: LogicalFloat32Vector, Physical: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector}, ReasonVectorScalarOperationUnsupported},
		{"adjacency", Descriptor{Logical: LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense}, ReasonAdjacencyScalarOperationUnsupported},
	}
	for _, tc := range cases {
		for _, op := range []Operation{OpEquality, OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != tc.reason {
				t.Fatalf("%s %s status=%s reason=%s", tc.name, op, cap.Status, cap.Reason)
			}
		}
	}
}

func TestCapabilityReasonCodesAreStable(t *testing.T) {
	checks := map[ReasonCode]string{
		ReasonFloatRawInt64BitPattern:             "float_raw_int64_bit_pattern",
		ReasonDictionaryOrderUnproven:             "dictionary_order_unproven",
		ReasonNullableCarrierAggregateSemantics:   "nullable_carrier_aggregate_semantics",
		ReasonVectorScalarOperationUnsupported:    "vector_scalar_operation_unsupported",
		ReasonAdjacencyScalarOperationUnsupported: "adjacency_scalar_operation_unsupported",
	}
	for got, want := range checks {
		if string(got) != want {
			t.Fatalf("reason code changed: got %q want %q", got, want)
		}
	}
}
