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
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported || cap.Phase != PhasePrepare {
			t.Fatalf("op %s status=%s reason=%s phase=%s", op, cap.Status, cap.Reason, cap.Phase)
		}
	}
	if cap := CapabilityFor(desc, OpStatsSum); cap.Status != StatusUnsupported || cap.Reason != ReasonStatsPayloadUnsupported {
		t.Fatalf("stats sum status=%s reason=%s", cap.Status, cap.Reason)
	}
}

func TestCapabilityInt64AggregateResultSemanticsAreExplicit(t *testing.T) {
	desc := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint}
	checks := map[Operation]ResultSemantics{
		OpCountRows:    {ResultType: "int64", OverflowPolicy: "checked row count"},
		OpCountNonNull: {ResultType: "int64", OverflowPolicy: "checked row count"},
		OpSum:          {ResultType: "int64", Accumulator: "int64", OverflowPolicy: "checked"},
		OpAvg:          {ResultType: "float64", Accumulator: "checked int64 sum and int64 count", OverflowPolicy: "checked sum", Precision: "float64 quotient"},
		OpMin:          {ResultType: "int64", Comparison: "signed int64 logical order"},
		OpMax:          {ResultType: "int64", Comparison: "signed int64 logical order"},
	}
	for op, want := range checks {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusSupported || cap.Result != want {
			t.Fatalf("%s capability=%+v want result=%+v", op, cap, want)
		}
	}
}

func TestCapabilityBoolSupportsEqualityButRejectsRangeSemantics(t *testing.T) {
	desc := Descriptor{Logical: LogicalBool, Physical: typedcolumn.ColumnTypeBool, Encoding: typedcolumn.EncodingBoolBitpackRLE}
	for _, op := range []Operation{OpAllRows, OpEquality, OpInequality, OpInList, OpBoolCounts, OpDirectScalarValueCarrier} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusSupported || cap.Reason != ReasonSupported || cap.Phase != PhasePrepare {
			t.Fatalf("%s capability=%+v", op, cap)
		}
	}
	for _, op := range []Operation{OpOrderedRange, OpPruneOrderedRange, OpMin, OpMax, OpStatsMinMax} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusUnsupported || cap.Reason != ReasonBoolRangeUnsupported {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
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
		for _, op := range []Operation{OpCountRows, OpCountNonNull} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusSupported || cap.Result.ResultType != "int64" {
				t.Fatalf("%s %s capability=%+v", logical, op, cap)
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

	// The nullable carrier encoding itself is sufficient to force nullable/default
	// semantics even if a direct caller forgets to set Descriptor.Nullable.
	desc.Nullable = false
	if cap := CapabilityFor(desc, OpSum); cap.Status != StatusFallback || cap.Reason != ReasonNullableCarrierAggregateSemantics {
		t.Fatalf("nullable encoding sum status=%s reason=%s", cap.Status, cap.Reason)
	}

	// A nullable descriptor with a non-nullable physical encoding is inconsistent
	// and must not silently route to nullable semantics.
	inconsistent := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Nullable: true}
	if cap := CapabilityFor(inconsistent, OpSum); cap.Status != StatusUnsupported || cap.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("inconsistent nullable descriptor status=%s reason=%s", cap.Status, cap.Reason)
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
		for _, op := range []Operation{OpCountRows, OpCountNonNull} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusSupported || cap.Result.ResultType != "int64" {
				t.Fatalf("%s %s capability=%+v", tc.name, op, cap)
			}
		}
		for _, op := range []Operation{OpEquality, OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != tc.reason {
				t.Fatalf("%s %s status=%s reason=%s", tc.name, op, cap.Status, cap.Reason)
			}
		}
	}
}

func TestCapabilityVectorAndAdjacencySpecificOperationsAreExplicitFallbacks(t *testing.T) {
	checks := []struct {
		name   string
		desc   Descriptor
		reason ReasonCode
	}{
		{"vector", Descriptor{Logical: LogicalFloat32Vector, Physical: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector}, ReasonVectorCapabilityDeferred},
		{"adjacency", Descriptor{Logical: LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense}, ReasonAdjacencyCapabilityDeferred},
	}
	for _, tc := range checks {
		for _, op := range []Operation{OpVectorSimilarity, OpVectorMetrics} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusFallback || cap.Reason != tc.reason || cap.Phase != PhasePrepare {
				t.Fatalf("%s %s capability=%+v", tc.name, op, cap)
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
		ReasonStatsPayloadUnsupported:             "stats_payload_unsupported",
	}
	for got, want := range checks {
		if string(got) != want {
			t.Fatalf("reason code changed: got %q want %q", got, want)
		}
	}
}
