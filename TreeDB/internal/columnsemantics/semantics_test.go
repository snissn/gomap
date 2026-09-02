package columnsemantics

import (
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestSemanticMatrixCoversCurrentLogicalTypesColumnTypesAndEncodings(t *testing.T) {
	for _, logical := range []LogicalType{LogicalBool, LogicalInt64, LogicalFloat32, LogicalDouble, LogicalString, LogicalInt8, LogicalUint8, LogicalInt16, LogicalUint16, LogicalInt32, LogicalUint32, LogicalUint64, LogicalFloat16, LogicalBFloat16, LogicalUint8Vector, LogicalInt8Vector, LogicalUint16Vector, LogicalInt16Vector, LogicalUint32Vector, LogicalInt32Vector, LogicalUint64Vector, LogicalInt64Vector, LogicalFloat16Vector, LogicalBFloat16Vector, LogicalFloat32Vector, LogicalFloat64Vector, LogicalUint32List, LogicalBytes, LogicalAdjacencyList} {
		if !IsKnownLogicalType(logical) {
			t.Fatalf("logical type %q not covered", logical)
		}
	}
	for _, columnType := range []typedcolumn.ColumnType{typedcolumn.ColumnTypeInt64, typedcolumn.ColumnTypeLowCardinalityCode, typedcolumn.ColumnTypeBool, typedcolumn.ColumnTypeFloat32, typedcolumn.ColumnTypeFloat64, typedcolumn.ColumnTypeInt8, typedcolumn.ColumnTypeUint8, typedcolumn.ColumnTypeInt16, typedcolumn.ColumnTypeUint16, typedcolumn.ColumnTypeInt32, typedcolumn.ColumnTypeUint32, typedcolumn.ColumnTypeUint64, typedcolumn.ColumnTypeFloat16, typedcolumn.ColumnTypeBFloat16, typedcolumn.ColumnTypeUint8Vector, typedcolumn.ColumnTypeInt8Vector, typedcolumn.ColumnTypeUint16Vector, typedcolumn.ColumnTypeInt16Vector, typedcolumn.ColumnTypeUint32Vector, typedcolumn.ColumnTypeInt32Vector, typedcolumn.ColumnTypeUint64Vector, typedcolumn.ColumnTypeInt64Vector, typedcolumn.ColumnTypeFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.ColumnTypeFloat64Vector, typedcolumn.ColumnTypeUint32List, typedcolumn.ColumnTypeBytes, typedcolumn.ColumnTypeAdjacencyList} {
		if !slices.Contains(ColumnTypes(), columnType) || !IsKnownColumnType(columnType) {
			t.Fatalf("typedcolumn column type %q not covered", columnType)
		}
	}
	for _, encoding := range []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint, typedcolumn.EncodingNullableInt64, typedcolumn.EncodingBoolBitpackRLE, typedcolumn.EncodingLowCardinalityUint32, typedcolumn.EncodingRawFloat32Vector, typedcolumn.EncodingRawUint32Dense, typedcolumn.EncodingRawFloat32, typedcolumn.EncodingRawFloat64, typedcolumn.EncodingRawUint32OffsetsList, typedcolumn.EncodingRawBytesOffsets, typedcolumn.EncodingRawInt8, typedcolumn.EncodingRawUint8, typedcolumn.EncodingRawInt16, typedcolumn.EncodingRawUint16, typedcolumn.EncodingRawInt32, typedcolumn.EncodingRawUint32, typedcolumn.EncodingRawUint64, typedcolumn.EncodingRawFloat16, typedcolumn.EncodingRawBFloat16, typedcolumn.EncodingRawUint8Vector, typedcolumn.EncodingRawInt8Vector, typedcolumn.EncodingRawUint16Vector, typedcolumn.EncodingRawInt16Vector, typedcolumn.EncodingRawUint32Vector, typedcolumn.EncodingRawInt32Vector, typedcolumn.EncodingRawUint64Vector, typedcolumn.EncodingRawInt64Vector, typedcolumn.EncodingRawFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, typedcolumn.EncodingRawFloat64Vector} {
		if !slices.Contains(Encodings(), encoding) || !IsKnownEncoding(encoding) {
			t.Fatalf("typedcolumn encoding %s not covered", encoding)
		}
	}
}

func TestCapabilityInt64SupportsPreparedPredicateAndAggregateSemantics(t *testing.T) {
	desc := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint}
	for _, op := range []Operation{OpAllRows, OpEquality, OpOrderedRange, OpCountRows, OpCountNonNull, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported || cap.Phase != PhasePrepare {
			t.Fatalf("op %s status=%s reason=%s phase=%s", op, cap.Status, cap.Reason, cap.Phase)
		}
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
		OpStatsSum:     {ResultType: "int64", Accumulator: "durable int64 block/part stats payload", OverflowPolicy: "checked"},
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
	for _, op := range []Operation{OpAllRows, OpEquality, OpInequality, OpInList, OpCountRows, OpCountNonNull, OpBoolCounts, OpDirectScalarValueCarrier} {
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
	for _, op := range []Operation{OpSum, OpAvg} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	if cap := CapabilityFor(desc, OpPruneEquality); cap.Status != StatusFallback || cap.Reason != ReasonPruningPayloadUnsupported {
		t.Fatalf("%s capability=%+v", OpPruneEquality, cap)
	}
	if cap := CapabilityFor(desc, OpStatsSum); cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
		t.Fatalf("%s capability=%+v", OpStatsSum, cap)
	}
}

func TestCapabilityFloatRawInt64MatrixFailsClosed(t *testing.T) {
	for _, logical := range []LogicalType{LogicalFloat32, LogicalDouble} {
		desc := Descriptor{Logical: logical, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64}
		for _, op := range []Operation{OpEquality, OpInequality, OpInList} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusFallback || cap.Reason != ReasonNativeFloatLayoutMissing || !strings.Contains(cap.Message, "NaN") || !strings.Contains(cap.Message, "signed-zero") || !strings.Contains(cap.Message, "infinity") || !strings.Contains(cap.Message, "precision/accumulation") {
				t.Fatalf("%s %s capability=%+v want explicit native-float policy fallback", logical, op, cap)
			}
		}
		for _, op := range []Operation{OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != ReasonFloatRawInt64BitPattern || !strings.Contains(cap.Message, "NaN") || !strings.Contains(cap.Message, "signed-zero") || !strings.Contains(cap.Message, "infinity") || !strings.Contains(cap.Message, "precision/accumulation") {
				t.Fatalf("%s %s capability=%+v want raw-bit numeric fail-closed policy", logical, op, cap)
			}
		}
		for _, op := range []Operation{OpCountRows, OpCountNonNull} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusSupported || cap.Result.ResultType != "int64" || cap.Result.OverflowPolicy != "checked row count" {
				t.Fatalf("%s %s capability=%+v", logical, op, cap)
			}
		}
	}
}

func TestCapabilityNativeScalarFloatMatrixPreservesCarrierButDefersNumericSemantics(t *testing.T) {
	for _, tc := range []struct {
		logical  LogicalType
		physical typedcolumn.ColumnType
		encoding typedcolumn.Encoding
	}{
		{LogicalFloat32, typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32},
		{LogicalDouble, typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64},
	} {
		desc := Descriptor{Logical: tc.logical, Physical: tc.physical, Encoding: tc.encoding}
		for _, op := range []Operation{OpAllRows, OpDirectScalarValueCarrier} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusSupported || cap.Reason != ReasonSupported {
				t.Fatalf("%s %s capability=%+v want native scalar carrier support", tc.logical, op, cap)
			}
		}
		for _, op := range []Operation{OpCountRows, OpCountNonNull} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusSupported || cap.Result.ResultType != "int64" {
				t.Fatalf("%s %s capability=%+v want row-count support", tc.logical, op, cap)
			}
		}
		for _, op := range []Operation{OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != ReasonNativeFloatLayoutMissing || !strings.Contains(cap.Message, "NaN") || !strings.Contains(cap.Message, "signed-zero") {
				t.Fatalf("%s %s capability=%+v want explicit native-float numeric deferral", tc.logical, op, cap)
			}
		}
	}
}

func TestCapabilityPrimitiveScalarSemantics1929(t *testing.T) {
	integerCases := []struct {
		logical  LogicalType
		physical typedcolumn.ColumnType
		encoding typedcolumn.Encoding
		stats    bool
	}{
		{LogicalInt8, typedcolumn.ColumnTypeInt8, typedcolumn.EncodingRawInt8, true},
		{LogicalUint8, typedcolumn.ColumnTypeUint8, typedcolumn.EncodingRawUint8, true},
		{LogicalInt16, typedcolumn.ColumnTypeInt16, typedcolumn.EncodingRawInt16, true},
		{LogicalUint16, typedcolumn.ColumnTypeUint16, typedcolumn.EncodingRawUint16, true},
		{LogicalInt32, typedcolumn.ColumnTypeInt32, typedcolumn.EncodingRawInt32, true},
		{LogicalUint32, typedcolumn.ColumnTypeUint32, typedcolumn.EncodingRawUint32, true},
		{LogicalUint64, typedcolumn.ColumnTypeUint64, typedcolumn.EncodingRawUint64, false},
	}
	for _, tc := range integerCases {
		desc := Descriptor{Logical: tc.logical, Physical: tc.physical, Encoding: tc.encoding}
		for _, op := range []Operation{OpAllRows, OpEquality, OpOrderedRange, OpCountRows, OpCountNonNull, OpDirectScalarValueCarrier} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
				t.Fatalf("%s %s capability=%+v want supported", tc.logical, op, cap)
			}
		}
		for _, op := range []Operation{OpSum, OpAvg, OpMin, OpMax} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
				t.Fatalf("%s %s capability=%+v want primitive aggregate kernels deferred", tc.logical, op, cap)
			}
		}
		for _, op := range []Operation{OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
			cap := CapabilityFor(desc, op)
			if tc.stats && cap.Status != StatusSupported {
				t.Fatalf("%s %s capability=%+v want int64-compatible stats/pruning support", tc.logical, op, cap)
			}
			if !tc.stats && cap.Status != StatusUnsupported {
				t.Fatalf("%s %s capability=%+v want uint64 stats/pruning deferred", tc.logical, op, cap)
			}
		}
	}

	for _, tc := range []struct {
		logical  LogicalType
		physical typedcolumn.ColumnType
		encoding typedcolumn.Encoding
	}{
		{LogicalFloat16, typedcolumn.ColumnTypeFloat16, typedcolumn.EncodingRawFloat16},
		{LogicalBFloat16, typedcolumn.ColumnTypeBFloat16, typedcolumn.EncodingRawBFloat16},
	} {
		desc := Descriptor{Logical: tc.logical, Physical: tc.physical, Encoding: tc.encoding}
		for _, op := range []Operation{OpAllRows, OpCountRows, OpCountNonNull, OpDirectScalarValueCarrier} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
				t.Fatalf("%s %s capability=%+v want storage carrier support", tc.logical, op, cap)
			}
		}
		for _, op := range []Operation{OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusUnsupported || cap.Reason != ReasonNativeFloatLayoutMissing {
				t.Fatalf("%s %s capability=%+v want storage-only numeric fail-closed", tc.logical, op, cap)
			}
		}
	}
}

func TestCapabilityNullableFloatRawBitCarrierFallbacksAreExplicit(t *testing.T) {
	for _, logical := range []LogicalType{LogicalFloat32, LogicalDouble} {
		desc := Descriptor{Logical: logical, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Nullable: true}
		for _, op := range []Operation{OpCountRows, OpCountNonNull, OpIsNull, OpIsNotNull} {
			if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
				t.Fatalf("%s %s capability=%+v want supported count/null operation", logical, op, cap)
			}
		}
		for _, op := range []Operation{OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusFallback || cap.Reason != ReasonNullableCarrierAggregateSemantics {
				t.Fatalf("%s %s capability=%+v want nullable aggregate fallback", logical, op, cap)
			}
		}
		for _, op := range []Operation{OpEquality, OpOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(desc, op)
			if cap.Status != StatusFallback || cap.Reason != ReasonNullableCarrierValueSemantics {
				t.Fatalf("%s %s capability=%+v want nullable value fallback", logical, op, cap)
			}
		}
	}
}

func TestCapabilityStringDictionaryMatrixIsExplicit(t *testing.T) {
	desc := Descriptor{Logical: LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32}
	for _, op := range []Operation{OpEquality, OpInequality, OpInList, OpDictionaryEquality, OpDictionaryInList, OpDictionaryCategory} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported || cap.Reason != ReasonSupported {
			t.Fatalf("%s capability=%+v want supported dictionary category/equality", op, cap)
		}
	}
	checks := map[Operation]ResultSemantics{
		OpCountRows:               {ResultType: "int64", OverflowPolicy: "checked row count"},
		OpCountNonNull:            {ResultType: "int64", OverflowPolicy: "checked row count"},
		OpDictionaryGroupBy:       {ResultType: "groups", GroupKey: "dictionary string value; dictionary codes are part-local unless identity metadata proves otherwise"},
		OpDictionaryCount:         {ResultType: "groups with int64 counts", OverflowPolicy: "checked row count", GroupKey: "dictionary string value; count by code is valid only within matching dictionary identity"},
		OpDictionaryCountDistinct: {ResultType: "groups with int64 distinct counts", OverflowPolicy: "checked row and distinct bitmap counts", GroupKey: "dictionary string value; distinct dictionaries must be translated by value unless identity metadata matches"},
	}
	for op, want := range checks {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusSupported || cap.Result != want {
			t.Fatalf("%s capability=%+v want result=%+v", op, cap, want)
		}
	}
}

func TestCapabilityStringDictionaryCodesDoNotImplyLexicalRangeWithoutProof(t *testing.T) {
	desc := Descriptor{Logical: LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32}
	for _, op := range []Operation{OpOrderedRange, OpStringPrefix, OpStringLexicalRange} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusFallback || cap.Reason != ReasonDictionaryOrderUnproven {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	for _, op := range []Operation{OpDictionaryRange, OpStatsMinMax, OpPruneOrderedRange, OpMin, OpMax} {
		cap := CapabilityFor(desc, op)
		if cap.Status != StatusUnsupported || cap.Reason != ReasonDictionaryOrderUnproven {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	desc.DictionaryOrder = true
	for _, op := range []Operation{OpOrderedRange, OpStringPrefix, OpStringLexicalRange} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusFallback || cap.Reason != ReasonDictionaryCollationUnproven {
			t.Fatalf("ordered dictionary without collation %s capability=%+v", op, cap)
		}
	}
	if cap := CapabilityFor(desc, OpDictionaryRange); cap.Status != StatusUnsupported || cap.Reason != ReasonDictionaryCollationUnproven {
		t.Fatalf("ordered dictionary range without collation capability=%+v", cap)
	}
	desc.DictionaryCollation = "unicode-codepoint-v1"
	for _, op := range []Operation{OpDictionaryRange, OpStringPrefix, OpStringLexicalRange} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
			t.Fatalf("ordered dictionary with collation %s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
}

func TestCapabilityNullableCarrierDistinguishesCountAndValueAggregateSemantics(t *testing.T) {
	desc := Descriptor{Logical: LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Nullable: true}
	for _, op := range []Operation{OpCountRows, OpCountNonNull, OpIsNull, OpIsNotNull} {
		if cap := CapabilityFor(desc, op); cap.Status != StatusSupported {
			t.Fatalf("%s status=%s reason=%s", op, cap.Status, cap.Reason)
		}
	}
	for _, op := range []Operation{OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange} {
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
		{"uint32_list", Descriptor{Logical: LogicalUint32List, Physical: typedcolumn.ColumnTypeUint32List, Encoding: typedcolumn.EncodingRawUint32OffsetsList}, ReasonUint32ListScalarOperationUnsupported},
		{"adjacency", Descriptor{Logical: LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense}, ReasonAdjacencyScalarOperationUnsupported},
	}
	for _, tc := range cases {
		for _, op := range []Operation{OpCountRows, OpCountNonNull} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusSupported || cap.Result.ResultType != "int64" {
				t.Fatalf("%s %s capability=%+v", tc.name, op, cap)
			}
		}
		for _, op := range []Operation{OpEquality, OpInequality, OpOrderedRange, OpInList, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier} {
			cap := CapabilityFor(tc.desc, op)
			if cap.Status != StatusUnsupported || cap.Reason != tc.reason {
				t.Fatalf("%s %s status=%s reason=%s", tc.name, op, cap.Status, cap.Reason)
			}
		}
	}
}

func TestCapabilityVectorAndAdjacencySpecificOperationsAreExplicit(t *testing.T) {
	vector := Descriptor{Logical: LogicalFloat32Vector, Physical: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector}
	for _, op := range []Operation{OpVectorSimilarity, OpVectorDotProduct, OpVectorDirectPayload, OpVectorMetrics} {
		cap := CapabilityFor(vector, op)
		if cap.Status != StatusSupported || cap.Reason != ReasonSupported || cap.Phase != PhasePrepare {
			t.Fatalf("vector %s capability=%+v want supported", op, cap)
		}
	}
	if cap := CapabilityFor(vector, OpAdjacencyTraversal); cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
		t.Fatalf("vector adjacency traversal capability=%+v want operation unsupported", cap)
	}

	uint32List := Descriptor{Logical: LogicalUint32List, Physical: typedcolumn.ColumnTypeUint32List, Encoding: typedcolumn.EncodingRawUint32OffsetsList}
	if cap := CapabilityFor(uint32List, OpUint32ListDirectPayload); cap.Status != StatusSupported || cap.Reason != ReasonSupported || cap.Phase != PhasePrepare {
		t.Fatalf("uint32_list direct payload capability=%+v want supported", cap)
	}
	for _, op := range []Operation{OpAdjacencyTraversal, OpAdjacencyDirectPayload, OpAdjacencyMetrics, OpVectorSimilarity, OpVectorDirectPayload} {
		if cap := CapabilityFor(uint32List, op); cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
			t.Fatalf("uint32_list %s capability=%+v want operation unsupported", op, cap)
		}
	}
	if cap := CapabilityFor(Descriptor{Logical: LogicalUint32List, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32OffsetsList}, OpUint32ListDirectPayload); cap.Status != StatusUnsupported || cap.Reason != ReasonLogicalPhysicalMismatch {
		t.Fatalf("uint32_list adjacency physical capability=%+v want logical/physical mismatch", cap)
	}

	adjacency := Descriptor{Logical: LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense}
	for _, op := range []Operation{OpAdjacencyTraversal, OpAdjacencyMetrics} {
		cap := CapabilityFor(adjacency, op)
		if cap.Status != StatusSupported || cap.Reason != ReasonSupported || cap.Phase != PhasePrepare {
			t.Fatalf("adjacency %s capability=%+v want supported", op, cap)
		}
	}
	if cap := CapabilityFor(adjacency, OpAdjacencyDirectPayload); cap.Status != StatusFallback || cap.Reason != ReasonAdjacencyCapabilityDeferred || cap.Phase != PhasePrepare {
		t.Fatalf("adjacency direct payload capability=%+v want deferred fallback", cap)
	}
	if cap := CapabilityFor(adjacency, OpVectorSimilarity); cap.Status != StatusUnsupported || cap.Reason != ReasonOperationUnsupported {
		t.Fatalf("adjacency vector similarity capability=%+v want operation unsupported", cap)
	}

	offsetsAdjacency := Descriptor{Logical: LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32OffsetsList}
	if cap := CapabilityFor(offsetsAdjacency, OpAdjacencyDirectPayload); cap.Status != StatusFallback || cap.Reason != ReasonAdjacencyCapabilityDeferred {
		t.Fatalf("offsets-list adjacency direct capability=%+v want semantic direct deferred", cap)
	}
	if cap := CapabilityFor(offsetsAdjacency, OpAdjacencyTraversal); cap.Status != StatusSupported {
		t.Fatalf("offsets-list adjacency traversal semantic capability=%+v want semantic support; layout/runtime gates remain separate", cap)
	}
}

func TestCapabilityReasonCodesAreStable(t *testing.T) {
	checks := map[ReasonCode]string{
		ReasonFloatRawInt64BitPattern:              "float_raw_int64_bit_pattern",
		ReasonDictionaryOrderUnproven:              "dictionary_order_unproven",
		ReasonDictionaryCollationUnproven:          "dictionary_collation_unproven",
		ReasonNullableCarrierAggregateSemantics:    "nullable_carrier_aggregate_semantics",
		ReasonVectorScalarOperationUnsupported:     "vector_scalar_operation_unsupported",
		ReasonUint32ListScalarOperationUnsupported: "uint32_list_scalar_operation_unsupported",
		ReasonAdjacencyScalarOperationUnsupported:  "adjacency_scalar_operation_unsupported",
		ReasonAdjacencyCapabilityDeferred:          "adjacency_capability_deferred",
		ReasonStatsPayloadUnsupported:              "stats_payload_unsupported",
		ReasonPruningPayloadUnsupported:            "pruning_payload_unsupported",
	}
	for got, want := range checks {
		if string(got) != want {
			t.Fatalf("reason code changed: got %q want %q", got, want)
		}
	}
}
