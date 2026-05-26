package columnlayout

import (
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestInt64RawLayoutCapabilitiesAndValidation(t *testing.T) {
	desc := Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone}
	caps := CapabilitiesFor(desc)
	if !caps.Layout.FixedWidth || caps.Layout.ElementWidthBytes != 8 || caps.Layout.Endian != EndianLittle || !caps.DirectView.Eligible || !caps.Reducers.Int64FixedWidthRaw || !caps.Reducers.Int64NumericAggregate || !caps.Pruning.ValueRows {
		t.Fatalf("raw int64 caps=%+v", caps)
	}
	if cap := caps.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); !cap.Supported() {
		t.Fatalf("raw int64 direct scalar cap=%+v want supported", cap)
	}
	g := typedcolumn.EncodedGranule{Rows: 3, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: 24, StoredBytes: 24, PayloadRef: typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: 24}}
	if err := caps.ValidateGranule(g); err != nil {
		t.Fatalf("ValidateGranule raw: %v", err)
	}

	bad := g
	bad.RawBytes = 23
	bad.StoredBytes = 23
	if err := caps.ValidateGranule(bad); err == nil || !strings.Contains(err.Error(), string(ReasonLengthMultipleMismatch)) {
		t.Fatalf("ValidateGranule raw length err=%v want %s", err, ReasonLengthMultipleMismatch)
	}
	bad = g
	bad.RawBytes = 16
	bad.StoredBytes = 16
	if err := caps.ValidateGranule(bad); err == nil || !strings.Contains(err.Error(), string(ReasonRawLengthRowCountMismatch)) {
		t.Fatalf("ValidateGranule row-count length err=%v want %s", err, ReasonRawLengthRowCountMismatch)
	}
	bad = g
	bad.Compression = typedcolumn.CompressionSnappy
	if err := caps.ValidateGranule(bad); err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("ValidateGranule compression err=%v want compression rejection", err)
	}
	if err := caps.ValidateGranulePayload(g, make([]byte, 16)); err == nil || !strings.Contains(err.Error(), "payload bytes=16") {
		t.Fatalf("ValidateGranulePayload err=%v want payload length rejection", err)
	}
}

func TestInt64DeltaLayoutStillSupportsStreamingReducer(t *testing.T) {
	desc := Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone}
	caps := CapabilitiesFor(desc)
	if !caps.Layout.VariableWidth || caps.DirectView.Eligible || !caps.Reducers.Int64Streaming || !caps.Reducers.Int64NumericAggregate || !caps.Pruning.ValueRows {
		t.Fatalf("delta caps=%+v", caps)
	}
	if cap := caps.SupportsSemanticOperation(columnsemantics.OpSum); !cap.Supported() {
		t.Fatalf("delta sum cap=%+v", cap)
	}
	if cap := caps.SupportsSemanticOperation(columnsemantics.OpPruneEquality); !cap.Supported() {
		t.Fatalf("delta equality pruning cap=%+v", cap)
	}
	if cap := caps.Supports(OpDirectView); cap.Supported() || cap.Reason != ReasonVariableWidthNoDirectView {
		t.Fatalf("delta direct view cap=%+v want reason %s", cap, ReasonVariableWidthNoDirectView)
	}
	g := typedcolumn.EncodedGranule{Rows: 3, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, RawBytes: 3, StoredBytes: 3, PayloadRef: typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: 3}}
	if err := caps.ValidateGranule(g); err != nil {
		t.Fatalf("ValidateGranule delta: %v", err)
	}
}

func TestCapabilityValidationRejectsWrongEncodingCompressionAndRows(t *testing.T) {
	desc := Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone}
	caps := CapabilitiesFor(desc)
	wrongEncoding := typedcolumn.EncodedGranule{Rows: 1, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, RawBytes: 8, StoredBytes: 8}
	if err := caps.ValidateGranule(wrongEncoding); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("wrong encoding err=%v", err)
	}
	wrongCompression := typedcolumn.EncodedGranule{Rows: 1, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionSnappy, RawBytes: 8, StoredBytes: 8}
	if err := caps.ValidateGranule(wrongCompression); err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("wrong compression err=%v want compression rejection", err)
	}
	compressedCaps := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionSnappy})
	if cap := compressedCaps.SupportsSemanticOperation(columnsemantics.OpSum); cap.Supported() || cap.Reason != ReasonUnsupportedCompression {
		t.Fatalf("compressed semantic cap=%+v want %s", cap, ReasonUnsupportedCompression)
	}
	emptyRows := typedcolumn.EncodedGranule{Rows: 0, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: 0, StoredBytes: 0}
	if err := caps.ValidateGranule(emptyRows); err != nil {
		t.Fatalf("empty rows err=%v want zero-row empty payload accepted", err)
	}
	badRows := typedcolumn.EncodedGranule{Rows: -1, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: 0, StoredBytes: 0}
	if err := caps.ValidateGranule(badRows); err == nil || !strings.Contains(err.Error(), "row count") {
		t.Fatalf("bad rows err=%v", err)
	}
}

func TestFloatAndNonInt64LayoutsDoNotAdvertiseUnsafeScalarCapabilities(t *testing.T) {
	for _, logical := range []columnsemantics.LogicalType{columnsemantics.LogicalFloat32, columnsemantics.LogicalDouble} {
		floatBits := CapabilitiesFor(Descriptor{Logical: logical, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone})
		if floatBits.DirectView.Eligible || floatBits.Reducers.Int64FixedWidthRaw || floatBits.Reducers.Int64NumericAggregate || floatBits.Stats.MinMax || floatBits.Stats.Sum || floatBits.Pruning.OrderedMinMax || floatBits.Pruning.ValueRows {
			t.Fatalf("%s raw-bit caps advertise unsafe int64 semantics: %+v", logical, floatBits)
		}
		for _, op := range []Operation{OpDirectView, OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpScalarNumericAggregate} {
			if cap := floatBits.Supports(op); cap.Supported() || cap.Reason != ReasonFloatBitPatternNotNumeric {
				t.Fatalf("%s %s cap=%+v want %s", logical, op, cap, ReasonFloatBitPatternNotNumeric)
			}
		}
		for _, op := range []columnsemantics.Operation{columnsemantics.OpEquality, columnsemantics.OpOrderedRange, columnsemantics.OpSum, columnsemantics.OpAvg, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpStatsMinMax, columnsemantics.OpStatsSum, columnsemantics.OpPruneEquality, columnsemantics.OpPruneOrderedRange, columnsemantics.OpDirectScalarValueCarrier} {
			if cap := floatBits.SupportsSemanticOperation(op); cap.Supported() || cap.Reason != ReasonFloatBitPatternNotNumeric {
				t.Fatalf("%s semantic %s cap=%+v want %s", logical, op, cap, ReasonFloatBitPatternNotNumeric)
			}
		}
		for _, op := range []columnsemantics.Operation{columnsemantics.OpAllRows, columnsemantics.OpCountRows, columnsemantics.OpCountNonNull} {
			if cap := floatBits.SupportsSemanticOperation(op); !cap.Supported() {
				t.Fatalf("%s semantic %s cap=%+v want supported count/all rows", logical, op, cap)
			}
		}
	}

	nullableFloat := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalFloat32, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Compression: typedcolumn.CompressionNone, Nullable: true, Defaultable: true})
	if !nullableFloat.Wrappers.CarrierAggregateUnsafe || nullableFloat.Stats.MinMax || nullableFloat.Stats.Sum || nullableFloat.Pruning.ValueRows {
		t.Fatalf("nullable float caps=%+v want wrapper fallback with stats/pruning disabled", nullableFloat)
	}
	if cap := nullableFloat.SupportsSemanticOperation(columnsemantics.OpCountNonNull); !cap.Supported() {
		t.Fatalf("nullable float count non-null cap=%+v want supported", cap)
	}
	if cap := nullableFloat.SupportsSemanticOperation(columnsemantics.OpSum); cap.Status != columnsemantics.StatusFallback || cap.Reason != ReasonNullDefaultWrapperRequired {
		t.Fatalf("nullable float sum cap=%+v want wrapper fallback", cap)
	}

	dict := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionNone, Dictionary: true})
	for _, op := range []columnsemantics.Operation{columnsemantics.OpDictionaryEquality, columnsemantics.OpDictionaryInList, columnsemantics.OpDictionaryCategory, columnsemantics.OpDictionaryGroupBy, columnsemantics.OpDictionaryCount, columnsemantics.OpDictionaryCountDistinct} {
		if cap := dict.SupportsSemanticOperation(op); !cap.Supported() {
			t.Fatalf("dictionary semantic %s cap=%+v want supported", op, cap)
		}
	}
	if cap := dict.Supports(OpLexicalRangePredicate); cap.Supported() || cap.Reason != ReasonDictionaryOrderUnproven {
		t.Fatalf("dictionary lexical cap=%+v want %s", cap, ReasonDictionaryOrderUnproven)
	}
	orderedNoCollation := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionNone, Dictionary: true, DictionaryOrder: true})
	if cap := orderedNoCollation.Supports(OpLexicalRangePredicate); cap.Supported() || cap.Reason != ReasonDictionaryCollationUnproven {
		t.Fatalf("ordered dictionary without collation cap=%+v want %s", cap, ReasonDictionaryCollationUnproven)
	}
	orderedDict := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalString, Physical: typedcolumn.ColumnTypeLowCardinalityCode, Encoding: typedcolumn.EncodingLowCardinalityUint32, Compression: typedcolumn.CompressionNone, Dictionary: true, DictionaryOrder: true, DictionaryCollation: "bytewise"})
	if cap := orderedDict.Supports(OpLexicalRangePredicate); !cap.Supported() {
		t.Fatalf("ordered dictionary lexical cap=%+v want supported", cap)
	}

	nullable := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingNullableInt64, Compression: typedcolumn.CompressionNone, Nullable: true, Defaultable: true})
	if !nullable.Wrappers.RequiresNullMask || !nullable.Wrappers.RequiresDefaultMask {
		t.Fatalf("nullable wrappers=%+v want null/default dependencies", nullable.Wrappers)
	}
	if cap := nullable.Supports(OpInt64NumericReducer); cap.Status != columnsemantics.StatusFallback || cap.Reason != ReasonNullDefaultWrapperRequired {
		t.Fatalf("nullable reducer cap=%+v want fallback %s", cap, ReasonNullDefaultWrapperRequired)
	}

	vector := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalFloat32Vector, Physical: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3})
	for _, op := range []Operation{OpVectorDirectView, OpVectorSimilarity, OpVectorMetricReducer} {
		if cap := vector.Supports(op); !cap.Supported() {
			t.Fatalf("vector %s cap=%+v want supported", op, cap)
		}
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpVectorDirectPayload, columnsemantics.OpVectorSimilarity, columnsemantics.OpVectorDotProduct, columnsemantics.OpVectorMetrics} {
		if cap := vector.SupportsSemanticOperation(op); !cap.Supported() {
			t.Fatalf("vector semantic %s cap=%+v want supported", op, cap)
		}
	}
	if cap := vector.Supports(OpScalarNumericAggregate); cap.Supported() || cap.Reason != ReasonVectorScalarUnsupported {
		t.Fatalf("vector scalar cap=%+v want %s", cap, ReasonVectorScalarUnsupported)
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpOrderedRange, columnsemantics.OpSum, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpDirectScalarValueCarrier} {
		if cap := vector.SupportsSemanticOperation(op); cap.Supported() || cap.Reason != ReasonVectorScalarUnsupported {
			t.Fatalf("vector semantic scalar %s cap=%+v want %s", op, cap, ReasonVectorScalarUnsupported)
		}
	}
	missingVectorDims := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalFloat32Vector, Physical: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone})
	if missingVectorDims.DirectView.Eligible || missingVectorDims.Layout.ElementsPerRow != 0 || missingVectorDims.Reducers.VectorMetrics || missingVectorDims.Pruning.VectorIndex {
		t.Fatalf("missing vector dims caps=%+v want fail-closed no direct/vector metrics", missingVectorDims)
	}
	for _, op := range []Operation{OpVectorDirectView, OpVectorSimilarity, OpVectorMetricReducer} {
		if cap := missingVectorDims.Supports(op); cap.Supported() || cap.Reason != ReasonFixedWidthElementsRequired {
			t.Fatalf("missing vector dims %s cap=%+v want %s", op, cap, ReasonFixedWidthElementsRequired)
		}
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpVectorDirectPayload, columnsemantics.OpVectorSimilarity, columnsemantics.OpVectorDotProduct, columnsemantics.OpVectorMetrics} {
		if cap := missingVectorDims.SupportsSemanticOperation(op); cap.Supported() || cap.Reason != ReasonFixedWidthElementsRequired {
			t.Fatalf("missing vector dims semantic %s cap=%+v want %s", op, cap, ReasonFixedWidthElementsRequired)
		}
	}
	for _, granule := range []typedcolumn.EncodedGranule{
		{Rows: 0, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, RawBytes: 0, StoredBytes: 0},
		{Rows: 1, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, RawBytes: 4, StoredBytes: 4},
	} {
		if err := missingVectorDims.ValidateGranule(granule); err == nil || !strings.Contains(err.Error(), string(ReasonFixedWidthElementsRequired)) {
			t.Fatalf("missing vector dims ValidateGranule rows=%d err=%v want %s", granule.Rows, err, ReasonFixedWidthElementsRequired)
		}
	}

	adjacency := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone, FixedWidthElements: 8})
	if adjacency.DirectView.Eligible {
		t.Fatalf("adjacency caps=%+v want direct view deferred", adjacency)
	}
	if cap := adjacency.Supports(OpAdjacencyDirectView); cap.Supported() || cap.Reason != ReasonAdjacencyDirectViewDeferred {
		t.Fatalf("adjacency direct cap=%+v want %s", cap, ReasonAdjacencyDirectViewDeferred)
	}
	for _, op := range []Operation{OpAdjacencyTraversal, OpAdjacencyMetricReducer} {
		if cap := adjacency.Supports(op); !cap.Supported() {
			t.Fatalf("adjacency %s cap=%+v want supported", op, cap)
		}
	}
	if cap := adjacency.SupportsSemanticOperation(columnsemantics.OpAdjacencyDirectPayload); cap.Supported() || cap.Reason != ReasonAdjacencyDirectViewDeferred {
		t.Fatalf("adjacency semantic direct cap=%+v want non-direct %s", cap, ReasonAdjacencyDirectViewDeferred)
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpAdjacencyTraversal, columnsemantics.OpAdjacencyMetrics} {
		if cap := adjacency.SupportsSemanticOperation(op); !cap.Supported() {
			t.Fatalf("adjacency semantic %s cap=%+v want supported", op, cap)
		}
	}
	if cap := adjacency.Supports(OpInt64RangePredicate); cap.Supported() || cap.Reason != ReasonAdjacencyScalarUnsupported {
		t.Fatalf("adjacency scalar cap=%+v want %s", cap, ReasonAdjacencyScalarUnsupported)
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpOrderedRange, columnsemantics.OpSum, columnsemantics.OpMin, columnsemantics.OpMax, columnsemantics.OpDirectScalarValueCarrier} {
		if cap := adjacency.SupportsSemanticOperation(op); cap.Supported() || cap.Reason != ReasonAdjacencyScalarUnsupported {
			t.Fatalf("adjacency semantic scalar %s cap=%+v want %s", op, cap, ReasonAdjacencyScalarUnsupported)
		}
	}
	missingAdjacencyDegree := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone})
	if missingAdjacencyDegree.DirectView.Eligible || missingAdjacencyDegree.Layout.ElementsPerRow != 0 || missingAdjacencyDegree.Reducers.AdjacencyMetrics || missingAdjacencyDegree.Pruning.AdjacencyIndex {
		t.Fatalf("missing adjacency degree caps=%+v want fail-closed no direct/adjacency metrics", missingAdjacencyDegree)
	}
	for _, op := range []Operation{OpAdjacencyDirectView, OpAdjacencyTraversal, OpAdjacencyMetricReducer} {
		if cap := missingAdjacencyDegree.Supports(op); cap.Supported() || cap.Reason != ReasonFixedWidthElementsRequired {
			t.Fatalf("missing adjacency degree %s cap=%+v want %s", op, cap, ReasonFixedWidthElementsRequired)
		}
	}
	for _, op := range []columnsemantics.Operation{columnsemantics.OpAdjacencyDirectPayload, columnsemantics.OpAdjacencyTraversal, columnsemantics.OpAdjacencyMetrics} {
		if cap := missingAdjacencyDegree.SupportsSemanticOperation(op); cap.Supported() || cap.Reason != ReasonFixedWidthElementsRequired {
			t.Fatalf("missing adjacency degree semantic %s cap=%+v want %s", op, cap, ReasonFixedWidthElementsRequired)
		}
	}
	for _, granule := range []typedcolumn.EncodedGranule{
		{Rows: 0, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone, RawBytes: 0, StoredBytes: 0},
		{Rows: 1, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone, RawBytes: 4, StoredBytes: 4},
	} {
		if err := missingAdjacencyDegree.ValidateGranule(granule); err == nil || !strings.Contains(err.Error(), string(ReasonFixedWidthElementsRequired)) {
			t.Fatalf("missing adjacency degree ValidateGranule rows=%d err=%v want %s", granule.Rows, err, ReasonFixedWidthElementsRequired)
		}
	}
}
