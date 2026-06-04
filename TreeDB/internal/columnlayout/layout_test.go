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
	if cap := compressedCaps.SupportsSemanticOperation(columnsemantics.OpSum); !cap.Supported() {
		t.Fatalf("compressed sum cap=%+v want decode-on-scan support", cap)
	}
	if cap := compressedCaps.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonCompressedDirectView {
		t.Fatalf("compressed direct carrier cap=%+v want %s", cap, ReasonCompressedDirectView)
	}
	if cap := compressedCaps.SupportsSemanticOperation(columnsemantics.OpStatsSum); !cap.Supported() {
		t.Fatalf("compressed stats cap=%+v want decode-on-scan stats support", cap)
	}
	if cap := compressedCaps.SupportsSemanticOperation(columnsemantics.OpPruneOrderedRange); !cap.Supported() {
		t.Fatalf("compressed pruning cap=%+v want stats-backed pruning support", cap)
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

func TestPackedUintVectorLayoutOverflowFailsClosed1931(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	caps := CapabilitiesFor(Descriptor{
		Logical:            columnsemantics.LogicalPackedUint4Vector,
		Physical:           typedcolumn.ColumnTypePackedUint4Vector,
		Encoding:           typedcolumn.EncodingRawPackedUint4Vector,
		Compression:        typedcolumn.CompressionNone,
		FixedWidthElements: maxInt/4 + 1,
		BitsPerElement:     4,
	})
	if caps.DirectView.Eligible || caps.Layout.BytesPerRow != 0 || caps.Layout.LogicalBitsPerRow != 0 {
		t.Fatalf("overflow caps=%+v want fail-closed without derived row metadata", caps)
	}
	if cap := caps.Supports(OpDirectView); cap.Supported() || cap.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("overflow direct cap=%+v want %s", cap, ReasonLengthMultipleMismatch)
	}
}

func TestFixedAndPackedGeometryMismatchesFailDescriptorValidation1931(t *testing.T) {
	for _, tc := range []struct {
		name   string
		desc   Descriptor
		reason ReasonCode
		want   string
	}{
		{
			name:   "fixed_bytes_bytes_per_row",
			desc:   Descriptor{Logical: columnsemantics.LogicalByteVector, Physical: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, BytesPerRow: 4},
			reason: ReasonLengthMultipleMismatch,
			want:   "bytes_per_row=4 want 3",
		},
		{
			name:   "fixed_bytes_bits_per_element",
			desc:   Descriptor{Logical: columnsemantics.LogicalByteVector, Physical: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, BitsPerElement: 1},
			reason: ReasonFixedBytesGeometryMismatch,
			want:   "bits_per_element=1 want 0",
		},
		{
			name:   "fixed_bytes_logical_bits_per_row",
			desc:   Descriptor{Logical: columnsemantics.LogicalByteVector, Physical: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, LogicalBitsPerRow: 25},
			reason: ReasonFixedBytesGeometryMismatch,
			want:   "logical_bits_per_row=25 want 24",
		},
		{
			name:   "packed_bits_per_element",
			desc:   Descriptor{Logical: columnsemantics.LogicalPackedUint4Vector, Physical: typedcolumn.ColumnTypePackedUint4Vector, Encoding: typedcolumn.EncodingRawPackedUint4Vector, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, BitsPerElement: 2},
			reason: ReasonPackedUintBitsMismatch,
			want:   "bits_per_element=2 want 4",
		},
		{
			name:   "packed_bytes_per_row",
			desc:   Descriptor{Logical: columnsemantics.LogicalPackedUint4Vector, Physical: typedcolumn.ColumnTypePackedUint4Vector, Encoding: typedcolumn.EncodingRawPackedUint4Vector, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, BitsPerElement: 4, BytesPerRow: 3},
			reason: ReasonLengthMultipleMismatch,
			want:   "bytes_per_row=3 want 2",
		},
		{
			name:   "packed_logical_bits_per_row",
			desc:   Descriptor{Logical: columnsemantics.LogicalPackedUint4Vector, Physical: typedcolumn.ColumnTypePackedUint4Vector, Encoding: typedcolumn.EncodingRawPackedUint4Vector, Compression: typedcolumn.CompressionNone, FixedWidthElements: 3, BitsPerElement: 4, LogicalBitsPerRow: 13},
			reason: ReasonPackedUintBitsMismatch,
			want:   "logical_bits_per_row=13 want 12",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caps := CapabilitiesFor(tc.desc)
			cap := caps.Supports(OpDirectView)
			if cap.Supported() || cap.Reason != tc.reason || !strings.Contains(cap.Error(), tc.want) {
				t.Fatalf("direct capability=%+v want reason=%s containing %q", cap, tc.reason, tc.want)
			}
			granule := typedcolumn.EncodedGranule{Rows: 1, Encoding: tc.desc.Encoding, Compression: tc.desc.Compression, RawBytes: 0, StoredBytes: 0}
			if err := caps.ValidateGranule(granule); err == nil || !strings.Contains(err.Error(), string(tc.reason)) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateGranule err=%v want reason=%s containing %q", err, tc.reason, tc.want)
			}
		})
	}
}

func TestPrimitiveScalarLayoutRejectsFixedWidthElements1929(t *testing.T) {
	valid := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalUint32, Physical: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone})
	if !valid.DirectView.Eligible || valid.Layout.ElementsPerRow != 1 || valid.Layout.ElementWidthBytes != 4 || valid.Reducers.Int64NumericAggregate || valid.Reducers.Int64FixedWidthRaw || !valid.Stats.MinMax || !valid.Pruning.ValueRows {
		t.Fatalf("valid uint32 primitive caps=%+v want one-value-per-row direct/stats/pruning without primitive reducers", valid)
	}

	malformed := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalUint32, Physical: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, FixedWidthElements: 2})
	if malformed.DirectView.Eligible || malformed.Layout.FixedWidth || malformed.Layout.ElementWidthBytes != 0 || malformed.Reducers.Int64NumericAggregate || malformed.Stats.MinMax || malformed.Pruning.ValueRows {
		t.Fatalf("malformed primitive caps=%+v want fail-closed without direct/stats/pruning", malformed)
	}
	if cap := malformed.Supports(OpDirectView); cap.Supported() || cap.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("malformed direct cap=%+v want %s", cap, ReasonEncodingPhysicalMismatch)
	}
	if cap := malformed.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("malformed semantic direct cap=%+v want %s", cap, ReasonEncodingPhysicalMismatch)
	}
	granule := typedcolumn.EncodedGranule{Rows: 1, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, RawBytes: 4, StoredBytes: 4}
	if err := malformed.ValidateGranule(granule); err == nil || !strings.Contains(err.Error(), "fixed_width_elements=0") {
		t.Fatalf("malformed ValidateGranule err=%v want fixed_width_elements=0 rejection", err)
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

	for _, tc := range []struct {
		logical  columnsemantics.LogicalType
		physical typedcolumn.ColumnType
		encoding typedcolumn.Encoding
		width    int
	}{
		{columnsemantics.LogicalFloat32, typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32, 4},
		{columnsemantics.LogicalDouble, typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64, 8},
	} {
		native := CapabilitiesFor(Descriptor{Logical: tc.logical, Physical: tc.physical, Encoding: tc.encoding, Compression: typedcolumn.CompressionNone})
		if !native.Layout.FixedWidth || native.Layout.ElementWidthBytes != tc.width || native.Layout.Endian != EndianLittle || !native.DirectView.Eligible || native.Reducers.Int64NumericAggregate || native.Stats.MinMax || native.Pruning.ValueRows {
			t.Fatalf("native %s caps=%+v", tc.logical, native)
		}
		if cap := native.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); !cap.Supported() {
			t.Fatalf("native %s direct scalar cap=%+v want supported", tc.logical, cap)
		}
		g := typedcolumn.EncodedGranule{Rows: 3, Encoding: tc.encoding, Compression: typedcolumn.CompressionNone, RawBytes: 3 * tc.width, StoredBytes: 3 * tc.width, PayloadRef: typedcolumn.PayloadRef{Kind: typedcolumn.PayloadRefInline, Length: 3 * tc.width}}
		if err := native.ValidateGranule(g); err != nil {
			t.Fatalf("native %s ValidateGranule: %v", tc.logical, err)
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

	uint32List := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalUint32List, Physical: typedcolumn.ColumnTypeUint32List, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: typedcolumn.CompressionNone})
	if uint32List.Layout.Kind != LayoutVariableWidth || !uint32List.Layout.VariableWidth || uint32List.Layout.FixedWidth || uint32List.Layout.ElementsPerRow != 0 {
		t.Fatalf("uint32_list caps=%+v want variable-length offsets/value layout", uint32List)
	}
	if !uint32List.DirectView.Eligible || uint32List.DirectView.Reason != ReasonSupported || uint32List.DirectView.WidthBytes != 4 {
		t.Fatalf("uint32_list direct=%+v want eligible uint32 value direct view", uint32List.DirectView)
	}
	if cap := uint32List.Supports(OpUint32ListDirectView); !cap.Supported() {
		t.Fatalf("uint32_list direct cap=%+v want supported", cap)
	}
	if cap := uint32List.Supports(OpDirectView); cap.Supported() || cap.Reason != ReasonOperationUnsupported {
		t.Fatalf("uint32_list generic direct cap=%+v want split-section operation unsupported", cap)
	}
	if cap := uint32List.SupportsSemanticOperation(columnsemantics.OpUint32ListDirectPayload); !cap.Supported() {
		t.Fatalf("uint32_list semantic direct cap=%+v want supported", cap)
	}
	if cap := uint32List.Supports(OpAdjacencyDirectView); cap.Supported() || cap.Reason != ReasonOperationUnsupported {
		t.Fatalf("uint32_list adjacency direct cap=%+v want operation unsupported", cap)
	}
	for _, op := range []Operation{OpInt64RangePredicate, OpScalarNumericAggregate} {
		if cap := uint32List.Supports(op); cap.Supported() || cap.Reason != ReasonUint32ListScalarUnsupported {
			t.Fatalf("uint32_list scalar %s cap=%+v want %s", op, cap, ReasonUint32ListScalarUnsupported)
		}
	}
	if cap := uint32List.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonUint32ListScalarUnsupported {
		t.Fatalf("uint32_list semantic direct scalar cap=%+v want %s", cap, ReasonUint32ListScalarUnsupported)
	}
	uint32ListWithFixedWidth := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalUint32List, Physical: typedcolumn.ColumnTypeUint32List, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: typedcolumn.CompressionNone, FixedWidthElements: 2})
	if uint32ListWithFixedWidth.DirectView.Eligible || uint32ListWithFixedWidth.DirectView.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("uint32_list fixed-width direct=%+v want %s", uint32ListWithFixedWidth.DirectView, ReasonEncodingPhysicalMismatch)
	}
	if cap := uint32ListWithFixedWidth.Supports(OpUint32ListDirectView); cap.Supported() || cap.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("uint32_list fixed-width direct cap=%+v want %s", cap, ReasonEncodingPhysicalMismatch)
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
	offsetsAdjacency := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: typedcolumn.CompressionNone})
	if offsetsAdjacency.Layout.Kind != LayoutVariableWidth || !offsetsAdjacency.Layout.VariableWidth || offsetsAdjacency.Layout.FixedWidth || offsetsAdjacency.Layout.ElementsPerRow != 0 {
		t.Fatalf("offsets-list adjacency caps=%+v want variable-length non-dense layout", offsetsAdjacency)
	}
	if !offsetsAdjacency.DirectView.Eligible || offsetsAdjacency.DirectView.Reason != ReasonSupported {
		t.Fatalf("offsets-list adjacency direct=%+v want certified direct-view eligible", offsetsAdjacency.DirectView)
	}
	if cap := offsetsAdjacency.Supports(OpAdjacencyDirectView); !cap.Supported() {
		t.Fatalf("offsets-list direct cap=%+v want supported", cap)
	}
	if cap := offsetsAdjacency.Supports(OpAdjacencyTraversal); cap.Supported() || cap.Reason != ReasonAdjacencyOffsetsListRuntimeDeferred {
		t.Fatalf("offsets-list traversal cap=%+v want runtime deferred", cap)
	}
	offsetsAdjacencyWithFixedWidth := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: typedcolumn.CompressionNone, FixedWidthElements: 2})
	if offsetsAdjacencyWithFixedWidth.DirectView.Eligible || offsetsAdjacencyWithFixedWidth.DirectView.Reason != ReasonEncodingPhysicalMismatch {
		t.Fatalf("offsets-list fixed-width direct=%+v want %s", offsetsAdjacencyWithFixedWidth.DirectView, ReasonEncodingPhysicalMismatch)
	}
	for _, op := range []Operation{OpAdjacencyDirectView, OpAdjacencyTraversal, OpAdjacencyMetricReducer} {
		if cap := offsetsAdjacencyWithFixedWidth.Supports(op); cap.Supported() || cap.Reason != ReasonEncodingPhysicalMismatch {
			t.Fatalf("offsets-list fixed-width %s cap=%+v want %s", op, cap, ReasonEncodingPhysicalMismatch)
		}
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
