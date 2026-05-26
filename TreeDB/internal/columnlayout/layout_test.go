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

func TestNonInt64LayoutsDoNotAdvertiseUnsafeScalarCapabilities(t *testing.T) {
	floatBits := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalFloat32, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone})
	if cap := floatBits.Supports(OpInt64NumericReducer); cap.Supported() || cap.Reason != ReasonFloatBitPatternNotNumeric {
		t.Fatalf("float int64 reducer cap=%+v want %s", cap, ReasonFloatBitPatternNotNumeric)
	}
	if cap := floatBits.Supports(OpInt64RangePredicate); cap.Supported() || cap.Reason != ReasonFloatBitPatternNotNumeric {
		t.Fatalf("float range cap=%+v want %s", cap, ReasonFloatBitPatternNotNumeric)
	}
	if cap := floatBits.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonFloatBitPatternNotNumeric {
		t.Fatalf("float direct scalar cap=%+v want %s", cap, ReasonFloatBitPatternNotNumeric)
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
	if cap := vector.Supports(OpScalarNumericAggregate); cap.Supported() || cap.Reason != ReasonVectorScalarUnsupported {
		t.Fatalf("vector scalar cap=%+v want %s", cap, ReasonVectorScalarUnsupported)
	}
	if cap := vector.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonVectorScalarUnsupported {
		t.Fatalf("vector direct scalar cap=%+v want %s", cap, ReasonVectorScalarUnsupported)
	}
	adjacency := CapabilitiesFor(Descriptor{Logical: columnsemantics.LogicalAdjacencyList, Physical: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone, FixedWidthElements: 8})
	if cap := adjacency.Supports(OpInt64RangePredicate); cap.Supported() || cap.Reason != ReasonAdjacencyScalarUnsupported {
		t.Fatalf("adjacency scalar cap=%+v want %s", cap, ReasonAdjacencyScalarUnsupported)
	}
	if cap := adjacency.SupportsSemanticOperation(columnsemantics.OpDirectScalarValueCarrier); cap.Supported() || cap.Reason != ReasonAdjacencyScalarUnsupported {
		t.Fatalf("adjacency direct scalar cap=%+v want %s", cap, ReasonAdjacencyScalarUnsupported)
	}
}
