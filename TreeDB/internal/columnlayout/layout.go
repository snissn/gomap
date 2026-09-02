package columnlayout

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Operation identifies physical layout/codec capabilities. Semantic capability
// remains owned by columnsemantics; this package answers whether the selected
// typed-column layout can safely provide a concrete physical fast path for that
// semantic operation. Callers resolve these once at prepare/read boundaries, not
// in row hot loops.
type Operation string

const (
	OpDirectView             Operation = "direct.view"
	OpInt64NumericReducer    Operation = "reducer.int64_numeric"
	OpInt64RangePredicate    Operation = "predicate.int64_range"
	OpMinMaxPruning          Operation = "pruning.min_max"
	OpValueRowPruning        Operation = "pruning.value_rows"
	OpMinMaxStats            Operation = "stats.min_max"
	OpSumStats               Operation = "stats.sum"
	OpLexicalRangePredicate  Operation = "predicate.lexical_range"
	OpDictionaryCodeLookup   Operation = "predicate.dictionary_code_lookup"
	OpDictionaryReducer      Operation = "aggregate.dictionary_reducer"
	OpScalarNumericAggregate Operation = "aggregate.scalar_numeric"
	OpVectorDirectView       Operation = "direct.vector_payload"
	OpUint32ListDirectView   Operation = "direct.uint32_list_payload"
	OpBytesDirectView        Operation = "direct.bytes_payload"
	OpAdjacencyDirectView    Operation = "direct.adjacency_payload"
	OpVectorSimilarity       Operation = "predicate.vector_similarity"
	OpVectorMetricReducer    Operation = "aggregate.vector_metrics"
	OpAdjacencyTraversal     Operation = "graph.adjacency_traversal"
	OpAdjacencyMetricReducer Operation = "aggregate.adjacency_metrics"
)

type Endian string

const (
	EndianNone         Endian = ""
	EndianLittle       Endian = "little"
	EndianCodecDefined Endian = "codec_defined"
)

type LayoutKind string

const (
	LayoutVariableWidth LayoutKind = "variable_width"
	LayoutFixedWidth    LayoutKind = "fixed_width"
	LayoutWrapper       LayoutKind = "wrapper"
)

// ReasonCode is a stable layout diagnostic token. Add new values instead of
// changing existing strings; tests and planner diagnostics may key on them.
type ReasonCode string

const (
	ReasonSupported                              ReasonCode = "supported"
	ReasonUnknownLogicalType                     ReasonCode = "layout_unknown_logical_type"
	ReasonUnsupportedPhysicalType                ReasonCode = "layout_unsupported_physical_type"
	ReasonUnsupportedEncoding                    ReasonCode = "layout_unsupported_encoding"
	ReasonUnsupportedCompression                 ReasonCode = "layout_unsupported_compression"
	ReasonLogicalPhysicalMismatch                ReasonCode = "layout_logical_physical_mismatch"
	ReasonEncodingPhysicalMismatch               ReasonCode = "layout_encoding_physical_mismatch"
	ReasonVariableWidthNoDirectView              ReasonCode = "layout_variable_width_no_direct_view"
	ReasonCompressedDirectView                   ReasonCode = "layout_compressed_direct_view"
	ReasonLengthMultipleMismatch                 ReasonCode = "layout_length_multiple_mismatch"
	ReasonRawLengthRowCountMismatch              ReasonCode = "layout_raw_length_row_count_mismatch"
	ReasonNullDefaultWrapperRequired             ReasonCode = "layout_null_default_wrapper_required"
	ReasonDictionaryOrderUnproven                ReasonCode = "layout_dictionary_order_unproven"
	ReasonDictionaryCollationUnproven            ReasonCode = "layout_dictionary_collation_unproven"
	ReasonFloatBitPatternNotNumeric              ReasonCode = "layout_float_bit_pattern_not_numeric"
	ReasonVectorScalarUnsupported                ReasonCode = "layout_vector_scalar_unsupported"
	ReasonUint32ListScalarUnsupported            ReasonCode = "layout_uint32_list_scalar_unsupported"
	ReasonBytesScalarUnsupported                 ReasonCode = "layout_bytes_scalar_unsupported"
	ReasonFixedBytesGeometryMismatch             ReasonCode = "layout_fixed_bytes_geometry_mismatch"
	ReasonPackedUintScalarUnsupported            ReasonCode = "layout_packed_uint_scalar_unsupported"
	ReasonPackedUintBitsMismatch                 ReasonCode = "layout_packed_uint_bits_mismatch"
	ReasonAdjacencyScalarUnsupported             ReasonCode = "layout_adjacency_scalar_unsupported"
	ReasonAdjacencyDirectViewDeferred            ReasonCode = "layout_adjacency_direct_view_deferred"
	ReasonAdjacencyOffsetsListDirectViewDeferred ReasonCode = "layout_adjacency_offsets_list_direct_view_deferred"
	ReasonAdjacencyOffsetsListRuntimeDeferred    ReasonCode = "layout_adjacency_offsets_list_runtime_deferred"
	ReasonStatsPayloadUnsupported                ReasonCode = "layout_stats_payload_unsupported"
	ReasonPruningPayloadUnsupported              ReasonCode = "layout_pruning_payload_unsupported"
	ReasonFixedWidthElementsRequired             ReasonCode = "layout_fixed_width_elements_required"
	ReasonOperationUnsupported                   ReasonCode = "layout_operation_unsupported"
)

// Descriptor is the layout/codec key. It deliberately includes both logical and
// physical identity plus encoding and compression; encoding alone is not enough
// to decide semantic validity or hot-path safety.
type Descriptor struct {
	Logical             columnsemantics.LogicalType
	Physical            typedcolumn.ColumnType
	Encoding            typedcolumn.Encoding
	Compression         typedcolumn.Compression
	Nullable            bool
	Defaultable         bool
	Dictionary          bool
	DictionaryOrder     bool
	DictionaryCollation string
	FixedWidthElements  int
	BitsPerElement      int
	BytesPerRow         int
	LogicalBitsPerRow   int
}

type Capability struct {
	Operation Operation
	Status    columnsemantics.Status
	Reason    ReasonCode
	Message   string
}

func (c Capability) Supported() bool { return c.Status == columnsemantics.StatusSupported }

func (c Capability) Error() string {
	if c.Status == columnsemantics.StatusSupported {
		return ""
	}
	if c.Message != "" {
		return fmt.Sprintf("%s: %s", c.Reason, c.Message)
	}
	return string(c.Reason)
}

func Supported(op Operation) Capability {
	return Capability{Operation: op, Status: columnsemantics.StatusSupported, Reason: ReasonSupported}
}

func Unsupported(op Operation, reason ReasonCode, msg string) Capability {
	return Capability{Operation: op, Status: columnsemantics.StatusUnsupported, Reason: reason, Message: msg}
}

func Fallback(op Operation, reason ReasonCode, msg string) Capability {
	return Capability{Operation: op, Status: columnsemantics.StatusFallback, Reason: reason, Message: msg}
}

type LayoutProperties struct {
	Kind                LayoutKind
	FixedWidth          bool
	VariableWidth       bool
	ElementWidthBytes   int
	ElementsPerRow      int
	BytesPerRow         int
	BitsPerElement      int
	LogicalBitsPerRow   int
	Endian              Endian
	AlignmentBytes      int
	LengthMultipleBytes int
}

type DirectViewCapability struct {
	Eligible             bool
	Reason               ReasonCode
	Endian               Endian
	WidthBytes           int
	AlignmentBytes       int
	RequiresUncompressed bool
	RequiresRowCount     bool
	RequiresNoNulls      bool
	RequiresNoDefaults   bool
	Lifetime             string
	ValidationBoundary   string
}

type ReducerCapabilities struct {
	Int64Streaming        bool
	Int64FixedWidthRaw    bool
	Int64NumericAggregate bool
	BoolCounts            bool
	DictionaryCodes       bool
	VectorMetrics         bool
	AdjacencyMetrics      bool
}

type StatsCapabilities struct {
	MinMax      bool
	Sum         bool
	BoolCounts  bool
	Dictionary  bool
	VectorStats bool
}

type PruningCapabilities struct {
	OrderedMinMax     bool
	ValueRows         bool
	DictionaryCodes   bool
	LexicalDictionary bool
	VectorIndex       bool
	AdjacencyIndex    bool
}

type WrapperCapabilities struct {
	Nullable               bool
	Defaultable            bool
	RequiresNullMask       bool
	RequiresDefaultMask    bool
	CarrierValueSemantics  bool
	CarrierAggregateUnsafe bool
}

type ValidationContract struct {
	Boundary            string
	ChecksumRequired    bool
	ValidateRowCount    bool
	ValidateLength      bool
	ValidateCompression bool
	ValidateEndian      bool
	ValidateNullDefault bool
}

type Capabilities struct {
	Descriptor Descriptor
	Layout     LayoutProperties
	DirectView DirectViewCapability
	Reducers   ReducerCapabilities
	Stats      StatsCapabilities
	Pruning    PruningCapabilities
	Wrappers   WrapperCapabilities
	Validation ValidationContract
}

func CapabilitiesFor(desc Descriptor) Capabilities {
	caps := Capabilities{
		Descriptor: desc,
		Validation: ValidationContract{
			Boundary:            "prepare_and_payload_read",
			ChecksumRequired:    true,
			ValidateRowCount:    true,
			ValidateLength:      true,
			ValidateCompression: true,
			ValidateEndian:      true,
			ValidateNullDefault: true,
		},
	}
	if desc.FixedWidthElements > 0 {
		caps.Layout.ElementsPerRow = desc.FixedWidthElements
	} else {
		caps.Layout.ElementsPerRow = 1
	}
	if desc.Nullable || desc.Defaultable || desc.Encoding == typedcolumn.EncodingNullableInt64 {
		caps.Layout.Kind = LayoutWrapper
		caps.Wrappers = WrapperCapabilities{
			Nullable:               true,
			Defaultable:            true,
			RequiresNullMask:       true,
			RequiresDefaultMask:    true,
			CarrierValueSemantics:  true,
			CarrierAggregateUnsafe: true,
		}
	}

	switch desc.Encoding {
	case typedcolumn.EncodingRawInt64:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 8
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 8
		caps.Layout.LengthMultipleBytes = 8
		if desc.Logical == columnsemantics.LogicalInt64 && desc.Physical == typedcolumn.ColumnTypeInt64 {
			caps.DirectView = directView(desc, 8, EndianLittle, 8)
		} else {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: rawInt64NonInt64DirectViewReason(desc), Endian: EndianLittle, WidthBytes: 8, AlignmentBytes: 8, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		}
		if desc.Logical == columnsemantics.LogicalInt64 && desc.Physical == typedcolumn.ColumnTypeInt64 && !caps.Wrappers.Nullable {
			caps.Reducers.Int64NumericAggregate = true
			caps.Stats.MinMax = true
			caps.Stats.Sum = true
			caps.Pruning.OrderedMinMax = true
			caps.Pruning.ValueRows = true
			if desc.Compression == typedcolumn.CompressionNone {
				caps.Reducers.Int64FixedWidthRaw = true
			} else {
				caps.Reducers.Int64Streaming = true
			}
		}
	case typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.Endian = EndianCodecDefined
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonVariableWidthNoDirectView, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		if desc.Logical == columnsemantics.LogicalInt64 && desc.Physical == typedcolumn.ColumnTypeInt64 && !caps.Wrappers.Nullable {
			caps.Reducers.Int64Streaming = true
			caps.Reducers.Int64NumericAggregate = true
			caps.Stats.MinMax = true
			caps.Stats.Sum = true
			caps.Pruning.OrderedMinMax = true
			caps.Pruning.ValueRows = true
		}
	case typedcolumn.EncodingNullableInt64:
		caps.Layout.Kind = LayoutWrapper
		caps.Layout.VariableWidth = true
		caps.Layout.Endian = EndianCodecDefined
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonNullDefaultWrapperRequired, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
	case typedcolumn.EncodingBoolBitpackRLE:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.Endian = EndianCodecDefined
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonVariableWidthNoDirectView, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		if desc.Logical == columnsemantics.LogicalBool && desc.Physical == typedcolumn.ColumnTypeBool {
			caps.Reducers.BoolCounts = true
			caps.Stats.BoolCounts = true
		}
	case typedcolumn.EncodingLowCardinalityUint32:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.Endian = EndianCodecDefined
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonVariableWidthNoDirectView, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		if desc.Logical == columnsemantics.LogicalString && desc.Physical == typedcolumn.ColumnTypeLowCardinalityCode {
			caps.Reducers.DictionaryCodes = true
			caps.Stats.Dictionary = true
			caps.Pruning.DictionaryCodes = true
			if desc.DictionaryOrder && desc.DictionaryCollation != "" {
				caps.Pruning.LexicalDictionary = true
			}
		}
	case typedcolumn.EncodingRawFloat32:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		if desc.Logical == columnsemantics.LogicalFloat32 && desc.Physical == typedcolumn.ColumnTypeFloat32 {
			caps.DirectView = directView(desc, 4, EndianLittle, 4)
		} else {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 4, AlignmentBytes: 4, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		}
	case typedcolumn.EncodingRawFloat64:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 8
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 8
		caps.Layout.LengthMultipleBytes = 8
		if desc.Logical == columnsemantics.LogicalDouble && desc.Physical == typedcolumn.ColumnTypeFloat64 {
			caps.DirectView = directView(desc, 8, EndianLittle, 8)
		} else {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 8, AlignmentBytes: 8, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		}
	case typedcolumn.EncodingRawInt8,
		typedcolumn.EncodingRawUint8,
		typedcolumn.EncodingRawInt16,
		typedcolumn.EncodingRawUint16,
		typedcolumn.EncodingRawInt32,
		typedcolumn.EncodingRawUint32,
		typedcolumn.EncodingRawUint64,
		typedcolumn.EncodingRawFloat16,
		typedcolumn.EncodingRawBFloat16:
		applyPrimitiveScalarLayout(&caps)
	case typedcolumn.EncodingRawFloat32Vector:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		if desc.FixedWidthElements <= 0 {
			caps.Layout.ElementsPerRow = 0
			caps.DirectView = denseFixedWidthElementsRequiredDirectView(desc, 4, EndianLittle, 4)
			break
		}
		caps.DirectView = directView(desc, 4, EndianLittle, 4)
		if desc.Logical == columnsemantics.LogicalFloat32Vector && desc.Physical == typedcolumn.ColumnTypeFloat32Vector {
			caps.Reducers.VectorMetrics = true
			caps.Stats.VectorStats = true
			caps.Pruning.VectorIndex = true
		}
	case typedcolumn.EncodingRawUint8Vector, typedcolumn.EncodingRawInt8Vector, typedcolumn.EncodingRawUint16Vector, typedcolumn.EncodingRawInt16Vector, typedcolumn.EncodingRawUint32Vector, typedcolumn.EncodingRawInt32Vector, typedcolumn.EncodingRawUint64Vector, typedcolumn.EncodingRawInt64Vector, typedcolumn.EncodingRawFloat16Vector, typedcolumn.EncodingRawBFloat16Vector, typedcolumn.EncodingRawFloat64Vector:
		applyDenseNumericVectorLayout(&caps)
	case typedcolumn.EncodingRawFixedBytes:
		applyFixedBytesLayout(&caps)
	case typedcolumn.EncodingRawPackedBitVector, typedcolumn.EncodingRawPackedUint2Vector, typedcolumn.EncodingRawPackedUint4Vector:
		applyPackedUintVectorLayout(&caps)
	case typedcolumn.EncodingRawUint32Dense:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		if desc.FixedWidthElements <= 0 {
			caps.Layout.ElementsPerRow = 0
			caps.DirectView = denseFixedWidthElementsRequiredDirectView(desc, 4, EndianLittle, 4)
			break
		}
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonAdjacencyDirectViewDeferred, Endian: EndianLittle, WidthBytes: 4, AlignmentBytes: 4, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, Lifetime: "dense fixed-degree adjacency direct views remain fallback/compatibility; #1983 quarantines graph-specific raw_uint32_offsets_list storage while #1985 defines uint32_list", ValidationBoundary: "prepare"}
		if desc.Logical == columnsemantics.LogicalAdjacencyList && desc.Physical == typedcolumn.ColumnTypeAdjacencyList {
			caps.Reducers.AdjacencyMetrics = true
			caps.Pruning.AdjacencyIndex = true
		}
	case typedcolumn.EncodingRawUint32OffsetsList:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.ElementsPerRow = 0
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		if ((desc.Logical == columnsemantics.LogicalUint32List && desc.Physical == typedcolumn.ColumnTypeUint32List) || (desc.Logical == columnsemantics.LogicalAdjacencyList && desc.Physical == typedcolumn.ColumnTypeAdjacencyList)) && desc.FixedWidthElements == 0 {
			caps.DirectView = directView(desc, 4, EndianLittle, 4)
			caps.DirectView.Lifetime = "caller-owned prepared/session offsets and values resource handles"
			caps.DirectView.ValidationBoundary = "prepare_and_offsets_values_read"
		} else if (desc.Logical == columnsemantics.LogicalUint32List && desc.Physical == typedcolumn.ColumnTypeUint32List) || (desc.Logical == columnsemantics.LogicalAdjacencyList && desc.Physical == typedcolumn.ColumnTypeAdjacencyList) {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, Endian: EndianLittle, WidthBytes: 4, AlignmentBytes: 4, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		} else {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 4, AlignmentBytes: 4, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		}
	case typedcolumn.EncodingRawBytesOffsets:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.ElementsPerRow = 0
		caps.Layout.ElementWidthBytes = 1
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 1
		caps.Layout.LengthMultipleBytes = 1
		if desc.Logical == columnsemantics.LogicalBytes && desc.Physical == typedcolumn.ColumnTypeBytes && desc.FixedWidthElements == 0 {
			caps.DirectView = directView(desc, 1, EndianLittle, 1)
			caps.DirectView.Lifetime = "caller-owned prepared/session offsets and byte-values resource handles"
			caps.DirectView.ValidationBoundary = "prepare_and_offsets_values_read"
		} else if desc.Logical == columnsemantics.LogicalBytes && desc.Physical == typedcolumn.ColumnTypeBytes {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		} else {
			caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		}
	default:
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonUnsupportedEncoding, ValidationBoundary: "prepare"}
	}
	return caps
}

func applyDenseNumericVectorLayout(caps *Capabilities) {
	if caps == nil {
		return
	}
	desc := caps.Descriptor
	width, ok := typedcolumn.DenseFixedWidthVectorElementWidth(desc.Physical)
	want, encOK := typedcolumn.DenseFixedWidthVectorEncoding(desc.Physical)
	if !ok || !encOK || desc.Encoding != want || desc.Physical == typedcolumn.ColumnTypeFloat32Vector {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.FixedWidthElements <= 0 {
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = width
		caps.Layout.ElementsPerRow = 0
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = width
		caps.Layout.LengthMultipleBytes = width
		caps.DirectView = denseFixedWidthElementsRequiredDirectView(desc, width, EndianLittle, width)
		return
	}
	caps.Layout.Kind = LayoutFixedWidth
	caps.Layout.FixedWidth = true
	caps.Layout.ElementWidthBytes = width
	caps.Layout.ElementsPerRow = desc.FixedWidthElements
	caps.Layout.Endian = EndianLittle
	caps.Layout.AlignmentBytes = width
	caps.Layout.LengthMultipleBytes = width
	if denseNumericVectorLogicalPhysicalMatch(desc.Logical, desc.Physical, desc.Encoding) {
		caps.DirectView = directView(desc, width, EndianLittle, width)
	} else {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: width, AlignmentBytes: width, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
	}
}

func applyFixedBytesLayout(caps *Capabilities) {
	if caps == nil {
		return
	}
	desc := caps.Descriptor
	caps.Layout.Kind = LayoutFixedWidth
	caps.Layout.FixedWidth = true
	caps.Layout.ElementWidthBytes = 1
	caps.Layout.ElementsPerRow = desc.FixedWidthElements
	caps.Layout.BytesPerRow = desc.FixedWidthElements
	caps.Layout.Endian = EndianLittle
	caps.Layout.AlignmentBytes = 1
	caps.Layout.LengthMultipleBytes = 1
	if desc.FixedWidthElements <= 0 {
		caps.Layout.ElementsPerRow = 0
		caps.Layout.BytesPerRow = 0
		caps.DirectView = denseFixedWidthElementsRequiredDirectView(desc, 1, EndianLittle, 1)
		return
	}
	if desc.BytesPerRow != 0 && desc.BytesPerRow != desc.FixedWidthElements {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLengthMultipleMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	logicalBits, err := checkedMul(desc.FixedWidthElements, 8, "fixed bytes logical bits")
	if err != nil {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLengthMultipleMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.LogicalBitsPerRow != 0 && desc.LogicalBitsPerRow != logicalBits {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonFixedBytesGeometryMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.Logical == columnsemantics.LogicalByteVector && desc.Physical == typedcolumn.ColumnTypeFixedBytes && desc.Encoding == typedcolumn.EncodingRawFixedBytes {
		caps.DirectView = directView(desc, 1, EndianLittle, 1)
		caps.DirectView.Lifetime = "caller-owned prepared/session fixed-byte payload resource handles"
		caps.DirectView.ValidationBoundary = "prepare_and_payload_read"
	} else {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
	}
}

func applyPackedUintVectorLayout(caps *Capabilities) {
	if caps == nil {
		return
	}
	desc := caps.Descriptor
	bitsPerElement, ok := typedcolumn.PackedUintVectorBits(desc.Physical)
	wantEncoding, encOK := typedcolumn.PackedUintVectorEncoding(desc.Physical)
	if !ok || !encOK || desc.Encoding != wantEncoding {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	caps.Layout.Kind = LayoutFixedWidth
	caps.Layout.FixedWidth = true
	caps.Layout.ElementWidthBytes = 1
	caps.Layout.ElementsPerRow = desc.FixedWidthElements
	caps.Layout.BitsPerElement = bitsPerElement
	caps.Layout.Endian = EndianLittle
	caps.Layout.AlignmentBytes = 1
	caps.Layout.LengthMultipleBytes = 1
	if desc.FixedWidthElements <= 0 {
		caps.Layout.ElementsPerRow = 0
		caps.Layout.LogicalBitsPerRow = 0
		caps.DirectView = denseFixedWidthElementsRequiredDirectView(desc, 1, EndianLittle, 1)
		return
	}
	rowBytes, err := typedcolumn.PackedUintRowBytes(desc.FixedWidthElements, bitsPerElement)
	if err != nil {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLengthMultipleMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	logicalBits, err := checkedMul(desc.FixedWidthElements, bitsPerElement, "packed uint logical bits")
	if err != nil {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLengthMultipleMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	caps.Layout.BytesPerRow = rowBytes
	caps.Layout.LogicalBitsPerRow = logicalBits
	if desc.BitsPerElement != bitsPerElement {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonPackedUintBitsMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.BytesPerRow != 0 && desc.BytesPerRow != rowBytes {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLengthMultipleMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.LogicalBitsPerRow != 0 && desc.LogicalBitsPerRow != logicalBits {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonPackedUintBitsMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if packedUintVectorLogicalPhysicalMatch(desc.Logical, desc.Physical, desc.Encoding) {
		caps.DirectView = directView(desc, 1, EndianLittle, 1)
		caps.DirectView.Lifetime = "caller-owned prepared/session packed-code byte payload resource handles"
		caps.DirectView.ValidationBoundary = "prepare_and_payload_read"
	} else {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: 1, AlignmentBytes: 1, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
	}
}

func packedUintVectorLogical(logical columnsemantics.LogicalType) bool {
	switch logical {
	case columnsemantics.LogicalPackedBitVector, columnsemantics.LogicalPackedUint2Vector, columnsemantics.LogicalPackedUint4Vector:
		return true
	default:
		return false
	}
}

func packedUintVectorLogicalPhysicalMatch(logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) bool {
	switch logical {
	case columnsemantics.LogicalPackedBitVector:
		return physical == typedcolumn.ColumnTypePackedBitVector && encoding == typedcolumn.EncodingRawPackedBitVector
	case columnsemantics.LogicalPackedUint2Vector:
		return physical == typedcolumn.ColumnTypePackedUint2Vector && encoding == typedcolumn.EncodingRawPackedUint2Vector
	case columnsemantics.LogicalPackedUint4Vector:
		return physical == typedcolumn.ColumnTypePackedUint4Vector && encoding == typedcolumn.EncodingRawPackedUint4Vector
	default:
		return false
	}
}

func denseNumericVectorLogicalPhysicalMatch(logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) bool {
	switch logical {
	case columnsemantics.LogicalUint8Vector:
		return physical == typedcolumn.ColumnTypeUint8Vector && encoding == typedcolumn.EncodingRawUint8Vector
	case columnsemantics.LogicalInt8Vector:
		return physical == typedcolumn.ColumnTypeInt8Vector && encoding == typedcolumn.EncodingRawInt8Vector
	case columnsemantics.LogicalUint16Vector:
		return physical == typedcolumn.ColumnTypeUint16Vector && encoding == typedcolumn.EncodingRawUint16Vector
	case columnsemantics.LogicalInt16Vector:
		return physical == typedcolumn.ColumnTypeInt16Vector && encoding == typedcolumn.EncodingRawInt16Vector
	case columnsemantics.LogicalUint32Vector:
		return physical == typedcolumn.ColumnTypeUint32Vector && encoding == typedcolumn.EncodingRawUint32Vector
	case columnsemantics.LogicalInt32Vector:
		return physical == typedcolumn.ColumnTypeInt32Vector && encoding == typedcolumn.EncodingRawInt32Vector
	case columnsemantics.LogicalUint64Vector:
		return physical == typedcolumn.ColumnTypeUint64Vector && encoding == typedcolumn.EncodingRawUint64Vector
	case columnsemantics.LogicalInt64Vector:
		return physical == typedcolumn.ColumnTypeInt64Vector && encoding == typedcolumn.EncodingRawInt64Vector
	case columnsemantics.LogicalFloat16Vector:
		return physical == typedcolumn.ColumnTypeFloat16Vector && encoding == typedcolumn.EncodingRawFloat16Vector
	case columnsemantics.LogicalBFloat16Vector:
		return physical == typedcolumn.ColumnTypeBFloat16Vector && encoding == typedcolumn.EncodingRawBFloat16Vector
	case columnsemantics.LogicalFloat64Vector:
		return physical == typedcolumn.ColumnTypeFloat64Vector && encoding == typedcolumn.EncodingRawFloat64Vector
	default:
		return false
	}
}

func applyPrimitiveScalarLayout(caps *Capabilities) {
	if caps == nil {
		return
	}
	desc := caps.Descriptor
	width, ok := primitiveScalarLayoutWidth(desc.Physical, desc.Encoding)
	if !ok {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	if desc.FixedWidthElements != 0 {
		caps.Layout = LayoutProperties{}
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonEncodingPhysicalMismatch, Endian: EndianLittle, WidthBytes: width, AlignmentBytes: width, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
		return
	}
	caps.Layout.Kind = LayoutFixedWidth
	caps.Layout.FixedWidth = true
	caps.Layout.ElementWidthBytes = width
	caps.Layout.ElementsPerRow = 1
	caps.Layout.Endian = EndianLittle
	caps.Layout.AlignmentBytes = width
	caps.Layout.LengthMultipleBytes = width
	if primitiveScalarLogicalPhysicalMatch(desc.Logical, desc.Physical, desc.Encoding) {
		caps.DirectView = directView(desc, width, EndianLittle, width)
	} else {
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonLogicalPhysicalMismatch, Endian: EndianLittle, WidthBytes: width, AlignmentBytes: width, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare_and_payload_read"}
	}
	if primitiveIntegerStatsCompatible(desc.Logical, desc.Physical, desc.Encoding) && !caps.Wrappers.Nullable {
		caps.Stats.MinMax = true
		caps.Stats.Sum = true
		caps.Pruning.OrderedMinMax = true
		caps.Pruning.ValueRows = true
	}
}

func primitiveScalarLayoutWidth(physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) (int, bool) {
	switch physical {
	case typedcolumn.ColumnTypeInt8:
		return 1, encoding == typedcolumn.EncodingRawInt8
	case typedcolumn.ColumnTypeUint8:
		return 1, encoding == typedcolumn.EncodingRawUint8
	case typedcolumn.ColumnTypeInt16:
		return 2, encoding == typedcolumn.EncodingRawInt16
	case typedcolumn.ColumnTypeUint16:
		return 2, encoding == typedcolumn.EncodingRawUint16
	case typedcolumn.ColumnTypeInt32:
		return 4, encoding == typedcolumn.EncodingRawInt32
	case typedcolumn.ColumnTypeUint32:
		return 4, encoding == typedcolumn.EncodingRawUint32
	case typedcolumn.ColumnTypeUint64:
		return 8, encoding == typedcolumn.EncodingRawUint64
	case typedcolumn.ColumnTypeFloat16:
		return 2, encoding == typedcolumn.EncodingRawFloat16
	case typedcolumn.ColumnTypeBFloat16:
		return 2, encoding == typedcolumn.EncodingRawBFloat16
	default:
		return 0, false
	}
}

func primitiveScalarLogicalPhysicalMatch(logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) bool {
	switch logical {
	case columnsemantics.LogicalInt8:
		return physical == typedcolumn.ColumnTypeInt8 && encoding == typedcolumn.EncodingRawInt8
	case columnsemantics.LogicalUint8:
		return physical == typedcolumn.ColumnTypeUint8 && encoding == typedcolumn.EncodingRawUint8
	case columnsemantics.LogicalInt16:
		return physical == typedcolumn.ColumnTypeInt16 && encoding == typedcolumn.EncodingRawInt16
	case columnsemantics.LogicalUint16:
		return physical == typedcolumn.ColumnTypeUint16 && encoding == typedcolumn.EncodingRawUint16
	case columnsemantics.LogicalInt32:
		return physical == typedcolumn.ColumnTypeInt32 && encoding == typedcolumn.EncodingRawInt32
	case columnsemantics.LogicalUint32:
		return physical == typedcolumn.ColumnTypeUint32 && encoding == typedcolumn.EncodingRawUint32
	case columnsemantics.LogicalUint64:
		return physical == typedcolumn.ColumnTypeUint64 && encoding == typedcolumn.EncodingRawUint64
	case columnsemantics.LogicalFloat16:
		return physical == typedcolumn.ColumnTypeFloat16 && encoding == typedcolumn.EncodingRawFloat16
	case columnsemantics.LogicalBFloat16:
		return physical == typedcolumn.ColumnTypeBFloat16 && encoding == typedcolumn.EncodingRawBFloat16
	default:
		return false
	}
}

func primitiveIntegerStatsCompatible(logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) bool {
	if !primitiveScalarLogicalPhysicalMatch(logical, physical, encoding) {
		return false
	}
	switch logical {
	case columnsemantics.LogicalInt8, columnsemantics.LogicalUint8, columnsemantics.LogicalInt16, columnsemantics.LogicalUint16, columnsemantics.LogicalInt32, columnsemantics.LogicalUint32:
		return true
	default:
		return false
	}
}

func rawInt64NonInt64DirectViewReason(desc Descriptor) ReasonCode {
	if desc.Logical == columnsemantics.LogicalFloat32 || desc.Logical == columnsemantics.LogicalDouble {
		return ReasonFloatBitPatternNotNumeric
	}
	return ReasonLogicalPhysicalMismatch
}

func denseFixedWidthElementsRequiredDirectView(desc Descriptor, width int, endian Endian, align int) DirectViewCapability {
	return DirectViewCapability{
		Eligible:             false,
		Reason:               ReasonFixedWidthElementsRequired,
		Endian:               endian,
		WidthBytes:           width,
		AlignmentBytes:       align,
		RequiresUncompressed: true,
		RequiresRowCount:     true,
		RequiresNoNulls:      true,
		RequiresNoDefaults:   true,
		Lifetime:             "caller-owned prepared/session resource handle",
		ValidationBoundary:   "prepare_and_payload_read",
	}
}

func directView(desc Descriptor, width int, endian Endian, align int) DirectViewCapability {
	cap := DirectViewCapability{
		Eligible:             desc.Compression == typedcolumn.CompressionNone && !desc.Nullable && !desc.Defaultable,
		Endian:               endian,
		WidthBytes:           width,
		AlignmentBytes:       align,
		RequiresUncompressed: true,
		RequiresRowCount:     true,
		RequiresNoNulls:      true,
		RequiresNoDefaults:   true,
		Lifetime:             "caller-owned prepared/session resource handle",
		ValidationBoundary:   "prepare_and_payload_read",
	}
	if !cap.Eligible {
		if desc.Compression != typedcolumn.CompressionNone {
			cap.Reason = ReasonCompressedDirectView
		} else {
			cap.Reason = ReasonNullDefaultWrapperRequired
		}
	} else {
		cap.Reason = ReasonSupported
	}
	return cap
}

func (c Capabilities) Supports(op Operation) Capability {
	if base := c.validateDescriptor(op); !base.Supported() {
		return base
	}
	if c.Wrappers.Nullable {
		switch op {
		case OpDirectView, OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpScalarNumericAggregate:
			return Fallback(op, ReasonNullDefaultWrapperRequired, "nullable/default wrapper exposes null/default masks separately from carrier values")
		}
	}
	if c.Descriptor.Compression != typedcolumn.CompressionNone {
		switch op {
		case OpDirectView, OpVectorDirectView, OpUint32ListDirectView, OpBytesDirectView, OpAdjacencyDirectView:
			return Unsupported(op, ReasonCompressedDirectView, fmt.Sprintf("compression=%s", c.Descriptor.Compression))
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalFloat32 || c.Descriptor.Logical == columnsemantics.LogicalDouble {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpScalarNumericAggregate:
			return Unsupported(op, ReasonFloatBitPatternNotNumeric, "float bit-pattern storage is not an int64 numeric layout")
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalFloat32Vector || denseNumericVectorLogicalPhysicalMatch(c.Descriptor.Logical, c.Descriptor.Physical, c.Descriptor.Encoding) {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonVectorScalarUnsupported, "vector layouts reject scalar aggregate/range shortcuts")
		case OpVectorDirectView, OpVectorSimilarity, OpVectorMetricReducer:
			if c.Descriptor.FixedWidthElements <= 0 {
				return Unsupported(op, ReasonFixedWidthElementsRequired, "vector layouts require positive fixed_width_elements/elements_per_row")
			}
			if c.Descriptor.Logical != columnsemantics.LogicalFloat32Vector && (op == OpVectorSimilarity || op == OpVectorMetricReducer) {
				return Unsupported(op, ReasonOperationUnsupported, "dense numeric vector scoring/query semantics are deferred")
			}
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalUint32List {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonUint32ListScalarUnsupported, "uint32_list layouts reject scalar aggregate/range shortcuts")
		case OpDirectView:
			return Unsupported(op, ReasonOperationUnsupported, "uint32_list direct views require split offsets/value validation through OpUint32ListDirectView")
		case OpUint32ListDirectView:
			if c.Descriptor.FixedWidthElements != 0 {
				return Unsupported(op, ReasonEncodingPhysicalMismatch, "raw_uint32_offsets_list uint32_list layouts require fixed_width_elements=0")
			}
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalBytes {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonBytesScalarUnsupported, "bytes layouts reject scalar aggregate/range shortcuts")
		case OpDirectView:
			return Unsupported(op, ReasonOperationUnsupported, "bytes direct views require split offsets/value validation through OpBytesDirectView")
		case OpBytesDirectView:
			if c.Descriptor.FixedWidthElements != 0 {
				return Unsupported(op, ReasonEncodingPhysicalMismatch, "raw_bytes_offsets bytes layouts require fixed_width_elements=0")
			}
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalAdjacencyList {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonAdjacencyScalarUnsupported, "adjacency layouts reject scalar aggregate/range shortcuts")
		case OpAdjacencyDirectView, OpAdjacencyTraversal, OpAdjacencyMetricReducer:
			switch c.Descriptor.Encoding {
			case typedcolumn.EncodingRawUint32Dense:
				if c.Descriptor.FixedWidthElements <= 0 {
					return Unsupported(op, ReasonFixedWidthElementsRequired, "dense adjacency layouts require positive fixed_width_elements/adjacency_degree")
				}
			case typedcolumn.EncodingRawUint32OffsetsList:
				if c.Descriptor.FixedWidthElements != 0 {
					return Unsupported(op, ReasonEncodingPhysicalMismatch, "raw_uint32_offsets_list adjacency layouts require fixed_width_elements=0")
				}
				if op != OpAdjacencyDirectView {
					return Unsupported(op, ReasonAdjacencyOffsetsListRuntimeDeferred, "raw_uint32_offsets_list graph/search runtime integration is deferred to #1917+")
				}
			}
		}
	}

	switch op {
	case OpDirectView:
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "layout is not eligible for direct view")
	case OpInt64NumericReducer, OpScalarNumericAggregate:
		if c.Reducers.Int64NumericAggregate {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise int64 numeric reducer")
	case OpInt64RangePredicate:
		if c.Reducers.Int64NumericAggregate || c.Reducers.Int64Streaming || c.Reducers.Int64FixedWidthRaw {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise int64 range predicate")
	case OpMinMaxPruning:
		if c.Pruning.OrderedMinMax {
			return Supported(op)
		}
		return Unsupported(op, ReasonPruningPayloadUnsupported, "layout does not advertise ordered min/max pruning")
	case OpValueRowPruning:
		if c.Pruning.ValueRows {
			return Supported(op)
		}
		return Unsupported(op, ReasonPruningPayloadUnsupported, "layout does not advertise value-row pruning")
	case OpMinMaxStats:
		if c.Stats.MinMax {
			return Supported(op)
		}
		return Unsupported(op, ReasonStatsPayloadUnsupported, "layout does not advertise min/max stats")
	case OpSumStats:
		if c.Stats.Sum {
			return Supported(op)
		}
		return Unsupported(op, ReasonStatsPayloadUnsupported, "layout does not advertise sum stats")
	case OpDictionaryCodeLookup:
		if c.Reducers.DictionaryCodes {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise dictionary-code lookup")
	case OpDictionaryReducer:
		if c.Reducers.DictionaryCodes {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise dictionary-code reducer")
	case OpLexicalRangePredicate:
		if c.Pruning.LexicalDictionary {
			return Supported(op)
		}
		if c.Descriptor.Logical == columnsemantics.LogicalString && c.Reducers.DictionaryCodes {
			if !c.Descriptor.DictionaryOrder {
				return Unsupported(op, ReasonDictionaryOrderUnproven, "dictionary code order is not lexical order proof")
			}
			return Unsupported(op, ReasonDictionaryCollationUnproven, "dictionary lexical range requires collation identity")
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise lexical range")
	case OpVectorDirectView:
		if c.Descriptor.Logical != columnsemantics.LogicalFloat32Vector && c.Descriptor.Logical != columnsemantics.LogicalByteVector && !denseNumericVectorLogicalPhysicalMatch(c.Descriptor.Logical, c.Descriptor.Physical, c.Descriptor.Encoding) && !packedUintVectorLogicalPhysicalMatch(c.Descriptor.Logical, c.Descriptor.Physical, c.Descriptor.Encoding) {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not a vector payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "vector payload layout is not eligible for direct view")
	case OpUint32ListDirectView:
		if c.Descriptor.Logical != columnsemantics.LogicalUint32List {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not a uint32_list payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "uint32_list payload layout is not eligible for direct view")
	case OpAdjacencyDirectView:
		if c.Descriptor.Logical != columnsemantics.LogicalAdjacencyList {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not an adjacency payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "adjacency payload layout is not eligible for direct view")
	case OpBytesDirectView:
		if c.Descriptor.Logical != columnsemantics.LogicalBytes && c.Descriptor.Logical != columnsemantics.LogicalByteVector {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not a bytes payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "bytes payload layout is not eligible for direct view")
	case OpVectorSimilarity, OpVectorMetricReducer:
		if c.Reducers.VectorMetrics {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise vector metric support")
	case OpAdjacencyTraversal:
		if c.Pruning.AdjacencyIndex || c.Reducers.AdjacencyMetrics {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise adjacency traversal support")
	case OpAdjacencyMetricReducer:
		if c.Reducers.AdjacencyMetrics {
			return Supported(op)
		}
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise adjacency metric support")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "unknown layout operation")
	}
}

func (c Capabilities) SupportsSemanticOperation(op columnsemantics.Operation) Capability {
	switch op {
	case columnsemantics.OpAllRows, columnsemantics.OpCountRows, columnsemantics.OpCountNonNull:
		return c.validateDescriptor(Operation(op))
	case columnsemantics.OpEquality, columnsemantics.OpInequality, columnsemantics.OpInList:
		if c.Descriptor.Logical == columnsemantics.LogicalString {
			if c.Reducers.DictionaryCodes || c.Descriptor.Encoding == typedcolumn.EncodingLowCardinalityUint32 {
				return c.validateDescriptor(Operation(op))
			}
			return Unsupported(Operation(op), ReasonOperationUnsupported, "string layout lacks dictionary-code equality")
		}
		if c.Descriptor.Logical == columnsemantics.LogicalBool {
			return c.validateDescriptor(Operation(op))
		}
		if c.Descriptor.Logical == columnsemantics.LogicalBytes || c.Descriptor.Logical == columnsemantics.LogicalByteVector {
			return Unsupported(Operation(op), ReasonBytesScalarUnsupported, "bytes layouts are opaque payloads, not scalar predicates")
		}
		if packedUintVectorLogical(c.Descriptor.Logical) {
			return Unsupported(Operation(op), ReasonPackedUintScalarUnsupported, "packed_uint vector layouts are not scalar predicates")
		}
		return c.Supports(OpInt64RangePredicate)
	case columnsemantics.OpOrderedRange:
		if c.Descriptor.Logical == columnsemantics.LogicalString {
			return c.Supports(OpLexicalRangePredicate)
		}
		if c.Descriptor.Logical == columnsemantics.LogicalBytes || c.Descriptor.Logical == columnsemantics.LogicalByteVector {
			return Unsupported(Operation(op), ReasonBytesScalarUnsupported, "bytes layouts are opaque payloads, not ordered scalars")
		}
		if packedUintVectorLogical(c.Descriptor.Logical) {
			return Unsupported(Operation(op), ReasonPackedUintScalarUnsupported, "packed_uint vector layouts are not ordered scalars")
		}
		return c.Supports(OpInt64RangePredicate)
	case columnsemantics.OpSum, columnsemantics.OpAvg, columnsemantics.OpMin, columnsemantics.OpMax:
		return c.Supports(OpInt64NumericReducer)
	case columnsemantics.OpStatsMinMax:
		return c.Supports(OpMinMaxStats)
	case columnsemantics.OpStatsSum:
		return c.Supports(OpSumStats)
	case columnsemantics.OpPruneEquality:
		return c.Supports(OpValueRowPruning)
	case columnsemantics.OpPruneOrderedRange:
		return c.Supports(OpValueRowPruning)
	case columnsemantics.OpDirectScalarValueCarrier:
		return c.supportsDirectScalarValueCarrier()
	case columnsemantics.OpStringLexicalRange, columnsemantics.OpStringPrefix, columnsemantics.OpDictionaryRange:
		return c.Supports(OpLexicalRangePredicate)
	case columnsemantics.OpDictionaryEquality, columnsemantics.OpDictionaryInList, columnsemantics.OpDictionaryCategory:
		if c.Reducers.DictionaryCodes {
			return c.Supports(OpDictionaryCodeLookup)
		}
		return Unsupported(Operation(op), ReasonOperationUnsupported, "layout lacks dictionary-code lookup support")
	case columnsemantics.OpDictionaryGroupBy, columnsemantics.OpDictionaryCount, columnsemantics.OpDictionaryCountDistinct:
		if c.Reducers.DictionaryCodes {
			return c.Supports(OpDictionaryReducer)
		}
		return Unsupported(Operation(op), ReasonOperationUnsupported, "layout lacks dictionary-code reducer support")
	case columnsemantics.OpBoolCounts:
		if c.Reducers.BoolCounts {
			return c.validateDescriptor(Operation(op))
		}
		return Unsupported(Operation(op), ReasonOperationUnsupported, "layout lacks bool count support")
	case columnsemantics.OpVectorDirectPayload:
		return c.Supports(OpVectorDirectView)
	case columnsemantics.OpUint32ListDirectPayload:
		return c.Supports(OpUint32ListDirectView)
	case columnsemantics.OpBytesDirectPayload:
		return c.Supports(OpBytesDirectView)
	case columnsemantics.OpVectorSimilarity:
		return c.Supports(OpVectorSimilarity)
	case columnsemantics.OpVectorDotProduct, columnsemantics.OpVectorMetrics:
		return c.Supports(OpVectorMetricReducer)
	case columnsemantics.OpAdjacencyDirectPayload:
		return c.Supports(OpAdjacencyDirectView)
	case columnsemantics.OpAdjacencyTraversal:
		return c.Supports(OpAdjacencyTraversal)
	case columnsemantics.OpAdjacencyMetrics:
		return c.Supports(OpAdjacencyMetricReducer)
	case columnsemantics.OpIsNull, columnsemantics.OpIsNotNull:
		if c.Wrappers.Nullable {
			return c.validateDescriptor(Operation(op))
		}
		return Unsupported(Operation(op), ReasonNullDefaultWrapperRequired, "non-null layout has no null/default masks")
	default:
		return Unsupported(Operation(op), ReasonOperationUnsupported, "semantic operation has no layout mapping")
	}
}

func (c Capabilities) supportsDirectScalarValueCarrier() Capability {
	op := Operation(columnsemantics.OpDirectScalarValueCarrier)
	switch c.Descriptor.Logical {
	case columnsemantics.LogicalInt64:
		return c.Supports(OpDirectView)
	case columnsemantics.LogicalFloat32:
		if c.Descriptor.Physical == typedcolumn.ColumnTypeFloat32 && c.Descriptor.Encoding == typedcolumn.EncodingRawFloat32 {
			return c.Supports(OpDirectView)
		}
		return Unsupported(op, ReasonFloatBitPatternNotNumeric, "float bit-pattern storage is not a direct scalar value carrier")
	case columnsemantics.LogicalDouble:
		if c.Descriptor.Physical == typedcolumn.ColumnTypeFloat64 && c.Descriptor.Encoding == typedcolumn.EncodingRawFloat64 {
			return c.Supports(OpDirectView)
		}
		return Unsupported(op, ReasonFloatBitPatternNotNumeric, "float bit-pattern storage is not a direct scalar value carrier")
	case columnsemantics.LogicalInt8, columnsemantics.LogicalUint8, columnsemantics.LogicalInt16, columnsemantics.LogicalUint16, columnsemantics.LogicalInt32, columnsemantics.LogicalUint32, columnsemantics.LogicalUint64, columnsemantics.LogicalFloat16, columnsemantics.LogicalBFloat16:
		if primitiveScalarLogicalPhysicalMatch(c.Descriptor.Logical, c.Descriptor.Physical, c.Descriptor.Encoding) {
			return c.Supports(OpDirectView)
		}
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "primitive scalar logical type does not match physical encoding")
	case columnsemantics.LogicalFloat32Vector:
		return Unsupported(op, ReasonVectorScalarUnsupported, "vector layouts reject scalar direct-value carriers")
	case columnsemantics.LogicalUint32List:
		return Unsupported(op, ReasonUint32ListScalarUnsupported, "uint32_list layouts reject scalar direct-value carriers")
	case columnsemantics.LogicalBytes:
		return Unsupported(op, ReasonBytesScalarUnsupported, "bytes layouts reject scalar direct-value carriers")
	case columnsemantics.LogicalAdjacencyList:
		return Unsupported(op, ReasonAdjacencyScalarUnsupported, "adjacency layouts reject scalar direct-value carriers")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "layout does not advertise a direct scalar value carrier")
	}
}

func (c Capabilities) validateDescriptor(op Operation) Capability {
	desc := c.Descriptor
	if !columnsemantics.IsKnownLogicalType(desc.Logical) {
		return Unsupported(op, ReasonUnknownLogicalType, fmt.Sprintf("logical_type=%q", desc.Logical))
	}
	if !columnsemantics.IsKnownColumnType(desc.Physical) {
		return Unsupported(op, ReasonUnsupportedPhysicalType, fmt.Sprintf("physical_type=%q", desc.Physical))
	}
	if !columnsemantics.IsKnownEncoding(desc.Encoding) {
		return Unsupported(op, ReasonUnsupportedEncoding, fmt.Sprintf("encoding=%s", desc.Encoding))
	}
	switch desc.Compression {
	case typedcolumn.CompressionNone, typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4:
	default:
		return Unsupported(op, ReasonUnsupportedCompression, fmt.Sprintf("compression=%s", desc.Compression))
	}
	if reason, ok := validatePhysicalEncoding(desc.Physical, desc.Encoding); !ok {
		return Unsupported(op, reason, fmt.Sprintf("physical_type=%s encoding=%s", desc.Physical, desc.Encoding))
	}
	if _, primitive := primitiveScalarLayoutWidth(desc.Physical, desc.Encoding); primitive && desc.FixedWidthElements != 0 {
		return Unsupported(op, ReasonEncodingPhysicalMismatch, fmt.Sprintf("primitive scalar layout requires fixed_width_elements=0 got %d", desc.FixedWidthElements))
	}
	if cap := validateFixedAndPackedDescriptorGeometry(op, desc); !cap.Supported() {
		return cap
	}
	if desc.Nullable && desc.Encoding != typedcolumn.EncodingNullableInt64 {
		return Unsupported(op, ReasonEncodingPhysicalMismatch, fmt.Sprintf("nullable layout requires encoding=%s got=%s", typedcolumn.EncodingNullableInt64, desc.Encoding))
	}
	return Supported(op)
}

func validateFixedAndPackedDescriptorGeometry(op Operation, desc Descriptor) Capability {
	switch desc.Physical {
	case typedcolumn.ColumnTypeFixedBytes:
		if desc.FixedWidthElements <= 0 {
			return Unsupported(op, ReasonFixedWidthElementsRequired, fmt.Sprintf("fixed_bytes requires positive fixed_width_elements got %d", desc.FixedWidthElements))
		}
		if desc.BitsPerElement != 0 {
			return Unsupported(op, ReasonFixedBytesGeometryMismatch, fmt.Sprintf("fixed_bytes bits_per_element=%d want 0", desc.BitsPerElement))
		}
		if desc.BytesPerRow != 0 && desc.BytesPerRow != desc.FixedWidthElements {
			return Unsupported(op, ReasonLengthMultipleMismatch, fmt.Sprintf("fixed_bytes bytes_per_row=%d want %d", desc.BytesPerRow, desc.FixedWidthElements))
		}
		logicalBits, err := checkedMul(desc.FixedWidthElements, 8, "fixed bytes logical bits")
		if err != nil {
			return Unsupported(op, ReasonLengthMultipleMismatch, err.Error())
		}
		if desc.LogicalBitsPerRow != 0 && desc.LogicalBitsPerRow != logicalBits {
			return Unsupported(op, ReasonFixedBytesGeometryMismatch, fmt.Sprintf("fixed_bytes logical_bits_per_row=%d want %d", desc.LogicalBitsPerRow, logicalBits))
		}
	case typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
		bitsPerElement, ok := typedcolumn.PackedUintVectorBits(desc.Physical)
		if !ok {
			return Supported(op)
		}
		if desc.FixedWidthElements <= 0 {
			return Unsupported(op, ReasonFixedWidthElementsRequired, fmt.Sprintf("packed_uint requires positive fixed_width_elements got %d", desc.FixedWidthElements))
		}
		if desc.BitsPerElement != bitsPerElement {
			return Unsupported(op, ReasonPackedUintBitsMismatch, fmt.Sprintf("packed_uint bits_per_element=%d want %d", desc.BitsPerElement, bitsPerElement))
		}
		rowBytes, err := typedcolumn.PackedUintRowBytes(desc.FixedWidthElements, bitsPerElement)
		if err != nil {
			return Unsupported(op, ReasonLengthMultipleMismatch, err.Error())
		}
		if desc.BytesPerRow != 0 && desc.BytesPerRow != rowBytes {
			return Unsupported(op, ReasonLengthMultipleMismatch, fmt.Sprintf("packed_uint bytes_per_row=%d want %d", desc.BytesPerRow, rowBytes))
		}
		logicalBits, err := checkedMul(desc.FixedWidthElements, bitsPerElement, "packed uint logical bits")
		if err != nil {
			return Unsupported(op, ReasonLengthMultipleMismatch, err.Error())
		}
		if desc.LogicalBitsPerRow != 0 && desc.LogicalBitsPerRow != logicalBits {
			return Unsupported(op, ReasonPackedUintBitsMismatch, fmt.Sprintf("packed_uint logical_bits_per_row=%d want %d", desc.LogicalBitsPerRow, logicalBits))
		}
	}
	return Supported(op)
}

func validatePhysicalEncoding(physical typedcolumn.ColumnType, encoding typedcolumn.Encoding) (ReasonCode, bool) {
	switch physical {
	case typedcolumn.ColumnTypeInt64:
		switch encoding {
		case typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint, typedcolumn.EncodingNullableInt64:
			return ReasonSupported, true
		}
	case typedcolumn.ColumnTypeLowCardinalityCode:
		switch encoding {
		case typedcolumn.EncodingLowCardinalityUint32, typedcolumn.EncodingNullableInt64:
			return ReasonSupported, true
		}
	case typedcolumn.ColumnTypeBool:
		switch encoding {
		case typedcolumn.EncodingBoolBitpackRLE, typedcolumn.EncodingNullableInt64:
			return ReasonSupported, true
		}
	case typedcolumn.ColumnTypeFloat32:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat32
	case typedcolumn.ColumnTypeFloat64:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat64
	case typedcolumn.ColumnTypeInt8:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawInt8
	case typedcolumn.ColumnTypeUint8:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint8
	case typedcolumn.ColumnTypeInt16:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawInt16
	case typedcolumn.ColumnTypeUint16:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint16
	case typedcolumn.ColumnTypeInt32:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawInt32
	case typedcolumn.ColumnTypeUint32:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint32
	case typedcolumn.ColumnTypeUint64:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint64
	case typedcolumn.ColumnTypeFloat16:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat16
	case typedcolumn.ColumnTypeBFloat16:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawBFloat16
	case typedcolumn.ColumnTypeFloat32Vector:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat32Vector
	case typedcolumn.ColumnTypeUint8Vector, typedcolumn.ColumnTypeInt8Vector, typedcolumn.ColumnTypeUint16Vector, typedcolumn.ColumnTypeInt16Vector, typedcolumn.ColumnTypeUint32Vector, typedcolumn.ColumnTypeInt32Vector, typedcolumn.ColumnTypeUint64Vector, typedcolumn.ColumnTypeInt64Vector, typedcolumn.ColumnTypeFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector, typedcolumn.ColumnTypeFloat64Vector:
		want, ok := typedcolumn.DenseFixedWidthVectorEncoding(physical)
		return ReasonEncodingPhysicalMismatch, ok && encoding == want
	case typedcolumn.ColumnTypeFixedBytes:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFixedBytes
	case typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
		want, ok := typedcolumn.PackedUintVectorEncoding(physical)
		return ReasonEncodingPhysicalMismatch, ok && encoding == want
	case typedcolumn.ColumnTypeUint32List:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint32OffsetsList
	case typedcolumn.ColumnTypeBytes:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawBytesOffsets
	case typedcolumn.ColumnTypeAdjacencyList:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint32Dense || encoding == typedcolumn.EncodingRawUint32OffsetsList
	}
	return ReasonEncodingPhysicalMismatch, false
}

func (c Capabilities) ValidateGranule(g typedcolumn.EncodedGranule) error {
	if cap := c.validateDescriptor("validate.granule"); !cap.Supported() {
		return fmt.Errorf("columnlayout: %s", cap.Error())
	}
	if g.Rows < 0 {
		return fmt.Errorf("columnlayout: invalid row count %d", g.Rows)
	}
	if g.Encoding != c.Descriptor.Encoding {
		return fmt.Errorf("columnlayout: granule encoding=%s want %s", g.Encoding, c.Descriptor.Encoding)
	}
	if g.Compression != c.Descriptor.Compression {
		return fmt.Errorf("columnlayout: granule compression=%s want %s", g.Compression, c.Descriptor.Compression)
	}
	if !c.Wrappers.Nullable && (g.NullCount != 0 || g.DefaultCount != 0) {
		return fmt.Errorf("columnlayout: non-null layout has null/default counts null=%d default=%d", g.NullCount, g.DefaultCount)
	}
	if c.Wrappers.Nullable {
		if g.NullCount < 0 || g.DefaultCount < 0 || g.NullCount > g.Rows || g.DefaultCount > g.Rows-g.NullCount {
			return fmt.Errorf("columnlayout: invalid null/default counts rows=%d null=%d default=%d", g.Rows, g.NullCount, g.DefaultCount)
		}
	}
	return c.validateGranuleLengths(g)
}

func (c Capabilities) ValidateGranulePayload(g typedcolumn.EncodedGranule, payload []byte) error {
	if err := c.ValidateGranule(g); err != nil {
		return err
	}
	if len(payload) != g.StoredBytes {
		return fmt.Errorf("columnlayout: payload bytes=%d want stored=%d", len(payload), g.StoredBytes)
	}
	return nil
}

func (c Capabilities) validateGranuleLengths(g typedcolumn.EncodedGranule) error {
	if g.RawBytes < 0 || g.StoredBytes < 0 {
		return fmt.Errorf("columnlayout: negative raw/stored bytes raw=%d stored=%d", g.RawBytes, g.StoredBytes)
	}
	if c.Descriptor.Compression == typedcolumn.CompressionNone && g.StoredBytes != g.RawBytes {
		return fmt.Errorf("columnlayout: uncompressed stored bytes=%d raw=%d", g.StoredBytes, g.RawBytes)
	}
	if c.Layout.LengthMultipleBytes > 0 && g.RawBytes%c.Layout.LengthMultipleBytes != 0 {
		return fmt.Errorf("columnlayout: raw bytes=%d not multiple of %d: %s", g.RawBytes, c.Layout.LengthMultipleBytes, ReasonLengthMultipleMismatch)
	}
	if c.Layout.FixedWidth && (c.Layout.ElementWidthBytes <= 0 || c.Layout.ElementsPerRow <= 0) {
		return fmt.Errorf("columnlayout: invalid fixed-width contract width=%d elements_per_row=%d: %s", c.Layout.ElementWidthBytes, c.Layout.ElementsPerRow, ReasonFixedWidthElementsRequired)
	}
	if c.Descriptor.Compression != typedcolumn.CompressionNone && g.StoredBytes <= 0 && g.Rows > 0 {
		return fmt.Errorf("columnlayout: compressed payload stored bytes=%d: %s", g.StoredBytes, ReasonRawLengthRowCountMismatch)
	}
	if g.Rows == 0 {
		if g.RawBytes != 0 || g.StoredBytes != 0 {
			return fmt.Errorf("columnlayout: zero-row payload raw=%d stored=%d want 0: %s", g.RawBytes, g.StoredBytes, ReasonRawLengthRowCountMismatch)
		}
		return nil
	}
	if !c.Layout.FixedWidth {
		if g.RawBytes == 0 || g.StoredBytes == 0 {
			return fmt.Errorf("columnlayout: variable-width payload has zero raw/stored bytes raw=%d stored=%d", g.RawBytes, g.StoredBytes)
		}
		return nil
	}
	want := 0
	var err error
	if c.Layout.BytesPerRow > 0 {
		want, err = checkedMul(g.Rows, c.Layout.BytesPerRow, "fixed-width row bytes")
		if err != nil {
			return err
		}
	} else {
		elements, err := checkedMul(g.Rows, c.Layout.ElementsPerRow, "fixed-width elements")
		if err != nil {
			return err
		}
		want, err = checkedMul(elements, c.Layout.ElementWidthBytes, "fixed-width raw bytes")
		if err != nil {
			return err
		}
	}
	if g.RawBytes != want {
		return fmt.Errorf("columnlayout: raw bytes raw=%d want rows=%d row_bytes=%d elements=%d width=%d => %d: %s", g.RawBytes, g.Rows, c.Layout.BytesPerRow, c.Layout.ElementsPerRow, c.Layout.ElementWidthBytes, want, ReasonRawLengthRowCountMismatch)
	}
	if c.Descriptor.Compression == typedcolumn.CompressionNone && g.StoredBytes != want {
		return fmt.Errorf("columnlayout: stored bytes raw=%d stored=%d want rows=%d row_bytes=%d elements=%d width=%d => %d: %s", g.RawBytes, g.StoredBytes, g.Rows, c.Layout.BytesPerRow, c.Layout.ElementsPerRow, c.Layout.ElementWidthBytes, want, ReasonRawLengthRowCountMismatch)
	}
	return nil
}

func checkedMul(a, b int, context string) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("columnlayout: %s negative operands %d*%d", context, a, b)
	}
	if b != 0 && a > int(^uint(0)>>1)/b {
		return 0, fmt.Errorf("columnlayout: %s overflow %d*%d", context, a, b)
	}
	return a * b, nil
}
