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
	ReasonSupported                   ReasonCode = "supported"
	ReasonUnknownLogicalType          ReasonCode = "layout_unknown_logical_type"
	ReasonUnsupportedPhysicalType     ReasonCode = "layout_unsupported_physical_type"
	ReasonUnsupportedEncoding         ReasonCode = "layout_unsupported_encoding"
	ReasonUnsupportedCompression      ReasonCode = "layout_unsupported_compression"
	ReasonLogicalPhysicalMismatch     ReasonCode = "layout_logical_physical_mismatch"
	ReasonEncodingPhysicalMismatch    ReasonCode = "layout_encoding_physical_mismatch"
	ReasonVariableWidthNoDirectView   ReasonCode = "layout_variable_width_no_direct_view"
	ReasonCompressedDirectView        ReasonCode = "layout_compressed_direct_view"
	ReasonLengthMultipleMismatch      ReasonCode = "layout_length_multiple_mismatch"
	ReasonRawLengthRowCountMismatch   ReasonCode = "layout_raw_length_row_count_mismatch"
	ReasonNullDefaultWrapperRequired  ReasonCode = "layout_null_default_wrapper_required"
	ReasonDictionaryOrderUnproven     ReasonCode = "layout_dictionary_order_unproven"
	ReasonDictionaryCollationUnproven ReasonCode = "layout_dictionary_collation_unproven"
	ReasonFloatBitPatternNotNumeric   ReasonCode = "layout_float_bit_pattern_not_numeric"
	ReasonVectorScalarUnsupported     ReasonCode = "layout_vector_scalar_unsupported"
	ReasonAdjacencyScalarUnsupported  ReasonCode = "layout_adjacency_scalar_unsupported"
	ReasonStatsPayloadUnsupported     ReasonCode = "layout_stats_payload_unsupported"
	ReasonPruningPayloadUnsupported   ReasonCode = "layout_pruning_payload_unsupported"
	ReasonOperationUnsupported        ReasonCode = "layout_operation_unsupported"
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
		if desc.Logical == columnsemantics.LogicalInt64 && desc.Physical == typedcolumn.ColumnTypeInt64 && desc.Compression == typedcolumn.CompressionNone && !caps.Wrappers.Nullable {
			caps.Reducers.Int64FixedWidthRaw = true
			caps.Reducers.Int64NumericAggregate = true
			caps.Stats.MinMax = true
			caps.Stats.Sum = true
			caps.Pruning.OrderedMinMax = true
			caps.Pruning.ValueRows = true
		}
	case typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint:
		caps.Layout.Kind = LayoutVariableWidth
		caps.Layout.VariableWidth = true
		caps.Layout.Endian = EndianCodecDefined
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonVariableWidthNoDirectView, RequiresUncompressed: true, RequiresRowCount: true, RequiresNoNulls: true, RequiresNoDefaults: true, ValidationBoundary: "prepare"}
		if desc.Logical == columnsemantics.LogicalInt64 && desc.Physical == typedcolumn.ColumnTypeInt64 && desc.Compression == typedcolumn.CompressionNone && !caps.Wrappers.Nullable {
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
	case typedcolumn.EncodingRawFloat32Vector:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		caps.DirectView = directView(desc, 4, EndianLittle, 4)
		if desc.Logical == columnsemantics.LogicalFloat32Vector && desc.Physical == typedcolumn.ColumnTypeFloat32Vector {
			caps.Reducers.VectorMetrics = true
			caps.Stats.VectorStats = true
			caps.Pruning.VectorIndex = true
		}
	case typedcolumn.EncodingRawUint32Dense:
		caps.Layout.Kind = LayoutFixedWidth
		caps.Layout.FixedWidth = true
		caps.Layout.ElementWidthBytes = 4
		caps.Layout.Endian = EndianLittle
		caps.Layout.AlignmentBytes = 4
		caps.Layout.LengthMultipleBytes = 4
		caps.DirectView = directView(desc, 4, EndianLittle, 4)
		if desc.Logical == columnsemantics.LogicalAdjacencyList && desc.Physical == typedcolumn.ColumnTypeAdjacencyList {
			caps.Reducers.AdjacencyMetrics = true
			caps.Pruning.AdjacencyIndex = true
		}
	default:
		caps.DirectView = DirectViewCapability{Eligible: false, Reason: ReasonUnsupportedEncoding, ValidationBoundary: "prepare"}
	}
	return caps
}

func rawInt64NonInt64DirectViewReason(desc Descriptor) ReasonCode {
	if desc.Logical == columnsemantics.LogicalFloat32 || desc.Logical == columnsemantics.LogicalDouble {
		return ReasonFloatBitPatternNotNumeric
	}
	return ReasonLogicalPhysicalMismatch
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
		case OpDirectView:
			return Unsupported(op, ReasonCompressedDirectView, fmt.Sprintf("compression=%s", c.Descriptor.Compression))
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpScalarNumericAggregate:
			return Unsupported(op, ReasonUnsupportedCompression, fmt.Sprintf("compression=%s", c.Descriptor.Compression))
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalFloat32 || c.Descriptor.Logical == columnsemantics.LogicalDouble {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpScalarNumericAggregate:
			return Unsupported(op, ReasonFloatBitPatternNotNumeric, "float bit-pattern storage is not an int64 numeric layout")
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalFloat32Vector {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonVectorScalarUnsupported, "vector layouts reject scalar aggregate/range shortcuts")
		}
	}
	if c.Descriptor.Logical == columnsemantics.LogicalAdjacencyList {
		switch op {
		case OpInt64NumericReducer, OpInt64RangePredicate, OpMinMaxPruning, OpValueRowPruning, OpMinMaxStats, OpSumStats, OpLexicalRangePredicate, OpScalarNumericAggregate:
			return Unsupported(op, ReasonAdjacencyScalarUnsupported, "adjacency layouts reject scalar aggregate/range shortcuts")
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
		if c.Descriptor.Logical != columnsemantics.LogicalFloat32Vector {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not a vector payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "vector payload layout is not eligible for direct view")
	case OpAdjacencyDirectView:
		if c.Descriptor.Logical != columnsemantics.LogicalAdjacencyList {
			return Unsupported(op, ReasonOperationUnsupported, "layout is not an adjacency payload")
		}
		if c.DirectView.Eligible {
			return Supported(op)
		}
		return Unsupported(op, c.DirectView.Reason, "adjacency payload layout is not eligible for direct view")
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
		return c.Supports(OpInt64RangePredicate)
	case columnsemantics.OpOrderedRange:
		if c.Descriptor.Logical == columnsemantics.LogicalString {
			return c.Supports(OpLexicalRangePredicate)
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
	case columnsemantics.LogicalFloat32, columnsemantics.LogicalDouble:
		return Unsupported(op, ReasonFloatBitPatternNotNumeric, "float bit-pattern storage is not a direct scalar value carrier")
	case columnsemantics.LogicalFloat32Vector:
		return Unsupported(op, ReasonVectorScalarUnsupported, "vector layouts reject scalar direct-value carriers")
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
	case typedcolumn.CompressionNone:
	case typedcolumn.CompressionSnappy, typedcolumn.CompressionLZ4:
		return Unsupported(op, ReasonUnsupportedCompression, fmt.Sprintf("compression=%s", desc.Compression))
	default:
		return Unsupported(op, ReasonUnsupportedCompression, fmt.Sprintf("compression=%s", desc.Compression))
	}
	if reason, ok := validatePhysicalEncoding(desc.Physical, desc.Encoding); !ok {
		return Unsupported(op, reason, fmt.Sprintf("physical_type=%s encoding=%s", desc.Physical, desc.Encoding))
	}
	if desc.Nullable && desc.Encoding != typedcolumn.EncodingNullableInt64 {
		return Unsupported(op, ReasonEncodingPhysicalMismatch, fmt.Sprintf("nullable layout requires encoding=%s got=%s", typedcolumn.EncodingNullableInt64, desc.Encoding))
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
	case typedcolumn.ColumnTypeFloat32Vector:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat32Vector
	case typedcolumn.ColumnTypeAdjacencyList:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawUint32Dense
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
	if c.Layout.ElementWidthBytes <= 0 || c.Layout.ElementsPerRow <= 0 {
		return fmt.Errorf("columnlayout: invalid fixed-width contract width=%d elements_per_row=%d", c.Layout.ElementWidthBytes, c.Layout.ElementsPerRow)
	}
	elements, err := checkedMul(g.Rows, c.Layout.ElementsPerRow, "fixed-width elements")
	if err != nil {
		return err
	}
	want, err := checkedMul(elements, c.Layout.ElementWidthBytes, "fixed-width raw bytes")
	if err != nil {
		return err
	}
	if g.RawBytes != want || g.StoredBytes != want {
		return fmt.Errorf("columnlayout: raw/stored bytes raw=%d stored=%d want rows=%d*elements=%d*width=%d=%d: %s", g.RawBytes, g.StoredBytes, g.Rows, c.Layout.ElementsPerRow, c.Layout.ElementWidthBytes, want, ReasonRawLengthRowCountMismatch)
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
