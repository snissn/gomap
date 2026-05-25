package columnsemantics

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// LogicalType names the collection-level value domain. It is intentionally
// separate from typedcolumn.ColumnType because several current encodings share
// carriers without sharing value semantics (for example float bits in int64).
type LogicalType string

const (
	LogicalBool          LogicalType = "bool"
	LogicalInt64         LogicalType = "int64"
	LogicalFloat32       LogicalType = "float32"
	LogicalDouble        LogicalType = "double"
	LogicalString        LogicalType = "string"
	LogicalFloat32Vector LogicalType = "float32_vector"
	LogicalAdjacencyList LogicalType = "adjacency_list"
)

// Operation identifies the semantic question answered by the matrix. Query
// planners resolve these at prepare time and then dispatch to concrete kernels;
// callers must not consult this package per row in hot loops.
type Operation string

const (
	OpAllRows                  Operation = "predicate.all_rows"
	OpEquality                 Operation = "predicate.equality"
	OpInequality               Operation = "predicate.inequality"
	OpOrderedRange             Operation = "predicate.ordered_range"
	OpInList                   Operation = "predicate.in_list"
	OpIsNull                   Operation = "predicate.is_null"
	OpIsNotNull                Operation = "predicate.is_not_null"
	OpStringPrefix             Operation = "predicate.string_prefix"
	OpStringLexicalRange       Operation = "predicate.string_lexical_range"
	OpUnknownPredicateKind     Operation = "predicate.unknown_kind"
	OpDictionaryEquality       Operation = "predicate.dictionary_equality"
	OpDictionaryRange          Operation = "predicate.dictionary_range"
	OpVectorSimilarity         Operation = "predicate.vector_similarity"
	OpCountRows                Operation = "aggregate.count_rows"
	OpCountNonNull             Operation = "aggregate.count_non_null"
	OpSum                      Operation = "aggregate.sum"
	OpAvg                      Operation = "aggregate.avg"
	OpMin                      Operation = "aggregate.min"
	OpMax                      Operation = "aggregate.max"
	OpBoolCounts               Operation = "aggregate.bool_counts"
	OpDictionaryGroupBy        Operation = "aggregate.dictionary_group_by"
	OpVectorMetrics            Operation = "aggregate.vector_metrics"
	OpStatsMinMax              Operation = "stats.min_max"
	OpStatsSum                 Operation = "stats.sum"
	OpPruneOrderedRange        Operation = "pruning.ordered_range"
	OpDirectScalarValueCarrier Operation = "direct.scalar_value_carrier"
)

// Status is a trinary capability outcome. Fallback is explicit: a planner may
// use a non-optimized implementation, but only outside direct typed-column hot
// loops that require StatusSupported.
type Status string

const (
	StatusSupported   Status = "supported"
	StatusUnsupported Status = "unsupported"
	StatusFallback    Status = "fallback"
)

// ResolutionPhase records where a caller is allowed to make the semantic
// dispatch decision. Current #1843 consumers resolve once during prepare; hot
// row loops must already have selected a concrete scan/reducer implementation.
type ResolutionPhase string

const (
	PhasePrepare ResolutionPhase = "prepare"
)

// ResultSemantics makes aggregate result shape explicit for capabilities that
// expose aggregate output. Empty fields mean the operation is not an aggregate
// result capability.
type ResultSemantics struct {
	ResultType     string
	Accumulator    string
	OverflowPolicy string
	Comparison     string
	GroupKey       string
	Precision      string
}

// ReasonCode is a stable diagnostic token. Add new values rather than changing
// existing strings; tests and planner diagnostics may key on them.
type ReasonCode string

const (
	ReasonSupported                           ReasonCode = "supported"
	ReasonUnknownLogicalType                  ReasonCode = "unknown_logical_type"
	ReasonUnsupportedPhysicalType             ReasonCode = "unsupported_physical_type"
	ReasonUnsupportedEncoding                 ReasonCode = "unsupported_encoding"
	ReasonLogicalPhysicalMismatch             ReasonCode = "logical_physical_mismatch"
	ReasonEncodingPhysicalMismatch            ReasonCode = "encoding_physical_mismatch"
	ReasonOperationUnsupported                ReasonCode = "operation_unsupported"
	ReasonFloatRawInt64BitPattern             ReasonCode = "float_raw_int64_bit_pattern"
	ReasonNativeFloatLayoutMissing            ReasonCode = "native_float_layout_missing"
	ReasonDictionaryOrderUnproven             ReasonCode = "dictionary_order_unproven"
	ReasonDictionaryCollationUnproven         ReasonCode = "dictionary_collation_unproven"
	ReasonNullableCarrierValueSemantics       ReasonCode = "nullable_carrier_value_semantics"
	ReasonNullableCarrierAggregateSemantics   ReasonCode = "nullable_carrier_aggregate_semantics"
	ReasonNotNullable                         ReasonCode = "not_nullable"
	ReasonVectorScalarOperationUnsupported    ReasonCode = "vector_scalar_operation_unsupported"
	ReasonAdjacencyScalarOperationUnsupported ReasonCode = "adjacency_scalar_operation_unsupported"
	ReasonVectorCapabilityDeferred            ReasonCode = "vector_capability_deferred"
	ReasonAdjacencyCapabilityDeferred         ReasonCode = "adjacency_capability_deferred"
	ReasonBoolRangeUnsupported                ReasonCode = "bool_range_unsupported"
	ReasonStatsPayloadUnsupported             ReasonCode = "stats_payload_unsupported"
)

// Descriptor binds logical semantics to the current typedcolumn physical shape.
type Descriptor struct {
	Logical             LogicalType
	Physical            typedcolumn.ColumnType
	Encoding            typedcolumn.Encoding
	Nullable            bool
	DictionaryOrder     bool
	DictionaryCollation string
}

type Capability struct {
	Operation Operation
	Status    Status
	Reason    ReasonCode
	Message   string
	Phase     ResolutionPhase
	Result    ResultSemantics
}

func (c Capability) Supported() bool { return c.Status == StatusSupported }

func (c Capability) Error() string {
	if c.Status == StatusSupported {
		return ""
	}
	if c.Message != "" {
		return fmt.Sprintf("%s: %s", c.Reason, c.Message)
	}
	return string(c.Reason)
}

func Supported(op Operation) Capability {
	return Capability{Operation: op, Status: StatusSupported, Reason: ReasonSupported, Phase: PhasePrepare}
}

func SupportedResult(op Operation, result ResultSemantics) Capability {
	cap := Supported(op)
	cap.Result = result
	return cap
}

func Unsupported(op Operation, reason ReasonCode, msg string) Capability {
	return Capability{Operation: op, Status: StatusUnsupported, Reason: reason, Message: msg, Phase: PhasePrepare}
}

func Fallback(op Operation, reason ReasonCode, msg string) Capability {
	return Capability{Operation: op, Status: StatusFallback, Reason: reason, Message: msg, Phase: PhasePrepare}
}

func LogicalTypes() []LogicalType {
	return []LogicalType{LogicalBool, LogicalInt64, LogicalFloat32, LogicalDouble, LogicalString, LogicalFloat32Vector, LogicalAdjacencyList}
}

func ColumnTypes() []typedcolumn.ColumnType {
	return []typedcolumn.ColumnType{typedcolumn.ColumnTypeInt64, typedcolumn.ColumnTypeLowCardinalityCode, typedcolumn.ColumnTypeBool, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.ColumnTypeAdjacencyList}
}

func Encodings() []typedcolumn.Encoding {
	return []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint, typedcolumn.EncodingNullableInt64, typedcolumn.EncodingBoolBitpackRLE, typedcolumn.EncodingLowCardinalityUint32, typedcolumn.EncodingRawFloat32Vector, typedcolumn.EncodingRawUint32Dense}
}

func IsKnownLogicalType(t LogicalType) bool {
	for _, known := range LogicalTypes() {
		if t == known {
			return true
		}
	}
	return false
}

func IsKnownColumnType(t typedcolumn.ColumnType) bool {
	for _, known := range ColumnTypes() {
		if t == known {
			return true
		}
	}
	return false
}

func IsKnownEncoding(e typedcolumn.Encoding) bool {
	for _, known := range Encodings() {
		if e == known {
			return true
		}
	}
	return false
}

func CapabilityFor(desc Descriptor, op Operation) Capability {
	if !IsKnownLogicalType(desc.Logical) {
		return Unsupported(op, ReasonUnknownLogicalType, fmt.Sprintf("logical_type=%q", desc.Logical))
	}
	if !IsKnownColumnType(desc.Physical) {
		return Unsupported(op, ReasonUnsupportedPhysicalType, fmt.Sprintf("physical_type=%q", desc.Physical))
	}
	if !IsKnownEncoding(desc.Encoding) {
		return Unsupported(op, ReasonUnsupportedEncoding, fmt.Sprintf("encoding=%s", desc.Encoding))
	}
	if reason, ok := validatePhysicalEncoding(desc.Physical, desc.Encoding); !ok {
		return Unsupported(op, reason, fmt.Sprintf("physical_type=%s encoding=%s", desc.Physical, desc.Encoding))
	}
	if desc.Nullable && desc.Encoding != typedcolumn.EncodingNullableInt64 {
		return Unsupported(op, ReasonEncodingPhysicalMismatch, fmt.Sprintf("nullable descriptor requires encoding=%s got=%s", typedcolumn.EncodingNullableInt64, desc.Encoding))
	}
	if desc.Encoding == typedcolumn.EncodingNullableInt64 {
		switch op {
		case OpCountRows, OpCountNonNull:
			return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
		case OpIsNull, OpIsNotNull:
			return Supported(op)
		case OpSum, OpAvg, OpMin, OpMax, OpStatsSum, OpStatsMinMax, OpPruneOrderedRange:
			return Fallback(op, ReasonNullableCarrierAggregateSemantics, "nullable/default carrier requires explicit count/value aggregate semantics")
		default:
			return Fallback(op, ReasonNullableCarrierValueSemantics, "nullable/default carrier requires explicit null/default filtering before value operation")
		}
	}

	switch desc.Logical {
	case LogicalInt64:
		return int64Capability(desc, op)
	case LogicalBool:
		return boolCapability(desc, op)
	case LogicalFloat32, LogicalDouble:
		return floatCapability(desc, op)
	case LogicalString:
		return stringCapability(desc, op)
	case LogicalFloat32Vector:
		return vectorCapability(desc, op)
	case LogicalAdjacencyList:
		return adjacencyCapability(desc, op)
	default:
		return Unsupported(op, ReasonUnknownLogicalType, fmt.Sprintf("logical_type=%q", desc.Logical))
	}
}

func RequireSupported(desc Descriptor, op Operation) error {
	cap := CapabilityFor(desc, op)
	if cap.Supported() {
		return nil
	}
	return fmt.Errorf("column semantics capability %s status=%s reason=%s", op, cap.Status, cap.Error())
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

func int64Capability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeInt64 {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "int64 semantics require typedcolumn int64 physical type")
	}
	switch op {
	case OpAllRows, OpEquality, OpInequality, OpOrderedRange, OpInList, OpStatsMinMax, OpPruneOrderedRange, OpDirectScalarValueCarrier:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpSum:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", Accumulator: "int64", OverflowPolicy: "checked"})
	case OpAvg:
		return SupportedResult(op, ResultSemantics{ResultType: "float64", Accumulator: "checked int64 sum and int64 count", OverflowPolicy: "checked sum", Precision: "float64 quotient"})
	case OpMin, OpMax:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", Comparison: "signed int64 logical order"})
	case OpStatsSum:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", Accumulator: "durable int64 block/part stats payload", OverflowPolicy: "checked"})
	case OpIsNull, OpIsNotNull:
		return Unsupported(op, ReasonNotNullable, "non-null int64 column")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not an int64 semantic capability")
	}
}

func boolCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeBool {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "bool semantics require typedcolumn bool physical type")
	}
	switch op {
	case OpAllRows, OpEquality, OpInequality, OpInList, OpDirectScalarValueCarrier:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpBoolCounts:
		return SupportedResult(op, ResultSemantics{ResultType: "int64 counts", GroupKey: "bool false/true/null"})
	case OpOrderedRange, OpPruneOrderedRange, OpMin, OpMax, OpStatsMinMax:
		return Unsupported(op, ReasonBoolRangeUnsupported, "bool ordering is not exposed as scalar range semantics")
	case OpIsNull, OpIsNotNull:
		return Unsupported(op, ReasonNotNullable, "non-null bool column")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not a bool semantic capability")
	}
}

func floatCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical == typedcolumn.ColumnTypeInt64 && desc.Encoding == typedcolumn.EncodingRawInt64 {
		switch op {
		case OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneOrderedRange, OpDirectScalarValueCarrier:
			return Unsupported(op, ReasonFloatRawInt64BitPattern, "raw int64 float bit patterns do not provide numeric float semantics")
		case OpAllRows:
			return Supported(op)
		case OpCountRows, OpCountNonNull:
			return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
		case OpEquality, OpInequality, OpInList:
			return Fallback(op, ReasonNativeFloatLayoutMissing, "native float equality semantics require NaN/signed-zero rules and a float layout")
		default:
			return Unsupported(op, ReasonNativeFloatLayoutMissing, "native float semantic capability is not implemented")
		}
	}
	return Unsupported(op, ReasonLogicalPhysicalMismatch, "current float columns are only represented as raw int64 bit-pattern carriers")
}

func stringCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeLowCardinalityCode {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "string semantics require low-cardinality dictionary-code physical type")
	}
	switch op {
	case OpAllRows, OpEquality, OpInequality, OpInList, OpDictionaryEquality:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpDictionaryGroupBy:
		return SupportedResult(op, ResultSemantics{ResultType: "groups", GroupKey: "dictionary string value with stable dictionary identity"})
	case OpOrderedRange, OpDictionaryRange, OpStringPrefix, OpStringLexicalRange, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange:
		if !desc.DictionaryOrder {
			return Unsupported(op, ReasonDictionaryOrderUnproven, "dictionary code order is not proof of lexical value order")
		}
		if desc.DictionaryCollation == "" {
			return Unsupported(op, ReasonDictionaryCollationUnproven, "dictionary order requires an explicit collation identity")
		}
		return Supported(op)
	case OpIsNull, OpIsNotNull:
		return Unsupported(op, ReasonNotNullable, "non-null string column")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not a string semantic capability")
	}
}

func vectorCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeFloat32Vector {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "float32_vector semantics require dense float32 vector physical type")
	}
	switch op {
	case OpAllRows:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpVectorSimilarity, OpVectorMetrics:
		return Fallback(op, ReasonVectorCapabilityDeferred, "vector-specific kernels are deferred")
	case OpEquality, OpInequality, OpOrderedRange, OpInList, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneOrderedRange, OpDirectScalarValueCarrier:
		return Unsupported(op, ReasonVectorScalarOperationUnsupported, "vector columns are not scalar comparable or scalar aggregate values")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not a vector semantic capability")
	}
}

func adjacencyCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeAdjacencyList {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "adjacency_list semantics require dense uint32 adjacency physical type")
	}
	switch op {
	case OpAllRows:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpVectorSimilarity, OpVectorMetrics:
		return Fallback(op, ReasonAdjacencyCapabilityDeferred, "graph/vector-specific adjacency capabilities are deferred")
	case OpEquality, OpInequality, OpOrderedRange, OpInList, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneOrderedRange, OpDirectScalarValueCarrier:
		return Unsupported(op, ReasonAdjacencyScalarOperationUnsupported, "adjacency columns are not scalar comparable or scalar aggregate values")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not an adjacency semantic capability")
	}
}
