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
	OpDictionaryInList         Operation = "predicate.dictionary_in_list"
	OpDictionaryCategory       Operation = "predicate.dictionary_category"
	OpDictionaryRange          Operation = "predicate.dictionary_range"
	OpVectorSimilarity         Operation = "predicate.vector_similarity"
	OpVectorDotProduct         Operation = "vector.dot_product"
	OpVectorDirectPayload      Operation = "direct.vector_payload"
	OpAdjacencyTraversal       Operation = "graph.adjacency_traversal"
	OpAdjacencyDirectPayload   Operation = "direct.adjacency_payload"
	OpCountRows                Operation = "aggregate.count_rows"
	OpCountNonNull             Operation = "aggregate.count_non_null"
	OpSum                      Operation = "aggregate.sum"
	OpAvg                      Operation = "aggregate.avg"
	OpMin                      Operation = "aggregate.min"
	OpMax                      Operation = "aggregate.max"
	OpBoolCounts               Operation = "aggregate.bool_counts"
	OpDictionaryGroupBy        Operation = "aggregate.dictionary_group_by"
	OpDictionaryCount          Operation = "aggregate.dictionary_count"
	OpDictionaryCountDistinct  Operation = "aggregate.dictionary_count_distinct"
	OpVectorMetrics            Operation = "aggregate.vector_metrics"
	OpAdjacencyMetrics         Operation = "aggregate.adjacency_metrics"
	OpStatsMinMax              Operation = "stats.min_max"
	OpStatsSum                 Operation = "stats.sum"
	OpPruneEquality            Operation = "pruning.equality"
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
	ReasonPruningPayloadUnsupported           ReasonCode = "pruning_payload_unsupported"
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
	return []typedcolumn.ColumnType{typedcolumn.ColumnTypeInt64, typedcolumn.ColumnTypeLowCardinalityCode, typedcolumn.ColumnTypeBool, typedcolumn.ColumnTypeFloat32, typedcolumn.ColumnTypeFloat64, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.ColumnTypeAdjacencyList}
}

func Encodings() []typedcolumn.Encoding {
	return []typedcolumn.Encoding{typedcolumn.EncodingRawInt64, typedcolumn.EncodingDeltaVarint, typedcolumn.EncodingDoubleDeltaVarint, typedcolumn.EncodingNullableInt64, typedcolumn.EncodingBoolBitpackRLE, typedcolumn.EncodingLowCardinalityUint32, typedcolumn.EncodingRawFloat32Vector, typedcolumn.EncodingRawUint32Dense, typedcolumn.EncodingRawFloat32, typedcolumn.EncodingRawFloat64}
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
		case OpSum, OpAvg, OpMin, OpMax, OpStatsSum, OpStatsMinMax, OpPruneEquality, OpPruneOrderedRange:
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
	case typedcolumn.ColumnTypeFloat32:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat32
	case typedcolumn.ColumnTypeFloat64:
		return ReasonEncodingPhysicalMismatch, encoding == typedcolumn.EncodingRawFloat64
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
	case OpAllRows, OpEquality, OpInequality, OpOrderedRange, OpInList, OpStatsMinMax, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier:
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
	case OpPruneEquality:
		return Fallback(op, ReasonPruningPayloadUnsupported, "bool pruning payload is deferred to the bool type-family slice")
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
		case OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier:
			return Unsupported(op, ReasonFloatRawInt64BitPattern, "raw int64 float bit patterns do not provide numeric float semantics; NaN, signed-zero, infinity, and precision/accumulation policy requires native float encoding")
		case OpAllRows:
			return Supported(op)
		case OpCountRows, OpCountNonNull:
			return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
		case OpEquality, OpInequality, OpInList:
			return Fallback(op, ReasonNativeFloatLayoutMissing, "native float equality semantics require NaN, signed-zero, infinity, and precision/accumulation rules")
		default:
			return Unsupported(op, ReasonNativeFloatLayoutMissing, "native float semantic capability is not implemented; NaN, signed-zero, infinity, and precision/accumulation policy is deferred")
		}
	}
	if desc.Logical == LogicalFloat32 && (desc.Physical != typedcolumn.ColumnTypeFloat32 || desc.Encoding != typedcolumn.EncodingRawFloat32) {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "float32 semantics require native float32 physical type and raw_float32 encoding")
	}
	if desc.Logical == LogicalDouble && (desc.Physical != typedcolumn.ColumnTypeFloat64 || desc.Encoding != typedcolumn.EncodingRawFloat64) {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "double semantics require native float64 physical type and raw_float64 encoding")
	}
	if (desc.Logical != LogicalFloat32 || desc.Physical != typedcolumn.ColumnTypeFloat32 || desc.Encoding != typedcolumn.EncodingRawFloat32) && (desc.Logical != LogicalDouble || desc.Physical != typedcolumn.ColumnTypeFloat64 || desc.Encoding != typedcolumn.EncodingRawFloat64) {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "native float semantics require matching float32/float64 physical encoding")
	}
	scalar := "float64"
	if desc.Logical == LogicalFloat32 {
		scalar = "float32"
	}
	switch op {
	case OpAllRows, OpDirectScalarValueCarrier:
		return Supported(op)
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpEquality, OpInequality, OpInList:
		return Fallback(op, ReasonNativeFloatLayoutMissing, scalar+" equality semantics over NaN payloads and signed zeroes are deferred to the scalar float type-family slice")
	case OpOrderedRange, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange:
		return Unsupported(op, ReasonNativeFloatLayoutMissing, scalar+" numeric semantics over NaNs, infinities, signed zeroes, and accumulation policy are deferred to the scalar float type-family slice")
	case OpIsNull, OpIsNotNull:
		return Unsupported(op, ReasonNotNullable, "non-null "+scalar+" column")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not a native float semantic capability")
	}
}

func stringCapability(desc Descriptor, op Operation) Capability {
	if desc.Physical != typedcolumn.ColumnTypeLowCardinalityCode {
		return Unsupported(op, ReasonLogicalPhysicalMismatch, "string semantics require low-cardinality dictionary-code physical type")
	}
	switch op {
	case OpAllRows, OpEquality, OpInequality, OpInList, OpDictionaryEquality, OpDictionaryInList, OpDictionaryCategory:
		return Supported(op)
	case OpPruneEquality:
		return Fallback(op, ReasonPruningPayloadUnsupported, "string pruning payload is deferred to the string type-family slice")
	case OpCountRows, OpCountNonNull:
		return SupportedResult(op, ResultSemantics{ResultType: "int64", OverflowPolicy: "checked row count"})
	case OpDictionaryGroupBy:
		return SupportedResult(op, ResultSemantics{ResultType: "groups", GroupKey: "dictionary string value; dictionary codes are part-local unless identity metadata proves otherwise"})
	case OpDictionaryCount:
		return SupportedResult(op, ResultSemantics{ResultType: "groups with int64 counts", OverflowPolicy: "checked row count", GroupKey: "dictionary string value; count by code is valid only within matching dictionary identity"})
	case OpDictionaryCountDistinct:
		return SupportedResult(op, ResultSemantics{ResultType: "groups with int64 distinct counts", OverflowPolicy: "checked row and distinct bitmap counts", GroupKey: "dictionary string value; distinct dictionaries must be translated by value unless identity metadata matches"})
	case OpOrderedRange, OpStringPrefix, OpStringLexicalRange:
		if !desc.DictionaryOrder {
			return Fallback(op, ReasonDictionaryOrderUnproven, "lexical string comparison requires value-level fallback; dictionary code order is not proof of lexical value order")
		}
		if desc.DictionaryCollation == "" {
			return Fallback(op, ReasonDictionaryCollationUnproven, "lexical string comparison requires value-level fallback unless dictionary order has an explicit collation identity")
		}
		return Supported(op)
	case OpDictionaryRange, OpMin, OpMax, OpStatsMinMax, OpPruneOrderedRange:
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
	case OpVectorDirectPayload:
		return Supported(op)
	case OpVectorSimilarity:
		return SupportedResult(op, ResultSemantics{ResultType: "top-k vector scores", Accumulator: "specialized vector kernel", Comparison: "vector metric order"})
	case OpVectorDotProduct:
		return SupportedResult(op, ResultSemantics{ResultType: "float32/float64 score", Accumulator: "specialized dot-product kernel", Precision: "kernel-defined vector precision"})
	case OpVectorMetrics:
		return SupportedResult(op, ResultSemantics{ResultType: "vector-specific metrics", Accumulator: "specialized vector metric collector"})
	case OpAdjacencyTraversal, OpAdjacencyDirectPayload, OpAdjacencyMetrics:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not a float32_vector semantic capability")
	case OpEquality, OpInequality, OpOrderedRange, OpInList, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier:
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
	case OpAdjacencyDirectPayload:
		return Fallback(op, ReasonAdjacencyCapabilityDeferred, "adjacency_list direct payload views are deferred to #1901")
	case OpAdjacencyTraversal:
		return SupportedResult(op, ResultSemantics{ResultType: "candidate row selection", Accumulator: "specialized graph traversal", GroupKey: "adjacency ordinal"})
	case OpAdjacencyMetrics:
		return SupportedResult(op, ResultSemantics{ResultType: "graph/adjacency-specific metrics", Accumulator: "specialized adjacency metric collector"})
	case OpVectorSimilarity, OpVectorDotProduct, OpVectorDirectPayload, OpVectorMetrics:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not an adjacency_list semantic capability")
	case OpEquality, OpInequality, OpOrderedRange, OpInList, OpSum, OpAvg, OpMin, OpMax, OpStatsMinMax, OpStatsSum, OpPruneEquality, OpPruneOrderedRange, OpDirectScalarValueCarrier:
		return Unsupported(op, ReasonAdjacencyScalarOperationUnsupported, "adjacency columns are not scalar comparable or scalar aggregate values")
	default:
		return Unsupported(op, ReasonOperationUnsupported, "operation is not an adjacency semantic capability")
	}
}
