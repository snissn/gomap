// Package typeddecode contains shared typed-column fast-decode planning and
// validated direct-view helpers. It deliberately keeps unsafe byte
// reinterpretation inside TreeDB/internal/mappedresource and only returns direct
// views after semantic, layout, writer-certification, row-count, endian,
// alignment, and lifetime checks have all succeeded.
package typeddecode

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// Path is the explicit fast-decode path selected for one column/operator/block.
type Path string

const (
	PathDirectView  Path = "direct_view"
	PathStreaming   Path = "streaming"
	PathMaterialize Path = "materialize"
	PathUnsupported Path = "unsupported"
)

// Reason is a stable planning/fallback diagnostic token. Add new values rather
// than changing strings; tests and PR benchmark output may key on them.
type Reason string

const (
	ReasonSupported               Reason = "supported"
	ReasonUnsupportedOperation    Reason = "unsupported_operation"
	ReasonLayoutCapability        Reason = "layout_capability"
	ReasonNotWriterCertified      Reason = "writer_certification_missing"
	ReasonCompressed              Reason = "compressed"
	ReasonVariableWidth           Reason = "variable_width"
	ReasonNullableWrapper         Reason = "nullable_default_wrapper"
	ReasonWrongEndian             Reason = "wrong_endian"
	ReasonLengthMultipleMismatch  Reason = "length_multiple_mismatch"
	ReasonPayloadLengthMismatch   Reason = "payload_length_mismatch"
	ReasonRowCountMismatch        Reason = "row_count_mismatch"
	ReasonDimensionMismatch       Reason = "dimension_mismatch"
	ReasonOffsetsCountMismatch    Reason = "offsets_count_mismatch"
	ReasonOffsetsStartMismatch    Reason = "offsets_start_mismatch"
	ReasonOffsetsNonMonotonic     Reason = "offsets_non_monotonic"
	ReasonOffsetsGoIntRange       Reason = "offsets_go_int_range"
	ReasonValuesLengthMismatch    Reason = "values_length_mismatch"
	ReasonAbsoluteOffsetUnaligned Reason = "absolute_offset_unaligned"
	ReasonActualPointerUnaligned  Reason = "actual_pointer_unaligned"
	ReasonDirectViewDeferred      Reason = "direct_view_deferred"
	// ReasonUnaligned is retained for compatibility with older diagnostics;
	// new direct-view validation distinguishes absolute storage offset alignment
	// from actual Go pointer alignment.
	ReasonUnaligned                  Reason = "unaligned"
	ReasonNilHandle                  Reason = "nil_handle"
	ReasonStaleHandle                Reason = "stale_handle"
	ReasonHandleSourceUnsupported    Reason = "handle_source_unsupported"
	ReasonDictionarySemanticsMissing Reason = "dictionary_semantics_missing"
	ReasonMaterializationRequired    Reason = "materialization_required"
	ReasonValidationFailed           Reason = "validation_failed"
	ReasonOwnerMismatch              Reason = "owner_mismatch"
	ReasonRoleMismatch               Reason = "role_mismatch"
	ReasonColumnMismatch             Reason = "column_mismatch"
	ReasonTypeEncodingMismatch       Reason = "type_encoding_mismatch"
	ReasonResourceMismatch           Reason = "resource_mismatch"
)

// Status describes the outcome of planning or validating a fast-decode path.
type Status struct {
	Path    Path
	Reason  Reason
	Message string
	Err     error
}

func DirectStatus() Status { return Status{Path: PathDirectView, Reason: ReasonSupported} }
func StreamingStatus(reason Reason, msg string) Status {
	return Status{Path: PathStreaming, Reason: reason, Message: msg}
}
func MaterializeStatus(reason Reason, msg string) Status {
	return Status{Path: PathMaterialize, Reason: reason, Message: msg}
}
func UnsupportedStatus(reason Reason, msg string) Status {
	return Status{Path: PathUnsupported, Reason: reason, Message: msg}
}

func (s Status) Direct() bool {
	return s.Path == PathDirectView && s.Reason == ReasonSupported && s.Err == nil
}
func (s Status) Streaming() bool   { return s.Path == PathStreaming }
func (s Status) Unsupported() bool { return s.Path == PathUnsupported }

func (s Status) String() string {
	if s.Err != nil {
		return s.Err.Error()
	}
	if s.Message != "" {
		return fmt.Sprintf("%s: %s", s.Reason, s.Message)
	}
	return string(s.Reason)
}

// Plan is a reusable per-column/operator decision. Per-block and per-handle
// validation still has to run before exposing a direct-view slice.
type Plan struct {
	Path              Path
	Reason            Reason
	Message           string
	ElementSize       int
	ElementsPerRow    int
	BytesPerRow       int
	BitsPerElement    int
	LogicalBitsPerRow int
	Alignment         int
	Rows              int
}

func (p Plan) Status() Status        { return Status{Path: p.Path, Reason: p.Reason, Message: p.Message} }
func (p Plan) DirectCandidate() bool { return p.Path == PathDirectView && p.Reason == ReasonSupported }

// Counter identifies a stable direct-view/fallback accounting bucket.
type Counter string

const (
	CounterMmapDirectView           Counter = "mmap_direct_view"
	CounterOffsetsMmapDirectView    Counter = "offsets_mmap_direct_view"
	CounterValuesMmapDirectView     Counter = "values_mmap_direct_view"
	CounterHeapCopyTypedView        Counter = "heap_copy_typed_view"
	CounterOffsetsHeapCopyTypedView Counter = "offsets_heap_copy_typed_view"
	CounterValuesHeapCopyTypedView  Counter = "values_heap_copy_typed_view"
	CounterScratchDecode            Counter = "scratch_decode"
	CounterStreamingFallback        Counter = "streaming_fallback"
	CounterSourceUnsupported        Counter = "source_unsupported"
	CounterCertificationFailure     Counter = "certification_failure"
	CounterAbsoluteOffsetUnaligned  Counter = "absolute_offset_unaligned"
	CounterActualPointerUnaligned   Counter = "actual_pointer_unaligned"
	CounterStaleHandle              Counter = "stale_handle"
	CounterOffsetsListValidation    Counter = "offsets_list_validation_failure"
)

// CounterVocabulary returns the stable counter names that benchmark/reporting
// code should use when distinguishing zero-copy mmap views from safe fallbacks.
func CounterVocabulary() []Counter {
	return []Counter{
		CounterMmapDirectView,
		CounterOffsetsMmapDirectView,
		CounterValuesMmapDirectView,
		CounterHeapCopyTypedView,
		CounterOffsetsHeapCopyTypedView,
		CounterValuesHeapCopyTypedView,
		CounterScratchDecode,
		CounterStreamingFallback,
		CounterSourceUnsupported,
		CounterCertificationFailure,
		CounterAbsoluteOffsetUnaligned,
		CounterActualPointerUnaligned,
		CounterStaleHandle,
		CounterOffsetsListValidation,
	}
}

// ReasonVocabulary returns stable direct-view/fallback reason names that tests,
// diagnostics, and benchmark summaries may key on.
func ReasonVocabulary() []Reason {
	return []Reason{
		ReasonSupported,
		ReasonUnsupportedOperation,
		ReasonLayoutCapability,
		ReasonNotWriterCertified,
		ReasonCompressed,
		ReasonVariableWidth,
		ReasonNullableWrapper,
		ReasonWrongEndian,
		ReasonLengthMultipleMismatch,
		ReasonPayloadLengthMismatch,
		ReasonRowCountMismatch,
		ReasonDimensionMismatch,
		ReasonOffsetsCountMismatch,
		ReasonOffsetsStartMismatch,
		ReasonOffsetsNonMonotonic,
		ReasonOffsetsGoIntRange,
		ReasonValuesLengthMismatch,
		ReasonAbsoluteOffsetUnaligned,
		ReasonActualPointerUnaligned,
		ReasonDirectViewDeferred,
		ReasonUnaligned,
		ReasonNilHandle,
		ReasonStaleHandle,
		ReasonHandleSourceUnsupported,
		ReasonDictionarySemanticsMissing,
		ReasonMaterializationRequired,
		ReasonValidationFailed,
		ReasonOwnerMismatch,
		ReasonRoleMismatch,
		ReasonColumnMismatch,
		ReasonTypeEncodingMismatch,
		ReasonResourceMismatch,
	}
}

// Counters is a small shared accounting shape for prepared scans and future
// kernels. It is caller-owned; there is no package-global cache.
type Counters struct {
	DirectViewPlans     uint64
	StreamingPlans      uint64
	MaterializePlans    uint64
	UnsupportedPlans    uint64
	DirectViewSuccesses uint64
	DirectViewFailures  uint64
	FallbackReasons     map[Reason]uint64
}

func (c *Counters) ObservePlan(p Plan)     { c.observe(p.Path, p.Reason) }
func (c *Counters) ObserveStatus(s Status) { c.observe(s.Path, s.Reason) }
func (c *Counters) ObserveDirectViewStatus(s Status) {
	if c == nil {
		return
	}
	if s.Direct() {
		c.DirectViewSuccesses++
		return
	}
	c.DirectViewFailures++
	c.observe(s.Path, s.Reason)
}

func (c *Counters) observe(path Path, reason Reason) {
	if c == nil {
		return
	}
	switch path {
	case PathDirectView:
		c.DirectViewPlans++
	case PathStreaming:
		c.StreamingPlans++
	case PathMaterialize:
		c.MaterializePlans++
	case PathUnsupported:
		c.UnsupportedPlans++
	}
	if reason != "" && reason != ReasonSupported {
		if c.FallbackReasons == nil {
			c.FallbackReasons = make(map[Reason]uint64, 1)
		}
		c.FallbackReasons[reason]++
	}
}

// Int64ReducerPlan chooses direct_view for certified raw int64 layouts and
// streaming for certified delta/double-delta or raw layouts that cannot direct
// view. Materialization is intentionally not selected by the shared int64
// aggregate planner.
func Int64ReducerPlan(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	if cap := layout.Supports(columnlayout.OpInt64NumericReducer); !cap.Supported() {
		return Plan{Path: PathUnsupported, Reason: ReasonLayoutCapability, Message: cap.Error()}
	}
	if layout.Reducers.Int64FixedWidthRaw {
		base := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 8, ElementsPerRow: 1, Alignment: 8, Rows: cert.Rows}
		if status := validateDirectViewCertification(layout, cert, 8, 1); !status.Direct() {
			if status.Unsupported() {
				return Plan{Path: PathUnsupported, Reason: status.Reason, Message: status.Message, ElementSize: 8, ElementsPerRow: 1, Alignment: 8, Rows: cert.Rows}
			}
			// Raw fixed-width int64 can still be safely streamed with explicit
			// little-endian loads when a direct view is not certified.
			return Plan{Path: PathStreaming, Reason: status.Reason, Message: status.Message, ElementSize: 8, ElementsPerRow: 1, Alignment: 8, Rows: cert.Rows}
		}
		return base
	}
	if layout.Reducers.Int64Streaming {
		if !cert.StreamingCertified {
			return Plan{Path: PathUnsupported, Reason: ReasonNotWriterCertified, Message: "int64 streaming layout is not writer-certified"}
		}
		if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
			return Plan{Path: PathUnsupported, Reason: ReasonNullableWrapper, Message: "null/default masks must be applied outside scalar int64 reducer"}
		}
		return Plan{Path: PathStreaming, Reason: ReasonVariableWidth, Message: "certified variable-width int64 streaming reducer", ElementSize: 8, ElementsPerRow: 1, Rows: cert.Rows}
	}
	return Plan{Path: PathUnsupported, Reason: ReasonUnsupportedOperation, Message: "layout does not advertise int64 reducer"}
}

func validateDirectViewCertification(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn, elementSize int, elementsPerRow int) Status {
	if status := validateDirectViewCertificationFields(cert, elementSize, elementsPerRow); !status.Direct() {
		return status
	}
	if cap := layout.Supports(columnlayout.OpDirectView); !cap.Supported() {
		return statusFromLayoutCapability(cap)
	}
	return DirectStatus()
}

func validateOffsetsListDirectViewCertification(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn) Status {
	if !cert.DirectViewCertified {
		return MaterializeStatus(ReasonNotWriterCertified, "column lacks writer-certified offsets-list direct-view contract")
	}
	if cert.Compression != typedcolumn.CompressionNone {
		return UnsupportedStatus(ReasonCompressed, fmt.Sprintf("compression=%s", cert.Compression))
	}
	if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
		return UnsupportedStatus(ReasonNullableWrapper, "null/default masks must be separate from offsets-list direct view")
	}
	if cert.Endian != typedcolumn.ColumnPartLayoutEndianLittle {
		return MaterializeStatus(ReasonWrongEndian, fmt.Sprintf("endian=%s", cert.Endian))
	}
	if cert.ElementSize != 4 || cert.Alignment <= 0 || cert.Alignment < 4 {
		return MaterializeStatus(ReasonDimensionMismatch, fmt.Sprintf("element_size=%d alignment=%d", cert.ElementSize, cert.Alignment))
	}
	if cert.LengthMultiple <= 0 || cert.LengthMultiple%4 != 0 {
		return MaterializeStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("length_multiple=%d", cert.LengthMultiple))
	}
	if cert.FixedWidthElements != 0 {
		return MaterializeStatus(ReasonDimensionMismatch, fmt.Sprintf("fixed_width_elements=%d want variable-width offsets list", cert.FixedWidthElements))
	}
	op := columnlayout.OpAdjacencyDirectView
	if cert.LogicalType == string(columnsemantics.LogicalUint32List) || cert.Type == typedcolumn.ColumnTypeUint32List {
		op = columnlayout.OpUint32ListDirectView
	}
	if cap := layout.Supports(op); !cap.Supported() {
		status := statusFromLayoutCapability(cap)
		if status.Streaming() {
			return MaterializeStatus(status.Reason, status.Message)
		}
		return status
	}
	return DirectStatus()
}

func validateBytesDirectViewCertification(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn) Status {
	if !cert.DirectViewCertified {
		return MaterializeStatus(ReasonNotWriterCertified, "column lacks writer-certified bytes direct-view contract")
	}
	if cert.Compression != typedcolumn.CompressionNone {
		return UnsupportedStatus(ReasonCompressed, fmt.Sprintf("compression=%s", cert.Compression))
	}
	if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
		return UnsupportedStatus(ReasonNullableWrapper, "null/default masks must be separate from bytes direct view")
	}
	if cert.Endian != typedcolumn.ColumnPartLayoutEndianLittle {
		return MaterializeStatus(ReasonWrongEndian, fmt.Sprintf("endian=%s", cert.Endian))
	}
	if cert.ElementSize != 1 || cert.Alignment <= 0 || cert.Alignment > 1 {
		return MaterializeStatus(ReasonDimensionMismatch, fmt.Sprintf("element_size=%d alignment=%d", cert.ElementSize, cert.Alignment))
	}
	if cert.LengthMultiple != 1 {
		return MaterializeStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("length_multiple=%d", cert.LengthMultiple))
	}
	if cert.FixedWidthElements != 0 {
		return MaterializeStatus(ReasonDimensionMismatch, fmt.Sprintf("fixed_width_elements=%d want variable-width bytes", cert.FixedWidthElements))
	}
	if cap := layout.Supports(columnlayout.OpBytesDirectView); !cap.Supported() {
		status := statusFromLayoutCapability(cap)
		if status.Streaming() {
			return MaterializeStatus(status.Reason, status.Message)
		}
		return status
	}
	return DirectStatus()
}

// Float32ScalarPlan selects a direct-view candidate only for writer-certified
// native raw little-endian float32 scalar sections. Raw-int64 compatibility
// carriers for logical float32 remain non-native fallback layouts.
func Float32ScalarPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:     columnsemantics.LogicalFloat32,
		Physical:    typedcolumn.ColumnTypeFloat32,
		Encoding:    cert.Encoding,
		Compression: cert.Compression,
		Nullable:    cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0,
	})
	return scalarDirectViewPlan(layout, cert, columnsemantics.LogicalFloat32, typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32, 4)
}

// Float64ScalarPlan selects a direct-view candidate only for writer-certified
// native raw little-endian float64/double scalar sections. Raw-int64
// compatibility carriers for logical double remain non-native fallback layouts.
func Float64ScalarPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:     columnsemantics.LogicalDouble,
		Physical:    typedcolumn.ColumnTypeFloat64,
		Encoding:    cert.Encoding,
		Compression: cert.Compression,
		Nullable:    cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0,
	})
	return scalarDirectViewPlan(layout, cert, columnsemantics.LogicalDouble, typedcolumn.ColumnTypeFloat64, typedcolumn.EncodingRawFloat64, 8)
}

// DenseFloat32VectorPlan selects a direct-view candidate only for writer-
// certified raw little-endian float32_vector sections with the requested fixed
// dimension. Callers must still validate each column/block payload and handle
// lifetime before exposing the returned []float32.
func DenseFloat32VectorPlan(cert typedcolumn.ColumnPartLayoutContractColumn, dims int) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:            columnsemantics.LogicalFloat32Vector,
		Physical:           typedcolumn.ColumnTypeFloat32Vector,
		Encoding:           cert.Encoding,
		Compression:        cert.Compression,
		Nullable:           cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable:        cert.DefaultMaskPresent || cert.DefaultCount != 0,
		FixedWidthElements: dims,
	})
	return denseDirectViewPlan(layout, cert, columnsemantics.LogicalFloat32Vector, typedcolumn.ColumnTypeFloat32Vector, typedcolumn.EncodingRawFloat32Vector, 4, dims)
}

// AdjacencyListPlan intentionally keeps the legacy dense fixed-degree adjacency
// layout as deferred/fallback-only. The #1901 offsets-list path is quarantined
// compatibility; generic uint32_list direct-view planning owns the reusable
// raw_uint32_offsets_list mechanics.
func DenseFixedWidthVectorBytesPlan(cert typedcolumn.ColumnPartLayoutContractColumn, logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding, elementSize int, elementsPerRow int) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:            logical,
		Physical:           physical,
		Encoding:           encoding,
		Compression:        cert.Compression,
		Nullable:           cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable:        cert.DefaultMaskPresent || cert.DefaultCount != 0,
		FixedWidthElements: elementsPerRow,
	})
	return denseDirectViewPlan(layout, cert, logical, physical, encoding, elementSize, elementsPerRow)
}

func AdjacencyListPlan(cert typedcolumn.ColumnPartLayoutContractColumn, degree int) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:            columnsemantics.LogicalAdjacencyList,
		Physical:           typedcolumn.ColumnTypeAdjacencyList,
		Encoding:           cert.Encoding,
		Compression:        cert.Compression,
		Nullable:           cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable:        cert.DefaultMaskPresent || cert.DefaultCount != 0,
		FixedWidthElements: degree,
	})
	return denseDirectViewPlan(layout, cert, columnsemantics.LogicalAdjacencyList, typedcolumn.ColumnTypeAdjacencyList, typedcolumn.EncodingRawUint32Dense, 4, degree)
}

// FixedBytesPlan selects a direct-view candidate only for writer-certified
// fixed_bytes/raw_fixed_bytes sections with the requested fixed bytes per row.
func FixedBytesPlan(cert typedcolumn.ColumnPartLayoutContractColumn, bytesPerRow int) Plan {
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:            columnsemantics.LogicalByteVector,
		Physical:           typedcolumn.ColumnTypeFixedBytes,
		Encoding:           typedcolumn.EncodingRawFixedBytes,
		Compression:        cert.Compression,
		Nullable:           cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable:        cert.DefaultMaskPresent || cert.DefaultCount != 0,
		FixedWidthElements: bytesPerRow,
		BytesPerRow:        bytesPerRow,
	})
	if bytesPerRow <= 0 {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("bytes_per_row=%d", bytesPerRow), ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if status := validateDirectViewCertification(layout, cert, 1, bytesPerRow); !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.LogicalType != string(columnsemantics.LogicalByteVector) || cert.Type != typedcolumn.ColumnTypeFixedBytes || cert.Encoding != typedcolumn.EncodingRawFixedBytes {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, columnsemantics.LogicalByteVector, typedcolumn.ColumnTypeFixedBytes, typedcolumn.EncodingRawFixedBytes), ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.BytesPerRow != 0 && cert.BytesPerRow != bytesPerRow {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("bytes_per_row=%d want %d", cert.BytesPerRow, bytesPerRow), ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	logicalBitsPerRow, ok := checkedMul3(bytesPerRow, 8, 1)
	if !ok {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: "logical_bits_per_row overflow", ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.LogicalBitsPerRow != 0 && cert.LogicalBitsPerRow != logicalBitsPerRow {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("logical_bits_per_row=%d want %d", cert.LogicalBitsPerRow, logicalBitsPerRow), ElementSize: 1, ElementsPerRow: bytesPerRow, BytesPerRow: bytesPerRow, Alignment: 1, Rows: cert.Rows}
	}
	return plan
}

// PackedUintVectorPlan selects a direct-view candidate only for writer-certified
// packed_bit_vector / packed_uint{2,4}_vector sections. The returned direct view is a byte payload;
// BitsPerElement and LogicalBitsPerRow describe packed-code interpretation.
func PackedUintVectorPlan(cert typedcolumn.ColumnPartLayoutContractColumn, logical columnsemantics.LogicalType, elementsPerRow int, bitsPerElement int) Plan {
	rowBytes, err := typedcolumn.PackedUintRowBytes(elementsPerRow, bitsPerElement)
	if err != nil {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: err.Error(), ElementSize: 1, ElementsPerRow: elementsPerRow, BitsPerElement: bitsPerElement, Rows: cert.Rows}
	}
	physical, ok := typedcolumn.PackedUintVectorTypeForBits(bitsPerElement)
	if !ok {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("bits_per_element=%d", bitsPerElement), ElementSize: 1, ElementsPerRow: elementsPerRow, BitsPerElement: bitsPerElement, Rows: cert.Rows}
	}
	encoding, _ := typedcolumn.PackedUintVectorEncodingForBits(bitsPerElement)
	logicalBitsPerRow, ok := checkedMul3(elementsPerRow, bitsPerElement, 1)
	if !ok {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: "logical_bits_per_row overflow", ElementSize: 1, ElementsPerRow: elementsPerRow, BitsPerElement: bitsPerElement, Rows: cert.Rows}
	}
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:            logical,
		Physical:           physical,
		Encoding:           encoding,
		Compression:        cert.Compression,
		Nullable:           cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable:        cert.DefaultMaskPresent || cert.DefaultCount != 0,
		FixedWidthElements: elementsPerRow,
		BitsPerElement:     bitsPerElement,
		BytesPerRow:        rowBytes,
		LogicalBitsPerRow:  logicalBitsPerRow,
	})
	if elementsPerRow <= 0 {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("elements_per_row=%d", elementsPerRow), ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if status := validateDirectViewCertification(layout, cert, 1, elementsPerRow); !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.LogicalType != string(logical) || cert.Type != physical || cert.Encoding != encoding {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, logical, physical, encoding), ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.BitsPerElement != bitsPerElement {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("bits_per_element=%d want %d", cert.BitsPerElement, bitsPerElement), ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.BytesPerRow != 0 && cert.BytesPerRow != rowBytes {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("bytes_per_row=%d want %d", cert.BytesPerRow, rowBytes), ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	if cert.LogicalBitsPerRow != 0 && cert.LogicalBitsPerRow != logicalBitsPerRow {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("logical_bits_per_row=%d want %d", cert.LogicalBitsPerRow, logicalBitsPerRow), ElementSize: 1, ElementsPerRow: elementsPerRow, BytesPerRow: rowBytes, BitsPerElement: bitsPerElement, LogicalBitsPerRow: logicalBitsPerRow, Alignment: 1, Rows: cert.Rows}
	}
	return plan
}

// Uint32ListPlan selects a direct-view candidate only for writer-certified
// generic uint32_list/raw_uint32_offsets_list sections: little-endian uint64
// offsets plus little-endian uint32 values.
func Uint32ListPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	return uint32OffsetsListPlan(cert, columnsemantics.LogicalUint32List, typedcolumn.ColumnTypeUint32List)
}

// BytesPlan selects a direct-view candidate only for writer-certified generic
// bytes/raw_bytes_offsets sections: little-endian uint64 offsets plus exact
// uninterpreted value bytes.
func BytesPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 1, ElementsPerRow: 0, Alignment: 1, Rows: cert.Rows}
	if cert.LogicalType != string(columnsemantics.LogicalBytes) || cert.Type != typedcolumn.ColumnTypeBytes || cert.Encoding != typedcolumn.EncodingRawBytesOffsets {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, columnsemantics.LogicalBytes, typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets), ElementSize: 1, ElementsPerRow: 0, Alignment: 1, Rows: cert.Rows}
	}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:     columnsemantics.LogicalBytes,
		Physical:    typedcolumn.ColumnTypeBytes,
		Encoding:    typedcolumn.EncodingRawBytesOffsets,
		Compression: cert.Compression,
		Nullable:    cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0,
	})
	if status := validateBytesDirectViewCertification(layout, cert); !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: 1, ElementsPerRow: 0, Alignment: 1, Rows: cert.Rows}
	}
	return plan
}

// AdjacencyOffsetsListPlan selects a direct-view candidate for the legacy
// adjacency_list/raw_uint32_offsets_list compatibility selector. Graph-specific
// naming is quarantined by #1989; prefer Uint32ListPlan for generic datastore
// primitives.
func AdjacencyOffsetsListPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	return uint32OffsetsListPlan(cert, columnsemantics.LogicalAdjacencyList, typedcolumn.ColumnTypeAdjacencyList)
}

func uint32OffsetsListPlan(cert typedcolumn.ColumnPartLayoutContractColumn, logical columnsemantics.LogicalType, physical typedcolumn.ColumnType) Plan {
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 4, ElementsPerRow: 0, Alignment: 4, Rows: cert.Rows}
	if cert.LogicalType != string(logical) || cert.Type != physical || cert.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, logical, physical, typedcolumn.EncodingRawUint32OffsetsList), ElementSize: 4, ElementsPerRow: 0, Alignment: 4, Rows: cert.Rows}
	}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:     logical,
		Physical:    physical,
		Encoding:    typedcolumn.EncodingRawUint32OffsetsList,
		Compression: cert.Compression,
		Nullable:    cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0,
	})
	if status := validateOffsetsListDirectViewCertification(layout, cert); !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: 4, ElementsPerRow: 0, Alignment: 4, Rows: cert.Rows}
	}
	return plan
}

// Uint32OffsetsListShapeRequest validates the #1914 primitive shape without
// constructing an unsafe direct view. Values is the number of uint32 values, not
// the values byte length.
type Uint32OffsetsListShapeRequest struct {
	Rows    int
	Offsets []uint64
	Values  uint64
}

func ValidateUint32OffsetsListShape(req Uint32OffsetsListShapeRequest) Status {
	if req.Rows < 0 {
		return UnsupportedStatus(ReasonRowCountMismatch, fmt.Sprintf("rows=%d", req.Rows))
	}
	maxInt := int(^uint(0) >> 1)
	if req.Rows == maxInt {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 overflows int")
	}
	wantOffsets := req.Rows + 1
	if len(req.Offsets) != wantOffsets {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets=%d want row_count+1=%d", len(req.Offsets), wantOffsets))
	}
	maxIndex := uint64(maxInt)
	if req.Offsets[0] != 0 {
		return UnsupportedStatus(ReasonOffsetsStartMismatch, fmt.Sprintf("offsets[0]=%d want 0", req.Offsets[0]))
	}
	if req.Values > maxIndex {
		return UnsupportedStatus(ReasonOffsetsGoIntRange, fmt.Sprintf("values=%d exceeds max int=%d", req.Values, maxInt))
	}
	prev := uint64(0)
	for i, offset := range req.Offsets {
		if offset > maxIndex {
			return UnsupportedStatus(ReasonOffsetsGoIntRange, fmt.Sprintf("offsets[%d]=%d exceeds max int=%d", i, offset, maxInt))
		}
		if i > 0 && offset < prev {
			return UnsupportedStatus(ReasonOffsetsNonMonotonic, fmt.Sprintf("offsets[%d]=%d previous=%d", i, offset, prev))
		}
		prev = offset
	}
	if final := req.Offsets[req.Rows]; final != req.Values {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("final_offset=%d values=%d", final, req.Values))
	}
	return DirectStatus()
}

// BytesShapeRequest validates the bytes/raw_bytes_offsets primitive shape.
// Values is the number of payload bytes.
type BytesShapeRequest struct {
	Rows    int
	Offsets []uint64
	Values  uint64
}

func ValidateBytesShape(req BytesShapeRequest) Status {
	if req.Rows < 0 {
		return UnsupportedStatus(ReasonRowCountMismatch, fmt.Sprintf("rows=%d", req.Rows))
	}
	maxInt := int(^uint(0) >> 1)
	if req.Rows == maxInt {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 overflows int")
	}
	wantOffsets := req.Rows + 1
	if len(req.Offsets) != wantOffsets {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets=%d want row_count+1=%d", len(req.Offsets), wantOffsets))
	}
	maxIndex := uint64(maxInt)
	if req.Offsets[0] != 0 {
		return UnsupportedStatus(ReasonOffsetsStartMismatch, fmt.Sprintf("offsets[0]=%d want 0", req.Offsets[0]))
	}
	if req.Values > maxIndex {
		return UnsupportedStatus(ReasonOffsetsGoIntRange, fmt.Sprintf("values=%d exceeds max int=%d", req.Values, maxInt))
	}
	prev := uint64(0)
	for i, offset := range req.Offsets {
		if offset > maxIndex {
			return UnsupportedStatus(ReasonOffsetsGoIntRange, fmt.Sprintf("offsets[%d]=%d exceeds max int=%d", i, offset, maxInt))
		}
		if i > 0 && offset < prev {
			return UnsupportedStatus(ReasonOffsetsNonMonotonic, fmt.Sprintf("offsets[%d]=%d previous=%d", i, offset, prev))
		}
		prev = offset
	}
	if final := req.Offsets[req.Rows]; final != req.Values {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("final_offset=%d values=%d", final, req.Values))
	}
	return DirectStatus()
}

func scalarDirectViewPlan(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn, logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding, elementSize int) Plan {
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: elementSize, ElementsPerRow: 1, Alignment: elementSize, Rows: cert.Rows}
	status := validateDirectViewCertification(layout, cert, elementSize, 1)
	if !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: elementSize, ElementsPerRow: 1, Alignment: elementSize, Rows: cert.Rows}
	}
	if cert.LogicalType != string(logical) {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical_type=%q want %q", cert.LogicalType, logical), ElementSize: elementSize, ElementsPerRow: 1, Alignment: elementSize, Rows: cert.Rows}
	}
	if cert.Type != physical || cert.Encoding != encoding {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("type/encoding=(%s,%s) want (%s,%s)", cert.Type, cert.Encoding, physical, encoding), ElementSize: elementSize, ElementsPerRow: 1, Alignment: elementSize, Rows: cert.Rows}
	}
	return plan
}

func denseDirectViewPlan(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn, logical columnsemantics.LogicalType, physical typedcolumn.ColumnType, encoding typedcolumn.Encoding, elementSize int, elementsPerRow int) Plan {
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: elementSize, ElementsPerRow: elementsPerRow, Alignment: elementSize, Rows: cert.Rows}
	if elementsPerRow <= 0 {
		return Plan{Path: PathUnsupported, Reason: ReasonDimensionMismatch, Message: fmt.Sprintf("elements_per_row=%d", elementsPerRow), ElementSize: elementSize, ElementsPerRow: elementsPerRow, Alignment: elementSize, Rows: cert.Rows}
	}
	status := validateDirectViewCertification(layout, cert, elementSize, elementsPerRow)
	if !status.Direct() {
		return Plan{Path: status.Path, Reason: status.Reason, Message: status.Message, ElementSize: elementSize, ElementsPerRow: elementsPerRow, Alignment: elementSize, Rows: cert.Rows}
	}
	if cert.LogicalType != string(logical) {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("logical_type=%q want %q", cert.LogicalType, logical), ElementSize: elementSize, ElementsPerRow: elementsPerRow, Alignment: elementSize, Rows: cert.Rows}
	}
	if cert.Type != physical || cert.Encoding != encoding {
		return Plan{Path: PathUnsupported, Reason: ReasonValidationFailed, Message: fmt.Sprintf("type/encoding=(%s,%s) want (%s,%s)", cert.Type, cert.Encoding, physical, encoding), ElementSize: elementSize, ElementsPerRow: elementsPerRow, Alignment: elementSize, Rows: cert.Rows}
	}
	return plan
}

func validateDirectViewCertificationFields(cert typedcolumn.ColumnPartLayoutContractColumn, elementSize int, elementsPerRow int) Status {
	if !cert.DirectViewCertified {
		return StreamingStatus(ReasonNotWriterCertified, "column lacks writer-certified direct-view contract")
	}
	if cert.Compression != typedcolumn.CompressionNone {
		return UnsupportedStatus(ReasonCompressed, fmt.Sprintf("compression=%s", cert.Compression))
	}
	if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
		return UnsupportedStatus(ReasonNullableWrapper, "null/default masks must be separate from value direct view")
	}
	if cert.Endian != typedcolumn.ColumnPartLayoutEndianLittle {
		return StreamingStatus(ReasonWrongEndian, fmt.Sprintf("endian=%s", cert.Endian))
	}
	if cert.ElementSize != elementSize {
		return StreamingStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("element_size=%d want %d", cert.ElementSize, elementSize))
	}
	if status := validateFixedWidthElements(cert, elementsPerRow); !status.Direct() {
		return status
	}
	if cert.Alignment <= 0 || cert.Alignment < elementSize || cert.LengthMultiple <= 0 || cert.LengthMultiple%elementSize != 0 {
		return StreamingStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("alignment=%d length_multiple=%d element_size=%d", cert.Alignment, cert.LengthMultiple, elementSize))
	}
	return DirectStatus()
}

func validateFixedWidthElements(cert typedcolumn.ColumnPartLayoutContractColumn, elementsPerRow int) Status {
	switch cert.Type {
	case typedcolumn.ColumnTypeFloat32Vector, typedcolumn.ColumnTypeAdjacencyList,
		typedcolumn.ColumnTypeUint8Vector, typedcolumn.ColumnTypeInt8Vector,
		typedcolumn.ColumnTypeUint16Vector, typedcolumn.ColumnTypeInt16Vector,
		typedcolumn.ColumnTypeUint32Vector, typedcolumn.ColumnTypeInt32Vector,
		typedcolumn.ColumnTypeUint64Vector, typedcolumn.ColumnTypeInt64Vector,
		typedcolumn.ColumnTypeFloat16Vector, typedcolumn.ColumnTypeBFloat16Vector,
		typedcolumn.ColumnTypeFloat64Vector,
		typedcolumn.ColumnTypeFixedBytes,
		typedcolumn.ColumnTypePackedBitVector, typedcolumn.ColumnTypePackedUint2Vector, typedcolumn.ColumnTypePackedUint4Vector:
		if elementsPerRow <= 0 || cert.FixedWidthElements != elementsPerRow {
			fieldName := "fixed_width_elements"
			if cert.Type == typedcolumn.ColumnTypeFixedBytes {
				fieldName = "bytes_per_row"
			}
			return StreamingStatus(ReasonDimensionMismatch, fmt.Sprintf("%s=%d want %d", fieldName, cert.FixedWidthElements, elementsPerRow))
		}
	default:
		if cert.FixedWidthElements != 0 {
			return StreamingStatus(ReasonDimensionMismatch, fmt.Sprintf("fixed_width_elements=%d want scalar", cert.FixedWidthElements))
		}
	}
	return DirectStatus()
}

func statusFromLayoutCapability(cap columnlayout.Capability) Status {
	reason := ReasonLayoutCapability
	switch cap.Reason {
	case columnlayout.ReasonCompressedDirectView, columnlayout.ReasonUnsupportedCompression:
		reason = ReasonCompressed
	case columnlayout.ReasonVariableWidthNoDirectView:
		reason = ReasonVariableWidth
	case columnlayout.ReasonNullDefaultWrapperRequired:
		reason = ReasonNullableWrapper
	case columnlayout.ReasonAdjacencyDirectViewDeferred, columnlayout.ReasonAdjacencyOffsetsListDirectViewDeferred, columnlayout.ReasonAdjacencyOffsetsListRuntimeDeferred:
		reason = ReasonDirectViewDeferred
	case columnlayout.ReasonOperationUnsupported:
		reason = ReasonUnsupportedOperation
	}
	if cap.Status == columnsemantics.StatusFallback {
		return StreamingStatus(reason, cap.Error())
	}
	return UnsupportedStatus(reason, cap.Error())
}

// DirectViewBlockRequest validates one payload/block against a direct-view plan.
type DirectViewBlockRequest struct {
	Plan          Plan
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Block         typedcolumn.ColumnPartLayoutContractBlock
	Rows          int
	PayloadBytes  int
	// AssetOffset is the absolute byte offset of the containing asset in its
	// mapped storage segment. Direct-view eligibility requires
	// AssetOffset+Block.PayloadOffset to satisfy Certification.Alignment; relative
	// image-local alignment alone is not sufficient.
	AssetOffset    int64
	HasAssetOffset bool
}

// DirectViewColumnRequest validates a complete fixed-width column-data section
// before callers expose a section-wide direct view.
type DirectViewColumnRequest struct {
	Plan          Plan
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Rows          int
	PayloadBytes  int
	// AssetOffset is the absolute byte offset of the containing asset in its
	// mapped storage segment. Direct-view eligibility requires
	// AssetOffset+Certification.Section.Offset and each block payload offset to
	// satisfy Certification.Alignment.
	AssetOffset    int64
	HasAssetOffset bool
}

// Uint32OffsetsListDirectViewRequest validates split offsets/value sections for
// raw_uint32_offsets_list. OffsetsBytes is the uint64 offsets-section byte
// length; ValuesBytes is the uint32 flattened values-section byte length.
type Uint32OffsetsListDirectViewRequest struct {
	Plan          Plan
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Rows          int
	OffsetsBytes  int
	ValuesBytes   int
	// AssetOffset is the absolute byte offset of the containing asset in its
	// mapped storage segment. Direct-view eligibility checks
	// AssetOffset+OffsetsSection.Offset against 8-byte alignment and
	// AssetOffset+ValuesSection.Offset against 4-byte alignment.
	AssetOffset    int64
	HasAssetOffset bool
}

// BytesDirectViewRequest validates split offsets/value sections for
// raw_bytes_offsets. OffsetsBytes is the uint64 offsets-section byte length;
// ValuesBytes is the exact concatenated opaque byte payload length.
type BytesDirectViewRequest struct {
	Plan          Plan
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Rows          int
	OffsetsBytes  int
	ValuesBytes   int
	// AssetOffset is the absolute byte offset of the containing asset in its
	// mapped storage segment. Direct-view eligibility checks
	// AssetOffset+OffsetsSection.Offset against 8-byte alignment. Byte values are
	// one-byte aligned but still require a known absolute storage identity.
	AssetOffset    int64
	HasAssetOffset bool
}

func ValidateDirectViewColumn(req DirectViewColumnRequest) Status {
	if !req.Plan.DirectCandidate() {
		return req.Plan.Status()
	}
	cert := req.Certification
	if status := validateDirectViewCertificationFields(cert, req.Plan.ElementSize, max(1, req.Plan.ElementsPerRow)); !status.Direct() {
		return status
	}
	if req.Rows < 0 || cert.Rows != req.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("cert_rows=%d request_rows=%d", cert.Rows, req.Rows))
	}
	if req.PayloadBytes != cert.Section.Length {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("payload_bytes=%d section_length=%d", req.PayloadBytes, cert.Section.Length))
	}
	if cert.LengthMultiple <= 0 || cert.Section.Length%cert.LengthMultiple != 0 {
		return StreamingStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("section_length=%d multiple=%d", cert.Section.Length, cert.LengthMultiple))
	}
	if cert.Alignment <= 0 || cert.Section.Offset%cert.Alignment != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("section_offset=%d alignment=%d", cert.Section.Offset, cert.Alignment))
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, cert.Section.Offset, cert.Alignment, "section"); !status.Direct() {
		return status
	}
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	want := 0
	if req.Plan.BytesPerRow > 0 {
		var ok bool
		want, ok = checkedMul3(req.Rows, req.Plan.BytesPerRow, 1)
		if !ok {
			return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width section byte count overflow")
		}
	} else {
		var ok bool
		want, ok = checkedMul3(req.Rows, elementsPerRow, req.Plan.ElementSize)
		if !ok {
			return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width section byte count overflow")
		}
	}
	if cert.Section.Length != want {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("section_length=%d want rows=%d row_bytes=%d elements=%d width=%d => %d", cert.Section.Length, req.Rows, req.Plan.BytesPerRow, elementsPerRow, req.Plan.ElementSize, want))
	}
	if len(cert.Blocks) == 0 {
		if req.Rows == 0 && req.PayloadBytes == 0 {
			return DirectStatus()
		}
		return UnsupportedStatus(ReasonValidationFailed, "direct-view column has no certified blocks")
	}
	nextRow := 0
	nextPayloadOffset := cert.Section.Offset
	totalPayload := 0
	for i, block := range cert.Blocks {
		if block.FirstRow != nextRow {
			return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("block %d first_row=%d want %d", i, block.FirstRow, nextRow))
		}
		if block.PayloadOffset != nextPayloadOffset {
			return UnsupportedStatus(ReasonValidationFailed, fmt.Sprintf("block %d payload_offset=%d want %d", i, block.PayloadOffset, nextPayloadOffset))
		}
		if cert.Alignment <= 0 || block.PayloadOffset%cert.Alignment != 0 {
			return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("block %d payload_offset=%d alignment=%d", i, block.PayloadOffset, cert.Alignment))
		}
		status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: req.Plan, Certification: cert, Block: block, Rows: block.RowCount, PayloadBytes: block.PayloadLength, AssetOffset: req.AssetOffset, HasAssetOffset: req.HasAssetOffset})
		if !status.Direct() {
			return status
		}
		nextRow += block.RowCount
		nextPayloadOffset += block.PayloadLength
		totalPayload += block.PayloadLength
	}
	if nextRow != req.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("block_rows=%d request_rows=%d", nextRow, req.Rows))
	}
	if totalPayload != req.PayloadBytes {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("block_payload=%d request_payload=%d", totalPayload, req.PayloadBytes))
	}
	return DirectStatus()
}

func ValidateUint32OffsetsListDirectView(req Uint32OffsetsListDirectViewRequest, offsets []uint64, values []uint32) Status {
	if status := ValidateUint32OffsetsListDirectViewSections(req); !status.Direct() {
		return status
	}
	return validateUint32OffsetsListDirectViewTypedViews(req, offsets, values)
}

func validateUint32OffsetsListDirectViewTypedViews(req Uint32OffsetsListDirectViewRequest, offsets []uint64, values []uint32) Status {
	if len(offsets) != req.offsetsCount() {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets=%d want row_count+1=%d", len(offsets), req.offsetsCount()))
	}
	if len(values) != req.valuesCount() {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values=%d want values_bytes/4=%d", len(values), req.valuesCount()))
	}
	return ValidateUint32OffsetsListShape(Uint32OffsetsListShapeRequest{Rows: req.Rows, Offsets: offsets, Values: uint64(len(values))})
}

func ValidateUint32OffsetsListDirectViewSections(req Uint32OffsetsListDirectViewRequest) Status {
	if !req.Plan.DirectCandidate() {
		return req.Plan.Status()
	}
	cert := req.Certification
	logical, physical, ok := uint32OffsetsListIdentity(cert)
	if !ok {
		return UnsupportedStatus(ReasonValidationFailed, fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s) or (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, columnsemantics.LogicalUint32List, typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList, columnsemantics.LogicalAdjacencyList, typedcolumn.ColumnTypeAdjacencyList, typedcolumn.EncodingRawUint32OffsetsList))
	}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: logical, Physical: physical, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: cert.Compression, Nullable: cert.NullMaskPresent || cert.NullCount != 0, Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0})
	if status := validateOffsetsListDirectViewCertification(layout, cert); !status.Direct() {
		return status
	}
	if !mappedresource.NativeLittleEndian() {
		return StreamingStatus(ReasonWrongEndian, "raw_uint32_offsets_list direct view requires little-endian host")
	}
	if req.Rows < 0 || cert.Rows != req.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("cert_rows=%d request_rows=%d", cert.Rows, req.Rows))
	}
	if req.Rows == int(^uint(0)>>1) {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 offsets byte count overflow")
	}
	wantOffsets, ok := checkedMul3(req.Rows+1, 8, 1)
	if !ok {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 offsets byte count overflow")
	}
	if req.OffsetsBytes != cert.OffsetsSection.Length || req.OffsetsBytes != cert.OffsetsBytes {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("offsets_bytes=%d section_length=%d contract_offsets_bytes=%d", req.OffsetsBytes, cert.OffsetsSection.Length, cert.OffsetsBytes))
	}
	if req.OffsetsBytes != wantOffsets {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets_bytes=%d want (rows+1)*8=%d", req.OffsetsBytes, wantOffsets))
	}
	if req.ValuesBytes != cert.ValuesSection.Length || req.ValuesBytes != cert.ValuesBytes {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("values_bytes=%d section_length=%d contract_values_bytes=%d", req.ValuesBytes, cert.ValuesSection.Length, cert.ValuesBytes))
	}
	if req.ValuesBytes < 0 || req.ValuesBytes%4 != 0 {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values_bytes=%d want multiple of 4", req.ValuesBytes))
	}
	if cert.OffsetsSection.Offset%8 != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("offsets section_offset=%d alignment=8", cert.OffsetsSection.Offset))
	}
	if cert.ValuesSection.Offset%4 != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("values section_offset=%d alignment=4", cert.ValuesSection.Offset))
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, cert.OffsetsSection.Offset, 8, "offsets"); !status.Direct() {
		return status
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, cert.ValuesSection.Offset, 4, "values"); !status.Direct() {
		return status
	}
	return DirectStatus()
}

func ValidateBytesDirectView(req BytesDirectViewRequest, offsets []uint64, values []byte) Status {
	if status := ValidateBytesDirectViewSections(req); !status.Direct() {
		return status
	}
	return validateBytesDirectViewTypedViews(req, offsets, values)
}

func validateBytesDirectViewTypedViews(req BytesDirectViewRequest, offsets []uint64, values []byte) Status {
	if len(offsets) != req.offsetsCount() {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets=%d want row_count+1=%d", len(offsets), req.offsetsCount()))
	}
	if len(values) != req.ValuesBytes {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values=%d want values_bytes=%d", len(values), req.ValuesBytes))
	}
	return ValidateBytesShape(BytesShapeRequest{Rows: req.Rows, Offsets: offsets, Values: uint64(len(values))})
}

func ValidateBytesDirectViewSections(req BytesDirectViewRequest) Status {
	if !req.Plan.DirectCandidate() {
		return req.Plan.Status()
	}
	cert := req.Certification
	if cert.LogicalType != string(columnsemantics.LogicalBytes) || cert.Type != typedcolumn.ColumnTypeBytes || cert.Encoding != typedcolumn.EncodingRawBytesOffsets {
		return UnsupportedStatus(ReasonValidationFailed, fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, columnsemantics.LogicalBytes, typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets))
	}
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalBytes, Physical: typedcolumn.ColumnTypeBytes, Encoding: typedcolumn.EncodingRawBytesOffsets, Compression: cert.Compression, Nullable: cert.NullMaskPresent || cert.NullCount != 0, Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0})
	if status := validateBytesDirectViewCertification(layout, cert); !status.Direct() {
		return status
	}
	if !mappedresource.NativeLittleEndian() {
		return StreamingStatus(ReasonWrongEndian, "raw_bytes_offsets direct view requires little-endian host for uint64 offsets")
	}
	if req.Rows < 0 || cert.Rows != req.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("cert_rows=%d request_rows=%d", cert.Rows, req.Rows))
	}
	if req.Rows == int(^uint(0)>>1) {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 offsets byte count overflow")
	}
	wantOffsets, ok := checkedMul3(req.Rows+1, 8, 1)
	if !ok {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, "row_count+1 offsets byte count overflow")
	}
	if req.OffsetsBytes != cert.OffsetsSection.Length || req.OffsetsBytes != cert.OffsetsBytes {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("offsets_bytes=%d section_length=%d contract_offsets_bytes=%d", req.OffsetsBytes, cert.OffsetsSection.Length, cert.OffsetsBytes))
	}
	if req.OffsetsBytes != wantOffsets {
		return UnsupportedStatus(ReasonOffsetsCountMismatch, fmt.Sprintf("offsets_bytes=%d want (rows+1)*8=%d", req.OffsetsBytes, wantOffsets))
	}
	if req.ValuesBytes != cert.ValuesSection.Length || req.ValuesBytes != cert.ValuesBytes {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("values_bytes=%d section_length=%d contract_values_bytes=%d", req.ValuesBytes, cert.ValuesSection.Length, cert.ValuesBytes))
	}
	if req.ValuesBytes < 0 {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values_bytes=%d", req.ValuesBytes))
	}
	if cert.OffsetsSection.Offset%8 != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("offsets section_offset=%d alignment=8", cert.OffsetsSection.Offset))
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, cert.OffsetsSection.Offset, 8, "offsets"); !status.Direct() {
		return status
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, cert.ValuesSection.Offset, 1, "values"); !status.Direct() {
		return status
	}
	return DirectStatus()
}

func uint32OffsetsListIdentity(cert typedcolumn.ColumnPartLayoutContractColumn) (columnsemantics.LogicalType, typedcolumn.ColumnType, bool) {
	if cert.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		return "", "", false
	}
	if cert.LogicalType == string(columnsemantics.LogicalUint32List) && cert.Type == typedcolumn.ColumnTypeUint32List {
		return columnsemantics.LogicalUint32List, typedcolumn.ColumnTypeUint32List, true
	}
	if cert.LogicalType == string(columnsemantics.LogicalAdjacencyList) && cert.Type == typedcolumn.ColumnTypeAdjacencyList {
		return columnsemantics.LogicalAdjacencyList, typedcolumn.ColumnTypeAdjacencyList, true
	}
	return "", "", false
}

func (req Uint32OffsetsListDirectViewRequest) offsetsCount() int {
	if req.Rows < 0 || req.Rows == int(^uint(0)>>1) {
		return -1
	}
	return req.Rows + 1
}

func (req Uint32OffsetsListDirectViewRequest) valuesCount() int {
	if req.ValuesBytes < 0 || req.ValuesBytes%4 != 0 {
		return -1
	}
	return req.ValuesBytes / 4
}

func (req BytesDirectViewRequest) offsetsCount() int {
	if req.Rows < 0 || req.Rows == int(^uint(0)>>1) {
		return -1
	}
	return req.Rows + 1
}

func ValidateDirectViewBlock(req DirectViewBlockRequest) Status {
	if !req.Plan.DirectCandidate() {
		return req.Plan.Status()
	}
	cert := req.Certification
	block := req.Block
	if status := validateDirectViewCertificationFields(cert, req.Plan.ElementSize, max(1, req.Plan.ElementsPerRow)); !status.Direct() {
		return status
	}
	if req.Rows < 0 || block.RowCount != req.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("block_rows=%d request_rows=%d", block.RowCount, req.Rows))
	}
	if block.Encoding != cert.Encoding || block.Compression != cert.Compression {
		return UnsupportedStatus(ReasonValidationFailed, fmt.Sprintf("block encoding/compression=(%s,%s) cert=(%s,%s)", block.Encoding, block.Compression, cert.Encoding, cert.Compression))
	}
	if block.NullCount != 0 || block.DefaultCount != 0 {
		return UnsupportedStatus(ReasonNullableWrapper, fmt.Sprintf("block null/default=(%d,%d)", block.NullCount, block.DefaultCount))
	}
	if req.PayloadBytes != block.PayloadLength {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("payload_bytes=%d block_length=%d", req.PayloadBytes, block.PayloadLength))
	}
	if cert.LengthMultiple <= 0 || block.PayloadLength%cert.LengthMultiple != 0 || block.RawBytes%cert.LengthMultiple != 0 {
		return StreamingStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("payload=%d raw=%d multiple=%d", block.PayloadLength, block.RawBytes, cert.LengthMultiple))
	}
	if cert.Alignment <= 0 || block.PayloadOffset%cert.Alignment != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("payload_offset=%d alignment=%d", block.PayloadOffset, cert.Alignment))
	}
	if status := validateAbsoluteDirectViewOffset(req.HasAssetOffset, req.AssetOffset, block.PayloadOffset, cert.Alignment, "payload"); !status.Direct() {
		return status
	}
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	want := 0
	if req.Plan.BytesPerRow > 0 {
		var ok bool
		want, ok = checkedMul3(req.Rows, req.Plan.BytesPerRow, 1)
		if !ok {
			return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width byte count overflow")
		}
	} else {
		var ok bool
		want, ok = checkedMul3(req.Rows, elementsPerRow, req.Plan.ElementSize)
		if !ok {
			return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width byte count overflow")
		}
	}
	if block.PayloadLength != want || block.RawBytes != want || block.StoredBytes != want {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("payload/raw/stored=(%d,%d,%d) want rows=%d row_bytes=%d elements=%d width=%d => %d", block.PayloadLength, block.RawBytes, block.StoredBytes, req.Rows, req.Plan.BytesPerRow, elementsPerRow, req.Plan.ElementSize, want))
	}
	return DirectStatus()
}

func validateAbsoluteDirectViewOffset(hasAssetOffset bool, assetOffset int64, payloadOffset int, alignment int, label string) Status {
	if !hasAssetOffset {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("%s absolute asset offset missing", label))
	}
	if assetOffset < 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("%s asset_offset=%d", label, assetOffset))
	}
	if payloadOffset < 0 || assetOffset > int64(^uint64(0)>>1)-int64(payloadOffset) {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("%s absolute offset overflow asset=%d payload=%d", label, assetOffset, payloadOffset))
	}
	absolute := assetOffset + int64(payloadOffset)
	if alignment <= 0 || absolute%int64(alignment) != 0 {
		return StreamingStatus(ReasonAbsoluteOffsetUnaligned, fmt.Sprintf("%s absolute_offset=%d asset_offset=%d payload_offset=%d alignment=%d", label, absolute, assetOffset, payloadOffset, alignment))
	}
	return DirectStatus()
}

func checkedMul3(a, b, c int) (int, bool) {
	if a < 0 || b < 0 || c < 0 {
		return 0, false
	}
	maxInt := int(^uint(0) >> 1)
	if b != 0 && a > maxInt/b {
		return 0, false
	}
	ab := a * b
	if c != 0 && ab > maxInt/c {
		return 0, false
	}
	return ab * c, true
}

// ResourceViewOptions controls handle-level direct-view validation.
type ResourceViewOptions struct {
	// ExpectedElements validates the view length. Use a negative value to skip
	// length validation; zero intentionally means an empty view is expected.
	ExpectedElements int
	RequireMapped    bool
}

func Int64View(mgr *mappedresource.Manager, h *mappedresource.Handle, opts ResourceViewOptions) ([]int64, Status) {
	status := validateHandle(h, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, status
	}
	var view []int64
	var err error
	if mgr != nil {
		view, err = mgr.Int64View(h)
	} else {
		view, err = mappedresource.Int64View(h.Bytes())
	}
	return validateViewLen(view, opts.ExpectedElements, err)
}

func Float32View(mgr *mappedresource.Manager, h *mappedresource.Handle, opts ResourceViewOptions) ([]float32, Status) {
	status := validateHandle(h, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, status
	}
	var view []float32
	var err error
	if mgr != nil {
		view, err = mgr.Float32View(h)
	} else {
		view, err = mappedresource.Float32View(h.Bytes())
	}
	return validateViewLen(view, opts.ExpectedElements, err)
}

func Float64View(mgr *mappedresource.Manager, h *mappedresource.Handle, opts ResourceViewOptions) ([]float64, Status) {
	status := validateHandle(h, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, status
	}
	var view []float64
	var err error
	if mgr != nil {
		view, err = mgr.Float64View(h)
	} else {
		view, err = mappedresource.Float64View(h.Bytes())
	}
	return validateViewLen(view, opts.ExpectedElements, err)
}

func Uint32View(mgr *mappedresource.Manager, h *mappedresource.Handle, opts ResourceViewOptions) ([]uint32, Status) {
	status := validateHandle(h, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, status
	}
	var view []uint32
	var err error
	if mgr != nil {
		view, err = mgr.Uint32View(h)
	} else {
		view, err = mappedresource.Uint32View(h.Bytes())
	}
	return validateViewLen(view, opts.ExpectedElements, err)
}

func Uint64View(mgr *mappedresource.Manager, h *mappedresource.Handle, opts ResourceViewOptions) ([]uint64, Status) {
	status := validateHandle(h, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, status
	}
	var view []uint64
	var err error
	if mgr != nil {
		view, err = mgr.Uint64View(h)
	} else {
		view, err = mappedresource.Uint64View(h.Bytes())
	}
	return validateViewLen(view, opts.ExpectedElements, err)
}

// Float32ByteView validates and exposes immutable bytes as []float32 without a
// mappedresource handle. Callers are responsible for tying the byte slice to an
// explicit lifetime; handle-backed optimized paths should prefer Float32View,
// Float32ScalarView, or DenseFloat32VectorView.
func Float32ByteView(raw []byte, opts ResourceViewOptions) ([]float32, Status) {
	view, err := mappedresource.Float32View(raw)
	return validateViewLen(view, opts.ExpectedElements, err)
}

// Uint32ByteView validates and exposes immutable bytes as []uint32 without a
// mappedresource handle. Callers are responsible for tying the byte slice to an
// explicit lifetime; handle-backed optimized paths should prefer Uint32View or
// AdjacencyListView.
func Uint32ByteView(raw []byte, opts ResourceViewOptions) ([]uint32, Status) {
	view, err := mappedresource.Uint32View(raw)
	return validateViewLen(view, opts.ExpectedElements, err)
}

// Uint64ByteView validates and exposes immutable bytes as []uint64 without a
// mappedresource handle. Callers are responsible for tying the byte slice to an
// explicit lifetime; handle-backed optimized paths should prefer Uint64View.
func Uint64ByteView(raw []byte, opts ResourceViewOptions) ([]uint64, Status) {
	view, err := mappedresource.Uint64View(raw)
	return validateViewLen(view, opts.ExpectedElements, err)
}

func Float32ScalarView(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, opts ResourceViewOptions) ([]float32, Status) {
	status := ValidateDirectViewColumn(req)
	if !status.Direct() {
		return nil, status
	}
	opts, status = normalizeFixedWidthViewOptions(req, opts)
	if !status.Direct() {
		return nil, status
	}
	return Float32View(mgr, h, opts)
}

func Float64ScalarView(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, opts ResourceViewOptions) ([]float64, Status) {
	status := ValidateDirectViewColumn(req)
	if !status.Direct() {
		return nil, status
	}
	opts, status = normalizeFixedWidthViewOptions(req, opts)
	if !status.Direct() {
		return nil, status
	}
	return Float64View(mgr, h, opts)
}

func DenseFloat32VectorView(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, opts ResourceViewOptions) ([]float32, Status) {
	status := ValidateDirectViewColumn(req)
	if !status.Direct() {
		return nil, status
	}
	opts, status = normalizeFixedWidthViewOptions(req, opts)
	if !status.Direct() {
		return nil, status
	}
	return Float32View(mgr, h, opts)
}

func AdjacencyListView(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, opts ResourceViewOptions) ([]uint32, Status) {
	status := ValidateDirectViewColumn(req)
	if !status.Direct() {
		return nil, status
	}
	opts, status = normalizeFixedWidthViewOptions(req, opts)
	if !status.Direct() {
		return nil, status
	}
	return Uint32View(mgr, h, opts)
}

func Uint32ListView(mgr *mappedresource.Manager, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle, req Uint32OffsetsListDirectViewRequest, opts ResourceViewOptions) ([]uint64, []uint32, Status) {
	return Uint32OffsetsListView(mgr, offsetsHandle, valuesHandle, req, opts)
}

func Uint32OffsetsListView(mgr *mappedresource.Manager, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle, req Uint32OffsetsListDirectViewRequest, opts ResourceViewOptions) ([]uint64, []uint32, Status) {
	if status := ValidateUint32OffsetsListDirectViewSections(req); !status.Direct() {
		return nil, nil, status
	}
	offsetsOpts := opts
	offsetsOpts.ExpectedElements = -1
	valuesOpts := opts
	valuesOpts.ExpectedElements = -1
	offsets, status := Uint64View(mgr, offsetsHandle, offsetsOpts)
	if !status.Direct() {
		return nil, nil, status
	}
	values, status := Uint32View(mgr, valuesHandle, valuesOpts)
	if !status.Direct() {
		return nil, nil, status
	}
	if status := validateUint32OffsetsListDirectViewTypedViews(req, offsets, values); !status.Direct() {
		return nil, nil, status
	}
	return offsets, values, DirectStatus()
}

func BytesView(mgr *mappedresource.Manager, offsetsHandle *mappedresource.Handle, valuesHandle *mappedresource.Handle, req BytesDirectViewRequest, opts ResourceViewOptions) ([]uint64, []byte, Status) {
	if status := ValidateBytesDirectViewSections(req); !status.Direct() {
		return nil, nil, status
	}
	offsetsOpts := opts
	offsetsOpts.ExpectedElements = -1
	offsets, status := Uint64View(mgr, offsetsHandle, offsetsOpts)
	if !status.Direct() {
		return nil, nil, status
	}
	status = validateHandle(valuesHandle, mappedresource.SourceMapped, opts.RequireMapped)
	if !status.Direct() {
		return nil, nil, status
	}
	values := valuesHandle.Bytes()
	if len(values) != req.ValuesBytes {
		return nil, nil, UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values bytes=%d want %d", len(values), req.ValuesBytes))
	}
	if status := validateBytesDirectViewTypedViews(req, offsets, values); !status.Direct() {
		return nil, nil, status
	}
	return offsets, values, DirectStatus()
}

func normalizeFixedWidthViewOptions(req DirectViewColumnRequest, opts ResourceViewOptions) (ResourceViewOptions, Status) {
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	expected := 0
	var ok bool
	if req.Plan.BytesPerRow > 0 {
		expected, ok = checkedMul3(req.Rows, req.Plan.BytesPerRow, 1)
	} else {
		expected, ok = checkedMul3(req.Rows, elementsPerRow, 1)
	}
	if !ok {
		return opts, UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width element count overflow")
	}
	if opts.ExpectedElements < 0 {
		opts.ExpectedElements = expected
	} else if opts.ExpectedElements != expected {
		return opts, UnsupportedStatus(ReasonRowCountMismatch, fmt.Sprintf("expected_elements=%d want %d", opts.ExpectedElements, expected))
	}
	return opts, DirectStatus()
}

func validateHandle(h *mappedresource.Handle, required mappedresource.Source, requireSource bool) Status {
	if h == nil {
		return StreamingStatus(ReasonNilHandle, "nil mappedresource handle")
	}
	if h.Released() {
		return UnsupportedStatus(ReasonStaleHandle, "mappedresource handle is released")
	}
	if requireSource && h.Source() != required {
		return StreamingStatus(ReasonHandleSourceUnsupported, fmt.Sprintf("source=%s want %s", h.Source(), required))
	}
	return DirectStatus()
}

func validateViewLen[T any](view []T, expected int, err error) ([]T, Status) {
	if err != nil {
		return nil, classifyViewError(err)
	}
	if expected >= 0 && len(view) != expected {
		return nil, UnsupportedStatus(ReasonRowCountMismatch, fmt.Sprintf("view elements=%d want %d", len(view), expected))
	}
	return view, DirectStatus()
}

func classifyViewError(err error) Status {
	msg := err.Error()
	switch {
	case errors.Is(err, mappedresource.ErrDirectViewNilHandle):
		return StreamingStatus(ReasonNilHandle, msg)
	case errors.Is(err, mappedresource.ErrDirectViewReleasedHandle):
		return UnsupportedStatus(ReasonStaleHandle, msg)
	case errors.Is(err, mappedresource.ErrDirectViewUnaligned):
		return StreamingStatus(ReasonActualPointerUnaligned, msg)
	case errors.Is(err, mappedresource.ErrDirectViewWrongEndian):
		return StreamingStatus(ReasonWrongEndian, msg)
	case errors.Is(err, mappedresource.ErrDirectViewLengthMultiple):
		return UnsupportedStatus(ReasonLengthMultipleMismatch, msg)
	default:
		return UnsupportedStatus(ReasonValidationFailed, msg)
	}
}
