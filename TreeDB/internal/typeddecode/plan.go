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
	ReasonSupported                  Reason = "supported"
	ReasonUnsupportedOperation       Reason = "unsupported_operation"
	ReasonLayoutCapability           Reason = "layout_capability"
	ReasonNotWriterCertified         Reason = "writer_certification_missing"
	ReasonCompressed                 Reason = "compressed"
	ReasonVariableWidth              Reason = "variable_width"
	ReasonNullableWrapper            Reason = "nullable_default_wrapper"
	ReasonWrongEndian                Reason = "wrong_endian"
	ReasonLengthMultipleMismatch     Reason = "length_multiple_mismatch"
	ReasonPayloadLengthMismatch      Reason = "payload_length_mismatch"
	ReasonRowCountMismatch           Reason = "row_count_mismatch"
	ReasonDimensionMismatch          Reason = "dimension_mismatch"
	ReasonUnaligned                  Reason = "unaligned"
	ReasonNilHandle                  Reason = "nil_handle"
	ReasonStaleHandle                Reason = "stale_handle"
	ReasonHandleSourceUnsupported    Reason = "handle_source_unsupported"
	ReasonDictionarySemanticsMissing Reason = "dictionary_semantics_missing"
	ReasonMaterializationRequired    Reason = "materialization_required"
	ReasonValidationFailed           Reason = "validation_failed"
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
	Path           Path
	Reason         Reason
	Message        string
	ElementSize    int
	ElementsPerRow int
	Alignment      int
	Rows           int
}

func (p Plan) Status() Status        { return Status{Path: p.Path, Reason: p.Reason, Message: p.Message} }
func (p Plan) DirectCandidate() bool { return p.Path == PathDirectView && p.Reason == ReasonSupported }

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
		if cert.Compression != typedcolumn.CompressionNone {
			return Plan{Path: PathUnsupported, Reason: ReasonCompressed, Message: fmt.Sprintf("compression=%s", cert.Compression)}
		}
		if cert.NullMaskPresent || cert.DefaultMaskPresent || cert.NullCount != 0 || cert.DefaultCount != 0 {
			return Plan{Path: PathUnsupported, Reason: ReasonNullableWrapper, Message: "null/default masks must be applied outside scalar int64 reducer"}
		}
		return Plan{Path: PathStreaming, Reason: ReasonVariableWidth, Message: "certified variable-width int64 streaming reducer", ElementSize: 8, ElementsPerRow: 1, Rows: cert.Rows}
	}
	return Plan{Path: PathUnsupported, Reason: ReasonUnsupportedOperation, Message: "layout does not advertise int64 reducer"}
}

func validateDirectViewCertification(layout columnlayout.Capabilities, cert typedcolumn.ColumnPartLayoutContractColumn, elementSize int, elementsPerRow int) Status {
	if cap := layout.Supports(columnlayout.OpDirectView); !cap.Supported() {
		return statusFromLayoutCapability(cap)
	}
	return validateDirectViewCertificationFields(cert, elementSize, elementsPerRow)
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

// AdjacencyListPlan selects a direct-view candidate only for writer-certified
// raw little-endian adjacency_list sections with the requested fixed degree.
// Callers must still validate each column/block payload and handle lifetime
// before exposing the returned []uint32.
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
	if cert.FixedWidthElements != 0 && cert.FixedWidthElements != elementsPerRow {
		return StreamingStatus(ReasonDimensionMismatch, fmt.Sprintf("elements_per_row=%d want %d", cert.FixedWidthElements, elementsPerRow))
	}
	if cert.Alignment <= 0 || cert.Alignment < elementSize || cert.LengthMultiple <= 0 || cert.LengthMultiple%elementSize != 0 {
		return StreamingStatus(ReasonLengthMultipleMismatch, fmt.Sprintf("alignment=%d length_multiple=%d element_size=%d", cert.Alignment, cert.LengthMultiple, elementSize))
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
}

// DirectViewColumnRequest validates a complete fixed-width column-data section
// before callers expose a section-wide direct view.
type DirectViewColumnRequest struct {
	Plan          Plan
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Rows          int
	PayloadBytes  int
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
		return StreamingStatus(ReasonUnaligned, fmt.Sprintf("section_offset=%d alignment=%d", cert.Section.Offset, cert.Alignment))
	}
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	want, ok := checkedMul3(req.Rows, elementsPerRow, req.Plan.ElementSize)
	if !ok {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width section byte count overflow")
	}
	if cert.Section.Length != want {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("section_length=%d want rows=%d*elements=%d*width=%d=%d", cert.Section.Length, req.Rows, elementsPerRow, req.Plan.ElementSize, want))
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
			return StreamingStatus(ReasonUnaligned, fmt.Sprintf("block %d payload_offset=%d alignment=%d", i, block.PayloadOffset, cert.Alignment))
		}
		status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: req.Plan, Certification: cert, Block: block, Rows: block.RowCount, PayloadBytes: block.PayloadLength})
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
		return StreamingStatus(ReasonUnaligned, fmt.Sprintf("payload_offset=%d alignment=%d", block.PayloadOffset, cert.Alignment))
	}
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	want, ok := checkedMul3(req.Rows, elementsPerRow, req.Plan.ElementSize)
	if !ok {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, "fixed-width byte count overflow")
	}
	if block.PayloadLength != want || block.RawBytes != want || block.StoredBytes != want {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("payload/raw/stored=(%d,%d,%d) want rows=%d*elements=%d*width=%d=%d", block.PayloadLength, block.RawBytes, block.StoredBytes, req.Rows, elementsPerRow, req.Plan.ElementSize, want))
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

// Float32ByteView validates and exposes immutable bytes as []float32 without a
// mappedresource handle. Callers are responsible for tying the byte slice to an
// explicit lifetime; handle-backed optimized paths should prefer Float32View or
// DenseFloat32VectorView.
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

func DenseFloat32VectorView(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, opts ResourceViewOptions) ([]float32, Status) {
	status := ValidateDirectViewColumn(req)
	if !status.Direct() {
		return nil, status
	}
	opts, status = normalizeDenseViewOptions(req, opts)
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
	opts, status = normalizeDenseViewOptions(req, opts)
	if !status.Direct() {
		return nil, status
	}
	return Uint32View(mgr, h, opts)
}

func normalizeDenseViewOptions(req DirectViewColumnRequest, opts ResourceViewOptions) (ResourceViewOptions, Status) {
	elementsPerRow := req.Plan.ElementsPerRow
	if elementsPerRow <= 0 {
		elementsPerRow = 1
	}
	expected, ok := checkedMul3(req.Rows, elementsPerRow, 1)
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
		return StreamingStatus(ReasonUnaligned, msg)
	case errors.Is(err, mappedresource.ErrDirectViewWrongEndian):
		return StreamingStatus(ReasonWrongEndian, msg)
	case errors.Is(err, mappedresource.ErrDirectViewLengthMultiple):
		return UnsupportedStatus(ReasonLengthMultipleMismatch, msg)
	default:
		return UnsupportedStatus(ReasonValidationFailed, msg)
	}
}
