package typeddecode

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// GraphDirectViewExpectation is the caller-supplied identity and shape contract
// for graph-search typed-column direct-view certification. The reusable helper
// validates these fields against the writer-certified typed-column layout and
// the live mappedresource handles before returning a prepared direct view.
//
// ExpectedOwner/ExpectedRole identify the graph/typed-column state the caller is
// preparing, while ActualOwner/ActualRole must come from the caller's manifest or
// asset context. The primitive certifier intentionally does not replace
// graph-owned TVIS/manifest validation; it fails closed when those supplied
// identities do not match.
type GraphDirectViewExpectation struct {
	ExpectedOwner  string
	ActualOwner    string
	ExpectedRole   string
	ActualRole     string
	Column         string
	Rows           int
	AssetOffset    int64
	HasAssetOffset bool
}

type GraphFloat32VectorDirectViewRequest struct {
	Expectation   GraphDirectViewExpectation
	Dims          int
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Section       typedcolumn.ColumnPartImageSection
	ExpectedKey   mappedresource.Key
	Handle        *mappedresource.Handle
	Manager       *mappedresource.Manager
}

type GraphFloat32DirectViewRequest struct {
	Expectation   GraphDirectViewExpectation
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Section       typedcolumn.ColumnPartImageSection
	ExpectedKey   mappedresource.Key
	Handle        *mappedresource.Handle
	Manager       *mappedresource.Manager
}

type GraphInt64DirectViewRequest struct {
	Expectation   GraphDirectViewExpectation
	Certification typedcolumn.ColumnPartLayoutContractColumn
	Section       typedcolumn.ColumnPartImageSection
	ExpectedKey   mappedresource.Key
	Handle        *mappedresource.Handle
	Manager       *mappedresource.Manager
}

type GraphUint32ListDirectViewRequest struct {
	Expectation        GraphDirectViewExpectation
	Certification      typedcolumn.ColumnPartLayoutContractColumn
	OffsetsSection     typedcolumn.ColumnPartImageSection
	ValuesSection      typedcolumn.ColumnPartImageSection
	ExpectedOffsetsKey mappedresource.Key
	ExpectedValuesKey  mappedresource.Key
	OffsetsHandle      *mappedresource.Handle
	ValuesHandle       *mappedresource.Handle
	Manager            *mappedresource.Manager
}

type GraphBytesDirectViewRequest struct {
	Expectation        GraphDirectViewExpectation
	Certification      typedcolumn.ColumnPartLayoutContractColumn
	OffsetsSection     typedcolumn.ColumnPartImageSection
	ValuesSection      typedcolumn.ColumnPartImageSection
	ExpectedOffsetsKey mappedresource.Key
	ExpectedValuesKey  mappedresource.Key
	OffsetsHandle      *mappedresource.Handle
	ValuesHandle       *mappedresource.Handle
	Manager            *mappedresource.Manager
}

// PreparedFloat32VectorDirectView is a certified row-major graph-search vector
// view. Values aliases Handle and is valid only while Handle remains live.
type PreparedFloat32VectorDirectView struct {
	Expectation GraphDirectViewExpectation
	Rows        int
	Dims        int
	Values      []float32
	Handle      *mappedresource.Handle
}

// PreparedFloat32DirectView is a certified fixed-width float32 scalar view.
type PreparedFloat32DirectView struct {
	Expectation GraphDirectViewExpectation
	Rows        int
	Values      []float32
	Handle      *mappedresource.Handle
}

// PreparedInt64DirectView is a certified fixed-width int64 scalar view.
type PreparedInt64DirectView struct {
	Expectation GraphDirectViewExpectation
	Rows        int
	Values      []int64
	Handle      *mappedresource.Handle
}

// PreparedUint32ListDirectView is a certified raw_uint32_offsets_list view.
type PreparedUint32ListDirectView struct {
	Expectation   GraphDirectViewExpectation
	Rows          int
	Offsets       []uint64
	Values        []uint32
	OffsetsHandle *mappedresource.Handle
	ValuesHandle  *mappedresource.Handle
}

// PreparedBytesDirectView is a certified raw_bytes_offsets view.
type PreparedBytesDirectView struct {
	Expectation   GraphDirectViewExpectation
	Rows          int
	Offsets       []uint64
	Values        []byte
	OffsetsHandle *mappedresource.Handle
	ValuesHandle  *mappedresource.Handle
}

func Int64DirectViewPlan(cert typedcolumn.ColumnPartLayoutContractColumn) Plan {
	layout := columnlayout.CapabilitiesFor(columnlayout.Descriptor{
		Logical:     columnsemantics.LogicalInt64,
		Physical:    typedcolumn.ColumnTypeInt64,
		Encoding:    cert.Encoding,
		Compression: cert.Compression,
		Nullable:    cert.NullMaskPresent || cert.NullCount != 0,
		Defaultable: cert.DefaultMaskPresent || cert.DefaultCount != 0,
	})
	return scalarDirectViewPlan(layout, cert, columnsemantics.LogicalInt64, typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64, 8)
}

func CertifyGraphFloat32VectorDirectView(req GraphFloat32VectorDirectViewRequest) (PreparedFloat32VectorDirectView, Status) {
	exp := req.Expectation
	if status := validateGraphDirectViewExpectation(exp, req.Certification, string(columnsemantics.LogicalFloat32Vector), typedcolumn.ColumnTypeFloat32Vector, typedcolumn.EncodingRawFloat32Vector); !status.Direct() {
		return PreparedFloat32VectorDirectView{}, status
	}
	if req.Dims <= 0 {
		return PreparedFloat32VectorDirectView{}, UnsupportedStatus(ReasonDimensionMismatch, fmt.Sprintf("dims=%d", req.Dims))
	}
	if status := validateFixedWidthGraphSection(exp, req.Certification, req.Section, typedcolumn.EncodingRawFloat32Vector); !status.Direct() {
		return PreparedFloat32VectorDirectView{}, status
	}
	plan := DenseFloat32VectorPlan(req.Certification, req.Dims)
	directReq := DirectViewColumnRequest{Plan: plan, Certification: req.Certification, Rows: exp.Rows, PayloadBytes: req.Section.Length, AssetOffset: exp.AssetOffset, HasAssetOffset: exp.HasAssetOffset}
	if status := ValidateDirectViewColumn(directReq); !status.Direct() {
		return PreparedFloat32VectorDirectView{}, status
	}
	if status := validateFixedWidthGraphHandle(req.Handle, exp, req.Section, typedcolumn.EncodingRawFloat32Vector, req.ExpectedKey); !status.Direct() {
		return PreparedFloat32VectorDirectView{}, status
	}
	expectedElements, ok := checkedMul3(exp.Rows, req.Dims, 1)
	if !ok {
		return PreparedFloat32VectorDirectView{}, UnsupportedStatus(ReasonPayloadLengthMismatch, "float32_vector element count overflow")
	}
	values, status := DenseFloat32VectorView(req.Manager, req.Handle, directReq, ResourceViewOptions{ExpectedElements: expectedElements, RequireMapped: true})
	if !status.Direct() {
		return PreparedFloat32VectorDirectView{}, status
	}
	return PreparedFloat32VectorDirectView{Expectation: exp, Rows: exp.Rows, Dims: req.Dims, Values: values, Handle: req.Handle}, DirectStatus()
}

func CertifyGraphFloat32DirectView(req GraphFloat32DirectViewRequest) (PreparedFloat32DirectView, Status) {
	exp := req.Expectation
	if status := validateGraphDirectViewExpectation(exp, req.Certification, string(columnsemantics.LogicalFloat32), typedcolumn.ColumnTypeFloat32, typedcolumn.EncodingRawFloat32); !status.Direct() {
		return PreparedFloat32DirectView{}, status
	}
	if status := validateFixedWidthGraphSection(exp, req.Certification, req.Section, typedcolumn.EncodingRawFloat32); !status.Direct() {
		return PreparedFloat32DirectView{}, status
	}
	plan := Float32ScalarPlan(req.Certification)
	directReq := DirectViewColumnRequest{Plan: plan, Certification: req.Certification, Rows: exp.Rows, PayloadBytes: req.Section.Length, AssetOffset: exp.AssetOffset, HasAssetOffset: exp.HasAssetOffset}
	if status := ValidateDirectViewColumn(directReq); !status.Direct() {
		return PreparedFloat32DirectView{}, status
	}
	if status := validateFixedWidthGraphHandle(req.Handle, exp, req.Section, typedcolumn.EncodingRawFloat32, req.ExpectedKey); !status.Direct() {
		return PreparedFloat32DirectView{}, status
	}
	values, status := Float32ScalarView(req.Manager, req.Handle, directReq, ResourceViewOptions{ExpectedElements: exp.Rows, RequireMapped: true})
	if !status.Direct() {
		return PreparedFloat32DirectView{}, status
	}
	return PreparedFloat32DirectView{Expectation: exp, Rows: exp.Rows, Values: values, Handle: req.Handle}, DirectStatus()
}

func CertifyGraphInt64DirectView(req GraphInt64DirectViewRequest) (PreparedInt64DirectView, Status) {
	exp := req.Expectation
	if status := validateGraphDirectViewExpectation(exp, req.Certification, string(columnsemantics.LogicalInt64), typedcolumn.ColumnTypeInt64, typedcolumn.EncodingRawInt64); !status.Direct() {
		return PreparedInt64DirectView{}, status
	}
	if status := validateFixedWidthGraphSection(exp, req.Certification, req.Section, typedcolumn.EncodingRawInt64); !status.Direct() {
		return PreparedInt64DirectView{}, status
	}
	plan := Int64DirectViewPlan(req.Certification)
	directReq := DirectViewColumnRequest{Plan: plan, Certification: req.Certification, Rows: exp.Rows, PayloadBytes: req.Section.Length, AssetOffset: exp.AssetOffset, HasAssetOffset: exp.HasAssetOffset}
	if status := ValidateDirectViewColumn(directReq); !status.Direct() {
		return PreparedInt64DirectView{}, status
	}
	if status := validateFixedWidthGraphHandle(req.Handle, exp, req.Section, typedcolumn.EncodingRawInt64, req.ExpectedKey); !status.Direct() {
		return PreparedInt64DirectView{}, status
	}
	values, status := Int64View(req.Manager, req.Handle, ResourceViewOptions{ExpectedElements: exp.Rows, RequireMapped: true})
	if !status.Direct() {
		return PreparedInt64DirectView{}, status
	}
	return PreparedInt64DirectView{Expectation: exp, Rows: exp.Rows, Values: values, Handle: req.Handle}, DirectStatus()
}

func CertifyGraphUint32ListDirectView(req GraphUint32ListDirectViewRequest) (PreparedUint32ListDirectView, Status) {
	exp := req.Expectation
	if status := validateGraphDirectViewExpectation(exp, req.Certification, string(columnsemantics.LogicalUint32List), typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList); !status.Direct() {
		return PreparedUint32ListDirectView{}, status
	}
	if status := validateOffsetsValuesGraphSections(exp, req.Certification, req.OffsetsSection, req.ValuesSection, typedcolumn.EncodingRawUint32OffsetsList, true); !status.Direct() {
		return PreparedUint32ListDirectView{}, status
	}
	plan := Uint32ListPlan(req.Certification)
	directReq := Uint32OffsetsListDirectViewRequest{Plan: plan, Certification: req.Certification, Rows: exp.Rows, OffsetsBytes: req.OffsetsSection.Length, ValuesBytes: req.ValuesSection.Length, AssetOffset: exp.AssetOffset, HasAssetOffset: exp.HasAssetOffset}
	if status := ValidateUint32OffsetsListDirectViewSections(directReq); !status.Direct() {
		return PreparedUint32ListDirectView{}, status
	}
	if status := validateOffsetsValuesGraphHandles(req.OffsetsHandle, req.ValuesHandle, exp, req.OffsetsSection, req.ValuesSection, typedcolumn.EncodingRawUint32OffsetsList, req.ExpectedOffsetsKey, req.ExpectedValuesKey); !status.Direct() {
		return PreparedUint32ListDirectView{}, status
	}
	offsets, values, status := Uint32ListView(req.Manager, req.OffsetsHandle, req.ValuesHandle, directReq, ResourceViewOptions{RequireMapped: true})
	if !status.Direct() {
		return PreparedUint32ListDirectView{}, status
	}
	return PreparedUint32ListDirectView{Expectation: exp, Rows: exp.Rows, Offsets: offsets, Values: values, OffsetsHandle: req.OffsetsHandle, ValuesHandle: req.ValuesHandle}, DirectStatus()
}

func CertifyGraphBytesDirectView(req GraphBytesDirectViewRequest) (PreparedBytesDirectView, Status) {
	exp := req.Expectation
	if status := validateGraphDirectViewExpectation(exp, req.Certification, string(columnsemantics.LogicalBytes), typedcolumn.ColumnTypeBytes, typedcolumn.EncodingRawBytesOffsets); !status.Direct() {
		return PreparedBytesDirectView{}, status
	}
	if status := validateOffsetsValuesGraphSections(exp, req.Certification, req.OffsetsSection, req.ValuesSection, typedcolumn.EncodingRawBytesOffsets, false); !status.Direct() {
		return PreparedBytesDirectView{}, status
	}
	plan := BytesPlan(req.Certification)
	directReq := BytesDirectViewRequest{Plan: plan, Certification: req.Certification, Rows: exp.Rows, OffsetsBytes: req.OffsetsSection.Length, ValuesBytes: req.ValuesSection.Length, AssetOffset: exp.AssetOffset, HasAssetOffset: exp.HasAssetOffset}
	if status := ValidateBytesDirectViewSections(directReq); !status.Direct() {
		return PreparedBytesDirectView{}, status
	}
	if status := validateOffsetsValuesGraphHandles(req.OffsetsHandle, req.ValuesHandle, exp, req.OffsetsSection, req.ValuesSection, typedcolumn.EncodingRawBytesOffsets, req.ExpectedOffsetsKey, req.ExpectedValuesKey); !status.Direct() {
		return PreparedBytesDirectView{}, status
	}
	offsets, values, status := BytesView(req.Manager, req.OffsetsHandle, req.ValuesHandle, directReq, ResourceViewOptions{RequireMapped: true})
	if !status.Direct() {
		return PreparedBytesDirectView{}, status
	}
	return PreparedBytesDirectView{Expectation: exp, Rows: exp.Rows, Offsets: offsets, Values: values, OffsetsHandle: req.OffsetsHandle, ValuesHandle: req.ValuesHandle}, DirectStatus()
}

func validateGraphDirectViewExpectation(exp GraphDirectViewExpectation, cert typedcolumn.ColumnPartLayoutContractColumn, logical string, typ typedcolumn.ColumnType, encoding typedcolumn.Encoding) Status {
	if exp.ExpectedOwner == "" || exp.ActualOwner == "" {
		return UnsupportedStatus(ReasonOwnerMismatch, fmt.Sprintf("expected_owner=%q actual_owner=%q", exp.ExpectedOwner, exp.ActualOwner))
	}
	if exp.ExpectedOwner != exp.ActualOwner {
		return UnsupportedStatus(ReasonOwnerMismatch, fmt.Sprintf("owner=%q want %q", exp.ActualOwner, exp.ExpectedOwner))
	}
	if exp.ExpectedRole == "" || exp.ActualRole == "" {
		return UnsupportedStatus(ReasonRoleMismatch, fmt.Sprintf("expected_role=%q actual_role=%q", exp.ExpectedRole, exp.ActualRole))
	}
	if exp.ExpectedRole != exp.ActualRole {
		return UnsupportedStatus(ReasonRoleMismatch, fmt.Sprintf("role=%q want %q", exp.ActualRole, exp.ExpectedRole))
	}
	if exp.Column == "" || cert.Name != exp.Column {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("cert_column=%q want %q", cert.Name, exp.Column))
	}
	if cert.LogicalType != logical || cert.Type != typ || cert.Encoding != encoding {
		return UnsupportedStatus(ReasonTypeEncodingMismatch, fmt.Sprintf("logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", cert.LogicalType, cert.Type, cert.Encoding, logical, typ, encoding))
	}
	if exp.Rows < 0 || cert.Rows != exp.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("cert_rows=%d request_rows=%d", cert.Rows, exp.Rows))
	}
	return DirectStatus()
}

func validateFixedWidthGraphSection(exp GraphDirectViewExpectation, cert typedcolumn.ColumnPartLayoutContractColumn, section typedcolumn.ColumnPartImageSection, encoding typedcolumn.Encoding) Status {
	if section.Kind != typedcolumn.ColumnPartImageSectionColumnData || section.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumns {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("section kind/category=(%s,%s) want (%s,%s)", section.Kind, section.Category, typedcolumn.ColumnPartImageSectionColumnData, typedcolumn.ColumnPartImageCategoryDeclaredColumns))
	}
	if section.Column != exp.Column {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("section column=%q want %q", section.Column, exp.Column))
	}
	if section.Encoding != encoding {
		return UnsupportedStatus(ReasonTypeEncodingMismatch, fmt.Sprintf("section encoding=%s want %s", section.Encoding, encoding))
	}
	if section.Compression != typedcolumn.CompressionNone {
		return UnsupportedStatus(ReasonCompressed, fmt.Sprintf("section compression=%s", section.Compression))
	}
	if section.Rows != exp.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("section_rows=%d request_rows=%d", section.Rows, exp.Rows))
	}
	if section.Offset != cert.Section.Offset || section.Length != cert.Section.Length {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("section offset/length=(%d,%d) cert=(%d,%d)", section.Offset, section.Length, cert.Section.Offset, cert.Section.Length))
	}
	return DirectStatus()
}

func validateOffsetsValuesGraphSections(exp GraphDirectViewExpectation, cert typedcolumn.ColumnPartLayoutContractColumn, offsetsSection, valuesSection typedcolumn.ColumnPartImageSection, encoding typedcolumn.Encoding, valuesNeedUint32Multiple bool) Status {
	if offsetsSection.Kind != typedcolumn.ColumnPartImageSectionColumnOffsets || offsetsSection.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumnOffsets {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("offsets section kind/category=(%s,%s)", offsetsSection.Kind, offsetsSection.Category))
	}
	if valuesSection.Kind != typedcolumn.ColumnPartImageSectionColumnValues || valuesSection.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumnValues {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("values section kind/category=(%s,%s)", valuesSection.Kind, valuesSection.Category))
	}
	if offsetsSection.Column != exp.Column || valuesSection.Column != exp.Column {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("section columns offsets=%q values=%q want %q", offsetsSection.Column, valuesSection.Column, exp.Column))
	}
	if offsetsSection.Encoding != encoding || valuesSection.Encoding != encoding {
		return UnsupportedStatus(ReasonTypeEncodingMismatch, fmt.Sprintf("section encodings offsets=%s values=%s want %s", offsetsSection.Encoding, valuesSection.Encoding, encoding))
	}
	if offsetsSection.Compression != typedcolumn.CompressionNone || valuesSection.Compression != typedcolumn.CompressionNone {
		return UnsupportedStatus(ReasonCompressed, fmt.Sprintf("section compression offsets=%s values=%s", offsetsSection.Compression, valuesSection.Compression))
	}
	if offsetsSection.Rows != exp.Rows || valuesSection.Rows != exp.Rows {
		return StreamingStatus(ReasonRowCountMismatch, fmt.Sprintf("section rows offsets=%d values=%d request=%d", offsetsSection.Rows, valuesSection.Rows, exp.Rows))
	}
	if offsetsSection.Offset != cert.OffsetsSection.Offset || offsetsSection.Length != cert.OffsetsSection.Length || valuesSection.Offset != cert.ValuesSection.Offset || valuesSection.Length != cert.ValuesSection.Length {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("section offset/length mismatch offsets=(%d,%d)/(%d,%d) values=(%d,%d)/(%d,%d)", offsetsSection.Offset, offsetsSection.Length, cert.OffsetsSection.Offset, cert.OffsetsSection.Length, valuesSection.Offset, valuesSection.Length, cert.ValuesSection.Offset, cert.ValuesSection.Length))
	}
	if valuesNeedUint32Multiple && valuesSection.Length%4 != 0 {
		return UnsupportedStatus(ReasonValuesLengthMismatch, fmt.Sprintf("values bytes=%d want multiple of 4", valuesSection.Length))
	}
	return DirectStatus()
}

func validateFixedWidthGraphHandle(h *mappedresource.Handle, exp GraphDirectViewExpectation, section typedcolumn.ColumnPartImageSection, encoding typedcolumn.Encoding, expectedKey mappedresource.Key) Status {
	if status := validateHandle(h, mappedresource.SourceMapped, true); !status.Direct() {
		return status
	}
	return validateGraphHandleKey(h, exp, section.Kind, section.Category, section.Length, encoding, expectedKey)
}

func validateOffsetsValuesGraphHandles(offsetsHandle, valuesHandle *mappedresource.Handle, exp GraphDirectViewExpectation, offsetsSection, valuesSection typedcolumn.ColumnPartImageSection, encoding typedcolumn.Encoding, expectedOffsetsKey, expectedValuesKey mappedresource.Key) Status {
	if status := validateHandle(offsetsHandle, mappedresource.SourceMapped, true); !status.Direct() {
		return status
	}
	if status := validateHandle(valuesHandle, mappedresource.SourceMapped, true); !status.Direct() {
		return status
	}
	if status := validateGraphHandleKey(offsetsHandle, exp, offsetsSection.Kind, offsetsSection.Category, offsetsSection.Length, encoding, expectedOffsetsKey); !status.Direct() {
		return status
	}
	return validateGraphHandleKey(valuesHandle, exp, valuesSection.Kind, valuesSection.Category, valuesSection.Length, encoding, expectedValuesKey)
}

func validateGraphHandleKey(h *mappedresource.Handle, exp GraphDirectViewExpectation, kind typedcolumn.ColumnPartImageSectionKind, category typedcolumn.ColumnPartImageSectionCategory, length int, encoding typedcolumn.Encoding, expectedKey mappedresource.Key) Status {
	key := h.Key()
	if status := validateGraphHandleKeyLayout(key, exp, kind, category, length, encoding, "resource"); !status.Direct() {
		return status
	}
	if status := validateGraphHandleKeyLayout(expectedKey, exp, kind, category, length, encoding, "expected resource"); !status.Direct() {
		return status
	}
	if key.Namespace != expectedKey.Namespace || key.Kind != expectedKey.Kind || key.Generation != expectedKey.Generation || key.PartID != expectedKey.PartID || key.FileID != expectedKey.FileID || key.Offset != expectedKey.Offset || key.Checksum != expectedKey.Checksum || key.Version != expectedKey.Version || key.Section.Name != expectedKey.Section.Name || key.Section.Ordinal != expectedKey.Section.Ordinal {
		return UnsupportedStatus(ReasonResourceMismatch, fmt.Sprintf("resource identity namespace/kind/generation/part/file/offset/checksum/version/section_name/section_ordinal=(%q,%q,%d,%d,%d,%d,%d,%d,%q,%d) want (%q,%q,%d,%d,%d,%d,%d,%d,%q,%d)", key.Namespace, key.Kind, key.Generation, key.PartID, key.FileID, key.Offset, key.Checksum, key.Version, key.Section.Name, key.Section.Ordinal, expectedKey.Namespace, expectedKey.Kind, expectedKey.Generation, expectedKey.PartID, expectedKey.FileID, expectedKey.Offset, expectedKey.Checksum, expectedKey.Version, expectedKey.Section.Name, expectedKey.Section.Ordinal))
	}
	return DirectStatus()
}

func validateGraphHandleKeyLayout(key mappedresource.Key, exp GraphDirectViewExpectation, kind typedcolumn.ColumnPartImageSectionKind, category typedcolumn.ColumnPartImageSectionCategory, length int, encoding typedcolumn.Encoding, label string) Status {
	if key.Class != mappedresource.ClassTypedColumnAsset {
		return UnsupportedStatus(ReasonOwnerMismatch, fmt.Sprintf("%s class=%s want %s", label, key.Class, mappedresource.ClassTypedColumnAsset))
	}
	if key.Section.Column != exp.Column {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("%s column=%q want %q", label, key.Section.Column, exp.Column))
	}
	if key.Section.Kind != string(kind) {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("%s section kind=%q want %q", label, key.Section.Kind, kind))
	}
	if key.Section.Category != string(category) {
		return UnsupportedStatus(ReasonColumnMismatch, fmt.Sprintf("%s section category=%q want %q", label, key.Section.Category, category))
	}
	if key.Encoding != encoding.String() {
		return UnsupportedStatus(ReasonTypeEncodingMismatch, fmt.Sprintf("%s encoding=%q want %q", label, key.Encoding, encoding.String()))
	}
	if key.Length != int64(length) {
		return UnsupportedStatus(ReasonPayloadLengthMismatch, fmt.Sprintf("%s length=%d want %d", label, key.Length, length))
	}
	return DirectStatus()
}

func (v PreparedFloat32VectorDirectView) Alive() bool {
	return v.Handle != nil && !v.Handle.Released() && len(v.Values) == v.Rows*v.Dims
}

func (v PreparedFloat32VectorDirectView) Row(row int) []float32 {
	if !v.Alive() || row < 0 || row >= v.Rows || v.Dims <= 0 {
		return nil
	}
	start := row * v.Dims
	end := start + v.Dims
	if start < 0 || end < start || end > len(v.Values) {
		return nil
	}
	return v.Values[start:end]
}

func (v *PreparedFloat32VectorDirectView) Close() error {
	if v == nil {
		return nil
	}
	h := v.Handle
	v.Handle = nil
	v.Values = nil
	v.Rows = 0
	v.Dims = 0
	if h == nil {
		return nil
	}
	return h.Release()
}

func (v PreparedFloat32DirectView) Alive() bool {
	return v.Handle != nil && !v.Handle.Released() && len(v.Values) == v.Rows
}

func (v PreparedFloat32DirectView) Value(row int) (float32, bool) {
	if !v.Alive() || row < 0 || row >= v.Rows {
		return 0, false
	}
	return v.Values[row], true
}

func (v *PreparedFloat32DirectView) Close() error {
	if v == nil {
		return nil
	}
	h := v.Handle
	v.Handle = nil
	v.Values = nil
	v.Rows = 0
	if h == nil {
		return nil
	}
	return h.Release()
}

func (v PreparedInt64DirectView) Alive() bool {
	return v.Handle != nil && !v.Handle.Released() && len(v.Values) == v.Rows
}

func (v PreparedInt64DirectView) Value(row int) (int64, bool) {
	if !v.Alive() || row < 0 || row >= v.Rows {
		return 0, false
	}
	return v.Values[row], true
}

func (v *PreparedInt64DirectView) Close() error {
	if v == nil {
		return nil
	}
	h := v.Handle
	v.Handle = nil
	v.Values = nil
	v.Rows = 0
	if h == nil {
		return nil
	}
	return h.Release()
}

func (v PreparedUint32ListDirectView) Alive() bool {
	return v.OffsetsHandle != nil && v.ValuesHandle != nil && !v.OffsetsHandle.Released() && !v.ValuesHandle.Released() && len(v.Offsets) == v.Rows+1
}

func (v PreparedUint32ListDirectView) Row(row int) []uint32 {
	if !v.Alive() || row < 0 || row >= v.Rows {
		return nil
	}
	begin := v.Offsets[row]
	end := v.Offsets[row+1]
	if begin > end || begin > uint64(len(v.Values)) || end > uint64(len(v.Values)) {
		return nil
	}
	return v.Values[int(begin):int(end)]
}

func (v *PreparedUint32ListDirectView) Close() error {
	if v == nil {
		return nil
	}
	offsets := v.OffsetsHandle
	values := v.ValuesHandle
	v.OffsetsHandle = nil
	v.ValuesHandle = nil
	v.Offsets = nil
	v.Values = nil
	v.Rows = 0
	return errors.Join(releaseDirectViewHandle(offsets), releaseDirectViewHandle(values))
}

func (v PreparedBytesDirectView) Alive() bool {
	return v.OffsetsHandle != nil && v.ValuesHandle != nil && !v.OffsetsHandle.Released() && !v.ValuesHandle.Released() && len(v.Offsets) == v.Rows+1
}

func (v PreparedBytesDirectView) Row(row int) []byte {
	if !v.Alive() || row < 0 || row >= v.Rows {
		return nil
	}
	begin := v.Offsets[row]
	end := v.Offsets[row+1]
	if begin > end || begin > uint64(len(v.Values)) || end > uint64(len(v.Values)) {
		return nil
	}
	return v.Values[int(begin):int(end)]
}

func (v *PreparedBytesDirectView) Close() error {
	if v == nil {
		return nil
	}
	offsets := v.OffsetsHandle
	values := v.ValuesHandle
	v.OffsetsHandle = nil
	v.ValuesHandle = nil
	v.Offsets = nil
	v.Values = nil
	v.Rows = 0
	return errors.Join(releaseDirectViewHandle(offsets), releaseDirectViewHandle(values))
}

func releaseDirectViewHandle(h *mappedresource.Handle) error {
	if h == nil {
		return nil
	}
	return h.Release()
}
