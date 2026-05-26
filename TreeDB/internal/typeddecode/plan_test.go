package typeddecode

import (
	"encoding/binary"
	"math"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestInt64DirectViewPlanAndHandleValidation(t *testing.T) {
	rows := 3
	caps := columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone})
	cert := testInt64DirectCert(rows)
	plan := Int64ReducerPlan(caps, cert)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	blockStatus := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: cert.Blocks[0], Rows: rows, PayloadBytes: rows * 8})
	if !blockStatus.Direct() {
		t.Fatalf("block status=%+v want direct", blockStatus)
	}

	raw := alignedBytes(8, rows*8)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(i+10))
	}
	mgr := mappedresource.NewManager()
	h, err := mgr.AcquireBytes(testKey(int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "typeddecode test"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	view, viewStatus := Int64View(mgr, h, ResourceViewOptions{ExpectedElements: rows, RequireMapped: true})
	if !viewStatus.Direct() {
		t.Fatalf("view status=%+v want direct", viewStatus)
	}
	if len(view) != rows || view[0] != 10 || view[2] != 12 {
		t.Fatalf("view=%v", view)
	}
	if err := h.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 1 || stats.DirectViewFailures != 0 {
		t.Fatalf("stats=%+v want one direct-view success", stats)
	}
}

func TestDirectViewValidationFailsClosedForLengthRowEndianAndNullable(t *testing.T) {
	cert := testInt64DirectCert(2)
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 8, ElementsPerRow: 1, Alignment: 8, Rows: 2}

	truncated := cert.Blocks[0]
	truncated.PayloadLength = 15
	truncated.RawBytes = 15
	truncated.StoredBytes = 15
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: truncated, Rows: 2, PayloadBytes: 15}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("truncated status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}

	wrongRows := cert.Blocks[0]
	wrongRows.RowCount = 3
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: wrongRows, Rows: 2, PayloadBytes: 16}); status.Reason != ReasonRowCountMismatch {
		t.Fatalf("row status=%+v want %s", status, ReasonRowCountMismatch)
	}
	wrongBytes := cert.Blocks[0]
	wrongBytes.StoredBytes = 8
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: wrongBytes, Rows: 2, PayloadBytes: 16}); status.Reason != ReasonPayloadLengthMismatch {
		t.Fatalf("byte contract status=%+v want %s", status, ReasonPayloadLengthMismatch)
	}

	wrongEndian := cert
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: wrongEndian, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16}); status.Reason != ReasonWrongEndian {
		t.Fatalf("endian status=%+v want %s", status, ReasonWrongEndian)
	}

	nullable := cert
	nullable.NullMaskPresent = true
	nullable.NullCount = 1
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: nullable, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16}); status.Reason != ReasonNullableWrapper {
		t.Fatalf("nullable status=%+v want %s", status, ReasonNullableWrapper)
	}

	compressed := cert
	compressed.Compression = typedcolumn.CompressionSnappy
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: compressed, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16}); status.Reason != ReasonCompressed {
		t.Fatalf("compressed status=%+v want %s", status, ReasonCompressed)
	}
}

func TestStatusDoesNotImplementError(t *testing.T) {
	if _, ok := any(DirectStatus()).(error); ok {
		t.Fatal("Status must not implement error; successful statuses would become non-nil errors")
	}
	if got := UnsupportedStatus(ReasonUnsupportedOperation, "nope").String(); got == "" {
		t.Fatal("Status.String returned empty diagnostic")
	}
}

func TestStatusFromLayoutCapabilityUsesTypedStatus(t *testing.T) {
	stream := statusFromLayoutCapability(columnlayout.Capability{
		Status:  columnsemantics.StatusFallback,
		Reason:  columnlayout.ReasonVariableWidthNoDirectView,
		Message: "no direct view",
	})
	if !stream.Streaming() || stream.Reason != ReasonVariableWidth {
		t.Fatalf("stream status=%+v", stream)
	}
	unsupported := statusFromLayoutCapability(columnlayout.Capability{
		Status:  columnsemantics.StatusUnsupported,
		Reason:  columnlayout.ReasonVariableWidthNoDirectView,
		Message: "no direct view",
	})
	if !unsupported.Unsupported() || unsupported.Reason != ReasonVariableWidth {
		t.Fatalf("unsupported status=%+v", unsupported)
	}
}

func TestDirectViewHandleRejectsUnalignedHeapAndStale(t *testing.T) {
	raw := alignedBytes(8, 17)
	misaligned := raw[1:17]
	mgr := mappedresource.NewManager()
	h, err := mgr.AcquireBytes(testKey(int64(len(misaligned))), testScope(), mappedresource.SourceMapped, misaligned, mappedresource.AcquireOptions{Reason: "typeddecode unaligned"})
	if err != nil {
		t.Fatalf("AcquireBytes mapped: %v", err)
	}
	if _, status := Int64View(mgr, h, ResourceViewOptions{ExpectedElements: 2, RequireMapped: true}); status.Reason != ReasonUnaligned {
		t.Fatalf("unaligned status=%+v want %s", status, ReasonUnaligned)
	}
	_ = h.Release()
	if _, status := Int64View(mgr, h, ResourceViewOptions{ExpectedElements: 2, RequireMapped: true}); status.Reason != ReasonStaleHandle {
		t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
	}

	heapRaw := alignedBytes(8, 16)
	heap, err := mgr.AcquireBytes(testKeyWithPart(2, int64(len(heapRaw))), testScope(), mappedresource.SourceHeapCopy, heapRaw, mappedresource.AcquireOptions{Reason: "typeddecode heap"})
	if err != nil {
		t.Fatalf("AcquireBytes heap: %v", err)
	}
	defer func() { _ = heap.Release() }()
	if _, status := Int64View(mgr, heap, ResourceViewOptions{ExpectedElements: 2, RequireMapped: true}); status.Reason != ReasonHandleSourceUnsupported {
		t.Fatalf("heap status=%+v want %s", status, ReasonHandleSourceUnsupported)
	}
}

func TestInt64DeltaPlanUsesStreamingNotDirectView(t *testing.T) {
	caps := columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone})
	cert := typedcolumn.ColumnPartLayoutContractColumn{Name: "v", LogicalType: "int64", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionNone, Rows: 4, Endian: typedcolumn.ColumnPartLayoutEndianCodecDefined, StreamingCertified: true}
	plan := Int64ReducerPlan(caps, cert)
	if plan.Path != PathStreaming || plan.Reason != ReasonVariableWidth {
		t.Fatalf("plan=%+v want variable-width streaming", plan)
	}
}

func TestDenseDirectViewBlockValidatesDimensionsAndLength(t *testing.T) {
	cert := testFloat32VectorDirectCert(2, 4)
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 4, ElementsPerRow: 4, Alignment: 4, Rows: 2}
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 32}); !status.Direct() {
		t.Fatalf("dense status=%+v want direct", status)
	}
	wrongDims := plan
	wrongDims.ElementsPerRow = 3
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: wrongDims, Certification: cert, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 32}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("dimension status=%+v want %s", status, ReasonDimensionMismatch)
	}
}

func TestDenseFloat32VectorDirectViewValidationCoversDimsLengthEndianAlignmentAndLifetime(t *testing.T) {
	cert := testFloat32VectorDirectCert(2, 3)
	plan := DenseFloat32VectorPlan(cert, 3)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	columnReq := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 24}
	if status := ValidateDirectViewColumn(columnReq); !status.Direct() {
		t.Fatalf("column status=%+v want direct", status)
	}

	raw := alignedBytes(4, 24)
	for i, value := range []float32{1, 2, 3, 4, 5, 6} {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	mgr := mappedresource.NewManager()
	h, err := mgr.AcquireBytes(testKeyWithPart(10, int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "typeddecode dense vector"})
	if err != nil {
		t.Fatalf("AcquireBytes vector: %v", err)
	}
	view, status := DenseFloat32VectorView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 6, RequireMapped: true})
	if !status.Direct() || len(view) != 6 || view[0] != 1 || view[5] != 6 {
		t.Fatalf("vector view=%v status=%+v", view, status)
	}

	wrongDims := DenseFloat32VectorPlan(cert, 2)
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongDims, Certification: cert, Rows: 2, PayloadBytes: 24}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("dims status=%+v want %s", status, ReasonDimensionMismatch)
	}
	wrongEndian := cloneDirectViewCert(cert)
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	wrongEndianPlan := DenseFloat32VectorPlan(wrongEndian, 3)
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongEndianPlan, Certification: wrongEndian, Rows: 2, PayloadBytes: 24}); status.Reason != ReasonWrongEndian {
		t.Fatalf("endian status=%+v want %s", status, ReasonWrongEndian)
	}
	truncated := cloneDirectViewCert(cert)
	truncated.Section.Length = 23
	truncated.Blocks[0].PayloadLength = 23
	truncated.Blocks[0].RawBytes = 23
	truncated.Blocks[0].StoredBytes = 23
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: truncated, Rows: 2, PayloadBytes: 23}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("truncated status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}
	corrupt := cloneDirectViewCert(cert)
	corrupt.Blocks[0].StoredBytes = 20
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: corrupt, Rows: 2, PayloadBytes: 24}); status.Reason != ReasonPayloadLengthMismatch {
		t.Fatalf("corrupt status=%+v want %s", status, ReasonPayloadLengthMismatch)
	}
	if _, status := DenseFloat32VectorView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 5, RequireMapped: true}); status.Reason != ReasonRowCountMismatch {
		t.Fatalf("expected-elements status=%+v want %s", status, ReasonRowCountMismatch)
	}

	misalignedRaw := alignedBytes(4, 25)[1:25]
	mh, err := mgr.AcquireBytes(testKeyWithPart(11, int64(len(misalignedRaw))), testScope(), mappedresource.SourceMapped, misalignedRaw, mappedresource.AcquireOptions{Reason: "typeddecode dense vector misaligned"})
	if err != nil {
		t.Fatalf("AcquireBytes misaligned vector: %v", err)
	}
	if _, status := DenseFloat32VectorView(mgr, mh, columnReq, ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != ReasonUnaligned {
		t.Fatalf("unaligned status=%+v want %s", status, ReasonUnaligned)
	}
	_ = mh.Release()
	if err := h.Release(); err != nil {
		t.Fatalf("Release vector: %v", err)
	}
	if _, status := DenseFloat32VectorView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != ReasonStaleHandle {
		t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
	}
}

func TestAdjacencyDirectViewValidationCoversDegreeLengthEndianAlignmentAndLifetime(t *testing.T) {
	cert := testAdjacencyDirectCert(2, 2)
	plan := AdjacencyListPlan(cert, 2)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	columnReq := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 16}
	if status := ValidateDirectViewColumn(columnReq); !status.Direct() {
		t.Fatalf("adjacency column status=%+v want direct", status)
	}

	raw := alignedBytes(4, 16)
	for i, value := range []uint32{7, 11, 13, 17} {
		binary.LittleEndian.PutUint32(raw[i*4:], value)
	}
	mgr := mappedresource.NewManager()
	h, err := mgr.AcquireBytes(testKeyWithPart(12, int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "typeddecode adjacency"})
	if err != nil {
		t.Fatalf("AcquireBytes adjacency: %v", err)
	}
	view, status := AdjacencyListView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 4, RequireMapped: true})
	if !status.Direct() || len(view) != 4 || view[0] != 7 || view[3] != 17 {
		t.Fatalf("adjacency view=%v status=%+v", view, status)
	}

	wrongDegree := AdjacencyListPlan(cert, 3)
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongDegree, Certification: cert, Rows: 2, PayloadBytes: 16}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("degree status=%+v want %s", status, ReasonDimensionMismatch)
	}
	wrongEndian := cloneDirectViewCert(cert)
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	wrongEndianPlan := AdjacencyListPlan(wrongEndian, 2)
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongEndianPlan, Certification: wrongEndian, Rows: 2, PayloadBytes: 16}); status.Reason != ReasonWrongEndian {
		t.Fatalf("endian status=%+v want %s", status, ReasonWrongEndian)
	}
	truncated := cloneDirectViewCert(cert)
	truncated.Section.Length = 15
	truncated.Blocks[0].PayloadLength = 15
	truncated.Blocks[0].RawBytes = 15
	truncated.Blocks[0].StoredBytes = 15
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: truncated, Rows: 2, PayloadBytes: 15}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("truncated status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}
	corrupt := cloneDirectViewCert(cert)
	corrupt.Blocks[0].Compression = typedcolumn.CompressionSnappy
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: corrupt, Rows: 2, PayloadBytes: 16}); status.Reason != ReasonValidationFailed {
		t.Fatalf("corrupt status=%+v want %s", status, ReasonValidationFailed)
	}

	misalignedRaw := alignedBytes(4, 17)[1:17]
	mh, err := mgr.AcquireBytes(testKeyWithPart(13, int64(len(misalignedRaw))), testScope(), mappedresource.SourceMapped, misalignedRaw, mappedresource.AcquireOptions{Reason: "typeddecode adjacency misaligned"})
	if err != nil {
		t.Fatalf("AcquireBytes misaligned adjacency: %v", err)
	}
	if _, status := AdjacencyListView(mgr, mh, columnReq, ResourceViewOptions{ExpectedElements: 4, RequireMapped: true}); status.Reason != ReasonUnaligned {
		t.Fatalf("unaligned status=%+v want %s", status, ReasonUnaligned)
	}
	_ = mh.Release()
	if err := h.Release(); err != nil {
		t.Fatalf("Release adjacency: %v", err)
	}
	if _, status := AdjacencyListView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 4, RequireMapped: true}); status.Reason != ReasonStaleHandle {
		t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
	}
}

func cloneDirectViewCert(cert typedcolumn.ColumnPartLayoutContractColumn) typedcolumn.ColumnPartLayoutContractColumn {
	cert.Blocks = append([]typedcolumn.ColumnPartLayoutContractBlock(nil), cert.Blocks...)
	return cert
}

func testFloat32VectorDirectCert(rows, dims int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * dims * 4
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "vec", LogicalType: "float32_vector", Type: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Length: bytes}, FixedWidthElements: dims, ElementSize: 4, Alignment: 4, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 4, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 0, PayloadLength: bytes}},
	}
}

func testAdjacencyDirectCert(rows, degree int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * degree * 4
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "neighbors", LogicalType: "adjacency_list", Type: typedcolumn.ColumnTypeAdjacencyList, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Length: bytes}, FixedWidthElements: degree, ElementSize: 4, Alignment: 4, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 4, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: typedcolumn.EncodingRawUint32Dense, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 0, PayloadLength: bytes}},
	}
}

func testInt64DirectCert(rows int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * 8
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "v", LogicalType: "int64", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone,
		Rows: rows, ElementSize: 8, Alignment: 8, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 8, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{RowCount: rows, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadLength: bytes}},
	}
}

func alignedBytes(align int, n int) []byte {
	buf := make([]byte, n+align)
	addr := uintptr(unsafe.Pointer(&buf[0]))
	off := int((uintptr(align) - addr%uintptr(align)) % uintptr(align))
	return buf[off : off+n]
}

func testKey(length int64) mappedresource.Key { return testKeyWithPart(1, length) }

func testKeyWithPart(partID uint64, length int64) mappedresource.Key {
	return mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: "typeddecode-test", Kind: "section", Generation: 1, PartID: partID, FileID: 1, Length: length}
}

func testScope() mappedresource.Scope {
	return mappedresource.Scope{Kind: mappedresource.ScopePreparedQuery, ID: "typeddecode-test", Namespace: "typeddecode-test"}
}
