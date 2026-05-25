package typeddecode

import (
	"encoding/binary"
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
	cert := typedcolumn.ColumnPartLayoutContractColumn{
		Name: "vec", LogicalType: "float32_vector", Type: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone,
		Rows: 2, FixedWidthElements: 4, ElementSize: 4, Alignment: 4, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 4, DirectViewCertified: true,
	}
	cert.Blocks = []typedcolumn.ColumnPartLayoutContractBlock{{RowCount: 2, Encoding: cert.Encoding, Compression: cert.Compression, RawBytes: 32, StoredBytes: 32, PayloadLength: 32}}
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
