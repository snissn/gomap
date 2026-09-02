package typeddecode

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/columnlayout"
	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestFixedBytesPlanRejectsLogicalBitsMismatch1931(t *testing.T) {
	cert := typedcolumn.ColumnPartLayoutContractColumn{
		LogicalType:         string(columnsemantics.LogicalByteVector),
		Type:                typedcolumn.ColumnTypeFixedBytes,
		Encoding:            typedcolumn.EncodingRawFixedBytes,
		Compression:         typedcolumn.CompressionNone,
		DirectViewCertified: true,
		Endian:              typedcolumn.ColumnPartLayoutEndianLittle,
		ElementSize:         1,
		Alignment:           1,
		LengthMultiple:      1,
		FixedWidthElements:  3,
		BytesPerRow:         3,
		LogicalBitsPerRow:   25,
		Rows:                2,
	}
	plan := FixedBytesPlan(cert, 3)
	if plan.Path != PathUnsupported || plan.Reason != ReasonDimensionMismatch || !strings.Contains(plan.Message, "logical_bits_per_row=25 want 24") {
		t.Fatalf("plan=%+v want logical_bits_per_row mismatch", plan)
	}
}

func TestFixedWidthElementsDiagnosticsUseStoredFieldNames1931(t *testing.T) {
	base := typedcolumn.ColumnPartLayoutContractColumn{
		DirectViewCertified: true,
		Compression:         typedcolumn.CompressionNone,
		Endian:              typedcolumn.ColumnPartLayoutEndianLittle,
		ElementSize:         1,
		Alignment:           1,
		LengthMultiple:      1,
	}
	fixed := base
	fixed.Type = typedcolumn.ColumnTypeFixedBytes
	fixed.FixedWidthElements = 3
	if status := validateDirectViewCertificationFields(fixed, 1, 4); status.Reason != ReasonDimensionMismatch || !strings.Contains(status.Message, "bytes_per_row=3 want 4") {
		t.Fatalf("fixed status=%+v want bytes_per_row diagnostic", status)
	}
	packed := base
	packed.Type = typedcolumn.ColumnTypePackedUint4Vector
	packed.FixedWidthElements = 3
	if status := validateDirectViewCertificationFields(packed, 1, 4); status.Reason != ReasonDimensionMismatch || !strings.Contains(status.Message, "fixed_width_elements=3 want 4") {
		t.Fatalf("packed status=%+v want fixed_width_elements diagnostic", status)
	}
}

func TestInt64DirectViewPlanAndHandleValidation(t *testing.T) {
	rows := 3
	caps := columnlayout.CapabilitiesFor(columnlayout.Descriptor{Logical: columnsemantics.LogicalInt64, Physical: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone})
	cert := testInt64DirectCert(rows)
	plan := Int64ReducerPlan(caps, cert)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	blockStatus := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: cert.Blocks[0], Rows: rows, PayloadBytes: rows * 8, AssetOffset: 0, HasAssetOffset: true})
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
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: truncated, Rows: 2, PayloadBytes: 15, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("truncated status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}

	wrongRows := cert.Blocks[0]
	wrongRows.RowCount = 3
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: wrongRows, Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonRowCountMismatch {
		t.Fatalf("row status=%+v want %s", status, ReasonRowCountMismatch)
	}
	wrongBytes := cert.Blocks[0]
	wrongBytes.StoredBytes = 8
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: wrongBytes, Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonPayloadLengthMismatch {
		t.Fatalf("byte contract status=%+v want %s", status, ReasonPayloadLengthMismatch)
	}

	wrongEndian := cert
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: wrongEndian, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonWrongEndian {
		t.Fatalf("endian status=%+v want %s", status, ReasonWrongEndian)
	}

	nullable := cert
	nullable.NullMaskPresent = true
	nullable.NullCount = 1
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: nullable, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonNullableWrapper {
		t.Fatalf("nullable status=%+v want %s", status, ReasonNullableWrapper)
	}

	compressed := cert
	compressed.Compression = typedcolumn.CompressionSnappy
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: compressed, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonCompressed {
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
	if _, status := Int64View(mgr, h, ResourceViewOptions{ExpectedElements: 2, RequireMapped: true}); status.Reason != ReasonActualPointerUnaligned {
		t.Fatalf("unaligned status=%+v want %s", status, ReasonActualPointerUnaligned)
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

func TestScalarFloatDirectViewPlansPreserveRawBitsAndValidateHandles(t *testing.T) {
	cases := []struct {
		name      string
		bits      []uint64
		cert      typedcolumn.ColumnPartLayoutContractColumn
		plan      func(typedcolumn.ColumnPartLayoutContractColumn) Plan
		put       func([]byte, uint64)
		viewBits  func(*mappedresource.Manager, *mappedresource.Handle, DirectViewColumnRequest, int) ([]uint64, Status)
		elemBytes int
	}{
		{
			name:      "float32",
			bits:      []uint64{0x00000000, 0x80000000, 0x7f800000, 0xff800000, 0x7f7fffff, 0xff7fffff, 0x7fc00001, 0x7fa12345},
			cert:      testFloat32ScalarDirectCert(8),
			plan:      Float32ScalarPlan,
			put:       func(dst []byte, bits uint64) { binary.LittleEndian.PutUint32(dst, uint32(bits)) },
			elemBytes: 4,
			viewBits: func(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, want int) ([]uint64, Status) {
				view, status := Float32ScalarView(mgr, h, req, ResourceViewOptions{ExpectedElements: want, RequireMapped: true})
				out := make([]uint64, len(view))
				for i, value := range view {
					out[i] = uint64(math.Float32bits(value))
				}
				return out, status
			},
		},
		{
			name:      "float64",
			bits:      []uint64{0x0000000000000000, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7fefffffffffffff, 0xffefffffffffffff, 0x7ff8000000000001, 0x7ff123456789abcd},
			cert:      testFloat64ScalarDirectCert(8),
			plan:      Float64ScalarPlan,
			put:       func(dst []byte, bits uint64) { binary.LittleEndian.PutUint64(dst, bits) },
			elemBytes: 8,
			viewBits: func(mgr *mappedresource.Manager, h *mappedresource.Handle, req DirectViewColumnRequest, want int) ([]uint64, Status) {
				view, status := Float64ScalarView(mgr, h, req, ResourceViewOptions{ExpectedElements: want, RequireMapped: true})
				out := make([]uint64, len(view))
				for i, value := range view {
					out[i] = math.Float64bits(value)
				}
				return out, status
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := tc.plan(tc.cert)
			if !plan.DirectCandidate() {
				t.Fatalf("plan=%+v want direct candidate", plan)
			}
			req := DirectViewColumnRequest{Plan: plan, Certification: tc.cert, Rows: len(tc.bits), PayloadBytes: len(tc.bits) * tc.elemBytes, AssetOffset: 0, HasAssetOffset: true}
			if status := ValidateDirectViewColumn(req); !status.Direct() {
				t.Fatalf("column status=%+v want direct", status)
			}
			raw := alignedBytes(tc.elemBytes, len(tc.bits)*tc.elemBytes)
			for i, bits := range tc.bits {
				tc.put(raw[i*tc.elemBytes:], bits)
			}
			mgr := mappedresource.NewManager()
			h, err := mgr.AcquireBytes(testKeyWithPart(uint64(20+tc.elemBytes), int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: tc.name + " scalar direct view"})
			if err != nil {
				t.Fatalf("AcquireBytes: %v", err)
			}
			got, status := tc.viewBits(mgr, h, req, len(tc.bits))
			if !status.Direct() {
				t.Fatalf("view status=%+v want direct", status)
			}
			for i, bits := range tc.bits {
				if got[i] != bits {
					t.Fatalf("bits[%d]=0x%x want 0x%x", i, got[i], bits)
				}
			}
			misalignedRaw := alignedBytes(tc.elemBytes, len(raw)+1)[1:]
			mh, err := mgr.AcquireBytes(testKeyWithPart(uint64(40+tc.elemBytes), int64(len(misalignedRaw))), testScope(), mappedresource.SourceMapped, misalignedRaw, mappedresource.AcquireOptions{Reason: tc.name + " scalar misaligned"})
			if err != nil {
				t.Fatalf("AcquireBytes misaligned: %v", err)
			}
			if _, status := tc.viewBits(mgr, mh, req, len(tc.bits)); status.Reason != ReasonActualPointerUnaligned {
				t.Fatalf("misaligned status=%+v want %s", status, ReasonActualPointerUnaligned)
			}
			_ = mh.Release()
			if err := h.Release(); err != nil {
				t.Fatalf("Release: %v", err)
			}
			if _, status := tc.viewBits(mgr, h, req, len(tc.bits)); status.Reason != ReasonStaleHandle {
				t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
			}
		})
	}
}

func TestScalarFloatDirectViewValidationRejectsFallbackLayouts(t *testing.T) {
	cert := testFloat32ScalarDirectCert(2)
	plan := Float32ScalarPlan(cert)
	valid := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 8, AssetOffset: 0, HasAssetOffset: true}
	if status := ValidateDirectViewColumn(valid); !status.Direct() {
		t.Fatalf("valid status=%+v want direct", status)
	}
	wrongEndian := cloneDirectViewCert(cert)
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: Float32ScalarPlan(wrongEndian), Certification: wrongEndian, Rows: 2, PayloadBytes: 8, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonWrongEndian {
		t.Fatalf("wrong endian status=%+v want %s", status, ReasonWrongEndian)
	}
	wrongRows := valid
	wrongRows.Rows = 3
	if status := ValidateDirectViewColumn(wrongRows); status.Reason != ReasonRowCountMismatch {
		t.Fatalf("wrong rows status=%+v want %s", status, ReasonRowCountMismatch)
	}
	wrongDims := cloneDirectViewCert(cert)
	wrongDims.FixedWidthElements = 1
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: Float32ScalarPlan(wrongDims), Certification: wrongDims, Rows: 2, PayloadBytes: 8, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("scalar dims status=%+v want %s", status, ReasonDimensionMismatch)
	}
	wrongLength := cloneDirectViewCert(cert)
	wrongLength.Section.Length = 7
	wrongLength.Blocks[0].PayloadLength = 7
	wrongLength.Blocks[0].RawBytes = 7
	wrongLength.Blocks[0].StoredBytes = 7
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: wrongLength, Rows: 2, PayloadBytes: 7, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("wrong length status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}
	notCertified := cloneDirectViewCert(cert)
	notCertified.DirectViewCertified = false
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: Float32ScalarPlan(notCertified), Certification: notCertified, Rows: 2, PayloadBytes: 8, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonNotWriterCertified {
		t.Fatalf("missing cert status=%+v want %s", status, ReasonNotWriterCertified)
	}
	rawInt64Carrier := typedcolumn.ColumnPartLayoutContractColumn{Name: "score", LogicalType: "float32", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, Rows: 2, Section: typedcolumn.ColumnPartLayoutContractSection{Length: 16}, ElementSize: 8, Alignment: 8, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 8, DirectViewCertified: true, Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{RowCount: 2, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, RawBytes: 16, StoredBytes: 16, PayloadLength: 16}}}
	if rawPlan := Float32ScalarPlan(rawInt64Carrier); rawPlan.DirectCandidate() || rawPlan.Reason == ReasonSupported {
		t.Fatalf("raw-int64 float carrier plan=%+v want non-native fallback/unsupported", rawPlan)
	}
}

func TestDenseDirectViewBlockValidatesDimensionsAndLength(t *testing.T) {
	cert := testFloat32VectorDirectCert(2, 4)
	plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 4, ElementsPerRow: 4, Alignment: 4, Rows: 2}
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: plan, Certification: cert, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 32, AssetOffset: 0, HasAssetOffset: true}); !status.Direct() {
		t.Fatalf("dense status=%+v want direct", status)
	}
	wrongDims := plan
	wrongDims.ElementsPerRow = 3
	if status := ValidateDirectViewBlock(DirectViewBlockRequest{Plan: wrongDims, Certification: cert, Block: cert.Blocks[0], Rows: 2, PayloadBytes: 32, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("dimension status=%+v want %s", status, ReasonDimensionMismatch)
	}
}

func TestDenseFloat32VectorDirectViewValidationCoversDimsLengthEndianAlignmentAndLifetime(t *testing.T) {
	cert := testFloat32VectorDirectCert(2, 3)
	plan := DenseFloat32VectorPlan(cert, 3)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	columnReq := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}
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
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongDims, Certification: cert, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonDimensionMismatch {
		t.Fatalf("dims status=%+v want %s", status, ReasonDimensionMismatch)
	}
	wrongEndian := cloneDirectViewCert(cert)
	wrongEndian.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
	wrongEndianPlan := DenseFloat32VectorPlan(wrongEndian, 3)
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: wrongEndianPlan, Certification: wrongEndian, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonWrongEndian {
		t.Fatalf("endian status=%+v want %s", status, ReasonWrongEndian)
	}
	truncated := cloneDirectViewCert(cert)
	truncated.Section.Length = 23
	truncated.Blocks[0].PayloadLength = 23
	truncated.Blocks[0].RawBytes = 23
	truncated.Blocks[0].StoredBytes = 23
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: truncated, Rows: 2, PayloadBytes: 23, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonLengthMultipleMismatch {
		t.Fatalf("truncated status=%+v want %s", status, ReasonLengthMultipleMismatch)
	}
	corrupt := cloneDirectViewCert(cert)
	corrupt.Blocks[0].StoredBytes = 20
	if status := ValidateDirectViewColumn(DirectViewColumnRequest{Plan: plan, Certification: corrupt, Rows: 2, PayloadBytes: 24, AssetOffset: 0, HasAssetOffset: true}); status.Reason != ReasonPayloadLengthMismatch {
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
	if _, status := DenseFloat32VectorView(mgr, mh, columnReq, ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != ReasonActualPointerUnaligned {
		t.Fatalf("unaligned status=%+v want %s", status, ReasonActualPointerUnaligned)
	}
	_ = mh.Release()
	if err := h.Release(); err != nil {
		t.Fatalf("Release vector: %v", err)
	}
	if _, status := DenseFloat32VectorView(mgr, h, columnReq, ResourceViewOptions{ExpectedElements: 6, RequireMapped: true}); status.Reason != ReasonStaleHandle {
		t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
	}
}

func TestUint32OffsetsListShapeValidation1914(t *testing.T) {
	valid := Uint32OffsetsListShapeRequest{Rows: 3, Offsets: []uint64{0, 2, 2, 5}, Values: 5}
	if status := ValidateUint32OffsetsListShape(valid); !status.Direct() {
		t.Fatalf("valid offsets-list status=%+v want supported", status)
	}
	cases := []struct {
		name string
		req  Uint32OffsetsListShapeRequest
		want Reason
	}{
		{name: "offset count", req: Uint32OffsetsListShapeRequest{Rows: 3, Offsets: []uint64{0, 1, 2}, Values: 2}, want: ReasonOffsetsCountMismatch},
		{name: "start", req: Uint32OffsetsListShapeRequest{Rows: 1, Offsets: []uint64{1, 2}, Values: 2}, want: ReasonOffsetsStartMismatch},
		{name: "monotonic", req: Uint32OffsetsListShapeRequest{Rows: 2, Offsets: []uint64{0, 3, 2}, Values: 2}, want: ReasonOffsetsNonMonotonic},
		{name: "values length", req: Uint32OffsetsListShapeRequest{Rows: 2, Offsets: []uint64{0, 1, 3}, Values: 2}, want: ReasonValuesLengthMismatch},
		{name: "go int values", req: Uint32OffsetsListShapeRequest{Rows: 1, Offsets: []uint64{0, uint64(int(^uint(0)>>1)) + 1}, Values: uint64(int(^uint(0)>>1)) + 1}, want: ReasonOffsetsGoIntRange},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if status := ValidateUint32OffsetsListShape(tc.req); status.Reason != tc.want {
				t.Fatalf("status=%+v want %s", status, tc.want)
			}
		})
	}
}

func TestUint32ListDirectViewPlanValidationAndView1985(t *testing.T) {
	cert := testUint32ListSpecCert(3, 5)
	plan := Uint32ListPlan(cert)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	if wrong := AdjacencyOffsetsListPlan(cert); wrong.Path != PathUnsupported || wrong.Reason != ReasonValidationFailed {
		t.Fatalf("adjacency plan for uint32_list=%+v want identity mismatch", wrong)
	}
	req := Uint32OffsetsListDirectViewRequest{Plan: plan, Certification: cert, Rows: 3, OffsetsBytes: 32, ValuesBytes: 20, AssetOffset: 0, HasAssetOffset: true}
	if status := ValidateUint32OffsetsListDirectViewSections(req); !status.Direct() {
		t.Fatalf("sections status=%+v want direct", status)
	}
	offsetsRaw := alignedBytes(8, 32)
	for i, v := range []uint64{0, 2, 2, 5} {
		binary.LittleEndian.PutUint64(offsetsRaw[i*8:], v)
	}
	valuesRaw := alignedBytes(4, 20)
	for i, v := range []uint32{7, 8, 9, 10, 11} {
		binary.LittleEndian.PutUint32(valuesRaw[i*4:], v)
	}
	mgr := mappedresource.NewManager()
	t.Cleanup(func() {
		if stats := mgr.Stats(); stats.ActiveHandles != 0 {
			t.Fatalf("active handles after cleanup: stats=%+v", stats)
		}
	})
	offsetsHandle, err := mgr.AcquireBytes(testKeyWithPart(198501, int64(len(offsetsRaw))), testScope(), mappedresource.SourceMapped, offsetsRaw, mappedresource.AcquireOptions{Reason: "uint32_list offsets"})
	if err != nil {
		t.Fatalf("AcquireBytes offsets: %v", err)
	}
	defer offsetsHandle.Release()
	valuesHandle, err := mgr.AcquireBytes(testKeyWithPart(198502, int64(len(valuesRaw))), testScope(), mappedresource.SourceMapped, valuesRaw, mappedresource.AcquireOptions{Reason: "uint32_list values"})
	if err != nil {
		t.Fatalf("AcquireBytes values: %v", err)
	}
	defer valuesHandle.Release()
	offsets, values, status := Uint32ListView(mgr, offsetsHandle, valuesHandle, req, ResourceViewOptions{RequireMapped: true})
	if !status.Direct() {
		t.Fatalf("view status=%+v want direct", status)
	}
	if !equalUint64s(offsets, []uint64{0, 2, 2, 5}) || !equalUint32s(values, []uint32{7, 8, 9, 10, 11}) {
		t.Fatalf("offsets=%v values=%v", offsets, values)
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 2 || stats.DirectViewFailures != 0 {
		t.Fatalf("stats=%+v want two mmap direct-view successes", stats)
	}

	badOffsets := append([]uint64(nil), offsets...)
	badOffsets[len(badOffsets)-1]++
	if status := ValidateUint32OffsetsListDirectView(req, badOffsets, values); status.Reason != ReasonValuesLengthMismatch {
		t.Fatalf("bad final offset status=%+v want %s", status, ReasonValuesLengthMismatch)
	}
	nullable := cert
	nullable.NullMaskPresent = true
	if got := Uint32ListPlan(nullable); got.Path != PathUnsupported || got.Reason != ReasonNullableWrapper {
		t.Fatalf("nullable uint32_list plan=%+v want unsupported nullable wrapper", got)
	}
}

func TestAdjacencyOffsetsListDirectViewPlanValidationAndView(t *testing.T) {
	cert := testAdjacencyOffsetsListSpecCert(2, 2)
	plan := AdjacencyOffsetsListPlan(cert)
	if !plan.DirectCandidate() {
		t.Fatalf("plan=%+v want direct candidate", plan)
	}
	req := Uint32OffsetsListDirectViewRequest{Plan: plan, Certification: cert, Rows: 2, OffsetsBytes: 24, ValuesBytes: 8, AssetOffset: 0, HasAssetOffset: true}
	offsetsRaw := alignedBytes(8, 24)
	for i, v := range []uint64{0, 2, 2} {
		binary.LittleEndian.PutUint64(offsetsRaw[i*8:], v)
	}
	valuesRaw := alignedBytes(4, 8)
	for i, v := range []uint32{7, 8} {
		binary.LittleEndian.PutUint32(valuesRaw[i*4:], v)
	}
	mgr := mappedresource.NewManager()
	t.Cleanup(func() {
		if stats := mgr.Stats(); stats.ActiveHandles != 0 {
			t.Fatalf("active handles after cleanup: stats=%+v", stats)
		}
	})
	offsetsHandle, err := mgr.AcquireBytes(testKeyWithPart(191601, int64(len(offsetsRaw))), testScope(), mappedresource.SourceMapped, offsetsRaw, mappedresource.AcquireOptions{Reason: "offsets-list offsets"})
	if err != nil {
		t.Fatalf("AcquireBytes offsets: %v", err)
	}
	defer offsetsHandle.Release()
	valuesHandle, err := mgr.AcquireBytes(testKeyWithPart(191602, int64(len(valuesRaw))), testScope(), mappedresource.SourceMapped, valuesRaw, mappedresource.AcquireOptions{Reason: "offsets-list values"})
	if err != nil {
		t.Fatalf("AcquireBytes values: %v", err)
	}
	defer valuesHandle.Release()
	offsets, values, status := Uint32OffsetsListView(mgr, offsetsHandle, valuesHandle, req, ResourceViewOptions{RequireMapped: true})
	if !status.Direct() {
		t.Fatalf("view status=%+v want direct", status)
	}
	if !equalUint64s(offsets, []uint64{0, 2, 2}) || !equalUint32s(values, []uint32{7, 8}) {
		t.Fatalf("offsets=%v values=%v", offsets, values)
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 2 || stats.DirectViewFailures != 0 {
		t.Fatalf("stats=%+v want two mmap direct-view successes", stats)
	}

	heapHandle, err := mgr.AcquireBytes(testKeyWithPart(191603, int64(len(offsetsRaw))), testScope(), mappedresource.SourceHeapCopy, offsetsRaw, mappedresource.AcquireOptions{Reason: "offsets-list heap"})
	if err != nil {
		t.Fatalf("AcquireBytes heap: %v", err)
	}
	defer heapHandle.Release()
	if _, _, status := Uint32OffsetsListView(mgr, heapHandle, valuesHandle, req, ResourceViewOptions{RequireMapped: true}); status.Reason != ReasonHandleSourceUnsupported {
		t.Fatalf("heap status=%+v want %s", status, ReasonHandleSourceUnsupported)
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 2 {
		t.Fatalf("heap-copy typed view counted as mmap direct success: stats=%+v", stats)
	}

	misalignedOffsets := alignedBytes(8, len(offsetsRaw)+1)[1 : len(offsetsRaw)+1]
	misalignedHandle, err := mgr.AcquireBytes(testKeyWithPart(191604, int64(len(misalignedOffsets))), testScope(), mappedresource.SourceMapped, misalignedOffsets, mappedresource.AcquireOptions{Reason: "offsets-list misaligned offsets"})
	if err != nil {
		t.Fatalf("AcquireBytes misaligned offsets: %v", err)
	}
	defer misalignedHandle.Release()
	if _, _, status := Uint32OffsetsListView(mgr, misalignedHandle, valuesHandle, req, ResourceViewOptions{RequireMapped: true}); status.Reason != ReasonActualPointerUnaligned {
		t.Fatalf("misaligned status=%+v want %s", status, ReasonActualPointerUnaligned)
	}

	truncatedValuesRaw := alignedBytes(4, 4)
	binary.LittleEndian.PutUint32(truncatedValuesRaw, 7)
	truncatedValuesHandle, err := mgr.AcquireBytes(testKeyWithPart(191605, int64(len(truncatedValuesRaw))), testScope(), mappedresource.SourceMapped, truncatedValuesRaw, mappedresource.AcquireOptions{Reason: "offsets-list truncated values"})
	if err != nil {
		t.Fatalf("AcquireBytes truncated values: %v", err)
	}
	defer truncatedValuesHandle.Release()
	if _, _, status := Uint32OffsetsListView(mgr, offsetsHandle, truncatedValuesHandle, req, ResourceViewOptions{RequireMapped: true}); status.Reason != ReasonValuesLengthMismatch {
		t.Fatalf("truncated values status=%+v want %s", status, ReasonValuesLengthMismatch)
	}
}

func TestAdjacencyOffsetsListDirectViewFailsClosed(t *testing.T) {
	base := testAdjacencyOffsetsListSpecCert(2, 2)
	plan := AdjacencyOffsetsListPlan(base)
	if !plan.DirectCandidate() {
		t.Fatalf("base plan=%+v", plan)
	}
	planCases := []struct {
		name     string
		edit     func(*typedcolumn.ColumnPartLayoutContractColumn)
		want     Reason
		wantPath Path
	}{
		{name: "missing_cert", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.DirectViewCertified = false }, want: ReasonNotWriterCertified, wantPath: PathMaterialize},
		{name: "wrong_logical", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.LogicalType = "int64" }, want: ReasonValidationFailed, wantPath: PathUnsupported},
		{name: "wrong_type", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.Type = typedcolumn.ColumnTypeInt64 }, want: ReasonValidationFailed, wantPath: PathUnsupported},
		{name: "wrong_encoding", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.Encoding = typedcolumn.EncodingRawUint32Dense }, want: ReasonValidationFailed, wantPath: PathUnsupported},
		{name: "wrong_endian", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) {
			c.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
		}, want: ReasonWrongEndian, wantPath: PathMaterialize},
		{name: "wrong_element_size", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.ElementSize = 8 }, want: ReasonDimensionMismatch, wantPath: PathMaterialize},
		{name: "wrong_length_multiple", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.LengthMultiple = 6 }, want: ReasonLengthMultipleMismatch, wantPath: PathMaterialize},
		{name: "fixed_width_elements", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.FixedWidthElements = 2 }, want: ReasonDimensionMismatch, wantPath: PathMaterialize},
		{name: "compressed", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.Compression = typedcolumn.CompressionSnappy }, want: ReasonCompressed, wantPath: PathUnsupported},
		{name: "nullable", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.NullMaskPresent = true; c.NullCount = 1 }, want: ReasonNullableWrapper, wantPath: PathUnsupported},
		{name: "defaultable", edit: func(c *typedcolumn.ColumnPartLayoutContractColumn) { c.DefaultMaskPresent = true; c.DefaultCount = 1 }, want: ReasonNullableWrapper, wantPath: PathUnsupported},
	}
	for _, tc := range planCases {
		t.Run("plan_"+tc.name, func(t *testing.T) {
			cert := cloneDirectViewCert(base)
			tc.edit(&cert)
			if got := AdjacencyOffsetsListPlan(cert); got.Reason != tc.want || got.Path != tc.wantPath {
				t.Fatalf("plan=%+v want path=%s reason=%s", got, tc.wantPath, tc.want)
			}
		})
	}

	validReq := Uint32OffsetsListDirectViewRequest{Plan: plan, Certification: base, Rows: 2, OffsetsBytes: 24, ValuesBytes: 8, AssetOffset: 0, HasAssetOffset: true}
	validOffsets := []uint64{0, 1, 2}
	validValues := []uint32{10, 20}
	if status := ValidateUint32OffsetsListDirectView(validReq, validOffsets, validValues); !status.Direct() {
		t.Fatalf("valid status=%+v", status)
	}
	validationCases := []struct {
		name    string
		req     Uint32OffsetsListDirectViewRequest
		offsets []uint64
		values  []uint32
		want    Reason
	}{
		{name: "row_count", req: func() Uint32OffsetsListDirectViewRequest { r := validReq; r.Rows = 3; return r }(), offsets: validOffsets, values: validValues, want: ReasonRowCountMismatch},
		{name: "offsets_length", req: func() Uint32OffsetsListDirectViewRequest { r := validReq; r.OffsetsBytes = 16; return r }(), offsets: validOffsets, values: validValues, want: ReasonPayloadLengthMismatch},
		{name: "values_length_multiple", req: func() Uint32OffsetsListDirectViewRequest {
			r := validReq
			r.ValuesBytes = 7
			r.Certification.ValuesBytes = 7
			r.Certification.ValuesSection.Length = 7
			return r
		}(), offsets: validOffsets, values: validValues, want: ReasonValuesLengthMismatch},
		{name: "offsets_absolute_alignment", req: func() Uint32OffsetsListDirectViewRequest {
			r := validReq
			r.Certification.OffsetsSection.Offset = 4
			return r
		}(), offsets: validOffsets, values: validValues, want: ReasonAbsoluteOffsetUnaligned},
		{name: "values_absolute_alignment", req: func() Uint32OffsetsListDirectViewRequest {
			r := validReq
			r.Certification.ValuesSection.Offset = 26
			return r
		}(), offsets: validOffsets, values: validValues, want: ReasonAbsoluteOffsetUnaligned},
		{name: "missing_asset_offset", req: func() Uint32OffsetsListDirectViewRequest { r := validReq; r.HasAssetOffset = false; return r }(), offsets: validOffsets, values: validValues, want: ReasonAbsoluteOffsetUnaligned},
		{name: "offsets_start", req: validReq, offsets: []uint64{1, 1, 2}, values: validValues, want: ReasonOffsetsStartMismatch},
		{name: "offsets_non_monotonic", req: validReq, offsets: []uint64{0, 2, 1}, values: validValues, want: ReasonOffsetsNonMonotonic},
		{name: "final_offset", req: validReq, offsets: []uint64{0, 1, 3}, values: validValues, want: ReasonValuesLengthMismatch},
	}
	for _, tc := range validationCases {
		t.Run(tc.name, func(t *testing.T) {
			if status := ValidateUint32OffsetsListDirectView(tc.req, tc.offsets, tc.values); status.Reason != tc.want {
				t.Fatalf("status=%+v want %s", status, tc.want)
			}
		})
	}
}

func TestAdjacencyDirectViewValidationIsDeferredForCurrentStack(t *testing.T) {
	cert := testAdjacencyDirectCert(2, 2)
	plan := AdjacencyListPlan(cert, 2)
	if plan.DirectCandidate() || plan.Reason != ReasonDirectViewDeferred {
		t.Fatalf("plan=%+v want deferred non-direct dense adjacency", plan)
	}
	columnReq := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: 2, PayloadBytes: 16, AssetOffset: 0, HasAssetOffset: true}
	if status := ValidateDirectViewColumn(columnReq); status.Path != PathUnsupported || status.Reason != ReasonDirectViewDeferred {
		t.Fatalf("adjacency status=%+v want deferred unsupported", status)
	}

	offsetsCert := testAdjacencyOffsetsListSpecCert(2, 3)
	offsetsPlan := AdjacencyOffsetsListPlan(offsetsCert)
	if !offsetsPlan.DirectCandidate() || offsetsPlan.ElementsPerRow != 0 {
		t.Fatalf("offsets-list plan=%+v want explicit variable-list direct candidate", offsetsPlan)
	}
	if offsetsPlan.ElementSize != 4 || offsetsPlan.Alignment != 4 {
		t.Fatalf("offsets-list plan=%+v want uint32 value identity", offsetsPlan)
	}
	denseCert := testAdjacencyDirectCert(2, 2)
	if plan := AdjacencyOffsetsListPlan(denseCert); plan.Reason != ReasonValidationFailed {
		t.Fatalf("dense cert offsets-list plan=%+v want identity validation failure", plan)
	}
}

var fixedWidthScalarDirectViewSink1899 float64

func BenchmarkFixedWidthScalarDirectView1899(b *testing.B) {
	const rows = 8192
	b.Run("int64_mmap", func(b *testing.B) {
		raw := alignedBytes(8, rows*8)
		for i := 0; i < rows; i++ {
			binary.LittleEndian.PutUint64(raw[i*8:], uint64(i))
		}
		cert := testInt64DirectCert(rows)
		plan := Plan{Path: PathDirectView, Reason: ReasonSupported, ElementSize: 8, ElementsPerRow: 1, Alignment: 8, Rows: rows}
		req := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: rows, PayloadBytes: len(raw), AssetOffset: 0, HasAssetOffset: true}
		if status := ValidateDirectViewColumn(req); !status.Direct() {
			b.Fatalf("ValidateDirectViewColumn: %s", status.String())
		}
		mgr := mappedresource.NewManager()
		h, err := mgr.AcquireBytes(testKeyWithPart(189901, int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "#1899 int64 direct-view benchmark"})
		if err != nil {
			b.Fatalf("AcquireBytes: %v", err)
		}
		b.Cleanup(func() { _ = h.Release() })
		opts := ResourceViewOptions{ExpectedElements: rows, RequireMapped: true}
		var direct uint64
		var sink int64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if status := ValidateDirectViewColumn(req); !status.Direct() {
				b.Fatalf("ValidateDirectViewColumn: %s", status.String())
			}
			viewOpts, status := normalizeFixedWidthViewOptions(req, opts)
			if !status.Direct() {
				b.Fatalf("normalizeFixedWidthViewOptions: %s", status.String())
			}
			view, status := Int64View(mgr, h, viewOpts)
			if !status.Direct() {
				b.Fatalf("Int64View: %s", status.String())
			}
			sink += view[i&7]
			direct++
		}
		b.StopTimer()
		fixedWidthScalarDirectViewSink1899 += float64(sink)
		b.ReportMetric(float64(direct)/float64(b.N), "mmap_direct_view/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
		b.ReportMetric(0, "certification_failure/op")
		b.ReportMetric(float64(len(raw)), "mapped_B")
	})
	b.Run("float32_mmap", func(b *testing.B) {
		raw := alignedBytes(4, rows*4)
		for i := 0; i < rows; i++ {
			binary.LittleEndian.PutUint32(raw[i*4:], uint32(i)|0x3f800000)
		}
		cert := testFloat32ScalarDirectCert(rows)
		plan := Float32ScalarPlan(cert)
		req := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: rows, PayloadBytes: len(raw), AssetOffset: 0, HasAssetOffset: true}
		if status := ValidateDirectViewColumn(req); !status.Direct() {
			b.Fatalf("ValidateDirectViewColumn: %s", status.String())
		}
		mgr := mappedresource.NewManager()
		h, err := mgr.AcquireBytes(testKeyWithPart(189904, int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "#1899 scalar float32 direct-view benchmark"})
		if err != nil {
			b.Fatalf("AcquireBytes: %v", err)
		}
		b.Cleanup(func() { _ = h.Release() })
		opts := ResourceViewOptions{ExpectedElements: rows, RequireMapped: true}
		var direct uint64
		var sink float32
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if status := ValidateDirectViewColumn(req); !status.Direct() {
				b.Fatalf("ValidateDirectViewColumn: %s", status.String())
			}
			viewOpts, status := normalizeFixedWidthViewOptions(req, opts)
			if !status.Direct() {
				b.Fatalf("normalizeFixedWidthViewOptions: %s", status.String())
			}
			view, status := Float32View(mgr, h, viewOpts)
			if !status.Direct() {
				b.Fatalf("Float32View: %s", status.String())
			}
			sink += view[i&7]
			direct++
		}
		b.StopTimer()
		fixedWidthScalarDirectViewSink1899 += float64(sink)
		b.ReportMetric(float64(direct)/float64(b.N), "mmap_direct_view/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
		b.ReportMetric(0, "certification_failure/op")
		b.ReportMetric(float64(len(raw)), "mapped_B")
	})
	b.Run("float64_mmap", func(b *testing.B) {
		raw := alignedBytes(8, rows*8)
		for i := 0; i < rows; i++ {
			binary.LittleEndian.PutUint64(raw[i*8:], uint64(i)|0x3ff0000000000000)
		}
		cert := testFloat64ScalarDirectCert(rows)
		plan := Float64ScalarPlan(cert)
		req := DirectViewColumnRequest{Plan: plan, Certification: cert, Rows: rows, PayloadBytes: len(raw), AssetOffset: 0, HasAssetOffset: true}
		if status := ValidateDirectViewColumn(req); !status.Direct() {
			b.Fatalf("ValidateDirectViewColumn: %s", status.String())
		}
		mgr := mappedresource.NewManager()
		h, err := mgr.AcquireBytes(testKeyWithPart(189908, int64(len(raw))), testScope(), mappedresource.SourceMapped, raw, mappedresource.AcquireOptions{Reason: "#1899 scalar float64 direct-view benchmark"})
		if err != nil {
			b.Fatalf("AcquireBytes: %v", err)
		}
		b.Cleanup(func() { _ = h.Release() })
		opts := ResourceViewOptions{ExpectedElements: rows, RequireMapped: true}
		var direct uint64
		var sink float64
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if status := ValidateDirectViewColumn(req); !status.Direct() {
				b.Fatalf("ValidateDirectViewColumn: %s", status.String())
			}
			viewOpts, status := normalizeFixedWidthViewOptions(req, opts)
			if !status.Direct() {
				b.Fatalf("normalizeFixedWidthViewOptions: %s", status.String())
			}
			view, status := Float64View(mgr, h, viewOpts)
			if !status.Direct() {
				b.Fatalf("Float64View: %s", status.String())
			}
			sink += view[i&7]
			direct++
		}
		b.StopTimer()
		fixedWidthScalarDirectViewSink1899 += sink
		b.ReportMetric(float64(direct)/float64(b.N), "mmap_direct_view/op")
		b.ReportMetric(0, "heap_copy_typed_view/op")
		b.ReportMetric(0, "scratch_decode/op")
		b.ReportMetric(0, "certification_failure/op")
		b.ReportMetric(float64(len(raw)), "mapped_B")
	})
}

func cloneDirectViewCert(cert typedcolumn.ColumnPartLayoutContractColumn) typedcolumn.ColumnPartLayoutContractColumn {
	cert.Blocks = append([]typedcolumn.ColumnPartLayoutContractBlock(nil), cert.Blocks...)
	return cert
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalUint32s(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testFloat32ScalarDirectCert(rows int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * 4
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "score32", LogicalType: "float32", Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Length: bytes}, ElementSize: 4, Alignment: 4, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 4, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 0, PayloadLength: bytes}},
	}
}

func testFloat64ScalarDirectCert(rows int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * 8
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "score64", LogicalType: "double", Type: typedcolumn.ColumnTypeFloat64, Encoding: typedcolumn.EncodingRawFloat64, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Length: bytes}, ElementSize: 8, Alignment: 8, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 8, DirectViewCertified: true,
		Blocks: []typedcolumn.ColumnPartLayoutContractBlock{{FirstRow: 0, RowCount: rows, Encoding: typedcolumn.EncodingRawFloat64, Compression: typedcolumn.CompressionNone, RawBytes: bytes, StoredBytes: bytes, PayloadOffset: 0, PayloadLength: bytes}},
	}
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

func testAdjacencyOffsetsListSpecCert(rows, values int) typedcolumn.ColumnPartLayoutContractColumn {
	return testUint32OffsetsListSpecCert("neighbors", "adjacency_list", typedcolumn.ColumnTypeAdjacencyList, rows, values)
}

func testUint32ListSpecCert(rows, values int) typedcolumn.ColumnPartLayoutContractColumn {
	return testUint32OffsetsListSpecCert("tags", "uint32_list", typedcolumn.ColumnTypeUint32List, rows, values)
}

func testUint32OffsetsListSpecCert(name, logical string, columnType typedcolumn.ColumnType, rows, values int) typedcolumn.ColumnPartLayoutContractColumn {
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: name, LogicalType: logical, Type: columnType, Encoding: typedcolumn.EncodingRawUint32OffsetsList, Compression: typedcolumn.CompressionNone,
		Rows:                rows,
		OffsetsSection:      typedcolumn.ColumnPartLayoutContractSection{Offset: 0, Length: (rows + 1) * 8},
		ValuesSection:       typedcolumn.ColumnPartLayoutContractSection{Offset: (rows + 1) * 8, Length: values * 4},
		OffsetsBytes:        (rows + 1) * 8,
		ValuesBytes:         values * 4,
		ElementSize:         4,
		Alignment:           4,
		Endian:              typedcolumn.ColumnPartLayoutEndianLittle,
		LengthMultiple:      4,
		DirectViewCertified: true,
	}
}

func testInt64DirectCert(rows int) typedcolumn.ColumnPartLayoutContractColumn {
	bytes := rows * 8
	return typedcolumn.ColumnPartLayoutContractColumn{
		Name: "v", LogicalType: "int64", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone,
		Rows: rows, Section: typedcolumn.ColumnPartLayoutContractSection{Length: bytes}, ElementSize: 8, Alignment: 8, Endian: typedcolumn.ColumnPartLayoutEndianLittle, LengthMultiple: 8, DirectViewCertified: true,
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
