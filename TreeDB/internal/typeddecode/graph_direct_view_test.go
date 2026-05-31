package typeddecode

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var graphDirectViewFloatSink float32
var graphDirectViewIntSink int64
var graphDirectViewUintSink uint32
var graphDirectViewByteSink byte

func TestGraphDirectViewCertifiersSuccessAndNoHotLoopAllocs(t *testing.T) {
	t.Run("float32_vector", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphFloat32VectorDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphFloat32VectorDirectView=%s", status.String())
		}
		t.Cleanup(func() { _ = view.Close() })
		if got := view.Row(1); len(got) != 3 || got[0] != 4 || got[2] != 6 {
			t.Fatalf("row=%v", got)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			row := view.Row(1)
			graphDirectViewFloatSink += row[0]
		})
		if allocs != 0 {
			t.Fatalf("Row allocations=%v want 0", allocs)
		}
	})

	t.Run("float32", func(t *testing.T) {
		req := graphFloat32Request(t, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphFloat32DirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphFloat32DirectView=%s", status.String())
		}
		t.Cleanup(func() { _ = view.Close() })
		if got, ok := view.Value(2); !ok || got != 3.5 {
			t.Fatalf("value=%v ok=%v", got, ok)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			value, ok := view.Value(1)
			if ok {
				graphDirectViewFloatSink += value
			}
		})
		if allocs != 0 {
			t.Fatalf("Value allocations=%v want 0", allocs)
		}
	})

	t.Run("int64", func(t *testing.T) {
		req := graphInt64Request(t, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphInt64DirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphInt64DirectView=%s", status.String())
		}
		t.Cleanup(func() { _ = view.Close() })
		if got, ok := view.Value(2); !ok || got != 102 {
			t.Fatalf("value=%v ok=%v", got, ok)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			value, ok := view.Value(1)
			if ok {
				graphDirectViewIntSink += value
			}
		})
		if allocs != 0 {
			t.Fatalf("Value allocations=%v want 0", allocs)
		}
	})

	t.Run("uint32_list", func(t *testing.T) {
		req := graphUint32ListRequest(t, []uint64{0, 2, 2, 5}, []uint32{7, 8, 9, 10, 11}, mappedresource.SourceMapped)
		view, status := CertifyGraphUint32ListDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphUint32ListDirectView=%s", status.String())
		}
		t.Cleanup(func() { _ = view.Close() })
		if got := view.Row(2); len(got) != 3 || got[0] != 9 || got[2] != 11 {
			t.Fatalf("row=%v", got)
		}
		if got := view.Row(1); len(got) != 0 {
			t.Fatalf("empty row=%v", got)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			row := view.Row(0)
			graphDirectViewUintSink += row[0]
		})
		if allocs != 0 {
			t.Fatalf("Row allocations=%v want 0", allocs)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		req := graphBytesRequest(t, []uint64{0, 2, 2, 5}, []byte{'a', 0xff, 'x', 0, 'z'}, mappedresource.SourceMapped)
		view, status := CertifyGraphBytesDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphBytesDirectView=%s", status.String())
		}
		t.Cleanup(func() { _ = view.Close() })
		if got := view.Row(2); len(got) != 3 || got[0] != 'x' || got[2] != 'z' {
			t.Fatalf("row=%v", got)
		}
		if got := view.Row(1); len(got) != 0 {
			t.Fatalf("empty row=%v", got)
		}
		allocs := testing.AllocsPerRun(1000, func() {
			row := view.Row(0)
			graphDirectViewByteSink ^= row[0]
		})
		if allocs != 0 {
			t.Fatalf("Row allocations=%v want 0", allocs)
		}
	})
}

func TestGraphDirectViewPreparedCloseReleasesHandles(t *testing.T) {
	t.Run("float32_vector", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphFloat32VectorDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphFloat32VectorDirectView=%s", status.String())
		}
		handle := view.Handle
		if !view.Alive() || handle == nil || handle.Released() {
			t.Fatalf("view alive=%v handle=%v released=%v", view.Alive(), handle != nil, handle != nil && handle.Released())
		}
		if err := view.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if view.Alive() || view.Row(0) != nil || !handle.Released() {
			t.Fatalf("closed vector view alive=%v row=%v handle_released=%v", view.Alive(), view.Row(0), handle.Released())
		}
		if err := view.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})

	t.Run("float32", func(t *testing.T) {
		req := graphFloat32Request(t, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphFloat32DirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphFloat32DirectView=%s", status.String())
		}
		handle := view.Handle
		if err := view.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, ok := view.Value(0); ok || !handle.Released() {
			t.Fatalf("closed float32 view ok=%v handle_released=%v", ok, handle.Released())
		}
	})

	t.Run("int64", func(t *testing.T) {
		req := graphInt64Request(t, 3, mappedresource.SourceMapped)
		view, status := CertifyGraphInt64DirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphInt64DirectView=%s", status.String())
		}
		handle := view.Handle
		if err := view.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, ok := view.Value(0); ok || !handle.Released() {
			t.Fatalf("closed int64 view ok=%v handle_released=%v", ok, handle.Released())
		}
	})

	t.Run("uint32_list", func(t *testing.T) {
		req := graphUint32ListRequest(t, []uint64{0, 1, 3}, []uint32{4, 5, 6}, mappedresource.SourceMapped)
		view, status := CertifyGraphUint32ListDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphUint32ListDirectView=%s", status.String())
		}
		offsetsHandle, valuesHandle := view.OffsetsHandle, view.ValuesHandle
		if err := view.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if view.Row(0) != nil || !offsetsHandle.Released() || !valuesHandle.Released() {
			t.Fatalf("closed uint32 list row=%v offsets_released=%v values_released=%v", view.Row(0), offsetsHandle.Released(), valuesHandle.Released())
		}
	})

	t.Run("bytes", func(t *testing.T) {
		req := graphBytesRequest(t, []uint64{0, 1, 3}, []byte("abc"), mappedresource.SourceMapped)
		view, status := CertifyGraphBytesDirectView(req)
		if !status.Direct() {
			t.Fatalf("CertifyGraphBytesDirectView=%s", status.String())
		}
		offsetsHandle, valuesHandle := view.OffsetsHandle, view.ValuesHandle
		if err := view.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if view.Row(0) != nil || !offsetsHandle.Released() || !valuesHandle.Released() {
			t.Fatalf("closed bytes row=%v offsets_released=%v values_released=%v", view.Row(0), offsetsHandle.Released(), valuesHandle.Released())
		}
	})
}

func TestGraphDirectViewCertifiersRejectIncompleteResourceIdentity(t *testing.T) {
	t.Run("fixed_width_missing_section_metadata", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		key := graphKey(req.Section.Column, req.Section, typedcolumn.EncodingRawFloat32Vector)
		key.Section = mappedresource.Section{}
		mgr := mappedresource.NewManager()
		req.Manager = mgr
		req.Handle = graphAcquireHandle(t, mgr, mappedresource.SourceMapped, req.Handle.Bytes(), key)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("blank section status=%+v want %s", status, ReasonColumnMismatch)
		}
	})

	t.Run("fixed_width_missing_encoding_metadata", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		key := graphKey(req.Section.Column, req.Section, typedcolumn.EncodingRawFloat32Vector)
		key.Encoding = ""
		mgr := mappedresource.NewManager()
		req.Manager = mgr
		req.Handle = graphAcquireHandle(t, mgr, mappedresource.SourceMapped, req.Handle.Bytes(), key)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonTypeEncodingMismatch {
			t.Fatalf("blank encoding status=%+v want %s", status, ReasonTypeEncodingMismatch)
		}
	})

	t.Run("split_values_missing_section_metadata", func(t *testing.T) {
		req := graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		key := graphKey(req.ValuesSection.Column, req.ValuesSection, typedcolumn.EncodingRawUint32OffsetsList)
		key.Section = mappedresource.Section{}
		mgr := mappedresource.NewManager()
		req.Manager = mgr
		req.ValuesHandle = graphAcquireHandle(t, mgr, mappedresource.SourceMapped, req.ValuesHandle.Bytes(), key)
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("blank split section status=%+v want %s", status, ReasonColumnMismatch)
		}
	})

	t.Run("fixed_width_physical_resource_mismatch", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		key := req.ExpectedKey
		key.Generation++
		mgr := mappedresource.NewManager()
		req.Manager = mgr
		req.Handle = graphAcquireHandle(t, mgr, mappedresource.SourceMapped, req.Handle.Bytes(), key)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonResourceMismatch {
			t.Fatalf("physical resource status=%+v want %s", status, ReasonResourceMismatch)
		}
	})
}

func TestGraphFixedWidthDirectViewCertifiersFailClosed(t *testing.T) {
	t.Run("owner_role_column_type", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Expectation.ActualOwner = "wrong_owner"
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonOwnerMismatch {
			t.Fatalf("owner status=%+v want %s", status, ReasonOwnerMismatch)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Expectation.ActualRole = "wrong_role"
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonRoleMismatch {
			t.Fatalf("role status=%+v want %s", status, ReasonRoleMismatch)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Certification.Name = "other_vec"
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("column status=%+v want %s", status, ReasonColumnMismatch)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Certification.Type = typedcolumn.ColumnTypeInt64
		req.Certification.Encoding = typedcolumn.EncodingRawInt64
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonTypeEncodingMismatch {
			t.Fatalf("type status=%+v want %s", status, ReasonTypeEncodingMismatch)
		}
	})

	t.Run("row_dims_payload_alignment_and_wrappers", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Expectation.Rows = 3
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonRowCountMismatch {
			t.Fatalf("row status=%+v want %s", status, ReasonRowCountMismatch)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Dims = 2
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonDimensionMismatch {
			t.Fatalf("dims status=%+v want %s", status, ReasonDimensionMismatch)
		}

		req = graphFloat32VectorLengthRequest(t, 2, 3, 20)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonPayloadLengthMismatch {
			t.Fatalf("short payload status=%+v want %s", status, ReasonPayloadLengthMismatch)
		}

		req = graphFloat32VectorLengthRequest(t, 2, 3, 28)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonPayloadLengthMismatch {
			t.Fatalf("trailing payload status=%+v want %s", status, ReasonPayloadLengthMismatch)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Expectation.AssetOffset = 1
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonAbsoluteOffsetUnaligned {
			t.Fatalf("misaligned status=%+v want %s", status, ReasonAbsoluteOffsetUnaligned)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Certification.NullMaskPresent = true
		req.Certification.NullCount = 1
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonNullableWrapper {
			t.Fatalf("nullable status=%+v want %s", status, ReasonNullableWrapper)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Certification.DefaultMaskPresent = true
		req.Certification.DefaultCount = 1
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonNullableWrapper {
			t.Fatalf("default status=%+v want %s", status, ReasonNullableWrapper)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		req.Section.Compression = typedcolumn.CompressionSnappy
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonCompressed {
			t.Fatalf("compressed status=%+v want %s", status, ReasonCompressed)
		}
	})

	t.Run("resource_lifetime_source_and_identity", func(t *testing.T) {
		req := graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceHeapCopy)
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonHandleSourceUnsupported {
			t.Fatalf("heap status=%+v want %s", status, ReasonHandleSourceUnsupported)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		if err := req.Handle.Release(); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonStaleHandle {
			t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
		}

		req = graphFloat32VectorRequest(t, 2, 3, mappedresource.SourceMapped)
		misalignedRaw := alignedBytes(4, req.Section.Length+1)[1:]
		for i := 0; i < len(misalignedRaw)/4; i++ {
			binary.LittleEndian.PutUint32(misalignedRaw[i*4:], math.Float32bits(float32(i+1)))
		}
		misalignedMgr := mappedresource.NewManager()
		req.Manager = misalignedMgr
		req.Handle = graphAcquireHandle(t, misalignedMgr, mappedresource.SourceMapped, misalignedRaw, graphKey(req.Section.Column, req.Section, typedcolumn.EncodingRawFloat32Vector))
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonActualPointerUnaligned {
			t.Fatalf("actual pointer status=%+v want %s", status, ReasonActualPointerUnaligned)
		}

		req = graphFloat32VectorRequestWithResourceColumn(t, 2, 3, "other_vec")
		if _, status := CertifyGraphFloat32VectorDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("resource column status=%+v want %s", status, ReasonColumnMismatch)
		}
	})

	t.Run("scalar_families", func(t *testing.T) {
		floatReq := graphFloat32Request(t, 3, mappedresource.SourceMapped)
		floatReq.Certification.Encoding = typedcolumn.EncodingRawInt64
		if _, status := CertifyGraphFloat32DirectView(floatReq); status.Reason != ReasonTypeEncodingMismatch {
			t.Fatalf("float32 type status=%+v want %s", status, ReasonTypeEncodingMismatch)
		}

		intReq := graphInt64Request(t, 3, mappedresource.SourceMapped)
		intReq.Certification.Endian = typedcolumn.ColumnPartLayoutEndianCodecDefined
		if _, status := CertifyGraphInt64DirectView(intReq); status.Reason != ReasonWrongEndian {
			t.Fatalf("int64 endian status=%+v want %s", status, ReasonWrongEndian)
		}

		intReq = graphInt64Request(t, 3, mappedresource.SourceMapped)
		intReq.Handle = nil
		if _, status := CertifyGraphInt64DirectView(intReq); status.Reason != ReasonNilHandle {
			t.Fatalf("nil handle status=%+v want %s", status, ReasonNilHandle)
		}
	})
}

func TestGraphOffsetsDirectViewCertifiersFailClosed(t *testing.T) {
	t.Run("uint32_list_identity_shape_and_sections", func(t *testing.T) {
		req := graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.Certification.LogicalType = "adjacency_list"
		req.Certification.Type = typedcolumn.ColumnTypeAdjacencyList
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonTypeEncodingMismatch {
			t.Fatalf("type status=%+v want %s", status, ReasonTypeEncodingMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.Expectation.Rows = 3
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonRowCountMismatch {
			t.Fatalf("rows status=%+v want %s", status, ReasonRowCountMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.ValuesSection.Length--
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonPayloadLengthMismatch {
			t.Fatalf("short values section status=%+v want %s", status, ReasonPayloadLengthMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.ValuesSection.Length += 4
		req.Certification.ValuesSection.Length += 4
		req.Certification.ValuesBytes += 4
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonPayloadLengthMismatch {
			t.Fatalf("trailing values section status=%+v want %s", status, ReasonPayloadLengthMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.Expectation.AssetOffset = 4
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonAbsoluteOffsetUnaligned {
			t.Fatalf("misaligned offsets status=%+v want %s", status, ReasonAbsoluteOffsetUnaligned)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.Certification.NullMaskPresent = true
		req.Certification.NullCount = 1
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonNullableWrapper {
			t.Fatalf("nullable status=%+v want %s", status, ReasonNullableWrapper)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		req.OffsetsSection.Compression = typedcolumn.CompressionSnappy
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonCompressed {
			t.Fatalf("compressed status=%+v want %s", status, ReasonCompressed)
		}
	})

	t.Run("uint32_list_corrupt_offsets_and_resources", func(t *testing.T) {
		req := graphUint32ListRequest(t, []uint64{1, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonOffsetsStartMismatch {
			t.Fatalf("start status=%+v want %s", status, ReasonOffsetsStartMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 4, 3}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonOffsetsNonMonotonic {
			t.Fatalf("monotonic status=%+v want %s", status, ReasonOffsetsNonMonotonic)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 6}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonValuesLengthMismatch {
			t.Fatalf("final status=%+v want %s", status, ReasonValuesLengthMismatch)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceHeapCopy)
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonHandleSourceUnsupported {
			t.Fatalf("heap status=%+v want %s", status, ReasonHandleSourceUnsupported)
		}

		req = graphUint32ListRequest(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, mappedresource.SourceMapped)
		if err := req.OffsetsHandle.Release(); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonStaleHandle {
			t.Fatalf("stale status=%+v want %s", status, ReasonStaleHandle)
		}

		req = graphUint32ListRequestWithResourceColumn(t, []uint64{0, 2, 5}, []uint32{1, 2, 3, 4, 5}, "other_tags")
		if _, status := CertifyGraphUint32ListDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("resource column status=%+v want %s", status, ReasonColumnMismatch)
		}
	})

	t.Run("bytes_corrupt_offsets_sections_and_resources", func(t *testing.T) {
		req := graphBytesRequest(t, []uint64{0, 2, 5}, []byte("abcde"), mappedresource.SourceMapped)
		req.Certification.Encoding = typedcolumn.EncodingRawUint32OffsetsList
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonTypeEncodingMismatch {
			t.Fatalf("type status=%+v want %s", status, ReasonTypeEncodingMismatch)
		}

		req = graphBytesRequest(t, []uint64{1, 2, 5}, []byte("abcde"), mappedresource.SourceMapped)
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonOffsetsStartMismatch {
			t.Fatalf("start status=%+v want %s", status, ReasonOffsetsStartMismatch)
		}

		req = graphBytesRequest(t, []uint64{0, 4, 3}, []byte("abcde"), mappedresource.SourceMapped)
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonOffsetsNonMonotonic {
			t.Fatalf("monotonic status=%+v want %s", status, ReasonOffsetsNonMonotonic)
		}

		req = graphBytesRequest(t, []uint64{0, 2, 6}, []byte("abcde"), mappedresource.SourceMapped)
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonValuesLengthMismatch {
			t.Fatalf("final status=%+v want %s", status, ReasonValuesLengthMismatch)
		}

		req = graphBytesRequest(t, []uint64{0, 2, 5}, []byte("abcde"), mappedresource.SourceMapped)
		req.ValuesSection.Length++
		req.Certification.ValuesSection.Length++
		req.Certification.ValuesBytes++
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonPayloadLengthMismatch {
			t.Fatalf("trailing bytes status=%+v want %s", status, ReasonPayloadLengthMismatch)
		}

		req = graphBytesRequest(t, []uint64{0, 2, 5}, []byte("abcde"), mappedresource.SourceMapped)
		req.Expectation.AssetOffset = 4
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonAbsoluteOffsetUnaligned {
			t.Fatalf("misaligned offsets status=%+v want %s", status, ReasonAbsoluteOffsetUnaligned)
		}

		req = graphBytesRequest(t, []uint64{0, 2, 5}, []byte("abcde"), mappedresource.SourceMapped)
		req.Certification.DefaultMaskPresent = true
		req.Certification.DefaultCount = 1
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonNullableWrapper {
			t.Fatalf("default status=%+v want %s", status, ReasonNullableWrapper)
		}

		req = graphBytesRequest(t, []uint64{0, 2, 5}, []byte("abcde"), mappedresource.SourceHeapCopy)
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonHandleSourceUnsupported {
			t.Fatalf("heap status=%+v want %s", status, ReasonHandleSourceUnsupported)
		}

		req = graphBytesRequestWithResourceColumn(t, []uint64{0, 2, 5}, []byte("abcde"), "other_opaque")
		if _, status := CertifyGraphBytesDirectView(req); status.Reason != ReasonColumnMismatch {
			t.Fatalf("resource column status=%+v want %s", status, ReasonColumnMismatch)
		}
	})
}

func BenchmarkGraphDirectViewPreparedAccessor(b *testing.B) {
	req := graphFloat32VectorRequest(b, 8, 16, mappedresource.SourceMapped)
	view, status := CertifyGraphFloat32VectorDirectView(req)
	if !status.Direct() {
		b.Fatalf("CertifyGraphFloat32VectorDirectView=%s", status.String())
	}
	b.Cleanup(func() { _ = view.Close() })
	var sum float32
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := view.Row(i & 7)
		sum += row[i&15]
	}
	graphDirectViewFloatSink += sum
}

func BenchmarkGraphDirectViewCertification(b *testing.B) {
	b.Run("float32_vector", func(b *testing.B) {
		req := graphFloat32VectorRequest(b, 1024, 8, mappedresource.SourceMapped)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, status := CertifyGraphFloat32VectorDirectView(req)
			if !status.Direct() {
				b.Fatalf("CertifyGraphFloat32VectorDirectView=%s", status.String())
			}
			graphDirectViewFloatSink += view.Values[0]
		}
	})

	b.Run("float32", func(b *testing.B) {
		req := graphFloat32Request(b, 1024, mappedresource.SourceMapped)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, status := CertifyGraphFloat32DirectView(req)
			if !status.Direct() {
				b.Fatalf("CertifyGraphFloat32DirectView=%s", status.String())
			}
			graphDirectViewFloatSink += view.Values[0]
		}
	})

	b.Run("int64", func(b *testing.B) {
		req := graphInt64Request(b, 1024, mappedresource.SourceMapped)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, status := CertifyGraphInt64DirectView(req)
			if !status.Direct() {
				b.Fatalf("CertifyGraphInt64DirectView=%s", status.String())
			}
			graphDirectViewIntSink += view.Values[0]
		}
	})

	b.Run("uint32_list", func(b *testing.B) {
		req := graphUint32ListRequest(b, []uint64{0, 2, 2, 5}, []uint32{7, 8, 9, 10, 11}, mappedresource.SourceMapped)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, status := CertifyGraphUint32ListDirectView(req)
			if !status.Direct() {
				b.Fatalf("CertifyGraphUint32ListDirectView=%s", status.String())
			}
			graphDirectViewUintSink += view.Values[0]
		}
	})

	b.Run("bytes", func(b *testing.B) {
		req := graphBytesRequest(b, []uint64{0, 2, 2, 5}, []byte{'a', 0xff, 'x', 0, 'z'}, mappedresource.SourceMapped)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			view, status := CertifyGraphBytesDirectView(req)
			if !status.Direct() {
				b.Fatalf("CertifyGraphBytesDirectView=%s", status.String())
			}
			graphDirectViewByteSink ^= view.Values[0]
		}
	})
}

func graphExpectation(column string, rows int) GraphDirectViewExpectation {
	return GraphDirectViewExpectation{
		ExpectedOwner:  "typed_column_part",
		ActualOwner:    "typed_column_part",
		ExpectedRole:   "graph_search_state",
		ActualRole:     "graph_search_state",
		Column:         column,
		Rows:           rows,
		AssetOffset:    0,
		HasAssetOffset: true,
	}
}

func graphFloat32VectorRequest(t testing.TB, rows, dims int, source mappedresource.Source) GraphFloat32VectorDirectViewRequest {
	return graphFloat32VectorRequestWithResourceColumnAndLength(t, rows, dims, source, "vec", rows*dims*4)
}

func graphFloat32VectorLengthRequest(t testing.TB, rows, dims, length int) GraphFloat32VectorDirectViewRequest {
	return graphFloat32VectorRequestWithResourceColumnAndLength(t, rows, dims, mappedresource.SourceMapped, "vec", length)
}

func graphFloat32VectorRequestWithResourceColumn(t testing.TB, rows, dims int, resourceColumn string) GraphFloat32VectorDirectViewRequest {
	return graphFloat32VectorRequestWithResourceColumnAndLength(t, rows, dims, mappedresource.SourceMapped, resourceColumn, rows*dims*4)
}

func graphFloat32VectorRequestWithResourceColumnAndLength(t testing.TB, rows, dims int, source mappedresource.Source, resourceColumn string, length int) GraphFloat32VectorDirectViewRequest {
	t.Helper()
	cert := testFloat32VectorDirectCert(rows, dims)
	cert.Section.Length = length
	cert.Blocks[0].RawBytes = length
	cert.Blocks[0].StoredBytes = length
	cert.Blocks[0].PayloadLength = length
	section := graphDataSection(cert, "vec", typedcolumn.EncodingRawFloat32Vector)
	raw := alignedBytes(4, length)
	for i := 0; i < length/4; i++ {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(float32(i+1)))
	}
	mgr := mappedresource.NewManager()
	expectedKey := graphKey("vec", section, typedcolumn.EncodingRawFloat32Vector)
	h := graphAcquireHandle(t, mgr, source, raw, graphKey(resourceColumn, section, typedcolumn.EncodingRawFloat32Vector))
	return GraphFloat32VectorDirectViewRequest{Expectation: graphExpectation("vec", rows), Dims: dims, Certification: cert, Section: section, ExpectedKey: expectedKey, Handle: h, Manager: mgr}
}

func graphFloat32Request(t testing.TB, rows int, source mappedresource.Source) GraphFloat32DirectViewRequest {
	t.Helper()
	cert := testFloat32ScalarDirectCert(rows)
	section := graphDataSection(cert, cert.Name, typedcolumn.EncodingRawFloat32)
	raw := alignedBytes(4, rows*4)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(float32(i)+1.5))
	}
	mgr := mappedresource.NewManager()
	expectedKey := graphKey(cert.Name, section, typedcolumn.EncodingRawFloat32)
	h := graphAcquireHandle(t, mgr, source, raw, expectedKey)
	return GraphFloat32DirectViewRequest{Expectation: graphExpectation(cert.Name, rows), Certification: cert, Section: section, ExpectedKey: expectedKey, Handle: h, Manager: mgr}
}

func graphInt64Request(t testing.TB, rows int, source mappedresource.Source) GraphInt64DirectViewRequest {
	t.Helper()
	cert := testInt64DirectCert(rows)
	section := graphDataSection(cert, cert.Name, typedcolumn.EncodingRawInt64)
	raw := alignedBytes(8, rows*8)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(100+i))
	}
	mgr := mappedresource.NewManager()
	expectedKey := graphKey(cert.Name, section, typedcolumn.EncodingRawInt64)
	h := graphAcquireHandle(t, mgr, source, raw, expectedKey)
	return GraphInt64DirectViewRequest{Expectation: graphExpectation(cert.Name, rows), Certification: cert, Section: section, ExpectedKey: expectedKey, Handle: h, Manager: mgr}
}

func graphUint32ListRequest(t testing.TB, offsets []uint64, values []uint32, source mappedresource.Source) GraphUint32ListDirectViewRequest {
	return graphUint32ListRequestWithResourceColumnAndSource(t, offsets, values, "tags", source)
}

func graphUint32ListRequestWithResourceColumn(t testing.TB, offsets []uint64, values []uint32, resourceColumn string) GraphUint32ListDirectViewRequest {
	return graphUint32ListRequestWithResourceColumnAndSource(t, offsets, values, resourceColumn, mappedresource.SourceMapped)
}

func graphUint32ListRequestWithResourceColumnAndSource(t testing.TB, offsets []uint64, values []uint32, resourceColumn string, source mappedresource.Source) GraphUint32ListDirectViewRequest {
	t.Helper()
	rows := len(offsets) - 1
	cert := testUint32ListSpecCert(rows, len(values))
	cert.Name = "tags"
	offsetsSection := graphOffsetsSection(cert, "tags", typedcolumn.EncodingRawUint32OffsetsList)
	valuesSection := graphValuesSection(cert, "tags", typedcolumn.EncodingRawUint32OffsetsList)
	offsetsRaw := encodeUint64Slice(offsets)
	valuesRaw := encodeUint32Slice(values)
	mgr := mappedresource.NewManager()
	expectedOffsetsKey := graphKey("tags", offsetsSection, typedcolumn.EncodingRawUint32OffsetsList)
	expectedValuesKey := graphKey("tags", valuesSection, typedcolumn.EncodingRawUint32OffsetsList)
	offsetsHandle := graphAcquireHandle(t, mgr, source, offsetsRaw, graphKey(resourceColumn, offsetsSection, typedcolumn.EncodingRawUint32OffsetsList))
	valuesHandle := graphAcquireHandle(t, mgr, source, valuesRaw, graphKey(resourceColumn, valuesSection, typedcolumn.EncodingRawUint32OffsetsList))
	return GraphUint32ListDirectViewRequest{Expectation: graphExpectation("tags", rows), Certification: cert, OffsetsSection: offsetsSection, ValuesSection: valuesSection, ExpectedOffsetsKey: expectedOffsetsKey, ExpectedValuesKey: expectedValuesKey, OffsetsHandle: offsetsHandle, ValuesHandle: valuesHandle, Manager: mgr}
}

func graphBytesRequest(t testing.TB, offsets []uint64, values []byte, source mappedresource.Source) GraphBytesDirectViewRequest {
	return graphBytesRequestWithResourceColumnAndSource(t, offsets, values, "opaque", source)
}

func graphBytesRequestWithResourceColumn(t testing.TB, offsets []uint64, values []byte, resourceColumn string) GraphBytesDirectViewRequest {
	return graphBytesRequestWithResourceColumnAndSource(t, offsets, values, resourceColumn, mappedresource.SourceMapped)
}

func graphBytesRequestWithResourceColumnAndSource(t testing.TB, offsets []uint64, values []byte, resourceColumn string, source mappedresource.Source) GraphBytesDirectViewRequest {
	t.Helper()
	rows := len(offsets) - 1
	cert := bytesDirectViewCert(rows, len(offsets)*8, len(values))
	offsetsSection := graphOffsetsSection(cert, "opaque", typedcolumn.EncodingRawBytesOffsets)
	valuesSection := graphValuesSection(cert, "opaque", typedcolumn.EncodingRawBytesOffsets)
	offsetsRaw := encodeUint64Slice(offsets)
	valuesRaw := append([]byte(nil), values...)
	mgr := mappedresource.NewManager()
	expectedOffsetsKey := graphKey("opaque", offsetsSection, typedcolumn.EncodingRawBytesOffsets)
	expectedValuesKey := graphKey("opaque", valuesSection, typedcolumn.EncodingRawBytesOffsets)
	offsetsHandle := graphAcquireHandle(t, mgr, source, offsetsRaw, graphKey(resourceColumn, offsetsSection, typedcolumn.EncodingRawBytesOffsets))
	valuesHandle := graphAcquireHandle(t, mgr, source, valuesRaw, graphKey(resourceColumn, valuesSection, typedcolumn.EncodingRawBytesOffsets))
	return GraphBytesDirectViewRequest{Expectation: graphExpectation("opaque", rows), Certification: cert, OffsetsSection: offsetsSection, ValuesSection: valuesSection, ExpectedOffsetsKey: expectedOffsetsKey, ExpectedValuesKey: expectedValuesKey, OffsetsHandle: offsetsHandle, ValuesHandle: valuesHandle, Manager: mgr}
}

func graphDataSection(cert typedcolumn.ColumnPartLayoutContractColumn, column string, encoding typedcolumn.Encoding) typedcolumn.ColumnPartImageSection {
	return typedcolumn.ColumnPartImageSection{Kind: typedcolumn.ColumnPartImageSectionColumnData, Category: typedcolumn.ColumnPartImageCategoryDeclaredColumns, Column: column, Offset: cert.Section.Offset, Length: cert.Section.Length, Rows: cert.Rows, Encoding: encoding, Compression: typedcolumn.CompressionNone}
}

func graphOffsetsSection(cert typedcolumn.ColumnPartLayoutContractColumn, column string, encoding typedcolumn.Encoding) typedcolumn.ColumnPartImageSection {
	return typedcolumn.ColumnPartImageSection{Kind: typedcolumn.ColumnPartImageSectionColumnOffsets, Category: typedcolumn.ColumnPartImageCategoryDeclaredColumnOffsets, Column: column, Offset: cert.OffsetsSection.Offset, Length: cert.OffsetsSection.Length, Rows: cert.Rows, Encoding: encoding, Compression: typedcolumn.CompressionNone}
}

func graphValuesSection(cert typedcolumn.ColumnPartLayoutContractColumn, column string, encoding typedcolumn.Encoding) typedcolumn.ColumnPartImageSection {
	return typedcolumn.ColumnPartImageSection{Kind: typedcolumn.ColumnPartImageSectionColumnValues, Category: typedcolumn.ColumnPartImageCategoryDeclaredColumnValues, Column: column, Offset: cert.ValuesSection.Offset, Length: cert.ValuesSection.Length, Rows: cert.Rows, Encoding: encoding, Compression: typedcolumn.CompressionNone}
}

func graphKey(column string, section typedcolumn.ColumnPartImageSection, encoding typedcolumn.Encoding) mappedresource.Key {
	return mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  "typeddecode-test",
		Kind:       "tcs1_typed_column_part",
		Generation: 2046,
		PartID:     1,
		FileID:     1,
		Length:     int64(section.Length),
		Encoding:   encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Column:   column,
		},
	}
}

func graphAcquireHandle(t testing.TB, mgr *mappedresource.Manager, source mappedresource.Source, raw []byte, key mappedresource.Key) *mappedresource.Handle {
	t.Helper()
	h, err := mgr.AcquireBytes(key, testScope(), source, raw, mappedresource.AcquireOptions{Reason: "graph direct-view certifier test"})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	t.Cleanup(func() { _ = h.Release() })
	return h
}

func encodeUint64Slice(values []uint64) []byte {
	raw := alignedBytes(8, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(raw[i*8:], value)
	}
	return raw
}

func encodeUint32Slice(values []uint32) []byte {
	raw := alignedBytes(4, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], value)
	}
	return raw
}
