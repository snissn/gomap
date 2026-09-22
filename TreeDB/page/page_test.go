package page

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func TestPageHeaderEncoding(t *testing.T) {
	original := PageHeader{
		PageID:   123456789,
		Checksum: 0xDEADBEEF,
		Flags:    uint16(PageTypeLeaf),
		Count:    42,
	}

	buf := make([]byte, PageHeaderSize)
	original.Encode(buf)

	decoded := DecodeHeader(buf)

	if decoded != original {
		t.Errorf("Decoded header mismatch. Got %+v, want %+v", decoded, original)
	}
}

func TestValuePtrEncoding(t *testing.T) {
	original := ValuePtr{
		Offset: 9876543210,
		Length: 1024,
		FileID: 5,
	}

	buf := make([]byte, ValuePtrSize)
	original.Encode(buf)

	decoded := DecodeValuePtr(buf)

	if decoded != original {
		t.Errorf("Decoded ValuePtr mismatch. Got %+v, want %+v", decoded, original)
	}
}

func TestValuePtrGroupedSubIndexUsesHighBit(t *testing.T) {
	for _, subIndex := range []uint8{0, 15, 16, 31, 32, 64, 127, 128, 254, 255} {
		ptr := ValuePtr{
			Offset: 1024,
			Length: ValuePtrMarkGrouped(ValuePtrGroupedMaxRecordLen, subIndex),
			FileID: ValueLogFileID(1),
		}
		if !ValuePtrIsGrouped(ptr) {
			t.Fatalf("subIndex=%d: pointer is not grouped", subIndex)
		}
		if ValuePtrIsCompressed(ptr) {
			t.Fatalf("subIndex=%d: grouped pointer must not report compressed", subIndex)
		}
		if got := ValuePtrSubIndex(ptr); got != subIndex {
			t.Fatalf("subIndex=%d: decoded sub-index=%d", subIndex, got)
		}
		if got := ValuePtrRecordLength(ptr); got != ValuePtrGroupedMaxRecordLen {
			t.Fatalf("subIndex=%d: record length=%d want %d", subIndex, got, ValuePtrGroupedMaxRecordLen)
		}
	}
}

func TestLeafLogPtrFromValuePtrPreservesGroupedFlag(t *testing.T) {
	original := ValuePtr{
		Offset: 128,
		Length: ValuePtrMarkGrouped(1234, 5),
		FileID: ValueLogFileID(7),
	}

	leafPtr, err := LeafLogPtrFromValuePtr(original)
	if err != nil {
		t.Fatalf("LeafLogPtrFromValuePtr: %v", err)
	}
	if !leafPtr.IsGrouped() {
		t.Fatalf("leaf ptr did not preserve grouped flag: %+v", leafPtr)
	}
	if got := leafPtr.RecordLength(); got != 1234 {
		t.Fatalf("RecordLength=%d want 1234", got)
	}
	if leafPtr.SubIndex != 5 {
		t.Fatalf("SubIndex=%d want 5", leafPtr.SubIndex)
	}
	if got := leafPtr.ValuePtr(); got != original {
		t.Fatalf("ValuePtr round trip=%+v want %+v", got, original)
	}
}

func TestLeafLogPtrValuePtrLeavesUngroupedPointersUngrouped(t *testing.T) {
	leafPtr := LeafLogPtr{FileID: 7, Offset: 128, RecordLengthHint: 1234, SubIndex: 5}
	got := leafPtr.ValuePtr()
	if ValuePtrIsGrouped(got) {
		t.Fatalf("ValuePtr unexpectedly grouped: %+v", got)
	}
	if gotLen := ValuePtrRecordLength(got); gotLen != 1234 {
		t.Fatalf("record length=%d want 1234", gotLen)
	}
}

func TestPackedValuePtrEncoding(t *testing.T) {
	original := ValuePtr{
		Offset: 123456789,
		Length: 1024,
		FileID: 5,
	}

	buf := make([]byte, PackedValuePtrSize)
	EncodePackedValuePtr(buf, original)

	decoded := DecodePackedValuePtr(buf)
	if decoded != original {
		t.Errorf("Decoded packed ValuePtr mismatch. Got %+v, want %+v", decoded, original)
	}
}

func TestEncodePackedValuePtr_OffsetOverflowPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for offset overflow")
		}
	}()
	buf := make([]byte, PackedValuePtrSize)
	EncodePackedValuePtr(buf, ValuePtr{Offset: uint64(^uint32(0)) + 1})
}

func TestStructAlignment(t *testing.T) {
	// Verify structs are 16 bytes and have the expected in-memory layout for the
	// on-disk wire format (used by fast-path Encode/Decode on little-endian).
	if size := unsafe.Sizeof(PageHeader{}); size != 16 {
		t.Errorf("PageHeader size is %d, expected 16", size)
	}
	if off := unsafe.Offsetof(PageHeader{}.PageID); off != 0 {
		t.Errorf("PageHeader.PageID offset is %d, expected 0", off)
	}
	if off := unsafe.Offsetof(PageHeader{}.Checksum); off != 8 {
		t.Errorf("PageHeader.Checksum offset is %d, expected 8", off)
	}
	if off := unsafe.Offsetof(PageHeader{}.Flags); off != 12 {
		t.Errorf("PageHeader.Flags offset is %d, expected 12", off)
	}
	if off := unsafe.Offsetof(PageHeader{}.Count); off != 14 {
		t.Errorf("PageHeader.Count offset is %d, expected 14", off)
	}

	if size := unsafe.Sizeof(ValuePtr{}); size != 16 {
		t.Errorf("ValuePtr size is %d, expected 16", size)
	}
	if off := unsafe.Offsetof(ValuePtr{}.Offset); off != 0 {
		t.Errorf("ValuePtr.Offset offset is %d, expected 0", off)
	}
	if off := unsafe.Offsetof(ValuePtr{}.Length); off != 8 {
		t.Errorf("ValuePtr.Length offset is %d, expected 8", off)
	}
	if off := unsafe.Offsetof(ValuePtr{}.FileID); off != 12 {
		t.Errorf("ValuePtr.FileID offset is %d, expected 12", off)
	}
}

func TestCRC32IEEE(t *testing.T) {
	data := []byte("hello world")
	// Calculated using a standard CRC-32/IEEE calculator.
	expected := uint32(0x0d4a1185)

	sum := Checksum(data)
	if sum != expected {
		t.Errorf("Checksum mismatch. Got 0x%x, want 0x%x", sum, expected)
	}
}

func TestUpdateChecksum(t *testing.T) {
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte((i * 17) & 0xff)
	}
	binary.LittleEndian.PutUint32(data[8:12], 0xdeadbeef)

	want := CalculateChecksum(data)
	got := UpdateChecksum(data)
	if got != want {
		t.Fatalf("UpdateChecksum returned 0x%x, want 0x%x", got, want)
	}
	if binary.LittleEndian.Uint32(data[8:12]) != want {
		t.Fatalf("header checksum not updated: got 0x%x want 0x%x", binary.LittleEndian.Uint32(data[8:12]), want)
	}
	if !VerifyChecksumNonMutating(data) {
		t.Fatalf("checksum should verify after UpdateChecksum")
	}
}

func TestCalculateChecksumWithZeroGap(t *testing.T) {
	canonical := make([]byte, PageSize)
	for i := range canonical {
		canonical[i] = byte((i*31 + 7) & 0xff)
	}
	prefixLen := 96
	suffixLen := 384
	gapLen := PageSize - prefixLen - suffixLen
	for i := prefixLen; i < PageSize-suffixLen; i++ {
		canonical[i] = 0
	}
	binary.LittleEndian.PutUint32(canonical[8:12], 0xdeadbeef)

	want := CalculateChecksum(canonical)
	got := CalculateChecksumWithZeroGap(canonical[:prefixLen], gapLen, canonical[PageSize-suffixLen:])
	if got != want {
		t.Fatalf("CalculateChecksumWithZeroGap=0x%x want 0x%x", got, want)
	}
}

func TestCalculateChecksumWithZeroGapPanicsOnInvalidInput(t *testing.T) {
	assertPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s did not panic", name)
			}
		}()
		fn()
	}

	assertPanic("short prefix", func() {
		_ = CalculateChecksumWithZeroGap(make([]byte, PageHeaderSize-1), 0, nil)
	})
	assertPanic("negative gap", func() {
		_ = CalculateChecksumWithZeroGap(make([]byte, PageHeaderSize), -1, nil)
	})
}

func TestUnsafeCastHeader(t *testing.T) {
	// Note: This test assumes LittleEndian machine.
	// If running on BigEndian, this test might fail or require adjustment if UnsafeCastHeader is used.
	if !nativeLittleEndian {
		t.Skip("UnsafeCastHeader requires little-endian host")
	}

	buf := make([]byte, PageSize)
	// Manually write LittleEndian values
	// PageID = 1
	buf[0] = 1
	// Checksum = 2
	buf[8] = 2
	// Flags = 3
	buf[12] = 3
	// Count = 4
	buf[14] = 4

	h := UnsafeCastHeader(buf)

	// On LittleEndian:
	if h.PageID != 1 {
		t.Errorf("UnsafeCast PageID: got %d, want 1", h.PageID)
	}
	if h.Checksum != 2 {
		t.Errorf("UnsafeCast Checksum: got %d, want 2", h.Checksum)
	}
	if h.Flags != 3 {
		t.Errorf("UnsafeCast Flags: got %d, want 3", h.Flags)
	}
	if h.Count != 4 {
		t.Errorf("UnsafeCast Count: got %d, want 4", h.Count)
	}
}
