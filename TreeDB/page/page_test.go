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

func TestCRC32C(t *testing.T) {
	data := []byte("hello world")
	// Calculated using a standard CRC32C (Castagnoli) calculator
	expected := uint32(0xc99465aa)

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
