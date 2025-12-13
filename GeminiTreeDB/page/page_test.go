package page

import (
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

func TestStructAlignment(t *testing.T) {
	// Verify structs are 16 bytes and naturally aligned (no padding needed for these specific definitions)
	if size := unsafe.Sizeof(PageHeader{}); size != 16 {
		t.Errorf("PageHeader size is %d, expected 16", size)
	}
	if size := unsafe.Sizeof(ValuePtr{}); size != 16 {
		t.Errorf("ValuePtr size is %d, expected 16", size)
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

func TestUnsafeCastHeader(t *testing.T) {
	// Note: This test assumes LittleEndian machine.
	// If running on BigEndian, this test might fail or require adjustment if UnsafeCastHeader is used.
	
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
