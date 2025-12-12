package page

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func TestValuePtrSizeAndEncoding(t *testing.T) {
	if got := unsafe.Sizeof(ValuePtr{}); got != ValuePtrSize {
		t.Fatalf("ValuePtr size: got %d want %d", got, ValuePtrSize)
	}

	p := ValuePtr{
		Offset: 0x1122334455667788,
		Length: 0x99aabbcc,
		FileID: 0xddeeff00,
	}
	var buf [ValuePtrSize]byte
	if err := p.EncodeLE(buf[:]); err != nil {
		t.Fatalf("EncodeLE error: %v", err)
	}

	if off := binary.LittleEndian.Uint64(buf[0:8]); off != p.Offset {
		t.Fatalf("Offset encoding mismatch: got %x want %x", off, p.Offset)
	}
	if ln := binary.LittleEndian.Uint32(buf[8:12]); ln != p.Length {
		t.Fatalf("Length encoding mismatch: got %x want %x", ln, p.Length)
	}
	if id := binary.LittleEndian.Uint32(buf[12:16]); id != p.FileID {
		t.Fatalf("FileID encoding mismatch: got %x want %x", id, p.FileID)
	}

	p2, err := DecodeValuePtrLE(buf[:])
	if err != nil {
		t.Fatalf("DecodeValuePtrLE error: %v", err)
	}
	if p2 != p {
		t.Fatalf("round-trip mismatch: got %+v want %+v", p2, p)
	}
}

