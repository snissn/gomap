package page

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unsafe"
)

var ErrInvalidPageType = errors.New("invalid page type")

const (
	// PageSize is the fixed size of a page in bytes.
	PageSize = 4096

	// DefaultInlineThreshold determines when a value is stored in the slab.
	DefaultInlineThreshold = 256

	// PageHeaderSize is the size of the PageHeader struct.
	PageHeaderSize = 16

	// ValuePtrSize is the size of the ValuePtr struct.
	ValuePtrSize = 16
)

// PageType represents the type of page (Meta, Freelist, Internal, Leaf).
type PageType uint16

const (
	PageTypeMeta     PageType = 0x01
	PageTypeFreelist PageType = 0x02
	PageTypeInternal PageType = 0x03
	PageTypeLeaf     PageType = 0x04
)

// PageHeader represents the 16-byte header of a page.
// | PageID   (8 bytes)       |
// | Checksum (4 bytes)       |
// | Flags    (2 bytes)       |
// | Count    (2 bytes)       |
type PageHeader struct {
	PageID   uint64
	Checksum uint32
	Flags    uint16
	Count    uint16
}

// ValuePtr points to data stored in the Slabs.
// | Offset   (8 bytes)      |  // 8-byte aligned
// | Length   (4 bytes)      |
// | FileID   (4 bytes)      |
type ValuePtr struct {
	Offset uint64
	Length uint32
	FileID uint32
}

// CRC32C Table using Castagnoli polynomial.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC32C checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// CalculateChecksum computes the checksum of the page data,
// treating the checksum field (bytes 8-12) as zero.
func CalculateChecksum(data []byte) uint32 {
	if len(data) < PageHeaderSize {
		return 0
	}
	// CRC over 0..8
	sum := crc32.Checksum(data[0:8], crcTable)
	// CRC over zeroed checksum field (4 bytes)
	var zero [4]byte
	sum = crc32.Update(sum, crcTable, zero[:])
	// CRC over rest (12..)
	sum = crc32.Update(sum, crcTable, data[12:])
	return sum
}

// VerifyChecksumNonMutating verifies that the page checksum matches the data,
// assuming the checksum field (bytes 8-12) is zero for the calculation.
// It avoids modifying the underlying buffer.
func VerifyChecksumNonMutating(data []byte) bool {
	if len(data) < PageHeaderSize {
		return false
	}
	stored := binary.LittleEndian.Uint32(data[8:12])
	computed := CalculateChecksum(data)
	return stored == computed
}

// EncodeHeader encodes the PageHeader into the provided buffer.
// The buffer must be at least PageHeaderSize bytes.
func (h *PageHeader) Encode(buf []byte) {
	_ = buf[PageHeaderSize-1] // Bounds check elimination
	binary.LittleEndian.PutUint64(buf[0:8], h.PageID)
	binary.LittleEndian.PutUint32(buf[8:12], h.Checksum)
	binary.LittleEndian.PutUint16(buf[12:14], h.Flags)
	binary.LittleEndian.PutUint16(buf[14:16], h.Count)
}

// DecodeHeader decodes the PageHeader from the provided buffer.
func DecodeHeader(buf []byte) PageHeader {
	_ = buf[PageHeaderSize-1] // Bounds check elimination
	return PageHeader{
		PageID:   binary.LittleEndian.Uint64(buf[0:8]),
		Checksum: binary.LittleEndian.Uint32(buf[8:12]),
		Flags:    binary.LittleEndian.Uint16(buf[12:14]),
		Count:    binary.LittleEndian.Uint16(buf[14:16]),
	}
}

// EncodeValuePtr encodes the ValuePtr into the provided buffer.
// The buffer must be at least ValuePtrSize bytes.
func (v *ValuePtr) Encode(buf []byte) {
	_ = buf[ValuePtrSize-1] // Bounds check elimination
	binary.LittleEndian.PutUint64(buf[0:8], v.Offset)
	binary.LittleEndian.PutUint32(buf[8:12], v.Length)
	binary.LittleEndian.PutUint32(buf[12:16], v.FileID)
}

// DecodeValuePtr decodes the ValuePtr from the provided buffer.
func DecodeValuePtr(buf []byte) ValuePtr {
	_ = buf[ValuePtrSize-1] // Bounds check elimination
	return ValuePtr{
		Offset: binary.LittleEndian.Uint64(buf[0:8]),
		Length: binary.LittleEndian.Uint32(buf[8:12]),
		FileID: binary.LittleEndian.Uint32(buf[12:16]),
	}
}

// CastHeader casts the beginning of a byte slice to a PageHeader struct pointer.
// Use with caution: implies unsafe access and assumes strict layout matching.
// This is an alternative to Encode/Decode for zero-copy access if needed,
// but requires the struct memory layout to match the wire format (packing).
// Go structs usually have padding/alignment.
// PageHeader: 8 + 4 + 2 + 2 = 16 bytes. Naturally aligned.
// ValuePtr: 8 + 4 + 2 + 2 = 16 bytes. Naturally aligned.
// So, we can use unsafe casting if endianness matches the machine's endianness.
// However, the spec requires LittleEndian. If the host is BigEndian, this fails.
// For now, we stick to Encode/Decode or binary.LittleEndian read/write for safety across archs
// unless zero-copy is strictly required and we add endianness checks.
// The spec mentions: "Implementation Note: To maximize throughput... use unsafe.Pointer casting".
// This implies the on-disk format should match the in-memory struct layout,
// AND the machine is likely LittleEndian (standard for Cosmos/x86/ARM).
// Let's implement a UnsafeCastHeader for when we have the mmap slice.
func UnsafeCastHeader(data []byte) *PageHeader {
	return (*PageHeader)(unsafe.Pointer(&data[0]))
}
