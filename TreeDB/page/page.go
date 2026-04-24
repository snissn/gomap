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

	// DefaultInlineThreshold determines when a value is stored in the value log.
	DefaultInlineThreshold = 512

	// PageHeaderSize is the size of the PageHeader struct.
	PageHeaderSize = 16

	// PageChecksumOffset is the byte offset of PageHeader.Checksum within a
	// serialized page header.
	PageChecksumOffset = 8

	// PageChecksumSize is the serialized size of PageHeader.Checksum.
	PageChecksumSize = 4

	// ValuePtrSize is the size of the ValuePtr struct.
	ValuePtrSize = 16

	// PackedValuePtrSize is the on-disk size of a packed ValuePtr encoding.
	//
	// Layout: Offset32 (u32 LE) | Length (u32 LE) | FileID (u32 LE).
	// This is used by experimental leaf encodings to reduce pointer payload.
	PackedValuePtrSize = 12
)

var nativeLittleEndian = func() bool {
	var v uint16 = 1
	return *(*byte)(unsafe.Pointer(&v)) == 1
}()

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

// ValuePtr points to data stored in the value log.
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
var checksumZeroField = [PageChecksumSize]byte{}

// Checksum returns the CRC32C checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crcTable)
}

// CalculateChecksum computes the checksum of the page data, treating the page
// checksum field bytes as zero.
func CalculateChecksum(data []byte) uint32 {
	if len(data) < PageHeaderSize {
		return 0
	}
	checksumEnd := PageChecksumOffset + PageChecksumSize
	sum := crc32.Update(0, crcTable, data[:PageChecksumOffset])
	sum = crc32.Update(sum, crcTable, checksumZeroField[:])
	sum = crc32.Update(sum, crcTable, data[checksumEnd:])
	return sum
}

// UpdateChecksum computes CRC32C for the page while treating the page checksum
// field bytes as zero, then writes the computed checksum back into the
// page header.
// It mutates data in-place and returns the computed checksum.
func UpdateChecksum(data []byte) uint32 {
	if len(data) < PageHeaderSize {
		return 0
	}
	clear(data[PageChecksumOffset : PageChecksumOffset+PageChecksumSize])
	sum := crc32.Checksum(data, crcTable)
	binary.LittleEndian.PutUint32(data[PageChecksumOffset:PageChecksumOffset+PageChecksumSize], sum)
	return sum
}

// VerifyChecksumNonMutating verifies that the page checksum matches the data,
// assuming the page checksum field bytes are zero for the calculation.
// It avoids modifying the underlying buffer.
func VerifyChecksumNonMutating(data []byte) bool {
	if len(data) < PageHeaderSize {
		return false
	}
	stored := binary.LittleEndian.Uint32(data[PageChecksumOffset : PageChecksumOffset+PageChecksumSize])
	computed := CalculateChecksum(data)
	return stored == computed
}

// EncodeHeader encodes the PageHeader into the provided buffer.
// The buffer must be at least PageHeaderSize bytes.
func (h *PageHeader) Encode(buf []byte) {
	_ = buf[PageHeaderSize-1] // Bounds check elimination
	if nativeLittleEndian {
		*(*[PageHeaderSize]byte)(unsafe.Pointer(&buf[0])) = *(*[PageHeaderSize]byte)(unsafe.Pointer(h))
		return
	}
	binary.LittleEndian.PutUint64(buf[0:8], h.PageID)
	binary.LittleEndian.PutUint32(buf[PageChecksumOffset:PageChecksumOffset+PageChecksumSize], h.Checksum)
	binary.LittleEndian.PutUint16(buf[12:14], h.Flags)
	binary.LittleEndian.PutUint16(buf[14:16], h.Count)
}

// DecodeHeader decodes the PageHeader from the provided buffer.
func DecodeHeader(buf []byte) PageHeader {
	_ = buf[PageHeaderSize-1] // Bounds check elimination
	if nativeLittleEndian {
		var h PageHeader
		*(*[PageHeaderSize]byte)(unsafe.Pointer(&h)) = *(*[PageHeaderSize]byte)(unsafe.Pointer(&buf[0]))
		return h
	}
	return PageHeader{
		PageID:   binary.LittleEndian.Uint64(buf[0:8]),
		Checksum: binary.LittleEndian.Uint32(buf[PageChecksumOffset : PageChecksumOffset+PageChecksumSize]),
		Flags:    binary.LittleEndian.Uint16(buf[12:14]),
		Count:    binary.LittleEndian.Uint16(buf[14:16]),
	}
}

// EncodeValuePtr encodes the ValuePtr into the provided buffer.
// The buffer must be at least ValuePtrSize bytes.
func (v *ValuePtr) Encode(buf []byte) {
	_ = buf[ValuePtrSize-1] // Bounds check elimination
	if nativeLittleEndian {
		*(*[ValuePtrSize]byte)(unsafe.Pointer(&buf[0])) = *(*[ValuePtrSize]byte)(unsafe.Pointer(v))
		return
	}
	binary.LittleEndian.PutUint64(buf[0:8], v.Offset)
	binary.LittleEndian.PutUint32(buf[8:12], v.Length)
	binary.LittleEndian.PutUint32(buf[12:16], v.FileID)
}

// DecodeValuePtr decodes the ValuePtr from the provided buffer.
func DecodeValuePtr(buf []byte) ValuePtr {
	_ = buf[ValuePtrSize-1] // Bounds check elimination
	if nativeLittleEndian {
		var v ValuePtr
		*(*[ValuePtrSize]byte)(unsafe.Pointer(&v)) = *(*[ValuePtrSize]byte)(unsafe.Pointer(&buf[0]))
		return v
	}
	return ValuePtr{
		Offset: binary.LittleEndian.Uint64(buf[0:8]),
		Length: binary.LittleEndian.Uint32(buf[8:12]),
		FileID: binary.LittleEndian.Uint32(buf[12:16]),
	}
}

// EncodePackedValuePtr encodes ptr into dst using the packed 12-byte encoding.
// dst must be at least PackedValuePtrSize bytes.
//
// Packed pointers store Offset as u32. Callers must ensure ptr.Offset fits.
func EncodePackedValuePtr(dst []byte, ptr ValuePtr) {
	_ = dst[PackedValuePtrSize-1] // Bounds check elimination
	if ptr.Offset > uint64(^uint32(0)) {
		panic("page: packed ValuePtr offset overflows u32")
	}
	binary.LittleEndian.PutUint32(dst[0:4], uint32(ptr.Offset))
	binary.LittleEndian.PutUint32(dst[4:8], ptr.Length)
	binary.LittleEndian.PutUint32(dst[8:12], ptr.FileID)
}

// DecodePackedValuePtr decodes a packed 12-byte ValuePtr from src.
// src must be at least PackedValuePtrSize bytes.
func DecodePackedValuePtr(src []byte) ValuePtr {
	_ = src[PackedValuePtrSize-1] // Bounds check elimination
	return ValuePtr{
		Offset: uint64(binary.LittleEndian.Uint32(src[0:4])),
		Length: binary.LittleEndian.Uint32(src[4:8]),
		FileID: binary.LittleEndian.Uint32(src[8:12]),
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
	if !nativeLittleEndian {
		panic("UnsafeCastHeader requires little-endian host")
	}
	return (*PageHeader)(unsafe.Pointer(&data[0]))
}
