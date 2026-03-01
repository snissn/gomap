package page

import "fmt"

// LeafRef encodes a value-log pointer for a B+Tree leaf page into a uint64 so it
// can be stored in InternalEntry.ChildPageID without changing the internal page
// wire format.
//
// Layout (little-endian on disk via u64 encoding, but treated as numeric here):
//   high 32 bits: ValuePtr.FileID (must be a value-log file id)
//   low  32 bits: ValuePtr.Offset (must fit in u32)
//
// LeafRef pointers intentionally omit ValuePtr.Length/sub-index. Leaf pages are
// stored as single-record grouped frames (K=1), so the reader can reconstruct a
// stable ValuePtr with grouped flag and zero record-length hint.

var leafRefPtrLength = ValuePtrMarkGrouped(0, 0)

// EncodeLeafRef encodes ptr as a LeafRef id.
func EncodeLeafRef(ptr ValuePtr) (uint64, error) {
	if !IsValueLogFileID(ptr.FileID) {
		return 0, fmt.Errorf("page: leafref requires value-log file id, got %d", ptr.FileID)
	}
	if ptr.Offset > uint64(^uint32(0)) {
		return 0, fmt.Errorf("page: leafref offset overflows u32: %d", ptr.Offset)
	}
	return (uint64(ptr.FileID) << 32) | uint64(uint32(ptr.Offset)), nil
}

// DecodeLeafRef decodes a LeafRef id into a ValuePtr.
func DecodeLeafRef(id uint64) (ValuePtr, bool) {
	fileID := uint32(id >> 32)
	if !IsValueLogFileID(fileID) {
		return ValuePtr{}, false
	}
	return ValuePtr{
		Offset: uint64(uint32(id)),
		Length: leafRefPtrLength,
		FileID: fileID,
	}, true
}

