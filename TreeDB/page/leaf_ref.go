package page

import "fmt"

// LeafRef encodes a value-log pointer for a B+Tree leaf page into a uint64 so it
// can be stored in InternalEntry.ChildPageID without changing the internal page
// wire format.
//
// Layout (little-endian on disk via u64 encoding, but treated as numeric here):
//   high 32 bits: ValuePtr.FileID (must be a value-log file id)
//   low  32 bits:
//     bits 31..29: grouped sub-index (0..7)
//     bits 28..0 : ValuePtr.Offset
//
// LeafRef pointers omit the record-length hint, but preserve the grouped
// sub-index so outer leaf pages can be written in small grouped frames.

const (
	leafRefSubIndexBits = 3
	leafRefOffsetBits   = 32 - leafRefSubIndexBits
	leafRefOffsetMask   = (uint64(1) << leafRefOffsetBits) - 1
	leafRefSubIndexMask = (uint64(1) << leafRefSubIndexBits) - 1
)

// LeafRefMaxOffset is the maximum value-log offset representable in a LeafRef.
const LeafRefMaxOffset uint64 = leafRefOffsetMask

// EncodeLeafRef encodes ptr as a LeafRef id.
func EncodeLeafRef(ptr ValuePtr) (uint64, error) {
	if !IsValueLogFileID(ptr.FileID) {
		return 0, fmt.Errorf("page: leafref requires value-log file id, got %d", ptr.FileID)
	}
	if ptr.Offset > LeafRefMaxOffset {
		return 0, fmt.Errorf("page: leafref offset overflows %d-bit field: %d", leafRefOffsetBits, ptr.Offset)
	}
	subIndex := uint64(ValuePtrSubIndex(ptr))
	if subIndex > leafRefSubIndexMask {
		return 0, fmt.Errorf("page: leafref sub-index overflows %d bits: %d", leafRefSubIndexBits, subIndex)
	}
	low := (subIndex << leafRefOffsetBits) | (ptr.Offset & leafRefOffsetMask)
	return (uint64(ptr.FileID) << 32) | low, nil
}

// DecodeLeafRef decodes a LeafRef id into a ValuePtr.
func DecodeLeafRef(id uint64) (ValuePtr, bool) {
	fileID := uint32(id >> 32)
	if !IsValueLogFileID(fileID) {
		return ValuePtr{}, false
	}
	low := uint64(uint32(id))
	subIndex := uint8((low >> leafRefOffsetBits) & leafRefSubIndexMask)
	return ValuePtr{
		Offset: low & leafRefOffsetMask,
		Length: ValuePtrMarkGrouped(0, subIndex),
		FileID: fileID,
	}, true
}
