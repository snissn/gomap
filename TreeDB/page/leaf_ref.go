package page

import "fmt"

// LeafRef encodes a leaf-log pointer into a uint64 so it can be stored in
// InternalEntry.ChildPageID without changing the internal page wire format.
//
// Layout:
//   bit 63:      leaf-ref marker
//   bits 32..62: LeafLogPtr.FileID (raw leaf-log file id; must fit in 31 bits)
//   bits  0..31: LeafLogPtr.Offset (must fit in u32)
//
// The marker keeps LeafRef ids disjoint from normal pager page ids while
// keeping file identity typed at the API level instead of smuggling class bits
// through ValuePtr.FileID.

const leafRefMarker uint64 = uint64(1) << 63

var leafRefPtrLength = ValuePtrMarkGrouped(0, 0)

func EncodeLeafRef(ptr LeafLogPtr) (uint64, error) {
	if ptr.FileID&(1<<31) != 0 {
		return 0, fmt.Errorf("page: leafref file id overflows 31 bits: %d", ptr.FileID)
	}
	if ptr.Offset > uint64(^uint32(0)) {
		return 0, fmt.Errorf("page: leafref offset overflows u32: %d", ptr.Offset)
	}
	return leafRefMarker | (uint64(ptr.FileID) << 32) | uint64(uint32(ptr.Offset)), nil
}

func DecodeLeafRef(id uint64) (LeafLogPtr, bool) {
	if id&leafRefMarker == 0 {
		return LeafLogPtr{}, false
	}
	return LeafLogPtr{
		Offset: uint64(uint32(id)),
		FileID: uint32((id >> 32) & ((uint64(1) << 31) - 1)),
	}, true
}
