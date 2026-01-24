package page

const (
	valuePtrCompressedMask uint32 = 0x80000000
	valuePtrGroupedMask    uint32 = 0x40000000

	valuePtrSubIndexMask  uint32 = 0x1c000000
	valuePtrSubIndexShift        = 26
)

// ValuePtrRecordLength returns the record length with internal flags stripped.
func ValuePtrRecordLength(ptr ValuePtr) uint32 {
	return ptr.Length &^ (valuePtrCompressedMask | valuePtrGroupedMask | valuePtrSubIndexMask)
}

// ValuePtrIsCompressed reports whether the pointer references a compressed value.
func ValuePtrIsCompressed(ptr ValuePtr) bool {
	return ptr.Length&valuePtrCompressedMask != 0
}

// ValuePtrIsGrouped reports whether the pointer references a grouped record.
func ValuePtrIsGrouped(ptr ValuePtr) bool {
	return ptr.Length&valuePtrGroupedMask != 0
}

// ValuePtrSubIndex returns the row index within a grouped record.
func ValuePtrSubIndex(ptr ValuePtr) uint8 {
	if !ValuePtrIsGrouped(ptr) {
		return 0
	}
	return uint8((ptr.Length & valuePtrSubIndexMask) >> valuePtrSubIndexShift)
}

// ValuePtrMarkCompressed sets the compression flag on a record length.
func ValuePtrMarkCompressed(length uint32) uint32 {
	return length | valuePtrCompressedMask
}

// ValuePtrMarkGrouped sets the grouped flag and sub-index (0-7) on a record length.
func ValuePtrMarkGrouped(length uint32, subIndex uint8) uint32 {
	idx := uint32(subIndex & 0x7)
	length &^= valuePtrSubIndexMask
	return length | valuePtrGroupedMask | (idx << valuePtrSubIndexShift)
}
