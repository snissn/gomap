package page

const (
	valuePtrCompressedMask uint32 = 0x80000000
	valuePtrGroupedMask    uint32 = 0x40000000

	valuePtrSubIndexMask  uint32 = 0x3c000000
	valuePtrSubIndexShift        = 26
)

// ValuePtrRecordLength returns the record length with internal flags stripped.
func ValuePtrRecordLength(ptr ValuePtr) uint32 {
	return ptr.Length &^ (valuePtrCompressedMask | valuePtrGroupedMask | valuePtrSubIndexMask)
}

// ValuePtrIsCompressed reports whether the pointer references a compressed value.
func ValuePtrIsCompressed(ptr ValuePtr) bool {
	// The value-log record header encodes compression; this bit is currently used
	// to extend the grouped sub-index range.
	if ValuePtrIsGrouped(ptr) {
		return false
	}
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
	idx := uint8((ptr.Length & valuePtrSubIndexMask) >> valuePtrSubIndexShift)
	if ptr.Length&valuePtrCompressedMask != 0 {
		idx |= 0x10
	}
	return idx
}

// ValuePtrMarkCompressed sets the compression flag on a record length.
func ValuePtrMarkCompressed(length uint32) uint32 {
	return length | valuePtrCompressedMask
}

// ValuePtrMarkGrouped sets the grouped flag and sub-index (0-31) on a record length.
func ValuePtrMarkGrouped(length uint32, subIndex uint8) uint32 {
	idx := uint32(subIndex & 0xf)
	length &^= (valuePtrSubIndexMask | valuePtrCompressedMask)
	out := length | valuePtrGroupedMask | (idx << valuePtrSubIndexShift)
	if subIndex&0x10 != 0 {
		out |= valuePtrCompressedMask
	}
	return out
}
