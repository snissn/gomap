package page

const (
	valuePtrCompressedMask uint32 = 0x80000000
	valuePtrGroupedMask    uint32 = 0x40000000

	valuePtrSubIndexMask  uint32 = 0x3c000000
	valuePtrSubIndexShift        = 26

	// Extended grouped sub-index bits (bits 25..24). These are only interpreted
	// when the grouped flag is set.
	valuePtrSubIndexHiMask  uint32 = 0x03000000
	valuePtrSubIndexHiShift        = 24
)

// ValuePtrGroupedMaxRecordLen is the maximum record length (excluding CRC) that
// can be encoded in a grouped ValuePtr length hint.
//
// Grouped pointers embed a sub-index in the Length field, leaving 24 bits for a
// best-effort record length hint. Larger records must set the hint to 0 and
// rely on the value-log record header's ValueLen instead.
const ValuePtrGroupedMaxRecordLen uint32 = 0x00ffffff

// ValuePtrRecordLength returns the record length with internal flags stripped.
func ValuePtrRecordLength(ptr ValuePtr) uint32 {
	mask := valuePtrCompressedMask | valuePtrGroupedMask | valuePtrSubIndexMask
	if ValuePtrIsGrouped(ptr) {
		mask |= valuePtrSubIndexHiMask
	}
	return ptr.Length &^ mask
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
	idx |= uint8((ptr.Length&valuePtrSubIndexHiMask)>>valuePtrSubIndexHiShift) << 5
	if ptr.Length&valuePtrCompressedMask != 0 {
		idx |= 0x10
	}
	return idx
}

// ValuePtrMarkCompressed sets the compression flag on a record length.
func ValuePtrMarkCompressed(length uint32) uint32 {
	return length | valuePtrCompressedMask
}

// ValuePtrMarkGrouped sets the grouped flag and sub-index (0-127) on a record length.
func ValuePtrMarkGrouped(length uint32, subIndex uint8) uint32 {
	idx := uint32(subIndex & 0x0f)
	hi := uint32((subIndex >> 5) & 0x03)
	length &^= (valuePtrSubIndexMask | valuePtrSubIndexHiMask | valuePtrCompressedMask)
	out := length | valuePtrGroupedMask | (idx << valuePtrSubIndexShift) | (hi << valuePtrSubIndexHiShift)
	if subIndex&0x10 != 0 {
		out |= valuePtrCompressedMask
	}
	return out
}
