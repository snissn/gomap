package page

const (
	valuePtrCompressedMask     uint32 = 0x80000000
	valuePtrFullCompressedMask uint32 = 0x40000000
	valuePtrOmittedKeyMask     uint32 = 0x20000000
)

// ValuePtrRecordLength returns the record length with internal flags stripped.
func ValuePtrRecordLength(ptr ValuePtr) uint32 {
	return ptr.Length &^ (valuePtrCompressedMask | valuePtrFullCompressedMask | valuePtrOmittedKeyMask)
}

// ValuePtrIsCompressed reports whether the pointer references a compressed value.
func ValuePtrIsCompressed(ptr ValuePtr) bool {
	return ptr.Length&valuePtrCompressedMask != 0
}

// ValuePtrIsFullCompressed reports whether the pointer references a compressed (key, value) pair.
func ValuePtrIsFullCompressed(ptr ValuePtr) bool {
	return ptr.Length&valuePtrFullCompressedMask != 0
}

// ValuePtrIsOmittedKey reports whether the pointer references a record without a key in the slab.
func ValuePtrIsOmittedKey(ptr ValuePtr) bool {
	return ptr.Length&valuePtrOmittedKeyMask != 0
}

// ValuePtrMarkCompressed sets the compression flag on a record length.
func ValuePtrMarkCompressed(length uint32) uint32 {
	return length | valuePtrCompressedMask
}

// ValuePtrMarkFullCompressed sets the full-compression flag on a record length.
func ValuePtrMarkFullCompressed(length uint32) uint32 {
	return length | valuePtrFullCompressedMask | valuePtrCompressedMask
}

// ValuePtrMarkOmittedKey sets the omitted-key flag on a record length.
func ValuePtrMarkOmittedKey(length uint32) uint32 {
	return length | valuePtrOmittedKeyMask
}
