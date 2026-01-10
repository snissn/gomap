package page

const (
	valuePtrCompressedMask     uint32 = 0x80000000
	valuePtrFullCompressedMask uint32 = 0x40000000
	valuePtrOmittedKeyMask     uint32 = 0x20000000
	valuePtrDictCompressedMask uint32 = 0x10000000
)

// ValuePtrRecordLength returns the record length with internal flags stripped.
func ValuePtrRecordLength(ptr ValuePtr) uint32 {
	return ptr.Length &^ (valuePtrCompressedMask | valuePtrFullCompressedMask | valuePtrOmittedKeyMask | valuePtrDictCompressedMask)
}

// ValuePtrIsCompressed reports whether the pointer references a compressed value.
func ValuePtrIsCompressed(ptr ValuePtr) bool {
	return ptr.Length&valuePtrCompressedMask != 0
}

// ValuePtrIsDictCompressed reports whether the pointer references a dictionary-compressed value.
func ValuePtrIsDictCompressed(ptr ValuePtr) bool {
	return ptr.Length&valuePtrDictCompressedMask != 0
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

// ValuePtrMarkDictCompressed sets the dictionary-compression flag on a record length.
func ValuePtrMarkDictCompressed(length uint32) uint32 {
	return length | valuePtrDictCompressedMask | valuePtrCompressedMask
}

// ValuePtrMarkOmittedKey sets the omitted-key flag on a record length.
func ValuePtrMarkOmittedKey(length uint32) uint32 {
	return length | valuePtrOmittedKeyMask
}
