package page

const valueLogFileIDMask uint32 = 0x80000000

// IsValueLogFileID reports whether the FileID references a value-log segment.
func IsValueLogFileID(id uint32) bool {
	return id&valueLogFileIDMask != 0
}

// ValueLogFileID marks a value-log segment ID for use in ValuePtr.FileID.
func ValueLogFileID(id uint32) uint32 {
	return id | valueLogFileIDMask
}

// ValueLogSegmentID strips the value-log marker bit from a FileID.
func ValueLogSegmentID(id uint32) uint32 {
	return id &^ valueLogFileIDMask
}
