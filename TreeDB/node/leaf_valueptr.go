package node

import "github.com/snissn/gomap/TreeDB/page"

func leafValuePtrSizeFromData(data []byte) int {
	if len(data) < NodeHeaderSize {
		return page.ValuePtrSize
	}
	flags := getUint16(data[12:14])
	if flags&leafPackedValuePtrFlag != 0 {
		return page.PackedValuePtrSize
	}
	return page.ValuePtrSize
}
