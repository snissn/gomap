package node

import (
	"encoding/binary"
)

const leafColumnarHeaderSize = 11

func parseLeafColumnarLayout(data []byte, offset int) (leafEntryLayout, error) {
	if offset+leafColumnarHeaderSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	keyLen := int(getUint16(data[offset : offset+2]))
	valLen := int(binary.LittleEndian.Uint32(data[offset+2 : offset+6]))
	flags := data[offset+6]
	keyOff := int(getUint16(data[offset+7 : offset+9]))
	valOff := int(getUint16(data[offset+9 : offset+11]))

	remaining := len(data) - offset
	if keyOff < leafColumnarHeaderSize || valOff < leafColumnarHeaderSize || keyOff > remaining || valOff > remaining {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	keyStart := offset + keyOff
	if keyStart+keyLen > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	valSize := valLen
	if flags&FlagPointer != 0 {
		valSize = leafValuePtrSizeFromData(data)
	}
	valStart := offset + valOff
	if valStart+valSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	return leafEntryLayout{
		headerSize: leafColumnarHeaderSize,
		suffixLen:  keyLen,
		keyLen:     keyLen,
		valLen:     valLen,
		flags:      flags,
		keyOff:     keyOff,
		valOff:     valOff,
	}, nil
}

func writeLeafColumnarHeader(dst []byte, keyLen, valLen int, flags byte, keyOff, valOff int) {
	_ = dst[leafColumnarHeaderSize-1]
	putUint16(dst[0:2], uint16(keyLen))
	putUint32(dst[2:6], uint32(valLen))
	dst[6] = flags
	putUint16(dst[7:9], uint16(keyOff))
	putUint16(dst[9:11], uint16(valOff))
}
