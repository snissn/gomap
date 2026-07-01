package node

import (
	"encoding/binary"
)

// leafColumnarHeaderSize is the size of the columnar leaf entry header.
//
// Layout:
//
//	keyLen(u16) | valLen(u32) | flags(u8)
//
// Offsets are implicit:
//
//	valOff = leafColumnarHeaderSize
//	keyOff = valOff + valSize
//
// This shrinks per-entry overhead vs the original columnar layout that stored
// keyOff/valOff explicitly. (Pre-alpha storage format: backward compatibility
// is not required.)
const leafColumnarHeaderSize = 7

// leafColumnarV2MetaSize is the per-entry metadata stored outside of the key
// directory for columnar v2 leaves.
//
// Layout: valOff(u16) | flags(u8).
// (key offsets are stored in the page directory.)
const leafColumnarV2MetaSize = 3

// leafColumnarPrefixV2MetaSize is the per-entry metadata stored at the top of
// combined columnar+prefix leaves.
//
// Layout: keyOff(u16) | valOff(u16) | flags(u8) | prefixLen(u16)
const leafColumnarPrefixV2MetaSize = 7

func parseLeafColumnarLayout(data []byte, offset int, valPtrSize int, entryRevisions bool) (leafEntryLayout, error) {
	if offset+leafColumnarHeaderSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	keyLen := int(getUint16(data[offset : offset+2]))
	valLen := int(binary.LittleEndian.Uint32(data[offset+2 : offset+6]))
	flags := data[offset+6]

	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	} else if flags&FlagTombstone == 0 {
		valSize = valLen
	}
	valOff := leafColumnarHeaderSize
	keyOff := valOff + valSize

	keyStart := offset + keyOff
	if keyStart+keyLen > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	valStart := offset + valOff
	if valStart+valSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	revision, err := decodeEntryRevision(data, keyStart+keyLen, entryRevisions)
	if err != nil {
		return leafEntryLayout{}, err
	}

	return leafEntryLayout{
		headerSize: leafColumnarHeaderSize,
		suffixLen:  keyLen,
		keyLen:     keyLen,
		valLen:     valLen,
		flags:      flags,
		revision:   revision,
		keyOff:     keyOff,
		valOff:     valOff,
	}, nil
}

func writeLeafColumnarHeader(dst []byte, keyLen, valLen int, flags byte) {
	_ = dst[leafColumnarHeaderSize-1]
	putUint16(dst[0:2], uint16(keyLen))
	putUint32(dst[2:6], uint32(valLen))
	dst[6] = flags
}
