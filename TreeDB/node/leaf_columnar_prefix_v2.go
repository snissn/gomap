package node

import (
	"encoding/binary"
	"math"
)

// parseLeafColumnarPrefixV2Layout parses the combined columnar+prefix leaf
// encoding. It reuses the v2 prefix header for shared/suffix lengths (and
// inline value length varint), then reads explicit key/value offsets (u16 each).
//
// The suffix bytes are located at entryStart+keyOff and have length suffixLen.
// The value bytes/pointer are located at entryStart+valOff.
func parseLeafColumnarPrefixV2Layout(data []byte, offset int, valPtrSize int) (leafEntryLayout, error) {
	const offsetsSize = 4 // keyOff(u16) + valOff(u16)
	if offset+leafPrefixV2HeaderBaseSize+offsetsSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	shared8 := data[offset]
	suffix8 := data[offset+1]
	flags := data[offset+2]
	headerSize := leafPrefixV2HeaderBaseSize

	prefixLen := 0
	suffixLen := 0
	if shared8 == 0xFF || suffix8 == 0xFF {
		if shared8 != 0xFF || suffix8 != 0xFF {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		if offset+leafPrefixV2HeaderBaseSize+leafPrefixV2HeaderExtSize+offsetsSize > len(data) {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		prefixLen = int(getUint16(data[offset+3 : offset+5]))
		suffixLen = int(getUint16(data[offset+5 : offset+7]))
		headerSize += leafPrefixV2HeaderExtSize
	} else {
		prefixLen = int(shared8)
		suffixLen = int(suffix8)
	}
	if prefixLen < 0 || suffixLen < 0 {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	if prefixLen > math.MaxInt-suffixLen {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	valLen := 0
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		v, n := binary.Uvarint(data[offset+headerSize:])
		if n <= 0 {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		if v > uint64(math.MaxInt) {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		valLen = int(v)
		headerSize += n
	}

	if offset+headerSize+offsetsSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	keyOff := int(getUint16(data[offset+headerSize : offset+headerSize+2]))
	valOff := int(getUint16(data[offset+headerSize+2 : offset+headerSize+4]))
	headerSize += offsetsSize

	remaining := len(data) - offset
	if keyOff < headerSize || valOff < headerSize || keyOff > remaining || valOff > remaining {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	keyStart := offset + keyOff
	if keyStart+suffixLen > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	valSize := valLen
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	}
	valStart := offset + valOff
	if valStart+valSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	return leafEntryLayout{
		headerSize: headerSize,
		prefixLen:  prefixLen,
		suffixLen:  suffixLen,
		keyLen:     prefixLen + suffixLen,
		valLen:     valLen,
		flags:      flags,
		keyOff:     keyOff,
		valOff:     valOff,
	}, nil
}
