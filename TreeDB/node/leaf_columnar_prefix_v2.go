package node

import (
	"encoding/binary"
	"math"
)

// parseLeafColumnarPrefixV2Layout parses the combined columnar+prefix leaf
// encoding. It reuses the v2 prefix header for shared/suffix lengths (and
// inline value length varint). Offsets are implicit:
//
//	valOff = headerSize
//	keyOff = headerSize + valSize
//
// The suffix bytes are located at entryStart+keyOff and have length suffixLen.
// The value bytes/pointer are located at entryStart+valOff.
func parseLeafColumnarPrefixV2Layout(data []byte, offset int, valPtrSize int) (leafEntryLayout, error) {
	if offset+leafPrefixV2HeaderBaseSize > len(data) {
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
		if offset+leafPrefixV2HeaderBaseSize+leafPrefixV2HeaderExtSize > len(data) {
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

	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	} else if flags&FlagTombstone == 0 {
		valSize = valLen
	}
	valOff := headerSize
	keyOff := valOff + valSize

	keyStart := offset + keyOff
	if keyStart+suffixLen > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
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
