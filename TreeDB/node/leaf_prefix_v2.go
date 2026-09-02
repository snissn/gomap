package node

import (
	"encoding/binary"
	"math"
)

const (
	leafPrefixV2HeaderBaseSize = 3 // shared8 + suffix8 + flags
	leafPrefixV2HeaderExtSize  = 4 // shared16 + suffix16 (little endian)
)

func uvarintLen(x uint64) int {
	switch {
	case x < 1<<7:
		return 1
	case x < 1<<14:
		return 2
	case x < 1<<21:
		return 3
	case x < 1<<28:
		return 4
	case x < 1<<35:
		return 5
	case x < 1<<42:
		return 6
	case x < 1<<49:
		return 7
	case x < 1<<56:
		return 8
	case x < 1<<63:
		return 9
	default:
		return 10
	}
}

func leafPrefixHeaderSizeV2(prefixLen, suffixLen int, flags byte, valLen int) int {
	headerSize := leafPrefixV2HeaderBaseSize
	if prefixLen > 254 || suffixLen > 254 {
		headerSize += leafPrefixV2HeaderExtSize
	}
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		if valLen < 0 {
			valLen = 0
		}
		headerSize += uvarintLen(uint64(valLen))
	}
	return headerSize
}

func parseLeafPrefixV2Layout(data []byte, offset int, entryRevisions bool) (leafEntryLayout, error) {
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

	keyStart := offset + headerSize
	if keyStart+suffixLen > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	valSize := valLen
	if flags&FlagPointer != 0 {
		valSize = leafValuePtrSizeFromData(data)
	}
	valStart := offset + headerSize + suffixLen
	if valStart+valSize > len(data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	revision, err := decodeEntryRevision(data, valStart+valSize, entryRevisions)
	if err != nil {
		return leafEntryLayout{}, err
	}

	return leafEntryLayout{
		headerSize: headerSize,
		prefixLen:  prefixLen,
		suffixLen:  suffixLen,
		keyLen:     prefixLen + suffixLen,
		valLen:     valLen,
		flags:      flags,
		revision:   revision,
		keyOff:     headerSize,
		valOff:     headerSize + suffixLen,
	}, nil
}
