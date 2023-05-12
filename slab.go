package gomap

import (
	"encoding/binary"
	"unsafe"
)

func (h *Hashmap) addSlab(item Item) Key {
	keyBytes := item.Key
	valueBytes := item.Value

	totalLength := len(keyBytes) + len(valueBytes) + 16 // 10 is the maximum length of LEB128 encoded uint64

	offset := *h.slabOffset

	// Make sure that offset + totalLength is within h.slabSize
	if uint64(offset)+uint64(totalLength) > uint64(h.slabSize) {
		err := h.doubleSlab()
		if err != nil {
			panic(err)
		}
	}

	slab := unsafe.Slice((*byte)(unsafe.Pointer(&h.slabMap[offset])), totalLength)

	// Write key length
	keyLength := encodeuint64(slab, uint64(len(keyBytes)))
	// Write value length
	valueLength := encodeuint64(slab[keyLength:], uint64(len(valueBytes)))
	// Write key
	copy(slab[keyLength+valueLength:], keyBytes)
	// Write value
	copy(slab[keyLength+valueLength+len(keyBytes):], valueBytes)

	actualTotalLength := keyLength + valueLength + len(keyBytes) + len(valueBytes)
	*h.slabOffset += SlabOffset(actualTotalLength)
	return Key(offset)
}

func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) Item {
	var ret Item

	rawBytes := h.slabMap[slabValues:]

	keyLength, n := decodeuint64(rawBytes)
	valueLength, m := decodeuint64(rawBytes[n:])

	ret.Key = rawBytes[n+m : n+m+int(keyLength)]
	ret.Value = rawBytes[n+m+int(keyLength) : n+m+int(keyLength)+int(valueLength)]

	return ret
}
func decodeuint64(input []byte) (uint64, int) {
	return binary.LittleEndian.Uint64(input), 8
}

func encodeuint64(slab []byte, input uint64) int {
	binary.LittleEndian.PutUint64(slab, input)
	return 8
}

func encodeLEB128(slab []byte, input uint64) int {
	var i int
	for input >= 0x80 {
		slab[i] = byte(input&0x7F | 0x80)
		input >>= 7
		i++
	}
	slab[i] = byte(input)
	return i + 1
}

func decodeLEB128(input []byte) (uint64, int) {
	var result uint64
	var shift uint
	var length int
	for {
		b := input[length]
		length++
		result |= (uint64(b&0x7F) << shift)
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, length
}
