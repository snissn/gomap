package node

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/snissn/gomap/TreeDB/page"
)

const (
	FlagInline    = 0x00
	FlagPointer   = 0x01
	FlagTombstone = 0x02
)

// LeafEntry represents a parsed entry from a Leaf Node.
type LeafEntry struct {
	Key      []byte
	Value    []byte
	ValuePtr page.ValuePtr // Valid if Flags & FlagPointer
	Flags    byte
}

type leafEntryLayout struct {
	headerSize int
	prefixLen  int
	suffixLen  int
	keyLen     int
	valLen     int
	flags      byte
	keyOff     int
	valOff     int
}

func (n *Node) ensureKeyScratch(size int) []byte {
	if size < 0 {
		return nil
	}
	if cap(n.keyScratch) < size {
		n.keyScratch = make([]byte, size)
	}
	return n.keyScratch[:size]
}

func (n *Node) leafEntryLayoutAt(offset int) (leafEntryLayout, error) {
	if offset < NodeHeaderSize || offset >= len(n.data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	if n.leafPrefixCompressed() {
		valPtrSize := page.ValuePtrSize
		if n.leafPackedValuePtr() {
			valPtrSize = page.PackedValuePtrSize
		}

		if n.leafPrefixV2() {
			if n.leafColumnar() {
				return parseLeafColumnarPrefixV2Layout(n.data, offset, valPtrSize)
			}
			return parseLeafPrefixV2Layout(n.data, offset)
		}
		if n.leafColumnar() {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		if offset+9 > len(n.data) {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		prefixLen := int(getUint16(n.data[offset : offset+2]))
		suffixLen := int(getUint16(n.data[offset+2 : offset+4]))
		valLen := int(binary.LittleEndian.Uint32(n.data[offset+4 : offset+8]))
		flags := n.data[offset+8]
		keyStart := offset + 9
		if keyStart+suffixLen > len(n.data) {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		keyOff := 9
		return leafEntryLayout{
			headerSize: 9,
			prefixLen:  prefixLen,
			suffixLen:  suffixLen,
			keyLen:     prefixLen + suffixLen,
			valLen:     valLen,
			flags:      flags,
			keyOff:     keyOff,
			valOff:     keyOff + suffixLen,
		}, nil
	}

	if n.leafColumnar() {
		if n.leafColumnarV2() {
			return leafEntryLayout{}, ErrCorruptedNode
		}
		valPtrSize := page.ValuePtrSize
		if n.leafPackedValuePtr() {
			valPtrSize = page.PackedValuePtrSize
		}
		return parseLeafColumnarLayout(n.data, offset, valPtrSize)
	}

	if offset+7 > len(n.data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	keyLen := int(getUint16(n.data[offset : offset+2]))
	valLen := int(binary.LittleEndian.Uint32(n.data[offset+2 : offset+6]))
	flags := n.data[offset+6]
	keyStart := offset + 7
	if keyStart+keyLen > len(n.data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}
	keyOff := 7
	return leafEntryLayout{
		headerSize: 7,
		suffixLen:  keyLen,
		keyLen:     keyLen,
		valLen:     valLen,
		flags:      flags,
		keyOff:     keyOff,
		valOff:     keyOff + keyLen,
	}, nil
}

func (n *Node) leafEntryKeyAt(index uint16) (key []byte, layout leafEntryLayout, entryStart int, err error) {
	offset, err := n.getOffset(index)
	if err != nil {
		return nil, leafEntryLayout{}, 0, err
	}
	if int(offset) >= len(n.data) {
		return nil, leafEntryLayout{}, 0, ErrCorruptedNode
	}
	entryStart = int(offset)

	if n.leafColumnar() && !n.leafPrefixCompressed() {
		if n.leafColumnarV2() {
			k, err := n.leafColumnarKeyViewAtIndex(int(index))
			if err != nil {
				return nil, leafEntryLayout{}, 0, err
			}
			n.leafValid = false
			return k, leafEntryLayout{
				suffixLen: len(k),
				keyLen:    len(k),
			}, entryStart, nil
		}
		layout, err = n.leafEntryLayoutAt(entryStart)
		if err != nil {
			return nil, leafEntryLayout{}, 0, err
		}
		keyStart := entryStart + layout.keyOff
		keyEnd := keyStart + layout.keyLen
		if keyEnd > len(n.data) {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}
		key = n.data[keyStart:keyEnd]
		n.leafValid = false
		return key, layout, entryStart, nil
	}

	if !n.leafPrefixCompressed() {
		layout, err = n.leafEntryLayoutAt(entryStart)
		if err != nil {
			return nil, leafEntryLayout{}, 0, err
		}
		keyStart := entryStart + layout.keyOff
		if keyStart+layout.keyLen > len(n.data) {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}
		key = n.data[keyStart : keyStart+layout.keyLen]
		n.leafValid = false
		return key, layout, entryStart, nil
	}

	count := n.Count()
	if index >= count {
		return nil, leafEntryLayout{}, 0, ErrCorruptedNode
	}
	if n.leafValid && n.leafIndex+1 == index {
		layout, err = n.leafEntryLayoutAt(entryStart)
		if err != nil {
			return nil, leafEntryLayout{}, 0, err
		}
		if index%leafPrefixRestartInterval == 0 && layout.prefixLen != 0 {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}
		keyStart := entryStart + layout.keyOff
		keyEnd := keyStart + layout.suffixLen
		if keyEnd > len(n.data) {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}
		if layout.prefixLen == 0 {
			key = n.data[keyStart:keyEnd]
			n.leafKey = key
			n.leafLayout = layout
			n.leafEntry = entryStart
			n.leafIndex = index
			n.leafValid = true
			return key, layout, entryStart, nil
		}
		if layout.prefixLen > len(n.leafKey) {
			n.leafValid = false
		} else {
			keyLen := layout.prefixLen + layout.suffixLen
			key = n.ensureKeyScratch(keyLen)
			sameBacking := len(n.leafKey) > 0 && len(key) > 0 && &n.leafKey[0] == &key[0]
			if !sameBacking && layout.prefixLen > 0 {
				copy(key, n.leafKey[:layout.prefixLen])
			}
			copy(key[layout.prefixLen:], n.data[keyStart:keyEnd])
			n.leafKey = key
			n.leafLayout = layout
			n.leafEntry = entryStart
			n.leafIndex = index
			n.leafValid = true
			return key, layout, entryStart, nil
		}
	}
	restart := index - (index % leafPrefixRestartInterval)
	var prevKey []byte
	for i := restart; i <= index; i++ {
		off, err := n.getOffset(i)
		if err != nil {
			return nil, leafEntryLayout{}, 0, err
		}
		if int(off) >= len(n.data) {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}
		ptr := int(off)
		layout, err = n.leafEntryLayoutAt(ptr)
		if err != nil {
			return nil, leafEntryLayout{}, 0, err
		}
		keyStart := ptr + layout.keyOff
		keyEnd := keyStart + layout.suffixLen
		if keyEnd > len(n.data) {
			return nil, leafEntryLayout{}, 0, ErrCorruptedNode
		}

		if i == restart {
			if layout.prefixLen != 0 {
				return nil, leafEntryLayout{}, 0, ErrCorruptedNode
			}
			key = n.data[keyStart:keyEnd]
		} else {
			if layout.prefixLen > len(prevKey) {
				return nil, leafEntryLayout{}, 0, ErrCorruptedNode
			}
			keyLen := layout.prefixLen + layout.suffixLen
			key = n.ensureKeyScratch(keyLen)
			sameBacking := len(prevKey) > 0 && len(key) > 0 && &prevKey[0] == &key[0]
			if !sameBacking && layout.prefixLen > 0 {
				copy(key, prevKey[:layout.prefixLen])
			}
			copy(key[layout.prefixLen:], n.data[keyStart:keyEnd])
		}

		prevKey = key
		if i == index {
			entryStart = ptr
			n.leafKey = key
			n.leafLayout = layout
			n.leafEntry = entryStart
			n.leafIndex = index
			n.leafValid = true
			return key, layout, entryStart, nil
		}
	}

	return nil, leafEntryLayout{}, 0, ErrCorruptedNode
}

// GetLeafEntryView returns a view of the entry at the given index.
// Key and Value slices point directly to the node's data when the node is not
// prefix-compressed. For prefix-compressed nodes, Key is reconstructed into a
// scratch buffer owned by the node and is only valid until the next call.
func (n *Node) GetLeafEntryView(index uint16) (key []byte, val []byte, valPtr page.ValuePtr, flags byte, err error) {
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		return n.getLeafEntryViewColumnarV2(index)
	}

	layout, entryStart := leafEntryLayout{}, 0
	key, layout, entryStart, err = n.leafEntryKeyAt(index)
	if err != nil {
		return nil, nil, page.ValuePtr{}, 0, err
	}
	flags = layout.flags

	if flags&FlagTombstone != 0 {
		return key, nil, page.ValuePtr{}, flags, nil
	}

	valueStart := entryStart + layout.valOff
	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		if n.leafPackedValuePtr() {
			valPtrSize = page.PackedValuePtrSize
		}
		if valueStart+valPtrSize > len(n.data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if n.leafPackedValuePtr() {
			valPtr = page.DecodePackedValuePtr(n.data[valueStart : valueStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(n.data[valueStart : valueStart+valPtrSize])
		}
		return key, nil, valPtr, flags, nil
	}

	// Inline
	if valueStart+layout.valLen > len(n.data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	val = n.data[valueStart : valueStart+layout.valLen]
	return key, val, page.ValuePtr{}, flags, nil
}

// GetLeafValueView returns the value (inline or pointer) at the given index
// without reconstructing the key. This is useful for point reads after SearchLeaf.
func (n *Node) GetLeafValueView(index uint16) (val []byte, valPtr page.ValuePtr, flags byte, err error) {
	if n.Type() != page.PageTypeLeaf {
		return nil, page.ValuePtr{}, 0, ErrInvalidType
	}

	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		return n.getLeafValueViewColumnarV2(index)
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return nil, page.ValuePtr{}, 0, err
	}
	if int(offset) >= len(n.data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	entryStart := int(offset)

	layout, err := n.leafEntryLayoutAt(entryStart)
	if err != nil {
		return nil, page.ValuePtr{}, 0, err
	}
	flags = layout.flags

	if flags&FlagTombstone != 0 {
		return nil, page.ValuePtr{}, flags, nil
	}

	valueStart := entryStart + layout.valOff
	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		if n.leafPackedValuePtr() {
			valPtrSize = page.PackedValuePtrSize
		}
		if valueStart+valPtrSize > len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if n.leafPackedValuePtr() {
			valPtr = page.DecodePackedValuePtr(n.data[valueStart : valueStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(n.data[valueStart : valueStart+valPtrSize])
		}
		return nil, valPtr, flags, nil
	}

	// Inline
	if valueStart+layout.valLen > len(n.data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	val = n.data[valueStart : valueStart+layout.valLen]
	return val, page.ValuePtr{}, flags, nil
}

// UpdateLeafValuePtr updates the ValuePtr bytes for the entry at index if the
// entry is a pointer and currently matches oldPtr. It updates the page checksum
// on success.
//
// This is intended for maintenance operations (e.g. value-log compaction) that need
// to swap pointers without rewriting the B+Tree structure.
func (n *Node) UpdateLeafValuePtr(index uint16, oldPtr, newPtr page.ValuePtr) (bool, error) {
	if n.Type() != page.PageTypeLeaf {
		return false, ErrInvalidType
	}
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		count := n.Count()
		if index >= count {
			return false, ErrCorruptedNode
		}

		data := n.data
		keyDirStart := NodeHeaderSize
		keyDirEnd := keyDirStart + int(count)*DirectoryEntrySize
		valDirStart := keyDirEnd
		valDirEnd := valDirStart + int(count)*DirectoryEntrySize
		flagsStart := valDirEnd
		headerEnd := flagsStart + int(count)
		if headerEnd > len(data) {
			return false, ErrCorruptedNode
		}

		flags := data[flagsStart+int(index)]
		if flags&FlagTombstone != 0 || flags&FlagPointer == 0 {
			return false, nil
		}

		keysStart := int(getUint16(data[keyDirStart : keyDirStart+2]))
		if keysStart < headerEnd || keysStart > len(data) {
			return false, ErrCorruptedNode
		}

		valStartOff := valDirStart + int(index)*2
		if valStartOff+2 > len(data) {
			return false, ErrCorruptedNode
		}
		valStart := int(getUint16(data[valStartOff : valStartOff+2]))
		valEnd := keysStart
		if index+1 < count {
			nextValOff := valStartOff + 2
			if nextValOff+2 > len(data) {
				return false, ErrCorruptedNode
			}
			valEnd = int(getUint16(data[nextValOff : nextValOff+2]))
		}
		if valStart < headerEnd || valEnd < valStart || valEnd > keysStart {
			return false, ErrCorruptedNode
		}

		valPtrSize := page.ValuePtrSize
		packed := n.leafPackedValuePtr()
		if packed {
			valPtrSize = page.PackedValuePtrSize
		}
		if valEnd-valStart != valPtrSize {
			return false, ErrCorruptedNode
		}
		if valStart+valPtrSize > len(data) {
			return false, ErrCorruptedNode
		}

		var cur page.ValuePtr
		if packed {
			cur = page.DecodePackedValuePtr(data[valStart : valStart+valPtrSize])
		} else {
			cur = page.DecodeValuePtr(data[valStart : valStart+valPtrSize])
		}
		if cur != oldPtr {
			return false, nil
		}

		if packed {
			page.EncodePackedValuePtr(data[valStart:valStart+valPtrSize], newPtr)
		} else {
			newPtr.Encode(data[valStart : valStart+valPtrSize])
		}
		n.UpdateChecksum()
		return true, nil
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return false, err
	}
	if int(offset) >= len(n.data) {
		return false, ErrCorruptedNode
	}

	ptr := int(offset)
	if ptr < NodeHeaderSize {
		return false, ErrCorruptedNode
	}

	layout, err := n.leafEntryLayoutAt(ptr)
	if err != nil {
		return false, err
	}
	flags := layout.flags

	// Tombstones and inline values have no ValuePtr bytes to rewrite.
	if flags&FlagTombstone != 0 || flags&FlagPointer == 0 {
		return false, nil
	}

	ptr += layout.valOff
	valPtrSize := page.ValuePtrSize
	packed := n.leafPackedValuePtr()
	if packed {
		valPtrSize = page.PackedValuePtrSize
	}
	if ptr+valPtrSize > len(n.data) {
		return false, ErrCorruptedNode
	}

	var cur page.ValuePtr
	if packed {
		cur = page.DecodePackedValuePtr(n.data[ptr : ptr+valPtrSize])
	} else {
		cur = page.DecodeValuePtr(n.data[ptr : ptr+valPtrSize])
	}
	if cur != oldPtr {
		return false, nil
	}

	if packed {
		page.EncodePackedValuePtr(n.data[ptr:ptr+valPtrSize], newPtr)
	} else {
		newPtr.Encode(n.data[ptr : ptr+valPtrSize])
	}
	n.UpdateChecksum()
	return true, nil
}

// GetLeafEntry reads the entry at the given index.
func (n *Node) GetLeafEntry(index uint16) (LeafEntry, error) {
	keyView, valView, valPtr, flags, err := n.GetLeafEntryView(index)
	if err != nil {
		return LeafEntry{}, err
	}

	// Make copies for safety
	key := make([]byte, len(keyView))
	copy(key, keyView)

	var val []byte
	if valView != nil {
		val = make([]byte, len(valView))
		copy(val, valView)
	}

	return LeafEntry{
		Key:      key,
		Value:    val,
		ValuePtr: valPtr,
		Flags:    flags,
	}, nil
}

// SearchLeaf performs a binary search for the given key in a Leaf Node.
// Returns the index of the first entry where Entry.Key >= key.
// If key is found, found=true.
// If key is greater than all entries, returns Count, false.
func (n *Node) SearchLeaf(key []byte) (uint16, bool, error) {
	if n.leafPrefixCompressed() {
		if n.leafColumnar() && n.leafPrefixV2() {
			return n.searchLeafColumnarPrefixV2(key)
		}
		return n.searchLeafPrefixCompressed(key)
	}
	if n.leafColumnar() {
		return n.searchLeafColumnar(key)
	}

	data := n.data
	count := n.Count()
	if count <= smallSearchThreshold {
		for idx := 0; idx < int(count); idx++ {
			dirOff := NodeHeaderSize + idx*2
			if dirOff+2 > len(data) {
				return 0, false, ErrCorruptedNode
			}
			offset := getUint16(data[dirOff : dirOff+2])
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+7 > len(data) {
				return 0, false, ErrCorruptedNode
			}
			keyLen := getUint16(data[ptr : ptr+2])
			keyPtr := ptr + 7
			if keyPtr+int(keyLen) > len(data) {
				return 0, false, ErrCorruptedNode
			}
			cmp := bytes.Compare(data[keyPtr:keyPtr+int(keyLen)], key)
			if cmp >= 0 {
				return uint16(idx), cmp == 0, nil
			}
		}
		return count, false, nil
	}
	i, j := 0, int(count)

	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow

		// Read key at index h without full decode
		// Validate bounds to avoid panics on corrupted pages.
		dirOff := NodeHeaderSize + h*2
		if dirOff+2 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		offset := getUint16(data[dirOff : dirOff+2])
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+7 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		keyLen := getUint16(data[ptr : ptr+2])
		// Skip ValLen(4) + Flags(1)
		keyPtr := ptr + 7
		if keyPtr+int(keyLen) > len(data) {
			return 0, false, ErrCorruptedNode
		}

		// Compare
		cmp := bytes.Compare(data[keyPtr:keyPtr+int(keyLen)], key)
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < int(count) {
		// Check for equality
		dirOff := NodeHeaderSize + i*2
		if dirOff+2 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		offset := getUint16(data[dirOff : dirOff+2])
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+7 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		keyLen := getUint16(data[ptr : ptr+2])
		keyPtr := ptr + 7
		if keyPtr+int(keyLen) > len(data) {
			return 0, false, ErrCorruptedNode
		}
		if bytes.Equal(data[keyPtr:keyPtr+int(keyLen)], key) {
			return uint16(i), true, nil
		}
	}

	return uint16(i), false, nil
}

func (n *Node) leafColumnarKeyViewAtIndex(idx int) ([]byte, error) {
	data := n.data
	if n.leafColumnarV2() {
		count := n.Count()
		if idx < 0 || idx >= int(count) {
			return nil, ErrCorruptedNode
		}
		keyDirOff := NodeHeaderSize + idx*2
		nextDirOff := keyDirOff + 2
		headerEnd := NodeHeaderSize + int(count)*DirectoryEntrySize + int(count)*DirectoryEntrySize + int(count)
		if headerEnd > len(data) || nextDirOff > len(data) {
			return nil, ErrCorruptedNode
		}

		keyStart := int(getUint16(data[keyDirOff : keyDirOff+2]))
		keyEnd := len(data)
		if idx+1 < int(count) {
			if nextDirOff+2 > len(data) {
				return nil, ErrCorruptedNode
			}
			keyEnd = int(getUint16(data[nextDirOff : nextDirOff+2]))
		}
		if keyStart < headerEnd || keyEnd < keyStart || keyEnd > len(data) {
			return nil, ErrCorruptedNode
		}
		return data[keyStart:keyEnd], nil
	}

	dirOff := NodeHeaderSize + idx*2
	if dirOff+2 > len(data) {
		return nil, ErrCorruptedNode
	}
	offset := getUint16(data[dirOff : dirOff+2])
	ptr := int(offset)
	if ptr < NodeHeaderSize || ptr+leafColumnarHeaderSize > len(data) {
		return nil, ErrCorruptedNode
	}

	keyLen := int(getUint16(data[ptr : ptr+2]))
	valLen := int(binary.LittleEndian.Uint32(data[ptr+2 : ptr+6]))
	flags := data[ptr+6]
	valSize := 0
	if flags&FlagPointer != 0 {
		if n.leafPackedValuePtr() {
			valSize = page.PackedValuePtrSize
		} else {
			valSize = page.ValuePtrSize
		}
	} else if flags&FlagTombstone == 0 {
		valSize = valLen
	}

	keyStart := ptr + leafColumnarHeaderSize + valSize
	if keyStart+keyLen > len(data) {
		return nil, ErrCorruptedNode
	}
	return data[keyStart : keyStart+keyLen], nil
}

func (n *Node) getLeafEntryViewColumnarV2(index uint16) (key []byte, val []byte, valPtr page.ValuePtr, flags byte, err error) {
	count := n.Count()
	if index >= count {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	data := n.data
	keyDirStart := NodeHeaderSize
	keyDirEnd := keyDirStart + int(count)*DirectoryEntrySize
	valDirStart := keyDirEnd
	valDirEnd := valDirStart + int(count)*DirectoryEntrySize
	flagsStart := valDirEnd
	headerEnd := flagsStart + int(count)
	if headerEnd > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	keyStartOff := keyDirStart + int(index)*2
	if keyStartOff+2 > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	keyStart := int(getUint16(data[keyStartOff : keyStartOff+2]))
	keyEnd := len(data)
	if index+1 < count {
		nextKeyOff := keyStartOff + 2
		if nextKeyOff+2 > len(data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		keyEnd = int(getUint16(data[nextKeyOff : nextKeyOff+2]))
	}
	if keyStart < headerEnd || keyEnd < keyStart || keyEnd > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	key = data[keyStart:keyEnd]

	flags = data[flagsStart+int(index)]
	if flags&FlagTombstone != 0 {
		return key, nil, page.ValuePtr{}, flags, nil
	}

	keysStart := int(getUint16(data[keyDirStart : keyDirStart+2]))
	if keysStart < headerEnd || keysStart > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	valStartOff := valDirStart + int(index)*2
	if valStartOff+2 > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	valStart := int(getUint16(data[valStartOff : valStartOff+2]))
	valEnd := keysStart
	if index+1 < count {
		nextValOff := valStartOff + 2
		if nextValOff+2 > len(data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valEnd = int(getUint16(data[nextValOff : nextValOff+2]))
	}
	if valStart < headerEnd || valEnd < valStart || valEnd > keysStart {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		packed := n.leafPackedValuePtr()
		if packed {
			valPtrSize = page.PackedValuePtrSize
		}
		if valEnd-valStart != valPtrSize {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if valStart+valPtrSize > len(data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if packed {
			valPtr = page.DecodePackedValuePtr(data[valStart : valStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(data[valStart : valStart+valPtrSize])
		}
		return key, nil, valPtr, flags, nil
	}

	val = data[valStart:valEnd]
	return key, val, page.ValuePtr{}, flags, nil
}

func (n *Node) getLeafValueViewColumnarV2(index uint16) (val []byte, valPtr page.ValuePtr, flags byte, err error) {
	count := n.Count()
	if index >= count {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	data := n.data
	keyDirStart := NodeHeaderSize
	keyDirEnd := keyDirStart + int(count)*DirectoryEntrySize
	valDirStart := keyDirEnd
	valDirEnd := valDirStart + int(count)*DirectoryEntrySize
	flagsStart := valDirEnd
	headerEnd := flagsStart + int(count)
	if headerEnd > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	flags = data[flagsStart+int(index)]
	if flags&FlagTombstone != 0 {
		return nil, page.ValuePtr{}, flags, nil
	}

	keysStart := int(getUint16(data[keyDirStart : keyDirStart+2]))
	if keysStart < headerEnd || keysStart > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	valStartOff := valDirStart + int(index)*2
	if valStartOff+2 > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	valStart := int(getUint16(data[valStartOff : valStartOff+2]))
	valEnd := keysStart
	if index+1 < count {
		nextValOff := valStartOff + 2
		if nextValOff+2 > len(data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valEnd = int(getUint16(data[nextValOff : nextValOff+2]))
	}
	if valStart < headerEnd || valEnd < valStart || valEnd > keysStart {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		packed := n.leafPackedValuePtr()
		if packed {
			valPtrSize = page.PackedValuePtrSize
		}
		if valEnd-valStart != valPtrSize {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if valStart+valPtrSize > len(data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if packed {
			valPtr = page.DecodePackedValuePtr(data[valStart : valStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(data[valStart : valStart+valPtrSize])
		}
		return nil, valPtr, flags, nil
	}

	val = data[valStart:valEnd]
	return val, page.ValuePtr{}, flags, nil
}

func (n *Node) searchLeafColumnar(key []byte) (uint16, bool, error) {
	count := n.Count()
	if count == 0 {
		return 0, false, nil
	}

	if count <= smallSearchThreshold {
		for idx := 0; idx < int(count); idx++ {
			k, err := n.leafColumnarKeyViewAtIndex(idx)
			if err != nil {
				return 0, false, err
			}
			cmp := bytes.Compare(k, key)
			if cmp >= 0 {
				return uint16(idx), cmp == 0, nil
			}
		}
		return count, false, nil
	}

	i, j := 0, int(count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow
		k, err := n.leafColumnarKeyViewAtIndex(h)
		if err != nil {
			return 0, false, err
		}
		cmp := bytes.Compare(k, key)
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < int(count) {
		k, err := n.leafColumnarKeyViewAtIndex(i)
		if err != nil {
			return 0, false, err
		}
		if bytes.Equal(k, key) {
			return uint16(i), true, nil
		}
	}
	return uint16(i), false, nil
}

func compareLeafPrefixVirtualKey(prevKey []byte, prefixLen int, suffix []byte, target []byte) int {
	// Compare prevKey[:prefixLen] + suffix against target without allocating.
	n := prefixLen
	if len(target) < n {
		n = len(target)
	}
	if n > 0 {
		if cmp := bytes.Compare(prevKey[:n], target[:n]); cmp != 0 {
			return cmp
		}
	}
	if len(target) < prefixLen {
		// target is a strict prefix of the virtual key.
		return 1
	}
	t := target[prefixLen:]
	n = len(suffix)
	if len(t) < n {
		n = len(t)
	}
	if n > 0 {
		if cmp := bytes.Compare(suffix[:n], t[:n]); cmp != 0 {
			return cmp
		}
	}
	if len(t) < len(suffix) {
		return 1
	}
	if len(t) > len(suffix) {
		return -1
	}
	return 0
}

func (n *Node) leafRestartKeyViewAtIndex(index uint16) ([]byte, error) {
	off, err := n.getOffset(index)
	if err != nil {
		return nil, err
	}
	if int(off) >= len(n.data) {
		return nil, ErrCorruptedNode
	}
	ptr := int(off)
	layout, err := n.leafEntryLayoutAt(ptr)
	if err != nil {
		return nil, err
	}
	if layout.prefixLen != 0 {
		return nil, ErrCorruptedNode
	}
	keyStart := ptr + layout.keyOff
	keyEnd := keyStart + layout.suffixLen
	if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
		return nil, ErrCorruptedNode
	}
	return n.data[keyStart:keyEnd], nil
}

func (n *Node) searchLeafPrefixBlock(blockStart, blockEnd uint16, target []byte) (uint16, bool, error) {
	if blockStart >= blockEnd {
		return blockEnd, false, nil
	}

	restartKey, err := n.leafRestartKeyViewAtIndex(blockStart)
	if err != nil {
		return 0, false, err
	}
	cmp := bytes.Compare(restartKey, target)
	if cmp >= 0 {
		return blockStart, cmp == 0, nil
	}

	prevKey := restartKey
	for idx := blockStart + 1; idx < blockEnd; idx++ {
		off, err := n.getOffset(idx)
		if err != nil {
			return 0, false, err
		}
		if int(off) >= len(n.data) {
			return 0, false, ErrCorruptedNode
		}

		ptr := int(off)
		layout, err := n.leafEntryLayoutAt(ptr)
		if err != nil {
			return 0, false, err
		}
		if layout.prefixLen > len(prevKey) {
			return 0, false, ErrCorruptedNode
		}
		keyStart := ptr + layout.keyOff
		keyEnd := keyStart + layout.suffixLen
		if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
			return 0, false, ErrCorruptedNode
		}
		suffix := n.data[keyStart:keyEnd]

		cmp = compareLeafPrefixVirtualKey(prevKey, layout.prefixLen, suffix, target)
		if cmp >= 0 {
			return idx, cmp == 0, nil
		}

		if layout.prefixLen == 0 {
			prevKey = suffix
			continue
		}

		keyLen := layout.prefixLen + layout.suffixLen
		cur := n.ensureKeyScratch(keyLen)
		// If prevKey and cur share backing storage, the existing prefix bytes are
		// already in place and we only need to overwrite the suffix below.
		sameBacking := len(prevKey) > 0 && len(cur) > 0 && &prevKey[0] == &cur[0]
		if !sameBacking && layout.prefixLen > 0 {
			copy(cur, prevKey[:layout.prefixLen])
		}
		copy(cur[layout.prefixLen:], suffix)
		prevKey = cur
	}

	return blockEnd, false, nil
}

func (n *Node) searchLeafPrefixCompressed(key []byte) (uint16, bool, error) {
	count := n.Count()
	if count == 0 {
		return 0, false, nil
	}
	if count <= smallSearchThreshold {
		for idx := uint16(0); idx < count; idx++ {
			k, _, _, err := n.leafEntryKeyAt(idx)
			if err != nil {
				return 0, false, err
			}
			cmp := bytes.Compare(k, key)
			if cmp >= 0 {
				return idx, cmp == 0, nil
			}
		}
		return count, false, nil
	}

	interval := leafPrefixRestartInterval
	restarts := (int(count) + interval - 1) / interval
	if restarts <= 0 {
		restarts = 1
	}

	// Find the first restart key strictly greater than the target. The target
	// can only be present in (or inserted into) the block starting at the
	// previous restart.
	lo := 0
	hi := restarts
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		idx := uint16(mid * interval)
		if idx >= count {
			hi = mid
			continue
		}
		k, err := n.leafRestartKeyViewAtIndex(idx)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(k, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	pos := lo

	blockStart := uint16(0)
	if pos > 0 {
		blockStart = uint16((pos - 1) * interval)
	}
	blockEnd := blockStart + uint16(interval)
	if blockEnd > count {
		blockEnd = count
	}

	return n.searchLeafPrefixBlock(blockStart, blockEnd, key)
}

func (n *Node) leafColumnarPrefixV2KeyPartsAt(index uint16) (prefixLen int, suffix []byte, err error) {
	off, err := n.getOffset(index)
	if err != nil {
		return 0, nil, err
	}
	ptr := int(off)
	if ptr < NodeHeaderSize || ptr+leafPrefixV2HeaderBaseSize > len(n.data) {
		return 0, nil, ErrCorruptedNode
	}

	shared8 := n.data[ptr]
	suffix8 := n.data[ptr+1]
	flags := n.data[ptr+2]
	headerSize := leafPrefixV2HeaderBaseSize
	suffixLen := 0
	valPtrSize := page.ValuePtrSize
	if n.leafPackedValuePtr() {
		valPtrSize = page.PackedValuePtrSize
	}

	if shared8 == 0xFF || suffix8 == 0xFF {
		if shared8 != 0xFF || suffix8 != 0xFF {
			return 0, nil, ErrCorruptedNode
		}
		if ptr+leafPrefixV2HeaderBaseSize+leafPrefixV2HeaderExtSize > len(n.data) {
			return 0, nil, ErrCorruptedNode
		}
		prefixLen = int(getUint16(n.data[ptr+3 : ptr+5]))
		suffixLen = int(getUint16(n.data[ptr+5 : ptr+7]))
		headerSize += leafPrefixV2HeaderExtSize
	} else {
		prefixLen = int(shared8)
		suffixLen = int(suffix8)
	}
	if prefixLen < 0 || suffixLen < 0 {
		return 0, nil, ErrCorruptedNode
	}

	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		v, nBytes := binary.Uvarint(n.data[ptr+headerSize:])
		if nBytes <= 0 {
			return 0, nil, ErrCorruptedNode
		}
		if v > uint64(math.MaxInt) {
			return 0, nil, ErrCorruptedNode
		}
		headerSize += nBytes
		valSize := int(v)
		keyOff := headerSize + valSize
		remaining := len(n.data) - ptr
		if keyOff < headerSize || keyOff > remaining {
			return 0, nil, ErrCorruptedNode
		}
		keyStart := ptr + keyOff
		keyEnd := keyStart + suffixLen
		if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
			return 0, nil, ErrCorruptedNode
		}
		return prefixLen, n.data[keyStart:keyEnd], nil
	}

	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	}
	keyOff := headerSize + valSize
	remaining := len(n.data) - ptr
	if keyOff < headerSize || keyOff > remaining {
		return 0, nil, ErrCorruptedNode
	}
	keyStart := ptr + keyOff
	keyEnd := keyStart + suffixLen
	if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
		return 0, nil, ErrCorruptedNode
	}
	return prefixLen, n.data[keyStart:keyEnd], nil
}

func (n *Node) leafColumnarPrefixV2RestartKeyViewAtIndex(index uint16) ([]byte, error) {
	prefixLen, suffix, err := n.leafColumnarPrefixV2KeyPartsAt(index)
	if err != nil {
		return nil, err
	}
	if prefixLen != 0 {
		return nil, ErrCorruptedNode
	}
	return suffix, nil
}

func (n *Node) searchLeafColumnarPrefixV2Block(blockStart, blockEnd uint16, target []byte) (uint16, bool, error) {
	if blockStart >= blockEnd {
		return blockEnd, false, nil
	}

	restartKey, err := n.leafColumnarPrefixV2RestartKeyViewAtIndex(blockStart)
	if err != nil {
		return 0, false, err
	}
	cmp := bytes.Compare(restartKey, target)
	if cmp >= 0 {
		return blockStart, cmp == 0, nil
	}

	prevKey := restartKey
	for idx := blockStart + 1; idx < blockEnd; idx++ {
		prefixLen, suffix, err := n.leafColumnarPrefixV2KeyPartsAt(idx)
		if err != nil {
			return 0, false, err
		}
		if prefixLen > len(prevKey) {
			return 0, false, ErrCorruptedNode
		}

		cmp = compareLeafPrefixVirtualKey(prevKey, prefixLen, suffix, target)
		if cmp >= 0 {
			return idx, cmp == 0, nil
		}

		if prefixLen == 0 {
			prevKey = suffix
			continue
		}

		keyLen := prefixLen + len(suffix)
		cur := n.ensureKeyScratch(keyLen)
		// If prevKey and cur share backing storage, the existing prefix bytes are
		// already in place and we only need to overwrite the suffix below.
		sameBacking := len(prevKey) > 0 && len(cur) > 0 && &prevKey[0] == &cur[0]
		if !sameBacking && prefixLen > 0 {
			copy(cur, prevKey[:prefixLen])
		}
		copy(cur[prefixLen:], suffix)
		prevKey = cur
	}

	return blockEnd, false, nil
}

func (n *Node) searchLeafColumnarPrefixV2(key []byte) (uint16, bool, error) {
	count := n.Count()
	if count == 0 {
		return 0, false, nil
	}
	if count <= smallSearchThreshold {
		return n.searchLeafColumnarPrefixV2Block(0, count, key)
	}

	interval := leafPrefixRestartInterval
	restarts := (int(count) + interval - 1) / interval
	if restarts <= 0 {
		restarts = 1
	}

	lo := 0
	hi := restarts
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		idx := uint16(mid * interval)
		if idx >= count {
			hi = mid
			continue
		}
		k, err := n.leafColumnarPrefixV2RestartKeyViewAtIndex(idx)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(k, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	pos := lo

	blockStart := uint16(0)
	if pos > 0 {
		blockStart = uint16((pos - 1) * interval)
	}
	blockEnd := blockStart + uint16(interval)
	if blockEnd > count {
		blockEnd = count
	}

	return n.searchLeafColumnarPrefixV2Block(blockStart, blockEnd, key)
}

// AddLeafEntry inserts a new entry into the Leaf Node.
// It maintains the sorted order of keys.
// If the key already exists, it updates it (by rewriting the entry).
// NOTE: This implementation assumes simple append-to-heap and shift-directory.
// It DOES NOT handle fragmentation (holes in heap) yet.
// A full implementation would compact the heap if FreeSpace check fails but logical space exists.
func (n *Node) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	if n.Type() != page.PageTypeLeaf {
		return ErrInvalidType
	}
	if n.leafColumnar() && n.leafPrefixCompressed() {
		return n.addLeafEntryColumnarPrefixV2Rebuild(key, value, flags, valPtr)
	}
	if n.leafColumnar() {
		if n.leafColumnarV2() {
			return n.addLeafEntryColumnarV2Rebuild(key, value, flags, valPtr)
		}
		return n.addLeafEntryColumnar(key, value, flags, valPtr)
	}
	if n.leafPrefixCompressed() {
		return n.addLeafEntryPrefixCompressed(key, value, flags, valPtr)
	}

	// Calculate size needed
	// KeyLen(2) + ValLen(4) + Flags(1) + Key + Value
	entrySize := 7 + len(key)
	valPtrSize := page.ValuePtrSize
	if n.leafPackedValuePtr() {
		valPtrSize = page.PackedValuePtrSize
	}
	if flags&FlagPointer != 0 {
		entrySize += valPtrSize
	} else {
		entrySize += len(value)
	}

	// Check directory space (2 bytes) + Entry space
	// Need to check if we are updating existing key (no new directory slot)
	// or inserting new (new directory slot).
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}

	needed := entrySize
	if !found {
		needed += DirectoryEntrySize
	}

	if n.FreeSpace() < needed {
		// Try to reclaim space from heap holes created by overwrites.
		if err := n.Compact(); err != nil {
			return err
		}
		if n.FreeSpace() < needed {
			return ErrNodeFull
		}
	}

	// Prepare Entry Data
	buf := make([]byte, entrySize)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(key)))

	// ValueLen
	// If Pointer, we store logical length? Or ignore?
	// Spec: "If Pointer: 16-byte ValuePtr (ValueLen ignored)."
	// But usually we want to store the logical length of the value.
	// The `valPtr.Length` stores the physical record length (CRC+Key+Val).
	// The actual value length is not directly in `ValuePtr`.
	// For now, let's put len(value) if inline, or 0 if pointer (unless caller provides logical len).
	// The `AddLeafEntry` signature takes `value []byte` even for pointers?
	// If pointer, `value` is likely nil.
	// Let's assume 0 for pointer for now, or maybe we should pass logical length.
	// For this phase, we probably focus on structure.

	if flags&FlagPointer != 0 {
		// ValueLen ignored
		putUint32(buf[2:6], 0) // or logic length?
		buf[6] = flags
		copy(buf[7:], key)
		if n.leafPackedValuePtr() {
			page.EncodePackedValuePtr(buf[7+len(key):7+len(key)+valPtrSize], valPtr)
		} else {
			valPtr.Encode(buf[7+len(key) : 7+len(key)+valPtrSize])
		}
	} else {
		putUint32(buf[2:6], uint32(len(value)))
		buf[6] = flags
		copy(buf[7:], key)
		copy(buf[7+len(key):], value)
	}

	// Allocate in Heap
	// We place the new entry at the "bottom" of the free space.
	// Current implementation: scan for lowest offset.
	// Optimization: If we just appended, we know where it is.
	// But to be robust:

	// Find lowest current offset
	heapStart := int(page.PageSize)
	count := n.Count()
	for k := uint16(0); k < count; k++ {
		// Don't check the one we are replacing yet
		off := getUint16(n.data[NodeHeaderSize+int(k)*2:])
		if int(off) < heapStart && int(off) != 0 {
			heapStart = int(off)
		}
	}

	newOffset := heapStart - entrySize
	// Directory ends at NodeHeaderSize + count*2
	// If !found, count+1.
	dirEnd := NodeHeaderSize + int(count)*2
	if !found {
		dirEnd += 2
	}

	if newOffset < dirEnd {
		return ErrNodeFull // Should have been caught by FreeSpace(), but double check
	}

	// Write Data
	copy(n.data[newOffset:], buf)

	// Update Directory
	if found {
		// Update existing slot to point to new location
		// OLD space becomes dead (fragmentation).
		// Ideally we compact.
		n.setOffset(idx, uint16(newOffset))
	} else {
		// Shift directory
		// Shift [idx..count-1] to [idx+1..count]
		// Each entry is 2 bytes.
		// Source: NodeHeaderSize + idx*2
		// Dest: NodeHeaderSize + (idx+1)*2
		// Len: (count - idx) * 2

		srcPos := NodeHeaderSize + int(idx)*2
		destPos := srcPos + 2
		moveLen := int(count-idx) * 2

		if moveLen > 0 {
			copy(n.data[destPos:destPos+moveLen], n.data[srcPos:srcPos+moveLen])
		}

		n.setOffset(idx, uint16(newOffset))
		n.SetCount(count + 1)
	}

	n.UpdateChecksum()
	return nil
}

func (n *Node) addLeafEntryColumnarV2Rebuild(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}

	count := n.Count()
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafColumnar:   true,
		PackedValuePtr: n.leafPackedValuePtr(),
	})
	b.SetPageID(n.PageID())

	inserted := false
	for i := uint16(0); i < count; i++ {
		if !inserted && i == idx {
			if err := b.AddLeafEntry(key, value, flags, valPtr); err != nil {
				return err
			}
			inserted = true
			if found {
				continue
			}
		}

		k, v, ptr, f, err := n.GetLeafEntryView(i)
		if err != nil {
			return err
		}
		if err := b.AddLeafEntry(k, v, f, ptr); err != nil {
			return err
		}
	}
	if !inserted {
		if err := b.AddLeafEntry(key, value, flags, valPtr); err != nil {
			return err
		}
	}

	newNode := b.Finish()
	copy(n.data, newNode.data)
	flags16 := getUint16(n.data[12:14])
	n.ptype = page.PageType(flags16 & pageTypeMask)
	n.count = getUint16(n.data[14:16])
	n.leafValid = false
	return nil
}

func (n *Node) addLeafEntryColumnarPrefixV2Rebuild(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}

	count := n.Count()
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        n.leafPackedValuePtr(),
	})
	b.SetPageID(n.PageID())

	inserted := false
	for i := uint16(0); i < count; i++ {
		if !inserted && i == idx {
			if err := b.AddLeafEntry(key, value, flags, valPtr); err != nil {
				return err
			}
			inserted = true
			if found {
				continue
			}
		}

		k, v, ptr, f, err := n.GetLeafEntryView(i)
		if err != nil {
			return err
		}
		if err := b.AddLeafEntry(k, v, f, ptr); err != nil {
			return err
		}
	}
	if !inserted {
		if err := b.AddLeafEntry(key, value, flags, valPtr); err != nil {
			return err
		}
	}

	newNode := b.Finish()
	copy(n.data, newNode.data)
	flags16 := getUint16(n.data[12:14])
	n.ptype = page.PageType(flags16 & pageTypeMask)
	n.count = getUint16(n.data[14:16])
	n.leafValid = false
	return nil
}

func (n *Node) addLeafEntryColumnar(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	valPtrSize := page.ValuePtrSize
	if n.leafPackedValuePtr() {
		valPtrSize = page.PackedValuePtrSize
	}
	valLen := 0
	valSize := 0
	entrySize := leafColumnarHeaderSize + len(key)
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
		entrySize += valSize
	} else if flags&FlagTombstone == 0 {
		valLen = len(value)
		valSize = valLen
		entrySize += valSize
	}

	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}

	needed := entrySize
	if !found {
		needed += DirectoryEntrySize
	}

	if n.FreeSpace() < needed {
		if err := n.Compact(); err != nil {
			return err
		}
		if n.FreeSpace() < needed {
			return ErrNodeFull
		}
	}

	buf := make([]byte, entrySize)
	writeLeafColumnarHeader(buf[:leafColumnarHeaderSize], len(key), valLen, flags)

	valueStart := leafColumnarHeaderSize
	if flags&FlagPointer != 0 {
		if n.leafPackedValuePtr() {
			page.EncodePackedValuePtr(buf[valueStart:valueStart+valPtrSize], valPtr)
		} else {
			valPtr.Encode(buf[valueStart : valueStart+valPtrSize])
		}
	} else if flags&FlagTombstone == 0 {
		copy(buf[valueStart:valueStart+valLen], value)
	}

	keyStart := valueStart + valSize
	copy(buf[keyStart:keyStart+len(key)], key)

	heapStart := int(page.PageSize)
	count := n.Count()
	for k := uint16(0); k < count; k++ {
		off := getUint16(n.data[NodeHeaderSize+int(k)*2:])
		if int(off) < heapStart && int(off) != 0 {
			heapStart = int(off)
		}
	}

	newOffset := heapStart - entrySize
	dirEnd := NodeHeaderSize + int(count)*2
	if !found {
		dirEnd += 2
	}

	if newOffset < dirEnd {
		return ErrNodeFull
	}

	copy(n.data[newOffset:], buf)

	if found {
		n.setOffset(idx, uint16(newOffset))
	} else {
		srcPos := NodeHeaderSize + int(idx)*2
		destPos := srcPos + 2
		moveLen := int(count-idx) * 2

		if moveLen > 0 {
			copy(n.data[destPos:destPos+moveLen], n.data[srcPos:srcPos+moveLen])
		}

		n.setOffset(idx, uint16(newOffset))
		n.SetCount(count + 1)
	}

	n.UpdateChecksum()
	return nil
}

func (n *Node) addLeafEntryPrefixCompressed(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	newEntry := LeafEntry{
		Key:      append([]byte(nil), key...),
		ValuePtr: valPtr,
		Flags:    flags,
	}
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		newEntry.Value = append([]byte(nil), value...)
	}

	entries := make([]LeafEntry, 0, int(n.Count())+1)
	inserted := false
	for i := uint16(0); i < n.Count(); i++ {
		k, v, ptr, f, err := n.GetLeafEntryView(i)
		if err != nil {
			return err
		}
		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			if !inserted {
				entries = append(entries, newEntry)
				inserted = true
			}
			continue
		}
		if !inserted && cmp > 0 {
			entries = append(entries, newEntry)
			inserted = true
		}
		entry := LeafEntry{
			Key:      append([]byte(nil), k...),
			ValuePtr: ptr,
			Flags:    f,
		}
		if f&FlagPointer == 0 && f&FlagTombstone == 0 {
			entry.Value = append([]byte(nil), v...)
		}
		entries = append(entries, entry)
	}
	if !inserted {
		entries = append(entries, newEntry)
	}

	b := NewBuilderWithOptions(n.data, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		PackedValuePtr:        n.leafPackedValuePtr(),
	})
	b.SetPageID(n.PageID())
	for _, entry := range entries {
		if err := b.AddLeafEntry(entry.Key, entry.Value, entry.Flags, entry.ValuePtr); err != nil {
			return err
		}
	}
	b.Finish()
	n.setRawFlags(getUint16(n.data[12:14]))
	n.count = getUint16(n.data[14:16])
	return nil
}
