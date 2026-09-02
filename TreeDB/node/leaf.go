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
	Revision page.EntryRevision
}

type leafEntryLayout struct {
	headerSize int
	prefixLen  int
	suffixLen  int
	keyLen     int
	valLen     int
	flags      byte
	revision   page.EntryRevision
	keyOff     int
	valOff     int
}

func decodeEntryRevision(data []byte, off int, enabled bool) (page.EntryRevision, error) {
	if !enabled {
		return page.LegacyEntryRevision, nil
	}
	if off < 0 || off+page.EntryRevisionSize > len(data) {
		return page.LegacyEntryRevision, ErrCorruptedNode
	}
	return page.EntryRevision(binary.LittleEndian.Uint64(data[off : off+page.EntryRevisionSize])), nil
}

func compareLeafKey(a, b []byte) int {
	if len(a) == 8 && len(b) == 8 {
		av := binary.BigEndian.Uint64(a)
		bv := binary.BigEndian.Uint64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return bytes.Compare(a, b)
}

func compareSmallBigEndian(a, b []byte) (int, bool) {
	if len(a) != len(b) || len(a) > 8 {
		return 0, false
	}
	if len(a) == 0 {
		return 0, true
	}

	var av uint64
	var bv uint64
	for i := 0; i < len(a); i++ {
		av = (av << 8) | uint64(a[i])
		bv = (bv << 8) | uint64(b[i])
	}
	if av < bv {
		return -1, true
	}
	if av > bv {
		return 1, true
	}
	return 0, true
}

func composePrefixVirtualKeyU64(prev uint64, prefixLen int, suffix []byte) (uint64, bool) {
	if prefixLen < 0 || prefixLen > 8 || len(suffix) != 8-prefixLen {
		return 0, false
	}

	var suffixV uint64
	for i := 0; i < len(suffix); i++ {
		suffixV = (suffixV << 8) | uint64(suffix[i])
	}

	bits := uint((8 - prefixLen) * 8)
	var mask uint64
	if bits == 64 {
		mask = 0
	} else {
		mask = ^uint64(0) << bits
	}
	return (prev & mask) | suffixV, true
}

func (n *Node) ensureKeyScratch(size int) []byte {
	if size < 0 {
		return nil
	}
	if size == 0 {
		return []byte{}
	}
	if cap(n.keyScratch) < size {
		nextCap := cap(n.keyScratch)
		if nextCap < 64 {
			nextCap = 64
		}
		// Grow geometrically so repeated small key-length increases do not
		// allocate on every decode step.
		for nextCap < size {
			if nextCap > int(^uint(0)>>1)/2 {
				nextCap = size
				break
			}
			nextCap *= 2
		}
		n.keyScratch = make([]byte, nextCap)
	}
	return n.keyScratch[:size]
}

func (n *Node) leafEntryLayoutAt(offset int) (leafEntryLayout, error) {
	if offset < NodeHeaderSize || offset >= len(n.data) {
		return leafEntryLayout{}, ErrCorruptedNode
	}

	if n.leafPrefixCompressed() {
		if n.leafPrefixV2() {
			if n.leafColumnar() {
				// Combined columnar+prefix v2 does not store per-entry headers at
				// key offsets; key/value metadata is stored in top-level columns.
				return leafEntryLayout{}, ErrCorruptedNode
			}
			return parseLeafPrefixV2Layout(n.data, offset, n.leafEntryRevisions())
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
		valSize := valLen
		if flags&FlagPointer != 0 {
			if n.leafPackedValuePtr() {
				valSize = page.PackedValuePtrSize
			} else {
				valSize = page.ValuePtrSize
			}
		} else if flags&FlagTombstone != 0 {
			valSize = 0
		}
		keyOff := 9
		revision, err := decodeEntryRevision(n.data, offset+keyOff+suffixLen+valSize, n.leafEntryRevisions())
		if err != nil {
			return leafEntryLayout{}, err
		}
		return leafEntryLayout{
			headerSize: 9,
			prefixLen:  prefixLen,
			suffixLen:  suffixLen,
			keyLen:     prefixLen + suffixLen,
			valLen:     valLen,
			flags:      flags,
			revision:   revision,
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
		return parseLeafColumnarLayout(n.data, offset, valPtrSize, n.leafEntryRevisions())
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
	valSize := valLen
	if flags&FlagPointer != 0 {
		if n.leafPackedValuePtr() {
			valSize = page.PackedValuePtrSize
		} else {
			valSize = page.ValuePtrSize
		}
	} else if flags&FlagTombstone != 0 {
		valSize = 0
	}
	keyOff := 7
	revision, err := decodeEntryRevision(n.data, offset+keyOff+keyLen+valSize, n.leafEntryRevisions())
	if err != nil {
		return leafEntryLayout{}, err
	}
	return leafEntryLayout{
		headerSize: 7,
		suffixLen:  keyLen,
		keyLen:     keyLen,
		valLen:     valLen,
		flags:      flags,
		revision:   revision,
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
	key, val, valPtr, flags, _, err = n.GetLeafEntryViewWithRevision(index)
	return key, val, valPtr, flags, err
}

// GetLeafEntryViewWithRevision returns a zero-copy leaf entry view plus the
// native entry revision stored in the same leaf entry.
func (n *Node) GetLeafEntryViewWithRevision(index uint16) (key []byte, val []byte, valPtr page.ValuePtr, flags byte, revision page.EntryRevision, err error) {
	if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		key, val, valPtr, flags, err = n.getLeafEntryViewColumnarPrefixV2(index)
		if err != nil {
			return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, err
		}
		revision, err = n.leafColumnarPrefixV2RevisionAt(index)
		return key, val, valPtr, flags, revision, err
	}
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		key, val, valPtr, flags, err = n.getLeafEntryViewColumnarV2(index)
		if err != nil {
			return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, err
		}
		revision, err = n.leafColumnarV2RevisionAt(index)
		return key, val, valPtr, flags, revision, err
	}

	layout, entryStart := leafEntryLayout{}, 0
	key, layout, entryStart, err = n.leafEntryKeyAt(index)
	if err != nil {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, err
	}
	flags = layout.flags
	revision = layout.revision

	if flags&FlagTombstone != 0 {
		return key, nil, page.ValuePtr{}, flags, revision, nil
	}

	valueStart := entryStart + layout.valOff
	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		if n.leafPackedValuePtr() {
			valPtrSize = page.PackedValuePtrSize
		}
		if valueStart+valPtrSize > len(n.data) {
			return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, ErrCorruptedNode
		}
		if n.leafPackedValuePtr() {
			valPtr = page.DecodePackedValuePtr(n.data[valueStart : valueStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(n.data[valueStart : valueStart+valPtrSize])
		}
		return key, nil, valPtr, flags, revision, nil
	}

	// Inline
	if valueStart+layout.valLen > len(n.data) {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, ErrCorruptedNode
	}
	val = n.data[valueStart : valueStart+layout.valLen]
	return key, val, page.ValuePtr{}, flags, revision, nil
}

// GetLeafKeyFlagsView returns the key and flags for the entry at index without
// decoding the value bytes/pointer payload.
func (n *Node) GetLeafKeyFlagsView(index uint16) (key []byte, flags byte, err error) {
	if n.Type() != page.PageTypeLeaf {
		return nil, 0, ErrInvalidType
	}
	if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		return n.getLeafKeyFlagsViewColumnarPrefixV2(index)
	}
	if n.leafPackedValuePtr() && !n.leafPrefixCompressed() && !n.leafColumnar() {
		offset, err := n.getOffset(index)
		if err != nil {
			return nil, 0, err
		}
		if int(offset) >= len(n.data) {
			return nil, 0, ErrCorruptedNode
		}
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+7 > len(n.data) {
			return nil, 0, ErrCorruptedNode
		}
		keyLen := int(getUint16(n.data[ptr : ptr+2]))
		flags = n.data[ptr+6]
		keyStart := ptr + 7
		keyEnd := keyStart + keyLen
		if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
			return nil, 0, ErrCorruptedNode
		}
		return n.data[keyStart:keyEnd], flags, nil
	}
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		count := n.Count()
		if index >= count {
			return nil, 0, ErrCorruptedNode
		}
		key, err = n.leafColumnarKeyViewAtIndex(int(index))
		if err != nil {
			return nil, 0, err
		}
		flagsStart := NodeHeaderSize + int(count)*DirectoryEntrySize + int(count)*DirectoryEntrySize
		flagsOff := flagsStart + int(index)
		if flagsOff >= len(n.data) {
			return nil, 0, ErrCorruptedNode
		}
		return key, n.data[flagsOff], nil
	}

	key, layout, _, err := n.leafEntryKeyAt(index)
	if err != nil {
		return nil, 0, err
	}
	return key, layout.flags, nil
}

// GetLeafValueView returns the value (inline or pointer) at the given index
// without reconstructing the key. This is useful for point reads after SearchLeaf.
func (n *Node) GetLeafValueView(index uint16) (val []byte, valPtr page.ValuePtr, flags byte, err error) {
	if n.Type() != page.PageTypeLeaf {
		return nil, page.ValuePtr{}, 0, ErrInvalidType
	}

	if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		return n.getLeafValueViewColumnarPrefixV2(index)
	}
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		return n.getLeafValueViewColumnarV2(index)
	}
	if n.leafPackedValuePtr() && !n.leafPrefixCompressed() && !n.leafColumnar() {
		offset, err := n.getOffset(index)
		if err != nil {
			return nil, page.ValuePtr{}, 0, err
		}
		if int(offset) >= len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		entryStart := int(offset)
		if entryStart < NodeHeaderSize || entryStart+7 > len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		keyLen := int(getUint16(n.data[entryStart : entryStart+2]))
		flags = n.data[entryStart+6]
		valueStart := entryStart + 7 + keyLen
		if valueStart < NodeHeaderSize || valueStart > len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}

		if flags&FlagTombstone != 0 {
			return nil, page.ValuePtr{}, flags, nil
		}
		if flags&FlagPointer != 0 {
			if valueStart+page.PackedValuePtrSize > len(n.data) {
				return nil, page.ValuePtr{}, 0, ErrCorruptedNode
			}
			return nil, page.DecodePackedValuePtr(n.data[valueStart : valueStart+page.PackedValuePtrSize]), flags, nil
		}

		valLenU32 := binary.LittleEndian.Uint32(n.data[entryStart+2 : entryStart+6])
		if uint64(valLenU32) > uint64(math.MaxInt) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valLen := int(valLenU32)
		if valueStart+valLen > len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		return n.data[valueStart : valueStart+valLen], page.ValuePtr{}, flags, nil
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
	if n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		return n.updateLeafValuePtrColumnarPrefixV2(index, oldPtr, newPtr)
	}
	if n.leafColumnar() && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		count := n.Count()
		if index >= count {
			return false, ErrCorruptedNode
		}

		data := n.data
		keyDirStart, valDirStart, flagsStart, _, headerEnd := n.leafColumnarV2MetaOffsets(count)
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
	keyView, valView, valPtr, flags, revision, err := n.GetLeafEntryViewWithRevision(index)
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
		Revision: revision,
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
	fixedBE8 := len(key) == 8
	targetBE8 := uint64(0)
	if fixedBE8 {
		targetBE8 = getUint64BEAt(key, 0)
	}
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

			cmp := 0
			if fixedBE8 && keyLen == 8 {
				entryBE8 := getUint64BEAt(data, keyPtr)
				if entryBE8 < targetBE8 {
					cmp = -1
				} else if entryBE8 > targetBE8 {
					cmp = 1
				}
			} else {
				cmp = compareLeafKey(data[keyPtr:keyPtr+int(keyLen)], key)
			}
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
		cmp := 0
		if fixedBE8 && keyLen == 8 {
			entryBE8 := getUint64BEAt(data, keyPtr)
			if entryBE8 < targetBE8 {
				cmp = -1
			} else if entryBE8 > targetBE8 {
				cmp = 1
			}
		} else {
			cmp = compareLeafKey(data[keyPtr:keyPtr+int(keyLen)], key)
		}
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
		if fixedBE8 && keyLen == 8 {
			if getUint64BEAt(data, keyPtr) == targetBE8 {
				return uint16(i), true, nil
			}
		} else {
			if compareLeafKey(data[keyPtr:keyPtr+int(keyLen)], key) == 0 {
				return uint16(i), true, nil
			}
		}
	}

	return uint16(i), false, nil
}

func (n *Node) leafColumnarV2MetaOffsets(count uint16) (keyDirStart, valDirStart, flagsStart, revisionsStart, headerEnd int) {
	c := int(count)
	keyDirStart = NodeHeaderSize
	valDirStart = keyDirStart + c*DirectoryEntrySize
	flagsStart = valDirStart + c*DirectoryEntrySize
	revisionsStart = flagsStart + c
	headerEnd = revisionsStart
	if n.leafEntryRevisions() {
		headerEnd += c * page.EntryRevisionSize
	}
	return keyDirStart, valDirStart, flagsStart, revisionsStart, headerEnd
}

func (n *Node) leafColumnarV2RevisionAt(index uint16) (page.EntryRevision, error) {
	if !n.leafEntryRevisions() {
		return page.LegacyEntryRevision, nil
	}
	count := n.Count()
	if index >= count {
		return page.LegacyEntryRevision, ErrCorruptedNode
	}
	_, _, _, revisionsStart, headerEnd := n.leafColumnarV2MetaOffsets(count)
	if headerEnd > len(n.data) {
		return page.LegacyEntryRevision, ErrCorruptedNode
	}
	return decodeEntryRevision(n.data, revisionsStart+int(index)*page.EntryRevisionSize, true)
}

func (n *Node) leafColumnarKeyViewAtIndex(idx int) ([]byte, error) {
	data := n.data
	if n.leafColumnarV2() {
		count := n.Count()
		if idx < 0 || idx >= int(count) {
			return nil, ErrCorruptedNode
		}
		keyDirStart, _, _, _, headerEnd := n.leafColumnarV2MetaOffsets(count)
		keyDirOff := NodeHeaderSize + idx*2
		nextDirOff := keyDirOff + 2
		if headerEnd > len(data) || nextDirOff > len(data) {
			return nil, ErrCorruptedNode
		}
		keyDirOff = keyDirStart + idx*DirectoryEntrySize
		nextDirOff = keyDirOff + DirectoryEntrySize

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
	keyDirStart, valDirStart, flagsStart, _, headerEnd := n.leafColumnarV2MetaOffsets(count)
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

	keysStart := int(getUint16At(data, keyDirStart))
	if keysStart < headerEnd || keysStart > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	valStartOff := valDirStart + int(index)*2
	if valStartOff+2 > len(data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	valStart := int(getUint16At(data, valStartOff))
	valEnd := keysStart
	if index+1 < count {
		nextValOff := valStartOff + 2
		if nextValOff+2 > len(data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valEnd = int(getUint16At(data, nextValOff))
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
	keyDirStart, valDirStart, flagsStart, _, headerEnd := n.leafColumnarV2MetaOffsets(count)
	if headerEnd > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	flags = data[flagsStart+int(index)]
	if flags&FlagTombstone != 0 {
		return nil, page.ValuePtr{}, flags, nil
	}

	keysStart := int(getUint16At(data, keyDirStart))
	if keysStart < headerEnd || keysStart > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}

	valStartOff := valDirStart + int(index)*2
	if valStartOff+2 > len(data) {
		return nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	valStart := int(getUint16At(data, valStartOff))
	valEnd := keysStart
	if index+1 < count {
		nextValOff := valStartOff + 2
		if nextValOff+2 > len(data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valEnd = int(getUint16At(data, nextValOff))
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

func (n *Node) leafColumnarPrefixV2EnsureMeta() error {
	count := n.Count()
	if count == 0 {
		n.leafColPrefixMetaValid = true
		n.leafColPrefixMetaCount = 0
		n.leafColPrefixValDirStart = NodeHeaderSize
		n.leafColPrefixFlagsStart = NodeHeaderSize
		n.leafColPrefixPrefixStart = NodeHeaderSize
		n.leafColPrefixRevisionStart = NodeHeaderSize
		n.leafColPrefixHeaderEnd = NodeHeaderSize
		n.leafColPrefixKeysBlobBase = len(n.data)
		return nil
	}
	if n.leafColPrefixMetaValid && n.leafColPrefixMetaCount == count {
		return nil
	}
	valDirStart := NodeHeaderSize + int(count)*DirectoryEntrySize
	flagsStart := valDirStart + int(count)*DirectoryEntrySize
	prefixStart := flagsStart + int(count)
	revisionStart := prefixStart + int(count)*DirectoryEntrySize
	headerEnd := revisionStart
	if n.leafEntryRevisions() {
		headerEnd += int(count) * page.EntryRevisionSize
	}
	if headerEnd > len(n.data) {
		return ErrCorruptedNode
	}
	keysStart := int(getUint16At(n.data, NodeHeaderSize))
	if keysStart < headerEnd || keysStart > len(n.data) {
		return ErrCorruptedNode
	}
	n.leafColPrefixMetaValid = true
	n.leafColPrefixMetaCount = count
	n.leafColPrefixValDirStart = valDirStart
	n.leafColPrefixFlagsStart = flagsStart
	n.leafColPrefixPrefixStart = prefixStart
	n.leafColPrefixRevisionStart = revisionStart
	n.leafColPrefixHeaderEnd = headerEnd
	n.leafColPrefixKeysBlobBase = keysStart
	return nil
}

func (n *Node) leafColumnarPrefixV2RevisionAt(index uint16) (page.EntryRevision, error) {
	if !n.leafEntryRevisions() {
		return page.LegacyEntryRevision, nil
	}
	count := n.Count()
	if index >= count {
		return page.LegacyEntryRevision, ErrCorruptedNode
	}
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		return page.LegacyEntryRevision, err
	}
	return decodeEntryRevision(n.data, n.leafColPrefixRevisionStart+int(index)*page.EntryRevisionSize, true)
}

func (n *Node) leafColumnarPrefixV2KeyMetaAt(index uint16) (prefixLen int, suffix []byte, flags byte, err error) {
	count := n.Count()
	if index >= count {
		return 0, nil, 0, ErrCorruptedNode
	}
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		return 0, nil, 0, err
	}
	data := n.data

	keyOff := NodeHeaderSize + int(index)*2
	if keyOff+2 > len(data) {
		return 0, nil, 0, ErrCorruptedNode
	}
	keyStart := int(getUint16At(data, keyOff))
	keyEnd := len(data)
	if index+1 < count {
		nextKeyOff := keyOff + 2
		if nextKeyOff+2 > len(data) {
			return 0, nil, 0, ErrCorruptedNode
		}
		keyEnd = int(getUint16At(data, nextKeyOff))
	}
	if keyStart < n.leafColPrefixKeysBlobBase || keyEnd < keyStart || keyEnd > len(data) {
		return 0, nil, 0, ErrCorruptedNode
	}

	flags = data[n.leafColPrefixFlagsStart+int(index)]
	prefixOff := n.leafColPrefixPrefixStart + int(index)*2
	if prefixOff+2 > len(data) {
		return 0, nil, 0, ErrCorruptedNode
	}
	prefixLen = int(getUint16At(data, prefixOff))
	suffix = data[keyStart:keyEnd]
	if prefixLen < 0 || prefixLen > math.MaxInt-len(suffix) {
		return 0, nil, 0, ErrCorruptedNode
	}
	return prefixLen, suffix, flags, nil
}

func (n *Node) leafColumnarPrefixV2ValueMetaAt(index uint16) (flags byte, valStart int, valEnd int, err error) {
	count := n.Count()
	if index >= count {
		return 0, 0, 0, ErrCorruptedNode
	}
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		return 0, 0, 0, err
	}
	data := n.data

	flags = data[n.leafColPrefixFlagsStart+int(index)]

	valOff := n.leafColPrefixValDirStart + int(index)*2
	if valOff+2 > len(data) {
		return 0, 0, 0, ErrCorruptedNode
	}
	valStart = int(getUint16At(data, valOff))
	valEnd = n.leafColPrefixKeysBlobBase
	if index+1 < count {
		nextValOff := valOff + 2
		if nextValOff+2 > len(data) {
			return 0, 0, 0, ErrCorruptedNode
		}
		valEnd = int(getUint16At(data, nextValOff))
	}
	if valStart < n.leafColPrefixHeaderEnd || valEnd < valStart || valEnd > n.leafColPrefixKeysBlobBase {
		return 0, 0, 0, ErrCorruptedNode
	}
	return flags, valStart, valEnd, nil
}

func (n *Node) leafColumnarPrefixV2FullKeyFlagsAt(index uint16) ([]byte, byte, error) {
	count := n.Count()
	if index >= count {
		return nil, 0, ErrCorruptedNode
	}
	if n.leafValid && n.leafIndex == index {
		return n.leafKey, n.leafFlags, nil
	}

	if n.leafValid && n.leafIndex+1 == index {
		prefixLen, suffix, flags, err := n.leafColumnarPrefixV2KeyMetaAt(index)
		if err != nil {
			return nil, 0, err
		}
		if index%leafPrefixRestartInterval == 0 && prefixLen != 0 {
			return nil, 0, ErrCorruptedNode
		}
		if prefixLen > len(n.leafKey) {
			return nil, 0, ErrCorruptedNode
		}

		var key []byte
		if prefixLen == 0 {
			key = suffix
		} else {
			keyLen := prefixLen + len(suffix)
			key = n.ensureKeyScratch(keyLen)
			sameBacking := len(n.leafKey) > 0 && len(key) > 0 && &n.leafKey[0] == &key[0]
			if !sameBacking && prefixLen > 0 {
				copy(key, n.leafKey[:prefixLen])
			}
			copy(key[prefixLen:], suffix)
		}
		n.leafKey = key
		n.leafIndex = index
		n.leafFlags = flags
		n.leafValid = true
		return key, flags, nil
	}

	restart := index - (index % leafPrefixRestartInterval)
	var prev []byte
	for i := restart; i <= index; i++ {
		prefixLen, suffix, flags, err := n.leafColumnarPrefixV2KeyMetaAt(i)
		if err != nil {
			return nil, 0, err
		}
		var key []byte
		if i == restart {
			if prefixLen != 0 {
				return nil, 0, ErrCorruptedNode
			}
			key = suffix
		} else {
			if prefixLen > len(prev) {
				return nil, 0, ErrCorruptedNode
			}
			keyLen := prefixLen + len(suffix)
			key = n.ensureKeyScratch(keyLen)
			sameBacking := len(prev) > 0 && len(key) > 0 && &prev[0] == &key[0]
			if !sameBacking && prefixLen > 0 {
				copy(key, prev[:prefixLen])
			}
			copy(key[prefixLen:], suffix)
		}
		prev = key
		if i == index {
			n.leafKey = key
			n.leafIndex = index
			n.leafFlags = flags
			n.leafValid = true
			return key, flags, nil
		}
	}
	return nil, 0, ErrCorruptedNode
}

func (n *Node) leafColumnarPrefixV2FullKeyAt(index uint16) ([]byte, error) {
	key, _, err := n.leafColumnarPrefixV2FullKeyFlagsAt(index)
	return key, err
}

func (n *Node) getLeafKeyFlagsViewColumnarPrefixV2(index uint16) (key []byte, flags byte, err error) {
	return n.leafColumnarPrefixV2FullKeyFlagsAt(index)
}

func (n *Node) getLeafValueViewColumnarPrefixV2(index uint16) (val []byte, valPtr page.ValuePtr, flags byte, err error) {
	flags, valStart, valEnd, err := n.leafColumnarPrefixV2ValueMetaAt(index)
	if err != nil {
		return nil, page.ValuePtr{}, 0, err
	}
	if flags&FlagTombstone != 0 {
		return nil, page.ValuePtr{}, flags, nil
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
		if valStart+valPtrSize > len(n.data) {
			return nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		if packed {
			valPtr = page.DecodePackedValuePtr(n.data[valStart : valStart+valPtrSize])
		} else {
			valPtr = page.DecodeValuePtr(n.data[valStart : valStart+valPtrSize])
		}
		return nil, valPtr, flags, nil
	}
	return n.data[valStart:valEnd], page.ValuePtr{}, flags, nil
}

func (n *Node) getLeafEntryViewColumnarPrefixV2(index uint16) (key []byte, val []byte, valPtr page.ValuePtr, flags byte, err error) {
	key, err = n.leafColumnarPrefixV2FullKeyAt(index)
	if err != nil {
		return nil, nil, page.ValuePtr{}, 0, err
	}
	val, valPtr, flags, err = n.getLeafValueViewColumnarPrefixV2(index)
	if err != nil {
		return nil, nil, page.ValuePtr{}, 0, err
	}
	return key, val, valPtr, flags, nil
}

func (n *Node) updateLeafValuePtrColumnarPrefixV2(index uint16, oldPtr, newPtr page.ValuePtr) (bool, error) {
	flags, valStart, valEnd, err := n.leafColumnarPrefixV2ValueMetaAt(index)
	if err != nil {
		return false, err
	}
	if flags&FlagTombstone != 0 || flags&FlagPointer == 0 {
		return false, nil
	}

	valPtrSize := page.ValuePtrSize
	packed := n.leafPackedValuePtr()
	if packed {
		valPtrSize = page.PackedValuePtrSize
	}
	if valEnd-valStart != valPtrSize {
		return false, ErrCorruptedNode
	}
	if valStart+valPtrSize > len(n.data) {
		return false, ErrCorruptedNode
	}

	var cur page.ValuePtr
	if packed {
		cur = page.DecodePackedValuePtr(n.data[valStart : valStart+valPtrSize])
	} else {
		cur = page.DecodeValuePtr(n.data[valStart : valStart+valPtrSize])
	}
	if cur != oldPtr {
		return false, nil
	}

	if packed {
		page.EncodePackedValuePtr(n.data[valStart:valStart+valPtrSize], newPtr)
	} else {
		newPtr.Encode(n.data[valStart : valStart+valPtrSize])
	}
	n.UpdateChecksum()
	return true, nil
}

func (n *Node) searchLeafColumnar(key []byte) (uint16, bool, error) {
	if n.leafColumnarV2() {
		return n.searchLeafColumnarV2(key)
	}

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
			cmp := compareLeafKey(k, key)
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
		cmp := compareLeafKey(k, key)
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
		if compareLeafKey(k, key) == 0 {
			return uint16(i), true, nil
		}
	}
	return uint16(i), false, nil
}

func (n *Node) searchLeafColumnarV2(key []byte) (uint16, bool, error) {
	count := int(n.Count())
	if count == 0 {
		return 0, false, nil
	}

	data := n.data
	keyDirStart, _, _, _, headerEnd := n.leafColumnarV2MetaOffsets(uint16(count))
	if headerEnd > len(data) {
		return 0, false, ErrCorruptedNode
	}

	keyAt := func(idx int) ([]byte, error) {
		if idx < 0 || idx >= count {
			return nil, ErrCorruptedNode
		}
		keyOff := keyDirStart + idx*2
		nextKeyOff := keyOff + 2
		if nextKeyOff > len(data) {
			return nil, ErrCorruptedNode
		}
		keyStart := int(getUint16(data[keyOff : keyOff+2]))
		keyEnd := len(data)
		if idx+1 < count {
			if nextKeyOff+2 > len(data) {
				return nil, ErrCorruptedNode
			}
			keyEnd = int(getUint16(data[nextKeyOff : nextKeyOff+2]))
		}
		if keyStart < headerEnd || keyEnd < keyStart || keyEnd > len(data) {
			return nil, ErrCorruptedNode
		}
		return data[keyStart:keyEnd], nil
	}

	if count <= smallSearchThreshold {
		for idx := 0; idx < count; idx++ {
			k, err := keyAt(idx)
			if err != nil {
				return 0, false, err
			}
			cmp := compareLeafKey(k, key)
			if cmp >= 0 {
				return uint16(idx), cmp == 0, nil
			}
		}
		return uint16(count), false, nil
	}

	i, j := 0, count
	for i < j {
		h := int(uint(i+j) >> 1)
		k, err := keyAt(h)
		if err != nil {
			return 0, false, err
		}
		if compareLeafKey(k, key) < 0 {
			i = h + 1
		} else {
			j = h
		}
	}

	if i < count {
		k, err := keyAt(i)
		if err != nil {
			return 0, false, err
		}
		if compareLeafKey(k, key) == 0 {
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
		if cmp, ok := compareSmallBigEndian(prevKey[:n], target[:n]); ok {
			if cmp != 0 {
				return cmp
			}
		} else if cmp := bytes.Compare(prevKey[:n], target[:n]); cmp != 0 {
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
		if cmp, ok := compareSmallBigEndian(suffix[:n], t[:n]); ok {
			if cmp != 0 {
				return cmp
			}
		} else if cmp := bytes.Compare(suffix[:n], t[:n]); cmp != 0 {
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
	cmp := compareLeafKey(restartKey, target)
	if cmp >= 0 {
		return blockStart, cmp == 0, nil
	}

	if len(target) == 8 && len(restartKey) == 8 {
		targetU := binary.BigEndian.Uint64(target)
		prevU := binary.BigEndian.Uint64(restartKey)
		fastOK := true

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
			keyStart := ptr + layout.keyOff
			keyEnd := keyStart + layout.suffixLen
			if keyStart < NodeHeaderSize || keyEnd > len(n.data) {
				return 0, false, ErrCorruptedNode
			}
			suffix := n.data[keyStart:keyEnd]
			nextU, ok := composePrefixVirtualKeyU64(prevU, layout.prefixLen, suffix)
			if !ok {
				fastOK = false
				break
			}
			prevU = nextU
			if prevU >= targetU {
				return idx, prevU == targetU, nil
			}
		}
		if fastOK {
			return blockEnd, false, nil
		}
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
		if compareLeafKey(k, key) <= 0 {
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
	prefixLen, suffix, _, err = n.leafColumnarPrefixV2KeyMetaAt(index)
	if err != nil {
		return 0, nil, err
	}
	return prefixLen, suffix, nil
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

func leafColumnarPrefixV2KeyPartsAtFast(data []byte, count uint16, keysBlobBase int, prefixStart int, index uint16) (prefixLen int, suffix []byte, err error) {
	if index >= count {
		return 0, nil, ErrCorruptedNode
	}
	keyOff := NodeHeaderSize + int(index)*2
	if keyOff+2 > len(data) {
		return 0, nil, ErrCorruptedNode
	}
	keyStart := int(getUint16At(data, keyOff))
	keyEnd := len(data)
	if index+1 < count {
		nextKeyOff := keyOff + 2
		if nextKeyOff+2 > len(data) {
			return 0, nil, ErrCorruptedNode
		}
		keyEnd = int(getUint16At(data, nextKeyOff))
	}
	if keyStart < keysBlobBase || keyEnd < keyStart || keyEnd > len(data) {
		return 0, nil, ErrCorruptedNode
	}
	prefixOff := prefixStart + int(index)*2
	if prefixOff+2 > len(data) {
		return 0, nil, ErrCorruptedNode
	}
	prefixLen = int(getUint16At(data, prefixOff))
	suffix = data[keyStart:keyEnd]
	if prefixLen < 0 || prefixLen > math.MaxInt-len(suffix) {
		return 0, nil, ErrCorruptedNode
	}
	return prefixLen, suffix, nil
}

func (n *Node) searchLeafColumnarPrefixV2BlockWithMeta(data []byte, count uint16, keysBlobBase int, prefixStart int, blockStart, blockEnd uint16, target []byte) (uint16, bool, error) {
	if blockStart >= blockEnd {
		return blockEnd, false, nil
	}

	var stackScratch [128]byte
	restartKeyDirOff := NodeHeaderSize + int(blockStart)*2
	keyDirNeeded := NodeHeaderSize + int(blockEnd)*2
	if blockEnd < count {
		keyDirNeeded += 2
	}
	if restartKeyDirOff < NodeHeaderSize || keyDirNeeded > len(data) {
		return 0, false, ErrCorruptedNode
	}
	prefixDirNeeded := prefixStart + int(blockEnd)*2
	if prefixStart < NodeHeaderSize || prefixDirNeeded > len(data) {
		return 0, false, ErrCorruptedNode
	}
	restartStart := int(getUint16At(data, restartKeyDirOff))
	restartEnd := len(data)
	if blockStart+1 < count {
		restartEnd = int(getUint16At(data, restartKeyDirOff+2))
	}
	if restartStart < keysBlobBase || restartEnd < restartStart || restartEnd > len(data) {
		return 0, false, ErrCorruptedNode
	}
	restartPrefixOff := prefixStart + int(blockStart)*2
	if getUint16At(data, restartPrefixOff) != 0 {
		return 0, false, ErrCorruptedNode
	}
	restartKey := data[restartStart:restartEnd]
	cmp := compareLeafKey(restartKey, target)
	if cmp >= 0 {
		return blockStart, cmp == 0, nil
	}

	if len(target) == 8 && len(restartKey) == 8 {
		targetU := getUint64BEAt(target, 0)
		prevU := getUint64BEAt(data, restartStart)
		fastOK := true
		curStart := restartEnd
		nextKeyDirOff := restartKeyDirOff + 4
		prefixOff := prefixStart + int(blockStart+1)*2

		for idx := blockStart + 1; idx < blockEnd; idx++ {
			curEnd := len(data)
			if idx+1 < count {
				curEnd = int(getUint16At(data, nextKeyDirOff))
				nextKeyDirOff += 2
			}
			if curStart < keysBlobBase || curEnd < curStart || curEnd > len(data) {
				return 0, false, ErrCorruptedNode
			}
			prefixLen := int(getUint16At(data, prefixOff))
			prefixOff += 2

			suffix := data[curStart:curEnd]
			nextU, ok := composePrefixVirtualKeyU64(prevU, prefixLen, suffix)
			if !ok {
				fastOK = false
				break
			}
			prevU = nextU
			curStart = curEnd
			if prevU >= targetU {
				return idx, prevU == targetU, nil
			}
		}
		if fastOK {
			return blockEnd, false, nil
		}
	}

	prevKey := restartKey
	curStart := restartEnd
	nextKeyDirOff := restartKeyDirOff + 4
	prefixOff := prefixStart + int(blockStart+1)*2
	for idx := blockStart + 1; idx < blockEnd; idx++ {
		curEnd := len(data)
		if idx+1 < count {
			if nextKeyDirOff+2 > len(data) {
				return 0, false, ErrCorruptedNode
			}
			curEnd = int(getUint16At(data, nextKeyDirOff))
			nextKeyDirOff += 2
		}
		if curStart < keysBlobBase || curEnd < curStart || curEnd > len(data) {
			return 0, false, ErrCorruptedNode
		}
		if prefixOff+2 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		prefixLen := int(getUint16At(data, prefixOff))
		prefixOff += 2
		suffix := data[curStart:curEnd]
		curStart = curEnd

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
		var cur []byte
		if keyLen <= len(stackScratch) {
			cur = stackScratch[:keyLen]
		} else {
			cur = n.ensureKeyScratch(keyLen)
		}
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
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		return 0, false, err
	}

	data := n.data
	count := n.Count()
	keysBlobBase := n.leafColPrefixKeysBlobBase
	prefixStart := n.leafColPrefixPrefixStart
	if count == 0 {
		return 0, false, nil
	}
	keyDirEnd := NodeHeaderSize + int(count)*2
	if keyDirEnd > len(data) {
		return 0, false, ErrCorruptedNode
	}
	prefixDirEnd := prefixStart + int(count)*2
	if prefixStart < NodeHeaderSize || prefixDirEnd > len(data) {
		return 0, false, ErrCorruptedNode
	}
	if count <= smallSearchThreshold {
		return n.searchLeafColumnarPrefixV2BlockWithMeta(data, count, keysBlobBase, prefixStart, 0, count, key)
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
		keyOff := NodeHeaderSize + int(idx)*2
		keyStart := int(getUint16At(data, keyOff))
		keyEnd := len(data)
		if idx+1 < count {
			keyEnd = int(getUint16At(data, keyOff+2))
		}
		if keyStart < keysBlobBase || keyEnd < keyStart || keyEnd > len(data) {
			return 0, false, ErrCorruptedNode
		}
		restartPrefixLen := int(getUint16At(data, prefixStart+int(idx)*2))
		if restartPrefixLen != 0 {
			return 0, false, ErrCorruptedNode
		}
		k := data[keyStart:keyEnd]
		if compareLeafKey(k, key) <= 0 {
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

	return n.searchLeafColumnarPrefixV2BlockWithMeta(data, count, keysBlobBase, prefixStart, blockStart, blockEnd, key)
}

func (n *Node) searchLeafColumnarPrefixV2Block(blockStart, blockEnd uint16, target []byte) (uint16, bool, error) {
	if err := n.leafColumnarPrefixV2EnsureMeta(); err != nil {
		return 0, false, err
	}
	return n.searchLeafColumnarPrefixV2BlockWithMeta(
		n.data,
		n.Count(),
		n.leafColPrefixKeysBlobBase,
		n.leafColPrefixPrefixStart,
		blockStart,
		blockEnd,
		target,
	)
}

// AddLeafEntry inserts a new entry into the Leaf Node.
// It maintains the sorted order of keys.
// If the key already exists, it updates it (by rewriting the entry).
// NOTE: This implementation assumes simple append-to-heap and shift-directory.
// It DOES NOT handle fragmentation (holes in heap) yet.
// A full implementation would compact the heap if FreeSpace check fails but logical space exists.
func (n *Node) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	return n.AddLeafEntryWithRevision(key, value, flags, valPtr, page.LegacyEntryRevision)
}

// AddLeafEntryWithRevision inserts a leaf entry and stores the native entry
// revision when this page uses revision-bearing leaf format.
func (n *Node) AddLeafEntryWithRevision(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
	if n.Type() != page.PageTypeLeaf {
		return ErrInvalidType
	}
	if revision != page.LegacyEntryRevision && !n.leafEntryRevisions() {
		if n.Count() != 0 {
			return ErrInvalidType
		}
		n.setLeafEntryRevisions(true)
	}
	if n.leafColumnar() && n.leafPrefixCompressed() {
		return n.addLeafEntryColumnarPrefixV2Rebuild(key, value, flags, valPtr, revision)
	}
	if n.leafColumnar() {
		if n.leafColumnarV2() {
			return n.addLeafEntryColumnarV2Rebuild(key, value, flags, valPtr, revision)
		}
		return n.addLeafEntryColumnar(key, value, flags, valPtr, revision)
	}
	if n.leafPrefixCompressed() {
		return n.addLeafEntryPrefixCompressed(key, value, flags, valPtr, revision)
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
	if n.leafEntryRevisions() {
		entrySize += page.EntryRevisionSize
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
	if n.leafEntryRevisions() {
		valSize := len(value)
		if flags&FlagPointer != 0 {
			valSize = valPtrSize
		} else if flags&FlagTombstone != 0 {
			valSize = 0
		}
		revisionOff := 7 + len(key) + valSize
		binary.LittleEndian.PutUint64(buf[revisionOff:revisionOff+page.EntryRevisionSize], uint64(revision))
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

func (n *Node) addLeafEntryColumnarV2Rebuild(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
	idx, found, err := n.SearchLeaf(key)
	if err != nil {
		return err
	}

	count := n.Count()
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafColumnar:   true,
		PackedValuePtr: n.leafPackedValuePtr(),
		EntryRevisions: n.leafEntryRevisions(),
	})
	b.SetPageID(n.PageID())

	inserted := false
	for i := uint16(0); i < count; i++ {
		if !inserted && i == idx {
			if err := b.AddLeafEntryWithRevision(key, value, flags, valPtr, revision); err != nil {
				return err
			}
			inserted = true
			if found {
				continue
			}
		}

		k, v, ptr, f, rev, err := n.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return err
		}
		if err := b.AddLeafEntryWithRevision(k, v, f, ptr, rev); err != nil {
			return err
		}
	}
	if !inserted {
		if err := b.AddLeafEntryWithRevision(key, value, flags, valPtr, revision); err != nil {
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

func (n *Node) addLeafEntryColumnarPrefixV2Rebuild(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
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
		EntryRevisions:        n.leafEntryRevisions(),
	})
	b.SetPageID(n.PageID())

	inserted := false
	for i := uint16(0); i < count; i++ {
		if !inserted && i == idx {
			if err := b.AddLeafEntryWithRevision(key, value, flags, valPtr, revision); err != nil {
				return err
			}
			inserted = true
			if found {
				continue
			}
		}

		k, v, ptr, f, rev, err := n.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return err
		}
		if err := b.AddLeafEntryWithRevision(k, v, f, ptr, rev); err != nil {
			return err
		}
	}
	if !inserted {
		if err := b.AddLeafEntryWithRevision(key, value, flags, valPtr, revision); err != nil {
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

func (n *Node) addLeafEntryColumnar(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
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
	if n.leafEntryRevisions() {
		entrySize += page.EntryRevisionSize
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
	if n.leafEntryRevisions() {
		revisionOff := keyStart + len(key)
		binary.LittleEndian.PutUint64(buf[revisionOff:revisionOff+page.EntryRevisionSize], uint64(revision))
	}

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

func (n *Node) addLeafEntryPrefixCompressed(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
	newEntry := LeafEntry{
		Key:      append([]byte(nil), key...),
		ValuePtr: valPtr,
		Flags:    flags,
		Revision: revision,
	}
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		newEntry.Value = append([]byte(nil), value...)
	}

	entries := make([]LeafEntry, 0, int(n.Count())+1)
	inserted := false
	for i := uint16(0); i < n.Count(); i++ {
		k, v, ptr, f, rev, err := n.GetLeafEntryViewWithRevision(i)
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
			Revision: rev,
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
		EntryRevisions:        n.leafEntryRevisions(),
	})
	b.SetPageID(n.PageID())
	for _, entry := range entries {
		if err := b.AddLeafEntryWithRevision(entry.Key, entry.Value, entry.Flags, entry.ValuePtr, entry.Revision); err != nil {
			return err
		}
	}
	b.Finish()
	n.setRawFlags(getUint16(n.data[12:14]))
	n.count = getUint16(n.data[14:16])
	return nil
}
