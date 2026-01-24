package node

import (
	"bytes"
	"encoding/binary"

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

	if n.leafColumnar() {
		return parseLeafColumnarLayout(n.data, offset)
	}

	if n.leafPrefixCompressed() {
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

	if n.leafColumnar() {
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
		if valueStart+page.ValuePtrSize > len(n.data) {
			return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
		}
		valPtr = page.DecodeValuePtr(n.data[valueStart : valueStart+page.ValuePtrSize])
		return key, nil, valPtr, flags, nil
	}

	// Inline
	if valueStart+layout.valLen > len(n.data) {
		return nil, nil, page.ValuePtr{}, 0, ErrCorruptedNode
	}
	val = n.data[valueStart : valueStart+layout.valLen]
	return key, val, page.ValuePtr{}, flags, nil
}

// UpdateLeafValuePtr updates the ValuePtr bytes for the entry at index if the
// entry is a pointer and currently matches oldPtr. It updates the page checksum
// on success.
//
// This is intended for maintenance operations (e.g. slab compaction) that need
// to swap pointers without rewriting the B+Tree structure.
func (n *Node) UpdateLeafValuePtr(index uint16, oldPtr, newPtr page.ValuePtr) (bool, error) {
	if n.Type() != page.PageTypeLeaf {
		return false, ErrInvalidType
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
	if ptr+page.ValuePtrSize > len(n.data) {
		return false, ErrCorruptedNode
	}

	cur := page.DecodeValuePtr(n.data[ptr : ptr+page.ValuePtrSize])
	if cur != oldPtr {
		return false, nil
	}

	newPtr.Encode(n.data[ptr : ptr+page.ValuePtrSize])
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
	if n.leafColumnar() {
		return n.searchLeafColumnar(key)
	}
	if n.leafPrefixCompressed() {
		return n.searchLeafPrefixCompressed(key)
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

func (n *Node) searchLeafColumnar(key []byte) (uint16, bool, error) {
	return n.searchLeafPrefixCompressed(key)
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

	i, j := 0, int(count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow
		k, _, _, err := n.leafEntryKeyAt(uint16(h))
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
		k, _, _, err := n.leafEntryKeyAt(uint16(i))
		if err != nil {
			return 0, false, err
		}
		if bytes.Equal(k, key) {
			return uint16(i), true, nil
		}
	}
	return uint16(i), false, nil
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
	if n.leafColumnar() {
		return n.addLeafEntryColumnar(key, value, flags, valPtr)
	}
	if n.leafPrefixCompressed() {
		return n.addLeafEntryPrefixCompressed(key, value, flags, valPtr)
	}

	// Calculate size needed
	// KeyLen(2) + ValLen(4) + Flags(1) + Key + Value
	entrySize := 7 + len(key)
	if flags&FlagPointer != 0 {
		entrySize += page.ValuePtrSize
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
		valPtr.Encode(buf[7+len(key):])
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

func (n *Node) addLeafEntryColumnar(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	valLen := 0
	entrySize := leafColumnarHeaderSize + len(key)
	if flags&FlagPointer != 0 {
		entrySize += page.ValuePtrSize
	} else if flags&FlagTombstone == 0 {
		valLen = len(value)
		entrySize += valLen
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
	keyOff := leafColumnarHeaderSize
	valOff := keyOff + len(key)
	writeLeafColumnarHeader(buf[:leafColumnarHeaderSize], len(key), valLen, flags, keyOff, valOff)
	copy(buf[keyOff:keyOff+len(key)], key)

	valueStart := valOff
	if flags&FlagPointer != 0 {
		valPtr.Encode(buf[valueStart : valueStart+page.ValuePtrSize])
	} else if flags&FlagTombstone == 0 {
		copy(buf[valueStart:valueStart+valLen], value)
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

	b := NewBuilderWithOptions(n.data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
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
