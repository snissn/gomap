package node

import (
	"bytes"
	"encoding/binary"

	"github.com/snissn/gomap-gemini/TreeDB/page"
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

// GetLeafEntry reads the entry at the given index.
func (n *Node) GetLeafEntry(index uint16) (LeafEntry, error) {
	offset, err := n.getOffset(index)
	if err != nil {
		return LeafEntry{}, err
	}
	
	// Bounds check
	if int(offset) >= len(n.data) {
		return LeafEntry{}, ErrCorruptedNode
	}
	
	ptr := int(offset)
	// Layout: KeyLen(2) | ValLen(4) | Flags(1) | Key | Value
	if ptr+7 > len(n.data) {
		return LeafEntry{}, ErrCorruptedNode
	}
	
	keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
	valLen := binary.LittleEndian.Uint32(n.data[ptr+2 : ptr+6])
	flags := n.data[ptr+6]
	
	ptr += 7
	
	if ptr+int(keyLen) > len(n.data) {
		return LeafEntry{}, ErrCorruptedNode
	}
	
	key := make([]byte, keyLen)
	copy(key, n.data[ptr:ptr+int(keyLen)])
	ptr += int(keyLen)
	
	entry := LeafEntry{
		Key:   key,
		Flags: flags,
	}
	
	if flags&FlagTombstone != 0 {
		return entry, nil
	}
	
	if flags&FlagPointer != 0 {
		if ptr+page.ValuePtrSize > len(n.data) {
			return LeafEntry{}, ErrCorruptedNode
		}
		entry.ValuePtr = page.DecodeValuePtr(n.data[ptr : ptr+page.ValuePtrSize])
		// Note: We don't allocate entry.Value for pointers, the caller must fetch it from Slab.
	} else {
		// Inline
		if ptr+int(valLen) > len(n.data) {
			return LeafEntry{}, ErrCorruptedNode
		}
		val := make([]byte, valLen)
		copy(val, n.data[ptr:ptr+int(valLen)])
		entry.Value = val
	}
	
	return entry, nil
}

// SearchLeaf performs a binary search for the given key in a Leaf Node.
// Returns the index of the first entry where Entry.Key >= key.
// If key is found, found=true.
// If key is greater than all entries, returns Count, false.
func (n *Node) SearchLeaf(key []byte) (uint16, bool) {
	count := n.Count()
	i, j := 0, int(count)
	
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow
		
		// Read key at index h without full decode
		// We trust offsets are valid (verified at insert/load?)
		// For speed, we just assume valid for now, panic on bounds if corrupted
		offset := binary.LittleEndian.Uint16(n.data[NodeHeaderSize+h*2:])
		ptr := int(offset)
		keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
		// Skip ValLen(4) + Flags(1)
		keyPtr := ptr + 7
		
		// Compare
		cmp := bytes.Compare(n.data[keyPtr:keyPtr+int(keyLen)], key)
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	
	if i < int(count) {
		// Check for equality
		offset := binary.LittleEndian.Uint16(n.data[NodeHeaderSize+i*2:])
		ptr := int(offset)
		keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
		keyPtr := ptr + 7
		if bytes.Equal(n.data[keyPtr:keyPtr+int(keyLen)], key) {
			return uint16(i), true
		}
	}
	
	return uint16(i), false
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
	idx, found := n.SearchLeaf(key)
	
	needed := entrySize
	if !found {
		needed += DirectoryEntrySize
	}
	
	if n.FreeSpace() < needed {
		// For now, just error.
		return ErrNodeFull
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
		binary.LittleEndian.PutUint32(buf[2:6], 0) // or logic length?
		buf[6] = flags
		copy(buf[7:], key)
		valPtr.Encode(buf[7+len(key):])
	} else {
		binary.LittleEndian.PutUint32(buf[2:6], uint32(len(value)))
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
		off := binary.LittleEndian.Uint16(n.data[NodeHeaderSize+int(k)*2:])
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
