package node

import (
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

// Builder facilitates O(N) sequential construction of a node.
// It avoids the O(log N) search and O(N) shift of standard insertion.
type Builder struct {
	data      []byte
	pageID    uint64
	pType     page.PageType
	count     uint16
	dirEnd    int // Offset where the next directory entry (offset) will be written
	heapStart int // Offset where the next heap entry will be written (grows down)
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(data []byte, pType page.PageType) *Builder {
	return &Builder{
		data:      data,
		pType:     pType,
		dirEnd:    NodeHeaderSize,
		heapStart: len(data),
	}
}

// SetPageID sets the page ID (can be done at finish too).
func (b *Builder) SetPageID(id uint64) {
	b.pageID = id
}

// PageID returns the configured page ID.
func (b *Builder) PageID() uint64 {
	return b.pageID
}

// Data returns the underlying buffer.
func (b *Builder) Data() []byte {
	return b.data
}

// AddLeafEntry appends a leaf entry. Assumes keys are added in sorted order.
func (b *Builder) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}

	// 1. Calculate Entry Size
	// KeyLen(2) + ValLen(4) + Flags(1) + Key + Value/Ptr
	entrySize := 7 + len(key)
	if flags&FlagPointer != 0 {
		entrySize += page.ValuePtrSize
	} else {
		entrySize += len(value)
	}

	// 2. Check Space
	// Need space for Entry + Directory Slot (2 bytes)
	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd

	if freeSpace < required {
		return ErrNodeFull
	}

	// 3. Allocate in Heap (Grow Down)
	entryStart := b.heapStart - entrySize

	// 4. Write Entry
	// Pointer to start of entry
	ptr := entryStart
	binary.LittleEndian.PutUint16(b.data[ptr:ptr+2], uint16(len(key)))

	if flags&FlagPointer != 0 {
		binary.LittleEndian.PutUint32(b.data[ptr+2:ptr+6], 0) // ValueLen ignored for pointer
		b.data[ptr+6] = flags
		copy(b.data[ptr+7:], key)
		valPtr.Encode(b.data[ptr+7+len(key):])
	} else {
		binary.LittleEndian.PutUint32(b.data[ptr+2:ptr+6], uint32(len(value)))
		b.data[ptr+6] = flags
		copy(b.data[ptr+7:], key)
		copy(b.data[ptr+7+len(key):], value)
	}

	// 5. Write Directory Offset (Grow Up)
	binary.LittleEndian.PutUint16(b.data[b.dirEnd:b.dirEnd+2], uint16(entryStart))

	// 6. Update State
	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++

	return nil
}

// AddInternalChild appends a child pointer. Assumes keys are added in sorted order.
func (b *Builder) AddInternalChild(key []byte, childPageID uint64) error {
	if b.pType != page.PageTypeInternal {
		return ErrInvalidType
	}

	// Layout: KeyLen(2) + ChildPageID(8) + Key
	entrySize := 2 + 8 + len(key)
	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd

	if freeSpace < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	ptr := entryStart

	binary.LittleEndian.PutUint16(b.data[ptr:ptr+2], uint16(len(key)))
	binary.LittleEndian.PutUint64(b.data[ptr+2:ptr+10], childPageID)
	copy(b.data[ptr+10:], key)

	binary.LittleEndian.PutUint16(b.data[b.dirEnd:b.dirEnd+2], uint16(entryStart))

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++

	return nil
}

// Finish finalizes the page header and checksum.
// Returns a Node wrapper for convenience.
func (b *Builder) Finish() *Node {
	// Write Header
	binary.LittleEndian.PutUint64(b.data[0:8], b.pageID)
	// Checksum at 8-12 (written by UpdateChecksum)
	binary.LittleEndian.PutUint16(b.data[12:14], uint16(b.pType))
	binary.LittleEndian.PutUint16(b.data[14:16], b.count)

	n := NewNode(b.data)
	n.UpdateChecksum()
	return n
}
