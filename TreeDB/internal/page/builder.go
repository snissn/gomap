package page

import (
	"encoding/binary"
)

// PageBuilder facilitates O(N) sequential construction of a node.
type PageBuilder struct {
	buf       []byte // The full page buffer (including header)
	body      []byte // The body slice (excluding header)
	header    *Header
	pageID    PageID
	pType     PageFlags
	count     uint16
	heapTop   uint16   // Offset where next heap entry starts (grows up)
	offsets   []uint16 // Buffered offsets for directory (to be written at Finish)
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(buf []byte, pid PageID, pType PageFlags) (*PageBuilder, error) {
	if len(buf) != PageSize {
		return nil, ErrPageTooSmall
	}
	
	// Initialize Header
	h, body, err := SplitPage(buf)
	if err != nil {
		return nil, err
	}
	
	// Clear body
	for i := range body {
		body[i] = 0
	}
	
	return &PageBuilder{
		buf:      buf,
		body:     body,
		header:   h,
		pageID:   pid,
		pType:    pType,
		heapTop:  slotHeaderSize,
		offsets:  make([]uint16, 0, 16), // Pre-alloc reasonable size
	}, nil
}

// AddLeafEntry appends a leaf entry. Assumes keys are added in sorted order.
func (b *PageBuilder) AddLeafEntry(key []byte, flags LeafFlags, inlineValue []byte, ptr ValuePtr) error {
	if b.pType != PageTypeLeaf {
		return ErrWrongPageType
	}

	// 1. Calculate Entry Size
	// Mirrors encodeLeafEntry logic
	entrySize := leafEntrySize(len(key), flags, len(inlineValue))
	
	// 2. Check Space
	// Need space for Entry + Directory Slot (2 bytes)
	// Directory size will be (len(offsets) + 1) * 2
	dirSize := (len(b.offsets) + 1) * 2
	used := int(b.heapTop) + dirSize
	freeSpace := len(b.body) - used
	
	if freeSpace < entrySize { // entrySize already accounts for entry bytes
		return ErrPageFull
	}

	// 3. Write Entry to Heap (Grow Up)
	off := b.heapTop
	if _, err := encodeLeafEntry(b.body, off, key, flags, inlineValue, ptr); err != nil {
		return err
	}

	// 4. Buffer Offset
	b.offsets = append(b.offsets, off)

	// 5. Update State
	b.heapTop += uint16(entrySize)
	b.count++

	return nil
}

// AddInternalChild appends a child pointer. Assumes keys are added in sorted order.
func (b *PageBuilder) AddInternalChild(key []byte, childPageID PageID) error {
	if b.pType != PageTypeInternal {
		return ErrWrongPageType
	}

	// Layout: ChildPageID(8) + Key
	// parse.go expects this layout and calculates key length from offsets.
	entrySize := 8 + len(key)
	
	dirSize := (len(b.offsets) + 1) * 2
	used := int(b.heapTop) + dirSize
	freeSpace := len(b.body) - used
	
	if freeSpace < entrySize {
		return ErrPageFull
	}

	// Write Entry
	off := b.heapTop
	dst := b.body[off:]
	if len(dst) < entrySize {
		return ErrPageCorrupt
	}
	
	binary.LittleEndian.PutUint64(dst[0:8], uint64(childPageID))
	copy(dst[8:], key)

	// Buffer Offset
	b.offsets = append(b.offsets, off)

	b.heapTop += uint16(entrySize)
	b.count++

	return nil
}

// Finish writes the directory, header and checksum.
func (b *PageBuilder) Finish() {
	b.header.PageID = b.pageID
	b.header.Flags = b.pType
	b.header.Count = b.count
	
	// Update Heap Top
	setHeapTop(b.body, b.heapTop)
	
	// Write Directory (Descending from end)
	// Entry 0 at Start, Entry N-1 at End-2?
	// No, dirStart = len - count*2.
	// Entry 0 is at dirStart.
	// Entry 1 is at dirStart + 2.
	// ...
	
	dirStart := len(b.body) - int(b.count)*2
	for i, off := range b.offsets {
		pos := dirStart + i*2
		binary.LittleEndian.PutUint16(b.body[pos:pos+2], off)
	}
	
	// Calculate CRC
	b.header.SetBodyCRC(b.body)
}