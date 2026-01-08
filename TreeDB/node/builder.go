package node

import (
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

// Builder facilitates O(N) sequential construction of a node.
// It avoids the O(log N) search and O(N) shift of standard insertion.
type Builder struct {
	data                  []byte
	pageID                uint64
	pType                 page.PageType
	count                 uint16
	dirEnd                int // Offset where the next directory entry (offset) will be written
	heapStart             int // Offset where the next heap entry will be written (grows down)
	leafPrefixCompression bool
	leafPrevKey           []byte
	leafIndex             int
}

type BuilderOptions struct {
	LeafPrefixCompression bool
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(data []byte, pType page.PageType) *Builder {
	return NewBuilderWithOptions(data, pType, BuilderOptions{})
}

func NewBuilderWithOptions(data []byte, pType page.PageType, opts BuilderOptions) *Builder {
	return &Builder{
		data:                  data,
		pType:                 pType,
		dirEnd:                NodeHeaderSize,
		heapStart:             len(data),
		leafPrefixCompression: opts.LeafPrefixCompression,
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

func (b *Builder) Count() uint16 {
	return b.count
}

func (b *Builder) FreeSpace() int {
	return b.heapStart - b.dirEnd
}

// Data returns the underlying buffer.
func (b *Builder) Data() []byte {
	return b.data
}

// LeafEntrySize returns the encoded size for a leaf entry if it were appended
// next, based on the builder's current prefix-compression state.
func (b *Builder) LeafEntrySize(key, value []byte, flags byte) int {
	entrySize, _, _ := b.leafEntrySize(key, value, flags)
	return entrySize
}

func (b *Builder) leafEntrySize(key, value []byte, flags byte) (entrySize int, prefixLen int, suffixLen int) {
	prefixLen = 0
	suffixLen = len(key)
	headerSize := 7
	if b.leafPrefixCompression {
		headerSize = 9
		if b.leafIndex%leafPrefixRestartInterval != 0 && len(b.leafPrevKey) > 0 {
			prefixLen = sharedPrefixLen(key, b.leafPrevKey)
			if prefixLen > len(key) {
				prefixLen = len(key)
			}
		}
		suffixLen = len(key) - prefixLen
	}

	entrySize = headerSize + suffixLen
	if flags&FlagPointer != 0 {
		entrySize += page.ValuePtrSize
	} else {
		entrySize += len(value)
	}
	return entrySize, prefixLen, suffixLen
}

// AddLeafEntry appends a leaf entry. Assumes keys are added in sorted order.
func (b *Builder) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}

	// 1. Calculate Entry Size
	// KeyPrefixLen(2) + KeySuffixLen(2) + ValLen(4) + Flags(1) + KeySuffix + Value/Ptr
	if flags&FlagValueID != 0 && len(value) != 8 {
		return ErrInvalidValueIDLength
	}

	entrySize, prefixLen, suffixLen := b.leafEntrySize(key, value, flags)
	headerSize := 7
	if b.leafPrefixCompression {
		headerSize = 9
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
	keyStart := ptr + headerSize
	if b.leafPrefixCompression {
		binary.LittleEndian.PutUint16(b.data[ptr:ptr+2], uint16(prefixLen))
		binary.LittleEndian.PutUint16(b.data[ptr+2:ptr+4], uint16(suffixLen))
		if flags&FlagPointer != 0 {
			binary.LittleEndian.PutUint32(b.data[ptr+4:ptr+8], 0) // ValueLen ignored for pointer
		} else {
			binary.LittleEndian.PutUint32(b.data[ptr+4:ptr+8], uint32(len(value)))
		}
		b.data[ptr+8] = flags
		copy(b.data[keyStart:], key[prefixLen:])
	} else {
		binary.LittleEndian.PutUint16(b.data[ptr:ptr+2], uint16(len(key)))
		if flags&FlagPointer != 0 {
			binary.LittleEndian.PutUint32(b.data[ptr+2:ptr+6], 0) // ValueLen ignored for pointer
		} else {
			binary.LittleEndian.PutUint32(b.data[ptr+2:ptr+6], uint32(len(value)))
		}
		b.data[ptr+6] = flags
		copy(b.data[keyStart:], key)
	}

	valueStart := keyStart + suffixLen
	if flags&FlagPointer != 0 {
		valPtr.Encode(b.data[valueStart : valueStart+page.ValuePtrSize])
	} else {
		copy(b.data[valueStart:valueStart+len(value)], value)
	}

	// 5. Write Directory Offset (Grow Up)
	binary.LittleEndian.PutUint16(b.data[b.dirEnd:b.dirEnd+2], uint16(entryStart))

	// 6. Update State
	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	if b.leafPrefixCompression {
		if cap(b.leafPrevKey) < len(key) {
			b.leafPrevKey = make([]byte, len(key))
		}
		b.leafPrevKey = b.leafPrevKey[:len(key)]
		copy(b.leafPrevKey, key)
	}

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
	flags := uint16(b.pType)
	if b.leafPrefixCompression && b.pType == page.PageTypeLeaf {
		flags |= leafPrefixCompressedFlag
	}
	binary.LittleEndian.PutUint16(b.data[12:14], flags)
	binary.LittleEndian.PutUint16(b.data[14:16], b.count)

	n := NewNode(b.data)
	n.UpdateChecksum()
	return n
}

func sharedPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
