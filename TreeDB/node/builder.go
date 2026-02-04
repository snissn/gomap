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
	leafPrefixV2          bool
	leafColumnar          bool
	leafPackedValuePtr    bool
	internalBaseDelta     bool
	leafPrevKeyBuf        [64]byte
	leafPrevKey           []byte
	leafIndex             int
}

type BuilderOptions struct {
	LeafPrefixCompression bool
	LeafColumnar          bool
	InternalBaseDelta     bool
	PackedValuePtr        bool
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(data []byte, pType page.PageType) *Builder {
	return NewBuilderWithOptions(data, pType, BuilderOptions{})
}

func NewBuilderWithOptions(data []byte, pType page.PageType, opts BuilderOptions) *Builder {
	leafPrefix := opts.LeafPrefixCompression
	return &Builder{
		data:                  data,
		pType:                 pType,
		dirEnd:                NodeHeaderSize,
		heapStart:             len(data),
		leafPrefixCompression: leafPrefix,
		leafPrefixV2:          leafPrefix,
		leafColumnar:          opts.LeafColumnar,
		leafPackedValuePtr:    opts.PackedValuePtr,
		internalBaseDelta:     opts.InternalBaseDelta,
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

// LeafPrevKey returns the last key appended to a leaf builder.
// It is only maintained when leaf prefix compression is enabled.
func (b *Builder) LeafPrevKey() []byte {
	return b.leafPrevKey
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

// LeafEntrySizeWithPrefix returns the encoded size and prefix/suffix lengths
// for a leaf entry if it were appended next.
func (b *Builder) LeafEntrySizeWithPrefix(key, value []byte, flags byte) (entrySize, prefixLen, suffixLen int) {
	return b.leafEntrySize(key, value, flags)
}

func (b *Builder) leafEntrySize(key, value []byte, flags byte) (entrySize int, prefixLen int, suffixLen int) {
	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}
	if b.leafColumnar && !b.leafPrefixCompression {
		prefixLen = 0
		suffixLen = len(key)
		entrySize = leafColumnarHeaderSize + suffixLen
		if flags&FlagPointer != 0 {
			entrySize += valPtrSize
		} else if flags&FlagTombstone == 0 {
			entrySize += len(value)
		}
		return entrySize, prefixLen, suffixLen
	}
	prefixLen = 0
	suffixLen = len(key)
	headerSize := 7
	if b.leafPrefixCompression {
		if b.leafIndex%leafPrefixRestartInterval != 0 && len(b.leafPrevKey) > 0 {
			prefixLen = sharedPrefixLen(key, b.leafPrevKey)
			if prefixLen > len(key) {
				prefixLen = len(key)
			}
		}
		suffixLen = len(key) - prefixLen
		if b.leafPrefixV2 {
			headerSize = leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, len(value))
		} else {
			headerSize = 9
		}
	}

	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	} else if flags&FlagTombstone == 0 {
		valSize = len(value)
	}

	if b.leafColumnar {
		// Columnar+prefix mode uses the v2 prefix entry header plus explicit
		// key/value offsets.
		headerSize += 4 // keyOff + valOff
		entrySize = headerSize + valSize + suffixLen
		return entrySize, prefixLen, suffixLen
	}

	entrySize = headerSize + suffixLen + valSize
	return entrySize, prefixLen, suffixLen
}

// AddLeafEntry appends a leaf entry. Assumes keys are added in sorted order.
func (b *Builder) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}
	if b.leafColumnar && !b.leafPrefixCompression {
		return b.addLeafEntryColumnar(key, value, flags, valPtr)
	}

	// 1. Calculate Entry Size
	// KeyPrefixLen(2) + KeySuffixLen(2) + ValLen(4) + Flags(1) + KeySuffix + Value/Ptr
	entrySize, prefixLen, suffixLen := b.leafEntrySize(key, value, flags)
	return b.AddLeafEntryWithPrefix(key, value, flags, valPtr, entrySize, prefixLen, suffixLen)
}

func (b *Builder) addLeafEntryColumnar(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
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

	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd
	if freeSpace < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	valOff := leafColumnarHeaderSize
	keyOff := valOff + valSize

	writeLeafColumnarHeader(b.data[entryStart:entryStart+leafColumnarHeaderSize], len(key), valLen, flags, keyOff, valOff)

	valueStart := entryStart + valOff
	if flags&FlagPointer != 0 {
		if b.leafPackedValuePtr {
			page.EncodePackedValuePtr(b.data[valueStart:valueStart+valPtrSize], valPtr)
		} else {
			valPtr.Encode(b.data[valueStart : valueStart+valPtrSize])
		}
	} else if flags&FlagTombstone == 0 {
		copy(b.data[valueStart:valueStart+valLen], value)
	}

	keyStart := entryStart + keyOff
	copy(b.data[keyStart:keyStart+len(key)], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	return nil
}

func (b *Builder) addLeafEntryColumnarPrefixV2(key, value []byte, flags byte, valPtr page.ValuePtr, entrySize, prefixLen, suffixLen int) error {
	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}

	valLen := 0
	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	} else if flags&FlagTombstone == 0 {
		valLen = len(value)
		valSize = valLen
	}

	headerSize := leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, valLen) + 4 // keyOff + valOff
	valOff := headerSize
	keyOff := valOff + valSize

	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd
	if freeSpace < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	ptr := entryStart

	headerOff := ptr
	extended := prefixLen > 254 || suffixLen > 254
	if extended {
		b.data[headerOff] = 0xFF
		b.data[headerOff+1] = 0xFF
	} else {
		b.data[headerOff] = byte(prefixLen)
		b.data[headerOff+1] = byte(suffixLen)
	}
	b.data[headerOff+2] = flags
	headerOff += 3
	if extended {
		putUint16(b.data[headerOff:headerOff+2], uint16(prefixLen))
		putUint16(b.data[headerOff+2:headerOff+4], uint16(suffixLen))
		headerOff += 4
	}
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], uint64(valLen))
		copy(b.data[headerOff:headerOff+n], tmp[:n])
		headerOff += n
	}

	putUint16(b.data[headerOff:headerOff+2], uint16(keyOff))
	putUint16(b.data[headerOff+2:headerOff+4], uint16(valOff))
	headerOff += 4

	valueStart := ptr + valOff
	if flags&FlagPointer != 0 {
		if b.leafPackedValuePtr {
			page.EncodePackedValuePtr(b.data[valueStart:valueStart+valPtrSize], valPtr)
		} else {
			valPtr.Encode(b.data[valueStart : valueStart+valPtrSize])
		}
	} else if flags&FlagTombstone == 0 {
		copy(b.data[valueStart:valueStart+valLen], value)
	}

	keyStart := ptr + keyOff
	copy(b.data[keyStart:keyStart+suffixLen], key[prefixLen:])

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	if b.leafPrefixCompression {
		if len(key) <= len(b.leafPrevKeyBuf) {
			b.leafPrevKey = b.leafPrevKeyBuf[:len(key)]
		} else {
			if cap(b.leafPrevKey) < len(key) {
				b.leafPrevKey = make([]byte, len(key))
			}
			b.leafPrevKey = b.leafPrevKey[:len(key)]
		}
		copy(b.leafPrevKey, key)
	}

	return nil
}

// AddLeafEntryWithPrefix appends a leaf entry using precomputed size/prefix data.
// The caller must ensure prefixLen/suffixLen are computed for this builder state.
func (b *Builder) AddLeafEntryWithPrefix(key, value []byte, flags byte, valPtr page.ValuePtr, entrySize, prefixLen, suffixLen int) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}

	if b.leafColumnar {
		if b.leafPrefixCompression {
			return b.addLeafEntryColumnarPrefixV2(key, value, flags, valPtr, entrySize, prefixLen, suffixLen)
		}
		return b.addLeafEntryColumnar(key, value, flags, valPtr)
	}

	headerSize := 7
	if b.leafPrefixCompression {
		if b.leafPrefixV2 {
			headerSize = leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, len(value))
		} else {
			headerSize = 9
		}
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
		if b.leafPrefixV2 {
			headerOff := ptr
			extended := prefixLen > 254 || suffixLen > 254
			if extended {
				b.data[headerOff] = 0xFF
				b.data[headerOff+1] = 0xFF
			} else {
				b.data[headerOff] = byte(prefixLen)
				b.data[headerOff+1] = byte(suffixLen)
			}
			b.data[headerOff+2] = flags
			headerOff += 3
			if extended {
				putUint16(b.data[headerOff:headerOff+2], uint16(prefixLen))
				putUint16(b.data[headerOff+2:headerOff+4], uint16(suffixLen))
				headerOff += 4
			}
			if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
				var tmp [binary.MaxVarintLen64]byte
				n := binary.PutUvarint(tmp[:], uint64(len(value)))
				copy(b.data[headerOff:headerOff+n], tmp[:n])
				headerOff += n
			}
			copy(b.data[keyStart:], key[prefixLen:])
		} else {
			b.data[ptr] = byte(prefixLen)
			b.data[ptr+1] = byte(prefixLen >> 8)
			b.data[ptr+2] = byte(suffixLen)
			b.data[ptr+3] = byte(suffixLen >> 8)
			if flags&FlagPointer != 0 {
				putUint32(b.data[ptr+4:ptr+8], 0) // ValueLen ignored for pointer
			} else {
				putUint32(b.data[ptr+4:ptr+8], uint32(len(value)))
			}
			b.data[ptr+8] = flags
			copy(b.data[keyStart:], key[prefixLen:])
		}
	} else {
		keyLen := len(key)
		b.data[ptr] = byte(keyLen)
		b.data[ptr+1] = byte(keyLen >> 8)
		if flags&FlagPointer != 0 {
			putUint32(b.data[ptr+2:ptr+6], 0) // ValueLen ignored for pointer
		} else {
			putUint32(b.data[ptr+2:ptr+6], uint32(len(value)))
		}
		b.data[ptr+6] = flags
		copy(b.data[keyStart:], key)
	}

	valueStart := keyStart + suffixLen
	if flags&FlagPointer != 0 {
		valPtrSize := page.ValuePtrSize
		if b.leafPackedValuePtr {
			valPtrSize = page.PackedValuePtrSize
			page.EncodePackedValuePtr(b.data[valueStart:valueStart+valPtrSize], valPtr)
		} else {
			valPtr.Encode(b.data[valueStart : valueStart+valPtrSize])
		}
	} else {
		copy(b.data[valueStart:valueStart+len(value)], value)
	}

	// 5. Write Directory Offset (Grow Up)
	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	// 6. Update State
	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	if b.leafPrefixCompression {
		if len(key) <= len(b.leafPrevKeyBuf) {
			b.leafPrevKey = b.leafPrevKeyBuf[:len(key)]
		} else {
			if cap(b.leafPrevKey) < len(key) {
				b.leafPrevKey = make([]byte, len(key))
			}
			b.leafPrevKey = b.leafPrevKey[:len(key)]
		}
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

	keyLen := len(key)
	b.data[ptr] = byte(keyLen)
	b.data[ptr+1] = byte(keyLen >> 8)
	putUint64(b.data[ptr+2:ptr+10], childPageID)
	copy(b.data[ptr+10:], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++

	return nil
}

// Finish finalizes the page header and checksum.
// Returns a Node wrapper for convenience.
func (b *Builder) Finish() *Node {
	// Write Header
	putUint64(b.data[0:8], b.pageID)
	// Checksum at 8-12 (written by UpdateChecksum)
	flags := uint16(b.pType)
	if b.pType == page.PageTypeLeaf {
		if b.leafPrefixCompression {
			flags |= leafPrefixCompressedFlag
			if b.leafPrefixV2 {
				flags |= leafPrefixV2Flag
			}
		}
		if b.leafColumnar {
			flags |= leafColumnarFlag
		}
		if b.leafPackedValuePtr {
			flags |= leafPackedValuePtrFlag
		}
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

func putUint16(dst []byte, v uint16) {
	_ = dst[1]
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
}

func putUint64(dst []byte, v uint64) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
	dst[4] = byte(v >> 32)
	dst[5] = byte(v >> 40)
	dst[6] = byte(v >> 48)
	dst[7] = byte(v >> 56)
}
