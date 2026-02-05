package node

import (
	"encoding/binary"
	"sync"

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
	leafColumnarV2        bool
	leafPackedValuePtr    bool
	internalBaseDelta     bool
	internalBaseChildID   uint64
	leafPrevKeyBuf        [64]byte
	leafPrevKey           []byte
	leafIndex             int

	leafColumnarV2Entries  []leafColumnarV2Entry
	leafColumnarV2Arena    []byte
	leafColumnarV2KeyBytes int
	leafColumnarV2ValBytes int
}

const internalBaseDeltaFooterSize = 10 // u16 prefixLen + u64 baseChildID

var internalBaseDeltaRewritePool = sync.Pool{
	New: func() any {
		return make([]byte, page.PageSize)
	},
}

var leafColumnarV2ArenaPool = sync.Pool{
	New: func() any {
		buf := make([]byte, page.PageSize)
		return buf[:0]
	},
}

type BuilderOptions struct {
	LeafPrefixCompression bool
	LeafColumnar          bool
	InternalBaseDelta     bool
	PackedValuePtr        bool
}

type leafColumnarV2Entry struct {
	key    []byte
	value  []byte
	valPtr page.ValuePtr
	flags  byte
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(data []byte, pType page.PageType) *Builder {
	return NewBuilderWithOptions(data, pType, BuilderOptions{})
}

func NewBuilderWithOptions(data []byte, pType page.PageType, opts BuilderOptions) *Builder {
	leafPrefix := opts.LeafPrefixCompression
	leafColumnarV2 := pType == page.PageTypeLeaf && opts.LeafColumnar && !leafPrefix
	heapStart := len(data)
	if pType == page.PageTypeInternal && opts.InternalBaseDelta {
		// Reserve space for the internal base-delta footer:
		//   [u16 prefixLen][u64 baseChildID] (prefix bytes are carved out of keys at Finish()).
		heapStart -= 10
		if heapStart < NodeHeaderSize {
			heapStart = NodeHeaderSize
		}
	}
	return &Builder{
		data:                  data,
		pType:                 pType,
		dirEnd:                NodeHeaderSize,
		heapStart:             heapStart,
		leafPrefixCompression: leafPrefix,
		leafPrefixV2:          leafPrefix,
		leafColumnar:          opts.LeafColumnar,
		leafColumnarV2:        leafColumnarV2,
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
		valSize := 0
		if flags&FlagPointer != 0 {
			valSize = valPtrSize
		} else if flags&FlagTombstone == 0 {
			valSize = len(value)
		}
		// Columnar v2 stores key/value bytes in separate blobs and writes
		// per-entry metadata (val offset + flags) in a columnar header region.
		entrySize = suffixLen + valSize + leafColumnarV2MetaSize
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
		// Columnar+prefix mode uses the v2 prefix entry header with implicit
		// offsets (value column first, then key suffix).
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
		if b.leafColumnarV2 {
			return b.addLeafEntryColumnarV2(key, value, flags, valPtr)
		}
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
	writeLeafColumnarHeader(b.data[entryStart:entryStart+leafColumnarHeaderSize], len(key), valLen, flags)

	valueStart := entryStart + leafColumnarHeaderSize
	if flags&FlagPointer != 0 {
		if b.leafPackedValuePtr {
			page.EncodePackedValuePtr(b.data[valueStart:valueStart+valPtrSize], valPtr)
		} else {
			valPtr.Encode(b.data[valueStart : valueStart+valPtrSize])
		}
	} else if flags&FlagTombstone == 0 {
		copy(b.data[valueStart:valueStart+valLen], value)
	}

	keyStart := valueStart + valSize
	copy(b.data[keyStart:keyStart+len(key)], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	return nil
}

func (b *Builder) addLeafEntryColumnarV2(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}

	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = valPtrSize
	} else if flags&FlagTombstone == 0 {
		valSize = len(value)
	}

	entrySize := leafColumnarV2MetaSize + len(key) + valSize
	required := entrySize + DirectoryEntrySize // key offset slot
	freeSpace := b.heapStart - b.dirEnd
	if freeSpace < required {
		return ErrNodeFull
	}

	keyCopy := b.leafColumnarV2CopyBytes(key)
	var valueCopy []byte
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		valueCopy = b.leafColumnarV2CopyBytes(value)
	}

	b.leafColumnarV2Entries = append(b.leafColumnarV2Entries, leafColumnarV2Entry{
		key:    keyCopy,
		value:  valueCopy,
		valPtr: valPtr,
		flags:  flags,
	})
	b.leafColumnarV2KeyBytes += len(key)
	b.leafColumnarV2ValBytes += valSize

	b.dirEnd += DirectoryEntrySize + leafColumnarV2MetaSize
	b.heapStart -= len(key) + valSize
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

	headerSize := leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, valLen)
	valOff := headerSize

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

	keyStart := valueStart + valSize
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
		if b.leafColumnarV2 {
			return b.addLeafEntryColumnarV2(key, value, flags, valPtr)
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

	if b.internalBaseDelta {
		return b.addInternalChildBaseDelta(key, childPageID)
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
	internalBaseDeltaApplied := false
	if b.pType == page.PageTypeInternal && b.internalBaseDelta {
		internalBaseDeltaApplied = b.finishInternalBaseDelta()
	}
	if b.pType == page.PageTypeLeaf && b.leafColumnarV2 && !b.leafPrefixCompression {
		b.finishLeafColumnarV2()
	}

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
			if b.leafColumnarV2 {
				flags |= leafColumnarV2Flag
			}
		}
		if b.leafPackedValuePtr {
			flags |= leafPackedValuePtrFlag
		}
	} else if b.pType == page.PageTypeInternal {
		if internalBaseDeltaApplied {
			flags |= internalBaseDeltaFlag
		}
	}
	binary.LittleEndian.PutUint16(b.data[12:14], flags)
	binary.LittleEndian.PutUint16(b.data[14:16], b.count)

	n := NewNode(b.data)
	n.UpdateChecksum()
	return n
}

func (b *Builder) finishLeafColumnarV2() {
	clear(b.data)

	count := int(b.count)
	if count == 0 {
		b.dirEnd = NodeHeaderSize
		b.heapStart = len(b.data)
		return
	}

	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}

	keyDirStart := NodeHeaderSize
	valDirStart := keyDirStart + count*DirectoryEntrySize
	flagsStart := valDirStart + count*DirectoryEntrySize
	metaEnd := flagsStart + count

	keysStart := len(b.data) - b.leafColumnarV2KeyBytes
	valuesStart := keysStart - b.leafColumnarV2ValBytes
	if valuesStart < metaEnd {
		panic("leaf columnar v2 packing overflow")
	}

	valOff := valuesStart
	keyOff := keysStart

	for i := 0; i < count; i++ {
		e := b.leafColumnarV2Entries[i]

		putUint16(b.data[keyDirStart+i*2:keyDirStart+i*2+2], uint16(keyOff))
		putUint16(b.data[valDirStart+i*2:valDirStart+i*2+2], uint16(valOff))
		b.data[flagsStart+i] = e.flags

		if e.flags&FlagPointer != 0 {
			if b.leafPackedValuePtr {
				page.EncodePackedValuePtr(b.data[valOff:valOff+valPtrSize], e.valPtr)
			} else {
				e.valPtr.Encode(b.data[valOff : valOff+valPtrSize])
			}
			valOff += valPtrSize
		} else if e.flags&FlagTombstone == 0 {
			copy(b.data[valOff:valOff+len(e.value)], e.value)
			valOff += len(e.value)
		}

		copy(b.data[keyOff:keyOff+len(e.key)], e.key)
		keyOff += len(e.key)
	}

	if valOff != keysStart || keyOff != len(b.data) {
		panic("leaf columnar v2 packing mismatch")
	}

	b.dirEnd = metaEnd
	b.heapStart = valuesStart

	b.leafColumnarV2Entries = nil
	b.leafColumnarV2KeyBytes = 0
	b.leafColumnarV2ValBytes = 0
	if b.leafColumnarV2Arena != nil {
		leafColumnarV2ArenaPool.Put(b.leafColumnarV2Arena[:0])
		b.leafColumnarV2Arena = nil
	}
}

func (b *Builder) leafColumnarV2CopyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if b.leafColumnarV2Arena == nil {
		arenaAny := leafColumnarV2ArenaPool.Get()
		arena := arenaAny.([]byte)
		b.leafColumnarV2Arena = arena[:0]
	}

	start := len(b.leafColumnarV2Arena)
	end := start + len(src)
	if end > cap(b.leafColumnarV2Arena) {
		dst := make([]byte, len(src))
		copy(dst, src)
		return dst
	}
	b.leafColumnarV2Arena = b.leafColumnarV2Arena[:end]
	dst := b.leafColumnarV2Arena[start:end]
	copy(dst, src)
	return dst
}

func (b *Builder) addInternalChildBaseDelta(key []byte, childPageID uint64) error {
	if len(key) > int(^uint16(0)) {
		return ErrKeyTooLarge
	}

	if b.count == 0 {
		b.internalBaseChildID = childPageID
	} else if childPageID < b.internalBaseChildID {
		if !b.rebaseInternalBaseDelta(childPageID) {
			if err := b.fallbackInternalBaseDeltaToUncompressed(); err != nil {
				return err
			}
			return b.AddInternalChild(key, childPageID)
		}
	}

	delta64 := childPageID - b.internalBaseChildID
	if delta64 > uint64(^uint32(0)) {
		if err := b.fallbackInternalBaseDeltaToUncompressed(); err != nil {
			return err
		}
		return b.AddInternalChild(key, childPageID)
	}
	delta := uint32(delta64)

	suffixLen := len(key)
	entrySize := 2 + 4 + suffixLen
	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd
	if freeSpace < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	putUint16(b.data[entryStart:entryStart+2], uint16(suffixLen))
	putUint32(b.data[entryStart+2:entryStart+6], delta)
	copy(b.data[entryStart+6:entryStart+6+suffixLen], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	return nil
}

func (b *Builder) rebaseInternalBaseDelta(newBase uint64) bool {
	if newBase >= b.internalBaseChildID {
		b.internalBaseChildID = newBase
		return true
	}
	diff := b.internalBaseChildID - newBase
	if diff > uint64(^uint32(0)) {
		return false
	}
	diff32 := uint32(diff)
	for i := uint16(0); i < b.count; i++ {
		dirOff := NodeHeaderSize + int(i)*2
		offset := getUint16(b.data[dirOff : dirOff+2])
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+6 > len(b.data) {
			return false
		}
		oldDelta := binary.LittleEndian.Uint32(b.data[ptr+2 : ptr+6])
		if ^uint32(0)-oldDelta < diff32 {
			return false
		}
		newDelta := oldDelta + diff32
		putUint32(b.data[ptr+2:ptr+6], newDelta)
	}
	b.internalBaseChildID = newBase
	return true
}

func (b *Builder) fallbackInternalBaseDeltaToUncompressed() error {
	count := b.count
	if count == 0 {
		b.internalBaseDelta = false
		b.internalBaseChildID = 0
		b.heapStart = int(page.PageSize)
		b.dirEnd = NodeHeaderSize
		return nil
	}

	tmpAny := internalBaseDeltaRewritePool.Get()
	tmp := tmpAny.([]byte)
	clear(tmp)

	dirEnd := NodeHeaderSize + int(count)*DirectoryEntrySize
	heapStart := int(page.PageSize)
	baseChildID := b.internalBaseChildID

	for i := uint16(0); i < count; i++ {
		dirOff := NodeHeaderSize + int(i)*2
		offset := getUint16(b.data[dirOff : dirOff+2])
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+6 > len(b.data) {
			internalBaseDeltaRewritePool.Put(tmp)
			return ErrCorruptedNode
		}

		keyLen := int(getUint16(b.data[ptr : ptr+2]))
		delta := binary.LittleEndian.Uint32(b.data[ptr+2 : ptr+6])
		keyStart := ptr + 6
		keyEnd := keyStart + keyLen
		if keyLen < 0 || keyEnd > len(b.data) {
			internalBaseDeltaRewritePool.Put(tmp)
			return ErrCorruptedNode
		}

		childID := baseChildID + uint64(delta)
		entrySize := 2 + 8 + keyLen
		heapStart -= entrySize
		if heapStart < dirEnd {
			internalBaseDeltaRewritePool.Put(tmp)
			return ErrNodeFull
		}

		putUint16(tmp[heapStart:heapStart+2], uint16(keyLen))
		putUint64(tmp[heapStart+2:heapStart+10], childID)
		copy(tmp[heapStart+10:heapStart+10+keyLen], b.data[keyStart:keyEnd])
		putUint16(tmp[dirOff:dirOff+2], uint16(heapStart))
	}

	copy(b.data, tmp)
	internalBaseDeltaRewritePool.Put(tmp)

	b.heapStart = heapStart
	b.dirEnd = dirEnd
	b.internalBaseDelta = false
	b.internalBaseChildID = 0
	return nil
}

func (b *Builder) finishInternalBaseDelta() bool {
	count := b.count
	if count == 0 {
		return false
	}

	baseOff := len(b.data) - 8
	prefixLenOff := baseOff - 2
	putUint16(b.data[prefixLenOff:prefixLenOff+2], 0)
	putUint64(b.data[baseOff:baseOff+8], b.internalBaseChildID)

	dirEnd := NodeHeaderSize + int(count)*DirectoryEntrySize
	firstKey, _, err := b.internalBaseDeltaEntryView(0)
	if err != nil {
		return true
	}
	lastKey, _, err := b.internalBaseDeltaEntryView(count - 1)
	if err != nil {
		return true
	}

	prefixLen := sharedPrefixLen(firstKey, lastKey)
	if count < 2 || prefixLen <= 0 {
		return true
	}
	if prefixLen > len(firstKey) {
		prefixLen = len(firstKey)
	}
	if prefixLen > int(^uint16(0)) {
		return true
	}

	footerBytes := prefixLen + internalBaseDeltaFooterSize
	footerStart := len(b.data) - footerBytes
	if footerStart < dirEnd {
		// Not enough room to store the prefix bytes. Keep prefixLen=0.
		return true
	}

	tmpAny := internalBaseDeltaRewritePool.Get()
	tmp := tmpAny.([]byte)
	clear(tmp)

	heapStart := footerStart

	prefixStart := footerStart
	copy(tmp[prefixStart:prefixStart+prefixLen], firstKey[:prefixLen])
	putUint16(tmp[prefixLenOff:prefixLenOff+2], uint16(prefixLen))
	putUint64(tmp[baseOff:baseOff+8], b.internalBaseChildID)

	for i := uint16(0); i < count; i++ {
		fullKey, delta, err := b.internalBaseDeltaEntryView(i)
		if err != nil {
			internalBaseDeltaRewritePool.Put(tmp)
			return true
		}
		if len(fullKey) < prefixLen {
			internalBaseDeltaRewritePool.Put(tmp)
			return true
		}
		suffix := fullKey[prefixLen:]
		suffixLen := len(suffix)

		entrySize := 2 + 4 + suffixLen
		heapStart -= entrySize
		if heapStart < dirEnd {
			internalBaseDeltaRewritePool.Put(tmp)
			return true
		}

		putUint16(tmp[heapStart:heapStart+2], uint16(suffixLen))
		putUint32(tmp[heapStart+2:heapStart+6], delta)
		copy(tmp[heapStart+6:heapStart+6+suffixLen], suffix)
		putUint16(tmp[NodeHeaderSize+int(i)*2:], uint16(heapStart))
	}

	copy(b.data, tmp)
	internalBaseDeltaRewritePool.Put(tmp)

	b.heapStart = heapStart
	b.dirEnd = dirEnd
	return true
}

func (b *Builder) internalBaseDeltaEntryView(index uint16) (key []byte, delta uint32, err error) {
	dirOff := NodeHeaderSize + int(index)*2
	if dirOff+2 > len(b.data) {
		return nil, 0, ErrCorruptedNode
	}
	offset := getUint16(b.data[dirOff : dirOff+2])
	ptr := int(offset)
	if ptr < NodeHeaderSize || ptr+6 > len(b.data) {
		return nil, 0, ErrCorruptedNode
	}

	keyLen := int(getUint16(b.data[ptr : ptr+2]))
	delta = binary.LittleEndian.Uint32(b.data[ptr+2 : ptr+6])
	ptr += 6
	if keyLen < 0 || ptr+keyLen > len(b.data) {
		return nil, 0, ErrCorruptedNode
	}
	key = b.data[ptr : ptr+keyLen]
	return key, delta, nil
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
