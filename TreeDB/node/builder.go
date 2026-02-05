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
	leafPackedValuePtr    bool
	internalBaseDelta     bool
	internalBaseDeltaOK   bool
	internalMinChildID    uint64
	internalMaxChildID    uint64
	internalPrefixLen     int
	internalTotalKeyBytes int
	internalEntries       []internalBuildEntry
	leafPrevKeyBuf        [64]byte
	leafPrevKey           []byte
	leafIndex             int
}

const internalBaseDeltaFooterSize = 10 // u16 prefixLen + u64 baseChildID

type internalBuildEntry struct {
	prefix  []byte
	suffix  []byte
	keyLen  uint16
	childID uint64
}

var internalEntryPool = sync.Pool{
	New: func() any {
		return make([]internalBuildEntry, 0, 256)
	},
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
		leafPackedValuePtr:    opts.PackedValuePtr,
		internalBaseDelta:     opts.InternalBaseDelta,
		internalBaseDeltaOK:   opts.InternalBaseDelta,
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
	return b.AddInternalChildParts(nil, key, childPageID)
}

// AddInternalChildParts appends a child pointer using a split key representation
// (prefix + suffix) to avoid allocating/reconstructing a contiguous key slice
// in callers that already have a prefix-coded view.
func (b *Builder) AddInternalChildParts(prefix, suffix []byte, childPageID uint64) error {
	if b.pType != page.PageTypeInternal {
		return ErrInvalidType
	}

	keyLen := len(prefix) + len(suffix)
	if keyLen > int(^uint16(0)) {
		return ErrKeyTooLarge
	}

	if b.internalBaseDelta {
		return b.addInternalChildBaseDeltaParts(prefix, suffix, childPageID)
	}

	// Layout: KeyLen(2) + ChildPageID(8) + Key
	entrySize := 2 + 8 + keyLen
	required := entrySize + DirectoryEntrySize
	freeSpace := b.heapStart - b.dirEnd

	if freeSpace < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	ptr := entryStart

	b.data[ptr] = byte(keyLen)
	b.data[ptr+1] = byte(keyLen >> 8)
	putUint64(b.data[ptr+2:ptr+10], childPageID)
	copy(b.data[ptr+10:], prefix)
	copy(b.data[ptr+10+len(prefix):], suffix)

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
	internalBaseDelta16Applied := false
	if b.pType == page.PageTypeInternal && b.internalBaseDelta {
		internalBaseDeltaApplied, internalBaseDelta16Applied = b.finishInternalBaseDelta()
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
		}
		if b.leafPackedValuePtr {
			flags |= leafPackedValuePtrFlag
		}
	} else if b.pType == page.PageTypeInternal {
		if internalBaseDeltaApplied {
			flags |= internalBaseDeltaFlag
			if internalBaseDelta16Applied {
				flags |= internalBaseDelta16Flag
			}
		}
	}
	binary.LittleEndian.PutUint16(b.data[12:14], flags)
	binary.LittleEndian.PutUint16(b.data[14:16], b.count)

	n := NewNode(b.data)
	n.UpdateChecksum()
	return n
}

func internalKeyLen(prefix, suffix []byte) int {
	return len(prefix) + len(suffix)
}

func sharedPrefixLenParts(aPrefix, aSuffix, bPrefix, bSuffix []byte) int {
	n := internalKeyLen(aPrefix, aSuffix)
	if total := internalKeyLen(bPrefix, bSuffix); total < n {
		n = total
	}
	for i := 0; i < n; i++ {
		var a byte
		if i < len(aPrefix) {
			a = aPrefix[i]
		} else {
			a = aSuffix[i-len(aPrefix)]
		}
		var b byte
		if i < len(bPrefix) {
			b = bPrefix[i]
		} else {
			b = bSuffix[i-len(bPrefix)]
		}
		if a != b {
			return i
		}
	}
	return n
}

func copyKeyPrefix(dst []byte, prefix, suffix []byte, n int) {
	if n <= 0 {
		return
	}
	if n <= len(prefix) {
		copy(dst, prefix[:n])
		return
	}
	copy(dst, prefix)
	copy(dst[len(prefix):], suffix[:n-len(prefix)])
}

func (b *Builder) addInternalChildBaseDeltaParts(prefix, suffix []byte, childPageID uint64) error {
	keyLen := internalKeyLen(prefix, suffix)
	if keyLen > int(^uint16(0)) {
		return ErrKeyTooLarge
	}

	if b.internalEntries == nil {
		b.internalEntries = internalEntryPool.Get().([]internalBuildEntry)[:0]
	}

	newCount := b.count + 1
	newTotalKeyBytes := b.internalTotalKeyBytes + keyLen

	newPrefixLen := b.internalPrefixLen
	if newCount == 1 {
		newPrefixLen = keyLen
	} else {
		first := b.internalEntries[0]
		lcp := sharedPrefixLenParts(first.prefix, first.suffix, prefix, suffix)
		if lcp < newPrefixLen {
			newPrefixLen = lcp
		}
	}

	newMinChildID := b.internalMinChildID
	newMaxChildID := b.internalMaxChildID
	if newCount == 1 {
		newMinChildID = childPageID
		newMaxChildID = childPageID
	} else {
		if childPageID < newMinChildID {
			newMinChildID = childPageID
		}
		if childPageID > newMaxChildID {
			newMaxChildID = childPageID
		}
	}

	baseDeltaOK := b.internalBaseDeltaOK
	if baseDeltaOK {
		if newMaxChildID-newMinChildID > uint64(^uint32(0)) {
			baseDeltaOK = false
		}
	}

	newDirEnd := NodeHeaderSize + int(newCount)*DirectoryEntrySize

	var newHeapStart int
	if baseDeltaOK {
		prefixLen := newPrefixLen
		if newCount < 2 {
			prefixLen = 0
		}

		deltaSize := 4
		if newMaxChildID-newMinChildID <= uint64(^uint16(0)) {
			deltaSize = 2
		}

		suffixBytes := newTotalKeyBytes - prefixLen*int(newCount)
		entryBytes := int(newCount)*(2+deltaSize) + suffixBytes
		footerBytes := internalBaseDeltaFooterSize + prefixLen
		newHeapStart = int(page.PageSize) - (footerBytes + entryBytes)
	} else {
		entryBytes := int(newCount)*(2+8) + newTotalKeyBytes
		newHeapStart = int(page.PageSize) - entryBytes
	}
	if newHeapStart < newDirEnd {
		return ErrNodeFull
	}

	b.internalEntries = append(b.internalEntries, internalBuildEntry{
		prefix:  prefix,
		suffix:  suffix,
		keyLen:  uint16(keyLen),
		childID: childPageID,
	})

	b.count = newCount
	b.dirEnd = newDirEnd
	b.heapStart = newHeapStart
	b.internalBaseDeltaOK = baseDeltaOK
	b.internalMinChildID = newMinChildID
	b.internalMaxChildID = newMaxChildID
	b.internalPrefixLen = newPrefixLen
	b.internalTotalKeyBytes = newTotalKeyBytes
	return nil
}

func (b *Builder) finishInternalUncompressed() {
	count := b.count
	if count == 0 {
		return
	}
	if len(b.internalEntries) != int(count) {
		panic("finishInternalUncompressed: entry count mismatch")
	}

	dirEnd := NodeHeaderSize + int(count)*DirectoryEntrySize
	heapStart := int(page.PageSize)

	for i := uint16(0); i < count; i++ {
		e := b.internalEntries[i]
		keyLen := internalKeyLen(e.prefix, e.suffix)

		entrySize := 2 + 8 + keyLen
		heapStart -= entrySize
		if heapStart < dirEnd {
			panic("finishInternalUncompressed: page overflow")
		}

		putUint16(b.data[heapStart:heapStart+2], uint16(keyLen))
		putUint64(b.data[heapStart+2:heapStart+10], e.childID)
		copy(b.data[heapStart+10:heapStart+10+len(e.prefix)], e.prefix)
		copy(b.data[heapStart+10+len(e.prefix):heapStart+10+keyLen], e.suffix)
		putUint16(b.data[NodeHeaderSize+int(i)*2:], uint16(heapStart))
	}

	b.heapStart = heapStart
	b.dirEnd = dirEnd
}

func (b *Builder) finishInternalBaseDelta() (applied bool, delta16 bool) {
	defer func() {
		if b.internalEntries != nil {
			clear(b.internalEntries[:cap(b.internalEntries)])
			internalEntryPool.Put(b.internalEntries[:0])
			b.internalEntries = nil
		}
	}()

	count := b.count
	if count == 0 {
		return false, false
	}
	if len(b.internalEntries) != int(count) {
		panic("finishInternalBaseDelta: entry count mismatch")
	}

	if !b.internalBaseDeltaOK {
		b.finishInternalUncompressed()
		return false, false
	}

	baseChildID := b.internalMinChildID
	maxDelta := b.internalMaxChildID - baseChildID
	deltaSize := 4
	if maxDelta <= uint64(^uint16(0)) {
		deltaSize = 2
		delta16 = true
	}

	prefixLen := b.internalPrefixLen
	if count < 2 {
		prefixLen = 0
	}

	first := b.internalEntries[0]
	if internalKeyLen(first.prefix, first.suffix) < prefixLen {
		prefixLen = 0
	}

	baseOff := len(b.data) - 8
	prefixLenOff := baseOff - 2
	footerBytes := prefixLen + internalBaseDeltaFooterSize
	footerStart := len(b.data) - footerBytes

	dirEnd := NodeHeaderSize + int(count)*DirectoryEntrySize
	if footerStart < dirEnd {
		// Should not happen if AddInternalChildParts space checks are correct.
		// Fall back to uncompressed encoding.
		b.internalBaseDeltaOK = false
		b.finishInternalUncompressed()
		return false, false
	}

	if prefixLen > 0 {
		copyKeyPrefix(b.data[footerStart:footerStart+prefixLen], first.prefix, first.suffix, prefixLen)
	}
	putUint16(b.data[prefixLenOff:prefixLenOff+2], uint16(prefixLen))
	putUint64(b.data[baseOff:baseOff+8], baseChildID)

	heapStart := footerStart

	for i := uint16(0); i < count; i++ {
		e := b.internalEntries[i]
		keyLen := internalKeyLen(e.prefix, e.suffix)
		if keyLen < prefixLen {
			panic("finishInternalBaseDelta: prefix out of bounds")
		}

		var suffixA, suffixB []byte
		if prefixLen <= len(e.prefix) {
			suffixA = e.prefix[prefixLen:]
			suffixB = e.suffix
		} else {
			skip := prefixLen - len(e.prefix)
			suffixA = e.suffix[skip:]
		}
		suffixLen := len(suffixA) + len(suffixB)

		delta64 := e.childID - baseChildID
		if delta64 > uint64(^uint32(0)) {
			// Unexpected overflow. Fall back to uncompressed encoding.
			b.internalBaseDeltaOK = false
			b.finishInternalUncompressed()
			return false, false
		}

		entrySize := 2 + deltaSize + suffixLen
		heapStart -= entrySize
		if heapStart < dirEnd {
			panic("finishInternalBaseDelta: page overflow")
		}

		putUint16(b.data[heapStart:heapStart+2], uint16(suffixLen))
		if deltaSize == 2 {
			putUint16(b.data[heapStart+2:heapStart+4], uint16(delta64))
		} else {
			putUint32(b.data[heapStart+2:heapStart+6], uint32(delta64))
		}
		dst := b.data[heapStart+2+deltaSize : heapStart+2+deltaSize+suffixLen]
		n := copy(dst, suffixA)
		copy(dst[n:], suffixB)
		putUint16(b.data[NodeHeaderSize+int(i)*2:], uint16(heapStart))
	}

	b.heapStart = heapStart
	b.dirEnd = dirEnd
	return true, delta16
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
