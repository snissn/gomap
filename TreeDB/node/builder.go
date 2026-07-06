package node

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"unsafe"

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
	leafEntryRevisions    bool
	internalBaseDelta     bool
	internalLeafLogRefs   bool
	internalBaseChildID   uint64
	internalBaseDeltaW    int
	internalBaseHasChild  bool
	internalBaseMinChild  uint64
	internalBaseMaxChild  uint64
	internalFenceBounds   bool
	internalFenceLow      []byte
	internalFenceHigh     []byte
	leafPrevKeyBuf        [64]byte
	leafPrevKey           []byte
	leafIndex             int

	leafColumnarV2Entries  []leafColumnarV2Entry
	leafColumnarV2EntriesH *leafColumnarV2EntriesHandle
	leafColumnarV2Arena    []byte
	leafColumnarV2ArenaH   *byteArenaHandle
	leafColumnarV2KeyBytes int
	leafColumnarV2ValBytes int

	leafColumnarPrefixV2Entries     []leafColumnarPrefixV2Entry
	leafColumnarPrefixV2EntriesH    *leafColumnarPrefixV2EntriesHandle
	leafColumnarPrefixV2KeyBytes    int
	leafColumnarPrefixV2ValBytes    int
	leafColumnarPrefixV2AllInline   bool
	leafColumnarPrefixV2AllPointer  bool
	leafColumnarPrefixV2ValueArena  []byte
	leafColumnarPrefixV2ValueArenaH *byteArenaHandle

	internalBaseEntries       []internalBaseDeltaEntry
	internalBaseEntriesH      *internalBaseEntriesHandle
	internalBaseArena         []byte
	internalBaseArenaH        *byteArenaHandle
	internalBaseTotalKeyBytes int
	internalBaseSharedPrefix  int
}

const (
	internalBaseDeltaFooterTailSize = 14 // u16 lowLen + u16 highLen + u16 prefixLen + u64 baseChildID
	internalLeafLogRefHeaderSize    = 2 + page.LogRecordRefSize
)

type byteArenaHandle struct {
	buf []byte
}

type leafColumnarV2EntriesHandle struct {
	entries []leafColumnarV2Entry
}

type leafColumnarPrefixV2EntriesHandle struct {
	entries []leafColumnarPrefixV2Entry
}

type internalBaseEntriesHandle struct {
	entries []internalBaseDeltaEntry
}

var leafColumnarV2ArenaPool = sync.Pool{
	New: func() any {
		return &byteArenaHandle{buf: make([]byte, 0, page.PageSize)}
	},
}

var leafColumnarPrefixV2ValueArenaPool = sync.Pool{
	New: func() any {
		return &byteArenaHandle{buf: make([]byte, 0, page.PageSize)}
	},
}

const (
	leafColumnarEntriesPoolInitCap = 256
	leafColumnarEntriesPoolMaxCap  = 1024
	internalBaseEntriesPoolInitCap = 256
	internalBaseEntriesPoolMaxCap  = 1024
)

var leafColumnarV2EntriesPool = sync.Pool{
	New: func() any {
		return &leafColumnarV2EntriesHandle{
			entries: make([]leafColumnarV2Entry, 0, leafColumnarEntriesPoolInitCap),
		}
	},
}

var leafColumnarPrefixV2EntriesPool = sync.Pool{
	New: func() any {
		return &leafColumnarPrefixV2EntriesHandle{
			entries: make([]leafColumnarPrefixV2Entry, 0, leafColumnarEntriesPoolInitCap),
		}
	},
}

var internalBaseEntriesPool = sync.Pool{
	New: func() any {
		return &internalBaseEntriesHandle{
			entries: make([]internalBaseDeltaEntry, 0, internalBaseEntriesPoolInitCap),
		}
	},
}

var internalBaseArenaPool = sync.Pool{
	New: func() any {
		return &byteArenaHandle{buf: make([]byte, 0, page.PageSize)}
	},
}

var builderNativeLittleEndian = func() bool {
	var x uint16 = 1
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

type BuilderOptions struct {
	LeafPrefixCompression bool
	LeafColumnar          bool
	InternalBaseDelta     bool
	PackedValuePtr        bool
	EntryRevisions        bool
}

// LeafHeuristicEntry describes one logical leaf entry for adaptive encoding
// selection.
type LeafHeuristicEntry struct {
	Key   []byte
	Flags byte
}

// AdaptiveLeafBuilderOptions chooses per-page leaf encoding flags from a base
// capability set using a deterministic lightweight heuristic.
//
// Heuristic goals:
// - prefer columnar for pointer-heavy pages,
// - prefer prefix compression for high shared-prefix key runs,
// - avoid extra metadata overhead on short, low-prefix, pointer-dense pages.
func AdaptiveLeafBuilderOptions(base BuilderOptions, entries []LeafHeuristicEntry) BuilderOptions {
	if len(entries) == 0 {
		return base
	}
	ordered := entries
	if !leafHeuristicEntriesSorted(entries) {
		ordered = append(make([]LeafHeuristicEntry, 0, len(entries)), entries...)
		sort.Slice(ordered, func(i, j int) bool {
			cmp := bytes.Compare(ordered[i].Key, ordered[j].Key)
			if cmp != 0 {
				return cmp < 0
			}
			return ordered[i].Flags < ordered[j].Flags
		})
	}

	putCount := 0
	pointerCount := 0
	deleteCount := 0
	prefixPairs := 0
	prefixBytes := 0
	var prevKey []byte

	for i := range ordered {
		e := ordered[i]
		if prevKey != nil {
			prefixPairs++
			prefixBytes += sharedPrefixLen(prevKey, e.Key)
		}
		prevKey = e.Key

		if e.Flags&FlagTombstone != 0 {
			deleteCount++
			continue
		}
		putCount++
		if e.Flags&FlagPointer != 0 {
			pointerCount++
		}
	}

	if putCount == 0 {
		return base
	}
	pointerRatio := float64(pointerCount) / float64(putCount)
	deleteRatio := float64(deleteCount) / float64(len(ordered))
	avgPrefix := 0.0
	if prefixPairs > 0 {
		avgPrefix = float64(prefixBytes) / float64(prefixPairs)
	}

	out := base
	if out.LeafColumnar {
		if pointerRatio < 0.20 {
			out.LeafColumnar = false
		}
		if avgPrefix >= 6.0 && pointerRatio < 0.35 {
			out.LeafColumnar = false
		}
		if deleteRatio > 0.35 && pointerRatio < 0.40 {
			out.LeafColumnar = false
		}
	}
	if out.LeafPrefixCompression {
		if avgPrefix < 1.5 && pointerRatio > 0.75 {
			out.LeafPrefixCompression = false
		}
		if avgPrefix < 1.0 && deleteRatio > 0.40 {
			out.LeafPrefixCompression = false
		}
	}
	return out
}

func leafHeuristicEntriesSorted(entries []LeafHeuristicEntry) bool {
	for i := 1; i < len(entries); i++ {
		cmp := bytes.Compare(entries[i-1].Key, entries[i].Key)
		if cmp > 0 || (cmp == 0 && entries[i-1].Flags > entries[i].Flags) {
			return false
		}
	}
	return true
}

type leafColumnarV2Entry struct {
	keyOff   uint32
	keyLen   uint16
	valueOff uint32
	valueLen uint16
	valPtr   page.ValuePtr
	flags    byte
	revision page.EntryRevision
}

type leafColumnarPrefixV2Entry struct {
	suffixOff uint32
	suffixLen uint16
	valueOff  uint32
	valueLen  uint16
	valPtr    page.ValuePtr
	flags     byte
	prefixLen uint16
	revision  page.EntryRevision
}

type internalBaseDeltaEntry struct {
	key   []byte
	child uint64
}

// NewBuilder initializes a builder for the given buffer.
func NewBuilder(data []byte, pType page.PageType) *Builder {
	return NewBuilderWithOptions(data, pType, BuilderOptions{})
}

func NewBuilderWithOptions(data []byte, pType page.PageType, opts BuilderOptions) *Builder {
	b := &Builder{}
	b.ResetWithOptions(data, pType, opts)
	return b
}

// ResetWithOptions reinitializes an existing builder instance for reuse.
func (b *Builder) ResetWithOptions(data []byte, pType page.PageType, opts BuilderOptions) {
	b.ReleaseScratch()

	leafPrefix := opts.LeafPrefixCompression
	leafColumnarV2 := pType == page.PageTypeLeaf && opts.LeafColumnar && !leafPrefix
	heapStart := len(data)
	*b = Builder{
		data:                  data,
		pType:                 pType,
		dirEnd:                NodeHeaderSize,
		heapStart:             heapStart,
		leafPrefixCompression: leafPrefix,
		leafPrefixV2:          leafPrefix,
		leafColumnar:          opts.LeafColumnar,
		leafColumnarV2:        leafColumnarV2,
		leafPackedValuePtr:    opts.PackedValuePtr,
		leafEntryRevisions:    pType == page.PageTypeLeaf && opts.EntryRevisions,
		internalBaseDelta:     opts.InternalBaseDelta,
	}
	if pType == page.PageTypeLeaf && opts.LeafColumnar {
		if leafPrefix {
			b.leafColumnarPrefixV2AllInline = true
			b.leafColumnarPrefixV2AllPointer = true
			if pooled, ok := leafColumnarPrefixV2EntriesPool.Get().(*leafColumnarPrefixV2EntriesHandle); ok {
				if cap(pooled.entries) == 0 {
					pooled.entries = make([]leafColumnarPrefixV2Entry, 0, leafColumnarEntriesPoolInitCap)
				}
				b.leafColumnarPrefixV2EntriesH = pooled
				b.leafColumnarPrefixV2Entries = pooled.entries[:0]
			}
		} else if leafColumnarV2 {
			if pooled, ok := leafColumnarV2EntriesPool.Get().(*leafColumnarV2EntriesHandle); ok {
				if cap(pooled.entries) == 0 {
					pooled.entries = make([]leafColumnarV2Entry, 0, leafColumnarEntriesPoolInitCap)
				}
				b.leafColumnarV2EntriesH = pooled
				b.leafColumnarV2Entries = pooled.entries[:0]
			}
		}
	}
	if pType == page.PageTypeInternal && opts.InternalBaseDelta {
		if pooled, ok := internalBaseEntriesPool.Get().(*internalBaseEntriesHandle); ok {
			if cap(pooled.entries) == 0 {
				pooled.entries = make([]internalBaseDeltaEntry, 0, internalBaseEntriesPoolInitCap)
			}
			b.internalBaseEntriesH = pooled
			b.internalBaseEntries = pooled.entries[:0]
		}
		if arena, ok := internalBaseArenaPool.Get().(*byteArenaHandle); ok {
			if arena.buf == nil {
				arena.buf = make([]byte, 0, page.PageSize)
			}
			b.internalBaseArenaH = arena
			b.internalBaseArena = arena.buf[:0]
		}
	}
}

// ReleaseScratch returns pooled scratch resources held by the builder and
// drops references so the builder can be reused safely.
func (b *Builder) ReleaseScratch() {
	if b == nil {
		return
	}
	b.releaseLeafColumnarV2Scratch()
	b.releaseLeafColumnarPrefixV2Scratch()
	b.releaseInternalBaseDeltaScratch()
	b.data = nil
	b.leafPrevKey = nil
	b.internalFenceLow = nil
	b.internalFenceHigh = nil
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

// LeafEntryRevisionsEnabled reports whether this leaf builder will encode
// native per-entry revision metadata.
func (b *Builder) LeafEntryRevisionsEnabled() bool {
	return b != nil && b.pType == page.PageTypeLeaf && b.leafEntryRevisions
}

// SetInternalFenceBounds configures exact subtree bounds for internal pages.
// low is inclusive and high is exclusive; nil high means unbounded.
func (b *Builder) SetInternalFenceBounds(low, high []byte) {
	if b.pType != page.PageTypeInternal {
		return
	}
	b.internalFenceBounds = true
	if len(low) == 0 {
		b.internalFenceLow = nil
	} else {
		b.internalFenceLow = append(b.internalFenceLow[:0], low...)
	}
	if len(high) == 0 {
		b.internalFenceHigh = nil
	} else {
		b.internalFenceHigh = append(b.internalFenceHigh[:0], high...)
	}
}

// SetInternalFenceBoundsBorrowed configures exact subtree bounds without
// copying the key slices. Callers must keep low and high immutable until
// Finish or FinishNoNode copies the bounds into the encoded page.
func (b *Builder) SetInternalFenceBoundsBorrowed(low, high []byte) {
	if b.pType != page.PageTypeInternal {
		return
	}
	b.internalFenceBounds = true
	if len(low) == 0 {
		b.internalFenceLow = nil
	} else {
		b.internalFenceLow = low
	}
	if len(high) == 0 {
		b.internalFenceHigh = nil
	} else {
		b.internalFenceHigh = high
	}
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

// LeafEntrySizeWithRevision returns the encoded size for a revision-bearing
// leaf entry. Callers that use this must build the page with EntryRevisions.
func (b *Builder) LeafEntrySizeWithRevision(key, value []byte, flags byte, revision page.EntryRevision) int {
	_ = revision
	entrySize, _, _ := b.leafEntrySize(key, value, flags)
	if !b.leafEntryRevisions {
		entrySize += page.EntryRevisionSize
	}
	return entrySize
}

func (b *Builder) leafEntrySize(key, value []byte, flags byte) (entrySize int, prefixLen int, suffixLen int) {
	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}
	if b.leafColumnar && b.leafPrefixCompression {
		prefixLen = 0
		suffixLen = len(key)
		if b.leafIndex%leafPrefixRestartInterval != 0 && len(b.leafPrevKey) > 0 {
			prefixLen = sharedPrefixLen(key, b.leafPrevKey)
			if prefixLen > len(key) {
				prefixLen = len(key)
			}
		}
		suffixLen = len(key) - prefixLen
		valSize := 0
		if flags&FlagPointer != 0 {
			valSize = valPtrSize
		} else if flags&FlagTombstone == 0 {
			valSize = len(value)
		}
		entrySize = suffixLen + valSize + leafColumnarPrefixV2MetaSize
		if b.leafEntryRevisions {
			entrySize += page.EntryRevisionSize
		}
		return entrySize, prefixLen, suffixLen
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
		if b.leafEntryRevisions {
			entrySize += page.EntryRevisionSize
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
		// Combined columnar+prefix mode uses top-level metadata columns and
		// separate value/suffix blobs.
		entrySize = leafColumnarPrefixV2MetaSize + valSize + suffixLen
		return entrySize, prefixLen, suffixLen
	}

	entrySize = headerSize + suffixLen + valSize
	if b.leafEntryRevisions {
		entrySize += page.EntryRevisionSize
	}
	return entrySize, prefixLen, suffixLen
}

// AddLeafEntry appends a leaf entry. Assumes keys are added in sorted order.
func (b *Builder) AddLeafEntry(key, value []byte, flags byte, valPtr page.ValuePtr) error {
	return b.AddLeafEntryWithRevision(key, value, flags, valPtr, page.LegacyEntryRevision)
}

// AddLeafEntryWithRevision appends a native revision-bearing leaf entry.
func (b *Builder) AddLeafEntryWithRevision(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}
	if err := b.requireLeafEntryRevisions(revision); err != nil {
		return err
	}
	if b.leafColumnar && !b.leafPrefixCompression {
		if b.leafColumnarV2 {
			return b.addLeafEntryColumnarV2(key, value, flags, valPtr, revision)
		}
		return b.addLeafEntryColumnar(key, value, flags, valPtr, revision)
	}

	// 1. Calculate Entry Size
	// KeyPrefixLen(2) + KeySuffixLen(2) + ValLen(4) + Flags(1) + KeySuffix + Value/Ptr
	entrySize, prefixLen, suffixLen := b.leafEntrySize(key, value, flags)
	return b.AddLeafEntryWithPrefixRevision(key, value, flags, valPtr, revision, entrySize, prefixLen, suffixLen)
}

func (b *Builder) requireLeafEntryRevisions(revision page.EntryRevision) error {
	if revision == page.LegacyEntryRevision || b.leafEntryRevisions {
		return nil
	}
	if b.pType == page.PageTypeLeaf && b.count == 0 {
		b.leafEntryRevisions = true
		return nil
	}
	if b.pType == page.PageTypeLeaf {
		return ErrNodeFull
	}
	return ErrInvalidType
}

func (b *Builder) addLeafEntryColumnar(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
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
	if b.leafEntryRevisions {
		entrySize += page.EntryRevisionSize
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
			encodePackedValuePtrAt(b.data, valueStart, valPtr)
		} else {
			encodeValuePtrAt(b.data, valueStart, valPtr)
		}
	} else if flags&FlagTombstone == 0 {
		copy(b.data[valueStart:valueStart+valLen], value)
	}

	keyStart := valueStart + valSize
	copy(b.data[keyStart:keyStart+len(key)], key)
	if b.leafEntryRevisions {
		revisionStart := keyStart + len(key)
		binary.LittleEndian.PutUint64(b.data[revisionStart:revisionStart+page.EntryRevisionSize], uint64(revision))
	}

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	b.leafIndex++
	return nil
}

func (b *Builder) addLeafEntryColumnarV2(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision) error {
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
	if b.leafEntryRevisions {
		entrySize += page.EntryRevisionSize
	}
	required := entrySize + DirectoryEntrySize // key offset slot
	freeSpace := b.heapStart - b.dirEnd
	if freeSpace < required {
		return ErrNodeFull
	}

	keyOff, keyLen := b.leafColumnarV2AppendBytes(key)
	valueOff, valueLen := uint32(0), uint16(0)
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		valueOff, valueLen = b.leafColumnarV2AppendBytes(value)
	}

	b.leafColumnarV2Entries = append(b.leafColumnarV2Entries, leafColumnarV2Entry{
		keyOff:   keyOff,
		keyLen:   keyLen,
		valueOff: valueOff,
		valueLen: valueLen,
		valPtr:   valPtr,
		flags:    flags,
		revision: revision,
	})
	b.leafColumnarV2KeyBytes += len(key)
	b.leafColumnarV2ValBytes += valSize

	b.dirEnd += DirectoryEntrySize + leafColumnarV2MetaSize
	if b.leafEntryRevisions {
		b.dirEnd += page.EntryRevisionSize
	}
	b.heapStart -= len(key) + valSize
	b.count++
	b.leafIndex++
	return nil
}

func (b *Builder) addLeafEntryColumnarPrefixV2(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision, entrySize, prefixLen, suffixLen int) error {
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

	if b.leafIndex%leafPrefixRestartInterval == 0 {
		prefixLen = 0
		suffixLen = len(key)
	}
	if prefixLen < 0 || prefixLen > len(key) || suffixLen != len(key)-prefixLen {
		return ErrCorruptedNode
	}
	if prefixLen > int(^uint16(0)) || suffixLen > int(^uint16(0)) {
		return ErrKeyTooLarge
	}
	_ = entrySize // Size was computed by caller; capacity check below is exact.

	nextCount := int(b.count) + 1
	nextKeyBytes := b.leafColumnarPrefixV2KeyBytes + suffixLen
	nextValBytes := b.leafColumnarPrefixV2ValBytes + valSize
	dirEnd := NodeHeaderSize + nextCount*leafColumnarPrefixV2MetaSize
	if b.leafEntryRevisions {
		dirEnd += nextCount * page.EntryRevisionSize
	}
	heapStart := len(b.data) - (nextKeyBytes + nextValBytes)
	if heapStart < dirEnd {
		return ErrNodeFull
	}

	suffixOff, suffixLenU16 := b.leafColumnarV2AppendBytes(key[prefixLen:])
	valueOff, valueLen := uint32(0), uint16(0)
	if flags&FlagPointer == 0 && flags&FlagTombstone == 0 {
		valueOff, valueLen = b.leafColumnarPrefixV2AppendValueBytes(value)
	} else {
		b.leafColumnarPrefixV2AllInline = false
	}
	if flags&FlagPointer == 0 {
		b.leafColumnarPrefixV2AllPointer = false
	}

	b.leafColumnarPrefixV2Entries = append(b.leafColumnarPrefixV2Entries, leafColumnarPrefixV2Entry{
		suffixOff: suffixOff,
		suffixLen: suffixLenU16,
		valueOff:  valueOff,
		valueLen:  valueLen,
		valPtr:    valPtr,
		flags:     flags,
		prefixLen: uint16(prefixLen),
		revision:  revision,
	})
	b.leafColumnarPrefixV2KeyBytes = nextKeyBytes
	b.leafColumnarPrefixV2ValBytes = nextValBytes
	b.count++
	b.leafIndex++
	b.dirEnd = dirEnd
	b.heapStart = heapStart

	if len(key) <= len(b.leafPrevKeyBuf) {
		b.leafPrevKey = b.leafPrevKeyBuf[:len(key)]
	} else {
		if cap(b.leafPrevKey) < len(key) {
			b.leafPrevKey = make([]byte, len(key))
		}
		b.leafPrevKey = b.leafPrevKey[:len(key)]
	}
	copy(b.leafPrevKey, key)
	return nil
}

// AddLeafEntryWithPrefix appends a leaf entry using precomputed size/prefix data.
// The caller must ensure prefixLen/suffixLen are computed for this builder state.
func (b *Builder) AddLeafEntryWithPrefix(key, value []byte, flags byte, valPtr page.ValuePtr, entrySize, prefixLen, suffixLen int) error {
	return b.AddLeafEntryWithPrefixRevision(key, value, flags, valPtr, page.LegacyEntryRevision, entrySize, prefixLen, suffixLen)
}

// AddLeafEntryWithPrefixRevision appends a native revision-bearing leaf entry
// using precomputed size/prefix data.
func (b *Builder) AddLeafEntryWithPrefixRevision(key, value []byte, flags byte, valPtr page.ValuePtr, revision page.EntryRevision, entrySize, prefixLen, suffixLen int) error {
	if b.pType != page.PageTypeLeaf {
		return ErrInvalidType
	}
	if err := b.requireLeafEntryRevisions(revision); err != nil {
		return err
	}

	if b.leafColumnar {
		if b.leafPrefixCompression {
			return b.addLeafEntryColumnarPrefixV2(key, value, flags, valPtr, revision, entrySize, prefixLen, suffixLen)
		}
		if b.leafColumnarV2 {
			return b.addLeafEntryColumnarV2(key, value, flags, valPtr, revision)
		}
		return b.addLeafEntryColumnar(key, value, flags, valPtr, revision)
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
		if b.leafPackedValuePtr {
			encodePackedValuePtrAt(b.data, valueStart, valPtr)
		} else {
			encodeValuePtrAt(b.data, valueStart, valPtr)
		}
	} else {
		copy(b.data[valueStart:valueStart+len(value)], value)
	}
	if b.leafEntryRevisions {
		valSize := 0
		if flags&FlagPointer != 0 {
			if b.leafPackedValuePtr {
				valSize = page.PackedValuePtrSize
			} else {
				valSize = page.ValuePtrSize
			}
		} else if flags&FlagTombstone == 0 {
			valSize = len(value)
		}
		revisionStart := valueStart + valSize
		binary.LittleEndian.PutUint64(b.data[revisionStart:revisionStart+page.EntryRevisionSize], uint64(revision))
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
	return b.AddInternalChildRef(key, page.PageChildRef(childPageID))
}

// AddInternalChildRef appends a typed child reference. All children in a single
// internal page must have the same storage kind. Leaf-log refs use an explicit
// first-class on-disk record instead of the historical packed uint64 LeafRef.
//
// TODO(treedb-format): leaf-log child pages can be compacted later by hoisting
// common fields such as kind/file id to the page header and narrowing offsets
// when segment sizing gives us a hard bound.
func (b *Builder) AddInternalChildRef(key []byte, ref page.ChildRef) error {
	if b.pType != page.PageTypeInternal {
		return ErrInvalidType
	}
	if ref.Kind == page.ChildRefLeafLog {
		return b.addInternalLeafLogChild(key, ref.Log)
	}
	if b.internalLeafLogRefs {
		return ErrInvalidType
	}

	if b.internalBaseDelta {
		return b.addInternalChildBaseDelta(key, ref.Page)
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
	putUint64(b.data[ptr+2:ptr+10], ref.Page)
	copy(b.data[ptr+10:], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++

	return nil
}

func (b *Builder) addInternalLeafLogChild(key []byte, ref page.LogRecordRef) error {
	if b.internalBaseDelta {
		return ErrInvalidType
	}
	if b.count > 0 && !b.internalLeafLogRefs {
		return ErrInvalidType
	}
	b.internalLeafLogRefs = true

	entrySize := internalLeafLogRefHeaderSize + len(key)
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
	page.EncodeLogRecordRef(b.data[ptr+2:ptr+2+page.LogRecordRefSize], ref)
	copy(b.data[ptr+internalLeafLogRefHeaderSize:], key)

	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)

	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	return nil
}

// AddInternalLeafLogChildFromNode appends an already-encoded leaf-log child
// entry from src. Zipper path-copy rebuilds use this when an internal child is
// unchanged, avoiding LogRecordRef decode/re-encode work while preserving the
// encoded on-page representation exactly.
func (b *Builder) AddInternalLeafLogChildFromNode(src *Node, index uint16) error {
	if b.pType != page.PageTypeInternal || src == nil || !src.internalLeafLogRefs() {
		return ErrInvalidType
	}
	if b.internalBaseDelta {
		return ErrInvalidType
	}
	if b.count > 0 && !b.internalLeafLogRefs {
		return ErrInvalidType
	}

	offset, err := src.getOffset(index)
	if err != nil {
		return err
	}
	ptr := int(offset)
	if ptr+internalLeafLogRefHeaderSize > len(src.data) {
		return ErrCorruptedNode
	}
	keyLen := int(binary.LittleEndian.Uint16(src.data[ptr : ptr+2]))
	entrySize := internalLeafLogRefHeaderSize + keyLen
	if ptr+entrySize > len(src.data) {
		return ErrCorruptedNode
	}
	required := entrySize + DirectoryEntrySize
	if b.heapStart-b.dirEnd < required {
		return ErrNodeFull
	}

	entryStart := b.heapStart - entrySize
	copy(b.data[entryStart:entryStart+entrySize], src.data[ptr:ptr+entrySize])
	b.data[b.dirEnd] = byte(entryStart)
	b.data[b.dirEnd+1] = byte(entryStart >> 8)
	b.internalLeafLogRefs = true
	b.heapStart = entryStart
	b.dirEnd += DirectoryEntrySize
	b.count++
	return nil
}

func (b *Builder) finalize() {
	internalBaseDeltaApplied := false
	if b.pType == page.PageTypeInternal && b.internalBaseDelta {
		internalBaseDeltaApplied = b.finishInternalBaseDelta()
	}
	if b.pType == page.PageTypeLeaf && b.leafColumnar && b.leafPrefixCompression {
		b.finishLeafColumnarPrefixV2()
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
		if b.leafEntryRevisions {
			flags |= leafEntryRevisionFlag
		}
	} else if b.pType == page.PageTypeInternal {
		if b.internalLeafLogRefs {
			flags |= internalLeafLogRefsFlag
		} else if internalBaseDeltaApplied {
			flags |= internalBaseDeltaFlag
			if b.internalBaseDeltaW == 2 {
				flags |= internalBaseDeltaU16Flag
			}
			if b.internalFenceBounds {
				flags |= internalFenceBoundsFlag
			}
		}
	}
	binary.LittleEndian.PutUint16(b.data[12:14], flags)
	binary.LittleEndian.PutUint16(b.data[14:16], b.count)

	page.UpdateChecksum(b.data)
}

// Finish finalizes the page header and checksum.
// Returns a Node wrapper for convenience.
func (b *Builder) Finish() *Node {
	b.finalize()
	return &Node{
		data:  b.data,
		count: b.count,
		ptype: b.pType,
	}
}

// FinishNoNode finalizes the page header and checksum without allocating a
// Node wrapper. Use this in hot paths that only need the encoded page bytes.
func (b *Builder) FinishNoNode() {
	b.finalize()
}

func (b *Builder) finishLeafColumnarV2() {
	count := int(b.count)
	if count == 0 {
		b.dirEnd = NodeHeaderSize
		b.heapStart = len(b.data)
		b.releaseLeafColumnarV2Scratch()
		return
	}

	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}

	keyDirStart := NodeHeaderSize
	valDirStart := keyDirStart + count*DirectoryEntrySize
	flagsStart := valDirStart + count*DirectoryEntrySize
	revisionsStart := flagsStart + count
	metaEnd := revisionsStart
	if b.leafEntryRevisions {
		metaEnd += count * page.EntryRevisionSize
	}

	keysStart := len(b.data) - b.leafColumnarV2KeyBytes
	valuesStart := keysStart - b.leafColumnarV2ValBytes
	if valuesStart < metaEnd {
		panic("leaf columnar v2 packing overflow")
	}

	valOff := valuesStart
	keyOff := keysStart

	for i := 0; i < count; i++ {
		e := &b.leafColumnarV2Entries[i]
		keyDirPos := keyDirStart + i*2
		valDirPos := valDirStart + i*2
		putUint16At(b.data, keyDirPos, uint16(keyOff))
		putUint16At(b.data, valDirPos, uint16(valOff))
		b.data[flagsStart+i] = e.flags
		if b.leafEntryRevisions {
			binary.LittleEndian.PutUint64(b.data[revisionsStart+i*page.EntryRevisionSize:], uint64(e.revision))
		}

		if e.flags&FlagPointer != 0 {
			if b.leafPackedValuePtr {
				encodePackedValuePtrAt(b.data, valOff, e.valPtr)
			} else {
				encodeValuePtrAt(b.data, valOff, e.valPtr)
			}
			valOff += valPtrSize
		} else if e.flags&FlagTombstone == 0 {
			valueStart := int(e.valueOff)
			valueEnd := valueStart + int(e.valueLen)
			copy(b.data[valOff:valOff+int(e.valueLen)], b.leafColumnarV2Arena[valueStart:valueEnd])
			valOff += int(e.valueLen)
		}

		keyStart := int(e.keyOff)
		keyEnd := keyStart + int(e.keyLen)
		copy(b.data[keyOff:keyOff+int(e.keyLen)], b.leafColumnarV2Arena[keyStart:keyEnd])
		keyOff += int(e.keyLen)
	}

	if valOff != keysStart || keyOff != len(b.data) {
		panic("leaf columnar v2 packing mismatch")
	}

	b.dirEnd = metaEnd
	b.heapStart = valuesStart

	b.releaseLeafColumnarV2Scratch()
}

func (b *Builder) finishLeafColumnarPrefixV2() {
	count := int(b.count)
	if count == 0 {
		b.dirEnd = NodeHeaderSize
		b.heapStart = len(b.data)
		b.releaseLeafColumnarPrefixV2Scratch()
		return
	}

	valPtrSize := page.ValuePtrSize
	if b.leafPackedValuePtr {
		valPtrSize = page.PackedValuePtrSize
	}

	keyDirStart := NodeHeaderSize
	valDirStart := keyDirStart + count*DirectoryEntrySize
	flagsStart := valDirStart + count*DirectoryEntrySize
	prefixStart := flagsStart + count
	revisionsStart := prefixStart + count*DirectoryEntrySize
	metaEnd := revisionsStart
	if b.leafEntryRevisions {
		metaEnd += count * page.EntryRevisionSize
	}

	suffixStart := len(b.data) - b.leafColumnarPrefixV2KeyBytes
	valuesStart := suffixStart - b.leafColumnarPrefixV2ValBytes
	if valuesStart < metaEnd {
		panic("leaf columnar+prefix v2 packing overflow")
	}

	entries := b.leafColumnarPrefixV2Entries
	suffixArena := b.leafColumnarV2Arena
	valueArena := b.leafColumnarPrefixV2ValueArena
	allInline := b.leafColumnarPrefixV2AllInline
	allPointer := b.leafColumnarPrefixV2AllPointer
	flagsCol := b.data[flagsStart:prefixStart]
	prefixCol := b.data[prefixStart:revisionsStart]
	writeRevision := func(i int, revision page.EntryRevision) {
		if b.leafEntryRevisions {
			binary.LittleEndian.PutUint64(b.data[revisionsStart+i*page.EntryRevisionSize:], uint64(revision))
		}
	}

	if allInline {
		if len(valueArena) != b.leafColumnarPrefixV2ValBytes || len(suffixArena) != b.leafColumnarPrefixV2KeyBytes {
			panic("leaf columnar+prefix v2 inline arena size mismatch")
		}
		copy(b.data[valuesStart:suffixStart], valueArena)
		copy(b.data[suffixStart:], suffixArena)

		if builderNativeLittleEndian {
			keyDirU16 := bytesAsUint16Slice(b.data[keyDirStart:valDirStart])
			valDirU16 := bytesAsUint16Slice(b.data[valDirStart:flagsStart])
			if prefixStart&1 == 0 {
				prefixDirU16 := bytesAsUint16Slice(prefixCol)
				for i := 0; i < count; i++ {
					e := &entries[i]
					keyDirU16[i] = uint16(suffixStart + int(e.suffixOff))
					valDirU16[i] = uint16(valuesStart + int(e.valueOff))
					flagsCol[i] = e.flags
					prefixDirU16[i] = e.prefixLen
					writeRevision(i, e.revision)
				}
			} else {
				for i := 0; i < count; i++ {
					e := &entries[i]
					keyDirU16[i] = uint16(suffixStart + int(e.suffixOff))
					valDirU16[i] = uint16(valuesStart + int(e.valueOff))
					flagsCol[i] = e.flags
					putUint16At(prefixCol, i*2, e.prefixLen)
					writeRevision(i, e.revision)
				}
			}
		} else {
			for i := 0; i < count; i++ {
				e := &entries[i]
				putUint16At(b.data, keyDirStart+i*2, uint16(suffixStart+int(e.suffixOff)))
				putUint16At(b.data, valDirStart+i*2, uint16(valuesStart+int(e.valueOff)))
				flagsCol[i] = e.flags
				putUint16At(b.data, prefixStart+i*2, e.prefixLen)
				writeRevision(i, e.revision)
			}
		}

		b.dirEnd = metaEnd
		b.heapStart = valuesStart
		b.releaseLeafColumnarPrefixV2Scratch()
		return
	}

	if allPointer {
		if len(suffixArena) != b.leafColumnarPrefixV2KeyBytes {
			panic("leaf columnar+prefix v2 pointer arena size mismatch")
		}
		copy(b.data[suffixStart:], suffixArena)

		valOff := valuesStart
		if builderNativeLittleEndian {
			keyDirU16 := bytesAsUint16Slice(b.data[keyDirStart:valDirStart])
			valDirU16 := bytesAsUint16Slice(b.data[valDirStart:flagsStart])
			if prefixStart&1 == 0 {
				prefixDirU16 := bytesAsUint16Slice(prefixCol)
				for i := 0; i < count; i++ {
					e := &entries[i]
					keyDirU16[i] = uint16(suffixStart + int(e.suffixOff))
					valDirU16[i] = uint16(valOff)
					flagsCol[i] = e.flags
					prefixDirU16[i] = e.prefixLen
					writeRevision(i, e.revision)
					if b.leafPackedValuePtr {
						encodePackedValuePtrAt(b.data, valOff, e.valPtr)
					} else {
						encodeValuePtrAt(b.data, valOff, e.valPtr)
					}
					valOff += valPtrSize
				}
			} else {
				for i := 0; i < count; i++ {
					e := &entries[i]
					keyDirU16[i] = uint16(suffixStart + int(e.suffixOff))
					valDirU16[i] = uint16(valOff)
					flagsCol[i] = e.flags
					putUint16At(prefixCol, i*2, e.prefixLen)
					writeRevision(i, e.revision)
					if b.leafPackedValuePtr {
						encodePackedValuePtrAt(b.data, valOff, e.valPtr)
					} else {
						encodeValuePtrAt(b.data, valOff, e.valPtr)
					}
					valOff += valPtrSize
				}
			}
		} else {
			for i := 0; i < count; i++ {
				e := &entries[i]
				putUint16At(b.data, keyDirStart+i*2, uint16(suffixStart+int(e.suffixOff)))
				putUint16At(b.data, valDirStart+i*2, uint16(valOff))
				flagsCol[i] = e.flags
				putUint16At(b.data, prefixStart+i*2, e.prefixLen)
				writeRevision(i, e.revision)
				if b.leafPackedValuePtr {
					encodePackedValuePtrAt(b.data, valOff, e.valPtr)
				} else {
					encodeValuePtrAt(b.data, valOff, e.valPtr)
				}
				valOff += valPtrSize
			}
		}

		if valOff != suffixStart {
			panic("leaf columnar+prefix v2 pointer packing mismatch")
		}

		b.dirEnd = metaEnd
		b.heapStart = valuesStart
		b.releaseLeafColumnarPrefixV2Scratch()
		return
	}

	valOff := valuesStart
	keyOff := suffixStart

	if builderNativeLittleEndian {
		keyDirU16 := bytesAsUint16Slice(b.data[keyDirStart:valDirStart])
		valDirU16 := bytesAsUint16Slice(b.data[valDirStart:flagsStart])

		if prefixStart&1 == 0 {
			prefixDirU16 := bytesAsUint16Slice(prefixCol)
			for i := 0; i < count; i++ {
				e := &entries[i]
				keyDirU16[i] = uint16(keyOff)
				valDirU16[i] = uint16(valOff)
				flagsCol[i] = e.flags
				prefixDirU16[i] = e.prefixLen
				writeRevision(i, e.revision)

				if e.flags&FlagPointer != 0 {
					if b.leafPackedValuePtr {
						encodePackedValuePtrAt(b.data, valOff, e.valPtr)
					} else {
						encodeValuePtrAt(b.data, valOff, e.valPtr)
					}
					valOff += valPtrSize
				} else if e.flags&FlagTombstone == 0 {
					valueLen := int(e.valueLen)
					valueStart := int(e.valueOff)
					copy(b.data[valOff:valOff+valueLen], valueArena[valueStart:valueStart+valueLen])
					valOff += valueLen
				}

				suffixLen := int(e.suffixLen)
				suffixStart := int(e.suffixOff)
				copy(b.data[keyOff:keyOff+suffixLen], suffixArena[suffixStart:suffixStart+suffixLen])
				keyOff += suffixLen
			}
		} else {
			for i := 0; i < count; i++ {
				e := &entries[i]
				keyDirU16[i] = uint16(keyOff)
				valDirU16[i] = uint16(valOff)
				flagsCol[i] = e.flags
				putUint16At(prefixCol, i*2, e.prefixLen)
				writeRevision(i, e.revision)

				if e.flags&FlagPointer != 0 {
					if b.leafPackedValuePtr {
						encodePackedValuePtrAt(b.data, valOff, e.valPtr)
					} else {
						encodeValuePtrAt(b.data, valOff, e.valPtr)
					}
					valOff += valPtrSize
				} else if e.flags&FlagTombstone == 0 {
					valueLen := int(e.valueLen)
					valueStart := int(e.valueOff)
					copy(b.data[valOff:valOff+valueLen], valueArena[valueStart:valueStart+valueLen])
					valOff += valueLen
				}

				suffixLen := int(e.suffixLen)
				suffixStart := int(e.suffixOff)
				copy(b.data[keyOff:keyOff+suffixLen], suffixArena[suffixStart:suffixStart+suffixLen])
				keyOff += suffixLen
			}
		}
	} else {
		for i := 0; i < count; i++ {
			e := &entries[i]
			keyDirPos := keyDirStart + i*2
			valDirPos := valDirStart + i*2
			prefixPos := prefixStart + i*2
			putUint16At(b.data, keyDirPos, uint16(keyOff))
			putUint16At(b.data, valDirPos, uint16(valOff))
			flagsCol[i] = e.flags
			putUint16At(b.data, prefixPos, e.prefixLen)
			writeRevision(i, e.revision)

			if e.flags&FlagPointer != 0 {
				if b.leafPackedValuePtr {
					encodePackedValuePtrAt(b.data, valOff, e.valPtr)
				} else {
					encodeValuePtrAt(b.data, valOff, e.valPtr)
				}
				valOff += valPtrSize
			} else if e.flags&FlagTombstone == 0 {
				valueLen := int(e.valueLen)
				valueStart := int(e.valueOff)
				copy(b.data[valOff:valOff+valueLen], valueArena[valueStart:valueStart+valueLen])
				valOff += valueLen
			}

			suffixLen := int(e.suffixLen)
			suffixStart := int(e.suffixOff)
			copy(b.data[keyOff:keyOff+suffixLen], suffixArena[suffixStart:suffixStart+suffixLen])
			keyOff += suffixLen
		}
	}

	if valOff != suffixStart || keyOff != len(b.data) {
		panic("leaf columnar+prefix v2 packing mismatch")
	}

	b.dirEnd = metaEnd
	b.heapStart = valuesStart

	b.releaseLeafColumnarPrefixV2Scratch()
}

func bytesAsUint16Slice(buf []byte) []uint16 {
	if len(buf) == 0 {
		return nil
	}
	return unsafe.Slice((*uint16)(unsafe.Pointer(&buf[0])), len(buf)/2)
}

func (b *Builder) releaseLeafColumnarV2Scratch() {
	if b.leafColumnarV2EntriesH != nil {
		if cap(b.leafColumnarV2Entries) <= leafColumnarEntriesPoolMaxCap {
			b.leafColumnarV2EntriesH.entries = b.leafColumnarV2Entries[:0]
		} else {
			b.leafColumnarV2EntriesH.entries = nil
		}
		leafColumnarV2EntriesPool.Put(b.leafColumnarV2EntriesH)
		b.leafColumnarV2EntriesH = nil
	}
	b.leafColumnarV2Entries = nil
	b.leafColumnarV2KeyBytes = 0
	b.leafColumnarV2ValBytes = 0
	if b.leafColumnarV2ArenaH != nil {
		b.leafColumnarV2ArenaH.buf = b.leafColumnarV2Arena[:0]
		leafColumnarV2ArenaPool.Put(b.leafColumnarV2ArenaH)
		b.leafColumnarV2ArenaH = nil
	}
	b.leafColumnarV2Arena = nil
}

func (b *Builder) releaseLeafColumnarPrefixV2Scratch() {
	if b.leafColumnarPrefixV2EntriesH != nil {
		if cap(b.leafColumnarPrefixV2Entries) <= leafColumnarEntriesPoolMaxCap {
			b.leafColumnarPrefixV2EntriesH.entries = b.leafColumnarPrefixV2Entries[:0]
		} else {
			b.leafColumnarPrefixV2EntriesH.entries = nil
		}
		leafColumnarPrefixV2EntriesPool.Put(b.leafColumnarPrefixV2EntriesH)
		b.leafColumnarPrefixV2EntriesH = nil
	}
	b.leafColumnarPrefixV2Entries = nil
	b.leafColumnarPrefixV2KeyBytes = 0
	b.leafColumnarPrefixV2ValBytes = 0
	b.leafColumnarPrefixV2AllInline = false
	b.leafColumnarPrefixV2AllPointer = false
	if b.leafColumnarV2ArenaH != nil {
		b.leafColumnarV2ArenaH.buf = b.leafColumnarV2Arena[:0]
		leafColumnarV2ArenaPool.Put(b.leafColumnarV2ArenaH)
		b.leafColumnarV2ArenaH = nil
	}
	b.leafColumnarV2Arena = nil
	if b.leafColumnarPrefixV2ValueArenaH != nil {
		b.leafColumnarPrefixV2ValueArenaH.buf = b.leafColumnarPrefixV2ValueArena[:0]
		leafColumnarPrefixV2ValueArenaPool.Put(b.leafColumnarPrefixV2ValueArenaH)
		b.leafColumnarPrefixV2ValueArenaH = nil
	}
	b.leafColumnarPrefixV2ValueArena = nil
}

func (b *Builder) releaseInternalBaseDeltaScratch() {
	if b.internalBaseEntriesH != nil {
		if cap(b.internalBaseEntries) <= internalBaseEntriesPoolMaxCap {
			clear(b.internalBaseEntries)
			b.internalBaseEntriesH.entries = b.internalBaseEntries[:0]
		} else {
			b.internalBaseEntriesH.entries = nil
		}
		internalBaseEntriesPool.Put(b.internalBaseEntriesH)
		b.internalBaseEntriesH = nil
	}
	b.internalBaseEntries = nil
	b.internalBaseTotalKeyBytes = 0
	b.internalBaseSharedPrefix = 0
	if b.internalBaseArenaH != nil {
		b.internalBaseArenaH.buf = b.internalBaseArena[:0]
		internalBaseArenaPool.Put(b.internalBaseArenaH)
		b.internalBaseArenaH = nil
	}
	b.internalBaseArena = nil
}

func (b *Builder) leafColumnarV2AppendBytes(src []byte) (off uint32, n uint16) {
	if len(src) == 0 {
		return 0, 0
	}
	if len(src) > int(^uint16(0)) {
		panic("leaf columnar blob too large")
	}
	if b.leafColumnarV2Arena == nil {
		arenaAny := leafColumnarV2ArenaPool.Get()
		arena := arenaAny.(*byteArenaHandle)
		if arena.buf == nil {
			arena.buf = make([]byte, 0, page.PageSize)
		}
		b.leafColumnarV2ArenaH = arena
		b.leafColumnarV2Arena = arena.buf[:0]
	}

	start := len(b.leafColumnarV2Arena)
	end := start + len(src)
	if end > cap(b.leafColumnarV2Arena) {
		expandedCap := cap(b.leafColumnarV2Arena) * 2
		if expandedCap < end {
			expandedCap = end
		}
		expanded := make([]byte, end, expandedCap)
		copy(expanded, b.leafColumnarV2Arena)
		b.leafColumnarV2Arena = expanded
	} else {
		b.leafColumnarV2Arena = b.leafColumnarV2Arena[:end]
	}
	copy(b.leafColumnarV2Arena[start:end], src)
	return uint32(start), uint16(len(src))
}

func (b *Builder) leafColumnarPrefixV2AppendValueBytes(src []byte) (off uint32, n uint16) {
	if len(src) == 0 && b.leafColumnarPrefixV2ValueArena == nil {
		// Avoid allocating an arena just to record an empty value.
		return 0, 0
	}
	if len(src) > int(^uint16(0)) {
		panic("leaf columnar prefix value too large")
	}
	if b.leafColumnarPrefixV2ValueArena == nil {
		arenaAny := leafColumnarPrefixV2ValueArenaPool.Get()
		arena := arenaAny.(*byteArenaHandle)
		if arena.buf == nil {
			arena.buf = make([]byte, 0, page.PageSize)
		}
		b.leafColumnarPrefixV2ValueArenaH = arena
		b.leafColumnarPrefixV2ValueArena = arena.buf[:0]
	}

	start := len(b.leafColumnarPrefixV2ValueArena)
	end := start + len(src)
	if end > cap(b.leafColumnarPrefixV2ValueArena) {
		expandedCap := cap(b.leafColumnarPrefixV2ValueArena) * 2
		if expandedCap < end {
			expandedCap = end
		}
		expanded := make([]byte, end, expandedCap)
		copy(expanded, b.leafColumnarPrefixV2ValueArena)
		b.leafColumnarPrefixV2ValueArena = expanded
	} else {
		b.leafColumnarPrefixV2ValueArena = b.leafColumnarPrefixV2ValueArena[:end]
	}
	copy(b.leafColumnarPrefixV2ValueArena[start:end], src)
	return uint32(start), uint16(len(src))
}

func (b *Builder) internalBaseCopyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	if b.internalBaseArena == nil {
		arenaAny := internalBaseArenaPool.Get()
		arena := arenaAny.(*byteArenaHandle)
		if arena.buf == nil {
			arena.buf = make([]byte, 0, page.PageSize)
		}
		b.internalBaseArenaH = arena
		b.internalBaseArena = arena.buf[:0]
	}

	start := len(b.internalBaseArena)
	end := start + len(src)
	if end > cap(b.internalBaseArena) {
		expandedCap := cap(b.internalBaseArena) * 2
		if expandedCap < end {
			expandedCap = end
		}
		expanded := make([]byte, end, expandedCap)
		copy(expanded, b.internalBaseArena)
		b.internalBaseArena = expanded
	} else {
		b.internalBaseArena = b.internalBaseArena[:end]
	}
	dst := b.internalBaseArena[start:end]
	copy(dst, src)
	return dst
}

func (b *Builder) addInternalChildBaseDelta(key []byte, childPageID uint64) error {
	if len(key) > int(^uint16(0)) {
		return ErrKeyTooLarge
	}
	if b.internalFenceBounds && (len(b.internalFenceLow) > int(^uint16(0)) || len(b.internalFenceHigh) > int(^uint16(0))) {
		return ErrKeyTooLarge
	}

	nextCount := int(b.count) + 1

	minChild := childPageID
	maxChild := childPageID
	if b.internalBaseHasChild {
		minChild = b.internalBaseMinChild
		maxChild = b.internalBaseMaxChild
		if childPageID < minChild {
			minChild = childPageID
		}
		if childPageID > maxChild {
			maxChild = childPageID
		}
	}

	maxDelta := maxChild - minChild
	if maxDelta > uint64(^uint32(0)) {
		return ErrInternalBaseDeltaOutOfRange
	}
	deltaWidth := 4
	if maxDelta <= uint64(^uint16(0)) {
		deltaWidth = 2
	}

	existingCount := len(b.internalBaseEntries)
	firstKey := key
	if existingCount > 0 {
		firstKey = b.internalBaseEntries[0].key
	}
	prefixLen := 0
	if nextCount > 1 {
		if existingCount > 1 {
			prefixLen = b.internalBaseSharedPrefix
			if p := sharedPrefixLen(firstKey, key); p < prefixLen {
				prefixLen = p
			}
		} else {
			prefixLen = sharedPrefixLen(firstKey, key)
		}
	}
	if prefixLen > int(^uint16(0)) {
		return ErrKeyTooLarge
	}

	existingSuffixBytes := b.internalBaseTotalKeyBytes - existingCount*prefixLen
	if existingSuffixBytes < 0 {
		return ErrCorruptedNode
	}
	totalSuffixBytes := existingSuffixBytes + len(key) - prefixLen

	lowLen := 0
	highLen := 0
	if b.internalFenceBounds {
		lowLen = len(b.internalFenceLow)
		highLen = len(b.internalFenceHigh)
	}
	footerBytes := internalBaseDeltaFooterTailSize + lowLen + highLen + prefixLen
	entriesBytes := nextCount*(2+deltaWidth) + totalSuffixBytes
	dirEnd := NodeHeaderSize + nextCount*DirectoryEntrySize
	heapStart := len(b.data) - footerBytes - entriesBytes
	if heapStart < dirEnd {
		return ErrNodeFull
	}

	keyCopy := b.internalBaseCopyBytes(key)
	b.internalBaseEntries = append(b.internalBaseEntries, internalBaseDeltaEntry{
		key:   keyCopy,
		child: childPageID,
	})
	b.count++
	b.internalBaseTotalKeyBytes += len(keyCopy)
	b.internalBaseHasChild = true
	b.internalBaseMinChild = minChild
	b.internalBaseMaxChild = maxChild
	b.internalBaseChildID = minChild
	b.internalBaseDeltaW = deltaWidth
	b.internalBaseSharedPrefix = prefixLen
	b.dirEnd = dirEnd
	b.heapStart = heapStart
	return nil
}

func (b *Builder) finishInternalBaseDelta() bool {
	count := int(b.count)
	if count == 0 {
		b.releaseInternalBaseDeltaScratch()
		return false
	}
	if len(b.internalBaseEntries) != count {
		b.releaseInternalBaseDeltaScratch()
		return false
	}

	firstKey := b.internalBaseEntries[0].key
	prefixLen := sharedPrefixLenAcrossEntries(b.internalBaseEntries)
	if prefixLen > int(^uint16(0)) {
		panic("internal base-delta prefix overflow")
	}

	baseChildID := b.internalBaseMinChild
	maxDelta := b.internalBaseMaxChild - baseChildID
	if maxDelta > uint64(^uint32(0)) {
		panic("internal base-delta child range overflow")
	}
	deltaWidth := 4
	if maxDelta <= uint64(^uint16(0)) {
		deltaWidth = 2
	}
	b.internalBaseDeltaW = deltaWidth
	b.internalBaseChildID = baseChildID

	totalSuffixBytes := 0
	for i := range b.internalBaseEntries {
		k := b.internalBaseEntries[i].key
		totalSuffixBytes += len(k) - prefixLen
	}

	lowLen := 0
	highLen := 0
	if b.internalFenceBounds {
		lowLen = len(b.internalFenceLow)
		highLen = len(b.internalFenceHigh)
	}
	footerBytes := internalBaseDeltaFooterTailSize + lowLen + highLen + prefixLen
	entriesBytes := count*(2+deltaWidth) + totalSuffixBytes

	dirEnd := NodeHeaderSize + count*DirectoryEntrySize
	footerStart := len(b.data) - footerBytes
	heapStart := footerStart - entriesBytes
	if heapStart < dirEnd {
		panic("internal base-delta packing overflow")
	}

	payloadPos := footerStart
	if lowLen > 0 {
		copy(b.data[payloadPos:payloadPos+lowLen], b.internalFenceLow)
		payloadPos += lowLen
	}
	if highLen > 0 {
		copy(b.data[payloadPos:payloadPos+highLen], b.internalFenceHigh)
		payloadPos += highLen
	}
	if prefixLen > 0 {
		copy(b.data[payloadPos:payloadPos+prefixLen], firstKey[:prefixLen])
		payloadPos += prefixLen
	}

	tailPos := len(b.data) - internalBaseDeltaFooterTailSize
	putUint16At(b.data, tailPos, uint16(lowLen))
	putUint16At(b.data, tailPos+2, uint16(highLen))
	putUint16At(b.data, tailPos+4, uint16(prefixLen))
	putUint64At(b.data, tailPos+6, baseChildID)

	writePos := footerStart
	for i := 0; i < count; i++ {
		e := &b.internalBaseEntries[i]
		suffix := e.key[prefixLen:]
		entrySize := 2 + deltaWidth + len(suffix)
		writePos -= entrySize

		putUint16At(b.data, writePos, uint16(len(suffix)))
		delta := e.child - baseChildID
		if deltaWidth == 2 {
			putUint16At(b.data, writePos+2, uint16(delta))
			copy(b.data[writePos+4:writePos+4+len(suffix)], suffix)
		} else {
			putUint32(b.data[writePos+2:writePos+6], uint32(delta))
			copy(b.data[writePos+6:writePos+6+len(suffix)], suffix)
		}
		putUint16At(b.data, NodeHeaderSize+i*2, uint16(writePos))
	}

	b.heapStart = heapStart
	b.dirEnd = dirEnd
	b.releaseInternalBaseDeltaScratch()
	return true
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

func sharedPrefixLenAcrossEntries(entries []internalBaseDeltaEntry) int {
	if len(entries) <= 1 {
		return 0
	}
	first := entries[0].key
	prefixLen := len(first)
	for i := 1; i < len(entries); i++ {
		if p := sharedPrefixLen(first, entries[i].key); p < prefixLen {
			prefixLen = p
			if prefixLen == 0 {
				return 0
			}
		}
	}
	return prefixLen
}

func putUint16At(dst []byte, pos int, v uint16) {
	_ = dst[pos+1]
	dst[pos] = byte(v)
	dst[pos+1] = byte(v >> 8)
}

func putUint32At(dst []byte, pos int, v uint32) {
	_ = dst[pos+3]
	dst[pos] = byte(v)
	dst[pos+1] = byte(v >> 8)
	dst[pos+2] = byte(v >> 16)
	dst[pos+3] = byte(v >> 24)
}

func encodePackedValuePtrAt(dst []byte, pos int, ptr page.ValuePtr) {
	_ = dst[pos+page.PackedValuePtrSize-1]
	if ptr.Offset > uint64(^uint32(0)) {
		panic("page: packed ValuePtr offset overflows u32")
	}
	putUint32At(dst, pos, uint32(ptr.Offset))
	putUint32At(dst, pos+4, ptr.Length)
	putUint32At(dst, pos+8, ptr.FileID)
}

func encodeValuePtrAt(dst []byte, pos int, ptr page.ValuePtr) {
	_ = dst[pos+page.ValuePtrSize-1]
	putUint64At(dst, pos, ptr.Offset)
	putUint32At(dst, pos+8, ptr.Length)
	putUint32At(dst, pos+12, ptr.FileID)
}

func putUint16(dst []byte, v uint16) {
	putUint16At(dst, 0, v)
}

func putUint64At(dst []byte, pos int, v uint64) {
	_ = dst[pos+7]
	dst[pos] = byte(v)
	dst[pos+1] = byte(v >> 8)
	dst[pos+2] = byte(v >> 16)
	dst[pos+3] = byte(v >> 24)
	dst[pos+4] = byte(v >> 32)
	dst[pos+5] = byte(v >> 40)
	dst[pos+6] = byte(v >> 48)
	dst[pos+7] = byte(v >> 56)
}

func putUint64(dst []byte, v uint64) {
	putUint64At(dst, 0, v)
}
