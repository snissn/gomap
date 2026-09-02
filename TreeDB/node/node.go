package node

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

// Common errors
var (
	ErrKeyTooLarge                 = errors.New("key too large")
	ErrValueTooLarge               = errors.New("value too large")
	ErrNodeFull                    = errors.New("node is full")
	ErrInvalidType                 = errors.New("invalid node type")
	ErrKeyNotFound                 = errors.New("key not found")
	ErrCorruptedNode               = errors.New("corrupted node: invalid offsets")
	ErrInternalBaseDeltaOutOfRange = errors.New("internal base-delta child range exceeds uint32")
)

const (
	// NodeHeaderSize is the size of the page header (16 bytes).
	NodeHeaderSize = page.PageHeaderSize

	// DirectoryEntrySize is the size of an offset (uint16).
	DirectoryEntrySize = 2

	smallSearchThreshold = 16

	leafPrefixCompressedFlag uint16 = 0x8000
	leafColumnarFlag         uint16 = 0x4000
	leafPrefixV2Flag         uint16 = 0x2000
	leafPackedValuePtrFlag   uint16 = 0x1000
	internalBaseDeltaFlag    uint16 = 0x0800
	leafColumnarV2Flag       uint16 = 0x0400
	internalBaseDeltaU16Flag uint16 = 0x0200
	internalFenceBoundsFlag  uint16 = 0x0100
	internalLeafLogRefsFlag  uint16 = 0x0080
	leafEntryRevisionFlag    uint16 = 0x0040

	// NOTE: TreeDB is currently pre-alpha; on-disk formats are not yet stable and
	// backward compatibility is not guaranteed. Leaf/internal flags may change.
	leafNodeFlagMask     = leafPrefixCompressedFlag | leafColumnarFlag | leafPrefixV2Flag | leafPackedValuePtrFlag | leafColumnarV2Flag | leafEntryRevisionFlag
	internalNodeFlagMask = internalBaseDeltaFlag | internalBaseDeltaU16Flag | internalFenceBoundsFlag | internalLeafLogRefsFlag
	nodeFlagMask         = leafNodeFlagMask | internalNodeFlagMask
	pageTypeMask         = ^nodeFlagMask

	leafPrefixRestartInterval = 16
)

func getUint16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func getUint16At(b []byte, off int) uint16 {
	return uint16(b[off]) | uint16(b[off+1])<<8
}

func getUint64LEAt(b []byte, off int) uint64 {
	return uint64(b[off]) |
		uint64(b[off+1])<<8 |
		uint64(b[off+2])<<16 |
		uint64(b[off+3])<<24 |
		uint64(b[off+4])<<32 |
		uint64(b[off+5])<<40 |
		uint64(b[off+6])<<48 |
		uint64(b[off+7])<<56
}

func getUint64BEAt(b []byte, off int) uint64 {
	return uint64(b[off])<<56 |
		uint64(b[off+1])<<48 |
		uint64(b[off+2])<<40 |
		uint64(b[off+3])<<32 |
		uint64(b[off+4])<<24 |
		uint64(b[off+5])<<16 |
		uint64(b[off+6])<<8 |
		uint64(b[off+7])
}

func putUint32(dst []byte, v uint32) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
}

// Node is a wrapper around a raw page byte slice.
// It implements the Slotted Page layout.
type Node struct {
	data       []byte        // The raw page data (4096 bytes)
	count      uint16        // Cached count
	ptype      page.PageType // Cached type
	keyScratch []byte
	leafKey    []byte
	leafLayout leafEntryLayout
	leafEntry  int
	leafIndex  uint16
	leafFlags  byte
	leafValid  bool

	leafColPrefixMetaValid     bool
	leafColPrefixMetaCount     uint16
	leafColPrefixValDirStart   int
	leafColPrefixFlagsStart    int
	leafColPrefixPrefixStart   int
	leafColPrefixRevisionStart int
	leafColPrefixHeaderEnd     int
	leafColPrefixKeysBlobBase  int

	internalMetaValid bool
	internalMetaCount uint16
	internalMetaFlags uint16
	internalMeta      internalBaseDeltaMeta
}

// NewNode creates a Node wrapper around the given page data.
func NewNode(data []byte) *Node {
	n := &Node{data: data}
	if len(data) >= NodeHeaderSize {
		flags := getUint16At(data, 12)
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16At(data, 14)
	}
	return n
}

// NewNodeView wraps the given page data without allocating a *Node.
// It is useful in hot paths that store Node values directly (e.g. iterators).
func NewNodeView(data []byte) Node {
	n := Node{data: data}
	if len(data) >= NodeHeaderSize {
		flags := getUint16At(data, 12)
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16At(data, 14)
	}
	return n
}

// InitNodeView resets n to wrap the given page data without allocating.
func InitNodeView(n *Node, data []byte) {
	if n == nil {
		return
	}
	n.data = data
	n.count = 0
	n.ptype = 0
	n.leafKey = nil
	n.leafEntry = 0
	n.leafIndex = 0
	n.leafFlags = 0
	n.leafValid = false
	n.leafColPrefixMetaValid = false
	n.internalMetaValid = false
	if len(data) >= NodeHeaderSize {
		flags := getUint16At(data, 12)
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16At(data, 14)
	}
}

// InitFreshNodeView wraps data in a fresh zero-value Node without clearing
// reusable decode caches. It must only be used when n has no live cached state.
func InitFreshNodeView(n *Node, data []byte) {
	if n == nil {
		return
	}
	n.data = data
	if len(data) >= NodeHeaderSize {
		flags := getUint16At(data, 12)
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16At(data, 14)
	}
}

// SetKeyScratch installs caller-owned key scratch for prefix key reconstruction.
// The slice must not be mutated concurrently while the node is in use.
func (n *Node) SetKeyScratch(buf []byte) {
	if n == nil {
		return
	}
	if cap(buf) == 0 {
		n.keyScratch = nil
		return
	}
	n.keyScratch = buf[:0]
}

// TakeKeyScratch detaches and returns the node key scratch buffer, if any.
func (n *Node) TakeKeyScratch() []byte {
	if n == nil || cap(n.keyScratch) == 0 {
		return nil
	}
	out := n.keyScratch[:0]
	n.keyScratch = nil
	return out
}

// Data returns the underlying byte slice.
func (n *Node) Data() []byte {
	return n.data
}

// PageID returns the page ID from the header.
func (n *Node) PageID() uint64 {
	return binary.LittleEndian.Uint64(n.data[0:8])
}

// SetPageID sets the page ID in the header.
func (n *Node) SetPageID(id uint64) {
	binary.LittleEndian.PutUint64(n.data[0:8], id)
}

// Type returns the page type from the header.
func (n *Node) Type() page.PageType {
	return n.ptype
}

// SetType sets the page type in the header.
func (n *Node) SetType(t page.PageType) {
	n.ptype = t
	flags := getUint16At(n.data, 12)
	flags = (flags & nodeFlagMask) | uint16(t)
	binary.LittleEndian.PutUint16(n.data[12:14], flags)
}

func (n *Node) rawFlags() uint16 {
	return getUint16At(n.data, 12)
}

func (n *Node) setRawFlags(flags uint16) {
	binary.LittleEndian.PutUint16(n.data[12:14], flags)
	n.ptype = page.PageType(flags & pageTypeMask)
	n.leafColPrefixMetaValid = false
	n.internalMetaValid = false
}

func (n *Node) leafPrefixCompressed() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafPrefixCompressedFlag != 0
}

func (n *Node) leafPrefixV2() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafPrefixV2Flag != 0
}

func (n *Node) leafColumnar() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafColumnarFlag != 0
}

func (n *Node) leafColumnarV2() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafColumnarV2Flag != 0
}

func (n *Node) leafPackedValuePtr() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafPackedValuePtrFlag != 0
}

func (n *Node) leafEntryRevisions() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafEntryRevisionFlag != 0
}

// LeafEntryRevisionsEnabled reports whether this leaf stores native per-entry
// revision metadata.
func (n *Node) LeafEntryRevisionsEnabled() bool {
	return n.leafEntryRevisions()
}

func (n *Node) internalBaseDelta() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	return n.rawFlags()&internalBaseDeltaFlag != 0
}

func (n *Node) internalLeafLogRefs() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	return n.rawFlags()&internalLeafLogRefsFlag != 0
}

func (n *Node) InternalLeafLogRefsEnabled() bool {
	return n.internalLeafLogRefs()
}

// InternalBaseDeltaEnabled reports whether this internal node uses base-delta
// key encoding. When true, GetInternalEntryView keys are scratch-backed and
// callers that retain keys must copy them.
func (n *Node) InternalBaseDeltaEnabled() bool {
	return n.internalBaseDelta()
}

func (n *Node) internalBaseDeltaU16() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	return n.rawFlags()&internalBaseDeltaU16Flag != 0
}

func (n *Node) internalFenceBounds() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	return n.rawFlags()&internalFenceBoundsFlag != 0
}

func (n *Node) setInternalBaseDelta(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= internalBaseDeltaFlag
	} else {
		flags &^= internalBaseDeltaFlag
	}
	n.setRawFlags(flags)
}

func (n *Node) setInternalLeafLogRefs(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= internalLeafLogRefsFlag
		flags &^= internalBaseDeltaFlag | internalBaseDeltaU16Flag
	} else {
		flags &^= internalLeafLogRefsFlag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafPrefixCompressed(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafPrefixCompressedFlag
	} else {
		flags &^= leafPrefixCompressedFlag
		flags &^= leafPrefixV2Flag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafPrefixV2(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafPrefixV2Flag
	} else {
		flags &^= leafPrefixV2Flag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafColumnar(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafColumnarFlag
	} else {
		flags &^= leafColumnarFlag
		// Columnar v2 encoding is only valid for columnar leaves.
		flags &^= leafColumnarV2Flag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafColumnarV2(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafColumnarV2Flag
	} else {
		flags &^= leafColumnarV2Flag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafPackedValuePtr(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafPackedValuePtrFlag
	} else {
		flags &^= leafPackedValuePtrFlag
	}
	n.setRawFlags(flags)
}

func (n *Node) setLeafEntryRevisions(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= leafEntryRevisionFlag
	} else {
		flags &^= leafEntryRevisionFlag
	}
	n.setRawFlags(flags)
}

// Count returns the number of items in the node.
func (n *Node) Count() uint16 {
	return n.count
}

// SetCount sets the number of items in the node.
func (n *Node) SetCount(c uint16) {
	n.count = c
	binary.LittleEndian.PutUint16(n.data[14:16], c)
	n.leafColPrefixMetaValid = false
	n.internalMetaValid = false
}

// Checksum returns the checksum from the header.
func (n *Node) Checksum() uint32 {
	return binary.LittleEndian.Uint32(n.data[8:12])
}

// UpdateChecksum calculates and updates the checksum in the header.
func (n *Node) UpdateChecksum() {
	page.UpdateChecksum(n.data)
}

// VerifyChecksum validates the node's checksum.
func (n *Node) VerifyChecksum() bool {
	return page.VerifyChecksumNonMutating(n.data)
}

// getOffset returns the offset for the item at the given index.
func (n *Node) getOffset(index uint16) (uint16, error) {
	if index >= n.count {
		return 0, errors.New("index out of bounds")
	}
	// For columnar v2 and combined columnar+prefix v2 leaves this directory stores
	// key offsets (not heap entry starts). For all other page layouts it stores
	// entry starts.
	// Directory starts at NodeHeaderSize
	// Offset is at NodeHeaderSize + index*2
	dirOffset := NodeHeaderSize + int(index)*DirectoryEntrySize
	return getUint16(n.data[dirOffset : dirOffset+2]), nil
}

// setOffset sets the offset for the item at the given index.
func (n *Node) setOffset(index uint16, offset uint16) {
	dirOffset := NodeHeaderSize + int(index)*DirectoryEntrySize
	binary.LittleEndian.PutUint16(n.data[dirOffset:dirOffset+2], offset)
	n.leafColPrefixMetaValid = false
	n.internalMetaValid = false
}

// FreeSpace returns the number of bytes available for new items.
// Free space is the gap between the end of the Directory and the start of the Heap.
func (n *Node) FreeSpace() int {
	dirEnd, heapStart, ok := n.liveByteBounds()
	if !ok {
		return 0
	}
	return heapStart - dirEnd
}

func (n *Node) liveByteBounds() (dirEnd, heapStart int, ok bool) {
	count := n.Count()
	dirEnd = NodeHeaderSize + int(count)*DirectoryEntrySize
	if dirEnd < NodeHeaderSize || dirEnd > len(n.data) {
		return 0, 0, false
	}
	if n.Type() == page.PageTypeLeaf && n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		// Combined columnar+prefix leaves use top-level metadata columns:
		// KeyOff[u16], ValOff[u16], Flags[u8], PrefixLen[u16], and
		// optionally Revision[u64].
		dirEnd += int(count) * DirectoryEntrySize
		dirEnd += int(count)
		dirEnd += int(count) * DirectoryEntrySize
		if n.leafEntryRevisions() {
			dirEnd += int(count) * page.EntryRevisionSize
		}
		if dirEnd < NodeHeaderSize || dirEnd > len(n.data) {
			return 0, 0, false
		}
	}
	if n.Type() == page.PageTypeLeaf && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		// Columnar v2 leaves store additional per-entry metadata immediately after
		// the key directory: ValOff (u16) + Flags (u8), and optionally
		// Revision[u64].
		dirEnd += int(count) * DirectoryEntrySize
		dirEnd += int(count)
		if n.leafEntryRevisions() {
			dirEnd += int(count) * page.EntryRevisionSize
		}
		if dirEnd < NodeHeaderSize || dirEnd > len(n.data) {
			return 0, 0, false
		}
	}
	heapStart = int(page.PageSize)
	if count == 0 {
		return dirEnd, heapStart, true
	}
	if n.Type() == page.PageTypeLeaf && n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		valDirStart := NodeHeaderSize + int(count)*DirectoryEntrySize
		if valDirStart+2 > len(n.data) {
			return 0, 0, false
		}
		heapStart = int(getUint16(n.data[valDirStart : valDirStart+2]))
		if heapStart < dirEnd || heapStart > len(n.data) {
			return 0, 0, false
		}
		return dirEnd, heapStart, true
	}
	if n.Type() == page.PageTypeLeaf && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		valDirStart := NodeHeaderSize + int(count)*DirectoryEntrySize
		if valDirStart+2 > len(n.data) {
			return 0, 0, false
		}
		heapStart = int(getUint16(n.data[valDirStart : valDirStart+2]))
		if heapStart < dirEnd || heapStart > len(n.data) {
			return 0, 0, false
		}
		return dirEnd, heapStart, true
	}
	for i := uint16(0); i < count; i++ {
		dirOff := NodeHeaderSize + int(i)*DirectoryEntrySize
		if dirOff+DirectoryEntrySize > len(n.data) {
			return 0, 0, false
		}
		off := getUint16(n.data[dirOff : dirOff+DirectoryEntrySize])
		if off == 0 {
			continue
		}
		if int(off) < dirEnd || int(off) > len(n.data) {
			return 0, 0, false
		}
		if int(off) < heapStart {
			heapStart = int(off)
		}
	}
	return dirEnd, heapStart, true
}

// LeafPageLiveBounds reports the live prefix and suffix lengths for a leaf page.
// Bytes between the prefix and suffix are free gap bytes that can be elided when
// persisting a canonical external representation.
func LeafPageLiveBounds(data []byte) (prefixLen, suffixLen int, err error) {
	if len(data) != page.PageSize {
		return 0, 0, ErrCorruptedNode
	}
	n := NewNodeView(data)
	if n.Type() != page.PageTypeLeaf {
		return 0, 0, ErrInvalidType
	}
	dirEnd, heapStart, ok := n.liveByteBounds()
	if !ok || dirEnd < NodeHeaderSize || heapStart < dirEnd || heapStart > len(data) {
		return 0, 0, ErrCorruptedNode
	}
	return dirEnd, len(data) - heapStart, nil
}
