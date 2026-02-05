package node

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

// Common errors
var (
	ErrKeyTooLarge   = errors.New("key too large")
	ErrValueTooLarge = errors.New("value too large")
	ErrNodeFull      = errors.New("node is full")
	ErrInvalidType   = errors.New("invalid node type")
	ErrKeyNotFound   = errors.New("key not found")
	ErrCorruptedNode = errors.New("corrupted node: invalid offsets")
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
	internalBaseDelta16Flag  uint16 = 0x0400
	leafNodeFlagMask                = leafPrefixCompressedFlag | leafColumnarFlag | leafPrefixV2Flag | leafPackedValuePtrFlag
	internalNodeFlagMask            = internalBaseDeltaFlag | internalBaseDelta16Flag
	nodeFlagMask                    = leafNodeFlagMask | internalNodeFlagMask
	pageTypeMask                    = ^nodeFlagMask

	leafPrefixRestartInterval = 16
)

func getUint16(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
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
	leafValid  bool

	internalPrefix       []byte
	internalBaseChildID  uint64
	internalFooterStart  int
	internalFooterCached bool
}

// NewNode creates a Node wrapper around the given page data.
func NewNode(data []byte) *Node {
	n := &Node{data: data}
	if len(data) >= NodeHeaderSize {
		flags := getUint16(data[12:14])
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16(data[14:16])
	}
	return n
}

// NewNodeView wraps the given page data without allocating a *Node.
// It is useful in hot paths that store Node values directly (e.g. iterators).
func NewNodeView(data []byte) Node {
	n := Node{data: data}
	if len(data) >= NodeHeaderSize {
		flags := getUint16(data[12:14])
		n.ptype = page.PageType(flags & pageTypeMask)
		n.count = getUint16(data[14:16])
	}
	return n
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
	flags := getUint16(n.data[12:14])
	flags = (flags & nodeFlagMask) | uint16(t)
	binary.LittleEndian.PutUint16(n.data[12:14], flags)
	n.internalFooterCached = false
}

func (n *Node) rawFlags() uint16 {
	return getUint16(n.data[12:14])
}

func (n *Node) setRawFlags(flags uint16) {
	binary.LittleEndian.PutUint16(n.data[12:14], flags)
	n.ptype = page.PageType(flags & pageTypeMask)
	n.internalFooterCached = false
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

func (n *Node) leafPackedValuePtr() bool {
	if n.ptype != page.PageTypeLeaf {
		return false
	}
	return n.rawFlags()&leafPackedValuePtrFlag != 0
}

func (n *Node) internalBaseDelta() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	return n.rawFlags()&internalBaseDeltaFlag != 0
}

func (n *Node) internalBaseDelta16() bool {
	if n.ptype != page.PageTypeInternal {
		return false
	}
	flags := n.rawFlags()
	return flags&internalBaseDeltaFlag != 0 && flags&internalBaseDelta16Flag != 0
}

func (n *Node) setInternalBaseDelta(enabled bool) {
	flags := n.rawFlags()
	if enabled {
		flags |= internalBaseDeltaFlag
	} else {
		flags &^= internalBaseDeltaFlag
		flags &^= internalBaseDelta16Flag
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

// Count returns the number of items in the node.
func (n *Node) Count() uint16 {
	return n.count
}

// SetCount sets the number of items in the node.
func (n *Node) SetCount(c uint16) {
	n.count = c
	binary.LittleEndian.PutUint16(n.data[14:16], c)
}

// Checksum returns the checksum from the header.
func (n *Node) Checksum() uint32 {
	return binary.LittleEndian.Uint32(n.data[8:12])
}

// UpdateChecksum calculates and updates the checksum in the header.
func (n *Node) UpdateChecksum() {
	sum := page.CalculateChecksum(n.data)
	binary.LittleEndian.PutUint32(n.data[8:12], sum)
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
	// Directory starts at NodeHeaderSize
	// Offset is at NodeHeaderSize + index*2
	dirOffset := NodeHeaderSize + int(index)*DirectoryEntrySize
	return getUint16(n.data[dirOffset : dirOffset+2]), nil
}

// setOffset sets the offset for the item at the given index.
func (n *Node) setOffset(index uint16, offset uint16) {
	dirOffset := NodeHeaderSize + int(index)*DirectoryEntrySize
	binary.LittleEndian.PutUint16(n.data[dirOffset:dirOffset+2], offset)
}

// FreeSpace returns the number of bytes available for new items.
// Free space is the gap between the end of the Directory and the start of the Heap.
func (n *Node) FreeSpace() int {
	// Directory End = Header + Count * 2
	dirEnd := NodeHeaderSize + int(n.Count())*DirectoryEntrySize

	// Heap Start = Minimum offset of all items, or PageSize if empty.
	// To find Heap Start efficiently without scanning all offsets:
	// 1. Maintain a "HeapStart" in the header? No, standard header doesn't have it.
	// 2. Scan all offsets? Slow O(N).
	// 3. Assume entries are added sequentially?
	//    If we implement "Append only" or "Sorted Insert", we can track it.
	//    But standard slotted pages usually compact or track the free pointer.
	//    Wait, checking `specs/spec.md`:
	//    "Directory (Top): Array of Offsets (uint16) growing downward."
	//    "Heap (Bottom): Data growing upward."
	//    It DOES NOT mention a "FreePtr".
	//    However, usually in Slotted Pages, we just check the offsets.
	//    Optimization: If we keep the items sorted by offset in the heap?
	//    No, usually items are sorted by Key in the Directory (logical order),
	//    but physical order in Heap can be anything (usually append-only).
	//    So to find the "Heap Top" (lowest used address), we usually need to know it.
	//    OR we scan.
	//    Since `Count` is small (max ~200-300 items per 4KB page), scanning is fast enough?
	//    Actually, we can just find the min offset.

	heapStart := int(page.PageSize)
	count := n.Count()
	if count > 0 {
		// Scan offsets to find the lowest one.
		// This is O(N). Is there a better way?
		// Most implementations store "FreePtr" or "HeapOffset" in the header or special slot.
		// The provided header `PageHeader` has `Flags` and `Count`, but no `HeapOffset`.
		// Unless `Flags` is used for something else?
		// Spec says `Flags` is Node Type.
		// So we must compute it or store it elsewhere.
		// Wait, if we compact on every write, or keep heap contiguous?
		// Let's assume we scan for now.
		for i := uint16(0); i < count; i++ {
			off := getUint16(n.data[NodeHeaderSize+int(i)*2:])
			if int(off) < heapStart && off != 0 { // 0 checks for safety
				heapStart = int(off)
			}
		}
	}

	return heapStart - dirEnd
}
