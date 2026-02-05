package node

import (
	"bytes"
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

func (n *Node) internalBaseDeltaFooter() (prefix []byte, baseChildID uint64, footerStart int, err error) {
	if len(n.data) < NodeHeaderSize {
		return nil, 0, 0, ErrCorruptedNode
	}
	baseOff := len(n.data) - 8
	prefixLenOff := baseOff - 2
	if prefixLenOff < NodeHeaderSize {
		return nil, 0, 0, ErrCorruptedNode
	}
	prefixLen := int(getUint16(n.data[prefixLenOff : prefixLenOff+2]))
	if prefixLen > prefixLenOff {
		return nil, 0, 0, ErrCorruptedNode
	}
	footerStart = prefixLenOff - prefixLen
	dirEnd := NodeHeaderSize + int(n.Count())*DirectoryEntrySize
	if footerStart < dirEnd {
		return nil, 0, 0, ErrCorruptedNode
	}
	prefix = n.data[footerStart:prefixLenOff]
	baseChildID = binary.LittleEndian.Uint64(n.data[baseOff : baseOff+8])
	return prefix, baseChildID, footerStart, nil
}

func comparePrefixedKey(prefix, suffix, key []byte) int {
	if len(prefix) == 0 {
		return bytes.Compare(suffix, key)
	}
	if len(key) < len(prefix) {
		cmp := bytes.Compare(prefix[:len(key)], key)
		if cmp != 0 {
			return cmp
		}
		// key is a strict prefix of the entry key (entry is longer).
		return 1
	}
	cmp := bytes.Compare(prefix, key[:len(prefix)])
	if cmp != 0 {
		return cmp
	}
	return bytes.Compare(suffix, key[len(prefix):])
}

func compareInternalSuffix(suffix, keySuffix []byte) int {
	return bytes.Compare(suffix, keySuffix)
}

// InternalEntry represents a parsed entry from an Internal Node.
type InternalEntry struct {
	Key         []byte
	ChildPageID uint64
}

// GetInternalChildID returns only the child page ID at the given index.
func (n *Node) GetInternalChildID(index uint16) (uint64, error) {
	if n.internalBaseDelta() {
		_, baseChildID, footerStart, err := n.internalBaseDeltaFooter()
		if err != nil {
			return 0, err
		}
		offset, err := n.getOffset(index)
		if err != nil {
			return 0, err
		}
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+6 > footerStart {
			return 0, ErrCorruptedNode
		}
		delta := binary.LittleEndian.Uint32(n.data[ptr+2 : ptr+6])
		return baseChildID + uint64(delta), nil
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return 0, err
	}

	ptr := int(offset)
	// Layout: KeyLen(2) | ChildPageID(8)
	if ptr+10 > len(n.data) {
		return 0, ErrCorruptedNode
	}

	return binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10]), nil
}

// GetInternalEntry reads the entry at the given index.
func (n *Node) GetInternalEntry(index uint16) (InternalEntry, error) {
	keyView, childID, err := n.GetInternalEntryView(index)
	if err != nil {
		return InternalEntry{}, err
	}
	key := make([]byte, len(keyView))
	copy(key, keyView)
	return InternalEntry{Key: key, ChildPageID: childID}, nil
}

// GetInternalEntryView returns a view of the entry at the given index.
// For uncompressed internal pages, the returned key slice points directly into
// the node's backing page.
//
// For internal base-delta pages, the returned key slice is backed by a node
// scratch buffer and is only valid until the next internal entry decode call
// on the same node. Callers that need to retain the key must copy it.
func (n *Node) GetInternalEntryView(index uint16) (key []byte, childID uint64, err error) {
	if n.internalBaseDelta() {
		prefix, baseChildID, footerStart, err := n.internalBaseDeltaFooter()
		if err != nil {
			return nil, 0, err
		}
		offset, err := n.getOffset(index)
		if err != nil {
			return nil, 0, err
		}
		ptr := int(offset)
		if ptr < NodeHeaderSize || ptr+6 > footerStart {
			return nil, 0, ErrCorruptedNode
		}

		suffixLen := int(getUint16(n.data[ptr : ptr+2]))
		delta := binary.LittleEndian.Uint32(n.data[ptr+2 : ptr+6])
		suffixStart := ptr + 6
		suffixEnd := suffixStart + suffixLen
		if suffixLen < 0 || suffixEnd > footerStart {
			return nil, 0, ErrCorruptedNode
		}
		suffix := n.data[suffixStart:suffixEnd]

		keyLen := len(prefix) + suffixLen
		out := n.ensureKeyScratch(keyLen)
		copy(out, prefix)
		copy(out[len(prefix):], suffix)
		return out, baseChildID + uint64(delta), nil
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return nil, 0, err
	}

	ptr := int(offset)
	if ptr+10 > len(n.data) {
		return nil, 0, ErrCorruptedNode
	}

	keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
	childID = binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10])

	ptr += 10
	if ptr+int(keyLen) > len(n.data) {
		return nil, 0, ErrCorruptedNode
	}

	key = n.data[ptr : ptr+int(keyLen)]
	return key, childID, nil
}

// SearchInternal performs a binary search for the given key in an Internal Node.
// Returns the index of the child that covers the range containing key.
// Logic: Find largest index i such that Entry[i].Key <= key.
// If key < Entry[0].Key, returns index 0 (Left-most child rule usually handles this).
func (n *Node) SearchInternal(key []byte) (uint16, bool) {
	if n.internalBaseDelta() {
		prefix, _, footerStart, err := n.internalBaseDeltaFooter()
		if err != nil {
			return 0, false
		}

		data := n.data
		count := n.Count()
		if count == 0 {
			return 0, false
		}
		keySuffix := key
		if len(prefix) > 0 {
			prefixLen := len(prefix)
			if len(key) < prefixLen {
				cmp := bytes.Compare(prefix[:len(key)], key)
				if cmp != 0 {
					if cmp < 0 {
						return count - 1, true
					}
					return 0, false
				}
				// key is a strict prefix of all entry keys.
				return 0, false
			}
			cmp := bytes.Compare(prefix, key[:prefixLen])
			if cmp != 0 {
				if cmp < 0 {
					return count - 1, true
				}
				return 0, false
			}
			keySuffix = key[prefixLen:]
		}
		if count <= smallSearchThreshold {
			last := -1
			for i := 0; i < int(count); i++ {
				offset := getUint16(data[NodeHeaderSize+i*2:])
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+6 > footerStart {
					return 0, false
				}
				suffixLen := int(getUint16(data[ptr : ptr+2]))
				suffixStart := ptr + 6
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > footerStart {
					return 0, false
				}
				cmp := compareInternalSuffix(data[suffixStart:suffixEnd], keySuffix)
				if cmp <= 0 {
					last = i
					continue
				}
				break
			}
			if last >= 0 {
				return uint16(last), true
			}
			return 0, false
		}

		i, j := 0, int(count)
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16(data[NodeHeaderSize+h*2:])
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+6 > footerStart {
				return 0, false
			}
			suffixLen := int(getUint16(data[ptr : ptr+2]))
			suffixStart := ptr + 6
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > footerStart {
				return 0, false
			}

			cmp := compareInternalSuffix(data[suffixStart:suffixEnd], keySuffix)
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}

		if i > 0 {
			return uint16(i - 1), true
		}
		return 0, false
	}

	data := n.data
	count := n.Count()
	if count == 0 {
		return 0, false
	}
	if count <= smallSearchThreshold {
		last := -1
		for i := 0; i < int(count); i++ {
			offset := binary.LittleEndian.Uint16(data[NodeHeaderSize+i*2:])
			ptr := int(offset)
			keyLen := binary.LittleEndian.Uint16(data[ptr : ptr+2])
			keyPtr := ptr + 10 // Skip KeyLen(2) + ChildID(8)
			cmp := bytes.Compare(data[keyPtr:keyPtr+int(keyLen)], key)
			if cmp <= 0 {
				last = i
				continue
			}
			break
		}
		if last >= 0 {
			return uint16(last), true
		}
		return 0, false
	}

	i, j := 0, int(count)

	// Find first element > key, then subtract 1
	// Upper Bound
	for i < j {
		h := int(uint(i+j) >> 1)

		offset := binary.LittleEndian.Uint16(data[NodeHeaderSize+h*2:])
		ptr := int(offset)
		keyLen := binary.LittleEndian.Uint16(data[ptr : ptr+2])
		keyPtr := ptr + 10 // Skip KeyLen(2) + ChildID(8)

		// Compare Entry.Key vs Key
		// We want to find position where Key would be inserted
		cmp := bytes.Compare(data[keyPtr:keyPtr+int(keyLen)], key)

		if cmp <= 0 { // Entry.Key <= Key
			i = h + 1
		} else {
			j = h
		}
	}

	// i is the first index where Entry.Key > Key.
	// So i-1 is the largest index where Entry.Key <= Key.
	if i > 0 {
		return uint16(i - 1), true
	}

	// If i == 0, it means Key < Entry[0].Key.
	// Return 0 (left-most child).
	return 0, false // found=false implies exact match? No.
	// Search usually returns the index to follow.
	// Let's rely on index.
}

// AddInternalChild adds a child pointer to the internal node.
func (n *Node) AddInternalChild(key []byte, childPageID uint64) error {
	// Calculate size
	entrySize := 2 + 8 + len(key)

	// Search to find insert position
	// For Internal Nodes, we want to keep them sorted by Key.
	// SearchInternal gives us the child to follow, but for insertion, we want exact position.
	// We can reuse SearchLeaf logic (First entry >= Key) to find insert slot.

	// Re-implement LowerBound search here for insertion index
	count := n.Count()
	i, j := 0, int(count)
	for i < j {
		h := int(uint(i+j) >> 1)
		offset := binary.LittleEndian.Uint16(n.data[NodeHeaderSize+h*2:])
		ptr := int(offset)
		keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
		keyPtr := ptr + 10
		cmp := bytes.Compare(n.data[keyPtr:keyPtr+int(keyLen)], key)
		if cmp < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	idx := uint16(i)
	// found := (idx < count) && keys match...

	// Check space
	needed := entrySize + DirectoryEntrySize // Assuming new entry
	if n.FreeSpace() < needed {
		if err := n.Compact(); err != nil {
			return err
		}
		if n.FreeSpace() < needed {
			return ErrNodeFull
		}
	}

	// Allocate
	heapStart := int(page.PageSize)
	for k := uint16(0); k < count; k++ {
		off := binary.LittleEndian.Uint16(n.data[NodeHeaderSize+int(k)*2:])
		if int(off) < heapStart && int(off) != 0 {
			heapStart = int(off)
		}
	}

	newOffset := heapStart - entrySize
	dirEnd := NodeHeaderSize + int(count)*2 + 2 // +2 for new slot

	if newOffset < dirEnd {
		return ErrNodeFull
	}

	// Write
	binary.LittleEndian.PutUint16(n.data[newOffset:newOffset+2], uint16(len(key)))
	binary.LittleEndian.PutUint64(n.data[newOffset+2:newOffset+10], childPageID)
	copy(n.data[newOffset+10:], key)

	// Shift Directory
	srcPos := NodeHeaderSize + int(idx)*2
	destPos := srcPos + 2
	moveLen := int(count-idx) * 2

	if moveLen > 0 {
		copy(n.data[destPos:destPos+moveLen], n.data[srcPos:srcPos+moveLen])
	}

	n.setOffset(idx, uint16(newOffset))
	n.SetCount(count + 1)
	n.UpdateChecksum()

	return nil
}
