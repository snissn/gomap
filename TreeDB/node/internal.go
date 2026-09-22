package node

import (
	"bytes"
	"encoding/binary"

	"github.com/snissn/gomap/TreeDB/page"
)

type internalBaseDeltaMeta struct {
	low         []byte
	high        []byte
	prefix      []byte
	baseChildID uint64
	footerStart int
	tailStart   int
}

func (n *Node) internalBaseDeltaMeta() (internalBaseDeltaMeta, error) {
	flags := n.rawFlags()
	if n.internalMetaValid && n.internalMetaCount == n.Count() && n.internalMetaFlags == flags {
		return n.internalMeta, nil
	}

	if len(n.data) < NodeHeaderSize {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}
	tailStart := len(n.data) - internalBaseDeltaFooterTailSize
	if tailStart < NodeHeaderSize {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}

	lowLen := int(getUint16At(n.data, tailStart))
	highLen := int(getUint16At(n.data, tailStart+2))
	prefixLen := int(getUint16At(n.data, tailStart+4))
	if lowLen < 0 || highLen < 0 || prefixLen < 0 {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}

	payloadLen := lowLen + highLen + prefixLen
	if payloadLen > tailStart {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}
	footerStart := tailStart - payloadLen
	dirEnd := NodeHeaderSize + int(n.Count())*DirectoryEntrySize
	if footerStart < dirEnd {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}

	payloadPos := footerStart
	lowEnd := payloadPos + lowLen
	highEnd := lowEnd + highLen
	prefixEnd := highEnd + prefixLen
	if lowEnd > len(n.data) || highEnd > len(n.data) || prefixEnd > len(n.data) {
		return internalBaseDeltaMeta{}, ErrCorruptedNode
	}

	meta := internalBaseDeltaMeta{
		low:         n.data[payloadPos:lowEnd],
		high:        n.data[lowEnd:highEnd],
		prefix:      n.data[highEnd:prefixEnd],
		baseChildID: binary.LittleEndian.Uint64(n.data[tailStart+6 : tailStart+14]),
		footerStart: footerStart,
		tailStart:   tailStart,
	}
	n.internalMeta = meta
	n.internalMetaCount = n.Count()
	n.internalMetaFlags = flags
	n.internalMetaValid = true
	return meta, nil
}

func (n *Node) internalBaseDeltaFooter() (prefix []byte, baseChildID uint64, footerStart int, err error) {
	meta, err := n.internalBaseDeltaMeta()
	if err != nil {
		return nil, 0, 0, err
	}
	return meta.prefix, meta.baseChildID, meta.footerStart, nil
}

func comparePrefixedKey(prefix, suffix, key []byte) int {
	if len(prefix) == 0 {
		return compareInternalSuffix(suffix, key)
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
	return compareInternalSuffix(suffix, key[len(prefix):])
}

func compareInternalSuffix(suffix, keySuffix []byte) int {
	return bytes.Compare(suffix, keySuffix)
}

func compareInternalSuffixAt(data []byte, suffixStart int, suffixLen int, keySuffix []byte) int {
	if suffixLen == len(keySuffix) {
		switch suffixLen {
		case 1:
			a := data[suffixStart]
			b := keySuffix[0]
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		case 2:
			a := binary.BigEndian.Uint16(data[suffixStart : suffixStart+2])
			b := binary.BigEndian.Uint16(keySuffix)
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		case 4:
			a := binary.BigEndian.Uint32(data[suffixStart : suffixStart+4])
			b := binary.BigEndian.Uint32(keySuffix)
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		case 8:
			a := binary.BigEndian.Uint64(data[suffixStart : suffixStart+8])
			b := binary.BigEndian.Uint64(keySuffix)
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		}
	}
	return bytes.Compare(data[suffixStart:suffixStart+suffixLen], keySuffix)
}

// InternalEntry represents a parsed entry from an Internal Node.
type InternalEntry struct {
	Key         []byte
	ChildPageID uint64
	ChildRef    page.ChildRef
}

func (n *Node) internalBaseDeltaEntryWidths() (deltaWidth int, entryHeader int) {
	if n.internalBaseDeltaU16() {
		return 2, 4
	}
	return 4, 6
}

func (n *Node) internalPlainEntryHeaderSize() int {
	if n.internalLeafLogRefs() {
		return internalLeafLogRefHeaderSize
	}
	return 10
}

func (n *Node) internalBaseDeltaChildIDAtIndex(meta internalBaseDeltaMeta, idx int, deltaWidth int, entryHeader int) (uint64, error) {
	if idx < 0 || idx >= int(n.Count()) {
		return 0, ErrCorruptedNode
	}
	dirOff := NodeHeaderSize + idx*DirectoryEntrySize
	if dirOff+2 > len(n.data) {
		return 0, ErrCorruptedNode
	}
	ptr := int(getUint16At(n.data, dirOff))
	return n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
}

func (n *Node) internalBaseDeltaChildIDAtPtr(meta internalBaseDeltaMeta, ptr int, deltaWidth int, entryHeader int) (uint64, error) {
	if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
		return 0, ErrCorruptedNode
	}
	deltaStart := ptr + 2
	var delta uint64
	if deltaWidth == 2 {
		delta = uint64(getUint16At(n.data, deltaStart))
	} else {
		delta = uint64(binary.LittleEndian.Uint32(n.data[deltaStart : deltaStart+4]))
	}
	return meta.baseChildID + delta, nil
}

// GetInternalChildID returns only the child page ID at the given index.
func (n *Node) GetInternalChildID(index uint16) (uint64, error) {
	ref, err := n.GetInternalChildRef(index)
	if err != nil {
		return 0, err
	}
	if ref.Kind != page.ChildRefPage {
		return 0, ErrInvalidType
	}
	return ref.Page, nil
}

func (n *Node) GetInternalChildRef(index uint16) (page.ChildRef, error) {
	if n.internalLeafLogRefs() {
		offset, err := n.getOffset(index)
		if err != nil {
			return page.ChildRef{}, err
		}
		ptr := int(offset)
		if ptr+internalLeafLogRefHeaderSize > len(n.data) {
			return page.ChildRef{}, ErrCorruptedNode
		}
		return page.LeafLogChildRef(page.DecodeLogRecordRef(n.data[ptr+2 : ptr+2+page.LogRecordRefSize])), nil
	}
	if n.internalBaseDelta() {
		meta, err := n.internalBaseDeltaMeta()
		if err != nil {
			return page.ChildRef{}, err
		}
		offset, err := n.getOffset(index)
		if err != nil {
			return page.ChildRef{}, err
		}
		ptr := int(offset)
		deltaWidth, entryHeader := n.internalBaseDeltaEntryWidths()
		if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
			return page.ChildRef{}, ErrCorruptedNode
		}
		deltaStart := ptr + 2
		var delta uint64
		if deltaWidth == 2 {
			delta = uint64(getUint16At(n.data, deltaStart))
		} else {
			delta = uint64(binary.LittleEndian.Uint32(n.data[deltaStart : deltaStart+4]))
		}
		return page.PageChildRef(meta.baseChildID + delta), nil
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return page.ChildRef{}, err
	}

	ptr := int(offset)
	// Layout: KeyLen(2) | ChildPageID(8)
	if ptr+10 > len(n.data) {
		return page.ChildRef{}, ErrCorruptedNode
	}

	return page.PageChildRef(binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10])), nil
}

func (n *Node) WalkInternalChildren(stack *[]uint64, visit func(page.LeafLogPtr) error) error {
	if stack == nil {
		return nil
	}
	if n.Type() != page.PageTypeInternal {
		return ErrInvalidType
	}
	children := *stack
	if n.internalBaseDelta() {
		meta, err := n.internalBaseDeltaMeta()
		if err != nil {
			return err
		}
		deltaWidth, entryHeader := n.internalBaseDeltaEntryWidths()
		dirOff := NodeHeaderSize
		for i := uint16(0); i < n.count; i++ {
			if dirOff+2 > len(n.data) {
				return ErrCorruptedNode
			}
			ptr := int(getUint16At(n.data, dirOff))
			dirOff += DirectoryEntrySize
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return ErrCorruptedNode
			}
			deltaStart := ptr + 2
			var childID uint64
			if deltaWidth == 2 {
				childID = meta.baseChildID + uint64(getUint16At(n.data, deltaStart))
			} else {
				childID = meta.baseChildID + uint64(binary.LittleEndian.Uint32(n.data[deltaStart:deltaStart+4]))
			}
			children = append(children, childID)
		}
		*stack = children
		return nil
	}

	dirOff := NodeHeaderSize
	for i := uint16(0); i < n.count; i++ {
		if dirOff+2 > len(n.data) {
			return ErrCorruptedNode
		}
		ptr := int(getUint16At(n.data, dirOff))
		dirOff += DirectoryEntrySize
		if n.internalLeafLogRefs() {
			if ptr+internalLeafLogRefHeaderSize > len(n.data) {
				return ErrCorruptedNode
			}
			if visit != nil {
				if err := visit(page.DecodeLogRecordRef(n.data[ptr+2 : ptr+2+page.LogRecordRefSize])); err != nil {
					return err
				}
			}
			continue
		}
		if ptr+10 > len(n.data) {
			return ErrCorruptedNode
		}
		childID := binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10])
		children = append(children, childID)
	}
	*stack = children
	return nil
}

func (n *Node) ForEachInternalChildID(fn func(uint64) error) error {
	if fn == nil {
		return nil
	}
	if n.Type() != page.PageTypeInternal {
		return ErrInvalidType
	}
	if n.internalLeafLogRefs() {
		return ErrInvalidType
	}
	if n.internalBaseDelta() {
		meta, err := n.internalBaseDeltaMeta()
		if err != nil {
			return err
		}
		deltaWidth, entryHeader := n.internalBaseDeltaEntryWidths()
		dirOff := NodeHeaderSize
		for i := uint16(0); i < n.count; i++ {
			if dirOff+2 > len(n.data) {
				return ErrCorruptedNode
			}
			ptr := int(getUint16At(n.data, dirOff))
			dirOff += DirectoryEntrySize
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return ErrCorruptedNode
			}
			deltaStart := ptr + 2
			var childID uint64
			if deltaWidth == 2 {
				childID = meta.baseChildID + uint64(getUint16At(n.data, deltaStart))
			} else {
				childID = meta.baseChildID + uint64(binary.LittleEndian.Uint32(n.data[deltaStart:deltaStart+4]))
			}
			if err := fn(childID); err != nil {
				return err
			}
		}
		return nil
	}

	dirOff := NodeHeaderSize
	for i := uint16(0); i < n.count; i++ {
		if dirOff+2 > len(n.data) {
			return ErrCorruptedNode
		}
		ptr := int(getUint16At(n.data, dirOff))
		dirOff += DirectoryEntrySize
		if ptr+10 > len(n.data) {
			return ErrCorruptedNode
		}
		if err := fn(binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10])); err != nil {
			return err
		}
	}
	return nil
}

// GetInternalEntry reads the entry at the given index.
func (n *Node) GetInternalEntry(index uint16) (InternalEntry, error) {
	keyView, childRef, err := n.GetInternalEntryRefView(index)
	if err != nil {
		return InternalEntry{}, err
	}
	key := make([]byte, len(keyView))
	copy(key, keyView)
	return InternalEntry{Key: key, ChildPageID: childRef.Page, ChildRef: childRef}, nil
}

// GetInternalEntryView returns a view of the entry at the given index.
// For uncompressed internal pages, the returned key slice points directly into
// the node's backing page.
//
// For internal base-delta pages, the returned key slice is backed by a node
// scratch buffer and is only valid until the next internal entry decode call
// on the same node. Callers that need to retain the key must copy it.
func (n *Node) GetInternalEntryView(index uint16) (key []byte, childID uint64, err error) {
	key, childRef, err := n.GetInternalEntryRefView(index)
	if err != nil {
		return nil, 0, err
	}
	if childRef.Kind != page.ChildRefPage {
		return nil, 0, ErrInvalidType
	}
	return key, childRef.Page, nil
}

func (n *Node) GetInternalEntryRefView(index uint16) (key []byte, childRef page.ChildRef, err error) {
	if n.internalLeafLogRefs() {
		offset, err := n.getOffset(index)
		if err != nil {
			return nil, page.ChildRef{}, err
		}
		ptr := int(offset)
		if ptr+internalLeafLogRefHeaderSize > len(n.data) {
			return nil, page.ChildRef{}, ErrCorruptedNode
		}

		keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
		ref := page.DecodeLogRecordRef(n.data[ptr+2 : ptr+2+page.LogRecordRefSize])
		ptr += internalLeafLogRefHeaderSize
		if ptr+int(keyLen) > len(n.data) {
			return nil, page.ChildRef{}, ErrCorruptedNode
		}

		key = n.data[ptr : ptr+int(keyLen)]
		return key, page.LeafLogChildRef(ref), nil
	}
	if n.internalBaseDelta() {
		meta, err := n.internalBaseDeltaMeta()
		if err != nil {
			return nil, page.ChildRef{}, err
		}
		offset, err := n.getOffset(index)
		if err != nil {
			return nil, page.ChildRef{}, err
		}
		ptr := int(offset)
		deltaWidth, entryHeader := n.internalBaseDeltaEntryWidths()
		if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
			return nil, page.ChildRef{}, ErrCorruptedNode
		}

		suffixLen := int(getUint16(n.data[ptr : ptr+2]))
		deltaStart := ptr + 2
		var delta uint64
		if deltaWidth == 2 {
			delta = uint64(getUint16(n.data[deltaStart : deltaStart+2]))
		} else {
			delta = uint64(binary.LittleEndian.Uint32(n.data[deltaStart : deltaStart+4]))
		}
		suffixStart := ptr + entryHeader
		suffixEnd := suffixStart + suffixLen
		if suffixLen < 0 || suffixEnd > meta.footerStart {
			return nil, page.ChildRef{}, ErrCorruptedNode
		}
		suffix := n.data[suffixStart:suffixEnd]

		keyLen := len(meta.prefix) + suffixLen
		out := n.ensureKeyScratch(keyLen)
		copy(out, meta.prefix)
		copy(out[len(meta.prefix):], suffix)
		return out, page.PageChildRef(meta.baseChildID + delta), nil
	}

	offset, err := n.getOffset(index)
	if err != nil {
		return nil, page.ChildRef{}, err
	}

	ptr := int(offset)
	if ptr+10 > len(n.data) {
		return nil, page.ChildRef{}, ErrCorruptedNode
	}

	keyLen := binary.LittleEndian.Uint16(n.data[ptr : ptr+2])
	childID := binary.LittleEndian.Uint64(n.data[ptr+2 : ptr+10])

	ptr += 10
	if ptr+int(keyLen) > len(n.data) {
		return nil, page.ChildRef{}, ErrCorruptedNode
	}

	key = n.data[ptr : ptr+int(keyLen)]
	return key, page.PageChildRef(childID), nil
}

// InternalFenceBounds returns exact subtree bounds when persisted on this
// internal page: low inclusive and high exclusive. A nil/empty high bound means
// unbounded.
func (n *Node) InternalFenceBounds() (low, high []byte, ok bool, err error) {
	if !n.internalBaseDelta() || !n.internalFenceBounds() {
		return nil, nil, false, nil
	}
	meta, err := n.internalBaseDeltaMeta()
	if err != nil {
		return nil, nil, false, err
	}
	return meta.low, meta.high, true, nil
}

// SearchInternal performs a binary search for the given key in an Internal Node.
// Returns the index of the child that covers the range containing key.
// Logic: Find largest index i such that Entry[i].Key <= key.
// If key < Entry[0].Key, returns index 0 (Left-most child rule usually handles this).
func (n *Node) SearchInternal(key []byte) (uint16, bool) {
	if n.internalBaseDelta() {
		meta, err := n.internalBaseDeltaMeta()
		if err != nil {
			return 0, false
		}

		data := n.data
		count := n.Count()
		if count == 0 {
			return 0, false
		}
		keySuffix := key
		if len(meta.prefix) > 0 {
			prefixLen := len(meta.prefix)
			if len(key) < prefixLen {
				cmp := bytes.Compare(meta.prefix[:len(key)], key)
				if cmp != 0 {
					if cmp < 0 {
						return count - 1, true
					}
					return 0, false
				}
				// key is a strict prefix of all entry keys.
				return 0, false
			}
			cmp := bytes.Compare(meta.prefix, key[:prefixLen])
			if cmp != 0 {
				if cmp < 0 {
					return count - 1, true
				}
				return 0, false
			}
			keySuffix = key[prefixLen:]
		}

		_, entryHeader := n.internalBaseDeltaEntryWidths()
		if count <= smallSearchThreshold {
			last := -1
			for i := 0; i < int(count); i++ {
				offset := getUint16(data[NodeHeaderSize+i*2:])
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false
				}
				suffixLen := int(getUint16(data[ptr : ptr+2]))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false
				}
				cmp := compareInternalSuffixAt(data, suffixStart, suffixLen, keySuffix)
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
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false
			}
			suffixLen := int(getUint16(data[ptr : ptr+2]))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false
			}

			cmp := compareInternalSuffixAt(data, suffixStart, suffixLen, keySuffix)
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
	entryHeader := n.internalPlainEntryHeaderSize()
	if count <= smallSearchThreshold {
		last := -1
		for i := 0; i < int(count); i++ {
			offset := binary.LittleEndian.Uint16(data[NodeHeaderSize+i*2:])
			ptr := int(offset)
			keyLen := binary.LittleEndian.Uint16(data[ptr : ptr+2])
			keyPtr := ptr + entryHeader
			cmp := compareInternalSuffix(data[keyPtr:keyPtr+int(keyLen)], key)
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
		keyPtr := ptr + entryHeader

		// Compare Entry.Key vs Key
		// We want to find position where Key would be inserted
		cmp := compareInternalSuffix(data[keyPtr:keyPtr+int(keyLen)], key)

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

// SearchInternalChildID resolves the child page ID for key in a single logical
// search. The found return value matches SearchInternal: true when key is >= at
// least one separator in this page.
func (n *Node) SearchInternalChildID(key []byte) (childID uint64, found bool, err error) {
	if n.internalLeafLogRefs() {
		ref, found, err := n.SearchInternalChildRef(key)
		if err != nil {
			return 0, false, err
		}
		if ref.Kind != page.ChildRefPage {
			return 0, found, ErrInvalidType
		}
		return ref.Page, found, nil
	}
	count := n.Count()
	if count == 0 {
		return 0, false, ErrCorruptedNode
	}
	fixedBE8 := len(key) == 8
	targetBE8 := uint64(0)
	if fixedBE8 {
		targetBE8 = getUint64BEAt(key, 0)
	}

	if !n.internalBaseDelta() {
		data := n.data
		if count <= smallSearchThreshold {
			lastIdx := -1
			lastChild := uint64(0)
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+10 > len(data) {
					return 0, false, ErrCorruptedNode
				}
				keyLen := int(getUint16At(data, ptr))
				keyPtr := ptr + 10
				keyEnd := keyPtr + keyLen
				if keyEnd > len(data) {
					return 0, false, ErrCorruptedNode
				}
				cmp := 0
				if fixedBE8 && keyLen == 8 {
					entryBE8 := getUint64BEAt(data, keyPtr)
					if entryBE8 < targetBE8 {
						cmp = -1
					} else if entryBE8 > targetBE8 {
						cmp = 1
					}
				} else {
					cmp = compareInternalSuffix(data[keyPtr:keyEnd], key)
				}
				if cmp <= 0 {
					lastIdx = i
					lastChild = getUint64LEAt(data, ptr+2)
					continue
				}
				break
			}
			if lastIdx >= 0 {
				return lastChild, true, nil
			}
			ptr := int(getUint16At(data, NodeHeaderSize))
			if ptr < NodeHeaderSize || ptr+10 > len(data) {
				return 0, false, ErrCorruptedNode
			}
			return getUint64LEAt(data, ptr+2), false, nil
		}

		i, j := 0, int(count)
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+10 > len(data) {
				return 0, false, ErrCorruptedNode
			}
			keyLen := int(getUint16At(data, ptr))
			keyPtr := ptr + 10
			keyEnd := keyPtr + keyLen
			if keyEnd > len(data) {
				return 0, false, ErrCorruptedNode
			}
			cmp := 0
			if fixedBE8 && keyLen == 8 {
				entryBE8 := getUint64BEAt(data, keyPtr)
				if entryBE8 < targetBE8 {
					cmp = -1
				} else if entryBE8 > targetBE8 {
					cmp = 1
				}
			} else {
				cmp = compareInternalSuffix(data[keyPtr:keyEnd], key)
			}
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}

		chosen := 0
		if i > 0 {
			chosen = i - 1
		}
		ptr := int(getUint16At(data, NodeHeaderSize+chosen*2))
		if ptr < NodeHeaderSize || ptr+10 > len(data) {
			return 0, false, ErrCorruptedNode
		}
		return getUint64LEAt(data, ptr+2), i > 0, nil
	}

	meta, err := n.internalBaseDeltaMeta()
	if err != nil {
		return 0, false, err
	}
	data := n.data
	deltaWidth, entryHeader := n.internalBaseDeltaEntryWidths()
	keySuffix := key
	if len(meta.prefix) > 0 {
		prefixLen := len(meta.prefix)
		if len(key) < prefixLen {
			cmp := bytes.Compare(meta.prefix[:len(key)], key)
			if cmp != 0 {
				if cmp < 0 {
					last := int(count) - 1
					childID, err := n.internalBaseDeltaChildIDAtIndex(meta, last, deltaWidth, entryHeader)
					return childID, true, err
				}
				childID, err := n.internalBaseDeltaChildIDAtIndex(meta, 0, deltaWidth, entryHeader)
				return childID, false, err
			}
			// key is a strict prefix of all entry keys.
			childID, err := n.internalBaseDeltaChildIDAtIndex(meta, 0, deltaWidth, entryHeader)
			return childID, false, err
		}
		cmp := bytes.Compare(meta.prefix, key[:prefixLen])
		if cmp != 0 {
			if cmp < 0 {
				last := int(count) - 1
				childID, err := n.internalBaseDeltaChildIDAtIndex(meta, last, deltaWidth, entryHeader)
				return childID, true, err
			}
			childID, err := n.internalBaseDeltaChildIDAtIndex(meta, 0, deltaWidth, entryHeader)
			return childID, false, err
		}
		keySuffix = key[prefixLen:]
	}

	keySuffixLen := len(keySuffix)

	if count <= smallSearchThreshold {
		last := -1
		lastChildID := uint64(0)
		switch keySuffixLen {
		case 1:
			keyByte := keySuffix[0]
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}
				suffixLen := int(getUint16At(data, ptr))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}

				var cmp int
				if suffixLen == 1 {
					a := data[suffixStart]
					if a < keyByte {
						cmp = -1
					} else if a > keyByte {
						cmp = 1
					}
				} else {
					cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
				}
				if cmp <= 0 {
					last = i
					childID, err := n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
					if err != nil {
						return 0, false, err
					}
					lastChildID = childID
					continue
				}
				break
			}
		case 2:
			keyU16 := binary.BigEndian.Uint16(keySuffix)
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}
				suffixLen := int(getUint16At(data, ptr))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}

				var cmp int
				if suffixLen == 2 {
					a := binary.BigEndian.Uint16(data[suffixStart : suffixStart+2])
					if a < keyU16 {
						cmp = -1
					} else if a > keyU16 {
						cmp = 1
					}
				} else {
					cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
				}
				if cmp <= 0 {
					last = i
					childID, err := n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
					if err != nil {
						return 0, false, err
					}
					lastChildID = childID
					continue
				}
				break
			}
		case 4:
			keyU32 := binary.BigEndian.Uint32(keySuffix)
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}
				suffixLen := int(getUint16At(data, ptr))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}

				var cmp int
				if suffixLen == 4 {
					a := binary.BigEndian.Uint32(data[suffixStart : suffixStart+4])
					if a < keyU32 {
						cmp = -1
					} else if a > keyU32 {
						cmp = 1
					}
				} else {
					cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
				}
				if cmp <= 0 {
					last = i
					childID, err := n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
					if err != nil {
						return 0, false, err
					}
					lastChildID = childID
					continue
				}
				break
			}
		case 8:
			keyU64 := binary.BigEndian.Uint64(keySuffix)
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}
				suffixLen := int(getUint16At(data, ptr))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}

				var cmp int
				if suffixLen == 8 {
					a := getUint64BEAt(data, suffixStart)
					if a < keyU64 {
						cmp = -1
					} else if a > keyU64 {
						cmp = 1
					}
				} else {
					cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
				}
				if cmp <= 0 {
					last = i
					childID, err := n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
					if err != nil {
						return 0, false, err
					}
					lastChildID = childID
					continue
				}
				break
			}
		default:
			for i := 0; i < int(count); i++ {
				offset := getUint16At(data, NodeHeaderSize+i*2)
				ptr := int(offset)
				if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}
				suffixLen := int(getUint16At(data, ptr))
				suffixStart := ptr + entryHeader
				suffixEnd := suffixStart + suffixLen
				if suffixLen < 0 || suffixEnd > meta.footerStart {
					return 0, false, ErrCorruptedNode
				}

				cmp := bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
				if cmp <= 0 {
					last = i
					childID, err := n.internalBaseDeltaChildIDAtPtr(meta, ptr, deltaWidth, entryHeader)
					if err != nil {
						return 0, false, err
					}
					lastChildID = childID
					continue
				}
				break
			}
		}
		if last >= 0 {
			return lastChildID, true, nil
		}
		childID, err := n.internalBaseDeltaChildIDAtIndex(meta, 0, deltaWidth, entryHeader)
		return childID, false, err
	}

	i, j := 0, int(count)
	switch keySuffixLen {
	case 1:
		keyByte := keySuffix[0]
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}
			suffixLen := int(getUint16At(data, ptr))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}

			var cmp int
			if suffixLen == 1 {
				a := data[suffixStart]
				if a < keyByte {
					cmp = -1
				} else if a > keyByte {
					cmp = 1
				}
			} else {
				cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
			}
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	case 2:
		keyU16 := binary.BigEndian.Uint16(keySuffix)
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}
			suffixLen := int(getUint16At(data, ptr))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}

			var cmp int
			if suffixLen == 2 {
				a := binary.BigEndian.Uint16(data[suffixStart : suffixStart+2])
				if a < keyU16 {
					cmp = -1
				} else if a > keyU16 {
					cmp = 1
				}
			} else {
				cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
			}
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	case 4:
		keyU32 := binary.BigEndian.Uint32(keySuffix)
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}
			suffixLen := int(getUint16At(data, ptr))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}

			var cmp int
			if suffixLen == 4 {
				a := binary.BigEndian.Uint32(data[suffixStart : suffixStart+4])
				if a < keyU32 {
					cmp = -1
				} else if a > keyU32 {
					cmp = 1
				}
			} else {
				cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
			}
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	case 8:
		keyU64 := binary.BigEndian.Uint64(keySuffix)
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}
			suffixLen := int(getUint16At(data, ptr))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}

			var cmp int
			if suffixLen == 8 {
				a := getUint64BEAt(data, suffixStart)
				if a < keyU64 {
					cmp = -1
				} else if a > keyU64 {
					cmp = 1
				}
			} else {
				cmp = bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
			}
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	default:
		for i < j {
			h := int(uint(i+j) >> 1)
			offset := getUint16At(data, NodeHeaderSize+h*2)
			ptr := int(offset)
			if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}
			suffixLen := int(getUint16At(data, ptr))
			suffixStart := ptr + entryHeader
			suffixEnd := suffixStart + suffixLen
			if suffixLen < 0 || suffixEnd > meta.footerStart {
				return 0, false, ErrCorruptedNode
			}

			cmp := bytes.Compare(data[suffixStart:suffixEnd], keySuffix)
			if cmp <= 0 {
				i = h + 1
			} else {
				j = h
			}
		}
	}

	if i > 0 {
		ptr := int(getUint16At(data, NodeHeaderSize+(i-1)*DirectoryEntrySize))
		if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
			return 0, false, ErrCorruptedNode
		}
		deltaStart := ptr + 2
		if deltaWidth == 2 {
			return meta.baseChildID + uint64(getUint16At(data, deltaStart)), true, nil
		}
		return meta.baseChildID + uint64(binary.LittleEndian.Uint32(data[deltaStart:deltaStart+4])), true, nil
	}
	ptr := int(getUint16At(data, NodeHeaderSize))
	if ptr < NodeHeaderSize || ptr+entryHeader > meta.footerStart {
		return 0, false, ErrCorruptedNode
	}
	deltaStart := ptr + 2
	if deltaWidth == 2 {
		return meta.baseChildID + uint64(getUint16At(data, deltaStart)), false, nil
	}
	return meta.baseChildID + uint64(binary.LittleEndian.Uint32(data[deltaStart:deltaStart+4])), false, nil
}

func (n *Node) SearchInternalChildRef(key []byte) (childRef page.ChildRef, found bool, err error) {
	if !n.internalLeafLogRefs() {
		childID, found, err := n.SearchInternalChildID(key)
		if err != nil {
			return page.ChildRef{}, false, err
		}
		return page.PageChildRef(childID), found, nil
	}
	idx, found := n.SearchInternal(key)
	ref, err := n.GetInternalChildRef(idx)
	return ref, found, err
}

// SearchInternalWithCompare is like SearchInternal but uses the provided compare
// function to compare entry keys against the search key.
func (n *Node) SearchInternalWithCompare(key []byte, cmpFn func(a, b []byte) int) (uint16, bool) {
	if cmpFn == nil {
		return n.SearchInternal(key)
	}

	count := n.Count()
	if count == 0 {
		return 0, false
	}

	i, j := 0, int(count)
	for i < j {
		h := int(uint(i+j) >> 1)
		k, _, err := n.GetInternalEntryView(uint16(h))
		if err != nil {
			return 0, false
		}
		if cmpFn(k, key) <= 0 {
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

// AddInternalChild inserts or updates an internal entry while maintaining sorted
// order.
//
// It is intended for point updates and small in-memory manipulations. Write paths
// that rebuild pages in bulk should prefer Builder.
func (n *Node) AddInternalChild(key []byte, childPageID uint64) error {
	if n.Type() != page.PageTypeInternal {
		return ErrInvalidType
	}

	entrySize := 2 + 8 + len(key)

	// Lower-bound insert position (first entry >= key).
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

	needed := entrySize + DirectoryEntrySize
	if n.FreeSpace() < needed {
		if err := n.Compact(); err != nil {
			return err
		}
		if n.FreeSpace() < needed {
			return ErrNodeFull
		}
	}

	buf := make([]byte, entrySize)
	putUint16(buf[0:2], uint16(len(key)))
	binary.LittleEndian.PutUint64(buf[2:10], childPageID)
	copy(buf[10:], key)

	heapStart := int(page.PageSize)
	for k := uint16(0); k < count; k++ {
		off := getUint16(n.data[NodeHeaderSize+int(k)*2:])
		if int(off) < heapStart && off != 0 {
			heapStart = int(off)
		}
	}

	newOffset := heapStart - entrySize
	dirEnd := NodeHeaderSize + int(count)*2 + 2
	if newOffset < dirEnd {
		return ErrNodeFull
	}

	copy(n.data[newOffset:], buf)

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
