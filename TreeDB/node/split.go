package node

import (
	"encoding/binary"
	"errors"

	"github.com/snissn/gomap/TreeDB/page"
)

// Split moves the upper half of the items from the receiver node (n)
// to the given new node (newNode).
// It returns the "pivot" key (the first key of the new node), which
// is used to insert into the parent.
func (n *Node) Split(newNode *Node) ([]byte, error) {
	if n.Count() < 2 {
		return nil, errors.New("cannot split node with fewer than 2 items")
	}

	// Columnar v2 leaves have count-dependent metadata regions (ValOff[]/Flags[])
	// immediately after the key directory. Truncating Count in-place without
	// rebuilding makes header-derived offsets inconsistent, so rebuild both sides.
	if n.Type() == page.PageTypeLeaf && n.leafColumnarV2() && !n.leafPrefixCompressed() {
		return n.splitLeafColumnarV2Rebuild(newNode)
	}
	if n.Type() == page.PageTypeLeaf && n.leafColumnar() && n.leafPrefixCompressed() && n.leafPrefixV2() {
		return n.splitLeafColumnarPrefixV2Rebuild(newNode)
	}
	if n.Type() == page.PageTypeInternal && n.internalBaseDelta() {
		return n.splitInternalBaseDeltaRebuild(newNode)
	}

	// We split at Count / 2
	splitIndex := n.Count() / 2
	count := n.Count()
	moveCount := count - splitIndex

	// 1. Setup newNode type
	newNode.SetType(n.Type())
	if n.Type() == page.PageTypeLeaf {
		newNode.setLeafPrefixCompressed(n.leafPrefixCompressed())
		newNode.setLeafColumnar(n.leafColumnar())
		newNode.setLeafColumnarV2(n.leafColumnarV2())
		newNode.setLeafPackedValuePtr(n.leafPackedValuePtr())
	}

	// 2. Iterate from splitIndex to count-1 and move items
	var pivotKey []byte
	var leafBuilder *Builder
	var internalBuilder *Builder

	for i := uint16(0); i < moveCount; i++ {
		srcIdx := splitIndex + i

		// Get Offset/Data from n
		offset, err := n.getOffset(srcIdx)
		if err != nil {
			return nil, err
		}

		// Read raw entry data to copy it exactly
		// We need to know the size of the entry to copy it.
		// For Leaf: KeyLen(2) | ValLen(4) | Flags(1) | Key | Val/Ptr
		// For Internal: KeyLen(2) | ChildID(8) | Key

		ptr := int(offset)
		if ptr >= len(n.data) {
			return nil, ErrCorruptedNode
		}

		if n.Type() == page.PageTypeLeaf {
			if leafBuilder == nil {
				opts := BuilderOptions{
					LeafPrefixCompression: n.leafPrefixCompressed(),
					LeafColumnar:          n.leafColumnar(),
					PackedValuePtr:        n.leafPackedValuePtr(),
					EntryRevisions:        n.leafEntryRevisions(),
				}
				leafBuilder = NewBuilderWithOptions(newNode.data, page.PageTypeLeaf, opts)
				leafBuilder.SetPageID(newNode.PageID())
			}
		} else if internalBuilder == nil {
			internalBuilder = NewBuilderWithOptions(newNode.data, page.PageTypeInternal, BuilderOptions{
				InternalBaseDelta: n.internalBaseDelta(),
			})
			internalBuilder.SetPageID(newNode.PageID())
		}

		// Ensure space in newNode
		// We can't use AddLeafEntry because we want exact raw copy (preserving flags, etc)
		// and we are doing bulk move.
		// But AddLeafEntry is safer?
		// AddLeafEntry re-encodes. If we just want to move raw bytes:

		// 1. Allocate space in newNode heap
		// Using high-level API `AddLeafEntry` might be slower but safer/easier?
		// BUT `AddLeafEntry` expects parsed args.
		// Let's implement a low-level "RawAdd" or just use high-level if possible.
		// High-level requires decoding.

		if n.Type() == page.PageTypeLeaf {
			key, val, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(srcIdx)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				pivotKey = append([]byte(nil), key...)
			}
			// AddLeafEntry copies data, so Views are safe here.
			err = leafBuilder.AddLeafEntryWithRevision(key, val, flags, ptr, revision)
			if err != nil {
				return nil, err
			}
		} else {
			key, childID, err := n.GetInternalEntryView(srcIdx)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				pivotKey = append([]byte(nil), key...)
			}
			err = internalBuilder.AddInternalChild(key, childID)
			if err != nil {
				return nil, err
			}
		}
	}

	// 3. Truncate n (the original node)
	// We just reduce the count.
	// The heap space remains "used" until we compact/defrag.
	// But `FreeSpace` calculation relies on scanning offsets.
	// Since we removed the offsets from the directory (by reducing count),
	// the scanner in `FreeSpace` won't see them!
	// Wait, `FreeSpace` scans `0..Count-1`.
	// So if we reduce Count, `FreeSpace` will verify fewer items.
	// The "dead" items in the heap are now effectively free space?
	// `FreeSpace` calculates: `minOffset - dirEnd`.
	// If the "dead" items are at lower offsets than the "live" items, `minOffset` will increase (good).
	// If the "dead" items are mixed...
	// In our Add implementation, we append to bottom (lower offsets).
	// So the NEWEST items are at LOWER offsets.
	// We are moving the UPPER HALF of the directory (highest indices).
	// Are these the newest items?
	// If we inserted in sorted order, yes.
	// If we inserted random, maybe.
	// But `Sort` happens in Directory.
	// So `splitIndex..count` are logically the largest keys.
	// Their physical location could be anywhere.
	// `FreeSpace` scans ALL active offsets.
	// Since we reduce Count, we stop scanning the moved items.
	// So `minOffset` will be determined ONLY by the remaining items.
	// So yes, the space is reclaimed!

	n.SetCount(splitIndex)
	n.UpdateChecksum()
	if leafBuilder != nil {
		leafBuilder.Finish()
		newNode.setRawFlags(binary.LittleEndian.Uint16(newNode.data[12:14]))
		newNode.count = binary.LittleEndian.Uint16(newNode.data[14:16])
	} else if internalBuilder != nil {
		internalBuilder.Finish()
		newNode.setRawFlags(binary.LittleEndian.Uint16(newNode.data[12:14]))
		newNode.count = binary.LittleEndian.Uint16(newNode.data[14:16])
	} else {
		newNode.UpdateChecksum() // Add* already updates, but safe to do.
	}

	return pivotKey, nil
}

func (n *Node) splitLeafColumnarV2Rebuild(newNode *Node) ([]byte, error) {
	splitIndex := n.Count() / 2
	count := n.Count()

	newNode.SetType(page.PageTypeLeaf)
	newNode.setLeafPrefixCompressed(n.leafPrefixCompressed())
	newNode.setLeafColumnar(n.leafColumnar())
	newNode.setLeafColumnarV2(n.leafColumnarV2())
	newNode.setLeafPackedValuePtr(n.leafPackedValuePtr())

	srcData := make([]byte, len(n.data))
	copy(srcData, n.data)
	src := NewNode(srcData)

	opts := BuilderOptions{
		LeafPrefixCompression: n.leafPrefixCompressed(),
		LeafColumnar:          n.leafColumnar(),
		PackedValuePtr:        n.leafPackedValuePtr(),
		EntryRevisions:        n.leafEntryRevisions(),
	}
	leftBuilder := NewBuilderWithOptions(n.data, page.PageTypeLeaf, opts)
	leftBuilder.SetPageID(n.PageID())
	rightBuilder := NewBuilderWithOptions(newNode.data, page.PageTypeLeaf, opts)
	rightBuilder.SetPageID(newNode.PageID())

	for i := uint16(0); i < splitIndex; i++ {
		key, val, ptr, flags, revision, err := src.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return nil, err
		}
		if err := leftBuilder.AddLeafEntryWithRevision(key, val, flags, ptr, revision); err != nil {
			return nil, err
		}
	}

	var pivotKey []byte
	for i := splitIndex; i < count; i++ {
		key, val, ptr, flags, revision, err := src.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return nil, err
		}
		if i == splitIndex {
			pivotKey = append([]byte(nil), key...)
		}
		if err := rightBuilder.AddLeafEntryWithRevision(key, val, flags, ptr, revision); err != nil {
			return nil, err
		}
	}

	leftBuilder.Finish()
	n.setRawFlags(binary.LittleEndian.Uint16(n.data[12:14]))
	n.count = binary.LittleEndian.Uint16(n.data[14:16])

	rightBuilder.Finish()
	newNode.setRawFlags(binary.LittleEndian.Uint16(newNode.data[12:14]))
	newNode.count = binary.LittleEndian.Uint16(newNode.data[14:16])

	return pivotKey, nil
}

func (n *Node) splitLeafColumnarPrefixV2Rebuild(newNode *Node) ([]byte, error) {
	splitIndex := n.Count() / 2
	count := n.Count()

	newNode.SetType(page.PageTypeLeaf)
	newNode.setLeafPrefixCompressed(n.leafPrefixCompressed())
	newNode.setLeafColumnar(n.leafColumnar())
	newNode.setLeafColumnarV2(n.leafColumnarV2())
	newNode.setLeafPackedValuePtr(n.leafPackedValuePtr())

	srcData := make([]byte, len(n.data))
	copy(srcData, n.data)
	src := NewNode(srcData)

	opts := BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        n.leafPackedValuePtr(),
		EntryRevisions:        n.leafEntryRevisions(),
	}
	leftBuilder := NewBuilderWithOptions(n.data, page.PageTypeLeaf, opts)
	leftBuilder.SetPageID(n.PageID())
	rightBuilder := NewBuilderWithOptions(newNode.data, page.PageTypeLeaf, opts)
	rightBuilder.SetPageID(newNode.PageID())

	for i := uint16(0); i < splitIndex; i++ {
		key, val, ptr, flags, revision, err := src.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return nil, err
		}
		if err := leftBuilder.AddLeafEntryWithRevision(key, val, flags, ptr, revision); err != nil {
			return nil, err
		}
	}

	var pivotKey []byte
	for i := splitIndex; i < count; i++ {
		key, val, ptr, flags, revision, err := src.GetLeafEntryViewWithRevision(i)
		if err != nil {
			return nil, err
		}
		if i == splitIndex {
			pivotKey = append([]byte(nil), key...)
		}
		if err := rightBuilder.AddLeafEntryWithRevision(key, val, flags, ptr, revision); err != nil {
			return nil, err
		}
	}

	leftBuilder.Finish()
	n.setRawFlags(binary.LittleEndian.Uint16(n.data[12:14]))
	n.count = binary.LittleEndian.Uint16(n.data[14:16])

	rightBuilder.Finish()
	newNode.setRawFlags(binary.LittleEndian.Uint16(newNode.data[12:14]))
	newNode.count = binary.LittleEndian.Uint16(newNode.data[14:16])

	return pivotKey, nil
}

func (n *Node) splitInternalBaseDeltaRebuild(newNode *Node) ([]byte, error) {
	splitIndex := n.Count() / 2
	count := n.Count()

	newNode.SetType(page.PageTypeInternal)

	srcData := make([]byte, len(n.data))
	copy(srcData, n.data)
	src := NewNode(srcData)

	leftBuilder := NewBuilderWithOptions(n.data, page.PageTypeInternal, BuilderOptions{
		InternalBaseDelta: true,
	})
	leftBuilder.SetPageID(n.PageID())
	rightBuilder := NewBuilderWithOptions(newNode.data, page.PageTypeInternal, BuilderOptions{
		InternalBaseDelta: true,
	})
	rightBuilder.SetPageID(newNode.PageID())

	var lowFence, highFence []byte
	if low, high, ok, err := src.InternalFenceBounds(); err != nil {
		return nil, err
	} else if ok {
		lowFence = append([]byte(nil), low...)
		highFence = append([]byte(nil), high...)
	}

	pivotView, _, err := src.GetInternalEntryView(splitIndex)
	if err != nil {
		return nil, err
	}
	pivotKey := append([]byte(nil), pivotView...)
	leftBuilder.SetInternalFenceBounds(lowFence, pivotKey)
	rightBuilder.SetInternalFenceBounds(pivotKey, highFence)

	for i := uint16(0); i < splitIndex; i++ {
		key, child, err := src.GetInternalEntryView(i)
		if err != nil {
			return nil, err
		}
		if err := leftBuilder.AddInternalChild(key, child); err != nil {
			return nil, err
		}
	}
	for i := splitIndex; i < count; i++ {
		key, child, err := src.GetInternalEntryView(i)
		if err != nil {
			return nil, err
		}
		if err := rightBuilder.AddInternalChild(key, child); err != nil {
			return nil, err
		}
	}

	leftBuilder.Finish()
	n.setRawFlags(binary.LittleEndian.Uint16(n.data[12:14]))
	n.count = binary.LittleEndian.Uint16(n.data[14:16])

	rightBuilder.Finish()
	newNode.setRawFlags(binary.LittleEndian.Uint16(newNode.data[12:14]))
	newNode.count = binary.LittleEndian.Uint16(newNode.data[14:16])

	return pivotKey, nil
}
