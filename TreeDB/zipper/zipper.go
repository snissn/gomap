package zipper

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type PageAllocator interface {
	Alloc() (uint64, error)
}

type Zipper struct {
	pager     *pager.Pager
	allocator PageAllocator

	leafReserveBytes     int
	internalReserveBytes int
}

type Split struct {
	Key    []byte
	NodeID uint64
}

type internalEntry struct {
	key   []byte
	child uint64
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:     p,
		allocator: a,
	}
}

// SetFillTargets configures soft-full thresholds for newly written pages.
// Targets are in parts-per-million where 1_000_000 means "allow full pages".
func (z *Zipper) SetFillTargets(leafPPM, internalPPM uint32) {
	z.leafReserveBytes = reserveBytesFromPPM(leafPPM)
	z.internalReserveBytes = reserveBytesFromPPM(internalPPM)
}

func reserveBytesFromPPM(ppm uint32) int {
	if ppm >= 1_000_000 {
		return 0
	}
	// Reserve a fixed fraction of the page size.
	reserve := int((uint64(page.PageSize) * uint64(1_000_000-ppm)) / 1_000_000)
	if reserve < 0 {
		return 0
	}
	return reserve
}

func (z *Zipper) leafSoftFull(b *node.Builder, entrySize int) bool {
	if z.leafReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.leafReserveBytes
}

func (z *Zipper) internalSoftFull(b *node.Builder, entrySize int) bool {
	if z.internalReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.internalReserveBytes
}

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID, list of retired pages, and commit metrics.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (uint64, []uint64, adaptive.Metrics, error) {
	var metrics adaptive.Metrics
	ops := b.SortedEntries()
	if len(ops) == 0 {
		return rootID, nil, metrics, nil
	}

	newRoot, splits, retired, err := z.writeRecursive(rootID, ops, &metrics)
	if err != nil {
		return 0, nil, metrics, err
	}

	if len(splits) > 0 {
		// Root split!
		// The children for the next level are:
		// 1. The new version of the old root (newRoot) with Key=[] (effectively min key)
		// 2. The splits (siblings) generated from it.

		currentLevelNodes := []Split{{Key: []byte{}, NodeID: newRoot}}
		currentLevelNodes = append(currentLevelNodes, splits...)

		// Iteratively build levels up until all nodes fit in one root.
		for {
			// If we only have 1 node left, that is our new root.
			if len(currentLevelNodes) == 1 {
				return currentLevelNodes[0].NodeID, retired, metrics, nil
			}

			var nextLevelNodes []Split

			// Allocate a node for the current batch of children
			var currentBuilder *node.Builder

			// We need to track the "Start Key" of the current builder to promote it.
			var currentStartKey []byte

			for i, child := range currentLevelNodes {
				if currentBuilder == nil {
					// Start new node
					pid, err := z.allocator.Alloc()
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}

					currentBuilder = node.NewBuilder(data, page.PageTypeInternal)
					currentBuilder.SetPageID(pid)

					currentStartKey = child.Key
				}

				// Add child
				childSize := 2 + 8 + len(child.Key)
				var err error
				if z.internalSoftFull(currentBuilder, childSize) {
					err = node.ErrNodeFull
				} else {
					err = currentBuilder.AddInternalChild(child.Key, child.NodeID)
				}
				if err == node.ErrNodeFull {
					// Finish current
					_ = currentBuilder.Finish()
					// Promote
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})

					// Start new for THIS child (retry)
					pid, err := z.allocator.Alloc()
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}
					currentBuilder = node.NewBuilder(data, page.PageTypeInternal)
					currentBuilder.SetPageID(pid)
					currentStartKey = child.Key

					if err := currentBuilder.AddInternalChild(child.Key, child.NodeID); err != nil {
						return 0, nil, metrics, err // Should fit in empty node
					}
				} else if err != nil {
					return 0, nil, metrics, err
				}

				// If this was the last child, finish
				if i == len(currentLevelNodes)-1 {
					_ = currentBuilder.Finish()
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})
					currentBuilder = nil
				}
			}

			// Move up
			currentLevelNodes = nextLevelNodes
		}
	}

	return newRoot, retired, metrics, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splits, retiredPages, error.
func (z *Zipper) writeRecursive(pageID uint64, ops []batch.Entry, metrics *adaptive.Metrics) (uint64, []Split, []uint64, error) {
	// 1. Allocate New Page (COW)
	newPageID, err := z.allocator.Alloc()
	if err != nil {
		return 0, nil, nil, err
	}

	newData, err := z.pager.GetForWrite(newPageID)
	if err != nil {
		return 0, nil, nil, err
	}

	// Zero-Copy Read: Use Get instead of ReadPage
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		return 0, nil, nil, err
	}
	oldNode := node.NewNode(oldData)

	// Create Builder for new page
	builder := node.NewBuilder(newData, oldNode.Type())
	builder.SetPageID(newPageID)

	// Track retired page
	var retired []uint64
	if pageID != 0 {
		retired = append(retired, pageID)
	}

	if oldNode.Type() == page.PageTypeLeaf {
		// Merge Leaf
		nr, splits, err := z.mergeLeaf(oldNode, builder, ops, metrics)
		if err == nil {
			n := builder.Finish() // Finish writes header/checksum
			// Update Metrics
			metrics.IndexWriteBytes += page.PageSize
			metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		}
		return nr, splits, retired, err
	} else if oldNode.Type() == page.PageTypeInternal {
		// Internal merge
		nr, splits, childRetired, err := z.mergeInternal(oldNode, builder, ops, metrics)
		if err != nil {
			return 0, nil, nil, err
		}

		_ = builder.Finish()
		metrics.IndexWriteBytes += page.PageSize

		retired = append(retired, childRetired...)
		return nr, splits, retired, nil
	} else {
		// Handle Page 0 / Empty / New Tree case
		if oldNode.Type() == 0 {
			// Reuse builder, set type
			builder = node.NewBuilder(newData, page.PageTypeLeaf)
			builder.SetPageID(newPageID)

			nr, splits, err := z.mergeLeaf(oldNode, builder, ops, metrics)
			if err == nil {
				n := builder.Finish()
				metrics.IndexWriteBytes += page.PageSize
				metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
			}
			return nr, splits, retired, err
		}
		return 0, nil, nil, page.ErrInvalidPageType
	}
}

func (z *Zipper) mergeLeaf(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, metrics *adaptive.Metrics) (uint64, []Split, error) {
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	opIdx := 0

	var splits []Split

	// Current target builder
	target := builder

	for {
		// Pick next key: min(oldNode[oldIdx], ops[opIdx])
		var useBatch bool

		if oldIdx >= oldCount && opIdx >= len(ops) {
			break
		}

		if oldIdx >= oldCount {
			useBatch = true
		} else if opIdx >= len(ops) {
			// useOld = true
		} else {
			// Compare
			// Optimization: GetLeafEntryView (Zero Copy)
			k, _, ptr, f, err := oldNode.GetLeafEntryView(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			batchKey := ops[opIdx].Key

			cmp := bytes.Compare(k, batchKey)
			if cmp < 0 {
				// useOld = true
			} else if cmp > 0 {
				useBatch = true
			} else {
				// Equal: Update (Batch wins)
				// The old entry is being overwritten or deleted.
				// If it was a pointer, track it as dead bytes.
				if f&node.FlagPointer != 0 {
					metrics.SlabDeadBytes += int(ptr.Length)
					if metrics.SlabDeadBytesByFile == nil {
						metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
					}
					metrics.SlabDeadBytesByFile[ptr.FileID] += int64(ptr.Length)
				}

				useBatch = true
				oldIdx++ // Skip old
			}
		}

		// Key/Val to insert
		var key, val []byte
		var flags byte
		var valPtr page.ValuePtr

		if useBatch {
			op := ops[opIdx]
			opIdx++
			if op.Type == batch.OpDelete {
				continue // Skip insert
			}
			key = op.Key
			if op.IsPtr {
				flags = node.FlagPointer
				valPtr = op.ValuePtr
			} else {
				flags = node.FlagInline
				val = op.Value
			}
		} else {
			// useOld
			// Optimization: View
			k, v, ptr, f, err := oldNode.GetLeafEntryView(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			oldIdx++
			key = k
			if f&node.FlagTombstone != 0 {
				continue // Skip tombstones
			}
			flags = f
			if f&node.FlagPointer != 0 {
				valPtr = ptr
			} else {
				val = v
			}
		}

		// Insert into target builder
		entrySize := 7 + len(key)
		if flags&node.FlagPointer != 0 {
			entrySize += page.ValuePtrSize
		} else {
			entrySize += len(val)
		}
		var err error
		if z.leafSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddLeafEntry(key, val, flags, valPtr)
		}
		if err == node.ErrNodeFull {
			// SPLIT!

			// 1. Finish current target (writes header/checksum)
			if target != builder {
				n := target.Finish()
				metrics.IndexWriteBytes += page.PageSize
				metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
				metrics.Splits++
			}

			// 2. Allocate NEW split node
			sid, err := z.allocator.Alloc()
			if err != nil {
				return 0, nil, err
			}

			sdata, err := z.pager.GetForWrite(sid)
			if err != nil {
				return 0, nil, err
			}

			// New Builder
			splitBuilder := node.NewBuilder(sdata, page.PageTypeLeaf)
			splitBuilder.SetPageID(sid)

			// Record split
			splitKey := append([]byte(nil), key...) // Deep copy
			splits = append(splits, Split{Key: splitKey, NodeID: sid})

			target = splitBuilder

			// Retry insert
			err = target.AddLeafEntry(key, val, flags, valPtr)
			if err != nil {
				return 0, nil, err
			}
		} else if err != nil {
			return 0, nil, err
		}
	}

	// Finalize last split node
	if target != builder {
		n := target.Finish()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		metrics.Splits++
	}

	// 'builder' is finalized by caller.

	return builder.PageID(), splits, nil
}

func (z *Zipper) mergeInternal(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, metrics *adaptive.Metrics) (uint64, []Split, []uint64, error) {
	count := oldNode.Count()

	var splits []Split

	var retired []uint64

	opIdx := 0

	entries := make([]internalEntry, 0, int(count)+4)

	for i := uint16(0); i < count; i++ {
		// Optimization: Use View to avoid alloc
		key, childID, err := oldNode.GetInternalEntryView(i)
		if err != nil {
			return 0, nil, nil, err
		}

		// Determine End Key for this child
		var endKey []byte
		if i+1 < count {
			nextKey, _, err := oldNode.GetInternalEntryView(i + 1)
			if err != nil {
				return 0, nil, nil, err
			}
			endKey = nextKey
		}

		// Identify ops range for this child
		// ops[opIdx] ... until op.Key >= endKey
		startOpIdx := opIdx
		for opIdx < len(ops) {
			if endKey == nil || bytes.Compare(ops[opIdx].Key, endKey) < 0 {
				opIdx++
			} else {
				break
			}
		}
		childOps := ops[startOpIdx:opIdx]

		var newChildID uint64
		var childSplits []Split

		if len(childOps) > 0 {
			// Recurse
			ncID, cs, childRet, err := z.writeRecursive(childID, childOps, metrics)
			if err != nil {
				return 0, nil, nil, err
			}
			newChildID = ncID
			childSplits = cs
			retired = append(retired, childRet...)
		} else {
			// Reuse
			newChildID = childID
		}

		if newChildID >= z.pager.PageCount() {
			return 0, nil, nil, errors.New("zipper: detected OOB child ID")
		}
		entries = append(entries, internalEntry{key: append([]byte(nil), key...), child: newChildID})

		// Add sibling splits
		for _, s := range childSplits {
			if s.NodeID >= z.pager.PageCount() {
				return 0, nil, nil, errors.New("zipper: detected OOB child ID (split)")
			}
			entries = append(entries, internalEntry{key: append([]byte(nil), s.Key...), child: s.NodeID})
		}
	}

	coalesced, extraRetired, err := z.coalesceLeafChildren(entries, metrics)
	if err != nil {
		return 0, nil, nil, err
	}
	if len(extraRetired) > 0 {
		retired = append(retired, extraRetired...)
	}

	coalesced, extraRetired, err = z.coalesceInternalChildren(coalesced, metrics)
	if err != nil {
		return 0, nil, nil, err
	}
	if len(extraRetired) > 0 {
		retired = append(retired, extraRetired...)
	}

	// Write final internal entries, splitting if needed.
	target := builder
	for i, e := range coalesced {
		if i == 0 && (e.key == nil) {
			e.key = []byte{}
		}
		entrySize := 2 + 8 + len(e.key)
		if z.internalSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddInternalChild(e.key, e.child)
		}
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, e.key, e.child, metrics)
			if err != nil {
				return 0, nil, nil, err
			}
		} else if err != nil {
			return 0, nil, nil, err
		}
	}

	// Finalize last split node
	if target != builder {
		_ = target.Finish()
		metrics.IndexWriteBytes += page.PageSize
	}

	// builder finalized by caller.
	return builder.PageID(), splits, retired, nil
}

func (z *Zipper) coalesceLeafChildren(entries []internalEntry, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}

	var retired []uint64

	loadLeaf := func(id uint64) (*node.Node, bool, error) {
		data, err := z.pager.Get(id)
		if err != nil {
			return nil, false, err
		}
		n := node.NewNode(data)
		if n.Type() != page.PageTypeLeaf {
			return nil, false, nil
		}
		return n, true, nil
	}

	// First pass: prune empty leaf children (except keep the first slot).
	out := entries[:0]
	for i, e := range entries {
		if i == 0 {
			out = append(out, e)
			continue
		}
		n, ok, err := loadLeaf(e.child)
		if err != nil {
			return nil, nil, err
		}
		if ok && n.Count() == 0 {
			retired = append(retired, e.child)
			continue
		}
		out = append(out, e)
	}
	entries = out
	if len(entries) < 2 {
		return entries, retired, nil
	}

	const underfullPPM = 350_000
	pageCap := page.PageSize - node.NodeHeaderSize

	leafEntryBytes := func(key, val []byte, ptr page.ValuePtr, flags byte) int {
		entrySize := 7 + len(key)
		if flags&node.FlagPointer != 0 {
			entrySize += page.ValuePtrSize
		} else {
			entrySize += len(val)
		}
		return entrySize + node.DirectoryEntrySize
	}

	leafRequiredBytes := func(n *node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			sum += leafEntryBytes(k, v, ptr, flags)
			if sum > pageCap {
				return sum, nil
			}
		}
		return sum, nil
	}

	fillPPM := func(n *node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	buildMergedLeaf := func(left, right *node.Node) (uint64, bool, error) {
		pid, err := z.allocator.Alloc()
		if err != nil {
			return 0, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, false, err
		}
		b := node.NewBuilder(data, page.PageTypeLeaf)
		b.SetPageID(pid)

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, v, ptr, flags, err := n.GetLeafEntryView(i)
				if err != nil {
					return err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if err := b.AddLeafEntry(k, v, flags, ptr); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		n := b.Finish()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-n.FreeSpace()) / float64(page.PageSize)
		return pid, true, nil
	}

	rebalanceLeaves := func(left, right *node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
		// Build new left leaf, filling until soft-full, leaving >=1 entry for right.
		lid, err := z.allocator.Alloc()
		if err != nil {
			return 0, 0, nil, false, err
		}
		ldata, err := z.pager.GetForWrite(lid)
		if err != nil {
			return 0, 0, nil, false, err
		}
		lb := node.NewBuilder(ldata, page.PageTypeLeaf)
		lb.SetPageID(lid)

		rid, err := z.allocator.Alloc()
		if err != nil {
			retired = append(retired, lid)
			return 0, 0, nil, false, err
		}
		rdata, err := z.pager.GetForWrite(rid)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}
		rb := node.NewBuilder(rdata, page.PageTypeLeaf)
		rb.SetPageID(rid)

		// Collect combined entries in-order without copying.
		type ev struct {
			k     []byte
			v     []byte
			ptr   page.ValuePtr
			flags byte
			size  int
		}
		combined := make([]ev, 0, int(left.Count()+right.Count()))
		for _, src := range []*node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, v, ptr, flags, err := src.GetLeafEntryView(i)
				if err != nil {
					retired = append(retired, lid, rid)
					return 0, 0, nil, false, err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				combined = append(combined, ev{k: k, v: v, ptr: ptr, flags: flags, size: leafEntryBytes(k, v, ptr, flags)})
			}
		}
		if len(combined) < 2 {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		// Greedy split: fill left near soft-full target, leave >=1 entry for right.
		splitAt := 0
		for i := 0; i < len(combined)-1; i++ {
			if z.leafSoftFull(lb, combined[i].size-node.DirectoryEntrySize) {
				break
			}
			if err := lb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				retired = append(retired, lid, rid)
				if err == node.ErrNodeFull {
					return 0, 0, nil, false, nil
				}
				return 0, 0, nil, false, err
			}
			splitAt = i + 1
		}
		if splitAt == 0 || splitAt >= len(combined) {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		rightStart = append([]byte(nil), combined[splitAt].k...)
		for i := splitAt; i < len(combined); i++ {
			if err := rb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				retired = append(retired, lid, rid)
				if err == node.ErrNodeFull {
					return 0, 0, nil, false, nil
				}
				return 0, 0, nil, false, err
			}
		}

		ln := lb.Finish()
		rn := rb.Finish()
		metrics.IndexWriteBytes += 2 * page.PageSize
		metrics.LeafFill += float64(page.PageSize-ln.FreeSpace()) / float64(page.PageSize)
		metrics.LeafFill += float64(page.PageSize-rn.FreeSpace()) / float64(page.PageSize)
		return lid, rid, rightStart, true, nil
	}

	// Second pass: attempt sibling merge/rebalance for underfull adjacent leaves.
	i := 0
	for i < len(entries)-1 {
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, okL, err := loadLeaf(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, okR, err := loadLeaf(rightID)
		if err != nil {
			return nil, nil, err
		}
		if !okL || !okR {
			i++
			continue
		}

		if left.Count() == 0 {
			// If this is a non-first child it would have been pruned already.
			i++
			continue
		}

		leftFill := fillPPM(left)
		rightFill := fillPPM(right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			i++
			continue
		}

		leftBytes, err := leafRequiredBytes(left)
		if err != nil {
			return nil, nil, err
		}
		rightBytes, err := leafRequiredBytes(right)
		if err != nil {
			return nil, nil, err
		}

		if leftBytes+rightBytes <= pageCap {
			mergedID, ok, err := buildMergedLeaf(left, right)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				retired = append(retired, leftID, rightID)
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				continue
			}
		}

		// If merge isn't possible, attempt a bounded rebalance.
		leftNewID, rightNewID, rightStart, ok, err := rebalanceLeaves(left, right)
		if err != nil {
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			retired = append(retired, leftID, rightID)
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) coalesceInternalChildren(entries []internalEntry, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}

	var retired []uint64

	loadInternal := func(id uint64) (*node.Node, bool, error) {
		data, err := z.pager.Get(id)
		if err != nil {
			return nil, false, err
		}
		n := node.NewNode(data)
		if n.Type() != page.PageTypeInternal {
			return nil, false, nil
		}
		return n, true, nil
	}

	fillPPM := func(n *node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	pageCap := page.PageSize - node.NodeHeaderSize
	internalEntryBytes := func(key []byte) int {
		if key == nil {
			key = []byte{}
		}
		// Internal entry: keylen(uint16) + child(uint64) + key bytes + directory entry.
		return (2 + 8 + len(key)) + node.DirectoryEntrySize
	}
	internalRequiredBytes := func(n *node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, _, err := n.GetInternalEntryView(i)
			if err != nil {
				return 0, err
			}
			sum += internalEntryBytes(k)
			if sum > pageCap {
				return sum, nil
			}
		}
		return sum, nil
	}

	buildMergedInternal := func(left, right *node.Node) (uint64, bool, error) {
		pid, err := z.allocator.Alloc()
		if err != nil {
			return 0, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, false, err
		}
		b := node.NewBuilder(data, page.PageTypeInternal)
		b.SetPageID(pid)

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, child, err := n.GetInternalEntryView(i)
				if err != nil {
					return err
				}
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChild(k, child); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		_ = b.Finish()
		metrics.IndexWriteBytes += page.PageSize
		return pid, true, nil
	}

	rebalanceInternals := func(left, right *node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
		lid, err := z.allocator.Alloc()
		if err != nil {
			return 0, 0, nil, false, err
		}
		ldata, err := z.pager.GetForWrite(lid)
		if err != nil {
			return 0, 0, nil, false, err
		}
		lb := node.NewBuilder(ldata, page.PageTypeInternal)
		lb.SetPageID(lid)

		rid, err := z.allocator.Alloc()
		if err != nil {
			retired = append(retired, lid)
			return 0, 0, nil, false, err
		}
		rdata, err := z.pager.GetForWrite(rid)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}
		rb := node.NewBuilder(rdata, page.PageTypeInternal)
		rb.SetPageID(rid)

		combined := make([]internalEntry, 0, int(left.Count()+right.Count()))
		for _, src := range []*node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, child, err := src.GetInternalEntryView(i)
				if err != nil {
					retired = append(retired, lid, rid)
					return 0, 0, nil, false, err
				}
				combined = append(combined, internalEntry{key: k, child: child})
			}
		}
		if len(combined) < 2 {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		splitAt := len(combined) / 2
		if splitAt < 1 {
			splitAt = 1
		}
		if splitAt >= len(combined) {
			splitAt = len(combined) - 1
		}

		build := func(b *node.Builder, list []internalEntry) error {
			for i, e := range list {
				k := e.key
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if i > 0 && z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChild(k, e.child); err != nil {
					return err
				}
			}
			return nil
		}

		try := func(splitAt int) ([]byte, bool, error) {
			lb2 := node.NewBuilder(ldata, page.PageTypeInternal)
			lb2.SetPageID(lid)
			rb2 := node.NewBuilder(rdata, page.PageTypeInternal)
			rb2.SetPageID(rid)

			if err := build(lb2, combined[:splitAt]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			if err := build(rb2, combined[splitAt:]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			lb2.Finish()
			rb2.Finish()
			rs := combined[splitAt].key
			if rs == nil {
				rs = []byte{}
			}
			return append([]byte(nil), rs...), true, nil
		}

		// Adjust split point until both sides fit.
		rightStart, ok, err = try(splitAt)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}
		if !ok {
			for d := 1; d < len(combined)-1; d++ {
				if splitAt-d >= 1 {
					if rs, ok2, err2 := try(splitAt - d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
				if splitAt+d < len(combined) {
					if rs, ok2, err2 := try(splitAt + d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
			}
			if !ok {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, nil
			}
		}

		metrics.IndexWriteBytes += 2 * page.PageSize
		return lid, rid, rightStart, true, nil
	}

	const underfullPPM = 350_000

	i := 0
	for i < len(entries)-1 {
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, okL, err := loadInternal(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, okR, err := loadInternal(rightID)
		if err != nil {
			return nil, nil, err
		}
		if !okL || !okR {
			i++
			continue
		}

		leftFill := fillPPM(left)
		rightFill := fillPPM(right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			i++
			continue
		}

		leftBytes, err := internalRequiredBytes(left)
		if err != nil {
			return nil, nil, err
		}
		rightBytes, err := internalRequiredBytes(right)
		if err != nil {
			return nil, nil, err
		}

		// Attempt a full sibling merge when the combined entries should fit in one page
		// while still respecting the configured soft-full reserve (if any).
		if leftBytes+rightBytes <= pageCap-z.internalReserveBytes {
			mergedID, ok, err := buildMergedInternal(left, right)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				retired = append(retired, leftID, rightID)
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				continue
			}
		}

		leftNewID, rightNewID, rightStart, ok, err := rebalanceInternals(left, right)
		if err != nil {
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			retired = append(retired, leftID, rightID)
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, rootBuilder *node.Builder, splits *[]Split, key []byte, val uint64, metrics *adaptive.Metrics) (*node.Builder, error) {
	// 1. Finish current (if not rootBuilder)
	if currentTarget != rootBuilder {
		_ = currentTarget.Finish()
		metrics.IndexWriteBytes += page.PageSize
	}

	// 2. Alloc new
	sid, err := z.allocator.Alloc()
	if err != nil {
		return nil, err
	}

	sdata, err := z.pager.GetForWrite(sid)
	if err != nil {
		return nil, err
	}

	sb := node.NewBuilder(sdata, page.PageTypeInternal)
	sb.SetPageID(sid)

	*splits = append(*splits, Split{Key: append([]byte(nil), key...), NodeID: sid})

	// Retry insert
	if err := sb.AddInternalChild(key, val); err != nil {
		return nil, err
	}

	return sb, nil
}
