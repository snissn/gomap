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
}

type Split struct {
	Key    []byte
	NodeID uint64
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:     p,
		allocator: a,
	}
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
				err := currentBuilder.AddInternalChild(child.Key, child.NodeID)
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
		err := target.AddLeafEntry(key, val, flags, valPtr)
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

	target := builder
	var retired []uint64

	opIdx := 0

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

		// Add (Key, NewChildID) to target builder
		if newChildID >= z.pager.PageCount() {
			return 0, nil, nil, errors.New("zipper: detected OOB child ID")
		}

		err = target.AddInternalChild(key, newChildID)
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, key, newChildID, metrics)
			if err != nil {
				return 0, nil, nil, err
			}
		} else if err != nil {
			return 0, nil, nil, err
		}

		// Add sibling splits
		for _, s := range childSplits {
			err = target.AddInternalChild(s.Key, s.NodeID)
			if err == node.ErrNodeFull {
				target, err = z.createNewSplitInternal(target, builder, &splits, s.Key, s.NodeID, metrics)
				if err != nil {
					return 0, nil, nil, err
				}
			} else if err != nil {
				return 0, nil, nil, err
			}
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
