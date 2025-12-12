package zipper

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/snissn/gomap-gemini/TreeDB/batch"
	"github.com/snissn/gomap-gemini/TreeDB/node"
	"github.com/snissn/gomap-gemini/TreeDB/page"
	"github.com/snissn/gomap-gemini/TreeDB/pager"
)

type PageAllocator interface {
	Alloc() (uint64, error)
}

type Zipper struct {
	pager     *pager.Pager
	allocator PageAllocator
	nodePool  sync.Pool
}

type Split struct {
	Key    []byte
	NodeID uint64
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:     p,
		allocator: a,
		nodePool: sync.Pool{
			New: func() any {
				b := make([]byte, page.PageSize)
				return &b
			},
		},
	}
}

func (z *Zipper) getPooledBuf() *[]byte {
	return z.nodePool.Get().(*[]byte)
}

func (z *Zipper) putPooledBuf(b *[]byte) {
	z.nodePool.Put(b)
}

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID and list of retired pages.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (uint64, []uint64, error) {
	ops := b.Ops()
	keys := make([]string, 0, len(ops))
	for k := range ops {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newRoot, splits, retired, err := z.writeRecursive(rootID, keys, ops)
	if err != nil {
		return 0, nil, err
	}

	if len(splits) > 0 {
		// Root split! Create new internal root.
		// If splits > 0, we have the new "original" root (newRoot) plus N splits.
		// The new internal root will point to:
		// - newRoot (First child)
		// - splits... (Subsequent children)

		newRootID, err := z.allocator.Alloc()
		if err != nil {
			return 0, nil, err
		}

		buf := z.getPooledBuf()
		defer z.putPooledBuf(buf)
		data := *buf

		n := node.NewNode(data)
		n.SetPageID(newRootID)
		n.SetType(page.PageTypeInternal)
		n.SetCount(0)

		// 1. Add first child (the "new version" of the old root)
		// Internal nodes usually start with a "dummy" key (empty byte slice) for the first child?
		// OR does the first child have a key?
		// In B-Trees, usually keys are separators.
		// If using "min-key" convention (where key K covers range [K, next K)), then first child key is min-key.
		// TreeDB seems to use "Separator Key K means child covers >= K"?
		// Let's check `node/internal.go` or usage.
		// `AddInternalChild` takes (key, val).
		// Existing code used `[]byte{}` for first child.

		// However, wait.
		// `newRoot` is the left-most node.
		// `splits[0]` is the next node to the right.
		// `splits[0].Key` is the separator key (first key of that node).

		// So for the first child, we need a key that is <= everything in it.
		// Since it's the root, it covers everything from -infinity.
		// So []byte{} is appropriate if it represents "Start".

		if err := n.AddInternalChild([]byte{}, newRoot); err != nil {
			return 0, nil, err
		}

		// 2. Add splits
		for _, s := range splits {
			if err := n.AddInternalChild(s.Key, s.NodeID); err != nil {
				// If the NEW ROOT fills up, we have a problem.
				// This means we split the root into SO MANY nodes that they don't fit in one parent.
				// This requires recursion (growing height by > 1).
				// For now, let's assume one level growth is enough.
				// If not, we error "root split overflow".
				return 0, nil, errors.New("root split overflow: batch too large for single level growth")
			}
		}

		n.UpdateChecksum()

		if err := z.pager.Write(newRootID, data); err != nil {
			return 0, nil, err
		}

		return newRootID, retired, nil
	}

	return newRoot, retired, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splits, retiredPages, error.
func (z *Zipper) writeRecursive(pageID uint64, keys []string, ops map[string]batch.Entry) (uint64, []Split, []uint64, error) {
	// 1. Allocate New Page (COW)
	newPageID, err := z.allocator.Alloc()
	if err != nil {
		return 0, nil, nil, err
	}

	buf := z.getPooledBuf()
	newData := *buf
	newNode := node.NewNode(newData)
	newNode.SetPageID(newPageID)
	newNode.SetCount(0)

	// Zero-Copy Read: Use Get instead of ReadPage
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		z.putPooledBuf(buf)
		return 0, nil, nil, err
	}
	oldNode := node.NewNode(oldData)

	// Track retired page
	var retired []uint64
	if pageID != 0 {
		retired = append(retired, pageID)
	}

	newNode.SetType(oldNode.Type())

	if oldNode.Type() == page.PageTypeLeaf {
		// Merge Leaf
		nr, splits, err := z.mergeLeaf(oldNode, newNode, keys, ops)
		if err == nil {
			if werr := z.pager.Write(newPageID, newData); werr != nil {
				z.putPooledBuf(buf)
				return 0, nil, nil, werr
			}
		}
		z.putPooledBuf(buf)
		return nr, splits, retired, err
	} else if oldNode.Type() == page.PageTypeInternal {
		// Internal merge
		nr, splits, childRetired, err := z.mergeInternal(oldNode, newNode, keys, ops)
		if err != nil {
			z.putPooledBuf(buf)
			return 0, nil, nil, err
		}

		if err := z.pager.Write(newPageID, newData); err != nil {
			z.putPooledBuf(buf)
			return 0, nil, nil, err
		}
		z.putPooledBuf(buf)

		retired = append(retired, childRetired...)
		return nr, splits, retired, nil
	} else {
		// Handle Page 0 / Empty / New Tree case
		if oldNode.Type() == 0 {
			newNode.SetType(page.PageTypeLeaf)
			nr, splits, err := z.mergeLeaf(oldNode, newNode, keys, ops)
			if err == nil {
				if werr := z.pager.Write(newPageID, newData); werr != nil {
					z.putPooledBuf(buf)
					return 0, nil, nil, werr
				}
			}
			z.putPooledBuf(buf)
			return nr, splits, retired, err
		}
		z.putPooledBuf(buf)
		return 0, nil, nil, page.ErrInvalidPageType
	}
}

func (z *Zipper) mergeLeaf(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []Split, error) {
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	keyIdx := 0

	var splits []Split
	
	// Buffers we need to release
	var splitBufs []*[]byte
	defer func() {
		for _, b := range splitBufs {
			z.putPooledBuf(b)
		}
	}()

	// Current target node we are filling (starts with newNode)
	target := newNode
	// If we split, current target changes to the split node.

	for {
		// Pick next key: min(oldNode[oldIdx], keys[keyIdx])
		var useBatch bool

		if oldIdx >= oldCount && keyIdx >= len(keys) {
			break
		}

		if oldIdx >= oldCount {
			useBatch = true
		} else if keyIdx >= len(keys) {
			// useOld = true
		} else {
			// Compare
			oldEntry, err := oldNode.GetLeafEntry(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			batchKey := []byte(keys[keyIdx])

			cmp := bytes.Compare(oldEntry.Key, batchKey)
			if cmp < 0 {
				// useOld = true
			} else if cmp > 0 {
				useBatch = true
			} else {
				// Equal: Update (Batch wins)
				useBatch = true
				oldIdx++ // Skip old
			}
		}

		// Key/Val to insert
		var key, val []byte
		var flags byte
		var valPtr page.ValuePtr

		if useBatch {
			op := ops[keys[keyIdx]]
			keyIdx++
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
			entry, err := oldNode.GetLeafEntry(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			oldIdx++
			key = entry.Key
			if entry.Flags&node.FlagTombstone != 0 {
				continue // Skip tombstones
			}
			flags = entry.Flags
			if flags&node.FlagPointer != 0 {
				valPtr = entry.ValuePtr
			} else {
				val = entry.Value
			}
		}

		// Insert into target
		err := target.AddLeafEntry(key, val, flags, valPtr)
		if err == node.ErrNodeFull {
			// SPLIT!
			
			// 1. Write the current target to disk (it's full)
			// (If it's newNode, it's written by caller, but we need checksum now?)
			// Caller writes newNode. We only write splits.
			
			// 2. Allocate NEW split node
			sid, err := z.allocator.Alloc()
			if err != nil {
				return 0, nil, err
			}
			
			buf := z.getPooledBuf()
			splitBufs = append(splitBufs, buf) // Track for cleanup
			sdata := *buf
			
			splitNode := node.NewNode(sdata)
			splitNode.SetPageID(sid)
			splitNode.SetType(page.PageTypeLeaf)
			splitNode.SetCount(0)

			// Record split
			// Key is the FIRST key of the new node.
			splitKey := append([]byte(nil), key...) // Deep copy
			splits = append(splits, Split{Key: splitKey, NodeID: sid})
			
			// Switch target
			// If previous target was a split node, we must write it now?
			// The caller writes `newNode`.
			// `splitBufs` holds all split buffers.
			// We can write them all at the end, or incrementally.
			// Writing incrementally is safer for memory if lots of splits?
			// But we have them pinned in `splitBufs`.
			
			// If we are chaining splits:
			// newNode -> split1 -> split2 -> split3
			// We need to write split1, split2, split3.
			// But since we are iterating, the *previous* split (or newNode) is "done" except for checksum.
			
			if target != newNode {
				target.UpdateChecksum()
				if err := z.pager.Write(target.PageID(), target.Data()); err != nil {
					return 0, nil, err
				}
			} else {
				// We don't write newNode here, caller does.
				// But we should UpdateChecksum? Caller does.
			}
			
			target = splitNode
			
			// Retry insert into new target
			err = target.AddLeafEntry(key, val, flags, valPtr)
			if err != nil {
				return 0, nil, err // Should not happen on empty node unless key too large
			}
		} else if err != nil {
			return 0, nil, err
		}
	}

	// Finalize last split node
	if target != newNode {
		target.UpdateChecksum()
		if err := z.pager.Write(target.PageID(), target.Data()); err != nil {
			return 0, nil, err
		}
	}
	
	// Note: splitBufs are released by defer.
	// But we wrote them to Pager (which copies them or uses them).
	// Pager.Write copies if it's in-memory pager?
	// `pager.Write` writes to mmap or file.
	// If mmap, it copies.
	// So we can release buffer.

	newNode.UpdateChecksum()
	return newNode.PageID(), splits, nil
}

func (z *Zipper) mergeInternal(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []Split, []uint64, error) {
	count := oldNode.Count()
	
	var splits []Split
	var splitBufs []*[]byte
	defer func() {
		for _, b := range splitBufs {
			z.putPooledBuf(b)
		}
	}()
	
	target := newNode
	var retired []uint64

	keyIdx := 0

	for i := uint16(0); i < count; i++ {
		entry, err := oldNode.GetInternalEntry(i)
		if err != nil {
			return 0, nil, nil, err
		}

		// Determine End Key for this child
		var endKey []byte
		if i+1 < count {
			nextEntry, err := oldNode.GetInternalEntry(i+1)
			if err != nil {
				return 0, nil, nil, err
			}
			endKey = nextEntry.Key
		}

		// Collect keys for this child
		var childKeys []string
		for keyIdx < len(keys) {
			k := keys[keyIdx]
			if endKey == nil || bytes.Compare([]byte(k), endKey) < 0 {
				childKeys = append(childKeys, k)
				keyIdx++
			} else {
				break
			}
		}

		var newChildID uint64
		var childSplits []Split

		if len(childKeys) > 0 {
			// Recurse
			ncID, cs, childRet, err := z.writeRecursive(entry.ChildPageID, childKeys, ops)
			if err != nil {
				return 0, nil, nil, err
			}
			newChildID = ncID
			childSplits = cs
			retired = append(retired, childRet...)
		} else {
			// Reuse
			newChildID = entry.ChildPageID
		}

		// Add (Key, NewChildID) to target
		if newChildID >= z.pager.PageCount() {
			return 0, nil, nil, errors.New("zipper: detected OOB child ID")
		}
		
		err = target.AddInternalChild(entry.Key, newChildID)
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, newNode, &splits, &splitBufs, entry.Key, newChildID)
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
				target, err = z.createNewSplitInternal(target, newNode, &splits, &splitBufs, s.Key, s.NodeID)
				if err != nil {
					return 0, nil, nil, err
				}
			} else if err != nil {
				return 0, nil, nil, err
			}
		}
	}
	
	// Finalize last split node
	if target != newNode {
		target.UpdateChecksum()
		if err := z.pager.Write(target.PageID(), target.Data()); err != nil {
			return 0, nil, nil, err
		}
	}

	newNode.UpdateChecksum()
	return newNode.PageID(), splits, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, newNode *node.Node, splits *[]Split, splitBufs *[]*[]byte, key []byte, val uint64) (*node.Node, error) {
	// 1. Write current (if not newNode)
	if currentTarget != newNode {
		currentTarget.UpdateChecksum()
		if err := z.pager.Write(currentTarget.PageID(), currentTarget.Data()); err != nil {
			return nil, err
		}
	}
	
	// 2. Alloc new
	sid, err := z.allocator.Alloc()
	if err != nil {
		return nil, err
	}
	
	buf := z.getPooledBuf()
	*splitBufs = append(*splitBufs, buf)
	sdata := *buf
	
	sn := node.NewNode(sdata)
	sn.SetPageID(sid)
	sn.SetType(page.PageTypeInternal)
	sn.SetCount(0)
	
	*splits = append(*splits, Split{Key: append([]byte(nil), key...), NodeID: sid})
	
	// Retry insert
	if err := sn.AddInternalChild(key, val); err != nil {
		return nil, err
	}
	
	return sn, nil
}