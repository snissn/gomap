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
		newRootID, err := z.allocator.Alloc()
		if err != nil {
			return 0, nil, err
		}

		buf := z.getPooledBuf()
		defer z.putPooledBuf(buf)
		data := *buf

		// Use Builder for new root
		builder := node.NewBuilder(data, page.PageTypeInternal)
		builder.SetPageID(newRootID)

		// 1. Add first child
		if err := builder.AddInternalChild([]byte{}, newRoot); err != nil {
			return 0, nil, err
		}

		// 2. Add splits
		for _, s := range splits {
			if err := builder.AddInternalChild(s.Key, s.NodeID); err != nil {
				return 0, nil, errors.New("root split overflow: batch too large for single level growth")
			}
		}

		n := builder.Finish()
		if err := z.pager.Write(newRootID, n.Data()); err != nil {
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
	
	// Zero-Copy Read: Use Get instead of ReadPage
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		z.putPooledBuf(buf)
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
		nr, splits, err := z.mergeLeaf(oldNode, builder, keys, ops)
		if err == nil {
			n := builder.Finish() // Finish writes header/checksum
			if werr := z.pager.Write(newPageID, n.Data()); werr != nil {
				z.putPooledBuf(buf)
				return 0, nil, nil, werr
			}
		}
		z.putPooledBuf(buf)
		return nr, splits, retired, err
	} else if oldNode.Type() == page.PageTypeInternal {
		// Internal merge
		nr, splits, childRetired, err := z.mergeInternal(oldNode, builder, keys, ops)
		if err != nil {
			z.putPooledBuf(buf)
			return 0, nil, nil, err
		}

		n := builder.Finish()
		if err := z.pager.Write(newPageID, n.Data()); err != nil {
			z.putPooledBuf(buf)
			return 0, nil, nil, err
		}
		z.putPooledBuf(buf)

		retired = append(retired, childRetired...)
		return nr, splits, retired, nil
	} else {
		// Handle Page 0 / Empty / New Tree case
		if oldNode.Type() == 0 {
			// Reuse builder, set type
			builder = node.NewBuilder(newData, page.PageTypeLeaf)
			builder.SetPageID(newPageID)
			
			nr, splits, err := z.mergeLeaf(oldNode, builder, keys, ops)
			if err == nil {
				n := builder.Finish()
				if werr := z.pager.Write(newPageID, n.Data()); werr != nil {
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

func (z *Zipper) mergeLeaf(oldNode *node.Node, builder *node.Builder, keys []string, ops map[string]batch.Entry) (uint64, []Split, error) {
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

	// Current target builder
	target := builder

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
			// Optimization: GetLeafEntryView (Zero Copy)
			k, _, _, _, err := oldNode.GetLeafEntryView(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			batchKey := []byte(keys[keyIdx])

			cmp := bytes.Compare(k, batchKey)
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
			// But wait, if target != builder, we need to write it to disk now.
			if target != builder {
				n := target.Finish()
				if err := z.pager.Write(target.PageID(), n.Data()); err != nil {
					return 0, nil, err
				}
			}
			
			// 2. Allocate NEW split node
			sid, err := z.allocator.Alloc()
			if err != nil {
				return 0, nil, err
			}
			
			buf := z.getPooledBuf()
			splitBufs = append(splitBufs, buf) // Track for cleanup
			sdata := *buf
			
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
		if err := z.pager.Write(target.PageID(), n.Data()); err != nil {
			return 0, nil, err
		}
	}
	
	// 'builder' is finalized by caller.

	return builder.PageID(), splits, nil
}

func (z *Zipper) mergeInternal(oldNode *node.Node, builder *node.Builder, keys []string, ops map[string]batch.Entry) (uint64, []Split, []uint64, error) {
	count := oldNode.Count()
	
	var splits []Split
	var splitBufs []*[]byte
	defer func() {
		for _, b := range splitBufs {
			z.putPooledBuf(b)
		}
	}()
	
	target := builder
	var retired []uint64

	keyIdx := 0

	for i := uint16(0); i < count; i++ {
		// Optimization: View
		// But InternalEntry doesn't have a View method yet?
		// Check node/internal.go. Yes, GetInternalEntry does copy.
		// GetInternalChildID and SearchInternal use offsets.
		// Let's use GetInternalEntry for now, can optimize later.
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

		// Add (Key, NewChildID) to target builder
		if newChildID >= z.pager.PageCount() {
			return 0, nil, nil, errors.New("zipper: detected OOB child ID")
		}
		
		err = target.AddInternalChild(entry.Key, newChildID)
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, &splitBufs, entry.Key, newChildID)
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
				target, err = z.createNewSplitInternal(target, builder, &splits, &splitBufs, s.Key, s.NodeID)
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
		n := target.Finish()
		if err := z.pager.Write(target.PageID(), n.Data()); err != nil {
			return 0, nil, nil, err
		}
	}

	// builder finalized by caller.
	return builder.PageID(), splits, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, rootBuilder *node.Builder, splits *[]Split, splitBufs *[]*[]byte, key []byte, val uint64) (*node.Builder, error) {
	// 1. Finish current (if not rootBuilder)
	if currentTarget != rootBuilder {
		n := currentTarget.Finish()
		if err := z.pager.Write(currentTarget.PageID(), n.Data()); err != nil {
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
	
	sb := node.NewBuilder(sdata, page.PageTypeInternal)
	sb.SetPageID(sid)
	
	*splits = append(*splits, Split{Key: append([]byte(nil), key...), NodeID: sid})
	
	// Retry insert
	if err := sb.AddInternalChild(key, val); err != nil {
		return nil, err
	}
	
	return sb, nil
}
