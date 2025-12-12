package zipper

import (
	"bytes"
	"errors"
	"sort"

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
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{pager: p, allocator: a}
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

	newRoot, splitKey, splitNode, retired, err := z.writeRecursive(rootID, keys, ops)
	if err != nil {
		return 0, nil, err
	}

	if splitNode != 0 {
		// Root split! Create new internal root.
		newRootID, err := z.allocator.Alloc()
		if err != nil {
			return 0, nil, err
		}
		
		data, err := z.pager.Get(newRootID)
		if err != nil {
			return 0, nil, err
		}
		
		n := node.NewNode(data)
		n.SetPageID(newRootID)
		n.SetType(page.PageTypeInternal)
		n.SetCount(0)
		
		if err := n.AddInternalChild([]byte{}, newRoot); err != nil {
			return 0, nil, err
		}
		
		if err := n.AddInternalChild(splitKey, splitNode); err != nil {
			return 0, nil, err
		}
		
		n.UpdateChecksum()
		return newRootID, retired, nil
	}

	return newRoot, retired, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splitKey (if split), splitPageID (if split), retiredPages, error.
func (z *Zipper) writeRecursive(pageID uint64, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, []uint64, error) {
	// 1. Allocate New Page (COW)
	newPageID, err := z.allocator.Alloc()
	if err != nil {
		return 0, nil, 0, nil, err
	}
	
	newData, err := z.pager.Get(newPageID)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	
	newNode := node.NewNode(newData)
	newNode.SetPageID(newPageID) // Set ID first
	
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		return 0, nil, 0, nil, err
	}
	oldNode := node.NewNode(oldData)
	
	// Track retired page (the old one we are copying from)
	var retired []uint64
	if pageID != 0 {
		retired = append(retired, pageID)
	}

	// Copy Header & Type
	newNode.SetType(oldNode.Type())
	
	if oldNode.Type() == page.PageTypeLeaf {
		// Merge Leaf returns NO additional retired pages (leaves don't have children to retire)
		// Wait, mergeLeaf signature needs update to match expected return type or handle it here?
		// mergeLeaf only returns split info.
		// So we return `retired` constructed here.
		nr, sk, sn, err := z.mergeLeaf(oldNode, newNode, keys, ops)
		return nr, sk, sn, retired, err
	} else if oldNode.Type() == page.PageTypeInternal {
		// Internal merge might return MORE retired pages from children recursion
		nr, sk, sn, childRetired, err := z.mergeInternal(oldNode, newNode, keys, ops)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		retired = append(retired, childRetired...)
		return nr, sk, sn, retired, nil
	} else {
		if oldNode.Type() == 0 {
			newNode.SetType(page.PageTypeLeaf)
			nr, sk, sn, err := z.mergeLeaf(oldNode, newNode, keys, ops)
			return nr, sk, sn, retired, err
		}
		return 0, nil, 0, nil, page.ErrInvalidPageType
	}
}

func (z *Zipper) mergeLeaf(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, error) {
	// ... (content unchanged, just signature matches call in writeRecursive which discards retired? No, mergeLeaf doesn't return retired)
	// I'll keep mergeLeaf signature as is, and writeRecursive wraps it.
	
	// Iterate old keys and batch keys in order
	// Merge into newNode.
	// If newNode fills, split.
	
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	keyIdx := 0
	
	var splitNode *node.Node
	var splitNodeID uint64
	var splitKey []byte
	
	target := newNode
	
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
				return 0, nil, 0, err
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
				return 0, nil, 0, err
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
			if splitNode == nil {
				// Allocate split node
				sid, err := z.allocator.Alloc()
				if err != nil {
					return 0, nil, 0, err
				}
				splitNodeID = sid
				sdata, err := z.pager.Get(sid)
				if err != nil {
					return 0, nil, 0, err
				}
				splitNode = node.NewNode(sdata)
				splitNode.SetPageID(sid)
				splitNode.SetType(page.PageTypeLeaf)
				
				// Set split key (first key of new node)
				splitKey = key
				target = splitNode
				
				// Retry insert
				err = target.AddLeafEntry(key, val, flags, valPtr)
				if err != nil {
					return 0, nil, 0, err // If split node is somehow full immediately (huge key?)
				}
			} else {
				return 0, nil, 0, errors.New("node overflow even after split")
			}
		} else if err != nil {
			return 0, nil, 0, err
		}
	}
	
	newNode.UpdateChecksum()
	if splitNode != nil {
		splitNode.UpdateChecksum()
	}
	
	return newNode.PageID(), splitKey, splitNodeID, nil
}

func (z *Zipper) mergeInternal(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, []uint64, error) {
	// Iterate children.
	// For each child, determine which batch keys belong to it.
	
	count := oldNode.Count()
	var splitNode *node.Node
	var splitNodeID uint64
	var splitKey []byte
	target := newNode
	var retired []uint64
	
	keyIdx := 0
	
	for i := uint16(0); i < count; i++ {
		entry, err := oldNode.GetInternalEntry(i)
		if err != nil {
			return 0, nil, 0, nil, err
		}
		
		// Determine End Key for this child
		var endKey []byte
		if i+1 < count {
			nextEntry, err := oldNode.GetInternalEntry(i+1)
			if err != nil {
				return 0, nil, 0, nil, err
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
		var childSplitKey []byte
		var childSplitID uint64
		
		if len(childKeys) > 0 {
			// Recurse
			ncID, sk, sID, childRet, err := z.writeRecursive(entry.ChildPageID, childKeys, ops)
			if err != nil {
				return 0, nil, 0, nil, err
			}
			newChildID = ncID
			childSplitKey = sk
			childSplitID = sID
			retired = append(retired, childRet...)
		} else {
			// Reuse
			newChildID = entry.ChildPageID
		}
		
		// Add (Key, NewChildID) to target
		if newChildID >= z.pager.PageCount() {
			return 0, nil, 0, nil, errors.New("zipper: detected OOB child ID")
		}
		err = target.AddInternalChild(entry.Key, newChildID)
		if z.handleFullInternal(target, &splitNode, &splitNodeID, &splitKey, entry.Key, newChildID, err) != nil {
             return 0, nil, 0, nil, err
        }
        
        // If child split, add sibling
        if childSplitID != 0 {
             err = target.AddInternalChild(childSplitKey, childSplitID)
             if z.handleFullInternal(target, &splitNode, &splitNodeID, &splitKey, childSplitKey, childSplitID, err) != nil {
                  return 0, nil, 0, nil, err
             }
        }
        
        // Update target ref if we switched
        if splitNode != nil {
            target = splitNode
        }
	}
	
	newNode.UpdateChecksum()
	if splitNode != nil {
		splitNode.UpdateChecksum()
	}
	
	return newNode.PageID(), splitKey, splitNodeID, retired, nil
}

func (z *Zipper) handleFullInternal(target *node.Node, splitNode **node.Node, splitNodeID *uint64, splitKey *[]byte, key []byte, val uint64, err error) error {
    if err == node.ErrNodeFull {
        if *splitNode == nil {
            sid, err := z.allocator.Alloc()
            if err != nil {
                return err
            }
            *splitNodeID = sid
            sdata, err := z.pager.Get(sid)
            if err != nil {
                return err
            }
            sn := node.NewNode(sdata)
            sn.SetPageID(sid)
            sn.SetType(page.PageTypeInternal)
            *splitNode = sn
            
            *splitKey = key // Separation key
            
            // Retry on new node
            return (*splitNode).AddInternalChild(key, val)
        } else {
             return errors.New("internal node overflow")
        }
    }
    return err
}
