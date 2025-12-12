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

type Zipper struct {
	pager *pager.Pager
}

func New(p *pager.Pager) *Zipper {
	return &Zipper{pager: p}
}

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (uint64, error) {
	ops := b.Ops()
	keys := make([]string, 0, len(ops))
	for k := range ops {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newRoot, splitKey, splitNode, err := z.writeRecursive(rootID, keys, ops)
	if err != nil {
		return 0, err
	}

	if splitNode != 0 {
		// Root split! Create new internal root.
		newRootID, err := z.pager.Alloc(1)
		if err != nil {
			return 0, err
		}
		
		data, err := z.pager.Get(newRootID)
		if err != nil {
			return 0, err
		}
		
		n := node.NewNode(data)
		n.SetPageID(newRootID)
		n.SetType(page.PageTypeInternal)
		n.SetCount(0)
		
		// Add left child (old root) - Key is implicit?
		// Internal Node Structure:
		// Entry 0: Key X, Child Y. -> Keys < X go to Y?
		// Wait, my Internal Node implementation (SearchInternal):
		// "Find largest i such that Entry[i].Key <= Key".
		// So Entry[i] covers range [Entry[i].Key, Entry[i+1].Key).
		// Left-most child usually covers (-inf, Entry[0].Key).
		// But in my implementation (TestInternalNode), I had:
		// Entry("10", 100). Search("05") -> 0.
		// This implies Entry 0 is the "Left-most" but it has a key "10"?
		// No, `SearchInternal` returned 0 because `10 <= 5` is false.
		// It returned 0 (default).
		// If I add a "dummy" minimal key? Or handle 0 explicitly?
		// "Internal Node: Directory (Top): Array of Offsets... Entry: [ChildID] [Key]"
		// My `AddInternalChild` adds `Key` and `ChildID`.
		
		// Standard B+Tree Internal Node:
		// Keys: K1, K2, ... Kn
		// Pointers: P0, P1, ... Pn
		// P0 covers < K1.
		// P1 covers [K1, K2).
		// ...
		// Pn covers >= Kn.
		
		// My Node Layout (Slotted Page):
		// Just entries.
		// If I use the "First Key" strategy:
		// Entry i: (Key_i, P_i).
		// P_i covers keys >= Key_i.
		// Then we need a P_0 with Key_0 = -Inf (or smallest possible).
		// My `SearchInternal` finds largest Key_i <= SearchKey.
		// So if I have (Key=10, P=1), (Key=20, P=2).
		// Query 15: 10 <= 15 (True). 20 <= 15 (False). Index 0 -> P=1.
		// So P=1 covers [10, 20).
		// What about Query 5? Returns 0 (Index 0). P=1.
		// So P=1 covers (-inf, 20)? No, "Key=10".
		// My logic returns index 0 if nothing matches?
		// `if i > 0 { return i-1 } return 0`.
		// So yes, it falls back to 0.
		// So Entry 0 covers (-inf, Key_1).
		// And Entry 0's Key is effectively ignored for lower bound?
		// Usually Entry 0's Key IS the lower bound of that page, but for routing from parent, parent uses it.
		// Inside the node, we just need to know which child to pick.
		
		// So, when Root Splits:
		// We have Left Node (newRoot) and Right Node (splitNode).
		// Right Node starts at `splitKey`.
		// We need to add:
		// 1. Left Node (covers -inf). Key? Maybe empty or copy first key?
		//    Let's use Empty Key "" for first child?
		// 2. Right Node (covers >= splitKey). Key = splitKey.
		
		// Add Left Child (newRoot)
		// We use Key=""? Or "00"?
		// `AddInternalChild` puts key in slot.
		// Let's use empty slice for "min key"?
		if err := n.AddInternalChild([]byte{}, newRoot); err != nil {
			return 0, err
		}
		
		// Add Right Child (splitNode)
		if err := n.AddInternalChild(splitKey, splitNode); err != nil {
			return 0, err
		}
		
		n.UpdateChecksum()
		return newRootID, nil
	}

	return newRoot, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splitKey (if split), splitPageID (if split), error.
func (z *Zipper) writeRecursive(pageID uint64, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, error) {
	// 1. Allocate New Page (COW)
	newPageID, err := z.pager.Alloc(1)
	if err != nil {
		return 0, nil, 0, err
	}
	
	newData, err := z.pager.Get(newPageID)
	if err != nil {
		return 0, nil, 0, err
	}
	
	newNode := node.NewNode(newData)
	newNode.SetPageID(newPageID) // Set ID first
	
	// Handle Empty Tree (pageID 0 might be uninitialized if fresh?)
	// If pageID == 0 (and we are root), we might be creating first leaf.
	// But `Apply` passes `rootID`. If `rootID` is valid, we read it.
	// If `rootID` points to empty/newly alloc page?
	// Let's assume `pageID` is valid.
	
	oldData, err := z.pager.Get(pageID)
	if err != nil {
		return 0, nil, 0, err
	}
	oldNode := node.NewNode(oldData)
	
	// Copy Header & Type
	newNode.SetType(oldNode.Type())
	// Count will be adjusted as we fill.
	// We DO NOT just copy raw bytes because we are merging.
	// We rebuild the node.
	
	if oldNode.Type() == page.PageTypeLeaf {
		return z.mergeLeaf(oldNode, newNode, keys, ops)
	} else if oldNode.Type() == page.PageTypeInternal {
		return z.mergeInternal(oldNode, newNode, keys, ops)
	} else {
		// Maybe unitialized root?
		// If Type is 0, assume Leaf?
		if oldNode.Type() == 0 {
			newNode.SetType(page.PageTypeLeaf)
			return z.mergeLeaf(oldNode, newNode, keys, ops)
		}
		return 0, nil, 0, page.ErrInvalidPageType // Need to define or import
	}
}

func (z *Zipper) mergeLeaf(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, error) {
	// Iterate old keys and batch keys in order
	// Merge into newNode.
	// If newNode fills, split.
	
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	keyIdx := 0
	
	// Helper to append and check split
	// We defer split handling? No, we must handle it as we go or at end?
	// Easiest: Fill newNode. If full, allocate splitNode and move half.
	// But "Node" struct doesn't support "Overflow".
	// We check `FreeSpace` before adding.
	// If full, we switch to `splitNode`.
	
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
				sid, err := z.pager.Alloc(1)
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
				// Split node also full?
				// Triple split? Not supported in this simplified logic.
				// Fail or support overflow pages.
				// For Phase 3, error.
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

func (z *Zipper) mergeInternal(oldNode, newNode *node.Node, keys []string, ops map[string]batch.Entry) (uint64, []byte, uint64, error) {
	// Iterate children.
	// For each child, determine which batch keys belong to it.
	// Range: [Entry[i].Key, Entry[i+1].Key)
	
	count := oldNode.Count()
	var splitNode *node.Node
	var splitNodeID uint64
	var splitKey []byte
	target := newNode
	
	keyIdx := 0
	
	for i := uint16(0); i < count; i++ {
		entry, err := oldNode.GetInternalEntry(i)
		if err != nil {
			return 0, nil, 0, err
		}
		
		// Determine End Key for this child
		var endKey []byte
		if i+1 < count {
			nextEntry, err := oldNode.GetInternalEntry(i+1)
			if err != nil {
				return 0, nil, 0, err
			}
			endKey = nextEntry.Key
		}
		// If endKey is nil, it means "to infinity" (last child)
		
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
		
		// If no keys for this child, just copy the pointer?
		// No, standard COW requires us to copy the path?
		// Actually, if a child is NOT modified, we can point to the OLD pageID!
		// Optimization: If childKeys is empty, just reuse Entry.ChildPageID.
		
		var newChildID uint64
		var childSplitKey []byte
		var childSplitID uint64
		
		if len(childKeys) > 0 {
			// Recurse
			ncID, sk, sID, err := z.writeRecursive(entry.ChildPageID, childKeys, ops)
			if err != nil {
				return 0, nil, 0, err
			}
			newChildID = ncID
			childSplitKey = sk
			childSplitID = sID
		} else {
			// Reuse
			newChildID = entry.ChildPageID
		}
		
		// Add (Key, NewChildID) to target
		// Note: Entry.Key is the separator.
		err = target.AddInternalChild(entry.Key, newChildID)
		if z.handleFullInternal(target, &splitNode, &splitNodeID, &splitKey, entry.Key, newChildID, err) != nil {
             return 0, nil, 0, err
        }
        
        // If child split, add sibling
        if childSplitID != 0 {
             err = target.AddInternalChild(childSplitKey, childSplitID)
             if z.handleFullInternal(target, &splitNode, &splitNodeID, &splitKey, childSplitKey, childSplitID, err) != nil {
                  return 0, nil, 0, err
             }
        }
        
        // Update target ref if we switched
        if splitNode != nil {
            target = splitNode
        }
	}
	
	// Check if any keys remain (should not if logic is correct, as last child covers infinity)
	// But if they are > all existing ranges?
	// The last child should have captured them.
	
	newNode.UpdateChecksum()
	if splitNode != nil {
		splitNode.UpdateChecksum()
	}
	
	return newNode.PageID(), splitKey, splitNodeID, nil
}

func (z *Zipper) handleFullInternal(target *node.Node, splitNode **node.Node, splitNodeID *uint64, splitKey *[]byte, key []byte, val uint64, err error) error {
    if err == node.ErrNodeFull {
        if *splitNode == nil {
            sid, err := z.pager.Alloc(1)
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
