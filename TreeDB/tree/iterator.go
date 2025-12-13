package tree

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap-gemini/TreeDB/node"
	"github.com/snissn/gomap-gemini/TreeDB/page"
)

type CursorItem struct {
	PageID uint64
	Node   *node.Node
	Index  int
}

type Iterator struct {
	tree     *Tree
	stack    []CursorItem
	start    []byte
	end      []byte
	valid    bool
	err      error
	currKey  []byte
	currVal  []byte
	reverse  bool
}

func (t *Tree) Iterator(start, end []byte) *Iterator {
	it := &Iterator{
		tree:  t,
		start: start,
		end:   end,
		reverse: false,
	}
	it.seek(start)
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) *Iterator {
	it := &Iterator{
		tree:    t,
		start:   start,
		end:     end,
		reverse: true,
	}
	// Reverse seek: Find >= end, then step back.
	if end == nil {
		it.seekRightMost()
	} else {
		it.seek(end)
		if it.valid {
			// Found key >= end. Move back 1.
			it.stepBackward()
		} else {
			// seek(end) fell off the end (all keys < end).
			// This means the last key is < end.
			// We should seek right-most?
			// Wait, seek(end) returns "exhausted" if all keys < end?
			// My seek implementation just drills down.
			// If `SearchLeaf` returns count (not found), we append it.
			// Then `loadCurrent` sees `Index >= Count`, and calls `advance`.
			// `advance` pops.
			// So `seek(end)` ends up invalid if `end` > all.
			
			// We need to distinguish "invalid because empty" vs "invalid because passed end".
			// If `seek(end)` is invalid, it means `end` is beyond last key.
			// So we should start at the very end of the tree.
			it.err = nil // Reset error if any?
			it.seekRightMost()
		}
	}
	
	// Check Start bound
	if it.valid && it.start != nil && bytes.Compare(it.currKey, it.start) < 0 {
		it.valid = false
	}
	
	return it
}

func (it *Iterator) seekRightMost() {
	it.stack = nil
	it.valid = false
	currID := it.tree.rootPageID
	
	for {
		data, err := it.tree.pager.Get(currID)
		if err != nil {
			it.err = err
			return
		}
		n := node.NewNode(data)
		
		if n.Count() == 0 {
			it.valid = false
			return
		}
		
		index := int(n.Count() - 1)
		it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: index})
		
		if n.Type() == page.PageTypeLeaf {
			it.loadCurrent()
			return
		} else if n.Type() == page.PageTypeInternal {
			entry, err := n.GetInternalEntry(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currID = entry.ChildPageID
		} else {
			it.err = page.ErrInvalidPageType
			return
		}
	}
}

func (it *Iterator) seek(key []byte) {
	it.stack = nil
	it.valid = false
	it.currKey = nil
	it.currVal = nil
	
	// If key is nil, start from beginning (Left-most)
	// Drill down
	currID := it.tree.rootPageID
	
	// To handle "nil key" as "start of tree", we treat nil as empty bytes for comparison?
	// But empty bytes is valid key.
	// If key is nil, we just follow index 0 everywhere.
	
	for {
		data, err := it.tree.pager.Get(currID)
		if err != nil {
			it.err = err
			return
		}
		
		n := node.NewNode(data)
		if !n.VerifyChecksum() {
			it.err = errors.New("checksum mismatch")
			return
		}
		
		var index int
		if n.Type() == page.PageTypeInternal {
			if key == nil {
				index = 0
			} else {
				idx, _ := n.SearchInternal(key)
				index = int(idx)
			}
			
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: index})
			
			entry, err := n.GetInternalEntry(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currID = entry.ChildPageID
			
		} else if n.Type() == page.PageTypeLeaf {
			if key == nil {
				index = 0
			} else {
				idx, found := n.SearchLeaf(key)
				_ = found // We just want position >= key
				index = int(idx)
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: index})
			
			// Load current item if valid index
			it.loadCurrent()
			return
		} else {
			it.err = page.ErrInvalidPageType
			return
		}
	}
}

func (it *Iterator) loadCurrent() {
	if len(it.stack) == 0 {
		it.valid = false
		return
	}
	
	top := it.stack[len(it.stack)-1]
	
	// Check Bounds
	if top.Index < 0 {
		// Exhausted backward
		it.stepBackward()
		return
	}
	if top.Index >= int(top.Node.Count()) {
		// Exhausted forward
		it.stepForward()
		return
	}
	
	entry, err := top.Node.GetLeafEntry(uint16(top.Index))
	if err != nil {
		it.err = err
		it.valid = false
		return
	}
	
	// Check Range Limits
	if !it.reverse {
		if it.end != nil && bytes.Compare(entry.Key, it.end) >= 0 {
			it.valid = false
			return
		}
	} else {
		if it.start != nil && bytes.Compare(entry.Key, it.start) < 0 {
			it.valid = false
			return
		}
	}
	
	// Check Tombstone
	if entry.Flags & node.FlagTombstone != 0 {
		// Skip
		if it.reverse {
			it.stack[len(it.stack)-1].Index--
		} else {
			it.stack[len(it.stack)-1].Index++
		}
		it.loadCurrent() // Recurse/Loop
		return
	}
	
	it.currKey = entry.Key
	
	if entry.Flags & node.FlagPointer != 0 {
		val, err := it.tree.slabManager.Read(entry.ValuePtr)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		it.currVal = val
	} else {
		it.currVal = entry.Value
	}
	
	it.valid = true
}

func (it *Iterator) stepForward() {
	// Pop exhausted nodes
	for len(it.stack) > 0 {
		idx := len(it.stack) - 1
		top := it.stack[idx]
		
		// Move next
		top.Index++
		it.stack[idx] = top
		
		if top.Index < int(top.Node.Count()) {
			// Found valid branch/item
			if top.Node.Type() == page.PageTypeLeaf {
				it.loadCurrent()
				return
			}
			
			// Internal: Drill down left-most from here
			entry, err := top.Node.GetInternalEntry(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := entry.ChildPageID
			
			// Drill down loop
			for {
				data, err := it.tree.pager.Get(currID)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				n := node.NewNode(data)
				
				item := CursorItem{PageID: currID, Node: n, Index: 0}
				it.stack = append(it.stack, item)
				
				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}
				
				// Internal -> 0
				e, err := n.GetInternalEntry(0)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currID = e.ChildPageID
			}
		}
		
		// If exhausted, pop and continue loop (back up)
		it.stack = it.stack[:idx]
	}
	
	// Stack empty -> EOF
	it.valid = false
}

func (it *Iterator) stepBackward() {
	for len(it.stack) > 0 {
		idx := len(it.stack) - 1
		top := it.stack[idx]
		
		top.Index--
		it.stack[idx] = top
		
		if top.Index >= 0 {
			if top.Node.Type() == page.PageTypeLeaf {
				it.loadCurrent()
				return
			}
			
			// Internal: Drill down right-most
			entry, err := top.Node.GetInternalEntry(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := entry.ChildPageID
			
			for {
				data, err := it.tree.pager.Get(currID)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				n := node.NewNode(data)
				
				if n.Count() == 0 { // Should not happen in valid B+Tree
					it.valid = false
					return
				}
				
				item := CursorItem{PageID: currID, Node: n, Index: int(n.Count() - 1)}
				it.stack = append(it.stack, item)
				
				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}
				
				e, err := n.GetInternalEntry(uint16(n.Count() - 1))
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currID = e.ChildPageID
			}
		}
		
		it.stack = it.stack[:idx]
	}
	it.valid = false
}

func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	// Move index of top
	if len(it.stack) > 0 {
		if it.reverse {
			it.stack[len(it.stack)-1].Index--
		} else {
			it.stack[len(it.stack)-1].Index++
		}
		it.loadCurrent() 
	}
}


func (it *Iterator) Valid() bool {
	return it.valid && it.err == nil
}

func (it *Iterator) Error() error {
	return it.err
}

func (it *Iterator) Key() []byte {
	// Return copy
	if it.currKey == nil {
		return nil
	}
	k := make([]byte, len(it.currKey))
	copy(k, it.currKey)
	return k
}

func (it *Iterator) Value() []byte {
	if it.currVal == nil {
		return nil
	}
	v := make([]byte, len(it.currVal))
	copy(v, it.currVal)
	return v
}

func (it *Iterator) Close() error {
	it.stack = nil
	return nil
}

func (it *Iterator) Domain() (start, end []byte) {
	return it.start, it.end
}
