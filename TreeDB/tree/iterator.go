package tree

import (
	"bytes"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type CursorItem struct {
	PageID uint64
	Node   node.Node
	Index  int
}

type Iterator struct {
	tree    *Tree
	stack   []CursorItem
	start   []byte
	end     []byte
	valid   bool
	err     error
	currKey []byte
	currVal []byte
	currPtr page.ValuePtr
	flags   byte
	valOK   bool
	reverse bool
}

func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator {
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil} // Invalid immediately
	}
	it := &Iterator{
		tree:    t,
		start:   start,
		end:     end,
		reverse: false,
	}
	it.Seek(start)
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) iterator.UnsafeIterator {
	if start != nil && end != nil && bytes.Compare(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil}
	}
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
		it.Seek(end)
		if it.valid {
			it.stepBackward()
		} else if it.err == nil {
			// seek(end) fell off the end (no error, just exhausted).
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
	it.err = nil
	currID := it.tree.rootPageID

	for {
		n, err := it.loadNode(currID)
		if err != nil {
			it.err = err
			return
		}

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
			childID, err := n.GetInternalChildID(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currID = childID
		} else {
			it.err = page.ErrInvalidPageType
			return
		}
	}
}

func (it *Iterator) seek(key []byte) {
	it.stack = nil
	it.valid = false
	it.err = nil
	it.currKey = nil
	it.currVal = nil
	it.currPtr = page.ValuePtr{}
	it.flags = 0
	it.valOK = false

	currID := it.tree.rootPageID

	for {
		n, err := it.loadNode(currID)
		if err != nil {
			it.err = err
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

			childID, err := n.GetInternalChildID(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currID = childID

		} else if n.Type() == page.PageTypeLeaf {
			if key == nil {
				index = 0
			} else {
				idx, found := n.SearchLeaf(key)
				_ = found
				index = int(idx)
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: index})

			it.loadCurrent()
			return
		} else {
			it.err = page.ErrInvalidPageType
			return
		}
	}
}

func (it *Iterator) loadCurrent() {
	for {
		if len(it.stack) == 0 {
			it.valid = false
			return
		}

		top := it.stack[len(it.stack)-1]

		// Check Bounds
		if top.Index < 0 {
			it.stepBackward()
			return
		}
		if top.Index >= int(top.Node.Count()) {
			it.stepForward()
			return
		}

		keyView, valView, valPtr, flags, err := top.Node.GetLeafEntryView(uint16(top.Index))
		if err != nil {
			it.err = err
			it.valid = false
			return
		}

		// Check Range Limits
		if !it.reverse {
			if it.end != nil && bytes.Compare(keyView, it.end) >= 0 {
				it.valid = false
				return
			}
		} else {
			if it.start != nil && bytes.Compare(keyView, it.start) < 0 {
				it.valid = false
				return
			}
		}

		// Skip tombstones (TreeDB does not currently persist them to disk, but
		// iterator supports the flag for completeness).
		if flags&node.FlagTombstone != 0 {
			if it.reverse {
				it.stack[len(it.stack)-1].Index--
			} else {
				it.stack[len(it.stack)-1].Index++
			}
			continue
		}

		it.currKey = keyView
		it.flags = flags
		it.currPtr = valPtr

		// Inline values are a view into the mmap. Pointer values are loaded on
		// demand in UnsafeValue/Value.
		if flags&node.FlagPointer != 0 {
			it.currVal = nil
			it.valOK = false
		} else {
			it.currVal = valView
			it.valOK = true
		}

		it.valid = true
		return
	}
}

func (it *Iterator) stepForward() {
	for len(it.stack) > 0 {
		idx := len(it.stack) - 1
		top := it.stack[idx]

		top.Index++
		it.stack[idx] = top

		if top.Index < int(top.Node.Count()) {
			if top.Node.Type() == page.PageTypeLeaf {
				it.loadCurrent()
				return
			}

			childID, err := top.Node.GetInternalChildID(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := childID

			for {
				n, err := it.loadNode(currID)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}

				item := CursorItem{PageID: currID, Node: n, Index: 0}
				it.stack = append(it.stack, item)

				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}

				childID, err := n.GetInternalChildID(0)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currID = childID
			}
		}
		it.stack = it.stack[:idx]
	}
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

			childID, err := top.Node.GetInternalChildID(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := childID

			for {
				n, err := it.loadNode(currID)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}

				if n.Count() == 0 {
					it.valid = false
					return
				}

				item := CursorItem{PageID: currID, Node: n, Index: int(n.Count() - 1)}
				it.stack = append(it.stack, item)

				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}

				childID, err := n.GetInternalChildID(uint16(n.Count() - 1))
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currID = childID
			}
		}
		it.stack = it.stack[:idx]
	}
	it.valid = false
}

func (it *Iterator) Next() {
	if !it.valid {
		panic("iterator invalid")
	}
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

func (it *Iterator) UnsafeKey() []byte {
	if it.currKey == nil {
		return nil
	}
	return it.currKey
}

func (it *Iterator) UnsafeValue() []byte {
	if it.currKey == nil {
		return nil
	}
	if it.flags&node.FlagPointer != 0 {
		if it.valOK {
			return it.currVal
		}
		val, err := it.tree.slabReader.Read(it.currPtr)
		if err != nil {
			it.err = err
			it.valid = false
			return nil
		}
		it.currVal = val
		it.valOK = true
	}
	return it.currVal
}

func (it *Iterator) Key() []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	ck := make([]byte, len(k))
	copy(ck, k)
	return ck
}

func (it *Iterator) Value() []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	cv := make([]byte, len(v))
	copy(cv, v)
	return cv
}

func (it *Iterator) Close() error {
	it.stack = nil
	return nil
}

func (it *Iterator) Seek(key []byte) {
	it.seek(key)
	// Check bounds? Handled in loadCurrent
}

func (it *Iterator) IsDeleted() bool {
	return false
}

func (it *Iterator) Domain() (start, end []byte) {
	return it.start, it.end
}

func (it *Iterator) loadNode(pageID uint64) (node.Node, error) {
	// Use Get (mmap) instead of ReadPage (copy).
	data, err := it.tree.pager.Get(pageID)
	if err != nil {
		return node.Node{}, err
	}
	n := node.NewNodeView(data)

	// Skip checksum verification for pages we've already verified.
	if !it.tree.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return node.Node{}, fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		it.tree.pager.MarkVerified(pageID)
	}

	return n, nil
}
