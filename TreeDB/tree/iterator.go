package tree

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap-gemini/TreeDB/internal/iterator"
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

func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator {
	it := &Iterator{
		tree:  t,
		start: start,
		end:   end,
		reverse: false,
	}
	it.Seek(start)
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) iterator.UnsafeIterator {
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
		} else {
			// seek(end) fell off the end.
			it.err = nil 
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
		data, err := it.tree.pager.ReadPage(currID)
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

	currID := it.tree.rootPageID

	for {
		data, err := it.tree.pager.ReadPage(currID)
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
		if it.reverse {
			it.stack[len(it.stack)-1].Index--
		} else {
			it.stack[len(it.stack)-1].Index++
		}
		it.loadCurrent() 
		return
	}

	it.currKey = entry.Key

	// Lazy Load Logic could go here, but for now eagerly loading as before
	// to match structure. UnsafeValue will return it.
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

			entry, err := top.Node.GetInternalEntry(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := entry.ChildPageID

			for {
				data, err := it.tree.pager.ReadPage(currID)
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

				e, err := n.GetInternalEntry(0)
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

			entry, err := top.Node.GetInternalEntry(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currID := entry.ChildPageID

			for {
				data, err := it.tree.pager.ReadPage(currID)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				n := node.NewNode(data)

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
	if it.currVal == nil {
		return nil
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