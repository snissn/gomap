package tree

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type CursorItem struct {
	PageID uint64
	Node   node.Node
	Index  int
}

var iteratorPool = sync.Pool{
	New: func() any {
		return &Iterator{}
	},
}

// IteratorMode controls value materialization behavior while scanning.
type IteratorMode uint8

const (
	// IteratorModeFull resolves inline and pointer-backed values on demand.
	IteratorModeFull IteratorMode = iota
	// IteratorModeKeysOnly skips value materialization entirely.
	IteratorModeKeysOnly
	// IteratorModePointerProjection keeps pointer metadata visible via UnsafeEntry
	// but skips pointer payload decoding.
	IteratorModePointerProjection
)

// IteratorOptions configures scan-time value materialization behavior.
type IteratorOptions struct {
	Mode IteratorMode
	// IncludeTombstones keeps tombstone rows visible to iteration callers.
	// Default false preserves user-facing behavior (tombstones hidden).
	IncludeTombstones bool
}

func normalizeIteratorMode(mode IteratorMode) IteratorMode {
	switch mode {
	case IteratorModeKeysOnly, IteratorModePointerProjection:
		return mode
	default:
		return IteratorModeFull
	}
}

type Iterator struct {
	tree             *Tree
	stack            []CursorItem
	stackBuf         [16]CursorItem
	start            []byte
	end              []byte
	valid            bool
	err              error
	mode             IteratorMode
	includeTombstones bool
	reverse          bool

	currKey []byte
	leafVal []byte
	leafPtr page.ValuePtr
	leafFlags byte

	valOK   bool
	currVal []byte
}

func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator {
	return t.IteratorWithOptions(start, end, IteratorOptions{})
}

// IteratorWithOptions returns a forward iterator over [start, end).
func (t *Tree) IteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	mode := normalizeIteratorMode(opts.Mode)
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		it := t.acquireIterator(nil, nil, mode, opts.IncludeTombstones, false)
		it.valid = false
		return it
	}
	it := t.acquireIterator(start, end, mode, opts.IncludeTombstones, false)
	it.Seek(start)
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return t.ReverseIteratorWithOptions(start, end, IteratorOptions{})
}

// ReverseIteratorWithOptions returns a reverse iterator over [start, end).
func (t *Tree) ReverseIteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	mode := normalizeIteratorMode(opts.Mode)
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		it := t.acquireIterator(nil, nil, mode, opts.IncludeTombstones, true)
		it.valid = false
		return it
	}
	it := t.acquireIterator(start, end, mode, opts.IncludeTombstones, true)
	// Reverse seek: position at the first key < end.
	if end == nil {
		it.seekRightMost()
	} else {
		// Seek(end) would immediately enforce the end-bound and invalidate the
		// iterator when positioned at a key >= end. For reverse scans we first
		// locate the seek position without the end-bound, then step back once.
		savedEnd := it.end
		it.end = nil
		it.Seek(end)
		it.end = savedEnd
		if it.valid && compareTreeKey(it.currKey, end) >= 0 {
			it.stepBackward()
		} else if !it.valid && it.err == nil {
			it.seekRightMost()
			for it.valid && compareTreeKey(it.currKey, end) >= 0 {
				it.stepBackward()
			}
		}
	}
	for it.valid && start != nil && compareTreeKey(it.currKey, start) < 0 {
		it.stepBackward()
	}
	return it
}

func (t *Tree) acquireIterator(start, end []byte, mode IteratorMode, includeTombstones bool, reverse bool) *Iterator {
	it := iteratorPool.Get().(*Iterator)
	*it = Iterator{
		tree:              t,
		start:             start,
		end:               end,
		mode:              mode,
		includeTombstones: includeTombstones,
		reverse:           reverse,
		stack:             it.stack[:0],
	}
	if cap(it.stack) == 0 {
		it.stack = it.stackBuf[:0]
	}
	return it
}

func (it *Iterator) resetStack() {
	if cap(it.stack) == 0 {
		it.stack = it.stackBuf[:0]
		return
	}
	it.stack = it.stack[:0]
}

func (it *Iterator) Domain() (start, end []byte) {
	return it.start, it.end
}

func (it *Iterator) Valid() bool {
	return it != nil && it.valid
}

func (it *Iterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *Iterator) Close() error {
	if it == nil {
		return nil
	}
	it.tree = nil
	it.resetStack()
	it.start = nil
	it.end = nil
	it.valid = false
	it.err = nil
	it.currKey = nil
	it.leafVal = nil
	it.leafPtr = page.ValuePtr{}
	it.leafFlags = 0
	it.valOK = false
	it.currVal = nil
	iteratorPool.Put(it)
	return nil
}

func (it *Iterator) UnsafeKey() []byte { return it.currKey }
func (it *Iterator) Key() []byte       { return it.currKey }

func (it *Iterator) KeyCopy(dst []byte) []byte {
	if !it.valid || it.currKey == nil {
		return dst
	}
	return append(dst, it.currKey...)
}

func (it *Iterator) IsDeleted() bool {
	return it.valid && (it.leafFlags&node.FlagTombstone) != 0
}

func (it *Iterator) UnsafeEntry() (val []byte, ptr page.ValuePtr, flags byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, 0
	}
	if it.mode == IteratorModeKeysOnly {
		return nil, it.leafPtr, it.leafFlags
	}
	if it.leafFlags&node.FlagPointer != 0 {
		if it.mode == IteratorModePointerProjection {
			return nil, it.leafPtr, it.leafFlags
		}
		// Full mode returns inline-only here; pointer payload is resolved via Value().
		return nil, it.leafPtr, it.leafFlags
	}
	return it.leafVal, it.leafPtr, it.leafFlags
}

func (it *Iterator) UnsafeValue() []byte { return it.Value() }

func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	if it.mode == IteratorModeKeysOnly {
		return nil
	}
	if it.leafFlags&node.FlagTombstone != 0 && !it.includeTombstones {
		return nil
	}
	if it.leafFlags&node.FlagPointer == 0 {
		return it.leafVal
	}
	if it.mode == IteratorModePointerProjection {
		return nil
	}
	if it.valOK {
		return it.currVal
	}
	if it.tree == nil || it.tree.slabReader == nil {
		it.err = fmt.Errorf("iterator: pointer read with nil reader")
		it.valid = false
		return nil
	}
	var out []byte
	var err error
	if it.tree.slabKeyReader != nil {
		out, err = it.tree.slabKeyReader.ReadUnsafeForKey(it.leafPtr, it.currKey)
	} else {
		out, err = it.tree.slabReader.ReadUnsafe(it.leafPtr)
	}
	if err != nil {
		it.err = err
		it.valid = false
		return nil
	}
	it.currVal = out
	it.valOK = true
	return out
}

func (it *Iterator) ValueCopy(dst []byte) []byte {
	v := it.Value()
	if v == nil {
		return dst
	}
	return append(dst, v...)
}

func (it *Iterator) Seek(key []byte) {
	if it == nil || it.tree == nil || it.tree.pager == nil {
		if it != nil {
			it.valid = false
			if it.err == nil {
				it.err = fmt.Errorf("iterator: nil tree/pager")
			}
		}
		return
	}
	it.resetStack()
	it.valid = false
	it.err = nil
	it.currKey = nil
	it.leafVal = nil
	it.leafPtr = page.ValuePtr{}
	it.leafFlags = 0
	it.valOK = false
	it.currVal = nil

	currID := it.tree.rootPageID
	verifyAlways := it.tree.pager.VerifyOnRead()

	for depth := 0; depth < 50; depth++ {
		data, err := it.tree.pager.Get(currID)
		if err != nil {
			it.err = err
			return
		}
		n := node.NewNodeView(data)
		if verifyAlways || !it.tree.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				it.err = fmt.Errorf("checksum mismatch on page %d", currID)
				return
			}
			if !verifyAlways {
				it.tree.pager.MarkVerified(currID)
			}
		}
		switch n.Type() {
		case page.PageTypeInternal:
			var idx uint16
			if key == nil {
				idx = 0
			} else {
				i, _ := n.SearchInternal(key)
				idx = i
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: int(idx)})
			childID, err := n.GetInternalChildID(idx)
			if err != nil {
				it.err = err
				return
			}
			currID = childID

		case page.PageTypeLeaf:
			var idx uint16
			if key == nil {
				idx = 0
			} else {
				i, _, err := n.SearchLeaf(key)
				if err != nil {
					it.err = err
					return
				}
				idx = i
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: int(idx)})
			it.loadCurrent()
			if !it.includeTombstones {
				it.skipDeleted()
			}
			it.enforceBounds()
			return

		default:
			it.err = fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
			return
		}
	}
	it.err = errors.New("tree too deep")
}

func (it *Iterator) Next() {
	if it == nil || !it.valid {
		return
	}
	if it.reverse {
		it.stepBackward()
	} else {
		it.stepForward()
	}
}

func (it *Iterator) stepForward() {
	for {
		if len(it.stack) == 0 {
			it.valid = false
			return
		}
		top := &it.stack[len(it.stack)-1]
		if top.Node.Type() != page.PageTypeLeaf {
			it.valid = false
			it.err = fmt.Errorf("iterator: top of stack is not leaf")
			return
		}
		top.Index++
		if top.Index < int(top.Node.Count()) {
			it.loadCurrent()
		} else if !it.advanceToNextLeaf() {
			it.valid = false
			return
		} else {
			it.loadCurrent()
		}
		if !it.includeTombstones {
			it.skipDeleted()
		}
		it.enforceBounds()
		return
	}
}

func (it *Iterator) stepBackward() {
	for {
		if len(it.stack) == 0 {
			it.valid = false
			return
		}
		top := &it.stack[len(it.stack)-1]
		if top.Node.Type() != page.PageTypeLeaf {
			it.valid = false
			it.err = fmt.Errorf("iterator: top of stack is not leaf")
			return
		}
		top.Index--
		if top.Index >= 0 {
			it.loadCurrent()
		} else if !it.advanceToPrevLeaf() {
			it.valid = false
			return
		} else {
			it.loadCurrent()
		}
		if !it.includeTombstones {
			it.skipDeleted()
		}
		it.enforceBounds()
		return
	}
}

func (it *Iterator) loadCurrent() {
	if len(it.stack) == 0 {
		it.valid = false
		return
	}
	top := &it.stack[len(it.stack)-1]
	if top.Node.Type() != page.PageTypeLeaf {
		it.valid = false
		it.err = fmt.Errorf("iterator: loadCurrent on non-leaf")
		return
	}
	if top.Index < 0 || top.Index >= int(top.Node.Count()) {
		it.valid = false
		return
	}
	k, v, ptr, flags, err := top.Node.GetLeafEntryView(uint16(top.Index))
	if err != nil {
		it.valid = false
		it.err = err
		return
	}
	it.currKey = k
	it.leafVal = v
	it.leafPtr = ptr
	it.leafFlags = flags
	it.valOK = false
	it.currVal = nil
	it.valid = true
}

func (it *Iterator) skipDeleted() {
	for it.valid && (it.leafFlags&node.FlagTombstone) != 0 {
		if it.reverse {
			it.stepBackward()
		} else {
			it.stepForward()
		}
	}
}

func (it *Iterator) enforceBounds() {
	if !it.valid {
		return
	}
	if it.end != nil && compareTreeKey(it.currKey, it.end) >= 0 {
		it.valid = false
		return
	}
	if it.start != nil && it.reverse && compareTreeKey(it.currKey, it.start) < 0 {
		it.valid = false
		return
	}
}

func (it *Iterator) seekRightMost() {
	if it == nil || it.tree == nil || it.tree.pager == nil {
		it.valid = false
		return
	}
	it.resetStack()
	it.valid = false
	it.err = nil
	currID := it.tree.rootPageID
	verifyAlways := it.tree.pager.VerifyOnRead()
	for depth := 0; depth < 50; depth++ {
		data, err := it.tree.pager.Get(currID)
		if err != nil {
			it.err = err
			return
		}
		n := node.NewNodeView(data)
		if verifyAlways || !it.tree.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				it.err = fmt.Errorf("checksum mismatch on page %d", currID)
				return
			}
			if !verifyAlways {
				it.tree.pager.MarkVerified(currID)
			}
		}
		if n.Count() == 0 {
			it.valid = false
			return
		}
		switch n.Type() {
		case page.PageTypeInternal:
			idx := int(n.Count() - 1)
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: idx})
			childID, err := n.GetInternalChildID(uint16(idx))
			if err != nil {
				it.err = err
				return
			}
			currID = childID
		case page.PageTypeLeaf:
			idx := int(n.Count() - 1)
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: idx})
			it.loadCurrent()
			if !it.includeTombstones {
				it.skipDeleted()
			}
			it.enforceBounds()
			return
		default:
			it.err = fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
			return
		}
	}
	it.err = errors.New("tree too deep")
}

func (it *Iterator) advanceToNextLeaf() bool {
	// Pop leaf.
	it.stack = it.stack[:len(it.stack)-1]
	for len(it.stack) > 0 {
		parent := &it.stack[len(it.stack)-1]
		if parent.Node.Type() != page.PageTypeInternal {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		parent.Index++
		if parent.Index < int(parent.Node.Count()) {
			childID, err := parent.Node.GetInternalChildID(uint16(parent.Index))
			if err != nil {
				it.err = err
				return false
			}
			return it.descendToLeaf(childID, false)
		}
		it.stack = it.stack[:len(it.stack)-1]
	}
	return false
}

func (it *Iterator) advanceToPrevLeaf() bool {
	// Pop leaf.
	it.stack = it.stack[:len(it.stack)-1]
	for len(it.stack) > 0 {
		parent := &it.stack[len(it.stack)-1]
		if parent.Node.Type() != page.PageTypeInternal {
			it.stack = it.stack[:len(it.stack)-1]
			continue
		}
		parent.Index--
		if parent.Index >= 0 {
			childID, err := parent.Node.GetInternalChildID(uint16(parent.Index))
			if err != nil {
				it.err = err
				return false
			}
			return it.descendToLeaf(childID, true)
		}
		it.stack = it.stack[:len(it.stack)-1]
	}
	return false
}

func (it *Iterator) descendToLeaf(currID uint64, rightMost bool) bool {
	if it.tree == nil || it.tree.pager == nil {
		return false
	}
	verifyAlways := it.tree.pager.VerifyOnRead()
	for depth := 0; depth < 50; depth++ {
		data, err := it.tree.pager.Get(currID)
		if err != nil {
			it.err = err
			return false
		}
		n := node.NewNodeView(data)
		if verifyAlways || !it.tree.pager.IsVerified(currID) {
			if !n.VerifyChecksum() {
				it.err = fmt.Errorf("checksum mismatch on page %d", currID)
				return false
			}
			if !verifyAlways {
				it.tree.pager.MarkVerified(currID)
			}
		}
		if n.Count() == 0 {
			return false
		}
		switch n.Type() {
		case page.PageTypeInternal:
			idx := 0
			if rightMost {
				idx = int(n.Count() - 1)
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: idx})
			childID, err := n.GetInternalChildID(uint16(idx))
			if err != nil {
				it.err = err
				return false
			}
			currID = childID
		case page.PageTypeLeaf:
			idx := 0
			if rightMost {
				idx = int(n.Count() - 1)
			}
			it.stack = append(it.stack, CursorItem{PageID: currID, Node: n, Index: idx})
			return true
		default:
			it.err = fmt.Errorf("invalid page type %d at page %d", n.Type(), currID)
			return false
		}
	}
	it.err = errors.New("tree too deep")
	return false
}
