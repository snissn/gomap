package tree

import (
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

const (
	iteratorLeafPrefixCompressedFlag uint16 = 0x8000
	iteratorLeafColumnarFlag         uint16 = 0x4000
	iteratorLeafPrefixV2Flag         uint16 = 0x2000
	iteratorLeafRestartInterval             = 16
)

type combinedLeafKeyState struct {
	valid bool

	pageID uint64
	data   []byte
	count  uint16

	flagsStart   int
	prefixStart  int
	headerEnd    int
	keysBlobBase int

	keyScratch []byte
	key        []byte
	flags      byte
	index      uint16
	keyValid   bool
	keyStart   int
	keyEnd     int
}

func getUint16LE(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func (s *combinedLeafKeyState) resetPage() {
	s.valid = false
	s.pageID = 0
	s.data = nil
	s.count = 0
	s.flagsStart = 0
	s.prefixStart = 0
	s.headerEnd = 0
	s.keysBlobBase = 0
	s.key = nil
	s.flags = 0
	s.index = 0
	s.keyValid = false
	s.keyStart = 0
	s.keyEnd = 0
}

func (s *combinedLeafKeyState) init(pageID uint64, n *node.Node) (bool, error) {
	if s.valid && s.pageID == pageID {
		return true, nil
	}

	s.resetPage()

	if n.Type() != page.PageTypeLeaf {
		return false, nil
	}

	data := n.Data()
	if len(data) < node.NodeHeaderSize {
		return false, node.ErrCorruptedNode
	}
	rawFlags := getUint16LE(data[12:14])
	required := iteratorLeafPrefixCompressedFlag | iteratorLeafColumnarFlag | iteratorLeafPrefixV2Flag
	if rawFlags&required != required {
		return false, nil
	}

	count := n.Count()
	valDirStart := node.NodeHeaderSize + int(count)*node.DirectoryEntrySize
	flagsStart := valDirStart + int(count)*node.DirectoryEntrySize
	prefixStart := flagsStart + int(count)
	headerEnd := prefixStart + int(count)*node.DirectoryEntrySize
	if headerEnd > len(data) {
		return false, node.ErrCorruptedNode
	}

	keysBlobBase := len(data)
	if count > 0 {
		firstKeyOff := node.NodeHeaderSize
		if firstKeyOff+2 > len(data) {
			return false, node.ErrCorruptedNode
		}
		keysBlobBase = int(getUint16LE(data[firstKeyOff : firstKeyOff+2]))
		if keysBlobBase < headerEnd || keysBlobBase > len(data) {
			return false, node.ErrCorruptedNode
		}
	}

	s.valid = true
	s.pageID = pageID
	s.data = data
	s.count = count
	s.flagsStart = flagsStart
	s.prefixStart = prefixStart
	s.headerEnd = headerEnd
	s.keysBlobBase = keysBlobBase
	return true, nil
}

func (s *combinedLeafKeyState) ensureScratch(size int) []byte {
	if cap(s.keyScratch) < size {
		s.keyScratch = make([]byte, size)
	}
	s.keyScratch = s.keyScratch[:size]
	return s.keyScratch
}

func (s *combinedLeafKeyState) setKey(index uint16, key []byte, flags byte, keyStart int, keyEnd int) {
	s.index = index
	s.key = key
	s.flags = flags
	s.keyValid = true
	s.keyStart = keyStart
	s.keyEnd = keyEnd
}

func (s *combinedLeafKeyState) keyMetaAt(index uint16) (prefixLen int, keyStart int, keyEnd int, suffix []byte, flags byte, err error) {
	if !s.valid || index >= s.count {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}
	data := s.data

	keyOff := node.NodeHeaderSize + int(index)*2
	if keyOff+2 > len(data) {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}
	keyStart = int(getUint16LE(data[keyOff : keyOff+2]))
	keyEnd = len(data)
	if index+1 < s.count {
		nextKeyOff := keyOff + 2
		if nextKeyOff+2 > len(data) {
			return 0, 0, 0, nil, 0, node.ErrCorruptedNode
		}
		keyEnd = int(getUint16LE(data[nextKeyOff : nextKeyOff+2]))
	}
	if keyStart < s.keysBlobBase || keyEnd < keyStart || keyEnd > len(data) {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}

	flagsOff := s.flagsStart + int(index)
	if flagsOff >= len(data) {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}
	flags = data[flagsOff]

	prefixOff := s.prefixStart + int(index)*2
	if prefixOff+2 > len(data) {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}
	prefixLen = int(getUint16LE(data[prefixOff : prefixOff+2]))
	if prefixLen < 0 || prefixLen > len(data) {
		return 0, 0, 0, nil, 0, node.ErrCorruptedNode
	}
	return prefixLen, keyStart, keyEnd, data[keyStart:keyEnd], flags, nil
}

func (s *combinedLeafKeyState) keyFlagsAt(index uint16) (key []byte, flags byte, err error) {
	if !s.valid {
		return nil, 0, node.ErrCorruptedNode
	}
	if index >= s.count {
		return nil, 0, node.ErrCorruptedNode
	}
	if s.keyValid {
		if s.index == index {
			return s.key, s.flags, nil
		}
		if s.index+1 == index {
			return s.advanceOne(index)
		}
	}
	return s.rebuildAt(index)
}

func (s *combinedLeafKeyState) advanceOne(index uint16) (key []byte, flags byte, err error) {
	if !s.valid || !s.keyValid || index != s.index+1 || index >= s.count {
		return nil, 0, node.ErrCorruptedNode
	}
	data := s.data
	flagsOff := s.flagsStart + int(index)
	if flagsOff >= len(data) {
		return nil, 0, node.ErrCorruptedNode
	}
	flags = data[flagsOff]

	prefixOff := s.prefixStart + int(index)*2
	if prefixOff+2 > len(data) {
		return nil, 0, node.ErrCorruptedNode
	}
	prefixLen := int(getUint16LE(data[prefixOff : prefixOff+2]))
	if prefixLen < 0 || prefixLen > len(s.key) {
		return nil, 0, node.ErrCorruptedNode
	}

	keyStart := s.keyEnd
	keyEnd := len(data)
	if index+1 < s.count {
		nextKeyOff := node.NodeHeaderSize + int(index+1)*2
		if nextKeyOff+2 > len(data) {
			return nil, 0, node.ErrCorruptedNode
		}
		keyEnd = int(getUint16LE(data[nextKeyOff : nextKeyOff+2]))
	}
	if keyStart < s.keysBlobBase || keyEnd < keyStart || keyEnd > len(data) {
		return nil, 0, node.ErrCorruptedNode
	}
	suffix := data[keyStart:keyEnd]

	if index%iteratorLeafRestartInterval == 0 {
		if prefixLen != 0 {
			return nil, 0, node.ErrCorruptedNode
		}
		s.setKey(index, suffix, flags, keyStart, keyEnd)
		return suffix, flags, nil
	}
	if prefixLen == 0 {
		s.setKey(index, suffix, flags, keyStart, keyEnd)
		return suffix, flags, nil
	}
	cur := s.ensureScratch(prefixLen + len(suffix))
	copy(cur, s.key[:prefixLen])
	copy(cur[prefixLen:], suffix)
	s.setKey(index, cur, flags, keyStart, keyEnd)
	return cur, flags, nil
}

func (s *combinedLeafKeyState) rebuildAt(index uint16) (key []byte, flags byte, err error) {
	restart := index - (index % iteratorLeafRestartInterval)
	prefixLen, keyStart, keyEnd, suffix, flags, err := s.keyMetaAt(restart)
	if err != nil {
		return nil, 0, err
	}
	if prefixLen != 0 {
		return nil, 0, node.ErrCorruptedNode
	}
	key = suffix
	if restart == index {
		s.setKey(index, key, flags, keyStart, keyEnd)
		return key, flags, nil
	}

	prev := key
	prevStart := keyStart
	prevEnd := keyEnd
	for i := restart + 1; i <= index; i++ {
		prefixLen, keyStart, keyEnd, suffix, flags, err = s.keyMetaAt(i)
		if err != nil {
			return nil, 0, err
		}
		if prefixLen > len(prev) {
			return nil, 0, node.ErrCorruptedNode
		}
		if prefixLen == 0 {
			key = suffix
			prev = key
			prevStart = keyStart
			prevEnd = keyEnd
			continue
		}
		cur := s.ensureScratch(prefixLen + len(suffix))
		copy(cur, prev[:prefixLen])
		copy(cur[prefixLen:], suffix)
		key = cur
		prev = key
		prevStart = keyStart
		prevEnd = keyEnd
	}
	s.setKey(index, key, flags, prevStart, prevEnd)
	return key, flags, nil
}

type Iterator struct {
	tree         *Tree
	stack        []CursorItem
	stackBuf     [16]CursorItem
	leafState    combinedLeafKeyState
	start        []byte
	end          []byte
	valid        bool
	err          error
	currKey      []byte
	currVal      []byte
	currPtr      page.ValuePtr
	flags        byte
	valOK        bool
	ptrOK        bool
	ptrScratch   []byte
	slabAppender slabUnsafeAppender
	reverse      bool
	verifyAlways bool
}

type slabUnsafeAppender interface {
	ReadUnsafeAppend(ptr page.ValuePtr, dst []byte) ([]byte, error)
}

func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator {
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil} // Invalid immediately
	}
	it := &Iterator{
		tree:         t,
		start:        start,
		end:          end,
		reverse:      false,
		verifyAlways: t.pager != nil && t.pager.VerifyOnRead(),
	}
	if app, ok := t.slabReader.(slabUnsafeAppender); ok {
		it.slabAppender = app
	}
	it.resetStack()
	it.Seek(start)
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) iterator.UnsafeIterator {
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil}
	}
	it := &Iterator{
		tree:         t,
		start:        start,
		end:          end,
		reverse:      true,
		verifyAlways: t.pager != nil && t.pager.VerifyOnRead(),
	}
	if app, ok := t.slabReader.(slabUnsafeAppender); ok {
		it.slabAppender = app
	}
	it.resetStack()
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
	if it.valid && it.start != nil && compareTreeKey(it.currKey, it.start) < 0 {
		it.valid = false
	}

	return it
}

func (it *Iterator) seekRightMost() {
	it.resetStack()
	it.leafState.resetPage()
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
	it.resetStack()
	it.leafState.resetPage()
	it.valid = false
	it.err = nil
	it.currKey = nil
	it.currVal = nil
	it.currPtr = page.ValuePtr{}
	it.flags = 0
	it.valOK = false
	it.ptrOK = false

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
				// Fence pruning is most effective at the root and cheapest to
				// apply there; deeper levels can rely on separator descent.
				if len(it.stack) == 0 {
					if low, high, ok, err := n.InternalFenceBounds(); err != nil {
						it.err = err
						return
					} else if ok {
						if len(high) > 0 && compareTreeKey(key, high) >= 0 {
							it.valid = false
							return
						}
						if len(low) > 0 && compareTreeKey(key, low) < 0 {
							index = 0
						} else {
							idx, _ := n.SearchInternal(key)
							index = int(idx)
						}
					} else {
						idx, _ := n.SearchInternal(key)
						index = int(idx)
					}
				} else {
					idx, _ := n.SearchInternal(key)
					index = int(idx)
				}
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
				idx, _, err := n.SearchLeaf(key)
				if err != nil {
					it.err = err
					return
				}
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

func (it *Iterator) resetStack() {
	it.stack = it.stackBuf[:0]
}

func (it *Iterator) loadCurrent() {
	for {
		if len(it.stack) == 0 {
			it.valid = false
			return
		}

		top := &it.stack[len(it.stack)-1]

		// Check Bounds
		if top.Index < 0 {
			it.stepBackward()
			return
		}
		if top.Index >= int(top.Node.Count()) {
			it.stepForward()
			return
		}

		keyView, flags, err := it.getLeafKeyFlags(top)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}

		// Check Range Limits
		if !it.reverse {
			if it.end != nil && compareTreeKey(keyView, it.end) >= 0 {
				it.valid = false
				return
			}
		} else {
			if it.start != nil && compareTreeKey(keyView, it.start) < 0 {
				it.valid = false
				return
			}
		}

		// Skip tombstones; they are persisted in the index but hidden from iteration.
		if flags&node.FlagTombstone != 0 {
			if it.reverse {
				top.Index--
			} else {
				top.Index++
			}
			continue
		}

		it.currKey = keyView
		it.flags = flags
		it.currPtr = page.ValuePtr{}
		it.ptrOK = false
		it.currVal = nil
		it.valOK = false

		it.valid = true
		return
	}
}

func (it *Iterator) getLeafKeyFlags(top *CursorItem) (key []byte, flags byte, err error) {
	if top.Node.Type() != page.PageTypeLeaf {
		return nil, 0, node.ErrInvalidType
	}
	isCombined, err := it.leafState.init(top.PageID, &top.Node)
	if err != nil {
		return nil, 0, err
	}
	if isCombined {
		return it.leafState.keyFlagsAt(uint16(top.Index))
	}
	return top.Node.GetLeafKeyFlagsView(uint16(top.Index))
}

func (it *Iterator) stepForward() {
	for len(it.stack) > 0 {
		idx := len(it.stack) - 1
		top := &it.stack[idx]

		top.Index++

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
		top := &it.stack[idx]

		top.Index--

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
		if !it.ensurePointerLoaded() {
			return nil
		}
		if it.valOK {
			return it.currVal
		}
		if it.slabAppender != nil {
			val, err := it.slabAppender.ReadUnsafeAppend(it.currPtr, it.ptrScratch[:0])
			if err != nil {
				it.err = err
				it.valid = false
				return nil
			}
			it.ptrScratch = val
			it.currVal = it.ptrScratch
			it.valOK = true
			return it.currVal
		}
		val, err := it.tree.slabReader.ReadUnsafe(it.currPtr)
		if err != nil {
			it.err = err
			it.valid = false
			return nil
		}
		it.currVal = val
		it.valOK = true
		return it.currVal
	}
	if !it.ensureInlineValueLoaded() {
		return nil
	}
	return it.currVal
}

func (it *Iterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if it.currKey == nil {
		return nil, page.ValuePtr{}, 0
	}
	if it.flags&node.FlagPointer != 0 {
		if !it.ensurePointerLoaded() {
			return nil, page.ValuePtr{}, it.flags
		}
	} else if !it.ensureInlineValueLoaded() {
		return nil, page.ValuePtr{}, it.flags
	}
	return it.currVal, it.currPtr, it.flags
}

func (it *Iterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *Iterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *Iterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *Iterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
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
	verifyAlways := it.verifyAlways
	if verifyAlways || !it.tree.pager.IsVerified(pageID) {
		if !n.VerifyChecksum() {
			return node.Node{}, fmt.Errorf("checksum mismatch on page %d", pageID)
		}
		if !verifyAlways {
			it.tree.pager.MarkVerified(pageID)
		}
	}

	return n, nil
}

func (it *Iterator) ensurePointerLoaded() bool {
	if it.ptrOK {
		return true
	}
	if it.flags&node.FlagPointer == 0 {
		it.ptrOK = true
		return true
	}
	if len(it.stack) == 0 {
		it.err = fmt.Errorf("iterator pointer load: empty stack")
		it.valid = false
		return false
	}
	top := &it.stack[len(it.stack)-1]
	_, ptr, flags, err := top.Node.GetLeafValueView(uint16(top.Index))
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	if flags&node.FlagPointer == 0 {
		it.err = fmt.Errorf("iterator pointer load: entry is not a pointer")
		it.valid = false
		return false
	}
	it.currPtr = ptr
	it.ptrOK = true
	return true
}

func (it *Iterator) ensureInlineValueLoaded() bool {
	if it.valOK {
		return true
	}
	if it.flags&node.FlagPointer != 0 {
		return true
	}
	if len(it.stack) == 0 {
		it.err = fmt.Errorf("iterator inline value load: empty stack")
		it.valid = false
		return false
	}
	top := &it.stack[len(it.stack)-1]
	val, _, flags, err := top.Node.GetLeafValueView(uint16(top.Index))
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	if flags&node.FlagPointer != 0 {
		it.err = fmt.Errorf("iterator inline value load: entry is a pointer")
		it.valid = false
		return false
	}
	it.currVal = val
	it.valOK = true
	it.ptrOK = true
	return true
}
