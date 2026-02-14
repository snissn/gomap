package tree

import (
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type CursorItem struct {
	PageID uint64
	Node   node.Node
	Index  int
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
	tree            *Tree
	stack           []CursorItem
	stackBuf        [16]CursorItem
	leafState       combinedLeafKeyState
	start           []byte
	end             []byte
	valid           bool
	err             error
	currKey         []byte
	currVal         []byte
	currPtr         page.ValuePtr
	flags           byte
	valOK           bool
	ptrOK           bool
	ptrScratch      []byte
	slabAppender    slabUnsafeAppender
	slabBatcher     slabUnsafeBatchAppender
	slabKeyReader   slabUnsafeKeyReader
	slabFenceBlocks slabUnsafeFenceBlockReader
	slabKeyAppender slabUnsafeKeyAppender
	slabKeyBatcher  slabUnsafeKeyBatchAppender

	fenceEntries []FenceBlockEntry
	fenceIndex   int
	fenceActive  bool

	pendingSeekKey        []byte
	pendingFencePageID    uint64
	pendingFenceLeafIndex int
	pendingFenceEntryIdx  int
	pendingFenceReady     bool

	prefetchPageID uint64
	prefetchStart  int
	prefetchLen    int
	prefetchStep   int
	prefetchPtrs   []page.ValuePtr
	prefetchKeys   [][]byte
	prefetchVals   [][]byte
	prefetchArmed  bool
	mode           IteratorMode
	reverse        bool
	verifyAlways   bool
}

const iteratorPointerBatchMax = 2

func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator {
	return t.IteratorWithOptions(start, end, IteratorOptions{})
}

func normalizeIteratorMode(mode IteratorMode) IteratorMode {
	switch mode {
	case IteratorModeKeysOnly, IteratorModePointerProjection:
		return mode
	default:
		return IteratorModeFull
	}
}

// IteratorWithOptions returns a forward iterator over [start, end) using the
// provided value materialization mode.
func (t *Tree) IteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil} // Invalid immediately
	}
	it := &Iterator{
		tree:         t,
		start:        start,
		end:          end,
		mode:         normalizeIteratorMode(opts.Mode),
		reverse:      false,
		verifyAlways: t.pager != nil && t.pager.VerifyOnRead(),
	}
	it.slabAppender = t.slabAppender
	if batch, ok := t.slabReader.(slabUnsafeBatchAppender); ok {
		it.slabBatcher = batch
	}
	it.slabKeyReader = t.slabKeyReader
	it.slabFenceBlocks = t.slabFenceBlocks
	it.slabKeyAppender = t.slabKeyAppender
	if keyBatch, ok := t.slabReader.(slabUnsafeKeyBatchAppender); ok {
		it.slabKeyBatcher = keyBatch
	}
	it.resetStack()
	it.Seek(start)
	if start == nil {
		it.prefetchArmed = true
	}
	return it
}

func (t *Tree) ReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return t.ReverseIteratorWithOptions(start, end, IteratorOptions{})
}

// ReverseIteratorWithOptions returns a reverse iterator over [start, end)
// using the provided value materialization mode.
func (t *Tree) ReverseIteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return &Iterator{tree: t, valid: false, err: nil}
	}
	it := &Iterator{
		tree:         t,
		start:        start,
		end:          end,
		mode:         normalizeIteratorMode(opts.Mode),
		reverse:      true,
		verifyAlways: t.pager != nil && t.pager.VerifyOnRead(),
	}
	it.slabAppender = t.slabAppender
	if batch, ok := t.slabReader.(slabUnsafeBatchAppender); ok {
		it.slabBatcher = batch
	}
	it.slabKeyReader = t.slabKeyReader
	it.slabFenceBlocks = t.slabFenceBlocks
	it.slabKeyAppender = t.slabKeyAppender
	if keyBatch, ok := t.slabReader.(slabUnsafeKeyBatchAppender); ok {
		it.slabKeyBatcher = keyBatch
	}
	it.resetStack()
	// Reverse seek: Find >= end, then step back.
	if end == nil {
		it.seekRightMost()
	} else {
		it.Seek(end)
		if it.valid {
			// Standard seek positions at the first key >= end; reverse iteration
			// then steps once to land on the first key < end.
			//
			// Fence-block expansion may already resolve directly to a key < end
			// when the first physical fence key >= end belongs to a block whose
			// logical keys are all below end.
			if compareTreeKey(it.currKey, end) >= 0 {
				it.stepBackward()
			}
		} else if it.err == nil {
			// seek(end) fell off the end (no error, just exhausted).
			it.seekRightMost()
		}
	}

	// Check Start bound
	if it.valid && it.start != nil && compareTreeKey(it.currKey, it.start) < 0 {
		it.valid = false
	}
	if end == nil {
		it.prefetchArmed = true
	}

	return it
}

func (it *Iterator) seekRightMost() {
	it.resetStack()
	it.resetPointerPrefetch()
	it.resetFenceCursor()
	it.clearPendingFenceSeek()
	it.prefetchArmed = false
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
	it.resetPointerPrefetch()
	it.resetFenceCursor()
	it.clearPendingFenceSeek()
	if !it.reverse && key != nil {
		it.pendingSeekKey = append(it.pendingSeekKey[:0], key...)
	}
	it.prefetchArmed = false
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
				idx, found, err := n.SearchLeaf(key)
				if err != nil {
					it.err = err
					return
				}
				if found {
					it.pendingSeekKey = it.pendingSeekKey[:0]
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

func (it *Iterator) resetFenceCursor() {
	it.fenceEntries = it.fenceEntries[:0]
	it.fenceIndex = 0
	it.fenceActive = false
}

func (it *Iterator) clearPendingFenceSeek() {
	it.pendingSeekKey = it.pendingSeekKey[:0]
	it.pendingFencePageID = 0
	it.pendingFenceLeafIndex = 0
	it.pendingFenceEntryIdx = 0
	it.pendingFenceReady = false
}

func (it *Iterator) setCurrentFenceEntry(entry FenceBlockEntry) {
	it.currKey = entry.Key
	it.flags = 0
	it.currPtr = page.ValuePtr{}
	it.ptrOK = true
	if it.mode == IteratorModeKeysOnly {
		it.currVal = nil
	} else {
		it.currVal = entry.Value
	}
	it.valOK = true
	it.valid = true
}

func lowerBoundFenceEntries(entries []FenceBlockEntry, key []byte) int {
	if len(key) == 0 {
		return 0
	}
	return sort.Search(len(entries), func(i int) bool {
		return compareTreeKey(entries[i].Key, key) >= 0
	})
}

func (it *Iterator) tryRepositionPendingFence(top *CursorItem) (bool, error) {
	if it.reverse || len(it.pendingSeekKey) == 0 || it.slabFenceBlocks == nil {
		return false, nil
	}
	if top == nil || top.Node.Type() != page.PageTypeLeaf || top.Index <= 0 {
		return false, nil
	}
	seekKey := it.pendingSeekKey
	for scan := top.Index - 1; scan >= 0; scan-- {
		_, ptr, flags, err := top.Node.GetLeafValueView(uint16(scan))
		if err != nil {
			return false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		entries, ok, err := it.slabFenceBlocks.ReadUnsafeFenceBlock(ptr)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		pos := lowerBoundFenceEntries(entries, seekKey)
		if pos < len(entries) {
			top.Index = scan
			it.pendingFencePageID = top.PageID
			it.pendingFenceLeafIndex = scan
			it.pendingFenceEntryIdx = pos
			it.pendingFenceReady = true
			return true, nil
		}
		// Nearest predecessor fence did not contain a candidate >= seek key.
		// Earlier fences are strictly smaller and cannot satisfy this seek.
		return false, nil
	}
	return false, nil
}

func (it *Iterator) expandFenceBlockAt(top *CursorItem) (handled bool, produced bool, exhausted bool, err error) {
	if top == nil || top.Node.Type() != page.PageTypeLeaf || it.slabFenceBlocks == nil {
		return false, false, false, nil
	}
	if it.mode == IteratorModePointerProjection {
		// Pointer projection must expose raw leaf pointers. Expanding fence blocks
		// into inline key/value pairs would hide pointer metadata from callers that
		// scan pointer reachability (GC/rewrite/retention accounting).
		return false, false, false, nil
	}
	_, ptr, flags, err := top.Node.GetLeafValueView(uint16(top.Index))
	if err != nil {
		return false, false, false, err
	}
	if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
		return false, false, false, nil
	}

	entries, ok, err := it.slabFenceBlocks.ReadUnsafeFenceBlock(ptr)
	if err != nil {
		return false, false, false, err
	}
	if !ok {
		return false, false, false, nil
	}
	handled = true
	if len(entries) == 0 {
		return handled, false, false, nil
	}

	pos := 0
	if it.pendingFenceReady && top.PageID == it.pendingFencePageID && top.Index == it.pendingFenceLeafIndex {
		pos = it.pendingFenceEntryIdx
	} else if it.reverse {
		pos = len(entries) - 1
		if it.end != nil {
			pos = lowerBoundFenceEntries(entries, it.end) - 1
		}
	} else {
		lower := it.start
		if len(it.pendingSeekKey) > 0 {
			lower = it.pendingSeekKey
		}
		if lower != nil {
			pos = lowerBoundFenceEntries(entries, lower)
		}
	}
	if pos < 0 || pos >= len(entries) {
		return handled, false, false, nil
	}

	if !it.reverse {
		if it.end != nil && compareTreeKey(entries[pos].Key, it.end) >= 0 {
			return handled, false, true, nil
		}
	} else if it.start != nil && compareTreeKey(entries[pos].Key, it.start) < 0 {
		return handled, false, true, nil
	}

	it.fenceEntries = entries
	it.fenceIndex = pos
	it.fenceActive = true
	it.setCurrentFenceEntry(entries[pos])
	return handled, true, false, nil
}

func (it *Iterator) loadCurrent() {
	it.resetFenceCursor()
	for {
		if len(it.stack) == 0 {
			it.valid = false
			it.clearPendingFenceSeek()
			return
		}

		top := &it.stack[len(it.stack)-1]

		// Check Bounds
		if top.Index < 0 {
			it.clearPendingFenceSeek()
			it.stepBackward()
			return
		}
		if top.Index >= int(top.Node.Count()) {
			it.clearPendingFenceSeek()
			it.stepForward()
			return
		}

		if moved, err := it.tryRepositionPendingFence(top); err != nil {
			it.err = err
			it.valid = false
			return
		} else if moved {
			continue
		} else if len(it.pendingSeekKey) > 0 {
			// One-shot seek lower-bound handling: once we've confirmed there is
			// no hidden predecessor fence candidate for this leaf position,
			// continue with regular key materialization.
			it.pendingSeekKey = it.pendingSeekKey[:0]
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
				it.clearPendingFenceSeek()
				return
			}
		} else {
			if it.start != nil && compareTreeKey(keyView, it.start) < 0 {
				it.valid = false
				it.clearPendingFenceSeek()
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

		if flags&node.FlagPointer != 0 {
			handled, produced, exhausted, err := it.expandFenceBlockAt(top)
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			if handled {
				if produced {
					it.clearPendingFenceSeek()
					return
				}
				if exhausted {
					it.valid = false
					it.clearPendingFenceSeek()
					return
				}
				it.clearPendingFenceSeek()
				if it.reverse {
					top.Index--
				} else {
					top.Index++
				}
				continue
			}
		}

		it.currKey = keyView
		it.flags = flags
		it.currPtr = page.ValuePtr{}
		it.ptrOK = false
		it.currVal = nil
		it.valOK = false

		it.valid = true
		it.clearPendingFenceSeek()
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
	if it.fenceActive && len(it.fenceEntries) > 0 {
		next := it.fenceIndex + 1
		if it.reverse {
			next = it.fenceIndex - 1
		}
		if next >= 0 && next < len(it.fenceEntries) {
			entry := it.fenceEntries[next]
			if !it.reverse {
				if it.end != nil && compareTreeKey(entry.Key, it.end) >= 0 {
					it.resetFenceCursor()
					it.valid = false
					return
				}
			} else {
				if it.start != nil && compareTreeKey(entry.Key, it.start) < 0 {
					it.resetFenceCursor()
					it.valid = false
					return
				}
			}
			it.fenceIndex = next
			it.setCurrentFenceEntry(entry)
			return
		}
		it.resetFenceCursor()
	}
	if len(it.stack) > 0 {
		it.prefetchArmed = true
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
	if it.mode == IteratorModeKeysOnly {
		return nil
	}
	if it.flags&node.FlagPointer != 0 {
		if it.mode == IteratorModePointerProjection {
			if !it.ensurePointerLoaded() {
				return nil
			}
			return nil
		}
		if it.valOK {
			return it.currVal
		}
		if it.tryUsePrefetchedPointerValue() {
			return it.currVal
		}
		if it.prefetchArmed && it.prefetchPointerRun() && it.tryUsePrefetchedPointerValue() {
			return it.currVal
		}
		if !it.ensurePointerLoaded() {
			return nil
		}
		if it.slabKeyAppender != nil {
			val, err := it.slabKeyAppender.ReadUnsafeAppendForKey(it.currPtr, it.currKey, it.ptrScratch[:0])
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
		if it.slabKeyReader != nil {
			val, err := it.slabKeyReader.ReadUnsafeForKey(it.currPtr, it.currKey)
			if err != nil {
				it.err = err
				it.valid = false
				return nil
			}
			it.currVal = val
			it.valOK = true
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
	if it.mode == IteratorModeKeysOnly {
		return nil, page.ValuePtr{}, it.flags
	}
	if it.flags&node.FlagPointer != 0 {
		if !it.ensurePointerLoaded() {
			return nil, page.ValuePtr{}, it.flags
		}
		if it.mode == IteratorModePointerProjection {
			return nil, it.currPtr, it.flags
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
	it.resetPointerPrefetch()
	it.resetFenceCursor()
	it.clearPendingFenceSeek()
	it.ptrScratch = nil
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

func (it *Iterator) resetPointerPrefetch() {
	it.prefetchPageID = 0
	it.prefetchStart = 0
	it.prefetchLen = 0
	it.prefetchStep = 0
	it.prefetchPtrs = it.prefetchPtrs[:0]
	it.prefetchKeys = it.prefetchKeys[:0]
	it.prefetchVals = it.prefetchVals[:0]
}

func (it *Iterator) tryUsePrefetchedPointerValue() bool {
	if len(it.stack) == 0 || it.prefetchLen == 0 {
		return false
	}
	top := &it.stack[len(it.stack)-1]
	if it.prefetchPageID != top.PageID {
		return false
	}
	var pos int
	switch it.prefetchStep {
	case 1:
		pos = top.Index - it.prefetchStart
	case -1:
		pos = it.prefetchStart - top.Index
	default:
		return false
	}
	if pos < 0 || pos >= it.prefetchLen {
		return false
	}
	it.currVal = it.prefetchVals[pos]
	it.valOK = true
	return true
}

func (it *Iterator) prefetchPointerRun() bool {
	if len(it.stack) == 0 {
		return false
	}
	top := &it.stack[len(it.stack)-1]
	if top.Node.Type() != page.PageTypeLeaf {
		return false
	}
	count := int(top.Node.Count())
	if count == 0 {
		return false
	}
	step := 1
	if it.reverse {
		step = -1
	}

	it.prefetchPageID = top.PageID
	it.prefetchStart = top.Index
	it.prefetchLen = 0
	it.prefetchStep = step
	it.prefetchPtrs = it.prefetchPtrs[:0]
	it.prefetchKeys = it.prefetchKeys[:0]

	for idx := top.Index; idx >= 0 && idx < count && len(it.prefetchPtrs) < iteratorPointerBatchMax; idx += step {
		key, _, ptr, flags, err := top.Node.GetLeafEntryView(uint16(idx))
		if err != nil {
			it.err = err
			it.valid = false
			it.resetPointerPrefetch()
			return false
		}
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			if len(it.prefetchPtrs) == 0 {
				return false
			}
			break
		}
		it.prefetchPtrs = append(it.prefetchPtrs, ptr)
		// GetLeafEntryView may return a scratch-backed key for prefix-compressed
		// leaves; copy so batched key-aware pointer reads see stable bytes.
		it.prefetchKeys = append(it.prefetchKeys, append([]byte(nil), key...))
	}

	// Keep isolated pointers on the single-read path.
	prefetchLen := len(it.prefetchPtrs)
	if prefetchLen < 2 {
		it.resetPointerPrefetch()
		return false
	}

	if cap(it.prefetchVals) < prefetchLen {
		it.prefetchVals = make([][]byte, prefetchLen)
	} else {
		it.prefetchVals = it.prefetchVals[:prefetchLen]
	}
	for i := range it.prefetchVals {
		it.prefetchVals[i] = it.prefetchVals[i][:0]
	}

	var err error
	if it.slabKeyBatcher != nil {
		it.prefetchVals, err = it.slabKeyBatcher.ReadUnsafeAppendBatchForKeys(it.prefetchPtrs, it.prefetchKeys, it.prefetchVals)
	} else if it.slabBatcher != nil {
		it.prefetchVals, err = it.slabBatcher.ReadUnsafeAppendBatch(it.prefetchPtrs, it.prefetchVals)
	} else if it.slabKeyAppender != nil {
		for i := range it.prefetchPtrs {
			it.prefetchVals[i], err = it.slabKeyAppender.ReadUnsafeAppendForKey(it.prefetchPtrs[i], it.prefetchKeys[i], it.prefetchVals[i][:0])
			if err != nil {
				break
			}
		}
	} else if it.slabAppender != nil {
		for i := range it.prefetchPtrs {
			it.prefetchVals[i], err = it.slabAppender.ReadUnsafeAppend(it.prefetchPtrs[i], it.prefetchVals[i][:0])
			if err != nil {
				break
			}
		}
	} else if it.slabKeyReader != nil {
		for i := range it.prefetchPtrs {
			var val []byte
			val, err = it.slabKeyReader.ReadUnsafeForKey(it.prefetchPtrs[i], it.prefetchKeys[i])
			if err != nil {
				break
			}
			it.prefetchVals[i] = append(it.prefetchVals[i][:0], val...)
		}
	} else {
		for i := range it.prefetchPtrs {
			var val []byte
			val, err = it.tree.slabReader.ReadUnsafe(it.prefetchPtrs[i])
			if err != nil {
				break
			}
			it.prefetchVals[i] = append(it.prefetchVals[i][:0], val...)
		}
	}
	if err != nil {
		it.err = err
		it.valid = false
		it.resetPointerPrefetch()
		return false
	}
	it.prefetchLen = prefetchLen
	return true
}
