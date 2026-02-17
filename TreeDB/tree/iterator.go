package tree

import (
	"fmt"
	"sort"
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

const (
	// Drop unusually large iterator scratch/state buffers instead of returning
	// them to the pool. This keeps long-tail scans from inflating steady-state
	// pool footprint.
	iteratorPoolMaxStackCap      = 128
	iteratorPoolMaxFenceEntryCap = 512
	iteratorPoolMaxFenceKeyCap   = 512
	iteratorPoolMaxPrefetchCap   = 32
	iteratorPoolMaxScratchBytes  = 256 << 10 // 256 KiB
)

type iteratorReusableBuffers struct {
	stack            []CursorItem
	fenceEntries     []FenceBlockEntry
	fenceKeys        [][]byte
	pendingFenceKeys [][]byte
	prefetchPtrs     []page.ValuePtr
	prefetchKeys     [][]byte
	prefetchVals     [][]byte
	ptrScratch       []byte
	keyScratch       []byte
}

func retainSliceCap[T any](s []T, maxCap int) []T {
	if cap(s) == 0 || cap(s) > maxCap {
		return nil
	}
	full := s[:cap(s)]
	clear(full)
	return full[:0]
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
	slabFenceKeys   slabUnsafeFenceBlockKeyReader
	slabFenceRange  slabUnsafeFenceBlockRangeKeyReader
	slabFenceRangeL slabUnsafeFenceBlockRangeKeyLeaseReader
	slabFenceSeek   slabUnsafeFenceBlockSeekReader
	slabFenceSeekL  slabUnsafeFenceBlockSeekLeaseReader
	slabFencePtrCls slabFencePointerClassifier
	slabKeyAppender slabUnsafeKeyAppender
	slabKeyBatcher  slabUnsafeKeyBatchAppender

	fenceEntries    []FenceBlockEntry
	fenceKeys       [][]byte
	fenceKeyLease   FenceKeysLease
	fenceIndex      int
	fenceActive     bool
	fenceValuesLazy bool
	fenceBlockPtr   page.ValuePtr

	pendingSeekKey        []byte
	pendingFencePageID    uint64
	pendingFenceLeafIndex int
	pendingFenceEntryIdx  int
	pendingFenceReady     bool
	pendingFenceKeys      [][]byte
	pendingFenceKeyLease  FenceKeysLease

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

func (it *Iterator) captureReusableBuffers() iteratorReusableBuffers {
	return iteratorReusableBuffers{
		stack:            it.stack,
		fenceEntries:     it.fenceEntries,
		fenceKeys:        it.fenceKeys,
		pendingFenceKeys: it.pendingFenceKeys,
		prefetchPtrs:     it.prefetchPtrs,
		prefetchKeys:     it.prefetchKeys,
		prefetchVals:     it.prefetchVals,
		ptrScratch:       it.ptrScratch,
		keyScratch:       it.leafState.keyScratch,
	}
}

func (it *Iterator) trimReusableBuffers() iteratorReusableBuffers {
	buf := iteratorReusableBuffers{}
	if cap(it.stack) > len(it.stackBuf) {
		buf.stack = retainSliceCap(it.stack, iteratorPoolMaxStackCap)
	}
	buf.fenceEntries = retainSliceCap(it.fenceEntries, iteratorPoolMaxFenceEntryCap)
	buf.fenceKeys = retainSliceCap(it.fenceKeys, iteratorPoolMaxFenceKeyCap)
	buf.pendingFenceKeys = retainSliceCap(it.pendingFenceKeys, iteratorPoolMaxFenceKeyCap)
	buf.prefetchPtrs = retainSliceCap(it.prefetchPtrs, iteratorPoolMaxPrefetchCap)
	buf.prefetchKeys = retainSliceCap(it.prefetchKeys, iteratorPoolMaxPrefetchCap)
	buf.prefetchVals = retainSliceCap(it.prefetchVals, iteratorPoolMaxPrefetchCap)
	buf.ptrScratch = retainSliceCap(it.ptrScratch, iteratorPoolMaxScratchBytes)
	buf.keyScratch = retainSliceCap(it.leafState.keyScratch, iteratorPoolMaxScratchBytes)
	return buf
}

func (it *Iterator) installReusableBuffers(buf iteratorReusableBuffers) {
	if cap(buf.stack) > 0 {
		it.stack = buf.stack[:0]
	} else {
		it.resetStack()
	}
	it.fenceEntries = buf.fenceEntries[:0]
	it.fenceKeys = buf.fenceKeys[:0]
	it.pendingFenceKeys = buf.pendingFenceKeys[:0]
	it.prefetchPtrs = buf.prefetchPtrs[:0]
	it.prefetchKeys = buf.prefetchKeys[:0]
	it.prefetchVals = buf.prefetchVals[:0]
	it.ptrScratch = buf.ptrScratch[:0]
	it.leafState.keyScratch = buf.keyScratch[:0]
}

func (t *Tree) acquireIterator(start, end []byte, mode IteratorMode, reverse bool) *Iterator {
	it := iteratorPool.Get().(*Iterator)
	buf := it.captureReusableBuffers()
	*it = Iterator{
		tree:            t,
		start:           start,
		end:             end,
		mode:            mode,
		reverse:         reverse,
		verifyAlways:    t.pager != nil && t.pager.VerifyOnRead(),
		slabAppender:    t.slabAppender,
		slabBatcher:     t.slabBatcher,
		slabKeyReader:   t.slabKeyReader,
		slabFenceBlocks: t.slabFenceBlocks,
		slabFenceKeys:   t.slabFenceKeys,
		slabFenceRange:  t.slabFenceRange,
		slabFenceRangeL: t.slabFenceRangeL,
		slabFenceSeek:   t.slabFenceSeek,
		slabFenceSeekL:  t.slabFenceSeekL,
		slabFencePtrCls: t.slabFencePtrCls,
		slabKeyAppender: t.slabKeyAppender,
		slabKeyBatcher:  t.slabKeyBatcher,
	}
	it.installReusableBuffers(buf)
	return it
}

// IteratorWithOptions returns a forward iterator over [start, end) using the
// provided value materialization mode.
func (t *Tree) IteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	mode := normalizeIteratorMode(opts.Mode)
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return t.acquireIterator(nil, nil, mode, false) // Invalid immediately
	}
	it := t.acquireIterator(start, end, mode, false)
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
	mode := normalizeIteratorMode(opts.Mode)
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return t.acquireIterator(nil, nil, mode, true)
	}
	it := t.acquireIterator(start, end, mode, true)
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
		// pendingSeekKey is intentionally forward-only. It seeds predecessor
		// fence-block repositioning in tryRepositionPendingFence; reverse seeks
		// use seek(end)+stepBackward semantics instead.
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

func (it *Iterator) releaseFenceKeyLease() {
	if it == nil || it.fenceKeyLease == nil {
		return
	}
	it.fenceKeyLease.Release()
	it.fenceKeyLease = nil
}

func (it *Iterator) releasePendingFenceKeyLease() {
	if it == nil || it.pendingFenceKeyLease == nil {
		return
	}
	it.pendingFenceKeyLease.Release()
	it.pendingFenceKeyLease = nil
}

func (it *Iterator) resetFenceCursor() {
	it.releaseFenceKeyLease()
	it.fenceEntries = it.fenceEntries[:0]
	it.fenceKeys = it.fenceKeys[:0]
	it.fenceIndex = 0
	it.fenceActive = false
	it.fenceValuesLazy = false
	it.fenceBlockPtr = page.ValuePtr{}
}

func (it *Iterator) clearPendingFenceSeek() {
	it.releasePendingFenceKeyLease()
	it.pendingSeekKey = it.pendingSeekKey[:0]
	it.pendingFencePageID = 0
	it.pendingFenceLeafIndex = 0
	it.pendingFenceEntryIdx = 0
	it.pendingFenceReady = false
	it.pendingFenceKeys = nil
}

func (it *Iterator) setCurrentFenceEntry(entry FenceBlockEntry) {
	it.currKey = entry.Key
	it.flags = 0
	it.currPtr = page.ValuePtr{}
	it.ptrOK = true
	if it.mode == IteratorModeKeysOnly {
		it.currVal = nil
		it.valOK = true
	} else if it.fenceValuesLazy {
		// Key-only fence expansion path: resolve values lazily on demand.
		it.currVal = nil
		it.valOK = false
	} else {
		it.currVal = entry.Value
		it.valOK = true
	}
	it.valid = true
}

func (it *Iterator) setCurrentFenceKey(key []byte) {
	it.currKey = key
	it.flags = 0
	it.currPtr = page.ValuePtr{}
	it.ptrOK = true
	if it.mode == IteratorModeKeysOnly {
		it.currVal = nil
		it.valOK = true
	} else if it.fenceValuesLazy {
		it.currVal = nil
		it.valOK = false
	} else {
		it.currVal = nil
		it.valOK = true
	}
	it.valid = true
}

func (it *Iterator) shouldPreferFenceKeyOnly() bool {
	if it.mode == IteratorModeKeysOnly {
		return true
	}
	// For bounded range scans in full mode, prefer key-only fence expansion.
	// Values remain available via lazy fallback when Value()/UnsafeValue() is
	// requested, but key-only scans avoid materializing block values they never use.
	return it.mode == IteratorModeFull && it.start != nil && it.end != nil
}

func lowerBoundFenceEntries(entries []FenceBlockEntry, key []byte) int {
	if len(key) == 0 {
		return 0
	}
	return sort.Search(len(entries), func(i int) bool {
		return compareTreeKey(entries[i].Key, key) >= 0
	})
}

func lowerBoundFenceKeys(keys [][]byte, key []byte) int {
	if len(key) == 0 {
		return 0
	}
	return sort.Search(len(keys), func(i int) bool {
		return compareTreeKey(keys[i], key) >= 0
	})
}

func (it *Iterator) pointerLikelyFenceBlock(ptr page.ValuePtr) bool {
	if it != nil && it.slabFencePtrCls != nil {
		return it.slabFencePtrCls.FencePointerLikelyBlock(ptr)
	}
	return true
}

func (it *Iterator) tryRepositionPendingFence(top *CursorItem) (bool, error) {
	if it.reverse || len(it.pendingSeekKey) == 0 || (it.slabFenceBlocks == nil && it.slabFenceKeys == nil && it.slabFenceSeek == nil && it.slabFenceSeekL == nil) {
		return false, nil
	}
	if top == nil || top.Node.Type() != page.PageTypeLeaf || top.Index <= 0 {
		return false, nil
	}
	seekKey := it.pendingSeekKey
	preferFenceEntries := it.slabFenceBlocks != nil && (it.slabFenceKeys == nil || !it.shouldPreferFenceKeyOnly())
	for scan := top.Index - 1; scan >= 0; scan-- {
		_, ptr, flags, err := top.Node.GetLeafValueView(uint16(scan))
		if err != nil {
			return false, err
		}
		if flags&node.FlagTombstone != 0 || flags&node.FlagPointer == 0 {
			continue
		}
		if !it.pointerLikelyFenceBlock(ptr) {
			continue
		}
		keyReaderUsable := false
		if it.slabFenceSeekL != nil {
			pos, below, above, seekLease, ok, err := it.slabFenceSeekL.ReadUnsafeFenceBlockSeekLease(ptr, seekKey)
			if err != nil {
				if seekLease != nil {
					seekLease.Release()
				}
				return false, err
			}
			if ok {
				keyReaderUsable = true
				var seekKeys [][]byte
				if seekLease != nil {
					seekKeys = seekLease.Keys()
				}
				if above {
					if seekLease != nil {
						seekLease.Release()
					}
					break
				}
				if below {
					if seekLease != nil {
						seekLease.Release()
					}
					continue
				}
				if pos >= 0 && pos < len(seekKeys) {
					top.Index = scan
					it.pendingFencePageID = top.PageID
					it.pendingFenceLeafIndex = scan
					it.pendingFenceEntryIdx = pos
					it.pendingFenceReady = true
					it.releasePendingFenceKeyLease()
					it.pendingFenceKeys = seekKeys
					it.pendingFenceKeyLease = seekLease
					// We already selected the predecessor block that contains seekKey.
					// Clearing pendingSeekKey avoids a redundant predecessor rescan on
					// the next loadCurrent loop iteration.
					it.pendingSeekKey = it.pendingSeekKey[:0]
					return true, nil
				}
				if seekLease != nil {
					seekLease.Release()
				}
				if len(seekKeys) == 0 {
					continue
				}
				break
			}
		}
		if it.slabFenceSeek != nil {
			pos, below, above, seekKeys, ok, err := it.slabFenceSeek.ReadUnsafeFenceBlockSeek(ptr, seekKey)
			if err != nil {
				return false, err
			}
			if ok {
				keyReaderUsable = true
				if above {
					break
				}
				if below {
					continue
				}
				if pos >= 0 && pos < len(seekKeys) {
					top.Index = scan
					it.pendingFencePageID = top.PageID
					it.pendingFenceLeafIndex = scan
					it.pendingFenceEntryIdx = pos
					it.pendingFenceReady = true
					it.releasePendingFenceKeyLease()
					it.pendingFenceKeys = seekKeys
					// We already selected the predecessor block that contains seekKey.
					// Clearing pendingSeekKey avoids a redundant predecessor rescan on
					// the next loadCurrent loop iteration.
					it.pendingSeekKey = it.pendingSeekKey[:0]
					return true, nil
				}
				if len(seekKeys) == 0 {
					continue
				}
				break
			}
		}
		if it.slabFenceKeys != nil {
			keys, ok, err := it.slabFenceKeys.ReadUnsafeFenceBlockKeys(ptr)
			if err != nil {
				return false, err
			}
			if ok {
				keyReaderUsable = true
				if len(keys) == 0 {
					continue
				}
				// Ordered, non-overlapping fence layouts dominate production data.
				// If seekKey is beyond this predecessor block's upper bound, earlier
				// predecessor blocks cannot contain it; stop scanning.
				if compareTreeKey(seekKey, keys[len(keys)-1]) > 0 {
					break
				}
				if compareTreeKey(seekKey, keys[0]) < 0 {
					continue
				}
				pos := lowerBoundFenceKeys(keys, seekKey)
				if pos < len(keys) {
					top.Index = scan
					it.pendingFencePageID = top.PageID
					it.pendingFenceLeafIndex = scan
					it.pendingFenceEntryIdx = pos
					it.pendingFenceReady = true
					it.releasePendingFenceKeyLease()
					it.pendingFenceKeys = keys
					// We already selected the predecessor block that contains seekKey.
					// Clearing pendingSeekKey avoids a redundant predecessor rescan on
					// the next loadCurrent loop iteration.
					it.pendingSeekKey = it.pendingSeekKey[:0]
					return true, nil
				}
				break
			}
		}
		if it.slabFenceBlocks != nil && (preferFenceEntries || !keyReaderUsable) {
			entries, ok, err := it.slabFenceBlocks.ReadUnsafeFenceBlock(ptr)
			if err != nil {
				return false, err
			}
			if !ok {
				continue
			}
			if len(entries) == 0 {
				continue
			}
			// Ordered, non-overlapping fence layouts dominate production data.
			// If seekKey is beyond this predecessor block's upper bound, earlier
			// predecessor blocks cannot contain it; stop scanning.
			if compareTreeKey(seekKey, entries[len(entries)-1].Key) > 0 {
				break
			}
			if compareTreeKey(seekKey, entries[0].Key) < 0 {
				continue
			}
			pos := lowerBoundFenceEntries(entries, seekKey)
			if pos < len(entries) {
				top.Index = scan
				it.pendingFencePageID = top.PageID
				it.pendingFenceLeafIndex = scan
				it.pendingFenceEntryIdx = pos
				it.pendingFenceReady = true
				it.releasePendingFenceKeyLease()
				// We already selected the predecessor block that contains seekKey.
				// Clearing pendingSeekKey avoids a redundant predecessor rescan on
				// the next loadCurrent loop iteration.
				it.pendingSeekKey = it.pendingSeekKey[:0]
				return true, nil
			}
			break
		}
	}
	return false, nil
}

func (it *Iterator) expandFenceBlockAt(top *CursorItem) (handled bool, produced bool, exhausted bool, err error) {
	if top == nil || top.Node.Type() != page.PageTypeLeaf || (it.slabFenceBlocks == nil && it.slabFenceKeys == nil && it.slabFenceRange == nil && it.slabFenceRangeL == nil) {
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
	if !it.pointerLikelyFenceBlock(ptr) {
		return false, false, false, nil
	}

	var (
		entries  []FenceBlockEntry
		keys     [][]byte
		keyLease FenceKeysLease
		ok       bool
	)
	preferKeyOnlyExpansion := it.shouldPreferFenceKeyOnly() || it.slabFenceBlocks == nil
	if preferKeyOnlyExpansion && (it.slabFenceRange != nil || it.slabFenceRangeL != nil || it.slabFenceKeys != nil) {
		var (
			readKeys  [][]byte
			readLease FenceKeysLease
			keyOK     bool
		)
		if it.pendingFenceReady && top.PageID == it.pendingFencePageID && top.Index == it.pendingFenceLeafIndex && it.pendingFenceKeys != nil {
			readKeys = it.pendingFenceKeys
			readLease = it.pendingFenceKeyLease
			keyOK = true
			it.pendingFenceKeys = nil
			it.pendingFenceKeyLease = nil
		} else {
			var readOK bool
			var keyErr error
			if !it.reverse {
				lower := it.start
				if len(it.pendingSeekKey) > 0 {
					lower = it.pendingSeekKey
				}
				if it.slabFenceRangeL != nil {
					readLease, readOK, keyErr = it.slabFenceRangeL.ReadUnsafeFenceBlockKeysRangeLease(ptr, lower, it.end)
					if readOK && readLease != nil {
						readKeys = readLease.Keys()
					}
				}
				if keyErr == nil && !readOK && it.slabFenceRange != nil {
					readKeys, readOK, keyErr = it.slabFenceRange.ReadUnsafeFenceBlockKeysRange(ptr, lower, it.end)
				}
				if keyErr == nil && !readOK && it.slabFenceKeys != nil {
					readKeys, readOK, keyErr = it.slabFenceKeys.ReadUnsafeFenceBlockKeys(ptr)
				}
			} else if it.slabFenceKeys != nil {
				readKeys, readOK, keyErr = it.slabFenceKeys.ReadUnsafeFenceBlockKeys(ptr)
			}
			if keyErr != nil {
				if readLease != nil {
					readLease.Release()
				}
				return false, false, false, keyErr
			}
			if readOK {
				keyOK = true
			} else if readLease != nil {
				readLease.Release()
				readLease = nil
			}
		}
		if keyOK {
			keys = readKeys
			keyLease = readLease
			ok = true
			it.fenceValuesLazy = it.mode == IteratorModeFull
			it.fenceBlockPtr = ptr
		}
	}
	if !ok {
		if keyLease != nil {
			keyLease.Release()
			keyLease = nil
		}
		if it.slabFenceBlocks == nil {
			return false, false, false, nil
		}
		entries, ok, err = it.slabFenceBlocks.ReadUnsafeFenceBlock(ptr)
		if err != nil {
			return false, false, false, err
		}
		if !ok {
			return false, false, false, nil
		}
		it.fenceValuesLazy = false
		it.fenceBlockPtr = ptr
		keys = nil
	}
	handled = true
	blockLen := len(entries)
	if len(keys) > 0 {
		blockLen = len(keys)
	}
	if blockLen == 0 {
		if keyLease != nil {
			keyLease.Release()
		}
		// loadCurrent() will advance the leaf index when handled && !produced,
		// so empty fence blocks still make forward/reverse progress.
		return handled, false, false, nil
	}

	pos := 0
	if it.pendingFenceReady && top.PageID == it.pendingFencePageID && top.Index == it.pendingFenceLeafIndex {
		pos = it.pendingFenceEntryIdx
	} else if it.reverse {
		pos = blockLen - 1
		if it.end != nil {
			if len(keys) > 0 {
				pos = lowerBoundFenceKeys(keys, it.end) - 1
			} else {
				pos = lowerBoundFenceEntries(entries, it.end) - 1
			}
		}
	} else {
		var lower []byte
		if len(it.pendingSeekKey) > 0 {
			lower = it.pendingSeekKey
		}
		if lower != nil {
			if len(keys) > 0 {
				pos = lowerBoundFenceKeys(keys, lower)
			} else {
				pos = lowerBoundFenceEntries(entries, lower)
			}
		}
	}
	if pos < 0 || pos >= blockLen {
		if keyLease != nil {
			keyLease.Release()
		}
		return handled, false, false, nil
	}

	var posKey []byte
	if len(keys) > 0 {
		posKey = keys[pos]
	} else {
		posKey = entries[pos].Key
	}
	if !it.reverse {
		if it.end != nil && compareTreeKey(posKey, it.end) >= 0 {
			if keyLease != nil {
				keyLease.Release()
			}
			return handled, false, true, nil
		}
	} else if it.start != nil && compareTreeKey(posKey, it.start) < 0 {
		if keyLease != nil {
			keyLease.Release()
		}
		return handled, false, true, nil
	}

	it.fenceEntries = entries
	it.fenceKeys = keys
	it.fenceKeyLease = keyLease
	it.fenceIndex = pos
	it.fenceActive = true
	if len(keys) > 0 {
		it.setCurrentFenceKey(keys[pos])
	} else {
		it.setCurrentFenceEntry(entries[pos])
	}
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
	if it.fenceActive {
		if len(it.fenceKeys) > 0 {
			next := it.fenceIndex + 1
			if it.reverse {
				next = it.fenceIndex - 1
			}
			if next >= 0 && next < len(it.fenceKeys) {
				key := it.fenceKeys[next]
				if !it.reverse {
					if it.end != nil && compareTreeKey(key, it.end) >= 0 {
						it.resetFenceCursor()
						it.valid = false
						return
					}
				} else {
					if it.start != nil && compareTreeKey(key, it.start) < 0 {
						it.resetFenceCursor()
						it.valid = false
						return
					}
				}
				it.fenceIndex = next
				it.setCurrentFenceKey(key)
				return
			}
			it.resetFenceCursor()
		} else if len(it.fenceEntries) > 0 {
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
	if it.fenceActive && it.fenceValuesLazy {
		if !it.ensureFenceValueLoaded() {
			return nil
		}
		return it.currVal
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
	if it.fenceActive && it.fenceValuesLazy {
		if !it.ensureFenceValueLoaded() {
			return nil, page.ValuePtr{}, it.flags
		}
		return it.currVal, page.ValuePtr{}, it.flags
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
	if it == nil || it.tree == nil {
		return nil
	}
	it.releaseFenceKeyLease()
	it.releasePendingFenceKeyLease()
	buf := it.trimReusableBuffers()
	*it = Iterator{}
	it.installReusableBuffers(buf)
	iteratorPool.Put(it)
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

func (it *Iterator) ensureFenceValueLoaded() bool {
	if it.valOK || !it.fenceActive || !it.fenceValuesLazy {
		return true
	}
	if it.slabFenceBlocks == nil {
		it.err = fmt.Errorf("iterator fence value load: block reader unavailable")
		it.valid = false
		return false
	}
	entries, ok, err := it.slabFenceBlocks.ReadUnsafeFenceBlock(it.fenceBlockPtr)
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	if !ok {
		it.err = fmt.Errorf("iterator fence value load: block unavailable")
		it.valid = false
		return false
	}
	if len(entries) == 0 {
		it.err = fmt.Errorf("iterator fence value load: empty block")
		it.valid = false
		return false
	}
	if it.fenceIndex < 0 || it.fenceIndex >= len(entries) {
		it.err = fmt.Errorf("iterator fence value load: index out of range %d/%d", it.fenceIndex, len(entries))
		it.valid = false
		return false
	}

	// Keep the expanded block for subsequent entries in this run so we only pay
	// one full decode when values are actually requested.
	it.fenceEntries = entries
	it.fenceKeys = nil
	it.fenceValuesLazy = false
	entry := entries[it.fenceIndex]
	if compareTreeKey(entry.Key, it.currKey) != 0 {
		pos := lowerBoundFenceEntries(entries, it.currKey)
		if pos < 0 || pos >= len(entries) {
			it.releaseFenceKeyLease()
			it.err = fmt.Errorf("iterator fence value load: key not found in fence block")
			it.valid = false
			return false
		}
		if compareTreeKey(entries[pos].Key, it.currKey) != 0 {
			it.releaseFenceKeyLease()
			it.err = fmt.Errorf("iterator fence value load: fence index out of sync")
			it.valid = false
			return false
		}
		it.fenceIndex = pos
		entry = entries[pos]
	}
	it.currKey = entry.Key
	it.releaseFenceKeyLease()
	it.currVal = entry.Value
	it.valOK = true
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
	needsKeys := it.slabKeyBatcher != nil || it.slabKeyAppender != nil || it.slabKeyReader != nil

	for idx := top.Index; idx >= 0 && idx < count && len(it.prefetchPtrs) < iteratorPointerBatchMax; idx += step {
		var (
			key   []byte
			ptr   page.ValuePtr
			flags byte
			err   error
		)
		if needsKeys {
			key, _, ptr, flags, err = top.Node.GetLeafEntryView(uint16(idx))
		} else {
			_, ptr, flags, err = top.Node.GetLeafValueView(uint16(idx))
		}
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
		if needsKeys {
			// GetLeafEntryView may return a scratch-backed key for prefix-compressed
			// leaves; copy so batched key-aware pointer reads see stable bytes.
			it.prefetchKeys = append(it.prefetchKeys, append([]byte(nil), key...))
		}
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
