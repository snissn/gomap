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
	Ref    page.ChildRef
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
	iteratorPoolMaxStackCap     = 128
	iteratorPoolMaxPrefetchCap  = 32
	iteratorPoolMaxScratchBytes = 256 << 10 // 256 KiB
)

type iteratorReusableBuffers struct {
	stack          []CursorItem
	prefetchPtrs   []page.ValuePtr
	prefetchKeys   [][]byte
	prefetchVals   [][]byte
	ptrScratch     []byte
	leafRefScratch []byte
	keyScratch     []byte
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
	// IncludeTombstones keeps tombstone rows visible to iteration callers.
	// Default false preserves user-facing behavior (tombstones hidden).
	IncludeTombstones bool
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
	if size == 0 {
		return []byte{}
	}
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
	leafRefScratch  []byte
	slabAppender    slabUnsafeAppender
	slabBatcher     slabUnsafeBatchAppender
	slabKeyReader   slabUnsafeKeyReader
	slabKeyAppender slabUnsafeKeyAppender
	slabKeyBatcher  slabUnsafeKeyBatchAppender

	prefetchRef       page.ChildRef
	prefetchStart     int
	prefetchLen       int
	prefetchStep      int
	prefetchPtrs      []page.ValuePtr
	prefetchKeys      [][]byte
	prefetchVals      [][]byte
	prefetchArmed     bool
	mode              IteratorMode
	includeTombstones bool
	reverse           bool
	emptyDomain       bool
	verifyAlways      bool
}

const (
	iteratorPointerBatchMax = 2
	// When iteration reaches a grouped value-log record, prefetch a larger
	// same-record run so checksum-verified scans validate the record once for the
	// operation-local batch instead of once per small pair. Non-grouped or large
	// records stay on the conservative two-value lookahead path to avoid broad
	// read-ahead memory growth for callers that may stop early.
	iteratorGroupedRecordBatchMax        = 16
	iteratorGroupedRecordPrefetchMaxHint = 1 << 20 // 1 MiB record-length hint
)

func iteratorGroupedRecordPrefetchEligible(ptr page.ValuePtr) bool {
	if !page.ValuePtrIsGrouped(ptr) {
		return false
	}
	recordLen := page.ValuePtrRecordLength(ptr)
	return recordLen > 0 && recordLen <= iteratorGroupedRecordPrefetchMaxHint
}

func iteratorSamePrefetchGroupedRecord(first, ptr page.ValuePtr) bool {
	if !iteratorGroupedRecordPrefetchEligible(first) || !page.ValuePtrIsGrouped(ptr) {
		return false
	}
	return ptr.FileID == first.FileID &&
		ptr.Offset == first.Offset &&
		page.ValuePtrRecordLength(ptr) == page.ValuePtrRecordLength(first)
}

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
		stack:          it.stack,
		prefetchPtrs:   it.prefetchPtrs,
		prefetchKeys:   it.prefetchKeys,
		prefetchVals:   it.prefetchVals,
		ptrScratch:     it.ptrScratch,
		leafRefScratch: it.leafRefScratch,
		keyScratch:     it.leafState.keyScratch,
	}
}

func (it *Iterator) trimReusableBuffers() iteratorReusableBuffers {
	buf := iteratorReusableBuffers{}
	if cap(it.stack) > len(it.stackBuf) {
		buf.stack = retainSliceCap(it.stack, iteratorPoolMaxStackCap)
	}
	buf.prefetchPtrs = retainSliceCap(it.prefetchPtrs, iteratorPoolMaxPrefetchCap)
	buf.prefetchKeys = retainSliceCap(it.prefetchKeys, iteratorPoolMaxPrefetchCap)
	buf.prefetchVals = retainSliceCap(it.prefetchVals, iteratorPoolMaxPrefetchCap)
	buf.ptrScratch = retainSliceCap(it.ptrScratch, iteratorPoolMaxScratchBytes)
	buf.leafRefScratch = retainSliceCap(it.leafRefScratch, page.PageSize)
	buf.keyScratch = retainSliceCap(it.leafState.keyScratch, iteratorPoolMaxScratchBytes)
	return buf
}

func (it *Iterator) installReusableBuffers(buf iteratorReusableBuffers) {
	if cap(buf.stack) > 0 {
		it.stack = buf.stack[:0]
	} else {
		it.resetStack()
	}
	it.prefetchPtrs = buf.prefetchPtrs[:0]
	it.prefetchKeys = buf.prefetchKeys[:0]
	it.prefetchVals = buf.prefetchVals[:0]
	it.ptrScratch = buf.ptrScratch[:0]
	it.leafRefScratch = buf.leafRefScratch[:0]
	it.leafState.keyScratch = buf.keyScratch[:0]
}

func (t *Tree) acquireIterator(start, end []byte, mode IteratorMode, includeTombstones bool, reverse bool) *Iterator {
	it := iteratorPool.Get().(*Iterator)
	buf := it.captureReusableBuffers()
	*it = Iterator{
		tree:              t,
		start:             start,
		end:               end,
		mode:              mode,
		includeTombstones: includeTombstones,
		reverse:           reverse,
		emptyDomain:       start != nil && end != nil && compareTreeKey(start, end) >= 0,
		verifyAlways:      t.pager != nil && t.pager.VerifyOnRead(),
		slabAppender:      t.slabAppender,
		slabBatcher:       t.slabBatcher,
		slabKeyReader:     t.slabKeyReader,
		slabKeyAppender:   t.slabKeyAppender,
		slabKeyBatcher:    t.slabKeyBatcher,
	}
	it.installReusableBuffers(buf)
	return it
}

// IteratorWithOptions returns a forward iterator over [start, end) using the
// provided value materialization mode.
func (t *Tree) IteratorWithOptions(start, end []byte, opts IteratorOptions) iterator.UnsafeIterator {
	mode := normalizeIteratorMode(opts.Mode)
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return t.acquireIterator(start, end, mode, opts.IncludeTombstones, false)
	}
	it := t.acquireIterator(start, end, mode, opts.IncludeTombstones, false)
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
		return t.acquireIterator(start, end, mode, opts.IncludeTombstones, true)
	}
	it := t.acquireIterator(start, end, mode, opts.IncludeTombstones, true)
	it.Seek(nil)

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
	it.prefetchArmed = false
	it.leafState.resetPage()
	it.valid = false
	it.err = nil
	currRef := page.PageChildRef(it.tree.rootPageID)

	for {
		n, err := it.loadNodeRef(currRef)
		if err != nil {
			it.err = err
			return
		}

		if n.Count() == 0 {
			it.valid = false
			return
		}

		index := int(n.Count() - 1)
		it.stack = append(it.stack, CursorItem{PageID: currRef.Page, Ref: currRef, Node: n, Index: index})

		if n.Type() == page.PageTypeLeaf {
			it.loadCurrent()
			return
		} else if n.Type() == page.PageTypeInternal {
			childRef, err := n.GetInternalChildRef(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currRef = childRef
		} else {
			it.err = page.ErrInvalidPageType
			return
		}
	}
}

func (it *Iterator) seek(key []byte) {
	it.resetStack()
	it.resetPointerPrefetch()
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

	currRef := page.PageChildRef(it.tree.rootPageID)

	for {
		n, err := it.loadNodeRef(currRef)
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

			it.stack = append(it.stack, CursorItem{PageID: currRef.Page, Ref: currRef, Node: n, Index: index})

			childRef, err := n.GetInternalChildRef(uint16(index))
			if err != nil {
				it.err = err
				return
			}
			currRef = childRef

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
			it.stack = append(it.stack, CursorItem{PageID: currRef.Page, Ref: currRef, Node: n, Index: index})

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

		// Forward scans can stop as soon as the physical leaf key reaches the
		// upper bound because all following keys are >= this key.
		if !it.reverse && it.end != nil && compareTreeKey(keyView, it.end) >= 0 {
			it.valid = false
			return
		}

		// By default tombstones are hidden from iteration. Internal maintenance
		// paths can opt in to keep them visible via IncludeTombstones.
		if !it.includeTombstones && flags&node.FlagTombstone != 0 {
			if it.reverse {
				top.Index--
			} else {
				top.Index++
			}
			continue
		}

		if it.reverse && it.start != nil && compareTreeKey(keyView, it.start) < 0 {
			it.valid = false
			return
		}

		it.currKey = keyView
		it.flags = flags
		it.currPtr = page.ValuePtr{}
		it.ptrOK = flags&node.FlagPointer == 0
		it.currVal = nil
		it.valOK = it.mode == IteratorModeKeysOnly

		it.valid = true
		return
	}
}

func (it *Iterator) getLeafKeyFlags(top *CursorItem) (key []byte, flags byte, err error) {
	if top.Node.Type() != page.PageTypeLeaf {
		return nil, 0, node.ErrInvalidType
	}
	if top.Ref.Kind == page.ChildRefLeafLog {
		return top.Node.GetLeafKeyFlagsView(uint16(top.Index))
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

			childRef, err := top.Node.GetInternalChildRef(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currRef := childRef

			for {
				n, err := it.loadNodeRef(currRef)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}

				item := CursorItem{PageID: currRef.Page, Ref: currRef, Node: n, Index: 0}
				it.stack = append(it.stack, item)

				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}

				childRef, err := n.GetInternalChildRef(0)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currRef = childRef
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

			childRef, err := top.Node.GetInternalChildRef(uint16(top.Index))
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			currRef := childRef

			for {
				n, err := it.loadNodeRef(currRef)
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				count := int(n.Count())
				item := CursorItem{PageID: currRef.Page, Ref: currRef, Node: n, Index: count - 1}
				it.stack = append(it.stack, item)

				if count == 0 {
					// Defensive: empty pages can appear transiently during churny
					// delete-heavy workloads. Forward iteration implicitly skips them
					// via bounds checks + stepForward; mirror that behavior here by
					// letting loadCurrent/stepBackward unwind and continue.
					it.loadCurrent()
					return
				}

				if n.Type() == page.PageTypeLeaf {
					it.loadCurrent()
					return
				}

				childRef, err := n.GetInternalChildRef(uint16(count - 1))
				if err != nil {
					it.err = err
					it.valid = false
					return
				}
				currRef = childRef
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
		if it.prefetchArmed {
			if it.prefetchPointerRun() && it.tryUsePrefetchedPointerValue() {
				return it.currVal
			}
			if it.err != nil || !it.valid {
				return nil
			}
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

func (it *Iterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if it == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	val, ptr, flags := it.UnsafeEntry()
	if !it.valid || len(it.stack) == 0 {
		return val, ptr, flags, page.LegacyEntryRevision
	}
	top := &it.stack[len(it.stack)-1]
	if top.Node.Type() != page.PageTypeLeaf || top.Index < 0 || top.Index >= int(top.Node.Count()) {
		return val, ptr, flags, page.LegacyEntryRevision
	}
	_, _, _, _, revision, err := top.Node.GetLeafEntryViewWithRevision(uint16(top.Index))
	if err != nil {
		it.err = err
		it.valid = false
		return val, ptr, flags, page.LegacyEntryRevision
	}
	return val, ptr, flags, revision
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
	buf := it.trimReusableBuffers()
	*it = Iterator{}
	it.installReusableBuffers(buf)
	iteratorPool.Put(it)
	return nil
}

func (it *Iterator) Seek(key []byte) {
	if it.emptyDomain {
		return
	}
	if it.reverse {
		it.seekReverse(key)
		return
	}
	if it.start != nil && (key == nil || compareTreeKey(key, it.start) < 0) {
		key = it.start
	}
	it.seek(key)
	// Check bounds? Handled in loadCurrent
}

func (it *Iterator) seekReverse(key []byte) {
	if key == nil || (it.end != nil && compareTreeKey(key, it.end) >= 0) {
		if it.end == nil {
			it.seekRightMost()
		} else {
			it.seek(it.end)
			if it.valid {
				if compareTreeKey(it.currKey, it.end) >= 0 {
					it.stepBackward()
				}
			} else if it.err == nil {
				it.seekRightMost()
			}
		}
		if it.valid && it.start != nil && compareTreeKey(it.currKey, it.start) < 0 {
			it.valid = false
		}
		return
	}

	it.seek(key)
	if it.valid {
		if compareTreeKey(it.currKey, key) > 0 {
			it.stepBackward()
		}
	} else if it.err == nil && (it.start == nil || compareTreeKey(key, it.start) >= 0) {
		it.seekRightMost()
	}
	if it.valid && it.start != nil && compareTreeKey(it.currKey, it.start) < 0 {
		it.valid = false
	}
}

func (it *Iterator) IsDeleted() bool {
	return it != nil && it.valid && it.flags&node.FlagTombstone != 0
}

func (it *Iterator) Domain() (start, end []byte) {
	return it.start, it.end
}

func (it *Iterator) loadNode(pageID uint64) (node.Node, error) {
	return it.loadNodeRef(page.PageChildRef(pageID))
}

func (it *Iterator) loadNodeRef(ref page.ChildRef) (node.Node, error) {
	if it == nil || it.tree == nil {
		return node.Node{}, errors.New("missing tree")
	}
	if ref.Kind == page.ChildRefLeafLog && (it.tree.leafLogToState != nil || it.tree.leafLogToReader != nil) {
		ptr := ref.Log
		if cap(it.leafRefScratch) != page.PageSize {
			it.leafRefScratch = make([]byte, 0, page.PageSize)
		}
		var (
			data    []byte
			usedDst bool
			state   LeafLogPageReadState
			err     error
		)
		if it.tree.leafLogToState != nil {
			data, usedDst, state, err = it.tree.leafLogToState.ReadLeafLogPageUnsafeToWithState(ptr, it.leafRefScratch[:0])
		} else {
			data, usedDst, err = it.tree.leafLogToReader.ReadLeafLogPageUnsafeTo(ptr, it.leafRefScratch[:0])
		}
		if err != nil {
			return node.Node{}, err
		}
		var n node.Node
		verifiedNow, err := validateLeafLogNodeIntoWithState(&n, data, ptr, it.tree.shouldVerifyLeafRefChecksum(), true, state)
		if err != nil {
			it.leafRefScratch = it.leafRefScratch[:0]
			return node.Node{}, err
		}
		if verifiedNow && state.RecordChecksumVerified && state.CacheEntryPresent {
			it.tree.markLeafLogPageChecksumVerified(ptr)
		}
		if usedDst {
			it.leafRefScratch = data[:0]
		} else {
			it.leafRefScratch = it.leafRefScratch[:0]
		}
		return n, nil
	}
	if ref.Kind == page.ChildRefLeafLog && it.slabAppender != nil {
		ptr := ref.Log
		if cap(it.leafRefScratch) != page.PageSize {
			it.leafRefScratch = make([]byte, 0, page.PageSize)
		}
		data, err := it.slabAppender.ReadUnsafeAppend(ptr.ValuePtr(), it.leafRefScratch[:0])
		if err != nil {
			return node.Node{}, err
		}
		n, err := validateLeafLogNode(data, ptr, it.tree.shouldVerifyLeafRefChecksum(), true)
		if err != nil {
			it.leafRefScratch = it.leafRefScratch[:0]
			return node.Node{}, err
		}
		it.leafRefScratch = data[:0]
		return n, nil
	}
	return it.tree.loadChildRefView(ref, it.verifyAlways, true)
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
	it.prefetchRef = page.ChildRef{}
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
	if it.prefetchRef != top.Ref {
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

	it.prefetchRef = top.Ref
	it.prefetchStart = top.Index
	it.prefetchLen = 0
	it.prefetchStep = step
	it.prefetchPtrs = it.prefetchPtrs[:0]
	it.prefetchKeys = it.prefetchKeys[:0]
	needsKeys := it.slabKeyBatcher != nil || it.slabKeyAppender != nil || it.slabKeyReader != nil
	needsBoundsKey := (!it.reverse && it.end != nil) || (it.reverse && it.start != nil)

	limit := iteratorPointerBatchMax
	var firstPtr page.ValuePtr
	for idx := top.Index; idx >= 0 && idx < count; idx += step {
		if len(it.prefetchPtrs) >= limit {
			break
		}
		var (
			key   []byte
			ptr   page.ValuePtr
			flags byte
			err   error
		)
		if needsKeys || needsBoundsKey {
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
		if needsBoundsKey {
			if !it.reverse && it.end != nil && compareTreeKey(key, it.end) >= 0 {
				if len(it.prefetchPtrs) == 0 {
					return false
				}
				break
			}
			if it.reverse && it.start != nil && compareTreeKey(key, it.start) < 0 {
				if len(it.prefetchPtrs) == 0 {
					return false
				}
				break
			}
		}
		if flags&node.FlagPointer == 0 || flags&node.FlagTombstone != 0 {
			if len(it.prefetchPtrs) == 0 {
				return false
			}
			break
		}
		if len(it.prefetchPtrs) == 0 {
			firstPtr = ptr
			if iteratorGroupedRecordPrefetchEligible(firstPtr) {
				limit = iteratorGroupedRecordBatchMax
			}
		} else if len(it.prefetchPtrs) >= iteratorPointerBatchMax && !iteratorSamePrefetchGroupedRecord(firstPtr, ptr) {
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
