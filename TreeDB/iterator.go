package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	cosmosdb "github.com/cosmos/cosmos-db"

	"treedb/internal/mvcc"
	"treedb/internal/page"
	"treedb/internal/pager"
)

const (
	userKeyPrefix  byte = 0x01
	slotHeaderSize      = 4
)

type cursorItem struct {
	pid   page.PageID
	ref   *pager.PageRef
	h     *page.Header
	body  []byte
	count int
	index int
}

type iterator struct {
	db      *DB
	snap    *mvcc.Snapshot
	st      *mvcc.DBState
	start   []byte
	end     []byte
	encFrom []byte
	encTo   []byte
	reverse bool

	stack  []cursorItem
	valid  bool
	closed bool
	err    error

	cacheValid     bool
	cacheLeafPID   page.PageID
	cacheLeafIndex int
	cacheKey       []byte
	cacheFlags     page.LeafFlags
	cacheInline    []byte
	cachePtr       page.ValuePtr

	boundsValid     bool
	boundsLeafPID   page.PageID
	boundsLeafLimit int
	boundsLeafMin   int

	copyBuf []byte
	copyOff int
}

func newIterator(db *DB, snap *mvcc.Snapshot, start, end []byte, reverse bool) *iterator {
	it := &iterator{
		db:      db,
		snap:    snap,
		st:      snap.State(),
		start:   append([]byte(nil), start...),
		end:     append([]byte(nil), end...),
		reverse: reverse,
	}
	if start != nil {
		it.encFrom = encodeUserKey(start)
	}
	if end != nil {
		it.encTo = encodeUserKey(end)
	}
	return it
}

func (it *iterator) popCursorItem() {
	if it == nil || len(it.stack) == 0 {
		return
	}
	last := len(it.stack) - 1
	pid := it.stack[last].pid
	if it.stack[last].ref != nil {
		it.stack[last].ref.Release()
		it.stack[last].ref = nil
	}
	if it.cacheValid && it.cacheLeafPID == pid {
		it.cacheValid = false
	}
	if it.boundsValid && it.boundsLeafPID == pid {
		it.boundsValid = false
	}
	it.stack = it.stack[:last]
}

func (it *iterator) truncateCursorStack(n int) {
	if it == nil {
		return
	}
	if n < 0 {
		n = 0
	}
	if n > len(it.stack) {
		n = len(it.stack)
	}
	for i := n; i < len(it.stack); i++ {
		pid := it.stack[i].pid
		if it.stack[i].ref != nil {
			it.stack[i].ref.Release()
			it.stack[i].ref = nil
		}
		if it.cacheValid && it.cacheLeafPID == pid {
			it.cacheValid = false
		}
		if it.boundsValid && it.boundsLeafPID == pid {
			it.boundsValid = false
		}
	}
	it.stack = it.stack[:n]
}

func (it *iterator) Domain() (start, end []byte) {
	if it == nil {
		return nil, nil
	}
	return append([]byte(nil), it.start...), append([]byte(nil), it.end...)
}

func (it *iterator) Valid() bool {
	if it == nil {
		return false
	}
	return it.valid
}

func (it *iterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *iterator) Close() error {
	if it == nil {
		return nil
	}
	if it.closed {
		return nil
	}
	it.closed = true
	it.valid = false
	it.cacheValid = false
	it.boundsValid = false
	it.copyBuf = nil
	it.copyOff = 0
	it.truncateCursorStack(0)
	if it.snap != nil {
		return it.snap.Close()
	}
	return nil
}

func (it *iterator) Next() {
	if it == nil || !it.Valid() {
		panic("treedb: Next on invalid iterator")
	}
	if it.reverse {
		it.retreatReverse()
		it.skipTombstonesReverse()
		it.enforceReverseBounds()
	} else {
		it.advanceForward()
		it.skipTombstonesForward()
		it.enforceForwardBounds()
	}
}

func (it *iterator) Key() []byte {
	if it == nil || !it.Valid() {
		panic("treedb: Key on invalid iterator")
	}
	key, _, _, _, err := it.currentLeafEntry()
	if err != nil {
		it.err = err
		it.valid = false
		panic(err)
	}
	if len(key) == 0 {
		return nil
	}
	if key[0] == userKeyPrefix {
		dst := it.allocCopy(len(key) - 1)
		copy(dst, key[1:])
		return dst
	}
	dst := it.allocCopy(len(key))
	copy(dst, key)
	return dst
}

func (it *iterator) Value() []byte {
	if it == nil || !it.Valid() {
		panic("treedb: Value on invalid iterator")
	}
	_, flags, inline, ptr, err := it.currentLeafEntry()
	if err != nil {
		it.err = err
		it.valid = false
		panic(err)
	}
	switch flags {
	case page.LeafFlagInline:
		dst := it.allocCopy(len(inline))
		copy(dst, inline)
		return dst
	case page.LeafFlagPointer:
		val, err := it.db.readPtr(ptr, it.st.SlabSet)
		if err != nil {
			it.err = err
			it.valid = false
			panic(err)
		}
		return val[:len(val):len(val)]
	default:
		dst := it.allocCopy(len(inline))
		copy(dst, inline)
		return dst
	}
}

const iteratorCopyChunkSize = 1024

func (it *iterator) allocCopy(n int) []byte {
	if it == nil || n == 0 {
		return nil
	}
	if it.copyBuf == nil || len(it.copyBuf)-it.copyOff < n {
		sz := iteratorCopyChunkSize
		if n > sz {
			sz = n
		}
		it.copyBuf = make([]byte, sz)
		it.copyOff = 0
	}
	start := it.copyOff
	end := start + n
	it.copyOff = end
	return it.copyBuf[start:end:end]
}

func (it *iterator) initForward() {
	root := it.st.UserRootPageID
	if root == 0 {
		it.valid = false
		it.cacheValid = false
		return
	}
	var err error
	if it.encFrom == nil {
		err = it.seekFirst(root)
	} else {
		err = it.search(root, it.encFrom)
		if err == nil {
			it.normalizeForward()
		}
	}
	if err != nil {
		it.err = err
		it.valid = false
		it.cacheValid = false
		return
	}
	it.skipTombstonesForward()
	it.enforceForwardBounds()
}

func (it *iterator) initReverse() {
	root := it.st.UserRootPageID
	if root == 0 {
		it.valid = false
		it.cacheValid = false
		return
	}
	var err error
	if it.encTo == nil {
		err = it.seekLast(root)
	} else {
		err = it.search(root, it.encTo)
		if err == nil {
			it.retreatReverse()
		}
	}
	if err != nil {
		it.err = err
		it.valid = false
		it.cacheValid = false
		return
	}
	it.skipTombstonesReverse()
	it.enforceReverseBounds()
}

func (it *iterator) enforceForwardBounds() {
	if !it.Valid() {
		return
	}
	if it.encTo == nil {
		return
	}
	it.refreshLeafBounds()
}

func (it *iterator) enforceReverseBounds() {
	if !it.Valid() {
		return
	}
	if it.encFrom == nil {
		return
	}
	it.refreshLeafBounds()
}

func (it *iterator) refreshLeafBounds() {
	if it == nil || !it.Valid() {
		return
	}
	if len(it.stack) == 0 {
		it.valid = false
		it.cacheValid = false
		it.boundsValid = false
		return
	}
	top := &it.stack[len(it.stack)-1]
	if top.h.Flags != page.PageTypeLeaf {
		it.boundsValid = false
		return
	}
	if it.boundsValid && it.boundsLeafPID == top.pid {
		if it.reverse {
			if top.index < it.boundsLeafMin {
				it.valid = false
			}
			return
		}
		if top.index >= it.boundsLeafLimit {
			it.valid = false
		}
		return
	}

	it.boundsValid = true
	it.boundsLeafPID = top.pid
	it.boundsLeafLimit = top.count
	it.boundsLeafMin = 0

	if it.reverse {
		if it.encFrom != nil && top.count > 0 {
			firstKey, err := leafKeyAt(top.body, top.count, 0)
			if err != nil {
				it.err = err
				it.valid = false
				it.cacheValid = false
				it.boundsValid = false
				return
			}
			if bytes.Compare(firstKey, it.encFrom) < 0 {
				idx, _, err := findLeafIndex(top.body, top.count, it.encFrom)
				if err != nil {
					it.err = err
					it.valid = false
					it.cacheValid = false
					it.boundsValid = false
					return
				}
				it.boundsLeafMin = idx
			}
		}
		if top.index < it.boundsLeafMin {
			it.valid = false
		}
		return
	}

	if it.encTo != nil && top.count > 0 {
		lastKey, err := leafKeyAt(top.body, top.count, top.count-1)
		if err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			it.boundsValid = false
			return
		}
		if bytes.Compare(lastKey, it.encTo) >= 0 {
			idx, _, err := findLeafIndex(top.body, top.count, it.encTo)
			if err != nil {
				it.err = err
				it.valid = false
				it.cacheValid = false
				it.boundsValid = false
				return
			}
			it.boundsLeafLimit = idx
		}
	}
	if top.index >= it.boundsLeafLimit {
		it.valid = false
	}
}

func (it *iterator) seekFirst(root page.PageID) error {
	it.truncateCursorStack(0)
	pid := root
	for {
		item, err := it.readCursorItem(pid)
		if err != nil {
			return err
		}
		switch item.h.Flags {
		case page.PageTypeLeaf:
			item.index = 0
			it.stack = append(it.stack, item)
			if item.count == 0 {
				it.valid = false
				it.cacheValid = false
			} else {
				it.valid = true
			}
			return nil
		case page.PageTypeInternal:
			if item.count == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			childPid, err := internalChildAt(item.body, item.count, 0)
			if err != nil {
				item.ref.Release()
				return err
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = childPid
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) seekLast(root page.PageID) error {
	it.truncateCursorStack(0)
	pid := root
	for {
		item, err := it.readCursorItem(pid)
		if err != nil {
			return err
		}
		switch item.h.Flags {
		case page.PageTypeLeaf:
			item.index = item.count - 1
			it.stack = append(it.stack, item)
			if item.count == 0 {
				it.valid = false
				it.cacheValid = false
			} else {
				it.valid = true
			}
			return nil
		case page.PageTypeInternal:
			if item.count == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = item.count - 1
			childPid, err := internalChildAt(item.body, item.count, item.index)
			if err != nil {
				item.ref.Release()
				return err
			}
			it.stack = append(it.stack, item)
			pid = childPid
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) search(root page.PageID, key []byte) error {
	it.truncateCursorStack(0)
	pid := root
	for {
		item, err := it.readCursorItem(pid)
		if err != nil {
			return err
		}
		switch item.h.Flags {
		case page.PageTypeLeaf:
			idx, _, err := findLeafIndex(item.body, item.count, key)
			if err != nil {
				item.ref.Release()
				return err
			}
			item.index = idx
			it.stack = append(it.stack, item)
			if idx < item.count {
				it.valid = true
			} else {
				it.valid = false
				it.cacheValid = false
			}
			return nil
		case page.PageTypeInternal:
			if item.count == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			childIdx, err := findInternalChildIndex(item.body, item.count, key)
			if err != nil {
				item.ref.Release()
				return err
			}
			if childIdx < 0 || childIdx >= item.count {
				item.ref.Release()
				return fmt.Errorf("treedb: child index out of bounds")
			}
			childPid, err := internalChildAt(item.body, item.count, childIdx)
			if err != nil {
				item.ref.Release()
				return err
			}
			item.index = childIdx
			it.stack = append(it.stack, item)
			pid = childPid
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) advanceForward() {
	if len(it.stack) == 0 {
		it.valid = false
		it.cacheValid = false
		return
	}
	top := &it.stack[len(it.stack)-1]
	top.index++
	it.normalizeForward()
}

func (it *iterator) normalizeForward() {
	for len(it.stack) > 0 {
		topIdx := len(it.stack) - 1
		top := &it.stack[topIdx]
		if top.h.Flags == page.PageTypeLeaf {
			if top.index < top.count {
				it.valid = true
				return
			}
			it.popCursorItem()
			if len(it.stack) == 0 {
				it.valid = false
				it.cacheValid = false
				return
			}
			parent := &it.stack[len(it.stack)-1]
			parent.index++
			for {
				if parent.index < parent.count {
					childPid, err := internalChildAt(parent.body, parent.count, parent.index)
					if err != nil {
						it.err = err
						it.valid = false
						it.cacheValid = false
						return
					}
					if err := it.drillDownLeft(childPid); err != nil {
						it.err = err
						it.valid = false
						it.cacheValid = false
						return
					}
					break
				}
				it.popCursorItem()
				if len(it.stack) == 0 {
					it.valid = false
					it.cacheValid = false
					return
				}
				parent = &it.stack[len(it.stack)-1]
				parent.index++
			}
			continue
		}

		if top.index >= top.count {
			it.popCursorItem()
			continue
		}
		childPid, err := internalChildAt(top.body, top.count, top.index)
		if err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
		if err := it.drillDownLeft(childPid); err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
	}
	it.valid = false
	it.cacheValid = false
}

func (it *iterator) retreatReverse() {
	if len(it.stack) == 0 {
		it.valid = false
		it.cacheValid = false
		return
	}
	top := &it.stack[len(it.stack)-1]
	top.index--
	it.normalizeReverse()
}

func (it *iterator) normalizeReverse() {
	for len(it.stack) > 0 {
		topIdx := len(it.stack) - 1
		top := &it.stack[topIdx]
		if top.h.Flags == page.PageTypeLeaf {
			if top.index >= 0 {
				it.valid = true
				return
			}
			it.popCursorItem()
			if len(it.stack) == 0 {
				it.valid = false
				it.cacheValid = false
				return
			}
			parent := &it.stack[len(it.stack)-1]
			parent.index--
			for {
				if parent.index >= 0 {
					childPid, err := internalChildAt(parent.body, parent.count, parent.index)
					if err != nil {
						it.err = err
						it.valid = false
						it.cacheValid = false
						return
					}
					if err := it.drillDownRight(childPid); err != nil {
						it.err = err
						it.valid = false
						it.cacheValid = false
						return
					}
					break
				}
				it.popCursorItem()
				if len(it.stack) == 0 {
					it.valid = false
					it.cacheValid = false
					return
				}
				parent = &it.stack[len(it.stack)-1]
				parent.index--
			}
			continue
		}

		if top.index < 0 {
			it.popCursorItem()
			continue
		}
		childPid, err := internalChildAt(top.body, top.count, top.index)
		if err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
		if err := it.drillDownRight(childPid); err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
	}
	it.valid = false
	it.cacheValid = false
}

func (it *iterator) drillDownLeft(pid page.PageID) error {
	for {
		item, err := it.readCursorItem(pid)
		if err != nil {
			return err
		}
		switch item.h.Flags {
		case page.PageTypeLeaf:
			item.index = 0
			it.stack = append(it.stack, item)
			return nil
		case page.PageTypeInternal:
			if item.count == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			childPid, err := internalChildAt(item.body, item.count, 0)
			if err != nil {
				item.ref.Release()
				return err
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = childPid
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) drillDownRight(pid page.PageID) error {
	for {
		item, err := it.readCursorItem(pid)
		if err != nil {
			return err
		}
		switch item.h.Flags {
		case page.PageTypeLeaf:
			item.index = item.count - 1
			it.stack = append(it.stack, item)
			return nil
		case page.PageTypeInternal:
			if item.count == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = item.count - 1
			childPid, err := internalChildAt(item.body, item.count, item.index)
			if err != nil {
				item.ref.Release()
				return err
			}
			it.stack = append(it.stack, item)
			pid = childPid
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) skipTombstonesForward() {
	for it.Valid() {
		_, flags, _, _, err := it.currentLeafEntry()
		if err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
		if flags != page.LeafFlagTombstone {
			return
		}
		it.advanceForward()
		it.enforceForwardBounds()
	}
}

func (it *iterator) skipTombstonesReverse() {
	for it.Valid() {
		_, flags, _, _, err := it.currentLeafEntry()
		if err != nil {
			it.err = err
			it.valid = false
			it.cacheValid = false
			return
		}
		if flags != page.LeafFlagTombstone {
			return
		}
		it.retreatReverse()
		it.enforceReverseBounds()
	}
}

func (it *iterator) currentLeafEntry() ([]byte, page.LeafFlags, []byte, page.ValuePtr, error) {
	if len(it.stack) == 0 {
		return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: empty cursor stack")
	}
	top := &it.stack[len(it.stack)-1]
	if top.h.Flags != page.PageTypeLeaf {
		return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: top is not leaf")
	}
	if top.index < 0 || top.index >= top.count {
		return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: leaf index out of bounds")
	}
	if it.cacheValid && it.cacheLeafPID == top.pid && it.cacheLeafIndex == top.index {
		return it.cacheKey, it.cacheFlags, it.cacheInline, it.cachePtr, nil
	}
	off, err := dirEntry(top.body, top.count, top.index)
	if err != nil {
		it.cacheValid = false
		return nil, 0, nil, page.ValuePtr{}, err
	}
	key, flags, inline, ptr, _, err := decodeLeafEntry(top.body, off)
	if err != nil {
		it.cacheValid = false
		return nil, 0, nil, page.ValuePtr{}, err
	}
	it.cacheValid = true
	it.cacheLeafPID = top.pid
	it.cacheLeafIndex = top.index
	it.cacheKey = key
	it.cacheFlags = flags
	it.cacheInline = inline
	it.cachePtr = ptr
	return key, flags, inline, ptr, nil
}

func (it *iterator) readCursorItem(pid page.PageID) (cursorItem, error) {
	ref, err := it.db.pager.ReadPageRef(pid)
	if err != nil {
		return cursorItem{}, err
	}
	buf := ref.Bytes()
	h, body, err := page.SplitPage(buf)
	if err != nil {
		ref.Release()
		return cursorItem{}, err
	}
	if it.db == nil || it.db.pager == nil || !it.db.pager.IsPageVerified(pid) {
		if err := h.VerifyBodyCRC(body); err != nil {
			ref.Release()
			return cursorItem{}, err
		}
		if it.db != nil && it.db.pager != nil {
			it.db.pager.MarkPageVerified(pid)
		}
	}
	return cursorItem{
		pid:   pid,
		ref:   ref,
		h:     h,
		body:  body,
		count: int(h.Count),
	}, nil
}

func encodeUserKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	enc := make([]byte, 1+len(key))
	enc[0] = userKeyPrefix
	copy(enc[1:], key)
	return enc
}

func decodeUserKey(enc []byte) []byte {
	if len(enc) == 0 {
		return nil
	}
	if enc[0] != userKeyPrefix {
		return append([]byte(nil), enc...)
	}
	return append([]byte(nil), enc[1:]...)
}

func heapTop(body []byte) uint16 {
	if len(body) < slotHeaderSize {
		return 0
	}
	return binary.LittleEndian.Uint16(body[0:2])
}

func dirStart(body []byte, count int) int {
	return len(body) - count*2
}

func dirEntry(body []byte, count int, i int) (uint16, error) {
	if i < 0 || i >= count {
		return 0, page.ErrPageCorrupt
	}
	start := dirStart(body, count)
	offPos := start + i*2
	if offPos+2 > len(body) {
		return 0, page.ErrPageCorrupt
	}
	return binary.LittleEndian.Uint16(body[offPos : offPos+2]), nil
}

func internalChildAt(body []byte, count int, i int) (page.PageID, error) {
	off, err := dirEntry(body, count, i)
	if err != nil {
		return 0, err
	}
	if off < slotHeaderSize || int(off)+8 > len(body) {
		return 0, page.ErrPageCorrupt
	}
	return page.PageID(binary.LittleEndian.Uint64(body[off : off+8])), nil
}

func internalKeyAt(body []byte, count int, i int) ([]byte, error) {
	off, err := dirEntry(body, count, i)
	if err != nil {
		return nil, err
	}
	var next uint16
	if i+1 < count {
		next, err = dirEntry(body, count, i+1)
		if err != nil {
			return nil, err
		}
	} else {
		next = heapTop(body)
	}
	if off < slotHeaderSize || next < off || next < off+8 || int(next) > len(body) {
		return nil, page.ErrPageCorrupt
	}
	return body[int(off)+8 : int(next)], nil
}

func findInternalChildIndex(body []byte, count int, key []byte) (int, error) {
	if count == 0 {
		return 0, nil
	}
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		midKey, err := internalKeyAt(body, count, mid)
		if err != nil {
			return 0, err
		}
		if bytes.Compare(midKey, key) > 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo == 0 {
		return 0, nil
	}
	return lo - 1, nil
}

func leafKeyAt(body []byte, count int, i int) ([]byte, error) {
	off, err := dirEntry(body, count, i)
	if err != nil {
		return nil, err
	}
	if int(off)+7 > len(body) {
		return nil, page.ErrPageCorrupt
	}
	src := body[off:]
	keyLen := int(binary.LittleEndian.Uint16(src[0:2]))
	base := 7 + keyLen
	if base > len(src) {
		return nil, page.ErrPageCorrupt
	}
	return src[7 : 7+keyLen], nil
}

func findLeafIndex(body []byte, count int, key []byte) (int, bool, error) {
	if count == 0 {
		return 0, false, nil
	}
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		off, err := dirEntry(body, count, mid)
		if err != nil {
			return 0, false, err
		}
		k, _, _, _, _, err := decodeLeafEntry(body, off)
		if err != nil {
			return 0, false, err
		}
		if bytes.Compare(k, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < count {
		off, err := dirEntry(body, count, lo)
		if err != nil {
			return 0, false, err
		}
		k, _, _, _, _, err := decodeLeafEntry(body, off)
		if err != nil {
			return 0, false, err
		}
		if bytes.Equal(k, key) {
			return lo, true, nil
		}
	}
	return lo, false, nil
}

func decodeLeafEntry(body []byte, off uint16) ([]byte, page.LeafFlags, []byte, page.ValuePtr, int, error) {
	if int(off)+7 > len(body) {
		return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
	}
	src := body[off:]
	keyLen := int(binary.LittleEndian.Uint16(src[0:2]))
	valLen := int(binary.LittleEndian.Uint32(src[2:6]))
	flags := page.LeafFlags(src[6])
	base := 7 + keyLen
	if base > len(src) {
		return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
	}
	key := src[7 : 7+keyLen]
	pos := base
	switch flags {
	case page.LeafFlagInline:
		if pos+valLen > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, page.ValuePtr{}, base + valLen, nil
	case page.LeafFlagPointer:
		if pos+page.ValuePtrSize > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		ptr, err := page.DecodeValuePtrLE(src[pos : pos+page.ValuePtrSize])
		if err != nil {
			return nil, 0, nil, page.ValuePtr{}, 0, err
		}
		return key, flags, nil, ptr, base + page.ValuePtrSize, nil
	case page.LeafFlagTombstone:
		return key, flags, nil, page.ValuePtr{}, base, nil
	default:
		if pos+valLen > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		val := src[pos : pos+valLen]
		return key, flags, val, page.ValuePtr{}, base + valLen, nil
	}
}

var _ cosmosdb.Iterator = (*iterator)(nil)
