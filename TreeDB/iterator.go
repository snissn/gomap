package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"

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
	pid       page.PageID
	ref       *pager.PageRef
	h         *page.Header
	body      []byte
	count     int
	index     int
	internals []internalKV
}

type internalKV struct {
	key   []byte
	child page.PageID
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
	valid  atomic.Bool
	closed atomic.Bool
	err    error
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
	if it.stack[last].ref != nil {
		it.stack[last].ref.Release()
		it.stack[last].ref = nil
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
		if it.stack[i].ref != nil {
			it.stack[i].ref.Release()
			it.stack[i].ref = nil
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
	return it.valid.Load()
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
	if it.closed.Swap(true) {
		return nil
	}
	it.valid.Store(false)
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
		it.valid.Store(false)
		panic(err)
	}
	return decodeUserKey(key)
}

func (it *iterator) Value() []byte {
	if it == nil || !it.Valid() {
		panic("treedb: Value on invalid iterator")
	}
	_, flags, inline, ptr, err := it.currentLeafEntry()
	if err != nil {
		it.err = err
		it.valid.Store(false)
		panic(err)
	}
	switch flags {
	case page.LeafFlagInline:
		return append([]byte(nil), inline...)
	case page.LeafFlagPointer:
		val, err := it.db.readPtr(ptr, it.st.SlabSet)
		if err != nil {
			it.err = err
			it.valid.Store(false)
			panic(err)
		}
		return append([]byte(nil), val...)
	default:
		return append([]byte(nil), inline...)
	}
}

func (it *iterator) initForward() {
	root := it.st.UserRootPageID
	if root == 0 {
		it.valid.Store(false)
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
		it.valid.Store(false)
		return
	}
	it.skipTombstonesForward()
	it.enforceForwardBounds()
}

func (it *iterator) initReverse() {
	root := it.st.UserRootPageID
	if root == 0 {
		it.valid.Store(false)
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
		it.valid.Store(false)
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
	k, _, _, _, err := it.currentLeafEntry()
	if err != nil {
		it.err = err
		it.valid.Store(false)
		return
	}
	if bytes.Compare(k, it.encTo) >= 0 {
		it.valid.Store(false)
	}
}

func (it *iterator) enforceReverseBounds() {
	if !it.Valid() {
		return
	}
	if it.encFrom == nil {
		return
	}
	k, _, _, _, err := it.currentLeafEntry()
	if err != nil {
		it.err = err
		it.valid.Store(false)
		return
	}
	if bytes.Compare(k, it.encFrom) < 0 {
		it.valid.Store(false)
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
				it.valid.Store(false)
			} else {
				it.valid.Store(true)
			}
			return nil
		case page.PageTypeInternal:
			item.internals, err = parseInternalEntries(item.body, item.count)
			if err != nil {
				item.ref.Release()
				return err
			}
			if len(item.internals) == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = item.internals[0].child
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
				it.valid.Store(false)
			} else {
				it.valid.Store(true)
			}
			return nil
		case page.PageTypeInternal:
			item.internals, err = parseInternalEntries(item.body, item.count)
			if err != nil {
				item.ref.Release()
				return err
			}
			if len(item.internals) == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = len(item.internals) - 1
			it.stack = append(it.stack, item)
			pid = item.internals[item.index].child
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
				it.valid.Store(true)
			} else {
				it.valid.Store(false)
			}
			return nil
		case page.PageTypeInternal:
			item.internals, err = parseInternalEntries(item.body, item.count)
			if err != nil {
				item.ref.Release()
				return err
			}
			if len(item.internals) == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			childIdx := findChildIndex(item.internals, key)
			if childIdx < 0 || childIdx >= len(item.internals) {
				item.ref.Release()
				return fmt.Errorf("treedb: child index out of bounds")
			}
			item.index = childIdx
			it.stack = append(it.stack, item)
			pid = item.internals[childIdx].child
		default:
			item.ref.Release()
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *iterator) advanceForward() {
	if len(it.stack) == 0 {
		it.valid.Store(false)
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
					it.valid.Store(true)
					return
				}
				it.popCursorItem()
				if len(it.stack) == 0 {
					it.valid.Store(false)
					return
				}
				parent := &it.stack[len(it.stack)-1]
			parent.index++
			for {
				if parent.index < parent.count {
					childPid := parent.internals[parent.index].child
					if err := it.drillDownLeft(childPid); err != nil {
						it.err = err
						it.valid.Store(false)
						return
						}
						break
					}
					it.popCursorItem()
					if len(it.stack) == 0 {
						it.valid.Store(false)
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
		childPid := top.internals[top.index].child
		if err := it.drillDownLeft(childPid); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
	}
	it.valid.Store(false)
}

func (it *iterator) retreatReverse() {
	if len(it.stack) == 0 {
		it.valid.Store(false)
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
					it.valid.Store(true)
					return
				}
				it.popCursorItem()
				if len(it.stack) == 0 {
					it.valid.Store(false)
					return
				}
				parent := &it.stack[len(it.stack)-1]
			parent.index--
			for {
				if parent.index >= 0 {
					childPid := parent.internals[parent.index].child
					if err := it.drillDownRight(childPid); err != nil {
						it.err = err
						it.valid.Store(false)
						return
						}
						break
					}
					it.popCursorItem()
					if len(it.stack) == 0 {
						it.valid.Store(false)
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
		childPid := top.internals[top.index].child
		if err := it.drillDownRight(childPid); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
	}
	it.valid.Store(false)
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
			item.internals, err = parseInternalEntries(item.body, item.count)
			if err != nil {
				item.ref.Release()
				return err
			}
			if len(item.internals) == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = item.internals[0].child
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
			item.internals, err = parseInternalEntries(item.body, item.count)
			if err != nil {
				item.ref.Release()
				return err
			}
			if len(item.internals) == 0 {
				item.ref.Release()
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = len(item.internals) - 1
			it.stack = append(it.stack, item)
			pid = item.internals[item.index].child
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
			it.valid.Store(false)
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
			it.valid.Store(false)
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
	top := it.stack[len(it.stack)-1]
	if top.h.Flags != page.PageTypeLeaf {
		return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: top is not leaf")
	}
	if top.index < 0 || top.index >= top.count {
		return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: leaf index out of bounds")
	}
	off, err := dirEntry(top.body, top.count, top.index)
	if err != nil {
		return nil, 0, nil, page.ValuePtr{}, err
	}
	key, flags, inline, ptr, _, err := decodeLeafEntry(top.body, off)
	if err != nil {
		return nil, 0, nil, page.ValuePtr{}, err
	}
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
	if err := h.VerifyBodyCRC(body); err != nil {
		ref.Release()
		return cursorItem{}, err
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

func parseInternalEntries(body []byte, count int) ([]internalKV, error) {
	if count == 0 {
		return nil, nil
	}
	out := make([]internalKV, count)
	for i := 0; i < count; i++ {
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
		if next < off || int(next) > len(body) || int(off)+8 > len(body) {
			return nil, page.ErrPageCorrupt
		}
		entryLen := int(next - off)
		child, key, _, err := decodeInternalEntry(body, off, entryLen)
		if err != nil {
			return nil, err
		}
		out[i] = internalKV{key: key, child: child}
	}
	return out, nil
}

func decodeInternalEntry(body []byte, off uint16, entryLen int) (page.PageID, []byte, int, error) {
	if entryLen < 8 || int(off)+entryLen > len(body) {
		return 0, nil, 0, page.ErrPageCorrupt
	}
	src := body[off:]
	child := page.PageID(binary.LittleEndian.Uint64(src[0:8]))
	key := append([]byte(nil), src[8:entryLen]...)
	return child, key, entryLen, nil
}

func findChildIndex(entries []internalKV, key []byte) int {
	if len(entries) == 0 {
		return 0
	}
	i := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) > 0
	})
	if i == 0 {
		return 0
	}
	return i - 1
}

func findLeafIndex(body []byte, count int, key []byte) (int, bool, error) {
	if count == 0 {
		return 0, false, nil
	}
	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		off, err := dirEntry(body, count, i)
		if err != nil {
			return 0, false, err
		}
		k, _, _, _, _, err := decodeLeafEntry(body, off)
		if err != nil {
			return 0, false, err
		}
		keys[i] = k
	}
	idx := sort.Search(len(keys), func(i int) bool {
		return bytes.Compare(keys[i], key) >= 0
	})
	if idx < len(keys) && bytes.Equal(keys[idx], key) {
		return idx, true, nil
	}
	return idx, false, nil
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
	key := append([]byte(nil), src[7:7+keyLen]...)
	pos := base
	switch flags {
	case page.LeafFlagInline:
		if pos+valLen > len(src) {
			return nil, 0, nil, page.ValuePtr{}, 0, page.ErrPageCorrupt
		}
		val := append([]byte(nil), src[pos:pos+valLen]...)
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
		val := append([]byte(nil), src[pos:pos+valLen]...)
		return key, flags, val, page.ValuePtr{}, base + valLen, nil
	}
}

var _ cosmosdb.Iterator = (*iterator)(nil)
