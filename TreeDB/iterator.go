package treedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/mvcc"
	"github.com/snissn/gomap/TreeDB/internal/page"
)

const (
	userKeyPrefix  byte = 0x01
	slotHeaderSize      = 4
)

type cursorItem struct {
	pid       page.PageID
	buf       []byte
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

// Iterator implements the iterator.UnsafeIterator interface for the disk B+Tree.
type Iterator struct {
	db      *DB
	snap    *mvcc.Snapshot
	st      *mvcc.DBState
	start   []byte // User-provided start key for the iterator domain
	end     []byte   // User-provided end key for the iterator domain
	encFrom []byte // Encoded start key
	encTo   []byte // Encoded end key
	reverse bool

	stack  []cursorItem
	valid  atomic.Bool
	closed atomic.Bool
	err    error

	// Current item data (lazy loaded)
	currKey     []byte // Encoded key
	currFlags   page.LeafFlags
	currInline  []byte
	currPtr     page.ValuePtr
	valBuf      []byte // Reusable buffer for lazy-loaded values from slabs
	valueLoaded bool // Flag to indicate if valBuf is loaded
}

// NewIterator creates a new disk iterator.
func newIterator(db *DB, snap *mvcc.Snapshot, start, end []byte, reverse bool) *Iterator {
	it := &Iterator{
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

// Domain returns the start and end keys of the iterator.
func (it *Iterator) Domain() (start, end []byte) {
	if it == nil {
		return nil, nil
	}
	return append([]byte(nil), it.start...), append([]byte(nil), it.end...)
}

// Valid returns true if the iterator is currently pointing to a valid item.
func (it *Iterator) Valid() bool {
	if it == nil {
		return false
	}
	return it.valid.Load()
}

// Error returns the last error encountered by the iterator.
func (it *Iterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close closes the iterator and releases associated resources.
func (it *Iterator) Close() error {
	if it == nil {
		return nil
	}
	if it.closed.Swap(true) {
		return nil
	}
	it.valid.Store(false)
	if it.snap != nil {
		return it.snap.Close()
	}
	return nil
}

// Seek positions the iterator.
func (it *Iterator) Seek(key []byte) {
	it.valueLoaded = false
	it.err = nil 

	root := it.st.UserRootPageID
	if root == 0 {
		it.valid.Store(false)
		return
	}

	it.stack = it.stack[:0] 

	// Handle Reverse Seek logic
	if it.reverse {
		if key == nil {
			// Nil key in Reverse means seek to end (last element)
			if err := it.seekLast(root); err != nil {
				it.err = err
				it.valid.Store(false)
				return
			}
		} else {
			// Seek to key (end bound), then step back to find < end
			it.encTo = encodeUserKey(key) // Ensure encTo is set if we seek to it
			if err := it.search(root, it.encTo); err != nil {
				it.err = err
				it.valid.Store(false)
				return
			}
			it.loadCurrent()
			// If we landed on something valid, we might be >= end.
			// ReverseIterator domain is [start, end).
			// We want largest key < end.
			// search() lands on >= end.
			// So we must retreat.
			// However, if search() landed on EOF (stack empty), it means all keys < end.
			// Wait, search() returns valid if >= key found.
			// If search() returns invalid (exhausted), does it mean all keys are < end?
			// Yes, search drills down right-most path if not found? No, search drills down for >= key.
			// If all keys < key, search returns invalid?
			// My `search` implementation sets `valid=false` if not found?
			// Let's check `search`.
			// It sets `it.stack`. `loadCurrent` determines validity.
			// If `loadCurrent` says valid, we are at `>= key`.
			// So we MUST retreat.
			if it.Valid() {
				it.retreatReverse()
			} else {
				// If invalid, it might mean we are past end (good?) or empty.
				// If we ran off the end (all keys < end), we should point to last key.
				// But search() doesn't give us "last key" if not found.
				// Standard B+Tree search for insertion point would give us the slot.
				// If we want "last key < end", and search(end) failed (all < end),
				// we should `seekLast`.
				// How to detect "all < end" vs "empty"?
				// Root check handled empty.
				// So if search failed, we must be at end.
				if err := it.seekLast(root); err != nil {
					it.err = err
					it.valid.Store(false)
					return
				}
			}
		}
		it.loadCurrent()
		it.skipTombstonesReverse()
		it.enforceReverseBounds()
		return
	}

	// Forward Seek
	if key == nil {
		if err := it.seekFirst(root); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
	} else {
		it.encFrom = encodeUserKey(key)
		if err := it.search(root, it.encFrom); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
	}
	it.loadCurrent()
	it.skipTombstonesForward()
	it.enforceForwardBounds()
}

// Next advances the iterator to the next item.
func (it *Iterator) Next() {
	if it == nil || !it.Valid() {
		panic("treedb: Next on invalid iterator")
	}
	it.valueLoaded = false // Value will need to be reloaded for next item

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

// UnsafeKey returns a view (no copy) of the current key.
func (it *Iterator) UnsafeKey() []byte {
	if it == nil || !it.Valid() {
		panic("treedb: UnsafeKey on invalid iterator")
	}
	// Key is already a slice from mmap or an existing slice
	return it.currKey
}

// UnsafeValue returns a view (no copy) of the current value.
// It performs lazy loading from slab files if the value is a pointer.
func (it *Iterator) UnsafeValue() []byte {
	if it == nil || !it.Valid() {
		panic("treedb: UnsafeValue on invalid iterator")
	}
	if it.valueLoaded {
		return it.valBuf
	}

	var val []byte
	var err error
	switch it.currFlags {
	case page.LeafFlagInline:
		val = it.currInline
	case page.LeafFlagPointer:
		val, err = it.db.readPtr(it.currPtr, it.st.SlabSet)
		if err != nil {
			it.err = err
			it.valid.Store(false)
			panic(err) // Panic on read error
		}
	case page.LeafFlagTombstone:
		val = nil // Tombs should be filtered, but if seen, value is nil
	default:
		val = it.currInline // Should not happen
	}

	if val == nil { // Ensure non-nil empty slice for empty values
		val = []byte{}
	}
	it.valBuf = val
	it.valueLoaded = true // Correctly set to true
	return it.valBuf
}

// IsDeleted returns true if the current item is a tombstone.
func (it *Iterator) IsDeleted() bool {
	if it == nil || !it.Valid() {
		panic("treedb: IsDeleted on invalid iterator")
	}
	return it.currFlags&page.LeafFlagTombstone != 0
}

// Public Key returns a copy of the current key.
func (it *Iterator) Key() []byte {
	return append([]byte(nil), it.UnsafeKey()...)
}

// Public Value returns a copy of the current value.
func (it *Iterator) Value() []byte {
	return append([]byte(nil), it.UnsafeValue()...)
}

func (it *Iterator) initForward() {
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
	it.loadCurrent() // Load current entry after positioning
	it.skipTombstonesForward()
	it.enforceForwardBounds()
}

func (it *Iterator) initReverse() {
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
	it.loadCurrent() // Load current entry after positioning
	it.skipTombstonesReverse()
	it.enforceReverseBounds()
}

func (it *Iterator) enforceForwardBounds() {
	if !it.Valid() {
		return
	}
	if it.end == nil {
		return
	}
	k := it.UnsafeKey()
	if bytes.Compare(k, it.end) >= 0 {
		it.valid.Store(false)
	}
}

func (it *Iterator) enforceReverseBounds() {
	if !it.Valid() {
		return
	}
	if it.start == nil {
		return
	}
	k := it.UnsafeKey()
	if bytes.Compare(k, it.start) < 0 {
		it.valid.Store(false)
	}
}

func (it *Iterator) seekFirst(root page.PageID) error {
	it.stack = it.stack[:0]
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
				return err
			}
			if len(item.internals) == 0 {
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = item.internals[0].child
		default:
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *Iterator) seekLast(root page.PageID) error {
	it.stack = it.stack[:0]
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
				return err
			}
			if len(item.internals) == 0 {
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = len(item.internals) - 1
			it.stack = append(it.stack, item)
			pid = item.internals[item.index].child
		default:
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *Iterator) search(root page.PageID, key []byte) error {
	it.stack = it.stack[:0]
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
				return err
			}
			if len(item.internals) == 0 {
				return fmt.Errorf("treedb: corrupt internal page")
			}
			childIdx := findChildIndex(item.internals, key)
			if childIdx < 0 || childIdx >= len(item.internals) {
				return fmt.Errorf("treedb: child index out of bounds")
			}
			item.index = childIdx
			it.stack = append(it.stack, item)
			pid = item.internals[childIdx].child
		default:
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *Iterator) advanceForward() {
	if len(it.stack) == 0 {
		it.valid.Store(false)
		return
	}
	top := &it.stack[len(it.stack)-1]
	top.index++
	it.normalizeForward()
}

func (it *Iterator) normalizeForward() {
	for len(it.stack) > 0 {
		topIdx := len(it.stack) - 1
		top := &it.stack[topIdx]
		if top.h.Flags == page.PageTypeLeaf {
			if top.index < top.count {
				it.loadCurrent() // Load current entry
				return
			}
			it.stack = it.stack[:topIdx]
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
					it.loadCurrent() // Load current entry
					return
				}
				it.stack = it.stack[:len(it.stack)-1]
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
			it.stack = it.stack[:topIdx]
			continue
		}
		childPid := top.internals[top.index].child
		if err := it.drillDownLeft(childPid); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
		it.loadCurrent() // Load current entry
		return
	}
	it.valid.Store(false)
}

func (it *Iterator) retreatReverse() {
	if len(it.stack) == 0 {
		it.valid.Store(false)
		return
	}
	top := &it.stack[len(it.stack)-1]
	top.index--
	it.normalizeReverse()
}

func (it *Iterator) normalizeReverse() {
	for len(it.stack) > 0 {
		topIdx := len(it.stack) - 1
		top := &it.stack[topIdx]
		if top.h.Flags == page.PageTypeLeaf {
			if top.index >= 0 {
				it.loadCurrent() // Load current entry
				return
			}
			it.stack = it.stack[:topIdx]
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
					it.loadCurrent() // Load current entry
					return
				}
				it.stack = it.stack[:len(it.stack)-1]
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
			it.stack = it.stack[:topIdx]
			continue
		}
		childPid := top.internals[top.index].child
		if err := it.drillDownRight(childPid); err != nil {
			it.err = err
			it.valid.Store(false)
			return
		}
		it.loadCurrent() // Load current entry
		return
	}
	it.valid.Store(false)
}

func (it *Iterator) drillDownLeft(pid page.PageID) error {
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
				return err
			}
			if len(item.internals) == 0 {
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = 0
			it.stack = append(it.stack, item)
			pid = item.internals[0].child
		default:
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *Iterator) drillDownRight(pid page.PageID) error {
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
				return err
			}
			if len(item.internals) == 0 {
				return fmt.Errorf("treedb: corrupt internal page")
			}
			item.index = len(item.internals) - 1
			it.stack = append(it.stack, item)
			pid = item.internals[item.index].child
		default:
			return fmt.Errorf("treedb: unexpected page type %d", item.h.Flags)
		}
	}
}

func (it *Iterator) skipTombstonesForward() {
	for it.Valid() && it.IsDeleted() { // Use IsDeleted()
		it.advanceForward()
		it.enforceForwardBounds()
	}
}

func (it *Iterator) skipTombstonesReverse() {
	for it.Valid() && it.IsDeleted() { // Use IsDeleted()
		it.retreatReverse()
		it.enforceReverseBounds()
	}
}

// loadCurrent loads the current entry from the stack into internal fields.
func (it *Iterator) loadCurrent() {
	if len(it.stack) == 0 {
		it.valid.Store(false)
		return
	}
	top := it.stack[len(it.stack)-1]
	if top.h.Flags != page.PageTypeLeaf {
		it.err = fmt.Errorf("treedb: top of stack is not leaf page %d", top.pid)
		it.valid.Store(false)
		return
	}
	if top.index < 0 || top.index >= top.count {
		it.valid.Store(false) // Exhausted
		return
	}

	off, err := dirEntry(top.body, top.count, top.index)
	if err != nil {
		it.err = err
		it.valid.Store(false)
		return
	}
	
	// Decode leaf entry and store in Iterator's fields
	key, flags, inline, ptr, _, err := decodeLeafEntry(top.body, off)
	if err != nil {
		it.err = err
		it.valid.Store(false)
		return
	}
	it.currKey = decodeUserKey(key) // Decode user key now
	it.currFlags = flags
	it.currInline = inline
	it.currPtr = ptr
	it.valueLoaded = false // Value not loaded yet (lazy)
	it.valid.Store(true)
}

func (it *Iterator) currentLeafEntry() ([]byte, page.LeafFlags, []byte, page.ValuePtr, error) {
	// This function is now replaced by loadCurrent setting internal fields
	// and UnsafeKey/UnsafeValue accessing those fields.
	// It's still called by Key() and Value() to keep them.
	// But it shouldn't be called if loadCurrent is the source of truth.
	// I will delete this function.
	return nil, 0, nil, page.ValuePtr{}, fmt.Errorf("treedb: currentLeafEntry should not be called")
}

func (it *Iterator) readCursorItem(pid page.PageID) (cursorItem, error) {
	buf, err := it.db.pager.ReadPage(pid)
	if err != nil {
		return cursorItem{}, err
	}
	h, body, err := page.SplitPage(buf)
	if err != nil {
		return cursorItem{}, err
	}
	if err := h.VerifyBodyCRC(body); err != nil {
		return cursorItem{}, err
	}
	return cursorItem{
		pid:   pid,
		buf:   buf,
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