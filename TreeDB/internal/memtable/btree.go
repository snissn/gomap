package memtable

import (
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	btree "github.com/tidwall/btree"
)

const btreeDefaultDegree = 32
const btreeUseLoadFastPath = false
const btreeArenaChunkSize = 1 << 20

type btreeEntry struct {
	value []byte
	flags byte
}

type BTree struct {
	mu        sync.RWMutex
	tree      *btree.Map[string, btreeEntry]
	sizeBytes int64
	arena     *btreeArena
	lastKey   string
	hasLast   bool
	degree    int
}

func (*BTree) StableUnsafeIteratorSlices() bool { return true }

func NewBTree() *BTree {
	return NewBTreeWithDegree(btreeDefaultDegree)
}

func (m *BTree) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, op := range entries {
		keyStr := bytesToStringNoCopy(op.Key)
		var prev btreeEntry
		var replaced bool

		if op.Type == batchpkg.OpDelete {
			if m.hasLast && keyStr > m.lastKey {
				prev, replaced = m.tree.Load(keyStr, btreeEntry{flags: node.FlagTombstone})
			} else {
				prev, replaced = m.tree.Set(keyStr, btreeEntry{flags: node.FlagTombstone})
			}
			if replaced {
				if prev.flags&node.FlagTombstone == 0 {
					m.sizeBytes -= int64(len(prev.value))
				}
			} else {
				m.sizeBytes += int64(len(op.Key))
			}
		} else if op.IsPtr {
			valCopy := m.encodeEntryValue(op.Value, op.ValuePtr, node.FlagPointer, true)
			entry := btreeEntry{value: valCopy, flags: node.FlagPointer}
			if m.hasLast && keyStr > m.lastKey {
				prev, replaced = m.tree.Load(keyStr, entry)
			} else {
				prev, replaced = m.tree.Set(keyStr, entry)
			}
			if replaced {
				oldLen := len(prev.value)
				if prev.flags&node.FlagTombstone != 0 {
					oldLen = 0
				}
				m.sizeBytes += int64(len(valCopy) - oldLen)
			} else {
				m.sizeBytes += int64(len(op.Key) + len(valCopy))
			}
		} else {
			entry := btreeEntry{value: op.Value, flags: node.FlagInline}
			if m.hasLast && keyStr > m.lastKey {
				prev, replaced = m.tree.Load(keyStr, entry)
			} else {
				prev, replaced = m.tree.Set(keyStr, entry)
			}
			if replaced {
				oldLen := len(prev.value)
				if prev.flags&node.FlagTombstone != 0 {
					oldLen = 0
				}
				m.sizeBytes += int64(len(op.Value) - oldLen)
			} else {
				m.sizeBytes += int64(len(op.Key) + len(op.Value))
			}
		}

		if !m.hasLast || keyStr > m.lastKey {
			m.lastKey = keyStr
			m.hasLast = true
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
}

func NewBTreeWithDegree(degree int) *BTree {
	if degree <= 0 {
		degree = btreeDefaultDegree
	}
	return &BTree{
		tree:   btree.NewMap[string, btreeEntry](degree),
		degree: degree,
		arena: &btreeArena{
			chunkSize: btreeArenaChunkSize,
		},
	}
}

// Reset clears all entries while retaining internal allocations.
func (m *BTree) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tree = btree.NewMap[string, btreeEntry](m.degree)
	m.sizeBytes = 0
	m.lastKey = ""
	m.hasLast = false
	if m.arena != nil {
		m.arena.resetKeepFirstChunk()
	}
}

func (m *BTree) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *BTree) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	if cb != nil {
		keyCopy := append([]byte(nil), key...)
		valCopy := append([]byte(nil), value...)
		if err := cb(keyCopy, valCopy); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStored := m.arena.Copy(key)
	valStored := m.encodeEntryValue(value, page.ValuePtr{}, node.FlagInline, false)
	keyStr := bytesToStringNoCopy(keyStored)
	prev, replaced := m.setMaybeLoadLocked(keyStr, btreeEntry{value: valStored, flags: node.FlagInline})
	if replaced {
		oldLen := len(prev.value)
		if prev.flags&node.FlagTombstone != 0 {
			oldLen = 0
		}
		m.sizeBytes += int64(len(valStored) - oldLen)
		return nil
	}
	m.sizeBytes += int64(len(keyStored) + len(valStored))
	if !m.hasLast || keyStr > m.lastKey {
		m.lastKey = keyStr
		m.hasLast = true
	}
	return nil
}

func (m *BTree) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *BTree) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	if key == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keyCopy := m.arena.Copy(key)
	valCopy := m.encodeEntryValue(value, ptr, flags, false)
	keyStr := bytesToStringNoCopy(keyCopy)
	prev, replaced := m.setMaybeLoadLocked(keyStr, btreeEntry{value: valCopy, flags: flags})
	if replaced {
		oldLen := len(prev.value)
		if prev.flags&node.FlagTombstone != 0 {
			oldLen = 0
		}
		m.sizeBytes += int64(len(valCopy) - oldLen)
		return
	}
	m.sizeBytes += int64(len(keyCopy) + len(valCopy))
	if !m.hasLast || keyStr > m.lastKey {
		m.lastKey = keyStr
		m.hasLast = true
	}
}

func (m *BTree) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	if key == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStr := bytesToStringNoCopy(key)
	valCopy := m.encodeEntryValue(value, ptr, flags, true)
	prev, replaced := m.setMaybeLoadLocked(keyStr, btreeEntry{value: valCopy, flags: flags})
	if replaced {
		oldLen := len(prev.value)
		if prev.flags&node.FlagTombstone != 0 {
			oldLen = 0
		}
		m.sizeBytes += int64(len(valCopy) - oldLen)
		return
	}
	m.sizeBytes += int64(len(key) + len(valCopy))
	if !m.hasLast || keyStr > m.lastKey {
		m.lastKey = keyStr
		m.hasLast = true
	}
}

func (m *BTree) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *BTree) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	if cb != nil {
		keyCopy := append([]byte(nil), key...)
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStored := m.arena.Copy(key)
	keyStr := bytesToStringNoCopy(keyStored)
	prev, replaced := m.tree.Set(keyStr, btreeEntry{flags: node.FlagTombstone})
	if replaced {
		if prev.flags&node.FlagTombstone == 0 {
			m.sizeBytes -= int64(len(prev.value))
		}
		return nil
	}
	m.sizeBytes += int64(len(keyStored))
	return nil
}

func (m *BTree) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *BTree) encodeEntryValue(value []byte, ptr page.ValuePtr, flags byte, steal bool) []byte {
	if flags&node.FlagPointer != 0 {
		size := page.ValuePtrSize + len(value)
		dst := m.arena.alloc(size)
		ptr.Encode(dst[:page.ValuePtrSize])
		if len(value) > 0 {
			copy(dst[page.ValuePtrSize:], value)
		}
		return dst[:size]
	}
	if flags&node.FlagTombstone != 0 {
		return nil
	}
	if steal {
		return value
	}
	return m.arena.Copy(value)
}

func (m *BTree) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, false, false
	}
	if val.flags&node.FlagPointer != 0 {
		if len(val.value) > page.ValuePtrSize {
			return val.value[page.ValuePtrSize:], val.flags&node.FlagTombstone != 0, true
		}
		return nil, val.flags&node.FlagTombstone != 0, true
	}
	return val.value, val.flags&node.FlagTombstone != 0, true
}

func (m *BTree) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, page.ValuePtr{}, 0, false
	}
	if val.flags&node.FlagPointer != 0 {
		if len(val.value) >= page.ValuePtrSize {
			inline := []byte(nil)
			if len(val.value) > page.ValuePtrSize {
				inline = val.value[page.ValuePtrSize:]
			}
			return inline, page.DecodeValuePtr(val.value[:page.ValuePtrSize]), val.flags, true
		}
		return nil, page.ValuePtr{}, val.flags, true
	}
	return val.value, page.ValuePtr{}, val.flags, true
}

func (m *BTree) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *BTree) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}

func (m *BTree) Freeze() {}

func (m *BTree) NewIterator(start, end []byte) iterator.UnsafeIterator {
	startKey := ""
	if start != nil {
		startKey = bytesToStringNoCopy(start)
	}
	endKey := ""
	hasEnd := false
	if end != nil {
		endKey = bytesToStringNoCopy(end)
		hasEnd = true
	}

	m.mu.RLock()
	iter := m.tree.Iter()

	valid := false
	if startKey == "" {
		valid = iter.First()
	} else {
		valid = iter.Seek(startKey)
	}

	it := &btreeIterator{
		iter:   iter,
		end:    endKey,
		hasEnd: hasEnd,
		valid:  valid,
		mu:     &m.mu,
	}
	it.refresh()
	return it
}

func (m *BTree) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	startKey := ""
	hasStart := false
	if start != nil {
		startKey = bytesToStringNoCopy(start)
		hasStart = true
	}
	endKey := ""
	hasEnd := false
	if end != nil {
		endKey = bytesToStringNoCopy(end)
		hasEnd = true
	}

	m.mu.RLock()
	iter := m.tree.Iter()

	valid := false
	if !hasEnd {
		valid = iter.Last()
	} else {
		valid = iter.Seek(endKey)
		if valid {
			// Seek positions at the first key >= end. Reverse iteration is over
			// [start, end), so step back to the last key < end.
			valid = iter.Prev()
		} else {
			// seek(end) fell off the end.
			valid = iter.Last()
		}
	}

	it := &btreeReverseIterator{
		iter:     iter,
		start:    startKey,
		hasStart: hasStart,
		end:      endKey,
		hasEnd:   hasEnd,
		valid:    valid,
		mu:       &m.mu,
	}
	it.refresh()
	return it
}

type btreeIterator struct {
	iter   btree.MapIter[string, btreeEntry]
	end    string
	hasEnd bool
	valid  bool
	cur    btreeEntry
	hasCur bool
	mu     *sync.RWMutex
}

func (it *btreeIterator) Seek(key []byte) {
	it.valid = it.iter.Seek(bytesToStringNoCopy(key))
	it.refresh()
}

func (it *btreeIterator) Next() {
	if !it.valid {
		return
	}
	it.valid = it.iter.Next()
	it.refresh()
}

func (it *btreeIterator) Valid() bool {
	if !it.valid || !it.hasCur {
		return false
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		return false
	}
	return true
}

func (it *btreeIterator) UnsafeKey() []byte {
	if !it.hasCur {
		return nil
	}
	return stringToBytesNoCopy(it.iter.Key())
}

func (it *btreeIterator) UnsafeValue() []byte {
	if !it.hasCur {
		return nil
	}
	if it.cur.flags&node.FlagPointer != 0 {
		if len(it.cur.value) > page.ValuePtrSize {
			return it.cur.value[page.ValuePtrSize:]
		}
		return nil
	}
	return it.cur.value
}

func (it *btreeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagPointer != 0 {
		if len(it.cur.value) >= page.ValuePtrSize {
			inline := []byte(nil)
			if len(it.cur.value) > page.ValuePtrSize {
				inline = it.cur.value[page.ValuePtrSize:]
			}
			return inline, page.DecodeValuePtr(it.cur.value[:page.ValuePtrSize]), it.cur.flags
		}
		return nil, page.ValuePtr{}, it.cur.flags
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline
}

func (it *btreeIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *btreeIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *btreeIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *btreeIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *btreeIterator) IsDeleted() bool {
	if !it.hasCur {
		return false
	}
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *btreeIterator) Error() error {
	return nil
}

func (it *btreeIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return nil
}

func (it *btreeIterator) Domain() (start, end []byte) {
	if !it.hasEnd {
		return nil, nil
	}
	return nil, []byte(it.end)
}

func (it *btreeIterator) refresh() {
	it.hasCur = false
	if !it.valid {
		return
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		it.valid = false
		return
	}
	it.cur = it.iter.Value()
	it.hasCur = true
}

type btreeReverseIterator struct {
	iter     btree.MapIter[string, btreeEntry]
	start    string
	hasStart bool
	end      string
	hasEnd   bool
	valid    bool
	cur      btreeEntry
	hasCur   bool
	mu       *sync.RWMutex
}

func (it *btreeReverseIterator) Seek(key []byte) {
	if key == nil {
		it.valid = it.iter.Last()
		it.refresh()
		return
	}
	it.valid = it.iter.Seek(bytesToStringNoCopy(key))
	if !it.valid {
		it.valid = it.iter.Last()
	}
	it.refresh()
}

func (it *btreeReverseIterator) Next() {
	if !it.valid {
		return
	}
	it.valid = it.iter.Prev()
	it.refresh()
}

func (it *btreeReverseIterator) Valid() bool {
	if !it.valid || !it.hasCur {
		return false
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		return false
	}
	if it.hasStart && strings.Compare(it.iter.Key(), it.start) < 0 {
		return false
	}
	return true
}

func (it *btreeReverseIterator) UnsafeKey() []byte {
	if !it.hasCur {
		return nil
	}
	return stringToBytesNoCopy(it.iter.Key())
}

func (it *btreeReverseIterator) UnsafeValue() []byte {
	if !it.hasCur {
		return nil
	}
	if it.cur.flags&node.FlagPointer != 0 {
		if len(it.cur.value) > page.ValuePtrSize {
			return it.cur.value[page.ValuePtrSize:]
		}
		return nil
	}
	return it.cur.value
}

func (it *btreeReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagPointer != 0 {
		if len(it.cur.value) >= page.ValuePtrSize {
			inline := []byte(nil)
			if len(it.cur.value) > page.ValuePtrSize {
				inline = it.cur.value[page.ValuePtrSize:]
			}
			return inline, page.DecodeValuePtr(it.cur.value[:page.ValuePtrSize]), it.cur.flags
		}
		return nil, page.ValuePtr{}, it.cur.flags
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline
}

func (it *btreeReverseIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *btreeReverseIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *btreeReverseIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *btreeReverseIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *btreeReverseIterator) IsDeleted() bool {
	if !it.hasCur {
		return false
	}
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *btreeReverseIterator) Error() error {
	return nil
}

func (it *btreeReverseIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return nil
}

func (it *btreeReverseIterator) Domain() (start, end []byte) {
	if !it.hasEnd && !it.hasStart {
		return nil, nil
	}
	var s []byte
	if it.hasStart {
		s = []byte(it.start)
	}
	var e []byte
	if it.hasEnd {
		e = []byte(it.end)
	}
	return s, e
}

func (it *btreeReverseIterator) refresh() {
	it.hasCur = false
	if !it.valid {
		return
	}
	if it.hasEnd && strings.Compare(it.iter.Key(), it.end) >= 0 {
		it.valid = false
		return
	}
	if it.hasStart && strings.Compare(it.iter.Key(), it.start) < 0 {
		it.valid = false
		return
	}
	it.cur = it.iter.Value()
	it.hasCur = true
}

func (m *BTree) setMaybeLoadLocked(key string, entry btreeEntry) (btreeEntry, bool) {
	if btreeUseLoadFastPath && m.hasLast && key > m.lastKey {
		return m.tree.Load(key, entry)
	}
	return m.tree.Set(key, entry)
}

type btreeArena struct {
	chunkSize int
	chunks    [][]byte
	offset    int
}

func (a *btreeArena) resetKeepFirstChunk() {
	if a == nil {
		return
	}
	if len(a.chunks) == 0 {
		a.offset = 0
		return
	}
	first := a.chunks[0]
	first = first[:len(first)]
	a.chunks = a.chunks[:1]
	a.chunks[0] = first
	a.offset = 0
}

func (a *btreeArena) Copy(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.alloc(len(src))
	copy(dst, src)
	return dst
}

func (a *btreeArena) alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	chunk := a.currentChunk(n)
	start := a.offset
	a.offset += n
	return chunk[start:a.offset]
}

func (a *btreeArena) currentChunk(n int) []byte {
	if a == nil {
		return make([]byte, n)
	}
	if len(a.chunks) == 0 {
		size := a.chunkSize
		if size < n {
			size = n
		}
		a.chunks = append(a.chunks, make([]byte, size))
		a.offset = 0
		return a.chunks[len(a.chunks)-1]
	}
	chunk := a.chunks[len(a.chunks)-1]
	if a.offset+n <= len(chunk) {
		return chunk
	}
	size := a.chunkSize
	if size < n {
		size = n
	}
	a.chunks = append(a.chunks, make([]byte, size))
	a.offset = 0
	return a.chunks[len(a.chunks)-1]
}
