package memtable

import (
	"strings"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	btree "github.com/tidwall/btree"
)

const btreeDefaultDegree = 32
const btreeUseLoadFastPath = false
const btreeArenaChunkSize = 1 << 20

type btreeEntry struct {
	value   []byte
	deleted bool
}

type BTree struct {
	mu        sync.RWMutex
	tree      *btree.Map[string, btreeEntry]
	sizeBytes int64
	arena     *btreeArena
	lastKey   string
	hasLast   bool
}

func NewBTree() *BTree {
	return NewBTreeWithDegree(btreeDefaultDegree)
}

func NewBTreeWithDegree(degree int) *BTree {
	if degree <= 0 {
		degree = btreeDefaultDegree
	}
	return &BTree{
		tree: btree.NewMap[string, btreeEntry](degree),
		arena: &btreeArena{
			chunkSize: btreeArenaChunkSize,
		},
	}
}

func (m *BTree) Set(key, value []byte) {
	_ = m.PutWithCallback(key, value, nil)
}

func (m *BTree) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	keyCopy := m.arena.Copy(key)
	valCopy := m.arena.Copy(value)
	if cb != nil {
		if err := cb(keyCopy, valCopy); err != nil {
			return err
		}
	}
	keyStr := bytesToStringNoCopy(keyCopy)

	m.mu.Lock()
	defer m.mu.Unlock()

	prev, replaced := m.setMaybeLoadLocked(keyStr, btreeEntry{value: valCopy})
	if replaced {
		oldLen := len(prev.value)
		if prev.deleted {
			oldLen = 0
		}
		m.sizeBytes += int64(len(valCopy) - oldLen)
		return nil
	}
	m.sizeBytes += int64(len(keyCopy) + len(valCopy))
	if !m.hasLast || keyStr > m.lastKey {
		m.lastKey = keyStr
		m.hasLast = true
	}
	return nil
}

func (m *BTree) SetSteal(key, value []byte) {
	if key == nil {
		return
	}
	keyStr := bytesToStringNoCopy(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	prev, replaced := m.setMaybeLoadLocked(keyStr, btreeEntry{value: value})
	if replaced {
		oldLen := len(prev.value)
		if prev.deleted {
			oldLen = 0
		}
		m.sizeBytes += int64(len(value) - oldLen)
		return
	}
	m.sizeBytes += int64(len(key) + len(value))
	if !m.hasLast || keyStr > m.lastKey {
		m.lastKey = keyStr
		m.hasLast = true
	}
}

func (m *BTree) Delete(key []byte) {
	_ = m.DeleteWithCallback(key, nil)
}

func (m *BTree) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	keyCopy := m.arena.Copy(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}
	keyStr := bytesToStringNoCopy(keyCopy)

	m.mu.Lock()
	defer m.mu.Unlock()

	prev, replaced := m.tree.Set(keyStr, btreeEntry{deleted: true})
	if replaced {
		if !prev.deleted {
			m.sizeBytes -= int64(len(prev.value))
		}
		return nil
	}
	m.sizeBytes += int64(len(keyCopy))
	return nil
}

func (m *BTree) DeleteSteal(key []byte) {
	if key == nil {
		return
	}
	keyStr := bytesToStringNoCopy(key)

	m.mu.Lock()
	defer m.mu.Unlock()

	prev, replaced := m.tree.Set(keyStr, btreeEntry{deleted: true})
	if replaced {
		if !prev.deleted {
			m.sizeBytes -= int64(len(prev.value))
		}
		return
	}
	m.sizeBytes += int64(len(key))
}

func (m *BTree) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, false, false
	}
	return val.value, val.deleted, true
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
	m.mu.RUnlock()

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
	return it.cur.value
}

func (it *btreeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur || it.cur.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
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
	return it.cur.deleted
}

func (it *btreeIterator) Error() error {
	return nil
}

func (it *btreeIterator) Close() error {
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

func stringToBytesNoCopy(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
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
