package memtable

import (
	"sort"
	"strings"
	"sync"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type hashEntry struct {
	key     []byte
	value   []byte
	deleted bool
}

type hashArena struct {
	chunks  [][]byte
	cur     []byte
	off     int
	nextCap int
}

func (a *hashArena) alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	if a.cur == nil || cap(a.cur)-a.off < n {
		c := a.nextCap
		if c < 64*1024 {
			c = 64 * 1024
		}
		if c < n {
			c = n
		}
		a.cur = make([]byte, c)
		a.chunks = append(a.chunks, a.cur)
		a.off = 0
		a.nextCap = c * 2
	}
	b := a.cur[a.off : a.off+n]
	a.off += n
	return b
}

func (a *hashArena) copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := a.alloc(len(src))
	copy(dst, src)
	return dst
}

type HashSorted struct {
	mu          sync.RWMutex
	items       map[string]hashEntry
	sizeBytes   int64
	sortedKeys  []string
	sortedValid bool
	frozen      bool
	arena       hashArena
}

func NewHashSorted() *HashSorted {
	return &HashSorted{
		items:       make(map[string]hashEntry),
		sortedValid: true,
	}
}

func (a *hashArena) resetKeepFirstChunk() {
	if len(a.chunks) == 0 {
		a.cur = nil
		a.off = 0
		a.nextCap = 0
		return
	}
	first := a.chunks[0]
	first = first[:cap(first)]
	a.chunks = a.chunks[:1]
	a.chunks[0] = first
	a.cur = first
	a.off = 0
	a.nextCap = cap(first) * 2
}

// Reset clears all entries while retaining internal allocations.
func (m *HashSorted) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.items)
	m.sizeBytes = 0
	m.sortedKeys = m.sortedKeys[:0]
	m.sortedValid = true
	m.frozen = false
	m.arena.resetKeepFirstChunk()
}

func (m *HashSorted) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range entries {
		if op.Type == batchpkg.OpDelete {
			m.deleteStealLocked(op.Key)
		} else {
			m.setStealLocked(op.Key, op.Value)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
}

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (m *HashSorted) Set(key, value []byte) {
	_ = m.PutWithCallback(key, value, nil)
}

func (m *HashSorted) SetSteal(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setStealLocked(key, value)
}

func (m *HashSorted) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		valCopy := m.arena.copyBytes(value)
		if cb != nil {
			if err := cb(ent.key, valCopy); err != nil {
				return err
			}
		}
		oldLen := len(ent.value)
		ent.value = valCopy
		ent.deleted = false
		m.sizeBytes += int64(len(valCopy) - oldLen)
		m.items[storedKey] = ent
		return nil
	}

	keyCopy := m.arena.copyBytes(key)
	valCopy := m.arena.copyBytes(value)
	if cb != nil {
		if err := cb(keyCopy, valCopy); err != nil {
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyStored] = hashEntry{key: keyCopy, value: valCopy}
	m.sizeBytes += int64(len(keyCopy) + len(valCopy))
	m.maybeTrackNewKeyLocked(keyStored)
	return nil
}

func (m *HashSorted) Delete(key []byte) {
	_ = m.DeleteWithCallback(key, nil)
}

func (m *HashSorted) DeleteSteal(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteStealLocked(key)
}

func (m *HashSorted) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		if cb != nil {
			if err := cb(ent.key, nil); err != nil {
				return err
			}
		}
		if !ent.deleted {
			m.sizeBytes -= int64(len(ent.value))
			ent.value = nil
			ent.deleted = true
		}
		m.items[storedKey] = ent
		return nil
	}

	keyCopy := m.arena.copyBytes(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyStored] = hashEntry{key: keyCopy, deleted: true}
	m.sizeBytes += int64(len(keyCopy))
	m.maybeTrackNewKeyLocked(keyStored)
	return nil
}

func (m *HashSorted) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ent, ok := m.items[bytesToStringNoCopy(key)]
	if !ok {
		return nil, false, false
	}
	return ent.value, ent.deleted, true
}

func (m *HashSorted) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *HashSorted) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

func (m *HashSorted) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return
	}
	m.frozen = true
}

func (m *HashSorted) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	sortedValid := m.sortedValid
	m.mu.RUnlock()
	if !sortedValid {
		m.mu.Lock()
		m.ensureSortedLocked()
		m.mu.Unlock()
	}

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
	keys := m.sortedKeys
	m.mu.RUnlock()

	idx := 0
	if startKey != "" {
		idx = sort.Search(len(keys), func(i int) bool {
			return strings.Compare(keys[i], startKey) >= 0
		})
	}

	return &hashIterator{
		mt:     m,
		keys:   keys,
		idx:    idx,
		end:    endKey,
		hasEnd: hasEnd,
	}
}

func (m *HashSorted) ensureSortedLocked() {
	if m.sortedValid {
		return
	}
	keys := make([]string, len(m.items))
	i := 0
	for k := range m.items {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	m.sortedKeys = keys
	m.sortedValid = true
}

func (m *HashSorted) maybeTrackNewKeyLocked(key string) {
	if !m.sortedValid {
		return
	}
	if len(m.sortedKeys) == 0 || strings.Compare(m.sortedKeys[len(m.sortedKeys)-1], key) < 0 {
		m.sortedKeys = append(m.sortedKeys, key)
		return
	}
	if len(m.sortedKeys) <= 4096 {
		i := sort.Search(len(m.sortedKeys), func(i int) bool {
			return m.sortedKeys[i] >= key
		})
		if i < len(m.sortedKeys) && m.sortedKeys[i] == key {
			return
		}
		m.sortedKeys = append(m.sortedKeys, "")
		copy(m.sortedKeys[i+1:], m.sortedKeys[i:])
		m.sortedKeys[i] = key
		return
	}
	m.sortedValid = false
}

func (m *HashSorted) setStealLocked(key, value []byte) {
	if key == nil {
		return
	}
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		oldLen := len(ent.value)
		ent.value = value
		ent.deleted = false
		m.sizeBytes += int64(len(value) - oldLen)
		m.items[storedKey] = ent
		return
	}
	keyStored := bytesToStringNoCopy(key)
	m.items[keyStored] = hashEntry{key: key, value: value}
	m.sizeBytes += int64(len(key) + len(value))
	m.maybeTrackNewKeyLocked(keyStored)
}

func (m *HashSorted) deleteStealLocked(key []byte) {
	if key == nil {
		return
	}
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		if !ent.deleted {
			m.sizeBytes -= int64(len(ent.value))
			ent.value = nil
			ent.deleted = true
		}
		m.items[storedKey] = ent
		return
	}
	keyStored := bytesToStringNoCopy(key)
	m.items[keyStored] = hashEntry{key: key, deleted: true}
	m.sizeBytes += int64(len(key))
	m.maybeTrackNewKeyLocked(keyStored)
}

type hashIterator struct {
	mt     *HashSorted
	keys   []string
	idx    int
	end    string
	hasEnd bool
	cur    hashEntry
	valid  bool
}

func (it *hashIterator) Seek(key []byte) {
	if it.keys == nil {
		it.valid = false
		return
	}
	seekKey := bytesToStringNoCopy(key)
	it.idx = sort.Search(len(it.keys), func(i int) bool {
		return strings.Compare(it.keys[i], seekKey) >= 0
	})
	it.refresh()
}

func (it *hashIterator) Next() {
	it.idx++
	it.refresh()
}

func (it *hashIterator) Valid() bool {
	if !it.valid {
		return false
	}
	if it.hasEnd && strings.Compare(it.keys[it.idx], it.end) >= 0 {
		return false
	}
	return true
}

func (it *hashIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.cur.key
}

func (it *hashIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	return it.cur.value
}

func (it *hashIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline
}

func (it *hashIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *hashIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *hashIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *hashIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *hashIterator) IsDeleted() bool {
	if !it.valid {
		return false
	}
	return it.cur.deleted
}

func (it *hashIterator) Error() error {
	return nil
}

func (it *hashIterator) Close() error {
	return nil
}

func (it *hashIterator) Domain() (start, end []byte) {
	if !it.hasEnd {
		return nil, nil
	}
	return nil, []byte(it.end)
}

func (it *hashIterator) refresh() {
	it.valid = false
	if it.idx < 0 || it.idx >= len(it.keys) {
		return
	}
	if it.hasEnd && strings.Compare(it.keys[it.idx], it.end) >= 0 {
		return
	}
	key := it.keys[it.idx]
	ent, ok := it.mt.items[key]
	if !ok {
		return
	}
	it.cur = ent
	it.valid = true
}
