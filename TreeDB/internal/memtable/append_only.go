package memtable

import (
	"bytes"
	"sort"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	appendOnlyEstimatedBytesPerEntry = 32
	appendOnlyMinInitialEntries      = 128
	appendOnlyMaxInitialEntries      = 1 << 20
	appendOnlyInlineKeyLen           = 8
)

type appendOnlyEntry struct {
	key       []byte
	value     []byte
	ptr       page.ValuePtr
	inlineKey [appendOnlyInlineKeyLen]byte
	flags     byte
	keyInline bool
}

type AppendOnly struct {
	mu sync.RWMutex

	entries   []appendOnlyEntry
	sizeBytes int64

	ordered bool
	hasLast bool
	lastIdx int
}

func (*AppendOnly) StableUnsafeIteratorSlices() bool { return true }

func NewAppendOnlyWithCapacity(capacity int) *AppendOnly {
	if capacity <= 0 {
		capacity = defaultMemtableCapacity
	}
	n := capacity / appendOnlyEstimatedBytesPerEntry
	if n < appendOnlyMinInitialEntries {
		n = appendOnlyMinInitialEntries
	}
	if n > appendOnlyMaxInitialEntries {
		n = appendOnlyMaxInitialEntries
	}
	return &AppendOnly{
		entries:   make([]appendOnlyEntry, 0, n),
		ordered:   true,
		hasLast:   false,
		lastIdx:   -1,
		sizeBytes: 0,
	}
}

func appendOnlyEntryKey(ent *appendOnlyEntry) []byte {
	if ent == nil {
		return nil
	}
	if ent.keyInline {
		return ent.inlineKey[:]
	}
	return ent.key
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func entryValueSize(flags byte, value []byte) int {
	if flags&node.FlagPointer != 0 {
		return page.ValuePtrSize + len(value)
	}
	if flags&node.FlagTombstone != 0 {
		return 0
	}
	return len(value)
}

func (m *AppendOnly) appendEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool) {
	if key == nil {
		return
	}
	ent := appendOnlyEntry{ptr: ptr, flags: flags}
	if steal {
		ent.key = key
		ent.value = value
	} else {
		if len(key) == appendOnlyInlineKeyLen {
			copy(ent.inlineKey[:], key)
			ent.keyInline = true
		} else {
			ent.key = cloneBytes(key)
		}
		ent.value = cloneBytes(value)
	}
	if flags&node.FlagTombstone != 0 {
		ent.value = nil
		ent.ptr = page.ValuePtr{}
	}
	m.entries = append(m.entries, ent)
	idx := len(m.entries) - 1
	k := appendOnlyEntryKey(&m.entries[idx])
	m.sizeBytes += int64(len(k) + entryValueSize(flags, ent.value))

	if !m.hasLast {
		m.lastIdx = idx
		m.hasLast = true
		return
	}
	if !m.ordered {
		return
	}
	prev := appendOnlyEntryKey(&m.entries[m.lastIdx])
	cmp := bytes.Compare(k, prev)
	if cmp <= 0 {
		m.ordered = false
		return
	}
	m.lastIdx = idx
}

func (m *AppendOnly) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	m.appendEntryLocked(key, value, ptr, flags, false)
	m.mu.Unlock()
}

func (m *AppendOnly) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	m.appendEntryLocked(key, value, ptr, flags, true)
	m.mu.Unlock()
}

func (m *AppendOnly) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *AppendOnly) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *AppendOnly) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	k := cloneBytes(key)
	v := cloneBytes(value)
	if cb != nil {
		if err := cb(k, v); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.appendEntryLocked(k, v, page.ValuePtr{}, node.FlagInline, true)
	m.mu.Unlock()
	return nil
}

func (m *AppendOnly) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	k := cloneBytes(key)
	if cb != nil {
		if err := cb(k, nil); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.appendEntryLocked(k, nil, page.ValuePtr{}, node.FlagTombstone, true)
	m.mu.Unlock()
	return nil
}

func (m *AppendOnly) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealBatch(entries, onKey)
}

func (m *AppendOnly) ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealBatch(entries, onKey)
}

func (m *AppendOnly) applyStealBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.mu.Lock()
	for i := range entries {
		op := entries[i]
		switch {
		case op.Type == batchpkg.OpDelete:
			m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, true)
		case op.IsPtr:
			m.appendEntryLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, true)
		default:
			m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, true)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
}

func (m *AppendOnly) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := len(m.entries) - 1; i >= 0; i-- {
		ent := &m.entries[i]
		if bytes.Equal(appendOnlyEntryKey(ent), key) {
			if ent.flags&node.FlagTombstone != 0 {
				return nil, true, true
			}
			return ent.value, false, true
		}
	}
	return nil, false, false
}

func (m *AppendOnly) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := len(m.entries) - 1; i >= 0; i-- {
		ent := &m.entries[i]
		if bytes.Equal(appendOnlyEntryKey(ent), key) {
			return ent.value, ent.ptr, ent.flags, true
		}
	}
	return nil, page.ValuePtr{}, 0, false
}

func (m *AppendOnly) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *AppendOnly) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.entries)
}

func (m *AppendOnly) Freeze() {}

func (m *AppendOnly) buildSortedLatestSnapshotLocked() []appendOnlyEntry {
	if len(m.entries) == 0 {
		return nil
	}
	latest := make(map[string]int, len(m.entries))
	for i := range m.entries {
		latest[string(appendOnlyEntryKey(&m.entries[i]))] = i
	}
	indices := make([]int, 0, len(latest))
	for _, idx := range latest {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(
			appendOnlyEntryKey(&m.entries[indices[i]]),
			appendOnlyEntryKey(&m.entries[indices[j]]),
		) < 0
	})
	snapshot := make([]appendOnlyEntry, len(indices))
	for i, idx := range indices {
		snapshot[i] = m.entries[idx]
	}
	return snapshot
}

func (m *AppendOnly) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	entries := m.entries
	if !m.ordered {
		entries = m.buildSortedLatestSnapshotLocked()
	}
	it := &appendOnlyIterator{
		entries: entries,
		end:     end,
		mu:      &m.mu,
	}
	if start != nil {
		it.Seek(start)
	}
	return it
}

type appendOnlyIterator struct {
	entries []appendOnlyEntry
	idx     int
	end     []byte
	mu      *sync.RWMutex
}

func (it *appendOnlyIterator) validIndex() bool {
	if it.idx < 0 || it.idx >= len(it.entries) {
		return false
	}
	if it.end != nil && bytes.Compare(appendOnlyEntryKey(&it.entries[it.idx]), it.end) >= 0 {
		return false
	}
	return true
}

func (it *appendOnlyIterator) Valid() bool {
	return it.validIndex()
}

func (it *appendOnlyIterator) Next() {
	if it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *appendOnlyIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(appendOnlyEntryKey(&it.entries[i]), key) >= 0
	})
}

func (it *appendOnlyIterator) UnsafeKey() []byte {
	if !it.validIndex() {
		return nil
	}
	return appendOnlyEntryKey(&it.entries[it.idx])
}

func (it *appendOnlyIterator) UnsafeValue() []byte {
	if !it.validIndex() {
		return nil
	}
	ent := it.entries[it.idx]
	if ent.flags&node.FlagTombstone != 0 {
		return nil
	}
	return ent.value
}

func (it *appendOnlyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.validIndex() {
		return nil, page.ValuePtr{}, 0
	}
	ent := it.entries[it.idx]
	return ent.value, ent.ptr, ent.flags
}

func (it *appendOnlyIterator) IsDeleted() bool {
	if !it.validIndex() {
		return false
	}
	return it.entries[it.idx].flags&node.FlagTombstone != 0
}

func (it *appendOnlyIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *appendOnlyIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *appendOnlyIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *appendOnlyIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *appendOnlyIterator) Error() error { return nil }

func (it *appendOnlyIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
	return nil
}

func (it *appendOnlyIterator) Domain() (start, end []byte) { return nil, it.end }
