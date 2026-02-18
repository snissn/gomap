package memtable

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	// Size accounting uses key+value/pointer payload bytes, not the in-memory
	// appendOnlyEntry struct footprint. A lower estimate reduces growth/copy
	// churn for pointer-heavy write paths.
	appendOnlyEstimatedBytesPerEntry = 24
	appendOnlyMinInitialEntries      = 128
	appendOnlyMaxInitialEntries      = 1 << 20
	appendOnlyInlineKeyLen           = 8
	appendOnlyPointerGrowCutoff      = 1 << 15
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
	latest    map[string]int
	latest64  map[uint64]int
	snapshot  []*appendOnlyEntry
	indexBuf  []int
	count     int
	snapCount int
	sizeBytes int64

	ordered     bool
	latestDirty bool
	hasLast     bool
	lastIdx     int
	frozen      bool
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
		entries:   make([]appendOnlyEntry, n),
		count:     0,
		ordered:   true,
		hasLast:   false,
		lastIdx:   -1,
		snapCount: 0,
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

func appendOnlyNextCapacity(current int, flags byte) int {
	if current < appendOnlyMinInitialEntries {
		return appendOnlyMinInitialEntries
	}
	next := current * 2
	// Pointer-heavy write paths can spend a disproportionate amount of time
	// copying entry arrays during growth. Grow more aggressively early, then
	// fall back to 2x to keep memory expansion bounded.
	if flags&node.FlagPointer != 0 && current < appendOnlyPointerGrowCutoff {
		next = current * 4
	}
	if next <= current {
		return current + appendOnlyMinInitialEntries
	}
	return next
}

func appendOnlyKeyString(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return unsafe.String(&key[0], len(key))
}

func appendOnlyKeyU64(key []byte) (uint64, bool) {
	if len(key) != appendOnlyInlineKeyLen {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
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

func (m *AppendOnly) clearSnapshotLocked() {
	if m.snapshot != nil {
		m.snapshot = m.snapshot[:0]
	}
	m.snapCount = 0
}

func (m *AppendOnly) rebuildLatestIndexLocked() {
	if m.count == 0 {
		if m.latest != nil {
			clear(m.latest)
		}
		if m.latest64 != nil {
			clear(m.latest64)
		}
		m.latestDirty = false
		m.clearSnapshotLocked()
		return
	}
	if m.latest != nil {
		clear(m.latest)
	}
	if m.latest64 != nil {
		clear(m.latest64)
	}
	reserve := m.count
	active := m.entries[:m.count]
	for i := range active {
		k := appendOnlyEntryKey(&active[i])
		if k64, ok := appendOnlyKeyU64(k); ok {
			if m.latest64 == nil {
				m.latest64 = make(map[uint64]int, reserve)
			}
			m.latest64[k64] = i
			continue
		}
		if m.latest == nil {
			m.latest = make(map[string]int, reserve)
		}
		m.latest[appendOnlyKeyString(k)] = i
	}
	m.latestDirty = false
	m.clearSnapshotLocked()
}

func (m *AppendOnly) appendEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool) {
	if key == nil {
		return
	}
	if m.frozen {
		m.frozen = false
	}
	if m.count == len(m.entries) {
		nextCap := appendOnlyNextCapacity(len(m.entries), flags)
		grown := make([]appendOnlyEntry, nextCap)
		copy(grown, m.entries[:m.count])
		m.entries = grown
	}
	idx := m.count
	m.count++
	ent := &m.entries[idx]
	ent.ptr = ptr
	ent.flags = flags
	ent.keyInline = false
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
	k := appendOnlyEntryKey(ent)
	m.sizeBytes += int64(len(k) + entryValueSize(flags, ent.value))

	if !m.hasLast {
		m.lastIdx = idx
		m.hasLast = true
		return
	}
	if m.ordered {
		prev := appendOnlyEntryKey(&m.entries[m.lastIdx])
		cmp := bytes.Compare(k, prev)
		if cmp > 0 {
			m.lastIdx = idx
			return
		}
		m.ordered = false
		m.latestDirty = true
		m.clearSnapshotLocked()
		return
	}
	m.latestDirty = true
	m.clearSnapshotLocked()
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
	if !m.ordered && !m.latestDirty {
		if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
			if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
				ent := &m.entries[idx]
				if bytes.Equal(appendOnlyEntryKey(ent), key) {
					if ent.flags&node.FlagTombstone != 0 {
						return nil, true, true
					}
					return ent.value, false, true
				}
			}
		}
		if m.latest != nil {
			if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
				ent := &m.entries[idx]
				if bytes.Equal(appendOnlyEntryKey(ent), key) {
					if ent.flags&node.FlagTombstone != 0 {
						return nil, true, true
					}
					return ent.value, false, true
				}
			}
		}
	}
	for i := m.count - 1; i >= 0; i-- {
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
	if !m.ordered && !m.latestDirty {
		if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
			if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
				ent := &m.entries[idx]
				if bytes.Equal(appendOnlyEntryKey(ent), key) {
					return ent.value, ent.ptr, ent.flags, true
				}
			}
		}
		if m.latest != nil {
			if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
				ent := &m.entries[idx]
				if bytes.Equal(appendOnlyEntryKey(ent), key) {
					return ent.value, ent.ptr, ent.flags, true
				}
			}
		}
	}
	for i := m.count - 1; i >= 0; i-- {
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
	return m.count
}

func (m *AppendOnly) Freeze() {
	m.mu.Lock()
	m.frozen = true
	m.mu.Unlock()
}

func (m *AppendOnly) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < m.count; i++ {
		m.entries[i] = appendOnlyEntry{}
	}
	clear(m.latest)
	clear(m.latest64)
	m.clearSnapshotLocked()
	m.count = 0
	m.sizeBytes = 0
	m.ordered = true
	m.latestDirty = false
	m.hasLast = false
	m.lastIdx = -1
	m.frozen = false
}

func (m *AppendOnly) buildSortedLatestSnapshotLocked() []*appendOnlyEntry {
	if m.count == 0 {
		return nil
	}
	if m.ordered {
		return nil
	}
	if m.snapshot != nil && m.snapCount == m.count {
		return m.snapshot
	}
	active := m.entries[:m.count]
	if m.latestDirty || (len(m.latest) == 0 && len(m.latest64) == 0) {
		m.rebuildLatestIndexLocked()
	}
	need := len(m.latest) + len(m.latest64)
	indices := m.indexBuf[:0]
	if cap(indices) < need {
		indices = make([]int, 0, need)
	}
	for _, idx := range m.latest {
		indices = append(indices, idx)
	}
	for _, idx := range m.latest64 {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(
			appendOnlyEntryKey(&active[indices[i]]),
			appendOnlyEntryKey(&active[indices[j]]),
		) < 0
	})
	snapshot := m.snapshot
	if cap(snapshot) < len(indices) {
		snapshot = make([]*appendOnlyEntry, len(indices))
	} else {
		snapshot = snapshot[:len(indices)]
	}
	for i, idx := range indices {
		snapshot[i] = &active[idx]
	}
	m.indexBuf = indices[:0]
	m.snapshot = snapshot
	m.snapCount = m.count
	return snapshot
}

func (m *AppendOnly) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	if m.ordered {
		entries := m.entries[:m.count]
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
	if m.frozen && !m.latestDirty && m.snapCount == m.count {
		snapshotPtrs := m.snapshot
		m.mu.RUnlock()
		it := &appendOnlyIterator{
			ptrEntries: snapshotPtrs,
			end:        end,
		}
		if start != nil {
			it.Seek(start)
		}
		return it
	}
	m.mu.RUnlock()

	// Unordered iterators need a sorted latest-key view. Build/update shared
	// caches under an exclusive lock. Frozen tables can iterate pointer views
	// directly; mutable tables still copy to avoid holding the write lock.
	m.mu.Lock()
	snapshotPtrs := m.buildSortedLatestSnapshotLocked()
	if m.frozen {
		m.mu.Unlock()
		it := &appendOnlyIterator{
			ptrEntries: snapshotPtrs,
			end:        end,
		}
		if start != nil {
			it.Seek(start)
		}
		return it
	}
	entries := make([]appendOnlyEntry, len(snapshotPtrs))
	for i := range snapshotPtrs {
		if snapshotPtrs[i] != nil {
			entries[i] = *snapshotPtrs[i]
		}
	}
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries: entries,
		end:     end,
	}
	if start != nil {
		it.Seek(start)
	}
	return it
}

type appendOnlyIterator struct {
	entries    []appendOnlyEntry
	ptrEntries []*appendOnlyEntry
	idx        int
	end        []byte
	mu         *sync.RWMutex
}

func (it *appendOnlyIterator) len() int {
	if it.ptrEntries != nil {
		return len(it.ptrEntries)
	}
	return len(it.entries)
}

func (it *appendOnlyIterator) entryAt(i int) *appendOnlyEntry {
	if i < 0 {
		return nil
	}
	if it.ptrEntries != nil {
		if i >= len(it.ptrEntries) {
			return nil
		}
		return it.ptrEntries[i]
	}
	if i >= len(it.entries) {
		return nil
	}
	return &it.entries[i]
}

func (it *appendOnlyIterator) validIndex() bool {
	if it.idx < 0 || it.idx >= len(it.entries) {
		if it.ptrEntries == nil || it.idx < 0 || it.idx >= len(it.ptrEntries) {
			return false
		}
	}
	ent := it.entryAt(it.idx)
	if ent == nil {
		return false
	}
	if it.end != nil && bytes.Compare(appendOnlyEntryKey(ent), it.end) >= 0 {
		return false
	}
	return true
}

func (it *appendOnlyIterator) Valid() bool {
	return it.validIndex()
}

func (it *appendOnlyIterator) Next() {
	if it.idx < it.len() {
		it.idx++
	}
}

func (it *appendOnlyIterator) Seek(key []byte) {
	it.idx = sort.Search(it.len(), func(i int) bool {
		ent := it.entryAt(i)
		if ent == nil {
			return true
		}
		return bytes.Compare(appendOnlyEntryKey(ent), key) >= 0
	})
}

func (it *appendOnlyIterator) UnsafeKey() []byte {
	if !it.validIndex() {
		return nil
	}
	ent := it.entryAt(it.idx)
	if ent == nil {
		return nil
	}
	return appendOnlyEntryKey(ent)
}

func (it *appendOnlyIterator) UnsafeValue() []byte {
	if !it.validIndex() {
		return nil
	}
	ent := it.entryAt(it.idx)
	if ent == nil || ent.flags&node.FlagTombstone != 0 {
		return nil
	}
	return ent.value
}

func (it *appendOnlyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.validIndex() {
		return nil, page.ValuePtr{}, 0
	}
	ent := it.entryAt(it.idx)
	if ent == nil {
		return nil, page.ValuePtr{}, 0
	}
	return ent.value, ent.ptr, ent.flags
}

func (it *appendOnlyIterator) IsDeleted() bool {
	if !it.validIndex() {
		return false
	}
	ent := it.entryAt(it.idx)
	return ent != nil && ent.flags&node.FlagTombstone != 0
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
