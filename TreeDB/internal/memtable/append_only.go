package memtable

import (
	"bytes"
	"encoding/binary"
	"math/bits"
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
	appendOnlyEstimatedBytesPerEntryPointer = 24
	appendOnlyMinInitialEntries             = 128
	appendOnlyMaxInitialEntries             = 1 << 20
	appendOnlyInlineKeyLen                  = 8
	appendOnlyPointerGrowCutoff             = 1 << 15
	appendOnlyEntryPoolMaxCap               = 1 << 20
	appendOnlyIteratorPoolMaxCap            = 1 << 20
	appendOnlyIteratorPtrPoolMaxCap         = 1 << 20
	appendOnlyValueArenaMinShift            = 12
	appendOnlyValueArenaMaxShift            = 20
	appendOnlyValueArenaClassCount          = appendOnlyValueArenaMaxShift - appendOnlyValueArenaMinShift + 1
	appendOnlyValueArenaDefaultChunk        = 32 << 10
	appendOnlyValueArenaPoolMaxCap          = 1 << appendOnlyValueArenaMaxShift
	appendOnlyValueArenaRetainMaxCap        = 4 << 20
	appendOnlyValueArenaRetainChunks        = 128
)

var appendOnlyEntryPool sync.Pool
var appendOnlyIteratorPool sync.Pool
var appendOnlyIteratorPtrPool sync.Pool
var appendOnlyValueArenaPools [appendOnlyValueArenaClassCount]sync.Pool

type appendOnlyEntry struct {
	key        []byte
	value      []byte
	ptr        page.ValuePtr
	inlineKey  [appendOnlyInlineKeyLen]byte
	flags      byte
	keyInline  bool
	valueOwned bool
}

type appendOnlyValueArena struct {
	chunks    [][]byte
	retained  [][]byte
	retainedB int
	cur       []byte
	curPos    int
}

type AppendOnly struct {
	mu sync.RWMutex

	entries    []appendOnlyEntry
	latest     map[string]int
	latest64   map[uint64]int
	snapshot   []*appendOnlyEntry
	indexBuf   []int
	keyArena   appendOnlyValueArena
	valueArena appendOnlyValueArena
	count      int
	snapCount  int
	sizeBytes  int64

	ordered     bool
	latestDirty bool
	frozen      bool
	hasLast     bool
	lastIdx     int

	iteratorLeaseMu   sync.Mutex
	iteratorLeaseCond *sync.Cond
	iteratorLeases    int
}

func (*AppendOnly) StableUnsafeIteratorSlices() bool { return true }

func getAppendOnlyEntries(length int) []appendOnlyEntry {
	if length < 0 {
		length = 0
	}
	if length > appendOnlyEntryPoolMaxCap {
		return make([]appendOnlyEntry, length)
	}
	if v := appendOnlyEntryPool.Get(); v != nil {
		if entries, ok := v.([]appendOnlyEntry); ok && cap(entries) >= length {
			return entries[:length]
		}
	}
	return make([]appendOnlyEntry, length)
}

func putAppendOnlyEntries(entries []appendOnlyEntry) {
	if entries == nil || cap(entries) == 0 || cap(entries) > appendOnlyEntryPoolMaxCap {
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	appendOnlyEntryPool.Put(full[:0])
}

func getAppendOnlyIteratorEntries(length int) []appendOnlyEntry {
	if length < 0 {
		length = 0
	}
	if length > appendOnlyIteratorPoolMaxCap {
		return make([]appendOnlyEntry, length)
	}
	if v := appendOnlyIteratorPool.Get(); v != nil {
		if entries, ok := v.([]appendOnlyEntry); ok && cap(entries) >= length {
			out := entries[:length]
			clear(out)
			return out
		}
	}
	return make([]appendOnlyEntry, length)
}

func putAppendOnlyIteratorEntries(entries []appendOnlyEntry) {
	if entries == nil || cap(entries) == 0 || cap(entries) > appendOnlyIteratorPoolMaxCap {
		return
	}
	clear(entries)
	appendOnlyIteratorPool.Put(entries[:0])
}

func getAppendOnlyIteratorPtrs(length int) []*appendOnlyEntry {
	if length < 0 {
		length = 0
	}
	if length > appendOnlyIteratorPtrPoolMaxCap {
		return make([]*appendOnlyEntry, length)
	}
	if v := appendOnlyIteratorPtrPool.Get(); v != nil {
		if entries, ok := v.([]*appendOnlyEntry); ok && cap(entries) >= length {
			out := entries[:length]
			clear(out)
			return out
		}
	}
	return make([]*appendOnlyEntry, length)
}

func putAppendOnlyIteratorPtrs(entries []*appendOnlyEntry) {
	if entries == nil || cap(entries) == 0 || cap(entries) > appendOnlyIteratorPtrPoolMaxCap {
		return
	}
	clear(entries)
	appendOnlyIteratorPtrPool.Put(entries[:0])
}

func appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry int) int {
	if capacity <= 0 {
		capacity = defaultMemtableCapacity
	}
	if estimatedBytesPerEntry <= 0 {
		estimatedBytesPerEntry = appendOnlyEstimatedBytesPerEntryPointer
	}
	n := capacity / estimatedBytesPerEntry
	if n < appendOnlyMinInitialEntries {
		n = appendOnlyMinInitialEntries
	}
	if n > appendOnlyMaxInitialEntries {
		n = appendOnlyMaxInitialEntries
	}
	return n
}

func NewAppendOnlyWithCapacity(capacity int) *AppendOnly {
	return NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, appendOnlyEstimatedBytesPerEntryPointer)
}

func NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, estimatedBytesPerEntry int) *AppendOnly {
	n := appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry)
	return &AppendOnly{
		entries:   getAppendOnlyEntries(n),
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

func appendOnlyValueArenaClassForLen(length int) (idx int, classCap int, ok bool) {
	if length <= 0 || length > appendOnlyValueArenaPoolMaxCap {
		return 0, 0, false
	}
	classCap = 1 << uint(bits.Len(uint(length-1)))
	minCap := 1 << appendOnlyValueArenaMinShift
	if classCap < minCap {
		classCap = minCap
	}
	if classCap > appendOnlyValueArenaPoolMaxCap {
		return 0, 0, false
	}
	shift := bits.Len(uint(classCap)) - 1
	idx = shift - appendOnlyValueArenaMinShift
	if idx < 0 || idx >= appendOnlyValueArenaClassCount {
		return 0, 0, false
	}
	return idx, classCap, true
}

func appendOnlyValueArenaClassForCap(capacity int) (idx int, ok bool) {
	minCap := 1 << appendOnlyValueArenaMinShift
	if capacity < minCap || capacity > appendOnlyValueArenaPoolMaxCap {
		return 0, false
	}
	if capacity&(capacity-1) != 0 {
		return 0, false
	}
	shift := bits.TrailingZeros(uint(capacity))
	idx = shift - appendOnlyValueArenaMinShift
	if idx < 0 || idx >= appendOnlyValueArenaClassCount {
		return 0, false
	}
	return idx, true
}

func getAppendOnlyValueArenaChunk(capacity int) []byte {
	if capacity <= 0 {
		capacity = appendOnlyValueArenaDefaultChunk
	}
	if capacity < appendOnlyValueArenaDefaultChunk {
		capacity = appendOnlyValueArenaDefaultChunk
	}
	if idx, classCap, ok := appendOnlyValueArenaClassForLen(capacity); ok {
		if v := appendOnlyValueArenaPools[idx].Get(); v != nil {
			if b, ok := v.([]byte); ok && cap(b) >= classCap {
				return b[:0]
			}
		}
		return make([]byte, 0, classCap)
	}
	return make([]byte, 0, capacity)
}

func putAppendOnlyValueArenaChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	if idx, ok := appendOnlyValueArenaClassForCap(cap(chunk)); ok {
		appendOnlyValueArenaPools[idx].Put(chunk[:0])
	}
}

func (a *appendOnlyValueArena) popRetained(minLen int) []byte {
	if len(a.retained) == 0 {
		return nil
	}
	for i := len(a.retained) - 1; i >= 0; i-- {
		chunk := a.retained[i]
		if cap(chunk) < minLen {
			continue
		}
		last := len(a.retained) - 1
		a.retained[i] = a.retained[last]
		a.retained[last] = nil
		a.retained = a.retained[:last]
		a.retainedB -= cap(chunk)
		if a.retainedB < 0 {
			a.retainedB = 0
		}
		return chunk[:0]
	}
	return nil
}

func (a *appendOnlyValueArena) retainChunk(chunk []byte) bool {
	if chunk == nil || cap(chunk) == 0 {
		return false
	}
	if len(a.retained) >= appendOnlyValueArenaRetainChunks {
		return false
	}
	next := a.retainedB + cap(chunk)
	if next > appendOnlyValueArenaRetainMaxCap {
		return false
	}
	a.retained = append(a.retained, chunk[:0])
	a.retainedB = next
	return true
}

func (a *appendOnlyValueArena) alloc(length int) []byte {
	if length <= 0 {
		return nil
	}
	if a.cur == nil || cap(a.cur)-a.curPos < length {
		chunk := a.popRetained(length)
		if chunk == nil {
			chunk = getAppendOnlyValueArenaChunk(length)
		}
		a.chunks = append(a.chunks, chunk)
		a.cur = chunk[:cap(chunk)]
		a.curPos = 0
	}
	out := a.cur[a.curPos : a.curPos+length : a.curPos+length]
	a.curPos += length
	return out
}

func (a *appendOnlyValueArena) reset() {
	for i := range a.chunks {
		if !a.retainChunk(a.chunks[i]) {
			putAppendOnlyValueArenaChunk(a.chunks[i])
		}
		a.chunks[i] = nil
	}
	a.chunks = a.chunks[:0]
	a.cur = nil
	a.curPos = 0
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
	} else if current < appendOnlyPointerGrowCutoff {
		// Route-mode inline-heavy workloads (e.g. batch_write_steady with valsize
		// below pointer threshold) still pay significant growth churn. Use a
		// moderate early growth factor for inline records as well to reduce
		// repeated reallocation/copy amplification.
		next = current * 3
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
	if m.count == len(m.entries) {
		nextCap := appendOnlyNextCapacity(len(m.entries), flags)
		prev := m.entries
		grown := getAppendOnlyEntries(nextCap)
		copy(grown, m.entries[:m.count])
		m.entries = grown
		putAppendOnlyEntries(prev)
	}
	idx := m.count
	m.count++
	ent := &m.entries[idx]
	ent.ptr = ptr
	ent.flags = flags
	ent.keyInline = false
	ent.valueOwned = false
	if steal {
		ent.key = key
		ent.value = value
	} else {
		if len(key) == appendOnlyInlineKeyLen {
			copy(ent.inlineKey[:], key)
			ent.keyInline = true
			ent.key = nil
		} else {
			ent.key = m.keyArena.alloc(len(key))
			copy(ent.key, key)
		}
		if len(value) > 0 {
			ent.value = m.valueArena.alloc(len(value))
			copy(ent.value, value)
			ent.valueOwned = true
		} else {
			ent.value = nil
		}
	}
	if flags&node.FlagTombstone != 0 {
		ent.value = nil
		ent.valueOwned = false
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
	if cb == nil {
		m.mu.Lock()
		m.appendEntryLocked(key, value, page.ValuePtr{}, node.FlagInline, false)
		m.mu.Unlock()
		return nil
	}
	k := cloneBytes(key)
	v := cloneBytes(value)
	if err := cb(k, v); err != nil {
		return err
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
	if cb == nil {
		m.mu.Lock()
		m.appendEntryLocked(key, nil, page.ValuePtr{}, node.FlagTombstone, false)
		m.mu.Unlock()
		return nil
	}
	k := cloneBytes(key)
	if err := cb(k, nil); err != nil {
		return err
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

func (m *AppendOnly) orderedLookupEntryLocked(key []byte) *appendOnlyEntry {
	if !m.ordered || m.count == 0 {
		return nil
	}
	active := m.entries[:m.count]
	idx := sort.Search(len(active), func(i int) bool {
		return bytes.Compare(appendOnlyEntryKey(&active[i]), key) >= 0
	})
	if idx >= len(active) {
		return nil
	}
	ent := &active[idx]
	if !bytes.Equal(appendOnlyEntryKey(ent), key) {
		return nil
	}
	return ent
}

func (m *AppendOnly) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ent := m.orderedLookupEntryLocked(key); ent != nil {
		if ent.flags&node.FlagTombstone != 0 {
			return nil, true, true
		}
		return ent.value, false, true
	}
	if m.ordered {
		return nil, false, false
	}
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
	if ent := m.orderedLookupEntryLocked(key); ent != nil {
		return ent.value, ent.ptr, ent.flags, true
	}
	if m.ordered {
		return nil, page.ValuePtr{}, 0, false
	}
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
	if m.frozen {
		m.mu.Unlock()
		return
	}
	m.frozen = true
	m.mu.Unlock()
}

func (m *AppendOnly) acquireIteratorLeaseLocked() {
	m.iteratorLeaseMu.Lock()
	if m.iteratorLeaseCond == nil {
		m.iteratorLeaseCond = sync.NewCond(&m.iteratorLeaseMu)
	}
	m.iteratorLeases++
	m.iteratorLeaseMu.Unlock()
}

func (m *AppendOnly) releaseIteratorLease() {
	m.iteratorLeaseMu.Lock()
	if m.iteratorLeases > 0 {
		m.iteratorLeases--
		if m.iteratorLeases == 0 && m.iteratorLeaseCond != nil {
			m.iteratorLeaseCond.Broadcast()
		}
	}
	m.iteratorLeaseMu.Unlock()
}

func (m *AppendOnly) waitIteratorLeasesLocked() {
	m.iteratorLeaseMu.Lock()
	for m.iteratorLeases > 0 {
		if m.iteratorLeaseCond == nil {
			m.iteratorLeaseCond = sync.NewCond(&m.iteratorLeaseMu)
		}
		m.iteratorLeaseCond.Wait()
	}
	m.iteratorLeaseMu.Unlock()
}

func (m *AppendOnly) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLocked()
}

// TryReset resets the table only when no iterator leases are currently held.
// It returns false when a frozen iterator is still open.
func (m *AppendOnly) TryReset() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.iteratorLeaseMu.Lock()
	held := m.iteratorLeases > 0
	m.iteratorLeaseMu.Unlock()
	if held {
		return false
	}
	m.resetLocked()
	return true
}

func (m *AppendOnly) resetLocked() {
	for i := 0; i < m.count; i++ {
		ent := &m.entries[i]
		ent.ptr = page.ValuePtr{}
		ent.flags = 0
		ent.keyInline = false
		ent.valueOwned = false
		ent.key = nil
		ent.value = nil
	}
	m.keyArena.reset()
	m.valueArena.reset()
	clear(m.latest)
	clear(m.latest64)
	m.clearSnapshotLocked()
	m.count = 0
	m.sizeBytes = 0
	m.ordered = true
	m.latestDirty = false
	m.frozen = false
	m.hasLast = false
	m.lastIdx = -1
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
	m.mu.RUnlock()

	// Unordered iterators need a sorted latest-key view. Build/update shared
	// caches under an exclusive lock.
	m.mu.Lock()
	snapshotPtrs := m.buildSortedLatestSnapshotLocked()
	if m.frozen {
		ptrs := getAppendOnlyIteratorPtrs(len(snapshotPtrs))
		copy(ptrs, snapshotPtrs)
		m.acquireIteratorLeaseLocked()
		m.mu.Unlock()
		it := &appendOnlyIterator{
			entryPtrs:       ptrs,
			end:             end,
			pooledEntryPtrs: true,
			leaseOwner:      m,
			leaseHeld:       true,
		}
		if start != nil {
			it.Seek(start)
		}
		return it
	}
	// Mutable unordered memtables must copy entries so readers can iterate
	// without holding write locks while writers append concurrently.
	entries := getAppendOnlyIteratorEntries(len(snapshotPtrs))
	for i := range snapshotPtrs {
		if snapshotPtrs[i] != nil {
			entries[i] = *snapshotPtrs[i]
		}
	}
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries:       entries,
		end:           end,
		pooledEntries: true,
	}
	if start != nil {
		it.Seek(start)
	}
	return it
}

type appendOnlyIterator struct {
	entries         []appendOnlyEntry
	entryPtrs       []*appendOnlyEntry
	idx             int
	end             []byte
	mu              *sync.RWMutex
	pooledEntries   bool
	pooledEntryPtrs bool
	leaseOwner      *AppendOnly
	leaseHeld       bool
}

func (it *appendOnlyIterator) len() int {
	if it.entryPtrs != nil {
		return len(it.entryPtrs)
	}
	return len(it.entries)
}

func (it *appendOnlyIterator) entryAt(idx int) *appendOnlyEntry {
	if idx < 0 {
		return nil
	}
	if it.entryPtrs != nil {
		if idx >= len(it.entryPtrs) {
			return nil
		}
		return it.entryPtrs[idx]
	}
	if idx >= len(it.entries) {
		return nil
	}
	return &it.entries[idx]
}

func (it *appendOnlyIterator) validIndex() bool {
	if it.idx < 0 || it.idx >= it.len() {
		return false
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
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil
	}
	return appendOnlyEntryKey(ent)
}

func (it *appendOnlyIterator) UnsafeValue() []byte {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil
	}
	if ent.flags&node.FlagTombstone != 0 {
		return nil
	}
	return ent.value
}

func (it *appendOnlyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil, page.ValuePtr{}, 0
	}
	return ent.value, ent.ptr, ent.flags
}

func (it *appendOnlyIterator) IsDeleted() bool {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return false
	}
	return ent.flags&node.FlagTombstone != 0
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
	if it.pooledEntries {
		putAppendOnlyIteratorEntries(it.entries)
		it.pooledEntries = false
	}
	if it.pooledEntryPtrs {
		putAppendOnlyIteratorPtrs(it.entryPtrs)
		it.pooledEntryPtrs = false
	}
	if it.leaseHeld && it.leaseOwner != nil {
		it.leaseOwner.releaseIteratorLease()
		it.leaseHeld = false
	}
	it.leaseOwner = nil
	it.entries = nil
	it.entryPtrs = nil
	return nil
}

func (it *appendOnlyIterator) Domain() (start, end []byte) { return nil, it.end }
