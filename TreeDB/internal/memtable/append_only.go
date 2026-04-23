package memtable

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
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
	appendOnlyEntryPoolMaxCap               = 1 << 20
	appendOnlyIteratorPoolMaxCap            = 1 << 20
	appendOnlyIteratorPtrPoolMaxCap         = 1 << 20
	appendOnlyReusableKeyMaxCap             = 1 << 10
	appendOnlyValueArenaMinShift            = 12
	appendOnlyValueArenaMaxShift            = 20
	appendOnlyValueArenaClassCount          = appendOnlyValueArenaMaxShift - appendOnlyValueArenaMinShift + 1
	appendOnlyValueArenaDefaultChunk        = 32 << 10
	appendOnlyValueArenaPoolMaxCap          = 1 << appendOnlyValueArenaMaxShift
	appendOnlyValueArenaRetainMaxCap        = 4 << 20
	appendOnlyValueArenaRetainChunks        = 128
	appendOnlyReuseOversizeFactor           = 4
	appendOnlyResetDropThresholdEntries     = 1 << 15
	appendOnlyAggressiveGrowCutoff          = appendOnlyResetDropThresholdEntries * 2
	appendOnlyPredictHintMinEntries         = 1 << 10
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

	entries        []appendOnlyEntry
	baseEntriesLen int
	growEntriesLen int
	latest         map[string]int
	latest64       map[uint64]int
	snapshot       []*appendOnlyEntry
	indexBuf       []int
	valueArena     appendOnlyValueArena
	count          int
	snapCount      int
	sizeBytes      int64

	ordered     bool
	latestDirty bool
	frozen      bool
	hasLast     bool
	lastIdx     int

	predictCapacityHintBytes int
	predictEntryHintSource   *atomic.Int32
	observeEntries           func(int)

	iteratorLeaseMu   sync.Mutex
	iteratorLeaseCond *sync.Cond
	iteratorLeases    int
}

func (*AppendOnly) StableUnsafeIteratorSlices() bool { return true }

func appendOnlyMaxReuseEntries(length int) int {
	maxReuse := appendOnlyEntryPoolMaxCap
	if length <= appendOnlyEntryPoolMaxCap/appendOnlyReuseOversizeFactor {
		target := length
		if target < appendOnlyMinInitialEntries {
			target = appendOnlyMinInitialEntries
		}
		maxReuse = target * appendOnlyReuseOversizeFactor
		if maxReuse > appendOnlyEntryPoolMaxCap {
			maxReuse = appendOnlyEntryPoolMaxCap
		}
	}
	return maxReuse
}

func getAppendOnlyEntriesFromPool(length int, pool *sync.Pool) []appendOnlyEntry {
	if length < 0 {
		length = 0
	}
	if length > appendOnlyEntryPoolMaxCap {
		return make([]appendOnlyEntry, length)
	}
	if pool == nil {
		return make([]appendOnlyEntry, length)
	}
	if v := pool.Get(); v != nil {
		if entries, ok := v.([]appendOnlyEntry); ok && cap(entries) >= length {
			// Prevent huge retained entry slices from being reused for much smaller
			// memtables/iterators, which can otherwise pin large heaps after short-lived
			// spikes (e.g. state-sync restore).
			if cap(entries) <= appendOnlyMaxReuseEntries(length) {
				return entries[:length]
			}
		}
	}
	return make([]appendOnlyEntry, length)
}

func getAppendOnlyEntries(length int) []appendOnlyEntry {
	return getAppendOnlyEntriesFromPool(length, &appendOnlyEntryPool)
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

func appendOnlyGrowthEntriesForHint(baseEntries, entryHint int) int {
	n := baseEntries
	if n < appendOnlyMinInitialEntries {
		n = appendOnlyMinInitialEntries
	}
	entryHint = appendOnlyClampRetainedEntries(entryHint)
	if entryHint <= 0 {
		return n
	}
	if entryHint > n {
		n = entryHint
	}
	return n
}

func appendOnlyClampRetainedEntries(entries int) int {
	if entries <= 0 {
		return 0
	}
	if entries < appendOnlyMinInitialEntries {
		return appendOnlyMinInitialEntries
	}
	if entries > appendOnlyMaxInitialEntries {
		return appendOnlyMaxInitialEntries
	}
	return entries
}

func NewAppendOnlyWithCapacity(capacity int) *AppendOnly {
	return NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, appendOnlyEstimatedBytesPerEntryPointer)
}

func NewAppendOnlyWithCapacityEstimatedEntryBytes(capacity, estimatedBytesPerEntry int) *AppendOnly {
	return NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint(capacity, estimatedBytesPerEntry, 0)
}

func NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint(capacity, estimatedBytesPerEntry, entryHint int) *AppendOnly {
	baseEntries := appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry)
	growEntries := appendOnlyGrowthEntriesForHint(baseEntries, entryHint)
	return &AppendOnly{
		entries:        getAppendOnlyEntries(baseEntries),
		baseEntriesLen: baseEntries,
		growEntriesLen: growEntries,
		count:          0,
		ordered:        true,
		hasLast:        false,
		lastIdx:        -1,
		snapCount:      0,
		sizeBytes:      0,
	}
}

// SetPredictiveGrowthHint configures a best-effort growth forecast for future
// appends. capacityHintBytes is converted to an entry target from the observed
// average entry size; entryHintSource supplies a shared floor learned from other
// append-only memtables. observeEntries is called after the memtable mutex is
// released and must be non-blocking because it can run on the write path.
func (m *AppendOnly) SetPredictiveGrowthHint(capacityHintBytes int, entryHintSource *atomic.Int32, observeEntries func(int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if capacityHintBytes <= 0 {
		capacityHintBytes = defaultMemtableCapacity
	}
	m.predictCapacityHintBytes = capacityHintBytes
	m.predictEntryHintSource = entryHintSource
	if entryHintSource != nil {
		if shared := appendOnlyClampRetainedEntries(int(entryHintSource.Load())); shared > m.growEntriesLen {
			m.growEntriesLen = shared
		}
	}
	m.observeEntries = observeEntries
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

func cloneOrReuseBytes(dst, src []byte, maxCap int) []byte {
	if len(src) == 0 {
		return nil
	}
	if maxCap <= 0 {
		maxCap = len(src)
	}
	if cap(dst) >= len(src) && cap(dst) <= maxCap {
		out := dst[:len(src)]
		copy(out, src)
		return out
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out
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

func (a *appendOnlyValueArena) dropRetained() {
	for i := range a.retained {
		if a.retained[i] != nil {
			putAppendOnlyValueArenaChunk(a.retained[i])
			a.retained[i] = nil
		}
	}
	a.retained = nil
	a.retainedB = 0
}

func appendOnlyNextCapacity(current int) int {
	if current < appendOnlyMinInitialEntries {
		return appendOnlyMinInitialEntries
	}
	next := current * 2
	// Entry-heavy write paths can spend a disproportionate amount of time
	// copying entry arrays during growth, even when values themselves are
	// borrowed from batch arenas or stored inline. Grow more aggressively up to
	// the retained warm-reset window, then fall back to 2x to keep expansion
	// bounded once the memtable is already large.
	if current < appendOnlyAggressiveGrowCutoff {
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

func appendOnlyShouldPredictHint(count int) bool {
	return count >= appendOnlyPredictHintMinEntries && count&(count-1) == 0
}

type appendOnlyObserveEvent struct {
	entries int
	observe func(int)
}

func (e *appendOnlyObserveEvent) record(observe func(int), entries int) {
	if observe == nil || entries <= 0 {
		return
	}
	if e.observe == nil {
		e.observe = observe
	}
	if entries > e.entries {
		e.entries = entries
	}
}

func (e *appendOnlyObserveEvent) recordEvent(other appendOnlyObserveEvent) {
	e.record(other.observe, other.entries)
}

func (e appendOnlyObserveEvent) emit() {
	if e.observe != nil && e.entries > 0 {
		e.observe(e.entries)
	}
}

func (m *AppendOnly) maybeRaisePredictedGrowthHintLocked(event *appendOnlyObserveEvent) {
	if m.predictCapacityHintBytes <= 0 || m.count <= 0 || m.sizeBytes <= 0 {
		return
	}
	avgBytesPerEntry := int((m.sizeBytes + int64(m.count) - 1) / int64(m.count))
	if avgBytesPerEntry <= 0 {
		avgBytesPerEntry = 1
	}
	predictedEntries := appendOnlyClampRetainedEntries(m.predictCapacityHintBytes / avgBytesPerEntry)
	if predictedEntries <= m.growEntriesLen {
		return
	}
	m.growEntriesLen = predictedEntries
	event.record(m.observeEntries, predictedEntries)
}

func (m *AppendOnly) clearSnapshotLocked() {
	if m.snapshot != nil {
		m.snapshot = m.snapshot[:0]
	}
	m.snapCount = 0
}

func (m *AppendOnly) buildSortedLatestIndicesLocked() []int {
	if m.count == 0 || m.ordered {
		return nil
	}
	if m.latestDirty || (len(m.latest) == 0 && len(m.latest64) == 0) {
		m.rebuildLatestIndexLocked()
	}
	// The returned slice aliases m.indexBuf scratch storage. It is only valid
	// while m.mu is held and may be overwritten by the next call. When
	// m.count == 0 or m.ordered is true, this helper returns nil, which callers
	// may treat as "no indices" (equivalent to an empty result).
	need := len(m.latest) + len(m.latest64)
	if cap(m.indexBuf) < need {
		m.indexBuf = make([]int, 0, need)
	} else {
		m.indexBuf = m.indexBuf[:0]
	}
	indices := m.indexBuf
	for _, idx := range m.latest {
		indices = append(indices, idx)
	}
	for _, idx := range m.latest64 {
		indices = append(indices, idx)
	}
	active := m.entries[:m.count]
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(
			appendOnlyEntryKey(&active[indices[i]]),
			appendOnlyEntryKey(&active[indices[j]]),
		) < 0
	})
	return indices
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

func (m *AppendOnly) updateLatestIndexLocked(key []byte, idx int) {
	if idx < 0 || idx >= m.count {
		return
	}
	if k64, ok := appendOnlyKeyU64(key); ok {
		if m.latest64 == nil {
			m.latest64 = make(map[uint64]int, 1)
		}
		m.latest64[k64] = idx
		return
	}
	if m.latest == nil {
		m.latest = make(map[string]int, 1)
	}
	m.latest[appendOnlyKeyString(key)] = idx
}

func (m *AppendOnly) appendEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool, borrowValue bool) appendOnlyObserveEvent {
	var observeEvent appendOnlyObserveEvent
	if m.count == len(m.entries) {
		if m.predictEntryHintSource != nil {
			if shared := appendOnlyClampRetainedEntries(int(m.predictEntryHintSource.Load())); shared > m.growEntriesLen {
				m.growEntriesLen = shared
			}
		}
		nextCap := appendOnlyNextCapacity(len(m.entries))
		if nextCap < m.growEntriesLen {
			nextCap = m.growEntriesLen
		}
		prev := m.entries
		grown := getAppendOnlyEntries(nextCap)
		copy(grown, m.entries[:m.count])
		m.entries = grown
		putAppendOnlyEntries(prev)
		observeEvent.record(m.observeEntries, nextCap)
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
			ent.key = cloneOrReuseBytes(ent.key, key, appendOnlyReusableKeyMaxCap)
		}
		if len(value) > 0 {
			if borrowValue {
				ent.value = value
			} else {
				ent.value = m.valueArena.alloc(len(value))
				copy(ent.value, value)
				ent.valueOwned = true
			}
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
	if appendOnlyShouldPredictHint(m.count) {
		m.maybeRaisePredictedGrowthHintLocked(&observeEvent)
	}

	if !m.hasLast {
		m.lastIdx = idx
		m.hasLast = true
		return observeEvent
	}
	if m.ordered {
		prev := appendOnlyEntryKey(&m.entries[m.lastIdx])
		cmp := bytes.Compare(k, prev)
		if cmp > 0 {
			m.lastIdx = idx
			return observeEvent
		}
		m.ordered = false
		// Once order breaks, invalidate the cached snapshot state and
		// materialize the latest-key index immediately so subsequent unordered
		// appends can maintain it incrementally instead of forcing the next
		// iterator/lookup to rebuild the full map from scratch.
		// rebuildLatestIndexLocked clears the cached snapshot as part of the
		// transition.
		m.rebuildLatestIndexLocked()
		return observeEvent
	}
	if !m.latestDirty {
		m.updateLatestIndexLocked(k, idx)
	}
	m.clearSnapshotLocked()
	return observeEvent
}

func (m *AppendOnly) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	observeEvent := m.appendEntryLocked(key, value, ptr, flags, false, false)
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	observeEvent := m.appendEntryLocked(key, value, ptr, flags, true, false)
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) SetEntryBorrowValue(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	observeEvent := m.appendEntryLocked(key, value, ptr, flags, false, true)
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *AppendOnly) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *AppendOnly) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	k := cloneBytes(key)
	v := cloneBytes(value)
	if cb != nil {
		if err := cb(k, v); err != nil {
			return err
		}
	}
	m.mu.Lock()
	observeEvent := m.appendEntryLocked(k, v, page.ValuePtr{}, node.FlagInline, true, false)
	m.mu.Unlock()
	observeEvent.emit()
	return nil
}

func (m *AppendOnly) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	k := cloneBytes(key)
	if cb != nil {
		if err := cb(k, nil); err != nil {
			return err
		}
	}
	m.mu.Lock()
	observeEvent := m.appendEntryLocked(k, nil, page.ValuePtr{}, node.FlagTombstone, true, false)
	m.mu.Unlock()
	observeEvent.emit()
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
	var observeEvent appendOnlyObserveEvent
	for i := range entries {
		op := entries[i]
		switch {
		case op.Type == batchpkg.OpDelete:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, true, false))
		case op.IsPtr:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, true, false))
		default:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, true, false))
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	observeEvent.emit()
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
	for {
		m.mu.RLock()
		if ent := m.orderedLookupEntryLocked(key); ent != nil {
			deleted := ent.flags&node.FlagTombstone != 0
			val := ent.value
			m.mu.RUnlock()
			if deleted {
				return nil, true, true
			}
			return val, false, true
		}
		if m.ordered {
			m.mu.RUnlock()
			return nil, false, false
		}

		if !m.latestDirty {
			if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
				if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(appendOnlyEntryKey(ent), key) {
						deleted := ent.flags&node.FlagTombstone != 0
						val := ent.value
						m.mu.RUnlock()
						if deleted {
							return nil, true, true
						}
						return val, false, true
					}
				}
			}
			if m.latest != nil {
				if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(appendOnlyEntryKey(ent), key) {
						deleted := ent.flags&node.FlagTombstone != 0
						val := ent.value
						m.mu.RUnlock()
						if deleted {
							return nil, true, true
						}
						return val, false, true
					}
				}
			}
			m.mu.RUnlock()
			return nil, false, false
		}
		m.mu.RUnlock()

		// Unordered but dirty: rebuild the latest-key index to avoid linear scans.
		m.mu.Lock()
		if !m.ordered && m.latestDirty {
			m.rebuildLatestIndexLocked()
		}
		m.mu.Unlock()
	}
}

func (m *AppendOnly) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	for {
		m.mu.RLock()
		if ent := m.orderedLookupEntryLocked(key); ent != nil {
			val := ent.value
			ptr := ent.ptr
			flags := ent.flags
			m.mu.RUnlock()
			return val, ptr, flags, true
		}
		if m.ordered {
			m.mu.RUnlock()
			return nil, page.ValuePtr{}, 0, false
		}

		if !m.latestDirty {
			if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
				if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(appendOnlyEntryKey(ent), key) {
						val := ent.value
						ptr := ent.ptr
						flags := ent.flags
						m.mu.RUnlock()
						return val, ptr, flags, true
					}
				}
			}
			if m.latest != nil {
				if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(appendOnlyEntryKey(ent), key) {
						val := ent.value
						ptr := ent.ptr
						flags := ent.flags
						m.mu.RUnlock()
						return val, ptr, flags, true
					}
				}
			}
			m.mu.RUnlock()
			return nil, page.ValuePtr{}, 0, false
		}
		m.mu.RUnlock()

		// Unordered but dirty: rebuild the latest-key index to avoid linear scans.
		m.mu.Lock()
		if !m.ordered && m.latestDirty {
			m.rebuildLatestIndexLocked()
		}
		m.mu.Unlock()
	}
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
	m.resetLockedWithPolicy(0, 0, 0, true)
}

// ResetWithCapacity resets the memtable and, when needed, shrinks retained
// internal buffers toward the capacity-derived baseline. Unlike Reset, callers
// provide a capacity estimate so post-spike entry retention can decay.
func (m *AppendOnly) ResetWithCapacity(capacity, estimatedBytesPerEntry int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, 0, true)
}

func (m *AppendOnly) ResetWithCapacityAndEntryHint(capacity, estimatedBytesPerEntry, entryHint int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, entryHint, true)
}

func (m *AppendOnly) resetLocked(capacity, estimatedBytesPerEntry int) {
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, 0, true)
}

// ResetWithCapacityHard resets and clamps retained internal buffers to the
// capacity-derived baseline (without carrying over recent spike cardinality).
func (m *AppendOnly) ResetWithCapacityHard(capacity, estimatedBytesPerEntry int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, 0, false)
}

func (m *AppendOnly) ResetWithCapacityHardAndEntryHint(capacity, estimatedBytesPerEntry, entryHint int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, entryHint, false)
}

func (m *AppendOnly) resetLockedWithPolicy(capacity, estimatedBytesPerEntry, entryHint int, retainObserved bool) {
	desiredEntries := m.baseEntriesLen
	if capacity > 0 {
		desiredEntries = appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry)
		m.baseEntriesLen = desiredEntries
	}
	if desiredEntries <= 0 {
		desiredEntries = appendOnlyMinInitialEntries
		m.baseEntriesLen = desiredEntries
	}
	// Hints should improve the next growth jump and warm-retention ceiling, but
	// reset itself must not allocate just to satisfy the hint.
	growthEntries := appendOnlyGrowthEntriesForHint(desiredEntries, entryHint)
	m.growEntriesLen = growthEntries

	oldCount := m.count
	maxRetainedEntries := appendOnlyMaxReuseEntries(growthEntries)
	retainedEntries := desiredEntries
	if retainObserved && oldCount > retainedEntries {
		retainedEntries = oldCount
		if retainedEntries > maxRetainedEntries {
			retainedEntries = maxRetainedEntries
		}
	}
	for i := 0; i < m.count; i++ {
		ent := &m.entries[i]
		ent.ptr = page.ValuePtr{}
		ent.flags = 0
		ent.keyInline = false
		ent.valueOwned = false
		if cap(ent.key) > appendOnlyReusableKeyMaxCap {
			ent.key = nil
		} else if ent.key != nil {
			ent.key = ent.key[:0]
		}
		ent.value = nil
	}
	m.valueArena.reset()
	if !retainObserved {
		m.valueArena.dropRetained()
	}
	// Clear small maps in-place; drop large ones so they don't pin hash tables
	// after one-off spikes.
	if !retainObserved || (oldCount > 0 && oldCount >= appendOnlyResetDropThresholdEntries) {
		m.latest = nil
		m.latest64 = nil
	} else {
		clear(m.latest)
		clear(m.latest64)
	}
	// Snapshot/index buffers are only needed for unordered memtables; drop large
	// ones on reset to keep post-spike memory bounded.
	if !retainObserved || (cap(m.snapshot) > 0 && cap(m.snapshot) >= appendOnlyResetDropThresholdEntries) {
		m.snapshot = nil
		m.snapCount = 0
	} else {
		m.clearSnapshotLocked()
	}
	if !retainObserved || (cap(m.indexBuf) > 0 && cap(m.indexBuf) >= appendOnlyResetDropThresholdEntries) {
		m.indexBuf = nil
	} else if m.indexBuf != nil {
		m.indexBuf = m.indexBuf[:0]
	}
	m.count = 0
	m.sizeBytes = 0
	m.ordered = true
	m.latestDirty = false
	m.frozen = false
	m.hasLast = false
	m.lastIdx = -1

	// If the entry slice grew far beyond the configured baseline, shrink it.
	// This avoids permanently ratcheting heap high-water when a workload briefly
	// spikes in write volume (common during state-sync restore), while still
	// keeping the next steady-state cycle warm if we just observed a larger-but-
	// still-bounded mutable memtable.
	if cap(m.entries) > maxRetainedEntries {
		m.replaceEntriesSlice(retainedEntries)
		return
	}
	if cap(m.entries) < retainedEntries {
		m.replaceEntriesSlice(retainedEntries)
		return
	}
	reuseEntries := retainedEntries
	if cap(m.entries) <= maxRetainedEntries && cap(m.entries) > reuseEntries {
		reuseEntries = cap(m.entries)
	}
	if len(m.entries) != reuseEntries {
		m.entries = m.entries[:reuseEntries]
	}
}

func (m *AppendOnly) replaceEntriesSlice(length int) {
	if length < 0 {
		length = 0
	}
	prev := m.entries
	m.entries = getAppendOnlyEntries(length)
	putAppendOnlyEntries(prev)
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
	indices := m.buildSortedLatestIndicesLocked()
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

func (m *AppendOnly) buildMutableSortedIteratorEntriesLocked() []appendOnlyEntry {
	if m.count == 0 {
		m.clearSnapshotLocked()
		return getAppendOnlyIteratorEntries(0)
	}
	indices := m.buildSortedLatestIndicesLocked()
	active := m.entries[:m.count]
	entries := getAppendOnlyIteratorEntries(len(indices))
	for i, idx := range indices {
		entries[i] = active[idx]
	}
	m.indexBuf = indices[:0]
	m.clearSnapshotLocked()
	return entries
}

func (m *AppendOnly) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	if m.ordered {
		entries := m.entries[:m.count]
		it := &appendOnlyIterator{
			entries: entries,
			start:   start,
			end:     end,
			mu:      &m.mu,
		}
		if start != nil {
			it.Seek(start)
		}
		return it
	}
	m.mu.RUnlock()

	// Unordered iterators need a sorted latest-key view. Frozen memtables can
	// share a cached pointer snapshot, while mutable memtables materialize a
	// per-iterator copy without pinning an extra shared []*entry snapshot.
	m.mu.Lock()
	if m.frozen {
		snapshotPtrs := m.buildSortedLatestSnapshotLocked()
		m.acquireIteratorLeaseLocked()
		m.mu.Unlock()
		it := &appendOnlyIterator{
			entryPtrs:       snapshotPtrs,
			start:           start,
			end:             end,
			pooledEntryPtrs: false,
			leaseOwner:      m,
			leaseHeld:       true,
			reverse:         false,
		}
		if start != nil {
			it.Seek(start)
		}
		return it
	}
	entries := m.buildMutableSortedIteratorEntriesLocked()
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries:       entries,
		start:         start,
		end:           end,
		pooledEntries: true,
		reverse:       false,
	}
	if start != nil {
		it.Seek(start)
	}
	return it
}

func (m *AppendOnly) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	if m.ordered {
		entries := m.entries[:m.count]
		it := &appendOnlyIterator{
			entries: entries,
			start:   start,
			end:     end,
			mu:      &m.mu,
			reverse: true,
		}
		it.seekToReverseEnd(end)
		return it
	}
	m.mu.RUnlock()

	// Unordered iterators need a sorted latest-key view. Frozen memtables can
	// share a cached pointer snapshot, while mutable memtables materialize a
	// per-iterator copy without pinning an extra shared []*entry snapshot.
	m.mu.Lock()
	if m.frozen {
		snapshotPtrs := m.buildSortedLatestSnapshotLocked()
		m.acquireIteratorLeaseLocked()
		m.mu.Unlock()
		it := &appendOnlyIterator{
			entryPtrs:       snapshotPtrs,
			start:           start,
			end:             end,
			pooledEntryPtrs: false,
			leaseOwner:      m,
			leaseHeld:       true,
			reverse:         true,
		}
		it.seekToReverseEnd(end)
		return it
	}
	entries := m.buildMutableSortedIteratorEntriesLocked()
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries:       entries,
		start:         start,
		end:           end,
		pooledEntries: true,
		reverse:       true,
	}
	it.seekToReverseEnd(end)
	return it
}

type appendOnlyIterator struct {
	entries         []appendOnlyEntry
	entryPtrs       []*appendOnlyEntry
	idx             int
	start           []byte
	end             []byte
	mu              *sync.RWMutex
	pooledEntries   bool
	pooledEntryPtrs bool
	leaseOwner      *AppendOnly
	leaseHeld       bool
	reverse         bool
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
	if it.start != nil && bytes.Compare(appendOnlyEntryKey(ent), it.start) < 0 {
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
	if it.reverse {
		it.idx--
		return
	}
	if it.idx < it.len() {
		it.idx++
	}
}

func (it *appendOnlyIterator) Seek(key []byte) {
	if !it.reverse {
		it.idx = sort.Search(it.len(), func(i int) bool {
			ent := it.entryAt(i)
			if ent == nil {
				return true
			}
			return bytes.Compare(appendOnlyEntryKey(ent), key) >= 0
		})
		return
	}

	if key == nil || (it.end != nil && bytes.Compare(key, it.end) >= 0) {
		it.seekToReverseEnd(it.end)
		return
	}

	// Reverse Seek positions at the greatest key <= target key.
	n := it.len()
	pos := sort.Search(n, func(i int) bool {
		ent := it.entryAt(i)
		if ent == nil {
			return true
		}
		return bytes.Compare(appendOnlyEntryKey(ent), key) > 0
	})
	it.idx = pos - 1
}

func (it *appendOnlyIterator) seekToReverseEnd(end []byte) {
	n := it.len()
	if n == 0 {
		it.idx = -1
		return
	}
	if end == nil {
		it.idx = n - 1
		return
	}
	pos := sort.Search(n, func(i int) bool {
		ent := it.entryAt(i)
		if ent == nil {
			return true
		}
		return bytes.Compare(appendOnlyEntryKey(ent), end) >= 0
	})
	it.idx = pos - 1
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
