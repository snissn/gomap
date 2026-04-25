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
	appendOnlyKeyPoolMaxCap                 = 1 << 20
	appendOnlyValuePoolMaxCap               = 1 << 20
	appendOnlyPtrPayloadPoolMaxCap          = 1 << 20
	appendOnlyIteratorPoolMaxCap            = 1 << 20
	appendOnlyIteratorKeyPoolMaxCap         = 1 << 20
	appendOnlyIteratorValuePoolMaxCap       = 1 << 20
	appendOnlyIteratorPtrPayloadPoolMaxCap  = 1 << 20
	appendOnlyIteratorPtrPoolMaxCap         = 1 << 20
	appendOnlyValueArenaMinShift            = 12
	appendOnlyValueArenaMaxShift            = 20
	appendOnlyValueArenaClassCount          = appendOnlyValueArenaMaxShift - appendOnlyValueArenaMinShift + 1
	appendOnlyValueArenaDefaultChunk        = 32 << 10
	appendOnlyValueArenaGrowthMaxChunk      = 64 << 10
	appendOnlyValueArenaPoolMaxCap          = 1 << appendOnlyValueArenaMaxShift
	appendOnlyValueArenaRetainMaxCap        = 4 << 20
	appendOnlyValueArenaRetainChunks        = 128
	appendOnlyReuseOversizeFactor           = 4
	appendOnlyResetDropThresholdEntries     = 1 << 15
	appendOnlyAggressiveGrowCutoff          = appendOnlyResetDropThresholdEntries * 2
	appendOnlyPredictHintMinEntries         = 1 << 10
	appendOnlyRecentValueDedupeMaxLen       = 256
)

var appendOnlyEntryPool sync.Pool
var appendOnlyKeyPool sync.Pool
var appendOnlyValuePool sync.Pool
var appendOnlyPtrPayloadPool sync.Pool
var appendOnlyIteratorPool sync.Pool
var appendOnlyIteratorKeyPool sync.Pool
var appendOnlyIteratorValuePool sync.Pool
var appendOnlyIteratorPtrPayloadPool sync.Pool
var appendOnlyIteratorPtrPool sync.Pool
var appendOnlyValueArenaPools [appendOnlyValueArenaClassCount]sync.Pool
var appendOnlyEmptyKey = make([]byte, 0)

type appendOnlyPointerPayload struct {
	value string
	ptr   page.ValuePtr
}

type appendOnlyEntry struct {
	inlineKey    [appendOnlyInlineKeyLen]byte
	keyIndex     uint32
	payloadIndex uint32
}

type appendOnlyInlineMapKey struct {
	key    [appendOnlyInlineKeyLen]byte
	length uint8
}

type appendOnlyIteratorKeyRef struct {
	idx    int
	offset int
	length int
}

const (
	appendOnlyEntryInlineLenShift  = 28
	appendOnlyEntryInlineLenMask   = uint32(0xF) << appendOnlyEntryInlineLenShift
	appendOnlyEntryKeyIndexMask    = ^appendOnlyEntryInlineLenMask
	appendOnlyEntryInlineFlagsMask = uint32(0xFF)
)

func appendOnlyEntryInlineKeyLen(ent *appendOnlyEntry) int {
	if ent == nil {
		return 0
	}
	return int((ent.keyIndex & appendOnlyEntryInlineLenMask) >> appendOnlyEntryInlineLenShift)
}

func appendOnlyEntrySetInlineKeyLen(ent *appendOnlyEntry, length int) {
	if ent == nil {
		return
	}
	if length < 0 || length > appendOnlyInlineKeyLen {
		panic("appendOnlyEntry inline key length out of range")
	}
	ent.keyIndex = (ent.keyIndex & appendOnlyEntryKeyIndexMask) |
		(uint32(length) << appendOnlyEntryInlineLenShift)
}

func appendOnlyEntryKeyIndex(ent *appendOnlyEntry) uint32 {
	if ent == nil {
		return 0
	}
	if appendOnlyEntryInlineKeyLen(ent) != 0 {
		return 0
	}
	return ent.keyIndex & appendOnlyEntryKeyIndexMask
}

func appendOnlyEntrySetKeyIndex(ent *appendOnlyEntry, idx uint32) {
	if ent == nil {
		return
	}
	if idx > appendOnlyEntryKeyIndexMask {
		panic("appendOnlyEntry key index overflow")
	}
	ent.keyIndex = (ent.keyIndex & appendOnlyEntryInlineLenMask) | idx
}

func appendOnlyEntryFlags(ent *appendOnlyEntry) byte {
	if ent == nil {
		return 0
	}
	if appendOnlyEntryInlineKeyLen(ent) != 0 {
		return byte(ent.keyIndex & appendOnlyEntryInlineFlagsMask)
	}
	if appendOnlyEntryKeyIndex(ent) != 0 {
		return ent.inlineKey[0]
	}
	return 0
}

func appendOnlyEntrySetFlags(ent *appendOnlyEntry, flags byte) {
	if ent == nil {
		return
	}
	if appendOnlyEntryInlineKeyLen(ent) != 0 {
		ent.keyIndex = (ent.keyIndex &^ appendOnlyEntryInlineFlagsMask) | uint32(flags)
		return
	}
	if appendOnlyEntryKeyIndex(ent) != 0 {
		ent.inlineKey[0] = flags
		return
	}
}

func appendOnlyEntryPayloadIndex(ent *appendOnlyEntry) uint32 {
	if ent == nil {
		return 0
	}
	return ent.payloadIndex
}

func appendOnlyEntrySetPayloadIndex(ent *appendOnlyEntry, idx uint32) {
	if ent == nil {
		return
	}
	ent.payloadIndex = idx
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
	keys           []string
	values         []string
	lastValueAlias bool
	ptrPayloads    []appendOnlyPointerPayload
	baseEntriesLen int
	growEntriesLen int
	latest         map[string]int
	latestInline   map[appendOnlyInlineMapKey]int
	latest64       map[uint64]int
	snapshot       []*appendOnlyEntry
	indexBuf       []int
	valueArena     appendOnlyValueArena
	count          int
	deleteCount    int
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

func (m *AppendOnly) Ordered() bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	ordered := m.ordered
	m.mu.RUnlock()
	return ordered
}

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

func getAppendOnlySliceFromPool[T any](length int, pool *sync.Pool, maxCap, defaultMaxCap int) []T {
	if length < 0 {
		length = 0
	}
	if maxCap <= 0 {
		maxCap = defaultMaxCap
	}
	if length > maxCap {
		return make([]T, length)
	}
	if pool == nil {
		return make([]T, length)
	}
	if v := pool.Get(); v != nil {
		if items, ok := v.([]T); ok && cap(items) >= length {
			if cap(items) <= appendOnlyMaxReuseEntries(length) {
				out := items[:length]
				clear(out)
				return out
			}
		}
	}
	return make([]T, length)
}

func putAppendOnlySliceToPool[T any](items []T, pool *sync.Pool, maxCap int) {
	if items == nil || cap(items) == 0 || cap(items) > maxCap {
		return
	}
	full := items[:cap(items)]
	clear(full)
	pool.Put(full[:0])
}

func getAppendOnlyKeysFromPool(length int, pool *sync.Pool, maxCap int) []string {
	return getAppendOnlySliceFromPool[string](length, pool, maxCap, appendOnlyKeyPoolMaxCap)
}

func getAppendOnlyKeys(length int) []string {
	return getAppendOnlyKeysFromPool(length, &appendOnlyKeyPool, appendOnlyKeyPoolMaxCap)
}

func putAppendOnlyKeys(keys []string) {
	putAppendOnlySliceToPool(keys, &appendOnlyKeyPool, appendOnlyKeyPoolMaxCap)
}

func getAppendOnlyValuesFromPool(length int, pool *sync.Pool, maxCap int) []string {
	return getAppendOnlySliceFromPool[string](length, pool, maxCap, appendOnlyValuePoolMaxCap)
}

func getAppendOnlyValues(length int) []string {
	return getAppendOnlyValuesFromPool(length, &appendOnlyValuePool, appendOnlyValuePoolMaxCap)
}

func putAppendOnlyValues(values []string) {
	putAppendOnlySliceToPool(values, &appendOnlyValuePool, appendOnlyValuePoolMaxCap)
}

func getAppendOnlyPtrPayloadsFromPool(length int, pool *sync.Pool, maxCap int) []appendOnlyPointerPayload {
	return getAppendOnlySliceFromPool[appendOnlyPointerPayload](length, pool, maxCap, appendOnlyPtrPayloadPoolMaxCap)
}

func getAppendOnlyPtrPayloads(length int) []appendOnlyPointerPayload {
	return getAppendOnlyPtrPayloadsFromPool(length, &appendOnlyPtrPayloadPool, appendOnlyPtrPayloadPoolMaxCap)
}

func putAppendOnlyPtrPayloads(payloads []appendOnlyPointerPayload) {
	putAppendOnlySliceToPool(payloads, &appendOnlyPtrPayloadPool, appendOnlyPtrPayloadPoolMaxCap)
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

func getAppendOnlyIteratorKeys(length int) []string {
	return getAppendOnlyKeysFromPool(length, &appendOnlyIteratorKeyPool, appendOnlyIteratorKeyPoolMaxCap)
}

func putAppendOnlyIteratorKeys(keys []string) {
	putAppendOnlySliceToPool(keys, &appendOnlyIteratorKeyPool, appendOnlyIteratorKeyPoolMaxCap)
}

func getAppendOnlyIteratorValues(length int) []string {
	return getAppendOnlyValuesFromPool(length, &appendOnlyIteratorValuePool, appendOnlyIteratorValuePoolMaxCap)
}

func putAppendOnlyIteratorValues(values []string) {
	putAppendOnlySliceToPool(values, &appendOnlyIteratorValuePool, appendOnlyIteratorValuePoolMaxCap)
}

func getAppendOnlyIteratorPtrPayloads(length int) []appendOnlyPointerPayload {
	return getAppendOnlyPtrPayloadsFromPool(length, &appendOnlyIteratorPtrPayloadPool, appendOnlyIteratorPtrPayloadPoolMaxCap)
}

func putAppendOnlyIteratorPtrPayloads(payloads []appendOnlyPointerPayload) {
	putAppendOnlySliceToPool(payloads, &appendOnlyIteratorPtrPayloadPool, appendOnlyIteratorPtrPayloadPoolMaxCap)
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
	if n > appendOnlyMaxInitialEntries {
		n = appendOnlyMaxInitialEntries
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

// NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint creates an append-only
// memtable using the capacity-derived baseline plus entryHint as the expected
// upcoming entry count. The hint does not allocate entries immediately; it only
// influences the next growth jump and bounded reuse after reset.
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
// appends. A positive capacityHintBytes is converted to an entry target from the
// observed average entry size; a non-positive value disables capacity-based
// prediction while still allowing entryHintSource and observeEntries to be
// configured. entryHintSource supplies a shared floor learned from other
// append-only memtables. observeEntries is called after the memtable mutex is
// released and must be non-blocking because it can run on the write path.
func (m *AppendOnly) SetPredictiveGrowthHint(capacityHintBytes int, entryHintSource *atomic.Int32, observeEntries func(int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if capacityHintBytes <= 0 {
		capacityHintBytes = 0
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

func appendOnlyEntryKeyFromKeys(ent *appendOnlyEntry, keys []string) []byte {
	if ent == nil {
		return nil
	}
	inlineLen := int(ent.keyIndex >> appendOnlyEntryInlineLenShift)
	if inlineLen != 0 {
		return ent.inlineKey[:inlineLen]
	}
	keyIndex := ent.keyIndex & appendOnlyEntryKeyIndexMask
	if keyIndex == 0 {
		return nil
	}
	idx := int(keyIndex - 1)
	if idx < 0 || idx >= len(keys) {
		return nil
	}
	key := keys[idx]
	if len(key) == 0 {
		return appendOnlyEmptyKey
	}
	return unsafe.Slice(unsafe.StringData(key), len(key))
}

func appendOnlyValueStringBytes(value string) []byte {
	if len(value) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(value), len(value))
}

func appendOnlyStringFromBytes(src []byte) string {
	if len(src) == 0 {
		return ""
	}
	return unsafe.String(&src[0], len(src))
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func appendOnlyArenaStringCopy(arena *appendOnlyValueArena, src []byte) string {
	if len(src) == 0 {
		return ""
	}
	buf := arena.alloc(len(src))
	copy(buf, src)
	return appendOnlyStringFromBytes(buf)
}

func appendOnlyStringEqualBytes(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	if len(b) == 0 {
		return true
	}
	// Cheaply reject non-matches before comparing the full payload. This keeps
	// the repeated-value fast path cheap for random small values.
	if s[0] != b[0] || s[len(s)-1] != b[len(b)-1] {
		return false
	}
	return bytes.Equal(appendOnlyValueStringBytes(s), b)
}

func (m *AppendOnly) appendOnlyEntryKey(ent *appendOnlyEntry) []byte {
	if m == nil {
		return appendOnlyEntryKeyFromKeys(ent, nil)
	}
	return appendOnlyEntryKeyFromKeys(ent, m.keys)
}

func (m *AppendOnly) appendOnlyEntryValue(ent *appendOnlyEntry) []byte {
	if m == nil || ent == nil {
		return nil
	}
	payloadIndex := ent.payloadIndex
	if payloadIndex == 0 {
		return nil
	}
	idx := int(payloadIndex - 1)
	if appendOnlyEntryFlags(ent)&node.FlagPointer != 0 {
		if idx < 0 || idx >= len(m.ptrPayloads) {
			return nil
		}
		return appendOnlyValueStringBytes(m.ptrPayloads[idx].value)
	}
	if idx < 0 || idx >= len(m.values) {
		return nil
	}
	return appendOnlyValueStringBytes(m.values[idx])
}

func (m *AppendOnly) appendOnlyEntryPtr(ent *appendOnlyEntry) page.ValuePtr {
	if m == nil || ent == nil {
		return page.ValuePtr{}
	}
	if appendOnlyEntryFlags(ent)&node.FlagPointer == 0 {
		return page.ValuePtr{}
	}
	payloadIndex := ent.payloadIndex
	if payloadIndex == 0 {
		return page.ValuePtr{}
	}
	idx := int(payloadIndex - 1)
	if idx >= 0 && idx < len(m.ptrPayloads) {
		return m.ptrPayloads[idx].ptr
	}
	return page.ValuePtr{}
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
		chunkCap := length
		if chunkCap < appendOnlyValueArenaDefaultChunk {
			chunkCap = appendOnlyValueArenaDefaultChunk
		}
		if n := len(a.chunks); n > 0 {
			prevCap := cap(a.chunks[n-1])
			if prevCap > chunkCap {
				chunkCap = prevCap
			}
			if prevCap < appendOnlyValueArenaGrowthMaxChunk {
				grownCap := prevCap << 1
				if grownCap > appendOnlyValueArenaGrowthMaxChunk {
					grownCap = appendOnlyValueArenaGrowthMaxChunk
				}
				if grownCap > chunkCap {
					chunkCap = grownCap
				}
			}
		}
		chunk := a.popRetained(chunkCap)
		if chunk == nil {
			chunk = getAppendOnlyValueArenaChunk(chunkCap)
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
	if current < appendOnlyAggressiveGrowCutoff {
		next = current * 4
	}
	if next <= current {
		return current + appendOnlyMinInitialEntries
	}
	return next
}

func appendOnlyLookupKeyString(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(key), len(key))
}

func appendOnlyInlineMapKeyFromBytes(key []byte) (appendOnlyInlineMapKey, bool) {
	if len(key) > appendOnlyInlineKeyLen {
		return appendOnlyInlineMapKey{}, false
	}
	var mapKey appendOnlyInlineMapKey
	mapKey.length = uint8(len(key))
	copy(mapKey.key[:], key)
	return mapKey, true
}

func appendOnlyInlineMapKeyFromEntry(ent *appendOnlyEntry) (appendOnlyInlineMapKey, bool) {
	if ent == nil || appendOnlyEntryKeyIndex(ent) != 0 {
		return appendOnlyInlineMapKey{}, false
	}
	inlineLen := appendOnlyEntryInlineKeyLen(ent)
	if inlineLen < 0 || inlineLen > appendOnlyInlineKeyLen {
		return appendOnlyInlineMapKey{}, false
	}
	var mapKey appendOnlyInlineMapKey
	mapKey.length = uint8(inlineLen)
	copy(mapKey.key[:], ent.inlineKey[:inlineLen])
	return mapKey, true
}

func (m *AppendOnly) appendOnlyEntryMapKey(ent *appendOnlyEntry) string {
	if m == nil || ent == nil {
		return ""
	}
	if inlineLen := appendOnlyEntryInlineKeyLen(ent); inlineLen != 0 {
		return string(ent.inlineKey[:inlineLen])
	}
	keyIndex := appendOnlyEntryKeyIndex(ent)
	if keyIndex == 0 {
		return ""
	}
	idx := int(keyIndex - 1)
	if idx < 0 || idx >= len(m.keys) {
		return ""
	}
	return m.keys[idx]
}

func appendOnlyKeyU64(key []byte) (uint64, bool) {
	if len(key) != appendOnlyInlineKeyLen {
		return 0, false
	}
	return binary.BigEndian.Uint64(key), true
}

func entryValueSize(flags byte, valueLen int) int {
	if flags&node.FlagPointer != 0 {
		return page.ValuePtrSize + valueLen
	}
	if flags&node.FlagTombstone != 0 {
		return 0
	}
	return valueLen
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
		defer func() {
			_ = recover()
		}()
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
	predictedEntries := appendOnlyClampRetainedEntries(1 + (m.predictCapacityHintBytes-1)/avgBytesPerEntry)
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
	if m.latestDirty || (len(m.latest) == 0 && len(m.latestInline) == 0 && len(m.latest64) == 0) {
		m.rebuildLatestIndexLocked()
	}
	// The returned slice aliases m.indexBuf scratch storage. It is only valid
	// while m.mu is held and may be overwritten by the next call. When
	// m.count == 0 or m.ordered is true, this helper returns nil, which callers
	// may treat as "no indices" (equivalent to an empty result).
	need := len(m.latest) + len(m.latestInline) + len(m.latest64)
	if cap(m.indexBuf) < need {
		m.indexBuf = make([]int, 0, need)
	} else {
		m.indexBuf = m.indexBuf[:0]
	}
	indices := m.indexBuf
	for _, idx := range m.latest {
		indices = append(indices, idx)
	}
	for _, idx := range m.latestInline {
		indices = append(indices, idx)
	}
	for _, idx := range m.latest64 {
		indices = append(indices, idx)
	}
	active := m.entries[:m.count]
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(
			m.appendOnlyEntryKey(&active[indices[i]]),
			m.appendOnlyEntryKey(&active[indices[j]]),
		) < 0
	})
	return indices
}

func (m *AppendOnly) rebuildLatestIndexLocked() {
	if m.count == 0 {
		if m.latest != nil {
			clear(m.latest)
		}
		if m.latestInline != nil {
			clear(m.latestInline)
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
	if m.latestInline != nil {
		clear(m.latestInline)
	}
	if m.latest64 != nil {
		clear(m.latest64)
	}
	reserve := m.count
	active := m.entries[:m.count]
	for i := range active {
		if inlineKey, ok := appendOnlyInlineMapKeyFromEntry(&active[i]); ok {
			if inlineKey.length == appendOnlyInlineKeyLen {
				if m.latest64 == nil {
					m.latest64 = make(map[uint64]int, reserve)
				}
				m.latest64[binary.BigEndian.Uint64(inlineKey.key[:])] = i
				continue
			}
			if m.latestInline == nil {
				m.latestInline = make(map[appendOnlyInlineMapKey]int, reserve)
			}
			m.latestInline[inlineKey] = i
			continue
		}
		k := m.appendOnlyEntryKey(&active[i])
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
		m.latest[m.appendOnlyEntryMapKey(&active[i])] = i
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
	if inlineKey, ok := appendOnlyInlineMapKeyFromBytes(key); ok {
		if m.latestInline == nil {
			m.latestInline = make(map[appendOnlyInlineMapKey]int, 1)
		}
		m.latestInline[inlineKey] = idx
		return
	}
	if m.latest == nil {
		m.latest = make(map[string]int, 1)
	}
	m.latest[m.appendOnlyEntryMapKey(&m.entries[idx])] = idx
}

func (m *AppendOnly) appendKeyLocked(key string) uint32 {
	if len(m.keys) == cap(m.keys) {
		nextCap := appendOnlyMinInitialEntries
		if cap(m.keys) > 0 {
			nextCap = appendOnlyNextCapacity(cap(m.keys))
		}
		if nextCap < len(m.keys)+1 {
			nextCap = len(m.keys) + 1
		}
		prev := m.keys
		grown := getAppendOnlyKeys(nextCap)
		copy(grown, prev)
		m.keys = grown[:len(prev)]
		putAppendOnlyKeys(prev)
	}
	m.keys = append(m.keys, key)
	return uint32(len(m.keys))
}

func (m *AppendOnly) appendEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool, borrowValue bool) appendOnlyObserveEvent {
	var observeEvent appendOnlyObserveEvent
	grewEntries := false
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
		grewEntries = true
	}
	idx := m.count
	m.count++
	if grewEntries {
		observeEvent.record(m.observeEntries, m.count)
	}
	ent := &m.entries[idx]
	ent.keyIndex = 0
	ent.payloadIndex = 0
	if len(key) == 0 {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(""))
	} else if len(key) <= appendOnlyInlineKeyLen {
		copy(ent.inlineKey[:], key)
		appendOnlyEntrySetInlineKeyLen(ent, len(key))
	} else if steal {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(appendOnlyStringFromBytes(key)))
	} else {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(appendOnlyArenaStringCopy(&m.valueArena, key)))
	}
	appendOnlyEntrySetFlags(ent, flags)
	payloadValueLen := 0
	if flags&node.FlagTombstone != 0 {
		m.deleteCount++
	} else if flags&node.FlagPointer != 0 {
		payloadValue := ""
		if len(value) > 0 {
			payloadValueLen = len(value)
			if steal || borrowValue {
				payloadValue = appendOnlyStringFromBytes(value)
			} else {
				payloadValue = appendOnlyArenaStringCopy(&m.valueArena, value)
			}
		}
		appendOnlyEntrySetPayloadIndex(ent, m.appendPtrPayloadLocked(payloadValue, ptr))
	} else if len(value) > 0 {
		payloadValueLen = len(value)
		appendOnlyEntrySetPayloadIndex(ent, m.appendValueBytesLocked(value, steal || borrowValue))
	}
	k := m.appendOnlyEntryKey(ent)
	m.sizeBytes += int64(len(k) + entryValueSize(flags, payloadValueLen))
	if appendOnlyShouldPredictHint(m.count) {
		m.maybeRaisePredictedGrowthHintLocked(&observeEvent)
	}

	if !m.hasLast {
		m.lastIdx = idx
		m.hasLast = true
		return observeEvent
	}
	if m.ordered {
		prev := m.appendOnlyEntryKey(&m.entries[m.lastIdx])
		cmp := bytes.Compare(k, prev)
		if cmp > 0 {
			m.lastIdx = idx
			return observeEvent
		}
		m.ordered = false
		// Keep the write path purely append-only. The latest-key index is only
		// materialized if a read or iterator needs the unordered view.
		m.latestDirty = true
		m.clearSnapshotLocked()
		return observeEvent
	}
	if !m.latestDirty {
		m.updateLatestIndexLocked(k, idx)
	}
	m.clearSnapshotLocked()
	return observeEvent
}

func (m *AppendOnly) canAppendTrustedOrderedBatchLocked(firstKey []byte) bool {
	if !m.ordered {
		return false
	}
	if !m.hasLast {
		return true
	}
	return bytes.Compare(firstKey, m.appendOnlyEntryKey(&m.entries[m.lastIdx])) > 0
}

func (m *AppendOnly) appendEntryTrustedOrderedLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool, borrowValue bool) appendOnlyObserveEvent {
	var observeEvent appendOnlyObserveEvent
	grewEntries := false
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
		grewEntries = true
	}
	idx := m.count
	m.count++
	if grewEntries {
		observeEvent.record(m.observeEntries, m.count)
	}
	ent := &m.entries[idx]
	ent.keyIndex = 0
	ent.payloadIndex = 0
	if len(key) == 0 {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(""))
	} else if len(key) <= appendOnlyInlineKeyLen {
		copy(ent.inlineKey[:], key)
		appendOnlyEntrySetInlineKeyLen(ent, len(key))
	} else if steal {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(appendOnlyStringFromBytes(key)))
	} else {
		appendOnlyEntrySetKeyIndex(ent, m.appendKeyLocked(appendOnlyArenaStringCopy(&m.valueArena, key)))
	}
	appendOnlyEntrySetFlags(ent, flags)
	payloadValueLen := 0
	if flags&node.FlagTombstone != 0 {
		m.deleteCount++
	} else if flags&node.FlagPointer != 0 {
		payloadValue := ""
		if len(value) > 0 {
			payloadValueLen = len(value)
			if steal || borrowValue {
				payloadValue = appendOnlyStringFromBytes(value)
			} else {
				payloadValue = appendOnlyArenaStringCopy(&m.valueArena, value)
			}
		}
		appendOnlyEntrySetPayloadIndex(ent, m.appendPtrPayloadLocked(payloadValue, ptr))
	} else if len(value) > 0 {
		payloadValueLen = len(value)
		appendOnlyEntrySetPayloadIndex(ent, m.appendValueBytesLocked(value, steal || borrowValue))
	}
	m.sizeBytes += int64(len(key) + entryValueSize(flags, payloadValueLen))
	if appendOnlyShouldPredictHint(m.count) {
		m.maybeRaisePredictedGrowthHintLocked(&observeEvent)
	}
	m.lastIdx = idx
	m.hasLast = true
	return observeEvent
}

func (m *AppendOnly) appendValueBytesLocked(value []byte, borrowed bool) uint32 {
	if len(value) == 0 {
		return 0
	}
	if borrowed {
		idx := m.appendValueLocked(appendOnlyStringFromBytes(value))
		m.lastValueAlias = true
		return idx
	}
	if idx := m.recentValueIndexLocked(value); idx != 0 {
		return idx
	}
	if len(m.values) == 0 {
		idx := m.appendValueLockedSmall(appendOnlyArenaStringCopy(&m.valueArena, value))
		m.lastValueAlias = false
		return idx
	}
	idx := m.appendValueLocked(appendOnlyArenaStringCopy(&m.valueArena, value))
	m.lastValueAlias = false
	return idx
}

func (m *AppendOnly) recentValueIndexLocked(value []byte) uint32 {
	if len(value) == 0 || len(value) > appendOnlyRecentValueDedupeMaxLen || len(m.values) == 0 || m.lastValueAlias {
		return 0
	}
	idx := len(m.values) - 1
	if appendOnlyStringEqualBytes(m.values[idx], value) {
		return uint32(idx + 1)
	}
	return 0
}

func (m *AppendOnly) appendValueLocked(value string) uint32 {
	return m.appendValueLockedWithPolicy(value, true)
}

func (m *AppendOnly) appendValueLockedSmall(value string) uint32 {
	return m.appendValueLockedWithPolicy(value, false)
}

func (m *AppendOnly) appendValueLockedWithPolicy(value string, useEntryCapHint bool) uint32 {
	if len(m.values) == cap(m.values) {
		nextCap := appendOnlyMinInitialEntries
		if cap(m.values) > 0 {
			nextCap = appendOnlyNextCapacity(cap(m.values))
			if useEntryCapHint {
				if entriesCap := cap(m.entries); entriesCap > nextCap {
					if entriesCap > appendOnlyResetDropThresholdEntries {
						entriesCap = appendOnlyResetDropThresholdEntries
					}
					nextCap = entriesCap
				}
			}
		} else if useEntryCapHint && len(m.values) == m.count-1 {
			if entriesCap := cap(m.entries); entriesCap > nextCap {
				if entriesCap > appendOnlyResetDropThresholdEntries {
					entriesCap = appendOnlyResetDropThresholdEntries
				}
				nextCap = entriesCap
			}
		}
		if nextCap < len(m.values)+1 {
			nextCap = len(m.values) + 1
		}
		prev := m.values
		grown := getAppendOnlyValues(nextCap)
		copy(grown, prev)
		m.values = grown[:len(prev)]
		putAppendOnlyValues(prev)
	}
	m.values = append(m.values, value)
	return uint32(len(m.values))
}

func (m *AppendOnly) appendPtrPayloadLocked(value string, ptr page.ValuePtr) uint32 {
	if len(m.ptrPayloads) == cap(m.ptrPayloads) {
		nextCap := appendOnlyMinInitialEntries
		if cap(m.ptrPayloads) > 0 {
			nextCap = appendOnlyNextCapacity(cap(m.ptrPayloads))
		}
		if nextCap < len(m.ptrPayloads)+1 {
			nextCap = len(m.ptrPayloads) + 1
		}
		prev := m.ptrPayloads
		grown := getAppendOnlyPtrPayloads(nextCap)
		copy(grown, prev)
		m.ptrPayloads = grown[:len(prev)]
		putAppendOnlyPtrPayloads(prev)
	}
	m.ptrPayloads = append(m.ptrPayloads, appendOnlyPointerPayload{
		value: value,
		ptr:   ptr,
	})
	return uint32(len(m.ptrPayloads))
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
	m.applyStealBatchTrusted(entries, onKey)
}

func (m *AppendOnly) ApplyBorrowValueSortedBatch(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.applyBorrowValueBatch(entries, storeInlinePtrValues, onKey)
}

func (m *AppendOnly) ApplyBorrowValueSortedBatchTrusted(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.applyBorrowValueBatchTrusted(entries, storeInlinePtrValues, onKey)
}

func (m *AppendOnly) ApplyStealSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.applyStealBatchIndicesTrusted(entries, idxs, onKey)
}

func (m *AppendOnly) ApplyBorrowValueSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.applyBorrowValueBatchIndicesTrusted(entries, idxs, storeInlinePtrValues, onKey)
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

func (m *AppendOnly) applyStealBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	if len(entries) > 0 && m.canAppendTrustedOrderedBatchLocked(entries[0].Key) {
		for i := range entries {
			op := entries[i]
			switch {
			case op.Type == batchpkg.OpDelete:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, true, false))
			case op.IsPtr:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, true, false))
			default:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, true, false))
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		m.mu.Unlock()
		observeEvent.emit()
		return
	}
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

func (m *AppendOnly) applyStealBatchIndices(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	for _, idx := range idxs {
		op := entries[idx]
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

func (m *AppendOnly) applyStealBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	if len(idxs) > 0 && m.canAppendTrustedOrderedBatchLocked(entries[idxs[0]].Key) {
		for _, idx := range idxs {
			op := entries[idx]
			switch {
			case op.Type == batchpkg.OpDelete:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, true, false))
			case op.IsPtr:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, true, false))
			default:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, true, false))
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		m.mu.Unlock()
		observeEvent.emit()
		return
	}
	for _, idx := range idxs {
		op := entries[idx]
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

func (m *AppendOnly) applyBorrowValueBatch(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	for i := range entries {
		op := entries[i]
		switch {
		case op.Type == batchpkg.OpDelete:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
		case op.IsPtr:
			memVal := []byte(nil)
			if storeInlinePtrValues {
				memVal = op.Value
			}
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
		default:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) applyBorrowValueBatchTrusted(entries []batchpkg.Entry, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	if len(entries) > 0 && m.canAppendTrustedOrderedBatchLocked(entries[0].Key) {
		for i := range entries {
			op := entries[i]
			switch {
			case op.Type == batchpkg.OpDelete:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
			case op.IsPtr:
				memVal := []byte(nil)
				if storeInlinePtrValues {
					memVal = op.Value
				}
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
			default:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		m.mu.Unlock()
		observeEvent.emit()
		return
	}
	for i := range entries {
		op := entries[i]
		switch {
		case op.Type == batchpkg.OpDelete:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
		case op.IsPtr:
			memVal := []byte(nil)
			if storeInlinePtrValues {
				memVal = op.Value
			}
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
		default:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) applyBorrowValueBatchIndices(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	for _, idx := range idxs {
		op := entries[idx]
		switch {
		case op.Type == batchpkg.OpDelete:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
		case op.IsPtr:
			memVal := []byte(nil)
			if storeInlinePtrValues {
				memVal = op.Value
			}
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
		default:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	observeEvent.emit()
}

func (m *AppendOnly) applyBorrowValueBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	var observeEvent appendOnlyObserveEvent
	if len(idxs) > 0 && m.canAppendTrustedOrderedBatchLocked(entries[idxs[0]].Key) {
		for _, idx := range idxs {
			op := entries[idx]
			switch {
			case op.Type == batchpkg.OpDelete:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
			case op.IsPtr:
				memVal := []byte(nil)
				if storeInlinePtrValues {
					memVal = op.Value
				}
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
			default:
				observeEvent.recordEvent(m.appendEntryTrustedOrderedLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		m.mu.Unlock()
		observeEvent.emit()
		return
	}
	for _, idx := range idxs {
		op := entries[idx]
		switch {
		case op.Type == batchpkg.OpDelete:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, false, false))
		case op.IsPtr:
			memVal := []byte(nil)
			if storeInlinePtrValues {
				memVal = op.Value
			}
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, memVal, op.ValuePtr, node.FlagPointer, false, len(memVal) > 0))
		default:
			observeEvent.recordEvent(m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, false, len(op.Value) > 0))
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
		return bytes.Compare(m.appendOnlyEntryKey(&active[i]), key) >= 0
	})
	if idx >= len(active) {
		return nil
	}
	ent := &active[idx]
	if !bytes.Equal(m.appendOnlyEntryKey(ent), key) {
		return nil
	}
	return ent
}

func (m *AppendOnly) Get(key []byte) ([]byte, bool, bool) {
	for {
		m.mu.RLock()
		if ent := m.orderedLookupEntryLocked(key); ent != nil {
			deleted := appendOnlyEntryFlags(ent)&node.FlagTombstone != 0
			val := m.appendOnlyEntryValue(ent)
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
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						deleted := appendOnlyEntryFlags(ent)&node.FlagTombstone != 0
						val := m.appendOnlyEntryValue(ent)
						m.mu.RUnlock()
						if deleted {
							return nil, true, true
						}
						return val, false, true
					}
				}
			}
			if inlineKey, ok := appendOnlyInlineMapKeyFromBytes(key); ok && m.latestInline != nil {
				if idx, ok := m.latestInline[inlineKey]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						deleted := appendOnlyEntryFlags(ent)&node.FlagTombstone != 0
						val := m.appendOnlyEntryValue(ent)
						m.mu.RUnlock()
						if deleted {
							return nil, true, true
						}
						return val, false, true
					}
				}
			}
			if m.latest != nil {
				if idx, ok := m.latest[appendOnlyLookupKeyString(key)]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						deleted := appendOnlyEntryFlags(ent)&node.FlagTombstone != 0
						val := m.appendOnlyEntryValue(ent)
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
			val := m.appendOnlyEntryValue(ent)
			ptr := m.appendOnlyEntryPtr(ent)
			flags := appendOnlyEntryFlags(ent)
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
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						val := m.appendOnlyEntryValue(ent)
						ptr := m.appendOnlyEntryPtr(ent)
						flags := appendOnlyEntryFlags(ent)
						m.mu.RUnlock()
						return val, ptr, flags, true
					}
				}
			}
			if inlineKey, ok := appendOnlyInlineMapKeyFromBytes(key); ok && m.latestInline != nil {
				if idx, ok := m.latestInline[inlineKey]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						val := m.appendOnlyEntryValue(ent)
						ptr := m.appendOnlyEntryPtr(ent)
						flags := appendOnlyEntryFlags(ent)
						m.mu.RUnlock()
						return val, ptr, flags, true
					}
				}
			}
			if m.latest != nil {
				if idx, ok := m.latest[appendOnlyLookupKeyString(key)]; ok && idx >= 0 && idx < m.count {
					ent := &m.entries[idx]
					if bytes.Equal(m.appendOnlyEntryKey(ent), key) {
						val := m.appendOnlyEntryValue(ent)
						ptr := m.appendOnlyEntryPtr(ent)
						flags := appendOnlyEntryFlags(ent)
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

func (m *AppendOnly) OperationMix() OperationMix {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return OperationMix{
		Entries: m.count,
		Deletes: m.deleteCount,
	}
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

// ResetWithCapacityAndEntryHint resets the memtable like ResetWithCapacity, but
// also records entryHint as the expected upcoming entry count. The hint is used
// to influence the next growth jump and the ceiling for retained buffer reuse.
// It does not cause reset to allocate entries immediately.
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

// ResetWithCapacityHardAndEntryHint resets the memtable like
// ResetWithCapacityHard, but records entryHint as the expected upcoming entry
// count for the next growth jump. The hint does not allocate entries during
// reset and hard reset still drops observed spike retention.
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
	oldValueCount := len(m.values)
	oldPtrPayloadCount := len(m.ptrPayloads)
	maxRetainedEntries := appendOnlyMaxReuseEntries(growthEntries)
	retainedEntries := desiredEntries
	if retainObserved && oldCount > retainedEntries {
		retainedEntries = oldCount
		if retainedEntries > maxRetainedEntries {
			retainedEntries = maxRetainedEntries
		}
	}
	for i := 0; i < m.count; i++ {
		m.entries[i] = appendOnlyEntry{}
	}
	if !retainObserved || cap(m.keys) > maxRetainedEntries {
		putAppendOnlyKeys(m.keys)
		m.keys = nil
	} else {
		clear(m.keys)
		m.keys = m.keys[:0]
	}
	if !retainObserved || cap(m.values) > maxRetainedEntries {
		putAppendOnlyValues(m.values)
		m.values = nil
	} else {
		clear(m.values)
		m.values = m.values[:0]
	}
	m.lastValueAlias = false
	if !retainObserved || cap(m.ptrPayloads) > maxRetainedEntries {
		putAppendOnlyPtrPayloads(m.ptrPayloads)
		m.ptrPayloads = nil
	} else {
		clear(m.ptrPayloads)
		m.ptrPayloads = m.ptrPayloads[:0]
	}
	m.valueArena.reset()
	if !retainObserved {
		m.valueArena.dropRetained()
	}
	// Clear small maps in-place; drop large ones so they don't pin hash tables
	// after one-off spikes.
	if !retainObserved || (oldCount > 0 && oldCount >= appendOnlyResetDropThresholdEntries) {
		m.latest = nil
		m.latestInline = nil
		m.latest64 = nil
	} else {
		clear(m.latest)
		clear(m.latestInline)
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
	m.deleteCount = 0
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
	} else if cap(m.entries) < retainedEntries {
		m.replaceEntriesSlice(retainedEntries)
	} else {
		reuseEntries := retainedEntries
		if cap(m.entries) <= maxRetainedEntries && cap(m.entries) > reuseEntries {
			reuseEntries = cap(m.entries)
		}
		if len(m.entries) != reuseEntries {
			m.entries = m.entries[:reuseEntries]
		}
	}
	maxRetainedValues := maxRetainedEntries
	if !retainObserved {
		maxRetainedValues = 0
	}
	retainedValues := 0
	if retainObserved && oldValueCount > 0 {
		retainedValues = oldValueCount
		if retainedValues > maxRetainedValues {
			retainedValues = maxRetainedValues
		}
	}
	if cap(m.values) > maxRetainedValues {
		m.replaceValuesSlice(retainedValues)
	} else if cap(m.values) < retainedValues {
		m.replaceValuesSlice(retainedValues)
	}

	maxRetainedPtrPayloads := maxRetainedEntries
	if !retainObserved {
		maxRetainedPtrPayloads = 0
	}
	retainedPtrPayloads := 0
	if retainObserved && oldPtrPayloadCount > 0 {
		retainedPtrPayloads = oldPtrPayloadCount
		if retainedPtrPayloads > maxRetainedPtrPayloads {
			retainedPtrPayloads = maxRetainedPtrPayloads
		}
	}
	if cap(m.ptrPayloads) > maxRetainedPtrPayloads {
		m.replacePtrPayloadsSlice(retainedPtrPayloads)
		return
	}
	if cap(m.ptrPayloads) < retainedPtrPayloads {
		m.replacePtrPayloadsSlice(retainedPtrPayloads)
		return
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

func (m *AppendOnly) replaceValuesSlice(length int) {
	if length < 0 {
		length = 0
	}
	prev := m.values
	if length == 0 {
		m.values = nil
		m.lastValueAlias = false
		putAppendOnlyValues(prev)
		return
	}
	m.values = getAppendOnlyValues(length)
	m.values = m.values[:0]
	m.lastValueAlias = false
	putAppendOnlyValues(prev)
}

func (m *AppendOnly) replacePtrPayloadsSlice(length int) {
	if length < 0 {
		length = 0
	}
	prev := m.ptrPayloads
	if length == 0 {
		m.ptrPayloads = nil
		putAppendOnlyPtrPayloads(prev)
		return
	}
	m.ptrPayloads = getAppendOnlyPtrPayloads(length)
	m.ptrPayloads = m.ptrPayloads[:0]
	putAppendOnlyPtrPayloads(prev)
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

func (m *AppendOnly) buildMutableSortedIteratorEntriesLocked() ([]appendOnlyEntry, []string, []string, []appendOnlyPointerPayload, []byte) {
	if m.count == 0 {
		m.clearSnapshotLocked()
		return getAppendOnlyIteratorEntries(0), getAppendOnlyIteratorKeys(0), getAppendOnlyIteratorValues(0), getAppendOnlyIteratorPtrPayloads(0), nil
	}
	indices := m.buildSortedLatestIndicesLocked()
	active := m.entries[:m.count]
	entries := getAppendOnlyIteratorEntries(len(indices))
	keyCount := 0
	valueCount := 0
	ptrPayloadCount := 0
	inlineKeyBytes := 0
	for _, idx := range indices {
		inlineLen := appendOnlyEntryInlineKeyLen(&active[idx])
		if inlineLen != 0 || appendOnlyEntryKeyIndex(&active[idx]) != 0 {
			keyCount++
		}
		inlineKeyBytes += inlineLen
		if active[idx].payloadIndex != 0 {
			if appendOnlyEntryFlags(&active[idx])&node.FlagPointer != 0 {
				ptrPayloadCount++
			} else {
				valueCount++
			}
		}
	}
	keys := getAppendOnlyIteratorKeys(keyCount)
	values := getAppendOnlyIteratorValues(valueCount)
	ptrPayloads := getAppendOnlyIteratorPtrPayloads(ptrPayloadCount)
	var keyBytes []byte
	if inlineKeyBytes > 0 {
		keyBytes = make([]byte, inlineKeyBytes)
	}
	nextKey := 0
	nextKeyByte := 0
	nextValue := 0
	nextPtrPayload := 0
	for i, idx := range indices {
		ent := active[idx]
		if inlineLen := appendOnlyEntryInlineKeyLen(&ent); inlineLen != 0 {
			flags := appendOnlyEntryFlags(&ent)
			next := nextKeyByte + inlineLen
			copy(keyBytes[nextKeyByte:next], ent.inlineKey[:inlineLen])
			keys[nextKey] = appendOnlyStringFromBytes(keyBytes[nextKeyByte:next])
			nextKeyByte = next
			ent.inlineKey = [appendOnlyInlineKeyLen]byte{}
			appendOnlyEntrySetInlineKeyLen(&ent, 0)
			appendOnlyEntrySetKeyIndex(&ent, uint32(nextKey+1))
			appendOnlyEntrySetFlags(&ent, flags)
			nextKey++
		} else if keyIndex := appendOnlyEntryKeyIndex(&ent); keyIndex != 0 {
			keys[nextKey] = m.keys[keyIndex-1]
			appendOnlyEntrySetKeyIndex(&ent, uint32(nextKey+1))
			nextKey++
		}
		if payloadIndex := ent.payloadIndex; payloadIndex != 0 {
			if appendOnlyEntryFlags(&ent)&node.FlagPointer != 0 {
				ptrPayloads[nextPtrPayload] = m.ptrPayloads[payloadIndex-1]
				appendOnlyEntrySetPayloadIndex(&ent, uint32(nextPtrPayload+1))
				nextPtrPayload++
			} else {
				values[nextValue] = m.values[payloadIndex-1]
				appendOnlyEntrySetPayloadIndex(&ent, uint32(nextValue+1))
				nextValue++
			}
		}
		entries[i] = ent
	}
	m.indexBuf = indices[:0]
	m.clearSnapshotLocked()
	return entries, keys, values, ptrPayloads, keyBytes
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
			owner:   m,
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
			owner:           m,
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
	entries, keys, values, ptrPayloads, keyBytes := m.buildMutableSortedIteratorEntriesLocked()
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries:       entries,
		keys:          keys,
		keyBytes:      keyBytes,
		values:        values,
		ptrPayloads:   ptrPayloads,
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
			owner:   m,
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
			owner:           m,
			pooledEntryPtrs: false,
			leaseOwner:      m,
			leaseHeld:       true,
			reverse:         true,
		}
		it.seekToReverseEnd(end)
		return it
	}
	entries, keys, values, ptrPayloads, keyBytes := m.buildMutableSortedIteratorEntriesLocked()
	m.mu.Unlock()

	it := &appendOnlyIterator{
		entries:       entries,
		keys:          keys,
		keyBytes:      keyBytes,
		values:        values,
		ptrPayloads:   ptrPayloads,
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
	keys            []string
	keyBytes        []byte
	values          []string
	ptrPayloads     []appendOnlyPointerPayload
	entryPtrs       []*appendOnlyEntry
	keyRefs         []appendOnlyIteratorKeyRef
	keyRefIndex     map[int]int
	idx             int
	start           []byte
	end             []byte
	mu              *sync.RWMutex
	owner           *AppendOnly
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

func (it *appendOnlyIterator) keyForEntry(ent *appendOnlyEntry) []byte {
	if ent == nil {
		return nil
	}
	if it.owner != nil {
		return it.owner.appendOnlyEntryKey(ent)
	}
	return appendOnlyEntryKeyFromKeys(ent, it.keys)
}

func (it *appendOnlyIterator) cachedStableKeyForIndex(idx int) ([]byte, bool) {
	if n := len(it.keyRefs); n > 0 {
		last := &it.keyRefs[n-1]
		if last.idx == idx {
			return it.keyBytes[last.offset : last.offset+last.length], true
		}
	}
	if it.keyRefIndex != nil {
		if pos, ok := it.keyRefIndex[idx]; ok {
			ref := &it.keyRefs[pos]
			return it.keyBytes[ref.offset : ref.offset+ref.length], true
		}
	} else {
		for i := len(it.keyRefs) - 1; i >= 0; i-- {
			if it.keyRefs[i].idx == idx {
				ref := &it.keyRefs[i]
				return it.keyBytes[ref.offset : ref.offset+ref.length], true
			}
		}
		if len(it.keyRefs) >= 8 {
			it.keyRefIndex = make(map[int]int, len(it.keyRefs)+1)
			for i := range it.keyRefs {
				it.keyRefIndex[it.keyRefs[i].idx] = i
			}
		}
	}
	return nil, false
}

func (it *appendOnlyIterator) stableKeyForEntry(idx int, ent *appendOnlyEntry) []byte {
	if ent == nil {
		return nil
	}
	if cached, ok := it.cachedStableKeyForIndex(idx); ok {
		return cached
	}
	key := it.keyForEntry(ent)
	if len(key) == 0 {
		return appendOnlyEmptyKey
	}
	offset := len(it.keyBytes)
	it.keyBytes = append(it.keyBytes, key...)
	it.keyRefs = append(it.keyRefs, appendOnlyIteratorKeyRef{
		idx:    idx,
		offset: offset,
		length: len(key),
	})
	if it.keyRefIndex != nil {
		it.keyRefIndex[idx] = len(it.keyRefs) - 1
	}
	return it.keyBytes[offset : offset+len(key)]
}

func (it *appendOnlyIterator) validIndex() bool {
	if it.idx < 0 || it.idx >= it.len() {
		return false
	}
	ent := it.entryAt(it.idx)
	if ent == nil {
		return false
	}
	if it.start != nil && bytes.Compare(it.keyForEntry(ent), it.start) < 0 {
		return false
	}
	if it.end != nil && bytes.Compare(it.keyForEntry(ent), it.end) >= 0 {
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
			return bytes.Compare(it.keyForEntry(ent), key) >= 0
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
		return bytes.Compare(it.keyForEntry(ent), key) > 0
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
		return bytes.Compare(it.keyForEntry(ent), end) >= 0
	})
	it.idx = pos - 1
}

func (it *appendOnlyIterator) UnsafeKey() []byte {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil
	}
	return it.stableKeyForEntry(it.idx, ent)
}

func (it *appendOnlyIterator) UnsafeStableKey() []byte {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil
	}
	key := it.keyForEntry(ent)
	if len(key) == 0 {
		return appendOnlyEmptyKey
	}
	return key
}

func (it *appendOnlyIterator) UnsafeValue() []byte {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil
	}
	flags := appendOnlyEntryFlags(ent)
	if flags&node.FlagTombstone != 0 {
		return nil
	}
	if it.owner != nil {
		return it.owner.appendOnlyEntryValue(ent)
	}
	idx := int(ent.payloadIndex - 1)
	if idx < 0 {
		return nil
	}
	if flags&node.FlagPointer != 0 {
		if idx >= len(it.ptrPayloads) {
			return nil
		}
		return appendOnlyValueStringBytes(it.ptrPayloads[idx].value)
	}
	if idx >= len(it.values) {
		return nil
	}
	return appendOnlyValueStringBytes(it.values[idx])
}

func (it *appendOnlyIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil, page.ValuePtr{}, 0
	}
	if it.owner != nil {
		return it.owner.appendOnlyEntryValue(ent), it.owner.appendOnlyEntryPtr(ent), appendOnlyEntryFlags(ent)
	}
	flags := appendOnlyEntryFlags(ent)
	idx := int(ent.payloadIndex - 1)
	if idx < 0 {
		return nil, page.ValuePtr{}, flags
	}
	if flags&node.FlagPointer != 0 {
		if idx >= len(it.ptrPayloads) {
			return nil, page.ValuePtr{}, flags
		}
		payload := it.ptrPayloads[idx]
		return appendOnlyValueStringBytes(payload.value), payload.ptr, flags
	}
	if idx >= len(it.values) {
		return nil, page.ValuePtr{}, flags
	}
	return appendOnlyValueStringBytes(it.values[idx]), page.ValuePtr{}, flags
}

func (it *appendOnlyIterator) IsDeleted() bool {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return false
	}
	return appendOnlyEntryFlags(ent)&node.FlagTombstone != 0
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
		putAppendOnlyIteratorKeys(it.keys)
		putAppendOnlyIteratorValues(it.values)
		putAppendOnlyIteratorPtrPayloads(it.ptrPayloads)
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
	it.owner = nil
	it.entries = nil
	it.keys = nil
	it.keyBytes = nil
	it.values = nil
	it.ptrPayloads = nil
	it.keyRefs = nil
	it.keyRefIndex = nil
	it.entryPtrs = nil
	return nil
}

func (it *appendOnlyIterator) Domain() (start, end []byte) { return nil, it.end }
