package memtable

import (
	"bytes"
	"encoding/binary"
	"errors"
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
	appendOnlyInlineKeyLen                  = 8
	// Pool size-class boundaries. Min/Max initial entries are derived from
	// these shifts so the three constants can't drift out of sync.
	appendOnlyEntryPoolMinShift      = 7
	appendOnlyEntryPoolMaxShift      = 20
	appendOnlyEntryPoolClassCount    = appendOnlyEntryPoolMaxShift - appendOnlyEntryPoolMinShift + 1
	appendOnlyEntryPoolMaxCap        = 1 << appendOnlyEntryPoolMaxShift // 1 << 20
	appendOnlyEntryPoolRetainMaxCap  = 1 << 17
	appendOnlyMinInitialEntries      = 1 << appendOnlyEntryPoolMinShift // 128
	appendOnlyMaxInitialEntries      = 1 << appendOnlyEntryPoolMaxShift // 1 << 20
	appendOnlyIteratorPoolMaxCap     = 1 << 20
	appendOnlyIteratorPtrPoolMaxCap  = 1 << 20
	appendOnlyReusableKeyMaxCap      = 1 << 10
	appendOnlyKeyArenaDefaultChunk   = 2 << 10
	appendOnlyValueArenaMinShift     = 12
	appendOnlyValueArenaMaxShift     = 20
	appendOnlyValueArenaClassCount   = appendOnlyValueArenaMaxShift - appendOnlyValueArenaMinShift + 1
	appendOnlyValueArenaDefaultChunk = 32 << 10
	appendOnlyValueArenaPoolMaxCap   = 1 << appendOnlyValueArenaMaxShift
	appendOnlyValueArenaRetainMaxCap = 4 << 20
	appendOnlyValueArenaRetainChunks = 128
	appendOnlySortedRunMaxCount      = 256
	// When random writes break append-only ordering, the table falls back to a
	// latest-key map. Pre-size that map from the table's steady-state entry hint
	// (bounded above) so hot random-write ingestion does not repeatedly grow maps
	// from tiny capacity after the sorted-run fast path is exhausted.
	appendOnlyLatestIndexMaxReserve     = 8 << 10
	appendOnlyReuseOversizeFactor       = 4
	appendOnlyResetDropThresholdEntries = 1 << 15
	appendOnlyAggressiveGrowCutoff      = appendOnlyResetDropThresholdEntries * 2
	// Reserve calls usually know the near-term write batch size. Keep amortized
	// headroom so append-heavy commit paths do not repeatedly grow and copy the
	// entry slice while filling a large mutable memtable.
	appendOnlyReserveHeadroomDivisor = 1
)

var appendOnlyEntryPoolRetainBudgetBytes = uint64(256 << 20)
var appendOnlyValueArenaPoolRetainBudgetBytes = uint64(64 << 20)

// Serializes package-pool replacement and retained entry backing accounting.
// Entry slices are held in bounded strong bins because sync.Pool can discard
// warm buffers at GC boundaries, which makes durable write-profile reuse
// unreliable.
var appendOnlyEntryPoolMu sync.Mutex
var appendOnlyEntryPoolPtrs [appendOnlyEntryPoolClassCount]atomic.Pointer[sync.Pool]
var appendOnlyEntryPoolBins [appendOnlyEntryPoolClassCount][][]appendOnlyEntry
var appendOnlyEntryPoolRetainedBytes atomic.Uint64
var appendOnlyEntryPoolRetainedBytesMax atomic.Uint64
var appendOnlyEntryPoolGetTotal atomic.Uint64
var appendOnlyEntryPoolPutTotal atomic.Uint64
var appendOnlyEntryPoolDropTotal atomic.Uint64
var appendOnlyEntryPoolDropBytesTotal atomic.Uint64
var appendOnlyEntryPoolAdmissionDropTotal atomic.Uint64
var appendOnlyEntryPoolAdmissionDropBytesTotal atomic.Uint64
var appendOnlyEntryReserveCallsTotal atomic.Uint64
var appendOnlyEntryReserveEntriesTotal atomic.Uint64
var appendOnlyEntryReserveGrowCallsTotal atomic.Uint64
var appendOnlyEntryReserveGrowBytesTotal atomic.Uint64
var appendOnlyEntryReserveSkippedGrowthAllocsTotal atomic.Uint64
var appendOnlyEntryReserveSkippedGrowthBytesTotal atomic.Uint64
var appendOnlyValueArenaPoolPtrs [appendOnlyValueArenaClassCount]atomic.Pointer[sync.Pool]
var appendOnlyValueArenaPoolRetainedBytes atomic.Uint64
var appendOnlyValueArenaPoolRetainedBytesMax atomic.Uint64
var appendOnlyValueArenaPoolGetTotal atomic.Uint64
var appendOnlyValueArenaPoolPutTotal atomic.Uint64
var appendOnlyValueArenaPoolDropTotal atomic.Uint64
var appendOnlyValueArenaPoolDropBytesTotal atomic.Uint64
var appendOnlyValueArenaPoolAdmissionDropTotal atomic.Uint64
var appendOnlyValueArenaPoolAdmissionDropBytesTotal atomic.Uint64
var appendOnlyIteratorPool sync.Pool
var appendOnlyIteratorPtrPool sync.Pool

type appendOnlyEntry struct {
	key         []byte
	value       []byte
	ptr         page.ValuePtr
	revision    page.EntryRevision
	inlineKey   [appendOnlyInlineKeyLen]byte
	flags       byte
	keyInline   bool
	keyArena    bool
	keyReusable bool
	valueOwned  bool
}

type appendOnlyValueArena struct {
	chunks    [][]byte
	retained  [][]byte
	retainedB int
	cur       []byte
	curPos    int
}

type appendOnlySortedRun struct {
	start int
	end   int
}

type appendOnlySortedRunCursor struct {
	run int
	idx int
	end int
}

type appendOnlyKeyArena struct {
	chunks [][]byte
	cur    []byte
	curPos int
}

type AppendOnly struct {
	mu sync.RWMutex

	entries        []appendOnlyEntry
	baseEntriesLen int
	orderedKey64   []uint64
	latest         map[string]int
	latest64       map[uint64]int
	sortedRuns     []appendOnlySortedRun
	runCursorBuf   []appendOnlySortedRunCursor
	runMergeBuf    []appendOnlySortedRunCursor
	snapshot       []*appendOnlyEntry
	indexBuf       []int
	keyArena       appendOnlyKeyArena
	valueArena     appendOnlyValueArena
	count          int
	snapCount      int
	sizeBytes      int64

	ordered     bool
	latestDirty bool
	frozen      bool
	frozenFast  atomic.Bool
	hasLast     bool
	lastIdx     int

	iteratorLeaseMu   sync.Mutex
	iteratorLeaseCond *sync.Cond
	iteratorLeases    int
}

type AppendOnlyEntryPoolStats struct {
	RetainedBytesEstimate    uint64
	RetainedBytesMaxEstimate uint64
	GetsTotal                uint64
	PutsTotal                uint64
	DropsTotal               uint64
	DropBytesTotal           uint64
	AdmissionDropsTotal      uint64
	AdmissionDropBytesTotal  uint64
}

type AppendOnlyEntryReserveStats struct {
	CallsTotal               uint64
	EntriesTotal             uint64
	GrowCallsTotal           uint64
	GrowBytesTotal           uint64
	SkippedGrowthAllocsTotal uint64
	SkippedGrowthBytesTotal  uint64
}

type AppendOnlyValueArenaStats struct {
	ActiveChunks   int64
	ActiveBytes    int64
	RetainedChunks int64
	RetainedBytes  int64
}

type AppendOnlyValueArenaPoolStats struct {
	RetainedBytesEstimate    uint64
	RetainedBytesMaxEstimate uint64
	GetsTotal                uint64
	PutsTotal                uint64
	DropsTotal               uint64
	DropBytesTotal           uint64
	AdmissionDropsTotal      uint64
	AdmissionDropBytesTotal  uint64
}

func (*AppendOnly) StableUnsafeIteratorSlices() bool { return true }

// PreferSortedPointProbes reports whether a sorted batch of point probes should
// use individual GetEntry lookups instead of one iterator scan. Frozen
// append-only tables can answer point probes without locks; for sparse random
// batches this avoids scanning the gaps between sorted keys.
func (m *AppendOnly) PreferSortedPointProbes(first, last []byte, refCount int) bool {
	if m == nil || refCount <= 0 || !m.frozenFast.Load() {
		return false
	}
	if refCount == 1 {
		return true
	}
	if first64, ok := appendOnlyKeyU64(first); ok {
		if last64, ok := appendOnlyKeyU64(last); ok && last64 >= first64 {
			span := last64 - first64 + 1
			if span <= uint64(refCount*4) {
				return false
			}
		}
	}
	return refCount*4 < m.count || refCount <= 256
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

func appendOnlyEntryPoolClassForLength(length int) (int, int, bool) {
	if length < 0 {
		length = 0
	}
	if length > appendOnlyEntryPoolRetainMaxCap {
		return 0, 0, false
	}
	capacity := appendOnlyMinInitialEntries
	if length > appendOnlyMinInitialEntries {
		capacity = 1 << bits.Len(uint(length-1))
	}
	if capacity > appendOnlyEntryPoolRetainMaxCap {
		return 0, 0, false
	}
	class := bits.Len(uint(capacity)) - 1 - appendOnlyEntryPoolMinShift
	if class < 0 || class >= appendOnlyEntryPoolClassCount {
		return 0, 0, false
	}
	return class, capacity, true
}

func appendOnlyEntryPoolClassForReusableCapacity(capacity int) (int, bool) {
	if capacity < appendOnlyMinInitialEntries || capacity > appendOnlyEntryPoolRetainMaxCap {
		return 0, false
	}
	class, _, ok := appendOnlyEntryPoolClassForLength(capacity)
	if !ok {
		return 0, false
	}
	return class, true
}

func appendOnlyEntryPoolForClass(class int) *sync.Pool {
	if class < 0 || class >= len(appendOnlyEntryPoolPtrs) {
		return nil
	}
	if pool := appendOnlyEntryPoolPtrs[class].Load(); pool != nil {
		return pool
	}
	pool := &sync.Pool{}
	if appendOnlyEntryPoolPtrs[class].CompareAndSwap(nil, pool) {
		return pool
	}
	return appendOnlyEntryPoolPtrs[class].Load()
}

func appendOnlyEntryPoolBytes(capacity int) uint64 {
	if capacity <= 0 {
		return 0
	}
	return uint64(capacity) * uint64(unsafe.Sizeof(appendOnlyEntry{}))
}

func tryAddAppendOnlyEntryPoolRetainedBytes(bytes uint64) bool {
	if bytes == 0 {
		return true
	}
	budget := appendOnlyEntryPoolRetainBudgetBytes
	if budget == 0 {
		return false
	}
	for {
		current := appendOnlyEntryPoolRetainedBytes.Load()
		if current >= budget || bytes > budget-current {
			return false
		}
		next := current + bytes
		if appendOnlyEntryPoolRetainedBytes.CompareAndSwap(current, next) {
			for {
				prev := appendOnlyEntryPoolRetainedBytesMax.Load()
				if next <= prev || appendOnlyEntryPoolRetainedBytesMax.CompareAndSwap(prev, next) {
					return true
				}
			}
		}
	}
}

func recordAppendOnlyEntryPoolAdmissionDrop(bytes uint64) {
	appendOnlyEntryPoolAdmissionDropTotal.Add(1)
	if bytes > 0 {
		appendOnlyEntryPoolAdmissionDropBytesTotal.Add(bytes)
	}
}

func subtractAppendOnlyEntryPoolRetainedBytes(bytes uint64) {
	if bytes == 0 {
		return
	}
	for {
		current := appendOnlyEntryPoolRetainedBytes.Load()
		if current == 0 {
			return
		}
		next := uint64(0)
		if current > bytes {
			next = current - bytes
		}
		if appendOnlyEntryPoolRetainedBytes.CompareAndSwap(current, next) {
			return
		}
	}
}

func dropAppendOnlyEntryPoolsLocked() {
	for i := range appendOnlyEntryPoolPtrs {
		appendOnlyEntryPoolPtrs[i].Store(&sync.Pool{})
		appendOnlyEntryPoolBins[i] = nil
	}
	if dropped := appendOnlyEntryPoolRetainedBytes.Swap(0); dropped > 0 {
		appendOnlyEntryPoolDropBytesTotal.Add(dropped)
	}
	appendOnlyEntryPoolDropTotal.Add(1)
}

// DropAppendOnlyEntryPools abandons package-level append-only entry slice pools.
// It is intended for cold transitions away from append-only mutable memtables,
// where retaining restore-sized warm buffers hurts steady-state RSS more than it
// helps near-term reuse.
func DropAppendOnlyEntryPools() {
	appendOnlyEntryPoolMu.Lock()
	defer appendOnlyEntryPoolMu.Unlock()

	dropAppendOnlyEntryPoolsLocked()
}

func AppendOnlyEntryPoolDropTotal() uint64 {
	return appendOnlyEntryPoolDropTotal.Load()
}

func AppendOnlyEntryPoolStatsSnapshot() AppendOnlyEntryPoolStats {
	return AppendOnlyEntryPoolStats{
		RetainedBytesEstimate:    appendOnlyEntryPoolRetainedBytes.Load(),
		RetainedBytesMaxEstimate: appendOnlyEntryPoolRetainedBytesMax.Load(),
		GetsTotal:                appendOnlyEntryPoolGetTotal.Load(),
		PutsTotal:                appendOnlyEntryPoolPutTotal.Load(),
		DropsTotal:               appendOnlyEntryPoolDropTotal.Load(),
		DropBytesTotal:           appendOnlyEntryPoolDropBytesTotal.Load(),
		AdmissionDropsTotal:      appendOnlyEntryPoolAdmissionDropTotal.Load(),
		AdmissionDropBytesTotal:  appendOnlyEntryPoolAdmissionDropBytesTotal.Load(),
	}
}

func AppendOnlyEntryReserveStatsSnapshot() AppendOnlyEntryReserveStats {
	return AppendOnlyEntryReserveStats{
		CallsTotal:               appendOnlyEntryReserveCallsTotal.Load(),
		EntriesTotal:             appendOnlyEntryReserveEntriesTotal.Load(),
		GrowCallsTotal:           appendOnlyEntryReserveGrowCallsTotal.Load(),
		GrowBytesTotal:           appendOnlyEntryReserveGrowBytesTotal.Load(),
		SkippedGrowthAllocsTotal: appendOnlyEntryReserveSkippedGrowthAllocsTotal.Load(),
		SkippedGrowthBytesTotal:  appendOnlyEntryReserveSkippedGrowthBytesTotal.Load(),
	}
}

func appendOnlyValueArenaPoolForClass(class int) *sync.Pool {
	if class < 0 || class >= len(appendOnlyValueArenaPoolPtrs) {
		return nil
	}
	if pool := appendOnlyValueArenaPoolPtrs[class].Load(); pool != nil {
		return pool
	}
	pool := &sync.Pool{}
	if appendOnlyValueArenaPoolPtrs[class].CompareAndSwap(nil, pool) {
		return pool
	}
	return appendOnlyValueArenaPoolPtrs[class].Load()
}

func appendOnlyValueArenaPoolBytes(capacity int) uint64 {
	if capacity <= 0 {
		return 0
	}
	return uint64(capacity)
}

func tryAddAppendOnlyValueArenaPoolRetainedBytes(bytes uint64) bool {
	if bytes == 0 {
		return true
	}
	budget := appendOnlyValueArenaPoolRetainBudgetBytes
	if budget == 0 {
		return false
	}
	for {
		current := appendOnlyValueArenaPoolRetainedBytes.Load()
		if current >= budget || bytes > budget-current {
			return false
		}
		next := current + bytes
		if appendOnlyValueArenaPoolRetainedBytes.CompareAndSwap(current, next) {
			for {
				prev := appendOnlyValueArenaPoolRetainedBytesMax.Load()
				if next <= prev || appendOnlyValueArenaPoolRetainedBytesMax.CompareAndSwap(prev, next) {
					return true
				}
			}
		}
	}
}

func recordAppendOnlyValueArenaPoolAdmissionDrop(bytes uint64) {
	appendOnlyValueArenaPoolAdmissionDropTotal.Add(1)
	if bytes > 0 {
		appendOnlyValueArenaPoolAdmissionDropBytesTotal.Add(bytes)
	}
}

func subtractAppendOnlyValueArenaPoolRetainedBytes(bytes uint64) {
	if bytes == 0 {
		return
	}
	for {
		current := appendOnlyValueArenaPoolRetainedBytes.Load()
		if current == 0 {
			return
		}
		next := uint64(0)
		if current > bytes {
			next = current - bytes
		}
		if appendOnlyValueArenaPoolRetainedBytes.CompareAndSwap(current, next) {
			return
		}
	}
}

func dropAppendOnlyValueArenaPools() {
	for i := range appendOnlyValueArenaPoolPtrs {
		appendOnlyValueArenaPoolPtrs[i].Store(&sync.Pool{})
	}
	if dropped := appendOnlyValueArenaPoolRetainedBytes.Swap(0); dropped > 0 {
		appendOnlyValueArenaPoolDropBytesTotal.Add(dropped)
	}
	appendOnlyValueArenaPoolDropTotal.Add(1)
}

// DropAppendOnlyValueArenaPools abandons package-level append-only value-arena
// chunk pools. Per-memtable active/retained arenas are unaffected.
func DropAppendOnlyValueArenaPools() {
	dropAppendOnlyValueArenaPools()
}

func AppendOnlyValueArenaPoolDropTotal() uint64 {
	return appendOnlyValueArenaPoolDropTotal.Load()
}

func AppendOnlyValueArenaPoolStatsSnapshot() AppendOnlyValueArenaPoolStats {
	return AppendOnlyValueArenaPoolStats{
		RetainedBytesEstimate:    appendOnlyValueArenaPoolRetainedBytes.Load(),
		RetainedBytesMaxEstimate: appendOnlyValueArenaPoolRetainedBytesMax.Load(),
		GetsTotal:                appendOnlyValueArenaPoolGetTotal.Load(),
		PutsTotal:                appendOnlyValueArenaPoolPutTotal.Load(),
		DropsTotal:               appendOnlyValueArenaPoolDropTotal.Load(),
		DropBytesTotal:           appendOnlyValueArenaPoolDropBytesTotal.Load(),
		AdmissionDropsTotal:      appendOnlyValueArenaPoolAdmissionDropTotal.Load(),
		AdmissionDropBytesTotal:  appendOnlyValueArenaPoolAdmissionDropBytesTotal.Load(),
	}
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
	if length < 0 {
		length = 0
	}
	class, _, ok := appendOnlyEntryPoolClassForLength(length)
	if !ok {
		return make([]appendOnlyEntry, length)
	}
	appendOnlyEntryPoolMu.Lock()
	bin := appendOnlyEntryPoolBins[class]
	best := -1
	bestCap := 0
	maxReuse := appendOnlyMaxReuseEntries(length)
	// Preserve smaller same-class buffers for future smaller requests instead
	// of popping and discarding them while looking for a larger backing.
	for i := len(bin) - 1; i >= 0; i-- {
		capacity := cap(bin[i])
		if capacity < length || capacity > maxReuse {
			continue
		}
		if best < 0 || capacity < bestCap {
			best = i
			bestCap = capacity
			if capacity == length {
				break
			}
		}
	}
	if best >= 0 {
		last := len(bin) - 1
		entries := bin[best]
		bin[best] = bin[last]
		bin[last] = nil
		bin = bin[:last]
		appendOnlyEntryPoolBins[class] = bin
		appendOnlyEntryPoolGetTotal.Add(1)
		subtractAppendOnlyEntryPoolRetainedBytes(appendOnlyEntryPoolBytes(cap(entries)))
		appendOnlyEntryPoolMu.Unlock()
		return entries[:length]
	}
	appendOnlyEntryPoolMu.Unlock()
	return make([]appendOnlyEntry, length)
}

func putAppendOnlyEntries(entries []appendOnlyEntry) {
	if entries == nil || cap(entries) == 0 {
		return
	}
	if cap(entries) > appendOnlyEntryPoolRetainMaxCap {
		recordAppendOnlyEntryPoolAdmissionDrop(appendOnlyEntryPoolBytes(cap(entries)))
		return
	}
	full := entries[:cap(entries)]
	clear(full)
	class, ok := appendOnlyEntryPoolClassForReusableCapacity(cap(entries))
	if !ok {
		recordAppendOnlyEntryPoolAdmissionDrop(appendOnlyEntryPoolBytes(cap(entries)))
		return
	}
	appendOnlyEntryPoolMu.Lock()
	defer appendOnlyEntryPoolMu.Unlock()
	if appendOnlyEntryPoolForClass(class) != nil {
		bytes := appendOnlyEntryPoolBytes(cap(full))
		if !tryAddAppendOnlyEntryPoolRetainedBytes(bytes) {
			dropAppendOnlyEntryPoolsLocked()
			if !tryAddAppendOnlyEntryPoolRetainedBytes(bytes) {
				recordAppendOnlyEntryPoolAdmissionDrop(bytes)
				return
			}
		}
		appendOnlyEntryPoolPutTotal.Add(1)
		appendOnlyEntryPoolBins[class] = append(appendOnlyEntryPoolBins[class], full[:0])
	}
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

func appendOnlyInitialEntriesForCount(entries int) int {
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
	n := appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry)
	return newAppendOnlyWithInitialEntries(n)
}

// NewAppendOnlyWithEntryCapacity creates an append-only table sized for an
// expected entry count instead of a byte-capacity estimate.
func NewAppendOnlyWithEntryCapacity(entries int) *AppendOnly {
	n := appendOnlyInitialEntriesForCount(entries)
	return newAppendOnlyWithInitialEntries(n)
}

func newAppendOnlyWithInitialEntries(n int) *AppendOnly {
	return &AppendOnly{
		entries:        getAppendOnlyEntries(n),
		baseEntriesLen: n,
		count:          0,
		ordered:        true,
		hasLast:        false,
		lastIdx:        -1,
		snapCount:      0,
		sizeBytes:      0,
	}
}

func appendOnlyEntryKey(ent *appendOnlyEntry) []byte {
	if ent == nil {
		return nil
	}
	if ent.keyInline {
		return ent.inlineKey[:]
	}
	if ent.key == nil {
		return []byte{}
	}
	return ent.key
}

func appendOnlyEntryInlineKeyU64(ent *appendOnlyEntry) (uint64, bool) {
	if ent == nil || !ent.keyInline {
		return 0, false
	}
	return binary.BigEndian.Uint64(ent.inlineKey[:]), true
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
		if pool := appendOnlyValueArenaPoolForClass(idx); pool != nil {
			if v := pool.Get(); v != nil {
				if b, ok := v.([]byte); ok {
					appendOnlyValueArenaPoolGetTotal.Add(1)
					subtractAppendOnlyValueArenaPoolRetainedBytes(appendOnlyValueArenaPoolBytes(cap(b)))
					if cap(b) >= classCap {
						return b[:0]
					}
				}
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
	bytes := appendOnlyValueArenaPoolBytes(cap(chunk))
	if idx, ok := appendOnlyValueArenaClassForCap(cap(chunk)); ok {
		if !tryAddAppendOnlyValueArenaPoolRetainedBytes(bytes) {
			recordAppendOnlyValueArenaPoolAdmissionDrop(bytes)
			return
		}
		if pool := appendOnlyValueArenaPoolForClass(idx); pool != nil {
			appendOnlyValueArenaPoolPutTotal.Add(1)
			pool.Put(chunk[:0])
			return
		}
		subtractAppendOnlyValueArenaPoolRetainedBytes(bytes)
		recordAppendOnlyValueArenaPoolAdmissionDrop(bytes)
	} else if bytes > 0 {
		recordAppendOnlyValueArenaPoolAdmissionDrop(bytes)
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

func appendOnlyValueArenaChunksBytes(chunks [][]byte) int64 {
	var bytes int64
	for _, chunk := range chunks {
		bytes += int64(cap(chunk))
	}
	return bytes
}

func (a *appendOnlyValueArena) backingStats() AppendOnlyValueArenaStats {
	if a == nil {
		return AppendOnlyValueArenaStats{}
	}
	return AppendOnlyValueArenaStats{
		ActiveChunks:   int64(len(a.chunks)),
		ActiveBytes:    appendOnlyValueArenaChunksBytes(a.chunks),
		RetainedChunks: int64(len(a.retained)),
		RetainedBytes:  int64(a.retainedB),
	}
}

func (m *AppendOnly) ValueArenaBackingStats() AppendOnlyValueArenaStats {
	if m == nil {
		return AppendOnlyValueArenaStats{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.valueArena.backingStats()
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

func (a *appendOnlyKeyArena) alloc(length int) []byte {
	if length <= 0 {
		return nil
	}
	if a.cur == nil || cap(a.cur)-a.curPos < length {
		chunkCap := appendOnlyKeyArenaDefaultChunk
		if length > chunkCap {
			chunkCap = 1 << uint(bits.Len(uint(length-1)))
		}
		chunk := make([]byte, chunkCap)
		a.chunks = append(a.chunks, chunk)
		a.cur = chunk
		a.curPos = 0
	}
	out := a.cur[a.curPos : a.curPos+length : a.curPos+length]
	a.curPos += length
	return out
}

func (a *appendOnlyKeyArena) reset() {
	for i := range a.chunks {
		a.chunks[i] = nil
	}
	a.chunks = a.chunks[:0]
	a.cur = nil
	a.curPos = 0
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

func appendOnlyReserveTargetCapacity(current, needed int) (target int, skippedGrowthAllocs int, skippedGrowthBytes uint64) {
	target = current
	if target < 0 {
		target = 0
	}
	if needed <= target {
		return target, 0, 0
	}
	for target < needed {
		next := appendOnlyNextCapacity(target)
		if next <= target || next < 0 {
			next = needed
		}
		if next < needed {
			skippedGrowthAllocs++
			skippedGrowthBytes += appendOnlyEntryPoolBytes(next)
		}
		target = next
	}
	if target > needed {
		headroom := needed / appendOnlyReserveHeadroomDivisor
		if headroom < appendOnlyMinInitialEntries {
			headroom = appendOnlyMinInitialEntries
		}
		maxInt := int(^uint(0) >> 1)
		boundedTarget := needed
		if needed > maxInt-headroom {
			boundedTarget = maxInt
		} else {
			boundedTarget += headroom
		}
		if target > boundedTarget {
			target = boundedTarget
		}
	}
	return target, skippedGrowthAllocs, skippedGrowthBytes
}

func (m *AppendOnly) growEntriesLocked(length int, poolPrev bool) bool {
	if length <= len(m.entries) {
		return false
	}
	if length <= cap(m.entries) {
		oldLen := len(m.entries)
		m.entries = m.entries[:length]
		clear(m.entries[oldLen:])
		return false
	}
	prev := m.entries
	grown := getAppendOnlyEntries(length)
	copy(grown, m.entries[:m.count])
	m.entries = grown
	if poolPrev {
		putAppendOnlyEntries(prev)
	}
	return true
}

func (m *AppendOnly) reserveAdditionalEntriesLocked(additional int) bool {
	if additional <= 0 {
		return false
	}
	needed := m.count + additional
	if needed < m.count {
		needed = int(^uint(0) >> 1)
	}
	if needed <= len(m.entries) {
		return false
	}
	target, skippedGrowthAllocs, skippedGrowthBytes := appendOnlyReserveTargetCapacity(len(m.entries), needed)
	if target <= len(m.entries) {
		return false
	}
	appendOnlyEntryReserveCallsTotal.Add(1)
	appendOnlyEntryReserveEntriesTotal.Add(uint64(additional))
	if skippedGrowthAllocs > 0 {
		appendOnlyEntryReserveSkippedGrowthAllocsTotal.Add(uint64(skippedGrowthAllocs))
		appendOnlyEntryReserveSkippedGrowthBytesTotal.Add(skippedGrowthBytes)
	}
	if m.growEntriesLocked(target, true) {
		appendOnlyEntryReserveGrowCallsTotal.Add(1)
		appendOnlyEntryReserveGrowBytesTotal.Add(appendOnlyEntryPoolBytes(target))
	}
	return true
}

func (m *AppendOnly) ReserveAdditionalEntries(additional int) {
	if additional <= 0 {
		return
	}
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(additional)
	m.mu.Unlock()
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

func appendOnlySortedRunValid(run appendOnlySortedRun, count int) bool {
	return run.start >= 0 && run.start < run.end && run.end <= count
}

func appendOnlySortedRunCursorLess(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor, i, j int) bool {
	return bytes.Compare(
		appendOnlyEntryKey(&active[cursors[i].idx]),
		appendOnlyEntryKey(&active[cursors[j].idx]),
	) < 0
}

func appendOnlySortedRunCursorDown(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor, i, n int) bool {
	orig := i
	for {
		child := 2*i + 1
		if child >= n || child < 0 {
			break
		}
		if right := child + 1; right < n && appendOnlySortedRunCursorLess(active, cursors, right, child) {
			child = right
		}
		if !appendOnlySortedRunCursorLess(active, cursors, child, i) {
			break
		}
		cursors[i], cursors[child] = cursors[child], cursors[i]
		i = child
	}
	return i > orig
}

func appendOnlySortedRunCursorUp(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor, i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !appendOnlySortedRunCursorLess(active, cursors, i, parent) {
			break
		}
		cursors[parent], cursors[i] = cursors[i], cursors[parent]
		i = parent
	}
}

func appendOnlySortedRunCursorHeapInit(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor) {
	for i := len(cursors)/2 - 1; i >= 0; i-- {
		appendOnlySortedRunCursorDown(active, cursors, i, len(cursors))
	}
}

func appendOnlySortedRunCursorHeapPop(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor) (appendOnlySortedRunCursor, []appendOnlySortedRunCursor) {
	n := len(cursors) - 1
	cursors[0], cursors[n] = cursors[n], cursors[0]
	appendOnlySortedRunCursorDown(active, cursors, 0, n)
	out := cursors[n]
	cursors[n] = appendOnlySortedRunCursor{}
	return out, cursors[:n]
}

func appendOnlySortedRunCursorHeapPush(active []appendOnlyEntry, cursors []appendOnlySortedRunCursor, cursor appendOnlySortedRunCursor) []appendOnlySortedRunCursor {
	cursors = append(cursors, cursor)
	appendOnlySortedRunCursorUp(active, cursors, len(cursors)-1)
	return cursors
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

func (m *AppendOnly) clearSortedRunsLocked() {
	if m.sortedRuns != nil {
		m.sortedRuns = m.sortedRuns[:0]
	}
	if m.runCursorBuf != nil {
		m.runCursorBuf = m.runCursorBuf[:0]
	}
	if m.runMergeBuf != nil {
		m.runMergeBuf = m.runMergeBuf[:0]
	}
}

func (m *AppendOnly) initSortedRunsAfterOrderBreakLocked(idx int) {
	m.clearSortedRunsLocked()
	if idx > 0 {
		m.sortedRuns = append(m.sortedRuns, appendOnlySortedRun{start: 0, end: idx})
	}
	m.sortedRuns = append(m.sortedRuns, appendOnlySortedRun{start: idx, end: idx + 1})
	m.latestDirty = false
	m.clearSnapshotLocked()
}

func (m *AppendOnly) extendSortedRunsLocked(idx int, key []byte) {
	if len(m.sortedRuns) == 0 {
		return
	}
	last := &m.sortedRuns[len(m.sortedRuns)-1]
	if appendOnlySortedRunValid(*last, idx) {
		prev := appendOnlyEntryKey(&m.entries[last.end-1])
		if bytes.Compare(key, prev) > 0 && last.end == idx {
			last.end = idx + 1
			m.latestDirty = false
			m.clearSnapshotLocked()
			return
		}
	}
	m.sortedRuns = append(m.sortedRuns, appendOnlySortedRun{start: idx, end: idx + 1})
	m.latestDirty = false
	m.clearSnapshotLocked()
	if len(m.sortedRuns) > appendOnlySortedRunMaxCount {
		// Arbitrary point-write streams can create one run per write. Fall back to
		// the hash latest-index once the run table stops being a compact sorted-run
		// description; sorted batch workloads normally stay at one or two runs and
		// avoid this rebuild on the ingest path.
		m.rebuildLatestIndexLocked()
	}
}

func (m *AppendOnly) lookupSortedRunsEntryLocked(key []byte) *appendOnlyEntry {
	if len(m.sortedRuns) == 0 || m.count == 0 {
		return nil
	}
	active := m.entries[:m.count]
	for runIdx := len(m.sortedRuns) - 1; runIdx >= 0; runIdx-- {
		run := m.sortedRuns[runIdx]
		if !appendOnlySortedRunValid(run, len(active)) {
			continue
		}
		base := run.start
		n := run.end - run.start
		pos := sort.Search(n, func(i int) bool {
			return bytes.Compare(appendOnlyEntryKey(&active[base+i]), key) >= 0
		})
		if pos >= n {
			continue
		}
		ent := &active[base+pos]
		if bytes.Equal(appendOnlyEntryKey(ent), key) {
			return ent
		}
	}
	return nil
}

func (m *AppendOnly) forEachSortedRunLatestLocked(visit func(*appendOnlyEntry)) {
	if len(m.sortedRuns) == 0 || m.count == 0 || visit == nil {
		return
	}
	active := m.entries[:m.count]
	cursors := m.runCursorBuf[:0]
	for runIdx, run := range m.sortedRuns {
		if !appendOnlySortedRunValid(run, len(active)) {
			continue
		}
		cursors = append(cursors, appendOnlySortedRunCursor{run: runIdx, idx: run.start, end: run.end})
	}
	if len(cursors) == 0 {
		m.runCursorBuf = cursors[:0]
		return
	}
	appendOnlySortedRunCursorHeapInit(active, cursors)
	popped := m.runMergeBuf[:0]
	for len(cursors) > 0 {
		var first appendOnlySortedRunCursor
		first, cursors = appendOnlySortedRunCursorHeapPop(active, cursors)
		key := appendOnlyEntryKey(&active[first.idx])
		chosen := first
		popped = append(popped[:0], first)
		for len(cursors) > 0 {
			next := cursors[0]
			if !bytes.Equal(appendOnlyEntryKey(&active[next.idx]), key) {
				break
			}
			next, cursors = appendOnlySortedRunCursorHeapPop(active, cursors)
			if next.run > chosen.run {
				chosen = next
			}
			popped = append(popped, next)
		}
		visit(&active[chosen.idx])
		for _, cursor := range popped {
			cursor.idx++
			if cursor.idx < cursor.end {
				cursors = appendOnlySortedRunCursorHeapPush(active, cursors, cursor)
			}
		}
	}
	m.runCursorBuf = cursors[:0]
	m.runMergeBuf = popped[:0]
}

func (m *AppendOnly) buildSortedRunLatestSnapshotLocked() []*appendOnlyEntry {
	if len(m.sortedRuns) == 0 || m.count == 0 {
		return nil
	}
	if m.snapshot != nil && m.snapCount == m.count {
		return m.snapshot
	}
	snapshot := m.snapshot
	if cap(snapshot) < m.count {
		snapshot = make([]*appendOnlyEntry, m.count)
	} else {
		snapshot = snapshot[:m.count]
	}
	used := 0
	m.forEachSortedRunLatestLocked(func(ent *appendOnlyEntry) {
		snapshot[used] = ent
		used++
	})
	clear(snapshot[used:])
	snapshot = snapshot[:used]
	m.snapshot = snapshot
	m.snapCount = m.count
	return snapshot
}

func (m *AppendOnly) buildSortedRunIteratorEntriesLocked() []appendOnlyEntry {
	if len(m.sortedRuns) == 0 || m.count == 0 {
		return getAppendOnlyIteratorEntries(0)
	}
	entries := getAppendOnlyIteratorEntries(m.count)
	used := 0
	m.forEachSortedRunLatestLocked(func(ent *appendOnlyEntry) {
		entries[used] = *ent
		used++
	})
	return entries[:used]
}

func (m *AppendOnly) buildSortedLatestIndicesLocked() []int {
	if m.count == 0 || m.ordered {
		return nil
	}
	if len(m.sortedRuns) > 0 {
		m.rebuildLatestIndexLocked()
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

func (m *AppendOnly) latestIndexReserveHintLocked(need int) int {
	if need < 0 {
		need = 0
	}
	reserve := need
	if need > appendOnlySortedRunMaxCount && m.baseEntriesLen > reserve {
		reserve = m.baseEntriesLen
	}
	if reserve > appendOnlyLatestIndexMaxReserve {
		return appendOnlyLatestIndexMaxReserve
	}
	return reserve
}

func (m *AppendOnly) rebuildLatestIndexLocked() {
	m.clearSortedRunsLocked()
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
	reserve := m.latestIndexReserveHintLocked(m.count)
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
			m.latest64 = make(map[uint64]int, m.latestIndexReserveHintLocked(idx+1))
		}
		m.latest64[k64] = idx
		return
	}
	if m.latest == nil {
		m.latest = make(map[string]int, m.latestIndexReserveHintLocked(idx+1))
	}
	m.latest[appendOnlyKeyString(key)] = idx
}

func (m *AppendOnly) appendEntryCoreLocked(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, steal bool, borrowValue bool) (idx int, ent *appendOnlyEntry, storedKey []byte) {
	if m.count == len(m.entries) {
		nextCap := appendOnlyNextCapacity(len(m.entries))
		m.growEntriesLocked(nextCap, true)
	}
	idx = m.count
	m.count++
	ent = &m.entries[idx]
	ent.ptr = ptr
	ent.revision = revision
	ent.flags = flags
	ent.keyInline = false
	ent.keyArena = false
	ent.keyReusable = false
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
			if len(key) > 0 && len(key) <= appendOnlyReusableKeyMaxCap {
				ent.key = m.keyArena.alloc(len(key))
				copy(ent.key, key)
				ent.keyArena = true
			} else {
				ent.key = cloneOrReuseBytes(ent.key, key, appendOnlyReusableKeyMaxCap)
				ent.keyReusable = true
			}
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
	storedKey = appendOnlyEntryKey(ent)
	m.sizeBytes += int64(len(storedKey) + entryValueSize(flags, ent.value))
	return idx, ent, storedKey
}

func (m *AppendOnly) appendEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, steal bool, borrowValue bool) *appendOnlyEntry {
	idx, ent, k := m.appendEntryCoreLocked(key, value, ptr, flags, revision, steal, borrowValue)

	if !m.hasLast {
		m.lastIdx = idx
		m.hasLast = true
		return ent
	}
	if m.ordered {
		prev := appendOnlyEntryKey(&m.entries[m.lastIdx])
		cmp := bytes.Compare(k, prev)
		if cmp > 0 {
			m.lastIdx = idx
			return ent
		}
		m.ordered = false
		m.initSortedRunsAfterOrderBreakLocked(idx)
		return ent
	}
	if len(m.sortedRuns) > 0 {
		m.extendSortedRunsLocked(idx, k)
		return ent
	}
	if !m.latestDirty {
		m.updateLatestIndexLocked(k, idx)
	}
	m.clearSnapshotLocked()
	return ent
}

func (m *AppendOnly) appendEntryTrustedOrderedLocked(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, steal bool, borrowValue bool) *appendOnlyEntry {
	idx, ent, _ := m.appendEntryCoreLocked(key, value, ptr, flags, revision, steal, borrowValue)
	m.lastIdx = idx
	m.hasLast = true
	return ent
}

func (m *AppendOnly) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *AppendOnly) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntryWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (m *AppendOnly) SetEntryWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	m.mu.Lock()
	m.appendEntryLocked(key, value, ptr, flags, revision, false, false)
	m.mu.Unlock()
}

func (m *AppendOnly) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntryStealWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (m *AppendOnly) SetEntryStealWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	m.mu.Lock()
	m.appendEntryLocked(key, value, ptr, flags, revision, true, false)
	m.mu.Unlock()
}

func (m *AppendOnly) copyKeyPartsLocked(first, second []byte) []byte {
	total := len(first) + len(second)
	if total <= 0 {
		return nil
	}
	key := m.keyArena.alloc(total)
	n := copy(key, first)
	copy(key[n:], second)
	return key
}

// SetInlineNilKeyParts stores an inline entry with a nil value and a key built
// from first+second. The key is copied into table-owned storage so callers can
// reuse or mutate both input slices after the call.
func (m *AppendOnly) SetInlineNilKeyParts(first, second []byte) {
	m.mu.Lock()
	key := m.copyKeyPartsLocked(first, second)
	ent := m.appendEntryLocked(key, nil, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision, true, false)
	ent.keyArena = true
	m.mu.Unlock()
}

func (m *AppendOnly) SetEntryBorrowValue(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntryBorrowValueWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (m *AppendOnly) SetEntryBorrowValueWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	m.mu.Lock()
	m.appendEntryLocked(key, value, ptr, flags, revision, false, true)
	m.mu.Unlock()
}

func (m *AppendOnly) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *AppendOnly) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

// DeleteKeyParts stores a tombstone whose key is built from first+second and
// copied into table-owned storage.
func (m *AppendOnly) DeleteKeyParts(first, second []byte) {
	m.mu.Lock()
	key := m.copyKeyPartsLocked(first, second)
	ent := m.appendEntryLocked(key, nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision, true, false)
	ent.keyArena = true
	m.mu.Unlock()
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
	m.appendEntryLocked(k, v, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision, true, false)
	m.mu.Unlock()
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
	m.appendEntryLocked(k, nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision, true, false)
	m.mu.Unlock()
	return nil
}

func (m *AppendOnly) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealBatch(entries, onKey, false)
}

func (m *AppendOnly) ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealBatch(entries, onKey, true)
}

func appendOnlyBatchEntryPayload(op batchpkg.Entry, storeInlinePtrValues bool) (value []byte, ptr page.ValuePtr, flags byte) {
	return batchEntryPayload(op, storeInlinePtrValues)
}

func (m *AppendOnly) canAppendTrustedSortedBatchLocked(entries []batchpkg.Entry) bool {
	if len(entries) == 0 || len(entries[0].Key) == 0 || !m.ordered {
		return false
	}
	return m.canAppendTrustedSortedFirstKeyLocked(entries[0].Key)
}

func (m *AppendOnly) canAppendTrustedSortedFirstKeyLocked(firstKey []byte) bool {
	if len(firstKey) == 0 || !m.ordered {
		return false
	}
	if m.count == 0 || !m.hasLast {
		return true
	}
	return bytes.Compare(firstKey, appendOnlyEntryKey(&m.entries[m.lastIdx])) > 0
}

func (m *AppendOnly) ApplyCopySortedBatchTrusted(entries []batchpkg.Entry, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	if len(entries) == 0 {
		return false
	}
	borrowedValues := false
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(len(entries))
	trustedOrdered := m.canAppendTrustedSortedBatchLocked(entries)
	for i := range entries {
		op := entries[i]
		value, ptr, flags := appendOnlyBatchEntryPayload(op, storeInlinePtrValues)
		borrowValue := borrowValues && len(value) > 0
		if borrowValue {
			borrowedValues = true
		}
		if trustedOrdered {
			m.appendEntryTrustedOrderedLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		} else {
			m.appendEntryLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	return borrowedValues
}

func (m *AppendOnly) ApplyCopySelectedSortedBatchTrusted(entries []batchpkg.Entry, selectors []int, selector int, count int, firstKey []byte, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	if count <= 0 {
		return false
	}
	borrowedValues := false
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(count)
	trustedOrdered := m.canAppendTrustedSortedFirstKeyLocked(firstKey)
	for i := range entries {
		if i >= len(selectors) || selectors[i] != selector {
			continue
		}
		op := entries[i]
		value, ptr, flags := appendOnlyBatchEntryPayload(op, storeInlinePtrValues)
		borrowValue := borrowValues && len(value) > 0
		if borrowValue {
			borrowedValues = true
		}
		if trustedOrdered {
			m.appendEntryTrustedOrderedLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		} else {
			m.appendEntryLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	return borrowedValues
}

func (m *AppendOnly) ApplyCopySortedBatchWithValueCopierTrusted(entries []batchpkg.Entry, copyValue func(value []byte) []byte, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	if len(entries) == 0 {
		return false
	}
	copiedValues := false
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(len(entries))
	trustedOrdered := m.canAppendTrustedSortedBatchLocked(entries)
	for i := range entries {
		op := entries[i]
		value, ptr, flags := appendOnlyBatchEntryPayload(op, storeInlinePtrValues)
		borrowValue := false
		if len(value) > 0 && copyValue != nil {
			if copied := copyValue(value); len(copied) == len(value) {
				value = copied
				borrowValue = true
				copiedValues = true
			}
		}
		if trustedOrdered {
			m.appendEntryTrustedOrderedLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		} else {
			m.appendEntryLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	return copiedValues
}

func (m *AppendOnly) ApplyCopySelectedSortedBatchWithValueCopierTrusted(entries []batchpkg.Entry, selectors []int, selector int, count int, firstKey []byte, copyValue func(value []byte) []byte, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	if count <= 0 {
		return false
	}
	copiedValues := false
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(count)
	trustedOrdered := m.canAppendTrustedSortedFirstKeyLocked(firstKey)
	for i := range entries {
		if i >= len(selectors) || selectors[i] != selector {
			continue
		}
		op := entries[i]
		value, ptr, flags := appendOnlyBatchEntryPayload(op, storeInlinePtrValues)
		borrowValue := false
		if len(value) > 0 && copyValue != nil {
			if copied := copyValue(value); len(copied) == len(value) {
				value = copied
				borrowValue = true
				copiedValues = true
			}
		}
		if trustedOrdered {
			m.appendEntryTrustedOrderedLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		} else {
			m.appendEntryLocked(op.Key, value, ptr, flags, op.Revision, false, borrowValue)
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()
	return copiedValues
}

func (m *AppendOnly) ApplyStealEntryFunc(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, err error)) error {
	if count <= 0 {
		return nil
	}
	if emit == nil {
		return errors.New("memtable: nil entry emitter")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveAdditionalEntriesLocked(count)
	for i := 0; i < count; i++ {
		key, value, ptr, flags, err := emit(i)
		if err != nil {
			return err
		}
		m.appendEntryLocked(key, value, ptr, flags, page.LegacyEntryRevision, true, false)
	}
	return nil
}

func (m *AppendOnly) ApplyStealEntryFuncWithRevision(count int, emit func(i int) (key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, err error)) error {
	if count <= 0 {
		return nil
	}
	if emit == nil {
		return errors.New("memtable: nil entry emitter")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserveAdditionalEntriesLocked(count)
	for i := 0; i < count; i++ {
		key, value, ptr, flags, revision, err := emit(i)
		if err != nil {
			return err
		}
		m.appendEntryLocked(key, value, ptr, flags, revision, true, false)
	}
	return nil
}

func (m *AppendOnly) applyStealBatch(entries []batchpkg.Entry, onKey func(key []byte), trustedOrder bool) {
	m.mu.Lock()
	m.reserveAdditionalEntriesLocked(len(entries))
	trustedOrdered := trustedOrder && m.canAppendTrustedSortedBatchLocked(entries)
	for i := range entries {
		op := entries[i]
		switch {
		case op.Type == batchpkg.OpDelete:
			if trustedOrdered {
				m.appendEntryTrustedOrderedLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, op.Revision, true, false)
			} else {
				m.appendEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, op.Revision, true, false)
			}
		case op.IsPtr:
			if trustedOrdered {
				m.appendEntryTrustedOrderedLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, op.Revision, true, false)
			} else {
				m.appendEntryLocked(op.Key, op.Value, op.ValuePtr, node.FlagPointer, op.Revision, true, false)
			}
		default:
			if trustedOrdered {
				m.appendEntryTrustedOrderedLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, op.Revision, true, false)
			} else {
				m.appendEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, op.Revision, true, false)
			}
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
	if key64, ok := appendOnlyKeyU64(key); ok {
		if len(m.orderedKey64) == len(active) {
			idx := sort.Search(len(m.orderedKey64), func(i int) bool {
				return m.orderedKey64[i] >= key64
			})
			if idx >= len(active) || m.orderedKey64[idx] != key64 {
				return nil
			}
			return &active[idx]
		}
		idx := sort.Search(len(active), func(i int) bool {
			if entryKey64, ok := appendOnlyEntryInlineKeyU64(&active[i]); ok {
				return entryKey64 >= key64
			}
			return bytes.Compare(appendOnlyEntryKey(&active[i]), key) >= 0
		})
		if idx >= len(active) {
			return nil
		}
		ent := &active[idx]
		if entryKey64, ok := appendOnlyEntryInlineKeyU64(ent); ok {
			if entryKey64 != key64 {
				return nil
			}
			return ent
		}
		if !bytes.Equal(appendOnlyEntryKey(ent), key) {
			return nil
		}
		return ent
	}
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

func (m *AppendOnly) getEntryFrozen(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool, bool) {
	if m == nil {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, true
	}
	if m.ordered {
		ent := m.orderedLookupEntryLocked(key)
		if ent == nil {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, true
		}
		return ent.value, ent.ptr, ent.flags, ent.revision, true, true
	}
	if len(m.sortedRuns) > 0 {
		ent := m.lookupSortedRunsEntryLocked(key)
		if ent == nil {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, true
		}
		return ent.value, ent.ptr, ent.flags, ent.revision, true, true
	}
	if m.latestDirty {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, false
	}
	if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
		if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
			ent := &m.entries[idx]
			if bytes.Equal(appendOnlyEntryKey(ent), key) {
				return ent.value, ent.ptr, ent.flags, ent.revision, true, true
			}
		}
	}
	if m.latest != nil {
		if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
			ent := &m.entries[idx]
			if bytes.Equal(appendOnlyEntryKey(ent), key) {
				return ent.value, ent.ptr, ent.flags, ent.revision, true, true
			}
		}
	}
	return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false, true
}

func (m *AppendOnly) Get(key []byte) ([]byte, bool, bool) {
	if m.frozenFast.Load() {
		val, _, flags, _, found, ok := m.getEntryFrozen(key)
		if ok {
			if !found {
				return nil, false, false
			}
			if flags&node.FlagTombstone != 0 {
				return nil, true, true
			}
			return val, false, true
		}
	}
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

		if len(m.sortedRuns) > 0 {
			if ent := m.lookupSortedRunsEntryLocked(key); ent != nil {
				deleted := ent.flags&node.FlagTombstone != 0
				val := ent.value
				m.mu.RUnlock()
				if deleted {
					return nil, true, true
				}
				return val, false, true
			}
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
	val, ptr, flags, _, found := m.GetEntryWithRevision(key)
	return val, ptr, flags, found
}

func (m *AppendOnly) lookupEntryLocked(key []byte) *appendOnlyEntry {
	if ent := m.orderedLookupEntryLocked(key); ent != nil {
		return ent
	}
	if m.ordered {
		return nil
	}
	if len(m.sortedRuns) > 0 {
		return m.lookupSortedRunsEntryLocked(key)
	}
	if m.latestDirty {
		return nil
	}
	if k64, ok := appendOnlyKeyU64(key); ok && m.latest64 != nil {
		if idx, ok := m.latest64[k64]; ok && idx >= 0 && idx < m.count {
			ent := &m.entries[idx]
			if bytes.Equal(appendOnlyEntryKey(ent), key) {
				return ent
			}
		}
	}
	if m.latest != nil {
		if idx, ok := m.latest[appendOnlyKeyString(key)]; ok && idx >= 0 && idx < m.count {
			ent := &m.entries[idx]
			if bytes.Equal(appendOnlyEntryKey(ent), key) {
				return ent
			}
		}
	}
	return nil
}

func (m *AppendOnly) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if m.frozenFast.Load() {
		val, ptr, flags, revision, found, ok := m.getEntryFrozen(key)
		if ok {
			return val, ptr, flags, revision, found
		}
	}
	for {
		m.mu.RLock()
		if ent := m.lookupEntryLocked(key); ent != nil {
			val := ent.value
			ptr := ent.ptr
			flags := ent.flags
			revision := ent.revision
			m.mu.RUnlock()
			return val, ptr, flags, revision, true
		}
		if m.ordered || len(m.sortedRuns) > 0 || !m.latestDirty {
			m.mu.RUnlock()
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
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

func appendOnlyEntryInRange(ent *appendOnlyEntry, start, end []byte) bool {
	if ent == nil {
		return false
	}
	key := appendOnlyEntryKey(ent)
	return bytes.Compare(key, start) >= 0 && (end == nil || bytes.Compare(key, end) < 0)
}

func (m *AppendOnly) seekGEEntryLocked(start, end []byte) *appendOnlyEntry {
	if m.count == 0 || (end != nil && bytes.Compare(start, end) >= 0) {
		return nil
	}
	active := m.entries[:m.count]
	if m.ordered {
		idx := sort.Search(len(active), func(i int) bool {
			return bytes.Compare(appendOnlyEntryKey(&active[i]), start) >= 0
		})
		if idx < len(active) && appendOnlyEntryInRange(&active[idx], start, end) {
			return &active[idx]
		}
		return nil
	}
	if len(m.sortedRuns) > 0 {
		var best *appendOnlyEntry
		bestRun := -1
		for runIdx, run := range m.sortedRuns {
			if !appendOnlySortedRunValid(run, len(active)) {
				continue
			}
			pos := sort.Search(run.end-run.start, func(i int) bool {
				return bytes.Compare(appendOnlyEntryKey(&active[run.start+i]), start) >= 0
			})
			if pos >= run.end-run.start {
				continue
			}
			ent := &active[run.start+pos]
			if !appendOnlyEntryInRange(ent, start, end) {
				continue
			}
			if best == nil || bytes.Compare(appendOnlyEntryKey(ent), appendOnlyEntryKey(best)) < 0 ||
				(bytes.Equal(appendOnlyEntryKey(ent), appendOnlyEntryKey(best)) && runIdx > bestRun) {
				best, bestRun = ent, runIdx
			}
		}
		return best
	}
	// The unordered fallback keeps only a latest-key hash index. Scan those
	// indices without allocating; exact-start hits remain O(1) in GetAt's common
	// commit-then-read path, while arbitrary successors remain correct.
	var best *appendOnlyEntry
	consider := func(idx int) {
		if idx < 0 || idx >= len(active) {
			return
		}
		ent := &active[idx]
		if !appendOnlyEntryInRange(ent, start, end) {
			return
		}
		if best == nil || bytes.Compare(appendOnlyEntryKey(ent), appendOnlyEntryKey(best)) < 0 {
			best = ent
		}
	}
	for _, idx := range m.latest {
		consider(idx)
	}
	for _, idx := range m.latest64 {
		consider(idx)
	}
	return best
}

func (m *AppendOnly) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	if m.frozenFast.Load() {
		if val, ptr, flags, revision, found, _ := m.getEntryFrozen(start); found {
			return start, val, ptr, flags, revision, true
		}
	}
	for {
		m.mu.RLock()
		if !m.ordered && len(m.sortedRuns) == 0 && m.latestDirty {
			m.mu.RUnlock()
			m.mu.Lock()
			if !m.ordered && len(m.sortedRuns) == 0 && m.latestDirty {
				m.rebuildLatestIndexLocked()
			}
			m.mu.Unlock()
			continue
		}
		ent := m.lookupEntryLocked(start)
		if ent == nil {
			ent = m.seekGEEntryLocked(start, end)
		}
		if ent == nil {
			m.mu.RUnlock()
			return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
		key, val, ptr, flags, revision := appendOnlyEntryKey(ent), ent.value, ent.ptr, ent.flags, ent.revision
		m.mu.RUnlock()
		if flags&node.FlagTombstone != 0 {
			return key, nil, page.ValuePtr{}, flags, revision, true
		}
		if flags&node.FlagPointer == 0 {
			ptr = page.ValuePtr{}
		}
		return key, val, ptr, flags, revision, true
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

// EntryCapacity reports the retained append-only entry-slot capacity.
func (m *AppendOnly) EntryCapacity() int {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cap(m.entries)
}

// EntryBackingBytes reports the retained backing bytes for entry slots.
func (m *AppendOnly) EntryBackingBytes() int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(cap(m.entries)) * int64(unsafe.Sizeof(appendOnlyEntry{}))
}

// TrimEntryCapacity shrinks retained entry-slot backing while preserving active
// entries. It is intended for maintenance boundaries after a transient ingest
// spike has left a live append-only memtable with large unused capacity.
func (m *AppendOnly) TrimEntryCapacity(maxEntries int) (before, after int64) {
	if m == nil {
		return 0, 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()

	before = int64(cap(m.entries)) * int64(unsafe.Sizeof(appendOnlyEntry{}))
	if maxEntries < m.count {
		maxEntries = m.count
	}
	if maxEntries > 0 && maxEntries < appendOnlyMinInitialEntries {
		maxEntries = appendOnlyMinInitialEntries
	}
	if cap(m.entries) <= maxEntries {
		return before, before
	}

	prev := m.entries
	next := getAppendOnlyEntries(maxEntries)
	copy(next, m.entries[:m.count])
	m.entries = next
	if m.baseEntriesLen > maxEntries {
		m.baseEntriesLen = maxEntries
	}
	m.snapshot = nil
	m.snapCount = 0
	putAppendOnlyEntries(prev)

	after = int64(cap(m.entries)) * int64(unsafe.Sizeof(appendOnlyEntry{}))
	return before, after
}

func (m *AppendOnly) Freeze() {
	m.mu.Lock()
	if m.frozen {
		m.mu.Unlock()
		return
	}
	m.buildOrderedKey64Locked()
	m.frozen = true
	m.frozenFast.Store(true)
	m.mu.Unlock()
}

func (m *AppendOnly) buildOrderedKey64Locked() {
	if !m.ordered || m.count == 0 {
		m.orderedKey64 = m.orderedKey64[:0]
		return
	}
	for i := 0; i < m.count; i++ {
		if !m.entries[i].keyInline {
			m.orderedKey64 = m.orderedKey64[:0]
			return
		}
	}
	if cap(m.orderedKey64) < m.count {
		m.orderedKey64 = make([]uint64, m.count)
	} else {
		m.orderedKey64 = m.orderedKey64[:m.count]
	}
	for i := 0; i < m.count; i++ {
		m.orderedKey64[i] = binary.BigEndian.Uint64(m.entries[i].inlineKey[:])
	}
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
	m.resetLockedWithPolicy(0, 0, true)
}

// Release returns large internal buffers to package pools when the table is no
// longer needed. Unlike Reset, it does not retain warm capacity on the table
// itself, so callers should only use it for short-lived tables they will drop.
func (m *AppendOnly) Release() {
	m.release(true)
}

// ReleaseDropEntries releases the table without returning its entry backing
// slice to the package pool. Use this for cold paths where the caller does not
// expect near-term append-only reuse and wants to shed post-spike heap.
func (m *AppendOnly) ReleaseDropEntries() {
	m.release(false)
}

func (m *AppendOnly) release(poolEntries bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()

	entries := m.entries
	m.entries = nil
	m.baseEntriesLen = 0
	m.orderedKey64 = nil
	m.latest = nil
	m.latest64 = nil
	m.sortedRuns = nil
	m.runCursorBuf = nil
	m.runMergeBuf = nil
	m.snapshot = nil
	m.indexBuf = nil
	m.keyArena.reset()
	m.valueArena.reset()
	m.valueArena.dropRetained()
	m.count = 0
	m.snapCount = 0
	m.sizeBytes = 0
	m.ordered = true
	m.latestDirty = false
	m.frozen = false
	m.frozenFast.Store(false)
	m.hasLast = false
	m.lastIdx = -1
	if poolEntries {
		putAppendOnlyEntries(entries)
	}
}

// ResetWithCapacity resets the memtable and, when needed, shrinks retained
// internal buffers toward the capacity-derived baseline. Unlike Reset, callers
// provide a capacity estimate so post-spike entry retention can decay.
func (m *AppendOnly) ResetWithCapacity(capacity, estimatedBytesPerEntry int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, true)
}

func (m *AppendOnly) resetLocked(capacity, estimatedBytesPerEntry int) {
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, true)
}

// ResetWithCapacityHard resets and clamps retained internal buffers to the
// capacity-derived baseline (without carrying over recent spike cardinality).
func (m *AppendOnly) ResetWithCapacityHard(capacity, estimatedBytesPerEntry int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitIteratorLeasesLocked()
	m.resetLockedWithPolicy(capacity, estimatedBytesPerEntry, false)
}

// ResetDropEntries resets the table for reuse without retaining entry backing.
// The next write will allocate from the normal minimum append-only capacity.
func (m *AppendOnly) ResetDropEntries() {
	m.release(false)
}

func (m *AppendOnly) resetLockedWithPolicy(capacity, estimatedBytesPerEntry int, retainObserved bool) {
	desiredEntries := m.baseEntriesLen
	if capacity > 0 {
		desiredEntries = appendOnlyInitialEntriesForCapacity(capacity, estimatedBytesPerEntry)
		m.baseEntriesLen = desiredEntries
	}
	if desiredEntries <= 0 {
		desiredEntries = appendOnlyMinInitialEntries
	}

	oldCount := m.count
	maxRetainedEntries := appendOnlyMaxReuseEntries(desiredEntries)
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
		ent.revision = page.LegacyEntryRevision
		ent.flags = 0
		if ent.keyArena {
			ent.key = nil
		}
		ent.keyInline = false
		ent.keyArena = false
		if !ent.keyReusable {
			ent.key = nil
		}
		ent.keyReusable = false
		ent.valueOwned = false
		if cap(ent.key) > appendOnlyReusableKeyMaxCap {
			ent.key = nil
		} else if ent.key != nil {
			ent.key = ent.key[:0]
		}
		ent.value = nil
	}
	m.keyArena.reset()
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
	m.clearSortedRunsLocked()
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
	m.orderedKey64 = m.orderedKey64[:0]
	m.ordered = true
	m.latestDirty = false
	m.frozen = false
	m.frozenFast.Store(false)
	m.hasLast = false
	m.lastIdx = -1

	if !retainObserved {
		if cap(m.entries) != retainedEntries {
			m.replaceEntriesSliceWithPolicy(retainedEntries, true)
			return
		}
		if len(m.entries) != retainedEntries {
			m.entries = m.entries[:retainedEntries]
		}
		return
	}

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
	m.replaceEntriesSliceWithPolicy(length, true)
}

func (m *AppendOnly) replaceEntriesSliceWithPolicy(length int, poolPrev bool) {
	if length < 0 {
		length = 0
	}
	prev := m.entries
	m.entries = getAppendOnlyEntries(length)
	if poolPrev {
		putAppendOnlyEntries(prev)
	}
}

func (m *AppendOnly) buildSortedLatestSnapshotLocked() []*appendOnlyEntry {
	if m.count == 0 {
		return nil
	}
	if m.ordered {
		return nil
	}
	if len(m.sortedRuns) > 0 {
		return m.buildSortedRunLatestSnapshotLocked()
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
	if len(m.sortedRuns) > 0 {
		entries := m.buildSortedRunIteratorEntriesLocked()
		m.clearSnapshotLocked()
		return entries
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

func (it *appendOnlyIterator) Len() int {
	if it == nil {
		return 0
	}
	return it.len()
}

func (*appendOnlyIterator) StableUnsafeIteratorSlices() bool { return true }

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
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *appendOnlyIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	ent := it.entryAt(it.idx)
	if ent == nil || !it.validIndex() {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	return ent.value, ent.ptr, ent.flags, ent.revision
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
