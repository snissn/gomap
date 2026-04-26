package memtable

import (
	"bytes"
	"math/bits"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	btree "github.com/snissn/tidwall-btree"
)

const btreeDefaultDegree = 32
const btreeArenaChunkSize = 1 << 20
const btreeArenaInitialChunkSize = 64 << 10
const btreeInlineValueDedupeMax = 1 << 20
const btreeArenaPoolMinShift = 16
const btreeArenaPoolMaxShift = 20
const btreeArenaPoolClassCount = btreeArenaPoolMaxShift - btreeArenaPoolMinShift + 1
const btreeArenaPoolBudgetBytes = 64 << 20
const btreeAdaptiveBothSplitMinEntries = 512
const btreeAdaptiveBothSplitMinNonAppend = 64
const btreeLeafItemArenaRetainChunks = 256
const btreeNodeArenaRetainChunks = 256
const btreeLoadSortedMinBatchEntries = 256

const (
	btreeEntryFlagsBits   = 8
	btreeEntryLenBits     = 20
	btreeEntryChunkBits   = 16
	btreeEntryOffsetBits  = 20
	btreeEntryLenShift    = btreeEntryFlagsBits
	btreeEntryChunkShift  = btreeEntryLenShift + btreeEntryLenBits
	btreeEntryOffsetShift = btreeEntryChunkShift + btreeEntryChunkBits
	btreeEntryMaxLen      = 1<<btreeEntryLenBits - 1
	btreeEntryMaxChunk    = 1<<btreeEntryChunkBits - 2
	btreeEntryExternal    = 1<<btreeEntryChunkBits - 1
	btreeEntryMaxOffset   = 1<<btreeEntryOffsetBits - 1
)

// BTreeBatchLoadSortedStats reports process-wide BTree memtable sorted-load
// fast-path decisions. The counters are cumulative and intended for benchmark
// deltas; they are not reset by individual memtable resets.
type BTreeBatchLoadSortedStats struct {
	Attempts                     uint64
	AttemptEntries               uint64
	Hits                         uint64
	HitEntries                   uint64
	CopyAttempts                 uint64
	CopyHits                     uint64
	StealAttempts                uint64
	StealHits                    uint64
	FallbackTooSmall             uint64
	FallbackTooSmallEntries      uint64
	FallbackNilKey               uint64
	FallbackNilKeyEntries        uint64
	FallbackOverlapLast          uint64
	FallbackOverlapLastEntries   uint64
	FallbackNonIncreasing        uint64
	FallbackNonIncreasingEntries uint64
}

type btreeBatchLoadSortedCounters struct {
	attempts                     atomic.Uint64
	attemptEntries               atomic.Uint64
	hits                         atomic.Uint64
	hitEntries                   atomic.Uint64
	copyAttempts                 atomic.Uint64
	copyHits                     atomic.Uint64
	stealAttempts                atomic.Uint64
	stealHits                    atomic.Uint64
	fallbackTooSmall             atomic.Uint64
	fallbackTooSmallEntries      atomic.Uint64
	fallbackNilKey               atomic.Uint64
	fallbackNilKeyEntries        atomic.Uint64
	fallbackOverlapLast          atomic.Uint64
	fallbackOverlapLastEntries   atomic.Uint64
	fallbackNonIncreasing        atomic.Uint64
	fallbackNonIncreasingEntries atomic.Uint64
}

var btreeBatchLoadSortedCountersGlobal btreeBatchLoadSortedCounters

// ResetBTreeBatchLoadSortedStats clears process-wide BTree sorted-load counters.
// It is primarily useful for tests and standalone benchmark harnesses.
func ResetBTreeBatchLoadSortedStats() {
	c := &btreeBatchLoadSortedCountersGlobal
	c.attempts.Store(0)
	c.attemptEntries.Store(0)
	c.hits.Store(0)
	c.hitEntries.Store(0)
	c.copyAttempts.Store(0)
	c.copyHits.Store(0)
	c.stealAttempts.Store(0)
	c.stealHits.Store(0)
	c.fallbackTooSmall.Store(0)
	c.fallbackTooSmallEntries.Store(0)
	c.fallbackNilKey.Store(0)
	c.fallbackNilKeyEntries.Store(0)
	c.fallbackOverlapLast.Store(0)
	c.fallbackOverlapLastEntries.Store(0)
	c.fallbackNonIncreasing.Store(0)
	c.fallbackNonIncreasingEntries.Store(0)
}

// BTreeBatchLoadSortedStatsSnapshot returns process-wide BTree sorted-load
// counters. Use deltas between snapshots when multiple DBs may share a process.
func BTreeBatchLoadSortedStatsSnapshot() BTreeBatchLoadSortedStats {
	c := &btreeBatchLoadSortedCountersGlobal
	return BTreeBatchLoadSortedStats{
		Attempts:                     c.attempts.Load(),
		AttemptEntries:               c.attemptEntries.Load(),
		Hits:                         c.hits.Load(),
		HitEntries:                   c.hitEntries.Load(),
		CopyAttempts:                 c.copyAttempts.Load(),
		CopyHits:                     c.copyHits.Load(),
		StealAttempts:                c.stealAttempts.Load(),
		StealHits:                    c.stealHits.Load(),
		FallbackTooSmall:             c.fallbackTooSmall.Load(),
		FallbackTooSmallEntries:      c.fallbackTooSmallEntries.Load(),
		FallbackNilKey:               c.fallbackNilKey.Load(),
		FallbackNilKeyEntries:        c.fallbackNilKeyEntries.Load(),
		FallbackOverlapLast:          c.fallbackOverlapLast.Load(),
		FallbackOverlapLastEntries:   c.fallbackOverlapLastEntries.Load(),
		FallbackNonIncreasing:        c.fallbackNonIncreasing.Load(),
		FallbackNonIncreasingEntries: c.fallbackNonIncreasingEntries.Load(),
	}
}

type btreeBatchLoadSortedKind uint8

const (
	btreeBatchLoadSortedKindSteal btreeBatchLoadSortedKind = iota
	btreeBatchLoadSortedKindCopy
)

type btreeBatchLoadSortedReject uint8

const (
	btreeBatchLoadSortedRejectNone btreeBatchLoadSortedReject = iota
	btreeBatchLoadSortedRejectTooSmall
	btreeBatchLoadSortedRejectNilKey
	btreeBatchLoadSortedRejectOverlapLast
	btreeBatchLoadSortedRejectNonIncreasing
)

func recordBTreeBatchLoadSortedAttempt(kind btreeBatchLoadSortedKind, n int) {
	c := &btreeBatchLoadSortedCountersGlobal
	c.attempts.Add(1)
	c.attemptEntries.Add(uint64(n))
	switch kind {
	case btreeBatchLoadSortedKindCopy:
		c.copyAttempts.Add(1)
	case btreeBatchLoadSortedKindSteal:
		c.stealAttempts.Add(1)
	}
}

func recordBTreeBatchLoadSortedHit(kind btreeBatchLoadSortedKind, n int) {
	c := &btreeBatchLoadSortedCountersGlobal
	c.hits.Add(1)
	c.hitEntries.Add(uint64(n))
	switch kind {
	case btreeBatchLoadSortedKindCopy:
		c.copyHits.Add(1)
	case btreeBatchLoadSortedKindSteal:
		c.stealHits.Add(1)
	}
}

func recordBTreeBatchLoadSortedReject(reason btreeBatchLoadSortedReject, n int) {
	c := &btreeBatchLoadSortedCountersGlobal
	switch reason {
	case btreeBatchLoadSortedRejectTooSmall:
		c.fallbackTooSmall.Add(1)
		c.fallbackTooSmallEntries.Add(uint64(n))
	case btreeBatchLoadSortedRejectNilKey:
		c.fallbackNilKey.Add(1)
		c.fallbackNilKeyEntries.Add(uint64(n))
	case btreeBatchLoadSortedRejectOverlapLast:
		c.fallbackOverlapLast.Add(1)
		c.fallbackOverlapLastEntries.Add(uint64(n))
	case btreeBatchLoadSortedRejectNonIncreasing:
		c.fallbackNonIncreasing.Add(1)
		c.fallbackNonIncreasingEntries.Add(uint64(n))
	}
}

var btreeArenaChunkPools [btreeArenaPoolClassCount]sync.Pool
var btreeArenaPoolBytes atomic.Int64
var btreeArenaPoolLastGC atomic.Uint64
var btreeArenaPoolNumGC = func() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return uint64(stats.NumGC)
}

type btreeEntry struct {
	bits uint64
}

type btreeArenaRef struct {
	chunk  uint32
	offset uint32
	length uint32
}

func newBTreeEntry(flags byte, ref btreeArenaRef) btreeEntry {
	return btreeEntry{
		bits: uint64(flags) |
			uint64(ref.length)<<btreeEntryLenShift |
			uint64(ref.chunk)<<btreeEntryChunkShift |
			uint64(ref.offset)<<btreeEntryOffsetShift,
	}
}

func newBTreeExternalEntry(flags byte, index int) btreeEntry {
	if index < 0 || uint64(index) > (uint64(btreeEntryMaxOffset)<<btreeEntryLenBits)|uint64(btreeEntryMaxLen) {
		panic("treedb: btree external entry index too large")
	}
	return btreeEntry{
		bits: uint64(flags) |
			uint64(index&btreeEntryMaxLen)<<btreeEntryLenShift |
			uint64(btreeEntryExternal)<<btreeEntryChunkShift |
			uint64(index>>btreeEntryLenBits)<<btreeEntryOffsetShift,
	}
}

func (entry btreeEntry) flags() byte {
	return byte(entry.bits)
}

func (entry btreeEntry) valueRef() btreeArenaRef {
	return btreeArenaRef{
		chunk:  uint32((entry.bits >> btreeEntryChunkShift) & btreeEntryExternal),
		offset: uint32((entry.bits >> btreeEntryOffsetShift) & btreeEntryMaxOffset),
		length: uint32((entry.bits >> btreeEntryLenShift) & btreeEntryMaxLen),
	}
}

func (entry btreeEntry) externalIndex() int {
	low := int((entry.bits >> btreeEntryLenShift) & btreeEntryMaxLen)
	high := int((entry.bits >> btreeEntryOffsetShift) & btreeEntryMaxOffset)
	return high<<btreeEntryLenBits | low
}

func (entry btreeEntry) isExternal() bool {
	return entry.valueRef().chunk == btreeEntryExternal
}

func btreePointerInlineValue(value []byte) []byte {
	if len(value) <= page.ValuePtrSize {
		return nil
	}
	return value[page.ValuePtrSize:]
}

func canonicalizeBTreeInlineValue(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return value
}

func normalizeBTreeEntryFlags(flags byte) byte {
	if flags&node.FlagTombstone != 0 {
		return flags &^ node.FlagPointer
	}
	if flags&node.FlagPointer != 0 {
		return flags &^ node.FlagTombstone
	}
	return flags &^ (node.FlagPointer | node.FlagTombstone)
}

func (m *BTree) btreeEntryValueBytes(entry btreeEntry) []byte {
	ref := entry.valueRef()
	if ref.chunk == btreeEntryExternal {
		index := entry.externalIndex()
		if index < 0 || index >= len(m.externalValues) {
			return nil
		}
		return m.externalValues[index]
	}
	if ref.length == 0 {
		return nil
	}
	return m.arena.slice(ref)
}

func (m *BTree) btreeEntryValue(entry btreeEntry) []byte {
	flags := entry.flags()
	if flags&node.FlagPointer != 0 {
		return btreePointerInlineValue(m.btreeEntryValueBytes(entry))
	}
	if flags&node.FlagTombstone != 0 {
		return nil
	}
	return m.btreeEntryValueBytes(entry)
}

func (m *BTree) btreeEntryValuePtr(entry btreeEntry) page.ValuePtr {
	value := m.btreeEntryValueBytes(entry)
	if entry.flags()&node.FlagPointer == 0 || len(value) < page.ValuePtrSize {
		return page.ValuePtr{}
	}
	return page.DecodeValuePtr(value[:page.ValuePtrSize])
}

// btreeEntryPayloadSize tracks the logical value payload bytes contributing to
// memtable flush thresholds, not the fixed in-memory struct footprint.
func (m *BTree) btreeEntryPayloadSize(entry btreeEntry) int {
	if entry.flags()&node.FlagTombstone != 0 {
		return 0
	}
	return len(m.btreeEntryValueBytes(entry))
}

func (m *BTree) makeBTreeEntry(flags byte, value []byte, ref btreeArenaRef) btreeEntry {
	if len(value) == 0 {
		return newBTreeEntry(flags, btreeArenaRef{})
	}
	if ref.canEncode() {
		return newBTreeEntry(flags, ref)
	}
	return m.makeExternalBTreeEntry(flags, value)
}

func (m *BTree) makeExternalBTreeEntry(flags byte, value []byte) btreeEntry {
	if len(value) == 0 {
		return newBTreeEntry(flags, btreeArenaRef{})
	}
	index := len(m.externalValues)
	m.externalValues = append(m.externalValues, value)
	return newBTreeExternalEntry(flags, index)
}

func (m *BTree) makeStealInlineBTreeEntry(value []byte) btreeEntry {
	value = canonicalizeBTreeInlineValue(value)
	if len(value) == 0 {
		return newBTreeEntry(node.FlagInline, btreeArenaRef{})
	}
	if len(value) <= btreeInlineValueDedupeMax && (m.lastInline == nil || bytes.Equal(value, m.lastInline)) {
		valueCopy, valueRef := m.copyInlineValueLocked(value)
		return m.makeBTreeEntry(node.FlagInline, valueCopy, valueRef)
	}
	return m.makeExternalBTreeEntry(node.FlagInline, value)
}

type BTree struct {
	mu                           sync.RWMutex
	tree                         *btree.Map[string, btreeEntry]
	sizeBytes                    int64
	arena                        *btreeArena
	externalValues               [][]byte
	lastKey                      string
	hasLast                      bool
	degree                       int
	lastInline                   []byte
	lastInlineRef                btreeArenaRef
	deleteCount                  int
	nonAppendSetCount            int
	reuseBothSplitInsertCapacity bool
}

func (*BTree) StableUnsafeIteratorSlices() bool { return true }

func NewBTree() *BTree {
	return NewBTreeWithDegree(btreeDefaultDegree)
}

func (m *BTree) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, nil, onKey)
}

func (m *BTree) ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, nil, onKey)
}

func (m *BTree) ApplyStealSortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	if len(idxs) == 0 {
		return
	}
	m.applyStealSortedBatch(entries, idxs, onKey)
}

func (m *BTree) ApplyCopySortedBatchIndicesTrusted(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applyCopySortedBatchLoadSortedLocked(entries, idxs, storeInlinePtrValues, onKey) {
		return
	}
	for _, idx := range idxs {
		op := entries[idx]
		if op.Key == nil {
			continue
		}
		keyCopy, entry := m.copyKeyEntryFromBatchOpLocked(op, storeInlinePtrValues)
		keyStr := bytesToStringNoCopy(keyCopy)
		prev, replaced := m.setMaybeSortedLoadLocked(keyStr, entry)
		m.recordSetLocked(keyStr, len(keyCopy), entry, prev, replaced)
		if onKey != nil {
			onKey(op.Key)
		}
	}
}

func (m *BTree) applyStealSortedBatch(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applyStealSortedBatchLoadSortedLocked(entries, idxs, onKey) {
		return
	}
	apply := func(op batchpkg.Entry) {
		if op.Key == nil {
			return
		}
		keyStr := bytesToStringNoCopy(op.Key)
		entry := m.btreeEntryFromBatchOpLocked(op)
		prev, replaced := m.setMaybeSortedLoadLocked(keyStr, entry)
		m.recordSetLocked(keyStr, len(op.Key), entry, prev, replaced)
		if onKey != nil {
			onKey(op.Key)
		}
	}

	if idxs == nil {
		for _, op := range entries {
			apply(op)
		}
		return
	}
	for _, idx := range idxs {
		apply(entries[idx])
	}
}

func (m *BTree) applyStealSortedBatchLoadSortedLocked(entries []batchpkg.Entry, idxs []int, onKey func(key []byte)) bool {
	n := btreeSortedBatchLen(entries, idxs)
	if n == 0 {
		return true
	}
	recordBTreeBatchLoadSortedAttempt(btreeBatchLoadSortedKindSteal, n)
	if n < btreeLoadSortedMinBatchEntries {
		recordBTreeBatchLoadSortedReject(btreeBatchLoadSortedRejectTooSmall, n)
		return false
	}
	if reason := m.canApplySortedBatchLoadSortedLocked(entries, idxs); reason != btreeBatchLoadSortedRejectNone {
		recordBTreeBatchLoadSortedReject(reason, n)
		return false
	}
	recordBTreeBatchLoadSortedHit(btreeBatchLoadSortedKindSteal, n)
	var addedSize int64
	var addedDeletes int
	var lastKey string
	m.tree.LoadSortedItemsTrusted(n, func(i int) (string, btreeEntry) {
		op := btreeSortedBatchEntryAt(entries, idxs, i)
		keyStr := bytesToStringNoCopy(op.Key)
		entry := m.btreeEntryFromBatchOpLocked(op)
		if onKey == nil {
			addedSize += int64(len(op.Key) + m.btreeEntryPayloadSize(entry))
			if entry.flags()&node.FlagTombstone != 0 {
				addedDeletes++
			}
			lastKey = keyStr
		} else {
			m.recordSetLocked(keyStr, len(op.Key), entry, btreeEntry{}, false)
		}
		if onKey != nil {
			onKey(op.Key)
		}
		return keyStr, entry
	})
	if onKey == nil {
		m.sizeBytes += addedSize
		m.deleteCount += addedDeletes
		m.lastKey = lastKey
		m.hasLast = true
	}
	return true
}

func (m *BTree) applyCopySortedBatchLoadSortedLocked(entries []batchpkg.Entry, idxs []int, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	n := btreeSortedBatchLen(entries, idxs)
	if n == 0 {
		return true
	}
	recordBTreeBatchLoadSortedAttempt(btreeBatchLoadSortedKindCopy, n)
	if n < btreeLoadSortedMinBatchEntries {
		recordBTreeBatchLoadSortedReject(btreeBatchLoadSortedRejectTooSmall, n)
		return false
	}
	if reason := m.canApplySortedBatchLoadSortedLocked(entries, idxs); reason != btreeBatchLoadSortedRejectNone {
		recordBTreeBatchLoadSortedReject(reason, n)
		return false
	}
	recordBTreeBatchLoadSortedHit(btreeBatchLoadSortedKindCopy, n)
	var addedSize int64
	var addedDeletes int
	var lastKey string
	m.tree.LoadSortedItemsTrusted(n, func(i int) (string, btreeEntry) {
		op := btreeSortedBatchEntryAt(entries, idxs, i)
		keyCopy, entry := m.copyKeyEntryFromBatchOpLocked(op, storeInlinePtrValues)
		keyStr := bytesToStringNoCopy(keyCopy)
		if onKey == nil {
			addedSize += int64(len(keyCopy) + m.btreeEntryPayloadSize(entry))
			if entry.flags()&node.FlagTombstone != 0 {
				addedDeletes++
			}
			lastKey = keyStr
		} else {
			m.recordSetLocked(keyStr, len(keyCopy), entry, btreeEntry{}, false)
		}
		if onKey != nil {
			onKey(op.Key)
		}
		return keyStr, entry
	})
	if onKey == nil {
		m.sizeBytes += addedSize
		m.deleteCount += addedDeletes
		m.lastKey = lastKey
		m.hasLast = true
	}
	return true
}

func (m *BTree) canApplySortedBatchLoadSortedLocked(entries []batchpkg.Entry, idxs []int) btreeBatchLoadSortedReject {
	first := btreeSortedBatchEntryAt(entries, idxs, 0)
	if first.Key == nil {
		return btreeBatchLoadSortedRejectNilKey
	}
	prev := bytesToStringNoCopy(first.Key)
	if m.hasLast && prev <= m.lastKey {
		return btreeBatchLoadSortedRejectOverlapLast
	}
	if idxs != nil {
		return btreeBatchLoadSortedRejectNone
	}
	n := btreeSortedBatchLen(entries, idxs)
	for i := 1; i < n; i++ {
		op := btreeSortedBatchEntryAt(entries, idxs, i)
		if op.Key == nil {
			return btreeBatchLoadSortedRejectNilKey
		}
		key := bytesToStringNoCopy(op.Key)
		if key <= prev {
			return btreeBatchLoadSortedRejectNonIncreasing
		}
		prev = key
	}
	return btreeBatchLoadSortedRejectNone
}

func btreeSortedBatchLen(entries []batchpkg.Entry, idxs []int) int {
	if idxs == nil {
		return len(entries)
	}
	return len(idxs)
}

func btreeSortedBatchEntryAt(entries []batchpkg.Entry, idxs []int, i int) batchpkg.Entry {
	if idxs == nil {
		return entries[i]
	}
	return entries[idxs[i]]
}

func (m *BTree) copyKeyEntryFromBatchOpLocked(op batchpkg.Entry, storeInlinePtrValues bool) ([]byte, btreeEntry) {
	switch {
	case op.Type == batchpkg.OpDelete:
		return m.arena.Copy(op.Key), newBTreeEntry(node.FlagTombstone, btreeArenaRef{})
	case op.IsPtr:
		value := op.Value
		if !storeInlinePtrValues {
			value = nil
		}
		keyCopy, ptrValue, ptrRef := m.copyKeyPointerValueLocked(op.Key, op.ValuePtr, value)
		return keyCopy, m.makeBTreeEntry(node.FlagPointer, ptrValue, ptrRef)
	default:
		value := canonicalizeBTreeInlineValue(op.Value)
		keyCopy, valueCopy, valueRef := m.copyKeyInlineValueLocked(op.Key, value)
		return keyCopy, m.makeBTreeEntry(node.FlagInline, valueCopy, valueRef)
	}
}

func NewBTreeWithDegree(degree int) *BTree {
	if degree <= 0 {
		degree = btreeDefaultDegree
	}
	return &BTree{
		tree:   newBTreeMap(degree),
		degree: degree,
		arena: &btreeArena{
			maxChunkSize:     btreeArenaChunkSize,
			initialChunkSize: btreeArenaInitialChunkSize,
		},
	}
}

func newBTreeMap(degree int) *btree.Map[string, btreeEntry] {
	return btree.NewMapWithOptions[string, btreeEntry](degree, btree.MapOptions{
		ReuseRightSplitCapacity:   true,
		ReuseSplitInsertCapacity:  true,
		LeafItemArena:             true,
		LeafItemArenaRetainChunks: btreeLeafItemArenaRetainChunks,
		NodeArena:                 true,
		NodeArenaRetainChunks:     btreeNodeArenaRetainChunks,
	})
}

// Reset clears all entries while retaining internal allocations.
func (m *BTree) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tree == nil {
		m.tree = newBTreeMap(m.degree)
	} else {
		m.tree.Clear()
	}
	m.sizeBytes = 0
	m.lastKey = ""
	m.hasLast = false
	m.lastInline = nil
	m.lastInlineRef = btreeArenaRef{}
	for i := range m.externalValues {
		m.externalValues[i] = nil
	}
	m.externalValues = nil
	m.deleteCount = 0
	m.nonAppendSetCount = 0
	m.reuseBothSplitInsertCapacity = false
	m.tree.SetReuseBothSplitInsertCapacity(false)
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

	keyStored, valStored, valRef := m.copyKeyInlineValueLocked(key, canonicalizeBTreeInlineValue(value))
	keyStr := bytesToStringNoCopy(keyStored)
	entry := m.makeBTreeEntry(node.FlagInline, valStored, valRef)
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyStored), entry, prev, replaced)
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

	entryFlags := normalizeBTreeEntryFlags(flags)
	entry := newBTreeEntry(entryFlags, btreeArenaRef{})
	var keyCopy []byte
	switch {
	case entryFlags&node.FlagTombstone != 0:
		keyCopy = m.arena.Copy(key)
	case entryFlags&node.FlagPointer != 0:
		var ptrValue []byte
		var ptrRef btreeArenaRef
		keyCopy, ptrValue, ptrRef = m.copyKeyPointerValueLocked(key, ptr, value)
		entry = m.makeBTreeEntry(entryFlags, ptrValue, ptrRef)
	default:
		value = canonicalizeBTreeInlineValue(value)
		var valueCopy []byte
		var valueRef btreeArenaRef
		keyCopy, valueCopy, valueRef = m.copyKeyInlineValueLocked(key, value)
		entry = m.makeBTreeEntry(entryFlags, valueCopy, valueRef)
	}
	keyStr := bytesToStringNoCopy(keyCopy)
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyCopy), entry, prev, replaced)
}

func (m *BTree) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	if key == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	keyStr := bytesToStringNoCopy(key)
	entryFlags := normalizeBTreeEntryFlags(flags)
	entry := newBTreeEntry(entryFlags, btreeArenaRef{})
	switch {
	case entryFlags&node.FlagTombstone != 0:
	case entryFlags&node.FlagPointer != 0:
		ptrValue, ptrRef := m.copyPointerValueLocked(ptr, value)
		entry = m.makeBTreeEntry(entryFlags, ptrValue, ptrRef)
	default:
		if entryFlags == node.FlagInline {
			entry = m.makeStealInlineBTreeEntry(value)
		} else {
			entry = m.makeExternalBTreeEntry(entryFlags, canonicalizeBTreeInlineValue(value))
		}
	}
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(key), entry, prev, replaced)
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
	entry := newBTreeEntry(node.FlagTombstone, btreeArenaRef{})
	prev, replaced := m.setMaybeLoadLocked(keyStr, entry)
	m.recordSetLocked(keyStr, len(keyStored), entry, prev, replaced)
	return nil
}

func (m *BTree) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *BTree) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, false, false
	}
	return m.btreeEntryValue(val), val.flags()&node.FlagTombstone != 0, true
}

func (m *BTree) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.tree.Get(bytesToStringNoCopy(key))
	if !ok {
		return nil, page.ValuePtr{}, 0, false
	}
	return m.btreeEntryValue(val), m.btreeEntryValuePtr(val), val.flags(), true
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

func (m *BTree) OperationMix() OperationMix {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return OperationMix{
		Entries: m.tree.Len(),
		Deletes: m.deleteCount,
	}
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
		owner:  m,
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
		owner:    m,
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
	owner  *BTree
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
	return it.owner.btreeEntryValue(it.cur)
}

func (it *btreeIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags()&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return it.owner.btreeEntryValue(it.cur), it.owner.btreeEntryValuePtr(it.cur), it.cur.flags()
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
	return it.cur.flags()&node.FlagTombstone != 0
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
	owner    *BTree
	mu       *sync.RWMutex
}

func (it *btreeReverseIterator) Seek(key []byte) {
	seekToReverseEnd := func() {
		if !it.hasEnd {
			it.valid = it.iter.Last()
			return
		}
		it.valid = it.iter.Seek(it.end)
		if it.valid {
			// Seek positions at the first key >= end. Reverse iteration is over
			// [start, end), so step back to the last key < end.
			it.valid = it.iter.Prev()
			return
		}
		it.valid = it.iter.Last()
	}

	if key == nil {
		seekToReverseEnd()
		it.refresh()
		return
	}
	keyStr := bytesToStringNoCopy(key)
	if it.hasEnd && strings.Compare(keyStr, it.end) >= 0 {
		seekToReverseEnd()
		it.refresh()
		return
	}
	found := it.iter.Seek(keyStr)
	if !found {
		it.valid = it.iter.Last()
		it.refresh()
		return
	}
	it.valid = true
	if strings.Compare(it.iter.Key(), keyStr) > 0 {
		it.valid = it.iter.Prev()
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
	return it.owner.btreeEntryValue(it.cur)
}

func (it *btreeReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.hasCur {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags()&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return it.owner.btreeEntryValue(it.cur), it.owner.btreeEntryValuePtr(it.cur), it.cur.flags()
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
	return it.cur.flags()&node.FlagTombstone != 0
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
	if !m.hasLast || key > m.lastKey {
		return m.tree.Load(key, entry)
	}
	prev, replaced := m.tree.Set(key, entry)
	if !replaced {
		m.noteNonAppendInsertLocked()
	}
	return prev, replaced
}

func (m *BTree) setMaybeSortedLoadLocked(key string, entry btreeEntry) (btreeEntry, bool) {
	if !m.hasLast || key > m.lastKey {
		return m.tree.Load(key, entry)
	}
	prev, replaced := m.tree.Set(key, entry)
	if !replaced {
		m.noteNonAppendInsertLocked()
	}
	return prev, replaced
}

func (m *BTree) noteNonAppendInsertLocked() {
	if m.reuseBothSplitInsertCapacity || m.tree.Len() < btreeAdaptiveBothSplitMinEntries {
		return
	}
	m.nonAppendSetCount++
	if m.nonAppendSetCount < btreeAdaptiveBothSplitMinNonAppend {
		return
	}
	m.reuseBothSplitInsertCapacity = true
	m.tree.SetReuseBothSplitInsertCapacity(true)
}

func (m *BTree) copyInlineValueLocked(value []byte) ([]byte, btreeArenaRef) {
	if len(value) == 0 {
		return nil, btreeArenaRef{}
	}
	if len(value) <= btreeInlineValueDedupeMax && bytes.Equal(value, m.lastInline) {
		return m.lastInline, m.lastInlineRef
	}
	stored, ref := m.arena.CopyWithRef(value)
	if len(stored) <= btreeInlineValueDedupeMax {
		m.lastInline = stored
		m.lastInlineRef = ref
	} else {
		m.lastInline = nil
		m.lastInlineRef = btreeArenaRef{}
	}
	return stored, ref
}

func (m *BTree) copyKeyInlineValueLocked(key, value []byte) ([]byte, []byte, btreeArenaRef) {
	if len(value) == 0 {
		return m.arena.Copy(key), nil, btreeArenaRef{}
	}
	if len(value) <= btreeInlineValueDedupeMax && bytes.Equal(value, m.lastInline) {
		return m.arena.Copy(key), m.lastInline, m.lastInlineRef
	}
	buf, ref := m.arena.allocWithRef(len(key) + len(value))
	keyCopy := buf[:len(key):len(key)]
	valueCopy := buf[len(key):]
	copy(keyCopy, key)
	copy(valueCopy, value)
	valueRef := ref.slice(len(key), len(value))
	if len(valueCopy) <= btreeInlineValueDedupeMax {
		m.lastInline = valueCopy
		m.lastInlineRef = valueRef
	} else {
		m.lastInline = nil
		m.lastInlineRef = btreeArenaRef{}
	}
	return keyCopy, valueCopy, valueRef
}

func (m *BTree) copyPointerValueLocked(ptr page.ValuePtr, value []byte) ([]byte, btreeArenaRef) {
	value = canonicalizeBTreeInlineValue(value)
	stored, ref := m.arena.allocWithRef(page.ValuePtrSize + len(value))
	ptr.Encode(stored[:page.ValuePtrSize])
	copy(stored[page.ValuePtrSize:], value)
	return stored, ref
}

func (m *BTree) copyKeyPointerValueLocked(key []byte, ptr page.ValuePtr, value []byte) ([]byte, []byte, btreeArenaRef) {
	value = canonicalizeBTreeInlineValue(value)
	buf, ref := m.arena.allocWithRef(len(key) + page.ValuePtrSize + len(value))
	keyCopy := buf[:len(key):len(key)]
	ptrValue := buf[len(key):]
	copy(keyCopy, key)
	ptr.Encode(ptrValue[:page.ValuePtrSize])
	copy(ptrValue[page.ValuePtrSize:], value)
	return keyCopy, ptrValue, ref.slice(len(key), page.ValuePtrSize+len(value))
}

func (m *BTree) btreeEntryFromBatchOpLocked(op batchpkg.Entry) btreeEntry {
	switch {
	case op.Type == batchpkg.OpDelete:
		return newBTreeEntry(node.FlagTombstone, btreeArenaRef{})
	case op.IsPtr:
		ptrValue, ptrRef := m.copyPointerValueLocked(op.ValuePtr, op.Value)
		return m.makeBTreeEntry(node.FlagPointer, ptrValue, ptrRef)
	default:
		return m.makeStealInlineBTreeEntry(op.Value)
	}
}

func (m *BTree) recordSetLocked(key string, keyLen int, entry, prev btreeEntry, replaced bool) {
	newSize := m.btreeEntryPayloadSize(entry)
	newDelete := entry.flags()&node.FlagTombstone != 0
	if replaced {
		oldSize := m.btreeEntryPayloadSize(prev)
		m.sizeBytes += int64(newSize - oldSize)
		oldDelete := prev.flags()&node.FlagTombstone != 0
		switch {
		case oldDelete && !newDelete:
			m.deleteCount--
		case !oldDelete && newDelete:
			m.deleteCount++
		}
	} else {
		m.sizeBytes += int64(keyLen + newSize)
		if newDelete {
			m.deleteCount++
		}
	}
	if !m.hasLast || key > m.lastKey {
		m.lastKey = key
		m.hasLast = true
	}
}

type btreeArena struct {
	maxChunkSize     int
	initialChunkSize int
	chunks           [][]byte
	offset           int
}

func (r btreeArenaRef) canEncode() bool {
	if r.length == 0 {
		return true
	}
	return r.chunk <= btreeEntryMaxChunk &&
		r.offset <= btreeEntryMaxOffset &&
		r.length <= btreeEntryMaxLen &&
		uint64(r.offset)+uint64(r.length) <= uint64(btreeEntryMaxOffset)+1
}

func (r btreeArenaRef) slice(offset, length int) btreeArenaRef {
	return btreeArenaRef{
		chunk:  r.chunk,
		offset: r.offset + uint32(offset),
		length: uint32(length),
	}
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
	if a.maxChunkSize > 0 && cap(first) > a.maxChunkSize {
		for i := range a.chunks {
			putBTreeArenaChunk(a.chunks[i])
			a.chunks[i] = nil
		}
		a.chunks = nil
		a.offset = 0
		return
	}
	first = first[:cap(first)]
	for i := 1; i < len(a.chunks); i++ {
		putBTreeArenaChunk(a.chunks[i])
		a.chunks[i] = nil
	}
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

func (a *btreeArena) CopyWithRef(src []byte) ([]byte, btreeArenaRef) {
	if len(src) == 0 {
		return nil, btreeArenaRef{}
	}
	dst, ref := a.allocWithRef(len(src))
	copy(dst, src)
	return dst, ref
}

func (a *btreeArena) alloc(n int) []byte {
	dst, _ := a.allocWithRef(n)
	return dst
}

func (a *btreeArena) allocWithRef(n int) ([]byte, btreeArenaRef) {
	if n <= 0 {
		return nil, btreeArenaRef{}
	}
	chunk := a.currentChunk(n)
	chunkIndex := len(a.chunks) - 1
	start := a.offset
	a.offset += n
	return chunk[start:a.offset], btreeArenaRef{
		chunk:  uint32(chunkIndex),
		offset: uint32(start),
		length: uint32(n),
	}
}

func (a *btreeArena) slice(ref btreeArenaRef) []byte {
	if ref.length == 0 || a == nil || int(ref.chunk) >= len(a.chunks) {
		return nil
	}
	chunk := a.chunks[ref.chunk]
	end := ref.offset + ref.length
	if end > uint32(len(chunk)) {
		return nil
	}
	return chunk[ref.offset:end]
}

func (a *btreeArena) currentChunk(n int) []byte {
	if a == nil {
		return make([]byte, n)
	}
	if len(a.chunks) == 0 {
		size := a.initialChunkSize
		if size <= 0 {
			size = a.maxChunkSize
		}
		if size < n {
			size = n
		}
		a.chunks = append(a.chunks, getBTreeArenaChunk(size))
		a.offset = 0
		return a.chunks[len(a.chunks)-1]
	}
	chunk := a.chunks[len(a.chunks)-1]
	if a.offset+n <= len(chunk) {
		return chunk
	}
	size := len(chunk) * 2
	if max := a.maxChunkSize; max > 0 && size > max {
		size = max
	}
	if size <= 0 {
		size = a.maxChunkSize
	}
	if size < n {
		size = n
	}
	a.chunks = append(a.chunks, getBTreeArenaChunk(size))
	a.offset = 0
	return a.chunks[len(a.chunks)-1]
}

func getBTreeArenaChunk(size int) []byte {
	if size <= 0 {
		return nil
	}
	idx, classSize, ok := btreeArenaClassForLen(size)
	if !ok {
		return make([]byte, size)
	}
	if v := btreeArenaChunkPools[idx].Get(); v != nil {
		if chunk, ok := v.([]byte); ok {
			if next := btreeArenaPoolBytes.Add(-int64(cap(chunk))); next < 0 {
				btreeArenaPoolBytes.Store(0)
			}
			if cap(chunk) == classSize {
				return chunk[:classSize]
			}
		}
	}
	maybeResetBTreeArenaPoolBytesAfterGC()
	return make([]byte, classSize)
}

func putBTreeArenaChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	idx, ok := btreeArenaClassForCap(cap(chunk))
	if !ok {
		return
	}
	size := int64(cap(chunk))
	noteEpoch := false
	for {
		held := btreeArenaPoolBytes.Load()
		if held+size > btreeArenaPoolBudgetBytes {
			before := held
			maybeResetBTreeArenaPoolBytesAfterGC()
			held = btreeArenaPoolBytes.Load()
			if held == before || held+size > btreeArenaPoolBudgetBytes {
				return
			}
			continue
		}
		if btreeArenaPoolBytes.CompareAndSwap(held, held+size) {
			noteEpoch = held == 0
			break
		}
	}
	if noteEpoch {
		noteBTreeArenaPoolGC(btreeArenaPoolNumGC())
	}
	btreeArenaChunkPools[idx].Put(chunk[:0])
}

func maybeResetBTreeArenaPoolBytesAfterGC() {
	if btreeArenaPoolBytes.Load() <= 0 {
		return
	}
	numGC := btreeArenaPoolNumGC()
	last := btreeArenaPoolLastGC.Load()
	if last == numGC {
		return
	}
	if btreeArenaPoolLastGC.CompareAndSwap(last, numGC) {
		btreeArenaPoolBytes.Store(0)
	}
}

func noteBTreeArenaPoolGC(numGC uint64) {
	if numGC == 0 {
		return
	}
	for {
		last := btreeArenaPoolLastGC.Load()
		if last >= numGC {
			return
		}
		if btreeArenaPoolLastGC.CompareAndSwap(last, numGC) {
			return
		}
	}
}

func btreeArenaClassForLen(size int) (idx int, classSize int, ok bool) {
	minSize := 1 << btreeArenaPoolMinShift
	maxSize := 1 << btreeArenaPoolMaxShift
	if size < minSize || size > maxSize {
		return 0, 0, false
	}
	classSize = 1 << uint(bits.Len(uint(size-1)))
	if classSize < minSize {
		classSize = minSize
	}
	if classSize > maxSize {
		return 0, 0, false
	}
	idx = bits.Len(uint(classSize)) - 1 - btreeArenaPoolMinShift
	if idx < 0 || idx >= btreeArenaPoolClassCount {
		return 0, 0, false
	}
	return idx, classSize, true
}

func btreeArenaClassForCap(capacity int) (idx int, ok bool) {
	minSize := 1 << btreeArenaPoolMinShift
	maxSize := 1 << btreeArenaPoolMaxShift
	if capacity < minSize || capacity > maxSize || capacity&(capacity-1) != 0 {
		return 0, false
	}
	idx = bits.TrailingZeros(uint(capacity)) - btreeArenaPoolMinShift
	if idx < 0 || idx >= btreeArenaPoolClassCount {
		return 0, false
	}
	return idx, true
}
