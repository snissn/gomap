package memtable

import (
	"bytes"
	"sort"
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type hashEntry struct {
	value    []byte // inline bytes (pointer bytes stored separately in ptr)
	ptr      page.ValuePtr
	revision page.EntryRevision
	flags    byte
}

const (
	// Heuristic for initial hash map sizing from configured memtable capacity.
	// Hash-sorted entries carry map/table overhead beyond key/value payload,
	// so use a conservative bytes-per-entry estimate to avoid over-allocation.
	hashSortedEstimatedBytesPerEntry = 64
	hashSortedMinInitialEntries      = 128
	hashSortedMaxInitialEntries      = 1 << 20
)

func hashEntryValueSize(flags byte, value []byte) int {
	if flags&node.FlagPointer != 0 {
		return page.ValuePtrSize + len(value)
	}
	if flags&node.FlagTombstone != 0 {
		return 0
	}
	return len(value)
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
	items       map[string]uint32
	entries     []hashEntry
	sizeBytes   int64
	maxKey      string
	hasMaxKey   bool
	sortedKeys  []string
	sortedDel   []bool
	sortedValid bool
	frozen      bool
	hasDeletes  bool
	arena       hashArena

	// Incremental ordering:
	// We record first-seen keys into pendingKeys and seal them into chunk slices.
	// A global background worker sorts those chunks and stores them as sorted runs.
	pendingKeys   []string
	pendingBytes  int
	pendingSorted bool
	nextSeq       uint64
	index         hashSortedIndex
	indexer       *HashSortedIndexer

	finalizeOnce sync.Once
	finalizeDone chan struct{}
}

func (*HashSorted) StableUnsafeIteratorSlices() bool { return true }

type hashSortedIndex struct {
	mu      sync.Mutex
	cond    *sync.Cond
	waiters int
	done    map[uint64]struct{}
	doneTo  uint64

	// runs stores sorted key chunks indexed by sequence number (seq-1).
	// When doneTo >= N, runs[0:N] are populated.
	runs [][]string
}

func NewHashSorted() *HashSorted {
	return NewHashSortedWithCapacityAndIndexer(0, nil)
}

func NewHashSortedWithIndexer(indexer *HashSortedIndexer) *HashSorted {
	return NewHashSortedWithCapacityAndIndexer(0, indexer)
}

func NewHashSortedWithCapacityAndIndexer(capacity int, indexer *HashSortedIndexer) *HashSorted {
	if indexer == nil {
		indexer = globalHashSortedIndexer
	}
	initialEntries := hashSortedInitialEntries(capacity)
	m := &HashSorted{
		items:       make(map[string]uint32, initialEntries),
		entries:     make([]hashEntry, 0, initialEntries),
		sortedValid: true,
		indexer:     indexer,
	}
	m.index.cond = sync.NewCond(&m.index.mu)
	return m
}

func hashSortedInitialEntries(capacity int) int {
	if capacity <= 0 {
		return 0
	}
	entries := capacity / hashSortedEstimatedBytesPerEntry
	if entries < hashSortedMinInitialEntries {
		entries = hashSortedMinInitialEntries
	}
	if entries > hashSortedMaxInitialEntries {
		entries = hashSortedMaxInitialEntries
	}
	return entries
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
	// Reset can be used to reuse the same memtable instance for full clears.
	// Because incremental indexing is asynchronous, wait for in-flight chunks to
	// finish before reusing the arena memory.
	m.mu.RLock()
	finalizeDone := m.finalizeDone
	m.mu.RUnlock()
	if finalizeDone != nil {
		<-finalizeDone
	}

	target := m.nextSeq
	m.index.wait(target)

	m.mu.Lock()

	clear(m.items)
	for i := range m.entries {
		m.entries[i] = hashEntry{}
	}
	m.entries = m.entries[:0]
	m.sizeBytes = 0
	m.sortedKeys = m.sortedKeys[:0]
	m.sortedDel = m.sortedDel[:0]
	m.sortedValid = true
	m.frozen = false
	m.hasDeletes = false
	m.maxKey = ""
	m.hasMaxKey = false
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.pendingSorted = false
	m.nextSeq = 0
	m.index.reset()
	m.finalizeOnce = sync.Once{}
	m.finalizeDone = nil
	m.arena.resetKeepFirstChunk()
	m.mu.Unlock()
}

func (m *HashSorted) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, onKey, false)
}

func (m *HashSorted) ApplyStealSortedBatchTrusted(entries []batchpkg.Entry, onKey func(key []byte)) {
	m.applyStealSortedBatch(entries, onKey, true)
}

func (m *HashSorted) ApplyCopySortedBatchTrusted(entries []batchpkg.Entry, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	borrowedValues := false
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	if m.canAppendAfterMaxLocked(entries) {
		keys := make([]string, 0, len(entries))
		keyBytes := 0
		for _, op := range entries {
			if keyStored, n, borrowed, ok := m.setEntryNewCopyNoChunkLocked(op, borrowValues, storeInlinePtrValues); ok {
				keys = append(keys, keyStored)
				keyBytes += n
				borrowedValues = borrowedValues || borrowed
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		if chunk, seq, sorted := m.noteNewKeysBatchLocked(keys, keyBytes, true); seq != 0 {
			chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk, sorted: sorted})
		}
	} else {
		for _, op := range entries {
			value, ptr, flags := hashSortedBatchEntryPayload(op, storeInlinePtrValues)
			borrowValue := borrowValues && hashSortedCanBorrowEntryValue(value, flags)
			if chunk, seq := m.setEntryCopyKeyLocked(op.Key, value, ptr, flags, op.Revision, borrowValue); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
			borrowedValues = borrowedValues || borrowValue
			if onKey != nil {
				onKey(op.Key)
			}
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.keys, c.sorted)
	}
	return borrowedValues
}

func (m *HashSorted) applyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte), trustedOrder bool) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	canAppend := false
	if trustedOrder {
		canAppend = m.canAppendAfterMaxLocked(entries)
	} else {
		canAppend = m.canAppendSortedBatchLocked(entries)
	}
	if canAppend {
		keys := make([]string, 0, len(entries))
		keyBytes := 0
		for _, op := range entries {
			if keyStored, n, ok := m.setEntryNewStealNoChunkLocked(op); ok {
				keys = append(keys, keyStored)
				keyBytes += n
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
		if chunk, seq, sorted := m.noteNewKeysBatchLocked(keys, keyBytes, true); seq != 0 {
			chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk, sorted: sorted})
		}
	} else {
		for _, op := range entries {
			if op.Type == batchpkg.OpDelete {
				if chunk, seq := m.setEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, op.Revision, true); seq != 0 {
					chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
				}
			} else if op.IsPtr {
				if chunk, seq := m.setEntryLocked(op.Key, nil, op.ValuePtr, node.FlagPointer, op.Revision, true); seq != 0 {
					chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
				}
			} else {
				if chunk, seq := m.setEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, op.Revision, true); seq != 0 {
					chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
				}
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.keys, c.sorted)
	}
}

func (m *HashSorted) indexApplySortedChunk(seq uint64, keys []string) {
	m.index.apply(seq, keys)
}

func (m *HashSorted) Set(key, value []byte) {
	m.SetEntry(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *HashSorted) SetSteal(key, value []byte) {
	m.SetEntrySteal(key, value, page.ValuePtr{}, node.FlagInline)
}

func (m *HashSorted) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntryWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (m *HashSorted) SetEntryWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	m.mu.Lock()
	chunk, seq := m.setEntryLocked(key, value, ptr, flags, revision, false)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk, false)
	}
}

func (m *HashSorted) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.SetEntryStealWithRevision(key, value, ptr, flags, page.LegacyEntryRevision)
}

func (m *HashSorted) SetEntryStealWithRevision(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) {
	m.mu.Lock()
	chunk, seq := m.setEntryLocked(key, value, ptr, flags, revision, true)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk, false)
	}
}

func (m *HashSorted) entryForWriteLocked(key string) (*hashEntry, bool) {
	idx, ok := m.items[key]
	if !ok {
		return nil, false
	}
	i := int(idx)
	if i < 0 || i >= len(m.entries) {
		// Corrupted/stale index entry: drop it and treat this operation as a miss
		// so callers still apply the mutation instead of silently losing it.
		delete(m.items, key)
		return nil, false
	}
	return &m.entries[i], true
}

func (m *HashSorted) entryForReadLocked(key string) (hashEntry, bool) {
	idx, ok := m.items[key]
	if !ok {
		return hashEntry{}, false
	}
	i := int(idx)
	if i < 0 || i >= len(m.entries) {
		delete(m.items, key)
		return hashEntry{}, false
	}
	return m.entries[i], true
}

func (m *HashSorted) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}

	var chunk []string
	var seq uint64

	m.mu.Lock()
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.entryForWriteLocked(keyLookup); ok {
		valCopy := m.encodeEntryValueLocked(value, page.ValuePtr{}, node.FlagInline, false)
		if cb != nil {
			keyView := key
			if len(key) > 0 {
				keyView = m.arena.copyBytes(key)
			}
			if err := cb(keyView, valCopy); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		oldLen := hashEntryValueSize(ent.flags, ent.value)
		ent.value = valCopy
		ent.ptr = page.ValuePtr{}
		ent.flags = node.FlagInline
		ent.revision = page.LegacyEntryRevision
		m.sizeBytes += int64(hashEntryValueSize(ent.flags, ent.value) - oldLen)
		m.mu.Unlock()
		return nil
	}

	keyCopy := m.arena.copyBytes(key)
	valCopy := m.encodeEntryValueLocked(value, page.ValuePtr{}, node.FlagInline, false)
	if cb != nil {
		if err := cb(keyCopy, valCopy); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.entries = append(m.entries, hashEntry{value: valCopy, flags: node.FlagInline})
	m.items[keyStored] = uint32(len(m.entries) - 1)
	m.sizeBytes += int64(len(keyCopy) + hashEntryValueSize(node.FlagInline, valCopy))
	m.updateMaxKeyLocked(keyStored)
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()

	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk, false)
	}
	return nil
}

func (m *HashSorted) Delete(key []byte) {
	m.SetEntry(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *HashSorted) DeleteSteal(key []byte) {
	m.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
}

func (m *HashSorted) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	var chunk []string
	var seq uint64

	m.mu.Lock()
	m.hasDeletes = true

	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.entryForWriteLocked(keyLookup); ok {
		if cb != nil {
			keyView := key
			if len(key) > 0 {
				keyView = m.arena.copyBytes(key)
			}
			if err := cb(keyView, nil); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		if ent.flags&node.FlagTombstone == 0 {
			m.sizeBytes -= int64(hashEntryValueSize(ent.flags, ent.value))
			ent.value = nil
			ent.ptr = page.ValuePtr{}
			ent.flags = node.FlagTombstone
		}
		ent.revision = page.LegacyEntryRevision
		m.mu.Unlock()
		return nil
	}

	keyCopy := m.arena.copyBytes(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.entries = append(m.entries, hashEntry{flags: node.FlagTombstone})
	m.items[keyStored] = uint32(len(m.entries) - 1)
	m.sizeBytes += int64(len(keyCopy))
	m.updateMaxKeyLocked(keyStored)
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk, false)
	}
	return nil
}

func (m *HashSorted) Get(key []byte) ([]byte, bool, bool) {
	keyLookup := bytesToStringNoCopy(key)

	m.mu.RLock()
	idx, ok := m.items[keyLookup]
	if !ok {
		m.mu.RUnlock()
		return nil, false, false
	}
	i := int(idx)
	if i < 0 || i >= len(m.entries) {
		m.mu.RUnlock()
		m.mu.Lock()
		ent, ok := m.entryForReadLocked(keyLookup)
		m.mu.Unlock()
		if !ok {
			return nil, false, false
		}
		return ent.value, ent.flags&node.FlagTombstone != 0, true
	}
	ent := m.entries[i]
	m.mu.RUnlock()
	if ent.flags&node.FlagPointer != 0 {
		return ent.value, ent.flags&node.FlagTombstone != 0, true
	}
	return ent.value, ent.flags&node.FlagTombstone != 0, true
}

func (m *HashSorted) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	val, ptr, flags, _, found := m.GetEntryWithRevision(key)
	return val, ptr, flags, found
}

func (m *HashSorted) GetEntryWithRevision(key []byte) ([]byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	keyLookup := bytesToStringNoCopy(key)

	m.mu.RLock()
	idx, ok := m.items[keyLookup]
	if !ok {
		m.mu.RUnlock()
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	i := int(idx)
	if i < 0 || i >= len(m.entries) {
		m.mu.RUnlock()
		m.mu.Lock()
		ent, ok := m.entryForReadLocked(keyLookup)
		m.mu.Unlock()
		if !ok {
			return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
		}
		if ent.flags&node.FlagPointer != 0 {
			return ent.value, ent.ptr, ent.flags, ent.revision, true
		}
		return ent.value, page.ValuePtr{}, ent.flags, ent.revision, true
	}
	ent := m.entries[i]
	m.mu.RUnlock()
	if ent.flags&node.FlagPointer != 0 {
		return ent.value, ent.ptr, ent.flags, ent.revision, true
	}
	return ent.value, page.ValuePtr{}, ent.flags, ent.revision, true
}

func (m *HashSorted) SeekGE(start, end []byte) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	m.mu.RLock()
	frozen := m.frozen
	finalizeDone := m.finalizeDone
	m.mu.RUnlock()
	if frozen && finalizeDone != nil {
		<-finalizeDone
		m.ensureIndexFrozen()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// CommitAt/GetAt commonly seeks the physical key it just wrote. Resolve that
	// exact hit from the hash index before rebuilding sortedKeys: every new key
	// invalidates the sorted index, so sorting here would turn an interleaved
	// write/read workload into an O(n log n) rebuild per read.
	if ent, ok := m.entryForReadLocked(bytesToStringNoCopy(start)); ok {
		return hashSortedSeekResult(start, ent)
	}
	if !m.sortedValid {
		m.ensureSortedLocked()
	}
	idx := sort.SearchStrings(m.sortedKeys, bytesToStringNoCopy(start))
	if idx >= len(m.sortedKeys) {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	keyString := m.sortedKeys[idx]
	key := stringToBytesNoCopy(keyString)
	if end != nil && bytes.Compare(key, end) >= 0 {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	ent, ok := m.entryForReadLocked(keyString)
	if !ok {
		return nil, nil, page.ValuePtr{}, 0, page.LegacyEntryRevision, false
	}
	return hashSortedSeekResult(key, ent)
}

func hashSortedSeekResult(key []byte, ent hashEntry) ([]byte, []byte, page.ValuePtr, byte, page.EntryRevision, bool) {
	if ent.flags&node.FlagTombstone != 0 {
		return key, nil, page.ValuePtr{}, ent.flags, ent.revision, true
	}
	if ent.flags&node.FlagPointer != 0 {
		return key, ent.value, ent.ptr, ent.flags, ent.revision, true
	}
	return key, ent.value, page.ValuePtr{}, ent.flags, ent.revision, true
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
	if m.frozen {
		m.mu.Unlock()
		return
	}
	m.frozen = true
	m.mu.Unlock()
	m.startFinalize()
}

// PreferSortedPointProbes reports whether a sorted batch of point probes should
// use individual hash lookups instead of one iterator scan. Frozen hash-sorted
// tables have stable maps; sparse random batches avoid scanning large key gaps.
func (m *HashSorted) PreferSortedPointProbes(first, last []byte, refCount int) bool {
	if m == nil || refCount <= 0 {
		return false
	}
	m.mu.RLock()
	frozen := m.frozen
	count := len(m.items)
	m.mu.RUnlock()
	if !frozen {
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
	return refCount*4 < count || refCount <= 256
}

func (m *HashSorted) NewIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	frozen := m.frozen
	finalizeDone := m.finalizeDone
	sortedValid := m.sortedValid
	m.mu.RUnlock()

	if frozen {
		if finalizeDone != nil {
			<-finalizeDone
		}
		m.ensureIndexFrozen()

		endKey := ""
		hasEnd := false
		if end != nil {
			endKey = bytesToStringNoCopy(end)
			hasEnd = true
		}

		m.mu.RLock()
		keys := m.sortedKeys
		it := hashIteratorPool.Get().(*hashIterator)
		*it = hashIterator{
			mt:     m,
			keys:   keys,
			end:    endKey,
			hasEnd: hasEnd,
			mu:     &m.mu,
		}
		it.Seek(start)
		return it
	}

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

	idx := 0
	if startKey != "" {
		idx = sort.SearchStrings(keys, startKey)
	}

	it := hashIteratorPool.Get().(*hashIterator)
	*it = hashIterator{
		mt:     m,
		keys:   keys,
		idx:    idx,
		end:    endKey,
		hasEnd: hasEnd,
		mu:     &m.mu,
	}
	it.refresh()
	return it
}

func (m *HashSorted) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	m.mu.RLock()
	frozen := m.frozen
	finalizeDone := m.finalizeDone
	sortedValid := m.sortedValid
	m.mu.RUnlock()

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

	if frozen {
		if finalizeDone != nil {
			<-finalizeDone
		}
		m.ensureIndexFrozen()

		m.mu.RLock()
		keys := m.sortedKeys
		idx := len(keys) - 1
		if hasEnd {
			pos := sort.SearchStrings(keys, endKey)
			idx = pos - 1
		}
		it := hashReverseIteratorPool.Get().(*hashReverseIterator)
		*it = hashReverseIterator{
			mt:       m,
			keys:     keys,
			idx:      idx,
			start:    startKey,
			hasStart: hasStart,
			end:      endKey,
			hasEnd:   hasEnd,
			mu:       &m.mu,
		}
		it.refresh()
		return it
	}

	if !sortedValid {
		m.mu.Lock()
		m.ensureSortedLocked()
		m.mu.Unlock()
	}

	m.mu.RLock()
	keys := m.sortedKeys
	idx := len(keys) - 1
	if hasEnd {
		pos := sort.SearchStrings(keys, endKey)
		idx = pos - 1
	}
	it := hashReverseIteratorPool.Get().(*hashReverseIterator)
	*it = hashReverseIterator{
		mt:       m,
		keys:     keys,
		idx:      idx,
		start:    startKey,
		hasStart: hasStart,
		end:      endKey,
		hasEnd:   hasEnd,
		mu:       &m.mu,
	}
	it.refresh()
	return it
}

func (m *HashSorted) ensureSortedLocked() {
	if m.sortedValid {
		return
	}
	if len(m.sortedKeys) != len(m.items) {
		keys := make([]string, len(m.items))
		i := 0
		for k := range m.items {
			keys[i] = k
			i++
		}
		m.sortedKeys = keys
	}
	sort.Strings(m.sortedKeys)
	m.sortedValid = true
}

func (m *HashSorted) ensureIndexFrozen() {
	m.mu.RLock()
	if m.sortedValid {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	m.mu.Lock()
	if m.sortedValid {
		m.mu.Unlock()
		return
	}
	target := m.nextSeq
	pending := m.pendingKeys
	pendingSorted := m.pendingSorted
	dst := m.sortedKeys
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.pendingSorted = false
	m.mu.Unlock()

	m.index.wait(target)

	// Seal any remaining keys locally as a final chunk after prior chunks have
	// completed to preserve the seq->run mapping.
	if len(pending) > 0 {
		if !pendingSorted {
			sort.Strings(pending)
		}
		finalSeq := target + 1
		m.index.apply(finalSeq, pending)
		target = finalSeq
		m.mu.Lock()
		if m.nextSeq < target {
			m.nextSeq = target
		}
		m.mu.Unlock()
	}

	runs, total := m.index.snapshotRuns(target)
	if total == 0 {
		m.mu.Lock()
		m.sortedKeys = m.sortedKeys[:0]
		m.sortedDel = m.sortedDel[:0]
		m.sortedValid = true
		m.mu.Unlock()
		m.index.dropRuns()
		return
	}

	dst = mergeSortedStringRunsInto(dst, runs, total)

	var del []bool
	if m.hasDeletes {
		if cap(m.sortedDel) >= len(dst) {
			del = m.sortedDel[:len(dst)]
			clear(del)
		} else {
			del = make([]bool, len(dst))
		}
		m.mu.RLock()
		for i, k := range dst {
			idx, ok := m.items[k]
			if !ok {
				continue
			}
			ei := int(idx)
			if ei < 0 || ei >= len(m.entries) {
				continue
			}
			ent := m.entries[ei]
			del[i] = ent.flags&node.FlagTombstone != 0
		}
		m.mu.RUnlock()
	}

	m.mu.Lock()
	if !m.sortedKeysMatchItemsLocked(dst, total) {
		m.mu.Unlock()

		// Safety fallback: if indexing missed keys, rebuild directly from the map.
		keys := make([]string, 0, len(m.items))
		m.mu.RLock()
		for k := range m.items {
			keys = append(keys, k)
		}
		m.mu.RUnlock()
		sort.Strings(keys)

		if m.hasDeletes {
			if cap(del) < len(keys) {
				del = make([]bool, len(keys))
			} else {
				del = del[:len(keys)]
				clear(del)
			}
			m.mu.RLock()
			for i, k := range keys {
				idx, ok := m.items[k]
				if !ok {
					continue
				}
				ei := int(idx)
				if ei < 0 || ei >= len(m.entries) {
					continue
				}
				ent := m.entries[ei]
				del[i] = ent.flags&node.FlagTombstone != 0
			}
			m.mu.RUnlock()
		} else {
			del = del[:0]
		}

		m.mu.Lock()
		m.sortedKeys = keys
		m.sortedDel = del
		m.sortedValid = true
		m.mu.Unlock()
		m.index.dropRuns()
		return
	}
	m.sortedKeys = dst
	if m.hasDeletes {
		m.sortedDel = del
	} else {
		m.sortedDel = m.sortedDel[:0]
	}
	m.sortedValid = true
	m.mu.Unlock()
	m.index.dropRuns()
}

func (m *HashSorted) sortedKeysMatchItemsLocked(keys []string, total int) bool {
	if len(m.items) != total || len(keys) != total {
		return false
	}
	prev := ""
	for i, k := range keys {
		if i > 0 && strings.Compare(prev, k) >= 0 {
			return false
		}
		if _, ok := m.items[k]; !ok {
			return false
		}
		prev = k
	}
	return true
}

func (m *HashSorted) startFinalize() {
	m.finalizeOnce.Do(func() {
		done := make(chan struct{})
		m.mu.Lock()
		m.finalizeDone = done
		m.mu.Unlock()
		go func() {
			m.ensureIndexFrozen()
			close(done)
		}()
	})
}

func (m *HashSorted) notePendingKeyOrderLocked(key string) {
	if len(m.pendingKeys) == 0 {
		m.pendingSorted = true
		return
	}
	if m.pendingSorted && strings.Compare(m.pendingKeys[len(m.pendingKeys)-1], key) < 0 {
		return
	}
	m.pendingSorted = false
}

func (m *HashSorted) noteNewKeyLocked(key string) (chunk []string, seq uint64) {
	m.sortedValid = false
	if m.pendingKeys == nil {
		m.pendingKeys = make([]string, 0, hashSortedPendingKeysInitialCap(1))
	}
	m.maybeUpgradePendingKeysLocked(1)
	m.notePendingKeyOrderLocked(key)
	m.pendingKeys = append(m.pendingKeys, key)
	m.pendingBytes += len(key)
	if m.pendingBytes < hashSortedSealBytesThreshold && len(m.pendingKeys) < hashSortedSealKeysThreshold {
		return nil, 0
	}

	chunk = m.pendingKeys
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.pendingSorted = false
	m.nextSeq++
	return chunk, m.nextSeq
}

func (m *HashSorted) noteNewKeysBatchLocked(keys []string, keyBytes int, keysSorted bool) (chunk []string, seq uint64, sorted bool) {
	if len(keys) == 0 {
		return nil, 0, false
	}
	m.sortedValid = false
	if m.pendingKeys == nil {
		m.pendingKeys = make([]string, 0, hashSortedPendingKeysInitialCap(len(keys)))
	} else {
		m.maybeUpgradePendingKeysLocked(len(keys))
	}
	if len(m.pendingKeys) == 0 {
		m.pendingSorted = keysSorted
	} else if !(m.pendingSorted && keysSorted && strings.Compare(m.pendingKeys[len(m.pendingKeys)-1], keys[0]) < 0) {
		m.pendingSorted = false
	}
	m.pendingKeys = append(m.pendingKeys, keys...)
	m.pendingBytes += keyBytes
	if m.pendingBytes < hashSortedSealBytesThreshold && len(m.pendingKeys) < hashSortedSealKeysThreshold {
		return nil, 0, false
	}

	chunk = m.pendingKeys
	sorted = m.pendingSorted
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.pendingSorted = false
	m.nextSeq++
	return chunk, m.nextSeq, sorted
}

func hashSortedPendingKeysInitialCap(keys int) int {
	if keys >= hashSortedPendingKeysUpgradeThreshold {
		if keys > hashSortedSealKeysThreshold {
			return keys
		}
		return hashSortedSealKeysThreshold
	}
	if keys > hashSortedPendingKeysInitCap {
		return keys
	}
	return hashSortedPendingKeysInitCap
}

func (m *HashSorted) maybeUpgradePendingKeysLocked(additional int) {
	if additional <= 0 {
		return
	}
	required := len(m.pendingKeys) + additional
	if required >= hashSortedPendingKeysUpgradeThreshold {
		if cap(m.pendingKeys) >= hashSortedSealKeysThreshold {
			return
		}
		nextCap := hashSortedSealKeysThreshold
		if required > nextCap {
			nextCap = required
		}
		next := make([]string, len(m.pendingKeys), nextCap)
		copy(next, m.pendingKeys)
		m.pendingKeys = next
		return
	}
	if required <= cap(m.pendingKeys) {
		return
	}
	nextCap := hashSortedPendingKeysUpgradeThreshold
	next := make([]string, len(m.pendingKeys), nextCap)
	copy(next, m.pendingKeys)
	m.pendingKeys = next
}

func (m *HashSorted) setEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, steal bool) ([]string, uint64) {
	return m.setEntryLockedWithOwnership(key, value, ptr, flags, revision, steal, steal)
}

func (m *HashSorted) setEntryCopyKeyLocked(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, borrowValue bool) ([]string, uint64) {
	return m.setEntryLockedWithOwnership(key, value, ptr, flags, revision, false, borrowValue)
}

func (m *HashSorted) setEntryLockedWithOwnership(key, value []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision, stealKey bool, borrowValue bool) ([]string, uint64) {
	if key == nil {
		return nil, 0
	}
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.entryForWriteLocked(keyLookup); ok {
		oldLen := hashEntryValueSize(ent.flags, ent.value)
		ent.value = m.encodeEntryValueLocked(value, ptr, flags, borrowValue)
		if flags&node.FlagPointer != 0 {
			ent.ptr = ptr
		} else {
			ent.ptr = page.ValuePtr{}
		}
		ent.flags = flags
		ent.revision = revision
		m.sizeBytes += int64(hashEntryValueSize(ent.flags, ent.value) - oldLen)
		if flags&node.FlagTombstone != 0 {
			m.hasDeletes = true
		}
		return nil, 0
	}

	var keyCopy []byte
	if stealKey {
		keyCopy = key
	} else {
		keyCopy = m.arena.copyBytes(key)
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	valCopy := m.encodeEntryValueLocked(value, ptr, flags, borrowValue)
	ent := hashEntry{value: valCopy, flags: flags, revision: revision}
	if flags&node.FlagPointer != 0 {
		ent.ptr = ptr
	}
	m.entries = append(m.entries, ent)
	m.items[keyStored] = uint32(len(m.entries) - 1)
	m.sizeBytes += int64(len(keyCopy) + hashEntryValueSize(flags, valCopy))
	if flags&node.FlagTombstone != 0 {
		m.hasDeletes = true
	}
	m.updateMaxKeyLocked(keyStored)
	return m.noteNewKeyLocked(keyStored)
}

func hashSortedBatchEntryPayload(op batchpkg.Entry, storeInlinePtrValues bool) (value []byte, ptr page.ValuePtr, flags byte) {
	return batchEntryPayload(op, storeInlinePtrValues)
}

func (m *HashSorted) setEntryNewCopyNoChunkLocked(op batchpkg.Entry, borrowValues bool, storeInlinePtrValues bool) (string, int, bool, bool) {
	if op.Key == nil {
		return "", 0, false, false
	}
	keyCopy := m.arena.copyBytes(op.Key)
	keyStored := bytesToStringNoCopy(keyCopy)
	value, ptr, flags := hashSortedBatchEntryPayload(op, storeInlinePtrValues)
	borrowValue := borrowValues && hashSortedCanBorrowEntryValue(value, flags)
	valCopy := m.encodeEntryValueLocked(value, ptr, flags, borrowValue)
	ent := hashEntry{value: valCopy, flags: flags, revision: op.Revision}
	if flags&node.FlagPointer != 0 {
		ent.ptr = ptr
	}
	m.entries = append(m.entries, ent)
	m.items[keyStored] = uint32(len(m.entries) - 1)
	m.sizeBytes += int64(len(keyCopy) + hashEntryValueSize(flags, valCopy))
	if flags&node.FlagTombstone != 0 {
		m.hasDeletes = true
	}
	// The fast path is append-only and entries are strictly increasing.
	m.maxKey = keyStored
	m.hasMaxKey = true
	return keyStored, len(keyCopy), borrowValue, true
}

func (m *HashSorted) setEntryNewStealNoChunkLocked(op batchpkg.Entry) (string, int, bool) {
	if op.Key == nil {
		return "", 0, false
	}

	keyStored := bytesToStringNoCopy(op.Key)
	flags := byte(node.FlagInline)
	val := op.Value
	ptr := page.ValuePtr{}
	switch {
	case op.Type == batchpkg.OpDelete:
		flags = node.FlagTombstone
		val = nil
	case op.IsPtr:
		flags = node.FlagPointer
		ptr = op.ValuePtr
	}

	ent := hashEntry{value: val, flags: flags, revision: op.Revision}
	if flags&node.FlagPointer != 0 {
		ent.ptr = ptr
	}
	m.entries = append(m.entries, ent)
	m.items[keyStored] = uint32(len(m.entries) - 1)
	m.sizeBytes += int64(len(op.Key) + hashEntryValueSize(flags, val))
	if flags&node.FlagTombstone != 0 {
		m.hasDeletes = true
	}
	// The fast path is append-only and entries are strictly increasing.
	m.maxKey = keyStored
	m.hasMaxKey = true
	return keyStored, len(op.Key), true
}

func (m *HashSorted) setEntryNewStealLocked(op batchpkg.Entry) ([]string, uint64) {
	keyStored, _, ok := m.setEntryNewStealNoChunkLocked(op)
	if !ok {
		return nil, 0
	}
	return m.noteNewKeyLocked(keyStored)
}

func (m *HashSorted) canAppendSortedBatchLocked(entries []batchpkg.Entry) bool {
	if len(entries) == 0 || entries[0].Key == nil {
		return false
	}
	prev := entries[0].Key
	for i := 1; i < len(entries); i++ {
		cur := entries[i].Key
		if cur == nil || bytes.Compare(cur, prev) <= 0 {
			return false
		}
		prev = cur
	}
	return m.canAppendAfterMaxLocked(entries)
}

func (m *HashSorted) canAppendAfterMaxLocked(entries []batchpkg.Entry) bool {
	if len(entries) == 0 || entries[0].Key == nil {
		return false
	}
	if len(m.items) == 0 {
		return true
	}
	if !m.hasMaxKey {
		return false
	}
	return strings.Compare(bytesToStringNoCopy(entries[0].Key), m.maxKey) > 0
}

func (m *HashSorted) updateMaxKeyLocked(key string) {
	if !m.hasMaxKey || strings.Compare(key, m.maxKey) > 0 {
		m.maxKey = key
		m.hasMaxKey = true
	}
}

func (m *HashSorted) encodeEntryValueLocked(value []byte, ptr page.ValuePtr, flags byte, steal bool) []byte {
	if flags&node.FlagPointer != 0 {
		if steal {
			return value
		}
		return m.arena.copyBytes(value)
	}
	if flags&node.FlagTombstone != 0 {
		return nil
	}
	if steal {
		return value
	}
	return m.arena.copyBytes(value)
}

func hashSortedCanBorrowEntryValue(value []byte, flags byte) bool {
	return len(value) > 0 && flags&node.FlagTombstone == 0
}

func (m *HashSorted) setStealLocked(key, value []byte) ([]string, uint64) {
	return m.setEntryLocked(key, value, page.ValuePtr{}, node.FlagInline, page.LegacyEntryRevision, true)
}

func (m *HashSorted) deleteStealLocked(key []byte) ([]string, uint64) {
	return m.setEntryLocked(key, nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision, true)
}

func (idx *hashSortedIndex) reset() {
	idx.mu.Lock()
	idx.runs = nil
	idx.doneTo = 0
	if idx.done != nil {
		clear(idx.done)
	}
	idx.mu.Unlock()
}

func (idx *hashSortedIndex) wait(target uint64) {
	if target == 0 || idx.cond == nil {
		return
	}
	idx.mu.Lock()
	for idx.doneTo < target {
		idx.waiters++
		idx.cond.Wait()
		idx.waiters--
	}
	idx.mu.Unlock()
}

func (idx *hashSortedIndex) apply(seq uint64, keys []string) {
	if len(keys) == 0 || seq == 0 {
		return
	}
	idx.mu.Lock()
	if int(seq) > len(idx.runs) {
		newRuns := make([][]string, seq)
		copy(newRuns, idx.runs)
		idx.runs = newRuns
	}
	idx.runs[seq-1] = keys
	idx.markDoneLocked(seq)
	if idx.waiters > 0 {
		idx.cond.Broadcast()
	}
	idx.mu.Unlock()
}

func (idx *hashSortedIndex) markDoneLocked(seq uint64) {
	if seq <= idx.doneTo {
		return
	}
	if seq == idx.doneTo+1 {
		idx.doneTo = seq
		for idx.done != nil {
			next := idx.doneTo + 1
			if _, ok := idx.done[next]; !ok {
				break
			}
			delete(idx.done, next)
			idx.doneTo = next
		}
		return
	}
	if idx.done == nil {
		idx.done = make(map[uint64]struct{}, 8)
	}
	idx.done[seq] = struct{}{}
}

func (idx *hashSortedIndex) snapshotRuns(target uint64) (runs [][]string, total int) {
	if target == 0 {
		return nil, 0
	}

	idx.mu.Lock()
	if int(target) > len(idx.runs) {
		target = uint64(len(idx.runs))
	}
	runs = make([][]string, 0, target)
	for i := uint64(0); i < target; i++ {
		run := idx.runs[i]
		if len(run) == 0 {
			continue
		}
		runs = append(runs, run)
		total += len(run)
	}
	idx.mu.Unlock()
	return runs, total
}

func (idx *hashSortedIndex) dropRuns() {
	idx.mu.Lock()
	idx.runs = nil
	idx.mu.Unlock()
}

type hashRunMergeItem struct {
	key string
	run int
	pos int
}

func heapPushMerge(h []hashRunMergeItem, item hashRunMergeItem) []hashRunMergeItem {
	h = append(h, item)
	i := len(h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if h[p].key <= h[i].key {
			break
		}
		h[p], h[i] = h[i], h[p]
		i = p
	}
	return h
}

func heapPopMerge(h []hashRunMergeItem) (hashRunMergeItem, []hashRunMergeItem) {
	n := len(h)
	out := h[0]
	last := h[n-1]
	h = h[:n-1]
	if n-1 == 0 {
		return out, h
	}
	h[0] = last
	i := 0
	for {
		l := 2*i + 1
		r := l + 1
		if l >= len(h) {
			break
		}
		small := l
		if r < len(h) && h[r].key < h[l].key {
			small = r
		}
		if h[i].key <= h[small].key {
			break
		}
		h[i], h[small] = h[small], h[i]
		i = small
	}
	return out, h
}

func mergeSortedStringRunsInto(dst []string, runs [][]string, total int) []string {
	if total <= 0 {
		return dst[:0]
	}

	if cap(dst) < total {
		dst = make([]string, total)
	} else {
		dst = dst[:total]
	}

	if len(runs) == 1 {
		copy(dst, runs[0])
		return dst
	}

	h := make([]hashRunMergeItem, 0, len(runs))
	for runIdx, run := range runs {
		if len(run) == 0 {
			continue
		}
		h = heapPushMerge(h, hashRunMergeItem{key: run[0], run: runIdx, pos: 0})
	}

	out := 0
	for len(h) > 0 {
		var item hashRunMergeItem
		item, h = heapPopMerge(h)
		dst[out] = item.key
		out++

		run := runs[item.run]
		nextPos := item.pos + 1
		if nextPos < len(run) {
			h = heapPushMerge(h, hashRunMergeItem{key: run[nextPos], run: item.run, pos: nextPos})
		}
	}

	return dst[:out]
}

type hashIterator struct {
	mt     *HashSorted
	keys   []string
	idx    int
	end    string
	hasEnd bool
	curKey string
	cur    hashEntry
	loaded bool
	valid  bool
	mu     *sync.RWMutex
}

var hashIteratorPool = sync.Pool{
	New: func() any {
		return new(hashIterator)
	},
}

type hashReverseIterator struct {
	mt       *HashSorted
	keys     []string
	idx      int
	start    string
	hasStart bool
	end      string
	hasEnd   bool
	curKey   string
	cur      hashEntry
	loaded   bool
	valid    bool
	mu       *sync.RWMutex
}

var hashReverseIteratorPool = sync.Pool{
	New: func() any {
		return new(hashReverseIterator)
	},
}

func (it *hashReverseIterator) Seek(key []byte) {
	if it.keys == nil {
		it.valid = false
		it.loaded = false
		return
	}
	if key == nil || (it.hasEnd && strings.Compare(bytesToStringNoCopy(key), it.end) >= 0) {
		if it.hasEnd {
			it.idx = sort.SearchStrings(it.keys, it.end) - 1
		} else {
			it.idx = len(it.keys) - 1
		}
		it.refresh()
		return
	}
	seekKey := bytesToStringNoCopy(key)
	pos := sort.Search(len(it.keys), func(i int) bool {
		return strings.Compare(it.keys[i], seekKey) > 0
	})
	it.idx = pos - 1
	it.refresh()
}

func (it *hashReverseIterator) Next() {
	it.idx--
	it.refresh()
}

func (it *hashReverseIterator) Valid() bool {
	return it.valid
}

func (it *hashReverseIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return stringToBytesNoCopy(it.curKey)
}

func (it *hashReverseIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	it.ensureLoaded()
	return it.cur.value
}

func (it *hashReverseIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *hashReverseIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision
	}
	it.ensureLoaded()
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone, it.cur.revision
	}
	if it.cur.flags&node.FlagPointer != 0 {
		return it.cur.value, it.cur.ptr, it.cur.flags, it.cur.revision
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline, it.cur.revision
}

func (it *hashReverseIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *hashReverseIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *hashReverseIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *hashReverseIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *hashReverseIterator) IsDeleted() bool {
	if !it.valid {
		return false
	}
	if !it.mt.hasDeletes {
		return false
	}
	if it.mt.frozen {
		if it.idx >= 0 && it.idx < len(it.mt.sortedDel) {
			return it.mt.sortedDel[it.idx]
		}
	}
	it.ensureLoaded()
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *hashReverseIterator) Error() error {
	return nil
}

func (it *hashReverseIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
	}
	*it = hashReverseIterator{}
	hashReverseIteratorPool.Put(it)
	return nil
}

func (it *hashReverseIterator) Domain() (start, end []byte) {
	if !it.hasStart && !it.hasEnd {
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

func (it *hashReverseIterator) refresh() {
	it.valid = false
	it.loaded = false
	it.curKey = ""
	if it.idx < 0 || it.idx >= len(it.keys) {
		return
	}
	k := it.keys[it.idx]
	if it.hasEnd && strings.Compare(k, it.end) >= 0 {
		return
	}
	if it.hasStart && strings.Compare(k, it.start) < 0 {
		return
	}
	it.curKey = k
	it.valid = true
}

func (it *hashReverseIterator) ensureLoaded() {
	if it.loaded {
		return
	}
	if !it.valid {
		return
	}
	idx, ok := it.mt.items[it.curKey]
	if !ok {
		it.valid = false
		return
	}
	i := int(idx)
	if i < 0 || i >= len(it.mt.entries) {
		it.valid = false
		return
	}
	ent := it.mt.entries[i]
	it.cur = ent
	it.loaded = true
}

func (it *hashIterator) Seek(key []byte) {
	if it.keys == nil {
		it.valid = false
		it.loaded = false
		return
	}
	seekKey := bytesToStringNoCopy(key)
	it.idx = sort.SearchStrings(it.keys, seekKey)
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
	return true
}

func (it *hashIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return stringToBytesNoCopy(it.curKey)
}

func (it *hashIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	it.ensureLoaded()
	if it.cur.flags&node.FlagPointer != 0 {
		return it.cur.value
	}
	return it.cur.value
}

func (it *hashIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}

func (it *hashIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone, page.LegacyEntryRevision
	}
	it.ensureLoaded()
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone, it.cur.revision
	}
	if it.cur.flags&node.FlagPointer != 0 {
		return it.cur.value, it.cur.ptr, it.cur.flags, it.cur.revision
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline, it.cur.revision
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
	if !it.mt.hasDeletes {
		return false
	}
	if it.mt.frozen {
		if it.idx >= 0 && it.idx < len(it.mt.sortedDel) {
			return it.mt.sortedDel[it.idx]
		}
	}
	it.ensureLoaded()
	return it.cur.flags&node.FlagTombstone != 0
}

func (it *hashIterator) Error() error {
	return nil
}

func (it *hashIterator) Close() error {
	if it.mu != nil {
		it.mu.RUnlock()
	}
	*it = hashIterator{}
	hashIteratorPool.Put(it)
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
	it.loaded = false
	it.curKey = ""
	if it.idx < 0 || it.idx >= len(it.keys) {
		return
	}
	if it.hasEnd && strings.Compare(it.keys[it.idx], it.end) >= 0 {
		return
	}
	it.curKey = it.keys[it.idx]
	it.valid = true
}

func (it *hashIterator) ensureLoaded() {
	if it.loaded {
		return
	}
	if !it.valid {
		return
	}
	idx, ok := it.mt.items[it.curKey]
	if !ok {
		it.valid = false
		return
	}
	i := int(idx)
	if i < 0 || i >= len(it.mt.entries) {
		it.valid = false
		return
	}
	ent := it.mt.entries[i]
	it.cur = ent
	it.loaded = true
}
