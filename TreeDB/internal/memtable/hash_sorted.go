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

	// Incremental ordering:
	// We record first-seen keys into pendingKeys and seal them into chunk slices.
	// A global background worker sorts those chunks and merges them into an
	// LSM-like leveled structure (index.levels).
	pendingKeys  []string
	pendingBytes int
	nextSeq      uint64
	index        hashSortedIndex
}

type hashSortedIndex struct {
	mu     sync.Mutex
	cond   *sync.Cond
	done   map[uint64]struct{}
	doneTo uint64

	// levels is a binomial run structure: each level holds 0 or 1 sorted run.
	// Inserting a new run merges/carries until an empty level is found.
	levels [][]string
}

func NewHashSorted() *HashSorted {
	m := &HashSorted{
		items:       make(map[string]hashEntry),
		sortedValid: true,
	}
	m.index.cond = sync.NewCond(&m.index.mu)
	return m
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
	target := m.nextSeq
	m.index.wait(target)

	m.mu.Lock()

	clear(m.items)
	m.sizeBytes = 0
	m.sortedKeys = m.sortedKeys[:0]
	m.sortedValid = true
	m.frozen = false
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.nextSeq = 0
	m.index.reset()
	m.arena.resetKeepFirstChunk()
	m.mu.Unlock()
}

func (m *HashSorted) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	for _, op := range entries {
		if op.Type == batchpkg.OpDelete {
			if chunk, seq := m.deleteStealLocked(op.Key); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
		} else {
			if chunk, seq := m.setStealLocked(op.Key, op.Value); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		globalHashSortedIndexer.enqueue(c.mt, c.seq, c.keys)
	}
}

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (m *HashSorted) indexApplySortedChunk(seq uint64, keys []string) {
	m.index.apply(seq, keys)
}

func (m *HashSorted) Set(key, value []byte) {
	_ = m.PutWithCallback(key, value, nil)
}

func (m *HashSorted) SetSteal(key, value []byte) {
	m.mu.Lock()
	chunk, seq := m.setStealLocked(key, value)
	m.mu.Unlock()
	if seq != 0 {
		globalHashSortedIndexer.enqueue(m, seq, chunk)
	}
}

func (m *HashSorted) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}

	var chunk []string
	var seq uint64

	m.mu.Lock()
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		valCopy := m.arena.copyBytes(value)
		if cb != nil {
			if err := cb(ent.key, valCopy); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		oldLen := len(ent.value)
		ent.value = valCopy
		ent.deleted = false
		m.sizeBytes += int64(len(valCopy) - oldLen)
		m.items[storedKey] = ent
		m.mu.Unlock()
		return nil
	}

	keyCopy := m.arena.copyBytes(key)
	valCopy := m.arena.copyBytes(value)
	if cb != nil {
		if err := cb(keyCopy, valCopy); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyStored] = hashEntry{key: keyCopy, value: valCopy}
	m.sizeBytes += int64(len(keyCopy) + len(valCopy))
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()

	if seq != 0 {
		globalHashSortedIndexer.enqueue(m, seq, chunk)
	}
	return nil
}

func (m *HashSorted) Delete(key []byte) {
	_ = m.DeleteWithCallback(key, nil)
}

func (m *HashSorted) DeleteSteal(key []byte) {
	m.mu.Lock()
	chunk, seq := m.deleteStealLocked(key)
	m.mu.Unlock()
	if seq != 0 {
		globalHashSortedIndexer.enqueue(m, seq, chunk)
	}
}

func (m *HashSorted) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	var chunk []string
	var seq uint64

	m.mu.Lock()

	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		if cb != nil {
			if err := cb(ent.key, nil); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		if !ent.deleted {
			m.sizeBytes -= int64(len(ent.value))
			ent.value = nil
			ent.deleted = true
		}
		m.items[storedKey] = ent
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
	m.items[keyStored] = hashEntry{key: keyCopy, deleted: true}
	m.sizeBytes += int64(len(keyCopy))
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()
	if seq != 0 {
		globalHashSortedIndexer.enqueue(m, seq, chunk)
	}
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
	frozen := m.frozen
	m.mu.RUnlock()
	if !sortedValid {
		// Frozen memtables are immutable; we can safely wait for background
		// indexing and build the final sorted key view without stalling writers.
		if frozen {
			m.ensureSortedFrozen()
		} else {
			// Mutable memtables should be rare in iterator paths (cached DB rotates
			// for snapshot isolation). Keep correctness simple.
			m.mu.Lock()
			m.ensureSortedLocked()
			m.mu.Unlock()
		}
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
			return keys[i] >= startKey
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

func (m *HashSorted) ensureSortedFrozen() {
	// Snapshot the number of sealed chunks, then wait for background indexing to
	// apply them before building the final view.
	m.mu.Lock()
	if m.sortedValid {
		m.mu.Unlock()
		return
	}
	target := m.nextSeq
	pending := m.pendingKeys
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.mu.Unlock()

	m.index.wait(target)

	// Apply any remaining new keys locally as a final chunk. This avoids an extra
	// round-trip through the global queue at freeze/flush time.
	if len(pending) > 0 {
		sort.Strings(pending)
		finalSeq := target + 1
		m.index.apply(finalSeq, pending)
		target = finalSeq
		m.mu.Lock()
		if m.nextSeq < target {
			m.nextSeq = target
		}
		m.mu.Unlock()
	}

	keys := m.index.mergedKeys()
	m.mu.Lock()
	m.sortedKeys = keys
	m.sortedValid = true
	m.mu.Unlock()
}

func (m *HashSorted) noteNewKeyLocked(key string) (chunk []string, seq uint64) {
	m.sortedValid = false
	m.pendingKeys = append(m.pendingKeys, key)
	m.pendingBytes += len(key)
	if m.pendingBytes < hashSortedSealBytesThreshold && len(m.pendingKeys) < hashSortedSealKeysThreshold {
		return nil, 0
	}

	chunk = m.pendingKeys
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.nextSeq++
	return chunk, m.nextSeq
}

func (m *HashSorted) setStealLocked(key, value []byte) ([]string, uint64) {
	if key == nil {
		return nil, 0
	}
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := bytesToStringNoCopy(ent.key)
		oldLen := len(ent.value)
		ent.value = value
		ent.deleted = false
		m.sizeBytes += int64(len(value) - oldLen)
		m.items[storedKey] = ent
		return nil, 0
	}
	keyStored := bytesToStringNoCopy(key)
	m.items[keyStored] = hashEntry{key: key, value: value}
	m.sizeBytes += int64(len(key) + len(value))
	return m.noteNewKeyLocked(keyStored)
}

func (m *HashSorted) deleteStealLocked(key []byte) ([]string, uint64) {
	if key == nil {
		return nil, 0
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
		return nil, 0
	}
	keyStored := bytesToStringNoCopy(key)
	m.items[keyStored] = hashEntry{key: key, deleted: true}
	m.sizeBytes += int64(len(key))
	return m.noteNewKeyLocked(keyStored)
}

func (idx *hashSortedIndex) reset() {
	idx.mu.Lock()
	idx.levels = nil
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
		idx.cond.Wait()
	}
	idx.mu.Unlock()
}

func (idx *hashSortedIndex) apply(seq uint64, keys []string) {
	if len(keys) == 0 || seq == 0 {
		return
	}
	idx.mu.Lock()
	idx.insertRunLocked(keys)
	idx.markDoneLocked(seq)
	idx.cond.Broadcast()
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

func (idx *hashSortedIndex) insertRunLocked(run []string) {
	level := 0
	for {
		if level >= len(idx.levels) {
			idx.levels = append(idx.levels, nil)
		}
		if idx.levels[level] == nil {
			idx.levels[level] = run
			return
		}
		run = mergeSortedUniqueStrings(idx.levels[level], run)
		idx.levels[level] = nil
		level++
	}
}

func (idx *hashSortedIndex) mergedKeys() []string {
	idx.mu.Lock()
	runs := make([][]string, 0, len(idx.levels))
	for _, run := range idx.levels {
		if len(run) > 0 {
			runs = append(runs, run)
		}
	}
	idx.mu.Unlock()

	return mergeManySortedRuns(runs)
}

func mergeManySortedRuns(runs [][]string) []string {
	if len(runs) == 0 {
		return nil
	}
	if len(runs) == 1 {
		return runs[0]
	}

	// Merge smaller runs first to reduce temporary memory spikes.
	sort.Slice(runs, func(i, j int) bool { return len(runs[i]) < len(runs[j]) })
	for len(runs) > 1 {
		a := runs[0]
		b := runs[1]
		merged := mergeSortedUniqueStrings(a, b)
		runs = append(runs[2:], merged)
		sort.Slice(runs, func(i, j int) bool { return len(runs[i]) < len(runs[j]) })
	}
	return runs[0]
}

func mergeSortedUniqueStrings(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		ai := a[i]
		bj := b[j]
		if ai < bj {
			out = append(out, ai)
			i++
			continue
		}
		if bj < ai {
			out = append(out, bj)
			j++
			continue
		}
		// Deduplicate equal keys defensively (should not happen if we only enqueue
		// first-seen keys).
		out = append(out, ai)
		i++
		j++
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
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
		return it.keys[i] >= seekKey
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
