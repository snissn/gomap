package memtable

import (
	"sort"
	"strings"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type hashEntry struct {
	key   string
	value []byte // inline bytes (pointer bytes stored separately in ptr)
	ptr   page.ValuePtr
	flags byte
}

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
	items       map[string]hashEntry
	sizeBytes   int64
	sortedKeys  []string
	sortedDel   []bool
	sortedValid bool
	frozen      bool
	hasDeletes  bool
	arena       hashArena

	// Incremental ordering:
	// We record first-seen keys into pendingKeys and seal them into chunk slices.
	// A global background worker sorts those chunks and stores them as sorted runs.
	pendingKeys  []string
	pendingBytes int
	nextSeq      uint64
	index        hashSortedIndex
	indexer      *HashSortedIndexer

	finalizeOnce sync.Once
	finalizeDone chan struct{}
}

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
	return NewHashSortedWithIndexer(nil)
}

func NewHashSortedWithIndexer(indexer *HashSortedIndexer) *HashSorted {
	if indexer == nil {
		indexer = globalHashSortedIndexer
	}
	m := &HashSorted{
		items:       make(map[string]hashEntry),
		sortedValid: true,
		indexer:     indexer,
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
	m.sizeBytes = 0
	m.sortedKeys = m.sortedKeys[:0]
	m.sortedDel = m.sortedDel[:0]
	m.sortedValid = true
	m.frozen = false
	m.hasDeletes = false
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.nextSeq = 0
	m.index.reset()
	m.finalizeOnce = sync.Once{}
	m.finalizeDone = nil
	m.arena.resetKeepFirstChunk()
	m.mu.Unlock()
}

func (m *HashSorted) ApplyStealSortedBatch(entries []batchpkg.Entry, onKey func(key []byte)) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	for _, op := range entries {
		if op.Type == batchpkg.OpDelete {
			if chunk, seq := m.setEntryLocked(op.Key, nil, page.ValuePtr{}, node.FlagTombstone, true); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
		} else if op.IsPtr {
			if chunk, seq := m.setEntryLocked(op.Key, nil, op.ValuePtr, node.FlagPointer, true); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
		} else {
			if chunk, seq := m.setEntryLocked(op.Key, op.Value, page.ValuePtr{}, node.FlagInline, true); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, keys: chunk})
			}
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.keys)
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
	m.mu.Lock()
	chunk, seq := m.setEntryLocked(key, value, ptr, flags, false)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk)
	}
}

func (m *HashSorted) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.mu.Lock()
	chunk, seq := m.setEntryLocked(key, value, ptr, flags, true)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk)
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
		storedKey := ent.key
		valCopy := m.encodeEntryValueLocked(value, page.ValuePtr{}, node.FlagInline, false)
		if cb != nil {
			if err := cb(stringToBytesNoCopy(ent.key), valCopy); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		oldLen := hashEntryValueSize(ent.flags, ent.value)
		ent.value = valCopy
		ent.ptr = page.ValuePtr{}
		ent.flags = node.FlagInline
		m.sizeBytes += int64(hashEntryValueSize(ent.flags, ent.value) - oldLen)
		m.items[storedKey] = ent
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
	m.items[keyStored] = hashEntry{key: keyStored, value: valCopy, flags: node.FlagInline}
	m.sizeBytes += int64(len(keyCopy) + hashEntryValueSize(node.FlagInline, valCopy))
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()

	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk)
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
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := ent.key
		if cb != nil {
			if err := cb(stringToBytesNoCopy(ent.key), nil); err != nil {
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
	m.items[keyStored] = hashEntry{key: keyStored, flags: node.FlagTombstone}
	m.sizeBytes += int64(len(keyCopy))
	chunk, seq = m.noteNewKeyLocked(keyStored)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, chunk)
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
	if ent.flags&node.FlagPointer != 0 {
		return ent.value, ent.flags&node.FlagTombstone != 0, true
	}
	return ent.value, ent.flags&node.FlagTombstone != 0, true
}

func (m *HashSorted) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ent, ok := m.items[bytesToStringNoCopy(key)]
	if !ok {
		return nil, page.ValuePtr{}, 0, false
	}
	if ent.flags&node.FlagPointer != 0 {
		return ent.value, ent.ptr, ent.flags, true
	}
	return ent.value, page.ValuePtr{}, ent.flags, true
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
	dst := m.sortedKeys
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.mu.Unlock()

	m.index.wait(target)

	// Seal any remaining keys locally as a final chunk after prior chunks have
	// completed to preserve the seq->run mapping.
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
			ent, ok := m.items[k]
			if !ok {
				continue
			}
			del[i] = ent.flags&node.FlagTombstone != 0
		}
		m.mu.RUnlock()
	}

	m.mu.Lock()
	if len(m.items) != total {
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
				ent, ok := m.items[k]
				if !ok {
					continue
				}
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

func (m *HashSorted) noteNewKeyLocked(key string) (chunk []string, seq uint64) {
	m.sortedValid = false
	if len(m.items) > cap(m.sortedKeys) {
		newCap := cap(m.sortedKeys)
		if newCap < hashSortedSortedKeysInitCap {
			newCap = hashSortedSortedKeysInitCap
		}
		for newCap < len(m.items) {
			newCap *= 2
		}
		m.sortedKeys = make([]string, 0, newCap)
	}
	if m.pendingKeys == nil {
		m.pendingKeys = make([]string, 0, hashSortedPendingKeysInitCap)
	}
	m.pendingKeys = append(m.pendingKeys, key)
	if len(m.pendingKeys) == hashSortedPendingKeysUpgradeThreshold && cap(m.pendingKeys) < hashSortedSealKeysThreshold {
		expanded := make([]string, len(m.pendingKeys), hashSortedSealKeysThreshold)
		copy(expanded, m.pendingKeys)
		m.pendingKeys = expanded
	}
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

func (m *HashSorted) setEntryLocked(key, value []byte, ptr page.ValuePtr, flags byte, steal bool) ([]string, uint64) {
	if key == nil {
		return nil, 0
	}
	keyLookup := bytesToStringNoCopy(key)
	if ent, ok := m.items[keyLookup]; ok {
		storedKey := ent.key
		oldLen := hashEntryValueSize(ent.flags, ent.value)
		ent.value = m.encodeEntryValueLocked(value, ptr, flags, steal)
		if flags&node.FlagPointer != 0 {
			ent.ptr = ptr
		} else {
			ent.ptr = page.ValuePtr{}
		}
		ent.flags = flags
		m.sizeBytes += int64(hashEntryValueSize(ent.flags, ent.value) - oldLen)
		if flags&node.FlagTombstone != 0 {
			m.hasDeletes = true
		}
		m.items[storedKey] = ent
		return nil, 0
	}

	var keyCopy []byte
	if steal {
		keyCopy = key
	} else {
		keyCopy = m.arena.copyBytes(key)
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	valCopy := m.encodeEntryValueLocked(value, ptr, flags, steal)
	ent := hashEntry{key: keyStored, value: valCopy, flags: flags}
	if flags&node.FlagPointer != 0 {
		ent.ptr = ptr
	}
	m.items[keyStored] = ent
	m.sizeBytes += int64(len(keyCopy) + hashEntryValueSize(flags, valCopy))
	if flags&node.FlagTombstone != 0 {
		m.hasDeletes = true
	}
	return m.noteNewKeyLocked(keyStored)
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

func (m *HashSorted) setStealLocked(key, value []byte) ([]string, uint64) {
	return m.setEntryLocked(key, value, page.ValuePtr{}, node.FlagInline, true)
}

func (m *HashSorted) deleteStealLocked(key []byte) ([]string, uint64) {
	return m.setEntryLocked(key, nil, page.ValuePtr{}, node.FlagTombstone, true)
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
	if it.loaded {
		return stringToBytesNoCopy(it.cur.key)
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
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	it.ensureLoaded()
	if it.cur.flags&node.FlagTombstone != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if it.cur.flags&node.FlagPointer != 0 {
		return it.cur.value, it.cur.ptr, it.cur.flags
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
	if !it.valid || it.curKey == "" {
		return
	}
	ent, ok := it.mt.items[it.curKey]
	if !ok {
		it.valid = false
		return
	}
	it.cur = ent
	it.loaded = true
}
