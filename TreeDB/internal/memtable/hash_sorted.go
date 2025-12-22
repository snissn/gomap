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
	hasDeletes  bool
	arena       hashArena

	// Incremental ordering:
	// We record first-seen keys into pendingKeys and seal them into chunk slices.
	// A global background worker sorts those chunks and merges them into an
	// LSM-like leveled structure (index.levels).
	pendingKeys  []string
	pendingBytes int
	nextSeq      uint64
	index        hashSortedIndex

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

	// compacted caches a single globally sorted key slice for a fully indexed
	// frozen memtable. It is built lazily on the first iterator/flush that needs
	// ordered traversal and then reused.
	compactedTo    uint64
	compactedRuns  [][]string
	compactedOrder []uint32
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
	m.hasDeletes = true

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
	small := len(m.items) <= 4096
	m.mu.RUnlock()

	if frozen && !small {
		if finalizeDone != nil {
			<-finalizeDone
		}
		runs, order, endKey, hasEnd := m.ensureIndexFrozen(end)
		if order == nil {
			var keys []string
			if len(runs) > 0 {
				keys = runs[0]
			}
			it := &hashIterator{
				mt:     m,
				keys:   keys,
				end:    endKey,
				hasEnd: hasEnd,
			}
			it.Seek(start)
			return it
		}
		it := &hashIndexIterator{
			mt:     m,
			runs:   runs,
			order:  order,
			end:    endKey,
			hasEnd: hasEnd,
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

func (m *HashSorted) ensureIndexFrozen(end []byte) (runs [][]string, order []uint32, endKey string, hasEnd bool) {
	m.mu.Lock()
	target := m.nextSeq
	pending := m.pendingKeys
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

	runs, order = m.index.compact(target)
	if end != nil {
		endKey = bytesToStringNoCopy(end)
		hasEnd = true
	}
	return runs, order, endKey, hasEnd
}

func (m *HashSorted) startFinalize() {
	m.finalizeOnce.Do(func() {
		done := make(chan struct{})
		m.mu.Lock()
		m.finalizeDone = done
		m.mu.Unlock()
		go func() {
			m.ensureIndexFrozen(nil)
			close(done)
		}()
	})
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
	m.hasDeletes = true
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
	idx.runs = nil
	idx.doneTo = 0
	idx.compactedTo = 0
	idx.compactedRuns = nil
	idx.compactedOrder = nil
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
	if seq > idx.compactedTo {
		// Invalidate any cached compaction when new runs arrive.
		idx.compactedTo = 0
		idx.compactedRuns = nil
		idx.compactedOrder = nil
	}
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

func (idx *hashSortedIndex) compact(target uint64) (runs [][]string, order []uint32) {
	if target == 0 {
		return nil, nil
	}

	idx.mu.Lock()
	if idx.compactedTo >= target {
		runs = idx.compactedRuns
		order = idx.compactedOrder
		idx.mu.Unlock()
		return runs, order
	}
	if int(target) > len(idx.runs) {
		target = uint64(len(idx.runs))
	}

	runs = make([][]string, 0, target)
	total := 0
	fallback := false
	for i := uint64(0); i < target; i++ {
		run := idx.runs[i]
		if len(run) == 0 {
			continue
		}
		if len(run) > 0xffff {
			fallback = true
		}
		runs = append(runs, run)
		total += len(run)
	}
	idx.mu.Unlock()

	if len(runs) == 0 {
		idx.mu.Lock()
		if idx.compactedTo < target {
			idx.compactedTo = target
			idx.compactedRuns = nil
			idx.compactedOrder = nil
		}
		runs = idx.compactedRuns
		order = idx.compactedOrder
		idx.mu.Unlock()
		return runs, order
	}

	if len(runs) > 0xffff {
		fallback = true
	}

	if len(runs) == 1 {
		if len(runs[0]) > 0xffff {
			fallback = true
		}
	}

	if fallback {
		merged := mergeSortedStringRuns(runs, total)
		runs = [][]string{merged}
		order = nil
		idx.mu.Lock()
		if idx.compactedTo < target {
			idx.compactedTo = target
			idx.compactedRuns = runs
			idx.compactedOrder = nil
		}
		runs = idx.compactedRuns
		order = idx.compactedOrder
		idx.mu.Unlock()
		return runs, order
	}

	if len(runs) == 1 {
		order = make([]uint32, len(runs[0]))
		for i := range order {
			order[i] = uint32(i)
		}
		idx.mu.Lock()
		if idx.compactedTo < target {
			idx.compactedTo = target
			idx.compactedRuns = runs
			idx.compactedOrder = order
		}
		runs = idx.compactedRuns
		order = idx.compactedOrder
		idx.mu.Unlock()
		return runs, order
	}

	order = mergeSortedRunOrder(runs, total)

	idx.mu.Lock()
	if idx.compactedTo < target {
		idx.compactedTo = target
		idx.compactedRuns = runs
		idx.compactedOrder = order
	}
	runs = idx.compactedRuns
	order = idx.compactedOrder
	idx.mu.Unlock()
	return runs, order
}

type hashRunMergeItem struct {
	key string
	run int
	pos int
}

func mergeSortedRunOrder(runs [][]string, total int) []uint32 {
	h := make([]hashRunMergeItem, 0, len(runs))
	for runIdx, run := range runs {
		if len(run) == 0 {
			continue
		}
		h = heapPushMerge(h, hashRunMergeItem{key: run[0], run: runIdx, pos: 0})
	}

	out := make([]uint32, 0, total)
	for len(h) > 0 {
		var item hashRunMergeItem
		item, h = heapPopMerge(h)
		out = append(out, (uint32(item.run)<<16)|uint32(item.pos))

		run := runs[item.run]
		nextPos := item.pos + 1
		if nextPos < len(run) {
			h = heapPushMerge(h, hashRunMergeItem{key: run[nextPos], run: item.run, pos: nextPos})
		}
	}
	return out
}

func mergeSortedStringRuns(runs [][]string, total int) []string {
	h := make([]hashRunMergeItem, 0, len(runs))
	for runIdx, run := range runs {
		if len(run) == 0 {
			continue
		}
		h = heapPushMerge(h, hashRunMergeItem{key: run[0], run: runIdx, pos: 0})
	}

	out := make([]string, 0, total)
	for len(h) > 0 {
		var item hashRunMergeItem
		item, h = heapPopMerge(h)
		out = append(out, item.key)

		run := runs[item.run]
		nextPos := item.pos + 1
		if nextPos < len(run) {
			h = heapPushMerge(h, hashRunMergeItem{key: run[nextPos], run: item.run, pos: nextPos})
		}
	}
	return out
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
}

type hashIndexIterator struct {
	mt     *HashSorted
	runs   [][]string
	order  []uint32
	idx    int
	end    string
	hasEnd bool
	curRef uint32
	curKey string
	cur    hashEntry
	loaded bool
	valid  bool
}

func (it *hashIterator) Seek(key []byte) {
	if it.keys == nil {
		it.valid = false
		it.loaded = false
		return
	}
	seekKey := bytesToStringNoCopy(key)
	it.idx = sort.Search(len(it.keys), func(i int) bool {
		return it.keys[i] >= seekKey
	})
	it.refresh()
}

func (it *hashIndexIterator) Seek(key []byte) {
	if it.order == nil {
		it.valid = false
		it.loaded = false
		return
	}
	if len(key) == 0 {
		it.idx = 0
		it.refresh()
		return
	}
	seekKey := bytesToStringNoCopy(key)
	it.idx = sort.Search(len(it.order), func(i int) bool {
		return it.keyAt(i) >= seekKey
	})
	it.refresh()
}

func (it *hashIndexIterator) Next() {
	it.idx++
	it.refresh()
}

func (it *hashIndexIterator) Valid() bool {
	if !it.valid {
		return false
	}
	return true
}

func (it *hashIndexIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	if it.loaded {
		return it.cur.key
	}
	return stringToBytesNoCopy(it.curKeyString())
}

func (it *hashIndexIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	it.ensureLoaded()
	return it.cur.value
}

func (it *hashIndexIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	it.ensureLoaded()
	if it.cur.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	return it.cur.value, page.ValuePtr{}, node.FlagInline
}

func (it *hashIndexIterator) Key() []byte {
	return it.UnsafeKey()
}

func (it *hashIndexIterator) Value() []byte {
	return it.UnsafeValue()
}

func (it *hashIndexIterator) KeyCopy(dst []byte) []byte {
	k := it.UnsafeKey()
	if k == nil {
		return nil
	}
	return append(dst[:0], k...)
}

func (it *hashIndexIterator) ValueCopy(dst []byte) []byte {
	v := it.UnsafeValue()
	if v == nil {
		return nil
	}
	return append(dst[:0], v...)
}

func (it *hashIndexIterator) IsDeleted() bool {
	if !it.valid {
		return false
	}
	if !it.mt.hasDeletes {
		return false
	}
	it.ensureLoaded()
	return it.cur.deleted
}

func (it *hashIndexIterator) Error() error {
	return nil
}

func (it *hashIndexIterator) Close() error {
	return nil
}

func (it *hashIndexIterator) Domain() (start, end []byte) {
	if !it.hasEnd {
		return nil, nil
	}
	return nil, []byte(it.end)
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
		return it.cur.key
	}
	return stringToBytesNoCopy(it.curKey)
}

func (it *hashIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	it.ensureLoaded()
	return it.cur.value
}

func (it *hashIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	it.ensureLoaded()
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
	if !it.mt.hasDeletes {
		return false
	}
	it.ensureLoaded()
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

func (it *hashIndexIterator) keyAt(i int) string {
	ref := it.order[i]
	run := int(ref >> 16)
	pos := int(ref & 0xffff)
	return it.runs[run][pos]
}

func (it *hashIndexIterator) refresh() {
	it.valid = false
	it.loaded = false
	it.curRef = 0
	it.curKey = ""
	if it.idx < 0 || it.idx >= len(it.order) {
		return
	}
	it.curRef = it.order[it.idx]
	if it.hasEnd {
		key := it.keyAt(it.idx)
		if strings.Compare(key, it.end) >= 0 {
			return
		}
		it.curKey = key
	}
	it.valid = true
}

func (it *hashIndexIterator) curKeyString() string {
	if it.curKey != "" {
		return it.curKey
	}
	run := int(it.curRef >> 16)
	pos := int(it.curRef & 0xffff)
	it.curKey = it.runs[run][pos]
	return it.curKey
}

func (it *hashIndexIterator) ensureLoaded() {
	if it.loaded {
		return
	}
	if !it.valid {
		return
	}
	key := it.curKeyString()
	ent, ok := it.mt.items[key]
	if !ok {
		it.valid = false
		return
	}
	it.cur = ent
	it.loaded = true
}
