package memtable

import (
	"bytes"
	"hash/maphash"
	"sort"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type hashEntry struct {
	keyRef  arenaRef
	valRef  arenaRef
	valExt  []byte
	deleted bool
}

type arenaRef struct {
	chunk uint32
	off   uint32
	len   uint32
}

type hashArena struct {
	chunks   [][]byte
	cur      []byte
	off      int
	nextCap  int
	curChunk int
}

func (a *hashArena) allocRef(n int) (arenaRef, []byte) {
	if n <= 0 {
		return arenaRef{}, nil
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
		a.curChunk = len(a.chunks) - 1
		a.off = 0
		a.nextCap = c * 2
	}
	start := a.off
	b := a.cur[start : start+n]
	a.off += n
	return arenaRef{
		chunk: uint32(a.curChunk),
		off:   uint32(start),
		len:   uint32(n),
	}, b
}

func (a *hashArena) sliceRef(ref arenaRef) []byte {
	if ref.len == 0 {
		return nil
	}
	if int(ref.chunk) >= len(a.chunks) {
		return nil
	}
	chunk := a.chunks[ref.chunk]
	start := int(ref.off)
	end := start + int(ref.len)
	if start < 0 || end < start || end > len(chunk) {
		return nil
	}
	return chunk[start:end]
}

func (a *hashArena) copyBytesRef(src []byte) (arenaRef, []byte) {
	if len(src) == 0 {
		return arenaRef{}, nil
	}
	ref, dst := a.allocRef(len(src))
	copy(dst, src)
	return ref, dst
}

func (a *hashArena) copyBytes(src []byte) []byte {
	_, dst := a.copyBytesRef(src)
	return dst
}

type HashSorted struct {
	mu          sync.RWMutex
	items       map[uint64]hashEntry
	collisions  map[string]hashEntry
	collideHash map[uint64]struct{}
	seed        maphash.Seed
	sizeBytes   int64
	sortedKeys  []arenaRef
	sortedDel   []bool
	sortedValid bool
	frozen      bool
	hasDeletes  bool
	arena       hashArena

	// Incremental ordering:
	// We record first-seen keys into pendingRefs and seal them into chunks.
	// A background worker sorts each chunk using the parallel pendingSortKeys slice
	// so it does not need to touch the arena while writes are ongoing.
	pendingRefs     []arenaRef
	pendingSortKeys []string
	pendingBytes    int
	nextSeq         uint64
	index           hashSortedIndex
	indexer         *HashSortedIndexer

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
	runs [][]arenaRef
}

func NewHashSorted() *HashSorted {
	return NewHashSortedWithIndexer(nil)
}

func NewHashSortedWithIndexer(indexer *HashSortedIndexer) *HashSorted {
	return NewHashSortedWithCapacityAndIndexer(0, indexer)
}

func NewHashSortedWithCapacityAndIndexer(capacity int, indexer *HashSortedIndexer) *HashSorted {
	if indexer == nil {
		indexer = globalHashSortedIndexer
	}
	if capacity < 0 {
		capacity = 0
	}

	mapHint := hashSortedEstimateMapHint(capacity)
	m := &HashSorted{
		items:       make(map[uint64]hashEntry, mapHint),
		seed:        maphash.MakeSeed(),
		sortedValid: true,
		indexer:     indexer,
	}
	if capHint := hashSortedEstimateSortedKeysCap(capacity, mapHint); capHint > 0 {
		m.sortedKeys = make([]arenaRef, 0, capHint)
	}
	if arenaCap := hashSortedEstimateArenaFirstChunk(capacity); arenaCap > 0 {
		m.arena.nextCap = arenaCap
	}
	m.index.cond = sync.NewCond(&m.index.mu)
	return m
}

func (m *HashSorted) keyHashBytes(key []byte) uint64 {
	return maphash.Bytes(m.seed, key)
}

func (m *HashSorted) keyHashString(key string) uint64 {
	return maphash.String(m.seed, key)
}

func (m *HashSorted) keyCountLocked() int {
	return len(m.items) + len(m.collisions)
}

func hashSortedEstimateMapHint(capacity int) int {
	const (
		avgEntryBytes = 512
		minHint       = 1024
		maxHint       = 1 << 20 // 1,048,576
	)
	if capacity <= 0 {
		return 0
	}
	hint := capacity / avgEntryBytes
	if hint < minHint {
		hint = minHint
	}
	if hint > maxHint {
		hint = maxHint
	}
	return hint
}

func hashSortedEstimateSortedKeysCap(capacity, mapHint int) int {
	// sortedKeys is populated during Freeze; preallocating here reduces slice
	// growth costs without eagerly materializing the keyset.
	if capacity <= 0 {
		return 0
	}
	return hashSortedSortedKeysInitCap
}

func hashSortedEstimateArenaFirstChunk(capacity int) int {
	if capacity <= 0 {
		return 0
	}
	// Allocate a larger first chunk than the default 64KiB to reduce chunk churn
	// when callers already have an arena size hint (e.g. FlushThreshold-derived).
	const (
		minChunk = 64 * 1024
		maxChunk = 8 << 20 // 8MiB
	)
	chunk := capacity / 8
	if chunk < minChunk {
		chunk = minChunk
	}
	if chunk > maxChunk {
		chunk = maxChunk
	}
	return chunk
}

func (a *hashArena) resetKeepFirstChunk() {
	if len(a.chunks) == 0 {
		a.cur = nil
		a.off = 0
		a.nextCap = 0
		a.curChunk = 0
		return
	}
	first := a.chunks[0]
	first = first[:cap(first)]
	a.chunks = a.chunks[:1]
	a.chunks[0] = first
	a.cur = first
	a.off = 0
	a.nextCap = cap(first) * 2
	a.curChunk = 0
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
	if m.collisions != nil {
		clear(m.collisions)
	}
	if m.collideHash != nil {
		clear(m.collideHash)
	}
	m.sizeBytes = 0
	m.sortedKeys = m.sortedKeys[:0]
	m.sortedDel = m.sortedDel[:0]
	m.sortedValid = true
	m.frozen = false
	m.hasDeletes = false
	m.pendingRefs = nil
	m.pendingSortKeys = nil
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
			if refs, keys, seq, _ := m.deleteStealLocked(op.Key); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		} else {
			if refs, keys, seq, _ := m.setStealLocked(op.Key, op.Value, false); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.refs, c.keys)
	}
}

// ApplyStealBatchIndices applies entries referenced by order (indices into entries)
// under a single lock acquisition. This avoids per-entry lock/unlock overhead in
// hot write paths that already group operations per-shard.
//
// The caller may optionally supply onKey, which is invoked with the original
// entry key in the same order as application.
func (m *HashSorted) ApplyStealBatchIndices(entries []batchpkg.Entry, order []int, onKey func(key []byte)) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	for _, idx := range order {
		if idx < 0 || idx >= len(entries) {
			continue
		}
		op := entries[idx]
		if op.Type == batchpkg.OpDelete {
			if refs, keys, seq, _ := m.deleteStealLocked(op.Key); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		} else {
			if refs, keys, seq, _ := m.setStealLocked(op.Key, op.Value, false); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.refs, c.keys)
	}
}

// ApplyStealBatchIndicesNoCopyValues applies entries referenced by order (indices
// into entries) under a single lock acquisition, while storing value slices by
// reference instead of copying them into the memtable arena.
//
// This is unsafe unless callers guarantee that entry values remain immutable for
// the lifetime of the memtable (which can exceed the batch lifetime).
func (m *HashSorted) ApplyStealBatchIndicesNoCopyValues(entries []batchpkg.Entry, order []int, onKey func(key []byte)) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	for _, idx := range order {
		if idx < 0 || idx >= len(entries) {
			continue
		}
		op := entries[idx]
		if op.Type == batchpkg.OpDelete {
			if refs, keys, seq, _ := m.deleteStealLocked(op.Key); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		} else {
			if refs, keys, seq, _ := m.setStealLocked(op.Key, op.Value, true); seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		}
		if onKey != nil {
			onKey(op.Key)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.refs, c.keys)
	}
}

// ApplyStealBatchIndicesWithStableKeyCallback applies entries referenced by order
// (indices into entries) under a single lock acquisition and invokes cb for each
// applied operation with the memtable-owned key bytes.
//
// The callback receives a stable key slice backed by the memtable arena, suitable
// for unsafe string conversions (e.g. bytesToStringNoCopy) as long as the memtable
// remains alive.
func (m *HashSorted) ApplyStealBatchIndicesWithStableKeyCallback(entries []batchpkg.Entry, order []int, cb func(stableKey []byte, op batchpkg.Entry)) {
	var chunks []hashSortedIndexWork

	m.mu.Lock()
	for _, idx := range order {
		if idx < 0 || idx >= len(entries) {
			continue
		}
		op := entries[idx]
		var stableKey []byte
		if op.Type == batchpkg.OpDelete {
			var refs []arenaRef
			var keys []string
			var seq uint64
			refs, keys, seq, stableKey = m.deleteStealLocked(op.Key)
			if seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		} else {
			var refs []arenaRef
			var keys []string
			var seq uint64
			refs, keys, seq, stableKey = m.setStealLocked(op.Key, op.Value, false)
			if seq != 0 {
				chunks = append(chunks, hashSortedIndexWork{mt: m, seq: seq, refs: refs, keys: keys})
			}
		}
		if cb != nil && stableKey != nil {
			cb(stableKey, op)
		}
	}
	m.mu.Unlock()

	for _, c := range chunks {
		c.mt.indexer.enqueue(c.mt, c.seq, c.refs, c.keys)
	}
}

func (m *HashSorted) indexApplySortedChunk(seq uint64, refs []arenaRef) {
	m.index.apply(seq, refs)
}

func (m *HashSorted) Set(key, value []byte) {
	_ = m.PutWithCallback(key, value, nil)
}

func (m *HashSorted) SetSteal(key, value []byte) {
	m.mu.Lock()
	refs, keys, seq, _ := m.setStealLocked(key, value, false)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, refs, keys)
	}
}

func (m *HashSorted) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}

	var refs []arenaRef
	var keys []string
	var seq uint64

	m.mu.Lock()
	keyHash := m.keyHashBytes(key)

	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			keyLookup := bytesToStringNoCopy(key)
			if ent, ok := m.collisions[keyLookup]; ok {
				keyBytes := m.arena.sliceRef(ent.keyRef)
				valRef, valCopy := m.arena.copyBytesRef(value)
				if cb != nil {
					if err := cb(keyBytes, valCopy); err != nil {
						m.mu.Unlock()
						return err
					}
				}
				oldLen := int(ent.valRef.len)
				if ent.valExt != nil {
					oldLen = len(ent.valExt)
					ent.valExt = nil
				}
				ent.valRef = valRef
				ent.deleted = false
				m.sizeBytes += int64(len(valCopy) - oldLen)
				storedKey := bytesToStringNoCopy(keyBytes)
				m.collisions[storedKey] = ent
				m.mu.Unlock()
				return nil
			}

			keyRef, keyCopy := m.arena.copyBytesRef(key)
			valRef, valCopy := m.arena.copyBytesRef(value)
			if cb != nil {
				if err := cb(keyCopy, valCopy); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			keyStored := bytesToStringNoCopy(keyCopy)
			if m.collisions == nil {
				m.collisions = make(map[string]hashEntry)
			}
			m.collisions[keyStored] = hashEntry{keyRef: keyRef, valRef: valRef}
			m.sizeBytes += int64(len(keyCopy) + len(valCopy))
			refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
			m.mu.Unlock()
			if seq != 0 {
				m.indexer.enqueue(m, seq, refs, keys)
			}
			return nil
		}
	}

	if ent, ok := m.items[keyHash]; ok {
		keyBytes := m.arena.sliceRef(ent.keyRef)
		if bytes.Equal(keyBytes, key) {
			valRef, valCopy := m.arena.copyBytesRef(value)
			if cb != nil {
				if err := cb(keyBytes, valCopy); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			oldLen := int(ent.valRef.len)
			if ent.valExt != nil {
				oldLen = len(ent.valExt)
				ent.valExt = nil
			}
			ent.valRef = valRef
			ent.deleted = false
			m.sizeBytes += int64(len(valCopy) - oldLen)
			m.items[keyHash] = ent
			m.mu.Unlock()
			return nil
		}

		// Hash collision: move the existing key and the new key into the collision map.
		delete(m.items, keyHash)
		if m.collideHash == nil {
			m.collideHash = make(map[uint64]struct{}, 8)
		}
		m.collideHash[keyHash] = struct{}{}
		if m.collisions == nil {
			m.collisions = make(map[string]hashEntry)
		}
		storedKey := bytesToStringNoCopy(keyBytes)
		m.collisions[storedKey] = ent

		keyRef, keyCopy := m.arena.copyBytesRef(key)
		valRef, valCopy := m.arena.copyBytesRef(value)
		if cb != nil {
			if err := cb(keyCopy, valCopy); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		keyStored := bytesToStringNoCopy(keyCopy)
		m.collisions[keyStored] = hashEntry{keyRef: keyRef, valRef: valRef}
		m.sizeBytes += int64(len(keyCopy) + len(valCopy))
		refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
		m.mu.Unlock()
		if seq != 0 {
			m.indexer.enqueue(m, seq, refs, keys)
		}
		return nil
	}

	keyRef, keyCopy := m.arena.copyBytesRef(key)
	valRef, valCopy := m.arena.copyBytesRef(value)
	if cb != nil {
		if err := cb(keyCopy, valCopy); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyHash] = hashEntry{keyRef: keyRef, valRef: valRef}
	m.sizeBytes += int64(len(keyCopy) + len(valCopy))
	refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
	m.mu.Unlock()

	if seq != 0 {
		m.indexer.enqueue(m, seq, refs, keys)
	}
	return nil
}

func (m *HashSorted) Delete(key []byte) {
	_ = m.DeleteWithCallback(key, nil)
}

func (m *HashSorted) DeleteSteal(key []byte) {
	m.mu.Lock()
	refs, keys, seq, _ := m.deleteStealLocked(key)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, refs, keys)
	}
}

func (m *HashSorted) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	if key == nil {
		return nil
	}
	var refs []arenaRef
	var keys []string
	var seq uint64

	m.mu.Lock()
	m.hasDeletes = true

	keyHash := m.keyHashBytes(key)

	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			keyLookup := bytesToStringNoCopy(key)
			if ent, ok := m.collisions[keyLookup]; ok {
				keyBytes := m.arena.sliceRef(ent.keyRef)
				if cb != nil {
					if err := cb(keyBytes, nil); err != nil {
						m.mu.Unlock()
						return err
					}
				}
				if !ent.deleted {
					valLen := int(ent.valRef.len)
					if ent.valExt != nil {
						valLen = len(ent.valExt)
						ent.valExt = nil
					}
					m.sizeBytes -= int64(valLen)
					ent.valRef = arenaRef{}
					ent.deleted = true
				}
				storedKey := bytesToStringNoCopy(keyBytes)
				m.collisions[storedKey] = ent
				m.mu.Unlock()
				return nil
			}

			keyRef, keyCopy := m.arena.copyBytesRef(key)
			if cb != nil {
				if err := cb(keyCopy, nil); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			keyStored := bytesToStringNoCopy(keyCopy)
			if m.collisions == nil {
				m.collisions = make(map[string]hashEntry)
			}
			m.collisions[keyStored] = hashEntry{keyRef: keyRef, deleted: true}
			m.sizeBytes += int64(len(keyCopy))
			refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
			m.mu.Unlock()
			if seq != 0 {
				m.indexer.enqueue(m, seq, refs, keys)
			}
			return nil
		}
	}

	if ent, ok := m.items[keyHash]; ok {
		keyBytes := m.arena.sliceRef(ent.keyRef)
		if bytes.Equal(keyBytes, key) {
			if cb != nil {
				if err := cb(keyBytes, nil); err != nil {
					m.mu.Unlock()
					return err
				}
			}
			if !ent.deleted {
				valLen := int(ent.valRef.len)
				if ent.valExt != nil {
					valLen = len(ent.valExt)
					ent.valExt = nil
				}
				m.sizeBytes -= int64(valLen)
				ent.valRef = arenaRef{}
				ent.deleted = true
			}
			m.items[keyHash] = ent
			m.mu.Unlock()
			return nil
		}

		// Hash collision: move the existing key and the new key into the collision map.
		delete(m.items, keyHash)
		if m.collideHash == nil {
			m.collideHash = make(map[uint64]struct{}, 8)
		}
		m.collideHash[keyHash] = struct{}{}
		if m.collisions == nil {
			m.collisions = make(map[string]hashEntry)
		}
		storedKey := bytesToStringNoCopy(keyBytes)
		m.collisions[storedKey] = ent

		keyRef, keyCopy := m.arena.copyBytesRef(key)
		if cb != nil {
			if err := cb(keyCopy, nil); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		keyStored := bytesToStringNoCopy(keyCopy)
		m.collisions[keyStored] = hashEntry{keyRef: keyRef, deleted: true}
		m.sizeBytes += int64(len(keyCopy))
		refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
		m.mu.Unlock()
		if seq != 0 {
			m.indexer.enqueue(m, seq, refs, keys)
		}
		return nil
	}

	keyRef, keyCopy := m.arena.copyBytesRef(key)
	if cb != nil {
		if err := cb(keyCopy, nil); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyHash] = hashEntry{keyRef: keyRef, deleted: true}
	m.sizeBytes += int64(len(keyCopy))
	refs, keys, seq = m.noteNewKeyLocked(keyRef, keyStored)
	m.mu.Unlock()
	if seq != 0 {
		m.indexer.enqueue(m, seq, refs, keys)
	}
	return nil
}

func (m *HashSorted) Get(key []byte) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keyHash := m.keyHashBytes(key)
	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			ent, ok := m.collisions[bytesToStringNoCopy(key)]
			if !ok {
				return nil, false, false
			}
			val := ent.valExt
			if val == nil {
				val = m.arena.sliceRef(ent.valRef)
			}
			return val, ent.deleted, true
		}
	}

	ent, ok := m.items[keyHash]
	if !ok {
		return nil, false, false
	}
	if !bytes.Equal(m.arena.sliceRef(ent.keyRef), key) {
		if m.collisions != nil {
			ent, ok = m.collisions[bytesToStringNoCopy(key)]
			if ok {
				val := ent.valExt
				if val == nil {
					val = m.arena.sliceRef(ent.valRef)
				}
				return val, ent.deleted, true
			}
		}
		return nil, false, false
	}
	val := ent.valExt
	if val == nil {
		val = m.arena.sliceRef(ent.valRef)
	}
	return val, ent.deleted, true
}

func (m *HashSorted) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *HashSorted) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keyCountLocked()
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

		m.mu.RLock()
		keys := m.sortedKeys
		it := hashIteratorPool.Get().(*hashIterator)
		*it = hashIterator{
			mt:     m,
			keys:   keys,
			end:    end,
			hasEnd: end != nil,
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

	m.mu.RLock()
	keys := m.sortedKeys
	it := hashIteratorPool.Get().(*hashIterator)
	*it = hashIterator{
		mt:     m,
		keys:   keys,
		end:    end,
		hasEnd: end != nil,
		mu:     &m.mu,
	}
	it.Seek(start)
	return it
}

func (m *HashSorted) ensureSortedLocked() {
	if m.sortedValid {
		return
	}
	count := m.keyCountLocked()
	if cap(m.sortedKeys) < count {
		m.sortedKeys = make([]arenaRef, 0, count)
	} else {
		m.sortedKeys = m.sortedKeys[:0]
	}
	for _, ent := range m.items {
		m.sortedKeys = append(m.sortedKeys, ent.keyRef)
	}
	if m.collisions != nil {
		for _, ent := range m.collisions {
			m.sortedKeys = append(m.sortedKeys, ent.keyRef)
		}
	}
	sort.Slice(m.sortedKeys, func(i, j int) bool {
		return bytes.Compare(m.arena.sliceRef(m.sortedKeys[i]), m.arena.sliceRef(m.sortedKeys[j])) < 0
	})
	m.sortedValid = true
}

func (m *HashSorted) ensureIndexFrozen() {
	m.mu.RLock()
	sortedValid := m.sortedValid
	hasPending := len(m.pendingRefs) > 0
	if sortedValid && !hasPending {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	m.mu.Lock()
	if m.sortedValid && len(m.pendingRefs) == 0 {
		m.mu.Unlock()
		return
	}
	target := m.nextSeq
	pendingRefs := m.pendingRefs
	pendingKeys := m.pendingSortKeys
	dst := m.sortedKeys
	m.pendingRefs = nil
	m.pendingSortKeys = nil
	m.pendingBytes = 0
	m.mu.Unlock()

	m.index.wait(target)

	// Seal any remaining keys locally as a final chunk after prior chunks have
	// completed to preserve the seq->run mapping.
	if len(pendingRefs) > 0 {
		if len(pendingKeys) == len(pendingRefs) {
			sort.Sort(hashSortedRunSorter{keys: pendingKeys, refs: pendingRefs})
		} else {
			m.mu.RLock()
			keys := make([]string, len(pendingRefs))
			for i, ref := range pendingRefs {
				keys[i] = bytesToStringNoCopy(m.arena.sliceRef(ref))
			}
			m.mu.RUnlock()
			sort.Sort(hashSortedRunSorter{keys: keys, refs: pendingRefs})
		}
		finalSeq := target + 1
		m.index.apply(finalSeq, pendingRefs)
		target = finalSeq
		m.mu.Lock()
		if m.nextSeq < target {
			m.nextSeq = target
		}
		m.mu.Unlock()
	}

	m.mu.RLock()
	runs, total := m.index.snapshotRuns(target)
	if total == 0 {
		m.mu.RUnlock()
		m.mu.Lock()
		m.sortedKeys = m.sortedKeys[:0]
		m.sortedDel = m.sortedDel[:0]
		m.sortedValid = true
		m.mu.Unlock()
		m.index.dropRuns()
		return
	}

	dst = mergeSortedRefRunsInto(m, dst, runs, total)

	var del []bool
	if m.hasDeletes {
		if cap(m.sortedDel) >= len(dst) {
			del = m.sortedDel[:len(dst)]
			clear(del)
		} else {
			del = make([]bool, len(dst))
		}
		for i, ref := range dst {
			ent, ok := m.entryByRefLocked(ref)
			if !ok {
				continue
			}
			del[i] = ent.deleted
		}
	}
	m.mu.RUnlock()

	m.mu.Lock()
	if m.keyCountLocked() != total {
		m.mu.Unlock()

		// Safety fallback: if indexing missed keys, rebuild directly from the map.
		keys := make([]arenaRef, 0, m.keyCountLocked())
		m.mu.RLock()
		for _, ent := range m.items {
			keys = append(keys, ent.keyRef)
		}
		for _, ent := range m.collisions {
			keys = append(keys, ent.keyRef)
		}
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(m.arena.sliceRef(keys[i]), m.arena.sliceRef(keys[j])) < 0
		})

		if m.hasDeletes {
			if cap(del) < len(keys) {
				del = make([]bool, len(keys))
			} else {
				del = del[:len(keys)]
				clear(del)
			}
			for i, k := range keys {
				ent, ok := m.entryByRefLocked(k)
				if ok {
					del[i] = ent.deleted
				}
			}
		} else {
			del = del[:0]
		}
		m.mu.RUnlock()

		m.mu.Lock()
		m.sortedKeys = keys
		if m.hasDeletes {
			m.sortedDel = del
		} else {
			m.sortedDel = m.sortedDel[:0]
		}
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

func (m *HashSorted) noteNewKeyLocked(ref arenaRef, sortKey string) (chunkRefs []arenaRef, chunkKeys []string, seq uint64) {
	m.sortedValid = false
	if m.pendingRefs == nil {
		m.pendingRefs = make([]arenaRef, 0, hashSortedPendingKeysInitCap)
		m.pendingSortKeys = make([]string, 0, hashSortedPendingKeysInitCap)
	}
	m.pendingRefs = append(m.pendingRefs, ref)
	m.pendingSortKeys = append(m.pendingSortKeys, sortKey)
	if len(m.pendingRefs) == hashSortedPendingKeysUpgradeThreshold && cap(m.pendingRefs) < hashSortedSealKeysThreshold {
		refsExpanded := make([]arenaRef, len(m.pendingRefs), hashSortedSealKeysThreshold)
		copy(refsExpanded, m.pendingRefs)
		m.pendingRefs = refsExpanded

		keysExpanded := make([]string, len(m.pendingSortKeys), hashSortedSealKeysThreshold)
		copy(keysExpanded, m.pendingSortKeys)
		m.pendingSortKeys = keysExpanded
	}
	m.pendingBytes += len(sortKey)
	if m.pendingBytes < hashSortedSealBytesThreshold && len(m.pendingRefs) < hashSortedSealKeysThreshold {
		return nil, nil, 0
	}

	chunkRefs = m.pendingRefs
	chunkKeys = m.pendingSortKeys
	m.pendingRefs = nil
	m.pendingSortKeys = nil
	m.pendingBytes = 0
	m.nextSeq++
	return chunkRefs, chunkKeys, m.nextSeq
}

func (m *HashSorted) setStealLocked(key, value []byte, noCopyValue bool) ([]arenaRef, []string, uint64, []byte) {
	if key == nil {
		return nil, nil, 0, nil
	}
	valueExt := value
	if noCopyValue && len(valueExt) == 0 {
		valueExt = nil
	}
	keyHash := m.keyHashBytes(key)
	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			keyLookup := bytesToStringNoCopy(key)
			if ent, ok := m.collisions[keyLookup]; ok {
				keyBytes := m.arena.sliceRef(ent.keyRef)
				oldLen := int(ent.valRef.len)
				if ent.valExt != nil {
					oldLen = len(ent.valExt)
				}
				ent.valExt = nil
				ent.valRef = arenaRef{}
				newLen := len(valueExt)
				if !noCopyValue {
					var valCopy []byte
					var valRef arenaRef
					valRef, valCopy = m.arena.copyBytesRef(value)
					ent.valRef = valRef
					newLen = len(valCopy)
				} else {
					ent.valExt = valueExt
				}
				ent.deleted = false
				m.sizeBytes += int64(newLen - oldLen)
				storedKey := bytesToStringNoCopy(keyBytes)
				m.collisions[storedKey] = ent
				return nil, nil, 0, keyBytes
			}
			keyRef, keyCopy := m.arena.copyBytesRef(key)
			valLen := len(valueExt)
			var valRef arenaRef
			var valCopy []byte
			if !noCopyValue {
				valRef, valCopy = m.arena.copyBytesRef(value)
				valLen = len(valCopy)
			}
			keyStored := bytesToStringNoCopy(keyCopy)
			if m.collisions == nil {
				m.collisions = make(map[string]hashEntry)
			}
			ent := hashEntry{keyRef: keyRef, valRef: valRef, valExt: valueExt}
			if !noCopyValue {
				ent.valExt = nil
			}
			m.collisions[keyStored] = ent
			m.sizeBytes += int64(len(keyCopy) + valLen)
			refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
			return refs, keys, seq, keyCopy
		}
	}

	if ent, ok := m.items[keyHash]; ok {
		keyBytes := m.arena.sliceRef(ent.keyRef)
		if bytes.Equal(keyBytes, key) {
			oldLen := int(ent.valRef.len)
			if ent.valExt != nil {
				oldLen = len(ent.valExt)
			}
			ent.valExt = nil
			ent.valRef = arenaRef{}
			newLen := len(valueExt)
			if !noCopyValue {
				var valCopy []byte
				var valRef arenaRef
				valRef, valCopy = m.arena.copyBytesRef(value)
				ent.valRef = valRef
				newLen = len(valCopy)
			} else {
				ent.valExt = valueExt
			}
			ent.deleted = false
			m.sizeBytes += int64(newLen - oldLen)
			m.items[keyHash] = ent
			return nil, nil, 0, keyBytes
		}

		delete(m.items, keyHash)
		if m.collideHash == nil {
			m.collideHash = make(map[uint64]struct{}, 8)
		}
		m.collideHash[keyHash] = struct{}{}
		if m.collisions == nil {
			m.collisions = make(map[string]hashEntry)
		}
		storedKey := bytesToStringNoCopy(keyBytes)
		m.collisions[storedKey] = ent

		keyRef, keyCopy := m.arena.copyBytesRef(key)
		valLen := len(valueExt)
		var valRef arenaRef
		var valCopy []byte
		if !noCopyValue {
			valRef, valCopy = m.arena.copyBytesRef(value)
			valLen = len(valCopy)
		}
		keyStored := bytesToStringNoCopy(keyCopy)
		ent := hashEntry{keyRef: keyRef, valRef: valRef, valExt: valueExt}
		if !noCopyValue {
			ent.valExt = nil
		}
		m.collisions[keyStored] = ent
		m.sizeBytes += int64(len(keyCopy) + valLen)
		refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
		return refs, keys, seq, keyCopy
	}

	keyRef, keyCopy := m.arena.copyBytesRef(key)
	valLen := len(valueExt)
	var valRef arenaRef
	var valCopy []byte
	if !noCopyValue {
		valRef, valCopy = m.arena.copyBytesRef(value)
		valLen = len(valCopy)
	}
	keyStored := bytesToStringNoCopy(keyCopy)
	ent := hashEntry{keyRef: keyRef, valRef: valRef, valExt: valueExt}
	if !noCopyValue {
		ent.valExt = nil
	}
	m.items[keyHash] = ent
	m.sizeBytes += int64(len(keyCopy) + valLen)
	refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
	return refs, keys, seq, keyCopy
}

func (m *HashSorted) deleteStealLocked(key []byte) ([]arenaRef, []string, uint64, []byte) {
	if key == nil {
		return nil, nil, 0, nil
	}
	m.hasDeletes = true
	keyHash := m.keyHashBytes(key)
	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			keyLookup := bytesToStringNoCopy(key)
			if ent, ok := m.collisions[keyLookup]; ok {
				keyBytes := m.arena.sliceRef(ent.keyRef)
				if !ent.deleted {
					valLen := int(ent.valRef.len)
					if ent.valExt != nil {
						valLen = len(ent.valExt)
						ent.valExt = nil
					}
					m.sizeBytes -= int64(valLen)
					ent.valRef = arenaRef{}
					ent.deleted = true
				}
				storedKey := bytesToStringNoCopy(keyBytes)
				m.collisions[storedKey] = ent
				return nil, nil, 0, keyBytes
			}

			keyRef, keyCopy := m.arena.copyBytesRef(key)
			keyStored := bytesToStringNoCopy(keyCopy)
			if m.collisions == nil {
				m.collisions = make(map[string]hashEntry)
			}
			m.collisions[keyStored] = hashEntry{keyRef: keyRef, deleted: true}
			m.sizeBytes += int64(len(keyCopy))
			refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
			return refs, keys, seq, keyCopy
		}
	}

	if ent, ok := m.items[keyHash]; ok {
		keyBytes := m.arena.sliceRef(ent.keyRef)
		if bytes.Equal(keyBytes, key) {
			if !ent.deleted {
				valLen := int(ent.valRef.len)
				if ent.valExt != nil {
					valLen = len(ent.valExt)
					ent.valExt = nil
				}
				m.sizeBytes -= int64(valLen)
				ent.valRef = arenaRef{}
				ent.deleted = true
			}
			m.items[keyHash] = ent
			return nil, nil, 0, keyBytes
		}

		delete(m.items, keyHash)
		if m.collideHash == nil {
			m.collideHash = make(map[uint64]struct{}, 8)
		}
		m.collideHash[keyHash] = struct{}{}
		if m.collisions == nil {
			m.collisions = make(map[string]hashEntry)
		}
		storedKey := bytesToStringNoCopy(keyBytes)
		m.collisions[storedKey] = ent

		keyRef, keyCopy := m.arena.copyBytesRef(key)
		keyStored := bytesToStringNoCopy(keyCopy)
		m.collisions[keyStored] = hashEntry{keyRef: keyRef, deleted: true}
		m.sizeBytes += int64(len(keyCopy))
		refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
		return refs, keys, seq, keyCopy
	}

	keyRef, keyCopy := m.arena.copyBytesRef(key)
	keyStored := bytesToStringNoCopy(keyCopy)
	m.items[keyHash] = hashEntry{keyRef: keyRef, deleted: true}
	m.sizeBytes += int64(len(keyCopy))
	refs, keys, seq := m.noteNewKeyLocked(keyRef, keyStored)
	return refs, keys, seq, keyCopy
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

func (idx *hashSortedIndex) apply(seq uint64, refs []arenaRef) {
	if len(refs) == 0 || seq == 0 {
		return
	}
	idx.mu.Lock()
	if int(seq) > len(idx.runs) {
		newRuns := make([][]arenaRef, seq)
		copy(newRuns, idx.runs)
		idx.runs = newRuns
	}
	idx.runs[seq-1] = refs
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

func (idx *hashSortedIndex) snapshotRuns(target uint64) (runs [][]arenaRef, total int) {
	if target == 0 {
		return nil, 0
	}

	idx.mu.Lock()
	if int(target) > len(idx.runs) {
		target = uint64(len(idx.runs))
	}
	runs = make([][]arenaRef, 0, target)
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
	ref arenaRef
	run int
	pos int
}

func heapPushMerge(mt *HashSorted, h []hashRunMergeItem, item hashRunMergeItem) []hashRunMergeItem {
	h = append(h, item)
	i := len(h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if bytes.Compare(mt.arena.sliceRef(h[p].ref), mt.arena.sliceRef(h[i].ref)) <= 0 {
			break
		}
		h[p], h[i] = h[i], h[p]
		i = p
	}
	return h
}

func heapPopMerge(mt *HashSorted, h []hashRunMergeItem) (hashRunMergeItem, []hashRunMergeItem) {
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
		if r < len(h) && bytes.Compare(mt.arena.sliceRef(h[r].ref), mt.arena.sliceRef(h[l].ref)) < 0 {
			small = r
		}
		if bytes.Compare(mt.arena.sliceRef(h[i].ref), mt.arena.sliceRef(h[small].ref)) <= 0 {
			break
		}
		h[i], h[small] = h[small], h[i]
		i = small
	}
	return out, h
}

func mergeSortedRefRunsInto(mt *HashSorted, dst []arenaRef, runs [][]arenaRef, total int) []arenaRef {
	if total <= 0 {
		return dst[:0]
	}

	if cap(dst) < total {
		dst = make([]arenaRef, total)
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
		h = heapPushMerge(mt, h, hashRunMergeItem{ref: run[0], run: runIdx, pos: 0})
	}

	out := 0
	for len(h) > 0 {
		var item hashRunMergeItem
		item, h = heapPopMerge(mt, h)
		dst[out] = item.ref
		out++

		run := runs[item.run]
		nextPos := item.pos + 1
		if nextPos < len(run) {
			h = heapPushMerge(mt, h, hashRunMergeItem{ref: run[nextPos], run: item.run, pos: nextPos})
		}
	}

	return dst[:out]
}

type hashIterator struct {
	mt     *HashSorted
	keys   []arenaRef
	idx    int
	end    []byte
	hasEnd bool
	curRef arenaRef
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
	if key == nil {
		it.idx = 0
		it.refresh()
		return
	}
	it.idx = sort.Search(len(it.keys), func(i int) bool {
		return bytes.Compare(it.mt.arena.sliceRef(it.keys[i]), key) >= 0
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
	return true
}

func (it *hashIterator) UnsafeKey() []byte {
	if !it.valid {
		return nil
	}
	return it.mt.arena.sliceRef(it.curRef)
}

func (it *hashIterator) UnsafeValue() []byte {
	if !it.valid {
		return nil
	}
	it.ensureLoaded()
	if it.cur.valExt != nil {
		return it.cur.valExt
	}
	return it.mt.arena.sliceRef(it.cur.valRef)
}

func (it *hashIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.valid {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	it.ensureLoaded()
	if it.cur.deleted {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	val := it.cur.valExt
	if val == nil {
		val = it.mt.arena.sliceRef(it.cur.valRef)
	}
	return val, page.ValuePtr{}, node.FlagInline
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
	return it.cur.deleted
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
	return nil, it.end
}

func (it *hashIterator) refresh() {
	it.valid = false
	it.loaded = false
	it.curRef = arenaRef{}
	if it.idx < 0 || it.idx >= len(it.keys) {
		return
	}
	it.curRef = it.keys[it.idx]
	if it.hasEnd && bytes.Compare(it.mt.arena.sliceRef(it.curRef), it.end) >= 0 {
		it.curRef = arenaRef{}
		return
	}
	it.valid = true
}

func (it *hashIterator) ensureLoaded() {
	if it.loaded {
		return
	}
	if !it.valid {
		return
	}
	ent, ok := it.mt.entryByRefLocked(it.curRef)
	if !ok {
		it.valid = false
		return
	}
	it.cur = ent
	it.loaded = true
}

func (m *HashSorted) entryByRefLocked(ref arenaRef) (hashEntry, bool) {
	keyBytes := m.arena.sliceRef(ref)
	keyHash := m.keyHashBytes(keyBytes)
	if m.collideHash != nil {
		if _, ok := m.collideHash[keyHash]; ok {
			if m.collisions == nil {
				return hashEntry{}, false
			}
			ent, ok := m.collisions[bytesToStringNoCopy(keyBytes)]
			return ent, ok
		}
	}
	ent, ok := m.items[keyHash]
	if ok {
		return ent, true
	}
	if m.collisions == nil {
		return hashEntry{}, false
	}
	ent, ok = m.collisions[bytesToStringNoCopy(keyBytes)]
	return ent, ok
}
