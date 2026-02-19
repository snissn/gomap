package db

import (
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/outerleaf"
	"github.com/snissn/gomap/TreeDB/page"
)

type outerLeafBlockKey struct {
	fileID uint32
	offset uint64
	length uint32
}

func newOuterLeafBlockKey(ptr page.ValuePtr) outerLeafBlockKey {
	return outerLeafBlockKey{
		fileID: ptr.FileID,
		offset: ptr.Offset,
		length: ptr.Length,
	}
}

type outerLeafBlockCacheEntry struct {
	key  outerLeafBlockKey
	ref  *outerLeafBlockRef
	prev int
	next int
}

type outerLeafBlockRef struct {
	block *outerleaf.DecodedBlock
	refs  atomic.Int32
}

var outerLeafBlockRefPool sync.Pool

func newOuterLeafBlockRef(block *outerleaf.DecodedBlock) *outerLeafBlockRef {
	if block == nil {
		return nil
	}
	var ref *outerLeafBlockRef
	if v := outerLeafBlockRefPool.Get(); v != nil {
		if pooled, ok := v.(*outerLeafBlockRef); ok {
			ref = pooled
		}
	}
	if ref == nil {
		ref = &outerLeafBlockRef{}
	}
	ref.block = block
	ref.refs.Store(1) // cache ownership
	return ref
}

func (r *outerLeafBlockRef) retain() bool {
	if r == nil {
		return false
	}
	for {
		n := r.refs.Load()
		if n <= 0 {
			return false
		}
		if r.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (r *outerLeafBlockRef) release() {
	if r == nil {
		return
	}
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.block != nil {
		block := r.block
		r.block = nil
		recycleOuterLeafDecodedBlock(block)
	}
	r.refs.Store(0)
	outerLeafBlockRefPool.Put(r)
}

func recycleOuterLeafDecodedBlock(block *outerleaf.DecodedBlock) {
	if block == nil {
		return
	}
	block.Release()
	*block = outerleaf.DecodedBlock{}
	outerLeafFenceDecodedBlockPut(block)
}

type outerLeafBlockCacheLease struct {
	ref *outerLeafBlockRef
}

func (l *outerLeafBlockCacheLease) Release() {
	if l == nil || l.ref == nil {
		return
	}
	l.ref.release()
	l.ref = nil
}

type outerLeafBlockCacheShard struct {
	mu       sync.RWMutex
	entries  map[outerLeafBlockKey]int
	nodes    []outerLeafBlockCacheEntry
	free     []int
	head     int
	tail     int
	hits     atomic.Uint64
	misses   atomic.Uint64
	capacity int
	// entryCount mirrors len(entries) for lock-free admission decisions.
	entryCount atomic.Int32
	admit      []atomic.Uint64
	// admitMask is len(admitBits)-1 for power-of-two indexing into admit.
	admitMask uint64
}

type outerLeafBlockCache struct {
	shards   []outerLeafBlockCacheShard
	capacity int
}

// On heavily contended read paths, full LRU promotion on every cache hit
// creates avoidable write-lock churn. Promote a sampled subset of hits to keep
// recency reasonably fresh while reducing lock pressure.
const outerLeafBlockCachePromoteSampleMask uint64 = 0x0f // 1/16

// Keep shard fanout bounded while aiming for modest per-shard queueing under
// mixed read/write contention.
const outerLeafBlockCacheTargetEntriesPerShard = 64
const outerLeafBlockCacheMaxShards = 64
const outerLeafBlockCacheAdmitBitsPerEntry = 16
const outerLeafBlockCacheAdmitMinBits = 256

func newOuterLeafBlockCache(capacity int) *outerLeafBlockCache {
	if capacity <= 0 {
		return nil
	}

	// Keep per-shard capacity reasonably sized while bounding lock fanout.
	shardCount := 1
	targetShards := capacity / outerLeafBlockCacheTargetEntriesPerShard
	if targetShards < 1 {
		targetShards = 1
	}
	for shardCount < targetShards && shardCount < outerLeafBlockCacheMaxShards {
		shardCount <<= 1
	}
	if shardCount > capacity {
		shardCount = 1
		for shardCount<<1 <= capacity {
			shardCount <<= 1
		}
	}

	shards := make([]outerLeafBlockCacheShard, shardCount)
	baseCap := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		capI := baseCap
		if i < extra {
			capI++
		}
		admitBits := 0
		if capI > 0 {
			targetBits := capI * outerLeafBlockCacheAdmitBitsPerEntry
			if targetBits < outerLeafBlockCacheAdmitMinBits {
				targetBits = outerLeafBlockCacheAdmitMinBits
			}
			admitBits = 1
			for admitBits < targetBits {
				admitBits <<= 1
			}
		}
		free := make([]int, capI)
		nodes := make([]outerLeafBlockCacheEntry, capI)
		for j := 0; j < capI; j++ {
			nodes[j].prev = -1
			nodes[j].next = -1
			free[j] = capI - 1 - j
		}
		shards[i] = outerLeafBlockCacheShard{
			entries:   make(map[outerLeafBlockKey]int, capI),
			nodes:     nodes,
			free:      free,
			head:      -1,
			tail:      -1,
			capacity:  capI,
			admit:     make([]atomic.Uint64, admitBits/64),
			admitMask: uint64(admitBits - 1),
		}
	}

	return &outerLeafBlockCache{
		shards:   shards,
		capacity: capacity,
	}
}

func (c *outerLeafBlockCache) get(key outerLeafBlockKey) (*outerleaf.DecodedBlock, outerLeafBlockCacheLease) {
	var lease outerLeafBlockCacheLease
	s := c.shardFor(key)
	s.mu.RLock()
	idx, ok := s.entries[key]
	if !ok {
		s.mu.RUnlock()
		s.misses.Add(1)
		return nil, lease
	}
	ref := s.nodes[idx].ref
	if ref == nil || !ref.retain() {
		s.mu.RUnlock()
		s.misses.Add(1)
		return nil, lease
	}
	block := ref.block
	needPromote := idx != s.head
	s.mu.RUnlock()
	if block == nil {
		ref.release()
		s.misses.Add(1)
		return nil, lease
	}
	hits := s.hits.Add(1)

	// Best-effort recency maintenance: avoid blocking readers on shard write
	// lock when contended. This preserves functional behavior and keeps LRU
	// ordering close under concurrency.
	if needPromote && (hits&outerLeafBlockCachePromoteSampleMask) == 0 && s.mu.TryLock() {
		if idx2, ok2 := s.entries[key]; ok2 && idx2 != s.head {
			s.moveToFront(idx2)
		}
		s.mu.Unlock()
	}
	lease.ref = ref
	return block, lease
}

func (c *outerLeafBlockCache) put(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	lease, admitted := c.putWithLease(key, block)
	if !admitted {
		recycleOuterLeafDecodedBlock(block)
	}
	lease.Release()
}

func (c *outerLeafBlockCache) putWithLease(key outerLeafBlockKey, block *outerleaf.DecodedBlock) (outerLeafBlockCacheLease, bool) {
	var lease outerLeafBlockCacheLease
	if c == nil || block == nil {
		return lease, false
	}
	s := c.shardFor(key)
	if s.capacity <= 0 {
		return lease, false
	}
	if ref := s.tryRetain(key); ref != nil {
		if ref.block != block {
			recycleOuterLeafDecodedBlock(block)
		}
		lease.ref = ref
		return lease, true
	}
	admit := s.shouldAdmit(key)
	// Recheck after admission; another goroutine may have inserted while we were
	// outside the shard lock.
	if ref := s.tryRetain(key); ref != nil {
		if ref.block != block {
			recycleOuterLeafDecodedBlock(block)
		}
		lease.ref = ref
		return lease, true
	}
	if !admit {
		return lease, false
	}

	s.mu.Lock()
	var releaseOld *outerLeafBlockRef
	var releaseEvicted *outerLeafBlockRef
	if idx, ok := s.entries[key]; ok {
		releaseOld = s.nodes[idx].ref
		if releaseOld != nil && releaseOld.block == block {
			s.nodes[idx].ref = releaseOld
			releaseOld = nil
		} else {
			s.nodes[idx].ref = newOuterLeafBlockRef(block)
		}
		s.moveToFront(idx)
		if ref := s.nodes[idx].ref; ref != nil && ref.retain() {
			lease.ref = ref
		}
		s.mu.Unlock()
		if releaseOld != nil {
			releaseOld.release()
		}
		return lease, lease.ref != nil
	}

	var idx int
	inserted := false
	if n := len(s.free); n > 0 {
		idx = s.free[n-1]
		s.free = s.free[:n-1]
		inserted = true
	} else {
		idx = s.tail
		if idx < 0 {
			s.mu.Unlock()
			return lease, false
		}
		evictedKey := s.nodes[idx].key
		releaseEvicted = s.nodes[idx].ref
		s.unlink(idx)
		delete(s.entries, evictedKey)
	}
	node := &s.nodes[idx]
	node.key = key
	if releaseEvicted != nil && releaseEvicted.block == block {
		node.ref = releaseEvicted
		releaseEvicted = nil
	} else {
		node.ref = newOuterLeafBlockRef(block)
	}
	node.prev = -1
	node.next = -1
	s.linkFront(idx)
	s.entries[key] = idx
	if inserted {
		s.entryCount.Add(1)
	}
	if ref := node.ref; ref != nil && ref.retain() {
		lease.ref = ref
	}
	s.mu.Unlock()
	if releaseEvicted != nil {
		releaseEvicted.release()
	}
	return lease, lease.ref != nil
}

func (s *outerLeafBlockCacheShard) tryRetain(key outerLeafBlockKey) *outerLeafBlockRef {
	if s == nil {
		return nil
	}
	// Fast-path existing keys without taking shard write lock.
	s.mu.RLock()
	idx, ok := s.entries[key]
	if !ok {
		s.mu.RUnlock()
		return nil
	}
	ref := s.nodes[idx].ref
	if ref == nil || !ref.retain() {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()
	return ref
}

func (c *outerLeafBlockCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
	var totalEntries int
	var totalHits uint64
	var totalMisses uint64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		totalEntries += len(s.entries)
		s.mu.RUnlock()
		totalHits += s.hits.Load()
		totalMisses += s.misses.Load()
	}
	return totalHits, totalMisses, totalEntries, c.capacity
}

func (c *outerLeafBlockCache) shardFor(key outerLeafBlockKey) *outerLeafBlockCacheShard {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	h := outerLeafBlockKeyHash(key)
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx]
}

func (s *outerLeafBlockCacheShard) shouldAdmit(key outerLeafBlockKey) bool {
	if s == nil || s.capacity <= 0 {
		return false
	}
	entries := int(s.entryCount.Load())
	if s.capacity <= 64 || entries < s.capacity/4 {
		return true
	}
	if len(s.admit) == 0 || s.admitMask == 0 {
		return true
	}
	h := outerLeafBlockKeyHash(key)
	idxA, idxB := outerLeafBlockCacheAdmitIndexes(h, s.admitMask)
	wordA := int(idxA >> 6)
	bitA := uint64(1) << (idxA & 63)
	wordB := int(idxB >> 6)
	bitB := uint64(1) << (idxB & 63)
	seenA := outerLeafBlockCacheAdmitSeenAndSet(&s.admit[wordA], bitA)
	seenB := outerLeafBlockCacheAdmitSeenAndSet(&s.admit[wordB], bitB)
	// Keep warm-up behavior close to prior tuning while reducing collision-driven
	// first-touch admits once shards are at least half occupied.
	if entries < s.capacity/2 {
		return seenA
	}
	return seenA && seenB
}

func outerLeafBlockCacheAdmitSeenAndSet(word *atomic.Uint64, bit uint64) bool {
	if word == nil || bit == 0 {
		return false
	}
	for {
		old := word.Load()
		if old&bit != 0 {
			return true
		}
		if word.CompareAndSwap(old, old|bit) {
			return false
		}
	}
}

func outerLeafBlockCacheAdmitIndexes(h, mask uint64) (uint64, uint64) {
	// Two derived bit positions preserve second-touch admission while sharply
	// lowering false positives from hash collisions under large random key sets.
	idxA := h & mask
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 29
	idxB := h & mask
	return idxA, idxB
}

func (s *outerLeafBlockCacheShard) moveToFront(idx int) {
	if idx == s.head {
		return
	}
	s.unlink(idx)
	s.linkFront(idx)
}

func (s *outerLeafBlockCacheShard) linkFront(idx int) {
	node := &s.nodes[idx]
	node.prev = -1
	node.next = s.head
	if s.head >= 0 {
		s.nodes[s.head].prev = idx
	}
	s.head = idx
	if s.tail < 0 {
		s.tail = idx
	}
}

func (s *outerLeafBlockCacheShard) unlink(idx int) {
	node := &s.nodes[idx]
	prev := node.prev
	next := node.next
	if prev >= 0 {
		s.nodes[prev].next = next
	} else {
		s.head = next
	}
	if next >= 0 {
		s.nodes[next].prev = prev
	} else {
		s.tail = prev
	}
	node.prev = -1
	node.next = -1
}
