package db

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/leafblock"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafBlockKey struct {
	fileID uint32
	offset uint64
	length uint32
}

func newLeafBlockKey(ptr page.ValuePtr) leafBlockKey {
	return leafBlockKey{
		fileID: ptr.FileID,
		offset: ptr.Offset,
		length: ptr.Length,
	}
}

type leafBlockCacheEntry struct {
	key     leafBlockKey
	ref     *leafBlockRef
	prev    int
	next    int
	segment uint8
}

type leafBlockRef struct {
	block *leafblock.DecodedBlock
	refs  atomic.Int32
}

var leafBlockRefPool sync.Pool

func getLeafBlockRefSlot() *leafBlockRef {
	var ref *leafBlockRef
	if v := leafBlockRefPool.Get(); v != nil {
		if pooled, ok := v.(*leafBlockRef); ok {
			ref = pooled
		}
	}
	if ref == nil {
		ref = &leafBlockRef{}
	}
	return ref
}

func initLeafBlockRefSlot(ref *leafBlockRef, block *leafblock.DecodedBlock) *leafBlockRef {
	if block == nil {
		return nil
	}
	if ref == nil {
		ref = getLeafBlockRefSlot()
	}
	ref.block = block
	ref.refs.Store(1) // cache ownership
	return ref
}

func newLeafBlockRef(block *leafblock.DecodedBlock) *leafBlockRef {
	return initLeafBlockRefSlot(nil, block)
}

func putLeafBlockRefSlot(ref *leafBlockRef) {
	if ref == nil {
		return
	}
	ref.block = nil
	ref.refs.Store(0)
	leafBlockRefPool.Put(ref)
}

func (r *leafBlockRef) retain() bool {
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

func (r *leafBlockRef) release() {
	if r == nil {
		return
	}
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.block != nil {
		block := r.block
		r.block = nil
		recycleLeafBlockDecodedBlock(block)
	}
	putLeafBlockRefSlot(r)
}

func recycleLeafBlockDecodedBlock(block *leafblock.DecodedBlock) {
	if block == nil {
		return
	}
	block.Release()
	*block = leafblock.DecodedBlock{}
	leafBlockDecodedBlockPool.Put(block)
}

var leafBlockDecodedBlockPool = sync.Pool{
	New: func() any {
		return &leafblock.DecodedBlock{}
	},
}

type leafBlockCacheLease struct {
	ref *leafBlockRef
}

func (l *leafBlockCacheLease) Release() {
	if l == nil || l.ref == nil {
		return
	}
	l.ref.release()
	l.ref = nil
}

type leafBlockCacheShard struct {
	mu       sync.RWMutex
	entries  map[leafBlockKey]int
	nodes    []leafBlockCacheEntry
	free     []int
	head     int
	tail     int
	hits     atomic.Uint64
	misses   atomic.Uint64
	capacity int
	// entryCount mirrors len(entries) for lock-free admission decisions.
	entryCount atomic.Int32
	// admit is a TinyLFU-style frequency sketch: packed 4-bit counters.
	admit []atomic.Uint64
	// admitMask is counterCount-1 for power-of-two indexing into admit.
	admitMask uint64
	// admitSamples tracks touches since last decay.
	admitSamples atomic.Uint32
	// admitDecayEvery controls decay cadence in sketch touch operations.
	admitDecayEvery uint32
	// admitDecaying serializes sketch decay.
	admitDecaying atomic.Uint32
	// admitFloor tracks recent victim frequency (EWMA) for admission thresholding.
	admitFloor atomic.Uint32
	// SIEVE state: per-entry reference bits and a best-effort scan hand.
	sieveRef  []atomic.Uint32
	sieveHand int
	// SLRU state: probation and protected segment lists.
	slruProbHead  int
	slruProbTail  int
	slruProtHead  int
	slruProtTail  int
	slruProtCount int
	slruProtCap   int
}

type leafBlockCache struct {
	shards   []leafBlockCacheShard
	capacity int
	policy   leafBlockCachePolicy
	// Temporary counters for validating put-path contention/allocation work.
	putAttempts       atomic.Uint64
	putAdmitted       atomic.Uint64
	putDuplicateDrops atomic.Uint64
	putLockContention atomic.Uint64
}

type leafBlockCachePolicy uint8

const (
	leafBlockCachePolicyLRU leafBlockCachePolicy = iota
	leafBlockCachePolicyLRULight
	leafBlockCachePolicySIEVE
	leafBlockCachePolicySLRU
)

const leafBlockCachePolicyEnv = "TREEDB_LEAF_BLOCK_CACHE_POLICY"

var leafBlockCachePolicyOnce sync.Once
var leafBlockCachePolicyCached leafBlockCachePolicy

// On heavily contended read paths, full LRU promotion on every cache hit
// creates avoidable write-lock churn. Promote a sampled subset of hits to keep
// recency reasonably fresh while reducing lock pressure.
const leafBlockCachePromoteSampleMask uint64 = 0x0f              // 1/16
const leafBlockCachePromoteSampleMaskLight uint64 = 0x3f         // 1/64
const leafBlockCacheSLRUProbationPromoteSampleMask uint64 = 0x03 // 1/4

// Keep shard fanout bounded while aiming for modest per-shard queueing under
// mixed read/write contention.
const leafBlockCacheTargetEntriesPerShard = 64
const leafBlockCacheMaxShards = 64
const leafBlockCacheAdmitCountersPerEntry = 16
const leafBlockCacheAdmitMinCounters = 256
const leafBlockCacheAdmitCountersPerWord = 16
const leafBlockCacheAdmitCounterMask uint64 = 0x0f
const leafBlockCacheAdmitDecayNibbleHalfMask uint64 = 0x7777777777777777
const leafBlockCacheAdmitCounterMax uint8 = 15
const leafBlockCacheAdmitDecayMultiplier = 8
const leafBlockCacheAdmitDecayMinSamples = 256
const leafBlockCacheAdmitThresholdMin uint8 = 2
const leafBlockCacheAdmitThresholdMax uint8 = 4
const leafBlockCacheAdmitVictimEWMAWeight = 7 // 7/8 old + 1/8 new
const leafBlockCacheSLRUProtectedFractionNum = 1
const leafBlockCacheSLRUProtectedFractionDen = 2
const leafBlockCacheSegmentProbation uint8 = 0
const leafBlockCacheSegmentProtected uint8 = 1

func newLeafBlockCache(capacity int) *leafBlockCache {
	return newLeafBlockCacheWithPolicy(capacity, leafBlockCacheDefaultPolicy())
}

func newLeafBlockCacheWithPolicy(capacity int, policy leafBlockCachePolicy) *leafBlockCache {
	if capacity <= 0 {
		return nil
	}

	// Keep per-shard capacity reasonably sized while bounding lock fanout.
	shardCount := 1
	targetShards := capacity / leafBlockCacheTargetEntriesPerShard
	if targetShards < 1 {
		targetShards = 1
	}
	for shardCount < targetShards && shardCount < leafBlockCacheMaxShards {
		shardCount <<= 1
	}
	if shardCount > capacity {
		shardCount = 1
		for shardCount<<1 <= capacity {
			shardCount <<= 1
		}
	}

	shards := make([]leafBlockCacheShard, shardCount)
	baseCap := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		capI := baseCap
		if i < extra {
			capI++
		}
		admitCounters := 0
		admitDecayEvery := uint32(0)
		if capI > 0 {
			targetCounters := capI * leafBlockCacheAdmitCountersPerEntry
			if targetCounters < leafBlockCacheAdmitMinCounters {
				targetCounters = leafBlockCacheAdmitMinCounters
			}
			admitCounters = 1
			for admitCounters < targetCounters {
				admitCounters <<= 1
			}
			admitDecayEvery = uint32(admitCounters * leafBlockCacheAdmitDecayMultiplier)
			if admitDecayEvery < leafBlockCacheAdmitDecayMinSamples {
				admitDecayEvery = leafBlockCacheAdmitDecayMinSamples
			}
		}
		admitWords := 0
		admitMask := uint64(0)
		if admitCounters > 0 {
			admitWords = (admitCounters + leafBlockCacheAdmitCountersPerWord - 1) / leafBlockCacheAdmitCountersPerWord
			admitMask = uint64(admitCounters - 1)
		}
		free := make([]int, capI)
		nodes := make([]leafBlockCacheEntry, capI)
		for j := 0; j < capI; j++ {
			nodes[j].prev = -1
			nodes[j].next = -1
			free[j] = capI - 1 - j
		}
		shards[i] = leafBlockCacheShard{
			entries:         make(map[leafBlockKey]int, capI),
			nodes:           nodes,
			free:            free,
			head:            -1,
			tail:            -1,
			capacity:        capI,
			admit:           make([]atomic.Uint64, admitWords),
			admitMask:       admitMask,
			admitDecayEvery: admitDecayEvery,
			sieveHand:       -1,
			slruProbHead:    -1,
			slruProbTail:    -1,
			slruProtHead:    -1,
			slruProtTail:    -1,
			slruProtCap:     0,
		}
		if policy == leafBlockCachePolicySIEVE {
			shards[i].sieveRef = make([]atomic.Uint32, capI)
		}
		if policy == leafBlockCachePolicySLRU {
			protCap := (capI * leafBlockCacheSLRUProtectedFractionNum) / leafBlockCacheSLRUProtectedFractionDen
			if capI <= 1 {
				protCap = capI
			} else {
				if protCap < 1 {
					protCap = 1
				}
				if protCap >= capI {
					protCap = capI - 1
				}
			}
			shards[i].slruProtCap = protCap
		}
	}

	return &leafBlockCache{
		shards:   shards,
		capacity: capacity,
		policy:   policy,
	}
}

func leafBlockCacheDefaultPolicy() leafBlockCachePolicy {
	leafBlockCachePolicyOnce.Do(func() {
		switch strings.ToLower(strings.TrimSpace(os.Getenv(leafBlockCachePolicyEnv))) {
		case "lru_light", "lru-light", "lrulight":
			leafBlockCachePolicyCached = leafBlockCachePolicyLRULight
		case "sieve":
			leafBlockCachePolicyCached = leafBlockCachePolicySIEVE
		case "slru", "2q":
			leafBlockCachePolicyCached = leafBlockCachePolicySLRU
		default:
			leafBlockCachePolicyCached = leafBlockCachePolicyLRU
		}
	})
	return leafBlockCachePolicyCached
}

func (c *leafBlockCache) policyName() string {
	if c == nil {
		return "disabled"
	}
	if c.policy == leafBlockCachePolicyLRULight {
		return "lru_light"
	}
	if c.policy == leafBlockCachePolicySIEVE {
		return "sieve"
	}
	if c.policy == leafBlockCachePolicySLRU {
		return "slru"
	}
	return "lru"
}

func (c *leafBlockCache) isSLRU() bool {
	return c != nil && c.policy == leafBlockCachePolicySLRU
}

func (c *leafBlockCache) isSIEVE() bool {
	return c != nil && c.policy == leafBlockCachePolicySIEVE
}

func (c *leafBlockCache) lruPromoteMask() uint64 {
	if c != nil && c.policy == leafBlockCachePolicyLRULight {
		return leafBlockCachePromoteSampleMaskLight
	}
	return leafBlockCachePromoteSampleMask
}

func (c *leafBlockCache) get(key leafBlockKey) (*leafblock.DecodedBlock, leafBlockCacheLease) {
	var lease leafBlockCacheLease
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
	if c.isSIEVE() {
		s.sieveMark(idx)
	}
	block := ref.block
	segment := s.nodes[idx].segment
	needPromoteLRU := c.policy == leafBlockCachePolicyLRU && idx != s.head
	needPromoteLRULight := c.policy == leafBlockCachePolicyLRULight && idx != s.head
	s.mu.RUnlock()
	if block == nil {
		ref.release()
		s.misses.Add(1)
		return nil, lease
	}
	hits := s.hits.Add(1)
	if c.isSLRU() {
		// Probation hits should promote into protected, but sample promotions so
		// the read path does not attempt a write lock on every hit.
		shouldPromoteProbation := segment == leafBlockCacheSegmentProbation && (hits&leafBlockCacheSLRUProbationPromoteSampleMask) == 0
		shouldRefreshProtected := segment == leafBlockCacheSegmentProtected && (hits&leafBlockCachePromoteSampleMask) == 0
		if (shouldPromoteProbation || shouldRefreshProtected) && s.mu.TryLock() {
			if idx2, ok2 := s.entries[key]; ok2 {
				if s.nodes[idx2].segment == leafBlockCacheSegmentProbation {
					if shouldPromoteProbation {
						s.slruPromoteToProtected(idx2)
					}
				} else if shouldRefreshProtected {
					s.slruMoveToFront(idx2)
				}
			}
			s.mu.Unlock()
		}
	} else if (needPromoteLRU || needPromoteLRULight) && (hits&c.lruPromoteMask()) == 0 && s.mu.TryLock() {
		// Best-effort recency maintenance: avoid blocking readers on shard write
		// lock when contended. This keeps LRU ordering close under concurrency.
		if idx2, ok2 := s.entries[key]; ok2 && idx2 != s.head {
			s.moveToFront(idx2)
		}
		s.mu.Unlock()
	}
	lease.ref = ref
	return block, lease
}

func (c *leafBlockCache) put(key leafBlockKey, block *leafblock.DecodedBlock) {
	_, admitted := c.putInternal(key, block, false)
	if !admitted {
		recycleLeafBlockDecodedBlock(block)
	}
}

// putAfterMissNoLease is a miss-aware insert path for decode callers that
// already observed a cache miss and do not require a lease. It intentionally
// avoids a pre-lock read probe and can drop admission under contention once the
// shard is warm to reduce write-lock amplification on parallel read misses.
func (c *leafBlockCache) putAfterMissNoLease(key leafBlockKey, block *leafblock.DecodedBlock) {
	if !c.putAfterMissNoLeaseInternal(key, block) {
		recycleLeafBlockDecodedBlock(block)
	}
}

func (c *leafBlockCache) putAfterMissNoLeaseInternal(key leafBlockKey, block *leafblock.DecodedBlock) bool {
	if c == nil || block == nil {
		return false
	}
	c.putAttempts.Add(1)
	s, keyHash := c.shardForAndHash(key)
	if s.capacity <= 0 {
		return false
	}
	if !s.shouldAdmitHash(keyHash) {
		return false
	}

	stagedRef := getLeafBlockRefSlot()
	warmOrFull := int(s.entryCount.Load()) >= s.capacity/2
	if !s.mu.TryLock() {
		c.putLockContention.Add(1)
		if warmOrFull {
			putLeafBlockRefSlot(stagedRef)
			return false
		}
		s.mu.Lock()
	}

	var releaseEvicted *leafBlockRef
	var duplicateDropBlock *leafblock.DecodedBlock
	var evictedHash uint64
	evicted := false
	if idx, ok := s.entries[key]; ok {
		ref := s.nodes[idx].ref
		if ref == nil {
			ref = initLeafBlockRefSlot(stagedRef, block)
			stagedRef = nil
			s.nodes[idx].ref = ref
		} else if ref.block != block {
			duplicateDropBlock = block
			c.putDuplicateDrops.Add(1)
		}
		if c.isSIEVE() {
			s.sieveMark(idx)
		}
		// Keep duplicate/no-lease updates lock-minimal; read hits already perform
		// sampled recency maintenance.
		s.mu.Unlock()
		if stagedRef != nil {
			putLeafBlockRefSlot(stagedRef)
		}
		if duplicateDropBlock != nil {
			recycleLeafBlockDecodedBlock(duplicateDropBlock)
		}
		c.putAdmitted.Add(1)
		return true
	}

	var idx int
	inserted := false
	if n := len(s.free); n > 0 {
		idx = s.free[n-1]
		s.free = s.free[:n-1]
		inserted = true
	} else {
		if c.isSLRU() {
			idx = s.slruPickVictim()
		} else if c.isSIEVE() {
			idx = s.sievePickVictim()
		} else {
			idx = s.tail
		}
		if idx < 0 {
			s.mu.Unlock()
			if stagedRef != nil {
				putLeafBlockRefSlot(stagedRef)
			}
			return false
		}
		evictedKey := s.nodes[idx].key
		evictedHash = leafBlockKeyHash(evictedKey)
		evicted = true
		releaseEvicted = s.nodes[idx].ref
		if c.isSLRU() {
			s.slruRemove(idx)
		} else if c.isSIEVE() {
			s.sieveRemove(idx)
		} else {
			s.unlink(idx)
		}
		delete(s.entries, evictedKey)
	}

	node := &s.nodes[idx]
	node.key = key
	if releaseEvicted != nil && releaseEvicted.block == block {
		node.ref = releaseEvicted
		releaseEvicted = nil
	} else {
		node.ref = initLeafBlockRefSlot(stagedRef, block)
		stagedRef = nil
	}
	node.prev = -1
	node.next = -1
	if c.isSLRU() {
		s.slruInsertProbation(idx)
	} else if c.isSIEVE() {
		s.linkFront(idx)
		s.sieveInsert(idx)
	} else {
		s.linkFront(idx)
	}
	s.entries[key] = idx
	if inserted {
		s.entryCount.Add(1)
	}
	s.mu.Unlock()

	if stagedRef != nil {
		putLeafBlockRefSlot(stagedRef)
	}
	if releaseEvicted != nil {
		releaseEvicted.release()
	}
	if evicted {
		s.admitObserveVictimHash(evictedHash)
	}
	c.putAdmitted.Add(1)
	return true
}

func (c *leafBlockCache) putWithLease(key leafBlockKey, block *leafblock.DecodedBlock) (leafBlockCacheLease, bool) {
	return c.putInternal(key, block, true)
}

func (c *leafBlockCache) putInternal(key leafBlockKey, block *leafblock.DecodedBlock, wantLease bool) (leafBlockCacheLease, bool) {
	var lease leafBlockCacheLease
	if c == nil || block == nil {
		return lease, false
	}
	c.putAttempts.Add(1)
	s, keyHash := c.shardForAndHash(key)
	if s.capacity <= 0 {
		return lease, false
	}
	if !wantLease {
		if hit, same := s.hasCachedRefMaybeMark(key, block, c.isSIEVE()); hit {
			if !same {
				recycleLeafBlockDecodedBlock(block)
				c.putDuplicateDrops.Add(1)
			}
			c.putAdmitted.Add(1)
			return lease, true
		}
	} else {
		if ref := s.tryRetainMaybeMark(key, c.isSIEVE()); ref != nil {
			if ref.block != block {
				recycleLeafBlockDecodedBlock(block)
				c.putDuplicateDrops.Add(1)
			}
			lease.ref = ref
			c.putAdmitted.Add(1)
			return lease, true
		}
	}
	if !s.shouldAdmitHash(keyHash) {
		return lease, false
	}
	if wantLease {
		// Recheck after admission; another goroutine may have inserted while we
		// were outside the shard lock.
		if ref := s.tryRetainMaybeMark(key, c.isSIEVE()); ref != nil {
			if ref.block != block {
				recycleLeafBlockDecodedBlock(block)
				c.putDuplicateDrops.Add(1)
			}
			lease.ref = ref
			c.putAdmitted.Add(1)
			return lease, true
		}
	}

	stagedRef := getLeafBlockRefSlot()
	if !s.mu.TryLock() {
		c.putLockContention.Add(1)
		s.mu.Lock()
	}
	var releaseEvicted *leafBlockRef
	var duplicateDropBlock *leafblock.DecodedBlock
	var evictedHash uint64
	evicted := false
	if idx, ok := s.entries[key]; ok {
		ref := s.nodes[idx].ref
		if ref == nil {
			ref = initLeafBlockRefSlot(stagedRef, block)
			stagedRef = nil
			s.nodes[idx].ref = ref
		} else if ref.block != block {
			duplicateDropBlock = block
			c.putDuplicateDrops.Add(1)
		}
		if wantLease {
			if c.isSLRU() {
				s.slruTouch(idx)
			} else if c.isSIEVE() {
				s.sieveMark(idx)
			} else {
				s.moveToFront(idx)
			}
		} else if c.isSIEVE() {
			s.sieveMark(idx)
		}
		if wantLease {
			if ref != nil && ref.retain() {
				lease.ref = ref
			}
		}
		s.mu.Unlock()
		if stagedRef != nil {
			putLeafBlockRefSlot(stagedRef)
		}
		if duplicateDropBlock != nil {
			recycleLeafBlockDecodedBlock(duplicateDropBlock)
		}
		if !wantLease {
			c.putAdmitted.Add(1)
			return lease, true
		}
		if lease.ref != nil {
			c.putAdmitted.Add(1)
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
		if c.isSLRU() {
			idx = s.slruPickVictim()
		} else if c.isSIEVE() {
			idx = s.sievePickVictim()
		} else {
			idx = s.tail
		}
		if idx < 0 {
			s.mu.Unlock()
			if stagedRef != nil {
				putLeafBlockRefSlot(stagedRef)
			}
			return lease, false
		}
		evictedKey := s.nodes[idx].key
		evictedHash = leafBlockKeyHash(evictedKey)
		evicted = true
		releaseEvicted = s.nodes[idx].ref
		if c.isSLRU() {
			s.slruRemove(idx)
		} else if c.isSIEVE() {
			s.sieveRemove(idx)
		} else {
			s.unlink(idx)
		}
		delete(s.entries, evictedKey)
	}
	node := &s.nodes[idx]
	node.key = key
	if releaseEvicted != nil && releaseEvicted.block == block {
		node.ref = releaseEvicted
		releaseEvicted = nil
	} else {
		node.ref = initLeafBlockRefSlot(stagedRef, block)
		stagedRef = nil
	}
	node.prev = -1
	node.next = -1
	if c.isSLRU() {
		s.slruInsertProbation(idx)
	} else if c.isSIEVE() {
		s.linkFront(idx)
		s.sieveInsert(idx)
	} else {
		s.linkFront(idx)
	}
	s.entries[key] = idx
	if inserted {
		s.entryCount.Add(1)
	}
	if wantLease {
		if ref := node.ref; ref != nil && ref.retain() {
			lease.ref = ref
		}
	}
	s.mu.Unlock()
	if stagedRef != nil {
		putLeafBlockRefSlot(stagedRef)
	}
	if releaseEvicted != nil {
		releaseEvicted.release()
	}
	if evicted {
		s.admitObserveVictimHash(evictedHash)
	}
	if !wantLease {
		c.putAdmitted.Add(1)
		return lease, true
	}
	if lease.ref != nil {
		c.putAdmitted.Add(1)
	}
	return lease, lease.ref != nil
}

func (s *leafBlockCacheShard) hasCachedRef(key leafBlockKey, block *leafblock.DecodedBlock) (hit bool, same bool) {
	return s.hasCachedRefMaybeMark(key, block, false)
}

func (s *leafBlockCacheShard) hasCachedRefMaybeMark(key leafBlockKey, block *leafblock.DecodedBlock, markSIEVE bool) (hit bool, same bool) {
	if s == nil {
		return false, false
	}
	s.mu.RLock()
	idx, ok := s.entries[key]
	if !ok {
		s.mu.RUnlock()
		return false, false
	}
	ref := s.nodes[idx].ref
	if ref == nil || ref.refs.Load() <= 0 || ref.block == nil {
		s.mu.RUnlock()
		return false, false
	}
	hit = true
	same = ref.block == block
	if markSIEVE {
		s.sieveMark(idx)
	}
	s.mu.RUnlock()
	return hit, same
}

func (s *leafBlockCacheShard) tryRetain(key leafBlockKey) *leafBlockRef {
	return s.tryRetainMaybeMark(key, false)
}

func (s *leafBlockCacheShard) tryRetainMaybeMark(key leafBlockKey, markSIEVE bool) *leafBlockRef {
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
	if markSIEVE {
		s.sieveMark(idx)
	}
	s.mu.RUnlock()
	return ref
}

func (c *leafBlockCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
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

func (c *leafBlockCache) putStats() (attempts uint64, admitted uint64, duplicateDrops uint64, lockContention uint64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	return c.putAttempts.Load(), c.putAdmitted.Load(), c.putDuplicateDrops.Load(), c.putLockContention.Load()
}

func (c *leafBlockCache) shardFor(key leafBlockKey) *leafBlockCacheShard {
	s, _ := c.shardForAndHash(key)
	return s
}

func (c *leafBlockCache) shardForAndHash(key leafBlockKey) (*leafBlockCacheShard, uint64) {
	h := leafBlockKeyHash(key)
	if len(c.shards) == 1 {
		return &c.shards[0], h
	}
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx], h
}

func (s *leafBlockCacheShard) shouldAdmit(key leafBlockKey) bool {
	return s.shouldAdmitHash(leafBlockKeyHash(key))
}

func (s *leafBlockCacheShard) shouldAdmitHash(h uint64) bool {
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
	freq := s.admitTouchHash(h)
	threshold := leafBlockCacheAdmitThresholdMin
	if entries >= s.capacity/2 {
		floor := uint8(s.admitFloor.Load())
		if floor > threshold {
			threshold = floor
		}
		if threshold > leafBlockCacheAdmitThresholdMax {
			threshold = leafBlockCacheAdmitThresholdMax
		}
	}
	return freq >= threshold
}

func (s *leafBlockCacheShard) admitTouchHash(h uint64) uint8 {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return 0
	}
	idxA, idxB := leafBlockCacheAdmitIndexes(h, s.admitMask)
	if idxA == idxB {
		est := s.admitIncrementCounter(idxA)
		s.admitMaybeDecay()
		return est
	}
	a := s.admitIncrementCounter(idxA)
	b := s.admitIncrementCounter(idxB)
	s.admitMaybeDecay()
	if a < b {
		return a
	}
	return b
}

func (s *leafBlockCacheShard) admitEstimateHash(h uint64) uint8 {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return 0
	}
	idxA, idxB := leafBlockCacheAdmitIndexes(h, s.admitMask)
	a := s.admitCounterAtIndex(idxA)
	if idxA == idxB {
		return a
	}
	b := s.admitCounterAtIndex(idxB)
	if a < b {
		return a
	}
	return b
}

func (s *leafBlockCacheShard) admitObserveVictimHash(h uint64) {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return
	}
	freq := s.admitEstimateHash(h)
	if freq == 0 {
		return
	}
	if freq > leafBlockCacheAdmitThresholdMax {
		freq = leafBlockCacheAdmitThresholdMax
	}
	for {
		old := s.admitFloor.Load()
		var next uint32
		if old == 0 {
			next = uint32(freq)
		} else {
			next = (old*leafBlockCacheAdmitVictimEWMAWeight + uint32(freq)) / (leafBlockCacheAdmitVictimEWMAWeight + 1)
		}
		if next > uint32(leafBlockCacheAdmitCounterMax) {
			next = uint32(leafBlockCacheAdmitCounterMax)
		}
		if s.admitFloor.CompareAndSwap(old, next) {
			return
		}
	}
}

func (s *leafBlockCacheShard) admitMaybeDecay() {
	if s == nil || s.admitDecayEvery == 0 || len(s.admit) == 0 {
		return
	}
	if s.admitSamples.Add(1) < s.admitDecayEvery {
		return
	}
	if !s.admitDecaying.CompareAndSwap(0, 1) {
		return
	}
	defer s.admitDecaying.Store(0)
	if s.admitSamples.Load() < s.admitDecayEvery {
		return
	}
	for i := range s.admit {
		word := &s.admit[i]
		for {
			old := word.Load()
			next := (old >> 1) & leafBlockCacheAdmitDecayNibbleHalfMask
			if word.CompareAndSwap(old, next) {
				break
			}
		}
	}
	s.admitSamples.Store(0)
	for {
		oldFloor := s.admitFloor.Load()
		if s.admitFloor.CompareAndSwap(oldFloor, oldFloor>>1) {
			break
		}
	}
}

func (s *leafBlockCacheShard) admitCounterAtIndex(idx uint64) uint8 {
	wordIdx, shift := leafBlockCacheAdmitWordShift(idx)
	if wordIdx < 0 || wordIdx >= len(s.admit) {
		return 0
	}
	word := s.admit[wordIdx].Load()
	return uint8((word >> shift) & leafBlockCacheAdmitCounterMask)
}

func (s *leafBlockCacheShard) admitIncrementCounter(idx uint64) uint8 {
	wordIdx, shift := leafBlockCacheAdmitWordShift(idx)
	if wordIdx < 0 || wordIdx >= len(s.admit) {
		return 0
	}
	word := &s.admit[wordIdx]
	mask := leafBlockCacheAdmitCounterMask << shift
	step := uint64(1) << shift
	for {
		old := word.Load()
		cur := (old & mask) >> shift
		if cur >= leafBlockCacheAdmitCounterMask {
			return leafBlockCacheAdmitCounterMax
		}
		if word.CompareAndSwap(old, old+step) {
			return uint8(cur + 1)
		}
	}
}

func leafBlockCacheAdmitWordShift(idx uint64) (int, uint) {
	word := int(idx >> 4)
	shift := uint((idx & 0x0f) << 2)
	return word, shift
}

func leafBlockCacheAdmitIndexes(h, mask uint64) (uint64, uint64) {
	// Two derived bit positions preserve second-touch admission while sharply
	// lowering false positives from hash collisions under large random key sets.
	idxA := h & mask
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 29
	idxB := h & mask
	return idxA, idxB
}

func (s *leafBlockCacheShard) sieveMark(idx int) {
	if s == nil || idx < 0 || idx >= len(s.sieveRef) {
		return
	}
	bit := &s.sieveRef[idx]
	if bit.Load() == 0 {
		bit.CompareAndSwap(0, 1)
	}
}

func (s *leafBlockCacheShard) sieveInsert(idx int) {
	if s == nil || idx < 0 || idx >= len(s.sieveRef) {
		return
	}
	// SIEVE keeps new entries as probationary; they need one observed hit to be
	// granted a second chance.
	s.sieveRef[idx].Store(0)
	if s.sieveHand < 0 {
		s.sieveHand = s.tail
	}
}

func (s *leafBlockCacheShard) sieveRemove(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	prev := s.nodes[idx].prev
	if s.sieveHand == idx {
		if prev >= 0 {
			s.sieveHand = prev
		} else {
			s.sieveHand = s.tail
		}
	}
	if idx >= 0 && idx < len(s.sieveRef) {
		s.sieveRef[idx].Store(0)
	}
	s.unlink(idx)
	if s.head < 0 || s.tail < 0 {
		s.sieveHand = -1
	}
}

func (s *leafBlockCacheShard) sievePickVictim() int {
	if s == nil {
		return -1
	}
	if s.tail < 0 {
		return -1
	}
	if s.sieveHand < 0 {
		s.sieveHand = s.tail
	}
	n := len(s.nodes)
	if n == 0 {
		return -1
	}
	for scanned := 0; scanned < n*2; scanned++ {
		idx := s.sieveHand
		if idx < 0 {
			idx = s.tail
		}
		if idx < 0 {
			return -1
		}
		node := &s.nodes[idx]
		prev := node.prev
		if node.ref == nil {
			if prev >= 0 {
				s.sieveHand = prev
			} else {
				s.sieveHand = s.tail
			}
			continue
		}
		if idx >= 0 && idx < len(s.sieveRef) && s.sieveRef[idx].Swap(0) != 0 {
			if prev >= 0 {
				s.sieveHand = prev
			} else {
				s.sieveHand = s.tail
			}
			continue
		}
		if prev >= 0 {
			s.sieveHand = prev
		} else {
			s.sieveHand = s.tail
		}
		return idx
	}
	return s.tail
}

func (s *leafBlockCacheShard) slruPickVictim() int {
	if s == nil {
		return -1
	}
	if s.slruProbTail >= 0 {
		return s.slruProbTail
	}
	return s.slruProtTail
}

func (s *leafBlockCacheShard) slruInsertProbation(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	s.nodes[idx].segment = leafBlockCacheSegmentProbation
	s.slruLinkFront(idx, leafBlockCacheSegmentProbation)
}

func (s *leafBlockCacheShard) slruTouch(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	if s.nodes[idx].segment == leafBlockCacheSegmentProtected {
		s.slruMoveToFront(idx)
		return
	}
	s.slruPromoteToProtected(idx)
}

func (s *leafBlockCacheShard) slruPromoteToProtected(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	if s.nodes[idx].segment == leafBlockCacheSegmentProtected {
		s.slruMoveToFront(idx)
		return
	}
	s.slruUnlink(idx)
	s.nodes[idx].segment = leafBlockCacheSegmentProtected
	s.slruLinkFront(idx, leafBlockCacheSegmentProtected)
	s.slruProtCount++
	if s.slruProtCount <= s.slruProtCap {
		return
	}
	demote := s.slruProtTail
	if demote < 0 {
		return
	}
	s.slruUnlink(demote)
	if s.slruProtCount > 0 {
		s.slruProtCount--
	}
	s.nodes[demote].segment = leafBlockCacheSegmentProbation
	s.slruLinkFront(demote, leafBlockCacheSegmentProbation)
}

func (s *leafBlockCacheShard) slruMoveToFront(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	node := &s.nodes[idx]
	if node.segment == leafBlockCacheSegmentProtected {
		if idx == s.slruProtHead {
			return
		}
	} else {
		if idx == s.slruProbHead {
			return
		}
	}
	segment := node.segment
	s.slruUnlink(idx)
	s.slruLinkFront(idx, segment)
}

func (s *leafBlockCacheShard) slruRemove(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	segment := s.nodes[idx].segment
	s.slruUnlink(idx)
	if segment == leafBlockCacheSegmentProtected && s.slruProtCount > 0 {
		s.slruProtCount--
	}
}

func (s *leafBlockCacheShard) slruLinkFront(idx int, segment uint8) {
	node := &s.nodes[idx]
	node.segment = segment
	node.prev = -1
	if segment == leafBlockCacheSegmentProtected {
		node.next = s.slruProtHead
		if s.slruProtHead >= 0 {
			s.nodes[s.slruProtHead].prev = idx
		}
		s.slruProtHead = idx
		if s.slruProtTail < 0 {
			s.slruProtTail = idx
		}
		return
	}
	node.next = s.slruProbHead
	if s.slruProbHead >= 0 {
		s.nodes[s.slruProbHead].prev = idx
	}
	s.slruProbHead = idx
	if s.slruProbTail < 0 {
		s.slruProbTail = idx
	}
}

func (s *leafBlockCacheShard) slruUnlink(idx int) {
	node := &s.nodes[idx]
	prev := node.prev
	next := node.next
	if node.segment == leafBlockCacheSegmentProtected {
		if prev >= 0 {
			s.nodes[prev].next = next
		} else {
			s.slruProtHead = next
		}
		if next >= 0 {
			s.nodes[next].prev = prev
		} else {
			s.slruProtTail = prev
		}
	} else {
		if prev >= 0 {
			s.nodes[prev].next = next
		} else {
			s.slruProbHead = next
		}
		if next >= 0 {
			s.nodes[next].prev = prev
		} else {
			s.slruProbTail = prev
		}
	}
	node.prev = -1
	node.next = -1
}

func (s *leafBlockCacheShard) moveToFront(idx int) {
	if idx == s.head {
		return
	}
	s.unlink(idx)
	s.linkFront(idx)
}

func (s *leafBlockCacheShard) linkFront(idx int) {
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

func (s *leafBlockCacheShard) unlink(idx int) {
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
