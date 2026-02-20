package db

import (
	"os"
	"strings"
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
	key     outerLeafBlockKey
	ref     *outerLeafBlockRef
	prev    int
	next    int
	segment uint8
}

type outerLeafBlockRef struct {
	block *outerleaf.DecodedBlock
	refs  atomic.Int32
}

var outerLeafBlockRefPool sync.Pool

func getOuterLeafBlockRefSlot() *outerLeafBlockRef {
	var ref *outerLeafBlockRef
	if v := outerLeafBlockRefPool.Get(); v != nil {
		if pooled, ok := v.(*outerLeafBlockRef); ok {
			ref = pooled
		}
	}
	if ref == nil {
		ref = &outerLeafBlockRef{}
	}
	return ref
}

func initOuterLeafBlockRefSlot(ref *outerLeafBlockRef, block *outerleaf.DecodedBlock) *outerLeafBlockRef {
	if block == nil {
		return nil
	}
	if ref == nil {
		ref = getOuterLeafBlockRefSlot()
	}
	ref.block = block
	ref.refs.Store(1) // cache ownership
	return ref
}

func newOuterLeafBlockRef(block *outerleaf.DecodedBlock) *outerLeafBlockRef {
	return initOuterLeafBlockRefSlot(nil, block)
}

func putOuterLeafBlockRefSlot(ref *outerLeafBlockRef) {
	if ref == nil {
		return
	}
	ref.block = nil
	ref.refs.Store(0)
	outerLeafBlockRefPool.Put(ref)
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
	putOuterLeafBlockRefSlot(r)
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

type outerLeafBlockCache struct {
	shards   []outerLeafBlockCacheShard
	capacity int
	policy   outerLeafBlockCachePolicy
	// Temporary counters for validating put-path contention/allocation work.
	putAttempts       atomic.Uint64
	putAdmitted       atomic.Uint64
	putDuplicateDrops atomic.Uint64
	putLockContention atomic.Uint64
}

type outerLeafBlockCachePolicy uint8

const (
	outerLeafBlockCachePolicyLRU outerLeafBlockCachePolicy = iota
	outerLeafBlockCachePolicyLRULight
	outerLeafBlockCachePolicySIEVE
	outerLeafBlockCachePolicySLRU
)

const outerLeafBlockCachePolicyEnv = "TREEDB_OUTER_LEAF_BLOCK_CACHE_POLICY"

var outerLeafBlockCachePolicyOnce sync.Once
var outerLeafBlockCachePolicyCached outerLeafBlockCachePolicy

// On heavily contended read paths, full LRU promotion on every cache hit
// creates avoidable write-lock churn. Promote a sampled subset of hits to keep
// recency reasonably fresh while reducing lock pressure.
const outerLeafBlockCachePromoteSampleMask uint64 = 0x0f              // 1/16
const outerLeafBlockCachePromoteSampleMaskLight uint64 = 0x3f         // 1/64
const outerLeafBlockCacheSLRUProbationPromoteSampleMask uint64 = 0x03 // 1/4

// Keep shard fanout bounded while aiming for modest per-shard queueing under
// mixed read/write contention.
const outerLeafBlockCacheTargetEntriesPerShard = 64
const outerLeafBlockCacheMaxShards = 64
const outerLeafBlockCacheAdmitCountersPerEntry = 16
const outerLeafBlockCacheAdmitMinCounters = 256
const outerLeafBlockCacheAdmitCountersPerWord = 16
const outerLeafBlockCacheAdmitCounterMask uint64 = 0x0f
const outerLeafBlockCacheAdmitDecayNibbleHalfMask uint64 = 0x7777777777777777
const outerLeafBlockCacheAdmitCounterMax uint8 = 15
const outerLeafBlockCacheAdmitDecayMultiplier = 8
const outerLeafBlockCacheAdmitDecayMinSamples = 256
const outerLeafBlockCacheAdmitThresholdMin uint8 = 2
const outerLeafBlockCacheAdmitThresholdMax uint8 = 4
const outerLeafBlockCacheAdmitVictimEWMAWeight = 7 // 7/8 old + 1/8 new
const outerLeafBlockCacheSLRUProtectedFractionNum = 1
const outerLeafBlockCacheSLRUProtectedFractionDen = 2
const outerLeafBlockCacheSegmentProbation uint8 = 0
const outerLeafBlockCacheSegmentProtected uint8 = 1

func newOuterLeafBlockCache(capacity int) *outerLeafBlockCache {
	return newOuterLeafBlockCacheWithPolicy(capacity, outerLeafBlockCacheDefaultPolicy())
}

func newOuterLeafBlockCacheWithPolicy(capacity int, policy outerLeafBlockCachePolicy) *outerLeafBlockCache {
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
		admitCounters := 0
		admitDecayEvery := uint32(0)
		if capI > 0 {
			targetCounters := capI * outerLeafBlockCacheAdmitCountersPerEntry
			if targetCounters < outerLeafBlockCacheAdmitMinCounters {
				targetCounters = outerLeafBlockCacheAdmitMinCounters
			}
			admitCounters = 1
			for admitCounters < targetCounters {
				admitCounters <<= 1
			}
			admitDecayEvery = uint32(admitCounters * outerLeafBlockCacheAdmitDecayMultiplier)
			if admitDecayEvery < outerLeafBlockCacheAdmitDecayMinSamples {
				admitDecayEvery = outerLeafBlockCacheAdmitDecayMinSamples
			}
		}
		admitWords := 0
		admitMask := uint64(0)
		if admitCounters > 0 {
			admitWords = (admitCounters + outerLeafBlockCacheAdmitCountersPerWord - 1) / outerLeafBlockCacheAdmitCountersPerWord
			admitMask = uint64(admitCounters - 1)
		}
		free := make([]int, capI)
		nodes := make([]outerLeafBlockCacheEntry, capI)
		for j := 0; j < capI; j++ {
			nodes[j].prev = -1
			nodes[j].next = -1
			free[j] = capI - 1 - j
		}
		shards[i] = outerLeafBlockCacheShard{
			entries:         make(map[outerLeafBlockKey]int, capI),
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
		if policy == outerLeafBlockCachePolicySIEVE {
			shards[i].sieveRef = make([]atomic.Uint32, capI)
		}
		if policy == outerLeafBlockCachePolicySLRU {
			protCap := (capI * outerLeafBlockCacheSLRUProtectedFractionNum) / outerLeafBlockCacheSLRUProtectedFractionDen
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

	return &outerLeafBlockCache{
		shards:   shards,
		capacity: capacity,
		policy:   policy,
	}
}

func outerLeafBlockCacheDefaultPolicy() outerLeafBlockCachePolicy {
	outerLeafBlockCachePolicyOnce.Do(func() {
		switch strings.ToLower(strings.TrimSpace(os.Getenv(outerLeafBlockCachePolicyEnv))) {
		case "lru_light", "lru-light", "lrulight":
			outerLeafBlockCachePolicyCached = outerLeafBlockCachePolicyLRULight
		case "sieve":
			outerLeafBlockCachePolicyCached = outerLeafBlockCachePolicySIEVE
		case "slru", "2q":
			outerLeafBlockCachePolicyCached = outerLeafBlockCachePolicySLRU
		case "clock":
			// Backward-compatible alias from the prior experiment.
			outerLeafBlockCachePolicyCached = outerLeafBlockCachePolicySLRU
		default:
			outerLeafBlockCachePolicyCached = outerLeafBlockCachePolicyLRU
		}
	})
	return outerLeafBlockCachePolicyCached
}

func (c *outerLeafBlockCache) policyName() string {
	if c == nil {
		return "disabled"
	}
	if c.policy == outerLeafBlockCachePolicyLRULight {
		return "lru_light"
	}
	if c.policy == outerLeafBlockCachePolicySIEVE {
		return "sieve"
	}
	if c.policy == outerLeafBlockCachePolicySLRU {
		return "slru"
	}
	return "lru"
}

func (c *outerLeafBlockCache) isSLRU() bool {
	return c != nil && c.policy == outerLeafBlockCachePolicySLRU
}

func (c *outerLeafBlockCache) isSIEVE() bool {
	return c != nil && c.policy == outerLeafBlockCachePolicySIEVE
}

func (c *outerLeafBlockCache) lruPromoteMask() uint64 {
	if c != nil && c.policy == outerLeafBlockCachePolicyLRULight {
		return outerLeafBlockCachePromoteSampleMaskLight
	}
	return outerLeafBlockCachePromoteSampleMask
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
	if c.isSIEVE() {
		s.sieveMark(idx)
	}
	block := ref.block
	segment := s.nodes[idx].segment
	needPromoteLRU := c.policy == outerLeafBlockCachePolicyLRU && idx != s.head
	needPromoteLRULight := c.policy == outerLeafBlockCachePolicyLRULight && idx != s.head
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
		shouldPromoteProbation := segment == outerLeafBlockCacheSegmentProbation && (hits&outerLeafBlockCacheSLRUProbationPromoteSampleMask) == 0
		shouldRefreshProtected := segment == outerLeafBlockCacheSegmentProtected && (hits&outerLeafBlockCachePromoteSampleMask) == 0
		if (shouldPromoteProbation || shouldRefreshProtected) && s.mu.TryLock() {
			if idx2, ok2 := s.entries[key]; ok2 {
				if s.nodes[idx2].segment == outerLeafBlockCacheSegmentProbation {
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

func (c *outerLeafBlockCache) put(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	_, admitted := c.putInternal(key, block, false)
	if !admitted {
		recycleOuterLeafDecodedBlock(block)
	}
}

// putAfterMissNoLease is a miss-aware insert path for decode callers that
// already observed a cache miss and do not require a lease. It intentionally
// avoids a pre-lock read probe and can drop admission under contention once the
// shard is warm to reduce write-lock amplification on parallel read misses.
func (c *outerLeafBlockCache) putAfterMissNoLease(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	if !c.putAfterMissNoLeaseInternal(key, block) {
		recycleOuterLeafDecodedBlock(block)
	}
}

func (c *outerLeafBlockCache) putAfterMissNoLeaseInternal(key outerLeafBlockKey, block *outerleaf.DecodedBlock) bool {
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

	stagedRef := getOuterLeafBlockRefSlot()
	warmOrFull := int(s.entryCount.Load()) >= s.capacity/2
	if !s.mu.TryLock() {
		c.putLockContention.Add(1)
		if warmOrFull {
			putOuterLeafBlockRefSlot(stagedRef)
			return false
		}
		s.mu.Lock()
	}

	var releaseEvicted *outerLeafBlockRef
	var duplicateDropBlock *outerleaf.DecodedBlock
	var evictedHash uint64
	evicted := false
	if idx, ok := s.entries[key]; ok {
		ref := s.nodes[idx].ref
		if ref == nil {
			ref = initOuterLeafBlockRefSlot(stagedRef, block)
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
			putOuterLeafBlockRefSlot(stagedRef)
		}
		if duplicateDropBlock != nil {
			recycleOuterLeafDecodedBlock(duplicateDropBlock)
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
				putOuterLeafBlockRefSlot(stagedRef)
			}
			return false
		}
		evictedKey := s.nodes[idx].key
		evictedHash = outerLeafBlockKeyHash(evictedKey)
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
		node.ref = initOuterLeafBlockRefSlot(stagedRef, block)
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
		putOuterLeafBlockRefSlot(stagedRef)
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

func (c *outerLeafBlockCache) putWithLease(key outerLeafBlockKey, block *outerleaf.DecodedBlock) (outerLeafBlockCacheLease, bool) {
	return c.putInternal(key, block, true)
}

func (c *outerLeafBlockCache) putInternal(key outerLeafBlockKey, block *outerleaf.DecodedBlock, wantLease bool) (outerLeafBlockCacheLease, bool) {
	var lease outerLeafBlockCacheLease
	if c == nil || block == nil {
		return lease, false
	}
	c.putAttempts.Add(1)
	s, keyHash := c.shardForAndHash(key)
	if s.capacity <= 0 {
		return lease, false
	}
	if !wantLease {
		if hit, same := s.hasCachedRef(key, block); hit {
			if c.isSIEVE() {
				s.markSIEVEByKey(key)
			}
			if !same {
				recycleOuterLeafDecodedBlock(block)
				c.putDuplicateDrops.Add(1)
			}
			c.putAdmitted.Add(1)
			return lease, true
		}
	} else {
		if ref := s.tryRetain(key); ref != nil {
			if c.isSIEVE() {
				s.markSIEVEByKey(key)
			}
			if ref.block != block {
				recycleOuterLeafDecodedBlock(block)
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
		if ref := s.tryRetain(key); ref != nil {
			if c.isSIEVE() {
				s.markSIEVEByKey(key)
			}
			if ref.block != block {
				recycleOuterLeafDecodedBlock(block)
				c.putDuplicateDrops.Add(1)
			}
			lease.ref = ref
			c.putAdmitted.Add(1)
			return lease, true
		}
	}

	stagedRef := getOuterLeafBlockRefSlot()
	if !s.mu.TryLock() {
		c.putLockContention.Add(1)
		s.mu.Lock()
	}
	var releaseEvicted *outerLeafBlockRef
	var duplicateDropBlock *outerleaf.DecodedBlock
	var evictedHash uint64
	evicted := false
	if idx, ok := s.entries[key]; ok {
		ref := s.nodes[idx].ref
		if ref == nil {
			ref = initOuterLeafBlockRefSlot(stagedRef, block)
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
			putOuterLeafBlockRefSlot(stagedRef)
		}
		if duplicateDropBlock != nil {
			recycleOuterLeafDecodedBlock(duplicateDropBlock)
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
				putOuterLeafBlockRefSlot(stagedRef)
			}
			return lease, false
		}
		evictedKey := s.nodes[idx].key
		evictedHash = outerLeafBlockKeyHash(evictedKey)
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
		node.ref = initOuterLeafBlockRefSlot(stagedRef, block)
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
		putOuterLeafBlockRefSlot(stagedRef)
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

func (s *outerLeafBlockCacheShard) hasCachedRef(key outerLeafBlockKey, block *outerleaf.DecodedBlock) (hit bool, same bool) {
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
	s.mu.RUnlock()
	return hit, same
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

func (s *outerLeafBlockCacheShard) markSIEVEByKey(key outerLeafBlockKey) {
	if s == nil || len(s.sieveRef) == 0 {
		return
	}
	s.mu.RLock()
	idx, ok := s.entries[key]
	if ok {
		s.sieveMark(idx)
	}
	s.mu.RUnlock()
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

func (c *outerLeafBlockCache) putStats() (attempts uint64, admitted uint64, duplicateDrops uint64, lockContention uint64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	return c.putAttempts.Load(), c.putAdmitted.Load(), c.putDuplicateDrops.Load(), c.putLockContention.Load()
}

func (c *outerLeafBlockCache) shardFor(key outerLeafBlockKey) *outerLeafBlockCacheShard {
	s, _ := c.shardForAndHash(key)
	return s
}

func (c *outerLeafBlockCache) shardForAndHash(key outerLeafBlockKey) (*outerLeafBlockCacheShard, uint64) {
	h := outerLeafBlockKeyHash(key)
	if len(c.shards) == 1 {
		return &c.shards[0], h
	}
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx], h
}

func (s *outerLeafBlockCacheShard) shouldAdmit(key outerLeafBlockKey) bool {
	return s.shouldAdmitHash(outerLeafBlockKeyHash(key))
}

func (s *outerLeafBlockCacheShard) shouldAdmitHash(h uint64) bool {
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
	threshold := outerLeafBlockCacheAdmitThresholdMin
	if entries >= s.capacity/2 {
		floor := uint8(s.admitFloor.Load())
		if floor > threshold {
			threshold = floor
		}
		if threshold > outerLeafBlockCacheAdmitThresholdMax {
			threshold = outerLeafBlockCacheAdmitThresholdMax
		}
	}
	return freq >= threshold
}

func (s *outerLeafBlockCacheShard) admitTouchHash(h uint64) uint8 {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return 0
	}
	idxA, idxB := outerLeafBlockCacheAdmitIndexes(h, s.admitMask)
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

func (s *outerLeafBlockCacheShard) admitEstimateHash(h uint64) uint8 {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return 0
	}
	idxA, idxB := outerLeafBlockCacheAdmitIndexes(h, s.admitMask)
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

func (s *outerLeafBlockCacheShard) admitObserveVictimHash(h uint64) {
	if s == nil || len(s.admit) == 0 || s.admitMask == 0 {
		return
	}
	freq := s.admitEstimateHash(h)
	if freq == 0 {
		return
	}
	if freq > outerLeafBlockCacheAdmitThresholdMax {
		freq = outerLeafBlockCacheAdmitThresholdMax
	}
	for {
		old := s.admitFloor.Load()
		var next uint32
		if old == 0 {
			next = uint32(freq)
		} else {
			next = (old*outerLeafBlockCacheAdmitVictimEWMAWeight + uint32(freq)) / (outerLeafBlockCacheAdmitVictimEWMAWeight + 1)
		}
		if next > uint32(outerLeafBlockCacheAdmitCounterMax) {
			next = uint32(outerLeafBlockCacheAdmitCounterMax)
		}
		if s.admitFloor.CompareAndSwap(old, next) {
			return
		}
	}
}

func (s *outerLeafBlockCacheShard) admitMaybeDecay() {
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
			next := (old >> 1) & outerLeafBlockCacheAdmitDecayNibbleHalfMask
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

func (s *outerLeafBlockCacheShard) admitCounterAtIndex(idx uint64) uint8 {
	wordIdx, shift := outerLeafBlockCacheAdmitWordShift(idx)
	if wordIdx < 0 || wordIdx >= len(s.admit) {
		return 0
	}
	word := s.admit[wordIdx].Load()
	return uint8((word >> shift) & outerLeafBlockCacheAdmitCounterMask)
}

func (s *outerLeafBlockCacheShard) admitIncrementCounter(idx uint64) uint8 {
	wordIdx, shift := outerLeafBlockCacheAdmitWordShift(idx)
	if wordIdx < 0 || wordIdx >= len(s.admit) {
		return 0
	}
	word := &s.admit[wordIdx]
	mask := outerLeafBlockCacheAdmitCounterMask << shift
	step := uint64(1) << shift
	for {
		old := word.Load()
		cur := (old & mask) >> shift
		if cur >= outerLeafBlockCacheAdmitCounterMask {
			return outerLeafBlockCacheAdmitCounterMax
		}
		if word.CompareAndSwap(old, old+step) {
			return uint8(cur + 1)
		}
	}
}

func outerLeafBlockCacheAdmitWordShift(idx uint64) (int, uint) {
	word := int(idx >> 4)
	shift := uint((idx & 0x0f) << 2)
	return word, shift
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

func (s *outerLeafBlockCacheShard) sieveMark(idx int) {
	if s == nil || idx < 0 || idx >= len(s.sieveRef) {
		return
	}
	s.sieveRef[idx].Store(1)
}

func (s *outerLeafBlockCacheShard) sieveInsert(idx int) {
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

func (s *outerLeafBlockCacheShard) sieveRemove(idx int) {
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

func (s *outerLeafBlockCacheShard) sievePickVictim() int {
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

func (s *outerLeafBlockCacheShard) slruPickVictim() int {
	if s == nil {
		return -1
	}
	if s.slruProbTail >= 0 {
		return s.slruProbTail
	}
	return s.slruProtTail
}

func (s *outerLeafBlockCacheShard) slruInsertProbation(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	s.nodes[idx].segment = outerLeafBlockCacheSegmentProbation
	s.slruLinkFront(idx, outerLeafBlockCacheSegmentProbation)
}

func (s *outerLeafBlockCacheShard) slruTouch(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	if s.nodes[idx].segment == outerLeafBlockCacheSegmentProtected {
		s.slruMoveToFront(idx)
		return
	}
	s.slruPromoteToProtected(idx)
}

func (s *outerLeafBlockCacheShard) slruPromoteToProtected(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	if s.nodes[idx].segment == outerLeafBlockCacheSegmentProtected {
		s.slruMoveToFront(idx)
		return
	}
	s.slruUnlink(idx)
	s.nodes[idx].segment = outerLeafBlockCacheSegmentProtected
	s.slruLinkFront(idx, outerLeafBlockCacheSegmentProtected)
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
	s.nodes[demote].segment = outerLeafBlockCacheSegmentProbation
	s.slruLinkFront(demote, outerLeafBlockCacheSegmentProbation)
}

func (s *outerLeafBlockCacheShard) slruMoveToFront(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	node := &s.nodes[idx]
	if node.segment == outerLeafBlockCacheSegmentProtected {
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

func (s *outerLeafBlockCacheShard) slruRemove(idx int) {
	if s == nil || idx < 0 || idx >= len(s.nodes) {
		return
	}
	segment := s.nodes[idx].segment
	s.slruUnlink(idx)
	if segment == outerLeafBlockCacheSegmentProtected && s.slruProtCount > 0 {
		s.slruProtCount--
	}
}

func (s *outerLeafBlockCacheShard) slruLinkFront(idx int, segment uint8) {
	node := &s.nodes[idx]
	node.segment = segment
	node.prev = -1
	if segment == outerLeafBlockCacheSegmentProtected {
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

func (s *outerLeafBlockCacheShard) slruUnlink(idx int) {
	node := &s.nodes[idx]
	prev := node.prev
	next := node.next
	if node.segment == outerLeafBlockCacheSegmentProtected {
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
