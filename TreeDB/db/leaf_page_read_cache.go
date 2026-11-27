package db

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	// LeafPageReadCacheEntriesEnvKey names the environment variable that
	// overrides the process default when Options.LeafPageReadCacheEntries is 0.
	LeafPageReadCacheEntriesEnvKey    = "TREEDB_LEAF_PAGE_CACHE_ENTRIES"
	defaultLeafPageReadCacheEntries   = 8192
	maxLeafPageReadCacheEntries       = 1 << 18
	leafPageReadCacheWays             = 4
	leafPageReadCacheBackingSlabPages = 64
	// Bound miss-admission observer retries so high contention yields a skipped
	// admission instead of unbounded reader spinning. 64 odd-epoch yields cover
	// transient reset windows; 256 stable-epoch retries bound CAS churn; and a
	// hard total-iteration cap guarantees termination under sustained churn.
	maxReadMissOddEpochSpins    = 64
	maxReadMissStableEpochSpins = 256
	maxReadMissTotalSpins       = 1024

	leafPageReadCacheWriteAdmissionSampleMask      = 63
	leafPageReadCacheWriteAdmissionMinReadCount    = 1024
	leafPageReadCacheWriteAdmissionHitMissRatioDiv = 32
)

var LeafPageReadCacheEntries = defaultLeafPageReadCacheEntries

// LeafPageReadCacheWriteAdmissionPolicy controls whether write-side outer-leaf
// appends immediately populate the in-memory read cache or use an opt-in,
// best-effort admission policy. It affects only cache population; leaf-log and
// value-log records remain persistent regardless of cache admission.
type LeafPageReadCacheWriteAdmissionPolicy uint8

const (
	// LeafPageReadCacheWriteAdmissionImmediate preserves the historical behavior:
	// every valid write-side outer-leaf append is copied into the read cache.
	LeafPageReadCacheWriteAdmissionImmediate LeafPageReadCacheWriteAdmissionPolicy = iota
	// LeafPageReadCacheWriteAdmissionAdaptive is an opt-in write-heavy policy that
	// warms the cache, samples cold write streams, re-admits when reads prove the
	// cache is hot, and skips rather than blocking on cache locks.
	LeafPageReadCacheWriteAdmissionAdaptive
)

// ParseLeafPageReadCacheWriteAdmissionPolicy parses public/user-facing policy
// strings. Empty/default keep the historical immediate behavior.
func ParseLeafPageReadCacheWriteAdmissionPolicy(raw string) (LeafPageReadCacheWriteAdmissionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default", "immediate", "always":
		return LeafPageReadCacheWriteAdmissionImmediate, nil
	case "adaptive", "guarded", "sampled":
		return LeafPageReadCacheWriteAdmissionAdaptive, nil
	default:
		return LeafPageReadCacheWriteAdmissionImmediate, fmt.Errorf("treedb: invalid leaf page read cache write admission policy %q", raw)
	}
}

func (p LeafPageReadCacheWriteAdmissionPolicy) String() string {
	switch p {
	case LeafPageReadCacheWriteAdmissionImmediate:
		return "immediate"
	case LeafPageReadCacheWriteAdmissionAdaptive:
		return "adaptive"
	default:
		return fmt.Sprintf("policy_%d", p)
	}
}

// ResolveLeafPageReadCacheEntries returns the effective cache size for an
// Options.LeafPageReadCacheEntries value after applying process/env defaults.
// It returns an error when the explicit option, environment override, or process
// default resolves outside the supported cache-size range.
func ResolveLeafPageReadCacheEntries(optionEntries int) (int, error) {
	return resolveLeafPageReadCacheEntries(optionEntries)
}

func configuredLeafPageReadCacheEntries(optionEntries int) int {
	entries, err := resolveLeafPageReadCacheEntries(optionEntries)
	if err != nil {
		return 0
	}
	return entries
}

func resolveLeafPageReadCacheEntries(optionEntries int) (int, error) {
	if optionEntries < 0 {
		return 0, nil
	}
	if optionEntries > 0 {
		return validateLeafPageReadCacheEntries(optionEntries)
	}
	if raw := strings.TrimSpace(os.Getenv(LeafPageReadCacheEntriesEnvKey)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			return validateLeafPageReadCacheEntries(v)
		}
	}
	return validateLeafPageReadCacheEntries(LeafPageReadCacheEntries)
}

func validateLeafPageReadCacheEntries(entries int) (int, error) {
	if entries < 0 {
		return 0, nil
	}
	if entries > maxLeafPageReadCacheEntries {
		return 0, fmt.Errorf("treedb: leaf page read cache entries out of range [0,%d]: %d", maxLeafPageReadCacheEntries, entries)
	}
	return entries, nil
}

type leafPageReadCacheEntry struct {
	key  leafPageReadCacheKey
	data []byte
}

type leafPageReadCacheKey struct {
	fileID   uint32
	offset   uint64
	subIndex uint16
}

func newLeafPageReadCacheKey(ptr page.LeafLogPtr) leafPageReadCacheKey {
	return leafPageReadCacheKey{
		fileID:   ptr.FileID,
		offset:   ptr.Offset,
		subIndex: ptr.SubIndex,
	}
}

type leafPageReadCacheSlot struct {
	owner                  *leafPageReadCache
	mu                     sync.RWMutex
	key                    leafPageReadCacheKey
	keyFP                  atomic.Uint64
	data                   []byte
	valid                  bool
	recordChecksumVerified bool
	// The ordinary key-only marker cannot establish proof for a bounded copy.
	copiedPageChecksumVerified atomic.Bool
	// A zero policy is uncredited. Only a successful bounded dictionary decode
	// may establish this certificate, under the same slot lock as its bytes.
	cowReadPolicy        valuelog.COWReadLimits
	cowShape             valuelog.COWRecordShape
	cowIdentity          rootpublication.StableIdentity
	hasCOWShape          bool
	pageChecksumVerified atomic.Bool
	recent               atomic.Bool
}

func (s *leafPageReadCacheSlot) ReleaseLeafLogPageView() {
	if s == nil {
		return
	}
	s.mu.RUnlock()
}

func (s *leafPageReadCacheSlot) MarkLeafLogPageViewChecksumVerified() {
	if s == nil || !s.recordChecksumVerified {
		return
	}
	if s.pageChecksumVerified.CompareAndSwap(false, true) && s.owner != nil {
		s.owner.pageChecksumVerifiedMarks.Add(1)
	}
}

func (s *leafPageReadCacheSlot) markCopiedPageChecksumVerifiedLocked() {
	if s == nil || !s.recordChecksumVerified {
		return
	}
	if !s.copiedPageChecksumVerified.Load() {
		s.copiedPageChecksumVerified.Store(true)
	}
	s.MarkLeafLogPageViewChecksumVerified()
}

func (s *leafPageReadCacheSlot) markRecent() {
	if !s.recent.Load() {
		s.recent.Store(true)
	}
}

type leafPageReadMissAdmissionLane struct {
	candidateFP atomic.Uint64
	epoch       atomic.Uint64
}

type leafPageReadCacheBucket struct {
	mu                 sync.Mutex
	readMissCandidates [leafPageReadCacheWays]leafPageReadMissAdmissionLane
	clockHand          uint32
}

const leafPageReadCacheHitStripes = 16

type leafPageReadCacheHitStripe struct {
	pageChecksumVerifiedHits   atomic.Uint64
	pageChecksumUnverifiedHits atomic.Uint64
	_                          [48]byte // pad to 64-byte cache line
}

type leafPageReadCache struct {
	slots   []leafPageReadCacheSlot
	buckets []leafPageReadCacheBucket
	ways    int

	writeAdmissionPolicy LeafPageReadCacheWriteAdmissionPolicy

	// disabled temporarily bypasses all cache reads and writes during storage
	// maintenance/compaction so stale leaf-log pages are not served while value-log
	// rewrite/GC and leaf-generation pack/GC mutate underlying storage.
	disabled atomic.Bool

	hitStripes [leafPageReadCacheHitStripes]leafPageReadCacheHitStripe

	hits      atomic.Uint64
	misses    atomic.Uint64
	stores    atomic.Uint64
	evictions atomic.Uint64
	entries   atomic.Uint64
	// Page backing belongs to fixed slots and survives eviction. Keep this
	// distinct from the bytes occupied by valid entries.
	backingBytes atomic.Uint64

	slabMu  sync.Mutex
	slab    []byte
	slabPos int

	conflictEvictions atomic.Uint64
	capacityEvictions atomic.Uint64

	readMissAdmissionSkips          atomic.Uint64
	readMissAdmissionCandidateSkips atomic.Uint64
	readMissAdmissionLockSkips      atomic.Uint64
	readMissAdmissionStores         atomic.Uint64

	writeAdmissionAttempts  atomic.Uint64
	writeAdmissionStores    atomic.Uint64
	writeAdmissionSkips     atomic.Uint64
	writeAdmissionLockSkips atomic.Uint64

	recordChecksumVerifiedStores atomic.Uint64
	pageChecksumVerifiedMarks    atomic.Uint64
	pageChecksumMarkMisses       atomic.Uint64
	pageChecksumMarkUnsafeSkips  atomic.Uint64
}

type leafPageReadCacheStats struct {
	Hits                            uint64
	Misses                          uint64
	Stores                          uint64
	Evictions                       uint64
	ConflictEvictions               uint64
	CapacityEvictions               uint64
	Entries                         uint64
	Capacity                        uint64
	Buckets                         uint64
	Ways                            uint64
	Bytes                           uint64
	BackingBytes                    uint64
	COWReadPolicyBytes              uint64
	ReadMissAdmissionSkips          uint64
	ReadMissAdmissionCandidateSkips uint64
	ReadMissAdmissionLockSkips      uint64
	ReadMissAdmissionStores         uint64
	WriteAdmissionAttempts          uint64
	WriteAdmissionStores            uint64
	WriteAdmissionSkips             uint64
	WriteAdmissionLockSkips         uint64
	RecordChecksumVerifiedStores    uint64
	PageChecksumVerifiedMarks       uint64
	PageChecksumVerifiedHits        uint64
	PageChecksumUnverifiedHits      uint64
	PageChecksumMarkMisses          uint64
	PageChecksumMarkUnsafeSkips     uint64
}

func newLeafPageReadCache(entries int) *leafPageReadCache {
	return newLeafPageReadCacheWithWriteAdmission(entries, LeafPageReadCacheWriteAdmissionImmediate)
}

func newLeafPageReadCacheWithWriteAdmission(entries int, writeAdmission LeafPageReadCacheWriteAdmissionPolicy) *leafPageReadCache {
	if entries <= 0 {
		return nil
	}
	if writeAdmission != LeafPageReadCacheWriteAdmissionAdaptive {
		writeAdmission = LeafPageReadCacheWriteAdmissionImmediate
	}
	ways := leafPageReadCacheWays
	if entries < ways {
		ways = entries
	}
	bucketCount := (entries + ways - 1) / ways
	c := &leafPageReadCache{
		slots:                make([]leafPageReadCacheSlot, entries),
		buckets:              make([]leafPageReadCacheBucket, bucketCount),
		ways:                 ways,
		writeAdmissionPolicy: writeAdmission,
	}
	for i := range c.slots {
		c.slots[i].owner = c
	}
	return c
}

func (c *leafPageReadCache) store(ptr page.LeafLogPtr, leafPage []byte) {
	c.storeWithRecordChecksumState(ptr, leafPage, false)
}

func (c *leafPageReadCache) storeWrite(ptr page.LeafLogPtr, leafPage []byte) {
	c.storeWriteWithRecordChecksumState(ptr, leafPage, false)
}

func (c *leafPageReadCache) storeWithRecordChecksumState(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || len(leafPage) != page.PageSize {
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	bucketIndex := c.bucketIndex(key)
	bucket := &c.buckets[bucketIndex]
	bucket.mu.Lock()
	result, _ := c.storeInBucketLocked(bucketIndex, key, leafPage, recordChecksumVerified, false)
	c.resetReadMissCandidatesForStoreLocked(bucketIndex, key, result)
	bucket.mu.Unlock()
	c.recordStore(result, key, recordChecksumVerified)
}

func (c *leafPageReadCache) storeWriteWithRecordChecksumState(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified bool) {
	if c == nil {
		return
	}
	if c.writeAdmissionPolicy != LeafPageReadCacheWriteAdmissionAdaptive {
		c.storeWithRecordChecksumState(ptr, leafPage, recordChecksumVerified)
		return
	}
	c.storeWriteAdaptiveWithRecordChecksumState(ptr, leafPage, recordChecksumVerified)
}

func (c *leafPageReadCache) storeWriteAdaptiveWithRecordChecksumState(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || len(leafPage) != page.PageSize {
		return
	}
	if !c.shouldAdmitWriteStoreAdaptive() {
		c.writeAdmissionSkips.Add(1)
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	bucketIndex := c.bucketIndex(key)
	bucket := &c.buckets[bucketIndex]
	if !bucket.mu.TryLock() {
		c.recordWriteAdmissionLockSkip()
		return
	}
	result, ok := c.storeInBucketLocked(bucketIndex, key, leafPage, recordChecksumVerified, true)
	if ok {
		c.resetReadMissCandidatesForStoreLocked(bucketIndex, key, result)
	}
	bucket.mu.Unlock()
	if !ok {
		c.recordWriteAdmissionLockSkip()
		return
	}
	c.writeAdmissionStores.Add(1)
	c.recordStore(result, key, recordChecksumVerified)
}

func (c *leafPageReadCache) shouldAdmitWriteStoreAdaptive() bool {
	attempt := c.writeAdmissionAttempts.Add(1)
	capacity := uint64(len(c.slots))
	if capacity == 0 {
		return false
	}
	if attempt <= capacity {
		return true
	}
	hits := c.totalHits()
	misses := c.misses.Load()
	reads := hits + misses
	if reads >= leafPageReadCacheWriteAdmissionMinReadCount && hits > 0 {
		if misses == 0 || hits >= (misses+leafPageReadCacheWriteAdmissionHitMissRatioDiv-1)/leafPageReadCacheWriteAdmissionHitMissRatioDiv {
			return true
		}
	}
	return attempt&leafPageReadCacheWriteAdmissionSampleMask == 0
}

// storeReadMiss admits read-miss pages only after a repeated miss in the same
// set-associative bucket with a matching fingerprint token in the same admission
// epoch (probabilistic under collisions). This keeps one-off sparse reads from
// copying 4KiB pages into the cache and evicting leaves that are likely to be
// reused during publish/apply.
func (c *leafPageReadCache) storeReadMiss(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified bool) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicy(ptr, leafPage, recordChecksumVerified, true, false, valuelog.COWReadLimits{})
}

type leafPageCOWShapeCert struct {
	shape    valuelog.COWRecordShape
	identity rootpublication.StableIdentity
	valid    bool
}

// storeReadMissWithCOWPolicy admits a read-miss page after repeated-miss
// probation and, when dictionary is true and recordChecksumVerified is true,
// publishes its COWReadLimits certificate under the same slot write lock as
// the copied bytes and recordChecksumVerified flag.
func (c *leafPageReadCache) storeReadMissWithCOWPolicy(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified, dictionary bool, limits valuelog.COWReadLimits) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr, leafPage, recordChecksumVerified, true, dictionary, limits, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeReadMissWithCOWPolicyAndShape(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr, leafPage, recordChecksumVerified, true, dictionary, limits, cert)
}

// storeReadMissNoAlloc follows the same probation, epoch and victim policy,
// but can only reuse page backing that already belongs to the selected slot.
func (c *leafPageReadCache) storeReadMissNoAlloc(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified bool) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr, leafPage, recordChecksumVerified, false, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeReadMissWithAllocationPolicy(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified, allowAllocation bool) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr, leafPage, recordChecksumVerified, allowAllocation, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeReadMissWithAllocationPolicyAndCOWPolicy(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified, allowAllocation, dictionary bool, limits valuelog.COWReadLimits) bool {
	return c.storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr, leafPage, recordChecksumVerified, allowAllocation, dictionary, limits, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeReadMissWithAllocationPolicyAndCOWPolicyAndShape(ptr page.LeafLogPtr, leafPage []byte, recordChecksumVerified, allowAllocation, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) bool {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || len(leafPage) != page.PageSize {
		return false
	}
	if dictionary && !cowReadPolicyAllows(limits, limits) {
		return false
	}
	key := newLeafPageReadCacheKey(ptr)
	bucketIndex := c.bucketIndex(key)
	bucket := &c.buckets[bucketIndex]

	fp := leafPageReadMissFingerprint(key)
	epoch, repeated := bucket.observeReadMissCandidate(fp)
	if !repeated {
		c.readMissAdmissionSkips.Add(1)
		return false
	}

	// Read-miss cache admission is best effort. If this bucket is currently under
	// store contention, skip admission rather than queuing behind a writer in the
	// random-read miss hot path.
	if !bucket.mu.TryLock() {
		c.recordReadMissAdmissionLockSkip()
		return false
	}
	defer bucket.mu.Unlock()

	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(bucketIndex)
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		if !slot.mu.TryRLock() {
			c.recordReadMissAdmissionLockSkip()
			return false
		}
		if slot.valid && slot.key == key {
			shapeOK := !cert.valid || (slot.hasCOWShape && slot.cowIdentity == cert.identity)
			if !dictionary && shapeOK {
				slot.markRecent()
				ok := !recordChecksumVerified || slot.recordChecksumVerified
				slot.mu.RUnlock()
				return ok
			}
			if dictionary && shapeOK && slot.recordChecksumVerified && cowReadPolicyAllows(slot.cowReadPolicy, limits) {
				slot.markRecent()
				slot.mu.RUnlock()
				return true
			}
			slot.mu.RUnlock()
			if !slot.mu.TryLock() {
				c.recordReadMissAdmissionLockSkip()
				return false
			}
			if slot.valid && slot.key == key {
				if !c.disabled.Load() && bytes.Equal(slot.data, leafPage) && (recordChecksumVerified || slot.recordChecksumVerified) {
					if recordChecksumVerified {
						slot.recordChecksumVerified = true
					}
					if dictionary && !cowReadPolicyAllows(slot.cowReadPolicy, limits) {
						slot.cowReadPolicy = limits
					}
					if cert.valid && slot.recordChecksumVerified {
						slot.cowShape = cert.shape
						slot.cowIdentity = cert.identity
						slot.hasCOWShape = true
					}
					slot.markRecent()
					slot.mu.Unlock()
					return true
				}
				slot.mu.Unlock()
				return false
			}
			slot.mu.Unlock()
			continue
		}
		slot.mu.RUnlock()
	}

	if !bucket.readMissCandidateStillCurrent(fp, epoch) {
		// A writer/read race can repurpose this bucket between miss observation and
		// lock acquisition. Candidate mismatches include epoch resets and same-epoch
		// candidate churn under concurrent misses.
		c.readMissAdmissionCandidateSkips.Add(1)
		return false
	}
	if c.disabled.Load() {
		return false
	}
	result, ok := c.storeInBucketWithAllocationPolicyAndCOWPolicyLocked(bucketIndex, key, leafPage, recordChecksumVerified, true, allowAllocation, dictionary, limits, cert)
	if !ok {
		if result.backingUnavailable {
			c.readMissAdmissionSkips.Add(1)
		} else {
			c.recordReadMissAdmissionLockSkip()
		}
		return false
	}
	c.resetReadMissCandidatesForStoreLocked(bucketIndex, key, result)
	c.readMissAdmissionStores.Add(1)
	c.recordStore(result, key, recordChecksumVerified)
	return true
}

type leafPageReadCacheStoreResult struct {
	wasValid           bool
	backingUnavailable bool
	prevKey            leafPageReadCacheKey
}

func (c *leafPageReadCache) storeInBucketLocked(bucketIndex int, key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, nonBlocking bool) (leafPageReadCacheStoreResult, bool) {
	return c.storeInBucketWithAllocationPolicyAndCOWPolicyLocked(bucketIndex, key, leafPage, recordChecksumVerified, nonBlocking, true, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeInBucketWithAllocationPolicyLocked(bucketIndex int, key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, nonBlocking, allowAllocation bool) (leafPageReadCacheStoreResult, bool) {
	return c.storeInBucketWithAllocationPolicyAndCOWPolicyLocked(bucketIndex, key, leafPage, recordChecksumVerified, nonBlocking, allowAllocation, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) storeInBucketWithAllocationPolicyAndCOWPolicyLocked(bucketIndex int, key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, nonBlocking, allowAllocation, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) (leafPageReadCacheStoreResult, bool) {
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(bucketIndex)
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		if !lockLeafPageReadCacheSlot(slot, nonBlocking) {
			return leafPageReadCacheStoreResult{}, false
		}
		if slot.valid && slot.key == key {
			result, ok := slot.storeWithAllocationPolicyAndCOWPolicyLocked(key, leafPage, recordChecksumVerified, allowAllocation, dictionary, limits, cert)
			slot.mu.Unlock()
			return result, ok
		}
		slot.mu.Unlock()
	}

	slot, ok := c.lockVictimSlotLocked(bucketIndex, nonBlocking)
	if !ok {
		return leafPageReadCacheStoreResult{}, false
	}
	result, ok := slot.storeWithAllocationPolicyAndCOWPolicyLocked(key, leafPage, recordChecksumVerified, allowAllocation, dictionary, limits, cert)
	slot.mu.Unlock()
	return result, ok
}

func (c *leafPageReadCache) lockVictimSlotLocked(bucketIndex int, nonBlocking bool) (*leafPageReadCacheSlot, bool) {
	start, end := c.bucketRange(bucketIndex)
	bucket := &c.buckets[bucketIndex]
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != 0 {
			continue
		}
		if !lockLeafPageReadCacheSlot(slot, nonBlocking) {
			continue
		}
		if !slot.valid {
			bucket.clockHand = uint32((i - start + 1) % (end - start))
			return slot, true
		}
		slot.mu.Unlock()
	}

	n := end - start
	if n <= 0 {
		return nil, false
	}
	hand := int(bucket.clockHand % uint32(n))
	for pass := 0; pass < 2; pass++ {
		firstVerifiedIdx := -1
		for step := 0; step < n; step++ {
			idx := start + (hand+step)%n
			slot := &c.slots[idx]
			if pass == 0 && slot.recent.Swap(false) {
				continue
			}
			if !lockLeafPageReadCacheSlot(slot, nonBlocking) {
				continue
			}
			if !slot.recordChecksumVerified {
				bucket.clockHand = uint32((idx - start + 1) % n)
				return slot, true
			}
			slot.mu.Unlock()
			if firstVerifiedIdx < 0 {
				firstVerifiedIdx = idx
			}
		}
		if firstVerifiedIdx >= 0 {
			slot := &c.slots[firstVerifiedIdx]
			if lockLeafPageReadCacheSlot(slot, nonBlocking) {
				bucket.clockHand = uint32((firstVerifiedIdx - start + 1) % n)
				return slot, true
			}
		}
	}

	if nonBlocking {
		for step := 0; step < n; step++ {
			idx := start + (hand+step)%n
			slot := &c.slots[idx]
			if !slot.mu.TryLock() {
				continue
			}
			bucket.clockHand = uint32((idx - start + 1) % n)
			return slot, true
		}
		return nil, false
	}

	idx := start + hand
	slot := &c.slots[idx]
	slot.mu.Lock()
	bucket.clockHand = uint32((hand + 1) % n)
	return slot, true
}

func lockLeafPageReadCacheSlot(slot *leafPageReadCacheSlot, nonBlocking bool) bool {
	if nonBlocking {
		return slot.mu.TryLock()
	}
	slot.mu.Lock()
	return true
}

func (s *leafPageReadCacheSlot) storeLocked(key leafPageReadCacheKey, leafPage []byte) leafPageReadCacheStoreResult {
	return s.storeLockedWithRecordChecksumState(key, leafPage, false)
}

func (s *leafPageReadCacheSlot) storeLockedWithRecordChecksumState(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified bool) leafPageReadCacheStoreResult {
	return s.storeLockedWithRecordChecksumAndCOWPolicy(key, leafPage, recordChecksumVerified, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) allocBackingPage() []byte {
	if c == nil {
		return make([]byte, page.PageSize)
	}
	c.slabMu.Lock()
	if c.slabPos+page.PageSize > len(c.slab) {
		slabPages := leafPageReadCacheBackingSlabPages
		if len(c.slots) > 0 && len(c.slots) < slabPages {
			slabPages = len(c.slots)
		}
		if slabPages < 1 {
			slabPages = 1
		}
		c.slab = make([]byte, slabPages*page.PageSize)
		c.slabPos = 0
	}
	off := c.slabPos
	c.slabPos += page.PageSize
	buf := c.slab[off : off+page.PageSize : off+page.PageSize]
	c.slabMu.Unlock()
	return buf
}

func (s *leafPageReadCacheSlot) storeLockedWithRecordChecksumAndCOWPolicy(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) leafPageReadCacheStoreResult {
	if cap(s.data) < page.PageSize {
		if s.owner != nil {
			s.data = s.owner.allocBackingPage()
			s.owner.backingBytes.Add(page.PageSize)
		} else {
			s.data = make([]byte, page.PageSize)
		}
	}
	return s.storeIntoBackingWithCOWPolicyLocked(key, leafPage, recordChecksumVerified, dictionary, limits, cert)
}

func (s *leafPageReadCacheSlot) storeWithAllocationPolicyLocked(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, allowAllocation bool) (leafPageReadCacheStoreResult, bool) {
	return s.storeWithAllocationPolicyAndCOWPolicyLocked(key, leafPage, recordChecksumVerified, allowAllocation, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (s *leafPageReadCacheSlot) storeWithAllocationPolicyAndCOWPolicyLocked(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, allowAllocation, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) (leafPageReadCacheStoreResult, bool) {
	if allowAllocation {
		return s.storeLockedWithRecordChecksumAndCOWPolicy(key, leafPage, recordChecksumVerified, dictionary, limits, cert), true
	}
	if cap(s.data) < page.PageSize {
		return leafPageReadCacheStoreResult{backingUnavailable: true}, false
	}
	return s.storeIntoBackingWithCOWPolicyLocked(key, leafPage, recordChecksumVerified, dictionary, limits, cert), true
}

// The caller holds the slot lock and has established sufficient existing
// backing. Keep the no-allocation copy separate from the allocating wrapper.
func (s *leafPageReadCacheSlot) storeIntoBackingLocked(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified bool) leafPageReadCacheStoreResult {
	return s.storeIntoBackingWithCOWPolicyLocked(key, leafPage, recordChecksumVerified, false, valuelog.COWReadLimits{}, leafPageCOWShapeCert{})
}

func (s *leafPageReadCacheSlot) storeIntoBackingWithCOWPolicyLocked(key leafPageReadCacheKey, leafPage []byte, recordChecksumVerified, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) leafPageReadCacheStoreResult {
	result := leafPageReadCacheStoreResult{
		wasValid: s.valid,
		prevKey:  s.key,
	}
	s.data = s.data[:page.PageSize]
	copy(s.data, leafPage)
	s.key = key
	s.valid = true
	s.recordChecksumVerified = recordChecksumVerified
	if dictionary && recordChecksumVerified && (s.owner == nil || !s.owner.disabled.Load()) && cowReadPolicyAllows(limits, limits) {
		s.cowReadPolicy = limits
	} else {
		s.cowReadPolicy = valuelog.COWReadLimits{}
	}
	if cert.valid && recordChecksumVerified && (s.owner == nil || !s.owner.disabled.Load()) {
		s.cowShape = cert.shape
		s.cowIdentity = cert.identity
		s.hasCOWShape = true
	} else {
		s.cowShape = valuelog.COWRecordShape{}
		s.cowIdentity = rootpublication.StableIdentity{}
		s.hasCOWShape = false
	}
	s.pageChecksumVerified.Store(false)
	s.copiedPageChecksumVerified.Store(false)
	s.recent.Store(true)
	s.keyFP.Store(leafPageReadCacheKeyFingerprint(key))
	return result
}

// observeReadMissCandidate implements the read-miss admission state machine for
// one cache-bucket candidate lane:
//   - odd lane epoch values mean reset in progress (lane unstable),
//   - even values mean stable epoch (candidate token can be observed/published),
//   - repeated=true is only returned when the token matches in a stable epoch
//     without crossing into a newer stable epoch mid-observation.
//
// Each bucket has one candidate lane per cache way. That keeps read-miss
// admission non-blocking while avoiding the old direct-map behavior where every
// key colliding in a slot constantly overwrote one shared probation token.
// The multiple epoch reads fence reset races; bounded retry counters ensure this
// path eventually gives up and reports non-repeated under heavy contention.
func (b *leafPageReadCacheBucket) observeReadMissCandidate(fp uint64) (epoch uint64, repeated bool) {
	return b.readMissCandidateLane(fp).observeReadMissCandidate(fp)
}

func (l *leafPageReadMissAdmissionLane) observeReadMissCandidate(fp uint64) (epoch uint64, repeated bool) {
	totalSpins := 0
	oddEpochSpins := 0
	stableEpochSpins := 0
	lastStableEpoch := uint64(0)
	haveStableEpoch := false
	observedEpochAdvance := false
	for {
		totalSpins++
		if totalSpins >= maxReadMissTotalSpins {
			return epoch, false
		}
		epoch = l.epoch.Load()
		if epoch&1 != 0 {
			// Writer is resetting this lane's admission candidate.
			oddEpochSpins++
			if oddEpochSpins >= maxReadMissOddEpochSpins {
				return epoch, false
			}
			runtime.Gosched()
			continue
		}
		oddEpochSpins = 0
		if !haveStableEpoch {
			lastStableEpoch = epoch
			haveStableEpoch = true
		} else if epoch != lastStableEpoch {
			// Crossing into a new stable epoch means this observer raced a reset.
			// Treat subsequent matches as first-miss observations in the new epoch.
			lastStableEpoch = epoch
			stableEpochSpins = 0
			observedEpochAdvance = true
		}
		candidateToken := readMissCandidateToken(fp, epoch)
		candidate := l.candidateFP.Load()
		if candidate == candidateToken {
			if l.epoch.Load() == epoch {
				if observedEpochAdvance {
					return epoch, false
				}
				return epoch, true
			}
		} else if l.epoch.Load() == epoch {
			if observedEpochAdvance {
				return epoch, false
			}
			if l.candidateFP.CompareAndSwap(candidate, candidateToken) {
				if l.epoch.Load() == epoch {
					return epoch, false
				}
				// Epoch-scoped tokens fence stale publishes: even if this CAS raced
				// with reset and landed in a newer epoch, token mismatch prevents
				// first-miss admission in the newer epoch.
			}
		}
		stableEpochSpins++
		if stableEpochSpins >= maxReadMissStableEpochSpins {
			return epoch, false
		}
		if stableEpochSpins&15 == 0 {
			runtime.Gosched()
		}
	}
}

func (b *leafPageReadCacheBucket) readMissCandidateStillCurrent(fp, epoch uint64) bool {
	return b.readMissCandidateLane(fp).readMissCandidateStillCurrent(fp, epoch)
}

func (l *leafPageReadMissAdmissionLane) readMissCandidateStillCurrent(fp, epoch uint64) bool {
	if epoch&1 != 0 {
		return false
	}
	return l.epoch.Load() == epoch && l.candidateFP.Load() == readMissCandidateToken(fp, epoch)
}

func (b *leafPageReadCacheBucket) readMissCandidateLane(fp uint64) *leafPageReadMissAdmissionLane {
	return &b.readMissCandidates[leafPageReadMissCandidateLaneIndex(fp)]
}

func leafPageReadMissCandidateLaneIndex(fp uint64) int {
	return int(fp % leafPageReadCacheWays)
}

func (b *leafPageReadCacheBucket) resetReadMissCandidateLocked() {
	for i := range b.readMissCandidates {
		b.readMissCandidates[i].resetReadMissCandidateLocked()
	}
}

func (b *leafPageReadCacheBucket) resetReadMissCandidateFingerprintLocked(fp uint64) {
	b.readMissCandidateLane(fp).resetReadMissCandidateLocked()
}

func (l *leafPageReadMissAdmissionLane) resetReadMissCandidateLocked() {
	// Mark reset in-progress, clear candidate, then publish the next stable epoch.
	// observeReadMissCandidate() only accepts even (stable) epochs.
	start := l.epoch.Load()
	if start&1 != 0 {
		start++
	}
	l.epoch.Store(start + 1)
	l.candidateFP.Store(0)
	l.epoch.Store(start + 2)
}

func (c *leafPageReadCache) resetReadMissCandidatesForStoreLocked(bucketIndex int, key leafPageReadCacheKey, result leafPageReadCacheStoreResult) {
	bucket := &c.buckets[bucketIndex]
	if result.wasValid && result.prevKey != key {
		bucket.resetReadMissCandidateLocked()
		return
	}
	bucket.resetReadMissCandidateFingerprintLocked(leafPageReadMissFingerprint(key))
	if !result.wasValid && !c.bucketHasInvalidLocked(bucketIndex) {
		bucket.resetReadMissCandidateLocked()
	}
}

func (c *leafPageReadCache) bucketHasInvalidLocked(bucketIndex int) bool {
	start, end := c.bucketRange(bucketIndex)
	for i := start; i < end; i++ {
		if !c.slots[i].valid {
			return true
		}
	}
	return false
}

func (c *leafPageReadCache) recordStore(result leafPageReadCacheStoreResult, key leafPageReadCacheKey, recordChecksumVerified bool) {
	c.stores.Add(1)
	if recordChecksumVerified {
		c.recordChecksumVerifiedStores.Add(1)
	}
	switch {
	case !result.wasValid:
		c.entries.Add(1)
	case result.prevKey != key:
		c.evictions.Add(1)
		if c.entries.Load() < uint64(len(c.slots)) {
			c.conflictEvictions.Add(1)
		} else {
			c.capacityEvictions.Add(1)
		}
	}
}

func (c *leafPageReadCache) recordReadMissAdmissionLockSkip() {
	c.readMissAdmissionSkips.Add(1)
	c.readMissAdmissionLockSkips.Add(1)
}

func (c *leafPageReadCache) recordWriteAdmissionLockSkip() {
	c.writeAdmissionSkips.Add(1)
	c.writeAdmissionLockSkips.Add(1)
}

func (c *leafPageReadCache) get(ptr page.LeafLogPtr) ([]byte, bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 {
		return nil, false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		slot.mu.RLock()
		if !slot.valid || slot.key != key {
			slot.mu.RUnlock()
			continue
		}
		state := leafPageReadCacheState{RecordChecksumVerified: slot.recordChecksumVerified, CacheEntryPresent: true, PageChecksumVerified: slot.pageChecksumVerified.Load()}
		data := cloneLeafPageReadCacheData(slot.data)
		slot.markRecent()
		slot.mu.RUnlock()
		c.recordHitState(keyFP, state)
		return data, true
	}
	c.misses.Add(1)
	return nil, false
}

func (c *leafPageReadCache) getTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, bool) {
	data, usedDst, _, ok := c.getToWithState(ptr, dst)
	return data, usedDst, ok
}

type leafPageReadCacheState struct {
	RecordChecksumVerified bool
	CacheEntryPresent      bool
	PageChecksumVerified   bool
}

func (c *leafPageReadCache) getToWithState(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, leafPageReadCacheState, bool) {
	return c.getToWithRecordPolicy(ptr, dst, false)
}

// A bounded COW read may only reuse bytes whose record CRC was checked. Unlike
// ordinary tree reads it cannot accept write-origin or unchecked read entries.
func (c *leafPageReadCache) getToWithRecordPolicy(ptr page.LeafLogPtr, dst []byte, requireVerified bool) ([]byte, bool, leafPageReadCacheState, bool) {
	return c.getToWithCOWPolicy(ptr, dst, requireVerified, valuelog.COWReadLimits{})
}

// A nonzero caller policy additionally requires credit from an actual bounded
// dictionary decode. No canonical-policy exception may bypass that credit.
func (c *leafPageReadCache) getToWithCOWPolicy(ptr page.LeafLogPtr, dst []byte, requireVerified bool, limits valuelog.COWReadLimits) ([]byte, bool, leafPageReadCacheState, bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 {
		return nil, false, leafPageReadCacheState{}, false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		slot.mu.RLock()
		if !slot.valid || slot.key != key {
			slot.mu.RUnlock()
			continue
		}
		if (requireVerified && (c.disabled.Load() || !slot.recordChecksumVerified || cap(dst) < len(slot.data))) ||
			(limits != (valuelog.COWReadLimits{}) && !cowReadPolicyAllows(slot.cowReadPolicy, limits)) {
			slot.mu.RUnlock()
			c.misses.Add(1)
			return nil, false, leafPageReadCacheState{}, false
		}
		state := leafPageReadCacheState{RecordChecksumVerified: slot.recordChecksumVerified, CacheEntryPresent: true}
		if requireVerified {
			state.PageChecksumVerified = slot.copiedPageChecksumVerified.Load()
		} else {
			state.PageChecksumVerified = slot.pageChecksumVerified.Load()
		}
		if cap(dst) >= len(slot.data) {
			out := dst[:len(slot.data)]
			copy(out, slot.data)
			slot.markRecent()
			slot.mu.RUnlock()
			c.recordHitState(keyFP, state)
			return out, true, state, true
		}
		data := cloneLeafPageReadCacheData(slot.data)
		slot.markRecent()
		slot.mu.RUnlock()
		c.recordHitState(keyFP, state)
		return data, false, state, true
	}
	c.misses.Add(1)
	return nil, false, leafPageReadCacheState{}, false
}

// markCopiedPageChecksumVerified follows CRC and type validation of a bounded
// copy. Publish only if the current slot still owns those exact verified bytes.
func (c *leafPageReadCache) markCopiedPageChecksumVerified(ptr page.LeafLogPtr, verifiedPage []byte) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || len(verifiedPage) != page.PageSize {
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		if !slot.mu.TryRLock() {
			return
		}
		if slot.valid && slot.key == key {
			if slot.recordChecksumVerified && !c.disabled.Load() && bytes.Equal(slot.data, verifiedPage) {
				slot.copiedPageChecksumVerified.Store(true)
				slot.MarkLeafLogPageViewChecksumVerified()
			}
			slot.mu.RUnlock()
			return
		}
		slot.mu.RUnlock()
	}
}

// cowReadPolicyAllows keeps one complete certificate; incomparable policies
// must never be synthesized from successful reads with different limits.
func cowReadPolicyAllows(credit, caller valuelog.COWReadLimits) bool {
	return credit.MaxRecordBytes != 0 && credit.MaxRawBytes != 0 && credit.MaxValueBytes != 0 &&
		caller.MaxRecordBytes >= credit.MaxRecordBytes && caller.MaxRawBytes >= credit.MaxRawBytes && caller.MaxValueBytes >= credit.MaxValueBytes
}

// markCOWReadPolicy follows a successful full CRC-verified dictionary decode.
// An intervening overwrite can only lose credit: it must match both the exact
// key and copied bytes, and the slot must still have record verification.
func (c *leafPageReadCache) markCOWReadPolicy(ptr page.LeafLogPtr, verifiedPage []byte, limits valuelog.COWReadLimits) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || !cowReadPolicyAllows(limits, limits) {
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	fp := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != fp {
			continue
		}
		if !slot.mu.TryLock() {
			return
		}
		if slot.valid && slot.key == key && slot.recordChecksumVerified && !c.disabled.Load() && bytes.Equal(slot.data, verifiedPage) {
			// Keep an existing stricter certificate; otherwise retain this actual one.
			if !cowReadPolicyAllows(slot.cowReadPolicy, limits) {
				slot.cowReadPolicy = limits
			}
			slot.mu.Unlock()
			return
		}
		slot.mu.Unlock()
	}
}

// markRecordChecksumVerified upgrades only an existing byte-identical entry
// after the original reader has fully verified and decoded the same record.
// A concurrent eviction/rewrite or maintenance bypass simply loses the upgrade.
// No slot/backing is created, and page-checksum state remains independent.
func (c *leafPageReadCache) markRecordChecksumVerified(ptr page.LeafLogPtr, verifiedPage []byte) {
	c.markRecordChecksumVerifiedWithLockPolicy(ptr, verifiedPage, false)
}

func (c *leafPageReadCache) markRecordChecksumVerifiedWithLockPolicy(ptr page.LeafLogPtr, verifiedPage []byte, nonBlocking bool) bool {
	return c.markRecordChecksumVerifiedAndCOWPolicyWithLockPolicy(ptr, verifiedPage, nonBlocking, false, valuelog.COWReadLimits{})
}

// markRecordChecksumVerifiedAndCOWPolicyWithLockPolicy upgrades an existing
// byte-identical entry under a single slot write lock and bytes.Equal check,
// setting recordChecksumVerified and (when dictionary is true) crediting
// limits unless the slot already holds a stricter certificate.
func (c *leafPageReadCache) markRecordChecksumVerifiedAndCOWPolicyWithLockPolicy(ptr page.LeafLogPtr, verifiedPage []byte, nonBlocking, dictionary bool, limits valuelog.COWReadLimits) bool {
	return c.markRecordChecksumVerifiedAndCOWPolicyWithShape(ptr, verifiedPage, nonBlocking, dictionary, limits, leafPageCOWShapeCert{})
}

func (c *leafPageReadCache) markRecordChecksumVerifiedAndCOWPolicyWithShape(ptr page.LeafLogPtr, verifiedPage []byte, nonBlocking, dictionary bool, limits valuelog.COWReadLimits, cert leafPageCOWShapeCert) bool {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || len(verifiedPage) != page.PageSize {
		return false
	}
	if dictionary && !cowReadPolicyAllows(limits, limits) {
		return false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		if !lockLeafPageReadCacheSlot(slot, nonBlocking) {
			return false
		}
		if slot.valid && slot.key == key && !c.disabled.Load() && bytes.Equal(slot.data, verifiedPage) {
			slot.recordChecksumVerified = true
			if dictionary && !cowReadPolicyAllows(slot.cowReadPolicy, limits) {
				slot.cowReadPolicy = limits
			}
			if cert.valid {
				slot.cowShape = cert.shape
				slot.cowIdentity = cert.identity
				slot.hasCOWShape = true
			}
			slot.mu.Unlock()
			return true
		}
		slot.mu.Unlock()
	}
	return false
}

// getVerifiedCOWViewLocked acquires a zero-copy read-locked view of a verified
// COW leaf page when its cached immutable shape and segment identity satisfy
// file/ptr/limits checks, dictionary decode credit (when DictID != 0), and
// caller codec/capability policy. On success, the caller MUST call
// slot.ReleaseLeafLogPageView() after finishing all reads from data. Any
// mismatch returns ok=false without recording a miss so the caller falls back
// to full inspection and canonical error reporting.
func (c *leafPageReadCache) getVerifiedCOWViewLocked(
	ptr page.LeafLogPtr,
	identity rootpublication.StableIdentity,
	file *valuelog.File,
	limits valuelog.COWReadLimits,
	checkShape func(valuelog.COWRecordShape) bool,
) ([]byte, *leafPageReadCacheSlot, leafPageReadCacheState, bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 || file == nil {
		return nil, nil, leafPageReadCacheState{}, false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		slot.mu.RLock()
		if !slot.valid || slot.key != key {
			slot.mu.RUnlock()
			continue
		}
		if c.disabled.Load() || !slot.recordChecksumVerified || !slot.hasCOWShape || slot.cowIdentity != identity {
			slot.mu.RUnlock()
			return nil, nil, leafPageReadCacheState{}, false
		}
		shape := slot.cowShape
		if !file.ValidateCachedCOWLeafShape(ptr.ValuePtr(), shape, limits) {
			slot.mu.RUnlock()
			return nil, nil, leafPageReadCacheState{}, false
		}
		if checkShape != nil && !checkShape(shape) {
			slot.mu.RUnlock()
			return nil, nil, leafPageReadCacheState{}, false
		}
		var state leafPageReadCacheState
		if shape.DictID != 0 {
			if !cowReadPolicyAllows(limits, limits) || !cowReadPolicyAllows(slot.cowReadPolicy, limits) {
				slot.mu.RUnlock()
				return nil, nil, leafPageReadCacheState{}, false
			}
			// Dictionary leaves never carry a reusable page-checksum proof.
			state = leafPageReadCacheState{}
		} else {
			state = leafPageReadCacheState{
				RecordChecksumVerified: true,
				CacheEntryPresent:      true,
				PageChecksumVerified:   slot.copiedPageChecksumVerified.Load(),
			}
		}
		data := slot.data
		slot.markRecent()
		c.recordHitState(keyFP, state)
		return data, slot, state, true
	}
	return nil, nil, leafPageReadCacheState{}, false
}

func (c *leafPageReadCache) getViewLocked(ptr page.LeafLogPtr) ([]byte, *leafPageReadCacheSlot, bool) {
	data, slot, _, ok := c.getViewLockedWithState(ptr)
	return data, slot, ok
}

func (c *leafPageReadCache) getViewLockedWithState(ptr page.LeafLogPtr) ([]byte, *leafPageReadCacheSlot, leafPageReadCacheState, bool) {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 {
		return nil, nil, leafPageReadCacheState{}, false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		slot.mu.RLock()
		if !slot.valid || slot.key != key {
			slot.mu.RUnlock()
			continue
		}
		state := leafPageReadCacheState{RecordChecksumVerified: slot.recordChecksumVerified, CacheEntryPresent: true, PageChecksumVerified: slot.pageChecksumVerified.Load()}
		data := slot.data
		slot.markRecent()
		c.recordHitState(keyFP, state)
		return data, slot, state, true
	}
	return nil, nil, leafPageReadCacheState{}, false
}

func (c *leafPageReadCache) recordHitState(keyFP uint64, state leafPageReadCacheState) {
	stripe := &c.hitStripes[keyFP&(leafPageReadCacheHitStripes-1)]
	if state.PageChecksumVerified {
		stripe.pageChecksumVerifiedHits.Add(1)
	} else {
		stripe.pageChecksumUnverifiedHits.Add(1)
	}
}

func (c *leafPageReadCache) markPageChecksumVerified(ptr page.LeafLogPtr) bool {
	if c == nil || c.disabled.Load() || len(c.slots) == 0 || len(c.buckets) == 0 {
		return false
	}
	key := newLeafPageReadCacheKey(ptr)
	keyFP := leafPageReadCacheKeyFingerprint(key)
	start, end := c.bucketRange(c.bucketIndex(key))
	for i := start; i < end; i++ {
		slot := &c.slots[i]
		if slot.keyFP.Load() != keyFP {
			continue
		}
		slot.mu.Lock()
		if !slot.valid || slot.key != key {
			slot.mu.Unlock()
			continue
		}
		if !slot.recordChecksumVerified {
			slot.mu.Unlock()
			c.pageChecksumMarkUnsafeSkips.Add(1)
			return false
		}
		if slot.pageChecksumVerified.CompareAndSwap(false, true) {
			c.pageChecksumVerifiedMarks.Add(1)
		}
		slot.mu.Unlock()
		return true
	}
	c.pageChecksumMarkMisses.Add(1)
	return false
}

func (c *leafPageReadCache) hitCounts() (verifiedHits, unverifiedHits uint64) {
	for i := range c.hitStripes {
		verifiedHits += c.hitStripes[i].pageChecksumVerifiedHits.Load()
		unverifiedHits += c.hitStripes[i].pageChecksumUnverifiedHits.Load()
	}
	return verifiedHits, unverifiedHits
}

func (c *leafPageReadCache) totalHits() uint64 {
	verifiedHits, unverifiedHits := c.hitCounts()
	return verifiedHits + unverifiedHits
}

func (c *leafPageReadCache) stats() leafPageReadCacheStats {
	if c == nil || len(c.slots) == 0 || len(c.buckets) == 0 {
		return leafPageReadCacheStats{}
	}
	entries := c.entries.Load()
	verifiedHits, unverifiedHits := c.hitCounts()
	return leafPageReadCacheStats{
		Hits:                            verifiedHits + unverifiedHits,
		Misses:                          c.misses.Load(),
		Stores:                          c.stores.Load(),
		Evictions:                       c.evictions.Load(),
		ConflictEvictions:               c.conflictEvictions.Load(),
		CapacityEvictions:               c.capacityEvictions.Load(),
		Entries:                         entries,
		Capacity:                        uint64(len(c.slots)),
		Buckets:                         uint64(len(c.buckets)),
		Ways:                            uint64(c.ways),
		Bytes:                           entries * page.PageSize,
		BackingBytes:                    c.backingBytes.Load(),
		COWReadPolicyBytes:              uint64(len(c.slots)) * uint64(unsafe.Sizeof(valuelog.COWReadLimits{})),
		ReadMissAdmissionSkips:          c.readMissAdmissionSkips.Load(),
		ReadMissAdmissionCandidateSkips: c.readMissAdmissionCandidateSkips.Load(),
		ReadMissAdmissionLockSkips:      c.readMissAdmissionLockSkips.Load(),
		ReadMissAdmissionStores:         c.readMissAdmissionStores.Load(),
		WriteAdmissionAttempts:          c.writeAdmissionAttempts.Load(),
		WriteAdmissionStores:            c.writeAdmissionStores.Load(),
		WriteAdmissionSkips:             c.writeAdmissionSkips.Load(),
		WriteAdmissionLockSkips:         c.writeAdmissionLockSkips.Load(),
		RecordChecksumVerifiedStores:    c.recordChecksumVerifiedStores.Load(),
		PageChecksumVerifiedMarks:       c.pageChecksumVerifiedMarks.Load(),
		PageChecksumVerifiedHits:        verifiedHits,
		PageChecksumUnverifiedHits:      unverifiedHits,
		PageChecksumMarkMisses:          c.pageChecksumMarkMisses.Load(),
		PageChecksumMarkUnsafeSkips:     c.pageChecksumMarkUnsafeSkips.Load(),
	}
}

func (c *leafPageReadCache) bucketRange(bucketIndex int) (int, int) {
	start := bucketIndex * c.ways
	end := start + c.ways
	if end > len(c.slots) {
		end = len(c.slots)
	}
	return start, end
}

func (c *leafPageReadCache) bucketIndex(key leafPageReadCacheKey) int {
	return int(leafPageReadCacheHash(key) % uint64(len(c.buckets)))
}

func leafPageReadCacheHash(key leafPageReadCacheKey) uint64 {
	h := uint64(key.fileID)
	h ^= key.offset + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h ^= uint64(key.subIndex) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	// Avalanche high bits into low bits so aligned offsets (e.g. 4 KiB pages
	// or fixed-stride leaf-log records within one file) distribute uniformly
	// across power-of-2 buckets.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

func leafPageReadCacheKeyFingerprint(key leafPageReadCacheKey) uint64 {
	h := leafPageReadCacheHash(key)
	if h == 0 {
		return 1
	}
	return h
}

func leafPageReadMissFingerprint(key leafPageReadCacheKey) uint64 {
	// Admission is intentionally probabilistic under collisions, so keep this
	// hash stable but well-mixed (SplitMix64-style finalizer constants) to reduce
	// accidental first-miss admissions when unrelated keys map to the same bucket.
	h := uint64(key.fileID)*0x9e3779b97f4a7c15 + key.offset*0xbf58476d1ce4e5b9
	h ^= uint64(key.subIndex) * 0x94d049bb133111eb
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	if h == 0 {
		return 1
	}
	return h
}

func readMissCandidateToken(fp, epoch uint64) uint64 {
	token := fp ^ (epoch * 0x9e3779b97f4a7c15)
	if token == 0 {
		// Keep zero reserved for "no candidate recorded" without collapsing every
		// zero-token case onto a single sentinel value.
		token = (fp << 1) ^ (epoch >> 1) ^ 0x9e3779b97f4a7c15
		if token == 0 {
			token = 0x9e3779b97f4a7c15
		}
	}
	return token
}

type cachedLeafPageReader struct {
	cache    *leafPageReadCache
	fallback interface {
		ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
	}
}

func newCachedLeafPageReader(cache *leafPageReadCache, fallback interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}) *cachedLeafPageReader {
	return &cachedLeafPageReader{cache: cache, fallback: fallback}
}

func (db *DB) leafPageReader(fallback zipper.LeafPageReader) zipper.LeafPageReader {
	if db != nil && db.leafPageReadCache != nil {
		return newCachedLeafPageReader(db.leafPageReadCache, fallback)
	}
	return fallback
}

func (db *DB) disableLeafPageReadCacheForMaintenance() func() {
	if db == nil || db.leafPageReadCache == nil {
		return func() {}
	}
	prev := db.leafPageReadCache.disabled.Swap(true)
	return func() {
		db.leafPageReadCache.disabled.Store(prev)
	}
}

func (r *cachedLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	var key page.LeafLogPtr
	var keyOK bool
	if k, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
		key = k
		keyOK = true
		if data, ok := r.cache.get(key); ok {
			return data, nil
		}
	}
	data, err := r.fallback.ReadUnsafe(ptr)
	if err != nil {
		return nil, err
	}
	r.storeReadMiss(key, keyOK, data)
	return data, nil
}

func (r *cachedLeafPageReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	data, usedDst, _, err := r.ReadUnsafeToWithCacheHit(ptr, dst)
	return data, usedDst, err
}

func (r *cachedLeafPageReader) ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error) {
	var key page.LeafLogPtr
	var keyOK bool
	if k, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
		key = k
		keyOK = true
		if data, usedDst, ok := r.cache.getTo(key, dst); ok {
			return data, usedDst, true, nil
		}
	}
	if toReader, ok := r.fallback.(interface {
		ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
	}); ok {
		data, usedDst, err := toReader.ReadUnsafeTo(ptr, dst)
		if err != nil {
			return nil, false, false, err
		}
		r.storeReadMiss(key, keyOK, data)
		return data, usedDst, false, nil
	}
	data, err := r.fallback.ReadUnsafe(ptr)
	if err != nil {
		return nil, false, false, err
	}
	r.storeReadMiss(key, keyOK, data)
	return data, false, false, nil
}

func (r *cachedLeafPageReader) storeReadMiss(key page.LeafLogPtr, keyOK bool, leafPage []byte) {
	if r == nil || r.cache == nil || !keyOK || len(leafPage) != page.PageSize {
		return
	}
	recordChecksumVerified := false
	if cap, ok := r.fallback.(readChecksumCapability); ok {
		recordChecksumVerified = cap.ReadChecksumEnabled()
	}
	r.cache.storeReadMiss(key, leafPage, recordChecksumVerified)
}

func cloneLeafPageReadCacheData(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	owned := make([]byte, len(data))
	copy(owned, data)
	return owned
}

func (c *leafPageReadCache) writeAdmissionPolicyName() string {
	if c == nil || len(c.slots) == 0 || len(c.buckets) == 0 {
		return "disabled"
	}
	return c.writeAdmissionPolicy.String()
}

func (db *DB) storeLeafPageReadCache(ptr page.LeafLogPtr, leafPage []byte) {
	if db == nil {
		return
	}
	db.leafPageReadCache.storeWrite(ptr, leafPage)
}
