package db

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	leafPageReadCacheEntriesEnvKey  = "TREEDB_LEAF_PAGE_CACHE_ENTRIES"
	defaultLeafPageReadCacheEntries = 4096
	maxLeafPageReadCacheEntries     = 1 << 18
)

var LeafPageReadCacheEntries = defaultLeafPageReadCacheEntries

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
	if raw := strings.TrimSpace(os.Getenv(leafPageReadCacheEntriesEnvKey)); raw != "" {
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
	mu                  sync.RWMutex
	key                 leafPageReadCacheKey
	data                []byte
	valid               bool
	readMissCandidateFP atomic.Uint64
	readMissEpoch       atomic.Uint64
}

type leafPageReadCache struct {
	slots []leafPageReadCacheSlot

	hits      atomic.Uint64
	misses    atomic.Uint64
	stores    atomic.Uint64
	evictions atomic.Uint64
	entries   atomic.Uint64

	readMissAdmissionSkips  atomic.Uint64
	readMissAdmissionStale  atomic.Uint64
	readMissAdmissionStores atomic.Uint64
}

type leafPageReadCacheStats struct {
	Hits                    uint64
	Misses                  uint64
	Stores                  uint64
	Evictions               uint64
	Entries                 uint64
	Capacity                uint64
	Bytes                   uint64
	ReadMissAdmissionSkips  uint64
	ReadMissAdmissionStale  uint64
	ReadMissAdmissionStores uint64
}

func newLeafPageReadCache(entries int) *leafPageReadCache {
	if entries <= 0 {
		return nil
	}
	return &leafPageReadCache{slots: make([]leafPageReadCacheSlot, entries)}
}

func (c *leafPageReadCache) store(ptr page.LeafLogPtr, leafPage []byte) {
	if c == nil || len(c.slots) == 0 || len(leafPage) != page.PageSize {
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	slot := &c.slots[c.slotIndex(key)]
	slot.mu.Lock()
	result := slot.storeLocked(key, leafPage)
	slot.mu.Unlock()
	c.recordStore(result, key)
}

// storeReadMiss admits read-miss pages only after a repeated miss in the same
// direct-mapped slot with a matching fingerprint token in the same admission
// epoch (probabilistic under collisions). Write-side stores remain immediate;
// this keeps one-off sparse reads from copying 4KiB pages into the cache and
// evicting recently-written leaves that are likely to be reused during
// publish/apply.
func (c *leafPageReadCache) storeReadMiss(ptr page.LeafLogPtr, leafPage []byte) {
	if c == nil || len(c.slots) == 0 || len(leafPage) != page.PageSize {
		return
	}
	key := newLeafPageReadCacheKey(ptr)
	slot := &c.slots[c.slotIndex(key)]

	fp := leafPageReadMissFingerprint(key)
	epoch, repeated := slot.observeReadMissCandidate(fp)
	if !repeated {
		c.readMissAdmissionSkips.Add(1)
		return
	}

	// Parallel read misses can race: another goroutine may populate this exact
	// slot/key before we reach admission. Fast-path that case with only a read
	// lock to avoid unnecessary writer lock traffic in the miss hot path.
	slot.mu.RLock()
	if slot.valid && slot.key == key {
		slot.mu.RUnlock()
		return
	}
	slot.mu.RUnlock()

	slot.mu.Lock()
	if slot.valid && slot.key == key {
		slot.mu.Unlock()
		return
	}
	if !slot.readMissCandidateStillCurrent(fp, epoch) {
		// A writer/read admission race can repurpose this slot and reset the
		// candidate between miss observation and lock acquisition. Track epoch
		// resets so a same-key first miss after reset cannot resurrect stale
		// admission.
		slot.mu.Unlock()
		c.readMissAdmissionStale.Add(1)
		return
	}
	result := slot.storeLocked(key, leafPage)
	slot.mu.Unlock()
	c.readMissAdmissionStores.Add(1)
	c.recordStore(result, key)
}

type leafPageReadCacheStoreResult struct {
	wasValid bool
	prevKey  leafPageReadCacheKey
}

func (s *leafPageReadCacheSlot) storeLocked(key leafPageReadCacheKey, leafPage []byte) leafPageReadCacheStoreResult {
	result := leafPageReadCacheStoreResult{
		wasValid: s.valid,
		prevKey:  s.key,
	}
	if cap(s.data) < page.PageSize {
		s.data = make([]byte, page.PageSize)
	}
	s.data = s.data[:page.PageSize]
	copy(s.data, leafPage)
	s.key = key
	s.valid = true
	s.resetReadMissCandidateLocked()
	return result
}

func (s *leafPageReadCacheSlot) observeReadMissCandidate(fp uint64) (epoch uint64, repeated bool) {
	for {
		epoch = s.readMissEpoch.Load()
		if epoch&1 != 0 {
			// Writer is resetting this slot's admission candidate.
			runtime.Gosched()
			continue
		}
		candidateToken := readMissCandidateToken(fp, epoch)
		candidate := s.readMissCandidateFP.Load()
		if candidate == candidateToken {
			if s.readMissEpoch.Load() == epoch {
				return epoch, true
			}
			continue
		}
		if s.readMissEpoch.Load() != epoch {
			continue
		}
		if s.readMissCandidateFP.CompareAndSwap(candidate, candidateToken) {
			if s.readMissEpoch.Load() == epoch {
				return epoch, false
			}
			// Epoch-scoped tokens fence stale publishes: even if this CAS raced with
			// reset and landed in a newer epoch, token mismatch prevents first-miss
			// admission in the newer epoch.
		}
	}
}

func (s *leafPageReadCacheSlot) readMissCandidateStillCurrent(fp, epoch uint64) bool {
	if epoch&1 != 0 {
		return false
	}
	return s.readMissEpoch.Load() == epoch && s.readMissCandidateFP.Load() == readMissCandidateToken(fp, epoch)
}

func (s *leafPageReadCacheSlot) resetReadMissCandidateLocked() {
	// Mark reset in-progress, clear candidate, then publish the next stable epoch.
	// observeReadMissCandidate() only accepts even (stable) epochs.
	start := s.readMissEpoch.Load()
	if start&1 != 0 {
		start++
	}
	s.readMissEpoch.Store(start + 1)
	s.readMissCandidateFP.Store(0)
	s.readMissEpoch.Store(start + 2)
}

func (c *leafPageReadCache) recordStore(result leafPageReadCacheStoreResult, key leafPageReadCacheKey) {
	c.stores.Add(1)
	switch {
	case !result.wasValid:
		c.entries.Add(1)
	case result.prevKey != key:
		c.evictions.Add(1)
	}
}

func (c *leafPageReadCache) get(ptr page.LeafLogPtr) ([]byte, bool) {
	if c == nil || len(c.slots) == 0 {
		return nil, false
	}
	key := newLeafPageReadCacheKey(ptr)
	slot := &c.slots[c.slotIndex(key)]
	slot.mu.RLock()
	if !slot.valid || slot.key != key {
		slot.mu.RUnlock()
		c.misses.Add(1)
		return nil, false
	}
	data := cloneLeafPageReadCacheData(slot.data)
	slot.mu.RUnlock()
	c.hits.Add(1)
	return data, true
}

func (c *leafPageReadCache) getTo(ptr page.LeafLogPtr, dst []byte) ([]byte, bool, bool) {
	if c == nil || len(c.slots) == 0 {
		return nil, false, false
	}
	key := newLeafPageReadCacheKey(ptr)
	slot := &c.slots[c.slotIndex(key)]
	slot.mu.RLock()
	if !slot.valid || slot.key != key {
		slot.mu.RUnlock()
		c.misses.Add(1)
		return nil, false, false
	}
	if cap(dst) >= len(slot.data) {
		out := dst[:len(slot.data)]
		copy(out, slot.data)
		slot.mu.RUnlock()
		c.hits.Add(1)
		return out, true, true
	}
	data := cloneLeafPageReadCacheData(slot.data)
	slot.mu.RUnlock()
	c.hits.Add(1)
	return data, false, true
}

func (c *leafPageReadCache) stats() leafPageReadCacheStats {
	if c == nil || len(c.slots) == 0 {
		return leafPageReadCacheStats{}
	}
	entries := c.entries.Load()
	return leafPageReadCacheStats{
		Hits:                    c.hits.Load(),
		Misses:                  c.misses.Load(),
		Stores:                  c.stores.Load(),
		Evictions:               c.evictions.Load(),
		Entries:                 entries,
		Capacity:                uint64(len(c.slots)),
		Bytes:                   entries * page.PageSize,
		ReadMissAdmissionSkips:  c.readMissAdmissionSkips.Load(),
		ReadMissAdmissionStale:  c.readMissAdmissionStale.Load(),
		ReadMissAdmissionStores: c.readMissAdmissionStores.Load(),
	}
}

func (c *leafPageReadCache) slotIndex(key leafPageReadCacheKey) int {
	h := uint64(key.fileID)
	h ^= key.offset + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	h ^= uint64(key.subIndex) + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
	return int(h % uint64(len(c.slots)))
}

func leafPageReadMissFingerprint(key leafPageReadCacheKey) uint64 {
	// Admission is intentionally probabilistic under collisions, so keep this
	// hash stable but well-mixed (SplitMix64-style finalizer constants) to reduce
	// accidental first-miss admissions when unrelated keys map to the same slot.
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
		// Keep zero reserved for "no candidate recorded".
		return 1
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

func (r *cachedLeafPageReader) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	if key, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
		if data, ok := r.cache.get(key); ok {
			return data, nil
		}
	}
	return r.fallback.ReadUnsafe(ptr)
}

func (r *cachedLeafPageReader) ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error) {
	data, usedDst, _, err := r.ReadUnsafeToWithCacheHit(ptr, dst)
	return data, usedDst, err
}

func (r *cachedLeafPageReader) ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error) {
	if key, err := page.LeafLogPtrFromValuePtr(ptr); err == nil {
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
		return data, usedDst, false, nil
	}
	data, err := r.fallback.ReadUnsafe(ptr)
	if err != nil {
		return nil, false, false, err
	}
	return data, false, false, nil
}

func cloneLeafPageReadCacheData(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	owned := make([]byte, len(data))
	copy(owned, data)
	return owned
}

func (db *DB) storeLeafPageReadCache(ptr page.LeafLogPtr, leafPage []byte) {
	if db == nil {
		return
	}
	db.leafPageReadCache.store(ptr, leafPage)
}
