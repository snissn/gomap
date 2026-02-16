package db

import "sync"

const (
	outerLeafKeyCacheMultiplier = 8
	outerLeafKeyCacheMaxEntries = 1 << 16
)

type outerLeafKeyCacheShard struct {
	mu       sync.Mutex
	entries  map[outerLeafBlockKey][][]byte
	hits     uint64
	misses   uint64
	capacity int
	admit    []uint64
	admitMask uint64
}

type outerLeafKeyCache struct {
	shards   []outerLeafKeyCacheShard
	capacity int
}

func deriveOuterLeafKeyCacheEntries(blockCacheEntries int) int {
	if blockCacheEntries <= 0 {
		return 0
	}
	capacity := blockCacheEntries * outerLeafKeyCacheMultiplier
	if capacity < blockCacheEntries {
		capacity = blockCacheEntries
	}
	if capacity > outerLeafKeyCacheMaxEntries {
		capacity = outerLeafKeyCacheMaxEntries
	}
	return capacity
}

func newOuterLeafKeyCache(blockCacheEntries int) *outerLeafKeyCache {
	capacity := deriveOuterLeafKeyCacheEntries(blockCacheEntries)
	if capacity <= 0 {
		return nil
	}

	shardCount := 1
	targetShards := capacity / 1024
	if targetShards < 1 {
		targetShards = 1
	}
	for shardCount < targetShards && shardCount < 64 {
		shardCount <<= 1
	}
	if shardCount > capacity {
		shardCount = 1
		for shardCount<<1 <= capacity {
			shardCount <<= 1
		}
	}

	shards := make([]outerLeafKeyCacheShard, shardCount)
	baseCap := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		capI := baseCap
		if i < extra {
			capI++
		}
		admitBits := 0
		if capI > 0 {
			targetBits := capI * 16
			if targetBits < 256 {
				targetBits = 256
			}
			admitBits = 1
			for admitBits < targetBits {
				admitBits <<= 1
			}
		}
		shards[i] = outerLeafKeyCacheShard{
			entries:   make(map[outerLeafBlockKey][][]byte, capI),
			capacity:  capI,
			admit:     make([]uint64, admitBits/64),
			admitMask: uint64(admitBits - 1),
		}
	}
	return &outerLeafKeyCache{
		shards:   shards,
		capacity: capacity,
	}
}

func (c *outerLeafKeyCache) get(key outerLeafBlockKey) [][]byte {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	keys, ok := s.entries[key]
	if !ok {
		s.misses++
		return nil
	}
	s.hits++
	return keys
}

func (c *outerLeafKeyCache) put(key outerLeafBlockKey, keys [][]byte) {
	if len(keys) == 0 {
		return
	}
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[key]; ok {
		s.entries[key] = keys
		return
	}
	if s.capacity <= 0 {
		return
	}
	if len(s.entries) >= s.capacity {
		for evict := range s.entries {
			delete(s.entries, evict)
			break
		}
	}
	if !s.shouldAdmit(key) {
		return
	}
	s.entries[key] = keys
}

func (c *outerLeafKeyCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
	var totalEntries int
	var totalHits uint64
	var totalMisses uint64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		totalHits += s.hits
		totalMisses += s.misses
		totalEntries += len(s.entries)
		s.mu.Unlock()
	}
	return totalHits, totalMisses, totalEntries, c.capacity
}

func (c *outerLeafKeyCache) shardFor(key outerLeafBlockKey) *outerLeafKeyCacheShard {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	h := outerLeafBlockKeyHash(key)
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx]
}

func outerLeafBlockKeyHash(key outerLeafBlockKey) uint64 {
	h := uint64(key.fileID)*11400714819323198485 ^
		key.offset*14029467366897019727 ^
		uint64(key.length)*1609587929392839161
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

func (s *outerLeafKeyCacheShard) shouldAdmit(key outerLeafBlockKey) bool {
	if s == nil || s.capacity <= 0 {
		return false
	}
	if s.capacity <= 64 || len(s.entries) < s.capacity/4 {
		return true
	}
	if len(s.admit) == 0 || s.admitMask == 0 {
		return true
	}
	idx := outerLeafBlockKeyHash(key) & s.admitMask
	word := idx >> 6
	bit := uint64(1) << (idx & 63)
	seen := s.admit[word]&bit != 0
	s.admit[word] |= bit
	return seen
}
