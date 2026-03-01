package db

import "sync"

const (
	leafBlockKeyCacheMultiplier = 8
	leafBlockKeyCacheMaxEntries = 1 << 16
)

type leafBlockKeyCacheShard struct {
	mu        sync.Mutex
	entries   map[leafBlockKey][][]byte
	hits      uint64
	misses    uint64
	capacity  int
	admit     []uint64
	admitMask uint64
}

type leafBlockKeyCache struct {
	shards   []leafBlockKeyCacheShard
	capacity int
}

func deriveLeafBlockKeyCacheEntries(blockCacheEntries int) int {
	if blockCacheEntries <= 0 {
		return 0
	}
	capacity := blockCacheEntries * leafBlockKeyCacheMultiplier
	if capacity < blockCacheEntries {
		capacity = blockCacheEntries
	}
	if capacity > leafBlockKeyCacheMaxEntries {
		capacity = leafBlockKeyCacheMaxEntries
	}
	return capacity
}

func newLeafBlockKeyCache(blockCacheEntries int) *leafBlockKeyCache {
	capacity := deriveLeafBlockKeyCacheEntries(blockCacheEntries)
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

	shards := make([]leafBlockKeyCacheShard, shardCount)
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
		shards[i] = leafBlockKeyCacheShard{
			entries:   make(map[leafBlockKey][][]byte, capI),
			capacity:  capI,
			admit:     make([]uint64, admitBits/64),
			admitMask: uint64(admitBits - 1),
		}
	}
	return &leafBlockKeyCache{
		shards:   shards,
		capacity: capacity,
	}
}

func (c *leafBlockKeyCache) get(key leafBlockKey) [][]byte {
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

func (c *leafBlockKeyCache) put(key leafBlockKey, keys [][]byte) {
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

func (c *leafBlockKeyCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
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

func (c *leafBlockKeyCache) shardFor(key leafBlockKey) *leafBlockKeyCacheShard {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	h := leafBlockKeyHash(key)
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx]
}

func leafBlockKeyHash(key leafBlockKey) uint64 {
	h := uint64(key.fileID)*11400714819323198485 ^
		key.offset*14029467366897019727 ^
		uint64(key.length)*1609587929392839161
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	return h
}

func (s *leafBlockKeyCacheShard) shouldAdmit(key leafBlockKey) bool {
	if s == nil || s.capacity <= 0 {
		return false
	}
	if s.capacity <= 64 || len(s.entries) < s.capacity/4 {
		return true
	}
	if len(s.admit) == 0 || s.admitMask == 0 {
		return true
	}
	idx := leafBlockKeyHash(key) & s.admitMask
	word := idx >> 6
	bit := uint64(1) << (idx & 63)
	seen := s.admit[word]&bit != 0
	s.admit[word] |= bit
	return seen
}
