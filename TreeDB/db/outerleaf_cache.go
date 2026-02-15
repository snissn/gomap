package db

import (
	"container/list"
	"sync"

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
	key   outerLeafBlockKey
	block *outerleaf.DecodedBlock
}

type outerLeafBlockCacheShard struct {
	mu       sync.Mutex
	list     *list.List
	entries  map[outerLeafBlockKey]*list.Element
	hits     uint64
	misses   uint64
	capacity int
}

type outerLeafBlockCache struct {
	shards   []outerLeafBlockCacheShard
	capacity int
}

func newOuterLeafBlockCache(capacity int) *outerLeafBlockCache {
	if capacity <= 0 {
		return nil
	}

	// Keep per-shard capacity reasonably sized (target ~256 entries/shard)
	// while bounding lock fanout.
	shardCount := 1
	targetShards := capacity / 256
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

	shards := make([]outerLeafBlockCacheShard, shardCount)
	baseCap := capacity / shardCount
	extra := capacity % shardCount
	for i := range shards {
		capI := baseCap
		if i < extra {
			capI++
		}
		shards[i] = outerLeafBlockCacheShard{
			list:     list.New(),
			entries:  make(map[outerLeafBlockKey]*list.Element, capI),
			capacity: capI,
		}
	}

	return &outerLeafBlockCache{
		shards:   shards,
		capacity: capacity,
	}
}

func (c *outerLeafBlockCache) get(key outerLeafBlockKey) *outerleaf.DecodedBlock {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	elem, ok := s.entries[key]
	if !ok {
		s.misses++
		return nil
	}
	s.hits++
	s.list.MoveToFront(elem)
	return elem.Value.(*outerLeafBlockCacheEntry).block
}

func (c *outerLeafBlockCache) put(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem, ok := s.entries[key]; ok {
		elem.Value.(*outerLeafBlockCacheEntry).block = block
		s.list.MoveToFront(elem)
		return
	}
	entry := &outerLeafBlockCacheEntry{key: key, block: block}
	elem := s.list.PushFront(entry)
	s.entries[key] = elem
	if s.list.Len() > s.capacity {
		tail := s.list.Back()
		if tail != nil {
			s.list.Remove(tail)
			delete(s.entries, tail.Value.(*outerLeafBlockCacheEntry).key)
		}
	}
}

func (c *outerLeafBlockCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
	var totalEntries int
	var totalHits uint64
	var totalMisses uint64
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		totalHits += s.hits
		totalMisses += s.misses
		totalEntries += s.list.Len()
		s.mu.Unlock()
	}
	return totalHits, totalMisses, totalEntries, c.capacity
}

func (c *outerLeafBlockCache) shardFor(key outerLeafBlockKey) *outerLeafBlockCacheShard {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	// Mix file/offset/length using fixed 64-bit odd constants in the style of
	// xxHash/SplitMix so shard selection stays stable and well-distributed.
	h := uint64(key.fileID)*11400714819323198485 ^
		key.offset*14029467366897019727 ^
		uint64(key.length)*1609587929392839161
	h ^= h >> 33
	// Final avalanche constant (SplitMix64 finalizer).
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx]
}
