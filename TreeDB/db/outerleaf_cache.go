package db

import (
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
	prev  int
	next  int
}

type outerLeafBlockCacheShard struct {
	mu       sync.Mutex
	entries  map[outerLeafBlockKey]int
	nodes    []outerLeafBlockCacheEntry
	free     []int
	head     int
	tail     int
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
		free := make([]int, capI)
		nodes := make([]outerLeafBlockCacheEntry, capI)
		for j := 0; j < capI; j++ {
			nodes[j].prev = -1
			nodes[j].next = -1
			free[j] = capI - 1 - j
		}
		shards[i] = outerLeafBlockCacheShard{
			entries:  make(map[outerLeafBlockKey]int, capI),
			nodes:    nodes,
			free:     free,
			head:     -1,
			tail:     -1,
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
	idx, ok := s.entries[key]
	if !ok {
		s.misses++
		return nil
	}
	s.hits++
	s.moveToFront(idx)
	return s.nodes[idx].block
}

func (c *outerLeafBlockCache) put(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if idx, ok := s.entries[key]; ok {
		old := s.nodes[idx].block
		if old != nil && old != block {
			old.Release()
			outerLeafFenceDecodedBlockPut(old)
		}
		s.nodes[idx].block = block
		s.moveToFront(idx)
		return
	}
	if s.capacity <= 0 {
		if block != nil {
			block.Release()
			outerLeafFenceDecodedBlockPut(block)
		}
		return
	}

	var idx int
	if n := len(s.free); n > 0 {
		idx = s.free[n-1]
		s.free = s.free[:n-1]
	} else {
		idx = s.tail
		if idx < 0 {
			if block != nil {
				block.Release()
				outerLeafFenceDecodedBlockPut(block)
			}
			return
		}
		evictedKey := s.nodes[idx].key
		evictedBlock := s.nodes[idx].block
		s.unlink(idx)
		delete(s.entries, evictedKey)
		if evictedBlock != nil && evictedBlock != block {
			evictedBlock.Release()
			outerLeafFenceDecodedBlockPut(evictedBlock)
		}
	}
	node := &s.nodes[idx]
	node.key = key
	node.block = block
	node.prev = -1
	node.next = -1
	s.linkFront(idx)
	s.entries[key] = idx
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
		totalEntries += len(s.entries)
		s.mu.Unlock()
	}
	return totalHits, totalMisses, totalEntries, c.capacity
}

func (c *outerLeafBlockCache) shardFor(key outerLeafBlockKey) *outerLeafBlockCacheShard {
	if len(c.shards) == 1 {
		return &c.shards[0]
	}
	h := uint64(key.fileID)*11400714819323198485 ^
		key.offset*14029467366897019727 ^
		uint64(key.length)*1609587929392839161
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	idx := int(h & uint64(len(c.shards)-1))
	return &c.shards[idx]
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
