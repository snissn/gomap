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

type outerLeafBlockCache struct {
	mu       sync.Mutex
	list     *list.List
	entries  map[outerLeafBlockKey]*list.Element
	hits     uint64
	misses   uint64
	capacity int
}

func newOuterLeafBlockCache(capacity int) *outerLeafBlockCache {
	if capacity <= 0 {
		return nil
	}
	return &outerLeafBlockCache{
		list:     list.New(),
		entries:  make(map[outerLeafBlockKey]*list.Element),
		capacity: capacity,
	}
}

func (c *outerLeafBlockCache) get(key outerLeafBlockKey) *outerleaf.DecodedBlock {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.entries[key]
	if !ok {
		c.misses++
		return nil
	}
	c.hits++
	c.list.MoveToFront(elem)
	return elem.Value.(*outerLeafBlockCacheEntry).block
}

func (c *outerLeafBlockCache) put(key outerLeafBlockKey, block *outerleaf.DecodedBlock) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		elem.Value.(*outerLeafBlockCacheEntry).block = block
		c.list.MoveToFront(elem)
		return
	}
	entry := &outerLeafBlockCacheEntry{key: key, block: block}
	elem := c.list.PushFront(entry)
	c.entries[key] = elem
	if c.list.Len() > c.capacity {
		tail := c.list.Back()
		if tail != nil {
			c.list.Remove(tail)
			delete(c.entries, tail.Value.(*outerLeafBlockCacheEntry).key)
		}
	}
}

func (c *outerLeafBlockCache) stats() (hits uint64, misses uint64, entries int, capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.list.Len(), c.capacity
}
