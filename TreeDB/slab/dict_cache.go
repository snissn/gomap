package slab

import (
	"container/list"
	"sync"
)

type dictCacheKey struct {
	slab   *SlabFile
	zoneID uint32
}

type dictPoolEntry struct {
	key  dictCacheKey
	pool *sync.Pool
}

type dictPoolCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[dictCacheKey]*list.Element
}

const defaultDictPoolCacheSize = 128

var dictPools = newDictPoolCache(defaultDictPoolCacheSize)

func newDictPoolCache(capacity int) *dictPoolCache {
	if capacity < 0 {
		capacity = 0
	}
	return &dictPoolCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[dictCacheKey]*list.Element),
	}
}

func (c *dictPoolCache) get(key dictCacheKey) (*sync.Pool, bool) {
	if c == nil || c.cap == 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*dictPoolEntry).pool, true
	}
	return nil, false
}

func (c *dictPoolCache) getOrAdd(key dictCacheKey, pool *sync.Pool) *sync.Pool {
	if c == nil || c.cap == 0 {
		return pool
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*dictPoolEntry).pool
	}
	elem := c.ll.PushFront(&dictPoolEntry{key: key, pool: pool})
	c.items[key] = elem
	if c.ll.Len() > c.cap {
		tail := c.ll.Back()
		if tail != nil {
			c.ll.Remove(tail)
			entry := tail.Value.(*dictPoolEntry)
			delete(c.items, entry.key)
		}
	}
	return pool
}

func (c *dictPoolCache) keys() []dictCacheKey {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]dictCacheKey, 0, c.ll.Len())
	for elem := c.ll.Front(); elem != nil; elem = elem.Next() {
		keys = append(keys, elem.Value.(*dictPoolEntry).key)
	}
	return keys
}
