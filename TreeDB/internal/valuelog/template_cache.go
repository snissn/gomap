package valuelog

import (
	"container/list"
	"sync"
	"sync/atomic"

	templ "github.com/snissn/gomap/TreeDB/template"
)

type templateDefCache struct {
	maxEntries int

	mu sync.Mutex
	ll list.List
	m  map[uint64]*list.Element

	hits   atomic.Uint64
	misses atomic.Uint64
}

type templateDefCacheEntry struct {
	id  uint64
	def templ.TemplateDef
}

func newTemplateDefCache(maxEntries int) *templateDefCache {
	if maxEntries <= 0 {
		return nil
	}
	return &templateDefCache{
		maxEntries: maxEntries,
		m:          make(map[uint64]*list.Element, maxEntries),
	}
}

func (c *templateDefCache) Stats() (hits, misses uint64, entries, capacity int) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	entries = len(c.m)
	capacity = c.maxEntries
	c.mu.Unlock()
	return c.hits.Load(), c.misses.Load(), entries, capacity
}

func (c *templateDefCache) Get(id uint64) (templ.TemplateDef, bool) {
	if c == nil || id == 0 {
		return templ.TemplateDef{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.m[id]; elem != nil {
		c.ll.MoveToFront(elem)
		c.hits.Add(1)
		if ent, ok := elem.Value.(*templateDefCacheEntry); ok && ent != nil {
			return ent.def, true
		}
		return templ.TemplateDef{}, true
	}
	c.misses.Add(1)
	return templ.TemplateDef{}, false
}

func (c *templateDefCache) Add(id uint64, def templ.TemplateDef) {
	if c == nil || id == 0 || c.maxEntries <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem := c.m[id]; elem != nil {
		if ent, ok := elem.Value.(*templateDefCacheEntry); ok && ent != nil {
			ent.def = def
		} else {
			elem.Value = &templateDefCacheEntry{id: id, def: def}
		}
		c.ll.MoveToFront(elem)
		return
	}
	elem := c.ll.PushFront(&templateDefCacheEntry{id: id, def: def})
	c.m[id] = elem
	if len(c.m) <= c.maxEntries {
		return
	}
	back := c.ll.Back()
	if back == nil {
		return
	}
	ent, ok := back.Value.(*templateDefCacheEntry)
	if ok && ent != nil {
		delete(c.m, ent.id)
	}
	c.ll.Remove(back)
}
