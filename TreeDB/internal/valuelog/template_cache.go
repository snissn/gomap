package valuelog

import (
	"container/list"
	"sync"
	"sync/atomic"

	templ "github.com/snissn/gomap/TreeDB/template"
)

type paddedAtomicUint64 struct {
	v atomic.Uint64
	_ [56]byte
}

type templateDefCache struct {
	maxEntries int

	mu   sync.Mutex
	ll   list.List
	m    map[uint64]*list.Element
	snap atomic.Pointer[map[uint64]*templateDefCacheEntry]

	hitShards [16]paddedAtomicUint64
	misses    atomic.Uint64
}

type templateDefCacheEntry struct {
	id      uint64
	def     templ.TemplateDef
	touched atomic.Bool
}

func newTemplateDefCache(maxEntries int) *templateDefCache {
	if maxEntries <= 0 {
		return nil
	}
	c := &templateDefCache{
		maxEntries: maxEntries,
		m:          make(map[uint64]*list.Element, maxEntries),
	}
	empty := make(map[uint64]*templateDefCacheEntry)
	c.snap.Store(&empty)
	return c
}

func (c *templateDefCache) publishSnapshotLocked() {
	next := make(map[uint64]*templateDefCacheEntry, len(c.m))
	for id, elem := range c.m {
		if ent, ok := elem.Value.(*templateDefCacheEntry); ok && ent != nil {
			next[id] = ent
		}
	}
	c.snap.Store(&next)
}

func (c *templateDefCache) Stats() (hits, misses uint64, entries, capacity int) {
	if c == nil {
		return 0, 0, 0, 0
	}
	if s := c.snap.Load(); s != nil {
		entries = len(*s)
	}
	for i := range c.hitShards {
		hits += c.hitShards[i].v.Load()
	}
	return hits, c.misses.Load(), entries, c.maxEntries
}

func (c *templateDefCache) Get(id uint64) (templ.TemplateDef, bool) {
	if c == nil || id == 0 {
		return templ.TemplateDef{}, false
	}
	if s := c.snap.Load(); s != nil {
		if ent := (*s)[id]; ent != nil {
			if !ent.touched.Load() {
				ent.touched.Store(true)
			}
			c.hitShards[id&15].v.Add(1)
			return ent.def, true
		}
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
		// Publish a fresh immutable entry so concurrent lock-free readers
		// never race with updates to ent.def.
		elem.Value = &templateDefCacheEntry{id: id, def: def}
		c.ll.MoveToFront(elem)
		c.publishSnapshotLocked()
		return
	}
	elem := c.ll.PushFront(&templateDefCacheEntry{id: id, def: def})
	c.m[id] = elem
	if len(c.m) > c.maxEntries {
		// Second-chance (CLOCK) eviction from the tail: entries hit since
		// their last scan get one reprieve and move to the front.
		for tries := 0; tries <= len(c.m); tries++ {
			back := c.ll.Back()
			if back == nil {
				break
			}
			ent, ok := back.Value.(*templateDefCacheEntry)
			if ok && ent != nil && ent.touched.Swap(false) {
				c.ll.MoveToFront(back)
				continue
			}
			if ok && ent != nil {
				delete(c.m, ent.id)
			}
			c.ll.Remove(back)
			break
		}
	}
	c.publishSnapshotLocked()
}
