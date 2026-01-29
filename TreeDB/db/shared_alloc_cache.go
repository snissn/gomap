package db

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/freelist"
)

type sharedAllocCache struct {
	alloc *freelist.Allocator

	mu   sync.Mutex
	pool []uint64

	// Refill size when pool is empty/low.
	refillBatch int

	// If pool grows beyond this due to Return(), spill back to freelist.
	maxPool int
	spill   int
}

func newSharedAllocCache(alloc *freelist.Allocator, _ignoredStripes int) *sharedAllocCache {
	return &sharedAllocCache{
		alloc:       alloc,
		refillBatch: 16384, // big: fewer allocator lock trips
		maxPool:     1 << 20,
		spill:       16384,
	}
}

func (c *sharedAllocCache) Alloc(hint uint64) (uint64, error) {
	// Fast path: pop from pool.
	c.mu.Lock()
	n := len(c.pool)
	if n > 0 {
		id := c.pool[n-1]
		c.pool = c.pool[:n-1]
		c.mu.Unlock()
		return id, nil
	}
	c.mu.Unlock()

	// Slow path: refill under allocator lock.
	ids, err := c.alloc.AllocMany(c.refillBatch, hint)
	if len(ids) == 0 {
		return 0, err
	}

	// Put all but one into pool, return one.
	c.mu.Lock()
	c.pool = append(c.pool, ids[:len(ids)-1]...)
	id := ids[len(ids)-1]
	c.mu.Unlock()

	_ = err
	return id, nil
}

func (c *sharedAllocCache) Return(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	c.pool = append(c.pool, ids...)

	// Spill if pool gets huge (optional safety).
	if len(c.pool) > c.maxPool {
		k := c.spill
		if k > len(c.pool) {
			k = len(c.pool)
		}
		spill := append([]uint64(nil), c.pool[len(c.pool)-k:]...)
		c.pool = c.pool[:len(c.pool)-k]
		c.mu.Unlock()
		return c.alloc.FreeMany(spill)
	}

	c.mu.Unlock()
	return nil
}

func (c *sharedAllocCache) Flush() error {
	c.mu.Lock()
	ids := append([]uint64(nil), c.pool...)
	c.pool = nil
	c.mu.Unlock()
	return c.alloc.FreeMany(ids)
}
