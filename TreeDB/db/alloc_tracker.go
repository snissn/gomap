package db

import (
	"sync"
)

// allocTracker remembers allocated pages so they can be returned if a write
// attempt is abandoned. It allocates from a sharedAllocCache to avoid global
// freelist lock contention.
type allocTracker struct {
	cache *sharedAllocCache

	mu    sync.Mutex
	pages []uint64
}

func newAllocTracker(cache *sharedAllocCache) *allocTracker {
	return &allocTracker{cache: cache}
}

func (t *allocTracker) Alloc(hint uint64) (uint64, error) {
	id, err := t.cache.Alloc(hint)
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	t.pages = append(t.pages, id)
	t.mu.Unlock()
	return id, nil
}

func (t *allocTracker) FreeAll() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	pages := append([]uint64(nil), t.pages...)
	t.pages = nil
	t.mu.Unlock()

	return t.cache.Return(pages)
}
