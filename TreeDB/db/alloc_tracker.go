package db

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/freelist"
)

// allocTracker wraps the freelist allocator and remembers allocated pages so
// they can be returned if a write attempt is abandoned.
type allocTracker struct {
	alloc *freelist.Allocator
	mu    sync.Mutex
	pages []uint64
}

// appendOnlyPageAllocator scopes append-only allocation to a single build.
// Unlike Allocator.SetPreferAppend, it cannot change the allocation policy of
// another writer that shares the underlying allocator.
type appendOnlyPageAllocator struct {
	alloc *freelist.Allocator
}

func (a appendOnlyPageAllocator) Alloc(uint64) (uint64, error) {
	return a.alloc.AllocAppend()
}

func newAllocTracker(alloc *freelist.Allocator) *allocTracker {
	return &allocTracker{alloc: alloc}
}

func (t *allocTracker) Alloc(hint uint64) (uint64, error) {
	id, err := t.alloc.Alloc(hint)
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	t.pages = append(t.pages, id)
	t.mu.Unlock()
	return id, nil
}

func (t *allocTracker) Pages() []uint64 {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]uint64(nil), t.pages...)
}

func (t *allocTracker) FreeAll() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	pages := append([]uint64(nil), t.pages...)
	t.pages = nil
	t.mu.Unlock()
	var firstErr error
	for _, id := range pages {
		if err := t.alloc.Free(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
