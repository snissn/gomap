package db

import "github.com/snissn/gomap/TreeDB/freelist"

// allocTracker wraps the freelist allocator and remembers allocated pages so
// they can be returned if a write attempt is abandoned.
type allocTracker struct {
	alloc *freelist.Allocator
	pages []uint64
}

func newAllocTracker(alloc *freelist.Allocator) *allocTracker {
	return &allocTracker{alloc: alloc}
}

func (t *allocTracker) Alloc(hint uint64) (uint64, error) {
	id, err := t.alloc.Alloc(hint)
	if err != nil {
		return 0, err
	}
	t.pages = append(t.pages, id)
	return id, nil
}

func (t *allocTracker) FreeAll() error {
	if t == nil {
		return nil
	}
	var firstErr error
	for _, id := range t.pages {
		if err := t.alloc.Free(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.pages = nil
	return firstErr
}
