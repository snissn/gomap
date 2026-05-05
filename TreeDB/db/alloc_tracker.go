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

	preparedOutputID    preparedOutputID
	preparedOutputState preparedOutputState
}

func newAllocTracker(alloc *freelist.Allocator) *allocTracker {
	return &allocTracker{alloc: alloc}
}

func newPreparedOutputAllocTracker(alloc *freelist.Allocator, id preparedOutputID) *allocTracker {
	return &allocTracker{
		alloc:               alloc,
		preparedOutputID:    id,
		preparedOutputState: preparedOutputStatePrepared,
	}
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

func (t *allocTracker) PreparedOutputID() preparedOutputID {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.preparedOutputID
}

func (t *allocTracker) PreparedOutputSnapshot() preparedOutputSnapshot {
	if t == nil {
		return preparedOutputSnapshot{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return preparedOutputSnapshot{
		ID:    t.preparedOutputID,
		State: t.preparedOutputState,
		Pages: append([]uint64(nil), t.pages...),
	}
}

func (t *allocTracker) MarkInstalled() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.preparedOutputID != 0 {
		t.preparedOutputState = preparedOutputStateInstalled
	}
	t.mu.Unlock()
}

func (t *allocTracker) FreeAll() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	if t.preparedOutputState == preparedOutputStateInstalled {
		t.mu.Unlock()
		return nil
	}
	pages := append([]uint64(nil), t.pages...)
	t.pages = nil
	if t.preparedOutputID != 0 {
		t.preparedOutputState = preparedOutputStateAbandoned
	}
	t.mu.Unlock()
	var firstErr error
	for _, id := range pages {
		if err := t.alloc.Free(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
