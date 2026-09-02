package caching

import (
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type countingIteratorBackend struct {
	*MockBackend
	iteratorCalls atomic.Int64
}

func (b *countingIteratorBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	b.iteratorCalls.Add(1)
	return b.MockBackend.Iterator(start, end)
}

func TestIterator_BackendOnlyFastPathUsesBackendIterator(t *testing.T) {
	backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
	backend.Set([]byte("k1"), []byte("v1"))
	backend.Set([]byte("k2"), []byte("v2"))

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("k1"), max: []byte("k2")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	got := 0
	for it.Valid() {
		got++
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if got != 2 {
		t.Fatalf("keys=%d want=2", got)
	}
	if calls := backend.iteratorCalls.Load(); calls != 1 {
		t.Fatalf("backend iterator calls=%d want=1", calls)
	}
}

func TestIterator_BackendOnlyFastPathSkipsBackendOnKnownDisjointRange(t *testing.T) {
	backend := &countingIteratorBackend{MockBackend: NewMockBackend()}
	backend.Set([]byte("k1"), []byte("v1"))
	backend.Set([]byte("k2"), []byte("v2"))

	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.mu.Lock()
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("k1"), max: []byte("k2")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	it, err := db.Iterator([]byte("z"), []byte("zz"))
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	if it.Valid() {
		t.Fatalf("expected empty iterator for disjoint range")
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if calls := backend.iteratorCalls.Load(); calls != 0 {
		t.Fatalf("backend iterator calls=%d want=0", calls)
	}
}
