package caching

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type countingReverseIteratorBackend struct {
	*MockBackend
	reverseIteratorCalls atomic.Int64
}

func (b *countingReverseIteratorBackend) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	b.reverseIteratorCalls.Add(1)
	return b.MockBackend.ReverseIterator(start, end)
}

func TestReverseIterator_PropagatesRotateError(t *testing.T) {
	backend := &countingReverseIteratorBackend{MockBackend: NewMockBackend()}
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

	setMutable(db, []byte("k"), []byte("v"))
	db.storeMemtableMode(memtable.Mode(255))

	it, err := db.ReverseIterator(nil, nil)
	if it != nil {
		_ = it.Close()
	}
	if err == nil {
		t.Fatalf("expected rotate error")
	}
	if !strings.Contains(err.Error(), "unknown memtable mode") {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls := backend.reverseIteratorCalls.Load(); calls != 0 {
		t.Fatalf("backend reverse calls=%d want=0", calls)
	}
}
