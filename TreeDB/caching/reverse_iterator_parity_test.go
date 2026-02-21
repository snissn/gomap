package caching

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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
		DisableWAL:         true,
		AllowUnsafe:        true,
		FlushThreshold:     1 << 30,
		MemtableShards:     1,
		IndexOuterLeafMode: backenddb.IndexOuterLeafModeV2FencePtr,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	setMutable(db, []byte("k"), []byte("v"))
	db.memtableMode = memtable.Mode(255)

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

func TestReverseIterator_FlushesDeferredValueLogBeforeBackendRead(t *testing.T) {
	backend := &countingReverseIteratorBackend{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:            true,
		AllowUnsafe:           true,
		FlushThreshold:        1 << 30,
		MemtableShards:        1,
		ForceValueLogPointers: true,
		IndexOuterLeafMode:    backenddb.IndexOuterLeafModeV2FencePtr,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.backendReadVlogDirtySeq.Store(7)
	db.backendReadVlogFlushedSeq.Store(6)

	it, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	if got := db.backendReadVlogFlushedSeq.Load(); got != 7 {
		t.Fatalf("backendReadVlogFlushedSeq=%d want=7", got)
	}
	if calls := backend.reverseIteratorCalls.Load(); calls != 1 {
		t.Fatalf("backend reverse calls=%d want=1", calls)
	}
}

func TestReverseIterator_PropagatesDeferredValueLogFlushError(t *testing.T) {
	backend := &countingReverseIteratorBackend{MockBackend: NewMockBackend()}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:            true,
		AllowUnsafe:           true,
		FlushThreshold:        1 << 30,
		MemtableShards:        1,
		ForceValueLogPointers: true,
		IndexOuterLeafMode:    backenddb.IndexOuterLeafModeV2FencePtr,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.backendReadVlogDirtySeq.Store(3)
	db.backendReadVlogFlushedSeq.Store(2)
	l := &db.lanes[0]
	l.vlogMu.Lock()
	oldWriter := l.vlog
	l.vlog = nil
	l.vlogDirty.Store(true)
	l.vlogMu.Unlock()
	if oldWriter != nil {
		// Release the file handle now that this test intentionally detaches the lane
		// writer; otherwise TempDir cleanup can fail on Windows.
		_ = oldWriter.Close()
	}
	t.Cleanup(func() {
		l.vlogMu.Lock()
		l.vlogDirty.Store(false)
		l.vlogMu.Unlock()
	})

	it, err := db.ReverseIterator(nil, nil)
	if it != nil {
		_ = it.Close()
	}
	if !errors.Is(err, errWALUnavailable) {
		t.Fatalf("expected errWALUnavailable, got %v", err)
	}
	if calls := backend.reverseIteratorCalls.Load(); calls != 0 {
		t.Fatalf("backend reverse calls=%d want=0", calls)
	}
}
