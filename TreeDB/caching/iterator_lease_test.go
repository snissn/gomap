package caching

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func clearQueueLockedForIteratorLeaseTest(db *DB) {
	db.queue = nil
	db.queueShardIDs = nil
	db.queueLaneIDs = nil
	db.queueIDs = nil
	db.queueEnqueueNS = nil
	db.queueRanges = nil
	db.queueWALPaths = nil
	db.queueValueLogPaths = nil
	db.queueBacklogBytes.Store(0)
}

func TestIterator_QueuedViewLeaseHeldUntilClose(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.mu.Unlock()

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}
	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs before iterator=%d want=1", refs)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}

	if refs := view.refs.Load(); refs != 2 {
		t.Fatalf("view refs while iterator open=%d want=2", refs)
	}

	db.mu.Lock()
	db.queueRetiredMemtableLocked(queued)
	clearQueueLockedForIteratorLeaseTest(db)
	db.publishMemtablesLocked()
	db.mu.Unlock()

	if refs := view.refs.Load(); refs != 1 {
		t.Fatalf("view refs after publish with iterator open=%d want=1", refs)
	}
	if got := queued.Len(); got != 2 {
		t.Fatalf("queued memtable reset too early len=%d want=2", got)
	}

	seen := map[string]string{}
	for it.Valid() {
		seen[string(it.Key())] = string(it.Value())
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(seen) != 2 || seen["k1"] != "v1" || seen["k2"] != "v2" {
		t.Fatalf("iterator values=%v want map[k1:v1 k2:v2]", seen)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after first close=%d want=0", refs)
	}
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after first close=%d want=0", got)
	}

	if err := it.Close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after second close=%d want=0", refs)
	}
}

type failingIteratorBackend struct {
	*MockBackend
	err error
}

func (b *failingIteratorBackend) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return nil, b.err
}

func TestIterator_BackendErrorClosesQueuedIterators(t *testing.T) {
	backend := &failingIteratorBackend{
		MockBackend: NewMockBackend(),
		err:         errors.New("backend iterator boom"),
	}
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate: %v", err)
	}
	db.backendRangeKnown = true
	db.backendRange = keyRange{valid: true, min: []byte("a"), max: []byte("z")}
	db.mu.Unlock()
	db.backendRangeInit.Do(func() {})

	view := db.memtables.Load()
	if view == nil || len(view.queue) != 1 {
		t.Fatalf("expected one queued memtable view, got=%v", view)
	}
	queued, ok := view.queue[0].(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("queued memtable type=%T want *memtable.AppendOnly", view.queue[0])
	}

	if _, err := db.Iterator(nil, nil); err == nil {
		t.Fatalf("expected iterator error")
	}

	done := make(chan struct{})
	go func() {
		db.mu.Lock()
		db.queueRetiredMemtableLocked(queued)
		clearQueueLockedForIteratorLeaseTest(db)
		db.publishMemtablesLocked()
		db.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked; queued iterator lease likely leaked on backend error")
	}

	if refs := view.refs.Load(); refs != 0 {
		t.Fatalf("view refs after publish=%d want=0", refs)
	}
	if got := queued.Len(); got != 0 {
		t.Fatalf("queued memtable len after publish=%d want=0", got)
	}
}
