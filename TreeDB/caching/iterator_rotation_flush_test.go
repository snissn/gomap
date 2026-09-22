package caching

import (
	"fmt"
	"testing"
	"time"
)

func TestIteratorRotation_TriggersBackgroundFlushWhenQueueAlreadyHasWork(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const iters = 64
	for i := 0; i < iters; i++ {
		key := []byte(fmt.Sprintf("k-%03d", i))
		if err := db.Set(key, []byte("v")); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		it, err := db.Iterator(nil, nil)
		if err != nil {
			t.Fatalf("iterator %d: %v", i, err)
		}
		if err := it.Close(); err != nil {
			t.Fatalf("iterator close %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		db.mu.RLock()
		queueLen := len(db.queue)
		db.mu.RUnlock()
		if queueLen <= 8 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue len stayed high after iterator-triggered rotations: got=%d want<=8", queueLen)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIteratorRotation_EnqueuesOnlyNonEmptyShards(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 16,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	db.mu.RLock()
	queueLen := len(db.queue)
	queueShardLen := len(db.queueShardIDs)
	queueBacklog := db.queueBacklogBytes.Load()
	db.mu.RUnlock()

	if queueLen != 1 {
		t.Fatalf("queue len after single-key iterator rotation=%d want=1", queueLen)
	}
	if queueShardLen != 1 {
		t.Fatalf("queue shard ids len=%d want=1", queueShardLen)
	}
	if queueBacklog <= 0 {
		t.Fatalf("queue backlog bytes=%d want >0", queueBacklog)
	}
}

func TestRotateWithCapacity_EnqueuesOnlyNonEmptyShards(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1 << 30,
		MemtableShards: 16,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	db.mu.Lock()
	if err := db.rotateMemtableLockedWithCapacity(false, db.memtableCap); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotate with capacity: %v", err)
	}
	queueLen := len(db.queue)
	queueShardLen := len(db.queueShardIDs)
	queueBacklog := db.queueBacklogBytes.Load()
	db.mu.Unlock()

	if queueLen != 1 {
		t.Fatalf("queue len after single-key rotate with capacity=%d want=1", queueLen)
	}
	if queueShardLen != 1 {
		t.Fatalf("queue shard ids len=%d want=1", queueShardLen)
	}
	if queueBacklog <= 0 {
		t.Fatalf("queue backlog bytes=%d want >0", queueBacklog)
	}
}
