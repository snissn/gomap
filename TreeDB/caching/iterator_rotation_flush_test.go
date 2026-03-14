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
