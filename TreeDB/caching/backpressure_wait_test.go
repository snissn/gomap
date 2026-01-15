package caching

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestWaitForStopSchedulesFlush(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	const (
		flushThreshold = 1024
		maxBacklog     = int64(4 * 1024)
	)

	db, err := Open(dir, backend, Options{
		FlushThreshold:          flushThreshold,
		DisableValueLog:         true,
		MemtableShards:          1,
		MaxBacklogBytes:         maxBacklog,
		SlowdownBacklogSeconds:  0,
		StopBacklogSeconds:      0,
		WriterFlushMaxMemtables: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload := bytes.Repeat([]byte("x"), 2048)
	for i := 0; db.QueueBacklogBytes() < maxBacklog*2; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		setMutable(db, key, payload)

		db.mu.Lock()
		if err := db.rotateMemtableLocked(false); err != nil {
			db.mu.Unlock()
			t.Fatalf("rotateMemtableLocked: %v", err)
		}
		db.mu.Unlock()
	}

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("final"), []byte("v"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked under backpressure; expected flush scheduling to make progress")
	}
}

func TestWaitForStopIgnoresStaleBacklog(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:         1024,
		DisableValueLog:        true,
		MemtableShards:         1,
		SlowdownBacklogSeconds: 0,
		StopBacklogSeconds:     1,
		MaxBacklogBytes:        0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.queueBacklogBytes.Store(2048)

	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("k"), []byte("v"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		db.queueBacklogBytes.Store(0)
		db.bpMu.Lock()
		db.bpCond.Broadcast()
		db.bpMu.Unlock()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Set after unblock: %v", err)
			}
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("Set blocked under stale backlog")
		}
	}
}

func TestWaitForStopFlushesWithoutBackground(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:          1024,
		DisableValueLog:         true,
		MemtableShards:          1,
		SlowdownBacklogSeconds:  0,
		StopBacklogSeconds:      1,
		MaxBacklogBytes:         0,
		WriterFlushMaxMemtables: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	payload := bytes.Repeat([]byte("x"), 2048)
	setMutable(db, []byte("k1"), payload)
	db.mu.Lock()
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	if db.QueueBacklogBytes() == 0 {
		t.Fatalf("expected backlog after rotation")
	}

	db.disableBackgroundFlush.Store(true)
	defer db.disableBackgroundFlush.Store(false)

	db.flushMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- db.Set([]byte("k2"), []byte("v"))
	}()

	select {
	case err := <-done:
		db.flushMu.Unlock()
		t.Fatalf("Set completed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	db.flushMu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked under backpressure without background flush")
	}
}

func TestLargeBatchBypassSkipsBackpressureStop(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:         1024,
		DisableValueLog:        true,
		MemtableShards:         1,
		SlowdownBacklogSeconds: 0,
		StopBacklogSeconds:     1,
		MaxBacklogBytes:        0,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	db.queueBacklogBytes.Store(10 * 1024)

	b := db.NewBatchWithSize(2048)
	payload := bytes.Repeat([]byte("x"), 2048)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if err := b.Set(key, payload); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Write()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked under backpressure for large bypass batch")
	}
}
