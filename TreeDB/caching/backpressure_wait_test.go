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
