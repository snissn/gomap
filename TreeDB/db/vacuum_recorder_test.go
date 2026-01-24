package db

import (
	"testing"
	"time"
)

func TestVacuumRecorder_DoesNotRecordUncommittedWrites(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		_ = d.Close()
	}()

	d.vacuum.Start()
	defer d.vacuum.Stop()

	// Block root reads/commit so a batch write can't complete. If the vacuum
	// recorder captures keys before commit, it can be drained while the write is
	// still in-flight, causing online vacuum to miss the eventual update.
	d.mu.Lock()

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		b := d.NewBatch()
		_ = b.Set([]byte("k"), []byte("v"))
		done <- b.Write()
		_ = b.Close()
	}()

	<-started

	// Give the write goroutine time to start and (if buggy) record its keys, but
	// ensure the recorder stays empty while the write is blocked.
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if keys := d.vacuum.Drain(); len(keys) > 0 {
			t.Fatalf("vacuum recorded keys before commit: %v", keys)
		}
		time.Sleep(2 * time.Millisecond)
	}

	d.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for write to complete")
	}

	keys := d.vacuum.Drain()
	if _, ok := keys["k"]; !ok {
		t.Fatalf("expected committed key to be recorded, got: %v", keys)
	}
}
