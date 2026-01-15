package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCompactionIndexSwap_DeadlockRegression(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, BackgroundCompactionIndexSwap: false} // Disable to prevent race
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		done := make(chan struct{})
		go func() {
			db.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Log("DB close timed out (expected during deadlock test)")
		}
	}()

	// Create Slab 0
	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(keys[i], []byte("val")); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	if _, err := db.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Pause Flush Loop (Writer for Slab 1)
	db.SlabManager().TestPauseActiveSlabWriter()
	defer db.SlabManager().TestResumeActiveSlabWriter()

	// Start Writer (Async Set) - write large data to fill buffer and block
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Write large data to fill buffer (5MB > 4MB default)
		large := make([]byte, 5*1024*1024)
		// This will block on Rotate/Flush if paused
		_ = db.Set([]byte("big"), large)
	}()

	// Wait a bit for Writer to grab lock and block
	time.Sleep(100 * time.Millisecond)

	// 4. Start Compaction
	compactionDone := make(chan error, 1)
	lockAcquired := make(chan struct{})
	var once sync.Once

	testHooks := &IndexSwapTestHooks{
		OnCutoverLockAcquired: func() {
			once.Do(func() { close(lockAcquired) })
		},
	}

	go func() {
		err := db.CompactSlabsIndexSwap(context.Background(), []uint32{0}, IndexSwapCompactionOptions{
			TestHooks: testHooks,
		})
		compactionDone <- err
	}()

	// Assert that compaction can acquire the lock
	select {
	case <-lockAcquired:
		t.Log("Compaction acquired lock (SUCCESS)")
	case err := <-compactionDone:
		t.Fatalf("Compaction failed early: %v", err)
	case <-time.After(30 * time.Second):
		t.Errorf("Compaction failed to acquire lock within timeout (Blocked by Writer?) - expected during deadlock test")
	}

	// Cleanup with timeout
	db.SlabManager().TestResumeActiveSlabWriter()
	done := make(chan struct{})
	go func() {
		<-compactionDone
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Log("Cleanup timed out (expected during deadlock test)")
	}
}
