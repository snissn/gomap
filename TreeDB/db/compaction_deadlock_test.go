package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestIndexSwapCutover_IsNotBlockedByWriterDurabilityWait(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                           dir,
		BackgroundCompactionIndexSwap: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
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

	// 1. Write some data to create Slab 0
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(keys[i], []byte("val")); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	// Rotate to make Slab 0 compactable
	if _, err := db.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// 2. Pause the active slab writer (Slab 1)
	db.SlabManager().TestPauseActiveSlabWriter()
	defer db.SlabManager().TestResumeActiveSlabWriter()

	// 3. Start a writer goroutine that will block on durability
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block because flush loop is paused.
		// Must be large enough to avoid inlining (>256 bytes).
		val := make([]byte, 1024)
		_ = db.SetSync([]byte("block"), val)
	}()

	// Wait for writer to enter SetSync and potentially block holding lock
	time.Sleep(100 * time.Millisecond)

	// 4. Start Compaction
	compactionDone := make(chan error)
	lockAcquired := make(chan struct{})

	testHooks := &IndexSwapTestHooks{
		OnCutoverLockAcquired: func() {
			close(lockAcquired)
		},
	}

	go func() {
		err := db.CompactSlabsIndexSwap(context.Background(), []uint32{0}, IndexSwapCompactionOptions{
			TestHooks: testHooks,
		})
		compactionDone <- err
	}()

	// 5. Assert that compaction can acquire the lock
	select {
	case <-lockAcquired:
		t.Log("Compaction acquired lock (SUCCESS)")
	case <-time.After(5 * time.Second):
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

func TestIndexSwap_AllowsConcurrentWrites_NoDeadlock_WithFlushPaused(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, BackgroundCompactionIndexSwap: true}
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

	// Start Compaction
	compactionDone := make(chan error)
	lockAcquired := make(chan struct{})

	testHooks := &IndexSwapTestHooks{
		OnCutoverLockAcquired: func() {
			close(lockAcquired)
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
	case <-time.After(5 * time.Second):
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
