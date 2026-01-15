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

func TestCompactionIndexSwap_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, BackgroundCompactionIndexSwap: false}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// 1. Create data to compact (Slab 0)
	// Need enough data to trigger Assist hook (>4MB).
	// 1000 keys * 5KB = ~5MB
	keys := make([][]byte, 1000)
	val := make([]byte, 5*1024) 
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
		if err := db.Set(keys[i], val); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	if _, err := db.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// 2. Setup hooks to synchronize
	compactionRunning := make(chan struct{})
	allowContinue := make(chan struct{})
	var once sync.Once

	assistHook := func() {
		once.Do(func() {
			close(compactionRunning)
			<-allowContinue
		})
	}

	// 3. Start Compaction in background
	compactionDone := make(chan error, 1)
	go func() {
		err := db.CompactSlabsIndexSwap(context.Background(), []uint32{0}, IndexSwapCompactionOptions{
			Assist: assistHook,
		})
		compactionDone <- err
	}()

	// 4. Wait for Compaction to be running (in Build phase)
	select {
	case <-compactionRunning:
		t.Log("Compaction is running (Assist hook reached)")
	case <-time.After(5 * time.Second):
		t.Fatalf("Compaction failed to reach Assist hook")
	}

	// 5. Perform Concurrent Write
	// This should succeed immediately because Compaction does not hold write locks during Build.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- db.Set([]byte("concurrent"), []byte("value"))
	}()

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("Concurrent Write failed: %v", err)
		}
		t.Log("Concurrent Write succeeded")
	case <-time.After(2 * time.Second):
		t.Fatalf("Concurrent Write timed out (Blocked by Compaction?)")
	}

	// 6. Finish Compaction
	close(allowContinue)
	select {
	case err := <-compactionDone:
		if err != nil {
			t.Fatalf("Compaction failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Compaction timed out after unblock")
	}

	// Verify concurrent write is visible
	gotVal, err := db.Get([]byte("concurrent"))
	if err != nil {
		t.Fatalf("Get concurrent failed: %v", err)
	}
	if string(gotVal) != "value" {
		t.Fatalf("Get concurrent mismatch: got %q", gotVal)
	}
}
