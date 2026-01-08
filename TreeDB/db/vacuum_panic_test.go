package db

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestVacuumRaceMissingKey simulates the race condition where a key is written
// during the vacuum process. If the vacuum recorder only captures the key (and not the value),
// and the vacuum process tries to look up the key in the *old* snapshot, it might fail
// if the key was committed *after* the snapshot was taken.
//
// The fix involves RecordOps capturing the full entry so lookup isn't needed.
func TestVacuumRaceMissingKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	dir, err := os.MkdirTemp("", "vacuum-race")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Dir:       dir,
		ChunkSize: 64 * 1024 * 1024,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Seed DB with some data
	initialKeys := 1000
	batch := db.NewBatch()
	for i := 0; i < initialKeys; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i))
		v := []byte(fmt.Sprintf("val-%06d", i))
		if err := batch.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Start Vacuum in background
	// We want to trigger the race: Write happens WHILE vacuum is running.
	// Specifically, we want a write that happens AFTER vacuum takes its snapshot
	// but BEFORE vacuum finishes.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 10)

	go func() {
		defer wg.Done()
		// Artificial delay to let writers start
		time.Sleep(10 * time.Millisecond)
		if err := db.VacuumIndexOnline(ctx); err != nil {
			errCh <- fmt.Errorf("vacuum failed: %v", err)
		}
	}()

	// 3. Concurrent Writers
	// Write NEW keys that definitely weren't in the snapshot.
	const newKeysStart = 2000
	const newKeysCount = 5000

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < newKeysCount; i++ {
			k := []byte(fmt.Sprintf("key-%06d", newKeysStart+i))
			v := []byte(fmt.Sprintf("val-%06d", newKeysStart+i))

			// Use separate batches to simulate interleaved commits
			b := db.NewBatch()
			if err := b.Set(k, v); err != nil {
				errCh <- err
				return
			}
			if err := b.Write(); err != nil {
				errCh <- err
				return
			}
			_ = b.Close()

			// Yield occasionally to let vacuum progress
			if i%100 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("Concurrent op error: %v", err)
	}

	// 4. Verification
	// Re-open DB to ensure we are reading from the new index (if vacuum succeeded)
	// gracefully handling the case where db might be closed/reopened implicitly or explicitly.
	// For this test, the db object persists, but internal state should point to new index.

	// Check strictly for the keys written concurrently.
	// If the race existed, some of these might be missing because:
	// - Write committed (updated old index)
	// - Vacuum recorded the OP (Key)
	// - Vacuum tried to lookup Key in OldSnapshot -> Not Found (because committed after snapshot)
	// - Vacuum treated it as Delete/Ignore -> Key lost in New Index.

	missing := 0
	for i := 0; i < newKeysCount; i++ {
		k := []byte(fmt.Sprintf("key-%06d", newKeysStart+i))
		val, err := db.Get(k)
		if err != nil {
			// t.Logf("Missing key: %s (%v)", k, err)
			missing++
		} else {
			expected := fmt.Sprintf("val-%06d", newKeysStart+i)
			if string(val) != expected {
				t.Errorf("Corrupt value for %s: got %q, want %q", k, val, expected)
			}
		}
	}

	if missing > 0 {
		t.Fatalf("Consistency failure: %d/%d concurrent keys are missing after Vacuum", missing, newKeysCount)
	}
}
