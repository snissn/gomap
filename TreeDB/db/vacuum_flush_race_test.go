package db

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestVacuumFlushRace simulates the race condition between Background Vacuum
// (applyVacuumDelta) and Foreground Flushes (commitBatch).
//
// It ensures that keys written during vacuum are not lost.
func TestVacuumFlushRace(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		DisableWAL:             true, // Backend only for this test (simulate flushing directly to backend)
		AllowUnsafe:            true,
		LeafPrefixCompression:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Writer: continually writes keys
	// We use a set of keys that we track.
	const keyCount = 1000
	written := make([]int64, keyCount)
	var writeMu sync.Mutex

	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				k := rng.Intn(keyCount)
				v := time.Now().UnixNano()
				key := []byte(fmt.Sprintf("key-%06d", k))
				val := []byte(fmt.Sprintf("val-%d", v))

				if err := db.Set(key, val); err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}
				
				// Manually flush to backend (since we are using Backend directly, Set IS a flush effectively?
				// No, Set just puts to Memtable if Cached, but here we opened Backend options?
				// Open(Options) returns *DB.
				// If Mode is not Cached, it returns Backend DB.
				// Set -> db.put -> commitBatch.
				// So Set IS the flush (to disk/slab).
				
				writeMu.Lock()
				written[k] = v
				writeMu.Unlock()
				
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	// 2. Vacuum Loop: continually runs Online Vacuum
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Run Vacuum
				if err := db.VacuumIndexOnline(ctx); err != nil {
					// Ignore "vacuum in progress" or cancelled errors
					if ctx.Err() != nil {
						return
					}
					// t.Logf("Vacuum error: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Run for 5 seconds
	time.Sleep(5 * time.Second)
	cancel()
	wg.Wait()

	// 3. Verify Data
	// Check that all last written values are present.
	t.Log("Verifying data...")
	for k, v := range written {
		if v == 0 {
			continue
		}
		key := []byte(fmt.Sprintf("key-%06d", k))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", key, err)
		}
		if got == nil {
			t.Fatalf("Key %s missing! Last written val: %d", key, v)
		}
		
		wantVal := fmt.Sprintf("val-%d", v)
		if string(got) != wantVal {
			// It's possible we read an OLDER value if the writer loop
			// updated 'written' but Set hadn't fully committed?
			// But 'Set' is synchronous here.
			// However, Vacuum might have swapped the index to an older state? 
			// No, Vacuum should preserve latest state.
			t.Errorf("Value mismatch for %s: got %s want %s", key, got, wantVal)
		}
	}
}
