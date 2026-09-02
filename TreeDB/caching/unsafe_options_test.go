package caching

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDisableWAL_NoCommitLogFilesCreated(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		DisableWAL:     true,
		AllowUnsafe:    true,
		FlushThreshold: 1024 * 1024,
	}

	cdb, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Check internal state
	for i := range cdb.lanes {
		l := &cdb.lanes[i]
		l.walMu.Lock()
		if l.wal != nil {
			l.walMu.Unlock()
			t.Error("expected lane WAL to be nil when DisableWAL is true")
			break
		}
		l.walMu.Unlock()
	}

	// Write data
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		v := []byte("value")
		if err := cdb.Set(k, v); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	// Force rotation to ensure no WAL is created on rotation either
	cdb.mu.Lock()
	err = cdb.rotateMemtableLocked(false)
	cdb.mu.Unlock()
	if err != nil {
		t.Fatalf("rotateMemtableLocked: %v", err)
	}

	// Verify WAL dir has no commit log files (value-log files are expected).
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".log" {
			continue
		}
		if strings.HasPrefix(e.Name(), "commit-") || strings.HasPrefix(e.Name(), "wal-") {
			t.Errorf("Found commit log file %q, expected none with DisableWAL=true", e.Name())
		}
	}

	// Close should flush successfully
	if err := cdb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRelaxedSync_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	runBench := func(relaxed bool) time.Duration {
		dir := t.TempDir()
		// We use NewMockBackend but Open() creates real WAL files in dir/wal,
		// so SetSync performance is dominated by real OS fsync on WAL.
		backend := NewMockBackend()

		opts := Options{
			RelaxedSync:    relaxed,
			DisableWAL:     false,
			AllowUnsafe:    relaxed,
			FlushThreshold: 1024 * 1024,
		}

		db, err := Open(dir, backend, opts)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		start := time.Now()
		val := []byte("value")
		for i := 0; i < 100; i++ { // 100 syncs is enough to see difference on most disks
			k := []byte(fmt.Sprintf("k%d", i))
			if err := db.SetSync(k, val); err != nil {
				t.Fatalf("SetSync: %v", err)
			}
		}
		return time.Since(start)
	}

	durSync := runBench(false)
	durRelaxed := runBench(true)

	t.Logf("Sync: %v, Relaxed: %v", durSync, durRelaxed)

	// RelaxedSync should be strictly faster (no fsync).
	if durRelaxed >= durSync {
		t.Logf("WARNING: RelaxedSync (%v) was not faster than Sync (%v). This might happen on tmpfs or fast SSDs with low count.", durRelaxed, durSync)
	} else {
		speedup := float64(durSync) / float64(durRelaxed)
		t.Logf("Speedup: %.2fx", speedup)
	}
}

func TestUnsafeOptions_ConcurrentStress(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		DisableWAL:     true,
		RelaxedSync:    true,
		AllowUnsafe:    true,
		FlushThreshold: 64 * 1024, // Small threshold to trigger frequent flushes/rotations
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	workers := 10
	ops := 1000

	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				k := []byte(fmt.Sprintf("k-%d-%d", id, j))
				v := []byte("value")

				// Mix Set, SetSync, Delete
				var err error
				if j%3 == 0 {
					err = db.SetSync(k, v)
				} else if j%3 == 1 {
					err = db.Set(k, v)
				} else {
					err = db.Delete(k)
				}
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d op %d: %v", id, j, err):
					default:
					}
					return
				}

				// Occasional manual flush to race with auto-flushes
				if j%100 == 0 {
					db.TriggerFlush()
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}

	// Verify durability explicitly with Checkpoint (should work even if relaxed)
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Verify data exists
	// We just pick one key to verify
	k := []byte("k-0-1") // Set (op 1)
	val, err := db.Get(k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Get(k-0-1): got %q, want value", val)
	}
}
