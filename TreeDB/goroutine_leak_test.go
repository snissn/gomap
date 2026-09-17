package treedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// exerciseDBLifecycle runs a small mixed workload against an open DB so that
// the surrounding goroutine-leak assertions cover the workers spawned by the
// write path, read path, iterators, snapshots, and checkpointing.
func exerciseDBLifecycle(t *testing.T, db *DB) {
	t.Helper()

	for i := 0; i < 256; i++ {
		key := []byte(fmt.Sprintf("leak-key-%04d", i))
		value := []byte(fmt.Sprintf("leak-value-%04d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	batch := db.NewBatch()
	for i := 256; i < 512; i++ {
		key := []byte(fmt.Sprintf("leak-key-%04d", i))
		if err := batch.Set(key, []byte("batch-value")); err != nil {
			t.Fatalf("batch.Set: %v", err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatalf("batch.Write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}

	for i := 0; i < 8; i++ {
		key := []byte(fmt.Sprintf("leak-key-%04d", i))
		if _, err := db.Get(key); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	iter, err := db.Iterator([]byte("leak-key-0000"), []byte("leak-key-0256"))
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter.Error: %v", err)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iter.Close: %v", err)
	}
	if count == 0 {
		t.Fatalf("iterator returned no keys")
	}

	snapshot := db.AcquireSnapshot()
	if snapshot != nil {
		if err := snapshot.Close(); err != nil {
			t.Fatalf("snapshot.Close: %v", err)
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
}

// TestGoroutineLeak_OpenWorkClose verifies that Open -> mixed work -> Close
// leaves no new goroutines running for each production profile.
func TestGoroutineLeak_OpenWorkClose(t *testing.T) {
	for _, profile := range []Profile{
		ProfileCommandWALDurable,
		ProfileCommandWALRelaxed,
		ProfileNoWALFast,
	} {
		t.Run(string(profile), func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			db, err := Open(OptionsFor(profile, t.TempDir()))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			exerciseDBLifecycle(t, db)
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestGoroutineLeak_FlushRotationConcurrent drives the cached layer hard
// enough to rotate memtables through the background flush machinery while
// concurrent readers run, then verifies Close reaps every worker.
func TestGoroutineLeak_FlushRotationConcurrent(t *testing.T) {
	for _, profile := range []Profile{
		ProfileCommandWALDurable,
		ProfileCommandWALRelaxed,
		ProfileNoWALFast,
	} {
		t.Run(string(profile), func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

			opts := OptionsFor(profile, t.TempDir())
			// Small flush threshold forces the queued-memtable/flush pipeline
			// to run several rotations during the workload below.
			opts.FlushThreshold = 1 << 20
			db, err := Open(opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			big := make([]byte, 64<<10)
			for i := range big {
				big[i] = byte(i)
			}

			var stopReaders atomic.Bool
			var readerWG sync.WaitGroup
			for w := 0; w < 4; w++ {
				readerWG.Add(1)
				go func(worker int) {
					defer readerWG.Done()
					for n := 0; !stopReaders.Load(); n++ {
						key := []byte(fmt.Sprintf("rot-key-%04d", n%4096))
						_, _ = db.Get(key)
						if n%256 == 0 {
							time.Sleep(time.Millisecond)
						}
					}
					_ = worker
				}(w)
			}

			// ~12 MiB of value-log-scale writes plus inline churn rotates the
			// 1 MiB memtable repeatedly and exercises the background pruner.
			for i := 0; i < 192; i++ {
				key := []byte(fmt.Sprintf("rot-key-%04d", i))
				if err := db.Set(key, big); err != nil {
					t.Fatalf("Set big: %v", err)
				}
				small := []byte(fmt.Sprintf("inline-%04d", i))
				if err := db.Set(key, small); err != nil {
					t.Fatalf("Set small: %v", err)
				}
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint mid-run: %v", err)
			}

			for i := 0; i < 96; i++ {
				key := []byte(fmt.Sprintf("rot-key-%04d", 4096+i))
				if err := db.Set(key, big); err != nil {
					t.Fatalf("Set post-checkpoint: %v", err)
				}
			}

			stopReaders.Store(true)
			readerWG.Wait()

			if err := db.DeleteRange([]byte("rot-key-0000"), []byte("rot-key-0064")); err != nil {
				t.Fatalf("DeleteRange: %v", err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint final: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		})
	}
}

// TestGoroutineLeak_Reopen verifies that close/reopen does not accumulate
// goroutines across generations of the same DB directory.
func TestGoroutineLeak_Reopen(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	dir := t.TempDir()
	for generation := 0; generation < 2; generation++ {
		db, err := Open(OptionsFor(ProfileCommandWALRelaxed, dir))
		if err != nil {
			t.Fatalf("Open generation %d: %v", generation, err)
		}
		exerciseDBLifecycle(t, db)
		if err := db.Close(); err != nil {
			t.Fatalf("Close generation %d: %v", generation, err)
		}
	}
}
