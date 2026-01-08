package caching

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/slab"
)

// TestConsistencyStress attempts to reproduce the "missing validator set" panic
// by simulating a high-churn workload with concurrent flushes, vacuum, and
// mixed batch sizes (bypass vs regular).
func TestConsistencyStress(t *testing.T) {
	dir := t.TempDir()

	// Enable aggressive compression and value log to match production
	opts := db.Options{
		Dir:                      dir,
		FlushThreshold:           1 * 1024 * 1024, // 1MB flush to trigger frequent flushes
		MemtableMode:             "adaptive",
		MemtableShards:           4,
		LeafPrefixCompression:    true,
		SlabCompression:          slab.CompressionOptions{Kind: slab.CompressionZSTD},
		ValueLogPointerThreshold: 32,   // Use value log for larger items
		AllowUnsafe:              true, // Required for some settings
	}

	backend, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	cdb, err := Open(dir, backend, Options{
		FlushThreshold:           opts.FlushThreshold,
		MemtableMode:             opts.MemtableMode,
		MemtableShards:           opts.MemtableShards,
		ValueLogPointerThreshold: opts.ValueLogPointerThreshold,
		AllowUnsafe:              true,
		SplitValueLog:            true,
		NotifyError: func(err error) {
			t.Logf("Background Error: %v", err)
			if err.Error() == "flush failed (read vlog): EOF" {
				t.Error("FAIL: Hit flush EOF race!")
			}
		},
	})
	if err != nil {
		backend.Close()
		t.Fatal(err)
	}
	defer cdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// 1. Validator Set Writer (Critical Data)
	// Writes "validators/<height>" keys.
	// Verification thread reads them back.
	const maxHeight = 100000
	currentHeight := atomic.Int64{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for h := int64(1); h <= maxHeight; h++ {
			if ctx.Err() != nil {
				return
			}
			key := []byte(fmt.Sprintf("validators/%d", h))
			val := make([]byte, 2048) // Large enough to maybe use value log
			rand.Read(val)
			binary.BigEndian.PutUint64(val, uint64(h)) // Embed height for verify

			// Write
			if err := cdb.Set(key, val); err != nil {
				t.Errorf("Set validator failed: %v", err)
				return
			}

			currentHeight.Store(h)

			// Occasionally write a "Bypass" batch
			if h%100 == 0 {
				b := cdb.NewBatch()
				for i := 0; i < 5000; i++ { // Large batch
					k := []byte(fmt.Sprintf("bypass/%d/%d", h, i))
					v := []byte("bypass-value")
					b.Set(k, v)
				}
				if err := b.Write(); err != nil {
					t.Errorf("Bypass write failed: %v", err)
				}
			}

			time.Sleep(100 * time.Microsecond)
		}
	}()

	// 2. Reader / Verifier
	wg.Add(1)
	go func() {
		defer wg.Done()
		lastVerified := int64(0)
		for {
			if ctx.Err() != nil {
				return
			}
			target := currentHeight.Load()
			if target > lastVerified {
				for h := lastVerified + 1; h <= target; h++ {
					key := []byte(fmt.Sprintf("validators/%d", h))
					val, err := cdb.Get(key)
					if err != nil {
						t.Errorf("Get failed for %s: %v", key, err)
						return
					}
					if val == nil {
						t.Errorf("CRITICAL: Key %s missing!", key)
						return
					}
					if len(val) != 2048 {
						t.Errorf("Key %s corrupted len %d", key, len(val))
					}
					readH := binary.BigEndian.Uint64(val)
					if readH != uint64(h) {
						t.Errorf("Key %s content mismatch: got height %d", key, readH)
					}
				}
				lastVerified = target
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// 3. Vacuum Runner
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Run Vacuum on the BACKEND
				// cdb.backend is an interface, need type assertion or access
				// But we have `backend` variable.
				err := backend.VacuumIndexOnline(ctx)
				if err != nil {
					// t.Logf("Vacuum: %v", err)
				}
			}
		}
	}()

	// 4. Checkpoint Runner (force flushing)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				cdb.Checkpoint()
			}
		}
	}()

	// Run for a while
	time.Sleep(20 * time.Second)
	cancel()
	wg.Wait()
}
