package db

import (
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/node"
)

// TestValueIndexConsistency verifies that reads are always consistent with writes,
// meaning if a User Key is visible, its ValueID must be resolvable.
func TestValueIndexConsistency(t *testing.T) {
	opts := Options{
		Dir:              t.TempDir(),
		EnableValueIndex: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(1))
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, rng.Uint64()%1000) // 1000 keys
			val := make([]byte, 1024)
			rng.Read(val)

			if err := db.Set(key, val); err != nil {
				t.Errorf("Set failed: %v", err)
				return
			}
			// Sleep slightly to allow readers to interleave
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// Reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(2))
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			snap := db.AcquireSnapshot()
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, rng.Uint64()%1000)

			entry, err := snap.GetEntry(key)
			if err == nil {
				// Key exists. If it's a ValueID, we MUST be able to resolve it.
				if entry.Flags&node.FlagValueID != 0 {
					_, err := snap.ResolveValueIDToPtr(entry.Value)
					if err != nil {
						t.Errorf("CONSISTENCY VIOLATION: Key %x found with ValueID but failed to resolve: %v", key, err)
						close(stopCh) // Stop test immediately
						snap.Close()
						return
					}
				}
			}
			snap.Close()
		}
	}()

	time.Sleep(2 * time.Second)
	close(stopCh)
	wg.Wait()
}
