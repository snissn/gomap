package caching

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
)

// TestRaceFlushRotate stresses the synchronization between log rotation (writer)
// and flushing (reader).
func TestRaceFlushRotate(t *testing.T) {
	dir, err := os.MkdirTemp("", "race-flush-rotate")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	backendOpts := db.Options{
		Dir:       dir,
		ChunkSize: 256 * 1024 * 1024,
	}
	backend, err := db.Open(backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	// Small threshold to force frequent rotations
	opts := Options{
		FlushThreshold:           64 * 1024,
		ValueLogPointerThreshold: 128,
		DisableWAL:               false,
		AllowUnsafe:              true,
	}

	cdb, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cdb.Close()

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: writes data to force rotation
	wg.Add(1)
	go func() {
		defer wg.Done()
		val := make([]byte, 1024) // 1KB value
		for i := 0; i < 10000; i++ {
			select {
			case <-done:
				return
			default:
			}
			key := []byte(fmt.Sprintf("key-%d", i))
			if err := cdb.Set(key, val); err != nil {
				t.Errorf("Set failed: %v", err)
				return
			}
			// Occasional manual rotation to stress lock path
			if i%100 == 0 {
				cdb.mu.Lock()
				_ = cdb.rotateMemtableLocked(false) // Ignore error, best effort
				cdb.mu.Unlock()
			}
		}
	}()

	// Flusher: concurrently flushes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			// Call internal flushOne to force read path
			if !cdb.flushOne() {
				// If queue empty, sleep briefly
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Run for a bit
	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()
}
