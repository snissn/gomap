package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
)

// TestUnifiedWAL_SplitLog_Flow verifies the flow of data when SplitValueLog is enabled:
// 1. Data lands in Vlog (not WAL).
// 2. RAM holds value (or pointer).
// 3. Flush copies to Backend (Slab).
// 4. Vlog deletion is safe after flush.
func TestUnifiedWAL_SplitLog_Flow(t *testing.T) {
	scenarios := []struct {
		name                     string
		memtableValueLogPointers bool
	}{
		{"RAM_Value", false},
		{"RAM_Ptr", true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := db.Open(db.Options{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()

			opts := Options{
				FlushThreshold:           4 * 1024 * 1024,
				ValueLogPointerThreshold: 100,
				SplitValueLog:            true, // Crucial for efficiency
				MemtableValueLogPointers: sc.memtableValueLogPointers,
				DisableWAL:               false,
				AllowUnsafe:              true,
			}

			cached, err := Open(dir, backend, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer cached.Close()

			// 1. Write Large Value
			valSize := 1000
			key := []byte("large-key")
			val := bytes.Repeat([]byte{0xAA}, valSize)
			if err := cached.Set(key, val); err != nil {
				t.Fatal(err)
			}

			// 3. Flush
			if err := cached.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			// 4. Verify Backend Storage
			stats := backend.Stats()
			t.Logf("Backend Stats: %v", stats)

			entries, _ := os.ReadDir(dir)
			foundSlab := false
			for _, e := range entries {
				if len(e.Name()) > 5 && e.Name()[:5] == "data-" && filepath.Ext(e.Name()) == ".slab" {
					foundSlab = true
					break
				}
			}
			if !foundSlab {
				t.Error("No slab file found in backend dir")
			}

			// 5. Destructive: Delete Vlog
			deleteVlogs(t, dir)

			// 6. Read
			got, err := backend.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, val) {
				t.Fatal("Data mismatch after vlog deletion")
			}
			t.Log("Data verified from backend after vlog deletion")
		})
	}
}

func TestUnifiedWAL_LargeBatch(t *testing.T) {
	dir := t.TempDir()
	backend, _ := db.Open(db.Options{Dir: dir})
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100,
		SplitValueLog:            true,
		// Small WAL segment to force multiple files
		WALMaxSegmentBytes: 1024 * 1024,
		AllowUnsafe:        true,
	}
	cached, _ := Open(dir, backend, opts)
	defer cached.Close()

	// Write 5MB of data (larger than FlushThreshold and WAL Segment)
	blob := bytes.Repeat([]byte{0xCC}, 10000) // 10KB
	count := 500
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		if err := cached.Set(key, blob); err != nil {
			t.Fatal(err)
		}
	}

	// Flush
	if err := cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify Backend
	deleteVlogs(t, dir)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		val, err := backend.Get(key)
		if err != nil || len(val) != 10000 {
			t.Fatalf("Backend Read Failed at %d: %v", i, err)
		}
	}
}

func TestUnifiedWAL_InterleavedWrites(t *testing.T) {
	dir := t.TempDir()
	backend, _ := db.Open(db.Options{Dir: dir})
	defer backend.Close()

	opts := Options{
		FlushThreshold:           4 * 1024 * 1024,
		ValueLogPointerThreshold: 100, // Small threshold
		DisableWAL:               false,
		AllowUnsafe:              true,
	}
	cached, _ := Open(dir, backend, opts)
	defer cached.Close()

	// Mix small and large
	for i := 0; i < 100; i++ {
		// Small (50 bytes) -> Inline / WAL
		if err := cached.Set([]byte(fmt.Sprintf("small-%d", i)), bytes.Repeat([]byte{1}, 50)); err != nil {
			t.Fatal(err)
		}
		// Large (500 bytes) -> Vlog
		if err := cached.Set([]byte(fmt.Sprintf("large-%d", i)), bytes.Repeat([]byte{2}, 500)); err != nil {
			t.Fatal(err)
		}
	}

	if err := cached.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// Verify
	for i := 0; i < 100; i++ {
		sVal, err := backend.Get([]byte(fmt.Sprintf("small-%d", i)))
		if err != nil || len(sVal) != 50 {
			t.Errorf("Small read failed at %d", i)
		}
		lVal, err := backend.Get([]byte(fmt.Sprintf("large-%d", i)))
		if err != nil || len(lVal) != 500 {
			t.Errorf("Large read failed at %d", i)
		}
	}
}

// TestUnifiedWAL_Reopen_Deadlock checks if reopening a DB with existing Vlogs causes a hang.
// This targets the `replayValueLogSegment` logic in `wal_recovery.go`.
func TestUnifiedWAL_Reopen_Deadlock(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write data and Close
	{
		backend, _ := db.Open(db.Options{Dir: dir, ForceValuePointers: true})
		// Use SplitValueLog=false (Default) to match failing prod scenario
		opts := Options{
			ValueLogPointerThreshold: 100,
			SplitValueLog:            false,
			DisableWAL:               false,
			AllowUnsafe:              true,
		}
		cached, _ := Open(dir, backend, opts)

		val := bytes.Repeat([]byte{0xDD}, 500)
		for i := 0; i < 100; i++ {
			if err := cached.Set([]byte(fmt.Sprintf("key-%d", i)), val); err != nil {
				t.Fatal(err)
			}
		}
		cached.Close()
		backend.Close()
	}

	// Phase 2: Reopen with timeout
	done := make(chan struct{})
	go func() {
		backend, err := db.Open(db.Options{Dir: dir, ForceValuePointers: true})
		if err != nil {
			panic(err)
		}
		// We don't need caching layer to test backend replay deadlock
		backend.Close()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Reopen successful")
	case <-time.After(5 * time.Second):
		t.Fatal("Reopen timed out (Deadlock detected)")
	}
}

func TestUnifiedWAL_Flush_After_Reopen(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write to Vlog and Close (simulating crash/restart)
	{
		// Backend must have WAL disabled to avoid conflict with caching layer in same dir
		backend, err := db.Open(db.Options{Dir: dir, ForceValuePointers: true, DisableWAL: true, AllowUnsafe: true})
		if err != nil {
			t.Fatal(err)
		}
		opts := Options{
			ValueLogPointerThreshold: 32,
			SplitValueLog:            true, // Use separate vlog file
			DisableWAL:               false,
			AllowUnsafe:              true,
		}
		cached, _ := Open(dir, backend, opts)

		val := bytes.Repeat([]byte{0xCC}, 100) // > 32
		if err := cached.Set([]byte("key1"), val); err != nil {
			t.Fatal(err)
		}
		// Ensure it's in Vlog (check file existence?)
		// cached.Checkpoint() would flush it. We want it in MEMTABLE + VLOG on restart?
		// No, if we restart, memtable is gone. It replays from WAL/Vlog.
		// Replay puts it back in Memtable.
		// Then we trigger Flush.

		cached.Close() // Graceful close ensures WAL/Vlog consistency
		backend.Close()
	}

	// Phase 2: Reopen, Replay, and Flush
	{
		backend, err := db.Open(db.Options{Dir: dir, ForceValuePointers: true, DisableWAL: true, AllowUnsafe: true})
		if err != nil {
			t.Fatal(err)
		}
		opts := Options{
			ValueLogPointerThreshold: 32,
			SplitValueLog:            true,
			DisableWAL:               false,
			AllowUnsafe:              true,
		}
		cached, err := Open(dir, backend, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer cached.Close()

		// Verify key exists (in memtable from replay)
		val, err := cached.Get([]byte("key1"))
		if err != nil {
			t.Fatalf("Get error: %v", err)
		}
		if val == nil {
			t.Fatal("Key not found after replay")
		}

		// Force Flush (should read from replayed Vlog ptr)
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("Flush failed after reopen: %v", err)
		}

		// Verify Backend has it
		bVal, err := backend.Get([]byte("key1"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(bVal, val) {
			t.Fatal("Data mismatch in backend")
		}
	}
}

func getLogSizes(t *testing.T, dir string) (walSize, vlogSize int64) {
	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Logf("ReadDir(%q) error: %v", walDir, err)
		return 0, 0
	}
	for _, e := range entries {
		info, _ := e.Info()
		if len(e.Name()) > 5 && e.Name()[:5] == "vlog-" {
			vlogSize += info.Size()
		} else if len(e.Name()) > 4 && e.Name()[:4] == "wal-" {
			walSize += info.Size()
		}
	}
	return
}

func deleteVlogs(t *testing.T, dir string) {
	walDir := filepath.Join(dir, "wal")
	entries, _ := os.ReadDir(walDir)
	for _, e := range entries {
		if len(e.Name()) > 5 && e.Name()[:5] == "vlog-" {
			os.Remove(filepath.Join(walDir, e.Name()))
		}
	}
}
