package caching

import (
	"bytes"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestUnifiedWAL_BackendThresholdMismatch(t *testing.T) {
	// Scenario 1: Default Backend (Threshold 256)
	// Expectation: Copy-on-Flush moves small values (e.g. 100 bytes) to Index (Inline).
	t.Run("DefaultBackend", func(t *testing.T) {
		dir := t.TempDir()
		// Default backend options (InlineThreshold=256)
		backend, err := db.Open(db.Options{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()

		opts := Options{
			FlushThreshold:           1024 * 1024,
			ValueLogPointerThreshold: 32, // Frontend wants vlogs for >32
			DisableWAL:               false,
			AllowUnsafe:              true,
		}
		cached, err := Open(dir, backend, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer cached.Close()

		// Write 100 bytes ( > 32, < 256 )
		val := bytes.Repeat([]byte{0xAA}, 100)
		if err := cached.Set([]byte("key1"), val); err != nil {
			t.Fatal(err)
		}
		if err := cached.Checkpoint(); err != nil {
			t.Fatal(err)
		}

		// Check Backend Stats
		// stats := backend.Stats()
		// slabs := stats["treedb.slabs.total_bytes"]

		// Check file existence
		entries, _ := os.ReadDir(dir)
		foundSlab := false
		for _, e := range entries {
			if len(e.Name()) > 5 && e.Name()[:5] == "data-" {
				info, _ := e.Info()
				if info.Size() > 0 {
					foundSlab = true
					break
				}
			}
		}

		if foundSlab {
			t.Errorf("Expected 0 slab bytes (inline), found non-empty slab file")
		}
	})

	// Scenario 2: Tuned Backend (ForceValuePointers=true)
	// Expectation: Copy-on-Flush moves values to Slabs.
	t.Run("TunedBackend", func(t *testing.T) {
		dir := t.TempDir()
		// Tune backend to force pointers
		backend, err := db.Open(db.Options{
			Dir:                dir,
			ForceValuePointers: true, // InlineThreshold = 0
		})
		if err != nil {
			t.Fatal(err)
		}
		defer backend.Close()

		opts := Options{
			FlushThreshold:           1024 * 1024,
			ValueLogPointerThreshold: 32,
			DisableWAL:               false,
			AllowUnsafe:              true,
		}
		cached, err := Open(dir, backend, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer cached.Close()

		// Write 100 bytes
		val := bytes.Repeat([]byte{0xBB}, 100)
		if err := cached.Set([]byte("key1"), val); err != nil {
			t.Fatal(err)
		}
		if err := cached.Checkpoint(); err != nil {
			t.Fatal(err)
		}

		// Check file existence
		entries, _ := os.ReadDir(dir)
		foundSlab := false
		for _, e := range entries {
			if len(e.Name()) > 5 && e.Name()[:5] == "data-" {
				foundSlab = true
				break
			}
		}

		if !foundSlab {
			t.Error("Expected slab file (forced pointers), got none")
		}
	})
}
