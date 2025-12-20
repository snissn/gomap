package slab

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMmapSafety_ZeroCopy_Remap verifies that holding a zero-copy view of an
// older mmap remains safe even after the SlabFile remaps due to growth.
// Without the fix (retaining dead mappings), this test would crash with SIGSEGV.
func TestMmapSafety_ZeroCopy_Remap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	// 1. Write Record 1 (Small) to establish initial mmap
	key := []byte("k1")
	val := bytes.Repeat([]byte("v1"), 1024) // 1KB
	ptr1, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}

	// 2. Read Record 1 to establish the initial Mmap (Size ~1KB)
	// We use verifyCRC=false to ensure we are stressing the raw path if needed,
	// though Read() calls readViaMmap regardless.
	// We need to use sm.Read to simulate real usage, but sm.Read calls s.Read.
	// s.Read returns a slice.
	view1, err := sm.Read(ptr1)
	if err != nil {
		t.Fatalf("Read 1: %v", err)
	}
	if !bytes.Equal(view1, val) {
		t.Fatalf("view1 mismatch")
	}

	// 3. Grow the file significantly to force a remap on next read
	// Write 10MB data
	blob := bytes.Repeat([]byte("X"), 1024*1024)
	for i := 0; i < 10; i++ {
		_, err := sm.Append([]byte("k_grow"), blob)
		if err != nil {
			t.Fatalf("Append grow %d: %v", i, err)
		}
	}

	// 4. Write a new record at the end
	key2 := []byte("k2")
	val2 := []byte("v2")
	ptr2, err := sm.Append(key2, val2)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	// 5. Read Record 2
	// This forces readViaMmap to see that the existing mmap (from step 2) is too small.
	// It triggers a re-mmap of the larger file.
	// CRITICAL: If the old mmap is unmapped here, view1 becomes invalid.
	view2, err := sm.Read(ptr2)
	if err != nil {
		t.Fatalf("Read 2: %v", err)
	}
	if string(view2) != "v2" {
		t.Fatalf("view2 mismatch")
	}

	// 6. Access view1 AGAIN.
	// This access checks if the underlying memory is still valid.
	// If unmapped, this triggers SIGSEGV (crash).
	// If fixed, it works.
	if !bytes.Equal(view1, val) {
		t.Fatalf("view1 corrupted or mismatch after remap")
	}

	// Optional: Verify address difference (sanity check that we did remap)
	// This depends on OS, but usually mmap moves.
	// We can't easily check pointers in pure Go safe code, but the fact we didn't crash is the test.
}

// TestMmapSafety_Concurrent_Remap stresses the race condition.
func TestMmapSafety_Concurrent_Remap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	// Initial data
	key := []byte("static")
	val := bytes.Repeat([]byte("S"), 1024)
	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Reader Goroutine: Constantly reads 'static' ptr and CHECKS content.
	// This holds the view for a short duration.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				v, err := sm.Read(ptr)
				if err != nil {
					t.Error(err)
					return
				}
				// Access the bytes to prove validity
				if len(v) != 1024 || v[0] != 'S' || v[1023] != 'S' {
					t.Errorf("corrupt read")
					return
				}
				// Sleep a tiny bit to hold the reference while other threads work?
				// Actually Go scheduler might preempt here.
			}
		}
	}()

	// Writer/Remapper Goroutine: Constantly appends data and reads it (forcing remap).
	wg.Add(1)
	go func() {
		defer wg.Done()
		blob := make([]byte, 1024*1024) // 1MB
		for i := 0; i < 50; i++ {
			p, err := sm.Append([]byte("grow"), blob)
			if err != nil {
				t.Error(err)
				return
			}
			// Force Read of NEW data to trigger Remap
			if _, err := sm.Read(p); err != nil {
				t.Error(err)
				return
			}
			// Yield to let Reader race
			time.Sleep(1 * time.Millisecond)
		}
		close(done)
	}()

	wg.Wait()
}
