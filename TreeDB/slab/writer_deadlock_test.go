package slab

import (
	"os"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDeadlock_ZoneRotate_LocalDict(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb_slab_deadlock")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Compression: compression.Options{
			Kind:  compression.KindZSTD,
			Level: 1,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create a dummy dictionary profile
	dict := make([]byte, 32*1024)
	dict[0] = 0xBE
	dict[1] = 0xEF
	profile := &compression.ActiveProfile{
		Dict:     dict,
		DictHash: 12345,
	}

	// Force a profile so that when we rotate, we try to use a local dict.
	sm.ForceAcceptProfileForTesting(profile)

	// Write data until we are close to boundary.
	// We need to write 2MB.
	val := make([]byte, 1024)
	key := []byte("key")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			tail := sm.ActiveSlabTail()
			// Keep writing until we cross the first Zone boundary (2MB)
			if tail > 2*1024*1024+100*1024 {
				break
			}

			if _, err := sm.Append(key, val); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	select {
	case <-done:
		// Passed
	case <-time.After(10 * time.Second):
		t.Fatal("Deadlock detected during zone rotation")
	}
}

func TestCRC_ZoneRotate_LargeDict(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb_slab_crc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Compression: compression.Options{
			Kind:  compression.KindZSTD,
			Level: 1,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	// Create an oversized dictionary profile (e.g. 41KB)
	// Slab V2 slot is 40KB now.
	dict := make([]byte, 41*1024)
	for i := range dict {
		dict[i] = byte(i % 256)
	}
	profile := &compression.ActiveProfile{
		Dict:     dict,
		DictHash: 999,
	}

	// Force this profile. The manager should now reject it or handle it safely.
	sm.ForceAcceptProfileForTesting(profile)

	// Write data until we cross boundary.
	val := make([]byte, 1024)
	key := []byte("key")

	var ptrs []page.ValuePtr
	for {
		tail := sm.ActiveSlabTail()
		if tail > 2*1024*1024+100*1024 {
			break
		}
		ptr, err := sm.Append(key, val)
		if err != nil {
			t.Fatal(err)
		}
		ptrs = append(ptrs, ptr)
	}

	// Read back and verify NO CRC failures.
	// Specifically we want to check records in the new zone.
	for i, ptr := range ptrs {
		if int64(ptr.Offset) < 2*1024*1024 {
			continue // skip zone 0
		}
		readVal, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("Read failed at index %d, offset %d: %v", i, ptr.Offset, err)
		}
		if len(readVal) != len(val) {
			t.Fatalf("Value mismatch at index %d", i)
		}
	}
}
