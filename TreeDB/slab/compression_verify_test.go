package slab

import (
	"bytes"
	"testing"
)

func TestSlabCompression_Zstd(t *testing.T) {
	dir := t.TempDir()

	// Create SlabManager with Zstd compression
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	// Write compressible data
	// 10KB of repeating bytes (highly compressible)
	key := []byte("testkey")
	val := bytes.Repeat([]byte{0xAA}, 10240)

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Verify Read
	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Data mismatch")
	}

	// Verify On-Disk Size (Compression Effectiveness)
	// Append another value and check the pointer offset delta.
	ptr2, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}

	// Distance between starts
	distance := int(ptr2.Offset - ptr.Offset)
	if distance < 0 {
		t.Fatalf("Negative distance")
	}

	t.Logf("Original Size: %d", len(val))
	t.Logf("Stored Size (Distance): %d", distance)

	if distance >= len(val) {
		t.Errorf("Compression failed: stored size %d >= original %d", distance, len(val))
	} else {
		t.Logf("Compression ratio: %.2fx", float64(len(val))/float64(distance))
	}
}
