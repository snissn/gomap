package slab

import (
	"bytes"
	"testing"
)

func TestSlabV2_AppendManyBoundary(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
		},
	}

	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManagerWithOptions: %v", err)
	}
	defer sm.Close()

	if sm.activeSlab.version != Version2 {
		t.Fatalf("expected V2 slab, got %d", sm.activeSlab.version)
	}
	// Disable compression to keep record sizing deterministic.
	sm.activeCompression.Kind = CompressionNone
	sm.compression.Kind = CompressionNone

	key := []byte("k")
	value := bytes.Repeat([]byte("v"), 1024)
	recordLen := HeaderSize + len(key) + len(value)

	for i := 0; i < 20000; i++ {
		current := sm.activeSlabWriter.Size()
		nextBoundary := ((current / ZoneSize) + 1) * ZoneSize
		remaining := nextBoundary - current
		if remaining > int64(recordLen) && remaining < int64(2*recordLen) {
			break
		}
		if _, err := sm.AppendWithOptions(key, value, AppendOptions{DisableCompression: true, SkipTraining: true}); err != nil {
			t.Fatalf("AppendWithOptions: %v", err)
		}
		if current >= 4*ZoneSize {
			t.Fatalf("failed to reach boundary window, current=%d remaining=%d", current, remaining)
		}
	}

	keys := [][]byte{key, key}
	values := [][]byte{value, value}
	ptrs, err := sm.appendWithOptionsMany(keys, values)
	if err != nil {
		t.Fatalf("AppendMany: %v", err)
	}
	for i, ptr := range ptrs {
		if ptr.Offset < uint64(SlabV2DataStart) {
			t.Fatalf("ptr %d offset=%d size=%d", i, ptr.Offset, sm.activeSlab.Size)
		}
	}
	if err := sm.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i, ptr := range ptrs {
		got, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("value mismatch %d", i)
		}
	}
}
