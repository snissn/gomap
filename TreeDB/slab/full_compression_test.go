package slab

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestFullRecordCompression(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	// Use long key and redundant value
	key := []byte("s/k:ibc/facks/ports/transfer/channels/channel-2/sequences/91042")
	val := bytes.Repeat([]byte("redundant_data_"), 20) // ~150 bytes

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !page.ValuePtrIsFullCompressed(ptr) && !page.ValuePtrIsDictCompressed(ptr) {
		t.Errorf("Expected compression (full or dict) for long key and small value")
	}

	// Verify Read
	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("val mismatch: got %s, want %s", got, val)
	}

	// Verify AppendMany
	keys := [][]byte{key, key}
	vals := [][]byte{val, val}
	ptrs, err := sm.AppendMany(keys, vals)
	if err != nil {
		t.Fatalf("AppendMany: %v", err)
	}

	for i, p := range ptrs {
		if !page.ValuePtrIsFullCompressed(p) && !page.ValuePtrIsDictCompressed(p) {
			t.Errorf("Expected compression (full or dict) for AppendMany[%d]", i)
		}
		got, err := sm.Read(p)
		if err != nil {
			t.Errorf("Read AppendMany[%d]: %v", i, err)
		}
		if !bytes.Equal(got, val) {
			t.Errorf("val mismatch AppendMany[%d]", i)
		}
	}
}

func TestFullRecordCompression_Rotation(t *testing.T) {
	orig := MaxSlabSize
	defer func() { MaxSlabSize = orig }()
	MaxSlabSize = slabV2DataStart + 200 // Small enough to force rotation

	dir := t.TempDir()
	opts := Options{
		Compression: CompressionOptions{
			Kind:            CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
	}
	sm, err := NewSlabManagerWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	key := []byte("key_with_some_length")
	val := bytes.Repeat([]byte("redundant_data_"), 10) // ~75 bytes

	// Write several records to force rotation
	var ptrs []page.ValuePtr
	for i := 0; i < 10; i++ {
		ptr, err := sm.Append(key, val)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		ptrs = append(ptrs, ptr)
	}

	// Verify they are across different files and all readable
	fileIDs := make(map[uint32]struct{})
	for i, ptr := range ptrs {
		fileIDs[ptr.FileID] = struct{}{}
		got, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, val) {
			t.Errorf("Data mismatch %d", i)
		}
	}

	if len(fileIDs) < 2 {
		t.Errorf("Expected rotation across at least 2 files, got %d", len(fileIDs))
	}
}
