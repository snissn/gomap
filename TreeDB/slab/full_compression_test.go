package slab

import (
	"bytes"
	"testing"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestFullRecordCompression(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Compression: CompressionOptions{
			Kind: CompressionZSTD,
			MinBytes: 1,
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

	if !page.ValuePtrIsFullCompressed(ptr) {
		t.Errorf("Expected full compression for long key and small value")
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
		if !page.ValuePtrIsFullCompressed(p) {
			t.Errorf("Expected full compression for AppendMany[%d]", i)
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
