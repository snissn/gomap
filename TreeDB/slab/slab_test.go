package slab

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSlabRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	key := []byte("testkey")
	val := []byte("testvalue")

	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	readVal, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(readVal) != string(val) {
		t.Errorf("Value mismatch. Got %s, want %s", readVal, val)
	}
}

func TestSlabRotation(t *testing.T) {
	// Override MaxSlabSize
	originalMax := MaxSlabSize
	defer func() { MaxSlabSize = originalMax }()
	
	// Set small limit: enough for 1 record but not 2?
	// Header(10) + Key(1) + Val(1) = 12 bytes.
	// Let's set limit to 20 bytes.
	MaxSlabSize = 20

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	// Record 1: 10 + 1 + 1 = 12 bytes. Offset 0.
	ptr1, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append 1 failed: %v", err)
	}
	if ptr1.FileID != 0 {
		t.Errorf("Expected FileID 0, got %d", ptr1.FileID)
	}

	// Record 2: 12 bytes.
	// Total 24 bytes > 20. Should trigger rotation.
	ptr2, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append 2 failed: %v", err)
	}
	if ptr2.FileID != 1 {
		t.Errorf("Expected FileID 1 for second record, got %d", ptr2.FileID)
	}

	// Verify files exist
	if _, err := os.Stat(filepath.Join(dir, "data-0000.slab")); err != nil {
		t.Error("data-0000.slab missing")
	}
	if _, err := os.Stat(filepath.Join(dir, "data-0001.slab")); err != nil {
		t.Error("data-0001.slab missing")
	}

	// Read back both
	val1, err := sm.Read(ptr1)
	if err != nil {
		t.Errorf("Read 1 failed: %v", err)
	}
	if string(val1) != "v" {
		t.Errorf("Value 1 mismatch")
	}

	val2, err := sm.Read(ptr2)
	if err != nil {
		t.Errorf("Read 2 failed: %v", err)
	}
	if string(val2) != "v" {
		t.Errorf("Value 2 mismatch")
	}
}

func TestDataCorruption(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	
	key := []byte("key")
	val := []byte("val")
	ptr, err := sm.Append(key, val)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	
	sm.Close() // Close to modify file

	// Corrupt file
	path := filepath.Join(dir, "data-0000.slab")
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	// Offset + HeaderSize + KeyLen is where Value starts?
	// Corruption in CRC (byte 0)
	if _, err := f.WriteAt([]byte{0x00}, 0); err != nil {
		t.Fatalf("Corrupt failed: %v", err)
	}
	f.Close()

	// Reopen
	sm2, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer sm2.Close()

	_, err = sm2.Read(ptr)
	if err != ErrChecksumMismatch {
		t.Errorf("Expected ErrChecksumMismatch, got %v", err)
	}
}
