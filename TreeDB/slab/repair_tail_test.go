package slab

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestRepairTail_TruncatesPartialGarbage(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSlab(filepath.Join(dir, "data-0000.slab"), 0)
	if err != nil {
		t.Fatalf("OpenSlab: %v", err)
	}
	defer s.Close()

	key := []byte("k")
	val := bytes.Repeat([]byte("v"), 100)

	off, err := s.Write(key, val)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	sizeBefore := s.Size

	// Append some garbage bytes (simulating a torn/partial tail write).
	if _, err := s.File.Write([]byte{0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("append garbage: %v", err)
	}

	if err := s.RepairTail(); err != nil {
		t.Fatalf("RepairTail: %v", err)
	}
	if s.Size != sizeBefore {
		t.Fatalf("expected size %d after repair, got %d", sizeBefore, s.Size)
	}

	got, err := s.Read(off+4, true)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch after repair")
	}
}
