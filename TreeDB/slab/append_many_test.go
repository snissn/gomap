package slab

import (
	"bytes"
	"testing"
)

func TestSlabAppendMany_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	keys := [][]byte{
		[]byte("k1"),
		[]byte("k2"),
		[]byte("k3"),
	}
	values := [][]byte{
		[]byte("v1"),
		bytes.Repeat([]byte("x"), 1024),
		[]byte("v3"),
	}

	ptrs, err := sm.AppendMany(keys, values)
	if err != nil {
		t.Fatalf("AppendMany failed: %v", err)
	}
	if len(ptrs) != len(keys) {
		t.Fatalf("unexpected ptr count: got %d want %d", len(ptrs), len(keys))
	}

	for i, ptr := range ptrs {
		got, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("Read(%d) failed: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("value mismatch at %d", i)
		}
	}
}

func TestSlabAppendMany_Rotates(t *testing.T) {
	orig := MaxSlabSize
	defer func() { MaxSlabSize = orig }()
	MaxSlabSize = slabV2DataStart + 16

	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager failed: %v", err)
	}
	defer sm.Close()

	keys := [][]byte{[]byte("k"), []byte("k"), []byte("k")}
	values := [][]byte{[]byte("v"), []byte("v"), []byte("v")}

	ptrs, err := sm.AppendMany(keys, values)
	if err != nil {
		t.Fatalf("AppendMany failed: %v", err)
	}
	if len(ptrs) != 3 {
		t.Fatalf("unexpected ptr count: got %d want 3", len(ptrs))
	}

	if ptrs[0].FileID != 0 || ptrs[1].FileID != 1 || ptrs[2].FileID != 2 {
		t.Fatalf("expected rotation across slab files, got fileIDs %d,%d,%d", ptrs[0].FileID, ptrs[1].FileID, ptrs[2].FileID)
	}

	for i, ptr := range ptrs {
		got, err := sm.Read(ptr)
		if err != nil {
			t.Fatalf("Read(%d) failed: %v", i, err)
		}
		if string(got) != "v" {
			t.Fatalf("unexpected value at %d: %q", i, string(got))
		}
	}
}
