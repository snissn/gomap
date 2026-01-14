package slab

import (
	"bytes"
	"sync/atomic"
	"testing"
)

func TestReadWaitsForActiveSlabDurability(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSlabManager(dir)
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer sm.Close()

	var syncCalls atomic.Int64
	slabSyncHook = func(*SlabFile) error {
		syncCalls.Add(1)
		return nil
	}
	t.Cleanup(func() { slabSyncHook = nil })

	val := bytes.Repeat([]byte("x"), 128*1024)
	ptr, err := sm.Append([]byte("k"), val)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("value mismatch")
	}
	if syncCalls.Load() != 0 {
		t.Fatalf("expected no fsync during read, got %d", syncCalls.Load())
	}
}
