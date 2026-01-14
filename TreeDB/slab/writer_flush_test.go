package slab

import (
	"sync/atomic"
	"testing"
)

func TestSlabWriterFlushDoesNotFsync(t *testing.T) {
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

	ptr, err := sm.Append([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := sm.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if syncCalls.Load() != 0 {
		t.Fatalf("expected no fsync during Flush, got %d", syncCalls.Load())
	}

	val, err := sm.Read(ptr)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(val) != "v" {
		t.Fatalf("value mismatch: got %q", string(val))
	}

	if err := sm.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if syncCalls.Load() == 0 {
		t.Fatalf("expected fsync during Sync")
	}
}
