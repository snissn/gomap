package batch

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap-gemini/TreeDB/page"
	"github.com/snissn/gomap-gemini/TreeDB/slab"
)

func TestBatchPreWrite(t *testing.T) {
	dir := t.TempDir()
	sm, err := slab.NewSlabManager(dir)
	if err != nil {
		t.Fatalf("SlabManager init failed: %v", err)
	}
	defer sm.Close()

	b := New(sm)

	// Case 1: Inline Value
	smallKey := []byte("small")
	smallVal := []byte("value")
	if err := b.Set(smallKey, smallVal); err != nil {
		t.Fatalf("Set small failed: %v", err)
	}

	entry, ok := b.Ops()[string(smallKey)]
	if !ok {
		t.Fatal("Small key not found in batch")
	}
	if entry.IsPtr {
		t.Error("Expected inline value, got pointer")
	}
	if !bytes.Equal(entry.Value, smallVal) {
		t.Errorf("Value mismatch: got %s, want %s", entry.Value, smallVal)
	}

	// Case 2: Large Value (> 256 bytes)
	largeKey := []byte("large")
	largeVal := bytes.Repeat([]byte("A"), page.InlineThreshold+10)
	if err := b.Set(largeKey, largeVal); err != nil {
		t.Fatalf("Set large failed: %v", err)
	}

	entry, ok = b.Ops()[string(largeKey)]
	if !ok {
		t.Fatal("Large key not found in batch")
	}
	if !entry.IsPtr {
		t.Error("Expected pointer value, got inline")
	}
	
	// Verify data in slab
	readVal, err := sm.Read(entry.ValuePtr)
	if err != nil {
		t.Fatalf("Failed to read from slab: %v", err)
	}
	if !bytes.Equal(readVal, largeVal) {
		t.Error("Slab value mismatch")
	}

	// Case 3: Delete
	delKey := []byte("del")
	if err := b.Delete(delKey); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	entry, ok = b.Ops()[string(delKey)]
	if !ok {
		t.Fatal("Delete key not found")
	}
	if entry.Type != OpDelete {
		t.Errorf("Expected OpDelete, got %v", entry.Type)
	}
}
