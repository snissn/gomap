package batch

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchPreWrite(t *testing.T) {
	reader := newMapValueReader()
	b := New(reader, page.DefaultInlineThreshold)

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

	// Case 2: Large Value (> 256 bytes) must use pointer API.
	largeKey := []byte("large")
	largeVal := bytes.Repeat([]byte("A"), page.DefaultInlineThreshold+10)
	if err := b.Set(largeKey, largeVal); err == nil {
		t.Fatalf("expected ErrValueTooLarge when setting large value")
	}

	ptr := reader.Add(largeVal)
	if err := b.SetPointer(largeKey, ptr); err != nil {
		t.Fatalf("SetPointer failed: %v", err)
	}

	entry, ok = b.Ops()[string(largeKey)]
	if !ok {
		t.Fatal("Large key not found in batch")
	}
	if !entry.IsPtr {
		t.Error("Expected pointer value, got inline")
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

func TestBatchSetOps_UsesSlabPointersForLargeValues(t *testing.T) {
	reader := newMapValueReader()
	b := New(reader, page.DefaultInlineThreshold)

	largeVal1 := bytes.Repeat([]byte("A"), page.DefaultInlineThreshold+10)
	largeVal2 := bytes.Repeat([]byte("B"), page.DefaultInlineThreshold+11)
	smallVal := []byte("small")

	ptr1 := reader.Add(largeVal1)
	ptr2 := reader.Add(largeVal2)
	ops := []Entry{
		{Type: OpPut, Key: []byte("k1"), ValuePtr: ptr1, IsPtr: true},
		{Type: OpPut, Key: []byte("k2"), Value: smallVal},
		{Type: OpPut, Key: []byte("k3"), ValuePtr: ptr2, IsPtr: true},
	}
	if err := b.SetOps(ops); err != nil {
		t.Fatalf("SetOps failed: %v", err)
	}

	got := b.Ops()
	if !got["k1"].IsPtr || !got["k3"].IsPtr {
		t.Fatalf("expected large values to be stored as pointers")
	}
	if got["k2"].IsPtr || !bytes.Equal(got["k2"].Value, smallVal) {
		t.Fatalf("expected small value to be stored inline")
	}

	read1, err := reader.Read(got["k1"].ValuePtr)
	if err != nil {
		t.Fatalf("Read k1 failed: %v", err)
	}
	if !bytes.Equal(read1, largeVal1) {
		t.Fatalf("k1 value mismatch")
	}

	read3, err := reader.Read(got["k3"].ValuePtr)
	if err != nil {
		t.Fatalf("Read k3 failed: %v", err)
	}
	if !bytes.Equal(read3, largeVal2) {
		t.Fatalf("k3 value mismatch")
	}
}
