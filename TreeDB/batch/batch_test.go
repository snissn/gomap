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

func TestBatchIsEmpty(t *testing.T) {
	if !((*Batch)(nil)).IsEmpty() {
		t.Fatalf("nil batch should be empty")
	}
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })
	if !b.IsEmpty() {
		t.Fatalf("new batch should be empty")
	}
	if err := b.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if b.IsEmpty() {
		t.Fatalf("batch with queued op should not be empty")
	}
	_ = b.SortedEntries()
	if b.IsEmpty() {
		t.Fatalf("batch remains non-empty after sorting entries")
	}
	b.Reset()
	if !b.IsEmpty() {
		t.Fatalf("reset batch should be empty")
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

func TestHasValueLogPointers(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if got := b.HasValueLogPointers(); got {
		t.Fatalf("empty batch: HasValueLogPointers() = %v, want false", got)
	}
	if err := b.Set([]byte("k-inline"), []byte("v")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	if got := b.HasValueLogPointers(); got {
		t.Fatalf("inline-only batch: HasValueLogPointers() = %v, want false", got)
	}

	if err := b.SetPointer([]byte("k-vlog"), page.ValuePtr{
		FileID: page.ValueLogFileID(1),
		Offset: 42,
		Length: 7,
	}); err != nil {
		t.Fatalf("SetPointer value-log: %v", err)
	}
	if got := b.HasValueLogPointers(); !got {
		t.Fatalf("value-log pointer batch: HasValueLogPointers() = %v, want true", got)
	}
}

func TestTouchedValueLogSegments(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("empty batch touched segments = %v, want []", got)
	}
	if err := b.Set([]byte("k-inline"), []byte("v")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("inline-only touched segments = %v, want []", got)
	}

	if err := b.SetPointer([]byte("k-a"), page.ValuePtr{
		FileID: page.ValueLogFileID(3),
		Offset: 10,
		Length: 2,
	}); err != nil {
		t.Fatalf("SetPointer k-a: %v", err)
	}
	if err := b.SetPointer([]byte("k-b"), page.ValuePtr{
		FileID: page.ValueLogFileID(1),
		Offset: 12,
		Length: 2,
	}); err != nil {
		t.Fatalf("SetPointer k-b: %v", err)
	}
	if err := b.SetPointer([]byte("k-c"), page.ValuePtr{
		FileID: page.ValueLogFileID(3),
		Offset: 14,
		Length: 2,
	}); err != nil {
		t.Fatalf("SetPointer k-c: %v", err)
	}

	got := b.TouchedValueLogSegments()
	want := []uint32{page.ValueLogFileID(1), page.ValueLogFileID(3)}
	if len(got) != len(want) {
		t.Fatalf("touched segments len=%d want=%d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("touched segments[%d]=%d want=%d (all=%v)", i, got[i], want[i], got)
		}
	}

	b.Reset()
	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("after Reset touched segments = %v, want []", got)
	}
}

func TestSetPointerViewNoTouch_SkipsTouchedSegmentTracking(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if err := b.SetPointerViewNoTouch([]byte("k-a"), page.ValuePtr{
		FileID: page.ValueLogFileID(7),
		Offset: 10,
		Length: 3,
	}); err != nil {
		t.Fatalf("SetPointerViewNoTouch k-a: %v", err)
	}
	if err := b.SetPointerViewNoTouch([]byte("k-b"), page.ValuePtr{
		FileID: page.ValueLogFileID(9),
		Offset: 20,
		Length: 5,
	}); err != nil {
		t.Fatalf("SetPointerViewNoTouch k-b: %v", err)
	}
	if !b.HasValueLogPointers() {
		t.Fatalf("HasValueLogPointers()=false, want true")
	}
	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("TouchedValueLogSegments()=%v, want []", got)
	}
}

func TestNoteTouchedValueLogFileID(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	b.NoteTouchedValueLogFileID(123) // non-value-log ID: ignored
	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("TouchedValueLogSegments()=%v, want []", got)
	}

	b.NoteTouchedValueLogFileID(page.ValueLogFileID(4))
	b.NoteTouchedValueLogFileID(page.ValueLogFileID(2))
	b.NoteTouchedValueLogFileID(page.ValueLogFileID(4)) // duplicate
	got := b.TouchedValueLogSegments()
	want := []uint32{page.ValueLogFileID(2), page.ValueLogFileID(4)}
	if len(got) != len(want) {
		t.Fatalf("len(TouchedValueLogSegments())=%d want=%d (got=%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("TouchedValueLogSegments()[%d]=%d want=%d (all=%v)", i, got[i], want[i], got)
		}
	}
	if !b.HasValueLogPointers() {
		t.Fatalf("HasValueLogPointers()=false, want true")
	}
}

func TestAppendPointerViewNoTouchTrustedSorted(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	b.AppendPointerViewNoTouchTrustedSorted([]byte("a"), page.ValuePtr{
		FileID: page.ValueLogFileID(3),
		Offset: 10,
		Length: 2,
	})
	b.AppendPointerViewNoTouchTrustedSorted([]byte("b"), page.ValuePtr{
		FileID: page.ValueLogFileID(4),
		Offset: 20,
		Length: 2,
	})

	if !b.HasValueLogPointers() {
		t.Fatalf("HasValueLogPointers()=false, want true")
	}
	if got := b.TouchedValueLogSegments(); len(got) != 0 {
		t.Fatalf("TouchedValueLogSegments()=%v, want []", got)
	}

	entries := b.SortedEntries()
	if len(entries) != 2 {
		t.Fatalf("len(SortedEntries())=%d want=2", len(entries))
	}
	if string(entries[0].Key) != "a" || string(entries[1].Key) != "b" {
		t.Fatalf("unexpected key order: %q, %q", entries[0].Key, entries[1].Key)
	}
	if entries[0].ValuePtr.FileID != page.ValueLogFileID(3) || entries[1].ValuePtr.FileID != page.ValueLogFileID(4) {
		t.Fatalf("unexpected pointer file IDs: %d, %d", entries[0].ValuePtr.FileID, entries[1].ValuePtr.FileID)
	}
}
