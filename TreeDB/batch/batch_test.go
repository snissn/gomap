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

func TestBatchSortedEntriesMemoizesCompactionUntilMutation(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if err := b.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatalf("Set old: %v", err)
	}
	if err := b.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatalf("Set new: %v", err)
	}
	if b.compacted {
		t.Fatalf("batch compacted before SortedEntries")
	}

	first := b.SortedEntries()
	if len(first) != 1 {
		t.Fatalf("first len=%d want 1", len(first))
	}
	if got := string(first[0].Value); got != "new" {
		t.Fatalf("first value=%q want new", got)
	}
	if !b.sorted || !b.compacted {
		t.Fatalf("after first SortedEntries sorted=%v compacted=%v, want true/true", b.sorted, b.compacted)
	}

	second := b.SortedEntries()
	if len(second) != 1 {
		t.Fatalf("second len=%d want 1", len(second))
	}
	if &second[0] != &first[0] {
		t.Fatalf("second SortedEntries did not return the memoized compacted entry slice")
	}
	if !b.compacted {
		t.Fatalf("second SortedEntries invalidated compaction")
	}
}

func TestBatchSortedEntriesInvalidatesCompactionAfterMutation(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if err := b.Set([]byte("b"), []byte("first-b")); err != nil {
		t.Fatalf("Set first b: %v", err)
	}
	if err := b.Set([]byte("a"), []byte("first-a")); err != nil {
		t.Fatalf("Set first a: %v", err)
	}
	entries := b.SortedEntries()
	if len(entries) != 2 {
		t.Fatalf("initial len=%d want 2", len(entries))
	}
	if !b.compacted {
		t.Fatalf("initial SortedEntries did not mark compacted")
	}

	if err := b.Set([]byte("a"), []byte("second-a")); err != nil {
		t.Fatalf("Set second a: %v", err)
	}
	if b.compacted {
		t.Fatalf("mutation after SortedEntries did not invalidate compaction")
	}

	entries = b.SortedEntries()
	if len(entries) != 2 {
		t.Fatalf("after mutation len=%d want 2", len(entries))
	}
	if gotKey, gotValue := string(entries[0].Key), string(entries[0].Value); gotKey != "a" || gotValue != "second-a" {
		t.Fatalf("entry[0]=%q/%q want a/second-a", gotKey, gotValue)
	}
	if gotKey, gotValue := string(entries[1].Key), string(entries[1].Value); gotKey != "b" || gotValue != "first-b" {
		t.Fatalf("entry[1]=%q/%q want b/first-b", gotKey, gotValue)
	}
	if !b.sorted || !b.compacted {
		t.Fatalf("after mutation SortedEntries sorted=%v compacted=%v, want true/true", b.sorted, b.compacted)
	}
}

func TestBatchAppendViewTrustedSortedUniquePreservesCompactedState(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if err := b.AppendViewTrustedSortedUnique([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("append a: %v", err)
	}
	if err := b.AppendViewTrustedSortedUnique([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("append b: %v", err)
	}
	if err := b.AppendDeleteViewTrustedSortedUnique([]byte("c")); err != nil {
		t.Fatalf("append delete c: %v", err)
	}
	if !b.sorted || !b.compacted {
		t.Fatalf("trusted appends sorted=%v compacted=%v, want true/true", b.sorted, b.compacted)
	}

	first := b.SortedEntries()
	if len(first) != 3 {
		t.Fatalf("len(first)=%d want 3", len(first))
	}
	if &first[0] != &b.entries[0] {
		t.Fatal("trusted sorted unique appends forced a SortedEntries compaction")
	}
	second := b.SortedEntries()
	if &second[0] != &first[0] {
		t.Fatal("second SortedEntries did not reuse trusted compacted slice")
	}
	if got := string(second[0].Key); got != "a" {
		t.Fatalf("entry[0] key=%q want a", got)
	}
	if second[2].Type != OpDelete || string(second[2].Key) != "c" {
		t.Fatalf("entry[2]=%v/%q want delete c", second[2].Type, second[2].Key)
	}
}

func TestBatchAppendTrustedSortedUniqueAcceptsEmptyKeys(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	if err := b.AppendViewTrustedSortedUnique([]byte{}, []byte("value")); err != nil {
		t.Fatalf("AppendViewTrustedSortedUnique empty key error=%v", err)
	}
	if err := b.AppendDeleteViewTrustedSortedUnique([]byte{}); err != nil {
		t.Fatalf("AppendDeleteViewTrustedSortedUnique empty key error=%v", err)
	}
}

func TestBatchAppendPointerViewTrustedSortedUniqueTracksTouchedSegment(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	ptr := page.ValuePtr{
		FileID: page.ValueLogFileID(7),
		Offset: 123,
		Length: 456,
	}
	if err := b.AppendPointerViewTrustedSortedUnique([]byte("a"), ptr); err != nil {
		t.Fatalf("append pointer: %v", err)
	}
	if !b.sorted || !b.compacted {
		t.Fatalf("trusted pointer append sorted=%v compacted=%v, want true/true", b.sorted, b.compacted)
	}
	if !b.HasValueLogPointers() {
		t.Fatal("HasValueLogPointers()=false, want true")
	}
	touched := b.TouchedValueLogSegments()
	if len(touched) != 1 || touched[0] != ptr.FileID {
		t.Fatalf("TouchedValueLogSegments()=%v want [%d]", touched, ptr.FileID)
	}
	entries := b.SortedEntries()
	if len(entries) != 1 || !entries[0].IsPtr || entries[0].ValuePtr != ptr {
		t.Fatalf("entries=%+v want pointer %+v", entries, ptr)
	}
}

func TestBatchRevisionFastHelpers(t *testing.T) {
	reader := newMapValueReader()
	b := New(reader, page.DefaultInlineThreshold)
	t.Cleanup(func() { _ = b.Close() })

	ptr := reader.Add([]byte("large-value"))
	if err := b.SetViewWithRevision([]byte("a"), []byte("value-a"), 11); err != nil {
		t.Fatalf("SetViewWithRevision: %v", err)
	}
	if err := b.SetPointerViewWithRevision([]byte("b"), ptr, 12); err != nil {
		t.Fatalf("SetPointerViewWithRevision: %v", err)
	}
	if err := b.DeleteViewWithRevision([]byte("c"), 13); err != nil {
		t.Fatalf("DeleteViewWithRevision: %v", err)
	}

	entries := b.SortedEntries()
	if len(entries) != 3 {
		t.Fatalf("entries=%d want 3", len(entries))
	}
	if entries[0].Revision != 11 || entries[0].Type != OpPut || string(entries[0].Value) != "value-a" {
		t.Fatalf("entry[0]=%+v, want inline rev 11", entries[0])
	}
	if entries[1].Revision != 12 || !entries[1].IsPtr || entries[1].ValuePtr != ptr {
		t.Fatalf("entry[1]=%+v, want pointer rev 12", entries[1])
	}
	if entries[2].Revision != 13 || entries[2].Type != OpDelete {
		t.Fatalf("entry[2]=%+v, want delete rev 13", entries[2])
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
