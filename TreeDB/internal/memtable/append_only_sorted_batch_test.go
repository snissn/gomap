package memtable

import (
	"encoding/binary"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyApplyCopySortedBatchTrusted_AppendKeepsOrderedAndCopiesKeys(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	keyA := []byte("a")
	keyB := []byte("b")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyA, Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: keyB, Value: []byte("vb")},
	}
	if borrowed := m.ApplyCopySortedBatchTrusted(entries, true, true, nil); !borrowed {
		t.Fatalf("ApplyCopySortedBatchTrusted borrowed=false, want true for inline values")
	}

	// Keys must be table-owned even when values are borrowed from the caller.
	keyA[0] = 'z'
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("b")); !ok || deleted || string(got) != "vb" {
		t.Fatalf("Get(b)=(%q,%v,%v), want (vb,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	latestLen := len(m.latest) + len(m.latest64)
	count := m.count
	m.mu.RUnlock()
	if !ordered {
		t.Fatalf("ordered=false after trusted sorted unique append")
	}
	if latestDirty || latestLen != 0 {
		t.Fatalf("latest index built for ordered append: dirty=%v len=%d", latestDirty, latestLen)
	}
	if count != 2 {
		t.Fatalf("count=%d want 2", count)
	}
}

func TestAppendOnlyApplyCopySelectedSortedBatchTrusted_AppendKeepsOrderedAndCopiesKeys(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)

	keyA := []byte("a")
	keyB := []byte("b")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyA, Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: keyB, Value: []byte("vb")},
	}
	selectors := []int{0, 0}
	borrowed := m.ApplyCopySelectedSortedBatchTrusted(entries, selectors, 0, len(entries), entries[0].Key, true, true, nil)
	if !borrowed {
		t.Fatalf("ApplyCopySelectedSortedBatchTrusted borrowed=false, want true for inline values")
	}

	keyA[0] = 'z'
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("b")); !ok || deleted || string(got) != "vb" {
		t.Fatalf("Get(b)=(%q,%v,%v), want (vb,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	latestLen := len(m.latest) + len(m.latest64)
	count := m.count
	m.mu.RUnlock()
	if !ordered {
		t.Fatalf("ordered=false after trusted selected sorted append")
	}
	if latestDirty || latestLen != 0 {
		t.Fatalf("latest index built for ordered selected append: dirty=%v len=%d", latestDirty, latestLen)
	}
	if count != 2 {
		t.Fatalf("count=%d want 2", count)
	}
}

func TestAppendOnlyApplyCopySelectedSortedBatchTrusted_FallbackKeepsEntries(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("m"), []byte("old"))

	keyA := []byte("a")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyA, Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	selectors := []int{0, 0}
	borrowed := m.ApplyCopySelectedSortedBatchTrusted(entries, selectors, 0, len(entries), entries[0].Key, true, true, nil)
	if !borrowed {
		t.Fatalf("ApplyCopySelectedSortedBatchTrusted borrowed=false, want true for inline values")
	}

	keyA[0] = 'z'
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("b")); !ok || deleted || string(got) != "vb" {
		t.Fatalf("Get(b)=(%q,%v,%v), want (vb,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("m")); !ok || deleted || string(got) != "old" {
		t.Fatalf("Get(m)=(%q,%v,%v), want (old,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	runCount := len(m.sortedRuns)
	count := m.count
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after selected sorted fallback appended below existing max key")
	}
	if latestDirty {
		t.Fatalf("latestDirty=true after selected sorted fallback; latest lookup should be immediately usable")
	}
	if runCount != 2 {
		t.Fatalf("sorted run count=%d want 2", runCount)
	}
	if count != 3 {
		t.Fatalf("count=%d want 3", count)
	}
}

func TestAppendOnlyApplyCopySortedBatchWithValueCopierOwnsValues(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("a")
	value := []byte("mutable-value")
	var copiedBacking [][]byte
	copied := m.ApplyCopySortedBatchWithValueCopierTrusted([]batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   key,
		Value: value,
	}}, func(src []byte) []byte {
		dst := append([]byte(nil), src...)
		copiedBacking = append(copiedBacking, dst)
		return dst
	}, true, nil)
	if !copied {
		t.Fatalf("copied=false, want true")
	}
	key[0] = 'z'
	value[0] = 'X'
	if len(copiedBacking) != 1 || string(copiedBacking[0]) != "mutable-value" {
		t.Fatalf("copied backing mutated or missing: %q", copiedBacking)
	}
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "mutable-value" {
		t.Fatalf("Get(a)=(%q,%v,%v), want copied immutable value", string(got), deleted, ok)
	}
}

func TestAppendOnlyApplyCopySelectedSortedBatchWithValueCopierOwnsValues(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key := []byte("a")
	value := []byte("mutable-value")
	entries := []batchpkg.Entry{{
		Type:  batchpkg.OpPut,
		Key:   key,
		Value: value,
	}}
	selectors := []int{0}
	var copiedBacking [][]byte
	copied := m.ApplyCopySelectedSortedBatchWithValueCopierTrusted(entries, selectors, 0, len(entries), key, func(src []byte) []byte {
		dst := append([]byte(nil), src...)
		copiedBacking = append(copiedBacking, dst)
		return dst
	}, true, nil)
	if !copied {
		t.Fatalf("copied=false, want true")
	}
	key[0] = 'z'
	value[0] = 'X'
	if len(copiedBacking) != 1 || string(copiedBacking[0]) != "mutable-value" {
		t.Fatalf("copied backing mutated or missing: %q", copiedBacking)
	}
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "mutable-value" {
		t.Fatalf("Get(a)=(%q,%v,%v), want copied immutable value", string(got), deleted, ok)
	}
}

func TestAppendOnlyApplyCopySelectedSortedBatchWithValueCopierTrusted_FallbackKeepsEntries(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("m"), []byte("old"))

	key := []byte("a")
	value := []byte("mutable-value")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: key, Value: value},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	selectors := []int{0, 0}
	var copiedBacking [][]byte
	copied := m.ApplyCopySelectedSortedBatchWithValueCopierTrusted(entries, selectors, 0, len(entries), key, func(src []byte) []byte {
		dst := append([]byte(nil), src...)
		copiedBacking = append(copiedBacking, dst)
		return dst
	}, true, nil)
	if !copied {
		t.Fatalf("copied=false, want true")
	}

	key[0] = 'z'
	value[0] = 'X'
	if len(copiedBacking) != 2 || string(copiedBacking[0]) != "mutable-value" || string(copiedBacking[1]) != "vb" {
		t.Fatalf("copied backing mutated or missing: %q", copiedBacking)
	}
	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "mutable-value" {
		t.Fatalf("Get(a)=(%q,%v,%v), want copied immutable value", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("b")); !ok || deleted || string(got) != "vb" {
		t.Fatalf("Get(b)=(%q,%v,%v), want (vb,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("m")); !ok || deleted || string(got) != "old" {
		t.Fatalf("Get(m)=(%q,%v,%v), want (old,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	runCount := len(m.sortedRuns)
	count := m.count
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after selected value-copier fallback appended below existing max key")
	}
	if latestDirty {
		t.Fatalf("latestDirty=true after selected value-copier fallback; latest lookup should be immediately usable")
	}
	if runCount != 2 {
		t.Fatalf("sorted run count=%d want 2", runCount)
	}
	if count != 3 {
		t.Fatalf("count=%d want 3", count)
	}
}

func TestAppendOnlyApplyCopySortedBatchTrusted_PointerInlinePolicy(t *testing.T) {
	ptr := page.ValuePtr{FileID: 42, Offset: 7, Length: 3}

	m := NewAppendOnlyWithCapacity(0)
	m.ApplyCopySortedBatchTrusted([]batchpkg.Entry{{
		Type:     batchpkg.OpPut,
		Key:      []byte("a"),
		Value:    []byte("raw"),
		ValuePtr: ptr,
		IsPtr:    true,
	}}, false, false, nil)
	if got, gotPtr, flags, ok := m.GetEntry([]byte("a")); !ok || got != nil || gotPtr != ptr || flags != node.FlagPointer {
		t.Fatalf("GetEntry without inline ptr value=(%q,%+v,%d,%v), want nil,%+v,%d,true", string(got), gotPtr, flags, ok, ptr, node.FlagPointer)
	}

	withInline := NewAppendOnlyWithCapacity(0)
	withInline.ApplyCopySortedBatchTrusted([]batchpkg.Entry{{
		Type:     batchpkg.OpPut,
		Key:      []byte("a"),
		Value:    []byte("raw"),
		ValuePtr: ptr,
		IsPtr:    true,
	}}, false, true, nil)
	if got, gotPtr, flags, ok := withInline.GetEntry([]byte("a")); !ok || string(got) != "raw" || gotPtr != ptr || flags != node.FlagPointer {
		t.Fatalf("GetEntry with inline ptr value=(%q,%+v,%d,%v), want raw,%+v,%d,true", string(got), gotPtr, flags, ok, ptr, node.FlagPointer)
	}
}

func TestAppendOnlyApplyCopySortedBatchTrusted_OverlappingSortedRunUsesRunIndex(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("m"), []byte("old"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("m"), Value: []byte("new")},
	}
	m.ApplyCopySortedBatchTrusted(entries, false, true, nil)

	if got, deleted, ok := m.Get([]byte("a")); !ok || deleted || string(got) != "va" {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", string(got), deleted, ok)
	}
	if got, deleted, ok := m.Get([]byte("m")); !ok || deleted || string(got) != "new" {
		t.Fatalf("Get(m)=(%q,%v,%v), want (new,false,true)", string(got), deleted, ok)
	}

	m.mu.RLock()
	ordered := m.ordered
	latestDirty := m.latestDirty
	runCount := len(m.sortedRuns)
	latestLen := len(m.latest) + len(m.latest64)
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after sorted run overlapped existing max key")
	}
	if latestDirty {
		t.Fatalf("latestDirty=true after sorted-run index; latest lookup should be immediately usable")
	}
	if runCount != 2 {
		t.Fatalf("sorted run count=%d want 2", runCount)
	}
	if latestLen != 0 {
		t.Fatalf("hash latest index len=%d want 0 before fallback", latestLen)
	}
}

func TestAppendOnlyKey64MonotonicOrderingMatchesLexicographic(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	var lo, hi [8]byte
	binary.BigEndian.PutUint64(lo[:], 0x00ff)
	binary.BigEndian.PutUint64(hi[:], 0x0100)

	m.Set(lo[:], []byte("lo"))
	m.Set(hi[:], []byte("hi"))

	m.mu.RLock()
	ordered := m.ordered
	latestLen := len(m.latest) + len(m.latest64)
	m.mu.RUnlock()
	if !ordered {
		t.Fatalf("ordered=false for lexicographically increasing 8-byte keys")
	}
	if latestLen != 0 {
		t.Fatalf("latest index len=%d want 0 while key64 run remains ordered", latestLen)
	}

	m.Set(lo[:], []byte("lo2"))
	if got, deleted, ok := m.Get(lo[:]); !ok || deleted || string(got) != "lo2" {
		t.Fatalf("Get(lo)=(%q,%v,%v), want (lo2,false,true)", string(got), deleted, ok)
	}
	m.mu.RLock()
	ordered = m.ordered
	latestDirty := m.latestDirty
	runCount := len(m.sortedRuns)
	latestLen = len(m.latest) + len(m.latest64)
	m.mu.RUnlock()
	if ordered {
		t.Fatalf("ordered=true after lower 8-byte key append")
	}
	if latestDirty {
		t.Fatalf("latestDirty=true after key64 sorted-run order break")
	}
	if runCount != 2 {
		t.Fatalf("sorted run count=%d want 2", runCount)
	}
	if latestLen != 0 {
		t.Fatalf("hash latest index len=%d want 0 before fallback", latestLen)
	}
}
