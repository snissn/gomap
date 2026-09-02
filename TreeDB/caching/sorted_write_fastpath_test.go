package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type recordingCopySortedMem struct {
	memtable.Table
	calls             int
	selectedCalls     int
	reserveCalls      int
	reserveAdditional int
	hasMax            bool
	maxKey            []byte
}

func (m *recordingCopySortedMem) noteKey(key []byte) {
	if key == nil {
		return
	}
	if !m.hasMax || bytes.Compare(key, m.maxKey) > 0 {
		m.maxKey = append(m.maxKey[:0], key...)
		m.hasMax = true
	}
}

func (m *recordingCopySortedMem) noteEntries(entries []batch.Entry) {
	for i := range entries {
		m.noteKey(entries[i].Key)
	}
}

func (m *recordingCopySortedMem) canRecordOrderedAppend(entries []batch.Entry) bool {
	if len(entries) == 0 || entries[0].Key == nil {
		return false
	}
	return !m.hasMax || bytes.Compare(entries[0].Key, m.maxKey) > 0
}

func (m *recordingCopySortedMem) Set(key, value []byte) {
	m.noteKey(key)
	m.Table.Set(key, value)
}

func (m *recordingCopySortedMem) SetSteal(key, value []byte) {
	m.noteKey(key)
	m.Table.SetSteal(key, value)
}

func (m *recordingCopySortedMem) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.noteKey(key)
	m.Table.SetEntry(key, value, ptr, flags)
}

func (m *recordingCopySortedMem) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {
	m.noteKey(key)
	m.Table.SetEntrySteal(key, value, ptr, flags)
}

func (m *recordingCopySortedMem) Delete(key []byte) {
	m.noteKey(key)
	m.Table.Delete(key)
}

func (m *recordingCopySortedMem) DeleteSteal(key []byte) {
	m.noteKey(key)
	m.Table.DeleteSteal(key)
}

func (m *recordingCopySortedMem) ReserveAdditionalEntries(additional int) {
	m.reserveCalls++
	m.reserveAdditional += additional
	if reserver, ok := m.Table.(memtable.EntryCapacityReserver); ok {
		reserver.ReserveAdditionalEntries(additional)
	}
}

func (m *recordingCopySortedMem) ApplyCopySortedBatchTrusted(entries []batch.Entry, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	orderedAppend := m.canRecordOrderedAppend(entries)
	appliedWithSortedApplier := false
	borrowed := false
	if applier, ok := m.Table.(memtable.CopySortedBatchApplier); ok {
		borrowed = applier.ApplyCopySortedBatchTrusted(entries, borrowValues, storeInlinePtrValues, onKey)
		appliedWithSortedApplier = true
	} else {
		for _, op := range entries {
			if op.Type == batch.OpDelete {
				m.Table.Delete(op.Key)
			} else if op.IsPtr {
				m.Table.SetEntry(op.Key, op.Value, op.ValuePtr, node.FlagPointer)
			} else {
				m.Table.Set(op.Key, op.Value)
			}
			if onKey != nil {
				onKey(op.Key)
			}
		}
	}
	if orderedAppend && appliedWithSortedApplier {
		m.calls++
	}
	m.noteEntries(entries)
	return borrowed
}

func batchSelectedEntries(entries []batch.Entry, selectors []int, selector int) []batch.Entry {
	selected := make([]batch.Entry, 0, len(entries))
	for i := range entries {
		if i < len(selectors) && selectors[i] == selector {
			selected = append(selected, entries[i])
		}
	}
	return selected
}

func (m *recordingCopySortedMem) ApplyCopySelectedSortedBatchTrusted(entries []batch.Entry, selectors []int, selector int, count int, firstKey []byte, borrowValues bool, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	selected := batchSelectedEntries(entries, selectors, selector)
	if len(selected) != count {
		panic("selected entry count mismatch")
	}
	entries = selected
	orderedAppend := m.canRecordOrderedAppend(entries)
	applier, ok := m.Table.(memtable.CopySelectedSortedBatchApplier)
	if !ok {
		return m.ApplyCopySortedBatchTrusted(entries, borrowValues, storeInlinePtrValues, onKey)
	}
	selectors = make([]int, len(entries))
	borrowed := applier.ApplyCopySelectedSortedBatchTrusted(entries, selectors, 0, count, firstKey, borrowValues, storeInlinePtrValues, onKey)
	if orderedAppend {
		m.calls++
		m.selectedCalls++
	}
	m.noteEntries(entries)
	return borrowed
}

func (m *recordingCopySortedMem) ApplyCopySelectedSortedBatchWithValueCopierTrusted(entries []batch.Entry, selectors []int, selector int, count int, firstKey []byte, copyValue func(value []byte) []byte, storeInlinePtrValues bool, onKey func(key []byte)) bool {
	selected := batchSelectedEntries(entries, selectors, selector)
	if len(selected) != count {
		panic("selected entry count mismatch")
	}
	entries = selected
	orderedAppend := m.canRecordOrderedAppend(entries)
	copied := false
	selectors = make([]int, len(entries))
	if applier, ok := m.Table.(memtable.CopySelectedSortedBatchValueCopier); ok {
		copied = applier.ApplyCopySelectedSortedBatchWithValueCopierTrusted(entries, selectors, 0, count, firstKey, copyValue, storeInlinePtrValues, onKey)
	} else if applier, ok := m.Table.(memtable.CopySortedBatchValueCopier); ok {
		copied = applier.ApplyCopySortedBatchWithValueCopierTrusted(entries, copyValue, storeInlinePtrValues, onKey)
	} else {
		return m.ApplyCopySortedBatchTrusted(entries, false, storeInlinePtrValues, onKey)
	}
	if orderedAppend {
		m.calls++
		m.selectedCalls++
	}
	m.noteEntries(entries)
	return copied
}

func newSortedWriteFastPathDB(t *testing.T) (*DB, *recordingCopySortedMem) {
	t.Helper()
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold: 8 << 20,
		MemtableMode:   "append_only",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	shard := &db.mutableShards[0]
	shard.mu.Lock()
	rec := &recordingCopySortedMem{Table: shard.mem}
	shard.mem = rec
	shard.mu.Unlock()
	db.mu.Lock()
	db.publishMemtablesLocked()
	db.mu.Unlock()
	return db, rec
}

func requireCachedValue(t *testing.T, db *DB, key, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q)=%q want %q", key, got, want)
	}
}

func TestBatchWriteSortedUniqueUsesCopySortedFastPath(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set(c): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 1 {
		t.Fatalf("copy sorted fast path calls=%d want 1", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
	requireCachedValue(t, db, []byte("c"), []byte("vc"))
}

func TestBatchWriteSmallSortedUniqueUsesSelectedFastPath(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set(c): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.selectedCalls != 1 {
		t.Fatalf("copy selected sorted fast path calls=%d want 1", rec.selectedCalls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
	requireCachedValue(t, db, []byte("c"), []byte("vc"))
}

func TestBatchWriteAppendOnlyReservesShardEntriesBeforeApply(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("Set(c): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.reserveCalls != 1 {
		t.Fatalf("reserve calls=%d want 1", rec.reserveCalls)
	}
	if rec.reserveAdditional != 3 {
		t.Fatalf("reserve additional=%d want 3", rec.reserveAdditional)
	}
	if rec.calls != 1 {
		t.Fatalf("copy sorted fast path calls=%d want 1", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
	requireCachedValue(t, db, []byte("c"), []byte("vc"))
}

func TestBatchWriteSortedSetViewCopiesExternalValues(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	keyA := []byte("a")
	valA := []byte("va")
	keyB := []byte("b")
	valB := []byte("vb")
	b := db.NewBatchWithSize(2)
	if err := b.SetView(keyA, valA); err != nil {
		t.Fatalf("SetView(a): %v", err)
	}
	if err := b.SetView(keyB, valB); err != nil {
		t.Fatalf("SetView(b): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}
	valA[0] = 'z'
	valB[0] = 'z'

	if rec.calls != 1 {
		t.Fatalf("copy sorted fast path calls=%d want 1", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
}

func TestBatchWriteSortedDuplicateFallsBackToGenericLatestWins(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(3)
	if err := b.Set([]byte("a"), []byte("old")); err != nil {
		t.Fatalf("Set(a old): %v", err)
	}
	if err := b.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatalf("Set(a new): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("copy sorted fast path used for duplicate-key batch: calls=%d", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("new"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
}

func TestBatchWriteSortedOverlappingExistingKeyFallsBackToGeneric(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	if err := db.Set([]byte("a"), []byte("original")); err != nil {
		t.Fatalf("Set existing: %v", err)
	}

	b := db.NewBatchWithSize(2)
	if err := b.Set([]byte("a"), []byte("updated")); err != nil {
		t.Fatalf("Set(a updated): %v", err)
	}
	if err := b.Set([]byte("b"), []byte("new")); err != nil {
		t.Fatalf("Set(b new): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("copy sorted fast path used for overlapping-key batch: calls=%d", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("updated"))
	requireCachedValue(t, db, []byte("b"), []byte("new"))
}

func TestBatchWriteOutOfOrderFallsBackToGeneric(t *testing.T) {
	db, rec := newSortedWriteFastPathDB(t)
	defer db.Close()

	b := db.NewBatchWithSize(2)
	if err := b.Set([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := b.Set([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("copy sorted fast path used for out-of-order batch: calls=%d", rec.calls)
	}
	requireCachedValue(t, db, []byte("a"), []byte("va"))
	requireCachedValue(t, db, []byte("b"), []byte("vb"))
}
