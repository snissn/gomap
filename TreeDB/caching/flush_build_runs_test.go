package caching

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func buildRunKey(v uint64) []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], v)
	return append([]byte(nil), key[:]...)
}

func collectRunEntriesForTest(t *testing.T, m memtable.Table, chunkCap int) ([]batch.Entry, int) {
	t.Helper()
	runs, deletes, err := buildOpRuns(m, chunkCap)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()
	var out []batch.Entry
	for _, run := range runs {
		out = append(out, run...)
	}
	return out, deletes
}

func TestMemtableBatchApplyPreservesEntryRevision(t *testing.T) {
	var mt memtable.Table = memtable.NewAppendOnlyWithEntryCapacity(4)
	memtableBatchSet(mt, false, false, false, batch.Entry{
		Type:     batch.OpPut,
		Key:      []byte("a"),
		Value:    []byte("value"),
		Revision: 44,
	})
	if err := memtableBatchDelete(mt, false, []byte("b"), 45); err != nil {
		t.Fatalf("memtableBatchDelete: %v", err)
	}

	revisions := mt.(memtable.RevisionTable)
	value, _, flags, revision, found := revisions.GetEntryWithRevision([]byte("a"))
	if !found || flags != node.FlagInline || string(value) != "value" || revision != 44 {
		t.Fatalf("put entry=(%q,%02x,%d,%v), want value inline rev 44", value, flags, revision, found)
	}
	_, _, flags, revision, found = revisions.GetEntryWithRevision([]byte("b"))
	if !found || flags&node.FlagTombstone == 0 || revision != 45 {
		t.Fatalf("delete entry=(%02x,%d,%v), want tombstone rev 45", flags, revision, found)
	}
}

func TestMemtableBatchDeleteRejectsRevisionWithoutRevisionTable(t *testing.T) {
	mt := &unstableRunTable{}
	if err := memtableBatchDelete(mt, false, []byte("b"), 45); err == nil {
		t.Fatalf("memtableBatchDelete succeeded without revision-preserving memtable")
	}
}

func TestBuildOpRunsAppendOnlySortedDuplicateTombstoneDeleteShapes(t *testing.T) {
	m := memtable.NewAppendOnlyWithEntryCapacity(16)
	m.Set(buildRunKey(1), []byte("old-1"))
	m.Set(buildRunKey(3), []byte("value-3"))
	m.Set(buildRunKey(2), []byte("value-2")) // order break: creates sorted-run state
	m.Set(buildRunKey(1), []byte("new-1"))   // duplicate latest wins
	m.Delete(buildRunKey(3))                 // tombstone latest wins
	m.Set(buildRunKey(4), []byte("value-4"))
	m.Freeze()

	entries, deletes := collectRunEntriesForTest(t, m, 2)
	if deletes != 1 {
		t.Fatalf("delete count=%d want 1", deletes)
	}
	if len(entries) != 4 {
		t.Fatalf("entries=%d want 4: %+v", len(entries), entries)
	}
	want := []struct {
		key    uint64
		op     batch.OpType
		value  string
		isTomb bool
	}{
		{key: 1, op: batch.OpPut, value: "new-1"},
		{key: 2, op: batch.OpPut, value: "value-2"},
		{key: 3, op: batch.OpDelete, isTomb: true},
		{key: 4, op: batch.OpPut, value: "value-4"},
	}
	for i := range want {
		gotKey := binary.BigEndian.Uint64(entries[i].Key)
		if gotKey != want[i].key || entries[i].Type != want[i].op {
			t.Fatalf("entry[%d] key/op=(%d,%d) want (%d,%d)", i, gotKey, entries[i].Type, want[i].key, want[i].op)
		}
		if !want[i].isTomb && string(entries[i].Value) != want[i].value {
			t.Fatalf("entry[%d] value=%q want %q", i, entries[i].Value, want[i].value)
		}
	}
}

func TestBuildOpRunsAppendOnlyRandomDeleteHeavyLatestWins(t *testing.T) {
	m := memtable.NewAppendOnlyWithEntryCapacity(128)
	for i := 0; i < 64; i++ {
		k := uint64((i*37 + 11) % 64)
		if i%3 == 0 {
			m.Delete(buildRunKey(k))
			continue
		}
		m.Set(buildRunKey(k), []byte(fmt.Sprintf("v-%02d", i)))
	}
	m.Freeze()

	entries, deletes := collectRunEntriesForTest(t, m, 7)
	if len(entries) == 0 {
		t.Fatalf("expected random/delete-heavy run output")
	}
	seenDeletes := 0
	for i := 1; i < len(entries); i++ {
		if bytes.Compare(entries[i-1].Key, entries[i].Key) >= 0 {
			t.Fatalf("entries not strictly sorted/latest at %d: %x >= %x", i, entries[i-1].Key, entries[i].Key)
		}
	}
	for _, entry := range entries {
		if entry.Type == batch.OpDelete {
			seenDeletes++
		}
	}
	if deletes != seenDeletes {
		t.Fatalf("delete count=%d want seen tombstones %d", deletes, seenDeletes)
	}
}

type unstableRunTable struct {
	entries []batch.Entry
}

func (t *unstableRunTable) Set(key, value []byte)                                     {}
func (t *unstableRunTable) SetEntry(key, value []byte, ptr page.ValuePtr, flags byte) {}
func (t *unstableRunTable) PutWithCallback(key, value []byte, cb func(k, v []byte) error) error {
	return nil
}
func (t *unstableRunTable) Delete(key []byte) {}
func (t *unstableRunTable) DeleteWithCallback(key []byte, cb func(k, v []byte) error) error {
	return nil
}
func (t *unstableRunTable) SetSteal(key, value []byte)                                     {}
func (t *unstableRunTable) SetEntrySteal(key, value []byte, ptr page.ValuePtr, flags byte) {}
func (t *unstableRunTable) DeleteSteal(key []byte)                                         {}
func (t *unstableRunTable) Get(key []byte) ([]byte, bool, bool)                            { return nil, false, false }
func (t *unstableRunTable) GetEntry(key []byte) ([]byte, page.ValuePtr, byte, bool) {
	return nil, page.ValuePtr{}, 0, false
}
func (t *unstableRunTable) Size() int64 { return 0 }
func (t *unstableRunTable) Len() int    { return len(t.entries) }
func (t *unstableRunTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return &unstableRunIterator{entries: t.entries}
}
func (t *unstableRunTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return &unstableRunIterator{entries: t.entries}
}
func (t *unstableRunTable) Freeze() {}

type unstableRunIterator struct {
	entries []batch.Entry
	idx     int
	keyBuf  []byte
	valBuf  []byte
}

func (it *unstableRunIterator) Valid() bool     { return it.idx < len(it.entries) }
func (it *unstableRunIterator) Next()           { it.idx++ }
func (it *unstableRunIterator) Seek(key []byte) {}
func (it *unstableRunIterator) Key() []byte     { return it.UnsafeKey() }
func (it *unstableRunIterator) Value() []byte   { return it.UnsafeValue() }
func (it *unstableRunIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}
func (it *unstableRunIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}
func (it *unstableRunIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	it.keyBuf = append(it.keyBuf[:0], it.entries[it.idx].Key...)
	return it.keyBuf
}
func (it *unstableRunIterator) UnsafeValue() []byte {
	if !it.Valid() || it.entries[it.idx].Type == batch.OpDelete {
		return nil
	}
	it.valBuf = append(it.valBuf[:0], it.entries[it.idx].Value...)
	return it.valBuf
}
func (it *unstableRunIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	entry := it.entries[it.idx]
	if entry.Type == batch.OpDelete {
		return nil, page.ValuePtr{}, 0x02
	}
	return it.UnsafeValue(), entry.ValuePtr, 0
}
func (it *unstableRunIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.idx].Type == batch.OpDelete
}
func (it *unstableRunIterator) Error() error             { return nil }
func (it *unstableRunIterator) Close() error             { return nil }
func (it *unstableRunIterator) Domain() ([]byte, []byte) { return nil, nil }

type stableRunTable struct {
	unstableRunTable
	iterErr    error
	closeErr   error
	closeCount *int
}

func (t *stableRunTable) StableUnsafeIteratorSlices() bool { return true }

func (t *stableRunTable) NewIterator(start, end []byte) iterator.UnsafeIterator {
	return &stableRunIterator{
		entries:    t.entries,
		iterErr:    t.iterErr,
		closeErr:   t.closeErr,
		closeCount: t.closeCount,
	}
}

func (t *stableRunTable) NewReverseIterator(start, end []byte) iterator.UnsafeIterator {
	return t.NewIterator(start, end)
}

type stableRunIterator struct {
	entries    []batch.Entry
	idx        int
	iterErr    error
	closeErr   error
	closeCount *int
}

func (it *stableRunIterator) Valid() bool     { return it.idx < len(it.entries) }
func (it *stableRunIterator) Next()           { it.idx++ }
func (it *stableRunIterator) Seek(key []byte) {}
func (it *stableRunIterator) Key() []byte     { return it.UnsafeKey() }
func (it *stableRunIterator) Value() []byte   { return it.UnsafeValue() }
func (it *stableRunIterator) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeKey()...)
}
func (it *stableRunIterator) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.UnsafeValue()...)
}
func (it *stableRunIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].Key
}
func (it *stableRunIterator) UnsafeValue() []byte {
	if !it.Valid() || it.entries[it.idx].Type == batch.OpDelete || it.entries[it.idx].IsPtr {
		return nil
	}
	return it.entries[it.idx].Value
}
func (it *stableRunIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags, _ := it.UnsafeEntryWithRevision()
	return val, ptr, flags
}
func (it *stableRunIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0, page.LegacyEntryRevision
	}
	entry := it.entries[it.idx]
	switch {
	case entry.Type == batch.OpDelete:
		return nil, page.ValuePtr{}, node.FlagTombstone, entry.Revision
	case entry.IsPtr:
		return nil, entry.ValuePtr, node.FlagPointer, entry.Revision
	default:
		return entry.Value, page.ValuePtr{}, node.FlagInline, entry.Revision
	}
}
func (it *stableRunIterator) IsDeleted() bool {
	return it.Valid() && it.entries[it.idx].Type == batch.OpDelete
}
func (it *stableRunIterator) Error() error { return it.iterErr }
func (it *stableRunIterator) Close() error {
	if it.closeCount != nil {
		(*it.closeCount)++
	}
	return it.closeErr
}
func (it *stableRunIterator) Domain() ([]byte, []byte) { return nil, nil }

func TestBuildOpRunsCopiesUnstableIteratorScratch(t *testing.T) {
	tbl := &unstableRunTable{entries: []batch.Entry{
		{Type: batch.OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Type: batch.OpPut, Key: []byte("k2"), Value: []byte("v2")},
	}}
	runs, _, err := buildOpRuns(tbl, 8)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()
	if len(runs) != 1 || len(runs[0]) != 2 {
		t.Fatalf("runs=%v", runs)
	}
	// Force the iterator scratch buffers to be reused and mutated; buildOpRuns
	// must not retain those transient views for non-stable iterator tables.
	it := tbl.NewIterator(nil, nil)
	_ = it.UnsafeKey()
	_ = it.UnsafeValue()
	copy(it.(*unstableRunIterator).keyBuf, []byte("XX"))
	copy(it.(*unstableRunIterator).valBuf, []byte("YY"))
	if string(runs[0][0].Key) != "k1" || string(runs[0][0].Value) != "v1" {
		t.Fatalf("run retained unstable iterator scratch: key=%q value=%q", runs[0][0].Key, runs[0][0].Value)
	}
}

func TestMergeCanonicalStableIteratorUnitsDuplicateTombstonePointer(t *testing.T) {
	older := memtable.NewAppendOnlyWithEntryCapacity(4)
	older.Set([]byte("dup"), []byte("old"))
	older.Set([]byte("old-only"), []byte("kept-old"))
	older.Freeze()

	ptr := page.ValuePtr{FileID: 7, Offset: 88, Length: 9}
	newer := memtable.NewAppendOnlyWithEntryCapacity(4)
	newer.Set([]byte("dup"), []byte("new"))
	newer.Delete([]byte("gone"))
	newer.SetEntry([]byte("ptr"), nil, ptr, node.FlagPointer)
	newer.Freeze()

	units := []flushUnit{{mem: older}, {mem: newer}}
	db := &DB{}
	if !db.canStreamCanonicalPointUnitsFromStableIterators(units, 0) {
		t.Fatalf("stable append-only units were not eligible for iterator streaming")
	}

	sourcePointOps, sourceDeleteOps, err := countCanonicalStableIteratorUnits(units)
	if err != nil {
		t.Fatalf("countCanonicalStableIteratorUnits: %v", err)
	}
	if sourcePointOps != 5 || sourceDeleteOps != 1 {
		t.Fatalf("source counts=(%d,%d) want (5,1)", sourcePointOps, sourceDeleteOps)
	}

	run := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  sourcePointOps,
		deletePointOps:  sourceDeleteOps,
	}
	var out []batch.Entry
	if err := mergeCanonicalStableIteratorUnits(units, run, func(entry batch.Entry) error {
		out = append(out, entry)
		return nil
	}); err != nil {
		t.Fatalf("mergeCanonicalStableIteratorUnits: %v", err)
	}

	if run.shadowedPointOps != 1 || run.plannedPointOps != 4 || run.deletePointOps != 1 {
		t.Fatalf("run stats shadowed/planned/deletes=(%d,%d,%d), want (1,4,1)", run.shadowedPointOps, run.plannedPointOps, run.deletePointOps)
	}
	if len(out) != 4 {
		t.Fatalf("merged entries=%d want 4: %+v", len(out), out)
	}
	want := []struct {
		key   string
		op    batch.OpType
		value string
		ptr   page.ValuePtr
		isPtr bool
	}{
		{key: "dup", op: batch.OpPut, value: "new"},
		{key: "gone", op: batch.OpDelete},
		{key: "old-only", op: batch.OpPut, value: "kept-old"},
		{key: "ptr", op: batch.OpPut, ptr: ptr, isPtr: true},
	}
	for i := range want {
		if string(out[i].Key) != want[i].key || out[i].Type != want[i].op {
			t.Fatalf("entry[%d] key/op=(%q,%d) want (%q,%d)", i, out[i].Key, out[i].Type, want[i].key, want[i].op)
		}
		if want[i].isPtr {
			if !out[i].IsPtr || out[i].ValuePtr != want[i].ptr || out[i].Value != nil {
				t.Fatalf("entry[%d] ptr=(%v,%+v,%q) want ptr %+v", i, out[i].IsPtr, out[i].ValuePtr, out[i].Value, want[i].ptr)
			}
			continue
		}
		if out[i].Type == batch.OpPut && string(out[i].Value) != want[i].value {
			t.Fatalf("entry[%d] value=%q want %q", i, out[i].Value, want[i].value)
		}
	}
}

func TestCanStreamCanonicalPointUnitsFromStableIteratorsRejectsFallbackCases(t *testing.T) {
	stable := memtable.NewAppendOnlyWithEntryCapacity(1)
	stable.Set([]byte("k"), []byte("v"))
	stable.Freeze()
	unstable := &unstableRunTable{entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}}}

	db := &DB{}
	if !db.canStreamCanonicalPointUnitsFromStableIterators([]flushUnit{{mem: stable}}, 0) {
		t.Fatalf("stable span-free units should be eligible")
	}
	if db.canStreamCanonicalPointUnitsFromStableIterators([]flushUnit{{mem: stable}}, 1) {
		t.Fatalf("range-span units must use the materializing fallback")
	}
	if db.canStreamCanonicalPointUnitsFromStableIterators([]flushUnit{{mem: unstable}}, 0) {
		t.Fatalf("unstable iterator units must use the materializing fallback")
	}
	db.flushSpanRunTargetPlanning = true
	if db.canStreamCanonicalPointUnitsFromStableIterators([]flushUnit{{mem: stable}}, 0) {
		t.Fatalf("span-target planning must use the materializing fallback")
	}
}

func TestMergeCanonicalStableIteratorUnitsClosesIteratorsOnEmitError(t *testing.T) {
	closeCount := 0
	units := []flushUnit{
		{mem: &stableRunTable{
			unstableRunTable: unstableRunTable{entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("1")}}},
			closeCount:       &closeCount,
		}},
		{mem: &stableRunTable{
			unstableRunTable: unstableRunTable{entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("b"), Value: []byte("2")}}},
			closeCount:       &closeCount,
		}},
	}
	emitErr := errors.New("emit stopped")
	err := mergeCanonicalStableIteratorUnits(units, &canonicalFlushRun{}, func(batch.Entry) error {
		return emitErr
	})
	if !errors.Is(err, emitErr) {
		t.Fatalf("merge error=%v want %v", err, emitErr)
	}
	if closeCount != len(units) {
		t.Fatalf("closed iterators=%d want %d", closeCount, len(units))
	}
}

func TestMergeCanonicalStableIteratorUnitsReturnsIteratorErrorAndCloses(t *testing.T) {
	closeCount := 0
	iterErr := errors.New("iterator failed")
	units := []flushUnit{{mem: &stableRunTable{
		unstableRunTable: unstableRunTable{entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("1")}}},
		iterErr:          iterErr,
		closeCount:       &closeCount,
	}}}
	err := mergeCanonicalStableIteratorUnits(units, &canonicalFlushRun{}, nil)
	if !errors.Is(err, iterErr) {
		t.Fatalf("merge error=%v want %v", err, iterErr)
	}
	if closeCount != 1 {
		t.Fatalf("closed iterators=%d want 1", closeCount)
	}
}

func benchmarkAppendOnlyRunShape(b *testing.B, writes int, shape string) memtable.Table {
	b.Helper()
	m := memtable.NewAppendOnlyWithEntryCapacity(writes)
	for i := 0; i < writes; i++ {
		var k uint64
		switch shape {
		case "sorted":
			k = uint64(i)
		case "duplicate_tombstone":
			k = uint64(i % (writes / 4))
		case "random_delete_heavy":
			k = uint64((i*1103515245 + 12345) & (writes - 1))
		default:
			k = uint64(i)
		}
		key := buildRunKey(k)
		if (shape == "duplicate_tombstone" && i%11 == 0) || (shape == "random_delete_heavy" && i%3 == 0) {
			m.Delete(key)
			continue
		}
		m.Set(key, []byte("value-payload"))
	}
	m.Freeze()
	return m
}

func BenchmarkBuildOpRunsAppendOnlyShapes(b *testing.B) {
	const writes = 1 << 15
	for _, shape := range []string{"sorted", "duplicate_tombstone", "random_delete_heavy"} {
		b.Run(shape, func(b *testing.B) {
			m := benchmarkAppendOnlyRunShape(b, writes, shape)
			runs, _, err := buildOpRuns(m, 8192)
			if err != nil {
				b.Fatalf("warm buildOpRuns: %v", err)
			}
			for _, run := range runs {
				putEntrySlice(run)
			}
			putEntryRuns(runs)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runs, _, err := buildOpRuns(m, 8192)
				if err != nil {
					b.Fatalf("buildOpRuns: %v", err)
				}
				for _, run := range runs {
					putEntrySlice(run)
				}
				putEntryRuns(runs)
			}
		})
	}
}

func benchmarkCanonicalFlushUnits(b *testing.B, unitCount, entriesPerUnit int) ([]flushUnit, int) {
	b.Helper()
	units := make([]flushUnit, 0, unitCount)
	totalLen := 0
	value := []byte("value-payload")
	for unit := 0; unit < unitCount; unit++ {
		m := memtable.NewAppendOnlyWithEntryCapacity(entriesPerUnit)
		base := unit * entriesPerUnit / 2
		for i := 0; i < entriesPerUnit; i++ {
			key := buildRunKey(uint64(base + i))
			if i%17 == 0 {
				m.Delete(key)
				continue
			}
			m.Set(key, value)
		}
		m.Freeze()
		units = append(units, flushUnit{mem: m, memLen: m.Len(), memBytes: m.Size()})
		totalLen += m.Len()
	}
	return units, totalLen
}

func benchmarkEmitCanonicalEntryChunk(b *testing.B, ops *[]batch.Entry, chunkCap int, entry batch.Entry) {
	b.Helper()
	if len(*ops) >= chunkCap {
		putEntrySlice(*ops)
		*ops = getEntrySlice(chunkCap)
	}
	*ops = append(*ops, entry)
}

func benchmarkMaterializedCanonicalFlushMerge(b *testing.B, db *DB, units []flushUnit, totalLen int, chunkCap int) {
	b.Helper()
	build, err := db.buildCanonicalUnitRuns(units, totalLen, 0)
	if err != nil {
		b.Fatalf("buildCanonicalUnitRuns: %v", err)
	}
	defer build.release()

	run := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  build.sourcePointOps,
		deletePointOps:  build.sourceDeleteOps,
	}
	ops := getEntrySlice(chunkCap)
	defer func() { putEntrySlice(ops) }()
	if err := mergeCanonicalUnitRuns(build.unitRuns, run, func(entry batch.Entry) error {
		benchmarkEmitCanonicalEntryChunk(b, &ops, chunkCap, entry)
		return nil
	}); err != nil {
		b.Fatalf("mergeCanonicalUnitRuns: %v", err)
	}
	if run.sourcePointOps != run.plannedPointOps+run.shadowedPointOps {
		b.Fatalf("materialized source=%d planned=%d shadowed=%d", run.sourcePointOps, run.plannedPointOps, run.shadowedPointOps)
	}
}

func benchmarkStableIteratorCanonicalFlushMerge(b *testing.B, units []flushUnit, chunkCap int) {
	b.Helper()
	sourcePointOps, sourceDeleteOps, err := countCanonicalStableIteratorUnits(units)
	if err != nil {
		b.Fatalf("countCanonicalStableIteratorUnits: %v", err)
	}
	run := &canonicalFlushRun{
		sourceMemtables: len(units),
		sourcePointOps:  sourcePointOps,
		deletePointOps:  sourceDeleteOps,
	}
	ops := getEntrySlice(chunkCap)
	defer func() { putEntrySlice(ops) }()
	if err := mergeCanonicalStableIteratorUnits(units, run, func(entry batch.Entry) error {
		benchmarkEmitCanonicalEntryChunk(b, &ops, chunkCap, entry)
		return nil
	}); err != nil {
		b.Fatalf("mergeCanonicalStableIteratorUnits: %v", err)
	}
	if run.sourcePointOps != run.plannedPointOps+run.shadowedPointOps {
		b.Fatalf("stable iterator source=%d planned=%d shadowed=%d", run.sourcePointOps, run.plannedPointOps, run.shadowedPointOps)
	}
}

func benchmarkDisableEntrySlicePooling(b *testing.B) {
	b.Helper()
	lockEntrySlicePoolStateForTest(b)
	resetEntrySlicePoolStateForTest(b)
	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBudgetBytes = 0
	b.Cleanup(func() {
		entrySlicePoolBudgetBytes = savedBudget
	})
}

func BenchmarkCanonicalFlushStableIteratorVsMaterialized(b *testing.B) {
	const (
		unitCount      = 4
		entriesPerUnit = 4096
		chunkCap       = 1024
	)
	units, totalLen := benchmarkCanonicalFlushUnits(b, unitCount, entriesPerUnit)
	db := &DB{flushBuildChunkCap: chunkCap}

	b.Run("materialized_unit_runs_cold_descriptor_pool", func(b *testing.B) {
		benchmarkDisableEntrySlicePooling(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkMaterializedCanonicalFlushMerge(b, db, units, totalLen, chunkCap)
		}
	})
	b.Run("stable_iterators_cold_descriptor_pool", func(b *testing.B) {
		benchmarkDisableEntrySlicePooling(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkStableIteratorCanonicalFlushMerge(b, units, chunkCap)
		}
	})
}

func TestOpMergeHeapEightByteDuplicatePriorityAndTombstone(t *testing.T) {
	older := []batch.Entry{
		{Type: batch.OpPut, Key: buildRunKey(1), Value: []byte("old-1")},
		{Type: batch.OpPut, Key: buildRunKey(2), Value: []byte("old-2")},
	}
	newer := []batch.Entry{
		{Type: batch.OpDelete, Key: buildRunKey(1)},
		{Type: batch.OpPut, Key: buildRunKey(3), Value: []byte("new-3")},
	}
	runs := [][]batch.Entry{older, newer}
	heap := getOpMergeHeap(len(runs))
	defer putOpMergeHeap(heap)
	for i := range runs {
		it := newOpRunIter([][]batch.Entry{runs[i]})
		priority := len(runs) - 1 - i
		heap = append(heap, opMergeItem{iter: it, priority: priority, key: it.Key()})
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		(&heap).down(i, len(heap))
	}

	var out []batch.Entry
	for len(heap) > 0 {
		top := heap.pop()
		currentKey := top.key
		for len(heap) > 0 {
			next := heap.peek()
			if next == nil || !bytes.Equal(next.key, currentKey) {
				break
			}
			shadowed := heap.pop()
			shadowed.iter.Next()
			if shadowed.iter.Valid() {
				shadowed.key = shadowed.iter.Key()
				heap.push(shadowed)
			}
		}
		out = append(out, top.iter.Entry())
		top.iter.Next()
		if top.iter.Valid() {
			top.key = top.iter.Key()
			heap.push(top)
		}
	}
	if len(out) != 3 {
		t.Fatalf("merged entries=%d want 3: %+v", len(out), out)
	}
	if binary.BigEndian.Uint64(out[0].Key) != 1 || out[0].Type != batch.OpDelete {
		t.Fatalf("first merged entry = key %d type %d, want delete tombstone for key 1", binary.BigEndian.Uint64(out[0].Key), out[0].Type)
	}
	if binary.BigEndian.Uint64(out[1].Key) != 2 || string(out[1].Value) != "old-2" {
		t.Fatalf("second merged entry = key %d value %q, want key 2 old-2", binary.BigEndian.Uint64(out[1].Key), out[1].Value)
	}
	if binary.BigEndian.Uint64(out[2].Key) != 3 || string(out[2].Value) != "new-3" {
		t.Fatalf("third merged entry = key %d value %q, want key 3 new-3", binary.BigEndian.Uint64(out[2].Key), out[2].Value)
	}
}
