package caching

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointerBatch struct {
	entries []batch.Entry
}

func (b *pointerBatch) Set(key, value []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, Value: value})
	return nil
}

func (b *pointerBatch) SetView(key, value []byte) error {
	return b.Set(key, value)
}

func (b *pointerBatch) Delete(key []byte) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: key})
	return nil
}

func (b *pointerBatch) DeleteView(key []byte) error {
	return b.Delete(key)
}

func (b *pointerBatch) SetPointer(key []byte, ptr page.ValuePtr) error {
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, ValuePtr: ptr, IsPtr: true})
	return nil
}

func (b *pointerBatch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.SetPointer(key, ptr)
}

func (b *pointerBatch) SetOps(ops []batch.Entry) error {
	b.entries = append(b.entries, ops...)
	return nil
}

func (b *pointerBatch) Write() error { return nil }

func (b *pointerBatch) WriteSync() error { return nil }

func (b *pointerBatch) Close() error { return nil }

func (b *pointerBatch) Replay(fn func(batch.Entry) error) error {
	for _, e := range b.entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func (b *pointerBatch) GetByteSize() (int, error) { return len(b.entries), nil }

func TestPutEntryRunsClearsReferences(t *testing.T) {
	runs := make([][]batch.Entry, 2)
	runs[0] = []batch.Entry{{Type: batch.OpPut, Key: []byte("k0"), Value: []byte("v0")}}
	runs[1] = []batch.Entry{{Type: batch.OpDelete, Key: []byte("k1")}}

	putEntryRuns(runs)

	for i := range runs {
		if runs[i] != nil {
			t.Fatalf("run %d not cleared", i)
		}
	}
}

func TestPutUnitRunsClearsReferences(t *testing.T) {
	unitRuns := make([][][]batch.Entry, 2)
	unitRuns[0] = [][]batch.Entry{{{Type: batch.OpPut, Key: []byte("k0"), Value: []byte("v0")}}}
	unitRuns[1] = [][]batch.Entry{{{Type: batch.OpDelete, Key: []byte("k1")}}}

	putUnitRuns(unitRuns)

	for i := range unitRuns {
		if unitRuns[i] != nil {
			t.Fatalf("unit runs %d not cleared", i)
		}
	}
}

func TestPutOpMergeHeapClearsReferences(t *testing.T) {
	h := make(opMergeHeap, 1, 2)
	h[0] = opMergeItem{
		iter:     &opRunIter{valid: true},
		priority: 7,
		key:      []byte("k"),
	}

	putOpMergeHeap(h)

	if h[0].iter != nil || h[0].priority != 0 || h[0].key != nil {
		t.Fatalf("heap item not cleared: %+v", h[0])
	}
}

func TestBuildOpRunsChunking(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("NewWithCapacityMode: %v", err)
	}
	var key [8]byte
	for i := 0; i < 5; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], []byte{byte(i + 1)})
	}
	binary.BigEndian.PutUint64(key[:], uint64(2))
	mt.Delete(key[:])
	mt.Freeze()

	runs, deleteOps, err := buildOpRuns(mt, 2)
	if err != nil {
		t.Fatalf("buildOpRuns: %v", err)
	}
	defer func() {
		for _, run := range runs {
			putEntrySlice(run)
		}
		putEntryRuns(runs)
	}()

	if deleteOps != 1 {
		t.Fatalf("deleteOps=%d want=1", deleteOps)
	}
	if len(runs) == 0 {
		t.Fatalf("expected at least one run")
	}
	for i, run := range runs {
		if len(run) == 0 || len(run) > 2 {
			t.Fatalf("run %d has unexpected size %d", i, len(run))
		}
	}
}

func TestFlushDeferredValueLogUnitsPointerOnly(t *testing.T) {
	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("NewWithCapacityMode: %v", err)
	}
	entries := []struct {
		key string
		ptr page.ValuePtr
	}{
		{key: "a", ptr: page.ValuePtr{Offset: 10, Length: 1, FileID: 1}},
		{key: "b", ptr: page.ValuePtr{Offset: 20, Length: 2, FileID: 1}},
		{key: "c", ptr: page.ValuePtr{Offset: 30, Length: 3, FileID: 1}},
	}
	for _, e := range entries {
		mt.SetEntry([]byte(e.key), nil, e.ptr, node.FlagPointer)
	}
	mt.Freeze()

	db := &DB{
		flushBuildChunkCap: 2,
	}
	backendBatch := &pointerBatch{}

	pendingOps, err := db.flushDeferredValueLogUnits([]flushUnit{{mem: mt}}, backendBatch, false, 0)
	if err != nil {
		t.Fatalf("flushDeferredValueLogUnits: %v", err)
	}
	if pendingOps != len(entries) {
		t.Fatalf("pendingOps=%d want=%d", pendingOps, len(entries))
	}

	if len(backendBatch.entries) != len(entries) {
		t.Fatalf("backend batch entries=%d want=%d", len(backendBatch.entries), len(entries))
	}
	for i, e := range backendBatch.entries {
		if !e.IsPtr {
			t.Fatalf("entry %d expected pointer op", i)
		}
	}
}

func TestEstimateUnitRunEntries(t *testing.T) {
	unitRuns := [][][]batch.Entry{
		{
			{{Type: batch.OpPut, Key: []byte("a"), Value: []byte("1")}},
			{{Type: batch.OpPut, Key: []byte("b"), Value: []byte("2")}, {Type: batch.OpDelete, Key: []byte("c")}},
		},
		{
			{{Type: batch.OpPut, Key: []byte("d"), Value: []byte("3")}},
		},
	}
	got := estimateUnitRunEntries(unitRuns, 2)
	if got != 6 {
		t.Fatalf("estimateUnitRunEntries=%d want=6", got)
	}
}

func TestValueLogKeyLeaseRoundTrip(t *testing.T) {
	valueLogKeyLeaseMu.Lock()
	saved := valueLogKeyLeases
	valueLogKeyLeases = nil
	valueLogKeyLeaseMu.Unlock()
	t.Cleanup(func() {
		valueLogKeyLeaseMu.Lock()
		valueLogKeyLeases = saved
		valueLogKeyLeaseMu.Unlock()
	})

	keys := make([][]byte, 1, 128)
	keys[0] = []byte("k0")
	first := &keys[0]
	putValueLogKeys(keys)

	valueLogKeyLeaseMu.Lock()
	if len(valueLogKeyLeases) != 1 {
		valueLogKeyLeaseMu.Unlock()
		t.Fatalf("expected one leased key slice, got %d", len(valueLogKeyLeases))
	}
	valueLogKeyLeaseMu.Unlock()

	got := getValueLogKeys(1)
	got = append(got, nil)
	if &got[0] != first {
		t.Fatalf("expected leased key-slice reuse")
	}
}

func TestEntrySliceLeaseRoundTrip(t *testing.T) {
	entrySliceLeaseMu.Lock()
	saved := entrySliceLeases
	entrySliceLeases = nil
	entrySliceLeaseMu.Unlock()
	t.Cleanup(func() {
		entrySliceLeaseMu.Lock()
		entrySliceLeases = saved
		entrySliceLeaseMu.Unlock()
	})

	entries := make([]batch.Entry, 1, 256)
	entries[0] = batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}
	first := &entries[0]
	putEntrySlice(entries)

	entrySliceLeaseMu.Lock()
	if len(entrySliceLeases) != 1 {
		entrySliceLeaseMu.Unlock()
		t.Fatalf("expected one leased entry slice, got %d", len(entrySliceLeases))
	}
	entrySliceLeaseMu.Unlock()

	got := getEntrySlice(1)
	got = append(got, batch.Entry{})
	if &got[0] != first {
		t.Fatalf("expected leased entry-slice reuse")
	}
}

func TestAppendOnlyMemtableLeaseReuse(t *testing.T) {
	db := &DB{}
	mt := memtable.NewAppendOnlyWithCapacity(4096)

	db.recycleMemtables([]memtable.Table{mt})

	got, err := db.newMutableMemtableWithCapacityMode(0, memtable.ModeAppendOnly)
	if err != nil {
		t.Fatalf("newMutableMemtableWithCapacityMode: %v", err)
	}
	gotAppendOnly, ok := got.(*memtable.AppendOnly)
	if !ok {
		t.Fatalf("expected append-only memtable, got %T", got)
	}
	if gotAppendOnly != mt {
		t.Fatalf("expected leased append-only memtable reuse")
	}
}

type stubMergingIterator struct {
	closeCalls int
}

func (it *stubMergingIterator) Next()                       {}
func (it *stubMergingIterator) Valid() bool                 { return false }
func (it *stubMergingIterator) Key() []byte                 { return nil }
func (it *stubMergingIterator) Value() []byte               { return nil }
func (it *stubMergingIterator) KeyCopy(dst []byte) []byte   { return dst[:0] }
func (it *stubMergingIterator) ValueCopy(dst []byte) []byte { return dst[:0] }
func (it *stubMergingIterator) Error() error                { return nil }
func (it *stubMergingIterator) Domain() (start, end []byte) { return nil, nil }
func (it *stubMergingIterator) Close() error                { it.closeCalls++; return nil }

func TestLeasedMergingIteratorCloseReleasesOnce(t *testing.T) {
	base := &stubMergingIterator{}
	released := 0
	it := &leasedMergingIterator{
		Iterator: base,
		release: func() {
			released++
		},
	}

	if err := it.Close(); err != nil {
		t.Fatalf("Close() first: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close() second: %v", err)
	}
	if base.closeCalls != 1 {
		t.Fatalf("base close calls=%d want=1", base.closeCalls)
	}
	if released != 1 {
		t.Fatalf("release calls=%d want=1", released)
	}
}
