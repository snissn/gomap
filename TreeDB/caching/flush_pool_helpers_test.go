package caching

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type pointerBatch struct {
	entries      []batch.Entry
	reserveCalls int
	lastReserve  int
}

var entrySlicePoolTestMu sync.Mutex

func lockEntrySlicePoolStateForTest(t *testing.T) {
	t.Helper()
	entrySlicePoolTestMu.Lock()
	t.Cleanup(entrySlicePoolTestMu.Unlock)
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

func (b *pointerBatch) Reserve(n int) {
	b.reserveCalls++
	b.lastReserve = n
}

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
	const chunkCap = 2
	_, classCap, ok := entrySliceLeaseClassForLen(chunkCap)
	if !ok {
		t.Fatalf("entrySliceLeaseClassForLen(%d) failed", chunkCap)
	}
	entrySliceLeaseMu.Lock()
	for i := range entrySliceLeases {
		entrySliceLeases[i] = nil
	}
	entrySliceLeaseMu.Unlock()
	for i := 0; i < 4; i++ {
		putEntrySlice(make([]batch.Entry, 0, classCap))
	}

	mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("NewWithCapacityMode: %v", err)
	}
	var key [8]byte
	entryCount := classCap*2 + 3
	for i := 0; i < entryCount; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], []byte{byte(i + 1)})
	}
	binary.BigEndian.PutUint64(key[:], uint64(2))
	mt.Delete(key[:])
	mt.Freeze()

	runs, deleteOps, err := buildOpRuns(mt, chunkCap)
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
	totalOps := 0
	for i, run := range runs {
		totalOps += len(run)
		if len(run) == 0 || len(run) > classCap {
			t.Fatalf("run %d has unexpected size %d", i, len(run))
		}
	}
	if totalOps != entryCount {
		t.Fatalf("total ops=%d want=%d", totalOps, entryCount)
	}
	if len(runs) < 3 {
		t.Fatalf("expected multiple runs, got=%d", len(runs))
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
	if backendBatch.reserveCalls == 0 {
		t.Fatalf("expected reserve hint before deferred flush loop")
	}
	if backendBatch.lastReserve < len(entries) {
		t.Fatalf("reserve hint=%d want >= %d", backendBatch.lastReserve, len(entries))
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

func TestFlushDeferredValueLogUnitsInlinePointerChunkingNoDropsOrDuplicates(t *testing.T) {
	const (
		maxDeferredInlineGroupKeysForTest = 32768
		totalKeys                         = maxDeferredInlineGroupKeysForTest + 513
	)

	mt := memtable.NewAppendOnlyWithCapacity(0)
	value := []byte("v")
	for i := 0; i < totalKeys; i++ {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(i))
		mt.Set(key[:], value)
	}
	mt.Freeze()

	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		FlushThreshold:           1 << 30,
		ForceValueLogPointers:    true,
		ValueLogPointerThreshold: 1,
		FlushBuildChunkCap:       1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	backendBatch := &pointerBatch{}
	pendingOps, err := db.flushDeferredValueLogUnits([]flushUnit{{mem: mt}}, backendBatch, false, 0)
	if err != nil {
		t.Fatalf("flushDeferredValueLogUnits: %v", err)
	}
	if pendingOps != totalKeys {
		t.Fatalf("pendingOps=%d want=%d", pendingOps, totalKeys)
	}
	if len(backendBatch.entries) != totalKeys {
		t.Fatalf("backend batch entries=%d want=%d", len(backendBatch.entries), totalKeys)
	}
	if backendBatch.reserveCalls == 0 {
		t.Fatalf("expected reserve hint before deferred flush loop")
	}

	seen := make(map[uint64]struct{}, totalKeys)
	for i, e := range backendBatch.entries {
		if e.Type != batch.OpPut || !e.IsPtr {
			t.Fatalf("entry %d expected pointer put op, got type=%v isPtr=%v", i, e.Type, e.IsPtr)
		}
		if len(e.Key) != 8 {
			t.Fatalf("entry %d key len=%d want=8", i, len(e.Key))
		}
		id := binary.BigEndian.Uint64(e.Key)
		if id >= uint64(totalKeys) {
			t.Fatalf("entry %d key id=%d out of expected range [0,%d)", i, id, totalKeys)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate pointer op for key id=%d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != totalKeys {
		t.Fatalf("seen pointer keys=%d want=%d", len(seen), totalKeys)
	}
	for i := 0; i < totalKeys; i++ {
		if _, ok := seen[uint64(i)]; !ok {
			t.Fatalf("missing pointer op for key id=%d", i)
		}
	}
}

func TestFlushDeferredValueLogMemtableReservesBackendBatch(t *testing.T) {
	mt := memtable.NewAppendOnlyWithCapacity(0)
	mt.Set([]byte("a"), []byte("va"))
	mt.Set([]byte("b"), []byte("vb"))
	mt.Freeze()

	iter := mt.NewIterator(nil, nil)
	backendBatch := &pointerBatch{}
	db := &DB{valueLogThreshold: page.DefaultInlineThreshold}
	const reserveHint = 7

	if emitted, err := db.flushDeferredValueLogMemtable(iter, backendBatch, reserveHint, false, 0); err != nil {
		_ = iter.Close()
		t.Fatalf("flushDeferredValueLogMemtable: %v", err)
	} else if emitted != 2 {
		t.Fatalf("flushDeferredValueLogMemtable emitted=%d want=2", emitted)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	if backendBatch.reserveCalls == 0 {
		t.Fatalf("expected reserve hint before deferred memtable flush loop")
	}
	if backendBatch.lastReserve != reserveHint {
		t.Fatalf("reserve hint=%d want=%d", backendBatch.lastReserve, reserveHint)
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

func resetOuterLeafArenaLeasesForTest(t *testing.T) {
	t.Helper()
	outerLeafArenaLeaseMu.Lock()
	saved := outerLeafArenaLeases
	outerLeafArenaLeases = [outerLeafArenaClassCount][][]byte{}
	outerLeafArenaLeaseMu.Unlock()
	t.Cleanup(func() {
		outerLeafArenaLeaseMu.Lock()
		outerLeafArenaLeases = saved
		outerLeafArenaLeaseMu.Unlock()
	})
}

func TestOuterLeafArenaLeaseRoundTripWithOversizeReuse(t *testing.T) {
	resetOuterLeafArenaLeasesForTest(t)

	arena := getOuterLeafArena(256 << 10)
	if cap(arena) < 256<<10 {
		t.Fatalf("arena cap=%d want >= %d", cap(arena), 256<<10)
	}
	arena = arena[:1]
	arena[0] = 1
	first := &arena[0]
	capFirst := cap(arena)
	putOuterLeafArena(arena[:0])

	// Request a smaller size; policy should allow bounded oversize reuse.
	got := getOuterLeafArena(192 << 10)
	if cap(got) < 192<<10 {
		t.Fatalf("reused arena cap=%d want >= %d", cap(got), 192<<10)
	}
	got = got[:1]
	if &got[0] != first {
		t.Fatalf("expected leased outer-leaf arena reuse")
	}
	if cap(got) != capFirst {
		t.Fatalf("reused cap=%d want=%d", cap(got), capFirst)
	}
}

func TestOuterLeafArenaReuseBoundCapsOversizedArena(t *testing.T) {
	resetOuterLeafArenaLeasesForTest(t)

	big := make([]byte, 0, 8<<20)
	putOuterLeafArena(big)
	bigIdx, ok := outerLeafArenaClassForCap(cap(big))
	if !ok {
		t.Fatalf("expected class for cap=%d", cap(big))
	}
	outerLeafArenaLeaseMu.Lock()
	before := len(outerLeafArenaLeases[bigIdx])
	outerLeafArenaLeaseMu.Unlock()
	if before == 0 {
		t.Fatalf("expected big arena to be leased")
	}

	got := getOuterLeafArena(32 << 10)
	if cap(got) > 1<<20 {
		t.Fatalf("small request reused oversized arena cap=%d (>1MiB bound)", cap(got))
	}
	outerLeafArenaLeaseMu.Lock()
	after := len(outerLeafArenaLeases[bigIdx])
	outerLeafArenaLeaseMu.Unlock()
	if after != before {
		t.Fatalf("expected oversized lease bucket unchanged: before=%d after=%d", before, after)
	}
}

func resetEntrySlicePoolStateForTest(t *testing.T) {
	t.Helper()
	entrySliceLeaseMu.Lock()
	saved := entrySliceLeases
	entrySliceLeases = [entrySliceLeaseClassCount][][]batch.Entry{}
	entrySliceLeaseMu.Unlock()
	savedBytes := entrySlicePoolBytes.Load()
	savedLastGC := entrySlicePoolLastGC.Load()
	entrySlicePoolBytes.Store(0)
	entrySlicePoolLastGC.Store(0)
	for i := range entrySlicePools {
		entrySlicePools[i] = sync.Pool{}
	}
	t.Cleanup(func() {
		entrySliceLeaseMu.Lock()
		entrySliceLeases = saved
		entrySliceLeaseMu.Unlock()
		entrySlicePoolBytes.Store(savedBytes)
		entrySlicePoolLastGC.Store(savedLastGC)
		for i := range entrySlicePools {
			entrySlicePools[i] = sync.Pool{}
		}
	})
}

func resetEntrySlicePoolsForTest(t *testing.T) {
	t.Helper()
	resetEntrySlicePoolStateForTest(t)
	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBudgetBytes = 0
	t.Cleanup(func() {
		entrySlicePoolBudgetBytes = savedBudget
	})
}

func TestEntrySliceLeaseRoundTrip(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)

	entries := make([]batch.Entry, 1, 256)
	entries[0] = batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}
	first := &entries[0]
	leaseIdx, ok := entrySliceLeaseClassForCap(cap(entries))
	if !ok {
		t.Fatalf("expected lease class for cap=%d", cap(entries))
	}
	putEntrySlice(entries)

	entrySliceLeaseMu.Lock()
	if len(entrySliceLeases[leaseIdx]) != 1 {
		entrySliceLeaseMu.Unlock()
		t.Fatalf("expected one leased entry slice in bucket %d, got %d", leaseIdx, len(entrySliceLeases[leaseIdx]))
	}
	entrySliceLeaseMu.Unlock()

	got := getEntrySlice(1)
	got = append(got, batch.Entry{})
	if &got[0] != first {
		t.Fatalf("expected leased entry-slice reuse")
	}
}

func TestEntrySliceLeaseRoundTripWithOversizeReuse(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)

	entries := make([]batch.Entry, 1, 512)
	entries[0] = batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}
	first := &entries[0]
	putEntrySlice(entries)

	got := getEntrySlice(320)
	got = append(got, batch.Entry{})
	if &got[0] != first {
		t.Fatalf("expected bounded oversize lease reuse")
	}
}

func TestEntrySliceLeaseBoundCapsOversizedReuse(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)

	big := make([]batch.Entry, 0, 1<<18)
	putEntrySlice(big)
	bigIdx, ok := entrySliceLeaseClassForCap(cap(big))
	if !ok {
		t.Fatalf("expected lease class for cap=%d", cap(big))
	}
	entrySliceLeaseMu.Lock()
	before := len(entrySliceLeases[bigIdx])
	entrySliceLeaseMu.Unlock()
	if before == 0 {
		t.Fatalf("expected oversized entry-slice lease to be retained")
	}

	got := getEntrySlice(32)
	if cap(got) > 1<<12 {
		t.Fatalf("small request reused oversized lease cap=%d (>4Ki entries bound)", cap(got))
	}

	entrySliceLeaseMu.Lock()
	after := len(entrySliceLeases[bigIdx])
	entrySliceLeaseMu.Unlock()
	if after != before {
		t.Fatalf("expected oversized lease bucket unchanged: before=%d after=%d", before, after)
	}
}

func TestPutEntrySliceClearsEntriesOnEarlyReturn(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)

	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBudgetBytes = 0
	t.Cleanup(func() { entrySlicePoolBudgetBytes = savedBudget })

	entries := make([]batch.Entry, 1, 64)
	entries[0] = batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}
	putEntrySlice(entries)

	if entries[0].Key != nil || entries[0].Value != nil {
		t.Fatalf("expected putEntrySlice to clear backing entries on budget early return; got key=%v value=%v", entries[0].Key, entries[0].Value)
	}
}

func TestPutEntrySliceClearsEntriesOnUnclassifiedEarlyReturn(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)

	entries := make([]batch.Entry, 1, 96)
	entries[0] = batch.Entry{Type: batch.OpPut, Key: []byte("k"), Value: []byte("v")}
	putEntrySlice(entries)

	if entries[0].Key != nil || entries[0].Value != nil {
		t.Fatalf("expected putEntrySlice to clear backing entries on unclassed early return; got key=%v value=%v", entries[0].Key, entries[0].Value)
	}
}

func TestPutEntrySliceBudgetCapsRetention(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	entrySlicePoolBudgetBytes = 64 * entrySliceEntrySizeBytes
	entrySlicePoolBytes.Store(0)

	leaseIdx, ok := entrySliceLeaseClassForCap(64)
	if !ok {
		t.Fatalf("expected lease class for cap=%d", 64)
	}

	putEntrySlice(make([]batch.Entry, 0, 64))
	putEntrySlice(make([]batch.Entry, 0, 64))

	if got := entrySlicePoolBytes.Load(); got < 0 || got > entrySlicePoolBudgetBytes {
		t.Fatalf("entrySlicePoolBytes=%d want within [0,%d]", got, entrySlicePoolBudgetBytes)
	}

	entrySliceLeaseMu.Lock()
	leased := len(entrySliceLeases[leaseIdx])
	entrySliceLeaseMu.Unlock()
	if leased > 1 {
		t.Fatalf("expected budget to cap retained entry slices; leased=%d", leased)
	}
}

func TestGetEntrySliceMissResetsPoolBytesAfterGC(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)

	origNumGC := entrySlicePoolNumGC
	defer func() { entrySlicePoolNumGC = origNumGC }()

	var fakeNumGC uint64 = 9
	entrySlicePoolNumGC = func() uint64 { return fakeNumGC }

	entrySlicePoolBytes.Store(1234)
	entrySlicePoolLastGC.Store(fakeNumGC)
	fakeNumGC++

	got := getEntrySlice(64)
	if cap(got) < 64 {
		t.Fatalf("entry slice cap=%d want >= 64", cap(got))
	}
	if gotBytes := entrySlicePoolBytes.Load(); gotBytes != 0 {
		t.Fatalf("entrySlicePoolBytes after GC miss=%d want 0", gotBytes)
	}
	if gotGC := entrySlicePoolLastGC.Load(); gotGC != fakeNumGC {
		t.Fatalf("entrySlicePoolLastGC=%d want %d", gotGC, fakeNumGC)
	}
}

func TestPutEntrySliceDoesNotAdvanceGCEpochWhenPoolAlreadyAccounted(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)

	origNumGC := entrySlicePoolNumGC
	defer func() { entrySlicePoolNumGC = origNumGC }()

	var fakeNumGC uint64 = 11
	entrySlicePoolNumGC = func() uint64 { return fakeNumGC }

	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBudgetBytes = 64 * entrySliceEntrySizeBytes
	t.Cleanup(func() { entrySlicePoolBudgetBytes = savedBudget })

	entrySlicePoolBytes.Store(1234)
	entrySlicePoolLastGC.Store(fakeNumGC - 1)

	putEntrySlice(make([]batch.Entry, 0, 64))

	if gotGC := entrySlicePoolLastGC.Load(); gotGC != fakeNumGC-1 {
		t.Fatalf("entrySlicePoolLastGC=%d want %d", gotGC, fakeNumGC-1)
	}
}

func TestMaybeResetEntrySlicePoolBytesAfterGC_FirstEpochKeepsAccountedBytes(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)

	origNumGC := entrySlicePoolNumGC
	defer func() { entrySlicePoolNumGC = origNumGC }()

	const fakeNumGC uint64 = 5
	entrySlicePoolNumGC = func() uint64 { return fakeNumGC }

	entrySlicePoolBytes.Store(1234)
	entrySlicePoolLastGC.Store(0)

	maybeResetEntrySlicePoolBytesAfterGC()

	if got := entrySlicePoolBytes.Load(); got != 1234 {
		t.Fatalf("entrySlicePoolBytes=%d want 1234", got)
	}
	if got := entrySlicePoolLastGC.Load(); got != fakeNumGC {
		t.Fatalf("entrySlicePoolLastGC=%d want %d", got, fakeNumGC)
	}
}

func TestMaybeResetEntrySlicePoolBytesAfterGC_PreservesLeaseBytes(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)
	poolPressureTestMu.Lock()
	t.Cleanup(poolPressureTestMu.Unlock)

	resetPoolPressureStateForTest()
	savedNow := poolPressureNow
	savedReadMemStats := poolPressureReadMemStats
	savedMemLimit := poolPressureMemoryLimit
	t.Cleanup(func() {
		poolPressureNow = savedNow
		poolPressureReadMemStats = savedReadMemStats
		poolPressureMemoryLimit = savedMemLimit
		resetPoolPressureStateForTest()
	})

	now := time.Unix(1, 0)
	poolPressureNow = func() time.Time { return now }
	poolPressureReadMemStats = func(ms *runtime.MemStats) { *ms = runtime.MemStats{} }
	poolPressureMemoryLimit = func() int64 { return -1 }

	origNumGC := entrySlicePoolNumGC
	defer func() { entrySlicePoolNumGC = origNumGC }()

	var fakeNumGC uint64 = 17
	entrySlicePoolNumGC = func() uint64 { return fakeNumGC }

	savedBudget := entrySlicePoolBudgetBytes
	entrySlicePoolBudgetBytes = 64 * entrySliceEntrySizeBytes
	t.Cleanup(func() { entrySlicePoolBudgetBytes = savedBudget })

	putEntrySlice(make([]batch.Entry, 0, 64))
	expected := int64(64) * entrySliceEntrySizeBytes
	if got := entrySlicePoolBytes.Load(); got != expected {
		t.Fatalf("entrySlicePoolBytes=%d want %d before GC", got, expected)
	}

	entrySlicePoolLastGC.Store(fakeNumGC)
	entrySlicePoolBytes.Store(expected + 1234)
	fakeNumGC++

	maybeResetEntrySlicePoolBytesAfterGC()

	if got := entrySlicePoolBytes.Load(); got != expected {
		t.Fatalf("entrySlicePoolBytes=%d want %d after GC", got, expected)
	}
	if got := entrySlicePoolLastGC.Load(); got != fakeNumGC {
		t.Fatalf("entrySlicePoolLastGC=%d want %d", got, fakeNumGC)
	}
}

func TestGetEntrySliceIgnoresUnexpectedPoolType(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolsForTest(t)
	capacity := 64
	idx, _, ok := entrySliceLeaseClassForLen(capacity)
	if !ok {
		t.Fatalf("entrySliceLeaseClassForLen(%d) failed", capacity)
	}
	entrySlicePools[idx].Put("invalid-entry-slice")

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("getEntrySlice panicked on unexpected pool type: %v", r)
		}
	}()
	got := getEntrySlice(capacity)
	if cap(got) < capacity {
		t.Fatalf("entry slice cap=%d want >= %d", cap(got), capacity)
	}
}

func TestOuterLeafArenaMaxReuseCapClampOverflow(t *testing.T) {
	huge := int(^uint(0) >> 1)
	if got := outerLeafArenaMaxReuseCap(huge); got != maxOuterLeafArenaPoolCap {
		t.Fatalf("outerLeafArenaMaxReuseCap(huge)=%d want=%d", got, maxOuterLeafArenaPoolCap)
	}
}

func TestEntrySliceMaxReuseCapClampOverflow(t *testing.T) {
	huge := int(^uint(0) >> 1)
	if got := entrySliceMaxReuseCap(huge); got != maxEntryPoolCap {
		t.Fatalf("entrySliceMaxReuseCap(huge)=%d want=%d", got, maxEntryPoolCap)
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
