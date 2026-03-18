package caching

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/page"
)

var batchArenaPoolTestMu sync.Mutex

func resetBatchArenaPoolsForTest() {
	batchArenaPoolBytes.Store(0)
	batchArenaPoolLastGC.Store(0)
	batchArenaLeasedBytesGlobal.Store(0)
	batchArenaRetainedHardCapOverride.Store(0)
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	batchArenaPoolDropHardCapBytesTotal.Store(0)
	batchArenaBorrowBlockedTotal.Store(0)
	batchArenaBorrowPreflightBlockedTotal.Store(0)
	batchArenaBorrowPreflightBlockedBytesTotal.Store(0)
	for i := range batchArenaPools {
		batchArenaPools[i] = sync.Pool{}
	}
}

func TestBatchArenaPoolAccountingRecoversOnUnexpectedCap(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	idx, ok := batchArenaClassForCap(classCap)
	if !ok {
		t.Fatal("batchArenaClassForCap failed")
	}
	wrong := make([]byte, 0, classCap*2)
	batchArenaPools[idx].Put(wrong)
	batchArenaPoolBytes.Store(int64(cap(wrong)))

	buf := getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena with wrong-cap pooled buffer returned cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes after wrong-cap recovery=%d want 0", got)
	}
}

func TestBatchArenaPoolAccountingMissWithoutGCDoesNotReset(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	origNumGC := batchArenaPoolNumGC
	defer func() { batchArenaPoolNumGC = origNumGC }()

	const fakeNumGC uint64 = 7
	batchArenaPoolNumGC = func() uint64 { return fakeNumGC }

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	batchArenaPoolBytes.Store(1234)
	batchArenaPoolLastGC.Store(fakeNumGC)
	buf := getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 1234 {
		t.Fatalf("batchArenaPoolBytes after non-GC miss=%d want 1234", got)
	}
}

func TestBatchArenaPoolAccountingMissResetsAfterGC(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	origNumGC := batchArenaPoolNumGC
	defer func() { batchArenaPoolNumGC = origNumGC }()

	var fakeNumGC uint64 = 7
	batchArenaPoolNumGC = func() uint64 { return fakeNumGC }

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	batchArenaPoolBytes.Store(1234)
	batchArenaPoolLastGC.Store(fakeNumGC)
	fakeNumGC++
	buf := getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes after GC miss=%d want 0", got)
	}
	if got := batchArenaPoolLastGC.Load(); got != fakeNumGC {
		t.Fatalf("batchArenaPoolLastGC=%d want %d", got, fakeNumGC)
	}
}

func TestPutBatchArenaBudgetResetsAfterGC(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	origNumGC := batchArenaPoolNumGC
	defer func() { batchArenaPoolNumGC = origNumGC }()

	var fakeNumGC uint64 = 11
	batchArenaPoolNumGC = func() uint64 { return fakeNumGC }

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}
	budget := currentBatchArenaPoolBudgetBytes()
	if budget <= int64(classCap) {
		t.Fatalf("budget=%d want > classCap=%d", budget, classCap)
	}

	// Simulate sync.Pool being cleared by GC while the byte counter stayed high.
	batchArenaPoolBytes.Store(budget)
	batchArenaPoolLastGC.Store(fakeNumGC)
	fakeNumGC++

	putBatchArena(make([]byte, 0, classCap))

	if got := batchArenaPoolBytes.Load(); got != int64(classCap) {
		t.Fatalf("batchArenaPoolBytes after GC-aware put=%d want %d", got, classCap)
	}
	if got := batchArenaPoolLastGC.Load(); got != fakeNumGC {
		t.Fatalf("batchArenaPoolLastGC=%d want %d", got, fakeNumGC)
	}
}

func TestBatchArenaPoolBudgetDoesNotOvercount(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}
	budget := currentBatchArenaPoolBudgetBytes()
	if budget <= 0 {
		t.Fatalf("currentBatchArenaPoolBudgetBytes=%d want > 0", budget)
	}

	loops := int(budget/int64(classCap)) + 8
	for i := 0; i < loops; i++ {
		putBatchArena(make([]byte, 0, classCap))
	}
	if got := batchArenaPoolBytes.Load(); got < 0 || got > budget {
		t.Fatalf("batchArenaPoolBytes=%d want in [0,%d]", got, budget)
	}
}

func TestDrainBatchArenaPoolToTargetBytes(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}
	for i := 0; i < 12; i++ {
		putBatchArena(make([]byte, 0, classCap))
	}
	before := batchArenaPoolBytes.Load()
	if before <= 0 {
		t.Fatalf("batchArenaPoolBytes before drain=%d want > 0", before)
	}
	target := before / 3
	dropped := drainBatchArenaPoolToTargetBytes(target)
	after := batchArenaPoolBytes.Load()
	if dropped <= 0 {
		t.Fatalf("drain dropped=%d want > 0", dropped)
	}
	// Draining is class-granular; allow one class-cap overshoot over target.
	if after > target+int64(classCap) {
		t.Fatalf("batchArenaPoolBytes after drain=%d want <= %d (target %d + classCap %d)", after, target+int64(classCap), target, classCap)
	}
}

func TestBatchArenaPoolBudgetCacheRecomputesOnProcsMismatch(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	currentProcs := runtime.GOMAXPROCS(0)
	if currentProcs < 1 {
		currentProcs = 1
	}
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{
		procs:  int32(currentProcs + 1),
		budget: 1,
	})
	got := currentBatchArenaPoolBudgetBytes()
	want := computeBatchArenaPoolBudgetBytesForProcs(currentProcs)
	if got != want {
		t.Fatalf("currentBatchArenaPoolBudgetBytes=%d want %d", got, want)
	}
	cached, _ := batchArenaPoolBudgetState.Load().(batchArenaPoolBudgetCache)
	if cached.procs != int32(currentProcs) || cached.budget != want {
		t.Fatalf("cached budget=%+v want procs=%d budget=%d", cached, currentProcs, want)
	}
}

func TestComputeBatchArenaPoolBudgetBytesForProcs_Saturates(t *testing.T) {
	const maxBudgetBytes = int64(128 << 20)
	if got := computeBatchArenaPoolBudgetBytesForProcs(int(^uint(0) >> 1)); got != maxBudgetBytes {
		t.Fatalf("computeBatchArenaPoolBudgetBytesForProcs(maxint)=%d want %d", got, maxBudgetBytes)
	}
}

func TestCurrentBatchArenaRetainedHardCap_UsesOverride(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const override = int64(123456)
	batchArenaRetainedHardCapOverride.Store(override)
	if got := currentBatchArenaRetainedHardCapBytes(); got != override {
		t.Fatalf("currentBatchArenaRetainedHardCapBytes=%d want %d", got, override)
	}
}

func TestShouldBorrowBatchArenaBytesForWrite_PreflightBlock(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const hardCap = int64(64 << 10)
	batchArenaRetainedHardCapOverride.Store(hardCap)
	batchArenaPoolBytes.Store(16 << 10)
	batchArenaLeasedBytesGlobal.Store(16 << 10)

	if allow, preflight := shouldBorrowBatchArenaBytesForWrite(8 << 10); !allow || preflight {
		t.Fatalf("small prospective retain unexpectedly blocked: allow=%t preflight=%t", allow, preflight)
	}
	if allow, preflight := shouldBorrowBatchArenaBytesForWrite(40 << 10); allow || !preflight {
		t.Fatalf("large prospective retain should preflight-block: allow=%t preflight=%t", allow, preflight)
	}
}

func TestShouldBorrowBatchArenaBytesForWriteWithHardCap_PreflightBlock(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const hardCap = int64(64 << 10)
	batchArenaPoolBytes.Store(16 << 10)
	batchArenaLeasedBytesGlobal.Store(16 << 10)

	if allow, preflight := shouldBorrowBatchArenaBytesForWriteWithHardCap(8<<10, hardCap); !allow || preflight {
		t.Fatalf("small prospective retain unexpectedly blocked: allow=%t preflight=%t", allow, preflight)
	}
	if allow, preflight := shouldBorrowBatchArenaBytesForWriteWithHardCap(40<<10, hardCap); allow || !preflight {
		t.Fatalf("large prospective retain should preflight-block: allow=%t preflight=%t", allow, preflight)
	}
}

func TestBatchArenaRetainedHardCapEffectiveBytes_ReducesUnderDeferredPressure(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	db := &DB{}
	const hardCap = int64(64 << 20)
	batchArenaRetainedHardCapOverride.Store(hardCap)

	if got := db.currentBatchArenaRetainedHardCapEffectiveBytes(); got != hardCap {
		t.Fatalf("effective hard cap without deferred pressure=%d want %d", got, hardCap)
	}

	db.memtableViewTelemetry.deferredBytesCurrent.Store(batchArenaDeferredPressureThresholdBytes)
	want := hardCap / batchArenaDeferredPressureHardCapDivisor
	if minCap := int64(batchCopyArenaMinChunk); want < minCap {
		want = minCap
	}
	if got := db.currentBatchArenaRetainedHardCapEffectiveBytes(); got != want {
		t.Fatalf("effective hard cap with deferred pressure=%d want %d", got, want)
	}
}

func TestPutBatchArena_DropsWhenRetainedHardCapExceeded(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	hardCap := int64(classCap * 2)
	batchArenaRetainedHardCapOverride.Store(hardCap)
	batchArenaLeasedBytesGlobal.Store(hardCap)
	batchArenaPoolBytes.Store(0)
	batchArenaPoolDropBytesTotal.Store(0)
	batchArenaPoolDropHardCapBytesTotal.Store(0)

	putBatchArena(make([]byte, 0, classCap))

	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes=%d want 0", got)
	}
	if got := batchArenaPoolDropBytesTotal.Load(); got != uint64(classCap) {
		t.Fatalf("pool_drop_bytes_total=%d want %d", got, classCap)
	}
	if got := batchArenaPoolDropHardCapBytesTotal.Load(); got != uint64(classCap) {
		t.Fatalf("pool_drop_hard_cap_bytes_total=%d want %d", got, classCap)
	}
}

func TestBatchArenaLeaseBytesTracksRetainReleaseLifecycle(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	db := &DB{}
	mt1 := memtable.NewBTree()
	mt2 := memtable.NewBTree()
	chunk := make([]byte, 0, 1<<batchArenaMinShift)

	db.retainBatchArenaChunksForMemtables([][]byte{chunk}, []memtable.Table{mt1, mt2})

	want := int64(cap(chunk))
	if got := db.batchArenaLeaseBytes.Load(); got != want {
		t.Fatalf("leased bytes after retain=%d want=%d", got, want)
	}
	if got := batchArenaLeasedBytesGlobal.Load(); got != want {
		t.Fatalf("global leased bytes after retain=%d want=%d", got, want)
	}
	if got := db.batchArenaLeaseBytesMax.Load(); got != want {
		t.Fatalf("leased bytes max after retain=%d want=%d", got, want)
	}

	db.releaseBatchArenaLeasesForMemtable(mt1)
	if got := db.batchArenaLeaseBytes.Load(); got != want {
		t.Fatalf("leased bytes after first release=%d want=%d", got, want)
	}

	db.releaseBatchArenaLeasesForMemtable(mt2)
	if got := db.batchArenaLeaseBytes.Load(); got != 0 {
		t.Fatalf("leased bytes after final release=%d want=0", got)
	}
	if got := batchArenaLeasedBytesGlobal.Load(); got != 0 {
		t.Fatalf("global leased bytes after final release=%d want=0", got)
	}
	if got := db.batchArenaLeaseBytesMax.Load(); got != want {
		t.Fatalf("leased bytes max after final release=%d want=%d", got, want)
	}
}

func TestBatchArenaTailCompaction_ClonesAliasedSlicesAndDropsTailChunk(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	tail := make([]byte, 64, 512<<10)
	copy(tail[0:6], []byte("key-01"))
	copy(tail[6:16], []byte("value-0001"))
	key := tail[0:6:6]
	value := tail[6:16:16]

	db := &DB{}
	b := &Batch{
		db:              db,
		entries:         []batch.Entry{{Type: batch.OpPut, Key: key, Value: value, IsPtr: true, ValuePtr: page.ValuePtr{Offset: 123, Length: 10}}},
		copyArena:       tail,
		copyArenaChunks: [][]byte{tail[:0]},
	}
	oldKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	oldValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))
	oldPtr := b.entries[0].ValuePtr

	b.compactUnderfilledMainArenaTail()

	if b.copyArena != nil {
		t.Fatalf("copyArena should be nil after compact")
	}
	if len(b.copyArenaChunks) != 0 {
		t.Fatalf("copyArenaChunks len=%d want 0", len(b.copyArenaChunks))
	}
	if got := db.batchArenaTailCompactRuns.Load(); got != 1 {
		t.Fatalf("tail_compact_runs_total=%d want 1", got)
	}
	wantCopied := uint64(len(key) + len(value))
	if got := db.batchArenaTailCompactCopied.Load(); got != wantCopied {
		t.Fatalf("tail_compact_copied_bytes_total=%d want %d", got, wantCopied)
	}
	if got := db.batchArenaTailCompactSaved.Load(); got == 0 {
		t.Fatalf("tail_compact_saved_bytes_total=%d want >0", got)
	}
	if !bytes.Equal(b.entries[0].Key, []byte("key-01")) {
		t.Fatalf("key mismatch after compact: %q", string(b.entries[0].Key))
	}
	if !bytes.Equal(b.entries[0].Value, []byte("value-0001")) {
		t.Fatalf("value mismatch after compact: %q", string(b.entries[0].Value))
	}
	newKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	newValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))
	if newKeyPtr == oldKeyPtr {
		t.Fatalf("key pointer did not change")
	}
	if newValPtr == oldValPtr {
		t.Fatalf("value pointer did not change")
	}
	if b.entries[0].ValuePtr != oldPtr {
		t.Fatalf("value pointer metadata changed: got=%+v want=%+v", b.entries[0].ValuePtr, oldPtr)
	}
}

func TestBatchArenaTailCompaction_SkipsNearFullTail(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	tail := make([]byte, 320<<10, 512<<10)
	copy(tail[0:6], []byte("key-01"))
	copy(tail[6:16], []byte("value-0001"))
	key := tail[0:6:6]
	value := tail[6:16:16]

	db := &DB{}
	b := &Batch{
		db:              db,
		entries:         []batch.Entry{{Type: batch.OpPut, Key: key, Value: value}},
		copyArena:       tail,
		copyArenaChunks: [][]byte{tail[:0]},
	}
	oldKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	oldValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))

	b.compactUnderfilledMainArenaTail()

	if b.copyArena == nil {
		t.Fatalf("copyArena unexpectedly nil")
	}
	if len(b.copyArenaChunks) != 1 {
		t.Fatalf("copyArenaChunks len=%d want 1", len(b.copyArenaChunks))
	}
	newKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	newValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))
	if newKeyPtr != oldKeyPtr {
		t.Fatalf("key pointer changed unexpectedly")
	}
	if newValPtr != oldValPtr {
		t.Fatalf("value pointer changed unexpectedly")
	}
	if got := db.batchArenaTailCompactRuns.Load(); got != 0 {
		t.Fatalf("tail_compact_runs_total=%d want 0", got)
	}
	if got := db.batchArenaTailCompactCopied.Load(); got != 0 {
		t.Fatalf("tail_compact_copied_bytes_total=%d want 0", got)
	}
	if got := db.batchArenaTailCompactSaved.Load(); got != 0 {
		t.Fatalf("tail_compact_saved_bytes_total=%d want 0", got)
	}
}

func TestBatchArenaTailCompaction_CompactsNearFullTailWhenDeferredViewsPinned(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	tail := make([]byte, 320<<10, 512<<10)
	copy(tail[0:6], []byte("key-01"))
	copy(tail[6:16], []byte("value-0001"))
	key := tail[0:6:6]
	value := tail[6:16:16]

	db := &DB{}
	db.memtableViewTelemetry.deferredViewsCurrent.Store(1)
	b := &Batch{
		db:              db,
		entries:         []batch.Entry{{Type: batch.OpPut, Key: key, Value: value}},
		copyArena:       tail,
		copyArenaChunks: [][]byte{tail[:0]},
	}
	oldKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	oldValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))

	b.compactUnderfilledMainArenaTail()

	if b.copyArena != nil {
		t.Fatalf("copyArena should be nil after compact")
	}
	if len(b.copyArenaChunks) != 0 {
		t.Fatalf("copyArenaChunks len=%d want 0", len(b.copyArenaChunks))
	}
	newKeyPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	newValPtr := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Value)))
	if newKeyPtr == oldKeyPtr {
		t.Fatalf("key pointer did not change")
	}
	if newValPtr == oldValPtr {
		t.Fatalf("value pointer did not change")
	}
	if got := db.batchArenaTailCompactRuns.Load(); got != 1 {
		t.Fatalf("tail_compact_runs_total=%d want 1", got)
	}
	if got := db.batchArenaTailCompactCopied.Load(); got == 0 {
		t.Fatalf("tail_compact_copied_bytes_total=%d want >0", got)
	}
	if got := db.batchArenaTailCompactSaved.Load(); got == 0 {
		t.Fatalf("tail_compact_saved_bytes_total=%d want >0", got)
	}
}
