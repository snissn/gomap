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
	batchArenaPoolDropBytesTotal.Store(0)
	batchArenaPoolDropHardCapBytesTotal.Store(0)
	batchArenaBorrowBlockedTotal.Store(0)
	batchArenaBorrowPreflightBlockedTotal.Store(0)
	batchArenaBorrowPreflightBlockedBytesTotal.Store(0)
	batchArenaFreeLeaseMu.Lock()
	batchArenaFreeLeases = [batchArenaClassCount][][]byte{}
	batchArenaFreeLeaseMu.Unlock()
	for i := range batchArenaPools {
		batchArenaPools[i] = sync.Pool{}
	}
}

func assertBatchEntriesDoNotAliasArenaTail(t *testing.T, entries []batch.Entry, tail []byte) {
	t.Helper()
	for i := range entries {
		if sliceAliasesArenaTail(entries[i].Key, tail) {
			t.Fatalf("entry %d key still aliases released arena tail", i)
		}
		if sliceAliasesArenaTail(entries[i].Value, tail) {
			t.Fatalf("entry %d value still aliases released arena tail", i)
		}
	}
}

func assertBatchEntriesAliasPackedRange(t *testing.T, entries []batch.Entry, base, limit uintptr) {
	t.Helper()
	for i := range entries {
		if len(entries[i].Key) > 0 {
			ptr := uintptr(unsafe.Pointer(unsafe.SliceData(entries[i].Key)))
			if ptr < base || ptr+uintptr(len(entries[i].Key)) > limit {
				t.Fatalf("entry %d key is outside packed range", i)
			}
		}
		if len(entries[i].Value) > 0 {
			ptr := uintptr(unsafe.Pointer(unsafe.SliceData(entries[i].Value)))
			if ptr < base || ptr+uintptr(len(entries[i].Value)) > limit {
				t.Fatalf("entry %d value is outside packed range", i)
			}
		}
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

func TestBatchArenaFreeLeaseRoundTripAccounting(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}
	buf := make([]byte, 1, classCap)
	wantPtr := unsafe.SliceData(buf)

	putBatchArena(buf)
	if got := batchArenaPoolBytes.Load(); got != int64(classCap) {
		t.Fatalf("batchArenaPoolBytes after put=%d want %d", got, classCap)
	}

	got := getBatchArena(classCap)
	if cap(got) != classCap {
		t.Fatalf("getBatchArena cap=%d want %d", cap(got), classCap)
	}
	got = append(got, 0)
	if gotPtr := unsafe.SliceData(got); gotPtr != wantPtr {
		t.Fatalf("getBatchArena did not reuse free lease backing")
	}
	if gotBytes := batchArenaPoolBytes.Load(); gotBytes != 0 {
		t.Fatalf("batchArenaPoolBytes after get=%d want 0", gotBytes)
	}
}

func TestBatchArenaFreeLeaseDropsAfterBucketCap(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	for i := 0; i < maxBatchArenaFreeLeasesPerBucket+1; i++ {
		putBatchArena(make([]byte, 0, classCap))
	}
	if got, want := batchArenaPoolBytes.Load(), int64(maxBatchArenaFreeLeasesPerBucket*classCap); got != want {
		t.Fatalf("batchArenaPoolBytes after capped puts=%d want %d", got, want)
	}
	if got := batchArenaPoolDropBytesTotal.Load(); got < uint64(classCap) {
		t.Fatalf("batchArenaPoolDropBytesTotal=%d want >= %d", got, classCap)
	}
}

func TestBatchArenaLeaseStaticHeaderRoundTrip(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()

	chunk := make([]byte, 0, 1<<batchArenaMinShift)
	chunks := [][]byte{chunk}
	lease := getBatchArenaLease(1, chunks)
	if lease == nil {
		t.Fatal("getBatchArenaLease returned nil")
	}
	if !lease.staticPool {
		t.Fatal("getBatchArenaLease did not use static lease header")
	}
	if got, want := lease.bytes, int64(cap(chunk)); got != want {
		t.Fatalf("lease bytes=%d want %d", got, want)
	}
	putBatchArenaLease(lease)

	got := getBatchArenaLease(1, chunks)
	if got != lease {
		t.Fatal("getBatchArenaLease did not reuse static lease header")
	}
	putBatchArenaLease(got)
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
	// sync.Pool visibility is intentionally lossy across runtimes (especially
	// under -race): a drain pass may not be able to observe every retained
	// buffer even when accounting still reflects them. Require monotonic
	// progress by at least one class-sized chunk rather than a strict target hit.
	maxAllowedAfter := before - int64(classCap)
	if after > maxAllowedAfter {
		t.Fatalf("batchArenaPoolBytes after drain=%d want <= %d (before=%d target=%d classCap=%d)", after, maxAllowedAfter, before, target, classCap)
	}
}

func BenchmarkBatchArenaFreeLeaseRoundTrip(b *testing.B) {
	batchArenaPoolTestMu.Lock()
	resetBatchArenaPoolsForTest()
	b.Cleanup(batchArenaPoolTestMu.Unlock)

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		b.Fatal("batchArenaClassForLen failed")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := getBatchArena(classCap)
		if cap(buf) != classCap {
			b.Fatalf("getBatchArena cap=%d want %d", cap(buf), classCap)
		}
		putBatchArena(buf)
	}
}

func BenchmarkBatchArenaLeaseHeaderRoundTrip(b *testing.B) {
	batchArenaPoolTestMu.Lock()
	b.Cleanup(batchArenaPoolTestMu.Unlock)

	chunk := make([]byte, 0, 1<<batchArenaMinShift)
	chunks := [][]byte{chunk}
	lease := getBatchArenaLease(1, chunks)
	putBatchArenaLease(lease)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease := getBatchArenaLease(1, chunks)
		if lease == nil {
			b.Fatal("getBatchArenaLease returned nil")
		}
		putBatchArenaLease(lease)
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

func TestBatchArenaSingleMemtableRetainCoalescesLeaseHeaders(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	db := &DB{}
	mt := memtable.NewBTree()
	chunkA := make([]byte, 0, 1<<batchArenaMinShift)
	chunkB := make([]byte, 0, 1<<batchArenaMinShift)

	db.retainBatchArenaChunksForMemtables([][]byte{chunkA}, []memtable.Table{mt})
	db.retainBatchArenaChunksForMemtables([][]byte{chunkB}, []memtable.Table{mt})

	wantBytes := int64(cap(chunkA) + cap(chunkB))
	if got := db.batchArenaLeaseBytes.Load(); got != wantBytes {
		t.Fatalf("leased bytes after coalesced retain=%d want=%d", got, wantBytes)
	}
	db.batchArenaLeaseMu.Lock()
	leases := append([]*batchArenaLease(nil), db.batchArenaLeasesByMem[mt]...)
	db.batchArenaLeaseMu.Unlock()
	if got := len(leases); got != 1 {
		t.Fatalf("lease headers for one memtable=%d want 1", got)
	}
	if got := len(leases[0].chunks); got != 2 {
		t.Fatalf("coalesced lease chunks=%d want 2", got)
	}
	if got := leases[0].bytes; got != wantBytes {
		t.Fatalf("coalesced lease bytes=%d want=%d", got, wantBytes)
	}

	db.releaseBatchArenaLeasesForMemtable(mt)
	if got := db.batchArenaLeaseBytes.Load(); got != 0 {
		t.Fatalf("leased bytes after release=%d want=0", got)
	}
	if got := batchArenaLeasedBytesGlobal.Load(); got != 0 {
		t.Fatalf("global leased bytes after release=%d want=0", got)
	}
}

func TestBatchArenaTailCompaction_PacksAliasedSlicesAndDropsTailChunk(t *testing.T) {
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
	if newValPtr != newKeyPtr+uintptr(len(key)) {
		t.Fatalf("key/value slices were not packed contiguously")
	}
	assertBatchEntriesDoNotAliasArenaTail(t, b.entries, tail)
	if b.entries[0].ValuePtr != oldPtr {
		t.Fatalf("value pointer metadata changed: got=%+v want=%+v", b.entries[0].ValuePtr, oldPtr)
	}
}

func TestBatchArenaTailCompaction_PacksAliasedTailWithoutDeferredViews(t *testing.T) {
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
	assertBatchEntriesDoNotAliasArenaTail(t, b.entries, tail)
	if got := db.batchArenaTailCompactRuns.Load(); got != 1 {
		t.Fatalf("tail_compact_runs_total=%d want 1", got)
	}
}

func TestBatchArenaTailCompaction_PacksManyAliasesIntoOneBacking(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const (
		entryCount = 2048
		keyLen     = 16
		valueLen   = 16
		used       = 128 << 10
		classCap   = 512 << 10
	)
	tail := make([]byte, used, classCap)
	entries := make([]batch.Entry, entryCount)
	offset := 0
	for i := range entries {
		key := tail[offset : offset+keyLen : offset+keyLen]
		offset += keyLen
		value := tail[offset : offset+valueLen : offset+valueLen]
		offset += valueLen
		entries[i] = batch.Entry{Type: batch.OpPut, Key: key, Value: value}
	}
	aliasedBytes, aliasedSlices := batchMainArenaTailAliasStats(entries, tail)
	if aliasedBytes == 0 || aliasedSlices != entryCount*2 {
		t.Fatalf("alias stats bytes=%d slices=%d want slices=%d", aliasedBytes, aliasedSlices, entryCount*2)
	}
	if !shouldCompactBatchArenaTailWithAliases(len(tail), cap(tail), aliasedBytes, aliasedSlices) {
		t.Fatalf("aliased tail unexpectedly failed compaction policy")
	}

	db := &DB{}
	b := &Batch{
		db:              db,
		entries:         entries,
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
	assertBatchEntriesDoNotAliasArenaTail(t, b.entries, tail)
	packedBase := uintptr(unsafe.Pointer(unsafe.SliceData(b.entries[0].Key)))
	assertBatchEntriesAliasPackedRange(t, b.entries, packedBase, packedBase+uintptr(aliasedBytes))
	if got := db.batchArenaTailCompactRuns.Load(); got != 1 {
		t.Fatalf("tail_compact_runs_total=%d want 1", got)
	}
	if got := db.batchArenaTailCompactCopied.Load(); got != uint64(aliasedBytes) {
		t.Fatalf("tail_compact_copied_bytes_total=%d want %d", got, aliasedBytes)
	}
	if got := db.batchArenaTailCompactSaved.Load(); got == 0 {
		t.Fatalf("tail_compact_saved_bytes_total=%d want >0", got)
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
	assertBatchEntriesDoNotAliasArenaTail(t, b.entries, tail)
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
