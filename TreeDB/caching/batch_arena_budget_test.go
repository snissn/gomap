package caching

import (
	"runtime"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

var batchArenaPoolTestMu sync.Mutex

func resetBatchArenaPoolsForTest() {
	batchArenaPoolBytes.Store(0)
	batchArenaPoolLastGC.Store(0)
	batchArenaPoolBudgetState.Store(batchArenaPoolBudgetCache{})
	for i := range batchArenaPools {
		batchArenaPools[i] = sync.Pool{}
	}
}

func prepareBatchArenaPoolsForTest(t *testing.T) {
	t.Helper()
	batchArenaPoolTestMu.Lock()
	t.Cleanup(func() {
		resetBatchArenaPoolsForTest()
		batchArenaPoolTestMu.Unlock()
	})
	resetBatchArenaPoolsForTest()
}

func TestBatchArenaPoolAccountingRecoversOnUnexpectedCap(t *testing.T) {
	prepareBatchArenaPoolsForTest(t)

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
	prepareBatchArenaPoolsForTest(t)

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
	prepareBatchArenaPoolsForTest(t)

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
	prepareBatchArenaPoolsForTest(t)

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
	prepareBatchArenaPoolsForTest(t)

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

func TestBatchArenaPoolBudgetCacheRecomputesOnProcsMismatch(t *testing.T) {
	prepareBatchArenaPoolsForTest(t)

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
	const maxBudgetBytes = int64(256 << 20)
	if got := computeBatchArenaPoolBudgetBytesForProcs(int(^uint(0) >> 1)); got != maxBudgetBytes {
		t.Fatalf("computeBatchArenaPoolBudgetBytesForProcs(maxint)=%d want %d", got, maxBudgetBytes)
	}
}

func TestBatchArenaLeaseBytesTracksRetainReleaseLifecycle(t *testing.T) {
	db := &DB{}
	mt1 := memtable.NewBTree()
	mt2 := memtable.NewBTree()
	chunk := make([]byte, 0, 1<<batchArenaMinShift)

	db.retainBatchArenaChunksForMemtables([][]byte{chunk}, []memtable.Table{mt1, mt2})

	want := int64(cap(chunk))
	if got := db.batchArenaLeaseBytes.Load(); got != want {
		t.Fatalf("leased bytes after retain=%d want=%d", got, want)
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
	if got := db.batchArenaLeaseBytesMax.Load(); got != want {
		t.Fatalf("leased bytes max after final release=%d want=%d", got, want)
	}
}
