package caching

import (
	"sync"
	"testing"
)

var batchArenaPoolTestMu sync.Mutex

func resetBatchArenaPoolsForTest() {
	batchArenaPoolBytes.Store(0)
	batchArenaPoolLastGC.Store(0)
	batchArenaPoolBudgetProcs.Store(0)
	batchArenaPoolBudgetCached.Store(0)
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

	const fakeNumGC uint32 = 7
	batchArenaPoolNumGC = func() uint32 { return fakeNumGC }

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

	var fakeNumGC uint32 = 7
	batchArenaPoolNumGC = func() uint32 { return fakeNumGC }

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
