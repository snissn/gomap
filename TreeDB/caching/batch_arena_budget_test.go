package caching

import (
	"sync"
	"testing"
)

func resetBatchArenaPoolsForTest() {
	batchArenaPoolBytes.Store(0)
	batchArenaPoolLastGC.Store(0)
	for i := range batchArenaPools {
		batchArenaPools[i] = sync.Pool{}
	}
}

func TestBatchArenaPoolAccountingRecoversOnUnexpectedCap(t *testing.T) {
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

func TestBatchArenaPoolAccountingMissOnlyResetsAfterGC(t *testing.T) {
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	batchArenaPoolBytes.Store(1234)
	batchArenaPoolLastGC.Store(^uint32(0))
	buf := getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 1234 {
		t.Fatalf("batchArenaPoolBytes after non-GC miss=%d want 1234", got)
	}
}

func TestBatchArenaPoolBudgetDoesNotOvercount(t *testing.T) {
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
