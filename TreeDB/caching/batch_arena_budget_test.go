package caching

import (
	"sync"
	"testing"
)

func resetBatchArenaPoolsForTest() {
	batchArenaPoolBytes.Store(0)
	for i := range batchArenaPools {
		batchArenaPools[i] = sync.Pool{}
	}
}

func TestBatchArenaPoolAccountingRecoversOnMissAndUnexpectedCap(t *testing.T) {
	resetBatchArenaPoolsForTest()

	_, classCap, ok := batchArenaClassForLen(1 << batchArenaMinShift)
	if !ok {
		t.Fatal("batchArenaClassForLen failed")
	}

	batchArenaPoolBytes.Store(1234)
	buf := getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes after pool miss=%d want 0", got)
	}

	idx, ok := batchArenaClassForCap(classCap)
	if !ok {
		t.Fatal("batchArenaClassForCap failed")
	}
	wrong := make([]byte, 0, classCap*2)
	batchArenaPools[idx].Put(wrong)
	batchArenaPoolBytes.Store(int64(cap(wrong)))

	buf = getBatchArena(classCap)
	if cap(buf) != classCap {
		t.Fatalf("getBatchArena with wrong-cap pooled buffer returned cap=%d want %d", cap(buf), classCap)
	}
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("batchArenaPoolBytes after wrong-cap recovery=%d want 0", got)
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
