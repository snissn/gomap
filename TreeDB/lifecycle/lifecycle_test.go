package lifecycle

import (
	"math"
	"testing"
)

func TestLifecycle(t *testing.T) {
	gy := NewGraveyard()
	reg := NewReaderRegistry()

	// Add pages from Seq 10
	gy.Add(10, []uint64{100, 101})

	// Case 1: Active Reader at Seq 5
	// MinPinned = 5.
	// Current = 20. KeepRecent = 0.
	// Limit = min(5, 20) = 5.
	// Seq 10 < 5 is False.
	// Should NOT extract.

	rid := reg.Register(5)
	min := reg.MinPinnedSeq()
	if min != 5 {
		t.Errorf("Expected min 5, got %d", min)
	}

	freed := gy.Extract(min, 20, 0)
	if len(freed) != 0 {
		t.Errorf("Extracted pages while reader active: %v", freed)
	}

	// Case 2: Reader moves to Seq 15 (Unregister 5, Register 15)
	reg.Unregister(rid)
	rid = reg.Register(15)

	min = reg.MinPinnedSeq()
	if min != 15 {
		t.Errorf("Expected min 15, got %d", min)
	}

	// Current = 20. KeepRecent = 5.
	// SafeHistory = 15.
	// Limit = min(15, 15) = 15.
	// Seq 10 < 15 is True.
	// Should extract.

	freed = gy.Extract(min, 20, 5)
	if len(freed) != 2 {
		t.Errorf("Expected 2 freed pages, got %d", len(freed))
	}

	// Case 3: KeepRecent blocking
	gy.Add(18, []uint64{200})
	// Current 20. KeepRecent 5. SafeHistory 15.
	// MinPinned 15.
	// Limit 15.
	// Seq 18 < 15 False.

	freed = gy.Extract(min, 20, 5)
	if len(freed) != 0 {
		t.Errorf("KeepRecent failed, freed: %v", freed)
	}
}

func TestReaderRegistry_FastPathSingleSeq(t *testing.T) {
	reg := NewReaderRegistry()
	rid1 := reg.Register(42)
	rid2 := reg.Register(42)

	if rid1 != rid2 {
		t.Fatalf("expected shared fast-path handle, got rid1=%d rid2=%d", rid1, rid2)
	}
	if min := reg.MinPinnedSeq(); min != 42 {
		t.Fatalf("expected min 42, got %d", min)
	}

	reg.Unregister(rid1)
	if min := reg.MinPinnedSeq(); min != 42 {
		t.Fatalf("expected min 42 with one fast reader remaining, got %d", min)
	}

	reg.Unregister(rid2)
	if min := reg.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected no pinned readers, got %d", min)
	}
}

func TestReaderRegistry_FastAndSlowReadersMin(t *testing.T) {
	reg := NewReaderRegistry()
	fastID := reg.Register(10)
	slowID := reg.Register(9)

	if min := reg.MinPinnedSeq(); min != 9 {
		t.Fatalf("expected min 9 with slow reader present, got %d", min)
	}

	reg.Unregister(slowID)
	if min := reg.MinPinnedSeq(); min != 10 {
		t.Fatalf("expected min 10 after unregistering slow reader, got %d", min)
	}

	reg.Unregister(fastID)
	if min := reg.MinPinnedSeq(); min != math.MaxUint64 {
		t.Fatalf("expected no pinned readers, got %d", min)
	}
}
