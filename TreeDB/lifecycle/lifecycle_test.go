package lifecycle

import (
	"math"
	"sync"
	"testing"
)

func isFastHandle(id int64) bool {
	return id < 0
}

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

	if !isFastHandle(rid1) || !isFastHandle(rid2) {
		t.Fatalf("expected fast handles for shared sequence, got rid1=%d rid2=%d", rid1, rid2)
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
	if !isFastHandle(fastID) {
		t.Fatalf("expected fast handle, got slow handle %d", fastID)
	}
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

func TestReaderRegistry_FastPathConcurrentRegisterUnregister(t *testing.T) {
	const (
		seq        = uint64(42)
		goroutines = 64
		rounds     = 200
	)

	reg := NewReaderRegistry()
	for r := 0; r < rounds; r++ {
		ids := make([]int64, goroutines)

		var regWG sync.WaitGroup
		regWG.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func(i int) {
				defer regWG.Done()
				ids[i] = reg.Register(seq)
			}(i)
		}
		regWG.Wait()

		for i, id := range ids {
			if !isFastHandle(id) {
				t.Fatalf("round %d: ids[%d]=%d, expected fast handle", r, i, id)
			}
		}
		if min := reg.MinPinnedSeq(); min != seq {
			t.Fatalf("round %d: expected min %d after concurrent register, got %d", r, seq, min)
		}

		var unregWG sync.WaitGroup
		unregWG.Add(goroutines)
		for _, id := range ids {
			go func(id int64) {
				defer unregWG.Done()
				reg.Unregister(id)
			}(id)
		}
		unregWG.Wait()

		if min := reg.MinPinnedSeq(); min != math.MaxUint64 {
			t.Fatalf("round %d: expected no pinned readers after concurrent unregister, got %d", r, min)
		}
	}
}

func TestReaderRegistry_ConcurrentFastAndSlowReaders(t *testing.T) {
	const (
		fastSeq     = uint64(100)
		slowSeq     = uint64(90)
		fastReaders = 32
		slowReaders = 16
		rounds      = 100
	)

	reg := NewReaderRegistry()
	for r := 0; r < rounds; r++ {
		anchor := reg.Register(fastSeq)
		if !isFastHandle(anchor) {
			t.Fatalf("round %d: anchor id=%d, expected fast handle", r, anchor)
		}

		fastIDs := make([]int64, fastReaders)
		slowIDs := make([]int64, slowReaders)

		var regWG sync.WaitGroup
		regWG.Add(fastReaders + slowReaders)
		for i := 0; i < fastReaders; i++ {
			go func(i int) {
				defer regWG.Done()
				fastIDs[i] = reg.Register(fastSeq)
			}(i)
		}
		for i := 0; i < slowReaders; i++ {
			go func(i int) {
				defer regWG.Done()
				slowIDs[i] = reg.Register(slowSeq)
			}(i)
		}
		regWG.Wait()

		for i, id := range fastIDs {
			if !isFastHandle(id) {
				t.Fatalf("round %d: fastIDs[%d]=%d, expected fast handle", r, i, id)
			}
		}
		if min := reg.MinPinnedSeq(); min != slowSeq {
			t.Fatalf("round %d: expected min %d with slow readers present, got %d", r, slowSeq, min)
		}

		var slowWG sync.WaitGroup
		slowWG.Add(slowReaders)
		for _, id := range slowIDs {
			go func(id int64) {
				defer slowWG.Done()
				reg.Unregister(id)
			}(id)
		}
		slowWG.Wait()
		if min := reg.MinPinnedSeq(); min != fastSeq {
			t.Fatalf("round %d: expected min %d after slow readers drain, got %d", r, fastSeq, min)
		}

		var fastWG sync.WaitGroup
		fastWG.Add(fastReaders + 1)
		go func() {
			defer fastWG.Done()
			reg.Unregister(anchor)
		}()
		for _, id := range fastIDs {
			go func(id int64) {
				defer fastWG.Done()
				reg.Unregister(id)
			}(id)
		}
		fastWG.Wait()
		if min := reg.MinPinnedSeq(); min != math.MaxUint64 {
			t.Fatalf("round %d: expected no pinned readers after fast readers drain, got %d", r, min)
		}
	}
}

func TestReaderRegistry_RegisterFastCountSaturationFallsBackToSlowHandle(t *testing.T) {
	reg := NewReaderRegistry()
	for i := 0; i < len(reg.fastShards); i++ {
		reg.fastShards[i].seq.Store(7)
		reg.fastShards[i].count.Store(math.MaxInt32)
	}

	id := reg.Register(7)
	if isFastHandle(id) {
		t.Fatalf("expected slow-handle fallback at fast-count saturation")
	}
	for i := 0; i < len(reg.fastShards); i++ {
		if got := reg.fastShards[i].count.Load(); got != math.MaxInt32 {
			t.Fatalf("shard %d fastCount overflowed/changed at saturation: got %d want %d", i, got, math.MaxInt32)
		}
	}

	if min := reg.MinPinnedSeq(); min != 7 {
		t.Fatalf("expected min 7 with saturated fast readers + slow fallback, got %d", min)
	}
}
