package lifecycle

import (
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestReaderRegistryPinPurposesDoNotMaskEqualSequenceReader(t *testing.T) {
	r := NewReaderRegistry()
	page := mustRegisterPageRetention(t, r, 7)
	reader := mustRegisterReader(t, r, 7)
	if page == reader {
		t.Fatal("different purposes shared a release counter")
	}
	pageAgain := mustRegisterPageRetention(t, r, 7)
	if pageAgain != page {
		t.Fatal("equal sequence/purpose did not share its cohort")
	}
	if got := r.MinReaderPinnedSeq(); got != 7 {
		t.Fatalf("reader minimum=%d want 7", got)
	}
	r.Unregister(pageAgain)
	r.Unregister(reader)
	if got := r.MinReaderPinnedSeq(); got != math.MaxUint64 {
		t.Fatalf("reader minimum after reader close=%d want no readers", got)
	}
	if got := r.MinPinnedSeq(); got != 7 {
		t.Fatalf("page retention minimum=%d want 7", got)
	}
	r.Unregister(page)
	if got := r.MinPinnedSeq(); got != math.MaxUint64 {
		t.Fatalf("all-pin minimum after drain=%d", got)
	}
	if got := unsafe.Sizeof(readerRegistryShard{}); got != 64 {
		t.Fatalf("purpose tag changed fixed shard footprint: %d", got)
	}
}

func TestReaderRegistryPinPurposesFallbackAndHandleReuse(t *testing.T) {
	r := NewReaderRegistry()
	var fast [FastReaderShardCount]int64
	for i := range fast {
		fast[i], _ = r.RegisterFastWithHint(uint64(100+i), -1)
		if fast[i] >= 0 {
			t.Fatal("initial reader cohort refused")
		}
	}
	page := mustRegisterPageRetention(t, r, 5)
	reader := mustRegisterReader(t, r, 5)
	if page <= 0 || reader <= 0 || page == reader {
		t.Fatal("fallback pins did not receive independent handles")
	}
	if got := r.MinReaderPinnedSeq(); got != 5 {
		t.Fatalf("fallback reader minimum=%d want 5", got)
	}
	r.Unregister(reader)
	if got := r.MinReaderPinnedSeq(); got != 100 {
		t.Fatalf("page-only fallback changed reader minimum: %d", got)
	}
	if got := r.MinPinnedSeq(); got != 5 {
		t.Fatalf("fallback page pin lost from all-pin minimum: %d", got)
	}
	// Reuse the same original slots in both purpose directions.
	pageAgain := mustRegisterPageRetention(t, r, 3)
	if pageAgain != reader {
		t.Fatal("released fallback slot was not reused")
	}
	r.Unregister(page)
	readerAgain := mustRegisterReader(t, r, 2)
	if readerAgain != page {
		t.Fatal("page fallback slot was not reusable by a reader")
	}
	if got := r.MinReaderPinnedSeq(); got != 2 {
		t.Fatalf("reused reader slot minimum=%d want 2", got)
	}
	r.Unregister(readerAgain)
	if got := r.MinPinnedSeq(); got != 3 {
		t.Fatalf("remaining page fallback minimum=%d want 3", got)
	}
	if got := r.MinReaderPinnedSeq(); got != 100 {
		t.Fatalf("reused page slot still counted as a reader: %d", got)
	}
	r.Unregister(pageAgain)
	for _, id := range fast {
		r.Unregister(id)
	}
	if r.MinPinnedSeq() != math.MaxUint64 || r.MinReaderPinnedSeq() != math.MaxUint64 {
		t.Fatal("fallback purpose pins did not drain")
	}
}

func TestReaderRegistryPurposeRecycleSerializesWithMinimumScan(t *testing.T) {
	r := NewReaderRegistry()
	page := mustRegisterPageRetention(t, r, 7)
	shard := int(-page - 1)
	if shard < 0 || shard >= fastReaderShardCount {
		t.Fatal("page pin did not use fixed shard")
	}
	r.Unregister(page)
	// A first reader in a formerly page-only cohort lowers the reader minimum
	// even when its sequence is unchanged. It must serialize with that scan.
	r.mu.Lock()
	done := make(chan int64, 1)
	go func() {
		id, _ := mustRegisterReaderWithHint(t, r, 7, shard)
		done <- id
	}()
	select {
	case id := <-done:
		r.mu.Unlock()
		r.Unregister(id)
		t.Fatal("new reader purpose published through the minimum scan")
	case <-time.After(25 * time.Millisecond):
	}
	r.mu.Unlock()
	select {
	case id := <-done:
		if id != page {
			r.Unregister(id)
			t.Fatal("empty shard was not reused")
		}
		if got := r.MinReaderPinnedSeq(); got != 7 {
			t.Fatalf("recycled shard retained page-only purpose: %d", got)
		}
		r.Unregister(id)
	case <-time.After(time.Second):
		t.Fatal("reader admission did not finish after scan unlocked")
	}
	if r.MinPinnedSeq() != math.MaxUint64 || r.MinReaderPinnedSeq() != math.MaxUint64 {
		t.Fatal("recycled purpose cohort did not drain")
	}
}

func TestReaderRegistryConcurrentPurposeCohorts(t *testing.T) {
	r := NewReaderRegistry()
	page := mustRegisterPageRetention(t, r, 1)
	reader := mustRegisterReader(t, r, 7)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 1000; i++ {
				p := mustRegisterPageRetention(t, r, 7)
				s := mustRegisterReader(t, r, 7)
				if r.MinPinnedSeq() != 1 || r.MinReaderPinnedSeq() != 7 {
					t.Error("concurrent purpose join hid a held pin")
				}
				r.Unregister(s)
				r.Unregister(p)
			}
		}()
	}
	workers.Wait()
	r.Unregister(reader)
	if r.MinReaderPinnedSeq() != math.MaxUint64 || r.MinPinnedSeq() != 1 {
		t.Fatal("directory cohort inherited reader purpose after concurrent drain")
	}
	r.Unregister(page)
	if r.MinPinnedSeq() != math.MaxUint64 {
		t.Fatal("concurrent purpose cohorts leaked")
	}
}
