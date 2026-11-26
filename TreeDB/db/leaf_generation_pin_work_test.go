package db

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestLeafPinWorkRetainRepairsOriginalPartialUnpin(t *testing.T) {
	var tracker leafGenerationPinTracker
	ids := make([]uint64, 80)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	pins := newLeafGenerationPinSet(tracker.refsForGenerationIDs(ids))
	if pins.retain(&tracker) {
		t.Fatal("fresh holder should not pin until stale publication")
	}
	pins.markStale(&tracker)
	var release leafGenerationPinReleaseWork
	zero := iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1 << 20}
	ready, err := pins.releasePinWork(&tracker, &release, &zero)
	if ready || err != nil || release.consumed || pins.holders != 1 {
		t.Fatal("zero-work changed original holder", ready, err)
	}
	w := iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	ready, err = pins.releasePinWork(&tracker, &release, &w)
	if ready || err != nil || pins.unpinPosition != 1 || pins.holders != 0 || tracker.count(ids[0]) != 0 || tracker.count(ids[1]) != 1 {
		t.Fatal("actual partial counter position", ready, err, pins.unpinPosition)
	}
	if !pins.retain(&tracker) {
		t.Fatal("new original holder did not report restored physical pins")
	}
	for _, id := range ids {
		if got := tracker.count(id); got != 1 {
			t.Fatalf("reader exposed before exact prefix repair id=%d count=%d", id, got)
		}
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, err = pins.releasePinWork(&tracker, &release, &w)
	if !ready || err != nil || pins.holders != 1 {
		t.Fatal("old receipt consumed new holder", ready, err)
	}
	for _, id := range ids {
		if tracker.count(id) != 1 {
			t.Fatal("old receipt removed restored pins", id)
		}
	}
	var last leafGenerationPinReleaseWork
	calls := 0
	for ; calls < 100; calls++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err = pins.releasePinWork(&tracker, &last, &w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("bounded actual release", err, w)
		}
		if ready {
			break
		}
	}
	if calls == 0 || calls == 100 || !last.done || pins.pinned || pins.unpinning || pins.holders != 0 {
		t.Fatal("original physical finish missing", calls, pins.holders)
	}
	for _, id := range ids {
		if tracker.count(id) != 0 {
			t.Fatal("actual last physical pin survives", id)
		}
	}
}

func TestLeafPinWorkPriorReceiptCannotConsumeLaterCycle(t *testing.T) {
	var tracker leafGenerationPinTracker
	pins := newLeafGenerationPinSet(tracker.refsForGenerationIDs([]uint64{1, 2, 3}))
	pins.retain(&tracker)
	pins.markStale(&tracker)
	var old leafGenerationPinReleaseWork
	w := iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	pins.releasePinWork(&tracker, &old, &w)
	pins.retain(&tracker)
	// The new last holder drives the same stages synchronously, before the old
	// receipt resumes. The old cycle must never underflow or replay that release.
	pins.release(&tracker)
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, err := pins.releasePinWork(&tracker, &old, &w)
	if !ready || err != nil || pins.holders != 0 {
		t.Fatal(ready, err, pins.holders)
	}
	for _, ref := range pins.refs {
		if ref.count.Load() != 0 {
			t.Fatal("counter replay", ref.count.Load())
		}
	}
}

func TestLeafPinSetConcurrentRetainReleaseRacingMarkStale(t *testing.T) {
	ids := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	for trial := 0; trial < 50; trial++ {
		var tracker leafGenerationPinTracker
		pins := newLeafGenerationPinSet(tracker.refsForGenerationIDs(ids))

		start := make(chan struct{})
		var staleDone atomic.Bool
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-start
				for i := 0; i < 200; i++ {
					pins.retain(&tracker)
					if staleDone.Load() {
						for _, genID := range ids {
							if got := tracker.count(genID); got != 1 {
								t.Errorf("trial %d: live holder observed unpinned gen=%d count=%d after markStale", trial, genID, got)
								pins.release(&tracker)
								return
							}
						}
					}
					if id == 0 && i%17 == 0 && staleDone.Load() {
						var work leafGenerationPinReleaseWork
						for {
							w := iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
							ready, err := pins.releasePinWork(&tracker, &work, &w)
							if err != nil {
								t.Errorf("trial %d: releasePinWork err=%v", trial, err)
								return
							}
							if ready {
								break
							}
						}
					} else {
						pins.release(&tracker)
					}
				}
			}(g)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pins.markStale(&tracker)
			staleDone.Store(true)
		}()

		close(start)
		wg.Wait()

		if got := atomic.LoadInt64(&pins.holders); got != 0 {
			t.Fatalf("trial %d: holders=%d want 0", trial, got)
		}
		if pins.pinned || pins.unpinning {
			t.Fatalf("trial %d: pinned=%v unpinning=%v want false/false", trial, pins.pinned, pins.unpinning)
		}
		for _, genID := range ids {
			if got := tracker.count(genID); got != 0 {
				t.Fatalf("trial %d: gen=%d count=%d want 0 after all releases", trial, genID, got)
			}
		}
	}
}
