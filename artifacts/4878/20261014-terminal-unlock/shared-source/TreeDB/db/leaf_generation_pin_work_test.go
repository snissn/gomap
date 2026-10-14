package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"testing"
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
