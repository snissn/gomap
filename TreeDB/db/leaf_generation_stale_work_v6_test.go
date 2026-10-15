package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// The same original pin set must remain untouched on pre-activation refusal or
// abort, then pin its old reader exactly once when visibility actually changes.
func TestLeafGenerationStaleAdmissionBeforeActivationV6(t *testing.T) {
	tracker := &leafGenerationPinTracker{}
	first, second := &leafGenerationPinRef{id: 7}, &leafGenerationPinRef{id: 8}
	set := newLeafGenerationPinSet([]*leafGenerationPinRef{first, nil, second})
	set.retain(tracker)
	small := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1}
	transition, ready := set.beginStaleTransitionV6(tracker, small)
	if ready {
		transition.finish(false)
		t.Fatal("insufficient work admitted the old-reader pin transition")
	}
	if set.stale || set.pinned || first.count.Load() != 0 || second.count.Load() != 0 {
		t.Fatal("refusal changed the original generation pins")
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	transition, ready = set.beginStaleTransitionV6(tracker, work)
	if !ready {
		t.Fatal("funded pin transition refused")
	}
	transition.finish(false) // allocator did not activate
	if set.stale || set.pinned || first.count.Load() != 0 || second.count.Load() != 0 {
		t.Fatal("failed allocator transition pinned or retired the old generation")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	transition, ready = set.beginStaleTransitionV6(tracker, work)
	if !ready {
		t.Fatal("funded retry refused")
	}
	transition.finish(true)
	if !set.stale || !set.pinned || first.count.Load() != 1 || second.count.Load() != 1 {
		t.Fatal("successful visibility transition lost the original reader pins")
	}
	// Ordinary callers share the same core and must not pin the stale set twice.
	set.markStale(tracker)
	if first.count.Load() != 1 || second.count.Load() != 1 {
		t.Fatal("stale transition replayed")
	}
	set.release(tracker)
	if first.count.Load() != 0 || second.count.Load() != 0 {
		t.Fatal("original last reader did not release its pins")
	}
}
