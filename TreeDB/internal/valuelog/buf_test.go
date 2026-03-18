package valuelog

import "testing"

func TestGrowBufferStatsSnapshotTracksDeltas(t *testing.T) {
	before := GrowBufferStatsSnapshot()

	var buf []byte
	buf = grow(buf, 4) // realloc
	buf = grow(buf, 2) // realloc
	buf = buf[:0]
	buf = grow(buf, 2) // no realloc
	_ = buf

	after := GrowBufferStatsSnapshot()

	callsDelta := after.CallsTotal - before.CallsTotal
	if callsDelta < 3 {
		t.Fatalf("calls delta=%d want >=3", callsDelta)
	}
	reallocDelta := after.ReallocCallsTotal - before.ReallocCallsTotal
	if reallocDelta < 2 {
		t.Fatalf("realloc calls delta=%d want >=2", reallocDelta)
	}
	requestedDelta := after.RequestedBytesTotal - before.RequestedBytesTotal
	if requestedDelta < 8 {
		t.Fatalf("requested bytes delta=%d want >=8", requestedDelta)
	}
	allocatedDelta := after.AllocatedBytesTotal - before.AllocatedBytesTotal
	if allocatedDelta < 12 {
		t.Fatalf("allocated bytes delta=%d want >=12", allocatedDelta)
	}
	copiedDelta := after.CopiedBytesTotal - before.CopiedBytesTotal
	if copiedDelta < 4 {
		t.Fatalf("copied bytes delta=%d want >=4", copiedDelta)
	}
	wasteDelta := after.CapacityWasteBytesTotal - before.CapacityWasteBytesTotal
	if wasteDelta < 2 {
		t.Fatalf("capacity waste bytes delta=%d want >=2", wasteDelta)
	}
}
