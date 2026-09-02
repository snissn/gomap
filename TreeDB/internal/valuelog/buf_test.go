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

func TestGrowBufferStatsSnapshotTracksReadAppendSites(t *testing.T) {
	before := GrowBufferStatsSnapshot()

	noteGrowReadAppendCompressedFallback(11)
	noteGrowReadAppendPayload(7)
	noteGrowReadAppendCurrentMmapDirectDecode(5)
	noteGrowReadAppendDecodedPayload(nil, 13)
	noteGrowReadAppendTemplateEncodedPayload(17)

	after := GrowBufferStatsSnapshot()

	if got := after.ReadAppendCompressedFallbackCallsTotal - before.ReadAppendCompressedFallbackCallsTotal; got != 1 {
		t.Fatalf("compressed fallback calls delta=%d want 1", got)
	}
	if got := after.ReadAppendCompressedFallbackRequestedBytesTotal - before.ReadAppendCompressedFallbackRequestedBytesTotal; got != 11 {
		t.Fatalf("compressed fallback requested delta=%d want 11", got)
	}
	buf := make([]byte, 3, 16)
	noteGrowReadAppendCompressedFallbackDst(buf, 8)
	afterDstFit := GrowBufferStatsSnapshot()
	if got := afterDstFit.ReadAppendCompressedFallbackDstPresentCallsTotal - after.ReadAppendCompressedFallbackDstPresentCallsTotal; got != 1 {
		t.Fatalf("compressed fallback dst-present calls delta=%d want 1", got)
	}
	if got := afterDstFit.ReadAppendCompressedFallbackDstFitCallsTotal - after.ReadAppendCompressedFallbackDstFitCallsTotal; got != 1 {
		t.Fatalf("compressed fallback dst-fit calls delta=%d want 1", got)
	}
	if got := afterDstFit.ReadAppendCompressedFallbackDstFitRequestedBytesTotal - after.ReadAppendCompressedFallbackDstFitRequestedBytesTotal; got != 8 {
		t.Fatalf("compressed fallback dst-fit requested delta=%d want 8", got)
	}
	if got := after.ReadAppendPayloadCallsTotal - before.ReadAppendPayloadCallsTotal; got != 1 {
		t.Fatalf("payload calls delta=%d want 1", got)
	}
	if got := after.ReadAppendPayloadRequestedBytesTotal - before.ReadAppendPayloadRequestedBytesTotal; got != 7 {
		t.Fatalf("payload requested delta=%d want 7", got)
	}
	if got := after.ReadAppendCurrentMmapDirectDecodeCallsTotal - before.ReadAppendCurrentMmapDirectDecodeCallsTotal; got != 1 {
		t.Fatalf("current mmap direct decode calls delta=%d want 1", got)
	}
	if got := after.ReadAppendCurrentMmapDirectDecodeRequestedBytesTotal - before.ReadAppendCurrentMmapDirectDecodeRequestedBytesTotal; got != 5 {
		t.Fatalf("current mmap direct decode requested delta=%d want 5", got)
	}
	if got := after.ReadAppendDecodedPayloadCallsTotal - before.ReadAppendDecodedPayloadCallsTotal; got != 1 {
		t.Fatalf("decoded payload calls delta=%d want 1", got)
	}
	if got := after.ReadAppendDecodedPayloadRequestedBytesTotal - before.ReadAppendDecodedPayloadRequestedBytesTotal; got != 13 {
		t.Fatalf("decoded payload requested delta=%d want 13", got)
	}
	if got := after.ReadAppendTemplateEncodedPayloadCallsTotal - before.ReadAppendTemplateEncodedPayloadCallsTotal; got != 1 {
		t.Fatalf("template encoded payload calls delta=%d want 1", got)
	}
	if got := after.ReadAppendTemplateEncodedPayloadRequestedBytesTotal - before.ReadAppendTemplateEncodedPayloadRequestedBytesTotal; got != 17 {
		t.Fatalf("template encoded payload requested delta=%d want 17", got)
	}

	noteGrowReadAppendDecodedPayload(buf, 8)
	afterDecodedDstFit := GrowBufferStatsSnapshot()
	if got := afterDecodedDstFit.ReadAppendDecodedPayloadDstPresentCallsTotal - after.ReadAppendDecodedPayloadDstPresentCallsTotal; got != 1 {
		t.Fatalf("decoded payload dst-present calls delta=%d want 1", got)
	}
	if got := afterDecodedDstFit.ReadAppendDecodedPayloadDstFitCallsTotal - after.ReadAppendDecodedPayloadDstFitCallsTotal; got != 1 {
		t.Fatalf("decoded payload dst-fit calls delta=%d want 1", got)
	}
	if got := afterDecodedDstFit.ReadAppendDecodedPayloadDstFitRequestedBytesTotal - after.ReadAppendDecodedPayloadDstFitRequestedBytesTotal; got != 8 {
		t.Fatalf("decoded payload dst-fit requested delta=%d want 8", got)
	}
}
