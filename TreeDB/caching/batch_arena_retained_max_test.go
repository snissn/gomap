package caching

import "testing"

func TestNoteBatchArenaRetainedBytesMax(t *testing.T) {
	prevPool := batchArenaPoolBytes.Load()
	prevLeased := batchArenaLeasedBytesGlobal.Load()
	prevMax := batchArenaRetainedBytesMaxGlobal.Load()
	t.Cleanup(func() {
		batchArenaPoolBytes.Store(prevPool)
		batchArenaLeasedBytesGlobal.Store(prevLeased)
		batchArenaRetainedBytesMaxGlobal.Store(prevMax)
	})

	batchArenaPoolBytes.Store(32)
	batchArenaLeasedBytesGlobal.Store(16)
	batchArenaRetainedBytesMaxGlobal.Store(0)
	noteBatchArenaRetainedBytesMax()
	if got := batchArenaRetainedBytesMaxGlobal.Load(); got != 48 {
		t.Fatalf("retained max after first sample = %d, want 48", got)
	}

	batchArenaPoolBytes.Store(8)
	batchArenaLeasedBytesGlobal.Store(4)
	noteBatchArenaRetainedBytesMax()
	if got := batchArenaRetainedBytesMaxGlobal.Load(); got != 48 {
		t.Fatalf("retained max after lower sample = %d, want 48", got)
	}

	batchArenaPoolBytes.Store(64)
	batchArenaLeasedBytesGlobal.Store(24)
	noteBatchArenaRetainedBytesMax()
	if got := batchArenaRetainedBytesMaxGlobal.Load(); got != 88 {
		t.Fatalf("retained max after higher sample = %d, want 88", got)
	}
}

func TestNoteBatchArenaComponentMaxes(t *testing.T) {
	prevPoolMax := batchArenaPoolBytesMaxGlobal.Load()
	prevLeasedMax := batchArenaLeasedBytesMaxGlobal.Load()
	t.Cleanup(func() {
		batchArenaPoolBytesMaxGlobal.Store(prevPoolMax)
		batchArenaLeasedBytesMaxGlobal.Store(prevLeasedMax)
	})

	batchArenaPoolBytesMaxGlobal.Store(0)
	batchArenaLeasedBytesMaxGlobal.Store(0)

	noteBatchArenaPoolBytesMax(64)
	noteBatchArenaPoolBytesMax(32)
	if got := batchArenaPoolBytesMaxGlobal.Load(); got != 64 {
		t.Fatalf("pool max=%d want 64", got)
	}
	noteBatchArenaPoolBytesMax(96)
	if got := batchArenaPoolBytesMaxGlobal.Load(); got != 96 {
		t.Fatalf("pool max after increase=%d want 96", got)
	}

	noteBatchArenaLeasedBytesGlobalMax(48)
	noteBatchArenaLeasedBytesGlobalMax(24)
	if got := batchArenaLeasedBytesMaxGlobal.Load(); got != 48 {
		t.Fatalf("leased max=%d want 48", got)
	}
	noteBatchArenaLeasedBytesGlobalMax(80)
	if got := batchArenaLeasedBytesMaxGlobal.Load(); got != 80 {
		t.Fatalf("leased max after increase=%d want 80", got)
	}
}
