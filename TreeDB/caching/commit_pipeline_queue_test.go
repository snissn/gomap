package caching

import "testing"

func TestCommitQueueCapacityScalesWithLanes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	opts := Options{
		FlushThreshold: 1,
		AllowUnsafe:    true,
		JournalLanes:   4,
	}

	db, err := Open(dir, backend, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if cap(db.commitCh) != 4*commitQueueDepthPerLane {
		t.Fatalf("commitCh cap = %d, want %d", cap(db.commitCh), 4*commitQueueDepthPerLane)
	}
}
