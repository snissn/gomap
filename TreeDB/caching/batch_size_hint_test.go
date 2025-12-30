package caching

import "testing"

func TestNewBatchWithSize_TreatsHintAsBytes(t *testing.T) {
	db := &DB{}
	b := db.NewBatchWithSize(1 << 20)
	if b == nil {
		t.Fatalf("expected batch")
	}
	// The hint is bytes; it should not become a huge entry capacity.
	if cap(b.entries) >= 1<<20 {
		t.Fatalf("unexpected entries capacity: got %d", cap(b.entries))
	}
}
