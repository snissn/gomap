package batch

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

// The admitted directory caller owns both this header and its exact backing.
// A retained borrow exposes stale payload aliases even after Len becomes zero.
func TestCloseWithoutPoolingRetiresOriginalBackingAndViews(t *testing.T) {
	reader := newMapValueReader()
	b := NewBoundedRetainingLargeEntries(reader, page.PageSize, 4)
	payload := []byte("keyvalue")
	if err := b.SetView(payload[:3], payload[3:]); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("copied"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	b.thresholdForKey = func([]byte) int { return page.PageSize }
	b.touchedValueLog = map[uint32]struct{}{1: {}}
	entries := b.entries[:cap(b.entries)]
	arenas := b.arenaChunks[:cap(b.arenaChunks)]
	if len(arenas) == 0 {
		t.Fatal("copied entry did not exercise original arena backing")
	}
	if err := b.CloseWithoutPooling(); err != nil {
		t.Fatal(err)
	}
	if !b.closed || b.entries != nil || b.arenaChunks != nil || b.reader != nil || b.thresholdForKey != nil || b.lastKey != nil || b.touchedValueLog != nil {
		t.Fatal("disposed header retained original aliases")
	}
	for i := range entries {
		if entries[i].Key != nil || entries[i].Value != nil {
			t.Fatalf("retired entry view %d still aliases payload", i)
		}
	}
	for i := range arenas {
		if arenas[i] != nil {
			t.Fatalf("retired arena view %d still aliases backing", i)
		}
	}
	if string(payload) != "keyvalue" {
		t.Fatal("disposal modified caller-owned payload")
	}
	entries, arenas, b, payload, reader = nil, nil, nil, nil, nil
	// The original caller can now refund its containing/header/payload charge.
}
