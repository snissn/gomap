package batch

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestOwnedBoundedReleaseDetachesBorrowedBackingBeforeRefund(t *testing.T) {
	b := NewOwnedBounded(nil, page.DefaultInlineThreshold, 4)
	if cap(b.entries) != 4 {
		t.Fatal(cap(b.entries))
	}
	key, value := []byte("key"), []byte("value")
	if err := b.AppendViewTrustedSortedUnique(key, value); err != nil {
		t.Fatal(err)
	}
	// Observe backing directly to detect aliases surviving a slice reset/drop.
	entries := b.entries[:cap(b.entries)]
	entries[3] = Entry{Key: key, Value: value}
	b.arenaChunks = [][]byte{key, value}
	arena := b.arenaChunks[:cap(b.arenaChunks)]
	b.thresholdForKey = func([]byte) int { return len(value) }
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Key != nil || entry.Value != nil || entry.IsPtr {
			t.Fatal("borrowed entry alias survived terminal release")
		}
	}
	for _, chunk := range arena {
		if chunk != nil {
			t.Fatal("borrowed arena alias survived terminal release")
		}
	}
	if b.entries != nil || b.arenaChunks != nil || b.reader != nil || b.thresholdForKey != nil || b.lastKey != nil || !b.closed || b.poolKind != batchPoolKindOwned {
		t.Fatal("owned header retains original backing or callbacks")
	}
	Release(b) // terminal duplicate cannot insert this header into a pool
	if err := b.Set(key, value); err != ErrBatchClosed {
		t.Fatal(err)
	}
}
