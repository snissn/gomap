package treedb

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestOverlayEntryBatchUsesProvidedBackingForSmallMutationSets(t *testing.T) {
	var backing [2]batch.Entry
	b := newOverlayEntryBatch(backing[:0])

	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set first entry: %v", err)
	}
	if err := b.Delete([]byte("k2")); err != nil {
		t.Fatalf("delete second entry: %v", err)
	}
	if len(b.entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(b.entries))
	}
	if &b.entries[0] != &backing[0] {
		t.Fatalf("expected first entry to reuse provided backing")
	}
	if &b.entries[1] != &backing[1] {
		t.Fatalf("expected second entry to reuse provided backing")
	}
}

func TestOverlayEntryBatchGrowsPastProvidedBackingWhenNeeded(t *testing.T) {
	var backing [1]batch.Entry
	b := newOverlayEntryBatch(backing[:0])

	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set first entry: %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set second entry: %v", err)
	}
	if len(b.entries) != 2 {
		t.Fatalf("entry count = %d, want 2", len(b.entries))
	}
	if &b.entries[0] == &backing[0] {
		t.Fatalf("expected growth past provided backing to reallocate")
	}
}
