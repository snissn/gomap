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

func TestOverlayEntryBatchSetOwnedBytesReusesCallerSlices(t *testing.T) {
	var backing [1]batch.Entry
	b := newOverlayEntryBatch(backing[:0])

	key := []byte("owned-key")
	value := []byte("owned-value")
	if err := b.SetOwnedBytes(key, value); err != nil {
		t.Fatalf("set owned bytes: %v", err)
	}
	if len(b.entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(b.entries))
	}
	if len(b.entries[0].Key) == 0 || &b.entries[0].Key[0] != &key[0] {
		t.Fatalf("expected owned key slice reuse")
	}
	if len(b.entries[0].Value) == 0 || &b.entries[0].Value[0] != &value[0] {
		t.Fatalf("expected owned value slice reuse")
	}
}

func TestOverlayEntryBatchSetOwnedKeyReusesCallerSlice(t *testing.T) {
	var backing [1]batch.Entry
	b := newOverlayEntryBatch(backing[:0])

	key := []byte("owned-key")
	if err := b.SetOwnedKey(key); err != nil {
		t.Fatalf("set owned key: %v", err)
	}
	if len(b.entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(b.entries))
	}
	if len(b.entries[0].Key) == 0 || &b.entries[0].Key[0] != &key[0] {
		t.Fatalf("expected owned key slice reuse")
	}
	if b.entries[0].Value != nil {
		t.Fatalf("expected owned key write to keep nil value")
	}
}

func TestOverlayEntryBatchDeleteOwnedKeyReusesCallerSlice(t *testing.T) {
	var backing [1]batch.Entry
	b := newOverlayEntryBatch(backing[:0])

	key := []byte("owned-key")
	if err := b.DeleteOwnedKey(key); err != nil {
		t.Fatalf("delete owned key: %v", err)
	}
	if len(b.entries) != 1 {
		t.Fatalf("entry count = %d, want 1", len(b.entries))
	}
	if len(b.entries[0].Key) == 0 || &b.entries[0].Key[0] != &key[0] {
		t.Fatalf("expected owned key slice reuse")
	}
	if b.entries[0].Type != batch.OpDelete {
		t.Fatalf("entry type = %v, want delete", b.entries[0].Type)
	}
}
