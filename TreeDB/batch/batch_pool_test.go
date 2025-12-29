package batch

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestBatchReleaseClearsPointers(t *testing.T) {
	sm, err := slab.NewSlabManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewSlabManager: %v", err)
	}
	defer func() { _ = sm.Close() }()

	b := New(sm, page.DefaultInlineThreshold)
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	entries := b.entries
	if len(entries) == 0 {
		t.Fatalf("expected entries to be populated")
	}

	b.largeKeys = [][]byte{[]byte("k")}
	b.largeVals = [][]byte{bytes.Repeat([]byte("v"), 4)}
	keys := b.largeKeys
	vals := b.largeVals

	Release(b)

	for i, entry := range entries {
		if entry.Key != nil || entry.Value != nil || entry.IsPtr || entry.ValuePtr != (page.ValuePtr{}) {
			t.Fatalf("entry %d not cleared: %+v", i, entry)
		}
	}
	for i, key := range keys {
		if key != nil {
			t.Fatalf("large key %d not cleared", i)
		}
	}
	for i, val := range vals {
		if val != nil {
			t.Fatalf("large value %d not cleared", i)
		}
	}
}
