package batch

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchReleaseClearsPointers(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
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

	Release(b)

	if b.reader != nil {
		t.Fatalf("expected reader to be cleared")
	}
	if b.inlineThreshold != 0 {
		t.Fatalf("expected inline threshold to be reset")
	}
	if !b.closed {
		t.Fatalf("expected batch to be marked closed")
	}
	for i, entry := range entries {
		if entry.Key != nil || entry.Value != nil || entry.IsPtr || entry.ValuePtr != (page.ValuePtr{}) {
			t.Fatalf("entry %d not cleared: %+v", i, entry)
		}
	}
}
