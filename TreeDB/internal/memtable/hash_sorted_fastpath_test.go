package memtable

import (
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

func TestHashSortedApplyStealSortedBatch_AppendAndFallback(t *testing.T) {
	m := NewHashSorted()
	m.SetSteal([]byte("b"), []byte("v1"))

	appendEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v2")},
		{Type: batchpkg.OpDelete, Key: []byte("d")},
	}
	m.ApplyStealSortedBatch(appendEntries, nil)

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v2" {
		t.Fatalf("key c mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("d")); !ok || !del || got != nil {
		t.Fatalf("key d tombstone mismatch: ok=%v del=%v val=%v", ok, del, got)
	}

	// First key <= current max, so this should use the regular update path.
	fallbackEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	m.ApplyStealSortedBatch(fallbackEntries, nil)

	if got, del, ok := m.Get([]byte("a")); !ok || del || string(got) != "va" {
		t.Fatalf("key a mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("b")); !ok || del || string(got) != "vb" {
		t.Fatalf("key b mismatch after update: ok=%v del=%v val=%q", ok, del, string(got))
	}
}

func TestHashSortedApplyStealSortedBatch_DuplicateKeyForcesFallback(t *testing.T) {
	m := NewHashSorted()
	m.SetSteal([]byte("b"), []byte("v0"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v1")},
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v2")},
	}
	m.ApplyStealSortedBatch(entries, nil)

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v2" {
		t.Fatalf("key c mismatch after duplicate fallback: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got := m.Len(); got != 2 {
		t.Fatalf("len=%d want 2", got)
	}
}
