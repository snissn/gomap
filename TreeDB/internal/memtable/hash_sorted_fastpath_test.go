package memtable

import (
	"fmt"
	"sort"
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

func TestHashSortedApplyCopySortedBatchTrusted_AppendAndFallback(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("b"), []byte("v1"))

	keyC := []byte("c")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: keyC, Value: []byte("v2")},
		{Type: batchpkg.OpDelete, Key: []byte("d")},
	}
	if borrowed := m.ApplyCopySortedBatchTrusted(entries, true, true, nil); borrowed {
		t.Fatalf("HashSorted reported borrowed values")
	}
	keyC[0] = 'z'

	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v2" {
		t.Fatalf("key c mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("d")); !ok || !del || got != nil {
		t.Fatalf("key d tombstone mismatch: ok=%v del=%v val=%v", ok, del, got)
	}

	fallbackEntries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("a"), Value: []byte("va")},
		{Type: batchpkg.OpPut, Key: []byte("b"), Value: []byte("vb")},
	}
	m.ApplyCopySortedBatchTrusted(fallbackEntries, true, true, nil)

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

func TestHashSortedApplyCopySortedBatchTrusted_MixedPendingChunkNotMarkedSorted(t *testing.T) {
	indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
	m := NewHashSortedWithCapacityAndIndexer(0, indexer)

	// Prior generic inserts leave an unsealed, unsorted pending chunk.
	m.Set([]byte("m"), []byte("vm"))
	m.Set([]byte("a"), []byte("va"))

	entries := make([]batchpkg.Entry, hashSortedSealKeysThreshold-2)
	for i := range entries {
		entries[i] = batchpkg.Entry{
			Type:  batchpkg.OpPut,
			Key:   []byte(fmt.Sprintf("n%05d", i)),
			Value: []byte("v"),
		}
	}
	m.ApplyCopySortedBatchTrusted(entries, false, true, nil)

	select {
	case work := <-indexer.ch:
		if work.sorted {
			t.Fatalf("mixed pending chunk marked sorted")
		}
		if len(work.keys) != hashSortedSealKeysThreshold {
			t.Fatalf("chunk len=%d want %d", len(work.keys), hashSortedSealKeysThreshold)
		}
		if sort.StringsAreSorted(work.keys) {
			t.Fatalf("test setup expected mixed chunk to require sorting")
		}
	default:
		t.Fatalf("expected sealed mixed pending chunk")
	}
}

func TestHashSortedApplyCopySortedBatchTrusted_ConsecutiveSortedPendingChunkMarkedSorted(t *testing.T) {
	indexer := &HashSortedIndexer{ch: make(chan hashSortedIndexWork, 1)}
	m := NewHashSortedWithCapacityAndIndexer(0, indexer)

	entries := make([]batchpkg.Entry, hashSortedSealKeysThreshold)
	for i := range entries {
		entries[i] = batchpkg.Entry{
			Type:  batchpkg.OpPut,
			Key:   []byte(fmt.Sprintf("n%05d", i)),
			Value: []byte("v"),
		}
	}
	m.ApplyCopySortedBatchTrusted(entries[:hashSortedSealKeysThreshold/2], false, true, nil)
	m.ApplyCopySortedBatchTrusted(entries[hashSortedSealKeysThreshold/2:], false, true, nil)

	select {
	case work := <-indexer.ch:
		if !work.sorted {
			t.Fatalf("pure sorted pending chunk not marked sorted")
		}
		if !sort.StringsAreSorted(work.keys) {
			t.Fatalf("pure sorted chunk is not sorted")
		}
	default:
		t.Fatalf("expected sealed sorted pending chunk")
	}
}
