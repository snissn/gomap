package memtable

import (
	"bytes"
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

func TestHashSortedApplyStealSortedBatchIndicesTrusted(t *testing.T) {
	m := NewHashSorted()
	m.SetSteal([]byte("b"), []byte("v0"))

	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("ignore"), Value: []byte("skip")},
		{Type: batchpkg.OpPut, Key: []byte("c"), Value: []byte("v1")},
		{Type: batchpkg.OpDelete, Key: []byte("d")},
	}
	var seen []string
	m.ApplyStealSortedBatchIndicesTrusted(entries, []int{1, 2}, func(key []byte) {
		seen = append(seen, string(key))
	})

	if _, _, ok := m.Get([]byte("ignore")); ok {
		t.Fatal("ignored entry was applied")
	}
	if got, del, ok := m.Get([]byte("c")); !ok || del || string(got) != "v1" {
		t.Fatalf("key c mismatch: ok=%v del=%v val=%q", ok, del, string(got))
	}
	if got, del, ok := m.Get([]byte("d")); !ok || !del || got != nil {
		t.Fatalf("key d tombstone mismatch: ok=%v del=%v val=%v", ok, del, got)
	}
	if want := []string{"c", "d"}; !equalStrings(seen, want) {
		t.Fatalf("onKey saw %v want %v", seen, want)
	}
}

func TestHashSortedApplyCopySortedBatchIndicesTrustedCopiesSelectedEntries(t *testing.T) {
	m := NewHashSorted()
	key := []byte("a")
	value := []byte("va")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("ignore"), Value: []byte("skip")},
		{Type: batchpkg.OpPut, Key: key, Value: value},
		{Type: batchpkg.OpDelete, Key: []byte("b")},
	}
	var seen []string
	m.ApplyCopySortedBatchIndicesTrusted(entries, []int{1, 2}, false, func(key []byte) {
		seen = append(seen, string(key))
	})

	key[0] = 'z'
	value[0] = 'X'
	if _, _, ok := m.Get([]byte("ignore")); ok {
		t.Fatal("ignored entry was applied")
	}
	got, del, ok := m.Get([]byte("a"))
	if !ok || del || !bytes.Equal(got, []byte("va")) {
		t.Fatalf("Get(a)=(%q,%v,%v), want (va,false,true)", got, del, ok)
	}
	if got, del, ok := m.Get([]byte("b")); !ok || !del || got != nil {
		t.Fatalf("Get(b)=(%v,%v,%v), want (nil,true,true)", got, del, ok)
	}
	if want := []string{"a", "b"}; !equalStrings(seen, want) {
		t.Fatalf("onKey saw %v want %v", seen, want)
	}
}

func TestHashSortedOperationMixTracksCurrentDeletes(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("k1"), []byte("v1"))
	m.Delete([]byte("k2"))
	m.Delete([]byte("k1"))

	mix := m.OperationMix()
	if mix.Entries != 2 || mix.Deletes != 2 {
		t.Fatalf("operation mix=(entries=%d deletes=%d), want (2,2)", mix.Entries, mix.Deletes)
	}

	m.Set([]byte("k1"), []byte("v2"))
	mix = m.OperationMix()
	if mix.Entries != 2 || mix.Deletes != 1 {
		t.Fatalf("operation mix after put=(entries=%d deletes=%d), want (2,1)", mix.Entries, mix.Deletes)
	}

	m.Reset()
	mix = m.OperationMix()
	if mix.Entries != 0 || mix.Deletes != 0 {
		t.Fatalf("operation mix after reset=(entries=%d deletes=%d), want (0,0)", mix.Entries, mix.Deletes)
	}
}
