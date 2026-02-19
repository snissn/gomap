package memtable

import "testing"

func TestPutAppendOnlyEntriesClearsReferences(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	entries[0].key = []byte("k0")
	entries[0].value = []byte("v0")
	entries[1].key = []byte("k1")
	entries[1].value = []byte("v1")

	putAppendOnlyEntries(entries)

	for i := range entries {
		if entries[i].key != nil || entries[i].value != nil {
			t.Fatalf("entry %d still retains references after put: %+v", i, entries[i])
		}
	}
}

func TestAppendOnlyIteratorCloseClearsPooledEntries(t *testing.T) {
	entries := make([]appendOnlyEntry, 2)
	entries[0].key = []byte("k0")
	entries[0].value = []byte("v0")
	entries[1].key = []byte("k1")
	entries[1].value = []byte("v1")

	it := &appendOnlyIterator{
		entries:       entries,
		pooledEntries: true,
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	for i := range entries {
		if entries[i].key != nil || entries[i].value != nil {
			t.Fatalf("entry %d still retains references after close: %+v", i, entries[i])
		}
	}
	if it.entries != nil {
		t.Fatalf("iterator entries not cleared on close")
	}
	if it.pooledEntries {
		t.Fatalf("pooledEntries flag not cleared on close")
	}
}

func TestAppendOnlyUnorderedIteratorUsesPooledEntries(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	m.Set([]byte("k2"), []byte("v2"))
	m.Set([]byte("k1"), []byte("v1")) // force unordered iterator path

	rawIt := m.NewIterator(nil, nil)
	it, ok := rawIt.(*appendOnlyIterator)
	if !ok {
		t.Fatalf("unexpected iterator type %T", rawIt)
	}
	if !it.pooledEntries {
		t.Fatalf("unordered iterator should use pooled entry copy buffer")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}
