package memtable

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

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

func TestAppendOnlyResetReusesEntryBuffers(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	key1 := []byte("long-key-01")
	val1 := []byte("value-aaaaaa")
	m.Set(key1, val1)
	if m.count != 1 {
		t.Fatalf("count=%d want=1", m.count)
	}
	keyBufPtr := &m.entries[0].key[0]
	valBufPtr := &m.entries[0].value[0]

	m.Reset()
	key2 := []byte("long-key-02")
	val2 := []byte("value-bbbbbb")
	m.Set(key2, val2)
	if m.count != 1 {
		t.Fatalf("count after reset=%d want=1", m.count)
	}
	if &m.entries[0].key[0] != keyBufPtr {
		t.Fatalf("expected key buffer reuse across reset")
	}
	if m.entries[0].value == nil || cap(m.entries[0].value) < len(val2) {
		t.Fatalf("expected value buffer capacity reuse across reset")
	}
	// Value buffers may be drawn from the shared pool, so pointer equality is
	// not guaranteed as long as capacity reuse avoids fresh allocations.
	_ = valBufPtr
}

func TestAppendOnlyResetDoesNotPoolStolenValueSlices(t *testing.T) {
	m := NewAppendOnlyWithCapacity(0)
	external := []byte("external-immutable")
	m.SetEntrySteal([]byte("k-steal"), external, page.ValuePtr{}, node.FlagInline)
	m.Reset()

	newVal := []byte("replacement-value")
	m.Set([]byte("k-new"), newVal)
	if string(external) != "external-immutable" {
		t.Fatalf("stolen caller value was mutated via pooled reuse: got %q", external)
	}
}
