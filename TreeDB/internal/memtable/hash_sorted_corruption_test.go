package memtable

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func forceHashSortedStaleIndex(t *testing.T, m *HashSorted, key string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.items[key]; !ok {
		t.Fatalf("missing key %q in items map", key)
	}
	m.items[key] = uint32(len(m.entries) + 32)
}

func TestHashSortedSetRecoversFromStaleIndex(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("k1"), []byte("v1"))
	forceHashSortedStaleIndex(t, m, "k1")

	m.Set([]byte("k1"), []byte("v2"))

	v, tomb, ok := m.Get([]byte("k1"))
	if !ok {
		t.Fatalf("expected key present after stale-index set")
	}
	if tomb {
		t.Fatalf("expected inline value, got tombstone")
	}
	if string(v) != "v2" {
		t.Fatalf("unexpected value %q", string(v))
	}
}

func TestHashSortedDeleteWithCallbackRecoversFromStaleIndex(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("k1"), []byte("v1"))
	forceHashSortedStaleIndex(t, m, "k1")

	called := false
	if err := m.DeleteWithCallback([]byte("k1"), func(k, v []byte) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("DeleteWithCallback: %v", err)
	}
	if !called {
		t.Fatalf("expected callback invocation")
	}

	_, tomb, ok := m.Get([]byte("k1"))
	if !ok {
		t.Fatalf("expected key present as tombstone after stale-index delete")
	}
	if !tomb {
		t.Fatalf("expected tombstone entry")
	}
}

func TestHashSortedGetRecoversFromStaleIndex(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("k1"), []byte("v1"))
	forceHashSortedStaleIndex(t, m, "k1")

	v, tomb, ok := m.Get([]byte("k1"))
	if ok {
		t.Fatalf("expected stale-index read miss before repair, got ok=%v tomb=%v val=%q", ok, tomb, string(v))
	}

	m.Set([]byte("k1"), []byte("v2"))

	v, tomb, ok = m.Get([]byte("k1"))
	if !ok {
		t.Fatalf("expected key present after stale-index read repair")
	}
	if tomb {
		t.Fatalf("expected inline value, got tombstone")
	}
	if string(v) != "v2" {
		t.Fatalf("unexpected value %q", string(v))
	}
}

func TestHashSortedGetEntryDropsStaleIndex(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("k1"), []byte("v1"))
	forceHashSortedStaleIndex(t, m, "k1")

	v, ptr, flags, ok := m.GetEntry([]byte("k1"))
	if ok || v != nil || ptr != (page.ValuePtr{}) || flags != 0 {
		t.Fatalf("expected stale-index GetEntry miss, got (%v,%+v,%#x,%v)", v, ptr, flags, ok)
	}

	m.mu.RLock()
	_, stillPresent := m.items["k1"]
	m.mu.RUnlock()
	if stillPresent {
		t.Fatalf("expected stale index to be dropped after GetEntry")
	}
}

func TestHashSortedEnsureIndexFrozen_RebuildsWrongKeySetEvenWhenCountsMatch(t *testing.T) {
	m := NewHashSorted()
	m.Set([]byte("a"), []byte("va"))
	m.Set([]byte("b"), []byte("vb"))
	m.Set([]byte("d"), []byte("vd"))

	m.mu.Lock()
	m.frozen = true
	m.sortedValid = false
	m.pendingKeys = nil
	m.pendingBytes = 0
	m.sortedKeys = nil
	m.nextSeq = 1
	m.index.runs = [][]string{{"a", "c", "d"}}
	m.index.doneTo = 1
	m.mu.Unlock()

	m.ensureIndexFrozen()

	m.mu.RLock()
	got := append([]string(nil), m.sortedKeys...)
	m.mu.RUnlock()

	want := []string{"a", "b", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted keys mismatch after rebuild: got %v want %v", got, want)
	}
}
