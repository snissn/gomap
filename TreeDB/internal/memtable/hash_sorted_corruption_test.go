package memtable

import "testing"

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
