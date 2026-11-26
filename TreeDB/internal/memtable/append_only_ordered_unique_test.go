package memtable

import (
	"bytes"
	"testing"
)

func TestAppendOnlyIteratorOrderedUniqueContract(t *testing.T) {
	type orderedUniqueIter interface {
		OrderedUniqueUnsafeIterator() bool
	}

	checkIter := func(t *testing.T, name string, m *AppendOnly) {
		t.Helper()
		it := m.NewIterator(nil, nil)
		defer func() { _ = it.Close() }()

		oui, ok := it.(orderedUniqueIter)
		if !ok || !oui.OrderedUniqueUnsafeIterator() {
			t.Fatalf("%s forward iterator OrderedUniqueUnsafeIterator()=%v (implemented=%v), want true", name, ok && oui.OrderedUniqueUnsafeIterator(), ok)
		}
		var prev []byte
		count := 0
		for it.Valid() {
			k := append([]byte(nil), it.UnsafeKey()...)
			if count > 0 && bytes.Compare(prev, k) >= 0 {
				t.Fatalf("%s forward iterator keys not strictly increasing: prev=%q cur=%q", name, prev, k)
			}
			prev = k
			count++
			it.Next()
		}
		if err := it.Error(); err != nil {
			t.Fatalf("%s forward iterator error: %v", name, err)
		}
		if count == 0 {
			t.Fatalf("%s forward iterator yielded 0 entries", name)
		}

		rev := m.NewReverseIterator(nil, nil)
		defer func() { _ = rev.Close() }()
		if revOUI, ok := rev.(orderedUniqueIter); ok && revOUI.OrderedUniqueUnsafeIterator() {
			t.Fatalf("%s reverse iterator OrderedUniqueUnsafeIterator()=true, want false", name)
		}
	}

	t.Run("ordered_mutable", func(t *testing.T) {
		m := NewAppendOnlyWithEntryCapacity(8)
		m.Set([]byte("a"), []byte("1"))
		m.Set([]byte("b"), []byte("2"))
		m.Set([]byte("c"), []byte("3"))
		checkIter(t, "ordered_mutable", m)
	})

	t.Run("ordered_frozen", func(t *testing.T) {
		m := NewAppendOnlyWithEntryCapacity(8)
		m.Set([]byte("a"), []byte("1"))
		m.Set([]byte("b"), []byte("2"))
		m.Set([]byte("c"), []byte("3"))
		m.Freeze()
		checkIter(t, "ordered_frozen", m)
	})

	t.Run("unordered_duplicates_mutable", func(t *testing.T) {
		m := NewAppendOnlyWithEntryCapacity(8)
		m.Set([]byte("c"), []byte("1"))
		m.Set([]byte("a"), []byte("2"))
		m.Set([]byte("b"), []byte("3"))
		m.Set([]byte("a"), []byte("4"))
		checkIter(t, "unordered_duplicates_mutable", m)
	})

	t.Run("unordered_duplicates_frozen", func(t *testing.T) {
		m := NewAppendOnlyWithEntryCapacity(8)
		m.Set([]byte("c"), []byte("1"))
		m.Set([]byte("a"), []byte("2"))
		m.Set([]byte("b"), []byte("3"))
		m.Set([]byte("a"), []byte("4"))
		m.Freeze()
		checkIter(t, "unordered_duplicates_frozen", m)
	})
}
