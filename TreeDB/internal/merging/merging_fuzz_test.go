package merging

import (
	"sort"
	"testing"
)

func buildEntries(data []byte, val string) []entry {
	if len(data) == 0 {
		return nil
	}
	m := make(map[string]entry, len(data))
	for _, b := range data {
		key := string([]byte{b})
		m[key] = entry{k: key, v: val, del: false}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]entry, len(keys))
	for i, k := range keys {
		entries[i] = m[k]
	}
	return entries
}

func FuzzMergingIterator(f *testing.F) {
	f.Add([]byte("abc"), []byte("bcd"))
	f.Fuzz(func(t *testing.T, a, b []byte) {
		entriesA := buildEntries(a, "a")
		entriesB := buildEntries(b, "b")

		ia := &mockUnsafeIter{data: entriesA}
		ib := &mockUnsafeIter{data: entriesB}
		it := NewMergingIterator([]IteratorSource{
			{Iter: ia, Priority: 0},
			{Iter: ib, Priority: 1},
		}, nil, nil)

		expected := make(map[string]string)
		for _, e := range entriesB {
			expected[e.k] = e.v
		}
		for _, e := range entriesA {
			expected[e.k] = e.v
		}

		prev := ""
		for it.Valid() {
			k := string(it.Key())
			if prev != "" && k < prev {
				t.Fatalf("keys out of order: %q before %q", prev, k)
			}
			if want, ok := expected[k]; !ok || want != string(it.Value()) {
				t.Fatalf("unexpected entry %q=%q", k, it.Value())
			}
			delete(expected, k)
			prev = k
			it.Next()
		}

		if len(expected) != 0 {
			t.Fatalf("missing keys: %d", len(expected))
		}
	})
}
