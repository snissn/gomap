package merging

import (
	"testing"
)

type mockIter struct {
	data      []entry
	idx       int
	isDeleted bool
}

type entry struct {
	k, v string
	del  bool
}

func (m *mockIter) Next() { m.idx++ }
func (m *mockIter) Valid() bool { return m.idx < len(m.data) }
func (m *mockIter) Key() []byte { return []byte(m.data[m.idx].k) }
func (m *mockIter) Value() []byte { return []byte(m.data[m.idx].v) }
func (m *mockIter) Close() error { return nil }
func (m *mockIter) IsDeleted() bool { return m.data[m.idx].del }

func TestMergingIterator(t *testing.T) {
	// Mutable: [A:1, B:del, C:1]
	mut := &mockIter{
		data: []entry{
			{"A", "valA_new", false},
			{"B", "", true},
			{"C", "valC_new", false},
		},
	}

	// Disk: [A:0, B:0, D:0]
	disk := &mockIter{
		data: []entry{
			{"A", "valA_old", false},
			{"B", "valB_old", false},
			{"D", "valD_old", false},
		},
	}

	mi := NewMergingIterator([]IteratorSource{
		{Iter: mut, Priority: 0},
		{Iter: disk, Priority: 10},
	})

	// Expect: A:valA_new, C:valC_new, D:valD_old
	// B is deleted in mut, so it masks disk, and is skipped.

	expected := []struct {
		k, v string
	}{
		{"A", "valA_new"},
		{"C", "valC_new"},
		{"D", "valD_old"},
	}

	for i, want := range expected {
		if !mi.Valid() {
			t.Fatalf("step %d: expected valid", i)
		}
		if string(mi.Key()) != want.k {
			t.Errorf("step %d: got key %s, want %s", i, string(mi.Key()), want.k)
		}
		if string(mi.Value()) != want.v {
			t.Errorf("step %d: got val %s, want %s", i, string(mi.Value()), want.v)
		}
		mi.Next()
	}

	if mi.Valid() {
		t.Errorf("expected iterator to be exhausted")
	}
}
