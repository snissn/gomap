package merging

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type mockUnsafeReverseIter struct {
	data      []entry
	idx       int
	seekedKey []byte
}

func (m *mockUnsafeReverseIter) Next() { m.idx-- }

func (m *mockUnsafeReverseIter) Valid() bool { return m.idx >= 0 && m.idx < len(m.data) }

func (m *mockUnsafeReverseIter) UnsafeKey() []byte { return []byte(m.data[m.idx].k) }

func (m *mockUnsafeReverseIter) UnsafeValue() []byte { return []byte(m.data[m.idx].v) }

func (m *mockUnsafeReverseIter) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}

func (m *mockUnsafeReverseIter) Key() []byte   { return m.UnsafeKey() }
func (m *mockUnsafeReverseIter) Value() []byte { return m.UnsafeValue() }
func (m *mockUnsafeReverseIter) KeyCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeKey()...)
}
func (m *mockUnsafeReverseIter) ValueCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeValue()...)
}
func (m *mockUnsafeReverseIter) Error() error    { return nil }
func (m *mockUnsafeReverseIter) Close() error    { return nil }
func (m *mockUnsafeReverseIter) IsDeleted() bool { return m.data[m.idx].del }

func (m *mockUnsafeReverseIter) Seek(key []byte) {
	m.seekedKey = key
	if len(m.data) == 0 {
		m.idx = -1
		return
	}
	if key == nil {
		m.idx = len(m.data) - 1
		return
	}
	for i, e := range m.data {
		if bytes.Compare([]byte(e.k), key) >= 0 {
			m.idx = i
			return
		}
	}
	m.idx = len(m.data) - 1
}

func (m *mockUnsafeReverseIter) Domain() (start, end []byte) { return nil, nil }

func TestReverseTwoWayMerger(t *testing.T) {
	// Mutable (src1, higher precedence): A:1, B:del, C:1, E:1
	mut := &mockUnsafeReverseIter{
		data: []entry{
			{"A", "valA_new", false},
			{"B", "", true},
			{"C", "valC_new", false},
			{"E", "valE", false},
		},
		idx: 3, // positioned at rightmost key
	}

	// Disk (src2, lower precedence): A:0, B:0, D:0, E:0
	disk := &mockUnsafeReverseIter{
		data: []entry{
			{"A", "valA_old", false},
			{"B", "valB_old", false},
			{"D", "valD_old", false},
			{"E", "valE_old", false},
		},
		idx: 3,
	}

	merger := NewReverseTwoWayMerger(mut, disk, nil, nil)

	expected := []struct {
		k, v string
	}{
		{"E", "valE"},
		{"D", "valD_old"},
		{"C", "valC_new"},
		{"A", "valA_new"},
	}

	var results []struct{ k, v string }
	for merger.Valid() {
		results = append(results, struct{ k, v string }{string(merger.Key()), string(merger.Value())})
		merger.Next()
	}

	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Reverse merge results mismatch.\nGot: %v\nWant:%v", results, expected)
	}

	// Test with domain [B, Y): should yield X, C, B (reverse order).
	mut2 := &mockUnsafeReverseIter{
		data: []entry{
			{"A", "1", false}, {"B", "2", false}, {"C", "3", false},
		},
		idx: 2,
	}
	disk2 := &mockUnsafeReverseIter{
		data: []entry{
			{"A", "10", false}, {"X", "100", false},
		},
		idx: 1,
	}
	merger2 := NewReverseTwoWayMerger(mut2, disk2, []byte("B"), []byte("Y"))
	expected2 := []struct {
		k, v string
	}{
		{"X", "100"}, {"C", "3"}, {"B", "2"},
	}
	results2 := []struct{ k, v string }{}
	for merger2.Valid() {
		results2 = append(results2, struct{ k, v string }{string(merger2.Key()), string(merger2.Value())})
		merger2.Next()
	}

	if !reflect.DeepEqual(results2, expected2) {
		t.Errorf("Reverse merge results mismatch (domain).\nGot: %v\nWant:%v", results2, expected2)
	}
}

func TestReverseMergingIterator_Heap(t *testing.T) {
	// Source 0 (highest precedence): includes tombstone for E.
	s0 := &mockUnsafeReverseIter{
		data: []entry{
			{"A", "a0", false},
			{"D", "d0", false},
			{"E", "", true},
			{"F", "f0", false},
		},
		idx: 3,
	}
	s1 := &mockUnsafeReverseIter{
		data: []entry{
			{"B", "b1", false},
			{"D", "d1", false},
			{"E", "e1", false},
		},
		idx: 2,
	}
	s2 := &mockUnsafeReverseIter{
		data: []entry{
			{"C", "c2", false},
			{"F", "f2", false},
			{"G", "g2", false},
		},
		idx: 2,
	}

	it := NewReverseMergingIterator([]IteratorSource{
		{Iter: s0, Priority: 0},
		{Iter: s1, Priority: 1},
		{Iter: s2, Priority: 2},
	}, nil, nil)

	expected := []struct {
		k, v string
	}{
		{"G", "g2"},
		{"F", "f0"},
		{"D", "d0"},
		{"C", "c2"},
		{"B", "b1"},
		{"A", "a0"},
	}

	var results []struct{ k, v string }
	for it.Valid() {
		results = append(results, struct{ k, v string }{string(it.Key()), string(it.Value())})
		it.Next()
	}

	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Reverse heap merge results mismatch.\nGot: %v\nWant:%v", results, expected)
	}
}
