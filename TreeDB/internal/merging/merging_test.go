package merging

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type mockUnsafeIter struct {
	data      []entry
	idx       int
	seekedKey []byte
}

type entry struct {
	k, v string
	del  bool
}

func (m *mockUnsafeIter) Next()               { m.idx++ }
func (m *mockUnsafeIter) Valid() bool         { return m.idx < len(m.data) }
func (m *mockUnsafeIter) UnsafeKey() []byte   { return []byte(m.data[m.idx].k) }
func (m *mockUnsafeIter) UnsafeValue() []byte { return []byte(m.data[m.idx].v) }
func (m *mockUnsafeIter) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}
func (m *mockUnsafeIter) Key() []byte   { return m.UnsafeKey() } // Copy in mock is fine
func (m *mockUnsafeIter) Value() []byte { return m.UnsafeValue() }
func (m *mockUnsafeIter) KeyCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeKey()...)
}
func (m *mockUnsafeIter) ValueCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeValue()...)
}
func (m *mockUnsafeIter) Error() error    { return nil }
func (m *mockUnsafeIter) Close() error    { return nil }
func (m *mockUnsafeIter) IsDeleted() bool { return m.data[m.idx].del }
func (m *mockUnsafeIter) Seek(key []byte) {
	m.seekedKey = key
	if key == nil {
		m.idx = 0
		return
	}
	for i, e := range m.data {
		if bytes.Compare([]byte(e.k), key) >= 0 {
			m.idx = i
			return
		}
	}
	m.idx = len(m.data) // Exhausted
}
func (m *mockUnsafeIter) Domain() (start, end []byte) { return nil, nil }

func TestTwoWayMerger(t *testing.T) {
	// Mutable (src1, higher precedence): A:1, B:del, C:1, E:1
	mut := &mockUnsafeIter{
		data: []entry{
			{"A", "valA_new", false},
			{"B", "", true},
			{"C", "valC_new", false},
			{"E", "valE", false},
		},
	}

	// Disk (src2, lower precedence): A:0, B:0, D:0, E:0
	disk := &mockUnsafeIter{
		data: []entry{
			{"A", "valA_old", false},
			{"B", "valB_old", false},
			{"D", "valD_old", false},
			{"E", "valE_old", false},
		},
	}

	merger := NewTwoWayMerger(mut, disk, nil, nil) // No specific domain filters here

	expected := []struct {
		k, v string
	}{
		{"A", "valA_new"},
		{"C", "valC_new"},
		{"D", "valD_old"},
		{"E", "valE"},
	}

	var results []struct{ k, v string }
	for merger.Valid() {
		results = append(results, struct{ k, v string }{string(merger.Key()), string(merger.Value())})
		merger.Next()
	}

	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Merge results mismatch.\nGot: %v\nWant:%v", results, expected)
	}

	// Test with domain
	mut2 := &mockUnsafeIter{
		data: []entry{
			{"A", "1", false}, {"B", "2", false}, {"C", "3", false},
		},
	}
	disk2 := &mockUnsafeIter{
		data: []entry{
			{"A", "10", false}, {"X", "100", false},
		},
	}
	merger2 := NewTwoWayMerger(mut2, disk2, []byte("B"), []byte("Y"))
	expected2 := []struct {
		k, v string
	}{
		{"B", "2"}, {"C", "3"}, {"X", "100"},
	}
	results2 := []struct{ k, v string }{}
	for merger2.Valid() {
		results2 = append(results2, struct{ k, v string }{string(merger2.Key()), string(merger2.Value())})
		merger2.Next()
	}

	if !reflect.DeepEqual(results2, expected2) {
		t.Errorf("Merge results mismatch (domain).\nGot: %v\nWant:%v", results2, expected2)
	}
}

func TestTwoWayReverseMerger(t *testing.T) {
	// Source 1 (newer): E:1, C:1, B:del, A:1
	mut := &mockUnsafeIter{
		data: []entry{
			{"E", "valE_new", false},
			{"C", "valC_new", false},
			{"B", "", true},
			{"A", "valA_new", false},
		},
	}
	// Source 2 (older): F:0, D:0, B:0, A:0
	disk := &mockUnsafeIter{
		data: []entry{
			{"F", "valF_old", false},
			{"D", "valD_old", false},
			{"B", "valB_old", false},
			{"A", "valA_old", false},
		},
	}

	merger := NewTwoWayReverseMerger(mut, disk, nil, nil)
	expected := []struct {
		k, v string
	}{
		{"F", "valF_old"},
		{"E", "valE_new"},
		{"D", "valD_old"},
		{"C", "valC_new"},
		{"A", "valA_new"},
	}
	results := make([]struct{ k, v string }, 0, len(expected))
	for merger.Valid() {
		results = append(results, struct{ k, v string }{string(merger.Key()), string(merger.Value())})
		merger.Next()
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Reverse merge results mismatch.\nGot: %v\nWant:%v", results, expected)
	}
}

func TestReverseMergingIterator_ThreeSources(t *testing.T) {
	src0 := &mockUnsafeIter{data: []entry{
		{"H", "h0", false},
		{"G", "g0", false},
		{"C", "", true},
	}}
	src1 := &mockUnsafeIter{data: []entry{
		{"I", "i1", false},
		{"G", "g1", false},
		{"E", "e1", false},
		{"C", "c1", false},
	}}
	src2 := &mockUnsafeIter{data: []entry{
		{"F", "f2", false},
		{"D", "d2", false},
	}}

	it := NewReverseMergingIterator([]IteratorSource{
		{Iter: src0, Priority: 0},
		{Iter: src1, Priority: 1},
		{Iter: src2, Priority: 2},
	}, []byte("D"), []byte("Z"))

	expected := []struct {
		k, v string
	}{
		{"I", "i1"},
		{"H", "h0"},
		{"G", "g0"},
		{"F", "f2"},
		{"E", "e1"},
		{"D", "d2"},
	}

	var got []struct{ k, v string }
	for it.Valid() {
		got = append(got, struct{ k, v string }{string(it.Key()), string(it.Value())})
		it.Next()
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("reverse merge mismatch got=%v want=%v", got, expected)
	}
}
