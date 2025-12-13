package merging

import (
	"bytes"
	"reflect"
	"testing"
	"treedb/internal/iterator"
)

type mockUnsafeIter struct {
	data []entry
	idx  int
	seekedKey []byte
}

type entry struct {
	k, v string
	del  bool
}

func (m *mockUnsafeIter) Next()                 { m.idx++ }
func (m *mockUnsafeIter) Valid() bool           { return m.idx < len(m.data) }
func (m *mockUnsafeIter) UnsafeKey() []byte     { return []byte(m.data[m.idx].k) }
func (m *mockUnsafeIter) UnsafeValue() []byte   { return []byte(m.data[m.idx].v) }
func (m *mockUnsafeIter) Close() error          { return nil }
func (m *mockUnsafeIter) IsDeleted() bool       { return m.data[m.idx].del }
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
