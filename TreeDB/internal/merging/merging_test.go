package merging

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type mockUnsafeIter struct {
	data       []entry
	idx        int
	seekedKey  []byte
	err        error
	closeErr   error
	closeCalls int
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
func (m *mockUnsafeIter) Error() error    { return m.err }
func (m *mockUnsafeIter) Close() error    { m.closeCalls++; return m.closeErr }
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

func TestMergingIteratorNWay_DedupTombstoneAndBounds(t *testing.T) {
	src0 := &mockUnsafeIter{data: []entry{
		{"A", "", true}, // tombstone should hide A from older sources
		{"C", "c-new", false},
	}}
	src1 := &mockUnsafeIter{data: []entry{
		{"A", "a-old", false},
		{"B", "b-mid", false},
		{"D", "d-mid", false},
	}}
	src2 := &mockUnsafeIter{data: []entry{
		{"B", "b-old", false},
		{"E", "e-old", false},
	}}

	it := NewMergingIterator([]IteratorSource{
		{Iter: src0, Priority: 0},
		{Iter: src1, Priority: 1},
		{Iter: src2, Priority: 2},
	}, []byte("B"), []byte("E"))

	got := make([][2]string, 0)
	for it.Valid() {
		key := string(it.KeyCopy(nil))
		val := string(it.ValueCopy(nil))
		got = append(got, [2]string{key, val})
		it.Next()
	}

	want := [][2]string{
		{"B", "b-mid"},
		{"C", "c-new"},
		{"D", "d-mid"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged output mismatch\n got=%v\nwant=%v", got, want)
	}

	start, end := it.Domain()
	if !bytes.Equal(start, []byte("B")) || !bytes.Equal(end, []byte("E")) {
		t.Fatalf("Domain()=%q,%q want %q,%q", start, end, []byte("B"), []byte("E"))
	}
}

func TestMergingIteratorCloseReturnsFirstError(t *testing.T) {
	err1 := errors.New("close-1")
	err2 := errors.New("close-2")
	src0 := &mockUnsafeIter{data: []entry{{"A", "a", false}}, closeErr: err1}
	src1 := &mockUnsafeIter{data: []entry{{"B", "b", false}}, closeErr: err2}
	src2 := &mockUnsafeIter{data: []entry{{"C", "c", false}}}

	it := NewMergingIterator([]IteratorSource{
		{Iter: src0, Priority: 0},
		{Iter: src1, Priority: 1},
		{Iter: src2, Priority: 2},
	}, nil, nil)
	if !it.Valid() {
		t.Fatalf("iterator should be valid initially")
	}

	err := it.Close()
	if !errors.Is(err, err1) {
		t.Fatalf("Close() err=%v, want first close error %v", err, err1)
	}
	if src0.closeCalls == 0 || src1.closeCalls == 0 || src2.closeCalls == 0 {
		t.Fatalf("expected all sources to be closed: calls=%d,%d,%d", src0.closeCalls, src1.closeCalls, src2.closeCalls)
	}
}
