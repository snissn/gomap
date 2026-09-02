package merging

import (
	"bytes"
	"errors"
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
	m.idx = -1
	for i, e := range m.data {
		if bytes.Compare([]byte(e.k), key) <= 0 {
			m.idx = i
			continue
		}
		break
	}
}

func (m *mockUnsafeReverseIter) Domain() (start, end []byte) { return nil, nil }

type valueErrorUnsafeReverseIter struct {
	*mockUnsafeReverseIter
	err         error
	valueLoaded bool
	entryLoaded bool
	valueCopied bool
}

type seekErrorUnsafeReverseIter struct {
	*mockUnsafeReverseIter
	err    error
	seeked bool
}

type seekAdvanceErrorUnsafeReverseIter struct {
	*mockUnsafeReverseIter
	err    error
	armed  bool
	failed bool
}

func (m *seekAdvanceErrorUnsafeReverseIter) Seek(key []byte) {
	m.mockUnsafeReverseIter.Seek(key)
	m.armed = true
	m.failed = false
}

func (m *seekAdvanceErrorUnsafeReverseIter) Next() {
	m.mockUnsafeReverseIter.Next()
	if m.armed {
		m.failed = true
		m.idx = -1
	}
}

func (m *seekAdvanceErrorUnsafeReverseIter) Error() error {
	if m.failed {
		return m.err
	}
	return nil
}

func (m *seekErrorUnsafeReverseIter) Seek(key []byte) {
	m.seeked = true
	m.mockUnsafeReverseIter.Seek(key)
	m.idx = -1
}

func (m *seekErrorUnsafeReverseIter) Error() error {
	if m.seeked {
		return m.err
	}
	return nil
}

func (m *valueErrorUnsafeReverseIter) UnsafeValue() []byte {
	m.valueLoaded = true
	return nil
}

func (m *valueErrorUnsafeReverseIter) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	m.entryLoaded = true
	return nil, page.ValuePtr{}, 0
}

func (m *valueErrorUnsafeReverseIter) Value() []byte { return m.UnsafeValue() }

func (m *valueErrorUnsafeReverseIter) ValueCopy(dst []byte) []byte {
	m.valueCopied = true
	return dst[:0]
}

func (m *valueErrorUnsafeReverseIter) Error() error {
	if m.valueLoaded || m.entryLoaded || m.valueCopied {
		return m.err
	}
	return nil
}

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

func TestReverseTwoWayMergerSurfacesCurrentValueError(t *testing.T) {
	want := errors.New("reverse value load failed")
	broken := &valueErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{
			data: []entry{{k: "Z", v: "bad"}},
			idx:  0,
		},
		err: want,
	}
	other := &mockUnsafeReverseIter{
		data: []entry{{k: "A", v: "ok"}},
		idx:  0,
	}

	merged := NewReverseTwoWayMerger(broken, other, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key Z")
	}
	if string(merged.Key()) != "Z" {
		t.Fatalf("first key = %q, want Z", merged.Key())
	}
	_ = merged.Value()
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestReverseTwoWayMergerSeekFailsClosedOnNonCurrentSourceError(t *testing.T) {
	want := errors.New("reverse two-way seek failed")
	broken := &seekErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{
			data: []entry{{k: "Z", v: "bad"}},
			idx:  0,
		},
		err: want,
	}
	healthy := &mockUnsafeReverseIter{data: []entry{{k: "B", v: "ok"}}, idx: 0}
	merged := NewReverseTwoWayMerger(broken, healthy, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("Z"))
	if merged.Valid() {
		t.Fatal("reverse merged iterator exposed a partial result after source seek error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestReverseHeapMergingIteratorSeekReportsEverySourceError(t *testing.T) {
	wantFirst := errors.New("first reverse heap seek failed")
	wantSecond := errors.New("second reverse heap seek failed")
	first := &seekErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{data: []entry{{k: "Z", v: "bad"}}, idx: 0},
		err:                   wantFirst,
	}
	second := &seekErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{data: []entry{{k: "Y", v: "bad"}}, idx: 0},
		err:                   wantSecond,
	}
	healthy := &mockUnsafeReverseIter{data: []entry{{k: "X", v: "ok"}}, idx: 0}
	merged := NewReverseMergingIterator([]IteratorSource{
		{Iter: first, Priority: 0},
		{Iter: second, Priority: 1},
		{Iter: healthy, Priority: 2},
	}, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("Z"))
	if merged.Valid() {
		t.Fatal("reverse heap iterator exposed a partial result after source seek errors")
	}
	if got := merged.Error(); !errors.Is(got, wantFirst) || !errors.Is(got, wantSecond) {
		t.Fatalf("Error() = %v, want both %v and %v", got, wantFirst, wantSecond)
	}
}

func TestReverseTwoWayMergerSeekReportsErrorFromTombstoneAdvance(t *testing.T) {
	want := errors.New("reverse two-way tombstone advance failed")
	broken := &seekAdvanceErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{data: []entry{{k: "Z", del: true}}, idx: 0},
		err:                   want,
	}
	healthy := &mockUnsafeReverseIter{data: []entry{{k: "Y", v: "partial"}}, idx: 0}
	merged := NewReverseTwoWayMerger(broken, healthy, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("Z"))
	if merged.Valid() {
		t.Fatal("reverse two-way iterator exposed a partial result after tombstone advance error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestReverseHeapMergingIteratorSeekReportsErrorFromTombstoneAdvance(t *testing.T) {
	want := errors.New("reverse heap tombstone advance failed")
	broken := &seekAdvanceErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{data: []entry{{k: "Z", del: true}}, idx: 0},
		err:                   want,
	}
	middle := &mockUnsafeReverseIter{data: []entry{{k: "Y", v: "partial"}}, idx: 0}
	oldest := &mockUnsafeReverseIter{data: []entry{{k: "X", v: "partial"}}, idx: 0}
	merged := NewReverseMergingIterator([]IteratorSource{
		{Iter: broken, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("Z"))
	if merged.Valid() {
		t.Fatal("reverse heap iterator exposed a partial result after tombstone advance error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestReverseHeapMergingIteratorSurfacesCurrentValueError(t *testing.T) {
	want := errors.New("reverse heap value load failed")
	broken := &valueErrorUnsafeReverseIter{
		mockUnsafeReverseIter: &mockUnsafeReverseIter{
			data: []entry{{k: "Z", v: "bad"}},
			idx:  0,
		},
		err: want,
	}
	middle := &mockUnsafeReverseIter{data: []entry{{k: "B", v: "ok"}}, idx: 0}
	oldest := &mockUnsafeReverseIter{data: []entry{{k: "A", v: "ok"}}, idx: 0}

	merged := NewReverseMergingIterator([]IteratorSource{
		{Iter: broken, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key Z")
	}
	if string(merged.Key()) != "Z" {
		t.Fatalf("first key = %q, want Z", merged.Key())
	}
	_ = merged.Value()
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
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
