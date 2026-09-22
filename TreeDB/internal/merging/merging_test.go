package merging

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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

type valueErrorUnsafeIter struct {
	*mockUnsafeIter
	err         error
	valueLoaded bool
	entryLoaded bool
	valueCopied bool
}

type seekErrorUnsafeIter struct {
	*mockUnsafeIter
	err    error
	seeked bool
}

type seekAdvanceErrorUnsafeIter struct {
	*mockUnsafeIter
	err    error
	armed  bool
	failed bool
}

func (m *seekAdvanceErrorUnsafeIter) Seek(key []byte) {
	m.mockUnsafeIter.Seek(key)
	m.armed = true
	m.failed = false
}

func (m *seekAdvanceErrorUnsafeIter) Next() {
	m.mockUnsafeIter.Next()
	if m.armed {
		m.failed = true
		m.idx = len(m.data)
	}
}

func (m *seekAdvanceErrorUnsafeIter) Error() error {
	if m.failed {
		return m.err
	}
	return nil
}

func (m *seekErrorUnsafeIter) Seek(key []byte) {
	m.seeked = true
	m.mockUnsafeIter.Seek(key)
	m.idx = len(m.data)
}

func (m *seekErrorUnsafeIter) Error() error {
	if m.seeked {
		return m.err
	}
	return nil
}

func (m *valueErrorUnsafeIter) UnsafeValue() []byte {
	m.valueLoaded = true
	return nil
}

func (m *valueErrorUnsafeIter) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	m.entryLoaded = true
	return nil, page.ValuePtr{}, 0
}

func (m *valueErrorUnsafeIter) Value() []byte { return m.UnsafeValue() }

func (m *valueErrorUnsafeIter) ValueCopy(dst []byte) []byte {
	m.valueCopied = true
	return dst[:0]
}

func (m *valueErrorUnsafeIter) Error() error {
	if m.valueLoaded || m.entryLoaded || m.valueCopied {
		return m.err
	}
	return nil
}

type revisionEntry struct {
	k, v string
	del  bool
	rev  page.EntryRevision
}

type mockRevisionUnsafeIter struct {
	data      []revisionEntry
	idx       int
	seekedKey []byte
}

func (m *mockRevisionUnsafeIter) Next()               { m.idx++ }
func (m *mockRevisionUnsafeIter) Valid() bool         { return m.idx < len(m.data) }
func (m *mockRevisionUnsafeIter) UnsafeKey() []byte   { return []byte(m.data[m.idx].k) }
func (m *mockRevisionUnsafeIter) UnsafeValue() []byte { return []byte(m.data[m.idx].v) }
func (m *mockRevisionUnsafeIter) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return m.UnsafeValue(), page.ValuePtr{}, 0
}
func (m *mockRevisionUnsafeIter) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	return m.UnsafeValue(), page.ValuePtr{}, 0, m.data[m.idx].rev
}
func (m *mockRevisionUnsafeIter) Key() []byte   { return m.UnsafeKey() }
func (m *mockRevisionUnsafeIter) Value() []byte { return m.UnsafeValue() }
func (m *mockRevisionUnsafeIter) KeyCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeKey()...)
}
func (m *mockRevisionUnsafeIter) ValueCopy(dst []byte) []byte {
	return append(dst[:0], m.UnsafeValue()...)
}
func (m *mockRevisionUnsafeIter) Error() error    { return nil }
func (m *mockRevisionUnsafeIter) Close() error    { return nil }
func (m *mockRevisionUnsafeIter) IsDeleted() bool { return m.data[m.idx].del }
func (m *mockRevisionUnsafeIter) Seek(key []byte) {
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
	m.idx = len(m.data)
}
func (m *mockRevisionUnsafeIter) Domain() (start, end []byte) { return nil, nil }

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

func TestTwoWayMergerSurfacesCurrentValueError(t *testing.T) {
	want := errors.New("value load failed")
	broken := &valueErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            want,
	}
	other := &mockUnsafeIter{data: []entry{{k: "B", v: "ok"}}}

	merged := NewTwoWayMerger(broken, other, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key A")
	}
	if string(merged.Key()) != "A" {
		t.Fatalf("first key = %q, want A", merged.Key())
	}
	_ = merged.Value()
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestTwoWayMergerSeekFailsClosedOnNonCurrentSourceError(t *testing.T) {
	want := errors.New("two-way seek failed")
	broken := &seekErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            want,
	}
	healthy := &mockUnsafeIter{data: []entry{{k: "B", v: "ok"}}}
	merged := NewTwoWayMerger(broken, healthy, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("B"))
	if merged.Valid() {
		t.Fatal("merged iterator exposed a partial result after source seek error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestHeapMergingIteratorSeekReportsEverySourceError(t *testing.T) {
	wantFirst := errors.New("first heap seek failed")
	wantSecond := errors.New("second heap seek failed")
	first := &seekErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            wantFirst,
	}
	second := &seekErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "B", v: "bad"}}},
		err:            wantSecond,
	}
	healthy := &mockUnsafeIter{data: []entry{{k: "C", v: "ok"}}}
	merged := NewMergingIterator([]IteratorSource{
		{Iter: first, Priority: 0},
		{Iter: second, Priority: 1},
		{Iter: healthy, Priority: 2},
	}, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("A"))
	if merged.Valid() {
		t.Fatal("heap iterator exposed a partial result after source seek errors")
	}
	if got := merged.Error(); !errors.Is(got, wantFirst) || !errors.Is(got, wantSecond) {
		t.Fatalf("Error() = %v, want both %v and %v", got, wantFirst, wantSecond)
	}
}

func TestTwoWayMergerSeekReportsErrorFromTombstoneAdvance(t *testing.T) {
	want := errors.New("two-way tombstone advance failed")
	broken := &seekAdvanceErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", del: true}}},
		err:            want,
	}
	healthy := &mockUnsafeIter{data: []entry{{k: "B", v: "partial"}}}
	merged := NewTwoWayMerger(broken, healthy, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("A"))
	if merged.Valid() {
		t.Fatal("two-way iterator exposed a partial result after tombstone advance error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestHeapMergingIteratorSeekReportsErrorFromTombstoneAdvance(t *testing.T) {
	want := errors.New("heap tombstone advance failed")
	broken := &seekAdvanceErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", del: true}}},
		err:            want,
	}
	middle := &mockUnsafeIter{data: []entry{{k: "B", v: "partial"}}}
	oldest := &mockUnsafeIter{data: []entry{{k: "C", v: "partial"}}}
	merged := NewMergingIterator([]IteratorSource{
		{Iter: broken, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()

	merged.Seek([]byte("A"))
	if merged.Valid() {
		t.Fatal("heap iterator exposed a partial result after tombstone advance error")
	}
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestTwoWayMergerSurfacesCurrentUnsafeEntryWithRevisionError(t *testing.T) {
	want := errors.New("entry load failed")
	broken := &valueErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            want,
	}
	other := &mockUnsafeIter{data: []entry{{k: "B", v: "ok"}}}

	merged := NewTwoWayMerger(broken, other, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key A")
	}
	revIt, ok := interface{}(merged).(iterator.RevisionUnsafeIterator)
	if !ok {
		t.Fatalf("merged iterator %T does not expose revisions", merged)
	}
	_, _, _, _ = revIt.UnsafeEntryWithRevision()
	if got := merged.Error(); got != want {
		t.Fatalf("Error() = %v, want direct %v", got, want)
	}
}

func TestHeapMergingIteratorSurfacesCurrentValueError(t *testing.T) {
	want := errors.New("heap value load failed")
	broken := &valueErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            want,
	}
	middle := &mockUnsafeIter{data: []entry{{k: "B", v: "ok"}}}
	oldest := &mockUnsafeIter{data: []entry{{k: "C", v: "ok"}}}

	merged := NewMergingIterator([]IteratorSource{
		{Iter: broken, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key A")
	}
	if string(merged.Key()) != "A" {
		t.Fatalf("first key = %q, want A", merged.Key())
	}
	_ = merged.Value()
	if !errors.Is(merged.Error(), want) {
		t.Fatalf("Error() = %v, want %v", merged.Error(), want)
	}
}

func TestHeapMergingIteratorSurfacesCurrentUnsafeEntryWithRevisionError(t *testing.T) {
	want := errors.New("heap entry load failed")
	broken := &valueErrorUnsafeIter{
		mockUnsafeIter: &mockUnsafeIter{data: []entry{{k: "A", v: "bad"}}},
		err:            want,
	}
	middle := &mockUnsafeIter{data: []entry{{k: "B", v: "ok"}}}
	oldest := &mockUnsafeIter{data: []entry{{k: "C", v: "ok"}}}

	merged := NewMergingIterator([]IteratorSource{
		{Iter: broken, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()
	if !merged.Valid() {
		t.Fatal("merged iterator invalid, want first key A")
	}
	revIt, ok := merged.(iterator.RevisionUnsafeIterator)
	if !ok {
		t.Fatalf("merged iterator %T does not expose revisions", merged)
	}
	_, _, _, _ = revIt.UnsafeEntryWithRevision()
	if got := merged.Error(); got != want {
		t.Fatalf("Error() = %v, want direct %v", got, want)
	}
}

func TestTwoWayMergerUnsafeEntryWithRevisionPreservesWinner(t *testing.T) {
	mut := &mockRevisionUnsafeIter{
		data: []revisionEntry{
			{k: "A", v: "new", rev: 11},
			{k: "B", v: "mut", rev: 12},
		},
	}
	disk := &mockRevisionUnsafeIter{
		data: []revisionEntry{
			{k: "A", v: "old", rev: 7},
			{k: "C", v: "disk", rev: 8},
		},
	}

	merged := NewMergingIterator([]IteratorSource{
		{Iter: mut, Priority: 0},
		{Iter: disk, Priority: 1},
	}, nil, nil)
	defer merged.Close()

	revIt, ok := merged.(iterator.RevisionUnsafeIterator)
	if !ok {
		t.Fatalf("merged iterator %T does not expose revisions", merged)
	}
	if !merged.Valid() || string(merged.Key()) != "A" {
		if !merged.Valid() {
			t.Fatal("merged iterator invalid, want first key A")
		}
		t.Fatalf("first key=%q, want A", merged.Key())
	}
	val, _, _, revision := revIt.UnsafeEntryWithRevision()
	if string(val) != "new" || revision != 11 {
		t.Fatalf("UnsafeEntryWithRevision=(%q,%d), want (new,11)", val, revision)
	}

	merged.Next()
	if !merged.Valid() || string(merged.Key()) != "B" {
		if !merged.Valid() {
			t.Fatal("merged iterator invalid, want second key B")
		}
		t.Fatalf("second key=%q, want B", merged.Key())
	}
	val, _, _, revision = revIt.UnsafeEntryWithRevision()
	if string(val) != "mut" || revision != 12 {
		t.Fatalf("second UnsafeEntryWithRevision=(%q,%d), want (mut,12)", val, revision)
	}
}

func TestHeapMergingIteratorUnsafeEntryWithRevisionPreservesWinner(t *testing.T) {
	newest := &mockRevisionUnsafeIter{
		data: []revisionEntry{
			{k: "A", v: "new", rev: 31},
			{k: "D", v: "new-d", rev: 34},
		},
	}
	middle := &mockRevisionUnsafeIter{
		data: []revisionEntry{
			{k: "A", v: "middle", rev: 21},
			{k: "B", v: "middle-b", rev: 22},
		},
	}
	oldest := &mockRevisionUnsafeIter{
		data: []revisionEntry{
			{k: "A", v: "old", rev: 11},
			{k: "C", v: "old-c", rev: 13},
		},
	}

	merged := NewMergingIterator([]IteratorSource{
		{Iter: newest, Priority: 0},
		{Iter: middle, Priority: 1},
		{Iter: oldest, Priority: 2},
	}, nil, nil)
	defer merged.Close()

	revIt, ok := merged.(iterator.RevisionUnsafeIterator)
	if !ok {
		t.Fatalf("merged iterator %T does not expose revisions", merged)
	}
	if !merged.Valid() || string(merged.Key()) != "A" {
		if !merged.Valid() {
			t.Fatal("merged iterator invalid, want first key A")
		}
		t.Fatalf("first key=%q, want A", merged.Key())
	}
	val, _, _, revision := revIt.UnsafeEntryWithRevision()
	if string(val) != "new" || revision != 31 {
		t.Fatalf("UnsafeEntryWithRevision=(%q,%d), want (new,31)", val, revision)
	}

	merged.Next()
	if !merged.Valid() || string(merged.Key()) != "B" {
		if !merged.Valid() {
			t.Fatal("merged iterator invalid, want second key B")
		}
		t.Fatalf("second key=%q, want B", merged.Key())
	}
	val, _, _, revision = revIt.UnsafeEntryWithRevision()
	if string(val) != "middle-b" || revision != 22 {
		t.Fatalf("second UnsafeEntryWithRevision=(%q,%d), want (middle-b,22)", val, revision)
	}
}
