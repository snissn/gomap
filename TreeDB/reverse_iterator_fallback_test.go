package treedb

import (
	"bytes"
	"errors"
	"testing"
)

type testIterator struct {
	keys   [][]byte
	vals   [][]byte
	idx    int
	err    error
	closed bool
}

func newTestIterator(keys ...[]byte) *testIterator {
	vals := make([][]byte, len(keys))
	for i := range keys {
		vals[i] = []byte("v")
	}
	return &testIterator{keys: keys, vals: vals}
}

func (it *testIterator) Valid() bool {
	return !it.closed && it.err == nil && it.idx >= 0 && it.idx < len(it.keys)
}

func (it *testIterator) Next() {
	if !it.Valid() {
		panic("iterator is invalid")
	}
	it.idx++
}

func (it *testIterator) Key() []byte {
	if !it.Valid() {
		panic("iterator is invalid")
	}
	return it.keys[it.idx]
}

func (it *testIterator) Value() []byte {
	if !it.Valid() {
		panic("iterator is invalid")
	}
	return it.vals[it.idx]
}

func (it *testIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		panic("iterator is invalid")
	}
	dst = append(dst[:0], it.keys[it.idx]...)
	return dst
}

func (it *testIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() {
		panic("iterator is invalid")
	}
	dst = append(dst[:0], it.vals[it.idx]...)
	return dst
}

func (it *testIterator) Close() error {
	it.closed = true
	return nil
}

func (it *testIterator) Error() error {
	return it.err
}

func prefixEndTestBytes(p []byte) []byte {
	out := append([]byte(nil), p...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xFF {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func collectKeys(it Iterator) ([][]byte, error) {
	defer it.Close()
	out := make([][]byte, 0, 8)
	for ; it.Valid(); it.Next() {
		out = append(out, append([]byte(nil), it.Key()...))
	}
	return out, it.Error()
}

func TestMaybeRecoverBoundedReverseIterator_RecoversEmptyBoundedPath(t *testing.T) {
	start := []byte("s/k:distribution/")
	end := prefixEndTestBytes(start)

	primary := newTestIterator()
	calls := 0
	openReverse := func(s, e []byte) (Iterator, error) {
		calls++
		if s != nil {
			// Broken bounded reverse path: returns empty without error.
			return primary, nil
		}
		// Unbounded reverse path contains an out-of-order key below start first,
		// then keys in the requested distribution range.
		return newTestIterator(
			[]byte("s/k:consensus/mstorage_version"),
			[]byte("s/k:distribution/\x73\x00\x00\x00\x00\x00\x98\x96\x80\x00\x00\x03\xc4"),
			[]byte("s/k:distribution/\x73\x00\x00\x00\x00\x00\x98\x96\x80\x00\x00\x03\xc3"),
		), nil
	}

	got, err := maybeRecoverBoundedReverseIterator(start, end, primary, openReverse)
	if err != nil {
		t.Fatalf("recover iterator err: %v", err)
	}
	keys, err := collectKeys(got)
	if err != nil {
		t.Fatalf("collect keys err: %v", err)
	}
	if calls < 1 {
		t.Fatalf("expected reverse opener to be called")
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 recovered keys, got %d", len(keys))
	}
	if !bytes.HasPrefix(keys[0], start) || !bytes.HasPrefix(keys[1], start) {
		t.Fatalf("expected recovered keys to stay in range prefix %q, got first=%X last=%X", start, keys[0], keys[1])
	}
	if bytes.Compare(keys[0], keys[1]) < 0 {
		t.Fatalf("expected descending reverse order, got first=%X last=%X", keys[0], keys[1])
	}
}

func TestMaybeRecoverBoundedReverseIterator_SkipsWhenPrimaryValid(t *testing.T) {
	start := []byte("aa/")
	end := []byte("ab")
	primary := newTestIterator([]byte("aa/9"))
	calls := 0

	got, err := maybeRecoverBoundedReverseIterator(start, end, primary, func(_, _ []byte) (Iterator, error) {
		calls++
		return nil, errors.New("unexpected")
	})
	if err != nil {
		t.Fatalf("recover err: %v", err)
	}
	if !got.Valid() {
		t.Fatalf("expected primary iterator to remain valid")
	}
	if calls != 0 {
		t.Fatalf("expected no fallback calls, got %d", calls)
	}
}

func TestMaybeRecoverBoundedReverseIterator_SkipsWhenUnbounded(t *testing.T) {
	primary := newTestIterator()
	calls := 0

	_, err := maybeRecoverBoundedReverseIterator(nil, []byte("z"), primary, func(_, _ []byte) (Iterator, error) {
		calls++
		return newTestIterator([]byte("a")), nil
	})
	if err != nil {
		t.Fatalf("recover err: %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected no fallback calls for unbounded range, got %d", calls)
	}
}
