package memtable

import (
	"bytes"
	"testing"
)

func newEmptyKeyTestTable(t *testing.T, mode Mode) Table {
	t.Helper()
	mt, err := NewWithCapacityMode(0, mode)
	if err != nil {
		t.Fatalf("NewWithCapacityMode(%s): %v", mode, err)
	}
	return mt
}

func TestMemtableModesConcreteEmptyKeyPointAndIterator(t *testing.T) {
	modes := []Mode{ModeSkiplist, ModeHashSorted, ModeBTree, ModeAppendOnly}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			mt := newEmptyKeyTestTable(t, mode)
			mt.Set([]byte{}, []byte("v-empty"))
			mt.Set([]byte("a"), []byte("va"))

			got, deleted, found := mt.Get([]byte{})
			if !found || deleted || !bytes.Equal(got, []byte("v-empty")) {
				t.Fatalf("Get(empty)=(%q deleted=%v found=%v), want v-empty/false/true", got, deleted, found)
			}

			it := mt.NewIterator(nil, nil)
			defer it.Close()
			if !it.Valid() {
				t.Fatalf("iterator invalid, want empty key first")
			}
			key := it.UnsafeKey()
			if key == nil || len(key) != 0 {
				t.Fatalf("iterator first key=%v len=%d, want non-nil empty", key, len(key))
			}
			if val := it.UnsafeValue(); !bytes.Equal(val, []byte("v-empty")) {
				t.Fatalf("iterator empty value=%q want v-empty", val)
			}
		})
	}
}

func TestMemtableModesConcreteEmptyUpperBound(t *testing.T) {
	modes := []Mode{ModeSkiplist, ModeHashSorted, ModeBTree, ModeAppendOnly}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			mt := newEmptyKeyTestTable(t, mode)
			mt.Set([]byte{}, []byte("v-empty"))
			mt.Set([]byte("a"), []byte("va"))

			it := mt.NewIterator(nil, []byte{})
			defer it.Close()
			if it.Valid() {
				t.Fatalf("forward iterator with empty upper bound valid at key %q, want empty range", it.UnsafeKey())
			}

			rit := mt.NewReverseIterator(nil, []byte{})
			defer rit.Close()
			if rit.Valid() {
				t.Fatalf("reverse iterator with empty upper bound valid at key %q, want empty range", rit.UnsafeKey())
			}
		})
	}
}
