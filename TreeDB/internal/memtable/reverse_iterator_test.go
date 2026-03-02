package memtable

import (
	"reflect"
	"testing"
)

func TestMemtableReverseIterator_ModeMatrix(t *testing.T) {
	modes := []Mode{ModeSkiplist, ModeHashSorted, ModeBTree, ModeAppendOnly}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			mt, err := NewWithCapacityMode(0, mode)
			if err != nil {
				t.Fatalf("NewWithCapacityMode(%s): %v", mode.String(), err)
			}

			// Insert out of order to exercise append-only unordered mode.
			mt.Set([]byte("C"), []byte("vC"))
			mt.Set([]byte("A"), []byte("vA"))
			mt.Set([]byte("G"), []byte("vG"))
			mt.Set([]byte("E"), []byte("vE"))

			it := mt.NewReverseIterator(nil, nil)
			var got []string
			for it.Valid() {
				got = append(got, string(it.Key()))
				it.Next()
			}
			_ = it.Close()

			want := []string{"G", "E", "C", "A"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("reverse keys: got=%v want=%v", got, want)
			}

			it = mt.NewReverseIterator([]byte("B"), []byte("F"))
			got = got[:0]
			for it.Valid() {
				got = append(got, string(it.Key()))
				it.Next()
			}
			_ = it.Close()

			want = []string{"E", "C"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("reverse range keys: got=%v want=%v", got, want)
			}
		})
	}
}
