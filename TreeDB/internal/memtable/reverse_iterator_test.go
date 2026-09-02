package memtable

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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

func TestMemtableReverseIterator_Seek_ModeMatrix(t *testing.T) {
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

			assertSeekKey := func(it iterator.UnsafeIterator, seekKey []byte, wantKey string, wantValid bool) {
				t.Helper()
				it.Seek(seekKey)
				if it.Valid() != wantValid {
					t.Fatalf("Seek(%q) valid=%t want %t", string(seekKey), it.Valid(), wantValid)
				}
				if !wantValid {
					return
				}
				if got := string(it.Key()); got != wantKey {
					t.Fatalf("Seek(%q) key=%q want %q", string(seekKey), got, wantKey)
				}
			}

			// Unbounded reverse seek should land on <= target.
			it := mt.NewReverseIterator(nil, nil)
			assertSeekKey(it, []byte("B"), "A", true) // predecessor
			assertSeekKey(it, []byte("C"), "C", true) // exact
			assertSeekKey(it, []byte("Z"), "G", true) // beyond max
			_ = it.Close()

			// Bounded reverse seek should respect end and never surface >= end keys.
			it = mt.NewReverseIterator(nil, []byte("F")) // includes {A,C,E}
			assertSeekKey(it, nil, "E", true)            // Seek(nil) -> last < end
			assertSeekKey(it, []byte("F"), "E", true)    // Seek(end) -> last < end
			assertSeekKey(it, []byte("Z"), "E", true)    // Seek(>end) -> clamp to end
			assertSeekKey(it, []byte("D"), "C", true)    // predecessor within bound
			_ = it.Close()

			// [B,F) includes {C,E}.
			it = mt.NewReverseIterator([]byte("B"), []byte("F"))
			assertSeekKey(it, nil, "E", true)
			assertSeekKey(it, []byte("D"), "C", true)
			assertSeekKey(it, []byte("B"), "", false) // no key <= start within [start,end)
			_ = it.Close()
		})
	}
}
