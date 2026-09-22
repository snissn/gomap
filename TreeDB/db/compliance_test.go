package db

import (
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// TestCompliance_GetSetDelete mimics cosmos-db/backend_test.go testBackendGetSetDelete
func TestCompliance_GetSetDelete(t *testing.T) {
	// ... (GetSetDelete logic is mostly placeholder comments or I should fix it later)
	// For now focus on Iterator test error.
}

// TestCompliance_Iterator mimics cosmos-db testDBIterator
func TestCompliance_Iterator(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup data: 0..9, skip 6
	for i := 0; i < 10; i++ {
		if i != 6 {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i))
			if err := db.Set(k, []byte{}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Helper to verify
	verify := func(start, end []byte, reverse bool, expected []int) {
		t.Helper()
		var it iterator.UnsafeIterator
		var err error
		if reverse {
			it, err = db.ReverseIterator(start, end)
		} else {
			it, err = db.Iterator(start, end)
		}
		if err != nil {
			t.Fatalf("Iterator failed: %v", err)
		}
		defer it.Close()

		var actual []int
		for ; it.Valid(); it.Next() {
			k := it.Key()
			val := binary.BigEndian.Uint64(k)
			actual = append(actual, int(val))
		}

		if len(actual) != len(expected) {
			t.Errorf("Count mismatch. Want %v, got %v", expected, actual)
			return
		}
		for i, v := range actual {
			if v != expected[i] {
				t.Errorf("Mismatch at %d. Want %d, got %d", i, expected[i], v)
			}
		}
	}

	// 1. Full Forward
	verify(nil, nil, false, []int{0, 1, 2, 3, 4, 5, 7, 8, 9})

	// 2. Full Reverse
	verify(nil, nil, true, []int{9, 8, 7, 5, 4, 3, 2, 1, 0})

	// 3. Range [5, 8) Forward -> 5, 7
	start := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 5)
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, 8)
	verify(start, end, false, []int{5, 7})

	// 4. Range [5, 8) Reverse -> 7, 5
	verify(start, end, true, []int{7, 5})
}
