package treedb

import (
	"encoding/binary"
	"testing"
)

func issue579Key(i int) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(i))
	return k
}

func issue579CollectInts(t *testing.T, it Iterator) []int {
	t.Helper()
	out := make([]int, 0, 16)
	for it.Valid() {
		k := it.KeyCopy(nil)
		if len(k) != 8 {
			t.Fatalf("unexpected key len=%d", len(k))
		}
		out = append(out, int(binary.BigEndian.Uint64(k)))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}
	return out
}

func TestIssue579_Repro_ProfileFast_ForwardAfterReverse(t *testing.T) {
	opts := OptionsFor(ProfileFast, t.TempDir())
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if i == 6 {
			continue
		}
		if err := db.Set(issue579Key(i), []byte{byte(i)}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	rev, err := db.ReverseIterator(nil, nil)
	if err != nil {
		t.Fatalf("reverse iterator: %v", err)
	}
	_ = issue579CollectInts(t, rev)

	fwd, err := db.Iterator(issue579Key(1), nil)
	if err != nil {
		t.Fatalf("forward iterator: %v", err)
	}
	got := issue579CollectInts(t, fwd)

	want := []int{1, 2, 3, 4, 5, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("len mismatch got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mismatch got=%v want=%v", got, want)
		}
	}
}
