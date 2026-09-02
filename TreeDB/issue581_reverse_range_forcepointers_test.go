package treedb

import "testing"

func TestIssue581_Repro_ReverseRangeForcePointers(t *testing.T) {
	opts := OptionsFor(ProfileDurable, t.TempDir())
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	for i := 0; i < 10; i++ {
		if i == 6 {
			continue
		}
		if err := db.Set(issue579Key(i), []byte{}); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	bounded, err := db.ReverseIterator(issue579Key(4), issue579Key(6))
	if err != nil {
		t.Fatalf("reverse [4,6): %v", err)
	}
	gotBounded := issue579CollectInts(t, bounded)
	wantBounded := []int{5, 4}
	if len(gotBounded) != len(wantBounded) {
		t.Fatalf("reverse [4,6) len mismatch got=%v want=%v", gotBounded, wantBounded)
	}
	for i := range wantBounded {
		if gotBounded[i] != wantBounded[i] {
			t.Fatalf("reverse [4,6) mismatch got=%v want=%v", gotBounded, wantBounded)
		}
	}

	lowerBounded, err := db.ReverseIterator(issue579Key(6), nil)
	if err != nil {
		t.Fatalf("reverse [6,nil): %v", err)
	}
	gotLowerBounded := issue579CollectInts(t, lowerBounded)
	wantLowerBounded := []int{9, 8, 7}
	if len(gotLowerBounded) != len(wantLowerBounded) {
		t.Fatalf("reverse [6,nil) len mismatch got=%v want=%v", gotLowerBounded, wantLowerBounded)
	}
	for i := range wantLowerBounded {
		if gotLowerBounded[i] != wantLowerBounded[i] {
			t.Fatalf("reverse [6,nil) mismatch got=%v want=%v", gotLowerBounded, wantLowerBounded)
		}
	}
}
