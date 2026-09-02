package treedb

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"
)

func expectedRangeInts(keys []int, start, end *int) []int {
	out := make([]int, 0, len(keys))
	for _, k := range keys {
		if start != nil && k < *start {
			continue
		}
		if end != nil && k >= *end {
			continue
		}
		out = append(out, k)
	}
	return out
}

func reverseInts(in []int) []int {
	out := make([]int, len(in))
	for i := range in {
		out[i] = in[len(in)-1-i]
	}
	return out
}

func assertIntSlicesEqual(t *testing.T, got, want []int, label string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s len mismatch got=%v want=%v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s mismatch at %d got=%v want=%v", label, i, got, want)
		}
	}
}

func keyFromPtr(v *int) []byte {
	if v == nil {
		return nil
	}
	return issue579Key(*v)
}

func formatBound(v *int) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *v)
}

func nonNegativeBoundPtr(v int) *int {
	if v < 0 {
		return nil
	}
	vv := v
	return &vv
}

func collectBackendReverseForDebug(t *testing.T, db *DB, startPtr, endPtr *int, phase string, probe int) []int {
	t.Helper()
	if db.backend == nil {
		return nil
	}
	bit, err := db.backend.ReverseIterator(keyFromPtr(startPtr), keyFromPtr(endPtr))
	if err != nil {
		t.Fatalf("%s random reverse probe=%d start=%s end=%s backend reverse err=%v", phase, probe, formatBound(startPtr), formatBound(endPtr), err)
	}
	return issue579CollectInts(t, bit)
}

func verifyIteratorRangeMatrix(t *testing.T, db *DB, live []int, phase string) {
	t.Helper()

	points := []int{-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 31, 40, 50, 60, 62, 63, 64, 65}
	for _, s := range points {
		for _, e := range points {
			var startPtr *int
			var endPtr *int
			if s >= 0 {
				sv := s
				startPtr = &sv
			}
			if e >= 0 {
				ev := e
				endPtr = &ev
			}
			want := expectedRangeInts(live, startPtr, endPtr)

			it, err := db.Iterator(keyFromPtr(startPtr), keyFromPtr(endPtr))
			if err != nil {
				t.Fatalf("%s forward iterator start=%v end=%v err=%v", phase, startPtr, endPtr, err)
			}
			gotForward := issue579CollectInts(t, it)
			assertIntSlicesEqual(t, gotForward, want, phase+" forward")

			rit, err := db.ReverseIterator(keyFromPtr(startPtr), keyFromPtr(endPtr))
			if err != nil {
				t.Fatalf("%s reverse iterator start=%v end=%v err=%v", phase, startPtr, endPtr, err)
			}
			gotReverse := issue579CollectInts(t, rit)
			assertIntSlicesEqual(t, gotReverse, reverseInts(want), phase+" reverse")
		}
	}
}

func liveKeysFromSet(live map[int]struct{}) []int {
	out := make([]int, 0, len(live))
	for k := range live {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func verifyRandomRangeProbes(t *testing.T, db *DB, live []int, r *rand.Rand, phase string, probes int) {
	t.Helper()
	for i := 0; i < probes; i++ {
		s := r.Intn(75) - 5
		e := r.Intn(75) - 5
		var startPtr *int
		var endPtr *int
		if r.Intn(4) != 0 {
			startPtr = nonNegativeBoundPtr(s)
		}
		if r.Intn(4) != 0 {
			endPtr = nonNegativeBoundPtr(e)
		}
		want := expectedRangeInts(live, startPtr, endPtr)

		it, err := db.Iterator(keyFromPtr(startPtr), keyFromPtr(endPtr))
		if err != nil {
			t.Fatalf("%s probe=%d forward start=%v end=%v err=%v", phase, i, startPtr, endPtr, err)
		}
		gotForward := issue579CollectInts(t, it)
		assertIntSlicesEqual(t, gotForward, want, fmt.Sprintf("%s random forward probe=%d start=%s end=%s", phase, i, formatBound(startPtr), formatBound(endPtr)))

		rit, err := db.ReverseIterator(keyFromPtr(startPtr), keyFromPtr(endPtr))
		if err != nil {
			t.Fatalf("%s probe=%d reverse start=%v end=%v err=%v", phase, i, startPtr, endPtr, err)
		}
		gotReverse := issue579CollectInts(t, rit)
		wantReverse := reverseInts(want)
		backendReverse := []int(nil)
		if len(gotReverse) != len(wantReverse) {
			backendReverse = collectBackendReverseForDebug(t, db, startPtr, endPtr, phase, i)
			t.Fatalf("%s random reverse probe=%d start=%s end=%s len mismatch got=%v want=%v backend=%v", phase, i, formatBound(startPtr), formatBound(endPtr), gotReverse, wantReverse, backendReverse)
		}
		for idx := range wantReverse {
			if gotReverse[idx] != wantReverse[idx] {
				if backendReverse == nil {
					backendReverse = collectBackendReverseForDebug(t, db, startPtr, endPtr, phase, i)
				}
				t.Fatalf("%s random reverse probe=%d start=%s end=%s mismatch at %d got=%v want=%v backend=%v", phase, i, formatBound(startPtr), formatBound(endPtr), idx, gotReverse, wantReverse, backendReverse)
			}
		}
	}
}

func TestIteratorRangeMatrix_ForcePointers(t *testing.T) {
	opts := OptionsFor(ProfileDurable, t.TempDir())
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	live := make([]int, 0, 64)
	for i := 0; i < 64; i++ {
		if err := db.Set(issue579Key(i), []byte{}); err != nil {
			_ = db.Close()
			t.Fatalf("set %d: %v", i, err)
		}
		live = append(live, i)
	}

	deletes := map[int]struct{}{
		6:  {},
		13: {},
		22: {},
		37: {},
		50: {},
		61: {},
	}
	for k := range deletes {
		if err := db.Delete(issue579Key(k)); err != nil {
			_ = db.Close()
			t.Fatalf("delete %d: %v", k, err)
		}
	}

	filtered := make([]int, 0, len(live))
	for _, k := range live {
		if _, ok := deletes[k]; ok {
			continue
		}
		filtered = append(filtered, k)
	}
	live = filtered

	verifyIteratorRangeMatrix(t, db, live, "pre-checkpoint")

	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	verifyIteratorRangeMatrix(t, db, live, "post-checkpoint")

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	verifyIteratorRangeMatrix(t, db, live, "post-reopen")
}

func TestIteratorRandomizedRanges_ForcePointers(t *testing.T) {
	if os.Getenv("TDB_ITERATOR_ATTACK") == "" {
		t.Skip("set TDB_ITERATOR_ATTACK=1 to run iterator attack probes")
	}

	opts := OptionsFor(ProfileDurable, t.TempDir())
	opts.ValueLog.ForcePointers = true

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	r := rand.New(rand.NewSource(1))
	live := make(map[int]struct{}, 64)

	for op := 0; op < 500; op++ {
		k := r.Intn(70)
		if r.Intn(100) < 68 {
			var v []byte
			switch r.Intn(3) {
			case 0:
				v = []byte{}
			case 1:
				v = []byte{byte(k)}
			default:
				v = []byte{byte(k), byte(k >> 1), byte(k >> 2)}
			}
			if err := db.Set(issue579Key(k), v); err != nil {
				t.Fatalf("set op=%d k=%d: %v", op, k, err)
			}
			live[k] = struct{}{}
		} else {
			if err := db.Delete(issue579Key(k)); err != nil {
				t.Fatalf("delete op=%d k=%d: %v", op, k, err)
			}
			delete(live, k)
		}

		if op%25 == 0 {
			verifyRandomRangeProbes(t, db, liveKeysFromSet(live), r, fmt.Sprintf("pre-checkpoint-op=%d", op), 20)
		}
		if op > 0 && op%100 == 0 {
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint op=%d: %v", op, err)
			}
			verifyRandomRangeProbes(t, db, liveKeysFromSet(live), r, fmt.Sprintf("post-checkpoint-op=%d", op), 20)
		}
	}

	verifyRandomRangeProbes(t, db, liveKeysFromSet(live), r, "final", 60)
}
