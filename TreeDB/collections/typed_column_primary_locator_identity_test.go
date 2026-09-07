package collections

import (
	"slices"
	"testing"
)

func TestTypedColumnPrimaryLocatorIdentity(t *testing.T) {
	for _, n := range []int{0, 1, 128, 1024} {
		ids := make([]int64, n)
		for i := range ids {
			ids[i] = int64(i)
		}
		locator, err := typedColumnAdapterRowsByPrimaryID(ids)
		if err != nil || locator != nil {
			t.Fatalf("rows%d locator=%v err=%v, want implicit identity", n, locator, err)
		}
		if !collectionsRaceEnabled {
			if a := testing.AllocsPerRun(100, func() {
				_, err := typedColumnAdapterRowsByPrimaryID(ids)
				if err != nil {
					panic(err)
				}
			}); a != 0 {
				t.Fatalf("rows%d allocations%g", n, a)
			}
		}
	}
}

func TestTypedColumnPrimaryLocatorPermutationAndErrors(t *testing.T) {
	ids := []int64{2, 0, 3, 1}
	locator, err := typedColumnAdapterRowsByPrimaryID(ids)
	if err != nil || !slices.Equal(locator, []int{1, 3, 0, 2}) {
		t.Fatalf("locator=%v err=%v", locator, err)
	}
	if !slices.Equal(ids, []int64{2, 0, 3, 1}) {
		t.Fatal("mutated primary IDs")
	}
	for _, tc := range []struct {
		ids  []int64
		want string
	}{
		{[]int64{0, 1, 1}, "collections: typed-column reconstruction duplicate primary_id=1"},
		{[]int64{0, 1, -1}, "collections: typed-column reconstruction primary_id=-1 outside rows=3"},
		{[]int64{0, 1, 3}, "collections: typed-column reconstruction primary_id=3 outside rows=3"},
	} {
		if _, err := typedColumnAdapterRowsByPrimaryID(tc.ids); err == nil || err.Error() != tc.want {
			t.Fatalf("ids%v err=%v want%q", tc.ids, err, tc.want)
		}
	}
}

func BenchmarkTypedColumnPrimaryLocatorPermutation(b *testing.B) {
	for _, late := range []bool{false, true} {
		name := "early"
		if late {
			name = "late"
		}
		b.Run(name, func(b *testing.B) {
			ids := make([]int64, 1024)
			for i := range ids {
				ids[i] = int64(i)
			}
			at := 0
			if late {
				at = len(ids) - 2
			}
			ids[at], ids[at+1] = ids[at+1], ids[at]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := typedColumnAdapterRowsByPrimaryID(ids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
