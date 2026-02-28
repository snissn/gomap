package caching

import "testing"

func TestMapRouteUnresolvedSourcePos(t *testing.T) {
	t.Run("identity_without_norm", func(t *testing.T) {
		unresolved := []int{5, 1, 3}
		for i, want := range unresolved {
			got, err := mapRouteUnresolvedSourcePos(unresolved, nil, i)
			if err != nil {
				t.Fatalf("pos=%d unexpected error: %v", i, err)
			}
			if got != want {
				t.Fatalf("pos=%d got=%d want=%d", i, got, want)
			}
		}
	})

	t.Run("uses_normalized_to_source", func(t *testing.T) {
		unresolved := []int{40, 10, 30, 20}
		normToSource := []int{1, 3, 2, 0}
		want := []int{10, 20, 30, 40}
		for i := range want {
			got, err := mapRouteUnresolvedSourcePos(unresolved, normToSource, i)
			if err != nil {
				t.Fatalf("pos=%d unexpected error: %v", i, err)
			}
			if got != want[i] {
				t.Fatalf("pos=%d got=%d want=%d", i, got, want[i])
			}
		}
	})

	t.Run("position_out_of_range", func(t *testing.T) {
		if _, err := mapRouteUnresolvedSourcePos([]int{1, 2}, nil, 2); err == nil {
			t.Fatalf("expected out-of-range error")
		}
	})

	t.Run("normalized_index_out_of_range", func(t *testing.T) {
		unresolved := []int{7, 8}
		normToSource := []int{0, 4}
		if _, err := mapRouteUnresolvedSourcePos(unresolved, normToSource, 1); err == nil {
			t.Fatalf("expected normalized source index error")
		}
	})
}
