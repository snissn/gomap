package caching

import (
	"strconv"
	"testing"
)

func TestDefaultJournalLaneCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		procs int
		want  int
	}{
		{procs: 1, want: 1},
		{procs: 2, want: 1},
		{procs: 3, want: 1},
		{procs: 4, want: 1},
		{procs: 7, want: 1},
		{procs: 8, want: 2},
		{procs: 12, want: 3},
		{procs: 16, want: 6},
		{procs: 20, want: 7},
		{procs: 24, want: 8},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("procs="+strconv.Itoa(tc.procs), func(t *testing.T) {
			t.Parallel()
			if got := defaultJournalLaneCount(tc.procs); got != tc.want {
				t.Fatalf("defaultJournalLaneCount(%d)=%d want %d", tc.procs, got, tc.want)
			}
		})
	}
}
