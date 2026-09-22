package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestDebugMemtableModeLabel_KnownModes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mode memtable.Mode
		want string
	}{
		{name: "skiplist", mode: memtable.ModeSkiplist, want: "skiplist"},
		{name: "hash_sorted", mode: memtable.ModeHashSorted, want: "hash_sorted"},
		{name: "btree", mode: memtable.ModeBTree, want: "btree"},
		{name: "append_only", mode: memtable.ModeAppendOnly, want: "append_only"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mt, err := memtable.NewWithCapacityMode(64, tc.mode)
			if err != nil {
				t.Fatalf("new memtable mode %s: %v", tc.mode, err)
			}
			if got := debugMemtableModeLabel(mt); got != tc.want {
				t.Fatalf("debugMemtableModeLabel(%s): got %q want %q", tc.mode, got, tc.want)
			}
		})
	}
}

func TestDebugMemtableModeLabel_Nil(t *testing.T) {
	t.Parallel()

	var mt memtable.Table
	if got := debugMemtableModeLabel(mt); got != "unknown" {
		t.Fatalf("debugMemtableModeLabel(nil): got %q want %q", got, "unknown")
	}
}
