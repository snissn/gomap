package main

import "testing"

func TestTreeDBUsageGroup(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{name: "treedb-flush-threshold", want: "TreeDB Main Knobs"},
		{name: "treedb-vlog-compression", want: "TreeDB Compression Knobs"},
		{name: "treedb-vlog-dict-train-bytes", want: "TreeDB Advanced Tuning"},
		{name: "treedb-disable-wal", want: "TreeDB Unsafe Knobs"},
	}

	for _, tc := range cases {
		if got := treeDBUsageGroup(tc.name); got != tc.want {
			t.Fatalf("treeDBUsageGroup(%q)=%q want %q", tc.name, got, tc.want)
		}
	}
}

func TestStyleUsageText(t *testing.T) {
	if got := styleUsageText("-flag", false); got != "-flag" {
		t.Fatalf("plain style=%q want %q", got, "-flag")
	}
	if got := styleUsageText("-flag", true); got != "\x1b[1m-flag\x1b[0m" {
		t.Fatalf("bold style=%q", got)
	}
}
