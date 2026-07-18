package main

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParsePublicProfileFlagAcceptsPublicProfiles(t *testing.T) {
	tests := []struct {
		raw  string
		want treedb.Profile
	}{
		{raw: "", want: treedb.ProfileCommandWALDurable},
		{raw: "command_wal_durable", want: treedb.ProfileCommandWALDurable},
		{raw: "command_wal_relaxed", want: treedb.ProfileCommandWALRelaxed},
		{raw: "no_wal_fast", want: treedb.ProfileNoWALFast},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := parsePublicProfileFlag(tt.raw)
			if err != nil {
				t.Fatalf("parsePublicProfileFlag(%q): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("profile=%q want %q", got, tt.want)
			}
		})
	}
}

func TestParsePublicProfileFlagRejectsDeprecatedProfiles(t *testing.T) {
	for _, raw := range []string{"fast", "wal_on_fast", "durable", "legacy_wal_durable", "legacy_wal_relaxed_fast", "bench", "bench_unsafe", "command-wal-durable"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parsePublicProfileFlag(raw)
			if err == nil {
				t.Fatal("parsePublicProfileFlag succeeded, want error")
			}
			if !strings.Contains(err.Error(), "allowed: "+treedb.ProfileFlagHelp) {
				t.Fatalf("error=%v, want allowed profile guidance", err)
			}
		})
	}
}
