package main

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParseTraceReplayProfileAcceptsBenchmarkProfiles(t *testing.T) {
	tests := []struct {
		raw  string
		want treedb.Profile
	}{
		{raw: "", want: treedb.ProfileCommandWALRelaxed},
		{raw: "command_wal_durable", want: treedb.ProfileCommandWALDurable},
		{raw: "command_wal_relaxed", want: treedb.ProfileCommandWALRelaxed},
		{raw: "no_wal_fast", want: treedb.ProfileNoWALFast},
		{raw: "bench_unsafe", want: treedb.ProfileBenchUnsafe},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := parseTraceReplayProfile(tt.raw)
			if err != nil {
				t.Fatalf("parseTraceReplayProfile(%q): %v", tt.raw, err)
			}
			if got != tt.want {
				t.Fatalf("profile=%q want %q", got, tt.want)
			}
		})
	}
}

func TestParseTraceReplayProfileRejectsDeprecatedProfiles(t *testing.T) {
	for _, raw := range []string{"fast", "wal_on_fast", "durable", "legacy_wal_durable", "legacy_wal_relaxed_fast", "bench", "command-wal-durable"} {
		t.Run(raw, func(t *testing.T) {
			_, err := parseTraceReplayProfile(raw)
			if err == nil {
				t.Fatal("parseTraceReplayProfile succeeded, want error")
			}
			if !strings.Contains(err.Error(), treedb.BenchmarkProfileFlagHelp) {
				t.Fatalf("error=%v, want allowed profile guidance", err)
			}
		})
	}
}
