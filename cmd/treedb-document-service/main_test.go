package main

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestParsePublicProfileFlagDocumentService(t *testing.T) {
	profile, err := parsePublicProfileFlag("command_wal_relaxed")
	if err != nil {
		t.Fatalf("parsePublicProfileFlag: %v", err)
	}
	if profile != treedb.ProfileCommandWALRelaxed {
		t.Fatalf("profile=%q", profile)
	}
	if _, err := parsePublicProfileFlag("fast"); err == nil || !strings.Contains(err.Error(), treedb.ProfileFlagHelp) {
		t.Fatalf("deprecated profile err=%v", err)
	}
}
