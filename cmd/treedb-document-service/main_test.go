package main

import (
	"net/http"
	"net/http/httptest"
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

func TestOptionalPprofHandler(t *testing.T) {
	if handler := optionalPprofHandler(""); handler != nil {
		t.Fatal("pprof handler enabled by default")
	}

	server := httptest.NewServer(optionalPprofHandler("127.0.0.1:6060"))
	defer server.Close()
	response, err := http.Get(server.URL + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status=%d want %d", response.StatusCode, http.StatusOK)
	}
}
