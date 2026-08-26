package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
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
	if handler, err := optionalPprofHandler("", http.NotFoundHandler()); err != nil || handler != nil {
		t.Fatal("pprof handler enabled by default")
	}

	handler, err := optionalPprofHandler("127.0.0.1:6060", nil)
	if err != nil {
		t.Fatalf("optionalPprofHandler: %v", err)
	}
	server := httptest.NewServer(handler)
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

func TestOptionalPprofHandlerRejectsNonLoopbackDiagnostics(t *testing.T) {
	if _, err := optionalPprofHandler("0.0.0.0:6060", http.NotFoundHandler()); err == nil {
		t.Fatal("non-loopback diagnostics listener was accepted")
	}
}

func TestShutdownDrainsDiagnosticsBeforeDatabaseCleanup(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup, stats, err := treedb.OpenBackendWithCachedLeafLogStats(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	service := documentservice.New(collections.NewCollectionManager(backend))
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		service.DiagnosticsHandler(stats).ServeHTTP(w, r)
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	diagnostics := &http.Server{Handler: handler}
	go func() { _ = diagnostics.Serve(listener) }()
	requestDone := make(chan struct{})
	go func() {
		response, err := http.Get("http://" + listener.Addr().String() + "/debug/treedb/stats")
		if err == nil {
			_ = response.Body.Close()
		}
		close(requestDone)
	}()
	<-started
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- shutdownDocumentService(context.Background(), nil, diagnostics, service, cleanup)
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown entered cleanup before diagnostics drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	<-requestDone
	reopened, reopenedCleanup, _, err := treedb.OpenBackendWithCachedLeafLogStats(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatalf("reopen after shutdown: %v", err)
	}
	if reopened == nil {
		t.Fatal("reopen returned nil backend")
	}
	if err := reopenedCleanup(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}
}
