package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func TestShutdownClosesNativeBeforeDatabaseCleanup(t *testing.T) {
	server := nativewire.NewServer(nativewire.ServerOptions{})
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, release, err := nativewire.NewInProcessClient(ctx, server)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := client.Hello(ctx); err != nil {
		t.Fatal(err)
	}
	cleaned := false
	var state documentServiceShutdownState
	var output bytes.Buffer
	err = state.run(ctx, nil, nil, nil, func() error {
		cleaned = true
		if err := client.Ping(ctx); err == nil {
			t.Error("native client survived into database cleanup")
		}
		return nil
	}, log.New(&output, "", 0), server)
	if err != nil || !cleaned {
		t.Fatalf("shutdown err=%v cleaned=%v", err, cleaned)
	}
	if output.Len() != 0 {
		t.Fatalf("diagnostics-disabled shutdown logged terminal work: %s", output.String())
	}
}

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
	manager := collections.NewCollectionManager(backend)
	service := documentservice.New(manager)
	if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "shutdown_tail", Indexes: []collections.IndexDefinition{{Name: "tag", Field: "tag", ValueType: collections.IndexValueString}}}); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("shutdown_tail")
	if err != nil {
		t.Fatal(err)
	}
	before := service.DiagnosticsSnapshot(nil).Work
	var state documentServiceShutdownState
	var output bytes.Buffer
	logger := log.New(&output, "", 0)
	var cleaned atomic.Bool
	var statsCalls atomic.Int64
	checkedStats := func() map[string]string {
		if cleaned.Load() {
			t.Error("database stats callback used after cleanup")
		}
		statsCalls.Add(1)
		return stats()
	}
	tailCleanup := func() error {
		// Real producer work after the last live sample, while cleanup still
		// owns the DB. No direct access to the internal counters is needed.
		if _, err := col.Insert([]byte("tail"), []byte(`{"tag":"tail"}`)); err != nil {
			return err
		}
		if err := cleanup(); err != nil {
			return err
		}
		cleaned.Store(true)
		return nil
	}
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		service.DiagnosticsHandler(checkedStats).ServeHTTP(w, r)
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
		shutdownDone <- state.run(context.Background(), nil, diagnostics, service, tailCleanup, logger)
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
	if !cleaned.Load() {
		t.Fatal("terminal record preceded database cleanup")
	}
	record := decodeTerminalWorkForTest(t, output.Bytes())
	if record.Work.IndexedJSON.ScalarRows <= before.IndexedJSON.ScalarRows || record.Work.PID != before.PID || record.Work.OriginUnixNano != before.OriginUnixNano || record.Work.OriginKind != before.OriginKind || record.Work.SchemaVersion != before.SchemaVersion {
		t.Fatalf("terminal work lost cleanup tail/origin: before=%+v after=%+v", before, record.Work)
	}
	if record.ShutdownFailures != 0 || statsCalls.Load() != 1 {
		t.Fatalf("failures=%d database callback calls=%d", record.ShutdownFailures, statsCalls.Load())
	}
	// Concurrent signal/deferred paths share one successful shutdown owner.
	var retries sync.WaitGroup
	for range 2 {
		retries.Go(func() {
			if err := state.run(context.Background(), nil, diagnostics, service, func() error { t.Error("duplicate cleanup"); return nil }, logger); err != nil {
				t.Error(err)
			}
		})
	}
	retries.Wait()
	decodeTerminalWorkForTest(t, output.Bytes())
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

func TestShutdownTimeoutLeavesServiceAndDatabaseLiveForRetry(t *testing.T) {
	dir := t.TempDir()
	backend, cleanup, stats, err := treedb.OpenBackendWithCachedLeafLogStats(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	service := documentservice.New(collections.NewCollectionManager(backend))
	started := make(chan struct{})
	release := make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	diagnostics := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		service.DiagnosticsHandler(stats).ServeHTTP(w, r)
	})}
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
	cleanupCalls := 0
	var state documentServiceShutdownState
	var output bytes.Buffer
	logger := log.New(&output, "", 0)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = state.run(timeoutCtx, nil, diagnostics, service, func() error {
		cleanupCalls++
		return cleanup()
	}, logger)
	cancel()
	if err == nil {
		t.Fatal("shutdown unexpectedly drained a blocked diagnostics handler")
	}
	if output.Len() != 0 {
		t.Fatalf("failed drain emitted terminal work: %s", output.String())
	}
	if cleanupCalls != 0 || service.DiagnosticsSnapshot(nil).ServiceClosed {
		t.Fatalf("timeout entered teardown: cleanup=%d closed=%v", cleanupCalls, service.DiagnosticsSnapshot(nil).ServiceClosed)
	}
	close(release)
	<-requestDone
	if err := state.run(context.Background(), nil, diagnostics, service, func() error {
		cleanupCalls++
		return cleanup()
	}, logger); err != nil {
		t.Fatalf("retry shutdown: %v", err)
	}
	if cleanupCalls != 1 || !service.DiagnosticsSnapshot(nil).ServiceClosed {
		t.Fatalf("retry teardown: cleanup=%d closed=%v", cleanupCalls, service.DiagnosticsSnapshot(nil).ServiceClosed)
	}
	if record := decodeTerminalWorkForTest(t, output.Bytes()); record.ShutdownFailures != 1 {
		t.Fatalf("successful retry erased failure history: %+v", record)
	}
}

type terminalWorkRecordForTest struct {
	documentservice.DiagnosticsSnapshot
	Event            string `json:"event"`
	Version          int    `json:"version"`
	CleanupCompleted bool   `json:"cleanup_completed"`
	ShutdownFailures uint64 `json:"shutdown_failures"`
}

func decodeTerminalWorkForTest(t *testing.T, raw []byte) terminalWorkRecordForTest {
	t.Helper()
	var record terminalWorkRecordForTest
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("expected one terminal JSON record: %s: %v", raw, err)
	}
	if record.Event != "treedb_document_service_terminal_work" || record.Version != 1 || !record.CleanupCompleted || record.ContractVersion != documentservice.ContractVersion || record.Work.PID == 0 || record.Work.OriginUnixNano == 0 {
		t.Fatalf("invalid terminal record: %+v", record)
	}
	return record
}

func TestShutdownCleanupFailureHistory(t *testing.T) {
	var state documentServiceShutdownState
	var output bytes.Buffer
	logger := log.New(&output, "", 0)
	diagnostics := &http.Server{}
	injected := errors.New("cleanup failed")
	if err := state.run(context.Background(), nil, diagnostics, nil, func() error { return injected }, logger); !errors.Is(err, injected) {
		t.Fatalf("cleanup failure=%v", err)
	}
	if output.Len() != 0 {
		t.Fatal("failed cleanup emitted a completed record")
	}
	if err := state.run(context.Background(), nil, diagnostics, nil, func() error { return nil }, logger); err != nil {
		t.Fatal(err)
	}
	if record := decodeTerminalWorkForTest(t, output.Bytes()); record.ShutdownFailures != 1 {
		t.Fatal("cleanup retry erased prior failure")
	}
}
