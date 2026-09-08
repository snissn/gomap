package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func main() {
	exitCode := 0
	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()

	addr := flag.String("addr", "127.0.0.1:7120", "HTTP address to listen on")
	nativeAddr := flag.String("native-addr", "", "optional native-wire TCP address using the same document service")
	dataDir := flag.String("dir", "/tmp/treedb-document-service", "TreeDB data directory")
	profile := flag.String("profile", string(treedb.ProfileCommandWALDurable), "TreeDB profile: "+treedb.ProfileFlagHelp)
	pprofAddr := flag.String("pprof", "", "optional net/http/pprof listen address, e.g. 127.0.0.1:6060")
	blockProfileRate := flag.Int("block-profile-rate", 0, "runtime.SetBlockProfileRate value for pprof diagnostics (0=disabled, 1=all blocking events)")
	mutexProfileFraction := flag.Int("mutex-profile-fraction", 0, "runtime.SetMutexProfileFraction value for pprof diagnostics (0=disabled, 1=all mutex contention)")
	constructionDecisionObserver := flag.Bool("diagnostic-construction-decisions", false, "include bounded column_graph construction decision work accounting in optimize responses")
	flag.Parse()

	if *dataDir == "" {
		log.Fatal("Data directory (-dir) is required")
	}
	normalizedProfile, err := parsePublicProfileFlag(*profile)
	if err != nil {
		log.Fatal(err)
	}
	opts := treedb.OptionsFor(normalizedProfile, *dataDir)
	if *blockProfileRate > 0 {
		runtime.SetBlockProfileRate(*blockProfileRate)
	}
	if *mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(*mutexProfileFraction)
	}
	database, cleanup, databaseStats, deferredMaintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)
	if err != nil {
		log.Fatalf("Failed to open TreeDB: %v", err)
	}
	manager := collections.NewCollectionManager(database)
	manager.SetVectorIndexConstructionDecisionObserverEnabled(*constructionDecisionObserver)
	service := documentservice.NewWithDeferredVectorBuildMaintenance(manager, deferredMaintenance)
	appServer := &http.Server{Addr: *addr, Handler: documentservice.NewHandler(service), ReadHeaderTimeout: 5 * time.Second}
	var diagnosticsServer *http.Server
	var nativeServer *nativewire.Server
	nativeErrors := make(chan error, 1)
	var shutdownState documentServiceShutdownState
	shutdown := func() {
		if err := shutdownState.run(nil, appServer, diagnosticsServer, service, cleanup, log.Default(), nativeServer); err != nil {
			log.Printf("TreeDB Document Service shutdown incomplete: %v", err)
			return
		}
	}
	defer shutdown()
	var diagnosticsHandler http.Handler
	if *pprofAddr != "" {
		diagnosticsHandler = service.DiagnosticsHandler(databaseStats)
	}
	if handler, err := optionalPprofHandler(*pprofAddr, diagnosticsHandler); err != nil {
		log.Print(err)
		exitCode = 1
		return
	} else if handler != nil {
		diagnosticsListener, err := net.Listen("tcp", *pprofAddr)
		if err != nil {
			log.Printf("Failed to listen for pprof on %s: %v", *pprofAddr, err)
			exitCode = 1
			return
		}
		diagnosticsServer = &http.Server{Addr: *pprofAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Printf("TreeDB Document Service pprof listening on http://%s/debug/pprof/", *pprofAddr)
			if err := diagnosticsServer.Serve(diagnosticsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Printf("Failed to listen on %s: %v", *addr, err)
		exitCode = 1
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *nativeAddr != "" {
		nativeListener, err := net.Listen("tcp", *nativeAddr)
		if err != nil {
			_ = listener.Close()
			log.Printf("Failed to listen for native wire on %s: %v", *nativeAddr, err)
			exitCode = 1
			return
		}
		nativeServer = nativewire.NewServer(nativewire.ServerOptions{Collections: manager, Backend: database, DocumentService: service})
		go func() {
			if err := nativeServer.Serve(ctx, nativeListener); err != nil && !errors.Is(err, context.Canceled) {
				nativeErrors <- err
				stop()
			}
		}()
		fmt.Printf("TreeDB Document Service native wire listening on %s\n", *nativeAddr)
	}
	go func() {
		<-ctx.Done()
		shutdown()
	}()

	fmt.Printf("TreeDB Document Service listening on http://%s\n", *addr)
	fmt.Printf("TreeDB data directory: %s\n", *dataDir)
	fmt.Printf("TreeDB profile: %s\n", normalizedProfile)
	if err := appServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("Server error: %v", err)
		exitCode = 1
		return
	}
	select {
	case err := <-nativeErrors:
		log.Printf("Native server error: %v", err)
		exitCode = 1
	default:
	}
}

// documentServiceShutdownState is the shared signal/deferred shutdown owner.
// Failure history stays under the same lock as completion; a successful retry
// must not erase an earlier failed drain or cleanup from terminal evidence.
type documentServiceShutdownState struct {
	mu       sync.Mutex
	complete bool
	failures uint64
}

func (s *documentServiceShutdownState) run(ctx context.Context, appServer, diagnosticsServer *http.Server, service *documentservice.Service, cleanup func() error, logger *log.Logger, nativeServers ...*nativewire.Server) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.complete {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	if err := shutdownDocumentService(ctx, appServer, diagnosticsServer, service, cleanup, nativeServers...); err != nil {
		s.failures++
		return err
	}
	s.complete = true
	if diagnosticsServer != nil {
		// The listener and DB are closed. Never call the former DB stats
		// callback here. Work retains its original package-init process origin;
		// its memory sample precedes this record's serialization/logging.
		snapshot := service.DiagnosticsSnapshot(nil)
		record, err := json.Marshal(map[string]any{
			"event": "treedb_document_service_terminal_work", "version": 1,
			"contract_version": snapshot.ContractVersion, "cleanup_completed": true,
			"shutdown_failures": s.failures, "work": snapshot.Work,
		})
		if err != nil {
			return fmt.Errorf("encode terminal work: %w", err)
		}
		logger.Printf("%s", record)
	}
	return nil
}

// shutdownDocumentService preserves callback lifetime: stop request admission,
// drain diagnostics, then release service state and its database.
func shutdownDocumentService(ctx context.Context, appServer, diagnosticsServer *http.Server, service *documentservice.Service, cleanup func() error, nativeServers ...*nativewire.Server) error {
	if appServer != nil {
		if err := appServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	for _, server := range nativeServers {
		if err := server.Close(); err != nil {
			return err
		}
	}
	if diagnosticsServer != nil {
		if err := diagnosticsServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	var err error
	if service != nil {
		err = errors.Join(err, service.Close())
	}
	if cleanup != nil {
		err = errors.Join(err, cleanup())
	}
	return err
}

func optionalPprofHandler(addr string, diagnostics http.Handler) (http.Handler, error) {
	if addr == "" {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid pprof listen address %q: %w", addr, err)
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("pprof diagnostics address %q must be loopback", addr)
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if diagnostics != nil && r.URL.Path == "/debug/treedb/stats" {
			diagnostics.ServeHTTP(w, r)
			return
		}
		http.DefaultServeMux.ServeHTTP(w, r)
	}), nil
}

func parsePublicProfileFlag(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, nil
}
