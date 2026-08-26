package main

import (
	"context"
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
)

func main() {
	exitCode := 0
	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()

	addr := flag.String("addr", "127.0.0.1:7120", "HTTP address to listen on")
	dataDir := flag.String("dir", "/tmp/treedb-document-service", "TreeDB data directory")
	profile := flag.String("profile", string(treedb.ProfileCommandWALDurable), "TreeDB profile: "+treedb.ProfileFlagHelp)
	pprofAddr := flag.String("pprof", "", "optional net/http/pprof listen address, e.g. 127.0.0.1:6060")
	blockProfileRate := flag.Int("block-profile-rate", 0, "runtime.SetBlockProfileRate value for pprof diagnostics (0=disabled, 1=all blocking events)")
	mutexProfileFraction := flag.Int("mutex-profile-fraction", 0, "runtime.SetMutexProfileFraction value for pprof diagnostics (0=disabled, 1=all mutex contention)")
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
	database, cleanup, databaseStats, err := treedb.OpenBackendWithCachedLeafLogStats(opts)
	if err != nil {
		log.Fatalf("Failed to open TreeDB: %v", err)
	}
	manager := collections.NewCollectionManager(database)
	service := documentservice.New(manager)
	appServer := &http.Server{Addr: *addr, Handler: documentservice.NewHandler(service), ReadHeaderTimeout: 5 * time.Second}
	var diagnosticsServer *http.Server
	var shutdownMu sync.Mutex
	shutdownComplete := false
	shutdown := func() {
		shutdownMu.Lock()
		defer shutdownMu.Unlock()
		if shutdownComplete {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdownDocumentService(ctx, appServer, diagnosticsServer, service, cleanup); err != nil {
			log.Printf("TreeDB Document Service shutdown incomplete: %v", err)
			return
		}
		shutdownComplete = true
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
}

// shutdownDocumentService preserves callback lifetime: stop request admission,
// drain diagnostics, then release service state and its database.
func shutdownDocumentService(ctx context.Context, appServer, diagnosticsServer *http.Server, service *documentservice.Service, cleanup func() error) error {
	if appServer != nil {
		if err := appServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
