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
	if handler := optionalPprofHandler(*pprofAddr); handler != nil {
		listener, err := net.Listen("tcp", *pprofAddr)
		if err != nil {
			log.Fatalf("Failed to listen for pprof on %s: %v", *pprofAddr, err)
		}
		go func() {
			log.Printf("TreeDB Document Service pprof listening on http://%s/debug/pprof/", *pprofAddr)
			if err := http.Serve(listener, handler); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}
	database, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		log.Fatalf("Failed to open TreeDB: %v", err)
	}
	defer func() { _ = cleanup() }()

	manager := collections.NewCollectionManager(database)
	service := documentservice.New(manager)
	defer func() { _ = service.Close() }()
	handler := documentservice.NewHandler(service)
	server := &http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	fmt.Printf("TreeDB Document Service listening on http://%s\n", *addr)
	fmt.Printf("TreeDB data directory: %s\n", *dataDir)
	fmt.Printf("TreeDB profile: %s\n", normalizedProfile)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("Server error: %v", err)
		exitCode = 1
		return
	}
}

func optionalPprofHandler(addr string) http.Handler {
	if addr == "" {
		return nil
	}
	return http.DefaultServeMux
}

func parsePublicProfileFlag(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, nil
}
