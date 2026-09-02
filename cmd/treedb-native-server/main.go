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

	"github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7100", "TCP address to listen on")
	dataDir := flag.String("dir", "/tmp/treedb-native-server", "TreeDB data directory")
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

	if *pprofAddr != "" {
		go func() {
			log.Printf("TreeDB Native Server pprof listening on http://%s/debug/pprof/", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil && err != http.ErrServerClosed {
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
	server := nativewire.NewServer(nativewire.ServerOptions{
		Collections: manager,
		Backend:     database,
	})

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}
	defer ln.Close()

	fmt.Printf("TreeDB Native Server listening on %s\n", ln.Addr().String())
	fmt.Printf("TreeDB data directory: %s\n", *dataDir)
	fmt.Printf("TreeDB profile: %s\n", normalizedProfile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = server.Serve(ctx, ln)
	closeErr := server.Close()
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		log.Fatalf("Server error: %v", err)
	}
	if closeErr != nil {
		log.Fatalf("Server close error: %v", closeErr)
	}
}

func parsePublicProfileFlag(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, nil
}
