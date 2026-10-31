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
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/cmd/internal/documentserviceowner"
	"github.com/snissn/gomap/internal/ycsbevidence"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	addr := flag.String("addr", "127.0.0.1:7100", "TCP address to listen on")
	dataDir := flag.String("dir", "/tmp/treedb-native-server", "TreeDB data directory")
	profile := flag.String("profile", string(treedb.ProfileCommandWALDurable), "TreeDB profile: "+treedb.ProfileFlagHelp+"; ColumnGraph requires command_wal_durable and offline public owner/API Optimize after vector writes")
	legacyStorage := flag.Bool("legacy-storage", false, "use legacy storage for specialized typed and normalized/quantized routes; default is shared COW")
	pprofAddr := flag.String("pprof", "", "optional net/http/pprof listen address, e.g. 127.0.0.1:6060")
	blockProfileRate := flag.Int("block-profile-rate", 0, "runtime.SetBlockProfileRate value for pprof diagnostics (0=disabled, 1=all blocking events)")
	mutexProfileFraction := flag.Int("mutex-profile-fraction", 0, "runtime.SetMutexProfileFraction value for pprof diagnostics (0=disabled, 1=all mutex contention)")
	flag.Parse()

	if *dataDir == "" {
		return errors.New("Data directory (-dir) is required")
	}

	normalizedProfile, err := parsePublicProfileFlag(*profile)
	if err != nil {
		return err
	}
	opts := treedb.OptionsFor(normalizedProfile, *dataDir)

	if *blockProfileRate > 0 {
		runtime.SetBlockProfileRate(*blockProfileRate)
	}
	if *mutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(*mutexProfileFraction)
	}

	owner, err := documentserviceowner.Open(opts, *legacyStorage)
	if err != nil {
		return fmt.Errorf("open TreeDB: %w", err)
	}
	var evidence *ycsbevidence.Observer
	defer func() { resultErr = errors.Join(resultErr, evidence.Close(owner.Close)) }()
	evidence, err = ycsbevidence.OpenFromEnv(string(normalizedProfile), *dataDir, owner.Backend, owner.Stats)
	if err != nil {
		return fmt.Errorf("owner evidence: %w", err)
	}
	if *pprofAddr != "" {
		diagnosticsListener, err := net.Listen("tcp", *pprofAddr)
		if err != nil {
			return fmt.Errorf("listen for pprof: %w", err)
		}
		diagnostics := &http.Server{Handler: http.DefaultServeMux}
		defer func() { resultErr = errors.Join(resultErr, diagnostics.Shutdown(context.Background())) }()
		go func() {
			log.Printf("TreeDB Native Server pprof listening on http://%s/debug/pprof/", diagnosticsListener.Addr())
			if err := diagnostics.Serve(diagnosticsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}
	server := nativewire.NewServer(nativewire.ServerOptions{
		Collections: owner.Manager, Backend: owner.Backend, DocumentService: owner.Service,
	})
	defer func() { resultErr = errors.Join(resultErr, server.Close()) }()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}
	defer ln.Close()

	fmt.Printf("TreeDB Native Server listening on %s\n", ln.Addr().String())
	fmt.Printf("TreeDB data directory: %s\n", *dataDir)
	fmt.Printf("TreeDB profile: %s\n", normalizedProfile)
	fmt.Printf("TreeDB storage: %s\n", owner.Storage)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = server.Serve(ctx, ln)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func parsePublicProfileFlag(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, nil
}
