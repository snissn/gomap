package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7120", "HTTP address to listen on")
	dataDir := flag.String("dir", "/tmp/treedb-document-service", "TreeDB data directory")
	profile := flag.String("profile", string(treedb.ProfileCommandWALDurable), "TreeDB profile: "+treedb.ProfileFlagHelp)
	flag.Parse()

	if *dataDir == "" {
		log.Fatal("Data directory (-dir) is required")
	}
	normalizedProfile, err := parsePublicProfileFlag(*profile)
	if err != nil {
		log.Fatal(err)
	}
	opts := treedb.OptionsFor(normalizedProfile, *dataDir)
	database, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		log.Fatalf("Failed to open TreeDB: %v", err)
	}
	defer func() { _ = cleanup() }()

	manager := collections.NewCollectionManager(database)
	handler := documentservice.NewHandler(documentservice.New(manager))
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
		return
	}
}

func parsePublicProfileFlag(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParsePublicProfile(raw, treedb.ProfileCommandWALDurable)
	if !ok {
		return "", fmt.Errorf("unsupported TreeDB profile %q; allowed: %s", raw, treedb.ProfileFlagHelp)
	}
	return profile, nil
}
