package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7100", "TCP address to listen on")
	dataDir := flag.String("dir", "/tmp/treedb-native-server", "TreeDB data directory")
	profile := flag.String("profile", "fast", "TreeDB profile: fast, durable, etc.")
	flag.Parse()

	if *dataDir == "" {
		log.Fatal("Data directory (-dir) is required")
	}

	opts := treedb.OptionsFor(treedb.Profile(*profile), *dataDir)

	database, err := db.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open TreeDB: %v", err)
	}
	defer database.Close()

	leafLog, err := db.NewStandaloneLeafPageLog(*dataDir, db.StandaloneLeafPageLogOptions{})
	if err != nil {
		log.Fatalf("Failed to open TreeDB leaf-page log: %v", err)
	}
	defer leafLog.Close()
	database.SetLeafPageLog(leafLog)

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = server.Serve(ctx, ln)
	if err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
