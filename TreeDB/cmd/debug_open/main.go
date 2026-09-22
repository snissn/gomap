package main

import (
	"fmt"
	"log"
	"os"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: debug_open <dir>")
	}
	dir := os.Args[1]

	fmt.Println("Opening DB...")
	start := time.Now()

	// Use same options as run_celestia.sh implies
	// But we need to be careful not to corrupt.
	// Just open and close.

	// Set env vars
	os.Setenv("TREEDB_FORCE_VALUE_POINTERS", "1")
	os.Setenv("TREEDB_SLAB_COMPRESSION_MIN_BYTES", "1")
	os.Setenv("TREEDB_SLAB_COMPRESSION_MIN_SAVINGS", "0")

	opts := treedb.Options{
		Dir:            dir,
		FlushThreshold: 64 * 1024 * 1024,
	}

	db, err := treedb.Open(opts)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	fmt.Printf("Open success in %v\n", time.Since(start))

	db.Close()
}
