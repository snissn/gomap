package db

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

func TestOptionsPropagation(t *testing.T) {
	dir, err := os.MkdirTemp("", "treedb-options-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		Dir:                   dir,
		SlabCompression:       slab.CompressionOptions{Kind: slab.CompressionZSTD},
		LeafPrefixCompression: true,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Verify Slab Compression
	if db.SlabManager().Compression() != slab.CompressionZSTD {
		t.Errorf("Slab compression not propagated. Got %v, want %v", db.SlabManager().Compression(), slab.CompressionZSTD)
	}

	// Verify Prefix Compression
	if !db.Zipper().LeafPrefixCompression() {
		t.Errorf("Leaf prefix compression not propagated to zipper")
	}
}
