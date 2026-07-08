package main

import (
	"flag"
	"testing"

	"github.com/snissn/gomap/internal/redisserver"
)

func TestRegisterConfigFlagsUsesTreeDBProfileSurface(t *testing.T) {
	fs := flag.NewFlagSet("redisserver-test", flag.ContinueOnError)
	var cfg redisserver.Config
	registerConfigFlags(fs, &cfg)

	for _, name := range []string{"treedb-profile", "treedb-write-lanes"} {
		if fs.Lookup(name) == nil {
			t.Fatalf("flag %q not registered", name)
		}
	}
	for _, name := range []string{"treedb-disable-wal", "treedb-relaxed-sync", "treedb-journal-lanes"} {
		if fs.Lookup(name) != nil {
			t.Fatalf("deprecated flag %q is still registered", name)
		}
	}
}
