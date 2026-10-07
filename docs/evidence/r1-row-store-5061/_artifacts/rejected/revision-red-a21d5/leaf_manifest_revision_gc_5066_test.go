package db

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func revisionFiles5066(t *testing.T, dir string) map[string]bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "manifest.durable.") {
			out[e.Name()] = true
		}
	}
	return out
}

func TestLeafGenerationGCReclaimsReleasedManifestRevisions5066(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact parent namespaces unavailable")
	}
	opts := Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			database.Close()
		}
	}()
	held, err := database.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	heldName := leafGenerationDurableManifestFileName(held.Revision())
	for range 9 {
		closure, err := database.PrepareLeafGenerationManifestStableClosure()
		if err != nil {
			t.Fatal(err)
		}
		closure.Release()
	}
	dir := LeafLogDirPath(opts.Dir)
	before := revisionFiles5066(t, dir)
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	after := revisionFiles5066(t, dir)
	if len(after) >= len(before) {
		t.Fatalf("released immutable revisions were not reclaimed: before=%d after=%d", len(before), len(after))
	}
	if !after[heldName] {
		t.Fatal("prepared revision deleted while pinned")
	}
	held.Release()
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	if revisionFiles5066(t, dir)[heldName] {
		t.Fatal("released prepared revision still retained")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	database, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen after revision GC: %v", err)
	}
}
