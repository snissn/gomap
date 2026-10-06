package db

import (
	"context"
	"testing"
)

func TestLeafManifestRevisionGCCompatibility5066(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// Exercise the platform compatibility mode without inventing exact tokens.
	database.leafGenerationManifestStore.mode = leafGenerationManifestCompatibility
	database.leafGenerationManifestStore.stableCapability = func() bool { return false }
	captures := database.stableIndexCaptures.Load()
	stats := LeafGenerationGCStats{}
	if err := database.gcLeafManifestRevisions(context.Background(), LeafGenerationGCOptions{}, &stats); err != nil {
		t.Fatal(err)
	}
	if !stats.ManifestRevisionGCUnsupported || stats.ManifestRevisionsDeleted != 0 || database.stableIndexCaptures.Load() != captures {
		t.Fatalf("compatibility revision phase acquired stable authority or claimed deletion: %+v", stats)
	}
	stats, err = database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil || !stats.ManifestRevisionGCUnsupported || stats.ManifestRevisionsDeleted != 0 {
		t.Fatalf("compatibility segment phase: %+v err=%v", stats, err)
	}
}
