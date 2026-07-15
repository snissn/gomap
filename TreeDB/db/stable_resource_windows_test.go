//go:build windows

package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureStableIndexFileResourceUsesCreateOnlyNamespacePrimitiveWindows(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	snapshot := database.AcquireStableSnapshot()
	if snapshot == nil {
		t.Fatal("AcquireStableSnapshot returned nil")
	}
	defer snapshot.Close()
	token, err := snapshot.CaptureStableIndexFileResource()
	if err != nil {
		t.Fatalf("CaptureStableIndexFileResource: %v", err)
	}
	defer token.Release()
	if got := token.Kind(); got != rootpublication.ResourceIndex {
		t.Fatalf("kind=%q want %q", got, rootpublication.ResourceIndex)
	}
	if got := token.Reachability(); got != rootpublication.ReachabilityIndexFile {
		t.Fatalf("reachability=%q want %q", got, rootpublication.ReachabilityIndexFile)
	}
	if token.Identity() == (rootpublication.StableIdentity{}) || token.Frontier().Bytes == 0 {
		t.Fatalf("missing exact identity/frontier: identity=%+v frontier=%+v", token.Identity(), token.Frontier())
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough: %v", err)
	}
}
