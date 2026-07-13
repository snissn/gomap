package treedb_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestErrLeafGenerationGCStaleScanReexportsBackendSentinel(t *testing.T) {
	if !errors.Is(treedbdb.ErrLeafGenerationGCStaleScan, treedb.ErrLeafGenerationGCStaleScan) {
		t.Fatal("public ErrLeafGenerationGCStaleScan does not match backend sentinel")
	}
}

func TestErrLeafGenerationManifestIncompatibleReexportsBackendSentinel(t *testing.T) {
	if !errors.Is(treedbdb.ErrLeafGenerationManifestIncompatible, treedb.ErrLeafGenerationManifestIncompatible) {
		t.Fatal("public ErrLeafGenerationManifestIncompatible does not match backend sentinel")
	}
}

func TestOpenRejectsStructurallyInvalidLeafGenerationManifestWithPublicSentinel(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("create DB: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close DB: %v", err)
	}

	manifestPath := filepath.Join(dir, "maindb", "leaf_vlog", "manifest.json")
	data := []byte(`{"version":2,"manifest_revision":1,"current_generation_id":1,"next_generation_id":2,"generations":[]}`)
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatalf("write structurally invalid manifest: %v", err)
	}

	reopened, err := treedb.Open(treedb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if reopened != nil {
		_ = reopened.Close()
		t.Fatal("Open returned a DB for a structurally invalid manifest")
	}
	if !errors.Is(err, treedb.ErrLeafGenerationManifestIncompatible) {
		t.Fatalf("Open error=%v want public ErrLeafGenerationManifestIncompatible", err)
	}
}
