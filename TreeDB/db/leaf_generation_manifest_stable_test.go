//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableLeafGenerationManifestReplacementReturnsExactSyncedToken(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	counters := &leafGenerationManifestDurabilityCounters{}
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	store.durabilityCounters = counters
	defer store.Close()

	manifest := newLeafGenerationManifest(10)
	token, err := store.Replace(manifest)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	defer token.Release()
	if manifest.ManifestRevision != 1 {
		t.Fatalf("ManifestRevision=%d want 1", manifest.ManifestRevision)
	}
	data, err := os.ReadFile(leafGenerationManifestPath(leafDir))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := token.Digest(), sha256.Sum256(data); got != want {
		t.Fatalf("token digest=%x want %x", got, want)
	}
	if got, want := token.Generation(), manifest.ManifestRevision; got != want {
		t.Fatalf("token generation=%d want manifest revision %d", got, want)
	}
	if token.Kind() != rootpublication.ResourceOuterLeafManifest || token.Reachability() != rootpublication.ReachabilityOuterLeafGeneration {
		t.Fatalf("unexpected token contract kind=%q reachability=%q", token.Kind(), token.Reachability())
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("SyncThrough: %v", err)
	}
	if got := counters.ContentSyncs.Load(); got != 1 {
		t.Fatalf("content syncs=%d want exactly 1", got)
	}
	if got := counters.NamespaceSyncs.Load(); got != 1 {
		t.Fatalf("namespace syncs=%d want exactly 1", got)
	}
	if got := registry.PinCount(token.Identity()); got != 1 {
		t.Fatalf("pin count=%d want 1", got)
	}
}

func TestStableLeafGenerationManifestReplacementIncrementsSameGenerationRevision(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	defer store.Close()
	manifest := newLeafGenerationManifest(10)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	firstRevision := manifest.ManifestRevision
	second, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if manifest.CurrentGenerationID != 1 {
		t.Fatalf("CurrentGenerationID=%d want unchanged", manifest.CurrentGenerationID)
	}
	if manifest.ManifestRevision != firstRevision+1 {
		t.Fatalf("ManifestRevision=%d want %d", manifest.ManifestRevision, firstRevision+1)
	}
}

func TestStableLeafGenerationManifestReplacementPinnedOldIdentityBlocksOverwrite(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	firstStore := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	secondStore := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	defer firstStore.Close()
	defer secondStore.Close()
	manifest := newLeafGenerationManifest(10)
	retained, err := firstStore.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secondStore.Replace(manifest.clone()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("Replace while retained error=%v want ErrResourcePinned", err)
	}
	retained.Release()
	next, err := secondStore.Replace(manifest.clone())
	if err != nil {
		t.Fatalf("Replace after release: %v", err)
	}
	next.Release()
}

func TestLeafGenerationManifestV2RejectsZeroRevisionOnReopen(t *testing.T) {
	leafDir := t.TempDir()
	manifest := newLeafGenerationManifest(10)
	manifest.ManifestRevision = 0
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leafGenerationManifestPath(leafDir), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadLeafGenerationManifest(leafDir); !errors.Is(err, ErrLeafGenerationManifestIncompatible) {
		t.Fatalf("load error=%v want ErrLeafGenerationManifestIncompatible", err)
	}
}
