//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestStableLeafGenerationManifestReplacementRevisionSurvivesStoreReopen(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	manifest := newLeafGenerationManifest(10)
	firstStore := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	first, err := firstStore.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	firstStore.Close()

	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil || !ok {
		t.Fatalf("load after first replace: ok=%v err=%v", ok, err)
	}
	secondStore := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	defer secondStore.Close()
	second, err := secondStore.Replace(loaded)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if got, want := loaded.ManifestRevision, uint64(2); got != want {
		t.Fatalf("persisted replacement revision=%d want %d", got, want)
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

func TestStableLeafGenerationManifestCapabilityFailsBeforeTempVisibility(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	store.stableCapability = func() bool { return false }
	var beforeTemp atomic.Int32
	store.hooks.BeforeTempCreate = func() error {
		beforeTemp.Add(1)
		return nil
	}
	defer store.Close()
	if _, err := store.Replace(newLeafGenerationManifest(1)); !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
		t.Fatalf("Replace error=%v want ErrNamespacePersistenceUnsupported", err)
	}
	if got := beforeTemp.Load(); got != 0 {
		t.Fatalf("temp-create hook calls=%d want 0", got)
	}
	entries, err := os.ReadDir(leafDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported stable replacement created files: %v", entries)
	}
}

func TestStableLeafGenerationManifestPreRenameFailurePreservesOldAndCleansTemp(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	defer store.Close()
	manifest := newLeafGenerationManifest(1)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	wantOld, err := os.ReadFile(leafGenerationManifestPath(leafDir))
	if err != nil {
		t.Fatal(err)
	}
	cut := errors.New("pre-rename cut")
	store.hooks.BeforeRename = func() error { return cut }
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Replace error=%v want pre-rename cut without recovery-required", err)
	}
	gotOld, err := os.ReadFile(leafGenerationManifestPath(leafDir))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotOld) != string(wantOld) {
		t.Fatal("pre-rename failure changed old manifest bytes")
	}
	temps, err := filepath.Glob(filepath.Join(leafDir, leafGenerationManifestFileName+".tmp.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temps) != 0 || registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("pre-rename leak temps=%v pins=%d identities=%d", temps, registry.ActivePins(), registry.ActiveIdentities())
	}
}

func TestStableLeafGenerationManifestPostRenameFailurePoisonsAndRetainsEvidence(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	var poisoned atomic.Bool
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, func() { poisoned.Store(true) })
	manifest := newLeafGenerationManifest(1)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	cut := errors.New("post-rename validation cut")
	store.hooks.BeforeDestinationValidation = func() error { return cut }
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Replace error=%v want cut + ErrRecoveryRequired", err)
	}
	if !poisoned.Load() || store.AmbiguousEvidenceCount() != 1 {
		t.Fatalf("poisoned=%v evidence=%d want true,1", poisoned.Load(), store.AmbiguousEvidenceCount())
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("poisoned retry error=%v want ErrRecoveryRequired", err)
	}
	store.Close()
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("close leak pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities())
	}
}

func TestCompatibleLeafGenerationManifestSaveDoesNotCertifyStableToken(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, nil, leafGenerationManifestCompatibility, nil)
	defer store.Close()
	manifest := newLeafGenerationManifest(1)
	token, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if token != nil {
		t.Fatal("compatibility replacement returned a stable token")
	}
	if manifest.ManifestRevision != 1 {
		t.Fatalf("ManifestRevision=%d want 1", manifest.ManifestRevision)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); err != nil {
		t.Fatal(err)
	}
}
