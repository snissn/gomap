//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableLeafGenerationManifestReplacementReturnsExactSyncedToken(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	counters := &leafGenerationManifestDurabilityCounters{}
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	store.durabilityCounters = counters
	defer store.Close()
	var creates, renames, contentSyncs, namespaceSyncs int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Namespace == durabilitycut.NamespaceCreate:
			creates++
		case event.Namespace == durabilitycut.NamespaceRename:
			renames++
		case event.Point == durabilitycut.AfterDependencyFileSync:
			contentSyncs++
		case event.Point == durabilitycut.AfterNewFileDirectorySync:
			namespaceSyncs++
		}
		return nil
	})
	defer restore()

	manifest := newLeafGenerationManifest(10)
	token, err := store.Replace(manifest)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	defer token.Release()
	if manifest.ManifestRevision != 1 {
		t.Fatalf("ManifestRevision=%d want 1", manifest.ManifestRevision)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("stable replacement must not create manifest.json: %v", err)
	}
	revisionPath := filepath.Join(leafDir, leafGenerationDurableManifestFileName(1))
	data, err := os.ReadFile(revisionPath)
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
	if creates != 1 || renames != 1 || contentSyncs != 1 || namespaceSyncs != 1 {
		t.Fatalf("durability observations create=%d rename=%d content-sync=%d namespace-sync=%d want 1,1,1,1", creates, renames, contentSyncs, namespaceSyncs)
	}
	if got := registry.PinCount(token.Identity()); got != 1 {
		t.Fatalf("pin count=%d want 1", got)
	}
}

func TestStableLeafGenerationManifestStoreRetainsParentAcrossPathRebind(t *testing.T) {
	root := t.TempDir()
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	defer store.Close()
	retainedDir := filepath.Join(root, "retained_leaf_vlog")
	if err := os.Rename(leafDir, retainedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	token, err := store.Replace(newLeafGenerationManifest(1))
	if err != nil {
		t.Fatalf("Replace after path rebind: %v", err)
	}
	token.Release()
	revisionName := leafGenerationDurableManifestFileName(1)
	if _, err := os.Stat(filepath.Join(retainedDir, revisionName)); err != nil {
		t.Fatalf("retained exact parent manifest revision: %v", err)
	}
	if _, err := os.Stat(filepath.Join(retainedDir, leafGenerationManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("stable replacement must not create manifest.json in retained parent: %v", err)
	}
	if _, err := os.Stat(filepath.Join(leafDir, revisionName)); !os.IsNotExist(err) {
		t.Fatalf("diagnostic replacement path became authoritative: %v", err)
	}
	if _, err := os.Stat(filepath.Join(leafDir, leafGenerationManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("diagnostic replacement path created manifest.json: %v", err)
	}
}

func TestStableLeafGenerationManifestCrashModelRetainsParentAcrossPathRebind(t *testing.T) {
	root := t.TempDir()
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	defer store.Close()
	model, err := powerlossoracle.Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	retainedDir := filepath.Join(root, "retained_leaf_vlog")
	if err := os.Rename(leafDir, retainedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{Point: durabilitycut.AfterNewFileDirectorySync, Path: root}); err != nil {
		t.Fatalf("stabilize rebind fixture: %v", err)
	}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		return model.Observe(root, event)
	})
	token, err := store.Replace(newLeafGenerationManifest(1))
	restore()
	if err != nil {
		t.Fatalf("Replace after path rebind: %v", err)
	}
	token.Release()

	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "retained_leaf_vlog", leafGenerationManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("stable replacement must not materialize manifest.json: %v", err)
	}
	revisionName := leafGenerationDurableManifestFileName(1)
	if _, err := os.Stat(filepath.Join(crashDir, "retained_leaf_vlog", revisionName)); err != nil {
		t.Fatalf("retained crash revision: %v", err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "leaf_vlog", leafGenerationManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("rebound crash path became authoritative: %v", err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "leaf_vlog", revisionName)); !os.IsNotExist(err) {
		t.Fatalf("rebound crash revision became authoritative: %v", err)
	}
}

func TestStableLeafGenerationManifestStoreLoadsOnlyRetainedParentAfterPathRebind(t *testing.T) {
	root := t.TempDir()
	leafDir := filepath.Join(root, "leaf_vlog")
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	original := newLeafGenerationManifest(1)
	if err := saveLeafGenerationManifest(leafDir, original); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	defer store.Close()
	retainedDir := filepath.Join(root, "retained_leaf_vlog")
	if err := os.Rename(leafDir, retainedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(leafDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rogue := &leafGenerationManifest{
		Version: leafGenerationManifestVersion, CurrentGenerationID: 9, NextGenerationID: 10,
		Generations: []leafGenerationRecord{{GenerationID: 9, State: leafGenerationStateWritable}},
	}
	if err := saveLeafGenerationManifest(leafDir, rogue); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := store.Load()
	if err != nil || !ok {
		t.Fatalf("Load retained parent: ok=%v err=%v", ok, err)
	}
	if got, want := loaded.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d want retained original %d", got, want)
	}
}

func TestStableLeafGenerationManifestReplacementSkipsUnknownStaleTempCollision(t *testing.T) {
	leafDir := t.TempDir()
	staleName := "manifest.stage.tmp.0000000000000001"
	stalePath := filepath.Join(leafDir, staleName)
	if err := os.WriteFile(stalePath, []byte("unknown-stale-temp"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	defer store.Close()
	token, err := store.Replace(newLeafGenerationManifest(1))
	if err != nil {
		t.Fatalf("Replace with stale temp collision: %v", err)
	}
	token.Release()
	stale, err := os.ReadFile(stalePath)
	if err != nil {
		t.Fatalf("unknown stale temp was removed: %v", err)
	}
	if string(stale) != "unknown-stale-temp" {
		t.Fatalf("unknown stale temp changed: %q", stale)
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

func TestStableLeafGenerationManifestReplacementPreservesPersistedSyntaxError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fileName string
	}{
		{name: "durable_revision", fileName: leafGenerationDurableManifestFileName(1)},
		{name: "legacy_manifest_json", fileName: leafGenerationManifestFileName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leafDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(leafDir, tc.fileName), []byte(`{"version":`), 0o600); err != nil {
				t.Fatal(err)
			}
			store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
			defer store.Close()

			_, err := store.Replace(newLeafGenerationManifest(10))
			if !errors.Is(err, ErrLeafGenerationManifestIncompatible) {
				t.Fatalf("Replace error=%v want ErrLeafGenerationManifestIncompatible", err)
			}
			var syntaxErr *json.SyntaxError
			if !errors.As(err, &syntaxErr) {
				t.Fatalf("Replace error=%v does not preserve json.SyntaxError", err)
			}
		})
	}
}

func TestStableLeafGenerationManifestReplacementRetainsPinnedOlderRevision(t *testing.T) {
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
	next, err := secondStore.Replace(manifest.clone())
	if err != nil {
		t.Fatalf("Replace while older revision retained: %v", err)
	}
	if rootpublication.SamePhysicalIdentity(retained.Identity(), next.Identity()) || retained.Generation() == next.Generation() {
		t.Fatalf("manifest revisions coalesced identities old=%+v new=%+v", retained.Identity(), next.Identity())
	}
	for _, token := range []*rootpublication.StableResourceToken{retained, next} {
		if err := token.WithPinnedFile(func(file *os.File) error {
			_, err := file.Stat()
			return err
		}); err != nil {
			t.Fatalf("retained revision %d: %v", token.Generation(), err)
		}
	}
	retained.Release()
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
	oldRevisionPath := filepath.Join(leafDir, leafGenerationDurableManifestFileName(1))
	wantOld, err := os.ReadFile(oldRevisionPath)
	if err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(leafDir)
	if err != nil {
		t.Fatal(err)
	}
	tempName := "manifest.stage.tmp.0000000000000002"
	var deletion []string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Namespace == durabilitycut.NamespaceUnlink:
			deletion = append(deletion, string(event.Namespace))
			if event.StableParent != store.parent || event.StablePath == nil || event.StableOldName != tempName {
				t.Errorf("unlink did not observe the exact retained parent and temp: %+v", event)
			}
		case event.Point == durabilitycut.BeforeDeletionDirectorySync, event.Point == durabilitycut.AfterDeletionDirectorySync:
			deletion = append(deletion, string(event.Point))
			if event.StablePath != store.parent {
				t.Errorf("deletion fence did not use the retained parent: %+v", event)
			}
		}
		return model.Observe(leafDir, event)
	})
	defer restore()
	cut := errors.New("pre-rename cut")
	store.hooks.BeforeRename = func() error {
		// The staging temp link could already survive a crash before the producer's
		// own directory sync. Establish that outcome deterministically.
		if err := store.parent.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, leafDir, leafDir, store.parent); err != nil {
			t.Fatal(err)
		}
		return cut
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Replace error=%v want pre-rename cut without recovery-required", err)
	}
	gotOld, err := os.ReadFile(oldRevisionPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotOld) != string(wantOld) {
		t.Fatal("pre-rename failure changed old manifest bytes")
	}
	if want := []string{string(durabilitycut.NamespaceUnlink), string(durabilitycut.BeforeDeletionDirectorySync), string(durabilitycut.AfterDeletionDirectorySync)}; !slices.Equal(deletion, want) {
		t.Errorf("deletion events=%v want %v", deletion, want)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, tempName)); !os.IsNotExist(err) {
		t.Errorf("early rollback temp survived modeled crash: %v", err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, leafGenerationDurableManifestFileName(2))); !os.IsNotExist(err) {
		t.Errorf("pre-rename revision appeared in modeled crash: %v", err)
	}
	temps, err := filepath.Glob(filepath.Join(leafDir, "manifest.stage.tmp.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temps) != 0 || registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("pre-rename leak temps=%v pins=%d identities=%d", temps, registry.ActivePins(), registry.ActiveIdentities())
	}
}

func TestStableLeafGenerationManifestPreRenameRebindPreservesForeignChild(t *testing.T) {
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
	path := filepath.Join(leafDir, "manifest.stage.tmp.0000000000000002")
	const foreign = "foreign manifest"
	var foreignIdentity os.FileInfo
	cut := errors.New("pre-rename cut after rebind")
	store.hooks.BeforeRename = func() error {
		if err := os.Rename(path, path+".displaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(foreign), 0o600); err != nil {
			t.Fatal(err)
		}
		var err error
		foreignIdentity, err = os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return cut
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Errorf("rebound revision error=%v want cut, resource conflict and recovery required", err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != foreign {
		t.Errorf("rebound foreign revision=%q err=%v", data, err)
	}
	if info, err := os.Stat(path); err != nil || !os.SameFile(info, foreignIdentity) {
		t.Errorf("rebound foreign inode changed: %v", err)
	}
	if data, err := os.ReadFile(path + ".displaced"); err != nil {
		t.Error(err)
	} else if displaced, err := decodeLeafGenerationManifest(data, path); err != nil || displaced.ManifestRevision != 2 {
		t.Errorf("displaced manifest=%v err=%v want revision 2", displaced, err)
	}
	if got := store.AmbiguousEvidenceCount(); got != 1 {
		t.Errorf("ambiguous revision evidence=%d want 1", got)
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, ErrRecoveryRequired) {
		t.Errorf("retry after rebound revision=%v want recovery required", err)
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
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("stable replacement must not create manifest.json: %v", err)
	}
	priorBytes, err := os.ReadFile(filepath.Join(leafDir, leafGenerationDurableManifestFileName(1)))
	if err != nil {
		t.Fatalf("prior durable revision: %v", err)
	}
	prior, err := decodeLeafGenerationManifest(priorBytes, leafGenerationDurableManifestFileName(1))
	if err != nil || prior.ManifestRevision != 1 {
		t.Fatalf("prior revision=%v err=%v want revision 1", prior, err)
	}
	durableBytes, err := os.ReadFile(filepath.Join(leafDir, leafGenerationDurableManifestFileName(2)))
	if err != nil {
		t.Fatalf("retained ambiguous durable revision: %v", err)
	}
	durable, err := decodeLeafGenerationManifest(durableBytes, leafGenerationDurableManifestFileName(2))
	if err != nil || durable.ManifestRevision != 2 {
		t.Fatalf("durable revision=%v err=%v want revision 2", durable, err)
	}
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil || !ok {
		t.Fatalf("reopen manifest: ok=%v err=%v", ok, err)
	}
	if loaded.ManifestRevision != 2 {
		t.Fatalf("reopen ManifestRevision=%d want complete revision 2", loaded.ManifestRevision)
	}
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatalf("close leak pins=%d identities=%d", registry.ActivePins(), registry.ActiveIdentities())
	}
}

func TestStableLeafGenerationManifestDestinationRebindFailsClosed(t *testing.T) {
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	manifest := newLeafGenerationManifest(1)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	store.hooks.BeforeDestinationValidation = func() error {
		path := filepath.Join(leafDir, leafGenerationDurableManifestFileName(2))
		if err := os.Rename(path, path+".displaced"); err != nil {
			return err
		}
		return os.WriteFile(path, []byte("rogue"), 0o600)
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, rootpublication.ErrResourceConflict) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("destination rebind error=%v want ErrResourceConflict + ErrRecoveryRequired", err)
	}
	if store.AmbiguousEvidenceCount() != 1 {
		t.Fatalf("evidence=%d want 1", store.AmbiguousEvidenceCount())
	}
	store.Close()
}

func TestStableLeafGenerationManifestParentSyncFailurePoisons(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	manifest := newLeafGenerationManifest(1)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	cut := errors.New("parent sync cut")
	store.hooks.BeforeParentSync = func() error { return cut }
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("parent sync error=%v want cut + ErrRecoveryRequired", err)
	}
	store.Close()
}

func TestStableLeafGenerationManifestAmbiguityRejectsLoadAndBootstrapScan(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	manifest := newLeafGenerationManifest(1)
	first, err := store.Replace(manifest)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()

	cut := errors.New("parent sync cut")
	store.hooks.BeforeParentSync = func() error { return cut }
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, cut) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("parent sync error=%v want cut + ErrRecoveryRequired", err)
	}
	if _, _, err := store.Load(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Load after ambiguous replacement error=%v want ErrRecoveryRequired", err)
	}
	if _, err := store.listBootstrapFiles(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("listBootstrapFiles after ambiguous replacement error=%v want ErrRecoveryRequired", err)
	}
	store.Close()
}

func TestStableLeafGenerationManifestReleaseReturnsResourcesToBaseline(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor baseline uses /proc/self/fd")
	}
	leafDir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	baseline := countLeafManifestTestFDs(t)
	manifest := newLeafGenerationManifest(1)
	for i := 0; i < 128; i++ {
		token, err := store.Replace(manifest)
		if err != nil {
			t.Fatalf("replace %d: %v", i, err)
		}
		token.Release()
		if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
			t.Fatalf("iteration %d leaked pins=%d identities=%d", i, registry.ActivePins(), registry.ActiveIdentities())
		}
	}
	store.Close()
	if got := countLeafManifestTestFDs(t); got > baseline+2 {
		t.Fatalf("descriptor count=%d baseline=%d", got, baseline)
	}
}

func countLeafManifestTestFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestCompatibleLeafGenerationManifestConcurrentCallsAllocateUniqueRevisions(t *testing.T) {
	leafDir := t.TempDir()
	base := newLeafGenerationManifest(1)
	store := newLeafGenerationManifestStore(leafDir, nil, leafGenerationManifestCompatibility, nil)
	defer store.Close()
	if _, err := store.Replace(base); err != nil {
		t.Fatal(err)
	}
	const writers = 16
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.Replace(base.clone())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got, want := loaded.ManifestRevision, uint64(writers+1); got != want {
		t.Fatalf("ManifestRevision=%d want %d", got, want)
	}
}

func BenchmarkStableLeafGenerationManifestReplacement(b *testing.B) {
	leafDir := b.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	counters := &leafGenerationManifestDurabilityCounters{}
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestStable, nil)
	store.durabilityCounters = counters
	manifest := newLeafGenerationManifest(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := store.Replace(manifest)
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
	b.StopTimer()
	store.Close()
	if got := counters.ContentSyncs.Load(); got != uint64(b.N) {
		b.Fatalf("content syncs=%d want %d", got, b.N)
	}
	if got := counters.NamespaceSyncs.Load(); got != uint64(b.N) {
		b.Fatalf("namespace syncs=%d want %d", got, b.N)
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

func TestStableLeafGenerationManifestNeverCreatesManifestJSONAcrossLifecycle(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable manifest replacement requires exact relative namespace support")
	}
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer := newRewriteWriter(ValueLogDirPath(dir), 0, 0, 64<<20)
	writer.ConfigureLeafLog(LeafLogDirPath(dir), rewriteLeafLogLaneID, 0)
	database.SetLeafPageLog(writer)
	defer func() {
		if writer != nil {
			_ = writer.Close()
		}
	}()
	leafDir := LeafLogDirPath(dir)
	assertNoManifestJSON := func(stage string) {
		t.Helper()
		if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
			t.Fatalf("%s: unexpected manifest.json on stable platform: %v", stage, err)
		}
	}
	assertNoManifestJSON("initial open")

	// Advance multiple commits / manifest closures, pin one, abandon one.
	for i := 0; i < 4; i++ {
		if i > 0 {
			if err := writer.rotateLeaf(); err != nil {
				t.Fatalf("rotateLeaf %d: %v", i, err)
			}
		}
		if err := database.SetSync([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("v-%02d", i))); err != nil {
			t.Fatalf("SetSync %d: %v", i, err)
		}
	}
	pinnedClosure, err := database.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		t.Fatalf("PrepareLeafGenerationManifestStableClosure pinned: %v", err)
	}
	pinnedName := leafGenerationDurableManifestFileName(pinnedClosure.Revision())
	for i := 0; i < 4; i++ {
		c, err := database.PrepareLeafGenerationManifestStableClosure()
		if err != nil {
			pinnedClosure.Release()
			t.Fatalf("PrepareLeafGenerationManifestStableClosure %d: %v", i, err)
		}
		c.Release()
	}
	database.mu.RLock()
	activeRev := database.leafGenerationManifest.ManifestRevision
	candidate := database.leafGenerationManifest.clone()
	database.mu.RUnlock()

	abandoned, _, err := database.prepareLeafGenerationManifestStableCandidate(candidate)
	if err != nil {
		pinnedClosure.Release()
		t.Fatalf("prepare abandoned candidate: %v", err)
	}
	abandonedName := leafGenerationDurableManifestFileName(abandoned.Revision())
	abandonedResources, err := abandoned.TakeStableResources()
	if err != nil {
		pinnedClosure.Release()
		t.Fatalf("TakeStableResources abandoned: %v", err)
	}
	abandonedResources.Release()
	if err := abandoned.abandonUnpublished(); err != nil {
		pinnedClosure.Release()
		t.Fatalf("abandonUnpublished: %v", err)
	}
	if _, err := os.Stat(filepath.Join(leafDir, abandonedName)); !os.IsNotExist(err) {
		pinnedClosure.Release()
		t.Fatalf("abandoned revision %s still exists: %v", abandonedName, err)
	}
	assertNoManifestJSON("after abandonment")

	// GC must retain pinned and current active revisions while deleting unreferenced ones.
	stats, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		pinnedClosure.Release()
		t.Fatalf("LeafGenerationGC with pin: %v", err)
	}
	if stats.ManifestRevisionsDeleted == 0 {
		pinnedClosure.Release()
		t.Fatalf("expected unreferenced manifest revisions deleted, stats=%+v", stats)
	}
	if _, err := os.Stat(filepath.Join(leafDir, pinnedName)); err != nil {
		pinnedClosure.Release()
		t.Fatalf("pinned revision %s removed by GC: %v", pinnedName, err)
	}
	database.mu.RLock()
	activeRevAfterGC1 := database.leafGenerationManifest.ManifestRevision
	database.mu.RUnlock()
	if activeRevAfterGC1 < activeRev {
		pinnedClosure.Release()
		t.Fatalf("active revision regressed after GC1: got %d want >= %d", activeRevAfterGC1, activeRev)
	}
	if _, err := os.Stat(filepath.Join(leafDir, leafGenerationDurableManifestFileName(activeRevAfterGC1))); err != nil {
		pinnedClosure.Release()
		t.Fatalf("active revision %s removed by GC1: %v", leafGenerationDurableManifestFileName(activeRevAfterGC1), err)
	}
	pinnedClosure.Release()
	stats2, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatalf("LeafGenerationGC after release: %v", err)
	}
	if stats2.ManifestRevisionsDeleted == 0 {
		t.Fatalf("expected released revision %s deleted, stats=%+v", pinnedName, stats2)
	}
	if _, err := os.Stat(filepath.Join(leafDir, pinnedName)); !os.IsNotExist(err) {
		t.Fatalf("released revision %s still exists: %v", pinnedName, err)
	}
	database.mu.RLock()
	activeRevAfterGC2 := database.leafGenerationManifest.ManifestRevision
	database.mu.RUnlock()
	if activeRevAfterGC2 < activeRevAfterGC1 {
		t.Fatalf("active revision regressed after GC2: got %d want >= %d", activeRevAfterGC2, activeRevAfterGC1)
	}
	if _, err := os.Stat(filepath.Join(leafDir, leafGenerationDurableManifestFileName(activeRevAfterGC2))); err != nil {
		t.Fatalf("active revision %s missing after GC2: %v", leafGenerationDurableManifestFileName(activeRevAfterGC2), err)
	}
	assertNoManifestJSON("after GC")

	if err := database.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	writer = nil
	reopened, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen without manifest.json: %v", err)
	}
	defer reopened.Close()
	assertNoManifestJSON("after reopen")
	for i := 0; i < 4; i++ {
		got, err := reopened.Get([]byte(fmt.Sprintf("k-%02d", i)))
		if err != nil || string(got) != fmt.Sprintf("v-%02d", i) {
			t.Fatalf("Get(k-%02d)=%q err=%v", i, got, err)
		}
	}
	nextClosure, err := reopened.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		t.Fatalf("PrepareLeafGenerationManifestStableClosure after reopen: %v", err)
	}
	if nextClosure.Revision() <= activeRevAfterGC2 {
		t.Fatalf("post-reopen revision=%d must exceed prior active revision=%d", nextClosure.Revision(), activeRevAfterGC2)
	}
	nextClosure.Release()
	assertNoManifestJSON("after post-reopen replacement")
}

// TestStableLeafGenerationManifestCrashAtNamespaceCreateReopensCleanly verifies
// that sudden process death immediately after NamespaceCreate during the first
// Open leaves only an unpublished staging temp file (never an empty
// manifest.durable.<rev>.json), so the next Open bootstraps cleanly instead of
// failing with ErrLeafGenerationManifestIncompatible.
func TestStableLeafGenerationManifestCrashAtNamespaceCreateReopensCleanly(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace not supported")
	}
	if crashDir := os.Getenv("TREEDB_TEST_CRASH_AT_MANIFEST_CREATE_DIR"); crashDir != "" {
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Namespace == durabilitycut.NamespaceCreate &&
				event.Resource == durabilitycut.ResourceOuterLeaf &&
				filepath.Clean(filepath.Dir(event.NewPath)) == filepath.Clean(LeafLogDirPath(crashDir)) {
				os.Exit(99)
			}
			return nil
		})
		defer restore()
		opts := Options{
			Dir:                        crashDir,
			IndexOuterLeavesInValueLog: true,
			DisableBackgroundPrune:     true,
		}
		_, _ = Open(opts)
		os.Exit(1)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStableLeafGenerationManifestCrashAtNamespaceCreateReopensCleanly$")
	cmd.Env = append(os.Environ(), "TREEDB_TEST_CRASH_AT_MANIFEST_CREATE_DIR="+dir)
	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 99 {
		t.Fatalf("subprocess exit=%v want exit code 99 at NamespaceCreate", err)
	}

	leafDir := LeafLogDirPath(dir)
	durableMatches, globErr := filepath.Glob(filepath.Join(leafDir, "manifest.durable.*.json"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(durableMatches) != 0 {
		t.Fatalf("unfinished creation exposed durable manifest filenames: %v", durableMatches)
	}
	tmpMatches, globErr := filepath.Glob(filepath.Join(leafDir, "manifest.stage.tmp.*"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(tmpMatches) != 1 {
		t.Fatalf("expected 1 orphaned staging temp file after crash, got %v", tmpMatches)
	}

	reopened, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("Open after crash at NamespaceCreate failed: %v", err)
	}
	if reopened.leafGenerationManifest == nil || reopened.leafGenerationManifest.ManifestRevision == 0 {
		_ = reopened.Close()
		t.Fatalf("expected bootstrapped leaf generation manifest after crash recovery, got %+v", reopened.leafGenerationManifest)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		_ = reopened.Close()
		t.Fatalf("reopen must not create manifest.json on stable platform: %v", err)
	}
	// Verify that an orphaned staging file never blocks subsequent manifest
	// revision GC census scans.
	if _, err := reopened.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		_ = reopened.Close()
		t.Fatalf("LeafGenerationGC with orphaned staging file failed: %v", err)
	}
	closure, err := reopened.PrepareLeafGenerationManifestStableClosure()
	if err != nil {
		_ = reopened.Close()
		t.Fatalf("PrepareLeafGenerationManifestStableClosure after crash recovery: %v", err)
	}
	revAfterCrash := closure.Revision()
	closure.Release()
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close after crash recovery: %v", err)
	}
	reopened2, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
		DisableBackgroundPrune:     true,
	})
	if err != nil {
		t.Fatalf("second Open after crash recovery failed: %v", err)
	}
	defer reopened2.Close()
	if reopened2.leafGenerationManifest == nil || reopened2.leafGenerationManifest.ManifestRevision < revAfterCrash {
		t.Fatalf("reopened manifest revision=%+v want >= %d", reopened2.leafGenerationManifest, revAfterCrash)
	}
}
