//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
	if creates != 2 || renames != 1 || contentSyncs != 2 || namespaceSyncs != 2 {
		t.Fatalf("durability observations create=%d rename=%d content-sync=%d namespace-sync=%d want 2,1,2,2", creates, renames, contentSyncs, namespaceSyncs)
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
	if _, err := os.Stat(filepath.Join(retainedDir, leafGenerationManifestFileName)); err != nil {
		t.Fatalf("retained exact parent manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(leafDir, leafGenerationManifestFileName)); !os.IsNotExist(err) {
		t.Fatalf("diagnostic replacement path became authoritative: %v", err)
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
	if _, err := os.Stat(filepath.Join(crashDir, "retained_leaf_vlog", leafGenerationManifestFileName)); err != nil {
		t.Fatalf("retained crash manifest: %v", err)
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
	staleName := leafGenerationManifestFileName + ".tmp.0000000000000001"
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
	leafDir := t.TempDir()
	if err := os.WriteFile(leafGenerationManifestPath(leafDir), []byte(`{"version":`), 0o600); err != nil {
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
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil || !ok {
		t.Fatalf("reopen compatibility manifest: ok=%v err=%v", ok, err)
	}
	if loaded.ManifestRevision != 1 {
		t.Fatalf("compatibility ManifestRevision=%d want prior complete revision 1", loaded.ManifestRevision)
	}
	durableBytes, err := os.ReadFile(filepath.Join(leafDir, leafGenerationDurableManifestFileName(2)))
	if err != nil {
		t.Fatalf("retained ambiguous durable revision: %v", err)
	}
	durable, err := decodeLeafGenerationManifest(durableBytes, leafGenerationDurableManifestFileName(2))
	if err != nil || durable.ManifestRevision != 2 {
		t.Fatalf("durable revision=%v err=%v want revision 2", durable, err)
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
