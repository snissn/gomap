//go:build darwin || linux || freebsd || netbsd || openbsd

package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func openStableManifestTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{
		Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestLeafGenerationManifestStableResourceBindsRevisionDigestAndExactHandle(t *testing.T) {
	db := openStableManifestTestDB(t)
	manifest := db.leafGenerationManifest.clone()
	manifest.NextGenerationID++
	token, err := db.saveLeafGenerationManifestWithStableResource(manifest, rootpublication.ReachabilityOuterLeafGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()

	path := leafGenerationManifestPath(LeafLogDirPath(db.dir))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted leafGenerationManifest
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Revision == 0 || token.Generation() != persisted.Revision {
		t.Fatalf("persisted revision=%d token generation=%d", persisted.Revision, token.Generation())
	}
	if got, want := token.Digest(), sha256.Sum256(data); got != want {
		t.Fatalf("token digest=%x want %x", got, want)
	}
	if token.Kind() != rootpublication.ResourceOuterLeafManifest || token.Reachability() != rootpublication.ReachabilityOuterLeafGeneration {
		t.Fatalf("token kind/reachability=%s/%s", token.Kind(), token.Reachability())
	}
	if token.Namespace() == nil || token.Namespace().Operation() != rootpublication.NamespaceRename {
		t.Fatalf("namespace=%v, want retained rename", token.Namespace())
	}
	current, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	identity, err := rootpublication.StableIdentityFromFile(current)
	if err != nil {
		t.Fatal(err)
	}
	if !rootpublication.SamePhysicalIdentity(token.Identity(), identity) {
		t.Fatal("token was not bound to the installed manifest inode")
	}
	if err := token.SyncThrough(); err != nil {
		t.Fatalf("retained token sync: %v", err)
	}
}

func TestLeafGenerationManifestStableResourceBlocksPinnedDestinationReplacement(t *testing.T) {
	db := openStableManifestTestDB(t)
	first := db.leafGenerationManifest.clone()
	first.NextGenerationID++
	token, err := db.saveLeafGenerationManifestWithStableResource(first, rootpublication.ReachabilityOuterLeafGeneration)
	if err != nil {
		t.Fatal(err)
	}
	second := first.clone()
	second.NextGenerationID++
	if _, err := db.saveLeafGenerationManifestWithStableResource(second, rootpublication.ReachabilityOuterLeafGeneration); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("replacement error=%v want ErrResourcePinned", err)
	}
	token.Release()
	next, err := db.saveLeafGenerationManifestWithStableResource(second, rootpublication.ReachabilityOuterLeafGeneration)
	if err != nil {
		t.Fatal(err)
	}
	next.Release()
}

func TestLeafGenerationManifestStableResourcePostRenameFailurePoisonsHandle(t *testing.T) {
	db := openStableManifestTestDB(t)
	manifest := db.leafGenerationManifest.clone()
	manifest.NextGenerationID++
	cutErr := errors.New("manifest rename cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.NewPath) == leafGenerationManifestFileName {
			return cutErr
		}
		return nil
	})
	token, err := db.saveLeafGenerationManifestWithStableResource(manifest, rootpublication.ReachabilityOuterLeafGeneration)
	restore()
	if token != nil {
		token.Release()
	}
	if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("save error=%v want cut and ErrRecoveryRequired", err)
	}
	if !db.publicationPoisoned.Load() {
		t.Fatal("post-rename failure did not poison DB")
	}
	if _, statErr := os.Stat(leafGenerationManifestPath(LeafLogDirPath(db.dir))); statErr != nil {
		t.Fatalf("renamed manifest missing after ambiguous cut: %v", statErr)
	}
}

func TestLeafGenerationManifestStableResourceExactSyncCountsAndNoPinGrowth(t *testing.T) {
	db := openStableManifestTestDB(t)
	baselinePins := db.valueLogIdentityPins.ActivePins()
	baselineIdentities := db.valueLogIdentityPins.ActiveIdentities()
	var contentBefore, contentAfter, namespaceBefore, namespaceAfter, renames int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		switch {
		case event.Point == durabilitycut.BeforeDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf:
			contentBefore++
		case event.Point == durabilitycut.AfterDependencyFileSync && event.Resource == durabilitycut.ResourceOuterLeaf:
			contentAfter++
		case event.Point == durabilitycut.BeforeNewFileDirectorySync && event.Resource == durabilitycut.ResourceOuterLeaf:
			namespaceBefore++
		case event.Point == durabilitycut.AfterNewFileDirectorySync && event.Resource == durabilitycut.ResourceOuterLeaf:
			namespaceAfter++
		case event.Namespace == durabilitycut.NamespaceRename && filepath.Base(event.NewPath) == leafGenerationManifestFileName:
			renames++
		}
		return nil
	})
	const replacements = 32
	for i := 0; i < replacements; i++ {
		manifest := db.leafGenerationManifest.clone()
		manifest.NextGenerationID += uint64(i + 1)
		token, err := db.saveLeafGenerationManifestWithStableResource(manifest, rootpublication.ReachabilityOuterLeafGeneration)
		if err != nil {
			restore()
			t.Fatalf("replacement %d: %v", i, err)
		}
		token.Release()
		db.leafGenerationManifest = manifest
	}
	restore()
	if contentBefore != replacements || contentAfter != replacements ||
		namespaceBefore != replacements || namespaceAfter != replacements || renames != replacements {
		t.Fatalf("events content=%d/%d namespace=%d/%d renames=%d want %d each",
			contentBefore, contentAfter, namespaceBefore, namespaceAfter, renames, replacements)
	}
	if got := db.valueLogIdentityPins.ActivePins(); got != baselinePins {
		t.Fatalf("active pins=%d want baseline %d", got, baselinePins)
	}
	if got := db.valueLogIdentityPins.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities=%d want baseline %d", got, baselineIdentities)
	}
}

func BenchmarkLeafGenerationManifestStableReplace(b *testing.B) {
	db, err := Open(Options{
		Dir: b.TempDir(), Durability: DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	manifest := db.leafGenerationManifest.clone()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manifest.NextGenerationID++
		token, err := db.saveLeafGenerationManifestWithStableResource(manifest, rootpublication.ReachabilityOuterLeafGeneration)
		if err != nil {
			b.Fatal(err)
		}
		token.Release()
	}
}
