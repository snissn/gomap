package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
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

func newRevisionStore5066(t *testing.T) (*leafGenerationManifestStore, *rootpublication.StableResourceToken, *rootpublication.StableResourceToken) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("exact namespaces unavailable")
	}
	s := newLeafGenerationManifestStore(t.TempDir(), rootpublication.NewIdentityPinRegistry(), leafGenerationManifestStable, nil)
	t.Cleanup(func() { s.Close() })
	m := newLeafGenerationManifest(1)
	old, err := s.Replace(m)
	if err != nil {
		t.Fatal(err)
	}
	current, err := s.Replace(m)
	if err != nil {
		old.Release()
		t.Fatal(err)
	}
	t.Cleanup(old.Release)
	t.Cleanup(current.Release)
	return s, old, current
}

func TestLeafManifestRevisionGCGates5066(t *testing.T) {
	for _, mode := range []string{"pins", "dry", "canceled", "limits", "malformed", "unsupported", "syncfailure", "retainedparent"} {
		t.Run(mode, func(t *testing.T) {
			s, old, current := newRevisionStore5066(t)
			oldPath := filepath.Join(s.leafDir, leafGenerationDurableManifestFileName(old.Generation()))
			ctx := context.Background()
			opts := LeafGenerationGCOptions{}
			stats := LeafGenerationGCStats{}
			switch mode {
			case "pins":
			case "dry":
				old.Release()
				opts.DryRun = true
			case "canceled":
				old.Release()
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "limits":
				old.Release()
				opts.MaintenanceLimits = LeafGenerationMaintenanceLimits{NativeEntries: 1, NativeBytes: 1, PagerPages: 1}
			case "malformed":
				old.Release()
				if err := os.WriteFile(filepath.Join(s.leafDir, "manifest.durable.bad.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unsupported":
				old.Release()
				s.stableCapability = func() bool { return false }
			case "syncfailure":
				old.Release()
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Point == durabilitycut.BeforeDeletionDirectorySync {
						return errors.New("sync cut")
					}
					return nil
				})
				defer restore()
			case "retainedparent":
				old.Release()
				retained := s.leafDir + "-retained"
				if err := os.Rename(s.leafDir, retained); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(s.leafDir, 0700); err != nil {
					t.Fatal(err)
				}
				oldPath = filepath.Join(retained, filepath.Base(oldPath))
			}
			err := s.gcRevisions(ctx, opts, &stats)
			switch mode {
			case "pins":
				if err != nil || stats.ManifestRevisionsProtected != 2 || stats.ManifestRevisionsDeleted != 0 {
					t.Fatalf("stats=%+v err=%v", stats, err)
				}
			case "dry":
				if err != nil || stats.ManifestRevisionsEligible != 1 || stats.ManifestRevisionsDeleted != 0 {
					t.Fatalf("stats=%+v err=%v", stats, err)
				}
			case "retainedparent":
				if err != nil || stats.ManifestRevisionsDeleted != 1 {
					t.Fatalf("stats=%+v err=%v", stats, err)
				}
				if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
					t.Fatalf("exact-parent old revision remains: %v", err)
				}
				return
			case "syncfailure":
				if !errors.Is(err, ErrRecoveryRequired) || !s.poisoned || stats.ManifestRevisionsDeleted != 0 {
					t.Fatalf("stats=%+v poisoned=%v err=%v", stats, s.poisoned, err)
				}
				return
			default:
				if err == nil || stats.ManifestRevisionsDeleted != 0 {
					t.Fatalf("stats=%+v err=%v", stats, err)
				}
			}
			if _, err := os.Stat(oldPath); err != nil {
				t.Fatalf("protected/rejected revision absent: %v", err)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(oldPath), leafGenerationDurableManifestFileName(current.Generation()))); err != nil {
				t.Fatalf("current revision absent: %v", err)
			}
		})
	}
}

func TestLeafManifestRevisionGCReboundChild5066(t *testing.T) {
	s, old, _ := newRevisionStore5066(t)
	old.Release()
	name := leafGenerationDurableManifestFileName(old.Generation())
	path := filepath.Join(s.leafDir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	moved := path + ".original"
	s.hooks.BeforeRename = func() error {
		if err := os.Rename(path, moved); err != nil {
			return err
		}
		return os.WriteFile(path, data, 0600)
	}
	stats := LeafGenerationGCStats{}
	err = s.gcRevisions(context.Background(), LeafGenerationGCOptions{}, &stats)
	if !errors.Is(err, ErrRecoveryRequired) || stats.ManifestRevisionsDeleted != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if _, err := os.Stat(moved); err != nil {
		t.Fatalf("captured original identity lost: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("rebound canonical identity lost: %v", err)
	}
}

func TestLeafManifestRevisionGCHeldAndRecovery5066(t *testing.T) {
	database, writer := openLeafGenerationGCTestDB(t)
	writeLeafGenerationKeys(t, database, "revision", 64, 'a')
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing held snapshot")
	}
	defer held.Close()
	heldName := leafGenerationDurableManifestFileName(held.state.LeafGenerations.sourceManifest.ManifestRevision)
	if err := writer.rotateLeaf(); err != nil {
		t.Fatal(err)
	}
	writeLeafGenerationKeys(t, database, "revision", 64, 'b')
	dir := LeafLogDirPath(database.dir)
	var recoveryNames []string
	database.durablePublishMu.Lock()
	for _, resources := range database.durableRoot.slotResources {
		for _, token := range resources.Tokens() {
			if token.Kind() == rootpublication.ResourceOuterLeafManifest {
				recoveryNames = append(recoveryNames, token.ResourceID())
			}
		}
	}
	database.durablePublishMu.Unlock()
	if len(recoveryNames) < 2 {
		t.Fatalf("both recovery slots not represented: %v", recoveryNames)
	}
	for range 5 {
		closure, err := database.PrepareLeafGenerationManifestStableClosure()
		if err != nil {
			t.Fatal(err)
		}
		closure.Release()
	}
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); err != nil {
		t.Fatal(err)
	}
	files := revisionFiles5066(t, dir)
	if !files[heldName] {
		t.Fatalf("held snapshot revision %s deleted", heldName)
	}
	for _, name := range recoveryNames {
		if !files[name] {
			t.Fatalf("recoverable slot revision %s deleted", name)
		}
	}
	got, err := held.Get([]byte("revision-0000"))
	if err != nil || len(got) != 32 || got[0] != 'a' {
		t.Fatalf("held row=%q err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	// Supersede both old durable slots through ordinary complete root writes.
	writeLeafGenerationKeys(t, database, "revision", 64, 'c')
	writeLeafGenerationKeys(t, database, "revision", 64, 'd')
	stats, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ManifestRevisionsDeleted == 0 {
		t.Fatalf("released old closure was not reclaimed: %+v", stats)
	}
	files = revisionFiles5066(t, dir)
	if files[heldName] {
		t.Fatalf("released held revision still retained: %s", heldName)
	}
	got, err = database.Get([]byte("revision-0000"))
	if err != nil || len(got) != 32 || got[0] != 'd' {
		t.Fatalf("current row=%q err=%v", got, err)
	}
}

func TestLeafManifestRevisionGCReadOnly5066(t *testing.T) {
	opts := Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		closure, err := database.PrepareLeafGenerationManifestStableClosure()
		if err != nil {
			t.Fatal(err)
		}
		closure.Release()
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	before := revisionFiles5066(t, LeafLogDirPath(opts.Dir))
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("apply read-only: %v", err)
	}
	if _, err := database.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true}); err != nil {
		t.Fatalf("dry read-only: %v", err)
	}
	after := revisionFiles5066(t, LeafLogDirPath(opts.Dir))
	if len(after) != len(before) {
		t.Fatalf("read-only changed revisions: %d -> %d", len(before), len(after))
	}
}
