package powerlossoracle

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureOmitsProcessLocalLock(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "LOCK"), []byte("process-local"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("durable"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "LOCK")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("excluded LOCK stat err=%v, want not-exist", err)
	}
	data, err := os.ReadFile(filepath.Join(crashDir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "durable" {
		t.Fatalf("materialized data=%q, want durable", data)
	}
}

func TestCaptureOmitsNestedProcessLocalLocks(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"maindb", "dictdb", "nested/leafdb"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(dir), "LOCK"), []byte("process-local"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(dir), "data"), []byte(dir), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	walDir := filepath.Join(root, "maindb", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "command-wal-journal-owner.lock"), []byte("process-local"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("durable"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"dictdb/data", "maindb/data", "maindb/wal/000001.wal", "nested/leafdb/data"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stable paths=%v want=%v", got, want)
	}
}

func TestCaptureExcludingKeepsNonExcludedNestedLock(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dictdb", "LOCK"), []byte("captured explicitly"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "skip"), []byte("excluded"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := captureExcluding(root, "skip")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"dictdb/LOCK"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stable paths=%v want=%v", got, want)
	}
}

func TestCaptureRegularPathSnapshotRejectsDirectoryRebind(t *testing.T) {
	_, _, err := captureRegularPathSnapshot(t.TempDir())
	if err == nil {
		t.Fatal("regular-file snapshot accepted a directory rebound")
	}
	if !strings.Contains(err.Error(), "rebound to") {
		t.Fatalf("regular-file snapshot err=%v, want rebound diagnostic", err)
	}
}

func TestCapturedModelObserveContinuesToOmitNestedLocks(t *testing.T) {
	root := t.TempDir()
	dictDir := filepath.Join(root, "dictdb")
	if err := os.Mkdir(dictDir, 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(dictDir, "LOCK")
	if err := os.WriteFile(lockPath, []byte("process-local"), 0o600); err != nil {
		t.Fatal(err)
	}
	ownerLockPath := filepath.Join(dictDir, "command-wal-journal-owner.lock")
	if err := os.WriteFile(ownerLockPath, []byte("process-local"), 0o600); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(dictDir, "data")
	if err := os.WriteFile(dataPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	models := []struct {
		name  string
		model *Model
	}{{"captured", model}, {"clone", model.Clone()}}
	if err := os.WriteFile(dataPath, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range models {
		t.Run(candidate.name, func(t *testing.T) {
			if err := candidate.model.Observe(root, durabilitycut.Event{
				Point: durabilitycut.AfterDependencyFileSync,
				Path:  dataPath,
			}); err != nil {
				t.Fatal(err)
			}
			if got, want := candidate.model.StablePaths(), []string{"dictdb/data"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("stable paths=%v want=%v", got, want)
			}
			crashDir := t.TempDir()
			if err := candidate.model.MaterializeStable(crashDir); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(crashDir, "dictdb", "data"))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "new" {
				t.Fatalf("stable data=%q want new", got)
			}
		})
	}
}

func TestObserveDependencyFileSyncPromotesOnlyExactPath(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"value.log", "leaf.log"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("old-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"value.log", "leaf.log"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("new-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point: durabilitycut.AfterDependencyFileSync,
		Path:  filepath.Join(root, "value.log"),
	}); err != nil {
		t.Fatal(err)
	}
	// Missing exact instrumentation must fail rather than silently promote an
	// inaccurate image or accept a false no-op.
	if err := model.Observe(root, durabilitycut.Event{Point: durabilitycut.AfterDependencyFileSync}); err == nil {
		t.Fatal("empty dependency-sync path was accepted")
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"value.log": "new-value.log",
		"leaf.log":  "old-leaf.log",
	} {
		got, err := os.ReadFile(filepath.Join(crashDir, name))
		if err != nil || string(got) != want {
			t.Fatalf("stable %s=%q err=%v want %q", name, got, err, want)
		}
	}
}

func TestObserveGroupedDependencySyncPromotesExactSetAtomically(t *testing.T) {
	root := t.TempDir()
	names := []string{"value.log", "leaf.log", "unrelated.log"}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(root, name), []byte("old-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(root, name), []byte("new-"+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point: durabilitycut.AfterDependencyFileSync,
		Paths: []string{filepath.Join(root, "value.log"), filepath.Join(root, "leaf.log")},
	}); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"value.log":     "new-value.log",
		"leaf.log":      "new-leaf.log",
		"unrelated.log": "old-unrelated.log",
	} {
		got, err := os.ReadFile(filepath.Join(crashDir, name))
		if err != nil || string(got) != want {
			t.Fatalf("stable %s=%q err=%v want %q", name, got, err, want)
		}
	}
}

func TestObserveRejectsMissingOrOutOfRootPersistencePaths(t *testing.T) {
	root := t.TempDir()
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	points := []durabilitycut.Point{
		durabilitycut.AfterDependencyFileSync,
		durabilitycut.AfterNewFileDirectorySync,
		durabilitycut.AfterIndexDataSync,
		durabilitycut.AfterMetaSync,
		durabilitycut.AfterDeletionDirectorySync,
	}
	for _, point := range points {
		if err := model.Observe(root, durabilitycut.Event{Point: point}); err == nil {
			t.Errorf("cut %s accepted empty path", point)
		}
		if err := model.Observe(root, durabilitycut.Event{Point: point, Path: filepath.Join(root, "..", "outside")}); err == nil {
			t.Errorf("cut %s accepted out-of-root path", point)
		}
	}
}

func TestObserveNewFileDirectorySyncPromotesExactDirectoryChildren(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"value_vlog", "unrelated"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	valuePath := filepath.Join(root, "value_vlog", "value-l0-1.log")
	unrelatedPath := filepath.Join(root, "unrelated", "other.log")
	for _, path := range []string{valuePath, unrelatedPath} {
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := model.Observe(root, durabilitycut.Event{Point: durabilitycut.AfterDependencyFileSync, Path: path}); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point: durabilitycut.AfterNewFileDirectorySync,
		Path:  filepath.Join(root, "value_vlog"),
	}); err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"value_vlog/value-l0-1.log"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stable paths after exact directory sync=%v want=%v", got, want)
	}
}

func TestObserveCreatedDirectoryChildSyncPromotesEntryNotContents(t *testing.T) {
	root := t.TempDir()
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "created")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Resource:  durabilitycut.ResourceAuxiliary,
		Namespace: durabilitycut.NamespaceCreate,
		Root:      root,
		NewPath:   child,
	}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(child, "still-volatile")
	if err := os.WriteFile(file, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point:            durabilitycut.AfterNewFileDirectorySync,
		Resource:         durabilitycut.ResourceAuxiliary,
		Root:             child,
		Path:             child,
		CreatedDirectory: child,
	}); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(crashDir, "created")); err != nil || !info.IsDir() {
		t.Fatalf("stable created directory info=%v err=%v", info, err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "created", "still-volatile")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child-handle creation sync persisted directory contents: %v", err)
	}
}

func TestStableAndVolatileBytesAreIndependent(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Write("index.db", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := model.Flush("index.db"); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(crashDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("stable bytes=%q want old", got)
	}
	if err := model.SyncFile("index.db"); err != nil {
		t.Fatal(err)
	}
	crashDir = t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(crashDir, "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("synced stable bytes=%q want new", got)
	}
}

func TestMaterializeVolatileWritesProcessVisibleImage(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "index.db"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.Write("index.db", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := model.Create("new/asset", []byte("dirty")); err != nil {
		t.Fatal(err)
	}
	dirtyDir := t.TempDir()
	if err := model.MaterializeVolatile(dirtyDir); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"index.db": "new", "new/asset": "dirty"} {
		got, err := os.ReadFile(filepath.Join(dirtyDir, filepath.FromSlash(path)))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Fatalf("volatile %s=%q want=%q", path, got, want)
		}
	}
}

func TestNestedCreateBecomesReachableOnlyAfterStableAncestorChain(t *testing.T) {
	model := newModel()
	if err := model.Create("a/b/value", []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("a/b/value"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("a/b"); err != nil {
		t.Fatalf("SyncDir(a/b): %v", err)
	}
	unreachable := t.TempDir()
	if err := model.MaterializeStable(unreachable); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(unreachable, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreachable stable child materialized before ancestor sync: %v", err)
	}
	for _, dir := range []string{".", "a"} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatalf("SyncDir(%q): %v", dir, err)
		}
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(crashDir, "a", "b", "value"))
	if err != nil || string(got) != "stable" {
		t.Fatalf("nested stable value=%q err=%v", got, err)
	}
}

func TestStableFingerprintExcludesUnreachableDurableSubtree(t *testing.T) {
	model := newModel()
	want := model.StableFingerprint()
	if err := model.Create("a/b/value", []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("a/b/value"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("a/b"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("a"); err != nil {
		t.Fatal(err)
	}
	if got := model.StableFingerprint(); got != want {
		t.Fatalf("unreachable durable subtree changed stable fingerprint: got=%s want=%s", got, want)
	}
	model.Crash()
	if got := model.StableFingerprint(); got != want {
		t.Fatalf("crash projection changed stable fingerprint: got=%s want=%s", got, want)
	}
}

func TestSyncDirReplacementPrunesOldDurableSubtree(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "dir", "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir", "old", "value"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity := model.stableDirs["dir"]
	if err := os.RemoveAll(filepath.Join(root, "dir")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(root); err != nil {
		t.Fatal(err)
	}
	newIdentity := model.volatileDirs["dir"]
	if !validStableIdentity(oldIdentity) || !validStableIdentity(newIdentity) || rootpublication.SamePhysicalIdentity(oldIdentity, newIdentity) {
		t.Skip("filesystem does not expose distinct stable directory identities")
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "dir", "old", "value")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement retained old durable subtree: %v", err)
	}
	if info, err := os.Stat(filepath.Join(crashDir, "dir")); err != nil || !info.IsDir() {
		t.Fatalf("replacement directory missing from stable image: info=%v err=%v", info, err)
	}
}

func TestSyncDirSyntheticReplacementPrunesOldDurableSubtree(t *testing.T) {
	model := newModel()
	if err := model.Create("dir/old/value", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("dir/old/value"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"dir/old", "dir", "."} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	oldIdentity := model.stableDirs["dir"]
	if err := model.Unlink("dir"); err != nil {
		t.Fatal(err)
	}
	if err := model.Create("dir/new/value", []byte("new")); err != nil {
		t.Fatal(err)
	}
	newIdentity := model.volatileDirs["dir"]
	if !validStableIdentity(oldIdentity) || !validStableIdentity(newIdentity) || rootpublication.SamePhysicalIdentity(oldIdentity, newIdentity) {
		t.Fatalf("synthetic directory replacement identities old=%+v new=%+v", oldIdentity, newIdentity)
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "dir", "old", "value")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("synthetic replacement retained old durable subtree: %v", err)
	}
	if info, err := os.Stat(filepath.Join(crashDir, "dir")); err != nil || !info.IsDir() {
		t.Fatalf("synthetic replacement directory missing from stable image: info=%v err=%v", info, err)
	}
}

func TestRenameOverwritePreservesInodeUntilDestinationDirectorySync(t *testing.T) {
	model := newModel()
	for path, value := range map[string]string{"dir/source": "source", "dir/destination": "destination"} {
		if err := model.Create(path, []byte(value)); err != nil {
			t.Fatal(err)
		}
		if err := model.SyncFile(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("dir"); err != nil {
		t.Fatal(err)
	}
	if err := model.Rename("dir/source", "dir/destination"); err != nil {
		t.Fatal(err)
	}
	before := t.TempDir()
	if err := model.MaterializeStable(before); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(before, "dir", "destination"))
	if err != nil || string(got) != "destination" {
		t.Fatalf("pre-sync destination=%q err=%v", got, err)
	}
	if err := model.SyncDir("dir"); err != nil {
		t.Fatal(err)
	}
	after := t.TempDir()
	if err := model.MaterializeStable(after); err != nil {
		t.Fatal(err)
	}
	got, err = os.ReadFile(filepath.Join(after, "dir", "destination"))
	if err != nil || string(got) != "source" {
		t.Fatalf("post-sync destination=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(after, "dir", "source")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("post-sync source err=%v want not-exist", err)
	}
}

func TestObservedRenameRecoversSourceDetachedByEarlierOverlay(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "health.json")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}

	tmp := filepath.Join(root, "health.json.tmp.1")
	if err := os.WriteFile(tmp, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   tmp,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point: durabilitycut.AfterDependencyFileSync,
		Path:  tmp,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, target); err != nil {
		t.Fatal(err)
	}

	// A callback from another concurrent producer can observe the physical
	// post-rename namespace before this rename's own callback is serialized.
	if err := model.Observe(root, durabilitycut.Event{Point: durabilitycut.BeforeUserspaceFlush}); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Namespace: durabilitycut.NamespaceRename,
		OldPath:   tmp,
		NewPath:   target,
	}); err != nil {
		t.Fatal(err)
	}

	before := t.TempDir()
	if err := model.MaterializeStable(before); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(before, "health.json")); err != nil || string(got) != "old" {
		t.Fatalf("pre-directory-sync target=%q err=%v want old", got, err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Point: durabilitycut.AfterNewFileDirectorySync,
		Path:  root,
	}); err != nil {
		t.Fatal(err)
	}
	after := t.TempDir()
	if err := model.MaterializeStable(after); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(after, "health.json")); err != nil || string(got) != "new" {
		t.Fatalf("post-directory-sync target=%q err=%v want new", got, err)
	}
}

func TestObservedCreateAcceptsSameFileCapturedBeforeCallback(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "health.json.tmp.1")
	if err := os.WriteFile(tmp, []byte("captured"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}

	// A background producer can complete CreateTemp while Capture is walking
	// the directory, then deliver that create's synchronous observer callback
	// after Capture returns. The callback describes the same physical file and
	// must reconcile with the captured baseline instead of reporting EEXIST.
	if err := model.Observe(root, durabilitycut.Event{
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   tmp,
	}); err != nil {
		t.Fatalf("observe delayed create for captured file: %v", err)
	}
	beforeSync := t.TempDir()
	if err := model.MaterializeStable(beforeSync); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(beforeSync, "health.json.tmp.1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("captured create became durable before directory sync: %v", err)
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	afterSync := t.TempDir()
	if err := model.MaterializeStable(afterSync); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(afterSync, "health.json.tmp.1")); err != nil || len(got) != 0 {
		t.Fatalf("captured create after directory-only sync=%q err=%v want empty unsynced bytes", got, err)
	}
	if err := model.SyncFile("health.json.tmp.1"); err != nil {
		t.Fatal(err)
	}
	afterFileSync := t.TempDir()
	if err := model.MaterializeStable(afterFileSync); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(afterFileSync, "health.json.tmp.1")); err != nil || string(got) != "captured" {
		t.Fatalf("captured create after file sync=%q err=%v want captured", got, err)
	}
}

func TestObservedCreatePreservesSyncsSerializedBeforeDelayedCallback(t *testing.T) {
	for _, tc := range []struct {
		name            string
		beforeCallback  func(t *testing.T, model *Model)
		afterCallback   func(t *testing.T, model *Model)
		wantStableName  bool
		wantStableBytes string
	}{
		{
			name: "file-sync",
			beforeCallback: func(t *testing.T, model *Model) {
				t.Helper()
				if err := model.SyncFile("health.json.tmp.1"); err != nil {
					t.Fatal(err)
				}
			},
			afterCallback: func(t *testing.T, model *Model) {
				t.Helper()
				if err := model.SyncDir("."); err != nil {
					t.Fatal(err)
				}
			},
			wantStableName:  true,
			wantStableBytes: "captured",
		},
		{
			name: "parent-directory-sync",
			beforeCallback: func(t *testing.T, model *Model) {
				t.Helper()
				if err := model.SyncDir("."); err != nil {
					t.Fatal(err)
				}
			},
			wantStableName:  true,
			wantStableBytes: "",
		},
		{
			name: "file-and-parent-directory-sync",
			beforeCallback: func(t *testing.T, model *Model) {
				t.Helper()
				if err := model.SyncFile("health.json.tmp.1"); err != nil {
					t.Fatal(err)
				}
				if err := model.SyncDir("."); err != nil {
					t.Fatal(err)
				}
			},
			wantStableName:  true,
			wantStableBytes: "captured",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			tmp := filepath.Join(root, "health.json.tmp.1")
			if err := os.WriteFile(tmp, []byte("captured"), 0o600); err != nil {
				t.Fatal(err)
			}
			model, err := Capture(root)
			if err != nil {
				t.Fatal(err)
			}
			tc.beforeCallback(t, model)
			if err := model.Observe(root, durabilitycut.Event{
				Namespace: durabilitycut.NamespaceCreate,
				NewPath:   tmp,
			}); err != nil {
				t.Fatalf("observe delayed create: %v", err)
			}
			if tc.afterCallback != nil {
				tc.afterCallback(t, model)
			}

			stable := t.TempDir()
			if err := model.MaterializeStable(stable); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(stable, "health.json.tmp.1")
			if !tc.wantStableName {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stable file after delayed create stat err=%v want not exist", err)
				}
				return
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != tc.wantStableBytes {
				t.Fatalf("stable file after delayed create=%q err=%v want=%q", got, err, tc.wantStableBytes)
			}
		})
	}
}

func TestObservedCreateRejectsDifferentFileAtCapturedPath(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "health.json.tmp.1")
	if err := os.WriteFile(tmp, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}

	// Keep the captured inode alive under another name so recreating tmp cannot
	// reuse its physical identity.
	if err := os.Rename(tmp, filepath.Join(root, "captured-inode")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Observe(root, durabilitycut.Event{
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   tmp,
	}); err == nil || !strings.Contains(err.Error(), "file already exists") {
		t.Fatalf("observe different file at captured path err=%v want duplicate-create error", err)
	}
}

func TestObservedCreateRejectsReplacementOverlaidAtCapturedPath(t *testing.T) {
	root := t.TempDir()
	tmp := filepath.Join(root, "health.json.tmp.1")
	if err := os.WriteFile(tmp, []byte("captured"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate another serialized callback overlaying the filesystem after the
	// captured path was rebound but before the delayed create callback arrives.
	// The volatile name now points at the replacement while the stable name
	// still points at the captured inode.
	if err := os.Rename(tmp, filepath.Join(root, "captured-inode")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := model.Overlay(root); err != nil {
		t.Fatal(err)
	}

	if err := model.Observe(root, durabilitycut.Event{
		Namespace: durabilitycut.NamespaceCreate,
		NewPath:   tmp,
	}); err == nil || !strings.Contains(err.Error(), "file already exists") {
		t.Fatalf("observe delayed create after replacement overlay err=%v want duplicate-create error", err)
	}
}

func TestCrossDirectoryRenameNeedsBothDirectorySyncs(t *testing.T) {
	model := newModel()
	if err := model.Create("from/value", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.Create("to/keep", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"from/value", "to/keep"} {
		if err := model.SyncFile(path); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range []string{".", "from", "to"} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Rename("from/value", "to/value"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("to"); err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"from/value", "to/keep", "to/value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("destination-only sync paths=%v want=%v", got, want)
	}
	if err := model.SyncDir("from"); err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"to/keep", "to/value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("both dirs synced paths=%v want=%v", got, want)
	}
}

func TestUnlinkStableNamePersistsUntilContainingDirectorySync(t *testing.T) {
	model := newModel()
	if err := model.Create("dir/value", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("dir/value"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{".", "dir"} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Unlink("dir/value"); err != nil {
		t.Fatal(err)
	}
	if got := model.StablePaths(); !reflect.DeepEqual(got, []string{"dir/value"}) {
		t.Fatalf("stable paths before deletion dir sync=%v", got)
	}
	if err := model.SyncDir("dir"); err != nil {
		t.Fatal(err)
	}
	if got := model.StablePaths(); len(got) != 0 {
		t.Fatalf("stable paths after deletion dir sync=%v", got)
	}
}

func TestUnlinkTreePersistsUntilParentDirectorySync(t *testing.T) {
	model := newModel()
	if err := model.Create("leaf/.leaf-pack-copy-1/leaf_vlog/segment.log", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("leaf/.leaf-pack-copy-1/leaf_vlog/segment.log"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{".", "leaf", "leaf/.leaf-pack-copy-1", "leaf/.leaf-pack-copy-1/leaf_vlog"} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.Unlink("leaf/.leaf-pack-copy-1"); err != nil {
		t.Fatal(err)
	}
	if got := model.StablePaths(); !reflect.DeepEqual(got, []string{"leaf/.leaf-pack-copy-1/leaf_vlog/segment.log"}) {
		t.Fatalf("stable paths before staging-parent sync=%v", got)
	}
	if err := model.SyncDir("leaf"); err != nil {
		t.Fatal(err)
	}
	if _, ok := model.stableDirs["leaf/.leaf-pack-copy-1"]; ok {
		t.Fatal("removed staging directory remained stable after parent sync")
	}
	if got := model.StablePaths(); len(got) != 0 {
		t.Fatalf("stable descendant paths after staging-parent sync=%v", got)
	}
	if _, ok := model.stableDirs["leaf/.leaf-pack-copy-1/leaf_vlog"]; ok {
		t.Fatal("removed staging descendant directory remained stable after parent sync")
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatalf("materialize stable tree after recursive unlink: %v", err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, "leaf", ".leaf-pack-copy-1")); !os.IsNotExist(err) {
		t.Fatalf("materialized staging tree stat error=%v, want not-exist", err)
	}
}

func TestRejectsAbsoluteAndTraversalPaths(t *testing.T) {
	model := newModel()
	for _, path := range []string{
		"../escape",
		`..\escape`,
		"/absolute",
		`\absolute`,
		`C:\absolute`,
		`C:/absolute`,
		`\\server\share\absolute`,
	} {
		if err := model.Create(path, nil); err == nil {
			t.Fatalf("Create(%q) succeeded", path)
		}
	}
}

func TestLogicalPathsUseCanonicalSlashSeparators(t *testing.T) {
	model := newModel()
	if err := model.Create(`dir\nested\value`, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("dir/nested/value"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{".", "dir", "dir/nested"} {
		if err := model.SyncDir(dir); err != nil {
			t.Fatalf("SyncDir(%q): %v", dir, err)
		}
	}
	if got, want := model.StablePaths(), []string{"dir/nested/value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("stable paths=%v want=%v", got, want)
	}
	for _, entry := range model.Trace() {
		if strings.Contains(entry, `\`) {
			t.Fatalf("trace contains host-specific separator: %v", model.Trace())
		}
	}
}

func TestDirectorySyncControlsCreateRenameAndUnlink(t *testing.T) {
	model := newModel()
	if err := model.Create("vlog/segment-1", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncFile("vlog/segment-1"); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	if err := model.SyncDir("vlog"); err != nil {
		t.Fatal(err)
	}
	if err := model.Rename("vlog/segment-1", "vlog/segment-2"); err != nil {
		t.Fatal(err)
	}
	if err := model.Unlink("vlog/segment-2"); err != nil {
		t.Fatal(err)
	}
	if got, want := model.StablePaths(), []string{"vlog/segment-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before directory sync stable paths=%v want %v", got, want)
	}
	if err := model.SyncDir("vlog"); err != nil {
		t.Fatal(err)
	}
	if got := model.StablePaths(); len(got) != 0 {
		t.Fatalf("after deletion directory sync stable paths=%v want empty", got)
	}
}

func TestCloneAndCrashDiscardVolatileState(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "wal"), []byte("stable"), 0o644); err != nil {
		t.Fatal(err)
	}
	model, err := Capture(source)
	if err != nil {
		t.Fatal(err)
	}
	clone := model.Clone()
	if err := clone.Write("wal", []byte("volatile")); err != nil {
		t.Fatal(err)
	}
	clone.Crash()
	if trace := clone.Trace(); len(trace) == 0 || trace[len(trace)-1] != "crash" {
		t.Fatalf("trace=%v want crash suffix", trace)
	}
	if !reflect.DeepEqual(model.StablePaths(), clone.VolatilePaths()) {
		t.Fatalf("crash volatile paths=%v stable=%v", clone.VolatilePaths(), model.StablePaths())
	}
}

func TestUseObservedTraceDoesNotChangeModeledBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "index.db")
	if err := os.WriteFile(path, []byte("stable"), 0o600); err != nil {
		t.Fatal(err)
	}
	selective, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	observed := selective.Clone()
	if err := os.WriteFile(path, []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := observed.Observe(root, durabilitycut.Event{Point: durabilitycut.AfterMetaWrite, Resource: durabilitycut.ResourceMeta, Path: path}); err != nil {
		t.Fatal(err)
	}
	stableBefore := selective.StableFingerprint()
	pathsBefore := selective.VolatilePaths()
	rangesBefore, err := selective.ChangedRanges("index.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := selective.UseObservedTrace(observed); err != nil {
		t.Fatal(err)
	}
	if got := selective.Trace(); len(got) < 1 || !strings.HasPrefix(got[len(got)-1], "cut:after-meta-write:") {
		t.Fatalf("bound trace=%v", got)
	}
	if got := selective.StableFingerprint(); got != stableBefore {
		t.Fatalf("stable fingerprint changed: got=%s want=%s", got, stableBefore)
	}
	if got := selective.VolatilePaths(); !reflect.DeepEqual(got, pathsBefore) {
		t.Fatalf("volatile paths changed: got=%v want=%v", got, pathsBefore)
	}
	if got, err := selective.ChangedRanges("index.db"); err != nil || !reflect.DeepEqual(got, rangesBefore) {
		t.Fatalf("changed ranges=%v err=%v want=%v", got, err, rangesBefore)
	}
}
