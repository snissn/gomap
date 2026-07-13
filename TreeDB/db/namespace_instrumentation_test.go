package db

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

func TestNamespaceMutationObserverFailureRequiresRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stale.asset")
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cutErr := errors.New("injected post-unlink cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceUnlink && event.OldPath == path {
			return cutErr
		}
		return nil
	})
	defer restore()

	removed, err := removePersistentFile(dir, path, durabilitycut.ResourceAuxiliary)
	if !removed {
		t.Fatal("removePersistentFile removed=false, want true")
	}
	if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("removePersistentFile error=%v, want injected cut and ErrRecoveryRequired", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("post-cut path stat error=%v, want not-exist", statErr)
	}
}

func TestRemovePersistentTreeEmitsDirectoryUnlink(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, ".leaf-pack-copy-test")
	if err := os.MkdirAll(filepath.Join(staging, leafVLogDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	var got durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		got = event
		return nil
	})
	defer restore()
	if err := removePersistentTree(root, staging, durabilitycut.ResourceOuterLeaf); err != nil {
		t.Fatal(err)
	}
	want := durabilitycut.Event{Resource: durabilitycut.ResourceOuterLeaf, Root: root, Namespace: durabilitycut.NamespaceUnlink, OldPath: staging}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("namespace event=%#v want=%#v", got, want)
	}
}

func TestRecoverIndexSwapEmitsNamespaceMutations(t *testing.T) {
	dir := t.TempDir()
	newPath := filepath.Join(dir, indexNewFileName)
	readyPath := filepath.Join(dir, indexReadyFileName)
	indexPath := filepath.Join(dir, indexFileName)
	if err := os.WriteFile(newPath, []byte("new"), 0o600); err != nil {
		t.Fatalf("WriteFile(new): %v", err)
	}
	if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		t.Fatalf("WriteFile(ready): %v", err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace != "" {
			events = append(events, event)
		}
		return model.Observe(dir, event)
	})
	defer restore()

	if err := recoverIndexSwap(dir); err != nil {
		t.Fatalf("recoverIndexSwap: %v", err)
	}
	want := []durabilitycut.Event{
		{Resource: durabilitycut.ResourceIndex, Root: dir, Namespace: durabilitycut.NamespaceRename, OldPath: newPath, NewPath: indexPath},
		{Resource: durabilitycut.ResourceIndex, Root: dir, Namespace: durabilitycut.NamespaceUnlink, OldPath: readyPath},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("namespace events = %#v, want %#v", events, want)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(crashDir, indexFileName))
	if err != nil {
		t.Fatalf("read stable recovered index: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("stable recovered index = %q, want new", got)
	}
	for _, stale := range []string{indexNewFileName, indexReadyFileName} {
		if _, err := os.Stat(filepath.Join(crashDir, stale)); !os.IsNotExist(err) {
			t.Fatalf("stable stale path %s error = %v, want not-exist", stale, err)
		}
	}
}

func TestDeletionDirectorySyncMaterializesStableUnlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stale.asset")
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != "" {
			points = append(points, event.Point)
		}
		return model.Observe(dir, event)
	})
	defer restore()
	removed, err := removePersistentFile(dir, path, durabilitycut.ResourceAuxiliary)
	if err != nil || !removed {
		t.Fatalf("removePersistentFile removed=%v error=%v", removed, err)
	}
	if err := syncDeletionNamespaceDirectory(dir, durabilitycut.ResourceAuxiliary); err != nil {
		t.Fatal(err)
	}
	wantPoints := []durabilitycut.Point{
		durabilitycut.BeforeDeletionDirectorySync,
		durabilitycut.AfterDeletionDirectorySync,
	}
	if !reflect.DeepEqual(points, wantPoints) {
		t.Fatalf("directory sync points = %v, want %v", points, wantPoints)
	}
	crashDir := t.TempDir()
	if err := model.MaterializeStable(crashDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(crashDir, filepath.Base(path))); !os.IsNotExist(err) {
		t.Fatalf("stable deleted path error = %v, want not-exist", err)
	}
}

func TestNamespaceDirectorySyncFailureRequiresRecovery(t *testing.T) {
	originalSyncDir := syncDirFn
	defer func() { syncDirFn = originalSyncDir }()
	syncErr := errors.New("injected directory sync failure")
	syncDirFn = func(string) error { return syncErr }

	err := syncDeletionNamespaceDirectory(t.TempDir(), durabilitycut.ResourceAuxiliary)
	if !errors.Is(err, syncErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("syncDeletionNamespaceDirectory error = %v, want sync failure and ErrRecoveryRequired", err)
	}
}
