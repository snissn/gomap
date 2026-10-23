package powerlossoracle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func assertStableContents(t *testing.T, m *Model, path, want string) {
	t.Helper()
	image := t.TempDir()
	if err := m.Clone().MaterializeStable(image); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(image, filepath.FromSlash(path)))
	if err != nil || string(got) != want {
		t.Fatalf("stable %s=%q, %v; want %q", path, got, err, want)
	}
}

func TestOverlayRecycledPhysicalIdentityDoesNotReuseStableOnlyInode(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old")
	if err := os.WriteFile(oldPath, []byte("old-stable"), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := Capture(root)
	if err != nil {
		t.Fatal(err)
	}
	oldID := m.volatile["old"]
	recycled := m.inodes[oldID].stableIdentity
	if !validStableIdentity(recycled) {
		t.Skip("native file identity unavailable")
	}
	// The old name remains durable until the parent barrier, but no live name
	// authorizes matching its inode to a newly observed physical object.
	if err := m.Unlink("old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(oldPath); err != nil {
		t.Fatal(err)
	}
	newPath := filepath.Join(root, "new")
	if err := os.WriteFile(newPath, []byte("new-unsynced"), 0600); err != nil {
		t.Fatal(err)
	}
	m.identityFromFile = func(file *os.File) (rootpublication.StableIdentity, error) {
		if filepath.Base(file.Name()) == "new" {
			return recycled, nil
		}
		return captureStableIdentityFromFile(file)
	}
	if err := m.Overlay(root); err != nil {
		t.Fatal(err)
	}
	if m.volatile["new"] == oldID {
		t.Fatal("recycled native ID inherited a stable-only model inode")
	}
	// Publishing the new name without syncing the file must not inherit old bytes.
	unsynced := m.Clone()
	if err := unsynced.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	unsynced.Crash()
	assertStableContents(t, unsynced, "new", "")
	// Syncing the new object cannot mutate the old crash-visible object either.
	if err := m.SyncFile("new"); err != nil {
		t.Fatal(err)
	}
	assertStableContents(t, m, "old", "old-stable")
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertNoStablePath(t, m, "old")
	assertStableContents(t, m, "new", "new-unsynced")
}

func unsupportedDirectoryIdentity(file *os.File) (rootpublication.StableIdentity, error) {
	info, err := file.Stat()
	if err != nil {
		return rootpublication.StableIdentity{}, err
	}
	if info.IsDir() {
		return rootpublication.StableIdentity{}, nil
	}
	return captureStableIdentityFromFile(file)
}

func TestUnsupportedDirectoryIdentityCaptureOverlayAndRename(t *testing.T) {
	for _, crossParent := range []bool{false, true} {
		name := "same-parent"
		if crossParent {
			name = "cross-parent"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			for _, dir := range []string{"source/inner", "destination"} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, "source/inner/data"), []byte("captured"), 0600); err != nil {
				t.Fatal(err)
			}
			m, err := captureWithIdentity(root, true, nil, unsupportedDirectoryIdentity)
			if err != nil {
				t.Fatal(err)
			}
			ids := make(map[rootpublication.StableIdentity]bool)
			for path, id := range m.volatileDirs {
				if !validStableIdentity(id) || ids[id] {
					t.Fatalf("directory %s has invalid or aliased identity %+v", path, id)
				}
				ids[id] = true
			}
			assertStableContents(t, m, "source/inner/data", "captured")
			sourceID := m.volatileDirs["source"]
			// Clone and an unchanged Overlay must reuse directory objects.
			m = m.Clone()
			if err := m.Overlay(root); err != nil {
				t.Fatal(err)
			}
			if m.volatileDirs["source"] != sourceID {
				t.Fatal("Overlay changed an unchanged synthetic directory identity")
			}
			// Use the maintained retained-directory opener: Windows requires delete
			// sharing for a real rename while the original handle remains open.
			handle, err := rootpublication.OpenStableParent(filepath.Join(root, "source"))
			if err != nil {
				t.Fatal(err)
			}
			defer handle.Close()
			target := "moved"
			if crossParent {
				target = "destination/moved"
			}
			if err := os.Rename(filepath.Join(root, "source"), filepath.Join(root, target)); err != nil {
				t.Fatal(err)
			}
			// A concurrent callback may scan the renamed tree before its rename event.
			if err := m.Overlay(root); err != nil {
				t.Fatal(err)
			}
			if err := m.Observe(root, durabilitycut.Event{Namespace: durabilitycut.NamespaceRename, OldPath: filepath.Join(root, "source"), NewPath: filepath.Join(root, target)}); err != nil {
				t.Fatal(err)
			}
			if m.volatileDirs[target] != sourceID {
				t.Fatal("rename lost synthetic directory identity")
			}
			if got, err := m.stableDirectoryPath(handle); err != nil || got != target {
				t.Fatalf("retained directory handle path=%q err=%v", got, err)
			}
			// A child sync updates the same object behind the still-old durable edge.
			child := target + "/inner/new"
			if err := os.WriteFile(filepath.Join(root, child), []byte("synced-child"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := m.Overlay(root); err != nil {
				t.Fatal(err)
			}
			if err := m.SyncFile(child); err != nil {
				t.Fatal(err)
			}
			if err := m.SyncDir(target + "/inner"); err != nil {
				t.Fatal(err)
			}
			assertStableContents(t, m, "source/inner/new", "synced-child")
			assertNoStablePath(t, m, target)
			if err := m.SyncDir("."); err != nil {
				t.Fatal(err)
			}
			if crossParent {
				// Source-parent-only writeback cannot publish the destination edge.
				assertNoStablePath(t, m, "source")
				assertNoStablePath(t, m, target)
				if err := m.SyncDir("destination"); err != nil {
					t.Fatal(err)
				}
			}
			m.Crash()
			assertNoStablePath(t, m, "source")
			assertStableContents(t, m, target+"/inner/data", "captured")
			assertStableContents(t, m, target+"/inner/new", "synced-child")
		})
	}
}

func TestUnsupportedDirectoryIdentityObservedCreateAndReplacement(t *testing.T) {
	root := t.TempDir()
	m, err := captureWithIdentity(root, true, nil, unsupportedDirectoryIdentity)
	if err != nil {
		t.Fatal(err)
	}
	createDir := func(name string) {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := m.Observe(root, durabilitycut.Event{Namespace: durabilitycut.NamespaceCreate, NewPath: path}); err != nil {
			t.Fatal(err)
		}
	}
	createDir("a")
	createDir("b")
	if m.volatileDirs["a"] == m.volatileDirs["b"] {
		t.Fatal("separate created directories alias")
	}
	if err := os.WriteFile(filepath.Join(root, "a/old"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.Overlay(root); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncFile("a/old"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("a"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	oldID := m.volatileDirs["a"]
	if err := os.RemoveAll(filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}
	if err := m.Observe(root, durabilitycut.Event{Namespace: durabilitycut.NamespaceUnlink, OldPath: filepath.Join(root, "a")}); err != nil {
		t.Fatal(err)
	}
	createDir("a")
	if m.volatileDirs["a"] == oldID {
		t.Fatal("replacement directory reused retired identity")
	}
	assertStableContents(t, m, "a/old", "old")
	if err := m.SyncDir("a"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("."); err != nil {
		t.Fatal(err)
	}
	m.Crash()
	assertNoStablePath(t, m, "a/old")
}
