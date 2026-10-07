package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// These regressions intentionally use no new production fields: the same file
// compiles against the pre-repair source and fails on observable unsafe behavior.
func retirementIdentityAtPath(t *testing.T, path string) rootpublication.StableIdentity {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func retirementIdentityClosedZombie(t *testing.T, additional bool) (*Manager, *File, rootpublication.StableIdentity) {
	t.Helper()
	dir := t.TempDir()
	segmentDir := dir
	if additional {
		segmentDir = t.TempDir()
	}
	id := writeTestSegment(t, segmentDir, 0, 1, 1, []byte("original retirement owner"))
	manager, err := NewManager(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if additional {
		if err := manager.AddScanDir(segmentDir); err != nil {
			t.Fatal(err)
		}
	}
	set := manager.CurrentSetNoRefresh()
	file := set.Files[id]
	if file == nil {
		t.Fatal("original segment missing from real manager set")
	}
	identity := retirementIdentityAtPath(t, file.Path)
	if err := manager.MarkZombie(id); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("injected retirement unlink failure")
	err = func() error {
		originalRemove := removeSegmentPath
		defer func() { removeSegmentPath = originalRemove }()
		removeSegmentPath = func(string, func(string) error) error { return wantErr }
		return manager.Release(set)
	}()
	if !errors.Is(err, wantErr) || !file.closed.Load() {
		t.Fatalf("real final Release did not close then fail unlink: %v", err)
	}
	if err := manager.Refresh(); err != nil {
		t.Fatalf("unchanged retired identity refused: %v", err)
	}
	if !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, file.Path)) {
		t.Fatal("failed unlink did not preserve the original physical identity")
	}
	return manager, file, identity
}

func retirementIdentityAssertOwner(t *testing.T, manager *Manager, file *File) {
	t.Helper()
	// Taking these locks after refusal also detects a leaked manager mutex.
	manager.mu.RLock()
	tracked := manager.files[file.ID]
	manager.mu.RUnlock()
	if tracked != file || !file.IsZombie.Load() {
		t.Fatal("refusal changed the exact tracked retirement owner")
	}
	set := manager.CurrentSetNoRefresh()
	if set.Files[file.ID] != nil {
		t.Fatal("refusal resurrected the retired segment")
	}
	if err := manager.Release(set); err != nil {
		t.Fatal(err)
	}
}

func retirementIdentityRemove(manager *Manager, file *File, identity rootpublication.StableIdentity, mode string) error {
	switch mode {
	case "RemoveSegment":
		return manager.RemoveSegment(file.ID)
	case "RemoveSegmentExpectedIdentity":
		return manager.RemoveSegmentExpectedIdentity(file.ID, identity)
	case "RemoveSegmentIfUnpinned":
		_, err := manager.RemoveSegmentIfUnpinned(file.ID)
		return err
	case "RemoveSegmentForce":
		return manager.RemoveSegmentForce(file.ID)
	default:
		panic("unknown retirement removal mode")
	}
}

func TestManagerRetirementIdentityNilRegistryReplacement(t *testing.T) {
	manager, file, originalIdentity := retirementIdentityClosedZombie(t, false)
	oldPath := file.Path + ".original"
	if err := os.Rename(file.Path, oldPath); err != nil {
		t.Fatal(err)
	}
	writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("replacement must survive"))
	replacementIdentity := retirementIdentityAtPath(t, file.Path)
	if rootpublication.SamePhysicalIdentity(originalIdentity, replacementIdentity) {
		t.Fatal("fixture did not rebound the original pathname")
	}
	wantBytes, err := os.ReadFile(file.Path)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := manager.Refresh(); !errors.Is(err, rootpublication.ErrResourceConflict) {
			t.Fatalf("nil-registry Refresh accepted a rebound retired path: %v", err)
		}
	}
	if err := manager.deleteZombieFile(file); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("nil-registry retirement retry accepted a replacement: %v", err)
	}
	retirementIdentityAssertOwner(t, manager, file)
	gotBytes, err := os.ReadFile(file.Path)
	if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
		t.Fatalf("replacement bytes or physical identity changed: %v", err)
	}
	if err := os.Remove(file.Path); err != nil {
		t.Fatal(err)
	}
	if err := manager.Refresh(); err != nil {
		t.Fatalf("missing retired path refused: %v", err)
	}
	retirementIdentityAssertOwner(t, manager, file)
	if err := os.Rename(oldPath, file.Path); err != nil {
		t.Fatal(err)
	}
	if err := manager.deleteZombieFile(file); err != nil {
		t.Fatalf("restored original identity did not retire: %v", err)
	}
	manager.mu.RLock()
	tracked := manager.files[file.ID]
	manager.mu.RUnlock()
	if tracked != nil {
		t.Fatal("successful retirement retained the original owner")
	}
}

func TestManagerRetirementIdentityNilRegistryDirectory(t *testing.T) {
	for _, namespace := range []string{"primary", "additional"} {
		t.Run(namespace, func(t *testing.T) {
			manager, file, _ := retirementIdentityClosedZombie(t, namespace == "additional")
			oldPath := file.Path + ".original"
			if err := os.Rename(file.Path, oldPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(file.Path, 0o700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(file.Path, "sentinel")
			wantBytes := []byte("directory replacement must survive")
			if err := os.WriteFile(sentinel, wantBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			identity := retirementIdentityAtPath(t, file.Path)
			assertRefusal := func(label string, err error) {
				t.Helper()
				// Exact-child directory opens may fail directly on Windows.
				if err == nil || (runtime.GOOS != "windows" && !errors.Is(err, rootpublication.ErrResourceConflict)) {
					t.Fatalf("nil-registry %s accepted a directory retired path: %v", label, err)
				}
			}
			for range 2 {
				assertRefusal("Refresh", manager.Refresh())
			}
			assertRefusal("retry", manager.deleteZombieFile(file))
			retirementIdentityAssertOwner(t, manager, file)
			gotBytes, err := os.ReadFile(sentinel)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatalf("replacement directory identity or sentinel changed: %v", err)
			}
			if err := os.Remove(sentinel); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(file.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.Refresh(); err != nil {
				t.Fatalf("missing retired path refused: %v", err)
			}
			retirementIdentityAssertOwner(t, manager, file)
			if err := os.Rename(oldPath, file.Path); err != nil {
				t.Fatal(err)
			}
			if err := manager.deleteZombieFile(file); err != nil {
				t.Fatalf("restored original directory path did not retire: %v", err)
			}
		})
	}
}

func TestManagerRetirementIdentityDirectPreexistingReplacement(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			manager, file, identity := retirementIdentityClosedZombie(t, false)
			if err := os.Rename(file.Path, file.Path+".original"); err != nil {
				t.Fatal(err)
			}
			writeTestSegment(t, filepath.Dir(file.Path), 0, 1, 2, []byte("direct removal replacement"))
			before := retirementIdentityAtPath(t, file.Path)
			wantBytes, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			if err := retirementIdentityRemove(manager, file, identity, mode); !errors.Is(err, rootpublication.ErrResourceConflict) {
				t.Fatalf("%s accepted a pre-existing retired replacement: %v", mode, err)
			}
			retirementIdentityAssertOwner(t, manager, file)
			gotBytes, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(before, retirementIdentityAtPath(t, file.Path)) {
				t.Fatalf("%s changed replacement bytes or identity: %v", mode, err)
			}
		})
	}
}

func TestManagerRetirementIdentityDirectQuarantineReplacement(t *testing.T) {
	for _, mode := range []string{"RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("direct original"))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			manager.mu.RLock()
			file := manager.files[id]
			manager.mu.RUnlock()
			identity := retirementIdentityAtPath(t, file.Path)
			var replacementIdentity rootpublication.StableIdentity
			var wantBytes []byte
			called := false
			err = func() error {
				originalRemove := removeSegmentPath
				defer func() { removeSegmentPath = originalRemove }()
				removeSegmentPath = func(path string, remove func(string) error) error {
					if called || !file.closed.Load() {
						t.Fatal("direct unlink hook did not run once after original close")
					}
					called = true
					// Old source passes the canonical path; repaired source passes
					// its captured private quarantine. Both fixtures rebound only
					// after the original handle is actually closed (Windows safe).
					if path == file.Path {
						if err := os.Rename(path, path+".original"); err != nil {
							return err
						}
					}
					writeTestSegment(t, dir, 0, 1, 2, []byte("canonical successor must survive"))
					replacementIdentity = retirementIdentityAtPath(t, file.Path)
					wantBytes, err = os.ReadFile(file.Path)
					if err != nil {
						return err
					}
					return originalRemove(path, remove)
				}
				return retirementIdentityRemove(manager, file, identity, mode)
			}()
			if err != nil || !called {
				t.Fatalf("%s actual direct unlink failed: called=%t error=%v", mode, called, err)
			}
			gotBytes, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(replacementIdentity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatalf("%s unlinked the new canonical replacement: %v", mode, err)
			}
		})
	}
}

func TestManagerRetirementIdentityCaptureFailure(t *testing.T) {
	for _, mode := range []string{"MarkZombie", "MarkZombieIfTracked", "RemoveSegment", "RemoveSegmentExpectedIdentity", "RemoveSegmentIfUnpinned", "RemoveSegmentForce"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			id := writeTestSegment(t, dir, 0, 1, 1, []byte("uncaptured live owner"))
			manager, err := NewManager(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Close() })
			manager.mu.RLock()
			file := manager.files[id]
			manager.mu.RUnlock()
			identity := retirementIdentityAtPath(t, file.Path)
			wantBytes, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			// No registry captured identity. Closing the only original handle
			// makes a later authority capture fail rather than adopt a pathname.
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "MarkZombie":
				err = manager.MarkZombie(id)
			case "MarkZombieIfTracked":
				var tracked, newlyMarked bool
				tracked, newlyMarked, err = manager.MarkZombieIfTracked(id)
				if !tracked || newlyMarked {
					t.Fatalf("failed capture changed tracked/newlyMarked: %t/%t", tracked, newlyMarked)
				}
			default:
				err = retirementIdentityRemove(manager, file, identity, mode)
			}
			if err == nil {
				t.Fatalf("%s accepted retirement without an original open identity handle", mode)
			}
			manager.mu.RLock()
			tracked := manager.files[id]
			manager.mu.RUnlock()
			if tracked != file || file.IsZombie.Load() {
				t.Fatal("capture refusal forgot or published the live owner as zombie")
			}
			gotBytes, err := os.ReadFile(file.Path)
			if err != nil || !bytes.Equal(gotBytes, wantBytes) || !rootpublication.SamePhysicalIdentity(identity, retirementIdentityAtPath(t, file.Path)) {
				t.Fatalf("capture refusal changed uncaptured original pathname: %v", err)
			}
		})
	}
}
